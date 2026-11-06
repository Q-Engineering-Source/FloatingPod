package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.util.Success

/** 实际 production worker/queue；不运行数据库，不把框架维护 seam 称 JDBC integration。 */
class OwnedSerialIoMaintenanceTest:
  private def tag(): IoOperationTag = IoOperationTag(UUID.randomUUID(), 1L, 1L, UUID.randomUUID())
  private def waitFor(value: CountDownLatch): Unit =
    assertTrue(value.await(5, TimeUnit.SECONDS), "bounded latch timed out")

  private def tail[C, A](io: OwnedSerialIo[C, A], worker: AtomicReference[Thread]): Unit =
    val failure = new AtomicReference[Throwable]()
    val helper = new Thread(() => {
      try
        assertTrue(io.awaitExitOffMain(5, TimeUnit.SECONDS), "actual worker exit timed out")
        Option(worker.get()).foreach { actual =>
          actual.join(5000L)
          assertFalse(actual.isAlive, "actual non-daemon worker leaked")
        }
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "maintenance-test-off-main-tail")
    helper.start()
    helper.join(11000L)
    assertFalse(helper.isAlive, "bounded tail helper leaked")
    if failure.get() != null then throw failure.get()

  private def protectedRun[C, A](io: OwnedSerialIo[C, A], worker: AtomicReference[Thread],
                               releases: List[CountDownLatch])(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      releases.foreach(_.countDown())
      try
        io.quiesce()
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup
        else if primary ne cleanup then primary.addSuppressed(cleanup)
      try tail(io, worker)
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup
        else if primary ne cleanup then primary.addSuppressed(cleanup)
    if primary != null then throw primary

  @Test def deadlineElapsedMathKeepsNegativeOriginAndWrapAndDoesNotReanchorLateReturn(): Unit =
    val negative = IoMaintenanceDeadline(-100L, Duration.ofNanos(100))
    assertEquals(90L, negative.remainingAt(-90L))
    assertEquals(0L, negative.remainingAt(0L))
    assertEquals(-10L, negative.remainingAt(10L), "late deadline must remain due rather than move forward")
    assertEquals(-100L, negative.observedAtNanos)
    val wrapped = IoMaintenanceDeadline(Long.MaxValue - 20L, Duration.ofNanos(100))
    assertEquals(59L, wrapped.remainingAt(Long.MinValue + 20L))
    assertThrows(classOf[IllegalArgumentException], () => negative.remainingAt(-101L))

  @Test def nonFatalQueuedRefusalsPreserveOriginalCompletionsAndRestrictedMaintenance():Unit =
    val original=new IllegalStateException("original retained operation failure")
    val worker=new AtomicReference[Thread]()
    val ioRef=new AtomicReference[OwnedSerialIo[Int,Int]]()
    val maintained=new CountDownLatch(2)
    val maintenance=new IoWorkerMaintenance:
      override def maintain():IoMaintenanceDeadline =
        assertSame(worker.get(),Thread.currentThread())
        maintained.countDown()
        IoMaintenanceDeadline.after(Duration.ofMillis(2))
    val io=new OwnedSerialIo[Int,Int](2,Thread.currentThread(),"retained-failure-maintenance",
      workerMaintenance=Some(maintenance))(value=>{
        worker.set(Thread.currentThread())
        if value==1 then ioRef.get().scheduleMaintenanceAfter(Duration.ofMillis(2))
        throw original
      })
    ioRef.set(io)
    val first=tag();val queued=tag()
    protectedRun(io,worker,Nil) {
      assertEquals(IoSubmission.Accepted,io.submit(first,1))
      assertEquals(IoSubmission.Accepted,io.submit(queued,2))
      waitFor(maintained)
      assertTrue(io.checkpoint.accepting)
      assertTrue(io.checkpoint.fatalCause.isEmpty)
      io.quiesce();tail(io,worker)
      val results=io.drain(2)
      assertEquals(Vector(first,queued),results.map(_.tag))
      results.foreach(result=>assertSame(original,result.result.failed.get))
      assertEquals(0,io.checkpoint.reserved)
    }

  @Test def optionalHookStartsDisabledAndDoesNotRunUntilActualWorkerArmsIt(): Unit =
    val calls = new AtomicInteger()
    val worker = new AtomicReference[Thread]()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        calls.incrementAndGet()
        IoMaintenanceDeadline.after(Duration.ofSeconds(1))
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "disabled-maintenance",
      workerMaintenance = Some(maintenance))(value => { worker.set(Thread.currentThread()); value })
    protectedRun(io, worker, Nil) {
      assertEquals(IoSubmission.Accepted, io.submit(tag(), 1))
      assertEquals(IoSubmission.Accepted, io.submit(tag(), 2))
      io.quiesce()
      tail(io, worker)
      assertEquals(Vector(1, 2), io.drain(2).map(_.result.get))
      assertEquals(0, calls.get())
      assertFalse(io.checkpoint.maintenanceScheduled)
      assertFalse(io.checkpoint.maintenanceRunning)
    }

  @Test def maintenanceContinuesWithoutMainDrainWhileCompletionReservationRemainsOwned(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val threeCalls = new CountDownLatch(3)
    val calls = new AtomicInteger()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        assertSame(worker.get(), Thread.currentThread())
        assertFalse(Thread.currentThread().isDaemon)
        calls.incrementAndGet()
        threeCalls.countDown()
        IoMaintenanceDeadline.after(Duration.ofMillis(2))
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "loading-no-main-ticks",
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        ioRef.get().scheduleMaintenanceAfter(Duration.ofMillis(2))
        value
      })
    ioRef.set(io)
    val originalTag = tag()
    protectedRun(io, worker, Nil) {
      assertEquals(IoSubmission.Accepted, io.submit(originalTag, 7))
      waitFor(threeCalls) // actual worker progresses; test owner did not drain/pump/submit renewal。
      assertTrue(calls.get() >= 3)
      assertEquals(1, io.checkpoint.reserved)
      assertEquals(IoSubmission.RejectedCapacity, io.submit(tag(), 8))
      io.quiesce()
      tail(io, worker)
      assertEquals(Vector(IoCompletion(originalTag, Success(7))), io.drain(1))
      assertEquals(0, io.checkpoint.reserved)
    }

  @Test def overdueMaintenanceWinsBeforeQueuedWorkAndSelectedQuiesceDrainsExactlyOnce(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val armed = new CountDownLatch(1)
    val commandRelease = new CountDownLatch(1)
    val selected = new CountDownLatch(1)
    val maintenanceRelease = new CountDownLatch(1)
    val calls = new AtomicInteger()
    val order = mutable.ArrayBuffer.empty[String]
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        assertSame(worker.get(), Thread.currentThread())
        order.synchronized { order += "maintenance" }
        calls.incrementAndGet()
        selected.countDown()
        waitFor(maintenanceRelease)
        IoMaintenanceDeadline.after(Duration.ofMillis(1))
    val io = new OwnedSerialIo[Int, Int](3, Thread.currentThread(), "busy-maintenance-priority",
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        order.synchronized { order += value.toString }
        if value == 1 then
          ioRef.get().scheduleMaintenanceAfter(Duration.ofNanos(1))
          armed.countDown()
          waitFor(commandRelease)
        value
      })
    ioRef.set(io)
    protectedRun(io, worker, List(commandRelease, maintenanceRelease)) {
      io.submit(tag(), 1)
      waitFor(armed)
      io.submit(tag(), 2)
      io.submit(tag(), 3)
      assertEquals(IoSubmission.RejectedCapacity, io.submit(tag(), 4))
      commandRelease.countDown()
      waitFor(selected)
      assertEquals(Vector("1", "maintenance"), order.synchronized { order.toVector })
      assertTrue(io.checkpoint.maintenanceRunning)
      io.quiesce() // selection happened before quiesce; this one callback must finish。
      assertEquals(IoSubmission.RejectedQuiescing, io.submit(tag(), 5))
      maintenanceRelease.countDown()
      tail(io, worker)
      assertEquals(1, calls.get())
      assertEquals(Vector("1", "maintenance", "2", "3"), order.synchronized { order.toVector })
      assertEquals(Vector(1, 2, 3), io.drain(3).map(_.result.get))
      assertEquals(0, io.checkpoint.reserved)
    }

  @Test def maintenanceFailureIsFatalAndPreservesQueuedCompletionsAndCleanupSuppressed(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val hookEntered = new CountDownLatch(1)
    val hookRelease = new CountDownLatch(1)
    val primary = new IllegalStateException("original-maintenance-failure")
    val cleanup = new IllegalArgumentException("original-cleanup-failure")
    val executions = new AtomicInteger()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        entered.countDown()
        waitFor(release)
        throw primary
    val io = new OwnedSerialIo[Int, Int](3, Thread.currentThread(), "fatal-maintenance",
      workerExitHook = Some(() => {
        assertSame(worker.get(), Thread.currentThread())
        hookEntered.countDown()
        waitFor(hookRelease)
        throw cleanup
      }), workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        executions.incrementAndGet()
        ioRef.get().scheduleMaintenanceAfter(Duration.ofNanos(1))
        value
      })
    ioRef.set(io)
    val first = tag(); val second = tag(); val third = tag()
    protectedRun(io, worker, List(release, hookRelease)) {
      io.submit(first, 1)
      waitFor(entered)
      io.submit(second, 2)
      io.submit(third, 3)
      release.countDown()
      waitFor(hookEntered)
      assertSame(primary, io.checkpoint.fatalCause.get)
      assertFalse(io.checkpoint.stopped, "blocked callback must not publish stopped/exit early")
      assertEquals(IoSubmission.RejectedStopped(Some(primary)), io.submit(tag(), 4))
      hookRelease.countDown()
      tail(io, worker)
      val results = io.drain(3)
      assertEquals(Vector(first, second, third), results.map(_.tag))
      assertEquals(Success(1), results.head.result)
      results.tail.foreach(value => assertSame(primary, value.result.failed.get))
      assertEquals(1, executions.get())
      assertSame(primary, io.checkpoint.exitFailure.get)
      assertTrue(primary.getSuppressed.exists(_ eq cleanup))
      assertEquals(0, io.checkpoint.reserved)
    }

  @Test def quiesceBeforeDeadlinePreventsMaintenanceAndPreservesAcceptedCommand(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val armed = new CountDownLatch(1)
    val calls = new AtomicInteger()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        calls.incrementAndGet()
        IoMaintenanceDeadline.after(Duration.ofMinutes(30))
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "quiesce-before-deadline",
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        ioRef.get().scheduleMaintenanceAfter(Duration.ofMinutes(30))
        armed.countDown()
        value
      })
    ioRef.set(io)
    protectedRun(io, worker, Nil) {
      io.submit(tag(), 1)
      waitFor(armed)
      io.quiesce()
      tail(io, worker)
      assertEquals(0, calls.get())
      assertEquals(1, io.drain(1).head.result.get)
      assertFalse(io.checkpoint.maintenanceScheduled)
    }

  @Test def schedulingRejectsMainAndMalformedDeadlineWithoutArmingOrChangingReservations(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline = throw new AssertionError("disabled callback ran")
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "maintenance-guards",
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        List(Duration.ZERO, Duration.ofNanos(-1), Duration.ofMinutes(31)).foreach { invalid =>
          assertThrows(classOf[IllegalArgumentException], () => ioRef.get().scheduleMaintenanceAfter(invalid))
        }
        assertThrows(classOf[IllegalArgumentException], () => ioRef.get().scheduleMaintenanceAfter(null))
        value
      })
    ioRef.set(io)
    protectedRun(io, worker, Nil) {
      assertThrows(classOf[IllegalArgumentException], () => io.scheduleMaintenanceAfter(Duration.ofSeconds(1)))
      assertFalse(io.checkpoint.maintenanceScheduled)
      io.submit(tag(), 1)
      io.quiesce()
      tail(io, worker)
      assertEquals(1, io.drain(1).head.result.get)
      assertTrue(io.checkpoint.fatalCause.isEmpty)
    }

  @Test def interruptDuringTimedIdleWaitRetainsCompletionAndRunsSameWorkerCleanup(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val armed = new CountDownLatch(1)
    val cleanup = new CountDownLatch(1)
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline = throw new AssertionError("far deadline ran")
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "maintenance-interrupt",
      workerExitHook = Some(() => { assertSame(worker.get(), Thread.currentThread()); cleanup.countDown() }),
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        ioRef.get().scheduleMaintenanceAfter(Duration.ofMinutes(30))
        armed.countDown()
        value
      })
    ioRef.set(io)
    protectedRun(io, worker, Nil) {
      val original = tag()
      io.submit(original, 9)
      waitFor(armed)
      worker.get().interrupt() // actual thread interrupt reaches timed wait; no production reflection。
      waitFor(cleanup)
      tail(io, worker)
      assertTrue(io.checkpoint.fatalCause.get.isInstanceOf[InterruptedException])
      assertEquals(Vector(IoCompletion(original, Success(9))), io.drain(1))
      assertEquals(0, io.checkpoint.reserved)
    }

  @Test def recurringOneNanosecondIsExplicitFatalAndNeverRepeatsHotLoopOrDropsPending(): Unit =
    val worker = new AtomicReference[Thread]()
    val ioRef = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val calls = new AtomicInteger()
    val executions = new AtomicInteger()
    val maintenance = new IoWorkerMaintenance:
      override def maintain(): IoMaintenanceDeadline =
        calls.incrementAndGet()
        entered.countDown()
        waitFor(release)
        IoMaintenanceDeadline.after(Duration.ofNanos(1))
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "reject-hot-loop",
      workerMaintenance = Some(maintenance))(value => {
        worker.set(Thread.currentThread())
        executions.incrementAndGet()
        ioRef.get().scheduleMaintenanceAfter(Duration.ofNanos(1))
        value
      })
    ioRef.set(io)
    protectedRun(io, worker, List(release)) {
      val first = tag()
      val pending = tag()
      io.submit(first, 1)
      waitFor(entered)
      io.submit(pending, 2)
      release.countDown()
      tail(io, worker)
      val original = io.checkpoint.fatalCause.get
      assertTrue(original.isInstanceOf[IllegalArgumentException])
      assertEquals(1, calls.get())
      assertEquals(1, executions.get())
      val results = io.drain(2)
      assertEquals(Vector(first, pending), results.map(_.tag))
      assertEquals(Success(1), results.head.result)
      assertSame(original, results.last.result.failed.get)
      assertSame(original, io.checkpoint.exitFailure.get)
      assertEquals(0, io.checkpoint.reserved)
    }
