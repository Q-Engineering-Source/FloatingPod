package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.io.*
import com.wi.floatingpod.storage.mysql.*
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage, CountDownLatch, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 实际bridge control和IO线程；没有driver/session injection，未执行SQL。 */
class OwnedIoPreparationTest:
  private val limit = Duration.ofSeconds(5)
  private def tag() = IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID())
  private def append(primary: Throwable, cleanup: Throwable): Throwable =
    if primary == null then cleanup
    else
      if primary ne cleanup then primary.addSuppressed(cleanup)
      primary
  private def join(t: Thread): Unit =
    t.join(5000)
    assertFalse(t.isAlive, "bounded thread join failed")
  private def waitFor(l: CountDownLatch): Unit = assertTrue(l.await(5, TimeUnit.SECONDS), "bounded latch timeout")

  /** 正常桥接生产control身份；sentinel仅结束测试bridge，不伪装数据库preparation。 */
  private def onControl(body: DedicatedBootstrapBridge => Unit): Unit =
    val sentinel = new IllegalStateException("unit control finished; no runtime bootstrap")
    val failure = new AtomicReference[Throwable]()
    val factory = new DedicatedBootstrapBridge.Factory:
      override def open(server: Object, control: DedicatedBootstrapBridge): DedicatedBootstrapBridge.Callbacks =
        new DedicatedBootstrapBridge.Callbacks:
          override def prepare(): CompletionStage[Object] =
            body(control)
            CompletableFuture.failedFuture[Object](sentinel)
          override def issueTransfer(p: Object, t: Thread): Object = throw new UnsupportedOperationException("not runtime factory")
          override def claim(p: Object, r: Object): Unit = throw new UnsupportedOperationException("not runtime factory")
          override def abortTransfer(p: Object, r: Object, c: Throwable): Unit = throw new UnsupportedOperationException("not runtime factory")
          override def finalizeFailure(p: Object, r: Object, c: Throwable): CompletionStage[Void] = CompletableFuture.completedFuture[Void](null)
          override def failedStartupTermination(report: DedicatedBootstrapBridge.FailureReport): Unit =
            if report.originalCause() ne sentinel then failure.set(report.originalCause())
    val binding = DedicatedBootstrapBridge.install(factory, new DedicatedBootstrapBridge.Limits(limit, limit, limit))
    val bridge = DedicatedBootstrapBridge.create(new Object(), () => ())
    var primary: Throwable = null
    try
      bridge.startDeferred()
      assertTrue(bridge.awaitFinishedOffGame(Duration.ofSeconds(15)), "control did not finish")
      if failure.get() != null then throw failure.get()
    catch case cause: Throwable => primary = cause
    finally
      try
        bridge.requestCancellation(new IllegalStateException("unit cleanup"))
        if bridge.controlThread() != null then join(bridge.controlThread())
      catch case cleanup: Throwable => primary = append(primary, cleanup)
      finally DedicatedBootstrapBridge.uninstall(binding)
    if primary != null then throw primary

  private final class Fixture(control: DedicatedBootstrapBridge, body: Int => Int, capacity: Int = 1):
    val signal = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val worker = new AtomicReference[Thread]()
    val helpers = mutable.ArrayBuffer.empty[(Thread, AtomicReference[Throwable])]
    val io = new OwnedSerialIo[Int, Int](capacity, control.controlThread(), "bootstrap-preparation-unit-worker",
      None, Some(signal))(value => {
        worker.set(Thread.currentThread())
        entered.countDown()
        waitFor(release)
        body(value)
      })
    val operation = tag()
    val holder = OwnedIoPreparation.forBootstrap(control, io, signal, operation, 41)
    def helper(body: => Unit): Thread =
      val failure = new AtomicReference[Throwable]()
      val thread = new Thread(() => {
        try body
        catch case cause: Throwable => failure.set(cause)
        ()
      }, "preparation-test-helper")
      helpers += ((thread, failure))
      thread.start()
      thread
    def waitExit(): Unit =
      val exit = new AtomicReference[java.lang.Boolean]()
      val t = helper { exit.set(io.awaitExitOffMain(5, TimeUnit.SECONDS)) }
      join(t)
      assertEquals(java.lang.Boolean.TRUE, exit.get(), "actual worker exit timed out")
    def cleanup(): Unit =
      var primary: Throwable = null
      def attempt(b: => Unit): Unit =
        try b
        catch case c: Throwable => primary = append(primary, c)
      release.countDown()
      attempt(io.quiesce())
      attempt(waitExit())
      helpers.foreach { case (thread, failure) =>
        attempt(join(thread))
        if failure.get() != null then attempt(throw failure.get())
      }
      if worker.get() != null then attempt(join(worker.get()))
      attempt(assertTrue(io.checkpoint.stopped))
      if primary != null then throw primary

  private def managed(f: Fixture)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      f.release.countDown()
      try f.cleanup()
      catch case cause: Throwable => primary = append(primary, cause)
    if primary != null then throw primary

  @Test def successPublishesOnlyAfterExactCompletionReservationConsumed(): Unit = onControl { control =>
    val f = new Fixture(control, _ + 1)
    managed(f) {
      f.release.countDown()
      val stage = f.holder.prepare(limit)
      assertEquals(42, stage.toCompletableFuture().getNow(-1))
      assertEquals(IoPreparationState.ConsumedSuccess, f.holder.observation.state)
      assertSame(f.operation, f.holder.consumedCompletion.get.tag)
      assertEquals(0, f.io.checkpoint.reserved)
      assertEquals(0, f.io.checkpoint.completions)
      assertTrue(f.holder.transferPermitted)
      assertThrows(classOf[IllegalArgumentException], () => f.holder.prepare(limit))
    }
  }

  @Test def secondPreparationOwnsFreshSignalAndSeparateRealCompletion():Unit = onControl { control =>
    val f=new Fixture(control,_+1)
    managed(f) {
      assertThrows(classOf[IllegalArgumentException],()=>f.io.rotatePreparationCompletionSignal(f.signal))
      f.release.countDown()
      assertEquals(42,f.holder.prepare(limit).toCompletableFuture().join())
      val nextSignal=f.io.rotatePreparationCompletionSignal(f.signal)
      assertNotSame(f.signal,nextSignal);assertEquals(1L,nextSignal.getCount)
      assertFalse(f.io.ownsCompletionSignal(f.signal));assertTrue(f.io.ownsCompletionSignal(nextSignal))
      assertThrows(classOf[IllegalArgumentException],()=>f.io.rotatePreparationCompletionSignal(f.signal))
      val nextTag=tag()
      val next=OwnedIoPreparation.forBootstrap(control,f.io,nextSignal,nextTag,99)
      assertFalse(next.successfulCompletionConsumed)
      assertEquals(100,next.prepare(limit).toCompletableFuture().join())
      assertSame(f.operation,f.holder.consumedCompletion.get.tag)
      assertSame(nextTag,next.consumedCompletion.get.tag)
      assertEquals(42,f.holder.consumedCompletion.get.result.get)
      assertEquals(100,next.consumedCompletion.get.result.get)
      assertEquals(0,f.io.checkpoint.reserved)
      f.io.quiesce()
      assertThrows(classOf[IllegalArgumentException],()=>f.io.rotatePreparationCompletionSignal(nextSignal))
    }
  }

  @Test def signalledButUnconsumedMailboxCannotRotate():Unit = onControl { control =>
    val f=new Fixture(control,identity)
    managed(f) {
      assertEquals(IoSubmission.Accepted,f.io.submitPreparation(f.operation,41))
      f.release.countDown();waitFor(f.signal)
      assertEquals(1,f.io.checkpoint.reserved)
      assertThrows(classOf[IllegalArgumentException],()=>f.io.rotatePreparationCompletionSignal(f.signal))
      assertSame(f.operation,f.io.drain(1).head.tag)
      assertEquals(1L,f.io.rotatePreparationCompletionSignal(f.signal).getCount)
    }
  }

  @Test def actualWorkFailurePreservesOriginalCauseAndCannotTransfer(): Unit = onControl { control =>
    val cause = new IllegalStateException("actual unit work failure")
    val f = new Fixture(control, _ => throw cause)
    managed(f) {
      f.release.countDown()
      val stage = f.holder.prepare(limit)
      val observed = assertThrows(classOf[CompletionException], () => stage.toCompletableFuture().getNow(-1))
      assertSame(cause, observed.getCause)
      assertSame(cause, f.holder.consumedCompletion.get.result.failed.get)
      assertEquals(IoPreparationState.ConsumedFailure(cause), f.holder.observation.state)
      assertFalse(f.holder.transferPermitted)
      assertEquals(0, f.io.checkpoint.reserved)
    }
  }

  @Test def deadlinePreservesPendingFutureAndLateActualSuccessButNeverStartupTransfer(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      val timeout = assertThrows(classOf[TimeoutException], () => f.holder.prepare(Duration.ofMillis(10)))
      assertTrue(f.holder.observation.unresolved)
      assertSame(timeout, f.holder.observation.startupFailure.get)
      assertEquals(1, f.io.checkpoint.reserved)
      assertFalse(f.holder.stage.toCompletableFuture().isDone)
      assertFalse(f.holder.transferPermitted)
      f.release.countDown()
      val result = f.holder.finalizePreparation(limit)
      assertEquals(IoPreparationState.ConsumedSuccess, result.state)
      assertFalse(result.unresolved)
      assertEquals(41, f.holder.stage.toCompletableFuture().getNow(-1))
      assertFalse(f.holder.transferPermitted, "late success does not undo startup deadline")
      assertSame(timeout, result.startupFailure.get)
      assertEquals(0, f.io.checkpoint.reserved)
    }
  }

  @Test def interruptCancellationRetainsOriginalWorkAndReadonlyFutureCannotCancelIt(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      val callerCopy = f.holder.stage.toCompletableFuture()
      assertTrue(callerCopy.cancel(true))
      val interrupter = f.helper { waitFor(f.entered); control.controlThread().interrupt() }
      val cancellation = assertThrows(classOf[InterruptedException], () => f.holder.prepare(limit))
      assertTrue(Thread.currentThread().isInterrupted,"preparation must preserve original control interruption")
      assertThrows(classOf[InterruptedException],()=>f.holder.finalizePreparation(limit))
      assertSame(cancellation,f.holder.observation.startupFailure.get)
      assertTrue(Thread.interrupted(),"test control explicitly acknowledges interruption before later cleanup")
      join(interrupter)
      assertSame(cancellation, f.holder.observation.startupFailure.get)
      assertFalse(f.holder.stage.toCompletableFuture().isDone)
      assertEquals(1, f.io.checkpoint.reserved)
      f.release.countDown()
      f.holder.finalizePreparation(limit)
      assertEquals(41, f.holder.stage.toCompletableFuture().getNow(-1))
      assertFalse(f.holder.transferPermitted)
      assertEquals(0, f.io.checkpoint.reserved)
    }
  }

  @Test def capacityRejectionDoesNotExecutePreparationOrConsumeOtherWork(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      val prior = tag()
      assertEquals(IoSubmission.Accepted, f.io.submit(prior, 7))
      waitFor(f.entered)
      val stage = f.holder.prepare(limit)
      assertEquals(IoPreparationState.NotAccepted(IoSubmission.RejectedCapacity), f.holder.observation.state)
      assertThrows(classOf[CompletionException], () => stage.toCompletableFuture().getNow(-1))
      assertTrue(f.holder.consumedCompletion.isEmpty)
      assertEquals(1, f.io.checkpoint.reserved)
      f.release.countDown()
      waitFor(f.signal)
      val completion = f.io.drain(1).head
      assertSame(prior, completion.tag)
      assertEquals(7, completion.result.get)
      assertEquals(0, f.io.checkpoint.reserved)
    }
  }

  @Test def fatalAndPendingFailuresSignalAfterAllActualCompletionsAreQueued(): Unit = onControl { control =>
    val fatal = new LinkageError("actual fatal work")
    val f = new Fixture(control, _ => throw fatal, 2)
    managed(f) {
      val timeout = assertThrows(classOf[TimeoutException], () => f.holder.prepare(Duration.ofMillis(10)))
      // deadline quiesced: queued responsibilities tested separately below without holder submission.
      f.release.countDown()
      f.holder.finalizePreparation(limit)
      assertSame(fatal, f.holder.consumedCompletion.get.result.failed.get)
      assertSame(timeout, f.holder.observation.startupFailure.get)
      f.waitExit()
      assertSame(fatal, f.io.checkpoint.fatalCause.get)
      assertEquals(0, f.io.checkpoint.reserved)
    }
    val queued = new Fixture(control, _ => throw fatal, 2)
    managed(queued) {
      val first = tag(); val second = tag()
      queued.io.submit(first, 1)
      waitFor(queued.entered)
      queued.io.submit(second, 2)
      queued.release.countDown()
      waitFor(queued.signal)
      assertEquals(2, queued.io.checkpoint.completions)
      assertEquals(2, queued.io.checkpoint.reserved)
      val completions = queued.io.drain(2)
      assertEquals(Vector(first, second), completions.map(_.tag))
      completions.foreach(c => assertSame(fatal, c.result.failed.get))
      assertEquals(0, queued.io.checkpoint.reserved)
    }
  }

  @Test def actualIdleWorkerInterruptPublishesFatalTerminationWithoutDiscardingConsumedResult(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      f.release.countDown()
      f.holder.prepare(limit)
      f.worker.get().interrupt() // 下一真实pending wait也会抛InterruptedException，无sleep/状态轮询。
      f.waitExit()
      assertTrue(f.io.checkpoint.fatalCause.get.isInstanceOf[InterruptedException])
      assertTrue(f.io.checkpoint.stopped)
      assertEquals(41, f.holder.stage.toCompletableFuture().getNow(-1))
      assertEquals(0, f.io.checkpoint.reserved)
      assertFalse(f.io.checkpoint.accepting)
    }
  }

  @Test def wrongActorAndWrongSignalCannotAcquirePumpAuthority(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      assertThrows(classOf[IllegalArgumentException], () =>
        OwnedIoPreparation.forBootstrap(control, f.io, new CountDownLatch(1), tag(), 1))
      val failure = new AtomicReference[Throwable]()
      val t = f.helper {
        try f.holder.prepare(limit)
        catch case c: Throwable => failure.set(c)
      }
      join(t)
      assertTrue(failure.get().isInstanceOf[IllegalArgumentException])
      assertEquals(IoPreparationState.NotSubmitted, f.holder.observation.state)
      assertEquals(0, f.io.checkpoint.reserved)
      f.release.countDown()
      f.holder.prepare(limit)
    }
  }

  @Test def invalidBudgetFailsBeforeAcceptanceAndFailureFinallyClosesActualWorker(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    managed(f) {
      assertThrows(classOf[IllegalArgumentException], () => f.holder.prepare(Duration.ZERO))
      assertThrows(classOf[ArithmeticException], () => f.holder.prepare(Duration.ofSeconds(Long.MaxValue)))
      assertEquals(IoPreparationState.NotSubmitted, f.holder.observation.state)
      assertEquals(0, f.io.checkpoint.reserved)
    }
    val failed = new Fixture(control, identity)
    val original = new AssertionError("actual pre-consumption test failure")
    val observed = assertThrows(classOf[AssertionError], () => managed(failed) {
      failed.io.submit(tag(), 9)
      waitFor(failed.entered)
      throw original
    })
    assertSame(original, observed)
    assertTrue(failed.io.checkpoint.stopped)
    assertFalse(failed.worker.get().isAlive)
  }

  @Test def actualWorkerCannotWaitOrConsumeItsOwnPreparation(): Unit = onControl { control =>
    val holderRef = new AtomicReference[OwnedIoPreparation[Int, Int]]()
    val f = new Fixture(control, value => { holderRef.get().prepare(limit); value })
    managed(f) {
      holderRef.set(f.holder)
      f.release.countDown()
      val stage = f.holder.prepare(limit)
      val failure = assertThrows(classOf[CompletionException], () => stage.toCompletableFuture().getNow(-1)).getCause
      assertTrue(failure.isInstanceOf[IllegalArgumentException])
      assertSame(failure, f.holder.consumedCompletion.get.result.failed.get)
      assertEquals(0, f.io.checkpoint.reserved)
      assertFalse(f.holder.transferPermitted)
    }
  }

  @Test def completedPreparationDoesNotGiveTransferredTargetOrOldControlPumpAuthority(): Unit = onControl { control =>
    val f = new Fixture(control, identity)
    val receipt = new AtomicReference[IoOwnerTransfer]()
    val claimed = new AtomicBoolean()
    val targetFailure = new AtomicReference[Throwable]()
    val target = new Thread(() => {
      try
        f.io.claimOwnerTransfer(receipt.get())
        claimed.set(true)
        assertThrows(classOf[IllegalArgumentException], () => f.holder.finalizePreparation(limit))
        assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      catch case cause: Throwable => targetFailure.set(cause)
      finally
        try f.io.quiesce()
        catch case cleanup: Throwable => targetFailure.set(append(targetFailure.get(), cleanup))
      ()
    }, "exact-preparation-transfer-target")
    var primary: Throwable = null
    var issued = false
    try
      f.release.countDown()
      f.holder.prepare(limit)
      assertEquals(0, f.io.checkpoint.reserved)
      assertTrue(f.holder.transferPermitted)
      receipt.set(f.io.issueOwnerTransfer(target, f.operation))
      issued = true
      assertThrows(classOf[IllegalArgumentException], () => f.holder.finalizePreparation(limit))
      target.start()
      join(target)
      if targetFailure.get() != null then throw targetFailure.get()
    catch case cause: Throwable => primary = cause
    finally
      f.release.countDown()
      def attempt(b: => Unit): Unit =
        try b
        catch case cleanup: Throwable => primary = append(primary, cleanup)
      if !issued then attempt(f.io.quiesce())
      attempt(join(target))
      if issued && !claimed.get() &&
          (target.getState == Thread.State.NEW || target.getState == Thread.State.TERMINATED) then
        val cause = if primary != null then primary else new IllegalStateException("transfer test target ended before claim")
        attempt(f.io.abortOwnerTransfer(receipt.get(), cause))
        attempt(f.io.quiesce())
      attempt(f.waitExit())
      f.helpers.foreach { case (thread, failure) =>
        attempt(join(thread))
        if failure.get() != null then attempt(throw failure.get())
      }
      if f.worker.get() != null then attempt(join(f.worker.get()))
      attempt(assertTrue(f.io.checkpoint.stopped))
      if targetFailure.get() != null then primary = append(primary, targetFailure.get())
    if primary != null then throw primary
  }

  @Test def adapterBootOnlyGuardRejectsOrdinaryDrainAndTransferUntilActualFailureConsumed(): Unit = onControl { control =>
    val boot = UUID.randomUUID()
    val adapter = new MySqlOwnedIoAdapter(1, control.controlThread(), "bootstrap-adapter-no-sql-worker", MySqlOwnedIoIdentity(boot, 1, 1))
    val operation = IoOperationTag(boot, 1, 1, UUID.randomUUID())
    var primary: Throwable = null
    try
      val holder = adapter.installBootstrapPreparation(control, operation, MySqlOwnedIoCommand.Boot(null, null, null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.submit(operation, MySqlOwnedIoCommand.Renew))
      assertThrows(classOf[IllegalArgumentException], () => adapter.drain(1))
      val neverStartedTarget = new Thread(() => ())
      assertThrows(classOf[IllegalArgumentException], () => adapter.issueOwnerTransfer(neverStartedTarget, operation))
      val stage = holder.prepare(limit)
      val failure = assertThrows(classOf[CompletionException], () => stage.toCompletableFuture().getNow(null))
      assertEquals(IoPreparationState.ConsumedFailure(failure.getCause), holder.observation.state)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertThrows(classOf[IllegalArgumentException], () => adapter.issueOwnerTransfer(neverStartedTarget, operation))
    catch case c: Throwable => primary = c
    finally
      try
        adapter.quiesce()
        val result = new AtomicReference[java.lang.Boolean]()
        val tail = new Thread(() => { result.set(adapter.awaitExitOffMain(5, TimeUnit.SECONDS)); () })
        tail.start()
        try join(tail)
        finally join(tail)
        assertEquals(java.lang.Boolean.TRUE, result.get())
        assertTrue(adapter.checkpoint.io.stopped)
      catch case c: Throwable => primary = append(primary, c)
    if primary != null then throw primary
  }

  @Test def consumedSuccessHistoryDoesNotAuthorizeUnclaimedApplicationRetirement(): Unit = onControl { control =>
    val f = new Fixture(control, _ + 1)
    managed(f) {
      f.release.countDown()
      assertEquals(42, f.holder.prepare(limit).toCompletableFuture.getNow(-1))
      assertTrue(f.holder.successfulCompletionConsumed)
      assertTrue(f.holder.transferPermitted)
      val completion = f.holder.consumedCompletion.get
      assertThrows(classOf[IllegalStateException], () =>
        f.holder.retireConsumedApplicationValue(control, null, new Object(), 42))
      assertSame(completion, f.holder.consumedCompletion.get)
      assertTrue(f.holder.successfulCompletionConsumed)
      assertTrue(f.holder.transferPermitted)
      assertThrows(classOf[IllegalArgumentException], () => f.holder.prepare(limit))
      assertThrows(classOf[IllegalArgumentException], () => f.holder.requireApplicationValueRetired())
    }
  }
