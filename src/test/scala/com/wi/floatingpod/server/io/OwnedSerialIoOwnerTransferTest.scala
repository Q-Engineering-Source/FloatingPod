package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.Success

class OwnedSerialIoOwnerTransferTest:
  private def tag(): IoOperationTag = IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID())
  private def waitFor(latch: CountDownLatch): Unit = assertTrue(latch.await(5, TimeUnit.SECONDS), "bounded latch timed out")
  private def append(primary: Throwable, secondary: Throwable): Throwable =
    if primary == null then secondary
    else
      if (primary ne secondary) && !primary.getSuppressed.contains(secondary) then primary.addSuppressed(secondary)
      primary

  /** 场景的 outerfinally 收口原 worker、唯一 exact target 与全部测试 helper。 */
  private final class Fixture(capacity: Int, work: (Int, CountDownLatch) => Int):
    val release = new CountDownLatch(1)
    val claimed = new CountDownLatch(1)
    val finish = new CountDownLatch(1)
    val done = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val workerThread = new AtomicReference[Thread]()
    val receipt = new AtomicReference[IoOwnerTransfer]()
    val helpers = mutable.ArrayBuffer.empty[Thread]
    var body: () => Unit = () => ()
    val io = new OwnedSerialIo[Int, Int](capacity, Thread.currentThread(), "managed-transfer-worker")(value => {
      val previous = workerThread.getAndSet(Thread.currentThread())
      if previous != null then assertSame(previous, Thread.currentThread())
      work(value, release)
    })
    val target = new Thread(() => {
      var owns = false
      var primary: Throwable = null
      try
        io.claimOwnerTransfer(receipt.get())
        owns = true
        claimed.countDown()
        body()
      catch case cause: Throwable => primary = cause
      finally
        try
          if !owns then
            try
              io.claimOwnerTransfer(receipt.get())
              owns = true
            catch case cleanup: Throwable => primary = append(primary, cleanup)
          if owns then
            try io.quiesce()
            catch case cleanup: Throwable => primary = append(primary, cleanup)
        finally
          release.countDown()
          finish.countDown()
          claimed.countDown()
          failure.set(primary)
          done.countDown()
    }, "managed-exact-target-Main")
    def issue(boot: IoOperationTag): Unit = receipt.set(io.issueOwnerTransfer(target, boot))

    def cleanup(): Unit =
      var primary: Throwable = null
      def attempt(action: => Unit): Unit =
        try action
        catch case cause: Throwable => primary = append(primary, cause)
      release.countDown()
      finish.countDown()
      claimed.countDown()
      if receipt.get() == null then attempt(io.quiesce())
      else
        if target.getState == Thread.State.NEW then attempt(target.start())
        attempt(waitFor(done))
        attempt(join(target))
        if failure.get() != null then primary = append(primary, failure.get())
      helpers.foreach(thread => attempt(join(thread)))
      attempt(exit(io))
      attempt {
        if workerThread.get() != null then join(workerThread.get())
        assertTrue(io.checkpoint.stopped, "worker did not stop")
      }
      if primary != null then throw primary

  private def join(thread: Thread): Unit =
    thread.join(5000) // 测试清理；production Main API 不等待。
    assertFalse(thread.isAlive, "test/worker thread leaked")

  private def exit(io: OwnedSerialIo[Int, Int]): Unit =
    val done = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      try assertTrue(io.awaitExitOffMain(5, TimeUnit.SECONDS), "owned IO exit timeout")
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown()
    }, "managed-off-Main-exit-tail")
    var primary: Throwable = null
    try
      waiter.start()
      waitFor(done)
      if failure.get() != null then throw failure.get()
    catch case cause: Throwable => primary = cause
    finally
      try join(waiter)
      catch case cleanup: Throwable => primary = append(primary, cleanup)
    if primary != null then throw primary

  private def managed(fixtures: Fixture*)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      fixtures.foreach { fixture =>
        try fixture.cleanup()
        catch case cleanup: Throwable => primary = append(primary, cleanup)
      }
    if primary != null then throw primary

  @Test def pendingAndCompletedReservationsTransferWithoutRebuildingWorker(): Unit =
    val blocked = new CountDownLatch(1)
    val f = new Fixture(3, (value, release) => {
      if value == 2 then
        blocked.countDown()
        waitFor(release)
      value
    })
    val other = new Fixture(1, (value, _) => value)
    val boot = tag()
    managed(f, other) {
      f.body = () => {
        assertThrows(classOf[IllegalArgumentException], () => other.io.claimOwnerTransfer(f.receipt.get()))
        assertSame(boot, f.receipt.get().bootTag)
        assertThrows(classOf[IllegalArgumentException], () => f.io.claimOwnerTransfer(f.receipt.get()))
        assertEquals(3, f.io.checkpoint.reserved)
        assertEquals(IoSubmission.RejectedCapacity, f.io.submit(tag(), 4))
        assertEquals(Vector(IoCompletion(boot, Success(1))), f.io.drain(1))
        assertEquals(IoSubmission.Accepted, f.io.submit(boot, 4))
        assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), boot))
        assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
        f.io.quiesce()
        waitFor(f.finish)
        assertEquals(Vector(2, 3, 4), f.io.drain(3).map(_.result.get))
        assertEquals(0, f.io.checkpoint.reserved)
      }
      f.io.submit(boot, 1)
      f.io.submit(boot, 2)
      waitFor(blocked)
      f.io.submit(boot, 3)
      assertEquals(1, f.io.checkpoint.completions)
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(f.workerThread.get(), boot))
      f.issue(boot)
      assertThrows(classOf[IllegalArgumentException], () => f.io.submit(boot, 9))
      assertThrows(classOf[IllegalArgumentException], () => f.io.drain(1))
      assertThrows(classOf[IllegalArgumentException], () => f.io.quiesce())
      assertThrows(classOf[IllegalArgumentException], () => f.io.claimOwnerTransfer(f.receipt.get()))
      assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      f.target.start()
      waitFor(f.claimed)
      f.release.countDown()
      exit(f.io)
      f.finish.countDown()
      waitFor(f.done)
      if f.failure.get() != null then throw f.failure.get()
    }

  @Test def workerFatalBetweenIssueAndClaimStillTransfersFailureConsumption(): Unit =
    val entered = new CountDownLatch(1)
    val cause = new LinkageError("fatal after healthy issue")
    val f = new Fixture(2, (_, release) => { entered.countDown(); waitFor(release); throw cause })
    managed(f) {
      f.body = () => {
        assertEquals(IoSubmission.RejectedStopped(Some(cause)), f.io.submit(tag(), 3))
        val completions = f.io.drain(2)
        assertEquals(2, completions.size)
        completions.foreach(c => assertSame(cause, c.result.failed.get))
        assertEquals(0, f.io.checkpoint.reserved)
        assertFalse(f.io.checkpoint.accepting)
        assertTrue(f.io.checkpoint.stopped)
        assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      }
      f.io.submit(tag(), 1)
      waitFor(entered)
      f.io.submit(tag(), 2)
      f.issue(tag())
      f.release.countDown()
      exit(f.io)
      f.target.start()
      waitFor(f.done)
    }

  @Test def invalidTargetsAndQuiescedWorkerCannotIssue(): Unit =
    val f = new Fixture(1, (value, _) => value)
    val targetDone = new CountDownLatch(1)
    val started = new Thread(() => targetDone.countDown(), "already-started-target")
    f.helpers += started
    managed(f) {
      started.start()
      waitFor(targetDone)
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(Thread.currentThread(), tag()))
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(started, tag()))
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), null))
      assertEquals(0, f.io.checkpoint.reserved)
      f.io.quiesce()
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), tag()))
    }

  @Test def wrongIssuerAndFatalStoppedWorkerCannotIssue(): Unit =
    val cause = new LinkageError("before transfer")
    val f = new Fixture(1, (_, _) => throw cause)
    val done = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), tag()))
      catch case error: Throwable => failure.set(error)
      finally done.countDown()
      ()
    }, "wrong-issuer")
    f.helpers += wrong
    managed(f) {
      wrong.start()
      waitFor(done)
      if failure.get() != null then throw failure.get()
      f.io.submit(tag(), 1)
      exit(f.io)
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), tag()))
      assertSame(cause, f.io.drain(1).head.result.failed.get)
    }

  @Test def workerCannotWaitForItsOwnExit(): Unit =
    val instance = new AtomicReference[OwnedSerialIo[Int, Int]]()
    val f = new Fixture(1, (value, _) => {
      assertThrows(classOf[IllegalArgumentException], () => instance.get().awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      value
    })
    instance.set(f.io)
    managed(f) {
      val operation = tag()
      f.io.submit(operation, 7)
      f.io.quiesce()
      exit(f.io)
      assertEquals(Vector(IoCompletion(operation, Success(7))), f.io.drain(1))
    }

  @Test def realPreIssueAssertionFailureKeepsPrimaryAndStopsEveryWorker(): Unit =
    val f = new Fixture(1, (value, _) => value)
    val other = new Fixture(1, (value, _) => value)
    val original = new AssertionError("real pre-issue assertion")
    val observed = assertThrows(classOf[AssertionError], () => managed(f, other) {
      f.io.submit(tag(), 1)
      throw original
    })
    assertSame(original, observed)
    assertTrue(f.io.checkpoint.stopped)
    assertTrue(other.io.checkpoint.stopped)

  @Test def realTargetAssertionFailureKeepsPrimaryAndCleansExactTargetAndWorker(): Unit =
    val f = new Fixture(1, (value, release) => { waitFor(release); value })
    val original = new AssertionError("real target assertion after exact claim")
    f.body = () => throw original
    val observed = assertThrows(classOf[AssertionError], () => managed(f) {
      f.io.submit(tag(), 1)
      f.issue(tag())
      // 故意未 start；outerfinally 仅启动该 exact target 来 claim/quiesce。
    })
    assertSame(original, observed)
    assertTrue(f.io.checkpoint.stopped)
    assertFalse(f.target.isAlive)
    assertEquals(Thread.State.TERMINATED, f.target.getState)
