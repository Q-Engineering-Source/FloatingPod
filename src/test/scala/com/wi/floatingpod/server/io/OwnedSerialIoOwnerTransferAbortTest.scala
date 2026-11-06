package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.Success

class OwnedSerialIoOwnerTransferAbortTest:
  private def tag() = IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID())
  private def waitFor(l: CountDownLatch): Unit = assertTrue(l.await(5, TimeUnit.SECONDS), "bounded latch timeout")
  private def join(t: Thread): Unit =
    t.join(5000)
    assertFalse(t.isAlive, "thread leaked")
  private def append(p: Throwable, c: Throwable): Throwable =
    if p == null then c
    else
      if p ne c then p.addSuppressed(c)
      p

  private final class Fixture(fatal: Option[Throwable] = None):
    val release = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    val targetRelease = new CountDownLatch(1)
    val targetEntered = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val receipt = new AtomicReference[IoOwnerTransfer]()
    val helpers = mutable.ArrayBuffer.empty[Thread]
    var claimed = false
    var body: () => Unit = () => ()
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "abort-owned-worker")(v => {
      entered.countDown()
      waitFor(release)
      fatal.foreach(c => throw c)
      v
    })
    val target = new Thread(() => {
      var primary: Throwable = null
      try body()
      catch case c: Throwable => primary = c
      finally
        try
          if claimed then io.quiesce()
        catch case c: Throwable => primary = append(primary, c)
        finally
          release.countDown()
          targetRelease.countDown()
          targetEntered.countDown()
          failure.set(primary)
      ()
    }, "abort-exact-target")
    def issue(): Unit = receipt.set(io.issueOwnerTransfer(target, tag()))
    def exit(): Unit =
      val error = new AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try assertTrue(io.awaitExitOffMain(5, TimeUnit.SECONDS), "worker exit timeout")
        catch case c: Throwable => error.set(c)
        ()
      }, "abort-off-main-tail")
      helpers += waiter
      waiter.start()
      join(waiter)
      if error.get() != null then throw error.get()
      assertTrue(io.checkpoint.stopped)
    def cleanup(): Unit =
      var primary: Throwable = null
      def attempt(b: => Unit): Unit =
        try b
        catch case c: Throwable => primary = append(primary, c)
      release.countDown()
      targetRelease.countDown()
      if target.getState != Thread.State.NEW then attempt(join(target))
      if receipt.get() != null && !claimed && io.ownerTransferAbortCause.isEmpty then
        attempt(io.abortOwnerTransfer(receipt.get(), new IllegalStateException("test cleanup")))
      if !claimed then attempt(io.quiesce())
      attempt(exit())
      helpers.foreach(t => attempt(join(t)))
      if failure.get() != null then primary = append(primary, failure.get())
      if primary != null then throw primary

  private def managed(fs: Fixture*)(b: => Unit): Unit =
    var primary: Throwable = null
    try b
    catch case c: Throwable => primary = c
    finally fs.foreach(f =>
      try f.cleanup()
      catch case c: Throwable => primary = append(primary, c))
    if primary != null then throw primary

  @Test def oldOwnerAbortsNewTargetAndPreservesAcceptedReservations(): Unit =
    val f = new Fixture()
    managed(f) {
      val first = tag(); val second = tag()
      assertEquals(IoSubmission.Accepted, f.io.submit(first, 1))
      waitFor(f.entered)
      assertEquals(IoSubmission.Accepted, f.io.submit(second, 2))
      f.issue()
      val cause = new AssertionError("Thread.start boundary failure")
      f.io.abortOwnerTransfer(f.receipt.get(), cause)
      assertSame(cause, f.io.ownerTransferAbortCause.get)
      assertTrue(f.io.checkpoint.fatalCause.isEmpty)
      assertFalse(f.io.checkpoint.accepting)
      assertEquals(2, f.io.checkpoint.reserved)
      assertEquals(IoSubmission.RejectedQuiescing, f.io.submit(tag(), 3))
      assertThrows(classOf[IllegalArgumentException], () => f.io.issueOwnerTransfer(new Thread(), tag()))
      assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), cause))
      assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      f.release.countDown()
      f.exit()
      assertEquals(Vector(IoCompletion(first, Success(1)), IoCompletion(second, Success(2))), f.io.drain(2))
      assertEquals(0, f.io.checkpoint.reserved)
      assertSame(cause, f.io.ownerTransferAbortCause.get)
    }

  @Test def exactTargetCanAbortPreclaimFailureAndOldOwnerConsumesCompletion(): Unit =
    val f = new Fixture()
    val cause = new AssertionError("run HEAD claim boundary failure")
    managed(f) {
      val op = tag()
      f.io.submit(op, 7)
      f.issue()
      f.body = () => {
        f.io.abortOwnerTransfer(f.receipt.get(), cause)
        assertThrows(classOf[IllegalArgumentException], () => f.io.claimOwnerTransfer(f.receipt.get()))
        assertThrows(classOf[IllegalArgumentException], () => f.io.drain(1))
        assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      }
      f.target.start()
      join(f.target)
      if f.failure.get() != null then throw f.failure.get()
      f.exit()
      assertSame(cause, f.io.ownerTransferAbortCause.get)
      assertEquals(Vector(IoCompletion(op, Success(7))), f.io.drain(1))
      assertTrue(f.io.checkpoint.fatalCause.isEmpty)
    }

  @Test def oldOwnerCanAbortActualTerminatedUnclaimedTarget(): Unit =
    val f = new Fixture()
    managed(f) {
      f.issue()
      f.target.start()
      join(f.target)
      assertEquals(Thread.State.TERMINATED, f.target.getState)
      val cause = new IllegalStateException("target ended before claim")
      f.io.abortOwnerTransfer(f.receipt.get(), cause)
      f.exit()
      assertSame(cause, f.io.ownerTransferAbortCause.get)
      assertFalse(f.io.checkpoint.accepting)
    }

  @Test def runnableTargetWrongActorCrossInstanceAndNullDoNotConsumeReceipt(): Unit =
    val f = new Fixture()
    val other = new Fixture()
    managed(f, other) {
      f.issue()
      val cause = new IllegalStateException("boundary")
      assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(null, cause))
      assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), null))
      assertThrows(classOf[IllegalArgumentException], () => other.io.abortOwnerTransfer(f.receipt.get(), cause))
      val error = new AtomicReference[Throwable]()
      val wrong = new Thread(() => {
        try assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), cause))
        catch case c: Throwable => error.set(c)
        ()
      }, "wrong-abort-actor")
      f.helpers += wrong
      wrong.start(); join(wrong)
      if error.get() != null then throw error.get()
      f.body = () => {
        f.targetEntered.countDown()
        // Spin only until bounded release: exposes a real RUNNABLE target state without mocking Thread.
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while f.targetRelease.getCount != 0 && System.nanoTime() < deadline do Thread.onSpinWait()
        assertEquals(0L, f.targetRelease.getCount, "target release timeout")
      }
      f.target.start()
      waitFor(f.targetEntered)
      assertEquals(Thread.State.RUNNABLE, f.target.getState)
      assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), cause))
      assertTrue(f.io.ownerTransferAbortCause.isEmpty)
      f.targetRelease.countDown()
      join(f.target)
      if f.failure.get() != null then throw f.failure.get()
      f.io.abortOwnerTransfer(f.receipt.get(), cause)
      f.exit()
    }

  @Test def successfulClaimRejectsBothActorsAbortWithoutChangingWorkerFailureState(): Unit =
    val f = new Fixture()
    managed(f) {
      f.issue()
      val cause = new IllegalStateException("too late")
      f.body = () => {
        f.io.claimOwnerTransfer(f.receipt.get())
        f.claimed = true
        assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), cause))
        f.targetEntered.countDown()
        waitFor(f.targetRelease)
        assertTrue(f.io.ownerTransferAbortCause.isEmpty)
      }
      f.target.start()
      waitFor(f.targetEntered)
      assertThrows(classOf[IllegalArgumentException], () => f.io.abortOwnerTransfer(f.receipt.get(), cause))
      f.targetRelease.countDown()
      join(f.target)
      if f.failure.get() != null then throw f.failure.get()
      f.exit()
      assertTrue(f.io.ownerTransferAbortCause.isEmpty)
    }

  @Test def abortDoesNotOverwriteActualFatalCauseOrDiscardFailedCompletions(): Unit =
    val fatal = new LinkageError("real worker fatal")
    val abort = new IllegalStateException("preclaim abort")
    val f = new Fixture(Some(fatal))
    managed(f) {
      f.io.submit(tag(), 1)
      waitFor(f.entered)
      f.io.submit(tag(), 2)
      f.issue()
      f.release.countDown()
      f.exit()
      f.io.abortOwnerTransfer(f.receipt.get(), abort)
      assertSame(abort, f.io.ownerTransferAbortCause.get)
      assertSame(fatal, f.io.checkpoint.fatalCause.get)
      assertEquals(IoSubmission.RejectedStopped(Some(fatal)), f.io.submit(tag(), 3))
      val completions = f.io.drain(2)
      assertEquals(2, completions.size)
      completions.foreach(c => assertSame(fatal, c.result.failed.get))
      assertEquals(0, f.io.checkpoint.reserved)
    }

  @Test def realAssertionFailuresPreservePrimaryAndCleanupAllWorkers(): Unit =
    val first = new Fixture()
    val second = new Fixture()
    val original = new AssertionError("preissue assertion")
    val observed = assertThrows(classOf[AssertionError], () => managed(first, second) {
      first.io.submit(tag(), 1)
      throw original
    })
    assertSame(original, observed)
    assertTrue(first.io.checkpoint.stopped)
    assertTrue(second.io.checkpoint.stopped)
    val targetFailure = new AssertionError("preclaim target assertion")
    val f = new Fixture()
    val targetObserved = assertThrows(classOf[AssertionError], () => managed(f) {
      f.issue()
      f.body = () => throw targetFailure
      f.target.start()
      join(f.target)
      if f.failure.get() != null then throw f.failure.get()
    })
    assertSame(targetFailure, targetObserved)
    assertTrue(f.io.checkpoint.stopped)
    assertFalse(f.target.isAlive)

