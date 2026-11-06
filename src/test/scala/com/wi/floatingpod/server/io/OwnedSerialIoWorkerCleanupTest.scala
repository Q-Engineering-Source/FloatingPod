package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.util.Success

class OwnedSerialIoWorkerCleanupTest:
  private def tag() = IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID())
  private def waitFor(l: CountDownLatch): Unit = assertTrue(l.await(5, TimeUnit.SECONDS), "bounded latch timeout")
  private def join(t: Thread): Unit =
    t.join(5000)
    assertFalse(t.isAlive, "thread leaked")
  private def append(primary: Throwable, cleanup: Throwable): Throwable =
    if primary == null then cleanup
    else
      if primary ne cleanup then primary.addSuppressed(cleanup)
      primary

  private final class Fixture(withHook: Boolean = true, fatal: Option[Throwable] = None):
    val workRelease = new CountDownLatch(1)
    val workEntered = new CountDownLatch(1)
    val hookRelease = new CountDownLatch(1)
    val hookEntered = new CountDownLatch(1)
    val hookCalls = new AtomicInteger()
    val workerThread = new AtomicReference[Thread]()
    val hookThread = new AtomicReference[Thread]()
    val helpers = mutable.ArrayBuffer.empty[Thread]
    var hookBody: () => Unit = () => ()
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "cleanup-owned-worker",
      if withHook then Some(() => {
        hookThread.set(Thread.currentThread())
        hookCalls.incrementAndGet()
        hookEntered.countDown()
        hookBody()
      }) else None)(v => {
        workerThread.set(Thread.currentThread())
        workEntered.countDown()
        waitFor(workRelease)
        fatal.foreach(c => throw c)
        v
      })

    def checkpointOff(): IoCheckpoint =
      val result = new AtomicReference[IoCheckpoint]()
      val failure = new AtomicReference[Throwable]()
      val probe = new Thread(() => {
        try result.set(io.checkpoint)
        catch case cause: Throwable => failure.set(cause)
        ()
      }, "cleanup-lock-probe")
      helpers += probe
      probe.start()
      join(probe) // 如 hook 错误持锁，bounded fail 后 outerfinally 先释放 hook 再 join。
      if failure.get() != null then throw failure.get()
      assertNotNull(result.get(), "checkpoint probe missing")
      result.get()

    def waitOff(timeout: Long, unit: TimeUnit): Boolean =
      val result = new AtomicReference[java.lang.Boolean]()
      val failure = new AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try result.set(io.awaitExitOffMain(timeout, unit))
        catch case c: Throwable => failure.set(c)
        ()
      }, "cleanup-off-main-tail")
      helpers += waiter
      var primary: Throwable = null
      try
        waiter.start()
        join(waiter)
      catch case c: Throwable => primary = c
      finally
        try join(waiter)
        catch case c: Throwable => primary = append(primary, c)
      if primary != null then throw primary
      if failure.get() != null then throw failure.get()
      assertNotNull(result.get(), "wait result missing")
      result.get().booleanValue()

    def cleanup(): Unit =
      var primary: Throwable = null
      def attempt(b: => Unit): Unit =
        try b
        catch case c: Throwable => primary = append(primary, c)
      workRelease.countDown()
      hookRelease.countDown()
      attempt(io.quiesce())
      attempt(assertTrue(waitOff(5, TimeUnit.SECONDS), "cleanup worker exit timeout"))
      helpers.foreach(t => attempt(join(t)))
      if workerThread.get() != null then attempt(join(workerThread.get()))
      if hookThread.get() != null then attempt(join(hookThread.get()))
      attempt(assertTrue(io.checkpoint.stopped))
      if primary != null then throw primary

  private def managed(fs: Fixture*)(b: => Unit): Unit =
    var primary: Throwable = null
    try b
    catch case c: Throwable => primary = c
    finally fs.foreach(f =>
      try f.cleanup()
      catch case c: Throwable => primary = append(primary, c))
    if primary != null then throw primary

  @Test def returnedHookRunsOnceOnSameWorkerAndKeepsCompletions(): Unit =
    val f = new Fixture()
    managed(f) {
      val first = tag(); val second = tag()
      f.hookBody = () => {
        assertSame(f.workerThread.get(), Thread.currentThread())
        assertFalse(Thread.currentThread().isDaemon)
        assertThrows(classOf[IllegalArgumentException], () => f.io.quiesce())
        assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
        // checkpoint acquires the production lock: this callback is not invoked while holding it.
        assertEquals(2, f.io.checkpoint.reserved)
      }
      f.io.submit(first, 1)
      waitFor(f.workEntered)
      f.io.submit(second, 2)
      f.io.quiesce()
      f.workRelease.countDown()
      assertTrue(f.waitOff(5, TimeUnit.SECONDS))
      assertEquals(1, f.hookCalls.get())
      assertEquals(Some(IoWorkerExitHookObservation.HookReturned), f.io.checkpoint.workerExitHook)
      assertTrue(f.io.checkpoint.exitFailure.isEmpty)
      assertEquals(Vector(IoCompletion(first, Success(1)), IoCompletion(second, Success(2))), f.io.drain(2))
      assertEquals(0, f.io.checkpoint.reserved)
      f.io.quiesce()
      assertEquals(1, f.hookCalls.get())
    }

  @Test def absentHookIsNoDeclarationAndStillCompletesExistingWork(): Unit =
    val f = new Fixture(false)
    managed(f) {
      val operation = tag()
      f.io.submit(operation, 4)
      f.io.quiesce()
      f.workRelease.countDown()
      assertTrue(f.waitOff(5, TimeUnit.SECONDS))
      assertEquals(Some(IoWorkerExitHookObservation.NoHook), f.io.checkpoint.workerExitHook)
      assertEquals(0, f.hookCalls.get())
      assertTrue(f.io.checkpoint.exitFailure.isEmpty)
      assertEquals(Vector(IoCompletion(operation, Success(4))), f.io.drain(1))
    }

  @Test def hookFailureIsExactExitFailureWithoutInventingWorkerFatal(): Unit =
    val cause = new IllegalStateException("actual hook failure")
    val f = new Fixture()
    managed(f) {
      f.hookBody = () => throw cause
      val operation = tag()
      f.io.submit(operation, 5)
      f.io.quiesce()
      f.workRelease.countDown()
      assertTrue(f.waitOff(5, TimeUnit.SECONDS))
      val checkpoint = f.io.checkpoint
      assertEquals(Some(IoWorkerExitHookObservation.HookFailed(cause)), checkpoint.workerExitHook)
      assertSame(cause, checkpoint.exitFailure.get)
      assertTrue(checkpoint.fatalCause.isEmpty)
      assertEquals(IoSubmission.RejectedStopped(Some(cause)), f.io.submit(tag(), 6))
      assertEquals(Vector(IoCompletion(operation, Success(5))), f.io.drain(1))
      assertEquals(1, f.hookCalls.get())
    }

  @Test def workerFatalRemainsPrimaryAndQueuedResponsibilitiesSurviveFailedHook(): Unit =
    val fatal = new LinkageError("actual work fatal")
    val cleanup = new IllegalStateException("hook after fatal")
    val f = new Fixture(true, Some(fatal))
    managed(f) {
      f.hookBody = () => { waitFor(f.hookRelease); throw cleanup }
      val first = tag(); val second = tag()
      f.io.submit(first, 1)
      waitFor(f.workEntered)
      f.io.submit(second, 2)
      f.workRelease.countDown()
      waitFor(f.hookEntered)
      val pendingHook = f.checkpointOff()
      assertFalse(pendingHook.stopped)
      assertTrue(pendingHook.workerExitHook.isEmpty)
      assertSame(fatal, pendingHook.fatalCause.get)
      assertEquals(IoSubmission.RejectedStopped(Some(fatal)), f.io.submit(tag(), 3))
      assertFalse(f.waitOff(10, TimeUnit.MILLISECONDS))
      assertEquals(2, f.io.checkpoint.reserved)
      f.hookRelease.countDown()
      assertTrue(f.waitOff(5, TimeUnit.SECONDS))
      assertSame(fatal, f.io.checkpoint.fatalCause.get)
      assertSame(fatal, f.io.checkpoint.exitFailure.get)
      assertEquals(Vector(cleanup), fatal.getSuppressed.toVector)
      assertEquals(Some(IoWorkerExitHookObservation.HookFailed(cleanup)), f.io.checkpoint.workerExitHook)
      val completions = f.io.drain(2)
      assertEquals(Vector(first, second), completions.map(_.tag))
      completions.foreach(c => assertSame(fatal, c.result.failed.get))
      assertEquals(0, f.io.checkpoint.reserved)
      assertEquals(1, f.hookCalls.get())
    }

  @Test def blockingHookDoesNotHoldLockOrPublishTerminalObservationEarly(): Unit =
    val f = new Fixture()
    managed(f) {
      f.hookBody = () => waitFor(f.hookRelease)
      val operation = tag()
      f.io.submit(operation, 8)
      f.io.quiesce()
      f.workRelease.countDown()
      waitFor(f.hookEntered)
      // 有界 helper 先证明 callback 没有持锁，之后 Main 在该仍阻塞的 callback 期间 drain。
      val pendingHook = f.checkpointOff()
      assertFalse(pendingHook.stopped)
      assertFalse(pendingHook.accepting)
      assertTrue(pendingHook.workerExitHook.isEmpty)
      assertTrue(pendingHook.exitFailure.isEmpty)
      assertFalse(f.waitOff(10, TimeUnit.MILLISECONDS), "timeout must not be reported as callback terminal")
      assertEquals(Vector(IoCompletion(operation, Success(8))), f.io.drain(1))
      assertThrows(classOf[IllegalArgumentException], () => f.io.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
      f.hookRelease.countDown()
      assertTrue(f.waitOff(5, TimeUnit.SECONDS))
      assertTrue(f.io.checkpoint.stopped)
      assertEquals(Some(IoWorkerExitHookObservation.HookReturned), f.io.checkpoint.workerExitHook)
      assertEquals(1, f.hookCalls.get())
    }

  @Test def assertionFailureFinallyReleasesWorkAndHookAndPreservesPrimary(): Unit =
    val f = new Fixture()
    val original = new AssertionError("actual test boundary assertion")
    f.hookBody = () => waitFor(f.hookRelease)
    val observed = assertThrows(classOf[AssertionError], () => managed(f) {
      f.io.submit(tag(), 9)
      waitFor(f.workEntered)
      throw original
    })
    assertSame(original, observed)
    assertTrue(f.io.checkpoint.stopped)
    assertEquals(Some(IoWorkerExitHookObservation.HookReturned), f.io.checkpoint.workerExitHook)
    assertEquals(1, f.hookCalls.get())
    assertFalse(f.workerThread.get().isAlive)
