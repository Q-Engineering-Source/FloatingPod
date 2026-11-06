package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.util.Success

class OwnedSerialIoWorkerTerminationTest:
  private def tag(): IoOperationTag = IoOperationTag(UUID.randomUUID(), 1L, 1L, UUID.randomUUID())

  private def waitFor(latch: CountDownLatch): Unit =
    assertTrue(latch.await(5, TimeUnit.SECONDS), "bounded lifecycle latch timed out")

  private def join(thread: Thread): Unit =
    thread.join(6000L)
    assertFalse(thread.isAlive, "test helper thread did not terminate")

  private def awaitOffMain[C, A](io: OwnedSerialIo[C, A], timeout: Long, unit: TimeUnit): IoWorkerTerminationWait =
    val result = new AtomicReference[IoWorkerTerminationWait]()
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val waiter = new Thread(() => {
      try result.set(io.awaitWorkerTerminationOffMain(timeout, unit))
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown()
    }, "actual-worker-termination-waiter")
    waiter.start()
    waitFor(done)
    join(waiter)
    if failure.get() != null then throw failure.get()
    assertNotNull(result.get(), "worker termination result missing")
    result.get()

  private def awaitPreInterruptedOffMain[C, A](io: OwnedSerialIo[C, A], timeout: Long, unit: TimeUnit): (IoWorkerTerminationWait, Boolean) =
    val result = new AtomicReference[IoWorkerTerminationWait]()
    val flagAfterWait = new AtomicReference[java.lang.Boolean]()
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val waiter = new Thread(() => {
      Thread.currentThread().interrupt()
      try result.set(io.awaitWorkerTerminationOffMain(timeout, unit))
      catch case cause: Throwable => failure.set(cause)
      finally
        flagAfterWait.set(Thread.currentThread().isInterrupted)
        done.countDown()
    }, "pre-interrupted-worker-termination-waiter")
    waiter.start()
    waitFor(done)
    join(waiter)
    if failure.get() != null then throw failure.get()
    assertNotNull(result.get(), "pre-interrupted worker wait result missing")
    (result.get(), flagAfterWait.get() == java.lang.Boolean.TRUE)

  private def requireProof(result: IoWorkerTerminationWait): IoWorkerTerminationProof = result match
    case IoWorkerTerminationWait.Terminated(proof) => proof
    case IoWorkerTerminationWait.TimedOut => fail("exact worker remained alive after bounded wait")
    case IoWorkerTerminationWait.Interrupted(cause) => throw cause

  private def finish[C, A](io: OwnedSerialIo[C, A]): Unit =
    io.quiesce()
    requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))

  @Test def blockedExitHookTimesOutAndProofArrivesOnlyAfterTheExactWorkerTerminates(): Unit =
    val hookEntered = new CountDownLatch(1)
    val hookRelease = new CountDownLatch(1)
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-blocked-hook",
      Some(() => {
        hookEntered.countDown()
        assertTrue(hookRelease.await(5, TimeUnit.SECONDS), "test exit-hook release timed out")
      }))(identity)
    try
      io.quiesce()
      waitFor(hookEntered)
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 0L, TimeUnit.MILLISECONDS), "zero timeout is an immediate observation, never Thread.join(0)")
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 10L, TimeUnit.MILLISECONDS))
      assertFalse(io.checkpoint.stopped, "exit-hook observation is not terminal while the actual worker remains blocked")
      assertTrue(io.checkpoint.workerExitHook.isEmpty)
      hookRelease.countDown()
      val proof = requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
      io.requireWorkerTerminationProof(proof)
      assertTrue(io.checkpoint.stopped)
      assertEquals(Some(IoWorkerExitHookObservation.HookReturned), io.checkpoint.workerExitHook)
    finally
      hookRelease.countDown()
      finish(io)

  @Test def mainAndWorkerCannotAwaitTheirOwnWorkerTermination(): Unit =
    val main = Thread.currentThread()
    var io: OwnedSerialIo[Int, Boolean] = null
    io = new OwnedSerialIo[Int, Boolean](1, main, "termination-forbidden-actors")(_ => {
      try
        io.awaitWorkerTerminationOffMain(1L, TimeUnit.SECONDS)
        false
      catch case _: IllegalArgumentException => true
    })
    try
      assertThrows(classOf[IllegalArgumentException], () => {
        io.awaitWorkerTerminationOffMain(1L, TimeUnit.SECONDS)
        ()
      })
      assertEquals(IoSubmission.Accepted, io.submit(tag(), 1))
      io.quiesce()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
      assertEquals(Success(true), io.drain(1).head.result)
    finally
      finish(io)

  @Test def transferredTargetMainCannotAwaitTheOwnedWorker(): Unit =
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-target-main")(identity)
    val receiptRef = new AtomicReference[IoOwnerTransfer]()
    val waitObservation = new AtomicReference[Throwable]()
    val ownerFailure = new AtomicReference[Throwable]()
    val targetClaimed = new AtomicBoolean(false)
    val targetDone = new CountDownLatch(1)
    val target = new Thread(() => {
      try
        io.claimOwnerTransfer(receiptRef.get())
        targetClaimed.set(true)
        try
          io.awaitWorkerTerminationOffMain(1L, TimeUnit.SECONDS)
          waitObservation.set(new AssertionError("transferred Main was allowed to wait"))
        catch case cause: Throwable => waitObservation.set(cause)
        io.quiesce()
      catch case cause: Throwable => ownerFailure.set(cause)
      finally targetDone.countDown()
    }, "termination-target-main")
    try
      receiptRef.set(io.issueOwnerTransfer(target, tag()))
      target.start()
      waitFor(targetDone)
      join(target)
      assertNull(ownerFailure.get(), "transfer claim and quiesce must succeed independently of the wait guard")
      assertTrue(waitObservation.get().isInstanceOf[IllegalArgumentException], "exact transferred Main must be rejected before waiting")
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
    finally
      if target.getState != Thread.State.NEW then join(target)
      if !targetClaimed.get() then
        val receipt = receiptRef.get()
        if receipt != null && (target.getState == Thread.State.NEW || target.getState == Thread.State.TERMINATED) then
          io.abortOwnerTransfer(receipt, new IllegalStateException("test target did not claim ownership"))
        io.quiesce()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))

  @Test def proofIsBoundToOneActualOwnedSerialIoInstance(): Unit =
    val main = Thread.currentThread()
    val first = new OwnedSerialIo[Int, Int](1, main, "termination-proof-first")(identity)
    val second = new OwnedSerialIo[Int, Int](1, main, "termination-proof-second")(identity)
    try
      first.quiesce()
      second.quiesce()
      val proof = requireProof(awaitOffMain(first, 5L, TimeUnit.SECONDS))
      first.requireWorkerTerminationProof(proof)
      assertThrows(classOf[IllegalArgumentException], () => {
        second.requireWorkerTerminationProof(proof)
        ()
      })
    finally
      finish(first)
      finish(second)

  @Test def interruptionReturnsNoProofAndRestoresTheWaiterInterruptFlag(): Unit =
    val hookEntered = new CountDownLatch(1)
    val hookRelease = new CountDownLatch(1)
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-interrupted-hook",
      Some(() => {
        hookEntered.countDown()
        assertTrue(hookRelease.await(5, TimeUnit.SECONDS), "test exit-hook release timed out")
      }))(identity)
    val result = new AtomicReference[IoWorkerTerminationWait]()
    val interrupted = new AtomicReference[java.lang.Boolean]()
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      Thread.currentThread().interrupt()
      try result.set(io.awaitWorkerTerminationOffMain(5L, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      finally interrupted.set(Thread.currentThread().isInterrupted)
    }, "termination-interrupted-waiter")
    try
      io.quiesce()
      waitFor(hookEntered)
      waiter.start()
      join(waiter)
      if failure.get() != null then throw failure.get()
      result.get() match
        case IoWorkerTerminationWait.Interrupted(cause) =>
          assertNotNull(cause)
          assertTrue(cause.getStackTrace.nonEmpty)
        case other => fail(s"interruption must not yield a worker proof: $other")
      assertEquals(java.lang.Boolean.TRUE, interrupted.get())
      hookRelease.countDown()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
    finally
      hookRelease.countDown()
      if waiter.getState != Thread.State.NEW then join(waiter)
      finish(io)

  @Test def preInterruptedObserverCannotReceiveProofForAnAlreadyTerminatedWorker(): Unit =
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-preinterrupted-observer")(identity)
    try
      io.quiesce()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
      val (interruptedResult, interruptFlagRestored) = awaitPreInterruptedOffMain(io, 0L, TimeUnit.NANOSECONDS)
      interruptedResult match
        case IoWorkerTerminationWait.Interrupted(cause) =>
          assertNotNull(cause)
          assertTrue(cause.getStackTrace.nonEmpty)
        case other => fail(s"pre-interrupted observer must not receive a termination proof: $other")
      assertTrue(interruptFlagRestored, "pre-existing caller interrupt must remain set")
      requireProof(awaitOffMain(io, 0L, TimeUnit.NANOSECONDS))
    finally
      finish(io)

  @Test def blockedSelectedExecuteCannotYieldProofAndItsCompletionSurvivesQuiesce(): Unit =
    val executeEntered = new CountDownLatch(1)
    val executeRelease = new CountDownLatch(1)
    val exitHookCalls = new AtomicInteger()
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-blocked-execute",
      Some(() => { exitHookCalls.incrementAndGet(); () }))(value => {
        executeEntered.countDown()
        assertTrue(executeRelease.await(5, TimeUnit.SECONDS), "test execute release timed out")
        value * 10
      })
    val operation = tag()
    try
      assertEquals(IoSubmission.Accepted, io.submit(operation, 7))
      waitFor(executeEntered)
      io.quiesce()
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 0L, TimeUnit.NANOSECONDS))
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 15L, TimeUnit.MILLISECONDS))
      assertTrue(io.checkpoint.running, "selected execute remains a live worker obligation")
      assertEquals(1, io.checkpoint.reserved)
      assertEquals(0, exitHookCalls.get())
      executeRelease.countDown()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
      assertEquals(1, exitHookCalls.get(), "worker cleanup runs after the selected execute completes")
      assertEquals(Vector(IoCompletion(operation, Success(70))), io.drain(1))
      assertEquals(0, io.checkpoint.reserved)
      assertEquals(Some(IoWorkerExitHookObservation.HookReturned), io.checkpoint.workerExitHook)
    finally
      executeRelease.countDown()
      finish(io)

  @Test def blockedSelectedMaintenanceCannotYieldProofAndKeepsCompletionAndCleanupObligations(): Unit =
    val maintenanceEntered = new CountDownLatch(1)
    val maintenanceRelease = new CountDownLatch(1)
    val maintenanceCalls = new AtomicInteger()
    val exitHookCalls = new AtomicInteger()
    val maintenance = new IoWorkerMaintenance:
      def maintain(): IoMaintenanceDeadline =
        maintenanceCalls.incrementAndGet()
        maintenanceEntered.countDown()
        assertTrue(maintenanceRelease.await(5, TimeUnit.SECONDS), "test maintenance release timed out")
        IoMaintenanceDeadline.after(Duration.ofMillis(10))
    var io: OwnedSerialIo[Int, Int] = null
    io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "termination-blocked-maintenance",
      Some(() => { exitHookCalls.incrementAndGet(); () }), None, Some(maintenance))(value => {
        io.scheduleMaintenanceAfter(Duration.ofMillis(1))
        value * 2
      })
    val operation = tag()
    try
      assertEquals(IoSubmission.Accepted, io.submit(operation, 9))
      waitFor(maintenanceEntered)
      assertEquals(1, io.checkpoint.completions, "execute completion remains retained while maintenance is selected")
      assertTrue(io.checkpoint.maintenanceRunning)
      io.quiesce()
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 0L, TimeUnit.NANOSECONDS))
      assertEquals(IoWorkerTerminationWait.TimedOut, awaitOffMain(io, 15L, TimeUnit.MILLISECONDS))
      assertEquals(1, io.checkpoint.reserved, "queued completion ownership remains reserved during maintenance")
      maintenanceRelease.countDown()
      requireProof(awaitOffMain(io, 5L, TimeUnit.SECONDS))
      assertEquals(1, maintenanceCalls.get(), "the selected maintenance callback completes exactly once")
      assertEquals(1, exitHookCalls.get(), "exit cleanup follows the selected maintenance callback")
      assertEquals(Vector(IoCompletion(operation, Success(18))), io.drain(1))
      assertEquals(0, io.checkpoint.reserved)
      assertEquals(Some(IoWorkerExitHookObservation.HookReturned), io.checkpoint.workerExitHook)
    finally
      maintenanceRelease.countDown()
      finish(io)

