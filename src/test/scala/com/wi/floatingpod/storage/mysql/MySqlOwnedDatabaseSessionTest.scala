package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

/** 直接测试 production actor gate 与不可变责任观察；不假造 JDBC open/admission。 */
class MySqlOwnedDatabaseSessionTest:
  @Test def restrictedRecallLifecycleNeverAdmitsOrReleasesBusinessByOpeningOnly(): Unit =
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    gate.beginOpen()
    gate.beginRecallRecovery()
    gate.requireRecallRecovering()
    assertThrows(classOf[IllegalArgumentException], () => gate.admit())
    assertThrows(classOf[IllegalArgumentException], () => gate.requireAdmitted())
    assertThrows(classOf[IllegalArgumentException], () => gate.beginRecallRecovery())
    gate.beginClose(None)
    assertFalse(gate.releaseEligible)
    assertThrows(classOf[IllegalArgumentException], () => gate.requireRecallRecovering())
    gate.finishClose()

  @Test def restrictedRecallLifecycleRetainsOriginalFailure(): Unit =
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    gate.beginOpen()
    gate.beginRecallRecovery()
    val original = new IllegalStateException("actual recovery failure")
    gate.markFailed(original)
    assertSame(original, assertThrows(classOf[IllegalStateException], () => gate.requireRecallRecovering()))
    assertSame(original, gate.failure.get)
    assertThrows(classOf[IllegalArgumentException], () => gate.admit())
  private def withDroppedRunner(seed: Long)(body: MySqlControlledDebugRunnerFixture => Unit): Unit =
    val runner = new MySqlControlledDebugRunnerFixture(seed,
      com.wi.floatingpod.domain.FloatingPodPose("publication-world", 8.5D, 64D, 8.5D, 0F, 0F, 7))
    var primary: Throwable = null
    try
      runner.dropMainReferencesAndAck()
      body(runner)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try runner.shutdownObservation()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def assertFailedExit(runner: MySqlControlledDebugRunnerFixture,
      checkpoint: com.wi.floatingpod.server.io.IoCheckpoint,
      observation: MySqlOwnedSessionCloseObservation, primary: Throwable): Unit =
    assertSame(primary, observation.failure.get)
    assertSame(primary, checkpoint.exitFailure.get)
    checkpoint.workerExitHook match
      case Some(com.wi.floatingpod.server.io.IoWorkerExitHookObservation.HookFailed(cause)) => assertSame(primary, cause)
      case other => fail("actual close failure must reach worker HookFailed: " + other)
    assertTrue(observation.jdbcCloseAttempted)
    assertEquals(1, runner.closeCalls)
    assertEquals(1, runner.getterCallsAfterClose)
    assertTrue(primary.getStackTrace.nonEmpty)

  private def assertWriterAlreadyReleased(observation: MySqlOwnedSessionCloseObservation): Unit =
    assertEquals(MySqlOwnedSessionRelease.Released, observation.release)
    assertTrue(observation.releaseAttempted)
    assertTrue(observation.releaseReturned)

  @Test def actualCloseExceptionStillReadsTrueGetterAndFailsWorkerExit(): Unit = withDroppedRunner(610L) { runner =>
    val close = new java.sql.SQLException("actual close failure")
    runner.armCloseFaults(Some(close), Some(true), None)
    val (checkpoint, observation) = runner.shutdownObservation()
    assertFailedExit(runner, checkpoint, observation, close)
    assertTrue(observation.jdbcClosed.contains(true))
    assertWriterAlreadyReleased(observation)
    assertTrue(close.getSuppressed.isEmpty)
  }

  @Test def actualFalseGetterFailsDespiteSuccessfulCloseAndWriterRelease(): Unit = withDroppedRunner(620L) { runner =>
    runner.armCloseFaults(None, Some(false), None)
    val (checkpoint, observation) = runner.shutdownObservation()
    val primary = observation.failure.get
    assertTrue(primary.isInstanceOf[IllegalStateException])
    assertEquals("owned JDBC isClosed observed false after close attempt", primary.getMessage)
    assertFailedExit(runner, checkpoint, observation, primary)
    assertTrue(observation.jdbcClosed.contains(false))
    assertTrue(observation.unresolvedOwnedResponsibility)
    assertWriterAlreadyReleased(observation)
  }

  @Test def actualGetterExceptionRecordsUnknownJdbcClosedAndOriginalFailure(): Unit = withDroppedRunner(630L) { runner =>
    val getter = new java.sql.SQLException("actual isClosed failure")
    runner.armCloseFaults(None, None, Some(getter))
    val (checkpoint, observation) = runner.shutdownObservation()
    assertFailedExit(runner, checkpoint, observation, getter)
    assertEquals(None, observation.jdbcClosed)
    assertWriterAlreadyReleased(observation)
  }

  @Test def actualCloseAndGetterExceptionsRetainExactPrimaryAndOneSuppressed(): Unit = withDroppedRunner(640L) { runner =>
    val close = new java.sql.SQLException("close original")
    val getter = new java.sql.SQLException("getter original")
    runner.armCloseFaults(Some(close), None, Some(getter))
    val (checkpoint, observation) = runner.shutdownObservation()
    assertFailedExit(runner, checkpoint, observation, close)
    assertEquals(Vector(getter), close.getSuppressed.toVector)
    assertEquals(None, observation.jdbcClosed)
    assertWriterAlreadyReleased(observation)
  }

  @Test def actualWorkerFatalKeepsEarlierSessionAndCloseFailuresWithoutWriterRelease(): Unit = withDroppedRunner(650L) { runner =>
    val earlier = new IllegalStateException("actual earlier session action")
    val fatal = new LinkageError("actual accepted worker fatal")
    val close = new java.sql.SQLException("fatal path close failure")
    val getter = new java.sql.SQLException("fatal path getter failure")
    runner.failActualSessionAction(earlier)
    runner.armCloseFaults(Some(close), None, Some(getter))
    assertEquals(com.wi.floatingpod.server.io.IoSubmission.Accepted, runner.submitFatal(fatal))
    val (checkpoint, observation) = runner.shutdownObservation()
    assertSame(fatal, checkpoint.fatalCause.get)
    assertFailedExit(runner, checkpoint, observation, fatal)
    assertEquals(Vector(earlier, close, getter), fatal.getSuppressed.toVector)
    assertEquals(MySqlOwnedSessionRelease.RetainedFailure, observation.release)
    assertFalse(observation.releaseAttempted)
    assertFalse(observation.releaseReturned)
    assertEquals(None, observation.jdbcClosed)
    assertTrue(observation.unresolvedOwnedResponsibility)
  }

  private def admittedGate(): MySqlOwnedSessionLifecycle =
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    gate.beginOpen()
    gate.admit()
    gate

  @Test def exactOwnerGuardRejectsOtherThreadWithoutConsumingOpen(): Unit =
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    val failure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try
        assertThrows(classOf[IllegalArgumentException], () => gate.beginOpen())
        assertThrows(classOf[IllegalArgumentException], () => gate.beginClose(None))
        assertThrows(classOf[IllegalArgumentException], () => gate.markFailed(new IllegalStateException("wrong actor")))
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "wrong-owned-session-actor")
    var primary: Throwable = null
    try
      wrong.start()
      wrong.join(5000)
      assertFalse(wrong.isAlive, "bounded wrong actor join timed out")
      if failure.get() != null then throw failure.get()
      gate.beginOpen()
      gate.admit()
      gate.requireAdmitted()
    catch case cause: Throwable => primary = cause
    finally
      try
        wrong.join(5000)
        assertFalse(wrong.isAlive, "wrong actor thread leaked")
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  @Test def lifecycleOpenAndShutdownAreOneShotAndNeverRearmAdmission(): Unit =
    val gate = admittedGate()
    assertThrows(classOf[IllegalArgumentException], () => gate.beginOpen())
    gate.requireAdmitted()
    gate.beginClose(None)
    assertTrue(gate.releaseEligible) // actor policy only; holder still requires real coordinator gate/SQL.
    assertThrows(classOf[IllegalArgumentException], () => gate.requireAdmitted())
    assertThrows(classOf[IllegalArgumentException], () => gate.beginClose(None))
    gate.finishClose()
    assertThrows(classOf[IllegalArgumentException], () => gate.finishClose())
    assertThrows(classOf[IllegalArgumentException], () => gate.beginOpen())
    assertThrows(classOf[IllegalArgumentException], () => gate.admit())

  @Test def originalFailurePreventsReleaseAndRetainsCleanupSuppressedIdentity(): Unit =
    val gate = admittedGate()
    val original = new IllegalStateException("repository action failure")
    val cleanup = new IllegalStateException("owned cleanup failure")
    gate.markFailed(original)
    assertThrows(classOf[IllegalArgumentException], () => gate.requireAdmitted())
    gate.beginClose(None)
    assertFalse(gate.releaseEligible)
    gate.markFailed(cleanup)
    gate.markFailed(cleanup)
    assertSame(original, gate.failure.get)
    assertEquals(Vector(cleanup), original.getSuppressed.toVector)
    gate.finishClose()

  @Test def workerFatalIsPrimaryEvenWhenEarlierActionFailed(): Unit =
    val gate = admittedGate()
    val earlier = new IllegalStateException("earlier action")
    val fatal = new LinkageError("real worker primary")
    val cleanup = new IllegalStateException("close failure")
    gate.markFailed(earlier)
    gate.beginClose(Some(fatal))
    gate.markFailed(cleanup)
    assertFalse(gate.releaseEligible)
    assertSame(fatal, gate.failure.get)
    assertEquals(Vector(earlier, cleanup), fatal.getSuppressed.toVector)
    gate.markFailed(fatal)
    assertEquals(Vector(earlier, cleanup), fatal.getSuppressed.toVector)
    gate.finishClose()

  @Test def initializationFailureCannotAdmitOrBecomeReleaseEligible(): Unit =
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    gate.beginOpen()
    val cause = new LinkageError("partial initialization original")
    gate.markFailed(cause)
    assertThrows(classOf[IllegalArgumentException], () => gate.admit())
    gate.beginClose(None)
    assertFalse(gate.releaseEligible)
    assertSame(cause, gate.failure.get)
    gate.finishClose()
    assertThrows(classOf[IllegalArgumentException], () => gate.markFailed(new IllegalStateException("late")))

  @Test def closeObservationSeparatesWriterResponsibilityFromActualJdbcGetterFact(): Unit =
    val boot = UUID.randomUUID()
    val failure = new IllegalStateException("frozen unknown")
    val retained = MySqlOwnedSessionCloseObservation(boot, 2, 3, true, true,
      MySqlOwnedSessionRelease.RetainedFailure, false, false, true, Some(true), Some(failure), Some(failure))
    assertTrue(retained.unresolvedOwnedResponsibility)
    assertTrue(retained.jdbcClosed.contains(true))
    assertFalse(retained.releaseAttempted)
    assertSame(failure, retained.frozenFailure.get)
    val getterFalse = retained.copy(release = MySqlOwnedSessionRelease.Released, releaseAttempted = true,
      releaseReturned = true, jdbcClosed = Some(false), frozenFailure = None)
    assertTrue(getterFalse.unresolvedOwnedResponsibility)
    val closedAndReleased = getterFalse.copy(jdbcClosed = Some(true), failure = None)
    assertFalse(closedAndReleased.unresolvedOwnedResponsibility)
    assertEquals("MySqlOwnedSessionCloseObservation[redacted; not IOExitProof]", closedAndReleased.toString)
    val notOpened = retained.copy(connectionOpened = false, acquireAttempted = false,
      release = MySqlOwnedSessionRelease.NoAcquireAttempt, jdbcCloseAttempted = false, jdbcClosed = None)
    assertFalse(notOpened.unresolvedOwnedResponsibility)
    assertTrue(notOpened.failure.nonEmpty) // no resource owned does not erase original open failure.

