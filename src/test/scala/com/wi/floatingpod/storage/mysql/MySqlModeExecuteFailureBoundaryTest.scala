package com.wi.floatingpod.storage.mysql

import java.sql.SQLNonTransientConnectionException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlModeExecuteFailureBoundaryTest:
  @Test def acknowledgedReserveAndKnownExecutePreflightFailureRetainTheOriginalCauseWithoutDml(): Unit =
    var phase = MySqlModeWorkflowPhase.ReserveCommittedExecutePending
    var executeDmlCalls = 0
    val preflightFailure = new SQLNonTransientConnectionException("exact command scope expired before execute")

    val blocked = assertThrows(classOf[MySqlModeWorkflowBlockedException], () => {
      MySqlModeExecuteFailureBoundary.afterReserve(primary => MySqlModeExecuteFailureBoundary.shouldRecordKnownFailure(phase == MySqlModeWorkflowPhase.ReserveCommittedExecutePending, primary)) {
        if phase != MySqlModeWorkflowPhase.ReserveCommittedExecutePending then
          throw new IllegalStateException("execute body escaped its retained phase")
        if preflightFailure != null then throw preflightFailure
        executeDmlCalls += 1
      } { primary =>
        assertSame(preflightFailure, primary)
        phase = MySqlModeWorkflowPhase.ExecuteFailedAfterReserve
        new MySqlModeWorkflowBlockedException(phase, primary)
      }
      ()
    })

    assertEquals(MySqlModeWorkflowPhase.ExecuteFailedAfterReserve, phase)
    assertSame(preflightFailure, blocked.getCause)
    assertEquals(0, executeDmlCalls, "known preflight refusal occurs before execute DML")

  @Test def anExistingExecuteCommitUnknownIsNotReclassifiedAsKnownFailure(): Unit =
    val commitCause = new java.sql.SQLException("execute commit acknowledgement was lost")
    val unknown = new MySqlModeWorkflowBlockedException(MySqlModeWorkflowPhase.ExecuteCommitUnknown, commitCause)
    var recorderCalls = 0

    val propagated = assertThrows(classOf[MySqlModeWorkflowBlockedException], () => {
      MySqlModeExecuteFailureBoundary.afterReserve(primary => MySqlModeExecuteFailureBoundary.shouldRecordKnownFailure(retainedExecutePending = true, primary)) {
        throw unknown
      } { _ =>
        recorderCalls += 1
        new MySqlModeWorkflowBlockedException(MySqlModeWorkflowPhase.ExecuteFailedAfterReserve, unknown)
      }
      ()
    })

    assertSame(unknown, propagated)
    assertEquals(MySqlModeWorkflowPhase.ExecuteCommitUnknown, propagated.phase)
    assertSame(commitCause, propagated.getCause)
    assertEquals(0, recorderCalls)

  @Test def failureToRecordKnownPostReserveFailureIsSuppressedOnTheOriginalRefusal(): Unit =
    val preflightFailure = new SQLNonTransientConnectionException("held barrier changed before execute")
    val recordingFailure = new IllegalStateException("responsibility transition could not be stored")

    val propagated = assertThrows(classOf[SQLNonTransientConnectionException], () => {
      MySqlModeExecuteFailureBoundary.afterReserve(primary => MySqlModeExecuteFailureBoundary.shouldRecordKnownFailure(retainedExecutePending = true, primary)) {
        throw preflightFailure
      } { _ =>
        throw recordingFailure
      }
      ()
    })

    assertSame(preflightFailure, propagated)
    assertEquals(1, propagated.getSuppressed.length)
    assertSame(recordingFailure, propagated.getSuppressed()(0))

  @Test def cleanupUncertaintyAfterReserveIsNotRecordedAsAKnownExecuteFailure(): Unit =
    val rootCause = new java.sql.SQLException("execute rollback acknowledgement was lost")
    val context = MySqlOperationContext(
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000101"), 7L,
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000102"),
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000103"),
      Vector(MySqlExpectedEffect("mode:pod-103", 4L, Vector.fill(32)(0.toByte))))
    val uncertain = new MySqlTransactionCleanupUncertainException(context, rootCause)
    var recorderCalls = 0

    val propagated = assertThrows(classOf[MySqlTransactionCleanupUncertainException], () => {
      MySqlModeExecuteFailureBoundary.afterReserve(primary =>
        MySqlModeExecuteFailureBoundary.shouldRecordKnownFailure(retainedExecutePending = true, primary)) {
        throw uncertain
      } { primary =>
        recorderCalls += 1
        new MySqlModeWorkflowBlockedException(MySqlModeWorkflowPhase.ExecuteFailedAfterReserve, primary)
      }
      ()
    })

    assertSame(uncertain, propagated)
    assertSame(rootCause, propagated.getCause)
    assertEquals(0, recorderCalls)