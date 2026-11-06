package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoIdentity, MySqlOwnedIoReply}
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.util.Success

class MainModeProjectionReleaseLedgerTest:
  private def id(value: Long): UUID = new UUID(0L, value)

  @Test def capacityRetryUsesOriginalTagAndSuccessAckIsSingleUse(): Unit =
    val identity = MySqlOwnedIoIdentity(id(1), 3L, 7L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, id(8))
    val ledger = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 2)
    ledger.reserve(tag)
    assertTrue(ledger.readyTags.isEmpty, "a reserved release cannot run while Main may still retain the full20 completion")
    ledger.arm(tag)
    ledger.submitted(tag, IoSubmission.RejectedCapacity)
    assertEquals(Vector(tag), ledger.readyTags, "known capacity rejection can retry the same unaccepted tag")
    ledger.submitted(tag, IoSubmission.Accepted)
    assertTrue(ledger.isSubmitted(tag))
    assertTrue(ledger.readyTags.isEmpty)

    val wrongTag = tag.copy(operationId = id(9))
    val stale = IoCompletion[MySqlOwnedIoReply](wrongTag,
      Success(MySqlOwnedIoReply.ModeProjectionReleased(wrongTag)))
    assertTrue(ledger.consume(stale).isInstanceOf[MainModeProjectionReleaseResult.Unmatched])
    assertTrue(ledger.isSubmitted(tag), "stale tag cannot consume the accepted release")

    val success = IoCompletion[MySqlOwnedIoReply](tag, Success(MySqlOwnedIoReply.ModeProjectionReleased(tag)))
    assertEquals(MainModeProjectionReleaseResult.Released(tag), ledger.consume(success))
    assertEquals(0, ledger.pendingCount)
    assertTrue(ledger.consume(success).isInstanceOf[MainModeProjectionReleaseResult.Unmatched],
      "duplicate release completion is retained as unmatched rather than applied twice")
    assertEquals(Vector(stale, success), ledger.unmatched)

  @Test def failedSubmissionOrWorkerReleaseNeverBecomesAReleaseAck(): Unit =
    val identity = MySqlOwnedIoIdentity(id(11), 2L, 4L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, id(12))
    val ledger = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 2)
    ledger.reserve(tag)
    ledger.arm(tag)
    ledger.submitted(tag, IoSubmission.Accepted)
    val cause = new IllegalStateException("worker could not release retained projection accounting")
    val failed = IoCompletion[MySqlOwnedIoReply](tag,
      Success(MySqlOwnedIoReply.ModeProjectionReleaseFailed(tag, cause)))
    val result = ledger.consume(failed)
    result match
      case MainModeProjectionReleaseResult.Failed(actualTag, actualCause) =>
        assertEquals(tag, actualTag)
        assertSame(cause, actualCause)
      case _ => fail("worker release failure must preserve exact tag and cause")
    assertEquals(1, ledger.pendingCount, "failed release retains its exact tag responsibility")
    assertFalse(ledger.isSubmitted(tag), "failed ACK cannot authorize compaction")
    val duplicateSuccess = IoCompletion[MySqlOwnedIoReply](tag,
      Success(MySqlOwnedIoReply.ModeProjectionReleased(tag)))
    assertTrue(ledger.consume(duplicateSuccess).isInstanceOf[MainModeProjectionReleaseResult.Unmatched])
    assertEquals(1, ledger.pendingCount)

  @Test def releaseLedgerRejectsForeignThreadWithoutChangingItsExactState(): Unit =
    val identity = MySqlOwnedIoIdentity(id(21), 5L, 6L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, id(22))
    val ledger = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 1)
    ledger.reserve(tag)
    ledger.arm(tag)
    var failure: Throwable = null
    val thread = new Thread(() =>
      try { ledger.readyTags; () }
      catch case cause: Throwable => failure = cause)
    thread.start(); thread.join()
    assertTrue(failure.isInstanceOf[IllegalArgumentException])
    assertEquals(Vector(tag), ledger.readyTags)

  @Test def anUnarmedPreparedReleaseCannotBeSubmittedOrConsumed(): Unit =
    val identity = MySqlOwnedIoIdentity(id(31), 2L, 8L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, id(32))
    val ledger = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 1)
    ledger.reserve(tag)
    assertTrue(ledger.readyTags.isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => ledger.submitted(tag, IoSubmission.Accepted))
    val premature = IoCompletion[MySqlOwnedIoReply](tag,
      Success(MySqlOwnedIoReply.ModeProjectionReleased(tag)))
    assertTrue(ledger.consume(premature).isInstanceOf[MainModeProjectionReleaseResult.Unmatched])
    assertEquals(1, ledger.pendingCount, "a premature or stale worker message cannot arm or consume the tag")

  @Test def releaseArmedDuringAnEndWaitsUntilTheNextEndSnapshot(): Unit =
    val identity = MySqlOwnedIoIdentity(id(41), 2L, 9L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, id(42))
    val ledger = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 1)

    val currentEnd = ledger.snapshotReadyAtEndStart()
    ledger.reserve(tag)
    ledger.arm(tag)

    assertTrue(currentEnd.isEmpty, "a graph compacted while this END drains must not release until its stack is gone")
    assertEquals(Vector(tag), ledger.snapshotReadyAtEndStart(), "the next END may submit the previously armed exact tag")
