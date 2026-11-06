package com.wi.floatingpod.server

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.debug.DebugItemEndpoint
import com.wi.floatingpod.server.io.{IoOperationTag, IoCompletion, IoSubmission}
import com.wi.floatingpod.storage.mysql.*
import java.util.UUID
import java.sql.SQLDataException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.util.{Success, Failure}

/** 真实Main ledger纯carrier责任；没有Ready/游戏/JDBC成功seam。 */
class MainDebugPlanCarrierTest:
  private def id(n: Long): UUID = new UUID(0L, n)
  private val identity = MySqlOwnedIoIdentity(id(1), 1L, 1L)
  private def limits(retained: Long = 1048576L): SourceLimitProfile =
    SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, retained, 1048576L, 1048576L)
  private def request: DebugItemEndpoint.Request = new DebugItemEndpoint.Request(
    new DebugItemEndpoint.Identity(id(1), id(2), id(3), id(4), "carrier-world", 7, -1, null, null),
    1L, id(10), DebugItemEndpoint.Action.PLACE, new DebugItemEndpoint.Position(8.5, 64, 8.5, 0F),
    new DebugItemEndpoint.PlacementContext(8, 63, 8, DebugItemEndpoint.PlacementFace.UP))
  private def tag(n: Long): IoOperationTag = IoOperationTag(id(1), 1L, 1L, id(n))
  private def completion(tag: IoOperationTag): IoCompletion[MySqlOwnedIoReply] =
    IoCompletion(tag, Success(MySqlOwnedIoReply.Transaction(Vector.empty)))

  @Test def stagePlansUseOneSeparateWorkflowAndRemainUntilActualFeedbackCompaction(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits())
    val entry = ledger.reserve(request).get
    val workflow = id(20)
    val operation = MySqlRepositoryOperation.FindAll
    val first = ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, workflow, tag(21), id(22), operation)
    assertNotEquals(entry.request.operation(), workflow)
    ledger.requirePlannedSubmission(entry, tag(21), MainDebugStage.Preparing)
    ledger.submitted(entry, tag(21), IoSubmission.Accepted, MainDebugStage.Preparing)
    ledger.retainBatch(Vector(completion(tag(21)))); ledger.prepared(entry)
    val second = ledger.reserveRepositoryPlan(entry, MainDebugStage.Committing, workflow, tag(23), id(24), operation)
    assertSame(first, entry.planSnapshot.head.plan.get)
    assertSame(second, entry.planSnapshot(1).plan.get)
    assertEquals(Vector(workflow, workflow), entry.planSnapshot.map(_.workflowUuid))
    assertEquals(4, entry.planSnapshot.flatMap(value => Vector(value.tag.operationId, value.reserveOperationUuid)).distinct.size)
    ledger.requirePlannedSubmission(entry, tag(23), MainDebugStage.Committing)
    ledger.submitted(entry, tag(23), IoSubmission.Accepted, MainDebugStage.Committing)
    ledger.retainBatch(Vector(completion(tag(23))))
    ledger.published(entry)
    assertEquals(1, ledger.retainedCount); assertEquals(2, entry.planSnapshot.size)
    ledger.acknowledged(entry); assertEquals(0, ledger.retainedCount)

  @Test def failedOriginalPlanRetainsMintedIdsOperationAndCannotRemintOrSubmit(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits(1L))
    val entry = ledger.reserve(request).get
    val operation = MySqlRepositoryOperation.FindByUuid(id(30))
    val failure = assertThrows(classOf[SQLDataException], () => {
      ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, id(20), tag(21), id(22), operation)
      ()
    })
    val original = entry.planSnapshot.head
    assertEquals(id(20), original.workflowUuid); assertEquals(tag(21), original.tag)
    assertEquals(id(22), original.reserveOperationUuid); assertSame(operation, original.operation)
    assertTrue(original.plan.isEmpty)
    assertThrows(classOf[IllegalStateException], () => ledger.requirePlannedSubmission(entry, tag(21), MainDebugStage.Preparing))
    assertThrows(classOf[IllegalArgumentException], () => {
      ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, id(31), tag(32), id(33), operation)
      ()
    })
    ledger.fail(entry, failure); assertSame(failure, entry.cause.get)
    assertEquals(Vector(original), entry.planSnapshot); assertEquals(1, ledger.retainedCount)

  @Test def capacityFailureAndRetirementKeepExactPlanAndCompletionResponsibility(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits())
    val entry = ledger.reserve(request).get
    val first = ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, id(20), tag(21), id(22), MySqlRepositoryOperation.FindAll)
    ledger.submitted(entry, tag(21), IoSubmission.Accepted, MainDebugStage.Preparing)
    val actual = completion(tag(21));ledger.retainBatch(Vector(actual));ledger.prepared(entry)
    val second = ledger.reserveRepositoryPlan(entry, MainDebugStage.Committing, id(20), tag(23), id(24), MySqlRepositoryOperation.FindAll)
    assertFalse(ledger.submitted(entry, tag(23), IoSubmission.RejectedCapacity, MainDebugStage.Committing))
    ledger.retire()
    assertSame(actual, entry.completionSnapshot.head)
    assertEquals(Vector(first, second), entry.planSnapshot.map(_.plan.get))
    assertEquals(MainDebugStage.Retired, entry.stage);assertEquals(1, ledger.retainedCount)
    assertThrows(classOf[IllegalArgumentException], () => ledger.releaseNotAccepted(entry))

  @Test def staleCompletionAndWorkflowReplacementCannotEraseOrReplaceOriginalStage(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits())
    val entry = ledger.reserve(request).get
    val first = ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, id(20), tag(21), id(22), MySqlRepositoryOperation.FindAll)
    ledger.submitted(entry, tag(21), IoSubmission.Accepted, MainDebugStage.Preparing)
    ledger.retainBatch(Vector(completion(tag(21))));ledger.prepared(entry)
    assertThrows(classOf[IllegalArgumentException], () => {
      ledger.reserveRepositoryPlan(entry, MainDebugStage.Committing, id(50), tag(23), id(24), MySqlRepositoryOperation.FindAll)
      ()
    })
    assertSame(first, entry.planSnapshot.head.plan.get)
    val stale = IoCompletion[MySqlOwnedIoReply](tag(21).copy(connectionGeneration = 2L), Failure(new IllegalStateException("late old generation")))
    ledger.retainBatch(Vector(stale))
    assertSame(stale, ledger.unmatchedCompletions.head)
    assertSame(first, entry.planSnapshot.head.plan.get);assertTrue(ledger.isRetired)

  @Test def foreignCompletionAfterPublishedRevokesAckAndRetainsAllOriginalResponsibility(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits())
    val entry = ledger.reserve(request).get
    val first = ledger.reserveRepositoryPlan(entry, MainDebugStage.Preparing, id(20), tag(21), id(22), MySqlRepositoryOperation.FindAll)
    ledger.submitted(entry, tag(21), IoSubmission.Accepted, MainDebugStage.Preparing)
    val prepared = completion(tag(21))
    ledger.retainBatch(Vector(prepared)); ledger.prepared(entry)
    val second = ledger.reserveRepositoryPlan(entry, MainDebugStage.Committing, id(20), tag(23), id(24), MySqlRepositoryOperation.FindAll)
    ledger.submitted(entry, tag(23), IoSubmission.Accepted, MainDebugStage.Committing)
    val committed = completion(tag(23))
    ledger.retainBatch(Vector(committed)); ledger.published(entry)
    assertEquals(MainDebugStage.Published, entry.stage)
    val originalFailure = new IllegalStateException("foreign late completion original failure")
    val foreign = IoCompletion[MySqlOwnedIoReply](tag(90), Failure(originalFailure))
    ledger.retainBatch(Vector(foreign))
    assertThrows(classOf[IllegalArgumentException], () => ledger.acknowledged(entry))
    assertTrue(ledger.isRetired); assertEquals(MainDebugStage.Retired, entry.stage)
    assertEquals(1, ledger.retainedCount)
    assertEquals(Vector(prepared, committed), entry.completionSnapshot)
    assertSame(first, entry.planSnapshot.head.plan.get); assertSame(second, entry.planSnapshot(1).plan.get)
    assertSame(foreign, ledger.unmatchedCompletions.head)
    assertSame(originalFailure, ledger.unmatchedCompletions.head.result.failed.get)
    ledger.fail(entry, originalFailure)
    assertSame(originalFailure, entry.cause.get)
    assertThrows(classOf[IllegalArgumentException], () => ledger.acknowledged(entry))
    assertEquals(1, ledger.retainedCount); assertEquals(2, entry.planSnapshot.size)
