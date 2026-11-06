package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** production pure graph验证，synthetic rows由实际unbound转换生成；非SQL integration。 */
class MySqlCommittedUnboundGraphTest:
  @Test def actualTransitionGraphRetainsFullInventoryAndReceipt(): Unit =
    val data = MainPublicationGraphFixture.create()
    val graph = data.graph
    assertEquals(data.receipt, graph.receipt)
    assertEquals(data.state, graph.state)
    assertEquals(data.inventory, graph.inventory)
    assertEquals(25, graph.locator.chunks.size)
    assertEquals(MySqlRecoveryIntentPhase.Committed, graph.intent.phase)

  @Test def rejectsWrongGenerationAndAckState(): Unit =
    val data = MainPublicationGraphFixture.create()
    assertThrows(classOf[IllegalArgumentException], () => data.copy(tag = data.tag.copy(connectionGeneration = 2L)).graph)
    val wrong = data.state.copy(stateVersion = 2L)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(outcome = UnboundCommitOutcome.Committed(wrong)).graph)

  @Test def missingChunkAndDuplicateHistoryCannotIssueGraph(): Unit =
    val data = MainPublicationGraphFixture.create()
    val graph = data.inventory.recovery
    val incomplete = graph.copy(locators = graph.locators.map(v => v.copy(chunks = v.chunks.tail)), chunks = graph.chunks.tail)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(inventory = data.inventory.copy(recovery = incomplete)).graph)
    assertThrows(classOf[java.sql.SQLDataException], () => data.copy(inventory = data.inventory.copy(recovery = graph.copy(intents = graph.intents ++ graph.intents))).graph)

  @Test def observedReceiptAndExpiredWriterAndRejectedAckCannotIssueGraph(): Unit =
    val data = MainPublicationGraphFixture.create()
    val observed = data.receipt.copy(origin = UnboundIntentOrigin.ObservedArtifactAdoption,
      observedIdentity = Some(ObservedUnboundIdentity(ObservedUnboundSourceKind.FreshArtifact, MainPublicationGraphFixture.id(9), "source")), createOperationUuid = None)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(receipt = observed).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(inventory = data.inventory.copy(writerLease = MySqlObservedWriterLease.OwnedExpired)).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(outcome = UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch)).graph)

  @Test def typedAfterAckFailureRetainsOriginalOutcomeAndCause(): Unit =
    val data = MainPublicationGraphFixture.create()
    val original = new IllegalStateException("readback unavailable")
    val value = MySqlUnboundAfterCommitReadbackFailure(data.tag, data.receipt, data.outcome, original)
    assertSame(original, value.cause)
    assertSame(data.outcome, value.actualCommit)
    assertEquals(data.receipt, value.receipt)
    val notice = new MySqlUnboundCommitObligationNotice(data.tag, data.receipt, data.outcome)
    original.addSuppressed(notice)
    assertSame(notice, original.getSuppressed()(0))
    assertNull(notice.getCause)
