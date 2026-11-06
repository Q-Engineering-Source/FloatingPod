package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** direct actual production graph validation；pure fixtures，不执行SQL。 */
class MySqlPreparedUnboundGraphTest:
  import MySqlPrepareModeGraphFixture.*

  @Test def actualPreparedGraphRetainsAllRawRowsAndExactReceipt(): Unit =
    val data = prepared()
    val graph = data.graph
    assertTrue(graph.isInstanceOf[MySqlPreparedUnboundGraph.PreparedForPhysical])
    assertSame(data.receipt, graph.receipt)
    assertFalse(graph.state.isActive)
    assertEquals(data.receipt.preparedVersion, graph.state.stateVersion)
    assertEquals(25, graph.locator.chunks.size)
    assertEquals(2, graph.inventory.pods.size)
    assertEquals(data.snapshot.entitlements, graph.inventory.entitlements)

  @Test def actualCommittedPrepareReplayIsDistinctWithoutNewPhysicalPermission(): Unit =
    val data = committedPreparation()
    val graph = data.graph
    assertTrue(graph.isInstanceOf[MySqlPreparedUnboundGraph.ExistingCommitted])
    assertFalse(graph.isInstanceOf[MySqlPreparedUnboundGraph.PreparedForPhysical])
    assertTrue(graph.state.isActive)
    assertEquals(MySqlRecoveryIntentPhase.Committed, graph.intent.phase)
    assertEquals(data.receipt, graph.receipt)
    assertNotEquals(data.request.proposedAttemptUuid, graph.receipt.attemptUuid)

  @Test def receiptUuidIsActualButRepositoryDifferentProposalStillConflicts(): Unit =
    val data = prepared()
    val changedRequest = data.request.copy(proposedPodUuid = id(100), proposedAttemptUuid = id(101))
    val graph = data.copy(request = changedRequest).graph // producer reads actual ACK; not fake repo success.
    assertEquals(data.receipt.podUuid, graph.state.uuid)
    assertNotEquals(changedRequest.proposedPodUuid, graph.state.uuid)
    assertThrows(classOf[IllegalStateException], () => MySqlUnboundTransitionRules.prepareCreate(data.snapshot,
      binding, changedRequest.proposedPodUuid, changedRequest.proposedAttemptUuid, pose, changedRequest.createOperationUuid))

  @Test def wrongOperationPoseAndObservedReceiptCannotIssueGraph(): Unit =
    val data = prepared()
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(createOperationUuid = id(200))).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(targetPose = pose.copy(posX = 9D))).graph)
    val observed = data.receipt.copy(origin = UnboundIntentOrigin.ObservedArtifactAdoption,
      observedIdentity = Some(ObservedUnboundIdentity(ObservedUnboundSourceKind.FreshArtifact, id(300), "original")), createOperationUuid = None)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(receipt = observed).graph)

  @Test def foreignGenerationExpiredWriterAndWrongManifestCannotIssueGraph(): Unit =
    val data = prepared(); val raw = inventory(data.snapshot)
    assertThrows(classOf[IllegalArgumentException], () => MySqlPreparedUnboundGraph.fromReadback(tag.copy(connectionGeneration = 2L),
      data.request, data.receipt, raw, admission, 256))
    assertThrows(classOf[IllegalArgumentException], () => MySqlPreparedUnboundGraph.fromReadback(tag, data.request, data.receipt,
      raw.copy(writerLease = MySqlObservedWriterLease.OwnedExpired), admission, 256))
    assertThrows(classOf[IllegalArgumentException], () => MySqlPreparedUnboundGraph.fromReadback(tag, data.request, data.receipt,
      raw.copy(manifest = raw.manifest.copy(bindingUuid = id(999))), admission, 256))

  @Test def completeGraphValidatesUnrelatedRowsAndExactChunkInventory(): Unit =
    val data = prepared()
    val corrupt = data.snapshot.copy(pods = data.snapshot.pods.map(row =>
      if row.uuidBytes == MySqlStoredRows.encodeUuid(id(99)) then row.copy(isActiveFlag = 9) else row))
    assertThrows(classOf[IllegalStateException], () => data.copy(snapshot = corrupt).graph)
    val graph = data.snapshot.recovery
    val missing = graph.copy(locators = graph.locators.map(value => value.copy(chunks = value.chunks.tail)))
    assertThrows(classOf[java.sql.SQLDataException], () => data.copy(snapshot = data.snapshot.copy(recovery = missing)).graph)

  @Test def committedHistoryAfterActualRecallRemainsObservationNotPrepared(): Unit =
    val data = committedPreparation()
    val current = MySqlStoredRows.materializeGenericState(data.snapshot.pods.find(_.uuidBytes == MySqlStoredRows.encodeUuid(data.receipt.podUuid)).get)
    val (inactive, _) = MySqlRepositoryTransitionRules.deactivate(data.snapshot, current.uuid, current.ownerUuid,
      current.stateVersion, Some(data.receipt.attemptUuid))
    val graph = data.copy(snapshot = inactive).graph
    assertTrue(graph.isInstanceOf[MySqlPreparedUnboundGraph.ExistingCommitted])
    assertFalse(graph.state.isActive)
    assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, graph.locator.state)
