package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState

import java.util.UUID
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

final class MySqlBootstrapSnapshotTransitionsTest:
  private val plan = MySqlBootstrapPlanIdentity(
    UUID.fromString("a1000000-0000-4000-8000-000000000001"),
    UUID.fromString("a2000000-0000-4000-8000-000000000002"),
    UUID.fromString("a3000000-0000-4000-8000-000000000003"),
    UUID.fromString("a4000000-0000-4000-8000-000000000004"),
    UUID.fromString("a5000000-0000-4000-8000-000000000005"),
    UUID.fromString("a6000000-0000-4000-8000-000000000006"),
    "fp_bootstrap_fixture",
    3,
    2,
    List.fill(32)("A7").mkString,
    UUID.fromString("a7000000-0000-4000-8000-000000000007")
  )
  private val sourceIdentity = "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=00000000000000000000000000000001"
  private val provisionReceiptSha256 = Vector.fill(32)(0x31.toByte)
  private val markerAckSha256 = Vector.fill(32)(0x42.toByte)

  @Test
  def freshBootstrapRequiresFormat3SchemaWithPhysicalFormat2(): Unit =
    assertEquals(3, plan.schemaFormat)
    assertEquals(2, plan.physicalFormat)
    assertThrows(classOf[IllegalArgumentException], () => plan.copy(schemaFormat = 2))
    assertThrows(classOf[IllegalArgumentException], () => plan.copy(physicalFormat = 3))

  @Test
  def freshBootstrapPersistsPreparedIntentBeforeCertifiedMarkerCompletion(): Unit =
    val prepared = MySqlBootstrapSnapshotTransitions.prepareFresh(
      MySqlFullRepositorySnapshot.empty,
      plan,
      sourceIdentity,
      provisionReceiptSha256
    )
    assertEquals(PhysicalRecoveryCutoverState.LegacyUncertified, prepared.recovery.binding.get.cutoverState)
    assertEquals(1, prepared.bootstrapIntents.size)
    val intent = prepared.bootstrapIntents.head
    assertEquals(plan.bootstrapOperationUuid, intent.bootstrapOperationUuid)
    assertEquals(plan.prepareWriteUuid, intent.prepareWriteUuid)
    assertEquals(plan.certifyWriteUuid, intent.certifyWriteUuid)
    assertEquals(sourceIdentity, intent.sourceWorldIdentity)
    assertEquals(provisionReceiptSha256, intent.provisionReceiptSha256)
    assertEquals(MySqlBootstrapIntentStage.Prepared, intent.stage)
    assertTrue(intent.markerAckSha256.isEmpty)

    val completed = MySqlBootstrapSnapshotTransitions.complete(
      prepared,
      plan,
      sourceIdentity,
      provisionReceiptSha256,
      markerAckSha256
    )
    assertEquals(PhysicalRecoveryCutoverState.Certified, completed.recovery.binding.get.cutoverState)
    assertEquals(MySqlBootstrapIntentStage.Complete, completed.bootstrapIntents.head.stage)
    assertEquals(Some(markerAckSha256), completed.bootstrapIntents.head.markerAckSha256)
    assertEquals(1, completed.bootstrapIntents.size)
    assertTrue(completed.pods.isEmpty)
    assertTrue(completed.entitlements.isEmpty)
    assertTrue(completed.recovery.locators.isEmpty)
    assertTrue(completed.recovery.intents.isEmpty)
    assertTrue(completed.recovery.chunks.isEmpty)
    MySqlBootstrapSnapshotTransitions.requireCompleteMatches(
      completed,
      plan,
      sourceIdentity,
      provisionReceiptSha256,
      markerAckSha256
    )

  @Test
  def bootstrapRejectsAnExistingBusinessOrRecoveryGraphAndChangedProvisionIdentity(): Unit =
    val occupied = MySqlFullRepositorySnapshot.empty.copy(pods = Vector(MySqlStoredRowData(
      MySqlStoredRows.encodeUuid(UUID.fromString("a8000000-0000-4000-8000-000000000008")),
      None,
      0L,
      "world",
      8.0d,
      64.0d,
      8.0d,
      0.0d,
      0.0d,
      0,
      10L,
      Some(7),
      "NORMAL"
    )))
    assertThrows(
      classOf[java.sql.SQLTransactionRollbackException],
      () => MySqlBootstrapSnapshotTransitions.prepareFresh(occupied, plan, sourceIdentity, provisionReceiptSha256)
    )

    val prepared = MySqlBootstrapSnapshotTransitions.prepareFresh(
      MySqlFullRepositorySnapshot.empty,
      plan,
      sourceIdentity,
      provisionReceiptSha256
    )
    assertThrows(
      classOf[java.sql.SQLTransactionRollbackException],
      () => MySqlBootstrapSnapshotTransitions.requirePreparedMatches(
        prepared,
        plan,
        sourceIdentity + "/changed",
        provisionReceiptSha256
      )
    )
    assertThrows(
      classOf[java.sql.SQLTransactionRollbackException],
      () => MySqlBootstrapSnapshotTransitions.requirePreparedMatches(
        prepared,
        plan,
        sourceIdentity,
        Vector.fill(32)(0x32.toByte)
      )
    )

  @Test
  def completedBootstrapRejectsAChangedMarkerAckDigest(): Unit =
    val prepared = MySqlBootstrapSnapshotTransitions.prepareFresh(
      MySqlFullRepositorySnapshot.empty,
      plan,
      sourceIdentity,
      provisionReceiptSha256
    )
    val completed = MySqlBootstrapSnapshotTransitions.complete(
      prepared,
      plan,
      sourceIdentity,
      provisionReceiptSha256,
      markerAckSha256
    )
    assertThrows(
      classOf[java.sql.SQLTransactionRollbackException],
      () => MySqlBootstrapSnapshotTransitions.requireCompleteMatches(
        completed,
        plan,
        sourceIdentity,
        provisionReceiptSha256,
        Vector.fill(32)(0x43.toByte)
      )
    )
