package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement

import java.sql.{SQLNonTransientConnectionException, SQLTransactionRollbackException}
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable

final class MySqlBusinessUnknownReconcilerTest:
  @Test
  def aLegacyUnknownWithoutCapturedPlanCannotBeReconciled(): Unit =
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireFrozenResponsibility(None, None)
    )

  @Test
  def aCapturedPlanWithoutItsOriginalAdmissionCannotBeReconciled(): Unit =
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(row(lastUpdated = 1700000000001L))))
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireFrozenResponsibility(Some(plan), None)
    )

  @Test
  def completePodAndEntitlementRowsProveTheExactDesiredEffects(): Unit =
    val pod = row(lastUpdated = 1700000000001L)
    val entitlement = PlayerEntitlement(uuid("00000000-0000-0000-0000-000000000010"), 6, 2)
    val plan = MySqlBusinessEffectPlan(Vector(
      MySqlBusinessMutation.InsertInactivePod(pod),
      MySqlBusinessMutation.ChangeEntitlement(None, Some(entitlement))
    ))
    val readback = MySqlBusinessUnknownEffectReadback(
      podRows = Map(uuidOf(pod) -> Some(pod.copy())),
      entitlementRows = Map(entitlement.playerUuid -> Some(entitlement.copy())),
      deletedPodReferences = Map.empty
    )

    assertDoesNotThrow((() => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, readback)): Executable)

  @Test
  def partialOrDifferentPodRowsCannotProveTheFrozenEffect(): Unit =
    val pod = row(lastUpdated = 1700000000001L)
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(pod)))
    val missingTarget = MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty)
    val changedTarget = MySqlBusinessUnknownEffectReadback(
      Map(uuidOf(pod) -> Some(pod.copy(worldName = "different-world"))),
      Map.empty,
      Map.empty
    )

    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, missingTarget))
    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, changedTarget))

  @Test
  def entitlementQuotaReadbackMustMatchBothStoredLimits(): Unit =
    val before = PlayerEntitlement(uuid("00000000-0000-0000-0000-000000000020"), 4, 2)
    val desired = before.copy(registrationLimit = 8, simultaneousActiveLimit = 3)
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.ChangeEntitlement(Some(before), Some(desired))))
    val partialQuota = MySqlBusinessUnknownEffectReadback(
      Map.empty,
      Map(before.playerUuid -> Some(desired.copy(simultaneousActiveLimit = 2))),
      Map.empty
    )

    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, partialQuota))

  @Test
  def deletedPodRequiresAbsenceFromDomainLocatorChunksAndIntent(): Unit =
    val pod = row(lastUpdated = 1700000000001L)
    val uuid = uuidOf(pod)
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.DeleteInactivePod(pod)))
    val fullyAbsent = MySqlBusinessUnknownEffectReadback(
      Map(uuid -> None),
      Map.empty,
      Map(uuid -> MySqlBusinessUnknownDeleteReferences(Vector.empty, Vector.empty, Vector.empty))
    )
    val lingeringIntent = fullyAbsent.copy(
      deletedPodReferences = Map(uuid -> MySqlBusinessUnknownDeleteReferences(Vector.empty, Vector.empty, Vector(uuid)))
    )

    assertDoesNotThrow((() => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, fullyAbsent)): Executable)
    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, lingeringIntent))

  @Test
  def rawDoubleDifferenceCannotBeRoundedIntoARecoveryProof(): Unit =
    val desired = row(posX = 0.0d, lastUpdated = 1700000000001L)
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(desired)))
    val differentBits = MySqlBusinessUnknownEffectReadback(
      Map(uuidOf(desired) -> Some(desired.copy(posX = -0.0d))),
      Map.empty,
      Map.empty
    )

    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlBusinessUnknownProofRules.requireCompleteEffects(plan, differentBits))

  @Test
  def onlyTheExactCurrentWriterSlotAndUnexpiredLeaseCanSupportReconciliation(): Unit =
    val boot = uuid("00000000-0000-0000-0000-000000000031")
    val binding = uuid("00000000-0000-0000-0000-000000000032")
    val operation = uuid("00000000-0000-0000-0000-000000000033")
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(row(lastUpdated = 1700000000001L))))
    val context = plan.operationContext(boot, 7L, binding, operation)
    val databaseNow = LocalDateTime.parse("2026-10-07T01:00:00")
    val leaseUntil = databaseNow.plusMinutes(2)
    val exact = MySqlBusinessWriterRow(
      binding,
      7L,
      Some(boot),
      leaseUntil,
      Some(operation),
      Some(context.canonicalFingerprintSha256),
      databaseNow
    )

    assertDoesNotThrow((() => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, binding, boot, 7L, leaseUntil, exact)): Executable)
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, binding, boot, 7L, leaseUntil, exact.copy(lastOperationUuid = None, lastFingerprint = None))
    )
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(
        context,
        binding,
        boot,
        7L,
        leaseUntil,
        exact.copy(lastOperationUuid = Some(uuid("00000000-0000-0000-0000-000000000034")))
      )
    )
    val conflictingFingerprint = exact.lastFingerprint.get.updated(0, (exact.lastFingerprint.get.head ^ 1).toByte)
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, binding, boot, 7L, leaseUntil, exact.copy(lastFingerprint = Some(conflictingFingerprint)))
    )
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, binding, boot, 8L, leaseUntil, exact)
    )
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, uuid("00000000-0000-0000-0000-000000000035"), boot, 7L, leaseUntil, exact)
    )
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(
        context,
        binding,
        uuid("00000000-0000-0000-0000-000000000036"),
        7L,
        leaseUntil,
        exact
      )
    )
    assertThrows(
      classOf[SQLNonTransientConnectionException],
      () => MySqlBusinessUnknownProofRules.requireExactWriterReceipt(context, binding, boot, 7L, leaseUntil, exact.copy(databaseNow = leaseUntil))
    )

  private def row(
      lastUpdated: Long,
      posX: Double = 10.0d
  ): MySqlStoredRowData =
    MySqlStoredRowData(
      uuidBytes = MySqlStoredRows.encodeUuid(uuid("00000000-0000-0000-0000-0000000000aa")),
      ownerUuidBytes = None,
      stateVersion = 0L,
      worldName = "world",
      posX = posX,
      posY = 64.0d,
      posZ = -3.0d,
      yaw = 90.0d,
      pitch = 0.0d,
      isActiveFlag = 0,
      lastUpdated = lastUpdated,
      dimensionId = Some(0),
      operatingModeToken = "NORMAL"
    )

  private def uuidOf(row: MySqlStoredRowData): UUID = MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid")
  private def uuid(value: String): UUID = UUID.fromString(value)
