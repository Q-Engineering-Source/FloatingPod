package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID

class MySqlCommitReceiptsTest:
  @Test
  def confirmsOnlyAnExactWriterSlotAndEffectFingerprint(): Unit =
    val expected = context()
    val exact = receipt(expected)

    assertEquals(
      MySqlCommitReceiptVerification.Confirmed(exact),
      MySqlCommitReceipts.verifyReadback(expected, Some(exact))
    )

  @Test
  def missingReceiptIsUnprovenRatherThanRollbackConfirmed(): Unit =
    val result = MySqlCommitReceipts.verifyReadback(context(), None)

    assertEquals(MySqlCommitReceiptVerification.Unproven, result)
    assertFalse(result.isInstanceOf[MySqlCommitReceiptVerification.Confirmed])

  @Test
  def sameSlotWithDifferentEffectsIsAConflict(): Unit =
    val expected = context()
    val changedEffects = context(seed = 12)
    val observed = receipt(changedEffects)

    assertTrue(expected.hasSameOperationIdentity(changedEffects))
    assertEquals(
      MySqlCommitReceiptVerification.Conflict(MySqlCommitReceiptConflict.EffectFingerprintMismatch),
      MySqlCommitReceipts.verifyReadback(expected, Some(observed))
    )

  @Test
  def anotherOperationInTheSameWriterScopeDoesNotProveThisOperationRolledBack(): Unit =
    val expected = context()
    val otherOperation = context(operationUuid = uuid("00000000-0000-0000-0000-000000000004"))

    assertEquals(
      MySqlCommitReceiptVerification.Unproven,
      MySqlCommitReceipts.verifyReadback(expected, Some(receipt(otherOperation)))
    )

  @Test
  def theSameOperationUuidInAnotherFenceScopeConflicts(): Unit =
    val expected = context()
    val otherEpoch = context(writerEpoch = 8L)

    assertEquals(
      MySqlCommitReceiptVerification.Conflict(MySqlCommitReceiptConflict.OperationSlotMismatch),
      MySqlCommitReceipts.verifyReadback(expected, Some(receipt(otherEpoch)))
    )

  @Test
  def rejectsMalformedReceiptFingerprints(): Unit =
    val slot = MySqlCommitReceiptSlot(
      uuid("00000000-0000-0000-0000-000000000001"),
      7L,
      uuid("00000000-0000-0000-0000-000000000002"),
      uuid("00000000-0000-0000-0000-000000000003")
    )

    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlCommitReceipt(slot, Vector(1.toByte))
    )

  private def receipt(context: MySqlOperationContext): MySqlCommitReceipt =
    MySqlCommitReceipt(
      MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
      context.canonicalFingerprintSha256
    )

  private def context(
      bootUuid: UUID = uuid("00000000-0000-0000-0000-000000000001"),
      writerEpoch: Long = 7L,
      bindingUuid: UUID = uuid("00000000-0000-0000-0000-000000000002"),
      operationUuid: UUID = uuid("00000000-0000-0000-0000-000000000003"),
      seed: Int = 11
  ): MySqlOperationContext =
    MySqlOperationContext(
      bootUuid,
      writerEpoch,
      bindingUuid,
      operationUuid,
      Vector(MySqlExpectedEffect("pod/one", 3L, Vector.tabulate(32)(index => (seed + index).toByte)))
    )

  private def uuid(value: String): UUID = UUID.fromString(value)
