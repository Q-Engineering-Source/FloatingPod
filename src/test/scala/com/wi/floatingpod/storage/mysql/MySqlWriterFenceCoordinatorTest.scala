package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.time.LocalDateTime
import java.util.UUID

class MySqlWriterFenceCoordinatorTest:
  private val boot = UUID.fromString("d2df04b8-fae6-4bc6-9af7-f3e5b2b7bd26")
  private val otherBoot = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val binding = UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604")
  private val operation = UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f")
  private val fingerprint = Vector.tabulate(32)(_.toByte)
  private val now = LocalDateTime.of(2026, 10, 7, 12, 0, 0)
  private val oldLease = now.plusSeconds(20)
  private val newLease = now.plusSeconds(60)

  private def row(
      epoch: Long = 7L,
      owner: Option[UUID] = Some(boot),
      lease: LocalDateTime = oldLease
  ): MySqlWriterFenceRow =
    MySqlWriterFenceRow(binding, epoch, owner, lease, Some(operation), Some(fingerprint))

  @Test
  def acquireOnlyClaimsUnownedExpiredRowAndPreservesTheReceiptSlot(): Unit =
    val unclaimed = row(owner = None, lease = now.minusSeconds(1))
    val acquired = MySqlWriterFenceRules.acquire(unclaimed, boot, now, newLease)

    assertEquals(8L, acquired.epoch)
    assertEquals(Some(boot), acquired.ownerBootUuid)
    assertEquals(newLease, acquired.leaseUntil)
    assertEquals(unclaimed.lastCommitOperationUuid, acquired.lastCommitOperationUuid)
    assertEquals(unclaimed.lastCommitFingerprint, acquired.lastCommitFingerprint)

    val expiredButOwned = row(owner = Some(otherBoot), lease = now.minusSeconds(1))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.acquire(expiredButOwned, boot, now, newLease)
    )

  @Test
  def renewRequiresExactCurrentOwnerAndStrictlyExtendsLeaseWithoutChangingEpochOrSlot(): Unit =
    val renewed = MySqlWriterFenceRules.renew(row(), boot, now, newLease)

    assertEquals(7L, renewed.epoch)
    assertEquals(Some(boot), renewed.ownerBootUuid)
    assertEquals(newLease, renewed.leaseUntil)
    assertEquals(Some(operation), renewed.lastCommitOperationUuid)
    assertEquals(Some(fingerprint), renewed.lastCommitFingerprint)
    assertTrue(MySqlWriterFenceRules.matchesTokenRow(row(), binding, 7L, boot, oldLease))
    assertFalse(MySqlWriterFenceRules.matchesTokenRow(row(lease = oldLease.plusNanos(1000L)), binding, 7L, boot, oldLease))
    assertFalse(MySqlWriterFenceRules.matchesTokenRow(row(), binding, 6L, boot, oldLease))

    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.renew(row(owner = Some(otherBoot)), boot, now, newLease)
    )
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.renew(row(), boot, now, oldLease)
    )
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.renew(row(lease = now), boot, now, newLease)
    )

  @Test
  def releaseKeepsEpochAndReceiptSlotAndMakesLeaseStrictlyPast(): Unit =
    val released = MySqlWriterFenceRules.release(row(), boot, now)

    assertEquals(7L, released.epoch)
    assertEquals(None, released.ownerBootUuid)
    assertEquals(now.minusNanos(1000L), released.leaseUntil)
    assertTrue(released.leaseUntil.isBefore(now))
    assertEquals(Some(operation), released.lastCommitOperationUuid)
    assertEquals(Some(fingerprint), released.lastCommitFingerprint)

    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.release(row(owner = Some(otherBoot)), boot, now)
    )
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.release(row(lease = now), boot, now)
    )

  @Test
  def epochOverflowFailsInsteadOfWrapping(): Unit =
    assertThrows(classOf[ArithmeticException], () =>
      MySqlWriterFenceRules.nextEpoch(Long.MaxValue)
    )

  @Test
  def pendingRenewalAndUnknownOldRowCannotOpenAdmissionOrConfirmCommit(): Unit =
    assertFalse(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceUnclaimed).nonEmpty)
    assertFalse(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceRetiredRecall).nonEmpty)
    assertFalse(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceAwaitingAudit(null)).nonEmpty)
    assertFalse(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceRenewing(null, null)).nonEmpty)

    val oldRow = row()
    assertFalse(MySqlWriterFenceRules.hasDistinctExpectedTarget(Some(oldRow), Some(oldRow)))
    val target = oldRow.copy(leaseUntil = newLease)
    assertTrue(MySqlWriterFenceRules.hasDistinctExpectedTarget(Some(target), Some(oldRow)))
    assertFalse(MySqlWriterFenceRules.hasDistinctExpectedTarget(None, Some(oldRow)))