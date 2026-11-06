package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.{Duration, LocalDateTime}
import java.util.UUID

/** 真实 receipt projection/window/mailbox/lifecycle/rules；不伪造 JDBC admission integration。 */
class MySqlOwnedAdmissionFreshnessTest:
  private val boot = UUID.fromString("d2df04b8-fae6-4bc6-9af7-f3e5b2b7bd26")
  private val server = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val binding = UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604")
  private val provisioning = UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f")
  private val identity = MySqlOwnedIoIdentity(boot, 4L, 7L)
  private val databaseTime = LocalDateTime.of(2026, 10, 7, 12, 0)
  private def receipt(origin: Long, leaseWindow: Long = 1000L, auditWindow: Long = 2000L,
                      generation: Long = 7L, actualBinding: UUID = binding,
                      actualProfile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format2): RuntimeSchemaAuditReceipt =
    val fence = MySqlWriterFenceExpectation.fromAcquiredFence(actualBinding, boot, 9L,
      databaseTime.plusSeconds(60), databaseTime, origin)
    // Existing package constructor is the stable unit seam; this is never a SQL/driver success。
    new RuntimeSchemaAuditReceipt(actualProfile, MySqlExpectedPhysicalFormat.Format2,
      new Object, boot, 4L, generation, server, "unit_schema", actualBinding, 9L, provisioning,
      "NONE", None, None, None, None, "11" * 32, "22" * 32, fence,
      origin + 5L, leaseWindow, origin + 10L, auditWindow)

  private def liveReceipt(generation: Long = 7L, actualBinding: UUID = binding,
                          actualProfile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format2): RuntimeSchemaAuditReceipt =
    receipt(System.nanoTime() - Duration.ofMillis(1).toNanos,
      Duration.ofSeconds(30).toNanos, Duration.ofSeconds(30).toNanos, generation, actualBinding, actualProfile)

  @Test def recoveredMailboxCannotSeedFromMissingActualServiceOwnerOrCallerSequence():Unit =
    val mailbox=new MySqlOwnedMaintenanceMailbox(identity)
    assertThrows(classOf[IllegalArgumentException],()=>mailbox.initializeRecoveredDbFirst(null))
    assertTrue(mailbox.poll.latest.isEmpty)
    assertTrue(mailbox.poll.failure.isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>mailbox.publish(liveReceipt().ownedFreshness(9L)))
    assertTrue(mailbox.poll.latest.isEmpty)
    val original=liveReceipt().ownedFreshness(0L)
    assertTrue(mailbox.publish(original))
    assertSame(original,mailbox.poll.latest.get)
    assertThrows(classOf[IllegalArgumentException],()=>mailbox.initializeRecoveredDbFirst(null))
    assertSame(original,mailbox.poll.latest.get)

  @Test def projectionCopiesActualPrivateFenceReceiptAndAuditWindowsWithoutReanchoring(): Unit =
    val actual = receipt(-10000L)
    val value = actual.ownedFreshness(0L)
    assertEquals(boot, value.bootId)
    assertEquals(4L, value.bootGeneration)
    assertEquals(7L, value.connectionGeneration)
    assertEquals(server, value.serverId)
    assertEquals(binding, value.bindingId)
    assertEquals(9L, value.writerEpoch)
    assertEquals(MySqlSchemaAuditProfile.Format2, value.profile)
    assertEquals(MySqlExpectedPhysicalFormat.Format2, value.physicalFormat)
    assertEquals(MySqlAdmissionWindow(-10000L, Duration.ofSeconds(60).toNanos), value.fenceLease)
    assertEquals(MySqlAdmissionWindow(-9995L, 1000L), value.receiptLease)
    assertEquals(MySqlAdmissionWindow(-9990L, 2000L), value.audit)
    assertEquals(995L, value.remainingAt(-9990L))
    assertThrows(classOf[IllegalArgumentException], () => value.nextMaintenanceAfter(-9990L))
    val schedulable = receipt(-10000L, leaseWindow = 10000000L, auditWindow = 20000000L).ownedFreshness(0L)
    assertEquals(Duration.ofNanos(4999997L), schedulable.nextMaintenanceAfter(-9990L))
    assertFalse(value.isCurrentAt(-8995L))
    assertThrows(classOf[IllegalArgumentException], () => value.nextMaintenanceAfter(-8995L))
    assertEquals(-9995L, value.receiptLease.observedAtNanos, "polling cannot move actual receipt origin")

  @Test def negativeNanoOriginAndSignedWrapUseElapsedDifferenceRatherThanAbsoluteDeadline(): Unit =
    val value = receipt(Long.MaxValue - 20L, leaseWindow = 100L, auditWindow = 100L).ownedFreshness(0L)
    val afterWrap = Long.MinValue + 20L
    assertEquals(64L, value.receiptLease.remainingAt(afterWrap))
    assertTrue(value.isCurrentAt(afterWrap))
    assertFalse(value.isCurrentAt(Long.MinValue + 100L))
    assertFalse(receipt(-1000L).ownedFreshness(0L).isCurrentAt(-1001L), "future origin is not fresh")

  @Test def eachWindowBoundsFreshnessAndZeroHalfRemainingDoesNotExtendDeadline(): Unit =
    val receiptLimited = receipt(100L, leaseWindow = 20L, auditWindow = 200L).ownedFreshness(0L)
    assertTrue(receiptLimited.isCurrentAt(124L))
    assertThrows(classOf[IllegalArgumentException], () => receiptLimited.nextMaintenanceAfter(124L))
    assertFalse(receiptLimited.isCurrentAt(125L))
    val auditLimited = receipt(100L, leaseWindow = 200L, auditWindow = 20L).ownedFreshness(0L)
    assertFalse(auditLimited.isCurrentAt(130L))
    val fenceLimited = receipt(100L, leaseWindow = MySqlSchemaAuditor.MaxReceiptAgeNanos,
      auditWindow = MySqlSchemaAuditor.MaxReceiptAgeNanos).ownedFreshness(0L)
    assertFalse(fenceLimited.isCurrentAt(100L + Duration.ofSeconds(60).toNanos))
    assertThrows(classOf[IllegalArgumentException], () => MySqlAdmissionWindow(0L, 0L))
    assertThrows(classOf[IllegalArgumentException], () => MySqlAdmissionWindow(-1L, Long.MaxValue))
    assertThrows(classOf[IllegalArgumentException], () => receipt(0L).ownedFreshness(-1L))
    assertThrows(classOf[IllegalArgumentException], () => receipt(0L, auditWindow = 0L).ownedFreshness(0L))
    assertThrows(classOf[IllegalArgumentException], () => receipt(0L, generation = 0L).ownedFreshness(0L))
    val exactMinimum = receipt(100L, leaseWindow = 2000005L, auditWindow = 4000000L).ownedFreshness(0L)
    assertEquals(Duration.ofMillis(1), exactMinimum.nextMaintenanceAfter(110L))
    assertThrows(classOf[IllegalArgumentException], () => exactMinimum.nextMaintenanceAfter(111L))

  @Test def capacityOneMailboxCoalescesAndRejectsReplayAndForeignGenerationWithoutReplacingCurrent(): Unit =
    val mailbox = new MySqlOwnedMaintenanceMailbox(identity)
    assertTrue(mailbox.poll.latest.isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => mailbox.publish(liveReceipt().ownedFreshness(1L)))
    val initial = liveReceipt().ownedFreshness(0L)
    assertTrue(mailbox.publish(initial))
    var current = initial
    (1L to 1000L).foreach { sequence =>
      current = liveReceipt().ownedFreshness(sequence)
      assertTrue(mailbox.publish(current))
    }
    assertSame(current, mailbox.poll.latest.get)
    assertFalse(mailbox.publish(initial))
    assertFalse(mailbox.publish(current))
    assertThrows(classOf[IllegalArgumentException], () => mailbox.publish(liveReceipt(generation = 6L).ownedFreshness(1001L)))
    assertThrows(classOf[IllegalArgumentException], () => mailbox.publish(
      liveReceipt(actualBinding = UUID.randomUUID()).ownedFreshness(1001L)))
    assertThrows(classOf[IllegalArgumentException], () => mailbox.publish(
      liveReceipt(actualProfile = MySqlSchemaAuditProfile.Format1).ownedFreshness(1001L)))
    val expired = receipt(System.nanoTime() - Duration.ofSeconds(60).toNanos).ownedFreshness(1001L)
    assertFalse(expired.isCurrent)
    assertThrows(classOf[IllegalArgumentException], () => mailbox.publish(expired))
    assertSame(current, mailbox.poll.latest.get)
    assertTrue(mailbox.poll.failure.isEmpty)

  @Test def stickyFailureSurvivesLateSuccessAndShutdownRetainsLatestFactsAndOriginalCause(): Unit =
    val mailbox = new MySqlOwnedMaintenanceMailbox(identity)
    val first = liveReceipt().ownedFreshness(0L)
    assertTrue(mailbox.publish(first))
    val primary = new IllegalStateException("original-maintenance-cause")
    val cleanup = new IllegalArgumentException("later-cleanup-cause")
    mailbox.fail(primary)
    mailbox.fail(cleanup)
    assertFalse(mailbox.publish(receipt(System.nanoTime()).ownedFreshness(1L)))
    mailbox.stop()
    assertSame(first, mailbox.poll.latest.get)
    assertSame(primary, mailbox.poll.failure.get)
    assertTrue(primary.getSuppressed.exists(_ eq cleanup))
    assertTrue(mailbox.poll.stopped)
    assertFalse(mailbox.poll.isCurrent)

  @Test def readOnlyWorkChecksSameWindowWithoutPublishingOrAdvancingSequence():Unit =
    val mailbox=new MySqlOwnedMaintenanceMailbox(identity)
    val first=liveReceipt().ownedFreshness(0L)
    assertTrue(mailbox.publish(first))
    mailbox.requireCurrentRead(first)
    mailbox.requireCurrentRead(first)
    assertSame(first,mailbox.poll.latest.get)
    assertThrows(classOf[IllegalArgumentException],()=>mailbox.requireCurrentRead(liveReceipt().ownedFreshness(1L)))
    assertSame(first,mailbox.poll.latest.get)
    mailbox.stop()
    assertThrows(classOf[IllegalArgumentException],()=>mailbox.requireCurrentRead(first))

  @Test def terminalProducerCanRetireFailedMailboxFactsWhileKeepingOriginalFailure():Unit =
    val mailbox=new MySqlOwnedMaintenanceMailbox(identity)
    val first=liveReceipt().ownedFreshness(0L)
    assertTrue(mailbox.publish(first))
    val original=new IllegalStateException("actual producer failed")
    mailbox.fail(original)
    assertThrows(classOf[IllegalStateException],()=>mailbox.retireAfterTerminalProducer())
    assertSame(first,mailbox.poll.latest.get)
    mailbox.stop();mailbox.retireAfterTerminalProducer()
    assertTrue(mailbox.poll.latest.isEmpty)
    assertSame(original,mailbox.poll.failure.get)
    assertTrue(mailbox.poll.stopped)
    assertFalse(mailbox.poll.isCurrent)
    assertFalse(mailbox.publish(liveReceipt().ownedFreshness(1L)))

  @Test def expiredLeaseAndUnknownPhaseCannotRenewOrAcquireOverOwnedRowOrChangeReceiptSlot(): Unit =
    val operation = UUID.randomUUID()
    val fingerprint = Vector.tabulate(32)(_.toByte)
    val expired = MySqlWriterFenceRow(binding, 9L, Some(boot), databaseTime,
      Some(operation), Some(fingerprint))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.renew(expired, boot, databaseTime, databaseTime.plusSeconds(60)))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlWriterFenceRules.acquire(expired, boot, databaseTime, databaseTime.plusSeconds(60)))
    assertEquals(9L, expired.epoch)
    assertEquals(Some(operation), expired.lastCommitOperationUuid)
    assertEquals(Some(fingerprint), expired.lastCommitFingerprint)
    val original = new IllegalStateException("unknown-unit-phase")
    val unknown = MySqlWriterFenceUnknown(MySqlWriterFenceOperation.Renew, Some(expired), Some(expired), null, original)
    assertTrue(MySqlWriterFenceRules.businessAdmission(unknown).isEmpty)
    assertSame(original, unknown.failure)
    assertTrue(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceUnclaimed).isEmpty)
    assertTrue(MySqlWriterFenceRules.businessAdmission(MySqlWriterFenceAwaitingAudit(null)).isEmpty)

  @Test def lifecycleFailureAndClosingRevokeAdmissionWithoutClearingPrimary(): Unit =
    val lifecycle = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException], () => lifecycle.requireAdmitted())
    lifecycle.beginOpen()
    lifecycle.admit()
    lifecycle.requireAdmitted()
    val original = new IllegalStateException("session-original-failure")
    lifecycle.markFailed(original)
    assertThrows(classOf[IllegalArgumentException], () => lifecycle.requireAdmitted())
    assertSame(original, lifecycle.failure.get)
    lifecycle.beginClose(None)
    assertFalse(lifecycle.releaseEligible)
    assertThrows(classOf[IllegalArgumentException], () => lifecycle.requireAdmitted())
    lifecycle.finishClose()
    assertSame(original, lifecycle.failure.get)
