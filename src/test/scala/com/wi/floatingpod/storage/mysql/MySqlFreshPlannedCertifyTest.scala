package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1FreshPrepareRecordFixture, B1ResumeRecord, B1ResumeRecordCanonical, WindowsMarkerIo}
import java.io.IOException
import java.sql.{SQLException, SQLNonTransientConnectionException, SQLTransactionRollbackException}
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Actual production state/tuple/full canonical logic. No fake JDBC/native ACK or self-issued capability. */
final class MySqlFreshPlannedCertifyTest:
  private def state = new MySqlFreshCertifyLifecycle(Thread.currentThread())
  private def observe(s: MySqlFreshCertifyLifecycle) = s.observe(None, None, None)
  private def throughIntent(s: MySqlFreshCertifyLifecycle): Unit =
    s.beginMarker(); s.markerAcknowledged(); s.beginPlanning(); s.planned(); s.beginIntent(); s.intentDurable()

  @Test def markerMustBeAcknowledgedBeforeReadonlyCertifyPlanning(): Unit =
    val s = state
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginPlanning())
    s.beginMarker()
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginPlanning())
    val primary = new IOException("controlled actual marker boundary failure")
    s.fail("marker", primary)
    assertTrue(observe(s).markerPublicationAttempted)
    assertFalse(observe(s).executionAttempted)
    assertSame(primary, assertThrows(classOf[IOException], () => s.beginPlanning()))

  @Test def sqlExecutionRequiresDurableIntentAndCannotResend(): Unit =
    val s = state
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginExecution())
    throughIntent(s); s.beginExecution()
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginExecution())
    assertTrue(observe(s).intentPublicationAttempted)
    assertTrue(observe(s).executionAttempted)

  @Test def commitReplyUnknownRetainsOriginalStackCauseAndPendingStage(): Unit =
    val s = state; throughIntent(s); s.beginExecution()
    val cause = new SQLException("controlled response lost", "08006", 9201)
    val context = MySqlOperationContext(UUID.randomUUID(), 1L, UUID.randomUUID(), UUID.randomUUID(),
      Vector(MySqlExpectedEffect("bootstrap/full-graph", 0L, Vector.fill[Byte](32)(1))))
    val primary = MySqlCommitUnknownException(context, cause)
    val stack = primary.getStackTrace.toVector
    val cleanup = new IOException("controlled cleanup")
    s.fail("sql-commit", primary); s.fail("cleanup", cleanup)
    val o = observe(s)
    assertEquals(MySqlFreshCertifyPhase.Frozen, o.phase)
    assertSame(primary, o.failure.get); assertSame(cause, o.failure.get.getCause)
    assertEquals(stack, primary.getStackTrace.toVector); assertEquals(Vector(cleanup), primary.getSuppressed.toVector)
    assertFalse(o.sqlCommitAcknowledged); assertSame(primary, assertThrows(classOf[MySqlCommitUnknownException], () => s.beginExecution()))

  @Test def sqlConfirmedNativeFailureDoesNotBecomeReadyOrPermitSqlReplay(): Unit =
    val s = state; throughIntent(s); s.beginExecution(); s.sqlAcknowledged(); s.readbackConfirmed()
    val primary = new IOException("controlled CERTIFIED native failure")
    s.fail("native-certified", primary)
    val o = observe(s)
    assertTrue(o.sqlCommitAcknowledged); assertTrue(o.fullReadbackConfirmed); assertFalse(o.nativeConfirmationAcknowledged)
    assertSame(primary, assertThrows(classOf[IOException], () => s.beginExecution()))
    assertSame(primary, assertThrows(classOf[IOException], () => s.beginAudit()))

  @Test def certificationNeedsReadbackThenNativeAckBeforeFreshAudit(): Unit =
    val s = state; throughIntent(s); s.beginExecution(); s.sqlAcknowledged()
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.nativeConfirmed())
    s.readbackConfirmed(); s.nativeConfirmed(); s.beginAudit()
    assertEquals(MySqlFreshCertifyPhase.Auditing, observe(s).phase)
    s.admitted()
    assertEquals(MySqlFreshCertifyPhase.Admitted, observe(s).phase)
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginAudit())

  @Test def freshAuditFailureKeepsCertifiedFactsAndFreezesAdmission(): Unit =
    val s = state; throughIntent(s); s.beginExecution(); s.sqlAcknowledged(); s.readbackConfirmed(); s.nativeConfirmed(); s.beginAudit()
    val original = new SQLException("controlled fresh post-audit failure", "HY000", 9202)
    s.fail("post-audit", original)
    assertTrue(observe(s).nativeConfirmationAcknowledged)
    assertSame(original, assertThrows(classOf[SQLException], () => s.admitted()))

  @Test def actualWrongThreadCannotAdvanceOrObserveCertifyState(): Unit =
    val s = state; val failure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => { try s.beginMarker() catch case t: Throwable => failure.set(t) })
    wrong.start(); wrong.join(5000); assertFalse(wrong.isAlive)
    assertInstanceOf(classOf[IllegalArgumentException], failure.get())
    assertEquals(MySqlFreshCertifyPhase.New, observe(s).phase)

  @Test def targetWriterPreservesLeaseEpochOwnerAndFixedCertifyReceipt(): Unit =
    val binding = UUID.randomUUID(); val boot = UUID.randomUUID(); val prepare = UUID.randomUUID(); val certify = UUID.randomUUID()
    val lease = LocalDateTime.of(2026, 10, 7, 12, 0, 0, 123456000)
    val old = MySqlBusinessWriterRow(binding, 3, Some(boot), lease, Some(prepare), Some(Vector.fill[Byte](32)(1)), lease.minusSeconds(1))
    val target = new MySqlFreshPrepareCapabilities.WriterTuple(binding, 3, boot, lease, certify, Array.fill[Byte](32)(2))
    val actual = old.copy(lastOperationUuid = Some(certify), lastFingerprint = Some(Vector.fill[Byte](32)(2)))
    MySqlFreshPrepareRules.requireWriter(target, actual)
    for changed <- Vector(actual.copy(writerEpoch = 4), actual.copy(ownerBootUuid = Some(UUID.randomUUID())),
      actual.copy(leaseUntil = lease.plusSeconds(1)), actual.copy(lastOperationUuid = Some(prepare)), actual.copy(lastFingerprint = old.lastFingerprint)) do
      assertThrows(classOf[SQLTransactionRollbackException], () => MySqlFreshPrepareRules.requireWriter(target, changed))
    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlFreshPrepareRules.requireUnexpired(lease, lease))
    assertEquals(lease, target.leaseUntil())

  @Test def fullSuccessorPreservesOriginalPrepareWitnessAndExactPredecessor(): Unit =
    val original = B1FreshPrepareRecordFixture.validRecord()
    val a = original.assertions(); val prepare = a.pendingAttempt()
    val preparedState = new B1ResumeRecord.FullStateAssertions(a.currentOwner(), null, null, prepare,
      a.targetWriter(), a.targetWriter(), null, null, null, null)
    val prepared = MySqlFreshCertifyRules.successor(original, B1ResumeRecord.Kind.PREPARED_CONFIRMED, preparedState)
    assertEquals(1L, prepared.sequence())
    assertArrayEquals(java.security.MessageDigest.getInstance("SHA-256").digest(B1ResumeRecordCanonical.encode(original)), prepared.previousRecordSha256())
    assertSame(prepare, prepared.assertions().originalPrepareWitness())
    val changed = prepared.previousRecordSha256(); changed(0) = (changed(0) ^ 1).toByte
    assertFalse(java.util.Arrays.equals(changed, prepared.previousRecordSha256()))
    assertArrayEquals(original.originalReceipt(), prepared.originalReceipt())

  @Test def missingActualCurrentRecordAndMissingMarkerAckExplicitlyFail(): Unit =
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlFreshCertifyRules.requireRecord(Array[Byte](1), null))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshCertifyRules.markerFacts(null, 1))
