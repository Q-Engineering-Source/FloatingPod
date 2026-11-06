package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical}
import java.io.IOException
import java.sql.{SQLException, SQLNonTransientConnectionException, SQLTransactionRollbackException}
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 直接复用production状态/cleanup/exacttuple/codec逻辑；不构造假的native ACK或opaque plan。 */
final class MySqlFreshPlannedPrepareTest:
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")
  private val other = UUID.fromString("20000000-0000-0000-0000-000000000002")
  private val h = Vector.fill[Byte](32)(1)
  private val t = LocalDateTime.of(2026, 10, 7, 12, 0, 0, 123456000)
  private def state = new MySqlFreshPrepareLifecycle(Thread.currentThread())
  private def writer = MySqlBusinessWriterRow(id, 1L, Some(id), t, Some(other), Some(h), t.minusSeconds(1))
  private def throughGenesis(s: MySqlFreshPrepareLifecycle): Unit =
    s.beginPlanning(); s.planningCleaned(); s.beginGenesis(); s.genesisDurable()
  private def observation(s: MySqlFreshPrepareLifecycle) = s.observe(None, None)

  @Test def noExecutionBeforeActualPhaseAndNoSecondExecutionAttempt(): Unit =
    val s = state
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginExecution())
    throughGenesis(s); s.beginExecution()
    assertTrue(observation(s).executionAttempted)
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginExecution())
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.beginPlanning())

  @Test def commitAndReadbackDoNotManufactureNativeConfirmation(): Unit =
    val s = state; throughGenesis(s); s.beginExecution(); s.sqlAcknowledged(); s.readbackConfirmed()
    val before = observation(s)
    assertTrue(before.sqlCommitAcknowledged); assertTrue(before.fullReadbackConfirmed)
    assertFalse(before.nativeConfirmationAcknowledged); assertTrue(before.retainedResponsibility)
    val primary = new IOException("controlled PREPARED_CONFIRMED native publication failure")
    s.fail(primary)
    val retained = observation(s)
    assertSame(primary, retained.failure.get); assertEquals(MySqlFreshPreparePhase.Frozen, retained.phase)
    assertTrue(retained.sqlCommitAcknowledged); assertTrue(retained.fullReadbackConfirmed)
    assertFalse(retained.nativeConfirmationAcknowledged); assertTrue(retained.retainedResponsibility)
    assertSame(primary, assertThrows(classOf[IOException], () => s.beginExecution()))

  @Test def realStateOrderRequiresBothReadbackAndNativeAck(): Unit =
    val s = state; throughGenesis(s); s.beginExecution(); s.sqlAcknowledged()
    assertThrows(classOf[SQLNonTransientConnectionException], () => s.nativeConfirmed())
    s.readbackConfirmed(); s.nativeConfirmed()
    val facts = observation(s)
    assertEquals(MySqlFreshPreparePhase.PreparedConfirmed, facts.phase)
    assertTrue(facts.nativeConfirmationAcknowledged)
    assertTrue(facts.retainedResponsibility) // 当前W/source仍由owned actor负责；不是RuntimeReady。

  @Test def unknownBeforeCommitKeepsOriginalCauseAndStopsAttempt(): Unit =
    val s = state; throughGenesis(s); s.beginExecution()
    val cause = new SQLException("controlled response lost", "08006", 9001)
    val primary = new IOException("controlled owned failure", cause)
    val cleanup = new SQLException("controlled cleanup", "08006", 9002)
    val stack = primary.getStackTrace.toVector
    s.fail(primary); s.fail(cleanup)
    val facts = observation(s)
    assertFalse(facts.sqlCommitAcknowledged); assertSame(cause, facts.failure.get.getCause)
    assertSame(primary, facts.failure.get); assertEquals(stack, primary.getStackTrace.toVector)
    assertEquals(Vector(cleanup), primary.getSuppressed.toVector)
    assertSame(primary, assertThrows(classOf[IOException], () => s.beginPlanning()))

  @Test def ownerCannotBeReplacedByAnotherActualThread(): Unit =
    val s = state; val failure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => { try s.beginPlanning() catch case t: Throwable => failure.set(t) })
    wrong.start(); wrong.join(5000L); assertFalse(wrong.isAlive)
    assertTrue(failure.get().isInstanceOf[IllegalArgumentException])
    assertEquals(MySqlFreshPreparePhase.New, observation(s).phase)

  private final class Cleanup extends MySqlFreshPrepareCleanupActions:
    var rollbackFailure: Throwable = null
    var restoreFailure: Throwable = null
    var calls = Vector.empty[String]
    def rollback(): Unit =
      calls :+= "rollback"
      if rollbackFailure != null then throw rollbackFailure
    def restoreAndObserveIdle(): Unit =
      calls :+= "restore+actualgetter"
      if restoreFailure != null then throw restoreFailure

  @Test def rollbackFailureCannotRestoreOrSignCleanPlanning(): Unit =
    val a = new Cleanup; val original = new SQLException("controlled body failure", "HY000", 9003)
    val cleanup = new SQLException("controlled rollback failure", "08006", 9004); a.rollbackFailure = cleanup
    val result = MySqlFreshPrepareReadOnlyCleanup.finish(a, Some(original))
    assertTrue(result.isLeft); assertSame(original, result.swap.toOption.get)
    assertEquals(Vector(cleanup), original.getSuppressed.toVector)
    assertEquals(Vector("rollback"), a.calls)

  @Test def restoreOrGetterFailureRemainsOriginalCleanupCause(): Unit =
    val a = new Cleanup; val cleanup = new SQLException("controlled restore/getter failure", "08006", 9005)
    a.restoreFailure = cleanup
    val result = MySqlFreshPrepareReadOnlyCleanup.finish(a, None)
    assertTrue(result.isLeft); assertSame(cleanup, result.swap.toOption.get)
    assertEquals(Vector("rollback", "restore+actualgetter"), a.calls)

  @Test def cleanRollbackAndRestoreAreOnlyCleanupFactsNotBodySuccess(): Unit =
    val a = new Cleanup; val original = new IOException("controlled body failed before plan")
    assertTrue(MySqlFreshPrepareReadOnlyCleanup.finish(a, Some(original)).isRight)
    assertEquals(Vector("rollback", "restore+actualgetter"), a.calls)
    assertEquals(Vector.empty, original.getSuppressed.toVector)
    // actualactor随后重抛body primary，不发行plan；本测试不声称SQL交易成功。

  @Test def writerTupleUsesAllSixStoredFieldsAndNotDatabaseObservationTime(): Unit =
    val w = writer; val tuple = MySqlFreshPrepareRules.writer(w)
    MySqlFreshPrepareRules.requireWriter(tuple, w.copy(databaseNow = t.plusDays(1)))
    val changed = Vector(w.copy(bindingUuid = other), w.copy(writerEpoch = 2L), w.copy(ownerBootUuid = None),
      w.copy(leaseUntil = t.plusNanos(1000)), w.copy(lastOperationUuid = Some(id)),
      w.copy(lastFingerprint = Some(Vector.fill[Byte](32)(2))))
    changed.foreach(v => assertThrows(classOf[SQLTransactionRollbackException], () => MySqlFreshPrepareRules.requireWriter(tuple, v)))
    val bytes = tuple.lastFingerprint(); bytes(0) = 9
    assertArrayEquals(h.toArray, tuple.lastFingerprint())
    // Stored lastslot不从current/new owner重新造receipt：即使owner为空仍保留原完整pair。
    val unowned = MySqlFreshPrepareRules.writer(w.copy(ownerBootUuid = None))
    assertEquals(other, unowned.lastOperation()); assertArrayEquals(h.toArray, unowned.lastFingerprint())

  @Test def plannedLeaseIsNotRecomputedWhenExpiredAndPrecisionIsExact(): Unit =
    MySqlFreshPrepareRules.requireUnexpired(t, t.minusNanos(1000))
    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlFreshPrepareRules.requireUnexpired(t, t))
    assertThrows(classOf[SQLTransactionRollbackException], () => MySqlFreshPrepareRules.requireUnexpired(t, t.plusNanos(1000)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshPrepareRules.writer(writer.copy(leaseUntil = t.plusNanos(1))))

  @Test def operationAdapterKeepsFullOriginalScopeAndAllEffects(): Unit =
    val context = MySqlOperationContext(id, 7L, other, id, Vector(
      MySqlExpectedEffect("b/effect", 2L, h), MySqlExpectedEffect("a/effect", 1L, Vector.fill[Byte](32)(2))))
    val assertions = MySqlFreshPrepareRules.operation(context)
    assertEquals(context.bootUuid, assertions.boot()); assertEquals(context.writerEpoch, assertions.writerEpoch())
    assertEquals(context.bindingUuid, assertions.binding()); assertEquals(context.operationUuid, assertions.operation())
    assertEquals(2, assertions.effects().size())
    assertEquals("a/effect", assertions.effects().get(0).targetIdentity())
    assertEquals(1L, assertions.effects().get(0).expectedVersion())
    assertArrayEquals(context.expectedEffects.head.desiredEffectFingerprintSha256.toArray, assertions.effects().get(0).desiredFingerprint())
    assertArrayEquals(context.canonicalFingerprintSha256.toArray, assertions.originalOperationFingerprint())
    val bytes = assertions.originalOperationFingerprint(); bytes(0) = 9
    assertArrayEquals(context.canonicalFingerprintSha256.toArray, assertions.originalOperationFingerprint())

  @Test def fullNativeRecordComparisonRejectsHashOnlyAndOneByteChange(): Unit =
    val full = Array.tabulate[Byte](512)(i => i.toByte)
    MySqlFreshPrepareRules.exactBytes(full, full.clone())
    val changed = full.clone(); changed(300) = (changed(300) ^ 1).toByte
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlFreshPrepareRules.exactBytes(full, changed))
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlFreshPrepareRules.exactBytes(full, h.toArray))

  @Test def actualRecordCodecRejectsTotalOversizeBeforeDmlCapability(): Unit =
    val valid = com.wi.floatingpod.server.B1FreshPrepareRecordFixture.validRecord()
    assertTrue(B1ResumeRecordCanonical.encode(valid).length <= 65536)
    val oversized = com.wi.floatingpod.server.B1FreshPrepareRecordFixture.oversizedValidRecord()
    val failure = assertThrows(classOf[IOException], () => B1ResumeRecordCanonical.encode(oversized))
    assertEquals("Full B1 state exceeds 65536 byte bound", failure.getMessage)
