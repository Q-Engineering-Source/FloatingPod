package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.mysql.MainBootProjectionFixture.*
import com.wi.floatingpod.server.{B1FreshPrepareRecordFixture, B1ResumeRecord, B1ResumeRecordCanonical, WindowsWorldProvisioner}
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.Assertions.*

/** Typed complete-full20 ordinary graph contract; these assertions do not claim actor/SQL/native completion. */
final class MySqlCertifiedOrdinaryHandoffTest:
  @Test def historicalReadBudgetStaysOnOriginalRootAndBoundsAllPasses(): Unit =
    val original = snapshot()
    val root = original.allocationBudget
    try
      val before = root.accountedMemoryBytes
      assertEquals(44L, MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(root, 2L))
      assertEquals(before + 1114112L + 44L * 1024L, root.accountedMemoryBytes)
      MySqlFull20ColdReadAccounting.requireV5HistoryMembers(root, 2L, 8L)
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlFull20ColdReadAccounting.requireV5HistoryMembers(root, root.profile.maxMembers(), 1L))
      assertThrows(classOf[ArithmeticException], () =>
        MySqlFull20ColdReadAccounting.requireV5HistoryMembers(root, 1L, Long.MaxValue))
      Vector(0L, -1L, Long.MaxValue).foreach { count =>
        assertThrows(classOf[IllegalArgumentException], () => MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(root, count))
      }
      val child = root.newCanonicalScope()
      try assertThrows(classOf[IllegalArgumentException], () => MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(child, 1L))
      finally child.closeScopedReadBudget()
      root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "V5 history test cumulative exhaustion")
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(root, 1L))
    finally root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(root, 1L))

  private val oldLease = LocalDateTime.of(2026, 10, 11, 1, 0, 0, 123456000)
  private val newLease = LocalDateTime.of(2026, 10, 11, 1, 1, 0, 654321000)

  private final case class SettlementFixture(handoff: B1ResumeRecord, before: MySqlBusinessWriterRow,
      target: MySqlBusinessWriterRow, actual: MySqlBusinessWriterRow, evidence: Array[Byte])
  private def settlementFixture(root: MySqlFormat3ReadBudget, released: Boolean = false): SettlementFixture =
    val original = B1FreshPrepareRecordFixture.validRecord()
    val binding = original.assertions().currentWriter().binding()
    val before = MySqlBusinessWriterRow(binding, 7L, if released then None else Some(id(90)), oldLease,
      Some(id(80)), Some(Vector.fill(32)(3.toByte)), oldLease.plusSeconds(1))
    val target = before.copy(writerEpoch = 8L, ownerBootUuid = Some(id(91)), leaseUntil = newLease)
    val actual = target.copy(databaseNow = oldLease.plusSeconds(2))
    val slot = new B1ResumeRecord.ReceiptSlotAssertions(id(90), 7L, binding, id(80), Array.fill[Byte](32)(3))
    def assertion(w: MySqlBusinessWriterRow) = new B1ResumeRecord.WriterAssertions(w.bindingUuid, w.writerEpoch,
      w.ownerBootUuid.orNull, w.leaseUntil, slot)
    val effect = MySqlOrdinaryHandoffGraph.effectFingerprint(before, target, id(93), id(92), root)
    val context = MySqlOperationContext(id(91), 8L, binding, id(92),
      Vector(MySqlExpectedEffect("certified-ordinary/writer", 7L, effect)))
    val op = new B1ResumeRecord.OperationAssertions(id(91), 8L, binding, id(92),
      java.util.List.of(new B1ResumeRecord.EffectAssertions("certified-ordinary/writer", 7L, effect.toArray)),
      context.canonicalFingerprintSha256.toArray)
    val graph = new B1ResumeRecord.CommonGraphAssertions("DATA", Array[Byte](1), "DATA", Array[Byte](2), "DATA", Array[Byte](3))
    val attempt = new B1ResumeRecord.AttemptAssertions(id(93), B1ResumeRecord.Phase.HANDOFF, id(92), 2L,
      id(94), 123L, original.barrierNamespace(), op, assertion(before), assertion(target), graph, graph)
    val handoff = new B1ResumeRecord(2L, Array.fill[Byte](32)(0), B1ResumeRecord.Kind.HANDOFF_PREPARED,
      original.sourceAnchorSha256(), original.originalReceipt(), attempt.namespace(),
      new B1ResumeRecord.FullStateAssertions(new B1ResumeRecord.NativeOwnerAssertions(2L, 2L, id(91), 2L),
        new B1ResumeRecord.NativeOwnerAssertions(1L, 1L, id(90), 1L), null, original.assertions().pendingAttempt(),
        attempt.beforeWriter(), attempt.targetWriter(), attempt, null, null, null))
    SettlementFixture(handoff, before, target, actual,
      MySqlOrdinaryHandoffGraph.commitEvidence(attempt, before, target, actual, root))

  private def settled(f: SettlementFixture, evidence: Array[Byte],
      protocol: String = "MYSQL_SAME_CID_NULL_SAFE_OCCUPIED_W_CAS_COMMIT_IDLE_TARGET_READBACK_V1",
      diagnostic: Option[Array[Byte]] = None): B1ResumeRecord =
    val old = f.handoff.assertions(); val attempt = old.pendingAttempt()
    val settlement = new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED, attempt,
      protocol, evidence, diagnostic.getOrElse(attempt.context().originalOperationFingerprint()))
    new B1ResumeRecord(3L, WindowsWorldProvisioner.digest(B1ResumeRecordCanonical.encode(f.handoff)),
      B1ResumeRecord.Kind.DB_ACQUIRED, f.handoff.sourceAnchorSha256(), f.handoff.originalReceipt(), f.handoff.barrierNamespace(),
      new B1ResumeRecord.FullStateAssertions(old.currentOwner(), old.previousOwner(), old.retirement(), old.originalPrepareWitness(),
        attempt.targetWriter(), attempt.targetWriter(), null, settlement, old.marker(), old.acknowledgedCertifyWitness()))

  @Test def originalCommitWireFormatAndFirstV4SettlementRemainCompatible(): Unit =
    val root = snapshot().allocationBudget
    try
      Vector(false, true).foreach { released =>
        val f = settlementFixture(root, released); val a = f.handoff.assertions().pendingAttempt()
        // Independent transcription of the pre-existing Actor wire format, including all length prefixes.
        val sink = new ByteArrayOutputStream(); val out = new DataOutputStream(sink)
        def uuid(v: java.util.UUID): Unit = { out.writeLong(v.getMostSignificantBits); out.writeLong(v.getLeastSignificantBits) }
        def text(v: String): Unit = { val b = v.getBytes(StandardCharsets.UTF_8); out.writeInt(b.length); out.write(b) }
        def time(v: LocalDateTime): Unit = { out.writeLong(v.toLocalDate.toEpochDay); out.writeLong(v.toLocalTime.toNanoOfDay) }
        text("FP_MYSQL_ORDINARY_W_CAS_COMMIT_READBACK_V1"); out.writeLong(123L); uuid(id(94)); uuid(id(91))
        out.writeLong(2L); uuid(id(93)); uuid(id(92)); text(a.namespace())
        Vector(f.before, f.target, f.actual).foreach { w =>
          uuid(w.bindingUuid); out.writeLong(w.writerEpoch); out.writeBoolean(w.ownerBootUuid.nonEmpty); w.ownerBootUuid.foreach(uuid)
          time(w.leaseUntil); out.writeBoolean(true); uuid(id(80)); out.writeBoolean(true); out.writeInt(32)
          out.write(Array.fill[Byte](32)(3)); time(w.databaseNow)
        }
        out.writeInt(32); out.write(a.context().originalOperationFingerprint()); out.close()
        assertArrayEquals(sink.toByteArray, f.evidence)
        val result = MySqlOrdinaryHandoffGraph.requireConfirmedHandoffData(f.handoff, settled(f, f.evidence), root)
        assertEquals(f.before, result.before); assertEquals(f.target, result.target); assertEquals(f.actual, result.actual)
      }
    finally root.closeRootReadBudget()

  @Test def strictCommitEvidenceRejectsWrongFramingIdentityAndWriterReadback(): Unit =
    val root = snapshot().allocationBudget
    try
      val f = settlementFixture(root); val a = f.handoff.assertions().pendingAttempt()
      def rejected(bytes: Array[Byte]): Unit = assertThrows(classOf[Exception], () =>
        MySqlOrdinaryHandoffGraph.requireConfirmedHandoffData(f.handoff, settled(f, bytes), root))
      val domainEnd = 4 + "FP_MYSQL_ORDINARY_W_CAS_COMMIT_READBACK_V1".length
      val writerStart = domainEnd + 8 + 16 + 16 + 8 + 16 + 16 + 4 + a.namespace().length
      Vector(0, 4, domainEnd, domainEnd + 8, domainEnd + 24, domainEnd + 40,
        domainEnd + 48, domainEnd + 64, writerStart - 1, writerStart,
        writerStart + 16, writerStart + 25, f.evidence.length - 36, f.evidence.length - 1).foreach { index =>
        val bytes = f.evidence.clone(); bytes(index) = (bytes(index) ^ 1).toByte; rejected(bytes)
      }
      val badOption = f.evidence.clone(); badOption(writerStart + 24) = 2; rejected(badOption)
      rejected(f.evidence.dropRight(1)); rejected(f.evidence ++ Array[Byte](0))
      def evidence(before: MySqlBusinessWriterRow = f.before, target: MySqlBusinessWriterRow = f.target,
          actual: MySqlBusinessWriterRow = f.actual) = MySqlOrdinaryHandoffGraph.commitEvidence(a, before, target, actual, root)
      rejected(evidence(before = f.before.copy(databaseNow = oldLease.minusSeconds(1))))
      rejected(evidence(target = f.target.copy(databaseNow = oldLease.plusSeconds(3))))
      rejected(evidence(actual = f.actual.copy(ownerBootUuid = Some(id(99)))))
      rejected(evidence(actual = f.actual.copy(databaseNow = newLease)))
      rejected(evidence(actual = f.actual.copy(lastFingerprint = Some(Vector.fill(32)(4.toByte)))))
    finally root.closeRootReadBudget()

  @Test def completeSettlementCannotDiscardOriginalStateOrChangeProofIdentity(): Unit =
    val root = snapshot().allocationBudget
    try
      val f = settlementFixture(root); val good = settled(f, f.evidence); val s = good.assertions()
      def reject(record: B1ResumeRecord): Unit = assertThrows(classOf[Exception], () =>
        MySqlOrdinaryHandoffGraph.requireConfirmedHandoffData(f.handoff, record, root))
      reject(settled(f, f.evidence, protocol = "OTHER"))
      reject(settled(f, f.evidence, diagnostic = Some(Array.fill[Byte](32)(0))))
      reject(new B1ResumeRecord(4L, good.previousRecordSha256(), good.kind(), good.sourceAnchorSha256(),
        good.originalReceipt(), good.barrierNamespace(), s))
      reject(new B1ResumeRecord(3L, Array.fill[Byte](32)(0), good.kind(), good.sourceAnchorSha256(),
        good.originalReceipt(), good.barrierNamespace(), s))
      reject(new B1ResumeRecord(3L, good.previousRecordSha256(), good.kind(), good.sourceAnchorSha256(),
        good.originalReceipt(), good.barrierNamespace(), new B1ResumeRecord.FullStateAssertions(s.currentOwner(), null,
          s.retirement(), s.originalPrepareWitness(), s.currentWriter(), s.targetWriter(), null, s.latestSettlement(), s.marker(), s.acknowledgedCertifyWitness())))
      root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "settlement cumulative root exhaustion")
      assertThrows(classOf[Exception], () => MySqlOrdinaryHandoffGraph.requireConfirmedHandoffData(f.handoff, good, root))
    finally root.closeRootReadBudget()

  @Test def recallHistoricalHandoffCannotUseSelfConsistentButIllegalWriterStep(): Unit =
    val previous = new B1ResumeRecord.NativeOwnerAssertions(1L, 1L, id(90), 1L)
    val owner = new B1ResumeRecord.NativeOwnerAssertions(2L, 2L, id(91), 2L)
    val graph = new B1ResumeRecord.CommonGraphAssertions("DATA", Array[Byte](1), "DATA", Array[Byte](2), "DATA", Array[Byte](3))
    def check(epoch: Long, beforeOwner: java.util.UUID, nextOwner: java.util.UUID, lease: LocalDateTime): Unit =
      val before = new B1ResumeRecord.WriterAssertions(id(2), 7L, beforeOwner, oldLease, null)
      val target = new B1ResumeRecord.WriterAssertions(id(2), epoch, nextOwner, lease, null)
      val context = new B1ResumeRecord.OperationAssertions(id(91), epoch, id(2), id(92),
        java.util.List.of(new B1ResumeRecord.EffectAssertions("certified-ordinary/writer", 7L, Array.fill[Byte](32)(1))), Array.fill[Byte](32)(2))
      val attempt = new B1ResumeRecord.AttemptAssertions(id(93), B1ResumeRecord.Phase.HANDOFF, id(92), 2L,
        id(94), 1L, "fpb1:" + ("A" * 43), context, before, target, graph, graph)
      val record = new B1ResumeRecord(1L, Array.fill[Byte](32)(0), B1ResumeRecord.Kind.HANDOFF_PREPARED,
        Array.fill[Byte](32)(0), Array[Byte](1), "fpb1:" + ("A" * 43),
        new B1ResumeRecord.FullStateAssertions(owner, previous, null, null, before, target, attempt, null, null, null))
      MySqlCertifiedRecoveryInspectionActor.requireRecallHistoricalWriterStep(attempt, record)
    check(8L, id(90), id(91), newLease)
    check(8L, null, id(91), newLease)
    assertThrows(classOf[IllegalArgumentException], () => check(9L, id(90), id(91), newLease))
    assertThrows(classOf[IllegalArgumentException], () => check(8L, id(99), id(91), newLease))
    assertThrows(classOf[IllegalArgumentException], () => check(8L, id(90), id(99), newLease))
    assertThrows(classOf[IllegalArgumentException], () => check(8L, id(90), id(91), oldLease))

  private def writer(epoch: Long, owner: Option[java.util.UUID], lease: LocalDateTime,
      operation: Option[java.util.UUID] = None, fingerprint: Option[Array[Byte]] = None): Vector[MySqlFormat3Cell] =
    row("fp_runtime_writer",
      "singleton_fixed1" -> i(1),
      "binding_uuid" -> u(id(2)),
      "writer_epoch" -> i(epoch),
      "owner_boot_uuid" -> owner.map(u).getOrElse(MySqlFormat3Cell.SqlNull),
      "lease_until" -> MySqlFormat3Cell.DateTime(lease),
      "last_commit_operation_uuid" -> operation.map(u).getOrElse(MySqlFormat3Cell.SqlNull),
      "last_commit_fingerprint" -> fingerprint.map(binary).getOrElse(MySqlFormat3Cell.SqlNull))

  private def completeFull20(w: Vector[MySqlFormat3Cell], yaw: Double = 1.25): MySqlFormat3FullSnapshot =
    snapshot(extra = Map(
      "fp_runtime_writer" -> Vector(w),
      "floating_pod_state" -> Vector(pod(yaw = yaw))))

  @Test def actualFull20AllowsOnlyThePlannedWriterRowToChange(): Unit =
    val before = completeFull20(writer(7L, None, oldLease))
    val target = completeFull20(writer(8L, Some(id(91)), newLease))
    assertDoesNotThrow((() => MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(before, target)): Executable)
    assertNotEquals(before.relation("fp_runtime_writer"), target.relation("fp_runtime_writer"))
    assertEquals(before.relations.filterNot(_.table == "fp_runtime_writer"),
      target.relations.filterNot(_.table == "fp_runtime_writer"))

    val changedBusinessRow = completeFull20(writer(8L, Some(id(91)), newLease), yaw = 2.0)
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(before, changedBusinessRow))

  @Test def historicalWriterWithNoReceiptRequiresBothActualSqlColumnsNull(): Unit =
    val emptySlot = completeFull20(writer(7L, None, oldLease))
    val expected = new B1ResumeRecord.WriterAssertions(id(2), 7L, null, oldLease, null)
    assertDoesNotThrow((() => MySqlOrdinaryHandoffGraph.requireHistoricalWriter(emptySlot, expected)): Executable)

    val inventedSqlSlot = completeFull20(writer(7L, None, oldLease,
      operation = Some(id(92)), fingerprint = Some(Array.fill[Byte](32)(3))))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlOrdinaryHandoffGraph.requireHistoricalWriter(inventedSqlSlot, expected))

  @Test def changedActualReceiptCannotReplaceNativeSlotWithoutOriginalActorProof(): Unit =
    val hash = Array.fill[Byte](32)(3)
    val slot = new B1ResumeRecord.ReceiptSlotAssertions(id(91), 7L, id(2), id(92), hash)
    val old = MySqlBusinessWriterRow(id(2), 7L, Some(id(91)), oldLease, Some(id(92)), Some(hash.toVector), newLease)
    val assertion = new B1ResumeRecord.WriterAssertions(id(2), 7L, id(91), oldLease, slot)
    val owner = new B1ResumeRecord.NativeOwnerAssertions(1234L, 1L, id(91), 1L)
    // 这里只给selector caller DATA；不伪造可publish的native record/原Actor proof。
    val record = new B1ResumeRecord(1L, Array.fill[Byte](32)(0), B1ResumeRecord.Kind.DB_ACQUIRED,
      Array.fill[Byte](32)(0), Array[Byte](1), "fpb1:" + ("A" * 43),
      new B1ResumeRecord.FullStateAssertions(owner, null, null, null, assertion, assertion, null, null, null, null))
    val image = completeFull20(writer(7L, Some(id(91)), oldLease, Some(id(92)), Some(hash)))
    try
      assertSame(slot, MySqlOrdinaryHandoffGraph.selectWriterReceipt(image, old, record, image.allocationBudget, None).get)
      val changed = old.copy(lastOperationUuid = Some(id(93)))
      assertFalse(MySqlOrdinaryHandoffGraph.originalSlotMatches(changed, Some(slot)))
      assertThrows(classOf[IllegalStateException], () =>
        MySqlOrdinaryHandoffGraph.selectWriterReceipt(image, changed, record, image.allocationBudget, None))
      assertFalse(MySqlOrdinaryHandoffGraph.originalSlotMatches(old.copy(lastFingerprint = Some(Vector.fill(32)(4.toByte))), Some(slot)))
      assertFalse(MySqlOrdinaryHandoffGraph.originalSlotMatches(old, None))
      assertTrue(MySqlOrdinaryHandoffGraph.originalSlotMatches(old.copy(lastOperationUuid = None, lastFingerprint = None), None))
    finally image.allocationBudget.closeRootReadBudget()
