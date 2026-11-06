package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, WindowsWorldProvisioner}
import com.wi.floatingpod.storage.PhysicalRecoveryBinding
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.util.{Arrays, UUID}

/** The only ordinary SQL delta is an exact W owner/epoch/lease update; all B1 and business rows stay byte-for-byte typed. */
private[mysql] object MySqlOrdinaryHandoffGraph:
  private val EffectDomain = "floatingpod.mysql.certified-ordinary-handoff-writer-delta/v1".getBytes(StandardCharsets.US_ASCII)
  private val CommitDomain = "FP_MYSQL_ORDINARY_W_CAS_COMMIT_READBACK_V1"
  private val CommitProtocol = "MYSQL_SAME_CID_NULL_SAFE_OCCUPIED_W_CAS_COMMIT_IDLE_TARGET_READBACK_V1"

  /** Historical DATA only. Actual native custody and full20 comparison stay with the original actor. */
  final class ConfirmedHandoffData private[MySqlOrdinaryHandoffGraph] (
      val handoff: B1ResumeRecord, val settled: B1ResumeRecord,
      val before: MySqlBusinessWriterRow, val target: MySqlBusinessWriterRow,
      val actual: MySqlBusinessWriterRow)

  /** Same existing production wire format; neither this encoder nor its bytes issue SQL authority. */
  def commitEvidence(attempt: B1ResumeRecord.AttemptAssertions, before: MySqlBusinessWriterRow,
      target: MySqlBusinessWriterRow, actual: MySqlBusinessWriterRow, root: MySqlFormat3ReadBudget): Array[Byte] =
    require(attempt != null && before != null && target != null && actual != null && root != null &&
      !root.isClosed && !root.isScopedReadBudget, "original bounded commit evidence inputs required")
    root.reserveRetainedMemory(32768L, "ordinary commit evidence encoding and complete defensive fingerprint")
    val output = new ByteArrayOutputStream()
    val out = new DataOutputStream(output)
    def text(value: String): Unit =
      val bytes = value.getBytes(StandardCharsets.UTF_8)
      require(bytes.nonEmpty && bytes.length <= 4096, "bounded commit evidence text required")
      out.writeInt(bytes.length); out.write(bytes)
    def time(value: LocalDateTime): Unit =
      require(value != null && value.getNano % 1000 == 0, "actual commit DATETIME(6) required")
      out.writeLong(value.toLocalDate.toEpochDay); out.writeLong(value.toLocalTime.toNanoOfDay)
    try
      text(CommitDomain)
      out.writeLong(attempt.connectionId()); writeUuid(out, attempt.connectionGeneration())
      writeUuid(out, attempt.context().boot()); out.writeLong(attempt.bootGeneration())
      writeUuid(out, attempt.attemptId()); writeUuid(out, attempt.logicalWriteUuid()); text(attempt.namespace())
      Vector(before, target, actual).foreach { writer => writeWriter(out, writer); time(writer.databaseNow) }
      val fingerprint = attempt.context().originalOperationFingerprint()
      out.writeInt(fingerprint.length); out.write(fingerprint); out.flush()
      val bytes = output.toByteArray
      require(bytes.nonEmpty && bytes.length <= 65536, "bounded ordinary commit evidence required")
      bytes
    finally out.close()

  def requireConfirmedHandoffData(handoff: B1ResumeRecord, settled: B1ResumeRecord,
      root: MySqlFormat3ReadBudget): ConfirmedHandoffData =
    require(handoff != null && settled != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "original open handoff settlement DATA required")
    root.reserveRetainedMemory(2097152L, "complete bounded settlement/attempt/record copies and strict evidence decoding")
    val attempt = handoff.assertions().pendingAttempt()
    MySqlCertifiedRecoveryInspectionActor.requireRecallHistoricalWriterStep(attempt, handoff)
    val settlement = settled.assertions().latestSettlement()
    require(settled.kind() == B1ResumeRecord.Kind.DB_ACQUIRED && settlement != null &&
      settlement.kind() == B1ResumeRecord.SettlementKind.CONFIRMED && settlement.proofProtocol() == CommitProtocol &&
      Arrays.equals(B1ResumeRecordCanonical.encodeAttemptData(attempt),
        B1ResumeRecordCanonical.encodeAttemptData(settlement.settledAttempt())) &&
      Arrays.equals(settlement.originalUnknownDiagnosticIdentity(), attempt.context().originalOperationFingerprint()),
      "original confirmed settlement protocol/attempt/context differs")
    val evidence = settlement.fullOriginalEvidence()
    val old = handoff.assertions()
    val expected = new B1ResumeRecord(Math.incrementExact(handoff.sequence()),
      WindowsWorldProvisioner.digest(B1ResumeRecordCanonical.encode(handoff)), B1ResumeRecord.Kind.DB_ACQUIRED,
      handoff.sourceAnchorSha256(), handoff.originalReceipt(), handoff.barrierNamespace(),
      new B1ResumeRecord.FullStateAssertions(old.currentOwner(), old.previousOwner(), old.retirement(),
        old.originalPrepareWitness(), attempt.targetWriter(), attempt.targetWriter(), null, settlement,
        old.marker(), old.acknowledgedCertifyWitness()))
    require(Arrays.equals(B1ResumeRecordCanonical.encode(expected), B1ResumeRecordCanonical.encode(settled)),
      "complete original DB_ACQUIRED transition differs from its exact HANDOFF")
    val in = new DataInputStream(new ByteArrayInputStream(evidence))
    def bytes(length: Int): Array[Byte] =
      require(length >= 0 && length <= in.available(), "truncated original commit evidence")
      val result = new Array[Byte](length); in.readFully(result); result
    def expectText(value: String): Unit =
      val expected = value.getBytes(StandardCharsets.UTF_8)
      require(in.readInt() == expected.length && Arrays.equals(bytes(expected.length), expected),
        "original commit evidence domain/namespace differs")
    def uuid(): UUID = new UUID(in.readLong(), in.readLong())
    def option[A](read: => A): Option[A] = in.readUnsignedByte() match
      case 0 => None
      case 1 => Some(read)
      case _ => throw new IllegalArgumentException("noncanonical original commit evidence option")
    def time(): LocalDateTime =
      val result = LocalDateTime.of(LocalDate.ofEpochDay(in.readLong()), LocalTime.ofNanoOfDay(in.readLong()))
      require(result.getYear >= 1000 && result.getYear <= 9999 && result.getNano % 1000 == 0,
        "original commit evidence DATETIME(6) differs")
      result
    def writer(): MySqlBusinessWriterRow =
      val binding = uuid(); val epoch = in.readLong(); val owner = option(uuid()); val lease = time()
      val operation = option(uuid())
      val fingerprint = option { require(in.readInt() == 32, "original W fingerprint length differs"); bytes(32).toVector }
      MySqlBusinessWriterRow(binding, epoch, owner, lease, operation, fingerprint, time())
    def matches(row: MySqlBusinessWriterRow, expected: B1ResumeRecord.WriterAssertions): Boolean =
      val slot = Option(expected.lastSlot())
      row.bindingUuid == expected.binding() && row.writerEpoch == expected.epoch() &&
        row.ownerBootUuid == Option(expected.ownerBoot()) && row.leaseUntil == expected.leaseUntil() &&
        row.lastOperationUuid == slot.map(_.operation()) && row.lastFingerprint == slot.map(_.fingerprint().toVector)
    try
      expectText(CommitDomain)
      require(in.readLong() == attempt.connectionId() && uuid() == attempt.connectionGeneration() &&
        uuid() == attempt.context().boot() && in.readLong() == attempt.bootGeneration() &&
        uuid() == attempt.attemptId() && uuid() == attempt.logicalWriteUuid(), "original commit evidence session/attempt differs")
      expectText(attempt.namespace())
      val before = writer(); val target = writer(); val actual = writer()
      require(in.readInt() == 32 && Arrays.equals(bytes(32), attempt.context().originalOperationFingerprint()) && in.read() == -1,
        "original commit evidence fingerprint or trailing bytes differ")
      require(matches(before, attempt.beforeWriter()) && matches(target, attempt.targetWriter()) &&
        actual.copy(databaseNow = target.databaseNow) == target && target.databaseNow == before.databaseNow &&
        (before.ownerBootUuid.isEmpty || !before.leaseUntil.isAfter(before.databaseNow)) &&
        target.leaseUntil.isAfter(before.databaseNow) && actual.leaseUntil.isAfter(actual.databaseNow),
        "complete original W commit/readback or lease boundary differs")
      require(Arrays.equals(evidence, commitEvidence(attempt, before, target, actual, root)),
        "noncanonical original commit evidence")
      new ConfirmedHandoffData(handoff, settled, before, target, actual)
    finally in.close()

  final case class Draft(
      beforeSnapshot: MySqlFormat3FullSnapshot,
      targetSnapshot: MySqlFormat3FullSnapshot,
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlBusinessWriterRow,
      beforeWriterAssertions: B1ResumeRecord.WriterAssertions,
      targetWriterAssertions: B1ResumeRecord.WriterAssertions,
      context: MySqlOperationContext,
      beforeGraph: MySqlB1CommonGraph,
      targetGraph: MySqlB1CommonGraph,
      transition: B1ResumeRecord,
      canonicalBytes: Array[Byte],
      targetParity: Int,
      leaseDeadline: LocalDateTime
  )

  def plan(before: MySqlFormat3FullSnapshot, manifest: MySqlLockedManifest,
      binding: Option[PhysicalRecoveryBinding], writer: MySqlBusinessWriterRow,
      currentRecord: B1ResumeRecord, sourceIdentity: String,
      currentBoot: UUID, bootGeneration: Long, connectionGeneration: UUID, connectionId: Long,
      handoffAttempt: UUID, handoffOperation: UUID, namespace: String,
      previousOwner: B1ResumeRecord.NativeOwnerAssertions, targetOwner: B1ResumeRecord.NativeOwnerAssertions,
      originalOwnerBytes: Array[Byte], leaseDeadline: LocalDateTime,
      root: MySqlFormat3ReadBudget, inputs: MySqlFormat3AuditInputs,
      receiptContinuation: Option[MySqlCertifiedRecoveryInspectionActor.OriginalOrdinaryReceiptContinuation] = None): Draft =
    require(before != null && manifest != null && binding != null && binding.forall(_ != null) && writer != null &&
      currentRecord != null && sourceIdentity != null && sourceIdentity.nonEmpty && currentBoot != null &&
      bootGeneration > 0L && connectionGeneration != null && connectionId > 0L && handoffAttempt != null &&
      handoffOperation != null && namespace != null && previousOwner != null && targetOwner != null &&
      originalOwnerBytes != null && originalOwnerBytes.nonEmpty && leaseDeadline != null && root != null && !root.isClosed && inputs != null,
      "complete actual ordinary before image, native owner facts, IDs and original allocation root required")
    require((before.limitProfile eq root.profile) && before.allocationBudget.sharesAllocationLedger(root),
      "ordinary handoff graph must use the original actor profile/ledger")
    val original = currentRecord
    val prior = original.assertions()
    require(original.kind() == B1ResumeRecord.Kind.CERTIFIED || original.kind() == B1ResumeRecord.Kind.DB_ACQUIRED,
      "ordinary HANDOFF requires a settled original CERTIFIED or DB_ACQUIRED source record")
    require(prior.pendingAttempt() == null && prior.originalPrepareWitness() != null &&
      prior.acknowledgedCertifyWitness() != null && prior.currentWriter() != null && prior.targetWriter() != null,
      "ordinary source must retain settled PREPARE/CERTIFY history and have no unresolved attempt")
    require(prior.latestSettlement() == null || prior.latestSettlement().kind() == B1ResumeRecord.SettlementKind.CONFIRMED,
      "ordinary source cannot advance from a cancelled/unknown native settlement")
    require(prior.currentOwner() != null && prior.currentOwner().equals(previousOwner),
      "actual old process guard owner must equal the settled native current owner")
    require(original.barrierNamespace().equals(namespace) && namespace.matches("fpb1:[A-Za-z0-9_-]{43}"),
      "ordinary source namespace must be the same held native/MySQL barrier")
    require(writer.bindingUuid == currentRecord.assertions().currentWriter().binding() &&
      writer.bindingUuid == manifest.bindingUuid && binding.exists(_.bindingUuid == writer.bindingUuid) &&
      writer.writerEpoch >= currentRecord.assertions().currentWriter().epoch(),
      "actual current W binding/epoch must remain on the original CERTIFIED source")
    require(leaseDeadline.isAfter(writer.databaseNow) && leaseDeadline.getNano % 1000 == 0,
      "ordinary target lease must be future database time with DATETIME(6) precision")
    require(targetOwner.boot().equals(currentBoot) && targetOwner.generation() == bootGeneration,
      "actual current target owner must match the explicit ordinary boot identity")

    root.reserveRetainedMemory(Math.addExact(8192L, Math.multiplyExact(before.relations.size.toLong, 128L)),
      "ordinary exact full20 target writer row and immutable before/target graph structures before encoding")
    val targetEpoch = Math.addExact(writer.writerEpoch, 1L)
    val targetWriter = writer.copy(writerEpoch = targetEpoch, ownerBootUuid = Some(currentBoot), leaseUntil = leaseDeadline)
    val retainedSlot = selectWriterReceipt(before, writer, currentRecord, root, receiptContinuation)
    val beforeAssertion = writerAssertion(writer, retainedSlot, root)
    val targetAssertion = writerAssertion(targetWriter, retainedSlot, root)
    val effect = effectFingerprint(writer, targetWriter, handoffAttempt, handoffOperation, root)
    val context = MySqlOperationContext(currentBoot, targetEpoch, writer.bindingUuid, handoffOperation,
      Vector(MySqlExpectedEffect("certified-ordinary/writer", writer.writerEpoch, effect)))
    val beforeRepository = MySqlFreshFormat3TransitionProjection.project(before)
    val targetFull20 = targetSnapshot(before, targetWriter, root)
    val targetRepository = MySqlFreshFormat3TransitionProjection.project(targetFull20)
    require(beforeRepository.exactlyEquals(targetRepository),
      "ordinary handoff must preserve the complete B1 and user repository projection")
    requireOnlyWriterChanged(before, targetFull20)
    val beforeCommon = MySqlB1CommonGraph.captureFormat3(manifest, binding, beforeRepository, before, inputs)
    val targetCommon = MySqlB1CommonGraph.captureFormat3(manifest, binding, targetRepository, targetFull20, inputs)
    val operation = new B1ResumeRecord.OperationAssertions(currentBoot, targetEpoch, writer.bindingUuid, handoffOperation,
      java.util.List.of(new B1ResumeRecord.EffectAssertions("certified-ordinary/writer", writer.writerEpoch, effect.toArray)),
      context.canonicalFingerprintSha256.toArray)
    val attempt = new B1ResumeRecord.AttemptAssertions(handoffAttempt, B1ResumeRecord.Phase.HANDOFF,
      handoffOperation, bootGeneration, connectionGeneration, connectionId, namespace, operation,
      beforeAssertion, targetAssertion, beforeCommon.toResumeAssertions, targetCommon.toResumeAssertions)
    val state = new B1ResumeRecord.FullStateAssertions(targetOwner, previousOwner,
      new B1ResumeRecord.RetirementAssertions("WIN32_PREVIOUS_PROCESS_EXIT_AND_SAME_NAMESPACE_SOURCE_BARRIER_V1", originalOwnerBytes),
      prior.originalPrepareWitness(), beforeAssertion, targetAssertion, attempt, prior.latestSettlement(),
      prior.marker(), prior.acknowledgedCertifyWitness())
    val sequence = Math.addExact(original.sequence(), 1L)
    val oldTransitionBytes = B1ResumeRecordCanonical.encode(original)
    val transition = new B1ResumeRecord(sequence, WindowsWorldProvisioner.digest(oldTransitionBytes),
      B1ResumeRecord.Kind.HANDOFF_PREPARED, original.sourceAnchorSha256(), original.originalReceipt(),
      original.barrierNamespace(), state)
    require(transition.sequence() == Math.addExact(original.sequence(), 1L) &&
      java.util.Arrays.equals(transition.previousRecordSha256(), WindowsWorldProvisioner.digest(oldTransitionBytes)),
      "ordinary HANDOFF transition must preserve exact predecessor sequence/hash")
    val bytes = B1ResumeRecordCanonical.encode(transition)
    require(bytes.length <= 65_536, "ordinary HANDOFF canonical V4 transition exceeds the existing exact record bound")
    Draft(before, targetFull20, writer, targetWriter, beforeAssertion, targetAssertion, context, beforeCommon, targetCommon,
      transition, bytes, (sequence & 1L).toInt, leaseDeadline)

  /** The V5 continuation target changes only W. The artifact identity is supplied only after its actual durable ACK. */
  def settledTransition(plan: Draft, actualHandoff: B1ResumeRecord,
      actualCommitEvidence: Array[Byte], originalUnknownDiagnosticIdentity: Array[Byte]): B1ResumeRecord =
    require(plan != null && actualHandoff != null && actualCommitEvidence != null && actualCommitEvidence.nonEmpty &&
      actualCommitEvidence.length <= 65_536 && originalUnknownDiagnosticIdentity != null &&
      originalUnknownDiagnosticIdentity.length == 32,
      "actual commit/readback evidence and exact unknown-diagnostic identity are required")
    require(plan.transition.sequence() == actualHandoff.sequence() &&
      java.util.Arrays.equals(B1ResumeRecordCanonical.encode(plan.transition), B1ResumeRecordCanonical.encode(actualHandoff)) &&
      java.util.Arrays.equals(plan.transition.sourceAnchorSha256(), actualHandoff.sourceAnchorSha256()) &&
      java.util.Arrays.equals(plan.transition.originalReceipt(), actualHandoff.originalReceipt()) &&
      plan.transition.barrierNamespace().equals(actualHandoff.barrierNamespace()),
      "settled ordinary transition must continue the exact actual HANDOFF record")
    val attempt = plan.transition.assertions().pendingAttempt()
    val state = new B1ResumeRecord.FullStateAssertions(plan.transition.assertions().currentOwner(),
      plan.transition.assertions().previousOwner(), plan.transition.assertions().retirement(),
      plan.transition.assertions().originalPrepareWitness(), plan.targetWriterAssertions, plan.targetWriterAssertions,
      null, new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED, attempt,
        "MYSQL_SAME_CID_NULL_SAFE_OCCUPIED_W_CAS_COMMIT_IDLE_TARGET_READBACK_V1",
        actualCommitEvidence, originalUnknownDiagnosticIdentity),
      plan.transition.assertions().marker(), plan.transition.assertions().acknowledgedCertifyWitness())
    val oldTransitionBytes = B1ResumeRecordCanonical.encode(plan.transition)
    val next = new B1ResumeRecord(Math.addExact(plan.transition.sequence(), 1L),
      WindowsWorldProvisioner.digest(oldTransitionBytes), B1ResumeRecord.Kind.DB_ACQUIRED,
      plan.transition.sourceAnchorSha256(), plan.transition.originalReceipt(), plan.transition.barrierNamespace(), state)
    require(next.sequence() == Math.addExact(plan.transition.sequence(), 1L) &&
      java.util.Arrays.equals(next.previousRecordSha256(), WindowsWorldProvisioner.digest(oldTransitionBytes)),
      "ordinary DB_ACQUIRED transition must preserve exact HANDOFF sequence/hash")
    next

  private[mysql] def originalSlotMatches(writer: MySqlBusinessWriterRow,
      slot: Option[B1ResumeRecord.ReceiptSlotAssertions]): Boolean =
    slot match
      case Some(value) => value.binding() == writer.bindingUuid && value.writerEpoch() <= writer.writerEpoch &&
        writer.lastOperationUuid.contains(value.operation()) &&
        writer.lastFingerprint.exists(fp => java.util.Arrays.equals(fp.toArray, value.fingerprint()))
      case None => writer.lastOperationUuid.isEmpty && writer.lastFingerprint.isEmpty

  private[mysql] def selectWriterReceipt(before: MySqlFormat3FullSnapshot, writer: MySqlBusinessWriterRow,
      currentRecord: B1ResumeRecord, root: MySqlFormat3ReadBudget,
      continuation: Option[MySqlCertifiedRecoveryInspectionActor.OriginalOrdinaryReceiptContinuation]): Option[B1ResumeRecord.ReceiptSlotAssertions] =
    require(before != null && writer != null && currentRecord != null && root != null && continuation != null,
      "exact original handoff receipt inputs required")
    val original = Option(currentRecord.assertions().currentWriter().lastSlot())
    if originalSlotMatches(writer, original) then
      require(continuation.isEmpty, "matching original slot cannot consume another receipt continuation")
      original
    else
      val proof = continuation.getOrElse(throw new IllegalStateException(
        "changed actual W receipt requires original actor continuation; DATA cannot replace the native slot"))
      Some(proof.consume(before, writer, currentRecord, root))

  private def writerAssertion(writer: MySqlBusinessWriterRow,
      retainedSlot: Option[B1ResumeRecord.ReceiptSlotAssertions], root: MySqlFormat3ReadBudget): B1ResumeRecord.WriterAssertions =
    // HANDOFF不创造逻辑receipt。这里保留原native slot或原actor核全证据接续的实际slot；
    // SQL W的两个receipt列仍须精确一致，不能借接续proof略过最终比较。
    root.reserveRetainedMemory(512L, "ordinary original W receipt slot and defensive fingerprint before allocation")
    retainedSlot match
      case Some(slot) =>
        require(slot.binding() == writer.bindingUuid && slot.writerEpoch() <= writer.writerEpoch &&
          writer.lastOperationUuid.contains(slot.operation()) &&
          writer.lastFingerprint.exists(value => java.util.Arrays.equals(value.toArray, slot.fingerprint())),
          "ordinary W actual last-commit columns differ from the complete preserved source slot")
      case None =>
        require(writer.lastOperationUuid.isEmpty && writer.lastFingerprint.isEmpty,
          "ordinary settled source lacks the original slot for the actual W last-commit columns")
    new B1ResumeRecord.WriterAssertions(writer.bindingUuid, writer.writerEpoch, writer.ownerBootUuid.orNull,
      writer.leaseUntil, retainedSlot.orNull)

  private[mysql] def effectFingerprint(before: MySqlBusinessWriterRow, target: MySqlBusinessWriterRow,
      attempt: UUID, operation: UUID, root: MySqlFormat3ReadBudget): Vector[Byte] =
    effectFingerprintData(before,target,attempt,operation,root)

  private[mysql] def effectFingerprint(before:B1ResumeRecord.WriterAssertions,target:B1ResumeRecord.WriterAssertions,
      attempt:UUID,operation:UUID,root:MySqlFormat3ReadBudget):Vector[Byte] =
    effectFingerprintData(before,target,attempt,operation,root)

  private def effectFingerprintData(before:MySqlBusinessWriterRow | B1ResumeRecord.WriterAssertions,
      target:MySqlBusinessWriterRow | B1ResumeRecord.WriterAssertions,
      attempt:UUID,operation:UUID,root:MySqlFormat3ReadBudget):Vector[Byte] =
    root.reserveRetainedMemory(4096L, "ordinary canonical exact W delta and effect fingerprint before allocation")
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try
      out.writeInt(EffectDomain.length); out.write(EffectDomain)
      writeWriter(out, before); writeWriter(out, target)
      out.writeLong(attempt.getMostSignificantBits); out.writeLong(attempt.getLeastSignificantBits)
      out.writeLong(operation.getMostSignificantBits); out.writeLong(operation.getLeastSignificantBits)
      out.flush()
      MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray).toVector
    finally out.close()

  private def writeWriter(out: DataOutputStream, writer: MySqlBusinessWriterRow): Unit =
    writeUuid(out, writer.bindingUuid); out.writeLong(writer.writerEpoch)
    writer.ownerBootUuid match
      case None => out.writeBoolean(false)
      case Some(value) => out.writeBoolean(true); writeUuid(out, value)
    out.writeLong(writer.leaseUntil.toLocalDate.toEpochDay); out.writeLong(writer.leaseUntil.toLocalTime.toNanoOfDay)
    writer.lastOperationUuid match
      case None => out.writeBoolean(false)
      case Some(value) => out.writeBoolean(true); writeUuid(out, value)
    writer.lastFingerprint match
      case None => out.writeBoolean(false)
      case Some(value) => out.writeBoolean(true); out.writeInt(value.length); value.foreach(byte => out.writeByte(byte.toInt))

  private def writeWriter(out:DataOutputStream,writer:MySqlBusinessWriterRow | B1ResumeRecord.WriterAssertions):Unit = writer match
    case value:MySqlBusinessWriterRow => writeWriter(out,value)
    case value:B1ResumeRecord.WriterAssertions =>
      writeUuid(out,value.binding());out.writeLong(value.epoch())
      if value.ownerBoot()==null then out.writeBoolean(false)
      else
        out.writeBoolean(true);writeUuid(out,value.ownerBoot())
      out.writeLong(value.leaseUntil().toLocalDate.toEpochDay);out.writeLong(value.leaseUntil().toLocalTime.toNanoOfDay)
      val slot=value.lastSlot()
      if slot==null then
        out.writeBoolean(false);out.writeBoolean(false)
      else
        out.writeBoolean(true);writeUuid(out,slot.operation())
        val fingerprint=slot.fingerprint()
        out.writeBoolean(true);out.writeInt(fingerprint.length);out.write(fingerprint)
    case null => throw new IllegalArgumentException("complete original HANDOFF writer DATA required")

  private def writeUuid(out: DataOutputStream, value: UUID): Unit =
    out.writeLong(value.getMostSignificantBits); out.writeLong(value.getLeastSignificantBits)

  private def targetSnapshot(before: MySqlFormat3FullSnapshot, writer: MySqlBusinessWriterRow,
      root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    val original = before.relation("fp_runtime_writer")
    require(original.rows.size == 1 && original.rows.head.size == 7, "exact singleton full20 W row required")
    val shape = MySqlFormat3RelationShapes.forTable("fp_runtime_writer")
    val byName = Map(
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> binaryUuid(writer.bindingUuid, root, "ordinary target W binding UUID"),
      "writer_epoch" -> MySqlFormat3Cell.SignedInteger(writer.writerEpoch),
      "owner_boot_uuid" -> writer.ownerBootUuid.map(value => binaryUuid(value, root, "ordinary target W owner UUID")).getOrElse(MySqlFormat3Cell.SqlNull),
      "lease_until" -> MySqlFormat3Cell.DateTime(writer.leaseUntil),
      "last_commit_operation_uuid" -> writer.lastOperationUuid.map(value => binaryUuid(value, root, "preserved ordinary target W slot operation")).getOrElse(MySqlFormat3Cell.SqlNull),
      "last_commit_fingerprint" -> writer.lastFingerprint.map(value => binary(value.toArray, root, "preserved ordinary target W slot fingerprint")).getOrElse(MySqlFormat3Cell.SqlNull)
    )
    val row = shape.columns.map(column => byName.getOrElse(column.name,
      throw new IllegalStateException("ordinary target writer shape has an unhandled column: " + column.name)))
    val replacement = MySqlFormat3RelationSnapshot("fp_runtime_writer", Vector(row))
    val relations = before.relations.map(value => if value.table == replacement.table then replacement else value)
    val target = new MySqlFormat3FullSnapshot(before.schema, before.connection, before.observationScope,
      before.maxAllowedPacketBytes, before.limitProfile, relations, before.allocationBudget)
    val beforeW = before.relation("fp_runtime_writer").rows.head
    val targetW = target.relation("fp_runtime_writer").rows.head
    val operationIndex = shape.columns.indexWhere(_.name == "last_commit_operation_uuid")
    val fingerprintIndex = shape.columns.indexWhere(_.name == "last_commit_fingerprint")
    require(operationIndex >= 0 && fingerprintIndex >= 0 && beforeW(operationIndex) == targetW(operationIndex) &&
      beforeW(fingerprintIndex) == targetW(fingerprintIndex),
      "ordinary W handoff must preserve the actual last commit operation/fingerprint slot")
    target

  private def binaryUuid(value: UUID, root: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    root.reserveRetainedMemory(128L, label + " fixed UUID workspace before allocation")
    val bytes = java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
    binary(bytes, root, label)

  private def binary(value: Array[Byte], root: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.copyOf(value, root, label))

  private[mysql] def requireOnlyWriterChanged(before: MySqlFormat3FullSnapshot, target: MySqlFormat3FullSnapshot): Unit =
    require(before.relations.size == target.relations.size && before.relations.map(_.table) == target.relations.map(_.table),
      "ordinary target must preserve full20 relation set/order")
    before.relations.zip(target.relations).foreach { (left, right) =>
      if left.table == "fp_runtime_writer" then
        require(right.rows.size == 1 && right.rows.head.size == 7, "ordinary target must preserve W singleton shape")
      else require(left == right, "ordinary W target changed a non-W full20 relation: " + left.table)
    }

  def requireHistoricalWriter(snapshot: MySqlFormat3FullSnapshot,
      expected: B1ResumeRecord.WriterAssertions): Unit =
    require(snapshot != null && expected != null, "actual full20 handoff writer image and source assertion required")
    val rows = snapshot.relation("fp_runtime_writer").rows
    require(rows.size == 1 && rows.head.size == 7, "ordinary historical writer image must retain its exact singleton row")
    val shape = MySqlFormat3RelationShapes.forTable("fp_runtime_writer")
    val row = rows.head
    def cell(name: String): MySqlFormat3Cell =
      val index = shape.columns.indexWhere(_.name == name)
      require(index >= 0, "ordinary historical W column is in the frozen full20 shape")
      row(index)
    def uuid(name: String, value: UUID): Boolean = value != null && (cell(name) match
      case MySqlFormat3Cell.Binary(actual) => actual.equalsUuid(value)
      case _ => false)
    def ownerMatches: Boolean = Option(expected.ownerBoot()) match
      case None => cell("owner_boot_uuid") == MySqlFormat3Cell.SqlNull
      case Some(value) => uuid("owner_boot_uuid", value)
    require(cell("singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L) &&
      uuid("binding_uuid", expected.binding()) &&
      cell("writer_epoch") == MySqlFormat3Cell.SignedInteger(expected.epoch()) && ownerMatches &&
      cell("lease_until") == MySqlFormat3Cell.DateTime(expected.leaseUntil()),
      "ordinary historical W binding/epoch/owner/lease differs from its exact assertion")
    Option(expected.lastSlot()) match
      case Some(slot) =>
        require(uuid("last_commit_operation_uuid", slot.operation()) &&
          (cell("last_commit_fingerprint") match
            case MySqlFormat3Cell.Binary(actual) => actual.equalsBytes(slot.fingerprint())
            case _ => false),
          "ordinary historical W receipt slot differs from its complete assertion")
      case None =>
        require(cell("last_commit_operation_uuid") == MySqlFormat3Cell.SqlNull &&
          cell("last_commit_fingerprint") == MySqlFormat3Cell.SqlNull,
          "ordinary historical W must retain null SQL last-commit fields when its source has no receipt slot")
