package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{DbFirstBootstrapRecordCodec, DbFirstWorldAdoptionPlanData,
  DbFirstWorldAdoptionReceiptCodec}

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}

/** FirstBinding6 Full27 transition DATA verifier; it does not issue SQL, native ACKs, or Ready. */
private[mysql] object MySqlDbFirstBootstrapProof:
  import DbFirstBootstrapRecordCodec.{Kind, MarkerData, Outcome, Phase, RecordData, SettlementData,
    WitnessRef}

  private val WriterTable = "fp_runtime_writer"
  private val MarkerMagic = 0x46505742
  private val MarkerFormat = 2
  private val Full27Columns = 480

  /** Validated historical DATA plus the exact operation context derived from its typed graph. */
  final class ValidatedPhase private[MySqlDbFirstBootstrapProof] (
      val record: RecordData,
      val before: MySqlScriptSchemaSnapshot,
      val target: MySqlScriptSchemaSnapshot,
      val context: MySqlOperationContext,
      private[mysql] val source: DbFirstWorldAdoptionReceiptCodec.SourceFacts,
      private[mysql] val sourceReceiptSha256: String,
      private[mysql] val inputs: MySqlScriptSchemaInputs,
      private[mysql] val plan: DbFirstWorldAdoptionPlanData
  ):
    require(record != null && before != null && target != null && context != null && source != null &&
      sourceReceiptSha256 != null && inputs != null && plan != null,
      "validated FirstBinding6 phase must retain its complete original DATA lineage")
    override def toString: String = s"FirstBinding6ValidatedPhase[${record.phase()},Full27 DATA]"

  /**
    * Validates the exact source receipt, historical Full27 witnesses, owner scope, and one typed phase graph.
    * Only INTENT records are accepted here; an independent SettlementData must establish actual readback.
    */
  def validate(
      record: RecordData,
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      source: DbFirstWorldAdoptionReceiptCodec.SourceFacts,
      sourceReceiptSha256: String,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      expectedQuotaMaximumRecords: Long
  ): ValidatedPhase =
    if record == null || before == null || target == null || source == null || inputs == null || root == null then
      reject("complete phase record, historical Full27 images, source facts, inputs and original root are required")
    if record.kind() != Kind.INTENT then reject("phase validation requires the durable INTENT record; CONFIRMED needs separate actual SettlementData")
    val plan = source.plan()
    requireDbFirstPlan(plan, inputs)
    requireRoot(inputs, root)
    val exactSourceSha = sourceReceiptSha(source, root)
    if !isLowerSha256(sourceReceiptSha256) || sourceReceiptSha256 != exactSourceSha ||
        record.sourceReceiptSha256() != exactSourceSha || record.owner().sourceReceiptSha256() != exactSourceSha then
      reject("phase/source/owner receipt hashes differ from the exact canonical ExistingWorldAdoption6 DATA")
    if record.schemaFormat() != DbFirstBootstrapRecordCodec.SCHEMA_FORMAT ||
        record.snapshotSelector() != DbFirstBootstrapRecordCodec.SNAPSHOT_SELECTOR then
      reject("phase record does not select schema6 Full27 DATA")
    requireHistoricalPair(record, before, target, inputs, root, plan)
    requireWitness(record.beforeFull27(), before, "before")
    requireWitness(record.targetFull27(), target, "target")

    val beforeWriter = writer(before, record.databaseNow())
    val targetWriter = writer(target, record.databaseNow())
    val targetWriterData = freshTarget(targetWriter)
    val receiptBytes = shaBytes(exactSourceSha)
    val markerAck = validatePhaseMarker(record.phase(), record.marker(), source, plan)
    val sourceIdentity = source.selectedSaveFileId()
    record.phase() match
      case Phase.PREPARE | Phase.CERTIFY | Phase.HANDOFF =>
        if !targetWriter.ownerBootUuid.contains(record.owner().boot()) then
          reject(s"${record.phase()} target W owner differs from the phase owner DATA")
      case Phase.RELEASE =>
        if !beforeWriter.ownerBootUuid.contains(record.owner().boot()) then
          reject("RELEASE before W owner differs from the phase owner DATA")

    val context = record.phase() match
      case Phase.PREPARE =>
        if record.operationUuid() != plan.prepareWriteUuid() then
          reject("PREPARE intent operation UUID differs from the exact schema6 plan")
        MySqlDbFirstPrepareComposition.requirePrepareTransition(before, target, inputs, root, plan,
          sourceIdentity, receiptBytes, beforeWriter, targetWriterData, expectedQuotaMaximumRecords)
        MySqlDbFirstPrepareComposition.contextForPrepare(before, target, inputs, root, plan,
          sourceIdentity, receiptBytes, targetWriterData, expectedQuotaMaximumRecords)
      case Phase.CERTIFY =>
        if record.operationUuid() != plan.certifyWriteUuid() then
          reject("CERTIFY intent operation UUID differs from the exact schema6 plan")
        val markerSha = markerAck.getOrElse(reject("CERTIFY requires the actual marker DATA hash"))
        MySqlDbFirstBootstrapGraph.requireCertifyTransition(before, target, inputs, root, plan,
          sourceIdentity, receiptBytes, markerSha, beforeWriter, targetWriterData, expectedQuotaMaximumRecords)
        MySqlDbFirstBootstrapGraph.contextForTransition("CERTIFY", before, target, plan,
          sourceIdentity, receiptBytes, targetWriterData, expectedQuotaMaximumRecords)
      case Phase.HANDOFF =>
        val fence = fenceTarget(targetWriter)
        MySqlDbFirstBootstrapWriterGraph.requireHandoffTransition(before, target, inputs, root, plan,
          sourceIdentity, receiptBytes, markerAck, beforeWriter, fence, expectedQuotaMaximumRecords)
        transitionContext(record, beforeWriter, targetWriter, plan, target.allocationBudget, "HANDOFF")
      case Phase.RELEASE =>
        val markerSha = markerAck.getOrElse(reject("RELEASE requires the actual marker DATA hash"))
        MySqlDbFirstBootstrapWriterGraph.requireReleaseTransition(before, target, inputs, root, plan,
          sourceIdentity, receiptBytes, Some(markerSha), beforeWriter, targetWriter, expectedQuotaMaximumRecords)
        transitionContext(record, beforeWriter, targetWriter, plan, target.allocationBudget, "RELEASE")

    new ValidatedPhase(record, before, target, context, source, exactSourceSha, inputs, plan)

  /**
    * Independently validates the current observer's exact Full27 readback for an earlier INTENT.
    * Observer scope is intentionally compared to this actual snapshot, not to the historical images.
    */
  def validateSettlement(
      settlement: SettlementData,
      validatedOriginalIntent: ValidatedPhase,
      actual: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget
  ): Unit =
    if settlement == null || validatedOriginalIntent == null || actual == null || root == null then
      reject("complete independent settlement DATA, validated original INTENT, actual Full27 image and root are required")
    val inputs = validatedOriginalIntent.inputs
    val plan = validatedOriginalIntent.plan
    requireDbFirstPlan(plan, inputs)
    requireRoot(inputs, root)
    val original = validatedOriginalIntent.record
    if original.kind() != Kind.INTENT || settlement.intent().kind() != Kind.INTENT ||
        settlement.intent() != original then
      reject("settlement must retain the exact validated original INTENT record")
    if recordSha256(original, root) != settlement.intentSha256() then
      reject("settlement SHA differs from the exact original durable INTENT encoding")
    if settlement.observer().sourceReceiptSha256() != validatedOriginalIntent.sourceReceiptSha256 ||
        original.sourceReceiptSha256() != validatedOriginalIntent.sourceReceiptSha256 then
      reject("actual observer and original INTENT do not bind the validated source receipt")

    requireSnapshot(actual, inputs, root, "actual settlement")
    if actual.connection.serverUuid != plan.mysqlServerUuid() || actual.connection.catalog != plan.schema() ||
        actual.schema != plan.schema() then
      reject("actual Full27 observer header differs from the source plan server or schema")
    if actual.observationScope.bootUuid != settlement.observer().boot() ||
        actual.observationScope.bootGeneration != settlement.observer().generation() then
      reject("actual Full27 observation scope differs from the independent settlement observer owner")
    requireWitness(settlement.actualFull27(), actual, "actual settlement")
    // The actual observer has its own boot/connection header. Compare all 27 typed relations only.
    val expected = settlement.outcome() match
      case Outcome.APPLIED => validatedOriginalIntent.target
      case Outcome.NOT_APPLIED => validatedOriginalIntent.before
      case _ => reject("settlement outcome is outside the closed APPLIED/NOT_APPLIED DATA set")
    if actual.relations != expected.relations then
      reject(s"actual Full27 relations differ from the original ${settlement.outcome()} image")
    writer(actual, settlement.databaseNow())

  /** Strict parser shared with the Main actor: it reads only the singleton W row from a complete Full27 image. */
  private[mysql] def writer(
      snapshot: MySqlScriptSchemaSnapshot,
      databaseNow: LocalDateTime
  ): MySqlBusinessWriterRow =
    if snapshot == null || databaseNow == null then reject("actual Full27 snapshot and MySQL databaseNow are required to read W")
    if snapshot.inputs == null || snapshot.inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 ||
        snapshot.inputs.canonicalOrder.size != 27 || snapshot.inputs.canonicalOrder.map(_.columns.size).sum != Full27Columns ||
        snapshot.relations.map(_.table) != snapshot.inputs.canonicalOrder.map(_.table) then
      reject("W parser requires the exact complete Full27 canonical relation profile")
    if snapshot.allocationBudget == null || snapshot.allocationBudget.isClosed then
      reject("W parser requires a live retained Full27 DATA owner")
    snapshot.allocationBudget.requireScriptDataInputs(snapshot.inputs)
    if databaseNow.getNano % 1000 != 0 then reject("W parser databaseNow must have exact DATETIME(6) precision")
    val shape = snapshot.inputs.forTable(WriterTable)
    val rows = snapshot.relation(WriterTable).rows
    val row = rows match
      case Vector(single) if single.size == shape.columns.size => single
      case _ => reject("actual Full27 writer relation must contain exactly one complete row")
    def field(name: String): MySqlFormat3Cell =
      val index = shape.columns.indexWhere(_.name == name)
      if index < 0 then reject(s"Full27 W schema lacks the frozen $name column")
      row(index)
    val binding = uuid(field("binding_uuid"), "W.binding_uuid")
    val epoch = field("writer_epoch") match
      case MySqlFormat3Cell.SignedInteger(value) if value >= 0L => value
      case _ => reject("actual Full27 W.writer_epoch must be a nonnegative typed integer")
    val owner = optionalUuid(field("owner_boot_uuid"), "W.owner_boot_uuid")
    val lease = field("lease_until") match
      case MySqlFormat3Cell.DateTime(value) if value.getNano % 1000 == 0 => value
      case _ => reject("actual Full27 W.lease_until must be a DATETIME(6) value")
    val operation = optionalUuid(field("last_commit_operation_uuid"), "W.last_commit_operation_uuid")
    val fingerprint = field("last_commit_fingerprint") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(value) if value.length == 32 =>
        Some(Vector.tabulate(value.length)(value.byteAt))
      case _ => reject("actual Full27 W.last_commit_fingerprint must be SQL NULL or exactly 32 typed binary bytes")
    if operation.isDefined != fingerprint.isDefined then
      reject("actual Full27 W receipt operation/fingerprint slot must be both NULL or both populated")
    MySqlBusinessWriterRow(binding, epoch, owner, lease, operation, fingerprint, databaseNow)

  private def requireDbFirstPlan(plan: DbFirstWorldAdoptionPlanData, inputs: MySqlScriptSchemaInputs): Unit =
    if plan == null || inputs == null || inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 ||
        inputs.deploymentBudget.isEmpty || inputs.schemaFormat != DbFirstBootstrapRecordCodec.SCHEMA_FORMAT ||
        inputs.canonicalOrder.size != 27 || inputs.canonicalOrder.map(_.columns.size).sum != Full27Columns ||
        plan.schemaFormat() != DbFirstBootstrapRecordCodec.SCHEMA_FORMAT ||
        plan.physicalFormat() != MySqlDbFirstSchemaDefinition.PhysicalBindingFormat ||
        plan.schema() != inputs.schema || !plan.ddlSha256().equalsIgnoreCase(inputs.ddlSha256) then
      reject("source plan and inputs must select the same closed schema6 Full27 recipe")

  private def requireRoot(inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    if root == null || root.isClosed || root.isScopedReadBudget || (root.profile ne inputs.sourceLimitProfile) then
      reject("open original source-profile read root is required for FirstBinding6 proof validation")
    root.requireScriptDataInputs(inputs)

  private def requireHistoricalPair(
      record: RecordData,
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData
  ): Unit =
    requireSnapshot(before, inputs, root, "historical before")
    requireSnapshot(target, inputs, root, "historical target")
    if before.schema != target.schema || before.connection != target.connection ||
        before.observationScope != target.observationScope ||
        before.maxAllowedPacketBytes != target.maxAllowedPacketBytes then
      reject("historical Full27 before/target headers must match exactly")
    if before.connection.serverUuid != plan.mysqlServerUuid() || before.connection.catalog != plan.schema() ||
        before.schema != plan.schema() then
      reject("historical Full27 header differs from the exact planned server or schema")
    val owner = record.owner()
    if before.observationScope.bootUuid != owner.boot() || before.observationScope.bootGeneration != owner.generation() ||
        target.observationScope.bootUuid != owner.boot() || target.observationScope.bootGeneration != owner.generation() then
      reject("historical Full27 observation scope differs from the phase owner boot/generation")

  private def requireSnapshot(
      snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      label: String
  ): Unit =
    if snapshot == null || (snapshot.inputs ne inputs) || snapshot.allocationBudget == null ||
        snapshot.allocationBudget.isClosed ||
        !(snapshot.allocationBudget eq root) && !snapshot.allocationBudget.isCanonicalChildOf(root) ||
        !snapshot.allocationBudget.sharesAllocationLedger(root) then
      reject(s"$label snapshot must belong to the exact Full27 inputs and original allocation root")
    snapshot.allocationBudget.requireScriptDataInputs(inputs)
    if snapshot.schema != inputs.schema || snapshot.connection.catalog != inputs.schema ||
        snapshot.relations.map(_.table) != inputs.canonicalOrder.map(_.table) then
      reject(s"$label snapshot must retain the full canonical 27-relation schema6 image")

  private def requireWitness(reference: WitnessRef, snapshot: MySqlScriptSchemaSnapshot, label: String): Unit =
    if reference == null || !reference.sha256().equalsIgnoreCase(snapshot.canonicalSha256Hex) ||
        reference.byteLength() != snapshot.canonicalByteLength then
      reject(s"$label witness reference differs from the actual canonical Full27 bytes/hash")

  private def validatePhaseMarker(
      phase: Phase,
      marker: MarkerData,
      source: DbFirstWorldAdoptionReceiptCodec.SourceFacts,
      plan: DbFirstWorldAdoptionPlanData
  ): Option[Vector[Byte]] = phase match
    case Phase.PREPARE =>
      if marker != null then reject("PREPARE cannot carry marker DATA")
      None
    case Phase.HANDOFF if marker == null => None
    case Phase.HANDOFF | Phase.CERTIFY | Phase.RELEASE =>
      if marker == null then reject(s"$phase requires marker DATA")
      val expected = expectedMarkerSha256(plan.bindingUuid())
      if marker.markerSha256() != expected then reject(s"$phase marker hash differs from the canonical schema6 binding marker bytes")
      val old = source.oldMarker()
      if marker.preservedOldMarkerPresent() != old.present() ||
          marker.preservedOldMarkerPresent() && marker.preservedOldFileId() != old.fileId() then
        reject(s"$phase preserved-old marker presence/FileId differs from exact original source facts")
      Some(shaBytes(expected))

  private def expectedMarkerSha256(bindingUuid: UUID): String =
    val marker = ByteBuffer.allocate(24).putInt(MarkerMagic).putInt(MarkerFormat)
      .putLong(bindingUuid.getMostSignificantBits).putLong(bindingUuid.getLeastSignificantBits).array()
    digestHex(marker)

  private def freshTarget(writer: MySqlBusinessWriterRow): MySqlFreshFormat3WriterTarget =
    val operation = writer.lastOperationUuid.getOrElse(reject("phase target W has no actual operation slot"))
    MySqlFreshFormat3WriterTarget(writer.bindingUuid, writer.writerEpoch, writer.ownerBootUuid,
      writer.leaseUntil, operation)

  private def fenceTarget(writer: MySqlBusinessWriterRow): MySqlWriterFenceRow =
    MySqlWriterFenceRow(writer.bindingUuid, writer.writerEpoch, writer.ownerBootUuid, writer.leaseUntil,
      writer.lastOperationUuid, writer.lastFingerprint)

  private def transitionContext(
      record: RecordData,
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlBusinessWriterRow,
      plan: DbFirstWorldAdoptionPlanData,
      root: MySqlFormat3ReadBudget,
      phase: String
  ): MySqlOperationContext =
    val temporary = 4096L
    root.reserveTemporaryMemory(temporary, s"FirstBinding6 $phase context digest workspace")
    var primary: Throwable = null
    var temporaryHeld = true
    var retained = false
    try
      val digest = MessageDigest.getInstance("SHA-256")
      val domain = s"floatingpod.mysql.db-first-full27-bootstrap-$phase-effect/v1"
      updateText(digest, domain)
      updateText(digest, record.sourceReceiptSha256())
      updateText(digest, record.phase().name())
      updateUuid(digest, record.operationUuid())
      updateUuid(digest, record.owner().boot())
      updateLong(digest, record.owner().generation())
      updateLong(digest, record.databaseNow().toLocalDate.toEpochDay)
      updateInt(digest, record.databaseNow().toLocalTime.toSecondOfDay)
      updateInt(digest, record.databaseNow().getNano)
      updateText(digest, record.sessionBarrierNamespace())
      updateLong(digest, record.actualConnectionId())
      updateUuid(digest, record.connectionGeneration())
      updatePlan(digest, plan)
      updateWitness(digest, record.beforeFull27())
      updateWitness(digest, record.targetFull27())
      updateWriter(digest, beforeWriter)
      updateWriter(digest, targetWriter)
      val fingerprint = digest.digest().toVector
      root.reserveRetainedMemory(1024L, s"FirstBinding6 $phase DATA operation context")
      retained = true
      val context = MySqlOperationContext(record.owner().boot(), targetWriter.writerEpoch, targetWriter.bindingUuid,
        record.operationUuid(), Vector(MySqlExpectedEffect(
          s"db-first-full27/$phase/${targetWriter.bindingUuid}", beforeWriter.writerEpoch, fingerprint)))
      root.releaseTemporaryMemory(temporary, s"FirstBinding6 $phase context digest workspace")
      temporaryHeld = false
      context
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      var cleanup: Throwable = null
      if temporaryHeld then
        try root.releaseTemporaryMemory(temporary, s"FirstBinding6 $phase context digest workspace")
        catch case failure: Throwable => cleanup = failure
      if retained && (primary != null || cleanup != null) then
        try root.releaseRetainedMemory(1024L, s"FirstBinding6 $phase DATA operation context")
        catch
          case failure: Throwable =>
            if cleanup == null then cleanup = failure
            else if failure ne cleanup then cleanup.addSuppressed(failure)
      if primary != null then
        if cleanup != null && (cleanup ne primary) then primary.addSuppressed(cleanup)
      else if cleanup != null then throw cleanup

  private def updatePlan(digest: MessageDigest, plan: DbFirstWorldAdoptionPlanData): Unit =
    if plan != null then
      updateUuid(digest, plan.creationOperationUuid()); updateUuid(digest, plan.bootstrapOperationUuid())
      updateUuid(digest, plan.prepareWriteUuid()); updateUuid(digest, plan.certifyWriteUuid())
      updateUuid(digest, plan.bindingUuid()); updateInt(digest, plan.physicalFormat())
      updateUuid(digest, plan.mysqlServerUuid()); updateText(digest, plan.schema())
      updateInt(digest, plan.schemaFormat()); updateText(digest, plan.ddlSha256())
      updateUuid(digest, plan.provisioningUuid())

  private def updateWitness(digest: MessageDigest, reference: WitnessRef): Unit =
    updateText(digest, reference.fileId()); updateText(digest, reference.sha256()); updateLong(digest, reference.byteLength())

  private def updateWriter(digest: MessageDigest, writer: MySqlBusinessWriterRow): Unit =
    updateUuid(digest, writer.bindingUuid); updateLong(digest, writer.writerEpoch)
    updateOptionUuid(digest, writer.ownerBootUuid); updateText(digest, writer.leaseUntil.toString)
    updateOptionUuid(digest, writer.lastOperationUuid)
    writer.lastFingerprint match
      case None => digest.update(0.toByte)
      case Some(value) => digest.update(1.toByte); value.foreach(byte => digest.update(byte))

  private def updateText(digest: MessageDigest, value: String): Unit =
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    updateInt(digest, bytes.length); digest.update(bytes)

  private def updateOptionUuid(digest: MessageDigest, value: Option[UUID]): Unit = value match
    case None => digest.update(0.toByte)
    case Some(uuid) => digest.update(1.toByte); updateUuid(digest, uuid)

  private def updateUuid(digest: MessageDigest, value: UUID): Unit =
    updateLong(digest, value.getMostSignificantBits); updateLong(digest, value.getLeastSignificantBits)

  private def updateInt(digest: MessageDigest, value: Int): Unit =
    digest.update((value >>> 24).toByte); digest.update((value >>> 16).toByte)
    digest.update((value >>> 8).toByte); digest.update(value.toByte)

  private def updateLong(digest: MessageDigest, value: Long): Unit =
    updateInt(digest, (value >>> 32).toInt); updateInt(digest, value.toInt)

  private def sourceReceiptSha(source: DbFirstWorldAdoptionReceiptCodec.SourceFacts,
      root: MySqlFormat3ReadBudget): String =
    withTemporary(root, 16_384L, "FirstBinding6 exact source receipt hashing workspace") {
      val bytes = try DbFirstWorldAdoptionReceiptCodec.encode(source)
      catch
        case cause: IOException => throw new SQLDataException("cannot encode exact ExistingWorldAdoption6 source DATA", cause)
      digestHex(bytes)
    }

  private def recordSha256(record: RecordData, root: MySqlFormat3ReadBudget): String =
    withTemporary(root, DbFirstBootstrapRecordCodec.MAX_RECORD_BYTES.toLong * 3L,
      "FirstBinding6 complete INTENT SHA workspace") {
      val bytes = try DbFirstBootstrapRecordCodec.encodeRecord(record)
      catch
        case cause: IOException => throw new SQLDataException("cannot encode exact FirstBinding6 INTENT DATA", cause)
      digestHex(bytes)
    }

  private def withTemporary[A](root: MySqlFormat3ReadBudget, amount: Long, context: String)(body: => A): A =
    root.reserveTemporaryMemory(amount, context)
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable => primary = failure; throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(() => root.releaseTemporaryMemory(amount, context)))

  private def digestHex(bytes: Array[Byte]): String = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
  private def shaBytes(hex: String): Vector[Byte] = HexFormat.of().parseHex(hex).iterator.map(_.toByte).toVector

  private def uuid(cell: MySqlFormat3Cell, label: String): UUID = cell match
    case MySqlFormat3Cell.Binary(value) if value.length == 16 =>
      val bytes = Array.tabulate(value.length)(value.byteAt)
      val buffer = ByteBuffer.wrap(bytes)
      val result = new UUID(buffer.getLong(), buffer.getLong())
      if isZero(result) then reject(s"$label must contain a nonzero UUID")
      result
    case _ => reject(s"$label must contain one exact 16-byte binary UUID")

  private def optionalUuid(cell: MySqlFormat3Cell, label: String): Option[UUID] = cell match
    case MySqlFormat3Cell.SqlNull => None
    case value => Some(uuid(value, label))

  private def isZero(value: UUID): Boolean = value.getMostSignificantBits == 0L && value.getLeastSignificantBits == 0L
  private def isLowerSha256(value: String): Boolean = value != null && value.matches("[0-9a-f]{64}")
  private def reject(message: String): Nothing = throw new SQLDataException("FirstBinding6 bootstrap proof rejected: " + message)
