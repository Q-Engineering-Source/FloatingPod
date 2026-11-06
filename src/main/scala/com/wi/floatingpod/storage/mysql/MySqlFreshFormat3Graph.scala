package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.PhysicalRecoveryBinding

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 仅用于规范化 self-referential W receipt 的目标字段；不是可写行或 capability。 */
private[mysql] final case class MySqlFreshFormat3WriterTarget(
    bindingUuid: UUID,
    writerEpoch: Long,
    ownerBootUuid: Option[UUID],
    leaseUntil: LocalDateTime,
    operationUuid: UUID
):
  require(bindingUuid != null && writerEpoch >= 0L && ownerBootUuid != null && leaseUntil != null && operationUuid != null,
    "Fresh Format3 normalized W target is incomplete")
  require(leaseUntil.getNano % 1000 == 0, "Fresh Format3 normalized W target must use DATETIME(6) precision")

/** Fresh B1 的 Format3 图变换。只允许改 B、B1 intent 与 W；其余二十表行完整保留。 */
private[mysql] object MySqlFreshFormat3Graph:
  private val EffectDomain = "floatingpod.mysql.b1-format3-full20-effect/v1"

  def requireManifestMatches(
      snapshot: MySqlFormat3FullSnapshot,
      locked: MySqlLockedManifest,
      inputs: MySqlFormat3AuditInputs
  ): Unit =
    if snapshot == null || locked == null || inputs == null then invalid("full manifest inputs")
    val relation = snapshot.relation("fp_schema_manifest")
    val shape = MySqlFormat3RelationShapes.forTable("fp_schema_manifest")
    if shape.columns.size != 29 || relation.rows.size != 1 || relation.rows.head.size != 29 || inputs.manifestLabels.size != 15 then
      invalid("exact 29-column singleton manifest shape")
    val row = relation.rows.head
    def cell(name: String): MySqlFormat3Cell =
      val index = shape.columns.indexWhere(_.name == name)
      if index < 0 then invalid("manifest field " + name)
      row(index)
    def text(name: String, value: String): Unit =
      if cell(name) != MySqlFormat3Cell.Text(value) then invalid("manifest field " + name)
    def integer(name: String, value: Long): Unit =
      if cell(name) != MySqlFormat3Cell.SignedInteger(value) then invalid("manifest field " + name)
    def binary(name: String, value: Vector[Byte]): Unit =
      if value == null || value.size != 32 || !cell(name).isInstanceOf[MySqlFormat3Cell.Binary] ||
        !cell(name).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsVector(value) then invalid("manifest field " + name)
    def uuid(name: String, value: UUID): Unit =
      if value == null || !cell(name).isInstanceOf[MySqlFormat3Cell.Binary] ||
        !cell(name).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(value) then invalid("manifest field " + name)
    def optionalHash(name: String, value: Option[Vector[Byte]]): Unit = value match
      case None => if cell(name) != MySqlFormat3Cell.SqlNull then invalid("manifest SQL NULL field " + name)
      case Some(bytes) => binary(name, bytes)
    def optionalUuid(name: String, value: Option[UUID]): Unit = value match
      case None => if cell(name) != MySqlFormat3Cell.SqlNull then invalid("manifest SQL NULL field " + name)
      case Some(id) => uuid(name, id)

    integer("singleton_fixed1", locked.singleton.toLong)
    text("backend", locked.backend)
    integer("format_version", locked.formatVersion.toLong)
    binary("ddl_sha256", locked.ddlSha256)
    uuid("provisioning_uuid", locked.provisioningUuid)
    text("bootstrap_state", locked.bootstrapState)
    uuid("expected_server_uuid", locked.serverUuid)
    uuid("expected_binding_uuid", locked.bindingUuid)
    optionalHash("import_manifest_sha256", locked.importManifestSha256)
    text("import_state", locked.importState)
    optionalUuid("import_operation_uuid", locked.importOperationUuid)
    optionalHash("import_snapshot_sha256", locked.importSnapshotSha256)
    optionalHash("import_receipt_sha256", locked.importReceiptSha256)
    optionalHash("guard_manifest_sha256", locked.guardManifestSha256)

    val appendedNames = inputs.manifestLabels
    appendedNames.zip(inputs.expectedManifestCells).foreach { (name, expected) =>
      val actual = cell(name)
      val matches = (actual, expected) match
        case (MySqlFormat3Cell.SqlNull, MySqlAuditNull) => true
        case (MySqlFormat3Cell.Text(left), MySqlAuditText(right)) => left == right
        case (MySqlFormat3Cell.Binary(left), right: MySqlAuditBinary) =>
          left.length == right.length && right.equalsFormat3Binary(left)
        case (MySqlFormat3Cell.DateTime(left), MySqlAuditTimestamp(right)) => left == right
        case (MySqlFormat3Cell.SignedInteger(left), MySqlAuditText(right)) => left.toString == right
        case _ => false
      if !matches then invalid("independent format3 manifest field " + name)
    }
    if snapshot.schema != inputs.schema || snapshot.connection.serverUuid != inputs.serverUuid ||
      snapshot.connection.serverVersion != inputs.serverVersion || snapshot.connection.catalog != inputs.schema ||
      snapshot.connection.effectiveUser != inputs.effectiveUser || snapshot.observationScope.bootUuid == null ||
      snapshot.limitProfile != inputs.limitProfile then invalid("format3 manifest observation identity/profile")

  def requireBindingMatches(snapshot: MySqlFormat3FullSnapshot, expected: Option[PhysicalRecoveryBinding]): Unit =
    if snapshot == null || expected == null then invalid("physical binding input")
    val rows = snapshot.relation("physical_recovery_binding").rows
    expected match
      case None => if rows.nonEmpty then invalid("unexpected physical binding row")
      case Some(binding) =>
        if binding == null then invalid("null physical binding")
        rows match
          case Vector(Vector(
                MySqlFormat3Cell.SignedInteger(1L),
                MySqlFormat3Cell.Binary(id),
                MySqlFormat3Cell.Text(state),
                MySqlFormat3Cell.SignedInteger(format)
              )) =>
            if !id.equalsUuid(binding.bindingUuid) || state != binding.cutoverState.databaseValue || format != binding.formatVersion.toLong then
              invalid("physical binding differs from the typed locked row")
          case _ => invalid("physical binding singleton row shape")

  /** 将完整二十表中的 B1 历史与状态转换投影逐字段对照，不能只凭相同行数认定相同。 */
  def requireBootstrapIntentsMatch(snapshot: MySqlFormat3FullSnapshot, expected: Vector[MySqlBootstrapIntentData]): Unit =
    if snapshot == null || expected == null || expected.exists(_ == null) then invalid("bootstrap intent graph inputs")
    val rows = snapshot.relation("binding_bootstrap_intent").rows
    val shape = MySqlFormat3RelationShapes.forTable("binding_bootstrap_intent")
    if shape.columns.size != 11 || expected.size > 1 || rows.size != expected.size then
      invalid("exact B1 intent row count and fixed eleven-column shape")
    val indices = shape.columns.iterator.zipWithIndex.map((column, index) => column.name -> index).toMap
    def matches(row: Vector[MySqlFormat3Cell], intent: MySqlBootstrapIntentData): Boolean =
      if row == null || row.size != shape.columns.size then false
      else
        def field(name: String): MySqlFormat3Cell = row(indices.getOrElse(name, invalid("B1 intent field " + name)))
        def integer(name: String, value: Long): Boolean = field(name) == MySqlFormat3Cell.SignedInteger(value)
        def text(name: String, value: String): Boolean = value != null && field(name) == MySqlFormat3Cell.Text(value)
        def uuid(name: String, value: UUID): Boolean = value != null && (field(name) match
          case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(value)
          case _ => false)
        def hash(name: String, value: Vector[Byte]): Boolean = value != null && value.size == 32 && (field(name) match
          case MySqlFormat3Cell.Binary(bytes) => bytes.length == 32 && bytes.equalsVector(value)
          case _ => false)
        def optionalHash(name: String, value: Option[Vector[Byte]]): Boolean =
          if value == null then false
          else value match
            case None => field(name) == MySqlFormat3Cell.SqlNull
            case Some(bytes) => hash(name, bytes)
        val stage = intent.stage match
          case MySqlBootstrapIntentStage.Prepared => "PREPARED"
          case MySqlBootstrapIntentStage.Complete => "COMPLETE"
          case null => null
        integer("singleton_fixed1", intent.singletonFixed1.toLong) &&
          uuid("bootstrap_operation_uuid", intent.bootstrapOperationUuid) &&
          uuid("prepare_write_uuid", intent.prepareWriteUuid) &&
          uuid("certify_write_uuid", intent.certifyWriteUuid) &&
          uuid("binding_uuid", intent.bindingUuid) &&
          integer("binding_format_version", intent.bindingFormatVersion.toLong) &&
          uuid("provisioning_uuid", intent.provisioningUuid) &&
          text("source_world_identity", intent.sourceWorldIdentity) &&
          hash("provision_receipt_sha256", intent.provisionReceiptSha256) &&
          text("stage", stage) &&
          optionalHash("marker_ack_sha256", intent.markerAckSha256)
    if !rows.zip(expected).forall((row, intent) => matches(row, intent)) then
      invalid("complete typed B1 intent rows differ from the transition projection")

  def requireWriterMatches(snapshot: MySqlFormat3FullSnapshot, expected: MySqlBusinessWriterRow): Unit =
    if snapshot == null || expected == null then invalid("runtime writer input")
    val rows = snapshot.relation("fp_runtime_writer").rows
    rows match
      case Vector(row) if row.size == 7 =>
        val shape = MySqlFormat3RelationShapes.forTable("fp_runtime_writer")
        def value(name: String): MySqlFormat3Cell = row(shape.columns.indexWhere(_.name == name))
        def matchesUuid(name: String, id: UUID): Boolean =
          if id == null then false
          else value(name) match
            case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
            case _ => false
        def matchesOptUuid(name: String, id: Option[UUID]): Boolean = id match
          case None => value(name) == MySqlFormat3Cell.SqlNull
          case Some(uuid) => matchesUuid(name, uuid)
        def matchesOptHash(name: String, hash: Option[Vector[Byte]]): Boolean = hash match
          case None => value(name) == MySqlFormat3Cell.SqlNull
          case Some(bytes) => value(name) match
            case MySqlFormat3Cell.Binary(binary) => binary.equalsVector(bytes)
            case _ => false
        if value("singleton_fixed1") != MySqlFormat3Cell.SignedInteger(1L) ||
          !matchesUuid("binding_uuid", expected.bindingUuid) ||
          value("writer_epoch") != MySqlFormat3Cell.SignedInteger(expected.writerEpoch) ||
          !matchesOptUuid("owner_boot_uuid", expected.ownerBootUuid) ||
          value("lease_until") != MySqlFormat3Cell.DateTime(expected.leaseUntil) ||
          !matchesOptUuid("last_commit_operation_uuid", expected.lastOperationUuid) ||
          !matchesOptHash("last_commit_fingerprint", expected.lastFingerprint) then invalid("runtime writer differs from the typed locked row")
      case _ => invalid("runtime writer singleton row shape")

  /** Fresh 只接受获批 M 与未认领的 W 控制行；B 和其余完整关系必须没有任何历史行。 */
  def requireFreshInitialSeed(
      snapshot: MySqlFormat3FullSnapshot,
      repository: MySqlFullRepositorySnapshot,
      lockedManifest: MySqlLockedManifest,
      lockedWriter: MySqlBusinessWriterRow,
      expectedWriterEpoch: Long,
      inputs: MySqlFormat3AuditInputs
  ): Unit =
    if snapshot == null || repository == null || lockedManifest == null || lockedWriter == null || inputs == null ||
      expectedWriterEpoch < 0L then invalid("Fresh initial seed inputs")
    if snapshot.relations.map(_.table) != MySqlFormat3RelationShapes.canonicalOrder.map(_.table) then
      invalid("Fresh initial seed requires the exact complete twenty-relation shape")
    val projectedRepository = MySqlFreshFormat3TransitionProjection.project(snapshot)
    if !projectedRepository.exactlyEquals(repository) then
      invalid("Fresh initial nine-table transition value must be derived from the exact full20 snapshot")
    if !repository.exactlyEquals(MySqlFullRepositorySnapshot.empty) then
      invalid("Fresh initial seed requires the exact empty nine-table transition projection")
    val allowedControlRelations = Set("fp_schema_manifest", "fp_runtime_writer")
    MySqlFormat3RelationShapes.canonicalOrder.iterator
      .filterNot(shape => allowedControlRelations.contains(shape.table))
      .find(shape => snapshot.relation(shape.table).rows.nonEmpty)
      .foreach(shape => invalid("Fresh initial seed relation must be empty: " + shape.table))
    requireManifestMatches(snapshot, lockedManifest, inputs)
    requireWriterMatches(snapshot, lockedWriter)
    requireBindingMatches(snapshot, None)
    requireBootstrapIntentsMatch(snapshot, Vector.empty)
    if lockedWriter.bindingUuid != lockedManifest.bindingUuid || lockedWriter.writerEpoch != expectedWriterEpoch ||
      lockedWriter.ownerBootUuid.nonEmpty || lockedWriter.lastOperationUuid.nonEmpty || lockedWriter.lastFingerprint.nonEmpty ||
      lockedWriter.leaseUntil.isAfter(lockedWriter.databaseNow) then
      invalid("Fresh initial seed W control row is not the exact unowned approved writer preimage")

  /** 生成确定的 full20 目标快照，保留其余全部 typed rows 与观察身份。 */
  def targetSnapshot(
      before: MySqlFormat3FullSnapshot,
      beforeRepository: MySqlFullRepositorySnapshot,
      targetRepository: MySqlFullRepositorySnapshot,
      targetWriter: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    if before == null || beforeRepository == null || targetRepository == null || targetWriter == null then
      invalid("full20 target inputs")
    if !MySqlFreshFormat3TransitionProjection.project(before).exactlyEquals(beforeRepository) then
      invalid("Fresh before transition value must be derived from its exact full20 snapshot")
    requireOnlyBootstrapRepositoryChanges(beforeRepository, targetRepository)
    targetRepository.validate()
    if targetRepository.bootstrapIntents.size != 1 then invalid("Fresh full20 target requires its exact single B1 history row")
    requireBindingMatches(before, beforeRepository.recovery.binding)
    requireBootstrapIntentsMatch(before, beforeRepository.bootstrapIntents)

    val budget = before.allocationBudget
    budget.reserveRetainedMemory(Math.addExact(1_024L, Math.multiplyExact(before.relations.size.toLong, 96L)), "Fresh format3 target snapshot structures")
    val bindingRelation = replacementRelation(before, "physical_recovery_binding",
      targetRepository.recovery.binding.toVector.map(bindingRow(_, budget)))
    val intentRelation = replacementRelation(before, "binding_bootstrap_intent",
      targetRepository.bootstrapIntents.map(intentRow(_, budget)))
    val writerRelation = replacementRelation(before, "fp_runtime_writer", Vector(writerRow(targetWriter, budget)))
    val replacements = Map(
      bindingRelation.table -> bindingRelation,
      intentRelation.table -> intentRelation,
      writerRelation.table -> writerRelation
    )
    val relations = before.relations.map(relation => replacements.getOrElse(relation.table, relation))
    val target = new MySqlFormat3FullSnapshot(before.schema, before.connection, before.observationScope, before.maxAllowedPacketBytes,
      before.limitProfile, relations, budget)
    requireBindingMatches(target, targetRepository.recovery.binding)
    requireBootstrapIntentsMatch(target, targetRepository.bootstrapIntents)
    requireWriterMatches(target, targetWriter)
    if target.relations.iterator.filterNot(relation =>
      relation.table == "physical_recovery_binding" || relation.table == "binding_bootstrap_intent" || relation.table == "fp_runtime_writer")
      .zip(before.relations.iterator.filterNot(relation =>
        relation.table == "physical_recovery_binding" || relation.table == "binding_bootstrap_intent" || relation.table == "fp_runtime_writer"))
      .exists((actual, original) => actual != original) then
      invalid("Fresh full20 target changed a relation outside B, B1 intent, and W")
    if !MySqlFreshFormat3TransitionProjection.project(target).exactlyEquals(targetRepository) then
      invalid("Fresh target transition value differs from the exact projected full20 B/B1 rows")
    target

  /** operation fingerprint commits the complete before graph and typed target delta, excluding only its self-reference. */
  def effectFingerprint(stage: String, plan: MySqlBootstrapPlanIdentity,
      before: MySqlFormat3FullSnapshot, beforeRepository: MySqlFullRepositorySnapshot,
      targetRepository: MySqlFullRepositorySnapshot,
      targetWriterWithoutFingerprint: MySqlFreshFormat3WriterTarget): Vector[Byte] =
    if (stage != "PREPARE" && stage != "CERTIFY") || plan == null || before == null || beforeRepository == null || targetRepository == null ||
      targetWriterWithoutFingerprint == null then
      invalid("full20 B1 operation fingerprint inputs")
    val expectedOperation = if stage == "PREPARE" then plan.prepareWriteUuid else plan.certifyWriteUuid
    if targetWriterWithoutFingerprint.bindingUuid != plan.bindingUuid ||
      targetWriterWithoutFingerprint.operationUuid != expectedOperation then
      invalid("full20 B1 normalized W target differs from its fixed plan operation")
    requireOnlyBootstrapRepositoryChanges(beforeRepository, targetRepository)
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(EffectDomain.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    digest.update(stage.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    digest.update(MySqlStoredRows.encodeUuid(plan.creationOperationUuid).toArray)
    digest.update(MySqlStoredRows.encodeUuid(plan.bootstrapOperationUuid).toArray)
    digest.update(MySqlStoredRows.encodeUuid(plan.mysqlServerUuid).toArray)
    sized(digest, plan.schema.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    digest.update(MySqlStoredRows.encodeUuid(plan.bindingUuid).toArray)
    digest.update(MySqlStoredRows.encodeUuid(plan.provisioningUuid).toArray)
    digest.update(plan.canonicalDdlSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    digest.update(before.canonicalSha256.toArray)
    digest.update(java.nio.ByteBuffer.allocate(8).putLong(before.canonicalByteLength).array())
    sized(digest, targetRepository.canonicalBytes.toArray)
    encodeWriterWithoutFingerprint(digest, targetWriterWithoutFingerprint)
    digest.digest().toVector

  private def requireOnlyBootstrapRepositoryChanges(before: MySqlFullRepositorySnapshot, target: MySqlFullRepositorySnapshot): Unit =
    if before.pods != target.pods || before.entitlements != target.entitlements ||
      before.recovery.copy(binding = None) != target.recovery.copy(binding = None) then
      invalid("Fresh B1 target changed pod, entitlement, locator, chunk, or recovery-intent history")

  private def bindingRow(binding: PhysicalRecoveryBinding, budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    row("physical_recovery_binding", Map(
      "singleton_fixed1" -> integer(1L),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(binding.bindingUuid), budget, "Fresh B1 binding UUID"),
      "cutover_state" -> text(binding.cutoverState.databaseValue, budget, "Fresh B1 binding state"),
      "format_version" -> integer(binding.formatVersion.toLong)
    ))

  private def intentRow(intent: MySqlBootstrapIntentData, budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    row("binding_bootstrap_intent", Map(
      "singleton_fixed1" -> integer(intent.singletonFixed1.toLong),
      "bootstrap_operation_uuid" -> binary(MySqlStoredRows.encodeUuid(intent.bootstrapOperationUuid), budget, "Fresh B1 bootstrap operation UUID"),
      "prepare_write_uuid" -> binary(MySqlStoredRows.encodeUuid(intent.prepareWriteUuid), budget, "Fresh B1 prepare operation UUID"),
      "certify_write_uuid" -> binary(MySqlStoredRows.encodeUuid(intent.certifyWriteUuid), budget, "Fresh B1 certify operation UUID"),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(intent.bindingUuid), budget, "Fresh B1 intent binding UUID"),
      "binding_format_version" -> integer(intent.bindingFormatVersion.toLong),
      "provisioning_uuid" -> binary(MySqlStoredRows.encodeUuid(intent.provisioningUuid), budget, "Fresh B1 provisioning UUID"),
      "source_world_identity" -> text(intent.sourceWorldIdentity, budget, "Fresh B1 source identity"),
      "provision_receipt_sha256" -> binary(intent.provisionReceiptSha256, budget, "Fresh B1 provision receipt SHA"),
      "stage" -> text(intent.stage match
        case MySqlBootstrapIntentStage.Prepared => "PREPARED"
        case MySqlBootstrapIntentStage.Complete => "COMPLETE", budget, "Fresh B1 intent stage"),
      "marker_ack_sha256" -> optionalBinary(intent.markerAckSha256, budget, "Fresh B1 marker ACK SHA")
    ))

  private def writerRow(writer: MySqlBusinessWriterRow, budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    row("fp_runtime_writer", Map(
      "singleton_fixed1" -> integer(1L),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(writer.bindingUuid), budget, "Fresh W binding UUID"),
      "writer_epoch" -> integer(writer.writerEpoch),
      "owner_boot_uuid" -> optionalUuid(writer.ownerBootUuid, budget, "Fresh W owner boot UUID"),
      "lease_until" -> dateTime(writer.leaseUntil),
      "last_commit_operation_uuid" -> optionalUuid(writer.lastOperationUuid, budget, "Fresh W operation UUID"),
      "last_commit_fingerprint" -> optionalBinary(writer.lastFingerprint, budget, "Fresh W fingerprint")
    ))

  private def encodeWriterWithoutFingerprint(digest: MessageDigest, writer: MySqlFreshFormat3WriterTarget): Unit =
    digest.update(MySqlStoredRows.encodeUuid(writer.bindingUuid).toArray)
    digest.update(java.nio.ByteBuffer.allocate(8).putLong(writer.writerEpoch).array())
    optionUuid(digest, writer.ownerBootUuid)
    digest.update(writer.leaseUntil.toString.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    digest.update(MySqlStoredRows.encodeUuid(writer.operationUuid).toArray)
    // Null tags the one self-referential slot; all other W columns and the fixed operation UUID are committed.
    digest.update(0.toByte)

  private def optionUuid(digest: MessageDigest, value: Option[UUID]): Unit = value match
    case None => digest.update(0.toByte)
    case Some(id) => digest.update(1.toByte); digest.update(MySqlStoredRows.encodeUuid(id).toArray)

  private def sized(digest: MessageDigest, bytes: Array[Byte]): Unit =
    digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array())
    digest.update(bytes)

  private def replacementRelation(snapshot: MySqlFormat3FullSnapshot, table: String,
      rows: Vector[Vector[MySqlFormat3Cell]]): MySqlFormat3RelationSnapshot =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    if rows.exists(row => row == null || row.size != shape.columns.size) then invalid("target relation " + table + " row shape")
    if rows.size > 1 then invalid("target relation " + table + " exceeds its exact singleton cardinality")
    MySqlFormat3RelationSnapshot(table, rows)

  private def row(table: String, values: Map[String, MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    if values.keySet != shape.columns.map(_.name).toSet then invalid("target relation " + table + " must set every fixed column exactly once")
    shape.columns.map(column => values.getOrElse(column.name, invalid("target relation " + table + " missing " + column.name)))

  private def binary(bytes: Vector[Byte], budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell =
    if bytes == null || budget == null then invalid(context)
    budget.reserveTemporaryMemory(bytes.size.toLong, context + " temporary byte copy")
    var primary: Throwable = null
    try
      val copied = new Array[Byte](bytes.size)
      var index = 0
      while index < bytes.size do
        copied(index) = bytes(index)
        index += 1
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.copyOf(copied, budget, context))
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(() => budget.releaseTemporaryMemory(bytes.size.toLong, context + " temporary byte copy")))

  private def optionalBinary(value: Option[Vector[Byte]], budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell =
    if value == null || value.exists(_ == null) then invalid(context)
    value match
      case None => MySqlFormat3Cell.SqlNull
      case Some(bytes) => binary(bytes, budget, context)

  private def optionalUuid(value: Option[UUID], budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell =
    if value == null || value.exists(_ == null) then invalid(context)
    value match
      case None => MySqlFormat3Cell.SqlNull
      case Some(id) => binary(MySqlStoredRows.encodeUuid(id), budget, context)

  private def text(value: String, budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell.Text =
    if value == null || budget == null then invalid(context)
    val utf8 = MySqlFormat3Utf8.byteLength(value)
    budget.reserveRetainedMemory(Math.addExact(64L, Math.max(utf8, Math.multiplyExact(value.length.toLong, 2L))), context)
    MySqlFormat3Cell.Text(value)

  private def integer(value: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(value)
  private def dateTime(value: LocalDateTime): MySqlFormat3Cell =
    if value == null || value.getNano % 1000 != 0 then invalid("DATETIME(6) target precision")
    MySqlFormat3Cell.DateTime(value)

  private def invalid(reason: String): Nothing = throw new SQLDataException("MySQL Fresh Format3 full20 graph rejected: " + reason)
