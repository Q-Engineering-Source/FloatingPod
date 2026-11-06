package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstWorldAdoptionPlanData
import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState

import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.{HexFormat, UUID}

/** Schema6 B1 writer settlement 的完整 Full27 DATA 变换；不证明进程退出、barrier 或 Ready。 */
private[mysql] object MySqlDbFirstBootstrapWriterGraph:
  enum Kind:
    case HANDOFF, RELEASE

  private val WriterTable = "fp_runtime_writer"
  private val ManifestTable = "fp_schema_manifest"
  private val BindingTable = "physical_recovery_binding"
  private val IntentTable = "binding_bootstrap_intent"
  private val CatalogTable = "fp_script_catalog"
  private val QuotaTable = MySqlDeploymentReceiptSchemaDefinition.QuotaTable
  private val ChangedTables = Set(WriterTable)
  private val AllowedSeedTables = Set(ManifestTable, WriterTable, BindingTable, IntentTable, CatalogTable, QuotaTable)

  /** PREPARED 或 COMPLETE 的旧 owner 租约必须由数据库事实证明已到期；外部停止证明仍由 actor 持有。 */
  def handoffTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlWriterFenceRow,
      expectedQuotaMaximumRecords: Long
  ): MySqlScriptSchemaSnapshot =
    requireHandoffArguments(plan, beforeWriter, targetWriter)
    val projected = handoffBusinessRow(beforeWriter, targetWriter)
    requireHandoffWriterTransition(beforeWriter, projected, plan)
    val target = buildTarget(before, inputs, originalRoot, projected)
    verifyGeneratedTarget(target, before, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, expectedQuotaMaximumRecords, Kind.HANDOFF)

  /** 重验实际 HANDOFF image；before/target 的完整 27 关系中只允许 W 不同。 */
  def requireHandoffTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlWriterFenceRow,
      expectedQuotaMaximumRecords: Long
  ): Unit =
    requireHandoffArguments(plan, beforeWriter, targetWriter)
    val projected = handoffBusinessRow(beforeWriter, targetWriter)
    requireHandoffWriterTransition(beforeWriter, projected, plan)
    verifyTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, projected, expectedQuotaMaximumRecords, Kind.HANDOFF)

  /** 仅已 CERTIFIED/COMPLETE 且 slot 指向固定 CERTIFY 的旧 owner 可以释放。 */
  def releaseTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlBusinessWriterRow,
      expectedQuotaMaximumRecords: Long
  ): MySqlScriptSchemaSnapshot =
    if plan == null then reject("complete DB-first plan is required for B1 RELEASE")
    requireReleaseWriterTransition(beforeWriter, targetWriter, plan)
    val target = buildTarget(before, inputs, originalRoot, targetWriter)
    verifyGeneratedTarget(target, before, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, expectedQuotaMaximumRecords, Kind.RELEASE)

  /** 重验实际 RELEASE image；旧 B/B1/C、M/Q 与整个 Full27 DATA 均保留。 */
  def requireReleaseTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlBusinessWriterRow,
      expectedQuotaMaximumRecords: Long
  ): Unit =
    if plan == null then reject("complete DB-first plan is required for B1 RELEASE")
    requireReleaseWriterTransition(beforeWriter, targetWriter, plan)
    verifyTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, expectedQuotaMaximumRecords, Kind.RELEASE)

  private def verifyGeneratedTarget(target: MySqlScriptSchemaSnapshot,
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlWriterFenceRow | MySqlBusinessWriterRow,
      expectedQuotaMaximumRecords: Long,
      kind: Kind): MySqlScriptSchemaSnapshot =
    try
      targetWriter match
        case handoff: MySqlWriterFenceRow if kind == Kind.HANDOFF =>
          requireHandoffTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
            provisionReceiptSha256, markerAckSha256, beforeWriter, handoff, expectedQuotaMaximumRecords)
        case release: MySqlBusinessWriterRow if kind == Kind.RELEASE =>
          requireReleaseTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
            provisionReceiptSha256, markerAckSha256, beforeWriter, release, expectedQuotaMaximumRecords)
        case _ => reject("writer settlement target row type differs from its closed transition kind")
      target
    catch
      case failure: Throwable =>
        closeTargetAfterFailure(target, failure)
        throw failure

  private def verifyTransition(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      beforeWriter: MySqlBusinessWriterRow,
      targetWriter: MySqlBusinessWriterRow,
      expectedQuotaMaximumRecords: Long,
      kind: Kind): Unit =
    requireOwner(before, inputs, originalRoot, "writer settlement before")
    requireOwner(target, inputs, originalRoot, "writer settlement target")
    requireSameHeader(before, target)
    requireInputs(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256,
      markerAckSha256, expectedQuotaMaximumRecords)
    requireManifest(before, plan)
    requireManifest(target, plan)
    requireManagedInstallRows(before, plan, sourceWorldIdentity, provisionReceiptSha256,
      markerAckSha256, expectedQuotaMaximumRecords, kind)
    requireManagedInstallRows(target, plan, sourceWorldIdentity, provisionReceiptSha256,
      markerAckSha256, expectedQuotaMaximumRecords, kind)
    MySqlDbFirstBootstrapGraph.requireWriterMatches(before, beforeWriter)
    MySqlDbFirstBootstrapGraph.requireWriterMatches(target, targetWriter)
    kind match
      case Kind.HANDOFF => requireHandoffWriterTransition(beforeWriter, targetWriter, plan)
      case Kind.RELEASE => requireReleaseWriterTransition(beforeWriter, targetWriter, plan)
    requireOnlyWriterChanged(before, target)

  private def requireInputs(before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      expectedQuotaMaximumRecords: Long): Unit =
    if before == null || inputs == null || originalRoot == null || plan == null then
      reject("complete DB-first plan, Full27 inputs, before image, and original allocation root are required")
    if inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 || inputs.deploymentBudget.isEmpty then
      reject("writer settlement requires explicit Full27 DB-first DATA inputs")
    if expectedQuotaMaximumRecords <= 0L then reject("writer settlement requires an explicit positive quota maximum")
    if originalRoot.isClosed || originalRoot.isScopedReadBudget || (originalRoot.profile ne inputs.sourceLimitProfile) then
      reject("writer settlement requires the exact open Full27 allocation root")
    originalRoot.requireScriptDataInputs(inputs)
    if plan.schemaFormat() != MySqlDbFirstSchemaDefinition.SchemaFormat ||
        plan.physicalFormat() != MySqlDbFirstSchemaDefinition.PhysicalBindingFormat ||
        plan.schema() != inputs.schema || !plan.schema().matches("[a-z][a-z0-9_]{0,63}") ||
        plan.ddlSha256() == null || !plan.ddlSha256().equalsIgnoreCase(inputs.ddlSha256) ||
        plan.mysqlServerUuid() != before.connection.serverUuid || before.schema != inputs.schema ||
        before.connection.catalog != inputs.schema then
      reject("schema6 writer settlement plan differs from exact Full27 schema/DDL/server DATA")
    val identities = Vector(plan.creationOperationUuid(), plan.bootstrapOperationUuid(), plan.prepareWriteUuid(),
      plan.certifyWriteUuid(), plan.bindingUuid(), plan.mysqlServerUuid(), plan.provisioningUuid())
    if identities.exists(isZero) || plan.bindingUuid().version() != 4 ||
        plan.prepareWriteUuid() == plan.certifyWriteUuid() then
      reject("schema6 writer settlement requires its fixed nonzero and distinct plan identities")
    if sourceWorldIdentity == null || sourceWorldIdentity.isBlank then
      reject("writer settlement source identity must be nonblank")
    val sourceShape = inputs.forTable(IntentTable)
    val sourceColumn = sourceShape.columns.find(_.name == "source_world_identity")
      .getOrElse(reject("Full27 intent shape is missing source_world_identity"))
    val sourceBytes = MySqlFormat3Utf8.byteLength(sourceWorldIdentity)
    if sourceBytes > inputs.maximumPayloadBytes(IntentTable, sourceColumn).getOrElse(
        reject("Full27 source identity has no explicit text payload bound")) then
      reject("writer settlement source identity exceeds its explicit DATA byte bound")
    if provisionReceiptSha256 == null || provisionReceiptSha256.size != 32 then
      reject("writer settlement provision receipt must be exactly 32 bytes")
    markerAckSha256 match
      case None => ()
      case Some(hash) if hash != null && hash.size == 32 => ()
      case _ => reject("writer settlement marker ACK must be absent or exactly 32 bytes")
    if inputs.canonicalOrder.size != 27 || inputs.canonicalOrder.map(_.columns.size).sum != 480 then
      reject("writer settlement requires the exact closed 27-relation 480-column profile")

  private def requireOwner(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      context: String): Unit =
    if snapshot == null || inputs == null || originalRoot == null || originalRoot.isClosed ||
        originalRoot.isScopedReadBudget || (originalRoot.profile ne inputs.sourceLimitProfile) then
      reject(s"$context requires the exact open Full27 source allocation root")
    originalRoot.requireScriptDataInputs(inputs)
    if (snapshot.inputs ne inputs) || snapshot.allocationBudget == null || snapshot.allocationBudget.isClosed ||
        !(snapshot.allocationBudget eq originalRoot) && !snapshot.allocationBudget.isCanonicalChildOf(originalRoot) ||
        !snapshot.allocationBudget.sharesAllocationLedger(originalRoot) then
      reject(s"$context snapshot does not belong to the exact root or its canonical child")
    snapshot.allocationBudget.requireScriptDataInputs(inputs)
    if snapshot.schema != inputs.schema || snapshot.connection.catalog != inputs.schema ||
        snapshot.relations.map(_.table) != inputs.canonicalOrder.map(_.table) then
      reject(s"$context requires the complete canonical Full27 snapshot")

  private def requireManifest(snapshot: MySqlScriptSchemaSnapshot, plan: DbFirstWorldAdoptionPlanData): Unit =
    val table = ManifestTable
    val row = onlyRow(snapshot, table)
    requireInteger(snapshot, table, row, "singleton_fixed1", 1L)
    requireText(snapshot, table, row, "backend", "MYSQL")
    requireInteger(snapshot, table, row, "format_version", 6L)
    requireText(snapshot, table, row, "bootstrap_state", "READY")
    requireText(snapshot, table, row, "import_state", "NONE")
    requireBinaryUuid(snapshot, table, row, "provisioning_uuid", plan.provisioningUuid())
    requireBinaryUuid(snapshot, table, row, "expected_server_uuid", plan.mysqlServerUuid())
    requireBinaryUuid(snapshot, table, row, "expected_binding_uuid", plan.bindingUuid())
    requireBinaryEquals(snapshot, table, row, "ddl_sha256", HexFormat.of().parseHex(plan.ddlSha256()))
    Vector("import_manifest_sha256", "import_operation_uuid", "import_snapshot_sha256", "import_receipt_sha256")
      .foreach(name => requireNull(snapshot, table, row, name))
    requireBinaryLength(snapshot, table, row, "configuration_sha256", 32)
    requireBinaryLength(snapshot, table, row, "guard_manifest_sha256", 32)
    requireText(snapshot, table, row, "limit_profile_encoding", "FP_SOURCE_LIMIT_PROFILE_BE_V1")
    val profileBytes = snapshot.inputs.sourceLimitProfile.canonicalBytes()
    requireBinaryEquals(snapshot, table, row, "limit_profile_bytes", profileBytes)
    requireBinaryEquals(snapshot, table, row, "limit_profile_sha256", MessageDigest.getInstance("SHA-256").digest(profileBytes))
    val profile = snapshot.inputs.sourceLimitProfile
    Vector(
      "max_region_bytes" -> profile.maxRegionBytes(),
      "max_chunk_bytes" -> profile.maxChunkBytes(),
      "max_decompressed_bytes" -> profile.maxDecompressedBytes(),
      "max_nbt_depth" -> profile.maxNbtDepth().toLong,
      "max_members" -> profile.maxMembers(),
      "max_retained_bytes" -> profile.maxRetainedBytes(),
      "max_sql_packet_bytes" -> profile.maxSqlPacketBytes(),
      "max_native_artifact_bytes" -> profile.maxNativeArtifactBytes()
    ).foreach { (name, expected) => requireInteger(snapshot, table, row, name, expected) }
    requireText(snapshot, table, row, "source_provenance_kind", "MANAGED_INSTALL")
    val provenance = binaryCell(snapshot, table, row, "source_provenance_bytes")
    val provenanceSha = binaryCell(snapshot, table, row, "source_provenance_sha256")
    if provenance.length == 0 || provenanceSha.length != 32 then
      reject("Full27 managed-install provenance bytes or digest has an invalid length")
    val digest = MessageDigest.getInstance("SHA-256")
    var index = 0
    while index < provenance.length do
      digest.update(provenance.byteAt(index))
      index += 1
    if !provenanceSha.equalsBytes(digest.digest()) then
      reject("Full27 managed-install provenance digest differs from its bytes")

  private def requireManagedInstallRows(snapshot: MySqlScriptSchemaSnapshot,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Option[Vector[Byte]],
      expectedQuotaMaximumRecords: Long,
      kind: Kind): Unit =
    MySqlDbFirstBootstrapGraph.requireBinding(snapshot,
      if kind == Kind.RELEASE then PhysicalRecoveryCutoverState.Certified else bindingState(snapshot), plan.bindingUuid())
    val state = textCell(snapshot, BindingTable, onlyRow(snapshot, BindingTable), "cutover_state")
    val stage = textCell(snapshot, IntentTable, onlyRow(snapshot, IntentTable), "stage")
    val expectedSlot = (state, stage) match
      case ("LEGACY_UNCERTIFIED", "PREPARED") if kind == Kind.HANDOFF && markerAckSha256.isEmpty => plan.prepareWriteUuid()
      case ("CERTIFIED", "COMPLETE") if markerAckSha256.nonEmpty => plan.certifyWriteUuid()
      case _ => reject("writer settlement requires exact LEGACY/PREPARED or CERTIFIED/COMPLETE phase pairing")
    if kind == Kind.RELEASE && (state != "CERTIFIED" || stage != "COMPLETE" || markerAckSha256.isEmpty) then
      reject("B1 RELEASE is permitted only after CERTIFIED/COMPLETE")

    val binding = onlyRow(snapshot, BindingTable)
    requireInteger(snapshot, BindingTable, binding, "singleton_fixed1", 1L)
    requireInteger(snapshot, BindingTable, binding, "format_version", 2L)
    requireBinaryUuid(snapshot, BindingTable, binding, "binding_uuid", plan.bindingUuid())
    val intent = onlyRow(snapshot, IntentTable)
    requireInteger(snapshot, IntentTable, intent, "singleton_fixed1", 1L)
    requireBinaryUuid(snapshot, IntentTable, intent, "bootstrap_operation_uuid", plan.bootstrapOperationUuid())
    requireBinaryUuid(snapshot, IntentTable, intent, "prepare_write_uuid", plan.prepareWriteUuid())
    requireBinaryUuid(snapshot, IntentTable, intent, "certify_write_uuid", plan.certifyWriteUuid())
    requireBinaryUuid(snapshot, IntentTable, intent, "binding_uuid", plan.bindingUuid())
    requireInteger(snapshot, IntentTable, intent, "binding_format_version", 2L)
    requireBinaryUuid(snapshot, IntentTable, intent, "provisioning_uuid", plan.provisioningUuid())
    requireText(snapshot, IntentTable, intent, "source_world_identity", sourceWorldIdentity)
    requireBinaryEquals(snapshot, IntentTable, intent, "provision_receipt_sha256", provisionReceiptSha256.toArray)
    val markerCell = cell(snapshot, IntentTable, intent, "marker_ack_sha256")
    markerAckSha256 match
      case None if markerCell == MySqlFormat3Cell.SqlNull => ()
      case Some(hash) => markerCell match
        case MySqlFormat3Cell.Binary(bytes) if bytes.equalsVector(hash) => ()
        case _ => reject("B1 writer settlement marker ACK differs from the exact supplied hash")
      case _ => reject("B1 PREPARED must retain SQL NULL marker ACK")
    requireExactGenesisCatalog(snapshot, plan.bindingUuid())
    MySqlScriptSchemaB1Graph.requireDbFirstFull27QuotaConfiguration(snapshot, expectedQuotaMaximumRecords)
    inputsEmptyBusinessRows(snapshot)
    val writer = onlyRow(snapshot, WriterTable)
    val operation = cell(snapshot, WriterTable, writer, "last_commit_operation_uuid") match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(expectedSlot) => expectedSlot
      case _ => reject(s"B1 writer settlement requires the exact $stage receipt slot")
    val fingerprint = cell(snapshot, WriterTable, writer, "last_commit_fingerprint") match
      case MySqlFormat3Cell.Binary(value) if value.length == 32 => value
      case _ => reject("B1 writer settlement receipt fingerprint must remain exact 32-byte data")
    if operation != expectedSlot || fingerprint.length != 32 then
      reject("B1 writer settlement slot differs from the exact phase operation")

  private def bindingState(snapshot: MySqlScriptSchemaSnapshot): PhysicalRecoveryCutoverState =
    textCell(snapshot, BindingTable, onlyRow(snapshot, BindingTable), "cutover_state") match
      case "LEGACY_UNCERTIFIED" => PhysicalRecoveryCutoverState.LegacyUncertified
      case "CERTIFIED" => PhysicalRecoveryCutoverState.Certified
      case _ => reject("B1 physical binding state is outside the closed Full27 settlement set")

  private def requireExactGenesisCatalog(snapshot: MySqlScriptSchemaSnapshot, bindingUuid: UUID): Unit =
    val table = CatalogTable
    val row = onlyRow(snapshot, table)
    requireBinaryUuid(snapshot, table, row, "binding_uuid", bindingUuid)
    requireInteger(snapshot, table, row, "catalog_revision", 0L)
    requireBinaryEquals(snapshot, table, row, "snapshot_bytes", Array.emptyByteArray)
    requireBinaryEquals(snapshot, table, row, "snapshot_sha256", MessageDigest.getInstance("SHA-256").digest(Array.emptyByteArray))
    requireInteger(snapshot, table, row, "next_version", 1L)
    requireNull(snapshot, table, row, "last_mutation_operation_uuid")

  private def inputsEmptyBusinessRows(snapshot: MySqlScriptSchemaSnapshot): Unit =
    snapshot.inputs.canonicalOrder.foreach { shape =>
      if !AllowedSeedTables.contains(shape.table) && snapshot.relation(shape.table).rows.nonEmpty then
        reject(s"B1 writer settlement requires empty ${shape.table} data")
    }

  private def requireHandoffWriterTransition(before: MySqlBusinessWriterRow,
      target: MySqlBusinessWriterRow, plan: DbFirstWorldAdoptionPlanData): Unit =
    if before == null || target == null || plan == null then reject("complete actual before/target W rows and DB-first plan are required")
    val phase = target.lastOperationUuid match
      case Some(id) if id == plan.prepareWriteUuid() => "PREPARED"
      case Some(id) if id == plan.certifyWriteUuid() => "COMPLETE"
      case _ => reject("B1 HANDOFF cannot change or invent the original writer receipt slot")
    val expectedOperation = if phase == "PREPARED" then plan.prepareWriteUuid() else plan.certifyWriteUuid()
    if before.bindingUuid != plan.bindingUuid() || target.bindingUuid != before.bindingUuid ||
        target.writerEpoch != safeNextEpoch(before.writerEpoch) ||
        before.leaseUntil.isAfter(before.databaseNow) || target.ownerBootUuid.isEmpty ||
        zeroUuid(target.ownerBootUuid.get) || target.ownerBootUuid == before.ownerBootUuid ||
        !target.leaseUntil.isAfter(before.databaseNow) ||
        target.leaseUntil.getNano % 1000 != 0 || target.lastOperationUuid != before.lastOperationUuid ||
        target.lastFingerprint != before.lastFingerprint || !before.lastOperationUuid.contains(expectedOperation) ||
        before.lastFingerprint.forall(_.size != 32) || before.leaseUntil.getNano % 1000 != 0 ||
        before.databaseNow.getNano % 1000 != 0 || target.databaseNow != before.databaseNow then
      reject(s"B1 $phase HANDOFF must preserve W slot, require expired old lease and install epoch+1 future owner")

  private def requireReleaseWriterTransition(before: MySqlBusinessWriterRow,
      target: MySqlBusinessWriterRow, plan: DbFirstWorldAdoptionPlanData): Unit =
    if before == null || target == null || plan == null then reject("complete actual before/target W rows and DB-first plan are required")
    if before.bindingUuid != plan.bindingUuid() || target.bindingUuid != before.bindingUuid ||
        !before.ownerBootUuid.exists(owner => !zeroUuid(owner)) || !before.lastOperationUuid.contains(plan.certifyWriteUuid()) ||
        before.lastFingerprint.forall(_.size != 32) || before.leaseUntil.getNano % 1000 != 0 ||
        before.databaseNow.getNano % 1000 != 0 || target.databaseNow != before.databaseNow || target.leaseUntil.getNano % 1000 != 0 ||
        target.lastOperationUuid != before.lastOperationUuid || target.lastFingerprint != before.lastFingerprint then
      reject("B1 RELEASE requires the exact owned CERTIFY writer and an unchanged receipt slot/database time")
    val current = fenceRow(before)
    val released = try MySqlWriterFenceRules.release(current, before.ownerBootUuid.get, before.databaseNow)
    catch
      case failure: IllegalArgumentException =>
        throw new SQLDataException("B1 RELEASE violates the actual writer-fence release rules", failure)
    if !sameWriterFence(released, fenceRow(target)) || target.writerEpoch != before.writerEpoch ||
        target.ownerBootUuid.nonEmpty || target.leaseUntil != before.databaseNow.minusNanos(1000L) then
      reject("B1 RELEASE must clear owner at the same epoch and set lease to actual databaseNow minus one microsecond")

  private def handoffBusinessRow(before: MySqlBusinessWriterRow,
      target: MySqlWriterFenceRow): MySqlBusinessWriterRow =
    if target == null then reject("exact target HANDOFF writer fence row is required")
    MySqlBusinessWriterRow(target.bindingUuid, target.epoch, target.ownerBootUuid, target.leaseUntil,
      target.lastCommitOperationUuid, target.lastCommitFingerprint, before.databaseNow)

  private def requireHandoffArguments(plan: DbFirstWorldAdoptionPlanData,
      beforeWriter: MySqlBusinessWriterRow, targetWriter: MySqlWriterFenceRow): Unit =
    if plan == null || beforeWriter == null || targetWriter == null then
      reject("complete DB-first plan and actual before/target W rows are required for B1 HANDOFF")

  private def safeNextEpoch(epoch: Long): Long =
    try MySqlWriterFenceRules.nextEpoch(epoch)
    catch
      case failure: ArithmeticException =>
        throw new SQLDataException("B1 HANDOFF writer epoch overflows its exact increment", failure)
      case failure: IllegalArgumentException =>
        throw new SQLDataException("B1 HANDOFF before writer epoch is invalid", failure)

  private def fenceRow(writer: MySqlBusinessWriterRow): MySqlWriterFenceRow =
    MySqlWriterFenceRow(writer.bindingUuid, writer.writerEpoch, writer.ownerBootUuid, writer.leaseUntil,
      writer.lastOperationUuid, writer.lastFingerprint)

  private def sameWriterFence(left: MySqlWriterFenceRow, right: MySqlWriterFenceRow): Boolean =
    left != null && right != null && left.bindingUuid == right.bindingUuid && left.epoch == right.epoch &&
      left.ownerBootUuid == right.ownerBootUuid && left.leaseUntil == right.leaseUntil &&
      left.lastCommitOperationUuid == right.lastCommitOperationUuid && left.lastCommitFingerprint == right.lastCommitFingerprint

  /** 仅构造原 root 下独立拥有的完整 DATA target，不授予 B1 阶段或 SQL 权限。 */
  private[mysql] def buildTarget(before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      targetWriter: MySqlBusinessWriterRow): MySqlScriptSchemaSnapshot =
    if before == null || inputs == null || originalRoot == null || targetWriter == null then
      reject("complete writer settlement DATA target inputs are required")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
      val child = originalRoot.newCanonicalDataScope()
      var primary: Throwable = null
      try
        MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          child.bindCanonicalDataPacketLimit(before.maxAllowedPacketBytes)
          child.reserveRetainedMemory(512L + Math.multiplyExact(inputs.canonicalOrder.size.toLong, 64L),
            "Full27 writer settlement relation vector")
          val rows = inputs.canonicalOrder.map { shape =>
            if shape.table == WriterTable then
              MySqlFormat3RelationSnapshot(WriterTable, Vector(writerRow(targetWriter, inputs, child)))
            else cloneRelation(before.relation(shape.table), child)
          }
          child.reserveRetainedMemory(512L, "Full27 writer settlement target snapshot holder")
          MySqlScriptSchemaSnapshot.decoded(before.schema, before.connection, before.observationScope,
            before.maxAllowedPacketBytes, inputs, rows, child)
        }
      catch
        case failure: Throwable =>
          primary = failure
          try child.closeScopedReadBudget()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private def cloneRelation(relation: MySqlFormat3RelationSnapshot,
      budget: MySqlFormat3ReadBudget): MySqlFormat3RelationSnapshot =
    budget.reserveRetainedMemory(Math.addExact(192L, Math.multiplyExact(relation.rows.size.toLong, 32L)),
      s"Full27 ${relation.table} owned relation rows")
    MySqlFormat3RelationSnapshot(relation.table, relation.rows.map(cloneRow(_, relation.table, budget)))

  private def cloneRow(row: Vector[MySqlFormat3Cell], table: String,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(row.size.toLong, 64L)),
      s"Full27 $table owned row/cell wrappers")
    row.map(cloneCell(_, table, budget))

  private def cloneCell(cell: MySqlFormat3Cell, table: String,
      budget: MySqlFormat3ReadBudget): MySqlFormat3Cell = cell match
    case MySqlFormat3Cell.SqlNull => MySqlFormat3Cell.SqlNull
    case MySqlFormat3Cell.Text(value) => ownedText(value, budget, s"Full27 $table owned text")
    case MySqlFormat3Cell.Binary(value) =>
      val bytes = value.copyBytes(budget, s"Full27 $table owned binary")
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))
    case MySqlFormat3Cell.SignedInteger(value) => MySqlFormat3Cell.SignedInteger(value)
    case MySqlFormat3Cell.Floating(value) => MySqlFormat3Cell.Floating(value)
    case MySqlFormat3Cell.DateTime(value) => MySqlFormat3Cell.DateTime(value)

  private def writerRow(writer: MySqlBusinessWriterRow,
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.requireScriptDataInputs(inputs)
    val shape = inputs.forTable(WriterTable)
    if shape.columns.map(_.name) != Vector("singleton_fixed1", "binding_uuid", "writer_epoch", "owner_boot_uuid",
        "lease_until", "last_commit_operation_uuid", "last_commit_fingerprint") then
      reject("Full27 runtime writer columns differ from the frozen complete row")
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(shape.columns.size.toLong, 64L)),
      "Full27 writer settlement target row/cell wrappers")
    shape.columns.map(_.name).map {
      case "singleton_fixed1" => MySqlFormat3Cell.SignedInteger(1L)
      case "binding_uuid" => ownedUuid(writer.bindingUuid, budget, "Full27 W binding UUID")
      case "writer_epoch" => MySqlFormat3Cell.SignedInteger(writer.writerEpoch)
      case "owner_boot_uuid" => writer.ownerBootUuid match
        case None => MySqlFormat3Cell.SqlNull
        case Some(value) => ownedUuid(value, budget, "Full27 W owner UUID")
      case "lease_until" => MySqlFormat3Cell.DateTime(writer.leaseUntil)
      case "last_commit_operation_uuid" => writer.lastOperationUuid match
        case None => MySqlFormat3Cell.SqlNull
        case Some(value) => ownedUuid(value, budget, "Full27 W receipt operation UUID")
      case "last_commit_fingerprint" => writer.lastFingerprint match
        case None => MySqlFormat3Cell.SqlNull
        case Some(value) => ownedBinary(value, budget, "Full27 W receipt fingerprint")
      case other => reject(s"unknown Full27 writer column $other")
    }

  private def ownedUuid(value: UUID, budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell =
    if value == null then reject(s"$context is null")
    budget.reserveRetainedMemory(80L, context)
    val bytes = new Array[Byte](16)
    var index = 0
    while index < 8 do
      bytes(index) = (value.getMostSignificantBits >>> (56 - index * 8)).toByte
      bytes(index + 8) = (value.getLeastSignificantBits >>> (56 - index * 8)).toByte
      index += 1
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))

  private def ownedBinary(value: Vector[Byte], budget: MySqlFormat3ReadBudget,
      context: String): MySqlFormat3Cell =
    if value == null then reject(s"$context is null")
    budget.reserveRetainedMemory(Math.addExact(value.size.toLong, 64L), context)
    val bytes = new Array[Byte](value.size)
    var index = 0
    while index < bytes.length do
      bytes(index) = value(index)
      index += 1
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))

  private def ownedText(value: String, budget: MySqlFormat3ReadBudget,
      context: String): MySqlFormat3Cell.Text =
    val utf8 = MySqlFormat3Utf8.byteLength(value)
    val characterBytes = Math.multiplyExact(value.length.toLong, 2L)
    val retained = Math.addExact(64L, Math.max(utf8, characterBytes))
    val temporary = Math.addExact(characterBytes, 32L)
    budget.reserveRetainedMemory(retained, context)
    budget.reserveTemporaryMemory(temporary, s"$context clone workspace")
    var primary: Throwable = null
    try MySqlFormat3Cell.Text(new String(value.toCharArray))
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(temporary, s"$context clone workspace")))

  private def requireOnlyWriterChanged(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    if before.relations.size != 27 || target.relations.size != 27 then
      reject("writer settlement requires the complete Full27 before and target images")
    var index = 0
    while index < before.relations.size do
      val oldRelation = before.relations(index)
      val newRelation = target.relations(index)
      if oldRelation.table != newRelation.table then reject("writer settlement changed canonical Full27 relation order")
      if !ChangedTables.contains(oldRelation.table) && !sameRelation(oldRelation, newRelation) then
        reject(s"writer settlement changed unrelated Full27 table ${oldRelation.table}")
      index += 1

  private def sameRelation(left: MySqlFormat3RelationSnapshot,
      right: MySqlFormat3RelationSnapshot): Boolean =
    if left == null || right == null || left.table != right.table || left.rows.size != right.rows.size then false
    else
      var rowIndex = 0
      while rowIndex < left.rows.size do
        val a = left.rows(rowIndex)
        val b = right.rows(rowIndex)
        if a.size != b.size then return false
        var columnIndex = 0
        while columnIndex < a.size do
          if !sameCell(a(columnIndex), b(columnIndex)) then return false
          columnIndex += 1
        rowIndex += 1
      true

  private def sameCell(left: MySqlFormat3Cell, right: MySqlFormat3Cell): Boolean = (left, right) match
    case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => true
    case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) => a == b
    case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a == b
    case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => a == b
    case (MySqlFormat3Cell.Floating(a), MySqlFormat3Cell.Floating(b)) => a == b
    case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a == b
    case _ => false

  private def requireSameHeader(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    if before == null || target == null || (before.inputs ne target.inputs) || before.schema != target.schema ||
        before.connection != target.connection || before.observationScope != target.observationScope ||
        before.maxAllowedPacketBytes != target.maxAllowedPacketBytes then
      reject("writer settlement before/target exact Full27 header differs")

  private def onlyRow(snapshot: MySqlScriptSchemaSnapshot, table: String): Vector[MySqlFormat3Cell] =
    snapshot.relation(table).rows match
      case Vector(row) => row
      case _ => reject(s"writer settlement requires exactly one $table row")

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Cell =
    val shape = snapshot.inputs.forTable(table)
    if row == null || row.size != shape.columns.size then reject(s"$table complete row width differs")
    val index = shape.columns.indexWhere(_.name == column)
    if index < 0 then reject(s"$table.$column is outside the frozen Full27 shape")
    row(index)

  private def requireInteger(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: Long): Unit =
    if cell(snapshot, table, row, column) != MySqlFormat3Cell.SignedInteger(expected) then
      reject(s"$table.$column differs from its exact writer-settlement integer")

  private def textCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): String = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => reject(s"$table.$column is not typed text")

  private def requireText(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: String): Unit =
    if textCell(snapshot, table, row, column) != expected then reject(s"$table.$column differs from its exact writer-settlement value")

  private def requireBinaryUuid(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: UUID): Unit = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.Binary(value) if value.equalsUuid(expected) => ()
    case _ => reject(s"$table.$column differs from the exact plan UUID")

  private def binaryCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Binary = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => reject(s"$table.$column is not typed binary")

  private def requireBinaryEquals(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: Array[Byte]): Unit =
    if !binaryCell(snapshot, table, row, column).equalsBytes(expected) then
      reject(s"$table.$column differs from the exact writer-settlement bytes")

  private def requireBinaryLength(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: Int): Unit =
    if binaryCell(snapshot, table, row, column).length != expected then reject(s"$table.$column has an invalid digest length")

  private def requireNull(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Unit =
    if cell(snapshot, table, row, column) != MySqlFormat3Cell.SqlNull then reject(s"$table.$column must remain SQL NULL")

  private def zeroUuid(value: UUID): Boolean =
    value == null || value.getMostSignificantBits == 0L && value.getLeastSignificantBits == 0L

  private def isZero(value: UUID): Boolean = zeroUuid(value)

  private def closeTargetAfterFailure(target: MySqlScriptSchemaSnapshot, primary: Throwable): Unit =
    if target != null && target.allocationBudget != null && !target.allocationBudget.isClosed then
      try target.allocationBudget.closeScopedReadBudget()
      catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)

  private def reject(message: String): Nothing =
    throw new SQLDataException("DB-first Full27 writer settlement rejected: " + message)
