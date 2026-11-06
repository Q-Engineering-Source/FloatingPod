package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.FirstBindingPlanData
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import java.io.{DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 两个封闭格式共享同一 pure DATA kernel；外部 typed facade 分别固定 plan/layout/family。 */
private[mysql] final class MySqlB1BootstrapGraph private[mysql](private val family: MySqlB1BootstrapGraph.Family):
  import MySqlB1BootstrapGraph.{PrepareBeforeWriter, prepareBeforeEpoch}
  private val EffectDomain = family.effectDomain
  private val BindingTable = "physical_recovery_binding"
  private val IntentTable = "binding_bootstrap_intent"
  private val WriterTable = "fp_runtime_writer"
  private val CatalogTable = "fp_script_catalog"
  private val ChangedTables = Set(BindingTable, IntentTable, WriterTable)

  /** 构造 PREPARE 的纯 DATA target。target 自有全部行字节；before child 关闭后仍可读取，任何新操作仍要求原 root 开启。 */
  def prepareTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Option[Long]
  ): MySqlScriptSchemaSnapshot =
    if originalRoot == null || originalRoot.isClosed || originalRoot.isScopedReadBudget then
      reject(s"exact open original ${family.label} root required before workspace allocation")
    withTemporary(originalRoot, 65536L, s"${family.label} B1 operation validation structures") {
      requireInputs(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256, expectedQuotaMaximumRecords)
      requireSnapshotOwner(before, inputs, originalRoot, "PREPARE before")
      requireManifest(before, inputs, plan, originalRoot)
      requirePrepareWriterMatches(before, beforeWriter)
      requireUnownedSeed(beforeWriter, plan)
      requireEmpty(before, BindingTable)
      requireEmpty(before, IntentTable)
      requirePrepareSeedState(before, inputs, expectedQuotaMaximumRecords)
      requirePrepareWriterTarget(beforeWriter, targetWriter, plan)
      val beforeFingerprint = canonicalFingerprint(before)
      buildTarget(before, inputs, originalRoot) { child =>
        val binding = bindingRow(PhysicalRecoveryCutoverState.LegacyUncertified, plan.bindingUuid(), inputs, child)
        val intent = intentRow(plan, sourceWorldIdentity, provisionReceiptSha256,
          MySqlBootstrapIntentStage.Prepared, None, inputs, child)
        withEffectFingerprint("PREPARE", before, plan, sourceWorldIdentity, provisionReceiptSha256,
            beforeFingerprint, relationRows(before, BindingTable), relationRows(before, IntentTable),
            binding, intent, targetWriter, child, retainResult = true) { fingerprint =>
          val context = contextForEffect("PREPARE", fingerprint, prepareBeforeEpoch(beforeWriter), targetWriter, child)
          val writer = writerRow(targetWriter, context.canonicalFingerprintSha256.toArray, inputs, child)
          val target = materializeTarget(before, inputs, child, binding, intent, writer)
          requirePrepareTransitionInScope(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
            provisionReceiptSha256, beforeWriter, targetWriter, beforeFingerprint, child, expectedQuotaMaximumRecords)
          target
        }
      }
    }

  /** 只在已 PREPARED 且 所给 before catalog 已存在时构造 CERTIFY 的纯 DATA target。 */
  def certifyTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Option[Long]
  ): MySqlScriptSchemaSnapshot =
    if originalRoot == null || originalRoot.isClosed || originalRoot.isScopedReadBudget then
      reject(s"exact open original ${family.label} root required before workspace allocation")
    withTemporary(originalRoot, 65536L, s"${family.label} B1 operation validation structures") {
      requireInputs(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256, expectedQuotaMaximumRecords)
      requireHash(markerAckSha256, "CERTIFY marker ACK SHA-256")
      requireSnapshotOwner(before, inputs, originalRoot, "CERTIFY before")
      requireManifest(before, inputs, plan, originalRoot)
      requirePrepareWriterMatches(before, beforeWriter)
      requirePreparedBinding(before, plan)
      requireIntentMatches(before, plan, sourceWorldIdentity, provisionReceiptSha256,
        MySqlBootstrapIntentStage.Prepared, None)
      requireExistingCatalog(before, plan.bindingUuid())
      requireCertifySeedState(before, inputs, expectedQuotaMaximumRecords, plan.bindingUuid())
      requirePreparedWriterReceipt(beforeWriter, plan)
      requireCertifyWriterTarget(beforeWriter, targetWriter, plan)
      val beforeFingerprint = canonicalFingerprint(before)
      buildTarget(before, inputs, originalRoot) { child =>
        val binding = bindingRow(PhysicalRecoveryCutoverState.Certified, plan.bindingUuid(), inputs, child)
        val intent = intentRow(plan, sourceWorldIdentity, provisionReceiptSha256,
          MySqlBootstrapIntentStage.Complete, Some(markerAckSha256), inputs, child)
        withEffectFingerprint("CERTIFY", before, plan, sourceWorldIdentity, provisionReceiptSha256,
            beforeFingerprint, relationRows(before, BindingTable), relationRows(before, IntentTable),
            binding, intent, targetWriter, child, retainResult = true) { fingerprint =>
          val context = contextForEffect("CERTIFY", fingerprint, prepareBeforeEpoch(beforeWriter), targetWriter, child)
          val writer = writerRow(targetWriter, context.canonicalFingerprintSha256.toArray, inputs, child)
          val target = materializeTarget(before, inputs, child, binding, intent, writer)
          requireCertifyTransitionInScope(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
            provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, beforeFingerprint, child,
            expectedQuotaMaximumRecords)
          target
        }
      }
    }

  /** 对实际 PREPARE image 重验 public DATA 不变量；不授予 Fresh 或 SQL 资格。 */
  def requirePrepareTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Option[Long]
  ): Unit =
    if originalRoot == null || originalRoot.isClosed || originalRoot.isScopedReadBudget then
      reject(s"exact open original ${family.label} root required before workspace allocation")
    withTemporary(originalRoot, 65536L, s"${family.label} B1 operation validation structures") {
      requireInputs(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256, expectedQuotaMaximumRecords)
      requireSnapshotOwner(before, inputs, originalRoot, "PREPARE before")
      requireSnapshotOwner(target, inputs, originalRoot, "PREPARE target")
      requireSameHeader(before, target)
      requireManifest(before, inputs, plan, originalRoot)
      requireManifest(target, inputs, plan, originalRoot)
      requirePrepareWriterMatches(before, beforeWriter)
      requireUnownedSeed(beforeWriter, plan)
      requireEmpty(before, BindingTable)
      requireEmpty(before, IntentTable)
      requirePrepareSeedState(before, inputs, expectedQuotaMaximumRecords)
      requirePrepareWriterTarget(beforeWriter, targetWriter, plan)
      val beforeFingerprint = canonicalFingerprint(before)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
        requirePrepareTransitionInScope(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
          provisionReceiptSha256, beforeWriter, targetWriter, beforeFingerprint, originalRoot, expectedQuotaMaximumRecords)
      }
    }

  /** 对实际 CERTIFY image 重验 public DATA 不变量；marker ACK 仍只是调用方给定的 32 字节 DATA。 */
  def requireCertifyTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Option[Long]
  ): Unit =
    if originalRoot == null || originalRoot.isClosed || originalRoot.isScopedReadBudget then
      reject(s"exact open original ${family.label} root required before workspace allocation")
    withTemporary(originalRoot, 65536L, s"${family.label} B1 operation validation structures") {
      requireInputs(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256, expectedQuotaMaximumRecords)
      requireHash(markerAckSha256, "CERTIFY marker ACK SHA-256")
      requireSnapshotOwner(before, inputs, originalRoot, "CERTIFY before")
      requireSnapshotOwner(target, inputs, originalRoot, "CERTIFY target")
      requireSameHeader(before, target)
      requireManifest(before, inputs, plan, originalRoot)
      requireManifest(target, inputs, plan, originalRoot)
      requirePrepareWriterMatches(before, beforeWriter)
      requirePreparedBinding(before, plan)
      requireIntentMatches(before, plan, sourceWorldIdentity, provisionReceiptSha256,
        MySqlBootstrapIntentStage.Prepared, None)
      requireExistingCatalog(before, plan.bindingUuid())
      requireCertifySeedState(before, inputs, expectedQuotaMaximumRecords, plan.bindingUuid())
      requirePreparedWriterReceipt(beforeWriter, plan)
      requireCertifyWriterTarget(beforeWriter, targetWriter, plan)
      val beforeFingerprint = canonicalFingerprint(before)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
        requireCertifyTransitionInScope(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
          provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, beforeFingerprint, originalRoot,
          expectedQuotaMaximumRecords)
      }
    }

  private def requirePrepareTransitionInScope(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      beforeFingerprint: Vector[Byte],
      workspaceBudget: MySqlFormat3ReadBudget,
      expectedQuotaMaximumRecords: Option[Long]
  ): Unit =
    requireSnapshotOwner(before, inputs, originalRoot, "PREPARE before")
    requireSnapshotOwner(target, inputs, originalRoot, "PREPARE target")
    requireSameHeader(before, target)
    requireUnownedSeed(beforeWriter, plan)
    requireEmpty(before, BindingTable)
    requireEmpty(before, IntentTable)
    requireBinding(target, PhysicalRecoveryCutoverState.LegacyUncertified, plan.bindingUuid())
    requireIntentMatches(target, plan, sourceWorldIdentity, provisionReceiptSha256,
      MySqlBootstrapIntentStage.Prepared, None)
    requireWriterMatches(target, targetWriter)
    requirePrepareWriterTarget(beforeWriter, targetWriter, plan)
    requireOnlyB1TablesChanged(before, target)
    requireEffectFingerprint("PREPARE", before, target, plan, sourceWorldIdentity,
      provisionReceiptSha256, beforeFingerprint, targetWriter, workspaceBudget)

  private def requireCertifyTransitionInScope(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      beforeFingerprint: Vector[Byte],
      workspaceBudget: MySqlFormat3ReadBudget,
      expectedQuotaMaximumRecords: Option[Long]
  ): Unit =
    requireSnapshotOwner(before, inputs, originalRoot, "CERTIFY before")
    requireSnapshotOwner(target, inputs, originalRoot, "CERTIFY target")
    requireSameHeader(before, target)
    requirePreparedBinding(before, plan)
    requireIntentMatches(before, plan, sourceWorldIdentity, provisionReceiptSha256,
      MySqlBootstrapIntentStage.Prepared, None)
    requireExistingCatalog(before, plan.bindingUuid())
    requireCertifyTargetRows(before, target, inputs, expectedQuotaMaximumRecords, plan.bindingUuid())
    requireBinding(target, PhysicalRecoveryCutoverState.Certified, plan.bindingUuid())
    requireIntentMatches(target, plan, sourceWorldIdentity, provisionReceiptSha256,
      MySqlBootstrapIntentStage.Complete, Some(markerAckSha256))
    requireWriterMatches(target, targetWriter)
    requirePreparedWriterReceipt(beforeWriter, plan)
    requireCertifyWriterTarget(beforeWriter, targetWriter, plan)
    requireOnlyB1TablesChanged(before, target)
    requireCertifyTargetRows(before, target, inputs, expectedQuotaMaximumRecords, plan.bindingUuid())
    requireEffectFingerprint("CERTIFY", before, target, plan, sourceWorldIdentity,
      provisionReceiptSha256, beforeFingerprint, targetWriter, workspaceBudget)

  private def requireInputs(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      expectedQuotaMaximumRecords: Option[Long]
  ): Unit =
    if before == null || inputs == null || originalRoot == null || plan == null then
      reject(s"complete ${family.label} plan, inputs, before snapshot, and original root are required")
    if inputs.dataLayout != family.dataLayout then
      reject(s"B1 ${family.label} phases require their explicit typed DATA inputs")
    if inputs.deploymentBudget.isEmpty then reject(s"${family.label} deployment DATA requires its explicit deployment budget")
    family match
      case MySqlB1BootstrapGraph.Family.DbFirstFull27 =>
        expectedQuotaMaximumRecords match
          case Some(value) if value > 0L => ()
          case _ => reject("DB-first Full27 requires the explicit positive quota maximum")
      case MySqlB1BootstrapGraph.Family.DeploymentReceiptFull26 =>
        if expectedQuotaMaximumRecords.nonEmpty then reject(s"${family.label} does not accept a DB-first quota maximum")
    requireHash(provisionReceiptSha256, "provision receipt SHA-256")
    if sourceWorldIdentity == null || sourceWorldIdentity.isBlank then
      reject("B1 source world identity must be nonblank")
    val sourceShape = inputs.forTable(IntentTable)
    val sourceColumn = sourceShape.columns.find(_.name == "source_world_identity")
      .getOrElse(reject(s"B1 source identity column is outside the frozen ${family.label} layout"))
    if MySqlFormat3Utf8.byteLength(sourceWorldIdentity) > inputs.maximumPayloadBytes(IntentTable, sourceColumn).getOrElse(
      reject(s"B1 source identity has no explicit ${family.label} payload budget")) then
      reject("B1 source world identity exceeds its explicit DATA payload budget")
    if plan.schemaFormat() != family.schemaFormat || plan.physicalFormat() != family.physicalFormat ||
        plan.schema() != inputs.schema || plan.ddlSha256() == null ||
        !plan.ddlSha256().equalsIgnoreCase(inputs.ddlSha256) ||
        before.schema != inputs.schema || before.connection.catalog != inputs.schema ||
        plan.mysqlServerUuid() != before.connection.serverUuid then
      reject(s"schema${family.schemaFormat} plan differs from the exact ${family.label} schema/DDL/server inputs")
    val ids = Vector(plan.creationOperationUuid(), plan.bootstrapOperationUuid(), plan.prepareWriteUuid(),
      plan.certifyWriteUuid(), plan.bindingUuid(), plan.mysqlServerUuid(), plan.provisioningUuid())
    if ids.exists(id => id == null || id.getMostSignificantBits == 0L && id.getLeastSignificantBits == 0L) ||
        plan.prepareWriteUuid() == plan.certifyWriteUuid() || plan.bindingUuid().version() != 4 then
      reject(s"schema${family.schemaFormat} plan requires its complete nonzero, distinct fixed identities")

  /** 首个 schema6 PREPARE 只接受三条静态配置种子；Full27 的其他业务关系必须仍为空。 */
  private def requirePrepareSeedState(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, expectedQuotaMaximumRecords: Option[Long]): Unit =
    if family.requiresManagedInstall then
      val expected = expectedQuotaMaximumRecords.getOrElse(reject("DB-first Full27 quota maximum must be explicit"))
      if expected <= 0L then reject("DB-first Full27 quota maximum must be positive")
      requireManagedQuota(snapshot, expected)
      val allowedRows = Set("fp_schema_manifest", WriterTable, MySqlDeploymentReceiptSchemaDefinition.QuotaTable)
      inputs.canonicalOrder.foreach { shape =>
        if !allowedRows.contains(shape.table) && snapshot.relation(shape.table).rows.nonEmpty then
          reject(s"DB-first Full27 first PREPARE requires empty ${shape.table}")
      }

  /** CERTIFY 开始时仅允许原始 M/W/Q、固定 B/B1 与已完成 genesis 的 C。 */
  private def requireCertifySeedState(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, expectedQuotaMaximumRecords: Option[Long], bindingUuid: UUID): Unit =
    if family.requiresManagedInstall then
      val expected = expectedQuotaMaximumRecords.getOrElse(reject("DB-first Full27 quota maximum must be explicit"))
      if expected <= 0L then reject("DB-first Full27 quota maximum must be positive")
      requireManagedQuota(snapshot, expected)
      requireExistingCatalog(snapshot, bindingUuid)
      val allowedRows = Set("fp_schema_manifest", WriterTable, MySqlDeploymentReceiptSchemaDefinition.QuotaTable,
        BindingTable, IntentTable, CatalogTable)
      inputs.canonicalOrder.foreach { shape =>
        if !allowedRows.contains(shape.table) && snapshot.relation(shape.table).rows.nonEmpty then
          reject(s"DB-first Full27 CERTIFY requires empty ${shape.table}")
      }

  private def requireCertifyTargetRows(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      expectedQuotaMaximumRecords: Option[Long], bindingUuid: UUID): Unit =
    if family.requiresManagedInstall then
      requireCertifySeedState(before, inputs, expectedQuotaMaximumRecords, bindingUuid)
      requireCertifySeedState(target, inputs, expectedQuotaMaximumRecords, bindingUuid)

  private def requireManagedQuota(snapshot: MySqlScriptSchemaSnapshot, expectedMaximumRecords: Long): Unit =
    val table = MySqlDeploymentReceiptSchemaDefinition.QuotaTable
    val row = onlyRow(snapshot, table)
    requireBinaryEquals(snapshot, table, row, "namespace",
      java.util.HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))
    requireInteger(snapshot, table, row, "max_records", expectedMaximumRecords)
    requireInteger(snapshot, table, row, "used_records", 0L)
    requireInteger(snapshot, table, row, "quota_revision", 0L)

  private def requireSnapshotOwner(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget, context: String): Unit =
    if snapshot == null || inputs == null || originalRoot == null || originalRoot.isClosed ||
        originalRoot.isScopedReadBudget || (originalRoot.profile ne inputs.sourceLimitProfile) then
      reject(s"$context requires the exact open original ${family.label} allocation root")
    originalRoot.requireScriptDataInputs(inputs)
    if (snapshot.inputs ne inputs) || snapshot.allocationBudget == null || snapshot.allocationBudget.isClosed ||
        !(snapshot.allocationBudget eq originalRoot) && !snapshot.allocationBudget.isCanonicalChildOf(originalRoot) ||
        !snapshot.allocationBudget.sharesAllocationLedger(originalRoot) then
      reject(s"$context snapshot must belong to the exact original root or its canonical DATA child")
    snapshot.allocationBudget.requireScriptDataInputs(inputs)
    if snapshot.schema != inputs.schema || snapshot.connection.catalog != inputs.schema ||
        snapshot.relations.map(_.table) != inputs.canonicalOrder.map(_.table) then
      reject(s"$context requires the exact ${family.label} observation and canonical relation shape")

  private def requireSameHeader(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Unit =
    if before == null || target == null || (before.inputs ne target.inputs) || before.schema != target.schema ||
        before.connection != target.connection || before.observationScope != target.observationScope ||
        before.maxAllowedPacketBytes != target.maxAllowedPacketBytes then
      reject(s"${family.label} before/target observation header or exact inputs differ")

  private def requireManifest(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      plan: FirstBindingPlanData,
      budget: MySqlFormat3ReadBudget): Unit =
    val shape = inputs.forTable("fp_schema_manifest")
    val row = onlyRow(snapshot, "fp_schema_manifest")
    def cell(name: String): MySqlFormat3Cell = row(fieldIndex(shape, name))
    def integer(name: String, expected: Long): Unit =
      if cell(name) != MySqlFormat3Cell.SignedInteger(expected) then reject(s"${family.label} manifest $name differs from the exact plan")
    def text(name: String, expected: String): Unit =
      if cell(name) != MySqlFormat3Cell.Text(expected) then reject(s"${family.label} manifest $name differs from its closed DATA contract")
    def uuid(name: String, expected: UUID): Unit = cell(name) match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(expected) => ()
      case _ => reject(s"${family.label} manifest $name differs from the exact plan")
    def binary(name: String, expected: Array[Byte]): Unit = cell(name) match
      case MySqlFormat3Cell.Binary(value) if value.equalsBytes(expected) => ()
      case _ => reject(s"${family.label} manifest $name differs from the explicit source profile")
    def hashLength(name: String): Unit = cell(name) match
      case MySqlFormat3Cell.Binary(value) if value.length == 32 => ()
      case _ => reject(s"${family.label} manifest $name must be a typed SHA-256 value")

    if cell("singleton_fixed1") != MySqlFormat3Cell.SignedInteger(1L) then reject(s"${family.label} manifest singleton differs")
    text("backend", "MYSQL")
    integer("format_version", family.schemaFormat.toLong)
    text("bootstrap_state", "READY")
    text("import_state", "NONE")
    uuid("provisioning_uuid", plan.provisioningUuid())
    uuid("expected_server_uuid", plan.mysqlServerUuid())
    uuid("expected_binding_uuid", plan.bindingUuid())
    if !asciiHexEquals(cell("ddl_sha256"), plan.ddlSha256()) then
      reject(s"${family.label} manifest DDL SHA-256 differs from the exact schema${family.schemaFormat} plan")
    Vector("import_manifest_sha256", "import_operation_uuid", "import_snapshot_sha256", "import_receipt_sha256").foreach { name =>
      if cell(name) != MySqlFormat3Cell.SqlNull then reject(s"${family.label} manifest $name must remain SQL NULL")
    }
    hashLength("configuration_sha256")
    hashLength("guard_manifest_sha256")

    withTemporary(budget, 192L, s"${family.label} schema${family.schemaFormat} source profile comparison") {
      val profileBytes = inputs.sourceLimitProfile.canonicalBytes()
      val profileHash = inputs.sourceLimitProfile.sha256()
      text("limit_profile_encoding", "FP_SOURCE_LIMIT_PROFILE_BE_V1")
      binary("limit_profile_bytes", profileBytes)
      binary("limit_profile_sha256", profileHash)
      val profile = inputs.sourceLimitProfile
      Vector(
        "max_region_bytes" -> profile.maxRegionBytes(),
        "max_chunk_bytes" -> profile.maxChunkBytes(),
        "max_decompressed_bytes" -> profile.maxDecompressedBytes(),
        "max_nbt_depth" -> profile.maxNbtDepth().toLong,
        "max_members" -> profile.maxMembers(),
        "max_retained_bytes" -> profile.maxRetainedBytes(),
        "max_sql_packet_bytes" -> profile.maxSqlPacketBytes(),
        "max_native_artifact_bytes" -> profile.maxNativeArtifactBytes()
      ).foreach { (name, value) => integer(name, value) }
    }
    val provenanceKind = textCell(snapshot, "fp_schema_manifest", row, "source_provenance_kind")
    if family.requiresManagedInstall && provenanceKind != "MANAGED_INSTALL" then
      reject("DB-first schema6 requires exact MANAGED_INSTALL provenance")
    text("source_provenance_kind", provenanceKind)
    requireDigestPair(snapshot, "fp_schema_manifest", row, "source_provenance_bytes", "source_provenance_sha256")

  private def requireDigestPair(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], valueName: String, digestName: String): Unit =
    val value = binaryCell(snapshot, table, row, valueName)
    val expected = binaryCell(snapshot, table, row, digestName)
    if value.length == 0 || expected.length != 32 then reject(s"$table.$valueName source digest pair is incomplete")
    val digest = MessageDigest.getInstance("SHA-256")
    var index = 0
    while index < value.length do
      digest.update(value.byteAt(index))
      index += 1
    if !expected.equalsBytes(digest.digest()) then reject(s"$table.$valueName source digest differs from its bytes")
    val allowed = Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT") ++
      (if family.requiresManagedInstall then Set("MANAGED_INSTALL") else Set.empty)
    textCell(snapshot, table, row, "source_provenance_kind") match
      case value if allowed.contains(value) => ()
      case _ => reject(s"${family.label} manifest source provenance token is outside its closed DATA set")

  private[mysql] def requireWriterMatches(snapshot: MySqlScriptSchemaSnapshot,
      expected: MySqlBusinessWriterRow): Unit =
    if expected == null then reject("typed actual W row is required")
    val row = onlyRow(snapshot, WriterTable)
    if !integerCell(snapshot, WriterTable, row, "singleton_fixed1", 1L) ||
        !uuidCell(snapshot, WriterTable, row, "binding_uuid", expected.bindingUuid) ||
        !integerCell(snapshot, WriterTable, row, "writer_epoch", expected.writerEpoch) ||
        !optionalUuidCell(snapshot, WriterTable, row, "owner_boot_uuid", expected.ownerBootUuid) ||
        !dateTimeCell(snapshot, WriterTable, row, "lease_until", expected.leaseUntil) ||
        !optionalUuidCell(snapshot, WriterTable, row, "last_commit_operation_uuid", expected.lastOperationUuid) ||
        !optionalHashCell(snapshot, WriterTable, row, "last_commit_fingerprint", expected.lastFingerprint) then
      reject("typed actual MySqlBusinessWriterRow differs from the complete stored W tuple")

  private def requireWriterMatches(snapshot: MySqlScriptSchemaSnapshot,
      expected: MySqlFreshFormat3WriterTarget): Unit =
    if expected == null then reject("normalized target W required")
    val row = onlyRow(snapshot, WriterTable)
    if !integerCell(snapshot, WriterTable, row, "singleton_fixed1", 1L) ||
        !uuidCell(snapshot, WriterTable, row, "binding_uuid", expected.bindingUuid) ||
        !integerCell(snapshot, WriterTable, row, "writer_epoch", expected.writerEpoch) ||
        !optionalUuidCell(snapshot, WriterTable, row, "owner_boot_uuid", expected.ownerBootUuid) ||
        !dateTimeCell(snapshot, WriterTable, row, "lease_until", expected.leaseUntil) ||
        !uuidCell(snapshot, WriterTable, row, "last_commit_operation_uuid", expected.operationUuid) then
      reject("normalized target differs from complete stored W tuple")

  private def requirePrepareWriterMatches(snapshot:MySqlScriptSchemaSnapshot,writer:PrepareBeforeWriter):Unit = writer match
    case null => reject("complete PREPARE before writer required")
    case actual:MySqlBusinessWriterRow => requireWriterMatches(snapshot,actual)
    case actual:com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions =>
      val row=onlyRow(snapshot,WriterTable)
      val slot=Option(actual.lastSlot())
      if !integerCell(snapshot,WriterTable,row,"singleton_fixed1",1L) ||
          !uuidCell(snapshot,WriterTable,row,"binding_uuid",actual.binding()) ||
          !integerCell(snapshot,WriterTable,row,"writer_epoch",actual.epoch()) ||
          !optionalUuidCell(snapshot,WriterTable,row,"owner_boot_uuid",Option(actual.ownerBoot())) ||
          !dateTimeCell(snapshot,WriterTable,row,"lease_until",actual.leaseUntil()) ||
          !optionalUuidCell(snapshot,WriterTable,row,"last_commit_operation_uuid",slot.map(_.operation())) ||
          !optionalHashCell(snapshot,WriterTable,row,"last_commit_fingerprint",slot.map(_.fingerprint().toVector)) then
        reject("historical PREPARE writer assertions differ from complete original W")

  private def requireUnownedSeed(writer: PrepareBeforeWriter,
      plan: FirstBindingPlanData): Unit =
    writer match
      case null => reject("complete PREPARE before writer required")
      case actual:MySqlBusinessWriterRow =>
        if actual.bindingUuid != plan.bindingUuid() || actual.writerEpoch < 0L || actual.ownerBootUuid.nonEmpty ||
            actual.lastOperationUuid.nonEmpty || actual.lastFingerprint.nonEmpty then
          reject("PREPARE requires the actual unowned/no-receipt seed W row")
      case actual:com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions =>
        if actual.binding()!=plan.bindingUuid() || actual.epoch()<0L || actual.ownerBoot()!=null || actual.lastSlot()!=null then
          reject("PREPARE requires the complete original unowned/no-receipt seed assertions")

  private def requirePrepareWriterTarget(before: PrepareBeforeWriter,
      target: MySqlFreshFormat3WriterTarget,
      plan: FirstBindingPlanData): Unit =
    val expectedEpoch = try Math.addExact(prepareBeforeEpoch(before), 1L)
    catch case failure: ArithmeticException => throw new SQLDataException("PREPARE writer epoch overflows its exact increment", failure)
    if target == null || target.bindingUuid != plan.bindingUuid() || target.writerEpoch != expectedEpoch ||
        target.ownerBootUuid.isEmpty || zeroUuid(target.ownerBootUuid.get) ||
        target.operationUuid != plan.prepareWriteUuid() || target.leaseUntil.getNano % 1000 != 0 then
      reject("PREPARE target W must use the exact binding, incremented epoch, nonzero owner, lease, and fixed operation")

  private def requirePreparedBinding(snapshot: MySqlScriptSchemaSnapshot,
      plan: FirstBindingPlanData): Unit =
    requireBinding(snapshot, PhysicalRecoveryCutoverState.LegacyUncertified, plan.bindingUuid())

  private[mysql] def requireBinding(snapshot: MySqlScriptSchemaSnapshot,
      state: PhysicalRecoveryCutoverState, bindingUuid: UUID): Unit =
    val row = onlyRow(snapshot, BindingTable)
    if !integerCell(snapshot, BindingTable, row, "singleton_fixed1", 1L) ||
        !uuidCell(snapshot, BindingTable, row, "binding_uuid", bindingUuid) ||
        textCell(snapshot, BindingTable, row, "cutover_state") != state.databaseValue ||
        !integerCell(snapshot, BindingTable, row, "format_version", family.physicalFormat.toLong) then
      reject(s"${family.label} physical binding does not match its exact ${state.databaseValue} phase")

  private def requireIntentMatches(snapshot: MySqlScriptSchemaSnapshot,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte],
      stage: MySqlBootstrapIntentStage,
      markerAckSha256: Option[Vector[Byte]]): Unit =
    val row = onlyRow(snapshot, IntentTable)
    if !integerCell(snapshot, IntentTable, row, "singleton_fixed1", 1L) ||
        !uuidCell(snapshot, IntentTable, row, "bootstrap_operation_uuid", plan.bootstrapOperationUuid()) ||
        !uuidCell(snapshot, IntentTable, row, "prepare_write_uuid", plan.prepareWriteUuid()) ||
        !uuidCell(snapshot, IntentTable, row, "certify_write_uuid", plan.certifyWriteUuid()) ||
        !uuidCell(snapshot, IntentTable, row, "binding_uuid", plan.bindingUuid()) ||
        !integerCell(snapshot, IntentTable, row, "binding_format_version", family.physicalFormat.toLong) ||
        !uuidCell(snapshot, IntentTable, row, "provisioning_uuid", plan.provisioningUuid()) ||
        textCell(snapshot, IntentTable, row, "source_world_identity") != sourceWorldIdentity ||
        !hashCell(snapshot, IntentTable, row, "provision_receipt_sha256", receiptSha256) ||
        textCell(snapshot, IntentTable, row, "stage") != stageToken(stage) ||
        !optionalHashCell(snapshot, IntentTable, row, "marker_ack_sha256", markerAckSha256) then
      reject(s"${family.label} B1 intent does not match the exact ${stageToken(stage)} plan and source DATA")

  private def requireExistingCatalog(snapshot: MySqlScriptSchemaSnapshot, bindingUuid: UUID): Unit =
    val row = onlyRow(snapshot, CatalogTable)
    if !uuidCell(snapshot, CatalogTable, row, "binding_uuid", bindingUuid) then
      reject("CERTIFY requires the existing supplied singleton catalog for the exact binding")
    if family.requiresManagedInstall then
      requireInteger(snapshot, CatalogTable, row, "catalog_revision", 0L)
      requireBinaryEquals(snapshot, CatalogTable, row, "snapshot_bytes", Array.emptyByteArray)
      requireBinaryEquals(snapshot, CatalogTable, row, "snapshot_sha256", MessageDigest.getInstance("SHA-256").digest(Array.emptyByteArray))
      requireInteger(snapshot, CatalogTable, row, "next_version", 1L)
      if cell(snapshot, CatalogTable, row, "last_mutation_operation_uuid") != MySqlFormat3Cell.SqlNull then
        reject("DB-first Full27 CERTIFY requires the exact empty genesis catalog")

  private def requirePreparedWriterReceipt(writer: PrepareBeforeWriter,
      plan: FirstBindingPlanData): Unit =
    writer match
      case null=>reject("complete CERTIFY before writer required")
      case actual:MySqlBusinessWriterRow=>
        if actual.bindingUuid != plan.bindingUuid() || actual.writerEpoch < 0L ||
            actual.ownerBootUuid.isEmpty || zeroUuid(actual.ownerBootUuid.get) ||
            !actual.lastOperationUuid.contains(plan.prepareWriteUuid()) ||
            actual.lastFingerprint.forall(_.size != 32) || actual.leaseUntil.getNano % 1000 != 0 then
          reject("CERTIFY requires the original PREPARE writer receipt and unchanged live tuple")
      case actual:com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions=>
        val slot=actual.lastSlot()
        if actual.binding()!=plan.bindingUuid() || actual.epoch()<0L || actual.ownerBoot()==null ||
            zeroUuid(actual.ownerBoot()) || slot==null || slot.operation()!=plan.prepareWriteUuid() ||
            slot.fingerprint().length!=32 || actual.leaseUntil().getNano%1000!=0 then
          reject("historical CERTIFY requires the complete original PREPARE receipt and W tuple")

  private def requireCertifyWriterTarget(before: PrepareBeforeWriter,
      target: MySqlFreshFormat3WriterTarget,
      plan: FirstBindingPlanData): Unit =
    val (binding,owner,lease)=before match
      case actual:MySqlBusinessWriterRow=>(actual.bindingUuid,actual.ownerBootUuid,actual.leaseUntil)
      case actual:com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions=>(actual.binding(),Option(actual.ownerBoot()),actual.leaseUntil())
      case null=>reject("complete CERTIFY before writer required")
    if target == null || target.bindingUuid != binding || target.writerEpoch != prepareBeforeEpoch(before) ||
        target.ownerBootUuid != owner || target.leaseUntil != lease ||
        target.operationUuid != plan.certifyWriteUuid() || target.leaseUntil.getNano % 1000 != 0 then
      reject("CERTIFY must preserve exact W binding/epoch/owner/lease and use its fixed operation")

  private def requireOnlyB1TablesChanged(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    val oldRelations = before.relations
    val newRelations = target.relations
    if oldRelations.size != newRelations.size then reject(s"${family.label} B1 phase changed the complete relation count")
    var index = 0
    while index < oldRelations.size do
      val left = oldRelations(index)
      val right = newRelations(index)
      if left.table != right.table then reject(s"${family.label} B1 phase changed canonical relation order")
      if !ChangedTables.contains(left.table) && !sameRelation(left, right) then
        reject(s"${family.label} B1 phase changed unrelated relation ${left.table}")
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

  private def requireEffectFingerprint(stage: String,
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeFingerprint: Vector[Byte],
      writer: MySqlFreshFormat3WriterTarget,
      workspaceBudget: MySqlFormat3ReadBudget): Unit =
    val targetBinding = onlyRow(target, BindingTable)
    val targetIntent = onlyRow(target, IntentTable)
    withEffectFingerprint(stage, before, plan, sourceWorldIdentity, provisionReceiptSha256,
        beforeFingerprint, relationRows(before, BindingTable), relationRows(before, IntentTable),
        targetBinding, targetIntent, writer, workspaceBudget, retainResult = false) { digest =>
      val actual = binaryCell(target, WriterTable, onlyRow(target, WriterTable), "last_commit_fingerprint")
      val context = contextForEffect(stage, digest, writerEpoch(before), writer, workspaceBudget)
      if !actual.equalsBytes(context.canonicalFingerprintSha256.toArray) then reject(s"${family.label} $stage W fingerprint differs from the exact operation context")
    }

  /** 有界DATA派生；原effect domain/bytes不变，receipt始终使用实际operation context摘要。 */
  def contextForTransition(stage: String, before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      plan: FirstBindingPlanData, source: String, receipt: Vector[Byte],
      writer: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Option[Long]): MySqlOperationContext =
    if stage != "PREPARE" && stage != "CERTIFY" then reject(s"closed ${family.label} B1 phase required")
    if family.requiresManagedInstall then
      if before == null || target == null || before.inputs == null || (before.inputs ne target.inputs) then
        reject("complete matching DB-first Full27 transition inputs are required")
      val budget = target.allocationBudget
      requireInputs(before, before.inputs, budget, plan, source, receipt, expectedQuotaMaximumRecords)
      requireManifest(before, before.inputs, plan, budget)
      requireManifest(target, target.inputs, plan, budget)
      requireSameHeader(before, target)
      if stage == "PREPARE" then requirePrepareSeedState(before, before.inputs, expectedQuotaMaximumRecords)
      else requireCertifyTargetRows(before, target, before.inputs, expectedQuotaMaximumRecords, plan.bindingUuid())
      requireOnlyB1TablesChanged(before, target)
    val budget=target.allocationBudget
    withEffectFingerprint(stage,before,plan,source,receipt,canonicalFingerprint(before),
      relationRows(before,BindingTable),relationRows(before,IntentTable),onlyRow(target,BindingTable),onlyRow(target,IntentTable),
      writer,budget,retainResult=true) { raw =>
        val context=contextForEffect(stage,raw,writerEpoch(before),writer,budget)
        if !binaryCell(target,WriterTable,onlyRow(target,WriterTable),"last_commit_fingerprint").equalsBytes(context.canonicalFingerprintSha256.toArray) then
          reject(s"${family.label} target W differs from derived actual context")
        context
      }

  private def writerEpoch(image: MySqlScriptSchemaSnapshot): Long =
    val row=onlyRow(image,WriterTable)
    row(fieldIndex(image.inputs.forTable(WriterTable),"writer_epoch")) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => reject(s"actual ${family.label} before writer epoch required")

  private def contextForEffect(stage: String, raw: Array[Byte], beforeEpoch: Long,
      writer: MySqlFreshFormat3WriterTarget, budget: MySqlFormat3ReadBudget): MySqlOperationContext =
    budget.reserveRetainedMemory(2048L,s"${family.label} actual phase effect/context/receipt before allocation")
    MySqlOperationContext(writer.ownerBootUuid.getOrElse(reject("actual phase writer owner required")),writer.writerEpoch,
      writer.bindingUuid,writer.operationUuid,Vector(MySqlExpectedEffect(s"${family.effectScope}/$stage/${writer.bindingUuid}",beforeEpoch,raw.toVector)))

  /** Effect preimage: BE length-prefixed UTF8 domain/phase, eleven recipe fields (UUIDs BE128,
    * physical/schema int32, schema UTF8, D uppercase ASCII64), source UTF8, receipt int32+32,
    * before canonical length int64 and int32+SHA32, named old/new B/B1 row vectors using
    * the existing typed-cell codec, then binding/epoch/optional owner/DATETIME6/operation W
    * and a zero byte replacing only its self-referential fingerprint. SHA-256 covers all bytes.
    */
  private def withEffectFingerprint[A](stage: String,
      before: MySqlScriptSchemaSnapshot,
      plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeFingerprint: Vector[Byte],
      oldBindingRows: Vector[Vector[MySqlFormat3Cell]],
      oldIntentRows: Vector[Vector[MySqlFormat3Cell]],
      targetBinding: Vector[MySqlFormat3Cell],
      targetIntent: Vector[MySqlFormat3Cell],
      targetWriter: MySqlFreshFormat3WriterTarget,
      budget: MySqlFormat3ReadBudget,
      retainResult: Boolean)(consume: Array[Byte] => A): A =
    if (stage != "PREPARE" && stage != "CERTIFY") || before == null || plan == null ||
        sourceWorldIdentity == null || provisionReceiptSha256 == null || beforeFingerprint == null ||
        beforeFingerprint.size != 32 || targetBinding == null || targetIntent == null || targetWriter == null || budget == null then
      reject(s"complete ${family.label} B1 effect fingerprint inputs are required")
    val textTemporary = Math.addExact(2_048L, Math.multiplyExact(sourceWorldIdentity.length.toLong, 8L))
    val resultReservation = 96L
    val maxBytes = Math.min(Math.min(before.inputs.budget.snapshotBytes, before.inputs.budget.retainedBytes),
      before.inputs.sourceLimitProfile.maxRetainedBytes())
    budget.reserveTemporaryMemory(textTemporary, s"${family.label} $stage effect fingerprint writer workspace")
    var resultReserved = false
    var primary: Throwable = null
    try
      if retainResult then budget.reserveRetainedMemory(resultReservation, s"${family.label} $stage target W fingerprint bytes/owner")
      else budget.reserveTemporaryMemory(resultReservation, s"${family.label} $stage effect fingerprint result")
      resultReserved = true
      MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
        val digest = MessageDigest.getInstance("SHA-256")
        val sink = new EffectDigestOutputStream(digest, maxBytes)
        val output = new DataOutputStream(sink)
        MySqlFormat3SnapshotCanonical.writeString(output, EffectDomain)
        MySqlFormat3SnapshotCanonical.writeString(output, stage)
        writePlan(output, plan)
        MySqlFormat3SnapshotCanonical.writeString(output, sourceWorldIdentity)
        output.writeInt(provisionReceiptSha256.size)
        provisionReceiptSha256.foreach(value => output.writeByte(value.toInt))
        output.writeLong(before.canonicalByteLength)
        output.writeInt(beforeFingerprint.size)
        beforeFingerprint.foreach(value => output.writeByte(value.toInt))
        writeRelationRows(output, BindingTable, oldBindingRows, before.inputs)
        writeRelationRows(output, IntentTable, oldIntentRows, before.inputs)
        writeRelationRows(output, BindingTable, Vector(targetBinding), before.inputs)
        writeRelationRows(output, IntentTable, Vector(targetIntent), before.inputs)
        writeNormalizedWriter(output, targetWriter)
        output.flush()
        consume(digest.digest())
      }
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      val cleanup = Vector.newBuilder[() => Unit]
      cleanup += (() => budget.releaseTemporaryMemory(textTemporary, s"${family.label} $stage effect fingerprint writer workspace"))
      if resultReserved && !retainResult then
        cleanup += (() => budget.releaseTemporaryMemory(resultReservation, s"${family.label} $stage effect fingerprint result"))
      MySqlFormat3BudgetCleanup.run(primary, cleanup.result())

  private def writePlan(output: DataOutputStream,
      plan: FirstBindingPlanData): Unit =
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.creationOperationUuid())
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.bootstrapOperationUuid())
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.prepareWriteUuid())
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.certifyWriteUuid())
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.bindingUuid())
    output.writeInt(plan.physicalFormat())
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.mysqlServerUuid())
    MySqlFormat3SnapshotCanonical.writeString(output, plan.schema())
    output.writeInt(plan.schemaFormat())
    MySqlFormat3SnapshotCanonical.writeAscii(output, plan.ddlSha256().toUpperCase(java.util.Locale.ROOT))
    MySqlFormat3SnapshotCanonical.writeUuid(output, plan.provisioningUuid())

  private def writeRelationRows(output: DataOutputStream,
      table: String,
      rows: Vector[Vector[MySqlFormat3Cell]],
      inputs: MySqlScriptSchemaInputs): Unit =
    MySqlFormat3SnapshotCanonical.writeString(output, table)
    output.writeInt(rows.size)
    val shape = inputs.forTable(table)
    rows.foreach { row =>
      if row == null || row.size != shape.columns.size then reject(s"$table effect row differs from the frozen ${family.label} shape")
      output.writeInt(row.size)
      row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_, output))
    }

  private def writeNormalizedWriter(output: DataOutputStream,
      writer: MySqlFreshFormat3WriterTarget): Unit =
    MySqlFormat3SnapshotCanonical.writeUuid(output, writer.bindingUuid)
    output.writeLong(writer.writerEpoch)
    writer.ownerBootUuid match
      case None => output.writeByte(0)
      case Some(value) => output.writeByte(1); MySqlFormat3SnapshotCanonical.writeUuid(output, value)
    val lease = writer.leaseUntil
    output.writeInt(lease.getYear)
    output.writeByte(lease.getMonthValue)
    output.writeByte(lease.getDayOfMonth)
    output.writeByte(lease.getHour)
    output.writeByte(lease.getMinute)
    output.writeByte(lease.getSecond)
    output.writeInt(lease.getNano)
    MySqlFormat3SnapshotCanonical.writeUuid(output, writer.operationUuid)
    output.writeByte(0) // W 自身 fingerprint 的固定自引用标记；其余 W 字段全部承诺。

  private def buildTarget[A](before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget)(body: MySqlFormat3ReadBudget => A): A =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
      val child = originalRoot.newCanonicalDataScope()
      var primary: Throwable = null
      try
        MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          child.bindCanonicalDataPacketLimit(before.maxAllowedPacketBytes)
          body(child)
        }
      catch
        case failure: Throwable =>
          primary = failure
          try child.closeScopedReadBudget()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private def materializeTarget(before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget,
      binding: Vector[MySqlFormat3Cell],
      intent: Vector[MySqlFormat3Cell],
      writer: Vector[MySqlFormat3Cell]): MySqlScriptSchemaSnapshot =
    budget.reserveRetainedMemory(512L + Math.multiplyExact(inputs.canonicalOrder.size.toLong, 64L),
      s"${family.label} target snapshot/reference vector")
    val replacements = Map(
      BindingTable -> singletonRelation(BindingTable, binding, inputs, budget),
      IntentTable -> singletonRelation(IntentTable, intent, inputs, budget),
      WriterTable -> singletonRelation(WriterTable, writer, inputs, budget))
    val relations = inputs.canonicalOrder.map { shape =>
      replacements.getOrElse(shape.table, cloneRelation(before.relation(shape.table), budget))
    }
    budget.reserveRetainedMemory(512L, s"${family.label} target snapshot holder")
    MySqlScriptSchemaSnapshot.decoded(before.schema, before.connection, before.observationScope,
      before.maxAllowedPacketBytes, inputs, relations, budget)

  private def singletonRelation(table: String,
      row: Vector[MySqlFormat3Cell],
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): MySqlFormat3RelationSnapshot =
    if row.size != inputs.forTable(table).columns.size then reject(s"complete ${family.label} target row width required")
    budget.reserveRetainedMemory(192L, s"${family.label} $table relation holder")
    MySqlFormat3RelationSnapshot(table, Vector(row))

  private def cloneRelation(relation: MySqlFormat3RelationSnapshot,
      budget: MySqlFormat3ReadBudget): MySqlFormat3RelationSnapshot =
    budget.reserveRetainedMemory(Math.addExact(192L, Math.multiplyExact(relation.rows.size.toLong, 32L)),
      s"${family.label} ${relation.table} owned relation/row vector")
    val rows = relation.rows.map(cloneRow(_, relation.table, budget))
    MySqlFormat3RelationSnapshot(relation.table, rows)

  private def cloneRow(row: Vector[MySqlFormat3Cell],
      table: String,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(row.size.toLong, 64L)),
      s"${family.label} $table owned row/cell wrappers")
    row.map(cell => cloneCell(cell, table, budget))

  private def cloneCell(cell: MySqlFormat3Cell,
      table: String,
      budget: MySqlFormat3ReadBudget): MySqlFormat3Cell = cell match
    case MySqlFormat3Cell.SqlNull => MySqlFormat3Cell.SqlNull
    case MySqlFormat3Cell.Text(value) => ownedText(value, budget, s"${family.label} $table owned text")
    case MySqlFormat3Cell.Binary(value) =>
      val copied = value.copyBytes(budget, s"${family.label} $table owned binary")
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(copied, budget))
    case MySqlFormat3Cell.SignedInteger(value) => MySqlFormat3Cell.SignedInteger(value)
    case MySqlFormat3Cell.Floating(rawBits) => MySqlFormat3Cell.Floating(rawBits)
    case MySqlFormat3Cell.DateTime(value) => MySqlFormat3Cell.DateTime(value)

  private def bindingRow(state: PhysicalRecoveryCutoverState,
      bindingUuid: UUID,
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    buildRow(BindingTable, inputs, budget) { name => name match
      case "singleton_fixed1" => MySqlFormat3Cell.SignedInteger(1L)
      case "binding_uuid" => ownedUuid(bindingUuid, budget, s"${family.label} B binding UUID")
      case "cutover_state" => ownedText(state.databaseValue, budget, s"${family.label} B cutover state")
      case "format_version" => MySqlFormat3Cell.SignedInteger(family.physicalFormat.toLong)
      case _ => reject(s"unknown ${family.label} physical binding field $name")
    }

  private def intentRow(plan: FirstBindingPlanData,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte],
      stage: MySqlBootstrapIntentStage,
      markerAckSha256: Option[Vector[Byte]],
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    buildRow(IntentTable, inputs, budget) { name => name match
      case "singleton_fixed1" => MySqlFormat3Cell.SignedInteger(1L)
      case "bootstrap_operation_uuid" => ownedUuid(plan.bootstrapOperationUuid(), budget, s"${family.label} B1 $name")
      case "prepare_write_uuid" => ownedUuid(plan.prepareWriteUuid(), budget, s"${family.label} B1 $name")
      case "certify_write_uuid" => ownedUuid(plan.certifyWriteUuid(), budget, s"${family.label} B1 $name")
      case "binding_uuid" => ownedUuid(plan.bindingUuid(), budget, s"${family.label} B1 $name")
      case "binding_format_version" => MySqlFormat3Cell.SignedInteger(family.physicalFormat.toLong)
      case "provisioning_uuid" => ownedUuid(plan.provisioningUuid(), budget, s"${family.label} B1 $name")
      case "source_world_identity" => ownedText(sourceWorldIdentity, budget, s"${family.label} B1 source world identity")
      case "provision_receipt_sha256" => ownedBinary(receiptSha256, budget, s"${family.label} B1 provision receipt SHA")
      case "stage" => ownedText(stageToken(stage), budget, s"${family.label} B1 stage")
      case "marker_ack_sha256" => markerAckSha256 match
        case None => MySqlFormat3Cell.SqlNull
        case Some(bytes) => ownedBinary(bytes, budget, s"${family.label} B1 marker ACK SHA")
      case _ => reject(s"unknown ${family.label} B1 field $name")
    }

  private def writerRow(writer: MySqlFreshFormat3WriterTarget,
      fingerprint: Array[Byte],
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    buildRow(WriterTable, inputs, budget) { name => name match
      case "singleton_fixed1" => MySqlFormat3Cell.SignedInteger(1L)
      case "binding_uuid" => ownedUuid(writer.bindingUuid, budget, s"${family.label} W $name")
      case "writer_epoch" => MySqlFormat3Cell.SignedInteger(writer.writerEpoch)
      case "owner_boot_uuid" => writer.ownerBootUuid match
        case None => MySqlFormat3Cell.SqlNull
        case Some(id) => ownedUuid(id, budget, s"${family.label} W $name")
      case "lease_until" => MySqlFormat3Cell.DateTime(writer.leaseUntil)
      case "last_commit_operation_uuid" => ownedUuid(writer.operationUuid, budget, s"${family.label} W $name")
      case "last_commit_fingerprint" => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(fingerprint, budget))
      case _ => reject(s"unknown ${family.label} W field $name")
    }

  private def buildRow(table: String,
      inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget)(cell: String => MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    budget.requireScriptDataInputs(inputs)
    val shape = inputs.forTable(table)
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(shape.columns.size.toLong, 64L)),
      s"${family.label} $table target row/cell wrappers")
    shape.columns.map(column => cell(column.name))

  private def ownedUuid(value: UUID,
      budget: MySqlFormat3ReadBudget,
      context: String): MySqlFormat3Cell =
    if value == null then reject(s"$context is null")
    budget.reserveRetainedMemory(80L, context)
    val bytes = new Array[Byte](16)
    putLong(bytes, 0, value.getMostSignificantBits)
    putLong(bytes, 8, value.getLeastSignificantBits)
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))

  private def ownedBinary(value: Vector[Byte],
      budget: MySqlFormat3ReadBudget,
      context: String): MySqlFormat3Cell =
    if value == null then reject(s"$context is null")
    val reservation = Math.addExact(value.size.toLong, 64L)
    budget.reserveRetainedMemory(reservation, context)
    val bytes = new Array[Byte](value.size)
    var index = 0
    while index < bytes.length do
      bytes(index) = value(index)
      index += 1
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))

  private def ownedText(value: String,
      budget: MySqlFormat3ReadBudget,
      context: String): MySqlFormat3Cell.Text =
    if value == null then reject(s"$context is null")
    val utf8 = MySqlFormat3Utf8.byteLength(value)
    val charBytes = Math.multiplyExact(value.length.toLong, 2L)
    val retained = Math.addExact(64L, Math.max(utf8, charBytes))
    val temporary = Math.addExact(charBytes, 32L)
    budget.reserveRetainedMemory(retained, context)
    budget.reserveTemporaryMemory(temporary, s"$context UTF-16 clone workspace")
    var primary: Throwable = null
    try
      val chars = value.toCharArray
      MySqlFormat3Cell.Text(new String(chars))
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(temporary, s"$context UTF-16 clone workspace")))

  private def putLong(bytes: Array[Byte], offset: Int, value: Long): Unit =
    var index = 0
    while index < 8 do
      bytes(offset + index) = (value >>> (56 - 8 * index)).toByte
      index += 1

  private def canonicalFingerprint(snapshot: MySqlScriptSchemaSnapshot): Vector[Byte] =
    val hash = snapshot.canonicalSha256
    if hash == null || hash.size != 32 || snapshot.canonicalByteLength <= 0L then
      reject(s"${family.label} before snapshot has no complete canonical identity")
    hash

  private def relationRows(snapshot: MySqlScriptSchemaSnapshot,
      table: String): Vector[Vector[MySqlFormat3Cell]] = snapshot.relation(table).rows

  private def requireEmpty(snapshot: MySqlScriptSchemaSnapshot, table: String): Unit =
    if snapshot.relation(table).rows.nonEmpty then reject(s"PREPARE before $table must be absent")

  private def onlyRow(snapshot: MySqlScriptSchemaSnapshot,
      table: String): Vector[MySqlFormat3Cell] = snapshot.relation(table).rows match
    case Vector(row) => row
    case _ => reject(s"${family.label} B1 phase requires exactly one $table row")

  private def fieldIndex(shape: MySqlFormat3RelationShape, name: String): Int =
    val index = shape.columns.indexWhere(_.name == name)
    if index < 0 then reject(s"${shape.table}.$name is outside its frozen ${family.label} shape")
    index

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String): MySqlFormat3Cell =
    val shape = snapshot.inputs.forTable(table)
    if row == null || row.size != shape.columns.size then reject(s"$table complete ${family.label} row width differs")
    row(fieldIndex(shape, name))

  private def integerCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Long): Boolean =
    cell(snapshot, table, row, name) == MySqlFormat3Cell.SignedInteger(expected)

  private def requireInteger(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Long): Unit =
    if !integerCell(snapshot, table, row, name, expected) then reject(s"$table.$name differs from its exact integer")

  private def requireBinaryEquals(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Array[Byte]): Unit =
    if !binaryCell(snapshot, table, row, name).equalsBytes(expected) then reject(s"$table.$name differs from its exact bytes")

  private def textCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String): String = cell(snapshot, table, row, name) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => reject(s"$table.$name is not typed text")

  private def binaryCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String): MySqlFormat3Binary = cell(snapshot, table, row, name) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => reject(s"$table.$name is not typed binary")

  private def uuidCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: UUID): Boolean =
    expected != null && (cell(snapshot, table, row, name) match
      case MySqlFormat3Cell.Binary(value) => value.equalsUuid(expected)
      case _ => false)

  private def optionalUuidCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Option[UUID]): Boolean =
    if expected == null || expected.exists(_ == null) then false
    else expected match
      case None => cell(snapshot, table, row, name) == MySqlFormat3Cell.SqlNull
      case Some(value) => uuidCell(snapshot, table, row, name, value)

  private def hashCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Vector[Byte]): Boolean =
    expected != null && (cell(snapshot, table, row, name) match
      case MySqlFormat3Cell.Binary(value) => value.equalsVector(expected)
      case _ => false)

  private def optionalHashCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: Option[Vector[Byte]]): Boolean =
    if expected == null || expected.exists(_ == null) then false
    else expected match
      case None => cell(snapshot, table, row, name) == MySqlFormat3Cell.SqlNull
      case Some(value) => hashCell(snapshot, table, row, name, value)

  private def dateTimeCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], name: String, expected: LocalDateTime): Boolean =
    expected != null && cell(snapshot, table, row, name) == MySqlFormat3Cell.DateTime(expected)

  private def asciiHexEquals(cell: MySqlFormat3Cell, hex: String): Boolean =
    if hex == null || hex.length != 64 then false
    else cell match
      case MySqlFormat3Cell.Binary(value) if value.length == 32 =>
        val digits = "0123456789ABCDEF"
        var index = 0
        while index < 32 do
          val high = digits.indexOf(Character.toUpperCase(hex.charAt(index * 2)))
          val low = digits.indexOf(Character.toUpperCase(hex.charAt(index * 2 + 1)))
          if high < 0 || low < 0 || value.byteAt(index) != ((high << 4) | low).toByte then return false
          index += 1
        true
      case _ => false

  private def stageToken(stage: MySqlBootstrapIntentStage): String = stage match
    case MySqlBootstrapIntentStage.Prepared => "PREPARED"
    case MySqlBootstrapIntentStage.Complete => "COMPLETE"

  private def requireHash(value: Vector[Byte], context: String): Unit =
    if value == null || value.size != 32 then reject(s"$context must contain exactly 32 bytes")

  private def zeroUuid(value: UUID): Boolean =
    value == null || value.getMostSignificantBits == 0L && value.getLeastSignificantBits == 0L

  private def withTemporary[A](budget: MySqlFormat3ReadBudget,
      amount: Long,
      context: String)(body: => A): A =
    budget.reserveTemporaryMemory(amount, context)
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(() => budget.releaseTemporaryMemory(amount, context)))

  private final class EffectDigestOutputStream(digest: MessageDigest, maximum: Long) extends OutputStream:
    if digest == null || maximum <= 0L then reject(s"${family.label} effect digest requires its exact positive cap")
    private var count = 0L
    override def write(value: Int): Unit =
      reserve(1L)
      digest.update(value.toByte)
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      if bytes == null || offset < 0 || length < 0 || offset > bytes.length - length then
        throw new IndexOutOfBoundsException(s"${family.label} effect digest byte slice")
      reserve(length.toLong)
      digest.update(bytes, offset, length)
    private def reserve(amount: Long): Unit =
      val next = Math.addExact(count, amount)
      if next > maximum then throw new SQLDataException(s"${family.label} B1 effect exceeds its explicit snapshot/retained budget")
      count = next

  private def reject(message: String): Nothing = throw new SQLDataException(s"${family.label} B1 DATA transition rejected: " + message)

private[mysql] object MySqlB1BootstrapGraph:
  private[mysql] type PrepareBeforeWriter = MySqlBusinessWriterRow | com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions

  private[mysql] sealed trait Family:
    def label: String
    def dataLayout: MySqlScriptSchemaDataLayout
    def schemaFormat: Int
    def physicalFormat: Int
    def effectDomain: String
    def effectScope: String
    def requiresManagedInstall: Boolean

  private[mysql] object Family:
    case object DeploymentReceiptFull26 extends Family:
      val label = "Full26"
      val dataLayout = MySqlScriptSchemaDataLayout.DeploymentReceiptFull26
      val schemaFormat = 5
      val physicalFormat = MySqlDeploymentReceiptSchemaDefinition.PhysicalBindingFormat
      val effectDomain = "floatingpod.mysql.script-publication1-deployment-receipt1-b1-effect/v1"
      val effectScope = "deployment-receipt"
      val requiresManagedInstall = false

    case object DbFirstFull27 extends Family:
      val label = "DB-first Full27"
      val dataLayout = MySqlScriptSchemaDataLayout.DbFirstFull27
      val schemaFormat = MySqlDbFirstSchemaDefinition.SchemaFormat
      val physicalFormat = MySqlDbFirstSchemaDefinition.PhysicalBindingFormat
      val effectDomain = "floatingpod.mysql.db-first-full27-b1-effect/v1"
      val effectScope = "db-first-full27"
      val requiresManagedInstall = true

  private[mysql] val deploymentReceiptFull26 = new MySqlB1BootstrapGraph(Family.DeploymentReceiptFull26)
  private[mysql] val dbFirstFull27 = new MySqlB1BootstrapGraph(Family.DbFirstFull27)

  private[mysql] def prepareBeforeEpoch(writer: PrepareBeforeWriter): Long = writer match
    case null => throw new SQLDataException("Full26 B1 DATA transition rejected: complete PREPARE before writer required")
    case value: MySqlBusinessWriterRow => value.writerEpoch
    case value: com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions => value.epoch()
