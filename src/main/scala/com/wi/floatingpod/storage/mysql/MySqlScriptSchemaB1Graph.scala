package com.wi.floatingpod.storage.mysql

import java.io.{DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.HexFormat

/** ScriptPublication1 B1 before/target DATA 的封闭比较与证书编码。 */
private[mysql] object MySqlScriptSchemaB1Graph:
  private val Domain = "floatingpod.mysql.script-publication1-b1-transition/v1"
  private val DeploymentReceiptFull26Domain =
    "floatingpod.mysql.script-publication1-deployment-receipt1-b1-transition/v1"
  private val DbFirstFull27Domain = "floatingpod.mysql.db-first-full27-b1-transition/v1"
  private val SnapshotTable = "fp_script_catalog"
  private val WriterTable = "fp_runtime_writer"
  private val RecoveryTable = "fp_recovery_operation"
  private val OperationTable = "fp_script_operation"
  private val CandidateTable = "fp_script_candidate"
  private val VersionTable = "fp_script_version"

  enum TransitionKind:
    case FreshGenesis, OrdinaryWriterHandoff

  final class Data private[MySqlScriptSchemaB1Graph] (
      val kind: TransitionKind,
      private[mysql] val inputs: MySqlScriptSchemaInputs,
      private[mysql] val beforeSnapshot: MySqlScriptSchemaSnapshot,
      private[mysql] val targetSnapshot: MySqlScriptSchemaSnapshot,
      private[mysql] val originalBudget: MySqlFormat3ReadBudget,
      private[mysql] val storageBudget: MySqlFormat3ReadBudget,
      private[mysql] val bytes: MySqlFormat3Binary,
      val sha256: Vector[Byte],
      val sha256Hex: String
  ):
    require(kind != null && inputs != null && beforeSnapshot != null && targetSnapshot != null &&
      (beforeSnapshot.inputs eq inputs) && (targetSnapshot.inputs eq inputs) &&
      originalBudget != null && storageBudget != null && bytes != null &&
      beforeSnapshot.allocationBudget.sharesAllocationLedger(originalBudget) &&
      targetSnapshot.allocationBudget.sharesAllocationLedger(originalBudget) &&
      sha256 != null && sha256.length == 32 && sha256Hex != null && sha256Hex.length == 64 &&
      storageBudget.sharesAllocationLedger(originalBudget), "complete closed-layout B1 certificate DATA ownership required")
    inputs.dataLayout match
      case MySqlScriptSchemaDataLayout.Full24 =>
        originalBudget.requireScriptPublication1Inputs(inputs)
        beforeSnapshot.allocationBudget.requireScriptPublication1Inputs(inputs)
        targetSnapshot.allocationBudget.requireScriptPublication1Inputs(inputs)
      case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 =>
        require(isRootOrCanonicalChild(beforeSnapshot.allocationBudget, originalBudget) &&
          isRootOrCanonicalChild(targetSnapshot.allocationBudget, originalBudget),
          "Full26 B1 snapshots must belong to the exact original root or its canonical DATA child")
        originalBudget.requireScriptDataInputs(inputs)
        beforeSnapshot.allocationBudget.requireScriptDataInputs(inputs)
        targetSnapshot.allocationBudget.requireScriptDataInputs(inputs)
      case MySqlScriptSchemaDataLayout.DbFirstFull27 =>
        require(isRootOrCanonicalChild(beforeSnapshot.allocationBudget, originalBudget) &&
          isRootOrCanonicalChild(targetSnapshot.allocationBudget, originalBudget),
          "Full27 B1 snapshots must belong to the exact original root or its canonical DATA child")
        originalBudget.requireScriptDataInputs(inputs)
        beforeSnapshot.allocationBudget.requireScriptDataInputs(inputs)
        targetSnapshot.allocationBudget.requireScriptDataInputs(inputs)
    def byteLength: Int = bytes.length
    override def toString: String = s"MySqlScriptSchemaB1Graph.Data[kind=$kind, bytes=$byteLength, sha256=$sha256Hex]"

  def requireFreshGenesisTransition(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    requireLayout(before, MySqlScriptSchemaDataLayout.Full24, "legacy genesis")
    requireLayout(target, MySqlScriptSchemaDataLayout.Full24, "legacy genesis")
    requireFreshGenesisTransitionInLayout(before, target)

  def requireDeploymentReceiptFull26FreshGenesisTransition(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    requireLayout(before, MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, "deployment receipt genesis")
    requireLayout(target, MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, "deployment receipt genesis")
    requireFreshGenesisTransitionInLayout(before, target)

  /** Schema6 catalog genesis is a distinct Full27 transition and certificate family. */
  def requireDbFirstFull27FreshGenesisTransition(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, expectedQuotaMaximumRecords: Long): Unit =
    if expectedQuotaMaximumRecords <= 0L then reject("DB-first Full27 genesis requires the explicit positive quota maximum")
    requireLayout(before, MySqlScriptSchemaDataLayout.DbFirstFull27, "DB-first Full27 genesis")
    requireLayout(target, MySqlScriptSchemaDataLayout.DbFirstFull27, "DB-first Full27 genesis")
    requireFreshGenesisTransitionInLayout(before, target, Some(expectedQuotaMaximumRecords))

  def requireDbFirstFull27QuotaConfiguration(snapshot: MySqlScriptSchemaSnapshot,
      expectedQuotaMaximumRecords: Long): Unit =
    if snapshot == null || snapshot.inputs == null ||
        snapshot.inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 then
      reject("DB-first Full27 quota validation requires its explicit schema6 layout")
    if expectedQuotaMaximumRecords <= 0L then reject("DB-first Full27 quota maximum must be explicit and positive")
    requireDbFirstQuotaConfiguration(snapshot, expectedQuotaMaximumRecords)

  private def requireFreshGenesisTransitionInLayout(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      expectedQuotaMaximumRecords: Option[Long] = None): Unit =
    requireSameHeader(before, target)
    requireOnlyChangedTable(before, target, SnapshotTable)
    if before.relation(SnapshotTable).rows.nonEmpty then reject("genesis before catalog must be empty")
    val catalogRows = target.relation(SnapshotTable).rows
    if catalogRows.size != 1 then reject("genesis target requires exactly one script catalog row")
    val isDeploymentFamily = target.inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
      target.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27
    val emptyHistory = Vector(OperationTable, CandidateTable, VersionTable) ++
      (if isDeploymentFamily then
        Vector(MySqlDeploymentReceiptSchemaDefinition.OperationTable)
      else Vector.empty)
    emptyHistory.foreach { table =>
      if before.relation(table).rows.nonEmpty || target.relation(table).rows.nonEmpty then
        reject(s"genesis requires empty $table history")
    }
    if target.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 &&
        (before.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.nonEmpty ||
          target.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.nonEmpty) then
      reject("DB-first Full27 catalog genesis requires an empty pod snapshot table")
    if target.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then
      val expected = expectedQuotaMaximumRecords.getOrElse(reject("DB-first Full27 quota maximum must be explicit"))
      Vector(before, target).foreach(requireDbFirstQuotaConfiguration(_, expected))
    else if isDeploymentFamily then
      Vector(before, target).foreach(requireZeroDeploymentQuota)
    requireGenesisCatalog(target, catalogRows.head)

  def requireOnlyWriterChanged(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    requireLayout(before, MySqlScriptSchemaDataLayout.Full24, "legacy ordinary handoff")
    requireLayout(target, MySqlScriptSchemaDataLayout.Full24, "legacy ordinary handoff")
    requireOnlyWriterChangedInLayout(before, target)

  def requireDeploymentReceiptFull26OnlyWriterChanged(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    requireLayout(before, MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, "deployment receipt ordinary handoff")
    requireLayout(target, MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, "deployment receipt ordinary handoff")
    requireOnlyWriterChangedInLayout(before, target)

  private def requireOnlyWriterChangedInLayout(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Unit =
    requireSameHeader(before, target)
    requireOnlyChangedTable(before, target, WriterTable)
    val oldRow = onlyRow(before, WriterTable)
    val newRow = onlyRow(target, WriterTable)
    Vector("singleton_fixed1", "binding_uuid", "last_commit_operation_uuid", "last_commit_fingerprint").foreach { name =>
      if cell(before, WriterTable, oldRow, name) != cell(target, WriterTable, newRow, name) then
        reject(s"ordinary handoff changed immutable writer field $name")
    }
    val changed = Vector("writer_epoch", "owner_boot_uuid", "lease_until").exists { name =>
      cell(before, WriterTable, oldRow, name) != cell(target, WriterTable, newRow, name)
    }
    if !changed then reject("ordinary handoff did not change any writer ownership field")

  def genesisData(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      originalBudget: MySqlFormat3ReadBudget): Data =
    requireFreshGenesisTransition(before, target)
    encode(TransitionKind.FreshGenesis, before, target, originalBudget)

  def deploymentReceiptFull26GenesisData(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      originalBudget: MySqlFormat3ReadBudget): Data =
    requireDeploymentReceiptFull26FreshGenesisTransition(before, target)
    encode(TransitionKind.FreshGenesis, before, target, originalBudget)

  def dbFirstFull27GenesisData(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      originalBudget: MySqlFormat3ReadBudget, expectedQuotaMaximumRecords: Long): Data =
    requireDbFirstFull27FreshGenesisTransition(before, target, expectedQuotaMaximumRecords)
    encode(TransitionKind.FreshGenesis, before, target, originalBudget)

  def ordinaryWriterHandoffData(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      originalBudget: MySqlFormat3ReadBudget): Data =
    requireOnlyWriterChanged(before, target)
    encode(TransitionKind.OrdinaryWriterHandoff, before, target, originalBudget)

  def deploymentReceiptFull26OrdinaryWriterHandoffData(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, originalBudget: MySqlFormat3ReadBudget): Data =
    requireDeploymentReceiptFull26OnlyWriterChanged(before, target)
    encode(TransitionKind.OrdinaryWriterHandoff, before, target, originalBudget)

  private def requireLayout(snapshot: MySqlScriptSchemaSnapshot,
      expected: MySqlScriptSchemaDataLayout, operation: String): Unit =
    if snapshot == null then reject(s"$operation requires complete before and target ScriptPublication1 snapshots")
    if snapshot.inputs == null || snapshot.inputs.dataLayout != expected then
      reject(s"$operation requires the explicit ${MySqlScriptSchemaInputs.familyName(expected)} layout")

  private def isRootOrCanonicalChild(snapshotBudget: MySqlFormat3ReadBudget,
      originalBudget: MySqlFormat3ReadBudget): Boolean =
    (snapshotBudget eq originalBudget) || snapshotBudget.isCanonicalChildOf(originalBudget)

  private def requireSameHeader(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Unit =
    if before == null || target == null then reject("complete before and target ScriptPublication1 snapshots are required")
    if (before.inputs ne target.inputs) || before.schema != target.schema || before.connection != target.connection ||
      before.observationScope != target.observationScope || before.maxAllowedPacketBytes != target.maxAllowedPacketBytes then
      reject("ScriptPublication1 before/target observation header or exact inputs differ")

  private def requireOnlyChangedTable(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      allowedTable: String): Unit =
    before.relations.zip(target.relations).foreach { (oldRelation, newRelation) =>
      if oldRelation.table != newRelation.table then reject("ScriptPublication1 canonical relation order differs")
      if oldRelation.table != allowedTable && oldRelation != newRelation then
        reject(s"ScriptPublication1 transition changed unrelated relation ${oldRelation.table}")
    }

  private def requireGenesisCatalog(snapshot: MySqlScriptSchemaSnapshot,
      catalogRow: Vector[MySqlFormat3Cell]): Unit =
    val manifest = onlyRow(snapshot, "fp_schema_manifest")
    val binding = onlyRow(snapshot, "physical_recovery_binding")
    val intent = onlyRow(snapshot, "binding_bootstrap_intent")
    val writer = onlyRow(snapshot, WriterTable)
    if snapshot.relation(RecoveryTable).rows.nonEmpty then reject("genesis requires no fp_recovery_operation rows")

    val bindingUuid = binary(snapshot, "fp_schema_manifest", manifest, "expected_binding_uuid")
    val provisioningUuid = binary(snapshot, "fp_schema_manifest", manifest, "provisioning_uuid")
    requireNonzeroUuid(snapshot, "fp_schema_manifest", manifest, "expected_binding_uuid")
    requireNonzeroUuid(snapshot, "fp_schema_manifest", manifest, "provisioning_uuid")
    requireInteger(snapshot, "fp_schema_manifest", manifest, "singleton_fixed1", 1L)
    requireInteger(snapshot, "fp_schema_manifest", manifest, "format_version", snapshot.inputs.schemaFormat.toLong)
    requireText(snapshot, "fp_schema_manifest", manifest, "bootstrap_state", "READY")
    requireText(snapshot, "fp_schema_manifest", manifest, "import_state", "NONE")
    requireBinaryUuid(snapshot, "fp_schema_manifest", manifest, "expected_server_uuid", snapshot.connection.serverUuid)
    requireBinaryLength(snapshot, "fp_schema_manifest", manifest, "configuration_sha256", 32)
    requireBinaryLength(snapshot, "fp_schema_manifest", manifest, "guard_manifest_sha256", 32)
    requireBinaryEquals(snapshot, "fp_schema_manifest", manifest, "ddl_sha256",
      HexFormat.of().parseHex(snapshot.inputs.ddlSha256))
    Vector("import_manifest_sha256", "import_operation_uuid", "import_snapshot_sha256", "import_receipt_sha256").foreach { name =>
      requireNull(snapshot, "fp_schema_manifest", manifest, name)
    }
    val profileBytes = snapshot.inputs.sourceLimitProfile.canonicalBytes()
    requireBinaryEquals(snapshot, "fp_schema_manifest", manifest, "limit_profile_bytes", profileBytes)
    requireBinaryEquals(snapshot, "fp_schema_manifest", manifest, "limit_profile_sha256",
      MessageDigest.getInstance("SHA-256").digest(profileBytes))
    requireText(snapshot, "fp_schema_manifest", manifest, "limit_profile_encoding", "FP_SOURCE_LIMIT_PROFILE_BE_V1")
    requireDigest(snapshot, "fp_schema_manifest", manifest, "source_provenance_bytes", "source_provenance_sha256")
    val provenanceKinds = if snapshot.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then
      Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT", "MANAGED_INSTALL")
    else Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT")
    requireTextOneOf(snapshot, "fp_schema_manifest", manifest, "source_provenance_kind", provenanceKinds)
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_region_bytes", snapshot.inputs.sourceLimitProfile.maxRegionBytes())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_chunk_bytes", snapshot.inputs.sourceLimitProfile.maxChunkBytes())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_decompressed_bytes", snapshot.inputs.sourceLimitProfile.maxDecompressedBytes())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_nbt_depth", snapshot.inputs.sourceLimitProfile.maxNbtDepth())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_members", snapshot.inputs.sourceLimitProfile.maxMembers())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_retained_bytes", snapshot.inputs.sourceLimitProfile.maxRetainedBytes())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_sql_packet_bytes", snapshot.inputs.sourceLimitProfile.maxSqlPacketBytes())
    requireInteger(snapshot, "fp_schema_manifest", manifest, "max_native_artifact_bytes", snapshot.inputs.sourceLimitProfile.maxNativeArtifactBytes())

    val physicalFormat = snapshot.inputs.dataLayout match
      case MySqlScriptSchemaDataLayout.DbFirstFull27 => MySqlDbFirstSchemaDefinition.PhysicalBindingFormat.toLong
      case _ => MySqlScriptSchemaProfile.PhysicalBindingFormat.toLong
    requireInteger(snapshot, "physical_recovery_binding", binding, "singleton_fixed1", 1L)
    requireInteger(snapshot, "physical_recovery_binding", binding, "format_version", physicalFormat)
    requireText(snapshot, "physical_recovery_binding", binding, "cutover_state", "LEGACY_UNCERTIFIED")
    requireBinaryEqual(snapshot, "physical_recovery_binding", binding, "binding_uuid", bindingUuid)

    requireInteger(snapshot, "binding_bootstrap_intent", intent, "singleton_fixed1", 1L)
    requireInteger(snapshot, "binding_bootstrap_intent", intent, "binding_format_version", physicalFormat)
    requireText(snapshot, "binding_bootstrap_intent", intent, "stage", "PREPARED")
    requireBinaryEqual(snapshot, "binding_bootstrap_intent", intent, "binding_uuid", bindingUuid)
    requireBinaryEqual(snapshot, "binding_bootstrap_intent", intent, "provisioning_uuid", provisioningUuid)
    requireNull(snapshot, "binding_bootstrap_intent", intent, "marker_ack_sha256")
    Vector("bootstrap_operation_uuid", "prepare_write_uuid", "certify_write_uuid").foreach { name =>
      requireNonzeroUuid(snapshot, "binding_bootstrap_intent", intent, name)
    }
    if binary(snapshot, "binding_bootstrap_intent", intent, "prepare_write_uuid") ==
      binary(snapshot, "binding_bootstrap_intent", intent, "certify_write_uuid") then
      reject("B1 PREPARED intent must keep distinct prepare and certify write identities")
    if text(snapshot, "binding_bootstrap_intent", intent, "source_world_identity").isEmpty then
      reject("B1 PREPARED intent requires its source world identity")
    requireBinaryLength(snapshot, "binding_bootstrap_intent", intent, "provision_receipt_sha256", 32)

    requireInteger(snapshot, WriterTable, writer, "singleton_fixed1", 1L)
    requireBinaryEqual(snapshot, WriterTable, writer, "binding_uuid", bindingUuid)
    requireInteger(snapshot, SnapshotTable, catalogRow, "catalog_revision", 0L)
    requireInteger(snapshot, SnapshotTable, catalogRow, "next_version", 1L)
    requireBinaryEqual(snapshot, SnapshotTable, catalogRow, "binding_uuid", bindingUuid)
    requireNull(snapshot, SnapshotTable, catalogRow, "last_mutation_operation_uuid")
    val empty = Array.emptyByteArray
    requireBinaryEquals(snapshot, SnapshotTable, catalogRow, "snapshot_bytes", empty)
    requireBinaryEquals(snapshot, SnapshotTable, catalogRow, "snapshot_sha256", MessageDigest.getInstance("SHA-256").digest(empty))

  private def requireZeroDeploymentQuota(snapshot: MySqlScriptSchemaSnapshot): Unit =
    val quota = onlyRow(snapshot, MySqlDeploymentReceiptSchemaDefinition.QuotaTable)
    val shape = snapshot.inputs.forTable(MySqlDeploymentReceiptSchemaDefinition.QuotaTable)
    val usedIndex = shape.columns.indexWhere(_.name == "used_records")
    if usedIndex < 0 then reject("deployment quota is missing used_records")
    quota(usedIndex) match
      case MySqlFormat3Cell.SignedInteger(0L) => ()
      case _ => reject("genesis deployment quota used_records must be zero")

  private def requireDbFirstQuotaConfiguration(snapshot: MySqlScriptSchemaSnapshot,
      expectedMaximumRecords: Long): Unit =
    val table = MySqlDeploymentReceiptSchemaDefinition.QuotaTable
    val quota = onlyRow(snapshot, table)
    requireBinaryEquals(snapshot, table, quota, "namespace",
      HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))
    requireInteger(snapshot, table, quota, "max_records", expectedMaximumRecords)
    requireInteger(snapshot, table, quota, "used_records", 0L)
    requireInteger(snapshot, table, quota, "quota_revision", 0L)

  private def requireInteger(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: Long): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.SignedInteger(value) if value == expected => ()
      case _ => reject(s"$table.$column differs from the required B1 value")

  private def requireText(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: String): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Text(value) if value == expected => ()
      case _ => reject(s"$table.$column differs from the required B1 value")

  private def requireTextOneOf(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, accepted: Set[String]): Unit =
    if !accepted.contains(text(snapshot, table, row, column)) then reject(s"$table.$column is outside the frozen B1 value set")

  private def requireNull(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Unit =
    if cell(snapshot, table, row, column) != MySqlFormat3Cell.SqlNull then reject(s"$table.$column must be SQL NULL for B1 genesis")

  private def requireBinaryUuid(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: java.util.UUID): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(expected) => ()
      case _ => reject(s"$table.$column differs from the observed B1 identity")

  private def requireNonzeroUuid(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) if value.length == 16 && !value.equalsBytes(new Array[Byte](16)) => ()
      case _ => reject(s"$table.$column must be a nonzero 16-byte B1 identity")

  private def requireBinaryEquals(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: Array[Byte]): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) if value.equalsBytes(expected) => ()
      case _ => reject(s"$table.$column differs from the required B1 bytes")

  private def requireBinaryEqual(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, expected: MySqlFormat3Binary): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) if value == expected => ()
      case _ => reject(s"$table.$column differs from the bound B1 identity")

  private def requireBinaryLength(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String, length: Int): Unit =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) if value.length == length => ()
      case _ => reject(s"$table.$column has an invalid B1 digest length")

  private def requireDigest(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], valueColumn: String, digestColumn: String): Unit =
    val value = binary(snapshot, table, row, valueColumn)
    val digest = binary(snapshot, table, row, digestColumn)
    if value.length == 0 || digest.length != 32 then reject(s"$table.$valueColumn B1 bytes or digest length is invalid")
    val expected = MessageDigest.getInstance("SHA-256")
    var index = 0
    while index < value.length do
      expected.update(value.byteAt(index))
      index += 1
    if !digest.equalsBytes(expected.digest()) then reject(s"$table.$valueColumn digest differs from B1 genesis bytes")

  private def onlyRow(snapshot: MySqlScriptSchemaSnapshot, table: String): Vector[MySqlFormat3Cell] =
    snapshot.relation(table).rows match
      case Vector(row) => row
      case _ => reject(s"B1 context requires exactly one $table row")

  private def index(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): Int =
    val found = snapshot.inputs.forTable(table).columns.indexWhere(_.name == column)
    if found < 0 then reject(s"$table.$column is outside the frozen ${MySqlScriptSchemaInputs.familyName(snapshot.inputs.dataLayout)} layout")
    found

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Cell =
    val columnIndex = index(snapshot, table, column)
    if row.size != snapshot.inputs.forTable(table).columns.size then
      reject(s"$table row width differs from ${MySqlScriptSchemaInputs.familyName(snapshot.inputs.dataLayout)} shape")
    row(columnIndex)

  private def text(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): String =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Text(value) => value
      case _ => reject(s"$table.$column is not typed text")

  private def binary(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Binary =
    cell(snapshot, table, row, column) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => reject(s"$table.$column is not typed binary")

  private def reject(message: String): Nothing = throw new SQLDataException(message)

  private def encode(kind: TransitionKind, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, originalBudget: MySqlFormat3ReadBudget): Data =
    val inputs = before.inputs
    require(originalBudget != null && !originalBudget.isClosed && !originalBudget.isScopedReadBudget &&
      (originalBudget.profile eq inputs.sourceLimitProfile), "B1 certificate requires its exact open closed-layout root")
    inputs.dataLayout match
      case MySqlScriptSchemaDataLayout.Full24 => originalBudget.requireScriptPublication1Inputs(inputs)
      case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => originalBudget.requireScriptDataInputs(inputs)
      case MySqlScriptSchemaDataLayout.DbFirstFull27 => originalBudget.requireScriptDataInputs(inputs)
    Vector(before, target).foreach { snapshot =>
      if (snapshot.inputs ne inputs) then reject("B1 before/target images must use the exact same closed-layout inputs")
      inputs.dataLayout match
        case MySqlScriptSchemaDataLayout.Full24 => snapshot.allocationBudget.requireScriptPublication1Inputs(inputs)
        case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 =>
          if !isRootOrCanonicalChild(snapshot.allocationBudget, originalBudget) then
            reject("Full26 B1 snapshots must belong to the exact original root or its canonical DATA child")
          snapshot.allocationBudget.requireScriptDataInputs(inputs)
        case MySqlScriptSchemaDataLayout.DbFirstFull27 =>
          if !isRootOrCanonicalChild(snapshot.allocationBudget, originalBudget) then
            reject("Full27 B1 snapshots must belong to the exact original root or its canonical DATA child")
          snapshot.allocationBudget.requireScriptDataInputs(inputs)
      if !snapshot.allocationBudget.sharesAllocationLedger(originalBudget) || snapshot.allocationBudget.isClosed then
        reject("B1 before/target images must remain charged to the original closed-layout ledger")
    }
    val maximum = Math.min(Math.min(inputs.budget.snapshotBytes, inputs.budget.retainedBytes), inputs.sourceLimitProfile.maxRetainedBytes())
    val beforeLength = before.canonicalByteLength
    val targetLength = target.canonicalByteLength
    if beforeLength > maximum || targetLength > maximum then reject("B1 snapshot DATA exceeds its explicit snapshot budget")
    inDataScope(originalBudget) { child =>
      child.reserveTemporaryMemory(512L, "ScriptPublication1 B1 canonical writer structures")
      var primary: Throwable = null
      try
        val counter = new MySqlFormat3CountingOutputStream
        val countOutput = new DataOutputStream(counter)
        writeTransition(kind, before, target, beforeLength, targetLength, child, countOutput)
        countOutput.flush()
        val length = counter.byteLength
        if length <= 0L || length > maximum then reject("B1 transition certificate exceeds explicit snapshot/retained budget")
        val size = MySqlFormat3JvmArrayLimit.requireLength(length, "ScriptPublication1 B1 exact DATA")
        child.reserveRetainedMemory(32L, "ScriptPublication1 B1 exact before/target snapshot references")
        child.reserveRetainedMemory(Math.addExact(length, 192L), "ScriptPublication1 B1 final DATA and holder")
        val sink = new ExactOutput(new Array[Byte](size))
        val output = new DataOutputStream(sink)
        writeTransition(kind, before, target, beforeLength, targetLength, child, output)
        output.flush()
        val content = sink.finish()
        child.reserveRetainedMemory(640L, "ScriptPublication1 B1 digest result")
        val digest = MessageDigest.getInstance("SHA-256").digest(content)
        val result = MySqlFormat3Binary.takeOwned(content)
        new Data(kind, inputs, before, target, originalBudget, child, result, digest.toVector,
          HexFormat.of().withUpperCase().formatHex(digest))
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => child.releaseTemporaryMemory(512L, "ScriptPublication1 B1 canonical writer structures")))
    }

  private def writeTransition(kind: TransitionKind, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, beforeLength: Long, targetLength: Long,
      budget: MySqlFormat3ReadBudget, output: DataOutputStream): Unit =
    val domain = before.inputs.dataLayout match
      case MySqlScriptSchemaDataLayout.Full24 => Domain
      case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => DeploymentReceiptFull26Domain
      case MySqlScriptSchemaDataLayout.DbFirstFull27 => DbFirstFull27Domain
    MySqlFormat3SnapshotCanonical.writeString(output, domain)
    output.writeByte(kind.ordinal)
    output.writeLong(beforeLength)
    MySqlScriptSchemaSnapshotCanonical.writeSnapshot(before, output, budget)
    output.writeLong(targetLength)
    MySqlScriptSchemaSnapshotCanonical.writeSnapshot(target, output, budget)

  private def inDataScope[A](root: MySqlFormat3ReadBudget)(body: MySqlFormat3ReadBudget => A): A =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val child = root.newCanonicalDataScope()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(child) { body(child) }
      catch
        case failure: Throwable =>
          try child.closeScopedReadBudget()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private final class ExactOutput(content: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= content.length then throw new SQLDataException("ScriptPublication1 B1 DATA output overrun")
      content(offset) = value.toByte
      offset += 1
    override def write(bytes: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, bytes.length)
      if length > content.length - offset then throw new SQLDataException("ScriptPublication1 B1 DATA output overrun")
      System.arraycopy(bytes, start, content, offset, length)
      offset += length
    def finish(): Array[Byte] =
      if offset != content.length then throw new SQLDataException("ScriptPublication1 B1 DATA output underrun")
      content
