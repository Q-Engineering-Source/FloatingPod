package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.nio.ByteBuffer
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import java.util.{HexFormat, UUID}

private[mysql] enum MySqlScriptSchemaDataLayout:
  case Full24, DeploymentReceiptFull26, DbFirstFull27

/** 固定闭合的 ScriptPublication1 DATA 输入；调用方不能提供 shape、SQL 或 canonical domain。 */
private[mysql] final class MySqlScriptSchemaInputs private (
    val sourceLimitProfile: SourceLimitProfile,
    val budget: MySqlScriptSchemaProfile.Budget,
    val schema: String,
    val ddlSha256: String,
    private[mysql] val dataLayout: MySqlScriptSchemaDataLayout,
    private[mysql] val deploymentBudget: Option[MySqlDeploymentReceiptSchemaDefinition.Budget]
):
  private[mysql] def schemaFormat: Int = dataLayout match
    case MySqlScriptSchemaDataLayout.Full24 => MySqlScriptSchemaProfile.SchemaFormat
    case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => MySqlDeploymentReceiptSchemaDefinition.SchemaFormat
    case MySqlScriptSchemaDataLayout.DbFirstFull27 => MySqlDbFirstSchemaDefinition.SchemaFormat

  private[mysql] def snapshotDomain: String = dataLayout match
    case MySqlScriptSchemaDataLayout.Full24 => MySqlScriptSchemaProfile.SnapshotDomain
    case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => MySqlDeploymentReceiptSchemaDefinition.SnapshotDomain
    case MySqlScriptSchemaDataLayout.DbFirstFull27 => MySqlDbFirstSchemaDefinition.SnapshotDomain

  private[mysql] def canonicalOrder: Vector[MySqlFormat3RelationShape] =
    MySqlScriptSchemaInputs.canonicalOrderFor(dataLayout)

  private[mysql] def forTable(table: String): MySqlFormat3RelationShape =
    MySqlScriptSchemaInputs.forTable(dataLayout, table)

  private[mysql] def requireSameInputs(other: MySqlScriptSchemaInputs): Unit =
    if other ne this then
      val family = MySqlScriptSchemaInputs.familyName(dataLayout)
      throw new SQLDataException(s"$family budget/input identity differs")

  private[mysql] def newAllocationRoot(packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    dataLayout match
      case MySqlScriptSchemaDataLayout.Full24 => MySqlFormat3ReadBudget.scriptPublication1Root(this, packetLimit, ledger)
      case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 =>
        MySqlFormat3ReadBudget.scriptPublication1DeploymentReceiptRoot(this, packetLimit, ledger)
      case MySqlScriptSchemaDataLayout.DbFirstFull27 =>
        MySqlFormat3ReadBudget.dbFirstRoot(this, packetLimit, ledger)

  private[mysql] def effectivePacketLimit(observedPacket: Long): Long =
    if observedPacket <= 0L then throw new SQLDataException(s"observed ${MySqlScriptSchemaInputs.familyName(dataLayout)} packet limit must be positive")
    Math.min(Math.min(sourceLimitProfile.maxSqlPacketBytes(), budget.packetBytes), observedPacket)

  private[mysql] def maximumPayloadBytes(table: String, column: MySqlFormat3ColumnShape): Option[Long] =
    column.sqlDeclaration match
      case "MEDIUMBLOB NOT NULL" =>
        (table, column.name) match
          case ("fp_script_candidate", "source_bytes") =>
            Some(MySqlScriptSchemaProfile.SourceLimit)
          case ("fp_script_version", "compilation_bytes") =>
            Some(MySqlScriptSchemaProfile.CompilationLimit)
          case _ => throw new SQLDataException(s"unrecognized ${MySqlScriptSchemaInputs.familyName(dataLayout)} MEDIUMBLOB ${table}.${column.name}")
      case declaration if declaration.startsWith("LONGBLOB ") || declaration.startsWith("LONGTEXT ") =>
        Some(largePayloadLimit(table, column.name))
      case _ => None

  /** 在 SQL 行成为 closed-layout image 或 canonical DATA 前验证全部行。 */
  private[mysql] def validateRelations(
      relations: Vector[MySqlFormat3RelationSnapshot],
      observedPacket: Long,
      allocationBudget: MySqlFormat3ReadBudget
  ): Long =
    validateRelationsOnBudget(relations, observedPacket, allocationBudget, None)

  /** comparison 仅返回测量值；逐行解码不取得 retained DATA 或 immutable borrow custody。 */
  private[mysql] def validateRelationsForComparison(
      relations: Vector[MySqlFormat3RelationSnapshot], observedPacket: Long,
      originalRoot: MySqlFormat3ReadBudget, comparisonBudget: MySqlFormat3ReadBudget
  ): Long =
    require(originalRoot != null && !originalRoot.isClosed && !originalRoot.isScopedReadBudget &&
      comparisonBudget != null && !comparisonBudget.isClosed &&
      (dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 || dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27) &&
      (originalRoot.profile eq sourceLimitProfile) && (comparisonBudget.profile eq originalRoot.profile) &&
      comparisonBudget.isCanonicalComparisonChildOf(originalRoot) &&
      comparisonBudget.sharesAllocationLedger(originalRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq comparisonBudget),
      "Full26 relation comparison requires the original root and its exact active comparison child")
    originalRoot.requireScriptDataInputs(this)
    validateRelationsOnBudget(relations, observedPacket, comparisonBudget, Some(originalRoot))

  private def validateRelationsOnBudget(
      relations: Vector[MySqlFormat3RelationSnapshot], observedPacket: Long,
      allocationBudget: MySqlFormat3ReadBudget, comparisonRoot: Option[MySqlFormat3ReadBudget]
  ): Long =
    if allocationBudget == null then throw new SQLDataException("ScriptPublication1 validation requires its exact allocation root")
    allocationBudget.requireScriptDataInputs(this)
    allocationBudget.reserveTemporaryMemory(MySqlScriptSchemaInputs.ValidationWorkspace,
      s"${MySqlScriptSchemaInputs.familyName(dataLayout)} complete relation/row validation workspace")
    var primary: Throwable = null
    try
      dataLayout match
        case MySqlScriptSchemaDataLayout.Full24 =>
          MySqlFormat3SnapshotValidation.validateScriptPublication1Relations(relations)
        case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 =>
          MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(relations)
        case MySqlScriptSchemaDataLayout.DbFirstFull27 =>
          MySqlFormat3SnapshotValidation.validateDbFirstRelations(relations)
      val rowCount = relations.foldLeft(0L) { (count, relation) =>
        Math.addExact(count, relation.rows.size.toLong)
      }
      if rowCount > budget.rows then throw new SQLDataException(s"${MySqlScriptSchemaInputs.familyName(dataLayout)} total row budget exceeded")

      val packetLimit = effectivePacketLimit(observedPacket)
      var retainedValueBytes = 0L
      relations.foreach { relation =>
        val shape = forTable(relation.table)
        var rowIndex = 0
        while rowIndex < relation.rows.size do
          val row = relation.rows(rowIndex)
          var packetBytes = Math.addExact(9L, shape.columns.size.toLong)
          var columnIndex = 0
          while columnIndex < shape.columns.size do
            val cell = row(columnIndex)
            val column = shape.columns(columnIndex)
            cell match
              case MySqlFormat3Cell.SqlNull => ()
              case MySqlFormat3Cell.Text(value) =>
                val length = MySqlFormat3Utf8.byteLength(value)
                maximumPayloadBytes(relation.table, column).foreach { maximum =>
                  if length > maximum then throw new SQLDataException(s"${relation.table}.${column.name} exceeds explicit text byte budget: actual=$length, maximum=$maximum")
                }
                retainedValueBytes = Math.addExact(retainedValueBytes, length)
                packetBytes = Math.addExact(packetBytes, Math.addExact(9L, length))
              case MySqlFormat3Cell.Binary(value) =>
                val length = value.length.toLong
                maximumPayloadBytes(relation.table, column).foreach { maximum =>
                  if length > maximum then throw new SQLDataException(s"${relation.table}.${column.name} exceeds explicit binary byte budget: actual=$length, maximum=$maximum")
                }
                retainedValueBytes = Math.addExact(retainedValueBytes, length)
                packetBytes = Math.addExact(packetBytes, Math.addExact(9L, length))
              case MySqlFormat3Cell.SignedInteger(_) =>
                packetBytes = Math.addExact(packetBytes, integerPacketWidth(column))
              case MySqlFormat3Cell.Floating(_) => packetBytes = Math.addExact(packetBytes, 64L)
              case MySqlFormat3Cell.DateTime(_) => packetBytes = Math.addExact(packetBytes, 32L)
            columnIndex += 1
          if packetBytes > packetLimit then
            throw new SQLDataException(s"${relation.table} row $rowIndex exceeds explicit ${MySqlScriptSchemaInputs.familyName(dataLayout)} packet budget")
          rowIndex += 1
      }

      val measuredRetainedValueBytes = MySqlScriptSchemaProfile.retainedSize(Vector(retainedValueBytes), rowCount, budget) match
        case Right(total) => total
        case Left(reason) => throw new SQLDataException(s"${MySqlScriptSchemaInputs.familyName(dataLayout)} retained DATA budget rejected: $reason")
      if dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 || dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then
        validateDeploymentReceiptRows(relations, allocationBudget, comparisonRoot)
      if dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then
        MySqlDbFirstSnapshotRows.validate(relations, this, allocationBudget)
      measuredRetainedValueBytes
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => allocationBudget.releaseTemporaryMemory(MySqlScriptSchemaInputs.ValidationWorkspace,
          s"${MySqlScriptSchemaInputs.familyName(dataLayout)} complete relation/row validation workspace")))

  private def validateDeploymentReceiptRows(
      relations: Vector[MySqlFormat3RelationSnapshot],
      allocationBudget: MySqlFormat3ReadBudget,
      comparisonRoot: Option[MySqlFormat3ReadBudget]
  ): Unit =
    val operationShape = MySqlDeploymentReceiptRelationShapes.operation
    val quotaShape = MySqlDeploymentReceiptRelationShapes.quota
    if operationShape.columns.map(_.name) != MySqlDeploymentOperationRows.columnNames then
      throw new SQLDataException("deployment receipt operation columns differ from the actual 31-column row decoder")
    val operation = relations(MySqlDeploymentReceiptRelationShapes.canonicalOrder.indexWhere(_.table == operationShape.table))
    val quota = relations(MySqlDeploymentReceiptRelationShapes.canonicalOrder.indexWhere(_.table == quotaShape.table))
    if quota.rows.size != 1 then throw new SQLDataException("deployment receipt quota must contain exactly one FPDC row")

    val quotaRow = quota.rows.head
    def columnIndex(shape: MySqlFormat3RelationShape, name: String): Int =
      val result = shape.columns.indexWhere(_.name == name)
      if result < 0 then throw new SQLDataException(s"${shape.table} is missing frozen column $name")
      result
    def integer(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape, name: String): Long =
      row(columnIndex(shape, name)) match
        case MySqlFormat3Cell.SignedInteger(value) => value
        case _ => throw new SQLDataException(s"${shape.table}.$name must be an integer")

    quotaRow(columnIndex(quotaShape, "namespace")) match
      case MySqlFormat3Cell.Binary(value) if value.equalsBytes(MySqlScriptSchemaInputs.DeploymentNamespaceBytes) => ()
      case _ => throw new SQLDataException("deployment receipt quota namespace must be FPDC")
    val maximum = integer(quotaRow, quotaShape, "max_records")
    val used = integer(quotaRow, quotaShape, "used_records")
    val revision = integer(quotaRow, quotaShape, "quota_revision")
    if maximum <= 0L then throw new SQLDataException("deployment receipt quota max_records must be positive")
    if used < 0L || used > maximum then throw new SQLDataException("deployment receipt quota used_records is outside its maximum")
    if revision < 0L then throw new SQLDataException("deployment receipt quota quota_revision must be nonnegative")
    if used != operation.rows.size.toLong then
      throw new SQLDataException("deployment receipt quota used_records differs from the complete operation row count")

    var rowIndex = 0
    while rowIndex < operation.rows.size do
      comparisonRoot match
        case Some(root) => validateDeploymentOperationRowForComparison(operation.rows(rowIndex), rowIndex, operationShape, root)
        case None => validateDeploymentOperationRow(operation.rows(rowIndex), rowIndex, operationShape, allocationBudget)
      rowIndex += 1
    MySqlRecoveryOperationHistoryValidation.validate(relations, this, allocationBudget)

  private def validateDeploymentOperationRow(
      row: Vector[MySqlFormat3Cell],
      rowIndex: Int,
      shape: MySqlFormat3RelationShape,
      allocationBudget: MySqlFormat3ReadBudget
  ): Unit =
    val child = allocationBudget.newCanonicalDataScope()
    var primary: Throwable = null
    // The inner frame owns the copied ColumnValues and decoded Row; it returns before its child ledger closes.
    try validateDeploymentOperationRowInScope(row, rowIndex, shape, child)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => child.closeScopedReadBudget()))

  /** 真 root 建立短命 comparison owner；不切换 lexical owner，也不将 row 或 custody 返回调用方。 */
  private def validateDeploymentOperationRowForComparison(
      row: Vector[MySqlFormat3Cell], rowIndex: Int, shape: MySqlFormat3RelationShape,
      originalRoot: MySqlFormat3ReadBudget
  ): Unit =
    originalRoot.reserveTemporaryMemory(256L, "deployment row comparison scope metadata")
    var child: MySqlFormat3ReadBudget = null
    var primary: Throwable = null
    try
      child = originalRoot.newCanonicalScope()
      require(child.isCanonicalComparisonChildOf(originalRoot) && child.sharesAllocationLedger(originalRoot),
        "deployment row comparison owner must share its original root and ledger")
      child.requireScriptDataInputs(this)
      validateDeploymentOperationRowInScope(row, rowIndex, shape, child)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => if child != null then child.closeScopedReadBudget(),
        () => originalRoot.releaseTemporaryMemory(256L, "deployment row comparison scope metadata release")))

  private def validateDeploymentOperationRowInScope(
      row: Vector[MySqlFormat3Cell],
      rowIndex: Int,
      shape: MySqlFormat3RelationShape,
      budget: MySqlFormat3ReadBudget
  ): Unit =
    budget.reserveTemporaryMemory(MySqlScriptSchemaInputs.DeploymentRowConversionWorkspace,
      s"${shape.table} row $rowIndex ColumnValues conversion workspace")
    var primary: Throwable = null
    try
      decodeDeploymentOperationRow(row, rowIndex, shape, budget)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(MySqlScriptSchemaInputs.DeploymentRowConversionWorkspace,
          s"${shape.table} row $rowIndex ColumnValues conversion workspace")))

  /** Values and the returned Row die with this frame before their temporary child or workspace is released. */
  private def decodeDeploymentOperationRow(
      row: Vector[MySqlFormat3Cell],
      rowIndex: Int,
      shape: MySqlFormat3RelationShape,
      budget: MySqlFormat3ReadBudget
  ): Unit =
    val values = row.zip(shape.columns).map { (cell, column) =>
      cell match
        case MySqlFormat3Cell.SqlNull => MySqlDeploymentOperationRows.ColumnValue.SqlNull
        case MySqlFormat3Cell.SignedInteger(value) => MySqlDeploymentOperationRows.ColumnValue.Integer(value)
        case MySqlFormat3Cell.Binary(value) =>
          MySqlDeploymentOperationRows.ColumnValue.Binary(value.copyBytes(budget,
            s"${shape.table} row $rowIndex ${column.name} decoder input copy"))
        case _ => throw new SQLDataException(s"${shape.table} row $rowIndex ${column.name} cannot be a deployment operation value")
    }
    def binary(column: String): Array[Byte] =
      val index = MySqlDeploymentOperationRows.columnNames.indexOf(column)
      values(index) match
        case MySqlDeploymentOperationRows.ColumnValue.Binary(bytes) => bytes
        case _ => throw new SQLDataException(s"${shape.table} row $rowIndex $column must be binary")
    def uuid(column: String): UUID =
      val buffer = ByteBuffer.wrap(binary(column))
      new UUID(buffer.getLong(), buffer.getLong())
    val actor = uuid("actor_uuid")
    val operation = uuid("operation_uuid")
    try
      MySqlDeploymentOperationRows.decode(MySqlDeploymentOperationRows.ColumnValues(values), actor, operation, budget)
      ()
    catch
      case failure: MySqlDeploymentOperationRows.DecodeFailure =>
        throw new SQLDataException(s"${shape.table} row $rowIndex failed ${failure.column} decoding", failure)

  private def largePayloadLimit(table: String, column: String): Long =
    (table, column) match
      case (MySqlDbFirstSchemaDefinition.SnapshotTable, "nbt_bytes") =>
        MySqlDbFirstSchemaDefinition.maximumNbtBytes(sourceLimitProfile)
      case (MySqlDbFirstSchemaDefinition.SnapshotTable, "limit_profile_bytes") =>
        sourceLimitProfile.canonicalBytes().length.toLong
      case (MySqlDeploymentReceiptSchemaDefinition.OperationTable, "enqueue_evidence_bytes") =>
        deploymentBudget.map(_.evidenceBytes).getOrElse(
          throw new SQLDataException(s"deployment receipt DATA has no explicit evidence budget for $table.$column"))
      case (MySqlDeploymentReceiptSchemaDefinition.OperationTable, "step_evidence_bytes") =>
        deploymentBudget.map(_.evidenceBytes).getOrElse(
          throw new SQLDataException(s"deployment receipt DATA has no explicit evidence budget for $table.$column"))
      case (MySqlDeploymentReceiptSchemaDefinition.OperationTable, "diagnostic_bytes") =>
        deploymentBudget.map(_.diagnosticsBytes).getOrElse(
          throw new SQLDataException(s"deployment receipt DATA has no explicit diagnostics budget for $table.$column"))
      case ("fp_schema_manifest", "source_provenance_bytes") => budget.evidenceBytes
      case ("fp_schema_manifest", "limit_profile_bytes") => budget.configBytes
      case ("fp_recovery_operation", "request_identity_bytes") => budget.requestBytes
      case ("fp_recovery_operation", "preimage_bytes") => budget.snapshotBytes
      case ("fp_recovery_operation", "target_bytes") => budget.snapshotBytes
      case ("fp_recovery_operation", "commit_witness_bytes") => budget.snapshotBytes
      case ("fp_recovery_operation", "original_diagnostic_identity") => budget.diagnosticsBytes
      case ("fp_recovery_operation", "original_context_bytes") => budget.evidenceBytes
      case ("fp_recovery_operation", "scope_bytes") => budget.evidenceBytes
      case ("fp_recovery_reservation", "resource_key") => budget.evidenceBytes
      case ("physical_recovery_intent", "fresh_absence_proof_bytes") => budget.evidenceBytes
      case ("physical_recovery_intent", "cleanup_proof_bytes") => budget.evidenceBytes
      case ("physical_recovery_obligation", "verification_proof_bytes") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "removal_result_bytes") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "ticket_result_bytes") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "checkpoint_bytes") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "original_context_bytes") => budget.evidenceBytes
      case ("physical_footprint_transfer", "move_plan_bytes") => budget.evidenceBytes
      case ("physical_footprint_transfer", "cleanup_proof_bytes") => budget.evidenceBytes
      case ("physical_source_unit", "source_observation_bytes") => budget.evidenceBytes
      case ("physical_source_forest", "forest_bytes") => budget.evidenceBytes
      case ("physical_source_forest", "fp_pod_uuid_index_bytes") => budget.evidenceBytes
      case ("physical_source_transfer", "transfer_plan_bytes") => budget.evidenceBytes
      case ("physical_source_transfer", "target_receipts_bytes") => budget.evidenceBytes
      case ("physical_source_transfer", "retirement_proof_bytes") => budget.evidenceBytes
      case ("physical_source_write", "carrier_key") => budget.evidenceBytes
      case ("physical_source_write", "payload_descriptor_bytes") => budget.evidenceBytes
      case ("physical_source_write", "undo_descriptor_bytes") => budget.evidenceBytes
      case ("physical_source_write", "member_coverage_bytes") => budget.evidenceBytes
      case ("physical_source_write", "obligation_revisions_bytes") => budget.evidenceBytes
      case ("physical_source_write", "durable_receipt_bytes") => budget.receiptBytes
      case ("physical_source_write", "failure_diagnostic_identity") => budget.diagnosticsBytes
      case ("physical_source_obligation", "carrier_key") => budget.evidenceBytes
      case ("physical_source_obligation", "discharge_proof_bytes") => budget.evidenceBytes
      case ("fp_script_catalog", "snapshot_bytes") => budget.snapshotBytes
      case ("fp_script_operation", "request_bytes") => budget.requestBytes
      case ("fp_script_operation", "before_bytes") => budget.snapshotBytes
      case ("fp_script_operation", "after_bytes") => budget.snapshotBytes
      case ("fp_script_operation", "receipt_bytes") => budget.receiptBytes
      case ("fp_script_candidate", "check_evidence_bytes") => budget.evidenceBytes
      case ("fp_script_candidate", "diagnostics_bytes") => budget.diagnosticsBytes
      case ("fp_script_version", "layout_config_bytes") => budget.configBytes
      case ("fp_schema_manifest", "world_name") => budget.evidenceBytes
      case ("floating_pod_state", "world_name") => budget.evidenceBytes
      case ("physical_recovery_binding", "source_world_identity") => budget.evidenceBytes
      case ("binding_bootstrap_intent", "source_world_identity") => budget.evidenceBytes
      case ("physical_recovery_intent", "target_world_name") => budget.evidenceBytes
      case ("physical_recovery_intent", "source_identity") => budget.evidenceBytes
      case ("physical_recovery_intent", "committed_world_name") => budget.evidenceBytes
      case ("physical_recovery_footprint", "target_world_name") => budget.evidenceBytes
      case ("physical_recovery_locator", "reason_code") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "world_identity") => budget.evidenceBytes
      case ("fp_lifecycle_completion", "source_identity") => budget.evidenceBytes
      case ("physical_footprint_transfer", "from_world_name") => budget.evidenceBytes
      case ("physical_footprint_transfer", "to_world_name") => budget.evidenceBytes
      case ("physical_source_unit", "world_identity") => budget.evidenceBytes
      case ("physical_source_unit", "save_identity") => budget.evidenceBytes
      case ("physical_source_unit", "loader_identity") => budget.evidenceBytes
      case ("physical_source_write", "save_identity") => budget.evidenceBytes
      case ("physical_source_write", "loader_identity") => budget.evidenceBytes
      case ("physical_source_write", "region_identity") => budget.evidenceBytes
      case ("physical_source_write", "payload_artifact_identity") => budget.evidenceBytes
      case ("physical_source_write", "undo_artifact_identity") => budget.evidenceBytes
      case _ => throw new SQLDataException(s"unbudgeted ScriptPublication1 large-value column $table.$column")

  private def integerPacketWidth(column: MySqlFormat3ColumnShape): Long =
    column.integerPacketWidth.toLong

private[mysql] object MySqlScriptSchemaInputs:
  val ExpectedTableCount = 24
  val ExpectedColumnCount = 432
  val ScriptTables = Set("fp_script_catalog", "fp_script_operation", "fp_script_candidate", "fp_script_version")
  private[mysql] val ValidationWorkspace = 32_768L
  private[mysql] val DeploymentRowConversionWorkspace = 2_048L
  private val DeploymentNamespaceBytes = HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex)

  private[mysql] def familyName(layout: MySqlScriptSchemaDataLayout): String = layout match
    case MySqlScriptSchemaDataLayout.Full24 => "ScriptPublication1"
    case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => "ScriptPublication1DeploymentReceipt1"
    case MySqlScriptSchemaDataLayout.DbFirstFull27 => MySqlDbFirstSchemaDefinition.Profile

  private def parseColumn(table: String, column: MySqlScriptSchemaProfile.Column): MySqlFormat3ColumnShape =
    val declaration = column.declaration
    val (kind, nullable, maximumLength) = declaration match
      case "TINYINT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "TINYINT NULL" => (MySqlFormat3SqlKind.Integer, true, None)
      case "INT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "INT NULL" => (MySqlFormat3SqlKind.Integer, true, None)
      case "BIGINT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "BIGINT NULL" => (MySqlFormat3SqlKind.Integer, true, None)
      case "DOUBLE NOT NULL" => (MySqlFormat3SqlKind.Floating, false, None)
      case "DOUBLE NULL" => (MySqlFormat3SqlKind.Floating, true, None)
      case "DATETIME(6) NOT NULL" => (MySqlFormat3SqlKind.DateTime, false, Some(6))
      case "DATETIME(6) NULL" => (MySqlFormat3SqlKind.DateTime, true, Some(6))
      case "VARBINARY(16) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(16))
      case "VARBINARY(16) NULL" => (MySqlFormat3SqlKind.Binary, true, Some(16))
      case "VARBINARY(32) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(32))
      case "VARBINARY(32) NULL" => (MySqlFormat3SqlKind.Binary, true, Some(32))
      case "VARBINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CAST(source_identity AS BINARY),256))) STORED" =>
        if table != "physical_recovery_intent" || column.name != "source_identity_sha256" then
          throw new SQLDataException(s"unexpected generated ScriptPublication1 column $table.${column.name}")
        (MySqlFormat3SqlKind.Binary, true, Some(32))
      case "MEDIUMBLOB NOT NULL" =>
        val maximum = (table, column.name) match
          case ("fp_script_candidate", "source_bytes") => MySqlScriptSchemaProfile.SourceLimit.toInt
          case ("fp_script_version", "compilation_bytes") => MySqlScriptSchemaProfile.CompilationLimit.toInt
          case _ => throw new SQLDataException(s"unrecognized ScriptPublication1 MEDIUMBLOB $table.${column.name}")
        (MySqlFormat3SqlKind.Binary, false, Some(maximum))
      case "LONGBLOB NOT NULL" => (MySqlFormat3SqlKind.Binary, false, None)
      case "LONGBLOB NULL" => (MySqlFormat3SqlKind.Binary, true, None)
      case "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL" => (MySqlFormat3SqlKind.Text, false, None)
      case "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL" => (MySqlFormat3SqlKind.Text, true, None)
      case text if text.matches("VARCHAR\\((8|16|24|32|40|48|64)\\) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin (NOT NULL|NULL)") =>
        val length = "VARCHAR\\(([0-9]+)\\).*".r.findFirstMatchIn(text).get.group(1).toInt
        (MySqlFormat3SqlKind.Text, !text.endsWith(" NOT NULL"), Some(length))
      case _ => throw new SQLDataException(s"unsupported ScriptPublication1 frozen SQL declaration for $table.${column.name}: $declaration")
    MySqlFormat3ColumnShape(column.name, declaration, kind, nullable, maximumLength)

  private lazy val shapes: Vector[MySqlFormat3RelationShape] =
    val profile = MySqlScriptSchemaProfile.tables
    if profile.size != ExpectedTableCount || profile.map(_.columns.size).sum != ExpectedColumnCount ||
      MySqlScriptSchemaProfile.guards.size != 72 then
      throw new IllegalStateException("accepted ScriptPublication1 profile no longer has its exact 24-table/432-column/72-guard shape")
    val lockByTable = MySqlFormat3RelationShapes.lockedReadOrder.iterator.map(shape => shape.table -> shape.lockClause).toMap
    profile.map { table =>
      val lockClause = lockByTable.getOrElse(table.name, "FOR SHARE")
      MySqlFormat3RelationShape(table.name, table.columns.map(parseColumn(table.name, _)), table.primaryKey, lockClause)
    }

  val canonicalOrder: Vector[MySqlFormat3RelationShape] = shapes
  val lockedReadOrder: Vector[MySqlFormat3RelationShape] =
    val byTable = shapes.iterator.map(shape => shape.table -> shape).toMap
    val old = MySqlFormat3RelationShapes.lockedReadOrder.map(shape => byTable(shape.table))
    old ++ shapes.filter(shape => ScriptTables.contains(shape.table))

  def forTable(table: String): MySqlFormat3RelationShape =
    forTable(MySqlScriptSchemaDataLayout.Full24, table)

  private[mysql] def canonicalOrderFor(layout: MySqlScriptSchemaDataLayout): Vector[MySqlFormat3RelationShape] =
    if layout == null then throw new IllegalArgumentException("closed ScriptPublication1 DATA layout is required")
    layout match
      case MySqlScriptSchemaDataLayout.Full24 => canonicalOrder
      case MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 => MySqlDeploymentReceiptRelationShapes.canonicalOrder
      case MySqlScriptSchemaDataLayout.DbFirstFull27 => MySqlDbFirstRelationShapes.canonicalOrder

  private[mysql] def forTable(layout: MySqlScriptSchemaDataLayout, table: String): MySqlFormat3RelationShape =
    if table == null then throw new IllegalArgumentException("ScriptPublication1 table name is required")
    canonicalOrderFor(layout).find(_.table == table).getOrElse(throw new IllegalArgumentException(
      s"table is outside the frozen ${familyName(layout)} ${canonicalOrderFor(layout).size}-relation layout"))

  def create(sourceLimitProfile: SourceLimitProfile, budget: MySqlScriptSchemaProfile.Budget): MySqlScriptSchemaInputs =
    if sourceLimitProfile == null || budget == null then throw new IllegalArgumentException("explicit ScriptPublication1 inputs and budgets are required")
    if budget.retainedBytes > sourceLimitProfile.maxRetainedBytes() then
      throw new IllegalArgumentException("ScriptPublication1 retained DATA budget exceeds the original allocation profile")
    if budget.packetBytes > sourceLimitProfile.maxSqlPacketBytes() then
      throw new IllegalArgumentException("ScriptPublication1 row-packet budget exceeds the original SQL packet profile")
    // 立即执行封闭转换，使新增或未知声明在输入边界失败。
    if canonicalOrder.size != ExpectedTableCount || canonicalOrder.flatMap(_.columns).size != ExpectedColumnCount then
      throw new SQLDataException("ScriptPublication1 typed profile shape is incomplete")
    new MySqlScriptSchemaInputs(sourceLimitProfile, budget, MySqlScriptSchemaProfile.Schema,
      MySqlScriptSchemaProfile.SqlSha256, MySqlScriptSchemaDataLayout.Full24, None)

  /** DATA-only schema5 inputs; this pins caller-supplied identity and budgets without issuing SQL or readiness. */
  def createDeploymentReceiptFull26(schema: String, ddlSha256: String,
      sourceLimitProfile: SourceLimitProfile, publicationBudget: MySqlScriptSchemaProfile.Budget,
      deploymentBudget: MySqlDeploymentReceiptSchemaDefinition.Budget): MySqlScriptSchemaInputs =
    if schema == null || !schema.matches("[a-z][a-z0-9_]{0,63}") then
      throw new IllegalArgumentException("full26 DATA requires an explicit canonical safe schema")
    if ddlSha256 == null || !MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256) then
      throw new IllegalArgumentException("full26 DATA requires an explicit lowercase SHA-256 DDL identity")
    if sourceLimitProfile == null || publicationBudget == null || deploymentBudget == null then
      throw new IllegalArgumentException("full26 DATA requires explicit source, publication and deployment budgets")
    if publicationBudget.retainedBytes > sourceLimitProfile.maxRetainedBytes() then
      throw new IllegalArgumentException("full26 retained DATA budget exceeds the original allocation profile")
    if publicationBudget.packetBytes > sourceLimitProfile.maxSqlPacketBytes() then
      throw new IllegalArgumentException("full26 row-packet budget exceeds the original SQL packet profile")
    val shapes = canonicalOrderFor(MySqlScriptSchemaDataLayout.DeploymentReceiptFull26)
    if shapes.size != MySqlDeploymentReceiptRelationShapes.ExpectedRelationCount ||
      shapes.flatMap(_.columns).size != MySqlDeploymentReceiptRelationShapes.ExpectedColumnCount then
      throw new SQLDataException("deployment receipt DATA requires its exact26/467 closed layout")
    new MySqlScriptSchemaInputs(sourceLimitProfile, publicationBudget, schema, ddlSha256,
      MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, Some(deploymentBudget))

  /** 独立 Full27 DATA 输入；实际 renderer 身份必须与显式参数一致。 */
  def createDbFirstFull27(rendered: MySqlDbFirstSchemaProfile.Rendered): MySqlScriptSchemaInputs =
    require(rendered != null, "schema6 rendered inputs required")
    val p = rendered.inputs
    require(p.schema.matches("[a-z][a-z0-9_]{0,63}"), "Full27 requires a canonical safe schema")
    new MySqlScriptSchemaInputs(p.sourceLimitProfile, p.publicationBudget, p.schema, rendered.sha256,
      MySqlScriptSchemaDataLayout.DbFirstFull27, Some(p.deploymentBudget))

/** 独立 control 的验证输入，不是审计证书、来源 issuer 或 Ready。 */
final class MySqlScriptSchemaAuditInputs(
    val serverUuid: UUID, val serverVersion: String, val schema: String,
    val effectiveUser: String, val connectedUser: String, val callerRoles: String,
    val provisioningUuid: UUID, val bindingUuid: UUID, val ddlSha256: String,
    val auditDefiner: String, val callerGrants: Vector[String],
    val definerPrivileges: Vector[MySqlAuditResultSet],
    val configurationSha256: MySqlAuditBinary, val sourceProvenanceKind: String,
    val sourceProvenance: MySqlAuditBinary, val sourceProvenanceSha256: MySqlAuditBinary,
    val limitProfile: SourceLimitProfile
):
  require(serverUuid != null && provisioningUuid != null && bindingUuid != null, "ScriptPublication1 independent identities are required")
  require(serverVersion != null && schema != null && effectiveUser != null && connectedUser != null && callerRoles != null,
    "ScriptPublication1 independent caller identity is required")
  require(ddlSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256), "ScriptPublication1 independent DDL pin is required")
  require(auditDefiner != null && callerGrants != null && callerGrants.nonEmpty && callerGrants.forall(g => g != null && g.nonEmpty),
    "independent complete caller grants are required; no empty default")
  require(callerGrants.distinct.size == callerGrants.size, "independent caller grants contain duplicates")
  require(definerPrivileges != null && definerPrivileges.map(_.id) == MySqlAuditCoverage.Format3PrivilegeResultSetIds,
    "independent four definer privilege sets are required")
  definerPrivileges.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).foreach { (set, labels) =>
    require(set.columns != null && set.columns.map(_.label) == labels && set.rows != null, "independent definer privilege shape differs")
    require(set.rows.forall(r => r != null && r.cells != null && r.cells.size == labels.size && r.cells.forall(_.isInstanceOf[MySqlAuditText])),
      "independent privileges require full exact text rows")
  }
  MySqlSchemaAuditor.rejectDuplicateRows(definerPrivileges)
  private val at = auditDefiner.lastIndexOf('@')
  require(at > 0 && at < auditDefiner.length - 1, "independent audit definer account is incomplete")
  private val grantee = "'" + auditDefiner.substring(0, at).replace("'", "''") + "'@'" + auditDefiner.substring(at + 1).replace("'", "''") + "'"
  definerPrivileges.zipWithIndex.foreach { (set, index) =>
    set.rows.foreach { row =>
      def text(label: String): String = row.cells(set.columns.indexWhere(_.label == label)).asInstanceOf[MySqlAuditText].value
      require(text("GRANTEE") == grantee && text("IS_GRANTABLE") == "NO", "audit definer identity/grant option differs from minimal profile")
      if index < 3 then require(text("TABLE_SCHEMA") == schema, "audit definer privileges escape the exact schema")
      val allowed = if index == 1 then Set("SELECT") else if index == 3 then Set("USAGE") else Set("SELECT", "TRIGGER")
      require(allowed.contains(text("PRIVILEGE_TYPE")), "audit definer has an unsupported privilege")
    }
  }
  private val schemaPrivileges = definerPrivileges(2)
  private val privilegeIndex = schemaPrivileges.columns.indexWhere(_.label == "PRIVILEGE_TYPE")
  require(schemaPrivileges.rows.map(_.cells(privilegeIndex).asInstanceOf[MySqlAuditText].value).toSet == Set("SELECT", "TRIGGER"),
    "ScriptPublication1 audit definer requires exact FP-only schema SELECT/TRIGGER metadata visibility")
  require(configurationSha256 != null && configurationSha256.length == 32 && sourceProvenance != null &&
    Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT").contains(sourceProvenanceKind) && sourceProvenanceSha256 != null && sourceProvenanceSha256.length == 32 && limitProfile != null,
    "complete independent ScriptPublication1 source/profile inputs are required")
  require(sourceProvenance.hasSha256(sourceProvenanceSha256),
    "independent source provenance bytes/SHA disagree")
  private def reject(reason: String): Nothing = throw new SQLNonTransientConnectionException(reason)
  private[mysql] def requireIdentity(server: UUID, version: String, targetSchema: String, principal: String,
      roles: String, provisioning: UUID, binding: UUID, ddl: String, definer: String): Unit =
    if server != serverUuid || version != serverVersion || targetSchema != schema || principal != effectiveUser || roles != callerRoles ||
      provisioning != provisioningUuid || binding != bindingUuid || !ddl.equalsIgnoreCase(ddlSha256) || definer != auditDefiner then
      reject("independent ScriptPublication1 inputs belong to another exact schema/identity/profile")
  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope, privileges: Vector[MySqlAuditResultSet]): Unit =
    if before != after || before.serverUuid != serverUuid || before.serverVersion != Some(serverVersion) || before.defaultSchema != schema ||
      before.effectivePrincipal != effectiveUser || before.connectedUser != connectedUser || before.effectiveRoles != callerRoles ||
      before.ownGrants != callerGrants || privileges != definerPrivileges then
      reject("ScriptPublication1 caller identity/full grants or complete definer privileges differ from independent inputs")
  private[mysql] def validateRetainedObservation(sets: Vector[MySqlAuditResultSet], before: MySqlCallerScope, after: MySqlCallerScope): Unit =
    var retained = 0L
    def add(count: Long): Unit =
      retained = Math.addExact(retained, count)
      if retained > limitProfile.maxRetainedBytes() then reject("complete ScriptPublication1 observation exceeds explicit retained framing")
    def text(value: String): Unit =
      val count = MySqlSchemaAuditor.encodeUtf8(value).length.toLong
      if count > limitProfile.maxSqlPacketBytes() then reject("ScriptPublication1 text exceeds explicit SQL packet bound")
      add(4L + count)
    for caller <- Vector(before, after) do
      add(16L + 8L + 4L)
      Vector(caller.serverVersion.getOrElse(reject("ScriptPublication1 caller version is missing")), caller.defaultSchema,
        caller.effectivePrincipal, caller.effectiveRoles, caller.connectedUser).foreach(text)
      caller.ownGrants.foreach(text)
    add(4L)
    sets.foreach { set =>
      text(set.id); add(8L)
      set.columns.foreach { column => text(column.label); text(column.typeName); add(17L) }
      set.rows.foreach { row =>
        add(4L)
        row.cells.foreach { cell =>
          add(1L)
          cell match
            case MySqlAuditNull => ()
            case MySqlAuditText(value) => text(value)
            case binary: MySqlAuditBinary =>
              val length = binary.length
              if length.toLong > limitProfile.maxSqlPacketBytes() then reject("ScriptPublication1 binary exceeds explicit SQL packet bound")
              add(4L + length.toLong)
            case MySqlAuditTimestamp(value) => text(value.toString)
        }
      }
    }
  private[mysql] val manifestLabels = Vector("configuration_sha256", "source_provenance_kind", "source_provenance_bytes", "source_provenance_sha256",
    "limit_profile_encoding", "limit_profile_bytes", "limit_profile_sha256", "max_region_bytes", "max_chunk_bytes", "max_decompressed_bytes",
    "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes", "max_native_artifact_bytes")
  private[mysql] def expectedManifestCells: Vector[MySqlAuditCell] = Vector(configurationSha256, MySqlAuditText(sourceProvenanceKind),
    sourceProvenance, sourceProvenanceSha256, MySqlAuditText(SourceLimitProfile.ENCODING), MySqlAuditBinary(limitProfile.canonicalBytes()),
    MySqlAuditBinary(limitProfile.sha256())) ++ Vector(limitProfile.maxRegionBytes(), limitProfile.maxChunkBytes(), limitProfile.maxDecompressedBytes(),
    limitProfile.maxNbtDepth(), limitProfile.maxMembers(), limitProfile.maxRetainedBytes(), limitProfile.maxSqlPacketBytes(), limitProfile.maxNativeArtifactBytes())
    .map(v => MySqlAuditText(v.toString))
  private[mysql] def validateManifest(set: MySqlAuditResultSet, row: MySqlAuditRow): Unit =
    val actual = manifestLabels.map { label =>
      val index = set.columns.indexWhere(_.label == label)
      if index < 0 || row.cells.size != set.columns.size then reject("ScriptPublication1 full manifest shape is incomplete")
      row.cells(index)
    }
    if actual != expectedManifestCells then reject("ScriptPublication1 full source/profile/budget manifest differs from independent inputs")
    // 唯一 codec 再核实际完整字节；预算必须来自这些字节而不是 detached scalar。
    val profile = SourceLimitProfile.decode(actual(5).asInstanceOf[MySqlAuditBinary].bytes, actual(6).asInstanceOf[MySqlAuditBinary].bytes)
    if profile != limitProfile then reject("ScriptPublication1 decoded LimitProfile differs from independent canonical bytes")
  override def toString: String = "MySqlScriptSchemaAuditInputs[redacted; independent validation inputs only]"

/** 无环配置投影；来源/capture/DDL 的批准摘要由独立 baseline 与完整 manifest 比较负责。
  * 不纳入可间接包含本配置摘要的外层文档 hash，也不读取任何 secret payload。
  */
private[mysql] object MySqlScriptSchemaConfiguration:
  val Domain = "FloatingPod-MySQL-ScriptPublication1-Inspection-Configuration-v1"

  def requireMatches(metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata,
      inputs: MySqlScriptSchemaInputs, policy: MySqlRuntimeAuditPolicy, security: MySqlScriptSchemaAuditInputs): Unit =
    if !security.configurationSha256.equalsBytes(sha256(metadata, inputs, policy, security)) then
      throw new SQLNonTransientConnectionException("schema4 actual configuration projection differs from independent approval")

  def sha256(metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata,
      inputs: MySqlScriptSchemaInputs, policy: MySqlRuntimeAuditPolicy, security: MySqlScriptSchemaAuditInputs): Array[Byte] =
    require(metadata != null && inputs != null && policy != null && security != null &&
      metadata.baseline().schemaFormat() == 4 && metadata.baseline().coverage() == MySqlScriptSchemaProfile.Coverage &&
      metadata.schema() == inputs.schema && (inputs.sourceLimitProfile eq security.limitProfile) &&
      policy.expectedImport == MySqlExpectedNoImport && policy.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2,
      "independent schema4 configuration projection requires its exact inputs")
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    var framedBytes = 0L
    def account(amount: Long): Unit =
      framedBytes = Math.addExact(framedBytes, amount)
      if framedBytes > inputs.budget.configBytes then throw new SQLDataException("schema4 configuration projection exceeds explicit config byte budget")
    def int(value: Int): Unit =
      account(4L)
      digest.update(java.nio.ByteBuffer.allocate(4).putInt(value).array())
    def bytes(value: Array[Byte]): Unit =
      account(value.length.toLong)
      int(value.length)
      digest.update(value)
    def text(value: String): Unit =
      val size = MySqlFormat3Utf8.byteLength(value)
      if size > inputs.budget.configBytes - framedBytes - 4L then
        throw new SQLDataException("schema4 configuration text exceeds explicit config byte budget")
      bytes(value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    def pair(name: String, value: Any): Unit =
      text(name)
      text(value.toString)
    text(Domain)
    pair("backend", "mysql"); pair("host", metadata.host()); pair("port", metadata.port())
    pair("schema", metadata.schema()); pair("expectedServerUuid", metadata.expectedServerUuid())
    pair("expectedBindingUuid", metadata.expectedBindingUuid()); pair("expectedEffectiveUser", metadata.expectedEffectiveUser())
    pair("username", metadata.username()); pair("pkcs12TrustStorePath", metadata.pkcs12TrustStorePath())
    pair("credentialSourceRef", "file:" + metadata.credentialSourceRef().absolutePath())
    pair("trustSecretSourceRef", "file:" + metadata.trustSecretSourceRef().absolutePath())
    pair("connectTimeoutMillis", metadata.connectTimeout().toMillis); pair("socketTimeoutMillis", metadata.socketTimeout().toMillis)
    pair("activeLimit", metadata.activeLimit()); pair("innodbFlushLogAtTrxCommit", metadata.innodbFlushLogAtTrxCommit())
    pair("syncBinlog", metadata.syncBinlog())
    val baseline = metadata.baseline()
    pair("approvedBaselinePath", baseline.absolutePath()); pair("approvedBaselineFormat", baseline.format())
    pair("approvedSchemaFormat", baseline.schemaFormat()); pair("approvedCoverage", baseline.coverage())
    pair("expectedServerVersion", baseline.serverVersion()); pair("expectedProvisioningUuid", baseline.provisioningUuid())
    pair("expectedAuditDefiner", baseline.auditDefiner())
    text("sourceLimitProfile"); bytes(inputs.sourceLimitProfile.canonicalBytes()); bytes(inputs.sourceLimitProfile.sha256())
    val budget = inputs.budget
    Vector("evidenceBytes" -> budget.evidenceBytes, "diagnosticsBytes" -> budget.diagnosticsBytes,
      "configBytes" -> budget.configBytes, "requestBytes" -> budget.requestBytes, "snapshotBytes" -> budget.snapshotBytes,
      "receiptBytes" -> budget.receiptBytes, "rows" -> budget.rows, "retainedBytes" -> budget.retainedBytes,
      "packetBytes" -> budget.packetBytes).foreach((name, value) => pair(name, value))
    pair("callerRoles", policy.callerRoles); pair("auditDefinerRoles", policy.auditDefinerRoles)
    pair("expectedImport", "NONE"); pair("expectedPhysicalFormat", 2); pair("maxReceiptAgeNanos", policy.maxReceiptAge.toNanos)
    pair("connectedUser", security.connectedUser)
    int(security.callerGrants.size); security.callerGrants.foreach(text)
    int(security.definerPrivileges.size)
    security.definerPrivileges.foreach { set =>
      text(set.id); int(set.columns.size)
      set.columns.foreach { column =>
        text(column.label); int(column.jdbcType); text(column.typeName); int(column.precision)
        int(column.scale); int(column.nullable); int(if column.signed then 1 else 0)
      }
      int(set.rows.size)
      set.rows.foreach { row =>
        int(row.cells.size)
        row.cells.foreach {
          case MySqlAuditText(value) => text(value)
          case _ => throw new SQLDataException("schema4 privilege configuration requires exact text cells")
        }
      }
    }
    digest.digest()
