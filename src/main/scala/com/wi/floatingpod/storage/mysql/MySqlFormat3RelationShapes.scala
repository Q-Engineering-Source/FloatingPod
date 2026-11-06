package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 直接由冻结 format3 relation 定义与 locked-read SQL 固化的列形状和遍历顺序。 */
private[mysql] enum MySqlFormat3SqlKind:
  case Text, Binary, Integer, Floating, DateTime

private[mysql] final case class MySqlFormat3ColumnShape(
    name: String,
    sqlDeclaration: String,
    kind: MySqlFormat3SqlKind,
    nullable: Boolean,
    maximumLength: Option[Int]
):
  def jdbcType: Int = kind match
    case MySqlFormat3SqlKind.Text => if sqlDeclaration.startsWith("LONGTEXT") then java.sql.Types.LONGVARCHAR else java.sql.Types.VARCHAR
    case MySqlFormat3SqlKind.Binary => if sqlDeclaration.startsWith("LONGBLOB") then java.sql.Types.LONGVARBINARY else java.sql.Types.VARBINARY
    case MySqlFormat3SqlKind.Integer =>
      if sqlDeclaration.startsWith("TINYINT") then java.sql.Types.TINYINT
      else if sqlDeclaration.startsWith("SMALLINT") then java.sql.Types.SMALLINT
      else if sqlDeclaration.startsWith("INT ") then java.sql.Types.INTEGER else java.sql.Types.BIGINT
    case MySqlFormat3SqlKind.Floating => java.sql.Types.DOUBLE
    case MySqlFormat3SqlKind.DateTime => java.sql.Types.TIMESTAMP


  def requireSignedInteger(value: Long, context: String): Unit =
    if kind != MySqlFormat3SqlKind.Integer then
      throw new SQLDataException(s"$context integer type")
    if sqlDeclaration.startsWith("TINYINT") && (value < Byte.MinValue || value > Byte.MaxValue) then
      throw new SQLDataException(s"$context TINYINT range")
    if sqlDeclaration.startsWith("SMALLINT") && (value < Short.MinValue || value > Short.MaxValue) then
      throw new SQLDataException(s"$context SMALLINT range")
    if sqlDeclaration.startsWith("INT ") && (value < Int.MinValue || value > Int.MaxValue) then
      throw new SQLDataException(s"$context INT range")

  def integerPacketWidth: Int =
    if kind != MySqlFormat3SqlKind.Integer then
      throw new SQLDataException("MySQL format3 integer packet width requires an integer column")
    if sqlDeclaration.startsWith("TINYINT") then 4
    else if sqlDeclaration.startsWith("SMALLINT") then 6
    else if sqlDeclaration.startsWith("INT ") then 11
    else 20

private[mysql] final case class MySqlFormat3RelationShape(
    table: String,
    columns: Vector[MySqlFormat3ColumnShape],
    primaryKey: Vector[String],
    lockClause: String
):
  require(table.matches("[a-z][a-z0-9_]{0,63}"), "format3 table identifier is unsafe")
  require(columns.nonEmpty && columns.map(_.name).distinct.size == columns.size, "format3 relation columns must be nonempty and unique")
  require(primaryKey.nonEmpty && primaryKey.forall(columns.map(_.name).contains), "format3 relation primary key is malformed")
  require(lockClause == "FOR SHARE" || lockClause == "FOR UPDATE", "format3 lock clause is not approved")

private[mysql] object MySqlFormat3RelationShapes:
  val SourceFoundationSha256 = "697BB277493D978D17069170A003836ED5906B10F75BA8D0E899ABDAE28922DE"
  val SourceRelationDefinitionsSha256 = "889B1D6901613A7BC64ECC2BDE85C3C688119BAA53AD39954C645A049B67CD08"
  val SourceLockedReadSha256 = "41882CC6F30184F0717B022BB39460CFF4D8B5AD3449BF81A8CEA44DB42BB9B1"
  val ExpectedRelationCount = 20
  val ExpectedColumnCount = 368
  /** 查询顺序固定于 full20-locked-read.sql，并决定行锁的取得顺序。 */
  val lockedReadOrder: Vector[MySqlFormat3RelationShape] = Vector(
    MySqlFormat3RelationShape(
      "fp_schema_manifest",
      Vector(
        MySqlFormat3ColumnShape("singleton_fixed1", "TINYINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("backend", "VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(8)),
        MySqlFormat3ColumnShape("format_version", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("ddl_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("provisioning_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("bootstrap_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("expected_server_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("expected_binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("import_manifest_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("import_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("import_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("import_snapshot_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("import_receipt_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("guard_manifest_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("configuration_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("source_provenance_kind", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("source_provenance_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("source_provenance_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("limit_profile_encoding", "VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(48)),
        MySqlFormat3ColumnShape("limit_profile_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("limit_profile_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("max_region_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_chunk_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_decompressed_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_nbt_depth", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_members", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_retained_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_sql_packet_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("max_native_artifact_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None)
      ),
      Vector("singleton_fixed1"),
      "FOR SHARE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_binding",
      Vector(
        MySqlFormat3ColumnShape("singleton_fixed1", "TINYINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("cutover_state", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("format_version", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None)
      ),
      Vector("singleton_fixed1"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "fp_runtime_writer",
      Vector(
        MySqlFormat3ColumnShape("singleton_fixed1", "TINYINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("writer_epoch", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("owner_boot_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("lease_until", "DATETIME(6) NOT NULL", MySqlFormat3SqlKind.DateTime, nullable = false, maximumLength = Some(6)),
        MySqlFormat3ColumnShape("last_commit_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("last_commit_fingerprint", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32))
      ),
      Vector("singleton_fixed1"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "fp_recovery_reservation",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_kind", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_key_hash", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("resource_key", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("reservation_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("workflow_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("reservation_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_pod_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_unit_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "resource_kind", "resource_key_hash"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "fp_recovery_operation",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("workflow_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("step", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("operation_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("footprint_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_unit_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("forest_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("boot_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("boot_generation", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("connection_generation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("connection_id", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("fence_epoch", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("request_identity_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("request_identity_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("expected_domain_version", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("expected_locator_revision", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("expected_source_revision", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("context_encoding", "VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(48)),
        MySqlFormat3ColumnShape("original_context_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("original_context_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("scope_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("scope_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("preimage_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("preimage_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("target_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("reserve_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("previous_step_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("settled_by_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("commit_witness_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("commit_witness_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("original_diagnostic_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("original_diagnostic_identity", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("planned_at", "DATETIME(6) NOT NULL", MySqlFormat3SqlKind.DateTime, nullable = false, maximumLength = Some(6)),
        MySqlFormat3ColumnShape("fixed_lease_until", "DATETIME(6) NOT NULL", MySqlFormat3SqlKind.DateTime, nullable = false, maximumLength = Some(6)),
        MySqlFormat3ColumnShape("result_at", "DATETIME(6) NULL", MySqlFormat3SqlKind.DateTime, nullable = true, maximumLength = Some(6)),
        MySqlFormat3ColumnShape("operation_fingerprint", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32))
      ),
      Vector("binding_uuid", "operation_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "floating_pod_state",
      Vector(
        MySqlFormat3ColumnShape("uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("owner_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("state_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("world_name", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("pos_x", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("pos_y", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("pos_z", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("yaw", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("pitch", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("is_active", "TINYINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_updated", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("dimension_id", "INT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("operating_mode", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "player_entitlement",
      Vector(
        MySqlFormat3ColumnShape("player_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("registration_limit", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("simultaneous_active_limit", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("player_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "binding_bootstrap_intent",
      Vector(
        MySqlFormat3ColumnShape("singleton_fixed1", "TINYINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("bootstrap_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("prepare_write_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("certify_write_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("binding_format_version", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("provisioning_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_world_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("provision_receipt_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("stage", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("marker_ack_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32))
      ),
      Vector("singleton_fixed1"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_footprint",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("origin_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_world_name", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_pos_x", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_pos_y", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_pos_z", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_yaw", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_pitch", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_dimension_id", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("center_chunk_x", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("center_chunk_z", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("purpose", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("seal_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "pod_uuid", "attempt_uuid", "footprint_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_footprint_chunk",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("dimension_id", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("chunk_x", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("chunk_z", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None)
      ),
      Vector("binding_uuid", "pod_uuid", "attempt_uuid", "footprint_uuid", "dimension_id", "chunk_x", "chunk_z"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_intent",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("origin", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("phase", "VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(24)),
        MySqlFormat3ColumnShape("target_footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("prepare_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("expected_owner_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("expected_domain_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("predecessor_attempt_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("predecessor_footprint_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_completion_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("fresh_absence_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("fresh_absence_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("source_kind", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL", MySqlFormat3SqlKind.Text, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("source_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL", MySqlFormat3SqlKind.Text, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("source_identity_sha256", "VARBINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CAST(source_identity AS BINARY),256))) STORED", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("catalog_generation", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("create_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_unit_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_forest_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("activation_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("committed_domain_version", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_at", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_world_name", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL", MySqlFormat3SqlKind.Text, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_pos_x", "DOUBLE NULL", MySqlFormat3SqlKind.Floating, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_pos_y", "DOUBLE NULL", MySqlFormat3SqlKind.Floating, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_pos_z", "DOUBLE NULL", MySqlFormat3SqlKind.Floating, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_yaw", "DOUBLE NULL", MySqlFormat3SqlKind.Floating, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_pitch", "DOUBLE NULL", MySqlFormat3SqlKind.Floating, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("committed_dimension_id", "INT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("cleanup_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("cleanup_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("cleanup_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("intent_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "pod_uuid", "attempt_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_locator",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("current_attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("current_footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("locator_state", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("reason_code", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("domain_version", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("target_dimension_id", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("locator_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "pod_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_recovery_obligation",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("origin_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("purpose", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_retirement_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("obligation_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("obligation_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("verified_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("verification_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("verification_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "pod_uuid", "attempt_uuid", "footprint_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "fp_lifecycle_completion",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("receipt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("completion_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("completion_kind", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("boot_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("boot_generation", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("world_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("source_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("owner_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("before_domain_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("after_domain_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("removal_result_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("removal_result_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("ticket_result_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("ticket_result_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("checkpoint_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("checkpoint_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("original_context_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("original_context_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("allowed_use", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("completion_state", "VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(24)),
        MySqlFormat3ColumnShape("consumed_by_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("completion_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "receipt_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_footprint_transfer",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("move_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("pod_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("attempt_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("from_footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("to_footprint_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("expected_domain_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_domain_version", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_world_name", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_pos_x", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_pos_y", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_pos_z", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_yaw", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_pitch", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("from_dimension_id", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_world_name", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_pos_x", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_pos_y", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_pos_z", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_yaw", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_pitch", "DOUBLE NOT NULL", MySqlFormat3SqlKind.Floating, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("to_dimension_id", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("prepare_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("commit_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("cleanup_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("phase", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("move_plan_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("move_plan_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("cleanup_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("cleanup_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("transfer_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "move_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_source_unit",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("world_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("save_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("loader_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("catalog_generation", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("read_generation", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("source_observation_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("source_observation_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("current_epoch", "BIGINT NULL", MySqlFormat3SqlKind.Integer, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("current_forest_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "unit_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_source_forest",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("forest_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("source_epoch", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("forest_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("forest_codec", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("forest_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("forest_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("member_count", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("root_count", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("carrier_count", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("retained_bytes", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("fp_pod_uuid_index_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("fp_pod_uuid_index_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32))
      ),
      Vector("binding_uuid", "unit_uuid", "forest_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_source_transfer",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("transfer_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("old_epoch", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("old_forest_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("target_epoch", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("target_forest_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("transfer_plan_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("transfer_plan_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("prepare_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("cutover_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("retire_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("phase", "VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(48)),
        MySqlFormat3ColumnShape("target_receipts_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("target_receipts_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("retirement_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("retirement_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("transfer_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "transfer_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_source_write",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("write_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("workflow_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_kind", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("carrier_key_hash", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("carrier_key", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("save_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("loader_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("region_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("slot_x", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("slot_z", "INT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("write_generation", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("payload_descriptor_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("payload_descriptor_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("payload_length", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("payload_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("payload_artifact_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("undo_kind", "VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(24)),
        MySqlFormat3ColumnShape("undo_descriptor_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("undo_descriptor_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("undo_length", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("undo_content_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("undo_artifact_identity", "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("member_coverage_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("member_coverage_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("obligation_revisions_bytes", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("obligation_revisions_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("limit_profile_sha256", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("receipt_codec", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("prepared_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("write_state", "VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(40)),
        MySqlFormat3ColumnShape("resolved_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("durable_receipt_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("durable_receipt_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("failure_diagnostic_identity", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("failure_diagnostic_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("write_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "write_uuid"),
      "FOR UPDATE"
    ),
    MySqlFormat3RelationShape(
      "physical_source_obligation",
      Vector(
        MySqlFormat3ColumnShape("binding_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("unit_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("forest_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("resource_kind", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("carrier_key_hash", "VARBINARY(32) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("carrier_key", "LONGBLOB NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("purpose", "VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(24)),
        MySqlFormat3ColumnShape("obligation_state", "VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL", MySqlFormat3SqlKind.Text, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("obligation_revision", "BIGINT NOT NULL", MySqlFormat3SqlKind.Integer, nullable = false, maximumLength = None),
        MySqlFormat3ColumnShape("created_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("write_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("discharged_operation_uuid", "VARBINARY(16) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(16)),
        MySqlFormat3ColumnShape("discharge_kind", "VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL", MySqlFormat3SqlKind.Text, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("discharge_proof_bytes", "LONGBLOB NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = None),
        MySqlFormat3ColumnShape("discharge_proof_sha256", "VARBINARY(32) NULL", MySqlFormat3SqlKind.Binary, nullable = true, maximumLength = Some(32)),
        MySqlFormat3ColumnShape("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL", MySqlFormat3SqlKind.Binary, nullable = false, maximumLength = Some(16))
      ),
      Vector("binding_uuid", "unit_uuid", "forest_uuid", "carrier_key_hash", "purpose"),
      "FOR UPDATE"
    )
  )
  /** 规范编码顺序单独固定，不与事务锁取得顺序混用。 */
  val canonicalOrder: Vector[MySqlFormat3RelationShape] = Vector(
    "fp_schema_manifest",
    "fp_runtime_writer",
    "physical_recovery_binding",
    "fp_recovery_operation",
    "floating_pod_state",
    "player_entitlement",
    "binding_bootstrap_intent",
    "fp_recovery_reservation",
    "physical_recovery_footprint",
    "physical_recovery_footprint_chunk",
    "physical_recovery_intent",
    "physical_recovery_locator",
    "physical_recovery_obligation",
    "fp_lifecycle_completion",
    "physical_footprint_transfer",
    "physical_source_unit",
    "physical_source_forest",
    "physical_source_transfer",
    "physical_source_write",
    "physical_source_obligation"
  ).map(forTable)

  require(lockedReadOrder.size == ExpectedRelationCount && lockedReadOrder.map(_.columns.size).sum == ExpectedColumnCount,
    "format3 relation shape differs from the pinned 20-relation/368-column input")
  require(lockedReadOrder.map(_.table).distinct.size == ExpectedRelationCount,
    "format3 relation shape repeats a table")
  require(canonicalOrder.size == ExpectedRelationCount && canonicalOrder.map(_.table).distinct.size == ExpectedRelationCount &&
    canonicalOrder.map(_.table).toSet == lockedReadOrder.map(_.table).toSet,
    "format3 canonical order differs from its exact locked relation set")
  def forTable(table: String): MySqlFormat3RelationShape =
    lockedReadOrder.find(_.table == table).getOrElse(throw new IllegalArgumentException("table is outside the pinned format3 relation set"))
