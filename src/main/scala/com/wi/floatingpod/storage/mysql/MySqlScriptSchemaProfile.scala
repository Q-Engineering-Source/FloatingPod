package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** 独立的 schema4 定义；不接管现 format3 repository、audit codec 或 B1。
  * 结构检查只证明 SOURCE 身份，不代表 MySQL 安装或事务行为验收。
  */
object MySqlScriptSchemaProfile:
  val SchemaFormat = 4
  val PublicationVersion = 1
  val PhysicalBindingFormat = 2
  val Schema = "fp_script_publication1"
  val Coverage = "script-publication1:exact24-tables-exact72-guards-auditself-and-privileges"
  val AuditRoutine = "fp_audit_schema_script_publication1"
  val AuditDomain = "FloatingPod-MySQL-Audit-script-publication1-v1"
  val SnapshotDomain = "floatingpod.mysql.script-publication1-full24/v1"
  val SourceLimit = 65536L
  val CompilationLimit = 20357L
  val Resource = "/db/mysql/script-publication-1.sql"
  val SqlSha256 = "55b98586d04ba246fe7546786212e2fafb245a33bfe81e76ad5d56a53adf764e"
  final case class Column(name: String, declaration: String)
  final case class ForeignKey(name: String, columns: Vector[String], target: String, targetColumns: Vector[String])
  final case class Index(name: String, columns: Vector[String], unique: Boolean)
  final case class Table(name: String, columns: Vector[Column], primaryKey: Vector[String], foreignKeys: Vector[ForeignKey], checks: Vector[String], indexes: Vector[Index], statementSha256: String)
  final case class Guard(name: String, table: String, event: String, statementSha256: String)

  /** 全部 quota 由调用者明确提供；不默认猜测生产容量。 */
  final case class Budget(evidenceBytes: Long, diagnosticsBytes: Long, configBytes: Long,
                          requestBytes: Long, snapshotBytes: Long, receiptBytes: Long,
                          rows: Long, retainedBytes: Long, packetBytes: Long):
    require(Vector(evidenceBytes, diagnosticsBytes, configBytes, requestBytes, snapshotBytes,
      receiptBytes, rows, retainedBytes, packetBytes).forall(_ > 0), "explicit positive publication budgets required")
    require(Vector(evidenceBytes, diagnosticsBytes, configBytes, requestBytes, snapshotBytes,
      receiptBytes).forall(n => n <= retainedBytes && n <= packetBytes), "blob budget exceeds retained/packet envelope")

  enum Payload:
    case Source, Compilation, Evidence, Diagnostics, Config, Request, Snapshot, Receipt

  def validatePayload(kind: Payload, bytes: Array[Byte], expectedSha256: Array[Byte], budget: Budget): Either[String, Unit] =
    val limit = kind match
      case Payload.Source => SourceLimit
      case Payload.Compilation => CompilationLimit
      case Payload.Evidence => budget.evidenceBytes
      case Payload.Diagnostics => budget.diagnosticsBytes
      case Payload.Config => budget.configBytes
      case Payload.Request => budget.requestBytes
      case Payload.Snapshot => budget.snapshotBytes
      case Payload.Receipt => budget.receiptBytes
    if bytes == null || expectedSha256 == null then Left("null payload/hash")
    else if bytes.length.toLong > limit || bytes.length.toLong > budget.packetBytes || bytes.length.toLong > budget.retainedBytes then Left("payload budget exceeded")
    else if expectedSha256.length != 32 || !MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(bytes), expectedSha256) then Left("payload SHA-256 mismatch")
    else Right(())

  def retainedSize(lengths: Iterable[Long], rowCount: Long, budget: Budget): Either[String, Long] =
    if rowCount < 0 || rowCount > budget.rows then Left("row budget exceeded")
    else
      try
        val total = lengths.foldLeft(0L) { (sum, n) =>
          require(n >= 0, "negative retained length")
          Math.addExact(sum, n)
        }
        if total > budget.retainedBytes then Left("retained budget exceeded") else Right(total)
      catch
        case _: ArithmeticException => Left("retained length overflow")
        case _: IllegalArgumentException => Left("negative retained length")

  def sha256(bytes: Array[Byte]): String =
    java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

  /** 固定 definition script 的语句分帧；不是任意 SQL parser，也不执行 SQL。 */
  def statements(sql: String): Either[String, Vector[String]] =
    val out = Vector.newBuilder[String]
    val buffer = new StringBuilder
    var delimiter = ";"
    var error: Option[String] = None
    sql.linesIterator.foreach { line =>
      if line.startsWith("DELIMITER ") then
        if buffer.nonEmpty then error = Some("delimiter inside statement")
        delimiter = line.substring(10).trim
        if delimiter != ";" && delimiter != "$$" then error = Some("unsupported delimiter")
      else if line.trim.nonEmpty && !line.trim.startsWith("--") then
        buffer.append(line).append('\n')
        if line.trim.endsWith(delimiter) then
          val s = buffer.toString.trim
          out += s.substring(0, s.length - delimiter.length).trim
          buffer.clear()
    }
    if buffer.nonEmpty then Left("unterminated SQL statement")
    else error.toLeft(out.result())

  def validateStructure(): Vector[String] =
    val errors = Vector.newBuilder[String]
    val byName = tables.map(t => t.name -> t).toMap
    if tables.size != 24 || byName.size != 24 then errors += "expected exact24 unique tables"
    tables.foreach { t =>
      val names = t.columns.map(_.name)
      if names.distinct.size != names.size || t.primaryKey.isEmpty || !t.primaryKey.forall(names.contains) then errors += s"invalid columns/PK: ${t.name}"
      t.indexes.foreach(i => if i.columns.isEmpty || !i.columns.forall(names.contains) then errors += s"invalid index: ${i.name}")
      t.foreignKeys.foreach { f =>
        byName.get(f.target) match
          case None => errors += s"missing FK target: ${f.name}"
          case Some(target) =>
            val uniqueKeys = target.primaryKey +: target.indexes.filter(_.unique).map(_.columns)
            if f.columns.isEmpty || f.columns.size != f.targetColumns.size || !f.columns.forall(names.contains) || !uniqueKeys.contains(f.targetColumns) then errors += s"invalid FK key: ${f.name}"
            else f.columns.zip(f.targetColumns).foreach { (a, b) =>
              val at = t.columns.find(_.name == a).get.declaration.replaceAll(" (NOT NULL|NULL)$", "")
              val bt = target.columns.find(_.name == b).map(_.declaration.replaceAll(" (NOT NULL|NULL)$", ""))
              if !bt.contains(at) then errors += s"incompatible FK type: ${f.name}"
            }
      }
    }
    if guards.size != 72 || guards.map(_.name).distinct.size != 72 then errors += "expected exact72 unique guards"
    tables.foreach(t => if guards.filter(_.table == t.name).map(_.event).sorted != Vector("DELETE", "INSERT", "UPDATE") then errors += s"incomplete guards: ${t.name}")
    errors.result()

  /** 精确身份检查覆盖完整 DDL、guard body、audit、seed 与 grants；不把哈希称为行为测试。 */
  def validateSql(sql: String): Either[Vector[String], Unit] =
    val errors = Vector.newBuilder[String]
    errors ++= validateStructure()
    statements(sql) match
      case Left(e) => errors += e
      case Right(all) =>
        val tablePattern = "(?s)CREATE TABLE (\\w+) .*".r
        val triggerPattern = "(?s)CREATE DEFINER=.*? TRIGGER (\\w+) BEFORE (INSERT|UPDATE|DELETE) ON (\\w+) .*".r
        val actualTables = all.collect { case s @ tablePattern(name) => name -> sha256(s.getBytes(StandardCharsets.UTF_8)) }
        val actualGuards = all.collect { case s @ triggerPattern(name, event, table) => Guard(name, table, event, sha256(s.getBytes(StandardCharsets.UTF_8))) }
        if actualTables.size != 24 || actualTables.toMap != tables.map(t => t.name -> t.statementSha256).toMap then errors += "table definitions differ"
        if actualGuards != guards then errors += "guard definitions/order differ"
        if all.count(_.matches("(?s)CREATE DEFINER='fp_s4_audit_definer'@'localhost'\\s+PROCEDURE.*")) != 1 then errors += "audit routine count differs"
    if sha256(sql.getBytes(StandardCharsets.UTF_8)) != SqlSha256 then errors += "complete SQL identity differs"
    val result = errors.result()
    if result.isEmpty then Right(()) else Left(result)

  val BaselineHashes: Map[String, String] = Map("foundation.sql" -> "697bb277493d978d17069170a003836ed5906b10f75ba8d0e899abdae28922de","relation-definitions.json" -> "889b1d6901613a7bc64ecc2bde85c3c688119baa53ad39954c645a049b67cd08","install-guards.sql" -> "64e0b71a860a41ff570f8400b793eab8be92532b47164d5fa2e2a8f915a00065","audit-procedure.sql" -> "e4b8cef7b70ade7f5d43eba20b2e0be56cf39f919ebd3653c6655afb207bb7d3","object-grants.sql" -> "e361ecf8d64ca32f408b81bf2b009793a0a5c70d600006e7bdd3aef731feaa1e","schema-and-accounts.sql" -> "46ce053e4af343c8a47bdbd4bf1ab3972b3969e44d591f83b82dbfc6d6ae5008")
  private def table00: Table = Table(
    name = "fp_schema_manifest",
    columns = Vector(
      Column("singleton_fixed1","TINYINT NOT NULL"),
      Column("backend","VARCHAR(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("format_version","INT NOT NULL"),
      Column("ddl_sha256","VARBINARY(32) NOT NULL"),
      Column("provisioning_uuid","VARBINARY(16) NOT NULL"),
      Column("bootstrap_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("expected_server_uuid","VARBINARY(16) NOT NULL"),
      Column("expected_binding_uuid","VARBINARY(16) NOT NULL"),
      Column("import_manifest_sha256","VARBINARY(32) NULL"),
      Column("import_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("import_operation_uuid","VARBINARY(16) NULL"),
      Column("import_snapshot_sha256","VARBINARY(32) NULL"),
      Column("import_receipt_sha256","VARBINARY(32) NULL"),
      Column("guard_manifest_sha256","VARBINARY(32) NULL"),
      Column("configuration_sha256","VARBINARY(32) NOT NULL"),
      Column("source_provenance_kind","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("source_provenance_bytes","LONGBLOB NOT NULL"),
      Column("source_provenance_sha256","VARBINARY(32) NOT NULL"),
      Column("limit_profile_encoding","VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("limit_profile_bytes","LONGBLOB NOT NULL"),
      Column("limit_profile_sha256","VARBINARY(32) NOT NULL"),
      Column("max_region_bytes","BIGINT NOT NULL"),
      Column("max_chunk_bytes","BIGINT NOT NULL"),
      Column("max_decompressed_bytes","BIGINT NOT NULL"),
      Column("max_nbt_depth","BIGINT NOT NULL"),
      Column("max_members","BIGINT NOT NULL"),
      Column("max_retained_bytes","BIGINT NOT NULL"),
      Column("max_sql_packet_bytes","BIGINT NOT NULL"),
      Column("max_native_artifact_bytes","BIGINT NOT NULL")
    ),
    primaryKey = Vector("singleton_fixed1"),
    foreignKeys = Vector(),
    checks = Vector(
      "singleton_fixed1=1",
      "backend='MYSQL'",
      "format_version=4",
      "bootstrap_state IN ('BOOTSTRAPPING','READY')",
      "import_state IN ('NONE','PENDING','COMMITTED')",
      "source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT')",
      "limit_profile_encoding='FP_SOURCE_LIMIT_PROFILE_BE_V1'",
      "(import_state='NONE' AND import_manifest_sha256 IS NULL AND import_operation_uuid IS NULL AND import_snapshot_sha256 IS NULL AND import_receipt_sha256 IS NULL) OR (import_state='PENDING' AND import_manifest_sha256 IS NOT NULL AND import_operation_uuid IS NOT NULL AND import_snapshot_sha256 IS NOT NULL AND import_receipt_sha256 IS NULL) OR (import_state='COMMITTED' AND import_manifest_sha256 IS NOT NULL AND import_operation_uuid IS NOT NULL AND import_snapshot_sha256 IS NOT NULL AND import_receipt_sha256 IS NOT NULL)",
      "bootstrap_state<>'READY' OR (guard_manifest_sha256 IS NOT NULL AND import_state IN ('NONE','COMMITTED'))",
      "max_region_bytes>0",
      "max_chunk_bytes>0",
      "max_decompressed_bytes>0",
      "max_nbt_depth>0",
      "max_members>0",
      "max_retained_bytes>0",
      "max_sql_packet_bytes>0",
      "max_native_artifact_bytes>0",
      "OCTET_LENGTH(ddl_sha256)=32",
      "OCTET_LENGTH(provisioning_uuid)=16 AND provisioning_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(expected_server_uuid)=16 AND expected_server_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(expected_binding_uuid)=16 AND expected_binding_uuid<>UNHEX(REPEAT('00',16))",
      "import_manifest_sha256 IS NULL OR OCTET_LENGTH(import_manifest_sha256)=32",
      "import_operation_uuid IS NULL OR (OCTET_LENGTH(import_operation_uuid)=16 AND import_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "import_snapshot_sha256 IS NULL OR OCTET_LENGTH(import_snapshot_sha256)=32",
      "import_receipt_sha256 IS NULL OR OCTET_LENGTH(import_receipt_sha256)=32",
      "guard_manifest_sha256 IS NULL OR OCTET_LENGTH(guard_manifest_sha256)=32",
      "OCTET_LENGTH(configuration_sha256)=32",
      "OCTET_LENGTH(source_provenance_bytes)>0",
      "OCTET_LENGTH(source_provenance_sha256)=32",
      "OCTET_LENGTH(limit_profile_bytes)>0",
      "OCTET_LENGTH(limit_profile_sha256)=32",
      "max_region_bytes>=0",
      "max_chunk_bytes>=0",
      "max_decompressed_bytes>=0",
      "max_nbt_depth>=0",
      "max_members>=0",
      "max_retained_bytes>=0",
      "max_sql_packet_bytes>=0",
      "max_native_artifact_bytes>=0",
      "source_provenance_sha256=UNHEX(SHA2(source_provenance_bytes,256))",
      "limit_profile_sha256=UNHEX(SHA2(limit_profile_bytes,256))"
    ),
    indexes = Vector(),
    statementSha256 = "6ae35e2468c78bbf825992fb6104daaea0ba0bbf79c46b6c15fbcc2285c71b2c"
  )
  private def table01: Table = Table(
    name = "fp_runtime_writer",
    columns = Vector(
      Column("singleton_fixed1","TINYINT NOT NULL"),
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("writer_epoch","BIGINT NOT NULL"),
      Column("owner_boot_uuid","VARBINARY(16) NULL"),
      Column("lease_until","DATETIME(6) NOT NULL"),
      Column("last_commit_operation_uuid","VARBINARY(16) NULL"),
      Column("last_commit_fingerprint","VARBINARY(32) NULL")
    ),
    primaryKey = Vector("singleton_fixed1"),
    foreignKeys = Vector(),
    checks = Vector(
      "singleton_fixed1=1",
      "((last_commit_operation_uuid IS NULL AND last_commit_fingerprint IS NULL) OR (last_commit_operation_uuid IS NOT NULL AND last_commit_fingerprint IS NOT NULL))",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "writer_epoch>=0",
      "owner_boot_uuid IS NULL OR (OCTET_LENGTH(owner_boot_uuid)=16 AND owner_boot_uuid<>UNHEX(REPEAT('00',16)))",
      "last_commit_operation_uuid IS NULL OR (OCTET_LENGTH(last_commit_operation_uuid)=16 AND last_commit_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "last_commit_fingerprint IS NULL OR OCTET_LENGTH(last_commit_fingerprint)=32"
    ),
    indexes = Vector(),
    statementSha256 = "06d915e2eb0421fbfaddd49926e980cc24ac05e6df92e44f525005aa9beefc9b"
  )
  private def table02: Table = Table(
    name = "physical_recovery_binding",
    columns = Vector(
      Column("singleton_fixed1","TINYINT NOT NULL"),
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("cutover_state","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("format_version","INT NOT NULL")
    ),
    primaryKey = Vector("singleton_fixed1"),
    foreignKeys = Vector(),
    checks = Vector(
      "singleton_fixed1=1",
      "cutover_state IN ('LEGACY_UNCERTIFIED','CERTIFIED')",
      "format_version=2",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(
      Index("ux_binding_uuid",Vector("binding_uuid"),true)
    ),
    statementSha256 = "ae13523f25480bc1a3659e0a0c3cb42d2f316f47f70b8a8b672e734dbaf84268"
  )
  private def table03: Table = Table(
    name = "fp_recovery_operation",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("operation_uuid","VARBINARY(16) NOT NULL"),
      Column("workflow_uuid","VARBINARY(16) NOT NULL"),
      Column("step","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("operation_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NULL"),
      Column("attempt_uuid","VARBINARY(16) NULL"),
      Column("footprint_uuid","VARBINARY(16) NULL"),
      Column("source_unit_uuid","VARBINARY(16) NULL"),
      Column("forest_uuid","VARBINARY(16) NULL"),
      Column("boot_uuid","VARBINARY(16) NOT NULL"),
      Column("boot_generation","BIGINT NOT NULL"),
      Column("connection_generation_uuid","VARBINARY(16) NOT NULL"),
      Column("connection_id","BIGINT NOT NULL"),
      Column("fence_epoch","BIGINT NOT NULL"),
      Column("request_identity_bytes","LONGBLOB NOT NULL"),
      Column("request_identity_sha256","VARBINARY(32) NOT NULL"),
      Column("expected_domain_version","BIGINT NULL"),
      Column("expected_locator_revision","BIGINT NULL"),
      Column("expected_source_revision","BIGINT NULL"),
      Column("context_encoding","VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("original_context_bytes","LONGBLOB NOT NULL"),
      Column("original_context_sha256","VARBINARY(32) NOT NULL"),
      Column("scope_bytes","LONGBLOB NOT NULL"),
      Column("scope_sha256","VARBINARY(32) NOT NULL"),
      Column("preimage_bytes","LONGBLOB NOT NULL"),
      Column("preimage_sha256","VARBINARY(32) NOT NULL"),
      Column("target_bytes","LONGBLOB NOT NULL"),
      Column("target_sha256","VARBINARY(32) NOT NULL"),
      Column("reserve_operation_uuid","VARBINARY(16) NULL"),
      Column("previous_step_operation_uuid","VARBINARY(16) NULL"),
      Column("settled_by_operation_uuid","VARBINARY(16) NULL"),
      Column("commit_witness_bytes","LONGBLOB NULL"),
      Column("commit_witness_sha256","VARBINARY(32) NULL"),
      Column("original_diagnostic_sha256","VARBINARY(32) NULL"),
      Column("original_diagnostic_identity","LONGBLOB NULL"),
      Column("planned_at","DATETIME(6) NOT NULL"),
      Column("fixed_lease_until","DATETIME(6) NOT NULL"),
      Column("result_at","DATETIME(6) NULL"),
      Column("operation_fingerprint","VARBINARY(32) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","operation_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_op_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_op_reserve",Vector("binding_uuid","reserve_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_op_previous",Vector("binding_uuid","previous_step_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_op_settled",Vector("binding_uuid","settled_by_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "step IN ('RESERVE','CANCEL','ENTITLEMENT_REGISTER','DOMAIN_MUTATION','REARM','ACTIVATE','RECALL_CHECKPOINT','ABORT_CLEANUP','MOVE_PREPARE','MOVE_COMMIT','MOVE_CLEANUP','MOVE_REMEDIATE','SOURCE_CAPTURE','SOURCE_PREPARE','SOURCE_WRITE_PREPARE','SOURCE_WRITE_RESULT','SOURCE_WRITE_RECONCILE','SOURCE_CUTOVER','SOURCE_RETIRE','SOURCE_REMEDIATE','RESERVATION_RELEASE','SCRIPT_PUBLICATION')",
      "operation_state IN ('RESERVED','COMMITTED','CANCELLED')",
      "context_encoding='FP_RECOVERY_OPERATION_BE_V1'",
      "boot_generation>0",
      "connection_id>0",
      "((original_diagnostic_sha256 IS NULL AND original_diagnostic_identity IS NULL) OR (original_diagnostic_sha256 IS NOT NULL AND original_diagnostic_identity IS NOT NULL))",
      "(operation_state='RESERVED' AND result_at IS NULL AND settled_by_operation_uuid IS NULL AND commit_witness_bytes IS NULL AND commit_witness_sha256 IS NULL) OR (operation_state='COMMITTED' AND result_at IS NOT NULL AND settled_by_operation_uuid IS NULL AND commit_witness_bytes IS NOT NULL AND commit_witness_sha256 IS NOT NULL) OR (operation_state='CANCELLED' AND result_at IS NOT NULL AND settled_by_operation_uuid IS NOT NULL AND commit_witness_bytes IS NOT NULL AND commit_witness_sha256 IS NOT NULL)",
      "reserve_operation_uuid IS NULL OR reserve_operation_uuid<>operation_uuid",
      "previous_step_operation_uuid IS NULL OR previous_step_operation_uuid<>operation_uuid",
      "settled_by_operation_uuid IS NULL OR settled_by_operation_uuid<>operation_uuid",
      "original_diagnostic_identity IS NULL OR original_diagnostic_sha256=UNHEX(SHA2(original_diagnostic_identity,256))",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))",
      "pod_uuid IS NULL OR (OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16)))",
      "attempt_uuid IS NULL OR (OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16)))",
      "footprint_uuid IS NULL OR (OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16)))",
      "source_unit_uuid IS NULL OR (OCTET_LENGTH(source_unit_uuid)=16 AND source_unit_uuid<>UNHEX(REPEAT('00',16)))",
      "forest_uuid IS NULL OR (OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16)))",
      "OCTET_LENGTH(boot_uuid)=16 AND boot_uuid<>UNHEX(REPEAT('00',16))",
      "boot_generation>=0",
      "OCTET_LENGTH(connection_generation_uuid)=16 AND connection_generation_uuid<>UNHEX(REPEAT('00',16))",
      "connection_id>=0",
      "fence_epoch>=0",
      "OCTET_LENGTH(request_identity_bytes)>0",
      "OCTET_LENGTH(request_identity_sha256)=32",
      "expected_domain_version IS NULL OR expected_domain_version>=0",
      "expected_locator_revision IS NULL OR expected_locator_revision>=0",
      "expected_source_revision IS NULL OR expected_source_revision>=0",
      "OCTET_LENGTH(original_context_bytes)>0",
      "OCTET_LENGTH(original_context_sha256)=32",
      "OCTET_LENGTH(scope_bytes)>0",
      "OCTET_LENGTH(scope_sha256)=32",
      "OCTET_LENGTH(preimage_bytes)>0",
      "OCTET_LENGTH(preimage_sha256)=32",
      "OCTET_LENGTH(target_bytes)>0",
      "OCTET_LENGTH(target_sha256)=32",
      "reserve_operation_uuid IS NULL OR (OCTET_LENGTH(reserve_operation_uuid)=16 AND reserve_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "previous_step_operation_uuid IS NULL OR (OCTET_LENGTH(previous_step_operation_uuid)=16 AND previous_step_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "settled_by_operation_uuid IS NULL OR (OCTET_LENGTH(settled_by_operation_uuid)=16 AND settled_by_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "commit_witness_sha256 IS NULL OR OCTET_LENGTH(commit_witness_sha256)=32",
      "original_diagnostic_sha256 IS NULL OR OCTET_LENGTH(original_diagnostic_sha256)=32",
      "OCTET_LENGTH(operation_fingerprint)=32",
      "request_identity_sha256=UNHEX(SHA2(request_identity_bytes,256))",
      "original_context_sha256=UNHEX(SHA2(original_context_bytes,256))",
      "scope_sha256=UNHEX(SHA2(scope_bytes,256))",
      "preimage_sha256=UNHEX(SHA2(preimage_bytes,256))",
      "target_sha256=UNHEX(SHA2(target_bytes,256))",
      "commit_witness_bytes IS NULL OR (commit_witness_sha256 IS NOT NULL AND commit_witness_sha256=UNHEX(SHA2(commit_witness_bytes,256)))"
    ),
    indexes = Vector(
      Index("ix_op_workflow",Vector("binding_uuid","workflow_uuid","step"),false)
    ),
    statementSha256 = "0ca54f511f1da13456b1c45f91d3869e9d6960b9e6651a2121db19c64620a3cc"
  )
  private def table04: Table = Table(
    name = "floating_pod_state",
    columns = Vector(
      Column("uuid","VARBINARY(16) NOT NULL"),
      Column("owner_uuid","VARBINARY(16) NULL"),
      Column("state_version","BIGINT NOT NULL"),
      Column("world_name","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("pos_x","DOUBLE NOT NULL"),
      Column("pos_y","DOUBLE NOT NULL"),
      Column("pos_z","DOUBLE NOT NULL"),
      Column("yaw","DOUBLE NOT NULL"),
      Column("pitch","DOUBLE NOT NULL"),
      Column("is_active","TINYINT NOT NULL"),
      Column("last_updated","BIGINT NOT NULL"),
      Column("dimension_id","INT NULL"),
      Column("operating_mode","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_domain_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_domain_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "is_active IN (0,1)",
      "operating_mode IN ('NORMAL','WORLD_IDLE')",
      "owner_uuid IS NOT NULL OR is_active=0 OR operating_mode='WORLD_IDLE'",
      "ABS(pos_x) < 1.0e300",
      "ABS(pos_y) < 1.0e300",
      "ABS(pos_z) < 1.0e300",
      "ABS(yaw) < 1.0e30",
      "ABS(pitch) < 1.0e30",
      "OCTET_LENGTH(uuid)=16 AND uuid<>UNHEX(REPEAT('00',16))",
      "owner_uuid IS NULL OR (OCTET_LENGTH(owner_uuid)=16 AND owner_uuid<>UNHEX(REPEAT('00',16)))",
      "state_version>=0",
      "OCTET_LENGTH(world_name)>0",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(
      Index("ix_domain_owner",Vector("owner_uuid"),false),
      Index("ix_domain_active",Vector("is_active"),false),
      Index("ix_domain_updated",Vector("last_updated"),false),
      Index("ix_domain_owner_active",Vector("owner_uuid","is_active"),false)
    ),
    statementSha256 = "b46c092a66e5033245fbf0581f3619c0df8ece73771ac71796b691d5130a645f"
  )
  private def table05: Table = Table(
    name = "player_entitlement",
    columns = Vector(
      Column("player_uuid","VARBINARY(16) NOT NULL"),
      Column("registration_limit","INT NOT NULL"),
      Column("simultaneous_active_limit","INT NOT NULL"),
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("player_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_entitlement_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_entitlement_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "registration_limit>=0",
      "simultaneous_active_limit>=0",
      "OCTET_LENGTH(player_uuid)=16 AND player_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(),
    statementSha256 = "1f87a9038ed6978d8272b400a4eefb27ffc1241509e5ed772a45ec628e331b41"
  )
  private def table06: Table = Table(
    name = "binding_bootstrap_intent",
    columns = Vector(
      Column("singleton_fixed1","TINYINT NOT NULL"),
      Column("bootstrap_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("prepare_write_uuid","VARBINARY(16) NOT NULL"),
      Column("certify_write_uuid","VARBINARY(16) NOT NULL"),
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("binding_format_version","INT NOT NULL"),
      Column("provisioning_uuid","VARBINARY(16) NOT NULL"),
      Column("source_world_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("provision_receipt_sha256","VARBINARY(32) NOT NULL"),
      Column("stage","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("marker_ack_sha256","VARBINARY(32) NULL")
    ),
    primaryKey = Vector("singleton_fixed1"),
    foreignKeys = Vector(),
    checks = Vector(
      "singleton_fixed1=1",
      "prepare_write_uuid<>certify_write_uuid",
      "binding_format_version=2",
      "stage IN ('PREPARED','COMPLETE')",
      "(stage='PREPARED' AND marker_ack_sha256 IS NULL) OR (stage='COMPLETE' AND marker_ack_sha256 IS NOT NULL)",
      "OCTET_LENGTH(bootstrap_operation_uuid)=16 AND bootstrap_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(prepare_write_uuid)=16 AND prepare_write_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(certify_write_uuid)=16 AND certify_write_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(provisioning_uuid)=16 AND provisioning_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(source_world_identity)>0",
      "OCTET_LENGTH(provision_receipt_sha256)=32",
      "marker_ack_sha256 IS NULL OR OCTET_LENGTH(marker_ack_sha256)=32"
    ),
    indexes = Vector(),
    statementSha256 = "96972504cd175facbe57dc1dddc83cf21b491cf5a6b54f5dd908a7423b164eda"
  )
  private def table07: Table = Table(
    name = "fp_recovery_reservation",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("resource_kind","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("resource_key_hash","VARBINARY(32) NOT NULL"),
      Column("resource_key","LONGBLOB NOT NULL"),
      Column("reservation_revision","BIGINT NOT NULL"),
      Column("workflow_uuid","VARBINARY(16) NOT NULL"),
      Column("operation_uuid","VARBINARY(16) NOT NULL"),
      Column("reservation_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("resource_pod_uuid","VARBINARY(16) NULL"),
      Column("resource_unit_uuid","VARBINARY(16) NULL")
    ),
    primaryKey = Vector("binding_uuid","resource_kind","resource_key_hash"),
    foreignKeys = Vector(
      ForeignKey("fk_reservation_operation",Vector("binding_uuid","operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_reservation_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "resource_kind IN ('POD','SOURCE_UNIT','CARRIER')",
      "reservation_state IN ('HELD','RELEASED')",
      "resource_key_hash=UNHEX(SHA2(resource_key,256))",
      "(resource_kind='POD' AND resource_pod_uuid IS NOT NULL AND resource_unit_uuid IS NULL) OR (resource_kind='SOURCE_UNIT' AND resource_pod_uuid IS NULL AND resource_unit_uuid IS NOT NULL) OR (resource_kind='CARRIER' AND resource_pod_uuid IS NULL AND resource_unit_uuid IS NULL)",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(resource_key_hash)=32",
      "OCTET_LENGTH(resource_key)>0",
      "reservation_revision>=0",
      "OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "resource_pod_uuid IS NULL OR (OCTET_LENGTH(resource_pod_uuid)=16 AND resource_pod_uuid<>UNHEX(REPEAT('00',16)))",
      "resource_unit_uuid IS NULL OR (OCTET_LENGTH(resource_unit_uuid)=16 AND resource_unit_uuid<>UNHEX(REPEAT('00',16)))"
    ),
    indexes = Vector(
      Index("ux_reservation_pod",Vector("binding_uuid","resource_kind","resource_pod_uuid"),true),
      Index("ux_reservation_unit",Vector("binding_uuid","resource_kind","resource_unit_uuid"),true)
    ),
    statementSha256 = "86e06ab871759b07ba7c8b6dfe1f1a103617d72dd28e7345c0d0ae00cc1e8b35"
  )
  private def table08: Table = Table(
    name = "physical_recovery_footprint",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("origin_version","BIGINT NOT NULL"),
      Column("target_world_name","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("target_pos_x","DOUBLE NOT NULL"),
      Column("target_pos_y","DOUBLE NOT NULL"),
      Column("target_pos_z","DOUBLE NOT NULL"),
      Column("target_yaw","DOUBLE NOT NULL"),
      Column("target_pitch","DOUBLE NOT NULL"),
      Column("target_dimension_id","INT NOT NULL"),
      Column("center_chunk_x","INT NOT NULL"),
      Column("center_chunk_z","INT NOT NULL"),
      Column("purpose","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("seal_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_footprint_domain",Vector("pod_uuid"),"floating_pod_state",Vector("uuid")),
      ForeignKey("fk_footprint_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_footprint_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "purpose IN ('CREATE_TARGET','MOVE_TARGET','MIGRATED_UNVERIFIED')",
      "seal_state IN ('DRAFT','SEALED')",
      "ABS(target_pos_x) < 1.0e300",
      "ABS(target_pos_y) < 1.0e300",
      "ABS(target_pos_z) < 1.0e300",
      "ABS(target_yaw) < 1.0e30",
      "ABS(target_pitch) < 1.0e30",
      "center_chunk_x=FLOOR(target_pos_x/16)",
      "center_chunk_z=FLOOR(target_pos_z/16)",
      "center_chunk_x BETWEEN -2147483646 AND 2147483645",
      "center_chunk_z BETWEEN -2147483646 AND 2147483645",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))",
      "origin_version>=0",
      "OCTET_LENGTH(target_world_name)>0",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(),
    statementSha256 = "5bf7c02d0fcc7393738bdfe1c9e6cb83838192222778e9766220730b4e1818a9"
  )
  private def table09: Table = Table(
    name = "physical_recovery_footprint_chunk",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("dimension_id","INT NOT NULL"),
      Column("chunk_x","INT NOT NULL"),
      Column("chunk_z","INT NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid","dimension_id","chunk_x","chunk_z"),
    foreignKeys = Vector(
      ForeignKey("fk_chunk_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"))
    ),
    checks = Vector(
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(),
    statementSha256 = "860400919fa013ec6d4ab5445d9e50998e53cb88a9ce66e6e9c4ce62485167bd"
  )
  private def table10: Table = Table(
    name = "physical_recovery_intent",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("origin","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("phase","VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("target_footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("prepare_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("expected_owner_uuid","VARBINARY(16) NULL"),
      Column("expected_domain_version","BIGINT NOT NULL"),
      Column("predecessor_attempt_uuid","VARBINARY(16) NULL"),
      Column("predecessor_footprint_uuid","VARBINARY(16) NULL"),
      Column("source_completion_uuid","VARBINARY(16) NULL"),
      Column("fresh_absence_proof_bytes","LONGBLOB NULL"),
      Column("fresh_absence_proof_sha256","VARBINARY(32) NULL"),
      Column("source_kind","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("source_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("source_identity_sha256","VARBINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CAST(source_identity AS BINARY),256))) STORED"),
      Column("catalog_generation","VARBINARY(16) NULL"),
      Column("create_operation_uuid","VARBINARY(16) NULL"),
      Column("source_unit_uuid","VARBINARY(16) NULL"),
      Column("source_forest_uuid","VARBINARY(16) NULL"),
      Column("activation_operation_uuid","VARBINARY(16) NULL"),
      Column("committed_domain_version","BIGINT NULL"),
      Column("committed_at","BIGINT NULL"),
      Column("committed_world_name","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("committed_pos_x","DOUBLE NULL"),
      Column("committed_pos_y","DOUBLE NULL"),
      Column("committed_pos_z","DOUBLE NULL"),
      Column("committed_yaw","DOUBLE NULL"),
      Column("committed_pitch","DOUBLE NULL"),
      Column("committed_dimension_id","INT NULL"),
      Column("cleanup_operation_uuid","VARBINARY(16) NULL"),
      Column("cleanup_proof_bytes","LONGBLOB NULL"),
      Column("cleanup_proof_sha256","VARBINARY(32) NULL"),
      Column("intent_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","pod_uuid","attempt_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_intent_target",Vector("binding_uuid","pod_uuid","attempt_uuid","target_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_intent_predecessor",Vector("binding_uuid","pod_uuid","predecessor_attempt_uuid","predecessor_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_intent_prepare",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_intent_activation",Vector("binding_uuid","activation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_intent_cleanup",Vector("binding_uuid","cleanup_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_intent_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_intent_completion",Vector("binding_uuid","source_completion_uuid"),"fp_lifecycle_completion",Vector("binding_uuid","receipt_uuid")),
      ForeignKey("fk_intent_source_forest",Vector("binding_uuid","source_unit_uuid","source_forest_uuid"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid"))
    ),
    checks = Vector(
      "origin IN ('UNBOUND_ADOPTION','DEBUG_ITEM_CREATE','OWNED_RELEASE','REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP','REARM_AFTER_FRESH_ABSENCE')",
      "phase IN ('PREPARED','ACTIVATED','CLEANUP_PENDING','CLEANUP_VERIFIED')",
      "((predecessor_attempt_uuid IS NULL AND predecessor_footprint_uuid IS NULL) OR (predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL))",
      "((source_unit_uuid IS NULL AND source_forest_uuid IS NULL) OR (source_unit_uuid IS NOT NULL AND source_forest_uuid IS NOT NULL))",
      "((fresh_absence_proof_bytes IS NULL AND fresh_absence_proof_sha256 IS NULL) OR (fresh_absence_proof_bytes IS NOT NULL AND fresh_absence_proof_sha256 IS NOT NULL))",
      "((cleanup_proof_bytes IS NULL AND cleanup_proof_sha256 IS NULL) OR (cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL))",
      "origin NOT IN ('UNBOUND_ADOPTION','DEBUG_ITEM_CREATE') OR expected_domain_version=0",
      "origin<>'UNBOUND_ADOPTION' OR (source_kind IS NOT NULL AND source_kind IN ('FRESH','LEGACY_UNREGISTERED') AND source_identity IS NOT NULL AND catalog_generation IS NOT NULL AND create_operation_uuid IS NULL)",
      "origin<>'DEBUG_ITEM_CREATE' OR (source_kind IS NULL AND source_identity IS NULL AND catalog_generation IS NULL AND create_operation_uuid IS NOT NULL)",
      "origin NOT IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP') OR (source_completion_uuid IS NOT NULL AND predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL AND fresh_absence_proof_bytes IS NULL)",
      "origin<>'REARM_AFTER_FRESH_ABSENCE' OR (fresh_absence_proof_bytes IS NOT NULL AND fresh_absence_proof_sha256 IS NOT NULL AND source_completion_uuid IS NULL AND predecessor_attempt_uuid IS NOT NULL AND predecessor_footprint_uuid IS NOT NULL)",
      "origin<>'OWNED_RELEASE' OR expected_owner_uuid IS NOT NULL",
      "(phase<>'ACTIVATED' AND activation_operation_uuid IS NULL AND committed_domain_version IS NULL AND committed_at IS NULL AND committed_world_name IS NULL AND committed_pos_x IS NULL AND committed_pos_y IS NULL AND committed_pos_z IS NULL AND committed_yaw IS NULL AND committed_pitch IS NULL AND committed_dimension_id IS NULL) OR (phase='ACTIVATED' AND activation_operation_uuid IS NOT NULL AND committed_domain_version IS NOT NULL AND committed_domain_version=expected_domain_version+1 AND committed_at IS NOT NULL AND committed_world_name IS NOT NULL AND committed_pos_x IS NOT NULL AND committed_pos_y IS NOT NULL AND committed_pos_z IS NOT NULL AND committed_yaw IS NOT NULL AND committed_pitch IS NOT NULL AND committed_dimension_id IS NOT NULL)",
      "phase<>'CLEANUP_VERIFIED' OR (cleanup_operation_uuid IS NOT NULL AND cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL)",
      "committed_pos_x IS NULL OR ABS(committed_pos_x) < 1.0e300",
      "committed_pos_y IS NULL OR ABS(committed_pos_y) < 1.0e300",
      "committed_pos_z IS NULL OR ABS(committed_pos_z) < 1.0e300",
      "committed_yaw IS NULL OR ABS(committed_yaw) < 1.0e30",
      "committed_pitch IS NULL OR ABS(committed_pitch) < 1.0e30",
      "origin IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP') OR source_completion_uuid IS NULL",
      "origin='REARM_AFTER_FRESH_ABSENCE' OR fresh_absence_proof_bytes IS NULL",
      "origin IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP','REARM_AFTER_FRESH_ABSENCE') OR predecessor_attempt_uuid IS NULL",
      "origin='UNBOUND_ADOPTION' OR (source_kind IS NULL AND source_identity IS NULL AND catalog_generation IS NULL)",
      "origin='DEBUG_ITEM_CREATE' OR create_operation_uuid IS NULL",
      "phase='CLEANUP_VERIFIED' OR cleanup_proof_bytes IS NULL",
      "phase IN ('CLEANUP_PENDING','CLEANUP_VERIFIED') OR cleanup_operation_uuid IS NULL",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(target_footprint_uuid)=16 AND target_footprint_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))",
      "expected_owner_uuid IS NULL OR (OCTET_LENGTH(expected_owner_uuid)=16 AND expected_owner_uuid<>UNHEX(REPEAT('00',16)))",
      "expected_domain_version>=0",
      "predecessor_attempt_uuid IS NULL OR (OCTET_LENGTH(predecessor_attempt_uuid)=16 AND predecessor_attempt_uuid<>UNHEX(REPEAT('00',16)))",
      "predecessor_footprint_uuid IS NULL OR (OCTET_LENGTH(predecessor_footprint_uuid)=16 AND predecessor_footprint_uuid<>UNHEX(REPEAT('00',16)))",
      "source_completion_uuid IS NULL OR (OCTET_LENGTH(source_completion_uuid)=16 AND source_completion_uuid<>UNHEX(REPEAT('00',16)))",
      "fresh_absence_proof_sha256 IS NULL OR OCTET_LENGTH(fresh_absence_proof_sha256)=32",
      "catalog_generation IS NULL OR (OCTET_LENGTH(catalog_generation)=16 AND catalog_generation<>UNHEX(REPEAT('00',16)))",
      "create_operation_uuid IS NULL OR (OCTET_LENGTH(create_operation_uuid)=16 AND create_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "source_unit_uuid IS NULL OR (OCTET_LENGTH(source_unit_uuid)=16 AND source_unit_uuid<>UNHEX(REPEAT('00',16)))",
      "source_forest_uuid IS NULL OR (OCTET_LENGTH(source_forest_uuid)=16 AND source_forest_uuid<>UNHEX(REPEAT('00',16)))",
      "activation_operation_uuid IS NULL OR (OCTET_LENGTH(activation_operation_uuid)=16 AND activation_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "committed_domain_version IS NULL OR committed_domain_version>=0",
      "cleanup_operation_uuid IS NULL OR (OCTET_LENGTH(cleanup_operation_uuid)=16 AND cleanup_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "cleanup_proof_sha256 IS NULL OR OCTET_LENGTH(cleanup_proof_sha256)=32",
      "intent_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "fresh_absence_proof_bytes IS NULL OR (fresh_absence_proof_sha256 IS NOT NULL AND fresh_absence_proof_sha256=UNHEX(SHA2(fresh_absence_proof_bytes,256)))",
      "cleanup_proof_bytes IS NULL OR (cleanup_proof_sha256 IS NOT NULL AND cleanup_proof_sha256=UNHEX(SHA2(cleanup_proof_bytes,256)))"
    ),
    indexes = Vector(
      Index("ux_intent_source",Vector("binding_uuid","source_kind","catalog_generation","source_identity_sha256"),true),
      Index("ux_intent_create",Vector("binding_uuid","create_operation_uuid"),true),
      Index("ux_intent_completion",Vector("binding_uuid","source_completion_uuid"),true)
    ),
    statementSha256 = "609f1bf94ba769c1c24edef403e9e559a78c013b5ca464751bcb404b803856b2"
  )
  private def table11: Table = Table(
    name = "physical_recovery_locator",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("current_attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("current_footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("locator_state","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("reason_code","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("domain_version","BIGINT NULL"),
      Column("target_dimension_id","INT NOT NULL"),
      Column("locator_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","pod_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_locator_target",Vector("binding_uuid","pod_uuid","current_attempt_uuid","current_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_locator_intent",Vector("binding_uuid","pod_uuid","current_attempt_uuid"),"physical_recovery_intent",Vector("binding_uuid","pod_uuid","attempt_uuid")),
      ForeignKey("fk_locator_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "locator_state IN ('CREATE_PREPARED','UNBOUND_ADOPTION_PREPARED','DEBUG_ITEM_CREATE_PREPARED','ROW_ACTIVE','ROW_INACTIVE_PENDING','ROW_INACTIVE_VERIFIED','ABSENT_PENDING','ABSENT_VERIFIED','LEGACY_UNCERTIFIED','QUARANTINED')",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(current_attempt_uuid)=16 AND current_attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(current_footprint_uuid)=16 AND current_footprint_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(reason_code)>0",
      "domain_version IS NULL OR domain_version>=0",
      "locator_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))"
    ),
    indexes = Vector(
      Index("ux_locator_attempt",Vector("binding_uuid","current_attempt_uuid"),true)
    ),
    statementSha256 = "7aaa08c6f1fe6f601f1597ca564156c76a9b15d79e87222b72997e69e9b9d1b8"
  )
  private def table12: Table = Table(
    name = "physical_recovery_obligation",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("origin_version","BIGINT NOT NULL"),
      Column("purpose","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_retirement_operation_uuid","VARBINARY(16) NULL"),
      Column("obligation_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("obligation_revision","BIGINT NOT NULL"),
      Column("verified_operation_uuid","VARBINARY(16) NULL"),
      Column("verification_proof_bytes","LONGBLOB NULL"),
      Column("verification_proof_sha256","VARBINARY(32) NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_obligation_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_obligation_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_obligation_retirement",Vector("binding_uuid","attempt_retirement_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_obligation_verified",Vector("binding_uuid","verified_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_obligation_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "purpose IN ('RETIRED_ATTEMPT','ABORTED_CREATE','SAME_ATTEMPT_CHECKPOINT')",
      "obligation_state IN ('PENDING','VERIFIED')",
      "((verification_proof_bytes IS NULL AND verification_proof_sha256 IS NULL) OR (verification_proof_bytes IS NOT NULL AND verification_proof_sha256 IS NOT NULL))",
      "(obligation_state='PENDING' AND verified_operation_uuid IS NULL AND verification_proof_bytes IS NULL) OR (obligation_state='VERIFIED' AND verified_operation_uuid IS NOT NULL AND verification_proof_bytes IS NOT NULL)",
      "purpose<>'RETIRED_ATTEMPT' OR attempt_retirement_operation_uuid IS NOT NULL",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))",
      "origin_version>=0",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))",
      "attempt_retirement_operation_uuid IS NULL OR (OCTET_LENGTH(attempt_retirement_operation_uuid)=16 AND attempt_retirement_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "obligation_revision>=0",
      "verified_operation_uuid IS NULL OR (OCTET_LENGTH(verified_operation_uuid)=16 AND verified_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "verification_proof_sha256 IS NULL OR OCTET_LENGTH(verification_proof_sha256)=32",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "verification_proof_bytes IS NULL OR (verification_proof_sha256 IS NOT NULL AND verification_proof_sha256=UNHEX(SHA2(verification_proof_bytes,256)))"
    ),
    indexes = Vector(),
    statementSha256 = "b33c00cb5860ae56a2b46345ca6fdc41cc88b89d9dfb8ff62323e1a1e39cd5e8"
  )
  private def table13: Table = Table(
    name = "fp_lifecycle_completion",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("receipt_uuid","VARBINARY(16) NOT NULL"),
      Column("completion_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("completion_kind","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("boot_uuid","VARBINARY(16) NOT NULL"),
      Column("boot_generation","BIGINT NOT NULL"),
      Column("world_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("source_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("owner_uuid","VARBINARY(16) NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("before_domain_version","BIGINT NOT NULL"),
      Column("after_domain_version","BIGINT NOT NULL"),
      Column("removal_result_bytes","LONGBLOB NOT NULL"),
      Column("removal_result_sha256","VARBINARY(32) NOT NULL"),
      Column("ticket_result_bytes","LONGBLOB NOT NULL"),
      Column("ticket_result_sha256","VARBINARY(32) NOT NULL"),
      Column("checkpoint_bytes","LONGBLOB NOT NULL"),
      Column("checkpoint_sha256","VARBINARY(32) NOT NULL"),
      Column("original_context_bytes","LONGBLOB NOT NULL"),
      Column("original_context_sha256","VARBINARY(32) NOT NULL"),
      Column("allowed_use","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("completion_state","VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("consumed_by_operation_uuid","VARBINARY(16) NULL"),
      Column("completion_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","receipt_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_completion_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_completion_operation",Vector("binding_uuid","completion_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_completion_consumed",Vector("binding_uuid","consumed_by_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_completion_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "completion_kind IN ('RECALL','ABORTED_CREATE_CLEANUP')",
      "allowed_use IN ('REARM_AFTER_RECALL','REARM_AFTER_ABORT_CLEANUP')",
      "completion_state='CHECKPOINT_COMMITTED'",
      "after_domain_version=before_domain_version+1",
      "boot_generation>0",
      "(completion_kind='RECALL' AND allowed_use='REARM_AFTER_RECALL') OR (completion_kind='ABORTED_CREATE_CLEANUP' AND allowed_use='REARM_AFTER_ABORT_CLEANUP')",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(receipt_uuid)=16 AND receipt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(completion_operation_uuid)=16 AND completion_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(boot_uuid)=16 AND boot_uuid<>UNHEX(REPEAT('00',16))",
      "boot_generation>=0",
      "OCTET_LENGTH(world_identity)>0",
      "OCTET_LENGTH(source_identity)>0",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "owner_uuid IS NULL OR (OCTET_LENGTH(owner_uuid)=16 AND owner_uuid<>UNHEX(REPEAT('00',16)))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(footprint_uuid)=16 AND footprint_uuid<>UNHEX(REPEAT('00',16))",
      "before_domain_version>=0",
      "after_domain_version>=0",
      "OCTET_LENGTH(removal_result_bytes)>0",
      "OCTET_LENGTH(removal_result_sha256)=32",
      "OCTET_LENGTH(ticket_result_bytes)>0",
      "OCTET_LENGTH(ticket_result_sha256)=32",
      "OCTET_LENGTH(checkpoint_bytes)>0",
      "OCTET_LENGTH(checkpoint_sha256)=32",
      "OCTET_LENGTH(original_context_bytes)>0",
      "OCTET_LENGTH(original_context_sha256)=32",
      "consumed_by_operation_uuid IS NULL OR (OCTET_LENGTH(consumed_by_operation_uuid)=16 AND consumed_by_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "completion_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "removal_result_sha256=UNHEX(SHA2(removal_result_bytes,256))",
      "ticket_result_sha256=UNHEX(SHA2(ticket_result_bytes,256))",
      "checkpoint_sha256=UNHEX(SHA2(checkpoint_bytes,256))",
      "original_context_sha256=UNHEX(SHA2(original_context_bytes,256))"
    ),
    indexes = Vector(
      Index("ux_completion_operation",Vector("binding_uuid","completion_operation_uuid"),true)
    ),
    statementSha256 = "466068f8023023d941907a662138f4871b86dd2bf4547b4fb4f5f958c6c04efd"
  )
  private def table14: Table = Table(
    name = "physical_footprint_transfer",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("move_uuid","VARBINARY(16) NOT NULL"),
      Column("pod_uuid","VARBINARY(16) NOT NULL"),
      Column("attempt_uuid","VARBINARY(16) NOT NULL"),
      Column("from_footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("to_footprint_uuid","VARBINARY(16) NOT NULL"),
      Column("expected_domain_version","BIGINT NOT NULL"),
      Column("target_domain_version","BIGINT NOT NULL"),
      Column("from_world_name","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("from_pos_x","DOUBLE NOT NULL"),
      Column("from_pos_y","DOUBLE NOT NULL"),
      Column("from_pos_z","DOUBLE NOT NULL"),
      Column("from_yaw","DOUBLE NOT NULL"),
      Column("from_pitch","DOUBLE NOT NULL"),
      Column("from_dimension_id","INT NOT NULL"),
      Column("to_world_name","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("to_pos_x","DOUBLE NOT NULL"),
      Column("to_pos_y","DOUBLE NOT NULL"),
      Column("to_pos_z","DOUBLE NOT NULL"),
      Column("to_yaw","DOUBLE NOT NULL"),
      Column("to_pitch","DOUBLE NOT NULL"),
      Column("to_dimension_id","INT NOT NULL"),
      Column("prepare_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("commit_operation_uuid","VARBINARY(16) NULL"),
      Column("cleanup_operation_uuid","VARBINARY(16) NULL"),
      Column("phase","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("move_plan_bytes","LONGBLOB NOT NULL"),
      Column("move_plan_sha256","VARBINARY(32) NOT NULL"),
      Column("cleanup_proof_bytes","LONGBLOB NULL"),
      Column("cleanup_proof_sha256","VARBINARY(32) NULL"),
      Column("transfer_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","move_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_move_from",Vector("binding_uuid","pod_uuid","attempt_uuid","from_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_move_to",Vector("binding_uuid","pod_uuid","attempt_uuid","to_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
      ForeignKey("fk_move_prepare",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_move_commit",Vector("binding_uuid","commit_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_move_cleanup",Vector("binding_uuid","cleanup_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_move_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "from_footprint_uuid<>to_footprint_uuid",
      "target_domain_version=expected_domain_version+1",
      "from_dimension_id=to_dimension_id",
      "phase IN ('MOVE_PREPARED','MOVE_COMMITTED_CLEANUP_PENDING','MOVE_COMPLETED','MOVE_REMEDIATION_PENDING')",
      "((cleanup_proof_bytes IS NULL AND cleanup_proof_sha256 IS NULL) OR (cleanup_proof_bytes IS NOT NULL AND cleanup_proof_sha256 IS NOT NULL))",
      "phase NOT IN ('MOVE_COMMITTED_CLEANUP_PENDING','MOVE_COMPLETED') OR commit_operation_uuid IS NOT NULL",
      "phase<>'MOVE_COMPLETED' OR (cleanup_operation_uuid IS NOT NULL AND cleanup_proof_bytes IS NOT NULL)",
      "ABS(from_pos_x) < 1.0e300",
      "ABS(from_pos_y) < 1.0e300",
      "ABS(from_pos_z) < 1.0e300",
      "ABS(from_yaw) < 1.0e30",
      "ABS(from_pitch) < 1.0e30",
      "ABS(to_pos_x) < 1.0e300",
      "ABS(to_pos_y) < 1.0e300",
      "ABS(to_pos_z) < 1.0e300",
      "ABS(to_yaw) < 1.0e30",
      "ABS(to_pitch) < 1.0e30",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(move_uuid)=16 AND move_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(pod_uuid)=16 AND pod_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(attempt_uuid)=16 AND attempt_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(from_footprint_uuid)=16 AND from_footprint_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(to_footprint_uuid)=16 AND to_footprint_uuid<>UNHEX(REPEAT('00',16))",
      "expected_domain_version>=0",
      "target_domain_version>=0",
      "OCTET_LENGTH(from_world_name)>0",
      "OCTET_LENGTH(to_world_name)>0",
      "OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))",
      "commit_operation_uuid IS NULL OR (OCTET_LENGTH(commit_operation_uuid)=16 AND commit_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "cleanup_operation_uuid IS NULL OR (OCTET_LENGTH(cleanup_operation_uuid)=16 AND cleanup_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "OCTET_LENGTH(move_plan_bytes)>0",
      "OCTET_LENGTH(move_plan_sha256)=32",
      "cleanup_proof_sha256 IS NULL OR OCTET_LENGTH(cleanup_proof_sha256)=32",
      "transfer_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "move_plan_sha256=UNHEX(SHA2(move_plan_bytes,256))",
      "cleanup_proof_bytes IS NULL OR (cleanup_proof_sha256 IS NOT NULL AND cleanup_proof_sha256=UNHEX(SHA2(cleanup_proof_bytes,256)))"
    ),
    indexes = Vector(),
    statementSha256 = "98a64f9ea4a590501d40931a6773455072019584465c9a4c210038aa7adaceee"
  )
  private def table15: Table = Table(
    name = "physical_source_unit",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("unit_uuid","VARBINARY(16) NOT NULL"),
      Column("world_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("save_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("loader_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("catalog_generation","VARBINARY(16) NOT NULL"),
      Column("read_generation","BIGINT NOT NULL"),
      Column("source_observation_bytes","LONGBLOB NOT NULL"),
      Column("source_observation_sha256","VARBINARY(32) NOT NULL"),
      Column("current_epoch","BIGINT NULL"),
      Column("current_forest_uuid","VARBINARY(16) NULL"),
      Column("unit_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("unit_revision","BIGINT NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","unit_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_unit_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_unit_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_unit_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_unit_current_forest",Vector("binding_uuid","unit_uuid","current_forest_uuid","current_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch"))
    ),
    checks = Vector(
      "unit_state IN ('DRAFT','SEALED')",
      "((current_epoch IS NULL AND current_forest_uuid IS NULL) OR (current_epoch IS NOT NULL AND current_forest_uuid IS NOT NULL))",
      "(unit_state='DRAFT' AND current_epoch IS NULL) OR (unit_state='SEALED' AND current_epoch IS NOT NULL AND current_epoch>=0)",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(world_identity)>0",
      "OCTET_LENGTH(save_identity)>0",
      "OCTET_LENGTH(loader_identity)>0",
      "OCTET_LENGTH(catalog_generation)=16 AND catalog_generation<>UNHEX(REPEAT('00',16))",
      "read_generation>=0",
      "OCTET_LENGTH(source_observation_bytes)>0",
      "OCTET_LENGTH(source_observation_sha256)=32",
      "current_epoch IS NULL OR current_epoch>=0",
      "current_forest_uuid IS NULL OR (OCTET_LENGTH(current_forest_uuid)=16 AND current_forest_uuid<>UNHEX(REPEAT('00',16)))",
      "unit_revision>=0",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "source_observation_sha256=UNHEX(SHA2(source_observation_bytes,256))"
    ),
    indexes = Vector(),
    statementSha256 = "751c03eb8674166d3c097524edc1843778bbcd70a0a1d30971454fa580052a4a"
  )
  private def table16: Table = Table(
    name = "physical_source_forest",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("unit_uuid","VARBINARY(16) NOT NULL"),
      Column("forest_uuid","VARBINARY(16) NOT NULL"),
      Column("source_epoch","BIGINT NOT NULL"),
      Column("forest_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("forest_codec","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("forest_bytes","LONGBLOB NOT NULL"),
      Column("forest_sha256","VARBINARY(32) NOT NULL"),
      Column("member_count","BIGINT NOT NULL"),
      Column("root_count","BIGINT NOT NULL"),
      Column("carrier_count","BIGINT NOT NULL"),
      Column("retained_bytes","BIGINT NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("fp_pod_uuid_index_bytes","LONGBLOB NULL"),
      Column("fp_pod_uuid_index_sha256","VARBINARY(32) NULL")
    ),
    primaryKey = Vector("binding_uuid","unit_uuid","forest_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_forest_unit",Vector("binding_uuid","unit_uuid"),"physical_source_unit",Vector("binding_uuid","unit_uuid")),
      ForeignKey("fk_forest_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "source_epoch>=0",
      "forest_state IN ('DRAFT','SEALED')",
      "forest_codec='FP_SOURCE_FOREST_BE_V2'",
      "member_count>0",
      "root_count>0",
      "root_count<=member_count",
      "carrier_count>0",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16))",
      "source_epoch>=0",
      "OCTET_LENGTH(forest_bytes)>0",
      "OCTET_LENGTH(forest_sha256)=32",
      "member_count>=0",
      "root_count>=0",
      "carrier_count>=0",
      "retained_bytes>=0",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))",
      "forest_sha256=UNHEX(SHA2(forest_bytes,256))",
      "(fp_pod_uuid_index_bytes IS NULL AND fp_pod_uuid_index_sha256 IS NULL) OR (fp_pod_uuid_index_bytes IS NOT NULL AND fp_pod_uuid_index_sha256 IS NOT NULL)",
      "fp_pod_uuid_index_sha256 IS NULL OR (OCTET_LENGTH(fp_pod_uuid_index_sha256)=32 AND fp_pod_uuid_index_sha256=UNHEX(SHA2(fp_pod_uuid_index_bytes,256)))",
      "forest_state<>'SEALED' OR (fp_pod_uuid_index_bytes IS NOT NULL AND fp_pod_uuid_index_sha256 IS NOT NULL)"
    ),
    indexes = Vector(
      Index("ux_forest_epoch_key",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch"),true),
      Index("ux_forest_epoch",Vector("binding_uuid","unit_uuid","source_epoch"),true)
    ),
    statementSha256 = "1dacbd937d19ab62ecda359a5ead0c901d30e0f50f08024b1e2f34c0aab42071"
  )
  private def table17: Table = Table(
    name = "physical_source_transfer",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("transfer_uuid","VARBINARY(16) NOT NULL"),
      Column("unit_uuid","VARBINARY(16) NOT NULL"),
      Column("old_epoch","BIGINT NOT NULL"),
      Column("old_forest_uuid","VARBINARY(16) NOT NULL"),
      Column("target_epoch","BIGINT NOT NULL"),
      Column("target_forest_uuid","VARBINARY(16) NOT NULL"),
      Column("transfer_plan_bytes","LONGBLOB NOT NULL"),
      Column("transfer_plan_sha256","VARBINARY(32) NOT NULL"),
      Column("prepare_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("cutover_operation_uuid","VARBINARY(16) NULL"),
      Column("retire_operation_uuid","VARBINARY(16) NULL"),
      Column("phase","VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("target_receipts_bytes","LONGBLOB NULL"),
      Column("target_receipts_sha256","VARBINARY(32) NULL"),
      Column("retirement_proof_bytes","LONGBLOB NULL"),
      Column("retirement_proof_sha256","VARBINARY(32) NULL"),
      Column("transfer_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","transfer_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_source_transfer_old",Vector("binding_uuid","unit_uuid","old_forest_uuid","old_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch")),
      ForeignKey("fk_source_transfer_target",Vector("binding_uuid","unit_uuid","target_forest_uuid","target_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch")),
      ForeignKey("fk_source_transfer_prepare",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_source_transfer_cutover",Vector("binding_uuid","cutover_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_source_transfer_retire",Vector("binding_uuid","retire_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_source_transfer_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "old_epoch>=0",
      "target_epoch=old_epoch+1",
      "old_forest_uuid<>target_forest_uuid",
      "phase IN ('SOURCE_TRANSFER_PREPARED','TARGETS_DURABLE','CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED','REMEDIATION_REQUIRED')",
      "((target_receipts_bytes IS NULL AND target_receipts_sha256 IS NULL) OR (target_receipts_bytes IS NOT NULL AND target_receipts_sha256 IS NOT NULL))",
      "((retirement_proof_bytes IS NULL AND retirement_proof_sha256 IS NULL) OR (retirement_proof_bytes IS NOT NULL AND retirement_proof_sha256 IS NOT NULL))",
      "phase NOT IN ('TARGETS_DURABLE','CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR target_receipts_bytes IS NOT NULL",
      "phase NOT IN ('CUTOVER_COMMITTED_RETIRE_PENDING','COMPLETED') OR cutover_operation_uuid IS NOT NULL",
      "phase<>'COMPLETED' OR (retire_operation_uuid IS NOT NULL AND retirement_proof_bytes IS NOT NULL)",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(transfer_uuid)=16 AND transfer_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))",
      "old_epoch>=0",
      "OCTET_LENGTH(old_forest_uuid)=16 AND old_forest_uuid<>UNHEX(REPEAT('00',16))",
      "target_epoch>=0",
      "OCTET_LENGTH(target_forest_uuid)=16 AND target_forest_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(transfer_plan_bytes)>0",
      "OCTET_LENGTH(transfer_plan_sha256)=32",
      "OCTET_LENGTH(prepare_operation_uuid)=16 AND prepare_operation_uuid<>UNHEX(REPEAT('00',16))",
      "cutover_operation_uuid IS NULL OR (OCTET_LENGTH(cutover_operation_uuid)=16 AND cutover_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "retire_operation_uuid IS NULL OR (OCTET_LENGTH(retire_operation_uuid)=16 AND retire_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "target_receipts_sha256 IS NULL OR OCTET_LENGTH(target_receipts_sha256)=32",
      "retirement_proof_sha256 IS NULL OR OCTET_LENGTH(retirement_proof_sha256)=32",
      "transfer_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "transfer_plan_sha256=UNHEX(SHA2(transfer_plan_bytes,256))",
      "target_receipts_bytes IS NULL OR (target_receipts_sha256 IS NOT NULL AND target_receipts_sha256=UNHEX(SHA2(target_receipts_bytes,256)))",
      "retirement_proof_bytes IS NULL OR (retirement_proof_sha256 IS NOT NULL AND retirement_proof_sha256=UNHEX(SHA2(retirement_proof_bytes,256)))"
    ),
    indexes = Vector(),
    statementSha256 = "2d0980fe2bac947f9a8597d4584925b18214f7a961e0b5c588cf417a3058a393"
  )
  private def table18: Table = Table(
    name = "physical_source_write",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("write_uuid","VARBINARY(16) NOT NULL"),
      Column("workflow_uuid","VARBINARY(16) NOT NULL"),
      Column("resource_kind","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("carrier_key_hash","VARBINARY(32) NOT NULL"),
      Column("carrier_key","LONGBLOB NOT NULL"),
      Column("save_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("loader_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("region_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("slot_x","INT NOT NULL"),
      Column("slot_z","INT NOT NULL"),
      Column("write_generation","BIGINT NOT NULL"),
      Column("payload_descriptor_bytes","LONGBLOB NOT NULL"),
      Column("payload_descriptor_sha256","VARBINARY(32) NOT NULL"),
      Column("payload_length","BIGINT NOT NULL"),
      Column("payload_sha256","VARBINARY(32) NOT NULL"),
      Column("payload_artifact_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("undo_kind","VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("undo_descriptor_bytes","LONGBLOB NOT NULL"),
      Column("undo_descriptor_sha256","VARBINARY(32) NOT NULL"),
      Column("undo_length","BIGINT NOT NULL"),
      Column("undo_content_sha256","VARBINARY(32) NULL"),
      Column("undo_artifact_identity","LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("member_coverage_bytes","LONGBLOB NOT NULL"),
      Column("member_coverage_sha256","VARBINARY(32) NOT NULL"),
      Column("obligation_revisions_bytes","LONGBLOB NOT NULL"),
      Column("obligation_revisions_sha256","VARBINARY(32) NOT NULL"),
      Column("limit_profile_sha256","VARBINARY(32) NOT NULL"),
      Column("receipt_codec","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("prepared_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("write_state","VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("resolved_operation_uuid","VARBINARY(16) NULL"),
      Column("durable_receipt_bytes","LONGBLOB NULL"),
      Column("durable_receipt_sha256","VARBINARY(32) NULL"),
      Column("failure_diagnostic_identity","LONGBLOB NULL"),
      Column("failure_diagnostic_sha256","VARBINARY(32) NULL"),
      Column("write_revision","BIGINT NOT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","write_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_write_carrier",Vector("binding_uuid","resource_kind","carrier_key_hash"),"fp_recovery_reservation",Vector("binding_uuid","resource_kind","resource_key_hash")),
      ForeignKey("fk_write_prepared",Vector("binding_uuid","prepared_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_write_resolved",Vector("binding_uuid","resolved_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_write_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "resource_kind='CARRIER'",
      "carrier_key_hash=UNHEX(SHA2(carrier_key,256))",
      "write_generation>=0",
      "payload_length>0",
      "undo_kind IN ('FULL_CHUNK','SLOT_ABSENT')",
      "(undo_kind='FULL_CHUNK' AND undo_length>0 AND undo_content_sha256 IS NOT NULL) OR (undo_kind='SLOT_ABSENT' AND undo_length=0 AND undo_content_sha256 IS NULL)",
      "receipt_codec='FP_SOURCE_WRITE_RECEIPT_BE_V1'",
      "write_state IN ('PREPARED','DURABLE','NO_PUT','SUPERSEDED','FAILED_NEEDS_RECONCILIATION')",
      "((durable_receipt_bytes IS NULL AND durable_receipt_sha256 IS NULL) OR (durable_receipt_bytes IS NOT NULL AND durable_receipt_sha256 IS NOT NULL))",
      "((failure_diagnostic_identity IS NULL AND failure_diagnostic_sha256 IS NULL) OR (failure_diagnostic_identity IS NOT NULL AND failure_diagnostic_sha256 IS NOT NULL))",
      "(write_state='PREPARED' AND resolved_operation_uuid IS NULL AND durable_receipt_bytes IS NULL AND failure_diagnostic_identity IS NULL) OR (write_state='DURABLE' AND resolved_operation_uuid IS NOT NULL AND durable_receipt_bytes IS NOT NULL) OR (write_state IN ('NO_PUT','SUPERSEDED','FAILED_NEEDS_RECONCILIATION') AND resolved_operation_uuid IS NOT NULL AND durable_receipt_bytes IS NULL)",
      "write_state<>'FAILED_NEEDS_RECONCILIATION' OR failure_diagnostic_identity IS NOT NULL",
      "failure_diagnostic_identity IS NULL OR failure_diagnostic_sha256=UNHEX(SHA2(failure_diagnostic_identity,256))",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(write_uuid)=16 AND write_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(workflow_uuid)=16 AND workflow_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(carrier_key_hash)=32",
      "OCTET_LENGTH(carrier_key)>0",
      "OCTET_LENGTH(save_identity)>0",
      "OCTET_LENGTH(loader_identity)>0",
      "OCTET_LENGTH(region_identity)>0",
      "write_generation>=0",
      "OCTET_LENGTH(payload_descriptor_bytes)>0",
      "OCTET_LENGTH(payload_descriptor_sha256)=32",
      "payload_length>=0",
      "OCTET_LENGTH(payload_sha256)=32",
      "OCTET_LENGTH(payload_artifact_identity)>0",
      "OCTET_LENGTH(undo_descriptor_bytes)>0",
      "OCTET_LENGTH(undo_descriptor_sha256)=32",
      "undo_length>=0",
      "undo_content_sha256 IS NULL OR OCTET_LENGTH(undo_content_sha256)=32",
      "OCTET_LENGTH(undo_artifact_identity)>0",
      "OCTET_LENGTH(member_coverage_bytes)>0",
      "OCTET_LENGTH(member_coverage_sha256)=32",
      "OCTET_LENGTH(obligation_revisions_bytes)>0",
      "OCTET_LENGTH(obligation_revisions_sha256)=32",
      "OCTET_LENGTH(limit_profile_sha256)=32",
      "OCTET_LENGTH(prepared_operation_uuid)=16 AND prepared_operation_uuid<>UNHEX(REPEAT('00',16))",
      "resolved_operation_uuid IS NULL OR (OCTET_LENGTH(resolved_operation_uuid)=16 AND resolved_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "durable_receipt_sha256 IS NULL OR OCTET_LENGTH(durable_receipt_sha256)=32",
      "failure_diagnostic_sha256 IS NULL OR OCTET_LENGTH(failure_diagnostic_sha256)=32",
      "write_revision>=0",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "payload_descriptor_sha256=UNHEX(SHA2(payload_descriptor_bytes,256))",
      "undo_descriptor_sha256=UNHEX(SHA2(undo_descriptor_bytes,256))",
      "member_coverage_sha256=UNHEX(SHA2(member_coverage_bytes,256))",
      "obligation_revisions_sha256=UNHEX(SHA2(obligation_revisions_bytes,256))",
      "durable_receipt_bytes IS NULL OR (durable_receipt_sha256 IS NOT NULL AND durable_receipt_sha256=UNHEX(SHA2(durable_receipt_bytes,256)))"
    ),
    indexes = Vector(
      Index("ux_write_generation",Vector("binding_uuid","carrier_key_hash","write_generation"),true)
    ),
    statementSha256 = "83245bc8e49a1799ef463091808a05ea8962e215f8a4cf90b0cdf53988de6540"
  )
  private def table19: Table = Table(
    name = "physical_source_obligation",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("unit_uuid","VARBINARY(16) NOT NULL"),
      Column("forest_uuid","VARBINARY(16) NOT NULL"),
      Column("resource_kind","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("carrier_key_hash","VARBINARY(32) NOT NULL"),
      Column("carrier_key","LONGBLOB NOT NULL"),
      Column("purpose","VARCHAR(24) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("obligation_state","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("obligation_revision","BIGINT NOT NULL"),
      Column("created_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("write_uuid","VARBINARY(16) NULL"),
      Column("discharged_operation_uuid","VARBINARY(16) NULL"),
      Column("discharge_kind","VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("discharge_proof_bytes","LONGBLOB NULL"),
      Column("discharge_proof_sha256","VARBINARY(32) NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","unit_uuid","forest_uuid","carrier_key_hash","purpose"),
    foreignKeys = Vector(
      ForeignKey("fk_source_obligation_forest",Vector("binding_uuid","unit_uuid","forest_uuid"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid")),
      ForeignKey("fk_source_obligation_carrier",Vector("binding_uuid","resource_kind","carrier_key_hash"),"fp_recovery_reservation",Vector("binding_uuid","resource_kind","resource_key_hash")),
      ForeignKey("fk_source_obligation_write",Vector("binding_uuid","write_uuid"),"physical_source_write",Vector("binding_uuid","write_uuid")),
      ForeignKey("fk_source_obligation_created",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_source_obligation_discharged",Vector("binding_uuid","discharged_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_source_obligation_mutation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "resource_kind='CARRIER'",
      "carrier_key_hash=UNHEX(SHA2(carrier_key,256))",
      "purpose IN ('OLD_HOLD','TARGET_STAGED','CURRENT_HOLD','OLD_RETIRE','UNDO')",
      "obligation_state IN ('OPEN','DISCHARGED')",
      "((discharge_proof_bytes IS NULL AND discharge_proof_sha256 IS NULL) OR (discharge_proof_bytes IS NOT NULL AND discharge_proof_sha256 IS NOT NULL))",
      "discharge_kind IS NULL OR discharge_kind IN ('CUTOVER','WORLD_RETIREMENT','SAFE_UNDO','SUCCESSOR_CUTOVER')",
      "(obligation_state='OPEN' AND discharged_operation_uuid IS NULL AND discharge_kind IS NULL AND discharge_proof_bytes IS NULL) OR (obligation_state='DISCHARGED' AND discharged_operation_uuid IS NOT NULL AND discharge_kind IS NOT NULL AND discharge_proof_bytes IS NOT NULL)",
      "purpose NOT IN ('TARGET_STAGED','UNDO','OLD_RETIRE') OR write_uuid IS NOT NULL",
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(unit_uuid)=16 AND unit_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(forest_uuid)=16 AND forest_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(carrier_key_hash)=32",
      "OCTET_LENGTH(carrier_key)>0",
      "obligation_revision>=0",
      "OCTET_LENGTH(created_operation_uuid)=16 AND created_operation_uuid<>UNHEX(REPEAT('00',16))",
      "write_uuid IS NULL OR (OCTET_LENGTH(write_uuid)=16 AND write_uuid<>UNHEX(REPEAT('00',16)))",
      "discharged_operation_uuid IS NULL OR (OCTET_LENGTH(discharged_operation_uuid)=16 AND discharged_operation_uuid<>UNHEX(REPEAT('00',16)))",
      "discharge_proof_sha256 IS NULL OR OCTET_LENGTH(discharge_proof_sha256)=32",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "discharge_proof_bytes IS NULL OR (discharge_proof_sha256 IS NOT NULL AND discharge_proof_sha256=UNHEX(SHA2(discharge_proof_bytes,256)))"
    ),
    indexes = Vector(),
    statementSha256 = "43a2680e02027cf54212e5651497538cf6769f7c3506bafffb7ee09039a254cc"
  )
  private def table20: Table = Table(
    name = "fp_script_catalog",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("catalog_revision","BIGINT NOT NULL"),
      Column("snapshot_bytes","LONGBLOB NOT NULL"),
      Column("snapshot_sha256","VARBINARY(32) NOT NULL"),
      Column("next_version","BIGINT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NULL")
    ),
    primaryKey = Vector("binding_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_sc_binding",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
      ForeignKey("fk_sc_op",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(snapshot_sha256)=32",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "catalog_revision>=0",
      "next_version IS NULL OR next_version>0",
      "snapshot_sha256=UNHEX(SHA2(snapshot_bytes,256))",
      "(catalog_revision=0 AND last_mutation_operation_uuid IS NULL) OR last_mutation_operation_uuid IS NOT NULL"
    ),
    indexes = Vector(),
    statementSha256 = "f90303ecbe31e89f1a4ab26be195bd7fabac55b341b3c1c89f0bbe9863505a90"
  )
  private def table21: Table = Table(
    name = "fp_script_operation",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("operation_uuid","VARBINARY(16) NOT NULL"),
      Column("request_kind","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("behavior_id","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("version","BIGINT NOT NULL"),
      Column("source_sha256","VARBINARY(32) NOT NULL"),
      Column("compilation_sha256","VARBINARY(32) NULL"),
      Column("request_bytes","LONGBLOB NOT NULL"),
      Column("request_sha256","VARBINARY(32) NOT NULL"),
      Column("expected_catalog_revision","BIGINT NOT NULL"),
      Column("after_catalog_revision","BIGINT NOT NULL"),
      Column("before_bytes","LONGBLOB NOT NULL"),
      Column("before_sha256","VARBINARY(32) NOT NULL"),
      Column("after_bytes","LONGBLOB NOT NULL"),
      Column("after_sha256","VARBINARY(32) NOT NULL"),
      Column("result_code","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("receipt_bytes","LONGBLOB NOT NULL"),
      Column("receipt_sha256","VARBINARY(32) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","operation_uuid"),
    foreignKeys = Vector(
      ForeignKey("fk_so_binding",Vector("binding_uuid"),"fp_script_catalog",Vector("binding_uuid")),
      ForeignKey("fk_so_original",Vector("binding_uuid","operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(operation_uuid)=16 AND operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(source_sha256)=32",
      "OCTET_LENGTH(compilation_sha256)=32",
      "OCTET_LENGTH(request_sha256)=32",
      "OCTET_LENGTH(before_sha256)=32",
      "OCTET_LENGTH(after_sha256)=32",
      "OCTET_LENGTH(receipt_sha256)=32",
      "(REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))",
      "version>0",
      "request_kind IN ('SUBMIT','CHECK','APPROVE','REVOKE')",
      "result_code IN ('APPLIED','REJECTED')",
      "expected_catalog_revision>=0",
      "after_catalog_revision>=0",
      "OCTET_LENGTH(request_bytes)>0",
      "OCTET_LENGTH(receipt_bytes)>0",
      "request_sha256=UNHEX(SHA2(request_bytes,256))",
      "before_sha256=UNHEX(SHA2(before_bytes,256))",
      "after_sha256=UNHEX(SHA2(after_bytes,256))",
      "receipt_sha256=UNHEX(SHA2(receipt_bytes,256))",
      "(result_code='APPLIED' AND request_kind IN ('APPROVE','REVOKE') AND expected_catalog_revision<9223372036854775807 AND after_catalog_revision=expected_catalog_revision+1) OR ((result_code='REJECTED' OR request_kind IN ('SUBMIT','CHECK')) AND after_catalog_revision=expected_catalog_revision AND after_bytes=before_bytes)",
      "request_kind<>'SUBMIT' OR compilation_sha256 IS NULL",
      "request_kind<>'APPROVE' OR compilation_sha256 IS NOT NULL"
    ),
    indexes = Vector(
      Index("ix_so_version",Vector("binding_uuid","behavior_id","version"),false)
    ),
    statementSha256 = "33d27b5a5b85c636685ba7812d5d9e67b85d6432a90d6c47ab45d66fd6b8fee0"
  )
  private def table22: Table = Table(
    name = "fp_script_candidate",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("behavior_id","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("version","BIGINT NOT NULL"),
      Column("source_bytes","MEDIUMBLOB NOT NULL"),
      Column("source_sha256","VARBINARY(32) NOT NULL"),
      Column("submitted_by","VARBINARY(16) NOT NULL"),
      Column("submission_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("check_revision","BIGINT NOT NULL"),
      Column("check_status","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("check_evidence_bytes","LONGBLOB NULL"),
      Column("check_evidence_sha256","VARBINARY(32) NULL"),
      Column("diagnostics_bytes","LONGBLOB NULL"),
      Column("diagnostics_sha256","VARBINARY(32) NULL"),
      Column("loader_id","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("loader_artifact_sha256","VARBINARY(32) NULL"),
      Column("compiler_version","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("schema_version","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL"),
      Column("schema_sha256","VARBINARY(32) NULL"),
      Column("compilation_sha256","VARBINARY(32) NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","behavior_id","version"),
    foreignKeys = Vector(
      ForeignKey("fk_sd_binding",Vector("binding_uuid"),"fp_script_catalog",Vector("binding_uuid")),
      ForeignKey("fk_sd_submit",Vector("binding_uuid","submission_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_sd_op",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(source_sha256)=32",
      "OCTET_LENGTH(submitted_by)=16 AND submitted_by<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(submission_operation_uuid)=16 AND submission_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(check_evidence_sha256)=32",
      "OCTET_LENGTH(diagnostics_sha256)=32",
      "OCTET_LENGTH(loader_artifact_sha256)=32",
      "OCTET_LENGTH(schema_sha256)=32",
      "OCTET_LENGTH(compilation_sha256)=32",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "(REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))",
      "version>0",
      "check_revision>=0",
      "OCTET_LENGTH(source_bytes)<=65536",
      "source_sha256=UNHEX(SHA2(source_bytes,256))",
      "check_evidence_sha256=UNHEX(SHA2(check_evidence_bytes,256))",
      "diagnostics_sha256=UNHEX(SHA2(diagnostics_bytes,256))",
      "loader_id IS NULL OR (REGEXP_LIKE(loader_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(loader_id,'[^a-z0-9_.-]','c'))",
      "(check_status='PENDING' AND check_revision=0 AND check_evidence_bytes IS NULL AND check_evidence_sha256 IS NULL AND diagnostics_bytes IS NULL AND diagnostics_sha256 IS NULL AND loader_id IS NULL AND loader_artifact_sha256 IS NULL AND compiler_version IS NULL AND schema_version IS NULL AND schema_sha256 IS NULL AND compilation_sha256 IS NULL) OR (check_status='INVALID' AND check_revision>0 AND check_evidence_bytes IS NOT NULL AND check_evidence_sha256 IS NOT NULL AND diagnostics_bytes IS NOT NULL AND diagnostics_sha256 IS NOT NULL AND loader_id IS NULL AND loader_artifact_sha256 IS NULL AND compiler_version IS NULL AND schema_version IS NULL AND schema_sha256 IS NULL AND compilation_sha256 IS NULL) OR (check_status='VALID' AND check_revision>0 AND check_evidence_bytes IS NOT NULL AND check_evidence_sha256 IS NOT NULL AND diagnostics_bytes IS NOT NULL AND diagnostics_sha256 IS NOT NULL AND loader_id IS NOT NULL AND loader_artifact_sha256 IS NOT NULL AND compiler_version IS NOT NULL AND schema_version IS NOT NULL AND schema_sha256 IS NOT NULL AND compilation_sha256 IS NOT NULL AND compiler_version='fp-skill-compiler/1' AND schema_version='fp-follow-layout/1' AND schema_sha256=UNHEX('fcdea1a4aef5d08c8ac275b9886986da6c9383cfa50ef57ff75d8cb0f12ffc22'))"
    ),
    indexes = Vector(
      Index("ix_sd_status",Vector("binding_uuid","check_status"),false)
    ),
    statementSha256 = "437e14e320321ffc87d502fc67d157e0a2f6c9f939a07f5b760af08ee72fad3c"
  )
  private def table23: Table = Table(
    name = "fp_script_version",
    columns = Vector(
      Column("binding_uuid","VARBINARY(16) NOT NULL"),
      Column("behavior_id","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("version","BIGINT NOT NULL"),
      Column("source_sha256","VARBINARY(32) NOT NULL"),
      Column("compilation_bytes","MEDIUMBLOB NOT NULL"),
      Column("compilation_sha256","VARBINARY(32) NOT NULL"),
      Column("compiler_version","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("schema_version","VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("schema_sha256","VARBINARY(32) NOT NULL"),
      Column("layout_config_bytes","LONGBLOB NOT NULL"),
      Column("layout_config_sha256","VARBINARY(32) NOT NULL"),
      Column("approved_by","VARBINARY(16) NOT NULL"),
      Column("approval_operation_uuid","VARBINARY(16) NOT NULL"),
      Column("approved_check_revision","BIGINT NOT NULL"),
      Column("approval_catalog_revision","BIGINT NOT NULL"),
      Column("status","VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
      Column("revoked_by","VARBINARY(16) NULL"),
      Column("revocation_operation_uuid","VARBINARY(16) NULL"),
      Column("revocation_catalog_revision","BIGINT NULL"),
      Column("last_mutation_operation_uuid","VARBINARY(16) NOT NULL")
    ),
    primaryKey = Vector("binding_uuid","behavior_id","version"),
    foreignKeys = Vector(
      ForeignKey("fk_sv_candidate",Vector("binding_uuid","behavior_id","version"),"fp_script_candidate",Vector("binding_uuid","behavior_id","version")),
      ForeignKey("fk_sv_approval",Vector("binding_uuid","approval_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_sv_revoke",Vector("binding_uuid","revocation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
      ForeignKey("fk_sv_op",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid"))
    ),
    checks = Vector(
      "OCTET_LENGTH(binding_uuid)=16 AND binding_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(source_sha256)=32",
      "OCTET_LENGTH(compilation_sha256)=32",
      "OCTET_LENGTH(schema_sha256)=32",
      "OCTET_LENGTH(layout_config_sha256)=32",
      "OCTET_LENGTH(approved_by)=16 AND approved_by<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(approval_operation_uuid)=16 AND approval_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(revoked_by)=16 AND revoked_by<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(revocation_operation_uuid)=16 AND revocation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "OCTET_LENGTH(last_mutation_operation_uuid)=16 AND last_mutation_operation_uuid<>UNHEX(REPEAT('00',16))",
      "(REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c') AND NOT REGEXP_LIKE(behavior_id,'[^a-z0-9_.-]','c'))",
      "version>0",
      "approved_check_revision>0",
      "approval_catalog_revision>0",
      "OCTET_LENGTH(compilation_bytes) BETWEEN 1 AND 20357",
      "compilation_sha256=UNHEX(SHA2(compilation_bytes,256))",
      "layout_config_sha256=UNHEX(SHA2(layout_config_bytes,256))",
      "compiler_version='fp-skill-compiler/1' AND schema_version='fp-follow-layout/1' AND schema_sha256=UNHEX('fcdea1a4aef5d08c8ac275b9886986da6c9383cfa50ef57ff75d8cb0f12ffc22')",
      "(status='APPROVED' AND revoked_by IS NULL AND revocation_operation_uuid IS NULL AND revocation_catalog_revision IS NULL) OR (status='REVOKED' AND revoked_by IS NOT NULL AND revocation_operation_uuid IS NOT NULL AND revocation_catalog_revision IS NOT NULL AND revocation_catalog_revision>approval_catalog_revision)"
    ),
    indexes = Vector(
      Index("ix_sv_status",Vector("binding_uuid","status"),false)
    ),
    statementSha256 = "3d0ce6f5ac7b04f2900f577d679bbd82ff01ba89a3e94beb0aeccc2e444ec55e"
  )

  lazy val tables: Vector[Table] = Vector(table00,table01,table02,table03,table04,table05,table06,table07,table08,table09,table10,table11,table12,table13,table14,table15,table16,table17,table18,table19,table20,table21,table22,table23)
  lazy val guards: Vector[Guard] = Vector(
    Guard("fp_s4_01_bi","fp_schema_manifest","INSERT","a8248618b56417f32e6e13cf03c33919ccb77c0c7ee858d0d56f65cc50436098"),
    Guard("fp_s4_01_bu","fp_schema_manifest","UPDATE","d0fbdcfcb543b54b9eb677d4b175fe1f973f388bc707f72c4c2ccf10f3e8ccd9"),
    Guard("fp_s4_01_bd","fp_schema_manifest","DELETE","1ea44a6a016d1130c66ff473051959cec7f127d55474b9d1c52ea5f713263837"),
    Guard("fp_s4_02_bi","fp_runtime_writer","INSERT","cfb7001b535765a7c021e5e2e2eeaefbda7407a9ec2c526969e3792869667919"),
    Guard("fp_s4_02_bu","fp_runtime_writer","UPDATE","134a4c20ba6442a05eff97fd3f7a21f23bacd874f73f14f5813a58d881d7be2b"),
    Guard("fp_s4_02_bd","fp_runtime_writer","DELETE","78dbcad9ca543b9377b6f3dd277f5885970bd1241e30a7baf49eb10b0ebd395f"),
    Guard("fp_s4_03_bi","physical_recovery_binding","INSERT","d28a4785ecb9e42fd1f6f4cb11f6016ad1eb7a934c34a1f8ae6eab13b494a510"),
    Guard("fp_s4_03_bu","physical_recovery_binding","UPDATE","3e3ae29a21f1f614051bc1b2c852120ba2343d431643c4c0dd6799c715b1138c"),
    Guard("fp_s4_03_bd","physical_recovery_binding","DELETE","2b2eb4cc9081be5bdd2db67a7863a4a8d74aa1e94ca83fb33e2b1f595752ee0a"),
    Guard("fp_s4_04_bi","fp_recovery_operation","INSERT","7588724fb5bc63efb1fc57178fa6ee0104aaefd400d2ae17e98ae30d75a6cf2e"),
    Guard("fp_s4_04_bu","fp_recovery_operation","UPDATE","624d99ba9025b62dda7957c0ec117205ddad9d5693050673dacf63f5bdba830a"),
    Guard("fp_s4_04_bd","fp_recovery_operation","DELETE","b6841d70905570bb1861f4f0bc0bf986ae1889592365e0417664e78a84309c8b"),
    Guard("fp_s4_05_bi","floating_pod_state","INSERT","308aed633aea62defa5eaab9a2102dd93e0f6ae632fe3b2d68af4fbcbe440963"),
    Guard("fp_s4_05_bu","floating_pod_state","UPDATE","f3d90bec8795475faf8e90b79d7072096df769a7fd15c54af4ff2b1dd66fe56a"),
    Guard("fp_s4_05_bd","floating_pod_state","DELETE","0e91c16a301207de6d077e5aad21129932bff86bbb360ddc026a9cbbe9d27ec2"),
    Guard("fp_s4_06_bi","player_entitlement","INSERT","6deb14dba7449cf1cea85ecf40279bbf7e17487e32e7d2a4e84211c9a5e3d9f2"),
    Guard("fp_s4_06_bu","player_entitlement","UPDATE","626bdd7fae058ad63ee9bdb48796b2f5877096a304e52048962c508354176510"),
    Guard("fp_s4_06_bd","player_entitlement","DELETE","dd1ca9cfa9c1ccbb3719c40eef8da7184e141d626d720ef36fb0afcfc2d70713"),
    Guard("fp_s4_07_bi","binding_bootstrap_intent","INSERT","be6d57452dc75a363544a2d072e60c06cfae7f22856a7f9a073ed885c8eeb465"),
    Guard("fp_s4_07_bu","binding_bootstrap_intent","UPDATE","a2650090968665b45445aa6a53bda9201d6941794a276c67ca6ad073fad6173b"),
    Guard("fp_s4_07_bd","binding_bootstrap_intent","DELETE","dfedc77e35ae604a7fdc9a52e92844a1e8482ea2fed01f195c78afdbbf688e71"),
    Guard("fp_s4_08_bi","fp_recovery_reservation","INSERT","55292714ffa293ce256645d9865dbaa21585c17e4be2b6f6d77f347ecbf8171a"),
    Guard("fp_s4_08_bu","fp_recovery_reservation","UPDATE","4c5d02ab06282479c211b4f1d4f0a408a21b17da0f64069e6dbcbeb67909383b"),
    Guard("fp_s4_08_bd","fp_recovery_reservation","DELETE","edc91eaa4f046d8c2319013bb0f71481bdaeaa99fc89044782f396d4a47733b7"),
    Guard("fp_s4_09_bi","physical_recovery_footprint","INSERT","4483e85d112253363d67c122c5d00d24920232bb3b2c71b64c4f0c39d64ca84f"),
    Guard("fp_s4_09_bu","physical_recovery_footprint","UPDATE","e089315fa75b5a7ee7820a150d0bde76ae171ea04e11f370c4ff79d47f6cb152"),
    Guard("fp_s4_09_bd","physical_recovery_footprint","DELETE","cf0d8fda8a1afe0700f586c8fe392615e1f6277f6739f3792a98f7c3a12daee6"),
    Guard("fp_s4_10_bi","physical_recovery_footprint_chunk","INSERT","91817d38c45c7977525eb594c03ba159270c0fb1160df9ed239ad123f7995dfb"),
    Guard("fp_s4_10_bu","physical_recovery_footprint_chunk","UPDATE","7f0b34473272e54c3eb4ce07603228810b65a4c10c93f177f180ce538c45ca82"),
    Guard("fp_s4_10_bd","physical_recovery_footprint_chunk","DELETE","649f793bc360151a8731ec88b6364d4b5697333a3837fe9b8b21cf549f1bb24b"),
    Guard("fp_s4_11_bi","physical_recovery_intent","INSERT","18141d17a3597be90def24bf6423909a838a03fd34c3f3a74896538798eee974"),
    Guard("fp_s4_11_bu","physical_recovery_intent","UPDATE","57d7108deabfe9231ef8806b719fcef6c452682e0a91d7f49d4278d1c79b7361"),
    Guard("fp_s4_11_bd","physical_recovery_intent","DELETE","981ae7236aef9e48740c9d4b507a51fff80a50e05bbc15cf60fdc4d7f7b84293"),
    Guard("fp_s4_12_bi","physical_recovery_locator","INSERT","b5208845e2e83c0fe13fae71a7745ef47f87076c70ccbf0ee66ef865a64aa32e"),
    Guard("fp_s4_12_bu","physical_recovery_locator","UPDATE","2d6f72fbe369aa1b120b6a85472d6ff826e6df5e78bc3ece99dcda352d9f6258"),
    Guard("fp_s4_12_bd","physical_recovery_locator","DELETE","313df5f13e999dbbaf7f8a2b88014c3dfee79969ce3f3be8257aad78fde2bb7c"),
    Guard("fp_s4_13_bi","physical_recovery_obligation","INSERT","6d76f91a50b22f867cbd450177696ce2e00cb71a7b854f2430151fbd440ae1ed"),
    Guard("fp_s4_13_bu","physical_recovery_obligation","UPDATE","26404a0ad44d26e5090f16de9786cddec810e4765bec41d7cb1a59a03506647c"),
    Guard("fp_s4_13_bd","physical_recovery_obligation","DELETE","799a129940dbc62ac0097f4fdf8bd91d5a574192b468f494b1d5de390a3c425a"),
    Guard("fp_s4_14_bi","fp_lifecycle_completion","INSERT","cf3911bd764716fa33047a50b499f1376483b03e36ebbdafade274c1d1f8265e"),
    Guard("fp_s4_14_bu","fp_lifecycle_completion","UPDATE","b75ce49f8310ccc5e83b52a45e93f7801be763689493f98473f434ee4fd0f597"),
    Guard("fp_s4_14_bd","fp_lifecycle_completion","DELETE","ab38b086c6858a1445d6b8b7043bceab7a88f1160ebb509e660f8ce26a16a245"),
    Guard("fp_s4_15_bi","physical_footprint_transfer","INSERT","c4de2dc68bffb3d61e557e9f06e632ba306c36220b5d1285a1c37df4b070b348"),
    Guard("fp_s4_15_bu","physical_footprint_transfer","UPDATE","3aee8e9269ded703ab698890ad3c4d96979545652485ad1eb1bae184dee2ec5f"),
    Guard("fp_s4_15_bd","physical_footprint_transfer","DELETE","6ea52f0892fc600526e2ac68b13118175f77e88402198f31b093891401c9f6ed"),
    Guard("fp_s4_16_bi","physical_source_unit","INSERT","1112a18b1d7ceaf1147562cc4867b3e19df7cbe24bd228a7deb169277b2b2aee"),
    Guard("fp_s4_16_bu","physical_source_unit","UPDATE","1dc3bcffa0c7b74ff9b25dd4c9c32ad45d28c5589e7aa9f40d52ce86d5027c4b"),
    Guard("fp_s4_16_bd","physical_source_unit","DELETE","34325ff62d38111fa99d4862c2d346474a3d3f3071c91c0d0d2619d906d76f3e"),
    Guard("fp_s4_17_bi","physical_source_forest","INSERT","91f2be47a8bce361e47535f1a5c06bea1d52ad8ce61bd984f96e127c38d9bc38"),
    Guard("fp_s4_17_bu","physical_source_forest","UPDATE","de92114d25b7c9d85151508a2473d9c1395c7af4280166da4e46f3199d112c1c"),
    Guard("fp_s4_17_bd","physical_source_forest","DELETE","96b6da18682af49b6abe2b05a8d1a294101f468163e42ab95b87b3a97ab2bab7"),
    Guard("fp_s4_18_bi","physical_source_transfer","INSERT","3ff5e4473af3ba6b6a302af8109d3b5a8218d7ae28a3f0618df88e6cb0769b75"),
    Guard("fp_s4_18_bu","physical_source_transfer","UPDATE","18e3a19759b394598c9a08c520801a9edb6c27eaa6c9c3c8838aee92b03db27c"),
    Guard("fp_s4_18_bd","physical_source_transfer","DELETE","0726b6c0ced73fd7465d48b4633504fb742606fde156cc2d005044be74a1f64f"),
    Guard("fp_s4_19_bi","physical_source_write","INSERT","0a92c23b8b2f437efe3d574476f950dd0d56d104d3c7fb326eaee6a1c5ec55b9"),
    Guard("fp_s4_19_bu","physical_source_write","UPDATE","89fd6f0c0a5998464448975d5754e484870ccd658c1f6c0b5296f2497c85cb3c"),
    Guard("fp_s4_19_bd","physical_source_write","DELETE","5236845454c5f1e292ad6b66c56579026cbc183e14fb07d86bbbe4755fa0c3b3"),
    Guard("fp_s4_20_bi","physical_source_obligation","INSERT","2451f01bc754408504da53343a2a65120f4b4d7b879200af3149ed0b0e642b83"),
    Guard("fp_s4_20_bu","physical_source_obligation","UPDATE","e0ea16ca48d3622b5bbdea9d72d4894002a1eb6f810ad25353a6f73f17b0a47d"),
    Guard("fp_s4_20_bd","physical_source_obligation","DELETE","3bbe78a45ca37702a46be04c8045b9e0fcf5e4401e04272b414f4eeb1c20626b"),
    Guard("fp_s4_21_bi","fp_script_catalog","INSERT","12e6cc9d045be8d111ab5160de22f85832ecbcba11fae64113eb7dd0a19af4cb"),
    Guard("fp_s4_21_bu","fp_script_catalog","UPDATE","4a069623b5c21afcc0339ecb217dcc01338b54097e6b1fa444ce43df490d0692"),
    Guard("fp_s4_21_bd","fp_script_catalog","DELETE","c57a57bb357569aa63b0195db7219d190ccfb54678418a0e24c1dffce8f15730"),
    Guard("fp_s4_22_bi","fp_script_candidate","INSERT","a74d2a394fedddc3174cc80aa2d3e91495b5f1ee9e9d713d418f9967c343a130"),
    Guard("fp_s4_22_bu","fp_script_candidate","UPDATE","20a197105fcdac47e110ab92ab7b502641c765eba0cc1b0e260746b1029898e7"),
    Guard("fp_s4_22_bd","fp_script_candidate","DELETE","a31f31fd7fc06d6c1ddcd346a3639819e3ec815d50402a98a67dd2bc90cadf30"),
    Guard("fp_s4_23_bi","fp_script_version","INSERT","e15d4af7559c729e74e4be0881b116f9634c972e5cd3d5fd62f6c7abd6ffbe36"),
    Guard("fp_s4_23_bu","fp_script_version","UPDATE","3093d28c52dbe059c959627b86530dde27c1daa868231329f7a95910a1c3dedb"),
    Guard("fp_s4_23_bd","fp_script_version","DELETE","666a9c45b4d14e232b4b7d42cb9a342e3b0d3fe5e634dcac223a8b3d3eaddd5e"),
    Guard("fp_s4_24_bi","fp_script_operation","INSERT","30e6a26244042aa268a5bc5026d838dab4df222a5863c9c82e56d93d7c1e574f"),
    Guard("fp_s4_24_bu","fp_script_operation","UPDATE","1db8744d30bbdff6e9765bf30a54ca5196f9a7d0045aa6ac8e94f06fa370efd7"),
    Guard("fp_s4_24_bd","fp_script_operation","DELETE","7e46fc7db25bf32245fcc262bfa783bd8e311e91a11a1d7926502383e72c40ba")
  )
