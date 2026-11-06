package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import MySqlScriptSchemaProfile.{Column, ForeignKey}
import java.util.HexFormat

/** DB-first 完整实体快照的独立 schema 身份。旧 schema5 的操作和编码不改义。
  * SQL 行约束不证明事务已提交，也不发行实体清理或恢复许可。
  */
object MySqlDbFirstSchemaDefinition:
  val SchemaFormat = 6
  val Profile = "ScriptPublication1DeploymentReceipt1DbFirst1"
  val PhysicalBindingFormat = 2
  val Coverage = "script-publication1-deployment-receipt1-db-first1:exact27-tables-exact81-guards-auditself-and-privileges"
  val AuditRoutine = "fp_audit_schema_db_first1"
  val AuditDomain = "FloatingPod-MySQL-Audit-script-publication1-deployment-receipt1-db-first1-v1"
  val SnapshotDomain = "floatingpod.mysql.script-publication1-deployment-receipt1-db-first1-full27/v1"
  val SnapshotTable = "fp_pod_snapshot"
  val RecallStep = "DB_FIRST_RECALL"
  val CheckpointStep = "DB_FIRST_ACTIVE_CHECKPOINT"
  val InitialActivationStep = "DB_FIRST_INITIAL_ACTIVATION"
  val RecallJournalStep = "DB_FIRST_RECALL_REQUEST"
  val RecallRefusedStep = "DB_FIRST_RECALL_REFUSED"
  val RecoverySteps: Vector[String] = MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps ++ Vector(RecallStep, InitialActivationStep, CheckpointStep, RecallJournalStep, RecallRefusedStep)
  // 读取与SQL只接受已闭合的协议族；真实提交/未应用资格仍由原执行owner验证。
  val DataRecoverySteps: Vector[String] = RecoverySteps

  // 每个绑定/Pod 的最新恢复快照；历史 before/target 仍由原操作记录持有。
  val snapshotColumns: Vector[Column] = Vector(
    Column("binding_uuid", "VARBINARY(16) NOT NULL"),
    Column("pod_uuid", "VARBINARY(16) NOT NULL"),
    Column("owner_uuid", "VARBINARY(16) NULL"),
    Column("attempt_uuid", "VARBINARY(16) NOT NULL"),
    Column("snapshot_format_version", "SMALLINT NOT NULL"),
    Column("nbt_bytes", "LONGBLOB NOT NULL"),
    Column("nbt_sha256", "VARBINARY(32) NOT NULL"),
    Column("limit_profile_encoding", "VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL"),
    Column("limit_profile_bytes", "LONGBLOB NOT NULL"),
    Column("limit_profile_sha256", "VARBINARY(32) NOT NULL"),
    Column("captured_state_version", "BIGINT NOT NULL"),
    Column("committed_state_version", "BIGINT NOT NULL"),
    Column("last_mutation_operation_uuid", "VARBINARY(16) NOT NULL")
  )
  val snapshotPrimaryKey: Vector[String] = Vector("binding_uuid", "pod_uuid")
  val snapshotForeignKeys: Vector[ForeignKey] = Vector(
    ForeignKey("fk_snapshot_binding", Vector("binding_uuid"), "physical_recovery_binding", Vector("binding_uuid")),
    ForeignKey("fk_snapshot_pod", Vector("pod_uuid"), "floating_pod_state", Vector("uuid")),
    ForeignKey("fk_snapshot_mutation", Vector("binding_uuid", "last_mutation_operation_uuid"),
      "fp_recovery_operation", Vector("binding_uuid", "operation_uuid"))
  )

  /** 单 blob 上限；完整行、事务及 retained 分配仍须由同一共享预算逐项准入。 */
  def maximumNbtBytes(profile: SourceLimitProfile): Long =
    require(profile != null, "DB-first snapshot requires an explicit source limit profile")
    Math.min(4294967295L, Math.min(profile.maxDecompressedBytes(), profile.maxSqlPacketBytes()))

  def snapshotChecks(profile: SourceLimitProfile): Vector[String] =
    val maximum = maximumNbtBytes(profile)
    val profileHex = HexFormat.of().formatHex(profile.canonicalBytes())
    val profileHash = profile.sha256Hex()
    val identities = Vector("binding_uuid", "pod_uuid", "attempt_uuid", "last_mutation_operation_uuid")
      .map(name => s"OCTET_LENGTH($name)=16 AND $name<>UNHEX(REPEAT('00',16))")
    identities ++ Vector(
      "owner_uuid IS NULL OR (OCTET_LENGTH(owner_uuid)=16 AND owner_uuid<>UNHEX(REPEAT('00',16)))",
      s"snapshot_format_version=${DbFirstPodSnapshot.FORMAT_VERSION}",
      s"OCTET_LENGTH(nbt_bytes)>0 AND OCTET_LENGTH(nbt_bytes)<=$maximum",
      "OCTET_LENGTH(nbt_sha256)=32 AND nbt_sha256=UNHEX(SHA2(nbt_bytes,256))",
      s"limit_profile_encoding='${SourceLimitProfile.ENCODING}'",
      s"limit_profile_bytes=UNHEX('$profileHex')",
      s"OCTET_LENGTH(limit_profile_sha256)=32 AND limit_profile_sha256=UNHEX('$profileHash') AND limit_profile_sha256=UNHEX(SHA2(limit_profile_bytes,256))",
      "captured_state_version>=0 AND captured_state_version<9223372036854775807",
      "committed_state_version>captured_state_version AND committed_state_version-captured_state_version=1"
    )
