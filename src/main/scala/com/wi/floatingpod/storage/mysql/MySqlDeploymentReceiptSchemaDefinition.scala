package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import MySqlScriptSchemaProfile.{Column, ForeignKey, Index}

/** schema5 部署回执闭合关系与 wire SQL 合同；不代表实际 MySQL 安装或 Ready 资格。 */
object MySqlDeploymentReceiptSchemaDefinition:
  val SchemaFormat = 5
  val Profile = "ScriptPublication1DeploymentReceipt1"
  val PhysicalBindingFormat = 2
  val Coverage = "script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges"
  val AuditRoutine = "fp_audit_schema_script_deployment_receipt1"
  val AuditDomain = "FloatingPod-MySQL-Audit-script-publication1-deployment-receipt1-v1"
  val SnapshotDomain = "floatingpod.mysql.script-publication1-deployment-receipt1-full26/v1"

  val OperationTable = "fp_deployment_operation"
  val QuotaTable = "fp_deployment_quota"
  val NamespaceHex = "46504443"
  val RecoverySteps: Vector[(Int, String)] = Vector(
    1 -> "DEPLOYMENT_REGISTER",
    2 -> "DEPLOYMENT_START",
    3 -> "DEPLOYMENT_FINALIZE",
    4 -> "DEPLOYMENT_RECONCILE",
    5 -> "DEPLOYMENT_CLEANUP"
  )

  val OldRecoverySteps: Vector[String] = Vector(
    "RESERVE", "CANCEL", "ENTITLEMENT_REGISTER", "DOMAIN_MUTATION", "REARM", "ACTIVATE",
    "RECALL_CHECKPOINT", "ABORT_CLEANUP", "MOVE_PREPARE", "MOVE_COMMIT", "MOVE_CLEANUP",
    "MOVE_REMEDIATE", "SOURCE_CAPTURE", "SOURCE_PREPARE", "SOURCE_WRITE_PREPARE",
    "SOURCE_WRITE_RESULT", "SOURCE_WRITE_RECONCILE", "SOURCE_CUTOVER", "SOURCE_RETIRE",
    "SOURCE_REMEDIATE", "RESERVATION_RELEASE", "SCRIPT_PUBLICATION"
  )
  val RecallPrepareSteps: Vector[String] = Vector("RECALL_PREPARE")
  val Schema5RecoverySteps: Vector[String] = OldRecoverySteps ++ RecallPrepareSteps ++ RecoverySteps.map(_._2)

  def recoveryStepToken(code: Int): String =
    RecoverySteps.find(_._1 == code).map(_._2).getOrElse(
      throw new IllegalArgumentException("deployment recovery step code is outside closed schema5 range"))

  /** 对 FPDC receipt-v1 字节的 SQL 校验与 DeploymentCommandWireCodec.isLegalReceipt 保持一致。 */
  val LegalReceiptSql: String =
    val phase = "ORD(SUBSTRING(receipt_bytes,136,1))"
    val reason = "CAST(CONV(HEX(SUBSTRING(receipt_bytes,137,2)),16,10) AS UNSIGNED)"
    val effect = "ORD(SUBSTRING(receipt_bytes,139,1))"
    val commit = "ORD(SUBSTRING(receipt_bytes,140,1))"
    val readiness = "ORD(SUBSTRING(receipt_bytes,141,1))"
    val cleanup = "ORD(SUBSTRING(receipt_bytes,142,1))"
    val phaseCodes = DeploymentCommandWireCodec.OperationPhase.values().map(_.wireCode()).sorted.mkString(",")
    val reasonCodes = DeploymentCommandWireCodec.ReceiptReason.values().map(_.wireCode()).sorted.mkString(",")
    val effectCodes = DeploymentCommandWireCodec.ActualEffectStatus.values().map(_.wireCode()).sorted.mkString(",")
    val commitCodes = DeploymentCommandWireCodec.CriticalCommitStatus.values().map(_.wireCode()).sorted.mkString(",")
    val readinessCodes = DeploymentCommandWireCodec.ReadinessStatus.values().map(_.wireCode()).sorted.mkString(",")
    val cleanupCodes = DeploymentCommandWireCodec.CleanupStatus.values().map(_.wireCode()).sorted.mkString(",")
    val legalTuples =
      s"(($phase=1 AND $reason=0 AND $effect=0 AND $commit=0 AND $readiness=0 AND $cleanup=0) OR " +
      s"($phase=2 AND $reason=0 AND $effect<>4 AND $commit IN (0,1) AND $cleanup NOT IN (3,4)) OR " +
      s"($phase=3 AND $reason=0 AND $effect=3 AND $commit=1 AND $cleanup NOT IN (3,4)) OR " +
      s"($phase=4 AND $reason IN (1,2,3,4,5,13) AND $effect=1 AND $commit=0 AND $cleanup=0 AND $readiness IN (0,3)) OR " +
      s"($phase=5 AND $effect<>4 AND $commit<>3 AND $cleanup<>4 AND ((($cleanup IN (1,3) AND $reason=11)) OR " +
      s"($cleanup NOT IN (1,3) AND $reason=9 AND $effect IN (1,2)) OR " +
      s"($cleanup NOT IN (1,3) AND $reason=10 AND $commit=2) OR " +
      s"($cleanup NOT IN (1,3) AND $reason=14 AND $effect IN (0,1,2,3) AND $commit IN (0,1,2) AND " +
      s"$readiness IN (0,1,2,3) AND $cleanup IN (0,2) AND NOT ($effect=3 AND $commit=1)))) OR " +
      s"($phase=6 AND ($effect=4 OR $commit=3 OR $cleanup=4) AND $readiness<>2 AND " +
      s"((($cleanup IN (1,3,4) AND $reason=11)) OR ($cleanup NOT IN (1,3,4) AND $reason=12))))"
    s"($phase IN ($phaseCodes) AND $reason IN ($reasonCodes) AND $effect IN ($effectCodes) AND " +
      s"$commit IN ($commitCodes) AND $readiness IN ($readinessCodes) AND $cleanup IN ($cleanupCodes) AND $legalTuples)"

  /** 实际 root 预算由调用者提供；这里只约束 SQL 列能够表达的长度，不代替共享账本预留。 */
  final case class Budget(evidenceBytes: Long, diagnosticsBytes: Long):
    require(evidenceBytes > 0 && diagnosticsBytes > 0, "explicit positive deployment receipt budgets required")
    require(evidenceBytes <= 4294967295L && diagnosticsBytes <= 4294967295L,
      "deployment receipt budget exceeds LONGBLOB capacity")

  val operationColumns: Vector[Column] = Vector(
    Column("namespace", "VARBINARY(4) NOT NULL"),
    Column("operation_uuid", "VARBINARY(16) NOT NULL"),
    Column("actor_uuid", "VARBINARY(16) NOT NULL"),
    Column("authority_binding_uuid", "VARBINARY(16) NOT NULL"),
    Column("request_encoding", "SMALLINT NOT NULL"),
    Column("request_bytes", "VARBINARY(134) NOT NULL"),
    Column("request_sha256", "VARBINARY(32) NOT NULL"),
    Column("receipt_encoding", "SMALLINT NOT NULL"),
    Column("receipt_bytes", "VARBINARY(142) NOT NULL"),
    Column("receipt_sha256", "VARBINARY(32) NOT NULL"),
    Column("row_revision", "BIGINT NOT NULL"),
    Column("created_boot_uuid", "VARBINARY(16) NOT NULL"),
    Column("created_boot_generation", "BIGINT NOT NULL"),
    Column("created_writer_epoch", "BIGINT NOT NULL"),
    Column("current_writer_boot_uuid", "VARBINARY(16) NOT NULL"),
    Column("current_writer_epoch", "BIGINT NOT NULL"),
    Column("register_step_uuid", "VARBINARY(16) NOT NULL"),
    Column("last_step_uuid", "VARBINARY(16) NOT NULL"),
    Column("last_step_code", "SMALLINT NOT NULL"),
    Column("last_step_fingerprint", "VARBINARY(32) NOT NULL"),
    Column("enqueued_end_ordinal", "BIGINT NOT NULL"),
    Column("max_queue_age_end_ordinals", "BIGINT NOT NULL"),
    Column("enqueue_evidence_encoding", "SMALLINT NOT NULL"),
    Column("enqueue_evidence_bytes", "LONGBLOB NOT NULL"),
    Column("enqueue_evidence_sha256", "VARBINARY(32) NOT NULL"),
    Column("step_evidence_encoding", "SMALLINT NOT NULL"),
    Column("step_evidence_bytes", "LONGBLOB NOT NULL"),
    Column("step_evidence_sha256", "VARBINARY(32) NOT NULL"),
    Column("diagnostic_encoding", "SMALLINT NULL"),
    Column("diagnostic_bytes", "LONGBLOB NULL"),
    Column("diagnostic_sha256", "VARBINARY(32) NULL")
  )

  val operationPrimaryKey: Vector[String] = Vector("namespace", "operation_uuid")
  val operationIndexes: Vector[Index] = Vector(
    Index("ix_fp_deployment_actor_operation", Vector("namespace", "actor_uuid", "operation_uuid"), unique = false)
  )
  // 历史回执不依赖当前 owner、pod 或 module 是否仍存在。
  val operationForeignKeys: Vector[ForeignKey] = Vector.empty

  def operationChecks(budget: Budget): Vector[String] = Vector(
    s"OCTET_LENGTH(namespace)=4 AND namespace=UNHEX('$NamespaceHex')",
    "OCTET_LENGTH(operation_uuid)=16",
    "OCTET_LENGTH(actor_uuid)=16",
    "OCTET_LENGTH(authority_binding_uuid)=16",
    "request_encoding=1",
    "OCTET_LENGTH(request_bytes)=134",
    "OCTET_LENGTH(request_sha256)=32",
    "request_sha256=UNHEX(SHA2(request_bytes,256))",
    "SUBSTRING(request_bytes,1,4)=UNHEX('46504443') AND ORD(SUBSTRING(request_bytes,5,1))=1 AND ORD(SUBSTRING(request_bytes,6,1)) IN (1,2)",
    "operation_uuid=SUBSTRING(request_bytes,7,16)",
    "ORD(SUBSTRING(request_bytes,55,1))<128 AND ORD(SUBSTRING(request_bytes,79,1))<128 AND ORD(SUBSTRING(request_bytes,103,1))<128 AND ORD(SUBSTRING(request_bytes,111,1))<128 AND ORD(SUBSTRING(request_bytes,119,1))<128 AND ORD(SUBSTRING(request_bytes,127,1))<128",
    "receipt_encoding=1",
    "OCTET_LENGTH(receipt_bytes)=142",
    "OCTET_LENGTH(receipt_sha256)=32",
    "receipt_sha256=UNHEX(SHA2(receipt_bytes,256))",
    "SUBSTRING(receipt_bytes,1,6)=UNHEX('465044430181')",
    "SUBSTRING(receipt_bytes,7,56)=SUBSTRING(request_bytes,7,56)",
    "SUBSTRING(receipt_bytes,63,1)=SUBSTRING(request_bytes,6,1)",
    "SUBSTRING(receipt_bytes,64,72)=SUBSTRING(request_bytes,63,72)",
    LegalReceiptSql,
    "row_revision>=0",
    "OCTET_LENGTH(created_boot_uuid)=16",
    "created_boot_generation>=0",
    "created_writer_epoch>=0",
    "OCTET_LENGTH(current_writer_boot_uuid)=16",
    "current_writer_epoch>=0",
    "OCTET_LENGTH(register_step_uuid)=16",
    "OCTET_LENGTH(last_step_uuid)=16",
    "last_step_code IN (1,2,3,4,5)",
    "OCTET_LENGTH(last_step_fingerprint)=32",
    "enqueued_end_ordinal>=0",
    "max_queue_age_end_ordinals>=0",
    "enqueue_evidence_encoding=1",
    s"OCTET_LENGTH(enqueue_evidence_bytes)>0 AND OCTET_LENGTH(enqueue_evidence_bytes)<=${budget.evidenceBytes}",
    "OCTET_LENGTH(enqueue_evidence_sha256)=32",
    "enqueue_evidence_sha256=UNHEX(SHA2(enqueue_evidence_bytes,256))",
    "step_evidence_encoding=1",
    s"OCTET_LENGTH(step_evidence_bytes)>0 AND OCTET_LENGTH(step_evidence_bytes)<=${budget.evidenceBytes}",
    "OCTET_LENGTH(step_evidence_sha256)=32",
    "step_evidence_sha256=UNHEX(SHA2(step_evidence_bytes,256))",
    // 显式排除部分 NULL；不能让 SQL CHECK 的 UNKNOWN 放过损坏的诊断三元组。
    "(diagnostic_encoding IS NULL AND diagnostic_bytes IS NULL AND diagnostic_sha256 IS NULL) OR " +
      "(diagnostic_encoding IS NOT NULL AND diagnostic_bytes IS NOT NULL AND diagnostic_sha256 IS NOT NULL AND " +
      "diagnostic_encoding=1 AND OCTET_LENGTH(diagnostic_bytes)>0 AND " +
      s"OCTET_LENGTH(diagnostic_bytes)<=${budget.diagnosticsBytes} AND OCTET_LENGTH(diagnostic_sha256)=32 AND " +
      "diagnostic_sha256=UNHEX(SHA2(diagnostic_bytes,256)))"
  )

  val quotaColumns: Vector[Column] = Vector(
    Column("namespace", "VARBINARY(4) NOT NULL"),
    Column("max_records", "BIGINT NOT NULL"),
    Column("used_records", "BIGINT NOT NULL"),
    Column("quota_revision", "BIGINT NOT NULL")
  )
  val quotaPrimaryKey: Vector[String] = Vector("namespace")
  val quotaIndexes: Vector[Index] = Vector.empty
  val quotaForeignKeys: Vector[ForeignKey] = Vector.empty
  val quotaChecks: Vector[String] = Vector(
    s"OCTET_LENGTH(namespace)=4 AND namespace=UNHEX('$NamespaceHex')",
    "max_records>0",
    "used_records>=0",
    "used_records<=max_records",
    "quota_revision>=0"
  )
