package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, ResultSet, SQLDataException}
import java.util.UUID

enum MySqlBootstrapIntentStage:
  case Prepared, Complete

/** B1 表的完整不可变行；保留 source 原文，不将数据库行解释为 Fresh 或 marker ACK 证明。 */
final case class MySqlBootstrapIntentData(
    singletonFixed1: Int,
    bootstrapOperationUuid: UUID,
    prepareWriteUuid: UUID,
    certifyWriteUuid: UUID,
    bindingUuid: UUID,
    bindingFormatVersion: Int,
    provisioningUuid: UUID,
    sourceWorldIdentity: String,
    provisionReceiptSha256: Vector[Byte],
    stage: MySqlBootstrapIntentStage,
    markerAckSha256: Option[Vector[Byte]]
)

/** 调用方拥有 Connection、事务与 IO thread；不改 autoCommit/isolation，不关闭 Connection。 */
final class MySqlBootstrapIntentRows(connection: Connection, schema: String):
  require(connection != null, "caller-owned connection is required")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "fixed schema identifier is unsafe")
  private val ownerThread = Thread.currentThread()

  /** 独立全读，不 join、不按 stage 过滤；一致性由外层 admitted snapshot transaction 保证。 */
  def readAll(): Vector[MySqlBootstrapIntentData] =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("MySQL bootstrap intent read requires its caller-owned IO thread")
    val columns = "singleton_fixed1,bootstrap_operation_uuid,prepare_write_uuid,certify_write_uuid,binding_uuid,binding_format_version,provisioning_uuid,source_world_identity,provision_receipt_sha256,stage,marker_ack_sha256"
    MySqlBusinessRows.using(connection.prepareStatement(s"SELECT $columns FROM `$schema`.`binding_bootstrap_intent` ORDER BY singleton_fixed1")) { statement =>
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        val rows = Vector.newBuilder[MySqlBootstrapIntentData]
        while result.next() do rows += MySqlBootstrapIntentRows.decode(result)
        rows.result()
      }
    }

object MySqlBootstrapIntentRows:
  private[mysql] def decode(row: ResultSet): MySqlBootstrapIntentData =
    val stage = requiredText(row, 10, "stage") match
      case "PREPARED" => MySqlBootstrapIntentStage.Prepared
      case "COMPLETE" => MySqlBootstrapIntentStage.Complete
      case _ => invalid("stage token")
    MySqlBootstrapIntentData(requiredInt(row, 1, "singleton_fixed1"),
      requiredUuid(row, 2, "bootstrap_operation_uuid"), requiredUuid(row, 3, "prepare_write_uuid"),
      requiredUuid(row, 4, "certify_write_uuid"), requiredUuid(row, 5, "binding_uuid"),
      requiredInt(row, 6, "binding_format_version"), requiredUuid(row, 7, "provisioning_uuid"),
      requiredText(row, 8, "source_world_identity"), requiredBinary(row, 9, "provision_receipt_sha256", 32),
      stage, binary(row, 11, "marker_ack_sha256", 32))

  /** 仅验证 actual DDL 的 PK/CHECK；该表无 FK，也无 operation UUID UNIQUE。
    * 不核外部 source 编码/新鲜度、receipt 内容或 marker ACK 的真实性。
    */
  def validateGraph(rows: Vector[MySqlBootstrapIntentData]): Unit =
    if rows.map(_.singletonFixed1).distinct.size != rows.size then invalid("duplicate singleton primary key")
    rows.foreach { row =>
      if row.singletonFixed1 != 1 then invalid("singleton_fixed1 check")
      if row.bootstrapOperationUuid == null || row.prepareWriteUuid == null || row.certifyWriteUuid == null ||
          row.bindingUuid == null || row.provisioningUuid == null then invalid("required UUID")
      if row.prepareWriteUuid == row.certifyWriteUuid then invalid("distinct prepare/certify write UUID check")
      if row.bindingFormatVersion != 2 then invalid("binding_format_version check")
      // MySQL TRIM 默认只去除 U+0020；不把 tab/newline 擅自扩充为 DDL 拒绝条件。
      if row.sourceWorldIdentity == null || !row.sourceWorldIdentity.exists(_ != ' ') then invalid("source_world_identity nonblank check")
      if row.provisionReceiptSha256 == null || row.provisionReceiptSha256.size != 32 then invalid("provision_receipt_sha256 byte length")
      if row.markerAckSha256 == null || row.markerAckSha256.exists(bytes => bytes == null || bytes.size != 32) then invalid("marker_ack_sha256 byte length")
      row.stage match
        case MySqlBootstrapIntentStage.Prepared => if row.markerAckSha256.nonEmpty then invalid("PREPARED marker ACK must be NULL")
        case MySqlBootstrapIntentStage.Complete => if row.markerAckSha256.isEmpty then invalid("COMPLETE marker ACK required")
        case null => invalid("required stage")
    }

  private def binary(row: ResultSet, index: Int, label: String, length: Int): Option[Vector[Byte]] =
    val bytes = row.getBytes(index)
    if bytes == null then None
    else
      if bytes.length != length then invalid(s"$label byte length")
      Some(bytes.toVector)
  private def requiredBinary(row: ResultSet, index: Int, label: String, length: Int): Vector[Byte] =
    binary(row, index, label, length).getOrElse(missing(label))
  private def requiredUuid(row: ResultSet, index: Int, label: String): UUID =
    MySqlStoredRows.decodeUuid(requiredBinary(row, index, label, 16), label)
  private def requiredInt(row: ResultSet, index: Int, label: String): Int =
    val value = row.getInt(index)
    if row.wasNull() then missing(label)
    value
  private def requiredText(row: ResultSet, index: Int, label: String): String =
    Option(row.getString(index)).getOrElse(missing(label))
  private def missing(label: String): Nothing = throw new SQLDataException(s"MySQL required bootstrap intent column $label is SQL NULL")
  private def invalid(boundary: String): Nothing = throw new SQLDataException(s"MySQL bootstrap intent row/graph violates $boundary")
