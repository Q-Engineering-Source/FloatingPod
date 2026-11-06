package com.wi.floatingpod.storage.mysql

import com.google.gson.stream.JsonWriter
import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.security.{DigestOutputStream, MessageDigest}
import java.sql.SQLDataException
import java.util.HexFormat

/** B 文件的有界对称编码；只持久化 DATA，不将输入提升为独立批准或运行资格。 */
private[mysql] object MySqlStaticBaselineEncoder:
  def write(identity: MySqlApprovedBaselineIdentity, objects: MySqlApprovedObjectManifest,
      destination: OutputStream, root: MySqlFormat3ReadBudget): String =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if identity == null || objects == null || destination == null ||
        identity.schemaFormat != objects.profile.schemaFormat || identity.coverage != objects.profile.coverage then
      throw new IllegalArgumentException("exact baseline identity, object DATA, output and root required")
    var maximumBinary = 0L
    objects.resultSets.foreach(_.rows.foreach(_.cells.foreach {
      case value: MySqlAuditBinary => maximumBinary = Math.max(maximumBinary, value.length.toLong)
      case _: MySqlAuditText | MySqlAuditNull => ()
      case _ => throw new SQLDataException("static baseline cannot encode temporal or unsupported cells")
    }))
    val workspace = Math.addExact(65536L, Math.multiplyExact(maximumBinary, 8L))
    MySqlManifestConfigurationIdentity.temporary(root, workspace) {
      val digest = MessageDigest.getInstance("SHA-256")
      val bounded = new LimitedOutput(destination, MySqlApprovedBaselineLoader.MaxApprovedFileBytes)
      val writer = new JsonWriter(new OutputStreamWriter(new DigestOutputStream(bounded, digest), StandardCharsets.UTF_8))
      // writer/destination 均由 caller 负责其最终发布；这里只flush，不关闭外部流。
      writer.beginObject()
      def text(key: String, value: String): Unit =
        if value == null || !StandardCharsets.UTF_8.newEncoder().canEncode(value) then
          throw new SQLDataException("static baseline text must be valid Unicode")
        writer.name(key).value(value)
        ()
      text("format", identity.format)
      writer.name("schemaFormat").value(identity.schemaFormat.toLong)
      text("coverage", identity.coverage)
      text("serverUuid", identity.serverUuid.toString)
      text("serverVersion", identity.serverVersion)
      text("schema", identity.schema)
      text("provisioningUuid", identity.provisioningUuid.toString)
      text("bindingUuid", identity.bindingUuid.toString)
      text("ddlSha256", identity.ddlSha256)
      text("auditDefiner", identity.auditDefiner)
      text("sourceCaptureSha256", identity.sourceCaptureSha256)
      writer.name("resultSets")
      writeResultSets(writer, objects.resultSets)
      writer.endObject()
      writer.flush()
      HexFormat.of().formatHex(digest.digest())
    }

  /** 共享的真实 JDBC DATA 编码；只写列/typed cells，不签发任何来源或批准。 */
  private[mysql] def writeResultSets(writer: JsonWriter, sets: Vector[MySqlAuditResultSet]): Unit =
    def text(key: String, value: String): Unit =
      if value == null || !StandardCharsets.UTF_8.newEncoder().canEncode(value) then
        throw new SQLDataException("static JDBC DATA text must be valid Unicode")
      writer.name(key).value(value)
      ()
    writer.beginArray()
    sets.foreach { set =>
      writer.beginObject()
      text("id", set.id)
      writer.name("columns").beginArray()
      set.columns.foreach { column =>
        writer.beginObject()
        text("label", column.label)
        writer.name("jdbcType").value(column.jdbcType.toLong)
        text("typeName", column.typeName)
        writer.name("precision").value(column.precision.toLong)
        writer.name("scale").value(column.scale.toLong)
        writer.name("nullable").value(column.nullable.toLong)
        writer.name("signed").value(column.signed)
        writer.endObject()
      }
      writer.endArray()
      writer.name("rows").beginArray()
      set.rows.foreach { row =>
        writer.beginArray()
        row.cells.foreach { cell =>
          writer.beginObject()
          cell match
            case MySqlAuditNull => text("tag", "SQL_NULL")
            case MySqlAuditText(value) => text("tag", "TEXT"); text("value", value)
            case binary: MySqlAuditBinary =>
              text("tag", "BINARY")
              text("hex", HexFormat.of().withUpperCase().formatHex(binary.bytes))
            case _ => throw new SQLDataException("unsupported static baseline cell")
          writer.endObject()
        }
        writer.endArray()
      }
      writer.endArray()
      writer.endObject()
    }
    writer.endArray()
    ()

  /** 在写入caller流之前检查精确UTF-8字节总量；不拥有或关闭caller流。 */
  private final class LimitedOutput(destination: OutputStream, maximum: Long) extends OutputStream:
    private var written = 0L
    private def count(length: Int): Unit =
      val next = Math.addExact(written, length.toLong)
      if next > maximum then throw new SQLDataException("encoded static baseline exceeds its file byte bound")
      written = next
    override def write(value: Int): Unit = { count(1); destination.write(value) }
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(offset, length, bytes.length)
      count(length)
      destination.write(bytes, offset, length)
    override def flush(): Unit = destination.flush()
