package com.wi.floatingpod.storage.mysql

import com.google.gson.stream.JsonWriter
import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.security.{DigestOutputStream, MessageDigest}
import java.sql.SQLDataException
import java.util.HexFormat

/** 独立的真实首启观察文件。源摘要来自本文件字节，绝不以待生成的 B 文件自我背书。 */
private[mysql] object MySqlManagedCaptureEncoder:
  def write(record: ManagedInstallationIdentity.Record, installer: ManagedMySqlConnectionOwner,
      catalog: MySqlManagedCatalogCapture.Captured, security: MySqlManagedCatalogCapture.Captured,
      destination: OutputStream, root: MySqlFormat3ReadBudget): String =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if record == null || installer == null || catalog == null || security == null || destination == null then
      reject("actual retained managed capture owners and destination required")
    installer.requireHeldInitializationLock()
    if installer.identity().serverUuid != record.serverUuid() || installer.identity().catalog != record.schema() then
      reject("managed capture target differs from persisted installation identity")
    val objects = catalog.resultSets
    val privileges = security.resultSets
    if objects.map(_.id) != MySqlAuditCoverage.StaticResultSetIds ||
        privileges.map(_.id) != MySqlAuditCoverage.Format3PrivilegeResultSetIds then
      reject("managed capture DATA coverage differs")
    var maximumBinary = 0L
    (objects ++ privileges).foreach(_.rows.foreach(_.cells.foreach {
      case value: MySqlAuditBinary => maximumBinary = math.max(maximumBinary, value.length.toLong)
      case _: MySqlAuditText | MySqlAuditNull => ()
      case _ => reject("managed static capture contains an unsupported temporal value")
    }))
    MySqlManifestConfigurationIdentity.temporary(root, Math.addExact(65536L, Math.multiplyExact(maximumBinary, 8L))) {
      val digest = MessageDigest.getInstance("SHA-256")
      val bounded = new BoundedOutput(destination)
      val writer = new JsonWriter(new OutputStreamWriter(new DigestOutputStream(bounded, digest), StandardCharsets.UTF_8))
      writer.beginObject()
      writer.name("format").value("FP_MANAGED_INSTALLATION_CAPTURE_V1")
      writer.name("observationPhase").value("PRE_READY_STATIC_OBJECTS_AND_DIRECT_GRANTS")
      writer.name("installationUuid").value(record.installationUuid().toString)
      writer.name("serverUuid").value(installer.identity().serverUuid.toString)
      writer.name("serverVersion").value(installer.identity().serverVersion)
      writer.name("schema").value(installer.identity().catalog)
      writer.name("installerEffectiveUser").value(installer.identity().effectiveUser)
      writer.name("recipeSha256").value(record.recipeSha256())
      writer.name("configurationIdentitySha256").value(record.configurationSha256())
      writer.name("staticResultSets")
      MySqlStaticBaselineEncoder.writeResultSets(writer, objects)
      writer.name("directPrivilegeResultSets")
      MySqlStaticBaselineEncoder.writeResultSets(writer, privileges)
      writer.endObject()
      writer.flush()
      installer.requireHeldInitializationLock()
      HexFormat.of().formatHex(digest.digest())
    }

  private final class BoundedOutput(destination: OutputStream) extends OutputStream:
    private var written = 0L
    private def count(bytes: Int): Unit =
      val next = Math.addExact(written, bytes.toLong)
      if next > ManagedInstallationFiles.MAX_BUNDLE_BYTES then reject("managed actual capture exceeds its file bound")
      written = next
    override def write(value: Int): Unit = { count(1); destination.write(value) }
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(offset, length, bytes.length)
      count(length)
      destination.write(bytes, offset, length)
    override def flush(): Unit = destination.flush()
  private def reject(message: String): Nothing = throw new SQLDataException(message)
