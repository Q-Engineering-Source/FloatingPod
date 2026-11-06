package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{ManagedBootstrapPolicy, ManagedDatabaseConfiguration}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.{Arrays, Collections, Map as JMap, TreeMap}
import scala.jdk.CollectionConverters.*

/** 由已确认的普通配置与发布身份生成 schema6 pin-free metadata DATA。 */
object MySqlManagedMetadataProducer:
  private val MaxMetadataBytes = MySqlRuntimeMetadata.MAX_METADATA_BYTES
  private val MaximumConnectTimeoutMillis = 30_000L
  private val MaximumSocketTimeoutMillis = 300_000L
  private val HexDigest = "(?i)[0-9a-f]{64}"
  private val PinKeys = Set("approvedBaselineSha256", "approvedSourceCaptureSha256", "dbFirstRuntimeContractSha256")

  /** 调用方从最终 B、B capture 与 K 字节计算的 pin；本类型本身不读取文件。 */
  final class Pins private (val approvedBaselineSha256: String,
      val approvedSourceCaptureSha256: String, val dbFirstRuntimeContractSha256: String):
    override def toString: String = "MySqlManagedMetadataPins[DATA; not file-verified]"

  object Pins:
    def apply(approvedBaselineSha256: String, approvedSourceCaptureSha256: String,
        dbFirstRuntimeContractSha256: String): Pins =
      requireDigest(approvedBaselineSha256, "approved B SHA-256")
      requireDigest(approvedSourceCaptureSha256, "approved B source-capture SHA-256")
      requireDigest(dbFirstRuntimeContractSha256, "schema6 K SHA-256")
      new Pins(approvedBaselineSha256.toUpperCase(java.util.Locale.ROOT),
        approvedSourceCaptureSha256.toUpperCase(java.util.Locale.ROOT),
        dbFirstRuntimeContractSha256.toUpperCase(java.util.Locale.ROOT))

  /** 持有闭合 projection 的 DATA；close 释放其在原始 root 中的 retained 预算。 */
  final class Projection private[MySqlManagedMetadataProducer] (
      private var values: JMap[String, String], private val root: MySqlFormat3ReadBudget,
      private val retainedFee: Long
  ) extends AutoCloseable:
    private var closed = false

    private def requireOpen(): Unit =
      if closed || values == null then fail("managed schema6 metadata projection is closed")
      MySqlManifestConfigurationIdentity.requireRoot(root)

    /** 返回不可变且严格无 B/K 文件 pin 的字段映射。 */
    def projectionFields: JMap[String, String] = synchronized {
      requireOpen()
      values
    }

    /** 序列化完整 schema6 metadata DATA；输出流归调用者所有，不会创建或发布文件。 */
    def serializeFull(pins: Pins): SerializedMetadata = synchronized {
      requireOpen()
      if pins == null then fail("actual B, source-capture and K SHA-256 pins are required")
      val workspace = propertyWorkspace(values, 256L)
      var issued: SerializedMetadata = null
      try {
        MySqlManifestConfigurationIdentity.temporary(root, workspace) {
        val full = new TreeMap[String, String](values)
        if PinKeys.exists(full.containsKey) then fail("pin-free projection unexpectedly contains a file pin")
        full.put("approvedBaselineSha256", pins.approvedBaselineSha256)
        full.put("approvedSourceCaptureSha256", pins.approvedSourceCaptureSha256)
        full.put("dbFirstRuntimeContractSha256", pins.dbFirstRuntimeContractSha256)
        val counter = new CountingOutputStream
        writeProperties(full, counter)
        if counter.length <= 0L || counter.length > MaxMetadataBytes then
          fail("complete schema6 metadata exceeds the existing 64 KiB file bound")
        val byteLength = Math.toIntExact(counter.length)
        val fee = Math.addExact(byteLength.toLong, 192L)
        root.reserveRetainedMemory(fee, "managed schema6 full metadata bytes BEFORE allocation")
        var bytes: Array[Byte] = null
        var published = false
        var primary: Throwable = null
        try
          bytes = new Array[Byte](byteLength)
          val exact = new ExactOutputStream(bytes)
          writeProperties(full, exact)
          exact.requireComplete()
          val parsed = MySqlRuntimeMetadata.fromDbFirstBytes(bytes)
          if parsed.completeCanonicalFields().asScala.toMap != full.asScala.toMap then
            fail("full schema6 metadata parser canonical round-trip differs")
          val result = new SerializedMetadata(bytes, root, fee)
          bytes = null
          published = true
          issued = result
          result
        catch
          case cause: Throwable => primary = cause; throw cause
        finally
          if !published then
            if bytes != null then Arrays.fill(bytes, 0.toByte)
            try root.releaseRetainedMemory(fee, "unpublished managed schema6 full metadata bytes")
            catch
              case cleanup: Throwable =>
                if primary == null then throw cleanup
                else if cleanup ne primary then primary.addSuppressed(cleanup)
        }
      } catch {
        case cause: Throwable =>
          if issued != null then
            try issued.close()
            catch
              case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
          throw cause
      }
    }

    override def close(): Unit = synchronized {
      if !closed then
        MySqlManifestConfigurationIdentity.requireRoot(root)
        root.releaseRetainedMemory(retainedFee, "managed schema6 metadata projection detached")
        values = null
        closed = true
    }

    override def toString: String = "ManagedSchema6MetadataProjection[DATA; no file approval or admission]"

  /** 序列化后的完整 metadata DATA；调用方必须在释放前写入或消费，然后 close。 */
  final class SerializedMetadata private[MySqlManagedMetadataProducer] (
      private var bytes: Array[Byte], private val root: MySqlFormat3ReadBudget,
      private val retainedFee: Long
  ) extends AutoCloseable:
    private var closed = false

    private def requireOpen(): Unit =
      if closed || bytes == null then fail("managed full metadata DATA is closed")
      MySqlManifestConfigurationIdentity.requireRoot(root)

    def length: Int = synchronized { requireOpen(); bytes.length }

    /** 仅写入调用方提供的流；不会关闭、刷新或缓存该流。 */
    def writeTo(destination: OutputStream): Unit = synchronized {
      requireOpen()
      if destination == null then fail("full metadata destination stream is required")
      destination.write(bytes)
    }

    override def close(): Unit = synchronized {
      if !closed then
        MySqlManifestConfigurationIdentity.requireRoot(root)
        root.releaseRetainedMemory(retainedFee, "managed full metadata DATA detached")
        Arrays.fill(bytes, 0.toByte)
        bytes = null
        closed = true
    }

    override def toString: String = "ManagedSchema6FullMetadata[DATA; no file approval or admission]"

  /**
    * 只从普通配置、安装记录和现有发布 profile 生成 schema6 projection。
    * stable B/K 路径作为绝对引用写入 metadata；本方法不读取或创建这些文件。
    */
  def create(root: MySqlFormat3ReadBudget, configuration: ManagedDatabaseConfiguration,
      identity: ManagedInstallationIdentity.Record, sourceLimitProfile: SourceLimitProfile,
      rendered: MySqlDbFirstSchemaProfile.Rendered, runtimeCredentialPath: Path,
      stableBaselinePath: Path, stableRuntimeContractPath: Path): Projection =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if configuration == null || identity == null || sourceLimitProfile == null || rendered == null then
      fail("managed configuration, installation identity, source profile and final schema6 render are required")
    if rendered.mode != MySqlDbFirstSchemaProfile.RenderMode.ExistingDatabaseOnly then
      fail("managed metadata requires the ExistingDatabaseOnly schema6 render")
    val profile = rendered.inputs
    if profile == null || (sourceLimitProfile ne root.profile) ||
      (profile.sourceLimitProfile ne sourceLimitProfile) then
      fail("managed metadata requires the exact source limit profile bound to the original root and render")
    if identity.schema != configuration.database() || identity.schema != profile.schema then
      fail("managed installation, ordinary configuration and rendered schema identities differ")
    if !identity.recipeSha256().equalsIgnoreCase(rendered.sha256) then
      fail("managed installation recipe digest differs from the actual ExistingDatabaseOnly render")
    if configuration.connectTimeoutMillis() > MaximumConnectTimeoutMillis ||
      configuration.socketTimeoutMillis() > MaximumSocketTimeoutMillis then
      fail("ordinary database timeout exceeds the closed schema6 metadata contract")

    var issued: Projection = null
    try {
      MySqlManifestConfigurationIdentity.temporary(root, 65_536L) {
      val instanceRoot = configuration.instanceRoot()
      val runtimeCredential = absolute(instanceRoot, runtimeCredentialPath, "runtime credential path")
      val baseline = absolute(instanceRoot, stableBaselinePath, "stable B path")
      val runtimeContract = absolute(instanceRoot, stableRuntimeContractPath, "stable K path")
      val trustStoreOption = configuration.trustStore()
      val trustPasswordOption = configuration.trustStorePasswordFile()
      if !trustStoreOption.isPresent || !trustPasswordOption.isPresent then
        fail("schema6 managed metadata requires an explicit PKCS12 trust store and its password file")
      val trustStore = trustStoreOption.get()
      val trustPassword = trustPasswordOption.get()
      val installerPasswordPath = configuration.passwordFile()
      if installerPasswordPath.isPresent && installerPasswordPath.get().toAbsolutePath.normalize() == runtimeCredential then
        fail("installer password file and runtime credential path must be distinct")
      val runtime = profile.accounts.runtime
      val audit = profile.accounts.auditDefiner

      val pairs = Vector(
        "backend" -> "mysql",
        "host" -> configuration.host(),
        "port" -> configuration.port().toString,
        "schema" -> configuration.database(),
        "expectedServerUuid" -> identity.serverUuid().toString,
        "expectedBindingUuid" -> identity.bindingUuid().toString,
        "expectedEffectiveUser" -> runtime.identity,
        "username" -> runtime.user,
        "pkcs12TrustStorePath" -> trustStore.toAbsolutePath.normalize().toString,
        "credentialSourceRef" -> s"file:${runtimeCredential}",
        "trustSecretSourceRef" -> s"file:${trustPassword.toAbsolutePath.normalize()}",
        "connectTimeoutMillis" -> configuration.connectTimeoutMillis().toString,
        "socketTimeoutMillis" -> configuration.socketTimeoutMillis().toString,
        "activeLimit" -> ManagedBootstrapPolicy.ACTIVE_LIMIT.toString,
        "innodbFlushLogAtTrxCommit" -> "1",
        "syncBinlog" -> "1",
        "approvedBaselinePath" -> baseline.toString,
        "approvedBaselineFormat" -> MySqlApprovedBaselineLoader.ApprovedFormat,
        "approvedSchemaFormat" -> "6",
        "approvedCoverage" -> MySqlRuntimeMetadata.DB_FIRST_COVERAGE,
        "expectedServerVersion" -> MySqlConnectionSource.SupportedServerVersion,
        "expectedProvisioningUuid" -> identity.provisioningUuid().toString,
        "approvedDdlSha256" -> rendered.sha256.toUpperCase(java.util.Locale.ROOT),
        "expectedAuditDefiner" -> audit.identity,
        "dbFirstRuntimeContractPath" -> runtimeContract.toString,
        "dbFirstManifestConfigurationEncoding" -> MySqlRuntimeMetadata.DB_FIRST_MANIFEST
      )

      val decodedSize = pairs.foldLeft(0L) { case (total, (key, value)) =>
        if key == null || value == null || !StandardCharsets.UTF_8.newEncoder().canEncode(value) then
          fail("schema6 metadata contains an invalid UTF-8 field")
        Math.addExact(total, Math.addExact(key.length.toLong, value.length.toLong))
      }
      val workspace = Math.addExact(65_536L, Math.multiplyExact(decodedSize, 48L))
      MySqlManifestConfigurationIdentity.temporary(root, workspace) {
        val mutable = new TreeMap[String, String]()
        pairs.foreach { case (key, value) => mutable.put(key, value) }
        if mutable.size != pairs.size || PinKeys.exists(mutable.containsKey) then
          fail("schema6 metadata projection contains duplicate fields or file pins")
        val unmodifiable = Collections.unmodifiableMap(mutable)
        val parsed = MySqlRuntimeMetadata.parseDbFirstManifestProjection(unmodifiable)
        if parsed.canonicalFields().asScala.toMap != unmodifiable.asScala.toMap then
          fail("schema6 metadata projection is not the parser's canonical closed field map")
        val escapedCount = new CountingOutputStream
        writeProperties(unmodifiable, escapedCount)
        if escapedCount.length > MaxMetadataBytes then
          fail("schema6 metadata projection exceeds the existing 64 KiB file bound")
        val retainedFee = mapRetainedFee(unmodifiable)
        root.reserveRetainedMemory(retainedFee, "managed schema6 metadata projection BEFORE publication")
        var published = false
        var primary: Throwable = null
        try
          val result = new Projection(unmodifiable, root, retainedFee)
          issued = result
          published = true
          result
        catch
          case cause: Throwable => primary = cause; throw cause
        finally
          if !published then
            try root.releaseRetainedMemory(retainedFee, "unpublished managed schema6 metadata projection")
            catch
              case cleanup: Throwable =>
                if primary == null then throw cleanup
                else if cleanup ne primary then primary.addSuppressed(cleanup)
      }
      }
    } catch {
      case cause: Throwable =>
        if issued != null then
          try issued.close()
          catch
            case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause
    }

  private def absolute(instanceRoot: Path, value: Path, field: String): Path =
    if instanceRoot == null || !instanceRoot.isAbsolute || value == null then
      throw new IllegalArgumentException(s"$field and absolute instance root are required")
    val resolved = (if value.isAbsolute then value else instanceRoot.resolve(value)).toAbsolutePath.normalize()
    if !resolved.isAbsolute then throw new IllegalArgumentException(s"$field must resolve to an absolute path")
    resolved

  private def requireDigest(value: String, field: String): Unit =
    if value == null || !value.matches(HexDigest) then
      throw new IllegalArgumentException(s"$field must be an explicit 32-byte hexadecimal pin")

  private def mapRetainedFee(values: JMap[String, String]): Long =
    values.asScala.iterator.foldLeft(512L) { case (total, (key, value)) =>
      Math.addExact(total, Math.addExact(160L, Math.multiplyExact(4L,
        Math.addExact(key.length.toLong, value.length.toLong))))
    }

  private def propertyWorkspace(values: JMap[String, String], extraChars: Long): Long =
    val chars = values.asScala.iterator.foldLeft(extraChars) { case (total, (key, value)) =>
      Math.addExact(total, Math.addExact(key.length.toLong, value.length.toLong))
    }
    Math.addExact(65_536L, Math.multiplyExact(chars, 48L))

  private def writeProperties(values: JMap[String, String], output: OutputStream): Unit =
    if values == null || output == null then fail("metadata fields and output stream are required")
    val sorted = new TreeMap[String, String](values)
    val entries = sorted.entrySet().iterator()
    while entries.hasNext do
      val entry = entries.next()
      writeUtf8(escapeProperty(entry.getKey), output)
      output.write('=')
      writeUtf8(escapeProperty(entry.getValue), output)
      output.write('\n')

  /** java.util.Properties 语法的无歧义 UTF-8 编码；所有分隔符和空白都显式转义。 */
  private def escapeProperty(value: String): String =
    if value == null || !StandardCharsets.UTF_8.newEncoder().canEncode(value) then
      fail("metadata property contains invalid UTF-8 text")
    val result = new StringBuilder(value.length)
    var index = 0
    while index < value.length do
      val current = value.charAt(index)
      current match
        case '\\' => result.append("\\\\")
        case '\t' => result.append("\\t")
        case '\n' => result.append("\\n")
        case '\r' => result.append("\\r")
        case '\f' => result.append("\\f")
        case ' ' | '#' | '!' | '=' | ':' => result.append('\\').append(current)
        case other if other < ' ' || other == 0x7f =>
          val hex = "0123456789ABCDEF"
          result.append("\\u")
          result.append(hex.charAt((other.toInt >>> 12) & 0xf))
          result.append(hex.charAt((other.toInt >>> 8) & 0xf))
          result.append(hex.charAt((other.toInt >>> 4) & 0xf))
          result.append(hex.charAt(other.toInt & 0xf))
        case other => result.append(other)
      index += 1
    result.result()

  private def writeUtf8(value: String, output: OutputStream): Unit =
    val bytes = value.getBytes(StandardCharsets.UTF_8)
    output.write(bytes)

  private final class CountingOutputStream extends OutputStream:
    var length = 0L
    override def write(value: Int): Unit = length = Math.addExact(length, 1L)
    override def write(bytes: Array[Byte], offset: Int, count: Int): Unit =
      if bytes == null || offset < 0 || count < 0 || offset > bytes.length - count then
        throw new IndexOutOfBoundsException("invalid metadata write slice")
      length = Math.addExact(length, count.toLong)

  private final class ExactOutputStream(private val target: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= target.length then fail("metadata serializer exceeded its counted byte length")
      target(offset) = value.toByte
      offset += 1
    override def write(bytes: Array[Byte], from: Int, count: Int): Unit =
      if bytes == null || from < 0 || count < 0 || from > bytes.length - count || count > target.length - offset then
        fail("metadata serializer exceeded its counted byte length")
      System.arraycopy(bytes, from, target, offset, count)
      offset += count
    def requireComplete(): Unit =
      if offset != target.length then fail("metadata serializer did not fill its counted byte length")

  private def fail(message: String): Nothing = throw new IllegalArgumentException(message)
