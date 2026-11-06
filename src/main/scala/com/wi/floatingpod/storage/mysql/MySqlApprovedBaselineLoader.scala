package com.wi.floatingpod.storage.mysql

import com.google.gson.Strictness
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.StringReader
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.sql.SQLNonTransientConnectionException
import java.util.HexFormat
import java.util.Locale
import java.util.UUID
import scala.collection.mutable

/** 固定 format 1 或 format 2 静态基线的显式身份合同，不从实际数据库观测推导。 */
final case class MySqlApprovedBaselineIdentity(
    format: String,
    schemaFormat: Int,
    coverage: String,
    serverUuid: UUID,
    serverVersion: String,
    schema: String,
    provisioningUuid: UUID,
    bindingUuid: UUID,
    ddlSha256: String,
    auditDefiner: String,
    sourceCaptureSha256: String
):
  require(format == MySqlApprovedBaselineLoader.ApprovedFormat, "approved baseline format is fixed")
  require(MySqlApprovedBaselineLoader.supportsProfile(schemaFormat, coverage), "approved baseline schema format and coverage pair is unsupported")

  require(serverUuid != null && provisioningUuid != null && bindingUuid != null, "approved baseline UUID identities are required")
  require(serverVersion == MySqlConnectionSource.SupportedServerVersion, "approved baseline server version is unsupported")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "approved baseline schema is malformed")
  require(ddlSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256), "approved baseline DDL SHA-256 is malformed")
  require(auditDefiner != null && auditDefiner.nonEmpty, "approved baseline audit definer is required")
  require(sourceCaptureSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(sourceCaptureSha256), "approved baseline source capture SHA-256 is malformed")

  override def toString: String = "MySqlApprovedBaselineIdentity[redacted]"

/** 已按独立固定文件 SHA-256 验证并经既有对象 manifest 校验的静态数据。 */
final class MySqlApprovedStaticBaseline private[mysql] (
    val identity: MySqlApprovedBaselineIdentity,
    val fileSha256: String,
    val objects: MySqlApprovedObjectManifest
):
  require(identity != null && fileSha256 != null && objects != null, "approved static baseline result is incomplete")
  override def toString: String = "MySqlApprovedStaticBaseline[static data only; no runtime qualification]"

/** 严格加载独立批准的静态基线；返回值不构成 Ready、schema qualification 或 writer receipt。 */
object MySqlApprovedBaselineLoader:
  val ApprovedFormat = "FP_MYSQL_APPROVED_STATIC_V1"
  val ApprovedSchemaFormat = 1
  val Format2SchemaFormat = 2
  val Format3SchemaFormat = 3
  val ScriptSchemaFormat = 4
  val DeploymentReceiptSchemaFormat = 5
  val DbFirstSchemaFormat = 6
  private[mysql] def supportsProfile(schemaFormat: Int, coverage: String): Boolean =
    (schemaFormat == ApprovedSchemaFormat && coverage == MySqlAuditCoverage.V4) ||
      (schemaFormat == Format2SchemaFormat && coverage == MySqlAuditCoverage.Format2) ||
      (schemaFormat == Format3SchemaFormat && coverage == MySqlAuditCoverage.Format3) ||
      (schemaFormat == ScriptSchemaFormat && coverage == MySqlScriptSchemaProfile.Coverage) ||
      (schemaFormat == DeploymentReceiptSchemaFormat && coverage == MySqlDeploymentReceiptSchemaProfile.Coverage) ||
      (schemaFormat == DbFirstSchemaFormat && coverage == MySqlDbFirstSchemaDefinition.Coverage)
  val MaxApprovedFileBytes = 16 * 1024 * 1024
  private val MaxJsonDepth = 6
  private val HexDigestLength = 64
  private val RootFields = Set(
    "format",
    "schemaFormat",
    "coverage",
    "serverUuid",
    "serverVersion",
    "schema",
    "provisioningUuid",
    "bindingUuid",
    "ddlSha256",
    "auditDefiner",
    "sourceCaptureSha256",
    "resultSets"
  )
  private val ResultSetFields = Set("id", "columns", "rows")
  private val ColumnFields = Set("label", "jdbcType", "typeName", "precision", "scale", "nullable", "signed")
  private val SqlNullCellFields = Set("tag")
  private val TextCellFields = Set("tag", "value")
  private val BinaryCellFields = Set("tag", "hex")
  private val IntegerPattern = "-?(0|[1-9][0-9]*)".r

  def load(
      path: Path,
      expectedSha256: String,
      expectedIdentity: MySqlApprovedBaselineIdentity
  ): MySqlApprovedStaticBaseline =
    if path == null then reject("approved static baseline path is required")
    if expectedIdentity == null then reject("approved static baseline identity contract is required")
    if expectedSha256 == null || !MySqlSchemaAuditor.Sha256Hex.matches(expectedSha256) then
      reject("approved static baseline expected SHA-256 is malformed")

    val bytes = readBoundedFile(path)
    val actualSha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
    if !actualSha256.equalsIgnoreCase(expectedSha256) then
      reject("approved static baseline file SHA-256 does not match its independent approval")

    val text = decodeStrictUtf8(bytes)
    val parsed = new StrictJsonParser(text).parse()
    if parsed.identity != expectedIdentity then
      reject("approved static baseline identity does not match the independent identity contract")

    val objects =
      try
        parsed.identity.schemaFormat match
          case MySqlApprovedBaselineLoader.ApprovedSchemaFormat => MySqlApprovedObjectManifest(parsed.resultSets)
          case MySqlApprovedBaselineLoader.Format2SchemaFormat => MySqlApprovedObjectManifest.forFormat2(parsed.resultSets)
          case MySqlApprovedBaselineLoader.ScriptSchemaFormat => MySqlApprovedObjectManifest.forScriptPublication1(parsed.resultSets)
          case MySqlApprovedBaselineLoader.Format3SchemaFormat => MySqlApprovedObjectManifest.forFormat3(parsed.resultSets)
          case MySqlApprovedBaselineLoader.DeploymentReceiptSchemaFormat => MySqlApprovedObjectManifest.forDeploymentReceipt1(parsed.resultSets)
          case MySqlApprovedBaselineLoader.DbFirstSchemaFormat => MySqlApprovedObjectManifest.forDbFirst1(parsed.resultSets)
          case _ => reject("approved static baseline schema format is unsupported")
      catch
        case failure: SQLNonTransientConnectionException => throw failure
        case failure: IllegalArgumentException =>
          reject("approved static baseline does not satisfy the existing object manifest contract", failure)

    new MySqlApprovedStaticBaseline(parsed.identity, actualSha256, objects)

  /** 显式 root-owned B；不签发 runtime/SQL 资格，也不关闭 caller root。 */
  private[mysql] final class OwnedBaseline private[MySqlApprovedBaselineLoader] (
      private var value: MySqlApprovedStaticBaseline,
      private val root: MySqlFormat3ReadBudget,
      private val graphFee: Long
  ) extends AutoCloseable:
    private var closed = false
    def baselineFor(expectedRoot: MySqlFormat3ReadBudget): MySqlApprovedStaticBaseline =
      if closed || value == null || root.isClosed || (root ne expectedRoot) then
        reject("owned baseline requires its exact open original root")
      value
    override def close(): Unit =
      if !closed then
        // 先断开 live graph，再结算自身债务；失败不签发 closed ACK。
        value = null
        root.releaseRetainedMemory(graphFee, "owned baseline graph dropped")
        closed = true
    override def toString: String = "OwnedBaseline[static DATA; no runtime admission]"

  /** 保持旧 load/parser 合同；新入口在每个分配阶段前预付自己的真实逻辑 custody。 */
  private[mysql] def loadOwned(path: Path, expectedSha256: String,
      expectedIdentity: MySqlApprovedBaselineIdentity, root: MySqlFormat3ReadBudget): OwnedBaseline =
    if root == null || root.isClosed || root.isScopedReadBudget then reject("exact open baseline workflow root required")
    if path == null || expectedIdentity == null then reject("explicit baseline path and independent identity required")
    if expectedSha256 == null || !MySqlSchemaAuditor.Sha256Hex.matches(expectedSha256) then
      reject("approved static baseline expected SHA-256 is malformed")
    var published: OwnedBaseline = null
    try ownedTemporary(root, 65536L, "baseline IO/SHA/parser fixed workspace") {
      val before = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      if before.isSymbolicLink || !before.isRegularFile || before.size() > MaxApprovedFileBytes then
        reject("owned baseline must be a bounded regular no-link file")
      val size = before.size()
      // Exact raw array, no BAOS growth/copy; the one-byte growth probe is covered by fixed workspace.
      ownedTemporary(root, Math.addExact(size, 64L), "baseline raw bytes") {
        val raw = new Array[Byte](Math.toIntExact(size))
        try
          readOwnedExact(path, before, raw)
          val actualSha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw))
          if !actualSha.equalsIgnoreCase(expectedSha256) then
            reject("approved static baseline file SHA-256 does not match its independent approval")
          // Decoder CharBuffer <=2*size; returned String backing <=2*size, separately retained through parsing.
          ownedTemporary(root, Math.addExact(64L, Math.multiplyExact(size, 2L)), "baseline decoded text") {
            var text: String = null
            try
              text = ownedTemporary(root, Math.addExact(4096L, Math.multiplyExact(size, 2L)), "baseline UTF8 decoder") {
                decodeStrictUtf8(raw)
              }
              val census = censusOwnedText(text)
              val graph = census.graphFee
              root.reserveRetainedMemory(graph, "baseline typed graph/builders BEFORE parse")
              var graphOwned = true
              var primary: Throwable = null
              try
                ownedTemporary(root, census.workspaceFee, "baseline parser/manifest workspace") {
                  val parsed = new StrictJsonParser(text).parse()
                  if parsed.identity != expectedIdentity then
                    reject("approved static baseline identity does not match the independent identity contract")
                  val objects = ownedManifest(parsed)
                  published = new OwnedBaseline(new MySqlApprovedStaticBaseline(parsed.identity, actualSha, objects), root, graph)
                  graphOwned = false
                  published
                }
              catch
                case failure: Throwable =>
                  primary = failure
                  throw failure
              finally
                if graphOwned then
                  try root.releaseRetainedMemory(graph, "unpublished baseline typed graph")
                  catch case cleanup: Throwable =>
                    if primary == null then throw cleanup
                    else if cleanup ne primary then primary.addSuppressed(cleanup)
            finally text = null
          }
        finally java.util.Arrays.fill(raw, 0.toByte)
      }
    }
    catch
      case failure: Throwable =>
        val primary = failure match
          case io: IOException => withCause("owned baseline file IO failed", io)
          case other => other
        if published != null then
          try published.close()
          catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
        throw primary

  private def readOwnedExact(path: Path, before: BasicFileAttributes, raw: Array[Byte]): Unit =
    val channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
    var primary: Throwable = null
    try
      if channel.size() != raw.length then reject("owned baseline size changed before read")
      val buffer = ByteBuffer.wrap(raw)
      while buffer.hasRemaining do
        if channel.read(buffer) < 0 then reject("owned baseline shrank during read")
      if channel.read(ByteBuffer.allocate(1)) != -1 || channel.size() != raw.length then
        reject("owned baseline grew during read")
      val after = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      if after.isSymbolicLink || !after.isRegularFile || after.size() != before.size() ||
        after.lastModifiedTime() != before.lastModifiedTime() || !java.util.Objects.equals(after.fileKey(), before.fileKey()) then
        reject("owned baseline file identity changed during read")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try channel.close()
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def ownedManifest(parsed: ParsedBaseline): MySqlApprovedObjectManifest =
    try parsed.identity.schemaFormat match
      case ApprovedSchemaFormat => MySqlApprovedObjectManifest(parsed.resultSets)
      case Format2SchemaFormat => MySqlApprovedObjectManifest.forFormat2(parsed.resultSets)
      case Format3SchemaFormat => MySqlApprovedObjectManifest.forFormat3(parsed.resultSets)
      case ScriptSchemaFormat => MySqlApprovedObjectManifest.forScriptPublication1(parsed.resultSets)
      case DeploymentReceiptSchemaFormat => MySqlApprovedObjectManifest.forDeploymentReceipt1(parsed.resultSets)
      case DbFirstSchemaFormat => MySqlApprovedObjectManifest.forDbFirst1(parsed.resultSets)
      case _ => reject("approved static baseline schema format is unsupported")
    catch
      case failure: SQLNonTransientConnectionException => throw failure
      case failure: IllegalArgumentException => reject("approved static baseline does not satisfy the existing object manifest contract", failure)

  /** 无 JSON 实图：只计词法上界；语法、转义、深度与字段合同仍由原 parser 验证。 */
  private final case class OwnedCensus(objects: Long, arrays: Long, slots: Long, strings: Long, chars: Long, maxString: Long):
    // 每object64涵盖cell/column/resultset节点；每array128、每slot16涵盖builder+Vector枝/引用。
    // 原parser最多MaxJsonDepth(6)层、每层<=12fields的小seen-set归固定64KiB临时workspace。
    // 全部quoted文本(含NAME/tag/hex)按String48+UTF16计；binary decoded+immutable clone <=chars。
    // 固定2048涵盖identity/UUID/manifest/hash/result/owner及固定容器；不是heap测量或JVM容量。
    def graphFee: Long = add(2048L, mul(objects,64L), mul(arrays,128L), mul(slots,16L), mul(strings,48L), mul(chars,3L))
    // Gson nextName/nextString的builder增长+返回副本 <=6*最大raw scalar字符。
    // 原manifest依序foreach/map/distinct/tuple/hash-set按每row array512(数组总数为上界)。
    // selfdescription方法scope的definitionBytes与canonical strict UTF8 ByteBuffer+byte[]、
    // binary.bytes clone及摘要按最大scalar8；固定64KiB含reader/encoder和有界seen-set。
    def workspaceFee: Long = add(65536L, mul(maxString,14L), mul(arrays,512L))

  private def censusOwnedText(text: String): OwnedCensus =
    var objects=0L; var arrays=0L; var slots=1L; var strings=0L; var chars=0L; var maximum=0L
    var index=0
    while index < text.length do
      text.charAt(index) match
        case '"' =>
          strings=Math.addExact(strings,1L)
          index+=1
          val start=index
          var ended=false
          while index < text.length && !ended do
            if text.charAt(index) == '\\' then index=Math.min(text.length,index+2)
            else if text.charAt(index) == '"' then ended=true
            else index+=1
          val length=(index-start).toLong
          chars=Math.addExact(chars,length); maximum=Math.max(maximum,length)
        case '{' => objects=Math.addExact(objects,1L)
        case '[' => arrays=Math.addExact(arrays,1L)
        case ',' | ':' => slots=Math.addExact(slots,1L)
        case c if !Character.isWhitespace(c) && c != '}' && c != ']' =>
          val start=index
          while index < text.length && !Character.isWhitespace(text.charAt(index)) &&
              "{}[],:\"".indexOf(text.charAt(index)) < 0 do index+=1
          maximum=Math.max(maximum,(index-start).toLong)
          index-=1
        case _ => ()
      index+=1
    OwnedCensus(objects,arrays,slots,strings,chars,maximum)

  private def add(values: Long*): Long = values.foldLeft(0L)(Math.addExact)
  private def mul(left: Long, right: Long): Long = Math.multiplyExact(left,right)
  private def ownedTemporary[A](root: MySqlFormat3ReadBudget, fee: Long, context: String)(body: => A): A =
    root.reserveTemporaryMemory(fee,context+" BEFORE allocation")
    var primary: Throwable=null
    try body
    catch
      case failure: Throwable =>
        primary=failure
        throw failure
    finally
      try root.releaseTemporaryMemory(fee,context+" dropped")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def readBoundedFile(path: Path): Array[Byte] =
    val attributes =
      try Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      catch
        case failure: IOException => reject("approved static baseline file metadata could not be read", failure)

    if attributes.isSymbolicLink || !attributes.isRegularFile then
      reject("approved static baseline must be a regular file without a symbolic-link target")
    if attributes.size() > MaxApprovedFileBytes.toLong then
      reject("approved static baseline file exceeds the byte bound")

    val channel =
      try FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
      catch
        case failure: IOException => reject("approved static baseline file could not be opened", failure)

    var primary: Throwable = null
    try
      val initialCapacity = Math.max(32L, Math.min(attributes.size(), MaxApprovedFileBytes.toLong)).toInt
      val output = new ByteArrayOutputStream(initialCapacity)
      val buffer = ByteBuffer.allocate(8192)
      var finished = false
      while !finished do
        val count = channel.read(buffer)
        if count < 0 then finished = true
        else if count > 0 then
          buffer.flip()
          val nextSize = output.size().toLong + buffer.remaining().toLong
          if nextSize > MaxApprovedFileBytes.toLong then
            reject("approved static baseline file grew beyond the byte bound while reading")
          output.write(buffer.array(), buffer.position(), buffer.remaining())
          buffer.clear()
      output.toByteArray
    catch
      case failure: SQLNonTransientConnectionException =>
        primary = failure
        throw failure
      case failure: IOException =>
        val contextual = withCause("approved static baseline file read failed", failure)
        primary = contextual
        throw contextual
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary == null then
        try channel.close()
        catch
          case failure: IOException => throw withCause("approved static baseline file close failed", failure)
      else
        try channel.close()
        catch case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)

  private def decodeStrictUtf8(bytes: Array[Byte]): String =
    if bytes.length >= 3 && bytes(0) == 0xef.toByte && bytes(1) == 0xbb.toByte && bytes(2) == 0xbf.toByte then
      reject("approved static baseline must not contain a UTF-8 BOM")
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try decoder.decode(ByteBuffer.wrap(bytes)).toString
    catch
      case failure: CharacterCodingException => reject("approved static baseline is not strict UTF-8", failure)

  private final case class ParsedBaseline(
      identity: MySqlApprovedBaselineIdentity,
      resultSets: Vector[MySqlAuditResultSet]
  )

  private final class PayloadBudget:
    private var depth = 0
    private var totalRows = 0
    private var totalCells = 0
    private var totalPayloadBytes = 0L

    def enterDepth(): Unit =
      depth += 1
      if depth > MaxJsonDepth then reject("approved static baseline exceeds the JSON depth bound")

    def leaveDepth(): Unit =
      depth -= 1
      if depth < 0 then reject("approved static baseline parser depth became invalid")

    def addRow(): Unit =
      totalRows += 1
      if totalRows > MySqlSchemaAuditor.MaxTotalRows then reject("approved static baseline exceeds the total row bound")

    def addCell(): Unit =
      totalCells += 1
      if totalCells > MySqlSchemaAuditor.MaxTotalCells then reject("approved static baseline exceeds the total cell bound")

    def addText(value: String, context: String, perValueLimit: Int = MySqlSchemaAuditor.MaxCellBytes): Unit =
      val byteCount = MySqlSchemaAuditor.boundedUtf8ByteLength(value, perValueLimit, context)
      addPayloadBytes(byteCount.toLong, context)

    def addPayloadBytes(count: Long, context: String): Unit =
      if count < 0L then reject("approved static baseline payload byte count is invalid")
      totalPayloadBytes += count
      if totalPayloadBytes > MySqlSchemaAuditor.MaxTotalBytes.toLong then
        reject("approved static baseline exceeds the decoded payload byte bound")

  private final class StrictJsonParser(text: String):
    private val reader = new JsonReader(new StringReader(text))
    private val budget = new PayloadBudget()

    def parse(): ParsedBaseline =
      reader.setStrictness(Strictness.STRICT)
      var primary: Throwable = null
      try
        val parsed = readRoot()
        if reader.peek() != JsonToken.END_DOCUMENT then reject("approved static baseline has trailing JSON content")
        parsed
      catch
        case failure: SQLNonTransientConnectionException =>
          primary = failure
          throw failure
        case failure: IOException =>
          val contextual = withCause("approved static baseline JSON is malformed or truncated", failure)
          primary = contextual
          throw contextual
        case failure: IllegalStateException =>
          val contextual = withCause("approved static baseline JSON structure is invalid", failure)
          primary = contextual
          throw contextual
        case failure: IllegalArgumentException =>
          val contextual = withCause("approved static baseline contains an invalid typed value", failure)
          primary = contextual
          throw contextual
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        if primary == null then
          try reader.close()
          catch
            case failure: IOException => throw withCause("approved static baseline JSON reader close failed", failure)
        else
          try reader.close()
          catch case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)

    private def readRoot(): ParsedBaseline =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var format: String = null
      var schemaFormat = Int.MinValue
      var coverage: String = null
      var serverUuid: UUID = null
      var serverVersion: String = null
      var schema: String = null
      var provisioningUuid: UUID = null
      var bindingUuid: UUID = null
      var ddlSha256: String = null
      var auditDefiner: String = null
      var sourceCaptureSha256: String = null
      var resultSets: Vector[MySqlAuditResultSet] = null

      while reader.peek() != JsonToken.END_OBJECT do
        val name = fieldName(seen, RootFields, "approved baseline root")
        name match
          case "format" => format = textValue("format")
          case "schemaFormat" => schemaFormat = integerValue("schemaFormat")
          case "coverage" => coverage = textValue("coverage")
          case "serverUuid" => serverUuid = uuidValue("serverUuid")
          case "serverVersion" => serverVersion = textValue("serverVersion")
          case "schema" => schema = textValue("schema")
          case "provisioningUuid" => provisioningUuid = uuidValue("provisioningUuid")
          case "bindingUuid" => bindingUuid = uuidValue("bindingUuid")
          case "ddlSha256" => ddlSha256 = textValue("ddlSha256")
          case "auditDefiner" => auditDefiner = textValue("auditDefiner")
          case "sourceCaptureSha256" => sourceCaptureSha256 = textValue("sourceCaptureSha256")
          case "resultSets" => resultSets = readResultSets()
          case _ => reject("approved baseline root contains an unsupported field")

      endObject()
      requireFields(seen, RootFields, "approved baseline root")
      if format != MySqlApprovedBaselineLoader.ApprovedFormat then reject("approved static baseline format is unknown")
      if !MySqlApprovedBaselineLoader.supportsProfile(schemaFormat, coverage) then
        reject("approved static baseline schema format and coverage pair is unsupported")

      ParsedBaseline(
        MySqlApprovedBaselineIdentity(
          format,
          schemaFormat,
          coverage,
          serverUuid,
          serverVersion,
          schema,
          provisioningUuid,
          bindingUuid,
          ddlSha256,
          auditDefiner,
          sourceCaptureSha256
        ),
        resultSets
      )

    private def readResultSets(): Vector[MySqlAuditResultSet] =
      beginArray()
      val resultSets = Vector.newBuilder[MySqlAuditResultSet]
      var count = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if count >= MySqlAuditCoverage.StaticResultSetIds.size then
          reject("approved static baseline has too many resultsets")
        resultSets += readResultSet()
        count += 1
      endArray()
      resultSets.result()

    private def readResultSet(): MySqlAuditResultSet =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var id: String = null
      var columns: Vector[MySqlAuditColumn] = null
      var rows: Vector[MySqlAuditRow] = null

      while reader.peek() != JsonToken.END_OBJECT do
        val name = fieldName(seen, ResultSetFields, "static resultset")
        name match
          case "id" => id = textValue("resultset id")
          case "columns" => columns = readColumns()
          case "rows" => rows = readRows()
          case _ => reject("static resultset contains an unsupported field")

      endObject()
      requireFields(seen, ResultSetFields, "static resultset")
      val materializedColumns = columns
      val materializedRows = rows
      if materializedRows.exists(_.cells.size != materializedColumns.size) then
        reject("static resultset row cell count does not match its column count")
      MySqlAuditResultSet(id, materializedColumns, materializedRows)

    private def readColumns(): Vector[MySqlAuditColumn] =
      beginArray()
      val columns = Vector.newBuilder[MySqlAuditColumn]
      var count = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if count >= MySqlSchemaAuditor.MaxColumnsPerResultSet then
          reject("approved static resultset exceeds the column bound")
        columns += readColumn()
        count += 1
      endArray()
      columns.result()

    private def readColumn(): MySqlAuditColumn =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var label: String = null
      var jdbcType = Int.MinValue
      var typeName: String = null
      var precision = Int.MinValue
      var scale = Int.MinValue
      var nullable = Int.MinValue
      var signed: java.lang.Boolean = null

      while reader.peek() != JsonToken.END_OBJECT do
        val name = fieldName(seen, ColumnFields, "resultset column")
        name match
          case "label" => label = textValue("column label")
          case "jdbcType" => jdbcType = integerValue("column jdbcType")
          case "typeName" => typeName = textValue("column typeName")
          case "precision" => precision = integerValue("column precision")
          case "scale" => scale = integerValue("column scale")
          case "nullable" => nullable = integerValue("column nullable")
          case "signed" => signed = booleanValue("column signed")
          case _ => reject("resultset column contains an unsupported field")

      endObject()
      requireFields(seen, ColumnFields, "resultset column")
      MySqlAuditColumn(label, jdbcType, typeName, precision, scale, nullable, signed.booleanValue())

    private def readRows(): Vector[MySqlAuditRow] =
      beginArray()
      val rows = Vector.newBuilder[MySqlAuditRow]
      var rowsInSet = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if rowsInSet >= MySqlSchemaAuditor.MaxRowsPerResultSet then
          reject("approved static resultset exceeds the per-resultset row bound")
        budget.addRow()
        rows += readRow()
        rowsInSet += 1
      endArray()
      rows.result()

    private def readRow(): MySqlAuditRow =
      beginArray()
      val cells = Vector.newBuilder[MySqlAuditCell]
      var cellCount = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if cellCount >= MySqlSchemaAuditor.MaxColumnsPerResultSet then
          reject("approved static row exceeds the per-row cell bound")
        budget.addCell()
        cells += readCell()
        cellCount += 1
      endArray()
      MySqlAuditRow(cells.result())

    private def readCell(): MySqlAuditCell =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var tag: String = null
      var text: String = null
      var bytes: Array[Byte] = null

      while reader.peek() != JsonToken.END_OBJECT do
        val name =
          if reader.peek() != JsonToken.NAME then reject("typed audit cell property name is missing")
          reader.nextName()
        if !seen.add(name) then reject("typed audit cell contains a duplicate property")
        name match
          case "tag" => tag = textValue("typed audit cell tag", 32)
          case "value" => text = textValue("audit text cell")
          case "hex" => bytes = binaryValue()
          case _ => reject("typed audit cell contains an unsupported property")

      endObject()
      tag match
        case "SQL_NULL" =>
          requireFields(seen, SqlNullCellFields, "SQL NULL audit cell")
          MySqlAuditNull
        case "TEXT" =>
          requireFields(seen, TextCellFields, "text audit cell")
          MySqlAuditText(text)
        case "BINARY" =>
          requireFields(seen, BinaryCellFields, "binary audit cell")
          MySqlAuditBinary(bytes)
        case _ => reject("typed audit cell tag is unknown")

    private def binaryValue(): Array[Byte] =
      val encoded = rawStringValue("binary hex cell", MySqlSchemaAuditor.MaxCellBytes * 2)
      if (encoded.length & 1) != 0 then reject("binary audit cell hex has an odd number of characters")
      if !encoded.forall(isUpperHex) then reject("binary audit cell hex must use uppercase hexadecimal")
      val decoded =
        try HexFormat.of().parseHex(encoded)
        catch
          case failure: IllegalArgumentException => reject("binary audit cell hex is malformed", failure)
      if decoded.length > MySqlSchemaAuditor.MaxCellBytes then reject("binary audit cell exceeds the cell byte bound")
      budget.addPayloadBytes(decoded.length.toLong, "binary audit cell")
      decoded

    private def uuidValue(context: String): UUID =
      val value = textValue(context)
      try
        val uuid = UUID.fromString(value)
        if uuid.toString != value.toLowerCase(Locale.ROOT) then reject(context + " is not a canonical UUID")
        uuid
      catch
        case failure: IllegalArgumentException => reject(context + " is not a valid UUID", failure)

    private def integerValue(context: String): Int =
      if reader.peek() != JsonToken.NUMBER then reject(context + " must be an integer JSON number")
      val value = reader.nextString()
      if value.length > 11 || !IntegerPattern.matches(value) then reject(context + " is not an exact 32-bit integer")
      try java.lang.Integer.parseInt(value)
      catch
        case failure: NumberFormatException => reject(context + " exceeds the 32-bit integer range", failure)

    private def booleanValue(context: String): Boolean =
      if reader.peek() != JsonToken.BOOLEAN then reject(context + " must be a JSON boolean")
      reader.nextBoolean()

    private def textValue(context: String, maxBytes: Int = MySqlSchemaAuditor.MaxCellBytes): String =
      val value = rawStringValue(context, maxBytes)
      budget.addText(value, context, maxBytes)
      value

    private def rawStringValue(context: String, maxBytes: Int): String =
      if reader.peek() != JsonToken.STRING then reject(context + " must be a JSON string")
      val value = reader.nextString()
      MySqlSchemaAuditor.boundedUtf8ByteLength(value, maxBytes, context)
      value

    private def fieldName(seen: mutable.Set[String], allowed: Set[String], context: String): String =
      if reader.peek() != JsonToken.NAME then reject(context + " property name is missing")
      val name = reader.nextName()
      if !allowed.contains(name) then reject(context + " contains an unknown property")
      if !seen.add(name) then reject(context + " contains a duplicate property")
      name

    private def requireFields(seen: mutable.Set[String], expected: Set[String], context: String): Unit =
      if seen.toSet != expected then reject(context + " is missing required properties")

    private def beginObject(): Unit =
      if reader.peek() != JsonToken.BEGIN_OBJECT then reject("approved static baseline expected a JSON object")
      budget.enterDepth()
      reader.beginObject()

    private def endObject(): Unit =
      if reader.peek() != JsonToken.END_OBJECT then reject("approved static baseline JSON object is incomplete")
      reader.endObject()
      budget.leaveDepth()

    private def beginArray(): Unit =
      if reader.peek() != JsonToken.BEGIN_ARRAY then reject("approved static baseline expected a JSON array")
      budget.enterDepth()
      reader.beginArray()

    private def endArray(): Unit =
      if reader.peek() != JsonToken.END_ARRAY then reject("approved static baseline JSON array is incomplete")
      reader.endArray()
      budget.leaveDepth()

  private def isUpperHex(value: Char): Boolean =
    (value >= '0' && value <= '9') || (value >= 'A' && value <= 'F')

  private def withCause(message: String, cause: Throwable): SQLNonTransientConnectionException =
    val failure = new SQLNonTransientConnectionException(message)
    failure.initCause(cause)
    failure

  private def reject(message: String): Nothing =
    throw new SQLNonTransientConnectionException(message)

  private def reject(message: String, cause: Throwable): Nothing =
    throw withCause(message, cause)
