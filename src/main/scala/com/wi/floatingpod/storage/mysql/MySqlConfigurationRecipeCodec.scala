package com.wi.floatingpod.storage.mysql

import com.google.gson.Strictness
import com.google.gson.stream.{JsonReader, JsonToken}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.io.{ByteArrayInputStream, InputStreamReader, OutputStream}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.sql.SQLDataException
import java.util.HexFormat
import MySqlManifestConfigurationIdentity.*

/** Schema-specific recipe framing, not a second runtime contract/metadata semantic parser. */
private[mysql] object MySqlConfigurationRecipeCodec:
  private val Selector = "format3ManifestConfigurationEncoding"
  private val MetadataKeys = Vector("backend", "host", "port", "schema", "expectedServerUuid",
    "expectedBindingUuid", "expectedEffectiveUser", "username", "pkcs12TrustStorePath",
    "credentialSourceRef", "trustSecretSourceRef", "connectTimeoutMillis", "socketTimeoutMillis",
    "activeLimit", "innodbFlushLogAtTrxCommit", "syncBinlog", "approvedBaselinePath",
    "approvedBaselineFormat", "approvedSchemaFormat", "approvedCoverage", "expectedServerVersion",
    "expectedProvisioningUuid", "approvedDdlSha256", "expectedAuditDefiner", "format3RuntimeContractPath", Selector).sorted
  private val ContractKeys = Vector("format", Selector, "schemaFormat", "coverage", "serverUuid",
    "serverVersion", "schema", "effectiveUser", "connectedUser", "callerRoles", "provisioningUuid",
    "bindingUuid", "ddlSha256", "auditDefiner", "auditDefinerRoles", "callerGrants", "physicalFormat",
    "importState", "maxReceiptAgeNanos", "sourceProvenanceKind", "sourceProvenanceHex",
    "sourceProvenanceSha256", "limitProfileEncoding", "limitProfileHex", "limitProfileSha256", "definerPrivileges").sorted
  private val ColumnKeys = Vector("label", "jdbcType", "typeName", "precision", "scale", "nullable", "signed").sorted
  private val WrapperBytes = "{\"contract\":,\"format\":\"FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V2\",\"metadata\":}".length
  val MaxInputBytes = MySqlRuntimeMetadata.MAX_METADATA_BYTES + MySqlFormat3RuntimeContractLoader.MaxContractBytes + WrapperBytes

  /** This wrapper cannot be passed to the runtime projector or treated as validated P semantics. */
  final class Structure private[MySqlConfigurationRecipeCodec] (val candidate: CandidateDigest) extends AutoCloseable:
    override def close(): Unit = candidate.close()

  private def fail(message: String): Nothing = throw new SQLDataException(message)

  /** Strict canonical JSON scalar writer, shared by structural comparison and runtime projection. */
  private[mysql] def quoted(output: OutputStream, value: String): Unit =
    if value == null then fail("configuration text is null")
    output.write('"')
    var at = 0
    while at < value.length do
      val first = value.charAt(at)
      val cp =
        if Character.isHighSurrogate(first) then
          if at + 1 >= value.length || !Character.isLowSurrogate(value.charAt(at + 1)) then fail("invalid configuration Unicode surrogate")
          at += 1
          Character.toCodePoint(first, value.charAt(at))
        else if Character.isLowSurrogate(first) then fail("invalid configuration Unicode surrogate")
        else first.toInt
      if cp == '"' || cp == '\\' then
        output.write('\\'); output.write(cp)
      else if cp < 32 then
        output.write('\\'); output.write('u'); output.write('0'); output.write('0')
        val digits = "0123456789ABCDEF"
        output.write(digits.charAt(cp >>> 4)); output.write(digits.charAt(cp & 15))
      else if cp < 128 then output.write(cp)
      else if cp < 2048 then
        output.write(0xc0 | (cp >>> 6)); output.write(0x80 | (cp & 63))
      else if cp < 65536 then
        output.write(0xe0 | (cp >>> 12)); output.write(0x80 | ((cp >>> 6) & 63)); output.write(0x80 | (cp & 63))
      else
        output.write(0xf0 | (cp >>> 18)); output.write(0x80 | ((cp >>> 12) & 63))
        output.write(0x80 | ((cp >>> 6) & 63)); output.write(0x80 | (cp & 63))
      at += 1
    output.write('"')

  private[mysql] def ascii(output: OutputStream, value: String): Unit =
    value.foreach(c => output.write(c.toInt))

  /** Takes borrowed immutable input; no file read/pin or approval is implied. */
  def inspect(input: Array[Byte], root: MySqlFormat3ReadBudget): Structure = inspectVersion(input,root,false)

  def inspectV3(input: Array[Byte], root: MySqlFormat3ReadBudget): Structure = inspectVersion(input,root,true)

  private def inspectVersion(input: Array[Byte], root: MySqlFormat3ReadBudget, v3: Boolean): Structure =
    requireRoot(root)
    if input == null || input.length > MaxInputBytes then fail("recipe input exceeds existing combined metadata/contract bounds")
    // Worst-case live reader/StringBuilder/String storage is bounded by eight bytes per source byte.
    // The fixed allowance covers the JsonReader buffer, decoder, schema stack and digest streams.
    // V3 内嵌权限图按原typed解析64B/输入字节组预付，不能沿V2纯流式8B组。
    temporary(root, Math.addExact(32768L, Math.multiplyExact(input.length.toLong, if v3 then 64L else 8L))) {
      def replay(output: OutputStream): Unit =
        val decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
        val reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(input), decoder))
        reader.setStrictness(Strictness.STRICT)
        var primary: Throwable = null
        try
          new RecipeReader(reader, output,v3).read()
        catch
          case cause: Throwable => primary = cause; throw cause
        finally
          try reader.close()
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else if cleanup ne primary then primary.addSuppressed(cleanup)
      var position = 0
      val comparison = new OutputStream:
        override def write(value: Int): Unit =
          if position >= input.length || input(position) != value.toByte then fail("recipe is not exact canonical JSON")
          position += 1
      replay(comparison)
      if position != input.length then fail("recipe has trailing bytes")
      val value = digest(root, replay,if v3 then EncodingV3 else Encoding)
      try new Structure(value)
      catch
        case cause: Throwable =>
          try value.close()
          catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
          throw cause
    }

  /** V2 semantic digest only; no loaded-file approval, source issuer or runtime admission. */
  def parseSemanticV2(input: Array[Byte], policy: MySqlRuntimeAuditPolicy,
      root: MySqlFormat3ReadBudget): ManifestSha256 = parseSemanticVersion(input,policy,root,false)

  def parseSemanticV3(input: Array[Byte], policy: MySqlRuntimeAuditPolicy,
      root: MySqlFormat3ReadBudget): ManifestSha256 = parseSemanticVersion(input,policy,root,true)

  private def parseSemanticVersion(input: Array[Byte], policy: MySqlRuntimeAuditPolicy,
      root: MySqlFormat3ReadBudget, v3: Boolean): ManifestSha256 =
    requireRoot(root)
    if input == null || input.length > MaxInputBytes || policy == null then fail("bounded recipe, policy and root required")
    // Own the immutable input copy, UTF-16 section strings, metadata map/semantic fields,
    // reader/canonical scratch before constructing them. Contract's retained graph is separately charged.
    var result: ManifestSha256=null
    try temporary(root,Math.addExact(65536L,Math.multiplyExact(input.length.toLong,if v3 then 64L else 16L))) {
      val owned=input.clone()
      val structural=inspectVersion(owned,root,v3)
      val expectedDigest=structural.candidate.hex
      structural.close()
      val decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
      val reader=new JsonReader(new InputStreamReader(new ByteArrayInputStream(owned),decoder))
      reader.setStrictness(Strictness.STRICT)
      var primary: Throwable=null
      var semantic: MySqlFormat3RuntimeContractLoader.ContractSemantics=null
      try
        val scan=new RecipeReader(reader,OutputStream.nullOutputStream(),v3)
        scan.read()
        val contractText=new String(owned,scan.contractStart,scan.contractEnd-scan.contractStart,StandardCharsets.UTF_8)
        semantic=if v3 then MySqlFormat3RuntimeContractLoader.parseManifestV3Projection(contractText,root)
          else MySqlFormat3RuntimeContractLoader.parseManifestV2Projection(contractText,root)
        val metadataText=new String(owned,scan.metadataStart,scan.metadataEnd-scan.metadataStart,StandardCharsets.UTF_8)
        val metadataReader=new JsonReader(new java.io.StringReader(metadataText))
        metadataReader.setStrictness(Strictness.STRICT)
        val fields=new java.util.HashMap[String,String]()
        var metadataFailure: Throwable=null
        val deployment=try
          metadataReader.beginObject()
          while metadataReader.hasNext do fields.put(metadataReader.nextName(),metadataReader.nextString())
          metadataReader.endObject()
          if v3 then MySqlRuntimeMetadata.parseManifestV3Projection(fields)
          else MySqlRuntimeMetadata.parseManifestV2Projection(fields)
        catch
          case failure: Throwable => metadataFailure=failure;throw failure
        finally
          try metadataReader.close()
          catch case cleanup: Throwable =>
            if metadataFailure == null then throw cleanup
            else if cleanup ne metadataFailure then metadataFailure.addSuppressed(cleanup)
        result=fromValidatedSemantics(deployment,semantic,policy,root)
        if result.hex != expectedDigest then fail("semantic recipe differs from canonical normalized deployment")
        result
      catch
        case failure: Throwable => primary=failure;throw failure
      finally
        var cleanupFailure: Throwable=primary
        try if semantic != null then semantic.close()
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure=cleanup
          else if cleanup ne cleanupFailure then cleanupFailure.addSuppressed(cleanup)
        try reader.close()
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure=cleanup
          else if cleanup ne cleanupFailure then cleanupFailure.addSuppressed(cleanup)
        java.util.Arrays.fill(owned,0.toByte)
        if primary == null && cleanupFailure != null then throw cleanupFailure
    }
    catch
      case failure: Throwable =>
        if result != null then
          try result.close()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  /** Explicit fixed schemas; no arbitrary JSON tree, coercion, default or semantic authority. */
  private final class RecipeReader(reader: JsonReader, destination: OutputStream, v3: Boolean):
    private var position = 0L
    var metadataStart = 0
    var metadataEnd = 0
    var contractStart = 0
    var contractEnd = 0
    private var rows = 0
    private var cells = 0
    private var payload = 0L
    private var contractPayload = false
    private val output = new OutputStream:
      override def write(value: Int): Unit =
        position = Math.addExact(position, 1L)
        destination.write(value)

    private def obj(keys: Vector[String])(field: String => Unit): Unit =
      reader.beginObject(); output.write('{')
      var index = 0
      keys.foreach { expected =>
        if !reader.hasNext || reader.nextName() != expected then fail("recipe missing/unknown/duplicate/out-of-order field")
        if index != 0 then output.write(',')
        quoted(output, expected); output.write(':'); field(expected); index += 1
      }
      if reader.hasNext then fail("recipe object has extra fields")
      reader.endObject(); output.write('}')

    private def arr(maximum: Int)(element: Int => Unit): Int =
      reader.beginArray(); output.write('[')
      var index = 0
      while reader.hasNext do
        if index >= maximum then fail("recipe array exceeds existing bound")
        if index != 0 then output.write(',')
        element(index); index = Math.incrementExact(index)
      reader.endArray(); output.write(']')
      index

    private def text(maximum: Int = MySqlSchemaAuditor.MaxCellBytes, expected: String = null): String =
      if reader.peek() != JsonToken.STRING then fail("recipe string type required")
      val value = reader.nextString()
      val length = MySqlFormat3Utf8.byteLength(value)
      if length > maximum then fail("recipe text exceeds existing byte bound")
      if contractPayload then
        payload = Math.addExact(payload, length)
        if payload > MySqlFormat3RuntimeContractLoader.MaxContractBytes then fail("recipe decoded contract payload exceeds existing bound")
      if expected != null && value != expected then fail("recipe encoding/version is not supported")
      quoted(output, value)
      value

    private def hexText(maximum: Int, exactSha: Boolean = false): Unit =
      val value = text(Math.multiplyExact(maximum, 2))
      if value.length % 2 != 0 || !value.forall(c => c >= '0' && c <= '9' || c >= 'A' && c <= 'F') ||
        (exactSha && value.length != 64) then fail("recipe hexadecimal is not canonical or has wrong length")

    private def integer(wide: Boolean): Unit =
      if reader.peek() != JsonToken.NUMBER then fail("recipe integer type required")
      val value = reader.nextString()
      if value.length > 20 || !value.matches("0|-?[1-9][0-9]*") then fail("recipe integer is not canonical")
      if wide then java.lang.Long.parseLong(value) else java.lang.Integer.parseInt(value)
      ascii(output, value)

    def read(): Unit =
      obj(Vector("contract", "format", "metadata")) {
        case "format" => text(64, if v3 then RecipeFormatV3 else RecipeFormat)
        case "metadata" =>
          val start = position
          metadataStart = Math.toIntExact(start)
          obj(MetadataKeys) { key => text(MySqlRuntimeMetadata.MAX_METADATA_BYTES, if key == Selector then (if v3 then EncodingV3 else Encoding) else null) }
          metadataEnd = Math.toIntExact(position)
          if position - start > MySqlRuntimeMetadata.MAX_METADATA_BYTES then fail("recipe metadata exceeds 64KiB")
        case "contract" =>
          val start = position
          contractStart = Math.toIntExact(start)
          contractPayload = true
          obj(if v3 then ContractKeys.filterNot(_ == "callerGrants").appended("callerPrivileges").sorted else ContractKeys) {
            case "format" => text(64, if v3 then ContractFormatV3 else ContractFormat)
            case Selector => text(64, if v3 then EncodingV3 else Encoding)
            case "schemaFormat" | "physicalFormat" => integer(false)
            case "maxReceiptAgeNanos" => integer(true)
            case "callerGrants" =>
              if arr(2048)(_ => { text(); () }) == 0 then fail("recipe requires explicit caller grants")
            case "callerPrivileges" =>
              val value=MySqlRuntimeCallerPrivileges.read(reader)
              MySqlRuntimeCallerPrivileges.write(output,value)
            case "definerPrivileges" => privileges()
            case "sourceProvenanceHex" => hexText(1048576)
            case "sourceProvenanceSha256" | "limitProfileSha256" | "ddlSha256" => hexText(32, true)
            case "limitProfileHex" => hexText(512)
            case "limitProfileEncoding" => text(64, com.wi.floatingpod.config.SourceLimitProfile.ENCODING)
            case _ => text()
          }
          contractEnd = Math.toIntExact(position)
          contractPayload = false
          if position - start > MySqlFormat3RuntimeContractLoader.MaxContractBytes then fail("recipe contract exceeds 16MiB")
        case _ => fail("unsupported recipe root")
      }
      if reader.peek() != JsonToken.END_DOCUMENT then fail("recipe has trailing JSON content")

    private def privileges(): Unit =
      var setCount = 0
      arr(4) { setIndex =>
        var columnCount = 0
        obj(Vector("columns", "id", "rows")) {
          case "id" => text(expected = MySqlAuditCoverage.Format3PrivilegeResultSetIds(setIndex))
          case "columns" =>
            columnCount = arr(64) { _ =>
            obj(ColumnKeys) {
              case "label" | "typeName" => text(256)
              case "signed" =>
                if reader.peek() != JsonToken.BOOLEAN then fail("recipe boolean type required")
                ascii(output, if reader.nextBoolean() then "true" else "false")
              case _ => integer(false)
            }
            }
            if columnCount == 0 then fail("recipe privilege columns are empty")
          case "rows" => arr(20000) { _ =>
            rows = Math.incrementExact(rows)
            if rows > 20000 then fail("recipe total privilege rows exceed existing contract bound")
            val width = arr(columnCount) { _ =>
              cells = Math.incrementExact(cells)
              if cells > 200000 then fail("recipe total privilege cells exceed existing contract bound")
              obj(Vector("tag", "value")) {
                case "tag" => text(32, "TEXT")
                case "value" => text()
                case _ => fail("unsupported privilege cell")
              }
            }
            if width != columnCount then fail("recipe privilege row width differs from columns")
          }
          case _ => fail("unsupported privilege set")
        }
        setCount += 1
      }
      if setCount != 4 then fail("recipe requires four ordered privilege sets")
