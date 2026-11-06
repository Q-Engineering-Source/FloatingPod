package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import java.io.OutputStream
import java.sql.SQLDataException
import java.util.HexFormat
import java.security.MessageDigest
import scala.jdk.CollectionConverters.*
import MySqlManifestConfigurationIdentity.*
import MySqlConfigurationRecipeCodec.{ascii, quoted}

/** Pure proposed V2 DATA from current typed inputs. No selector, C issuer or admission is fabricated. */
private[mysql] object MySqlCurrentConfigurationProjection:
  final class CurrentRuntimeProjection private[MySqlCurrentConfigurationProjection] (
      val candidate: CandidateDigest
  ) extends AutoCloseable:
    override def close(): Unit = candidate.close()

  /**
    * The present metadata/K are V1 and lack the actual V2 selector. This method therefore returns
    * a proposal type, never ManifestSha256. K/B/R pins are checked here but excluded from C DATA.
    * Integration must add the approved selector and shared pin-free semantic parser before issuing C.
    */
  def proposeV2FromCurrent(metadata: MySqlRuntimeMetadata, contract: MySqlFormat3RuntimeContract,
      policy: MySqlRuntimeAuditPolicy, inputs: MySqlFormat3AuditInputs,
      root: MySqlFormat3ReadBudget): CurrentRuntimeProjection =
    requireRoot(root)
    if metadata == null || contract == null || policy == null || inputs == null ||
      (inputs.limitProfile ne root.profile) || (contract.limitProfile ne root.profile) then
      throw new IllegalArgumentException("actual metadata/contract/policy/inputs and exact original root profile required")
    val b = metadata.baseline()
    val id = contract.identity
    if b.schemaFormat() != 3 || b.coverage() != MySqlAuditCoverage.Format3 ||
      id.format != b.format() || id.schemaFormat != b.schemaFormat() || id.coverage != b.coverage() ||
      id.serverUuid != metadata.expectedServerUuid() || id.bindingUuid != metadata.expectedBindingUuid() ||
      id.schema != metadata.schema() || id.serverVersion != b.serverVersion() ||
      id.provisioningUuid != b.provisioningUuid() || !id.ddlSha256.equalsIgnoreCase(b.ddlSha256()) ||
      id.auditDefiner != b.auditDefiner() || !id.sourceCaptureSha256.equalsIgnoreCase(b.sourceCaptureSha256()) ||
      !contract.approvedBaselineSha256.equalsIgnoreCase(b.fileSha256()) ||
      contract.effectiveUser != metadata.expectedEffectiveUser() || metadata.format3RuntimeContract().isEmpty then
      throw new SQLDataException("current projection metadata and independently loaded contract disagree")
    if contract.callerPrivileges != null then
      throw new SQLDataException("V3 semantic contract cannot enter the legacy V2 proposal")
    contract.requirePolicy(policy)
    // Covers cloned provenance, its hex, source/profile digests, validation input wrappers,
    // fixed metadata maps and count/digest streams. Original typed inputs remain caller-owned.
    var validationWorkspace = 0L
    var setIndex = 0
    while setIndex < inputs.definerPrivileges.size do
      val set = inputs.definerPrivileges(setIndex)
      validationWorkspace = Math.addExact(validationWorkspace, Math.multiplyExact(set.columns.size.toLong, 128L))
      var rowIndex = 0
      while rowIndex < set.rows.size do
        validationWorkspace = Math.addExact(validationWorkspace,
          Math.addExact(256L, Math.multiplyExact(set.rows(rowIndex).cells.size.toLong, 64L)))
        rowIndex += 1
      setIndex += 1
    val workspace = Math.addExact(validationWorkspace, Math.addExact(65536L + 8L * MySqlRuntimeMetadata.MAX_METADATA_BYTES,
      Math.multiplyExact(inputs.sourceProvenance.length.toLong, 16L)))
    temporary(root, workspace) {
      val expected = contract.auditInputs(inputs.configurationSha256.bytes)
      if inputs.serverUuid != expected.serverUuid || inputs.serverVersion != expected.serverVersion ||
        inputs.schema != expected.schema || inputs.effectiveUser != expected.effectiveUser ||
        inputs.connectedUser != expected.connectedUser || inputs.callerRoles != expected.callerRoles ||
        inputs.provisioningUuid != expected.provisioningUuid || inputs.bindingUuid != expected.bindingUuid ||
        !inputs.ddlSha256.equalsIgnoreCase(expected.ddlSha256) || inputs.auditDefiner != expected.auditDefiner ||
        inputs.callerGrants != expected.callerGrants || inputs.definerPrivileges != expected.definerPrivileges ||
        inputs.sourceProvenanceKind != expected.sourceProvenanceKind ||
        inputs.sourceProvenance != expected.sourceProvenance || inputs.sourceProvenanceSha256 != expected.sourceProvenanceSha256 then
        throw new SQLDataException("current projection inputs differ from exact contract semantics")
      val source = contract.sourceProvenanceBytes
      val sourceSha = contract.sourceProvenanceSha256
      val profileBytes = root.profile.canonicalBytes()
      val profileSha = root.profile.sha256()
      val hex = HexFormat.of().withUpperCase()
      val metadataFields = Map(
        "backend" -> "mysql", "host" -> metadata.host(), "port" -> metadata.port().toString,
        "schema" -> metadata.schema(), "expectedServerUuid" -> metadata.expectedServerUuid().toString,
        "expectedBindingUuid" -> metadata.expectedBindingUuid().toString,
        "expectedEffectiveUser" -> metadata.expectedEffectiveUser(), "username" -> metadata.username(),
        "pkcs12TrustStorePath" -> metadata.pkcs12TrustStorePath().toString,
        "credentialSourceRef" -> ("file:" + metadata.credentialSourceRef().absolutePath()),
        "trustSecretSourceRef" -> ("file:" + metadata.trustSecretSourceRef().absolutePath()),
        "connectTimeoutMillis" -> metadata.connectTimeout().toMillis.toString,
        "socketTimeoutMillis" -> metadata.socketTimeout().toMillis.toString,
        "activeLimit" -> metadata.activeLimit().toString,
        "innodbFlushLogAtTrxCommit" -> metadata.innodbFlushLogAtTrxCommit().toString,
        "syncBinlog" -> metadata.syncBinlog().toString,
        "approvedBaselinePath" -> b.absolutePath().toString, "approvedBaselineFormat" -> b.format(),
        "approvedSchemaFormat" -> b.schemaFormat().toString, "approvedCoverage" -> b.coverage(),
        "expectedServerVersion" -> b.serverVersion(), "expectedProvisioningUuid" -> b.provisioningUuid().toString,
        "approvedDdlSha256" -> b.ddlSha256().toUpperCase(java.util.Locale.ROOT),
        "expectedAuditDefiner" -> b.auditDefiner(),
        "format3RuntimeContractPath" -> metadata.format3RuntimeContract().get().absolutePath().toString,
        "format3ManifestConfigurationEncoding" -> Encoding)
      val contractFields = Map(
        "format" -> ContractFormat, "format3ManifestConfigurationEncoding" -> Encoding,
        "coverage" -> id.coverage, "serverUuid" -> id.serverUuid.toString,
        "serverVersion" -> id.serverVersion, "schema" -> id.schema,
        "effectiveUser" -> contract.effectiveUser, "connectedUser" -> contract.connectedUser,
        "callerRoles" -> contract.callerRoles, "provisioningUuid" -> id.provisioningUuid.toString,
        "bindingUuid" -> id.bindingUuid.toString, "ddlSha256" -> id.ddlSha256.toUpperCase(java.util.Locale.ROOT),
        "auditDefiner" -> id.auditDefiner, "auditDefinerRoles" -> contract.auditDefinerRoles,
        "importState" -> "NONE", "sourceProvenanceKind" -> contract.sourceProvenanceKind,
        "sourceProvenanceHex" -> hex.formatHex(source), "sourceProvenanceSha256" -> hex.formatHex(sourceSha),
        "limitProfileEncoding" -> SourceLimitProfile.ENCODING, "limitProfileHex" -> hex.formatHex(profileBytes),
        "limitProfileSha256" -> hex.formatHex(profileSha))

      def write(output: OutputStream): Unit =
        writeCanonical(output,metadataFields,contractFields,policy.maxReceiptAge.toNanos,contract.callerGrants,expected.definerPrivileges)
      val value = digest(root, write)
      try new CurrentRuntimeProjection(value)
      catch
        case cause: Throwable =>
          try value.close()
          catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
          throw cause
    }

  private[mysql] def writeCanonical(destination: OutputStream, metadataFields: Map[String,String],
      contractFields: Map[String,String], maxAgeNanos: Long, grants: Vector[String],
      privileges: Vector[MySqlAuditResultSet], callerPrivileges: MySqlRuntimeCallerPrivileges.Value = null): Unit =
    def objectFields(output: OutputStream, keys: Vector[String])(field: String => Unit): Unit =
      output.write('{')
      var index = 0
      while index < keys.size do
        val key = keys(index)
        if index > 0 then output.write(',')
        quoted(output, key); output.write(':'); field(key)
        index += 1
      output.write('}')
    def array[A](output: OutputStream, values: Vector[A])(element: A => Unit): Unit =
      output.write('[')
      var index = 0
      while index < values.size do
        if index > 0 then output.write(',')
        element(values(index))
        index += 1
      output.write(']')
    def emit(destination: OutputStream): Unit =
      var position = 0L
      val output = new OutputStream:
        override def write(value: Int): Unit =
          position = Math.addExact(position, 1L)
          if position > MySqlConfigurationRecipeCodec.MaxInputBytes then
            throw new SQLDataException("proposed runtime recipe exceeds combined original bounds")
          destination.write(value)
      objectFields(output, Vector("contract", "format", "metadata")) {
        case "format" => quoted(output, if callerPrivileges == null then RecipeFormat else RecipeFormatV3)
        case "metadata" =>
          val start = position
          objectFields(output, metadataFields.keys.toVector.sorted)(key => quoted(output, metadataFields(key)))
          if position - start > MySqlRuntimeMetadata.MAX_METADATA_BYTES then
            throw new SQLDataException("proposed runtime metadata exceeds original 64KiB bound")
        case "contract" =>
          val start = position
          val keys = (contractFields.keys ++ Vector("schemaFormat", "physicalFormat", "maxReceiptAgeNanos", if callerPrivileges == null then "callerGrants" else "callerPrivileges", "definerPrivileges")).toVector.sorted
          objectFields(output, keys) {
            case "schemaFormat" => ascii(output, "3")
            case "physicalFormat" => ascii(output, "2")
            case "maxReceiptAgeNanos" => ascii(output, maxAgeNanos.toString)
            case "callerGrants" => array(output, grants)(quoted(output, _))
            case "callerPrivileges" => MySqlRuntimeCallerPrivileges.write(output,callerPrivileges)
            case "definerPrivileges" => array(output, privileges) { set =>
              objectFields(output, Vector("columns", "id", "rows")) {
                case "id" => quoted(output, set.id)
                case "columns" => array(output, set.columns) { column =>
                  objectFields(output, Vector("jdbcType", "label", "nullable", "precision", "scale", "signed", "typeName")) {
                    case "jdbcType" => ascii(output, column.jdbcType.toString)
                    case "label" => quoted(output, column.label)
                    case "nullable" => ascii(output, column.nullable.toString)
                    case "precision" => ascii(output, column.precision.toString)
                    case "scale" => ascii(output, column.scale.toString)
                    case "signed" => ascii(output, column.signed.toString)
                    case "typeName" => quoted(output, column.typeName)
                    case _ => throw new SQLDataException("unsupported fixed privilege column")
                  }
                }
                case "rows" => array(output, set.rows) { row => array(output, row.cells) {
                  case MySqlAuditText(value) => objectFields(output, Vector("tag", "value")) {
                    case "tag" => quoted(output, "TEXT")
                    case "value" => quoted(output, value)
                    case _ => throw new SQLDataException("unsupported fixed privilege cell")
                  }
                  case _ => throw new SQLDataException("current contract privileges must remain exact TEXT")
                } }
                case _ => throw new SQLDataException("unsupported fixed privilege set")
              }
            }
            case key => quoted(output, contractFields(key))
          }
          if position - start > MySqlFormat3RuntimeContractLoader.MaxContractBytes then
            throw new SQLDataException("proposed runtime contract exceeds original 16MiB bound")
        case _ => throw new SQLDataException("unsupported fixed recipe root")
      }
    emit(destination)

  /** 完整 schema5 C 投影；三个后向文件 pin 已由真实装配加载验证，在此不形成环。 */
  private[mysql] def writeDeploymentReceiptCanonical(out: OutputStream, metadata: Map[String, String],
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy): Unit =
    require(contract.family == MySqlDeploymentReceiptRuntimeContract.DeploymentFamily, "schema5 C requires schema5 K")
    writeStaticCanonical(out, metadata, contract, policy)

  private[mysql] def writeDbFirstCanonical(out: OutputStream, metadata: Map[String, String],
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy): Unit =
    require(contract.family == MySqlDeploymentReceiptRuntimeContract.DbFirstFamily, "schema6 C requires schema6 K")
    writeStaticCanonical(out, metadata, contract.writeSemantic, policy)

  /**
   * Computes the same pin-free schema6 C bytes before a B/K file exists. The result is digest DATA only;
   * it is deliberately not a ManifestSha256, DbFirstBinding, admission or writer capability.
   */
  private[mysql] def managedDbFirstDigest(metadata: MySqlRuntimeMetadata.DeploymentSemantics,
      contract: MySqlDeploymentReceiptRuntimeContract.ManagedSemantic,
      policy: MySqlRuntimeAuditPolicy): Array[Byte] =
    if metadata == null || contract == null || policy == null then
      throw new SQLDataException("managed schema6 metadata, semantic K DATA and policy are required")
    val root = contract.root
    requireRoot(root)
    val fields = metadata.canonicalFields().asScala.toMap
    def same(metadataKey: String, contractKey: String): Unit =
      if fields.get(metadataKey) != Some(contract.text(contractKey)) then
        throw new SQLDataException("managed schema6 metadata and recipe differ: " + metadataKey)
    if fields.get("dbFirstManifestConfigurationEncoding") != Some(MySqlRuntimeMetadata.DB_FIRST_MANIFEST) ||
      fields.get("approvedSchemaFormat") != Some("6") ||
      fields.get("approvedCoverage") != Some(MySqlDbFirstSchemaDefinition.Coverage) then
      throw new SQLDataException("managed schema6 metadata selector/profile differs")
    Vector("expectedServerUuid" -> "serverUuid", "expectedBindingUuid" -> "bindingUuid",
      "expectedProvisioningUuid" -> "provisioningUuid", "expectedServerVersion" -> "serverVersion",
      "schema" -> "schema", "expectedAuditDefiner" -> "auditDefiner", "expectedEffectiveUser" -> "effectiveUser").foreach {
      (metadataKey, contractKey) => same(metadataKey, contractKey)
    }
    val ddl = fields.getOrElse("approvedDdlSha256", throw new SQLDataException("managed schema6 DDL identity is required"))
    if !ddl.equalsIgnoreCase(contract.rendered.sha256) then throw new SQLDataException("managed schema6 metadata DDL differs from final render")
    if policy.callerRoles != contract.text("callerRoles") || policy.auditDefinerRoles != contract.text("auditDefinerRoles") ||
      policy.maxReceiptAge.toNanos != contract.long("maxReceiptAgeNanos") ||
      policy.expectedImport != MySqlExpectedNoImport || policy.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
      (contract.profile.sourceLimitProfile ne root.profile) then
      throw new SQLDataException("managed schema6 policy/profile differs from semantic K")
    val fieldWorkspace = fields.iterator.foldLeft(65536L) { case (sum, (key, value)) =>
      Math.addExact(sum, 256L + 16L * (key.length.toLong + value.length.toLong))
    }
    val workspace = Math.addExact(fieldWorkspace, contract.canonicalWorkspaceBytes)
    MySqlManifestConfigurationIdentity.temporary(root, workspace) {
      val candidate = MySqlManifestConfigurationIdentity.digest(root,
        out => writeStaticCanonical(out, fields, contract.writeSemantic, policy),
        MySqlDbFirstRuntimeContract.Encoding)
      var primary: Throwable = null
      try HexFormat.of().parseHex(candidate.hex)
      catch
        case failure: Throwable => primary = failure; throw failure
      finally
        try candidate.close()
        catch
          case cleanup: Throwable =>
            if primary != null then primary.addSuppressed(cleanup)
            else throw cleanup
    }

  private def writeStaticCanonical(out: OutputStream, metadata: Map[String, String],
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy): Unit =
    writeStaticCanonical(out, metadata, contract.writeSemantic, policy)

  private def writeStaticCanonical(out: OutputStream, metadata: Map[String, String],
      writeContract: OutputStream => Unit, policy: MySqlRuntimeAuditPolicy): Unit =
    import com.google.gson.{JsonObject, JsonPrimitive}
    val m = new JsonObject()
    metadata.foreach { (k, v) => m.addProperty(k, v) }
    val p = new JsonObject()
    p.addProperty("callerRoles", policy.callerRoles)
    p.addProperty("auditDefinerRoles", policy.auditDefinerRoles)
    p.addProperty("physicalFormat", 2)
    p.addProperty("importState", "NONE")
    p.addProperty("maxReceiptAgeNanos", policy.maxReceiptAge.toNanos)
    out.write("{\"contract\":".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    writeContract(out)
    out.write(",\"metadata\":".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    MySqlDeploymentReceiptRuntimeContract.writeCanonical(m, out)
    out.write(",\"policy\":".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    MySqlDeploymentReceiptRuntimeContract.writeCanonical(p, out)
    out.write('}'.toInt)
