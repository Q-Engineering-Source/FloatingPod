package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonPrimitive, Strictness}
import com.google.gson.stream.{JsonReader, JsonToken}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import java.io.StringReader
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.file.{Files, LinkOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.Duration
import java.util.{Arrays, HexFormat, UUID}
import scala.jdk.CollectionConverters.*

/** 已批准文件的静态语义；没有 Session、native Plan 或运行资格转换。 */
private[mysql] object MySqlDeploymentReceiptRuntimeContract:
  val Format = "FP_MYSQL_DEPLOYMENT_RECEIPT1_RUNTIME_CONTRACT_V1"
  val Encoding = "FP_MYSQL_DEPLOYMENT_RECEIPT1_MANIFEST_CONFIGURATION_BE_V1"
  val RecipeEncoding = "FP_MYSQL_DEPLOYMENT_RECEIPT1_FRESH_RECIPE_V1"
  val MaxBytes = MySqlFormat3RuntimeContractLoader.MaxContractBytes
  private[mysql] final case class Family(schemaFormat: Int, format: String, encoding: String,
      recipeEncoding: String, coverage: String, manifestKey: String)
  private[mysql] val DeploymentFamily = Family(5, Format, Encoding, RecipeEncoding,
    MySqlDeploymentReceiptSchemaProfile.Coverage, "deploymentReceiptManifestConfigurationEncoding")
  private[mysql] val DbFirstFamily = Family(6, "FP_MYSQL_DB_FIRST1_RUNTIME_CONTRACT_V1",
    MySqlRuntimeMetadata.DB_FIRST_MANIFEST, "FP_MYSQL_DB_FIRST1_FRESH_RECIPE_V1",
    MySqlDbFirstSchemaDefinition.Coverage, "dbFirstManifestConfigurationEncoding")
  private[mysql] val ManagedDbFirstRecipeEncoding = "FP_MYSQL_DB_FIRST1_MANAGED_RECIPE_V1"
  private[mysql] final class Generated private[MySqlDeploymentReceiptRuntimeContract] (
      val inputs: MySqlDeploymentReceiptSchemaProfile.Inputs,
      val sha256: String, validate: (String, Vector[MySqlAuditResultSet]) => Unit,
      val managedExistingDatabase: Boolean = false):
    def validateSecurity(schema: String, privileges: Vector[MySqlAuditResultSet]): Unit = validate(schema, privileges)
  private def generate(profile: MySqlDeploymentReceiptSchemaProfile.Inputs, family: Family): Generated =
    if family == DeploymentFamily then
      val rendered = MySqlDeploymentReceiptSchemaProfile.render(profile)
      new Generated(profile, rendered.sha256, (schema, privileges) =>
        MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(profile, rendered, schema, privileges))
    else if family == DbFirstFamily then
      val rendered = MySqlDbFirstSchemaProfile.render(profile)
      new Generated(profile, rendered.sha256, (schema, privileges) =>
        MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(profile, rendered, schema, privileges))
    else fail("unsupported static runtime family")

  /** managed来源只选择发布代码的既有库配方，不接受caller自填hash或成功validator。 */
  private[mysql] def generateManagedDbFirst(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): Generated =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
    new Generated(profile, rendered.sha256, (schema, privileges) =>
      MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(profile, rendered, schema, privileges), true)
  private val fields = Set("format", "schemaFormat", "coverage", "approvedBaselineSha256", "serverUuid",
    "serverVersion", "schema", "effectiveUser", "connectedUser", "callerRoles", "provisioningUuid", "bindingUuid",
    "ddlSha256", "auditDefiner", "sourceCaptureSha256", "auditDefinerRoles", "callerGrants", "physicalFormat",
    "importState", "maxReceiptAgeNanos", "sourceProvenanceKind", "sourceProvenanceHex", "sourceProvenanceSha256",
    "limitProfileEncoding", "limitProfileHex", "limitProfileSha256", "definerPrivileges",
    "deploymentReceiptManifestConfigurationEncoding", "profile")
  private val recipeKeys = Vector("creationOperationUuid", "bootstrapOperationUuid", "prepareWriteUuid", "certifyWriteUuid",
    "bindingUuid", "physicalFormat", "mysqlServerUuid", "schema", "schemaFormat", "ddlSha256", "provisioningUuid")
  private val publicationKeys = Vector("evidenceBytes", "diagnosticsBytes", "configBytes", "requestBytes", "snapshotBytes",
    "receiptBytes", "rows", "retainedBytes", "packetBytes")
  private val accountKeys = Vector("guardDefiner", "auditDefiner", "runtime", "provisioner", "importer", "operationQuotaUpdater")

  /** 规范持久 recipe DATA；构造器不接受 Plan/permit，亦不能签发它们。 */
  final class Recipe private[MySqlDeploymentReceiptRuntimeContract] (private val values: Vector[String], private val family: Family,
      private val encoding: String):
    def canonicalBytes: Array[Byte] =
      (encoding + "\n" + recipeKeys.zip(values).map((k, v) => s"$k=$v\n").mkString).getBytes(StandardCharsets.UTF_8)
    private[mysql] def requireIdentity(id: MySqlApprovedBaselineIdentity): Unit =
      if values(4) != id.bindingUuid.toString || values(5) != "2" || values(6) != id.serverUuid.toString ||
        values(7) != id.schema || values(8) != family.schemaFormat.toString || values(9) != id.ddlSha256.toLowerCase(java.util.Locale.ROOT) ||
        values(10) != id.provisioningUuid.toString then fail("schema5 recipe differs from approved identity")
    private[mysql] def requirePlanData(plan: com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData): Unit =
      if family != DeploymentFamily || plan == null || plan.physicalFormat() != 2 || plan.schemaFormat() != 5 ||
        values(0) != plan.creationOperationUuid().toString || values(1) != plan.bootstrapOperationUuid().toString ||
        values(2) != plan.prepareWriteUuid().toString || values(3) != plan.certifyWriteUuid().toString ||
        values(4) != plan.bindingUuid().toString || values(5) != "2" || values(6) != plan.mysqlServerUuid().toString ||
        values(7) != plan.schema() || values(8) != family.schemaFormat.toString || !values(9).equalsIgnoreCase(plan.ddlSha256()) ||
        values(10) != plan.provisioningUuid().toString then fail("schema5 recipe differs from complete actual PlanData")
    private[mysql] def requireManagedIdentity(serverUuid: UUID, schema: String, provisioningUuid: UUID,
        bindingUuid: UUID, ddlSha256: String, family: Family): Unit =
      if serverUuid == null || provisioningUuid == null || bindingUuid == null || family != DbFirstFamily ||
        values(4) != bindingUuid.toString || values(5) != "2" || values(6) != serverUuid.toString ||
        values(7) != schema || values(8) != family.schemaFormat.toString ||
        !values(9).equalsIgnoreCase(ddlSha256) || values(10) != provisioningUuid.toString then
        fail("schema6 managed recipe differs from the selected server/schema identities")
    override def toString: String = "DeploymentReceiptRecipe[DATA; not native provenance]"

  def decodeRecipe(bytes: Array[Byte]): Recipe = decodeRecipeFor(bytes, DeploymentFamily)
  private[mysql] def decodeRecipeFor(bytes: Array[Byte], family: Family): Recipe =
    if bytes == null || bytes.length > MySqlRuntimeMetadata.MAX_METADATA_BYTES then fail("bounded recipe required")
    val value = utf8(bytes)
    val lines = value.split("\n", -1).toVector
    if lines.size != 13 || lines.head != family.recipeEncoding || lines.last != "" then fail("recipe requires encoding plus eleven fields and final LF")
    val values = recipeKeys.zipWithIndex.map { (key, index) =>
      val prefix = key + "="
      val line = lines(index + 1)
      if !line.startsWith(prefix) then fail("recipe field order differs")
      line.substring(prefix.length)
    }
    Vector(0, 1, 2, 3, 4, 6, 10).foreach(i => canonicalUuid(values(i)))
    if values(5) != "2" || values(8) != family.schemaFormat.toString || !values(7).matches("[a-z][a-z0-9_]{0,63}") ||
      !values(9).matches("[0-9a-f]{64}") then fail("recipe schema/physical/DDL value differs")
    val result = new Recipe(values, family, family.recipeEncoding)
    if !Arrays.equals(bytes, result.canonicalBytes) then fail("noncanonical recipe bytes")
    result

  private[mysql] def decodeManagedDbFirstRecipe(bytes: Array[Byte]): Recipe =
    if bytes == null || bytes.length > MySqlRuntimeMetadata.MAX_METADATA_BYTES then fail("bounded managed schema6 recipe required")
    val value = utf8(bytes)
    val lines = value.split("\n", -1).toVector
    if lines.size != 13 || lines.head != ManagedDbFirstRecipeEncoding || lines.last != "" then
      fail("managed schema6 recipe requires the exact encoding and eleven fields")
    val values = recipeKeys.zipWithIndex.map { (key, index) =>
      val line = lines(index + 1)
      if !line.startsWith(key + "=") then fail("managed schema6 recipe field order differs")
      line.substring(key.length + 1)
    }
    Vector(0, 1, 2, 3, 4, 6, 10).foreach(i => canonicalUuid(values(i)))
    if values(5) != "2" || values(8) != DbFirstFamily.schemaFormat.toString ||
      !values(7).matches("[A-Za-z0-9_]{1,64}") || !values(9).matches("[0-9a-f]{64}") then
      fail("managed schema6 recipe identity is malformed")
    val result = new Recipe(values, DbFirstFamily, ManagedDbFirstRecipeEncoding)
    if !Arrays.equals(bytes, result.canonicalBytes) then fail("managed schema6 recipe is noncanonical")
    result

  private[mysql] final class Semantics private[MySqlDeploymentReceiptRuntimeContract] (
      private val data: JsonObject, val profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      val rendered: Generated, val privileges: Vector[MySqlAuditResultSet],
      val recipe: Recipe, val root: MySqlFormat3ReadBudget, val codecWorkspace: Long, private[mysql] val owner: MySqlRuntimeInputs.AssemblyOwner, val family: Family
  ):
    private[mysql] def requireAlive(): Unit = { owner.requireAssemblingOrReadable(); MySqlManifestConfigurationIdentity.requireRoot(root) }
    private[mysql] def text(key: String): String = { requireAlive(); string(data, key) }
    private[mysql] def long(key: String): Long = { requireAlive(); number(data, key) }
    private[mysql] def grants: Vector[String] = { requireAlive(); array(data, "callerGrants").map(asString) }
    private[mysql] def source: Array[Byte] = { requireAlive(); hex(text("sourceProvenanceHex")) }
    private[mysql] def sourceSha: Array[Byte] = { requireAlive(); hex(text("sourceProvenanceSha256")) }
    private[mysql] def historicalSource: Option[MySqlDbFirstRuntimeContract.HistorySource] =
      requireAlive()
      if family != DbFirstFamily then throw new IllegalStateException("historical source belongs only to schema6 K")
      MySqlDbFirstRuntimeContract.validateHistory(obj(data,"historicalSchema5"))
    private[mysql] def writeSemantic(destination: java.io.OutputStream): Unit =
      requireAlive()
      writeSemanticFields(data, destination)

  /** Canonical schema6 K DATA from the supported recipe, before B/source-capture pins exist.
    * It owns only bounded semantic bytes; it cannot be adopted as runtime K or issue qualification.
    */
  private[mysql] final class ManagedSemantic private[MySqlDeploymentReceiptRuntimeContract] (
      private var data: JsonObject, initialProfile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      initialRendered: Generated, initialRecipe: Recipe, val root: MySqlFormat3ReadBudget,
    private val retainedFee: Long, private val codecWorkspace: Long
  ) extends AutoCloseable:
    private var managedProfile = initialProfile
    private var managedRendered = initialRendered
    private var managedRecipe = initialRecipe
    private var closed = false
    private def requireOpen(): Unit =
      if closed || data == null || root.isClosed then fail("managed schema6 semantic DATA owner is closed")
      MySqlManifestConfigurationIdentity.requireRoot(root)
    private[mysql] def profile: MySqlDeploymentReceiptSchemaProfile.Inputs = { requireOpen(); managedProfile }
    private[mysql] def rendered: Generated = { requireOpen(); managedRendered }
    private[mysql] def recipe: Recipe = { requireOpen(); managedRecipe }
    private[mysql] def text(key: String): String = { requireOpen(); string(data, key) }
    private[mysql] def long(key: String): Long = { requireOpen(); number(data, key) }
    private[mysql] def grants: Vector[String] = { requireOpen(); array(data, "callerGrants").map(asString) }
    private[mysql] def privileges: Vector[MySqlAuditResultSet] = { requireOpen(); decodePrivileges(array(data, "definerPrivileges")) }
    private[mysql] def canonicalWorkspaceBytes: Long = { requireOpen(); codecWorkspace }
    private[mysql] def writeSemantic(destination: java.io.OutputStream): Unit =
      requireOpen()
      writeSemanticFields(data, destination)
    override def close(): Unit =
      if !closed then
        requireOpen()
        root.releaseRetainedMemory(retainedFee, "managed schema6 semantic DATA detached")
        data = null; managedProfile = null; managedRendered = null; managedRecipe = null
        closed = true
    override def toString: String = "ManagedSchema6Semantic[DATA; no B/K pin, qualification or admission]"

  /** Shared pin-free K normalization used by external validated K and managed pre-pin K. */
  private[mysql] def writeSemanticFields(source: JsonObject, destination: java.io.OutputStream): Unit =
    if source == null || destination == null then fail("closed K semantic source and destination are required")
    val copy = source.deepCopy()
    copy.remove("approvedBaselineSha256"); copy.remove("sourceCaptureSha256")
    Vector("ddlSha256", "sourceProvenanceHex", "sourceProvenanceSha256", "limitProfileHex", "limitProfileSha256").foreach { k =>
      copy.addProperty(k, string(copy, k).toLowerCase(java.util.Locale.ROOT))
    }
    val limits = copy.getAsJsonObject("profile").getAsJsonObject("sourceLimitProfile")
    Vector("hex", "sha256").foreach(k => limits.addProperty(k, string(limits, k).toLowerCase(java.util.Locale.ROOT)))
    val grants = new JsonArray()
    array(copy, "callerGrants").map(asString).sorted.foreach(v => grants.add(v))
    copy.add("callerGrants", grants)
    copy.getAsJsonArray("definerPrivileges").asScala.foreach { set =>
      val rows = new JsonArray()
      set.getAsJsonObject.getAsJsonArray("rows").asScala.toVector.sortBy { row =>
        row.getAsJsonArray.asScala.map(cell => string(cell.getAsJsonObject, "value")).mkString("\u0000")
      }.foreach(v => rows.add(v))
      set.getAsJsonObject.add("rows", rows)
    }
    writeCanonical(copy, destination)

  private val managedDbFirstFields = (fields - DeploymentFamily.manifestKey - "approvedBaselineSha256" - "sourceCaptureSha256") ++
    Set(DbFirstFamily.manifestKey, "historicalSchema5")

  /** Parse program-built closed schema6 semantic bytes. This is not an external file or approval loader. */
  private[mysql] def parseManagedDbFirstSemantic(bytes: Array[Byte], root: MySqlFormat3ReadBudget,
      profile: MySqlDeploymentReceiptSchemaProfile.Inputs, rendered: Generated): ManagedSemantic =
    if bytes == null || bytes.length > MaxBytes || root == null || root.isClosed || root.isScopedReadBudget ||
      profile == null || rendered == null || !rendered.managedExistingDatabase ||
      (rendered.inputs ne profile) || (profile.sourceLimitProfile ne root.profile) then
      fail("bounded managed schema6 semantic bytes, final typed render and exact open assembly root required")
    MySqlManifestConfigurationIdentity.requireRoot(root)
    // 48x raw bytes covers decoded text, strict JSON tree, profile/privilege decoding and the
    // normalized byte/tree overlap. Reserve it before those allocations, as the pinned loader does.
    val parseWorkspace = Math.addExact(65536L, Math.multiplyExact(bytes.length.toLong, 48L))
    MySqlManifestConfigurationIdentity.temporary(root, parseWorkspace) {
      val json = parse(utf8(bytes))
      exact(json, managedDbFirstFields)
      val family = DbFirstFamily
      if string(json, "format") != family.format || number(json, "schemaFormat") != 6L ||
        string(json, "coverage") != family.coverage || number(json, "physicalFormat") != 2L ||
        string(json, "importState") != "NONE" || string(json, family.manifestKey) != family.encoding then
        fail("managed schema6 semantic family or selector differs")
      MySqlDbFirstRuntimeContract.validateHistory(obj(json, "historicalSchema5"))
      val decoded = decodeProfile(obj(json, "profile"), root.profile)
      if decoded.profileIdentitySha256 != profile.profileIdentitySha256 || (decoded.sourceLimitProfile ne root.profile) then
        fail("managed schema6 profile differs from final typed render or original root")
      if string(json, "schema") != profile.schema || !string(json, "ddlSha256").equalsIgnoreCase(rendered.sha256) ||
        string(json, "serverVersion") != MySqlConnectionSource.SupportedServerVersion ||
        string(json, "auditDefiner") != profile.accounts.auditDefiner.identity then
        fail("managed schema6 semantic identity differs from compiled renderer")
      val server = canonicalUuid(string(json, "serverUuid"))
      val provisioning = canonicalUuid(string(json, "provisioningUuid"))
      val binding = canonicalUuid(string(json, "bindingUuid"))
      val sourceKind = string(json, "sourceProvenanceKind")
      if sourceKind != "MANAGED_INSTALL" then fail("managed schema6 requires its exact managed-install provenance")
      val source = hex(string(json, "sourceProvenanceHex"))
      if !MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(source), hex(string(json, "sourceProvenanceSha256"))) then
        fail("managed schema6 recipe SHA differs")
      val recipe = decodeManagedDbFirstRecipe(source)
      recipe.requireManagedIdentity(server, profile.schema, provisioning, binding, rendered.sha256, family)
      val privileges = decodePrivileges(array(json, "definerPrivileges"))
      validateManagedSemantic(json, profile, rendered, privileges, source)
      val canonical = new java.io.ByteArrayOutputStream(Math.min(bytes.length, MaxBytes))
      writeSemanticFields(json, canonical)
      val canonicalBytes = canonical.toByteArray
      if canonicalBytes.length > MaxBytes then fail("managed schema6 semantic projection exceeds its closed bound")
      // Retained Gson nodes/strings and the immutable recipe exceed their serialized byte count.
      // Keep a conservative separate charge after parseWorkspace settles.
      val retainedFee = Math.addExact(4096L, Math.multiplyExact(canonicalBytes.length.toLong, 48L))
      root.reserveRetainedMemory(retainedFee, "managed schema6 semantic holder BEFORE publication")
      var published = false
      var primary: Throwable = null
      try
        // Parse canonical bytes into a private closed object; the input copy is the only retained value.
        val canonicalJson = parse(utf8(canonicalBytes))
        val codecWorkspace = Math.addExact(65536L, Math.multiplyExact(canonicalBytes.length.toLong, 48L))
        val result = new ManagedSemantic(canonicalJson, profile, rendered, recipe, root, retainedFee, codecWorkspace)
        published = true
        result
      catch
        case failure: Throwable => primary = failure; throw failure
      finally
        if !published then
          try root.releaseRetainedMemory(retainedFee, "unpublished managed schema6 semantic holder")
          catch case cleanup: Throwable =>
            if primary == null then throw cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)
    }

  private def validateManagedSemantic(json: JsonObject, profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      rendered: Generated, privilegeSets: Vector[MySqlAuditResultSet], source: Array[Byte]): Unit =
    val fieldsByKey = Vector("serverUuid", "provisioningUuid", "bindingUuid", "effectiveUser", "connectedUser",
      "callerRoles", "auditDefinerRoles", "ddlSha256", "schema", "sourceProvenanceKind")
    fieldsByKey.foreach(key => string(json, key))
    if string(json, "effectiveUser").isEmpty || string(json, "connectedUser").isEmpty ||
      string(json, "callerRoles").isEmpty || string(json, "auditDefinerRoles").isEmpty then
      fail("managed schema6 caller identity and role policy must be explicit")
    if number(json, "maxReceiptAgeNanos") <= 0L || number(json, "maxReceiptAgeNanos") > MySqlSchemaAuditor.MaxReceiptAgeNanos then
      fail("managed schema6 receipt freshness is outside the existing bound")
    val grants = array(json, "callerGrants").map(asString)
    if grants.isEmpty || grants.exists(_.isEmpty) || grants.distinct.size != grants.size then
      fail("managed schema6 caller grant plan must be complete and unique")
    if privilegeSets.map(_.id) != MySqlAuditCoverage.Format3PrivilegeResultSetIds then
      fail("managed schema6 requires exact ordered privilege result sets")
    privilegeSets.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).foreach { (set, labels) =>
      if set.columns.map(_.label) != labels || set.rows.exists(row => row.cells.size != labels.size ||
        !row.cells.forall(_.isInstanceOf[MySqlAuditText])) then fail("managed schema6 privilege shape differs")
    }
    MySqlSchemaAuditor.rejectDuplicateRows(privilegeSets)
    if source == null || !MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(source),
        hex(string(json, "sourceProvenanceSha256"))) then fail("managed schema6 provenance is not self-consistent")
    if rendered.inputs ne profile then fail("managed schema6 renderer lost exact profile input")
    rendered.validateSecurity(profile.schema, privilegeSets)

  private[mysql] final class Loaded private[MySqlDeploymentReceiptRuntimeContract] (
      private var value: Semantics, private val root: MySqlFormat3ReadBudget, private val fee: Long
  ) extends AutoCloseable:
    private var closed = false
    def semantics: Semantics =
      if closed || value == null then fail("schema5 K owner closed")
      value.requireAlive(); value
    override def close(): Unit =
      if !closed then
        value = null
        root.releaseRetainedMemory(fee, "schema5 K/security/profile graph detached")
        closed = true

  private[mysql] def load(metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic,
      baseline: MySqlApprovedStaticBaseline, owner: MySqlRuntimeInputs.AssemblyOwner): Loaded =
    loadPinned(metadata.runtimeContract(), baseline, owner, DeploymentFamily)

  private[mysql] def loadDbFirst(metadata: MySqlRuntimeMetadata.DbFirstStatic,
      baseline: MySqlApprovedStaticBaseline, owner: MySqlRuntimeInputs.AssemblyOwner): Loaded =
    loadPinned(metadata.runtimeContract(), baseline, owner, DbFirstFamily)

  private[mysql] def loadHistory(metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic,
      baseline: MySqlApprovedStaticBaseline, owner: MySqlRuntimeInputs.AssemblyOwner): Loaded =
    loadPinned(metadata.runtimeContract(), baseline, owner, DeploymentFamily, historical = true)

  private def loadPinned(pin: MySqlRuntimeMetadata.RuntimeContract,
      baseline: MySqlApprovedStaticBaseline, owner: MySqlRuntimeInputs.AssemblyOwner, family: Family, historical: Boolean = false): Loaded =
    val root = owner.staticAssemblyRoot
    if historical then owner.requireHistoricalBaseline(baseline) else owner.requireStaticBaseline(baseline)
    MySqlManifestConfigurationIdentity.requireRoot(root)
    val path = pin.absolutePath()
    val before = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    if !path.isAbsolute || before.isSymbolicLink || !before.isRegularFile || before.size() > MaxBytes then fail("bounded regular schema5 K required")
    val n = before.size()
    MySqlManifestConfigurationIdentity.temporary(root, Math.addExact(65536L, Math.multiplyExact(n, 12L))) {
      val raw = new Array[Byte](Math.toIntExact(n))
      val stream = Files.newInputStream(path, java.nio.file.StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
      var readFailure: Throwable = null
      try
        var offset = 0
        while offset < raw.length do
          val got = stream.read(raw, offset, raw.length - offset)
          if got < 0 then fail("schema5 K truncated during bounded read")
          offset += got
        if stream.read() != -1 then fail("schema5 K grew beyond prepaid bytes")
      catch
        case failure: Throwable =>
          readFailure = failure
          throw failure
      finally
        try stream.close()
        catch case cleanup: Throwable =>
          if readFailure == null then throw cleanup else if cleanup ne readFailure then readFailure.addSuppressed(cleanup)
      val after = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      if raw.length.toLong != n || after.size() != n || before.fileKey() != after.fileKey() ||
        before.lastModifiedTime() != after.lastModifiedTime() || !sha(raw).equalsIgnoreCase(pin.fileSha256()) then
        fail("schema5 K changed or raw pin differs")
      val template = MySqlDeploymentReceiptSchemaProfile.sourceSqlLength()
      val fee = Math.addExact(Math.addExact(65536L, Math.multiplyExact(n, 48L)), Math.multiplyExact(template, 64L))
      root.reserveRetainedMemory(fee, "schema5 K/profile/rendered/security graph BEFORE parse")
      var published = false
      var primary: Throwable = null
      try
        val json = parse(utf8(raw))
        val expectedFields = fields - DeploymentFamily.manifestKey + family.manifestKey
        exact(json, if family == DbFirstFamily then expectedFields + "historicalSchema5" else expectedFields)
        if family == DbFirstFamily then MySqlDbFirstRuntimeContract.validateHistory(obj(json,"historicalSchema5"))
        val id = baseline.identity
        if string(json, "format") != family.format || number(json, "schemaFormat") != family.schemaFormat.toLong ||
          string(json, "coverage") != family.coverage || number(json, "physicalFormat") != 2L ||
          string(json, "importState") != "NONE" || string(json, family.manifestKey) != family.encoding ||
          !string(json, "approvedBaselineSha256").equalsIgnoreCase(baseline.fileSha256) ||
          !string(json, "sourceCaptureSha256").equalsIgnoreCase(id.sourceCaptureSha256) then fail("schema5 K profile/pins differ")
        val identity = Map("serverUuid" -> id.serverUuid.toString, "serverVersion" -> id.serverVersion, "schema" -> id.schema,
          "provisioningUuid" -> id.provisioningUuid.toString, "bindingUuid" -> id.bindingUuid.toString,
          "ddlSha256" -> id.ddlSha256, "auditDefiner" -> id.auditDefiner)
        identity.foreach { (k, v) =>
          val same = if k == "ddlSha256" then string(json, k).equalsIgnoreCase(v) else string(json, k) == v
          if !same then fail("schema5 K identity differs: " + k)
        }
        val profile = decodeProfile(obj(json, "profile"), root.profile)
        if profile.schema != id.schema || !(profile.sourceLimitProfile == root.profile) then fail("schema5 K profile/root differs")
        val sourceKind = string(json, "sourceProvenanceKind")
        val managed = family == DbFirstFamily && sourceKind == "MANAGED_INSTALL"
        if !managed && sourceKind != "FRESH_PLAN" then fail("static source kind has no supported recipe codec")
        val rendered = if managed then generateManagedDbFirst(profile) else generate(profile, family)
        if !rendered.sha256.equalsIgnoreCase(id.ddlSha256) then fail("schema5 K rendered DDL differs")
        if string(json, "limitProfileEncoding") != SourceLimitProfile.ENCODING ||
          !Arrays.equals(hex(string(json, "limitProfileHex")), root.profile.canonicalBytes()) ||
          !Arrays.equals(hex(string(json, "limitProfileSha256")), root.profile.sha256()) then fail("schema5 K source profile differs")
        val source = hex(string(json, "sourceProvenanceHex"))
        if !MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(source), hex(string(json, "sourceProvenanceSha256"))) then fail("recipe SHA differs")
        val recipe = if managed then decodeManagedDbFirstRecipe(source) else decodeRecipeFor(source, family)
        recipe.requireIdentity(id)
        if id.schemaFormat != family.schemaFormat || id.coverage != family.coverage then fail("static baseline family differs")
        val sets = decodePrivileges(array(json, "definerPrivileges"))
        val semantic = new Semantics(json, profile, rendered, sets, recipe, root, Math.addExact(65536L, Math.multiplyExact(n, 64L)), owner, family)
        // 验证真正完整安全图；不借 dummy C 触发 constructor。
        MySqlDeploymentReceiptSchemaAuditInputs.validateSemantics(semantic)
        val result = new Loaded(semantic, root, fee)
        if historical then owner.adoptHistoricalContract(result)
        else if family == DeploymentFamily then owner.adoptStaticContract(result)
        else owner.adoptDbFirstContract(result)
        published = true
        result
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        if !published then
          try root.releaseRetainedMemory(fee, "unpublished schema5 K graph")
          catch case cleanup: Throwable =>
            if primary == null then throw cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)
    }

  private def decodeProfile(p: JsonObject, actualLimit: SourceLimitProfile): MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    exact(p, Set("schema", "accounts", "sourceLimitProfile", "publicationBudget", "deploymentBudget", "quotaMaximumRecords", "trustedAdministrativeProxyRows"))
    val accounts = obj(p, "accounts"); exact(accounts, accountKeys.toSet)
    val a = accountKeys.map { key => val x = obj(accounts, key); exact(x, Set("user", "host")); Account(string(x, "user"), string(x, "host")) }
    val source = obj(p, "sourceLimitProfile"); exact(source, Set("encoding", "hex", "sha256"))
    if string(source, "encoding") != SourceLimitProfile.ENCODING then fail("source profile encoding")
    val decodedLimit = SourceLimitProfile.decode(hex(string(source, "hex")), hex(string(source, "sha256")))
    if decodedLimit != actualLimit then fail("profile source limits differ from original root")
    val limit = actualLimit
    val pub = obj(p, "publicationBudget"); exact(pub, publicationKeys.toSet)
    val b = publicationKeys.map(number(pub, _))
    val dep = obj(p, "deploymentBudget"); exact(dep, Set("evidenceBytes", "diagnosticsBytes"))
    val proxies = array(p, "trustedAdministrativeProxyRows").map { e =>
      val x = asObject(e); exact(x, Set("user", "host", "proxiedUser", "proxiedHost", "withGrant"))
      TrustedProxyRow(string(x, "user"), string(x, "host"), string(x, "proxiedUser"), string(x, "proxiedHost"), boolean(x, "withGrant"))
    }
    if proxies != proxies.sortBy(_.tuple) then fail("proxy tuples must be canonical sorted")
    inputs(string(p, "schema"), Accounts(a(0), a(1), a(2), a(3), a(4), a(5)), limit,
      MySqlScriptSchemaProfile.Budget(b(0), b(1), b(2), b(3), b(4), b(5), b(6), b(7), b(8)),
      MySqlDeploymentReceiptSchemaDefinition.Budget(number(dep, "evidenceBytes"), number(dep, "diagnosticsBytes")), number(p, "quotaMaximumRecords"), proxies)

  private def decodePrivileges(values: Vector[JsonElement]): Vector[MySqlAuditResultSet] = values.map { e =>
    val s = asObject(e); exact(s, Set("id", "columns", "rows"))
    val cols = array(s, "columns").map { e =>
      val c = asObject(e); exact(c, Set("label", "jdbcType", "typeName", "precision", "scale", "nullable", "signed"))
      MySqlAuditColumn(string(c, "label"), Math.toIntExact(number(c, "jdbcType")), string(c, "typeName"),
        Math.toIntExact(number(c, "precision")), Math.toIntExact(number(c, "scale")), Math.toIntExact(number(c, "nullable")), boolean(c, "signed"))
    }
    val rows = array(s, "rows").map { e =>
      if !e.isJsonArray then fail("privilege row array required")
      MySqlAuditRow(e.getAsJsonArray.asScala.toVector.map { e =>
        val c = asObject(e); exact(c, Set("tag", "value"))
        if string(c, "tag") != "TEXT" then fail("privilege cell must be TEXT")
        MySqlAuditText(string(c, "value"))
      })
    }
    MySqlAuditResultSet(string(s, "id"), cols, rows)
  }

  private def parse(text: String): JsonObject =
    val reader = new JsonReader(new StringReader(text)); reader.setStrictness(Strictness.STRICT)
    def read(depth: Int): JsonElement =
      if depth > 12 then fail("schema5 K nesting exceeds closed structure")
      reader.peek() match
        case JsonToken.BEGIN_OBJECT =>
          val o = new JsonObject(); reader.beginObject()
          while reader.hasNext do
            val key = reader.nextName()
            if o.has(key) then fail("duplicate schema5 K key")
            o.add(key, read(depth + 1))
          reader.endObject(); o
        case JsonToken.BEGIN_ARRAY =>
          val a = new JsonArray(); reader.beginArray()
          while reader.hasNext do a.add(read(depth + 1))
          reader.endArray(); a
        case JsonToken.STRING => new JsonPrimitive(reader.nextString())
        case JsonToken.NUMBER =>
          val n = reader.nextString()
          if !n.matches("-?(0|[1-9][0-9]*)") then fail("canonical integer required")
          new JsonPrimitive(java.lang.Long.valueOf(n))
        case JsonToken.BOOLEAN => new JsonPrimitive(reader.nextBoolean())
        case _ => fail("NULL or unsupported K value")
    try
      val value = asObject(read(0))
      if reader.peek() != JsonToken.END_DOCUMENT then fail("trailing schema5 K bytes")
      value
    finally reader.close()

  private[mysql] def writeCanonical(value: JsonElement, out: java.io.OutputStream): Unit =
    def emit(s: String): Unit = out.write(s.getBytes(StandardCharsets.UTF_8))
    if value.isJsonObject then
      emit("{")
      value.getAsJsonObject.keySet().asScala.toVector.sorted.zipWithIndex.foreach { (k, i) =>
        if i != 0 then emit(",")
        emit(new JsonPrimitive(k).toString); emit(":"); writeCanonical(value.getAsJsonObject.get(k), out)
      }
      emit("}")
    else if value.isJsonArray then
      emit("[")
      value.getAsJsonArray.asScala.toVector.zipWithIndex.foreach { (v, i) => if i != 0 then emit(","); writeCanonical(v, out) }
      emit("]")
    else emit(value.toString)

  private def exact(o: JsonObject, keys: Set[String]): Unit = if o.keySet().asScala.toSet != keys then fail("schema5 K closed key set differs")
  private def asObject(e: JsonElement): JsonObject = if e != null && e.isJsonObject then e.getAsJsonObject else fail("object required")
  private def obj(o: JsonObject, k: String): JsonObject = asObject(o.get(k))
  private def asString(e: JsonElement): String =
    if e == null || !e.isJsonPrimitive || !e.getAsJsonPrimitive.isString then fail("string required")
    val s = e.getAsString
    if !StandardCharsets.UTF_8.newEncoder().canEncode(s) then fail("unpaired string surrogate")
    s
  private def string(o: JsonObject, k: String): String = asString(o.get(k))
  private def number(o: JsonObject, k: String): Long =
    val e = o.get(k)
    if e == null || !e.isJsonPrimitive || !e.getAsJsonPrimitive.isNumber then fail("integer required")
    e.getAsLong
  private def boolean(o: JsonObject, k: String): Boolean =
    val e = o.get(k)
    if e == null || !e.isJsonPrimitive || !e.getAsJsonPrimitive.isBoolean then fail("boolean required")
    e.getAsBoolean
  private def array(o: JsonObject, k: String): Vector[JsonElement] =
    val e = o.get(k)
    if e == null || !e.isJsonArray then fail("array required")
    e.getAsJsonArray.asScala.toVector
  private def canonicalUuid(s: String): UUID =
    val u = UUID.fromString(s)
    if u.toString != s || u == new UUID(0L, 0L) then fail("canonical nonzero recipe UUID required")
    u
  private def utf8(raw: Array[Byte]): String =
    if raw.length >= 3 && raw(0) == 0xef.toByte && raw(1) == 0xbb.toByte && raw(2) == 0xbf.toByte then fail("UTF8 BOM forbidden")
    StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString
  private def hex(s: String): Array[Byte] = HexFormat.of().parseHex(s)
  private def sha(raw: Array[Byte]): String = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw))
  private def fail(message: String): Nothing = throw new SQLDataException(message)
