package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.{HexFormat, UUID}

/**
  * 根据类型化发布输入与同连接安全观测生成 managed-install K 语义 DATA。返回 holder 不含 baseline/source-capture pin，
  * 也不能签发 Ready；带 pins 的完整 K 由同一 DATA 的独立显式序列化操作生成。
  */
private[mysql] object MySqlManagedSemanticProducer:
  private val MaxBytes = MySqlDeploymentReceiptRuntimeContract.MaxBytes
  private val Hex = HexFormat.of()
  private val RecipeFields = Vector("creationOperationUuid", "bootstrapOperationUuid", "prepareWriteUuid",
    "certifyWriteUuid", "bindingUuid", "physicalFormat", "mysqlServerUuid", "schema", "schemaFormat",
    "ddlSha256", "provisioningUuid")
  private val AccountFields = Vector("guardDefiner", "auditDefiner", "runtime", "provisioner", "importer",
    "operationQuotaUpdater")
  private val PublicationFields = Vector("evidenceBytes", "diagnosticsBytes", "configBytes", "requestBytes",
    "snapshotBytes", "receiptBytes", "rows", "retainedBytes", "packetBytes")

  final class Data private[MySqlManagedSemanticProducer] (
      private var semantic: Array[Byte], private val root: MySqlFormat3ReadBudget,
      private val retainedFee: Long
  ) extends AutoCloseable:
    private var closed = false
    private def requireOpen(): Unit =
      if closed || semantic == null then fail("managed schema6 semantic DATA holder is closed")
      MySqlManifestConfigurationIdentity.requireRoot(root)
    def semanticLength: Int = { requireOpen(); semantic.length }
    def writeSemantic(destination: OutputStream): Unit =
      requireOpen()
      if destination == null then fail("managed schema6 semantic destination is required")
      destination.write(semantic)

    /** 通过已闭合的 MySQL metadata 投影和同一 K parser 生成 pre-seed C DATA。 */
    def configurationDigest(metadata: MySqlRuntimeMetadata.DeploymentSemantics,
        profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
        generated: MySqlDeploymentReceiptRuntimeContract.Generated,
        policy: MySqlRuntimeAuditPolicy): Array[Byte] =
      requireOpen()
      var parsed: MySqlDeploymentReceiptRuntimeContract.ManagedSemantic = null
      var primary: Throwable = null
      try
        parsed = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(semantic, root, profile, generated)
        MySqlCurrentConfigurationProjection.managedDbFirstDigest(metadata, parsed, policy)
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        if parsed != null then
          try parsed.close()
          catch case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)

    /** 只加入调用方提供的最终 B/capture pins；本地不会伪造 baseline 或 capture。 */
    def writeFullK(destination: OutputStream, approvedBaselineSha256: String,
        sourceCaptureSha256: String): Unit =
      requireOpen()
      if destination == null then fail("managed schema6 full K destination is required")
      val baseline = canonicalSha(approvedBaselineSha256, "actual approved baseline SHA-256")
      val capture = canonicalSha(sourceCaptureSha256, "actual source-capture SHA-256")
      val workspace = Math.addExact(65536L, Math.multiplyExact(semantic.length.toLong, 48L))
      MySqlManifestConfigurationIdentity.temporary(root, workspace) {
        val text = new String(semantic, StandardCharsets.UTF_8)
        val parsed = JsonParser.parseString(text)
        if !parsed.isJsonObject then fail("producer-owned semantic K is not a JSON object")
        val json = parsed.getAsJsonObject
        if json.has("approvedBaselineSha256") || json.has("sourceCaptureSha256") then
          fail("managed semantic DATA already contains a final K pin")
        json.addProperty("approvedBaselineSha256", baseline)
        json.addProperty("sourceCaptureSha256", capture)
        val counter = new CountingOutputStream
        MySqlDeploymentReceiptRuntimeContract.writeCanonical(json, counter)
        if counter.length > MaxBytes then fail("managed full schema6 K exceeds the existing contract bound")
        MySqlDeploymentReceiptRuntimeContract.writeCanonical(json, new BoundedOutputStream(destination, MaxBytes))
      }

    override def close(): Unit =
      if !closed then
        MySqlManifestConfigurationIdentity.requireRoot(root)
        root.releaseRetainedMemory(retainedFee, "managed schema6 semantic bytes detached")
        semantic = null
        closed = true
    override def toString: String = "ManagedSchema6SemanticBytes[DATA; no B/K pins, approval or readiness]"

  /** 生产入口要求调用方先取得并独立验证真实同连接安全观测；此处再次使用发布渲染器校验，所有输入仍是 DATA。 */
  def create(root: MySqlFormat3ReadBudget, profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      serverUuid: UUID, provisioningUuid: UUID, bindingUuid: UUID,
      creationOperationUuid: UUID, bootstrapOperationUuid: UUID, prepareWriteUuid: UUID,
      certifyWriteUuid: UUID, effectiveUser: String, connectedUser: String,
      policy: MySqlRuntimeAuditPolicy, callerGrants: Vector[String],
      definerPrivileges: Vector[MySqlAuditResultSet]): Data =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if profile == null || (profile.sourceLimitProfile ne root.profile) then
      fail("managed schema6 producer requires the exact profile bound to the original open root")
    if policy == null || policy.expectedImport != MySqlExpectedNoImport ||
      policy.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      fail("managed schema6 requires explicit no-import physical-format-2 runtime policy")
    if effectiveUser != profile.accounts.runtime.identity then
      fail("managed schema6 effective user differs from the profile runtime account")
    checkedText(effectiveUser, "managed effective user")
    checkedText(connectedUser, "managed connected user")
    if connectedUser.isBlank then fail("managed connected user must be a real non-empty connection fact")
    if profile.trustedAdministrativeProxyRows.size > MySqlSchemaAuditor.MaxRowsPerResultSet then
      fail("managed schema6 profile proxy rows exceed the existing row bound")
    var proxyIndex = 1
    while proxyIndex < profile.trustedAdministrativeProxyRows.size do
      if compareProxy(profile.trustedAdministrativeProxyRows(proxyIndex - 1),
          profile.trustedAdministrativeProxyRows(proxyIndex)) >= 0 then
        fail("managed schema6 profile proxy rows must use unique canonical order")
      proxyIndex += 1
    val operationIds = Vector(creationOperationUuid, bootstrapOperationUuid, prepareWriteUuid, certifyWriteUuid)
    if operationIds.exists(u => u == null || u == new UUID(0L, 0L)) ||
      operationIds(0) == operationIds(1) || operationIds(0) == operationIds(2) || operationIds(0) == operationIds(3) ||
      operationIds(1) == operationIds(2) || operationIds(1) == operationIds(3) || operationIds(2) == operationIds(3) then
      fail("managed schema6 requires four distinct canonical nonzero recipe operation UUIDs")
    Vector(serverUuid, provisioningUuid, bindingUuid).foreach { value =>
      if value == null || value == new UUID(0L, 0L) then fail("managed schema6 server/provisioning/binding UUIDs must be nonzero")
    }
    validateGrantTexts(callerGrants)
    val inputFootprint = validateAndMeasurePrivileges(definerPrivileges)
    val grantFootprint = callerGrants.foldLeft(0L)((n, grant) => Math.addExact(n,
      MySqlSchemaAuditor.boundedUtf8ByteLength(grant, MySqlSchemaAuditor.MaxCellBytes, "managed caller grant").toLong))
    val identityFootprint = Vector(effectiveUser, connectedUser, policy.callerRoles, policy.auditDefinerRoles)
      .foldLeft(0L)((n, value) => Math.addExact(n, checkedText(value, "managed caller identity/role").toLong))
    val proxyFootprint = profile.trustedAdministrativeProxyRows.foldLeft(0L) { (total, row) =>
      Vector(row.user, row.host, row.proxiedUser, row.proxiedHost).foldLeft(total) { (n, value) =>
        Math.addExact(n, checkedText(value, "managed trusted proxy identity").toLong)
      }
    }
    val profileTemplateBytes = MySqlManifestConfigurationIdentity.temporary(root, 16384L) {
      MySqlDeploymentReceiptSchemaProfile.sourceSqlLength()
    }
    // 授权推导的显式渲染与 Generated 的验证闭包同时存活，因此为两个真实渲染图各保留原 64 倍源码预算。
    val workspace = Math.addExact(65536L, Math.addExact(Math.multiplyExact(profileTemplateBytes, 128L),
      Math.multiplyExact(Math.addExact(Math.addExact(inputFootprint, grantFootprint),
        Math.addExact(identityFootprint, proxyFootprint)), 64L)))

    MySqlManifestConfigurationIdentity.temporary(root, workspace) {
      var semanticBytes: Array[Byte] = null
      var retainedFee = 0L
      var parsed: MySqlDeploymentReceiptRuntimeContract.ManagedSemantic = null
      var published = false
      var primary: Throwable = null
      try
        MySqlSchemaAuditor.rejectDuplicateRows(definerPrivileges)
        val schemaRendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
        val generated = MySqlDeploymentReceiptRuntimeContract.generateManagedDbFirst(profile)
        if schemaRendered.sha256 != generated.sha256 then fail("managed schema6 renderer identity changed during production")
        val encodedRecipe = recipeBytes(serverUuid, provisioningUuid, bindingUuid, operationIds, profile.schema,
          generated.sha256)
        val recipe = MySqlDeploymentReceiptRuntimeContract.decodeManagedDbFirstRecipe(encodedRecipe)
        recipe.requireManagedIdentity(serverUuid, profile.schema, provisioningUuid, bindingUuid, generated.sha256,
          MySqlDeploymentReceiptRuntimeContract.DbFirstFamily)
        val expectedGrants = expectedRuntimeGrants(schemaRendered)
        MySqlRuntimeCallerPrivileges.requireScopedObservation(callerGrants, profile.accounts.runtime.user,
          profile.accounts.runtime.host, profile.schema, expectedGrants, root)
        generated.validateSecurity(profile.schema, definerPrivileges)
        val semanticData = semanticObject(profile, serverUuid, provisioningUuid, bindingUuid,
          effectiveUser, connectedUser, policy, callerGrants, definerPrivileges, encodedRecipe, generated.sha256)
        val counter = new CountingOutputStream
        MySqlDeploymentReceiptRuntimeContract.writeSemanticFields(semanticData, counter)
        if counter.length > MaxBytes then fail("managed semantic K exceeds the existing contract byte bound")
        val length = Math.toIntExact(counter.length)
        retainedFee = Math.addExact(length.toLong, 256L)
        root.reserveRetainedMemory(retainedFee, "managed semantic K bytes BEFORE serialization")
        val outputWorkspace = Math.addExact(4096L, Math.multiplyExact(length.toLong, 2L))
        semanticBytes = MySqlManifestConfigurationIdentity.temporary(root, outputWorkspace) {
          val output = new BoundedByteArrayOutputStream(length, MaxBytes)
          MySqlDeploymentReceiptRuntimeContract.writeSemanticFields(semanticData, output)
          if output.size() != length then fail("managed semantic K count/write lengths differ")
          output.toByteArray
        }
        val producerGraphWorkspace = Math.multiplyExact(semanticBytes.length.toLong, 48L)
        MySqlManifestConfigurationIdentity.temporary(root, producerGraphWorkspace) {
          parsed = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(
            semanticBytes, root, profile, generated)
        }
        parsed.close()
        parsed = null
        val result = new Data(semanticBytes, root, retainedFee)
        semanticBytes = null
        retainedFee = 0L
        published = true
        result
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        if !published then
          var cleanupFailure: Throwable = null
          def preserveCleanup(failure: Throwable): Unit =
            if primary != null then
              if failure ne primary then primary.addSuppressed(failure)
            else if cleanupFailure == null then cleanupFailure = failure
            else if failure ne cleanupFailure then cleanupFailure.addSuppressed(failure)
          if parsed != null then
            try parsed.close()
            catch case cleanup: Throwable => preserveCleanup(cleanup)
          if retainedFee != 0L then
            try root.releaseRetainedMemory(retainedFee, "unpublished managed schema6 semantic bytes")
            catch case cleanup: Throwable => preserveCleanup(cleanup)
          if primary == null && cleanupFailure != null then throw cleanupFailure
    }

  private def semanticObject(profile: MySqlDeploymentReceiptSchemaProfile.Inputs, serverUuid: UUID, provisioningUuid: UUID,
      bindingUuid: UUID, effectiveUser: String, connectedUser: String, policy: MySqlRuntimeAuditPolicy,
      callerGrants: Vector[String], privileges: Vector[MySqlAuditResultSet], recipe: Array[Byte], ddlSha256: String): JsonObject =
    val limits = profile.sourceLimitProfile
    val limitBytes = limits.canonicalBytes()
    val sourceSha = sha(recipe)
    val source = new JsonObject()
    source.addProperty("format", MySqlDeploymentReceiptRuntimeContract.DbFirstFamily.format)
    source.addProperty("schemaFormat", 6)
    source.addProperty("coverage", MySqlDeploymentReceiptRuntimeContract.DbFirstFamily.coverage)
    source.addProperty("serverUuid", serverUuid.toString)
    source.addProperty("serverVersion", MySqlConnectionSource.SupportedServerVersion)
    source.addProperty("schema", profile.schema)
    source.addProperty("effectiveUser", effectiveUser)
    source.addProperty("connectedUser", connectedUser)
    source.addProperty("callerRoles", policy.callerRoles)
    source.addProperty("provisioningUuid", provisioningUuid.toString)
    source.addProperty("bindingUuid", bindingUuid.toString)
    source.addProperty("ddlSha256", ddlSha256)
    source.addProperty("auditDefiner", profile.accounts.auditDefiner.identity)
    source.addProperty("auditDefinerRoles", policy.auditDefinerRoles)
    val grants = new JsonArray()
    callerGrants.foreach(grants.add)
    source.add("callerGrants", grants)
    source.addProperty("physicalFormat", 2)
    source.addProperty("importState", "NONE")
    source.addProperty("maxReceiptAgeNanos", policy.maxReceiptAge.toNanos)
    source.addProperty("sourceProvenanceKind", "MANAGED_INSTALL")
    source.addProperty("sourceProvenanceHex", Hex.formatHex(recipe))
    source.addProperty("sourceProvenanceSha256", Hex.formatHex(sourceSha))
    source.addProperty("limitProfileEncoding", SourceLimitProfile.ENCODING)
    source.addProperty("limitProfileHex", Hex.formatHex(limitBytes))
    source.addProperty("limitProfileSha256", Hex.formatHex(limits.sha256()))
    source.add("definerPrivileges", privilegeArray(privileges))
    source.addProperty("dbFirstManifestConfigurationEncoding", MySqlDeploymentReceiptRuntimeContract.DbFirstFamily.encoding)
    source.add("profile", profileObject(profile))
    val history = new JsonObject()
    history.addProperty("kind", "NONE")
    source.add("historicalSchema5", history)
    source

  private def profileObject(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): JsonObject =
    import MySqlDeploymentReceiptSchemaProfile.*
    val result = new JsonObject()
    result.addProperty("schema", profile.schema)
    val accounts = new JsonObject()
    def account(value: Account): JsonObject =
      val encoded = new JsonObject()
      encoded.addProperty("user", value.user)
      encoded.addProperty("host", value.host)
      encoded
    AccountFields.zip(profile.accounts.all).foreach((key, value) => accounts.add(key, account(value)))
    result.add("accounts", accounts)
    val limits = new JsonObject()
    val limitBytes = profile.sourceLimitProfile.canonicalBytes()
    limits.addProperty("encoding", SourceLimitProfile.ENCODING)
    limits.addProperty("hex", Hex.formatHex(limitBytes))
    limits.addProperty("sha256", profile.sourceLimitProfile.sha256Hex())
    result.add("sourceLimitProfile", limits)
    val budget = new JsonObject()
    val p = profile.publicationBudget
    Vector(p.evidenceBytes, p.diagnosticsBytes, p.configBytes, p.requestBytes, p.snapshotBytes,
      p.receiptBytes, p.rows, p.retainedBytes, p.packetBytes).zip(PublicationFields).foreach { (value, key) =>
      budget.addProperty(key, value)
    }
    result.add("publicationBudget", budget)
    val deployment = new JsonObject()
    deployment.addProperty("evidenceBytes", profile.deploymentBudget.evidenceBytes)
    deployment.addProperty("diagnosticsBytes", profile.deploymentBudget.diagnosticsBytes)
    result.add("deploymentBudget", deployment)
    result.addProperty("quotaMaximumRecords", profile.quotaMaximumRecords)
    val proxies = new JsonArray()
    profile.trustedAdministrativeProxyRows.foreach { row =>
      val proxy = new JsonObject()
      proxy.addProperty("user", row.user)
      proxy.addProperty("host", row.host)
      proxy.addProperty("proxiedUser", row.proxiedUser)
      proxy.addProperty("proxiedHost", row.proxiedHost)
      proxy.addProperty("withGrant", row.withGrant)
      proxies.add(proxy)
    }
    result.add("trustedAdministrativeProxyRows", proxies)
    result

  private def privilegeArray(privileges: Vector[MySqlAuditResultSet]): JsonArray =
    val output = new JsonArray()
    privileges.foreach { set =>
      val encoded = new JsonObject()
      encoded.addProperty("id", set.id)
      val columns = new JsonArray()
      set.columns.foreach { column =>
        val value = new JsonObject()
        value.addProperty("label", column.label)
        value.addProperty("jdbcType", column.jdbcType)
        value.addProperty("typeName", column.typeName)
        value.addProperty("precision", column.precision)
        value.addProperty("scale", column.scale)
        value.addProperty("nullable", column.nullable)
        value.addProperty("signed", column.signed)
        columns.add(value)
      }
      encoded.add("columns", columns)
      val rows = new JsonArray()
      set.rows.foreach { row =>
        val cells = new JsonArray()
        row.cells.foreach {
          case MySqlAuditText(text) =>
            val cell = new JsonObject()
            cell.addProperty("tag", "TEXT")
            cell.addProperty("value", text)
            cells.add(cell)
          case _ => fail("managed definer privilege capture must retain complete non-NULL TEXT cells")
        }
        rows.add(cells)
      }
      encoded.add("rows", rows)
      output.add(encoded)
    }
    output

  private def expectedRuntimeGrants(rendered: MySqlDbFirstSchemaProfile.Rendered): Set[MySqlRuntimeCallerPrivileges.Grant] =
    val rows = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(rendered)
    val runtime = rendered.inputs.accounts.runtime.grantee
    val expected = Set.newBuilder[MySqlRuntimeCallerPrivileges.Grant]
    def add(value: MySqlRuntimeCallerPrivileges.Grant): Unit = { expected += value; () }
    rows(0).filter(_(0) == runtime).foreach { row =>
      add(MySqlRuntimeCallerPrivileges.Grant("TABLE", row(2), row(3), row(4), Vector.empty))
    }
    rows(1).filter(_(0) == runtime).foreach { row =>
      add(MySqlRuntimeCallerPrivileges.Grant("TABLE", row(2), row(3), row(5), Vector(row(4))))
    }
    rows(2).filter(_(0) == runtime).foreach { row =>
      add(MySqlRuntimeCallerPrivileges.Grant("SCHEMA", row(2), "", row(3), Vector.empty))
    }
    rows(3).filter(_(0) == runtime).foreach { row =>
      if row(2) != "USAGE" then fail("managed runtime renderer exposes an unsupported global privilege")
      add(MySqlRuntimeCallerPrivileges.Grant("GLOBAL", "*", "*", row(2), Vector.empty))
    }
    // expectedPrivilegeRows 已验证该渲染器对 runtime 与 audit definer 的 routine 授权；caller SHOW GRANTS 只包含 runtime。
    add(MySqlRuntimeCallerPrivileges.Grant("PROCEDURE", rendered.inputs.schema,
      MySqlDbFirstSchemaDefinition.AuditRoutine, "EXECUTE", Vector.empty))
    val result = expected.result()
    if result.isEmpty then fail("schema6 renderer has no exact runtime privilege recipe")
    result

  private def recipeBytes(serverUuid: UUID, provisioningUuid: UUID, bindingUuid: UUID,
      operations: Vector[UUID], schema: String, ddlSha256: String): Array[Byte] =
    val values = Vector(operations(0).toString, operations(1).toString, operations(2).toString,
      operations(3).toString, bindingUuid.toString, "2", serverUuid.toString, schema, "6",
      ddlSha256.toLowerCase(java.util.Locale.ROOT), provisioningUuid.toString)
    (MySqlDeploymentReceiptRuntimeContract.ManagedDbFirstRecipeEncoding + "\n" +
      RecipeFields.zip(values).map((key, value) => s"$key=$value\n").mkString).getBytes(StandardCharsets.UTF_8)

  private def validateGrantTexts(values: Vector[String]): Unit =
    if values == null || values.isEmpty || values.size > MySqlSchemaAuditor.MaxRowsPerResultSet then
      fail("managed schema6 requires complete, bounded and unique SHOW GRANTS data")
    var i = 0
    while i < values.size do
      val value = values(i)
      if value == null || value.isEmpty then fail("managed schema6 requires complete, bounded and unique SHOW GRANTS data")
      checkedText(value, "managed caller grant")
      i += 1

  private def validateAndMeasurePrivileges(values: Vector[MySqlAuditResultSet]): Long =
    if values == null || values.size != MySqlAuditCoverage.Format3PrivilegeResultSetIds.size then
      fail("managed schema6 requires the exact ordered RS15-RS18 security capture")
    var bytes = 0L
    var cells = 0L
    var setIndex = 0
    while setIndex < values.size do
      val set = values(setIndex)
      val labels = MySqlSchemaAuditor.Format3PrivilegeLabels(setIndex)
      if set == null || set.id != MySqlAuditCoverage.Format3PrivilegeResultSetIds(setIndex) ||
        set.columns == null || set.rows == null || set.columns.size != labels.size ||
        set.rows.size > MySqlSchemaAuditor.MaxRowsPerResultSet then
        fail("managed schema6 security result-set shape or row bound differs")
      var columnIndex = 0
      while columnIndex < set.columns.size do
        val column = set.columns(columnIndex)
        if column == null then fail("managed schema6 security column metadata is missing")
        if column.label != labels(columnIndex) then fail("managed schema6 security column labels differ")
        bytes = Math.addExact(bytes, checkedText(column.label, "managed privilege column label").toLong)
        bytes = Math.addExact(bytes, checkedText(column.typeName, "managed privilege JDBC type").toLong)
        bytes = Math.addExact(bytes, 64L)
        columnIndex += 1
      var rowIndex = 0
      while rowIndex < set.rows.size do
        val row = set.rows(rowIndex)
        if row == null || row.cells == null || row.cells.size != labels.size then
          fail("managed schema6 privilege row width differs from its complete JDBC metadata")
        var cellIndex = 0
        while cellIndex < row.cells.size do
          row.cells(cellIndex) match
            case MySqlAuditText(text) =>
              bytes = Math.addExact(bytes, checkedText(text, "managed privilege value").toLong)
              cells = Math.incrementExact(cells)
            case _ => fail("managed schema6 privilege cells must preserve non-NULL TEXT values")
          cellIndex += 1
        rowIndex += 1
      setIndex += 1
    if cells > MySqlSchemaAuditor.MaxTotalCells then fail("managed schema6 security capture exceeds the existing total-cell bound")
    bytes

  private def compareProxy(left: MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow,
      right: MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow): Int =
    val user = left.user.compareTo(right.user)
    if user != 0 then return user
    val host = left.host.compareTo(right.host)
    if host != 0 then return host
    val proxiedUser = left.proxiedUser.compareTo(right.proxiedUser)
    if proxiedUser != 0 then return proxiedUser
    val proxiedHost = left.proxiedHost.compareTo(right.proxiedHost)
    if proxiedHost != 0 then return proxiedHost
    java.lang.Boolean.compare(left.withGrant, right.withGrant)

  private def checkedText(value: String, context: String): Int =
    MySqlSchemaAuditor.boundedUtf8ByteLength(value, MySqlSchemaAuditor.MaxCellBytes, context)

  private def canonicalSha(value: String, context: String): String =
    if value == null || !value.matches("(?i)[0-9a-f]{64}") then fail(s"$context must be an explicit 32-byte hex DATA pin")
    value.toLowerCase(java.util.Locale.ROOT)

  private def sha(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)
  private def fail(message: String): Nothing = throw new SQLDataException(message)

  private final class CountingOutputStream extends OutputStream:
    var length = 0L
    override def write(value: Int): Unit = length = Math.addExact(length, 1L)
    override def write(bytes: Array[Byte], offset: Int, count: Int): Unit =
      if bytes == null || offset < 0 || count < 0 || offset > bytes.length - count then
        throw new IndexOutOfBoundsException("invalid managed K writer slice")
      length = Math.addExact(length, count.toLong)

  private final class BoundedByteArrayOutputStream(initial: Int, maximum: Int) extends ByteArrayOutputStream(initial):
    override def write(value: Int): Unit =
      if count >= maximum then fail("managed schema6 semantic K exceeds the existing contract byte bound")
      super.write(value)
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      if bytes == null || offset < 0 || length < 0 || offset > bytes.length - length then
        throw new IndexOutOfBoundsException("invalid managed K writer slice")
      if length > maximum - count then fail("managed schema6 semantic K exceeds the existing contract byte bound")
      super.write(bytes, offset, length)

  private final class BoundedOutputStream(delegate: OutputStream, maximum: Int) extends OutputStream:
    private var count = 0L
    override def write(value: Int): Unit =
      if count >= maximum then fail("managed full schema6 K exceeds the existing contract byte bound")
      delegate.write(value)
      count += 1L
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      if bytes == null || offset < 0 || length < 0 || offset > bytes.length - length then
        throw new IndexOutOfBoundsException("invalid managed full K writer slice")
      if length.toLong > maximum.toLong - count then fail("managed full schema6 K exceeds the existing contract byte bound")
      delegate.write(bytes, offset, length)
      count += length.toLong
