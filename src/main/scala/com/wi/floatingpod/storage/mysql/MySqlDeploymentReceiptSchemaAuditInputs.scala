package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLNonTransientConnectionException
import java.util.HexFormat
import java.util.UUID

/** 独立 schema5 审计输入；构造时校验来源值，但不签发批准或运行资格。 */
abstract class MySqlStaticSchemaAuditInputs private[mysql] (
    private var binding: MySqlManifestConfigurationIdentity.StaticBinding,
    private var lease: MySqlRuntimeInputs.ConsumerLease, expectedFamily: Int, extraFee: Long = 0L
) extends AutoCloseable:
  binding.requireLease(lease)
  require(binding.semantics.family.schemaFormat == expectedFamily, "independent audit consumer family differs")
  private var closeFailure: Throwable = null
  private val consumerAccount = binding.semantics.root
  private var consumerFee = extraFee
  private var auditScratchActive = false
  private var configurationValue: MySqlAuditBinary = null
  protected def detachAdditional(): Unit = ()
  private def requireOpen(): Unit =
    if binding == null then throw new IllegalStateException(s"schema$expectedFamily audit consumer detached", closeFailure)
    binding.requireLease(lease)
  override def close(): Unit =
    if closeFailure != null then throw new IllegalStateException(s"schema$expectedFamily audit consumer cleanup failed; no automatic retry", closeFailure)
    if binding != null then
      if expectedFamily == 6 then binding.requireLease(lease)
      binding = null
      configurationValue = null
      detachAdditional()
      try
        if consumerFee != 0L then
          consumerAccount.releaseRetainedMemory(consumerFee, "schema6 audit consumer/expectation detached")
          consumerFee = 0L
        lease.close()
        lease = null
      catch
        case failure: Throwable =>
          closeFailure = failure
          throw failure
  private[mysql] def requireBinding(actual: MySqlManifestConfigurationIdentity.StaticBinding): Unit =
    requireOpen()
    if binding ne actual then throw new IllegalArgumentException("audit consumer belongs to another exact configuration owner")
  private def semantics = { requireOpen(); binding.semantics }
  private def profileInputs = semantics.profile
  private def rendered = semantics.rendered
  def serverUuid = UUID.fromString(semantics.text("serverUuid"))
  def serverVersion = semantics.text("serverVersion")
  def schema = semantics.text("schema")
  def effectiveUser = semantics.text("effectiveUser")
  def connectedUser = semantics.text("connectedUser")
  def callerRoles = semantics.text("callerRoles")
  def provisioningUuid = UUID.fromString(semantics.text("provisioningUuid"))
  def bindingUuid = UUID.fromString(semantics.text("bindingUuid"))
  def ddlSha256 = semantics.text("ddlSha256")
  def auditDefiner = semantics.text("auditDefiner")
  def auditDefinerRoles = semantics.text("auditDefinerRoles")
  private def callerGrants = semantics.grants
  // schema6 K 规范 SHOW GRANTS 行序；仅忽略排列，逐字文本与重数保持不变。
  private def sameCallerGrants(actual: Vector[String]): Boolean =
    val expected = callerGrants
    actual != null && (if expectedFamily == 6 then
      actual.size == expected.size && expected.forall(grant => expected.count(_ == grant) == actual.count(_ == grant))
    else actual == expected)

  private def sameCallerScope(before: MySqlCallerScope, after: MySqlCallerScope): Boolean =
    if expectedFamily == 6 then
      before.copy(ownGrants=after.ownGrants) == after && sameCallerGrants(before.ownGrants) && sameCallerGrants(after.ownGrants)
    else before == after

  private def expectedPrivileges = semantics.privileges
  def configurationSha256: MySqlAuditBinary =
    requireOpen()
    if expectedFamily == 6 then
      if configurationValue == null then configurationValue = MySqlAuditBinary(HexFormat.of().parseHex(binding.hex))
      configurationValue
    else MySqlAuditBinary(HexFormat.of().parseHex(binding.hex))
  private[mysql] def sourceProvenanceKind = semantics.text("sourceProvenanceKind")
  private def sourceProvenance = MySqlAuditBinary(semantics.source)
  private[mysql] def sourceProvenanceSha256 = MySqlAuditBinary(semantics.sourceSha)
  def limitProfile = profileInputs.sourceLimitProfile
  import MySqlDeploymentReceiptSchemaProfile as Profile

  MySqlDeploymentReceiptSchemaAuditInputs.validateSemantics(semantics)

  private def reject(message: String): Nothing = throw new SQLNonTransientConnectionException(message)

  private[mysql] def requireIdentity(server: UUID, version: String, targetSchema: String, principal: String,
      roles: String, provisioning: UUID, binding: UUID, ddl: String, definer: String): Unit =
    requireOpen()
    if server != serverUuid || version != serverVersion || targetSchema != schema || principal != effectiveUser ||
      roles != callerRoles || provisioning != provisioningUuid || binding != bindingUuid ||
      !ddl.equalsIgnoreCase(ddlSha256) || definer != auditDefiner then
      reject("independent schema5 inputs belong to another exact server/schema/configuration")

  private[mysql] def validateCapturedCallerGrants(grants: Vector[String]): Unit =
    requireOpen()
    if expectedFamily == 6 && !auditScratchActive then
      withAuditScratch(semantics.root, semantics.codecWorkspace, "schema6 captured grant comparison") {
        validateCapturedCallerGrants(grants)
      }
      return
    if !sameCallerGrants(grants) then reject(s"schema$expectedFamily captured SHOW GRANTS differs from independent exact caller grants")

  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope,
      privileges: Vector[MySqlAuditResultSet]): Unit =
    requireOpen()
    if expectedFamily == 6 && !auditScratchActive then
      validateSecurity(before, after, privileges, semantics.root)
      return
    if !sameCallerScope(before, after) || before.serverUuid != serverUuid || before.serverVersion != Some(serverVersion) ||
      before.defaultSchema != schema || before.effectivePrincipal != effectiveUser || before.connectedUser != connectedUser ||
      before.effectiveRoles != callerRoles || !sameCallerGrants(before.ownGrants) || !samePrivilegeSets(privileges) then
      reject(s"schema$expectedFamily caller scope, complete SHOW GRANTS or six-subject privileges differ from independent inputs")

  /** The UNKNOWN-owned path precharges both actual and private approved privilege sort graphs. */
  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope,
      privileges: Vector[MySqlAuditResultSet], auditAccount: MySqlFormat3ReadBudget): Unit =
    requireOpen()
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    val fee = privilegeComparisonFee(privileges, expectedPrivileges)
    withAuditScratch(auditAccount, fee, s"schema$expectedFamily UNKNOWN security comparison maps and sort keys") {
      validateSecurity(before, after, privileges)
    }

  private[mysql] def validateRetainedObservation(sets: Vector[MySqlAuditResultSet],
      before: MySqlCallerScope, after: MySqlCallerScope): Unit =
    requireOpen()
    if expectedFamily == 6 && !auditScratchActive then
      validateRetainedObservation(sets, before, after, semantics.root)
      return
    var retained = 0L
    def add(n: Long): Unit =
      retained = Math.addExact(retained, n)
      if retained > limitProfile.maxRetainedBytes() then reject(s"schema$expectedFamily complete audit observation exceeds explicit retained-byte bound")
    def text(value: String): Unit =
      val bytes = MySqlSchemaAuditor.encodeUtf8(value).length.toLong
      if bytes > limitProfile.maxSqlPacketBytes() then reject(s"schema$expectedFamily audit text exceeds explicit SQL packet bound")
      add(4L + bytes)
    Vector(before, after).foreach { scope =>
      add(28L)
      Vector(scope.serverVersion.getOrElse(reject(s"schema$expectedFamily caller server version is missing")), scope.defaultSchema,
        scope.effectivePrincipal, scope.effectiveRoles, scope.connectedUser).foreach(text)
      scope.ownGrants.foreach(text)
    }
    sets.foreach { set =>
      text(set.id); add(8L)
      set.columns.foreach { c => text(c.label); text(c.typeName); add(17L) }
      set.rows.foreach { row =>
        add(4L)
        row.cells.foreach {
          case MySqlAuditNull => add(1L)
          case MySqlAuditText(value) => add(1L); text(value)
          case b: MySqlAuditBinary => add(5L + b.length.toLong)
          case MySqlAuditTimestamp(value) => add(1L); text(value.toString)
        }
      }
    }

  /** The retained-observation validator encodes one string at a time; reserve its actual maximum scratch first. */
  private[mysql] def validateRetainedObservation(sets: Vector[MySqlAuditResultSet],
      before: MySqlCallerScope, after: MySqlCallerScope, auditAccount: MySqlFormat3ReadBudget): Unit =
    requireOpen()
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    val maximum = maxRetainedTextUtf8(before, after, sets)
    val fee = Math.addExact(512L, utf8LogicalFee(maximum))
    withAuditScratch(auditAccount, fee, s"schema$expectedFamily UNKNOWN retained-observation UTF-8 workspace") {
      validateRetainedObservation(sets, before, after)
    }

  private[mysql] val manifestLabels: Vector[String] = Vector(
    "configuration_sha256", "source_provenance_kind", "source_provenance_bytes", "source_provenance_sha256",
    "limit_profile_encoding", "limit_profile_bytes", "limit_profile_sha256", "max_region_bytes", "max_chunk_bytes",
    "max_decompressed_bytes", "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes", "max_native_artifact_bytes")

  private def expectedManifestCells: Vector[MySqlAuditCell] =
    requireOpen()
    Vector(
    configurationSha256, MySqlAuditText(sourceProvenanceKind), sourceProvenance, sourceProvenanceSha256,
    MySqlAuditText(SourceLimitProfile.ENCODING), MySqlAuditBinary(limitProfile.canonicalBytes()), MySqlAuditBinary(limitProfile.sha256())) ++
    Vector(limitProfile.maxRegionBytes(), limitProfile.maxChunkBytes(), limitProfile.maxDecompressedBytes(), limitProfile.maxNbtDepth(),
      limitProfile.maxMembers(), limitProfile.maxRetainedBytes(), limitProfile.maxSqlPacketBytes(), limitProfile.maxNativeArtifactBytes())
      .map(v => MySqlAuditText(v.toString))

  private[mysql] def validateManifest(set: MySqlAuditResultSet, row: MySqlAuditRow): Unit =
    requireOpen()
    if expectedFamily == 6 && !auditScratchActive then
      validateManifest(set, row, semantics.root)
      return
    val actual = manifestLabels.map { label =>
      val index = set.columns.indexWhere(_.label == label)
      if index < 0 || row.cells.size != set.columns.size then reject(s"schema$expectedFamily manifest shape is incomplete")
      row.cells(index)
    }
    if actual != expectedManifestCells then reject(s"schema$expectedFamily manifest configuration, source provenance or source-limit bytes differ from independent inputs")
    val profile = SourceLimitProfile.decode(
      actual(5).asInstanceOf[MySqlAuditBinary].bytes,
      actual(6).asInstanceOf[MySqlAuditBinary].bytes)
    if profile != limitProfile then reject(s"schema$expectedFamily decoded source-limit profile differs from exact renderer input")

  /** Account the expected-value clones, actual profile clones, and profile decode before building either graph. */
  private[mysql] def validateManifest(set: MySqlAuditResultSet, row: MySqlAuditRow,
      auditAccount: MySqlFormat3ReadBudget): Unit =
    requireOpen()
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    val provenanceBytes = semantics.text("sourceProvenanceHex").length.toLong / 2L
    val actualProfileBytes = profileBinaryLength(set, row, "limit_profile_bytes")
    val actualProfileShaBytes = profileBinaryLength(set, row, "limit_profile_sha256")
    val expectedArrayBytes = Math.addExact(386L, Math.multiplyExact(provenanceBytes, 2L))
    val actualArrayBytes = Math.addExact(actualProfileBytes, actualProfileShaBytes)
    val copiedBytes = Math.addExact(expectedArrayBytes, actualArrayBytes)
    val structures = Math.multiplyExact(96L, 15L + 15L + 5L)
    val fee = Math.addExact(Math.addExact(4096L, Math.multiplyExact(copiedBytes, 32L)), structures)
    withAuditScratch(auditAccount, fee, s"schema$expectedFamily UNKNOWN manifest binary clones/profile decode") {
      validateManifest(set, row)
    }

  private def samePrivilegeSets(actual: Vector[MySqlAuditResultSet]): Boolean =
    actual != null && actual.size == expectedPrivileges.size && actual.zip(expectedPrivileges).forall { (left, right) =>
      left != null && left.id == right.id && left.columns == right.columns && left.rows != null &&
        canonicalRows(left.rows.map(row => row.cells.map {
          case MySqlAuditText(value) => value
          case _ => throw new IllegalArgumentException(s"schema$expectedFamily observed privilege value must be text")
        })) == canonicalRows(right.rows.map(_.cells.map(_.asInstanceOf[MySqlAuditText].value)))
    }

  private def privilegeComparisonFee(actual: Vector[MySqlAuditResultSet], expected: Vector[MySqlAuditResultSet]): Long =
    var rows = 0L
    var cells = 0L
    var sortChars = 0L
    def collect(sets: Vector[MySqlAuditResultSet]): Unit =
      if sets != null then sets.foreach { set =>
        if set != null && set.rows != null then set.rows.foreach { row =>
          rows = Math.addExact(rows, 1L)
          if row != null && row.cells != null then
            cells = Math.addExact(cells, row.cells.size.toLong)
            var rowChars = 0L
            row.cells.foreach {
              case MySqlAuditText(value) if value != null => rowChars = Math.addExact(rowChars, value.length.toLong)
              case _ => ()
            }
            if row.cells.nonEmpty then rowChars = Math.addExact(rowChars, row.cells.size.toLong - 1L)
            sortChars = Math.addExact(sortChars, rowChars)
        }
      }
    collect(actual)
    collect(expected)
    // Existing project logical rule: 96 B per retained vector row, plus the B1 UTF-8 writer's
    // 32 B per character workspace for each simultaneous mkString sort-key graph.
    val rowAndCellVectors = Math.multiplyExact(Math.addExact(rows, cells), 96L)
    val setPairs = Math.multiplyExact(Math.addExact(
      if actual == null then 0L else actual.size.toLong,
      if expected == null then 0L else expected.size.toLong), 96L)
    Math.addExact(Math.addExact(4096L, Math.multiplyExact(sortChars, 32L)),
      Math.addExact(rowAndCellVectors, setPairs))

  private def maxRetainedTextUtf8(before: MySqlCallerScope, after: MySqlCallerScope,
      sets: Vector[MySqlAuditResultSet]): Long =
    var maximum = 0L
    def visit(value: String): Unit =
      if value != null then maximum = Math.max(maximum, utf8FeeLength(value))
    def visitScope(scope: MySqlCallerScope): Unit =
      if scope != null then
        scope.serverVersion.foreach(visit)
        visit(scope.defaultSchema); visit(scope.effectivePrincipal); visit(scope.effectiveRoles); visit(scope.connectedUser)
        scope.ownGrants.foreach(visit)
    visitScope(before); visitScope(after)
    if sets != null then sets.foreach { set =>
      if set != null then
        visit(set.id)
        if set.columns != null then set.columns.foreach { column =>
          if column != null then { visit(column.label); visit(column.typeName) }
        }
        if set.rows != null then set.rows.foreach { row =>
          if row != null && row.cells != null then row.cells.foreach {
            case MySqlAuditText(value) => visit(value)
            case _: MySqlAuditTimestamp => maximum = Math.max(maximum, 29L)
            case _ => ()
          }
        }
    }
    maximum

  /** Estimate without validating Unicode so the original encoder remains the failure boundary. */
  private def utf8FeeLength(value: String): Long =
    var bytes = 0L
    var index = 0
    while index < value.length do
      val current = value.charAt(index)
      if current <= 0x7f then bytes = Math.addExact(bytes, 1L)
      else if current <= 0x7ff then bytes = Math.addExact(bytes, 2L)
      else if Character.isHighSurrogate(current) && index + 1 < value.length &&
          Character.isLowSurrogate(value.charAt(index + 1)) then
        bytes = Math.addExact(bytes, 4L)
        index += 1
      else bytes = Math.addExact(bytes, 3L)
      index += 1
    bytes

  private def profileBinaryLength(set: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Long =
    if set == null || row == null || set.columns == null || row.cells == null then 0L
    else
      val index = set.columns.indexWhere(column => column != null && column.label == label)
      if index < 0 || index >= row.cells.size then 0L
      else row.cells(index) match
        case binary: MySqlAuditBinary => binary.length.toLong
        case _ => 0L

  private def utf8LogicalFee(bytes: Long): Long =
    Math.addExact(4096L, Math.multiplyExact(bytes, 32L))

  private def withAuditScratch[A](account: MySqlFormat3ReadBudget, amount: Long,
      context: String)(body: => A): A =
    if expectedFamily == 6 then
      requireOpen()
      val root = semantics.root
      if account == null || account.isClosed || (account.profile ne root.profile) ||
          !account.sharesAllocationLedger(root) || !((account eq root) || account.isCanonicalChildOf(root)) then
        throw new IllegalArgumentException("schema6 audit scratch requires its exact root/profile/ledger owner")
    account.reserveTemporaryMemory(amount, context + " BEFORE allocation")
    val previousScratch = auditScratchActive
    auditScratchActive = true
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      auditScratchActive = previousScratch
      try account.releaseTemporaryMemory(amount, context + " detached")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlSchemaAuditor.addSuppressed(primary, cleanup)

  private def canonicalRows(rows: Vector[Vector[String]]): Vector[Vector[String]] =
    rows.sortBy(_.mkString("\u0000"))

  private def text(set: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String =
    row.cells(set.columns.indexWhere(_.label == label)) match
      case MySqlAuditText(value) => value
      case _ => throw new IllegalArgumentException(s"schema$expectedFamily privilege $label must be text")

  override def toString: String = "MySqlDeploymentReceiptSchemaAuditInputs[redacted; independent schema5 validation only]"

final class MySqlDeploymentReceiptSchemaAuditInputs private[mysql] (
    binding: MySqlManifestConfigurationIdentity.DeploymentReceiptBinding, lease: MySqlRuntimeInputs.ConsumerLease)
    extends MySqlStaticSchemaAuditInputs(binding, lease, 5)

object MySqlDeploymentReceiptSchemaAuditInputs:
  private[mysql] def validateSemantics(s: MySqlDeploymentReceiptRuntimeContract.Semantics): Unit =
    s.requireAlive()
    val profileInputs=s.profile; val rendered=s.rendered
    val serverUuid=UUID.fromString(s.text("serverUuid"));val provisioningUuid=UUID.fromString(s.text("provisioningUuid"))
    val bindingUuid=UUID.fromString(s.text("bindingUuid"));val serverVersion=s.text("serverVersion");val schema=s.text("schema")
    val effectiveUser=s.text("effectiveUser");val connectedUser=s.text("connectedUser");val callerRoles=s.text("callerRoles")
    val ddlSha256=s.text("ddlSha256");val auditDefiner=s.text("auditDefiner");val auditDefinerRoles=s.text("auditDefinerRoles")
    val callerGrants=s.grants;val expectedPrivileges=s.privileges
    val sourceProvenanceKind=s.text("sourceProvenanceKind")
    val sourceProvenance=MySqlAuditBinary(s.source);val sourceProvenanceSha256=MySqlAuditBinary(s.sourceSha)
    val limitProfile=profileInputs.sourceLimitProfile
    require(profileInputs != null && rendered != null && (rendered.inputs eq profileInputs),
      "schema5 independent inputs must retain the exact rendered profile input")
    require(serverUuid != null && provisioningUuid != null && bindingUuid != null,
      "schema5 server, provisioning and binding identities are required")
    require(serverVersion == MySqlConnectionSource.SupportedServerVersion,
      "schema5 audit inputs require the explicitly supported MySQL server version")
    require(schema == profileInputs.schema && MySqlSchemaAuditor.SchemaIdentifier.matches(schema),
      "schema5 audit schema differs from the exact renderer input")
    require(effectiveUser != null && effectiveUser.nonEmpty && connectedUser != null && connectedUser.nonEmpty &&
      callerRoles != null && callerRoles.nonEmpty, "schema5 caller identity and roles are explicit")
    require(ddlSha256 != null && ddlSha256.equalsIgnoreCase(rendered.sha256) && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256),
      "schema5 DDL identity must equal the actual generated source SHA-256")
    require(auditDefiner == profileInputs.accounts.auditDefiner.identity && auditDefinerRoles != null && auditDefinerRoles.nonEmpty,
      "schema5 audit definer and role identity must match the generated profile")
    require(callerGrants != null && callerGrants.nonEmpty && callerGrants.forall(g => g != null && g.nonEmpty) &&
      callerGrants.distinct.size == callerGrants.size, "schema5 caller SHOW GRANTS input must be complete and unique")
    require(expectedPrivileges != null && expectedPrivileges.map(_.id) == MySqlAuditCoverage.Format3PrivilegeResultSetIds,
      "schema5 requires the exact ordered four privilege result sets")
    expectedPrivileges.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).foreach { (set, labels) =>
      require(set != null && set.columns != null && set.columns.map(_.label) == labels && set.rows != null,
        "schema5 privilege resultset labels differ from the fixed RS15-RS18 contract")
      require(set.rows.forall(r => r != null && r.cells != null && r.cells.size == labels.size &&
        r.cells.forall(_.isInstanceOf[MySqlAuditText])), "schema5 privileges require complete non-NULL text rows")
    }
    MySqlSchemaAuditor.rejectDuplicateRows(expectedPrivileges)
    val managedSource = s.family == MySqlDeploymentReceiptRuntimeContract.DbFirstFamily &&
      rendered.managedExistingDatabase && sourceProvenanceKind == "MANAGED_INSTALL"
    require(managedSource || sourceProvenanceKind != null &&
      Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT").contains(sourceProvenanceKind),
      "schema5 source provenance kind is outside the closed manifest vocabulary")
    require(sourceProvenance != null && sourceProvenanceSha256 != null && sourceProvenanceSha256.length == 32 &&
      sourceProvenance.hasSha256(sourceProvenanceSha256), "schema5 source provenance bytes and SHA-256 must agree")
    require(limitProfile != null && (limitProfile eq profileInputs.sourceLimitProfile),
      "schema5 source profile must be the same explicit object used for generation")
    rendered.validateSecurity(schema, expectedPrivileges)

  private[mysql] def validateSecurity(profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      generated: MySqlDeploymentReceiptSchemaProfile.Rendered, targetSchema: String,
      privileges: Vector[MySqlAuditResultSet]): Unit =
    require(profile != null && generated != null && (generated.inputs eq profile))
    new Security(profile, MySqlDeploymentReceiptSchemaProfile.expectedPrivilegeRows(generated),
      MySqlDeploymentReceiptSchemaProfile.TableNames, targetSchema, privileges).validatePrivileges()

  /** schema6 复用角色规则，但完整权限预期与表闭集来自其自己的 renderer。 */
  private[mysql] def validateSecurity(profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      generated: MySqlDbFirstSchemaProfile.Rendered, targetSchema: String,
      privileges: Vector[MySqlAuditResultSet]): Unit =
    require(profile != null && generated != null && (generated.inputs eq profile))
    new Security(profile, MySqlDbFirstSchemaProfile.expectedPrivilegeRows(generated),
      MySqlDbFirstSchemaProfile.TableNames, targetSchema, privileges).validatePrivileges()

  private final class Security(val profileInputs: MySqlDeploymentReceiptSchemaProfile.Inputs,
      val generated: Vector[Vector[Vector[String]]], val tableNames: Vector[String], val schema: String,
      val expectedPrivileges: Vector[MySqlAuditResultSet]):
    require(profileInputs != null && generated != null && tableNames != null && schema == profileInputs.schema)
    require(expectedPrivileges != null && expectedPrivileges.map(_.id) == MySqlAuditCoverage.Format3PrivilegeResultSetIds)
    expectedPrivileges.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).foreach { (set, labels) =>
      require(set != null && set.columns != null && set.columns.map(_.label) == labels && set.rows != null)
      require(set.rows.forall(r => r != null && r.cells != null && r.cells.size == labels.size &&
        r.cells.forall(_.isInstanceOf[MySqlAuditText])))
    }
    MySqlSchemaAuditor.rejectDuplicateRows(expectedPrivileges)
    def validatePrivileges(): Unit =
      val supplied = expectedPrivileges.map(set => set.rows.map(row => row.cells.map {
        case MySqlAuditText(value) => value
        case _ => throw new IllegalArgumentException("schema5 expected privilege value must be text")
      }))
      if supplied.map(canonicalRows) != generated.map(canonicalRows) then
        throw new IllegalArgumentException("schema5 privilege expectations do not equal every rendered direct grant")
      val accounts = profileInputs.accounts
      val byGrantee = accounts.all.map(a => a.grantee -> a).toMap
      val tableSet = expectedPrivileges(0)
      tableSet.rows.foreach { row =>
        val grantee = text(tableSet, row, "GRANTEE")
        val account = byGrantee.getOrElse(grantee, throw new IllegalArgumentException("schema5 table grant names an unapproved account"))
        if text(tableSet, row, "TABLE_SCHEMA") != schema || !tableNames.contains(text(tableSet, row, "TABLE_NAME")) ||
          text(tableSet, row, "IS_GRANTABLE") != "NO" then
          throw new IllegalArgumentException("schema5 direct table grants escape target schema or carry grant option")
        val kind = text(tableSet, row, "PRIVILEGE_TYPE")
        val allowed =
          if account == accounts.guardDefiner || account == accounts.auditDefiner then Set("SELECT", "TRIGGER")
          else if account == accounts.runtime then Set("SELECT", "INSERT", "UPDATE", "DELETE")
          else if account == accounts.provisioner then Set("SELECT", "INSERT", "UPDATE", "CREATE", "INDEX", "TRIGGER", "ALTER", "REFERENCES")
          else if account == accounts.importer then Set("SELECT", "INSERT", "UPDATE")
          else Set("SELECT", "TRIGGER")
        if !allowed.contains(kind) then throw new IllegalArgumentException("schema5 direct table privilege exceeds its closed role set")
      }
      val columnSet = expectedPrivileges(1)
      columnSet.rows.foreach { row =>
        val account = byGrantee.getOrElse(text(columnSet, row, "GRANTEE"),
          throw new IllegalArgumentException("schema5 column grant names an unapproved account"))
        val table = text(columnSet, row, "TABLE_NAME")
        val column = text(columnSet, row, "COLUMN_NAME")
        val allowedQuotaColumn = account == accounts.operationQuotaUpdater && table == MySqlDeploymentReceiptSchemaDefinition.QuotaTable &&
          Set("used_records", "quota_revision").contains(column)
        val allowedProvisionerManifestColumn = account == accounts.provisioner && table == "fp_schema_manifest" &&
          Set("bootstrap_state", "guard_manifest_sha256", "import_manifest_sha256", "import_state", "import_operation_uuid",
            "import_snapshot_sha256", "import_receipt_sha256").contains(column)
        val allowedUpdate = account == accounts.runtime || account == accounts.importer || allowedQuotaColumn || allowedProvisionerManifestColumn
        if text(columnSet, row, "TABLE_SCHEMA") != schema || !tableNames.contains(table) ||
          text(columnSet, row, "PRIVILEGE_TYPE") != "UPDATE" || text(columnSet, row, "IS_GRANTABLE") != "NO" ||
          !allowedUpdate then
          throw new IllegalArgumentException("schema5 column privilege is outside the closed UPDATE role set")
      }
      val schemaSet = expectedPrivileges(2)
      schemaSet.rows.foreach { row =>
        val account = byGrantee.getOrElse(text(schemaSet, row, "GRANTEE"),
          throw new IllegalArgumentException("schema5 schema grant names an unapproved account"))
        val db = text(schemaSet, row, "TABLE_SCHEMA")
        val kind = text(schemaSet, row, "PRIVILEGE_TYPE")
        val allowed = (account == accounts.auditDefiner && db == schema && Set("SELECT", "TRIGGER").contains(kind)) ||
          (account == accounts.auditDefiner && db == "mysql" && kind == "SELECT") ||
          (account == accounts.runtime && db == schema && kind == "LOCK TABLES") ||
          (account == accounts.provisioner && db == schema && kind == "CREATE ROUTINE")
        if text(schemaSet, row, "IS_GRANTABLE") != "NO" || !allowed then
          throw new IllegalArgumentException("schema5 schema-level grant exceeds the exact audit/runtime visibility set")
      }
      val userSet = expectedPrivileges(3)
      val usage = Vector.newBuilder[String]
      var auditShowRoutineCount = 0
      userSet.rows.foreach { row =>
        val grantee = text(userSet, row, "GRANTEE")
        val kind = text(userSet, row, "PRIVILEGE_TYPE")
        if !byGrantee.contains(grantee) || text(userSet, row, "TABLE_CATALOG") != "def" ||
          text(userSet, row, "IS_GRANTABLE") != "NO" then
          throw new IllegalArgumentException("schema5 global or dynamic privilege is not approved")
        kind match
          case "USAGE" => usage += grantee
          case "SHOW_ROUTINE" if grantee == accounts.auditDefiner.grantee => auditShowRoutineCount += 1
          case _ => throw new IllegalArgumentException("schema5 global or dynamic privilege is not approved")
      }
      val usageRows = usage.result()
      if usageRows.distinct.size != 6 || usageRows.size != 6 || auditShowRoutineCount != 1 ||
        userSet.rows.size != 7 then
        throw new IllegalArgumentException("schema5 requires six USAGE rows and only the audit definer SHOW_ROUTINE grant")

    private def canonicalRows(rows: Vector[Vector[String]]): Vector[Vector[String]] = rows.sortBy(_.mkString("\u0000"))
    private def text(set: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String =
      row.cells(set.columns.indexWhere(_.label == label)) match
        case MySqlAuditText(value) => value
        case _ => throw new IllegalArgumentException("schema5 privilege must be text")
