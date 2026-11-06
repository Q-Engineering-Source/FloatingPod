package com.wi.floatingpod.storage.mysql

import java.io.{ByteArrayOutputStream, IOException, InputStream, Reader}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets
import java.nio.charset.CodingErrorAction
import java.security.MessageDigest
import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLException, SQLNonTransientConnectionException, Statement, Types}
import java.time.{Duration, LocalDateTime}
import java.util.{Arrays, HexFormat, Locale, UUID}

/** 一个 audit resultset 的只读 JDBC 字段形状。 */
final case class MySqlAuditColumn(
    label: String,
    jdbcType: Int,
    typeName: String,
    precision: Int,
    scale: Int,
    nullable: Int,
    signed: Boolean
)

sealed trait MySqlAuditCell
case object MySqlAuditNull extends MySqlAuditCell
private[mysql] enum MySqlAuditJdbcValueKind:
  case Timestamp, Binary, Numeric, Text, SqlNull

final case class MySqlAuditText(value: String) extends MySqlAuditCell:
  require(value != null, "audit text cell cannot be null")
  override def toString: String = "MySqlAuditText[redacted]"

final case class MySqlAuditTimestamp(value: LocalDateTime) extends MySqlAuditCell:
  require(value != null, "audit timestamp cell cannot be null")

final class MySqlAuditBinary private (private val value: Array[Byte]) extends MySqlAuditCell:
  private[mysql] def length: Int = value.length
  /** 仅在包内对照不可变SQL快照，避免为批准输入再分配一份完整blob。 */
  private[mysql] def equalsBytes(other: Array[Byte]): Boolean = other != null && Arrays.equals(value, other)
  private[mysql] def equalsFormat3Binary(other: MySqlFormat3Binary): Boolean = other != null && other.equalsBytes(value)
  /** 仅分配固定32字节摘要；不复制完整来源证明或profile payload。 */
  private[mysql] def hasSha256(expected: MySqlAuditBinary): Boolean =
    expected != null && expected.length == 32 && MessageDigest.isEqual(
      MessageDigest.getInstance("SHA-256").digest(value), expected.value)
  def bytes: Array[Byte] = value.clone()
  override def equals(other: Any): Boolean = other match
    case that: MySqlAuditBinary => Arrays.equals(value, that.value)
    case _ => false
  override def hashCode(): Int = Arrays.hashCode(value)
  override def toString: String = s"MySqlAuditBinary(${value.length} bytes)"

object MySqlAuditBinary:
  def apply(bytes: Array[Byte]): MySqlAuditBinary =
    require(bytes != null, "audit binary cell cannot be null")
    new MySqlAuditBinary(bytes.clone())

final case class MySqlAuditRow(cells: Vector[MySqlAuditCell])

/** Resultsets preserve JDBC column order, row order, SQL NULL, text and binary values. */
final case class MySqlAuditResultSet(
    id: String,
    columns: Vector[MySqlAuditColumn],
    rows: Vector[MySqlAuditRow]
)

/** v4 coverage is deliberately narrower than the complete routine namespace. */
object MySqlAuditCoverage:
  val V4 = "format1:all-fixed-schema-tables-and-static-objects;all-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed"
  val Format2 = "format2:all-nine-fixed-schema-tables-and-static-objects;all-eighteen-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed"
  val Format3 = "format3:all-twenty-fixed-schema-tables-and-static-objects;all-sixty-fixed-schema-triggers;required-auditself-only;other-routine-absence-unclaimed"
  val StaticResultSetIds = Vector(
    "RS02_TABLES",
    "RS03_COLUMNS",
    "RS04_STATISTICS",
    "RS05_TABLE_CONSTRAINTS",
    "RS06_KEY_COLUMN_USAGE",
    "RS07_REFERENTIAL_CONSTRAINTS",
    "RS08_CHECK_CONSTRAINTS",
    "RS09_TRIGGERS",
    "RS10_AUDITSELF",
    "RS11_AUDITSELF_PARAMETERS"
  )
  val AllResultSetIds = Vector(
    "RS01_HEADER",
    "RS02_TABLES",
    "RS03_COLUMNS",
    "RS04_STATISTICS",
    "RS05_TABLE_CONSTRAINTS",
    "RS06_KEY_COLUMN_USAGE",
    "RS07_REFERENTIAL_CONSTRAINTS",
    "RS08_CHECK_CONSTRAINTS",
    "RS09_TRIGGERS",
    "RS10_AUDITSELF",
    "RS11_AUDITSELF_PARAMETERS",
    "RS12_BINDING",
    "RS13_WRITER",
    "RS14_FOOTER"
  )

  val Format3PrivilegeResultSetIds = Vector("RS15_TABLE_PRIVILEGES", "RS16_COLUMN_PRIVILEGES", "RS17_SCHEMA_PRIVILEGES", "RS18_USER_PRIVILEGES")
  def resultSetIds(profile: MySqlSchemaAuditProfile): Vector[String] =
    if profile.extendedAudit then AllResultSetIds ++ Format3PrivilegeResultSetIds else AllResultSetIds

/** schema 格式与物理绑定格式是彼此独立的合同。 */
enum MySqlSchemaAuditProfile(
    val schemaFormat: Int,
    val coverage: String,
    val requiredProcedure: String,
    val hashDomain: String,
    val expectedTableCount: Option[Int],
    val expectedTriggerCount: Option[Int]
):
  def extendedAudit: Boolean = this == Format3 || this == ScriptPublication1 || this == DeploymentReceipt1 || this == DbFirst1

  case DbFirst1 extends MySqlSchemaAuditProfile(
    6, MySqlDbFirstSchemaDefinition.Coverage, MySqlDbFirstSchemaDefinition.AuditRoutine,
    MySqlDbFirstSchemaDefinition.AuditDomain, Some(27), Some(81)
  )

  case ScriptPublication1 extends MySqlSchemaAuditProfile(
    4, MySqlScriptSchemaProfile.Coverage, MySqlScriptSchemaProfile.AuditRoutine,
    MySqlScriptSchemaProfile.AuditDomain, Some(24), Some(72)
  )

  case DeploymentReceipt1 extends MySqlSchemaAuditProfile(
    5, MySqlDeploymentReceiptSchemaProfile.Coverage, MySqlDeploymentReceiptSchemaProfile.AuditRoutine,
    MySqlDeploymentReceiptSchemaProfile.AuditDomain, Some(26), Some(78)
  )

  case Format1 extends MySqlSchemaAuditProfile(
    1,
    MySqlAuditCoverage.V4,
    "fp_audit_schema_format1",
    "FloatingPod-MySQL-Audit-Canonical-v4",
    None,
    None
  )
  case Format2 extends MySqlSchemaAuditProfile(
    2,
    MySqlAuditCoverage.Format2,
    "fp_audit_schema_format2",
    "FloatingPod-MySQL-Audit-Canonical-format2-v1",
    Some(9),
    Some(18)
  )

  case Format3 extends MySqlSchemaAuditProfile(
    3, MySqlAuditCoverage.Format3, "fp_audit_schema_format3",
    "FloatingPod-MySQL-Audit-Canonical-format3-v1", Some(20), Some(60)
  )

/** 显式目标物理格式；Format2 的 Fresh 语义由调用方的真实创建合同负责。 */
enum MySqlExpectedPhysicalFormat(val version: Int):
  case Format1 extends MySqlExpectedPhysicalFormat(1)
  case Format2 extends MySqlExpectedPhysicalFormat(2)

object MySqlSchemaAuditProfile:
  private[mysql] def matchesTarget(
      expectedProfile: MySqlSchemaAuditProfile,
      expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
      actualProfile: MySqlSchemaAuditProfile,
      actualPhysicalFormat: MySqlExpectedPhysicalFormat
  ): Boolean =
    expectedProfile != null && expectedPhysicalFormat != null &&
      actualProfile == expectedProfile && actualPhysicalFormat == expectedPhysicalFormat

/** 从另一条已审核配置路径加载的对象基线；不由实际 CALL 结果生成。 */
final class MySqlApprovedObjectManifest private[mysql] (
    val profile: MySqlSchemaAuditProfile,
    val resultSets: Vector[MySqlAuditResultSet]
):
  require(profile != null, "approved MySQL schema audit profile is required")
  require(resultSets != null, "approved MySQL object manifest is required")
  require(resultSets.forall(_ != null), "approved MySQL object manifest contains a missing resultset")
  require(resultSets.map(_.id) == MySqlAuditCoverage.StaticResultSetIds, "approved manifest must use the exact ordered static-object coverage")
  resultSets.foreach(resultSet => MySqlSchemaAuditor.validateStaticSetShape(resultSet, profile))
  MySqlSchemaAuditor.validateSelfDescriptions(resultSets, requireTriggerRows = true, profile = profile)
  MySqlSchemaAuditor.validateProfileObjectCounts(profile, resultSets)
  MySqlSchemaAuditor.rejectDuplicateRows(resultSets)
  private[mysql] val canonicalSha256: String = MySqlSchemaAuditor.canonicalSha256(profile, resultSets)
  override def toString: String = s"MySqlApprovedObjectManifest(${canonicalSha256.take(12)}…)"

object MySqlApprovedObjectManifest:
  def apply(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.Format1, resultSets)

  def forFormat2(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.Format2, resultSets)

  def forScriptPublication1(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.ScriptPublication1, resultSets)

  def forDeploymentReceipt1(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.DeploymentReceipt1, resultSets)

  def forDbFirst1(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.DbFirst1, resultSets)

  def forFormat3(resultSets: Vector[MySqlAuditResultSet]): MySqlApprovedObjectManifest =
    new MySqlApprovedObjectManifest(MySqlSchemaAuditProfile.Format3, resultSets)

/** 已独立批准的 MySQL schema profile、角色及固定对象基线。 */
final class MySqlSchemaAuditExpectation private[mysql] (
    val profile: MySqlSchemaAuditProfile,
    val expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
    val serverUuid: UUID,
    val serverVersion: String,
    val schema: String,
    val effectiveUser: String,
    val callerRoles: String,
    val provisioningUuid: UUID,
    val bindingUuid: UUID,
    val ddlSha256: String,
    val auditDefiner: String,
    val auditDefinerRoles: String,
    val expectedImport: MySqlExpectedImportState,
    val objects: MySqlApprovedObjectManifest,
    val format3Inputs: Option[MySqlFormat3AuditInputs] = None,
    val scriptSchemaInputs: Option[MySqlScriptSchemaAuditInputs] = None,
    val deploymentReceiptInputs: Option[MySqlDeploymentReceiptSchemaAuditInputs] = None,
    val dbFirstInputs: Option[MySqlDbFirstSchemaAuditInputs] = None
):
  require(profile != null && expectedPhysicalFormat != null, "approved schema profile and physical target are required")
  require(profile != MySqlSchemaAuditProfile.DbFirst1 ||
    (expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && dbFirstInputs.nonEmpty &&
      format3Inputs.isEmpty && scriptSchemaInputs.isEmpty && deploymentReceiptInputs.isEmpty),
    "DbFirst1 requires its independent complete schema6 security consumer")
  require(dbFirstInputs.isEmpty || profile == MySqlSchemaAuditProfile.DbFirst1,
    "schema6 consumer cannot qualify another family")
  require(serverUuid != null && provisioningUuid != null && bindingUuid != null, "approved MySQL identities are required")
  require(serverVersion != null && serverVersion.nonEmpty, "approved MySQL server version is required")
  require(serverVersion == MySqlConnectionSource.SupportedServerVersion, "only the explicitly supported MySQL server version may be approved")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "approved schema name is not a safe fixed identifier")
  require(effectiveUser != null && effectiveUser.nonEmpty, "approved effective account is required")
  require(callerRoles != null && callerRoles.nonEmpty, "approved caller roles are required")
  require(ddlSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256), "approved DDL SHA-256 is malformed")
  require(auditDefiner != null && auditDefiner.nonEmpty, "approved audit definer is required")
  require(auditDefinerRoles != null && auditDefinerRoles.nonEmpty, "approved audit definer roles are required")
  require(expectedImport != null, "approved MySQL import state is required")
  require(objects != null, "approved MySQL object manifest is required")
  require(objects.profile == profile, "approved object manifest profile differs from the schema expectation")
  require(profile != MySqlSchemaAuditProfile.Format1 || expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format1,
    "schema format 1 only permits physical format 1")
  require(profile != MySqlSchemaAuditProfile.Format3 || (expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && format3Inputs.nonEmpty),
    "format 3 requires physical2 and independent complete audit inputs")
  format3Inputs.foreach(_.requireIdentity(serverUuid, serverVersion, schema, effectiveUser, callerRoles, provisioningUuid, bindingUuid, ddlSha256, auditDefiner))
  require(profile != MySqlSchemaAuditProfile.ScriptPublication1 ||
    (expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && scriptSchemaInputs.nonEmpty && format3Inputs.isEmpty),
    "ScriptPublication1 requires independent schema4 security inputs and physical2")
  require(scriptSchemaInputs.isEmpty || profile == MySqlSchemaAuditProfile.ScriptPublication1,
    "schema4 security inputs cannot qualify an older profile")
  scriptSchemaInputs.foreach(_.requireIdentity(serverUuid, serverVersion, schema, effectiveUser, callerRoles,
    provisioningUuid, bindingUuid, ddlSha256, auditDefiner))
  require(profile != MySqlSchemaAuditProfile.DeploymentReceipt1 ||
    (expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && deploymentReceiptInputs.nonEmpty &&
      format3Inputs.isEmpty && scriptSchemaInputs.isEmpty),
    "DeploymentReceipt1 requires independent schema5 security inputs and physical2")
  require(deploymentReceiptInputs.isEmpty || profile == MySqlSchemaAuditProfile.DeploymentReceipt1,
    "schema5 security inputs cannot qualify an older profile")
  deploymentReceiptInputs.foreach(_.requireIdentity(serverUuid, serverVersion, schema, effectiveUser, callerRoles,
    provisioningUuid, bindingUuid, ddlSha256, auditDefiner))
  dbFirstInputs.foreach { inputs =>
    inputs.requireIdentity(serverUuid, serverVersion, schema, effectiveUser, callerRoles,
      provisioningUuid, bindingUuid, ddlSha256, auditDefiner)
    require(auditDefinerRoles == inputs.auditDefinerRoles && expectedImport == MySqlExpectedNoImport,
      "schema6 audit roles/import differ from its complete pinned fresh security inputs")
  }
  private[mysql] def auditLimitProfile = format3Inputs.map(_.limitProfile).orElse(scriptSchemaInputs.map(_.limitProfile))
    .orElse(deploymentReceiptInputs.map(_.limitProfile)).orElse(dbFirstInputs.map(_.limitProfile))
  private[mysql] def validateCapturedCallerGrants(grants: Vector[String]): Unit =
    format3Inputs match
      case Some(inputs) => inputs.validateCapturedCallerGrants(grants)
      case None =>
        if dbFirstInputs.nonEmpty then dbFirstInputs.foreach(_.validateCapturedCallerGrants(grants))
        else if deploymentReceiptInputs.nonEmpty then deploymentReceiptInputs.foreach(_.validateCapturedCallerGrants(grants))
        else if grants.isEmpty || grants.distinct.size != grants.size then
          throw new SQLNonTransientConnectionException("SHOW GRANTS is empty or duplicated")
  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope, sets: Vector[MySqlAuditResultSet]): Unit =
    format3Inputs.foreach(_.validateSecurity(before, after, sets))
    scriptSchemaInputs.foreach(_.validateSecurity(before, after, sets))
    deploymentReceiptInputs.foreach(_.validateSecurity(before, after, sets))
    dbFirstInputs.foreach(_.validateSecurity(before, after, sets))
  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope,
      sets: Vector[MySqlAuditResultSet], auditAccount: MySqlFormat3ReadBudget): Unit =
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    if profile == MySqlSchemaAuditProfile.DeploymentReceipt1 then
      deploymentReceiptInputs.getOrElse(throw new IllegalStateException("schema5 audit inputs are unavailable"))
        .validateSecurity(before, after, sets, auditAccount)
    else if profile == MySqlSchemaAuditProfile.DbFirst1 then
      dbFirstInputs.getOrElse(throw new IllegalStateException("schema6 audit inputs are unavailable"))
        .validateSecurity(before, after, sets, auditAccount)
    else validateSecurity(before, after, sets)
  private[mysql] def validateRetained(sets: Vector[MySqlAuditResultSet], before: MySqlCallerScope, after: MySqlCallerScope): Unit =
    format3Inputs.foreach(_.validateRetainedObservation(sets, before, after))
    scriptSchemaInputs.foreach(_.validateRetainedObservation(sets, before, after))
    deploymentReceiptInputs.foreach(_.validateRetainedObservation(sets, before, after))
    dbFirstInputs.foreach(_.validateRetainedObservation(sets, before, after))
  private[mysql] def validateRetained(sets: Vector[MySqlAuditResultSet], before: MySqlCallerScope,
      after: MySqlCallerScope, auditAccount: MySqlFormat3ReadBudget): Unit =
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    if profile == MySqlSchemaAuditProfile.DeploymentReceipt1 then
      deploymentReceiptInputs.getOrElse(throw new IllegalStateException("schema5 audit inputs are unavailable"))
        .validateRetainedObservation(sets, before, after, auditAccount)
    else if profile == MySqlSchemaAuditProfile.DbFirst1 then
      dbFirstInputs.getOrElse(throw new IllegalStateException("schema6 audit inputs are unavailable"))
        .validateRetainedObservation(sets, before, after, auditAccount)
    else validateRetained(sets, before, after)
  private[mysql] def validateSourceManifest(set: MySqlAuditResultSet, row: MySqlAuditRow): Unit =
    format3Inputs.foreach(_.validateManifest(set, row))
    scriptSchemaInputs.foreach(_.validateManifest(set, row))
    deploymentReceiptInputs.foreach(_.validateManifest(set, row))
    dbFirstInputs.foreach(_.validateManifest(set, row))
  private[mysql] def validateSourceManifest(set: MySqlAuditResultSet, row: MySqlAuditRow,
      auditAccount: MySqlFormat3ReadBudget): Unit =
    if auditAccount == null then throw new IllegalArgumentException("original schema5 audit DATA account required")
    if profile == MySqlSchemaAuditProfile.DeploymentReceipt1 then
      deploymentReceiptInputs.getOrElse(throw new IllegalStateException("schema5 audit inputs are unavailable"))
        .validateManifest(set, row, auditAccount)
    else if profile == MySqlSchemaAuditProfile.DbFirst1 then
      dbFirstInputs.getOrElse(throw new IllegalStateException("schema6 audit inputs are unavailable"))
        .validateManifest(set, row, auditAccount)
    else validateSourceManifest(set, row)
  override def toString: String = s"MySqlSchemaAuditExpectation(schema=$schema, schemaFormat=${profile.schemaFormat}, physicalFormat=${expectedPhysicalFormat.version}, objects=${objects.canonicalSha256.take(12)}…)"

object MySqlSchemaAuditExpectation:
  def apply(
      serverUuid: UUID,
      serverVersion: String,
      schema: String,
      effectiveUser: String,
      callerRoles: String,
      provisioningUuid: UUID,
      bindingUuid: UUID,
      ddlSha256: String,
      auditDefiner: String,
      auditDefinerRoles: String,
      expectedImport: MySqlExpectedImportState,
      objects: MySqlApprovedObjectManifest
  ): MySqlSchemaAuditExpectation =
    require(ddlSha256 != null, "approved DDL SHA-256 is required")
    new MySqlSchemaAuditExpectation(
      MySqlSchemaAuditProfile.Format1,
      MySqlExpectedPhysicalFormat.Format1,
      serverUuid,
      serverVersion,
      schema,
      effectiveUser,
      callerRoles,
      provisioningUuid,
      bindingUuid,
      ddlSha256.toLowerCase(java.util.Locale.ROOT),
      auditDefiner,
      auditDefinerRoles,
      expectedImport,
      objects
    )

  /** Format 2 physical target is explicit: choose 1 for verified old restore, 2 only for a true fresh receipt. */
  def forFormat2(
      serverUuid: UUID,
      serverVersion: String,
      schema: String,
      effectiveUser: String,
      callerRoles: String,
      provisioningUuid: UUID,
      bindingUuid: UUID,
      ddlSha256: String,
      auditDefiner: String,
      auditDefinerRoles: String,
      expectedImport: MySqlExpectedImportState,
      objects: MySqlApprovedObjectManifest,
      expectedPhysicalFormat: MySqlExpectedPhysicalFormat
  ): MySqlSchemaAuditExpectation =
    require(ddlSha256 != null, "approved DDL SHA-256 is required")
    new MySqlSchemaAuditExpectation(
      MySqlSchemaAuditProfile.Format2,
      expectedPhysicalFormat,
      serverUuid,
      serverVersion,
      schema,
      effectiveUser,
      callerRoles,
      provisioningUuid,
      bindingUuid,
      ddlSha256.toLowerCase(java.util.Locale.ROOT),
      auditDefiner,
      auditDefinerRoles,
      expectedImport,
      objects
    )

  def forFormat3(serverUuid: UUID, serverVersion: String, schema: String, effectiveUser: String,
      callerRoles: String, provisioningUuid: UUID, bindingUuid: UUID, ddlSha256: String,
      auditDefiner: String, auditDefinerRoles: String, expectedImport: MySqlExpectedImportState,
      objects: MySqlApprovedObjectManifest, inputs: MySqlFormat3AuditInputs): MySqlSchemaAuditExpectation =
    require(inputs != null, "independent format3 audit inputs are required")
    new MySqlSchemaAuditExpectation(MySqlSchemaAuditProfile.Format3, MySqlExpectedPhysicalFormat.Format2,
      serverUuid, serverVersion, schema, effectiveUser, callerRoles, provisioningUuid, bindingUuid,
      ddlSha256.toLowerCase(Locale.ROOT), auditDefiner, auditDefinerRoles, expectedImport, objects, Some(inputs))

  def forDeploymentReceipt1(serverUuid: UUID, serverVersion: String, schema: String, effectiveUser: String,
      callerRoles: String, provisioningUuid: UUID, bindingUuid: UUID, ddlSha256: String,
      auditDefiner: String, auditDefinerRoles: String, expectedImport: MySqlExpectedImportState,
      objects: MySqlApprovedObjectManifest, inputs: MySqlDeploymentReceiptSchemaAuditInputs): MySqlSchemaAuditExpectation =
    require(inputs != null, "independent schema5 audit inputs are required")
    new MySqlSchemaAuditExpectation(MySqlSchemaAuditProfile.DeploymentReceipt1, MySqlExpectedPhysicalFormat.Format2,
      serverUuid, serverVersion, schema, effectiveUser, callerRoles, provisioningUuid, bindingUuid,
      ddlSha256.toLowerCase(Locale.ROOT), auditDefiner, auditDefinerRoles, expectedImport, objects,
      deploymentReceiptInputs = Some(inputs))

  def forDbFirst1(serverUuid: UUID, serverVersion: String, schema: String, effectiveUser: String,
      callerRoles: String, provisioningUuid: UUID, bindingUuid: UUID, ddlSha256: String,
      auditDefiner: String, auditDefinerRoles: String, expectedImport: MySqlExpectedImportState,
      objects: MySqlApprovedObjectManifest, inputs: MySqlDbFirstSchemaAuditInputs): MySqlSchemaAuditExpectation =
    require(inputs != null, "independent schema6 audit consumer required")
    new MySqlSchemaAuditExpectation(MySqlSchemaAuditProfile.DbFirst1, MySqlExpectedPhysicalFormat.Format2,
      serverUuid, serverVersion, schema, effectiveUser, callerRoles, provisioningUuid, bindingUuid,
      ddlSha256.toLowerCase(Locale.ROOT), auditDefiner, auditDefinerRoles, expectedImport, objects,
      dbFirstInputs = Some(inputs))

final case class MySqlCallerScope(
    serverUuid: UUID,
    connectionId: Long,
    defaultSchema: String,
    effectivePrincipal: String,
    effectiveRoles: String,
    connectedUser: String,
    serverVersion: Option[String] = None,
    ownGrants: Vector[String] = Vector.empty
):
  /** 旧 JVM 六参数入口仅构造 legacy 身份；不能满足 Format3 的 mandatory security gate。 */
  def this(serverUuid: UUID, connectionId: Long, defaultSchema: String,
      effectivePrincipal: String, effectiveRoles: String, connectedUser: String) =
    this(serverUuid, connectionId, defaultSchema, effectivePrincipal, effectiveRoles, connectedUser, None, Vector.empty)
  /** 旧 copy descriptor 保留接收对象已有完整身份/grants，不降级格式3。 */
  def copy(serverUuid: UUID, connectionId: Long, defaultSchema: String,
      effectivePrincipal: String, effectiveRoles: String, connectedUser: String): MySqlCallerScope =
    new MySqlCallerScope(serverUuid, connectionId, defaultSchema, effectivePrincipal, effectiveRoles, connectedUser,
      this.serverVersion, this.ownGrants)
  def copy(serverUuid: UUID = this.serverUuid, connectionId: Long = this.connectionId,
      defaultSchema: String = this.defaultSchema, effectivePrincipal: String = this.effectivePrincipal,
      effectiveRoles: String = this.effectiveRoles, connectedUser: String = this.connectedUser,
      serverVersion: Option[String] = this.serverVersion, ownGrants: Vector[String] = this.ownGrants): MySqlCallerScope =
    new MySqlCallerScope(serverUuid, connectionId, defaultSchema, effectivePrincipal, effectiveRoles, connectedUser, serverVersion, ownGrants)

object MySqlCallerScope:
  def apply(serverUuid: UUID, connectionId: Long, defaultSchema: String,
      effectivePrincipal: String, effectiveRoles: String, connectedUser: String): MySqlCallerScope =
    new MySqlCallerScope(serverUuid, connectionId, defaultSchema, effectivePrincipal, effectiveRoles, connectedUser, None, Vector.empty)
  def apply(serverUuid: UUID, connectionId: Long, defaultSchema: String, effectivePrincipal: String,
      effectiveRoles: String, connectedUser: String, serverVersion: Option[String] = None,
      ownGrants: Vector[String] = Vector.empty): MySqlCallerScope =
    new MySqlCallerScope(serverUuid, connectionId, defaultSchema, effectivePrincipal, effectiveRoles, connectedUser, serverVersion, ownGrants)

sealed trait MySqlObservedPhysicalBinding
case object MySqlNoPhysicalBinding extends MySqlObservedPhysicalBinding
final case class MySqlLegacyPhysicalBinding(bindingUuid: UUID, formatVersion: Int) extends MySqlObservedPhysicalBinding
final case class MySqlCertifiedPhysicalBinding(bindingUuid: UUID, formatVersion: Int) extends MySqlObservedPhysicalBinding

sealed trait MySqlExpectedImportState
case object MySqlExpectedNoImport extends MySqlExpectedImportState
final case class MySqlExpectedCommittedImport(
    operationUuid: UUID,
    manifestSha256: String,
    snapshotSha256: String,
    receiptSha256: String
) extends MySqlExpectedImportState:
  require(operationUuid != null, "approved import operation identity is required")
  require(manifestSha256 != null && snapshotSha256 != null && receiptSha256 != null, "approved import hashes are required")
  require(MySqlSchemaAuditor.Sha256Hex.matches(manifestSha256), "approved import manifest hash is malformed")
  require(MySqlSchemaAuditor.Sha256Hex.matches(snapshotSha256), "approved import snapshot hash is malformed")
  require(MySqlSchemaAuditor.Sha256Hex.matches(receiptSha256), "approved import receipt hash is malformed")

private[mysql] final case class MySqlReadyManifest(
    importState: String,
    importOperationUuid: Option[UUID],
    importManifestSha256: Option[String],
    importSnapshotSha256: Option[String],
    importReceiptSha256: Option[String]
)

private[mysql] final case class MySqlWriterObservation(
    bindingUuid: UUID,
    epoch: Long,
    ownerBootUuid: Option[UUID],
    leaseUntil: LocalDateTime
)

/** 此观测只表示解码完成，并不代表 schema READY、fence、世界来源或产品可用。 */
final class UnverifiedMySqlSchemaObservation private[mysql] (
    private[mysql] val scope: MySqlAuditBootScope,
    private[mysql] val before: MySqlCallerScope,
    private[mysql] val after: MySqlCallerScope,
    val resultSets: Vector[MySqlAuditResultSet],
    val auditStartedAtNanos: Long,
    val observedAtNanos: Long,
    val profile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format1
):
  require(scope != null && before != null && after != null, "observation boot scope and caller scopes are required")
  require(profile != null, "observation schema audit profile is required")
  require(resultSets != null && resultSets.forall(_ != null), "observation resultsets are required")
  require(resultSets.map(_.id) == MySqlAuditCoverage.resultSetIds(profile), "observation must contain exactly the ordered profile resultsets")
  val coverage: String = profile.coverage
  override def toString: String = s"UnverifiedMySqlSchemaObservation(${resultSets.size} resultsets, schemaFormat=${profile.schemaFormat})"

/** 结构比较结果仍不具有 READY 或 writer 权限。 */
final class ComparedMySqlObjectManifest private[mysql] (
    val observation: UnverifiedMySqlSchemaObservation,
    val actualCanonicalSha256: String
):
  require(observation != null, "compared MySQL observation is required")
  val profile: MySqlSchemaAuditProfile = observation.profile
  val coverage: String = profile.coverage
  override def toString: String = s"ComparedMySqlObjectManifest(${actualCanonicalSha256.take(12)}…, schemaFormat=${profile.schemaFormat})"

/** READY schema qualification；可记录尚未认领或 legacy binding，绝不代表 runtime-ready。 */
final class VerifiedSchemaQualification private[mysql] (
    val profile: MySqlSchemaAuditProfile,
    val expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
    private val verifiedConnection: VerifiedMySqlConnection,
    private val ownerThread: Thread,
    val bootUuid: UUID,
    val bootGeneration: Long,
    val connectionGeneration: Long,
    val serverUuid: UUID,
    val schema: String,
    val bindingUuid: UUID,
    val physicalBinding: MySqlObservedPhysicalBinding,
    val provisioningUuid: UUID,
    val ddlSha256: String,
    val objectManifestSha256: String,
    val importState: String,
    val importOperationUuid: Option[UUID],
    val importManifestSha256: Option[String],
    val importSnapshotSha256: Option[String],
    val importReceiptSha256: Option[String],
    val writerEpochObservedBeforeAcquire: Long,
    val writerOwnerBootObservedBeforeAcquire: Option[UUID],
    val observedAtNanos: Long,
    private val maxAgeNanos: Long
):
  require(profile != null && expectedPhysicalFormat != null, "schema qualification profile and physical target are required")
  val coverage: String = profile.coverage
  def isCurrent(
      boot: UUID,
      bootGen: Long,
      connectionGen: Long,
      actualServerUuid: UUID,
      actualSchema: String,
      expectedBindingUuid: UUID,
      expectedProfile: MySqlSchemaAuditProfile,
      actualPhysicalFormat: MySqlExpectedPhysicalFormat,
      actualConnection: VerifiedMySqlConnection,
      actualOwnerThread: Thread,
      nowNanos: Long
  ): Boolean =
    (actualConnection eq verifiedConnection) && (actualOwnerThread eq ownerThread) && boot == bootUuid && bootGen == bootGeneration && connectionGen == connectionGeneration &&
      actualServerUuid == serverUuid && actualSchema == schema && expectedBindingUuid == bindingUuid &&
      MySqlSchemaAuditProfile.matchesTarget(profile, expectedPhysicalFormat, expectedProfile, actualPhysicalFormat) &&
        fresh(observedAtNanos, maxAgeNanos, nowNanos)

  override def toString: String = s"VerifiedSchemaQualification(schema=$schema, schemaFormat=${profile.schemaFormat}, physicalFormat=${expectedPhysicalFormat.version}; runtime-ready=false)"

/** DB fence claim confirmed by its own successful acquisition/readback path. */
final class MySqlWriterFenceExpectation private[mysql] (
    val bindingUuid: UUID,
    val ownerBootUuid: UUID,
    val epoch: Long,
    val leaseUntil: LocalDateTime,
    val observedDbTime: LocalDateTime,
    val observedAtNanos: Long
):
  require(bindingUuid != null && ownerBootUuid != null && epoch > 0L, "a positive acquired writer fence is required")
  require(leaseUntil != null && observedDbTime != null, "acquired writer fence database timestamps are required")
  private[mysql] val leaseValidityNanos: Long = MySqlSchemaAuditor.boundedLeaseValidityNanos(observedDbTime, leaseUntil)
  private[mysql] def isCurrentAt(nowNanos: Long): Boolean = monotonicWindowValid(observedAtNanos, leaseValidityNanos, nowNanos)

object MySqlWriterFenceExpectation:
  private[mysql] def fromAcquiredFence(
      bindingUuid: UUID,
      ownerBootUuid: UUID,
      epoch: Long,
      leaseUntil: LocalDateTime,
      observedDbTime: LocalDateTime,
      observedAtNanos: Long
  ): MySqlWriterFenceExpectation =
    new MySqlWriterFenceExpectation(bindingUuid, ownerBootUuid, epoch, leaseUntil, observedDbTime, observedAtNanos)

/** 只证明同一 boot/connection 的 MySQL schema 与 DB fence；不证明 world custody。 */
final class RuntimeSchemaAuditReceipt private[mysql] (
    val profile: MySqlSchemaAuditProfile,
    val expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
    private val connectionToken: AnyRef,
    val bootUuid: UUID,
    val bootGeneration: Long,
    val connectionGeneration: Long,
    val serverUuid: UUID,
    val schema: String,
    val bindingUuid: UUID,
    val writerEpoch: Long,
    val provisioningUuid: UUID,
    val importState: String,
    val importOperationUuid: Option[UUID],
    val importManifestSha256: Option[String],
    val importSnapshotSha256: Option[String],
    val importReceiptSha256: Option[String],
    val ddlSha256: String,
    val objectManifestSha256: String,
    private val acquiredFence: MySqlWriterFenceExpectation,
    private val leaseObservedAtNanos: Long,
    private val leaseValidityNanos: Long,
    val observedAtNanos: Long,
    private val maxAgeNanos: Long
):
  require(profile != null && expectedPhysicalFormat != null, "runtime schema receipt profile and physical target are required")
  require(connectionToken != null && acquiredFence != null, "runtime schema receipt requires its connection and acquired fence tokens")
  require(acquiredFence.bindingUuid == bindingUuid && acquiredFence.ownerBootUuid == bootUuid && acquiredFence.epoch == writerEpoch,
    "runtime schema receipt writer fence identity is inconsistent")
  require(leaseValidityNanos > 0L, "runtime schema receipt writer lease window must be positive")
  val coverage: String = profile.coverage
  /** 只投影实际 receipt/fence 已保存的窗口，不接受 caller 自填 observed/validity/success。 */
  private[mysql] def ownedFreshness(renewSequence: Long): MySqlOwnedAdmissionFreshness =
    new MySqlOwnedAdmissionFreshness(bootUuid, bootGeneration, connectionGeneration, serverUuid, schema,
      bindingUuid, writerEpoch, profile, expectedPhysicalFormat, renewSequence,
      MySqlAdmissionWindow(acquiredFence.observedAtNanos, acquiredFence.leaseValidityNanos),
      MySqlAdmissionWindow(leaseObservedAtNanos, leaseValidityNanos),
      MySqlAdmissionWindow(observedAtNanos, maxAgeNanos))
  def assertCurrent(
      boot: UUID,
      bootGen: Long,
      connectionGen: Long,
      actualServerUuid: UUID,
      actualSchema: String,
      actualBindingUuid: UUID,
      actualProfile: MySqlSchemaAuditProfile,
      actualPhysicalFormat: MySqlExpectedPhysicalFormat,
      actualConnection: VerifiedMySqlConnection,
      currentFence: Option[MySqlWriterFenceExpectation],
      nowNanos: Long
  ): Unit =
    if actualConnection == null || actualConnection.connection == null then MySqlSchemaAuditor.reject("current MySQL connection token is unavailable")
    assertCurrentForToken(boot, bootGen, connectionGen, actualServerUuid, actualSchema, actualBindingUuid, actualProfile, actualPhysicalFormat,
      actualConnection.connection, currentFence, nowNanos)

  private[mysql] def assertCurrentForToken(
      boot: UUID,
      bootGen: Long,
      connectionGen: Long,
      actualServerUuid: UUID,
      actualSchema: String,
      actualBindingUuid: UUID,
      actualProfile: MySqlSchemaAuditProfile,
      actualPhysicalFormat: MySqlExpectedPhysicalFormat,
      actualConnectionToken: AnyRef,
      currentFence: Option[MySqlWriterFenceExpectation],
      nowNanos: Long
  ): Unit =
    val fence = currentFence match
      case Some(value) if value != null => value
      case _ => MySqlSchemaAuditor.reject("current MySQL writer fence token is missing or invalid")
    if boot != bootUuid || bootGen != bootGeneration || connectionGen != connectionGeneration ||
      actualServerUuid != serverUuid || actualSchema != schema || actualBindingUuid != bindingUuid ||
      !MySqlSchemaAuditProfile.matchesTarget(profile, expectedPhysicalFormat, actualProfile, actualPhysicalFormat) ||
      (actualConnectionToken ne connectionToken) ||
      (fence ne acquiredFence) || fence.bindingUuid != bindingUuid || fence.ownerBootUuid != bootUuid || fence.epoch != writerEpoch ||
      !fence.isCurrentAt(nowNanos) || !monotonicWindowValid(leaseObservedAtNanos, leaseValidityNanos, nowNanos) ||
      !fresh(observedAtNanos, maxAgeNanos, nowNanos) then
      MySqlSchemaAuditor.reject("MySQL schema audit receipt is stale or belongs to a different boot, connection, or schema")

  override def toString: String = s"RuntimeSchemaAuditReceipt(schema=$schema, schemaFormat=${profile.schemaFormat}, physicalFormat=${expectedPhysicalFormat.version}, writerEpoch=$writerEpoch)"

/** 只读的 MySQL metadata auditor；所有 SQL 入口都限定在建立 connection 的 owned IO thread。 */
final class MySqlSchemaAuditor(private val expectation: MySqlSchemaAuditExpectation, maxReceiptAge: Duration):
  require(expectation != null, "approved MySQL schema expectation is required")
  require(maxReceiptAge != null && !maxReceiptAge.isNegative && !maxReceiptAge.isZero, "schema receipt freshness must be positive")
  private val maxAgeNanos = maxReceiptAge.toNanos
  require(maxAgeNanos > 0L && maxAgeNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos, "schema receipt freshness exceeds the bounded policy")

  private def requirePublicAuditOwner():Unit =
    if expectation.profile == MySqlSchemaAuditProfile.DbFirst1 then
      reject("schema6 audit requires its registered owned allocation holder")

  /** 验证 caller default schema 后，只 CALL 固定 schema 下的 auditself，并读取全部 14 个 resultset。 */
  def readCurrentObjectManifest(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): UnverifiedMySqlSchemaObservation =
    requirePublicAuditOwner()
    readCurrentObjectManifestInternal(verified,scope,null)

  private[mysql] def beginSourceUnknownInspectionAudit(
      verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,
      originalRoot:MySqlFormat3ReadBudget,
      auditAccount:MySqlFormat3ReadBudget,
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5
  ):MySqlSchemaAuditor.SourceUnknownInspectionAudit =
    if verified==null || scope==null || originalRoot==null || auditAccount==null || owner==null ||
        expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 ||
        expectation.deploymentReceiptInputs.isEmpty || !expectation.auditLimitProfile.exists(_ eq originalRoot.profile) ||
        originalRoot.isClosed || auditAccount.isClosed || !auditAccount.isCanonicalComparisonChildOf(originalRoot) ||
        !auditAccount.sharesAllocationLedger(originalRoot) || (auditAccount.profile ne originalRoot.profile) then
      reject("schema5 UNKNOWN audit requires the exact original root/account and approved schema5 expectation")
    owner.requireAuditBinding(expectation,verified,scope,originalRoot,auditAccount)
    auditAccount.reserveRetainedMemory(1024L,
      "schema5 UNKNOWN audit partial-holder owner BEFORE allocation")
    new MySqlSchemaAuditor.SourceUnknownInspectionAudit(this,verified,scope,originalRoot,auditAccount,owner)

  private[mysql] def beginDbFirstAudit(inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      owner:MySqlDbFirstSession):MySqlSchemaAuditor.DbFirstAudit =
    require(owner != null && inputs != null && (expectation eq inputs.auditExpectation),"original schema6 audit expectation required")
    owner.requireDbFirstAuditSource(root,inputs,verified,scope)
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && expectation.dbFirstInputs.nonEmpty,
      "schema6 owned audit requires independent DbFirst1 security inputs")
    root.reserveRetainedMemory(1024L,"schema6 partial audit holder BEFORE allocation")
    try new MySqlSchemaAuditor.DbFirstAudit(this,inputs,root,verified,scope,owner)
    catch case cause:Throwable =>
      try root.releaseRetainedMemory(1024L,"unallocated schema6 partial audit holder")
      catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

  private[mysql] def beginDbFirstWriterAudit(inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,owner:MySqlDbFirstSession,
      coordinator:MySqlWriterFenceCoordinator):MySqlSchemaAuditor.DbFirstAudit =
    require(owner != null && inputs != null && (expectation eq inputs.auditExpectation),"original schema6 writer expectation required")
    owner.requireDbFirstWriterAuditSource(root,inputs,verified,scope,coordinator)
    root.reserveRetainedMemory(1024L,"schema6 writer audit holder BEFORE allocation")
    try new MySqlSchemaAuditor.DbFirstAudit(this,inputs,root,verified,scope,owner,coordinator)
    catch case cause:Throwable =>
      try root.releaseRetainedMemory(1024L,"unallocated schema6 writer audit holder")
      catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

  private[mysql] def readSourceUnknownInspectionManifest(
      holder:MySqlSchemaAuditor.SourceUnknownInspectionAudit,
      allocation:MySqlSchemaAuditor.SourceUnknownInspectionAllocation
  ):UnverifiedMySqlSchemaObservation =
    if holder==null || allocation==null || !holder.isBoundTo(holder.owner) then
      reject("registered schema5 UNKNOWN partial audit holder required")
    holder.owner.requireAuditBinding(expectation,holder.verified,holder.scope,holder.root,holder.auditAccount)
    readCurrentObjectManifestInternal(holder.verified,holder.scope,allocation)

  private def readCurrentObjectManifestInternal(
      verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,
      allocation:MySqlSchemaAuditor.AuditAllocation
  ):UnverifiedMySqlSchemaObservation =
    checkOwnedConnection(verified, scope)
    val before = if allocation==null then readCallerScopeBeforeCall(verified,scope)
      else readCallerScope(verified,scope,allocation,"before")
    if allocation!=null then allocation.retainCallerScope("before",before)
    validateCallerScope(before)
    val (resultSets, auditStartedAtNanos) =
      try readAuditCall(verified.connection,allocation)
      catch
        case primary: Throwable =>
          if expectation.profile.extendedAudit then
            try
              val failedAfter = if allocation==null then readCallerScopeAfterCall(verified,scope)
                else readCallerScope(verified,scope,allocation,"failed-after")
              if allocation!=null then allocation.retainCallerScope("failed-after",failedAfter)
              validateCallerScope(failedAfter)
              if before != failedAfter then reject("format3 caller identity/grants changed across failed CALL")
            catch case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)
          throw primary
    val after = if allocation==null then readCallerScopeAfterCall(verified,scope)
      else readCallerScope(verified,scope,allocation,"after")
    if allocation!=null then allocation.retainCallerScope("after",after)
    validateCallerScope(after)
    if before != after then reject("MySQL caller scope changed across the audit call")
    val header = resultSets.head
    val footer = resultSets(13)
    if allocation==null then validateHeaderFooterIdentity(header,footer,before,after,scope)
    else allocation.withTemporary(headerFooterFee(header,footer),"schema5 UNKNOWN header/footer identity validation") {
      validateHeaderFooterIdentity(header,footer,before,after,scope)
    }
    // V3 只改变批准权限的比较；原 raw grants/身份窗口必须先完全一致。
    if allocation==null then
      expectation.validateSecurity(before,after,resultSets.slice(14,18))
    else allocation.withTemporary(Math.addExact(512L,MySqlSchemaAuditor.vectorSliceFee(resultSets,14,18)),
      "schema5 UNKNOWN privilege subset view") {
      val privileges=resultSets.slice(14,18)
      allocation.retainScratch(privileges,"schema5 UNKNOWN privilege subset graph")
      expectation.validateSecurity(before,after,privileges,allocation.account)
    }
    if allocation==null then expectation.validateRetained(resultSets,before,after)
    else expectation.validateRetained(resultSets,before,after,allocation.account)
    if allocation==null then
      MySqlSchemaAuditor.validateSelfDescriptions(resultSets.slice(1,11),requireTriggerRows=false,profile=expectation.profile)
    else allocation.withTemporary(Math.addExact(1024L,MySqlSchemaAuditor.staticValidationScratch(resultSets,1,11,expectation.profile)),
      "schema5 UNKNOWN initial static self-description validation") {
      val staticSets=resultSets.slice(1,11)
      allocation.retainScratch(staticSets,"schema5 UNKNOWN static-set view")
      MySqlSchemaAuditor.validateSelfDescriptions(staticSets,requireTriggerRows=false,profile=expectation.profile)
    }
    if allocation!=null then allocation.retain(1024L,"schema5 UNKNOWN retained observation holder BEFORE allocation")
    val observation=new UnverifiedMySqlSchemaObservation(scope,before,after,resultSets,auditStartedAtNanos,
      System.nanoTime(),expectation.profile)
    if allocation!=null then allocation.retainObservation(observation)
    observation

  def readCallerScopeBeforeCall(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope): MySqlCallerScope =
    requirePublicAuditOwner()
    readCallerScope(verified, scope)

  def readCallerScopeAfterCall(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope): MySqlCallerScope =
    requirePublicAuditOwner()
    readCallerScope(verified, scope)

  /** 比较唯一 required auditself 与完整 fixed-schema static/trigger manifest，并使用所选 schema profile 的 hash domain。 */
  def compareRequiredObjectManifest(observation: UnverifiedMySqlSchemaObservation): ComparedMySqlObjectManifest =
    requirePublicAuditOwner()
    requireObservationScope(observation)
    expectation.validateSecurity(observation.before, observation.after, observation.resultSets.slice(14, 18))
    val actualSets = observation.resultSets.slice(1, 11)
    if actualSets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then
      reject("MySQL audit static-object resultset coverage is incomplete or reordered")
    actualSets.foreach(resultSet => MySqlSchemaAuditor.validateStaticSetShape(resultSet, expectation.profile))
    MySqlSchemaAuditor.validateSelfDescriptions(actualSets, requireTriggerRows = false, profile = expectation.profile)
    MySqlSchemaAuditor.validateProfileObjectCounts(expectation.profile, actualSets)
    MySqlSchemaAuditor.rejectDuplicateRows(actualSets)
    val auditSelf = actualSets(8)
    val auditSelfRow = auditSelf.rows.head
    if MySqlSchemaAuditor.textAt(auditSelf, auditSelfRow, "ROUTINE_SCHEMA") != expectation.schema ||
      MySqlSchemaAuditor.textAt(auditSelf, auditSelfRow, "DEFINER") != expectation.auditDefiner then
      reject("required MySQL auditself definition has an unexpected schema or definer")
    if expectation.objects.profile != expectation.profile then reject("approved MySQL object manifest belongs to a different schema profile")
    if actualSets != expectation.objects.resultSets then
      throw MySqlSchemaAuditor.staticManifestMismatch(expectation.profile, expectation.objects.resultSets, actualSets)
    val actualHash = MySqlSchemaAuditor.canonicalSha256(expectation.profile, actualSets)
    if actualHash != expectation.objects.canonicalSha256 then
      reject(s"MySQL static-object manifest hash differs from the independently approved format ${expectation.profile.schemaFormat} baseline")
    new ComparedMySqlObjectManifest(observation, actualHash)

  /** UNKNOWN-only comparison. Shape-dependent views, sort graphs and binary hash copies are prepaid. */
  private def compareRequiredObjectManifestOwned(
      observation:UnverifiedMySqlSchemaObservation,
      allocation:MySqlSchemaAuditor.AuditAllocation
  ):ComparedMySqlObjectManifest =
    if allocation==null then reject("schema5 UNKNOWN comparison owner is required")
    requireObservationScope(observation)
    val sets=observation.resultSets
    if sets==null || sets.size!=18 then reject("schema5 UNKNOWN comparison requires the exact eighteen-set observation")
    val fee=Math.addExact(2048L,Math.addExact(
      MySqlSchemaAuditor.staticValidationScratch(sets,1,11,expectation.profile),
      MySqlSchemaAuditor.staticValidationScratch(expectation.objects.resultSets,0,expectation.objects.resultSets.size,expectation.profile)))
    allocation.withTemporary(fee,"schema5 UNKNOWN full static comparison/hash workspace") {
      val privilegeSets=sets.slice(14,18)
      allocation.retainScratch(privilegeSets,"schema5 UNKNOWN qualification privilege view")
      expectation.validateSecurity(observation.before,observation.after,privilegeSets,allocation.account)
      val actualSets=sets.slice(1,11)
      allocation.retainScratch(actualSets,"schema5 UNKNOWN actual static manifest view")
      val actualIds=actualSets.map(_.id)
      allocation.retainScratch(actualIds,"schema5 UNKNOWN static resultset identity vector")
      if actualIds!=MySqlAuditCoverage.StaticResultSetIds then
        reject("MySQL audit static-object resultset coverage is incomplete or reordered")
      actualSets.foreach(resultSet=>MySqlSchemaAuditor.validateStaticSetShape(resultSet,expectation.profile))
      MySqlSchemaAuditor.validateSelfDescriptions(actualSets,requireTriggerRows=false,profile=expectation.profile)
      MySqlSchemaAuditor.validateProfileObjectCounts(expectation.profile,actualSets)
      MySqlSchemaAuditor.rejectDuplicateRows(actualSets)
      val auditSelf=actualSets(8)
      val auditSelfRow=auditSelf.rows.head
      if MySqlSchemaAuditor.textAt(auditSelf,auditSelfRow,"ROUTINE_SCHEMA")!=expectation.schema ||
          MySqlSchemaAuditor.textAt(auditSelf,auditSelfRow,"DEFINER")!=expectation.auditDefiner then
        reject("required MySQL auditself definition has an unexpected schema or definer")
      if expectation.objects.profile!=expectation.profile then
        reject("approved MySQL object manifest belongs to a different schema profile")
      if actualSets!=expectation.objects.resultSets then
        throw MySqlSchemaAuditor.staticManifestMismatch(expectation.profile, expectation.objects.resultSets, actualSets)
      val actualHash=allocation.withTemporary(
        MySqlSchemaAuditor.canonicalHashScratch(actualSets),
        "schema5 UNKNOWN canonical object hash text and binary copies") {
        MySqlSchemaAuditor.canonicalSha256(expectation.profile,actualSets)
      }
      if actualHash!=expectation.objects.canonicalSha256 then
        reject(s"MySQL static-object manifest hash differs from the independently approved format ${expectation.profile.schemaFormat} baseline")
      allocation.retain(256L,"schema5 UNKNOWN compared-manifest holder BEFORE allocation")
      new ComparedMySqlObjectManifest(observation,actualHash)
    }

  /** 在 writer acquire 前验证 READY 与 schema qualification；不认领物理 binding，也不生成 runtime receipt。 */
  def auditBeforeWriterAcquire(
      observation: UnverifiedMySqlSchemaObservation,
      currentScope: MySqlAuditBootScope
  ): VerifiedSchemaQualification =
    requirePublicAuditOwner()
    auditBeforeWriterAcquireInternal(observation,currentScope,null)

  private[mysql] def auditBeforeWriterAcquireOwned(
      observation:UnverifiedMySqlSchemaObservation,
      currentScope:MySqlAuditBootScope,
      allocation:MySqlSchemaAuditor.SourceUnknownInspectionAllocation
  ):VerifiedSchemaQualification =
    if allocation==null then reject("schema5 UNKNOWN owned-audit allocation account is required")
    auditBeforeWriterAcquireInternal(observation,currentScope,allocation)

  private def auditBeforeWriterAcquireInternal(
      observation:UnverifiedMySqlSchemaObservation,
      currentScope:MySqlAuditBootScope,
      allocation:MySqlSchemaAuditor.AuditAllocation
  ):VerifiedSchemaQualification =
    checkObservationCurrent(observation, currentScope)
    validateObservationEnvelope(observation)
    val compared = if allocation==null then compareRequiredObjectManifest(observation)
      else compareRequiredObjectManifestOwned(observation,allocation)
    val headerSet = observation.resultSets.head
    val header = oneRow(headerSet, "header")
    val manifest = if allocation==null then validateReadyManifest(headerSet,header,compared.actualCanonicalSha256)
      else allocation.withTemporary(MySqlSchemaAuditor.headerQualificationScratch(headerSet,header),
        "schema5 UNKNOWN READY manifest parsing and binding/writer qualification") {
        validateReadyManifest(headerSet,header,compared.actualCanonicalSha256,allocation.account)
      }
    val bindingSet = observation.resultSets(11)
    val binding = if allocation==null then validatePreAcquirePhysicalBinding(bindingSet)
      else allocation.withTemporary(MySqlSchemaAuditor.resultSetValidationScratch(bindingSet),
        "schema5 UNKNOWN physical binding qualification") { validatePreAcquirePhysicalBinding(bindingSet) }
    val writerSet = observation.resultSets(12)
    val writer = if allocation==null then validatePreAcquireWriterObservation(writerSet)
      else allocation.withTemporary(MySqlSchemaAuditor.resultSetValidationScratch(writerSet),
        "schema5 UNKNOWN writer and lease qualification") { validatePreAcquireWriterObservation(writerSet) }
    if allocation!=null then allocation.retain(1536L,"schema5 UNKNOWN qualification holder BEFORE allocation")
    new VerifiedSchemaQualification(
      expectation.profile,
      expectation.expectedPhysicalFormat,
      currentScope.verifiedConnection.getOrElse(reject("verified MySQL connection is unavailable")),
      currentScope.ownerThread,
      currentScope.bootUuid,
      currentScope.bootGeneration,
      currentScope.connectionGeneration,
      expectation.serverUuid,
      expectation.schema,
      expectation.bindingUuid,
      binding,
      expectation.provisioningUuid,
      expectation.ddlSha256,
      compared.actualCanonicalSha256,
      manifest.importState,
      manifest.importOperationUuid,
      manifest.importManifestSha256,
      manifest.importSnapshotSha256,
      manifest.importReceiptSha256,
      writer.epoch,
      writer.ownerBootUuid,
      observation.observedAtNanos,
      maxAgeNanos
    )

  /** 使用 writer acquire 后的新 CALL，确认 exact CERTIFIED binding 与同一 boot/epoch fence。 */
  def auditAfterWriterAcquire(
      observation: UnverifiedMySqlSchemaObservation,
      qualification: VerifiedSchemaQualification,
      currentScope: MySqlAuditBootScope,
      acquiredFence: MySqlWriterFenceExpectation
  ): RuntimeSchemaAuditReceipt =
    requirePublicAuditOwner()
    auditAfterWriterAcquireInternal(observation,qualification,currentScope,acquiredFence,null)

  private def auditAfterWriterAcquireInternal(observation:UnverifiedMySqlSchemaObservation,
      qualification:VerifiedSchemaQualification,currentScope:MySqlAuditBootScope,
      acquiredFence:MySqlWriterFenceExpectation,allocation:MySqlSchemaAuditor.AuditAllocation):RuntimeSchemaAuditReceipt =
    def construct():RuntimeSchemaAuditReceipt =
      if qualification == null || acquiredFence == null || currentScope == null then reject("verified schema qualification, current scope, and acquired fence are required")
      if !qualification.isCurrent(
        currentScope.bootUuid,
        currentScope.bootGeneration,
        currentScope.connectionGeneration,
        expectation.serverUuid,
        expectation.schema,
        expectation.bindingUuid,
        expectation.profile,
        expectation.expectedPhysicalFormat,
        currentScope.verifiedConnection.getOrElse(reject("verified MySQL connection is unavailable")),
        currentScope.ownerThread,
        System.nanoTime()
      ) then reject("pre-acquire MySQL schema qualification is stale")
      if acquiredFence.bindingUuid != expectation.bindingUuid || acquiredFence.ownerBootUuid != currentScope.bootUuid then
        reject("acquired MySQL writer fence does not belong to this boot and binding")
      checkObservationCurrent(observation, currentScope)
      validateObservationEnvelope(observation)
      val compared = if allocation == null then compareRequiredObjectManifest(observation)
        else compareRequiredObjectManifestOwned(observation,allocation)
      if compared.actualCanonicalSha256 != qualification.objectManifestSha256 then
        reject("MySQL object manifest changed across writer acquisition")
      val headerSet = observation.resultSets.head
      val header = oneRow(headerSet, "header")
      val manifest = validateReadyManifest(headerSet, header, compared.actualCanonicalSha256,
        if allocation == null then null else allocation.account)
      if qualification.provisioningUuid != expectation.provisioningUuid || qualification.ddlSha256 != expectation.ddlSha256 ||
        qualification.importState != manifest.importState || qualification.importOperationUuid != manifest.importOperationUuid ||
        qualification.importManifestSha256 != manifest.importManifestSha256 || qualification.importSnapshotSha256 != manifest.importSnapshotSha256 ||
        qualification.importReceiptSha256 != manifest.importReceiptSha256 then
        reject("MySQL schema manifest provenance changed across writer acquisition")
      val writer = validateAcquiredWriterFence(
        observation.resultSets(11),
        observation.resultSets(12),
        observation.resultSets(13),
        acquiredFence
      )
      val footerTime = dateTimeCell(observation.resultSets(13), oneRow(observation.resultSets(13), "footer"), "observed_db_time")
      val receiptLeaseValidityNanos = MySqlSchemaAuditor.boundedLeaseValidityNanos(footerTime, writer.leaseUntil, maxAgeNanos)
      val afterDecodeNanos = System.nanoTime()
      if !acquiredFence.isCurrentAt(afterDecodeNanos) ||
        !monotonicWindowValid(observation.auditStartedAtNanos, receiptLeaseValidityNanos, afterDecodeNanos) then
        reject("MySQL writer fence lease expired before schema receipt construction")
      new RuntimeSchemaAuditReceipt(
        expectation.profile,
        expectation.expectedPhysicalFormat,
        currentScope.connectionToken,
        currentScope.bootUuid,
        currentScope.bootGeneration,
        currentScope.connectionGeneration,
        expectation.serverUuid,
        expectation.schema,
        expectation.bindingUuid,
        writer.epoch,
        expectation.provisioningUuid,
        manifest.importState,
        manifest.importOperationUuid,
        manifest.importManifestSha256,
        manifest.importSnapshotSha256,
        manifest.importReceiptSha256,
        expectation.ddlSha256,
        compared.actualCanonicalSha256,
        acquiredFence,
        observation.auditStartedAtNanos,
        receiptLeaseValidityNanos,
        observation.observedAtNanos,
        maxAgeNanos
      )
    if allocation == null then construct()
    else
      val workspace=observation.resultSets.foldLeft(65536L)((total,set) =>
        Math.addExact(total,MySqlSchemaAuditor.resultSetValidationScratch(set)))
      allocation.withTemporary(workspace,"schema6 post-acquire manifest/writer receipt validation") { construct() }

  private def checkObservationCurrent(observation: UnverifiedMySqlSchemaObservation, currentScope: MySqlAuditBootScope): Unit =
    if observation == null then reject("MySQL schema observation is required")
    if observation.profile != expectation.profile || observation.coverage != expectation.profile.coverage then
      reject("MySQL schema observation belongs to a different or unknown schema profile")
    if observation.scope ne currentScope then reject("MySQL schema observation belongs to a different immutable boot scope")
    val verified = currentScope.verifiedConnection.getOrElse(reject("verified MySQL connection is unavailable"))
    checkOwnedConnection(verified, currentScope)
    if !fresh(observation.observedAtNanos, maxAgeNanos, System.nanoTime()) then
      reject("MySQL schema observation exceeded its monotonic freshness window")

  private[mysql] def validateObservationEnvelope(observation: UnverifiedMySqlSchemaObservation): Unit =
    if observation == null then reject("MySQL schema observation is required")
    if observation.profile != expectation.profile then reject("MySQL schema observation belongs to a different schema profile")
    validateCallerScope(observation.before)
    validateCallerScope(observation.after)
    if observation.before != observation.after then reject("MySQL caller scope changed across the audit call")
    validateHeaderFooterIdentity(
      observation.resultSets.head,
      observation.resultSets(13),
      observation.before,
      observation.after,
      observation.scope
    )

  private def requireObservationScope(observation: UnverifiedMySqlSchemaObservation): Unit =
    if observation == null then reject("MySQL schema observation is required")
    if observation.profile != expectation.profile || observation.coverage != expectation.profile.coverage then
      reject("MySQL schema observation belongs to a different or unknown schema profile")
    if observation.scope == null then reject("MySQL schema observation is missing its immutable boot scope")
    checkApprovedTargetIdentity(observation.scope.connectionIdentity)

  private def checkOwnedConnection(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope): Unit =
    checkVerifiedTarget(verified, scope)
    if verified.connection == null then reject("verified MySQL connection has no JDBC session")
    if Thread.currentThread() ne scope.ownerThread then reject("MySQL audit SQL must run on its owned IO thread")

  private def checkVerifiedTarget(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope): Unit =
    if verified == null || scope == null || !scope.verifiedConnection.contains(verified) ||
      (verified.connection ne scope.connectionToken) || (verified.identity ne scope.connectionIdentity) then
      reject("MySQL audit requires the exact verified connection bound to this boot scope")
    checkApprovedTargetIdentity(verified.identity)

  private def checkApprovedTargetIdentity(identity: MySqlConnectionIdentity): Unit =
    if identity == null then reject("MySQL audit boot scope is missing verified target identity")
    if identity.serverUuid != expectation.serverUuid || identity.serverVersion != expectation.serverVersion ||
      identity.catalog != expectation.schema || identity.effectiveUser != expectation.effectiveUser then
      reject("verified MySQL connection identity differs from the approved audit target")

  private def readCallerScope(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope): MySqlCallerScope =
    checkOwnedConnection(verified, scope)
    val connection = verified.connection
    withResource(connection.createStatement()) { statement =>
      withResource(statement.executeQuery(if expectation.profile.extendedAudit then MySqlSchemaAuditor.ReadCallerScopeFormat3 else MySqlSchemaAuditor.ReadCallerScope)) { result =>
        val columns = readColumns(result.getMetaData)
        MySqlSchemaAuditor.requireLabels(columns, if expectation.profile.extendedAudit then MySqlSchemaAuditor.CallerLabelsFormat3 else MySqlSchemaAuditor.CallerLabels, "caller scope")
        val identity = MySqlSchemaAuditor.decodeCallerIdentity(result, expectation.profile)
        if expectation.profile.extendedAudit then
          val grants = withResource(connection.createStatement()) { grantStatement =>
            withResource(grantStatement.executeQuery("SHOW GRANTS")) { rs =>
              if rs.getMetaData.getColumnCount != 1 then reject("SHOW GRANTS must have one full text column")
              val rows = Vector.newBuilder[String]
              var count = 0
              var retainedBytes = 0L
              while rs.next() do
                count += 1
                if count > MySqlSchemaAuditor.MaxRowsPerResultSet then reject("SHOW GRANTS exceeds the row bound")
                val value = requiredText(rs.getString(1), "caller grant")
                retainedBytes += MySqlSchemaAuditor.boundedUtf8ByteLength(value, MySqlSchemaAuditor.MaxCellBytes, "caller grant").toLong + 16L
                if retainedBytes > expectation.auditLimitProfile.get.maxRetainedBytes() || retainedBytes > MySqlSchemaAuditor.MaxTotalBytes then
                  reject("caller grants exceed the explicit retained-byte bound")
                rows += value
              val captured = rows.result()
              expectation.validateCapturedCallerGrants(captured)
              captured
            }
          }
          identity.copy(ownGrants = grants)
        else identity
      }
    }

  private def readCallerScope(
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      allocation:MySqlSchemaAuditor.AuditAllocation,stage:String
  ):MySqlCallerScope =
    if allocation==null || stage==null then reject("schema5 UNKNOWN caller-scope allocation and stage are required")
    checkOwnedConnection(verified,scope)
    val connection=verified.connection
    allocation.retain(512L,"schema5 UNKNOWN caller-scope JDBC statement and result owner BEFORE allocation")
    withResource(connection.createStatement()) { statement =>
      allocation.retain(256L,"schema5 UNKNOWN caller-scope resultset owner BEFORE acquisition")
      withResource(statement.executeQuery(if expectation.profile.extendedAudit then
        MySqlSchemaAuditor.ReadCallerScopeFormat3 else MySqlSchemaAuditor.ReadCallerScope)) { result =>
        allocation.retain(256L,"schema5 UNKNOWN caller-scope metadata owner BEFORE acquisition")
        val columns=readColumns(result.getMetaData,allocation)
        MySqlSchemaAuditor.requireLabels(columns,
          if expectation.profile.extendedAudit then MySqlSchemaAuditor.CallerLabelsFormat3 else MySqlSchemaAuditor.CallerLabels,
          "caller scope")
        val identity=MySqlSchemaAuditor.decodeCallerIdentityOwned(result,expectation.profile,allocation)
        val captured=if expectation.profile.extendedAudit then
          allocation.retain(512L,"schema5 UNKNOWN SHOW GRANTS statement and result owner BEFORE allocation")
          withResource(connection.createStatement()) { grantStatement =>
            allocation.retain(256L,"schema5 UNKNOWN SHOW GRANTS resultset owner BEFORE acquisition")
            withResource(grantStatement.executeQuery("SHOW GRANTS")) { rs =>
              allocation.retain(256L,"schema5 UNKNOWN SHOW GRANTS metadata owner BEFORE acquisition")
              val grantColumns=rs.getMetaData.getColumnCount
              if grantColumns!=1 then reject("SHOW GRANTS must have one full text column")
              allocation.retain(384L,"schema5 UNKNOWN SHOW GRANTS builder BEFORE allocation")
              val rows=Vector.newBuilder[String]
              allocation.retainScratch(rows,"schema5 UNKNOWN partial SHOW GRANTS builder")
              var count=0
              var retainedBytes=0L
              while rs.next() do
                count=Math.addExact(count,1)
                if count>MySqlSchemaAuditor.MaxRowsPerResultSet then reject("SHOW GRANTS exceeds the row bound")
                val text=MySqlSchemaAuditor.readOwnedTextColumn(rs,1,"caller grant",allocation)
                val bytes=MySqlSchemaAuditor.boundedUtf8ByteLength(text,
                  MySqlSchemaAuditor.MaxCellBytes,"caller grant")
                retainedBytes=Math.addExact(retainedBytes,bytes.toLong+16L)
                if retainedBytes>expectation.auditLimitProfile.get.maxRetainedBytes() ||
                    retainedBytes>MySqlSchemaAuditor.MaxTotalBytes then
                  reject("caller grants exceed the explicit retained-byte bound")
                allocation.retain(Math.addExact(96L,Math.multiplyExact(32L,bytes.toLong)),
                  "schema5 UNKNOWN retained caller grant BEFORE vector insertion")
                rows+=text
              allocation.retain(Math.multiplyExact(96L,count.toLong),
                "schema5 UNKNOWN captured grant vector BEFORE allocation")
              val grants=rows.result()
              allocation.retainScratch(grants,"schema5 UNKNOWN captured SHOW GRANTS")
              expectation.validateCapturedCallerGrants(grants)
              grants
            }
          }
        else Vector.empty
        val actual=if expectation.profile.extendedAudit then
          allocation.retain(256L,"schema5 UNKNOWN caller scope with captured grants BEFORE allocation")
          identity.copy(ownGrants=captured)
        else identity
        actual
      }
    }

  private def validateCallerScope(scope: MySqlCallerScope): Unit =
    if scope.serverUuid != expectation.serverUuid || scope.defaultSchema != expectation.schema ||
      scope.effectivePrincipal != expectation.effectiveUser || scope.effectiveRoles != expectation.callerRoles then
      reject("MySQL caller scope does not match the approved server, schema, principal, or roles")
    if scope.connectionId <= 0L then reject("MySQL caller connection ID is invalid")

  private def headerFooterFee(header:MySqlAuditResultSet,footer:MySqlAuditResultSet):Long =
    Math.addExact(MySqlSchemaAuditor.resultSetValidationScratch(header),
      MySqlSchemaAuditor.resultSetValidationScratch(footer))

  private def readAuditCall(connection: Connection,
      allocation:MySqlSchemaAuditor.AuditAllocation=null): (Vector[MySqlAuditResultSet], Long) =
    val qualifiedSchema = expectation.schema
    if !MySqlSchemaAuditor.SchemaIdentifier.matches(qualifiedSchema) then reject("fixed MySQL audit schema identifier is malformed")
    if allocation!=null then
      allocation.retain(Math.addExact(256L,Math.multiplyExact(32L,
        qualifiedSchema.length.toLong+expectation.profile.requiredProcedure.length.toLong)),
        "schema5 UNKNOWN fixed CALL text BEFORE allocation")
    val sql = s"CALL `$qualifiedSchema`.`${expectation.profile.requiredProcedure}`()"
    if allocation!=null then allocation.retain(512L,"schema5 UNKNOWN CALL JDBC statement owner BEFORE allocation")
    withResource(connection.createStatement()) { statement =>
      var expectedIds:Vector[String]=null
      var expectedLabels:Vector[Vector[String]]=null
      if allocation!=null then
        val fee=MySqlSchemaAuditor.auditShapeScratch(expectation.profile)
        allocation.retain(fee,"schema5 UNKNOWN fixed profile IDs/labels BEFORE allocation")
        expectedIds=MySqlAuditCoverage.resultSetIds(expectation.profile)
        expectedLabels=MySqlSchemaAuditor.labelsFor(expectation.profile)
        allocation.retainScratch(expectedIds,"schema5 UNKNOWN exact profile resultset IDs")
        allocation.retainScratch(expectedLabels,"schema5 UNKNOWN exact profile labels")
      if allocation!=null then allocation.retain(512L,"schema5 UNKNOWN resultset and protocol builders BEFORE allocation")
      val resultSets = Vector.newBuilder[MySqlAuditResultSet]
      if allocation!=null then allocation.retainScratch(resultSets,"schema5 UNKNOWN partial resultset builder")
      val budget = new MySqlSchemaAuditor.ReadBudget(expectation.auditLimitProfile,allocation)
      if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN CALL protocol builder BEFORE allocation")
      val protocol = Vector.newBuilder[Int]
      var protocolCount = 0L
      if allocation!=null then allocation.retainScratch(protocol,"schema5 UNKNOWN partial CALL protocol builder")
      val auditStartedAtNanos = System.nanoTime()
      var hasResult = statement.execute(sql)
      var resultCount = 0
      var updateCountSeen = false
      var terminal = false
      while !terminal do
        if hasResult then
          resultCount += 1
          if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN protocol ordinal BEFORE insertion")
          protocolCount = Math.incrementExact(protocolCount)
          protocol += resultCount
          if resultCount > (if allocation==null then MySqlAuditCoverage.resultSetIds(expectation.profile).size else expectedIds.size) then
            reject("MySQL audit CALL returned extra resultsets")
          if allocation!=null then
            allocation.retain(256L,"schema5 UNKNOWN JDBC resultset metadata owner BEFORE acquisition")
          val result = statement.getResultSet
          if result == null then reject("MySQL audit CALL reported a missing resultset")
          if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN resultset reference BEFORE insertion")
          resultSets += withResource(result)(rs => decodeResultSet(rs, resultCount, budget,allocation,expectedIds,expectedLabels))
          hasResult = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)
        else
          val updateCount = statement.getUpdateCount
          if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN protocol update-count BEFORE insertion")
          protocolCount = Math.incrementExact(protocolCount)
          protocol += updateCount
          if updateCount == -1 then terminal = true
          else if updateCount == 0 && !updateCountSeen then
            updateCountSeen = true
            hasResult = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)
            if hasResult || statement.getUpdateCount != -1 then
              reject("MySQL audit CALL returned an unexpected update-count/result protocol")
            if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN terminal protocol marker BEFORE insertion")
            protocolCount = Math.incrementExact(protocolCount)
            protocol += -1
            terminal = true
          else reject("MySQL audit CALL returned a non-empty or repeated update count")
      if expectation.profile.extendedAudit then
        if allocation!=null then allocation.retain(Math.multiplyExact(96L,protocolCount),
          "schema5 UNKNOWN completed CALL protocol vector BEFORE allocation")
        val protocolResult=protocol.result()
        if allocation!=null then
          allocation.retainScratch(protocolResult,"schema5 UNKNOWN completed CALL protocol")
          allocation.withTemporary(4096L,"schema5 UNKNOWN fixed protocol expectation vector") {
            MySqlSchemaAuditor.validateFormat3Protocol(protocolResult)
          }
        else MySqlSchemaAuditor.validateFormat3Protocol(protocolResult)
      if expectation.profile.extendedAudit && !updateCountSeen then
        reject("format3 audit CALL requires one actual zero update count followed by terminal -1")
      if allocation!=null then allocation.retain(Math.multiplyExact(96L,resultCount.toLong),
        "schema5 UNKNOWN complete resultset vector BEFORE allocation")
      val captured = resultSets.result()
      if allocation!=null then allocation.retainScratch(captured,"schema5 UNKNOWN complete audit resultsets")
      if captured.size != (if allocation==null then MySqlAuditCoverage.resultSetIds(expectation.profile).size else expectedIds.size) then
        reject("MySQL audit CALL did not return the exact profile resultset count")
      if allocation==null then
        if captured.map(_.id) != MySqlAuditCoverage.resultSetIds(expectation.profile) then
          reject("MySQL audit CALL resultsets are missing, reordered, or duplicated")
      else
        var index=0
        while index<captured.size do
          if captured(index).id!=expectedIds(index) then
            reject("MySQL audit CALL resultsets are missing, reordered, or duplicated")
          index+=1
      (captured, auditStartedAtNanos)
    }

  private def decodeResultSet(result: ResultSet, resultNumber: Int, budget: MySqlSchemaAuditor.ReadBudget,
      allocation:MySqlSchemaAuditor.AuditAllocation=null,
      expectedIds:Vector[String]=null,expectedLabels:Vector[Vector[String]]=null): MySqlAuditResultSet =
    if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN resultset metadata owner BEFORE acquisition")
    val metadata = result.getMetaData
    val columns = readColumns(metadata,allocation)
    if allocation!=null then allocation.retain(192L,"schema5 UNKNOWN resultset label formatting BEFORE allocation")
    val requiredLabels=if allocation==null then MySqlSchemaAuditor.labelsFor(expectation.profile)(resultNumber-1)
      else expectedLabels(resultNumber-1)
    MySqlSchemaAuditor.requireLabels(columns,requiredLabels,String.format(Locale.ROOT,"RS%02d",Int.box(resultNumber)))
    if columns.size > MySqlSchemaAuditor.MaxColumnsPerResultSet then reject("MySQL audit resultset exceeds the column bound")
    if expectation.profile.extendedAudit then
      columns.foreach { column =>
        val labelBytes=if allocation==null then MySqlSchemaAuditor.encodeUtf8(column.label).length
          else MySqlSchemaAuditor.boundedUtf8ByteLength(column.label,MySqlSchemaAuditor.MaxCellBytes,"column label")
        val typeBytes=if allocation==null then MySqlSchemaAuditor.encodeUtf8(column.typeName).length
          else MySqlSchemaAuditor.boundedUtf8ByteLength(column.typeName,MySqlSchemaAuditor.MaxCellBytes,"column type")
        budget.addBytes(Math.addExact(32,Math.addExact(labelBytes,typeBytes)), "column framing")
      }
    if allocation!=null then allocation.retain(512L,"schema5 UNKNOWN row and cell builders BEFORE allocation")
    val rows = Vector.newBuilder[MySqlAuditRow]
    if allocation!=null then allocation.retainScratch(rows,"schema5 UNKNOWN partial resultset row builder")
    var rowCount = 0
    while result.next() do
      rowCount += 1
      budget.addRow(rowCount)
      if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN row cell builder BEFORE allocation")
      val cells = Vector.newBuilder[MySqlAuditCell]
      if allocation!=null then allocation.retainScratch(cells,"schema5 UNKNOWN partial resultset cell builder")
      var index = 1
      while index <= columns.size do
        budget.addCell()
        cells += readCell(
          result,
          if allocation==null then MySqlAuditCoverage.resultSetIds(expectation.profile)(resultNumber-1)
            else expectedIds(resultNumber-1),
          rowCount,
          index,
          columns(index - 1),
          budget,
          allocation
        )
        index += 1
      if allocation!=null then allocation.retain(Math.multiplyExact(96L,columns.size.toLong),
        "schema5 UNKNOWN row-cell vector BEFORE allocation")
      val rowCells=cells.result()
      if allocation!=null then allocation.retainScratch(rowCells,"schema5 UNKNOWN completed row cells")
      if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN row holder BEFORE allocation")
      rows += MySqlAuditRow(rowCells)
    if allocation!=null then allocation.retain(Math.multiplyExact(96L,rowCount.toLong),
      "schema5 UNKNOWN resultset row vector BEFORE allocation")
    val completedRows=rows.result()
    if allocation!=null then allocation.retainScratch(completedRows,"schema5 UNKNOWN completed resultset rows")
    if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN resultset holder BEFORE allocation")
    MySqlAuditResultSet(if allocation==null then MySqlAuditCoverage.resultSetIds(expectation.profile)(resultNumber-1)
      else expectedIds(resultNumber-1),columns,completedRows)

  private def readColumns(metadata: ResultSetMetaData): Vector[MySqlAuditColumn] =
    readColumns(metadata,null)

  private def readColumns(metadata: ResultSetMetaData,
      allocation:MySqlSchemaAuditor.AuditAllocation):Vector[MySqlAuditColumn] =
    if metadata == null then reject("MySQL audit resultset has no metadata")
    val count = metadata.getColumnCount
    if count <= 0 || count > MySqlSchemaAuditor.MaxColumnsPerResultSet then reject("MySQL audit resultset has an invalid column count")
    if allocation!=null then allocation.retain(Math.addExact(512L,Math.multiplyExact(192L,count.toLong)),
      "schema5 UNKNOWN JDBC column metadata graph BEFORE allocation")
    val columns = Vector.tabulate(count) { offset =>
      val index = offset + 1
      val label = requiredText(metadata.getColumnLabel(index), "audit column label")
      val typeName = requiredText(metadata.getColumnTypeName(index), s"audit JDBC type for $label")
      if allocation!=null then
        allocation.retain(Math.addExact(96L,Math.multiplyExact(32L,
          MySqlSchemaAuditor.boundedUtf8ByteLength(label,MySqlSchemaAuditor.MaxCellBytes,"audit column label").toLong +
            MySqlSchemaAuditor.boundedUtf8ByteLength(typeName,MySqlSchemaAuditor.MaxCellBytes,"audit JDBC type").toLong)),
          "schema5 UNKNOWN actual column description BEFORE holder allocation")
      MySqlAuditColumn(
        label,
        metadata.getColumnType(index),
        typeName,
        metadata.getPrecision(index),
        metadata.getScale(index),
        metadata.isNullable(index),
        metadata.isSigned(index)
      )
    }
    if allocation==null then
      if columns.map(_.label).distinct.size != columns.size then reject("MySQL audit resultset has duplicate column labels")
    else allocation.withTemporary(Math.addExact(256L,Math.multiplyExact(96L,count.toLong)),
      "schema5 UNKNOWN duplicate column-label set and mapped view") {
      val labels=columns.map(_.label)
      allocation.retainScratch(labels,"schema5 UNKNOWN column label view")
      if labels.distinct.size!=columns.size then reject("MySQL audit resultset has duplicate column labels")
    }
    columns

  private def readCell(
      result: ResultSet,
      resultSetId: String,
      rowNumber: Int,
      index: Int,
      column: MySqlAuditColumn,
      budget: MySqlSchemaAuditor.ReadBudget,
      allocation:MySqlSchemaAuditor.AuditAllocation=null
  ): MySqlAuditCell =
    if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN JDBC cell context BEFORE allocation")
    val context = s"$resultSetId row $rowNumber column $index '${column.label}' JDBC ${column.jdbcType}/${column.typeName}"
    MySqlSchemaAuditor.jdbcValueKind(column.jdbcType, context) match
      case MySqlAuditJdbcValueKind.Timestamp =>
        if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN typed timestamp cell BEFORE JDBC allocation")
        val value = MySqlSchemaAuditor.jdbcRead(context, "getObject")(result.getObject(index))
        val wasNull = MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull())
        val decoded = MySqlSchemaAuditor.decodeTimestamp(column.jdbcType, value, wasNull)
        if expectation.profile.extendedAudit then
          decoded match
            case MySqlAuditTimestamp(value) =>
              if allocation==null then budget.addBytes(MySqlSchemaAuditor.encodeUtf8(value.toString).length,context)
              else budget.addBytes(32,context)
            case _ => ()
        decoded
      case MySqlAuditJdbcValueKind.Binary =>
        if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN binary stream owner BEFORE acquisition")
        val stream = MySqlSchemaAuditor.jdbcRead(context, "getBinaryStream")(result.getBinaryStream(index))
        if stream == null then
          if !MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull()) then reject(s"MySQL audit $context returned no binary stream for a non-NULL value")
          MySqlAuditNull
        else
          MySqlSchemaAuditor.jdbcRead(context, "binary stream decode") {
            val bytes = withResource(stream)(reader=>if allocation==null then readBoundedBytes(reader)
              else MySqlSchemaAuditor.readBoundedBytesOwned(reader,allocation,context))
            budget.addBytes(bytes.length, context)
            MySqlAuditBinary(bytes)
          }
      case MySqlAuditJdbcValueKind.Numeric =>
        if allocation!=null then
          return MySqlSchemaAuditor.readOwnedNumericColumn(result,index,context,allocation,budget)
        val value = MySqlSchemaAuditor.jdbcRead(context, "getString")(result.getString(index))
        val wasNull = MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull())
        MySqlSchemaAuditor.decodeNumericCell(value, wasNull, budget, context)
      case MySqlAuditJdbcValueKind.SqlNull =>
        if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN SQL NULL typed cell BEFORE JDBC allocation")
        val value = MySqlSchemaAuditor.jdbcRead(context, "getObject")(result.getObject(index))
        val wasNull = MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull())
        MySqlSchemaAuditor.decodeJdbcNullCell(value, wasNull, context)
      case MySqlAuditJdbcValueKind.Text =>
        if allocation!=null then allocation.retain(256L,"schema5 UNKNOWN text stream owner BEFORE acquisition")
        val reader = MySqlSchemaAuditor.jdbcRead(context, "getCharacterStream")(result.getCharacterStream(index))
        if reader == null then
          if !MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull()) then reject(s"MySQL audit $context returned no character stream for a non-NULL value")
          MySqlAuditNull
        else
          MySqlSchemaAuditor.jdbcRead(context, "character stream decode") {
            val text = withResource(reader)(value=>if allocation==null then readBoundedText(value)
              else MySqlSchemaAuditor.readBoundedTextOwned(value,allocation,context))
            val byteCount = MySqlSchemaAuditor.boundedUtf8ByteLength(text, MySqlSchemaAuditor.MaxCellBytes, context)
            budget.addBytes(byteCount, context)
            MySqlAuditText(text)
          }

  private def readBoundedBytes(stream: InputStream): Array[Byte] =
    val output = new ByteArrayOutputStream(8192)
    val buffer = new Array[Byte](8192)
    var read = stream.read(buffer)
    while read >= 0 do
      if read == 0 then reject("MySQL audit binary stream made no read progress")
      if output.size().toLong + read > MySqlSchemaAuditor.MaxCellBytes then reject("MySQL audit binary cell exceeds the byte bound")
      output.write(buffer, 0, read)
      read = stream.read(buffer)
    output.toByteArray

  private def readBoundedText(reader: Reader): String =
    val output = new java.lang.StringBuilder()
    val buffer = new Array[Char](8192)
    var read = reader.read(buffer)
    while read >= 0 do
      if read == 0 then reject("MySQL audit text stream made no read progress")
      if output.length().toLong + read > MySqlSchemaAuditor.MaxCellBytes then reject("MySQL audit text cell exceeds the character bound")
      output.append(buffer, 0, read)
      read = reader.read(buffer)
    val value = output.toString
    MySqlSchemaAuditor.boundedUtf8ByteLength(value, MySqlSchemaAuditor.MaxCellBytes, "text cell")
    value

  private def validateHeaderFooterIdentity(
      headerSet: MySqlAuditResultSet,
      footerSet: MySqlAuditResultSet,
      before: MySqlCallerScope,
      after: MySqlCallerScope,
      scope: MySqlAuditBootScope
  ): Unit =
    MySqlSchemaAuditor.requireLabels(headerSet.columns, MySqlSchemaAuditor.labelsFor(expectation.profile).head, "header")
    MySqlSchemaAuditor.requireLabels(footerSet.columns, MySqlSchemaAuditor.labelsFor(expectation.profile).head, "footer")
    val header = oneRow(headerSet, "header")
    val footer = oneRow(footerSet, "footer")
    val timeIndex = headerSet.columns.indexWhere(_.label == "observed_db_time")
    if timeIndex < 0 || headerSet.columns != footerSet.columns then reject("MySQL audit header/footer resultset shape differs")
    header.cells.zip(footer.cells).zipWithIndex.foreach { case ((left, right), index) =>
      if index != timeIndex && left != right then reject("MySQL audit persistent header/footer identity tuple changed")
    }
    val id = MySqlSchemaAuditor.uuidTextCell(textCell(headerSet, header, "server_uuid"), "audit server UUID")
    if id != expectation.serverUuid || textCell(headerSet, header, "server_version") != expectation.serverVersion then reject("MySQL audit procedure reports the wrong server")
    if longCell(headerSet, header, "connection_id") != before.connectionId || before.connectionId != after.connectionId then
      reject("MySQL audit resultsets do not belong to the caller-owned connection")
    if textCell(headerSet, header, "audit_definer") != expectation.auditDefiner then reject("MySQL audit procedure definer identity is unexpected")
    if textCell(headerSet, header, "effective_roles") != expectation.auditDefinerRoles then reject("MySQL audit definer effective roles are unexpected")
    if textCell(headerSet, header, "connected_caller") != before.connectedUser || before.connectedUser != after.connectedUser then
      reject("MySQL audit caller identity changed across the routine boundary")
    if textCell(headerSet, header, "routine_default_schema") != expectation.schema || textCell(headerSet, header, "audited_schema") != expectation.schema then
      reject("MySQL audit procedure ran outside the fixed schema")
    if intCell(headerSet, header, "format_version") != expectation.profile.schemaFormat then
      reject("MySQL audit header format differs from the explicitly selected schema profile")
    dateTimeCell(headerSet, header, "observed_db_time")
    dateTimeCell(footerSet, footer, "observed_db_time")
    if scope.connectionIdentity.serverUuid != id then reject("verified connection and audit result report different MySQL instances")

  private def validateReadyManifest(headerSet: MySqlAuditResultSet, header: MySqlAuditRow,
      actualObjectSha256: String,auditAccount:MySqlFormat3ReadBudget=null): MySqlReadyManifest =
    if intCell(headerSet, header, "singleton_fixed1") != 1 || textCell(headerSet, header, "backend") != "MYSQL" ||
      intCell(headerSet, header, "format_version") != expectation.profile.schemaFormat then
      reject(s"MySQL schema manifest is not singleton format ${expectation.profile.schemaFormat}")
    if binaryUuid(headerSet, header, "provisioning_uuid") != expectation.provisioningUuid then reject("MySQL provisioning identity differs from the approved manifest")
    if textCell(headerSet, header, "bootstrap_state") != "READY" then reject("MySQL schema manifest is not READY")
    if binaryUuid(headerSet, header, "expected_server_uuid") != expectation.serverUuid || binaryUuid(headerSet, header, "expected_binding_uuid") != expectation.bindingUuid then
      reject("MySQL manifest server or expected binding identity differs from configuration")
    if binaryHex(headerSet, header, "ddl_sha256") != expectation.ddlSha256 then reject("MySQL DDL hash differs from the independently approved candidate")
    val importState = textCell(headerSet, header, "import_state")
    val importOperation = optionalBinaryUuid(headerSet, header, "import_operation_uuid")
    val importManifest = optionalBinaryHash(headerSet, header, "import_manifest_sha256")
    val importSnapshot = optionalBinaryHash(headerSet, header, "import_snapshot_sha256")
    val importReceipt = optionalBinaryHash(headerSet, header, "import_receipt_sha256")
    importState match
      case "NONE" if importOperation.isEmpty && importManifest.isEmpty && importSnapshot.isEmpty && importReceipt.isEmpty => ()
      case "COMMITTED" if importOperation.nonEmpty && importManifest.nonEmpty && importSnapshot.nonEmpty && importReceipt.nonEmpty => ()
      case _ => reject("MySQL schema manifest import state is incomplete or not committed")
    expectation.expectedImport match
      case MySqlExpectedNoImport if importState == "NONE" => ()
      case approved: MySqlExpectedCommittedImport
          if importState == "COMMITTED" && importOperation.contains(approved.operationUuid) &&
            importManifest.contains(approved.manifestSha256.toLowerCase(java.util.Locale.ROOT)) &&
            importSnapshot.contains(approved.snapshotSha256.toLowerCase(java.util.Locale.ROOT)) &&
            importReceipt.contains(approved.receiptSha256.toLowerCase(java.util.Locale.ROOT)) => ()
      case _ => reject("MySQL import state or receipt differs from the independently approved import evidence")
    val guardHash = binaryHex(headerSet, header, "guard_manifest_sha256")
    if guardHash != actualObjectSha256 || guardHash != expectation.objects.canonicalSha256 then
      reject(s"MySQL READY manifest guard hash does not match the complete format ${expectation.profile.schemaFormat} object audit")
    if auditAccount==null then expectation.validateSourceManifest(headerSet,header)
    else expectation.validateSourceManifest(headerSet,header,auditAccount)
    MySqlReadyManifest(importState, importOperation, importManifest, importSnapshot, importReceipt)

  /** 纯数据边界，供观测验证与 fixture 测试共用；不创建 schema qualification。 */
  private[mysql] def validateReadyManifestForQualification(
      headerSet: MySqlAuditResultSet,
      actualObjectSha256: String
  ): MySqlReadyManifest =
    validateReadyManifest(headerSet, oneRow(headerSet, "header"), actualObjectSha256)

  /** BNone/Legacy 可参与 READY schema qualification；本检查不认领或创建 physical binding。 */
  private[mysql] def validatePreAcquirePhysicalBinding(bindingSet: MySqlAuditResultSet): MySqlObservedPhysicalBinding =
    val binding = parsePhysicalBinding(bindingSet, oneRowOrEmpty(bindingSet, "binding"))
    binding match
      case MySqlNoPhysicalBinding => ()
      case MySqlLegacyPhysicalBinding(id, version) =>
        if id != expectation.bindingUuid || version != expectation.expectedPhysicalFormat.version then
          reject("legacy MySQL binding differs from the explicitly approved binding identity or physical format")
      case MySqlCertifiedPhysicalBinding(id, version) =>
        if id != expectation.bindingUuid || version != expectation.expectedPhysicalFormat.version then
          reject("certified MySQL binding differs from the explicitly approved binding identity or physical format")
    binding

  /** 只比较 post-acquire 的数据库 fence；不读取 JDBC，也不创建 runtime receipt。 */
  private[mysql] def validateAcquiredWriterFence(
      bindingSet: MySqlAuditResultSet,
      writerSet: MySqlAuditResultSet,
      footerSet: MySqlAuditResultSet,
      acquiredFence: MySqlWriterFenceExpectation
  ): MySqlWriterObservation =
    if acquiredFence == null then reject("acquired MySQL writer fence is required")
    parsePhysicalBinding(bindingSet, oneRowOrEmpty(bindingSet, "binding")) match
      case MySqlCertifiedPhysicalBinding(id, version)
          if id == expectation.bindingUuid && id == acquiredFence.bindingUuid && version == expectation.expectedPhysicalFormat.version => ()
      case _ => reject("runtime schema receipt requires the exact certified physical binding selected by its schema expectation")
    val writer = parseWriter(writerSet, oneRow(writerSet, "writer"))
    if writer.bindingUuid != acquiredFence.bindingUuid || writer.ownerBootUuid != Some(acquiredFence.ownerBootUuid) || writer.epoch != acquiredFence.epoch then
      reject("MySQL writer row does not match the acquired boot and epoch fence")
    if writer.leaseUntil != acquiredFence.leaseUntil then reject("MySQL writer lease differs from the acquired fence token")
    if !writer.leaseUntil.isAfter(dateTimeCell(footerSet, oneRow(footerSet, "footer"), "observed_db_time")) then
      reject("MySQL writer fence lease is not valid through the completed audit")
    writer

  private def parsePhysicalBinding(resultSet: MySqlAuditResultSet, row: Option[MySqlAuditRow]): MySqlObservedPhysicalBinding = row match
    case None => MySqlNoPhysicalBinding
    case Some(value) =>
      if intCell(resultSet, value, "singleton_fixed1") != 1 then reject("MySQL physical binding singleton is invalid")
      val id = binaryUuid(resultSet, value, "binding_uuid")
      val state = textCell(resultSet, value, "cutover_state")
      val version = intCell(resultSet, value, "format_version")
      if version != 1 && version != 2 then reject("MySQL physical binding format is unknown")
      state match
        case "LEGACY_UNCERTIFIED" => MySqlLegacyPhysicalBinding(id, version)
        case "CERTIFIED" => MySqlCertifiedPhysicalBinding(id, version)
        case _ => reject("MySQL physical binding state is unknown")

  /** 纯 DATA 比较，复用原 pre-acquire writer 规则；不取得 fence、JDBC 或运行资格。 */
  private[mysql] def validatePreAcquireWriterObservation(writerSet: MySqlAuditResultSet): MySqlWriterObservation =
    val writer = parseWriter(writerSet, oneRow(writerSet, "writer"))
    if writer.bindingUuid != expectation.bindingUuid then reject("MySQL writer row binding does not match the approved binding")
    writer

  private def parseWriter(resultSet: MySqlAuditResultSet, row: MySqlAuditRow): MySqlWriterObservation =
    if intCell(resultSet, row, "singleton_fixed1") != 1 then reject("MySQL runtime writer singleton is invalid")
    val binding = binaryUuid(resultSet, row, "binding_uuid")
    val epoch = longCell(resultSet, row, "writer_epoch")
    if epoch < 0L then reject("MySQL runtime writer epoch is invalid")
    val ownerBoot = optionalBinaryUuid(resultSet, row, "owner_boot_uuid")
    val lease = dateTimeCell(resultSet, row, "lease_until")
    val operation = optionalBinaryUuid(resultSet, row, "last_commit_operation_uuid")
    val fingerprint = optionalBinaryHash(resultSet, row, "last_commit_fingerprint")
    if operation.isDefined != fingerprint.isDefined then reject("MySQL last-commit receipt fields are incomplete")
    MySqlWriterObservation(binding, epoch, ownerBoot, lease)

  private def oneRow(resultSet: MySqlAuditResultSet, label: String): MySqlAuditRow =
    if resultSet.rows.size != 1 then reject(s"MySQL $label resultset must contain exactly one row")
    val row = resultSet.rows.head
    if row.cells.size != resultSet.columns.size then reject(s"MySQL $label row does not match its resultset shape")
    row

  private def oneRowOrEmpty(resultSet: MySqlAuditResultSet, label: String): Option[MySqlAuditRow] =
    if resultSet.rows.size > 1 then reject(s"MySQL $label resultset returned multiple rows")
    resultSet.rows.headOption.map { row =>
      if row.cells.size != resultSet.columns.size then reject(s"MySQL $label row does not match its resultset shape")
      row
    }

  private def textCell(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String =
    cell(resultSet, row, label) match
      case MySqlAuditText(value) => value
      case _ => reject(s"MySQL audit field $label is null or not text")

  private def dateTimeCell(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): LocalDateTime =
    val index = resultSet.columns.indexWhere(_.label == label)
    if index < 0 || resultSet.columns(index).jdbcType != Types.TIMESTAMP then
      reject(s"MySQL audit field $label does not use the approved JDBC TIMESTAMP type")
    row.cells(index) match
      case MySqlAuditTimestamp(value) => value
      case _ => reject(s"MySQL audit field $label is null or not a JDBC TIMESTAMP")

  private def intCell(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Int =
    val value = textCell(resultSet, row, label)
    try value.toInt
    catch case failure: NumberFormatException => MySqlSchemaAuditor.reject(s"MySQL audit integer field $label is malformed", failure)

  private def longCell(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Long =
    val value = textCell(resultSet, row, label)
    try value.toLong
    catch case failure: NumberFormatException => MySqlSchemaAuditor.reject(s"MySQL audit long field $label is malformed", failure)

  private def binaryUuid(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): UUID = cell(resultSet, row, label) match
    case bytes: MySqlAuditBinary => MySqlSchemaAuditor.uuidFromBytes(bytes.bytes, label)
    case _ => reject(s"MySQL audit binary UUID field $label is null or not binary")

  private def optionalBinaryUuid(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Option[UUID] = cell(resultSet, row, label) match
    case MySqlAuditNull => None
    case bytes: MySqlAuditBinary => Some(MySqlSchemaAuditor.uuidFromBytes(bytes.bytes, label))
    case _ => reject(s"MySQL optional UUID field $label has an invalid representation")

  private def optionalBinaryHash(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Option[String] = cell(resultSet, row, label) match
    case MySqlAuditNull => None
    case bytes: MySqlAuditBinary => Some(MySqlSchemaAuditor.hashFromBytes(bytes.bytes, label))
    case _ => reject(s"MySQL optional hash field $label has an invalid representation")

  private def binaryHex(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String = cell(resultSet, row, label) match
    case bytes: MySqlAuditBinary => MySqlSchemaAuditor.hashFromBytes(bytes.bytes, label)
    case _ => reject(s"MySQL binary hash field $label is null or not binary")

  private def uuidText(result: ResultSet, index: Int, label: String): UUID =
    val value = result.getString(index)
    if value == null || result.wasNull() || !MySqlSchemaAuditor.CanonicalUuid.matches(value) then reject(s"MySQL $label is unavailable or malformed")
    try UUID.fromString(value)
    catch case failure: IllegalArgumentException => MySqlSchemaAuditor.reject(s"MySQL $label is malformed", failure)

  private def longValue(result: ResultSet, index: Int, label: String): Long =
    val value = result.getString(index)
    if value == null || result.wasNull() then reject(s"MySQL $label is unavailable")
    try value.toLong
    catch case failure: NumberFormatException => MySqlSchemaAuditor.reject(s"MySQL $label is malformed", failure)

  private def requiredText(value: String, field: String): String =
    if value == null || value.isBlank then reject(s"MySQL $field is unavailable")
    value

  private def cell(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): MySqlAuditCell =
    val columns = resultSet.columns
    val index = columns.indexWhere(_.label == label)
    if index < 0 || index >= row.cells.size then reject(s"MySQL audit result is missing field $label")
    row.cells(index)

  private def reject(message: String): Nothing = MySqlSchemaAuditor.reject(message)

  private def withResource[R <: AutoCloseable, A](resource: R)(body: R => A): A =
    var primary: Throwable = null
    try body(resource)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary == null then resource.close()
      else
        try resource.close()
        catch case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)


object MySqlSchemaAuditor:
  /** 仅由本对象的比较器生成安全摘要；异常边界不必输出可能含秘密的 SQLException message。 */
  private[floatingpod] final class StaticManifestMismatch private[MySqlSchemaAuditor] (
      val safeDifference: String, schemaFormat: Int)
      extends SQLNonTransientConnectionException(
        s"MySQL static objects, all triggers, or required auditself differ from the approved format $schemaFormat manifest; $safeDifference")

  private[floatingpod] def staticManifestMismatch(profile: MySqlSchemaAuditProfile,
      expected: Vector[MySqlAuditResultSet], actual: Vector[MySqlAuditResultSet]): StaticManifestMismatch =
    new StaticManifestMismatch(firstStaticDifference(expected, actual).getOrElse("firstDifference=unavailable"),profile.schemaFormat)

  /** One registered, private UNKNOWN audit attempt; it reuses the original issuer without changing public audit semantics. */
  private[mysql] final class SourceUnknownInspectionAudit private[MySqlSchemaAuditor] (
      private[mysql] val issuer:MySqlSchemaAuditor,
      private[mysql] val verified:VerifiedMySqlConnection,
      private[mysql] val scope:MySqlAuditBootScope,
      private[mysql] val root:MySqlFormat3ReadBudget,
      private[mysql] val auditAccount:MySqlFormat3ReadBudget,
      private[mysql] val owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5
  ):
    private var attempted=false
    private[mysql] var allocation:SourceUnknownInspectionAllocation=null
    private[mysql] var observation:UnverifiedMySqlSchemaObservation=null
    private[mysql] var qualification:VerifiedSchemaQualification=null
    private[mysql] var failure:Throwable=null

    private[mysql] def isBoundTo(actual:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):Boolean =
      (actual ne null) && (owner eq actual) && (issuer ne null) && (verified ne null) && (scope ne null) &&
        (root ne null) && (auditAccount ne null) && (owner.auditAccount eq auditAccount) &&
        (owner.newVerified eq verified) && (owner.newScope eq scope) && (owner.root eq root) &&
        (owner.failure eq null) && (failure eq null)

    private[mysql] def readAndQualifyOnce():VerifiedSchemaQualification =
      owner.requireAuditBinding(issuer.expectation,verified,scope,root,auditAccount)
      if attempted || allocation!=null || observation!=null || qualification!=null || failure!=null then
        throw new IllegalStateException("schema5 UNKNOWN owned audit is one-shot",failure)
      auditAccount.reserveRetainedMemory(1024L,
        "schema5 UNKNOWN per-call allocation account BEFORE allocation")
      attempted=true
      allocation=new SourceUnknownInspectionAllocation(auditAccount,owner,this)
      try
        observation=issuer.readSourceUnknownInspectionManifest(this,allocation)
        allocation.retainScratch(observation,"schema5 UNKNOWN complete fresh observation")
        qualification=issuer.auditBeforeWriterAcquireOwned(observation,scope,allocation)
        allocation.retainScratch(qualification,"schema5 UNKNOWN fresh qualification")
        qualification
      catch
        case cause:Throwable =>
          failure=cause
          try allocation.retainFailure(cause)
          catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          throw cause

  /** Session在SQL前登记，原child保存全部partial audit graph及qualification寿命。 */
  private[mysql] final class DbFirstAudit private[MySqlSchemaAuditor] (
      private val issuer:MySqlSchemaAuditor,private val inputs:MySqlDbFirstSessionInputs,
      private val root:MySqlFormat3ReadBudget,private val verified:VerifiedMySqlConnection,
      private val scope:MySqlAuditBootScope,private val owner:MySqlDbFirstSession,
      private val writerCoordinator:MySqlWriterFenceCoordinator=null
  ):
    private val thread=Thread.currentThread()
    private var attempted=false
    private var closed=false
    private var closing=false
    private var account:MySqlFormat3ReadBudget=null
    private var allocation:AuditAllocation=null
    private var observation:UnverifiedMySqlSchemaObservation=null
    private var qualification:VerifiedSchemaQualification=null
    private var failure:Throwable=null
    private var cleanupFailure:Throwable=null
    private var holderFee=1024L
    private var currentReceipt:RuntimeSchemaAuditReceipt=null
    private def requireOwner():Unit =
      if Thread.currentThread() ne thread then throw new IllegalStateException("schema6 audit requires original worker")
      if closed || closing then throw new IllegalStateException("schema6 audit detached or cleanup incomplete",cleanupFailure)
      owner.requireDbFirstAuditBinding(this,root,inputs,verified,scope)
      if account != null && (!account.isCanonicalChildOf(root) || !account.sharesAllocationLedger(root) ||
        (account.profile ne root.profile) || account.isClosed) then
        throw new IllegalStateException("schema6 audit requires original live child")
    private[mysql] def requireExclusiveSessionOwner(actual:MySqlDbFirstSession):Unit =
      requireOwner()
      require(actual!=null && (owner eq actual) && attempted && failure==null && cleanupFailure==null,
        "actual live audit must already belong to the exact service Session")
    private[mysql] def requireCurrentPreConsumer(actual:MySqlWriterFenceCoordinator,token:MySqlWriterFenceToken):Unit =
      requireWriter(actual,token.verified,token.scope)
      require(qualification!=null && (token.qualification eq qualification),"actual token must consume this pre-audit qualification")
    private[mysql] def requireCurrentPostConsumer(actual:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission):Unit =
      requireWriter(actual,admission.token.verified,admission.token.scope)
      require(currentReceipt!=null && (admission.receipt eq currentReceipt),"actual admission must consume this post-audit receipt")
    private def requireWriter(actual:MySqlWriterFenceCoordinator,actualVerified:VerifiedMySqlConnection,
        actualScope:MySqlAuditBootScope):Unit =
      requireOwner()
      if writerCoordinator == null || (actual ne writerCoordinator) || (actualVerified ne verified) || (actualScope ne scope) then
        throw new IllegalStateException("exact registered schema6 writer audit/coordinator required")
      owner.requireDbFirstWriterAuditSource(root,inputs,verified,scope,actual)
    private[mysql] def preAcquire(actual:MySqlWriterFenceCoordinator,actualVerified:VerifiedMySqlConnection,
        actualScope:MySqlAuditBootScope):(UnverifiedMySqlSchemaObservation,VerifiedSchemaQualification) =
      requireWriter(actual,actualVerified,actualScope)
      val qualified=readAndQualifyOnce()
      (observation,qualified)
    private[mysql] def caller(actual:MySqlWriterFenceCoordinator,actualVerified:VerifiedMySqlConnection,
        actualScope:MySqlAuditBootScope):MySqlCallerScope =
      requireWriter(actual,actualVerified,actualScope)
      if allocation == null then throw new IllegalStateException("owned writer audit allocation unavailable")
      issuer.readCallerScope(verified,scope,allocation,"schema6 writer fence caller")
    private[mysql] def postAcquire(actual:MySqlWriterFenceCoordinator,token:MySqlWriterFenceToken):RuntimeSchemaAuditReceipt =
      if token == null then throw new IllegalArgumentException("actual schema6 writer token required")
      requireWriter(actual,token.verified,token.scope)
      runOnce {
        val receipt=issuer.auditAfterWriterAcquireInternal(observation,token.qualification,scope,token.fence,allocation)
        currentReceipt=receipt
        allocation.retainScratch(receipt,"schema6 retained post-acquire receipt")
        receipt
      }
    def readAndQualifyOnce():VerifiedSchemaQualification = runOnce {
      qualification=issuer.auditBeforeWriterAcquireInternal(observation,scope,allocation)
      allocation.retainScratch(qualification,"schema6 retained qualification")
      qualification
    }
    private def runOnce[A](decode: => A):A =
      requireOwner()
      if attempted then throw new IllegalStateException("schema6 owned audit is one-shot",failure)
      attempted=true
      try
        account=root.newCanonicalDataScope()
        account.reserveRetainedMemory(1024L,"schema6 per-call allocation graph BEFORE allocation")
        allocation=new AuditAllocation(account):
          override protected def requireOwner():Unit = DbFirstAudit.this.requireOwner()
          override protected def requireObservationOwner(value:UnverifiedMySqlSchemaObservation):Unit =
            if value==null || (value.scope ne scope) || value.profile != MySqlSchemaAuditProfile.DbFirst1 then
              throw new IllegalStateException("original schema6 observation required",failure)
        observation=issuer.readCurrentObjectManifestInternal(verified,scope,allocation)
        allocation.retainScratch(observation,"schema6 complete owned observation")
        decode
      catch case cause:Throwable =>
        failure=cause
        if allocation != null then
          try allocation.retainFailure(cause)
          catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
    def close():Unit =
      if Thread.currentThread() ne thread then throw new IllegalStateException("schema6 audit requires original worker")
      if closed then return
      if cleanupFailure != null then throw new IllegalStateException("schema6 audit cleanup remains incomplete",cleanupFailure)
      owner.requireDbFirstAuditBinding(this,root,inputs,verified,scope)
      closing=true
      try
        qualification=null;observation=null;allocation=null;currentReceipt=null
        if account != null then
          account.closeScopedReadBudget()
          account=null
        if holderFee != 0L then
          root.releaseRetainedMemory(holderFee,"schema6 partial audit holder detached")
          holderFee=0L
        closed=true
      catch case cause:Throwable =>
        cleanupFailure=cause
        if failure == null then failure=cause else MySqlOwnedSessionLifecycle.suppress(failure,cause)
        throw cause

  /** 共用逐行/stream/caller/comparison实际计费；具体发行者必须提供原owner身份guard。 */
  private[mysql] abstract class AuditAllocation private[mysql] (val account:MySqlFormat3ReadBudget):
    protected def requireOwner():Unit
    protected def requireFailureOwner():Unit = requireOwner()
    protected def requireObservationOwner(value:UnverifiedMySqlSchemaObservation):Unit
    private val scratch = new java.util.ArrayList[AnyRef]()
    private val callerScopes = new java.util.ArrayList[MySqlCallerScope]()
    private var retainedFailure:Throwable=null

    def retain(bytes:Long,label:String):Unit =
      requireOwner()
      if bytes<0L || label==null then throw new IllegalArgumentException("non-negative actual UNKNOWN audit allocation fee and label required")
      account.reserveRetainedMemory(bytes,label)

    def retainScratch(value:AnyRef,label:String):Unit =
      if value==null then throw new IllegalArgumentException("non-null UNKNOWN audit partial allocation required")
      retain(96L,label + " owner reference BEFORE retention")
      scratch.add(value)

    def retainCallerScope(stage:String,value:MySqlCallerScope):Unit =
      if stage==null || value==null then throw new IllegalArgumentException("named actual caller-scope observation required")
      var utf8=0L
      def add(value:String):Unit = utf8=Math.addExact(utf8,MySqlSchemaAuditor.boundedUtf8ByteLength(
        value,MySqlSchemaAuditor.MaxCellBytes,"UNKNOWN caller-scope observation").toLong)
      utf8=Math.addExact(utf8,36L) // canonical UUID text has a fixed actual 36-byte representation.
      add(value.defaultSchema)
      add(value.effectivePrincipal)
      add(value.effectiveRoles)
      add(value.connectedUser)
      value.serverVersion.foreach(add)
      value.ownGrants.foreach(add)
      val strings=6L + value.serverVersion.size.toLong + value.ownGrants.size.toLong
      retain(Math.addExact(256L,Math.addExact(Math.multiplyExact(32L,utf8),Math.multiplyExact(96L,strings))),
        s"schema5 UNKNOWN $stage caller-scope retained observation BEFORE allocation")
      callerScopes.add(value)

    def retainObservation(value:UnverifiedMySqlSchemaObservation):Unit =
      requireOwner()
      requireObservationOwner(value)
      retain(512L,"owned audit retained observation owner BEFORE allocation")

    def retainFailure(cause:Throwable):Unit =
      requireFailureOwner()
      if cause==null then throw new IllegalArgumentException("schema5 UNKNOWN audit failure cause required")
      if retainedFailure==null then retainedFailure=cause
      else if retainedFailure ne cause then MySqlSchemaAuditor.addSuppressed(retainedFailure,cause)

    private[mysql] def failureObservation:Option[Throwable] =
      requireFailureOwner()
      Option(retainedFailure)

    def withTemporary[A](bytes:Long,label:String)(body: => A):A =
      requireOwner()
      if bytes<0L || label==null then throw new IllegalArgumentException("non-negative actual UNKNOWN audit scratch fee and label required")
      account.reserveTemporaryMemory(bytes,label + " BEFORE allocation")
      var primary:Throwable=null
      try body
      catch
        case cause:Throwable => primary=cause;throw cause
      finally
        try account.releaseTemporaryMemory(bytes,label + " released")
        catch
          case cleanup:Throwable =>
            if primary==null then throw cleanup
            else MySqlSchemaAuditor.addSuppressed(primary,cleanup)

  /** 原UNKNOWN专用入口仍只由原inspection owner构造。 */
  private[mysql] final class SourceUnknownInspectionAllocation private[MySqlSchemaAuditor] (
      auditAccount:MySqlFormat3ReadBudget,
      private val owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5,
      private val audit:SourceUnknownInspectionAudit
  ) extends AuditAllocation(auditAccount):
    override protected def requireOwner():Unit =
      owner.requireAuditBinding(audit.issuer.expectation,audit.verified,audit.scope,audit.root,account)
    override protected def requireFailureOwner():Unit =
      if (Thread.currentThread() ne owner.worker) || (audit.owner ne owner) || (owner.root ne audit.root) ||
        (owner.auditAccount ne account) || (owner.newVerified ne audit.verified) || (owner.newScope ne audit.scope) then
        throw new IllegalStateException("original UNKNOWN audit failure owner required")
    override protected def requireObservationOwner(value:UnverifiedMySqlSchemaObservation):Unit =
      if value==null || (audit.owner ne owner) || (value.scope ne audit.scope) ||
        value.profile != MySqlSchemaAuditProfile.DeploymentReceipt1 then
        throw new IllegalStateException("exact completed schema5 UNKNOWN audit observation required",audit.failure)

  /** 仅描述首个差异位置、类型和摘要；保留严格相等准入，不输出 SQL/body 或任意 cell 原文。 */
  private[mysql] def firstStaticDifference(expected: Vector[MySqlAuditResultSet],
                                         actual: Vector[MySqlAuditResultSet]): Option[String] =
    def location(set: Int, row: Int, column: Int, field: String): String =
      s"resultSet=${MySqlAuditCoverage.StaticResultSetIds.lift(set).getOrElse("unknown")} row=$row column=$column field=$field"
    def bytesSummary(kind: String, bytes: Array[Byte]): String =
      try s"$kind[bytes=${bytes.length},sha256=${HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))}]"
      finally Arrays.fill(bytes, 0.toByte)
    def textSummary(value: String): String = bytesSummary("TEXT", value.getBytes(StandardCharsets.UTF_8))
    def cellSummary(cell: MySqlAuditCell): String = cell match
      case MySqlAuditNull => "SQL_NULL"
      case MySqlAuditText(value) => textSummary(value)
      case binary: MySqlAuditBinary => bytesSummary("BINARY", binary.bytes)
      case MySqlAuditTimestamp(value) => bytesSummary("TIMESTAMP", value.toString.getBytes(StandardCharsets.UTF_8))
    def metadata(column: MySqlAuditColumn): Vector[(String, String | Int | Boolean)] = Vector(
      "label" -> column.label, "jdbcType" -> column.jdbcType,
      "typeName" -> column.typeName, "precision" -> column.precision,
      "scale" -> column.scale, "nullable" -> column.nullable,
      "signed" -> column.signed)
    def metadataSummary(value: String | Int | Boolean): String = value match
      case text: String => textSummary(text)
      case number: Int => number.toString
      case flag: Boolean => flag.toString
    if expected.size != actual.size then
      return Some(s"field=resultSet.count expected=${expected.size} actual=${actual.size}")
    var set = 0
    while set < expected.size do
      val before = expected(set)
      val after = actual(set)
      if before.id != after.id then
        return Some(s"${location(set, 0, 0, "resultSet.id")} expected=${textSummary(before.id)} actual=${textSummary(after.id)}")
      if before.columns.size != after.columns.size then
        return Some(s"${location(set, 0, 0, "column.count")} expected=${before.columns.size} actual=${after.columns.size}")
      var column = 0
      while column < before.columns.size do
        if before.columns(column) != after.columns(column) then
          val beforeMetadata = metadata(before.columns(column))
          val afterMetadata = metadata(after.columns(column))
          val field = beforeMetadata.indices.find(i => beforeMetadata(i) != afterMetadata(i)).get
          return Some(s"${location(set, 0, column + 1, "column." + beforeMetadata(field)._1)} expected=${metadataSummary(beforeMetadata(field)._2)} actual=${metadataSummary(afterMetadata(field)._2)}")
        column += 1
      if before.rows.size != after.rows.size then
        return Some(s"${location(set, 0, 0, "row.count")} expected=${before.rows.size} actual=${after.rows.size}")
      var row = 0
      while row < before.rows.size do
        val beforeCells = before.rows(row).cells
        val afterCells = after.rows(row).cells
        if beforeCells.size != afterCells.size then
          return Some(s"${location(set, row + 1, 0, "cell.count")} expected=${beforeCells.size} actual=${afterCells.size}")
        column = 0
        while column < beforeCells.size do
          if beforeCells(column) != afterCells(column) then
            return Some(s"${location(set, row + 1, column + 1, "cell")} expected=${cellSummary(beforeCells(column))} actual=${cellSummary(afterCells(column))}")
          column += 1
        row += 1
      set += 1
    None

  private[mysql] def staticValidationScratch(
      sets:Vector[MySqlAuditResultSet],from:Int,until:Int,
      profile:MySqlSchemaAuditProfile=MySqlSchemaAuditProfile.DeploymentReceipt1
  ):Long =
    if sets==null || from<0 || until<from || until>sets.size then
      reject("schema5 UNKNOWN static-validation fee requires actual bounded resultset indexes")
    var entries=0L
    var textBytes=0L
    var maxBinary=0L
    var index=from
    while index<until do
      val set=sets(index)
      if set==null then reject("schema5 UNKNOWN static-validation input contains a missing resultset")
      entries=Math.addExact(entries,Math.addExact(1L,set.columns.size.toLong))
      set.columns.foreach { column =>
        if column!=null then textBytes=Math.addExact(textBytes,
          boundedUtf8ByteLength(column.label,MaxCellBytes,"schema5 UNKNOWN static column label").toLong +
            boundedUtf8ByteLength(column.typeName,MaxCellBytes,"schema5 UNKNOWN static JDBC type").toLong)
      }
      entries=Math.addExact(entries,set.rows.size.toLong)
      set.rows.foreach { row =>
        if row!=null && row.cells!=null then
          entries=Math.addExact(entries,row.cells.size.toLong)
          row.cells.foreach {
            case MySqlAuditText(value) => textBytes=Math.addExact(textBytes,
              boundedUtf8ByteLength(value,MaxCellBytes,"schema5 UNKNOWN static text").toLong)
            case binary:MySqlAuditBinary => maxBinary=Math.max(maxBinary,binary.length.toLong)
            case MySqlAuditTimestamp(value) =>
              // Fixed representation, accounted without allocating its String form.
              textBytes=Math.addExact(textBytes,32L)
            case MySqlAuditNull => ()
            case null => ()
          }
      }
      index+=1
    val profileEntries=if until-from==10 then
      val closedEntries=if profile == MySqlSchemaAuditProfile.DbFirst1 then 27L+480L+81L else 26L+467L+78L
      Math.addExact(closedEntries,entries)
    else entries
    val profileScratch=Math.multiplyExact(96L,profileEntries)
    Math.addExact(4096L,Math.addExact(profileScratch,
      Math.addExact(Math.multiplyExact(32L,textBytes),Math.multiplyExact(32L,maxBinary))))

  private[mysql] def canonicalHashScratch(sets:Vector[MySqlAuditResultSet]):Long =
    if sets==null then reject("schema5 UNKNOWN hash fee requires actual static sets")
    var maxEncoded=0L
    sets.foreach { set =>
      if set==null then reject("schema5 UNKNOWN hash fee encountered a missing static set")
      set.columns.foreach { column =>
        maxEncoded=Math.max(maxEncoded,boundedUtf8ByteLength(column.label,MaxCellBytes,"schema5 hash label").toLong)
        maxEncoded=Math.max(maxEncoded,boundedUtf8ByteLength(column.typeName,MaxCellBytes,"schema5 hash type").toLong)
      }
      set.rows.foreach(_.cells.foreach {
        case MySqlAuditText(value) => maxEncoded=Math.max(maxEncoded,
          boundedUtf8ByteLength(value,MaxCellBytes,"schema5 canonical hash text").toLong)
        case binary:MySqlAuditBinary => maxEncoded=Math.max(maxEncoded,binary.length.toLong)
        case _ => ()
      })
    }
    Math.addExact(1024L,Math.addExact(Math.multiplyExact(32L,maxEncoded),192L))

  private[mysql] def resultSetValidationScratch(set:MySqlAuditResultSet):Long =
    if set==null then reject("schema5 UNKNOWN qualification fee requires an actual resultset")
    var text=0L
    var maxBinary=0L
    set.columns.foreach { column =>
      text=Math.addExact(text,boundedUtf8ByteLength(column.label,MaxCellBytes,"schema5 qualification label").toLong)
      text=Math.addExact(text,boundedUtf8ByteLength(column.typeName,MaxCellBytes,"schema5 qualification type").toLong)
    }
    set.rows.foreach(_.cells.foreach {
      case MySqlAuditText(value) => text=Math.addExact(text,
        boundedUtf8ByteLength(value,MaxCellBytes,"schema5 qualification text").toLong)
      case binary:MySqlAuditBinary => maxBinary=Math.max(maxBinary,binary.length.toLong)
      case _ => ()
    })
    Math.addExact(512L,Math.addExact(Math.multiplyExact(96L,Math.addExact(set.columns.size.toLong,set.rows.size.toLong)),
      Math.addExact(Math.multiplyExact(32L,text),Math.multiplyExact(32L,maxBinary))))

  private[mysql] def headerQualificationScratch(set:MySqlAuditResultSet,row:MySqlAuditRow):Long =
    if set==null || row==null then reject("schema5 UNKNOWN header fee requires actual manifest row")
    var text=0L
    var binary=0L
    row.cells.foreach {
      case MySqlAuditText(value) => text=Math.addExact(text,
        boundedUtf8ByteLength(value,MaxCellBytes,"schema5 READY manifest text").toLong)
      case value:MySqlAuditBinary => binary=Math.max(binary,value.length.toLong)
      case _ => ()
    }
    Math.addExact(1024L,Math.addExact(Math.multiplyExact(32L,text),Math.multiplyExact(32L,binary)))

  private[mysql] def vectorSliceFee(sets:Vector[MySqlAuditResultSet],from:Int,until:Int):Long =
    if sets==null || from<0 || until<from || until>sets.size then
      reject("schema5 UNKNOWN resultset-view fee requires bounded indexes")
    Math.multiplyExact(96L,(until-from).toLong)

  val RequiredProcedure = "fp_audit_schema_format1"
  val SchemaIdentifier = "[a-z][a-z0-9_]{0,63}".r
  val Sha256Hex = "(?i)[0-9a-f]{64}".r
  val MaxCellBytes = 1024 * 1024
  val MaxTotalBytes = 8 * 1024 * 1024
  val MaxRowsPerResultSet = 4096
  val MaxTotalRows = 16384
  val MaxColumnsPerResultSet = 64
  val MaxTotalCells = 262144
  val ExpectedResultSetCount = 14
  val MaxReceiptAgeNanos = Duration.ofMinutes(30).toNanos

  private[mysql] def boundedLeaseValidityNanos(
      observedDbTime: LocalDateTime,
      leaseUntil: LocalDateTime,
      maxValidityNanos: Long = MaxReceiptAgeNanos
  ): Long =
    if observedDbTime == null || leaseUntil == null then reject("MySQL fence lease timestamps are required")
    if maxValidityNanos <= 0L then reject("MySQL fence lease validity bound must be positive")
    val validity = Duration.between(observedDbTime, leaseUntil)
    if validity.isNegative || validity.isZero then reject("MySQL fence lease is not valid after its database observation")
    if validity.compareTo(Duration.ofNanos(maxValidityNanos)) >= 0 then maxValidityNanos else validity.toNanos

  private val ReadCallerScope =
    "SELECT @@server_uuid AS server_uuid, CONNECTION_ID() AS connection_id, DATABASE() AS caller_default_schema, CURRENT_USER() AS caller_effective_principal, CURRENT_ROLE() AS caller_effective_roles, USER() AS connected_user"
  /** 读取真实 cursor 的第一行全部身份字段，再推进唯一性校验；不拥有或关闭 ResultSet。 */
  private[mysql] def decodeCallerIdentity(result: ResultSet, profile: MySqlSchemaAuditProfile): MySqlCallerScope =
    if result == null || profile == null then reject("caller identity result/profile is required")
    val expected = if profile.extendedAudit then CallerLabelsFormat3 else CallerLabels
    val metadata = jdbcRead("caller identity", "getMetaData")(result.getMetaData)
    if metadata == null || metadata.getColumnCount != expected.size then reject("caller identity column count differs")
    val labels = Vector.tabulate(expected.size)(i => metadata.getColumnLabel(i + 1))
    if labels != expected then reject("caller identity column labels differ")
    if !jdbcRead("caller identity", "next")(result.next()) then reject("MySQL caller scope query returned no row")
    def text(index: Int, label: String): String =
      val value = jdbcRead("caller identity", s"getString[$index]")(result.getString(index))
      if value == null || value.isEmpty then reject(s"caller identity $label is unavailable")
      boundedUtf8ByteLength(value, MaxCellBytes, "caller identity")
      value
    val server = uuidTextCell(text(1, "server UUID"), "caller server UUID")
    val connectionId = jdbcRead("caller identity", "getLong[2]")(result.getLong(2))
    if jdbcRead("caller identity", "wasNull[2]")(result.wasNull()) || connectionId <= 0 then reject("caller identity connection ID is unavailable")
    val schema = text(3, "schema")
    val principal = text(4, "effective principal")
    val roles = text(5, "roles")
    val user = text(6, "connected user")
    val version = if profile.extendedAudit then Some(text(7, "server version")) else None
    if jdbcRead("caller identity", "next uniqueness")(result.next()) then reject("MySQL caller scope query returned more than one row")
    new MySqlCallerScope(server, connectionId, schema, principal, roles, user, version, Vector.empty)

  private val ReadCallerScopeFormat3 = ReadCallerScope + ", @@version AS caller_server_version"
  private val CallerLabelsFormat3 = Vector("server_uuid", "connection_id", "caller_default_schema", "caller_effective_principal", "caller_effective_roles", "connected_user", "caller_server_version")
  private val CallerLabels = Vector("server_uuid", "connection_id", "caller_default_schema", "caller_effective_principal", "caller_effective_roles", "connected_user")
  private val BinaryTypes = Set(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB, Types.BIT)
  private val NumericTypes = Set(
    Types.TINYINT,
    Types.SMALLINT,
    Types.INTEGER,
    Types.BIGINT,
    Types.NUMERIC,
    Types.DECIMAL,
    Types.FLOAT,
    Types.REAL,
    Types.DOUBLE
  )
  private val CharacterTypes = Set(
    Types.CHAR,
    Types.VARCHAR,
    Types.LONGVARCHAR,
    Types.NCHAR,
    Types.NVARCHAR,
    Types.LONGNVARCHAR,
    Types.CLOB,
    Types.NCLOB
  )
  private val ResultSetLabels = Vector(
    Vector("server_uuid", "server_version", "connection_id", "audit_definer", "connected_caller", "effective_roles", "routine_default_schema", "observed_db_time", "audited_schema", "singleton_fixed1", "backend", "format_version", "ddl_sha256", "provisioning_uuid", "bootstrap_state", "expected_server_uuid", "expected_binding_uuid", "import_state", "import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256", "guard_manifest_sha256"),
    Vector("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_COLLATION", "CREATE_OPTIONS", "TABLE_COMMENT"),
    Vector("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "COLUMN_COMMENT", "GENERATION_EXPRESSION"),
    Vector("TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE", "clause_octets", "clause_sha256"),
    Vector("TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "EVENT_MANIPULATION", "ACTION_TIMING", "ACTION_ORDER", "ACTION_ORIENTATION", "ACTION_STATEMENT", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "statement_octets", "statement_sha256"),
    Vector("ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "ROUTINE_DEFINITION", "SECURITY_TYPE", "SQL_DATA_ACCESS", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "IS_DETERMINISTIC", "DATA_TYPE", "DTD_IDENTIFIER", "ROUTINE_COMMENT", "definition_octets", "definition_sha256"),
    Vector("SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME", "DATA_TYPE", "DTD_IDENTIFIER", "CHARACTER_SET_NAME", "COLLATION_NAME"),
    Vector("singleton_fixed1", "binding_uuid", "cutover_state", "format_version"),
    Vector("singleton_fixed1", "binding_uuid", "writer_epoch", "owner_boot_uuid", "lease_until", "last_commit_operation_uuid", "last_commit_fingerprint"),
    Vector("server_uuid", "server_version", "connection_id", "audit_definer", "connected_caller", "effective_roles", "routine_default_schema", "observed_db_time", "audited_schema", "singleton_fixed1", "backend", "format_version", "ddl_sha256", "provisioning_uuid", "bootstrap_state", "expected_server_uuid", "expected_binding_uuid", "import_state", "import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256", "guard_manifest_sha256")
  )
  /** 实际 Statement 每一 RS ordinal / update count 状态；不是 caller 成功标记。 */
  private[mysql] def validateFormat3Protocol(states: Vector[Int]): Unit =
    if states != (1 to 18).toVector ++ Vector(0, -1) then
      reject("format3 audit protocol must contain exact18 resultsets, one actual0, terminal-1")
  private val Format3ManifestLabels = Vector("configuration_sha256", "source_provenance_kind", "source_provenance_bytes", "source_provenance_sha256", "limit_profile_encoding", "limit_profile_bytes", "limit_profile_sha256", "max_region_bytes", "max_chunk_bytes", "max_decompressed_bytes", "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes", "max_native_artifact_bytes")
  private[mysql] val Format3PrivilegeLabels = Vector(
    Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
    Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
    Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
    Vector("GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"))
  private[mysql] def labelsFor(profile: MySqlSchemaAuditProfile): Vector[Vector[String]] =
    if profile.extendedAudit then
      ResultSetLabels.updated(0, ResultSetLabels.head ++ Format3ManifestLabels)
        .updated(13, ResultSetLabels(13) ++ Format3ManifestLabels) ++ Format3PrivilegeLabels
    else ResultSetLabels
  private[mysql] def auditShapeScratch(profile:MySqlSchemaAuditProfile):Long =
    if profile==null || !profile.extendedAudit then reject("schema5 UNKNOWN exact extended audit profile required")
    val baseLabels=ResultSetLabels.foldLeft(0L)((count,labels)=>Math.addExact(count,labels.size.toLong))
    val manifestLabels=Math.multiplyExact(2L,Format3ManifestLabels.size.toLong)
    val privilegeLabels=Format3PrivilegeLabels.foldLeft(0L)((count,labels)=>Math.addExact(count,labels.size.toLong))
    val actualLabels=Math.addExact(Math.addExact(baseLabels,manifestLabels),privilegeLabels)
    val vectorHeaders=Math.addExact(Math.multiplyExact(3L,ResultSetLabels.size.toLong),18L)
    val entries=Math.addExact(Math.addExact(actualLabels,vectorHeaders),36L)
    Math.addExact(2048L,Math.multiplyExact(96L,entries))
  private val HeaderLabels = ResultSetLabels.head
  private val StaticLabels = ResultSetLabels.slice(1, 11)
  private[mysql] final class ReadBudget(
      profile: Option[com.wi.floatingpod.config.SourceLimitProfile] = None,
      allocation:AuditAllocation=null):
    private var bytes = 0L
    private var rows = 0
    private var cells = 0
    def addBytes(count: Int, context: String = "MySQL audit"): Unit =
      if count < 0 then reject("negative audit framed byte count")
      if allocation!=null then allocation.retain(Math.multiplyExact(32L,count.toLong),
        s"schema5 UNKNOWN $context decoded bytes BEFORE retained cell insertion")
      profile.foreach(p => if count.toLong > p.maxSqlPacketBytes() then reject("audit field framing exceeds explicit SQL packet bound"))
      bytes = Math.addExact(bytes, count.toLong)
      profile.foreach(p => if bytes > p.maxRetainedBytes() then reject("audit framing exceeds explicit retained-byte bound"))
      if bytes > MaxTotalBytes then reject(s"$context exceeded the total decoded-byte bound")
    def addRow(rowInSet: Int): Unit =
      if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN result row graph BEFORE allocation")
      if profile.nonEmpty then addBytes(8, "row framing")
      if rowInSet > MaxRowsPerResultSet then reject("MySQL audit resultset exceeded the per-set row bound")
      rows += 1
      if rows > MaxTotalRows then reject("MySQL audit exceeded the total row bound")
    def addCell(): Unit =
      if allocation!=null then allocation.retain(96L,"schema5 UNKNOWN typed result cell graph BEFORE allocation")
      if profile.nonEmpty then addBytes(8, "cell framing")
      cells += 1
      if cells > MaxTotalCells then reject("MySQL audit exceeded the total cell bound")

  private[mysql] def jdbcValueKind(jdbcType: Int, context: String = "MySQL audit resultset"): MySqlAuditJdbcValueKind =
    if jdbcType == Types.TIMESTAMP then MySqlAuditJdbcValueKind.Timestamp
    else if BinaryTypes.contains(jdbcType) then MySqlAuditJdbcValueKind.Binary
    else if NumericTypes.contains(jdbcType) then MySqlAuditJdbcValueKind.Numeric
    else if jdbcType == Types.NULL then MySqlAuditJdbcValueKind.SqlNull
    else if CharacterTypes.contains(jdbcType) then MySqlAuditJdbcValueKind.Text
    else reject(s"$context uses unsupported JDBC type $jdbcType")

  /** JDBC Types.NULL 只接受 getObject=null 且 wasNull=true 的一致结果。 */
  private[mysql] def decodeJdbcNullCell(value: Any, wasNull: Boolean, context: String): MySqlAuditCell =
    if value == null && wasNull then MySqlAuditNull
    else if value == null then reject(s"MySQL audit $context returned null without JDBC confirming SQL NULL")
    else if wasNull then reject(s"MySQL audit $context returned an object while JDBC reported SQL NULL")
    else reject(s"MySQL audit $context returned a non-NULL object for JDBC Types.NULL")

  /** 数值 JDBC 值保留驱动返回的文本，避免经 Float 或 Double 损失十进制精度。 */
  private[mysql] def decodeNumericCell(
      value: String,
      wasNull: Boolean,
      budget: ReadBudget,
      context: String
  ): MySqlAuditCell =
    if budget == null then reject(s"MySQL audit $context has no decoded-byte budget")
    if value == null then
      if wasNull then MySqlAuditNull
      else reject(s"MySQL audit $context returned a null numeric string for a non-NULL value")
    else
      if wasNull then reject(s"MySQL audit $context returned text while JDBC reported SQL NULL")
      val byteCount = boundedUtf8ByteLength(value, MaxCellBytes, context)
      budget.addBytes(byteCount, context)
      MySqlAuditText(value)

  private[mysql] def boundedUtf8ByteLength(value: String, maxBytes: Int, context: String): Int =
    if value == null then reject(s"MySQL audit $context text is unavailable")
    if maxBytes <= 0 then reject(s"MySQL audit $context has an invalid UTF-8 bound")
    var bytes = 0
    var index = 0
    while index < value.length do
      val current = value.charAt(index)
      if current <= 0x7f then bytes += 1
      else if current <= 0x7ff then bytes += 2
      else if Character.isHighSurrogate(current) then
        if index + 1 >= value.length || !Character.isLowSurrogate(value.charAt(index + 1)) then
          reject(s"MySQL audit $context text contains an unpaired UTF-16 surrogate")
        bytes += 4
        index += 1
      else if Character.isLowSurrogate(current) then
        reject(s"MySQL audit $context text contains an unpaired UTF-16 surrogate")
      else bytes += 3
      if bytes > maxBytes then reject(s"MySQL audit $context exceeds the UTF-8 byte bound")
      index += 1
    bytes

  private[mysql] def readOwnedNumericColumn(result:ResultSet,index:Int,context:String,
      allocation:AuditAllocation,budget:ReadBudget):MySqlAuditCell =
    if result==null || allocation==null || budget==null then reject("owned numeric reader requires JDBC result and original allocation/budget")
    // MySQL DECIMAL最多65位；128字符同时覆盖符号、小数点和浮点指数。数值协议不支持字符流。
    val maximumCharacters=128
    allocation.retain(256L+Math.multiplyExact(32L,maximumCharacters.toLong),
      s"owned numeric $context bounded string BEFORE JDBC allocation")
    val value=jdbcRead(context,"getString numeric")(result.getString(index))
    val wasNull=jdbcRead(context,"wasNull numeric")(result.wasNull())
    if value!=null then
      if value.length>maximumCharacters then reject(s"MySQL audit $context numeric text exceeds the pinned MySQL representation bound")
      allocation.retainScratch(value,s"owned numeric $context retained string")
    decodeNumericCell(value,wasNull,budget,context)

  private[mysql] def readOwnedTextColumn(result:ResultSet,index:Int,context:String,
      allocation:AuditAllocation):String =
    readOwnedNullableTextColumn(result,index,context,allocation).getOrElse(
      reject(s"MySQL audit $context returned SQL NULL where text is required"))

  private[mysql] def readOwnedNullableTextColumn(result:ResultSet,index:Int,context:String,
      allocation:AuditAllocation):Option[String] =
    if result==null || allocation==null then reject("schema5 UNKNOWN text reader requires an actual JDBC result and allocation owner")
    allocation.retain(256L,s"schema5 UNKNOWN $context JDBC character reader owner BEFORE acquisition")
    val reader=jdbcRead(context,"getCharacterStream")(result.getCharacterStream(index))
    if reader==null then
      if !jdbcRead(context,"wasNull")(result.wasNull()) then reject(s"MySQL audit $context returned no character stream for a non-NULL value")
      None
    else
      val value=withResource(reader)(stream=>readBoundedTextOwned(stream,allocation,context))
      allocation.retain(96L,s"schema5 UNKNOWN $context present text option BEFORE allocation")
      Some(value)

  private[mysql] def readBoundedTextOwned(reader:Reader,allocation:AuditAllocation,
      context:String):String =
    if reader==null || allocation==null then reject("schema5 UNKNOWN text stream and allocation owner are required")
    val initial=256
    // builder/buffer 只活到本次解码返回；不能作为每个目录cell的常驻内容挤占后续验证工作区。
    allocation.withTemporary(256L+Math.multiplyExact(64L,initial.toLong),
        s"schema5 UNKNOWN $context text builder and reader buffer") {
      val output=new java.lang.StringBuilder(initial)
      val buffer=new Array[Char](initial)
      def remaining():String =
        var read=jdbcRead(context,"character stream read")(reader.read(buffer))
        while read>=0 do
          if read==0 then reject("MySQL audit text stream made no read progress")
          val required=Math.addExact(output.length(),read)
          if required.toLong>MaxCellBytes then reject("MySQL audit text cell exceeds the character bound")
          val oldCapacity=output.capacity()
          if required>oldCapacity then
            val doubled=Math.addExact(Math.multiplyExact(oldCapacity,2),2)
            val target=Math.min(MaxCellBytes,Math.max(required,doubled))
            // 仅在几何增长时嵌套临时scope，深度由固定cell上限约束；异常释放保留原cause/suppressed。
            return allocation.withTemporary(Math.multiplyExact(32L,target.toLong),
                s"schema5 UNKNOWN $context StringBuilder growth") {
              output.ensureCapacity(target)
              output.append(buffer,0,read)
              remaining()
            }
          output.append(buffer,0,read)
          read=jdbcRead(context,"character stream read")(reader.read(buffer))
        val length=output.length()
        allocation.retain(Math.addExact(96L,Math.multiplyExact(2L,length.toLong)),
          s"schema5 UNKNOWN $context retained String BEFORE allocation")
        val value=output.toString
        boundedUtf8ByteLength(value,MaxCellBytes,context)
        allocation.retainScratch(value,s"schema5 UNKNOWN retained $context text")
        value
      remaining()
    }

  private[mysql] def readBoundedBytesOwned(stream:InputStream,allocation:AuditAllocation,
      context:String):Array[Byte] =
    if stream==null || allocation==null then reject("schema5 UNKNOWN binary stream and allocation owner are required")
    val initial=8192
    allocation.withTemporary(256L+Math.multiplyExact(64L,initial.toLong),
        s"schema5 UNKNOWN $context binary accumulation and read buffers") {
      var capacity=initial
      var bytes=new Array[Byte](capacity)
      val chunk=new Array[Byte](initial)
      var size=0
      def remaining():Array[Byte] =
        var read=jdbcRead(context,"binary stream read")(stream.read(chunk))
        while read>=0 do
          if read==0 then reject("MySQL audit binary stream made no read progress")
          val required=Math.addExact(size,read)
          if required>MaxCellBytes then reject("MySQL audit binary cell exceeds the byte bound")
          if required>capacity then
            val doubled=Math.multiplyExact(capacity,2)
            val target=Math.min(MaxCellBytes,Math.max(required,doubled))
            return allocation.withTemporary(Math.multiplyExact(32L,target.toLong),
                s"schema5 UNKNOWN $context binary buffer growth") {
              bytes=Arrays.copyOf(bytes,target)
              capacity=target
              System.arraycopy(chunk,0,bytes,size,read)
              size=required
              remaining()
            }
          System.arraycopy(chunk,0,bytes,size,read)
          size=required
          read=jdbcRead(context,"binary stream read")(stream.read(chunk))
        allocation.retain(Math.multiplyExact(32L,size.toLong),
          s"schema5 UNKNOWN $context exact binary value BEFORE allocation")
        val exact=new Array[Byte](size)
        System.arraycopy(bytes,0,exact,0,size)
        allocation.retainScratch(exact,s"schema5 UNKNOWN retained $context binary value")
        exact
      remaining()
    }

  private[mysql] def decodeCallerIdentityOwned(result:ResultSet,profile:MySqlSchemaAuditProfile,
      allocation:AuditAllocation):MySqlCallerScope =
    if result==null || profile==null || allocation==null then reject("schema5 UNKNOWN caller identity result/profile/account are required")
    val expected=if profile.extendedAudit then CallerLabelsFormat3 else CallerLabels
    allocation.retain(Math.addExact(512L,Math.multiplyExact(96L,expected.size.toLong)),
      "schema5 UNKNOWN caller identity metadata and label vector BEFORE allocation")
    val metadata=jdbcRead("caller identity","getMetaData")(result.getMetaData)
    if metadata==null || metadata.getColumnCount!=expected.size then reject("caller identity column count differs")
    val labels=Vector.tabulate(expected.size)(i=>metadata.getColumnLabel(i+1))
    allocation.retainScratch(labels,"schema5 UNKNOWN caller identity actual labels")
    if labels!=expected then reject("caller identity column labels differ")
    if !jdbcRead("caller identity","next")(result.next()) then reject("MySQL caller scope query returned no row")
    def text(index:Int,label:String):String =
      val value=readOwnedTextColumn(result,index,s"caller identity $label",allocation)
      if value.isEmpty then reject(s"caller identity $label is unavailable")
      boundedUtf8ByteLength(value,MaxCellBytes,"caller identity")
      value
    allocation.retain(512L,"schema5 UNKNOWN caller identity strings and scope holder BEFORE allocation")
    val server=uuidTextCell(text(1,"server UUID"),"caller server UUID")
    val connectionId=jdbcRead("caller identity","getLong[2]")(result.getLong(2))
    if jdbcRead("caller identity","wasNull[2]")(result.wasNull()) || connectionId<=0L then
      reject("caller identity connection ID is unavailable")
    val schema=text(3,"schema")
    val principal=text(4,"effective principal")
    val roles=text(5,"roles")
    val user=text(6,"connected user")
    val version=if profile.extendedAudit then Some(text(7,"server version")) else None
    if jdbcRead("caller identity","next uniqueness")(result.next()) then
      reject("MySQL caller scope query returned more than one row")
    new MySqlCallerScope(server,connectionId,schema,principal,roles,user,version,Vector.empty)

  private[mysql] def validateStaticSetShape(resultSet: MySqlAuditResultSet): Unit =
    validateStaticSetShape(resultSet, MySqlSchemaAuditProfile.Format1)

  private[mysql] def validateStaticSetShape(resultSet: MySqlAuditResultSet, profile: MySqlSchemaAuditProfile): Unit =
    if profile == null then reject("MySQL static resultset schema profile is required")
    if resultSet == null || resultSet.columns == null || resultSet.rows == null then
      reject("approved or observed MySQL static resultset is incomplete")
    if resultSet.columns.exists(_ == null) then reject("MySQL static resultset contains a missing column description")
    val index = MySqlAuditCoverage.StaticResultSetIds.indexOf(resultSet.id)
    if index < 0 || resultSet.columns.map(_.label) != StaticLabels(index) then
      reject(s"approved or observed MySQL static resultset does not match the fixed format ${profile.schemaFormat} column contract")
    if resultSet.rows.size > MaxRowsPerResultSet then reject("MySQL static resultset exceeds the row bound")
    if resultSet.columns.size > MaxColumnsPerResultSet then reject("MySQL static resultset exceeds the column bound")
    resultSet.rows.foreach { row =>
      if row == null || row.cells == null then reject("MySQL audit row is incomplete")
      if row.cells.size != resultSet.columns.size then reject("MySQL audit row does not match its column count")
      row.cells.foreach(validateCell)
    }

  private[mysql] def validateSelfDescriptions(resultSets: Vector[MySqlAuditResultSet], requireTriggerRows: Boolean): Unit =
    validateSelfDescriptions(resultSets, requireTriggerRows, MySqlSchemaAuditProfile.Format1)

  private[mysql] def validateSelfDescriptions(
      resultSets: Vector[MySqlAuditResultSet],
      requireTriggerRows: Boolean,
      profile: MySqlSchemaAuditProfile
  ): Unit =
    if profile == null then reject("MySQL object manifest schema profile is required")
    if resultSets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then reject("MySQL object manifest has incomplete ordered static-object scope")
    val triggers = resultSets(7)
    if requireTriggerRows && triggers.rows.isEmpty then reject("approved MySQL trigger manifest is empty")
    triggers.rows.foreach { row =>
      if row.cells.contains(MySqlAuditNull) then reject("MySQL trigger metadata contains a null field")
      val statement = textAt(triggers, row, "ACTION_STATEMENT")
      if statement.isBlank then reject("MySQL trigger definition body is empty")
      val actual = encodeUtf8(statement)
      val octets = longAt(triggers, row, "statement_octets")
      val hash = binaryAt(triggers, row, "statement_sha256")
      if octets != actual.length.toLong || !Arrays.equals(hash, sha256(actual)) then
        reject("MySQL trigger definition byte length or SHA-256 is inconsistent")
    }
    val self = resultSets(8)
    if self.rows.size != 1 then reject("required MySQL auditself must be exactly one result row")
    val selfRow = self.rows.head
    if textAt(self, selfRow, "ROUTINE_SCHEMA").isEmpty || textAt(self, selfRow, "ROUTINE_NAME") != profile.requiredProcedure ||
      textAt(self, selfRow, "ROUTINE_TYPE") != "PROCEDURE" then reject("required MySQL auditself identity is malformed")
    val definition = textAt(self, selfRow, "ROUTINE_DEFINITION")
    if definition.isBlank then reject("required MySQL auditself definition body is empty")
    if textAt(self, selfRow, "SECURITY_TYPE") != "DEFINER" || textAt(self, selfRow, "SQL_DATA_ACCESS") != "READS SQL DATA" ||
      textAt(self, selfRow, "DEFINER").isEmpty || textAt(self, selfRow, "SQL_MODE").isEmpty then
      reject("required MySQL auditself security, SQL mode, or definition is unavailable")
    val definitionBytes = encodeUtf8(definition)
    if longAt(self, selfRow, "definition_octets") != definitionBytes.length.toLong ||
      !Arrays.equals(binaryAt(self, selfRow, "definition_sha256"), sha256(definitionBytes)) then
      reject("required MySQL auditself definition byte length or SHA-256 is inconsistent")
    if resultSets(9).rows.nonEmpty then reject("required MySQL auditself must have zero parameters")

  private[mysql] def validateProfileObjectCounts(profile: MySqlSchemaAuditProfile, resultSets: Vector[MySqlAuditResultSet]): Unit =
    if profile == null || resultSets == null || resultSets.size != MySqlAuditCoverage.StaticResultSetIds.size then
      reject("MySQL object manifest profile or static resultset collection is incomplete")
    profile match
      case MySqlSchemaAuditProfile.Format1 => ()
      case MySqlSchemaAuditProfile.Format2 =>
        val expectedTables = profile.expectedTableCount.getOrElse(reject("format 2 profile is missing its table-count contract"))
        val expectedTriggers = profile.expectedTriggerCount.getOrElse(reject("format 2 profile is missing its trigger-count contract"))
        val tableSet = resultSets(0)
        val triggerSet = resultSets(7)
        val tableNames = tableSet.rows.map(row => textAt(tableSet, row, "TABLE_NAME")).toSet
        val triggerNames = triggerSet.rows.map(row => textAt(triggerSet, row, "TRIGGER_NAME")).toSet
        if tableSet.rows.size != expectedTables || !tableNames.contains("binding_bootstrap_intent") then
          reject("format 2 audit profile requires the exact nine-table baseline including binding_bootstrap_intent")
        if triggerSet.rows.size != expectedTriggers ||
          !triggerNames.contains("trg_binding_bootstrap_intent_insert") ||
          !triggerNames.contains("trg_binding_bootstrap_intent_update") then
          reject("format 2 audit profile requires the exact eighteen-trigger baseline including both B1 guards")

      case MySqlSchemaAuditProfile.ScriptPublication1 =>
        val tables = resultSets(0); val columns = resultSets(1); val triggers = resultSets(7)
        val names = tables.rows.map(row => textAt(tables, row, "TABLE_NAME"))
        if names.size != 24 || names.toSet != MySqlScriptSchemaProfile.tables.map(_.name).toSet then
          reject("ScriptPublication1 requires exact24 tables")
        val actualColumns = columns.rows.map(row => (textAt(columns, row, "TABLE_NAME"), textAt(columns, row, "COLUMN_NAME")))
        val expectedColumns = MySqlScriptSchemaProfile.tables.flatMap(t => t.columns.map(c => (t.name, c.name))).toSet
        if actualColumns.size != 432 || actualColumns.toSet != expectedColumns then reject("ScriptPublication1 requires exact432 columns")
        val namesOfGuards = triggers.rows.map(row => textAt(triggers, row, "TRIGGER_NAME"))
        if namesOfGuards.size != 72 || namesOfGuards.toSet != MySqlScriptSchemaProfile.guards.map(_.name).toSet then
          reject("ScriptPublication1 requires exact72 guards")
      case MySqlSchemaAuditProfile.DbFirst1 =>
        val tables = resultSets(0); val columns = resultSets(1); val triggers = resultSets(7)
        val names = tables.rows.map(row => textAt(tables, row, "TABLE_NAME"))
        if names.size != 27 || names.toSet != MySqlDbFirstSchemaProfile.TableNames.toSet then
          reject("DbFirst1 requires the exact27 table set including fp_pod_snapshot")
        val actualColumns = columns.rows.map(row => (textAt(columns, row, "TABLE_NAME"), textAt(columns, row, "COLUMN_NAME")))
        val expectedColumns = MySqlDbFirstRelationShapes.canonicalOrder.flatMap(t => t.columns.map(c => (t.table, c.name)))
        if actualColumns.size != 480 || actualColumns.distinct.size != 480 || actualColumns.toSet != expectedColumns.toSet then
          reject("DbFirst1 requires exact480 table/column identities")
        val namesOfGuards = triggers.rows.map(row => textAt(triggers, row, "TRIGGER_NAME"))
        if namesOfGuards.size != 81 || namesOfGuards.toSet != MySqlDbFirstSchemaProfile.GuardNames.toSet then
          reject("DbFirst1 requires exact81 guard identities")
      case MySqlSchemaAuditProfile.DeploymentReceipt1 =>
        val tables = resultSets(0); val columns = resultSets(1); val triggers = resultSets(7)
        val names = tables.rows.map(row => textAt(tables, row, "TABLE_NAME"))
        if names.size != 26 || names.toSet != MySqlDeploymentReceiptSchemaProfile.TableNames.toSet then
          reject("DeploymentReceipt1 requires the exact26 table set")
        val actualColumns = columns.rows.map(row => (textAt(columns, row, "TABLE_NAME"), textAt(columns, row, "COLUMN_NAME")))
        val expectedColumns = MySqlScriptSchemaProfile.tables.flatMap(t => t.columns.map(c => (t.name, c.name))) ++
          MySqlDeploymentReceiptSchemaDefinition.operationColumns.map(c => (MySqlDeploymentReceiptSchemaDefinition.OperationTable, c.name)) ++
          MySqlDeploymentReceiptSchemaDefinition.quotaColumns.map(c => (MySqlDeploymentReceiptSchemaDefinition.QuotaTable, c.name))
        if actualColumns.size != 467 || actualColumns.distinct.size != 467 || actualColumns.toSet != expectedColumns.toSet then
          reject("DeploymentReceipt1 requires exact467 table/column identities")
        val guardNames = triggers.rows.map(row => textAt(triggers, row, "TRIGGER_NAME"))
        if guardNames.size != 78 || guardNames.toSet != MySqlDeploymentReceiptSchemaProfile.GuardNames.toSet then
          reject("DeploymentReceipt1 requires exact78 guard identities")
      case MySqlSchemaAuditProfile.Format3 =>
        val tableSet = resultSets(0); val columns = resultSets(1); val triggers = resultSets(7)
        val names = tableSet.rows.map(row => textAt(tableSet, row, "TABLE_NAME"))
        if names.size != 20 || names.toSet != Format3TableNames then reject("format3 requires the exact twenty-table set")
        if columns.rows.size != 368 then reject("format3 requires all368 column definitions")
        val actualColumns = columns.rows.map(row => (textAt(columns, row, "TABLE_NAME"), textAt(columns, row, "COLUMN_NAME")))
        if actualColumns.distinct.size != actualColumns.size || actualColumns.toSet != Format3ColumnNames then reject("format3 requires the exact368 table/column identities")
        val triggerNames = triggers.rows.map(row => textAt(triggers, row, "TRIGGER_NAME"))
        if triggerNames.size != 60 || triggerNames.toSet != Format3TriggerNames then reject("format3 requires exact all60 guard identities")

  private val Format3TriggerNames = Set("fp_f3_01_bi", "fp_f3_01_bu", "fp_f3_01_bd", "fp_f3_02_bi", "fp_f3_02_bu", "fp_f3_02_bd", "fp_f3_03_bi", "fp_f3_03_bu", "fp_f3_03_bd", "fp_f3_04_bi", "fp_f3_04_bu", "fp_f3_04_bd", "fp_f3_05_bi", "fp_f3_05_bu", "fp_f3_05_bd", "fp_f3_06_bi", "fp_f3_06_bu", "fp_f3_06_bd", "fp_f3_07_bi", "fp_f3_07_bu", "fp_f3_07_bd", "fp_f3_08_bi", "fp_f3_08_bu", "fp_f3_08_bd", "fp_f3_09_bi", "fp_f3_09_bu", "fp_f3_09_bd", "fp_f3_10_bi", "fp_f3_10_bu", "fp_f3_10_bd", "fp_f3_11_bi", "fp_f3_11_bu", "fp_f3_11_bd", "fp_f3_12_bi", "fp_f3_12_bu", "fp_f3_12_bd", "fp_f3_13_bi", "fp_f3_13_bu", "fp_f3_13_bd", "fp_f3_14_bi", "fp_f3_14_bu", "fp_f3_14_bd", "fp_f3_15_bi", "fp_f3_15_bu", "fp_f3_15_bd", "fp_f3_16_bi", "fp_f3_16_bu", "fp_f3_16_bd", "fp_f3_17_bi", "fp_f3_17_bu", "fp_f3_17_bd", "fp_f3_18_bi", "fp_f3_18_bu", "fp_f3_18_bd", "fp_f3_19_bi", "fp_f3_19_bu", "fp_f3_19_bd", "fp_f3_20_bi", "fp_f3_20_bu", "fp_f3_20_bd")
  private val Format3ColumnNames = Set(("fp_schema_manifest", "singleton_fixed1"), ("fp_schema_manifest", "backend"), ("fp_schema_manifest", "format_version"), ("fp_schema_manifest", "ddl_sha256"), ("fp_schema_manifest", "provisioning_uuid"), ("fp_schema_manifest", "bootstrap_state"), ("fp_schema_manifest", "expected_server_uuid"), ("fp_schema_manifest", "expected_binding_uuid"), ("fp_schema_manifest", "import_manifest_sha256"), ("fp_schema_manifest", "import_state"), ("fp_schema_manifest", "import_operation_uuid"), ("fp_schema_manifest", "import_snapshot_sha256"), ("fp_schema_manifest", "import_receipt_sha256"), ("fp_schema_manifest", "guard_manifest_sha256"), ("fp_schema_manifest", "configuration_sha256"), ("fp_schema_manifest", "source_provenance_kind"), ("fp_schema_manifest", "source_provenance_bytes"), ("fp_schema_manifest", "source_provenance_sha256"), ("fp_schema_manifest", "limit_profile_encoding"), ("fp_schema_manifest", "limit_profile_bytes"), ("fp_schema_manifest", "limit_profile_sha256"), ("fp_schema_manifest", "max_region_bytes"), ("fp_schema_manifest", "max_chunk_bytes"), ("fp_schema_manifest", "max_decompressed_bytes"), ("fp_schema_manifest", "max_nbt_depth"), ("fp_schema_manifest", "max_members"), ("fp_schema_manifest", "max_retained_bytes"), ("fp_schema_manifest", "max_sql_packet_bytes"), ("fp_schema_manifest", "max_native_artifact_bytes"), ("fp_runtime_writer", "singleton_fixed1"), ("fp_runtime_writer", "binding_uuid"), ("fp_runtime_writer", "writer_epoch"), ("fp_runtime_writer", "owner_boot_uuid"), ("fp_runtime_writer", "lease_until"), ("fp_runtime_writer", "last_commit_operation_uuid"), ("fp_runtime_writer", "last_commit_fingerprint"), ("physical_recovery_binding", "singleton_fixed1"), ("physical_recovery_binding", "binding_uuid"), ("physical_recovery_binding", "cutover_state"), ("physical_recovery_binding", "format_version"), ("fp_recovery_operation", "binding_uuid"), ("fp_recovery_operation", "operation_uuid"), ("fp_recovery_operation", "workflow_uuid"), ("fp_recovery_operation", "step"), ("fp_recovery_operation", "operation_state"), ("fp_recovery_operation", "pod_uuid"), ("fp_recovery_operation", "attempt_uuid"), ("fp_recovery_operation", "footprint_uuid"), ("fp_recovery_operation", "source_unit_uuid"), ("fp_recovery_operation", "forest_uuid"), ("fp_recovery_operation", "boot_uuid"), ("fp_recovery_operation", "boot_generation"), ("fp_recovery_operation", "connection_generation_uuid"), ("fp_recovery_operation", "connection_id"), ("fp_recovery_operation", "fence_epoch"), ("fp_recovery_operation", "request_identity_bytes"), ("fp_recovery_operation", "request_identity_sha256"), ("fp_recovery_operation", "expected_domain_version"), ("fp_recovery_operation", "expected_locator_revision"), ("fp_recovery_operation", "expected_source_revision"), ("fp_recovery_operation", "context_encoding"), ("fp_recovery_operation", "original_context_bytes"), ("fp_recovery_operation", "original_context_sha256"), ("fp_recovery_operation", "scope_bytes"), ("fp_recovery_operation", "scope_sha256"), ("fp_recovery_operation", "preimage_bytes"), ("fp_recovery_operation", "preimage_sha256"), ("fp_recovery_operation", "target_bytes"), ("fp_recovery_operation", "target_sha256"), ("fp_recovery_operation", "reserve_operation_uuid"), ("fp_recovery_operation", "previous_step_operation_uuid"), ("fp_recovery_operation", "settled_by_operation_uuid"), ("fp_recovery_operation", "commit_witness_bytes"), ("fp_recovery_operation", "commit_witness_sha256"), ("fp_recovery_operation", "original_diagnostic_sha256"), ("fp_recovery_operation", "original_diagnostic_identity"), ("fp_recovery_operation", "planned_at"), ("fp_recovery_operation", "fixed_lease_until"), ("fp_recovery_operation", "result_at"), ("fp_recovery_operation", "operation_fingerprint"), ("floating_pod_state", "uuid"), ("floating_pod_state", "owner_uuid"), ("floating_pod_state", "state_version"), ("floating_pod_state", "world_name"), ("floating_pod_state", "pos_x"), ("floating_pod_state", "pos_y"), ("floating_pod_state", "pos_z"), ("floating_pod_state", "yaw"), ("floating_pod_state", "pitch"), ("floating_pod_state", "is_active"), ("floating_pod_state", "last_updated"), ("floating_pod_state", "dimension_id"), ("floating_pod_state", "operating_mode"), ("floating_pod_state", "binding_uuid"), ("floating_pod_state", "last_mutation_operation_uuid"), ("player_entitlement", "player_uuid"), ("player_entitlement", "registration_limit"), ("player_entitlement", "simultaneous_active_limit"), ("player_entitlement", "binding_uuid"), ("player_entitlement", "created_operation_uuid"), ("binding_bootstrap_intent", "singleton_fixed1"), ("binding_bootstrap_intent", "bootstrap_operation_uuid"), ("binding_bootstrap_intent", "prepare_write_uuid"), ("binding_bootstrap_intent", "certify_write_uuid"), ("binding_bootstrap_intent", "binding_uuid"), ("binding_bootstrap_intent", "binding_format_version"), ("binding_bootstrap_intent", "provisioning_uuid"), ("binding_bootstrap_intent", "source_world_identity"), ("binding_bootstrap_intent", "provision_receipt_sha256"), ("binding_bootstrap_intent", "stage"), ("binding_bootstrap_intent", "marker_ack_sha256"), ("fp_recovery_reservation", "binding_uuid"), ("fp_recovery_reservation", "resource_kind"), ("fp_recovery_reservation", "resource_key_hash"), ("fp_recovery_reservation", "resource_key"), ("fp_recovery_reservation", "reservation_revision"), ("fp_recovery_reservation", "workflow_uuid"), ("fp_recovery_reservation", "operation_uuid"), ("fp_recovery_reservation", "reservation_state"), ("fp_recovery_reservation", "last_mutation_operation_uuid"), ("fp_recovery_reservation", "resource_pod_uuid"), ("fp_recovery_reservation", "resource_unit_uuid"), ("physical_recovery_footprint", "binding_uuid"), ("physical_recovery_footprint", "pod_uuid"), ("physical_recovery_footprint", "attempt_uuid"), ("physical_recovery_footprint", "footprint_uuid"), ("physical_recovery_footprint", "origin_version"), ("physical_recovery_footprint", "target_world_name"), ("physical_recovery_footprint", "target_pos_x"), ("physical_recovery_footprint", "target_pos_y"), ("physical_recovery_footprint", "target_pos_z"), ("physical_recovery_footprint", "target_yaw"), ("physical_recovery_footprint", "target_pitch"), ("physical_recovery_footprint", "target_dimension_id"), ("physical_recovery_footprint", "center_chunk_x"), ("physical_recovery_footprint", "center_chunk_z"), ("physical_recovery_footprint", "purpose"), ("physical_recovery_footprint", "seal_state"), ("physical_recovery_footprint", "created_operation_uuid"), ("physical_recovery_footprint_chunk", "binding_uuid"), ("physical_recovery_footprint_chunk", "pod_uuid"), ("physical_recovery_footprint_chunk", "attempt_uuid"), ("physical_recovery_footprint_chunk", "footprint_uuid"), ("physical_recovery_footprint_chunk", "dimension_id"), ("physical_recovery_footprint_chunk", "chunk_x"), ("physical_recovery_footprint_chunk", "chunk_z"), ("physical_recovery_intent", "binding_uuid"), ("physical_recovery_intent", "pod_uuid"), ("physical_recovery_intent", "attempt_uuid"), ("physical_recovery_intent", "origin"), ("physical_recovery_intent", "phase"), ("physical_recovery_intent", "target_footprint_uuid"), ("physical_recovery_intent", "prepare_operation_uuid"), ("physical_recovery_intent", "expected_owner_uuid"), ("physical_recovery_intent", "expected_domain_version"), ("physical_recovery_intent", "predecessor_attempt_uuid"), ("physical_recovery_intent", "predecessor_footprint_uuid"), ("physical_recovery_intent", "source_completion_uuid"), ("physical_recovery_intent", "fresh_absence_proof_bytes"), ("physical_recovery_intent", "fresh_absence_proof_sha256"), ("physical_recovery_intent", "source_kind"), ("physical_recovery_intent", "source_identity"), ("physical_recovery_intent", "source_identity_sha256"), ("physical_recovery_intent", "catalog_generation"), ("physical_recovery_intent", "create_operation_uuid"), ("physical_recovery_intent", "source_unit_uuid"), ("physical_recovery_intent", "source_forest_uuid"), ("physical_recovery_intent", "activation_operation_uuid"), ("physical_recovery_intent", "committed_domain_version"), ("physical_recovery_intent", "committed_at"), ("physical_recovery_intent", "committed_world_name"), ("physical_recovery_intent", "committed_pos_x"), ("physical_recovery_intent", "committed_pos_y"), ("physical_recovery_intent", "committed_pos_z"), ("physical_recovery_intent", "committed_yaw"), ("physical_recovery_intent", "committed_pitch"), ("physical_recovery_intent", "committed_dimension_id"), ("physical_recovery_intent", "cleanup_operation_uuid"), ("physical_recovery_intent", "cleanup_proof_bytes"), ("physical_recovery_intent", "cleanup_proof_sha256"), ("physical_recovery_intent", "intent_revision"), ("physical_recovery_intent", "last_mutation_operation_uuid"), ("physical_recovery_locator", "binding_uuid"), ("physical_recovery_locator", "pod_uuid"), ("physical_recovery_locator", "current_attempt_uuid"), ("physical_recovery_locator", "current_footprint_uuid"), ("physical_recovery_locator", "locator_state"), ("physical_recovery_locator", "reason_code"), ("physical_recovery_locator", "domain_version"), ("physical_recovery_locator", "target_dimension_id"), ("physical_recovery_locator", "locator_revision"), ("physical_recovery_locator", "last_mutation_operation_uuid"), ("physical_recovery_obligation", "binding_uuid"), ("physical_recovery_obligation", "pod_uuid"), ("physical_recovery_obligation", "attempt_uuid"), ("physical_recovery_obligation", "footprint_uuid"), ("physical_recovery_obligation", "origin_version"), ("physical_recovery_obligation", "purpose"), ("physical_recovery_obligation", "created_operation_uuid"), ("physical_recovery_obligation", "attempt_retirement_operation_uuid"), ("physical_recovery_obligation", "obligation_state"), ("physical_recovery_obligation", "obligation_revision"), ("physical_recovery_obligation", "verified_operation_uuid"), ("physical_recovery_obligation", "verification_proof_bytes"), ("physical_recovery_obligation", "verification_proof_sha256"), ("physical_recovery_obligation", "last_mutation_operation_uuid"), ("fp_lifecycle_completion", "binding_uuid"), ("fp_lifecycle_completion", "receipt_uuid"), ("fp_lifecycle_completion", "completion_operation_uuid"), ("fp_lifecycle_completion", "completion_kind"), ("fp_lifecycle_completion", "boot_uuid"), ("fp_lifecycle_completion", "boot_generation"), ("fp_lifecycle_completion", "world_identity"), ("fp_lifecycle_completion", "source_identity"), ("fp_lifecycle_completion", "pod_uuid"), ("fp_lifecycle_completion", "owner_uuid"), ("fp_lifecycle_completion", "attempt_uuid"), ("fp_lifecycle_completion", "footprint_uuid"), ("fp_lifecycle_completion", "before_domain_version"), ("fp_lifecycle_completion", "after_domain_version"), ("fp_lifecycle_completion", "removal_result_bytes"), ("fp_lifecycle_completion", "removal_result_sha256"), ("fp_lifecycle_completion", "ticket_result_bytes"), ("fp_lifecycle_completion", "ticket_result_sha256"), ("fp_lifecycle_completion", "checkpoint_bytes"), ("fp_lifecycle_completion", "checkpoint_sha256"), ("fp_lifecycle_completion", "original_context_bytes"), ("fp_lifecycle_completion", "original_context_sha256"), ("fp_lifecycle_completion", "allowed_use"), ("fp_lifecycle_completion", "completion_state"), ("fp_lifecycle_completion", "consumed_by_operation_uuid"), ("fp_lifecycle_completion", "completion_revision"), ("fp_lifecycle_completion", "last_mutation_operation_uuid"), ("physical_footprint_transfer", "binding_uuid"), ("physical_footprint_transfer", "move_uuid"), ("physical_footprint_transfer", "pod_uuid"), ("physical_footprint_transfer", "attempt_uuid"), ("physical_footprint_transfer", "from_footprint_uuid"), ("physical_footprint_transfer", "to_footprint_uuid"), ("physical_footprint_transfer", "expected_domain_version"), ("physical_footprint_transfer", "target_domain_version"), ("physical_footprint_transfer", "from_world_name"), ("physical_footprint_transfer", "from_pos_x"), ("physical_footprint_transfer", "from_pos_y"), ("physical_footprint_transfer", "from_pos_z"), ("physical_footprint_transfer", "from_yaw"), ("physical_footprint_transfer", "from_pitch"), ("physical_footprint_transfer", "from_dimension_id"), ("physical_footprint_transfer", "to_world_name"), ("physical_footprint_transfer", "to_pos_x"), ("physical_footprint_transfer", "to_pos_y"), ("physical_footprint_transfer", "to_pos_z"), ("physical_footprint_transfer", "to_yaw"), ("physical_footprint_transfer", "to_pitch"), ("physical_footprint_transfer", "to_dimension_id"), ("physical_footprint_transfer", "prepare_operation_uuid"), ("physical_footprint_transfer", "commit_operation_uuid"), ("physical_footprint_transfer", "cleanup_operation_uuid"), ("physical_footprint_transfer", "phase"), ("physical_footprint_transfer", "move_plan_bytes"), ("physical_footprint_transfer", "move_plan_sha256"), ("physical_footprint_transfer", "cleanup_proof_bytes"), ("physical_footprint_transfer", "cleanup_proof_sha256"), ("physical_footprint_transfer", "transfer_revision"), ("physical_footprint_transfer", "last_mutation_operation_uuid"), ("physical_source_unit", "binding_uuid"), ("physical_source_unit", "unit_uuid"), ("physical_source_unit", "world_identity"), ("physical_source_unit", "save_identity"), ("physical_source_unit", "loader_identity"), ("physical_source_unit", "catalog_generation"), ("physical_source_unit", "read_generation"), ("physical_source_unit", "source_observation_bytes"), ("physical_source_unit", "source_observation_sha256"), ("physical_source_unit", "current_epoch"), ("physical_source_unit", "current_forest_uuid"), ("physical_source_unit", "unit_state"), ("physical_source_unit", "unit_revision"), ("physical_source_unit", "created_operation_uuid"), ("physical_source_unit", "last_mutation_operation_uuid"), ("physical_source_forest", "binding_uuid"), ("physical_source_forest", "unit_uuid"), ("physical_source_forest", "forest_uuid"), ("physical_source_forest", "source_epoch"), ("physical_source_forest", "forest_state"), ("physical_source_forest", "forest_codec"), ("physical_source_forest", "forest_bytes"), ("physical_source_forest", "forest_sha256"), ("physical_source_forest", "member_count"), ("physical_source_forest", "root_count"), ("physical_source_forest", "carrier_count"), ("physical_source_forest", "retained_bytes"), ("physical_source_forest", "created_operation_uuid"), ("physical_source_forest", "fp_pod_uuid_index_bytes"), ("physical_source_forest", "fp_pod_uuid_index_sha256"), ("physical_source_transfer", "binding_uuid"), ("physical_source_transfer", "transfer_uuid"), ("physical_source_transfer", "unit_uuid"), ("physical_source_transfer", "old_epoch"), ("physical_source_transfer", "old_forest_uuid"), ("physical_source_transfer", "target_epoch"), ("physical_source_transfer", "target_forest_uuid"), ("physical_source_transfer", "transfer_plan_bytes"), ("physical_source_transfer", "transfer_plan_sha256"), ("physical_source_transfer", "prepare_operation_uuid"), ("physical_source_transfer", "cutover_operation_uuid"), ("physical_source_transfer", "retire_operation_uuid"), ("physical_source_transfer", "phase"), ("physical_source_transfer", "target_receipts_bytes"), ("physical_source_transfer", "target_receipts_sha256"), ("physical_source_transfer", "retirement_proof_bytes"), ("physical_source_transfer", "retirement_proof_sha256"), ("physical_source_transfer", "transfer_revision"), ("physical_source_transfer", "last_mutation_operation_uuid"), ("physical_source_write", "binding_uuid"), ("physical_source_write", "write_uuid"), ("physical_source_write", "workflow_uuid"), ("physical_source_write", "resource_kind"), ("physical_source_write", "carrier_key_hash"), ("physical_source_write", "carrier_key"), ("physical_source_write", "save_identity"), ("physical_source_write", "loader_identity"), ("physical_source_write", "region_identity"), ("physical_source_write", "slot_x"), ("physical_source_write", "slot_z"), ("physical_source_write", "write_generation"), ("physical_source_write", "payload_descriptor_bytes"), ("physical_source_write", "payload_descriptor_sha256"), ("physical_source_write", "payload_length"), ("physical_source_write", "payload_sha256"), ("physical_source_write", "payload_artifact_identity"), ("physical_source_write", "undo_kind"), ("physical_source_write", "undo_descriptor_bytes"), ("physical_source_write", "undo_descriptor_sha256"), ("physical_source_write", "undo_length"), ("physical_source_write", "undo_content_sha256"), ("physical_source_write", "undo_artifact_identity"), ("physical_source_write", "member_coverage_bytes"), ("physical_source_write", "member_coverage_sha256"), ("physical_source_write", "obligation_revisions_bytes"), ("physical_source_write", "obligation_revisions_sha256"), ("physical_source_write", "limit_profile_sha256"), ("physical_source_write", "receipt_codec"), ("physical_source_write", "prepared_operation_uuid"), ("physical_source_write", "write_state"), ("physical_source_write", "resolved_operation_uuid"), ("physical_source_write", "durable_receipt_bytes"), ("physical_source_write", "durable_receipt_sha256"), ("physical_source_write", "failure_diagnostic_identity"), ("physical_source_write", "failure_diagnostic_sha256"), ("physical_source_write", "write_revision"), ("physical_source_write", "last_mutation_operation_uuid"), ("physical_source_obligation", "binding_uuid"), ("physical_source_obligation", "unit_uuid"), ("physical_source_obligation", "forest_uuid"), ("physical_source_obligation", "resource_kind"), ("physical_source_obligation", "carrier_key_hash"), ("physical_source_obligation", "carrier_key"), ("physical_source_obligation", "purpose"), ("physical_source_obligation", "obligation_state"), ("physical_source_obligation", "obligation_revision"), ("physical_source_obligation", "created_operation_uuid"), ("physical_source_obligation", "write_uuid"), ("physical_source_obligation", "discharged_operation_uuid"), ("physical_source_obligation", "discharge_kind"), ("physical_source_obligation", "discharge_proof_bytes"), ("physical_source_obligation", "discharge_proof_sha256"), ("physical_source_obligation", "last_mutation_operation_uuid"))
  private val Format3TableNames = Set("fp_schema_manifest", "fp_runtime_writer", "physical_recovery_binding", "fp_recovery_operation", "floating_pod_state", "player_entitlement", "binding_bootstrap_intent", "fp_recovery_reservation", "physical_recovery_footprint", "physical_recovery_footprint_chunk", "physical_recovery_intent", "physical_recovery_locator", "physical_recovery_obligation", "fp_lifecycle_completion", "physical_footprint_transfer", "physical_source_unit", "physical_source_forest", "physical_source_transfer", "physical_source_write", "physical_source_obligation")

  private[mysql] def canonicalSha256(resultSets: Vector[MySqlAuditResultSet]): String =
    if resultSets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then reject("cannot hash an incomplete MySQL v4 object coverage")
    val digest = MessageDigest.getInstance("SHA-256")
    digestString(digest, MySqlSchemaAuditProfile.Format1.hashDomain)
    digestString(digest, MySqlSchemaAuditProfile.Format1.coverage)
    digestInt(digest, resultSets.size)
    resultSets.foreach { resultSet =>
      digestString(digest, resultSet.id)
      digestInt(digest, resultSet.columns.size)
      resultSet.columns.foreach { column =>
        digestString(digest, column.label)
        digestInt(digest, column.jdbcType)
        digestString(digest, column.typeName)
        digestInt(digest, column.precision)
        digestInt(digest, column.scale)
        digestInt(digest, column.nullable)
        digest.update(if column.signed then 1.toByte else 0.toByte)
      }
      digestInt(digest, resultSet.rows.size)
      resultSet.rows.foreach { row =>
        digestInt(digest, row.cells.size)
        row.cells.foreach {
          case MySqlAuditNull => digest.update(0.toByte)
          case MySqlAuditText(value) =>
            digest.update(1.toByte)
            digestBytes(digest, encodeUtf8(value))
          case binary: MySqlAuditBinary =>
            digest.update(2.toByte)
            digestBytes(digest, binary.bytes)
          case _: MySqlAuditTimestamp => reject("timestamp cells are outside the fixed v4 static-object manifest")
        }
      }
    }
    HexFormat.of().formatHex(digest.digest())

  private[mysql] def canonicalSha256(profile: MySqlSchemaAuditProfile, resultSets: Vector[MySqlAuditResultSet]): String =
    if profile == null then reject("MySQL object manifest hash profile is required")
    profile match
      case MySqlSchemaAuditProfile.Format1 => canonicalSha256(resultSets)
      case MySqlSchemaAuditProfile.Format2 => canonicalSha256Format2(resultSets)
      case MySqlSchemaAuditProfile.Format3 => canonicalSha256Format3(resultSets, profile)
      case MySqlSchemaAuditProfile.ScriptPublication1 => canonicalSha256Format3(resultSets, profile)
      case MySqlSchemaAuditProfile.DeploymentReceipt1 => canonicalSha256Format3(resultSets, profile)
      case MySqlSchemaAuditProfile.DbFirst1 => canonicalSha256Format3(resultSets, profile)

  private def canonicalSha256Format2(resultSets: Vector[MySqlAuditResultSet]): String =
    if resultSets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then reject("cannot hash an incomplete MySQL format 2 object coverage")
    val digest = MessageDigest.getInstance("SHA-256")
    digestString(digest, MySqlSchemaAuditProfile.Format2.hashDomain)
    digestString(digest, MySqlAuditCoverage.Format2)
    digestInt(digest, resultSets.size)
    resultSets.foreach { resultSet =>
      digestString(digest, resultSet.id)
      digestInt(digest, resultSet.columns.size)
      resultSet.columns.foreach { column =>
        digestString(digest, column.label)
        digestInt(digest, column.jdbcType)
        digestString(digest, column.typeName)
        digestInt(digest, column.precision)
        digestInt(digest, column.scale)
        digestInt(digest, column.nullable)
        digest.update(if column.signed then 1.toByte else 0.toByte)
      }
      digestInt(digest, resultSet.rows.size)
      resultSet.rows.foreach { row =>
        digestInt(digest, row.cells.size)
        row.cells.foreach {
          case MySqlAuditNull => digest.update(0.toByte)
          case MySqlAuditText(value) =>
            digest.update(1.toByte)
            digestBytes(digest, encodeUtf8(value))
          case binary: MySqlAuditBinary =>
            digest.update(2.toByte)
            digestBytes(digest, binary.bytes)
          case _: MySqlAuditTimestamp => reject("timestamp cells are outside the fixed format 2 static-object manifest")
        }
      }
    }
    HexFormat.of().formatHex(digest.digest())

  private def canonicalSha256Format3(resultSets: Vector[MySqlAuditResultSet], profile: MySqlSchemaAuditProfile): String =
    if resultSets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then reject("cannot hash an incomplete MySQL format 3 object coverage")
    val digest = MessageDigest.getInstance("SHA-256")
    digestString(digest, profile.hashDomain)
    digestString(digest, profile.coverage)
    digestInt(digest, resultSets.size)
    resultSets.foreach { resultSet =>
      digestString(digest, resultSet.id)
      digestInt(digest, resultSet.columns.size)
      resultSet.columns.foreach { column =>
        digestString(digest, column.label)
        digestInt(digest, column.jdbcType)
        digestString(digest, column.typeName)
        digestInt(digest, column.precision)
        digestInt(digest, column.scale)
        digestInt(digest, column.nullable)
        digest.update(if column.signed then 1.toByte else 0.toByte)
      }
      digestInt(digest, resultSet.rows.size)
      resultSet.rows.foreach { row =>
        digestInt(digest, row.cells.size)
        row.cells.foreach {
          case MySqlAuditNull => digest.update(0.toByte)
          case MySqlAuditText(value) =>
            digest.update(1.toByte)
            digestBytes(digest, encodeUtf8(value))
          case binary: MySqlAuditBinary =>
            digest.update(2.toByte)
            digestBytes(digest, binary.bytes)
          case _: MySqlAuditTimestamp => reject("timestamp cells are outside the fixed format 3 static-object manifest")
        }
      }
    }
    HexFormat.of().formatHex(digest.digest())

  private[mysql] def uuidFromBytes(bytes: Array[Byte], label: String): UUID =
    if bytes == null || bytes.length != 16 then reject(s"MySQL audit UUID field $label is not exactly 16 bytes")
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    new UUID(buffer.getLong(), buffer.getLong())

  private[mysql] def hashFromBytes(bytes: Array[Byte], label: String): String =
    if bytes == null || bytes.length != 32 then reject(s"MySQL audit SHA-256 field $label is not exactly 32 bytes")
    HexFormat.of().formatHex(bytes)

  private[mysql] def decodeTimestamp(jdbcType: Int, value: Any, wasNull: Boolean): MySqlAuditCell =
    if jdbcType != Types.TIMESTAMP then reject("MySQL audit timestamp is not reported as JDBC TIMESTAMP")
    if value == null then
      if !wasNull then reject("MySQL audit timestamp cell could not be decoded")
      MySqlAuditNull
    else value match
      case timestamp: LocalDateTime => MySqlAuditTimestamp(timestamp)
      case other => reject(s"MySQL audit JDBC TIMESTAMP decoded as ${other.getClass.getName}, expected java.time.LocalDateTime")

  private[mysql] def reject(message: String): Nothing = throw new SQLNonTransientConnectionException(message)

  private[mysql] def reject(message: String, cause: Throwable): Nothing =
    val failure = new SQLNonTransientConnectionException(message)
    failure.initCause(cause)
    throw failure

  private[mysql] def jdbcRead[A](context: String, accessor: String)(read: => A): A =
    try read
    catch
      case failure: SQLException =>
        val contextual = new SQLNonTransientConnectionException(s"MySQL audit $context JDBC $accessor failed")
        contextual.initCause(failure)
        throw contextual
      case failure: IOException =>
        val contextual = new SQLNonTransientConnectionException(s"MySQL audit $context stream read failed")
        contextual.initCause(failure)
        throw contextual

  private[mysql] def addSuppressed(primary: Throwable, cleanup: Throwable): Unit =
    if cleanup != null && (cleanup ne primary) then primary.addSuppressed(cleanup)

  private def withResource[R <: AutoCloseable,A](resource:R)(body:R=>A):A =
    if resource==null then throw new IllegalArgumentException("owned JDBC resource is required")
    var primary:Throwable=null
    try body(resource)
    catch
      case failure:Throwable=>
        primary=failure
        throw failure
    finally
      if primary==null then resource.close()
      else
        try resource.close()
        catch case cleanup:Throwable=>addSuppressed(primary,cleanup)

  private def validateCell(cell: MySqlAuditCell): Unit = cell match
    case null => reject("MySQL audit cell is null instead of a typed SQL cell")
    case MySqlAuditNull => ()
    case MySqlAuditText(value) =>
      if encodeUtf8(value).length > MaxCellBytes then reject("approved MySQL text cell exceeds the byte bound")
    case bytes: MySqlAuditBinary =>
      if bytes.length > MaxCellBytes then reject("approved MySQL binary cell exceeds the byte bound")
    case _: MySqlAuditTimestamp => reject("timestamp cells are outside the fixed v4 static-object manifest")

  private[mysql] def rejectDuplicateRows(resultSets: Vector[MySqlAuditResultSet]): Unit =
    resultSets.foreach { resultSet =>
      if resultSet.rows.distinct.size != resultSet.rows.size then reject(s"MySQL ${resultSet.id} has duplicate metadata rows")
    }

  private def requireLabels(columns: Vector[MySqlAuditColumn], expected: Vector[String], name: String): Unit =
    if columns.map(_.label) != expected then reject(s"MySQL $name columns do not match the fixed audit contract")

  private def oneRow(resultSet: MySqlAuditResultSet, label: String): MySqlAuditRow =
    if resultSet.rows.size != 1 then reject(s"MySQL $label resultset must contain exactly one row")
    resultSet.rows.head

  private def textAt(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String =
    at(resultSet, row, label) match
      case MySqlAuditText(value) => value
      case _ => reject(s"MySQL ${resultSet.id} field $label is null or not text")

  private def longAt(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Long =
    val value = textAt(resultSet, row, label)
    try value.toLong
    catch case failure: NumberFormatException => reject(s"MySQL ${resultSet.id} field $label is malformed", failure)

  private def binaryAt(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): Array[Byte] = at(resultSet, row, label) match
    case bytes: MySqlAuditBinary => bytes.bytes
    case _ => reject(s"MySQL ${resultSet.id} field $label is null or not binary")

  private def at(resultSet: MySqlAuditResultSet, row: MySqlAuditRow, label: String): MySqlAuditCell =
    val index = resultSet.columns.indexWhere(_.label == label)
    if index < 0 || index >= row.cells.size then reject(s"MySQL ${resultSet.id} is missing field $label")
    row.cells(index)

  private def digestString(digest: MessageDigest, value: String): Unit = digestBytes(digest, encodeUtf8(value))
  private def digestBytes(digest: MessageDigest, value: Array[Byte]): Unit =
    digestInt(digest, value.length)
    digest.update(value)
  private def digestInt(digest: MessageDigest, value: Int): Unit =
    digest.update((value >>> 24).toByte)
    digest.update((value >>> 16).toByte)
    digest.update((value >>> 8).toByte)
    digest.update(value.toByte)
  private def sha256(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)

  private[mysql] def encodeUtf8(value: String): Array[Byte] =
    try
      val encoder = StandardCharsets.UTF_8.newEncoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
      val encoded = encoder.encode(CharBuffer.wrap(value))
      val bytes = new Array[Byte](encoded.remaining())
      encoded.get(bytes)
      bytes
    catch case failure: java.nio.charset.CharacterCodingException => reject("MySQL audit text is not valid Unicode", failure)

  private def uuidTextCell(value: String, label: String): UUID =
    if value == null || !CanonicalUuid.matches(value) then reject(s"MySQL audit $label is malformed")
    try UUID.fromString(value)
    catch case failure: IllegalArgumentException => reject(s"MySQL audit $label is malformed", failure)

  private val CanonicalUuid = "(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".r


private def fresh(observedAtNanos: Long, maxAgeNanos: Long, nowNanos: Long): Boolean =
  val age = nowNanos - observedAtNanos
  age >= 0L && age <= maxAgeNanos

private def monotonicWindowValid(observedAtNanos: Long, validityNanos: Long, nowNanos: Long): Boolean =
  val age = nowNanos - observedAtNanos
  age >= 0L && age < validityNanos
