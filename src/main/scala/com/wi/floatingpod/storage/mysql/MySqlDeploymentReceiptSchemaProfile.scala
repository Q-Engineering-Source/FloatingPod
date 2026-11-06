package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.HexFormat
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.util.Using

/** 显式 schema5 渲染器；不可变 schema4 resource 仅作为结构来源模板。 */
object MySqlDeploymentReceiptSchemaProfile:
  import MySqlDeploymentReceiptSchemaDefinition as Definition
  import MySqlScriptSchemaProfile.{Column, ForeignKey, Index, Table}

  final case class Account(user: String, host: String):
    require(user != null && user.matches("[A-Za-z0-9_]{1,32}"), "schema5 account user must be an explicit safe token")
    require(host != null && host.matches("[A-Za-z0-9_.:%-]{1,255}"), "schema5 account host must be an explicit safe token")
    def grantee: String = s"'$user'@'$host'"
    def identity: String = s"$user@$host"

  final case class Accounts(
      guardDefiner: Account,
      auditDefiner: Account,
      runtime: Account,
      provisioner: Account,
      importer: Account,
      operationQuotaUpdater: Account
  ):
    require(Vector(guardDefiner, auditDefiner, runtime, provisioner, importer, operationQuotaUpdater).forall(_ != null),
      "all six schema5 accounts must be explicit")
    private val values = Vector(guardDefiner, auditDefiner, runtime, provisioner, importer, operationQuotaUpdater)
    require(values.map(_.identity).distinct.size == 6, "schema5 requires six distinct account identities")
    def all: Vector[Account] = values

  /** 管理代理例外由 owner 明确输入；空向量也必须显式传入。 */
  final case class TrustedProxyRow(
      user: String,
      host: String,
      proxiedUser: String,
      proxiedHost: String,
      withGrant: Boolean
  ):
    require(user != null && host != null && proxiedUser != null && proxiedHost != null,
      "trusted proxy row must explicitly contain both account identities")
    require(user.matches("[A-Za-z0-9_]{0,32}") && host.matches("[A-Za-z0-9_.:%-]{1,255}") &&
      proxiedUser.matches("[A-Za-z0-9_]{0,32}") && proxiedHost.matches("[A-Za-z0-9_.:%-]{0,255}"),
      "trusted proxy row contains an unsupported account token")
    def tuple: (String, String, String, String, Boolean) = (user, host, proxiedUser, proxiedHost, withGrant)

  final class Inputs private[mysql] (
      val schema: String,
      val accounts: Accounts,
      val sourceLimitProfile: SourceLimitProfile,
      val publicationBudget: MySqlScriptSchemaProfile.Budget,
      val deploymentBudget: Definition.Budget,
      val quotaMaximumRecords: Long,
      val trustedAdministrativeProxyRows: Vector[TrustedProxyRow]
  ):
    require(schema != null && schema.matches("[A-Za-z0-9_]{1,64}"), "schema5 requires an explicit safe schema name")
    require(accounts != null && sourceLimitProfile != null && publicationBudget != null && deploymentBudget != null,
      "schema5 source, account and publication/deployment budgets must be explicit")
    require(quotaMaximumRecords > 0L, "schema5 requires an explicit positive deployment quota maximum")
    require(trustedAdministrativeProxyRows != null && trustedAdministrativeProxyRows.forall(_ != null),
      "schema5 administrative proxy allowlist must be explicitly supplied")
    require(trustedAdministrativeProxyRows.map(_.tuple).distinct.size == trustedAdministrativeProxyRows.size,
      "schema5 administrative proxy allowlist contains duplicate rows")
    require(publicationBudget.retainedBytes <= sourceLimitProfile.maxRetainedBytes() &&
      publicationBudget.packetBytes <= sourceLimitProfile.maxSqlPacketBytes(),
      "schema5 publication profile exceeds explicit source limits")
    require(deploymentBudget.evidenceBytes <= publicationBudget.packetBytes &&
      deploymentBudget.diagnosticsBytes <= publicationBudget.packetBytes &&
      deploymentBudget.evidenceBytes <= publicationBudget.retainedBytes &&
      deploymentBudget.diagnosticsBytes <= publicationBudget.retainedBytes,
      "schema5 deployment receipt budgets exceed explicit publication envelope")
    private[mysql] lazy val profileIdentitySha256: String = configurationHash(this)
    override def toString: String = "MySqlDeploymentReceiptSchemaProfile.Inputs[redacted]"

  def inputs(
      schema: String,
      accounts: Accounts,
      sourceLimitProfile: SourceLimitProfile,
      publicationBudget: MySqlScriptSchemaProfile.Budget,
      deploymentBudget: Definition.Budget,
      quotaMaximumRecords: Long,
      trustedAdministrativeProxyRows: Vector[TrustedProxyRow]
  ): Inputs = new Inputs(schema, accounts, sourceLimitProfile, publicationBudget, deploymentBudget,
    quotaMaximumRecords, trustedAdministrativeProxyRows)

  final class Rendered private[mysql] (
      val inputs: Inputs,
      val sql: String,
      val sha256: String,
      val tables: Vector[Table],
      val guards: Vector[MySqlScriptSchemaProfile.Guard]
  ):
    require(inputs != null && sql != null && sha256 != null && tables.size == 26 && guards.size == 78,
      "schema5 rendered profile must be complete")
    override def toString: String = s"MySqlDeploymentReceiptSchemaProfile.Rendered(schema=${inputs.schema}, sha256=${sha256.take(12)}… )"

  val SchemaFormat: Int = Definition.SchemaFormat
  val Coverage: String = Definition.Coverage
  val AuditRoutine: String = Definition.AuditRoutine
  val AuditDomain: String = Definition.AuditDomain
  val PhysicalBindingFormat: Int = Definition.PhysicalBindingFormat
  val SnapshotDomain: String = Definition.SnapshotDomain
  private[mysql] val RecoveryRegisterPreimageCheck: String =
    "(step='DEPLOYMENT_REGISTER' AND OCTET_LENGTH(preimage_bytes)=0) OR " +
      "(step<>'DEPLOYMENT_REGISTER' AND OCTET_LENGTH(preimage_bytes)>0)"
  private[mysql] val RecoveryStepCheck: String =
    s"step IN (${Definition.Schema5RecoverySteps.map(x => s"'$x'").mkString(",")})"
  val Resource: String = MySqlScriptSchemaProfile.Resource
  val TableNames: Vector[String] = MySqlScriptSchemaProfile.tables.map(_.name) ++ Vector(Definition.OperationTable, Definition.QuotaTable)
  val GuardNames: Vector[String] = MySqlScriptSchemaProfile.guards.map(g => g.name.replace("fp_s4_", "fp_s5_")) ++ Vector(
    "fp_s5_25_bi", "fp_s5_25_bu", "fp_s5_25_bd", "fp_s5_26_bi", "fp_s5_26_bu", "fp_s5_26_bd")

  def render(inputs: Inputs): Rendered =
    if inputs == null then throw new IllegalArgumentException("explicit schema5 renderer inputs are required")
    val oldSql = readSourceSql()
    MySqlScriptSchemaProfile.validateSql(oldSql) match
      case Left(errors) => throw new SQLDataException("immutable schema4 source profile failed identity validation: " + errors.mkString("; "))
      case Right(_) => ()
    var sql = oldSql.replace("fp_audit_schema_script_publication1", Definition.AuditRoutine)
    sql = sql.replace("fp_script_publication1", "__FP_SCHEMA5_TARGET__")
    sql = sql.replace("fp_s4_", "fp_s5_")
    sql = replaceExactlyOnce(sql, "CREATE DATABASE `__FP_SCHEMA5_TARGET__`", s"CREATE DATABASE `${inputs.schema}`")
    sql = replaceExactlyOnce(sql, "USE __FP_SCHEMA5_TARGET__;", s"USE `${inputs.schema}`;")
    sql = sql.replace("__FP_SCHEMA5_TARGET__", inputs.schema)
    val accountTemplates = Vector(
      ("fp_s5_guard_definer", inputs.accounts.guardDefiner, "{{FP_SCHEMA5_ACCOUNT_GUARD}}"),
      ("fp_s5_audit_definer", inputs.accounts.auditDefiner, "{{FP_SCHEMA5_ACCOUNT_AUDIT}}"),
      ("fp_s5_runtime", inputs.accounts.runtime, "{{FP_SCHEMA5_ACCOUNT_RUNTIME}}"),
      ("fp_s5_provisioner", inputs.accounts.provisioner, "{{FP_SCHEMA5_ACCOUNT_PROVISIONER}}"),
      ("fp_s5_importer", inputs.accounts.importer, "{{FP_SCHEMA5_ACCOUNT_IMPORTER}}"))
    accountTemplates.foreach { (templateUser, _, placeholder) =>
      val templateGrantee = s"'$templateUser'@'localhost'"
      if !sql.contains(templateGrantee) then throw new SQLDataException(s"schema4 template account missing: $templateUser")
      sql = sql.replace(templateGrantee, placeholder)
    }
    accountTemplates.foreach { (_, account, placeholder) => sql = sql.replace(placeholder, account.grantee) }
    val importerCreate = s"CREATE USER ${inputs.accounts.importer.grantee} ACCOUNT LOCK;"
    sql = replaceExactlyOnce(sql, importerCreate, importerCreate + s"\nCREATE USER ${inputs.accounts.operationQuotaUpdater.grantee} ACCOUNT LOCK;")
    sql = sql.replace("format_version=4", "format_version=5")
    sql = sql.replace("format_version = 4", "format_version = 5")
    sql = sql.replace("VALUES (1,'MYSQL',4,", "VALUES (1,'MYSQL',5,")
    sql = replaceRecoveryStepCheck(sql)
    sql = replaceRecoveryRegisterPreimageCheck(sql)
    sql = rewriteAuditRoutine(sql, inputs)
    sql = sql.replace("-- FloatingPod schema format 4 / ScriptPublication 1. SOURCE DEFINITION ONLY.",
      "-- FloatingPod schema 格式5 / ScriptPublication 1 + Deployment Receipt 1。仅为源码定义。")
    sql = sql.replace("-- SOURCE v4 ONLY: 60 actual guards", "-- SOURCE v5：沿用原72个守卫并增加6个部署/配额守卫。")
    sql = insertBefore(sql, "-- Two control seeds only;", tableDdl(inputs))
    sql = replaceExactlyOnce(sql,
      "-- SOURCE v4: complete actual 20-table/368-column/60-trigger capture, same18sets.",
      "-- SOURCE v5：准确覆盖26张表、467列、78个守卫与相同的18个结果集。")
    sql = insertBefore(sql, "DELIMITER ;\n\n-- SOURCE v5：准确覆盖26张表、467列、78个守卫", triggerDdl(inputs))
    sql = sql + "\n" + privilegeDdl(inputs)
    val statements = MySqlScriptSchemaProfile.statements(sql).fold(e => throw new SQLDataException("schema5 renderer framing failed: " + e), identity)
    val tables = parseTables(statements, inputs)
    val guards = parseGuards(statements)
    if tables.size != 26 || tables.map(_.name).distinct.size != 26 then throw new SQLDataException("schema5 renderer did not produce exactly26 tables")
    if tables.map(_.columns.size).sum != 467 then throw new SQLDataException("schema5 renderer did not produce exactly467 columns")
    if guards.size != 78 || guards.map(_.name).distinct.size != 78 then throw new SQLDataException("schema5 renderer did not produce exactly78 guards")
    val hash = MySqlScriptSchemaProfile.sha256(sql.getBytes(StandardCharsets.UTF_8))
    new Rendered(inputs, sql, hash, tables, guards)

  /** 从最终 GRANT 语句生成独立预期的四个授权结果行；不把 SQL 文本当运行行为证据。 */
  private[mysql] def expectedPrivilegeRows(rendered: Rendered): Vector[Vector[Vector[String]]] =
    if rendered == null then throw new IllegalArgumentException("schema5 rendered grant source is required")
    expectedPrivilegeRows(rendered.sql, rendered.inputs, Definition.AuditRoutine)

  /** 共享 GRANT 解析规则；调用者仍须提供其独立 profile 的确切 routine 身份。 */
  private[mysql] def expectedPrivilegeRows(sql: String, inputs: Inputs,
      auditRoutine: String): Vector[Vector[Vector[String]]] =
    require(sql != null && inputs != null && auditRoutine != null && auditRoutine.matches("[a-z][a-z0-9_]{0,63}"),
      "explicit rendered SQL, accounts and audit routine required")
    val tableRows = scala.collection.mutable.Set.empty[Vector[String]]
    val columnRows = scala.collection.mutable.Set.empty[Vector[String]]
    val schemaRows = scala.collection.mutable.Set.empty[Vector[String]]
    val routineRows = scala.collection.mutable.Set.empty[(String, String, String, String)]
    val globalRows = scala.collection.mutable.Set.empty[Vector[String]]
    val grantLine = "^GRANT (.+) ON (.+) TO '([^']+)'@'([^']+)';$".r
    val tableTarget = "^`([A-Za-z0-9_]+)`\\.`([A-Za-z0-9_]+)`$".r
    val schemaTarget = "^`([A-Za-z0-9_]+)`\\.\\*$".r
    val procedureTarget = "^PROCEDURE `([A-Za-z0-9_]+)`\\.`([A-Za-z0-9_]+)`$".r
    val globalTarget = "^\\*\\.\\*$".r
    val validGrantees = inputs.accounts.all.map(a => (a.user, a.host)).toSet
    sql.linesIterator.filter(_.startsWith("GRANT ")).foreach {
      case grantLine(privilegeText, target, user, host) =>
        if !validGrantees.contains((user, host)) then throw new SQLDataException("schema5 grant names an account outside the explicit six-account profile")
        target match
          case procedureTarget(db, routine) =>
            privilegeText match
              case "EXECUTE" => routineRows += ((user, host, db, routine))
              case _ => throw new SQLDataException("schema5 renderer produced an unsupported routine privilege")
          case tableTarget(db, table) =>
            val updateColumns = "^UPDATE \\(([^)]*)\\)$".r
            privilegeText match
              case updateColumns(columns) => columns.split(",", -1).foreach { column =>
                if column.isEmpty || column.exists(ch => !(ch.isLetterOrDigit || ch == '_')) then
                  throw new SQLDataException("schema5 renderer produced a malformed column privilege")
                columnRows += Vector(inputs.accounts.all.find(a => a.user == user && a.host == host).get.grantee,
                  "def", db, table, column, "UPDATE", "NO")
              }
              case many => many.split(",", -1).foreach { privilege =>
                if !privilege.matches("[A-Z]+(?: [A-Z]+)*") then
                  throw new SQLDataException("schema5 renderer produced an unsupported table privilege")
                tableRows += Vector(inputs.accounts.all.find(a => a.user == user && a.host == host).get.grantee,
                  "def", db, table, privilege, "NO")
              }
          case schemaTarget(db) =>
            privilegeText.split(",", -1).foreach { privilege =>
              if !privilege.matches("[A-Z]+(?: [A-Z]+)*") then
                throw new SQLDataException("schema5 renderer produced an unsupported schema privilege")
              schemaRows += Vector(inputs.accounts.all.find(a => a.user == user && a.host == host).get.grantee,
                "def", db, privilege, "NO")
            }
          case globalTarget() if privilegeText == "SHOW_ROUTINE" &&
              user == inputs.accounts.auditDefiner.user && host == inputs.accounts.auditDefiner.host =>
            globalRows += Vector(inputs.accounts.auditDefiner.grantee, "def", "SHOW_ROUTINE", "NO")
          case _ => throw new SQLDataException("schema5 renderer produced an unsupported GRANT target")
      case malformed => throw new SQLDataException("schema5 renderer emitted an unparseable GRANT statement: " + malformed.take(80))
    }
    val expectedRoutineRows = Set(
      (inputs.accounts.auditDefiner.user, inputs.accounts.auditDefiner.host, inputs.schema, auditRoutine),
      (inputs.accounts.runtime.user, inputs.accounts.runtime.host, inputs.schema, auditRoutine))
    if routineRows.toSet != expectedRoutineRows then throw new SQLDataException("schema5 routine EXECUTE grants differ from exact definer/runtime policy")
    val userRows = inputs.accounts.all.map(a => Vector(a.grantee, "def", "USAGE", "NO")) ++ globalRows
    Vector(tableRows.toVector.sortBy(r => (r(0), r(1), r(2), r(3), r(4))),
      columnRows.toVector.sortBy(r => (r(0), r(1), r(2), r(3), r(4), r(5))),
      schemaRows.toVector.sortBy(r => (r(0), r(1), r(2), r(3))),
      userRows.sortBy(r => (r(0), r(1), r(2))))

  private[mysql] def sourceSqlLength(): Long =
    val stream = Option(getClass.getResourceAsStream(Resource)).getOrElse(
      throw new SQLDataException("immutable schema4 SQL source resource is unavailable"))
    Using.resource(stream) { in =>
      val buffer = new Array[Byte](8192)
      var count = 0L
      var n = in.read(buffer)
      while n != -1 do
        count = Math.addExact(count, n.toLong)
        n = in.read(buffer)
      count
    }

  private def readSourceSql(): String =
    val stream = Option(getClass.getResourceAsStream(Resource)).getOrElse(
      throw new SQLDataException("immutable schema4 SQL source resource is unavailable"))
    Using.resource(stream)(in => new String(in.readAllBytes(), StandardCharsets.UTF_8))

  private def replaceExactlyOnce(source: String, old: String, replacement: String): String =
    val first = source.indexOf(old)
    if first < 0 || source.indexOf(old, first + old.length) >= 0 then
      throw new SQLDataException(s"schema5 renderer expected one exact source anchor: $old")
    source.substring(0, first) + replacement + source.substring(first + old.length)

  private def replaceRecoveryStepCheck(sql: String): String =
    val oldTokens = Definition.OldRecoverySteps.map(x => s"'$x'").mkString(",")
    val old = s"step IN ($oldTokens)"
    if sql.indexOf(old) < 0 || sql.indexOf(old, sql.indexOf(old) + old.length) >= 0 then
      throw new SQLDataException("schema4 recovery-step check anchor is missing or duplicated")
    sql.replace(old, RecoveryStepCheck)

  private def replaceRecoveryRegisterPreimageCheck(sql: String): String =
    replaceExactlyOnce(sql,
      "CONSTRAINT ck_f3_04_34 CHECK (OCTET_LENGTH(preimage_bytes)>0),",
      s"CONSTRAINT ck_f3_04_34 CHECK ($RecoveryRegisterPreimageCheck),")

  private def tableDdl(inputs: Inputs): String =
    val opChecks = Definition.operationChecks(inputs.deploymentBudget).zipWithIndex.map { (check, i) =>
      val ordinal = if i + 1 < 10 then s"0${i + 1}" else (i + 1).toString
      s"  CONSTRAINT ck_s5_25_$ordinal CHECK ($check)"
    }
    val quotaChecks = Definition.quotaChecks.zipWithIndex.map { (check, i) =>
      val ordinal = if i + 1 < 10 then s"0${i + 1}" else (i + 1).toString
      s"  CONSTRAINT ck_s5_26_$ordinal CHECK ($check)"
    }
    val op = createTable(Definition.OperationTable, Definition.operationColumns, Definition.operationPrimaryKey,
      Definition.operationIndexes, opChecks)
    val quota = createTable(Definition.QuotaTable, Definition.quotaColumns, Definition.quotaPrimaryKey,
      Definition.quotaIndexes, quotaChecks)
    s"\n-- 部署恢复历史与命名空间配额的闭合布局。\n$op\n\n$quota\n" +
      s"\nINSERT INTO `${inputs.schema}`.`${Definition.QuotaTable}` (namespace,max_records,used_records,quota_revision) " +
      s"VALUES (UNHEX('${Definition.NamespaceHex}'),${inputs.quotaMaximumRecords},0,0);\n\n"

  private[mysql] def operationPrechargeNotNullColumns: Vector[String] =
    Definition.operationColumns.collect { case column if column.declaration.contains("NOT NULL") => column.name }

  private[mysql] def operationPrechargeCheckExpressions(budget: Definition.Budget): Vector[String] =
    Definition.operationChecks(budget).map { check =>
      Definition.operationColumns.foldLeft(check) { (expression, column) =>
        val pattern = Pattern.compile(s"(?<![A-Za-z0-9_.])${Pattern.quote(column.name)}(?![A-Za-z0-9_])")
        pattern.matcher(expression).replaceAll(Matcher.quoteReplacement(s"NEW.${column.name}"))
      }
    }

  private def createTable(name: String, columns: Vector[Column], primaryKey: Vector[String], indexes: Vector[Index], checks: Vector[String]): String =
    val parts = Vector.newBuilder[String]
    columns.foreach(c => parts += s"  `${c.name}` ${c.declaration}")
    parts += s"  PRIMARY KEY (${primaryKey.map(x => s"`$x`").mkString(",")})"
    indexes.foreach(i => parts += s"  ${if i.unique then "UNIQUE " else ""}KEY `${i.name}` (${i.columns.map(x => s"`$x`").mkString(",")})")
    checks.foreach(parts += _)
    s"CREATE TABLE `${name}` (\n${parts.result().mkString(",\n")}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;"

  private def triggerDdl(inputs: Inputs): String =
    val updater = inputs.accounts.operationQuotaUpdater.grantee
    val guard = inputs.accounts.guardDefiner.grantee
    val schema = s"`${inputs.schema}`"
    val op = Definition.OperationTable
    val quota = Definition.QuotaTable
    val requestIdentity = "CONCAT(NEW.namespace,NEW.actor_uuid,NEW.request_bytes)"
    val requiredNullChecks = operationPrechargeNotNullColumns.map(name => s"NEW.$name IS NOT NULL")
    val definitionChecks = operationPrechargeCheckExpressions(inputs.deploymentBudget).map(check => s"($check) <=> TRUE")
    val prechargeContract = (requiredNullChecks ++ definitionChecks).mkString(" AND\n  ")
    val commonNew = s"""
 IF NOT (($prechargeContract) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment receipt row fails a required column or exact schema5 check before quota charge'; END IF;
"""
    val liveOwner = s"""
 IF NOT ((EXISTS(SELECT 1 FROM ${schema}.fp_schema_manifest m
  JOIN ${schema}.physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid
  JOIN ${schema}.fp_runtime_writer w ON w.binding_uuid=b.binding_uuid
  WHERE m.singleton_fixed1=1 AND m.format_version=5 AND m.bootstrap_state='READY'
   AND b.binding_uuid=NEW.authority_binding_uuid AND b.format_version=2 AND b.cutover_state='CERTIFIED'
   AND w.owner_boot_uuid=NEW.current_writer_boot_uuid AND w.writer_epoch=NEW.current_writer_epoch
   AND w.owner_boot_uuid IS NOT NULL AND w.lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment operation has no exact schema5 B2 live writer'; END IF;
"""
    val operationInsert = s"""
CREATE DEFINER=$updater TRIGGER fp_s5_25_bi BEFORE INSERT ON `${op}` FOR EACH ROW
BEGIN
 DECLARE v_found BOOLEAN DEFAULT TRUE;
 DECLARE v_max BIGINT DEFAULT NULL;
 DECLARE v_used BIGINT DEFAULT NULL;
 DECLARE v_revision BIGINT DEFAULT NULL;
 DECLARE v_existing_actor VARBINARY(16) DEFAULT NULL;
 DECLARE v_existing_request VARBINARY(134) DEFAULT NULL;
 DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_found=FALSE;
$commonNew
 IF NOT ((NEW.row_revision=0 AND NEW.last_step_code=1 AND NEW.register_step_uuid=NEW.last_step_uuid AND
  NEW.current_writer_boot_uuid=NEW.created_boot_uuid AND NEW.current_writer_epoch=NEW.created_writer_epoch AND
  NEW.step_evidence_encoding=NEW.enqueue_evidence_encoding AND NEW.step_evidence_bytes=NEW.enqueue_evidence_bytes AND
  NEW.step_evidence_sha256=NEW.enqueue_evidence_sha256 AND ORD(SUBSTRING(NEW.receipt_bytes,136,1))=1 AND
  NEW.diagnostic_encoding IS NULL AND NEW.diagnostic_bytes IS NULL AND NEW.diagnostic_sha256 IS NULL) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment REGISTER must begin from exact original enqueue evidence'; END IF;
 $liveOwner
 IF NOT ((EXISTS(SELECT 1 FROM ${schema}.fp_recovery_operation r JOIN ${schema}.fp_runtime_writer w ON w.binding_uuid=NEW.authority_binding_uuid
  WHERE r.binding_uuid=NEW.authority_binding_uuid AND r.operation_uuid=NEW.last_step_uuid AND r.step='DEPLOYMENT_REGISTER' AND r.operation_state='RESERVED'
   AND r.operation_fingerprint=NEW.last_step_fingerprint AND r.request_identity_bytes=$requestIdentity
   AND OCTET_LENGTH(r.request_identity_bytes)=154 AND r.request_identity_sha256=UNHEX(SHA2(r.request_identity_bytes,256))
   AND r.target_bytes=NEW.step_evidence_bytes AND r.target_sha256=NEW.step_evidence_sha256
   AND OCTET_LENGTH(r.preimage_bytes)=0 AND r.preimage_sha256=UNHEX(SHA2(r.preimage_bytes,256))
   AND r.expected_domain_version=0 AND r.previous_step_operation_uuid IS NULL
   AND r.boot_uuid=NEW.created_boot_uuid AND r.boot_generation=NEW.created_boot_generation
   AND r.fence_epoch=NEW.created_writer_epoch AND r.boot_uuid=w.owner_boot_uuid
   AND r.fence_epoch=w.writer_epoch AND r.connection_id=CONNECTION_ID() AND r.fixed_lease_until=w.lease_until
   AND r.fixed_lease_until>UTC_TIMESTAMP(6))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment REGISTER is not bound to its exact original RESERVED recovery row'; END IF;
 IF NOT ((NEW.created_boot_generation>=0 AND NEW.created_writer_epoch>=0 AND NEW.current_writer_epoch>=0 AND
  NEW.enqueued_end_ordinal>=0 AND NEW.max_queue_age_end_ordinals>=0 AND NEW.last_step_code IN (1,2,3,4,5) AND
  OCTET_LENGTH(NEW.last_step_uuid)=16 AND OCTET_LENGTH(NEW.last_step_fingerprint)=32) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment REGISTER numeric or step identity is malformed'; END IF;
 SELECT max_records,used_records,quota_revision INTO v_max,v_used,v_revision
  FROM ${schema}.${quota} WHERE namespace=NEW.namespace FOR UPDATE;
 IF NOT ((v_found AND v_max>0 AND v_used>=0 AND v_used<=v_max AND v_revision>=0) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota row is missing or malformed'; END IF;
 SET v_found=TRUE;
 SELECT actor_uuid,request_bytes INTO v_existing_actor,v_existing_request
  FROM ${schema}.${op} WHERE namespace=NEW.namespace AND operation_uuid=NEW.operation_uuid FOR SHARE;
 IF v_found THEN
  IF NOT ((v_existing_actor=NEW.actor_uuid AND v_existing_request=NEW.request_bytes) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment operation identity conflicts with existing request'; END IF;
 ELSE
  IF NOT ((v_used<v_max AND v_revision<9223372036854775807) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota is full or revision exhausted'; END IF;
  UPDATE ${schema}.${quota} SET used_records=used_records+1,quota_revision=quota_revision+1
   WHERE namespace=NEW.namespace AND max_records=v_max AND used_records=v_used AND quota_revision=v_revision;
  IF ROW_COUNT()<>1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota reservation lost its locked row'; END IF;
 END IF;
END$$$$
"""
    val operationUpdate = s"""
CREATE DEFINER=$guard TRIGGER fp_s5_25_bu BEFORE UPDATE ON `${op}` FOR EACH ROW
BEGIN
$commonNew
$liveOwner
 IF NOT (((OLD.namespace <=> NEW.namespace) AND (OLD.operation_uuid <=> NEW.operation_uuid) AND
  (OLD.actor_uuid <=> NEW.actor_uuid) AND (OLD.authority_binding_uuid <=> NEW.authority_binding_uuid) AND
  (OLD.request_encoding <=> NEW.request_encoding) AND (OLD.request_bytes <=> NEW.request_bytes) AND
  (OLD.request_sha256 <=> NEW.request_sha256) AND (OLD.created_boot_uuid <=> NEW.created_boot_uuid) AND
  (OLD.created_boot_generation <=> NEW.created_boot_generation) AND (OLD.created_writer_epoch <=> NEW.created_writer_epoch) AND
  (OLD.register_step_uuid <=> NEW.register_step_uuid) AND (OLD.enqueued_end_ordinal <=> NEW.enqueued_end_ordinal) AND
  (OLD.max_queue_age_end_ordinals <=> NEW.max_queue_age_end_ordinals) AND
  (OLD.enqueue_evidence_encoding <=> NEW.enqueue_evidence_encoding) AND
  (OLD.enqueue_evidence_bytes <=> NEW.enqueue_evidence_bytes) AND (OLD.enqueue_evidence_sha256 <=> NEW.enqueue_evidence_sha256) AND
  NEW.row_revision=OLD.row_revision+1) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment operation original identity, enqueue or revision changed'; END IF;
 IF NOT ((EXISTS(SELECT 1 FROM ${schema}.fp_recovery_operation r
  WHERE r.binding_uuid=NEW.authority_binding_uuid AND r.operation_uuid=NEW.last_step_uuid AND r.step=CASE NEW.last_step_code
    WHEN 1 THEN 'DEPLOYMENT_REGISTER' WHEN 2 THEN 'DEPLOYMENT_START' WHEN 3 THEN 'DEPLOYMENT_FINALIZE'
    WHEN 4 THEN 'DEPLOYMENT_RECONCILE' WHEN 5 THEN 'DEPLOYMENT_CLEANUP' ELSE '' END
   AND r.operation_state='RESERVED' AND r.operation_fingerprint=NEW.last_step_fingerprint
   AND r.request_identity_bytes=$requestIdentity AND OCTET_LENGTH(r.request_identity_bytes)=154
   AND r.request_identity_sha256=UNHEX(SHA2(r.request_identity_bytes,256))
   AND r.target_bytes=NEW.step_evidence_bytes AND r.target_sha256=NEW.step_evidence_sha256
   AND r.preimage_bytes=OLD.receipt_bytes AND r.preimage_sha256=UNHEX(SHA2(r.preimage_bytes,256))
   AND r.expected_domain_version=OLD.row_revision AND r.previous_step_operation_uuid=OLD.last_step_uuid
   AND r.boot_uuid=NEW.current_writer_boot_uuid AND r.fence_epoch=NEW.current_writer_epoch
   AND r.connection_id=CONNECTION_ID() AND r.fixed_lease_until>UTC_TIMESTAMP(6)
   AND EXISTS(SELECT 1 FROM ${schema}.fp_runtime_writer w WHERE w.binding_uuid=NEW.authority_binding_uuid
    AND w.owner_boot_uuid=NEW.current_writer_boot_uuid AND w.writer_epoch=NEW.current_writer_epoch
    AND w.lease_until=r.fixed_lease_until))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment transition lacks exact mapped RESERVED recovery evidence and current owner'; END IF;
 IF NOT (((NEW.last_step_code=2 AND ORD(SUBSTRING(OLD.receipt_bytes,136,1))=1 AND
    ORD(SUBSTRING(NEW.receipt_bytes,136,1))=2 AND NEW.current_writer_boot_uuid=OLD.created_boot_uuid) OR
   (NEW.last_step_code=3 AND ((ORD(SUBSTRING(OLD.receipt_bytes,136,1))=1 AND ORD(SUBSTRING(NEW.receipt_bytes,136,1)) IN (4,5,6)) OR
    (ORD(SUBSTRING(OLD.receipt_bytes,136,1))=2 AND ORD(SUBSTRING(NEW.receipt_bytes,136,1)) IN (3,5,6)))) OR
   (NEW.last_step_code=4 AND ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (3,4,5,6) AND
    ((ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (3,4) AND
      ORD(SUBSTRING(NEW.receipt_bytes,136,1))=ORD(SUBSTRING(OLD.receipt_bytes,136,1))) OR
     (ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (5,6) AND ORD(SUBSTRING(NEW.receipt_bytes,136,1)) IN (3,5,6)))) OR
   (NEW.last_step_code=5 AND ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (3,4,5,6) AND
    ((ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (3,4) AND
      ORD(SUBSTRING(NEW.receipt_bytes,136,1))=ORD(SUBSTRING(OLD.receipt_bytes,136,1))) OR
     (ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (5,6) AND ORD(SUBSTRING(NEW.receipt_bytes,136,1)) IN (3,5,6))))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment step cannot resurrect, skip or rewrite terminal debt'; END IF;
 IF NOT ((OLD.diagnostic_bytes IS NULL OR
  ((OLD.diagnostic_encoding <=> NEW.diagnostic_encoding) AND (OLD.diagnostic_bytes <=> NEW.diagnostic_bytes) AND
   (OLD.diagnostic_sha256 <=> NEW.diagnostic_sha256))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='existing deployment diagnostic cannot be erased or rewritten'; END IF;
END$$$$
"""
    val operationDelete = s"""
CREATE DEFINER=$guard TRIGGER fp_s5_25_bd BEFORE DELETE ON `${op}` FOR EACH ROW
BEGIN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment operation history DELETE is forbidden';
END$$$$
"""
    val quotaInsert = s"""
CREATE DEFINER=$guard TRIGGER fp_s5_26_bi BEFORE INSERT ON `${quota}` FOR EACH ROW
BEGIN
 IF NOT ((NEW.namespace=UNHEX('${Definition.NamespaceHex}') AND NEW.max_records>0 AND
  NEW.used_records=0 AND NEW.quota_revision=0 AND NOT EXISTS(SELECT 1 FROM ${schema}.${op})) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota may only seed a fresh empty namespace'; END IF;
END$$$$
"""
    val quotaUpdate = s"""
CREATE DEFINER=$guard TRIGGER fp_s5_26_bu BEFORE UPDATE ON `${quota}` FOR EACH ROW
BEGIN
 IF NOT (((OLD.namespace <=> NEW.namespace) AND (OLD.max_records <=> NEW.max_records) AND
  OLD.used_records<9223372036854775807 AND OLD.quota_revision<9223372036854775807 AND
  NEW.used_records=OLD.used_records+1 AND NEW.quota_revision=OLD.quota_revision+1 AND
  NEW.used_records<=NEW.max_records) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota permits only exact bounded used/revision increment'; END IF;
END$$$$
"""
    val quotaDelete = s"""
CREATE DEFINER=$guard TRIGGER fp_s5_26_bd BEFORE DELETE ON `${quota}` FOR EACH ROW
BEGIN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='deployment quota deletion is forbidden';
END$$$$
"""
    Vector(operationInsert, operationUpdate, operationDelete, quotaInsert, quotaUpdate, quotaDelete).mkString("\n")

  private def rewriteAuditRoutine(source: String, inputs: Inputs): String =
    val accountGrantees = inputs.accounts.all.map(_.grantee)
    val accounts = accountGrantees.map(x => "'" + x.replace("'", "''") + "'").mkString(",")
    val schema = inputs.schema
    val oldRoutineFilter = s"AND CAST(ROUTINE_NAME AS BINARY)=CAST('${Definition.AuditRoutine.replace("deployment_receipt1", "script_publication1")}' AS BINARY)\n    AND ROUTINE_TYPE='PROCEDURE'"
    val renamedRoutineFilter = s"AND CAST(ROUTINE_NAME AS BINARY)=CAST('${Definition.AuditRoutine}' AS BINARY)\n    AND ROUTINE_TYPE='PROCEDURE'"
    var sql = source.replace(oldRoutineFilter, "")
    if sql == source then sql = source.replace(renamedRoutineFilter, "")
    if sql == source then throw new SQLDataException("schema5 routine inventory filter anchor was not found")
    val oldGrantee = "GRANTEE='''fp_s5_audit_definer''@''localhost'''"
    val newGrantee = s"GRANTEE IN ($accounts)"
    sql = sql.replace(oldGrantee, newGrantee)
    sql = sql.replace(s"TABLE_SCHEMA='$schema' AND $newGrantee", s"$newGrantee")
    sql = sql.replace(
      "-- RS15..18: effective direct privileges visible for this locked audit definer ONLY.\n  -- Caller effective grants/roles require the outer same-connection SHOW GRANTS envelope.",
      "-- RS15..18: six explicit subject accounts across every grant scope.\n  -- Caller roles and grants are independently pinned by the outer same-connection envelope.")
    // RS15-RS17 必须枚举六个账号在所有 schema 的实际授权，由独立 allowlist 拒绝越界行。
    val marker = "BEGIN\n  -- RS01 HEADER"
    val securityChecks = securityPrelude(inputs)
    if !sql.contains(marker) then throw new SQLDataException("schema5 audit procedure body anchor is missing")
    sql = sql.replace("ORDER BY CAST(TABLE_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);",
      "ORDER BY CAST(GRANTEE AS BINARY),CAST(TABLE_SCHEMA AS BINARY),CAST(TABLE_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);")
    sql = sql.replace("ORDER BY CAST(TABLE_NAME AS BINARY),CAST(COLUMN_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);",
      "ORDER BY CAST(GRANTEE AS BINARY),CAST(TABLE_SCHEMA AS BINARY),CAST(TABLE_NAME AS BINARY),CAST(COLUMN_NAME AS BINARY),CAST(PRIVILEGE_TYPE AS BINARY);")
    sql.replace(marker, s"BEGIN\n$securityChecks\n  -- RS01 HEADER")

  private def securityPrelude(inputs: Inputs): String =
    val names = inputs.accounts.all.map(a => s"'${a.user}'").mkString(",")
    val lockedAccounts = Vector(inputs.accounts.guardDefiner, inputs.accounts.auditDefiner,
      inputs.accounts.provisioner, inputs.accounts.importer, inputs.accounts.operationQuotaUpdater)
      .map(a => s"(BINARY u.User=BINARY '${a.user}' AND BINARY u.Host=BINARY '${a.host}')").mkString(" OR ")
    val routine = Definition.AuditRoutine
    val routineGranteePairs = Vector(inputs.accounts.auditDefiner, inputs.accounts.runtime).map { account =>
      s"(BINARY p.User=BINARY '${account.user}' AND BINARY p.Host=BINARY '${account.host}' AND " +
        s"BINARY p.Db=BINARY '${inputs.schema}' AND BINARY p.Routine_name=BINARY '$routine' AND " +
        "p.Routine_type='PROCEDURE' AND p.Proc_priv='Execute')"
    }
    val allowedRoutineRows = routineGranteePairs.mkString("(", " OR ", ")")
    val exactProxyRows = inputs.trustedAdministrativeProxyRows.map { row =>
      s"(BINARY p.User=BINARY '${sqlLiteral(row.user)}' AND BINARY p.Host=BINARY '${sqlLiteral(row.host)}' AND " +
      s"BINARY p.Proxied_user=BINARY '${sqlLiteral(row.proxiedUser)}' AND BINARY p.Proxied_host=BINARY '${sqlLiteral(row.proxiedHost)}' AND " +
        s"p.With_grant=${if row.withGrant then 1 else 0})"
    }
    val proxyAllow = if exactProxyRows.isEmpty then "FALSE" else exactProxyRows.mkString("(", " OR ", ")")
    s"""  DECLARE fp_bad BIGINT DEFAULT 0;
  DECLARE fp_account_count BIGINT DEFAULT 0;
  DECLARE fp_mandatory_roles TEXT DEFAULT '';
  SELECT COUNT(*),COALESCE(SUM(u.account_locked='Y'),0) INTO fp_account_count,fp_bad
   FROM mysql.user u WHERE $lockedAccounts;
  IF fp_account_count<>5 OR fp_bad<>5 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='all five non-runtime schema5 helper accounts must exist and remain locked'; END IF;
  SELECT COUNT(*) INTO fp_bad FROM mysql.role_edges r
   WHERE r.TO_USER IN ($names);
  IF fp_bad<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema5 account has a direct or inherited role edge'; END IF;
  SELECT COUNT(*) INTO fp_bad FROM mysql.default_roles r
   WHERE r.USER IN ($names);
  IF fp_bad<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema5 account has an enabled default role'; END IF;
  SELECT COUNT(*) INTO fp_bad FROM mysql.procs_priv p
   WHERE p.User IN ($names) AND NOT $allowedRoutineRows;
  IF fp_bad<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema5 has an unapproved direct routine privilege'; END IF;
  SELECT COUNT(*) INTO fp_bad FROM mysql.procs_priv p
   WHERE p.User IN ($names) AND $allowedRoutineRows;
  IF fp_bad<>2 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema5 audit routine requires exact definer/runtime EXECUTE grants'; END IF;
  SET fp_mandatory_roles=COALESCE(@@GLOBAL.mandatory_roles,'');
  IF fp_mandatory_roles<>'' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='server mandatory_roles must be empty for schema5'; END IF;
  SELECT COUNT(*) INTO fp_bad FROM mysql.proxies_priv p
   WHERE (p.User IS NULL OR p.Host IS NULL OR p.Proxied_user IS NULL OR p.Proxied_host IS NULL OR p.With_grant IS NULL OR
    p.User='' OR p.Proxied_user='' OR p.User IN ($names) OR p.Proxied_user IN ($names) OR
    LOCATE('%',p.User)>0 OR LOCATE('_',p.User)>0 OR LOCATE(CHAR(92),p.User)>0 OR
    LOCATE('%',p.Proxied_user)>0 OR LOCATE('_',p.Proxied_user)>0 OR LOCATE(CHAR(92),p.Proxied_user)>0)
    AND NOT $proxyAllow;
  IF fp_bad<>0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='schema5 encountered unapproved, wildcard or ambiguous proxy delegation'; END IF;"""

  private def privilegeDdl(inputs: Inputs): String =
    val schema = s"`${inputs.schema}`"
    val accounts = inputs.accounts
    val op = Definition.OperationTable
    val quota = Definition.QuotaTable
    val mutableColumns = Vector("current_writer_boot_uuid", "current_writer_epoch", "last_step_uuid", "last_step_code",
      "last_step_fingerprint", "step_evidence_encoding", "step_evidence_bytes", "step_evidence_sha256",
      "diagnostic_encoding", "diagnostic_bytes", "diagnostic_sha256", "receipt_bytes", "receipt_sha256", "row_revision")
    val columnUpdate = mutableColumns.sorted.mkString(",")
    val out = new StringBuilder("\n-- Schema5 部署回执专用权限闭包。\n")
    def grant(priv: String, target: String, account: Account): Unit = out.append(s"GRANT $priv ON $target TO ${account.grantee};\n")
    Vector(op, quota).foreach { table =>
      grant("SELECT,TRIGGER", s"$schema.`$table`", accounts.guardDefiner)
      grant("SELECT", s"$schema.`$table`", accounts.auditDefiner)
      grant("SELECT", s"$schema.`$table`", accounts.runtime)
      grant("SELECT", s"$schema.`$table`", accounts.provisioner)
    }
    grant("INSERT", s"$schema.`$op`", accounts.runtime)
    grant(s"UPDATE ($columnUpdate)", s"$schema.`$op`", accounts.runtime)
    grant("INSERT", s"$schema.`$op`", accounts.importer)
    grant("INSERT", s"$schema.`$quota`", accounts.provisioner)
    grant("CREATE,INDEX,TRIGGER", s"$schema.`$op`", accounts.provisioner)
    grant("CREATE,INDEX,TRIGGER", s"$schema.`$quota`", accounts.provisioner)
    grant(s"TRIGGER", s"$schema.`$op`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`fp_schema_manifest`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`physical_recovery_binding`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`fp_runtime_writer`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`fp_recovery_operation`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`$op`", accounts.operationQuotaUpdater)
    grant("SELECT", s"$schema.`$quota`", accounts.operationQuotaUpdater)
    grant("UPDATE (quota_revision,used_records)", s"$schema.`$quota`", accounts.operationQuotaUpdater)
    grant("SELECT", "`mysql`.*", accounts.auditDefiner)
    grant("SHOW_ROUTINE", "*.*", accounts.auditDefiner)
    out.result()

  private def sqlLiteral(value: String): String = value.replace("'", "''")

  private def insertBefore(source: String, anchor: String, value: String): String =
    val index = source.indexOf(anchor)
    if index < 0 || source.indexOf(anchor, index + anchor.length) >= 0 then
      throw new SQLDataException(s"schema5 renderer requires one insertion anchor: $anchor")
    source.substring(0, index) + value + source.substring(index)

  private def parseTables(statements: Vector[String], inputs: Inputs): Vector[Table] =
    val pattern = "(?s)CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*".r
    val tableStatements = statements.collect { case statement @ pattern(name) => name -> statement }
    val oldByName = MySqlScriptSchemaProfile.tables.map(t => t.name -> t).toMap
    tableStatements.map { (name, statement) =>
      val sha = MySqlScriptSchemaProfile.sha256(statement.getBytes(StandardCharsets.UTF_8))
      oldByName.get(name) match
        case Some(old) if name == "fp_schema_manifest" =>
          old.copy(checks = old.checks.map { check =>
            if check == "format_version=4" then "format_version=5" else check
          }, statementSha256 = sha)
        case Some(old) if name == "fp_recovery_operation" =>
          old.copy(checks = old.checks.map { check =>
            if check == "OCTET_LENGTH(preimage_bytes)>0" then RecoveryRegisterPreimageCheck
            else if check.startsWith("step IN (") then RecoveryStepCheck
            else check
          }, statementSha256 = sha)
        case Some(old) => old.copy(statementSha256 = sha)
        case None if name == Definition.OperationTable =>
          Table(name, Definition.operationColumns, Definition.operationPrimaryKey, Definition.operationForeignKeys,
            Definition.operationChecks(inputs.deploymentBudget), Definition.operationIndexes, sha)
        case None if name == Definition.QuotaTable =>
          Table(name, Definition.quotaColumns, Definition.quotaPrimaryKey, Definition.quotaForeignKeys,
            Definition.quotaChecks, Definition.quotaIndexes, sha)
        case None => throw new SQLDataException(s"schema5 renderer found unexpected table $name")
    }

  private def parseGuards(statements: Vector[String]): Vector[MySqlScriptSchemaProfile.Guard] =
    val pattern = "(?s)CREATE DEFINER=.*? TRIGGER ([A-Za-z0-9_]+) BEFORE (INSERT|UPDATE|DELETE) ON `?([A-Za-z0-9_]+)`? .*".r
    statements.collect { case statement @ pattern(name, event, table) =>
      MySqlScriptSchemaProfile.Guard(name, table, event,
        MySqlScriptSchemaProfile.sha256(statement.getBytes(StandardCharsets.UTF_8)))
    }

  private def configurationHash(inputs: Inputs): String =
    val digest = MessageDigest.getInstance("SHA-256")
    def frame(bytes: Array[Byte]): Unit =
      digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array())
      digest.update(bytes)
    def text(value: String): Unit = frame(value.getBytes(StandardCharsets.UTF_8))
    def long(value: Long): Unit = digest.update(ByteBuffer.allocate(8).putLong(value).array())
    text("FloatingPod-MySQL-ScriptPublication1-DeploymentReceipt1-Configuration-v1")
    text(inputs.schema)
    inputs.accounts.all.foreach(a => { text(a.user); text(a.host) })
    frame(inputs.sourceLimitProfile.canonicalBytes())
    text(MySqlScriptSchemaProfile.Coverage)
    val pb = inputs.publicationBudget
    Vector(pb.evidenceBytes, pb.diagnosticsBytes, pb.configBytes, pb.requestBytes, pb.snapshotBytes,
      pb.receiptBytes, pb.rows, pb.retainedBytes, pb.packetBytes).foreach(long)
    long(inputs.deploymentBudget.evidenceBytes); long(inputs.deploymentBudget.diagnosticsBytes)
    long(inputs.quotaMaximumRecords)
    inputs.trustedAdministrativeProxyRows.foreach { p =>
      text(p.user); text(p.host); text(p.proxiedUser); text(p.proxiedHost); digest.update(if p.withGrant then 1.toByte else 0.toByte)
    }
    HexFormat.of().formatHex(digest.digest())
