package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import MySqlScriptSchemaProfile.{Guard, Table}

/** 复用旧表定义的显式 schema6 渲染器；不修改 schema5 资源或签发运行资格。 */
object MySqlDbFirstSchemaProfile:
  import MySqlDbFirstSchemaDefinition as Definition

  enum RenderMode:
    case CreateDatabaseAndSchema, ExistingDatabaseOnly

  final class Rendered private[mysql] (
      val inputs: MySqlDeploymentReceiptSchemaProfile.Inputs,
      val sql: String,
      val sha256: String,
      val tables: Vector[Table],
      val guards: Vector[Guard],
      val mode: RenderMode
  ):
    require(tables.size == 27 && tables.map(_.columns.size).sum == 480 && guards.size == 81,
      "schema6 requires the complete Full27 profile")
    /** schema5 的所有显式配置仍参与摘要，但 schema6 不复用其身份。 */
    private[mysql] lazy val profileIdentitySha256: String =
      val domain = mode match
        case RenderMode.CreateDatabaseAndSchema => "FloatingPod-MySQL-DbFirst1-Configuration-v1"
        case RenderMode.ExistingDatabaseOnly => "FloatingPod-MySQL-DbFirst1-ExistingDatabase-Configuration-v1"
      hash(domain + "\n" + inputs.profileIdentitySha256 + "\n" + sha256 + "\n")

  private[mysql] def expectedPrivilegeRows(rendered: Rendered): Vector[Vector[Vector[String]]] =
    require(rendered != null, "schema6 rendered grant source required")
    MySqlDeploymentReceiptSchemaProfile.expectedPrivilegeRows(rendered.sql, rendered.inputs, Definition.AuditRoutine)

  val TableNames: Vector[String] = MySqlDeploymentReceiptSchemaProfile.TableNames :+ Definition.SnapshotTable
  val GuardNames: Vector[String] = MySqlDeploymentReceiptSchemaProfile.GuardNames.map(_.replace("fp_s5_", "fp_s6_")) ++
    Vector("fp_s6_27_bi", "fp_s6_27_bu", "fp_s6_27_bd")
  private[mysql] val RecoveryStepCheck = s"step IN (${Definition.RecoverySteps.map(s => s"'$s'").mkString(",")})"

  def render(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): Rendered =
    require(inputs != null, "schema6 requires explicit accounts and budgets")
    val base = MySqlDeploymentReceiptSchemaProfile.render(inputs)
    // 账号名称由输入原样保留；仅改实际 trigger 标识，不能全局替换 fp_s5_ 账号前缀。
    var sql = base.sql.replace("TRIGGER fp_s5_", "TRIGGER fp_s6_")
      .replace(MySqlDeploymentReceiptSchemaDefinition.AuditRoutine, Definition.AuditRoutine)
      .replace("format_version=5", "format_version=6")
      .replace("format_version = 5", "format_version = 6")
      .replace("VALUES (1,'MYSQL',5,", "VALUES (1,'MYSQL',6,")
    sql = once(sql, MySqlDeploymentReceiptSchemaProfile.RecoveryStepCheck, RecoveryStepCheck)
    sql = once(sql, "-- Two control seeds only;", snapshotDdl(inputs) + "\n-- Two control seeds only;")
    val delimiter = "DELIMITER ;\n\n-- SOURCE v5：准确覆盖26张表、467列、78个守卫"
    sql = once(sql, delimiter, snapshotGuards(inputs) + "\n" + delimiter)
    sql = once(sql, "o.step IN ('RECALL_CHECKPOINT','ABORT_CLEANUP')",
      s"o.step IN ('RECALL_CHECKPOINT','ABORT_CLEANUP','${Definition.RecallStep}')")
    // 快照先写入，domain 再切为 inactive；最终 COMMITTED 再校验两者同属原 operation。
    sql = appendGuard(sql, "fp_s6_05_bu", domainRecallCheck)
    sql = appendGuard(sql, "fp_s6_04_bu", committedRecallCheck)
    sql = replaceInGuard(sql,"fp_s6_05_bu","o.step='ACTIVATE'",s"o.step IN ('ACTIVATE','${Definition.InitialActivationStep}')")
    sql = replaceInGuard(sql,"fp_s6_11_bu","o.step='ACTIVATE'",s"o.step IN ('ACTIVATE','${Definition.InitialActivationStep}')")
    sql = replaceInGuard(sql,"fp_s6_11_bu","o.step IN ('ACTIVATE','ABORT_CLEANUP')",
      s"o.step IN ('ACTIVATE','ABORT_CLEANUP','${Definition.InitialActivationStep}')")
    sql = appendGuard(sql,"fp_s6_05_bu",domainInitialActivationCheck)
    sql = appendGuard(sql,"fp_s6_04_bu",committedInitialActivationCheck)
    sql = appendGuard(sql,"fp_s6_05_bu",domainCheckpointCheck)
    sql = appendGuard(sql,"fp_s6_12_bu",locatorCheckpointCheck)
    sql = appendGuard(sql,"fp_s6_04_bu",committedCheckpointCheck)
    sql = appendGuard(sql,"fp_s6_04_bi",recallJournalGuard(inputs,true))
    sql = appendGuard(sql,"fp_s6_04_bu",recallJournalGuard(inputs,false))
    sql = appendGuard(sql,"fp_s6_04_bi",journalBoundRecallGuard(inputs))
    sql = appendGuard(sql,"fp_s6_04_bu",journalBoundRecallGuard(inputs))
    sql = appendGuard(sql,"fp_s6_04_bi",recallRefusalGuard(inputs,true))
    sql = appendGuard(sql,"fp_s6_04_bu",recallRefusalGuard(inputs,false))
    sql = replaceReservationRelease(sql)
    sql += "\n" + snapshotGrants(inputs)
    val statements = MySqlScriptSchemaProfile.statements(sql).fold(
      error => throw new SQLDataException("schema6 SQL framing failed: " + error), identity)
    val tablePattern = "(?s)CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*".r
    val baseTables = base.tables.map(t => t.name -> t).toMap
    val tables = statements.collect { case statement @ tablePattern(name) =>
      if name == Definition.SnapshotTable then
        Table(name, Definition.snapshotColumns, Definition.snapshotPrimaryKey, Definition.snapshotForeignKeys,
          Definition.snapshotChecks(inputs.sourceLimitProfile), Vector.empty, hash(statement))
      else
        val original = baseTables.getOrElse(name, throw new SQLDataException("unexpected schema6 table: " + name))
        val checks = original.checks.map { check =>
          if name == "fp_schema_manifest" && check == "format_version=5" then "format_version=6"
          else if name == "fp_recovery_operation" && check == MySqlDeploymentReceiptSchemaProfile.RecoveryStepCheck then RecoveryStepCheck
          else check
        }
        original.copy(checks = checks, statementSha256 = hash(statement))
    }
    val guardPattern = "(?s)CREATE DEFINER=.*? TRIGGER ([A-Za-z0-9_]+) BEFORE (INSERT|UPDATE|DELETE) ON `?([A-Za-z0-9_]+)`? .*".r
    val guards = statements.collect { case statement @ guardPattern(name, event, table) => Guard(name, table, event, hash(statement)) }
    if tables.map(_.name) != TableNames || guards.map(_.name) != GuardNames then
      throw new SQLDataException("schema6 generated relation/guard order differs from its closed profile")
    new Rendered(inputs, sql, hash(sql), tables, guards, RenderMode.CreateDatabaseAndSchema)

  /** 供已存在的 Database 使用；重新排列完整初始化区段并移除目标精确相等的 CREATE DATABASE 语句。 */
  private[mysql] def renderExistingDatabase(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): Rendered =
    val source = render(inputs)
    val expected = s"CREATE DATABASE `${inputs.schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"
    val statements = MySqlScriptSchemaProfile.statements(source.sql).fold(
      error => throw new SQLDataException("schema6 existing-database source framing failed: " + error), identity)
    if statements.count(_ == expected) != 1 then
      throw new SQLDataException("schema6 existing-database source requires one exact CREATE DATABASE statement")
    val oldProvenanceCheck = "source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT')"
    val managedProvenanceCheck = "source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT','MANAGED_INSTALL')"
    // 首启的独立预期不能从 innodb_default_row_format 学习；发布配方明确固定每张表的行格式。
    val normalizedTables = normalizeManagedTableOptions(source.sql)
    val inherited = once(normalizedTables, oldProvenanceCheck, managedProvenanceCheck)
    val managedSource = inherited
    val withoutDatabaseCreation = removeFramedStatement(managedSource, expected, "CREATE DATABASE")
    val quotaSeed = managedQuotaInitializationSql(inputs)
    val withoutQuotaSeed = removeFramedStatement(withoutDatabaseCreation, quotaSeed, "quota initialization")
    val sql = reorderManagedExistingSections(withoutQuotaSeed, inputs.schema, quotaSeed)
    val remaining = MySqlScriptSchemaProfile.statements(sql).fold(
      error => throw new SQLDataException("schema6 existing-database plan framing failed: " + error), identity)
    if remaining.contains(expected) || remaining.exists(_.startsWith("CREATE DATABASE ")) then
      throw new SQLDataException("schema6 existing-database plan still contains a database-creation statement")
    val tables = source.tables.map { table =>
      val matching = remaining.filter {
        statement => statement.startsWith(s"CREATE TABLE ${table.name} (") || statement.startsWith(s"CREATE TABLE `${table.name}` (")
      }
      if matching.size != 1 then throw new SQLDataException("schema6 existing-database table statement changed: " + table.name)
      val checks = if table.name == "fp_schema_manifest" then
        if table.checks.count(_ == oldProvenanceCheck) != 1 then
          throw new SQLDataException("schema6 manifest provenance constraint differs from its closed source")
        table.checks.map(value => if value == oldProvenanceCheck then managedProvenanceCheck else value)
      else table.checks
      table.copy(checks = checks, statementSha256 = hash(matching.head))
    }
    val guards = source.guards.map { guard =>
      val marker = s" TRIGGER ${guard.name} "
      val matching = remaining.filter(_.contains(marker))
      if matching.size != 1 then throw new SQLDataException("schema6 existing-database guard statement changed: " + guard.name)
      guard.copy(statementSha256 = hash(matching.head))
    }
    new Rendered(inputs, sql, hash(sql), tables, guards, RenderMode.ExistingDatabaseOnly)

  /** 只规范最终 renderer 中的27个真实表语句，并接受两个已验证的原始尾部形式。 */
  private def normalizeManagedTableOptions(sql: String): String =
    val statements = MySqlScriptSchemaProfile.statements(sql).fold(
      error => throw new SQLDataException("schema6 managed table-option source framing failed: " + error), identity)
    val tableStatements = statements.filter(_.startsWith("CREATE TABLE "))
    if tableStatements.size != TableNames.size then
      throw new SQLDataException("managed existing-database recipe requires exactly the closed Full27 table statements")
    val tablePattern = "(?s)^CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*$".r
    val namedTables = tableStatements.map { statement =>
      statement match
        case tablePattern(name) => name -> statement
        case _ => throw new SQLDataException("managed existing-database recipe contains an unrecognized table statement")
    }
    if namedTables.map(_._1) != TableNames then
      throw new SQLDataException("managed existing-database table statements differ from the exact Full27 source order")

    val bareTail = ") ENGINE=InnoDB"
    val explicitCharsetTail = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
    val managedTail = ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
    var bareCount = 0
    var explicitCharsetCount = 0
    var normalized = sql
    namedTables.foreach { (name, statement) =>
      val inheritedTail =
        if statement.endsWith(bareTail) then
          bareCount += 1
          bareTail
        else if statement.endsWith(explicitCharsetTail) then
          explicitCharsetCount += 1
          explicitCharsetTail
        else throw new SQLDataException("managed existing-database table has an unsupported inherited option tail: " + name)
      normalized = once(normalized, statement, statement.dropRight(inheritedTail.length) + managedTail)
    }
    if bareCount != 21 || explicitCharsetCount != 6 then
      throw new SQLDataException("managed existing-database source table-option forms differ from the verified Full27 profile")
    normalized

  /** 仅移除经 tokenizer 精确分帧的一个独立语句，并保留脚本其余字节不变。 */
  private def removeFramedStatement(sql: String, expected: String, label: String): String =
    val lines = sql.split("(?<=\\n)", -1).toVector
    val matching = lines.indices.filter { index =>
      val line = lines(index)
      val withoutEnding = line.stripSuffix("\n").stripSuffix("\r")
      withoutEnding.trim == expected + ";"
    }
    if matching.size != 1 then
      throw new SQLDataException(s"schema6 existing-database $label statement must occupy one exact source line")
    lines.patch(matching.head, Nil, 1).mkString

  private def managedQuotaInitializationSql(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): String =
    s"INSERT INTO `${inputs.schema}`.`${MySqlDeploymentReceiptSchemaDefinition.QuotaTable}` " +
      s"(namespace,max_records,used_records,quota_revision) VALUES " +
      s"(UNHEX('${MySqlDeploymentReceiptSchemaDefinition.NamespaceHex}'),${inputs.quotaMaximumRecords},0,0)"

  /** 保留原routine/授权块字节，将固定quota初始化和两条control seed移至其后；trigger仍保留原SQL mode。 */
  private def reorderManagedExistingSections(sql: String, schema: String, quotaSeed: String): String =
    val seedAnchor = "-- Two control seeds only; no fake B1/domain/operation/source history or READY seal."
    val triggerAnchor = "-- SOURCE v5：沿用原72个守卫并增加6个部署/配额守卫。"
    val routineAnchor = "-- SOURCE v5：准确覆盖26张表、467列、78个守卫与相同的18个结果集。"
    val seedStart = uniqueAnchor(sql, seedAnchor, "managed Full27 seeds")
    val triggerStart = uniqueAnchor(sql, triggerAnchor, "managed Full27 triggers")
    val routineStart = uniqueAnchor(sql, routineAnchor, "managed audit routine and grants")
    if seedStart >= triggerStart || triggerStart >= routineStart then
      throw new SQLDataException("managed schema source sections are not in the exact original order")

    val header = sql.substring(0, seedStart)
    val seedBlock = sql.substring(seedStart, triggerStart)
    val triggerBlock = sql.substring(triggerStart, routineStart)
    val routineGrantBlock = sql.substring(routineStart)
    def framed(source: String, label: String): Vector[String] = MySqlScriptSchemaProfile.statements(source).fold(
      error => throw new SQLDataException(s"managed schema $label framing failed: $error"), identity)
    val headerStatements = framed(header, "Full27 table prefix")
    val seedStatements = framed(seedBlock, "two-row seed block")
    val triggerStatements = framed(triggerBlock, "81-trigger block")
    val routineGrantStatements = framed(routineGrantBlock, "audit routine and grant block")

    val tablePattern = "(?s)^CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*$".r
    val headerTables = headerStatements.collect { case tablePattern(name) => name }
    if headerTables != TableNames then
      throw new SQLDataException("managed existing-database audit/grant block must follow all exact Full27 tables")
    if headerStatements.exists(statement => statement.startsWith("INSERT INTO ") || statement.startsWith("CREATE DEFINER=") ||
        statement.startsWith("GRANT ")) then
      throw new SQLDataException("managed existing-database preseed table section contains a seed, routine, trigger, or grant")

    val seedPattern = "(?s)^INSERT INTO `?([A-Za-z0-9_]+)`? \\(.*$".r
    val seedNames = seedStatements.collect { case seedPattern(name) => name }
    if seedNames != Vector("fp_schema_manifest", "fp_runtime_writer") || seedStatements.size != 2 then
      throw new SQLDataException("managed existing-database seed block differs from the exact two-row recipe")

    val triggerPattern = "(?s)^CREATE DEFINER=.*? TRIGGER ([A-Za-z0-9_]+) BEFORE (INSERT|UPDATE|DELETE) ON `?([A-Za-z0-9_]+)`? .*".r
    val triggerNames = triggerStatements.collect { case statement @ triggerPattern(name, _, _) => name }
    if triggerStatements.size != GuardNames.size + 2 || triggerStatements.headOption != Some(s"USE `$schema`") ||
        triggerStatements.lift(1) != Some("SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") ||
        triggerNames != GuardNames then
      throw new SQLDataException("managed existing-database trigger section lost its target, session mode, or exact 81 guards")

    val strictAll = "SET SESSION sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
    val strictAllAudit = "SET SESSION sql_mode = 'STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
    val setNames = "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin"
    if headerStatements.count(_ == strictAll) != 1 || headerStatements.count(_ == setNames) != 1 ||
        headerStatements.count(_ == s"USE `$schema`") != 1 then
      throw new SQLDataException("managed existing-database original Full27 strict-all and charset setup is missing")

    val routinePattern = "(?s)^CREATE DEFINER='[A-Za-z0-9_]{1,32}'@'[A-Za-z0-9_.:%-]{1,255}'\\s+PROCEDURE `([A-Za-z0-9_]+)`\\.`([A-Za-z0-9_]+)`\\(.*$".r
    val routineIndexes = routineGrantStatements.indices.filter(index => routineGrantStatements(index).matches(routinePattern.regex))
    if routineGrantStatements.take(2) != Vector(strictAllAudit, setNames) || routineIndexes.size != 1 ||
        routineGrantStatements.drop(routineIndexes.head + 1).isEmpty ||
        !routineGrantStatements.drop(routineIndexes.head + 1).forall(_.startsWith("GRANT ")) then
      throw new SQLDataException("managed existing-database audit routine and all following grants are not a closed movable block")

    val resetBeforeSeeds = s"$strictAll;\n$setNames;\n"
    val reordered = header + routineGrantBlock + resetBeforeSeeds + quotaSeed + ";\n\n" + seedBlock + triggerBlock
    val actual = framed(reordered, "reordered Full27 recipe")
    if actual.count(_ == strictAll) != 2 || actual.count(_ == strictAllAudit) != 1 ||
        actual.count(_ == setNames) != 3 ||
        actual.count(_ == "SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'") != 1 ||
        actual.count(_ == s"USE `$schema`") != 2 then
      throw new SQLDataException("managed existing-database source does not contain the exact seven session setup statements")
    reordered

  private def uniqueAnchor(sql: String, anchor: String, label: String): Int =
    val first = sql.indexOf(anchor)
    if first < 0 || sql.indexOf(anchor, first + anchor.length) >= 0 then
      throw new SQLDataException(s"managed existing-database source requires one exact $label anchor")
    first

  /** Fresh journal只接纳原请求，不以它作为domain/snapshot/reservation mutation的step。
    * 固定wire offsets对应独立V1/Fresh；完整Full27 delta及历史仍由strict reader/实际owner核验。
    */
  private def recallJournalGuard(inputs:MySqlDeploymentReceiptSchemaProfile.Inputs,insert:Boolean):String =
    def header(domain:String):Array[Byte]=
      val bytes=domain.getBytes(StandardCharsets.US_ASCII)
      java.nio.ByteBuffer.allocate(5+bytes.length).putInt(bytes.length).put(bytes).put(1.toByte).array()
    val hex=java.util.HexFormat.of()
    val requestPrefix=header(MySqlDbFirstRecallJournalRequest.Domain) :+ 1.toByte
    val scopePrefix=header(MySqlDbFirstRecallJournalCanonical.Scope)
    val profile=inputs.sourceLimitProfile.canonicalBytes()
    val hash=inputs.sourceLimitProfile.sha256()
    val tail=java.nio.ByteBuffer.allocate(8+profile.length+hash.length)
      .putInt(profile.length).put(profile).putInt(hash.length).put(hash).array()
    val minimum=199+MySqlDbFirstRecallJournalRequest.Domain.length+profile.length
    val fixed=s"""
     NEW.workflow_uuid<>NEW.operation_uuid
     AND LEFT(NEW.request_identity_bytes,${requestPrefix.length})=UNHEX('${hex.formatHex(requestPrefix)}')
     AND LEFT(NEW.scope_bytes,${scopePrefix.length})=UNHEX('${hex.formatHex(scopePrefix)}')
     AND SUBSTRING(NEW.request_identity_bytes,47,16)=NEW.boot_uuid
     AND SUBSTRING(NEW.request_identity_bytes,95,16)=NEW.boot_uuid
     AND SUBSTRING(NEW.request_identity_bytes,63,8)=UNHEX(LPAD(HEX(NEW.boot_generation),16,'0'))
     AND SUBSTRING(NEW.request_identity_bytes,111,8)=SUBSTRING(NEW.request_identity_bytes,63,8)
     AND SUBSTRING(NEW.request_identity_bytes,71,8)=SUBSTRING(NEW.request_identity_bytes,119,8)
     AND SUBSTRING(NEW.request_identity_bytes,71,8)<>UNHEX(REPEAT('00',8))
     AND ASCII(SUBSTRING(NEW.request_identity_bytes,71,1))<128
     AND SUBSTRING(NEW.request_identity_bytes,79,16)=NEW.operation_uuid
     AND SUBSTRING(NEW.request_identity_bytes,127,16)=NEW.workflow_uuid
     AND SUBSTRING(NEW.request_identity_bytes,143,16)=NEW.binding_uuid
     AND SUBSTRING(NEW.request_identity_bytes,175,16)=NEW.pod_uuid
     AND SUBSTRING(NEW.request_identity_bytes,191,8)=UNHEX(LPAD(HEX(NEW.expected_domain_version),16,'0'))
     AND NEW.expected_domain_version>=0 AND NEW.expected_domain_version<9223372036854775807
     AND ((OCTET_LENGTH(NEW.request_identity_bytes)=$minimum AND SUBSTRING(NEW.request_identity_bytes,199,1)=UNHEX('00'))
      OR (OCTET_LENGTH(NEW.request_identity_bytes)=${minimum+16} AND SUBSTRING(NEW.request_identity_bytes,199,1)=UNHEX('01')
       AND SUBSTRING(NEW.request_identity_bytes,200,16)<>UNHEX(REPEAT('00',16))))
     AND RIGHT(NEW.request_identity_bytes,${tail.length})=UNHEX('${hex.formatHex(tail)}')
     AND NEW.source_unit_uuid IS NULL AND NEW.forest_uuid IS NULL AND NEW.expected_source_revision IS NULL
     AND NEW.reserve_operation_uuid IS NULL AND NEW.previous_step_operation_uuid IS NULL
     AND NEW.settled_by_operation_uuid IS NULL AND NEW.original_diagnostic_identity IS NULL AND NEW.original_diagnostic_sha256 IS NULL
     AND EXISTS(SELECT 1 FROM floating_pod_state d JOIN physical_recovery_locator l
      ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid
      WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.owner_uuid IS NOT NULL
       AND d.owner_uuid=SUBSTRING(NEW.request_identity_bytes,159,16)
       AND d.is_active=1 AND d.state_version=NEW.expected_domain_version
       AND l.locator_state='ROW_ACTIVE' AND l.domain_version=d.state_version
       AND l.locator_revision=NEW.expected_locator_revision AND l.current_attempt_uuid=NEW.attempt_uuid
       AND (l.current_footprint_uuid <=> NEW.footprint_uuid))
     AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation c
      WHERE c.binding_uuid=NEW.binding_uuid AND c.operation_uuid=NEW.workflow_uuid)
"""
    val state=if insert then "NEW.operation_state='RESERVED'" else "OLD.operation_state='RESERVED' AND NEW.operation_state='COMMITTED'"
    val duplicate=if !insert then "" else s"""
  IF EXISTS(SELECT 1 FROM fp_recovery_operation j WHERE j.binding_uuid=NEW.binding_uuid
   AND j.step='${Definition.RecallJournalStep}' AND (j.workflow_uuid=NEW.workflow_uuid
    OR (SUBSTRING(NEW.request_identity_bytes,199,1)=UNHEX('01')
     AND SUBSTRING(j.request_identity_bytes,199,1)=UNHEX('01')
     AND SUBSTRING(j.request_identity_bytes,159,16)=SUBSTRING(NEW.request_identity_bytes,159,16)
     AND SUBSTRING(j.request_identity_bytes,200,16)=SUBSTRING(NEW.request_identity_bytes,200,16))
    OR (j.pod_uuid=NEW.pod_uuid AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation c
     WHERE c.binding_uuid=j.binding_uuid AND c.operation_uuid=j.workflow_uuid AND c.operation_state='COMMITTED'
      AND c.step IN ('${Definition.RecallStep}','DB_FIRST_RECALL_REFUSED'))))) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first journal retains exact request or unresolved pod responsibility'; END IF;
"""
    s"""
 IF NEW.step='${Definition.RecallJournalStep}' THEN
  IF NOT (($state AND $fixed) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first Fresh journal lacks exact original request, active owner or scope'; END IF;
$duplicate
 END IF;
"""

  /** V2/V3固定owner=Some(actor)后的BE wire布局与原J footer；V3另保原C全tag；不由此签发freeze/C执行资格。
    * 更新仍先经过原不可变字段/终局转换守卫；这里只补齐新请求族和提交快照关联。
    */
  private def journalBoundRecallGuard(inputs:MySqlDeploymentReceiptSchemaProfile.Inputs):String =
    val hex=java.util.HexFormat.of()
    def header(domain:String):Array[Byte]=
      val bytes=domain.getBytes(StandardCharsets.US_ASCII)
      java.nio.ByteBuffer.allocate(5+bytes.length).putInt(bytes.length).put(bytes).put(1.toByte).array()
    def requestPrefix(kind:String):Array[Byte]=
      val bytes=kind.getBytes(StandardCharsets.US_ASCII)
      header("FP_MYSQL_REPOSITORY_REQUEST_BE_V1") ++
        java.nio.ByteBuffer.allocate(8+bytes.length).putInt(1).putInt(bytes.length).put(bytes).array()
    val v1=requestPrefix("DB_FIRST_RECALL_V1")
    val v2=requestPrefix("DB_FIRST_RECALL_V2")
    val v3=requestPrefix("DB_FIRST_RECALL_V3")
    val recovery=s"LEFT(NEW.request_identity_bytes,${v3.length})=UNHEX('${hex.formatHex(v3)}')"
    val scope=header(MySqlDbFirstRecallCanonical.JournalScope)
    val recoveryScope=header(MySqlDbFirstRecallCanonical.RecoveryScope)
    require(v2.length==v3.length && scope.length==recoveryScope.length,"V3 fixed prefix offsets must match V2")
    val profile=inputs.sourceLimitProfile.canonicalBytes()
    val profileFrame=java.nio.ByteBuffer.allocate(8+profile.length+32).putInt(profile.length).put(profile)
      .putInt(32).put(inputs.sourceLimitProfile.sha256()).array()
    // 1-based SQL offsets与真实repository encoder逐字段测试绑定；world长度为UTF8字节数。
    val worldLength=s"CONV(HEX(SUBSTRING(NEW.request_identity_bytes,${270+profile.length},4)),16,10)"
    val footerLength=s"(IF(SUBSTRING(j.request_identity_bytes,199,1)=UNHEX('01'),117,101)+IF($recovery,48,0))"
    val footer=s"""CONCAT(SUBSTRING(j.request_identity_bytes,47,48),SUBSTRING(j.request_identity_bytes,159,16),
       SUBSTRING(j.request_identity_bytes,199,1),
       IF(SUBSTRING(j.request_identity_bytes,199,1)=UNHEX('01'),SUBSTRING(j.request_identity_bytes,200,16),UNHEX('')),
       UNHEX('00000020'),j.request_identity_sha256,
       IF($recovery,SUBSTRING(j.request_identity_bytes,95,48),UNHEX('')))"""
    s"""
 IF NEW.step='${Definition.RecallStep}' THEN
  IF LEFT(NEW.request_identity_bytes,${v2.length})=UNHEX('${hex.formatHex(v2)}') OR $recovery THEN
   IF NOT ((NEW.workflow_uuid=NEW.operation_uuid AND NEW.previous_step_operation_uuid IS NOT NULL
    AND NEW.source_unit_uuid IS NULL AND NEW.forest_uuid IS NULL AND NEW.expected_source_revision IS NULL
    AND NEW.reserve_operation_uuid IS NULL
    AND LEFT(NEW.scope_bytes,${scope.length})=IF($recovery,UNHEX('${hex.formatHex(recoveryScope)}'),UNHEX('${hex.formatHex(scope)}'))
    AND NEW.expected_domain_version>=0 AND NEW.expected_domain_version<9223372036854775807
    AND SUBSTRING(NEW.request_identity_bytes,65,16)=NEW.boot_uuid
    AND SUBSTRING(NEW.request_identity_bytes,81,8)=UNHEX(LPAD(HEX(NEW.boot_generation),16,'0'))
    AND SUBSTRING(NEW.request_identity_bytes,97,16)=NEW.operation_uuid
    AND SUBSTRING(NEW.request_identity_bytes,113,16)=NEW.binding_uuid
    AND SUBSTRING(NEW.request_identity_bytes,129,16)=NEW.pod_uuid
    AND SUBSTRING(NEW.request_identity_bytes,145,1)=UNHEX('01')
    AND SUBSTRING(NEW.request_identity_bytes,162,16)=NEW.attempt_uuid
    AND SUBSTRING(NEW.request_identity_bytes,178,8)=UNHEX(LPAD(HEX(NEW.expected_domain_version),16,'0'))
    AND SUBSTRING(NEW.request_identity_bytes,186,4)=UNHEX(LPAD(HEX(${DbFirstPodSnapshot.FORMAT_VERSION}),8,'0'))
    AND CONV(HEX(SUBSTRING(NEW.request_identity_bytes,190,4)),16,10) BETWEEN 1 AND ${Definition.maximumNbtBytes(inputs.sourceLimitProfile)}
    AND SUBSTRING(NEW.request_identity_bytes,194,4)=UNHEX('00000020')
    AND SUBSTRING(NEW.request_identity_bytes,230,${profileFrame.length})=UNHEX('${hex.formatHex(profileFrame)}')
    AND $worldLength BETWEEN 1 AND ${inputs.publicationBudget.snapshotBytes}
    AND EXISTS(SELECT 1 FROM fp_recovery_operation j WHERE j.binding_uuid=NEW.binding_uuid
     AND j.operation_uuid=NEW.previous_step_operation_uuid AND j.workflow_uuid=NEW.operation_uuid
     AND j.step='${Definition.RecallJournalStep}' AND j.operation_state='COMMITTED'
     AND j.pod_uuid=NEW.pod_uuid AND j.attempt_uuid=NEW.attempt_uuid AND (j.footprint_uuid <=> NEW.footprint_uuid)
     AND j.expected_domain_version=NEW.expected_domain_version AND j.expected_locator_revision=NEW.expected_locator_revision
     AND (($recovery AND SUBSTRING(NEW.request_identity_bytes,65,48)<>SUBSTRING(j.request_identity_bytes,95,48))
       OR (NOT ($recovery) AND j.boot_uuid=NEW.boot_uuid AND j.boot_generation=NEW.boot_generation
        AND SUBSTRING(NEW.request_identity_bytes,65,48)=SUBSTRING(j.request_identity_bytes,95,48)))
     AND SUBSTRING(NEW.request_identity_bytes,146,16)=SUBSTRING(j.request_identity_bytes,159,16)
     AND SUBSTRING(j.request_identity_bytes,159,16)<>UNHEX(REPEAT('00',16))
     AND SUBSTRING(j.request_identity_bytes,199,1) IN (UNHEX('00'),UNHEX('01'))
     AND OCTET_LENGTH(NEW.request_identity_bytes)=${309+profile.length}+$worldLength+$footerLength
     AND RIGHT(NEW.request_identity_bytes,$footerLength)=$footer)
    AND (NEW.operation_state<>'COMMITTED' OR EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s
     WHERE s.binding_uuid=NEW.binding_uuid AND s.pod_uuid=NEW.pod_uuid
      AND s.last_mutation_operation_uuid=NEW.operation_uuid AND s.attempt_uuid=NEW.attempt_uuid
      AND s.owner_uuid=SUBSTRING(NEW.request_identity_bytes,146,16)
      AND s.captured_state_version=NEW.expected_domain_version
      AND s.committed_state_version=NEW.expected_domain_version+1
      AND SUBSTRING(NEW.request_identity_bytes,186,4)=UNHEX(LPAD(HEX(s.snapshot_format_version),8,'0'))
      AND SUBSTRING(NEW.request_identity_bytes,190,4)=UNHEX(LPAD(HEX(OCTET_LENGTH(s.nbt_bytes)),8,'0'))
      AND s.nbt_sha256=SUBSTRING(NEW.request_identity_bytes,198,32)))) <=> TRUE) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first V2/V3 requires exact original J/C request and committed snapshot'; END IF;
  ELSEIF LEFT(NEW.request_identity_bytes,${v1.length})=UNHEX('${hex.formatHex(v1)}') THEN
   IF NOT ((NEW.previous_step_operation_uuid IS NULL AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation j
    WHERE j.binding_uuid=NEW.binding_uuid AND j.step='${Definition.RecallJournalStep}'
     AND (j.workflow_uuid=NEW.operation_uuid OR (j.pod_uuid=NEW.pod_uuid
      AND NOT EXISTS(SELECT 1 FROM fp_recovery_operation c WHERE c.binding_uuid=j.binding_uuid
       AND c.operation_uuid=j.workflow_uuid AND c.operation_state='COMMITTED'
       AND c.step IN ('${Definition.RecallStep}','DB_FIRST_RECALL_REFUSED')))))) <=> TRUE) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first V1 cannot bypass original journal responsibility'; END IF;
  ELSE
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first recall requires an explicit supported request version';
  END IF;
 END IF;
"""

  /** 原C未应用结果的独立wire协议；原J/C身份不可被当前结算scope替换。
    * guard只约束持久行/当前fence，旧executor排除与未应用资格仍须实际受限owner证明。
    */
  private def recallRefusalGuard(inputs:MySqlDeploymentReceiptSchemaProfile.Inputs,insert:Boolean):String =
    def header(domain:String,version:Int=1):Array[Byte]=
      val bytes=domain.getBytes(StandardCharsets.US_ASCII)
      java.nio.ByteBuffer.allocate(5+bytes.length).putInt(bytes.length).put(bytes).put(version.toByte).array()
    val hex=java.util.HexFormat.of()
    val prefix=header(MySqlDbFirstRecallRefusalRequest.Domain) :+ 1.toByte
    val reasonPrefix=header(MySqlDbFirstRecallRefusalRequest.ReasonDomain,2) :+ 1.toByte
    require(reasonPrefix.length==prefix.length,"refusal fixed identity offsets must remain unchanged")
    val legacy=s"LEFT(NEW.request_identity_bytes,${prefix.length})=UNHEX('${hex.formatHex(prefix)}')"
    val reasoned=s"LEFT(NEW.request_identity_bytes,${reasonPrefix.length})=UNHEX('${hex.formatHex(reasonPrefix)}')"
    val scope=header(MySqlDbFirstRecallRefusalCanonical.Scope)
    val journalPrefix=header(MySqlDbFirstRecallJournalRequest.Domain) :+ 1.toByte
    val journalScope=header(MySqlDbFirstRecallJournalCanonical.Scope)
    val profile=inputs.sourceLimitProfile.canonicalBytes();val digest=inputs.sourceLimitProfile.sha256()
    val tail=java.nio.ByteBuffer.allocate(8+profile.length+digest.length).putInt(profile.length).put(profile)
      .putInt(digest.length).put(digest).array()
    val minimum=283+MySqlDbFirstRecallRefusalRequest.Domain.length+profile.length
    val start=prefix.length // 下面request内部offset为0-based，转SQL时统一加1。
    def at(offset:Int,length:Int)=s"SUBSTRING(NEW.request_identity_bytes,${start+offset+1},$length)"
    def scopeAt(offset:Int,length:Int)=s"SUBSTRING(NEW.scope_bytes,${scope.length+offset+1},$length)"
    val flag=at(200,1)
    val clientLength=s"IF($flag=UNHEX('01'),16,0)"
    val journalDigest=s"SUBSTRING(NEW.request_identity_bytes,${start+205}+$clientLength+1,32)"
    val state=if insert then "NEW.operation_state='RESERVED'" else "OLD.operation_state='RESERVED' AND NEW.operation_state='COMMITTED'"
    s"""
 IF NEW.step='${Definition.RecallRefusedStep}' THEN
  IF NOT (($state AND NEW.operation_uuid=NEW.workflow_uuid
   AND NEW.previous_step_operation_uuid IS NOT NULL AND NEW.previous_step_operation_uuid<>NEW.operation_uuid
   AND NEW.expected_domain_version>=0 AND NEW.expected_domain_version<9223372036854775807
   AND ($legacy OR $reasoned)
   AND LEFT(NEW.scope_bytes,${scope.length})=UNHEX('${hex.formatHex(scope)}')
   AND ${at(0,16)}=NEW.boot_uuid
   AND ${at(16,8)}=UNHEX(LPAD(HEX(NEW.boot_generation),16,'0'))
   AND ${at(24,8)}<>UNHEX(REPEAT('00',8)) AND ASCII(${at(24,1)})<128
   AND ${at(32,16)}=NEW.operation_uuid AND ${at(80,16)}=NEW.operation_uuid
   AND ${at(48,32)}=${at(96,32)} AND ${at(48,16)}<>UNHEX(REPEAT('00',16))
   AND ${at(64,8)}<>UNHEX(REPEAT('00',8)) AND ASCII(${at(64,1)})<128
   AND ${at(72,8)}<>UNHEX(REPEAT('00',8)) AND ASCII(${at(72,1)})<128
   AND ${at(128,16)}=NEW.previous_step_operation_uuid
   AND ${at(144,16)}=NEW.binding_uuid AND ${at(160,16)}<>UNHEX(REPEAT('00',16))
   AND ${at(176,16)}=NEW.pod_uuid
   AND ${at(192,8)}=UNHEX(LPAD(HEX(NEW.expected_domain_version),16,'0'))
   AND (($legacy AND ((OCTET_LENGTH(NEW.request_identity_bytes)=$minimum AND $flag=UNHEX('00'))
    OR (OCTET_LENGTH(NEW.request_identity_bytes)=${minimum+16} AND $flag=UNHEX('01') AND ${at(201,16)}<>UNHEX(REPEAT('00',16)))))
    OR ($reasoned AND ((OCTET_LENGTH(NEW.request_identity_bytes)=${minimum+1} AND $flag=UNHEX('00'))
     OR (OCTET_LENGTH(NEW.request_identity_bytes)=${minimum+17} AND $flag=UNHEX('01') AND ${at(201,16)}<>UNHEX(REPEAT('00',16))))
     AND SUBSTRING(NEW.request_identity_bytes,${start+237}+$clientLength+1,1) IN
      (UNHEX('01'),UNHEX('02'),UNHEX('03'),UNHEX('04'),UNHEX('05'),UNHEX('06'))))
   AND SUBSTRING(NEW.request_identity_bytes,${start+201}+$clientLength+1,4)=UNHEX('00000020')
   AND RIGHT(NEW.request_identity_bytes,${tail.length})=UNHEX('${hex.formatHex(tail)}')
   AND ${scopeAt(0,48)}=${at(0,48)} AND ${scopeAt(48,16)}=NEW.binding_uuid
   AND ${scopeAt(64,16)}=NEW.pod_uuid AND ${scopeAt(80,16)}=NEW.previous_step_operation_uuid
   AND ${scopeAt(96,16)}=NEW.connection_generation_uuid
   AND ${scopeAt(112,8)}=UNHEX(LPAD(HEX(NEW.connection_id),16,'0'))
   AND ${scopeAt(120,8)}=UNHEX(LPAD(HEX(NEW.fence_epoch),16,'0'))
   AND NEW.source_unit_uuid IS NULL AND NEW.forest_uuid IS NULL AND NEW.expected_source_revision IS NULL
   AND NEW.reserve_operation_uuid IS NULL AND NEW.settled_by_operation_uuid IS NULL
   AND NEW.original_diagnostic_identity IS NULL AND NEW.original_diagnostic_sha256 IS NULL
   AND EXISTS(SELECT 1 FROM fp_recovery_operation j WHERE j.binding_uuid=NEW.binding_uuid
    AND j.operation_uuid=NEW.previous_step_operation_uuid AND j.workflow_uuid=NEW.operation_uuid
    AND j.step='${Definition.RecallJournalStep}' AND j.operation_state='COMMITTED'
    AND j.pod_uuid=NEW.pod_uuid AND j.attempt_uuid=NEW.attempt_uuid AND (j.footprint_uuid <=> NEW.footprint_uuid)
    AND j.expected_domain_version=NEW.expected_domain_version AND j.expected_locator_revision=NEW.expected_locator_revision
    AND LEFT(j.request_identity_bytes,${journalPrefix.length})=UNHEX('${hex.formatHex(journalPrefix)}')
    AND LEFT(j.scope_bytes,${journalScope.length})=UNHEX('${hex.formatHex(journalScope)}')
    AND RIGHT(j.request_identity_bytes,${tail.length})=UNHEX('${hex.formatHex(tail)}')
    AND OCTET_LENGTH(j.request_identity_bytes)=${199+MySqlDbFirstRecallJournalRequest.Domain.length+profile.length}+$clientLength
    AND j.request_identity_sha256=UNHEX(SHA2(j.request_identity_bytes,256))
    AND SUBSTRING(j.scope_bytes,${journalScope.length+1},48)=SUBSTRING(j.request_identity_bytes,47,48)
    AND j.boot_uuid=${at(96,16)} AND j.boot_generation=CONV(HEX(${at(112,8)}),16,10)
    AND ${at(48,48)}=SUBSTRING(j.request_identity_bytes,95,48)
    AND ${at(96,48)}=SUBSTRING(j.request_identity_bytes,47,48)
    AND ${at(144,16)}=SUBSTRING(j.request_identity_bytes,143,16)
    AND ${at(160,16)}=SUBSTRING(j.request_identity_bytes,159,16)
    AND ${at(176,16)}=SUBSTRING(j.request_identity_bytes,175,16)
    AND ${at(192,8)}=SUBSTRING(j.request_identity_bytes,191,8)
    AND $flag=SUBSTRING(j.request_identity_bytes,199,1)
    AND ($flag=UNHEX('00') OR ${at(201,16)}=SUBSTRING(j.request_identity_bytes,200,16))
    AND $journalDigest=j.request_identity_sha256)) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first refusal requires exact original J/C identity and current settlement scope'; END IF;
 END IF;
"""

  private def snapshotDdl(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): String =
    val columns = Definition.snapshotColumns.map(c => s" ${c.name} ${c.declaration}")
    val key = s" PRIMARY KEY (${Definition.snapshotPrimaryKey.mkString(",")})"
    val checks = Definition.snapshotChecks(inputs.sourceLimitProfile).zipWithIndex.map { (check, i) =>
      s" CONSTRAINT ck_s6_27_${i + 1} CHECK ($check)"
    }
    val references = Definition.snapshotForeignKeys.map(f =>
      s" CONSTRAINT ${f.name} FOREIGN KEY (${f.columns.mkString(",")}) REFERENCES ${f.target} (${f.targetColumns.mkString(",")})")
    s"CREATE TABLE ${Definition.SnapshotTable} (\n${(columns ++ Vector(key) ++ checks ++ references).mkString(",\n")}\n) ENGINE=InnoDB;\n"

  private def snapshotGuards(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): String =
    val definer = inputs.accounts.guardDefiner.grantee
    val common = s"""
 IF NOT ((EXISTS(SELECT 1 FROM fp_schema_manifest m
  JOIN physical_recovery_binding b ON b.binding_uuid=m.expected_binding_uuid
  JOIN fp_runtime_writer w ON w.binding_uuid=b.binding_uuid
  JOIN fp_recovery_operation o ON o.binding_uuid=b.binding_uuid
  JOIN floating_pod_state d ON d.uuid=o.pod_uuid AND d.binding_uuid=o.binding_uuid
  JOIN physical_recovery_locator l ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid
  WHERE m.singleton_fixed1=1 AND m.format_version=6 AND m.bootstrap_state='READY'
   AND b.format_version=2 AND b.cutover_state='CERTIFIED' AND b.binding_uuid=NEW.binding_uuid
   AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step IN ('${Definition.RecallStep}','${Definition.InitialActivationStep}','${Definition.CheckpointStep}')
   AND o.operation_state='RESERVED' AND o.pod_uuid=NEW.pod_uuid AND o.attempt_uuid=NEW.attempt_uuid
   AND o.expected_domain_version=NEW.captured_state_version
   AND o.boot_uuid=w.owner_boot_uuid AND o.fence_epoch=w.writer_epoch
   AND o.connection_id=CONNECTION_ID() AND o.fixed_lease_until=w.lease_until AND w.lease_until>UTC_TIMESTAMP(6)
   AND d.state_version=NEW.captured_state_version AND (d.owner_uuid <=> NEW.owner_uuid)
   AND ((o.step='${Definition.RecallStep}' AND d.is_active=1)
    OR (o.step='${Definition.CheckpointStep}' AND d.is_active=1 AND l.locator_state='ROW_ACTIVE')
    OR (o.step='${Definition.InitialActivationStep}' AND d.is_active=0 AND d.state_version=0
     AND d.owner_uuid IS NULL AND d.operating_mode='WORLD_IDLE' AND l.locator_state='DEBUG_ITEM_CREATE_PREPARED'
     AND EXISTS(SELECT 1 FROM physical_recovery_intent i JOIN fp_recovery_operation p
      ON p.binding_uuid=i.binding_uuid AND p.operation_uuid=i.prepare_operation_uuid
      WHERE i.binding_uuid=d.binding_uuid AND i.pod_uuid=d.uuid AND i.attempt_uuid=NEW.attempt_uuid
       AND i.phase='PREPARED' AND i.origin='DEBUG_ITEM_CREATE' AND i.expected_domain_version=0
       AND i.expected_owner_uuid IS NULL AND i.create_operation_uuid IS NOT NULL
       AND p.step='REARM' AND p.operation_state='COMMITTED' AND p.pod_uuid=d.uuid AND p.attempt_uuid=i.attempt_uuid
       AND d.last_mutation_operation_uuid=p.operation_uuid AND l.last_mutation_operation_uuid=p.operation_uuid)))
   AND l.current_attempt_uuid=NEW.attempt_uuid AND l.domain_version=d.state_version
   AND m.limit_profile_bytes=NEW.limit_profile_bytes AND m.limit_profile_sha256=NEW.limit_profile_sha256
   AND EXISTS(SELECT 1 FROM fp_recovery_reservation r WHERE r.binding_uuid=o.binding_uuid
    AND r.operation_uuid=o.operation_uuid AND r.resource_kind='POD' AND r.resource_pod_uuid=o.pod_uuid
    AND r.reservation_state='HELD'))) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first snapshot lacks its exact active domain, reserved operation or live writer'; END IF;
"""
    val insert = s"""CREATE DEFINER=$definer TRIGGER fp_s6_27_bi BEFORE INSERT ON `${Definition.SnapshotTable}` FOR EACH ROW
BEGIN
$common
END$$$$
"""
    val update = s"""CREATE DEFINER=$definer TRIGGER fp_s6_27_bu BEFORE UPDATE ON `${Definition.SnapshotTable}` FOR EACH ROW
BEGIN
$common
 IF NOT (((OLD.binding_uuid <=> NEW.binding_uuid) AND (OLD.pod_uuid <=> NEW.pod_uuid)
  AND NEW.captured_state_version>=OLD.committed_state_version
  AND NEW.last_mutation_operation_uuid<>OLD.last_mutation_operation_uuid) <=> TRUE) THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first snapshot replacement changed identity or regressed committed history'; END IF;
END$$$$
"""
    val delete = s"""CREATE DEFINER=$definer TRIGGER fp_s6_27_bd BEFORE DELETE ON `${Definition.SnapshotTable}` FOR EACH ROW
BEGIN
 SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first recovery snapshot deletion is forbidden';
END$$$$
"""
    Vector(insert, update, delete).mkString("\n")

  private val originalReleaseBranch = "OLD.reservation_state='HELD' AND NEW.reservation_state='RELEASED' AND NEW.operation_uuid=OLD.operation_uuid AND NEW.workflow_uuid=OLD.workflow_uuid AND o.operation_uuid=OLD.operation_uuid AND o.workflow_uuid=OLD.workflow_uuid AND o.operation_state='COMMITTED' AND o.step='DOMAIN_MUTATION'"

  // 绑定新prepare规范域及版本；旧REARM不能借用此释放例外。
  private val preparationScopePrefix =
    val domain=MySqlDbFirstInitialPreparationCanonical.Scope.getBytes(StandardCharsets.US_ASCII)
    val bytes=java.nio.ByteBuffer.allocate(5+domain.length).putInt(domain.length).put(domain).put(1.toByte).array()
    java.util.HexFormat.of().formatHex(bytes)
  private val preparationRelease = s"""(o.step='REARM' AND o.workflow_uuid=o.operation_uuid
   AND LEFT(o.scope_bytes,${preparationScopePrefix.length/2})=UNHEX('$preparationScopePrefix')
   AND o.expected_domain_version=0 AND o.expected_locator_revision=0
   AND o.source_unit_uuid IS NULL AND o.forest_uuid IS NULL AND o.reserve_operation_uuid IS NULL
   AND o.commit_witness_bytes IS NOT NULL AND o.commit_witness_sha256 IS NOT NULL AND o.result_at IS NOT NULL
   AND NEW.resource_kind='POD' AND NEW.resource_unit_uuid IS NULL AND NEW.resource_pod_uuid=o.pod_uuid
   AND EXISTS(SELECT 1 FROM floating_pod_state d
    JOIN physical_recovery_intent i ON i.binding_uuid=d.binding_uuid AND i.pod_uuid=d.uuid AND i.attempt_uuid=o.attempt_uuid
    JOIN physical_recovery_locator l ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid
    JOIN physical_recovery_footprint f ON f.binding_uuid=d.binding_uuid AND f.pod_uuid=d.uuid
      AND f.attempt_uuid=o.attempt_uuid AND f.footprint_uuid=o.footprint_uuid
    WHERE d.binding_uuid=o.binding_uuid AND d.uuid=o.pod_uuid AND d.is_active=0 AND d.state_version=0
     AND d.owner_uuid IS NULL AND d.operating_mode='WORLD_IDLE' AND d.last_mutation_operation_uuid=o.operation_uuid
     AND i.phase='PREPARED' AND i.origin='DEBUG_ITEM_CREATE' AND i.expected_domain_version=0
     AND i.expected_owner_uuid IS NULL AND i.activation_operation_uuid IS NULL AND i.committed_domain_version IS NULL
     AND i.target_footprint_uuid=o.footprint_uuid AND i.prepare_operation_uuid=o.operation_uuid AND i.intent_revision=0
     AND i.last_mutation_operation_uuid=o.operation_uuid
     AND l.locator_state='DEBUG_ITEM_CREATE_PREPARED' AND l.domain_version=0 AND l.locator_revision=0
     AND l.current_attempt_uuid=o.attempt_uuid AND l.current_footprint_uuid=o.footprint_uuid
     AND l.last_mutation_operation_uuid=o.operation_uuid
     AND f.purpose='CREATE_TARGET' AND f.origin_version=0 AND f.seal_state='SEALED' AND f.created_operation_uuid=o.operation_uuid
     AND NOT EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s WHERE s.binding_uuid=d.binding_uuid AND s.pod_uuid=d.uuid)))"""

  /** R只退出数据库排他；原Entry/freeze及UNKNOWN责任不由此guard完成。 */
  private val dbFirstReleaseBranch = s"""OLD.reservation_state='HELD' AND NEW.reservation_state='RELEASED' AND NEW.operation_uuid=OLD.operation_uuid AND NEW.workflow_uuid=OLD.workflow_uuid AND o.operation_uuid=OLD.operation_uuid AND o.workflow_uuid=OLD.workflow_uuid AND o.operation_state='COMMITTED' AND (o.step='DOMAIN_MUTATION' OR $preparationRelease OR (o.step IN ('${Definition.RecallStep}','${Definition.InitialActivationStep}','${Definition.CheckpointStep}')
   AND NEW.resource_kind='POD' AND NEW.resource_unit_uuid IS NULL AND NEW.resource_pod_uuid=o.pod_uuid
   AND EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s JOIN floating_pod_state d
    ON d.binding_uuid=s.binding_uuid AND d.uuid=s.pod_uuid
    WHERE s.binding_uuid=o.binding_uuid AND s.pod_uuid=o.pod_uuid AND s.attempt_uuid=o.attempt_uuid
     AND s.captured_state_version=o.expected_domain_version AND s.committed_state_version=o.expected_domain_version+1
     AND s.last_mutation_operation_uuid=o.operation_uuid AND d.last_mutation_operation_uuid=o.operation_uuid
     AND d.is_active=IF(o.step='${Definition.RecallStep}',0,1)
     AND d.state_version=s.committed_state_version AND (d.owner_uuid <=> s.owner_uuid))))"""

  private def replaceReservationRelease(sql: String): String =
    val marker=" TRIGGER fp_s6_08_bu "
    val start=sql.indexOf(marker)
    if start < 0 || sql.indexOf(marker,start+marker.length) >= 0 then
      throw new SQLDataException("schema6 requires one exact reservation update guard")
    val end=sql.indexOf("END$$",start)
    if end < 0 then throw new SQLDataException("schema6 reservation update terminator missing")
    val guard=sql.substring(start,end)
    val patched=once(guard,originalReleaseBranch,dbFirstReleaseBranch)
    sql.substring(0,start)+patched+sql.substring(end)

  private val domainRecallCheck = s"""
 IF EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid
  AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='${Definition.RecallStep}') THEN
  IF NOT ((OLD.is_active=1 AND NEW.is_active=0 AND (OLD.owner_uuid <=> NEW.owner_uuid)
   AND EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s WHERE s.binding_uuid=NEW.binding_uuid
    AND s.pod_uuid=NEW.uuid AND s.last_mutation_operation_uuid=NEW.last_mutation_operation_uuid
    AND s.captured_state_version=OLD.state_version AND s.committed_state_version=NEW.state_version
    AND (s.owner_uuid <=> NEW.owner_uuid))) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first inactive transition requires the original complete snapshot'; END IF;
 END IF;
"""

  private val committedRecallCheck = s"""
 IF NEW.step='${Definition.RecallStep}' AND NEW.operation_state='COMMITTED' THEN
  IF NOT ((EXISTS(SELECT 1 FROM floating_pod_state d JOIN ${Definition.SnapshotTable} s
   ON s.binding_uuid=d.binding_uuid AND s.pod_uuid=d.uuid
   WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.is_active=0
    AND d.last_mutation_operation_uuid=NEW.operation_uuid AND s.last_mutation_operation_uuid=NEW.operation_uuid
    AND s.attempt_uuid=NEW.attempt_uuid AND s.captured_state_version=NEW.expected_domain_version
    AND d.state_version=s.committed_state_version AND (d.owner_uuid <=> s.owner_uuid))) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first commit requires matching snapshot and inactive domain'; END IF;
 END IF;
"""

  /** checkpoint保存完整NBT及同中心区块内的真实pose，不负责mode/owner或footprint迁移。 */
  private val domainCheckpointCheck = s"""
 IF EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid
  AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='${Definition.CheckpointStep}') THEN
  IF NOT ((OLD.is_active=1 AND NEW.is_active=1 AND (OLD.owner_uuid <=> NEW.owner_uuid)
   AND (CAST(OLD.operating_mode AS BINARY) <=> CAST(NEW.operating_mode AS BINARY))
   AND (CAST(OLD.world_name AS BINARY) <=> CAST(NEW.world_name AS BINARY))
   AND (OLD.dimension_id <=> NEW.dimension_id)
   AND (FLOOR(OLD.pos_x/16) <=> FLOOR(NEW.pos_x/16))
   AND (FLOOR(OLD.pos_z/16) <=> FLOOR(NEW.pos_z/16))
   AND FLOOR(NEW.pos_x/16) BETWEEN -2147483646 AND 2147483645
   AND FLOOR(NEW.pos_z/16) BETWEEN -2147483646 AND 2147483645
   AND EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s JOIN physical_recovery_locator l
    ON l.binding_uuid=s.binding_uuid AND l.pod_uuid=s.pod_uuid
    JOIN fp_recovery_operation o ON o.binding_uuid=s.binding_uuid AND o.operation_uuid=s.last_mutation_operation_uuid
    WHERE s.binding_uuid=NEW.binding_uuid AND s.pod_uuid=NEW.uuid
     AND s.last_mutation_operation_uuid=NEW.last_mutation_operation_uuid
     AND s.captured_state_version=OLD.state_version AND s.committed_state_version=NEW.state_version
     AND (s.owner_uuid <=> NEW.owner_uuid) AND l.locator_state='ROW_ACTIVE'
     AND l.current_attempt_uuid=s.attempt_uuid AND l.current_attempt_uuid=o.attempt_uuid
     AND l.current_footprint_uuid=o.footprint_uuid AND l.locator_revision=o.expected_locator_revision
     AND o.expected_domain_version=OLD.state_version AND o.workflow_uuid=o.operation_uuid)) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first active checkpoint requires unchanged business state and original snapshot'; END IF;
 END IF;
"""

  private val locatorCheckpointCheck = s"""
 IF EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid
  AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='${Definition.CheckpointStep}') THEN
  IF NOT ((OLD.locator_state='ROW_ACTIVE' AND NEW.locator_state='ROW_ACTIVE'
   AND (OLD.current_attempt_uuid <=> NEW.current_attempt_uuid) AND (OLD.current_footprint_uuid <=> NEW.current_footprint_uuid)
   AND (OLD.target_dimension_id <=> NEW.target_dimension_id) AND (CAST(OLD.reason_code AS BINARY) <=> CAST(NEW.reason_code AS BINARY))
   AND NEW.domain_version=OLD.domain_version+1
   AND EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid
    AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.pod_uuid=NEW.pod_uuid
    AND o.attempt_uuid=NEW.current_attempt_uuid AND o.footprint_uuid=NEW.current_footprint_uuid
    AND o.expected_locator_revision=OLD.locator_revision AND o.expected_domain_version=OLD.domain_version)) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first active checkpoint cannot migrate locator or footprint'; END IF;
 END IF;
"""

  private val committedCheckpointCheck = s"""
 IF NEW.step='${Definition.CheckpointStep}' AND NEW.operation_state='COMMITTED' THEN
  IF NOT ((NEW.workflow_uuid=NEW.operation_uuid AND EXISTS(SELECT 1 FROM floating_pod_state d
   JOIN ${Definition.SnapshotTable} s ON s.binding_uuid=d.binding_uuid AND s.pod_uuid=d.uuid
   JOIN physical_recovery_locator l ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid
   JOIN physical_recovery_footprint f ON f.binding_uuid=l.binding_uuid AND f.pod_uuid=l.pod_uuid
    AND f.attempt_uuid=l.current_attempt_uuid AND f.footprint_uuid=l.current_footprint_uuid
   WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.is_active=1
    AND d.last_mutation_operation_uuid=NEW.operation_uuid AND s.last_mutation_operation_uuid=NEW.operation_uuid
    AND s.attempt_uuid=NEW.attempt_uuid AND s.captured_state_version=NEW.expected_domain_version
    AND d.state_version=s.committed_state_version AND (d.owner_uuid <=> s.owner_uuid)
    AND l.locator_state='ROW_ACTIVE' AND l.domain_version=d.state_version AND l.current_attempt_uuid=NEW.attempt_uuid
    AND l.current_footprint_uuid=NEW.footprint_uuid AND l.locator_revision=NEW.expected_locator_revision+1
    AND l.last_mutation_operation_uuid=NEW.operation_uuid AND f.seal_state='SEALED'
    AND f.center_chunk_x=FLOOR(d.pos_x/16) AND f.center_chunk_z=FLOOR(d.pos_z/16))) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first active checkpoint commit requires original active snapshot and locator'; END IF;
 END IF;
"""

  private val domainInitialActivationCheck = s"""
 IF EXISTS(SELECT 1 FROM fp_recovery_operation o WHERE o.binding_uuid=NEW.binding_uuid
  AND o.operation_uuid=NEW.last_mutation_operation_uuid AND o.step='${Definition.InitialActivationStep}') THEN
  IF NOT ((OLD.is_active=0 AND OLD.state_version=0 AND OLD.owner_uuid IS NULL
   AND NEW.is_active=1 AND NEW.state_version=1 AND NEW.owner_uuid IS NULL
   AND OLD.operating_mode='WORLD_IDLE' AND NEW.operating_mode='WORLD_IDLE'
   AND EXISTS(SELECT 1 FROM ${Definition.SnapshotTable} s WHERE s.binding_uuid=NEW.binding_uuid
    AND s.pod_uuid=NEW.uuid AND s.last_mutation_operation_uuid=NEW.last_mutation_operation_uuid
    AND s.captured_state_version=0 AND s.committed_state_version=1 AND s.owner_uuid IS NULL)) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first initial activation requires original full snapshot and prepared v0 domain'; END IF;
 END IF;
"""

  private val committedInitialActivationCheck = s"""
 IF NEW.step='${Definition.InitialActivationStep}' AND NEW.operation_state='COMMITTED' THEN
  IF NOT ((NEW.expected_domain_version=0 AND EXISTS(SELECT 1 FROM floating_pod_state d
   JOIN ${Definition.SnapshotTable} s ON s.binding_uuid=d.binding_uuid AND s.pod_uuid=d.uuid
   JOIN physical_recovery_intent i ON i.binding_uuid=d.binding_uuid AND i.pod_uuid=d.uuid AND i.attempt_uuid=s.attempt_uuid
   JOIN physical_recovery_locator l ON l.binding_uuid=d.binding_uuid AND l.pod_uuid=d.uuid
   WHERE d.binding_uuid=NEW.binding_uuid AND d.uuid=NEW.pod_uuid AND d.is_active=1 AND d.state_version=1
    AND d.owner_uuid IS NULL AND d.operating_mode='WORLD_IDLE' AND s.owner_uuid IS NULL
    AND s.captured_state_version=0 AND s.committed_state_version=1 AND s.attempt_uuid=NEW.attempt_uuid
    AND d.last_mutation_operation_uuid=NEW.operation_uuid AND s.last_mutation_operation_uuid=NEW.operation_uuid
    AND i.phase='ACTIVATED' AND i.origin='DEBUG_ITEM_CREATE' AND i.activation_operation_uuid=NEW.operation_uuid
    AND i.expected_domain_version=0 AND i.committed_domain_version=1 AND i.expected_owner_uuid IS NULL
    AND l.locator_state='ROW_ACTIVE' AND l.domain_version=1 AND l.current_attempt_uuid=NEW.attempt_uuid
    AND l.last_mutation_operation_uuid=NEW.operation_uuid)) <=> TRUE) THEN
   SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='DB-first initial commit requires snapshot domain intent and locator'; END IF;
 END IF;
"""

  private def replaceInGuard(sql: String, name: String, original: String, replacement: String): String =
    val start=sql.indexOf(s" TRIGGER $name ")
    if start<0 then throw new SQLDataException("schema6 missing guard " + name)
    val end=sql.indexOf("END$$",start)
    if end<0 then throw new SQLDataException("schema6 missing guard terminator " + name)
    sql.substring(0,start)+once(sql.substring(start,end),original,replacement)+sql.substring(end)

  private def snapshotGrants(inputs: MySqlDeploymentReceiptSchemaProfile.Inputs): String =
    val target = s"`${inputs.schema}`.`${Definition.SnapshotTable}`"
    val a = inputs.accounts
    val mutableColumns = Definition.snapshotColumns.map(_.name).filterNot(Definition.snapshotPrimaryKey.contains).sorted.mkString(",")
    Vector(
      s"GRANT SELECT,TRIGGER ON $target TO ${a.guardDefiner.grantee};",
      s"GRANT SELECT ON $target TO ${a.auditDefiner.grantee};",
      s"GRANT SELECT,INSERT ON $target TO ${a.runtime.grantee};",
      s"GRANT UPDATE ($mutableColumns) ON $target TO ${a.runtime.grantee};",
      s"GRANT SELECT,CREATE,INDEX,TRIGGER ON $target TO ${a.provisioner.grantee};"
    ).mkString("\n") + "\n"

  private def appendGuard(sql: String, name: String, extra: String): String =
    val start = sql.indexOf(s" TRIGGER $name ")
    if start < 0 then throw new SQLDataException("schema6 missing guard " + name)
    val end = sql.indexOf("END$$", start)
    if end < 0 then throw new SQLDataException("schema6 missing guard terminator " + name)
    sql.substring(0, end) + extra + sql.substring(end)

  private def once(source: String, anchor: String, replacement: String): String =
    val index = source.indexOf(anchor)
    if index < 0 || source.indexOf(anchor, index + anchor.length) >= 0 then
      throw new SQLDataException("schema6 requires one exact source anchor: " + anchor)
    source.substring(0, index) + replacement + source.substring(index + anchor.length)

  private def hash(value: String): String = MySqlScriptSchemaProfile.sha256(value.getBytes(StandardCharsets.UTF_8))
