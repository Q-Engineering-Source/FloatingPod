package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.HexFormat

/** 仅将已渲染的 schema6 定义收敛成封闭、可审阅的初始化步骤；不执行 SQL 或签发 Ready。 */
private[mysql] object MySqlManagedSchemaPlan:
  enum Kind:
    case SessionSetup, TargetDatabase, ManagedAccount, TableDefinition, TableConstraint, QuotaInitialization, SeedInsert
    case TriggerDefinition, RoutineDefinition, PrivilegeGrant

  final case class Step(ordinal: Int, identity: String, kind: Kind, sql: String, sha256: String):
    require(ordinal >= 0 && identity.nonEmpty && kind != null && sql.nonEmpty && sha256.matches("[0-9a-f]{64}"),
      "managed schema step must be complete and content-pinned")

  final case class SeedTemplate(table: String, variables: Vector[String], stepOrdinal: Int, sha256: String):
    require(table.matches("[A-Za-z0-9_]{1,64}") && variables.nonEmpty && stepOrdinal >= 0 &&
      variables.distinct.size == variables.size && sha256.matches("[0-9a-f]{64}"),
      "managed seed template must expose its exact ordered bind variables")

  final class Plan private[MySqlManagedSchemaPlan] (
      val schema: String,
      val ddlSha256: String,
      val profileIdentitySha256: String,
      val steps: Vector[Step],
      val tables: Vector[String],
      val accounts: Vector[String],
      val seedTemplates: Vector[SeedTemplate]
  ):
    require(schema.matches("[A-Za-z0-9_]{1,64}") && ddlSha256.matches("[0-9a-f]{64}"),
      "managed schema plan identity must be canonical")
    require(steps.map(_.ordinal) == steps.indices.toVector && steps.map(_.identity).distinct.size == steps.size,
      "managed schema steps must have unique ordered identities")
    require(tables.size == 27 && accounts.size == 6, "managed schema plan requires exact Full27 and six locked accounts")
    require(steps.count(_.kind == Kind.QuotaInitialization) == 1,
      "managed schema plan requires its exact deployment-quota initialization row")
    require(steps.count(_.kind == Kind.TableConstraint) == 3,
      "managed schema plan requires its exact three deferred source foreign keys")
    require(seedTemplates.map(_.table) == Vector("fp_schema_manifest", "fp_runtime_writer"),
      "managed schema plan requires the exact ordered bootstrap seed templates")
    def quotaInitialization: Step = steps.find(_.kind == Kind.QuotaInitialization).get
    def deferredConstraintSteps: Vector[Step] = steps.filter(_.kind == Kind.TableConstraint)
    def requiredSeedVariables: Vector[String] = seedTemplates.flatMap(_.variables).distinct
    override def toString: String = s"ManagedSchemaPlan(schema=$schema,steps=${steps.size},ddl=$ddlSha256)"

  private val CreateAccount = "^CREATE USER '([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})' ACCOUNT LOCK$".r
  private val CreateTable = "(?s)^CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*$".r
  private val Seed = "(?s)^INSERT INTO ([A-Za-z0-9_]+) \\(.*$".r
  private val Trigger = "(?s)^CREATE DEFINER='([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})' TRIGGER ([A-Za-z0-9_]+) BEFORE (INSERT|UPDATE|DELETE) ON `?([A-Za-z0-9_]+)`? .*".r
  private val Routine = "(?s)^CREATE DEFINER='([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})'\\s+PROCEDURE `([A-Za-z0-9_]+)`\\.`([A-Za-z0-9_]+)`\\(.*$".r
  private val Grant = "(?s)^GRANT (.+) ON (.+) TO '([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})'$".r
  private val SeedParameter = "@fp_[a-z0-9_]+".r
  private val DeferredTableConstraints = Vector(
    ("physical_source_unit", "fk_unit_current_forest",
      "ALTER TABLE physical_source_unit ADD CONSTRAINT fk_unit_current_forest\n" +
        " FOREIGN KEY (binding_uuid,unit_uuid,current_forest_uuid,current_epoch)\n" +
        " REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid,source_epoch)"),
    ("physical_recovery_intent", "fk_intent_completion",
      "ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_completion\n" +
        " FOREIGN KEY (binding_uuid,source_completion_uuid) REFERENCES fp_lifecycle_completion(binding_uuid,receipt_uuid)"),
    ("physical_recovery_intent", "fk_intent_source_forest",
      "ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_source_forest\n" +
        " FOREIGN KEY (binding_uuid,source_unit_uuid,source_forest_uuid) REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid)"))

  def build(rendered: MySqlDbFirstSchemaProfile.Rendered): Plan =
    if rendered == null || rendered.mode != MySqlDbFirstSchemaProfile.RenderMode.ExistingDatabaseOnly then
      throw new IllegalArgumentException("managed schema plan requires explicit existing-database rendering")
    val canonical = MySqlDbFirstSchemaProfile.renderExistingDatabase(rendered.inputs)
    if rendered.sql != canonical.sql || rendered.sha256 != canonical.sha256 ||
      rendered.profileIdentitySha256 != canonical.profileIdentitySha256 then
      throw new SQLDataException("managed schema plan differs from the exact release-rendered existing-database recipe")
    // 既有完整授权解析器仍核验账号范围、grant 语法及 routine execute 对。
    MySqlDbFirstSchemaProfile.expectedPrivilegeRows(rendered)
    val statements = MySqlScriptSchemaProfile.statements(rendered.sql).fold(
      error => throw new SQLDataException("managed schema plan framing failed: " + error), identity)
    val accountNames = rendered.inputs.accounts.all.map(_.identity)
    val accountGrantees = rendered.inputs.accounts.all.map(_.grantee).toSet
    val tableNames = rendered.tables.map(_.name)
    val quotaInitializationSql =
      s"INSERT INTO `${rendered.inputs.schema}`.`${MySqlDeploymentReceiptSchemaDefinition.QuotaTable}` " +
        s"(namespace,max_records,used_records,quota_revision) VALUES " +
        s"(UNHEX('${MySqlDeploymentReceiptSchemaDefinition.NamespaceHex}'),${rendered.inputs.quotaMaximumRecords},0,0)"
    if tableNames != MySqlDbFirstSchemaProfile.TableNames || tableNames.distinct.size != 27 then
      throw new SQLDataException("managed schema plan table descriptors differ from the exact Full27 profile")

    val classified = statements.zipWithIndex.map { (statement, index) =>
      classify(statement, index, rendered.inputs.schema, quotaInitializationSql,
        accountNames.toSet, accountGrantees, tableNames.toSet)
    }
    def count(kind: Kind): Int = classified.count(_.kind == kind)
    if count(Kind.TargetDatabase) != 2 || count(Kind.ManagedAccount) != 6 ||
      count(Kind.TableDefinition) != 27 || count(Kind.QuotaInitialization) != 1 || count(Kind.SeedInsert) != 2 ||
      count(Kind.TableConstraint) != DeferredTableConstraints.size ||
      count(Kind.TriggerDefinition) != 81 || count(Kind.RoutineDefinition) != 1 ||
      count(Kind.SessionSetup) != 7 then
      throw new SQLDataException("managed schema plan object counts differ from the closed schema6 recipe")
    if count(Kind.PrivilegeGrant) == 0 then throw new SQLDataException("managed schema plan has no privilege grants")

    val tableOrdinals = classified.indices.filter(index => classified(index).kind == Kind.TableDefinition)
    val routineOrdinals = classified.indices.filter(index => classified(index).kind == Kind.RoutineDefinition)
    val grantOrdinals = classified.indices.filter(index => classified(index).kind == Kind.PrivilegeGrant)
    val constraintSteps = classified.filter(_.kind == Kind.TableConstraint)
    if constraintSteps.map(_.identity) != DeferredTableConstraints.map { (table, name, _) => s"constraint:$table:$name" } ||
        constraintSteps.map(_.sql) != DeferredTableConstraints.map(_._3) then
      throw new SQLDataException("managed schema deferred foreign keys differ from the exact ordered source profile")
    val quotaOrdinal = classified.indexWhere(_.kind == Kind.QuotaInitialization)
    val seedOrdinals = classified.indices.filter(index => classified(index).kind == Kind.SeedInsert)
    val triggerOrdinals = classified.indices.filter(index => classified(index).kind == Kind.TriggerDefinition)
    if tableOrdinals.max >= routineOrdinals.head || routineOrdinals.head >= grantOrdinals.min ||
        grantOrdinals.max >= quotaOrdinal || quotaOrdinal >= seedOrdinals.min || seedOrdinals.max >= triggerOrdinals.min then
      throw new SQLDataException("managed schema order must be Full27 tables, routine/grants, quota row, two control seeds, then all guards")

    val actualTables = classified.collect { case step if step.kind == Kind.TableDefinition =>
      step.identity.stripPrefix("table:")
    }
    val actualGuards = rendered.guards.map(_.name)
    if actualTables != tableNames then throw new SQLDataException("managed schema table order differs from the closed profile")
    val guardNames = classified.collect { case step if step.kind == Kind.TriggerDefinition => step.identity.stripPrefix("trigger:") }
    if guardNames != actualGuards then throw new SQLDataException("managed schema trigger order differs from the closed profile")
    val createdAccounts = classified.collect { case step if step.kind == Kind.ManagedAccount => step.identity.stripPrefix("account:") }
    if createdAccounts.toSet != accountNames.toSet || createdAccounts.size != 6 then
      throw new SQLDataException("managed schema locked accounts differ from the exact six-account profile")
    val seeds = classified.collect { case step if step.kind == Kind.SeedInsert => step.identity.stripPrefix("seed:") }
    if seeds != Vector("fp_schema_manifest", "fp_runtime_writer") then
      throw new SQLDataException("managed schema bootstrap seeds differ from the exact two-row recipe")

    val steps = classified.zipWithIndex.map { (step, index) =>
      step.copy(ordinal = index, sha256 = sha256(step.sql))
    }
    val seedTemplates = steps.filter(_.kind == Kind.SeedInsert).map { step =>
      val name = step.identity.stripPrefix("seed:")
      val variables = SeedParameter.findAllIn(step.sql).toVector.distinct
      if variables.isEmpty then throw new SQLDataException("managed bootstrap seed has no explicit typed bind variables: " + name)
      SeedTemplate(name, variables, step.ordinal, step.sha256)
    }
    new Plan(rendered.inputs.schema, rendered.sha256, rendered.profileIdentitySha256,
      steps, tableNames, accountNames, seedTemplates)

  private def classify(
      statement: String,
      ordinal: Int,
      schema: String,
      quotaInitializationSql: String,
      accounts: Set[String],
      accountGrantees: Set[String],
      tables: Set[String]
  ): Step =
    def step(kind: Kind, identity: String): Step = Step(ordinal, identity, kind, statement, sha256(statement))
    statement match
      case "SET SESSION sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'" =>
        step(Kind.SessionSetup, s"session:sql-mode-strict-all:$ordinal")
      case "SET SESSION sql_mode='STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'" =>
        step(Kind.SessionSetup, s"session:sql-mode-strict-trans:$ordinal")
      case "SET SESSION sql_mode = 'STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'" =>
        step(Kind.SessionSetup, s"session:sql-mode-audit:$ordinal")
      case "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin" =>
        step(Kind.SessionSetup, s"session:charset:$ordinal")
      case value if value == s"USE `$schema`" => step(Kind.TargetDatabase, s"database:$schema:$ordinal")
      case CreateAccount(user, host) =>
        val identity = s"$user@$host"
        if !accounts(identity) then throw new SQLDataException("managed schema plan has an unapproved account identity")
        step(Kind.ManagedAccount, "account:" + identity)
      case CreateTable(name) =>
        if !tables(name) then throw new SQLDataException("managed schema plan has an unknown table definition: " + name)
        step(Kind.TableDefinition, "table:" + name)
      case value if value == quotaInitializationSql =>
        step(Kind.QuotaInitialization, "quota-initialization:" + MySqlDeploymentReceiptSchemaDefinition.QuotaTable)
      case value if DeferredTableConstraints.exists(_._3 == value) =>
        val (table, name, _) = DeferredTableConstraints.find(_._3 == value).get
        if !tables(table) then throw new SQLDataException("managed schema deferred constraint table is outside the Full27 profile")
        step(Kind.TableConstraint, s"constraint:$table:$name")
      case Seed(name) if name == "fp_schema_manifest" || name == "fp_runtime_writer" =>
        step(Kind.SeedInsert, "seed:" + name)
      case Trigger(user, host, name, _, table) =>
        if !accounts(s"$user@$host") || !tables(table) then
          throw new SQLDataException("managed schema plan trigger definer or table is outside the closed profile")
        step(Kind.TriggerDefinition, "trigger:" + name)
      case Routine(user, host, routineSchema, name) =>
        if !accounts(s"$user@$host") || routineSchema != schema || name != MySqlDbFirstSchemaDefinition.AuditRoutine then
          throw new SQLDataException("managed schema plan routine is outside the closed profile")
        step(Kind.RoutineDefinition, "routine:" + name)
      case Grant(privileges, target, user, host) =>
        val grantee = s"'$user'@'$host'"
        if !accountGrantees(grantee) then throw new SQLDataException("managed schema grant target account is outside the closed profile")
        val allowedTarget = target == s"`$schema`.*" || target == "`mysql`.*" ||
          target == "*.*" && privileges == "SHOW_ROUTINE" ||
          target.startsWith(s"`$schema`.`") && target.endsWith("`") ||
          target.startsWith(s"PROCEDURE `$schema`.`") && target.endsWith("`")
        if !allowedTarget then throw new SQLDataException("managed schema grant target is outside the explicit schema/account recipe")
        step(Kind.PrivilegeGrant, s"grant:$ordinal:$user@$host:$target:$privileges")
      case _ =>
        throw new SQLDataException("managed schema plan rejected an unexpected SQL statement at ordinal " + ordinal)

  private def sha256(value: String): String = HexFormat.of().formatHex(
    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)))
