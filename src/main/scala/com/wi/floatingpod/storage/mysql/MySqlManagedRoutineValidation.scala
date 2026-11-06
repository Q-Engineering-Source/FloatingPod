package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import MySqlManagedStaticRecipe.{Recipe, Token, TokenKind, CreationEnvironment, ConstraintKind}

/** 独立发布 SOURCE 与实际静态 DATA 比较。不会 CALL 被检查的过程，也不发行运行资格。 */
private[mysql] object MySqlManagedRoutineValidation:
  def validate(recipe: Recipe, sets: Vector[MySqlAuditResultSet], root: MySqlFormat3ReadBudget, routineSource: MySqlAuditResultSet): Unit =
    require(recipe != null && sets != null && root != null, "exact recipe/capture/root required")
    if sets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then reject("managed static result-set order differs")
    def selected(id: String): MySqlAuditResultSet = sets.find(_.id == id).getOrElse(reject("managed static result-set missing"))

    validateChecks(recipe.schema, recipe.tables, selected("RS08_CHECK_CONSTRAINTS"), root, false)

    val triggerSet = selected("RS09_TRIGGERS")
    val triggers = recipe.triggers.sortBy(_.name)
    if triggerSet.rows.size != triggers.size then reject("managed trigger count differs from release recipe")
    triggerSet.rows.zip(triggers).foreach { (row, expected) =>
      validateTrigger(recipe.schema, expected, triggerSet, row, root)
    }

    validateRoutine(recipe, selected("RS10_AUDITSELF"), selected("RS11_AUDITSELF_PARAMETERS"), root, routineSource)

  private[mysql] def validateTrigger(schema: String, expected: MySqlManagedStaticRecipe.Trigger,
      triggerSet: MySqlAuditResultSet, row: MySqlAuditRow, root: MySqlFormat3ReadBudget): Unit =
    val data = fields(triggerSet, row)
    equal(data, "TRIGGER_SCHEMA", schema)
    equal(data, "TRIGGER_NAME", expected.name)
    equal(data, "EVENT_OBJECT_SCHEMA", schema)
    equal(data, "EVENT_OBJECT_TABLE", expected.table)
    equal(data, "EVENT_MANIPULATION", expected.event)
    equal(data, "ACTION_TIMING", "BEFORE")
    equal(data, "ACTION_ORDER", "1")
    equal(data, "ACTION_ORIENTATION", "ROW")
    val body = text(data, "ACTION_STATEMENT")
    bodyMatches(expected.bodyTokens, body, root, "trigger " + expected.name)
    equal(data, "DEFINER", expected.definerUser + "@" + expected.definerHost)
    environment(data, expected.environment)
    selfDescription(data, body, "statement_octets", "statement_sha256")

  /** 表定义不确定提交后的 CHECK 核对；表所属约束名称已由 RS05 的精确投影验证。 */
  def validateTableChecks(recipe: Recipe, table: String, set: MySqlAuditResultSet,
      root: MySqlFormat3ReadBudget): Unit =
    val selected = recipe.tables.filter(_.name == table)
    if selected.size != 1 then reject("managed table CHECK inspection requires an exact recipe table")
    validateChecks(recipe.schema, selected, set, root, true)

  private def validateChecks(schema: String, tables: Vector[MySqlManagedStaticRecipe.Table],
      checkSet: MySqlAuditResultSet, root: MySqlFormat3ReadBudget, subset: Boolean): Unit =
    if checkSet.id != "RS08_CHECK_CONSTRAINTS" then reject("managed CHECK inspection set differs")
    val checks = tables.flatMap(_.constraints.filter(_.kind == ConstraintKind.Check))
      .map { value =>
        val name = value.name.getOrElse(reject("managed CHECK must have an explicit source name"))
        val tokens = value.tokens
        if tokens.size < 6 || tokens(3).value != "(" || tokens.last.value != ")" then
          reject("managed CHECK source framing differs")
        name -> tokens.slice(4, tokens.size - 1)
      }.sortBy(_._1)
    val names = checks.map(_._1).toSet
    val rows = if subset then checkSet.rows.filter(row => names.contains(text(fields(checkSet, row), "CONSTRAINT_NAME")))
      else checkSet.rows
    if rows.size != checks.size then reject("managed CHECK count differs from release recipe")
    rows.zip(checks).foreach { (row, expected) =>
      val data = fields(checkSet, row)
      equal(data, "CONSTRAINT_CATALOG", "def")
      equal(data, "CONSTRAINT_SCHEMA", schema)
      equal(data, "CONSTRAINT_NAME", expected._1)
      val clause = text(data, "CHECK_CLAUSE")
      temporary(root, clause) {
        if !MySqlManagedCheckExpression.equivalent(expected._2, clause) then
          reject("managed CHECK expression differs from release recipe: " + expected._1)
      }
      selfDescription(data, clause, "clause_octets", "clause_sha256")
    }

  private[mysql] def validateRoutine(recipe: Recipe, routineSet: MySqlAuditResultSet,
      parameters: MySqlAuditResultSet, root: MySqlFormat3ReadBudget, routineSource: MySqlAuditResultSet): Unit =
    if routineSet.rows.size != 1 then reject("managed audit procedure must have exactly one catalog definition")
    val expected = recipe.auditRoutine
    val data = fields(routineSet, routineSet.rows.head)
    equal(data, "ROUTINE_SCHEMA", recipe.schema)
    equal(data, "ROUTINE_NAME", expected.name)
    equal(data, "ROUTINE_TYPE", "PROCEDURE")
    val body = text(data, "ROUTINE_DEFINITION")
    temporary(root, body) { MySqlManagedRoutineSource.validate(recipe, body, routineSource, root) }
    equal(data, "SECURITY_TYPE", expected.securityType)
    equal(data, "SQL_DATA_ACCESS", expected.dataAccess)
    equal(data, "DEFINER", expected.definerUser + "@" + expected.definerHost)
    environment(data, expected.environment)
    // 发布过程没有 DETERMINISTIC、返回类型或参数；这些是过程种类的固定元数据，不是旧对象内容。
    if expected.headerTokens.exists(t => t.kind == TokenKind.Word && t.value == "deterministic") then
      reject("managed release audit procedure unexpectedly declares determinism")
    equal(data, "IS_DETERMINISTIC", "NO")
    equal(data, "DATA_TYPE", "")
    if data.getOrElse("DTD_IDENTIFIER", reject("managed routine type descriptor missing")) != MySqlAuditNull then
      reject("managed procedure unexpectedly has a return type descriptor")
    val comments = expected.headerTokens.sliding(2).collect {
      case Vector(keyword, literal) if keyword.kind == TokenKind.Word && keyword.value == "comment" &&
          literal.kind == TokenKind.StringLiteral => literal.value
    }.toVector
    if comments.size != 1 || !comments.head.startsWith("'") || !comments.head.endsWith("'") ||
        comments.head.contains("\\") then reject("managed release audit comment source is unsupported")
    equal(data, "ROUTINE_COMMENT", comments.head.substring(1, comments.head.length - 1).replace("''", "'"))
    selfDescription(data, body, "definition_octets", "definition_sha256")
    if parameters.rows.nonEmpty then reject("managed audit procedure has unexpected parameters")

  private def bodyMatches(expected: Vector[Token], actual: String, root: MySqlFormat3ReadBudget, operation: String): Unit =
    temporary(root, actual) {
      if MySqlManagedStaticRecipe.tokenize(actual) != expected then
        reject("managed " + operation + " body differs from release recipe")
    }

  private def environment(data: Map[String, MySqlAuditCell], expected: CreationEnvironment): Unit =
    def modes(value: String): Vector[String] =
      val parts = value.split(",", -1).toVector
      if parts.exists(part => !part.matches("[A-Z_]+")) || parts.distinct.size != parts.size then
        reject("managed object has malformed SQL_MODE")
      parts.sorted
    if modes(text(data, "SQL_MODE")) != modes(expected.sqlMode) then reject("managed object SQL_MODE differs from source")
    equal(data, "CHARACTER_SET_CLIENT", expected.characterSetClient)
    equal(data, "COLLATION_CONNECTION", expected.collationConnection)
    equal(data, "DATABASE_COLLATION", expected.requiredDatabaseCollation)

  private def selfDescription(data: Map[String, MySqlAuditCell], source: String, length: String, hash: String): Unit =
    val bytes = source.getBytes(StandardCharsets.UTF_8)
    equal(data, length, bytes.length.toString)
    data.getOrElse(hash, reject("managed object digest column missing")) match
      case binary: MySqlAuditBinary if MessageDigest.isEqual(binary.bytes, MessageDigest.getInstance("SHA-256").digest(bytes)) => ()
      case _ => reject("managed object self-description digest differs")

  private def fields(set: MySqlAuditResultSet, row: MySqlAuditRow): Map[String, MySqlAuditCell] =
    if set.columns.size != row.cells.size || set.columns.map(_.label).distinct.size != set.columns.size then
      reject("managed catalog row has different fields")
    set.columns.map(_.label).zip(row.cells).toMap
  private def text(data: Map[String, MySqlAuditCell], name: String): String =
    data.getOrElse(name, reject("managed catalog field missing: " + name)) match
      case MySqlAuditText(value) => value
      case _ => reject("managed catalog field is not non-NULL text: " + name)
  private def equal(data: Map[String, MySqlAuditCell], name: String, expected: String): Unit =
    if text(data, name) != expected then reject("managed catalog value differs: " + name)
  private def temporary[A](root: MySqlFormat3ReadBudget, value: String)(body: => A): A =
    MySqlManifestConfigurationIdentity.temporary(root, Math.addExact(4096L, Math.multiplyExact(128L, value.length.toLong)))(body)
  private def reject(message: String): Nothing = throw new SQLDataException(message)
