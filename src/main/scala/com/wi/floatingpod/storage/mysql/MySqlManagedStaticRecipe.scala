package com.wi.floatingpod.storage.mysql

import java.util.Locale
import java.sql.SQLDataException

/** 仅提取已渲染的 schema6 SOURCE recipe；不验证观测结果或发行运行资格。 */
private[mysql] object MySqlManagedStaticRecipe:
  enum TokenKind:
    case Word, QuotedWord, StringLiteral, Number, Symbol, Comment

  /** 字符串字面量与注释保留原始拼写；反引号标识符已解码但保留 quoted 状态。 */
  final case class Token(kind: TokenKind, value: String)

  final case class CreationEnvironment(
      sqlMode: String,
      characterSetClient: String,
      collationConnection: String,
      requiredDatabaseCharacterSet: String,
      requiredDatabaseCollation: String
  ):
    require(sqlMode.nonEmpty && characterSetClient.nonEmpty && collationConnection.nonEmpty &&
      requiredDatabaseCharacterSet.nonEmpty && requiredDatabaseCollation.nonEmpty,
      "compiled object creation environment must be explicit")

  enum ConstraintKind:
    case PrimaryKey, UniqueIndex, Index, ForeignKey, Check

  sealed trait TablePart:
    def tokens: Vector[Token]
  final case class GeneratedColumn(
      expression: Vector[Token],
      storage: String,
      clauseTokens: Vector[Token]
  ):
    require(expression.nonEmpty && Set("STORED", "VIRTUAL").contains(storage) && clauseTokens.nonEmpty,
      "compiled generated-column recipe must preserve expression and storage")
  final case class Column(
      name: String,
      typeSource: Vector[Token],
      declaration: Vector[Token],
      nullable: Boolean,
      generated: Option[GeneratedColumn],
      tokens: Vector[Token]
  ) extends TablePart
  final case class Constraint(kind: ConstraintKind, name: Option[String], tokens: Vector[Token]) extends TablePart

  final case class Table(
      name: String,
      columns: Vector[Column],
      constraints: Vector[Constraint],
      options: Vector[Token],
      environment: CreationEnvironment,
      statementTokens: Vector[Token],
      statementSha256: String
  ):
    require(name.matches("[a-zA-Z0-9_]{1,64}") && columns.nonEmpty && environment != null &&
      statementTokens.nonEmpty && statementSha256.matches("[0-9a-f]{64}"),
      "compiled table recipe must be complete")

  final case class Trigger(
      name: String,
      table: String,
      event: String,
      definerUser: String,
      definerHost: String,
      environment: CreationEnvironment,
      headerTokens: Vector[Token],
      bodyTokens: Vector[Token],
      statementSha256: String
  ):
    require(name.matches("[a-zA-Z0-9_]{1,64}") && table.matches("[a-zA-Z0-9_]{1,64}") &&
      Set("INSERT", "UPDATE", "DELETE").contains(event) && definerUser.nonEmpty && definerHost.nonEmpty &&
      headerTokens.nonEmpty && bodyTokens.nonEmpty && statementSha256.matches("[0-9a-f]{64}"),
      "compiled trigger recipe must be complete")

  final case class AuditRoutine(
      schema: String,
      name: String,
      definerUser: String,
      definerHost: String,
      securityType: String,
      dataAccess: String,
      environment: CreationEnvironment,
      headerTokens: Vector[Token],
      bodyTokens: Vector[Token],
      statementSha256: String
  ):
    require(schema.matches("[A-Za-z0-9_]{1,64}") && name.matches("[A-Za-z0-9_]{1,64}") &&
      definerUser.nonEmpty && definerHost.nonEmpty && securityType == "DEFINER" &&
      dataAccess == "READS SQL DATA" && headerTokens.nonEmpty && bodyTokens.nonEmpty &&
      statementSha256.matches("[0-9a-f]{64}"), "compiled audit routine recipe must be complete")

  final class Recipe private[MySqlManagedStaticRecipe](
      val schema: String,
      val ddlSha256: String,
      val profileIdentitySha256: String,
      val tables: Vector[Table],
      val triggers: Vector[Trigger],
      val auditRoutine: AuditRoutine,
      val sessionEnvironments: Vector[CreationEnvironment],
      val appliedConstraintOrdinals: Set[Int]
  ):
    require(schema.matches("[A-Za-z0-9_]{1,64}") && ddlSha256.matches("[0-9a-f]{64}") &&
      profileIdentitySha256.matches("[0-9a-f]{64}"), "compiled schema6 recipe identity is malformed")
    require(tables.size == 27 && tables.map(_.name).distinct.size == 27 &&
      tables.map(_.columns.size).sum == 480, "compiled schema6 recipe must contain exact Full27 columns")
    require(triggers.size == 81 && triggers.map(_.name).distinct.size == 81,
      "compiled schema6 recipe must contain exact81 unique guards")
    require(appliedConstraintOrdinals != null, "managed deferred-constraint prefix must be explicit")
    override def toString: String = s"ManagedStaticRecipe(schema=$schema,tables=${tables.size},triggers=${triggers.size},ddl=$ddlSha256)"

  private val DatabaseCharset = "utf8mb4"
  private val DatabaseCollation = "utf8mb4_0900_bin"
  private val SupportedColumnTypes = Set(
    "bigint", "datetime", "double", "int", "longblob", "longtext", "mediumblob",
    "smallint", "tinyint", "varbinary", "varchar")
  private final case class DeferredConstraintDefinition(
      table: String,
      name: String,
      childColumns: Vector[String],
      parentTable: String,
      parentColumns: Vector[String]
  )
  private val DeferredConstraintDefinitions = Vector(
    DeferredConstraintDefinition("physical_source_unit", "fk_unit_current_forest",
      Vector("binding_uuid", "unit_uuid", "current_forest_uuid", "current_epoch"),
      "physical_source_forest", Vector("binding_uuid", "unit_uuid", "forest_uuid", "source_epoch")),
    DeferredConstraintDefinition("physical_recovery_intent", "fk_intent_completion",
      Vector("binding_uuid", "source_completion_uuid"),
      "fp_lifecycle_completion", Vector("binding_uuid", "receipt_uuid")),
    DeferredConstraintDefinition("physical_recovery_intent", "fk_intent_source_forest",
      Vector("binding_uuid", "source_unit_uuid", "source_forest_uuid"),
      "physical_source_forest", Vector("binding_uuid", "unit_uuid", "forest_uuid")))

  /** 仅从精确 rendered release plan 提取不可变 SOURCE 描述，不接受运行时观测值。 */
  def fromPlan(plan: MySqlManagedSchemaPlan.Plan): Recipe =
    if plan == null then reject("compiled existing-database schema6 plan is required")
    fromPlanAt(plan, plan.deferredConstraintSteps.map(_.ordinal).toSet)

  /** 创建表定义保持完整身份；只将安装日志确认的延期外键前缀并入静态 recipe。 */
  def fromPlanAt(plan: MySqlManagedSchemaPlan.Plan, appliedConstraintOrdinals: Set[Int]): Recipe =
    if plan == null then reject("compiled existing-database schema6 plan is required")
    val deferredConstraints = validateDeferredConstraintSteps(plan)
    val applied = validateAppliedConstraintPrefix(deferredConstraints.map(_._1.ordinal), appliedConstraintOrdinals)
    var sqlMode: Option[String] = None
    var charset: Option[String] = None
    var collation: Option[String] = None
    var targetSelected = false
    val tables = Vector.newBuilder[Table]
    val triggers = Vector.newBuilder[Trigger]
    var routine: Option[AuditRoutine] = None
    val environments = Vector.newBuilder[CreationEnvironment]

    def environment(): CreationEnvironment =
      if !targetSelected then reject("managed schema object appears before the exact target database selection")
      val result = CreationEnvironment(
        sqlMode.getOrElse(reject("managed schema object has no explicit session sql_mode")),
        charset.getOrElse(reject("managed schema object has no explicit character_set_client")),
        collation.getOrElse(reject("managed schema object has no explicit collation_connection")),
        DatabaseCharset,
        DatabaseCollation)
      environments += result
      result

    plan.steps.foreach { step =>
      step.kind match
        case MySqlManagedSchemaPlan.Kind.SessionSetup =>
          val tokens = tokenize(step.sql)
          if starts(tokens, "SET", "SESSION", "SQL_MODE") && tokens.size == 5 && tokens(3) == symbol("=") &&
              tokens(4).kind == TokenKind.StringLiteral then
            sqlMode = Some(stringContents(tokens(4)))
          else if starts(tokens, "SET", "NAMES") && tokens.size == 5 &&
              tokens(2).kind == TokenKind.Word && isWord(tokens(3), "COLLATE") && tokens(4).kind == TokenKind.Word then
            charset = Some(tokens(2).value)
            collation = Some(tokens(4).value)
          else reject("managed schema plan contains an unsupported session setup statement")
        case MySqlManagedSchemaPlan.Kind.TargetDatabase =>
          if step.identity != s"database:${plan.schema}:${step.ordinal}" then
            reject("managed target database step identity differs from the compiled schema")
          val tokens = tokenize(step.sql)
          if tokens != Vector(Token(TokenKind.Word, "use"), Token(TokenKind.QuotedWord, plan.schema)) then
            reject("managed target database statement differs from the exact configured schema")
          targetSelected = true
        case MySqlManagedSchemaPlan.Kind.TableDefinition =>
          val env = environment()
          val parsed = parseTable(step, env)
          if parsed.name != step.identity.stripPrefix("table:") then
            reject("managed table statement identity differs from its compiled plan step")
          tables += parsed
        case MySqlManagedSchemaPlan.Kind.TableConstraint => ()
        case MySqlManagedSchemaPlan.Kind.TriggerDefinition =>
          val env = environment()
          val parsed = parseTrigger(step, env)
          if parsed.name != step.identity.stripPrefix("trigger:") then
            reject("managed trigger statement identity differs from its compiled plan step")
          triggers += parsed
        case MySqlManagedSchemaPlan.Kind.RoutineDefinition =>
          val env = environment()
          if routine.nonEmpty then reject("managed recipe contains multiple audit routines")
          val parsed = parseRoutine(step, env, plan.schema)
          if parsed.name != step.identity.stripPrefix("routine:") then
            reject("managed audit routine identity differs from its compiled plan step")
          routine = Some(parsed)
        case MySqlManagedSchemaPlan.Kind.ManagedAccount | MySqlManagedSchemaPlan.Kind.QuotaInitialization |
             MySqlManagedSchemaPlan.Kind.SeedInsert |
             MySqlManagedSchemaPlan.Kind.PrivilegeGrant => ()
    }

    val tableVector = tables.result()
    val triggerVector = triggers.result()
    if tableVector.map(_.name) != plan.tables then reject("managed recipe table order differs from plan")
    val expectedTriggers = plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.TriggerDefinition)
      .map(_.identity.stripPrefix("trigger:"))
    if triggerVector.map(_.name) != expectedTriggers then reject("managed recipe trigger order differs from plan")
    val auditRoutine = routine.getOrElse(reject("managed recipe lacks its exact compiled audit routine"))
    if auditRoutine.name != MySqlDbFirstSchemaDefinition.AuditRoutine then
      reject("managed recipe lacks its exact compiled audit routine")
    val selectedConstraints = deferredConstraints.collect {
      case (step, tableName, constraint) if applied(step.ordinal) => tableName -> constraint
    }.groupMap(_._1)(_._2)
    val augmentedTables = tableVector.map { table =>
      val additions = selectedConstraints.getOrElse(table.name, Vector.empty)
      table.copy(constraints = table.constraints ++ additions)
    }
    new Recipe(plan.schema, plan.ddlSha256, plan.profileIdentitySha256,
      augmentedTables, triggerVector, auditRoutine, environments.result(), applied)

  private def validateDeferredConstraintSteps(
      plan: MySqlManagedSchemaPlan.Plan
  ): Vector[(MySqlManagedSchemaPlan.Step, String, Constraint)] =
    val steps = plan.deferredConstraintSteps
    if steps.size != DeferredConstraintDefinitions.size then
      reject("managed schema plan must contain the exact three deferred foreign-key clauses")
    if plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.TableConstraint) != steps ||
        steps.map(_.ordinal) != steps.map(_.ordinal).sorted || steps.map(_.ordinal).distinct.size != steps.size then
      reject("managed deferred-constraint steps differ from the complete ordered plan clauses")
    steps.zip(DeferredConstraintDefinitions).map { (step, expected) =>
      if step.ordinal >= plan.steps.size || plan.steps(step.ordinal) != step ||
          step.kind != MySqlManagedSchemaPlan.Kind.TableConstraint ||
          step.identity != s"constraint:${expected.table}:${expected.name}" then
        reject("managed deferred-constraint ordinal or identity differs from the exact Full27 recipe")
      val (tableName, constraint) = parseDeferredConstraint(step)
      val actual = parseForeignKeyConstraint(constraint)
      if tableName != expected.table || constraint.name != Some(expected.name) ||
          actual != (expected.childColumns, expected.parentTable, expected.parentColumns) then
        reject("managed deferred foreign-key clause differs from its exact compiled source definition")
      (step, tableName, constraint)
    }

  private def validateAppliedConstraintPrefix(ordinals: Vector[Int], applied: Set[Int]): Set[Int] =
    if applied == null then reject("managed deferred foreign-key applied prefix must be explicit")
    val allowed = ordinals.toSet
    if !applied.subsetOf(allowed) then reject("managed deferred foreign-key prefix contains an unknown step ordinal")
    if applied != ordinals.take(applied.size).toSet then
      reject("managed deferred foreign-key ordinals must form one exact recipe-order prefix")
    applied

  private def parseDeferredConstraint(step: MySqlManagedSchemaPlan.Step): (String, Constraint) =
    val tokens = tokenize(step.sql)
    if tokens.size < 12 || !starts(tokens, "ALTER", "TABLE") || !isWord(tokens(3), "ADD") then
      reject("managed deferred table-constraint statement is outside the ALTER TABLE ADD grammar")
    val tableName = identifier(tokens(2))
    val constraint = parseTablePart(tokens.drop(4)) match
      case parsed: Constraint if parsed.kind == ConstraintKind.ForeignKey => parsed
      case _ => reject("managed deferred table-constraint statement must add one named foreign key")
    if step.identity != s"constraint:$tableName:${constraint.name.getOrElse("")}" then
      reject("managed deferred foreign-key step identity differs from its ALTER TABLE clause")
    (tableName, constraint)

  private def parseForeignKeyConstraint(
      constraint: Constraint
  ): (Vector[String], String, Vector[String]) =
    val tokens = constraint.tokens
    if tokens.size < 9 || !starts(tokens, "CONSTRAINT") ||
        constraint.name.forall(_ != identifier(tokens(1))) ||
        !isWord(tokens(2), "FOREIGN") || !isWord(tokens(3), "KEY") then
      reject("managed foreign-key clause is outside the named FOREIGN KEY grammar")
    val childOpen = 4
    val childColumns = parseIdentifierTuple(tokens, childOpen)
    var cursor = matchingParen(tokens, childOpen) + 1
    if !isWordAt(tokens, cursor, "REFERENCES") then reject("managed foreign-key clause lacks REFERENCES")
    cursor += 1
    if cursor >= tokens.size then reject("managed foreign-key clause lacks a referenced table")
    val parentTable = identifier(tokens(cursor))
    cursor += 1
    if cursor >= tokens.size || tokens(cursor) != symbol("(") then
      reject("managed deferred foreign-key parent columns must be explicit")
    val parentColumns = parseIdentifierTuple(tokens, cursor)
    if matchingParen(tokens, cursor) != tokens.size - 1 then
      reject("managed deferred foreign-key clause has unsupported trailing actions")
    if childColumns.size != parentColumns.size then reject("managed foreign-key tuples have different arities")
    (childColumns, parentTable, parentColumns)

  private def parseIdentifierTuple(tokens: Vector[Token], open: Int): Vector[String] =
    val close = matchingParen(tokens, open)
    val pieces = splitTopLevel(tokens.slice(open + 1, close), ",")
    if pieces.isEmpty || pieces.exists(part => part.size != 1 || !isIdentifier(part.head)) then
      reject("managed foreign-key tuple contains an unsupported key expression")
    val columns = pieces.map(part => identifier(part.head))
    if columns.distinct.size != columns.size then reject("managed foreign-key tuple repeats a column")
    columns

  /**
    * 为封闭生成 SQL 语法提供保守词法切分：只折叠 ASCII 裸词与空白；字符串、注释、运算符和分组原样保留。
    * 这只定义 SOURCE 词法等价，不推断 SQL 语义或服务器重写。
    */
  def tokenize(sql: String): Vector[Token] = tokenizeInternal(sql, false)

  /** MySQL 9.7 CHECK_CLAUSE 会为简单 utf8mb4 字面量的两端引号保留反斜线。
    * 此入口仅解码目录表示；不扩大生成 SQL 或普通 SQL 的语法。
    */
  private[mysql] def tokenizeCheckCatalog(sql: String): Vector[Token] = tokenizeInternal(sql, true)

  private def tokenizeInternal(sql: String, catalog: Boolean): Vector[Token] =
    if sql == null then reject("SQL source is required for managed recipe tokenization")
    val out = Vector.newBuilder[Token]
    var i = 0
    while i < sql.length do
      val c = sql.charAt(i)
      if Character.isWhitespace(c) then i += 1
      else if c == '\'' || c == '"' then
        val start = i
        val quote = c
        i += 1
        var closed = false
        while i < sql.length && !closed do
          if sql.charAt(i) == '\\' then
            if i + 1 >= sql.length then reject("unterminated SQL string escape")
            i += 2
          else if sql.charAt(i) == quote then
            if i + 1 < sql.length && sql.charAt(i + 1) == quote then i += 2
            else { i += 1; closed = true }
          else i += 1
        if !closed then reject("unterminated SQL string literal")
        out += Token(TokenKind.StringLiteral, sql.substring(start, i))
      else if c == '`' then
        i += 1
        val decoded = new StringBuilder
        var closed = false
        while i < sql.length && !closed do
          if sql.charAt(i) == '`' then
            if i + 1 < sql.length && sql.charAt(i + 1) == '`' then { decoded.append('`'); i += 2 }
            else { i += 1; closed = true }
          else { decoded.append(sql.charAt(i)); i += 1 }
        if !closed || decoded.isEmpty then reject("malformed or empty backtick identifier")
        // 保留 quoted 标识符大小写；服务器文件系统对表名大小写的处理可能不同。
        out += Token(TokenKind.QuotedWord, decoded.toString)
      else if c == '-' && i + 1 < sql.length && sql.charAt(i + 1) == '-' &&
          (i + 2 == sql.length || Character.isWhitespace(sql.charAt(i + 2))) then
        val start = i
        while i < sql.length && sql.charAt(i) != '\n' && sql.charAt(i) != '\r' do i += 1
        out += Token(TokenKind.Comment, sql.substring(start, i))
      else if c == '/' && i + 1 < sql.length && sql.charAt(i + 1) == '*' then
        val start = i
        val end = sql.indexOf("*/", i + 2)
        if end < 0 then reject("unterminated SQL block comment")
        i = end + 2
        out += Token(TokenKind.Comment, sql.substring(start, i))
      else if catalog && sql.regionMatches(true, i, "_utf8mb4\\'", 0, 10) then
        // 引号内保留可打印ASCII字面值（含正则符号），不将其解释为SQL；内嵌引号、反斜线和控制字符仍拒绝。
        i += 10
        val start = i
        while i < sql.length && sql.charAt(i) >= ' ' && sql.charAt(i) <= '~' &&
            sql.charAt(i) != '\\' && sql.charAt(i) != '\'' do i += 1
        if !sql.startsWith("\\'", i) then reject("unsupported escaped CHECK catalog literal")
        out += Token(TokenKind.Word, "_utf8mb4")
        out += Token(TokenKind.StringLiteral, "'" + sql.substring(start, i) + "'")
        i += 2
      else if isAsciiWordStart(c) then
        val start = i
        i += 1
        while i < sql.length && isAsciiWordPart(sql.charAt(i)) do i += 1
        out += Token(TokenKind.Word, asciiFold(sql.substring(start, i)))
      else if c >= '0' && c <= '9' then
        val start = i
        while i < sql.length && isAsciiDigit(sql.charAt(i)) do i += 1
        if i + 1 < sql.length && sql.charAt(i) == '.' && isAsciiDigit(sql.charAt(i + 1)) then
          i += 1
          while i < sql.length && isAsciiDigit(sql.charAt(i)) do i += 1
        if i < sql.length && (sql.charAt(i) == 'e' || sql.charAt(i) == 'E') then
          val exponent = i
          i += 1
          if i < sql.length && (sql.charAt(i) == '+' || sql.charAt(i) == '-') then i += 1
          val digits = i
          while i < sql.length && isAsciiDigit(sql.charAt(i)) do i += 1
          if digits == i then reject("malformed SQL numeric exponent")
          if i == exponent + 1 then reject("malformed SQL numeric exponent")
        if i < sql.length && (isAsciiWordPart(sql.charAt(i)) || sql.charAt(i) == '.') then
          reject("unsupported SQL numeric literal spelling")
        out += Token(TokenKind.Number, sql.substring(start, i))
      else
        val op = Vector("->>", "<=>", "->", "<=", ">=", "<>", "!=", "==", ":=", "||", "&&", "<<", ">>")
          .find(sql.startsWith(_, i))
        op match
          case Some(value) => out += Token(TokenKind.Symbol, value); i += value.length
          case None if "(),.;+-*/%=<>!~&|^?:@[]{}".contains(c) => out += Token(TokenKind.Symbol, c.toString); i += 1
          case None => reject("unsupported SQL source character")
    val tokens = out.result()
    validateParentheses(tokens)
    tokens

  private def parseTable(step: MySqlManagedSchemaPlan.Step, env: CreationEnvironment): Table =
    val tokens = tokenize(step.sql)
    if tokens.size < 7 || !starts(tokens, "CREATE", "TABLE") then reject("malformed compiled table statement")
    val name = identifier(tokens(2))
    if tokens(3) != symbol("(") then reject("compiled table statement has no opening column list")
    val close = matchingParen(tokens, 3)
    if close <= 3 then reject("compiled table definition is empty")
    val pieces = splitTopLevel(tokens.slice(4, close), ",")
    if pieces.exists(_.isEmpty) then reject("compiled table contains an empty column/constraint definition")
    val parts = pieces.map(parseTablePart)
    val columns = parts.collect { case c: Column => c }
    val constraints = parts.collect { case c: Constraint => c }
    if columns.isEmpty || columns.map(_.name).distinct.size != columns.size then
      reject("compiled table columns are empty or duplicated")
    val options = tokens.drop(close + 1)
    if options != words("ENGINE", "=", "InnoDB", "ROW_FORMAT", "=", "DYNAMIC", "DEFAULT", "CHARSET", "=",
        "utf8mb4", "COLLATE", "=", "utf8mb4_0900_bin") then
      reject("compiled table options are outside the fixed InnoDB profile")
    Table(name, columns, constraints, options, env, tokens, step.sha256)

  private def parseTablePart(tokens: Vector[Token]): TablePart =
    if tokens.isEmpty then reject("empty table element")
    if starts(tokens, "CONSTRAINT") then
      if tokens.size < 4 then reject("truncated named table constraint")
      val name = identifier(tokens(1))
      val tail = tokens.drop(2)
      if starts(tail, "PRIMARY", "KEY") then Constraint(ConstraintKind.PrimaryKey, Some(name), tokens)
      else if starts(tail, "UNIQUE") then Constraint(ConstraintKind.UniqueIndex, Some(name), tokens)
      else if starts(tail, "FOREIGN", "KEY") then Constraint(ConstraintKind.ForeignKey, Some(name), tokens)
      else if starts(tail, "CHECK") then Constraint(ConstraintKind.Check, Some(name), tokens)
      else reject("unsupported named table constraint kind")
    else if starts(tokens, "PRIMARY", "KEY") then Constraint(ConstraintKind.PrimaryKey, None, tokens)
    else if starts(tokens, "UNIQUE") then
      if tokens.size < 5 || !(isWord(tokens(1), "KEY") || isWord(tokens(1), "INDEX")) ||
          tokens(3) != symbol("(") then reject("compiled UNIQUE declaration is outside the named-key grammar")
      val name = Some(identifier(tokens(2)))
      if matchingParen(tokens, 3) != tokens.size - 1 then reject("compiled UNIQUE declaration has trailing unsupported syntax")
      Constraint(ConstraintKind.UniqueIndex, name, tokens)
    else if starts(tokens, "KEY") || starts(tokens, "INDEX") then
      if tokens.size < 4 || tokens(2) != symbol("(") || matchingParen(tokens, 2) != tokens.size - 1 then
        reject("truncated or unsupported named table index")
      Constraint(ConstraintKind.Index, Some(identifier(tokens(1))), tokens)
    else if starts(tokens, "FOREIGN", "KEY") || starts(tokens, "CHECK") then
      reject("compiled table constraint is missing its required explicit name")
    else
      val name = identifier(tokens.head)
      if tokens.size < 2 || tokens(1).kind != TokenKind.Word then reject("column declaration has no supported type token")
      val typeEnd = typeSourceEnd(tokens)
      val typeSource = tokens.slice(1, typeEnd)
      if typeSource.isEmpty then reject("column declaration type source is empty")
      val (nullable, generated) = parseColumnOptions(tokens.drop(typeEnd))
      Column(name, typeSource, tokens.drop(1), nullable, generated, tokens)

  private def parseColumnOptions(options: Vector[Token]): (Boolean, Option[GeneratedColumn]) =
    var cursor = 0
    var generated: Option[GeneratedColumn] = None
    if options.nonEmpty && isWord(options.head, "GENERATED") then
      if options.size < 7 || !starts(options, "GENERATED", "ALWAYS", "AS") || options(3) != symbol("(") then
        reject("compiled generated column is outside the GENERATED ALWAYS AS expression grammar")
      val close = matchingParen(options, 3)
      if close <= 4 then reject("compiled generated column expression is empty")
      val storagePos = close + 1
      if storagePos >= options.size || !(isWord(options(storagePos), "STORED") || isWord(options(storagePos), "VIRTUAL")) then
        reject("compiled generated column has no explicit STORED or VIRTUAL mode")
      val storage = wordUpper(options(storagePos))
      generated = Some(GeneratedColumn(options.slice(4, close), storage, options.take(storagePos + 1)))
      cursor = storagePos + 1
    val nullable = options.drop(cursor) match
      case Vector() => true
      case Vector(nullWord) if isWord(nullWord, "NULL") => true
      case Vector(notWord, nullWord) if isWord(notWord, "NOT") && isWord(nullWord, "NULL") => false
      case _ => reject("compiled column has unsupported or ambiguous nullability/options")
    (nullable, generated)

  private def typeSourceEnd(tokens: Vector[Token]): Int =
    if !SupportedColumnTypes(tokens(1).value) then reject("compiled column type is outside the supported schema6 recipe")
    var i = 2
    if i < tokens.size && tokens(i) == symbol("(") then
      val close = matchingParen(tokens, i)
      val dimensions = tokens.slice(i + 1, close)
      val isSingle = dimensions.size == 1 && dimensions.head.kind == TokenKind.Number
      val isPair = dimensions.size == 3 && dimensions(0).kind == TokenKind.Number &&
        dimensions(1) == symbol(",") && dimensions(2).kind == TokenKind.Number
      if !isSingle && !isPair then reject("compiled column type dimensions are outside the supported grammar")
      i = close + 1
    if i < tokens.size && isWord(tokens(i), "CHARACTER") then
      if i + 2 >= tokens.size || !isWord(tokens(i + 1), "SET") || !isIdentifier(tokens(i + 2)) then
        reject("compiled character column type has an unsupported charset clause")
      i += 3
    if i < tokens.size && isWord(tokens(i), "COLLATE") then
      if i + 1 >= tokens.size || !isIdentifier(tokens(i + 1)) then
        reject("compiled character column type has an unsupported collation clause")
      i += 2
    i

  private def parseTrigger(step: MySqlManagedSchemaPlan.Step, env: CreationEnvironment): Trigger =
    val tokens = tokenize(step.sql)
    if tokens.size <= 15 || !starts(tokens, "CREATE", "DEFINER") || tokens(2) != symbol("=") then
      reject("malformed compiled trigger statement")
    val (user, host) = account(tokens, 3)
    val triggerPos = tokens.indexWhere(isWord(_, "TRIGGER"))
    val beforePos = tokens.indexWhere(isWord(_, "BEFORE"))
    if triggerPos != 6 || beforePos != 8 || beforePos + 7 >= tokens.size then
      reject("compiled trigger header is outside the supported grammar")
    val name = identifier(tokens(7))
    val event = wordUpper(tokens(9))
    if !isWord(tokens(10), "ON") then reject("compiled trigger has no ON target")
    val table = identifier(tokens(11))
    if !starts(tokens.drop(12), "FOR", "EACH", "ROW", "BEGIN") then
      reject("compiled trigger body must use the fixed FOR EACH ROW BEGIN grammar")
    val bodyStart = 15
    val body = tokens.drop(bodyStart)
    if body.size < 2 || !isWord(body.head, "BEGIN") || !isWord(body.last, "END") then
      reject("compiled trigger has no bounded BEGIN/END body")
    Trigger(name, table, event, user, host, env, tokens.take(bodyStart), body, step.sha256)

  private def parseRoutine(step: MySqlManagedSchemaPlan.Step, env: CreationEnvironment, expectedSchema: String): AuditRoutine =
    val tokens = tokenize(step.sql)
    if !starts(tokens, "CREATE", "DEFINER") || tokens.size <= 6 || tokens(2) != symbol("=") then
      reject("malformed compiled audit routine")
    val (user, host) = account(tokens, 3)
    val procedurePos = tokens.indexWhere(isWord(_, "PROCEDURE"))
    if procedurePos != 6 || tokens.size <= procedurePos + 5 then reject("audit procedure header is unsupported")
    val schema = identifier(tokens(procedurePos + 1))
    if tokens(procedurePos + 2) != symbol(".") then reject("audit procedure database qualification is missing")
    val name = identifier(tokens(procedurePos + 3))
    if tokens(procedurePos + 4) != symbol("(") || tokens(procedurePos + 5) != symbol(")") then
      reject("audit procedure must have zero parameters")
    val bodyStart = tokens.indexWhere(isWord(_, "BEGIN"), procedurePos + 6)
    if bodyStart < 0 || !isWord(tokens.last, "END") then reject("audit procedure has no bounded BEGIN/END body")
    val header = tokens.take(bodyStart)
    val headerWords = header.filter(_.kind == TokenKind.Word).map(_.value)
    if !containsSequence(headerWords, Vector("sql", "security", "definer")) then
      reject("audit procedure release SQL lacks SQL SECURITY DEFINER")
    if !containsSequence(headerWords, Vector("reads", "sql", "data")) then
      reject("audit procedure release SQL lacks READS SQL DATA")
    if schema != expectedSchema || name != MySqlDbFirstSchemaDefinition.AuditRoutine then
      reject("audit procedure release identity differs from target schema or profile")
    AuditRoutine(schema, name, user, host, "DEFINER", "READS SQL DATA", env, header,
      tokens.drop(bodyStart), step.sha256)

  /** 仅比较 SOURCE 词法 token；不推断 SQL 语义或数据库引擎改写。 */
  private[mysql] def lexicallyEquivalent(left: String, right: String): Boolean = tokenize(left) == tokenize(right)

  private def matchingParen(tokens: Vector[Token], open: Int): Int =
    if open < 0 || open >= tokens.size || tokens(open) != symbol("(") then reject("SQL opening parenthesis is absent")
    var depth = 0
    var i = open
    while i < tokens.size do
      if tokens(i) == symbol("(") then depth += 1
      else if tokens(i) == symbol(")") then
        depth -= 1
        if depth == 0 then return i
        if depth < 0 then reject("SQL parenthesis nesting is malformed")
      i += 1
    reject("unterminated SQL parenthesis group")

  private def splitTopLevel(tokens: Vector[Token], delimiter: String): Vector[Vector[Token]] =
    val out = Vector.newBuilder[Vector[Token]]
    var depth = 0
    var start = 0
    var i = 0
    while i < tokens.size do
      if tokens(i) == symbol("(") then depth += 1
      else if tokens(i) == symbol(")") then
        depth -= 1
        if depth < 0 then reject("table definition has unmatched closing parenthesis")
      else if depth == 0 && tokens(i) == symbol(delimiter) then
        out += tokens.slice(start, i)
        start = i + 1
      i += 1
    if depth != 0 then reject("table definition has unmatched parentheses")
    out += tokens.drop(start)
    out.result()

  private def account(tokens: Vector[Token], start: Int): (String, String) =
    if start + 2 >= tokens.size || tokens(start).kind != TokenKind.StringLiteral ||
        tokens(start + 1) != symbol("@") || tokens(start + 2).kind != TokenKind.StringLiteral then
      reject("compiled object definer must use the fixed quoted user@host grammar")
    val user = stringContents(tokens(start))
    val host = stringContents(tokens(start + 2))
    if user.isEmpty || host.isEmpty then reject("compiled object definer account is incomplete")
    (user, host)

  private def stringContents(token: Token): String =
    if token.kind != TokenKind.StringLiteral || token.value.length < 2 then reject("SQL string token required")
    val body = token.value.substring(1, token.value.length - 1)
    val quote = token.value.head
    val doubled = quote.toString + quote.toString
    body.replace(doubled, quote.toString)

  private def identifier(token: Token): String = token.kind match
    case TokenKind.Word | TokenKind.QuotedWord => token.value
    case _ => reject("SQL identifier token required")

  private def asciiFold(value: String): String =
    val out = new StringBuilder(value.length)
    value.foreach { c => if c >= 'A' && c <= 'Z' then out.append((c + 32).toChar) else out.append(c) }
    out.toString

  private def isAsciiWordStart(c: Char): Boolean = c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
  private def isAsciiWordPart(c: Char): Boolean = isAsciiWordStart(c) || c >= '0' && c <= '9'
  private def isAsciiDigit(c: Char): Boolean = c >= '0' && c <= '9'
  private def isIdentifier(token: Token): Boolean = token.kind == TokenKind.Word || token.kind == TokenKind.QuotedWord
  private def isWord(token: Token, value: String): Boolean = token.kind == TokenKind.Word && token.value == value.toLowerCase(Locale.ROOT)
  private def isWordAt(tokens: Vector[Token], position: Int, value: String): Boolean =
    position >= 0 && position < tokens.size && isWord(tokens(position), value)
  private def wordUpper(token: Token): String = token.kind match
    case TokenKind.Word => token.value.toUpperCase(Locale.ROOT)
    case _ => reject("SQL keyword token required")
  private def containsSequence(haystack: Vector[String], needle: Vector[String]): Boolean =
    haystack.sliding(needle.size).exists(_ == needle)
  private def words(values: String*): Vector[Token] = values.map { value =>
    if value == "=" || value == "." || value == "(" || value == ")" then Token(TokenKind.Symbol, value)
    else if value.startsWith("`") && value.endsWith("`") then Token(TokenKind.QuotedWord, value.substring(1, value.length - 1))
    else Token(TokenKind.Word, asciiFold(value))
  }.toVector
  private def starts(tokens: Vector[Token], values: String*): Boolean = tokens.startsWith(words(values*))
  private def symbol(value: String): Token = Token(TokenKind.Symbol, value)
  private def validateParentheses(tokens: Vector[Token]): Unit =
    var depth = 0
    tokens.foreach { token =>
      if token == symbol("(") then depth += 1
      else if token == symbol(")") then
        depth -= 1
        if depth < 0 then reject("SQL parenthesis nesting is malformed")
    }
    if depth != 0 then reject("unterminated SQL parenthesis group")
  private def reject(message: String): Nothing = throw new SQLDataException(message)
