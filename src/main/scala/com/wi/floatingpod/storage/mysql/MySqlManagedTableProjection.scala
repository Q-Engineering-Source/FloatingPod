package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.Locale

/**
  * Derives the release-owned, static TABLES/COLUMNS/STATISTICS/constraint catalog projection
  * from the exact managed schema6 source. It is DATA only: it neither observes JDBC metadata
  * nor authorizes installation, runtime readiness, or recovery.
  */
private[mysql] object MySqlManagedTableProjection:
  import MySqlManagedStaticRecipe.*

  /** Generated-column source expressions are deliberately handed to the later AST comparator. */
  private[mysql] enum ExpectedField:
    case Exact(value: MySqlAuditCell)
    case GeneratedExpression(tokens: Vector[Token])

  private[mysql] final case class ExpectedSet(id: String, rows: Vector[Vector[ExpectedField]])
  private[mysql] final case class Projection(sets: Vector[ExpectedSet]):
    require(sets.map(_.id) == Vector(
      "RS02_TABLES", "RS03_COLUMNS", "RS04_STATISTICS", "RS05_TABLE_CONSTRAINTS",
      "RS06_KEY_COLUMN_USAGE", "RS07_REFERENTIAL_CONSTRAINTS"),
      "managed table projection must contain the exact ordered catalog sets")
    def set(id: String): ExpectedSet =
      sets.find(_.id == id).getOrElse(throw new IllegalArgumentException("unknown managed catalog projection set"))

  private final case class Index(name: String, columns: Vector[String], unique: Boolean, kind: ConstraintKind,
      automaticallyCreated: Boolean = false)
  private final case class ForeignKey(
      name: String,
      childColumns: Vector[String],
      requestedIndexName: Option[String],
      parentTable: String,
      requestedParentColumns: Option[Vector[String]],
      deleteRule: String,
      updateRule: String
  )
  private final case class Check(name: String, enforced: String)
  private final case class TableModel(
      source: Table,
      indexes: Vector[Index],
      foreignKeys: Vector[ForeignKey],
      checks: Vector[Check]
  ):
    def primary: Index = indexes.find(_.kind == ConstraintKind.PrimaryKey).getOrElse(
      reject("managed table source has no primary-key catalog projection"))

  private final case class ColumnType(
      dataType: String,
      columnType: String,
      characterMaximumLength: Option[Long],
      characterOctetLength: Option[Long],
      numericPrecision: Option[Int],
      numericScale: Option[Int],
      datetimePrecision: Option[Int],
      characterSet: Option[String],
      collation: Option[String]
  )

  /** Rechecks the exact rendered plan and refuses recipes from any other installation identity. */
  def project(recipe: Recipe, rendered: MySqlDbFirstSchemaProfile.Rendered): Projection =
    if recipe == null || rendered == null then reject("managed table projection requires a recipe and final rendering")
    val plan = MySqlManagedSchemaPlan.build(rendered)
    projectAtPlan(recipe, plan, plan.deferredConstraintSteps.map(_.ordinal).toSet)

  /** Projects only the exact table-constraint prefix authorized by the installation journal. */
  def projectAt(
      recipe: Recipe,
      rendered: MySqlDbFirstSchemaProfile.Rendered,
      appliedConstraintOrdinals: Set[Int]
  ): Projection =
    if recipe == null || rendered == null then reject("managed table projection requires a recipe and final rendering")
    val plan = MySqlManagedSchemaPlan.build(rendered)
    projectAtPlan(recipe, plan, appliedConstraintOrdinals)

  private def projectAtPlan(
      recipe: Recipe,
      plan: MySqlManagedSchemaPlan.Plan,
      appliedConstraintOrdinals: Set[Int]
  ): Projection =
    val canonical = MySqlManagedStaticRecipe.fromPlanAt(plan, appliedConstraintOrdinals)
    if recipe.schema != canonical.schema || recipe.ddlSha256 != canonical.ddlSha256 ||
        recipe.profileIdentitySha256 != canonical.profileIdentitySha256 || recipe.tables != canonical.tables ||
        recipe.triggers != canonical.triggers || recipe.auditRoutine != canonical.auditRoutine ||
        recipe.sessionEnvironments != canonical.sessionEnvironments ||
        recipe.appliedConstraintOrdinals != canonical.appliedConstraintOrdinals then
      reject("managed table recipe differs from the exact final rendered schema6 identity")

    val parsed = recipe.tables.map(parseTable)
    if parsed.map(_.source.name) != MySqlDbFirstSchemaProfile.TableNames ||
        parsed.map(_.source.columns.size).sum != 480 then
      reject("managed catalog source differs from the exact release Full27/480-column profile")
    val byTable = parsed.map(table => table.source.name -> table).toMap
    val indexesWithForeignSupport = parsed.map { table =>
      val all = table.foreignKeys.foldLeft(table.indexes) { (current, foreignKey) =>
        if current.exists(index => index.columns.startsWith(foreignKey.childColumns)) then current
        else
          val name = foreignKey.requestedIndexName.getOrElse(foreignKey.name)
          if current.exists(_.name == name) then
            reject("managed foreign-key auto-index name collides with another source index")
          // InnoDB 会以新外键的较长左前缀索引替换已生成的冗余短索引；显式索引不能被此规则删除。
          // https://dev.mysql.com/doc/refman/9.7/en/create-table-foreign-keys.html
          val retained = current.filterNot(index => index.automaticallyCreated &&
            foreignKey.childColumns.startsWith(index.columns))
          retained :+ Index(name, foreignKey.childColumns, unique = false, kind = ConstraintKind.Index,
            automaticallyCreated = true)
      }
      table.source.name -> all
    }.toMap

    val resolvedForeignKeys = parsed.flatMap { table =>
      table.foreignKeys.map { foreignKey =>
        val parent = byTable.getOrElse(foreignKey.parentTable,
          reject("managed foreign key references a table outside the exact schema6 release"))
        val parentColumns = foreignKey.requestedParentColumns.getOrElse(parent.primary.columns)
        val candidates = parent.indexes.filter(index => index.unique && index.columns == parentColumns)
        if candidates.size != 1 then
          reject("managed foreign key does not resolve to one exact parent primary or unique key")
        (table.source.name, foreignKey, parentColumns, candidates.head.name)
      }
    }

    val tableRows = parsed.sortBy(_.source.name).map { table =>
      Vector(text(recipe.schema), text(table.source.name), text("BASE TABLE"), text("InnoDB"), text("10"),
        text("Dynamic"), text("utf8mb4_0900_bin"), text("row_format=DYNAMIC"), text(""))
    }
    val columnRows = parsed.sortBy(_.source.name).flatMap { table =>
      table.source.columns.zipWithIndex.map { (column, zeroBasedPosition) =>
        if column.tokens.exists(token => isWord(token, "DEFAULT")) then
          reject("managed release column has an unprojected DEFAULT clause")
        if column.tokens.exists(token => isWord(token, "COMMENT")) then
          reject("managed release column has an unprojected comment clause")
        val columnType = parseColumnType(table.source, column)
        val key = columnKey(column.name, indexesWithForeignSupport(table.source.name))
        val extra = column.generated.fold("")(generated =>
          if generated.storage == "STORED" then "STORED GENERATED" else "VIRTUAL GENERATED")
        val generationExpression = column.generated match
          case Some(generated) => ExpectedField.GeneratedExpression(generated.expression)
          case None => exact("")
        Vector(
          exact(recipe.schema), exact(table.source.name), exact(column.name), exact((zeroBasedPosition + 1).toString),
          ExpectedField.Exact(MySqlAuditNull), exact(if column.nullable then "YES" else "NO"),
          exact(columnType.dataType), optionalLong(columnType.characterMaximumLength),
          optionalLong(columnType.characterOctetLength), optionalInt(columnType.numericPrecision),
          optionalInt(columnType.numericScale), optionalInt(columnType.datetimePrecision),
          optionalText(columnType.characterSet), optionalText(columnType.collation), exact(columnType.columnType),
          exact(key), exact(extra), exact(""), generationExpression)
      }
    }

    val statisticRows = parsed.sortBy(_.source.name).flatMap { table =>
      val columnsByName = table.source.columns.map(column => column.name -> column).toMap
      indexesWithForeignSupport(table.source.name).sortBy(_.name).flatMap { index =>
        index.columns.zipWithIndex.map { (columnName, zeroBasedPosition) =>
          val column = columnsByName.getOrElse(columnName,
            reject("managed index references a column outside its table definition"))
          Vector(
            exact(recipe.schema), exact(table.source.name), exact(if index.unique then "0" else "1"),
            exact(recipe.schema), exact(index.name), exact((zeroBasedPosition + 1).toString), exact(columnName),
            exact("A"), ExpectedField.Exact(MySqlAuditNull), ExpectedField.Exact(MySqlAuditNull),
            exact(if column.nullable then "YES" else ""), exact("BTREE"), exact(""), exact(""),
            exact("YES"), ExpectedField.Exact(MySqlAuditNull))
        }
      }
    }

    val constraintRows = parsed.sortBy(_.source.name).flatMap { table =>
      val keys = table.indexes.filter(index => index.kind == ConstraintKind.PrimaryKey ||
        index.kind == ConstraintKind.UniqueIndex).map { index =>
        val kind = if index.kind == ConstraintKind.PrimaryKey then "PRIMARY KEY" else "UNIQUE"
        (index.name, kind)
      }
      val foreign = table.foreignKeys.map(fk => (fk.name, "FOREIGN KEY"))
      val checks = table.checks.map(check => (check.name, "CHECK"))
      (keys ++ foreign ++ checks).sortBy { (name, kind) => (name, kind) }.map { (name, kind) =>
        val enforced = table.checks.find(_.name == name).map(_.enforced).getOrElse("YES")
        Vector(exact("def"), exact(recipe.schema), exact(name), exact(recipe.schema),
          exact(table.source.name), exact(kind), exact(enforced))
      }
    }

    val keyColumnRows = parsed.sortBy(_.source.name).flatMap { table =>
      val keyIndexes = table.indexes.filter(index => index.kind == ConstraintKind.PrimaryKey ||
        index.kind == ConstraintKind.UniqueIndex)
      val localKeys = keyIndexes.flatMap { index =>
        index.columns.zipWithIndex.map { (columnName, zeroBasedPosition) =>
          Vector(exact("def"), exact(recipe.schema), exact(index.name), exact("def"), exact(recipe.schema),
            exact(table.source.name), exact(columnName), exact((zeroBasedPosition + 1).toString),
            ExpectedField.Exact(MySqlAuditNull), ExpectedField.Exact(MySqlAuditNull),
            ExpectedField.Exact(MySqlAuditNull), ExpectedField.Exact(MySqlAuditNull))
        }
      }
      val foreignRows = resolvedForeignKeys.filter(_._1 == table.source.name).flatMap {
        (_, foreignKey, parentColumns, _) =>
          val resolvedParentColumns = foreignKey.requestedParentColumns.getOrElse(parentColumns)
          foreignKey.childColumns.zip(resolvedParentColumns).zipWithIndex.map {
            case ((childColumn, _), zeroBasedPosition) =>
              Vector(exact("def"), exact(recipe.schema), exact(foreignKey.name), exact("def"), exact(recipe.schema),
                exact(table.source.name), exact(childColumn), exact((zeroBasedPosition + 1).toString),
                exact((zeroBasedPosition + 1).toString), exact(recipe.schema), exact(foreignKey.parentTable),
                exact(resolvedParentColumns(zeroBasedPosition)))
          }
      }
      (localKeys ++ foreignRows).sortBy { row =>
        (literal(row(5)).get, literal(row(2)).get, literal(row(7)).get.toInt)
      }
    }

    val referentialRows = resolvedForeignKeys.sortBy { (tableName, foreignKey, _, _) =>
      (tableName, foreignKey.name)
    }.map { (tableName, foreignKey, _, parentIndexName) =>
      Vector(exact("def"), exact(recipe.schema), exact(foreignKey.name), exact("def"), exact(recipe.schema),
        exact(parentIndexName), exact("NONE"), exact(foreignKey.updateRule), exact(foreignKey.deleteRule),
        exact(tableName), exact(foreignKey.parentTable))
    }

    Projection(Vector(
      ExpectedSet("RS02_TABLES", tableRows),
      ExpectedSet("RS03_COLUMNS", columnRows),
      ExpectedSet("RS04_STATISTICS", statisticRows),
      ExpectedSet("RS05_TABLE_CONSTRAINTS", constraintRows),
      ExpectedSet("RS06_KEY_COLUMN_USAGE", keyColumnRows),
      ExpectedSet("RS07_REFERENTIAL_CONSTRAINTS", referentialRows)))

  private def parseTable(source: Table): TableModel =
    val indexes = Vector.newBuilder[Index]
    val foreignKeys = Vector.newBuilder[ForeignKey]
    val checks = Vector.newBuilder[Check]
    source.constraints.foreach { constraint =>
      constraint.kind match
        case ConstraintKind.PrimaryKey | ConstraintKind.UniqueIndex | ConstraintKind.Index =>
          indexes += parseIndex(constraint)
        case ConstraintKind.ForeignKey => foreignKeys += parseForeignKey(constraint)
        case ConstraintKind.Check => checks += parseCheck(constraint)
    }
    val parsedIndexes = indexes.result()
    if parsedIndexes.count(_.kind == ConstraintKind.PrimaryKey) != 1 then
      reject("managed table source must have exactly one primary key")
    if parsedIndexes.map(_.name).distinct.size != parsedIndexes.size then
      reject("managed table source contains duplicate index identities")
    val parsedForeignKeys = foreignKeys.result()
    val columnNames = source.columns.iterator.map(_.name).toSet
    if parsedForeignKeys.exists(fk => fk.childColumns.exists(column => !columnNames.contains(column))) then
      reject("managed foreign key references a child column outside its table definition")
    val keyConstraintNames = parsedIndexes.filter(index => index.kind == ConstraintKind.PrimaryKey ||
      index.kind == ConstraintKind.UniqueIndex).map(_.name) ++ parsedForeignKeys.map(_.name)
    if keyConstraintNames.distinct.size != keyConstraintNames.size then
      reject("managed table source has ambiguous key-column-usage constraint ordering")
    TableModel(source, parsedIndexes, parsedForeignKeys, checks.result())

  private def parseIndex(constraint: Constraint): Index =
    val tokens = constraint.tokens
    var cursor = 0
    if isWord(tokens(cursor), "CONSTRAINT") then
      requireNamedConstraint(tokens, constraint)
      cursor += 2
    val (name, unique, kind, openPosition) = constraint.kind match
      case ConstraintKind.PrimaryKey =>
        if !isWordAt(tokens, cursor, "PRIMARY") || !isWordAt(tokens, cursor + 1, "KEY") then
          reject("managed primary-key source uses unsupported syntax")
        ("PRIMARY", true, ConstraintKind.PrimaryKey, cursor + 2)
      case ConstraintKind.UniqueIndex =>
        if !isWordAt(tokens, cursor, "UNIQUE") then reject("managed unique-index source uses unsupported syntax")
        cursor += 1
        if isWordAt(tokens, cursor, "KEY") || isWordAt(tokens, cursor, "INDEX") then cursor += 1
        if cursor >= tokens.size || tokens(cursor) == symbol("(") then
          reject("managed unique index must have an explicit index identifier")
        val indexName = identifier(tokens(cursor)); cursor += 1
        (indexName, true, ConstraintKind.UniqueIndex, cursor)
      case ConstraintKind.Index =>
        if !(isWordAt(tokens, cursor, "KEY") || isWordAt(tokens, cursor, "INDEX")) then
          reject("managed nonunique index source uses unsupported syntax")
        cursor += 1
        if cursor >= tokens.size || tokens(cursor) == symbol("(") then
          reject("managed nonunique index must have an explicit index identifier")
        val indexName = identifier(tokens(cursor)); cursor += 1
        (indexName, false, ConstraintKind.Index, cursor)
      case _ => reject("managed index parser received a non-index source constraint")
    if openPosition >= tokens.size || tokens(openPosition) != symbol("(") then
      reject("managed index has no key-part list")
    val closePosition = matchingParen(tokens, openPosition)
    if closePosition != tokens.size - 1 then reject("managed index has unsupported trailing options")
    val columns = parseColumnList(tokens, openPosition)
    Index(name, columns, unique, kind)

  private def parseForeignKey(constraint: Constraint): ForeignKey =
    val tokens = constraint.tokens
    var cursor = 0
    if isWordAt(tokens, cursor, "CONSTRAINT") then
      requireNamedConstraint(tokens, constraint)
      cursor += 2
    val name = constraint.name.getOrElse(reject("managed foreign key must have an explicit name"))
    if !isWordAt(tokens, cursor, "FOREIGN") || !isWordAt(tokens, cursor + 1, "KEY") then
      reject("managed foreign-key source uses unsupported syntax")
    cursor += 2
    val requestedIndexName = if cursor < tokens.size && tokens(cursor) != symbol("(") then
      val value = identifier(tokens(cursor)); cursor += 1; Some(value)
    else None
    if cursor >= tokens.size || tokens(cursor) != symbol("(") then reject("managed foreign key has no child-key list")
    val childColumns = parseColumnList(tokens, cursor)
    cursor = matchingParen(tokens, cursor) + 1
    if !isWordAt(tokens, cursor, "REFERENCES") then reject("managed foreign key has no REFERENCES target")
    cursor += 1
    if cursor >= tokens.size then reject("managed foreign key target is missing")
    val parentTable = identifier(tokens(cursor)); cursor += 1
    if cursor < tokens.size && tokens(cursor) == symbol(".") then
      reject("managed foreign keys must use the local unqualified table namespace")
    val requestedParentColumns = if cursor < tokens.size && tokens(cursor) == symbol("(") then
      val value = parseColumnList(tokens, cursor)
      cursor = matchingParen(tokens, cursor) + 1
      Some(value)
    else None
    var deleteRule = "NO ACTION"
    var updateRule = "NO ACTION"
    var sawDelete = false
    var sawUpdate = false
    while cursor < tokens.size do
      if isWordAt(tokens, cursor, "ON") && cursor + 2 < tokens.size then
        val actionKind = if isWordAt(tokens, cursor + 1, "DELETE") then "DELETE"
          else if isWordAt(tokens, cursor + 1, "UPDATE") then "UPDATE"
          else reject("managed foreign-key action has an unknown ON target")
        val action = parseAction(tokens, cursor + 2)
        if actionKind == "DELETE" then
          if sawDelete then reject("managed foreign key repeats ON DELETE")
          deleteRule = action._1; sawDelete = true
        else
          if sawUpdate then reject("managed foreign key repeats ON UPDATE")
          updateRule = action._1; sawUpdate = true
        cursor = action._2
      else reject("managed foreign key has unsupported trailing syntax")
    if childColumns.isEmpty || requestedParentColumns.exists(_.size != childColumns.size) then
      reject("managed foreign-key child and parent tuples have incompatible arity")
    ForeignKey(name, childColumns, requestedIndexName, parentTable, requestedParentColumns, deleteRule, updateRule)

  private def parseAction(tokens: Vector[Token], start: Int): (String, Int) =
    if isWordAt(tokens, start, "CASCADE") then ("CASCADE", start + 1)
    else if isWordAt(tokens, start, "RESTRICT") then ("RESTRICT", start + 1)
    else if isWordAt(tokens, start, "NO") && isWordAt(tokens, start + 1, "ACTION") then ("NO ACTION", start + 2)
    else if isWordAt(tokens, start, "SET") && isWordAt(tokens, start + 1, "NULL") then ("SET NULL", start + 2)
    else if isWordAt(tokens, start, "SET") && isWordAt(tokens, start + 1, "DEFAULT") then
      reject("InnoDB does not support SET DEFAULT foreign-key actions")
    else reject("managed foreign-key referential action is unsupported")

  private def parseCheck(constraint: Constraint): Check =
    val tokens = constraint.tokens
    var cursor = 0
    if isWordAt(tokens, cursor, "CONSTRAINT") then
      requireNamedConstraint(tokens, constraint)
      cursor += 2
    val name = constraint.name.getOrElse(reject("managed CHECK constraint must have an explicit name"))
    if !isWordAt(tokens, cursor, "CHECK") || cursor + 1 >= tokens.size || tokens(cursor + 1) != symbol("(") then
      reject("managed CHECK source uses unsupported syntax")
    val close = matchingParen(tokens, cursor + 1)
    val suffix = tokens.drop(close + 1)
    val enforced = suffix match
      case Vector() => "YES"
      case Vector(token) if isWord(token, "ENFORCED") => "YES"
      case Vector(not, enforcedToken) if isWord(not, "NOT") && isWord(enforcedToken, "ENFORCED") => "NO"
      case _ => reject("managed CHECK constraint has unsupported enforcement syntax")
    Check(name, enforced)

  private def parseColumnType(table: Table, column: Column): ColumnType =
    val tokens = column.typeSource
    if tokens.isEmpty then reject("managed column type source is empty")
    val dataType = tokens.head.value.toLowerCase(Locale.ROOT)
    var cursor = 1
    var dimensions: Option[Vector[Int]] = None
    if cursor < tokens.size && tokens(cursor) == symbol("(") then
      val close = matchingParen(tokens, cursor)
      val dims = tokens.slice(cursor + 1, close)
      val parsed = dims match
        case Vector(number) if number.kind == TokenKind.Number => Vector(parsePositiveInt(number.value))
        case Vector(left, comma, right) if left.kind == TokenKind.Number && comma == symbol(",") && right.kind == TokenKind.Number =>
          Vector(parsePositiveInt(left.value), parsePositiveInt(right.value))
        case _ => reject("managed column type dimension is outside the closed numeric grammar")
      dimensions = Some(parsed)
      cursor = close + 1
    var characterSet: Option[String] = None
    var collation: Option[String] = None
    if cursor < tokens.size then
      if !isWordAt(tokens, cursor, "CHARACTER") || !isWordAt(tokens, cursor + 1, "SET") || cursor + 2 >= tokens.size then
        reject("managed column type contains an unsupported charset clause")
      characterSet = Some(identifier(tokens(cursor + 2)))
      cursor += 3
    if cursor < tokens.size then
      if !isWordAt(tokens, cursor, "COLLATE") || cursor + 1 >= tokens.size then
        reject("managed column type contains unsupported trailing syntax")
      collation = Some(identifier(tokens(cursor + 1)))
      cursor += 2
    if cursor != tokens.size then reject("managed column type parser left unconsumed tokens")

    val effectiveCharacterSet = if Set("varchar", "longtext").contains(dataType) then
      val actual = characterSet.getOrElse(table.environment.requiredDatabaseCharacterSet)
      if actual != "utf8mb4" then reject("managed character column charset differs from the pinned release profile")
      Some(actual)
    else
      if characterSet.nonEmpty || collation.nonEmpty then reject("binary/numeric managed column has a character collation")
      None
    val effectiveCollation = effectiveCharacterSet.map { _ =>
      val actual = collation.getOrElse(table.environment.requiredDatabaseCollation)
      if actual != "utf8mb4_0900_bin" then reject("managed character column collation differs from the pinned release profile")
      actual
    }

    def length(n: Long, bytesPerCharacter: Long): (Option[Long], Option[Long]) =
      (Some(n), Some(Math.multiplyExact(n, bytesPerCharacter)))
    val noLength = (None, None)
    val (characterMaximumLength, characterOctetLength, numericPrecision, numericScale, datetimePrecision, columnType) =
      dataType match
        case "varchar" =>
          val size = dimensions match
            case Some(Vector(n)) => n.toLong
            case _ => reject("managed VARCHAR requires one exact character length")
          if size <= 0 || size > 16383 then reject("managed VARCHAR length is outside the MySQL 9.7 utf8mb4 bound")
          val (chars, octets) = length(size, 4L)
          (chars, octets, None, None, None, s"varchar($size)")
        case "varbinary" =>
          val size = dimensions match
            case Some(Vector(n)) => n.toLong
            case _ => reject("managed VARBINARY requires one exact byte length")
          if size <= 0 || size > 65535 then reject("managed VARBINARY length is outside the supported bound")
          val (chars, octets) = length(size, 1L)
          (chars, octets, None, None, None, s"varbinary($size)")
        case "longblob" =>
          if dimensions.nonEmpty then reject("managed LONGBLOB cannot have a dimension")
          (Some(4294967295L), Some(4294967295L), None, None, None, "longblob")
        case "mediumblob" =>
          if dimensions.nonEmpty then reject("managed MEDIUMBLOB cannot have a dimension")
          (Some(16777215L), Some(16777215L), None, None, None, "mediumblob")
        case "longtext" =>
          if dimensions.nonEmpty then reject("managed LONGTEXT cannot have a dimension")
          // MySQL 9.7.2 routes LONGTEXT through MYSQL_TYPE_LONG_BLOB; the character-length
          // calculation divides by utf8mb4.mbminlen (1), while the octet result is byte-based.
          (Some(4294967295L), Some(4294967295L), None, None, None, "longtext")
        case "bigint" => integerType(dataType, dimensions, 19)
        case "int" => integerType(dataType, dimensions, 10)
        case "smallint" => integerType(dataType, dimensions, 5)
        case "tinyint" => integerType(dataType, dimensions, 3)
        case "double" =>
          if dimensions.nonEmpty then reject("managed DOUBLE source cannot declare legacy display width/scale")
          (noLength._1, noLength._2, Some(22), None, None, "double")
        case "datetime" =>
          val precision = dimensions match
            case None => 0
            case Some(Vector(n)) if n >= 0 && n <= 6 => n
            case _ => reject("managed DATETIME precision is outside the MySQL 0..6 range")
          (None, None, None, None, Some(precision), dimensions.fold("datetime")(_ => s"datetime($precision)"))
        case _ => reject("managed column data type is outside the release projection grammar")
    ColumnType(dataType, columnType, characterMaximumLength, characterOctetLength,
      numericPrecision, numericScale, datetimePrecision, effectiveCharacterSet, effectiveCollation)

  private def integerType(dataType: String, dimensions: Option[Vector[Int]], precision: Int):
      (Option[Long], Option[Long], Option[Int], Option[Int], Option[Int], String) =
    if dimensions.nonEmpty then reject("managed integer source cannot use an obsolete display width")
    (None, None, Some(precision), Some(0), None, dataType)

  private def columnKey(column: String, indexes: Vector[Index]): String =
    if indexes.exists(index => index.kind == ConstraintKind.PrimaryKey && index.columns.contains(column)) then "PRI"
    else if indexes.exists(index => index.kind == ConstraintKind.UniqueIndex && index.columns == Vector(column)) then "UNI"
    else if indexes.exists(index => index.columns.headOption.contains(column)) then "MUL"
    else ""

  private def parseColumnList(tokens: Vector[Token], openPosition: Int): Vector[String] =
    if openPosition >= tokens.size || tokens(openPosition) != symbol("(") then
      reject("managed key tuple has no opening parenthesis")
    val close = matchingParen(tokens, openPosition)
    val inside = tokens.slice(openPosition + 1, close)
    val names = Vector.newBuilder[String]
    var cursor = 0
    while cursor < inside.size do
      if inside(cursor).kind != TokenKind.Word && inside(cursor).kind != TokenKind.QuotedWord then
        reject("managed key tuple contains a non-column key part")
      names += identifier(inside(cursor))
      cursor += 1
      if cursor < inside.size then
        if inside(cursor) != symbol(",") then reject("managed key tuple contains a prefix or ordering expression")
        cursor += 1
    val result = names.result()
    if result.isEmpty || result.distinct.size != result.size then reject("managed key tuple is empty or repeats a column")
    result

  private def parsePositiveInt(value: String): Int =
    try value.toInt
    catch case _: NumberFormatException => reject("managed type dimension is outside the supported integer range")

  private def matchingParen(tokens: Vector[Token], openPosition: Int): Int =
    if openPosition >= tokens.size || tokens(openPosition) != symbol("(") then
      reject("managed catalog SQL group has no opening parenthesis")
    var depth = 0
    var cursor = openPosition
    while cursor < tokens.size do
      if tokens(cursor) == symbol("(") then depth += 1
      else if tokens(cursor) == symbol(")") then
        depth -= 1
        if depth == 0 then return cursor
        if depth < 0 then reject("managed catalog SQL parentheses are malformed")
      cursor += 1
    reject("managed catalog SQL group has no closing parenthesis")

  private def requireNamedConstraint(tokens: Vector[Token], constraint: Constraint): Unit =
    if tokens.size < 2 || constraint.name.forall(_ != identifier(tokens(1))) then
      reject("managed named constraint identity differs from its parsed source")

  private def identifier(token: Token): String =
    if token == null || !(token.kind == TokenKind.Word || token.kind == TokenKind.QuotedWord) ||
        !token.value.matches("[A-Za-z0-9_]{1,64}") then
      reject("managed catalog source contains an identifier outside the safe release grammar")
    token.value

  private def isWord(token: Token, expected: String): Boolean =
    token != null && token.kind == TokenKind.Word && token.value.equalsIgnoreCase(expected)

  private def isWordAt(tokens: Vector[Token], position: Int, expected: String): Boolean =
    position >= 0 && position < tokens.size && isWord(tokens(position), expected)

  private def symbol(value: String): Token = Token(TokenKind.Symbol, value)

  private def text(value: String): ExpectedField = exact(value)
  private def exact(value: String): ExpectedField = ExpectedField.Exact(MySqlAuditText(value))
  private def optionalText(value: Option[String]): ExpectedField = value.fold(ExpectedField.Exact(MySqlAuditNull))(exact)
  private def optionalInt(value: Option[Int]): ExpectedField = value.fold(ExpectedField.Exact(MySqlAuditNull))(n => exact(n.toString))
  private def optionalLong(value: Option[Long]): ExpectedField = value.fold(ExpectedField.Exact(MySqlAuditNull))(n => exact(n.toString))

  private def literal(field: ExpectedField): Option[String] = field match
    case ExpectedField.Exact(MySqlAuditText(value)) => Some(value)
    case _ => None

  private def reject(message: String): Nothing = throw new SQLDataException(message)
