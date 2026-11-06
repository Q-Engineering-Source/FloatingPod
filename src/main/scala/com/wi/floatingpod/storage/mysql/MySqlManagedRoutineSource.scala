package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import MySqlManagedStaticRecipe.{Recipe, Token, TokenKind}

/** SHOW CREATE 保留原始转义；I_S 的有损正文仅核对目录传输表示，不能单独批准执行语义。 */
private[mysql] object MySqlManagedRoutineSource:
  val ResultId = "SHOW_CREATE_AUDIT_ROUTINE"
  private val Labels = Vector("Procedure", "sql_mode", "Create Procedure", "character_set_client",
    "collation_connection", "Database Collation")

  def validate(recipe: Recipe, catalogBody: String, actual: MySqlAuditResultSet, root: MySqlFormat3ReadBudget): Unit =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if actual == null || catalogBody == null then reject("managed SHOW CREATE evidence is required")
    val textLength = actual.rows.foldLeft(0L) { (sum,row) =>
      row.cells.foldLeft(sum) { (current,cell) => cell match
        case MySqlAuditText(value) => Math.addExact(current,value.length.toLong)
        case _ => current
      }
    }
    val workspace = Math.addExact(4096L,Math.multiplyExact(128L,Math.addExact(textLength,catalogBody.length.toLong)))
    MySqlManifestConfigurationIdentity.temporary(root,workspace) { validateContents(recipe,catalogBody,actual) }

  private def validateContents(recipe: Recipe, catalogBody: String, actual: MySqlAuditResultSet): Unit =
    if actual == null || actual.id != ResultId || actual.columns.map(_.label) != Labels || actual.rows.size != 1 ||
        actual.rows.head.cells.size != Labels.size then reject("managed SHOW CREATE procedure shape differs")
    def text(index: Int): String = actual.rows.head.cells(index) match
      case MySqlAuditText(value) => value
      case _ => reject("managed SHOW CREATE procedure field is NULL or not text")
    val expected = recipe.auditRoutine
    if text(0) != expected.name || modes(text(1)) != modes(expected.environment.sqlMode) ||
        text(3) != expected.environment.characterSetClient || text(4) != expected.environment.collationConnection ||
        text(5) != expected.environment.requiredDatabaseCollation then reject("managed SHOW CREATE procedure identity/environment differs")
    val sql = text(2)
    val tokens = MySqlManagedStaticRecipe.tokenize(sql)
    def word(value: String) = Token(TokenKind.Word,value)
    def quoted(value: String) = Token(TokenKind.QuotedWord,value)
    def symbol(value: String) = Token(TokenKind.Symbol,value)
    val comment = expected.headerTokens.sliding(2).collect {
      case Vector(key,value) if key == word("comment") && value.kind == TokenKind.StringLiteral => value
    }.toVector
    if comment.size != 1 then reject("managed SOURCE routine comment is not exact")
    // SHOW CREATE 省略默认 SQL SECURITY DEFINER；该值仍由独立 I_S 元数据严格验证。
    val header = Vector(word("create"),word("definer"),symbol("="),quoted(expected.definerUser),symbol("@"),
      quoted(expected.definerHost),word("procedure"),quoted(expected.name),symbol("("),symbol(")"),
      word("reads"),word("sql"),word("data"),word("comment"),comment.head)
    if tokens != header ++ expected.bodyTokens then reject("managed SHOW CREATE procedure source differs")
    val begin = sql.indexOf("\nBEGIN")
    if begin < 0 then reject("managed SHOW CREATE procedure body boundary differs")
    val body = sql.substring(begin+1)
    if MySqlManagedStaticRecipe.tokenize(body) != expected.bodyTokens then reject("managed SHOW CREATE procedure body differs")
    if catalogRepresentation(body) != catalogBody then reject("managed routine I_S body differs from verified SHOW CREATE representation")

  private def modes(value: String): Vector[String] =
    val parts = value.split(",",-1).toVector
    if parts.exists(!_.matches("[A-Z_]+")) || parts.distinct.size != parts.size then reject("managed SHOW CREATE SQL_MODE malformed")
    parts.sorted

  /** 固定发布语法仅使用 doubled quote；不把有损目录文字重新解释成 SQL。 */
  private[mysql] def catalogRepresentation(body: String): String =
    val out = new StringBuilder
    var i = 0
    while i < body.length do
      val c = body.charAt(i)
      if body.startsWith("--",i) && (i+2 == body.length || Character.isWhitespace(body.charAt(i+2))) then
        val start = i
        while i < body.length && body.charAt(i) != '\n' && body.charAt(i) != '\r' do i += 1
        out.append(body.substring(start,i))
      else if body.startsWith("/*",i) then
        val end = body.indexOf("*/",i+2)
        if end < 0 then reject("managed routine comment is unterminated")
        out.append(body.substring(i,end+2)); i = end+2
      else if c == '\'' || c == '"' || c == '`' then
        val quote = c
        out.append(quote); i += 1
        var closed = false
        while i < body.length && !closed do
          val value = body.charAt(i)
          if value == '\\' then reject("managed routine catalog projection does not support backslash escapes")
          if value == quote && i+1 < body.length && body.charAt(i+1) == quote then
            out.append(quote)
            if quote == '`' then out.append(quote)
            i += 2
          else if value == quote then
            out.append(quote); i += 1; closed = true
          else
            out.append(value); i += 1
        if !closed then reject("managed routine literal/identifier is unterminated")
      else
        out.append(c); i += 1
    out.toString

  private def reject(message: String): Nothing = throw new SQLDataException(message)
