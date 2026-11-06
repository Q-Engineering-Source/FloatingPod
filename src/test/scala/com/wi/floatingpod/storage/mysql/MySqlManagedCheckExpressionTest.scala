package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedCheckExpressionTest:
  private def renderedRecipe: MySqlManagedStaticRecipe.Recipe =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    val inputs = MySqlDeploymentReceiptSchemaProfile.inputs("fp_managed_expression_test",
      Accounts(account("fp_s6_guard"), account("fp_s6_audit"), account("fp_s6_runtime"),
        account("fp_s6_provisioner"), account("fp_s6_importer"), account("fp_s6_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))

  @Test def parsesEveryFinalRenderedCheckAndGeneratedColumnExpression(): Unit =
    val recipe = renderedRecipe
    val checks = recipe.tables.flatMap(_.constraints).filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
    val parsedChecks = checks.map(check => MySqlManagedCheckExpression.parseCheckConstraint(check.tokens))
    val generated = recipe.tables.flatMap(_.columns).flatMap(_.generated)
    val parsedGenerated = generated.map(column => MySqlManagedCheckExpression.parse(column.expression))
    assertTrue(parsedChecks.nonEmpty)
    assertEquals(checks.size, parsedChecks.size)
    assertEquals(generated.size, parsedGenerated.size)
    assertEquals(Vector("source_identity_sha256"), recipe.tables.flatMap(_.columns)
      .filter(_.generated.nonEmpty).map(_.name))

  @Test def serverFormattingMayAddSafeParenthesesBackticksAndUtf8mb4Introducers(): Unit =
    assertTrue(MySqlManagedCheckExpression.equivalentSql(
      "state IN ('A','B') AND OCTET_LENGTH(payload)>0",
      "((`state` IN (_utf8mb4'A', _utf8mb4'B')) AND (octet_length(`payload`) > (0)))"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql(
      "NOT (a=1 OR b=2) AND c BETWEEN -4 AND 9",
      "(((NOT ((`a` = 1) OR (`b` = 2))) AND (`c` BETWEEN (-4) AND (9))))"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql(
      "UNHEX(SHA2(CAST(source_identity AS BINARY),256))",
      "((unhex((sha2((CAST(`source_identity` AS BINARY)), (256))))))"))
    assertTrue(MySqlManagedCheckExpression.equivalent(
      MySqlManagedStaticRecipe.tokenize("state='READY'"), "((`state` = _utf8mb4'READY'))", "utf8mb4"))

  @Test def expressionAstPreservesOperatorPrecedenceOrderAndLiteralIdentity(): Unit =
    assertFalse(MySqlManagedCheckExpression.equivalentSql("a AND (b OR c)", "(a AND b) OR c"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("a=1", "a<>1"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("state='READY'", "state='ready'"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("a-b-c", "a-(b-c)"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("f(a,b)", "f(b,a)"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("OCTET_LENGTH(payload)>0", "payload>0"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("x=1.0e300", "x=1.0E300"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql("`state`='READY'", "state='READY'"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("state='READY'", "`STATE`='READY'"))

  @Test def unsupportedIntroducersCollationsAndMalformedExpressionsFailClosed(): Unit =
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("state=_latin1'READY'"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("state='READY' COLLATE utf8mb4_general_ci"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("CAST(bytes AS DECIMAL) = 1"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSource(MySqlManagedStaticRecipe.tokenize("state='READY'"), "latin1"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("state IN ('READY',)"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("(state='READY'"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("state IS TRUE"))

  @Test def actualRbssCheckClausesMatchCompiledRecipe(): Unit =
    assertActualCheckClauses("/mysql/rbss-check-clauses-20261105.json", 41)

  @Test def actualFirstFiveTableCheckClausesMatchCompiledRecipe(): Unit =
    assertActualCheckClauses("/mysql/rbss-five-table-checks-20261105.json", 115)

  private def assertActualCheckClauses(resource: String, count: Int): Unit =
    val stream = getClass.getResourceAsStream(resource)
    val fixture = try com.google.gson.JsonParser.parseReader(new java.io.InputStreamReader(stream,
      java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject finally stream.close()
    val checks = renderedRecipe.tables.flatMap(_.constraints)
      .filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
      .map(c => c.name.get -> c.tokens.slice(4, c.tokens.size - 1)).toMap
    val rows = fixture.getAsJsonArray("rows")
    assertEquals(count, rows.size())
    rows.forEach { entry =>
      val row = entry.getAsJsonObject
      val name = row.get("constraintName").getAsString
      assertTrue(MySqlManagedCheckExpression.equivalent(checks(name), row.get("checkClause").getAsString), name)
    }

  @Test def catalogEscapedLiteralsStayExactAndOutsideGeneralSqlGrammar(): Unit =
    val expected = MySqlManagedStaticRecipe.tokenize("state='READY'")
    assertTrue(MySqlManagedCheckExpression.equivalent(expected, "`state`=_utf8mb4\\'READY\\'"))
    assertFalse(MySqlManagedCheckExpression.equivalent(expected, "`state`=_utf8mb4\\'ready\\'"))
    for invalid <- Vector("state=\\'READY\\'", "state=_latin1\\'READY\\'",
        "state=_utf8mb4\\'REA\\'DY\\'", "state=_utf8mb4\\'READY") do
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedCheckExpression.equivalent(expected, invalid))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedCheckExpression.parseSql("state=_utf8mb4\\'READY\\'"))

  @Test def mysqlByteLengthAliasPreservesArgumentsAndCharacterLengthDistinction(): Unit =
    assertTrue(MySqlManagedCheckExpression.equivalentSql("OCTET_LENGTH(payload)=32", "length(`payload`)=32"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("OCTET_LENGTH(payload)=32", "CHAR_LENGTH(payload)=32"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("OCTET_LENGTH(payload)=32", "LENGTH(other)=32"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("octet_length=32", "length=32"))

  @Test def generatedBinaryCastCatalogSpellingKeepsCharsetAndLengthBoundaries(): Unit =
    val source = MySqlManagedStaticRecipe.tokenize("UNHEX(SHA2(CAST(source_identity AS BINARY),256))")
    assertTrue(MySqlManagedCheckExpression.equivalent(source,
      "unhex(sha2(cast(`source_identity` as char charset binary),256))"))
    for target <- Vector("CHAR", "CHAR CHARSET utf8mb4", "CHAR(16) CHARSET BINARY", "BINARY(16)", "CHAR BINARY") do
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedCheckExpression.equivalent(source,
        s"UNHEX(SHA2(CAST(source_identity AS $target),256))"))
    assertFalse(MySqlManagedCheckExpression.equivalent(source,
      "unhex(sha2(cast(`other_identity` as char charset binary),256))"))
    assertFalse(MySqlManagedCheckExpression.equivalent(source,
      "unhex(sha2(cast(`source_identity` as char charset binary),512))"))

  @Test def escapedRegexAndVersionLiteralSymbolsRemainExact(): Unit =
    val source = MySqlManagedStaticRecipe.tokenize("REGEXP_LIKE(behavior_id,'^[a-z][a-z0-9_.-]{0,63}$','c')")
    val actual = "regexp_like(`behavior_id`,_utf8mb4\\'^[a-z][a-z0-9_.-]{0,63}$\\',_utf8mb4\\'c\\')"
    assertTrue(MySqlManagedCheckExpression.equivalent(source,actual))
    assertFalse(MySqlManagedCheckExpression.equivalent(source,actual.replace("{0,63}","{0,64}")))
    assertFalse(MySqlManagedCheckExpression.equivalent(source,actual.replace("\\'c\\'","\\'i\\'")))
    assertFalse(MySqlManagedCheckExpression.equivalent(MySqlManagedStaticRecipe.tokenize("state='READY'"),
      "state=_utf8mb4\\'READY$\\'"))
    assertTrue(MySqlManagedCheckExpression.equivalent(MySqlManagedStaticRecipe.tokenize("version='fp-skill-compiler/1'"),
      "version=_utf8mb4\\'fp-skill-compiler/1\\'"))
    for content <- Vector("line\nbreak","line\rbreak","tab\tvalue","bad"+0.toChar+"value","bad"+127.toChar+"value","bad\\escape","raw'quote") do
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedCheckExpression.equivalent(source,
        "regexp_like(behavior_id,_utf8mb4\\'"+content+"\\',_utf8mb4\\'c\\')"))

  /** 合成目录引号表示只验证封闭配方词法覆盖，不是新增服务器观测或Full27验收。 */
  @Test def catalogEscapedLiteralsCoverEveryCompiledFull27CheckWithoutChangingItsAst(): Unit =
    import MySqlManagedStaticRecipe.TokenKind
    val checks = renderedRecipe.tables.flatMap(_.constraints)
      .filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
    assertTrue(checks.size >= 480)
    checks.foreach { check =>
      val expected = check.tokens.slice(4,check.tokens.size-1)
      val actual = expected.map { token => token.kind match
        case TokenKind.StringLiteral =>
          assertTrue(token.value.startsWith("'") && token.value.endsWith("'"))
          "_utf8mb4\\'"+token.value.substring(1,token.value.length-1)+"\\'"
        case TokenKind.QuotedWord => "`"+token.value.replace("`","``")+"`"
        case _ => token.value
      }.mkString(" ")
      assertTrue(MySqlManagedCheckExpression.equivalent(expected,actual),check.name.get)
    }

  @Test def substringAliasPreservesEveryArgumentAndIdentifier(): Unit =
    assertTrue(MySqlManagedCheckExpression.equivalentSql("SUBSTRING(payload,7,16)", "SUBSTR(payload,7,16)"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql("SUBSTRING(payload,7)", "SUBSTR(payload,7)"))
    for altered <- Vector("SUBSTR(other,7,16)", "SUBSTR(payload,8,16)", "SUBSTR(payload,7,15)", "SUBSTR(payload,7)") do
      assertFalse(MySqlManagedCheckExpression.equivalentSql("SUBSTRING(payload,7,16)", altered))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("substring=1", "substr=1"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("SUBSTRING(payload,7,16)", "`substr`(payload,7,16)"))

  @Test def catalogNegationPushdownPreservesComparisonsAndThreeValuedStructure(): Unit =
    assertTrue(MySqlManagedCheckExpression.equivalentSql("NOT (a=3 AND b=1)", "a<>3 OR b<>1"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql("NOT (a=3 OR b<>1)", "a<>3 AND b=1"))
    assertTrue(MySqlManagedCheckExpression.equivalentSql("NOT (a=NULL AND b=1)", "a<>NULL OR b<>1"))
    for altered <- Vector("a<>3 AND b<>1", "a<>4 OR b<>1", "a<>3 OR c<>1", "a<=>3 OR b<>1") do
      assertFalse(MySqlManagedCheckExpression.equivalentSql("NOT (a=3 AND b=1)", altered))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("NOT NOT a", "a"))
    assertFalse(MySqlManagedCheckExpression.equivalentSql("NOT (a AND b)", "a OR b"))
