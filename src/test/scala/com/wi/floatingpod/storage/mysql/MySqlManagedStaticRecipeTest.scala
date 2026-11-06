package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedStaticRecipeTest:
  private def inputs: MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs("fp_managed_recipe_test",
      Accounts(account("fp_s6_guard"), account("fp_s6_audit"), account("fp_s6_runtime"),
        account("fp_s6_provisioner"), account("fp_s6_importer"), account("fp_s6_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)

  @Test def extractsAllFinalSchema6TablesGuardsAndRoutineFromTheCompiledManagedPlan(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val recipe = MySqlManagedStaticRecipe.fromPlan(plan)

    assertEquals(27, recipe.tables.size)
    assertEquals(480, recipe.tables.map(_.columns.size).sum)
    assertEquals(MySqlDbFirstSchemaProfile.TableNames, recipe.tables.map(_.name))
    assertEquals(81, recipe.triggers.size)
    assertEquals(MySqlDbFirstSchemaProfile.GuardNames, recipe.triggers.map(_.name))
    assertEquals(MySqlDbFirstSchemaDefinition.AuditRoutine, recipe.auditRoutine.name)
    assertEquals("DEFINER", recipe.auditRoutine.securityType)
    assertEquals("READS SQL DATA", recipe.auditRoutine.dataAccess)
    assertTrue(recipe.auditRoutine.definerUser.nonEmpty && recipe.auditRoutine.definerHost.nonEmpty)
    assertTrue(recipe.auditRoutine.bodyTokens.headOption.exists(_.value == "begin"))
    assertTrue(recipe.auditRoutine.bodyTokens.lastOption.exists(_.value == "end"))

    val allParts = recipe.tables.flatMap(_.constraints)
    assertTrue(allParts.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.PrimaryKey))
    assertTrue(allParts.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.ForeignKey))
    assertTrue(allParts.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check))
    assertTrue(allParts.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Index))
    assertTrue(allParts.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.ForeignKey).forall(_.name.nonEmpty))
    assertTrue(allParts.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check).forall(_.name.nonEmpty))
    assertTrue(allParts.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Index).forall(_.name.nonEmpty))
    assertTrue(allParts.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.UniqueIndex).forall(_.name.nonEmpty))

    val manifest = recipe.tables.find(_.name == "fp_schema_manifest").get
    assertEquals(Vector("engine", "=", "innodb", "row_format", "=", "dynamic", "default", "charset", "=",
      "utf8mb4", "collate", "=", "utf8mb4_0900_bin"), manifest.options.map(_.value))
    assertTrue(manifest.constraints.exists(c => c.kind == MySqlManagedStaticRecipe.ConstraintKind.Check &&
      c.tokens.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.StringLiteral &&
        t.value.contains("MANAGED_INSTALL"))))
    assertTrue(manifest.columns.exists(c => c.name == "ddl_sha256" && !c.nullable &&
      c.typeSource.map(_.value).mkString.contains("varbinary")))
    val charsetColumn = recipe.tables.find(_.name == MySqlDbFirstSchemaDefinition.SnapshotTable).get
      .columns.find(_.name == "limit_profile_encoding").get
    assertEquals(Vector("varchar", "(", "48", ")", "character", "set", "utf8mb4", "collate", "utf8mb4_0900_bin"),
      charsetColumn.typeSource.map(_.value))
    assertTrue(recipe.tables.forall(_.environment.requiredDatabaseCharacterSet == "utf8mb4"))
    assertTrue(recipe.tables.forall(_.environment.requiredDatabaseCollation == "utf8mb4_0900_bin"))
    val generatedColumns = recipe.tables.flatMap(_.columns).filter(_.generated.nonEmpty)
    assertEquals(Vector("source_identity_sha256"), generatedColumns.map(_.name))
    val generated = generatedColumns.head
    val generation = generated.generated.get
    assertTrue(generated.nullable)
    assertEquals("STORED", generation.storage)
    assertEquals("unhex", generation.expression.head.value)
    assertTrue(generation.expression.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.Word && t.value == "cast"))
    assertTrue(generation.clauseTokens.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.Word && t.value == "generated"))
    assertEquals(generation.clauseTokens.count(_.value == "("), generation.clauseTokens.count(_.value == ")"))
    assertTrue(recipe.triggers.forall(_.bodyTokens.headOption.exists(_.value == "begin")))
    assertTrue(recipe.triggers.forall(_.bodyTokens.lastOption.exists(_.value == "end")))
    assertTrue(recipe.triggers.forall(t => t.environment.sqlMode.nonEmpty &&
      t.environment.characterSetClient == "utf8mb4" && t.environment.collationConnection == "utf8mb4_0900_bin"))
    assertTrue(recipe.triggers.forall(t => t.definerUser.nonEmpty && t.definerHost.nonEmpty))
    assertTrue(recipe.sessionEnvironments.nonEmpty)
    assertTrue(recipe.tables.forall(_.columns.forall(c => c.typeSource.nonEmpty && c.declaration.nonEmpty)))

  @Test def compilesDeferredForeignKeysOnlyAsExactPlanOrderPrefixes(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val deferred = plan.deferredConstraintSteps
    val names = Vector(
      ("physical_source_unit", "fk_unit_current_forest"),
      ("physical_recovery_intent", "fk_intent_completion"),
      ("physical_recovery_intent", "fk_intent_source_forest"))
    assertEquals(3, deferred.size)
    assertEquals(Vector(29, 30, 31), deferred.map(_.ordinal))
    assertEquals(names.map { (table, name) => s"constraint:$table:$name" }, deferred.map(_.identity))

    val full = MySqlManagedStaticRecipe.fromPlan(plan)
    val allExplicit = MySqlManagedStaticRecipe.fromPlanAt(plan, deferred.map(_.ordinal).toSet)
    assertEquals(full.appliedConstraintOrdinals, deferred.map(_.ordinal).toSet)
    assertEquals(full.tables, allExplicit.tables)
    assertEquals(full.tables.map(_.statementSha256), allExplicit.tables.map(_.statementSha256))

    val prefixes = (0 to deferred.size).map { size =>
      val selected = deferred.take(size).map(_.ordinal).toSet
      val recipe = MySqlManagedStaticRecipe.fromPlanAt(plan, selected)
      val installedDeferredNames = recipe.tables.flatMap(table => table.constraints.collect {
        case constraint if names.contains(table.name -> constraint.name.getOrElse("")) => table.name -> constraint.name.get
      }).sortBy(names.indexOf)
      assertEquals(names.take(size), installedDeferredNames)
      assertEquals(selected, recipe.appliedConstraintOrdinals)
      assertEquals(full.tables.map(_.statementSha256), recipe.tables.map(_.statementSha256))
      assertEquals(full.tables.map(_.statementTokens), recipe.tables.map(_.statementTokens))
      recipe
    }
    assertEquals(names, prefixes.last.tables.flatMap(table => table.constraints.collect {
      case constraint if names.contains(table.name -> constraint.name.getOrElse("")) => table.name -> constraint.name.get
    }).sortBy(names.indexOf))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedStaticRecipe.fromPlanAt(plan, Set(deferred(1).ordinal)))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlManagedStaticRecipe.fromPlanAt(plan, Set(deferred(0).ordinal, deferred(2).ordinal)))

  @Test def lexerFoldsOnlyAsciiWordsAndWhitespaceAndPreservesQuotedLiteralsAndStructure(): Unit =
    val left = "SELECT `MiXeD` FROM t WHERE x=(1 + 2) AND label='Keep Case'"
    val sameTokens = " select `MiXeD`\n from t where x = ( 1 + 2 ) and label = 'Keep Case' "
    assertTrue(MySqlManagedStaticRecipe.lexicallyEquivalent(left, sameTokens))
    assertFalse(MySqlManagedStaticRecipe.lexicallyEquivalent(left, sameTokens.replace("`MiXeD`", "`mixed`")))
    assertFalse(MySqlManagedStaticRecipe.lexicallyEquivalent(left, sameTokens.replace("Keep Case", "keep case")))
    assertFalse(MySqlManagedStaticRecipe.lexicallyEquivalent(left, sameTokens.replace("1 + 2", "1 - 2")))
    assertFalse(MySqlManagedStaticRecipe.lexicallyEquivalent(left, sameTokens.replace("( 1 + 2 )", "1 + 2")))

    val escaped = MySqlManagedStaticRecipe.tokenize("SELECT `a``b`, _utf8mb4'X\\'Y'")
    assertTrue(escaped.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.QuotedWord && t.value == "a`b"))
    assertTrue(escaped.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.StringLiteral && t.value == "'X\\'Y'"))
    assertTrue(escaped.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.Word && t.value == "_utf8mb4"))

    val nested = MySqlManagedStaticRecipe.tokenize(
      "CREATE TABLE `a` (`c` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL, " +
        "CONSTRAINT `ck` CHECK ((JSON_EXTRACT(`c`,'$.k')='a,b') OR LENGTH(`c`)>0)) ENGINE=InnoDB")
    assertEquals(1, nested.count(t => t.kind == MySqlManagedStaticRecipe.TokenKind.StringLiteral && t.value == "'a,b'"))
    assertTrue(nested.contains(MySqlManagedStaticRecipe.Token(MySqlManagedStaticRecipe.TokenKind.Symbol, ",")))
    assertTrue(nested.count(t => t == MySqlManagedStaticRecipe.Token(MySqlManagedStaticRecipe.TokenKind.Symbol, "(")) > 3)
    assertTrue(nested.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.QuotedWord && t.value == "ck"))
    val withComment = MySqlManagedStaticRecipe.tokenize("SELECT 1 /*x,()*/ + 2")
    assertTrue(withComment.exists(t => t.kind == MySqlManagedStaticRecipe.TokenKind.Comment && t.value == "/*x,()*/"))
    assertFalse(MySqlManagedStaticRecipe.lexicallyEquivalent("SELECT 1 /*x*/ + 2", "SELECT 1 /*X*/ + 2"))

  @Test def malformedStringCommentsAndGroupingFailClosed(): Unit =
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("SELECT 'unterminated"))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("SELECT /* unterminated"))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("CREATE TABLE t (a INT"))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("SELECT 1)"))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("SELECT 1.2.3"))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlManagedStaticRecipe.tokenize("SELECT 'bad\\"))

