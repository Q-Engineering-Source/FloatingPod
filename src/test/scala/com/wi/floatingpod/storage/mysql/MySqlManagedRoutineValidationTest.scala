package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{SQLDataException, Types}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 合成 catalog DATA 验证检查器；这些列描述和行不进入生产 baseline 或 JDBC 校准。 */
class MySqlManagedRoutineValidationTest:
  private val profile = DeploymentReceiptStaticTestData.defaultProfile()
  private def recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(
    MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)))
  private def root(): MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile.sourceLimitProfile,
      profile.sourceLimitProfile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))

  private val labels = Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME",
    "CHECK_CLAUSE", "clause_octets", "clause_sha256")
  private def observed(name: String, clause: String): MySqlAuditRow =
    val bytes = clause.getBytes(StandardCharsets.UTF_8)
    MySqlAuditRow(Vector(MySqlAuditText("def"), MySqlAuditText(profile.schema), MySqlAuditText(name),
      MySqlAuditText(clause), MySqlAuditText(bytes.length.toString),
      MySqlAuditBinary(MessageDigest.getInstance("SHA-256").digest(bytes))))
  private def set(rows: Vector[MySqlAuditRow]): MySqlAuditResultSet =
    MySqlAuditResultSet("RS08_CHECK_CONSTRAINTS", labels.map(label =>
      MySqlAuditColumn(label, Types.VARCHAR, "SYNTHETIC_TEST_ONLY", 0, 0, 1, false)), rows)

  @Test def changedCheckWithAConsistentSelfHashStillFailsSourceRecipeComparison(): Unit =
    val compiled = recipe
    val table = compiled.tables.find(_.constraints.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)).get
    val checks = table.constraints.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
      .sortBy(_.name.get)
    // 原文 token 只用于构造正常 fixture；故障行自洽的 octets/hash 不能证明表达式正确。
    val rows = checks.map { check =>
      val expression = check.tokens.slice(4, check.tokens.size - 1).map(_.value).mkString(" ")
      observed(check.name.get, expression)
    }
    val budget = root()
    try
      MySqlManagedRoutineValidation.validateTableChecks(compiled, table.name, set(rows), budget)
      val changed = rows.updated(0, observed(checks.head.name.get, "1 = 1"))
      assertThrows(classOf[SQLDataException], () =>
        MySqlManagedRoutineValidation.validateTableChecks(compiled, table.name, set(changed), budget))
      assertThrows(classOf[SQLDataException], () =>
        MySqlManagedRoutineValidation.validateTableChecks(compiled, table.name, set(rows.tail), budget))
    finally budget.closeRootReadBudget()

  @Test def matchingExpressionWithAnIncorrectDigestFails(): Unit =
    val compiled = recipe
    val table = compiled.tables.find(_.constraints.exists(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)).get
    val rows = table.constraints.filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
      .sortBy(_.name.get).map { check =>
        observed(check.name.get, check.tokens.slice(4, check.tokens.size - 1).map(_.value).mkString(" "))
      }
    val corrupted = rows.updated(0, rows.head.copy(cells = rows.head.cells.updated(5, MySqlAuditBinary(new Array[Byte](32)))))
    val budget = root()
    try
      assertThrows(classOf[SQLDataException], () =>
        MySqlManagedRoutineValidation.validateTableChecks(compiled, table.name, set(corrupted), budget))
    finally budget.closeRootReadBudget()

  /** 所有81条为固定SOURCE构造的合成目录，验证后续触发器同一共享门；不是服务器观测。 */
  @Test def allCompiledTriggerSelfDescriptionsUseTheActualQueryAliases(): Unit =
    val compiled = recipe
    val budget = root()
    val labels = Vector("TRIGGER_SCHEMA","TRIGGER_NAME","EVENT_OBJECT_SCHEMA","EVENT_OBJECT_TABLE",
      "EVENT_MANIPULATION","ACTION_TIMING","ACTION_ORDER","ACTION_ORIENTATION","ACTION_STATEMENT",
      "DEFINER","SQL_MODE","CHARACTER_SET_CLIENT","COLLATION_CONNECTION","DATABASE_COLLATION",
      "statement_octets","statement_sha256")
    def tokenSql(token: MySqlManagedStaticRecipe.Token): String =
      if token.kind == MySqlManagedStaticRecipe.TokenKind.QuotedWord then "`"+token.value.replace("`","``")+"`"
      else token.value
    try
      assertEquals(81,compiled.triggers.size)
      compiled.triggers.foreach { trigger =>
        val body = trigger.bodyTokens.map(tokenSql).mkString(" ")
        val bytes = body.getBytes(StandardCharsets.UTF_8)
        val text = Vector(compiled.schema,trigger.name,compiled.schema,trigger.table,trigger.event,"BEFORE","1","ROW",body,
          trigger.definerUser+"@"+trigger.definerHost,trigger.environment.sqlMode,trigger.environment.characterSetClient,
          trigger.environment.collationConnection,trigger.environment.requiredDatabaseCollation,bytes.length.toString)
        val row = MySqlAuditRow(text.map(MySqlAuditText.apply) :+ MySqlAuditBinary(MessageDigest.getInstance("SHA-256").digest(bytes)))
        val set = MySqlAuditResultSet("RS09_TRIGGERS",labels.map(label =>
          MySqlAuditColumn(label,Types.VARCHAR,"SYNTHETIC_TEST_ONLY",0,0,1,false)),Vector(row))
        MySqlManagedRoutineValidation.validateTrigger(compiled.schema,trigger,set,row,budget)
        for index <- Vector(14,15) do
          val bad = if index == 14 then MySqlAuditText("0") else MySqlAuditBinary(new Array[Byte](32))
          assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineValidation.validateTrigger(
            compiled.schema,trigger,set,row.copy(cells=row.cells.updated(index,bad)),budget))
      }
    finally budget.closeRootReadBudget()
