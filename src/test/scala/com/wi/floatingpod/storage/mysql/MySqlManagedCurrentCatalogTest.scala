package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonObject, JsonParser}
import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.jdk.CollectionConverters.*

/** 回放11张实际已建表；未观察的routine/trigger集合仅补充既定metadata空框架，不作观测或批准。 */
class MySqlManagedCurrentCatalogTest:
  private def read(resource: String): JsonObject =
    val stream = getClass.getResourceAsStream(resource)
    assertNotNull(stream)
    try JsonParser.parseString(new String(stream.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject
    finally stream.close()

  private def columns(set: JsonObject): Vector[MySqlAuditColumn] =
    set.getAsJsonArray("columns").asScala.toVector.map { raw =>
      val c = raw.getAsJsonObject
      MySqlAuditColumn(c.get("label").getAsString, c.get("jdbcType").getAsInt,
        c.get("typeName").getAsString, c.get("precision").getAsInt, c.get("scale").getAsInt,
        c.get("nullable").getAsInt, c.get("signed").getAsBoolean)
    }

  private def actualSets(resource: String = "/mysql/rbss-eleven-tables-20261105.json"): Vector[MySqlAuditResultSet] =
    val observed = read(resource).getAsJsonArray("sets").asScala.map { raw =>
      val set = raw.getAsJsonObject
      val rows = set.getAsJsonArray("rows").asScala.toVector.map { rawRow =>
        MySqlAuditRow(rawRow.getAsJsonArray.asScala.toVector.map { rawCell =>
          val c = rawCell.getAsJsonObject
          c.get("kind").getAsString match
            case "null" => MySqlAuditNull
            case "text" | "integer" | "decimal" => MySqlAuditText(c.get("value").getAsString)
            case "binaryHex" => MySqlAuditBinary(HexFormat.of().parseHex(c.get("value").getAsString))
            case other => throw new IllegalArgumentException("unhandled actual catalog cell kind: " + other)
        })
      }
      set.get("id").getAsString -> MySqlAuditResultSet(set.get("id").getAsString, columns(set), rows)
    }.toMap
    assertEquals(if resource.contains("routine-catalog") then 10 else 7, observed.size)
    read("/db/mysql/managed-direct-static-jdbc-profile-v1.json").getAsJsonArray("sets").asScala.toVector.map { raw =>
      val set = raw.getAsJsonObject
      val id = set.get("id").getAsString
      observed.getOrElse(id, MySqlAuditResultSet(id, columns(set), Vector.empty))
    }

  private def withRoot(body: (MySqlDbFirstSchemaProfile.Rendered, MySqlFormat3ReadBudget) => Unit): Unit =
    val profile = MySqlManagedReleasePolicy.profile("floatingpodrbssv1",
      UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "127.0.0.1")
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
    val root = MySqlFormat3ReadBudget.rootOnLedger(profile.sourceLimitProfile,
      profile.sourceLimitProfile.maxSqlPacketBytes(), new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))
    try body(rendered, root) finally root.closeRootReadBudget()

  @Test def allElevenActualTablesPassTheProductionPerTableGate(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets()
    val tables = sets.head.rows.map(_.cells(1)).map { case MySqlAuditText(value) => value; case _ => fail("table name is not text"); "" }
    assertEquals(11, tables.size)
    assertEquals(217, sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get.rows.size)
    tables.foreach(table => assertTrue(MySqlManagedStaticValidation.verifyTableAt(rendered, table, sets, root, Set.empty), table))
  }

  @Test def generatedExpressionWithDifferentCharsetStillFailsTheProductionGate(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets()
    val index = sets.indexWhere(_.id == "RS03_COLUMNS")
    val columns = sets(index)
    val altered = columns.rows.map { row =>
      if row.cells(1) == MySqlAuditText("physical_recovery_intent") && row.cells(2) == MySqlAuditText("source_identity_sha256") then
        row.copy(cells = row.cells.updated(18, MySqlAuditText("unhex(sha2(cast(`source_identity` as char charset utf8mb4),256))")))
      else row
    }
    assertThrows(classOf[SQLDataException], () => MySqlManagedStaticValidation.verifyTableAt(rendered,
      "physical_recovery_intent", sets.updated(index, columns.copy(rows = altered)), root, Set.empty))
  }

  @Test def actualNineteenTableStreamCaptureAndProductionValidationShareTheOriginalBudget(): Unit = withRoot { (rendered, root) =>
    val resource = "/mysql/rbss-nineteen-tables-20261105.json"
    val allocation = new MySqlSchemaAuditor.AuditAllocation(root):
      override protected def requireOwner(): Unit = assertFalse(root.isClosed)
      override protected def requireObservationOwner(value: UnverifiedMySqlSchemaObservation): Unit =
        throw new UnsupportedOperationException("catalog text replay does not issue observations")
    // 仅按实际目录的text类型调用真实reader；数值不能伪装文字stream，fixture解析本身不作capture计费证据。
    var cells = 0
    read(resource).getAsJsonArray("sets").asScala.foreach { rawSet =>
      rawSet.getAsJsonObject.getAsJsonArray("rows").asScala.foreach { rawRow =>
        rawRow.getAsJsonArray.asScala.foreach { rawCell =>
          val cell = rawCell.getAsJsonObject
          if cell.get("kind").getAsString == "text" then
            val text = cell.get("value").getAsString
            assertEquals(text, MySqlSchemaAuditor.readBoundedTextOwned(new java.io.StringReader(text), allocation, "actual19 catalog text"))
            cells += 1
          else if cell.get("kind").getAsString == "binaryHex" then
            val bytes = HexFormat.of().parseHex(cell.get("value").getAsString)
            assertArrayEquals(bytes, MySqlSchemaAuditor.readBoundedBytesOwned(new java.io.ByteArrayInputStream(bytes), allocation,
              "actual19 catalog binary"))
        }
      }
    }
    assertTrue(cells > 10000)
    val sets = actualSets(resource)
    assertEquals(19, sets.head.rows.size)
    assertEquals(430, sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get.rows.size)
    sets.head.rows.foreach { row =>
      val table = row.cells(1) match
        case MySqlAuditText(value) => value
        case _ => fail("table name is not text"); ""
      assertTrue(MySqlManagedStaticValidation.verifyTableAt(rendered, table, sets, root, Set.empty), table)
    }
  }

  @Test def actualTwentyTwoTableCatalogAndRegexChecksPassTheProductionGate(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-twentytwo-tables-20261105.json")
    // 该实际失败点是fp_script_operation建表后；只按固定SOURCE计划中此前已执行的延期约束选择阶段。
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val tableOrdinal = plan.steps.find(_.identity == "table:fp_script_operation").get.ordinal
    val applied = plan.deferredConstraintSteps.filter(_.ordinal < tableOrdinal).map(_.ordinal).toSet
    assertEquals(Set(29,30,31),applied)
    assertEquals(22, sets.head.rows.size)
    assertEquals(480, sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get.rows.size)
    sets.head.rows.foreach { row =>
      val table = row.cells(1) match
        case MySqlAuditText(value) => value
        case _ => fail("table name is not text"); ""
      assertTrue(MySqlManagedStaticValidation.verifyTableAt(rendered, table, sets, root, applied), table)
    }
  }

  @Test def actualTwentyFiveTableCatalogPassesTheProductionGate(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-twentyfive-tables-20261105.json")
    // 该实际失败点是fp_script_operation建表后；只按固定SOURCE计划中此前已执行的延期约束选择阶段。
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val tableOrdinal = plan.steps.find(_.identity == "table:fp_script_operation").get.ordinal
    val applied = plan.deferredConstraintSteps.filter(_.ordinal < tableOrdinal).map(_.ordinal).toSet
    assertEquals(Set(29,30,31),applied)
    assertEquals(25, sets.head.rows.size)
    assertEquals(559, sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get.rows.size)
    sets.head.rows.foreach { row =>
      val table = row.cells(1) match
        case MySqlAuditText(value) => value
        case _ => fail("table name is not text"); ""
      assertTrue(MySqlManagedStaticValidation.verifyTableAt(rendered, table, sets, root, applied), table)
    }
  }

  @Test def everyActualTwentyFiveTableCheckMatchesWithAllDifferencesReported(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-twentyfive-tables-20261105.json")
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    val checks = recipe.tables.flatMap(_.constraints).filter(_.kind == MySqlManagedStaticRecipe.ConstraintKind.Check)
      .map(c => c.name.get -> c.tokens.slice(4,c.tokens.size-1)).toMap
    val set = sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get
    val failures = set.rows.flatMap { row =>
      val data = set.columns.map(_.label).zip(row.cells).toMap
      def text(name: String): String = data(name) match
        case MySqlAuditText(value) => value
        case _ => fail("not text: "+name); ""
      val name = text("CONSTRAINT_NAME")
      val actual = text("CHECK_CLAUSE")
      val expected = checks(name)
      try
        if MySqlManagedCheckExpression.equivalent(expected,actual) then None
        else Some(name+" SOURCE="+expected.map(_.value).mkString(" ")+" ACTUAL="+actual)
      catch case e: java.sql.SQLDataException => Some(name+" ERROR="+e.getMessage+" ACTUAL="+actual)
    }
    assertEquals(Vector.empty[String], failures, failures.mkString("\n"))
  }

  @Test def actualRoutineBodyMatchesTheProductionRecipe(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-routine-catalog-20261105.json")
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    val routine = sets.find(_.id == "RS10_AUDITSELF").get
    assertEquals(1,routine.rows.size)
    val body = routine.rows.head.cells(routine.columns.indexWhere(_.label == "ROUTINE_DEFINITION")) match
      case MySqlAuditText(value) => value
      case _ => fail("routine definition not text"); ""
    assertThrows(classOf[SQLDataException], () => MySqlManagedStaticRecipe.tokenize(body))
    MySqlManagedRoutineSource.validate(recipe,body,actualRoutineSource(),root)
  }

  @Test def allActualTwentySevenTablesPassProductionGate(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-routine-catalog-20261105.json")
    assertEquals(27, sets.head.rows.size)
    assertEquals(577, sets.find(_.id == "RS08_CHECK_CONSTRAINTS").get.rows.size)
    sets.head.rows.foreach { row =>
      val table = row.cells(1) match
        case MySqlAuditText(value) => value
        case _ => fail("table not text"); ""
      assertTrue(MySqlManagedStaticValidation.verifyTable(rendered,table,sets,root),table)
    }
  }

  private def actualRoutineSource(): MySqlAuditResultSet =
    val set = read("/mysql/rbss-routine-show-create-20261105.json")
    val rows = set.getAsJsonArray("rows").asScala.toVector.map { raw =>
      MySqlAuditRow(raw.getAsJsonArray.asScala.toVector.map { cell =>
        val c = cell.getAsJsonObject
        if c.get("kind").getAsString == "text" then MySqlAuditText(c.get("value").getAsString)
        else if c.get("kind").getAsString == "null" then MySqlAuditNull
        else throw new IllegalArgumentException("unexpected SHOW CREATE fixture cell")
      })
    }
    MySqlAuditResultSet(MySqlManagedRoutineSource.ResultId,columns(set),rows)

  @Test def showCreateChangesCannotPassEvenWithMatchingLossyCatalog(): Unit = withRoot { (rendered, root) =>
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    val sets = actualSets("/mysql/rbss-routine-catalog-20261105.json")
    val routine = sets.find(_.id == "RS10_AUDITSELF").get
    val body = routine.rows.head.cells(routine.columns.indexWhere(_.label == "ROUTINE_DEFINITION")).asInstanceOf[MySqlAuditText].value
    val show = actualRoutineSource()
    def changed(index: Int,value: MySqlAuditCell) = show.copy(rows=Vector(show.rows.head.copy(cells=show.rows.head.cells.updated(index,value))))
    val sql = show.rows.head.cells(2).asInstanceOf[MySqlAuditText].value
    for altered <- Vector(sql.replace(recipe.auditRoutine.definerUser,"wrong_definer"),
        sql.replace(recipe.auditRoutine.name,"wrong_routine"), sql.replace("fp_bad<>5","fp_bad=5"),
        sql.replace("remain locked","remain unlocked")) do
      assertNotEquals(sql,altered)
      assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body,changed(2,MySqlAuditText(altered)),root))
    for index <- Vector(0,1,3,4,5) do
      assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body,changed(index,MySqlAuditText("WRONG")),root))
    assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body,changed(2,MySqlAuditNull),root))
    assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body,show.copy(rows=Vector.empty),root))
    assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body,show.copy(rows=show.rows++show.rows),root))
    assertThrows(classOf[SQLDataException], () => MySqlManagedRoutineSource.validate(recipe,body+" ",show,root))
    // 有损目录表示存在碰撞，故其一致绝不替代上面的原始SQL验证。
    assertEquals(MySqlManagedRoutineSource.catalogRepresentation("RETURN 'a'' OR ''b';"),
      MySqlManagedRoutineSource.catalogRepresentation("RETURN 'a' OR 'b';"))
  }

  @Test def actualRoutinePassesCompleteSharedInstallationAndFinalValidation(): Unit = withRoot { (rendered, root) =>
    val sets = actualSets("/mysql/rbss-routine-catalog-20261105.json")
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    val routine = sets.find(_.id == "RS10_AUDITSELF").get
    val parameters = sets.find(_.id == "RS11_AUDITSELF_PARAMETERS").get
    MySqlManagedRoutineValidation.validateRoutine(recipe,routine,parameters,root,actualRoutineSource())
    for label <- Vector("definition_octets","definition_sha256") do
      val column = routine.columns.indexWhere(_.label == label)
      assertTrue(column >= 0)
      val bad = if label == "definition_octets" then MySqlAuditText("0") else MySqlAuditBinary(new Array[Byte](32))
      val changed = routine.copy(rows=Vector(routine.rows.head.copy(cells=routine.rows.head.cells.updated(column,bad))))
      assertThrows(classOf[SQLDataException], () =>
        MySqlManagedRoutineValidation.validateRoutine(recipe,changed,parameters,root,actualRoutineSource()))
  }
