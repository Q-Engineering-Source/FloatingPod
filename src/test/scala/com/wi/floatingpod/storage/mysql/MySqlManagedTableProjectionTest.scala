package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.sql.SQLDataException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedTableProjectionTest:
  import MySqlManagedTableProjection.ExpectedField

  private def inputs(schema: String = "fp_managed_projection_test"): MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs(schema,
      Accounts(account("fp_s6_guard"), account("fp_s6_audit"), account("fp_s6_runtime"),
        account("fp_s6_provisioner"), account("fp_s6_importer"), account("fp_s6_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)

  private def compiled(schema: String = "fp_managed_projection_test"):
      (MySqlDbFirstSchemaProfile.Rendered, MySqlManagedStaticRecipe.Recipe) =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs(schema))
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    (rendered, recipe)

  private def text(row: Vector[ExpectedField], position: Int): String = row(position) match
    case ExpectedField.Exact(MySqlAuditText(value)) => value
    case value => fail(s"expected exact text at field $position, got $value"); ""

  private def isNull(row: Vector[ExpectedField], position: Int): Boolean = row(position) match
    case ExpectedField.Exact(MySqlAuditNull) => true
    case _ => false

  @Test def derivesAllSixStaticCatalogSetsFromTheFinalFull27ReleaseRecipe(): Unit =
    val (rendered, recipe) = compiled()
    val projection = MySqlManagedTableProjection.project(recipe, rendered)
    val tables = projection.set("RS02_TABLES").rows
    val columns = projection.set("RS03_COLUMNS").rows
    val statistics = projection.set("RS04_STATISTICS").rows
    val constraints = projection.set("RS05_TABLE_CONSTRAINTS").rows
    val keyUsage = projection.set("RS06_KEY_COLUMN_USAGE").rows
    val references = projection.set("RS07_REFERENTIAL_CONSTRAINTS").rows

    assertEquals(27, tables.size)
    assertEquals(480, columns.size)
    assertTrue(statistics.nonEmpty)
    assertTrue(constraints.nonEmpty)
    assertTrue(keyUsage.nonEmpty)
    assertTrue(references.nonEmpty)
    assertTrue(tables.forall(row => text(row, 2) == "BASE TABLE" && text(row, 3) == "InnoDB" &&
      text(row, 4) == "10" && text(row, 5) == "Dynamic" && text(row, 6) == "utf8mb4_0900_bin" &&
      text(row, 7) == "row_format=DYNAMIC" && text(row, 8).isEmpty))
    assertEquals(tables.map(row => text(row, 1)), tables.map(row => text(row, 1)).sorted)
    assertTrue(columns.forall(row => isNull(row, 4)))
    assertTrue(columns.forall(row => text(row, 17).isEmpty))

    val snapshot = MySqlDbFirstSchemaDefinition.SnapshotTable
    def stat(indexName: String): Vector[ExpectedField] = statistics.find(row =>
      text(row, 1) == snapshot && text(row, 4) == indexName).getOrElse(fail(s"missing snapshot index $indexName"))
    assertEquals("PRIMARY", text(stat("PRIMARY"), 4))
    assertEquals("binding_uuid", text(stat("PRIMARY"), 6))
    val secondPrimaryPart = statistics.find(row => text(row, 1) == snapshot && text(row, 4) == "PRIMARY" &&
      text(row, 5) == "2").getOrElse(fail("missing second snapshot primary-key part"))
    assertEquals("pod_uuid", text(secondPrimaryPart, 6))
    assertTrue(statistics.exists(row => text(row, 1) == snapshot && text(row, 4) == "fk_snapshot_pod"))
    assertTrue(statistics.exists(row => text(row, 1) == snapshot && text(row, 4) == "fk_snapshot_mutation"))
    assertFalse(statistics.exists(row => text(row, 1) == snapshot && text(row, 4) == "fk_snapshot_binding"),
      "the binding FK is already covered by the primary key's leftmost column")

    val snapshotReferences = references.filter(row => text(row, 9) == snapshot)
    assertEquals(3, snapshotReferences.size)
    assertTrue(snapshotReferences.forall(row => text(row, 6) == "NONE" && text(row, 7) == "NO ACTION" &&
      text(row, 8) == "NO ACTION"))
    assertTrue(constraints.exists(row => text(row, 4) == snapshot && text(row, 5) == "PRIMARY KEY"))
    assertTrue(constraints.exists(row => text(row, 4) == snapshot && text(row, 5) == "FOREIGN KEY"))

    val generated = columns.find(row => text(row, 1) == "physical_recovery_intent" && text(row, 2) == "source_identity_sha256")
      .getOrElse(fail("missing generated source identity column"))
    assertEquals("YES", text(generated, 5))
    assertEquals("STORED GENERATED", text(generated, 16))
    generated(18) match
      case ExpectedField.GeneratedExpression(tokens) =>
        assertEquals(recipe.tables.find(_.name == "physical_recovery_intent").get.columns
          .find(_.name == "source_identity_sha256").get.generated.get.expression, tokens)
      case value => fail(s"generated expression must be preserved as a model, got $value")

    val numeric = columns.find(row => text(row, 1) == "floating_pod_state" && text(row, 2) == "pos_x")
      .getOrElse(fail("missing release DOUBLE column"))
    assertEquals("double", text(numeric, 6))
    assertEquals("22", text(numeric, 9))
    assertTrue(isNull(numeric, 10))
    val temporal = columns.find(row => text(row, 2) == "lease_until")
      .getOrElse(fail("missing release DATETIME(6) column"))
    assertEquals("6", text(temporal, 11))

    val blob = columns.find(row => text(row, 1) == snapshot && text(row, 2) == "nbt_bytes")
      .getOrElse(fail("missing snapshot LONGBLOB column"))
    assertEquals("4294967295", text(blob, 7))
    assertEquals("4294967295", text(blob, 8))
    assertTrue(isNull(blob, 12))
    assertTrue(isNull(blob, 13))
    val longText = columns.find(row => text(row, 1) == "floating_pod_state" && text(row, 2) == "world_name")
      .getOrElse(fail("missing release LONGTEXT column"))
    assertEquals("4294967295", text(longText, 7))
    assertEquals("4294967295", text(longText, 8))
    assertEquals("utf8mb4", text(longText, 12))
    assertEquals("utf8mb4_0900_bin", text(longText, 13))
    val binary = columns.find(row => text(row, 1) == snapshot && text(row, 2) == "binding_uuid")
      .getOrElse(fail("missing release VARBINARY column"))
    assertEquals("varbinary", text(binary, 6))
    assertEquals("16", text(binary, 7))
    assertEquals("16", text(binary, 8))
    assertEquals("varbinary(16)", text(binary, 14))
    assertTrue(isNull(binary, 12))
    assertTrue(isNull(binary, 13))
    val varchar = columns.find(row => text(row, 1) == "fp_schema_manifest" && text(row, 2) == "source_provenance_kind")
      .getOrElse(fail("missing release VARCHAR column"))
    assertEquals("varchar", text(varchar, 6))
    assertEquals("32", text(varchar, 7))
    assertEquals("128", text(varchar, 8))
    assertEquals("utf8mb4", text(varchar, 12))
    assertEquals("utf8mb4_0900_bin", text(varchar, 13))

    val podId = columns.find(row => text(row, 1) == snapshot && text(row, 2) == "pod_uuid").get
    assertEquals("PRI", text(podId, 15))
    val fkParentKeys = references.map(row => text(row, 5))
    assertTrue(fkParentKeys.forall(_.nonEmpty))

  @Test def rejectsRecipeOrRenderedSqlFromAnotherIdentity(): Unit =
    val (rendered, recipe) = compiled()
    val (_, otherRecipe) = compiled("fp_managed_projection_other")
    assertThrows(classOf[SQLDataException], () => MySqlManagedTableProjection.project(otherRecipe, rendered))

    val tampered = new MySqlDbFirstSchemaProfile.Rendered(rendered.inputs, rendered.sql + " ", rendered.sha256,
      rendered.tables, rendered.guards, rendered.mode)
    assertThrows(classOf[SQLDataException], () => MySqlManagedTableProjection.project(recipe, tampered))

  @Test def projectsOnlyEachAuthorizedDeferredForeignKeyPrefixAndChangesCatalogSetsPrecisely(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs())
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val steps = plan.deferredConstraintSteps
    val ordinals = steps.map(_.ordinal)
    val expected = Vector(
      ("physical_source_unit", "fk_unit_current_forest", 4),
      ("physical_recovery_intent", "fk_intent_completion", 2),
      ("physical_recovery_intent", "fk_intent_source_forest", 3))
    assertEquals(3, steps.size)
    assertEquals(Vector(29, 30, 31), ordinals)
    assertEquals(expected.map(entry => s"constraint:${entry._1}:${entry._2}"), steps.map(_.identity))

    val prefixes = (0 to steps.size).map { count =>
      val applied = ordinals.take(count).toSet
      val recipe = MySqlManagedStaticRecipe.fromPlanAt(plan, applied)
      val projection = MySqlManagedTableProjection.projectAt(recipe, rendered, applied)
      (recipe, projection)
    }

    def rows(prefix: Int, id: String): Vector[Vector[ExpectedField]] = prefixes(prefix)._2.set(id).rows
    def foreignConstraints(prefix: Int) = rows(prefix, "RS05_TABLE_CONSTRAINTS")
      .count(row => text(row, 5) == "FOREIGN KEY")
    def keyUsage(prefix: Int) = rows(prefix, "RS06_KEY_COLUMN_USAGE")
    def referential(prefix: Int) = rows(prefix, "RS07_REFERENTIAL_CONSTRAINTS")
    def statistics(prefix: Int) = rows(prefix, "RS04_STATISTICS")

    (0 until steps.size).foreach { index =>
      assertEquals(foreignConstraints(index) + 1, foreignConstraints(index + 1))
      assertEquals(keyUsage(index).size + expected(index)._3, keyUsage(index + 1).size)
      assertEquals(referential(index).size + 1, referential(index + 1).size)
      assertEquals(rows(0, "RS02_TABLES"), rows(index + 1, "RS02_TABLES"))
      assertEquals(rows(0, "RS03_COLUMNS"), rows(index + 1, "RS03_COLUMNS"))
    }

    assertEquals(statistics(0).size + 4, statistics(1).size)
    assertEquals(statistics(1).size, statistics(2).size,
      "fk_intent_completion is already supported by ux_intent_completion")
    assertEquals(statistics(2).size + 3, statistics(3).size)
    expected.zipWithIndex.foreach { case ((table, name, _), index) =>
      val installed = prefixes(index + 1)._1.tables.find(_.name == table).get.constraints.collect {
        case constraint if constraint.kind == MySqlManagedStaticRecipe.ConstraintKind.ForeignKey &&
            constraint.name.contains(name) => constraint.name.get
      }
      assertEquals(Vector(name), installed)
    }

    val full = MySqlManagedStaticRecipe.fromPlan(plan)
    assertEquals(full.tables, prefixes.last._1.tables)
    assertEquals(MySqlManagedTableProjection.project(full, rendered), prefixes.last._2)
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedStaticRecipe.fromPlanAt(plan, Set(ordinals(1))))
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedStaticRecipe.fromPlanAt(plan, Set(ordinals(0), ordinals(2))))
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedTableProjection.projectAt(prefixes(1)._1, rendered, Set(ordinals(0), ordinals(1))))


  @Test def actualFirstFiveTableCatalogRowsMatchCompiledPreDeferredProjection(): Unit =
    val stream = getClass.getResourceAsStream("/mysql/rbss-table-catalog-20261105.json")
    val fixture = try com.google.gson.JsonParser.parseReader(new java.io.InputStreamReader(stream,
      java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject finally stream.close()
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs("floatingpodrbssv1"))
    val plan = MySqlManagedSchemaPlan.build(rendered)
    val projection = MySqlManagedTableProjection.projectAt(MySqlManagedStaticRecipe.fromPlanAt(plan, Set.empty), rendered, Set.empty)
    val indexes = Map("RS02_TABLES" -> 1, "RS03_COLUMNS" -> 1, "RS04_STATISTICS" -> 1,
      "RS05_TABLE_CONSTRAINTS" -> 4, "RS06_KEY_COLUMN_USAGE" -> 5, "RS07_REFERENTIAL_CONSTRAINTS" -> 9)
    import scala.jdk.CollectionConverters.*
    fixture.getAsJsonArray("sets").asScala.foreach { entry =>
      val set = entry.getAsJsonObject
      val id = set.get("id").getAsString
      val actual = set.getAsJsonArray("rows").asScala.toVector.map { row =>
        row.getAsJsonArray.asScala.toVector.map { raw =>
          val cell = raw.getAsJsonObject
          ExpectedField.Exact(if cell.get("kind").getAsString == "null" then MySqlAuditNull
            else MySqlAuditText(cell.get("value").getAsString))
        }
      }
      val tables = actual.map(row => text(row, indexes(id))).toSet
      val expected = projection.set(id).rows.filter(row => tables.contains(text(row, indexes(id))))
      if id == "RS02_TABLES" then assertEquals(5, tables.size)
      assertEquals(actual, expected, id)
    }
