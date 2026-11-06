package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.HexFormat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedSchemaPlanTest:
  private def inputs: MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs("fp_managed_test",
      Accounts(account("fp_s5_guard"), account("fp_s5_audit"), account("fp_s5_runtime"),
        account("fp_s5_provisioner"), account("fp_s5_importer"), account("fp_s5_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)

  private def statements(rendered: MySqlDbFirstSchemaProfile.Rendered): Vector[String] =
    MySqlScriptSchemaProfile.statements(rendered.sql).fold(error => fail(error), identity)

  private def sha256(value: String): String = HexFormat.of().formatHex(
    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)))

  private def isAuditRoutine(statement: String): Boolean =
    statement.matches("(?s)^CREATE DEFINER=.*\\s+PROCEDURE\\s+`[^`]+`\\.`[^`]+`\\(.*$")

  private val DeferredConstraintSql = Vector(
    "ALTER TABLE physical_source_unit ADD CONSTRAINT fk_unit_current_forest\n" +
      " FOREIGN KEY (binding_uuid,unit_uuid,current_forest_uuid,current_epoch)\n" +
      " REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid,source_epoch)",
    "ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_completion\n" +
      " FOREIGN KEY (binding_uuid,source_completion_uuid) REFERENCES fp_lifecycle_completion(binding_uuid,receipt_uuid)",
    "ALTER TABLE physical_recovery_intent ADD CONSTRAINT fk_intent_source_forest\n" +
      " FOREIGN KEY (binding_uuid,source_unit_uuid,source_forest_uuid) REFERENCES physical_source_forest(binding_uuid,unit_uuid,forest_uuid)"
  )

  @Test def existingDatabaseModePinsTheManagedRecipeWithoutChangingLegacyRender(): Unit =
    val configured = inputs
    val full = MySqlDbFirstSchemaProfile.render(configured)
    val again = MySqlDbFirstSchemaProfile.render(configured)
    val existing = MySqlDbFirstSchemaProfile.renderExistingDatabase(configured)
    assertEquals(full.sql, again.sql)
    assertEquals(full.sha256, again.sha256)
    assertEquals(MySqlDbFirstSchemaProfile.RenderMode.CreateDatabaseAndSchema, full.mode)
    assertEquals(MySqlDbFirstSchemaProfile.RenderMode.ExistingDatabaseOnly, existing.mode)
    val before = statements(full)
    val after = statements(existing)
    val createDatabase = s"CREATE DATABASE `${configured.schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin"
    assertEquals(1, before.count(_ == createDatabase))
    assertFalse(after.exists(_.startsWith("CREATE DATABASE ")))
    assertEquals(2, after.count(_ == s"USE `${configured.schema}`"))
    assertNotEquals(full.sha256, existing.sha256)
    assertNotEquals(full.profileIdentitySha256, existing.profileIdentitySha256)
    assertEquals(27, existing.tables.size)
    assertEquals(480, existing.tables.map(_.columns.size).sum)
    assertEquals(81, existing.guards.size)
    val oldProvenanceCheck = "source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT')"
    val managedProvenanceCheck = "source_provenance_kind IN ('FRESH_PLAN','FORMAT3_RESTORE','FORMAT2_IMPORT','SQLITE_IMPORT','MANAGED_INSTALL')"
    assertFalse(full.sql.contains("MANAGED_INSTALL"))
    assertFalse(full.sql.contains("ROW_FORMAT=DYNAMIC"))
    assertEquals(5, before.count(statement => statement.startsWith("SET SESSION sql_mode") || statement.startsWith("SET NAMES ")))
    val legacySeed = before.indexWhere(_.startsWith("INSERT INTO fp_schema_manifest"))
    val legacyTrigger = before.indexWhere(statement => statement.startsWith("CREATE DEFINER=") && statement.contains(" TRIGGER "))
    val legacyRoutine = before.indexWhere(isAuditRoutine)
    assertTrue(legacySeed >= 0 && legacySeed < legacyTrigger && legacyTrigger < legacyRoutine,
      "legacy rendering keeps its original seed/trigger/routine order")
    assertFalse(full.tables.find(_.name == "fp_schema_manifest").get.checks.contains(managedProvenanceCheck))
    val actualByName = after.collect {
      case statement if statement.startsWith("CREATE TABLE ") =>
        val name = "(?s)^CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*$".r.findFirstMatchIn(statement).get.group(1)
        name -> statement
    }.toMap
    assertTrue(actualByName.values.forall(_.contains("ROW_FORMAT=DYNAMIC")),
      "the managed existing-database plan pins row format for every relation")
    assertEquals(1, after.count(_.startsWith(s"INSERT INTO `${configured.schema}`.`fp_deployment_quota`")))
    existing.tables.foreach(table => assertEquals(sha256(actualByName(table.name)), table.statementSha256))
    assertFalse(existing.tables.find(_.name == "fp_schema_manifest").get.checks.contains(oldProvenanceCheck))
    assertTrue(existing.tables.find(_.name == "fp_schema_manifest").get.checks.contains(managedProvenanceCheck))
    assertTrue(actualByName("fp_schema_manifest").contains(managedProvenanceCheck))

  @Test def managedPlanPlacesRoutineAndEveryGrantBeforeSeedsAndAllGuardsAfterSeeds(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    val statements = this.statements(rendered)
    val tableIndexes = statements.indices.filter(index => statements(index).startsWith("CREATE TABLE "))
    val routineIndex = statements.indexWhere(isAuditRoutine)
    val grantIndexes = statements.indices.filter(index => statements(index).startsWith("GRANT "))
    val insertIndexes = statements.indices.filter(index => statements(index).startsWith("INSERT INTO "))
    val quotaIndex = statements.indexWhere(statement => statement.startsWith("INSERT INTO `fp_managed_test`.`fp_deployment_quota`"))
    val controlSeedIndexes = statements.indices.filter(index =>
      statements(index).startsWith("INSERT INTO fp_schema_manifest") || statements(index).startsWith("INSERT INTO fp_runtime_writer"))
    val triggerIndexes = statements.indices.filter(index =>
      statements(index).startsWith("CREATE DEFINER=") && statements(index).contains(" TRIGGER "))
    assertEquals(27, tableIndexes.size)
    assertEquals(3, statements.count(_.startsWith("ALTER TABLE ")))
    assertEquals(3, insertIndexes.size)
    assertEquals(2, controlSeedIndexes.size)
    assertEquals(81, triggerIndexes.size)
    assertTrue(quotaIndex >= 0)
    assertTrue(tableIndexes.max < routineIndex)
    assertTrue(routineIndex < grantIndexes.min)
    assertTrue(grantIndexes.max < insertIndexes.min)
    assertTrue(insertIndexes.max < triggerIndexes.min)
    assertTrue(quotaIndex < controlSeedIndexes.min)

    val plan = MySqlManagedSchemaPlan.build(rendered)
    assertEquals(DeferredConstraintSql,
      statements.filter(_.startsWith("ALTER TABLE ")),
      "the frozen schema renderer exposes exactly the three delayed foreign keys")
    assertEquals(DeferredConstraintSql, plan.deferredConstraintSteps.map(_.sql))
    assertEquals(Vector("constraint:physical_source_unit:fk_unit_current_forest",
      "constraint:physical_recovery_intent:fk_intent_completion",
      "constraint:physical_recovery_intent:fk_intent_source_forest"),
      plan.deferredConstraintSteps.map(_.identity))
    assertEquals(Vector(29, 30, 31), plan.deferredConstraintSteps.map(_.ordinal))
    val strictAll = "STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
    val strictTrans = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
    var currentMode = ""
    var currentNames = ""
    val activeModes = Vector.newBuilder[(MySqlManagedSchemaPlan.Kind, String, String)]
    plan.steps.foreach { step =>
      if step.kind == MySqlManagedSchemaPlan.Kind.SessionSetup then
        if step.sql.startsWith("SET SESSION sql_mode") then
          currentMode = step.sql.substring(step.sql.indexOf('=') + 1).trim.stripPrefix("'").stripSuffix("'")
        else if step.sql.startsWith("SET NAMES ") then currentNames = step.sql
      else if step.kind == MySqlManagedSchemaPlan.Kind.RoutineDefinition ||
          step.kind == MySqlManagedSchemaPlan.Kind.QuotaInitialization ||
          step.kind == MySqlManagedSchemaPlan.Kind.SeedInsert || step.kind == MySqlManagedSchemaPlan.Kind.TriggerDefinition then
        activeModes += ((step.kind, currentMode, currentNames))
    }
    val observations = activeModes.result()
    val utf8Names = "SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin"
    assertEquals(Vector((MySqlManagedSchemaPlan.Kind.RoutineDefinition, strictAll, utf8Names)),
      observations.filter(_._1 == MySqlManagedSchemaPlan.Kind.RoutineDefinition))
    assertEquals(Vector((MySqlManagedSchemaPlan.Kind.QuotaInitialization, strictAll, utf8Names)),
      observations.filter(_._1 == MySqlManagedSchemaPlan.Kind.QuotaInitialization))
    assertEquals(Vector.fill(2)((MySqlManagedSchemaPlan.Kind.SeedInsert, strictAll, utf8Names)),
      observations.filter(_._1 == MySqlManagedSchemaPlan.Kind.SeedInsert))
    assertEquals(Vector.fill(81)((MySqlManagedSchemaPlan.Kind.TriggerDefinition, strictTrans, utf8Names)),
      observations.filter(_._1 == MySqlManagedSchemaPlan.Kind.TriggerDefinition))

  @Test def managedPlanClassifiesTheActualClosedFull27ScriptAndExactBootstrapObjects(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    val plan = MySqlManagedSchemaPlan.build(rendered)
    assertEquals(rendered.inputs.schema, plan.schema)
    assertEquals(rendered.sha256, plan.ddlSha256)
    assertEquals(rendered.profileIdentitySha256, plan.profileIdentitySha256)
    assertEquals(MySqlDbFirstSchemaProfile.TableNames, plan.tables)
    assertEquals(6, plan.accounts.size)
    assertEquals(27, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.TableDefinition))
    assertEquals(3, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.TableConstraint))
    assertEquals(6, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.ManagedAccount))
    assertEquals(81, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.TriggerDefinition))
    assertEquals(1, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.RoutineDefinition))
    assertEquals(1, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.QuotaInitialization))
    assertEquals(2, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.SeedInsert))
    assertEquals(2, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.TargetDatabase))
    assertEquals(7, plan.steps.count(_.kind == MySqlManagedSchemaPlan.Kind.SessionSetup))
    assertEquals(Vector("fp_schema_manifest", "fp_runtime_writer"), plan.seedTemplates.map(_.table))
    assertEquals(MySqlManagedSchemaPlan.Kind.QuotaInitialization, plan.quotaInitialization.kind)
    assertEquals("INSERT INTO `fp_managed_test`.`fp_deployment_quota` (namespace,max_records,used_records,quota_revision) " +
      "VALUES (UNHEX('46504443'),4096,0,0)", plan.quotaInitialization.sql)
    assertEquals(Set("@fp_ddl_sha256", "@fp_provisioning_uuid", "@fp_expected_server_uuid", "@fp_expected_binding_uuid",
      "@fp_configuration_sha256", "@fp_source_provenance_kind", "@fp_source_provenance_bytes", "@fp_source_provenance_sha256",
      "@fp_limit_profile_bytes", "@fp_limit_profile_sha256", "@fp_max_region_bytes", "@fp_max_chunk_bytes",
      "@fp_max_decompressed_bytes", "@fp_max_nbt_depth", "@fp_max_members", "@fp_max_retained_bytes",
      "@fp_max_sql_packet_bytes", "@fp_max_native_artifact_bytes"), plan.requiredSeedVariables.toSet)
    assertTrue(plan.steps.forall(step => step.sha256 == sha256(step.sql)))
    assertEquals(plan.steps.indices.toVector, plan.steps.map(_.ordinal))
    assertTrue(plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.ManagedAccount).forall(_.sql.endsWith("ACCOUNT LOCK")))
    assertFalse(plan.steps.exists(_.sql.startsWith("CREATE DATABASE ")))
    assertFalse(plan.toString.contains("PASSWORD"))

  @Test def legacyDatabaseCreationModeCannotBeMistakenForManagedExistingDatabasePlan(): Unit =
    val legacy = MySqlDbFirstSchemaProfile.render(inputs)
    assertThrows(classOf[IllegalArgumentException], () => MySqlManagedSchemaPlan.build(legacy))

  @Test def managedPlanRejectsAnyUnclassifiedMutationInsteadOfIgnoringIt(): Unit =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)
    val hostileSql = rendered.sql + "\nDELETE FROM fp_schema_manifest;\n"
    val hostile = new MySqlDbFirstSchemaProfile.Rendered(rendered.inputs, hostileSql, sha256(hostileSql),
      rendered.tables, rendered.guards, rendered.mode)
    assertThrows(classOf[SQLDataException], () => MySqlManagedSchemaPlan.build(hostile))
    val hostileAlterSql = rendered.sql +
      "\nALTER TABLE physical_source_unit ADD CONSTRAINT fk_unapproved FOREIGN KEY (binding_uuid,unit_uuid) " +
      "REFERENCES physical_source_unit(binding_uuid,unit_uuid);\n"
    val hostileAlter = new MySqlDbFirstSchemaProfile.Rendered(rendered.inputs, hostileAlterSql, sha256(hostileAlterSql),
      rendered.tables, rendered.guards, rendered.mode)
    assertThrows(classOf[SQLDataException], () => MySqlManagedSchemaPlan.build(hostileAlter))
