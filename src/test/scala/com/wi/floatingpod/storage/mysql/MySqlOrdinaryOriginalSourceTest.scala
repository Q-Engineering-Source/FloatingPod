package com.wi.floatingpod.storage.mysql

import java.sql.SQLNonTransientConnectionException
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.io.TempDir
import scala.jdk.CollectionConverters.*

/** Separates static-object, runtime-audit, and business Full20 DATA scopes without issuing source authority. */
final class MySqlOrdinaryOriginalSourceTest:
  @TempDir private var baselineDirectory: Path = _

  @Test def independentlySpecifiedStaticTenPackLoadsThroughActualFormat3BaselineLoader(): Unit =
    val loaded = MySqlOrdinaryCertifiedHandoffFixture.loadIndependentStaticBaseline(baselineDirectory)
    assertEquals(MySqlAuditCoverage.Format3, loaded.identity.coverage)
    assertEquals(MySqlSchemaAuditProfile.Format3, loaded.objects.profile)
    assertEquals(MySqlAuditCoverage.StaticResultSetIds, loaded.objects.resultSets.map(_.id))
    assertEquals(20, loaded.objects.resultSets.head.rows.size)
    assertEquals(368, loaded.objects.resultSets(1).rows.size)
    assertEquals(60, loaded.objects.resultSets(7).rows.size)
    assertEquals("SYNTHETIC_ORDINARY_TEST_ONLY", MySqlOrdinaryCertifiedHandoffFixture.syntheticContract().get("attribution").getAsString)

  @Test def pinnedDefinitionsKeepStatic10Runtime18AndFull20Separate(): Unit =
    val staticIds = MySqlOrdinaryCertifiedHandoffFixture.StaticResultSetIds
    val runtimeIds = MySqlOrdinaryCertifiedHandoffFixture.RuntimeResultSetIds
    val tableNames = MySqlOrdinaryCertifiedHandoffFixture.tableNames()
    val columns = MySqlOrdinaryCertifiedHandoffFixture.columnDeclarations()

    assertEquals(10, staticIds.size)
    assertEquals(MySqlAuditCoverage.StaticResultSetIds, staticIds)
    assertEquals(18, runtimeIds.size)
    assertEquals(MySqlAuditCoverage.resultSetIds(MySqlSchemaAuditProfile.Format3), runtimeIds)
    assertEquals(20, tableNames.size)
    assertEquals(368, columns.size)
    assertEquals(MySqlOrdinaryCertifiedHandoffFixture.Full20RelationNames.toSet, tableNames.toSet)
    assertTrue(columns.exists { case (table, name, ddl) =>
      table == "fp_schema_manifest" && name == "limit_profile_bytes" && ddl == "LONGBLOB NOT NULL"
    })
    assertTrue(columns.exists { case (table, name, ddl) =>
      table == "fp_runtime_writer" && name == "last_commit_operation_uuid" && ddl.endsWith("NULL")
    })

  @Test def format3RuntimeProtocolRequiresAllEighteenOrderedSetsAndTerminalCounts(): Unit =
    val valid = (1 to 18).toVector ++ Vector(0, -1)
    assertDoesNotThrow((() => MySqlSchemaAuditor.validateFormat3Protocol(valid)): Executable)
    val missingPrivilege = valid.dropRight(3) ++ Vector(0, -1)
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSchemaAuditor.validateFormat3Protocol(missingPrivilege))
    val reordered = valid.updated(0, 2).updated(1, 1)
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSchemaAuditor.validateFormat3Protocol(reordered))

  @Test def independentSyntheticObjectsHaveMeaningfulBodiesExactNamesAndMinimumPrivilegeShape(): Unit =
    val contract = MySqlOrdinaryCertifiedHandoffFixture.syntheticContract()
    val triggerNames = MySqlOrdinaryCertifiedHandoffFixture.syntheticTriggerNames()
    val triggerBody = contract.get("triggerBody").getAsString
    val routine = contract.get("auditRoutineBody").getAsString
    val queryOrder = Vector(
      "SELECT @@server_uuid AS server_uuid",
      "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE",
      "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME",
      "SELECT TABLE_SCHEMA,TABLE_NAME,NON_UNIQUE",
      "SELECT CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_SCHEMA",
      "SELECT CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,TABLE_CATALOG",
      "SELECT CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,UNIQUE_CONSTRAINT_CATALOG",
      "SELECT CONSTRAINT_CATALOG,CONSTRAINT_SCHEMA,CONSTRAINT_NAME,CHECK_CLAUSE",
      "SELECT TRIGGER_SCHEMA,TRIGGER_NAME,EVENT_OBJECT_SCHEMA",
      "SELECT ROUTINE_SCHEMA,ROUTINE_NAME,ROUTINE_TYPE",
      "SELECT SPECIFIC_CATALOG,SPECIFIC_SCHEMA,SPECIFIC_NAME",
      "SELECT singleton_fixed1,binding_uuid,cutover_state,format_version",
      "SELECT singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid",
      "SELECT @@server_uuid AS server_uuid",
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PRIVILEGE_TYPE",
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME",
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,PRIVILEGE_TYPE",
      "SELECT GRANTEE,TABLE_CATALOG,PRIVILEGE_TYPE")

    assertEquals("SYNTHETIC_ORDINARY_TEST_ONLY", contract.get("attribution").getAsString)
    assertEquals("fp_audit_schema_format3", contract.get("auditProcedure").getAsString)
    assertEquals("BEFORE_ROW_INSERT_UPDATE_DELETE", contract.get("triggerEventShape").getAsString)
    assertEquals(60, triggerNames.size)
    assertEquals(60, triggerNames.distinct.size)
    assertTrue(triggerBody.contains("SIGNAL SQLSTATE '45000'"),
      "synthetic trigger deliberately fails closed on DML; it is not an empty-body stand-in")
    assertTrue(triggerBody.contains("SYNTHETIC_FORMAT3_GUARD_REJECTS_DML"))
    assertFalse(routine.isBlank)
    assertEquals(18, "(?m)^\\s*SELECT\\s".r.findAllIn(routine).length,
      "synthetic audit routine must express all eighteen result positions")
    var previous = -1
    queryOrder.foreach { query =>
      val next = routine.indexOf(query, previous + 1)
      assertTrue(next > previous, s"synthetic audit routine is missing or reorders $query")
      previous = next
    }

    val privileges = contract.getAsJsonObject("minimumAuditDefinerPrivileges")
    assertEquals(Vector("SELECT"), privileges.getAsJsonArray("table").iterator().asScala.map(_.getAsString).toVector)
    assertTrue(privileges.getAsJsonArray("column").isEmpty)
    assertEquals(Set("SELECT", "TRIGGER"), privileges.getAsJsonArray("schema").iterator().asScala.map(_.getAsString).toSet)
    assertEquals(Vector("USAGE"), privileges.getAsJsonArray("user").iterator().asScala.map(_.getAsString).toVector)
    assertFalse(privileges.get("grantable").getAsBoolean)
    assertEquals("fp_ordinary_synthetic", contract.get("schema").getAsString)
    assertEquals("9.7.2", contract.get("serverVersion").getAsString)

  @Test def historicalSqlNullReceiptIsCheckedAgainstTheFull20BusinessImage(): Unit =
    val snapshot = MySqlOrdinaryCertifiedHandoffFixture.full20WithUnownedHistoricalWriter()
    val assertions = MySqlOrdinaryCertifiedHandoffFixture.historicalWriterAssertion()
    assertEquals(20, snapshot.relations.size)
    assertTrue(snapshot.relations.exists(_.rows.nonEmpty))
    assertEquals(1, snapshot.relation("floating_pod_state").rows.size)
    assertDoesNotThrow((() => MySqlOrdinaryHandoffGraph.requireHistoricalWriter(snapshot, assertions)): Executable)

    val writer = snapshot.relation("fp_runtime_writer")
    assertEquals(MySqlFormat3Cell.SqlNull, writer.rows.head(5))
    assertEquals(MySqlFormat3Cell.SqlNull, writer.rows.head(6))
