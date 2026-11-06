package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlManagedCatalogCaptureTest:
  private def inputs: MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs("fp_managed_catalog_test",
      Accounts(account("fp_s5_guard"), account("fp_s5_audit"), account("fp_s5_runtime"),
        account("fp_s5_provisioner"), account("fp_s5_importer"), account("fp_s5_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)

  private def rendered: MySqlDbFirstSchemaProfile.Rendered =
    MySqlDbFirstSchemaProfile.renderExistingDatabase(inputs)

  @Test def catalogCapturePlanUsesTenActualSingleSelectsBoundToRenderedSchema(): Unit =
    val actual = rendered
    val queries = MySqlManagedCatalogCapture.directCatalogQueries(actual)
    assertEquals(Vector("RS02_TABLES", "RS03_COLUMNS", "RS04_STATISTICS", "RS05_TABLE_CONSTRAINTS",
      "RS06_KEY_COLUMN_USAGE", "RS07_REFERENTIAL_CONSTRAINTS", "RS08_CHECK_CONSTRAINTS", "RS09_TRIGGERS",
      "RS10_AUDITSELF", "RS11_AUDITSELF_PARAMETERS"), queries.map(_.id))
    assertEquals(Vector("TABLES", "COLUMNS", "STATISTICS", "TABLE_CONSTRAINTS", "KEY_COLUMN_USAGE",
      "REFERENTIAL_CONSTRAINTS", "CHECK_CONSTRAINTS", "TRIGGERS", "ROUTINES", "PARAMETERS"), queries.map(_.source))
    assertEquals(Vector("TABLE_SCHEMA", "TABLE_SCHEMA", "TABLE_SCHEMA", "TABLE_SCHEMA", "TABLE_SCHEMA",
      "CONSTRAINT_SCHEMA", "CONSTRAINT_SCHEMA", "TRIGGER_SCHEMA", "ROUTINE_SCHEMA", "SPECIFIC_SCHEMA"),
      queries.map(_.schemaColumn))
    queries.foreach { query =>
      assertTrue(query.sql.matches("(?is)^SELECT\\b.*"), query.id)
      assertEquals(1, "(?i)\\bSELECT\\b".r.findAllIn(query.sql).length, query.id)
      assertEquals(1, "(?i)\\bFROM\\b".r.findAllIn(query.sql).length, query.id)
      assertTrue(query.sql.contains(s"CAST(${query.schemaColumn} AS BINARY)=CAST('${actual.inputs.schema}' AS BINARY)"), query.id)
      assertEquals(MySqlScriptSchemaProfile.sha256(query.sql.getBytes(StandardCharsets.UTF_8)), query.sha256)
    }

  @Test def directGrantQueriesComeFromCompiledRoutineAndScopeAllSixConfiguredAccounts(): Unit =
    val actual = rendered
    val queries = MySqlManagedCatalogCapture.directSecurityQueries(actual)
    val securityMarker = actual.sql.indexOf("-- RS15..18:")
    assertTrue(securityMarker >= 0)
    val followingEndMarkers = "(?m)^[ \\t]*END\\$\\$[ \\t]*$".r.findAllIn(actual.sql.substring(securityMarker)).length
    assertTrue(followingEndMarkers > 1, "moved routine is followed by trigger terminators; extraction must stop at its first END$$")
    assertEquals(MySqlAuditCoverage.Format3PrivilegeResultSetIds, queries.map(_.id))
    assertEquals(Vector("TABLE_PRIVILEGES", "COLUMN_PRIVILEGES", "SCHEMA_PRIVILEGES", "USER_PRIVILEGES"),
      queries.map(_.source))
    val subjects = actual.inputs.accounts.all.map(account => "'" + account.grantee.replace("'", "''") + "'").mkString(",")
    queries.foreach { query =>
      assertTrue(query.sql.matches("(?is)^SELECT\\b.*"), query.id)
      assertTrue(query.sql.contains(s"GRANTEE IN ($subjects)"), query.id)
      assertFalse(query.sql.contains("TABLE_SCHEMA='"), query.id)
      assertEquals(MySqlScriptSchemaProfile.sha256(query.sql.getBytes(StandardCharsets.UTF_8)), query.sha256)
    }
    assertEquals(6, actual.inputs.accounts.all.map(_.identity).distinct.size)

  @Test def catalogAndSecurityMarkerIntervalsRejectUnknownAndDuplicateMarkers(): Unit =
    val actual = rendered
    val unknownCatalogMarker = actual.sql.replace("-- RS06 KEY_COLUMN_USAGE:", "-- RS99 KEY_COLUMN_USAGE:")
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedCatalogCapture.extractCatalogQueries(unknownCatalogMarker, actual.inputs.schema))
    val duplicateCatalogMarker = actual.sql.replace("-- RS06 KEY_COLUMN_USAGE:", "-- RS05 KEY_COLUMN_USAGE:")
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedCatalogCapture.extractCatalogQueries(duplicateCatalogMarker, actual.inputs.schema))

    val unknownSecurityMarker = actual.sql.replace("-- RS15..18:", "-- RS15..17:")
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedCatalogCapture.extractSecurityQueries(unknownSecurityMarker, actual.inputs))
    val marker = "-- RS15..18:"
    val offset = actual.sql.indexOf(marker)
    assertTrue(offset >= 0)
    val duplicateSecurityMarker = actual.sql.substring(0, offset) + "-- RS15..18: duplicate\n  " + actual.sql.substring(offset)
    assertThrows(classOf[SQLDataException], () =>
      MySqlManagedCatalogCapture.extractSecurityQueries(duplicateSecurityMarker, actual.inputs))
