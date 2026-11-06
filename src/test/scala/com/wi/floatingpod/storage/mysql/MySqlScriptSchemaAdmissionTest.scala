package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 固定 DATA layout 的交叉 admission 检查；不伪造 schema4 JDBC owner 或 Ready 资格。 */
final class MySqlScriptSchemaAdmissionTest:
  import MySqlScriptSchemaDataTestFixture.*

  private def metadata(port: Int = 3307, capture: String = "c" * 64): com.wi.floatingpod.config.MySqlRuntimeMetadata =
    val base = java.nio.file.Path.of("build/schema4-configuration-fixture").toAbsolutePath.toString.replace('\\', '/')
    val text = s"""backend=mysql
host=database.example.invalid
port=$port
schema=${MySqlScriptSchemaProfile.Schema}
expectedServerUuid=00000000-0000-0000-0000-000000000001
expectedBindingUuid=00000000-0000-0000-0000-000000000002
expectedEffectiveUser=runtime@localhost
username=runtime
pkcs12TrustStorePath=$base/trust.p12
credentialSourceRef=file:$base/credential.txt
trustSecretSourceRef=file:$base/trust-secret.txt
connectTimeoutMillis=3000
socketTimeoutMillis=10000
activeLimit=256
innodbFlushLogAtTrxCommit=1
syncBinlog=1
approvedBaselinePath=$base/approved.json
approvedBaselineSha256=${"a" * 64}
approvedBaselineFormat=FP_MYSQL_APPROVED_STATIC_V1
approvedSchemaFormat=4
approvedCoverage=${MySqlScriptSchemaProfile.Coverage}
expectedServerVersion=9.7.2
expectedProvisioningUuid=00000000-0000-0000-0000-000000000003
approvedDdlSha256=${"b" * 64}
expectedAuditDefiner=audit@localhost
approvedSourceCaptureSha256=$capture
"""
    com.wi.floatingpod.config.MySqlRuntimeMetadata.fromBytes(text.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  private val policy = MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
    MySqlExpectedPhysicalFormat.Format2, java.time.Duration.ofSeconds(10))

  private def security(configuration: Array[Byte], provenance: Array[Byte] = Array[Byte](1, 2, 3)): MySqlScriptSchemaAuditInputs =
    val m = metadata()
    val privileges = MySqlAuditCoverage.Format3PrivilegeResultSetIds.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).zipWithIndex.map {
      case ((id, labels), index) =>
        val columns = labels.map(label => MySqlAuditColumn(label, java.sql.Types.VARCHAR, "VARCHAR", 1024, 0, 0, false))
        val rows = if index != 2 then Vector.empty else Vector("SELECT", "TRIGGER").map { privilege =>
          MySqlAuditRow(labels.map {
            case "GRANTEE" => MySqlAuditText("'audit'@'localhost'")
            case "TABLE_SCHEMA" => MySqlAuditText(MySqlScriptSchemaProfile.Schema)
            case "PRIVILEGE_TYPE" => MySqlAuditText(privilege)
            case "IS_GRANTABLE" => MySqlAuditText("NO")
            case _ => MySqlAuditText("def")
          })
        }
        MySqlAuditResultSet(id, columns, rows)
    }
    new MySqlScriptSchemaAuditInputs(m.expectedServerUuid(), "9.7.2", m.schema(), m.expectedEffectiveUser(),
      "runtime@localhost", "NONE", m.baseline().provisioningUuid(), m.expectedBindingUuid(), m.baseline().ddlSha256(),
      "audit@localhost", Vector("GRANT SELECT ON fp_script_publication1.* TO 'runtime'@'localhost'"), privileges,
      MySqlAuditBinary(configuration), "FRESH_PLAN", MySqlAuditBinary(provenance),
      MySqlAuditBinary(java.security.MessageDigest.getInstance("SHA-256").digest(provenance)), SourceProfile)

  @Test def actualSchema4ConfigurationChangesRejectBeforeAnyConnectionOrCredentialAccess(): Unit =
    val unsigned = security(new Array[Byte](32))
    val m = metadata()
    val digest = MySqlScriptSchemaConfiguration.sha256(m, Inputs, policy, unsigned)
    val approved = security(digest)
    MySqlScriptSchemaConfiguration.requireMatches(m, Inputs, policy, approved)
    val failure = assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlOwnedDatabaseSession.inspectScriptSchema(metadata(port = 3308), Inputs, approved, policy,
        java.util.UUID.randomUUID(), 1L, 1L)(_ => fail("configuration drift must reject before consumer")))
    assertTrue(failure.getMessage.contains("actual configuration projection"), failure.getMessage)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlScriptSchemaConfiguration.requireMatches(m, Inputs,
        policy.copy(maxReceiptAge = java.time.Duration.ofSeconds(11)), approved))
    // capture 可以包含配置摘要；不把其外层hash纳回自身，独立批准验证另行负责。
    assertArrayEquals(digest, MySqlScriptSchemaConfiguration.sha256(metadata(capture = "d" * 64), Inputs, policy, unsigned))

  @Test def independentSourceBytesRemainStrictlyBoundOutsideTheConfigurationProjection(): Unit =
    val expected = security(new Array[Byte](32))
    val changed = security(new Array[Byte](32), Array[Byte](1, 2, 4))
    val columns = expected.manifestLabels.map(label => MySqlAuditColumn(label, java.sql.Types.VARCHAR, "VARCHAR", 100, 0, 0, false))
    val set = MySqlAuditResultSet("RS01_HEADER", columns, Vector(MySqlAuditRow(expected.expectedManifestCells)))
    expected.validateManifest(set, set.rows.head)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => changed.validateManifest(set, set.rows.head))
    assertThrows(classOf[IllegalArgumentException], () =>
      new MySqlScriptSchemaSnapshotJdbc(null, null, null))

  @Test def schema4AuditRequiresAllStaticIdentitiesAndExactEighteenResultProtocolWithoutRuntimeReady(): Unit =
    val profile = MySqlSchemaAuditProfile.ScriptPublication1
    assertEquals(18, MySqlAuditCoverage.resultSetIds(profile).size)
    assertEquals(18, MySqlSchemaAuditor.labelsFor(profile).size)
    assertTrue(MySqlApprovedBaselineLoader.supportsProfile(4, MySqlScriptSchemaProfile.Coverage))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(3, MySqlScriptSchemaProfile.Coverage))
    MySqlSchemaAuditor.validateFormat3Protocol((1 to 18).toVector ++ Vector(0, -1))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.validateFormat3Protocol((1 to 14).toVector ++ Vector(0, -1)))
    val sets = MySqlAuditCoverage.StaticResultSetIds.zip(MySqlSchemaAuditor.labelsFor(profile).slice(1, 11)).map { (id, labels) =>
      val columns = labels.map(label => MySqlAuditColumn(label, java.sql.Types.VARCHAR, "VARCHAR", 1024, 0, 0, false))
      def row(values: Map[String, String]) = MySqlAuditRow(labels.map(label => MySqlAuditText(values.getOrElse(label, "fixture"))))
      val rows = id match
        case "RS02_TABLES" => MySqlScriptSchemaProfile.tables.map(t => row(Map("TABLE_NAME" -> t.name)))
        case "RS03_COLUMNS" => MySqlScriptSchemaProfile.tables.flatMap(t => t.columns.map(c => row(Map("TABLE_NAME" -> t.name, "COLUMN_NAME" -> c.name))))
        case "RS09_TRIGGERS" => MySqlScriptSchemaProfile.guards.map(g => row(Map("TRIGGER_NAME" -> g.name)))
        case _ => Vector.empty
      MySqlAuditResultSet(id, columns, rows)
    }
    MySqlSchemaAuditor.validateProfileObjectCounts(profile, sets)
    Vector(0, 1, 7).foreach { index =>
      val missing = sets.updated(index, sets(index).copy(rows = sets(index).rows.dropRight(1)))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlSchemaAuditor.validateProfileObjectCounts(profile, missing))
    }
    assertNotEquals(MySqlSchemaAuditor.canonicalSha256(profile, sets),
      MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format3, sets))
    val failure = assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlRuntimeInputs.assemble(metadata(), Thread.currentThread(), java.util.UUID.randomUUID(), 1L, policy))
    assertTrue(failure.getMessage.contains("schema4 runtime Ready is not supported"))

  @Test def full20AndFull24LayoutsRejectEachOthersOwnersAndCanonicalWires(): Unit =
    val format3 = MySqlFull20CodecFixture.snapshot(SourceProfile)
    val format3Root = format3.allocationBudget
    val format3Payload = MySqlFormat3SnapshotCodec.encode(format3, format3Root)
    val scriptRoot = root()
    var format3AsScript: MySqlScriptSchemaCanonicalPayload = null
    var scriptPayload: MySqlScriptSchemaCanonicalPayload = null
    var scriptAsFormat3: MySqlFormat3CanonicalPayload = null
    try
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
        MySqlScriptSchemaSnapshotCodec.capture(Array[Byte](1), Inputs, format3Root))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
        MySqlFormat3SnapshotCodec.capture(Array[Byte](1), SourceProfile, scriptRoot))

      val format3Bytes = MySqlFull20CodecFixture.bytes(format3Payload)
      format3AsScript = MySqlScriptSchemaSnapshotCodec.capture(format3Bytes, Inputs, scriptRoot)
      val scriptBeforeDecode = scriptRoot.accountedMemoryBytes
      val format3WireFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshotCodec.decode(format3AsScript, Inputs, scriptRoot))
      assertTrue(format3WireFailure.getMessage.contains("ScriptPublication1 frozen shape text length differs"),
        format3WireFailure.getMessage)
      assertEquals(scriptBeforeDecode, scriptRoot.accountedMemoryBytes,
        "a format3 wire refusal must release only decoder-local allocation debt")
      assertFalse(format3AsScript.storageBudget.isClosed)

      val full24 = snapshot(scriptRoot, relations(Map.empty))
      scriptPayload = MySqlScriptSchemaSnapshotCodec.encode(full24, scriptRoot)
      val scriptBytes = Array.tabulate[Byte](scriptPayload.byteLength)(scriptPayload.bytes.byteAt)
      scriptAsFormat3 = MySqlFormat3SnapshotCodec.capture(scriptBytes, SourceProfile, format3Root)
      val format3BeforeDecode = format3Root.accountedMemoryBytes
      val full24WireFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlFormat3SnapshotCodec.decode(scriptAsFormat3, SourceProfile, format3Root))
      assertTrue(full24WireFailure.getMessage.contains("full20 frozen shape text length differs"), full24WireFailure.getMessage)
      assertEquals(format3BeforeDecode, format3Root.accountedMemoryBytes,
        "a full24 wire refusal must release only full20 decoder-local allocation debt")
      assertFalse(scriptAsFormat3.storageBudget.isClosed)
    finally
      if scriptAsFormat3 != null then scriptAsFormat3.storageBudget.closeScopedReadBudget()
      if scriptPayload != null then scriptPayload.storageBudget.closeScopedReadBudget()
      if format3AsScript != null then format3AsScript.storageBudget.closeScopedReadBudget()
      scriptRoot.closeRootReadBudget()
      format3Payload.storageBudget.closeScopedReadBudget()
      format3Root.closeRootReadBudget()
