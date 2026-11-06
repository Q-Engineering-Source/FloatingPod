package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.ResultSetMetaData
import java.sql.Types
import java.util.HexFormat
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlDeploymentReceiptSchemaProfileTest:
  @org.junit.jupiter.api.io.TempDir var temporary: java.nio.file.Path = _
  private val auditConsumers = scala.collection.mutable.ArrayBuffer.empty[MySqlDeploymentReceiptSchemaAuditInputs]
  private val retained = scala.collection.mutable.ArrayBuffer.empty[(MySqlRuntimeInputs.AssemblyOwner, MySqlFormat3ReadBudget)]
  @org.junit.jupiter.api.AfterEach def releaseStaticGraphs(): Unit =
    auditConsumers.reverseIterator.foreach(_.close())
    auditConsumers.clear()
    retained.reverseIterator.foreach { (owner, root) =>
      try owner.close() finally root.closeRootReadBudget()
    }
    retained.clear()

  private val serverUuid = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val provisioningUuid = UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f")
  private val bindingUuid = UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604")

  private val sourceLimits = SourceLimitProfile.of(
    1_000_000L, 65_536L, 2_000_000L, 64L, 100_000L, 134_217_728L, 1_000_000L, 2_000_000L)
  private val publicationBudget = MySqlScriptSchemaProfile.Budget(
    evidenceBytes = 16_384L,
    diagnosticsBytes = 16_384L,
    configBytes = 16_384L,
    requestBytes = 4096L,
    snapshotBytes = 65_536L,
    receiptBytes = 142L,
    rows = 20_000L,
    retainedBytes = 2_000_000L,
    packetBytes = 1_000_000L)
  private val deploymentBudget = MySqlDeploymentReceiptSchemaDefinition.Budget(
    evidenceBytes = 8192L, diagnosticsBytes = 8192L)

  private def accounts(
      guard: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_guard", "127.0.0.1"),
      audit: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_audit", "127.0.0.1"),
      runtime: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_runtime", "127.0.0.1"),
      provisioner: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_provisioner", "127.0.0.1"),
      importer: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_importer", "127.0.0.1"),
      quota: MySqlDeploymentReceiptSchemaProfile.Account = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_quota", "127.0.0.1")
  ): MySqlDeploymentReceiptSchemaProfile.Accounts =
    MySqlDeploymentReceiptSchemaProfile.Accounts(guard, audit, runtime, provisioner, importer, quota)

  private def profileInputs(
      schema: String = "fp_schema5_test",
      profileAccounts: MySqlDeploymentReceiptSchemaProfile.Accounts = accounts(),
      quotaMaximumRecords: Long = 4096L,
      trustedProxyRows: Vector[MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow] = Vector.empty
  ): MySqlDeploymentReceiptSchemaProfile.Inputs =
    MySqlDeploymentReceiptSchemaProfile.inputs(schema, profileAccounts, sourceLimits, publicationBudget,
      deploymentBudget, quotaMaximumRecords, trustedProxyRows)

  @Test
  def schema5RendererProducesTheClosed26Table467Column78GuardLayout(): Unit =
    val rendered = MySqlDeploymentReceiptSchemaProfile.render(profileInputs())
    val statements = MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val manifest = rendered.tables.find(_.name == "fp_schema_manifest").get
    val manifestDdl = statements.find(_.startsWith("CREATE TABLE fp_schema_manifest ")).get
    val operation = rendered.tables.find(_.name == MySqlDeploymentReceiptSchemaDefinition.OperationTable).get
    val quota = rendered.tables.find(_.name == MySqlDeploymentReceiptSchemaDefinition.QuotaTable).get

    assertEquals(26, rendered.tables.size)
    assertEquals(467, rendered.tables.map(_.columns.size).sum)
    assertEquals(78, rendered.guards.size)
    assertEquals(MySqlDeploymentReceiptSchemaProfile.GuardNames, rendered.guards.map(_.name))
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.operationColumns.map(_.name), operation.columns.map(_.name))
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.quotaColumns.map(_.name), quota.columns.map(_.name))
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.operationPrimaryKey, operation.primaryKey)
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.quotaPrimaryKey, quota.primaryKey)
    assertTrue(manifest.checks.contains("format_version=5"))
    assertFalse(manifest.checks.contains("format_version=4"))
    assertTrue(manifestDdl.contains("CONSTRAINT ck_f3_01_03 CHECK (format_version=5)"))
    assertTrue(MySqlScriptSchemaProfile.tables.find(_.name == "fp_schema_manifest").get.checks.contains("format_version=4"))
    assertEquals(26, MySqlSchemaAuditProfile.DeploymentReceipt1.expectedTableCount.get)
    assertEquals(78, MySqlSchemaAuditProfile.DeploymentReceipt1.expectedTriggerCount.get)
    assertEquals(MySqlDeploymentReceiptSchemaProfile.Coverage, MySqlSchemaAuditProfile.DeploymentReceipt1.coverage)
    assertEquals(MySqlDeploymentReceiptSchemaProfile.AuditDomain, MySqlSchemaAuditProfile.DeploymentReceipt1.hashDomain)
    assertEquals(4, MySqlScriptSchemaProfile.SchemaFormat)
    assertFalse(MySqlScriptSchemaProfile.AuditDomain == MySqlDeploymentReceiptSchemaProfile.AuditDomain)
    assertEquals(MySqlScriptSchemaProfile.sha256(rendered.sql.getBytes(StandardCharsets.UTF_8)), rendered.sha256)
    assertEquals(64, rendered.sha256.length)

  @Test
  def schema5RecoveryRegisterAllowsOnlyTheExactEmptyPreimageAndKeepsItsDigestCheck(): Unit =
    val rendered = MySqlDeploymentReceiptSchemaProfile.render(profileInputs())
    val statements = MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val recoveryTable = rendered.tables.find(_.name == "fp_recovery_operation").get
    val recoveryDdl = statements.find(_.startsWith("CREATE TABLE fp_recovery_operation ")).get
    val expectedException = MySqlDeploymentReceiptSchemaProfile.RecoveryRegisterPreimageCheck
    val expectedStepCheck = s"step IN (${MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.map(x => s"'$x'").mkString(",")})"

    assertEquals("(step='DEPLOYMENT_REGISTER' AND OCTET_LENGTH(preimage_bytes)=0) OR " +
      "(step<>'DEPLOYMENT_REGISTER' AND OCTET_LENGTH(preimage_bytes)>0)", expectedException)
    assertTrue(recoveryTable.checks.contains(expectedException))
    assertFalse(recoveryTable.checks.contains("OCTET_LENGTH(preimage_bytes)>0"))
    assertTrue(recoveryDdl.contains(s"CONSTRAINT ck_f3_04_34 CHECK ($expectedException)"))
    assertTrue(recoveryDdl.contains("preimage_sha256=UNHEX(SHA2(preimage_bytes,256))"))
    assertTrue(recoveryTable.checks.contains(expectedStepCheck))
    assertTrue(recoveryDdl.contains(s"CONSTRAINT ck_f3_04_01 CHECK ($expectedStepCheck)"))
    assertEquals(22,MySqlDeploymentReceiptSchemaDefinition.OldRecoverySteps.size)
    assertEquals(Vector("RECALL_PREPARE"),MySqlDeploymentReceiptSchemaDefinition.RecallPrepareSteps)
    assertEquals(28,MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.size)
    assertEquals(28,MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.distinct.size)
    assertTrue(MySqlScriptSchemaProfile.tables.find(_.name == "fp_recovery_operation").get.checks
      .contains("OCTET_LENGTH(preimage_bytes)>0"))
    assertEquals(4, MySqlScriptSchemaProfile.SchemaFormat)

  @Test
  def schema5RendererRequiresExplicitDistinctIdentitiesBudgetsAndPositiveQuota(): Unit =
    val valid = profileInputs()
    assertEquals(4096L, valid.quotaMaximumRecords)
    assertEquals(sourceLimits, valid.sourceLimitProfile)
    assertEquals(8192L, valid.deploymentBudget.evidenceBytes)
    assertThrows(classOf[IllegalArgumentException], () => { profileInputs(schema = "bad-name"); () })
    assertThrows(classOf[IllegalArgumentException], () => { profileInputs(quotaMaximumRecords = 0L); () })
    assertThrows(classOf[IllegalArgumentException], () => {
      profileInputs(profileAccounts = accounts(importer = MySqlDeploymentReceiptSchemaProfile.Account("fp_s5_runtime", "127.0.0.1")))
      ()
    })
    assertThrows(classOf[IllegalArgumentException], () => {
      MySqlDeploymentReceiptSchemaProfile.inputs("fp_schema5_test", accounts(), sourceLimits, publicationBudget,
        deploymentBudget, 4096L, null)
      ()
    })
    assertNotEquals(valid.profileIdentitySha256, profileInputs(schema = "fp_schema5_other").profileIdentitySha256)
    assertNotEquals(valid.profileIdentitySha256,
      profileInputs(quotaMaximumRecords = 4097L).profileIdentitySha256)
    assertNotEquals(valid.profileIdentitySha256,
      profileInputs(trustedProxyRows = Vector(MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow(
        "trusted_admin", "localhost", "fp_s5_runtime", "127.0.0.1", withGrant = false))).profileIdentitySha256)
    val explicitReservedTokens = MySqlDeploymentReceiptSchemaProfile.render(profileInputs(
      schema = "target_fp_s4_fp_script_publication1",
      profileAccounts = accounts(
        guard = MySqlDeploymentReceiptSchemaProfile.Account("g_fp_s4_helper", "127.0.0.1"),
        audit = MySqlDeploymentReceiptSchemaProfile.Account("a_fp_script_publication1", "127.0.0.1"))))
    assertTrue(explicitReservedTokens.sql.contains("CREATE DATABASE `target_fp_s4_fp_script_publication1`"))
    assertTrue(explicitReservedTokens.sql.contains("USE `target_fp_s4_fp_script_publication1`;"))
    assertTrue(explicitReservedTokens.sql.contains(
      s"PROCEDURE `target_fp_s4_fp_script_publication1`.`${MySqlDeploymentReceiptSchemaProfile.AuditRoutine}`"))
    assertTrue(explicitReservedTokens.sql.contains("CREATE USER 'g_fp_s4_helper'@'127.0.0.1' ACCOUNT LOCK;"))
    assertTrue(explicitReservedTokens.sql.contains("'a_fp_script_publication1'@'127.0.0.1'"))
    assertFalse(explicitReservedTokens.sql.contains("'g_fp_s5_helper'@'127.0.0.1'"))

  @Test
  def schema5OperationInsertChecksEveryRequiredColumnAndDefinitionPredicateBeforeQuotaCharge(): Unit =
    val inputs = profileInputs()
    val rendered = MySqlDeploymentReceiptSchemaProfile.render(inputs)
    val statements = MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val insert = statements.find(statement => statement.startsWith("CREATE DEFINER=") &&
      statement.contains("TRIGGER fp_s5_25_bi ")).get
    val chargeAt = insert.indexOf("SET used_records=used_records+1,quota_revision=quota_revision+1")
    val requiredColumns = MySqlDeploymentReceiptSchemaProfile.operationPrechargeNotNullColumns
    val exactChecks = MySqlDeploymentReceiptSchemaProfile.operationPrechargeCheckExpressions(deploymentBudget)
    val update = statements.find(statement => statement.startsWith("CREATE DEFINER=") &&
      statement.contains("TRIGGER fp_s5_25_bu ")).get

    assertTrue(chargeAt > 0)
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.operationColumns.collect {
      case column if column.declaration.contains("NOT NULL") => column.name
    }, requiredColumns)
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.operationChecks(deploymentBudget).size, exactChecks.size)
    assertTrue(requiredColumns.forall(name => insert.indexOf(s"NEW.$name IS NOT NULL") >= 0 &&
      insert.indexOf(s"NEW.$name IS NOT NULL") < chargeAt))
    assertTrue(exactChecks.forall(check => insert.indexOf(check) >= 0 && insert.indexOf(check) < chargeAt))
    assertTrue(exactChecks.exists(_.contains("NEW.max_queue_age_end_ordinals>=0")))
    assertTrue(exactChecks.exists(_.contains("NEW.diagnostic_bytes IS NULL")))
    assertTrue(insert.indexOf("fp_recovery_operation") < chargeAt)
    assertTrue(insert.indexOf("FOR UPDATE") < chargeAt)
    assertTrue(insert.indexOf("FOR SHARE") < chargeAt)
    assertTrue(insert.indexOf("IF v_found THEN") < chargeAt)
    assertTrue(update.contains("ORD(SUBSTRING(OLD.receipt_bytes,136,1)) IN (5,6) AND " +
      "ORD(SUBSTRING(NEW.receipt_bytes,136,1)) IN (3,5,6)"))

  @Test
  def schema5RenderedPrivilegeAndLockChecksUseVisibleDirectMetadataWithoutCredentialReads(): Unit =
    val rendered = MySqlDeploymentReceiptSchemaProfile.render(profileInputs())
    val statements = MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val auditRoutine = statements.find(statement => statement.startsWith("CREATE DEFINER=") &&
      statement.contains("PROCEDURE `fp_schema5_test`.`" + MySqlDeploymentReceiptSchemaProfile.AuditRoutine + "`" )).get
    val privilegeQueries = Vector(
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,PRIVILEGE_TYPE,IS_GRANTABLE",
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,PRIVILEGE_TYPE,IS_GRANTABLE",
      "SELECT GRANTEE,TABLE_CATALOG,TABLE_SCHEMA,PRIVILEGE_TYPE,IS_GRANTABLE")
      .map { start =>
        val begin = auditRoutine.indexOf(start)
        auditRoutine.substring(begin, auditRoutine.indexOf(';', begin))
      }

    assertTrue(auditRoutine.contains("FROM mysql.user u"))
    assertTrue(auditRoutine.contains("u.account_locked='Y'"))
    assertTrue(auditRoutine.contains("fp_account_count<>5 OR fp_bad<>5"))
    assertTrue(auditRoutine.contains("FROM mysql.procs_priv p"))
    assertFalse(auditRoutine.contains("INFORMATION_SCHEMA.ROUTINE_PRIVILEGES"))
    assertFalse(auditRoutine.contains("authentication_string"))
    assertFalse(auditRoutine.contains("password_last_changed"))
    assertTrue(auditRoutine.contains("p.Proc_priv='Execute'"))
    assertTrue(auditRoutine.contains("p.Routine_type='PROCEDURE'"))
    assertTrue(privilegeQueries.forall(_.contains("GRANTEE IN (")))
    assertTrue(privilegeQueries.forall(query => !query.contains("TABLE_SCHEMA='fp_schema5_test'")))
    assertFalse(auditRoutine.contains("locked audit definer ONLY"))
    assertTrue(rendered.sql.contains("GRANT SELECT ON `mysql`.* TO 'fp_s5_audit'@'127.0.0.1';"))
    assertTrue(rendered.sql.contains("CREATE USER 'fp_s5_quota'@'127.0.0.1' ACCOUNT LOCK;"))
    assertTrue(rendered.sql.contains("GRANT UPDATE (quota_revision,used_records) ON `fp_schema5_test`.`fp_deployment_quota` TO 'fp_s5_quota'@'127.0.0.1';"))
    assertFalse(rendered.sql.contains("GRANT UPDATE ON `fp_schema5_test`.`fp_deployment_quota` TO 'fp_s5_quota'@'127.0.0.1';"))
    assertTrue(rendered.sql.contains("GRANT SHOW_ROUTINE ON *.* TO 'fp_s5_audit'@'127.0.0.1';"))
    assertFalse(rendered.sql.contains("GRANT SHOW_ROUTINE ON *.* TO 'fp_s5_runtime'@'127.0.0.1';"))
    assertFalse(rendered.sql.contains("GRANT SELECT ON *.*"))
    val proxyRows = Vector(
      MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow("trusted_reader", "proxy-host", "target_writer", "target-host", withGrant = false),
      MySqlDeploymentReceiptSchemaProfile.TrustedProxyRow("trusted_owner", "owner-host", "target_admin", "admin-host", withGrant = true))
    val proxyRendered = MySqlDeploymentReceiptSchemaProfile.render(profileInputs(trustedProxyRows = proxyRows))
    val proxyAudit = MySqlScriptSchemaProfile.statements(proxyRendered.sql).toOption.get.find(statement =>
      statement.startsWith("CREATE DEFINER=") && statement.contains("PROCEDURE `fp_schema5_test`.`" +
        MySqlDeploymentReceiptSchemaProfile.AuditRoutine + "`" )).get
    assertTrue(proxyAudit.contains("BINARY p.User=BINARY 'trusted_reader' AND BINARY p.Host=BINARY 'proxy-host' AND " +
      "BINARY p.Proxied_user=BINARY 'target_writer' AND BINARY p.Proxied_host=BINARY 'target-host' AND p.With_grant=0"))
    assertTrue(proxyAudit.contains("BINARY p.User=BINARY 'trusted_owner' AND BINARY p.Host=BINARY 'owner-host' AND " +
      "BINARY p.Proxied_user=BINARY 'target_admin' AND BINARY p.Proxied_host=BINARY 'admin-host' AND p.With_grant=1"))
    assertFalse(proxyAudit.contains("p.With_grant=BINARY 'N'"))
    assertFalse(proxyAudit.contains("p.With_grant=BINARY 'Y'"))

  @Test
  def schema5ReceiptPrefixMatchesTheActualJavaWireCodec(): Unit =
    val operationId = UUID.fromString("9a5d3e5a-799e-47aa-90f6-5d8c472fb041")
    val versions = new DeploymentCommandWireCodec.ExpectedCommandVersions(
      UUID.fromString("d2df04b8-fae6-4bc6-9af7-f3e5b2b7bd26"), 0L,
      UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604"), 0L, 0L, 0L, 0L)
    val receipt = new DeploymentCommandWireCodec.MutationReceipt(
      operationId,
      UUID.fromString("8d5c1c0b-e9f0-42b1-8900-62608cf111a2"),
      UUID.fromString("73dc7005-bbd7-4ef7-a7a9-8a2a2dc290bc"),
      0L,
      DeploymentCommandWireCodec.MutationKind.DEPLOY,
      versions,
      DeploymentCommandWireCodec.OperationPhase.QUEUED,
      DeploymentCommandWireCodec.ReceiptReason.NONE,
      DeploymentCommandWireCodec.ActualEffectStatus.NOT_STARTED,
      DeploymentCommandWireCodec.CriticalCommitStatus.NOT_ATTEMPTED,
      DeploymentCommandWireCodec.ReadinessStatus.NOT_EVALUATED,
      DeploymentCommandWireCodec.CleanupStatus.NOT_REQUIRED)
    val encoded = DeploymentCommandWireCodec.encodeResponse(receipt)
    val expectedPrefix = Array(0x46.toByte, 0x50.toByte, 0x44.toByte, 0x43.toByte, 0x01.toByte, 0x81.toByte)
    val check = MySqlDeploymentReceiptSchemaDefinition.operationChecks(deploymentBudget)
      .find(_.startsWith("SUBSTRING(receipt_bytes,1,6)=")).get

    assertArrayEquals(expectedPrefix, encoded.take(expectedPrefix.length))
    assertTrue(check.contains("UNHEX('465044430181')"))
    assertFalse(check.contains("UNHEX('465044438101')"))
    assertTrue(DeploymentCommandWireCodec.decodeResponse(encoded)
      .isInstanceOf[DeploymentCommandWireCodec.Decoded[?]])
    val closedAxes = Vector(
      "ORD(SUBSTRING(receipt_bytes,136,1))" -> DeploymentCommandWireCodec.OperationPhase.values().map(_.wireCode()),
      "CAST(CONV(HEX(SUBSTRING(receipt_bytes,137,2)),16,10) AS UNSIGNED)" -> DeploymentCommandWireCodec.ReceiptReason.values().map(_.wireCode()),
      "ORD(SUBSTRING(receipt_bytes,139,1))" -> DeploymentCommandWireCodec.ActualEffectStatus.values().map(_.wireCode()),
      "ORD(SUBSTRING(receipt_bytes,140,1))" -> DeploymentCommandWireCodec.CriticalCommitStatus.values().map(_.wireCode()),
      "ORD(SUBSTRING(receipt_bytes,141,1))" -> DeploymentCommandWireCodec.ReadinessStatus.values().map(_.wireCode()),
      "ORD(SUBSTRING(receipt_bytes,142,1))" -> DeploymentCommandWireCodec.CleanupStatus.values().map(_.wireCode()))
      .map { (column, codes) => s"$column IN (${codes.sorted.mkString(",")})" }
    assertTrue(closedAxes.forall(code => MySqlDeploymentReceiptSchemaDefinition.LegalReceiptSql.contains(code)))
    val terminalDebt = new DeploymentCommandWireCodec.MutationReceipt(
      operationId, receipt.podUuid(), receipt.moduleInstanceId(), 0L, receipt.commandKind(), versions,
      DeploymentCommandWireCodec.OperationPhase.FAILED, DeploymentCommandWireCodec.ReceiptReason.CLEANUP_DEBT,
      DeploymentCommandWireCodec.ActualEffectStatus.NONE_CONFIRMED,
      DeploymentCommandWireCodec.CriticalCommitStatus.NOT_ATTEMPTED,
      DeploymentCommandWireCodec.ReadinessStatus.NOT_EVALUATED, DeploymentCommandWireCodec.CleanupStatus.PENDING)
    val unknownOutcome = new DeploymentCommandWireCodec.MutationReceipt(
      operationId, receipt.podUuid(), receipt.moduleInstanceId(), 0L, receipt.commandKind(), versions,
      DeploymentCommandWireCodec.OperationPhase.UNKNOWN, DeploymentCommandWireCodec.ReceiptReason.OUTCOME_UNKNOWN,
      DeploymentCommandWireCodec.ActualEffectStatus.UNKNOWN,
      DeploymentCommandWireCodec.CriticalCommitStatus.NOT_ATTEMPTED,
      DeploymentCommandWireCodec.ReadinessStatus.NOT_EVALUATED, DeploymentCommandWireCodec.CleanupStatus.NOT_REQUIRED)
    val applied = new DeploymentCommandWireCodec.MutationReceipt(
      operationId, receipt.podUuid(), receipt.moduleInstanceId(), 0L, receipt.commandKind(), versions,
      DeploymentCommandWireCodec.OperationPhase.APPLIED, DeploymentCommandWireCodec.ReceiptReason.NONE,
      DeploymentCommandWireCodec.ActualEffectStatus.APPLIED_CONFIRMED,
      DeploymentCommandWireCodec.CriticalCommitStatus.COMMITTED,
      DeploymentCommandWireCodec.ReadinessStatus.QUALIFIED, DeploymentCommandWireCodec.CleanupStatus.COMPLETE)
    Vector(receipt, terminalDebt, unknownOutcome, applied).foreach { legalReceipt =>
      assertTrue(DeploymentCommandWireCodec.decodeResponse(DeploymentCommandWireCodec.encodeResponse(legalReceipt))
        .isInstanceOf[DeploymentCommandWireCodec.Decoded[?]])
    }
    val invalidAxisFrames = Vector(135 -> 0xff, 136 -> 0x7f, 138 -> 0xff, 139 -> 0xff, 140 -> 0xff, 141 -> 0xff).map {
      (index, value) =>
        val invalid = encoded.clone()
        invalid(index) = value.toByte
        invalid
    }
    assertTrue(invalidAxisFrames.forall(frame =>
      DeploymentCommandWireCodec.decodeResponse(frame).isInstanceOf[DeploymentCommandWireCodec.StructuralFailure[?]]))

  @Test
  def schema5AuditInputsRequireExactGeneratedPrivilegeClosureAndRejectAddedRows(): Unit =
    val inputs = profileInputs()
    val rendered = MySqlDeploymentReceiptSchemaProfile.render(inputs)
    val privileges = privilegeSets(rendered)
    val audit = auditInputs(inputs, rendered, privileges)
    val runtime = inputs.accounts.runtime
    val callerGrants = Vector(s"GRANT USAGE ON *.* TO ${runtime.grantee}")
    val scope = MySqlCallerScope(serverUuid, 51L, inputs.schema, runtime.identity, "NONE", runtime.identity,
      Some("9.7.2"), callerGrants)

    audit.validateSecurity(scope, scope, privileges)
    audit.validateCapturedCallerGrants(callerGrants)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      audit.requireIdentity(serverUuid, "9.7.2", inputs.schema, runtime.identity, "NONE", provisioningUuid,
        bindingUuid, "00" * 32, inputs.accounts.auditDefiner.identity))

    val extraSchemaPrivilege = MySqlAuditRow(Vector(runtime.grantee, "def", "other_schema", "SELECT", "NO").map(MySqlAuditText.apply))
    val withExtraSchemaPrivilege = privileges.updated(2,
      privileges(2).copy(rows = privileges(2).rows :+ extraSchemaPrivilege))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      audit.validateSecurity(scope, scope, withExtraSchemaPrivilege))

    val extraDynamicPrivilege = MySqlAuditRow(Vector(runtime.grantee, "def", "SYSTEM_VARIABLES_ADMIN", "NO").map(MySqlAuditText.apply))
    val withDynamicPrivilege = privileges.updated(3,
      privileges(3).copy(rows = privileges(3).rows :+ extraDynamicPrivilege))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      audit.validateSecurity(scope, scope, withDynamicPrivilege))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      audit.validateCapturedCallerGrants(Vector("GRANT USAGE ON *.* TO another")))
    assertThrows(classOf[IllegalArgumentException], () => {
      auditInputs(inputs, rendered, withDynamicPrivilege)
      ()
    })
    val userPrivileges = privileges(3).rows.map(_.cells.map(_.asInstanceOf[MySqlAuditText].value))
    assertEquals(7, userPrivileges.size)
    assertTrue(userPrivileges.contains(Vector(inputs.accounts.auditDefiner.grantee, "def", "SHOW_ROUTINE", "NO")))
    val unauthorizedShowRoutine = privileges.updated(3, privileges(3).copy(rows = privileges(3).rows.map { row =>
      if row.cells(2) == MySqlAuditText("SHOW_ROUTINE") then
        MySqlAuditRow(Vector(runtime.grantee, "def", "SHOW_ROUTINE", "NO").map(MySqlAuditText.apply))
      else row
    }))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      audit.validateSecurity(scope, scope, unauthorizedShowRoutine))

  @Test
  def schema5AuditAndBaselineDispatchRejectCrossProfileIdentities(): Unit =
    assertTrue(MySqlSchemaAuditProfile.DeploymentReceipt1.extendedAudit)
    assertEquals(5, MySqlSchemaAuditProfile.DeploymentReceipt1.schemaFormat)
    assertEquals(18, MySqlAuditCoverage.resultSetIds(MySqlSchemaAuditProfile.DeploymentReceipt1).size)
    assertTrue(MySqlApprovedBaselineLoader.supportsProfile(5, MySqlDeploymentReceiptSchemaProfile.Coverage))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(4, MySqlDeploymentReceiptSchemaProfile.Coverage))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(5, MySqlScriptSchemaProfile.Coverage))
    assertTrue(MySqlApprovedBaselineLoader.supportsProfile(4, MySqlScriptSchemaProfile.Coverage))

  private def privilegeSets(rendered: MySqlDeploymentReceiptSchemaProfile.Rendered): Vector[MySqlAuditResultSet] =
    val rows = MySqlDeploymentReceiptSchemaProfile.expectedPrivilegeRows(rendered)
    rows.zipWithIndex.map { (setRows, index) =>
      val labels = MySqlSchemaAuditor.Format3PrivilegeLabels(index)
      val columns = labels.map(label => MySqlAuditColumn(label, Types.VARCHAR, "VARCHAR", 512, 0,
        ResultSetMetaData.columnNoNulls, signed = false))
      val resultRows = setRows.map(row => MySqlAuditRow(row.map(MySqlAuditText.apply)))
      MySqlAuditResultSet(MySqlAuditCoverage.Format3PrivilegeResultSetIds(index), columns, resultRows)
    }

  private def auditInputs(
      profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      rendered: MySqlDeploymentReceiptSchemaProfile.Rendered,
      privileges: Vector[MySqlAuditResultSet]
  ): MySqlDeploymentReceiptSchemaAuditInputs =
    val fixture = DeploymentReceiptStaticTestData.fixture(temporary.resolve(java.util.UUID.randomUUID().toString).toAbsolutePath, profile, privileges)
    val root = fixture.root()
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    retained += ((owner, root))
    val audit = MySqlRuntimeInputs.assembleDeploymentReceiptStatic(fixture.metadata, owner, DeploymentReceiptStaticTestData.policy).openAuditInputs()
    auditConsumers += audit
    audit
