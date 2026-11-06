package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 纯结构及预算行为测试；没有数据库连接，也不证明 MySQL 执行语义。 */
final class MySqlScriptSchemaProfileTest:
  import MySqlScriptSchemaProfile.*

  private def resource: String =
    val in = getClass.getResourceAsStream(Resource)
    assertNotNull(in, Resource)
    try new String(in.readAllBytes(), UTF_8)
    finally in.close()

  private def allStatements: Vector[String] = statements(resource).toOption.get
  private def table(name: String): Table = tables.find(_.name == name).get
  private def digest(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)
  private val budget = Budget(70000, 70000, 70000, 70000, 70000, 70000, 10, 500000, 100000)

  @Test def completeProfileAndSqlAgree(): Unit =
    assertEquals(Vector.empty[String], validateStructure())
    assertEquals(Right(()), validateSql(resource))
    assertEquals(24, tables.size)
    assertEquals(432, tables.map(_.columns.size).sum)
    assertEquals(518, tables.map(_.checks.size).sum)
    assertEquals(73, tables.map(_.foreignKeys.size).sum)
    assertEquals(72, guards.size)
    assertEquals(368, tables.take(20).map(_.columns.size).sum)
    assertEquals(450, tables.take(20).map(_.checks.size).sum)
    assertEquals(62, tables.take(20).map(_.foreignKeys.size).sum)

  @Test def physicalAndSchemaIdentitiesRemainDistinct(): Unit =
    assertEquals(4, SchemaFormat)
    assertEquals(1, PublicationVersion)
    assertEquals(2, PhysicalBindingFormat)
    assertEquals("script-publication1:exact24-tables-exact72-guards-auditself-and-privileges", Coverage)
    assertEquals("FloatingPod-MySQL-Audit-script-publication1-v1", AuditDomain)
    assertEquals("floatingpod.mysql.script-publication1-full24/v1", SnapshotDomain)
    assertTrue(table("fp_schema_manifest").checks.contains("format_version=4"))
    assertTrue(table("physical_recovery_binding").checks.contains("format_version=2"))
    assertTrue(table("binding_bootstrap_intent").checks.contains("binding_format_version=2"))
    val step = table("fp_recovery_operation").checks.find(_.startsWith("step IN (")).get
    val values = "'([^']+)'".r.findAllMatchIn(step).map(_.group(1)).toVector
    assertEquals(1, values.count(_ == "SCRIPT_PUBLICATION"))
    assertEquals("697bb277493d978d17069170a003836ed5906b10f75ba8d0e899abdae28922de", BaselineHashes("foundation.sql"))

  @Test def exactPublicationKeysAndColumns(): Unit =
    val expected = Map(
      "fp_script_catalog" -> Vector("binding_uuid", "catalog_revision", "snapshot_bytes", "snapshot_sha256", "next_version", "last_mutation_operation_uuid"),
      "fp_script_candidate" -> Vector("binding_uuid", "behavior_id", "version", "source_bytes", "source_sha256", "submitted_by", "submission_operation_uuid", "check_revision", "check_status", "check_evidence_bytes", "check_evidence_sha256", "diagnostics_bytes", "diagnostics_sha256", "loader_id", "loader_artifact_sha256", "compiler_version", "schema_version", "schema_sha256", "compilation_sha256", "last_mutation_operation_uuid"),
      "fp_script_version" -> Vector("binding_uuid", "behavior_id", "version", "source_sha256", "compilation_bytes", "compilation_sha256", "compiler_version", "schema_version", "schema_sha256", "layout_config_bytes", "layout_config_sha256", "approved_by", "approval_operation_uuid", "approved_check_revision", "approval_catalog_revision", "status", "revoked_by", "revocation_operation_uuid", "revocation_catalog_revision", "last_mutation_operation_uuid"),
      "fp_script_operation" -> Vector("binding_uuid", "operation_uuid", "request_kind", "behavior_id", "version", "source_sha256", "compilation_sha256", "request_bytes", "request_sha256", "expected_catalog_revision", "after_catalog_revision", "before_bytes", "before_sha256", "after_bytes", "after_sha256", "result_code", "receipt_bytes", "receipt_sha256")
    )
    expected.foreach { (name, columns) => assertEquals(columns, table(name).columns.map(_.name), name) }
    assertEquals(Vector("binding_uuid"), table("fp_script_catalog").primaryKey)
    assertEquals(Vector("binding_uuid", "operation_uuid"), table("fp_script_operation").primaryKey)
    for name <- Vector("fp_script_candidate", "fp_script_version") do
      assertEquals(Vector("binding_uuid", "behavior_id", "version"), table(name).primaryKey)
    val original = table("fp_script_operation").foreignKeys.find(_.target == "fp_recovery_operation").get
    assertEquals(Vector("binding_uuid", "operation_uuid"), original.columns)
    assertEquals(original.columns, original.targetColumns)
    assertFalse(table("fp_script_operation").foreignKeys.exists(f => Set("fp_script_candidate", "fp_script_version").contains(f.target)))
    assertEquals(Vector("binding_uuid", "check_status"), table("fp_script_candidate").indexes.head.columns)
    assertEquals(Vector("binding_uuid", "status"), table("fp_script_version").indexes.head.columns)

  @Test def definitionTamperingIsRejectedByObjectIdentity(): Unit =
    val sql = resource
    val changed = sql.replace("OCTET_LENGTH(source_bytes)<=65536", "OCTET_LENGTH(source_bytes)<=65537")
    assertNotEquals(sql, changed)
    val errors = validateSql(changed).left.toOption.get
    assertTrue(errors.contains("table definitions differ"))
    assertTrue(errors.contains("complete SQL identity differs"))
    val trigger = allStatements.find(_.matches("(?s).*TRIGGER fp_s4_21_bd .*" )).get
    val alteredTrigger = trigger.replace("FALSE", "TRUE")
    val changedGuard = sql.replace(trigger, alteredTrigger)
    assertNotEquals(sql, changedGuard)
    assertTrue(validateSql(changedGuard).left.toOption.get.contains("guard definitions/order differ"))

  @Test def extraObjectsAndPrivilegesInvalidateExactProfile(): Unit =
    for extra <- Vector(
      "CREATE TABLE intruder (id BIGINT PRIMARY KEY);\n",
      "GRANT DELETE ON `fp_script_publication1`.`fp_script_operation` TO 'fp_s4_runtime'@'localhost';\n"
    ) do assertTrue(validateSql(resource + extra).isLeft)

  @Test def lockedAccountsAndPublicationPrivilegesAreExact(): Unit =
    val all = allStatements
    val users = all.filter(_.startsWith("CREATE USER "))
    assertEquals(5, users.size)
    assertTrue(users.forall(_.matches("CREATE USER 'fp_s4_(guard_definer|audit_definer|runtime|provisioner|importer)'@'localhost' ACCOUNT LOCK")))
    val grant = "GRANT (.+) ON `fp_script_publication1`\\.`(fp_script_\\w+)` TO 'fp_s4_(\\w+)'@'localhost'".r
    val publication = all.collect { case grant(privileges, target, account) => (privileges, target, account) }
    assertEquals(19, publication.size)
    assertEquals(4, publication.count { case (privileges, _, account) => account == "runtime" && privileges == "SELECT,INSERT" })
    assertFalse(publication.exists { case (privileges, _, _) => privileges.split(",").contains("DELETE") })
    val runtimeUpdates = publication.collect { case (privileges, target, "runtime") if privileges.startsWith("UPDATE (") => target }
    assertEquals(Set("fp_script_catalog", "fp_script_candidate", "fp_script_version"), runtimeUpdates.toSet)
    val routine = all.filter(_.matches("(?s)CREATE DEFINER='fp_s4_audit_definer'@'localhost'\\s+PROCEDURE.*"))
    assertEquals(1, routine.size)
    assertEquals(18, "(?m)^  SELECT |^   FROM INFORMATION_SCHEMA".r.findAllIn(routine.head).count(_ .startsWith("  SELECT ")))

  @Test def statementFramingRejectsIncompleteInput(): Unit =
    assertEquals(Right(Vector("SELECT 1", "SELECT 2")), statements("-- comment\nSELECT 1;\nSELECT 2;\n"))
    assertEquals(Right(Vector("CREATE PROCEDURE p()\nBEGIN\nSELECT 1;\nEND")), statements("DELIMITER $$\nCREATE PROCEDURE p()\nBEGIN\nSELECT 1;\nEND$$\nDELIMITER ;\n"))
    assertEquals(Left("unterminated SQL statement"), statements("SELECT 1"))
    assertTrue(statements("SELECT 1\nDELIMITER $$\nEND$$\n").isLeft)
    assertTrue(statements("DELIMITER //\nSELECT 1//\n").isLeft)

  @Test def explicitBudgetsRejectInvalidContracts(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => Budget(0, 1, 1, 1, 1, 1, 1, 10, 10))
    assertThrows(classOf[IllegalArgumentException], () => Budget(11, 1, 1, 1, 1, 1, 1, 10, 10))
    assertThrows(classOf[IllegalArgumentException], () => Budget(1, 1, 1, 1, 1, 1, -1, 10, 10))
    assertEquals(10L, budget.rows)

  @Test def sourceAndCompilationEnforceFrozenBoundsAndRawHashes(): Unit =
    for (kind, limit) <- Vector(Payload.Source -> 65536, Payload.Compilation -> 20357) do
      val exact = Array.fill[Byte](limit)(0x41.toByte)
      assertEquals(Right(()), validatePayload(kind, exact, digest(exact), budget))
      val over = exact :+ 0.toByte
      assertEquals(Left("payload budget exceeded"), validatePayload(kind, over, digest(over), budget))
      val corrupt = digest(exact)
      corrupt(0) = (corrupt(0) ^ 1).toByte
      assertEquals(Left("payload SHA-256 mismatch"), validatePayload(kind, exact, corrupt, budget))
      assertTrue(validatePayload(kind, exact, Array.emptyByteArray, budget).isLeft)
    assertTrue(validatePayload(Payload.Source, null, Array.emptyByteArray, budget).isLeft)

  @Test def everyVariablePayloadUsesItsExplicitLimit(): Unit =
    val small = Budget(2, 3, 4, 5, 6, 7, 2, 100, 20)
    for (kind, limit) <- Vector(Payload.Evidence -> 2, Payload.Diagnostics -> 3, Payload.Config -> 4,
      Payload.Request -> 5, Payload.Snapshot -> 6, Payload.Receipt -> 7) do
      val bytes = new Array[Byte](limit)
      assertEquals(Right(()), validatePayload(kind, bytes, digest(bytes), small))
      val tooLong = bytes :+ 0.toByte
      assertEquals(Left("payload budget exceeded"), validatePayload(kind, tooLong, digest(tooLong), small))
    val source = new Array[Byte](21)
    assertTrue(validatePayload(Payload.Source, source, digest(source), small).isLeft)

  @Test def retainedAccountingRejectsOverflowAndLimits(): Unit =
    assertEquals(Right(100L), retainedSize(Vector(20L, 80L), 2, budget))
    assertEquals(Left("row budget exceeded"), retainedSize(Vector(1L), 11, budget))
    assertEquals(Left("row budget exceeded"), retainedSize(Vector.empty, -1, budget))
    assertEquals(Left("retained budget exceeded"), retainedSize(Vector(500001L), 1, budget))
    assertEquals(Left("negative retained length"), retainedSize(Vector(-1L), 1, budget))
    assertEquals(Left("retained length overflow"), retainedSize(Vector(Long.MaxValue, 1L), 2, budget))

