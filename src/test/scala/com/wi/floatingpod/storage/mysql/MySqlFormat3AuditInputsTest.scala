package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.security.MessageDigest
import java.sql.{ResultSetMetaData, SQLNonTransientConnectionException, Types}
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlFormat3AuditInputsTest:
  private val id = UUID.fromString("01000000-0000-4000-8000-000000000001")
  private val profile = SourceLimitProfile.of(101L, 102L, 103L, 104L, 105L, 8192L, 4096L, 16384L)
  private def col(label: String) = MySqlAuditColumn(label, Types.VARCHAR, "VARCHAR", 255, 0, ResultSetMetaData.columnNoNulls, false)
  private def privileges = MySqlAuditCoverage.Format3PrivilegeResultSetIds.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).map { (id, labels) =>
    val rows = if id == "RS17_SCHEMA_PRIVILEGES" then Vector("SELECT", "TRIGGER").map(p =>
      MySqlAuditRow(Vector("'audit'@'localhost'", "def", "fp_fixture", p, "NO").map(MySqlAuditText.apply))) else Vector.empty
    MySqlAuditResultSet(id, labels.map(col), rows)
  }
  private def inputs(
      grants: Vector[String] = Vector("GRANT USAGE ON *.* TO 'fixture'@'localhost'"),
      selectedProfile: SourceLimitProfile = profile,
      sourcePayload: Array[Byte] = Array[Byte](1, 2, 3),
      sourceDigest: Array[Byte] = null
  ) =
    val bytes = MySqlAuditBinary(sourcePayload)
    val digest = if sourceDigest == null then MessageDigest.getInstance("SHA-256").digest(sourcePayload) else sourceDigest
    new MySqlFormat3AuditInputs(id, "9.7.2", "fp_fixture", "fixture@localhost", "fixture@localhost", "NONE",
      id, id, "ab" * 32, "audit@localhost", grants, privileges, MySqlAuditBinary(Array.fill(32)(1.toByte)),
      "FRESH_PLAN", bytes, MySqlAuditBinary(digest), selectedProfile)
  private def caller(i: MySqlFormat3AuditInputs) = MySqlCallerScope(id, 17L, "fp_fixture", "fixture@localhost", "NONE",
    "fixture@localhost", Some("9.7.2"), i.callerGrants)
  @Test def ordered18SetsKeepFooter14AndAllFourPrivileges(): Unit =
    val ids = MySqlAuditCoverage.resultSetIds(MySqlSchemaAuditProfile.Format3)
    assertEquals(18, ids.size); assertEquals("RS14_FOOTER", ids(13)); assertEquals("RS18_USER_PRIVILEGES", ids.last)
    assertEquals(38, MySqlSchemaAuditor.labelsFor(MySqlSchemaAuditProfile.Format3).head.size)
    assertEquals(MySqlSchemaAuditor.labelsFor(MySqlSchemaAuditProfile.Format3).head,
      MySqlSchemaAuditor.labelsFor(MySqlSchemaAuditProfile.Format3)(13))
    assertEquals(14, MySqlAuditCoverage.resultSetIds(MySqlSchemaAuditProfile.Format2).size)
    assertEquals(23, MySqlSchemaAuditor.labelsFor(MySqlSchemaAuditProfile.Format2).head.size)
  @Test def exactActualProtocolRejectsMissingRepeatedOrExtraStates(): Unit =
    val good = (1 to 18).toVector ++ Vector(0, -1)
    MySqlSchemaAuditor.validateFormat3Protocol(good)
    for bad <- Vector((1 to 14).toVector ++ Vector(0, -1), (1 to 18).toVector :+ -1,
      (1 to 18).toVector ++ Vector(0, 0, -1), (1 to 19).toVector ++ Vector(0, -1),
      (1 to 18).toVector ++ Vector(1, -1), good.updated(5, 5)) do
      assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSchemaAuditor.validateFormat3Protocol(bad))
  @Test def explicitRetainedAndPacketLimitsHaveNoImplicitDefault(): Unit =
    val small = SourceLimitProfile.of(101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L)
    val budget = new MySqlSchemaAuditor.ReadBudget(Some(small))
    budget.addBytes(100)
    assertThrows(classOf[SQLNonTransientConnectionException], () => budget.addBytes(7))
    val packet = new MySqlSchemaAuditor.ReadBudget(Some(small))
    assertThrows(classOf[SQLNonTransientConnectionException], () => packet.addBytes(108))
  @Test def missingIndependentInputsAndEmptyGrantDefaultsAreRejected(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => inputs(Vector.empty))
    assertThrows(classOf[IllegalArgumentException], () => MySqlSchemaAuditExpectation.forFormat3(id, "9.7.2", "fp_fixture",
      "fixture@localhost", "NONE", id, id, "ab" * 32, "audit@localhost", "NONE", MySqlExpectedNoImport, null, null))
  @Test def exactCallerIdentityAndFullGrantsAreMandatory(): Unit =
    val i = inputs(); val c = caller(i)
    i.validateSecurity(c, c, privileges)
    i.validateRetainedObservation(privileges, c, c)
    for changed <- Vector(c.copy(serverVersion = Some("9.7.1")), c.copy(connectedUser = "other@localhost"),
      c.copy(ownGrants = c.ownGrants :+ "GRANT UPDATE ON fp_fixture.* TO 'fixture'@'localhost'"), c.copy(connectionId = 18L)) do
      assertThrows(classOf[SQLNonTransientConnectionException], () => i.validateSecurity(c, changed, privileges))
  @Test def unexpectedPrivilegeAndChangedMetadataCannotBeHidden(): Unit =
    val i = inputs(); val c = caller(i); val original = privileges.head
    val unexpected = original.copy(rows = Vector(MySqlAuditRow(Vector.fill(original.columns.size)(MySqlAuditText("unexpected")))))
    assertThrows(classOf[SQLNonTransientConnectionException], () => i.validateSecurity(c, c, privileges.updated(0, unexpected)))
    val metadata = original.copy(columns = original.columns.updated(0, original.columns.head.copy(precision = 254)))
    assertThrows(classOf[SQLNonTransientConnectionException], () => i.validateSecurity(c, c, privileges.updated(0, metadata)))
  @Test def fullSourceProfileBytesAndEachBudgetAreCompared(): Unit =
    val i = inputs(); val columns = i.manifestLabels.map(col); val row = MySqlAuditRow(i.expectedManifestCells)
    val set = MySqlAuditResultSet("RS01_HEADER", columns, Vector(row))
    i.validateManifest(set, row)
    for index <- row.cells.indices do
      val changed = row.cells(index) match
        case _: MySqlAuditBinary => MySqlAuditBinary(Array.fill(32)(9.toByte))
        case _: MySqlAuditText => MySqlAuditText("changed")
        case _ => MySqlAuditNull
      assertThrows(classOf[SQLNonTransientConnectionException], () => i.validateManifest(set, row.copy(cells = row.cells.updated(index, changed))))
  @Test def profilePairDispatchRejectsOldCoverageAndUnknownSchema(): Unit =
    assertTrue(MySqlApprovedBaselineLoader.supportsProfile(3, MySqlAuditCoverage.Format3))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(3, MySqlAuditCoverage.Format2))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(2, MySqlAuditCoverage.Format3))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(4, MySqlAuditCoverage.Format3))
  @Test def identityInputsCannotCrossSchemaProvisionOrDdl(): Unit =
    val i = inputs()
    i.requireIdentity(id, "9.7.2", "fp_fixture", "fixture@localhost", "NONE", id, id, "ab" * 32, "audit@localhost")
    assertThrows(classOf[SQLNonTransientConnectionException], () => i.requireIdentity(id, "9.7.2", "other", "fixture@localhost", "NONE", id, id, "ab" * 32, "audit@localhost"))
    assertThrows(classOf[SQLNonTransientConnectionException], () => i.requireIdentity(id, "9.7.2", "fp_fixture", "fixture@localhost", "NONE", UUID.randomUUID(), id, "ab" * 32, "audit@localhost"))
    assertThrows(classOf[SQLNonTransientConnectionException], () => i.requireIdentity(id, "9.7.2", "fp_fixture", "fixture@localhost", "NONE", id, id, "cd" * 32, "audit@localhost"))

  @Test def largeProvenanceLengthIsBudgetedBeforeInternalComparisonCopies(): Unit =
    val payload = Array.fill[Byte](1_048_577)(0x5a.toByte)
    val inputProfile = SourceLimitProfile.of(2_000_000L, 2_000_000L, 3_000_000L, 64L, 1_000_000L,
      2_000_000L, 1_024L, 2_000_000L)
    val value = inputs(selectedProfile = inputProfile, sourcePayload = payload)
    assertEquals(payload.length, value.sourceProvenance.length)
    assertTrue(value.sourceProvenance.hasSha256(value.sourceProvenanceSha256))
    val callerScope = caller(value)
    val binaryColumn = MySqlAuditColumn("large_payload", Types.LONGVARBINARY, "LONGBLOB", 2_000_000, 0,
      ResultSetMetaData.columnNullable, false)
    val set = MySqlAuditResultSet("RS01_HEADER", Vector(binaryColumn),
      Vector(MySqlAuditRow(Vector(MySqlAuditBinary(payload)))))
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      value.validateRetainedObservation(Vector(set), callerScope, callerScope))

  @Test def provenanceDigestComparisonRetainsImmutableBytesAndRejectsWrongDigest(): Unit =
    val payload = Array.tabulate[Byte](128_000)(index => (index * 29).toByte)
    val original = payload.clone()
    val expected = MessageDigest.getInstance("SHA-256").digest(original)
    val value = inputs(sourcePayload = payload)
    payload(0) = (payload(0) ^ 0x7f).toByte
    assertTrue(value.sourceProvenance.equalsBytes(original))
    assertTrue(value.sourceProvenance.hasSha256(value.sourceProvenanceSha256))
    val wrong = expected.clone()
    wrong(0) = (wrong(0) ^ 0x01).toByte
    assertThrows(classOf[IllegalArgumentException], () => inputs(sourcePayload = Array.tabulate[Byte](128_000)(index => (index * 29).toByte), sourceDigest = wrong))
  @Test def staticCanonicalSeparatesProfilesAndAllSevenMetadataFields(): Unit =
    val sets = MySqlAuditCoverage.StaticResultSetIds.map(id => MySqlAuditResultSet(id, Vector(col("test")), Vector(MySqlAuditRow(Vector(MySqlAuditText("exact"))))))
    val expected = MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format3, sets)
    assertNotEquals(expected, MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format2, sets))
    val first = sets.head; val c = first.columns.head
    for changed <- Vector(c.copy(label = "other"), c.copy(jdbcType = Types.CHAR), c.copy(typeName = "CHAR"),
      c.copy(precision = 254), c.copy(scale = 1), c.copy(nullable = ResultSetMetaData.columnNullable), c.copy(signed = true)) do
      assertNotEquals(expected, MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format3, sets.updated(0, first.copy(columns = Vector(changed)))))
    for changed <- Vector(MySqlAuditNull, MySqlAuditText("exact "), MySqlAuditBinary(Array[Byte](1))) do
      assertNotEquals(expected, MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format3, sets.updated(0, first.copy(rows = Vector(MySqlAuditRow(Vector(changed)))))))
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.Format3,
      sets.updated(0, first.copy(rows = Vector(MySqlAuditRow(Vector(MySqlAuditTimestamp(java.time.LocalDateTime.of(2026, 10, 7, 0, 0)))))))))

  @Test def v3SemanticPrivilegesKeepRawWindowAndExactOriginalBudget(): Unit =
    val budget=CallerPrivilegesV3TestData.root()
    try
      val sets=privileges.map(set=>set.copy(rows=set.rows.map(row=>row.copy(cells=row.cells.map {
        case MySqlAuditText("fp_fixture")=>MySqlAuditText(MySqlRuntimeCallerPrivileges.Schema)
        case other=>other
      }))))
      val source=MySqlAuditBinary(Array[Byte](1,2,3))
      val i=new MySqlFormat3AuditInputs(id,"9.7.2",MySqlRuntimeCallerPrivileges.Schema,"fp_f3_runtime@localhost",
        "fp_f3_runtime@127.0.0.1","NONE",id,id,"ab"*32,"audit@localhost",Vector.empty,sets,
        MySqlAuditBinary(Array.fill(32)(1.toByte)),"FRESH_PLAN",source,
        MySqlAuditBinary(MessageDigest.getInstance("SHA-256").digest(Array[Byte](1,2,3))),budget.profile,
        CallerPrivilegesV3TestData.value,budget)
      val before=MySqlCallerScope(id,17L,MySqlRuntimeCallerPrivileges.Schema,"fp_f3_runtime@localhost","NONE",
        "fp_f3_runtime@127.0.0.1",Some("9.7.2"),CallerPrivilegesV3TestData.raw())
      i.validateCapturedCallerGrants(before.ownGrants)
      i.validateSecurity(before,before,sets)
      val repeated=before.copy(ownGrants=before.ownGrants :+ before.ownGrants.head)
      i.validateCapturedCallerGrants(repeated.ownGrants)
      i.validateSecurity(repeated,repeated,sets)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>i.validateSecurity(before,repeated,sets))
      val legacy=inputs()
      legacy.validateCapturedCallerGrants(legacy.callerGrants)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>legacy.validateCapturedCallerGrants(legacy.callerGrants ++ legacy.callerGrants))
      val merged=before.copy(ownGrants=CallerPrivilegesV3TestData.merged)
      i.validateSecurity(merged,merged,sets)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>i.validateSecurity(before,merged,sets))
      val extra=before.copy(ownGrants=before.ownGrants :+ "GRANT DELETE ON `fp_test_20261007_format3_b2`.`fp_runtime_writer` TO `fp_f3_runtime`@`localhost`")
      assertThrows(classOf[java.sql.SQLDataException],()=>i.validateSecurity(extra,extra,sets))
      assertEquals(0L,budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()
