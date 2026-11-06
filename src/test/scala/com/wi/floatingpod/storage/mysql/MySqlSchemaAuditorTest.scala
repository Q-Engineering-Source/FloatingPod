package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{SQLException, Types}
import java.time.{Duration, LocalDateTime}
import java.util.{HexFormat, UUID}

class MySqlSchemaAuditorTest:
  private val serverUuid = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val bindingUuid = UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604")
  private val provisioningUuid = UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f")
  private val bootUuid = UUID.fromString("d2df04b8-fae6-4bc6-9af7-f3e5b2b7bd26")
  private val schemaName = "fp_test"
  private val effectiveUser = "fp_runtime@127.0.0.1"
  private val auditDefiner = "fp_audit@127.0.0.1"
  private val headerDbTime = LocalDateTime.of(2026, 10, 6, 12, 5, 32)
  private val footerDbTime = headerDbTime.plusSeconds(1)
  private val writerLeaseUntil = LocalDateTime.of(2026, 10, 6, 12, 6, 0)
  private val triggerNames = Vector(
    "trg_floating_pod_state_reject_active_delete",
    "trg_floating_pod_state_reject_active_insert",
    "trg_floating_pod_state_reject_active_null_dimension",
    "trg_floating_pod_state_reject_prepared_unbound_update",
    "trg_floating_pod_state_reject_unbound_delete",
    "trg_floating_pod_state_reject_unclaimed_active_normal_insert",
    "trg_floating_pod_state_reject_unclaimed_active_normal_update",
    "trg_physical_recovery_intent_reject_delete",
    "trg_physical_recovery_intent_validate_commit",
    "trg_physical_recovery_intent_validate_insert",
    "trg_physical_recovery_locator_chunk_reject_unbound_delete",
    "trg_physical_recovery_locator_chunk_reject_unbound_update",
    "trg_physical_recovery_locator_chunk_validate_unbound_insert",
    "trg_physical_recovery_locator_reject_unbound_delete",
    "trg_physical_recovery_locator_validate_unbound_insert",
    "trg_physical_recovery_locator_validate_unbound_update"
  )

  @Test
  def comparesCompleteV4CoverageAndRejectsMissingOrChangedGuardRows(): Unit =
    val baseline = expectedObjects()
    val auditor = auditorFor(baseline)
    val good = observation(baseline.resultSets)

    assertEquals(baseline.canonicalSha256, auditor.compareRequiredObjectManifest(good).actualCanonicalSha256)

    val missing = baseline.resultSets.updated(7, baseline.resultSets(7).copy(rows = baseline.resultSets(7).rows.dropRight(1)))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.compareRequiredObjectManifest(observation(missing)))

    val changedRows = baseline.resultSets(7).rows.updated(0, triggerRow("BEGIN SIGNAL SQLSTATE '45001'; END", triggerNames.head))
    val changed = baseline.resultSets.updated(7, baseline.resultSets(7).copy(rows = changedRows))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.compareRequiredObjectManifest(observation(changed)))

  @Test
  def staticManifestRejectionLocatesChangedCellWithoutExposingItsText(): Unit =
    val baseline = expectedObjects()
    val opaqueBody = "BEGIN SIGNAL SQLSTATE '45001' SET MESSAGE_TEXT='private-static-body'; END"
    val changedRows = baseline.resultSets(7).rows.updated(0, triggerRow(opaqueBody, triggerNames.head))
    val changed = baseline.resultSets.updated(7, baseline.resultSets(7).copy(rows = changedRows))
    val failure = assertThrows(classOf[java.sql.SQLNonTransientConnectionException],
      () => auditorFor(baseline).compareRequiredObjectManifest(observation(changed)))
    assertTrue(failure.getMessage.contains("resultSet=RS09_TRIGGERS"))
    assertTrue(failure.getMessage.contains("row=1"))
    assertTrue(failure.getMessage.contains("field=cell"))
    assertTrue(failure.getMessage.contains("sha256="))
    assertFalse(failure.getMessage.contains(opaqueBody))
    assertFalse(failure.getMessage.contains("private-static-body"))

  @Test
  def staticManifestDifferenceLocatesMetadataAndRetainsExactEquality(): Unit =
    val original = expectedObjects().resultSets
    assertTrue(MySqlSchemaAuditor.firstStaticDifference(original, original).isEmpty)
    val column = original.head.columns.head
    val changed = original.updated(0, original.head.copy(columns =
      original.head.columns.updated(0, column.copy(precision = column.precision + 1))))
    val message = MySqlSchemaAuditor.firstStaticDifference(original, changed).get
    assertTrue(message.contains("resultSet=RS02_TABLES row=0 column=1 field=column.precision"))
    assertTrue(message.contains(s"expected=${column.precision} actual=${column.precision + 1}"))

  @Test
  def staticManifestDifferenceKeepsNullBinaryAndCardinalityDistinct(): Unit =
    val column = MySqlAuditColumn("controlled", Types.VARBINARY, "VARBINARY", 32, 0, 1, false)
    val binary = MySqlAuditBinary(Array[Byte](1, 2, 3))
    val expected = Vector(MySqlAuditResultSet("RS02_TABLES", Vector(column), Vector(MySqlAuditRow(Vector(binary)))))
    val equalClone = expected.updated(0, expected.head.copy(rows = Vector(MySqlAuditRow(Vector(MySqlAuditBinary(binary.bytes))))))
    assertTrue(MySqlSchemaAuditor.firstStaticDifference(expected, equalClone).isEmpty)
    val nullCell = expected.updated(0, expected.head.copy(rows = Vector(MySqlAuditRow(Vector(MySqlAuditNull)))))
    val difference = MySqlSchemaAuditor.firstStaticDifference(expected, nullCell).get
    assertTrue(difference.contains("row=1 column=1 field=cell"))
    assertTrue(difference.contains("expected=BINARY[bytes=3,sha256="))
    assertTrue(difference.contains("actual=SQL_NULL"))
    val fewerRows = expected.updated(0, expected.head.copy(rows = Vector.empty))
    assertTrue(MySqlSchemaAuditor.firstStaticDifference(expected, fewerRows).get.contains("field=row.count expected=1 actual=0"))
    assertArrayEquals(Array[Byte](1, 2, 3), binary.bytes)

  @Test
  def schemaFormatsKeepIndependentCoverageAndCanonicalHashDomains(): Unit =
    val format1 = expectedObjects()
    val format2 = expectedFormat2Objects()

    assertEquals(MySqlSchemaAuditProfile.Format1, format1.profile)
    assertEquals(MySqlSchemaAuditProfile.Format2, format2.profile)
    assertEquals(MySqlAuditCoverage.V4, format1.profile.coverage)
    assertEquals(MySqlAuditCoverage.Format2, format2.profile.coverage)
    assertEquals("FloatingPod-MySQL-Audit-Canonical-v4", format1.profile.hashDomain)
    assertEquals("FloatingPod-MySQL-Audit-Canonical-format2-v1", format2.profile.hashDomain)
    assertFalse(format1.profile.hashDomain == format2.profile.hashDomain)
    assertFalse(format1.canonicalSha256 == format2.canonicalSha256)
    assertEquals(8, format1.resultSets.head.rows.size)
    assertEquals(9, format2.resultSets.head.rows.size)
    assertEquals(16, format1.resultSets(7).rows.size)
    assertEquals(18, format2.resultSets(7).rows.size)
    assertEquals(MySqlAuditCoverage.StaticResultSetIds, format2.resultSets.map(_.id))
    assertEquals("fp_audit_schema_format1", MySqlSchemaAuditProfile.Format1.requiredProcedure)
    assertEquals("fp_audit_schema_format2", MySqlSchemaAuditProfile.Format2.requiredProcedure)
    assertTrue(MySqlSchemaAuditProfile.matchesTarget(
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format1,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format1
    ))
    assertTrue(MySqlSchemaAuditProfile.matchesTarget(
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format2,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format2
    ))
    assertFalse(MySqlSchemaAuditProfile.matchesTarget(
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format1,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format2
    ))
    assertFalse(MySqlSchemaAuditProfile.matchesTarget(
      MySqlSchemaAuditProfile.Format1, MySqlExpectedPhysicalFormat.Format1,
      MySqlSchemaAuditProfile.Format1, MySqlExpectedPhysicalFormat.Format2
    ))

    val format2Tables = format2.resultSets.head
    val tableNameColumn = format2Tables.columns.indexWhere(_.label == "TABLE_NAME")
    assertTrue(tableNameColumn >= 0)
    val bindingBootstrapIntentRows = format2Tables.rows.indices.filter { rowIndex =>
      format2Tables.rows(rowIndex).cells(tableNameColumn) == MySqlAuditText("binding_bootstrap_intent")
    }
    assertEquals(1, bindingBootstrapIntentRows.size)
    val bindingBootstrapIntentRowIndex = bindingBootstrapIntentRows.head
    val bindingBootstrapIntentRow = format2Tables.rows(bindingBootstrapIntentRowIndex)
    val missingB1Table = format2.resultSets.updated(0, format2Tables.copy(rows = format2Tables.rows.updated(
      bindingBootstrapIntentRowIndex,
      bindingBootstrapIntentRow.copy(cells = bindingBootstrapIntentRow.cells.updated(tableNameColumn, MySqlAuditText("not_binding_bootstrap_intent")))
    )))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest.forFormat2(missingB1Table))
    val missingB1Guard = format2.resultSets.updated(7, format2.resultSets(7).copy(rows = format2.resultSets(7).rows.dropRight(1)))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest.forFormat2(missingB1Guard))

  @Test
  def format3ColumnContractRequiresAll368ExactIdentitiesIncludingForestUuidIndex(): Unit =
    val shapes = MySqlFormat3RelationShapes.canonicalOrder
    val tables = simpleSet(
      "RS02_TABLES",
      Vector("TABLE_NAME"),
      shapes.map(shape => Vector[Any](shape.table))
    )
    val columnRows = shapes.flatMap { shape =>
      shape.columns.map(column => Vector[Any](shape.table, column.name))
    }
    val columns = simpleSet("RS03_COLUMNS", Vector("TABLE_NAME", "COLUMN_NAME"), columnRows)
    val triggerRows = (1 to 20).toVector.flatMap { ordinal =>
      val number = if ordinal < 10 then "0" + ordinal else ordinal.toString
      Vector("bi", "bu", "bd").map(suffix => Vector[Any]("fp_f3_" + number + "_" + suffix))
    }
    val triggers = simpleSet("RS09_TRIGGERS", Vector("TRIGGER_NAME"), triggerRows)
    val emptyStaticSets = MySqlAuditCoverage.StaticResultSetIds.map { id =>
      simpleSet(id, Vector("TABLE_NAME"), Vector.empty)
    }
    val exact = emptyStaticSets.updated(0, tables).updated(1, columns).updated(7, triggers)

    assertEquals(20, tables.rows.size)
    assertEquals(368, columns.rows.size)
    assertEquals(60, triggers.rows.size)
    MySqlSchemaAuditor.validateProfileObjectCounts(MySqlSchemaAuditProfile.Format3, exact)

    val omittedIndexDigest = columns.copy(rows = columns.rows.filterNot(_.cells == Vector(
      MySqlAuditText("physical_source_forest"), MySqlAuditText("fp_pod_uuid_index_sha256")
    )))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.validateProfileObjectCounts(
        MySqlSchemaAuditProfile.Format3, exact.updated(1, omittedIndexDigest)))

    val digestRowIndex = columns.rows.indexWhere(_.cells == Vector(
      MySqlAuditText("physical_source_forest"), MySqlAuditText("fp_pod_uuid_index_sha256")
    ))
    val wrongIdentity = columns.copy(rows = columns.rows.updated(
      digestRowIndex,
      columns.rows(digestRowIndex).copy(cells = columns.rows(digestRowIndex).cells.updated(
        1, MySqlAuditText("fp_pod_uuid_index_digest")
      ))
    ))
    assertEquals(368, wrongIdentity.rows.size)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.validateProfileObjectCounts(
        MySqlSchemaAuditProfile.Format3, exact.updated(1, wrongIdentity)))

  @Test
  def format2ComparisonRequiresItsOwnNineTableEighteenGuardBaseline(): Unit =
    val baseline = expectedFormat2Objects()
    val auditor = auditorForFormat2(baseline, MySqlExpectedPhysicalFormat.Format1)
    val good = observation(baseline.resultSets, bindingRows = Vector.empty, profile = MySqlSchemaAuditProfile.Format2)
    val compared = auditor.compareRequiredObjectManifest(good)
    assertEquals(MySqlSchemaAuditProfile.Format2, compared.profile)
    assertEquals(MySqlAuditCoverage.Format2, compared.coverage)
    assertEquals(baseline.canonicalSha256, compared.actualCanonicalSha256)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditorFor(expectedObjects()).compareRequiredObjectManifest(good))

    val changedRows = baseline.resultSets(7).rows.updated(0, triggerRow("BEGIN SIGNAL SQLSTATE '45001'; END", triggerNames.head))
    val changedObjects = baseline.resultSets.updated(7, baseline.resultSets(7).copy(rows = changedRows))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.compareRequiredObjectManifest(
      observation(changedObjects, bindingRows = Vector.empty, profile = MySqlSchemaAuditProfile.Format2)
    ))

    val versionIndex = good.resultSets.head.columns.indexWhere(_.label == "format_version")
    val wrongHeader = good.resultSets.head.copy(rows = Vector(MySqlAuditRow(good.resultSets.head.rows.head.cells.updated(versionIndex, MySqlAuditText("1")))))
    val wrongFooter = good.resultSets.last.copy(rows = Vector(MySqlAuditRow(good.resultSets.last.rows.head.cells.updated(versionIndex, MySqlAuditText("1")))))
    val wrongFormatEnvelope = new UnverifiedMySqlSchemaObservation(
      good.scope, good.before, good.after,
      good.resultSets.updated(0, wrongHeader).updated(13, wrongFooter),
      good.auditStartedAtNanos, good.observedAtNanos, MySqlSchemaAuditProfile.Format2
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.validateObservationEnvelope(wrongFormatEnvelope))
    val unknownHeader = good.resultSets.head.copy(rows = Vector(MySqlAuditRow(good.resultSets.head.rows.head.cells.updated(versionIndex, MySqlAuditText("3")))))
    val unknownFooter = good.resultSets.last.copy(rows = Vector(MySqlAuditRow(good.resultSets.last.rows.head.cells.updated(versionIndex, MySqlAuditText("3")))))
    val unknownFormatEnvelope = new UnverifiedMySqlSchemaObservation(
      good.scope, good.before, good.after,
      good.resultSets.updated(0, unknownHeader).updated(13, unknownFooter),
      good.auditStartedAtNanos, good.observedAtNanos, MySqlSchemaAuditProfile.Format2
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.validateObservationEnvelope(unknownFormatEnvelope))

  @Test
  def format2PhysicalExpectationIsExplicitAndNeverLearnedFromObservedBinding(): Unit =
    val baseline = expectedFormat2Objects()
    val restoredAuditor = auditorForFormat2(baseline, MySqlExpectedPhysicalFormat.Format1)
    val restoredLegacy = observation(
      baseline.resultSets,
      bindingRows = Vector(bindingRow("LEGACY_UNCERTIFIED", 1)),
      profile = MySqlSchemaAuditProfile.Format2
    )
    assertEquals(MySqlLegacyPhysicalBinding(bindingUuid, 1), restoredAuditor.validatePreAcquirePhysicalBinding(restoredLegacy.resultSets(11)))

    val freshAuditor = auditorForFormat2(baseline, MySqlExpectedPhysicalFormat.Format2)
    val unclaimedFresh = observation(baseline.resultSets, bindingRows = Vector.empty, profile = MySqlSchemaAuditProfile.Format2)
    assertEquals(MySqlNoPhysicalBinding, freshAuditor.validatePreAcquirePhysicalBinding(unclaimedFresh.resultSets(11)))
    val certifiedFresh = observation(
      baseline.resultSets,
      bindingRows = Vector(bindingRow("CERTIFIED", 2)),
      writerEpoch = 9L,
      writerBoot = Some(bootUuid),
      profile = MySqlSchemaAuditProfile.Format2
    )
    assertEquals(MySqlCertifiedPhysicalBinding(bindingUuid, 2), freshAuditor.validatePreAcquirePhysicalBinding(certifiedFresh.resultSets(11)))
    assertEquals(9L, freshAuditor.validateAcquiredWriterFence(
      certifiedFresh.resultSets(11), certifiedFresh.resultSets(12), certifiedFresh.resultSets(13), acquiredFence(9L)
    ).epoch)

    val observedFormat2ForRestore = observation(
      baseline.resultSets,
      bindingRows = Vector(bindingRow("CERTIFIED", 2)),
      profile = MySqlSchemaAuditProfile.Format2
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => restoredAuditor.validatePreAcquirePhysicalBinding(observedFormat2ForRestore.resultSets(11)))
    val observedFormat1ForFresh = observation(
      baseline.resultSets,
      bindingRows = Vector(bindingRow("CERTIFIED", 1)),
      profile = MySqlSchemaAuditProfile.Format2
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => freshAuditor.validatePreAcquirePhysicalBinding(observedFormat1ForFresh.resultSets(11)))
    val unknownFormat = observation(
      baseline.resultSets,
      bindingRows = Vector(bindingRow("CERTIFIED", 3)),
      profile = MySqlSchemaAuditProfile.Format2
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => freshAuditor.validatePreAcquirePhysicalBinding(unknownFormat.resultSets(11)))

    val format1 = expectedObjects()
    val format1Auditor = auditorFor(format1)
    val format1WithPhysical2 = observation(format1.resultSets, bindingRows = Vector(bindingRow("CERTIFIED", 2)))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => format1Auditor.validatePreAcquirePhysicalBinding(format1WithPhysical2.resultSets(11)))

  @Test
  def requiresCompleteNonNullAuditselfAndEmptyParameterResultset(): Unit =
    val baseline = expectedObjects()
    val self = baseline.resultSets(8)
    val nullDefinition = self.rows.head.cells.updated(3, MySqlAuditNull)
    val badSelf = baseline.resultSets.updated(8, self.copy(rows = Vector(MySqlAuditRow(nullDefinition))))

    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest(badSelf))

    val missingSqlMode = self.rows.head.cells.updated(6, MySqlAuditNull)
    val badSqlMode = baseline.resultSets.updated(8, self.copy(rows = Vector(MySqlAuditRow(missingSqlMode))))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest(badSqlMode))

    val triggers = baseline.resultSets(7)
    val blankTrigger = triggers.rows.updated(0, triggerRow(" ", triggerNames.head))
    val badTrigger = baseline.resultSets.updated(7, triggers.copy(rows = blankTrigger))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest(badTrigger))

    val withParameter = baseline.resultSets.updated(9, baseline.resultSets(9).copy(rows = Vector(MySqlAuditRow(Vector.fill(10)(MySqlAuditNull)))))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlApprovedObjectManifest(withParameter))

  @Test
  def preservesSqlNullTextAndBinaryIdentityAndClonesBinaryInputs(): Unit =
    val supplied = Array[Byte](0, 1, -1, 3)
    val cell = MySqlAuditBinary(supplied)
    supplied(0) = 99
    val returned = cell.bytes
    assertArrayEquals(Array[Byte](0, 1, -1, 3), returned)
    returned(1) = 88
    assertArrayEquals(Array[Byte](0, 1, -1, 3), cell.bytes)
    assertFalse(MySqlAuditNull == MySqlAuditText(""))
    assertFalse(MySqlAuditText("00") == MySqlAuditBinary(Array[Byte](0)))
    assertFalse(MySqlAuditTimestamp(headerDbTime) == MySqlAuditText("2026-10-06 12:05:32"))

  @Test
  def decodesJdbcTimestampFromLocalDateTimeAndRejectsOtherRepresentations(): Unit =
    assertEquals(MySqlAuditTimestamp(headerDbTime), MySqlSchemaAuditor.decodeTimestamp(Types.TIMESTAMP, headerDbTime, wasNull = false))
    assertEquals(MySqlAuditNull, MySqlSchemaAuditor.decodeTimestamp(Types.TIMESTAMP, null, wasNull = true))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlSchemaAuditor.decodeTimestamp(Types.TIMESTAMP, null, wasNull = false))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlSchemaAuditor.decodeTimestamp(Types.VARCHAR, headerDbTime, wasNull = false))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlSchemaAuditor.decodeTimestamp(Types.TIMESTAMP, "2026-10-06 12:05:32", wasNull = false))

  @Test
  def dispatchesNumericJdbcTypesToExactBoundedStringCells(): Unit =
    val numericTypes = Vector(
      Types.TINYINT,
      Types.SMALLINT,
      Types.INTEGER,
      Types.BIGINT,
      Types.NUMERIC,
      Types.DECIMAL,
      Types.FLOAT,
      Types.REAL,
      Types.DOUBLE
    )
    numericTypes.foreach { jdbcType =>
      assertEquals(MySqlAuditJdbcValueKind.Numeric, MySqlSchemaAuditor.jdbcValueKind(jdbcType))
    }
    assertEquals(MySqlAuditJdbcValueKind.Text, MySqlSchemaAuditor.jdbcValueKind(Types.VARCHAR))
    assertEquals(MySqlAuditJdbcValueKind.Binary, MySqlSchemaAuditor.jdbcValueKind(Types.VARBINARY))
    assertEquals(MySqlAuditJdbcValueKind.Timestamp, MySqlSchemaAuditor.jdbcValueKind(Types.TIMESTAMP))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlSchemaAuditor.jdbcValueKind(Types.OTHER))

    val observedTypeInventory = Vector(
      -6 -> MySqlAuditJdbcValueKind.Numeric,
      -5 -> MySqlAuditJdbcValueKind.Numeric,
      -3 -> MySqlAuditJdbcValueKind.Binary,
      -1 -> MySqlAuditJdbcValueKind.Text,
      0 -> MySqlAuditJdbcValueKind.SqlNull,
      1 -> MySqlAuditJdbcValueKind.Text,
      4 -> MySqlAuditJdbcValueKind.Numeric,
      12 -> MySqlAuditJdbcValueKind.Text,
      93 -> MySqlAuditJdbcValueKind.Timestamp
    )
    observedTypeInventory.foreach { case (jdbcType, expectedKind) =>
      assertEquals(expectedKind, MySqlSchemaAuditor.jdbcValueKind(jdbcType, s"metadata type $jdbcType"))
    }
    val nullTypeContext = "RS04_STATISTICS row 1 column 10 'PACKED' JDBC 0/NULL"
    assertEquals(MySqlAuditNull, MySqlSchemaAuditor.decodeJdbcNullCell(null, true, nullTypeContext))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeJdbcNullCell(null, false, nullTypeContext)
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeJdbcNullCell("unexpected", true, nullTypeContext)
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeJdbcNullCell("unexpected", false, nullTypeContext)
    )
    assertEquals(2, MySqlSchemaAuditor.boundedUtf8ByteLength("é", MySqlSchemaAuditor.MaxCellBytes, "UTF-8 fixture"))
    assertEquals(4, MySqlSchemaAuditor.boundedUtf8ByteLength(new String(Character.toChars(0x1f680)), MySqlSchemaAuditor.MaxCellBytes, "UTF-8 fixture"))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.boundedUtf8ByteLength("é", 1, "UTF-8 fixture")
    )

    val exactDecimal = "9007199254740993.12345678901234567890"
    val budget = new MySqlSchemaAuditor.ReadBudget
    assertEquals(
      MySqlAuditText(exactDecimal),
      MySqlSchemaAuditor.decodeNumericCell(exactDecimal, false, budget, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )
    assertEquals(MySqlAuditNull, MySqlSchemaAuditor.decodeNumericCell(null, true, budget, "RS03 row 1 column 4 'ORDINAL_POSITION' JDBC INTEGER"))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeNumericCell(null, false, budget, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeNumericCell("1", true, budget, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeNumericCell("9".repeat(MySqlSchemaAuditor.MaxCellBytes + 1), false, budget, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeNumericCell(new String(Array(0xd800.toChar)), false, budget, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )

    val totalBound = new MySqlSchemaAuditor.ReadBudget
    totalBound.addBytes(MySqlSchemaAuditor.MaxTotalBytes - 1)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.decodeNumericCell("12", false, totalBound, "RS04 row 1 column 3 'NON_UNIQUE' JDBC DECIMAL")
    )

  @Test
  def jdbcReadFailuresRetainDriverCauseAndIdentifyOnlyTheAuditField(): Unit =
    val context = "RS04_STATISTICS row 1 column 3 'NON_UNIQUE' JDBC DECIMAL"
    val driverFailure = new SQLException("driver detail must remain in the cause")
    val failure = assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlSchemaAuditor.jdbcRead(context, "getString") { throw driverFailure }
    )

    assertSame(driverFailure, failure.getCause)
    assertTrue(failure.getMessage.contains(context))
    assertFalse(failure.getMessage.contains(driverFailure.getMessage))

  @Test
  def readyManifestRejectsBootstrappingAndPermitsUnclaimedObservation(): Unit =
    val baseline = expectedObjects()
    val readyAuditor = auditorFor(baseline)
    val unboundReady = observation(baseline.resultSets, bootstrapState = "READY", bindingRows = Vector.empty)
    val ready = readyAuditor.validateReadyManifestForQualification(
      unboundReady.resultSets.head,
      baseline.canonicalSha256
    )
    assertEquals("NONE", ready.importState)
    assertEquals(MySqlNoPhysicalBinding, readyAuditor.validatePreAcquirePhysicalBinding(unboundReady.resultSets(11)))

    val bootstrapping = observation(baseline.resultSets, bootstrapState = "BOOTSTRAPPING", bindingRows = Vector.empty)
    assertEquals(baseline.canonicalSha256, readyAuditor.compareRequiredObjectManifest(bootstrapping).actualCanonicalSha256)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => readyAuditor.validateReadyManifestForQualification(
      bootstrapping.resultSets.head,
      baseline.canonicalSha256
    ))

    val changedFooter = bootstrapping.resultSets.last
    val footerRow = changedFooter.rows.head
    val backendIndex = changedFooter.columns.indexWhere(_.label == "backend")
    val badFooter = changedFooter.copy(rows = Vector(MySqlAuditRow(footerRow.cells.updated(backendIndex, MySqlAuditText("SQLITE")))))
    val mismatchedTuple = copyObservationWithFooter(
      observation(baseline.resultSets, bootstrapState = "READY", bindingRows = Vector.empty),
      badFooter
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => readyAuditor.validateObservationEnvelope(mismatchedTuple))

  @Test
  def postAcquireFenceValidationRequiresExactCertifiedBindingAndSameBootEpoch(): Unit =
    val baseline = expectedObjects()
    val auditor = auditorFor(baseline)
    val acquired = acquiredFence(9L)

    val unboundAfterAcquire = observation(baseline.resultSets, bootstrapState = "READY", bindingRows = Vector.empty, writerEpoch = 9L, writerBoot = Some(bootUuid))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.validateAcquiredWriterFence(
      unboundAfterAcquire.resultSets(11),
      unboundAfterAcquire.resultSets(12),
      unboundAfterAcquire.resultSets.last,
      acquired
    ))

    val certified = observation(
      baseline.resultSets,
      bootstrapState = "READY",
      bindingRows = Vector(bindingRow("CERTIFIED")),
      writerEpoch = 9L,
      writerBoot = Some(bootUuid)
    )
    val observedFence = auditor.validateAcquiredWriterFence(
      certified.resultSets(11),
      certified.resultSets(12),
      certified.resultSets.last,
      acquired
    )
    assertEquals(9L, observedFence.epoch)

    val wrongEpoch = acquiredFence(10L)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => auditor.validateAcquiredWriterFence(
      certified.resultSets(11),
      certified.resultSets(12),
      certified.resultSets.last,
      wrongEpoch
    ))

  @Test
  def runtimeReceiptRequiresSameLiveAcquiredFenceAndUnexpiredLease(): Unit =
    assertEquals(Duration.ofMinutes(2).toNanos, MySqlSchemaAuditor.boundedLeaseValidityNanos(
      footerDbTime,
      footerDbTime.plusMinutes(5),
      Duration.ofMinutes(2).toNanos
    ))
    val connectionToken = new Object()
    val acquired = acquiredFence(9L, observedAtNanos = 1_000L, leaseUntil = footerDbTime.plusSeconds(120))
    val schemaObservedAtNanos = 5_000L
    val callStartedAtNanos = 6_000L
    val receipt = receiptFor(connectionToken, acquired, schemaObservedAtNanos, callStartedAtNanos, footerDbTime, footerDbTime.plusSeconds(20))

    receipt.assertCurrentForToken(bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid,
      MySqlSchemaAuditProfile.Format1, MySqlExpectedPhysicalFormat.Format1,
      connectionToken, Some(acquired), callStartedAtNanos + 1L)

    val replacedEpoch = acquiredFence(10L, observedAtNanos = 1_000L, leaseUntil = acquired.leaseUntil)
    assertReceiptRejected(receipt, connectionToken, Some(replacedEpoch), callStartedAtNanos + 1L)
    assertReceiptRejected(receipt, connectionToken, None, callStartedAtNanos + 1L)
    assertReceiptRejected(receipt, connectionToken, Some(null), callStartedAtNanos + 1L)
    assertReceiptRejected(receipt, new Object(), Some(acquired), callStartedAtNanos + 1L)
    assertReceiptRejected(receipt, connectionToken, Some(acquired), schemaObservedAtNanos + Duration.ofMinutes(2).toNanos + 1L)

    val expiredFence = acquiredFence(9L, observedAtNanos = 1_000L, leaseUntil = footerDbTime.plusNanos(1))
    assertFalse(expiredFence.isCurrentAt(expiredFence.observedAtNanos + expiredFence.leaseValidityNanos))

    val shortAcquired = acquiredFence(9L, observedAtNanos = 1_000L, leaseUntil = footerDbTime.plusSeconds(3))
    val shortReceiptLease = receiptFor(connectionToken, shortAcquired, schemaObservedAtNanos, callStartedAtNanos,
      footerDbTime, shortAcquired.leaseUntil)
    assertReceiptRejected(shortReceiptLease, connectionToken, Some(shortAcquired), callStartedAtNanos + Duration.ofSeconds(3).toNanos)

  @Test
  def runtimeReceiptCurrentnessPinsSchemaProfileAndPhysicalTarget(): Unit =
    val token = new Object()
    val acquired = acquiredFence(12L, observedAtNanos = 1_000L)
    val profile2RestoreReceipt = receiptFor(
      token,
      acquired,
      5_000L,
      6_000L,
      footerDbTime,
      acquired.leaseUntil,
      MySqlSchemaAuditProfile.Format2,
      MySqlExpectedPhysicalFormat.Format1
    )
    profile2RestoreReceipt.assertCurrentForToken(
      bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format1,
      token, Some(acquired), 6_001L
    )
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => profile2RestoreReceipt.assertCurrentForToken(
      bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid,
      MySqlSchemaAuditProfile.Format1, MySqlExpectedPhysicalFormat.Format1,
      token, Some(acquired), 6_001L
    ))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => profile2RestoreReceipt.assertCurrentForToken(
      bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format2,
      token, Some(acquired), 6_001L
    ))

    val profile2FreshReceipt = receiptFor(
      token,
      acquired,
      5_000L,
      6_000L,
      footerDbTime,
      acquired.leaseUntil,
      MySqlSchemaAuditProfile.Format2,
      MySqlExpectedPhysicalFormat.Format2
    )
    profile2FreshReceipt.assertCurrentForToken(
      bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid,
      MySqlSchemaAuditProfile.Format2, MySqlExpectedPhysicalFormat.Format2,
      token, Some(acquired), 6_001L
    )

  private def auditorFor(objects: MySqlApprovedObjectManifest): MySqlSchemaAuditor =
    val expectation = MySqlSchemaAuditExpectation(
      serverUuid,
      "9.7.2",
      schemaName,
      effectiveUser,
      "NONE",
      provisioningUuid,
      bindingUuid,
      "11" * 32,
      auditDefiner,
      "NONE",
      MySqlExpectedNoImport,
      objects
    )
    new MySqlSchemaAuditor(expectation, Duration.ofMinutes(2))

  private def expectedObjects(): MySqlApprovedObjectManifest =
    val sets = Vector(
      simpleSet("RS02_TABLES", Vector("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_COLLATION", "CREATE_OPTIONS", "TABLE_COMMENT"), Vector.tabulate(8)(index => Vector("fp_test", s"table_$index", "BASE TABLE", "InnoDB", "10", "Dynamic", "utf8mb4_0900_bin", "", ""))),
      simpleSet("RS03_COLUMNS", Vector("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "COLUMN_COMMENT", "GENERATION_EXPRESSION"), Vector(Vector.fill(19)("fixture"))),
      simpleSet("RS04_STATISTICS", Vector("TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"), Vector(Vector.fill(16)("fixture"))),
      simpleSet("RS05_TABLE_CONSTRAINTS", Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"), Vector(Vector.fill(7)("fixture"))),
      simpleSet("RS06_KEY_COLUMN_USAGE", Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"), Vector(Vector.fill(12)("fixture"))),
      simpleSet("RS07_REFERENTIAL_CONSTRAINTS", Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"), Vector(Vector.fill(11)("fixture"))),
      simpleSet("RS08_CHECK_CONSTRAINTS", Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE", "clause_octets", "clause_sha256"), Vector(Vector("def", "fp_test", "ck_fixture", "singleton_fixed1 = 1", "singleton_fixed1 = 1".getBytes(StandardCharsets.UTF_8).length.toString, MySqlAuditBinary(sha256("singleton_fixed1 = 1".getBytes(StandardCharsets.UTF_8)))))),
      triggerSet(triggerNames.map(triggerRow("BEGIN SIGNAL SQLSTATE '45000'; END", _))),
      auditSelfSet(),
      simpleSet("RS11_AUDITSELF_PARAMETERS", Vector("SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME", "DATA_TYPE", "DTD_IDENTIFIER", "CHARACTER_SET_NAME", "COLLATION_NAME"), Vector.empty)
    )
    MySqlApprovedObjectManifest(sets)

  /** 仅供纯模型测试的 fixture，不是独立批准的生产基线。 */
  private def expectedFormat2Objects(): MySqlApprovedObjectManifest =
    val tableNames = Vector(
      "binding_bootstrap_intent",
      "floating_pod_state",
      "fp_runtime_writer",
      "fp_schema_manifest",
      "physical_recovery_binding",
      "physical_recovery_intent",
      "physical_recovery_locator",
      "physical_recovery_locator_chunk",
      "player_entitlement"
    )
    val tables = simpleSet(
      "RS02_TABLES",
      Vector("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_COLLATION", "CREATE_OPTIONS", "TABLE_COMMENT"),
      tableNames.map(name => Vector(schemaName, name, "BASE TABLE", "InnoDB", "10", "Dynamic", "utf8mb4_0900_bin", "", ""))
    )
    val columns = simpleSet(
      "RS03_COLUMNS",
      Vector("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "COLUMN_COMMENT", "GENERATION_EXPRESSION"),
      Vector(Vector.fill(19)("format2-fixture"))
    )
    val statistics = simpleSet(
      "RS04_STATISTICS",
      Vector("TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"),
      Vector(Vector.fill(16)("format2-fixture"))
    )
    val tableConstraints = simpleSet(
      "RS05_TABLE_CONSTRAINTS",
      Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"),
      Vector(Vector.fill(7)("format2-fixture"))
    )
    val keyColumnUsage = simpleSet(
      "RS06_KEY_COLUMN_USAGE",
      Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"),
      Vector(Vector.fill(12)("format2-fixture"))
    )
    val referentialConstraints = simpleSet(
      "RS07_REFERENTIAL_CONSTRAINTS",
      Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"),
      Vector(Vector.fill(11)("format2-fixture"))
    )
    val checkClause = "singleton_fixed1 = 1"
    val checkBytes = checkClause.getBytes(StandardCharsets.UTF_8)
    val checkConstraints = simpleSet(
      "RS08_CHECK_CONSTRAINTS",
      Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE", "clause_octets", "clause_sha256"),
      Vector(Vector("def", schemaName, "ck_format2_fixture", checkClause, checkBytes.length.toString, MySqlAuditBinary(sha256(checkBytes))))
    )
    val format2TriggerNames = (triggerNames ++ Vector(
      "trg_binding_bootstrap_intent_insert",
      "trg_binding_bootstrap_intent_update"
    )).sorted
    val triggers = triggerSet(format2TriggerNames.map { name =>
      if name == "trg_binding_bootstrap_intent_insert" then
        triggerRow("BEGIN SIGNAL SQLSTATE '45000'; END", name, "binding_bootstrap_intent", "INSERT")
      else if name == "trg_binding_bootstrap_intent_update" then
        triggerRow("BEGIN SIGNAL SQLSTATE '45000'; END", name, "binding_bootstrap_intent", "UPDATE")
      else triggerRow("BEGIN SIGNAL SQLSTATE '45000'; END", name)
    })
    val parameters = simpleSet(
      "RS11_AUDITSELF_PARAMETERS",
      Vector("SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME", "DATA_TYPE", "DTD_IDENTIFIER", "CHARACTER_SET_NAME", "COLLATION_NAME"),
      Vector.empty
    )
    MySqlApprovedObjectManifest.forFormat2(Vector(
      tables,
      columns,
      statistics,
      tableConstraints,
      keyColumnUsage,
      referentialConstraints,
      checkConstraints,
      triggers,
      auditSelfSet(MySqlSchemaAuditProfile.Format2.requiredProcedure),
      parameters
    ))

  private def approvedManifest(sets: Vector[MySqlAuditResultSet], profile: MySqlSchemaAuditProfile): MySqlApprovedObjectManifest =
    profile match
      case MySqlSchemaAuditProfile.Format1 => MySqlApprovedObjectManifest(sets)
      case MySqlSchemaAuditProfile.Format2 => MySqlApprovedObjectManifest.forFormat2(sets)

  private def auditorForFormat2(
      objects: MySqlApprovedObjectManifest,
      expectedPhysicalFormat: MySqlExpectedPhysicalFormat
  ): MySqlSchemaAuditor =
    val expectation = MySqlSchemaAuditExpectation.forFormat2(
      serverUuid,
      "9.7.2",
      schemaName,
      effectiveUser,
      "NONE",
      provisioningUuid,
      bindingUuid,
      "11" * 32,
      auditDefiner,
      "NONE",
      MySqlExpectedNoImport,
      objects,
      expectedPhysicalFormat
    )
    new MySqlSchemaAuditor(expectation, Duration.ofMinutes(2))

  private def simpleSet(id: String, labels: Vector[String], values: Vector[Vector[Any]]): MySqlAuditResultSet =
    val columns = labels.map(column)
    val rows = values.map(values => MySqlAuditRow(values.map(cell).toVector))
    MySqlAuditResultSet(id, columns, rows)

  private def triggerSet(rows: Vector[MySqlAuditRow]): MySqlAuditResultSet =
    val labels = Vector("TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "EVENT_MANIPULATION", "ACTION_TIMING", "ACTION_ORDER", "ACTION_ORIENTATION", "ACTION_STATEMENT", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "statement_octets", "statement_sha256")
    MySqlAuditResultSet("RS09_TRIGGERS", labels.map(column), rows)

  private def triggerRow(
      statement: String,
      name: String,
      tableName: String = "floating_pod_state",
      eventManipulation: String = "INSERT"
  ): MySqlAuditRow =
    val body = statement.getBytes(StandardCharsets.UTF_8)
    MySqlAuditRow(Vector(
      MySqlAuditText(schemaName), MySqlAuditText(name), MySqlAuditText(schemaName), MySqlAuditText(tableName),
      MySqlAuditText(eventManipulation), MySqlAuditText("BEFORE"), MySqlAuditText("1"), MySqlAuditText("ROW"), MySqlAuditText(statement),
      MySqlAuditText("STRICT_ALL_TABLES"), MySqlAuditText(auditDefiner), MySqlAuditText("utf8mb4"), MySqlAuditText("utf8mb4_0900_bin"),
      MySqlAuditText("utf8mb4_0900_bin"), MySqlAuditText(body.length.toString), MySqlAuditBinary(sha256(body))
    ))

  private def auditSelfSet(procedureName: String = "fp_audit_schema_format1"): MySqlAuditResultSet =
    val labels = Vector("ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "ROUTINE_DEFINITION", "SECURITY_TYPE", "SQL_DATA_ACCESS", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "IS_DETERMINISTIC", "DATA_TYPE", "DTD_IDENTIFIER", "ROUTINE_COMMENT", "definition_octets", "definition_sha256")
    val definition = "BEGIN SELECT 1; END"
    val bytes = definition.getBytes(StandardCharsets.UTF_8)
    simpleSet("RS10_AUDITSELF", labels, Vector(Vector(
      schemaName, procedureName, "PROCEDURE", definition, "DEFINER", "READS SQL DATA", "STRICT_ALL_TABLES", auditDefiner,
      "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin", "NO", "", "", "fixed schema audit", bytes.length.toString,
      MySqlAuditBinary(sha256(bytes))
    )))

  private def observation(
      staticSets: Vector[MySqlAuditResultSet],
      bootstrapState: String = "READY",
      bindingRows: Vector[MySqlAuditRow] = Vector(bindingRow("CERTIFIED")),
      writerEpoch: Long = 0L,
      writerBoot: Option[UUID] = None,
      profile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format1
  ): UnverifiedMySqlSchemaObservation =
    val connectionToken = new Object()
    val identity = new MySqlConnectionIdentity(serverUuid, "9.7.2", schemaName, effectiveUser, "InnoDB", 1, 1L, "READ-COMMITTED", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val scope = new MySqlAuditBootScope(connectionToken, identity, None, Thread.currentThread(), bootUuid, 4L, 7L)
    val before = MySqlCallerScope(serverUuid, 101L, schemaName, effectiveUser, "NONE", "fp_runtime@127.0.0.1")
    val header = headerSet(staticSets, bootstrapState, profile = profile)
    val footer = headerSet(staticSets, bootstrapState, footerDbTime, "RS14_FOOTER", profile)
    val binding = MySqlAuditResultSet("RS12_BINDING", Vector("singleton_fixed1", "binding_uuid", "cutover_state", "format_version").map(column), bindingRows)
    val writer = MySqlAuditResultSet(
      "RS13_WRITER",
      Vector("singleton_fixed1", "binding_uuid", "writer_epoch", "owner_boot_uuid", "lease_until", "last_commit_operation_uuid", "last_commit_fingerprint").map(column),
      Vector(MySqlAuditRow(Vector(MySqlAuditText("1"), MySqlAuditBinary(uuidBytes(bindingUuid)), MySqlAuditText(writerEpoch.toString), writerBoot.map(id => MySqlAuditBinary(uuidBytes(id))).getOrElse(MySqlAuditNull), MySqlAuditTimestamp(writerLeaseUntil), MySqlAuditNull, MySqlAuditNull)))
    )
    val sets = Vector(header) ++ staticSets ++ Vector(binding, writer, footer)
    val now = System.nanoTime()
    new UnverifiedMySqlSchemaObservation(scope, before, before, sets, now, now, profile)

  private def bindingRow(state: String, formatVersion: Int = 1): MySqlAuditRow =
    MySqlAuditRow(Vector(MySqlAuditText("1"), MySqlAuditBinary(uuidBytes(bindingUuid)), MySqlAuditText(state), MySqlAuditText(formatVersion.toString)))

  private def headerSet(
      objects: Vector[MySqlAuditResultSet],
      state: String,
      observedAt: LocalDateTime = headerDbTime,
      id: String = "RS01_HEADER",
      profile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format1
  ): MySqlAuditResultSet =
    val labels = Vector("server_uuid", "server_version", "connection_id", "audit_definer", "connected_caller", "effective_roles", "routine_default_schema", "observed_db_time", "audited_schema", "singleton_fixed1", "backend", "format_version", "ddl_sha256", "provisioning_uuid", "bootstrap_state", "expected_server_uuid", "expected_binding_uuid", "import_state", "import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256", "guard_manifest_sha256")
    val expected = approvedManifest(objects, profile)
    val cells = Vector[MySqlAuditCell](
      MySqlAuditText(serverUuid.toString), MySqlAuditText("9.7.2"), MySqlAuditText("101"), MySqlAuditText(auditDefiner), MySqlAuditText("fp_runtime@127.0.0.1"),
      MySqlAuditText("NONE"), MySqlAuditText(schemaName), MySqlAuditTimestamp(observedAt), MySqlAuditText(schemaName), MySqlAuditText("1"), MySqlAuditText("MYSQL"), MySqlAuditText(profile.schemaFormat.toString),
      MySqlAuditBinary(HexFormat.of().parseHex("11" * 32)), MySqlAuditBinary(uuidBytes(provisioningUuid)), MySqlAuditText(state), MySqlAuditBinary(uuidBytes(serverUuid)),
      MySqlAuditBinary(uuidBytes(bindingUuid)), MySqlAuditText("NONE"), MySqlAuditNull, MySqlAuditNull, MySqlAuditNull, MySqlAuditNull,
      MySqlAuditBinary(HexFormat.of().parseHex(expected.canonicalSha256))
    )
    MySqlAuditResultSet(id, labels.map(column), Vector(MySqlAuditRow(cells)))

  private def column(label: String): MySqlAuditColumn =
    if label == "observed_db_time" || label == "lease_until" then MySqlAuditColumn(label, Types.TIMESTAMP, "DATETIME", 26, 6, 1, false)
    else MySqlAuditColumn(label, Types.VARCHAR, "VARCHAR", 255, 0, 1, false)

  private def cell(value: Any): MySqlAuditCell = value match
    case bytes: MySqlAuditBinary => bytes
    case null => MySqlAuditNull
    case text: String => MySqlAuditText(text)
    case number: Int => MySqlAuditText(number.toString)
    case number: Long => MySqlAuditText(number.toString)
    case timestamp: LocalDateTime => MySqlAuditTimestamp(timestamp)
    case other => throw new IllegalArgumentException(s"unsupported test fixture cell ${other.getClass.getSimpleName}")

  private def uuidBytes(value: UUID): Array[Byte] =
    ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()

  private def sha256(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)

  private def copyObservationWithFooter(
      observation: UnverifiedMySqlSchemaObservation,
      footer: MySqlAuditResultSet
  ): UnverifiedMySqlSchemaObservation =
    new UnverifiedMySqlSchemaObservation(
      observation.scope,
      observation.before,
      observation.after,
      observation.resultSets.updated(13, footer),
      observation.auditStartedAtNanos,
      observation.observedAtNanos,
      observation.profile
    )

  private def acquiredFence(
      epoch: Long,
      observedAtNanos: Long = 1_000L,
      leaseUntil: LocalDateTime = writerLeaseUntil
  ): MySqlWriterFenceExpectation =
    MySqlWriterFenceExpectation.fromAcquiredFence(bindingUuid, bootUuid, epoch, leaseUntil, headerDbTime, observedAtNanos)

  private def receiptFor(
      connectionToken: AnyRef,
      acquiredFence: MySqlWriterFenceExpectation,
      schemaObservedAtNanos: Long,
      callStartedAtNanos: Long,
      footerTime: LocalDateTime,
      leaseUntil: LocalDateTime,
      profile: MySqlSchemaAuditProfile = MySqlSchemaAuditProfile.Format1,
      expectedPhysicalFormat: MySqlExpectedPhysicalFormat = MySqlExpectedPhysicalFormat.Format1
  ): RuntimeSchemaAuditReceipt =
    new RuntimeSchemaAuditReceipt(
      profile,
      expectedPhysicalFormat,
      connectionToken,
      bootUuid,
      4L,
      7L,
      serverUuid,
      schemaName,
      bindingUuid,
      acquiredFence.epoch,
      provisioningUuid,
      "NONE",
      None,
      None,
      None,
      None,
      "11" * 32,
      "22" * 32,
      acquiredFence,
      callStartedAtNanos,
      MySqlSchemaAuditor.boundedLeaseValidityNanos(footerTime, leaseUntil, Duration.ofMinutes(2).toNanos),
      schemaObservedAtNanos,
      Duration.ofMinutes(2).toNanos
    )

  private def assertReceiptRejected(
      receipt: RuntimeSchemaAuditReceipt,
      connectionToken: AnyRef,
      currentFence: Option[MySqlWriterFenceExpectation],
      nowNanos: Long
  ): Unit =
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => receipt.assertCurrentForToken(
      bootUuid, 4L, 7L, serverUuid, schemaName, bindingUuid, receipt.profile, receipt.expectedPhysicalFormat,
      connectionToken, currentFence, nowNanos
    ))

  @Test def v3EquivalentPrivilegesDoNotEraseRawCallerWindowDifference(): Unit =
    val root=CallerPrivilegesV3TestData.root()
    try
      val expected=CallerPrivilegesV3TestData.value
      val before=MySqlCallerScope(serverUuid,101L,MySqlRuntimeCallerPrivileges.Schema,"fp_f3_runtime@localhost",
        "NONE","fp_f3_runtime@127.0.0.1",Some("9.7.2"),CallerPrivilegesV3TestData.raw())
      val after=before.copy(ownGrants=CallerPrivilegesV3TestData.merged)
      MySqlRuntimeCallerPrivileges.requireObservation(expected,before.ownGrants,root)
      MySqlRuntimeCallerPrivileges.requireObservation(expected,after.ownGrants,root)
      assertFalse(before == after)
      assertEquals(before, before.copy())
      assertEquals(0L,root.accountedMemoryBytes)
    finally root.closeRootReadBudget()
