package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import java.sql.{SQLDataException, Types}
import com.mysql.cj.MysqlType
import java.io.EOFException
import java.time.LocalDateTime
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.SQLNonTransientConnectionException
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 验证 full24 DATA 输入边界在扫描之前计费，并在拒绝时归还临时工作区。 */
final class MySqlScriptSchemaSnapshotTest:
  private val Schema = MySqlScriptSchemaProfile.Schema
  private val Connection = MySqlFormat3ConnectionObservation(
    UUID.fromString("00000000-0000-0000-0000-000000000024"), "9.7.2", Schema, "data@test", "InnoDB",
    1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
  private val Scope = MySqlFormat3ObservationScope(
    UUID.fromString("00000000-0000-0000-0000-000000000025"), 1L, 1L)

  private def profile(retainedBytes: Long): SourceLimitProfile =
    SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
      retainedBytes, 4_000_000L, 2_000_000L)

  private def inputs(profile: SourceLimitProfile, evidenceBytes: Long): MySqlScriptSchemaInputs =
    val blobLimit = Math.min(100_000L, profile.maxRetainedBytes())
    val snapshotLimit = Math.min(1_000_000L, profile.maxRetainedBytes())
    MySqlScriptSchemaInputs.create(profile,
      MySqlScriptSchemaProfile.Budget(evidenceBytes, blobLimit, blobLimit, blobLimit,
        snapshotLimit, blobLimit, 100L, profile.maxRetainedBytes(), profile.maxSqlPacketBytes()))

  private def root(inputs: MySqlScriptSchemaInputs): MySqlFormat3ReadBudget =
    inputs.newAllocationRoot(inputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(inputs.sourceLimitProfile.maxRetainedBytes()))

  private def validRow(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    shape.columns.map { column =>
      column.kind match
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(0L)
        case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(0.0d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 23, 0, 0))
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("")
        case MySqlFormat3SqlKind.Binary =>
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](column.maximumLength.getOrElse(0))))
    }

  private def evidenceRows(bytes: Array[Byte]): Vector[MySqlFormat3RelationSnapshot] =
    MySqlScriptSchemaInputs.canonicalOrder.map { shape =>
      if shape.table == "fp_schema_manifest" then
        val index = shape.columns.indexWhere(_.name == "source_provenance_bytes")
        val row = validRow(shape).updated(index, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes)))
        MySqlFormat3RelationSnapshot(shape.table, Vector(row))
      else MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
    }

  private def completeRows: Vector[MySqlFormat3RelationSnapshot] =
    MySqlScriptSchemaInputs.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, Vector(validRow(shape))))

  private def full26Inputs(sourceProfile: SourceLimitProfile): MySqlScriptSchemaInputs =
    val publication = inputs(sourceProfile, evidenceBytes = 100_000L)
    MySqlScriptSchemaInputs.createDeploymentReceiptFull26(Schema,
      HexFormat.of().formatHex(Array.fill[Byte](32)(0x5a.toByte)), sourceProfile, publication.budget,
      MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 100_000L, diagnosticsBytes = 100_000L))

  private def full26Rows(inputs: MySqlScriptSchemaInputs): Vector[MySqlFormat3RelationSnapshot] =
    MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(inputs)

  private def replaceDeploymentQuota(rows: Vector[MySqlFormat3RelationSnapshot], replacement: Vector[Vector[MySqlFormat3Cell]]) =
    val table = MySqlDeploymentReceiptSchemaDefinition.QuotaTable
    rows.map(relation => if relation.table == table then relation.copy(rows = replacement) else relation)

  private def mutateDeploymentQuota(rows: Vector[MySqlFormat3RelationSnapshot], column: String,
      cell: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val shape = MySqlDeploymentReceiptRelationShapes.quota
    val index = shape.columns.indexWhere(_.name == column)
    val quota = rows.find(_.table == shape.table).get.rows.head.updated(index, cell)
    replaceDeploymentQuota(rows, Vector(quota))

  private def inspectDeploymentOperationRow(
      row: Vector[MySqlFormat3Cell],
      allocation: MySqlFormat3ReadBudget,
      expectedPhase: OperationPhase,
      expectedRevision: Long,
      expectDiagnostic: Boolean
  ): UUID =
    val child = allocation.newCanonicalDataScope()
    var primary: Throwable = null
    try inspectDeploymentOperationRowInScope(row, child, expectedPhase, expectedRevision, expectDiagnostic)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => child.closeScopedReadBudget()))

  private def inspectDeploymentOperationRowInScope(
      row: Vector[MySqlFormat3Cell],
      budget: MySqlFormat3ReadBudget,
      expectedPhase: OperationPhase,
      expectedRevision: Long,
      expectDiagnostic: Boolean
  ): UUID =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    val values = row.zip(shape.columns).map { (cell, column) =>
      cell match
        case MySqlFormat3Cell.SqlNull => MySqlDeploymentOperationRows.ColumnValue.SqlNull
        case MySqlFormat3Cell.SignedInteger(value) => MySqlDeploymentOperationRows.ColumnValue.Integer(value)
        case MySqlFormat3Cell.Binary(value) => MySqlDeploymentOperationRows.ColumnValue.Binary(
          value.copyBytes(budget, s"test ${shape.table}.${column.name} decoder input copy"))
        case _ => throw new AssertionError(s"unexpected operation cell ${shape.table}.${column.name}")
    }
    def uuid(column: String): UUID =
      val index = MySqlDeploymentOperationRows.columnNames.indexOf(column)
      values(index) match
        case MySqlDeploymentOperationRows.ColumnValue.Binary(bytes) =>
          val buffer = ByteBuffer.wrap(bytes)
          new UUID(buffer.getLong(), buffer.getLong())
        case _ => throw new AssertionError(s"operation $column must be binary")
    val actor = uuid("actor_uuid")
    val operation = uuid("operation_uuid")
    val decoded = MySqlDeploymentOperationRows.decode(
      MySqlDeploymentOperationRows.ColumnValues(values), actor, operation, budget)
    assertEquals(expectedRevision, decoded.rowRevision, "row revision must match its complete deployment step chain")
    assertEquals(expectedPhase, decoded.receipt.phase())
    val diagnostic = decoded.copyBinary("diagnostic_bytes", budget)
    assertEquals(expectDiagnostic, diagnostic.isDefined)
    if expectDiagnostic then
      assertArrayEquals(Array[Byte](7, 8, 9), diagnostic.get)
      assertTrue(decoded.copyBinary("diagnostic_sha256", budget).isDefined)
    actor

  private def canonicalBytes(payload: MySqlScriptSchemaCanonicalPayload): Array[Byte] =
    Array.tabulate[Byte](payload.byteLength)(payload.bytes.byteAt)

  private def assertColdDecodeRejected(bytes: Array[Byte], selectedInputs: MySqlScriptSchemaInputs,
      allocation: MySqlFormat3ReadBudget, expectedFailureClass: Class[_ <: Throwable], expectedMessage: String): Unit =
    var payload: MySqlScriptSchemaCanonicalPayload = null
    try
      payload = MySqlScriptSchemaSnapshotCodec.capture(bytes, selectedInputs, allocation)
      val retainedBeforeDecode = allocation.accountedMemoryBytes
      var failure: Throwable = null
      try MySqlScriptSchemaSnapshotCodec.decode(payload, selectedInputs, allocation)
      catch case caught: Exception => failure = caught
      assertNotNull(failure, "malformed canonical DATA must fail at the decoder boundary")
      assertTrue(expectedFailureClass.isInstance(failure), s"unexpected failure type: $failure")
      assertTrue(failure.getMessage.contains(expectedMessage), failure.getMessage)
      assertTrue(failure.getStackTrace.nonEmpty)
      assertFalse(payload.storageBudget.isClosed, "failed decode must leave the captured input payload alive")
      assertEquals(retainedBeforeDecode, allocation.accountedMemoryBytes,
        "failed cold decode must release only its temporary typed image and comparison scope")
    finally
      if payload != null then payload.storageBudget.closeScopedReadBudget()

  @Test def everyFrozenLargeValueColumnHasAnExplicitQuotaMapping(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val columns = MySqlScriptSchemaInputs.canonicalOrder.flatMap { shape =>
      shape.columns.collect {
        case column if column.sqlDeclaration.startsWith("MEDIUMBLOB ") ||
            column.sqlDeclaration.startsWith("LONGBLOB ") ||
            column.sqlDeclaration.startsWith("LONGTEXT ") => (shape.table, column)
      }
    }
    assertTrue(columns.nonEmpty)
    val missing = Vector.newBuilder[String]
    columns.foreach { (table, column) =>
      try
        if selectedInputs.maximumPayloadBytes(table, column).forall(_ <= 0L) then missing += s"$table.${column.name}"
      catch
        case _: SQLDataException => missing += s"$table.${column.name}"
    }
    val missingColumns = missing.result()
    assertTrue(missingColumns.isEmpty, s"large-value columns lack explicit quotas: ${missingColumns.mkString(", ")}")

  @Test def healthyFull24SnapshotSurvivesStrictCanonicalRoundtrip(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val allocation = root(selectedInputs)
    var payload: MySqlScriptSchemaCanonicalPayload = null
    var decoded: MySqlScriptSchemaSnapshot = null
    try
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, completeRows, allocation)
      assertEquals(24, snapshot.relations.size)
      assertEquals(432, MySqlScriptSchemaInputs.canonicalOrder.map(_.columns.size).sum)
      assertEquals(24L, snapshot.rowCount)

      payload = MySqlScriptSchemaSnapshotCodec.encode(snapshot, allocation)
      assertTrue(payload.byteLength > 0)
      decoded = MySqlScriptSchemaSnapshotCodec.decode(payload, selectedInputs, allocation)

      assertTrue(snapshot.exactlyEquals(decoded))
      assertEquals(snapshot.canonicalSha256Hex, decoded.canonicalSha256Hex)
    finally
      if decoded != null then decoded.allocationBudget.closeScopedReadBudget()
      if payload != null then payload.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def healthyDeploymentReceiptFull26PreservesUnknownRowsThroughColdCanonicalRoundtrip(): Unit =
    val selectedInputs = full26Inputs(profile(2_000_000L))
    val rows = full26Rows(selectedInputs)
    val allocation = root(selectedInputs)
    var payload: MySqlScriptSchemaCanonicalPayload = null
    var reencoded: MySqlScriptSchemaCanonicalPayload = null
    var decoded: MySqlScriptSchemaSnapshot = null
    try
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, rows, allocation)
      assertEquals(MySqlDeploymentReceiptSchemaDefinition.SnapshotDomain, selectedInputs.snapshotDomain)
      assertEquals(5, selectedInputs.schemaFormat)
      assertEquals(26, snapshot.relations.size)
      assertEquals(467, selectedInputs.canonicalOrder.flatMap(_.columns).size)
      assertEquals(33L, snapshot.rowCount,
        "all 24 existing relation rows, two operations, seven recovery steps and the quota row must survive")
      assertEquals(2, snapshot.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.size)
      assertEquals(7, snapshot.relation("fp_recovery_operation").rows.size,
        "both actors' REGISTER/START/FINALIZE chains and the UNKNOWN reconciliation step must survive cold DATA")
      assertEquals(1, snapshot.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.size)
      val operationRows = snapshot.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows
      val terminalActor = inspectDeploymentOperationRow(operationRows(0), allocation, OperationPhase.APPLIED,
        expectedRevision = 2L, expectDiagnostic = false)
      val unknownActor = inspectDeploymentOperationRow(operationRows(1), allocation, OperationPhase.UNKNOWN,
        expectedRevision = 3L, expectDiagnostic = true)
      assertNotEquals(terminalActor, unknownActor, "historical terminal and UNKNOWN rows belong to different actors")
      assertEquals(2L, snapshot.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(2)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertEquals(11L, snapshot.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(3)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value,
        "quota_revision remains independent from operation row count")

      payload = MySqlScriptSchemaSnapshotCodec.encode(snapshot, allocation)
      decoded = MySqlScriptSchemaSnapshotCodec.decode(payload, selectedInputs, allocation)
      reencoded = MySqlScriptSchemaSnapshotCodec.encode(decoded, allocation)
      assertTrue(snapshot.exactlyEquals(decoded))
      assertEquals(payload.byteLength, reencoded.byteLength)
      var offset = 0
      while offset < payload.byteLength do
        assertEquals(payload.bytes.byteAt(offset).toInt, reencoded.bytes.byteAt(offset).toInt,
          s"canonical full26 byte $offset differs after cold decode")
        offset += 1
    finally
      if decoded != null then decoded.allocationBudget.closeScopedReadBudget()
      if reencoded != null then reencoded.storageBudget.closeScopedReadBudget()
      if payload != null then payload.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def full26AdmissionSeparatesIndividualPayloadPacketAndAggregateRetainedLimits(): Unit =
    val sourceProfile = profile(4_000_000L)
    val publication = inputs(sourceProfile, evidenceBytes = 1_000_000L).budget
    def selected(retained: Long): MySqlScriptSchemaInputs =
      MySqlScriptSchemaInputs.createDeploymentReceiptFull26(Schema,
        MySqlScriptSchemaDataTestFixture.Full26DdlSha256, sourceProfile,
        publication.copy(retainedBytes = retained),
        MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 100_000L, diagnosticsBytes = 100_000L))
    val selectedInputs = selected(2_000_000L)
    val original = full26Rows(selectedInputs)
    val evidence = Array.fill[Byte](600_000)(0x31.toByte)
    val snapshotBytes = Array.fill[Byte](600_000)(0x42.toByte)
    def payload(rows: Vector[MySqlFormat3RelationSnapshot], table: String, column: String,
        digestColumn: String, bytes: Array[Byte]): Vector[MySqlFormat3RelationSnapshot] =
      val shape = selectedInputs.forTable(table)
      rows.map { relation =>
        if relation.table != table then relation
        else relation.copy(rows = relation.rows.map { row =>
          row.updated(shape.columns.indexWhere(_.name == column), MySqlScriptSchemaDataTestFixture.binary(bytes))
            .updated(shape.columns.indexWhere(_.name == digestColumn),
              MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.digest(bytes)))
        })
      }
    val rows = payload(payload(original, "fp_schema_manifest", "source_provenance_bytes",
      "source_provenance_sha256", evidence), "fp_script_catalog", "snapshot_bytes", "snapshot_sha256", snapshotBytes)
    assertTrue(evidence.length < selectedInputs.budget.snapshotBytes)
    assertTrue(snapshotBytes.length < selectedInputs.budget.snapshotBytes)
    val allocation = root(selectedInputs)
    try
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, rows, allocation)
      assertEquals(26, snapshot.relations.size)
      assertEquals(rows, snapshot.relations)
      assertEquals(original.find(_.table == "fp_recovery_operation").get,
        snapshot.relation("fp_recovery_operation"), "original terminal/UNKNOWN history remains fully validated and unchanged")
      assertTrue(snapshot.retainedValueBytes > selectedInputs.budget.snapshotBytes,
        "complete retained DATA may contain several individually bounded payloads")
      assertTrue(snapshot.retainedValueBytes < selectedInputs.budget.retainedBytes)
      assertSame(allocation, snapshot.allocationBudget)
      assertFalse(allocation.isClosed)
      assertTrue(allocation.successfulReserveHighWaterBytes >= 32_768L,
        "real ledger must admit the complete validation workspace")
    finally allocation.closeRootReadBudget()

    def rejected(boundaryInputs: MySqlScriptSchemaInputs, actualRows: Vector[MySqlFormat3RelationSnapshot],
        observedPacket: Long, expectedMessage: String): Unit =
      val actualRoot = root(boundaryInputs)
      val baseline = actualRoot.accountedMemoryBytes
      try
        val failure = assertThrows(classOf[SQLDataException], () =>
          MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
            observedPacket, boundaryInputs, actualRows, actualRoot))
        assertTrue(failure.getMessage.contains(expectedMessage), failure.getMessage)
        assertTrue(failure.getStackTrace.nonEmpty)
        assertEquals(baseline, actualRoot.accountedMemoryBytes,
          "failed admission must release its validation workspace and decoder scopes")
      finally actualRoot.closeRootReadBudget()

    val oversizedBlob = payload(rows, "fp_script_catalog", "snapshot_bytes", "snapshot_sha256",
      new Array[Byte](Math.toIntExact(selectedInputs.budget.snapshotBytes + 1L)))
    rejected(selectedInputs, oversizedBlob, selectedInputs.budget.packetBytes,
      "fp_script_catalog.snapshot_bytes exceeds explicit binary byte budget")
    rejected(selectedInputs, rows, 500_000L, "packet budget")
    rejected(selected(1_100_000L), rows, selectedInputs.budget.packetBytes,
      "retained DATA budget rejected: retained budget exceeded")

  @Test def deploymentReceiptQuotaRequiresOneFpdcRowAndActualOperationCount(): Unit =
    val selectedInputs = full26Inputs(profile(2_000_000L))
    val valid = full26Rows(selectedInputs)
    val allocation = root(selectedInputs)
    val baseline = allocation.accountedMemoryBytes
    try
      val cases = Vector(
        "used_records does not match operation rows" -> mutateDeploymentQuota(valid, "used_records", MySqlFormat3Cell.SignedInteger(1L)),
        "used_records exceeds max_records" -> mutateDeploymentQuota(valid, "max_records", MySqlFormat3Cell.SignedInteger(1L)),
        "max_records must be positive" -> mutateDeploymentQuota(valid, "max_records", MySqlFormat3Cell.SignedInteger(0L)),
        "used_records must be nonnegative" -> mutateDeploymentQuota(valid, "used_records", MySqlFormat3Cell.SignedInteger(-1L)),
        "quota_revision must be nonnegative" -> mutateDeploymentQuota(valid, "quota_revision", MySqlFormat3Cell.SignedInteger(-1L)),
        "namespace is not FPDC" -> mutateDeploymentQuota(valid, "namespace",
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array[Byte](1, 2, 3, 4)))),
        "quota row is missing" -> replaceDeploymentQuota(valid, Vector.empty),
        "quota row is duplicated" -> replaceDeploymentQuota(valid,
          Vector(valid.last.rows.head, valid.last.rows.head.updated(3, MySqlFormat3Cell.SignedInteger(12L))))
      )
      cases.foreach { (label, malformed) =>
        val failure = assertThrows(classOf[SQLDataException], () => {
          selectedInputs.validateRelations(malformed, selectedInputs.budget.packetBytes, allocation)
          ()
        }, label)
        assertTrue(failure.getStackTrace.nonEmpty, label)
        assertEquals(baseline, allocation.accountedMemoryBytes, s"$label must not leave validation or decoder debt")
      }
    finally allocation.closeRootReadBudget()

  @Test def deploymentOperationValidationUsesActual31ColumnDecoderAndClosesTemporaryLedgerScopes(): Unit =
    val selectedInputs = full26Inputs(profile(2_000_000L))
    val rows = full26Rows(selectedInputs)
    val operationShape = MySqlDeploymentReceiptRelationShapes.operation
    assertEquals(MySqlDeploymentOperationRows.columnNames, operationShape.columns.map(_.name))
    val allocation = root(selectedInputs)
    val baseline = allocation.accountedMemoryBytes
    try
      Vector.fill(3)(()).foreach { _ =>
        assertTrue(selectedInputs.validateRelations(rows, selectedInputs.budget.packetBytes, allocation) >= 0L)
        assertEquals(baseline, allocation.accountedMemoryBytes,
          "a successful validation discards its decoded Row before closing the temporary child")
      }
      assertTrue(allocation.successfulReserveHighWaterBytes > MySqlScriptSchemaInputs.ValidationWorkspace,
        "the actual decoder row clones and child ownership must count against the original shared ledger")

      val stepHashIndex = operationShape.columns.indexWhere(_.name == "step_evidence_sha256")
      val corruptOperations = rows.map { relation =>
        if relation.table == operationShape.table then
          val corrupted = relation.rows.updated(1, relation.rows(1).updated(stepHashIndex,
            MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](32)))))
          relation.copy(rows = corrupted)
        else relation
      }
      val failure = assertThrows(classOf[SQLDataException], () => selectedInputs.validateRelations(
        corruptOperations, selectedInputs.budget.packetBytes, allocation))
      assertInstanceOf(classOf[MySqlDeploymentOperationRows.DecodeFailure], failure.getCause)
      assertTrue(failure.getStackTrace.nonEmpty)
      assertEquals(baseline, allocation.accountedMemoryBytes,
        "a decoder rejection must close its temporary child and preserve the original failure cause")
    finally allocation.closeRootReadBudget()

  @Test def full26DataCannotEnterLegacyFull24B1OrSchema4ImageOwners(): Unit =
    val sourceProfile = profile(2_000_000L)
    val full24 = inputs(sourceProfile, evidenceBytes = 100_000L)
    val full26 = full26Inputs(sourceProfile)
    val full24Root = root(full24)
    val full26Root = root(full26)
    try
      full24Root.requireScriptPublication1Inputs(full24)
      assertThrows(classOf[SQLNonTransientConnectionException], () =>
        full24Root.requireScriptDataInputs(full26))
      assertThrows(classOf[SQLNonTransientConnectionException], () =>
        full26Root.requireScriptPublication1Inputs(full26))
      assertThrows(classOf[SQLNonTransientConnectionException], () =>
        full26Root.issueScriptSchemaSnapshotImage(full26, Schema, Connection, Scope, full26.budget.packetBytes))
      assertThrows(classOf[SQLNonTransientConnectionException], () =>
        full26Root.requireScriptDataInputs(full24))
      assertEquals(0L, full24Root.accountedMemoryBytes)
      assertEquals(0L, full26Root.accountedMemoryBytes)
    finally
      full26Root.closeRootReadBudget()
      full24Root.closeRootReadBudget()

  @Test def full26ColdDecoderRejectsTruncationTrailingAndEveryPinnedIdentityChange(): Unit =
    val sourceProfile = profile(2_000_000L)
    val full24Inputs = inputs(sourceProfile, evidenceBytes = 100_000L)
    val full26 = full26Inputs(sourceProfile)
    val full24Root = root(full24Inputs)
    val full26Root = root(full26)
    var full24Payload: MySqlScriptSchemaCanonicalPayload = null
    var full26Payload: MySqlScriptSchemaCanonicalPayload = null
    try
      val snapshot24 = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        full24Inputs.budget.packetBytes, full24Inputs, completeRows, full24Root)
      val snapshot26 = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        full26.budget.packetBytes, full26, full26Rows(full26), full26Root)
      full24Payload = MySqlScriptSchemaSnapshotCodec.encode(snapshot24, full24Root)
      full26Payload = MySqlScriptSchemaSnapshotCodec.encode(snapshot26, full26Root)
      val bytes = canonicalBytes(full26Payload)
      val domainBytes = full26.snapshotDomain.getBytes(StandardCharsets.UTF_8)
      val ddlOffset = 4 + domainBytes.length
      val schemaFormatOffset = ddlOffset + full26.ddlSha256.length
      val schemaLengthOffset = schemaFormatOffset + 4
      val schemaBytesOffset = schemaLengthOffset + 4
      def changed(offset: Int, value: Byte): Array[Byte] =
        val result = bytes.clone()
        result(offset) = value
        result

      assertColdDecodeRejected(bytes.dropRight(1), full26, full26Root, classOf[EOFException],
        "truncated ScriptPublication1 canonical framing")
      assertColdDecodeRejected(bytes ++ Array[Byte](0), full26, full26Root, classOf[SQLDataException],
        "trailing ScriptPublication1 canonical bytes")
      assertColdDecodeRejected(changed(4, (bytes(4) ^ 1).toByte), full26, full26Root,
        classOf[SQLDataException], "frozen domain/profile shape differs")
      assertColdDecodeRejected(changed(ddlOffset, (bytes(ddlOffset) ^ 1).toByte), full26, full26Root,
        classOf[SQLDataException], "pinned SQL source identity differs")
      assertColdDecodeRejected(changed(schemaFormatOffset + 3, 4.toByte), full26, full26Root,
        classOf[SQLDataException], "schema format differs")
      assertColdDecodeRejected(changed(schemaBytesOffset, (bytes(schemaBytesOffset) ^ 1).toByte), full26, full26Root,
        classOf[SQLDataException], "frozen domain/profile shape differs")

      assertColdDecodeRejected(canonicalBytes(full24Payload), full26, full26Root,
        classOf[SQLDataException], "frozen shape text length differs")
      assertColdDecodeRejected(bytes, full24Inputs, full24Root,
        classOf[SQLDataException], "frozen shape text length differs")
      assertFalse(full24Payload.storageBudget.isClosed)
      assertFalse(full26Payload.storageBudget.isClosed)
    finally
      if full26Payload != null then full26Payload.storageBudget.closeScopedReadBudget()
      if full24Payload != null then full24Payload.storageBudget.closeScopedReadBudget()
      full26Root.closeRootReadBudget()
      full24Root.closeRootReadBudget()

  @Test def full26RequiresEveryUniqueCanonicalRelationAndStrictPrimaryKeyOrder(): Unit =
    val selectedInputs = full26Inputs(profile(2_000_000L))
    val valid = full26Rows(selectedInputs)
    val operationTable = MySqlDeploymentReceiptSchemaDefinition.OperationTable
    val operationRows = valid.find(_.table == operationTable).get.rows
    val relationCases = Vector(
      "missing one relation" -> valid.dropRight(1),
      "duplicate relation identity" -> valid.updated(1, valid.head),
      "noncanonical full26 relation order" -> valid.updated(0, valid(1)).updated(1, valid(0)),
      "duplicate operation primary key" -> valid.map(relation =>
        if relation.table == operationTable then relation.copy(rows = operationRows.updated(1, operationRows.head)) else relation),
      "unsorted operation primary keys" -> valid.map(relation =>
        if relation.table == operationTable then relation.copy(rows = operationRows.reverse) else relation)
    )
    val allocation = root(selectedInputs)
    val baseline = allocation.accountedMemoryBytes
    try
      relationCases.foreach { (label, malformed) =>
        val failure = assertThrows(classOf[SQLDataException], () => {
          selectedInputs.validateRelations(malformed, selectedInputs.budget.packetBytes, allocation)
          ()
        }, label)
        assertTrue(failure.getStackTrace.nonEmpty, label)
        assertEquals(baseline, allocation.accountedMemoryBytes, s"$label must release its shared temporary workspace")
      }
    finally allocation.closeRootReadBudget()

  @Test def deploymentEvidenceAndDiagnosticBudgetsFailBeforeOperationCloning(): Unit =
    val sourceProfile = profile(2_000_000L)
    val publication = inputs(sourceProfile, evidenceBytes = 100_000L).budget
    val ddlSha256 = HexFormat.of().formatHex(Array.fill[Byte](32)(0x5a.toByte))
    val cases = Vector(
      "evidence" -> MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 2L, diagnosticsBytes = 100_000L),
      "diagnostic" -> MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 100_000L, diagnosticsBytes = 2L)
    )
    cases.foreach { (kind, deploymentBudget) =>
      val selectedInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(Schema, ddlSha256,
        sourceProfile, publication, deploymentBudget)
      val allocation = root(selectedInputs)
      val baseline = allocation.accountedMemoryBytes
      try
        val failure = assertThrows(classOf[SQLDataException], () => selectedInputs.validateRelations(
          full26Rows(selectedInputs), selectedInputs.budget.packetBytes, allocation))
        val expectedColumn = if kind == "evidence" then "enqueue_evidence_bytes" else "diagnostic_bytes"
        assertTrue(failure.getMessage.contains(expectedColumn), failure.getMessage)
        assertTrue(failure.getMessage.contains("explicit binary byte budget"), failure.getMessage)
        assertEquals(baseline, allocation.accountedMemoryBytes,
          s"the explicit deployment $kind limit must reject before retaining decoder clones")
      finally allocation.closeRootReadBudget()
    }

  @Test def deploymentDecoderCloneReservationFailsAgainstTheOriginalSharedLedger(): Unit =
    val selectedInputs = full26Inputs(profile(2_000_000L))
    val allocation = selectedInputs.newAllocationRoot(selectedInputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(41_000L))
    val baseline = allocation.accountedMemoryBytes
    try
      val failure = assertThrows(classOf[SQLDataException], () => selectedInputs.validateRelations(
        full26Rows(selectedInputs), selectedInputs.budget.packetBytes, allocation))
      assertInstanceOf(classOf[MySqlDeploymentOperationRows.DecodeFailure], failure.getCause)
      assertInstanceOf(classOf[SQLDataException], failure.getCause.getCause)
      assertTrue(failure.getCause.getCause.getMessage.contains("shared explicit retained-memory budget"),
        failure.getCause.getCause.getMessage)
      assertEquals(baseline, allocation.accountedMemoryBytes,
        "clone reservation rejection must close the temporary decoder child and validation workspace")
    finally allocation.closeRootReadBudget()

  @Test def strictDecoderRejectsAlteredDomainAndTrailingCanonicalBytes(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val allocation = root(selectedInputs)
    var valid: MySqlScriptSchemaCanonicalPayload = null
    var wrongDomain: MySqlScriptSchemaCanonicalPayload = null
    var nullCell: MySqlScriptSchemaCanonicalPayload = null
    var trailing: MySqlScriptSchemaCanonicalPayload = null
    try
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, completeRows, allocation)
      valid = MySqlScriptSchemaSnapshotCodec.encode(snapshot, allocation)
      val bytes = Array.tabulate[Byte](valid.byteLength)(valid.bytes.byteAt)
      val changedDomain = bytes.clone()
      changedDomain(4) = (changedDomain(4) ^ 0x01).toByte
      wrongDomain = MySqlScriptSchemaSnapshotCodec.capture(changedDomain, selectedInputs, allocation)
      val headerLength = MySqlScriptSchemaSnapshotCanonical.fixedHeaderBytes(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, allocation).toInt
      val nullTag = bytes.clone()
      nullTag(headerLength - MySqlScriptSchemaInputs.ExpectedTableCount * 4 + 8) = 0.toByte
      nullCell = MySqlScriptSchemaSnapshotCodec.capture(nullTag, selectedInputs, allocation)
      trailing = MySqlScriptSchemaSnapshotCodec.capture(bytes ++ Array[Byte](0), selectedInputs, allocation)

      val domainFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshotCodec.decode(wrongDomain, selectedInputs, allocation))
      assertTrue(domainFailure.getMessage.contains("frozen domain/profile shape differs"), domainFailure.getMessage)

      val nullFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshotCodec.decode(nullCell, selectedInputs, allocation))
      assertTrue(nullFailure.getMessage.contains("unexpectedly encodes SQL NULL"), nullFailure.getMessage)

      val beforeTrailingDecode = allocation.accountedMemoryBytes
      val trailingFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshotCodec.decode(trailing, selectedInputs, allocation))
      assertEquals("trailing ScriptPublication1 canonical bytes", trailingFailure.getMessage)
      assertTrue(trailingFailure.getStackTrace.nonEmpty)
      assertEquals(beforeTrailingDecode, allocation.accountedMemoryBytes,
        "failed strict decode must release its temporary typed image and keep both retained inputs")
      assertFalse(wrongDomain.storageBudget.isClosed)
      assertFalse(nullCell.storageBudget.isClosed)
      assertFalse(trailing.storageBudget.isClosed)
    finally
      if trailing != null then trailing.storageBudget.closeScopedReadBudget()
      if nullCell != null then nullCell.storageBudget.closeScopedReadBudget()
      if wrongDomain != null then wrongDomain.storageBudget.closeScopedReadBudget()
      if valid != null then valid.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def nullRowsAndNoncanonicalRelationOrderFailAfterBudgetedValidationStarts(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val allocation = root(selectedInputs)
    val baseline = allocation.accountedMemoryBytes
    try
      val nullCellRows = completeRows.map { relation =>
        if relation.table == "fp_schema_manifest" then
          val index = MySqlScriptSchemaInputs.forTable(relation.table).columns.indexWhere(_.name == "singleton_fixed1")
          relation.copy(rows = Vector(relation.rows.head.updated(index, MySqlFormat3Cell.SqlNull)))
        else relation
      }
      val nullFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, nullCellRows, allocation))
      assertTrue(nullFailure.getMessage.contains("unexpected SQL NULL"), nullFailure.getMessage)
      assertTrue(allocation.successfulReserveHighWaterBytes >= 32_768L,
        "the malformed cell must be reached only after reserving the full relation-validation workspace")
      assertEquals(baseline, allocation.accountedMemoryBytes)

      val shuffledRows = completeRows.updated(0, completeRows(1)).updated(1, completeRows(0))
      val orderFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, shuffledRows, allocation))
      assertTrue(orderFailure.getMessage.contains("canonical relation order"), orderFailure.getMessage)
      assertEquals(baseline, allocation.accountedMemoryBytes)

      val catalogShape = MySqlScriptSchemaInputs.forTable("fp_script_catalog")
      val bindingIndex = catalogShape.columns.indexWhere(_.name == "binding_uuid")
      val lowerKey = validRow(catalogShape).updated(bindingIndex,
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](16))))
      val higherKey = validRow(catalogShape).updated(bindingIndex,
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](16)(1))))
      val unsortedPrimaryKey = completeRows.map { relation =>
        if relation.table == catalogShape.table then relation.copy(rows = Vector(higherKey, lowerKey)) else relation
      }
      val keyFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, unsortedPrimaryKey, allocation))
      assertTrue(keyFailure.getMessage.contains("rows are not strictly ordered"), keyFailure.getMessage)
      assertEquals(baseline, allocation.accountedMemoryBytes)

      val schemaShape = MySqlScriptSchemaInputs.forTable("fp_schema_manifest")
      val backendIndex = schemaShape.columns.indexWhere(_.name == "backend")
      val tooLongVarchar = completeRows.map { relation =>
        if relation.table == schemaShape.table then
          relation.copy(rows = Vector(relation.rows.head.updated(backendIndex, MySqlFormat3Cell.Text("TOO-LONG!"))))
        else relation
      }
      val varcharFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, tooLongVarchar, allocation))
      assertTrue(varcharFailure.getMessage.contains("fp_schema_manifest.backend text length"), varcharFailure.getMessage)
      assertEquals(baseline, allocation.accountedMemoryBytes)
    finally allocation.closeRootReadBudget()

  @Test def totalRowQuotaRejectsOnlyAfterTheBudgetedFull24ValidationPass(): Unit =
    val sourceProfile = profile(2_000_000L)
    val base = inputs(sourceProfile, evidenceBytes = 100_000L)
    val selectedInputs = MySqlScriptSchemaInputs.create(sourceProfile, base.budget.copy(rows = 23L))
    val allocation = root(selectedInputs)
    try
      val failure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, completeRows, allocation))
      assertTrue(failure.getMessage.contains("total row budget exceeded"), failure.getMessage)
      assertTrue(allocation.successfulReserveHighWaterBytes >= 32_768L,
        "the complete typed graph is validated under its workspace before enforcing the total row cap")
      assertEquals(0L, allocation.accountedMemoryBytes)
    finally allocation.closeRootReadBudget()

  @Test def largeTextValuesRespectTheirExplicitTableAndColumnLimit(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 256L)
    val allocation = root(selectedInputs)
    val schemaShape = MySqlScriptSchemaInputs.forTable("floating_pod_state")
    val worldName = schemaShape.columns.indexWhere(_.name == "world_name")
    val oversizedRows = completeRows.map { relation =>
      if relation.table == schemaShape.table then
        relation.copy(rows = Vector(relation.rows.head.updated(worldName, MySqlFormat3Cell.Text("x" * 257))))
      else relation
    }
    val baseline = allocation.accountedMemoryBytes
    try
      val failure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, oversizedRows, allocation))
      assertTrue(failure.getMessage.contains("floating_pod_state.world_name exceeds explicit text byte budget"), failure.getMessage)
      assertTrue(allocation.successfulReserveHighWaterBytes >= 32_768L)
      assertEquals(baseline, allocation.accountedMemoryBytes)

      val malformedUtf16 = completeRows.map { relation =>
        if relation.table == schemaShape.table then
          relation.copy(rows = Vector(relation.rows.head.updated(worldName, MySqlFormat3Cell.Text("\uD800"))))
        else relation
      }
      val encodingFailure = assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
          selectedInputs.budget.packetBytes, selectedInputs, malformedUtf16, allocation))
      assertTrue(encodingFailure.getMessage.contains("unpaired UTF-16 surrogate"), encodingFailure.getMessage)
      assertEquals(baseline, allocation.accountedMemoryBytes)
    finally allocation.closeRootReadBudget()

  @Test def malformedUtf8InsideAValidFull24WireFailsStrictDecodeAndReleasesItsLocalImage(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val allocation = root(selectedInputs)
    var valid: MySqlScriptSchemaCanonicalPayload = null
    var malformed: MySqlScriptSchemaCanonicalPayload = null
    try
      val schemaShape = MySqlScriptSchemaInputs.forTable("floating_pod_state")
      val worldName = schemaShape.columns.indexWhere(_.name == "world_name")
      val rows = completeRows.map { relation =>
        if relation.table == schemaShape.table then
          relation.copy(rows = Vector(relation.rows.head.updated(worldName, MySqlFormat3Cell.Text("😀"))))
        else relation
      }
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, rows, allocation)
      valid = MySqlScriptSchemaSnapshotCodec.encode(snapshot, allocation)
      val original = Array.tabulate[Byte](valid.byteLength)(valid.bytes.byteAt)
      val emoji = "😀".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      val offset = original.indices.find(index => index + emoji.length <= original.length &&
        original.slice(index, index + emoji.length).sameElements(emoji)).getOrElse(
        throw new AssertionError("the encoded full24 source row must contain its distinct emoji value"))
      val changed = original.clone()
      changed(offset) = 0xc0.toByte
      malformed = MySqlScriptSchemaSnapshotCodec.capture(changed, selectedInputs, allocation)

      val beforeDecode = allocation.accountedMemoryBytes
      assertThrows(classOf[java.nio.charset.CharacterCodingException], () =>
        MySqlScriptSchemaSnapshotCodec.decode(malformed, selectedInputs, allocation))
      assertEquals(beforeDecode, allocation.accountedMemoryBytes,
        "strict UTF-8 failure must drop the temporary decoded graph but keep the malformed input owner")
      assertFalse(malformed.storageBudget.isClosed)
    finally
      if malformed != null then malformed.storageBudget.closeScopedReadBudget()
      if valid != null then valid.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def validationWorkspaceIsChargedAndReleasedAfterAnInBudgetScanRejectsPayload(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 1L)
    val allocation = root(selectedInputs)
    val originalAccounted = allocation.accountedMemoryBytes
    val originalAvailable = allocation.availableMemoryBytes

    val failure = assertThrows(classOf[SQLDataException], () =>
      MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, evidenceRows(Array[Byte](1, 2)), allocation))

    assertTrue(failure.getMessage.contains("explicit binary byte budget"), failure.getMessage)
    assertTrue(allocation.successfulReserveHighWaterBytes >= 32_768L,
      "the validation workspace must be reserved before scanning materialized rows")
    assertEquals(originalAccounted, allocation.accountedMemoryBytes,
      "validation failure must release the complete temporary workspace")
    assertEquals(originalAvailable, allocation.availableMemoryBytes)

  @Test def validationWorkspaceReservationRejectsBeforeRelationScanningWhenCapacityIsSmaller(): Unit =
    val selectedInputs = inputs(profile(16_384L), evidenceBytes = 1L)
    val allocation = root(selectedInputs)
    val originalAvailable = allocation.availableMemoryBytes

    val failure = assertThrows(classOf[SQLDataException], () =>
      MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
        selectedInputs.budget.packetBytes, selectedInputs, evidenceRows(Array[Byte](1, 2)), allocation))

    assertTrue(failure.getMessage.contains("exceeds explicit retained-memory budget"), failure.getMessage)
    assertEquals(0L, allocation.successfulReserveHighWaterBytes,
      "an unavailable workspace must fail before row validation begins")
    assertEquals(0L, allocation.accountedMemoryBytes)
    assertEquals(originalAvailable, allocation.availableMemoryBytes)


  @Test def fixedJdbcSqlRejectsUnknownTablesAndKeepsAllTwentyFourLockStatementsBounded(): Unit =
    val names = MySqlScriptSchemaInputs.lockedReadOrder.map(_.table)
    assertEquals(Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer"), names.take(3))
    assertEquals(24, names.distinct.size)
    assertEquals(Set("fp_script_catalog", "fp_script_operation", "fp_script_candidate", "fp_script_version"), names.takeRight(4).toSet)
    names.foreach { table =>
      val sql = MySqlScriptSchemaSnapshotSql.select(table)
      assertEquals(sql.getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toLong,
        MySqlScriptSchemaSnapshotSql.selectUtf8Length(table), table)
      assertTrue(sql.contains(" FROM `fp_script_publication1`.`" + table + "` ORDER BY "))
      assertTrue(sql.endsWith(MySqlScriptSchemaInputs.forTable(table).lockClause))
    }
    assertThrows(classOf[IllegalArgumentException], () => MySqlScriptSchemaSnapshotSql.select("fp_script_catalog`;DELETE"))

  @Test def lockedConnectorMetadataExposesMediumBlobAsLongVarbinaryAndRejectsWrongColumns(): Unit =
    import com.mysql.cj.MysqlType
    import com.mysql.cj.result.Field
    import com.mysql.cj.util.LazyString
    val shape = MySqlScriptSchemaInputs.forTable("fp_script_candidate")
    def field(column: MySqlFormat3ColumnShape): Field =
      val kind = if column.sqlDeclaration.startsWith("MEDIUMBLOB") then MysqlType.MEDIUMBLOB
        else if column.sqlDeclaration.startsWith("LONGBLOB") then MysqlType.LONGBLOB
        else column.kind match
          case MySqlFormat3SqlKind.Text => MysqlType.VARCHAR
          case MySqlFormat3SqlKind.Binary => MysqlType.VARBINARY
          case MySqlFormat3SqlKind.Integer => MysqlType.BIGINT
          case _ => throw new AssertionError("unexpected candidate fixture kind")
      new Field(new LazyString(Schema), new LazyString(shape.table), new LazyString(shape.table),
        new LazyString(column.name), new LazyString(column.name), column.maximumLength.getOrElse(1024).toLong,
        0, (if column.nullable then 0 else MysqlType.FIELD_FLAG_NOT_NULL).toShort, 0, 63, "UTF-8", kind)
    val fields = shape.columns.map(field).toArray
    val metadata = new com.mysql.cj.jdbc.result.ResultSetMetaData(null, fields, false, true, null)
    val sourceIndex = shape.columns.indexWhere(_.name == "source_bytes")
    assertEquals(java.sql.Types.LONGVARBINARY, metadata.getColumnType(sourceIndex + 1))
    assertEquals("MEDIUMBLOB", metadata.getColumnTypeName(sourceIndex + 1))
    MySqlScriptSchemaSnapshotSql.validateMetadata(metadata, shape.table)
    fields(sourceIndex).setMysqlType(MysqlType.VARBINARY)
    assertThrows(classOf[SQLDataException], () => MySqlScriptSchemaSnapshotSql.validateMetadata(metadata, shape.table))

  @Test def schema4JdbcImagesUseOriginalRootAndRejectObservationChangesWithoutLeakingChildDebt(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 100_000L)
    val allocation = root(selectedInputs)
    var child: MySqlFormat3ReadBudget = null
    try
      child = allocation.issueScriptSchemaSnapshotImage(selectedInputs, Schema, Connection, Scope, 4_000_000L)
      assertTrue(child.isSnapshotReadChildOf(allocation))
      assertTrue(child.sharesAllocationLedger(allocation))
      val before = allocation.accountedMemoryBytes
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
        allocation.issueScriptSchemaSnapshotImage(selectedInputs, Schema, Connection, Scope, 3_999_999L))
      assertEquals(before, allocation.accountedMemoryBytes)
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
        allocation.issueOwnedSnapshotImage(Schema, Connection, Scope, 4_000_000L))
    finally
      if child != null then child.closeScopedReadBudget()
      allocation.closeRootReadBudget()
    assertEquals(0L, allocation.accountedMemoryBytes)


  @Test def schema4LongTextQuotaReachesTheRealStreamDecoderAsBytesWithoutReplacingCharacters(): Unit =
    val selectedInputs = inputs(profile(2_000_000L), evidenceBytes = 3L)
    val shape = MySqlScriptSchemaInputs.forTable("floating_pod_state")
    val column = shape.columns.find(_.name == "world_name").get
    assertTrue(column.sqlDeclaration.startsWith("LONGTEXT"))
    assertEquals(Some(3L), selectedInputs.maximumPayloadBytes(shape.table, column))
    def decode(text: String, accepted: Boolean): Unit =
      val allocation = root(selectedInputs)
      allocation.initialize(Schema, Connection, Scope, selectedInputs.budget.packetBytes)
      allocation.beginRow(1, shape.table, 0L)
      val before = allocation.accountedMemoryBytes
      try
        if accepted then
          assertEquals(text, MySqlFormat3CellStreamDecoder.readText(new java.io.StringReader(text),
            column.maximumLength, allocation, "schema4 world_name", selectedInputs.maximumPayloadBytes(shape.table, column)))
        else
          val failure = assertThrows(classOf[SQLDataException], () => MySqlFormat3CellStreamDecoder.readText(
            new java.io.StringReader(text), column.maximumLength, allocation, "schema4 world_name",
            selectedInputs.maximumPayloadBytes(shape.table, column)))
          assertTrue(failure.getMessage.contains("explicit UTF-8 byte length"))
          assertEquals(before, allocation.accountedMemoryBytes)
      finally
        allocation.endRow(shape.table, 0L)
        allocation.closeRootReadBudget()
    decode("汉", true)
    decode("汉汉", false)
    decode("😀", false)

  @Test def smallintColumnUsesActualJdbcMetadataType(): Unit =
    assertEquals(Types.SMALLINT,MysqlType.SMALLINT.getJdbcType)
    Vector(false,true).foreach { nullable =>
      val declaration=if nullable then "SMALLINT NULL" else "SMALLINT NOT NULL"
      val column=MySqlFormat3ColumnShape("small_value",declaration,MySqlFormat3SqlKind.Integer,nullable,None)
      assertEquals(Types.SMALLINT,column.jdbcType,"SMALLINT must match actual Connector/J metadata for either nullability")
      Vector(("TINYINT",Types.TINYINT),("INT",Types.INTEGER),("BIGINT",Types.BIGINT)).foreach { (name,expected) =>
        val adjacent=MySqlFormat3ColumnShape("adjacent",name+" NOT NULL",MySqlFormat3SqlKind.Integer,false,None)
        assertEquals(expected,adjacent.jdbcType)
      }
    }

  @Test def smallintCellEnforcesSignedShortRange(): Unit =
    Vector(false,true).foreach { nullable =>
      val column=MySqlFormat3ColumnShape("small_value",if nullable then "SMALLINT NULL" else "SMALLINT NOT NULL",
        MySqlFormat3SqlKind.Integer,nullable,None)
      Vector(Short.MinValue.toLong,0L,Short.MaxValue.toLong).foreach { value =>
        MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SignedInteger(value),column,"smallint_data")
      }
      Vector(-32769L,32768L).foreach { value =>
        val failure=assertThrows(classOf[SQLDataException],()=>
          MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SignedInteger(value),column,"smallint_data"))
        assertTrue(failure.getMessage.contains("smallint_data.small_value SMALLINT range"),failure.getMessage)
      }
      if nullable then MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SqlNull,column,"smallint_data")
      else assertThrows(classOf[SQLDataException],()=>
        MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SqlNull,column,"smallint_data"))
    }
    Vector(("TINYINT",Byte.MinValue.toLong,Byte.MaxValue.toLong),("INT",Int.MinValue.toLong,Int.MaxValue.toLong),
      ("BIGINT",Long.MinValue,Long.MaxValue)).foreach { (name,minimum,maximum) =>
      val column=MySqlFormat3ColumnShape("adjacent",name+" NOT NULL",MySqlFormat3SqlKind.Integer,false,None)
      MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SignedInteger(minimum),column,"smallint_data")
      MySqlFormat3SnapshotValidation.validateCell(MySqlFormat3Cell.SignedInteger(maximum),column,"smallint_data")
      if name!="BIGINT" then
        assertThrows(classOf[SQLDataException],()=>MySqlFormat3SnapshotValidation.validateCell(
          MySqlFormat3Cell.SignedInteger(minimum-1L),column,"smallint_data"))
        assertThrows(classOf[SQLDataException],()=>MySqlFormat3SnapshotValidation.validateCell(
          MySqlFormat3Cell.SignedInteger(maximum+1L),column,"smallint_data"))
    }
