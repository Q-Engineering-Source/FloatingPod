package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.B1ResumeRecord
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.security.MessageDigest
import java.sql.{ResultSetMetaData, Types}
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 直接验证 Fresh Format3 全二十关系图、29列独立manifest和完整效果摘要。 */
final class MySqlB1CommonGraphColdCodecTest:
  private val schema = "fp_format3_full20_test"
  private val serverUuid = UUID.fromString("00000000-0000-4000-8000-000000000001")
  private val bindingUuid = UUID.fromString("a5000000-0000-4000-8000-000000000005")
  private val provisioningUuid = UUID.fromString("a7000000-0000-4000-8000-000000000007")
  private val bootUuid = UUID.fromString("a8000000-0000-4000-8000-000000000008")
  private val sha = Vector.fill[Byte](32)(0x31.toByte)
  private val ddlHex = java.util.HexFormat.of().formatHex(sha.toArray).toUpperCase(java.util.Locale.ROOT)
  private val profile = SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
    8_000_000L, 4_000_000L, 2_000_000L)
  private val sourceBytes = Array[Byte](1, 2, 3, 4, 5, 6)
  private val sourceDigest = MessageDigest.getInstance("SHA-256").digest(sourceBytes)

  private def auditColumn(label: String): MySqlAuditColumn =
    MySqlAuditColumn(label, Types.VARCHAR, "VARCHAR", 255, 0, ResultSetMetaData.columnNoNulls, signed = false)

  private def privilegeRows: Vector[MySqlAuditResultSet] =
    MySqlAuditCoverage.Format3PrivilegeResultSetIds.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).map { (id, labels) =>
      val rows = if id == "RS17_SCHEMA_PRIVILEGES" then Vector("SELECT", "TRIGGER").map { privilege =>
        Vector("'audit'@'localhost'", "def", schema, privilege, "NO").map(MySqlAuditText.apply)
      } else Vector.empty
      MySqlAuditResultSet(id, labels.map(auditColumn), rows.map(MySqlAuditRow.apply))
    }

  private val inputs = new MySqlFormat3AuditInputs(serverUuid, "9.7.2", schema, "fixture@localhost",
    "fixture@localhost", "NONE", provisioningUuid, bindingUuid, ddlHex, "audit@localhost",
    Vector("GRANT USAGE ON *.* TO 'fixture'@'localhost'"), privilegeRows,
    MySqlAuditBinary(Array.fill[Byte](32)(0x41.toByte)), "FRESH_PLAN",
    MySqlAuditBinary(sourceBytes), MySqlAuditBinary(sourceDigest), profile)

  private val manifest = MySqlLockedManifest(1, "MYSQL", 3, sha, provisioningUuid, "READY", serverUuid,
    bindingUuid, None, "NONE", None, None, None, Some(Vector.fill[Byte](32)(0x52.toByte)))

  private val observation = MySqlFormat3ConnectionObservation(serverUuid, "9.7.2", schema, "fixture@localhost",
    "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00",
    "TLSv1.3", "TLS_AES_256_GCM_SHA384")
  private val observationScope = MySqlFormat3ObservationScope(bootUuid, 1L, 1L)
  private val databaseNow = LocalDateTime.of(2026, 10, 8, 1, 2, 3, 100_000_000)
  private val originalWriter = MySqlBusinessWriterRow(bindingUuid, 0L, None, databaseNow.minusSeconds(1), None, None, databaseNow)

  private val plan = MySqlBootstrapPlanIdentity(
    UUID.fromString("a1000000-0000-4000-8000-000000000001"),
    UUID.fromString("a2000000-0000-4000-8000-000000000002"),
    UUID.fromString("a3000000-0000-4000-8000-000000000003"),
    UUID.fromString("a4000000-0000-4000-8000-000000000004"),
    bindingUuid,
    serverUuid,
    schema,
    3,
    2,
    ddlHex,
    provisioningUuid
  )

  private def binary(bytes: Array[Byte]): MySqlFormat3Cell.Binary =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.clone()))

  private def optionalBinary(bytes: Option[Vector[Byte]]): MySqlFormat3Cell = bytes match
    case None => MySqlFormat3Cell.SqlNull
    case Some(value) => binary(value.toArray)

  private def optionalUuid(value: Option[UUID]): MySqlFormat3Cell = value match
    case None => MySqlFormat3Cell.SqlNull
    case Some(uuid) => binary(MySqlStoredRows.encodeUuid(uuid).toArray)

  private def auditCell(label: String, value: MySqlAuditCell): MySqlFormat3Cell = value match
    case MySqlAuditNull => MySqlFormat3Cell.SqlNull
    case MySqlAuditText(text) if label.startsWith("max_") => MySqlFormat3Cell.SignedInteger(text.toLong)
    case MySqlAuditText(text) => MySqlFormat3Cell.Text(text)
    case value: MySqlAuditBinary => binary(value.bytes)
    case MySqlAuditTimestamp(value) => MySqlFormat3Cell.DateTime(value)

  private def manifestRow: Vector[MySqlFormat3Cell] =
    val base = Map[String, MySqlFormat3Cell](
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(manifest.singleton.toLong),
      "backend" -> MySqlFormat3Cell.Text(manifest.backend),
      "format_version" -> MySqlFormat3Cell.SignedInteger(manifest.formatVersion.toLong),
      "ddl_sha256" -> binary(manifest.ddlSha256.toArray),
      "provisioning_uuid" -> binary(MySqlStoredRows.encodeUuid(manifest.provisioningUuid).toArray),
      "bootstrap_state" -> MySqlFormat3Cell.Text(manifest.bootstrapState),
      "expected_server_uuid" -> binary(MySqlStoredRows.encodeUuid(manifest.serverUuid).toArray),
      "expected_binding_uuid" -> binary(MySqlStoredRows.encodeUuid(manifest.bindingUuid).toArray),
      "import_manifest_sha256" -> optionalBinary(manifest.importManifestSha256),
      "import_state" -> MySqlFormat3Cell.Text(manifest.importState),
      "import_operation_uuid" -> optionalUuid(manifest.importOperationUuid),
      "import_snapshot_sha256" -> optionalBinary(manifest.importSnapshotSha256),
      "import_receipt_sha256" -> optionalBinary(manifest.importReceiptSha256),
      "guard_manifest_sha256" -> optionalBinary(manifest.guardManifestSha256)
    ) ++ inputs.manifestLabels.zip(inputs.expectedManifestCells).map { (label, value) => label -> auditCell(label, value) }
    val shape = MySqlFormat3RelationShapes.forTable("fp_schema_manifest")
    shape.columns.map(column => base.getOrElse(column.name, throw new AssertionError("unmapped manifest cell " + column.name)))

  private def bindingRow(value: PhysicalRecoveryBinding): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable("physical_recovery_binding")
    val cells = Map[String, MySqlFormat3Cell](
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(value.bindingUuid).toArray),
      "cutover_state" -> MySqlFormat3Cell.Text(value.cutoverState.databaseValue),
      "format_version" -> MySqlFormat3Cell.SignedInteger(value.formatVersion.toLong)
    )
    shape.columns.map(column => cells(column.name))

  private def bootstrapIntentRow(value: MySqlBootstrapIntentData): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable("binding_bootstrap_intent")
    val cells = Map[String, MySqlFormat3Cell](
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(value.singletonFixed1.toLong),
      "bootstrap_operation_uuid" -> binary(MySqlStoredRows.encodeUuid(value.bootstrapOperationUuid).toArray),
      "prepare_write_uuid" -> binary(MySqlStoredRows.encodeUuid(value.prepareWriteUuid).toArray),
      "certify_write_uuid" -> binary(MySqlStoredRows.encodeUuid(value.certifyWriteUuid).toArray),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(value.bindingUuid).toArray),
      "binding_format_version" -> MySqlFormat3Cell.SignedInteger(value.bindingFormatVersion.toLong),
      "provisioning_uuid" -> binary(MySqlStoredRows.encodeUuid(value.provisioningUuid).toArray),
      "source_world_identity" -> MySqlFormat3Cell.Text(value.sourceWorldIdentity),
      "provision_receipt_sha256" -> binary(value.provisionReceiptSha256.toArray),
      "stage" -> MySqlFormat3Cell.Text(value.stage match
        case MySqlBootstrapIntentStage.Prepared => "PREPARED"
        case MySqlBootstrapIntentStage.Complete => "COMPLETE"),
      "marker_ack_sha256" -> optionalBinary(value.markerAckSha256)
    )
    shape.columns.map(column => cells(column.name))

  private def writerRow(value: MySqlBusinessWriterRow): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable("fp_runtime_writer")
    val cells = Map[String, MySqlFormat3Cell](
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> binary(MySqlStoredRows.encodeUuid(value.bindingUuid).toArray),
      "writer_epoch" -> MySqlFormat3Cell.SignedInteger(value.writerEpoch),
      "owner_boot_uuid" -> optionalUuid(value.ownerBootUuid),
      "lease_until" -> MySqlFormat3Cell.DateTime(value.leaseUntil),
      "last_commit_operation_uuid" -> optionalUuid(value.lastOperationUuid),
      "last_commit_fingerprint" -> optionalBinary(value.lastFingerprint)
    )
    shape.columns.map(column => cells(column.name))

  private def defaultCell(column: MySqlFormat3ColumnShape): MySqlFormat3Cell =
    if column.nullable then MySqlFormat3Cell.SqlNull
    else column.kind match
      case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
      case MySqlFormat3SqlKind.Binary => binary(Array.fill[Byte](column.maximumLength.getOrElse(1))(1))
      case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
      case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
      case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(databaseNow)

  private def buildSnapshot(
      binding: Option[PhysicalRecoveryBinding] = None,
      writer: MySqlBusinessWriterRow = originalWriter,
      bootstrapIntents: Vector[MySqlBootstrapIntentData] = Vector.empty,
      relationsOverride: Vector[MySqlFormat3RelationSnapshot] = null,
      withObligation: Boolean = false
  ): MySqlFormat3FullSnapshot =
    val relations = if relationsOverride != null then relationsOverride else MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      val rows = shape.table match
        case "fp_schema_manifest" => Vector(manifestRow)
        case "physical_recovery_binding" => binding.toVector.map(bindingRow)
        case "binding_bootstrap_intent" => bootstrapIntents.map(bootstrapIntentRow)
        case "fp_runtime_writer" => Vector(writerRow(writer))
        case "physical_source_obligation" if withObligation => Vector(shape.columns.map(defaultCell))
        case _ => Vector.empty
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    budget.initialize(schema, observation, observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, observation, observationScope, 16_777_216L, profile, relations, budget)

  private def replaceRelation(snapshot: MySqlFormat3FullSnapshot, table: String,
      transform: MySqlFormat3RelationSnapshot => MySqlFormat3RelationSnapshot): MySqlFormat3FullSnapshot =
    buildSnapshot(relationsOverride = snapshot.relations.map { relation =>
      if relation.table == table then transform(relation) else relation
    })

  private def graph(snapshot: MySqlFormat3FullSnapshot, repository: MySqlFullRepositorySnapshot = MySqlFullRepositorySnapshot.empty) =
    MySqlB1CommonGraph.captureFormat3(manifest, repository.recovery.binding, repository, snapshot, inputs)

  @Test def uniqueOriginalEncoderRoundtripsAllCompleteColdComponents(): Unit =
    val source = buildSnapshot()
    val common = graph(source)
    val draft = MySqlB1CommonGraphColdCodec.draft(common.toResumeAssertions, source, source.allocationBudget)
    val decoded = MySqlB1CommonGraphColdCodec.decode(draft, inputs)
    assertTrue(decoded.exactlyEquals(common))
    assertSame(decoded, draft.result)
    assertFalse(source.allocationBudget.isClosed)
    MySqlFormat3B1ContextCorrespondence.requireCorrespondence(decoded, source, inputs, source.allocationBudget)

  @Test def completeAdditionalHistoryAbove64KiBRemainsInOriginalFullTwenty(): Unit =
    val shape = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val large = Array.fill[Byte](70000)(0x41.toByte)
    val history = shape.columns.map(defaultCell)
      .updated(shape.columns.indexWhere(_.name == "carrier_key"), binary(large))
      .updated(shape.columns.indexWhere(_.name == "carrier_key_hash"), binary(MessageDigest.getInstance("SHA-256").digest(large)))
    val seed = buildSnapshot()
    val source = buildSnapshot(relationsOverride = seed.relations.map(r =>
      if r.table == shape.table then MySqlFormat3RelationSnapshot(shape.table, Vector(history)) else r))
    assertTrue(source.canonicalByteLength > 65536L)
    val common = graph(source)
    val draft = MySqlB1CommonGraphColdCodec.draft(common.toResumeAssertions, source, source.allocationBudget)
    val decoded = MySqlB1CommonGraphColdCodec.decode(draft, inputs)
    MySqlFormat3B1ContextCorrespondence.requireCorrespondence(decoded, source, inputs, source.allocationBudget)
    assertSame(source, decoded.format3Snapshot.get)
    assertEquals(1, source.relation(shape.table).rows.size)
    assertEquals(70000, source.relation(shape.table).rows.head(shape.columns.indexWhere(_.name == "carrier_key"))
      .asInstanceOf[MySqlFormat3Cell.Binary].value.length)

  @Test def trailingComponentFailureRetainsOriginalTypedGraphAndCause(): Unit =
    val source = buildSnapshot()
    val original = graph(source).toResumeAssertions
    val bad = new com.wi.floatingpod.server.B1ResumeRecord.CommonGraphAssertions(original.manifestEncoding(),
      original.manifestBytes(), original.bindingEncoding(), original.bindingBytes(), original.repositoryEncoding(),
      original.repositoryBytes() ++ Array[Byte](0))
    val draft = MySqlB1CommonGraphColdCodec.draft(bad, source, source.allocationBudget)
    val failure = assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraphColdCodec.decode(draft, inputs))
    assertSame(failure, draft.failure)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertNotNull(draft.manifest)
    assertNotNull(draft.binding)
    assertNotNull(draft.repositoryBytes)
    assertNull(draft.result)
    assertFalse(source.allocationBudget.isClosed)

  @Test def nearCapacityRejectsBeforeComponentCopyAndPreservesSharedRoot(): Unit =
    val source = buildSnapshot()
    val original = graph(source).toResumeAssertions
    val root = source.allocationBudget
    val draft = MySqlB1CommonGraphColdCodec.draft(original, source, root)
    root.reserveTemporaryMemory(root.availableMemoryBytes - 1L, "actual live fixture workspace")
    val before = root.accountedMemoryBytes
    val failure = assertThrows(classOf[java.sql.SQLDataException], () => MySqlB1CommonGraphColdCodec.decode(draft, inputs))
    assertSame(failure, draft.failure)
    assertEquals(before, root.accountedMemoryBytes)
    assertNull(draft.manifestBytes)
    assertFalse(root.isClosed)

  @Test def foreignProfileAndClosedRootCannotStartColdDecode(): Unit =
    val source = buildSnapshot()
    val assertion = graph(source).toResumeAssertions
    val foreign = new MySqlFormat3ReadBudget(SourceLimitProfile.decode(profile.canonicalBytes()), profile.maxSqlPacketBytes())
    assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraphColdCodec.draft(assertion, source, foreign))
    source.allocationBudget.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraphColdCodec.draft(assertion, source, source.allocationBudget))

  @Test def freshFullTargetRejectsExtraTypedRelationWithUnchangedLegacyEffect(): Unit =
    val before = buildSnapshot()
    val source = graph(before)
    val shape = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val key = Array[Byte](1, 2, 3, 4)
    val row = shape.columns.map(defaultCell)
      .updated(shape.columns.indexWhere(_.name == "carrier_key"), binary(key))
      .updated(shape.columns.indexWhere(_.name == "carrier_key_hash"), binary(MessageDigest.getInstance("SHA-256").digest(key)))
    val relations = before.relations.map(r =>
      if r.table == shape.table then MySqlFormat3RelationSnapshot(shape.table, Vector(row)) else r)
    before.allocationBudget.recordMaterializedSnapshot(relations)
    val target = new MySqlFormat3FullSnapshot(before.schema, before.connection, before.observationScope,
      before.maxAllowedPacketBytes, profile, relations, before.allocationBudget)
    val targetCommon = graph(target)
    assertTrue(source.repository.exactlyEquals(targetCommon.repository))
    assertNotEquals(source.repositoryBytes, targetCommon.repositoryBytes) // full20 commitment was genuinely reencoded
    assertEquals(source.manifestBytes, targetCommon.manifestBytes)
    assertEquals(source.bindingBytes, targetCommon.bindingBytes)
    val decodedTarget = MySqlB1CommonGraphColdCodec.decode(
      MySqlB1CommonGraphColdCodec.draft(targetCommon.toResumeAssertions, target, before.allocationBudget), inputs)
    assertTrue(decodedTarget.exactlyEquals(targetCommon))
    val writer = MySqlFreshFormat3WriterTarget(bindingUuid, 1L, Some(bootUuid), databaseNow.plusSeconds(30), plan.prepareWriteUuid)
    val originalEffect = MySqlFreshFormat3Graph.effectFingerprint("PREPARE", plan, before, source.repository, source.repository, writer)
    val sameEffect = MySqlFreshFormat3Graph.effectFingerprint("PREPARE", plan, before, source.repository, targetCommon.repository, writer)
    assertEquals(originalEffect, sameEffect)
    val cause = assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlCertifiedRecoveryInspectionActor.requireFreshTargetRelations(before, target, before.allocationBudget))
    assertTrue(cause.getMessage.endsWith(shape.table))
    assertTrue(cause.getStackTrace.nonEmpty)
    assertFalse(before.allocationBudget.isClosed)
    assertSame(row, target.relation(shape.table).rows.head)
    MySqlCertifiedRecoveryInspectionActor.requireFreshTargetRelations(before, before, before.allocationBudget)

  @Test def deploymentReceiptFull26ColdCommonGraphReconstructsActualDecodedImageAndUnknownHistory(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val inputs = MySqlB1CommonGraphTestFixtures.inputsWithSnapshotBudget(500_000L)
    val root = MySqlScriptSchemaDataTestFixture.root(inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    var live: MySqlB1CommonGraph = null
    var cold: MySqlB1CommonGraph = null
    var payload: MySqlScriptSchemaCanonicalPayload = null
    var decodedSnapshot: MySqlScriptSchemaSnapshot = null
    try
      val (_, target, handoff) = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(inputs, root, largeHistoryBytes = 70_000)
      data = handoff
      live = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target)
      assertTrue(target.canonicalByteLength > MySqlB1CommonGraph.MaxCanonicalBytes)
      assertTrue(live.repositoryBytes.size < MySqlB1CommonGraph.MaxCanonicalBytes)
      payload = MySqlScriptSchemaSnapshotCodec.encode(target, root)
      decodedSnapshot = MySqlScriptSchemaSnapshotCodec.decode(payload, inputs, root)
      val draft = MySqlB1CommonGraphColdCodec.deploymentReceiptFull26Draft(
        live.toResumeAssertions, decodedSnapshot, inputs, root)
      cold = MySqlB1CommonGraphColdCodec.decodeDeploymentReceiptFull26(draft)
      assertTrue(cold.exactlyEquals(live))
      assertSame(decodedSnapshot, cold.deploymentReceiptSnapshot.get)
      assertTrue(cold.matchesResumeAssertions(live.toResumeAssertions))
      val operations = cold.deploymentReceiptSnapshot.get.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable)
      val shape = inputs.forTable(MySqlDeploymentReceiptSchemaDefinition.OperationTable)
      val unknownIndex = shape.columns.indexWhere(_.name == "diagnostic_bytes")
      val actorIndex = shape.columns.indexWhere(_.name == "actor_uuid")
      assertEquals(2, operations.rows.size)
      assertFalse(operations.rows(0)(actorIndex).asInstanceOf[MySqlFormat3Cell.Binary].value.equals(
        operations.rows(1)(actorIndex).asInstanceOf[MySqlFormat3Cell.Binary].value))
      assertTrue(operations.rows(1)(unknownIndex).isInstanceOf[MySqlFormat3Cell.Binary])
      assertEquals(2L, cold.deploymentReceiptSnapshot.get
        .relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(2)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertThrows(classOf[IllegalStateException], () => cold.repository)
    finally
      if cold != null then cold.storageBudget.foreach(_.closeScopedReadBudget())
      if live != null then live.storageBudget.foreach(_.closeScopedReadBudget())
      if decodedSnapshot != null then decodedSnapshot.allocationBudget.closeScopedReadBudget()
      if payload != null then payload.storageBudget.closeScopedReadBudget()
      if data != null then data.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()

  @Test def deploymentReceiptFull26ColdCommonRejectsCorruptDomainLengthHashAndTrailingBytes(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val root = MySqlScriptSchemaDataTestFixture.root(Full26Inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    var live: MySqlB1CommonGraph = null
    try
      val (_, target, handoff) = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(Full26Inputs, root)
      data = handoff
      live = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target)
      val assertions = live.toResumeAssertions
      def altered(transform: Array[Byte] => Array[Byte]): B1ResumeRecord.CommonGraphAssertions =
        new B1ResumeRecord.CommonGraphAssertions(assertions.manifestEncoding(), assertions.manifestBytes(),
          assertions.bindingEncoding(), assertions.bindingBytes(), assertions.repositoryEncoding(),
          transform(assertions.repositoryBytes()))
      def rejected(value: B1ResumeRecord.CommonGraphAssertions, expected: Class[? <: Throwable]): Unit =
        val draft = MySqlB1CommonGraphColdCodec.deploymentReceiptFull26Draft(value, target, Full26Inputs, root)
        val failure = assertThrows(classOf[Throwable], () => MySqlB1CommonGraphColdCodec.decodeDeploymentReceiptFull26(draft))
        assertTrue(expected.isInstance(failure), s"expected ${expected.getName}, got ${failure.getClass.getName}")
        assertSame(failure, draft.failure)
        assertNull(draft.result)
        assertTrue(failure.getStackTrace.nonEmpty)

      rejected(altered { bytes => bytes(4) = (bytes(4) ^ 1).toByte; bytes }, classOf[IllegalArgumentException])
      rejected(altered { bytes =>
        var offset = 0
        def skipText(): Unit =
          val length = java.nio.ByteBuffer.wrap(bytes, offset, 4).getInt
          offset += 4 + length
        skipText(); offset += 4; skipText(); skipText(); skipText()
        java.nio.ByteBuffer.wrap(bytes).putLong(offset, target.canonicalByteLength + 1L)
        bytes
      }, classOf[IllegalArgumentException])
      rejected(altered { bytes => bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 1).toByte; bytes },
        classOf[IllegalArgumentException])
      rejected(altered { bytes => bytes ++ Array[Byte](1) }, classOf[IllegalArgumentException])
      rejected(altered { bytes => java.util.Arrays.copyOf(bytes, bytes.length - 1) }, classOf[java.io.EOFException])
      assertFalse(root.isClosed)
    finally
      if live != null then live.storageBudget.foreach(_.closeScopedReadBudget())
      if data != null then data.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()

  @Test def deploymentReceiptFull26ColdCommonRejectsSiblingRootAndFull24Layout(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val ledger = new MySqlFormat3AllocationLedger(SourceProfile.maxRetainedBytes())
    val root = MySqlScriptSchemaDataTestFixture.root(Full26Inputs, ledger)
    val sibling = MySqlScriptSchemaDataTestFixture.root(Full26Inputs, ledger)
    var data: MySqlScriptSchemaB1Graph.Data = null
    var live: MySqlB1CommonGraph = null
    try
      val (_, target, handoff) = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(Full26Inputs, root)
      data = handoff
      live = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target)
      val assertions = live.toResumeAssertions
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlB1CommonGraphColdCodec.deploymentReceiptFull26Draft(assertions, target, Full26Inputs, sibling))

      val full24Root = MySqlScriptSchemaDataTestFixture.root()
      val full24 = MySqlScriptSchemaDataTestFixture.snapshot(full24Root,
        MySqlScriptSchemaDataTestFixture.completeRelations)
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlB1CommonGraphColdCodec.deploymentReceiptFull26Draft(assertions, full24, Inputs, full24Root))
      full24Root.closeRootReadBudget()
      assertFalse(root.isClosed)
      assertFalse(sibling.isClosed)
    finally
      if live != null then live.storageBudget.foreach(_.closeScopedReadBudget())
      if data != null then data.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()
      sibling.closeRootReadBudget()
