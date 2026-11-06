package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.security.MessageDigest
import java.sql.{ResultSetMetaData, Types}
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 直接验证 Fresh Format3 全二十关系图、29列独立manifest和完整效果摘要。 */
final class MySqlFreshFormat3GraphTest:
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

  private def requireFreshSeed(snapshot: MySqlFormat3FullSnapshot): Unit =
    MySqlFreshFormat3Graph.requireFreshInitialSeed(snapshot, MySqlFullRepositorySnapshot.empty, manifest,
      originalWriter, originalWriter.writerEpoch, inputs)

  private def assertLegacyNineProjectionEmpty(repository: MySqlFullRepositorySnapshot): Unit =
    assertTrue(repository.pods.isEmpty)
    assertTrue(repository.entitlements.isEmpty)
    assertTrue(repository.recovery.binding.isEmpty)
    assertTrue(repository.recovery.locators.isEmpty)
    assertTrue(repository.recovery.chunks.isEmpty)
    assertTrue(repository.recovery.intents.isEmpty)
    assertTrue(repository.bootstrapIntents.isEmpty)

  @Test def freshInitialSeedAcceptsOnlyTheApprovedManifestAndUnownedWriterControlRows(): Unit =
    val snapshot = buildSnapshot()
    requireFreshSeed(snapshot)
    assertEquals(1, snapshot.relation("fp_schema_manifest").rows.size)
    assertEquals(1, snapshot.relation("fp_runtime_writer").rows.size)
    assertTrue(MySqlFormat3RelationShapes.canonicalOrder.filterNot(shape =>
      Set("fp_schema_manifest", "fp_runtime_writer").contains(shape.table)).forall(shape => snapshot.relation(shape.table).rows.isEmpty))

  @Test def everyFormat3RelationAddedBeyondTheLegacyNineMustBeEmptyWhileThatProjectionIsEmpty(): Unit =
    val initial = buildSnapshot()
    val repository = MySqlFullRepositorySnapshot.empty
    assertLegacyNineProjectionEmpty(repository)
    val legacyNineRelations = Set("fp_schema_manifest", "fp_runtime_writer", "physical_recovery_binding",
      "floating_pod_state", "player_entitlement", "binding_bootstrap_intent", "physical_recovery_intent",
      "physical_recovery_locator", "physical_recovery_locator_chunk")
    val sharedLegacyRelations = legacyNineRelations.intersect(MySqlFormat3RelationShapes.canonicalOrder.map(_.table).toSet)
    assertEquals(8, sharedLegacyRelations.size, "Format3 removes legacy physical_recovery_locator_chunk")
    val additionalRelations = MySqlFormat3RelationShapes.canonicalOrder.filterNot(shape => sharedLegacyRelations.contains(shape.table))
    assertEquals(MySqlFormat3RelationShapes.ExpectedRelationCount - sharedLegacyRelations.size, additionalRelations.size)
    additionalRelations.foreach { shape =>
      val populated = replaceRelation(initial, shape.table, relation =>
        relation.copy(rows = Vector(shape.columns.map(defaultCell))))
      val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlFreshFormat3Graph.requireFreshInitialSeed(populated, repository, manifest, originalWriter,
          originalWriter.writerEpoch, inputs), shape.table)
      assertTrue(failure.getMessage.contains(shape.table), failure.getMessage)
      assertLegacyNineProjectionEmpty(repository)
    }

  @Test def prepareReplayRejectsHistoryAddedAfterTheInitialFull20PlanWasObserved(): Unit =
    val plannedBefore = buildSnapshot()
    requireFreshSeed(plannedBefore)
    val historyShape = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val replayRead = replaceRelation(plannedBefore, historyShape.table, relation =>
      relation.copy(rows = Vector(historyShape.columns.map(defaultCell))))
    val failure = assertThrows(classOf[java.sql.SQLDataException], () => requireFreshSeed(replayRead))
    assertTrue(failure.getMessage.contains(historyShape.table), failure.getMessage)

  @Test def freshTransitionProjectionDecodesExactSeedPreparedAndCompletedRowsFromFormat3(): Unit =
    val emptyProjection = MySqlFreshFormat3TransitionProjection.project(buildSnapshot())
    assertTrue(emptyProjection.exactlyEquals(MySqlFullRepositorySnapshot.empty))

    val sourceIdentity = "FP_TEST_SOURCE_IDENTITY_V1"
    val receiptSha = Vector.fill[Byte](32)(0x61.toByte)
    val prepared = MySqlBootstrapSnapshotTransitions.prepareFresh(emptyProjection, plan, sourceIdentity, receiptSha)
    val preparedSnapshot = buildSnapshot(binding = prepared.recovery.binding, bootstrapIntents = prepared.bootstrapIntents)
    val preparedProjection = MySqlFreshFormat3TransitionProjection.project(preparedSnapshot)
    assertTrue(preparedProjection.exactlyEquals(prepared))

    val markerAckSha = Vector.fill[Byte](32)(0x62.toByte)
    val completed = MySqlBootstrapSnapshotTransitions.complete(prepared, plan, sourceIdentity, receiptSha, markerAckSha)
    val completedSnapshot = buildSnapshot(binding = completed.recovery.binding, bootstrapIntents = completed.bootstrapIntents)
    assertTrue(MySqlFreshFormat3TransitionProjection.project(completedSnapshot).exactlyEquals(completed))

  @Test def freshProjectionRejectsBindingRowsFromAnyPhysicalFormatOtherThanTwo(): Unit =
    val wrongFormat = buildSnapshot(binding = Some(PhysicalRecoveryBinding(bindingUuid,
      PhysicalRecoveryCutoverState.LegacyUncertified, 3)))
    val failure = assertThrows(classOf[java.sql.SQLDataException], () => {
      MySqlFreshFormat3TransitionProjection.project(wrongFormat)
      ()
    })
    assertTrue(failure.getMessage.contains("physical format 2"), failure.getMessage)

  @Test def everyTypedB1CellMustMatchTheExactFreshTransitionIntent(): Unit =
    val sourceIdentity = "FP_TEST_SOURCE_IDENTITY_V1"
    val receiptSha = Vector.fill[Byte](32)(0x61.toByte)
    val expected = MySqlBootstrapSnapshotTransitions.prepareFresh(MySqlFullRepositorySnapshot.empty, plan,
      sourceIdentity, receiptSha).bootstrapIntents.head
    val snapshot = buildSnapshot(binding = Some(PhysicalRecoveryBinding(bindingUuid,
      PhysicalRecoveryCutoverState.LegacyUncertified, 2)), bootstrapIntents = Vector(expected))
    val shape = MySqlFormat3RelationShapes.forTable("binding_bootstrap_intent")
    assertEquals(11, shape.columns.size)
    val original = snapshot.relation("binding_bootstrap_intent").rows.head
    shape.columns.indices.foreach { index =>
      val changedCell = original(index) match
        case MySqlFormat3Cell.SignedInteger(value) => MySqlFormat3Cell.SignedInteger(Math.addExact(value, 1L))
        case MySqlFormat3Cell.Text(value) => MySqlFormat3Cell.Text(value + "-changed")
        case MySqlFormat3Cell.Binary(value) =>
          val bytes = value.copyBytes(relationBudget(snapshot), "test changed B1 field")
          bytes(0) = (bytes(0) ^ 0x01).toByte
          binary(bytes)
        case MySqlFormat3Cell.SqlNull => binary(Array.fill[Byte](32)(0x7e.toByte))
        case other => throw new AssertionError("unexpected B1 cell " + other)
      val changed = replaceRelation(snapshot, "binding_bootstrap_intent", relation =>
        relation.copy(rows = Vector(relation.rows.head.updated(index, changedCell))))
      assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlFreshFormat3Graph.requireBootstrapIntentsMatch(changed, Vector(expected)), shape.columns(index).name)
    }

  @Test def freshProjectionRejectsEveryNonB1NonControlFormat3RelationInsteadOfDroppingRows(): Unit =
    val initial = buildSnapshot()
    val allowed = Set("fp_schema_manifest", "fp_runtime_writer", "physical_recovery_binding", "binding_bootstrap_intent")
    val forbidden = MySqlFormat3RelationShapes.canonicalOrder.filterNot(shape => allowed.contains(shape.table))
    assertEquals(16, forbidden.size)
    forbidden.foreach { shape =>
      val populated = replaceRelation(initial, shape.table, relation =>
        relation.copy(rows = Vector(shape.columns.map(defaultCell))))
      val failure = assertThrows(classOf[java.sql.SQLDataException], () => {
        MySqlFreshFormat3TransitionProjection.project(populated)
        ()
      }, shape.table)
      assertTrue(failure.getMessage.contains(shape.table), failure.getMessage)
    }

  @Test def everyIndependentAppendedManifestCellIsComparedAgainstTheExpectedInputs(): Unit =
    val snapshot = buildSnapshot()
    MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
    inputs.manifestLabels.foreach { label =>
      val shape = MySqlFormat3RelationShapes.forTable("fp_schema_manifest")
      val index = shape.columns.indexWhere(_.name == label)
      assertTrue(index >= 14)
      val changed = replaceRelation(snapshot, "fp_schema_manifest", relation =>
        relation.copy(rows = Vector(relation.rows.head.updated(index, relation.rows.head(index) match
          case MySqlFormat3Cell.Text(value) => MySqlFormat3Cell.Text(value + "-changed")
          case MySqlFormat3Cell.SignedInteger(value) => MySqlFormat3Cell.SignedInteger(Math.addExact(value, 1L))
          case MySqlFormat3Cell.Binary(value) =>
            val bytes = value.copyBytes(relationBudget(snapshot), "test changed manifest cell")
            bytes(0) = (bytes(0) ^ 0x01).toByte
            binary(bytes)
          case other => throw new AssertionError("unexpected independent manifest cell " + other)
        ))))
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlFreshFormat3Graph.requireManifestMatches(changed, manifest, inputs), label)
    }

  @Test def completeGraphAndEffectFingerprintRetainRowsOutsideTheLegacyRepositoryProjection(): Unit =
    val beforeFull20 = buildSnapshot(withObligation = true)
    val obligation = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val textColumn = obligation.columns.indexWhere(column => column.kind == MySqlFormat3SqlKind.Text && !obligation.primaryKey.contains(column.name))
    assertTrue(textColumn >= 0)
    val changedFull20 = replaceRelation(beforeFull20, obligation.table, relation =>
      relation.copy(rows = Vector(relation.rows.head.updated(textColumn, MySqlFormat3Cell.Text("changed-history")))))
    val beforeGraph = graph(beforeFull20)
    val changedGraph = graph(changedFull20)
    assertTrue(beforeGraph.repository.exactlyEquals(changedGraph.repository))
    assertFalse(beforeGraph.exactlyEquals(changedGraph))
    assertNotEquals(beforeGraph.repositorySha256, changedGraph.repositorySha256)

    val beforeRepository = MySqlFullRepositorySnapshot.empty
    val sourceIdentity = "FP_TEST_SOURCE_IDENTITY_V1"
    val provisionSha = Vector.fill[Byte](32)(0x61.toByte)
    val prepared = MySqlBootstrapSnapshotTransitions.prepareFresh(beforeRepository, plan, sourceIdentity, provisionSha)
    val lease = databaseNow.plusMinutes(2)
    val normalizedWriter = MySqlFreshFormat3WriterTarget(bindingUuid, 1L, Some(bootUuid), lease, plan.prepareWriteUuid)
    val baseEffect = MySqlFreshFormat3Graph.effectFingerprint("PREPARE", plan, beforeFull20,
      beforeRepository, prepared, normalizedWriter)
    val changedEffect = MySqlFreshFormat3Graph.effectFingerprint("PREPARE", plan, changedFull20,
      beforeRepository, prepared, normalizedWriter)
    assertNotEquals(baseEffect, changedEffect)

    val targetWriter = originalWriter.copy(bindingUuid = bindingUuid, writerEpoch = 1L, ownerBootUuid = Some(bootUuid),
      leaseUntil = lease, lastOperationUuid = Some(plan.prepareWriteUuid),
      lastFingerprint = Some(Vector.fill[Byte](32)(0x63.toByte)))
    assertThrows(classOf[java.sql.SQLDataException], () => {
      MySqlFreshFormat3Graph.targetSnapshot(beforeFull20, beforeRepository, prepared, targetWriter)
      ()
    }, "Fresh B1 cannot project away unrelated Format3 history")

  private def relationBudget(snapshot: MySqlFormat3FullSnapshot): MySqlFormat3ReadBudget = snapshot.allocationBudget
