package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstWorldAdoptionPlanData
import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** B1 writer settlement 的真实 Full27 DATA 边界；不模拟进程退出或 session barrier。 */
final class MySqlDbFirstBootstrapWriterGraphTest:
  import MySqlScriptSchemaDataTestFixture.*

  private val Configuration = DeploymentReceiptStaticTestData.defaultProfile()
  private val Rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(Configuration)
  private val Inputs = MySqlScriptSchemaInputs.createDbFirstFull27(Rendered)
  private val Full26Inputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
    Configuration.schema, MySqlDeploymentReceiptSchemaProfile.render(Configuration).sha256,
    Configuration.sourceLimitProfile, Configuration.publicationBudget, Configuration.deploymentBudget)
  private val QuotaMaximum = Configuration.quotaMaximumRecords
  private val BootOne = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c1")
  private val BootTwo = UUID.fromString("28a0b51d-4ff4-4a71-a27f-d67f48d3c7c2")
  private val Plan = new DbFirstWorldAdoptionPlanData(
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c701"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c702"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c703"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c704"),
    DeploymentReceiptStaticTestData.binding, 2, Connection.serverUuid, Inputs.schema, 6,
    Inputs.ddlSha256, DeploymentReceiptStaticTestData.provisioning)
  private val Source = "world://managed/floatingpod-primary"
  private val Receipt = Vector.fill[Byte](32)(0x44.toByte)
  private val MarkerAck = Vector.fill[Byte](32)(0x55.toByte)
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private val SeedWriter = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private val PreparedTargetWriter = MySqlFreshFormat3WriterTarget(
    Plan.bindingUuid(), 1L, Some(BootOne), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private def root(): MySqlFormat3ReadBudget = Inputs.newAllocationRoot(Inputs.budget.packetBytes,
    new MySqlFormat3AllocationLedger(Inputs.sourceLimitProfile.maxRetainedBytes()))

  private def replaceCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = Inputs.forTable(table).columns.indexWhere(_.name == name)
    require(index >= 0, "the actual Full27 column must exist")
    row.updated(index, value)

  private def initialRows(): Vector[MySqlFormat3RelationSnapshot] =
    val old = full26GenesisRelations(Full26Inputs, catalogPresent = false).map(relation => relation.table -> relation).toMap
    val configured = Inputs.canonicalOrder.map { shape =>
      shape.table match
        case MySqlDbFirstSchemaDefinition.SnapshotTable => MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
        case MySqlDeploymentReceiptSchemaDefinition.QuotaTable =>
          val row = shape.columns.map(_.name match
            case "namespace" => binary(java.util.HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))
            case "max_records" => MySqlFormat3Cell.SignedInteger(QuotaMaximum)
            case "used_records" => MySqlFormat3Cell.SignedInteger(0L)
            case "quota_revision" => MySqlFormat3Cell.SignedInteger(0L)
            case other => throw new AssertionError("unexpected quota column " + other))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case "fp_schema_manifest" =>
          var row = old(shape.table).rows.head
          row = replaceCell(row, shape.table, "format_version", MySqlFormat3Cell.SignedInteger(6L))
          row = replaceCell(row, shape.table, "ddl_sha256", binary(java.util.HexFormat.of().parseHex(Rendered.sha256)))
          row = replaceCell(row, shape.table, "provisioning_uuid", binary(uuidBytes(Plan.provisioningUuid())))
          row = replaceCell(row, shape.table, "expected_binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "source_provenance_kind", MySqlFormat3Cell.Text("MANAGED_INSTALL"))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case "fp_runtime_writer" =>
          var row = old(shape.table).rows.head
          row = replaceCell(row, shape.table, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "lease_until", MySqlFormat3Cell.DateTime(Now))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case name => old.getOrElse(name, MySqlFormat3RelationSnapshot(name, Vector.empty))
    }
    configured.map { relation =>
      if relation.table == "physical_recovery_binding" || relation.table == "binding_bootstrap_intent" then
        relation.copy(rows = Vector.empty)
      else relation
    }

  private def capture(rows: Vector[MySqlFormat3RelationSnapshot],
      allocation: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Inputs.schema,
      Connection.copy(catalog = Inputs.schema), Scope, Inputs.budget.packetBytes, Inputs, rows, allocation)

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell =
    val index = Inputs.forTable(table).columns.indexWhere(_.name == column)
    require(index >= 0, "the actual Full27 column must exist")
    snapshot.relation(table).rows.head(index)

  private def text(value: MySqlFormat3Cell): String = value match
    case MySqlFormat3Cell.Text(actual) => actual
    case _ => throw new AssertionError("expected typed text")

  private def uuid(value: MySqlFormat3Cell): UUID = value match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
      val buffer = ByteBuffer.allocate(16)
      var index = 0
      while index < 16 do
        buffer.put(bytes.byteAt(index))
        index += 1
      buffer.flip()
      new UUID(buffer.getLong(), buffer.getLong())
    case _ => throw new AssertionError("expected exact binary UUID")

  private def byteVector(value: MySqlFormat3Cell): Vector[Byte] = value match
    case MySqlFormat3Cell.Binary(bytes) => Vector.tabulate(bytes.length)(bytes.byteAt)
    case _ => throw new AssertionError("expected exact binary receipt bytes")

  private def writer(snapshot: MySqlScriptSchemaSnapshot,
      databaseNow: LocalDateTime): MySqlBusinessWriterRow =
    val owner = cell(snapshot, "fp_runtime_writer", "owner_boot_uuid") match
      case MySqlFormat3Cell.SqlNull => None
      case value: MySqlFormat3Cell.Binary => Some(uuid(value))
      case _ => throw new AssertionError("writer owner must be typed UUID or SQL NULL")
    val operation = cell(snapshot, "fp_runtime_writer", "last_commit_operation_uuid") match
      case MySqlFormat3Cell.SqlNull => None
      case value: MySqlFormat3Cell.Binary => Some(uuid(value))
      case _ => throw new AssertionError("writer operation must be typed UUID or SQL NULL")
    val fingerprint = cell(snapshot, "fp_runtime_writer", "last_commit_fingerprint") match
      case MySqlFormat3Cell.SqlNull => None
      case value: MySqlFormat3Cell.Binary => Some(byteVector(value))
      case _ => throw new AssertionError("writer fingerprint must be typed binary or SQL NULL")
    val epoch = cell(snapshot, "fp_runtime_writer", "writer_epoch") match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new AssertionError("writer epoch must be typed integer")
    val lease = cell(snapshot, "fp_runtime_writer", "lease_until") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => throw new AssertionError("writer lease must be typed datetime")
    MySqlBusinessWriterRow(uuid(cell(snapshot, "fp_runtime_writer", "binding_uuid")),
      epoch, owner, lease, operation, fingerprint, databaseNow)

  private def withCatalog(snapshot: MySqlScriptSchemaSnapshot): Vector[MySqlFormat3RelationSnapshot] =
    val catalogRow = Inputs.forTable("fp_script_catalog").columns.map(_.name match
      case "binding_uuid" => binary(uuidBytes(Plan.bindingUuid()))
      case "catalog_revision" => MySqlFormat3Cell.SignedInteger(0L)
      case "snapshot_bytes" => binary(Array.emptyByteArray)
      case "snapshot_sha256" => binary(digest(Array.emptyByteArray))
      case "next_version" => MySqlFormat3Cell.SignedInteger(1L)
      case "last_mutation_operation_uuid" => MySqlFormat3Cell.SqlNull
      case other => throw new AssertionError("unexpected catalog column " + other))
    snapshot.relations.map { relation =>
      if relation.table == "fp_script_catalog" then relation.copy(rows = Vector(catalogRow)) else relation
    }

  private def closeTarget(snapshot: MySqlScriptSchemaSnapshot): Unit =
    if snapshot != null && snapshot.allocationBudget.isScopedReadBudget && !snapshot.allocationBudget.isClosed then
      snapshot.allocationBudget.closeScopedReadBudget()

  @Test def preparedAndCertifiedB1WriterSettlementKeepExactPhaseSlotsAndOnlyChangeWriter(): Unit =
    val allocation = root()
    var prepared: MySqlScriptSchemaSnapshot = null
    var preparedWithCatalog: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    var released: MySqlScriptSchemaSnapshot = null
    var preparedHandoff: MySqlScriptSchemaSnapshot = null
    var ownedCompleteHandoff: MySqlScriptSchemaSnapshot = null
    var certifiedHandoff: MySqlScriptSchemaSnapshot = null
    try
      val beforePrepare = capture(initialRows(), allocation)
      prepared = MySqlDbFirstBootstrapGraph.prepareTarget(beforePrepare, Inputs, allocation, Plan, Source,
        Receipt, SeedWriter, PreparedTargetWriter, QuotaMaximum)
      preparedWithCatalog = capture(withCatalog(prepared), allocation)

      val preparedWriter = writer(preparedWithCatalog, Now.plusMinutes(2))
      val preparedHandoffFence = MySqlWriterFenceRow(Plan.bindingUuid(), preparedWriter.writerEpoch + 1L,
        Some(BootTwo), Now.plusMinutes(3), preparedWriter.lastOperationUuid, preparedWriter.lastFingerprint)
      preparedHandoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(preparedWithCatalog, Inputs, allocation,
        Plan, Source, Receipt, None, preparedWriter, preparedHandoffFence, QuotaMaximum)
      MySqlDbFirstBootstrapWriterGraph.requireHandoffTransition(preparedWithCatalog, preparedHandoff, Inputs,
        allocation, Plan, Source, Receipt, None, preparedWriter, preparedHandoffFence, QuotaMaximum)
      assertEquals(PhysicalRecoveryCutoverState.LegacyUncertified.databaseValue,
        text(cell(preparedHandoff, "physical_recovery_binding", "cutover_state")))
      assertEquals(Some(BootOne), preparedWriter.ownerBootUuid)
      assertEquals(Plan.prepareWriteUuid(), uuid(cell(preparedHandoff, "fp_runtime_writer", "last_commit_operation_uuid")))
      assertTrue(preparedHandoff.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.isEmpty)

      val unexpiredWriter = preparedWriter.copy(databaseNow = Now)
      val unexpiredFence = preparedHandoffFence.copy(epoch = unexpiredWriter.writerEpoch + 1L,
        leaseUntil = Now.plusMinutes(2))
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlDbFirstBootstrapWriterGraph.handoffTarget(
        preparedWithCatalog, Inputs, allocation, Plan, Source, Receipt, None, unexpiredWriter, unexpiredFence, QuotaMaximum))

      val actualPreparedWriter = writer(preparedWithCatalog, Now)
      val certifyWriter = MySqlFreshFormat3WriterTarget(actualPreparedWriter.bindingUuid,
        actualPreparedWriter.writerEpoch, actualPreparedWriter.ownerBootUuid,
        actualPreparedWriter.leaseUntil, Plan.certifyWriteUuid())
      certified = MySqlDbFirstBootstrapGraph.certifyTarget(preparedWithCatalog, Inputs, allocation, Plan,
        Source, Receipt, MarkerAck, actualPreparedWriter, certifyWriter, QuotaMaximum)
      val certifiedWriter = writer(certified, Now)
      val completeRestartNow = Now.plusMinutes(2)
      val expiredOwnedCertifiedWriter = writer(certified, completeRestartNow)
      val completeHandoffFence = MySqlWriterFenceRow(Plan.bindingUuid(), expiredOwnedCertifiedWriter.writerEpoch + 1L,
        Some(BootTwo), completeRestartNow.plusMinutes(1), expiredOwnedCertifiedWriter.lastOperationUuid,
        expiredOwnedCertifiedWriter.lastFingerprint)
      ownedCompleteHandoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(certified, Inputs, allocation, Plan,
        Source, Receipt, Some(MarkerAck), expiredOwnedCertifiedWriter, completeHandoffFence, QuotaMaximum)
      MySqlDbFirstBootstrapWriterGraph.requireHandoffTransition(certified, ownedCompleteHandoff, Inputs, allocation,
        Plan, Source, Receipt, Some(MarkerAck), expiredOwnedCertifiedWriter, completeHandoffFence, QuotaMaximum)
      assertEquals(Some(BootOne), expiredOwnedCertifiedWriter.ownerBootUuid)
      assertEquals(Some(BootTwo), writer(ownedCompleteHandoff, completeRestartNow).ownerBootUuid)
      assertEquals(Plan.certifyWriteUuid(), uuid(cell(ownedCompleteHandoff, "fp_runtime_writer", "last_commit_operation_uuid")))
      val releaseWriter = certifiedWriter.copy(ownerBootUuid = None, leaseUntil = Now.minusNanos(1000L))
      released = MySqlDbFirstBootstrapWriterGraph.releaseTarget(certified, Inputs, allocation, Plan, Source,
        Receipt, Some(MarkerAck), certifiedWriter, releaseWriter, QuotaMaximum)
      MySqlDbFirstBootstrapWriterGraph.requireReleaseTransition(certified, released, Inputs, allocation, Plan,
        Source, Receipt, Some(MarkerAck), certifiedWriter, releaseWriter, QuotaMaximum)
      assertEquals(certifiedWriter.writerEpoch, writer(released, Now).writerEpoch)
      assertEquals(None, writer(released, Now).ownerBootUuid)
      assertEquals(Now.minusNanos(1000L), writer(released, Now).leaseUntil)
      assertEquals(certifiedWriter.lastOperationUuid, writer(released, Now).lastOperationUuid)
      assertEquals(certifiedWriter.lastFingerprint, writer(released, Now).lastFingerprint)

      val restartNow = Now.plusMinutes(2)
      val actualReleasedWriter = writer(released, restartNow)
      val nextFence = MySqlWriterFenceRow(Plan.bindingUuid(), actualReleasedWriter.writerEpoch + 1L,
        Some(BootTwo), restartNow.plusMinutes(1), actualReleasedWriter.lastOperationUuid,
        actualReleasedWriter.lastFingerprint)
      certifiedHandoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(released, Inputs, allocation, Plan,
        Source, Receipt, Some(MarkerAck), actualReleasedWriter, nextFence, QuotaMaximum)
      MySqlDbFirstBootstrapWriterGraph.requireHandoffTransition(released, certifiedHandoff, Inputs,
        allocation, Plan, Source, Receipt, Some(MarkerAck), actualReleasedWriter, nextFence, QuotaMaximum)
      assertEquals(PhysicalRecoveryCutoverState.Certified.databaseValue,
        text(cell(certifiedHandoff, "physical_recovery_binding", "cutover_state")))
      assertEquals(Plan.certifyWriteUuid(), uuid(cell(certifiedHandoff, "fp_runtime_writer", "last_commit_operation_uuid")))
      assertEquals(27, certifiedHandoff.relations.size)
    finally
      closeTarget(certifiedHandoff)
      closeTarget(ownedCompleteHandoff)
      closeTarget(preparedHandoff)
      closeTarget(released)
      closeTarget(certified)
      closeTarget(prepared)
      closeTarget(preparedWithCatalog)
      allocation.closeRootReadBudget()

  @Test def releaseRejectsPreparedPhaseAndHandoffCannotReplaceTheReceiptSlot(): Unit =
    val allocation = root()
    var prepared: MySqlScriptSchemaSnapshot = null
    var preparedWithCatalog: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(initialRows(), allocation)
      prepared = MySqlDbFirstBootstrapGraph.prepareTarget(before, Inputs, allocation, Plan, Source,
        Receipt, SeedWriter, PreparedTargetWriter, QuotaMaximum)
      preparedWithCatalog = capture(withCatalog(prepared), allocation)
      val actualWriter = writer(preparedWithCatalog, Now)
      val impossibleRelease = actualWriter.copy(ownerBootUuid = None, leaseUntil = Now.minusNanos(1000L))
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlDbFirstBootstrapWriterGraph.releaseTarget(
        preparedWithCatalog, Inputs, allocation, Plan, Source, Receipt, None, actualWriter, impossibleRelease,
        QuotaMaximum))

      val expiredBefore = actualWriter.copy(databaseNow = Now.plusMinutes(2))
      val originalFence = MySqlWriterFenceRow(Plan.bindingUuid(), expiredBefore.writerEpoch + 1L,
        Some(BootTwo), Now.plusMinutes(3), expiredBefore.lastOperationUuid, expiredBefore.lastFingerprint)
      val alteredSlot = originalFence.copy(lastCommitOperationUuid = Some(Plan.certifyWriteUuid()))
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlDbFirstBootstrapWriterGraph.handoffTarget(
        preparedWithCatalog, Inputs, allocation, Plan, Source, Receipt, None, expiredBefore, alteredSlot,
        QuotaMaximum))
    finally
      closeTarget(prepared)
      closeTarget(preparedWithCatalog)
      allocation.closeRootReadBudget()
