package com.wi.floatingpod.storage.mysql

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection as JdbcConnection, PreparedStatement, SQLException, Types}
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.UUID
import scala.collection.mutable
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** FirstBinding6 固定 DML 的 JDBC ABI、CAS 与未知清理结果都由真实 typed rows 驱动。 */
final class MySqlDbFirstBootstrapDmlTest:
  import MySqlScriptSchemaDataTestFixture.{binary, full26GenesisRelations, uuidBytes}
  import DeploymentReceiptStaticTestData.*

  private val Configuration = defaultProfile()
  private val Rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(Configuration)
  private val Inputs = MySqlScriptSchemaInputs.createDbFirstFull27(Rendered)
  private val Full26Inputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
    Configuration.schema, MySqlDeploymentReceiptSchemaProfile.render(Configuration).sha256,
    Configuration.sourceLimitProfile, Configuration.publicationBudget, Configuration.deploymentBudget)
  private val QuotaMaximum = Configuration.quotaMaximumRecords
  private val Observed = MySqlScriptSchemaDataTestFixture.Connection
  private val ObservationScope = MySqlScriptSchemaDataTestFixture.Scope
  private val BootOne = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c1")
  private val BootTwo = UUID.fromString("28a0b51d-4ff4-4a71-a27f-d67f48d3c7c2")
  private val SnapshotPod = UUID.fromString("38a0b51d-4ff4-4a71-a27f-d67f48d3c701")
  private val SnapshotAttempt = UUID.fromString("38a0b51d-4ff4-4a71-a27f-d67f48d3c702")
  private val ActivationOperation = UUID.fromString("38a0b51d-4ff4-4a71-a27f-d67f48d3c703")
  private val PreparationOperation = UUID.fromString("38a0b51d-4ff4-4a71-a27f-d67f48d3c704")
  private val CreateOperation = UUID.fromString("38a0b51d-4ff4-4a71-a27f-d67f48d3c705")
  private val Plan = new com.wi.floatingpod.server.DbFirstWorldAdoptionPlanData(
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c701"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c702"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c703"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c704"),
    binding, 2, Observed.serverUuid, Inputs.schema, 6, Inputs.ddlSha256, provisioning)
  private val SourceWorldIdentity = "world://managed/floatingpod-primary"
  private val ReceiptSha256 = Vector.fill[Byte](32)(0x44.toByte)
  private val MarkerAckSha256 = Vector.fill[Byte](32)(0x55.toByte)
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private val SeedWriter = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private val PrepareWriter = MySqlFreshFormat3WriterTarget(
    Plan.bindingUuid(), 1L, Some(BootOne), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private final case class BoundValue(method: String, value: Any)

  private final class StatementRecord(val sql: String):
    val bindings = mutable.Map.empty[Int, BoundValue]
    var executeCalls = 0
    var closeCalls = 0

  private final class JdbcState(val affectedRows: Int, val failStatementCloseAt: Option[Int]):
    val statements = mutable.ArrayBuffer.empty[StatementRecord]
    var statementCloseCalls = 0
    var commits = 0
    var rollbacks = 0
    var autoCommitChanges = 0
    var isolationChanges = 0
    var autoCommit = false
    var closed = false

  private final class Harness(root: MySqlFormat3ReadBudget, affectedRows: Int = 1,
      failStatementCloseAt: Option[Int] = None):
    val jdbc = new JdbcState(affectedRows, failStatementCloseAt)
    val connection: JdbcConnection = connectionProxy(jdbc)
    val identity = new MySqlConnectionIdentity(Observed.serverUuid, Observed.serverVersion, Inputs.schema,
      Observed.effectiveUser, "InnoDB", 1, 1L, "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00",
      "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val verified = new VerifiedMySqlConnection(connection, identity)
    val scope = MySqlAuditBootScope.forOwnedIo(verified, Thread.currentThread(), BootOne, 1L, 1L)
    val rollbackOnly = new MySqlRollbackOnlyState
    require(MySqlConnectionTransactionGate.enter(connection, rollbackOnly).isEmpty,
      "test owns the actual JDBC transaction gate")
    val gate = MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly)
    val dml = new MySqlDbFirstBootstrapDml(Inputs, root)
    private var gateHeld = true

    def execute(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
        phase: MySqlDbFirstBootstrapDml.Phase): Unit =
      dml.execute(verified, scope, gate, rollbackOnly, before, target, phase)

    def closeConnection(): Unit = if !jdbc.closed then connection.close()

    def close(): Unit =
      try
        if dml.hasUnresolvedStatement then
          closeConnection()
          dml.retireAfterClosedConnection(verified)
        else dml.close()
      finally
        try leaveGate()
        finally closeConnection()

    def leaveGate(): Unit =
      if gateHeld then
        MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
        gateHeld = false

  private final class Scenario:
    val root = Inputs.newAllocationRoot(Inputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(Inputs.sourceLimitProfile.maxRetainedBytes()))
    val beforePrepare = capture(initialRows(), root)
    val prepared = MySqlDbFirstPrepareComposition.prepareTarget(beforePrepare, Inputs, root, Plan,
      SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter, QuotaMaximum)
    private val prepareNow = Now.plusMinutes(2)
    val preparedWriter = MySqlDbFirstBootstrapProof.writer(prepared, prepareNow)
    val handoffFence = MySqlWriterFenceRow(Plan.bindingUuid(), preparedWriter.writerEpoch + 1L,
      Some(BootTwo), Now.plusMinutes(3), preparedWriter.lastOperationUuid, preparedWriter.lastFingerprint)
    val handoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(prepared, Inputs, root, Plan,
      SourceWorldIdentity, ReceiptSha256, None, preparedWriter, handoffFence, QuotaMaximum)
    val handoffNow = Now.plusMinutes(2)
    val handoffWriter = MySqlDbFirstBootstrapProof.writer(handoff, handoffNow)
    private val certifyWriter = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), handoffWriter.writerEpoch,
      handoffWriter.ownerBootUuid, handoffWriter.leaseUntil, Plan.certifyWriteUuid())
    val certified = MySqlDbFirstBootstrapGraph.certifyTarget(handoff, Inputs, root, Plan,
      SourceWorldIdentity, ReceiptSha256, MarkerAckSha256, handoffWriter, certifyWriter, QuotaMaximum)
    val certifiedWriter = MySqlDbFirstBootstrapProof.writer(certified, handoffNow)
    val releaseWriter = certifiedWriter.copy(ownerBootUuid = None, leaseUntil = handoffNow.minusNanos(1000L))
    val released = MySqlDbFirstBootstrapWriterGraph.releaseTarget(certified, Inputs, root, Plan,
      SourceWorldIdentity, ReceiptSha256, Some(MarkerAckSha256), certifiedWriter, releaseWriter, QuotaMaximum)

    def captureRows(rows: Vector[MySqlFormat3RelationSnapshot]): MySqlScriptSchemaSnapshot =
      MySqlScriptSchemaSnapshot.captureMaterialized(Inputs.schema, Observed.copy(catalog = Inputs.schema),
        ObservationScope, Inputs.budget.packetBytes, Inputs, rows, root)

    def close(): Unit =
      var primary: Throwable = null
      def settle(action: => Unit): Unit =
        try action
        catch
          case cause: Throwable =>
            if primary == null then primary = cause
            else if primary ne cause then primary.addSuppressed(cause)
      settle(closeChild(released, root))
      settle(closeChild(certified, root))
      settle(closeChild(handoff, root))
      settle(closeChild(prepared, root))
      settle(root.closeRootReadBudget())
      if primary != null then throw primary

  private def capture(rows: Vector[MySqlFormat3RelationSnapshot], allocation: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Inputs.schema, Observed.copy(catalog = Inputs.schema),
      ObservationScope, Inputs.budget.packetBytes, Inputs, rows, allocation)

  private def initialRows(): Vector[MySqlFormat3RelationSnapshot] =
    val oldRows = full26GenesisRelations(Full26Inputs, catalogPresent = false)
      .iterator.map(relation => relation.table -> relation).toMap
    Inputs.canonicalOrder.map { shape =>
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
          var row = oldRows(shape.table).rows.head
          row = replaceCell(row, shape.table, "format_version", MySqlFormat3Cell.SignedInteger(6L))
          row = replaceCell(row, shape.table, "ddl_sha256", binary(java.util.HexFormat.of().parseHex(Rendered.sha256)))
          row = replaceCell(row, shape.table, "provisioning_uuid", binary(uuidBytes(Plan.provisioningUuid())))
          row = replaceCell(row, shape.table, "expected_binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "source_provenance_kind", MySqlFormat3Cell.Text("MANAGED_INSTALL"))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case "fp_runtime_writer" =>
          var row = oldRows(shape.table).rows.head
          row = replaceCell(row, shape.table, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "lease_until", MySqlFormat3Cell.DateTime(Now))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case name => oldRows.get(name).getOrElse(MySqlFormat3RelationSnapshot(name, Vector.empty))
    }.map { relation =>
      if relation.table == "physical_recovery_binding" || relation.table == "binding_bootstrap_intent" then
        relation.copy(rows = Vector.empty)
      else relation
    }

  private def replaceCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = Inputs.forTable(table).columns.indexWhere(_.name == name)
    require(index >= 0, "actual Full27 column must exist")
    row.updated(index, value)

  private def dataRow(table: String, values: (String, MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
    values.foldLeft(MySqlScriptSchemaDataTestFixture.defaultRow(Inputs.forTable(table))) {
      case (row, (name, value)) => replaceCell(row, table, name, value)
    }

  private def sha256(bytes: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes)

  private def validSnapshotRelations(relations: Vector[MySqlFormat3RelationSnapshot]): Vector[MySqlFormat3RelationSnapshot] =
    val domain = dataRow("floating_pod_state",
      "uuid" -> binary(uuidBytes(SnapshotPod)),
      "binding_uuid" -> binary(uuidBytes(Plan.bindingUuid())),
      "state_version" -> MySqlFormat3Cell.SignedInteger(1L),
      "is_active" -> MySqlFormat3Cell.SignedInteger(1L),
      "owner_uuid" -> MySqlFormat3Cell.SqlNull,
      "last_mutation_operation_uuid" -> binary(uuidBytes(ActivationOperation)),
      "operating_mode" -> MySqlFormat3Cell.Text("WORLD_IDLE"))
    def operation(id: UUID, step: String, version: Long): Vector[MySqlFormat3Cell] =
      val payload = Array[Byte](1)
      val context = MySqlOperationContext(BootOne, 1L, Plan.bindingUuid(), id,
        Vector(MySqlExpectedEffect(s"snapshot-fixture/$id/$step", version, sha256(payload).toVector)))
      val contextBytes = MySqlScriptSchemaDataTestFixture.encodeGenericContext(context)
      val payloadFields = Vector("request_identity", "scope", "preimage", "target", "commit_witness").flatMap(name =>
        Vector(name + "_bytes" -> binary(payload), name + "_sha256" -> binary(sha256(payload))))
      dataRow("fp_recovery_operation", (payloadFields ++ Vector(
        "binding_uuid" -> binary(uuidBytes(Plan.bindingUuid())),
        "operation_uuid" -> binary(uuidBytes(id)),
        "workflow_uuid" -> binary(uuidBytes(CreateOperation)),
        "boot_uuid" -> binary(uuidBytes(BootOne)),
        "boot_generation" -> MySqlFormat3Cell.SignedInteger(1L),
        "connection_generation_uuid" -> binary(uuidBytes(BootTwo)),
        "connection_id" -> MySqlFormat3Cell.SignedInteger(1L),
        "fence_epoch" -> MySqlFormat3Cell.SignedInteger(1L),
        "context_encoding" -> MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"),
        "original_context_bytes" -> binary(contextBytes),
        "original_context_sha256" -> binary(sha256(contextBytes)),
        "operation_fingerprint" -> binary(context.canonicalFingerprintSha256.toArray),
        "result_at" -> MySqlFormat3Cell.DateTime(Now),
        "step" -> MySqlFormat3Cell.Text(step),
        "operation_state" -> MySqlFormat3Cell.Text("COMMITTED"),
        "pod_uuid" -> binary(uuidBytes(SnapshotPod)),
        "attempt_uuid" -> binary(uuidBytes(SnapshotAttempt)),
        "expected_domain_version" -> MySqlFormat3Cell.SignedInteger(version)))*)
    val activation = operation(ActivationOperation, MySqlDbFirstSchemaDefinition.InitialActivationStep, 0L)
    val preparation = operation(PreparationOperation, "REARM", 0L)
    val intent = dataRow("physical_recovery_intent",
      "binding_uuid" -> binary(uuidBytes(Plan.bindingUuid())),
      "pod_uuid" -> binary(uuidBytes(SnapshotPod)),
      "attempt_uuid" -> binary(uuidBytes(SnapshotAttempt)),
      "create_operation_uuid" -> binary(uuidBytes(CreateOperation)),
      "prepare_operation_uuid" -> binary(uuidBytes(PreparationOperation)),
      "phase" -> MySqlFormat3Cell.Text("ACTIVATED"),
      "origin" -> MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE"),
      "activation_operation_uuid" -> binary(uuidBytes(ActivationOperation)),
      "expected_domain_version" -> MySqlFormat3Cell.SignedInteger(0L),
      "committed_domain_version" -> MySqlFormat3Cell.SignedInteger(1L),
      "expected_owner_uuid" -> MySqlFormat3Cell.SqlNull)
    val nbt = Array[Byte](1)
    val profileBytes = Inputs.sourceLimitProfile.canonicalBytes()
    val snapshot = dataRow(MySqlDbFirstSchemaDefinition.SnapshotTable,
      "binding_uuid" -> binary(uuidBytes(Plan.bindingUuid())),
      "pod_uuid" -> binary(uuidBytes(SnapshotPod)),
      "owner_uuid" -> MySqlFormat3Cell.SqlNull,
      "attempt_uuid" -> binary(uuidBytes(SnapshotAttempt)),
      "snapshot_format_version" -> MySqlFormat3Cell.SignedInteger(com.wi.floatingpod.entity.DbFirstPodSnapshot.FORMAT_VERSION.toLong),
      "nbt_bytes" -> binary(nbt),
      "nbt_sha256" -> binary(sha256(nbt)),
      "limit_profile_encoding" -> MySqlFormat3Cell.Text(com.wi.floatingpod.config.SourceLimitProfile.ENCODING),
      "limit_profile_bytes" -> binary(profileBytes),
      "limit_profile_sha256" -> binary(sha256(profileBytes)),
      "captured_state_version" -> MySqlFormat3Cell.SignedInteger(0L),
      "committed_state_version" -> MySqlFormat3Cell.SignedInteger(1L),
      "last_mutation_operation_uuid" -> binary(uuidBytes(ActivationOperation)))
    val replacements = Map(
      "floating_pod_state" -> Vector(domain),
      "fp_recovery_operation" -> Vector(activation, preparation),
      "physical_recovery_intent" -> Vector(intent),
      MySqlDbFirstSchemaDefinition.SnapshotTable -> Vector(snapshot))
    relations.map(relation => replacements.get(relation.table)
      .map(rows => relation.copy(rows = rows)).getOrElse(relation))

  private def closeChild(snapshot: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): Unit =
    if snapshot != null && (snapshot.allocationBudget ne root) && !snapshot.allocationBudget.isClosed then
      snapshot.allocationBudget.closeScopedReadBudget()

  @Test def abandonedStartupInspectionAcceptsOnlyExactAcquireRenewDataWithoutGrantingAuthority(): Unit =
    val scenario = new Scenario
    val images = mutable.ArrayBuffer.empty[MySqlScriptSchemaSnapshot]
    val tail = MySqlDbFirstBootstrapProof.writer(scenario.released, Now.plusHours(1))
    def image(epoch: Long = tail.writerEpoch + 1L, boot: UUID = BootTwo,
        lease: LocalDateTime = Now.plusMinutes(5), change: Option[String] = None): MySqlScriptSchemaSnapshot =
      val rows = scenario.released.relations.map { relation =>
        if relation.table == "fp_runtime_writer" then
          var row = relation.rows.head
          row = replaceCell(row, relation.table, "writer_epoch", MySqlFormat3Cell.SignedInteger(epoch))
          row = replaceCell(row, relation.table, "owner_boot_uuid", binary(uuidBytes(boot)))
          row = replaceCell(row, relation.table, "lease_until", MySqlFormat3Cell.DateTime(lease))
          if change.contains("binding") then
            row = replaceCell(row, relation.table, "binding_uuid", binary(uuidBytes(BootOne)))
          if change.contains("fingerprint") then
            row = replaceCell(row, relation.table, "last_commit_fingerprint", binary(Array.fill[Byte](32)(0x33.toByte)))
          if change.contains("slot") then
            row = replaceCell(row, relation.table, "last_commit_operation_uuid", binary(uuidBytes(BootOne)))
          relation.copy(rows = Vector(row))
        else if change.contains("data") && relation.table == "fp_schema_manifest" then
          relation.copy(rows = Vector(replaceCell(relation.rows.head, relation.table,
            "format_version", MySqlFormat3Cell.SignedInteger(7L))))
        else relation
      }
      val result = scenario.captureRows(rows)
      images += result
      result
    def inspect(value: MySqlScriptSchemaSnapshot, auditedEpoch: Long = tail.writerEpoch + 1L): MySqlBusinessWriterRow =
      MySqlDbFirstAbandonedStartupData.inspect(scenario.released, value, BootTwo, Some(BootTwo),
        auditedEpoch, Now.plusHours(1))
    try
      assertEquals(Some(BootTwo), inspect(image()).ownerBootUuid)
      assertEquals(tail.writerEpoch + 1L, inspect(image(lease = Now.plusMinutes(30))).writerEpoch)
      // 正常 Acquire/Release 会跨越历史空图多个 epoch；本次 actual audit 必须精确匹配最新 epoch。
      assertEquals(tail.writerEpoch+3L,inspect(image(epoch=tail.writerEpoch+3L),tail.writerEpoch+3L).writerEpoch)
      val multi=tail.writerEpoch+3L
      def rejected(value:MySqlScriptSchemaSnapshot,reason:String):Unit =
        assertEquals(reason,assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>inspect(value,multi)).safeSummary)
      rejected(image(epoch=tail.writerEpoch),"REJECTED_ACQUIRE_IDENTITY")
      rejected(image(epoch=tail.writerEpoch-1L),"REJECTED_ACQUIRE_IDENTITY")
      rejected(image(epoch=multi,boot=BootOne),"REJECTED_ACQUIRE_IDENTITY")
      rejected(image(epoch=multi,change=Some("binding")),"REJECTED_ACQUIRE_IDENTITY")
      rejected(image(epoch=multi,change=Some("data")),"REJECTED_NON_WRITER_DATA")
      rejected(image(epoch=multi,change=Some("slot")),"REJECTED_COMMIT_SLOT_CHANGED")
      rejected(image(epoch=multi,change=Some("fingerprint")),"REJECTED_COMMIT_SLOT_CHANGED")
      rejected(image(epoch=multi,lease=Now.plusHours(2)),"REJECTED_LEASE_NOT_EXPIRED")
      rejected(image(epoch=multi,lease=tail.leaseUntil),"REJECTED_LEASE_NOT_AFTER_RELEASE_TAIL")
      assertEquals("REJECTED_AUDIT_CHANGED",assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
        MySqlDbFirstAbandonedStartupData.inspect(scenario.released,image(epoch=multi),BootTwo,Some(BootOne),multi,Now.plusHours(1))).safeSummary)
      for value <- Vector(image(epoch = tail.writerEpoch), image(boot = BootOne),
          image(lease = Now.plusHours(2)), image(lease = tail.leaseUntil), image(lease = tail.leaseUntil.minusNanos(1000L)),
          image(change = Some("slot")), image(change = Some("data"))) do
        assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection], () => inspect(value))
      assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection], () => inspect(image(), tail.writerEpoch))
    finally
      images.reverseIterator.foreach(value => closeChild(value, scenario.root))
      scenario.close()

  @Test def startupRetirementUsesSeparateExactCasAndPreservesEpochSlotAndOther26Relations(): Unit =
    val scenario = new Scenario
    var acquired: MySqlScriptSchemaSnapshot = null
    var target: MySqlScriptSchemaSnapshot = null
    val harness = new Harness(scenario.root)
    try
      val tail = MySqlDbFirstBootstrapProof.writer(scenario.released, Now.plusHours(1))
      acquired = scenario.captureRows(scenario.released.relations.map { relation =>
        if relation.table != "fp_runtime_writer" then relation
        else
          var row = replaceCell(relation.rows.head, relation.table, "writer_epoch", MySqlFormat3Cell.SignedInteger(tail.writerEpoch + 1L))
          row = replaceCell(row, relation.table, "owner_boot_uuid", binary(uuidBytes(BootTwo)))
          row = replaceCell(row, relation.table, "lease_until", MySqlFormat3Cell.DateTime(Now.plusMinutes(5)))
          relation.copy(rows = Vector(row))
      })
      target = MySqlDbFirstAbandonedStartupData.retirementTarget(acquired, Now.plusHours(1), scenario.root)
      assertNotSame(scenario.root, target.allocationBudget)
      assertTrue(target.allocationBudget.isScopedReadBudget)
      assertTrue(target.allocationBudget.sharesAllocationLedger(scenario.root))
      MySqlDbFirstAbandonedStartupData.requireRetirementTarget(acquired, target, Now.plusHours(1))
      val before = MySqlDbFirstBootstrapProof.writer(acquired, Now.plusHours(1))
      val after = MySqlDbFirstBootstrapProof.writer(target, Now.plusHours(1))
      assertEquals(before.copy(ownerBootUuid = None, leaseUntil = Now.plusHours(1).minusNanos(1000)), after)
      assertEquals(acquired.relations.filterNot(_.table == "fp_runtime_writer"), target.relations.filterNot(_.table == "fp_runtime_writer"))
      harness.execute(acquired, target, MySqlDbFirstBootstrapDml.Phase.StartupRetirement)
      assertPhaseAbi(harness, acquired, target, MySqlDbFirstBootstrapDml.Phase.StartupRetirement)
      assertEquals(1, harness.jdbc.statements.size)
      assertEquals(0, harness.jdbc.commits, "only the actor owning durable intent may commit")
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlDbFirstAbandonedStartupData.requireRetirementTarget(acquired, scenario.released, Now.plusHours(1)))
    finally
      harness.close()
      closeChild(target, scenario.root)
      assertFalse(scenario.root.isClosed, "retirement target cleanup must keep original root open")
      closeChild(acquired, scenario.root)
      scenario.close()

  private final case class ExpectedStatement(sql: String, values: Vector[MySqlFormat3Cell])

  private def phaseTables(phase: MySqlDbFirstBootstrapDml.Phase): Vector[String] = phase match
    case MySqlDbFirstBootstrapDml.Phase.Prepare =>
      Vector("binding_bootstrap_intent", "physical_recovery_binding", "fp_script_catalog", "fp_runtime_writer")
    case MySqlDbFirstBootstrapDml.Phase.Certify =>
      Vector("physical_recovery_binding", "binding_bootstrap_intent", "fp_runtime_writer")
    case MySqlDbFirstBootstrapDml.Phase.Handoff | MySqlDbFirstBootstrapDml.Phase.Release |
        MySqlDbFirstBootstrapDml.Phase.StartupRetirement =>
      Vector("fp_runtime_writer")

  private def expectedStatements(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      phase: MySqlDbFirstBootstrapDml.Phase): Vector[ExpectedStatement] =
    phaseTables(phase).map { table =>
      val columns = Inputs.forTable(table).columns
      val oldRows = before.relation(table).rows
      val targetRows = target.relation(table).rows
      require(oldRows.size <= 1 && targetRows.size == 1, "closed phase fixture uses exact old/new singleton row counts")
      val next = targetRows.head
      val inserted = oldRows.isEmpty
      val changed = columns.indices.filter(index => inserted || oldRows.head(index) != next(index)).toVector
      require(changed.nonEmpty, "actual phase fixture must mutate each fixed table")
      val quoted = columns.map(column => "`" + column.name + "`")
      val prefix = s"`${Inputs.schema}`.`$table`"
      val sql = if inserted then
        s"INSERT INTO $prefix (${quoted.mkString(",")}) VALUES (${columns.map(_ => "?").mkString(",")})"
      else
        s"UPDATE $prefix SET ${changed.map(index => quoted(index) + "=?").mkString(",")} WHERE ${quoted.map(_ + " <=> ?").mkString(" AND ")}"
      ExpectedStatement(sql, changed.map(next) ++ oldRows.headOption.getOrElse(Vector.empty))
    }

  private def expectedBinding(cell: MySqlFormat3Cell): BoundValue = cell match
    case MySqlFormat3Cell.SqlNull => BoundValue("setNull", Types.NULL)
    case MySqlFormat3Cell.Text(value) => BoundValue("setString", value)
    case MySqlFormat3Cell.Binary(value) =>
      BoundValue("setBytes", Vector.tabulate(value.length)(value.byteAt))
    case MySqlFormat3Cell.SignedInteger(value) => BoundValue("setLong", value)
    case MySqlFormat3Cell.Floating(value) =>
      BoundValue("setDouble", java.lang.Double.longBitsToDouble(value))
    case MySqlFormat3Cell.DateTime(value) => BoundValue("setObject", value)

  private def actualBindings(statement: StatementRecord): Vector[BoundValue] =
    val sorted = statement.bindings.toVector.sortBy(_._1)
    assertEquals(sorted.indices.map(_ + 1).toVector, sorted.map(_._1), "JDBC parameter positions are contiguous")
    sorted.map(_._2)

  private def assertPhaseAbi(harness: Harness, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, phase: MySqlDbFirstBootstrapDml.Phase): Unit =
    val expected = expectedStatements(before, target, phase)
    assertEquals(expected.map(_.sql), harness.jdbc.statements.map(_.sql).toVector,
      s"${phase.toString} sends its exact ordered UPDATE/INSERT ABI")
    expected.zip(harness.jdbc.statements).foreach { (want, actual) =>
      assertEquals(want.values.map(expectedBinding), actualBindings(actual), s"${phase.toString} values include changed data then every old row cell")
      assertEquals(1, actual.executeCalls)
      assertEquals(1, actual.closeCalls)
    }
    assertTrue(harness.rollbackOnly.cause.isEmpty)
    assertEquals(0, harness.jdbc.commits)
    assertEquals(0, harness.jdbc.rollbacks)
    assertEquals(0, harness.jdbc.autoCommitChanges)
    assertEquals(0, harness.jdbc.isolationChanges)

  private def connectionProxy(state: JdbcState): JdbcConnection = proxy(classOf[JdbcConnection], new InvocationHandler:
    override def invoke(instance: Object, method: Method, args: Array[Object]): Object =
      val values = Option(args).getOrElse(Array.empty[Object])
      method.getName match
        case "getAutoCommit" => java.lang.Boolean.valueOf(state.autoCommit)
        case "getTransactionIsolation" => Int.box(JdbcConnection.TRANSACTION_REPEATABLE_READ)
        case "isClosed" => java.lang.Boolean.valueOf(state.closed)
        case "prepareStatement" =>
          require(!state.closed, "the actual proxy JDBC connection remains open during DML")
          val record = new StatementRecord(values(0).asInstanceOf[String])
          state.statements += record
          statementProxy(state, record)
        case "setAutoCommit" => state.autoCommitChanges += 1; state.autoCommit = values(0).asInstanceOf[Boolean]; null
        case "setTransactionIsolation" => state.isolationChanges += 1; null
        case "commit" => state.commits += 1; null
        case "rollback" => state.rollbacks += 1; null
        case "close" => state.closed = true; null
        case "toString" => "FirstBinding6DmlTypedProxyConnection"
        case "hashCode" => Int.box(System.identityHashCode(instance))
        case "equals" => java.lang.Boolean.valueOf(instance.asInstanceOf[AnyRef] eq values(0))
        case _ => defaultValue(method.getReturnType)
  )

  private def statementProxy(state: JdbcState, record: StatementRecord): PreparedStatement =
    proxy(classOf[PreparedStatement], new InvocationHandler:
      override def invoke(instance: Object, method: Method, args: Array[Object]): Object =
        val values = Option(args).getOrElse(Array.empty[Object])
        val index = if values.nonEmpty then values(0).asInstanceOf[Int] else 0
        method.getName match
          case "setNull" => record.bindings(index) = BoundValue("setNull", values(1).asInstanceOf[Int]); null
          case "setString" => record.bindings(index) = BoundValue("setString", values(1).asInstanceOf[String]); null
          case "setBytes" =>
            val bytes = values(1).asInstanceOf[Array[Byte]]
            record.bindings(index) = BoundValue("setBytes", Vector.tabulate(bytes.length)(bytes(_)))
            null
          case "setLong" => record.bindings(index) = BoundValue("setLong", values(1).asInstanceOf[java.lang.Long].longValue()); null
          case "setDouble" => record.bindings(index) = BoundValue("setDouble", values(1).asInstanceOf[java.lang.Double].doubleValue()); null
          case "setObject" => record.bindings(index) = BoundValue("setObject", values(1)); null
          case "executeUpdate" => record.executeCalls += 1; Int.box(state.affectedRows)
          case "close" =>
            record.closeCalls += 1
            state.statementCloseCalls += 1
            if state.failStatementCloseAt.contains(state.statementCloseCalls) then
              throw new SQLException("synthetic original FirstBinding6 DML statement close failure")
            null
          case "toString" => "FirstBinding6DmlTypedProxyStatement"
          case "hashCode" => Int.box(System.identityHashCode(instance))
          case "equals" => java.lang.Boolean.valueOf(instance.asInstanceOf[AnyRef] eq values(0))
          case _ => defaultValue(method.getReturnType)
    )

  private def proxy[A](clazz: Class[A], handler: InvocationHandler): A =
    Proxy.newProxyInstance(clazz.getClassLoader, Array[Class[?]](clazz), handler).asInstanceOf[A]

  private def defaultValue(returnType: Class[?]): Object =
    if !returnType.isPrimitive then null
    else if returnType == java.lang.Boolean.TYPE then java.lang.Boolean.FALSE
    else if returnType == java.lang.Integer.TYPE then Int.box(0)
    else if returnType == java.lang.Long.TYPE then Long.box(0L)
    else if returnType == java.lang.Double.TYPE then Double.box(0.0d)
    else if returnType == java.lang.Float.TYPE then Float.box(0.0f)
    else if returnType == java.lang.Short.TYPE then Short.box(0.toShort)
    else if returnType == java.lang.Byte.TYPE then Byte.box(0.toByte)
    else if returnType == java.lang.Character.TYPE then java.lang.Character.valueOf(0.toChar)
    else null

  @Test def allFourPhasesBindChangedColumnsAndCompareEveryOldRowColumn(): Unit =
    val scenario = new Scenario
    try
      val prepare = new Harness(scenario.root)
      try
        prepare.execute(scenario.beforePrepare, scenario.prepared, MySqlDbFirstBootstrapDml.Phase.Prepare)
        assertPhaseAbi(prepare, scenario.beforePrepare, scenario.prepared, MySqlDbFirstBootstrapDml.Phase.Prepare)
      finally prepare.close()

      val handoff = new Harness(scenario.root)
      try
        handoff.execute(scenario.prepared, scenario.handoff, MySqlDbFirstBootstrapDml.Phase.Handoff)
        assertPhaseAbi(handoff, scenario.prepared, scenario.handoff, MySqlDbFirstBootstrapDml.Phase.Handoff)
      finally handoff.close()

      val certify = new Harness(scenario.root)
      try
        certify.execute(scenario.handoff, scenario.certified, MySqlDbFirstBootstrapDml.Phase.Certify)
        assertPhaseAbi(certify, scenario.handoff, scenario.certified, MySqlDbFirstBootstrapDml.Phase.Certify)
      finally certify.close()

      val release = new Harness(scenario.root)
      try
        release.execute(scenario.certified, scenario.released, MySqlDbFirstBootstrapDml.Phase.Release)
        assertPhaseAbi(release, scenario.certified, scenario.released, MySqlDbFirstBootstrapDml.Phase.Release)
      finally release.close()
    finally scenario.close()

  @Test def unexpectedTwentySeventhRelationDriftRejectsBeforeAnyStatementAndMarksRollbackOnly(): Unit =
    val scenario = new Scenario
    val validBeforeRows = validSnapshotRelations(scenario.prepared.relations)
    val before = scenario.captureRows(validBeforeRows)
    val driftRows = validBeforeRows.map { relation =>
      if relation.table == "fp_runtime_writer" then
        relation.copy(rows = scenario.handoff.relation("fp_runtime_writer").rows)
      else if relation.table == MySqlDbFirstSchemaDefinition.SnapshotTable then
        val changedNbt = Array[Byte](2)
        var row = relation.rows.head
        row = replaceCell(row, relation.table, "nbt_bytes", binary(changedNbt))
        row = replaceCell(row, relation.table, "nbt_sha256", binary(sha256(changedNbt)))
        relation.copy(rows = Vector(row))
      else relation
    }
    val driftTarget = scenario.captureRows(driftRows)
    val harness = new Harness(scenario.root)
    try
      val failure = assertThrows(classOf[SQLException], () => harness.execute(before, driftTarget,
        MySqlDbFirstBootstrapDml.Phase.Handoff))
      assertTrue(failure.getMessage.contains("unrelated Full27 relation: " + MySqlDbFirstSchemaDefinition.SnapshotTable))
      assertSame(failure, harness.rollbackOnly.cause.get)
      assertTrue(harness.jdbc.statements.isEmpty, "the 27th snapshot relation is checked before SQL allocation")
      assertEquals(0, harness.jdbc.commits)
      assertEquals(0, harness.jdbc.rollbacks)
    finally
      harness.close()
      scenario.close()

  @Test def affectedRowsOtherThanOneMarksRollbackOnlyAndTheFailedDmlCannotRetry(): Unit =
    val scenario = new Scenario
    val harness = new Harness(scenario.root, affectedRows = 0)
    try
      val failure = assertThrows(classOf[SQLException], () => harness.execute(scenario.beforePrepare,
        scenario.prepared, MySqlDbFirstBootstrapDml.Phase.Prepare))
      assertTrue(failure.getMessage.contains("changed other than one row"))
      assertSame(failure, harness.rollbackOnly.cause.get)
      assertEquals(1, harness.jdbc.statements.size, "the first failed CAS stops the remaining fixed phase DML")
      assertEquals(1, harness.jdbc.statements.head.executeCalls)
      assertEquals(1, harness.jdbc.statements.head.closeCalls)
      assertThrows(classOf[IllegalArgumentException], () => harness.execute(scenario.beforePrepare,
        scenario.prepared, MySqlDbFirstBootstrapDml.Phase.Prepare))
      assertEquals(1, harness.jdbc.statements.size, "a failed one-shot phase does not replay its SQL")
      assertEquals(0, harness.jdbc.commits)
      assertEquals(0, harness.jdbc.rollbacks)
    finally
      harness.close()
      scenario.close()

  @Test def unknownStatementCloseRetainsFeesForbidsRetryAndRetiresOnlyAfterOriginalConnectionCloses(): Unit =
    val scenario = new Scenario
    val harness = new Harness(scenario.root, failStatementCloseAt = Some(1))
    val baseline = scenario.root.accountedMemoryBytes
    try
      val failure = assertThrows(classOf[SQLException], () => harness.execute(scenario.prepared,
        scenario.handoff, MySqlDbFirstBootstrapDml.Phase.Handoff))
      assertEquals("synthetic original FirstBinding6 DML statement close failure", failure.getMessage)
      assertSame(failure, harness.rollbackOnly.cause.get)
      assertTrue(harness.dml.hasUnresolvedStatement)
      assertTrue(scenario.root.accountedMemoryBytes > baseline, "unknown statement close keeps SQL and parameter fees charged")
      assertEquals(1, harness.jdbc.statements.size)
      assertEquals(1, harness.jdbc.statementCloseCalls)
      assertThrows(classOf[IllegalArgumentException], () => harness.execute(scenario.prepared,
        scenario.handoff, MySqlDbFirstBootstrapDml.Phase.Handoff))
      val closeRetry = assertThrows(classOf[IllegalStateException], () => harness.dml.close())
      assertSame(failure, closeRetry.getCause)
      assertEquals(1, harness.jdbc.statementCloseCalls, "unknown close is never retried")
      harness.closeConnection()
      assertTrue(harness.connection.isClosed)
      harness.dml.retireAfterClosedConnection(harness.verified)
      assertFalse(harness.dml.hasUnresolvedStatement)
      assertEquals(baseline, scenario.root.accountedMemoryBytes,
        "the retained original fee is released only after its exact connection is confirmed closed")
      assertEquals(1, harness.jdbc.statementCloseCalls)
      assertEquals(0, harness.jdbc.commits)
      assertEquals(0, harness.jdbc.rollbacks)
    finally
      harness.close()
      scenario.close()
