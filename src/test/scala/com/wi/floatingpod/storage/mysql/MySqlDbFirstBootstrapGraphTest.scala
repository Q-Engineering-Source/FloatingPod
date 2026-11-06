package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{DbFirstWorldAdoptionPlanData, WindowsWorldProvisionReceiptDataCodec}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Full27 B1 DATA 只验证静态种子与固定两阶段变换，不签发运行或 SQL 资格。 */
final class MySqlDbFirstBootstrapGraphTest:
  import MySqlScriptSchemaDataTestFixture.*

  private val Configuration = DeploymentReceiptStaticTestData.defaultProfile()
  private val Rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(Configuration)
  private val Inputs = MySqlScriptSchemaInputs.createDbFirstFull27(Rendered)
  private val Full26Inputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
    Configuration.schema, MySqlDeploymentReceiptSchemaProfile.render(Configuration).sha256,
    Configuration.sourceLimitProfile, Configuration.publicationBudget, Configuration.deploymentBudget)
  private val ExpectedQuotaMaximumRecords = Configuration.quotaMaximumRecords
  private val Boot = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c1")
  private val Plan = new DbFirstWorldAdoptionPlanData(
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c701"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c702"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c703"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c704"),
    DeploymentReceiptStaticTestData.binding, 2, Connection.serverUuid, Inputs.schema, 6,
    Inputs.ddlSha256, DeploymentReceiptStaticTestData.provisioning)
  private val SourceWorldIdentity = "world://managed/floatingpod-primary"
  private val ReceiptSha256 = Vector.fill[Byte](32)(0x44.toByte)
  private val MarkerAckSha256 = Vector.fill[Byte](32)(0x55.toByte)
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private val SeedWriter = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private val PrepareWriter = MySqlFreshFormat3WriterTarget(
    Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private def root(): MySqlFormat3ReadBudget = Inputs.newAllocationRoot(Inputs.budget.packetBytes,
    new MySqlFormat3AllocationLedger(Inputs.sourceLimitProfile.maxRetainedBytes()))

  private def replaceCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = Inputs.forTable(table).columns.indexWhere(_.name == name)
    require(index >= 0, s"actual Full27 column $table.$name")
    row.updated(index, value)

  private def managedSeedRows(quotaMaximum: Long = ExpectedQuotaMaximumRecords,
      provenance: String = "MANAGED_INSTALL", catalogPresent: Boolean = false): Vector[MySqlFormat3RelationSnapshot] =
    val old = full26GenesisRelations(Full26Inputs, catalogPresent).map(relation => relation.table -> relation).toMap
    Inputs.canonicalOrder.map { shape =>
      shape.table match
        case MySqlDbFirstSchemaDefinition.SnapshotTable | "physical_recovery_binding" | "binding_bootstrap_intent" =>
          MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
        case MySqlDeploymentReceiptSchemaDefinition.QuotaTable =>
          val row = shape.columns.map(_.name match
            case "namespace" => binary(java.util.HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))
            case "max_records" => MySqlFormat3Cell.SignedInteger(quotaMaximum)
            case "used_records" => MySqlFormat3Cell.SignedInteger(0L)
            case "quota_revision" => MySqlFormat3Cell.SignedInteger(0L)
            case other => throw new AssertionError(s"unexpected quota column $other"))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case "fp_schema_manifest" =>
          var row = old(shape.table).rows.head
          row = replaceCell(row, shape.table, "format_version", MySqlFormat3Cell.SignedInteger(6L))
          row = replaceCell(row, shape.table, "ddl_sha256",
            binary(java.util.HexFormat.of().parseHex(Rendered.sha256)))
          row = replaceCell(row, shape.table, "provisioning_uuid", binary(uuidBytes(Plan.provisioningUuid())))
          row = replaceCell(row, shape.table, "expected_binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "source_provenance_kind", MySqlFormat3Cell.Text(provenance))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case "fp_runtime_writer" =>
          var row = old(shape.table).rows.head
          row = replaceCell(row, shape.table, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
          row = replaceCell(row, shape.table, "lease_until", MySqlFormat3Cell.DateTime(Now))
          MySqlFormat3RelationSnapshot(shape.table, Vector(row))
        case name => old.getOrElse(name, MySqlFormat3RelationSnapshot(name, Vector.empty))
    }

  private def capture(rows: Vector[MySqlFormat3RelationSnapshot], budget: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Inputs.schema,
      Connection.copy(catalog = Inputs.schema), Scope, Inputs.budget.packetBytes, Inputs, rows, budget)

  private def closeTarget(snapshot: MySqlScriptSchemaSnapshot): Unit =
    if snapshot != null && !snapshot.allocationBudget.isClosed then snapshot.allocationBudget.closeScopedReadBudget()

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell =
    val shape = Inputs.forTable(table)
    snapshot.relation(table).rows.head(shape.columns.indexWhere(_.name == column))

  private def preparedWriter(snapshot: MySqlScriptSchemaSnapshot): MySqlBusinessWriterRow =
    val fingerprint = cell(snapshot, "fp_runtime_writer", "last_commit_fingerprint") match
      case MySqlFormat3Cell.Binary(value) => Vector.tabulate(value.length)(value.byteAt)
      case _ => throw new AssertionError("prepared W fingerprint must be typed binary")
    MySqlBusinessWriterRow(Plan.bindingUuid(), 1L, Some(Boot), PrepareWriter.leaseUntil,
      Some(Plan.prepareWriteUuid()), Some(fingerprint), Now)

  private def genesisCatalogRow: Vector[MySqlFormat3Cell] =
    val table = "fp_script_catalog"
    Inputs.forTable(table).columns.map(_.name match
      case "binding_uuid" => binary(uuidBytes(Plan.bindingUuid()))
      case "catalog_revision" => MySqlFormat3Cell.SignedInteger(0L)
      case "snapshot_bytes" => binary(Array.emptyByteArray)
      case "snapshot_sha256" => binary(digest(Array.emptyByteArray))
      case "next_version" => MySqlFormat3Cell.SignedInteger(1L)
      case "last_mutation_operation_uuid" => MySqlFormat3Cell.SqlNull
      case other => throw new AssertionError(s"unexpected genesis catalog column $other"))

  private def withCatalog(snapshot: MySqlScriptSchemaSnapshot): Vector[MySqlFormat3RelationSnapshot] =
    snapshot.relations.map { relation =>
      if relation.table == "fp_script_catalog" then relation.copy(rows = Vector(genesisCatalogRow)) else relation
    }

  @Test def schema6RecipeIsIndependentAndCannotEnterTheFrozenFull26Graph(): Unit =
    assertEquals(6, Plan.schemaFormat())
    assertEquals(2, Plan.physicalFormat())
    assertEquals(Plan.ddlSha256().toUpperCase(java.util.Locale.ROOT), Plan.ddlSha256())
    assertThrows(classOf[IllegalArgumentException], () => new DbFirstWorldAdoptionPlanData(
      Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.certifyWriteUuid(),
      Plan.bindingUuid(), 2, Plan.mysqlServerUuid(), "FloatingPod", 6, Plan.ddlSha256(), Plan.provisioningUuid()))
    assertThrows(classOf[IllegalArgumentException], () => new DbFirstWorldAdoptionPlanData(
      Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.prepareWriteUuid(),
      Plan.bindingUuid(), 2, Plan.mysqlServerUuid(), Plan.schema(), 6, Plan.ddlSha256(), Plan.provisioningUuid()))
    assertThrows(classOf[IllegalArgumentException], () => new DbFirstWorldAdoptionPlanData(
      Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.certifyWriteUuid(),
      Plan.bindingUuid(), 2, Plan.mysqlServerUuid(), Plan.schema(), 5, Plan.ddlSha256(), Plan.provisioningUuid()))
    assertThrows(classOf[IllegalArgumentException], () => new DbFirstWorldAdoptionPlanData(
      Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.certifyWriteUuid(),
      Plan.bindingUuid(), 2, Plan.mysqlServerUuid(), Plan.schema(), 6, "not-a-sha", Plan.provisioningUuid()))

    val root = this.root()
    try
      val before = capture(managedSeedRows(), root)
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(
        before, Inputs, root,
        new WindowsWorldProvisionReceiptDataCodec.PlanData(Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(),
          Plan.prepareWriteUuid(), Plan.certifyWriteUuid(), Plan.bindingUuid(), 2, Plan.mysqlServerUuid(),
          Plan.schema(), 5, Plan.ddlSha256(), Plan.provisioningUuid()),
        SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter))
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapGraph.prepareTarget(
        before, Full26Inputs, root, Plan, SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter,
        ExpectedQuotaMaximumRecords))
    finally root.closeRootReadBudget()

  @Test def prepareAndCertifyRequireOnlyManagedFull27SeedsAndExactQuotaConfiguration(): Unit =
    val root = this.root()
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(managedSeedRows(), root)
      prepared = MySqlDbFirstBootstrapGraph.prepareTarget(before, Inputs, root, Plan, SourceWorldIdentity,
        ReceiptSha256, SeedWriter, PrepareWriter, ExpectedQuotaMaximumRecords)
      MySqlDbFirstBootstrapGraph.requirePrepareTransition(before, prepared, Inputs, root, Plan,
        SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter, ExpectedQuotaMaximumRecords)
      assertEquals(27, prepared.relations.size)
      assertEquals(480, Inputs.canonicalOrder.map(_.columns.size).sum)
      assertTrue(prepared.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.isEmpty)
      assertTrue(prepared.relation("fp_script_catalog").rows.isEmpty)
      assertEquals(MySqlFormat3Cell.Text("PREPARED"), cell(prepared, "binding_bootstrap_intent", "stage"))

      val catalogBefore = capture(withCatalog(prepared), root)
      val actualPreparedWriter = preparedWriter(prepared)
      val certifyWriter = PrepareWriter.copy(operationUuid = Plan.certifyWriteUuid())
      certified = MySqlDbFirstBootstrapGraph.certifyTarget(catalogBefore, Inputs, root, Plan, SourceWorldIdentity,
        ReceiptSha256, MarkerAckSha256, actualPreparedWriter, certifyWriter, ExpectedQuotaMaximumRecords)
      MySqlDbFirstBootstrapGraph.requireCertifyTransition(catalogBefore, certified, Inputs, root, Plan,
        SourceWorldIdentity, ReceiptSha256, MarkerAckSha256, actualPreparedWriter, certifyWriter,
        ExpectedQuotaMaximumRecords)
      assertEquals(MySqlFormat3Cell.Text("CERTIFIED"), cell(certified, "physical_recovery_binding", "cutover_state"))
      assertEquals(MySqlFormat3Cell.Text("COMPLETE"), cell(certified, "binding_bootstrap_intent", "stage"))
      assertTrue(certified.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.isEmpty)
      assertEquals(genesisCatalogRow, certified.relation("fp_script_catalog").rows.head)

      val wrongQuota = capture(managedSeedRows(quotaMaximum = ExpectedQuotaMaximumRecords + 1L), root)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapGraph.prepareTarget(wrongQuota,
        Inputs, root, Plan, SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter,
        ExpectedQuotaMaximumRecords))
      val nonManaged = capture(managedSeedRows(provenance = "FRESH_PLAN"), root)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapGraph.prepareTarget(nonManaged,
        Inputs, root, Plan, SourceWorldIdentity, ReceiptSha256, SeedWriter, PrepareWriter,
        ExpectedQuotaMaximumRecords))
      val wrongCatalog = withCatalog(prepared).map { relation =>
        if relation.table == "fp_script_catalog" then
          val row = relation.rows.head
          relation.copy(rows = Vector(replaceCell(row, relation.table, "catalog_revision", MySqlFormat3Cell.SignedInteger(1L))))
        else relation
      }
      val invalidCertify = capture(wrongCatalog, root)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapGraph.certifyTarget(invalidCertify,
        Inputs, root, Plan, SourceWorldIdentity, ReceiptSha256, MarkerAckSha256, actualPreparedWriter,
        certifyWriter, ExpectedQuotaMaximumRecords))
    finally
      closeTarget(certified)
      closeTarget(prepared)
      root.closeRootReadBudget()
