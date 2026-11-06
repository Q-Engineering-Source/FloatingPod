package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.mysql.MySqlScriptSchemaDataTestFixture as Fixture
import com.wi.floatingpod.server.{DbFirstWorldAdoptionPlanData, WindowsWorldProvisionReceiptDataCodec}
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Full27 catalog genesis is a schema6 DATA transition, not a widened Full26 proof. */
final class MySqlDbFirstFreshPrepareCompositionTest:
  private val QuotaMaximumRecords = 8L
  private val Binding = "fp_script_catalog"
  private val Manifest = "fp_schema_manifest"
  private val PhysicalBinding = "physical_recovery_binding"
  private val BootstrapIntent = "binding_bootstrap_intent"
  private val Quota = MySqlDeploymentReceiptSchemaDefinition.QuotaTable
  private val PodSnapshot = MySqlDbFirstSchemaDefinition.SnapshotTable

  private lazy val ProfileInputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String): Account = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs(Fixture.Schema,
      Accounts(account("fp_s5_guard"), account("fp_s5_audit"), account("fp_s5_runtime"),
        account("fp_s5_provisioner"), account("fp_s5_importer"), account("fp_s5_quota")),
      Fixture.SourceProfile, Fixture.Inputs.budget,
      MySqlDeploymentReceiptSchemaDefinition.Budget(1_000_000L, 1_000_000L),
      QuotaMaximumRecords, Vector.empty)
  private lazy val Rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(ProfileInputs)
  private lazy val Inputs = MySqlScriptSchemaInputs.createDbFirstFull27(Rendered)
  private def id(value: String): UUID = UUID.fromString(value)
  private lazy val Plan = new DbFirstWorldAdoptionPlanData(
    id("10000000-0000-4000-8000-000000000001"), id("10000000-0000-4000-8000-000000000002"),
    id("10000000-0000-4000-8000-000000000003"), id("10000000-0000-4000-8000-000000000004"),
    id("10000000-0000-4000-8000-000000000005"), MySqlDbFirstSchemaDefinition.PhysicalBindingFormat,
    Fixture.Connection.serverUuid, Inputs.schema, Inputs.schemaFormat, Inputs.ddlSha256.toUpperCase(java.util.Locale.ROOT),
    id("20000000-0000-4000-8000-000000000001"))
  private lazy val WrongBindingPlan = new DbFirstWorldAdoptionPlanData(
    Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.certifyWriteUuid(),
    id("10000000-0000-4000-8000-000000000006"), MySqlDbFirstSchemaDefinition.PhysicalBindingFormat,
    Fixture.Connection.serverUuid, Inputs.schema, Inputs.schemaFormat, Inputs.ddlSha256.toUpperCase(java.util.Locale.ROOT),
    Plan.provisioningUuid())
  private val Boot = id("30000000-0000-4000-8000-000000000001")
  private val Now = LocalDateTime.of(2026, 11, 5, 12, 0)
  private val SourceWorld = "world://source/primary"
  private val ReceiptSha256 = digest("managed-install-receipt".getBytes(java.nio.charset.StandardCharsets.US_ASCII)).toVector
  private lazy val BeforeWriter = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private lazy val TargetWriter = MySqlFreshFormat3WriterTarget(
    Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private def replace(relations: Vector[MySqlFormat3RelationSnapshot], table: String, column: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val shape = Inputs.forTable(table)
    val index = shape.columns.indexWhere(_.name == column)
    require(index >= 0, s"$table.$column belongs to the Full27 layout")
    relations.map { relation =>
      if relation.table != table then relation
      else relation.copy(rows = relation.rows.map(_.updated(index, value)))
    }

  private def full27IntermediateRows(): Vector[MySqlFormat3RelationSnapshot] =
    val old = Fixture.full26GenesisRelations(Fixture.Full26Inputs, catalogPresent = false)
      .iterator.map(relation => relation.table -> relation).toMap
    var relations = Inputs.canonicalOrder.map { shape =>
      if shape.table == PodSnapshot then MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
      else old.getOrElse(shape.table, throw new AssertionError(s"Full26 source is missing ${shape.table}"))
    }
    relations = replace(relations, Manifest, "format_version", MySqlFormat3Cell.SignedInteger(6L))
    relations = replace(relations, Manifest, "ddl_sha256", binary(HexFormat.of().parseHex(Inputs.ddlSha256)))
    relations = replace(relations, Manifest, "expected_binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    relations = replace(relations, Manifest, "provisioning_uuid", binary(uuidBytes(Plan.provisioningUuid())))
    relations = replace(relations, Manifest, "source_provenance_kind", MySqlFormat3Cell.Text("MANAGED_INSTALL"))
    relations = replace(relations, PhysicalBinding, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    relations = replace(relations, PhysicalBinding, "format_version",
      MySqlFormat3Cell.SignedInteger(MySqlDbFirstSchemaDefinition.PhysicalBindingFormat.toLong))
    relations = replace(relations, BootstrapIntent, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    relations = replace(relations, BootstrapIntent, "provisioning_uuid", binary(uuidBytes(Plan.provisioningUuid())))
    relations = replace(relations, BootstrapIntent, "binding_format_version",
      MySqlFormat3Cell.SignedInteger(MySqlDbFirstSchemaDefinition.PhysicalBindingFormat.toLong))
    relations = replace(relations, "fp_runtime_writer", "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    relations = replace(relations, "fp_runtime_writer", "lease_until", MySqlFormat3Cell.DateTime(Now))
    relations = replace(relations, Quota, "quota_revision", MySqlFormat3Cell.SignedInteger(0L))
    relations

  private def initialCatalog(): Vector[MySqlFormat3Cell] =
    val binding = Plan.bindingUuid()
    val empty = Array.emptyByteArray
    Inputs.forTable(Binding).columns.map(_.name).map {
      case "binding_uuid" => binary(uuidBytes(binding))
      case "catalog_revision" => MySqlFormat3Cell.SignedInteger(0L)
      case "snapshot_bytes" => binary(empty)
      case "snapshot_sha256" => binary(digest(empty))
      case "next_version" => MySqlFormat3Cell.SignedInteger(1L)
      case "last_mutation_operation_uuid" => MySqlFormat3Cell.SqlNull
      case other => throw new AssertionError(s"unexpected closed catalog column $other")
    }

  private def withCatalog(rows: Vector[MySqlFormat3RelationSnapshot]): Vector[MySqlFormat3RelationSnapshot] =
    rows.map(relation => if relation.table == Binding then relation.copy(rows = Vector(initialCatalog())) else relation)

  private def independentEffect(before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot): Array[Byte] =
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    def string(value: String): Unit =
      val encoded = value.getBytes(StandardCharsets.UTF_8)
      out.writeInt(encoded.length); out.write(encoded)
    def uuid(value: UUID): Unit =
      out.writeLong(value.getMostSignificantBits); out.writeLong(value.getLeastSignificantBits)
    string("floatingpod.mysql.db-first-full27-fresh-prepare-composition-effect/v1")
    string("PREPARE")
    uuid(Plan.creationOperationUuid()); uuid(Plan.bootstrapOperationUuid()); uuid(Plan.prepareWriteUuid())
    uuid(Plan.certifyWriteUuid()); uuid(Plan.bindingUuid()); out.writeInt(Plan.physicalFormat())
    uuid(Plan.mysqlServerUuid()); string(Plan.schema()); out.writeInt(Plan.schemaFormat())
    out.write(Plan.ddlSha256().getBytes(StandardCharsets.US_ASCII)); uuid(Plan.provisioningUuid())
    string(SourceWorld); out.writeInt(ReceiptSha256.size); ReceiptSha256.foreach(value => out.writeByte(value.toInt))
    out.writeLong(before.canonicalByteLength); out.writeInt(before.canonicalSha256.size)
    before.canonicalSha256.foreach(value => out.writeByte(value.toInt))
    Vector(PhysicalBinding,BootstrapIntent,Binding).foreach { table =>
      string(table); val rows = target.relation(table).rows; out.writeInt(rows.size)
      rows.foreach { row => out.writeInt(row.size); row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_,out)) }
    }
    uuid(TargetWriter.bindingUuid); out.writeLong(TargetWriter.writerEpoch)
    TargetWriter.ownerBootUuid match
      case None => out.writeByte(0)
      case Some(boot) => out.writeByte(1); uuid(boot)
    val lease = TargetWriter.leaseUntil
    out.writeInt(lease.getYear); out.writeByte(lease.getMonthValue); out.writeByte(lease.getDayOfMonth)
    out.writeByte(lease.getHour); out.writeByte(lease.getMinute); out.writeByte(lease.getSecond); out.writeInt(lease.getNano)
    uuid(TargetWriter.operationUuid); out.writeByte(0)
    target.relations.foreach { relation =>
      if relation.table != PhysicalBinding && relation.table != BootstrapIntent && relation.table != Binding && relation.table != "fp_runtime_writer" then
        string(relation.table); out.writeInt(relation.rows.size)
        relation.rows.foreach { row => out.writeInt(row.size); row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_,out)) }
    }
    out.flush(); bytes.toByteArray

  private def empty(relations: Vector[MySqlFormat3RelationSnapshot], table: String): Vector[MySqlFormat3RelationSnapshot] =
    relations.map(relation => if relation.table == table then relation.copy(rows = Vector.empty) else relation)

  private def prepareBeforeRows(): Vector[MySqlFormat3RelationSnapshot] =
    empty(empty(full27IntermediateRows(), PhysicalBinding), BootstrapIntent)

  private def capture(root: MySqlFormat3ReadBudget,
      rows: Vector[MySqlFormat3RelationSnapshot]): MySqlScriptSchemaSnapshot =
    Fixture.snapshot(Inputs, root, rows)

  @Test def schema6GenesisChecksAllTwentySevenRelationsAndUsesItsOwnB1Family(): Unit =
    val root = Fixture.root(Inputs)
    var proof: MySqlScriptSchemaB1Graph.Data = null
    try
      val before = capture(root, full27IntermediateRows())
      val target = capture(root, withCatalog(full27IntermediateRows()))
      MySqlScriptSchemaB1Graph.requireDbFirstFull27FreshGenesisTransition(before, target, QuotaMaximumRecords)
      proof = MySqlScriptSchemaB1Graph.dbFirstFull27GenesisData(before, target, root, QuotaMaximumRecords)
      assertEquals(MySqlScriptSchemaB1Graph.TransitionKind.FreshGenesis, proof.kind)
      assertEquals(Inputs, proof.inputs)
      assertEquals(27, proof.inputs.canonicalOrder.size)
      assertEquals(480, proof.inputs.canonicalOrder.map(_.columns.size).sum)
      assertEquals(64, proof.sha256Hex.length)
      assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireFreshGenesisTransition(before, target))
      assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(before, target))
    finally
      if proof != null && !proof.storageBudget.isClosed then proof.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()

  @Test def schema6GenesisRejectsQuotaDriftAndUnchangedRelationDrift(): Unit =
    val root = Fixture.root(Inputs)
    try
      val beforeRows = full27IntermediateRows()
      val before = capture(root, beforeRows)
      val targetRows = withCatalog(beforeRows)
      val target = capture(root, targetRows)
      assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDbFirstFull27FreshGenesisTransition(before, target, QuotaMaximumRecords + 1L))
      val wrongCatalog = replace(targetRows, Binding, "catalog_revision", MySqlFormat3Cell.SignedInteger(1L))
      assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDbFirstFull27FreshGenesisTransition(before, capture(root, wrongCatalog), QuotaMaximumRecords))

      val changedWriter = replace(targetRows, "fp_runtime_writer", "writer_epoch",
        MySqlFormat3Cell.SignedInteger(1L))
      val mutated = capture(root, changedWriter)
      assertThrows(classOf[SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDbFirstFull27FreshGenesisTransition(before, mutated, QuotaMaximumRecords))
    finally root.closeRootReadBudget()

  @Test def dbFirstCompositionPublishesOnlyOneExactFull27PrepareImage(): Unit =
    val root = Fixture.root(Inputs)
    var target: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(root, prepareBeforeRows())
      target = MySqlDbFirstPrepareComposition.prepareTarget(before, Inputs, root, Plan,
        SourceWorld, ReceiptSha256, BeforeWriter, TargetWriter, QuotaMaximumRecords)
      MySqlDbFirstPrepareComposition.requirePrepareTransition(before, target, Inputs, root, Plan,
        SourceWorld, ReceiptSha256, BeforeWriter, TargetWriter, QuotaMaximumRecords)
      val context = MySqlDbFirstPrepareComposition.contextForPrepare(before, target, Inputs, root,
        Plan, SourceWorld, ReceiptSha256, TargetWriter, QuotaMaximumRecords)
      val expectedEffect = independentEffect(before, target)
      assertArrayEquals(digest(expectedEffect), context.expectedEffects.head.desiredEffectFingerprintSha256.toArray)
      assertFalse(java.util.Arrays.equals(digest(expectedEffect), context.canonicalFingerprintSha256.toArray))
      assertEquals(27, target.relations.size)
      assertEquals(480, target.relations.map(relation => Inputs.forTable(relation.table).columns.size).sum)
      val allowed = Set(PhysicalBinding, BootstrapIntent, "fp_runtime_writer", Binding)
      before.relations.zip(target.relations).foreach { (oldRelation, finalRelation) =>
        assertEquals(oldRelation.table, finalRelation.table)
        if !allowed.contains(oldRelation.table) then assertEquals(oldRelation, finalRelation, oldRelation.table)
      }
      assertEquals(1, target.relation(Binding).rows.size)
      assertEquals(0L, integer(target, Binding, "catalog_revision"))
      assertEquals(1L, integer(target, Binding, "next_version"))
      assertEquals(MySqlFormat3Cell.SqlNull, cell(target, Binding, "last_mutation_operation_uuid"))
      assertEquals(0, bytes(target, Binding, "snapshot_bytes").length)
      assertArrayEquals(digest(Array.emptyByteArray), bytes(target, Binding, "snapshot_sha256"))
      assertArrayEquals(uuidBytes(Plan.bindingUuid()), bytes(target, Binding, "binding_uuid"))
      assertTrue(target.relation(PodSnapshot).rows.isEmpty)
      assertEquals("LEGACY_UNCERTIFIED", text(target, PhysicalBinding, "cutover_state"))
      assertEquals(MySqlFormat3Cell.SqlNull, cell(target, BootstrapIntent, "marker_ack_sha256"))
      assertArrayEquals(HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex),
        bytes(target, Quota, "namespace"))
      assertEquals(QuotaMaximumRecords, integer(target, Quota, "max_records"))
      assertEquals(0L, integer(target, Quota, "used_records"))
      assertEquals(0L, integer(target, Quota, "quota_revision"))
      val writerFingerprint = bytes(target, "fp_runtime_writer", "last_commit_fingerprint")
      assertArrayEquals(context.canonicalFingerprintSha256.toArray, writerFingerprint)
      assertEquals("PREPARED", text(target, BootstrapIntent, "stage"))
    finally
      if target != null && (target.allocationBudget ne root) && !target.allocationBudget.isClosed then
        target.allocationBudget.closeScopedReadBudget()
      root.closeRootReadBudget()

  @Test def dbFirstCompositionRejectsWrongQuotaPlanAndWriterIdentity(): Unit =
    val root = Fixture.root(Inputs)
    try
      val before = capture(root, prepareBeforeRows())
      assertThrows(classOf[SQLDataException], () =>
        MySqlDbFirstPrepareComposition.prepareTarget(before, Inputs, root, Plan,
          SourceWorld, ReceiptSha256, BeforeWriter, TargetWriter, QuotaMaximumRecords + 1L))
      val wrongWriter = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1),
        id("40000000-0000-4000-8000-000000000001"))
      assertThrows(classOf[SQLDataException], () =>
        MySqlDbFirstPrepareComposition.prepareTarget(before, Inputs, root, Plan,
          SourceWorld, ReceiptSha256, BeforeWriter, wrongWriter, QuotaMaximumRecords))
      assertThrows(classOf[SQLDataException], () =>
        MySqlDbFirstPrepareComposition.prepareTarget(before, Inputs, root, WrongBindingPlan,
          SourceWorld, ReceiptSha256, BeforeWriter, TargetWriter, QuotaMaximumRecords))
      val legacyPlan = new WindowsWorldProvisionReceiptDataCodec.PlanData(
        id("50000000-0000-4000-8000-000000000001"), id("50000000-0000-4000-8000-000000000002"),
        id("50000000-0000-4000-8000-000000000003"), id("50000000-0000-4000-8000-000000000004"),
        id("50000000-0000-4000-8000-000000000005"), 2, Fixture.Connection.serverUuid,
        Inputs.schema, 5, Inputs.ddlSha256, id("60000000-0000-4000-8000-000000000001"))
      assertThrows(classOf[SQLDataException], () =>
        MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before, Inputs, root, legacyPlan,
          SourceWorld, ReceiptSha256, BeforeWriter, TargetWriter))
    finally root.closeRootReadBudget()

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell =
    snapshot.relation(table).rows.head(Inputs.forTable(table).columns.indexWhere(_.name == column))

  private def integer(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): Long = cell(snapshot, table, column) match
    case MySqlFormat3Cell.SignedInteger(value) => value
    case _ => throw new AssertionError(s"$table.$column must be an integer")

  private def text(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): String = cell(snapshot, table, column) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => throw new AssertionError(s"$table.$column must be text")

  private def bytes(snapshot: MySqlScriptSchemaSnapshot, table: String, column: String): Array[Byte] =
    cell(snapshot, table, column) match
      case MySqlFormat3Cell.Binary(value) => Array.tabulate(value.length)(value.byteAt)
      case _ => throw new AssertionError(s"$table.$column must be binary")

  private def binary(bytes: Array[Byte]): MySqlFormat3Cell =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.clone()))

  private def digest(bytes: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(bytes)

  private def uuidBytes(value: UUID): Array[Byte] =
    java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
