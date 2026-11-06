package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 真实 Full26 DATA 边界；ACK/hash/不透明历史字节均不授予 native 或 SQL 权限。 */
final class MySqlDeploymentReceiptBootstrapGraphTest:
  import MySqlScriptSchemaDataTestFixture.*
  private val I = Full26Inputs
  private val B = "physical_recovery_binding"
  private val B1 = "binding_bootstrap_intent"
  private val W = "fp_runtime_writer"
  private val Manifest = "fp_schema_manifest"
  private val Catalog = "fp_script_catalog"
  private val History = "fp_recovery_operation"
  private val Source = "world://source/primary"
  private val Receipt = Vector.fill[Byte](32)(0x44.toByte)
  private val Ack = Vector.fill[Byte](32)(0x55.toByte)
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private val Boot = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c1")
  private val Plan = new WindowsWorldProvisionReceiptDataCodec.PlanData(
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c701"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c702"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c703"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c704"),
    UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c705"),
    2, Connection.serverUuid, I.schema, 5, I.ddlSha256, ProvisioningUuid)
  private val Seed = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private val PrepareW = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private def update(rows: Vector[MySqlFormat3RelationSnapshot], table: String,
      column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val index = I.forTable(table).columns.indexWhere(_.name == column)
    require(index >= 0, s"real column $table.$column")
    rows.map(r => if r.table == table then r.copy(rows = r.rows.map(_.updated(index, value))) else r)
  private def replaceRows(rows: Vector[MySqlFormat3RelationSnapshot], table: String,
      values: Vector[Vector[MySqlFormat3Cell]]): Vector[MySqlFormat3RelationSnapshot] =
    rows.map(r => if r.table == table then r.copy(rows = values) else r)
  private def cell(s: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell =
    s.relation(table).rows.head(I.forTable(table).columns.indexWhere(_.name == column))
  private def bytes(value: MySqlFormat3Cell): Array[Byte] = value match
    case MySqlFormat3Cell.Binary(b) => Array.tabulate(b.length)(b.byteAt)
    case _ => throw new AssertionError("binary expected")
  private def seedRows: Vector[MySqlFormat3RelationSnapshot] =
    var rows = full26GenesisRelations(I, catalogPresent = true)
    rows = replaceRows(replaceRows(rows, B, Vector.empty), B1, Vector.empty)
    rows = update(rows, Manifest, "expected_binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows, Catalog, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows, W, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows, W, "lease_until", MySqlFormat3Cell.DateTime(Now))
    rows
  private def capture(rows: Vector[MySqlFormat3RelationSnapshot], budget: MySqlFormat3ReadBudget,
      inputs: MySqlScriptSchemaInputs = I): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema, Connection, Scope, inputs.budget.packetBytes, inputs, rows, budget)
  private def prepare(before: MySqlScriptSchemaSnapshot, budget: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData = Plan,
      writer: MySqlFreshFormat3WriterTarget = PrepareW): MySqlScriptSchemaSnapshot =
    MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before, I, budget, plan, Source, Receipt, Seed, writer)
  private def preparedWriter(s: MySqlScriptSchemaSnapshot): MySqlBusinessWriterRow =
    MySqlBusinessWriterRow(Plan.bindingUuid(), 1L, Some(Boot), PrepareW.leaseUntil,
      Some(Plan.prepareWriteUuid()), Some(bytes(cell(s, W, "last_commit_fingerprint")).toVector), Now)
  private def certifyWriter = PrepareW.copy(operationUuid = Plan.certifyWriteUuid())
  private def certify(before: MySqlScriptSchemaSnapshot, budget: MySqlFormat3ReadBudget,
      writer: MySqlFreshFormat3WriterTarget = certifyWriter): MySqlScriptSchemaSnapshot =
    MySqlDeploymentReceiptBootstrapGraph.certifyTarget(before, I, budget, Plan, Source, Receipt, Ack, preparedWriter(before), writer)
  private def closeTarget(s: MySqlScriptSchemaSnapshot): Unit =
    if s != null && !s.allocationBudget.isClosed then s.allocationBudget.closeScopedReadBudget()
  private def unchanged(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Unit =
    assertEquals(26, target.relations.size)
    before.relations.filterNot(r => Set(B, B1, W).contains(r.table)).foreach { r =>
      assertEquals(r, target.relation(r.table), r.table)
    }
  private def publicationRow: Vector[MySqlFormat3Cell] =
    val operation = UUID.fromString("00000000-0000-0000-0000-000000000835")
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000632")
    val context = MySqlOperationContext(boot, 4L, Plan.bindingUuid(), operation,
      Vector(MySqlExpectedEffect(s"fp_deployment_operation/$operation/SCRIPT_PUBLICATION", 0L, digest(Array[Byte](4,5,6)).toVector)))
    val encoded = encodeGenericContext(context)
    var rows = Vector(MySqlFormat3RelationSnapshot(History, Vector(legacyHistoryRow(operation, "SCRIPT_PUBLICATION"))))
    rows = update(rows, History, "binding_uuid", binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows, History, "original_context_bytes", binary(encoded))
    rows = update(rows, History, "original_context_sha256", binary(digest(encoded)))
    rows.head.rows.head

  @Test def prepareAndCertifyKeepAllTwentyThreeLegalRelationsAndExactWriterSemantics(): Unit =
    val allocation = root(I)
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows, allocation)
      prepared = prepare(before, allocation)
      unchanged(before, prepared)
      assertEquals(MySqlFormat3Cell.Text("LEGACY_UNCERTIFIED"), cell(prepared, B, "cutover_state"))
      assertEquals(MySqlFormat3Cell.Text("PREPARED"), cell(prepared, B1, "stage"))
      assertEquals(MySqlFormat3Cell.SqlNull, cell(prepared, B1, "marker_ack_sha256"))
      assertArrayEquals(uuidBytes(Plan.prepareWriteUuid()), bytes(cell(prepared, W, "last_commit_operation_uuid")))
      MySqlDeploymentReceiptBootstrapGraph.requirePrepareTransition(before, prepared, I, allocation, Plan, Source, Receipt, Seed, PrepareW)
      certified = certify(prepared, allocation)
      unchanged(prepared, certified)
      assertEquals(MySqlFormat3Cell.Text("CERTIFIED"), cell(certified, B, "cutover_state"))
      assertEquals(MySqlFormat3Cell.Text("COMPLETE"), cell(certified, B1, "stage"))
      assertArrayEquals(Ack.toArray, bytes(cell(certified, B1, "marker_ack_sha256")))
      assertArrayEquals(uuidBytes(Plan.certifyWriteUuid()), bytes(cell(certified, W, "last_commit_operation_uuid")))
      Vector("binding_uuid", "writer_epoch", "owner_boot_uuid", "lease_until").foreach(c => assertEquals(cell(prepared,W,c), cell(certified,W,c)))
      MySqlDeploymentReceiptBootstrapGraph.requireCertifyTransition(prepared, certified, I, allocation, Plan, Source, Receipt, Ack, preparedWriter(prepared), certifyWriter)
    finally
      closeTarget(certified); closeTarget(prepared); allocation.closeRootReadBudget()

  @Test def certifyPreservesGenuineRecoveryOnlyScriptPublicationWithOpaqueDiagnostics(): Unit =
    val allocation = root(I)
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    try
      prepared = prepare(capture(seedRows, allocation), allocation)
      var rows = replaceRows(prepared.relations, History, Vector(publicationRow))
      val diagnostic = "opaque actual DATA diagnostic".getBytes(StandardCharsets.UTF_8)
      rows = update(rows, History, "original_diagnostic_identity", binary(diagnostic))
      rows = update(rows, History, "original_diagnostic_sha256", binary(digest(diagnostic)))
      val before = capture(rows, allocation) // real accepted row/context/history validator, no fabricated snapshot
      certified = certify(before, allocation)
      unchanged(before, certified)
      assertEquals(1, certified.relation(History).rows.size)
      assertArrayEquals(diagnostic, bytes(cell(certified, History, "original_diagnostic_identity")))
      assertEquals(MySqlFormat3Cell.SignedInteger(0L), cell(certified, MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "used_records"))
    finally
      closeTarget(certified); closeTarget(prepared); allocation.closeRootReadBudget()

  @Test def actualCaptureRejectsHistoryWithoutBindingAndDeploymentBeforeCertification(): Unit =
    val allocation = root(I)
    try
      val missingBinding = replaceRows(seedRows, History, Vector(publicationRow))
      assertThrows(classOf[SQLDataException], () => capture(missingBinding, allocation))
      val ordinary = full26OrdinaryRelations(I)
      val uncertified = update(ordinary, B, "cutover_state", MySqlFormat3Cell.Text("LEGACY_UNCERTIFIED"))
      assertThrows(classOf[SQLDataException], () => capture(uncertified, allocation))
    finally allocation.closeRootReadBudget()

  @Test def fixedPreparePlanAndNormalizedWriterRejectWrongFamilyAndEveryIdentityMismatch(): Unit =
    val allocation = root(I)
    try
      val before = capture(seedRows, allocation)
      val wrongId = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c799")
      for field <- Vector("server", "binding", "provisioning", "schema", "ddl", "format") do
        val p = new WindowsWorldProvisionReceiptDataCodec.PlanData(Plan.creationOperationUuid(), Plan.bootstrapOperationUuid(), Plan.prepareWriteUuid(), Plan.certifyWriteUuid(),
          if field == "binding" then wrongId else Plan.bindingUuid(), 2, if field == "server" then wrongId else Plan.mysqlServerUuid(),
          if field == "schema" then "other_schema" else Plan.schema(), if field == "format" then 3 else 5,
          if field == "ddl" then "AB" * 32 else Plan.ddlSha256(), if field == "provisioning" then wrongId else Plan.provisioningUuid())
        assertThrows(classOf[SQLDataException], () => prepare(before, allocation, p))
      assertThrows(classOf[IllegalArgumentException], () => new WindowsWorldProvisionReceiptDataCodec.PlanData(Plan.creationOperationUuid(),Plan.bootstrapOperationUuid(),Plan.prepareWriteUuid(),Plan.certifyWriteUuid(),Plan.bindingUuid(),3,Plan.mysqlServerUuid(),Plan.schema(),5,Plan.ddlSha256(),Plan.provisioningUuid()))
      for writer <- Vector(PrepareW.copy(bindingUuid = wrongId), PrepareW.copy(writerEpoch = 2L), PrepareW.copy(ownerBootUuid = None), PrepareW.copy(operationUuid = wrongId)) do
        assertThrows(classOf[SQLDataException], () => prepare(before, allocation, writer = writer))
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,I,allocation,Plan," ",Receipt,Seed,PrepareW))
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,I,allocation,Plan,Source,Vector.empty,Seed,PrepareW))
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.certifyTarget(before,I,allocation,Plan,Source,Receipt,Ack,Seed,certifyWriter))
    finally allocation.closeRootReadBudget()

  @Test def validatorsRejectTargetMutationWrongSourceReceiptAckAndWriterFingerprint(): Unit =
    val allocation = root(I)
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows, allocation); prepared = prepare(before, allocation); certified = certify(prepared, allocation)
      for (table, column, value) <- Vector(
          (Catalog, "catalog_revision", MySqlFormat3Cell.SignedInteger(7L)),
          (W, "last_commit_fingerprint", binary(Array.fill[Byte](32)(9))),
          (B1, "source_world_identity", MySqlFormat3Cell.Text("world://different")),
          (B1, "provision_receipt_sha256", binary(Array.fill[Byte](32)(9)))) do
        val altered = capture(update(prepared.relations, table, column, value), allocation)
        assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.requirePrepareTransition(before,altered,I,allocation,Plan,Source,Receipt,Seed,PrepareW))
      val wrongAck = capture(update(certified.relations,B1,"marker_ack_sha256",binary(Array.fill[Byte](32)(9))), allocation)
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.requireCertifyTransition(prepared,wrongAck,I,allocation,Plan,Source,Receipt,Ack,preparedWriter(prepared),certifyWriter))
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.requireCertifyTransition(prepared,certified,I,allocation,Plan,Source,Receipt,Vector.fill[Byte](32)(9),preparedWriter(prepared),certifyWriter))
    finally
      closeTarget(certified); closeTarget(prepared); allocation.closeRootReadBudget()

  @Test def everyFixedRecipeOperationIdentityIsCommittedByIntentOrWholeEffect(): Unit =
    val allocation = root(I)
    var target: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows,allocation); target = prepare(before,allocation)
      val changed = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c799")
      for field <- 0 until 4 do
        val ids = Vector(Plan.creationOperationUuid(),Plan.bootstrapOperationUuid(),Plan.prepareWriteUuid(),Plan.certifyWriteUuid()).updated(field,changed)
        val plan = new WindowsWorldProvisionReceiptDataCodec.PlanData(ids(0),ids(1),ids(2),ids(3),Plan.bindingUuid(),2,Plan.mysqlServerUuid(),Plan.schema(),5,Plan.ddlSha256(),Plan.provisioningUuid())
        assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.requirePrepareTransition(before,target,I,allocation,plan,Source,Receipt,Seed,PrepareW))
    finally
      closeTarget(target);allocation.closeRootReadBudget()

  @Test def certifyRequiresExistingMatchingCatalogAndUnchangedPrepareWriterTuple(): Unit =
    val allocation = root(I)
    var prepared: MySqlScriptSchemaSnapshot = null
    try
      prepared = prepare(capture(seedRows, allocation), allocation)
      val noCatalog = capture(replaceRows(prepared.relations,Catalog,Vector.empty), allocation)
      val foreignCatalog = capture(update(prepared.relations,Catalog,"binding_uuid",binary(uuidBytes(BindingUuid))), allocation)
      assertThrows(classOf[SQLDataException], () => certify(noCatalog, allocation))
      assertThrows(classOf[SQLDataException], () => certify(foreignCatalog, allocation))
      for target <- Vector(certifyWriter.copy(writerEpoch = 2L), certifyWriter.copy(ownerBootUuid = None), certifyWriter.copy(leaseUntil = Now.plusMinutes(2)), certifyWriter.copy(operationUuid = Plan.prepareWriteUuid())) do
        assertThrows(classOf[SQLDataException], () => certify(prepared, allocation, target))
    finally
      closeTarget(prepared); allocation.closeRootReadBudget()

  @Test def ownedTargetSurvivesSourceChildCloseAndRoundtripsCompleteFull26(): Unit =
    val allocation = root(I)
    var payload: MySqlScriptSchemaCanonicalPayload = null
    var source: MySqlScriptSchemaSnapshot = null
    var target: MySqlScriptSchemaSnapshot = null
    var targetPayload: MySqlScriptSchemaCanonicalPayload = null
    var decoded: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows, allocation)
      payload = MySqlScriptSchemaSnapshotCodec.encode(before, allocation)
      source = MySqlScriptSchemaSnapshotCodec.decode(payload,I,allocation)
      target = prepare(source, allocation)
      val canonical = target.canonicalSha256
      closeTarget(source); source = null
      payload.storageBudget.closeScopedReadBudget(); payload = null
      assertEquals(canonical, target.canonicalSha256)
      targetPayload = MySqlScriptSchemaSnapshotCodec.encode(target, allocation)
      decoded = MySqlScriptSchemaSnapshotCodec.decode(targetPayload,I,allocation)
      assertEquals(target.relations, decoded.relations)
      assertEquals(target.canonicalSha256, decoded.canonicalSha256)
      // No payload binary in the target aliases an owner-backed source binary.
      assertArrayEquals(Receipt.toArray, bytes(cell(target,B1,"provision_receipt_sha256")))
    finally
      closeTarget(decoded)
      if targetPayload != null then targetPayload.storageBudget.closeScopedReadBudget()
      closeTarget(target); closeTarget(source)
      if payload != null then payload.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def exactRootInputsProfileAndClosedChildAreMandatory(): Unit =
    val ledger = new MySqlFormat3AllocationLedger(SourceProfile.maxRetainedBytes())
    val allocation = root(I,ledger); val sibling = root(I,ledger)
    try
      val before = capture(seedRows,allocation)
      assertThrows(classOf[SQLDataException], () => prepare(before,sibling))
      val foreignPayload = MySqlScriptSchemaSnapshotCodec.encode(capture(seedRows,sibling),sibling)
      val foreignChild = MySqlScriptSchemaSnapshotCodec.decode(foreignPayload,I,sibling)
      try assertThrows(classOf[SQLDataException], () => prepare(foreignChild,allocation))
      finally
        closeTarget(foreignChild);foreignPayload.storageBudget.closeScopedReadBudget()
      val copyInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(I.schema,I.ddlSha256,SourceProfile,I.budget,I.deploymentBudget.get)
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,copyInputs,allocation,Plan,Source,Receipt,Seed,PrepareW))
      val otherProfile = com.wi.floatingpod.config.SourceLimitProfile.decode(SourceProfile.canonicalBytes(),SourceProfile.sha256())
      val otherInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(I.schema,I.ddlSha256,otherProfile,I.budget,I.deploymentBudget.get)
      assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,otherInputs,allocation,Plan,Source,Receipt,Seed,PrepareW))
      val payload = MySqlScriptSchemaSnapshotCodec.encode(before,allocation)
      val child = MySqlScriptSchemaSnapshotCodec.decode(payload,I,allocation)
      closeTarget(child); payload.storageBudget.closeScopedReadBudget()
      assertThrows(classOf[SQLDataException], () => prepare(child,allocation))
      allocation.closeRootReadBudget()
      assertThrows(classOf[SQLDataException], () => prepare(before,allocation))
    finally
      if !allocation.isClosed then allocation.closeRootReadBudget()
      sibling.closeRootReadBudget()

  @Test def budgetFailureReleasesTemporaryAndUnpublishedTargetCharges(): Unit =
    val allocation = root(I)
    try
      val before = capture(seedRows, allocation)
      before.canonicalSha256 // stable retained digest is not a failed-target debt
      val baseline = allocation.accountedMemoryBytes
      val hold = allocation.availableMemoryBytes - 66000L
      allocation.reserveRetainedMemory(hold,"test hold leaves insufficient target workspace")
      val held = allocation.accountedMemoryBytes
      try
        assertThrows(classOf[SQLDataException], () => prepare(before,allocation))
        assertEquals(held,allocation.accountedMemoryBytes)
      finally allocation.releaseRetainedMemory(hold,"test hold dropped")
      assertEquals(baseline,allocation.accountedMemoryBytes)
    finally allocation.closeRootReadBudget()

  @Test def effectFingerprintMatchesIndependentCompletePreimageFraming(): Unit =
    val allocation = root(I)
    var target: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows,allocation); target = prepare(before,allocation)
      val sink = new ByteArrayOutputStream(); val out = new DataOutputStream(sink)
      def text(v: String): Unit = { val b=v.getBytes(StandardCharsets.UTF_8);out.writeInt(b.length);out.write(b) }
      def uuid(v: UUID): Unit = { out.writeLong(v.getMostSignificantBits);out.writeLong(v.getLeastSignificantBits) }
      def rowCell(v: MySqlFormat3Cell): Unit = v match
        case MySqlFormat3Cell.SqlNull => out.writeByte(0)
        case MySqlFormat3Cell.Text(v) => out.writeByte(1);text(v)
        case b: MySqlFormat3Cell.Binary => out.writeByte(2);val raw=bytes(b);out.writeInt(raw.length);out.write(raw)
        case MySqlFormat3Cell.SignedInteger(v) => out.writeByte(3);out.writeLong(v)
        case _ => throw new AssertionError("B/B1 oracle has only exact null/text/binary/integer fields")
      def rows(table: String, snapshot: MySqlScriptSchemaSnapshot): Unit =
        text(table);val values=snapshot.relation(table).rows;out.writeInt(values.size)
        values.foreach { row => out.writeInt(row.size);row.foreach(rowCell) }
      text("floatingpod.mysql.script-publication1-deployment-receipt1-b1-effect/v1");text("PREPARE")
      out.flush()
      // Fixed independent Python struct/hashlib vector for the exact domain/phase framing.
      assertEquals(85,sink.size())
      assertEquals("0FE52C617006C235ED0D0A79219A0BC7ACDFAAD1BEC9F480CE762742EB972999",
        HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(sink.toByteArray)))
      Vector(Plan.creationOperationUuid(),Plan.bootstrapOperationUuid(),Plan.prepareWriteUuid(),Plan.certifyWriteUuid(),Plan.bindingUuid()).foreach(uuid)
      out.writeInt(2);uuid(Plan.mysqlServerUuid());text(Plan.schema());out.writeInt(5)
      out.write(Plan.ddlSha256().toUpperCase(java.util.Locale.ROOT).getBytes(StandardCharsets.US_ASCII));uuid(Plan.provisioningUuid())
      text(Source);out.writeInt(32);out.write(Receipt.toArray);out.writeLong(before.canonicalByteLength);out.writeInt(32);out.write(before.canonicalSha256.toArray)
      rows(B,before);rows(B1,before);rows(B,target);rows(B1,target)
      uuid(PrepareW.bindingUuid);out.writeLong(1L);out.writeByte(1);uuid(Boot)
      val t=PrepareW.leaseUntil;out.writeInt(t.getYear);Vector(t.getMonthValue,t.getDayOfMonth,t.getHour,t.getMinute,t.getSecond).foreach(out.writeByte);out.writeInt(t.getNano)
      uuid(Plan.prepareWriteUuid());out.writeByte(0);out.flush()
      val expected=MessageDigest.getInstance("SHA-256").digest(sink.toByteArray)
      val context=MySqlDeploymentReceiptBootstrapGraph.contextForTransition("PREPARE",before,target,Plan,Source,Receipt,PrepareW)
      assertArrayEquals(expected,context.expectedEffects.head.desiredEffectFingerprintSha256.toArray)
      assertFalse(java.util.Arrays.equals(expected,context.canonicalFingerprintSha256.toArray))
      assertArrayEquals(context.canonicalFingerprintSha256.toArray,bytes(cell(target,W,"last_commit_fingerprint")))
      Vector(PrepareW.copy(ownerBootUuid=Some(UUID.randomUUID())),PrepareW.copy(writerEpoch=2L),
        PrepareW.copy(bindingUuid=UUID.randomUUID()),PrepareW.copy(operationUuid=UUID.randomUUID())).foreach { changed=>
        assertThrows(classOf[Exception],()=>MySqlDeploymentReceiptBootstrapGraph.contextForTransition("PREPARE",before,target,Plan,Source,Receipt,changed))
      }
      assertThrows(classOf[Exception],()=>MySqlDeploymentReceiptBootstrapGraph.contextForTransition("PREPARE",before,target,Plan,Source,Vector.fill(32)(9.toByte),PrepareW))
      assertEquals(70, "floatingpod.mysql.script-publication1-deployment-receipt1-b1-effect/v1".getBytes(StandardCharsets.UTF_8).length)
    finally
      closeTarget(target);allocation.closeRootReadBudget()
