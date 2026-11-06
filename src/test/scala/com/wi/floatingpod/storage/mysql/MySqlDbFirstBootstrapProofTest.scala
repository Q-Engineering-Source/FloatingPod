package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{DbFirstBootstrapRecordCodec, DbFirstWorldAdoptionPlanData,
  DbFirstWorldAdoptionReceiptCodec, WindowsMarkerIo, WindowsWorldProvisioner}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Real typed Full27 graph fixtures; this test verifies DATA transitions only. */
final class MySqlDbFirstBootstrapProofTest:
  import DbFirstBootstrapRecordCodec.{Kind, MarkerData, Outcome, Phase, RecordData, SettlementData, WitnessRef}
  import MySqlScriptSchemaDataTestFixture as Fixture

  private val Configuration = DeploymentReceiptStaticTestData.defaultProfile()
  private val Rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(Configuration)
  private val Inputs = MySqlScriptSchemaInputs.createDbFirstFull27(Rendered)
  private val Full26Inputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
    Configuration.schema, MySqlDeploymentReceiptSchemaProfile.render(Configuration).sha256,
    Configuration.sourceLimitProfile, Configuration.publicationBudget, Configuration.deploymentBudget)
  private val QuotaMaximumRecords = Configuration.quotaMaximumRecords
  private val BindingTable = "physical_recovery_binding"
  private val IntentTable = "binding_bootstrap_intent"
  private val WriterTable = "fp_runtime_writer"
  private val CatalogTable = "fp_script_catalog"
  private val SnapshotTable = MySqlDbFirstSchemaDefinition.SnapshotTable
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private val Boot = Fixture.Scope.bootUuid
  private val NewBoot = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c2")
  private val OtherObserverBoot = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c3")
  private val Plan = new DbFirstWorldAdoptionPlanData(
    id("10000000-0000-4000-8000-000000000001"), id("10000000-0000-4000-8000-000000000002"),
    id("10000000-0000-4000-8000-000000000003"), id("10000000-0000-4000-8000-000000000004"),
    DeploymentReceiptStaticTestData.binding, MySqlDbFirstSchemaDefinition.PhysicalBindingFormat,
    Fixture.Connection.serverUuid, Inputs.schema, Inputs.schemaFormat, Inputs.ddlSha256,
    DeploymentReceiptStaticTestData.provisioning)
  private val SourceFileId = fileId("00000000000000000000000000000001")
  private val ParentFileId = fileId("00000000000000000000000000000002")
  private val DataFileId = fileId("00000000000000000000000000000003")
  private val CustodyFileId = fileId("00000000000000000000000000000004")
  private val OldMarkerFileId = fileId("00000000000000000000000000000005")
  private val NewMarkerFileId = fileId("00000000000000000000000000000006")
  private val BeforeWitnessFileId = fileId("00000000000000000000000000000007")
  private val TargetWitnessFileId = fileId("00000000000000000000000000000008")
  private val ActualWitnessFileId = fileId("00000000000000000000000000000009")
  private val OldMarker = new DbFirstWorldAdoptionReceiptCodec.PreviousMarker(
    true, OldMarkerFileId, 2, id("11111111-1111-4111-8111-111111111111"))
  private val Namespace = "fpb1:" + "A" * 43

  private def id(value: String): UUID = UUID.fromString(value)
  private def root(inputs: MySqlScriptSchemaInputs = Inputs): MySqlFormat3ReadBudget =
    inputs.newAllocationRoot(inputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(inputs.sourceLimitProfile.maxRetainedBytes()))

  private def sourceFacts(configurationSha256: String = "0123456789abcdef" * 4,
      marker: DbFirstWorldAdoptionReceiptCodec.PreviousMarker = OldMarker): DbFirstWorldAdoptionReceiptCodec.SourceFacts =
    new DbFirstWorldAdoptionReceiptCodec.SourceFacts(Plan, SourceFileId, ParentFileId, DataFileId, CustodyFileId,
      marker, 1234L, 132537600000000000L,
      new WindowsWorldProvisioner.BootIdentity(Boot, Fixture.Scope.bootGeneration),
      configurationSha256, "fedcba9876543210" * 4)

  private def receiptSha(source: DbFirstWorldAdoptionReceiptCodec.SourceFacts): String =
    digestHex(DbFirstWorldAdoptionReceiptCodec.encode(source))

  private def capture(allocation: MySqlFormat3ReadBudget, rows: Vector[MySqlFormat3RelationSnapshot],
      scope: MySqlFormat3ObservationScope = Fixture.Scope,
      connection: MySqlFormat3ConnectionObservation = Fixture.Connection.copy(catalog = Inputs.schema)):
      MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Inputs.schema, connection, scope, Inputs.budget.packetBytes,
      Inputs, rows, allocation)

  private def captureFull26(allocation: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Full26Inputs.schema,
      Fixture.Connection.copy(catalog = Full26Inputs.schema), Fixture.Scope, Full26Inputs.budget.packetBytes,
      Full26Inputs, Fixture.full26GenesisRelations(Full26Inputs, catalogPresent = false), allocation)

  private def replaceCell(relations: Vector[MySqlFormat3RelationSnapshot], table: String, column: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val shape = Inputs.forTable(table)
    val index = shape.columns.indexWhere(_.name == column)
    require(index >= 0, s"$table.$column belongs to the exact Full27 shape")
    relations.map { relation =>
      if relation.table != table then relation
      else relation.copy(rows = relation.rows.map(_.updated(index, value)))
    }

  private def full27IntermediateRows(): Vector[MySqlFormat3RelationSnapshot] =
    val old = Fixture.full26GenesisRelations(Full26Inputs, catalogPresent = false)
      .iterator.map(relation => relation.table -> relation).toMap
    val rows = Inputs.canonicalOrder.map { shape =>
      shape.table match
        case SnapshotTable => MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
        case MySqlDeploymentReceiptSchemaDefinition.QuotaTable =>
          val quotaRow: Vector[MySqlFormat3Cell] = shape.columns.map(_.name match
            case "namespace" => Fixture.binary(HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))
            case "max_records" => MySqlFormat3Cell.SignedInteger(QuotaMaximumRecords)
            case "used_records" | "quota_revision" => MySqlFormat3Cell.SignedInteger(0L)
            case other => throw new AssertionError(s"unexpected quota column $other"))
          MySqlFormat3RelationSnapshot(shape.table, Vector(quotaRow))
        case name => old.getOrElse(name, MySqlFormat3RelationSnapshot(name, Vector.empty))
    }
    var result = replaceCell(rows, "fp_schema_manifest", "format_version", MySqlFormat3Cell.SignedInteger(6L))
    result = replaceCell(result, "fp_schema_manifest", "ddl_sha256", Fixture.binary(HexFormat.of().parseHex(Inputs.ddlSha256)))
    result = replaceCell(result, "fp_schema_manifest", "provisioning_uuid", Fixture.binary(uuidBytes(Plan.provisioningUuid())))
    result = replaceCell(result, "fp_schema_manifest", "expected_binding_uuid", Fixture.binary(uuidBytes(Plan.bindingUuid())))
    result = replaceCell(result, "fp_schema_manifest", "source_provenance_kind", MySqlFormat3Cell.Text("MANAGED_INSTALL"))
    result = replaceCell(result, "fp_schema_manifest", "expected_server_uuid", Fixture.binary(uuidBytes(Plan.mysqlServerUuid())))
    result = replaceCell(result, "fp_schema_manifest", "limit_profile_encoding",
      MySqlFormat3Cell.Text(com.wi.floatingpod.config.SourceLimitProfile.ENCODING))
    result = replaceCell(result, "fp_schema_manifest", "limit_profile_bytes",
      Fixture.binary(Inputs.sourceLimitProfile.canonicalBytes()))
    result = replaceCell(result, "fp_schema_manifest", "limit_profile_sha256",
      Fixture.binary(Inputs.sourceLimitProfile.sha256()))
    val profile = Inputs.sourceLimitProfile
    Vector(
      "max_region_bytes" -> profile.maxRegionBytes(),
      "max_chunk_bytes" -> profile.maxChunkBytes(),
      "max_decompressed_bytes" -> profile.maxDecompressedBytes(),
      "max_nbt_depth" -> profile.maxNbtDepth().toLong,
      "max_members" -> profile.maxMembers(),
      "max_retained_bytes" -> profile.maxRetainedBytes(),
      "max_sql_packet_bytes" -> profile.maxSqlPacketBytes(),
      "max_native_artifact_bytes" -> profile.maxNativeArtifactBytes()
    ).foreach { (column, value) =>
      result = replaceCell(result, "fp_schema_manifest", column, MySqlFormat3Cell.SignedInteger(value))
    }
    result = replaceCell(result, BindingTable, "binding_uuid", Fixture.binary(uuidBytes(Plan.bindingUuid())))
    result = replaceCell(result, BindingTable, "format_version",
      MySqlFormat3Cell.SignedInteger(MySqlDbFirstSchemaDefinition.PhysicalBindingFormat.toLong))
    result = replaceCell(result, IntentTable, "bootstrap_operation_uuid", Fixture.binary(uuidBytes(Plan.bootstrapOperationUuid())))
    result = replaceCell(result, IntentTable, "prepare_write_uuid", Fixture.binary(uuidBytes(Plan.prepareWriteUuid())))
    result = replaceCell(result, IntentTable, "certify_write_uuid", Fixture.binary(uuidBytes(Plan.certifyWriteUuid())))
    result = replaceCell(result, IntentTable, "binding_uuid", Fixture.binary(uuidBytes(Plan.bindingUuid())))
    result = replaceCell(result, IntentTable, "binding_format_version",
      MySqlFormat3Cell.SignedInteger(MySqlDbFirstSchemaDefinition.PhysicalBindingFormat.toLong))
    result = replaceCell(result, IntentTable, "provisioning_uuid", Fixture.binary(uuidBytes(Plan.provisioningUuid())))
    result = replaceCell(result, WriterTable, "binding_uuid", Fixture.binary(uuidBytes(Plan.bindingUuid())))
    result = replaceCell(result, WriterTable, "lease_until", MySqlFormat3Cell.DateTime(Now))
    result

  private def prepareBeforeRows(): Vector[MySqlFormat3RelationSnapshot] =
    full27IntermediateRows().map { relation =>
      if relation.table == BindingTable || relation.table == IntentTable then relation.copy(rows = Vector.empty)
      else relation
    }

  private def genesisCatalogRow: Vector[MySqlFormat3Cell] =
    Inputs.forTable(CatalogTable).columns.map(_.name match
      case "binding_uuid" => Fixture.binary(uuidBytes(Plan.bindingUuid()))
      case "catalog_revision" => MySqlFormat3Cell.SignedInteger(0L)
      case "snapshot_bytes" => Fixture.binary(Array.emptyByteArray)
      case "snapshot_sha256" => Fixture.binary(digest(Array.emptyByteArray))
      case "next_version" => MySqlFormat3Cell.SignedInteger(1L)
      case "last_mutation_operation_uuid" => MySqlFormat3Cell.SqlNull
      case other => throw new AssertionError(s"unexpected Full27 catalog column $other"))

  private def withCatalog(snapshot: MySqlScriptSchemaSnapshot): Vector[MySqlFormat3RelationSnapshot] =
    snapshot.relations.map { relation =>
      if relation.table == CatalogTable then relation.copy(rows = Vector(genesisCatalogRow)) else relation
    }

  private def prepareTarget(root: MySqlFormat3ReadBudget,
      before: MySqlScriptSchemaSnapshot,
      source: DbFirstWorldAdoptionReceiptCodec.SourceFacts): MySqlScriptSchemaSnapshot =
    val beforeWriter = MySqlDbFirstBootstrapProof.writer(before, Now)
    MySqlDbFirstPrepareComposition.prepareTarget(before, Inputs, root, Plan,
      source.selectedSaveFileId(), shaBytes(receiptSha(source)), beforeWriter,
      MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), beforeWriter.writerEpoch + 1L,
        Some(Boot), Now.plusMinutes(1), Plan.prepareWriteUuid()), QuotaMaximumRecords)

  private def certify(root: MySqlFormat3ReadBudget,
      before: MySqlScriptSchemaSnapshot,
      source: DbFirstWorldAdoptionReceiptCodec.SourceFacts): MySqlScriptSchemaSnapshot =
    val writer = MySqlDbFirstBootstrapProof.writer(before, Now)
    val targetWriter = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), writer.writerEpoch,
      writer.ownerBootUuid, writer.leaseUntil, Plan.certifyWriteUuid())
    MySqlDbFirstBootstrapGraph.certifyTarget(before, Inputs, root, Plan, source.selectedSaveFileId(),
      shaBytes(receiptSha(source)), shaBytes(expectedMarker(source).markerSha256()),
      writer, targetWriter, QuotaMaximumRecords)

  private def intent(phase: Phase, before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      source: DbFirstWorldAdoptionReceiptCodec.SourceFacts, marker: MarkerData = null,
      operation: UUID = null,
      ownerBoot: UUID = Boot, ownerGeneration: Long = Fixture.Scope.bootGeneration,
      databaseNow: LocalDateTime = Now,
      connectionGeneration: UUID = id("60000000-0000-4000-8000-000000000001")): RecordData =
    val actualOperation = if operation != null then operation else phase match
      case Phase.PREPARE => Plan.prepareWriteUuid()
      case Phase.CERTIFY => Plan.certifyWriteUuid()
      case _ => id("50000000-0000-4000-8000-000000000001")
    val sourceSha = receiptSha(source)
    new RecordData(phase, Kind.INTENT, actualOperation, sourceSha,
      owner(ownerBoot, ownerGeneration, sourceSha), Namespace, 321L, databaseNow,
      connectionGeneration, witness(BeforeWitnessFileId, before),
      witness(TargetWitnessFileId, target), null, marker)

  private def owner(boot: UUID, generation: Long, sourceSha: String): DbFirstBootstrapRecordCodec.OwnerData =
    new DbFirstBootstrapRecordCodec.OwnerData(boot, generation, 1234L, 132537600000000000L, sourceSha)

  private def expectedMarker(source: DbFirstWorldAdoptionReceiptCodec.SourceFacts,
      markerFileId: String = NewMarkerFileId): MarkerData =
    val bytes = WindowsMarkerIo.markerBytes(Plan.bindingUuid(), 2)
    new MarkerData(markerFileId, digestHex(bytes), source.oldMarker().present(),
      if source.oldMarker().present() then source.oldMarker().fileId() else null)

  private def witness(fileId: String, snapshot: MySqlScriptSchemaSnapshot): WitnessRef =
    new WitnessRef(fileId, snapshot.canonicalSha256Hex.toLowerCase(java.util.Locale.ROOT), snapshot.canonicalByteLength)

  private def settled(proof: MySqlDbFirstBootstrapProof.ValidatedPhase,
      actual: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      scope: MySqlFormat3ObservationScope, outcome: Outcome): SettlementData =
    val record = proof.record
    val result = new SettlementData(record, digestHex(DbFirstBootstrapRecordCodec.encodeRecord(record)),
      owner(scope.bootUuid, scope.bootGeneration, proof.sourceReceiptSha256), Namespace, 456L,
      Now.plusSeconds(5), id("70000000-0000-4000-8000-000000000001"),
      witness(ActualWitnessFileId, actual), outcome)
    MySqlDbFirstBootstrapProof.validateSettlement(result, proof, actual, root)
    result

  private def digest(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)
  private def digestHex(value: Array[Byte]): String = HexFormat.of().formatHex(digest(value))
  private def shaBytes(value: String): Vector[Byte] = HexFormat.of().parseHex(value).iterator.toVector
  private def uuidBytes(value: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
  private def fileId(hex: String): String = s"FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=$hex"
  private def close(snapshot: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): Unit =
    if snapshot != null && (snapshot.allocationBudget ne root) && !snapshot.allocationBudget.isClosed then
      snapshot.allocationBudget.closeScopedReadBudget()

  @Test def validatesActualPrepareAndCertifyGraphsAndDerivesTheirContexts(): Unit =
    val source = sourceFacts()
    val sourceSha = receiptSha(source)
    val allocation = root()
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(allocation, prepareBeforeRows())
      prepared = prepareTarget(allocation, before, source)
      val prepareRecord = intent(Phase.PREPARE, before, prepared, source)
      val prepareProof = MySqlDbFirstBootstrapProof.validate(prepareRecord, before, prepared, source,
        sourceSha, Inputs, allocation, QuotaMaximumRecords)
      assertEquals(prepareRecord, prepareProof.record)
      assertEquals(before, prepareProof.before)
      assertEquals(prepared, prepareProof.target)
      assertEquals(Plan.prepareWriteUuid(), prepareProof.context.operationUuid)
      assertEquals(1L, prepareProof.context.writerEpoch)
      assertEquals(Boot, MySqlDbFirstBootstrapProof.writer(prepared, Now).ownerBootUuid.get)

      val certifyBefore = capture(allocation, withCatalog(prepared))
      certified = certify(allocation, certifyBefore, source)
      val certifyRecord = intent(Phase.CERTIFY, certifyBefore, certified, source, expectedMarker(source))
      val certifyProof = MySqlDbFirstBootstrapProof.validate(certifyRecord, certifyBefore, certified, source,
        sourceSha, Inputs, allocation, QuotaMaximumRecords)
      assertEquals(Plan.certifyWriteUuid(), certifyProof.context.operationUuid)
      assertEquals(MySqlDbFirstBootstrapGraph.contextForTransition("CERTIFY", certifyBefore, certified,
        Plan, source.selectedSaveFileId(), shaBytes(sourceSha),
        MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1), Plan.certifyWriteUuid()),
        QuotaMaximumRecords), certifyProof.context)
    finally
      close(certified, allocation)
      close(prepared, allocation)
      allocation.closeRootReadBudget()

  @Test def rejectsWrongSourceOldFamilyPhaseAndWriterDriftAgainstRealFull27Images(): Unit =
    val source = sourceFacts()
    val sourceSha = receiptSha(source)
    val allocation = root()
    var prepared: MySqlScriptSchemaSnapshot = null
    var drifted: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(allocation, prepareBeforeRows())
      prepared = prepareTarget(allocation, before, source)
      val goodRecord = intent(Phase.PREPARE, before, prepared, source)
      val wrongSource = sourceFacts(configurationSha256 = "abcdef0123456789" * 4)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapProof.validate(goodRecord,
        before, prepared, wrongSource, receiptSha(wrongSource), Inputs, allocation, QuotaMaximumRecords))

      val wrongPhase = intent(Phase.PREPARE, before, prepared, source, operation = Plan.certifyWriteUuid())
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapProof.validate(wrongPhase,
        before, prepared, source, sourceSha, Inputs, allocation, QuotaMaximumRecords))
      val confirmed = new RecordData(Phase.PREPARE, Kind.CONFIRMED, Plan.prepareWriteUuid(), sourceSha,
        owner(Boot, Fixture.Scope.bootGeneration, sourceSha), Namespace, 321L, Now,
        id("60000000-0000-4000-8000-000000000001"), witness(BeforeWitnessFileId, before),
        witness(TargetWitnessFileId, prepared), "0123456789abcdef" * 4, null)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapProof.validate(confirmed,
        before, prepared, source, sourceSha, Inputs, allocation, QuotaMaximumRecords))

      val full26Root = root(Full26Inputs)
      try
        val full26 = captureFull26(full26Root)
        assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapProof.validate(goodRecord,
          full26, full26, source, sourceSha, Full26Inputs, full26Root, QuotaMaximumRecords))
      finally full26Root.closeRootReadBudget()

      val changedWriter = MySqlDbFirstBootstrapProof.writer(prepared, Now)
      val writerIndex = Inputs.forTable(WriterTable).columns.indexWhere(_.name == "writer_epoch")
      val driftRows = prepared.relations.map { relation =>
        if relation.table != WriterTable then relation
        else relation.copy(rows = Vector(relation.rows.head.updated(writerIndex,
          MySqlFormat3Cell.SignedInteger(changedWriter.writerEpoch + 1L))))
      }
      drifted = capture(allocation, driftRows)
      val driftRecord = intent(Phase.PREPARE, before, drifted, source)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstBootstrapProof.validate(driftRecord,
        before, drifted, source, sourceSha, Inputs, allocation, QuotaMaximumRecords))
    finally
      close(drifted, allocation)
      close(prepared, allocation)
      allocation.closeRootReadBudget()

  @Test def validatesHandoffAndReleaseOnlyThroughTheirFull27WriterGraph(): Unit =
    val source = sourceFacts()
    val sourceSha = receiptSha(source)
    val allocation = root()
    val handoffRoot = root()
    val handoffScope = MySqlFormat3ObservationScope(NewBoot, 2L, 9L)
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    var preparedHandoff: MySqlScriptSchemaSnapshot = null
    var certifiedHandoff: MySqlScriptSchemaSnapshot = null
    var released: MySqlScriptSchemaSnapshot = null
    try
      val prepareBefore = capture(allocation, prepareBeforeRows())
      prepared = prepareTarget(allocation, prepareBefore, source)
      val certifyBefore = capture(allocation, withCatalog(prepared))
      certified = certify(allocation, certifyBefore, source)

      val handoffNow = Now.plusMinutes(2)
      val expiredPreparedRows = replaceCell(prepared.relations, WriterTable, "lease_until",
        MySqlFormat3Cell.DateTime(Now.plusMinutes(1)))
      val expiredPrepared = capture(handoffRoot, expiredPreparedRows, handoffScope)
      val preparedWriter = MySqlDbFirstBootstrapProof.writer(expiredPrepared, handoffNow)
      val preparedFence = MySqlWriterFenceRow(Plan.bindingUuid(), preparedWriter.writerEpoch + 1L,
        Some(NewBoot), handoffNow.plusMinutes(1), preparedWriter.lastOperationUuid, preparedWriter.lastFingerprint)
      preparedHandoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(expiredPrepared, Inputs, handoffRoot,
        Plan, source.selectedSaveFileId(), shaBytes(sourceSha), None, preparedWriter, preparedFence,
        QuotaMaximumRecords)
      val preparedHandoffRecord = intent(Phase.HANDOFF, expiredPrepared, preparedHandoff, source,
        databaseNow = handoffNow, ownerBoot = NewBoot, ownerGeneration = 2L)
      val preparedHandoffProof = MySqlDbFirstBootstrapProof.validate(preparedHandoffRecord,
        expiredPrepared, preparedHandoff, source, sourceSha, Inputs, handoffRoot, QuotaMaximumRecords)
      assertEquals(preparedFence.epoch, preparedHandoffProof.context.writerEpoch)

      val expiredCertifiedRows = replaceCell(certified.relations, WriterTable, "lease_until",
        MySqlFormat3Cell.DateTime(Now.plusMinutes(1)))
      val expiredCertified = capture(handoffRoot, expiredCertifiedRows, handoffScope)
      val certifiedWriter = MySqlDbFirstBootstrapProof.writer(expiredCertified, handoffNow)
      val certifiedFence = MySqlWriterFenceRow(Plan.bindingUuid(), certifiedWriter.writerEpoch + 1L,
        Some(NewBoot), handoffNow.plusMinutes(1), certifiedWriter.lastOperationUuid, certifiedWriter.lastFingerprint)
      certifiedHandoff = MySqlDbFirstBootstrapWriterGraph.handoffTarget(expiredCertified, Inputs, handoffRoot,
        Plan, source.selectedSaveFileId(), shaBytes(sourceSha), Some(shaBytes(expectedMarker(source).markerSha256())),
        certifiedWriter, certifiedFence, QuotaMaximumRecords)
      val certifiedHandoffRecord = intent(Phase.HANDOFF, expiredCertified, certifiedHandoff, source,
        expectedMarker(source), databaseNow = handoffNow, ownerBoot = NewBoot, ownerGeneration = 2L)
      val certifiedHandoffProof = MySqlDbFirstBootstrapProof.validate(certifiedHandoffRecord,
        expiredCertified, certifiedHandoff, source, sourceSha, Inputs, handoffRoot, QuotaMaximumRecords)
      assertEquals(certifiedFence.epoch, certifiedHandoffProof.context.writerEpoch)

      val releaseWriter = MySqlDbFirstBootstrapProof.writer(certified, Now)
      val releaseTargetWriter = releaseWriter.copy(ownerBootUuid = None,
        leaseUntil = Now.minusNanos(1000L), databaseNow = Now)
      released = MySqlDbFirstBootstrapWriterGraph.releaseTarget(certified, Inputs, allocation, Plan,
        source.selectedSaveFileId(), shaBytes(sourceSha), Some(shaBytes(expectedMarker(source).markerSha256())),
        releaseWriter, releaseTargetWriter, QuotaMaximumRecords)
      val releaseRecord = intent(Phase.RELEASE, certified, released, source, expectedMarker(source))
      val releaseProof = MySqlDbFirstBootstrapProof.validate(releaseRecord, certified, released, source,
        sourceSha, Inputs, allocation, QuotaMaximumRecords)
      assertEquals(releaseWriter.writerEpoch, releaseProof.context.writerEpoch)
    finally
      close(released, allocation)
      close(certifiedHandoff, handoffRoot)
      close(preparedHandoff, handoffRoot)
      close(certified, allocation)
      close(prepared, allocation)
      handoffRoot.closeRootReadBudget()
      allocation.closeRootReadBudget()

  @Test def settlementUsesIndependentActualHeaderAndRejectsRowsThatDoNotMatchTheOutcome(): Unit =
    val source = sourceFacts()
    val sourceSha = receiptSha(source)
    val originalRoot = root()
    var prepared: MySqlScriptSchemaSnapshot = null
    val actualRoot = root()
    try
      val before = capture(originalRoot, prepareBeforeRows())
      prepared = prepareTarget(originalRoot, before, source)
      val record = intent(Phase.PREPARE, before, prepared, source)
      val proof = MySqlDbFirstBootstrapProof.validate(record, before, prepared, source,
        sourceSha, Inputs, originalRoot, QuotaMaximumRecords)

      val observer = owner(OtherObserverBoot, 2L, sourceSha)
      val observerScope = MySqlFormat3ObservationScope(OtherObserverBoot, 2L, 7L)
      val actual = capture(actualRoot, prepared.relations, observerScope)
      val settlement = new SettlementData(record, digestHex(DbFirstBootstrapRecordCodec.encodeRecord(record)),
        observer, Namespace, 456L, Now.plusSeconds(5), id("70000000-0000-4000-8000-000000000001"),
        witness(ActualWitnessFileId, actual), Outcome.APPLIED)
      MySqlDbFirstBootstrapProof.validateSettlement(settlement, proof, actual, actualRoot)

      val notAppliedImage = capture(actualRoot, before.relations, observerScope)
      val notApplied = new SettlementData(record, settlement.intentSha256(), observer, Namespace, 456L,
        Now.plusSeconds(5), id("70000000-0000-4000-8000-000000000001"),
        witness(ActualWitnessFileId, notAppliedImage), Outcome.NOT_APPLIED)
      MySqlDbFirstBootstrapProof.validateSettlement(notApplied, proof, notAppliedImage, actualRoot)
      val wrongOutcome = new SettlementData(record, settlement.intentSha256(), observer, Namespace, 456L,
        Now.plusSeconds(5), id("70000000-0000-4000-8000-000000000001"),
        witness(ActualWitnessFileId, notAppliedImage), Outcome.APPLIED)
      assertThrows(classOf[SQLDataException], () =>
        MySqlDbFirstBootstrapProof.validateSettlement(wrongOutcome, proof, notAppliedImage, actualRoot))

      val wrongObserver = owner(Boot, Fixture.Scope.bootGeneration, sourceSha)
      val wrongObserverRoot = root()
      try
        val wrongScope = capture(wrongObserverRoot, prepared.relations,
          MySqlFormat3ObservationScope(OtherObserverBoot, 2L, 8L))
        val wrongObserverSettlement = new SettlementData(record, settlement.intentSha256(), wrongObserver,
          Namespace, 456L, Now.plusSeconds(5), id("70000000-0000-4000-8000-000000000001"),
          witness(ActualWitnessFileId, wrongScope), Outcome.APPLIED)
        assertThrows(classOf[SQLDataException], () =>
          MySqlDbFirstBootstrapProof.validateSettlement(wrongObserverSettlement, proof, wrongScope, wrongObserverRoot))
      finally wrongObserverRoot.closeRootReadBudget()
    finally
      close(prepared, originalRoot)
      originalRoot.closeRootReadBudget()
      actualRoot.closeRootReadBudget()

  @Test def originalHistoryClassifiesFull27ReadbackAndRejectsForksDisconnectionsAndMultiplePendingTails(): Unit =
    val source = sourceFacts()
    val sourceSha = receiptSha(source)
    val originalRoot = root()
    val actualRoot = root()
    val secondRoot = root()
    val secondActualRoot = root()
    val observerScope = MySqlFormat3ObservationScope(OtherObserverBoot, 2L, 7L)
    var prepared: MySqlScriptSchemaSnapshot = null
    var certified: MySqlScriptSchemaSnapshot = null
    var secondTarget: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(originalRoot, prepareBeforeRows())
      prepared = prepareTarget(originalRoot, before, source)
      val prepareRecord = intent(Phase.PREPARE, before, prepared, source)
      val prepareProof = MySqlDbFirstBootstrapProof.validate(prepareRecord, before, prepared, source,
        sourceSha, Inputs, originalRoot, QuotaMaximumRecords)

      val certifyBefore = capture(originalRoot, withCatalog(prepared))
      assertEquals(prepared.relations, certifyBefore.relations,
        "the applied PREPARE target must be the exact next phase before image")
      certified = certify(originalRoot, certifyBefore, source)
      val certifyRecord = intent(Phase.CERTIFY, certifyBefore, certified, source, expectedMarker(source))
      val certifyProof = MySqlDbFirstBootstrapProof.validate(certifyRecord, certifyBefore, certified, source,
        sourceSha, Inputs, originalRoot, QuotaMaximumRecords)

      val actualPrepared = capture(actualRoot, prepared.relations, observerScope)
      val actualCertifyBefore = capture(actualRoot, certifyBefore.relations, observerScope)
      val actualCertified = capture(actualRoot, certified.relations, observerScope)
      assertEquals(Outcome.NOT_APPLIED, MySqlDbFirstBootstrapActor.classifyOriginal(certifyProof, actualCertifyBefore),
        "a new observer header with the complete before image classifies as NOT_APPLIED")
      assertEquals(Outcome.APPLIED, MySqlDbFirstBootstrapActor.classifyOriginal(certifyProof, actualCertified),
        "a new observer header with the complete target image classifies as APPLIED")

      val preparedSettlement = settled(prepareProof, actualPrepared, actualRoot, observerScope, Outcome.APPLIED)
      val certifySettlement = settled(certifyProof, actualCertified, actualRoot, observerScope, Outcome.APPLIED)
      val prepareHistory = MySqlDbFirstBootstrapActor.HistoryPhase(prepareProof, Some(preparedSettlement))
      val certifyHistory = MySqlDbFirstBootstrapActor.HistoryPhase(certifyProof, Some(certifySettlement))
      val appliedTail = MySqlDbFirstBootstrapActor.historyTail(Vector(prepareHistory, certifyHistory)).get
      assertEquals(certified.relations, appliedTail.relations,
        "a fully linked APPLIED chain ends at the exact Full27 target")

      val pendingTail = MySqlDbFirstBootstrapActor.historyTail(Vector(prepareHistory,
        MySqlDbFirstBootstrapActor.HistoryPhase(certifyProof, None))).get
      assertEquals(prepared.relations, pendingTail.relations,
        "one unresolved intent is permitted only at the applied history tail")
      val notAppliedSettlement = settled(certifyProof, actualCertifyBefore, actualRoot, observerScope, Outcome.NOT_APPLIED)
      val notAppliedTail = MySqlDbFirstBootstrapActor.historyTail(Vector(prepareHistory,
        MySqlDbFirstBootstrapActor.HistoryPhase(certifyProof, Some(notAppliedSettlement)))).get
      assertEquals(prepared.relations, notAppliedTail.relations,
        "a settled NOT_APPLIED tail leaves the applied history at its before image")

      val driftRows = replaceCell(certified.relations, BindingTable, "format_version", MySqlFormat3Cell.SignedInteger(1L))
      val full27Drift = capture(actualRoot, driftRows, observerScope)
      assertThrows(classOf[SQLDataException], () => {
        MySqlDbFirstBootstrapActor.classifyOriginal(certifyProof, full27Drift); () },
        "a non-W relation drift is UNKNOWN even when the new observer header is valid")

      assertThrows(classOf[IllegalArgumentException], () => { MySqlDbFirstBootstrapActor.historyTail(
        Vector(prepareHistory, prepareHistory)); () }, "duplicate applied edges form a fork")

      val alternateSeedRows = replaceCell(prepareBeforeRows(), WriterTable, "writer_epoch",
        MySqlFormat3Cell.SignedInteger(8L))
      val alternateBefore = capture(secondRoot, alternateSeedRows)
      secondTarget = prepareTarget(secondRoot, alternateBefore, source)
      val alternateRecord = intent(Phase.PREPARE, alternateBefore, secondTarget, source,
        connectionGeneration = id("60000000-0000-4000-8000-000000000002"))
      val alternateProof = MySqlDbFirstBootstrapProof.validate(alternateRecord, alternateBefore, secondTarget,
        source, sourceSha, Inputs, secondRoot, QuotaMaximumRecords)
      val alternateActual = capture(secondActualRoot, secondTarget.relations, observerScope)
      val alternateSettlement = settled(alternateProof, alternateActual, secondActualRoot, observerScope, Outcome.APPLIED)
      assertThrows(classOf[IllegalArgumentException], () => { MySqlDbFirstBootstrapActor.historyTail(Vector(
        prepareHistory, MySqlDbFirstBootstrapActor.HistoryPhase(alternateProof, Some(alternateSettlement)))); () },
        "a second PREPARE seed disconnected from the original root is rejected")

      val secondPendingRecord = intent(Phase.PREPARE, before, prepared, source,
        connectionGeneration = id("60000000-0000-4000-8000-000000000003"))
      val secondPendingProof = MySqlDbFirstBootstrapProof.validate(secondPendingRecord, before, prepared, source,
        sourceSha, Inputs, originalRoot, QuotaMaximumRecords)
      assertThrows(classOf[IllegalArgumentException], () => { MySqlDbFirstBootstrapActor.historyTail(Vector(
        MySqlDbFirstBootstrapActor.HistoryPhase(prepareProof, None),
        MySqlDbFirstBootstrapActor.HistoryPhase(secondPendingProof, None))); () },
        "more than one unresolved intent is rejected even when both name the same tail")
    finally
      close(secondTarget, secondRoot)
      close(certified, originalRoot)
      close(prepared, originalRoot)
      secondActualRoot.closeRootReadBudget()
      secondRoot.closeRootReadBudget()
      actualRoot.closeRootReadBudget()
      originalRoot.closeRootReadBudget()
