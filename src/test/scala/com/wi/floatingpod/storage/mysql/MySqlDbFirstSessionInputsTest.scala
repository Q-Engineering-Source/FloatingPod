package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.server.io.IoOperationTag
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.HexFormat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 显式 synthetic static inputs；实际文件/pin/renderer/security 装配，不签 SQL/native 资格。 */
class MySqlDbFirstSessionInputsTest:
  import DeploymentReceiptStaticTestData.*
  @TempDir var directory: Path = _
  private type Fixture = MySqlDbFirstHistoricalTestFixture.StaticFixture
  private def metadata(fields: Map[String,String]): MySqlRuntimeMetadata.DbFirstStatic =
    MySqlDbFirstHistoricalTestFixture.metadata(fields)
  private def fixture(): Fixture = MySqlDbFirstHistoricalTestFixture.fixture(directory,defaultProfile())

  private def identity = MySqlOwnedIoIdentity(java.util.UUID.fromString("12345678-1234-1234-1234-123456789abc"),7L,1L)

  @Test def firstBindingCapabilityIsUnavailableToOrdinarySchema6Semantics(): Unit =
    val f=fixture();val root=f.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    try
      assertFalse(inputs.managedFirstBindingAllowed)
      assertThrows(classOf[IllegalStateException],()=>inputs.firstBindingQuotaMaximumRecords)
    finally { inputs.close();owner.close();root.closeRootReadBudget() }

  @Test def sessionRejectsUnbundledFirstBindingControlBeforeChargingActorHolder():Unit =
    val f=fixture()
    val profile=f.profile.sourceLimitProfile
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    val session=MySqlDbFirstSession.allocate(inputs,root,io,policy.maxReceiptAge,ledger,new Thread(),8)
    val control=MySqlDbFirstHistoricalTestFixture.productionControl(f,io,
      java.util.UUID.fromString("22345678-1234-1234-1234-123456789abc"),
      java.util.UUID.fromString("32345678-1234-1234-1234-123456789abc"),graph.configurationSha256)
    val before=root.accountedMemoryBytes
    try
      assertThrows(classOf[IllegalArgumentException],()=>session.registerManagedFirstBinding(control))
      assertEquals(before,root.accountedMemoryBytes,"a control without the managed bundle must not reserve actor DATA")
      assertTrue(session.resourceObservation.managedFirstBindingActor.isEmpty)
      assertEquals(0L,session.resourceObservation.managedFirstBindingFee)
    finally
      try session.closeOnOwnerThread()
      finally
        try owner.close()
        finally root.closeRootReadBudget()

  @Test def serviceInputBindingRequiresActualRecoveryAndNeverAcceptsOnlyNewGenerationMetadata():Unit =
    val f=fixture();val root=f.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    val next=io.copy(connectionGeneration=2L)
    val before=root.accountedMemoryBytes
    try
      assertThrows(classOf[IllegalArgumentException],()=>inputs.bindServiceOwner(null,null,root,next))
      assertThrows(classOf[IllegalArgumentException],()=>inputs.detachClosedServiceOwner(null,null,root,next))
      assertThrows(classOf[IllegalArgumentException],()=>inputs.requireForSession(root,next))
      assertThrows(classOf[IllegalArgumentException],()=>inputs.requireForSession(root,io.copy()))
      assertNull(inputs.serviceOwnerForSnapshot(root,io))
      assertThrows(classOf[IllegalArgumentException],()=>inputs.serviceOwnerForSnapshot(root,next))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstSnapshotJdbc.create(inputs,root,next))
      inputs.requireForSession(root,io)
      assertEquals(before,root.accountedMemoryBytes,"unregistered service metadata cannot acquire input identity or charge a holder")
    finally { inputs.close();owner.close();root.closeRootReadBudget() }

  @Test def actualSchema6InputsBindPendingRootAndKeepFull27FeeAfterConsumersClose(): Unit =
    val f=fixture();val root=f.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val foreignRoot=f.root()
    val before=root.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,foreignRoot,io))
    assertEquals(before,root.accountedMemoryBytes)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    inputs.requireOpen();inputs.requireForSession(root,io)
    assertSame(io,inputs.ioIdentity)
    assertThrows(classOf[IllegalArgumentException],()=>inputs.requireForSession(root,io.copy()))
    assertThrows(classOf[IllegalArgumentException],()=>inputs.requireForSession(foreignRoot,io))
    assertEquals(MySqlSchemaAuditProfile.DbFirst1,inputs.auditExpectation.profile)
    assertEquals(graph.configurationSha256,inputs.configurationSha256)
    assertSame(root.profile,inputs.sourceLimitProfile)
    assertEquals(MySqlScriptSchemaDataLayout.DbFirstFull27,inputs.full27Inputs.dataLayout)
    assertEquals(f.profile.schema,inputs.full27Inputs.schema)
    assertEquals(MySqlDbFirstSchemaProfile.render(f.profile).sha256,inputs.full27Inputs.ddlSha256)
    assertTrue(inputs.historicalInputs.isEmpty) // NONE仅表示未提供来源。
    assertFalse(inputs.settings.utf8mb4ResultsRequired, "external schema6 keeps its approved result encoding")
    assertNotNull(inputs.settings) // fixture无秘密文件，证明装配不提前读取凭证。
    assertFalse(Files.exists(Path.of(f.fields("credentialSourceRef").substring(5))))
    assertFalse(Files.exists(Path.of(f.fields("trustSecretSourceRef").substring(5))))
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io))
    val independent=graph.openAuditInputs()
    val bound=inputs.full27Inputs
    val expectation=inputs.auditExpectation
    inputs.close()
    assertThrows(classOf[IllegalStateException],()=>inputs.requireOpen())
    assertThrows(classOf[IllegalStateException],()=>expectation.validateCapturedCallerGrants(Vector.empty))
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    independent.close();owner.close()
    root.requireScriptDataInputs(bound)
    assertTrue(root.accountedMemoryBytes > 0L,"actual Full27/budgets remain retained by original root")
    root.closeRootReadBudget();foreignRoot.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)

  @Test def wrongWorkerCannotUseOrCloseActualInputsAndDoesNotChangeOwnership(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    val retained=root.accountedMemoryBytes
    val failures=new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()
    val thread=new Thread(()=>
      try inputs.requireOpen()
      catch case cause: Throwable=>failures.add(cause)
      try inputs.close()
      catch case cause: Throwable=>failures.add(cause)
      ())
    thread.start();thread.join(5000L)
    assertFalse(thread.isAlive);assertEquals(2,failures.size())
    failures.forEach(cause=>assertInstanceOf(classOf[IllegalStateException],cause))
    assertEquals(retained,root.accountedMemoryBytes)
    assertTrue(inputs.cleanupObservation.failure.isEmpty)
    inputs.requireOpen();inputs.close();owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)

  @Test def failedRegisteredAttemptPreservesCauseAndRefundsOnlyActualUnpublishedResources(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val before=root.accountedMemoryBytes
    val pressure=root.availableMemoryBytes-(256L+95L)
    root.reserveRetainedMemory(pressure,"synthetic real ledger pressure")
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity))
    val observed=owner.dbFirstInputObservation.get
    assertSame(failure,owner.failure.get);assertSame(failure,observed.failure.get)
    assertEquals("session-lease",observed.stage)
    assertFalse(observed.published);assertFalse(observed.sessionLeasePending);assertFalse(observed.auditPending)
    assertEquals(0L,observed.temporaryFee);assertEquals(0L,observed.inputFee);assertEquals(0L,observed.bindingFee)
    assertEquals(before+pressure+256L,root.accountedMemoryBytes,"failed attempt holder remains owned until owner retirement")
    assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity))
    root.releaseRetainedMemory(pressure,"synthetic pressure removed")
    owner.close();root.closeRootReadBudget();assertEquals(0L,root.accountedMemoryBytes)

  @Test def rejectedOriginalRootBindingRetainsItsCauseAndSettlesConstructedConsumerSlots(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val original=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try root.bindDbFirstAssembly(owner,graph,null,null)
      catch case cause: Throwable=>original.set(cause))
    thread.start();thread.join(5000L);assertFalse(thread.isAlive);assertNotNull(original.get())
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity))
    assertSame(original.get(),failure);assertSame(failure,owner.failure.get)
    val observed=owner.dbFirstInputObservation.get
    assertEquals("root-bind",observed.stage);assertSame(failure,observed.failure.get)
    assertFalse(observed.inputsPending);assertFalse(observed.sessionLeasePending)
    assertFalse(observed.auditPending);assertFalse(observed.auditLeasePending)
    assertEquals(0L,observed.inputFee);assertEquals(0L,observed.bindingFee);assertEquals(0L,observed.temporaryFee)
    owner.close();root.closeRootReadBudget();assertEquals(0L,root.accountedMemoryBytes)

  @Test def readerBorrowPinsInputsUntilOriginalWorkerClosesEveryTokenExactlyOnce(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    val baseline=root.accountedMemoryBytes
    val before=inputs.cleanupObservation
    val first=inputs.borrowForReader()
    val second=inputs.borrowForReader()
    assertEquals(baseline+128L,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>inputs.close())
    assertEquals(before,inputs.cleanupObservation)
    inputs.requireOpen()
    val observed=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try first.close()
      catch case cause: Throwable=>observed.set(cause))
    thread.start();thread.join(5000L)
    assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalStateException],observed.get())
    assertEquals(baseline+128L,root.accountedMemoryBytes)
    assertEquals(before,inputs.cleanupObservation)
    first.close();first.close()
    assertEquals(baseline+64L,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>inputs.close())
    inputs.requireOpen();assertEquals(before,inputs.cleanupObservation)
    second.close();second.close()
    assertEquals(baseline,root.accountedMemoryBytes)
    inputs.close()
    first.close();second.close() // 原worker上的退休token仍幂等，不能重复退费。
    owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)

  @Test def full27ManifestUsesActualPinnedSecurityAndRefundsComparisonWorkspace(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    val expected=inputs.auditExpectation
    val p=inputs.sourceLimitProfile
    def binary(bytes: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
    def hex(value: String): MySqlFormat3Cell = binary(HexFormat.of().parseHex(value))
    def uuid(value: java.util.UUID): MySqlFormat3Cell = binary(java.nio.ByteBuffer.allocate(16)
      .putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array())
    def number(value: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(value)
    def text(value: String): MySqlFormat3Cell = MySqlFormat3Cell.Text(value)
    val values=Map(
      "singleton_fixed1"->number(1),"backend"->text("MYSQL"),"format_version"->number(6),
      "ddl_sha256"->hex(expected.ddlSha256),"provisioning_uuid"->uuid(expected.provisioningUuid),
      "bootstrap_state"->text("READY"),"expected_server_uuid"->uuid(expected.serverUuid),
      "expected_binding_uuid"->uuid(expected.bindingUuid),"import_state"->text("NONE"),
      "configuration_sha256"->hex(graph.configurationSha256),
      "source_provenance_kind"->text(f.contract.get("sourceProvenanceKind").getAsString),
      "source_provenance_bytes"->hex(f.contract.get("sourceProvenanceHex").getAsString),
      "source_provenance_sha256"->hex(f.contract.get("sourceProvenanceSha256").getAsString),
      "limit_profile_encoding"->text(com.wi.floatingpod.config.SourceLimitProfile.ENCODING),
      "limit_profile_bytes"->binary(p.canonicalBytes()),"limit_profile_sha256"->binary(p.sha256()),
      "max_region_bytes"->number(p.maxRegionBytes()),"max_chunk_bytes"->number(p.maxChunkBytes()),
      "max_decompressed_bytes"->number(p.maxDecompressedBytes()),"max_nbt_depth"->number(p.maxNbtDepth()),
      "max_members"->number(p.maxMembers()),"max_retained_bytes"->number(p.maxRetainedBytes()),
      "max_sql_packet_bytes"->number(p.maxSqlPacketBytes()),"max_native_artifact_bytes"->number(p.maxNativeArtifactBytes()))
    val connection=MySqlScriptSchemaDataTestFixture.Connection.copy(catalog = expected.schema)
    val scope=MySqlScriptSchemaDataTestFixture.Scope
    def check(overrides: Map[String,MySqlFormat3Cell], accept: Boolean): Unit =
      val child=root.newCanonicalDataScope()
      try
        val fields=values++overrides
        val relations=inputs.full27Inputs.canonicalOrder.map { shape =>
          val rows=if shape.table=="fp_schema_manifest" then
            Vector(shape.columns.map(c=>fields.getOrElse(c.name,MySqlFormat3Cell.SqlNull)))
          else if shape.table==MySqlDeploymentReceiptSchemaDefinition.QuotaTable then
            Vector(Vector(binary(Array[Byte](70,80,68,67)),number(4096),number(0),number(0)))
          else Vector.empty
          MySqlFormat3RelationSnapshot(shape.table,rows)
        }
        val snapshot=MySqlScriptSchemaSnapshot.captureMaterialized(expected.schema,connection,scope,
          inputs.full27Inputs.budget.packetBytes,inputs.full27Inputs,relations,child)
        val retained=root.accountedMemoryBytes
        if accept then MySqlDbFirstSnapshotSql.requireManifest(snapshot,expected)
        else assertThrows(classOf[Exception],()=>MySqlDbFirstSnapshotSql.requireManifest(snapshot,expected))
        assertEquals(retained,root.accountedMemoryBytes,"manifest comparison scratch must be refunded on both success and rejection")
      finally child.closeScopedReadBudget()
    try
      check(Map.empty,true)
      Vector(
        "format_version"->number(5),"bootstrap_state"->text("PREPARING"),
        "configuration_sha256"->binary(new Array[Byte](32)),"ddl_sha256"->binary(new Array[Byte](32)),
        "source_provenance_bytes"->binary(Array[Byte](1)),"max_members"->number(p.maxMembers()+1)
      ).foreach { (key,value)=>check(Map(key->value),false) }
    finally
      inputs.close();owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)

  @Test def jdbcHolderExistsBeforeSqlAndRetainsRejectedAttemptUntilExplicitClose(): Unit =
    val f=fixture();val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    val baseline=root.accountedMemoryBytes
    val pressure=root.availableMemoryBytes-64L
    root.reserveRetainedMemory(pressure,"synthetic holder construction pressure")
    assertThrows(classOf[Exception],()=>MySqlDbFirstSnapshotJdbc.create(inputs,root,io))
    assertEquals(baseline+pressure,root.accountedMemoryBytes,"failed holder construction must return its input borrow")
    root.releaseRetainedMemory(pressure,"synthetic pressure detached")
    val holder=MySqlDbFirstSnapshotJdbc.create(inputs,root,io)
    assertThrows(classOf[IllegalStateException],()=>holder.observedRecallLease(java.util.UUID.randomUUID(),
      java.util.UUID.randomUUID(),1L,java.time.LocalDateTime.of(2026,10,29,13,0)))
    assertFalse(holder.cleanupObservation.writerTimePending)
    assertEquals(0L,holder.cleanupObservation.writerTimeFee)
    assertThrows(classOf[IllegalArgumentException],()=>holder.detachCompletedReadAuthority())
    assertThrows(classOf[IllegalStateException],()=>inputs.close())
    assertThrows(classOf[IllegalStateException],()=>holder.snapshot)
    val before=holder.cleanupObservation
    val foreign=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try holder.close()
      catch case cause:Throwable=>foreign.set(cause))
    thread.start();thread.join(5000L)
    assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalStateException],foreign.get())
    assertEquals(before,holder.cleanupObservation)
    val rollback=new MySqlRollbackOnlyState()
    val failure=assertThrows(classOf[IllegalArgumentException],()=>holder.readLocked(null,null,null,null,rollback))
    assertSame(failure,holder.firstFailure.get)
    assertThrows(classOf[IllegalArgumentException],()=>holder.detachCompletedReadAuthority())
    assertTrue(rollback.cause.isEmpty,"unverified foreign rollback state must not be marked")
    assertFalse(holder.cleanupObservation.imagePending)
    assertTrue(holder.cleanupObservation.inputBorrowPending)
    assertThrows(classOf[IllegalStateException],()=>holder.readLocked(null,null,null,null,rollback))
    assertSame(failure,holder.firstFailure.get)
    holder.close();holder.close()
    assertTrue(holder.cleanupObservation.closed)
    assertEquals(baseline,root.accountedMemoryBytes)
    inputs.close();owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)

  @Test def sessionAllocationHasNoJdbcEffectsAndOnlyOriginalWorkerCanRetireInputs(): Unit =
    val f=fixture()
    val ledger=new MySqlFormat3AllocationLedger(f.profile.sourceLimitProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(f.profile.sourceLimitProfile,
      f.profile.sourceLimitProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    val mainStarted=new java.util.concurrent.CountDownLatch(1)
    val mainStop=new java.util.concurrent.CountDownLatch(1)
    val actualMain=new Thread(()=>
      mainStarted.countDown()
      mainStop.await()
      (),"schema6-session-test-main")
    actualMain.start()
    assertTrue(mainStarted.await(3L,java.util.concurrent.TimeUnit.SECONDS))
    try
      val baseline=root.accountedMemoryBytes
      val foreignLedger=new MySqlFormat3AllocationLedger(f.profile.sourceLimitProfile.maxRetainedBytes())
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstSession.allocate(inputs,root,io,policy.maxReceiptAge,
        foreignLedger,actualMain,2))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstSession.allocate(inputs,root,io,policy.maxReceiptAge,
        ledger,Thread.currentThread(),2))
      assertEquals(baseline,root.accountedMemoryBytes)
      val session=MySqlDbFirstSession.allocate(inputs,root,io,policy.maxReceiptAge,ledger,actualMain,2)
      val before=session.resourceObservation
      assertEquals(MySqlDbFirstSession.State.Allocated,before.state)
      assertFalse(before.closed)
      assertTrue(before.retainedOwnerPending)
      assertTrue(session.requireRetainedOwner().plans.isEmpty)
      assertTrue(session.firstFailure.isEmpty)
      val retainedBeforeWriter=root.accountedMemoryBytes
      assertEquals(io.connectionGeneration,before.scopeConnectionGeneration)
      assertThrows(classOf[IllegalArgumentException],()=>
        session.requireDbFirstObservationReaderSource(inputs,root,io))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstSnapshotJdbc.createForObservation(session,inputs,root,io))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstSnapshotJdbc.createForObservation(null,inputs,root,io))
      assertThrows(classOf[IllegalArgumentException],()=>
        session.requireInspectionImageIssuer(null,root,inputs,io,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>
        root.issueDbFirstInspectionSnapshotImage(inputs.full27Inputs,inputs.full27Inputs.schema,null,null,1L,null))
      assertEquals(retainedBeforeWriter,root.accountedMemoryBytes,
        "unregistered inspection cannot allocate an image or charge the original ledger")
      assertEquals(before,session.resourceObservation,
        "unregistered inspection cannot alter original Session/ordinary issuer state")
      assertThrows(classOf[IllegalArgumentException],()=>session.observeFailedDbFirstRecall(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.observeFailedDbFirstRecallAfterBarrierHandoff(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.observeFailedRecallCommitOnce(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.qualifyFailedRecallCleanup(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.retainedRecallQualification(null,new Exception("foreign")))
      assertThrows(classOf[IllegalArgumentException],()=>session.maintainRetainedRecall(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.maintainRetiredRecallHold(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.observeRetiredRecallOnce(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.retireRetainedRecallWriterOnce(null))
      assertFalse(session.resourceObservation.retiredRecallWriterAttempted)
      assertTrue(session.resourceObservation.retiredRecallWriter.isEmpty)
      assertEquals(0L,session.resourceObservation.retiredRecallWriterAllocationFee)

      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstSnapshotJdbc.createForRetiredObservation(null,inputs,root,io))
      assertFalse(session.resourceObservation.retiredRecallObservationAttempted)
      assertTrue(session.resourceObservation.retiredRecallObservation.isEmpty)
      assertEquals(0L,session.resourceObservation.retiredRecallObservationAllocationFee)

      assertThrows(classOf[IllegalArgumentException],()=>session.requireFailedRecallCleanupQualification(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>session.retainedIssuerForOwnershipPublication()
        .recordFailedDbFirstRecallQualification(session,null,null,inputs.full27Inputs))
      assertThrows(classOf[IllegalArgumentException],()=>session.requireFailedRecallGateRetirement(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.prepareFailedRecallMainRead(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.observeFailedRecallLease(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.auditFailedRecallObservation(null))
      assertTrue(session.resourceObservation.recallObservedLease.isEmpty)
      assertFalse(session.resourceObservation.recallLeaseObservationAttempted)
      assertFalse(session.resourceObservation.recallObservationAuditAttempted)
      assertFalse(session.resourceObservation.recallObservationAuditCompleted)
      assertThrows(classOf[IllegalArgumentException],()=>session.retireFailedRecallSql(null,null))
      val counterfeitReadback=MySqlDbFirstSession.FailedRecallReadback(
        IoOperationTag(io.bootId,io.bootGeneration,io.connectionGeneration,java.util.UUID.randomUUID()),
        java.util.UUID.randomUUID(),"0"*64,0L)
      assertThrows(classOf[IllegalArgumentException],()=>session.retireFailedRecallSql(null,counterfeitReadback))
      assertThrows(classOf[IllegalArgumentException],()=>session.requireFailedRecallSqlSettlement(null,counterfeitReadback))
      assertThrows(classOf[IllegalArgumentException],()=>session.retainedIssuerForOwnershipPublication()
        .settleFailedDbFirstRecallReadback(session,null,counterfeitReadback,inputs.full27Inputs))
      assertThrows(classOf[IllegalArgumentException],()=>session.requirePreparedFailedRecallReadback(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>session.detachPreparedFailedRecallDraft(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>session.failedRecallSnapshotOnOwnerThread(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.compareFailedRecallImageOnOwnerThread(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.verifyFailedRecallSnapshotHistoryOnOwnerThread(null))
      assertThrows(classOf[IllegalArgumentException],()=>session.closeFailedRecallObservationOnOwnerThread(null))
      assertFalse(session.resourceObservation.recallCleanupQualificationAttempted)
      assertFalse(session.resourceObservation.recallCleanupQualificationPending)
      assertEquals(0L,session.resourceObservation.recallCleanupQualificationFee)
      assertFalse(session.resourceObservation.recallInspectionAttempted)
      assertTrue(session.resourceObservation.recallInspection.isEmpty)
      assertTrue(session.resourceObservation.recallImageObservation.isEmpty)
      assertFalse(session.resourceObservation.recallFailedSqlGateRetired)
      assertTrue(session.resourceObservation.recallPreparedReadback.isEmpty)
      assertFalse(session.resourceObservation.recallFailedReadersRetired)
      assertFalse(session.resourceObservation.recallSqlRetirementAttempted)
      assertFalse(session.resourceObservation.recallFailedSqlResourcesRetired)
      assertFalse(session.resourceObservation.recallSqlResponsibilityReleased)
      assertEquals(0L,session.resourceObservation.recallInspectionAllocationFee)
      assertThrows(classOf[IllegalStateException],()=>session.currentSqlWriterFreshness())
      assertThrows(classOf[IllegalStateException],()=>session.prepareMainRead(null,null,null,0L))
      assertNotNull(session.retainedIssuerForOwnershipPublication())
      assertThrows(classOf[IllegalStateException],()=>session.renewSqlWriter())
      assertThrows(classOf[IllegalStateException],()=>session.renewSqlWriter())
      assertThrows(classOf[IllegalStateException],()=>session.acquireSqlWriter(java.time.Duration.ofSeconds(1),java.time.Duration.ZERO))
      assertEquals(retainedBeforeWriter,root.accountedMemoryBytes)
      assertEquals(before,session.resourceObservation)
      assertFalse(session.resourceObservation.barrierAttempted)
      assertFalse(session.resourceObservation.barrierHeld)
      assertFalse(session.resourceObservation.mayRetainServerLock)
      assertTrue(session.resourceObservation.lastBarrierObservation.isEmpty)
      val writerForeign=new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val writerThread=new Thread(()=>
        try
          session.acquireSqlWriter(java.time.Duration.ofSeconds(1),java.time.Duration.ZERO)
          ()
        catch case cause:Throwable=>writerForeign.set(cause))
      writerThread.start();writerThread.join(5000L)
      assertFalse(writerThread.isAlive)
      assertInstanceOf(classOf[IllegalStateException],writerForeign.get())
      assertEquals(before,session.resourceObservation)
      assertEquals(retainedBeforeWriter,root.accountedMemoryBytes)
      val renewForeign=new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val renewThread=new Thread(()=>
        try
          session.renewSqlWriter()
          ()
        catch case cause:Throwable=>renewForeign.set(cause))
      renewThread.start();renewThread.join(5000L)
      assertFalse(renewThread.isAlive)
      assertInstanceOf(classOf[IllegalStateException],renewForeign.get())
      assertEquals(before,session.resourceObservation)
      assertEquals(retainedBeforeWriter,root.accountedMemoryBytes)
      assertThrows(classOf[IllegalStateException],()=>session.observationOnOwnerThread)
      assertThrows(classOf[IllegalStateException],()=>inputs.close())
      val foreign=new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val thread=new Thread(()=>
        try session.closeOnOwnerThread()
        catch case cause:Throwable=>foreign.set(cause))
      thread.start();thread.join(5000L)
      assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalStateException],foreign.get())
      assertEquals(before,session.resourceObservation)
      val command=MySqlOwnedIoCommand.ObserveDbFirst(f.metadata,f.profile.sourceLimitProfile,policy)
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(command),"observation must not issue a writer transaction or Ready")
      session.closeOnOwnerThread();session.closeOnOwnerThread()
      assertTrue(session.resourceObservation.closed)
      assertThrows(classOf[IllegalStateException],()=>session.currentSqlWriterFreshness())
      assertThrows(classOf[IllegalStateException],()=>session.retainedIssuerForOwnershipPublication())
      assertThrows(classOf[IllegalStateException],()=>session.renewSqlWriter())
      assertFalse(session.resourceObservation.retainedOwnerPending)
      assertThrows(classOf[IllegalStateException],()=>inputs.requireOpen())
      owner.close();root.closeRootReadBudget()
      assertEquals(0L,root.accountedMemoryBytes)
    finally
      mainStop.countDown()
      actualMain.join(4000L)
      assertFalse(actualMain.isAlive,"actual Main fixture did not retire")

  @Test def originalUnacquiredIssuerFollowsMainTransferAndRetiresOnOriginalWorker(): Unit =
    val f=fixture()
    val ledger=new MySqlFormat3AllocationLedger(f.profile.sourceLimitProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(f.profile.sourceLimitProfile,
      f.profile.sourceLimitProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val io=identity
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,io)
    val oldMain=java.util.concurrent.Executors.newSingleThreadExecutor()
    val newMain=java.util.concurrent.Executors.newSingleThreadExecutor()
    def on[A](executor:java.util.concurrent.ExecutorService)(action: => A):A =
      executor.submit(new java.util.concurrent.Callable[A] { def call():A=action })
        .get(5L,java.util.concurrent.TimeUnit.SECONDS)
    try
      val originalMain=on(oldMain)(Thread.currentThread())
      val session=MySqlDbFirstSession.allocate(inputs,root,io,policy.maxReceiptAge,ledger,originalMain,2)
      val issuer=session.retainedIssuerForOwnershipPublication()
      val retained=root.accountedMemoryBytes
      assertSame(issuer,session.retainedIssuerForOwnershipPublication())
      assertThrows(classOf[IllegalStateException],()=>session.currentSqlWriterFreshness())
      on(oldMain)(issuer.suspendMain())
      on(newMain)(issuer.claimMain(Thread.currentThread()))
      on(oldMain) {
        assertThrows(classOf[IllegalArgumentException],()=>issuer.suspendMain())
        ()
      }
      on(newMain)(issuer.suspendMain())
      on(oldMain)(issuer.claimMain(Thread.currentThread()))
      assertEquals(retained,root.accountedMemoryBytes)
      assertSame(issuer,session.retainedIssuerForOwnershipPublication())
      assertThrows(classOf[IllegalStateException],()=>session.currentSqlWriterFreshness())
      session.closeOnOwnerThread()
      assertTrue(session.resourceObservation.closed)
      owner.close();root.closeRootReadBudget()
      assertEquals(0L,ledger.accountedBytes)
    finally
      oldMain.shutdownNow();newMain.shutdownNow()
      assertTrue(oldMain.awaitTermination(5L,java.util.concurrent.TimeUnit.SECONDS))
      assertTrue(newMain.awaitTermination(5L,java.util.concurrent.TimeUnit.SECONDS))

  private def pinHistory(f: Fixture, old: DeploymentReceiptStaticTestData.Fixture): MySqlRuntimeMetadata.DbFirstStatic =
    MySqlDbFirstHistoricalTestFixture.pinHistory(f,old)

  @Test def explicitHistoryIsConsumedByOriginalSessionAndBorrowPinsOriginalOwner(): Unit =
    val f=fixture();val old=DeploymentReceiptStaticTestData.fixture(directory.resolve("history"))
    val noneRoot=f.root();val noneOwner=MySqlRuntimeInputs.beginV2(noneRoot,Thread.currentThread())
    val noneGraph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,noneOwner,policy)
    val noneConfiguration=noneGraph.configurationSha256
    noneOwner.close();noneRoot.closeRootReadBudget()
    val pinned=pinHistory(f,old);val root=f.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy)
    assertNotEquals(noneConfiguration,graph.configurationSha256)
    val session=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    val history=session.historicalInputs.get;val actual=history.inputs
    assertEquals(MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,actual.dataLayout)
    assertSame(root.profile,actual.sourceLimitProfile)
    assertEquals(MySqlDeploymentReceiptSchemaProfile.render(old.profile).sha256,actual.ddlSha256)
    assertNotEquals(session.full27Inputs.ddlSha256,actual.ddlSha256)
    val before=root.accountedMemoryBytes;val lease=history.borrow()
    lease.requireInputs(root,actual)
    assertThrows(classOf[IllegalStateException],()=>history.closeOwned())
    val foreign=f.root()
    assertThrows(classOf[IllegalStateException],()=>lease.requireInputs(foreign,actual))
    foreign.closeRootReadBudget()
    val caught=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val worker=new Thread(()=>
      try lease.close() catch case cause: Throwable => caught.set(cause))
    worker.start();worker.join(5000L)
    assertFalse(worker.isAlive);assertInstanceOf(classOf[IllegalStateException],caught.get())
    session.close()
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    lease.requireInputs(root,actual);lease.close()
    assertTrue(root.accountedMemoryBytes < before)
    owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>history.inputs)

  @Test def everyHistoricalFilePinFailurePreservesCauseAndRefundsPartialOwner(): Unit =
    Vector("metadata","baseline","contract").foreach { target =>
      val f=fixture();val old=DeploymentReceiptStaticTestData.fixture(directory.resolve("history_"+target))
      val pinned=pinHistory(f,old)
      val path=target match
        case "metadata" => old.directory.resolve("historical.properties")
        case "baseline" => old.directory.resolve("baseline.json")
        case "contract" => old.directory.resolve("contract.json")
      Files.writeString(path,Files.readString(path)+" ")
      val root=f.root();val before=root.accountedMemoryBytes
      val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val cause=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy))
      assertSame(cause,owner.failure.get)
      owner.close();assertEquals(before,root.accountedMemoryBytes)
      root.closeRootReadBudget()
    }

  @Test def historicalForeignIdentityAndProfileCannotBeAdopted(): Unit =
    Vector("identity","profile").foreach { target =>
      val f=fixture()
      val oldProfile=if target == "identity" then
        MySqlDeploymentReceiptSchemaProfile.inputs("foreign_history",f.profile.accounts,f.profile.sourceLimitProfile,
          f.profile.publicationBudget,f.profile.deploymentBudget,f.profile.quotaMaximumRecords,f.profile.trustedAdministrativeProxyRows)
      else
        val limits=SourceLimitProfile.of(999999L,65536L,2000000L,64L,100000L,134217728L,1000000L,2000000L)
        MySqlDeploymentReceiptSchemaProfile.inputs(f.profile.schema,f.profile.accounts,limits,
          f.profile.publicationBudget,f.profile.deploymentBudget,f.profile.quotaMaximumRecords,f.profile.trustedAdministrativeProxyRows)
      val old=DeploymentReceiptStaticTestData.fixture(directory.resolve("foreign_"+target),oldProfile)
      val pinned=pinHistory(f,old);val root=f.root()
      val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val cause=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy))
      assertSame(cause,owner.failure.get)
      owner.close();root.closeRootReadBudget();assertEquals(0L,root.accountedMemoryBytes)
    }

  @Test def schema6PublicAuditWrappersRejectBeforeConnectionOrAllocation(): Unit =
    val f=fixture();val root=f.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,identity)
    try
      val auditor=new MySqlSchemaAuditor(inputs.auditExpectation,policy.maxReceiptAge)
      val before=root.accountedMemoryBytes
      // null连接/观察值刻意证明owner早门优先；不伪造JDBC或audit成功资格。
      val publicCalls=Vector[() => Unit](
        () => { auditor.readCurrentObjectManifest(null,null); () },
        () => { auditor.auditBeforeWriterAcquire(null,null); () },
        () => { auditor.auditAfterWriterAcquire(null,null,null,null); () },
        () => { auditor.readCallerScopeBeforeCall(null,null); () },
        () => { auditor.readCallerScopeAfterCall(null,null); () },
        () => { auditor.compareRequiredObjectManifest(null); () })
      publicCalls.foreach { call =>
        val cause=assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>call())
        assertEquals("schema6 audit requires its registered owned allocation holder",cause.getMessage)
        assertEquals(before,root.accountedMemoryBytes)
        assertTrue(owner.failure.isEmpty)
        inputs.requireOpen()
      }
    finally
      inputs.close();owner.close();root.closeRootReadBudget()
    assertEquals(0L,root.accountedMemoryBytes)
