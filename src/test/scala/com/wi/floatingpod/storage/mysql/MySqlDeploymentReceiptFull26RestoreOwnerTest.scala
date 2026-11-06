package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, B1ResumeRecordV6, WindowsB1Full26WitnessStore, WindowsB1SourceAnchorStore, WindowsB1V6WitnessRecordStore, WindowsB1V6WitnessRecordStoreTest, WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}

/** 完整26 DATA比较与显式child的native intent fixture；不构造actor或SQL资格。 */
final class MySqlDeploymentReceiptFull26RestoreOwnerTest:
  import MySqlScriptSchemaDataTestFixture.*
  import MySqlDeploymentReceiptFull26RestoreOwner.Position

  /** 仅由真实隔离native child调用；静态测试配置不冒充SQL audit/commit。 */
  def exerciseFreshNativeStoreInventory(directory:java.nio.file.Path):Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val metadataPath=directory.resolve("runtime.properties").toAbsolutePath
    val properties=new java.util.Properties()
    f.properties.foreach((k,v)=>properties.setProperty(k,v))
    val output=new java.io.ByteArrayOutputStream()
    properties.store(output,"schema5 native fixture static metadata only")
    java.nio.file.Files.write(metadataPath,output.toByteArray)
    def operation(n:Int)=java.util.UUID.fromString(f"00000000-0000-0000-0000-$n%012x")
    val plan=new com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData(
      operation(17),operation(18),operation(19),operation(20),DeploymentReceiptStaticTestData.binding,2,
      DeploymentReceiptStaticTestData.server,f.profile.schema,5,f.metadata.baseline().ddlSha256(),
      DeploymentReceiptStaticTestData.provisioning)
    MySqlDeploymentReceiptRuntimeContract.decodeRecipe(
      DeploymentReceiptStaticTestData.recipe(f.profile,f.metadata.baseline().ddlSha256())).requirePlanData(plan)
    val allocation=f.root()
    val boot=new com.wi.floatingpod.server.WindowsWorldProvisioner.BootIdentity(
      java.util.UUID.fromString("b14d7eaa-436b-48b4-a022-a644cc67d424"),1L)
    val native=com.wi.floatingpod.server.WindowsB1V6WitnessRecordStoreTest.NativeSourceFixture.create(
      metadataPath,f.metadata,directory.resolve("world").toAbsolutePath,plan,boot,allocation)
    native.source.validateLive()
    assertEquals(plan,native.world.creationProof().plan())
    assertEquals(boot,native.world.creationProof().creationBoot())
    val inventory=native.records.discoverCurrent()
    assertEquals(com.wi.floatingpod.server.WindowsB1V6WitnessRecordStore.CurrentInventory.Kind.PRE_GENESIS,inventory.kind())
    assertThrows(classOf[java.io.IOException],()=>inventory.current())
    assertNull(native.records.retainedFailure())
    // 未获terminal许可，不能普通close或退root；实际child退出结束此source的真实custody。
    // NativeSourceFixture原静态retained槽跨helper返回保留完整/partial对象及原root。
  private final class NativeIntentCase:
    var fixture:DeploymentReceiptStaticTestData.Fixture=null
    var inputs:MySqlScriptSchemaInputs=null
    var root:MySqlFormat3ReadBudget=null
    var native:WindowsB1V6WitnessRecordStoreTest.NativeSourceFixture=null
    var before:MySqlScriptSchemaSnapshot=null
    var target:MySqlScriptSchemaSnapshot=null
    var beforeGraph:MySqlB1CommonGraph=null
    var targetGraph:MySqlB1CommonGraph=null
    var body:MySqlFull26WitnessCodec.Owner=null
    var output:java.io.ByteArrayOutputStream=null
    var bytes:Array[Byte]=null
    var record:B1ResumeRecord=null
    var container:B1ResumeRecordV6=null
    var artifact:WindowsB1Full26WitnessStore.DurableFull26Ack=null
    var ack:WindowsB1V6WitnessRecordStore.DurableV6Ack=null
    var outputFee=0L
    var foreignRecords:WindowsB1V6WitnessRecordStore=null
    var pressureFee=0L
    var supplierCalls=0
    var expectedReservationFailure:java.io.IOException=null
    var expectedCapacityFailure:java.sql.SQLDataException=null
    var confirmationWorkspaceFee=0L
    var confirmationInventory:WindowsB1V6WitnessRecordStore.CurrentInventory=null
    var confirmationEvidence:Array[Byte]=null
    var confirmationPredecessorBytes:Array[Byte]=null
    var confirmationInnerHash:Array[Byte]=null
    var confirmationOuterHash:Array[Byte]=null
    var confirmationSettlement:B1ResumeRecord.SettlementAssertions=null
    var confirmationState:B1ResumeRecord.FullStateAssertions=null
    var confirmationTransition:B1ResumeRecord=null
    var confirmationCandidate:B1ResumeRecordV6=null
    var confirmationBytes:Array[Byte]=null
    var confirmationSupplier:java.util.function.Supplier[B1ResumeRecordV6]=null
    var confirmationToken:WindowsB1V6WitnessRecordStore.ConfirmationReservation=null
    var confirmationExpectedE=0L
    var confirmationExpectedP=0L
    var confirmationBefore=0L
    var confirmationWrongIdentity:java.io.IOException=null
    var confirmationWrongPredecessor:java.io.IOException=null
    var confirmationDuplicate:java.io.IOException=null
    var failure:Throwable=null
  private var nativeIntent:NativeIntentCase=null

  /** 真native intent发布；所有SQL图/connection字段仍是DATA，不建立CONFIRMED settlement。 */
  def exerciseFreshNativeIntent(directory:java.nio.file.Path):Unit =
    if nativeIntent!=null then throw new IllegalStateException("one native intent case per child",nativeIntent.failure)
    nativeIntent=new NativeIntentCase
    val c=nativeIntent
    try
      c.fixture=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
      val f=c.fixture
      c.inputs=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(f.profile.schema,
        f.metadata.baseline().ddlSha256(),f.profile.sourceLimitProfile,
        f.profile.publicationBudget.copy(),f.profile.deploymentBudget.copy())
      val i=c.inputs
      c.root=i.newAllocationRoot(i.budget.packetBytes,new MySqlFormat3AllocationLedger(i.sourceLimitProfile.maxRetainedBytes()))
      val r=c.root
      val metadataPath=directory.resolve("runtime.properties").toAbsolutePath
      val properties=new java.util.Properties()
      f.properties.foreach((k,v)=>properties.setProperty(k,v))
      val metadataOutput=new java.io.ByteArrayOutputStream()
      properties.store(metadataOutput,"schema5 native intent fixture static metadata only")
      java.nio.file.Files.write(metadataPath,metadataOutput.toByteArray)
      def operation(n:Int)=java.util.UUID.fromString(f"00000000-0000-0000-0000-$n%012x")
      val plan=new WindowsWorldProvisionReceiptDataCodec.PlanData(operation(17),operation(18),operation(19),operation(20),
        DeploymentReceiptStaticTestData.binding,2,DeploymentReceiptStaticTestData.server,i.schema,5,i.ddlSha256,
        DeploymentReceiptStaticTestData.provisioning)
      val provenance=DeploymentReceiptStaticTestData.recipe(f.profile,i.ddlSha256)
      MySqlDeploymentReceiptRuntimeContract.decodeRecipe(provenance).requirePlanData(plan)
      val boot=new WindowsWorldProvisioner.BootIdentity(java.util.UUID.fromString("b14d7eaa-436b-48b4-a022-a644cc67d424"),1L)
      c.native=WindowsB1V6WitnessRecordStoreTest.NativeSourceFixture.create(metadataPath,f.metadata,
        directory.resolve("world").toAbsolutePath,plan,boot,r)
      val source=c.native.source
      assertEquals(WindowsB1V6WitnessRecordStore.CurrentInventory.Kind.PRE_GENESIS,c.native.records.discoverCurrent().kind())
      val configuration=java.util.HexFormat.of().parseHex(
        WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(f.metadata).configurationSha256())
      // 明确固定微秒DATA时间；不声称这是实际databaseNow或已执行SQL。
      val now=java.time.LocalDateTime.of(2026,10,23,12,0)
      val seed=MySqlBusinessWriterRow(plan.bindingUuid(),0L,None,now,None,None,now)
      val targetWriter=MySqlFreshFormat3WriterTarget(plan.bindingUuid(),1L,Some(boot.bootId()),now.plusMinutes(1),plan.prepareWriteUuid())
      def change(table:String,row:Vector[MySqlFormat3Cell],name:String,value:MySqlFormat3Cell)=
        row.updated(i.forTable(table).columns.indexWhere(_.name==name),value)
      var rows=full26GenesisRelations(i,catalogPresent=false).map { relation=>
        if Set("physical_recovery_binding","binding_bootstrap_intent").contains(relation.table) then relation.copy(rows=Vector.empty)
        else relation
      }
      def update(table:String,name:String,value:MySqlFormat3Cell):Unit =
        rows=rows.map(x=>if x.table==table then x.copy(rows=x.rows.map(row=>change(table,row,name,value))) else x)
      val m="fp_schema_manifest"
      Vector("expected_server_uuid"->binary(uuidBytes(plan.mysqlServerUuid())),
        "expected_binding_uuid"->binary(uuidBytes(plan.bindingUuid())),"provisioning_uuid"->binary(uuidBytes(plan.provisioningUuid())),
        "configuration_sha256"->binary(configuration),"source_provenance_bytes"->binary(provenance),
        "source_provenance_sha256"->binary(digest(provenance)),"limit_profile_bytes"->binary(i.sourceLimitProfile.canonicalBytes()),
        "limit_profile_sha256"->binary(i.sourceLimitProfile.sha256())).foreach((name,value)=>update(m,name,value))
      Vector("max_region_bytes"->i.sourceLimitProfile.maxRegionBytes(),"max_chunk_bytes"->i.sourceLimitProfile.maxChunkBytes(),
        "max_decompressed_bytes"->i.sourceLimitProfile.maxDecompressedBytes(),"max_nbt_depth"->i.sourceLimitProfile.maxNbtDepth().toLong,
        "max_members"->i.sourceLimitProfile.maxMembers(),"max_retained_bytes"->i.sourceLimitProfile.maxRetainedBytes(),
        "max_sql_packet_bytes"->i.sourceLimitProfile.maxSqlPacketBytes(),"max_native_artifact_bytes"->i.sourceLimitProfile.maxNativeArtifactBytes())
        .foreach((name,value)=>update(m,name,MySqlFormat3Cell.SignedInteger(value)))
      update("fp_runtime_writer","binding_uuid",binary(uuidBytes(plan.bindingUuid())))
      update("fp_runtime_writer","lease_until",MySqlFormat3Cell.DateTime(now))
      val connection=Connection.copy(serverUuid=plan.mysqlServerUuid(),catalog=i.schema)
      val scope=MySqlFormat3ObservationScope(boot.bootId(),boot.generation(),1L)
      c.before=MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,connection,scope,i.budget.packetBytes,i,rows,r)
      val receipt=source.originalReceipt()
      val receiptHash=digest(receipt).toVector
      c.target=MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(c.before,i,r,plan,
        source.sourceWorldIdentity(),receiptHash,seed,targetWriter)
      val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(c.before,c.target,i,r,plan,
        source.sourceWorldIdentity(),receiptHash,targetWriter)
      c.beforeGraph=MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(c.before,i,r)
      c.targetGraph=MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(c.target,i,r)
      val beforeWriter=new B1ResumeRecord.WriterAssertions(plan.bindingUuid(),0L,null,now,null)
      val targetSlot=new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid,context.writerEpoch,context.bindingUuid,
        context.operationUuid,context.canonicalFingerprintSha256.toArray)
      val afterWriter=new B1ResumeRecord.WriterAssertions(plan.bindingUuid(),1L,boot.bootId(),targetWriter.leaseUntil,targetSlot)
      // connection generation/ID是明确fixture DATA；不声称存在实际JDBC连接或资格。
      val attempt=new B1ResumeRecord.AttemptAssertions(operation(21),B1ResumeRecord.Phase.PREPARE,plan.prepareWriteUuid(),
        boot.generation(),operation(22),1L,source.namespace(),MySqlFreshPrepareRules.operation(context),
        beforeWriter,afterWriter,c.beforeGraph.toResumeAssertions,c.targetGraph.toResumeAssertions)
      val state=new B1ResumeRecord.FullStateAssertions(source.currentOwner(),null,null,attempt,
        beforeWriter,afterWriter,attempt,null,null,null)
      c.record=new B1ResumeRecord(0L,new Array[Byte](32),B1ResumeRecord.Kind.GENESIS,
        java.util.HexFormat.of().parseHex(source.anchorSha256()),receipt,source.namespace(),state)
      c.body=MySqlFull26WitnessCodec.begin(i,r)
      c.body.encode(c.record,c.before,c.target)
      val size=c.body.byteLength
      val outputFee=Math.addExact(512L,Math.multiplyExact(4L,size.toLong))
      r.reserveTemporaryMemory(outputFee,"native intent test bounded complete body transfer BEFORE allocation")
      c.outputFee=outputFee
      c.output=new java.io.ByteArrayOutputStream(size)
      c.body.writeBodyTo(c.output)
      c.bytes=c.output.toByteArray
      c.artifact=c.native.artifacts.publish(c.record,c.bytes,configuration)
      c.body.requireReference(c.artifact.reference())
      c.container=new B1ResumeRecordV6(c.record,new Array[Byte](32),new B1ResumeRecordV6.References(
        c.artifact.reference(),c.artifact.reference(),null,null))
      c.ack=c.native.records.publishGenesis(c.container,java.util.List.of(c.artifact))
      c.ack.requireRegisteredCurrent()
      c.ack.requireCurrent()
      assertArrayEquals(B1ResumeRecordV6.encodeData(c.container),B1ResumeRecordV6.encodeData(c.ack.record()))
      assertSame(c.ack,c.native.records.retainCurrentInventory(c.ack).current())
      assertNull(c.record.assertions().latestSettlement())
      assertEquals(B1ResumeRecord.Phase.PREPARE,c.record.assertions().pendingAttempt().phase())
      assertNull(c.native.records.retainedFailure())
      // 保留实际输出/codec/snapshot及其费用和source；原静态runner持有本case到child真实退出。
    catch
      case cause:Throwable=>c.failure=cause;throw cause
  /** 仅store-local identity与真实失败边界；不冒充Actor Planned5或SQL admission。 */
  def exerciseNativeReservationEntryFailures(directory:java.nio.file.Path):Unit =
    exerciseFreshNativeIntent(directory)
    val c=nativeIntent
    val r=c.root
    val records=c.native.records
    val identity=c // opaque store-local identity marker，不是Actor plan。
    val supplier:java.util.function.Supplier[B1ResumeRecordV6]=()=>
      c.supplierCalls+=1
      c.container // 原已发布GENESIS，不伪造合法confirmation候选。
    try
      val beforeNull=r.accountedMemoryBytes
      val absent=assertThrows(classOf[java.io.IOException],()=>records.reserveConfirmation(identity,null,supplier))
      assertEquals("exact current V6 native ACK required",absent.getMessage)
      assertEquals(0,c.supplierCalls)
      assertEquals(beforeNull,r.accountedMemoryBytes)
      assertNull(records.retainedFailure())
      c.foreignRecords=new WindowsB1V6WitnessRecordStore(c.native.source,c.native.artifacts,r)
      val beforeForeign=r.accountedMemoryBytes
      val foreign=assertThrows(classOf[java.io.IOException],()=>c.foreignRecords.reserveConfirmation(identity,c.ack,supplier))
      assertEquals("exact current V6 native ACK required",foreign.getMessage)
      assertEquals(0,c.supplierCalls)
      assertEquals(beforeForeign,r.accountedMemoryBytes)
      assertNull(c.foreignRecords.retainedFailure())
      c.ack.requireCurrent()
      val workspace=Math.addExact(512L,Math.multiplyExact(4L,B1ResumeRecordV6.MAX_RECORD_BYTES.toLong))
      val readFee=Math.addExact(256L,Math.multiplyExact(2L,c.artifact.reference().fullLength()))
      assertTrue(readFee<workspace,"actual reference rebarrier must fit below the isolated encoding workspace")
      assertTrue(r.availableMemoryBytes>=workspace,"actual fixture must first have room for the encoding workspace")
      val pressure=Math.subtractExact(r.availableMemoryBytes,workspace-1L)
      r.reserveTemporaryMemory(pressure,"test-only isolated V6 encoding workspace pressure")
      c.pressureFee=pressure
      var pressurePrimary:Throwable=null
      try
        val pressured=r.accountedMemoryBytes
        c.expectedCapacityFailure=assertThrows(classOf[java.sql.SQLDataException],()=>records.reserveConfirmation(identity,c.ack,supplier))
        val capacity=c.expectedCapacityFailure
        assertTrue(capacity.getMessage.startsWith("V6 complete confirmation bounded encoding BEFORE allocation exceeds"),capacity.getMessage)
        assertEquals(0,c.supplierCalls)
        assertEquals(pressured,r.accountedMemoryBytes)
        assertNull(records.retainedFailure())
      catch
        case cause:Throwable=>pressurePrimary=cause;throw cause
      finally
        try
          r.releaseTemporaryMemory(c.pressureFee,"test-only isolated V6 encoding workspace pressure removed")
          c.pressureFee=0L
        catch
          case cleanup:Throwable=>
            if pressurePrimary==null then throw cleanup
            else pressurePrimary.addSuppressed(cleanup)
      c.ack.requireCurrent()
      val beforeInvalid=r.accountedMemoryBytes
      c.expectedReservationFailure=assertThrows(classOf[java.io.IOException],()=>records.reserveConfirmation(identity,c.ack,supplier))
      // 原GENESIS不是其自身的outer-chain successor；拒绝来自真consecutiveData。
      assertEquals("V6 outer predecessor changed",c.expectedReservationFailure.getMessage)
      assertEquals(1,c.supplierCalls)
      assertSame(c.expectedReservationFailure,records.retainedFailure())
      assertEquals(Math.addExact(beforeInvalid,workspace),r.accountedMemoryBytes)
      val retained=r.accountedMemoryBytes
      val retry=assertThrows(classOf[java.io.IOException],()=>records.reserveConfirmation(identity,c.ack,supplier))
      assertEquals("V6 native publication/discovery/retirement unavailable",retry.getMessage)
      assertSame(c.expectedReservationFailure,retry.getCause)
      assertEquals(1,c.supplierCalls)
      val genesisBlock=assertThrows(classOf[java.io.IOException],()=>records.publishGenesis(c.container,java.util.List.of(c.artifact)))
      assertEquals("reserved confirmation owns original publication slot",genesisBlock.getMessage)
      val nextBlock=assertThrows(classOf[java.io.IOException],()=>records.publishNext(c.ack,c.container,java.util.List.of(c.artifact)))
      assertEquals("reserved confirmation owns original publication slot",nextBlock.getMessage)
      assertSame(c.expectedReservationFailure,records.retainedFailure())
      assertEquals(retained,r.accountedMemoryBytes)
      // 原失败reservation/fee、extra store及source均由静态runner保留到真实child退出。
    catch
      case cause:Throwable=>c.failure=cause;throw cause
  /** 只到健康reservation前缀；候选与Actor在SQL前预备的DATA同构，绝不发布CONFIRMED。 */
  def exerciseNativeHealthyConfirmationReservation(directory:java.nio.file.Path):Unit =
    exerciseFreshNativeIntent(directory)
    val c=nativeIntent
    val r=c.root
    val records=c.native.records
    try
      // 保留所有候选编码、clone/hash、wrapper/closure及失败槽；不把此测试费与store的E/P混算。
      c.confirmationWorkspaceFee=Math.addExact(8192L,Math.multiplyExact(16L,B1ResumeRecordV6.MAX_RECORD_BYTES.toLong))
      r.reserveRetainedMemory(c.confirmationWorkspaceFee,"healthy reservation test candidate/encoding/partial slots BEFORE allocation")
      c.confirmationInventory=records.retainCurrentInventory(c.ack)
      val history=c.confirmationInventory.records()
      assertEquals(1,history.size(),"this finite prefix starts from only the original durable GENESIS")
      assertSame(c.ack,history.getFirst())
      val old=c.ack.record().transition()
      val attempt=old.assertions().pendingAttempt()
      assertNotNull(attempt)
      c.confirmationEvidence=B1ResumeRecordCanonical.encodeSchema5Data(old)
      c.confirmationPredecessorBytes=B1ResumeRecordV6.encodeData(c.ack.record())
      c.confirmationInnerHash=java.security.MessageDigest.getInstance("SHA-256").digest(c.confirmationEvidence)
      c.confirmationOuterHash=java.security.MessageDigest.getInstance("SHA-256").digest(c.confirmationPredecessorBytes)
      // 原Actor reserveConfirmation在executePlan之前使用此固定candidate协议；这里没有SQL/readback事实。
      c.confirmationSettlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
        "MYSQL_ORIGINAL_COMPOSITE_PREPARE_COMMIT_IDLE_FULL26_READBACK_V1",c.confirmationEvidence,new Array[Byte](32))
      c.confirmationState=new B1ResumeRecord.FullStateAssertions(old.assertions().currentOwner(),null,null,attempt,
        attempt.targetWriter(),attempt.targetWriter(),null,c.confirmationSettlement,null,null)
      c.confirmationTransition=new B1ResumeRecord(Math.addExact(old.sequence(),1L),c.confirmationInnerHash,
        B1ResumeRecord.Kind.PREPARED_CONFIRMED,old.sourceAnchorSha256(),old.originalReceipt(),old.barrierNamespace(),c.confirmationState)
      c.confirmationCandidate=new B1ResumeRecordV6(c.confirmationTransition,c.confirmationOuterHash,
        new B1ResumeRecordV6.References(c.artifact.reference(),null,c.artifact.reference(),null))
      c.confirmationBytes=B1ResumeRecordV6.encodeData(c.confirmationCandidate)
      B1ResumeRecordV6.consecutiveData(c.container,c.confirmationPredecessorBytes,c.confirmationCandidate)
      c.confirmationExpectedE=Math.addExact(512L,Math.multiplyExact(4L,c.confirmationBytes.length.toLong))
      var historyFee=Math.addExact(256L,Math.multiplyExact(Math.addExact(history.size().toLong,1L),256L))
      var referencePeak=0L
      val oldRecords=history.iterator()
      while oldRecords.hasNext do
        val ack=oldRecords.next()
        historyFee=Math.addExact(historyFee,Math.multiplyExact(ack.retainedCanonicalByteLength().toLong,4L))
        val references=ack.record().allReferences().iterator()
        while references.hasNext do
          referencePeak=Math.max(referencePeak,Math.addExact(256L,Math.multiplyExact(2L,references.next().fullLength())))
      historyFee=Math.addExact(historyFee,Math.multiplyExact(B1ResumeRecordV6.MAX_RECORD_BYTES.toLong,4L))
      val references=c.confirmationCandidate.allReferences().iterator()
      while references.hasNext do
        referencePeak=Math.max(referencePeak,Math.addExact(256L,Math.multiplyExact(2L,references.next().fullLength())))
      c.confirmationExpectedP=Math.max(historyFee,referencePeak)
      c.confirmationSupplier=()=>
        c.supplierCalls+=1
        c.confirmationCandidate
      c.confirmationBefore=r.accountedMemoryBytes
      // c仅是store-local identity，绝不是Actor Planned5或任何执行/安装资格。
      c.confirmationToken=records.reserveConfirmation(c,c.ack,c.confirmationSupplier)
      assertEquals(1,c.supplierCalls)
      assertEquals(Math.addExact(c.confirmationBefore,Math.addExact(c.confirmationExpectedE,c.confirmationExpectedP)),r.accountedMemoryBytes)
      c.confirmationToken.requirePrepared(c,c.ack)
      c.confirmationWrongIdentity=assertThrows(classOf[java.io.IOException],()=>c.confirmationToken.requirePrepared(c.body,c.ack))
      assertEquals("exact original plan/current V6 confirmation reservation required",c.confirmationWrongIdentity.getMessage)
      c.confirmationToken.requirePrepared(c,c.ack)
      // 不伪造第二份ACK；null是明确的非原predecessor。
      c.confirmationWrongPredecessor=assertThrows(classOf[java.io.IOException],()=>c.confirmationToken.requirePrepared(c,null))
      assertEquals("exact original plan/current V6 confirmation reservation required",c.confirmationWrongPredecessor.getMessage)
      c.confirmationToken.requirePrepared(c,c.ack)
      c.confirmationDuplicate=assertThrows(classOf[java.io.IOException],()=>records.reserveConfirmation(c,c.ack,c.confirmationSupplier))
      assertEquals("original confirmation reservation is single-use",c.confirmationDuplicate.getMessage)
      assertEquals(1,c.supplierCalls)
      c.confirmationToken.requirePrepared(c,c.ack)
      c.ack.requireCurrent()
      assertNull(records.retainedFailure())
      assertEquals(Math.addExact(c.confirmationBefore,Math.addExact(c.confirmationExpectedE,c.confirmationExpectedP)),r.accountedMemoryBytes)
      assertNull(c.ack.record().transition().assertions().latestSettlement(),"native current record remains the original unconfirmed GENESIS")
      assertEquals(B1ResumeRecord.Kind.GENESIS,c.ack.record().transition().kind())
      // 不调用publishConfirmation、不退P或普通close；原强holder保留candidate/token/费用到实际child退出。
    catch
      case cause:Throwable=>c.failure=cause;throw cause
  private def dataId(n:Long)=new java.util.UUID(0L,n)
  private def dataContext(i:MySqlScriptSchemaInputs,format:Int,
      requestSha:Vector[Byte]=Vector.fill(32)(3.toByte),binding:java.util.UUID=dataId(205))=new MySqlRepositoryOperationContextIdentity(
    com.wi.floatingpod.server.io.IoOperationTag(Scope.bootUuid,Scope.bootGeneration,Scope.connectionGeneration,dataId(201)),
    dataId(202),dataId(203),dataId(204),1L,"fpb1:"+("A"*43),binding,1L,
    java.time.LocalDateTime.of(2026,10,8,18,0,0,123456000),Connection.serverUuid,Connection.serverVersion,
    i.schema,Connection.effectiveUser,dataId(206),format,2,i.ddlSha256,"a"*64,Vector.fill(32)(1.toByte),
    "FRESH_PLAN",Vector.fill(32)(2.toByte),i.sourceLimitProfile.sha256().toVector,"RUN",
    requestSha,i.sourceLimitProfile)

  @Test def controlledDebugMaterializesCompleteFull26AndRejectsLegacyGetterWithoutLosingHistory():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val contextSeed=dataContext(i,5,binding=BindingUuid)
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(BindingUuid,
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val request=MySqlControlledDebugPrepareRequest(binding,dataId(701),dataId(702),dataId(703),
        com.wi.floatingpod.domain.FloatingPodPose("world",0d,64d,0d,0f,0f,0),dataId(704))
      val operation=request.repositoryOperation
      val plan=MySqlOwnedRepositoryOperationPlan.single(contextSeed.tag,contextSeed.workflowUuid,
        contextSeed.reserveOperationUuid,operation,i.sourceLimitProfile)
      val context=dataContext(i,5,plan.requestIdentitySha256,BindingUuid)
      val writerShape=i.forTable("fp_runtime_writer")
      def writerCell(row:Vector[MySqlFormat3Cell],name:String,value:MySqlFormat3Cell)=
        row.updated(writerShape.columns.indexWhere(_.name==name),value)
      val rows=full26OrdinaryRelations(i).map { relation=>
        if relation.table==writerShape.table then relation.copy(rows=relation.rows.map { row=>
          writerCell(writerCell(writerCell(row,"owner_boot_uuid",binary(uuidBytes(Scope.bootUuid))),
            "writer_epoch",MySqlFormat3Cell.SignedInteger(context.writerEpoch)),
            "lease_until",MySqlFormat3Cell.DateTime(context.fixedLeaseUntil))
        }) else relation
      }
      val before=snapshot(i,allocation,rows)
      val materialized=MySqlControlledDebugWorkflowMaterializer.materializeDeploymentReceipt(request,plan,operation,
        context,context.fixedLeaseUntil,before,false,256,allocation)
      assertSame(before,materialized.deploymentPreimage)
      assertEquals(26,materialized.deploymentReserveTarget.relations.size)
      assertEquals(26,materialized.deploymentExecuteTarget.relations.size)
      i.canonicalOrder.filterNot(shape=>MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==shape.table)).foreach { shape=>
        assertEquals(before.relation(shape.table),materialized.deploymentReserveTarget.relation(shape.table))
        assertEquals(before.relation(shape.table),materialized.deploymentExecuteTarget.relation(shape.table))
      }
      materialized.requireObservedDeploymentReceipt(MySqlOperationEffectPhase.Execute,materialized.deploymentExecuteTarget,true)
      def contextForPlan(actual:MySqlOwnedRepositoryOperationPlan)=new MySqlRepositoryOperationContextIdentity(
        actual.tag,actual.workflowUuid,actual.reserveOperationUuid,context.barrierGenerationUuid,context.actualConnectionId,
        context.barrierNamespace,context.bindingUuid,context.writerEpoch,context.fixedLeaseUntil,context.serverUuid,
        context.serverVersion,context.schema,context.effectiveUser,context.provisioningUuid,5,2,context.ddlSha256,
        context.approvedObjectManifestSha256,context.configurationSha256,context.sourceProvenanceKind,
        context.sourceProvenanceSha256,context.limitProfileSha256,"RUN",actual.requestIdentitySha256,i.sourceLimitProfile)
      val activation=MySqlRepositoryOperation.CommitControlledDebugCreate(binding,request.podUuid,request.attemptUuid,
        request.footprintUuid,0L,request.targetPose,request.createOperationUuid,256)
      val activationTag=context.tag.copy(operationId=dataId(711))
      val activationPlan=MySqlOwnedRepositoryOperationPlan.single(activationTag,plan.workflowUuid,dataId(712),activation,i.sourceLimitProfile)
      val activationContext=contextForPlan(activationPlan)
      val active=MySqlControlledDebugWorkflowMaterializer.materializeDeploymentReceipt(request,activationPlan,activation,
        activationContext,context.fixedLeaseUntil,materialized.deploymentExecuteTarget,true,256,allocation)
      active.verify()
      val base=com.wi.floatingpod.domain.FloatingPodState(request.podUuid,None,1L,"world",0d,64d,0d,0f,0f,true,
        context.fixedLeaseUntil.toInstant(java.time.ZoneOffset.UTC).toEpochMilli,Some(0),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
      val driftOperation=MySqlTrackedDriftRequest(binding,base,base.copy(posX=2d),request.attemptUuid).repositoryOperation
      val driftPlan=MySqlOwnedRepositoryOperationPlan.single(context.tag.copy(operationId=dataId(721)),plan.workflowUuid,
        dataId(722),driftOperation,i.sourceLimitProfile)
      val driftContext=contextForPlan(driftPlan)
      val drift=MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(driftPlan,driftOperation,driftContext,
        context.fixedLeaseUntil,active.deploymentExecuteTarget,1L,allocation)
      drift.verify()
      val driftAfter=drift.completeExecuteTarget match
        case image:MySqlScriptSchemaSnapshot=>image
        case _=>throw new AssertionError("Full26 drift lost complete source")
      val unknown=before.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable)
      assertTrue(unknown.rows.nonEmpty)
      assertEquals(unknown,driftAfter.relation(unknown.table))
      assertEquals(before.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable),
        driftAfter.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable))
      active.deploymentExecuteTarget.relation("fp_recovery_operation").rows.foreach(r=>
        assertTrue(driftAfter.relation("fp_recovery_operation").rows.contains(r)))
      val operationShape=i.forTable("fp_recovery_operation")
      val originalExecute=materialized.deploymentExecuteTarget.relation(operationShape.table).rows.find(row=>
        MySqlModeOperationEffectCodec.uuidEquals(row(operationShape.columns.indexWhere(_.name=="operation_uuid")),
          plan.executeOperationUuid)).get
      val historical=MySqlHistoricalBusinessReceipt.verify(originalExecute,materialized.deploymentExecuteTarget,
        materialized.deploymentExecuteTarget,allocation)
      assertEquals(originalExecute,historical.row)
      assertEquals(26,historical.before.size)
      assertEquals(materialized.deploymentExecuteTarget.relations,historical.target)
      MySqlControlledDebugPreparedComparison.requireOriginalDeploymentReceipt(materialized.deploymentExecuteTarget,
        materialized.deploymentExecuteTarget,request,allocation,allocation,None)
      val quota=MySqlDeploymentReceiptRelationShapes.quota
      val quotaIndex=quota.columns.indexWhere(column=> !quota.primaryKey.contains(column.name))
      val changed=materialized.deploymentExecuteTarget.relations.map { relation=>
        if relation.table==quota.table then relation.copy(rows=relation.rows.map(_.updated(quotaIndex,MySqlFormat3Cell.SignedInteger(2L))))
        else relation
      }
      assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlControlledDebugPreparedComparison.requireOriginalDeploymentReceipt(materialized.deploymentExecuteTarget,
          snapshot(i,allocation,changed),request,allocation,allocation,None))
      assertThrows(classOf[java.sql.SQLDataException],()=>materialized.preimage)
    finally allocation.closeRootReadBudget()

  @Test def modeCanonicalFull26UsesActualPlanContextAndObservesAdditionalQuotaRows():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val image=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      val contextSeed=dataContext(i,5)
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(contextSeed.bindingUuid,
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val operation=MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding,dataId(207),dataId(208),0L,
        dataId(209),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle,
        com.wi.floatingpod.domain.FloatingPodPose("world",0d,64d,0d,0f,0f,0))
      val plan=MySqlOwnedRepositoryOperationPlan.single(contextSeed.tag,contextSeed.workflowUuid,
        contextSeed.reserveOperationUuid,operation,i.sourceLimitProfile)
      val context=dataContext(i,5,plan.requestIdentitySha256)
      def project(value:MySqlScriptSchemaSnapshot,identity:MySqlRepositoryOperationContextIdentity)=
        MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
          MySqlModeOperationEffectCodec.projectImage(value,i,allocation,plan,operation,identity,0L,
            MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Before)
        }
      val canonical=project(image,context)
      assertEquals(MySqlModeProjectionVersion.V2.encoding,canonical.encoding)
      val quota=MySqlDeploymentReceiptRelationShapes.quota
      val index=quota.columns.indexWhere(column=> !quota.primaryKey.contains(column.name))
      val changed=image.relations.map { relation=>
        if relation.table==quota.table then relation.copy(rows=relation.rows.map(_.updated(index,MySqlFormat3Cell.SignedInteger(2L))))
        else relation
      }
      assertFalse(canonical.exactlyEquals(project(snapshot(i,allocation,changed),context)))
      assertThrows(classOf[java.sql.SQLDataException],()=>project(image,dataContext(i,3,plan.requestIdentitySha256)))
      val locator=row("physical_recovery_locator",
        "binding_uuid"->binary(uuidBytes(binding.bindingUuid)),"pod_uuid"->binary(uuidBytes(dataId(207))),
        "locator_revision"->MySqlFormat3Cell.SignedInteger(0L))
      val withLocator=snapshot(i,allocation,image.relations.map { relation=>
        if relation.table=="physical_recovery_locator" then relation.copy(rows=Vector(locator)) else relation
      })
      val phase=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlModeOperationEffectCodec.canonicalDeploymentReceiptPhase(plan,operation,context,0L,
          MySqlOperationEffectPhase.Reserve,withLocator,withLocator,allocation)
      }
      assertTrue(phase.preimage.exactlyEquals(project(withLocator,context)))
      assertEquals(plan.reserveOperationUuid,phase.operationContext.operationUuid)
      assertEquals(context.writerEpoch,phase.operationContext.writerEpoch)
      assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
          MySqlModeOperationEffectCodec.canonicalDeploymentReceiptPhase(plan,operation,context,1L,
            MySqlOperationEffectPhase.Reserve,withLocator,withLocator,allocation)
        })
    finally allocation.closeRootReadBudget()

  @Test def driftFull26ProjectionIncludesAdditionalRelationsAndRejectsLegacyContext():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val seed=dataContext(i,5)
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(seed.bindingUuid,
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val base=com.wi.floatingpod.domain.FloatingPodState(dataId(901),None,0L,"world",1d,64d,1d,
        0f,0f,true,1L,Some(0),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
      val request=MySqlTrackedDriftRequest(binding,base,base.copy(posX=2d),dataId(902))
      val operation=request.repositoryOperation
      val plan=MySqlOwnedRepositoryOperationPlan.single(seed.tag,seed.workflowUuid,seed.reserveOperationUuid,operation,i.sourceLimitProfile)
      val context=dataContext(i,5,plan.requestIdentitySha256)
      val image=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      def project(value:MySqlScriptSchemaSnapshot,actual:MySqlRepositoryOperationContextIdentity)=
        MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
          MySqlDriftOperationEffectCodec.projectImage(value,i,allocation,plan,operation,actual,0L,
            MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Before)
        }
      val original=project(image,context)
      val quota=MySqlDeploymentReceiptRelationShapes.quota
      val column=quota.columns.indexWhere(c=> !quota.primaryKey.contains(c.name))
      val changed=image.relations.map { relation=>
        if relation.table==quota.table then relation.copy(rows=relation.rows.map(_.updated(column,MySqlFormat3Cell.SignedInteger(2L))))
        else relation
      }
      assertFalse(original.exactlyEquals(project(snapshot(i,allocation,changed),context)))
      assertThrows(classOf[java.sql.SQLDataException],()=>project(image,dataContext(i,3,plan.requestIdentitySha256)))
    finally allocation.closeRootReadBudget()

  @Test def full26ModeMaterializerRejectsMissingOwnedDomainWithoutInventingAWorkflow():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val seed=dataContext(i,5,binding=BindingUuid)
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(BindingUuid,
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val operation=MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding,dataId(951),dataId(952),0L,
        dataId(953),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle,
        com.wi.floatingpod.domain.FloatingPodPose("world",0d,64d,0d,0f,0f,0))
      val plan=MySqlOwnedRepositoryOperationPlan.single(seed.tag,seed.workflowUuid,seed.reserveOperationUuid,operation,i.sourceLimitProfile)
      val context=dataContext(i,5,plan.requestIdentitySha256,BindingUuid)
      val before=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      val draft=new MySqlModeOriginalWitnessDraft(plan,operation,context,allocation)
      val cause=assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlModeWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,context,
          context.fixedLeaseUntil,before,0L,allocation,draft))
      assertTrue(cause.getStackTrace.nonEmpty)
      assertTrue(before.relation("floating_pod_state").rows.isEmpty)
      assertTrue(before.relation("fp_recovery_operation").rows.isEmpty)
    finally allocation.closeRootReadBudget()

  @Test def full26ModeAndDriftMaterializeBothPhasesFromCompleteActiveGraph():Unit =
    for drift <- Vector(false,true) do
      val images=new MySqlModeOperationEffectCodecTest().modeWorkflowImages(SourceProfile,BindingUuid)
      val f=images.fixture
      val old=f.context
      val i=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(old.schema,Full26DdlSha256,
        f.profile,Inputs.budget,MySqlDeploymentReceiptSchemaDefinition.Budget(1_000_000L,1_000_000L))
      val allocation=root(i)
      val fixtureRoots=Vector(images.before,images.reserveTarget,images.executeTarget).map(_.allocationBudget).distinct
      try
        def put(table:String,r:Vector[MySqlFormat3Cell],name:String,value:MySqlFormat3Cell)=
          r.updated(i.forTable(table).columns.indexWhere(_.name==name),value)
        // 原活动图的history/context字节原封保留；schema5六关系在新原root中按实际identity建立。
        val common=images.before.relations.map { relation=>
          if relation.table=="fp_schema_manifest" then relation.copy(rows=relation.rows.map(r=>
            put(relation.table,put(relation.table,r,"format_version",MySqlFormat3Cell.SignedInteger(5L)),
              "ddl_sha256",binary(java.util.HexFormat.of().parseHex(i.ddlSha256)))))
          else relation
        }.map(r=>r.table->r).toMap
        val carried=full26OrdinaryRelations(i)
        val carriedHistory=carried.find(_.table=="fp_recovery_operation").get.rows
        val rows=carried.map { relation=>
          val commonRelation=common.get(relation.table).map { original=>
            if relation.table=="fp_recovery_operation" then original.copy(rows=(original.rows++carriedHistory).sortWith((a,b)=>
              MySqlControlledDebugDelta.compareKey(a,b,i.forTable(relation.table))<0)) else original
          }
          commonRelation.getOrElse(relation.copy(rows=relation.rows.map { original=>
            val shape=i.forTable(relation.table)
            var r=original
            if shape.columns.exists(_.name=="binding_uuid") then r=put(relation.table,r,"binding_uuid",binary(uuidBytes(f.binding.bindingUuid)))
            if shape.columns.exists(_.name=="provisioning_uuid") then r=put(relation.table,r,"provisioning_uuid",binary(uuidBytes(old.provisioningUuid)))
            if relation.table=="binding_bootstrap_intent" then
              r=put(relation.table,r,"stage",MySqlFormat3Cell.Text("COMPLETE"))
              r=put(relation.table,r,"marker_ack_sha256",binary(Array.fill[Byte](32)(0x45.toByte)))
            r
          }))
        }
        val before=MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,images.before.connection,
          images.before.observationScope,i.budget.packetBytes,i,rows,allocation)
        MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(before,i,allocation)
        val base=com.wi.floatingpod.domain.FloatingPodState(f.podUuid,Some(dataId(0x12)),0L,"world",0d,64d,0d,
          0f,0f,true,100L,Some(7),com.wi.floatingpod.domain.PodOperatingMode.Normal)
        val operation=if drift then MySqlTrackedDriftRequest(f.binding,base,base.copy(posX=2d,yaw=12f),f.attemptUuid).repositoryOperation
          else f.operation
        val plan=MySqlOwnedRepositoryOperationPlan.single(old.tag,old.workflowUuid,old.reserveOperationUuid,operation,f.profile)
        val context=new MySqlRepositoryOperationContextIdentity(old.tag,old.workflowUuid,old.reserveOperationUuid,
          old.barrierGenerationUuid,old.actualConnectionId,old.barrierNamespace,old.bindingUuid,old.writerEpoch,
          old.fixedLeaseUntil,old.serverUuid,old.serverVersion,old.schema,old.effectiveUser,old.provisioningUuid,
          5,old.physicalFormat,i.ddlSha256,old.approvedObjectManifestSha256,old.configurationSha256,
          old.sourceProvenanceKind,old.sourceProvenanceSha256,old.limitProfileSha256,"RUN",plan.requestIdentitySha256,f.profile)
        def full(image:MySqlModeOperationEffectCodec.Image):MySqlScriptSchemaSnapshot=image match
          case value:MySqlScriptSchemaSnapshot=>value
          case _=>throw new AssertionError("materializer lost actual Full26 image")
        val (reserve,execute)=if drift then
          val result=MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,context,
            context.fixedLeaseUntil,before,0L,allocation)
          result.verify()
          assertThrows(classOf[java.sql.SQLDataException],()=>result.executeTarget)
          (full(result.completeReserveTarget),full(result.completeExecuteTarget))
        else
          val draft=new MySqlModeOriginalWitnessDraft(plan,operation,context,allocation)
          val result=MySqlModeWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,context,
            context.fixedLeaseUntil,before,0L,allocation,draft)
          result.verify()
          assertThrows(classOf[java.sql.SQLDataException],()=>result.executeTarget)
          (full(result.completeReserveTarget),full(result.completeExecuteTarget))
        assertEquals(26,reserve.relations.size)
        assertEquals(26,execute.relations.size)
        val additional=i.canonicalOrder.filterNot(s=>MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==s.table))
        assertEquals(6,additional.size)
        additional.foreach { shape=>
          assertEquals(before.relation(shape.table),reserve.relation(shape.table))
          assertEquals(before.relation(shape.table),execute.relation(shape.table))
        }
        assertTrue(before.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.nonEmpty)
        assertEquals(carried.find(_.table==MySqlDeploymentReceiptSchemaDefinition.QuotaTable).get,
          before.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable))
        carriedHistory.foreach(r=>assertTrue(before.relation("fp_recovery_operation").rows.contains(r)))
        val originalHistory=before.relation("fp_recovery_operation").rows
        originalHistory.foreach { r=>
          assertTrue(reserve.relation("fp_recovery_operation").rows.contains(r))
          assertTrue(execute.relation("fp_recovery_operation").rows.contains(r))
        }
        val domain=execute.relation("floating_pod_state").rows.head
        assertEquals(MySqlFormat3Cell.SignedInteger(1L),MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","state_version"))
        assertEquals(MySqlFormat3Cell.SignedInteger(1L),MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","is_active"))
        assertEquals(before.relation("physical_recovery_footprint_chunk"),execute.relation("physical_recovery_footprint_chunk"))
      finally
        allocation.closeRootReadBudget()
        fixtureRoots.foreach(_.closeRootReadBudget())

  @Test def full26DriftMaterializerRejectsMissingCriticalBaseWithoutInventingAWorkflow():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val seed=dataContext(i,5,binding=BindingUuid)
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(BindingUuid,
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val base=com.wi.floatingpod.domain.FloatingPodState(dataId(961),None,0L,"world",1d,64d,1d,
        0f,0f,true,1L,Some(0),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
      val operation=MySqlTrackedDriftRequest(binding,base,base.copy(posX=2d),dataId(962)).repositoryOperation
      val plan=MySqlOwnedRepositoryOperationPlan.single(seed.tag,seed.workflowUuid,seed.reserveOperationUuid,operation,i.sourceLimitProfile)
      val context=dataContext(i,5,plan.requestIdentitySha256,BindingUuid)
      val before=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      val cause=assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,context,
          context.fixedLeaseUntil,before,0L,allocation))
      assertTrue(cause.getStackTrace.nonEmpty)
      assertTrue(before.relation("floating_pod_state").rows.isEmpty)
      assertTrue(before.relation("fp_recovery_operation").rows.isEmpty)
    finally allocation.closeRootReadBudget()

  @Test def full26BootSourceProtectionUsesOriginalCompleteRootAndRejectsForeignRoot():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    val foreign=root(i)
    try
      val bindingShape=i.forTable("physical_recovery_binding")
      val rows=full26GenesisRelations(i,catalogPresent=true).map { relation=>
        if relation.table==bindingShape.table then relation.copy(rows=relation.rows.map(_.updated(
          bindingShape.columns.indexWhere(_.name=="cutover_state"),MySqlFormat3Cell.Text("CERTIFIED"))))
        else relation
      }
      val before=snapshot(i,allocation,rows)
      val index=MySqlBootSourceProtectionIndex.fromDeploymentReceipt(before,i,allocation)
      assertTrue(index.carriers.isEmpty)
      assertTrue(index.unresolved.isEmpty,index.unresolved.toString)
      assertEquals(MySqlBootSourceProtectionIndex.Unprotected,index.classify(0,0,0))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlBootSourceProtectionIndex.fromDeploymentReceipt(before,i,foreign))
    finally
      foreign.closeRootReadBudget()
      allocation.closeRootReadBudget()

  @Test def originalScopeKeepsActualSchema5FormatAndLegacyDecoderRemainsClosed():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    val legacyRoot=new MySqlFormat3ReadBudget(i.sourceLimitProfile,i.budget.packetBytes)
    def context(format:Int)=dataContext(i,format)
    def scopeBytes(value:MySqlRepositoryOperationContextIdentity)=
      MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_SCOPE_DATA_TEST",i.sourceLimitProfile)(value.writeScopeIdentityFields)
      }.borrowedBinary
    try
      val encoded=scopeBytes(context(5))
      val actual=MySqlNormalRecallCanonical.decodeDeploymentReceiptScopeFields(encoded,i,allocation)
      assertEquals(5,actual.stableSession.schemaFormat)
      assertEquals(2,actual.stableSession.physicalFormat)
      assertEquals(i.ddlSha256,actual.stableSession.ddl)
      assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.decodeScopeFields(encoded,legacyRoot))
      val legacy=scopeBytes(context(3))
      assertEquals(3,MySqlNormalRecallCanonical.decodeScopeFields(legacy,legacyRoot).stableSession.schemaFormat)
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlNormalRecallCanonical.decodeDeploymentReceiptScopeFields(legacy,i,allocation))
      assertThrows(classOf[IllegalArgumentException],()=>context(4))
    finally
      legacyRoot.closeRootReadBudget();allocation.closeRootReadBudget()

  @Test def handoffBodyPreservesUnownedBeforeAndRejectsWrongEpochOrNonWriterDelta():Unit =
    import com.wi.floatingpod.server.B1ResumeRecord
    import com.wi.floatingpod.server.B1ResumeRecord.*
    val i=Full26Inputs
    val allocation=root(i)
    val now=java.time.LocalDateTime.of(2026,10,28,1,0)
    val oldBoot=dataId(850);val newBoot=dataId(851);val oldOperation=dataId(852)
    val logical=dataId(853);val attemptId=dataId(854)
    val fingerprint=Vector.fill(32)(7.toByte)
    val oldWriter=MySqlBusinessWriterRow(BindingUuid,3L,None,now,Some(oldOperation),Some(fingerprint),now.plusHours(1))
    val nextWriter=oldWriter.copy(writerEpoch=4L,ownerBootUuid=Some(newBoot),leaseUntil=now.plusHours(2))
    val shape=i.forTable("fp_runtime_writer")
    def writerRows(writer:MySqlBusinessWriterRow)=full26OrdinaryRelations(i).map { relation=>
      if relation.table!=shape.table then relation else relation.copy(rows=relation.rows.map { row=>
        val changes=Map("writer_epoch"->MySqlFormat3Cell.SignedInteger(writer.writerEpoch),
          "owner_boot_uuid"->writer.ownerBootUuid.map(id=>binary(uuidBytes(id))).getOrElse(MySqlFormat3Cell.SqlNull),
          "lease_until"->MySqlFormat3Cell.DateTime(writer.leaseUntil),
          "last_commit_operation_uuid"->binary(uuidBytes(oldOperation)),"last_commit_fingerprint"->binary(fingerprint.toArray))
        shape.columns.zip(row).map((column,value)=>changes.getOrElse(column.name,value))
      })
    }
    try
      val before=snapshot(i,allocation,writerRows(oldWriter))
      val target=snapshot(i,allocation,writerRows(nextWriter))
      val effect=MySqlOrdinaryHandoffGraph.effectFingerprint(oldWriter,nextWriter,attemptId,logical,allocation)
      val context=MySqlOperationContext(newBoot,4L,BindingUuid,logical,
        Vector(MySqlExpectedEffect("certified-ordinary/writer",3L,effect)))
      val slot=new ReceiptSlotAssertions(oldBoot,3L,BindingUuid,oldOperation,fingerprint.toArray)
      val old=new WriterAssertions(BindingUuid,3L,null,oldWriter.leaseUntil,slot)
      def record(epoch:Long)=
        val next=new WriterAssertions(BindingUuid,epoch,newBoot,nextWriter.leaseUntil,slot)
        val attempt=new AttemptAssertions(attemptId,Phase.HANDOFF,logical,2L,dataId(855),1L,"fpb1:"+("A"*43),
          MySqlFreshPrepareRules.operation(context),old,next,
          MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,i,allocation).toResumeAssertions,
          MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,i,allocation).toResumeAssertions)
        // Native owner/retirement均为caller DATA；此测试只调用完整body比较，不构造运行时资格。
        val state=new FullStateAssertions(new NativeOwnerAssertions(1234L,133800000000000123L,newBoot,2L),
          new NativeOwnerAssertions(1235L,133800000000000124L,oldBoot,1L),
          new RetirementAssertions("DATA_TEST_ONLY",Array[Byte](1)),null,old,next,attempt,null,null,null)
        new B1ResumeRecord(1L,Array.fill[Byte](32)(1),Kind.HANDOFF_PREPARED,Array.fill[Byte](32)(2),
          Array[Byte](1),"fpb1:"+("A"*43),state)
      MySqlDeploymentReceiptFull26RestoreOwner.requireBootstrapBody(record(4L),before,target,i,allocation)
      assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlDeploymentReceiptFull26RestoreOwner.requireBootstrapBody(record(5L),before,target,i,allocation))
      val quota=MySqlDeploymentReceiptRelationShapes.quota
      val changed=target.relations.map { relation=>
        if relation.table!=quota.table then relation else relation.copy(rows=relation.rows.map { row=>
          row.updated(quota.columns.indexWhere(column=> !quota.primaryKey.contains(column.name)),MySqlFormat3Cell.SignedInteger(2L))
        })
      }
      assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlDeploymentReceiptFull26RestoreOwner.requireBootstrapBody(record(4L),before,snapshot(i,allocation,changed),i,allocation))
    finally allocation.closeRootReadBudget()

  @Test def recallPhysicalDataRoundtripKeepsFull26RootAndLegacyDecoderClosed():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    val legacy=new MySqlFormat3ReadBudget(i.sourceLimitProfile,i.budget.packetBytes)
    try
      val facts=MySqlNormalRecallRemovalFacts(
        com.wi.floatingpod.server.io.IoOperationTag(Scope.bootUuid,Scope.bootGeneration,Scope.connectionGeneration,dataId(801)),
        dataId(802),dataId(803),7L,-1,9,10L,12L,13L)
      val chunks=(for x <- -2 to 2; z <- -2 to 2 yield
        com.wi.floatingpod.storage.PhysicalRecoveryChunk(-1,x,z)).toVector
      val released=MySqlNormalRecallTicketFacts(facts,dataId(804),chunks)
      val removed=MySqlNormalRecallCanonical.removal(facts,allocation)
      val ticket=MySqlNormalRecallCanonical.ticket(released,allocation)
      assertEquals(facts,MySqlNormalRecallCanonical.decodeDeploymentReceiptRemoval(removed.borrowedBinary,i,allocation))
      assertEquals(released,MySqlNormalRecallCanonical.decodeDeploymentReceiptTicket(ticket.borrowedBinary,i,allocation))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.decodeRemoval(removed.borrowedBinary,allocation))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.decodeTicket(ticket.borrowedBinary,allocation))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.decodeDeploymentReceiptTicket(ticket.borrowedBinary,i,legacy))
    finally
      legacy.closeRootReadBudget();allocation.closeRootReadBudget()

  @Test def historicalModeClosureKeepsAllSixAdditionalRelationsAndExactInputRoot():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    val foreign=root(i)
    try
      val image=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      val binding=java.util.UUID.fromString("00000000-0000-0000-0000-000000000101")
      val pod=java.util.UUID.fromString("00000000-0000-0000-0000-000000000102")
      val closure=MySqlModeOperationEffectCodec.selectModeClosure(image,i,allocation,binding,pod)
      assertEquals(i.canonicalOrder,closure.map(_._1))
      val additional=i.canonicalOrder.filterNot(shape=>MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==shape.table))
      assertEquals(6,additional.size)
      assertTrue(additional.exists(shape=>image.relation(shape.table).rows.nonEmpty))
      additional.foreach { shape=>
        val rows=closure.find(_._1.table==shape.table).get._2
        assertSame(image.relation(shape.table).rows,rows)
        assertEquals(image.relation(shape.table).rows,rows)
      }
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlModeOperationEffectCodec.selectModeClosure(image,i,foreign,binding,pod))
    finally
      foreign.closeRootReadBudget();allocation.closeRootReadBudget()

  @Test def actualCompleteFull26ClassifiesBeforeTargetAndNeitherWithoutDroppingAdditionalRelations():Unit =
    val i=Full26Inputs
    val allocation=root(i)
    try
      val before=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=false))
      val target=snapshot(i,allocation,full26GenesisRelations(i,catalogPresent=true))
      assertEquals(26,before.relations.size)
      assertEquals(26,target.relations.size)
      assertEquals(Position.Before,MySqlDeploymentReceiptFull26RestoreOwner.classify(before,before,target))
      assertEquals(Position.After,MySqlDeploymentReceiptFull26RestoreOwner.classify(target,before,target))
      val quota=MySqlDeploymentReceiptRelationShapes.quota
      val index=quota.columns.indexWhere(column=> !quota.primaryKey.contains(column.name))
      assertTrue(index>=0)
      val changed=target.relations.map { relation=>
        if relation.table==quota.table then relation.copy(rows=relation.rows.map(_.updated(index,MySqlFormat3Cell.SignedInteger(2L))))
        else relation
      }
      val neither=snapshot(i,allocation,changed)
      assertEquals(Position.Unresolved,MySqlDeploymentReceiptFull26RestoreOwner.classify(neither,before,target))
      assertFalse(MySqlDeploymentReceiptFull26RestoreOwner.sameData(target,neither))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDeploymentReceiptFull26RestoreOwner.classify(null,before,target))
    finally allocation.closeRootReadBudget()

  @Test def dataEqualityRetainsExactInputsAndServerWhileAllowingHistoricalObservationScope():Unit =
    val i=Full26Inputs
    val original=root(i)
    val historical=root(i)
    val otherServer=root(i)
    val otherInputs=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(i.schema,i.ddlSha256,i.sourceLimitProfile,i.budget,
      MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes=1_000_000L,diagnosticsBytes=1_000_000L))
    val foreign=root(otherInputs)
    try
      val rows=full26GenesisRelations(i,catalogPresent=false)
      val before=snapshot(i,original,rows)
      val old=MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,Connection,
        Scope.copy(bootGeneration=Scope.bootGeneration+1L),i.budget.packetBytes,i,rows,historical)
      assertTrue(MySqlDeploymentReceiptFull26RestoreOwner.sameData(before,old))
      val differentServer=MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,
        Connection.copy(serverUuid=java.util.UUID.fromString("00000000-0000-0000-0000-000000000999")),Scope,
        i.budget.packetBytes,i,rows,otherServer)
      assertFalse(MySqlDeploymentReceiptFull26RestoreOwner.sameData(before,differentServer))
      val differentInputs=snapshot(otherInputs,foreign,full26GenesisRelations(otherInputs,catalogPresent=false))
      assertFalse(MySqlDeploymentReceiptFull26RestoreOwner.sameData(before,differentInputs))
    finally
      foreign.closeRootReadBudget();otherServer.closeRootReadBudget();historical.closeRootReadBudget();original.closeRootReadBudget()
