package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodPose
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding,PhysicalRecoveryCutoverState}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接生产 DATA materializer/独立cycle/full20比较。没有伪造 held/admission/JDBC ACK。 */
class MySqlControlledDebugMaterializerTest:
  @Test def bothWriterTargetsMatchTheActualTransactionReceiptContext(): Unit =
    val m = build(data())
    Vector(m.reserve, m.execute).foreach { phase =>
      val observed = MySqlModeOperationEffectCodec.value(
        phase.target.relation("fp_runtime_writer").rows.head, "fp_runtime_writer", "last_commit_fingerprint")
      val receiptDigest = phase.exactOperationContext.canonicalFingerprintSha256
      assertNotEquals(phase.fingerprint.sha256, receiptDigest, "phase and receipt use distinct canonical domains")
      observed match
        case MySqlFormat3Cell.Binary(bytes) =>
          assertTrue(bytes.equalsVector(receiptDigest), "writer target must match updateWriterReceipt's complete context digest")
        case _ => fail("writer fingerprint must remain a binary digest")
    }

  private def id(n: Long)=new UUID(0L,n)
  private val at=LocalDateTime.of(2026,10,9,1,2,3,123456000)
  private val pose=FloatingPodPose("world",1.25,64.0,2.5,1.25f,-0.0f,0)
  private val schema="fp_controlled_data_test"
  private def u(n: Long): MySqlFormat3Cell=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
    ByteBuffer.allocate(16).putLong(0L).putLong(n).array()))
  private def text(s: String)=MySqlFormat3Cell.Text(s)
  private def integer(n: Long)=MySqlFormat3Cell.SignedInteger(n)
  private def row(table: String,fields: (String,MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
    val supplied=fields.toMap
    MySqlFormat3RelationShapes.forTable(table).columns.map { column => supplied.getOrElse(column.name,
      if column.nullable then MySqlFormat3Cell.SqlNull else column.kind match
        case MySqlFormat3SqlKind.Text => text("pure-data")
        case MySqlFormat3SqlKind.Integer => integer(1L)
        case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(at)
        case MySqlFormat3SqlKind.Binary => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
          Array.fill[Byte](column.maximumLength.getOrElse(32))(1)))) }
  private final case class Data(snapshot: MySqlFormat3FullSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity)
  private def data(state: String="READY"): Data =
    val profile=SourceLimitProfile.of(536870912L,16777216L,134217728L,64L,256L,536870912L,67108864L,536870912L)
    val provenance="controlled-materializer-pure-data-v1".getBytes(StandardCharsets.UTF_8)
    def binary(bytes: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
    val root=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val connection=MySqlFormat3ConnectionObservation(id(18),"9.7.2",schema,"pure@localhost","InnoDB",
      1,123L,"READ-COMMITTED","REPEATABLE-READ","STRICT_ALL_TABLES","+00:00","TLSv1.3","TLS_AES_256_GCM_SHA384")
    val scope=MySqlFormat3ObservationScope(id(17),5L,9L)
    val rows=Map(
      "fp_schema_manifest"->Vector(row("fp_schema_manifest","singleton_fixed1"->integer(1),"backend"->text("MYSQL"),
        "format_version"->integer(3),"ddl_sha256"->binary(Array.fill[Byte](32)(0xaa.toByte)),
        "provisioning_uuid"->u(20),"bootstrap_state"->text(state),"expected_server_uuid"->u(18),
        "expected_binding_uuid"->u(10),"import_state"->text("NONE"),
        "guard_manifest_sha256"->(if state=="READY" then binary(Array.fill[Byte](32)(0xbb.toByte)) else MySqlFormat3Cell.SqlNull),
        "configuration_sha256"->binary(Array.fill[Byte](32)(1)),"source_provenance_kind"->text("FRESH_PLAN"),
        "source_provenance_bytes"->binary(provenance),
        "source_provenance_sha256"->binary(MessageDigest.getInstance("SHA-256").digest(provenance)),
        "limit_profile_encoding"->text("FP_SOURCE_LIMIT_PROFILE_BE_V1"),
        "limit_profile_bytes"->binary(profile.canonicalBytes()),"limit_profile_sha256"->binary(profile.sha256()),
        "max_region_bytes"->integer(profile.maxRegionBytes()),"max_chunk_bytes"->integer(profile.maxChunkBytes()),
        "max_decompressed_bytes"->integer(profile.maxDecompressedBytes()),"max_nbt_depth"->integer(profile.maxNbtDepth()),
        "max_members"->integer(profile.maxMembers()),"max_retained_bytes"->integer(profile.maxRetainedBytes()),
        "max_sql_packet_bytes"->integer(profile.maxSqlPacketBytes()),"max_native_artifact_bytes"->integer(profile.maxNativeArtifactBytes()))),
      "physical_recovery_binding"->Vector(row("physical_recovery_binding","singleton_fixed1"->integer(1),
        "binding_uuid"->u(10),"cutover_state"->text("CERTIFIED"),"format_version"->integer(2))),
      "fp_runtime_writer"->Vector(row("fp_runtime_writer","singleton_fixed1"->integer(1),"binding_uuid"->u(10),
        "writer_epoch"->integer(3),"owner_boot_uuid"->u(17),"lease_until"->MySqlFormat3Cell.DateTime(at.plusHours(1)))),
      "binding_bootstrap_intent"->Vector(row("binding_bootstrap_intent","singleton_fixed1"->integer(1),
        "binding_uuid"->u(10),"binding_format_version"->integer(2),"provisioning_uuid"->u(20),"stage"->text("CERTIFIED"),
        "bootstrap_operation_uuid"->u(23),"prepare_write_uuid"->u(24),"certify_write_uuid"->u(25),
        "source_world_identity"->text("world"),"marker_ack_sha256"->binary(Array.fill[Byte](32)(3)))))
    val relations=MySqlFormat3RelationShapes.canonicalOrder.map(shape=>MySqlFormat3RelationSnapshot(shape.table,
      rows.getOrElse(shape.table,Vector.empty)))
    root.initialize(schema,connection,scope,profile.maxSqlPacketBytes());root.recordMaterializedSnapshot(relations)
    val snapshot=new MySqlFormat3FullSnapshot(schema,connection,scope,profile.maxSqlPacketBytes(),profile,relations,root)
    val request=MySqlControlledDebugPrepareRequest(PhysicalRecoveryBinding(id(10),PhysicalRecoveryCutoverState.Certified,2),
      id(11),id(13),id(21),pose,id(22))
    val tag=IoOperationTag(id(17),5L,9L,id(16))
    val plan=MySqlOwnedRepositoryOperationPlan.single(tag,id(14),id(15),request.repositoryOperation,profile)
    val context=new MySqlRepositoryOperationContextIdentity(tag,id(14),id(15),id(19),123L,"fpb1:"+("A"*43),
      id(10),3L,at.plusHours(1),id(18),"9.7.2",schema,"pure@localhost",id(20),3,2,"a"*64,"b"*64,
      Vector.fill(32)(1.toByte),"FRESH_PLAN",MessageDigest.getInstance("SHA-256").digest(provenance).toVector,profile.sha256().toVector,"RUN",plan.requestIdentitySha256,profile)
    Data(snapshot,request,plan,context)
  private def build(d: Data)=MySqlControlledDebugWorkflowMaterializer.materialize(d.request,d.plan,d.request.repositoryOperation,
    d.context,at,d.snapshot,false,256,d.snapshot.allocationBudget)
  private def change(m: MySqlControlledDebugWorkflowMaterialization,table: String,name: String,value: MySqlFormat3Cell,
      execute: Boolean=true): MySqlFormat3FullSnapshot =
    val target=m.executeTarget
    val selected=MySqlControlledDebugImages.one(target,table,r=> table!="fp_recovery_operation" ||
      MySqlModeOperationEffectCodec.value(r,table,"operation_uuid").asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(
        if execute then m.plan.executeOperationUuid else m.plan.reserveOperationUuid))
    val index=MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name==name)
    MySqlControlledDebugImages.put(target,table,selected.updated(index,value),m.originalRoot,true)

  @Test def emptyCertifiedDataProducesPreparedTargetZeroFullGraph(): Unit =
    val m=build(data());m.verify()
    assertEquals(25,m.executeTarget.relation("physical_recovery_footprint_chunk").rows.size)
    assertEquals(2,m.executeTarget.relation("fp_recovery_operation").rows.size)
    val domain=m.executeTarget.relation("floating_pod_state").rows.head
    assertEquals(integer(0),MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","state_version"))
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","owner_uuid"))
  @Test def rearmCurrentExpectedPreimageIsNullNotZero(): Unit =
    val m=build(data());val execute=m.executeTarget.relation("fp_recovery_operation").rows.find(r=>
      MySqlModeOperationEffectCodec.value(r,"fp_recovery_operation","operation_uuid").asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(m.plan.executeOperationUuid)).get
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlModeOperationEffectCodec.value(execute,"fp_recovery_operation","expected_domain_version"))
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlModeOperationEffectCodec.value(execute,"fp_recovery_operation","expected_locator_revision"))
  @Test def bootstrappingCannotMaterializeOrdinaryCreate(): Unit =
    assertThrows(classOf[SQLDataException],()=>build(data("BOOTSTRAPPING")))
  @Test def changedOrdinaryOwnerIsRejectedByCompleteReadback(): Unit =
    val m=build(data());assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,
      change(m,"floating_pod_state","owner_uuid",u(90)),true))
  @Test def changedCurrentMaskedFingerprintCannotCertifyItself(): Unit =
    val m=build(data());val bad=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(8)))
    assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,
      change(m,"fp_recovery_operation","operation_fingerprint",bad),true))
  @Test def changedIndependentReservePreimageCannotCertifyItself(): Unit =
    val m=build(data());val bad=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array[Byte](1,2,3)))
    assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,
      change(m,"fp_recovery_operation","preimage_bytes",bad,false),true))
  @Test def changedWitnessAndOrdinaryTimestampAreRejected(): Unit =
    val m=build(data());assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,
      change(m,"fp_recovery_operation","result_at",MySqlFormat3Cell.DateTime(at.plusSeconds(1))),true))
    val witness=tamper(op(m.executeTarget,m.plan.executeOperationUuid),"commit_witness_bytes",m.originalRoot)
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(witness,m.originalRoot.profile,m.originalRoot)
    val actual=MySqlControlledDebugImages.put(m.executeTarget,O,witness,m.originalRoot,true)
    assertThrows(classOf[SQLDataException],()=>m.execute.requireObserved(actual,true))
  @Test def changedWriterCycleRequiresIndependentRegeneration(): Unit =
    val m=build(data());val bad=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(9)))
    assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,
      change(m,"fp_runtime_writer","last_commit_fingerprint",bad),true))
  @Test def rootRevocationCannotIssueDataSuccess(): Unit =
    val d=data();d.snapshot.allocationBudget.closeRootReadBudget()
    assertThrows(classOf[SQLDataException],()=>build(d))
  @Test def negativeZeroPoseIsPartOfOriginalRequest(): Unit =
    assertFalse(MySqlControlledDebugWorkflowMaterializer.exactPose(pose,pose.copy(pitch=0.0f)))
    assertTrue(MySqlControlledDebugWorkflowMaterializer.exactPose(pose,pose))
  @Test def constructorDoesNotIssueRuntimeGraphWithoutPrivateProof(): Unit =
    val d=data();assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugGraph.prepared(d.plan.tag,d.request,
      d.plan,d.context,d.plan.executeOperationUuid,d.snapshot,d.snapshot.allocationBudget,null))
  @Test def completeImageInsertionRejectsDuplicateKeyAndPreservesOtherRows(): Unit =
    val d=data();val domain=MySqlControlledDebugRows.preparedDomain(id(10),id(11),pose,id(16),at,d.snapshot.allocationBudget)
    val next=MySqlControlledDebugImages.put(d.snapshot,"floating_pod_state",domain,d.snapshot.allocationBudget,false)
    assertSame(d.snapshot.relation("physical_recovery_binding"),next.relation("physical_recovery_binding"))
    assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugImages.put(next,"floating_pod_state",domain,d.snapshot.allocationBudget,false))
  @Test def oneByteRemainingInSameLedgerRejectsBeforeImageAllocation(): Unit =
    val d=data();val root=d.snapshot.allocationBudget;val obligation=root.newCanonicalScope()
    val pressure=Math.subtractExact(root.availableMemoryBytes,1L)
    obligation.reserveRetainedMemory(pressure,"independent same-ledger obligation")
    try
      assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugImages.put(d.snapshot,"physical_recovery_binding",
        d.snapshot.relation("physical_recovery_binding").rows.head,root,true))
    finally obligation.closeScopedReadBudget()
    assertTrue(root.availableMemoryBytes>1L)
  @Test def adequateIndependentComparisonReleasesOnlyItsScratch(): Unit =
    val m=build(data());val root=m.originalRoot;val retained=root.accountedMemoryBytes
    MySqlControlledDebugFullComparison.requireExactReadback(m.executeTarget,m.executeTarget,root)
    assertEquals(retained,root.accountedMemoryBytes)
    assertTrue(retained>0L)
  @Test def unrelatedPodMutationCannotHideOutsideCanonicalClosure(): Unit =
    val m=build(data());val domain=MySqlControlledDebugRows.preparedDomain(id(10),id(99),pose,id(16),at,m.originalRoot)
    val extra=MySqlControlledDebugImages.put(m.executeTarget,"floating_pod_state",domain,m.originalRoot,false)
    assertThrows(classOf[SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Execute,extra,true))
  private def second(first: MySqlControlledDebugWorkflowMaterialization): MySqlControlledDebugWorkflowMaterialization =
    val profile=first.originalRoot.profile
    val request=MySqlControlledDebugPrepareRequest(first.request.expectedBinding,id(111),id(113),id(121),pose,id(122))
    val tag=IoOperationTag(id(17),5L,9L,id(116))
    val plan=MySqlOwnedRepositoryOperationPlan.single(tag,id(114),id(115),request.repositoryOperation,profile)
    val context=new MySqlRepositoryOperationContextIdentity(tag,id(114),id(115),id(19),123L,"fpb1:"+("A"*43),
      id(10),3L,at.plusHours(1),id(18),"9.7.2",schema,"pure@localhost",id(20),3,2,"a"*64,"b"*64,
      Vector.fill(32)(1.toByte),"FRESH_PLAN",Vector.fill(32)(2.toByte),profile.sha256().toVector,"RUN",plan.requestIdentitySha256,profile)
    MySqlControlledDebugWorkflowMaterializer.materialize(request,plan,request.repositoryOperation,context,at,
      first.executeTarget,false,256,first.originalRoot)
  @Test def secondPodCreatePreservesEveryHistoricalRawOperationCell(): Unit =
    val first=build(data());val next=second(first);next.verify()
    first.executeTarget.relation("fp_recovery_operation").rows.foreach { original =>
      val key=MySqlModeOperationEffectCodec.value(original,"fp_recovery_operation","operation_uuid")
      val observed=MySqlControlledDebugImages.one(next.executeTarget,"fp_recovery_operation",row=>
        MySqlModeOperationEffectCodec.value(row,"fp_recovery_operation","operation_uuid")==key)
      MySqlControlledDebugFullComparison.requireExactOperationRow(original,observed,first.originalRoot)
    }
    assertEquals(4,next.executeTarget.relation("fp_recovery_operation").rows.size)
    assertEquals(50,next.executeTarget.relation("physical_recovery_footprint_chunk").rows.size)
    // 不把 first 嵌入图的 raw blobs 复制进第二个 pod 的 canonical 闭包。
    assertTrue(next.execute.targetBytes.length < first.execute.targetBytes.length+4096)
  @Test def foreignHistoricalRawMutationStillFailsSecondPodReadback(): Unit =
    val first=build(data());val next=second(first)
    val original=MySqlControlledDebugImages.one(next.executeTarget,"fp_recovery_operation",row=>
      MySqlModeOperationEffectCodec.value(row,"fp_recovery_operation","operation_uuid") match
        case MySqlFormat3Cell.Binary(value) => value.equalsUuid(first.plan.reserveOperationUuid)
        case _ => false)
    val index=MySqlFormat3RelationShapes.forTable("fp_recovery_operation").columns.indexWhere(_.name=="planned_at")
    val changed=MySqlControlledDebugImages.put(next.executeTarget,"fp_recovery_operation",
      original.updated(index,MySqlFormat3Cell.DateTime(at.plusSeconds(1))),first.originalRoot,true)
    assertThrows(classOf[SQLDataException],()=>next.requireObserved(MySqlOperationEffectPhase.Execute,changed,true))
  private def activate(first: MySqlControlledDebugWorkflowMaterialization): MySqlControlledDebugWorkflowMaterialization =
    val profile=first.originalRoot.profile
    val request=first.request
    val operation=MySqlRepositoryOperation.CommitControlledDebugCreate(request.expectedBinding,request.podUuid,
      request.attemptUuid,request.footprintUuid,0L,request.targetPose,request.createOperationUuid,256)
    val tag=IoOperationTag(id(17),5L,9L,id(216))
    val plan=MySqlOwnedRepositoryOperationPlan.single(tag,first.plan.workflowUuid,id(215),operation,profile)
    val context=new MySqlRepositoryOperationContextIdentity(tag,first.plan.workflowUuid,id(215),id(19),123L,"fpb1:"+("A"*43),
      id(10),3L,at.plusHours(1),id(18),"9.7.2",schema,"pure@localhost",id(20),3,2,"a"*64,"b"*64,
      Vector.fill(32)(1.toByte),"FRESH_PLAN",Vector.fill(32)(2.toByte),profile.sha256().toVector,"RUN",plan.requestIdentitySha256,profile)
    MySqlControlledDebugWorkflowMaterializer.materialize(request,plan,operation,context,at,first.executeTarget,
      true,256,first.originalRoot)
  @Test def activationUsesOriginalTupleAndTerminalExecuteRelease(): Unit =
    val first=build(data());val committed=activate(first);committed.verify()
    val domain=committed.executeTarget.relation("floating_pod_state").rows.head
    assertEquals(integer(1),MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","state_version"))
    assertEquals(integer(1),MySqlModeOperationEffectCodec.value(domain,"floating_pod_state","is_active"))
    val reservation=committed.executeTarget.relation("fp_recovery_reservation").rows.head
    assertEquals(text("RELEASED"),MySqlModeOperationEffectCodec.value(reservation,"fp_recovery_reservation","reservation_state"))
    MySqlModeOperationEffectCodec.value(reservation,"fp_recovery_reservation","last_mutation_operation_uuid") match
      case MySqlFormat3Cell.Binary(value) => assertTrue(value.equalsUuid(committed.plan.executeOperationUuid))
      case _ => fail("terminal execute operation UUID absent")
    val old=first.executeTarget.relation("fp_recovery_operation").rows
    old.foreach { row =>
      val key=MySqlModeOperationEffectCodec.value(row,"fp_recovery_operation","operation_uuid")
      val current=MySqlControlledDebugImages.one(committed.executeTarget,"fp_recovery_operation",r=>
        MySqlModeOperationEffectCodec.value(r,"fp_recovery_operation","operation_uuid")==key)
      MySqlControlledDebugFullComparison.requireExactOperationRow(row,current,first.originalRoot)
    }
  @Test def partialPreparedFootprintCannotActivate(): Unit =
    val first=build(data())
    val relations=first.executeTarget.relations.map { relation =>
      if relation.table=="physical_recovery_footprint_chunk" then
        MySqlFormat3RelationSnapshot(relation.table,relation.rows.tail) else relation
    }
    val bad=new MySqlFormat3FullSnapshot(first.executeTarget.schema,first.executeTarget.connection,
      first.executeTarget.observationScope,first.executeTarget.maxAllowedPacketBytes,first.originalRoot.profile,relations,first.originalRoot)
    val descriptor=new MySqlControlledDebugWorkflowMaterialization(first.request,first.activation,first.plannedAt,
      first.plan,first.operation,first.context,first.originalRoot,first.preimage,first.reserveTarget,bad,first.reserve,first.execute)
    assertThrows(classOf[SQLDataException],()=>activate(descriptor))
  @Test def samePodHistoryDoesNotRecursivelyEmbedItsOriginalGraphs(): Unit =
    val first=build(data());val committed=activate(first)
    // 两个历史 O 的 payload 都保持原 bytes；canonical 只写独立验证后的有限 descriptor。
    assertEquals(4,committed.executeTarget.relation("fp_recovery_operation").rows.size)
    assertTrue(committed.execute.targetBytes.length < first.execute.targetBytes.length+16384)
    MySqlControlledDebugPreparedComparison.requireOriginal(first.executeTarget,committed.preimage,
      first.request,first.originalRoot)
  @Test def dedicatedAdapterPrepareConsumesExactOriginalPlan(): Unit =
    val d=data()
    val command=MySqlOwnedIoCommand.PrepareControlledDebug(d.request,d.plan)
    assertSame(d.plan,command.requireRepositoryPlan(d.plan.tag,d.snapshot.limitProfile))
    assertThrows(classOf[IllegalArgumentException],()=>command.requireRepositoryPlan(
      IoOperationTag(d.plan.tag.bootId,d.plan.tag.bootGeneration,d.plan.tag.connectionGeneration,id(999)),d.snapshot.limitProfile))
  @Test def dedicatedPrepareCannotReusePlanAfterFootprintMutation(): Unit =
    val d=data();val foreign=d.request.copy(footprintUuid=id(999))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedIoCommand.PrepareControlledDebug(foreign,d.plan)
      .requireRepositoryPlan(d.plan.tag,d.snapshot.limitProfile))
  private val O="fp_recovery_operation"
  private val pairs=Map("original_context_bytes"->"original_context_sha256","preimage_bytes"->"preimage_sha256",
    "target_bytes"->"target_sha256","commit_witness_bytes"->"commit_witness_sha256")
  private def op(snapshot: MySqlFormat3FullSnapshot,id: UUID): Vector[MySqlFormat3Cell] =
    MySqlControlledDebugImages.one(snapshot,O,row=>MySqlModeOperationEffectCodec.value(row,O,"operation_uuid") match
      case MySqlFormat3Cell.Binary(value)=>value.equalsUuid(id)
      case _=>false)
  private def putCell(row: Vector[MySqlFormat3Cell],name: String,cell: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    row.updated(MySqlFormat3RelationShapes.forTable(O).columns.indexWhere(_.name==name),cell)
  private def replacePayload(row: Vector[MySqlFormat3Cell],name: String,bytes: Array[Byte]): Vector[MySqlFormat3Cell] =
    val hash=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
    putCell(putCell(row,name,MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))),pairs(name),
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(hash)))
  private def tamper(row: Vector[MySqlFormat3Cell],slot: String,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val payload=if pairs.contains(slot) then slot else pairs.find(_._2==slot).map(_._1).getOrElse(slot)
    val bytes=MySqlModeOperationEffectCodec.value(row,O,payload) match
      case MySqlFormat3Cell.Binary(value)=>value.copyBytes(root,"single byte cycle test")
      case _=>throw new IllegalStateException("matrix slot was not a nonNULL binary field")
    require(bytes.nonEmpty,"one-byte matrix payload")
    bytes(0)=(bytes(0)^1).toByte
    if pairs.contains(payload) then replacePayload(row,payload,bytes)
    else putCell(row,payload,MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes)))
  private val commonSlots=Vector("original_context_bytes","original_context_sha256","target_bytes","target_sha256","operation_fingerprint")
  private val pSlots=Vector("preimage_bytes","preimage_sha256")
  private val hSlots=Vector("commit_witness_bytes","commit_witness_sha256")
  private def matrix(phase: MySqlOperationEffectPhase,target: Boolean,reserve: Boolean,slots: Vector[String]): Unit =
    Vector(build(data()),activate(build(data()))).foreach { m =>
      val effect=if phase==MySqlOperationEffectPhase.Reserve then m.reserve else m.execute
      val image=if target then effect.target else effect.before
      val operation=if reserve then m.plan.reserveOperationUuid else m.plan.executeOperationUuid
      slots.foreach { slot =>
        val changed=tamper(op(image,operation),slot,m.originalRoot)
        // 同步 SHA 合法，真实 RAW 语义验证和重编码先成功；不是 digest mismatch 假覆盖。
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,m.originalRoot.profile,m.originalRoot)
        MySqlRecoveryCanonicalBytes.withAllocationBudget(m.originalRoot) {
          val raw=MySqlRecoveryOperationRawEnvelopeCodec.encode(changed,m.originalRoot.profile)
          assertTrue(raw.exactlyMatches(changed,m.originalRoot.profile),s"raw reencode $phase/$target/$reserve/$slot")
        }
        val actual=MySqlControlledDebugImages.put(image,O,changed,m.originalRoot,true)
        assertThrows(classOf[SQLDataException],()=>effect.requireObserved(actual,target),s"independent cycle $phase/$target/$reserve/$slot")
      }
    }
  @Test def reserveTargetReserveAllSevenTag6SlotsRejectLegalSingleByteTamper(): Unit =
    matrix(MySqlOperationEffectPhase.Reserve,true,true,commonSlots++hSlots)
  @Test def reserveTargetExecuteAllSevenTag6SlotsRejectLegalSingleByteTamper(): Unit =
    matrix(MySqlOperationEffectPhase.Reserve,true,false,commonSlots++pSlots)
  @Test def executeBeforeExecuteAllSevenTag6SlotsRejectLegalSingleByteTamper(): Unit =
    matrix(MySqlOperationEffectPhase.Execute,false,false,commonSlots++pSlots)
  @Test def executeTargetExecuteAllSevenTag6AndWriterSlotsRejectLegalSingleByteTamper(): Unit =
    matrix(MySqlOperationEffectPhase.Execute,true,false,commonSlots++hSlots)
    Vector(build(data()),activate(build(data()))).foreach { m =>
      Vector(m.reserve,m.execute).foreach { effect =>
        val image=effect.target;val row=image.relation("fp_runtime_writer").rows.head
        val index=MySqlFormat3RelationShapes.forTable("fp_runtime_writer").columns.indexWhere(_.name=="last_commit_fingerprint")
        val bytes=row(index).asInstanceOf[MySqlFormat3Cell.Binary].value.copyBytes(m.originalRoot,"writer cycle test")
        bytes(0)=(bytes(0)^1).toByte
        val changed=MySqlControlledDebugImages.put(image,"fp_runtime_writer",row.updated(index,
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))),m.originalRoot,true)
        assertThrows(classOf[SQLDataException],()=>effect.requireObserved(changed,true))
      }
    }
  @Test def reservedExecuteWitnessNullCannotBecomeEmptyBytes(): Unit =
    val m=build(data());val row=op(m.reserveTarget,m.plan.executeOperationUuid)
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlModeOperationEffectCodec.value(row,O,"commit_witness_bytes"))
    val changed=replacePayload(row,"commit_witness_bytes",Array.emptyByteArray)
    assertThrows(classOf[SQLDataException],()=>MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,m.originalRoot.profile,m.originalRoot))
    val actual=MySqlControlledDebugImages.put(m.reserveTarget,O,changed,m.originalRoot,true)
    assertThrows(classOf[SQLDataException],()=>m.reserve.requireObserved(actual,true))
  @Test def reservedExecuteResultNullCannotBecomeNumeric(): Unit =
    val m=build(data());val changed=putCell(op(m.reserveTarget,m.plan.executeOperationUuid),"result_at",integer(0))
    assertThrows(classOf[SQLDataException],()=>MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,m.originalRoot.profile,m.originalRoot))
    assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugImages.put(m.reserveTarget,O,changed,m.originalRoot,true))
  private def historicalImage(m: MySqlControlledDebugWorkflowMaterialization,image: MySqlFormat3FullSnapshot,
      oldOperation: UUID,size: Int): MySqlFormat3FullSnapshot =
    val changed=replacePayload(op(image,oldOperation),"preimage_bytes",Array.fill[Byte](size)(7))
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,m.originalRoot.profile,m.originalRoot)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(m.originalRoot) {
      val raw=MySqlRecoveryOperationRawEnvelopeCodec.encode(changed,m.originalRoot.profile)
      assertTrue(raw.exactlyMatches(changed,m.originalRoot.profile))
    }
    MySqlControlledDebugImages.put(image,O,changed,m.originalRoot,true)
  private def historicalLengths(m: MySqlControlledDebugWorkflowMaterialization,oldOperation: UUID,size: Int): Vector[Int] =
    val before=historicalImage(m,m.execute.before,oldOperation,size)
    val target=historicalImage(m,m.execute.target,oldOperation,size)
    val p=MySqlControlledDebugCanonical.projection(before,m.request,m.plan,m.context,true,
      MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Before,m.originalRoot)
    val t=MySqlControlledDebugCanonical.projection(target,m.request,m.plan,m.context,true,
      MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Target,m.originalRoot)
    val fp=MySqlControlledDebugCanonical.fingerprint(m.request,m.plan,m.context,MySqlOperationEffectPhase.Execute,
      m.execute.scopeBytes,p,t,m.originalRoot)
    val h=MySqlControlledDebugCanonical.witness(m.request,m.plan,MySqlOperationEffectPhase.Execute,m.plannedAt,
      m.execute.scopeBytes,fp,m.execute.contextBytes,p,t,m.execute.originalReserve,m.originalRoot)
    assertEquals(m.execute.before.relation(O).rows.size,before.relation(O).rows.size)
    assertEquals(m.execute.target.relation(O).rows.size,target.relation(O).rows.size)
    Vector(p.length,t.length,fp.length,h.length)
  @Test def samePodHistoricalPayloadSizeDoesNotGrowProjectionFingerprintOrWitness(): Unit =
    val first=build(data());val m=activate(first)
    assertEquals(historicalLengths(m,first.plan.reserveOperationUuid,8),
      historicalLengths(m,first.plan.reserveOperationUuid,32768))
  @Test def lawfulHistoricalPayloadAndSynchronizedRawShaStillFailExactFullReadback(): Unit =
    val first=build(data());val m=activate(first)
    val changed=historicalImage(m,m.executeTarget,first.plan.reserveOperationUuid,32768)
    // 前面真实RAW验证/重编码已通过；这里是完整原始历史义务，不是仅 descriptor length。
    assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugFullComparison.requireExactReadback(
      m.executeTarget,changed,m.originalRoot))
    assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugFullComparison.requireExactOperationRow(
      op(m.executeTarget,first.plan.reserveOperationUuid),op(changed,first.plan.reserveOperationUuid),m.originalRoot))
    assertThrows(classOf[SQLDataException],()=>m.execute.requireObserved(changed,true))

  @Test def historicalRawValidationOnOriginalRootPreservesRetainedGraph(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlControlledDebugHistoricalRow.requireValid(row,root)
      assertSame(root,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    assertSame(row,m.execute.originalReserve.get.row)

  @Test def historicalRawValidationUsesActualComparisonChildWithoutParentSwitch(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val charged=root.accountedMemoryBytes;var child: MySqlFormat3ReadBudget=null
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        assertNotSame(root,child);assertTrue(child.isCanonicalChildOf(root))
        val within=child.accountedMemoryBytes
        MySqlControlledDebugHistoricalRow.requireValid(row,root)
        assertSame(child,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
        assertEquals(within,child.accountedMemoryBytes)
      }
      assertSame(root,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
    }
    assertTrue(child.isClosed);assertFalse(root.isClosed);assertEquals(charged,root.accountedMemoryBytes)
    assertSame(row,m.execute.originalReserve.get.row)

  @Test def historicalRawValidationRejectsForeignActualRootBeforeValidation(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val foreign=new MySqlFormat3ReadBudget(root.profile,root.profile.maxSqlPacketBytes())
    val charged=root.accountedMemoryBytes;val foreignCharge=foreign.accountedMemoryBytes
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(foreign) {
        val failure=assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugHistoricalRow.requireValid(row,root))
        assertTrue(failure.getStackTrace.nonEmpty)
        assertSame(foreign,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
      }
      assertEquals(charged,root.accountedMemoryBytes);assertEquals(foreignCharge,foreign.accountedMemoryBytes)
    finally foreign.closeRootReadBudget()

  @Test def historicalRawValidationRejectsSiblingAsOriginalWithoutScopeSwitch(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val first=root.newCanonicalScope();val sibling=root.newCanonicalScope();val charged=root.accountedMemoryBytes
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.withAllocationBudget(first) {
          assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugHistoricalRow.requireValid(row,sibling))
          assertSame(first,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
          MySqlControlledDebugHistoricalRow.requireValid(row,root)
        }
      }
      assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    finally
      sibling.closeScopedReadBudget()
      first.closeScopedReadBudget()

  @Test def historicalRawValidationChildCapacityRefusalPreservesRootObligation(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        val held=child.availableMemoryBytes-1L
        assertTrue(held>0L);child.reserveRetainedMemory(held,"another retained DATA responsibility")
        try
          val pressured=root.accountedMemoryBytes
          val failure=assertThrows(classOf[SQLDataException],()=>MySqlControlledDebugHistoricalRow.requireValid(row,root))
          assertTrue(failure.getStackTrace.nonEmpty);assertNull(failure.getCause)
          assertEquals(pressured,root.accountedMemoryBytes)
          assertSame(child,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
        finally child.releaseRetainedMemory(held,"another retained DATA responsibility release")
      }
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    assertSame(row,m.execute.originalReserve.get.row)

  @Test def historicalRawDataFailureKeepsOriginalExceptionAndClosesOnlyChild(): Unit =
    val m=build(data());val root=m.originalRoot;val row=m.execute.originalReserve.get.row
    val charged=root.accountedMemoryBytes;var original: SQLDataException=null;var child: MySqlFormat3ReadBudget=null
    val escaped=assertThrows(classOf[SQLDataException],()=> {
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          try MySqlControlledDebugHistoricalRow.requireValid(row.dropRight(1),root)
          catch
            case failure: SQLDataException =>
              original=failure
              throw failure
        }
      }
    })
    assertSame(original,escaped);assertTrue(escaped.getStackTrace.nonEmpty);assertNull(escaped.getCause)
    assertTrue(child.isClosed);assertFalse(root.isClosed);assertEquals(charged,root.accountedMemoryBytes)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) { MySqlControlledDebugHistoricalRow.requireValid(row,root) }
    assertEquals(charged,root.accountedMemoryBytes);assertSame(row,m.execute.originalReserve.get.row)

  @Test def comparisonUnitValidatesCurrentLocatorOnExactActualChild(): Unit =
    val m=activate(build(data()));val root=m.originalRoot;val snapshot=m.executeTarget
    val charged=root.accountedMemoryBytes;var child: MySqlFormat3ReadBudget=null
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        val result: Unit=MySqlFormat3QueryProjection.validateForComparison(snapshot,root)
        assertSame(child,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
        assertTrue(child.isCanonicalChildOf(root));assertTrue(child.accountedMemoryBytes>0L)
      }
    }
    assertTrue(child.isClosed);assertFalse(root.isClosed);assertEquals(charged,root.accountedMemoryBytes)
    assertSame(snapshot,m.executeTarget)

  @Test def comparisonUnitRequiresChildRatherThanOrdinaryRootScope(): Unit =
    val m=build(data());val root=m.originalRoot;val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(m.executeTarget,root))
      assertSame(root,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)

  @Test def comparisonUnitRejectsForeignActualScopeBeforeAllocation(): Unit =
    val m=build(data());val root=m.originalRoot
    val foreign=new MySqlFormat3ReadBudget(root.profile,root.profile.maxSqlPacketBytes())
    val charged=root.accountedMemoryBytes;val foreignCharge=foreign.accountedMemoryBytes
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(foreign) {
        assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(m.executeTarget,root))
        assertSame(foreign,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
      }
      assertEquals(charged,root.accountedMemoryBytes);assertEquals(foreignCharge,foreign.accountedMemoryBytes)
    finally foreign.closeRootReadBudget()

  @Test def comparisonUnitRejectsSiblingAsOriginalSnapshotRoot(): Unit =
    val m=build(data());val root=m.originalRoot;val sibling=root.newCanonicalScope()
    val charged=root.accountedMemoryBytes
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          assertNotSame(sibling,child)
          assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(m.executeTarget,sibling))
          assertSame(child,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
        }
      }
      assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    finally sibling.closeScopedReadBudget()

  @Test def comparisonUnitRejectsClosedActualChildWithoutReopening(): Unit =
    val m=build(data());val root=m.originalRoot;val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        child.closeScopedReadBudget()
        assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(m.executeTarget,root))
        assertTrue(child.isClosed);assertFalse(root.isClosed)
      }
    }
    assertEquals(charged,root.accountedMemoryBytes)

  @Test def comparisonUnitCapacityFailurePreservesSameLedgerObligation(): Unit =
    val m=build(data());val root=m.originalRoot;val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        val held=child.availableMemoryBytes-1L
        child.reserveRetainedMemory(held,"another retained comparison responsibility")
        try
          val pressured=root.accountedMemoryBytes
          val failure=assertThrows(classOf[SQLDataException],()=>MySqlFormat3QueryProjection.validateForComparison(m.executeTarget,root))
          assertTrue(failure.getStackTrace.nonEmpty);assertNull(failure.getCause)
          assertEquals(pressured,root.accountedMemoryBytes)
        finally child.releaseRetainedMemory(held,"another retained comparison responsibility release")
      }
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)

  @Test def comparisonUnitWrongPosePreservesOriginalFailureAndChildCleanup(): Unit =
    val m=activate(build(data()));val root=m.originalRoot;val snapshot=m.executeTarget
    val row=snapshot.relation("floating_pod_state").rows.head
    val index=MySqlFormat3RelationShapes.forTable("floating_pod_state").columns.indexWhere(_.name=="pos_x")
    val bad=MySqlControlledDebugImages.put(snapshot,"floating_pod_state",
      row.updated(index,MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(33.25))),root,true)
    val charged=root.accountedMemoryBytes;var original: SQLDataException=null;var child: MySqlFormat3ReadBudget=null
    val escaped=assertThrows(classOf[SQLDataException],()=> {
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          try MySqlFormat3QueryProjection.validateForComparison(bad,root)
          catch
            case failure: SQLDataException =>
              original=failure
              throw failure
        }
      }
    })
    assertSame(original,escaped);assertTrue(escaped.getStackTrace.nonEmpty);assertNull(escaped.getCause)
    assertTrue(child.isClosed);assertFalse(root.isClosed);assertEquals(charged,root.accountedMemoryBytes)
    assertSame(snapshot,m.executeTarget)

  @Test def businessQueryAndChunkEntryKeepOriginalRootOnlyForCurrentLocator(): Unit =
    val m=activate(build(data()));val root=m.originalRoot;val snapshot=m.executeTarget
    val chunks=MySqlRepositoryTransitionRules.chunksAt(0,m.request.targetPose.posX,m.request.targetPose.posZ).toList
    val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.from(snapshot,child))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.requireCurrentChunks(snapshot,child,chunks,0,m.request.targetPose.posX,m.request.targetPose.posZ))
        assertSame(child,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
      }
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)

  /** 真实 reader factory 的 DATA budget role；不伪造 JDBC/admission/continuation。 */
  private def readImage(image: MySqlFormat3FullSnapshot, root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    val read=root.newScopedReadBudget(image.maxAllowedPacketBytes)
    var primary: Throwable=null
    try
      read.initialize(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes)
      read.recordMaterializedSnapshot(image.relations)
      new MySqlFormat3FullSnapshot(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,
        image.limitProfile,image.relations,read)
    catch
      case failure: Throwable =>
        primary=failure
        try read.closeScopedReadBudget()
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
        throw primary

  @Test def factoryRolesKeepLegacyParentButSeparateReadAndComparison(): Unit =
    val d=data();val root=d.snapshot.allocationBudget
    val read=root.newScopedReadBudget(d.snapshot.maxAllowedPacketBytes);val comparison=root.newCanonicalScope()
    try
      assertTrue(read.isCanonicalChildOf(root));assertTrue(comparison.isCanonicalChildOf(root))
      assertTrue(read.isSnapshotReadChildOf(root));assertFalse(read.isCanonicalComparisonChildOf(root))
      assertTrue(comparison.isCanonicalComparisonChildOf(root));assertFalse(comparison.isSnapshotReadChildOf(root))
      assertFalse(read.isSnapshotReadChildOf(comparison));assertFalse(comparison.isCanonicalComparisonChildOf(read))
    finally
      read.closeScopedReadBudget();comparison.closeScopedReadBudget()

  @Test def actualReadScopedSourcePassesPhaseFullReadbackAndComparisonUnit(): Unit =
    val m=activate(build(data()));val root=m.originalRoot;val beforeRead=root.accountedMemoryBytes
    val actual=readImage(m.executeTarget,root);val read=actual.allocationBudget
    try
      val charged=root.accountedMemoryBytes
      m.requireObserved(MySqlOperationEffectPhase.Execute,actual,true)
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val scratch=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        MySqlFormat3QueryProjection.validateForComparison(actual,root)
        assertTrue(scratch.isCanonicalComparisonChildOf(root));assertNotSame(read,scratch)
      }
      assertFalse(read.isClosed);assertFalse(root.isClosed);assertEquals(charged,root.accountedMemoryBytes)
      assertSame(read,actual.allocationBudget);assertSame(root,m.executeTarget.allocationBudget)
    finally read.closeScopedReadBudget()
    assertEquals(beforeRead,root.accountedMemoryBytes)

  @Test def canonicalChildCannotImpersonateSnapshotReadSource(): Unit =
    val m=build(data());val root=m.originalRoot;val forgedRole=root.newCanonicalScope()
    val image=m.executeTarget
    val candidate=new MySqlFormat3FullSnapshot(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,
      image.limitProfile,image.relations,forgedRole)
    val charged=root.accountedMemoryBytes
    try
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(candidate,root))
      }
      assertEquals(charged,root.accountedMemoryBytes)
    finally forgedRole.closeScopedReadBudget()

  @Test def snapshotReadSiblingCannotBecomeItsSourceWorkflowRoot(): Unit =
    val m=build(data());val root=m.originalRoot;val actual=readImage(m.executeTarget,root)
    val sibling=root.newScopedReadBudget(actual.maxAllowedPacketBytes)
    try
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        assertThrows(classOf[IllegalArgumentException],()=>
          MySqlFormat3QueryProjection.validateForComparison(actual,sibling,root,None))
      }
      assertFalse(root.isClosed);assertFalse(actual.allocationBudget.isClosed)
    finally
      sibling.closeScopedReadBudget();actual.allocationBudget.closeScopedReadBudget()

  @Test def closedSnapshotReadSourceCannotBeReusedByComparison(): Unit =
    val m=build(data());val root=m.originalRoot;val actual=readImage(m.executeTarget,root)
    actual.allocationBudget.closeScopedReadBudget();val charged=root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.validateForComparison(actual,root))
    }
    assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)

  @Test def sameLedgerDifferentWorkflowWithoutIssuedContinuationIsNoProof(): Unit =
    val d=data();val image=d.snapshot;val profile=image.limitProfile
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val source=MySqlFormat3ReadBudget.rootOnLedger(profile,image.maxAllowedPacketBytes,ledger)
    val current=MySqlFormat3ReadBudget.rootOnLedger(profile,image.maxAllowedPacketBytes,ledger)
    try
      source.initialize(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes)
      source.recordMaterializedSnapshot(image.relations)
      val old=new MySqlFormat3FullSnapshot(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,
        profile,image.relations,source)
      val charged=source.accountedMemoryBytes
      val failure=assertThrows(classOf[MySqlControlledDebugNoProofException],()=>
        MySqlFormat3QueryProjection.validateForComparison(old,source,current,None))
      assertThrows(classOf[MySqlControlledDebugNoProofException],()=>
        MySqlFormat3SourceForestMembershipScan.containsControlled(old,current,d.request.podUuid,source,None))
      assertThrows(classOf[MySqlControlledDebugNoProofException],()=>
        MySqlFormat3SourceForestMembershipScan.containsControlled(old,current,d.request.podUuid,source,Some(null)))
      assertTrue(failure.getStackTrace.nonEmpty);assertNull(failure.getCause)
      assertEquals(charged,source.accountedMemoryBytes);assertFalse(source.isClosed);assertFalse(current.isClosed)
    finally
      current.closeRootReadBudget();source.closeRootReadBudget()

  @Test def snapshotReadScopeIsNotComparisonReentryAndFailureKeepsScope(): Unit =
    val d=data();val root=d.snapshot.allocationBudget;val read=root.newScopedReadBudget(d.snapshot.maxAllowedPacketBytes)
    val charged=root.accountedMemoryBytes;var reached=false
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.withAllocationBudget(read) {
          val failure=assertThrows(classOf[IllegalStateException],()=>MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { reached=true })
          assertTrue(failure.getStackTrace.nonEmpty);assertFalse(reached)
          assertSame(read,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
        }
        assertSame(root,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
      }
      assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    finally read.closeScopedReadBudget()

  @Test def snapshotReadComparisonPressureDoesNotReleaseReadOrOriginalDebt(): Unit =
    val m=build(data());val root=m.originalRoot;val actual=readImage(m.executeTarget,root)
    try
      val charged=root.accountedMemoryBytes
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val scratch=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        val held=scratch.availableMemoryBytes-1L
        scratch.reserveRetainedMemory(held,"other actual comparison responsibility")
        try
          val pressure=root.accountedMemoryBytes
          assertThrows(classOf[SQLDataException],()=>MySqlFormat3QueryProjection.validateForComparison(actual,root))
          assertEquals(pressure,root.accountedMemoryBytes);assertFalse(actual.allocationBudget.isClosed)
        finally scratch.releaseRetainedMemory(held,"other actual comparison responsibility")
      }
      assertEquals(charged,root.accountedMemoryBytes);assertFalse(root.isClosed)
    finally actual.allocationBudget.closeScopedReadBudget()
