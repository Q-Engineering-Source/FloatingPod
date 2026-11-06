package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodPose
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding,PhysicalRecoveryCutoverState}
import com.wi.floatingpod.server.source.{SourceForestCodec,SourceForestMembershipCodec,SourceForestSharedBudgetFixtures}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接生产 DATA materializer/独立cycle/full20比较。没有伪造 held/admission/JDBC ACK。 */
class MySqlInactiveDomainWorkflowMaterializerTest:
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
  /** 合法 DATA existing row，无 locator/current intent；历史 O/footprint 完整保留，无 SQL 发行。 */
  private def existing(locatorVersion: Option[Option[Long]] = None): (MySqlFormat3FullSnapshot,MySqlInactiveDomainUpdateRequest,MySqlOwnedRepositoryOperationPlan,MySqlRepositoryOperationContextIdentity) =
    val old=data()
    val created=build(old)
    var before=created.executeTarget
    val root=before.allocationBudget
    locatorVersion.foreach { version =>
      // 仅 typed DATA：不是实际 cleanup/native proof，也不据此发行 Session admission。
      val originalIntent=before.relation("physical_recovery_intent").rows.head
      val diagnosticProof="inactive-typed-data-cleanup-fixture".getBytes(StandardCharsets.UTF_8)
      val intent=MySqlControlledDebugRows.update(originalIntent,"physical_recovery_intent",Map(
        "phase"->text("CLEANUP_VERIFIED"),"cleanup_operation_uuid"->u(16),
        "cleanup_proof_bytes"->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(diagnosticProof)),
        "cleanup_proof_sha256"->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
          MessageDigest.getInstance("SHA-256").digest(diagnosticProof)))),root)
      before=MySqlControlledDebugImages.put(before,"physical_recovery_intent",intent,root,true)
      val originalLocator=before.relation("physical_recovery_locator").rows.head
      val cell=version match
        case None => MySqlFormat3Cell.SqlNull
        case Some(value) => integer(value)
      val locator=MySqlControlledDebugRows.update(originalLocator,"physical_recovery_locator",Map(
        "locator_state"->text("LEGACY_UNCERTIFIED"),"domain_version"->cell),root)
      before=MySqlControlledDebugImages.put(before,"physical_recovery_locator",locator,root,true)
    }
    root.reserveRetainedMemory(16384L,"inactive pure existing graph fixture containers")
    val relations=before.relations.map { relation =>
      if locatorVersion.isEmpty && (relation.table=="physical_recovery_locator" || relation.table=="physical_recovery_intent") then
        MySqlFormat3RelationSnapshot(relation.table,Vector.empty)
      else relation
    }
    val snapshot=new MySqlFormat3FullSnapshot(before.schema,before.connection,before.observationScope,
      before.maxAllowedPacketBytes,before.limitProfile,relations,root)
    val desired=com.wi.floatingpod.domain.FloatingPodState(id(11),None,0L,"world",2.25,64.0,3.5,
      1.25f,-0.0f,false,0L,Some(0),com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
    val request=MySqlInactiveDomainUpdateRequest(old.request.expectedBinding,id(11),None,0L,desired)
    val tag=IoOperationTag(id(17),5L,9L,id(116))
    val plan=MySqlOwnedRepositoryOperationPlan.single(tag,id(114),id(115),request.repositoryOperation,before.limitProfile)
    val provenance="controlled-materializer-pure-data-v1".getBytes(StandardCharsets.UTF_8)
    val context=new MySqlRepositoryOperationContextIdentity(tag,id(114),id(115),id(119),123L,"fpb1:"+("A"*43),
      id(10),3L,at.plusHours(1),id(18),"9.7.2",schema,"pure@localhost",id(20),3,2,"a"*64,"b"*64,
      Vector.fill(32)(1.toByte),"FRESH_PLAN",MessageDigest.getInstance("SHA-256").digest(provenance).toVector,
      before.limitProfile.sha256().toVector,"RUN",plan.requestIdentitySha256,before.limitProfile)
    MySqlInactiveDomainUpdatePreflight.requireInputGraph(snapshot,root)
    (snapshot,request,plan,context)

  @Test def existingInactiveUpdateHasTwoFixedEffectsAndPreservesHistory(): Unit =
    val (snapshot,request,plan,context)=existing()
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    m.verify()
    assertEquals(2,m.effects.size)
    assertEquals(plan.reserveOperationUuid,m.reserve.operationUuid)
    assertEquals(plan.executeOperationUuid,m.execute.operationUuid)
    val row=m.executeTarget.relation("floating_pod_state").rows.head
    assertEquals(integer(1),MySqlControlledDebugRows.value(row,"floating_pod_state","state_version"))
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlControlledDebugRows.value(row,"floating_pod_state","owner_uuid"))
    assertEquals(snapshot.relation("physical_recovery_footprint").rows,m.executeTarget.relation("physical_recovery_footprint").rows)
    assertEquals(snapshot.relation("physical_recovery_footprint_chunk").rows,m.executeTarget.relation("physical_recovery_footprint_chunk").rows)
    assertTrue(m.executeTarget.relation("physical_recovery_locator").rows.isEmpty)

  @Test def ownerAndVersionConflictsAreNormalRejectionsWithoutMutation(): Unit =
    val (snapshot,request,_,_)=existing()
    val before=snapshot.canonicalSha256Hex
    val wrongOwner=request.copy(expectedOwner=Some(id(99)),desired=request.desired.copy(ownerUuid=Some(id(99))))
    assertEquals(Some(MySqlInactiveDomainUpdateRejection.OwnerMismatch),
      MySqlInactiveDomainUpdatePreflight.rejection(snapshot,wrongOwner,snapshot.allocationBudget))
    val wrongVersion=request.copy(expectedVersion=1L,desired=request.desired.copy(stateVersion=1L))
    assertEquals(Some(MySqlInactiveDomainUpdateRejection.VersionMismatch(0L)),
      MySqlInactiveDomainUpdatePreflight.rejection(snapshot,wrongVersion,snapshot.allocationBudget))
    assertEquals(before,snapshot.canonicalSha256Hex)

  @Test def requestCannotActivateOrReplacePodOrOwner(): Unit =
    val (_,request,_,_)=existing()
    assertThrows(classOf[IllegalArgumentException],()=>request.copy(desired=request.desired.copy(isActive=true)))
    assertThrows(classOf[IllegalArgumentException],()=>request.copy(desired=request.desired.copy(uuid=id(999))))
    assertThrows(classOf[IllegalArgumentException],()=>request.copy(desired=request.desired.copy(ownerUuid=Some(id(999)))))

  @Test def closedOriginalRootCannotMaterialize(): Unit =
    val (snapshot,request,plan,context)=existing()
    snapshot.allocationBudget.closeRootReadBudget()
    assertThrows(classOf[SQLDataException],()=>MySqlInactiveDomainWorkflowMaterializer.materialize(
      request,plan,context,at,snapshot,snapshot.allocationBudget))

  @Test def foreignRootCannotMaterializeEvenWithIdenticalProfile(): Unit =
    val (snapshot,request,plan,context)=existing()
    val foreign=new MySqlFormat3ReadBudget(snapshot.limitProfile,snapshot.limitProfile.maxSqlPacketBytes())
    try assertThrows(classOf[SQLDataException],()=>MySqlInactiveDomainWorkflowMaterializer.materialize(
      request,plan,context,at,snapshot,foreign))
    finally foreign.closeRootReadBudget()

  @Test def sqlNullOwnerAndZeroUuidOwnerHaveDistinctOriginalRequestBytes(): Unit =
    val (snapshot,request,plan,_)=existing()
    val zero=new UUID(0L,0L)
    val owned=request.copy(expectedOwner=Some(zero),desired=request.desired.copy(ownerUuid=Some(zero)))
    val other=MySqlOwnedRepositoryOperationPlan.single(plan.tag,id(214),id(215),owned.repositoryOperation,snapshot.limitProfile)
    assertNotEquals(plan.requestIdentitySha256,other.requestIdentitySha256)
  @Test def changedCurrentFingerprintCannotApproveItself(): Unit =
    val (snapshot,request,plan,context)=existing()
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    val operation=MySqlControlledDebugImages.one(m.executeTarget,"fp_recovery_operation",row =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(row,"fp_recovery_operation","operation_uuid"),plan.executeOperationUuid))
    val changed=MySqlControlledDebugRows.update(operation,"fp_recovery_operation",Map(
      "operation_fingerprint" -> MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(99)))),m.originalRoot)
    val observed=MySqlControlledDebugImages.put(m.executeTarget,"fp_recovery_operation",changed,m.originalRoot,true)
    assertThrows(classOf[SQLDataException],()=>m.execute.requireObserved(observed,true))

  @Test def unexpectedUnrelatedEntitlementInsertionCannotPassFull20Readback(): Unit =
    val (snapshot,request,plan,context)=existing()
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    val table="player_entitlement"
    val oldDomain=snapshot.relation("floating_pod_state").rows.head
    val extra=row(table,"player_uuid"->u(999),"registration_limit"->integer(0),"simultaneous_active_limit"->integer(0),
      "binding_uuid"->u(10),"created_operation_uuid"->MySqlControlledDebugRows.value(oldDomain,"floating_pod_state","last_mutation_operation_uuid"))
    val observed=MySqlControlledDebugImages.put(m.executeTarget,table,extra,m.originalRoot,false)
    assertThrows(classOf[SQLDataException],()=>m.execute.requireObserved(observed,true))

  @Test def equalRequestBytesCannotReplaceOriginalRootIdentity(): Unit =
    val (snapshot,request,plan,context)=existing()
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    val other=MySqlOwnedRepositoryOperationPlan.single(plan.tag,plan.workflowUuid,plan.reserveOperationUuid,
      request.repositoryOperation,snapshot.limitProfile)
    assertNotSame(plan,other)
    assertEquals(plan.requestIdentitySha256,other.requestIdentitySha256)
    val foreign=new MySqlFormat3ReadBudget(snapshot.limitProfile,snapshot.limitProfile.maxSqlPacketBytes())
    try assertThrows(classOf[SQLDataException],()=>m.verifyUsingRoot(foreign))
    finally foreign.closeRootReadBudget()
  /** 真 codec fixture + complete SQL typed rows/FKs；无 source custody/SQL issuer。 */
  private def withSourceForest(snapshot: MySqlFormat3FullSnapshot,currentHold: Boolean,brokenIndex: Boolean): MySqlFormat3FullSnapshot =
    val root=snapshot.allocationBudget
    val seed=SourceForestSharedBudgetFixtures.forest(10L,false,false,false)
    val prior=seed.identity()
    val creator=id(16) // existing DATA create 的原 execute tag；不新造已执行 SQL。
    val identity=new SourceForestCodec.Identity(id(10),id(2010),id(2011),prior.sourceEpoch(),creator,
      prior.worldIdentity(),prior.saveIdentity(),prior.loaderProfile(),prior.catalogGeneration(),prior.readGeneration())
    val forest=new SourceForestCodec.Forest(identity,seed.coverage(),seed.sourceObservation(),seed.rawRoots(),
      seed.fixedRoots(),seed.actualRoots(),seed.carriers(),seed.members())
    val encoded=SourceForestCodec.encode(forest,snapshot.limitProfile)
    val membership=SourceForestMembershipCodec.derive(forest,snapshot.limitProfile)
    val index=membership.bytes().copy()
    if brokenIndex then index(index.length-1)=(index(index.length-1)^1).toByte
    def bin(bytes: Array[Byte])=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
    val observation=forest.sourceObservation().copy()
    val unit=row("physical_source_unit","binding_uuid"->u(10),"unit_uuid"->u(2010),
      "world_identity"->text(identity.worldIdentity()),"save_identity"->text(identity.saveIdentity()),
      "loader_identity"->text(identity.loaderProfile()),
      "catalog_generation"->bin(ByteBuffer.allocate(16).putLong(identity.catalogGeneration().getMostSignificantBits)
        .putLong(identity.catalogGeneration().getLeastSignificantBits).array()),
      "read_generation"->integer(identity.readGeneration()),"source_observation_bytes"->bin(observation),
      "source_observation_sha256"->bin(MessageDigest.getInstance("SHA-256").digest(observation)),
      "current_epoch"->(if currentHold then integer(identity.sourceEpoch()) else MySqlFormat3Cell.SqlNull),
      "current_forest_uuid"->(if currentHold then u(2011) else MySqlFormat3Cell.SqlNull),
      "unit_state"->text(if currentHold then "SEALED" else "DRAFT"),
      "unit_revision"->integer(0),"created_operation_uuid"->u(16),"last_mutation_operation_uuid"->u(16))
    val forestRow=row("physical_source_forest","binding_uuid"->u(10),"unit_uuid"->u(2010),"forest_uuid"->u(2011),
      "source_epoch"->integer(identity.sourceEpoch()),"forest_state"->text("SEALED"),
      "forest_codec"->text(SourceForestCodec.DOMAIN),"forest_bytes"->bin(encoded.bytes().copy()),
      "forest_sha256"->bin(encoded.sha256().copy()),"member_count"->integer(encoded.memberCount()),
      "root_count"->integer(encoded.rootCount()),"carrier_count"->integer(encoded.carrierCount()),
      "retained_bytes"->integer(encoded.retainedBytes()),"created_operation_uuid"->u(16),
      "fp_pod_uuid_index_bytes"->bin(index),
      "fp_pod_uuid_index_sha256"->bin(MessageDigest.getInstance("SHA-256").digest(index)))
    var result=MySqlControlledDebugImages.put(snapshot,"physical_source_unit",unit,root,false)
    result=MySqlControlledDebugImages.put(result,"physical_source_forest",forestRow,root,false)
    if currentHold then
      var ordinal=0
      while ordinal<forest.carriers().size() do
        val carrier=forest.carriers().get(ordinal)
        val key=MySqlRecoveryResourceKey.carrier(identity.saveIdentity(),identity.loaderProfile(),carrier.regionKey(),
          carrier.chunkX(),carrier.chunkZ(),snapshot.limitProfile)
        val keyBytes=key.copyCanonicalBytes(root,"CURRENT_HOLD DATA fixture exact carrier key")
        val reservation=row("fp_recovery_reservation","binding_uuid"->u(10),"resource_kind"->text("CARRIER"),
          "resource_key_hash"->bin(key.keyHashSha256.toArray),"resource_key"->bin(keyBytes),
          "resource_pod_uuid"->MySqlFormat3Cell.SqlNull,"resource_unit_uuid"->MySqlFormat3Cell.SqlNull,
          "workflow_uuid"->u(14),"operation_uuid"->u(16),"reservation_revision"->integer(0),
          "reservation_state"->text("HELD"),"last_mutation_operation_uuid"->u(16))
        result=MySqlControlledDebugImages.put(result,"fp_recovery_reservation",reservation,root,false)
        val obligation=row("physical_source_obligation","binding_uuid"->u(10),"unit_uuid"->u(2010),"forest_uuid"->u(2011),
          "resource_kind"->text("CARRIER"),"carrier_key_hash"->bin(key.keyHashSha256.toArray),"carrier_key"->bin(keyBytes.clone()),
          "purpose"->text("CURRENT_HOLD"),"obligation_state"->text("OPEN"),"obligation_revision"->integer(0),
          "created_operation_uuid"->u(16),"last_mutation_operation_uuid"->u(16))
        result=MySqlControlledDebugImages.put(result,"physical_source_obligation",obligation,root,false)
        ordinal+=1
    // SQL shape/SHA and full graph FK checks do not themselves decode forest or membership bytes.
    MySqlFormat3SnapshotValidation.validateRelations(result.relations)
    MySqlControlledDebugRelations.requireValidFullGraph(result,root)
    result

  @Test def currentHoldOnlyValidForestRemainsLegalWithoutPendingTransfer(): Unit =
    val (snapshot,request,plan,context)=existing()
    val source=withSourceForest(snapshot,true,false)
    assertTrue(source.relation("physical_source_transfer").rows.isEmpty)
    assertEquals(None,MySqlInactiveDomainUpdatePreflight.rejection(source,request,source.allocationBudget))
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),source,source.allocationBudget)
    m.verify()
    assertEquals(source.relation("physical_source_forest").rows,m.executeTarget.relation("physical_source_forest").rows)
    assertEquals(source.relation("physical_source_obligation").rows,m.executeTarget.relation("physical_source_obligation").rows)

  @Test def malformedUnrelatedHistoryWithoutPendingTransferFailsActualMaterializer(): Unit =
    val (snapshot,request,plan,context)=existing()
    val source=withSourceForest(snapshot,false,true)
    assertTrue(source.relation("physical_source_obligation").rows.isEmpty)
    assertTrue(source.relation("physical_source_transfer").rows.isEmpty)
    val baseline=source.allocationBudget.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException],()=>MySqlInactiveDomainWorkflowMaterializer.materialize(
      request,plan,context,at.plusSeconds(1),source,source.allocationBudget))
    assertEquals(baseline,source.allocationBudget.accountedMemoryBytes)

  @Test def malformedCurrentHoldOnlyForestFailsActualMaterializerBeforeEffectsAllocation(): Unit =
    val (snapshot,request,plan,context)=existing()
    val source=withSourceForest(snapshot,true,true)
    assertTrue(source.relation("physical_source_transfer").rows.isEmpty)
    val baseline=source.allocationBudget.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException],()=>MySqlInactiveDomainWorkflowMaterializer.materialize(
      request,plan,context,at.plusSeconds(1),source,source.allocationBudget))
    assertEquals(baseline,source.allocationBudget.accountedMemoryBytes)
  @Test def existingNonnullLocatorKeepsReserveAndAdvancesOnlyNecessaryExecuteFields(): Unit =
    val (snapshot,request,plan,context)=existing(Some(Some(0L)))
    val original=snapshot.relation("physical_recovery_locator").rows.head
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    m.verify()
    assertEquals(original,m.reserveTarget.relation("physical_recovery_locator").rows.head)
    val target=m.executeTarget.relation("physical_recovery_locator").rows.head
    assertEquals(integer(1),MySqlControlledDebugRows.value(target,"physical_recovery_locator","domain_version"))
    assertEquals(integer(1),MySqlControlledDebugRows.value(target,"physical_recovery_locator","locator_revision"))
    val shape=MySqlFormat3RelationShapes.forTable("physical_recovery_locator")
    shape.columns.zipWithIndex.foreach { (column,index) =>
      if !Set("domain_version","locator_revision","last_mutation_operation_uuid").contains(column.name) then
        assertEquals(original(index),target(index),column.name)
    }

  /** 只验证 DATA phase targets；真实 DDL6 trigger 顺序仍必须独立 SQL RED/GREEN。 */
  @Test def nullableLocatorMustBeNormalizedInsideReserveAndThenAdvanceInExecute(): Unit =
    val (snapshot,request,plan,context)=existing(Some(None))
    val original=snapshot.relation("physical_recovery_locator").rows.head
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlControlledDebugRows.value(original,"physical_recovery_locator","domain_version"))
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    m.verify()
    val reserved=m.reserveTarget.relation("physical_recovery_locator").rows.head
    assertEquals(integer(0),MySqlControlledDebugRows.value(reserved,"physical_recovery_locator","domain_version"))
    assertEquals(integer(1),MySqlControlledDebugRows.value(reserved,"physical_recovery_locator","locator_revision"))
    assertTrue(MySqlModeOperationEffectCodec.uuidEquals(
      MySqlControlledDebugRows.value(reserved,"physical_recovery_locator","last_mutation_operation_uuid"),plan.executeOperationUuid))
    val target=m.executeTarget.relation("physical_recovery_locator").rows.head
    assertEquals(integer(1),MySqlControlledDebugRows.value(target,"physical_recovery_locator","domain_version"))
    assertEquals(integer(2),MySqlControlledDebugRows.value(target,"physical_recovery_locator","locator_revision"))
    val shape=MySqlFormat3RelationShapes.forTable("physical_recovery_locator")
    shape.columns.zipWithIndex.foreach { (column,index) =>
      if !Set("domain_version","locator_revision","last_mutation_operation_uuid").contains(column.name) then
        assertEquals(original(index),reserved(index),column.name)
        assertEquals(original(index),target(index),column.name)
    }
  @Test def nullableReserveRetainsOriginalExpectedRevisionAndRejectsChangedReadback(): Unit =
    val (snapshot,request,plan,context)=existing(Some(None))
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    val originalRevision=MySqlControlledDebugRows.value(snapshot.relation("physical_recovery_locator").rows.head,
      "physical_recovery_locator","locator_revision")
    Vector(m.reserveTarget,m.executeTarget).foreach { image =>
      image.relation("fp_recovery_operation").rows.foreach { operation =>
        if MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(operation,"fp_recovery_operation","operation_uuid"),plan.reserveOperationUuid) ||
          MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(operation,"fp_recovery_operation","operation_uuid"),plan.executeOperationUuid) then
          assertEquals(originalRevision,MySqlControlledDebugRows.value(operation,"fp_recovery_operation","expected_locator_revision"))
      }
    }
    val reserved=m.reserveTarget.relation("physical_recovery_locator").rows.head
    val altered=MySqlControlledDebugRows.update(reserved,"physical_recovery_locator",
      Map("locator_revision"->integer(2)),snapshot.allocationBudget)
    val observed=MySqlControlledDebugImages.put(m.reserveTarget,"physical_recovery_locator",altered,snapshot.allocationBudget,true)
    assertThrows(classOf[java.sql.SQLDataException],()=>m.requireObserved(MySqlOperationEffectPhase.Reserve,observed,true))
    assertEquals(MySqlFormat3Cell.SqlNull,MySqlControlledDebugRows.value(snapshot.relation("physical_recovery_locator").rows.head,
      "physical_recovery_locator","domain_version"))
    m.verify()
  private def requireActualGuardBindings(table: String,before: Vector[MySqlFormat3Cell],after: Vector[MySqlFormat3Cell],
      expectedGuards: Vector[String]): MySqlInactiveDomainDeltaWriter.GuardedUpdate =
    val command=MySqlInactiveDomainDeltaWriter.guardedUpdate("fp_guard_structure_test",table,before,after)
    val shape=MySqlFormat3RelationShapes.forTable(table)
    assertEquals(shape,command.shape)
    val parts=command.sql.split(" WHERE ",-1)
    assertEquals(2,parts.length)
    val changed=shape.columns.indices.filter(i => !shape.primaryKey.contains(shape.columns(i).name) && before(i)!=after(i)).toVector
    val setColumns=parts(0).substring(parts(0).indexOf(" SET ")+5).split(",").toVector.map { fragment =>
      assertTrue(fragment.matches("`[a-z_][a-z0-9_]*`=\\?"))
      fragment.substring(1,fragment.indexOf('`',1))
    }
    val predicates=parts(1).split(" AND ").toVector.map { fragment =>
      assertTrue(fragment.matches("`[a-z_][a-z0-9_]*` <=> \\?"))
      fragment.substring(1,fragment.indexOf('`',1))
    }
    assertEquals(changed.map(i=>shape.columns(i).name),setColumns)
    assertEquals(shape.primaryKey++expectedGuards,predicates)
    assertEquals(command.sql.count(_=='?'),command.values.size)
    assertEquals(changed.map(i=>(i,after(i)))++predicates.map { name =>
      val index=shape.columns.indexWhere(_.name==name)
      assertTrue(index>=0,name)
      (index,before(index))
    },command.values)
    command

  @Test def realLocatorCasConstructionResolvesNonnullGuardsOnActualShape(): Unit =
    val (snapshot,request,plan,context)=existing(Some(Some(0L)))
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    requireActualGuardBindings("physical_recovery_locator",m.reserveTarget.relation("physical_recovery_locator").rows.head,
      m.executeTarget.relation("physical_recovery_locator").rows.head,
      Vector("domain_version","locator_revision","locator_state","reason_code","current_attempt_uuid","current_footprint_uuid","last_mutation_operation_uuid"))

  @Test def realLocatorCasConstructionPreservesOldNullTypedPredicate(): Unit =
    val (snapshot,request,plan,context)=existing(Some(None))
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    val original=snapshot.relation("physical_recovery_locator").rows.head
    val command=requireActualGuardBindings("physical_recovery_locator",original,m.reserveTarget.relation("physical_recovery_locator").rows.head,
      Vector("domain_version","locator_revision","locator_state","reason_code","current_attempt_uuid","current_footprint_uuid","last_mutation_operation_uuid"))
    val column=command.shape.columns.indexWhere(_.name=="domain_version")
    assertTrue(command.values.exists { (index,cell) => index==column && cell==MySqlFormat3Cell.SqlNull })
    assertTrue(command.values.exists { (index,cell) => index==column && cell==integer(0) })

  @Test def realDomainOperationReservationCasResolveAllFixedGuardsAndParameters(): Unit =
    val (snapshot,request,plan,context)=existing()
    val m=MySqlInactiveDomainWorkflowMaterializer.materialize(request,plan,context,at.plusSeconds(1),snapshot,snapshot.allocationBudget)
    requireActualGuardBindings("floating_pod_state",m.reserveTarget.relation("floating_pod_state").rows.head,
      m.executeTarget.relation("floating_pod_state").rows.head,
      Vector("owner_uuid","state_version","is_active","last_mutation_operation_uuid"))
    val operationTable="fp_recovery_operation"
    def executeRow(image: MySqlFormat3FullSnapshot): Vector[MySqlFormat3Cell] =
      image.relation(operationTable).rows.find(row=>MySqlModeOperationEffectCodec.uuidEquals(
        MySqlControlledDebugRows.value(row,operationTable,"operation_uuid"),plan.executeOperationUuid)).get
    requireActualGuardBindings(operationTable,executeRow(m.reserveTarget),executeRow(m.executeTarget),
      Vector("operation_state","operation_fingerprint"))
    val reservationTable="fp_recovery_reservation"
    requireActualGuardBindings(reservationTable,m.reserveTarget.relation(reservationTable).rows.head,
      m.executeTarget.relation(reservationTable).rows.head,
      Vector("reservation_revision","reservation_state","workflow_uuid","operation_uuid","last_mutation_operation_uuid"))