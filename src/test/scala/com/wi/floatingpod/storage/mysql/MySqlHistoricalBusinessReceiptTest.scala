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
class MySqlHistoricalBusinessReceiptTest:
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
  private def data(state: String="READY", profile: SourceLimitProfile =
      SourceLimitProfile.of(536870912L,16777216L,134217728L,64L,256L,536870912L,67108864L,536870912L)): Data =
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
    val plan=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlOwnedRepositoryOperationPlan.single(tag,id(14),id(15),request.repositoryOperation,profile)
    }
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

  private def raw(snapshot: MySqlFormat3FullSnapshot, operation: UUID): Vector[MySqlFormat3Cell] =
    snapshot.relation("fp_recovery_operation").rows.find(r =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(r, "fp_recovery_operation", "operation_uuid"), operation)).get

  private def checkPhases(before: MySqlFormat3FullSnapshot, reserved: MySqlFormat3FullSnapshot,
      executed: MySqlFormat3FullSnapshot, plan: MySqlOwnedRepositoryOperationPlan): Unit =
    val root = before.allocationBudget
    Vector((reserved, plan.reserveOperationUuid, before), (executed, plan.executeOperationUuid, reserved)).foreach { (actual, id, expected) =>
      val result = MySqlHistoricalBusinessReceipt.verify(raw(actual, id), actual, actual, root)
      assertEquals(expected.relations, result.before)
      assertEquals(actual.relations, result.target)
      val writer = actual.relation("fp_runtime_writer").rows.head
      assertTrue(MySqlModeOperationEffectCodec.value(writer, "fp_runtime_writer", "last_commit_fingerprint") match
        case MySqlFormat3Cell.Binary(b) => b.equalsVector(result.receiptFingerprint)
        case _ => false)
      val history = MySqlNormalRecallRecovery.verifyReceiptHistoryData(actual, before, root)
      assertEquals(if id == plan.reserveOperationUuid then 1 else 2, history.newestFirst.size)
      assertTrue(history.newestFirst.forall(_.recall.isEmpty))
    }

  @Test def actualDebugPrepareAndActivationReceiptsRoundtripCompleteGraphs(): Unit =
    val d = data(); val root = d.snapshot.allocationBudget
    try
      val first = build(d)
      checkPhases(d.snapshot, first.reserveTarget, first.executeTarget, d.plan)
      val operation = MySqlRepositoryOperation.CommitControlledDebugCreate(d.request.expectedBinding, d.request.podUuid,
        d.request.attemptUuid, d.request.footprintUuid, 0L, d.request.targetPose, d.request.createOperationUuid, 256)
      val tag = IoOperationTag(id(17), 5L, 9L, id(216))
      val plan = MySqlOwnedRepositoryOperationPlan.single(tag, d.plan.workflowUuid, id(215), operation, root.profile)
      val context = contextFor(plan, root)
      val activated = MySqlControlledDebugWorkflowMaterializer.materialize(d.request, plan, operation, context,
        at.plusSeconds(1), first.executeTarget, true, 256, root)
      checkPhases(first.executeTarget, activated.reserveTarget, activated.executeTarget, plan)
      assertEquals(4, MySqlNormalRecallRecovery.verifyReceiptHistoryData(activated.executeTarget, d.snapshot, root).newestFirst.size)
    finally root.closeRootReadBudget()

  @Test def actualInactiveReceiptKeepsUnselectedFullRows(): Unit =
    val (initial, request, plan, context) = existing()
    val root = initial.allocationBudget
    try
      val unrelated = row("player_entitlement", "player_uuid" -> u(900), "binding_uuid" -> u(10),
        "registration_limit" -> integer(256), "simultaneous_active_limit" -> integer(256), "created_operation_uuid" -> u(16))
      val before = MySqlControlledDebugImages.put(initial, "player_entitlement", unrelated, root, false)
      val m = MySqlInactiveDomainWorkflowMaterializer.materialize(request, plan, context, at.plusSeconds(2), before, root)
      checkPhases(before, m.reserveTarget, m.executeTarget, plan)
      val result = MySqlHistoricalBusinessReceipt.verify(raw(m.executeTarget, plan.executeOperationUuid), m.executeTarget, m.executeTarget, root)
      assertEquals(before.relation("player_entitlement"), result.before.find(_.table == "player_entitlement").get)
      assertNotEquals(result.context.canonicalFingerprintSha256, result.receiptFingerprint)
    finally root.closeRootReadBudget()

  private def contextFor(plan: MySqlOwnedRepositoryOperationPlan, root: MySqlFormat3ReadBudget): MySqlRepositoryOperationContextIdentity =
    new MySqlRepositoryOperationContextIdentity(plan.tag, plan.workflowUuid, plan.reserveOperationUuid, id(19), 123L, "fpb1:" + ("A" * 43),
      id(10), 3L, at.plusHours(1), id(18), "9.7.2", schema, "pure@localhost", id(20), 3, 2, "a" * 64, "b" * 64,
      Vector.fill(32)(1.toByte), "FRESH_PLAN", MessageDigest.getInstance("SHA-256").digest(
        "controlled-materializer-pure-data-v1".getBytes(StandardCharsets.UTF_8)).toVector,
      root.profile.sha256().toVector, "RUN", plan.requestIdentitySha256, root.profile)

  @Test def controlledRowsCopyOnlyUnownedBackingAndPreserveForeignStorage(): Unit =
    val d = data()
    val root = d.snapshot.allocationBudget
    val foreign = new MySqlFormat3ReadBudget(root.profile, root.profile.maxSqlPacketBytes())
    var primary: Throwable = null
    try
      val table = "physical_recovery_binding"
      val shape = MySqlFormat3RelationShapes.forTable(table)
      val index = shape.columns.indexWhere(_.name == "binding_uuid")
      def binaryCell(cell: MySqlFormat3Cell): MySqlFormat3Cell.Binary = cell match
        case value: MySqlFormat3Cell.Binary => value
        case _ => throw new AssertionError("actual binding UUID must remain a binary cell")
      val source = binaryCell(u(10))
      val template = d.snapshot.relation(table).rows.head.updated(index, source)
      val supplied = shape.columns.map(_.name).zip(template).toMap
      val rowCharge = 256L + shape.columns.size.toLong * 96L
      val copyCharge = source.value.length.toLong + 64L
      val beforeCopy = root.accountedMemoryBytes
      val copied = MySqlControlledDebugRows.row(table, supplied, root)
      val copiedCell = binaryCell(copied(index))
      assertNotSame(source, copiedCell)
      assertEquals(source, copiedCell)
      assertNull(source.value.storageBudget)
      assertSame(root, copiedCell.value.storageBudget)
      assertEquals(beforeCopy + rowCharge + copyCharge, root.accountedMemoryBytes)
      val foreignCell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
        source.value.copyBytes(foreign, "foreign immutable producer fixture"), foreign))
      val foreignCharge = foreign.accountedMemoryBytes
      val ownedSupplied = supplied.updated("binding_uuid", foreignCell)
      val beforeOwned = root.accountedMemoryBytes
      val retained = MySqlControlledDebugRows.row(table, ownedSupplied, root)
      assertSame(foreignCell, retained(index))
      assertSame(foreign, binaryCell(foreignCell).value.storageBudget)
      assertEquals(beforeOwned + rowCharge, root.accountedMemoryBytes)
      assertEquals(foreignCharge, foreign.accountedMemoryBytes)
      val beforeUpdate = root.accountedMemoryBytes
      val updated = MySqlControlledDebugRows.update(retained, table, Map("binding_uuid" -> source), root)
      assertEquals(source, updated(index))
      assertNotSame(source, updated(index))
      assertSame(root, binaryCell(updated(index)).value.storageBudget)
      assertNull(source.value.storageBudget)
      assertEquals(beforeUpdate + rowCharge + copyCharge, root.accountedMemoryBytes)
      val beforeOwnedUpdate = root.accountedMemoryBytes
      val kept = MySqlControlledDebugRows.update(retained, table, Map("cutover_state" -> text("CERTIFIED")), root)
      assertSame(foreignCell, kept(index))
      assertSame(foreign, binaryCell(foreignCell).value.storageBudget)
      assertEquals(beforeOwnedUpdate + rowCharge, root.accountedMemoryBytes)
      assertEquals(foreignCharge, foreign.accountedMemoryBytes)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(
      () => root.closeRootReadBudget(), () => foreign.closeRootReadBudget()))

  @Test def modeUsesActualEffectReceiptAndOriginalClosure(): Unit =
    checkMixedMode(data())

  @Test def modeThenRecallKeepsBothDomainsInOneCompleteHistory(): Unit =
    checkMixedMode(data(), recall = true)

  @Test def completeMixedModeFitsEightMegabyteRoot(): Unit =
    val handoff = produceMixedMode(data(profile = SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,100_000L,
      8_000_000L,4_000_000L,2_000_000L)), cold = true).get
    checkColdMode(handoff)

  private def checkMixedMode(d: Data, recall: Boolean = false): Unit =
    assertTrue(produceMixedMode(d, recall = recall).isEmpty)

  private def produceMixedMode(d: Data, cold: Boolean = false, recall: Boolean = false): Option[ColdModeHandoff] =
    val root = d.snapshot.allocationBudget
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), d.plan.tag.bootId,
      d.plan.tag.bootGeneration, d.plan.tag.connectionGeneration)
    var retainedTag: Option[IoOperationTag] = None
    var primary: Throwable = null
    def construct(): Option[ColdModeHandoff] =
      val prepared = build(d)
      val activate = MySqlRepositoryOperation.CommitControlledDebugCreate(d.request.expectedBinding, d.request.podUuid,
        d.request.attemptUuid, d.request.footprintUuid, 0L, d.request.targetPose, d.request.createOperationUuid, 256)
      val activationPlan = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlOwnedRepositoryOperationPlan.single(IoOperationTag(id(17), 5L, 9L, id(216)), id(14), id(215), activate, root.profile)
      }
      val active = MySqlControlledDebugWorkflowMaterializer.materialize(d.request, activationPlan, activate, contextFor(activationPlan, root),
        at.plusSeconds(1), prepared.executeTarget, true, 256, root)
      // Mode的有限初始DATA fixture：明确给owned active base；不声称此赋owner来自SQL业务回执。
      val domain = MySqlControlledDebugRows.update(active.executeTarget.relation("floating_pod_state").rows.head, "floating_pod_state",
        Map("owner_uuid" -> u(50)), root)
      var before = MySqlControlledDebugImages.put(active.executeTarget, "floating_pod_state", domain, root, true)
      val entitlement = row("player_entitlement", "player_uuid" -> u(50), "binding_uuid" -> u(10),
        "registration_limit" -> integer(256), "simultaneous_active_limit" -> integer(256), "created_operation_uuid" -> u(16))
      before = MySqlControlledDebugImages.put(before, "player_entitlement", entitlement, root, false)
      val operation = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(d.request.expectedBinding, id(11), id(50), 1L,
        d.request.attemptUuid, com.wi.floatingpod.domain.PodOperatingMode.Normal, pose)
      val plan = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlOwnedRepositoryOperationPlan.single(IoOperationTag(id(17), 5L, 9L, id(316)), id(314), id(315), operation, root.profile)
      }
      val locator = MySqlModeOperationEffectCodec.value(before.relation("physical_recovery_locator").rows.head,
        "physical_recovery_locator", "locator_revision").asInstanceOf[MySqlFormat3Cell.SignedInteger].value
      val context = contextFor(plan, root)
      val draft = new MySqlModeOriginalWitnessDraft(plan, operation, context, root)
      registry.retainRoot(plan.tag, root)
      retainedTag = Some(plan.tag)
      registry.retainWitnessDraft(plan.tag, draft)
      def reportSizes(): Unit =
        println(s"MODE_DIAGNOSTIC packet=${root.profile.maxSqlPacketBytes()} retained=${root.profile.maxRetainedBytes()} accounted=${root.accountedMemoryBytes}")
        Vector(MySqlModeWitnessProducerStage.ReserveTarget, MySqlModeWitnessProducerStage.ExecuteTarget).foreach { stage =>
          val image = draft.producerSnapshot(stage)
          if image != null then image.relation("fp_recovery_operation").rows.foreach { row =>
            var count = 0L
            val sink = new java.io.OutputStream:
              override def write(value: Int): Unit = count = Math.addExact(count, 1L)
              override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = count = Math.addExact(count, length.toLong)
            val output = new java.io.DataOutputStream(sink)
            MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, row, MySqlFormat3RelationShapes.forTable("fp_recovery_operation"))
            output.flush()
            val operation = MySqlModeOperationEffectCodec.rowOperationUuid(row)
            val fields = MySqlFormat3RelationShapes.forTable("fp_recovery_operation").columns.zip(row).collect {
              case (column, MySqlFormat3Cell.Binary(bytes)) if column.name.endsWith("_bytes") => s"${column.name}=${bytes.length}"
            }.mkString(",")
            println(s"MODE_DIAGNOSTIC stage=$stage operation=$operation rawBytes=$count $fields")
          }
        }
      val m = try MySqlModeWorkflowMaterializer.materialize(plan, operation, context, at.plusSeconds(2), before, locator, draft)
      catch
        case cause: Throwable => reportSizes(); throw cause
      reportSizes()
      if cold then Some(writeColdMode(before, m.reserveTarget, m.executeTarget, plan))
      else
        checkPhases(before, m.reserveTarget, m.executeTarget, plan)
        val result = MySqlHistoricalBusinessReceipt.verify(raw(m.executeTarget, plan.executeOperationUuid), m.executeTarget, m.executeTarget, root)
        assertNotEquals(result.context.canonicalFingerprintSha256, result.receiptFingerprint)
        if recall then
          val current = m.executeTarget.relation("floating_pod_state").rows.head
          def number(column: String): Long = MySqlModeOperationEffectCodec.value(current,
            "floating_pod_state", column).asInstanceOf[MySqlFormat3Cell.SignedInteger].value
          val state = com.wi.floatingpod.domain.FloatingPodState(id(11), Some(id(50)), number("state_version"),
            pose.worldName, pose.posX, pose.posY, pose.posZ, pose.yaw, pose.pitch, true,
            number("last_updated"), Some(pose.dimensionId), com.wi.floatingpod.domain.PodOperatingMode.Normal)
          val nativeId = "FP_WIN32_FILE_ID_V1;volume=0123456789abcdef;id=0123456789abcdef0123456789abcdef"
          val request = MySqlNormalRecallRequest(id(410), d.request.expectedBinding, state, pose, 1L,
            d.request.attemptUuid, d.request.footprintUuid,
            (for x <- -2 to 2; z <- -2 to 2 yield com.wi.floatingpod.storage.PhysicalRecoveryChunk(0, x, z)).toVector,
            MySqlNormalRecallSourceIdentity(nativeId, nativeId))
          val recallPlan = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
            MySqlOwnedRepositoryOperationPlan.single(IoOperationTag(id(17), 5L, 9L, id(416)),
              id(414), id(415), MySqlRepositoryOperation.PrepareNormalRecall(request), root.profile)
          }
          val prepared = MySqlNormalRecallWorkflowMaterializer.prepareInitial(request, recallPlan,
            contextFor(recallPlan, root), at.plusSeconds(3), m.executeTarget, root)
          val history = MySqlNormalRecallRecovery.verifyReceiptHistoryData(prepared.executeTarget, before, root)
          assertEquals(Vector(recallPlan.executeOperationUuid, recallPlan.reserveOperationUuid,
            plan.executeOperationUuid, plan.reserveOperationUuid), history.newestFirst.map(_.context.operationUuid))
          assertEquals(Vector(true, true, false, false), history.newestFirst.map(_.recall.nonEmpty))
        None
    try construct()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => retainedTag match
        case Some(tag) => registry.release(tag)
        case None => root.closeRootReadBudget()))

  @Test def historicalPayloadCannotPassWithOnlyItsDigestRecomputed(): Unit =
    Vector("request_identity_bytes" -> "request_identity_sha256", "scope_bytes" -> "scope_sha256",
      "commit_witness_bytes" -> "commit_witness_sha256").foreach { (payloadColumn, digestColumn) =>
      val d = data(); val root = d.snapshot.allocationBudget
      try
        val m = build(d)
        val original = raw(m.executeTarget, d.plan.executeOperationUuid)
        val bytes = MySqlModeOperationEffectCodec.value(original, "fp_recovery_operation", payloadColumn)
          .asInstanceOf[MySqlFormat3Cell.Binary].value.copyBytes(root, "negative actual historical payload")
        // 改实际domain字节并同步正确digest；RAW结构校验成功仍不能代替原业务语义。
        bytes(4) = (bytes(4) ^ 1).toByte
        val altered = MySqlControlledDebugRows.update(original, "fp_recovery_operation", Map(
          payloadColumn -> MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes)),
          digestColumn -> MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MessageDigest.getInstance("SHA-256").digest(bytes)))), root)
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(altered, root.profile, root)
        val inventory = MySqlControlledDebugImages.put(m.executeTarget, "fp_recovery_operation", altered, root, true)
        assertThrows(classOf[Exception], () => MySqlHistoricalBusinessReceipt.verify(altered, inventory, inventory, root))
      finally root.closeRootReadBudget()
    }

  /** 三次producer的留存不伪装cold reader生命周期。完整DATA文件交接后，两个receipt共用一个8MB root。 */
  private final case class ColdModeHandoff(directory: java.nio.file.Path, hashes: Vector[Vector[Byte]],
      profile: SourceLimitProfile, reserveOperationUuid: UUID, executeOperationUuid: UUID, tag: IoOperationTag)

  private def writeColdMode(before: MySqlFormat3FullSnapshot, reserved: MySqlFormat3FullSnapshot,
      executed: MySqlFormat3FullSnapshot, plan: MySqlOwnedRepositoryOperationPlan): ColdModeHandoff =
    val producerRoot = before.allocationBudget
    val profile = producerRoot.profile
    val directory = java.nio.file.Files.createTempDirectory(java.nio.file.Path.of("build"), "mode-eight-data-")
    val hashes = Vector(before, reserved, executed).zipWithIndex.map { (snapshot, index) =>
      val path = directory.resolve(s"image$index.bin")
      producerRoot.reserveTemporaryMemory(9216L, "complete DATA handoff stream")
      var temporaryPrimary: Throwable = null
      try
        val digest = MessageDigest.getInstance("SHA-256")
        val rawOutput = java.nio.file.Files.newOutputStream(path, java.nio.file.StandardOpenOption.CREATE_NEW)
        var output: java.io.DataOutputStream = null
        var streamPrimary: Throwable = null
        try
          output = new java.io.DataOutputStream(new java.io.BufferedOutputStream(
            new java.security.DigestOutputStream(rawOutput, digest), 8192))
          MySqlFormat3SnapshotCanonical.writeSnapshot(snapshot, output, producerRoot)
        catch
          case failure: Throwable =>
            streamPrimary = failure
            throw failure
        finally MySqlFormat3BudgetCleanup.run(streamPrimary, Vector(
          () => if output != null then output.close() else rawOutput.close()))
        assertTrue(java.nio.file.Files.size(path) <= profile.maxNativeArtifactBytes())
        digest.digest().toVector
      catch
        case failure: Throwable =>
          temporaryPrimary = failure
          throw failure
      finally MySqlFormat3BudgetCleanup.run(temporaryPrimary, Vector(
        () => producerRoot.releaseTemporaryMemory(9216L, "complete DATA handoff stream dropped")))
    }
    ColdModeHandoff(directory, hashes, profile, plan.reserveOperationUuid, plan.executeOperationUuid, plan.tag)

  private def checkColdMode(handoff: ColdModeHandoff): Unit =
    val directory = handoff.directory
    val hashes = handoff.hashes
    val profile = handoff.profile
    val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), handoff.tag.bootId,
      handoff.tag.bootGeneration, handoff.tag.connectionGeneration)
    registry.retainRoot(handoff.tag, root)
    var primary: Throwable = null
    def read(): Unit =
      val path = directory.resolve("image2.bin")
      root.reserveRetainedMemory(java.nio.file.Files.size(path) + 64L, "actual complete cold input")
      val payload = new MySqlFormat3CanonicalPayload(MySqlFormat3Binary.takeOwned(java.nio.file.Files.readAllBytes(path)), root, root)
      val decoded = MySqlFormat3SnapshotCodec.decode(payload, profile, root)
      def image(relations: Vector[MySqlFormat3RelationSnapshot]): MySqlFormat3FullSnapshot =
        root.reserveRetainedMemory(512L, "cold full image wrapper")
        new MySqlFormat3FullSnapshot(decoded.schema, decoded.connection, decoded.observationScope,
          decoded.maxAllowedPacketBytes, profile, relations, root)
      val actual = image(decoded.relations)
      assertEquals(hashes(2), actual.canonicalSha256)
      val e = MySqlHistoricalBusinessReceipt.verify(raw(actual, handoff.executeOperationUuid), actual, actual, root)
      val reserveImage = image(e.before)
      assertEquals(hashes(1), reserveImage.canonicalSha256)
      val r = MySqlHistoricalBusinessReceipt.verify(raw(actual, handoff.reserveOperationUuid), actual, reserveImage, root)
      assertEquals(hashes(0), image(r.before).canonicalSha256)
      assertTrue(root.successfulReserveHighWaterBytes <= 8_000_000L)
      println(s"MODE_EIGHT_COLD directory=$directory highWater=${root.successfulReserveHighWaterBytes} retained=${root.accountedMemoryBytes}")
    try read()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(() => registry.release(handoff.tag)))

  @Test def minimalModeFixtureCannotReplaceCompleteHistoricalGraph(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val f = images.fixture
    val root = images.before.allocationBudget
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), f.plan.tag.bootId,
      f.plan.tag.bootGeneration, f.plan.tag.connectionGeneration)
    registry.retainRoot(f.plan.tag, root)
    var primary: Throwable = null
    try
      val draft = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
      registry.retainWitnessDraft(f.plan.tag, draft)
      val m = MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
        LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000), images.before, 0L, draft)
      // 此旧fixture只覆盖Mode局部规则，没有current locator intent；完整history不得接纳它。
      val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlHistoricalBusinessReceipt.verify(raw(m.reserveTarget, f.plan.reserveOperationUuid),
          m.reserveTarget, m.reserveTarget, root))
      assertTrue(failure.getMessage.contains("current locator intent"), failure.getMessage)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(() => registry.release(f.plan.tag)))

