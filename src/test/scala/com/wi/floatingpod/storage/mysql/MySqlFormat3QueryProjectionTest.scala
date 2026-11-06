package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.storage.*
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.util.Success

/** 直接调用生产 typed projection/dispatcher/borrow；不是 JDBC 或 admission 验收。 */
class MySqlFormat3QueryProjectionTest:
  private val schema = "fp_query_projection_test"
  private def id(n: Long): UUID = new UUID(0L,n)
  private def i(n: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(n)
  private def t(value: String): MySqlFormat3Cell = MySqlFormat3Cell.Text(value)
  private def f(value: Double): MySqlFormat3Cell = MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value))
  private def u(value: UUID): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()))
  private def binary(value: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value))
  private def row(table: String, values: (String,MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
    val supplied = values.toMap
    MySqlFormat3RelationShapes.forTable(table).columns.map { c => supplied.getOrElse(c.name,
      if c.nullable then MySqlFormat3Cell.SqlNull else c.kind match
        case MySqlFormat3SqlKind.Text => t("x")
        case MySqlFormat3SqlKind.Binary => binary(Array.fill[Byte](c.maximumLength.getOrElse(1))(1))
        case MySqlFormat3SqlKind.Integer => i(1)
        case MySqlFormat3SqlKind.Floating => f(1.25)
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026,10,9,0,0,0,123456000))) }
  private def pod(number: Long = 10L,owner: MySqlFormat3Cell = MySqlFormat3Cell.SqlNull,
      active: Long = 0L,dimension: MySqlFormat3Cell = i(0),yaw: Double = 1.25,updated: Long = 1234567890123L): Vector[MySqlFormat3Cell] =
    row("floating_pod_state","uuid"->u(id(number)),"owner_uuid"->owner,"binding_uuid"->u(id(2)),
      "state_version"->i(5),"world_name"->t("世界"),"pos_x"->f(1.25),"pos_y"->f(2.5),"pos_z"->f(3.75),
      "yaw"->f(yaw),"pitch"->f(-0.0),"is_active"->i(active),"last_updated"->i(updated),
      "dimension_id"->dimension,"operating_mode"->t("WORLD_IDLE"))
  private def snapshot(extra: Map[String,Vector[Vector[MySqlFormat3Cell]]] = Map.empty,
      state: String = "READY",retained: Long = 32_000_000L): MySqlFormat3FullSnapshot =
    val profile = SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,256L,retained,4_000_000L,2_000_000L)
    val base = Map(
      "fp_schema_manifest"->Vector(row("fp_schema_manifest","bootstrap_state"->t(state),"format_version"->i(3))),
      "physical_recovery_binding"->Vector(row("physical_recovery_binding","singleton_fixed1"->i(1),
        "binding_uuid"->u(id(2)),"cutover_state"->t("CERTIFIED"),"format_version"->i(2))))
    val all = base ++ extra
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map(shape => MySqlFormat3RelationSnapshot(shape.table,all.getOrElse(shape.table,Vector.empty)))
    val connection = MySqlFormat3ConnectionObservation(id(1),"9.7.2",schema,"pure@localhost","InnoDB",1,1L,
      "READ-COMMITTED","REPEATABLE-READ","STRICT_ALL_TABLES","+00:00","TLSv1.3","TLS_AES_256_GCM_SHA384")
    val scope = MySqlFormat3ObservationScope(id(3),1L,1L)
    val budget = new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    budget.initialize(schema,connection,scope,4_000_000L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema,connection,scope,4_000_000L,profile,relations,budget)
  private def projection(s: MySqlFormat3FullSnapshot): MySqlFormat3QueryProjection = MySqlFormat3QueryProjection.from(s,s.allocationBudget)
  private def changes(row: Vector[MySqlFormat3Cell],table: String,name: String,value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    row.updated(MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name==name),value)
  private def history(origin: String = "DEBUG_ITEM_CREATE",phase: String = "PREPARED",seal: String = "SEALED",
      current: Boolean = true): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val footprint = row("physical_recovery_footprint","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),
      "attempt_uuid"->u(id(20)),"footprint_uuid"->u(id(30)),"origin_version"->i(0),"target_world_name"->t("世界"),
      "target_pos_x"->f(1.25),"target_pos_y"->f(2.5),"target_pos_z"->f(3.75),"target_yaw"->f(1.25),"target_pitch"->f(-0.0),
      "target_dimension_id"->i(0),"center_chunk_x"->i(0),"center_chunk_z"->i(0),"seal_state"->t(seal))
    val cs = (for x <- -2 to 2; z <- -2 to 2 yield row("physical_recovery_footprint_chunk","binding_uuid"->u(id(2)),
      "pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),"footprint_uuid"->u(id(30)),"dimension_id"->i(0),"chunk_x"->i(x),"chunk_z"->i(z))).toVector
    var intent = row("physical_recovery_intent","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),
      "origin"->t(origin),"phase"->t(phase),"target_footprint_uuid"->u(id(30)),"expected_domain_version"->i(0),
      "create_operation_uuid"->(if origin=="DEBUG_ITEM_CREATE" then u(id(40)) else MySqlFormat3Cell.SqlNull))
    if origin=="UNBOUND_ADOPTION" then
      val identity = "raw source identity"
      intent = changes(intent,"physical_recovery_intent","source_kind",t("FRESH"))
      intent = changes(intent,"physical_recovery_intent","source_identity",t(identity))
      intent = changes(intent,"physical_recovery_intent","source_identity_sha256",binary(java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8))))
      intent = changes(intent,"physical_recovery_intent","catalog_generation",u(id(50)))
    val locator = row("physical_recovery_locator","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),
      "current_attempt_uuid"->u(id(20)),"current_footprint_uuid"->u(id(30)),"locator_state"->t(if origin=="DEBUG_ITEM_CREATE" then "DEBUG_ITEM_CREATE_PREPARED" else "UNBOUND_ADOPTION_PREPARED"),
      "reason_code"->t(if origin=="DEBUG_ITEM_CREATE" then "DEBUG_ITEM_CREATE_PREPARED" else "UNBOUND_ADOPTION_PREPARED"),"domain_version"->i(0),"target_dimension_id"->i(0))
    Map("floating_pod_state"->Vector(changes(pod(),"floating_pod_state","state_version",i(0))),"physical_recovery_footprint"->Vector(footprint),"physical_recovery_footprint_chunk"->(if seal=="DRAFT" then cs.take(3) else cs),
      "physical_recovery_intent"->Vector(intent),"physical_recovery_locator"->(if current then Vector(locator) else Vector.empty))

  @Test def missingUuidAndEntitlementRemainNoneWithoutInsert(): Unit =
    val q = projection(snapshot()); assertEquals(None,q.findByUuid(id(99))); assertEquals(None,q.findByPlayerUuid(id(99)))
  @Test def ownerNoneDiffersFromZeroUuid(): Unit =
    val q = projection(snapshot(Map("floating_pod_state"->Vector(pod(10),pod(11,u(id(0)))))))
    assertEquals(List(id(11)),q.findByOwner(id(0)).map(_.uuid)); assertEquals(None,q.findStoredByUuid(id(10)).get.ownerUuid)
  @Test def activeWorldIdleUnclaimedIsIncluded(): Unit =
    val q = projection(snapshot(Map("floating_pod_state"->Vector(pod(active=1)))))
    assertEquals(List(id(10)),q.findAllActive().map(_.uuid)); assertTrue(q.findAllActive().head.ownerUuid.isEmpty)
  @Test def rawActiveNullDimensionIsPreservedAndPublicMaterializationRejects(): Unit =
    val q = projection(snapshot(Map("floating_pod_state"->Vector(pod(active=1,dimension=MySqlFormat3Cell.SqlNull)))))
    assertEquals(None,q.findDesiredActiveRows().head.dimensionId)
    assertThrows(classOf[IllegalStateException],() => q.findAllActive())
  @Test def exactPoseNegativeZeroAndUnixMillisecondsRemain(): Unit =
    val q = projection(snapshot(Map("floating_pod_state"->Vector(pod())))); val actual=q.findStoredByUuid(id(10)).get
    assertEquals(1234567890123L,actual.lastUpdated); assertEquals(java.lang.Float.floatToRawIntBits(-0.0f),java.lang.Float.floatToRawIntBits(actual.pitch))
  @Test def inexactDomainFloatRejectedWithoutRounding(): Unit =
    assertThrows(classOf[IllegalStateException],() => projection(snapshot(Map("floating_pod_state"->Vector(pod(yaw=0.1))))))
  @Test def negativeTimeRejected(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(Map("floating_pod_state"->Vector(pod(updated = -1L))))))
  @Test def invalidActiveFlagRejected(): Unit =
    assertThrows(classOf[IllegalStateException],() => projection(snapshot(Map("floating_pod_state"->Vector(pod(active=2L))))))
  @Test def duplicateCurrentPodRejectedBeforeQuery(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => snapshot(Map("floating_pod_state"->Vector(pod(),pod()))))
  @Test def listsUseUnsignedBinaryUuidOrder(): Unit =
    val q=projection(snapshot(Map("floating_pod_state"->Vector(pod(10),pod(11)))))
    assertEquals(List(id(10),id(11)),q.findAllRecoverable().map(_.uuid)); assertEquals(q.findAll(),q.findAllRecoverable())
  @Test def zeroEntitlementLimitsAreRealRows(): Unit =
    val e=row("player_entitlement","player_uuid"->u(id(10)),"binding_uuid"->u(id(2)),"registration_limit"->i(0),"simultaneous_active_limit"->i(0))
    assertEquals(0,projection(snapshot(Map("player_entitlement"->Vector(e)))).findByPlayerUuid(id(10)).get.registrationLimit)
  @Test def bootstrappingRejectedBeforeProjection(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(state="BOOTSTRAPPING")))
  @Test def fixedVectorSharesOneImmutableCapturedGraph(): Unit =
    val s=snapshot(Map("floating_pod_state"->Vector(pod()))); val before=s.canonicalSha256Hex
    val replies=MySqlFormat3ReadDispatcher.execute(Vector(MySqlRepositoryOperation.FindByUuid(id(10)),MySqlRepositoryOperation.FindAll,MySqlRepositoryOperation.FindStoredByUuid(id(10))),projection(s))
    assertEquals(3,replies.size); assertEquals(before,s.canonicalSha256Hex); assertEquals(20,s.relations.size)
  @Test def mutationAndMixedVectorsRejectedBeforeProjection(): Unit =
    assertThrows(classOf[IllegalArgumentException],() => MySqlFormat3ReadDispatcher.requireReads(Vector(MySqlRepositoryOperation.FindAll,MySqlRepositoryOperation.Delete(id(10)))))
  @Test def allTwelveReadSelectorsAreClassifiedAndExecutable(): Unit =
    val s=snapshot(); val ops=Vector(MySqlRepositoryOperation.FindByUuid(id(10)),MySqlRepositoryOperation.FindByOwner(id(10)),MySqlRepositoryOperation.FindAllActive,MySqlRepositoryOperation.FindAll,MySqlRepositoryOperation.FindByPlayerUuid(id(10)),MySqlRepositoryOperation.FindDesiredActiveRows,MySqlRepositoryOperation.FindStoredByUuid(id(10)),MySqlRepositoryOperation.FindAllRecoverable,MySqlRepositoryOperation.FindPhysicalRecoveryBinding,MySqlRepositoryOperation.FindPhysicalRecoveryLocator(id(10)),MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators,MySqlRepositoryOperation.FindUnboundPreparation(id(10)))
    assertEquals(12,MySqlFormat3ReadDispatcher.execute(ops,projection(s)).size)
  @Test def completeDebugCreateReceiptRetainsNullSourceAndOperation(): Unit =
    val actual=projection(snapshot(history())).findUnboundPreparation(id(10)).get
    assertEquals(UnboundIntentOrigin.DebugItemCreate,actual.origin); assertEquals(None,actual.observedIdentity); assertEquals(Some(id(40)),actual.createOperationUuid)
  @Test def completeObservedAdoptionRetainsIdentityAndNullCreate(): Unit =
    val actual=projection(snapshot(history("UNBOUND_ADOPTION"))).findUnboundPreparation(id(10)).get
    assertEquals(UnboundIntentOrigin.ObservedArtifactAdoption,actual.origin); assertEquals("raw source identity",actual.observedIdentity.get.sourceIdentity); assertEquals(None,actual.createOperationUuid)
  @Test def partialDebugSourceTopologyFailsRatherThanNone(): Unit =
    val h=history(); val bad=changes(h("physical_recovery_intent").head,"physical_recovery_intent","catalog_generation",u(id(50)))
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(h.updated("physical_recovery_intent",Vector(bad)))))
  @Test def partialObservedSourceAndWrongDigestBothFail(): Unit =
    val h=history("UNBOUND_ADOPTION"); val original=h("physical_recovery_intent").head
    for (name,value) <- Vector("catalog_generation"->MySqlFormat3Cell.SqlNull,"source_identity_sha256"->binary(Array.fill[Byte](32)(7))) do
      assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(h.updated("physical_recovery_intent",Vector(changes(original,"physical_recovery_intent",name,value))))))
  @Test def draftHistoryPartialChunksRetainedWithoutCurrentAuthority(): Unit =
    val s=snapshot(history(seal="DRAFT",current=false)); val q=projection(s)
    assertEquals(3,s.relation("physical_recovery_footprint_chunk").rows.size); assertEquals(None,q.findPhysicalRecoveryLocator(id(10))); assertEquals(None,q.findUnboundPreparation(id(10)))
  @Test def currentDraftOrIncompleteFootprintRejected(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(history(seal="DRAFT"))))
    val h=history(); assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(h.updated("physical_recovery_footprint_chunk",h("physical_recovery_footprint_chunk").take(24)))))
  @Test def currentAttemptMismatchDoesNotSelectHistory(): Unit =
    val h=history(); val wrong=changes(h("physical_recovery_locator").head,"physical_recovery_locator","current_attempt_uuid",u(id(99)))
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(h.updated("physical_recovery_locator",Vector(wrong)))))
  @Test def oldAttemptHistoryIsPreservedButCurrentReceiptUsesExactAttempt(): Unit =
    val h=history(); val old=history(current=false)
    def older(table: String,row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
      var changed=changes(row,table,"attempt_uuid",u(id(19)))
      if table=="physical_recovery_intent" then
        changed=changes(changed,table,"target_footprint_uuid",u(id(29)))
        changed=changes(changed,table,"create_operation_uuid",u(id(39)))
      else changed=changes(changed,table,"footprint_uuid",u(id(29)))
      changed
    val merged=Vector("physical_recovery_footprint","physical_recovery_footprint_chunk","physical_recovery_intent").foldLeft(h) { (all,table) =>
      all.updated(table,old(table).map(r => older(table,r)) ++ h(table))
    }
    val s=snapshot(merged); val actual=projection(s).findUnboundPreparation(id(10)).get
    assertEquals(id(20),actual.attemptUuid); assertEquals(Some(id(40)),actual.createOperationUuid)
    assertEquals(2,s.relation("physical_recovery_intent").rows.size); assertEquals(50,s.relation("physical_recovery_footprint_chunk").rows.size)
  @Test def duplicateHistoryCreateIdentityIsNotDeduplicated(): Unit =
    val h=history(current=false); val original=h("physical_recovery_intent").head
    val other=changes(original,"physical_recovery_intent","attempt_uuid",u(id(21)))
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(h.updated("physical_recovery_intent",Vector(original,other)))))

  @Test def actualSharedBudgetChargesProjectionAndRejectsCapacity(): Unit =
    val s=snapshot(Map("floating_pod_state"->Vector(pod()))); val before=s.allocationBudget.accountedMemoryBytes
    projection(s).findAll(); assertTrue(s.allocationBudget.accountedMemoryBytes>before)
    val constrained=snapshot(Map("floating_pod_state"->Vector(pod())))
    val obligation=constrained.allocationBudget.newCanonicalScope()
    assertTrue(obligation.sharesAllocationLedger(constrained.allocationBudget))
    val pressure=constrained.limitProfile.maxRetainedBytes()-constrained.allocationBudget.accountedMemoryBytes-1024L
    obligation.reserveRetainedMemory(pressure,"test separate same-ledger retained obligation")
    try assertThrows(classOf[java.sql.SQLDataException],() => projection(constrained))
    finally obligation.closeScopedReadBudget()
  @Test def revokedBorrowRejectsBeforeBodyAndPreservesCause(): Unit =
    var calls=0; var failure=Option.empty[Throwable]
    val borrow=new MySqlOwnedRepositoryBorrow(Thread.currentThread(),1L,() => true,() => failure,cause => failure=Some(cause))
    borrow.revoke(); val actual=assertThrows(classOf[IllegalStateException],() => borrow.invoke { calls+=1;Success(1) })
    assertEquals(0,calls); assertSame(actual,failure.get); assertTrue(actual.getStackTrace.nonEmpty)
  @Test def wrongThreadBorrowRejectsBeforeBody(): Unit =
    var calls=0; val borrow=new MySqlOwnedRepositoryBorrow(new Thread(),1L,() => true,() => None,_ => ())
    assertThrows(classOf[IllegalStateException],() => borrow.invoke { calls+=1;Success(1) }); assertEquals(0,calls)
  @Test def unadmittedLifecycleRejectsBeforeBody(): Unit =
    val actor=new MySqlOwnedSessionLifecycle(Thread.currentThread()); actor.beginOpen()
    assertThrows(classOf[IllegalArgumentException],() => actor.requireAdmitted())
  @Test def productionVectorCaptureSeamInvokesFull20SourceExactlyOnce(): Unit =
    val s=snapshot(); var captures=0
    val source=new MySqlFormat3QuerySnapshotSource:
      override def readWithinQueryWorkflow(root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
        captures+=1; assertSame(s.allocationBudget,root); s
    val ops=Vector(MySqlRepositoryOperation.FindAll,MySqlRepositoryOperation.FindByUuid(id(99)),MySqlRepositoryOperation.FindByPlayerUuid(id(99)))
    val captured=MySqlFormat3ReadDispatcher.capture(ops,source,s.allocationBudget)
    val replies=MySqlFormat3ReadDispatcher.execute(ops,projection(captured))
    assertEquals(1,captures); assertEquals(3,replies.size); assertSame(s,captured)
  @Test def invalidVectorStopsAtCaptureBoundaryBeforeSourceBody(): Unit =
    val s=snapshot(); var captures=0
    val source=new MySqlFormat3QuerySnapshotSource:
      override def readWithinQueryWorkflow(root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
        captures+=1; s
    assertThrows(classOf[IllegalArgumentException],() => MySqlFormat3ReadDispatcher.capture(Vector(MySqlRepositoryOperation.Delete(id(10))),source,s.allocationBudget))
    assertEquals(0,captures)
  @Test def activatedHistoryKeepsCompleteCommittedFieldsAndPreparedReceipt(): Unit =
    var h=history(phase="ACTIVATED")
    var intent=h("physical_recovery_intent").head
    val values=Vector("activation_operation_uuid"->u(id(60)),"committed_domain_version"->i(1),"committed_at"->i(1234567890123L),
      "committed_world_name"->t("世界"),"committed_pos_x"->f(1.25),"committed_pos_y"->f(2.5),"committed_pos_z"->f(3.75),
      "committed_yaw"->f(1.25),"committed_pitch"->f(-0.0),"committed_dimension_id"->i(0))
    values.foreach { (name,value) => intent=changes(intent,"physical_recovery_intent",name,value) }
    var locator=h("physical_recovery_locator").head
    locator=changes(locator,"physical_recovery_locator","locator_state",t("ROW_ACTIVE"))
    locator=changes(locator,"physical_recovery_locator","reason_code",t("ROW_ACTIVE"))
    locator=changes(locator,"physical_recovery_locator","domain_version",i(1))
    h=h.updated("physical_recovery_intent",Vector(intent)).updated("physical_recovery_locator",Vector(locator))
      .updated("floating_pod_state",Vector(changes(pod(active=1),"floating_pod_state","state_version",i(1))))
    val actual=projection(snapshot(h)).findUnboundPreparation(id(10)).get
    assertEquals(0L,actual.preparedVersion); assertEquals(Some(id(40)),actual.createOperationUuid)
  @Test def partialActivatedFieldsRejectedAtTypedHistoryBoundary(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(history(phase="ACTIVATED"))))

  private def referencedDebugHistory(forestState: String = "SEALED",foreign: Boolean = false): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val original=history()
    val intent=changes(changes(original("physical_recovery_intent").head,"physical_recovery_intent","source_unit_uuid",u(id(60))),
      "physical_recovery_intent","source_forest_uuid",u(id(70)))
    val unit=row("physical_source_unit","binding_uuid"->u(id(2)),"unit_uuid"->u(id(60)))
    val forest=row("physical_source_forest","binding_uuid"->u(id(2)),"unit_uuid"->u(id(60)),
      "forest_uuid"->u(id(if foreign then 71 else 70)),"forest_state"->t(forestState))
    original ++ Map("physical_recovery_intent"->Vector(intent),"physical_source_unit"->Vector(unit),"physical_source_forest"->Vector(forest))
  @Test def debugOriginAllowsExactOptionalSealedSourcePair(): Unit =
    val q=projection(snapshot(referencedDebugHistory()))
    assertEquals(Some(id(40)),q.findUnboundPreparation(id(10)).get.createOperationUuid)
    assertEquals(None,q.findUnboundPreparation(id(10)).get.observedIdentity)
  @Test def intentSourcePairRejectsDraftAndForeignForest(): Unit =
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(referencedDebugHistory("DRAFT"))))
    assertThrows(classOf[java.sql.SQLDataException],() => projection(snapshot(referencedDebugHistory(foreign=true))))

  @Test def currentLocatorScratchReleasesWhileRawAndDtoRemainCharged(): Unit =
    var h=history(phase="ACTIVATED")
    var intent=h("physical_recovery_intent").head
    Vector("activation_operation_uuid"->u(id(60)),"committed_domain_version"->i(1),"committed_at"->i(1234567890123L),"committed_world_name"->t("世界"),"committed_dimension_id"->i(0),
      "committed_pos_x"->f(1.25),"committed_pos_y"->f(2.5),"committed_pos_z"->f(3.75),
      "committed_yaw"->f(1.25),"committed_pitch"->f(-0.0))
      .foreach { (name,value) => intent=changes(intent,"physical_recovery_intent",name,value) }
    var locator=changes(h("physical_recovery_locator").head,"physical_recovery_locator","locator_state",t("ROW_ACTIVE"))
    locator=changes(locator,"physical_recovery_locator","domain_version",i(1))
    val actualPod=changes(pod(active=1),"floating_pod_state","state_version",i(1))
    h=h.updated("physical_recovery_intent",Vector(intent)).updated("physical_recovery_locator",Vector(locator)).updated("floating_pod_state",Vector(actualPod))
    val s=snapshot(h);val before=s.allocationBudget.accountedMemoryBytes
    val q=projection(s);val retained=s.allocationBudget.accountedMemoryBytes
    assertTrue(retained>before)
    val actualLocator=q.findPhysicalRecoveryLocator(id(10)).get
    assertEquals(25,actualLocator.chunks.size)
    val afterGetter=s.allocationBudget.accountedMemoryBytes
    assertTrue(afterGetter>retained)
    MySqlFormat3QueryProjection.requireCurrentChunks(s,s.allocationBudget,actualLocator.chunks,0,1.25,3.75)
    assertEquals(afterGetter,s.allocationBudget.accountedMemoryBytes)
    assertTrue(s.allocationBudget.accountedMemoryBytes>before)

  @Test def currentChunkActualGateRejectsSameLedgerPressureBeforeScratch(): Unit =
    val s=snapshot();val root=s.allocationBudget
    val chunks=MySqlRepositoryTransitionRules.chunksAt(0,1.25,3.75).toList
    val obligation=root.newCanonicalScope()
    obligation.reserveRetainedMemory(root.availableMemoryBytes-1024L,"test other same-ledger obligation")
    val before=root.accountedMemoryBytes
    try
      assertThrows(classOf[java.sql.SQLDataException],() => MySqlFormat3QueryProjection.requireCurrentChunks(s,root,chunks,0,1.25,3.75))
      assertEquals(before,root.accountedMemoryBytes)
    finally obligation.closeScopedReadBudget()
  @Test def currentChunkActualGateReleasesScratchOnSuccessAndFailure(): Unit =
    val s=snapshot();val root=s.allocationBudget
    val chunks=MySqlRepositoryTransitionRules.chunksAt(0,1.25,3.75).toList
    val before=root.accountedMemoryBytes
    MySqlFormat3QueryProjection.requireCurrentChunks(s,root,chunks,0,1.25,3.75)
    assertEquals(before,root.accountedMemoryBytes)
    val failure=assertThrows(classOf[java.sql.SQLDataException],() => MySqlFormat3QueryProjection.requireCurrentChunks(s,root,chunks.take(24),0,1.25,3.75))
    assertTrue(failure.getStackTrace.nonEmpty);assertEquals(before,root.accountedMemoryBytes)
