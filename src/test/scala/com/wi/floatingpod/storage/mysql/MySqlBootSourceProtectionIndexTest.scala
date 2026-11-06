package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.source.{SourceForestCodec, SourceForestSharedBudgetFixtures}
import com.wi.floatingpod.server.io.IoOperationTag
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.security.MessageDigest
import java.time.LocalDateTime
import scala.jdk.CollectionConverters.*

class MySqlBootSourceProtectionIndexTest:
  import MainBootProjectionFixture.*
  import MySqlBootSourceProtectionIndex.*
  private def sha(bytes: Array[Byte]) = MessageDigest.getInstance("SHA-256").digest(bytes)
  private def op(state: String = "COMMITTED"): Vector[MySqlFormat3Cell] =
    val payload = Array[Byte](1,2,3)
    val context=MySqlOperationContext(id(3),1L,id(2),id(1004),
      Vector(MySqlExpectedEffect("fixture-source-capture",0L,Vector.fill(32)(1.toByte))))
    val source=snapshot()
    val contextBytes=try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(source.allocationBudget) {
        val encoded=MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1",source.limitProfile)(out=>
          MySqlModeOperationEffectCodec.writeOperationContext(out,context))
        encoded.binaryCell match
          case MySqlFormat3Cell.Binary(value)=>value.copyBytes(source.allocationBudget,"test encoded original context")
          case _=>throw new AssertionError("encoded context must be binary")
      }
    finally source.allocationBudget.closeRootReadBudget()
    val pairs = Vector("request_identity","scope","preimage","target").flatMap(n =>
      Vector(n+"_bytes" -> binary(payload),n+"_sha256" -> binary(sha(payload))))
    row("fp_recovery_operation",(pairs ++ Vector(
      "binding_uuid"->u(id(2)),"operation_uuid"->u(id(1004)),"workflow_uuid"->u(id(2000)),
      "boot_uuid"->u(id(3)),"fence_epoch"->i(1),"operation_fingerprint"->binary(context.canonicalFingerprintSha256.toArray),
      "original_context_bytes"->binary(contextBytes),"original_context_sha256"->binary(sha(contextBytes)),
      "step"->t("SOURCE_CAPTURE"),"operation_state"->t(state),"context_encoding"->t("FP_RECOVERY_OPERATION_BE_V1"),
      "result_at"->(if state == "RESERVED" then MySqlFormat3Cell.SqlNull else MySqlFormat3Cell.DateTime(LocalDateTime.of(2026,10,9,0,0))),
      "commit_witness_bytes"->(if state == "RESERVED" then MySqlFormat3Cell.SqlNull else binary(payload)),
      "commit_witness_sha256"->(if state == "RESERVED" then MySqlFormat3Cell.SqlNull else binary(sha(payload)))))*)
  private def sourceRows(): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val forest = SourceForestSharedBudgetFixtures.forest(2,false,false,false)
    val profileSource = snapshot()
    val data = SourceForestCodec.encode(forest,profileSource.limitProfile)
    profileSource.allocationBudget.closeRootReadBudget()
    val identity = forest.identity()
    val unit = row("physical_source_unit","binding_uuid"->u(id(2)),"unit_uuid"->u(identity.unitId()),
      "world_identity"->t(identity.worldIdentity()),"save_identity"->t(identity.saveIdentity()),"loader_identity"->t(identity.loaderProfile()),
      "catalog_generation"->u(identity.catalogGeneration()),"read_generation"->i(identity.readGeneration()),
      "source_observation_bytes"->binary(forest.sourceObservation().copy()),"source_observation_sha256"->binary(sha(forest.sourceObservation().copy())),
      "current_epoch"->i(identity.sourceEpoch()),"current_forest_uuid"->u(identity.forestId()),"unit_state"->t("SEALED"),
      "created_operation_uuid"->u(identity.createdOperationId()),"last_mutation_operation_uuid"->u(identity.createdOperationId()))
    val r = row("physical_source_forest","binding_uuid"->u(id(2)),"unit_uuid"->u(identity.unitId()),"forest_uuid"->u(identity.forestId()),
      "source_epoch"->i(identity.sourceEpoch()),"forest_state"->t("SEALED"),"forest_codec"->t(SourceForestCodec.DOMAIN),
      "forest_bytes"->binary(data.bytes().copy()),"forest_sha256"->binary(data.sha256().copy()),
      "member_count"->i(data.memberCount()),"root_count"->i(data.rootCount()),"carrier_count"->i(data.carrierCount()),
      "retained_bytes"->i(data.retainedBytes()),"created_operation_uuid"->u(identity.createdOperationId()))
    Map("fp_recovery_operation"->Vector(op()),"physical_source_unit"->Vector(unit),"physical_source_forest"->Vector(r))
  private def hold(base: Map[String,Vector[Vector[MySqlFormat3Cell]]],purpose: String = "CURRENT_HOLD",
      state: String = "OPEN"): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val s = snapshot()
    val f = SourceForestSharedBudgetFixtures.forest(2,false,false,false)
    val carrier = f.carriers().getFirst()
    val key = MySqlRecoveryResourceKey.carrier(f.identity().saveIdentity(),f.identity().loaderProfile(),carrier.regionKey(),carrier.chunkX(),carrier.chunkZ(),s.limitProfile)
    val kb = key.copyCanonicalBytes(s.allocationBudget,"test full carrier key")
    val reservation = row("fp_recovery_reservation","binding_uuid"->u(id(2)),"resource_kind"->t("CARRIER"),
      "resource_key_hash"->binary(sha(kb)),"resource_key"->binary(kb),"workflow_uuid"->u(id(2000)),"operation_uuid"->u(id(1004)),
      "reservation_state"->t(if state == "OPEN" then "HELD" else "RELEASED"),"last_mutation_operation_uuid"->u(id(1004)))
    val proof = Array[Byte](9,8,7)
    val obligation = row("physical_source_obligation","binding_uuid"->u(id(2)),"unit_uuid"->u(f.identity().unitId()),"forest_uuid"->u(f.identity().forestId()),
      "resource_kind"->t("CARRIER"),"carrier_key_hash"->binary(sha(kb)),"carrier_key"->binary(kb),"purpose"->t(purpose),
      "obligation_state"->t(state),"created_operation_uuid"->u(id(1004)),"last_mutation_operation_uuid"->u(id(1004)),
      "discharged_operation_uuid"->(if state == "OPEN" then MySqlFormat3Cell.SqlNull else u(id(1004))),
      "discharge_kind"->(if state == "OPEN" then MySqlFormat3Cell.SqlNull else t("CUTOVER")),
      "discharge_proof_bytes"->(if state == "OPEN" then MySqlFormat3Cell.SqlNull else binary(proof)),
      "discharge_proof_sha256"->(if state == "OPEN" then MySqlFormat3Cell.SqlNull else binary(sha(proof))))
    s.allocationBudget.closeRootReadBudget()
    base ++ Map("fp_recovery_reservation"->Vector(reservation),"physical_source_obligation"->Vector(obligation))
  private def index(extra: Map[String,Vector[Vector[MySqlFormat3Cell]]] = Map.empty): MySqlBootSourceProtectionIndex =
    val s = snapshot(extra)
    MySqlBootSourceProtectionIndex.from(s,s.allocationBudget)
  private def completeHistory(): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val h = history(origin="OWNED_RELEASE",current=false)
    h.map { (table,rs) => table -> rs.map { original =>
      MySqlFormat3RelationShapes.forTable(table).columns.zipWithIndex.foldLeft(original) { case (r,(column,n)) =>
        if column.name.endsWith("operation_uuid") && r(n) != MySqlFormat3Cell.SqlNull then r.updated(n,u(id(1004))) else r
      }
    }}.map { (table,rs) => table -> (if table == "physical_recovery_intent" then rs.map(r => changes(r,table,"expected_owner_uuid",u(id(5)))) else rs) } ++
      Map("fp_recovery_operation"->Vector(op()))

  @Test def completeEmptyResponsibilityImageClassifiesUnprotected(): Unit =
    val result = index()
    assertEquals(Unprotected,result.classify(0,0,0))
    assertTrue(result.unresolved.isEmpty)

  @Test def fullForestDecodePreservesIdentitiesAndLeavesUnrelatedChunksUnprotected(): Unit =
    val result = index(sourceRows())
    assertTrue(result.unresolved.isEmpty,result.unresolved.toString)
    assertEquals(2,result.carriers.size)
    assertEquals("save",result.carriers.head.saveIdentity)
    assertEquals("cleanroom/scalar",result.carriers.head.loaderIdentity)
    assertEquals(Unknown(Vector("SOURCE_CARRIER_RELEASE_NOT_ESTABLISHED"),Vector.empty),result.classify(0,0,0))
    assertEquals(Unprotected,result.classify(0,99,99))
    assertEquals(Unprotected,result.classify(-1,0,0))

  @Test def everyOpenPurposeProtectsItsExactCarrier(): Unit =
    Vector("OLD_HOLD","TARGET_STAGED","CURRENT_HOLD","OLD_RETIRE","UNDO").foreach { purpose =>
      val result = index(hold(sourceRows(),purpose))
      assertTrue(result.classify(0,0,0).isInstanceOf[Protected],purpose)
      assertEquals(Unprotected,result.classify(0,99,99))
    }

  @Test def full26OriginalCarrierGraphPreservesProtectedAndUnknownResponsibility():Unit =
    for openHold <- Vector(false,true) do
      val original=snapshot(if openHold then hold(sourceRows()) else sourceRows())
      val inputs=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(original.schema,
        MySqlScriptSchemaDataTestFixture.Full26DdlSha256,original.limitProfile,
        MySqlScriptSchemaDataTestFixture.Inputs.budget,
        MySqlDeploymentReceiptSchemaDefinition.Budget(1_000_000L,1_000_000L))
      val root=inputs.newAllocationRoot(inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(original.limitProfile.maxRetainedBytes()))
      try
        def put(table:String,r:Vector[MySqlFormat3Cell],name:String,v:MySqlFormat3Cell)=
          r.updated(inputs.forTable(table).columns.indexWhere(_.name==name),v)
        val common=original.relations.map { r=>
          if r.table=="fp_schema_manifest" then r.copy(rows=r.rows.map(row=>
            put(r.table,put(r.table,row,"format_version",i(5)),"ddl_sha256",
              binary(java.util.HexFormat.of().parseHex(inputs.ddlSha256)))))
          else r
        }.map(r=>r.table->r).toMap
        val rows=MySqlScriptSchemaDataTestFixture.full26GenesisRelations(inputs,catalogPresent=true).map { relation=>
          common.getOrElse(relation.table,relation.copy(rows=relation.rows.map { r=>
            if inputs.forTable(relation.table).columns.exists(_.name=="binding_uuid") then
              put(relation.table,r,"binding_uuid",u(id(2)))
            else r
          }))
        }
        val image=MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,original.connection,
          original.observationScope,inputs.budget.packetBytes,inputs,rows,root)
        val result=MySqlBootSourceProtectionIndex.fromDeploymentReceipt(image,inputs,root)
        assertEquals(26,image.relations.size)
        assertEquals(2,result.carriers.size)
        assertTrue(result.unresolved.isEmpty,result.unresolved.toString)
        if openHold then assertInstanceOf(classOf[Protected],result.classify(0,0,0))
        else assertEquals(Unknown(Vector("SOURCE_CARRIER_RELEASE_NOT_ESTABLISHED"),Vector.empty),result.classify(0,0,0))
        assertEquals(Unprotected,result.classify(0,99,99))
        assertEquals(original.relation("physical_source_forest"),image.relation("physical_source_forest"))
        assertEquals(original.relation("fp_recovery_operation"),image.relation("fp_recovery_operation"))
      finally
        root.closeRootReadBudget()
        original.allocationBudget.closeRootReadBudget()

  @Test def dischargedHashAndCommittedOperationDoNotInventReleaseProof(): Unit =
    val result = index(hold(sourceRows(),state="DISCHARGED"))
    val found = result.classify(0,0,0).asInstanceOf[Unknown]
    assertTrue(found.localReasons.contains("UNINTERPRETED_RELEASE_PROOF"))
    assertTrue(found.global.isEmpty,found.global.toString)
    assertEquals(Unprotected,result.classify(0,99,99))

  @Test def orphanAndUnjoinableCarrierRemainGlobalUnknown(): Unit =
    val all = hold(sourceRows())
    val orphan = all("physical_source_obligation").map(r => changes(r,"physical_source_obligation","forest_uuid",u(id(9999))))
    assertThrows(classOf[java.sql.SQLDataException],() => index(all.updated("physical_source_obligation",orphan)))
    // A valid SQL carrier reservation may still have no interpreted forest-coordinate counterpart.
    val kb = Array[Byte](1,2,3)
    val changed = changes(changes(all("fp_recovery_reservation").head,"fp_recovery_reservation","resource_key",binary(kb)),
      "fp_recovery_reservation","resource_key_hash",binary(sha(kb)))
    val result = index(sourceRows().updated("fp_recovery_reservation",Vector(changed)))
    assertTrue(result.classify(0,99,99).isInstanceOf[Unknown])
    assertTrue(result.unresolved.exists(_.code.startsWith("UNLOCATED_")))

  @Test def hashMatchCannotHideDifferentFullKeyBytes(): Unit =
    val all = hold(sourceRows())
    val original = all("physical_source_obligation").head
    val changed = changes(original,"physical_source_obligation","carrier_key",binary(Array[Byte](1,2,3)))
    val result = index(all.updated("physical_source_obligation",Vector(changed)))
    assertTrue(result.unresolved.exists(_.code == "SOURCE_OBLIGATION_KEY_HASH"))
    assertTrue(result.classify(0,99,99).isInstanceOf[Unknown])

  @Test def reservedOperationCannotBecomeAbsenceEvenWithoutOpenObligations(): Unit =
    val result = index(Map("fp_recovery_operation"->Vector(op("RESERVED"))))
    assertTrue(result.unresolved.exists(_.code == "RESERVED_OPERATION_SCOPE_UNINTERPRETED"))
    assertTrue(result.classify(0,99,99).isInstanceOf[Unknown])

  @Test def historicalPendingFootprintProtectsWithoutCurrentLocator(): Unit =
    val h = completeHistory()
    val obligation = row("physical_recovery_obligation","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),
      "footprint_uuid"->u(id(30)),"obligation_state"->t("PENDING"),"purpose"->t("ABORTED_CREATE"))
    val fixedObligation = changes(changes(obligation,"physical_recovery_obligation","created_operation_uuid",u(id(1004))),
      "physical_recovery_obligation","last_mutation_operation_uuid",u(id(1004)))
    val result = index(h ++ Map("physical_recovery_obligation"->Vector(fixedObligation),"fp_recovery_operation"->Vector(op())))
    assertTrue(result.classify(0,-2,2).isInstanceOf[Protected])

  @Test def intentCompletionFkAcceptsNullAndExactTargetAndRejectsOrphan(): Unit =
    val h = completeHistory()
    assertDoesNotThrow(() => index(h))
    val intent = Vector("origin"->t("REARM_AFTER_RECALL"),"source_completion_uuid"->u(id(3000)),
      "predecessor_attempt_uuid"->u(id(20)),"predecessor_footprint_uuid"->u(id(30))).foldLeft(h("physical_recovery_intent").head) {
        case (r,(name,value)) => changes(r,"physical_recovery_intent",name,value)
      }
    val withReference = h.updated("physical_recovery_intent",Vector(intent))
    assertThrows(classOf[java.sql.SQLDataException],() => index(withReference))
    val completion = row("fp_lifecycle_completion","binding_uuid"->u(id(2)),"receipt_uuid"->u(id(3000)),
      "pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),"footprint_uuid"->u(id(30)),
      "completion_operation_uuid"->u(id(1004)),"last_mutation_operation_uuid"->u(id(1004)))
    assertDoesNotThrow(() => index(withReference.updated("fp_lifecycle_completion",Vector(completion))))

  @Test def preparedAndFailedSourceWritesProtectExactCarrierAndTerminalResultRemainsUnknown(): Unit =
    val all = hold(sourceRows(),state="DISCHARGED")
    val f = SourceForestSharedBudgetFixtures.forest(2,false,false,false)
    val c = f.carriers().getFirst()
    val reservation = all("fp_recovery_reservation").head
    val keyCell = MySqlModeOperationEffectCodec.value(reservation,"fp_recovery_reservation","resource_key")
    val hashCell = MySqlModeOperationEffectCodec.value(reservation,"fp_recovery_reservation","resource_key_hash")
    Vector("PREPARED","FAILED_NEEDS_RECONCILIATION","DURABLE","NO_PUT","SUPERSEDED").foreach { state =>
      val write = row("physical_source_write","binding_uuid"->u(id(2)),"write_uuid"->u(id(4000)),"resource_kind"->t("CARRIER"),
        "carrier_key"->keyCell,"carrier_key_hash"->hashCell,"save_identity"->t(f.identity().saveIdentity()),
        "loader_identity"->t(f.identity().loaderProfile()),"region_identity"->t(c.regionKey()),"slot_x"->i(c.chunkX()),"slot_z"->i(c.chunkZ()),
        "prepared_operation_uuid"->u(id(1004)),"resolved_operation_uuid"->(if state=="PREPARED" then MySqlFormat3Cell.SqlNull else u(id(1004))),
        "last_mutation_operation_uuid"->u(id(1004)),"write_state"->t(state))
      val result = index(all.updated("physical_source_write",Vector(write)))
      if state == "PREPARED" || state == "FAILED_NEEDS_RECONCILIATION" then assertTrue(result.classify(0,0,0).isInstanceOf[Protected],state)
      else assertTrue(result.classify(0,0,0).isInstanceOf[Unknown],state)
      assertEquals(Unprotected,result.classify(0,99,99))
    }

  @Test def malformedForestShaCountAndUnitIdentityFailExplicitly(): Unit =
    val all = sourceRows()
    val original = all("physical_source_forest").head
    Vector("forest_sha256"->binary(Array.fill[Byte](32)(7)),"member_count"->i(999)).foreach { (field,value) =>
      assertThrows(classOf[IllegalArgumentException],() => index(all.updated("physical_source_forest",Vector(changes(original,"physical_source_forest",field,value)))))
    }
    val wrongUnit = changes(all("physical_source_unit").head,"physical_source_unit","loader_identity",t("foreign"))
    assertThrows(classOf[java.sql.SQLDataException],() => index(all.updated("physical_source_unit",Vector(wrongUnit))))

  @Test def aggregateBudgetIsRequiredBeforeRetainedIndexAndFailureReleasesScratch(): Unit =
    val s = snapshot(sourceRows())
    val root = s.allocationBudget
    val original = root.accountedMemoryBytes
    root.reserveRetainedMemory(root.profile.maxRetainedBytes()-original-1000L,"test held budget")
    val before = root.accountedMemoryBytes
    assertThrows(classOf[java.sql.SQLDataException],() => MySqlBootSourceProtectionIndex.from(s,root))
    assertEquals(before,root.accountedMemoryBytes)

  @Test def graphInternalBudgetFailurePropagatesAfterIndexScratchWasAdmitted(): Unit =
    val s = snapshot()
    val root = s.allocationBudget
    val rowCount = s.relations.foldLeft(0L)((n,r) => n+r.rows.size)
    val scratch = 16384L+rowCount*4096L
    // Initial protection scratch fits. The real full20 query validator needs its own >8KiB
    // plus twenty relation reservations, which cannot fit in the remaining 1KiB.
    root.reserveRetainedMemory(root.profile.maxRetainedBytes()-root.accountedMemoryBytes-scratch-1024L,"hold graph budget boundary")
    val before = root.accountedMemoryBytes
    val failure = assertThrows(classOf[java.sql.SQLDataException],() => MySqlBootSourceProtectionIndex.from(s,root))
    assertTrue(failure.getStackTrace.exists(_.getClassName.contains("MySqlFormat3QueryProjection")))
    assertEquals(before,root.accountedMemoryBytes)

  @Test def bootDataAssemblyAndOriginalEpochRemainSeparateResponsibilities(): Unit =
    val s = snapshot()
    val tag = IoOperationTag(id(3),1L,1L,id(300))
    val owned = stampedBoot(s,tag,0L)
    assertEquals(Unprotected,owned.requireBootReadData.sourceProtection.classify(0,0,0))
    assertEquals(0L,owned.requireOriginalBootEpoch(tag))
    assertThrows(classOf[IllegalArgumentException],() => owned.requireOriginalBootEpoch(IoOperationTag(id(3),1L,1L,id(300))))
