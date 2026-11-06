package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{Decoded, MutationReceipt, OperationPhase}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState}
import java.sql.SQLDataException
import java.time.LocalDateTime
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import MySqlModeOperationEffectCodec.{relations, rowCount, relation}

/** 真实生产 materializer 的完整 DATA 图与读回比较；不签 JDBC/physical/native 资格。 */
class MySqlNormalRecallWorkflowTest:
  import MainBootProjectionFixture.*
  private val at = LocalDateTime.of(2026, 10, 23, 1, 2, 3, 123456000)
  private val nativeId = "FP_WIN32_FILE_ID_V1;volume=0123456789abcdef;id=0123456789abcdef0123456789abcdef"
  private case class Inputs(snapshot: MySqlFormat3FullSnapshot, request: MySqlNormalRecallRequest,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity)
  private def fixture(packet: Long = 4_000_000L, identity: Option[MySqlApprovedBaselineIdentity] = None,
      exactProfile: Option[com.wi.floatingpod.config.SourceLimitProfile] = None): Inputs =
    var h = history(phase = "ACTIVATED")
    var intent = h("physical_recovery_intent").head
    Vector("activation_operation_uuid" -> u(id(40)), "committed_domain_version" -> i(1),
      "committed_at" -> i(1234567890123L), "committed_world_name" -> t("世界"),
      "committed_pos_x" -> f(1.25), "committed_pos_y" -> f(2.5), "committed_pos_z" -> f(3.75),
      "committed_yaw" -> f(1.25), "committed_pitch" -> f(-0.0), "committed_dimension_id" -> i(0))
      .foreach { (name, value) => intent = changes(intent, "physical_recovery_intent", name, value) }
    var locator = h("physical_recovery_locator").head
    Vector("locator_state" -> t("ROW_ACTIVE"), "reason_code" -> t("ROW_ACTIVE"), "domain_version" -> i(5))
      .foreach { (name, value) => locator = changes(locator, "physical_recovery_locator", name, value) }
    val domain = changes(pod(owner = u(id(50)), active = 1), "floating_pod_state", "operating_mode", t("NORMAL"))
    h = h.updated("floating_pod_state", Vector(domain)).updated("physical_recovery_intent", Vector(intent))
      .updated("physical_recovery_locator", Vector(locator)).updated("player_entitlement", Vector(row("player_entitlement",
        "player_uuid" -> u(id(50)), "binding_uuid" -> u(id(2)), "registration_limit" -> i(256), "simultaneous_active_limit" -> i(256))))
    // 明确的历史 DATA ID：本 fixture 不假造实际 session 或归档 witness 的语义资格。
    h = h.map { (table, rows) =>
      val shape = MySqlFormat3RelationShapes.forTable(table)
      table -> rows.map(r => shape.columns.zip(r).map { (column, value) =>
        if column.name.endsWith("operation_uuid") && value != MySqlFormat3Cell.SqlNull then u(id(40)) else value
      })
    }
    h = withCommittedOperationReferences(h, Set(id(40)))
    h = h.updated("fp_runtime_writer", Vector(row("fp_runtime_writer", "singleton_fixed1" -> i(1),
      "binding_uuid" -> u(id(2)), "writer_epoch" -> i(3), "owner_boot_uuid" -> u(id(3)),
      "lease_until" -> MySqlFormat3Cell.DateTime(at.plusHours(1)))))
    val binding=identity.map(_.bindingUuid).getOrElse(id(2))
    val source = identity match
      case None => snapshot(h, retained = 128_000_000L, packet = packet)
      case Some(actual) =>
        val profile=exactProfile.getOrElse(throw new AssertionError("explicit historical fixture profile required"))
        val base=Map("fp_schema_manifest" -> Vector(row("fp_schema_manifest","bootstrap_state" -> t("READY"),"format_version" -> i(3))),
          "physical_recovery_binding" -> Vector(row("physical_recovery_binding","singleton_fixed1" -> i(1),
            "binding_uuid" -> u(binding),"cutover_state" -> t("CERTIFIED"),"format_version" -> i(2))))
        val all=base ++ h
        val relations=MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
          MySqlFormat3RelationSnapshot(shape.table,all.getOrElse(shape.table,Vector.empty).map { original =>
            shape.columns.zip(original).map { (column,cell) =>
              column.name match
                case "binding_uuid" => u(binding)
                case "server_uuid" => u(actual.serverUuid)
                case "provisioning_uuid" => u(actual.provisioningUuid)
                case _ => cell
            }
          })
        }
        val connection=MySqlFormat3ConnectionObservation(actual.serverUuid,actual.serverVersion,actual.schema,
          "pure@localhost","InnoDB",1,1L,"READ-COMMITTED","REPEATABLE-READ","STRICT_ALL_TABLES","+00:00",
          "TLSv1.3","TLS_AES_256_GCM_SHA384")
        val scope=MySqlFormat3ObservationScope(id(3),1L,1L)
        val budget=new MySqlFormat3ReadBudget(profile,packet)
        budget.initialize(actual.schema,connection,scope,packet)
        budget.recordMaterializedSnapshot(relations)
        new MySqlFormat3FullSnapshot(actual.schema,connection,scope,packet,profile,relations,budget)
    val state = FloatingPodState(id(10), Some(id(50)), 5L, "世界", 1.25, 2.5, 3.75,
      1.25f, -0.0f, true, 1234567890123L, Some(0), PodOperatingMode.Normal)
    val request = MySqlNormalRecallRequest(id(100), PhysicalRecoveryBinding(binding, PhysicalRecoveryCutoverState.Certified, 2),
      state, FloatingPodPose("世界", 1.5, 2.75, 3.8, 1.5f, -0.0f, 0), 9L, id(20), id(30),
      (for x <- -2 to 2; z <- -2 to 2 yield PhysicalRecoveryChunk(0, x, z)).toVector,
      MySqlNormalRecallSourceIdentity(nativeId, nativeId))
    val tag = IoOperationTag(id(3), 1L, 1L, id(101))
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, id(102), id(103),
      MySqlRepositoryOperation.PrepareNormalRecall(request), source.limitProfile)
    val context = new MySqlRepositoryOperationContextIdentity(tag, id(102), id(103), id(104), 1L, "fpb1:" + ("A" * 43),
      binding, 3L, at.plusHours(1), identity.map(_.serverUuid).getOrElse(id(1)),
      identity.map(_.serverVersion).getOrElse("9.7.2"), identity.map(_.schema).getOrElse(schema), "pure@localhost",
      identity.map(_.provisioningUuid).getOrElse(id(105)), 3, 2, "a" * 64, "b" * 64,
      Vector.fill(32)(1.toByte), "FRESH_PLAN", Vector.fill(32)(2.toByte), source.limitProfile.sha256().toVector,
      "RUN", plan.requestIdentitySha256, source.limitProfile)
    Inputs(source, request, plan, context)
  private def build(input: Inputs): MySqlNormalRecallMaterialization =
    MySqlNormalRecallWorkflowMaterializer.prepareInitial(input.request, input.plan, input.context,
      at, input.snapshot, input.snapshot.allocationBudget)
  private def value(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row, table, column)
  private def full(image: MySqlModeOperationEffectCodec.Image): MySqlScriptSchemaSnapshot = image match
    case actual: MySqlScriptSchemaSnapshot => actual
    case _ => throw new AssertionError("recall lost complete schema5 image")

  @Test def streamingRecallValidationPreservesWireAndOnlyRetainsFixedDigestData(): Unit =
    val root = fixture().snapshot.allocationBudget
    val domain = "FP_NORMAL_RECALL_FINGERPRINT_BE_V1"
    Vector(32, 200000).foreach { size =>
      val payload = Array.tabulate[Byte](size)(index => (index % 251).toByte)
      def body(out: java.io.DataOutputStream): Unit =
        out.writeInt(payload.length); out.write(payload)
      val encoded = MySqlNormalRecallCanonical.encode(domain, root)(body)
      val original = encoded.binaryCell.value
      val before = root.accountedMemoryBytes
      val streamed = MySqlNormalRecallCanonical.fingerprintStream(domain, root)(body)
      assertEquals(encoded.length, streamed.length)
      assertEquals(encoded.sha256, streamed.sha256)
      assertEquals(640L, root.accountedMemoryBytes - before)
      val comparisonBefore = root.accountedMemoryBytes
      (0 until 3).foreach { _ =>
        assertTrue(MySqlNormalRecallCanonical.matchesStream(domain, original, root)(body))
      }
      assertFalse(MySqlNormalRecallCanonical.matchesStream(domain, original, root) { out =>
        body(out); out.writeByte(0)
      })
      assertFalse(MySqlNormalRecallCanonical.matchesStream(domain, original, root) { out =>
        out.writeInt(payload.length); out.write(payload, 0, payload.length - 1)
      })
      assertFalse(MySqlNormalRecallCanonical.matchesStream(domain, original, root) { out =>
        out.writeInt(payload.length); out.writeByte(payload(0) ^ 1); out.write(payload, 1, payload.length - 1)
      })
      assertEquals(comparisonBefore, root.accountedMemoryBytes)
    }
    val beforeFailure = root.accountedMemoryBytes
    val failure = new java.io.IOException("original Recall streaming writer failure")
    val observed = assertThrows(classOf[java.io.IOException], () =>
      MySqlNormalRecallCanonical.fingerprintStream(domain, root) { _ => throw failure })
    assertSame(failure, observed)
    assertEquals(beforeFailure, root.accountedMemoryBytes)
    val block = new Array[Byte](8192)
    assertThrows(classOf[SQLDataException], () =>
      MySqlNormalRecallCanonical.fingerprintStream(domain, root) { out =>
        (0 until 489).foreach(_ => out.write(block))
      })
    assertEquals(beforeFailure, root.accountedMemoryBytes)

  private def withFull26Prepared(input: Inputs, inputs: MySqlScriptSchemaInputs)(
      body: (MySqlFormat3ReadBudget,MySqlScriptSchemaSnapshot,MySqlRepositoryOperationContextIdentity,
        MySqlNormalRecallMaterialization) => Unit): Unit =
    val old=input.context
    val profile=input.snapshot.limitProfile
    val root=inputs.newAllocationRoot(profile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    var primary: Throwable=null
    try
      def value(r:Vector[MySqlFormat3Cell],table:String,name:String):MySqlFormat3Cell =
        val shape=inputs.forTable(table)
        val index=shape.columns.indexWhere(_.name==name)
        require(r.size==shape.columns.size && index>=0,"exact Full26 fixture row/column required")
        r(index)
      def put(table:String,r:Vector[MySqlFormat3Cell],name:String,v:MySqlFormat3Cell)=
        r.updated(inputs.forTable(table).columns.indexWhere(_.name==name),v)
      val common=input.snapshot.relations.map { r=>
        if r.table=="fp_schema_manifest" then r.copy(rows=r.rows.map(row=>
          put(r.table,put(r.table,row,"format_version",i(5)),"ddl_sha256",
            binary(java.util.HexFormat.of().parseHex(inputs.ddlSha256)))))
        else if r.table=="fp_recovery_operation" then r.copy(rows=r.rows.map { original=>
          def uuid(name:String):java.util.UUID = value(original,r.table,name) match
            case MySqlFormat3Cell.Binary(bytes) if bytes.length==16 =>
              val buffer=java.nio.ByteBuffer.wrap(Array.tabulate[Byte](bytes.length)(bytes.byteAt))
              new java.util.UUID(buffer.getLong(),buffer.getLong())
            case _ => throw new AssertionError("common history UUID missing: "+name)
          val operation=uuid("operation_uuid")
          assertEquals(id(40),operation,"common history must retain the declared synthetic reference")
          val epoch=value(original,r.table,"fence_epoch") match
            case MySqlFormat3Cell.SignedInteger(n) if n>=0L => n
            case _ => throw new AssertionError("common history fence epoch missing")
          val targetHash=value(original,r.table,"target_sha256") match
            case MySqlFormat3Cell.Binary(bytes) if bytes.length==32 => Vector.tabulate[Byte](bytes.length)(bytes.byteAt)
            case _ => throw new AssertionError("common history target digest missing")
          // 固定引用 DATA 的第零版；不把共享 fixture 的三字节 scope/witness 声称为可回放业务历史。
          val context=MySqlOperationContext(uuid("boot_uuid"),epoch,uuid("binding_uuid"),operation,
            Vector(MySqlExpectedEffect(s"full26-fixture-history-reference/$operation",0L,targetHash)))
          val canonical=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
            MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context,profile)
          }
          put(r.table,put(r.table,original,"original_context_bytes",canonical.binaryCell),
            "original_context_sha256",canonical.sha256Cell)
        })
        else r
      }.map(r=>r.table->r).toMap
      // 无关ordinary/UNKNOWN是现有真实DATA fixture；只重绑generic context的authority，不能发行任何运行资格。
      val ordinary = MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(inputs).filter(r =>
        Set("fp_recovery_operation",MySqlDeploymentReceiptSchemaDefinition.OperationTable,
          MySqlDeploymentReceiptSchemaDefinition.QuotaTable).contains(r.table)).map { relation =>
        relation.copy(rows = relation.rows.map { original =>
          var r = original
          inputs.forTable(relation.table).columns.foreach { column =>
            if Set("binding_uuid","expected_binding_uuid","authority_binding_uuid").contains(column.name) then
              r = put(relation.table,r,column.name,u(old.bindingUuid))
          }
          if relation.table == "fp_recovery_operation" then
            def uuid(name:String):java.util.UUID = value(r,relation.table,name) match
              case MySqlFormat3Cell.Binary(bytes) =>
                val buffer=java.nio.ByteBuffer.wrap(Array.tabulate[Byte](bytes.length)(bytes.byteAt))
                new java.util.UUID(buffer.getLong(),buffer.getLong())
              case _ => throw new AssertionError("ordinary history UUID missing: "+name)
            val operation=uuid("operation_uuid")
            val step=value(r,relation.table,"step").asInstanceOf[MySqlFormat3Cell.Text].value
            val version=value(r,relation.table,"expected_domain_version").asInstanceOf[MySqlFormat3Cell.SignedInteger].value
            val hash=value(r,relation.table,"target_sha256").asInstanceOf[MySqlFormat3Cell.Binary].value
            val context=MySqlOperationContext(uuid("boot_uuid"),
              value(r,relation.table,"fence_epoch").asInstanceOf[MySqlFormat3Cell.SignedInteger].value,
              old.bindingUuid,operation,Vector(MySqlExpectedEffect(s"fp_deployment_operation/$operation/$step",version,
                Vector.tabulate[Byte](hash.length)(hash.byteAt))))
            val canonical=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
              MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context,profile)
            }
            r=put(relation.table,put(relation.table,r,"original_context_bytes",canonical.binaryCell),
              "original_context_sha256",canonical.sha256Cell)
          r
        })
      }.map(r=>r.table->r).toMap
      val rows=MySqlScriptSchemaDataTestFixture.full26GenesisRelations(inputs,catalogPresent=true).map { relation=>
        if relation.table=="fp_recovery_operation" then relation.copy(rows=(common(relation.table).rows++ordinary(relation.table).rows)
          .sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(relation.table))<0))
        else if ordinary.contains(relation.table) then ordinary(relation.table)
        else common.getOrElse(relation.table,relation.copy(rows=relation.rows.map { original=>
          var r=original
          val shape=inputs.forTable(relation.table)
          if shape.columns.exists(_.name=="binding_uuid") then r=put(relation.table,r,"binding_uuid",u(old.bindingUuid))
          if shape.columns.exists(_.name=="provisioning_uuid") then r=put(relation.table,r,"provisioning_uuid",u(old.provisioningUuid))
          if relation.table=="binding_bootstrap_intent" then
            r=put(relation.table,r,"stage",t("COMPLETE"))
            r=put(relation.table,r,"marker_ack_sha256",binary(Array.fill[Byte](32)(0x46.toByte)))
          r
        }))
      }
      val before=MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,input.snapshot.connection,
        input.snapshot.observationScope,profile.maxSqlPacketBytes(),inputs,rows,root)
      val context=new MySqlRepositoryOperationContextIdentity(old.tag,old.workflowUuid,old.reserveOperationUuid,
        old.barrierGenerationUuid,old.actualConnectionId,old.barrierNamespace,old.bindingUuid,old.writerEpoch,
        old.fixedLeaseUntil,old.serverUuid,old.serverVersion,old.schema,old.effectiveUser,old.provisioningUuid,
        5,old.physicalFormat,inputs.ddlSha256,old.approvedObjectManifestSha256,old.configurationSha256,
        old.sourceProvenanceKind,old.sourceProvenanceSha256,old.limitProfileSha256,"RUN",input.plan.requestIdentitySha256,profile)
      def full(image:MySqlModeOperationEffectCodec.Image):MySqlScriptSchemaSnapshot=image match
        case actual:MySqlScriptSchemaSnapshot=>actual
        case _=>throw new AssertionError("recall lost complete schema5 image")
      val p=MySqlNormalRecallWorkflowMaterializer.prepareInitial(input.request,input.plan,context,at,before,root)
      body(root,before,context,p)
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      var cleanupFailure: Throwable=null
      def cleanup(action: => Unit): Unit =
        try action
        catch case cause: Throwable =>
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary,cause)
          else if cleanupFailure == null then cleanupFailure=cause
          else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cause)
      cleanup(root.closeRootReadBudget())
      cleanup(input.snapshot.allocationBudget.closeRootReadBudget())
      if primary == null && cleanupFailure != null then throw cleanupFailure

  @Test def full26PrepareAndCheckpointKeepPhaseSeparationAndCompleteHistory():Unit =
    // 完整历史 inactive recovery 的单行 RAW 实测 5075253 bytes；仅本生命周期用例使用 8 MB packet。
    val input=fixture(packet = 8_000_000L)
    val old=input.context
    val profile=input.snapshot.limitProfile
    // 完整 P2 predecessor RAW 的 scope 实测为 374285 bytes；显式设置 evidence，单 blob 上限仍受原 packet envelope 约束。
    val budget=MySqlScriptSchemaDataTestFixture.Inputs.budget.copy(snapshotBytes=4_000_000L, evidenceBytes=1_000_000L,
      rows=profile.maxMembers(),retainedBytes=profile.maxRetainedBytes(),packetBytes=profile.maxSqlPacketBytes())
    val inputs=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(old.schema,
      MySqlScriptSchemaDataTestFixture.Full26DdlSha256,profile,budget,
      MySqlDeploymentReceiptSchemaDefinition.Budget(1_000_000L,1_000_000L))
    withFull26Prepared(input,inputs) { (root,before,context,p) =>
      def value(r:Vector[MySqlFormat3Cell],table:String,name:String):MySqlFormat3Cell =
        val shape=inputs.forTable(table)
        val index=shape.columns.indexWhere(_.name==name)
        require(r.size==shape.columns.size && index>=0,"exact Full26 fixture row/column required")
        r(index)
      def put(table:String,r:Vector[MySqlFormat3Cell],name:String,v:MySqlFormat3Cell)=
        r.updated(inputs.forTable(table).columns.indexWhere(_.name==name),v)
      p.verify()
      val pr=full(p.completeReserveTarget);val pe=full(p.completeExecuteTarget)
      Vector("floating_pod_state","physical_recovery_locator","fp_lifecycle_completion").foreach { table=>
        assertEquals(before.relation(table),pr.relation(table))
        assertEquals(before.relation(table),pe.relation(table))
      }
      def execution(image:MySqlScriptSchemaSnapshot,operation:java.util.UUID)=
        image.relation("fp_recovery_operation").rows.find(r=>value(r,"fp_recovery_operation","operation_uuid")==u(operation)).get
      assertEquals(t("RECALL_PREPARE"),value(execution(pe,p.plan.executeOperationUuid),"fp_recovery_operation","step"))
      assertEquals(t("RESERVED"),value(execution(pr,p.plan.executeOperationUuid),"fp_recovery_operation","operation_state"))
      assertEquals(t("COMMITTED"),value(execution(pe,p.plan.executeOperationUuid),"fp_recovery_operation","operation_state"))
      assertEquals(t("HELD"),value(pe.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_state"))
      val decoded=MySqlNormalRecallCanonical.decodePrepare(p.scope.borrowedBinary,root,Some(inputs))
      assertEquals(5,decoded.header.scope.stableSession.schemaFormat)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ReconcileOriginal],MySqlNormalRecallRecovery.discover(pr,old.bindingUuid).head.cursor)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.VerifyUnsubmittedCheckpoint],MySqlNormalRecallRecovery.discover(pe,old.bindingUuid).head.cursor)
      val recoveryRequest = p.header.request.copy(finalPose = p.header.request.committedPose,
        purpose = MySqlNormalRecallPurpose.RestoreCommittedActive)
      val p2 = activeRecovery(p, pe, recoveryRequest)
      val p2r = full(p2.completeReserveTarget); val p2e = full(p2.completeExecuteTarget)
      assertEquals(2,pe.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.size)
      assertEquals(i(2),pe.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(2))
      assertTrue(pe.relation("fp_recovery_operation").rows.exists(r=>value(r,"fp_recovery_operation","step")==t("DEPLOYMENT_RECONCILE")))
      val ordinaryReceipts=pe.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.map { row =>
        value(row,MySqlDeploymentReceiptSchemaDefinition.OperationTable,"receipt_bytes") match
          case MySqlFormat3Cell.Binary(bytes) =>
            DeploymentCommandWireCodec.decodeResponse(Array.tabulate[Byte](bytes.length)(bytes.byteAt)) match
              case decoded:Decoded[?] => decoded.value() match
                case receipt:MutationReceipt => receipt
                case _ => throw new AssertionError("ordinary fixture must retain its actual MutationReceipt")
              case _ => throw new AssertionError("ordinary fixture receipt must decode")
          case _ => throw new AssertionError("ordinary fixture receipt bytes missing")
      }
      assertEquals(1,ordinaryReceipts.count(_.phase()==OperationPhase.UNKNOWN))
      Vector(p2r, p2e).foreach { image =>
        before.relations.filterNot(r => Set("fp_runtime_writer", "fp_recovery_operation", "fp_recovery_reservation").contains(r.table))
          .foreach(r => assertEquals(pe.relation(r.table), image.relation(r.table)))
        pe.relation("fp_recovery_operation").rows.foreach(r => assertTrue(image.relation("fp_recovery_operation").rows.contains(r)))
        assertEquals(t("HELD"), value(image.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_state"))
      }
      Vector(p2.reserve, p2.execute).foreach { phase =>
        val stored = execution(p2e, phase.operationUuid)
        val reserve = if phase.phase == MySqlOperationEffectPhase.Execute then Some(execution(p2e,p2.plan.reserveOperationUuid)) else None
        val checked = MySqlNormalRecallRecovery.verifyPersistedPhase(stored,reserve,root,Some(inputs))
        val resolved = MySqlNormalRecallRecovery.resolveCommittedEffectData(checked,p2e,root)
        assertEquals(phase.before.relations,resolved.before)
        assertEquals(phase.target.relations,resolved.target)
        MySqlNormalRecallRecovery.requireCommittedEffectDelta(resolved,root)
      }
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ResumeRecoveryPrepare],
        MySqlNormalRecallRecovery.discover(p2r,old.bindingUuid).head.cursor)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical],
        MySqlNormalRecallRecovery.discover(p2e,old.bindingUuid).head.cursor)
      // 自洽重签的effect/context/witness也不能给P2增加domain写权限。
      val alteredDomain = put("floating_pod_state",p2e.relation("floating_pod_state").rows.head,"last_updated",i(999))
      var forgedP2 = MySqlControlledDebugImages.put(p2e,"floating_pod_state",alteredDomain,root,true)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        val target = MySqlNormalRecallOperationEffectCodec.projection(forgedP2,p2.header,MySqlOperationEffectPhase.Execute,
          MySqlOperationEffectImage.Target,None,root)
        val fp = MySqlNormalRecallOperationEffectCodec.fingerprint(p2.header,MySqlOperationEffectPhase.Execute,
          p2.plan.requestIdentity.canonical,p2.scope,p2.execute.beforeBytes,target,root)
        val operationContext = MySqlNormalRecallOperationEffectCodec.context(p2.header,p2.plan.executeOperationUuid,fp)
        val contextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(operationContext,root.profile)
        val reserve = execution(p2e,p2.plan.reserveOperationUuid)
        val raw = MySqlRecoveryOperationRawEnvelopeCodec.encode(reserve,root.profile,inputs)
        val witness = MySqlNormalRecallWorkflowMaterializer.witness(p2.header,MySqlOperationEffectPhase.Execute,
          p2.plannedAt,p2.scope,fp,contextBytes,p2.execute.beforeBytes,target,Some(raw),root)
        val terminal = MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.phase(
          execution(p2e,p2.plan.executeOperationUuid),contextBytes,p2.execute.beforeBytes,target,fp,root),witness,p2.plannedAt,root)
        forgedP2 = MySqlControlledDebugImages.put(forgedP2,"fp_recovery_operation",terminal,root,true)
        forgedP2 = MySqlControlledDebugOperationRows.writer(forgedP2,p2.plan.executeOperationUuid,
          operationContext.canonicalFingerprintSha256,root)
        val checked = MySqlNormalRecallRecovery.verifyPersistedPhase(terminal,Some(reserve),root,Some(inputs))
        val resolved = MySqlNormalRecallRecovery.resolveCommittedEffectData(checked,forgedP2,root)
        val rejected=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallRecovery.requireCommittedEffectDelta(resolved,root))
        assertEquals("requirement failed: Recall full20 target changes values outside its exact original phase delta",rejected.getMessage)
      }
      // 原请求的final pose不同于committed pose；P2只能恢复原committed位置。
      assertThrows(classOf[IllegalArgumentException],()=>activeRecovery(p,pe,
        recoveryRequest.copy(finalPose=p.header.request.finalPose)))
      val c=checkpoint(p)
      c.verify()
      val cr=full(c.completeReserveTarget);val ce=full(c.completeExecuteTarget)
      assertThrows(classOf[IllegalArgumentException],()=>activeRecovery(p,cr,recoveryRequest))
      val decodedP2=MySqlNormalRecallCanonical.decodePrepare(p2.scope.borrowedBinary,root,Some(inputs))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlNormalRecallRecovery.requireActivePrepareWithoutCheckpointHistoryData(decodedP2,cr,root))
      MySqlNormalRecallCanonical.decodeCheckpoint(c.scope.borrowedBinary,root,Some(inputs))
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ReconcileOriginal],MySqlNormalRecallRecovery.discover(cr,old.bindingUuid).head.cursor)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.VerifyCommittedCheckpoint],MySqlNormalRecallRecovery.discover(ce,old.bindingUuid).head.cursor)
      val domain=ce.relation("floating_pod_state").rows.head
      assertEquals(i(0),value(domain,"floating_pod_state","is_active"))
      assertEquals(i(6),value(domain,"floating_pod_state","state_version"))
      assertEquals(t("ROW_INACTIVE_PENDING"),value(ce.relation("physical_recovery_locator").rows.head,"physical_recovery_locator","locator_state"))
      assertEquals(t("RELEASED"),value(ce.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_state"))
      assertEquals(t("CHECKPOINT_COMMITTED"),value(ce.relation("fp_lifecycle_completion").rows.head,"fp_lifecycle_completion","completion_state"))
      val inactiveRequest = committedInactiveRequest(c)
      val inactive = inactiveRecovery(p,c,ce,inactiveRequest)
      val ir = full(inactive.completeReserveTarget); val ie = full(inactive.completeExecuteTarget)
      val decodedInactive = MySqlNormalRecallCanonical.decodePrepare(inactive.scope.borrowedBinary,root,Some(inputs))
      assertEquals(MySqlNormalRecallPurpose.FinishCommittedInactive,decodedInactive.purpose)
      assertEquals(Some(p.plan.executeOperationUuid),decodedInactive.originalPrepare)
      assertEquals(Some(c.plan.executeOperationUuid),decodedInactive.originalCheckpoint)
      assertEquals(Some(c.plan.executeOperationUuid),decodedInactive.header.previous)
      assertEquals(execution(ce,c.plan.executeOperationUuid),decodedInactive.predecessor.get.row)
      val originalReservation=ce.relation("fp_recovery_reservation").rows.head
      val oldRevision=value(originalReservation,"fp_recovery_reservation","reservation_revision")
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value
      Vector(ir,ie).zipWithIndex.foreach { (image,index) =>
        ce.relations.filterNot(r=>Set("fp_runtime_writer","fp_recovery_operation","fp_recovery_reservation").contains(r.table))
          .foreach(r=>assertEquals(r,image.relation(r.table)))
        ce.relation("fp_recovery_operation").rows.foreach(r=>assertTrue(image.relation("fp_recovery_operation").rows.contains(r)))
        val reservation=image.relation("fp_recovery_reservation").rows.head
        Vector("binding_uuid","workflow_uuid","resource_kind","resource_key","resource_key_hash","resource_pod_uuid","resource_unit_uuid")
          .foreach(n=>assertEquals(value(originalReservation,"fp_recovery_reservation",n),value(reservation,"fp_recovery_reservation",n)))
        assertEquals(t("HELD"),value(reservation,"fp_recovery_reservation","reservation_state"))
        assertEquals(u(inactive.plan.executeOperationUuid),value(reservation,"fp_recovery_reservation","operation_uuid"))
        assertEquals(i(oldRevision+index+1L),value(reservation,"fp_recovery_reservation","reservation_revision"))
        assertEquals(u(if index==0 then inactive.plan.reserveOperationUuid else inactive.plan.executeOperationUuid),
          value(reservation,"fp_recovery_reservation","last_mutation_operation_uuid"))
      }
      Vector(inactive.reserve,inactive.execute).foreach { phase =>
        val stored=execution(ie,phase.operationUuid)
        val reserve=if phase.phase==MySqlOperationEffectPhase.Execute then Some(execution(ie,inactive.plan.reserveOperationUuid)) else None
        val checked=MySqlNormalRecallRecovery.verifyPersistedPhase(stored,reserve,root,Some(inputs))
        val resolved=MySqlNormalRecallRecovery.resolveCommittedEffectData(checked,ie,root)
        assertEquals(phase.before.relations,resolved.before)
        assertEquals(phase.target.relations,resolved.target)
        MySqlNormalRecallRecovery.requireCommittedEffectDelta(resolved,root)
      }
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ResumeRecoveryPrepare],
        MySqlNormalRecallRecovery.discover(ir,old.bindingUuid).head.cursor)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical],
        MySqlNormalRecallRecovery.discover(ie,old.bindingUuid).head.cursor)
      // 直接核同一生产history边界的Before：不得偷用HELD、外workflow、错key或过期revision。
      Vector("reservation_state"->t("HELD"),"workflow_uuid"->u(id(998)),"operation_uuid"->u(p.plan.executeOperationUuid),
        "resource_key_hash"->binary(Array.fill[Byte](32)(0x17.toByte)),"reservation_revision"->i(oldRevision+1L)).foreach { (name,v) =>
        val invalidBefore=ce.relations.map(r=>if r.table=="fp_recovery_reservation" then
          r.copy(rows=Vector(put(r.table,originalReservation,name,v))) else r)
        val rejected=assertThrows(classOf[IllegalArgumentException],()=>
          MySqlNormalRecallRecovery.requireInactivePrepareHistoryData(decodedInactive,ce,invalidBefore,MySqlOperationEffectPhase.Reserve,root))
        assertEquals("requirement failed: inactive P2 before must retain the exact original C reservation or its single Rp2 reacquisition",rejected.getMessage)
      }
      val alteredCompletion=ce.relations.map(r=>if r.table=="fp_lifecycle_completion" then
        r.copy(rows=r.rows.map(row=>put(r.table,row,"completion_revision",i(1)))) else r)
      val completionRejected=assertThrows(classOf[IllegalArgumentException],()=>
        MySqlNormalRecallRecovery.requireInactivePrepareHistoryData(decodedInactive,ce,alteredCompletion,MySqlOperationEffectPhase.Reserve,root))
      assertEquals("requirement failed: inactive P2 original committed C completion changed or was consumed",completionRejected.getMessage)
      val pendingRejected=assertThrows(classOf[SQLDataException],()=>inactiveRecovery(p,c,cr,inactiveRequest))
      assertEquals("normal recall materialization: original committed Normal domain differs",pendingRejected.getMessage)
      val additional=inputs.canonicalOrder.filterNot(s=>MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==s.table))
      assertEquals(6,additional.size)
      Vector(pr,pe,cr,ce).foreach { image=>
        inputs.validateRelations(image.relations,profile.maxSqlPacketBytes(),root)
        additional.foreach(s=>assertEquals(before.relation(s.table),image.relation(s.table)))
        before.relation("fp_recovery_operation").rows.foreach(r=>assertTrue(image.relation("fp_recovery_operation").rows.contains(r)))
      }
      assertThrows(classOf[java.sql.SQLDataException],()=>p.executeTarget)
      val operationTable="fp_recovery_operation"
      def capture(history:Vector[Vector[MySqlFormat3Cell]],source:MySqlScriptSchemaSnapshot=cr)=
        MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,source.connection,source.observationScope,
          profile.maxSqlPacketBytes(),inputs,source.relations.map(r=>
            if r.table==operationTable then r.copy(rows=history.sortWith((a,b)=>
              MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(operationTable))<0)) else r),root)
      def replace(history:Vector[Vector[MySqlFormat3Cell]],operation:java.util.UUID,
          transform:Vector[MySqlFormat3Cell]=>Vector[MySqlFormat3Cell])=
        history.map(r=>if value(r,operationTable,"operation_uuid")==u(operation) then transform(r) else r)
      def reject(reasons:String*)(body: =>Unit):Unit =
        val failure=assertThrows(classOf[java.sql.SQLDataException],()=>body)
        assertTrue(failure.getStackTrace.nonEmpty)
        assertTrue(reasons.exists(failure.getMessage.contains),failure.getMessage)
      val history=cr.relation(operationTable).rows
      val originalC=execution(cr,c.plan.executeOperationUuid)
      // 仅待核negative cursor的行拓扑：opaque字节不证明CANCEL witness语义或SQL settlement。
      val cancelId=id(115)
      val settlementContext=MySqlOperationContext(c.execute.operationContext.bootUuid,c.execute.operationContext.writerEpoch,
        c.execute.operationContext.bindingUuid,cancelId,c.execute.operationContext.expectedEffects)
      val contextBytes=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(settlementContext,profile)
      }
      val negative=MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_TEST_NEGATIVE_SETTLEMENT_DATA_V1",root) { out=>
        MySqlRecoveryCanonicalBytes.writeUuid(out,c.plan.executeOperationUuid)
        MySqlRecoveryCanonicalBytes.writeUuid(out,cancelId)
      }
      def terminal(row:Vector[MySqlFormat3Cell],state:String)=
        put(operationTable,put(operationTable,put(operationTable,put(operationTable,row,
          "operation_state",t(state)),"result_at",MySqlFormat3Cell.DateTime(at.plusSeconds(2))),
          "commit_witness_bytes",MySqlFormat3Cell.Binary(negative.borrowedBinary)),"commit_witness_sha256",negative.sha256Cell)
      val cancelledC=put(operationTable,terminal(originalC,"CANCELLED"),"settled_by_operation_uuid",u(cancelId))
      var cancel=terminal(originalC,"COMMITTED")
      Vector("operation_uuid"->u(cancelId),"step"->t("CANCEL"),
        "previous_step_operation_uuid"->u(c.plan.executeOperationUuid),
        "original_context_bytes"->MySqlFormat3Cell.Binary(contextBytes.borrowedBinary),
        "original_context_sha256"->contextBytes.sha256Cell,
        "operation_fingerprint"->binary(settlementContext.canonicalFingerprintSha256.toArray)).foreach { (name,v)=>
          cancel=put(operationTable,cancel,name,v)
        }
      val negativeRows=replace(history,c.plan.executeOperationUuid,_=>cancelledC):+cancel
      val cancelled=capture(negativeRows)
      val negativeCursor=MySqlNormalRecallRecovery.discover(cancelled,old.bindingUuid).head.cursor
      negativeCursor match
        case MySqlNormalRecallRecoveryCursor.VerifyNegativeSettlement(checkpoint,settlement)=>
          assertEquals(c.plan.executeOperationUuid,checkpoint.operationUuid)
          assertEquals(cancelId,settlement.operationUuid)
        case other=>throw new AssertionError("cancelled C lost its exact negative settlement: "+other)
      reject("no same-binding primary-key target","workflow predecessor missing") { MySqlNormalRecallRecovery.discover(capture(history.filterNot(r=>
        value(r,operationTable,"operation_uuid")==u(p.plan.executeOperationUuid))),old.bindingUuid);() }
      // 重复原P/C，即使其他行及原context仍在，也不是一个可恢复的健康inventory。
      Vector(p.plan.executeOperationUuid,c.plan.executeOperationUuid).foreach { operation=>
        reject("duplicate operation_uuid","duplicate original operation identity","rows are not strictly ordered by the frozen primary key") { MySqlNormalRecallRecovery.discover(capture(history:+execution(cr,operation)),old.bindingUuid);() }
      }
      reject("C does not follow original P") { MySqlNormalRecallRecovery.discover(capture(replace(history,c.plan.executeOperationUuid,
        row=>put(operationTable,row,"previous_step_operation_uuid",u(p.plan.reserveOperationUuid)))),old.bindingUuid);() }
      reject("no same-binding primary-key target","missing exact workflow link: settled_by_operation_uuid") { MySqlNormalRecallRecovery.discover(capture(negativeRows.filterNot(r=>
        value(r,operationTable,"operation_uuid")==u(cancelId))),old.bindingUuid);() }
      reject("C cancellation lacks exact negative settlement") { MySqlNormalRecallRecovery.discover(capture(replace(negativeRows,cancelId,
        row=>put(operationTable,row,"previous_step_operation_uuid",u(p.plan.executeOperationUuid)))),old.bindingUuid);() }
      val committedC=execution(ce,c.plan.executeOperationUuid)
      val alteredWitness=put(operationTable,put(operationTable,committedC,
        "commit_witness_bytes",MySqlFormat3Cell.Binary(negative.borrowedBinary)),"commit_witness_sha256",negative.sha256Cell)
      val changedWitness=assertThrows(classOf[IllegalArgumentException],()=>
        MySqlNormalRecallRecovery.verifyPersistedPhase(alteredWitness,Some(execution(ce,c.plan.reserveOperationUuid)),root,Some(inputs)))
      assertTrue(changedWitness.getMessage.contains("Recall complete witness differs from original phase"))
    }

  @Test def actualHistoricalHolderValidatesEveryFull27PersistedPrepareAndCheckpoint(): Unit =
    val limits=com.wi.floatingpod.config.SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,
      64L,256L,128_000_000L,8_000_000L,2_000_000L)
    val defaults=DeploymentReceiptStaticTestData.defaultProfile()
    val publication=MySqlScriptSchemaDataTestFixture.Inputs.budget.copy(snapshotBytes=8_000_000L,
      evidenceBytes=1_000_000L,rows=limits.maxMembers(),retainedBytes=limits.maxRetainedBytes(),packetBytes=limits.maxSqlPacketBytes())
    val configured=MySqlDeploymentReceiptSchemaProfile.inputs(defaults.schema,defaults.accounts,limits,publication,
      MySqlDeploymentReceiptSchemaDefinition.Budget(1_000_000L,1_000_000L),defaults.quotaMaximumRecords,Vector.empty)
    val directory=java.nio.file.Files.createTempDirectory("fp-real-historical-phase")
    val owned=MySqlDbFirstHistoricalTestFixture.open(directory,configured,MySqlOwnedIoIdentity(id(3),1L,1L))
    var primary: Throwable=null
    try
      val input=fixture(8_000_000L,Some(owned.historicalIdentity),Some(limits))
      withFull26Prepared(input,owned.history.inputs) { (producer,before,context,p) =>
        val c=checkpoint(p)
        val current=owned.inputs.full27Inputs
        def copyImage(source: MySqlScriptSchemaSnapshot): MySqlScriptSchemaSnapshot =
          val relations=source.relations.map { relation =>
            val shape=current.forTable(relation.table)
            relation.copy(rows=relation.rows.map { original =>
              val row=if relation.table == "fp_schema_manifest" then original
                .updated(shape.columns.indexWhere(_.name == "format_version"),i(6))
                .updated(shape.columns.indexWhere(_.name == "ddl_sha256"),binary(java.util.HexFormat.of().parseHex(current.ddlSha256)))
              else original
              row.map {
                case MySqlFormat3Cell.Binary(bytes) =>
                  MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.copyBytes(owned.root,"actual Full27 history handoff"),owned.root))
                case cell => cell
              }
            })
          } :+ MySqlFormat3RelationSnapshot(MySqlDbFirstSchemaDefinition.SnapshotTable,Vector.empty)
          MySqlScriptSchemaSnapshot.captureMaterialized(current.schema,source.connection,source.observationScope,
            limits.maxSqlPacketBytes(),current,relations,owned.root)
        def observe(snapshot: MySqlScriptSchemaSnapshot, history: Option[MySqlDbFirstHistoryInputs]):
            MySqlNormalRecallRecovery.PersistedPhaseSummary =
          owned.root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"test historical read holder BEFORE allocation")
          val holder=new MySqlDbFirstSession.HistoricalRead(owned.root,current)
          var failure: Throwable=null
          try
            holder.acquire(history)
            holder.validate(snapshot)
            holder.result
          catch case cause: Throwable =>
            failure=cause
            throw cause
          finally
            try holder.close()
            catch case cleanup: Throwable =>
              if failure == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(failure,cleanup)
        Vector((p.completeReserveTarget,1L),(p.completeExecuteTarget,1L),
          (c.completeReserveTarget,2L),(c.completeExecuteTarget,2L)).foreach { (actual,count) =>
          val snapshot=copyImage(full(actual))
          assertEquals(27,snapshot.relations.size)
          assertEquals(full(actual).relation("fp_recovery_operation"),snapshot.relation("fp_recovery_operation"))
          val checked=observe(snapshot,Some(owned.history))
          assertEquals(count,checked.verifiedExecutions)
          assertEquals(count,checked.verifiedReserves)
          assertEquals(0L,checked.pendingSettlements)
          val absent=assertThrows(classOf[IllegalStateException],()=>observe(snapshot,None))
          assertTrue(absent.getMessage.contains("trusted original Full26"))
          val table="fp_recovery_operation"
          def changedRows(f: Vector[Vector[MySqlFormat3Cell]] => Vector[Vector[MySqlFormat3Cell]]) =
            val changed=snapshot.relations.map(r=>if r.table == table then r.copy(rows=f(r.rows)) else r)
            MySqlScriptSchemaSnapshot.captureMaterialized(current.schema,snapshot.connection,snapshot.observationScope,
              limits.maxSqlPacketBytes(),current,changed,owned.root)
          if count == 2L then
            assertThrows(classOf[IllegalArgumentException],()=>
              observe(changedRows(_.map(r=>if value(r,table,"operation_uuid") == u(c.plan.executeOperationUuid) then
                r.updated(current.forTable(table).columns.indexWhere(_.name == "step"),t("DOMAIN_MUTATION")) else r)),Some(owned.history)))
            assertThrows(classOf[IllegalArgumentException],()=>
              observe(changedRows(_.map(r=>if value(r,table,"operation_uuid") == u(p.plan.executeOperationUuid) then
                r.updated(current.forTable(table).columns.indexWhere(_.name == "reserve_operation_uuid"),u(c.plan.reserveOperationUuid))
                else r)),Some(owned.history)))
        }
        val borrow=owned.history.borrow()
        owned.root.reserveTemporaryMemory(512L,"test lexical children BEFORE allocation")
        val child=owned.root.newCanonicalScope()
        val sibling=owned.root.newCanonicalScope()
        var scopeFailure: Throwable=null
        try
          assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>owned.root.requireHistoricalPayloadInputs(owned.history.inputs))
          MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
            child.withHistoricalPayloadInputs(owned.history.inputs,borrow) {
              child.requireHistoricalPayloadInputs(owned.history.inputs)
              val actual=owned.history.inputs
              val foreign=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(actual.schema,actual.ddlSha256,
                actual.sourceLimitProfile,actual.budget,actual.deploymentBudget.get)
              assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>child.requireHistoricalPayloadInputs(foreign))
              assertThrows(classOf[IllegalArgumentException],()=>sibling.withHistoricalPayloadInputs(actual,borrow) { () })
              assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>child.requireScriptDataInputs(owned.history.inputs))
              assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>sibling.requireHistoricalPayloadInputs(owned.history.inputs))
            }
            assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>child.requireHistoricalPayloadInputs(owned.history.inputs))
          }
        catch case cause: Throwable =>
          scopeFailure=cause
          throw cause
        finally
          def cleanup(action: => Unit): Unit =
            try action
            catch case cause: Throwable =>
              if scopeFailure == null then scopeFailure=cause else MySqlOwnedSessionLifecycle.suppress(scopeFailure,cause)
          cleanup(child.closeScopedReadBudget())
          cleanup(sibling.closeScopedReadBudget())
          cleanup(owned.root.releaseTemporaryMemory(512L,"test lexical children detached"))
          cleanup(borrow.close())
          if scopeFailure == null then
            assertThrows(classOf[IllegalStateException],()=>borrow.requireInputs(owned.root,owned.history.inputs))
          if scopeFailure != null then throw scopeFailure
      }
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try owned.close()
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  @Test def strictPrepareAndCheckpointDataRoundtripTheActualMaterializer(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      val decodedP = MySqlNormalRecallCanonical.decodePrepare(p.scope.borrowedBinary, p.root)
      val decodedC = MySqlNormalRecallCanonical.decodeCheckpoint(c.scope.borrowedBinary, p.root)
      assertEquals(p.plan.tag, decodedP.header.scope.tag)
      assertEquals(p.header.context.barrierGenerationUuid, decodedP.header.scope.stableSession.barrier)
      assertEquals(p.header.request.source, decodedP.header.source)
      assertEquals(p.header.request.originalFootprint, decodedP.header.chunks)
      assertEquals(decodedP, decodedC.prepare)
      assertEquals(c.plan.tag, decodedC.header.scope.tag)
      assertEquals(6L, decodedC.afterVersion)
      assertEquals(id(110), decodedC.receipt)
      assertEquals(java.lang.Float.floatToRawIntBits(-0.0f), decodedP.header.finalPose.pitch)
      assertEquals(c.executeTarget.relation("floating_pod_state"), decodedC.targetDomainLocator.find(_.table == "floating_pod_state").get)
    finally p.root.closeRootReadBudget()

  @Test def strictFull20EffectDataPreservesTypedCellsAndExactDerivedLocations(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      Vector(p, c).foreach { m =>
        val h = if m eq p then MySqlNormalRecallCanonical.decodePrepare(m.scope.borrowedBinary, m.root).header
          else MySqlNormalRecallCanonical.decodeCheckpoint(m.scope.borrowedBinary, m.root).header
        val receipt = if m eq c then Some(id(110)) else None
        Vector(m.reserve, m.execute).foreach { phase =>
          Vector((phase.beforeBytes, MySqlOperationEffectImage.Before, phase.before),
            (phase.targetBytes, MySqlOperationEffectImage.Target, phase.target)).foreach { (bytes, image, original) =>
            val decoded = MySqlNormalRecallCanonical.decodeEffect(bytes.borrowedBinary, h, phase.phase, image, receipt, m.root)
            assertEquals(original.relations.map(_.table), decoded.relations.map(_.table))
            assertEquals(original.rowCount, decoded.relations.map(_.rows.size.toLong).sum)
            decoded.relations.zip(original.relations).foreach { (relation, actual) =>
              relation.rows.zip(actual.rows).foreach { (row, raw) => row.zip(raw).foreach {
                case (MySqlNormalRecallCanonical.EffectCell.Scalar(value), expected) => assertEquals(expected, value)
                case _ => ()
              } }
            }
            if phase.phase == MySqlOperationEffectPhase.Execute && image == MySqlOperationEffectImage.Target && (m eq c) then
              assertEquals(2, decoded.relations.find(_.table == "fp_lifecycle_completion").get.rows.flatten.count(
                _ == MySqlNormalRecallCanonical.EffectCell.CompletionDerived))
            val corrupt = bytes.copyBytes(m.root, "effect domain negative")
            corrupt(4) = (corrupt(4) ^ 1).toByte
            assertThrows(classOf[Exception], () => MySqlNormalRecallCanonical.decodeEffect(
              MySqlFormat3Binary.takeOwned(corrupt), h, phase.phase, image, receipt, m.root))
            val wrongImage = if image == MySqlOperationEffectImage.Before then MySqlOperationEffectImage.Target else MySqlOperationEffectImage.Before
            assertThrows(classOf[Exception], () => MySqlNormalRecallCanonical.decodeEffect(
              bytes.borrowedBinary, h, phase.phase, wrongImage, receipt, m.root))
          }
        }
      }
    finally p.root.closeRootReadBudget()

  @Test def strictFull20EffectRejectsMarkerRelocationDescriptorsAndBrokenFraming(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      val decoded = MySqlNormalRecallCanonical.decodeCheckpoint(c.scope.borrowedBinary, p.root)
      val bytes = c.execute.targetBytes.copyBytes(p.root, "effect negative fixture")
      def atText(text: String, from: Int = 0): Int =
        val target = text.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val index = (from to (bytes.length - target.length)).find(i => target.indices.forall(j => bytes(i + j) == target(j))).get
        index
      def rejects(changed: Array[Byte]): Unit = MySqlRecoveryCanonicalBytes.withComparisonBudget(p.root) {
        assertThrows(classOf[Exception], () => MySqlNormalRecallCanonical.decodeEffect(MySqlFormat3Binary.takeOwned(changed),
          decoded.header, MySqlOperationEffectPhase.Execute, MySqlOperationEffectImage.Target, Some(id(110)), p.root))
      }
      Vector(0, 3, 40, bytes.length / 2, bytes.length - 1).foreach(n => rejects(bytes.take(n)))
      rejects(bytes :+ 0.toByte)
      val marker = atText("FP_NORMAL_RECALL_DERIVED_CELL_V1")
      val source = atText("C_ORIGINAL_CONTEXT", marker)
      val wrongSource = bytes.clone(); wrongSource(source) = 'X'.toByte; rejects(wrongSource)
      val wrongKind = bytes.clone(); wrongKind(source + "C_ORIGINAL_CONTEXT".length) = 9.toByte; rejects(wrongKind)
      val operation = atText("FP_NORMAL_RECALL_OPERATION_DERIVED_V1")
      val key = atText("singleton_fixed1", operation)
      val wrongKey = bytes.clone(); wrongKey(key) = 'X'.toByte; rejects(wrongKey)
      val history = atText("FP_NORMAL_RECALL_HISTORICAL_PAYLOAD_V1")
      val wrongDescriptorTag = bytes.clone(); wrongDescriptorTag(history - 5) = 2.toByte; rejects(wrongDescriptorTag)
      // 固定descriptor中domain后依次是column/type/length/hash；篡改hash而不改成另一个原行。
      def intAt(offset: Int): Int = java.nio.ByteBuffer.wrap(bytes, offset, 4).getInt
      val columnLengthAt = history + "FP_NORMAL_RECALL_HISTORICAL_PAYLOAD_V1".length
      val typeLengthAt = columnLengthAt + 4 + intAt(columnLengthAt)
      val lengthAt = typeLengthAt + 4 + intAt(typeLengthAt)
      val badLength = bytes.clone(); java.util.Arrays.fill(badLength, lengthAt, lengthAt + 8, 0xff.toByte); rejects(badLength)
      val badDigest = bytes.clone(); badDigest(lengthAt + 8) = (badDigest(lengthAt + 8) ^ 1).toByte; rejects(badDigest)
    finally p.root.closeRootReadBudget()

  @Test def committedEffectResolutionUsesCompleteActualOriginalRows(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      Vector(p, c).foreach { m =>
        def raw(operation: java.util.UUID) = m.executeTarget.relation("fp_recovery_operation").rows.find(r =>
          value(r, "fp_recovery_operation", "operation_uuid") == u(operation)).get
        Vector(m.reserve, m.execute).foreach { phase =>
          val checked = MySqlNormalRecallRecovery.verifyPersistedPhase(raw(phase.operationContext.operationUuid),
            if phase.phase == MySqlOperationEffectPhase.Execute then Some(raw(m.plan.reserveOperationUuid)) else None, m.root)
          val resolved = MySqlNormalRecallRecovery.resolveCommittedEffectData(checked, m.executeTarget, m.root)
          assertEquals(phase.before.relations, resolved.before)
          assertEquals(phase.target.relations, resolved.target)
          MySqlNormalRecallRecovery.requireCommittedEffectDelta(resolved, m.root)
          val old = raw(id(40))
          val altered = changes(old, "fp_recovery_operation", "connection_id", i(999))
          val other = MySqlControlledDebugImages.put(m.executeTarget, "fp_recovery_operation", altered, m.root, true)
          assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.resolveCommittedEffectData(checked, other, m.root))
        }
        val pending = m.reserveTarget.relation("fp_recovery_operation").rows.find(r =>
          value(r, "fp_recovery_operation", "operation_uuid") == u(m.plan.executeOperationUuid)).get
        val pendingData = MySqlNormalRecallRecovery.verifyPersistedPhase(pending, Some(raw(m.plan.reserveOperationUuid)), m.root)
        assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.resolveCommittedEffectData(pendingData, m.reserveTarget, m.root))
      }
    finally p.root.closeRootReadBudget()

  @Test def pendingCheckpointExpectedImageKeepsActualInventoryAndCannotBecomePositive(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      val actual = c.reserveTarget
      def raw(id: java.util.UUID) = actual.relation("fp_recovery_operation").rows.find(r =>
        value(r, "fp_recovery_operation", "operation_uuid") == u(id)).get
      val original = actual.relations
      val phase = MySqlNormalRecallRecovery.verifyPersistedPhase(raw(c.plan.executeOperationUuid),
        Some(raw(c.plan.reserveOperationUuid)), p.root)
      val expected = MySqlNormalRecallRecovery.resolvePendingCheckpointImages(phase, actual, p.root)
      assertEquals(c.execute.before.relations, expected.before)
      assertEquals(c.executeTarget.relations, expected.expectedTarget)
      assertEquals(original, actual.relations)
      assertEquals(t("RESERVED"), value(raw(c.plan.executeOperationUuid), "fp_recovery_operation", "operation_state"))
      assertEquals(MySqlFormat3Cell.SqlNull, value(raw(c.plan.executeOperationUuid), "fp_recovery_operation", "commit_witness_bytes"))
      assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.resolveCommittedEffectData(phase, actual, p.root))
      p.root.reserveRetainedMemory(p.root.availableMemoryBytes - 1L, "pending target exhausted root")
      assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.resolvePendingCheckpointImages(phase, actual, p.root))
    finally p.root.closeRootReadBudget()

  @Test def checkpointDecisionRequiresActualHistoryForAllThreeOriginalBranches(): Unit =
    Vector(0, 1, 2).foreach { branch =>
      val input = fixture()
      val p = build(input)
      try
        val actual = if branch == 0 then p.executeTarget else
          val c = checkpoint(p)
          if branch == 1 then c.reserveTarget else c.executeTarget
        val history = MySqlNormalRecallRecovery.verifyReceiptHistoryData(actual, input.snapshot, p.root)
        val inventory = MySqlNormalRecallRecovery.discover(actual, id(2)).head
        val decision = MySqlNormalRecallRecovery.classifyCheckpointData(actual, Vector(history), inventory, p.root)
        assertEquals(Vector(MySqlNormalRecallCheckpointObservation.NotReserved,
          MySqlNormalRecallCheckpointObservation.ReservedNeedsSettlement,
          MySqlNormalRecallCheckpointObservation.Committed)(branch), decision.observation)
        assertEquals(branch == 1, decision.pending.nonEmpty)
        assertEquals(branch == 2, decision.committed.nonEmpty)
        assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.classifyCheckpointData(actual, Vector.empty, inventory, p.root))
        assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.classifyCheckpointData(actual, Vector(history, history), inventory, p.root))
      finally p.root.closeRootReadBudget()
    }

  @Test def exactDeltaRejectsSelfConsistentWitnessForUnauthorizedChanges(): Unit =
    Vector("player_entitlement", "floating_pod_state", "fp_recovery_reservation").foreach { table =>
      val p = build(fixture())
      try
        val m = if table == "player_entitlement" then p else checkpoint(p)
        val root = m.root
        val original = m.executeTarget.relation(table).rows.head
        val altered = table match
          case "player_entitlement" => changes(original, table, "registration_limit", i(255))
          case "floating_pod_state" => changes(original, table, "last_updated", i(m.plannedAt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli + 1))
          case _ => changes(original, table, "reservation_revision", i(999))
        var forged = MySqlControlledDebugImages.put(m.executeTarget, table, altered, root, true)
        val receipt = if m eq p then None else Some(id(110))
        val target = MySqlNormalRecallOperationEffectCodec.projection(forged, m.header, MySqlOperationEffectPhase.Execute,
          MySqlOperationEffectImage.Target, receipt, root)
        val fp = MySqlNormalRecallOperationEffectCodec.fingerprint(m.header, MySqlOperationEffectPhase.Execute,
          m.plan.requestIdentity.canonical, m.scope, m.execute.beforeBytes, target, root)
        val context = MySqlNormalRecallOperationEffectCodec.context(m.header, m.plan.executeOperationUuid, fp)
        val contextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context, root.profile)
        def raw(id: java.util.UUID) = forged.relation("fp_recovery_operation").rows.find(r =>
          value(r, "fp_recovery_operation", "operation_uuid") == u(id)).get
        val reserve = raw(m.plan.reserveOperationUuid)
        val reserveRaw = MySqlRecoveryOperationRawEnvelopeCodec.encode(reserve, root.profile)
        val witness = MySqlNormalRecallWorkflowMaterializer.witness(m.header, MySqlOperationEffectPhase.Execute,
          m.plannedAt, m.scope, fp, contextBytes, m.execute.beforeBytes, target, Some(reserveRaw), root)
        val forgedRow = MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.phase(
          raw(m.plan.executeOperationUuid), contextBytes, m.execute.beforeBytes, target, fp, root), witness, m.plannedAt, root)
        forged = MySqlControlledDebugImages.put(forged, "fp_recovery_operation", forgedRow, root, true)
        forged = MySqlControlledDebugOperationRows.writer(forged, m.plan.executeOperationUuid, context.canonicalFingerprintSha256, root)
        receipt.foreach { _ =>
          val completion = forged.relation("fp_lifecycle_completion").rows.head
          val corrected = changes(changes(completion, "fp_lifecycle_completion", "original_context_bytes", contextBytes.binaryCell),
            "fp_lifecycle_completion", "original_context_sha256", contextBytes.sha256Cell)
          forged = MySqlControlledDebugImages.put(forged, "fp_lifecycle_completion", corrected, root, true)
        }
        // 不是故意破坏hash：scope/request/context/完整witness及RAW解析全部先真实通过。
        val checked = MySqlNormalRecallRecovery.verifyPersistedPhase(forgedRow, Some(reserve), root)
        val resolved = MySqlNormalRecallRecovery.resolveCommittedEffectData(checked, forged, root)
        assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallRecovery.requireCommittedEffectDelta(resolved, root))
        if m ne p then
          val pending = MySqlControlledDebugRows.update(forgedRow, "fp_recovery_operation", Map(
            "operation_state" -> t("RESERVED"), "commit_witness_bytes" -> MySqlFormat3Cell.SqlNull,
            "commit_witness_sha256" -> MySqlFormat3Cell.SqlNull, "result_at" -> MySqlFormat3Cell.SqlNull), root)
          val actualPending = MySqlControlledDebugImages.put(m.reserveTarget, "fp_recovery_operation", pending, root, true)
          val pendingPhase = MySqlNormalRecallRecovery.verifyPersistedPhase(pending, Some(reserve), root)
          // 自洽RAW/context/fingerprint的恶意future target仍须在独立delta处被拒绝。
          val failure = assertThrows(classOf[IllegalArgumentException], () =>
            MySqlNormalRecallRecovery.resolvePendingCheckpointImages(pendingPhase, actualPending, root))
          assertTrue(failure.getMessage.contains("full20 target"))
      finally p.root.closeRootReadBudget()
    }

  @Test def latestReceiptDataRequiresWholeInventoryAndFiniteOldWriterLifecycle(): Unit =
    for checkpointPhase <- Vector(false, true); reservePhase <- Vector(false, true) do
      val p = build(fixture())
      try
        val m = if checkpointPhase then checkpoint(p) else p
        val phase = if reservePhase then m.reserve else m.execute
        val context = phase.operationContext
        val fixed = m.header.context.fixedLeaseUntil
        val now = fixed.plusHours(3)
        val phaseTarget = phase.target match
          case legacy: MySqlFormat3FullSnapshot => legacy
          case _ => throw new AssertionError("legacy receipt lifecycle fixture must retain its Full20 target")
        def current(owner: Option[java.util.UUID], lease: LocalDateTime, epoch: Long = 3L) =
          val old = phaseTarget.relation("fp_runtime_writer").rows.head
          val changed = MySqlControlledDebugRows.update(old, "fp_runtime_writer", Map(
            "owner_boot_uuid" -> owner.map(u).getOrElse(MySqlFormat3Cell.SqlNull),
            "writer_epoch" -> i(epoch), "lease_until" -> MySqlFormat3Cell.DateTime(lease)), m.root)
          val image = MySqlControlledDebugImages.put(phaseTarget, "fp_runtime_writer", changed, m.root, true)
          val w = MySqlBusinessWriterRow(id(2), epoch, owner, lease, Some(context.operationUuid), Some(context.canonicalFingerprintSha256), now)
          (image, w)
        Vector((Some(id(3)), fixed), (Some(id(3)), fixed.plusHours(1)), (None, fixed.minusSeconds(1))).foreach { (owner, lease) =>
          val (image, w) = current(owner, lease)
          assertEquals(context, MySqlNormalRecallRecovery.verifyLatestReceiptData(image, w, id(3), 3L, m.root).phase.context)
        }
        Vector((Some(id(99)), fixed, 3L), (Some(id(3)), fixed.minusSeconds(1), 3L),
          (Some(id(3)), now.plusSeconds(1), 3L), (Some(id(3)), fixed, 4L)).foreach { (owner, lease, epoch) =>
          val (image, w) = current(owner, lease, epoch)
          assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallRecovery.verifyLatestReceiptData(image, w, id(3), 3L, m.root))
        }
        val (image, w) = current(Some(id(3)), fixed)
        val entitlement = image.relation("player_entitlement").rows.head
        val foreignImage = MySqlControlledDebugImages.put(image, "player_entitlement",
          changes(entitlement, "player_entitlement", "registration_limit", i(255)), m.root, true)
        assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallRecovery.verifyLatestReceiptData(foreignImage, w, id(3), 3L, m.root))
      finally p.root.closeRootReadBudget()

  @Test def receiptHistoryReachesFullAnchorAcrossRenewalAndEveryOriginalPhase(): Unit =
    val input = fixture()
    val root = input.snapshot.allocationBudget
    try
      val w = input.snapshot.relation("fp_runtime_writer").rows.head
      val anchor = MySqlControlledDebugImages.put(input.snapshot, "fp_runtime_writer",
        changes(w, "fp_runtime_writer", "lease_until", MySqlFormat3Cell.DateTime(at.plusMinutes(30))), root, true)
      val p = build(input)
      val c = checkpoint(p)
      Vector(p.reserveTarget, p.executeTarget, c.reserveTarget, c.executeTarget).zipWithIndex.foreach { (actual, count) =>
        val result = MySqlNormalRecallRecovery.verifyReceiptHistoryData(actual, anchor, root)
        assertSame(anchor, result.anchor)
        assertEquals(count + 1, result.newestFirst.size)
        assertEquals(p.reserve.operationContext, result.newestFirst.last.context)
      }
      val changed = changes(c.executeTarget.relation("fp_runtime_writer").rows.head, "fp_runtime_writer",
        "lease_until", MySqlFormat3Cell.DateTime(at.plusHours(2)))
      val renewed = MySqlControlledDebugImages.put(c.executeTarget, "fp_runtime_writer", changed, root, true)
      assertEquals(4, MySqlNormalRecallRecovery.verifyReceiptHistoryData(renewed, anchor, root).newestFirst.size)
      root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "history test cumulative root exhaustion")
      assertThrows(classOf[SQLDataException], () => MySqlNormalRecallRecovery.verifyReceiptHistoryData(renewed, anchor, root))
    finally root.closeRootReadBudget()

  @Test def receiptHistoryPreservesOtherPodSuccessorsAndAllUnrelatedRows(): Unit =
    twoPodHistory(false)

  @Test def stoppedSegmentsCrossEpochWithoutRemovingLaterOriginalInventoryRows(): Unit =
    twoPodHistory(true)

  private def twoPodHistory(crossEpoch: Boolean): Unit =
    val input = fixture()
    val root = input.snapshot.allocationBudget
    try
      var anchor = input.snapshot
      // A second actual DATA Pod/attempt/F exists before either workflow. All other
      // original rows remain in both complete graphs; no per-Pod projection is used.
      input.snapshot.relations.foreach { relation => relation.rows.filter(_.contains(u(id(10)))).foreach { original =>
        val changed = original.map {
          case v if v == u(id(10)) => u(id(210))
          case v if v == u(id(20)) => u(id(220))
          case v if v == u(id(30)) => u(id(230))
          case v if v == u(id(40)) => u(id(240))
          case v => v
        }
        anchor = MySqlControlledDebugImages.put(anchor, relation.table, changed, root, false)
      } }
      val originalHistory = input.snapshot.relation("fp_recovery_operation").rows.find(r =>
        value(r, "fp_recovery_operation", "operation_uuid") == u(id(40))).get
      anchor = MySqlControlledDebugImages.put(anchor, "fp_recovery_operation",
        changes(originalHistory, "fp_recovery_operation", "operation_uuid", u(id(240))), root, false)
      val p = build(input.copy(snapshot = anchor))
      val c = checkpoint(p)
      val oldW = c.executeTarget.relation("fp_runtime_writer").rows.head
      val beforeHandoff = if crossEpoch then MySqlControlledDebugImages.put(c.executeTarget, "fp_runtime_writer",
        changes(changes(oldW, "fp_runtime_writer", "owner_boot_uuid", MySqlFormat3Cell.SqlNull),
          "fp_runtime_writer", "lease_until", MySqlFormat3Cell.DateTime(at.plusSeconds(5))), root, true) else c.executeTarget
      val nextLease = if crossEpoch then at.plusHours(2) else input.context.fixedLeaseUntil
      val nextBoot = if crossEpoch then id(4) else id(3)
      val nextEpoch = if crossEpoch then 4L else 3L
      var epochAnchor = beforeHandoff
      if crossEpoch then
        val w = beforeHandoff.relation("fp_runtime_writer").rows.head
        val nextW = changes(changes(changes(w, "fp_runtime_writer", "owner_boot_uuid", u(nextBoot)),
          "fp_runtime_writer", "writer_epoch", i(nextEpoch)), "fp_runtime_writer", "lease_until", MySqlFormat3Cell.DateTime(nextLease))
        epochAnchor = MySqlControlledDebugImages.put(beforeHandoff, "fp_runtime_writer", nextW, root, true)
        // A new boot's actual SQL observation has its own header; historical rows keep their original scopes.
        epochAnchor = new MySqlFormat3FullSnapshot(epochAnchor.schema, epochAnchor.connection,
          MySqlFormat3ObservationScope(nextBoot, 2L, 2L), epochAnchor.maxAllowedPacketBytes,
          root.profile, epochAnchor.relations, root)
        MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(beforeHandoff, epochAnchor)
      val request = input.request.copy(stopRequest = id(300), criticalBase = input.request.criticalBase.copy(uuid = id(210)),
        attemptUuid = id(220), footprintUuid = id(230))
      val tag = IoOperationTag(nextBoot, if crossEpoch then 2L else 1L, if crossEpoch then 2L else 1L, id(301))
      val plan = MySqlOwnedRepositoryOperationPlan.single(tag, id(302), id(303),
        MySqlRepositoryOperation.PrepareNormalRecall(request), root.profile)
      val old = input.context
      val context = new MySqlRepositoryOperationContextIdentity(tag, plan.workflowUuid, plan.reserveOperationUuid,
        if crossEpoch then id(404) else old.barrierGenerationUuid, if crossEpoch then 2L else old.actualConnectionId,
        old.barrierNamespace, old.bindingUuid, nextEpoch,
        nextLease, old.serverUuid, old.serverVersion, old.schema, old.effectiveUser, old.provisioningUuid,
        old.schemaFormat, old.physicalFormat, old.ddlSha256, old.approvedObjectManifestSha256, old.configurationSha256,
        old.sourceProvenanceKind, old.sourceProvenanceSha256, old.limitProfileSha256, "RUN", plan.requestIdentitySha256, root.profile)
      val pOther = MySqlNormalRecallWorkflowMaterializer.prepareInitial(request, plan, context, at.plusSeconds(7), epochAnchor, root)
      val cOther = checkpoint(pOther, 200)
      val segments = if crossEpoch then
        val oldStopped = MySqlBusinessWriterRow(id(2), 3L, None, at.plusSeconds(5), Some(c.execute.operationContext.operationUuid),
          Some(c.execute.operationContext.canonicalFingerprintSha256), at.plusSeconds(6))
        val latestStopped = MySqlBusinessWriterRow(id(2), 4L, Some(nextBoot), nextLease, Some(cOther.execute.operationContext.operationUuid),
          Some(cOther.execute.operationContext.canonicalFingerprintSha256), at.plusHours(3))
        val newer = MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(cOther.executeTarget, cOther.executeTarget, epochAnchor, latestStopped, root)
        val older = MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(cOther.executeTarget, beforeHandoff, anchor, oldStopped, root)
        assertEquals(4, newer.newestFirst.size); assertEquals(4, older.newestFirst.size)
        assertTrue(cOther.executeTarget.relation("fp_recovery_operation").rows.size > beforeHandoff.relation("fp_recovery_operation").rows.size)
        Vector(newer, older)
      else Vector(MySqlNormalRecallRecovery.verifyReceiptHistoryData(cOther.executeTarget, anchor, root))
      val phases = segments.flatMap(_.newestFirst)
      assertEquals(Vector(id(311), id(312), id(301), id(303), id(111), id(112), id(101), id(103)),
        phases.map(_.context.operationUuid))
      assertEquals(Set(id(10), id(210)), phases.map(_.recall.get.phase.header.pod).toSet)
      val completeActual = cOther.executeTarget.relations
      val oldInventory = MySqlNormalRecallRecovery.discover(cOther.executeTarget, id(2)).find(_.pod == id(10)).get
      val oldDecision = MySqlNormalRecallRecovery.classifyCheckpointData(cOther.executeTarget, segments, oldInventory, root)
      assertEquals(MySqlNormalRecallCheckpointObservation.Committed, oldDecision.observation)
      assertEquals(id(111), oldDecision.committed.get.phase.context.operationUuid)
      assertEquals(completeActual, cOther.executeTarget.relations)
      val entitlement = cOther.executeTarget.relation("player_entitlement").rows.head
      val extra = MySqlControlledDebugImages.put(cOther.executeTarget, "player_entitlement",
        changes(entitlement, "player_entitlement", "registration_limit", i(255)), root, true)
      val failure = assertThrows(classOf[IllegalArgumentException], () =>
        MySqlNormalRecallRecovery.verifyReceiptHistoryData(extra, if crossEpoch then epochAnchor else anchor, root))
      assertTrue(failure.getMessage.contains("target/next-before"))
    finally root.closeRootReadBudget()

  @Test def receiptHistoryRejectsChangedWriterBrokenAnchorAndMissingOriginal(): Unit =
    val input = fixture()
    val root = input.snapshot.allocationBudget
    try
      val p = build(input)
      val c = checkpoint(p)
      val w = c.executeTarget.relation("fp_runtime_writer").rows.head
      Vector("writer_epoch" -> i(4), "owner_boot_uuid" -> u(id(99)), "owner_boot_uuid" -> MySqlFormat3Cell.SqlNull,
        "lease_until" -> MySqlFormat3Cell.DateTime(at), "last_commit_operation_uuid" -> u(id(999)),
        "last_commit_fingerprint" -> binary(Array.fill[Byte](32)(99))).foreach { (column, next) =>
        val bad = MySqlControlledDebugImages.put(c.executeTarget, "fp_runtime_writer", changes(w, "fp_runtime_writer", column, next), root, true)
        assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.verifyReceiptHistoryData(bad, input.snapshot, root))
      }
      val entitlement = input.snapshot.relation("player_entitlement").rows.head
      val wrongAnchor = MySqlControlledDebugImages.put(input.snapshot, "player_entitlement",
        changes(entitlement, "player_entitlement", "registration_limit", i(255)), root, true)
      assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.verifyReceiptHistoryData(c.executeTarget, wrongAnchor, root))
      val relations = c.executeTarget.relations.map(r => if r.table == "fp_recovery_operation" then
        r.copy(rows = r.rows.filterNot(row => value(row, r.table, "operation_uuid") == u(id(103)))) else r)
      val missing = new MySqlFormat3FullSnapshot(c.executeTarget.schema, c.executeTarget.connection,
        c.executeTarget.observationScope, c.executeTarget.maxAllowedPacketBytes, root.profile, relations, root)
      assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.verifyReceiptHistoryData(missing, input.snapshot, root))
    finally root.closeRootReadBudget()

  @Test def stoppedTerminalAllowsOneExactReleaseOrExpiryButNeverAnEpochOrReceiptMask(): Unit =
    val input = fixture(); val root = input.snapshot.allocationBudget
    try
      val p = build(input); val c = checkpoint(p)
      val context = c.execute.operationContext
      val stopped = MySqlBusinessWriterRow(id(2), 3L, None, at.plusSeconds(5), Some(context.operationUuid),
        Some(context.canonicalFingerprintSha256), at.plusHours(2))
      def image(base: MySqlFormat3FullSnapshot, w: MySqlBusinessWriterRow): MySqlFormat3FullSnapshot =
        val old = base.relation("fp_runtime_writer").rows.head
        val values = Vector("binding_uuid" -> u(w.bindingUuid), "writer_epoch" -> i(w.writerEpoch),
          "owner_boot_uuid" -> w.ownerBootUuid.map(u).getOrElse(MySqlFormat3Cell.SqlNull),
          "lease_until" -> MySqlFormat3Cell.DateTime(w.leaseUntil),
          "last_commit_operation_uuid" -> w.lastOperationUuid.map(u).getOrElse(MySqlFormat3Cell.SqlNull),
          "last_commit_fingerprint" -> w.lastFingerprint.map(v => binary(v.toArray)).getOrElse(MySqlFormat3Cell.SqlNull))
        val changed = values.foldLeft(old)((r, v) => changes(r, "fp_runtime_writer", v._1, v._2))
        MySqlControlledDebugImages.put(base, "fp_runtime_writer", changed, root, true)
      val released = image(c.executeTarget, stopped)
      assertEquals(4, MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(released, released, input.snapshot, stopped, root).newestFirst.size)
      assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.verifyReceiptHistoryData(released, input.snapshot, root))
      val expiredAtEquality = stopped.copy(databaseNow = stopped.leaseUntil)
      val equalityImage = image(c.executeTarget, expiredAtEquality)
      assertEquals(4, MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(
        equalityImage, equalityImage, input.snapshot, expiredAtEquality, root).newestFirst.size)
      val renewed = stopped.copy(ownerBootUuid = Some(id(3)), leaseUntil = at.plusMinutes(90))
      val expired = image(c.executeTarget, renewed)
      assertEquals(4, MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(expired, expired, input.snapshot, renewed, root).newestFirst.size)
      val zeroWriter = stopped.copy(lastOperationUuid = None, lastFingerprint = None)
      val zero = image(input.snapshot, zeroWriter)
      assertTrue(MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(c.executeTarget, zero, input.snapshot, zeroWriter, root).newestFirst.isEmpty)
      Vector(stopped.copy(ownerBootUuid = Some(id(99))), stopped.copy(writerEpoch = 4L),
        stopped.copy(lastOperationUuid = Some(id(999))), stopped.copy(lastFingerprint = Some(Vector.fill(32)(7.toByte))),
        stopped.copy(databaseNow = stopped.leaseUntil.minusNanos(1000L)),
        stopped.copy(ownerBootUuid = Some(id(3)), leaseUntil = at)).foreach { wrong =>
        val bad = image(c.executeTarget, wrong)
        assertThrows(classOf[Exception], () =>
          MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(bad, bad, input.snapshot, wrong, root))
      }
      val entitlement = released.relation("player_entitlement").rows.head
      val unrelated = MySqlControlledDebugImages.put(released, "player_entitlement",
        changes(entitlement, "player_entitlement", "registration_limit", i(255)), root, true)
      assertThrows(classOf[Exception], () =>
        MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(unrelated, unrelated, input.snapshot, stopped, root))
      root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "stopped segment cumulative budget exhaustion")
      assertThrows(classOf[Exception], () =>
        MySqlNormalRecallRecovery.verifyStoppedReceiptHistoryData(released, released, input.snapshot, stopped, root))
    finally root.closeRootReadBudget()

  @Test def persistedPhaseDataChecksOriginalRequestContextAndCompleteWitness(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      Vector(p, c).foreach { m =>
        def raw(snapshot: MySqlFormat3FullSnapshot, id: java.util.UUID) = snapshot.relation("fp_recovery_operation").rows.find(r =>
          value(r, "fp_recovery_operation", "operation_uuid") == u(id)).get
        val reserve = raw(m.reserveTarget, m.plan.reserveOperationUuid)
        val pending = raw(m.reserveTarget, m.plan.executeOperationUuid)
        val committed = raw(m.executeTarget, m.plan.executeOperationUuid)
        assertEquals(m.reserve.operationContext, MySqlNormalRecallRecovery.verifyPersistedPhase(reserve, None, m.root).context)
        assertEquals(m.execute.operationContext, MySqlNormalRecallRecovery.verifyPersistedPhase(pending, Some(reserve), m.root).context)
        val verified = MySqlNormalRecallRecovery.verifyPersistedPhase(committed, Some(reserve), m.root)
        assertEquals(m.execute.operationContext, verified.context)
        assertEquals(m.header.context.tag, verified.header.scope.tag)
        assertEquals(20, verified.beforeEffect.relations.size)
        assertEquals(20, verified.targetEffect.relations.size)
        val foreign = changes(reserve, "fp_recovery_operation", "scope_bytes", p.scope.binaryCell)
        if m eq c then assertThrows(classOf[Exception], () => MySqlNormalRecallRecovery.verifyPersistedPhase(pending, Some(foreign), m.root))
        val hashValidButWrongRequest = changes(changes(committed, "fp_recovery_operation", "request_identity_bytes", m.scope.binaryCell),
          "fp_recovery_operation", "request_identity_sha256", m.scope.sha256Cell)
        assertThrows(classOf[IllegalArgumentException], () =>
          MySqlNormalRecallRecovery.verifyPersistedPhase(hashValidButWrongRequest, Some(reserve), m.root))
        val alteredWitness = m.execute.witness.copyBytes(m.root, "witness corruption")
        alteredWitness(alteredWitness.length - 1) = (alteredWitness.last ^ 1).toByte
        val corrupt = changes(changes(committed, "fp_recovery_operation", "commit_witness_bytes", binary(alteredWitness)),
          "fp_recovery_operation", "commit_witness_sha256", binary(java.security.MessageDigest.getInstance("SHA-256").digest(alteredWitness)))
        assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallRecovery.verifyPersistedPhase(corrupt, Some(reserve), m.root))
      }
    finally p.root.closeRootReadBudget()

  @Test def strictRecallDataRejectsWrongDomainsTruncationTailAndScopeOverflow(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      Vector((p.scope, false), (c.scope, true)).foreach { (encoded, isC) =>
        val bytes = encoded.copyBytes(p.root, "P/C negative fixture")
        def rejects(changed: Array[Byte]): Unit =
          // 每个失败解码只借原共享ledger的comparison子预算，输入仍由本fixture保留。
          MySqlRecoveryCanonicalBytes.withComparisonBudget(p.root) {
            assertThrows(classOf[Exception], () => {
              val binary = MySqlFormat3Binary.takeOwned(changed)
              if isC then MySqlNormalRecallCanonical.decodeCheckpoint(binary, p.root)
              else MySqlNormalRecallCanonical.decodePrepare(binary, p.root)
              ()
            })
          }
        for length <- Vector(0, 1, 4, bytes.length / 2, bytes.length - 1) do rejects(bytes.take(length))
        rejects(bytes :+ 0.toByte)
        val domain = bytes.clone(); domain(4) = 'X'.toByte; rejects(domain)
        val scopeLength = 4 + encoded.encoding.length + 16 * 4 + 8 * 4
        for length <- Vector(-1, Int.MaxValue) do
          val overflow = bytes.clone(); java.nio.ByteBuffer.wrap(overflow).putInt(scopeLength, length); rejects(overflow)
      }
    finally p.root.closeRootReadBudget()

  @Test def strictCheckpointRejectsNestedSourceChangeEvenWithRecomputedNestedDigest(): Unit =
    val p = build(fixture())
    try
      val c = checkpoint(p)
      val original = c.scope.copyBytes(p.root, "nested P identity mutation")
      val nestedP = p.scope.copyBytes(p.root, "nested P location")
      val start = original.indices.find(i => i + nestedP.length <= original.length &&
        nestedP.indices.forall(j => original(i + j) == nestedP(j))).get
      val headerStart = start + 4 + MySqlNormalRecallCanonical.PrepareDomain.length
      val scopeStart = headerStart + 16 * 4 + 8 * 4 + 4
      // scope: boot, boot/connection generations, operation, workflow, reserve, barrier UUID.
      val barrierLastByte = scopeStart + 16 + 8 + 8 + 16 + 16 + 16 + 15
      original(barrierLastByte) = (original(barrierLastByte) ^ 1).toByte
      val hash = java.security.MessageDigest.getInstance("SHA-256").digest(java.util.Arrays.copyOfRange(original, start, start + nestedP.length))
      System.arraycopy(hash, 0, original, start + nestedP.length, hash.length)
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlNormalRecallCanonical.decodeCheckpoint(MySqlFormat3Binary.takeOwned(original), p.root))
    finally p.root.closeRootReadBudget()

  @Test def completeInventoryDiscoversOriginalRecallBeforeBusinessClaim(): Unit =
    val input = fixture()
    val root = input.snapshot.allocationBudget
    try
      assertTrue(MySqlNormalRecallRecovery.discover(input.snapshot, id(2)).isEmpty)
      val p = build(input)
      val reserved = MySqlNormalRecallRecovery.discover(p.reserveTarget, id(2))
      assertEquals(1, reserved.size)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.ReconcileOriginal], reserved.head.cursor)
      val committed = MySqlNormalRecallRecovery.discover(p.executeTarget, id(2))
      assertEquals(input.plan.workflowUuid, committed.head.workflow)
      assertEquals(input.request.podUuid, committed.head.pod)
      assertEquals(input.plan.executeOperationUuid, committed.head.prepare)
      assertTrue(committed.head.checkpoint.isEmpty)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.VerifyUnsubmittedCheckpoint], committed.head.cursor)
      val c = checkpoint(p)
      val after = MySqlNormalRecallRecovery.discover(c.executeTarget, id(2))
      assertEquals(Some(c.plan.executeOperationUuid), after.head.checkpoint)
      assertInstanceOf(classOf[MySqlNormalRecallRecoveryCursor.VerifyCommittedCheckpoint], after.head.cursor)
    finally root.closeRootReadBudget()

  @Test def completeInventoryRejectsForeignBindingRatherThanReturningEmpty(): Unit =
    val input = fixture()
    try
      val p = build(input)
      assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallRecovery.discover(p.executeTarget, id(999)))
    finally input.snapshot.allocationBudget.closeRootReadBudget()

  private def activeRecovery(m: MySqlNormalRecallMaterialization, before: MySqlModeOperationEffectCodec.Image,
      request: MySqlNormalRecallRequest): MySqlNormalRecallMaterialization =
    val c = m.header.context
    val tag = IoOperationTag(c.tag.bootId,c.tag.bootGeneration,c.tag.connectionGeneration,id(121))
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag,m.plan.workflowUuid,id(122),
      MySqlRepositoryOperation.PrepareNormalRecall(request),m.root.profile)
    val context = new MySqlRepositoryOperationContextIdentity(tag,plan.workflowUuid,plan.reserveOperationUuid,
      c.barrierGenerationUuid,c.actualConnectionId,c.barrierNamespace,c.bindingUuid,c.writerEpoch,
      c.fixedLeaseUntil,c.serverUuid,c.serverVersion,c.schema,c.effectiveUser,c.provisioningUuid,
      c.schemaFormat,c.physicalFormat,c.ddlSha256,c.approvedObjectManifestSha256,c.configurationSha256,
      c.sourceProvenanceKind,c.sourceProvenanceSha256,c.limitProfileSha256,"RUN",plan.requestIdentitySha256,m.root.profile)
    MySqlNormalRecallWorkflowMaterializer.prepareActiveRecoveryWithoutCheckpoint(request,plan,context,at.plusSeconds(2),
      before,m.plan.executeOperationUuid,m.root)

  private def committedInactiveRequest(c: MySqlNormalRecallMaterialization): MySqlNormalRecallRequest =
    val original=c.header.request
    val pose=original.finalPose
    val base=original.criticalBase.copy(stateVersion=Math.incrementExact(original.criticalBase.stateVersion),
      isActive=false,posX=pose.posX,posY=pose.posY,posZ=pose.posZ,yaw=pose.yaw,pitch=pose.pitch,
      lastUpdated=c.plannedAt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli)
    original.copy(criticalBase=base,finalPose=pose,purpose=MySqlNormalRecallPurpose.FinishCommittedInactive)

  private def inactiveRecovery(p: MySqlNormalRecallMaterialization, c: MySqlNormalRecallMaterialization,
      before: MySqlModeOperationEffectCodec.Image, request: MySqlNormalRecallRequest): MySqlNormalRecallMaterialization =
    val old=c.header.context
    val tag=IoOperationTag(old.tag.bootId,old.tag.bootGeneration,old.tag.connectionGeneration,id(131))
    val plan=MySqlOwnedRepositoryOperationPlan.single(tag,c.plan.workflowUuid,id(132),
      MySqlRepositoryOperation.PrepareNormalRecall(request),c.root.profile)
    val context=new MySqlRepositoryOperationContextIdentity(tag,plan.workflowUuid,plan.reserveOperationUuid,
      old.barrierGenerationUuid,old.actualConnectionId,old.barrierNamespace,old.bindingUuid,old.writerEpoch,
      old.fixedLeaseUntil,old.serverUuid,old.serverVersion,old.schema,old.effectiveUser,old.provisioningUuid,
      old.schemaFormat,old.physicalFormat,old.ddlSha256,old.approvedObjectManifestSha256,old.configurationSha256,
      old.sourceProvenanceKind,old.sourceProvenanceSha256,old.limitProfileSha256,"RUN",plan.requestIdentitySha256,c.root.profile)
    MySqlNormalRecallWorkflowMaterializer.prepareInactiveRecovery(request,plan,context,at.plusSeconds(3),before,
      p.plan.executeOperationUuid,c.plan.executeOperationUuid,c.root)

  private def checkpoint(m: MySqlNormalRecallMaterialization, offset: Int = 0): MySqlNormalRecallMaterialization =
    import MySqlModeOperationEffectCodec.relation
    val historicalPayloadBytes = m.completeExecuteTarget.relation("fp_recovery_operation").rows.iterator.flatMap(_.iterator)
      .collect { case MySqlFormat3Cell.Binary(bytes) => bytes.length.toLong }.sum
    println(s"Recall original P: reserveBefore=${m.reserve.beforeBytes.length}, reserveTarget=${m.reserve.targetBytes.length}, " +
      s"executeBefore=${m.execute.beforeBytes.length}, executeTarget=${m.execute.targetBytes.length}, " +
      s"reserveWitness=${m.reserve.witness.length}, executeWitness=${m.execute.witness.length}, historyBinaryBytes=$historicalPayloadBytes")
    val removal = MySqlNormalRecallRemovalFacts(m.plan.tag, m.header.request.podUuid, m.header.request.attemptUuid,
      m.header.request.criticalBase.stateVersion, 0, 77, 20L, 21L, 22L)
    val ticket = MySqlNormalRecallTicketFacts(removal, m.header.request.stopRequest, m.header.request.originalFootprint)
    val request = MySqlNormalRecallCheckpointRequest(m.header.request, removal, ticket, id(110 + offset))
    val c = m.header.context
    val tag = IoOperationTag(c.tag.bootId, c.tag.bootGeneration, c.tag.connectionGeneration, id(111 + offset))
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, m.plan.workflowUuid, id(112 + offset),
      MySqlRepositoryOperation.CheckpointNormalRecall(request), m.root.profile)
    val context = new MySqlRepositoryOperationContextIdentity(tag, plan.workflowUuid, plan.reserveOperationUuid,
      c.barrierGenerationUuid, c.actualConnectionId, c.barrierNamespace, c.bindingUuid, c.writerEpoch,
      c.fixedLeaseUntil, c.serverUuid, c.serverVersion, c.schema, c.effectiveUser, c.provisioningUuid,
      c.schemaFormat, c.physicalFormat, c.ddlSha256, c.approvedObjectManifestSha256, c.configurationSha256,
      c.sourceProvenanceKind, c.sourceProvenanceSha256, c.limitProfileSha256, "RUN", plan.requestIdentitySha256, m.root.profile)
    val result = MySqlNormalRecallWorkflowMaterializer.checkpoint(request, m, plan, context, at.plusSeconds(1 + offset), m.completeExecuteTarget, m.root)
    println(s"Recall C: reserveBefore=${result.reserve.beforeBytes.length}, reserveTarget=${result.reserve.targetBytes.length}, " +
      s"executeBefore=${result.execute.beforeBytes.length}, executeTarget=${result.execute.targetBytes.length}, " +
      s"reserveWitness=${result.reserve.witness.length}, executeWitness=${result.execute.witness.length}, " +
      s"rootAccounted=${m.root.accountedMemoryBytes}, packetLimit=${m.root.profile.maxSqlPacketBytes()}")
    result

  @Test def checkpointPersistsOriginalAckAndFinalPoseWithoutClaimingDiskAbsence(): Unit =
    val p = build(fixture())
    val c = checkpoint(p)
    val domain = c.executeTarget.relation("floating_pod_state").rows.head
    assertEquals(i(0), value(domain, "floating_pod_state", "is_active"))
    assertEquals(i(6), value(domain, "floating_pod_state", "state_version"))
    assertEquals(f(p.header.request.finalPose.posY), value(domain, "floating_pod_state", "pos_y"))
    val completion = c.executeTarget.relation("fp_lifecycle_completion").rows.head
    assertEquals(c.execute.contextBytes.binaryCell, value(completion, "fp_lifecycle_completion", "original_context_bytes"))
    assertEquals(c.execute.contextBytes.sha256Cell, value(completion, "fp_lifecycle_completion", "original_context_sha256"))
    assertEquals(t(nativeId), value(completion, "fp_lifecycle_completion", "source_identity"))
    assertEquals(t("PENDING"), value(c.executeTarget.relation("physical_recovery_obligation").rows.head,
      "physical_recovery_obligation", "obligation_state"))
    assertEquals(t("RELEASED"), value(c.executeTarget.relation("fp_recovery_reservation").rows.head,
      "fp_recovery_reservation", "reservation_state"))
    Vector("physical_recovery_footprint", "physical_recovery_footprint_chunk", "physical_recovery_intent", "physical_source_unit",
      "physical_source_forest", "physical_source_transfer", "physical_source_write", "physical_source_obligation")
      .foreach(table => assertEquals(p.preimage.relation(table), c.executeTarget.relation(table)))
    assertTrue(MySqlNormalRecallRecovery.inspect(c.executeTarget, c.header.context.bindingUuid, c.plan.workflowUuid,
      c.header.request.podUuid, p.plan.executeOperationUuid, Some(c.plan.executeOperationUuid))
      .isInstanceOf[MySqlNormalRecallRecoveryCursor.VerifyCommittedCheckpoint])

  @Test def completionContextAndPhysicalPayloadCannotCertifyTheirOwnTampering(): Unit =
    val c = checkpoint(build(fixture()))
    val table = "fp_lifecycle_completion"
    val completion = c.executeTarget.relation(table).rows.head
    Vector("original_context_bytes", "removal_result_bytes", "ticket_result_bytes", "checkpoint_bytes").foreach { name =>
      val changed = MySqlControlledDebugRows.update(completion, table, Map(name -> binary(Array[Byte](9, 8, 7))), c.root)
      val actual = MySqlControlledDebugImages.put(c.executeTarget, table, changed, c.root, true)
      assertThrows(classOf[SQLDataException], () => c.execute.requireObserved(actual, true), name)
    }
    c.verify()

  @Test def prepareKeepsDomainAndAllPhysicalHistoryAndHoldsReservation(): Unit =
    val m = build(fixture())
    m.preimage.relations.filterNot(r => Set("fp_recovery_operation", "fp_recovery_reservation", "fp_runtime_writer").contains(r.table))
      .foreach(r => assertEquals(r, m.executeTarget.relation(r.table), r.table))
    val reservation = m.executeTarget.relation("fp_recovery_reservation").rows.head
    assertEquals(t("HELD"), value(reservation, "fp_recovery_reservation", "reservation_state"))
    assertEquals(u(m.plan.executeOperationUuid), value(reservation, "fp_recovery_reservation", "last_mutation_operation_uuid"))
    val cursor = MySqlNormalRecallRecovery.inspect(m.executeTarget, m.header.context.bindingUuid,
      m.plan.workflowUuid, m.header.request.podUuid, m.plan.executeOperationUuid, None)
    assertTrue(cursor.isInstanceOf[MySqlNormalRecallRecoveryCursor.VerifyUnsubmittedCheckpoint])
    assertTrue(m.executeTarget.relation("fp_lifecycle_completion").rows.isEmpty)

  @Test def independentlyDerivedCellsDoNotHideChangedReadback(): Unit =
    val m = build(fixture())
    val writer = m.executeTarget.relation("fp_runtime_writer").rows.head
    val corrupted = MySqlControlledDebugRows.update(writer, "fp_runtime_writer",
      Map("last_commit_fingerprint" -> binary(Array.fill[Byte](32)(7))), m.root)
    val actual = MySqlControlledDebugImages.put(m.executeTarget, "fp_runtime_writer", corrupted, m.root, true)
    assertThrows(classOf[SQLDataException], () => m.execute.requireObserved(actual, true))
    m.verify()

  @Test def originalRequestAndCompleteFrozenBaseCannotDrift(): Unit =
    val input = fixture()
    val changedRequest = input.request.copy(dirtyRevision = input.request.dirtyRevision + 1)
    assertThrows(classOf[IllegalArgumentException], () => build(input.copy(request = changedRequest)))
    val old = input.snapshot.relation("floating_pod_state").rows.head
    val changed = MySqlControlledDebugRows.update(old, "floating_pod_state", Map("pos_y" -> f(99)), input.snapshot.allocationBudget)
    val actual = MySqlControlledDebugImages.put(input.snapshot, "floating_pod_state", changed, input.snapshot.allocationBudget, true)
    assertThrows(classOf[SQLDataException], () => build(input.copy(snapshot = actual)))

  @Test def ownedPrepareRequiresOriginalPlanAndAdvancesBootEpoch(): Unit =
    val input = fixture()
    val command = MySqlOwnedIoCommand.PrepareNormalRecall(input.request, input.plan)
    assertSame(input.plan, command.requireRepositoryPlan(input.plan.tag, input.snapshot.limitProfile))
    assertTrue(MySqlBootReadEpoch.mayChangeRepository(command))
    val changed = MySqlOwnedIoCommand.PrepareNormalRecall(input.request.copy(dirtyRevision = 100), input.plan)
    assertThrows(classOf[IllegalArgumentException], () => changed.requireRepositoryPlan(input.plan.tag, input.snapshot.limitProfile))
    val foreignTag = input.plan.tag.copy(operationId = id(900))
    assertThrows(classOf[IllegalArgumentException], () => command.requireRepositoryPlan(foreignTag, input.snapshot.limitProfile))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlOwnedIoCommand.CheckpointNormalRecall(null, null, input.plan).requireRepositoryPlan(input.plan.tag, input.snapshot.limitProfile))

  @Test def bootProjectionRetainsOriginalAttemptAndFootprintWithoutGuessingFromChunks(): Unit =
    val input = fixture()
    val data = MySqlBootMainReadData.from(input.snapshot, input.snapshot.allocationBudget)
    assertEquals(Some((input.request.attemptUuid, input.request.footprintUuid)), data.activeAttemptFootprints.get(input.request.podUuid))

  @Test def recallUnknownAndCleanupUncertaintyKeepOriginalCauseAndNeverBecomeKnownFailure(): Unit =
    val m = build(fixture())
    val cause = new java.sql.SQLException("original recall commit acknowledgement lost")
    val unknown = new MySqlNormalRecallWorkflowBlockedException(MySqlNormalRecallWorkflowPhase.ExecuteCommitUnknown, cause)
    val cleanup = new MySqlTransactionCleanupUncertainException(m.execute.operationContext, cause)
    Vector(unknown, cleanup).foreach { original =>
      var recorded = false
      val propagated = assertThrows(classOf[Throwable], () =>
        MySqlNormalRecallExecuteFailureBoundary.afterReserve(primary =>
          MySqlNormalRecallExecuteFailureBoundary.shouldRecordKnownFailure(true, primary)) {
          throw original
        } { primary =>
          recorded = true
          new MySqlNormalRecallWorkflowBlockedException(MySqlNormalRecallWorkflowPhase.ExecuteFailedAfterReserve, primary)
        })
      assertSame(original, propagated)
      assertSame(cause, propagated.getCause)
      assertFalse(recorded)
    }

  @Test def recallPostReserveRecordingFailureRemainsSuppressedOnOriginalRefusal(): Unit =
    val original = new java.sql.SQLNonTransientConnectionException("original barrier changed before C")
    val secondary = new IllegalStateException("recording failed")
    val propagated = assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () =>
      MySqlNormalRecallExecuteFailureBoundary.afterReserve(primary =>
        MySqlNormalRecallExecuteFailureBoundary.shouldRecordKnownFailure(true, primary)) {
        throw original
      } { _ => throw secondary })
    assertSame(original, propagated)
    assertEquals(1, propagated.getSuppressed.length)
    assertSame(secondary, propagated.getSuppressed()(0))

  @Test def originalRootRevocationStopsReadback(): Unit =
    val m = build(fixture())
    m.root.closeRootReadBudget()
    assertThrows(classOf[SQLDataException], () => m.execute.requireObserved(m.executeTarget, true))

  @Test def historicalRawPayloadAndDigestChangesStillFailCompleteReadback(): Unit =
    val m = build(fixture())
    val table = "fp_recovery_operation"
    val old = m.executeTarget.relation(table).rows.find(r => value(r, table, "operation_uuid") == u(id(40))).get
    val bytes = Array[Byte](7, 8, 9)
    val changedPayload = MySqlControlledDebugRows.update(old, table, Map("target_bytes" -> binary(bytes)), m.root)
    val selfConsistent = MySqlControlledDebugRows.update(changedPayload, table,
      Map("target_sha256" -> binary(java.security.MessageDigest.getInstance("SHA-256").digest(bytes))), m.root)
    Vector(changedPayload, selfConsistent).foreach { changed =>
      val actual = MySqlControlledDebugImages.put(m.executeTarget, table, changed, m.root, true)
      assertThrows(classOf[SQLDataException], () => m.execute.requireObserved(actual, true))
    }

  @Test def descriptorSelectionNeverHidesCurrentOperationOrOtherRelations(): Unit =
    val m = build(fixture())
    val table = "fp_recovery_operation"
    val reserve = m.executeTarget.relation(table).rows.find(r => value(r, table, "operation_uuid") == u(m.plan.reserveOperationUuid)).get
    val execute = m.executeTarget.relation(table).rows.find(r => value(r, table, "operation_uuid") == u(m.plan.executeOperationUuid)).get
    assertFalse(MySqlNormalRecallOperationEffectCodec.historicalRow(table, reserve, m.header, MySqlOperationEffectPhase.Reserve))
    assertFalse(MySqlNormalRecallOperationEffectCodec.historicalRow(table, execute, m.header, MySqlOperationEffectPhase.Reserve))
    assertTrue(MySqlNormalRecallOperationEffectCodec.historicalRow(table, reserve, m.header, MySqlOperationEffectPhase.Execute))
    assertFalse(MySqlNormalRecallOperationEffectCodec.historicalRow(table, execute, m.header, MySqlOperationEffectPhase.Execute))
    assertFalse(MySqlNormalRecallOperationEffectCodec.historicalRow("fp_lifecycle_completion", Vector.empty, m.header, MySqlOperationEffectPhase.Execute))
    val reserved = MySqlControlledDebugRows.update(reserve, table, Map("operation_state" -> t("RESERVED")), m.root)
    assertFalse(MySqlNormalRecallOperationEffectCodec.historicalRow(table, reserved, m.header, MySqlOperationEffectPhase.Execute))

  @Test def descriptorRejectsEveryChangedByteTruncationTailNullAndUnknownColumn(): Unit =
    val m = build(fixture())
    val table = "fp_recovery_operation"
    val old = m.executeTarget.relation(table).rows.find(r => value(r, table, "operation_uuid") == u(id(40))).get
    MySqlNormalRecallHistoricalPayload.requireValid(old, m.root)
    val buffer = new java.io.ByteArrayOutputStream
    val output = new java.io.DataOutputStream(buffer)
    assertTrue(MySqlNormalRecallHistoricalPayload.writeDescriptor(output, old, "target_bytes"))
    val bytes = buffer.toByteArray
    MySqlNormalRecallHistoricalPayload.requireExactDescriptor(bytes, old, "target_bytes", m.root)
    bytes.indices.foreach { index =>
      val changed = bytes.clone()
      changed(index) = (changed(index) ^ 1).toByte
      assertThrows(classOf[SQLDataException], () => MySqlNormalRecallHistoricalPayload.requireExactDescriptor(changed, old, "target_bytes", m.root))
    }
    Vector(bytes.dropRight(1), bytes ++ Array[Byte](0)).foreach { changed =>
      assertThrows(classOf[SQLDataException], () => MySqlNormalRecallHistoricalPayload.requireExactDescriptor(changed, old, "target_bytes", m.root))
    }
    assertFalse(MySqlNormalRecallHistoricalPayload.writeDescriptor(output, old, "operation_fingerprint"))
    assertFalse(MySqlNormalRecallHistoricalPayload.writeDescriptor(output, old, "original_diagnostic_identity"))
    val badNull = MySqlControlledDebugRows.update(old, table,
      Map("original_diagnostic_sha256" -> binary(Array.fill[Byte](32)(1))), m.root)
    assertThrows(classOf[SQLDataException], () => MySqlNormalRecallHistoricalPayload.requireValid(badNull, m.root))

  @Test def descriptorValidationUsesOnlyOriginalOpenLedgerAndReleasesComparisonMemory(): Unit =
    val m = build(fixture())
    val old = m.executeTarget.relation("fp_recovery_operation").rows.find(r =>
      value(r, "fp_recovery_operation", "operation_uuid") == u(id(40))).get
    val foreign = new MySqlFormat3ReadBudget(m.root.profile, m.root.profile.maxSqlPacketBytes())
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(foreign) {
        assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallHistoricalPayload.requireValid(old, m.root))
      }
      val child = m.root.newCanonicalScope()
      try
        MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          MySqlNormalRecallHistoricalPayload.requireValid(old, m.root)
        }
      finally child.closeScopedReadBudget()
      val buffer = new java.io.ByteArrayOutputStream
      assertTrue(MySqlNormalRecallHistoricalPayload.writeDescriptor(new java.io.DataOutputStream(buffer), old, "target_bytes"))
      val invalid = buffer.toByteArray ++ Array[Byte](0)
      val baseline = m.root.accountedMemoryBytes
      (0 until 5).foreach { _ =>
        MySqlRecoveryCanonicalBytes.withComparisonBudget(m.root) {
          val cause = assertThrows(classOf[SQLDataException], () =>
            MySqlNormalRecallHistoricalPayload.requireExactDescriptor(invalid, old, "target_bytes", m.root))
          assertTrue(cause.getMessage.contains("trailing"))
        }
        assertEquals(baseline, m.root.accountedMemoryBytes, "failed lexical comparison must release only its own allocations")
      }
      m.root.reserveRetainedMemory(m.root.availableMemoryBytes - 1L, "test original cumulative budget exhaustion")
      assertThrows(classOf[SQLDataException], () => MySqlNormalRecallHistoricalPayload.requireValid(old, m.root))
    finally
      foreign.closeRootReadBudget()
      m.root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallHistoricalPayload.requireValid(old, m.root))

  @Test def recoveryPrepareStillEmbedsCompleteOriginalRawPredecessor(): Unit =
    val m = build(fixture())
    val next = checkpoint(m)
    val row = m.executeTarget.relation("fp_recovery_operation").rows.find(r =>
      value(r, "fp_recovery_operation", "operation_uuid") == u(m.plan.executeOperationUuid)).get
    MySqlRecoveryCanonicalBytes.withAllocationBudget(m.root) {
      val raw = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, m.root.profile)
      val header = next.header.copy(request = m.header.request.copy(purpose = MySqlNormalRecallPurpose.RestoreCommittedActive))
      val result = MySqlNormalRecallCanonical.prepare(header, MySqlNormalRecallPurpose.RestoreCommittedActive,
        m.executeTarget, Some(raw), Some(m.plan.executeOperationUuid), None, None, m.root)
      val bytes = result.copyBytes(m.root, "test recovery predecessor complete canonical")
      val predecessor = raw.canonical.copyBytes(m.root, "test original raw predecessor")
      val offset = bytes.toVector.indexOfSlice(predecessor.toVector)
      assertTrue(offset >= 4, "P2 scope must contain the complete original RAW, not its descriptor")
      assertEquals(predecessor.length, java.nio.ByteBuffer.wrap(bytes, offset - 4, 4).getInt())
      val decoded = MySqlNormalRecallCanonical.decodePrepare(result.borrowedBinary, m.root)
      assertEquals(MySqlNormalRecallPurpose.RestoreCommittedActive, decoded.purpose)
      assertEquals(Some(m.plan.executeOperationUuid), decoded.originalPrepare)
      assertEquals(raw.row, decoded.predecessor.get.row)
      val wrongOriginal=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.prepare(
        header,MySqlNormalRecallPurpose.RestoreCommittedActive,m.executeTarget,Some(raw),Some(id(998)),None,None,m.root))
      assertEquals("requirement failed: active P2 without C must follow original P without S",wrongOriginal.getMessage)
      val missingSettlement=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.prepare(
        header,MySqlNormalRecallPurpose.RestoreCommittedActive,m.executeTarget,Some(raw),Some(m.plan.executeOperationUuid),Some(id(997)),None,m.root))
      assertEquals("requirement failed: active P2 with C must follow its exact committed S",missingSettlement.getMessage)
      val aliased=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.prepare(
        header,MySqlNormalRecallPurpose.RestoreCommittedActive,m.executeTarget,Some(raw),Some(m.plan.executeOperationUuid),
        Some(m.plan.executeOperationUuid),Some(id(996)),m.root))
      assertEquals("requirement failed: P2 original P/C/S identities must be distinct",aliased.getMessage)
      val reused=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.prepare(
        header,MySqlNormalRecallPurpose.RestoreCommittedActive,m.executeTarget,Some(raw),Some(m.plan.executeOperationUuid),
        Some(header.context.tag.operationId),Some(id(996)),m.root))
      assertEquals("requirement failed: P2 cannot reuse an original P/C/S operation identity",reused.getMessage)      // 三个Option与raw frame布局独立定位：P UUID后为C空、S空、predecessor存在及i32长度。
      val originalOffset=offset-4-1-1-1-16
      val actualOriginal=java.nio.ByteBuffer.wrap(bytes,originalOffset,16)
      assertEquals(m.plan.executeOperationUuid,new java.util.UUID(actualOriginal.getLong(),actualOriginal.getLong()))
      val malformed=bytes.clone()
      java.nio.ByteBuffer.wrap(malformed,originalOffset,16).putLong(id(998).getMostSignificantBits).putLong(id(998).getLeastSignificantBits)
      val rejected=assertThrows(classOf[IllegalArgumentException],()=>MySqlNormalRecallCanonical.decodePrepare(
        MySqlFormat3Binary.takeOwned(malformed,m.root),m.root))
      assertEquals("requirement failed: active P2 without C must follow original P without S",rejected.getMessage)
    }
