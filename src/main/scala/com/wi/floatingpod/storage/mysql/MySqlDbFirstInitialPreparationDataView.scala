package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{UnboundPreparationReceipt, UnboundIntentOrigin}

/** 原读回派生的不可变业务DATA；本值不授予SQL、实体或Ready资格。 */
private[floatingpod] final case class MySqlDbFirstInitialPreparationDataView private[mysql] (
    state:FloatingPodState, receipt:UnboundPreparationReceipt)

private[mysql] object MySqlDbFirstInitialPreparationDataView:
  def read(request:MySqlControlledDebugPrepareRequest,snapshot:MySqlScriptSchemaSnapshot,
      sourceRoot:MySqlFormat3ReadBudget,queryBudget:MySqlFormat3ReadBudget,
      retainedRoot:MySqlFormat3ReadBudget):MySqlDbFirstInitialPreparationDataView =
    require(request!=null && retainedRoot!=null && !retainedRoot.isClosed &&
      retainedRoot.sharesAllocationLedger(sourceRoot),"original preparation DATA allocation family required")
    val query=MySqlFormat3QueryProjection.fromDbFirstData(snapshot,snapshot.inputs,sourceRoot,queryBudget)
    readWithQuery(request,query,retainedRoot)

  private def readWithQuery(request:MySqlControlledDebugPrepareRequest,query:MySqlFormat3QueryProjection,
      retainedRoot:MySqlFormat3ReadBudget):MySqlDbFirstInitialPreparationDataView =
    val receipt=query.findUnboundPreparation(request.podUuid).getOrElse(
      throw new java.sql.SQLDataException("preparation readback receipt missing"))
    val state=query.findByUuid(request.podUuid).getOrElse(
      throw new java.sql.SQLDataException("preparation readback domain missing"))
    require(receipt.origin==UnboundIntentOrigin.DebugItemCreate && receipt.observedIdentity.isEmpty &&
      receipt.binding==request.expectedBinding && receipt.podUuid==request.podUuid &&
      receipt.attemptUuid==request.attemptUuid && receipt.createOperationUuid.contains(request.createOperationUuid) &&
      receipt.preparedVersion==0L && state.uuid==request.podUuid && state.stateVersion==0L &&
      !state.isActive && state.ownerUuid.isEmpty && state.operatingMode==PodOperatingMode.WorldIdle &&
      MySqlControlledDebugWorkflowMaterializer.exactPose(receipt.targetPose,request.targetPose) &&
      MySqlControlledDebugWorkflowMaterializer.exactPose(MySqlPreparedUnboundGraph.poseOf(state),request.targetPose),
      "preparation readback DATA differs from original target-zero request")
    // immutable text共享不依赖reader；Entry独立计费，关闭query child不退掉跨Main保留值。
    retainedRoot.reserveRetainedMemory(2048L+MySqlFormat3Utf8.byteLength(state.worldName)+
      MySqlFormat3Utf8.byteLength(receipt.targetPose.worldName),"preparation Main DATA BEFORE retention")
    MySqlDbFirstInitialPreparationDataView(state,receipt)

  /** 每一实际Main读代重建的有界索引；只保typed标量，不保原P comparison图或执行资格。 */
  private[mysql] def currentPreparedInitials(snapshot:MySqlScriptSchemaSnapshot,sourceRoot:MySqlFormat3ReadBudget,
      retained:MySqlFormat3ReadBudget,query:MySqlFormat3QueryProjection):Map[java.util.UUID,MySqlDbFirstInitialPreparationDataView] =
    MySqlFormat3QueryProjection.requireDbFirstSource(snapshot,snapshot.inputs,sourceRoot)
    MySqlFormat3QueryProjection.requireDbFirstDataAllocation(sourceRoot,retained)
    val count=snapshot.relations.foldLeft(0L)((n,r)=>Math.addExact(n,r.rows.size.toLong))
    val workspace=Math.addExact(16384L,Math.multiplyExact(count,128L))
    retained.reserveTemporaryMemory(workspace,"current prepared-create index workspace BEFORE allocation")
    try
      val result=scala.collection.mutable.LinkedHashMap.empty[java.util.UUID,MySqlDbFirstInitialPreparationDataView]
      def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=
        MySqlDbFirstInitialActivationMaterializer.value(row,table,name,snapshot.inputs)
      def fail():Nothing=throw new java.sql.SQLDataException("current prepared-create identity/lineage differs")
      def check(ok:Boolean):Unit=if !ok then fail()
      def one(table:String)(predicate:Vector[MySqlFormat3Cell]=>Boolean):Vector[MySqlFormat3Cell] =
        val selected=snapshot.relation(table).rows.filter(predicate)
        if selected.size!=1 then fail()
        selected.head
      val intents=snapshot.relation("physical_recovery_intent").rows
      intents.foreach { intent=>
        val it="physical_recovery_intent"
        if value(intent,it,"phase")==MySqlFormat3Cell.Text("PREPARED") &&
            value(intent,it,"origin")==MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE") then
          val binding=value(intent,it,"binding_uuid")
          val pod=value(intent,it,"pod_uuid")
          val attempt=value(intent,it,"attempt_uuid")
          val preparation=value(intent,it,"prepare_operation_uuid")
          val footprint=value(intent,it,"target_footprint_uuid")
          val op=one("fp_recovery_operation")(r=>value(r,"fp_recovery_operation","binding_uuid")==binding &&
            value(r,"fp_recovery_operation","operation_uuid")==preparation)
          check(MySqlDbFirstInitialPreparationPersistedRead.hasPreparationScope(value(op,"fp_recovery_operation","scope_bytes")))
          val domain=one("floating_pod_state")(r=>value(r,"floating_pod_state","uuid")==pod)
          val locator=one("physical_recovery_locator")(r=>value(r,"physical_recovery_locator","binding_uuid")==binding &&
            value(r,"physical_recovery_locator","pod_uuid")==pod)
          val target=one("physical_recovery_footprint")(r=>value(r,"physical_recovery_footprint","binding_uuid")==binding &&
            value(r,"physical_recovery_footprint","pod_uuid")==pod && value(r,"physical_recovery_footprint","attempt_uuid")==attempt &&
            value(r,"physical_recovery_footprint","footprint_uuid")==footprint)
          check(value(intent,it,"intent_revision")==MySqlFormat3Cell.SignedInteger(0) &&
            value(intent,it,"expected_domain_version")==MySqlFormat3Cell.SignedInteger(0) &&
            value(intent,it,"expected_owner_uuid")==MySqlFormat3Cell.SqlNull &&
            value(domain,"floating_pod_state","binding_uuid")==binding &&
            value(locator,"physical_recovery_locator","current_attempt_uuid")==attempt &&
            value(locator,"physical_recovery_locator","current_footprint_uuid")==footprint &&
            value(locator,"physical_recovery_locator","domain_version")==MySqlFormat3Cell.SignedInteger(0) &&
            value(locator,"physical_recovery_locator","locator_state")==MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE_PREPARED") &&
            value(target,"physical_recovery_footprint","seal_state")==MySqlFormat3Cell.Text("SEALED") &&
            value(target,"physical_recovery_footprint","purpose")==MySqlFormat3Cell.Text("CREATE_TARGET") &&
            value(target,"physical_recovery_footprint","origin_version")==MySqlFormat3Cell.SignedInteger(0) &&
            value(target,"physical_recovery_footprint","created_operation_uuid")==preparation)
          Vector((domain,"floating_pod_state"),(locator,"physical_recovery_locator"),(intent,it)).foreach { (row,table)=>
            check(value(row,table,"last_mutation_operation_uuid")==preparation)
          }
          check(!snapshot.relation("fp_pod_snapshot").rows.exists(r=>value(r,"fp_pod_snapshot","binding_uuid")==binding &&
            value(r,"fp_pod_snapshot","pod_uuid")==pod))
          MySqlDbFirstInitialPreparationPersistedRead.withCommittedRequest(op,snapshot.inputs,sourceRoot) { request=>
            def matches(cell:MySqlFormat3Cell,id:java.util.UUID)=MySqlDbFirstInitialActivationMaterializer.uuidEquals(cell,id)
            check(matches(binding,request.expectedBinding.bindingUuid) && matches(pod,request.podUuid) &&
              matches(attempt,request.attemptUuid) && matches(footprint,request.footprintUuid) &&
              matches(value(intent,it,"create_operation_uuid"),request.createOperationUuid))
            val view=readWithQuery(request,query,retained)
            check(!result.contains(request.createOperationUuid))
            retained.reserveRetainedMemory(256L,"current prepared-create index entry BEFORE retention")
            result.put(request.createOperationUuid,view)
          }
      }
      retained.reserveRetainedMemory(128L+128L*result.size,"immutable prepared-create index BEFORE retention")
      result.toMap
    finally retained.releaseTemporaryMemory(workspace,"current prepared-create index workspace retired")
