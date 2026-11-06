package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 仅重建准备操作的业务行；不持有Entry、SQL或物理执行资格。调用者负责原前像及分配作用域。 */
private[mysql] object MySqlDbFirstInitialPreparationRows:
  def domain(before:MySqlScriptSchemaSnapshot,request:MySqlControlledDebugPrepareRequest,
      execute:UUID,plannedAt:LocalDateTime,allocation:MySqlFormat3ReadBudget):Vector[MySqlFormat3RelationSnapshot] =
    require(before != null && request != null && execute != null && plannedAt != null &&
      !allocation.isClosed && before.allocationBudget.sharesAllocationLedger(allocation) &&
      before.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "complete preparation business DATA/allocation family required")
    allocation.requireScriptDataInputs(before.inputs)
    var construction=before.relations
    def append(table:String,row:Vector[MySqlFormat3Cell]):Unit =
      val shape=before.inputs.forTable(table)
      val old=construction.find(_.table==table).getOrElse(throw new SQLDataException("preparation missing original relation "+table))
      def value(r:Vector[MySqlFormat3Cell],name:String)=MySqlDbFirstInitialActivationMaterializer.value(r,table,name,before.inputs)
      if old.rows.exists(r=>shape.primaryKey.forall(name=>value(r,name)==value(row,name))) then
        throw new SQLDataException("preparation cannot replace existing responsibility in "+table)
      allocation.reserveRetainedMemory(4096L+128L*(old.rows.size.toLong+1L)+64L*construction.size,
        "preparation append/sort relation workspace BEFORE allocation")
      val ordered=(old.rows :+ row).sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,shape)<0)
      construction=construction.map(r=>if r.table==table then MySqlFormat3RelationSnapshot(table,ordered) else r)
    val binding=request.expectedBinding.bindingUuid
    val rows=MySqlControlledDebugRows
    append(rows.Domain,rows.preparedDomain(binding,request.podUuid,request.targetPose,execute,plannedAt,allocation))
    append(rows.Footprint,rows.preparedFootprint(binding,request.podUuid,request.attemptUuid,
      request.footprintUuid,request.targetPose,execute,allocation))
    rows.preparedChunks(binding,request.podUuid,request.attemptUuid,request.footprintUuid,request.targetPose,allocation)
      .foreach(row=>append(rows.Chunk,row))
    append(rows.Intent,rows.preparedIntent(binding,request.podUuid,request.attemptUuid,request.footprintUuid,
      execute,request.createOperationUuid,allocation))
    append(rows.Locator,rows.preparedLocator(binding,request.podUuid,request.attemptUuid,request.footprintUuid,
      request.targetPose.dimensionId,execute,allocation))
    construction

  def releasedReservation(data:MySqlDbFirstInitialPreparationData,inputs:MySqlScriptSchemaInputs,
      allocation:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    val request=data.request
    val op=data.tag.operationId
    val key=MySqlRecoveryResourceKey.pod(request.podUuid,allocation.profile)
    val bytes=key.copyCanonicalBytes(allocation,"preparation exact POD resource")
    allocation.reserveRetainedMemory(96L,"preparation POD resource digest BEFORE allocation")
    val hash=MySqlFormat3Binary.takeOwned(java.security.MessageDigest.getInstance("SHA-256").digest(bytes),allocation)
    def uuid(id:UUID):MySqlFormat3Cell =
      allocation.reserveRetainedMemory(80L,"preparation reservation UUID BEFORE allocation")
      val value=java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array()
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value,allocation))
    val fields=Map("binding_uuid"->uuid(request.expectedBinding.bindingUuid),"resource_kind"->MySqlFormat3Cell.Text("POD"),
      "resource_key_hash"->MySqlFormat3Cell.Binary(hash),"resource_key"->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,allocation)),
      "reservation_revision"->MySqlFormat3Cell.SignedInteger(1),"workflow_uuid"->uuid(op),"operation_uuid"->uuid(op),
      "reservation_state"->MySqlFormat3Cell.Text("RELEASED"),"last_mutation_operation_uuid"->uuid(op),
      "resource_pod_uuid"->uuid(request.podUuid),"resource_unit_uuid"->MySqlFormat3Cell.SqlNull)
    val shape=inputs.forTable("fp_recovery_reservation")
    require(fields.size==shape.columns.size && shape.columns.forall(c=>fields.contains(c.name)),"complete preparation reservation required")
    allocation.reserveRetainedMemory(256L+96L*shape.columns.size,"preparation reservation row BEFORE allocation")
    shape.columns.map(c=>fields(c.name))

  /** 独立从原before重做业务增量，包含未触及关系与历史RAW；不接收目标业务列作为输入。 */
  def requireLocalTransition(before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot,
      raw:Vector[MySqlFormat3Cell],data:MySqlDbFirstInitialPreparationData,
      facts:MySqlDbFirstInitialPreparationFacts,originalRoot:MySqlFormat3ReadBudget):Unit =
    val allocation=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
    require((before.allocationBudget eq allocation) && (target.allocationBudget eq allocation) &&
      allocation.isCanonicalComparisonChildOf(originalRoot) && !allocation.isClosed,
      "exact original cold comparison DATA required")
    allocation.reserveRetainedMemory(16384L,"preparation semantic reconstruction metadata BEFORE allocation")
    MySqlControlledDebugRelations.requireValidDbFirstComparisonGraph(before,before.inputs,originalRoot)
    MySqlDbFirstInitialPreparationPreflight.requireFreshRows(before,data.request,data.tag.operationId,data.activeLimit)
    if MySqlFormat3SourceForestMembershipScan.containsDbFirstComparison(before,originalRoot,
        data.request.expectedBinding.bindingUuid,data.request.podUuid) then
      throw new SQLDataException("persisted preparation POD already occurs in verified forest")
    var expected=domain(before,data.request,data.tag.operationId,facts.plannedAt,allocation)
    val inputs=before.inputs
    def put(table:String,replacement:Vector[MySqlFormat3Cell],replace:Boolean):Unit =
      val shape=inputs.forTable(table)
      val old=expected.find(_.table==table).get
      def key(r:Vector[MySqlFormat3Cell])=shape.primaryKey.map(n=>r(shape.columns.indexWhere(_.name==n)))
      if old.rows.count(r=>key(r)==key(replacement))!=(if replace then 1 else 0) then
        throw new SQLDataException("persisted preparation original PK expectation differs: "+table)
      allocation.reserveRetainedMemory(4096L+128L*(old.rows.size.toLong+1L)+64L*expected.size,
        "preparation semantic relation replacement BEFORE allocation")
      val rows=if replace then old.rows.map(r=>if key(r)==key(replacement) then replacement else r) else old.rows :+ replacement
      expected=expected.map(r=>if r.table==table then MySqlFormat3RelationSnapshot(table,
        rows.sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,shape)<0)) else r)
    put("fp_recovery_reservation",releasedReservation(data,inputs,allocation),false)
    // 原O已经通过scope身份、完整RAW、canonical重编码与指纹核验，只借其不可递归的完整编码。
    put("fp_recovery_operation",raw,false)
    val writers=before.relation("fp_runtime_writer").rows
    if writers.size!=1 then throw new SQLDataException("persisted preparation original writer is not unique")
    val generic=MySqlDbFirstInitialActivationMaterializer.value(raw,"fp_recovery_operation","operation_fingerprint",inputs)
    val writer=MySqlControlledDebugRows.update(writers.head,"fp_runtime_writer",Map(
      "last_commit_operation_uuid"->MySqlControlledDebugRows.uuid(data.tag.operationId),
      "last_commit_fingerprint"->generic),allocation)
    put("fp_runtime_writer",writer,true)
    if expected!=target.relations then throw new SQLDataException("independent preparation business transition differs")
