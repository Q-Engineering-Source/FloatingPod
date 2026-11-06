package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 所有二十关系先合并比较，未授权行和历史 payload 不得因 canonical selection 而隐形。 */
private[mysql] object MySqlInactiveDomainDelta:
  def requireAllowed(before: MySqlFormat3FullSnapshot,target: MySqlFormat3FullSnapshot,
      request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,root: MySqlFormat3ReadBudget): Unit =
    if before==null || target==null || request==null || plan==null || phase==null || root==null ||
      root.isClosed || root.isScopedReadBudget || (root.profile ne before.limitProfile) ||
      (root.profile ne target.limitProfile) || before.schema!=target.schema || before.connection!=target.connection ||
      before.observationScope!=target.observationScope || before.maxAllowedPacketBytes!=target.maxAllowedPacketBytes then
      fail("exact full20 original context required")
    MySqlControlledDebugRelations.requireValidFullGraph(before,root)
    MySqlControlledDebugRelations.requireValidFullGraph(target,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      child.reserveTemporaryMemory(4096L,"inactive all20 ordered delta scratch")
      var primary: Throwable=null
      try
        var relationIndex=0
        while relationIndex<MySqlFormat3RelationShapes.ExpectedRelationCount do
          val shape=MySqlFormat3RelationShapes.canonicalOrder(relationIndex)
          val old=before.relations(relationIndex)
          val next=target.relations(relationIndex)
          if old.table!=shape.table || next.table!=shape.table then fail("full20 ordered relation identity")
          var a=0
          var b=0
          var inserted=0
          var updated=0
          while a<old.rows.size || b<next.rows.size do
            if b>=next.rows.size then fail("inactive update cannot delete history or domain rows")
            val order=if a>=old.rows.size then 1 else MySqlControlledDebugDelta.compareKey(old.rows(a),next.rows(b),shape)
            if order<0 then fail("inactive update cannot remove or replace any primary key")
            else if order>0 then
              requireIdentity(next.table,next.rows(b),request,plan,phase,true)
              inserted=Math.addExact(inserted,1)
              b+=1
            else
              if old.rows(a)!=next.rows(b) then
                requireIdentity(next.table,next.rows(b),request,plan,phase,false)
                var column=0
                while column<shape.columns.size do
                  if old.rows(a)(column)!=next.rows(b)(column) && !allowedColumn(next.table,shape.columns(column).name,phase) then
                    fail("inactive update changed an unrelated raw column")
                  column+=1
                updated=Math.addExact(updated,1)
              a+=1
              b+=1
          requireCounts(shape.table,inserted,updated,phase,before,request)
          relationIndex+=1
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try child.releaseTemporaryMemory(4096L,"inactive all20 delta scratch release")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
    }

  private def requireCounts(table: String,inserted: Int,updated: Int,phase: MySqlOperationEffectPhase,
      before: MySqlFormat3FullSnapshot,request: MySqlInactiveDomainUpdateRequest): Unit =
    val reserve=phase==MySqlOperationEffectPhase.Reserve
    if reserve && table=="fp_recovery_reservation" then
      if inserted+updated!=1 then fail("one exact reserve R insert or released reuse required")
    else
      val expectedInserted=if reserve && table=="fp_recovery_operation" then 2 else 0
      val reserveLocatorUpdate=reserve && table=="physical_recovery_locator" && before.relation(table).rows.exists(row =>
        matches(row,table,"pod_uuid",request.podUuid) &&
        MySqlControlledDebugRows.value(row,table,"domain_version")==MySqlFormat3Cell.SqlNull)
      val expectedUpdated=if table=="fp_runtime_writer" || reserveLocatorUpdate then 1 else if reserve then 0 else table match
        case "floating_pod_state" | "fp_recovery_operation" | "fp_recovery_reservation" => 1
        case "physical_recovery_locator" =>
          var count=0
          before.relation(table).rows.foreach { row => if matches(row,table,"pod_uuid",request.podUuid) then count+=1 }
          count
        case _ => 0
      if inserted!=expectedInserted || updated!=expectedUpdated then fail("fixed inactive phase statement cardinality differs")

  private def requireIdentity(table: String,row: Vector[MySqlFormat3Cell],request: MySqlInactiveDomainUpdateRequest,
      plan: MySqlOwnedRepositoryOperationPlan,phase: MySqlOperationEffectPhase,inserting: Boolean): Unit =
    if !matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) then fail("changed row binding differs")
    val reserve=phase==MySqlOperationEffectPhase.Reserve
    table match
      case "fp_runtime_writer" if !inserting => ()
      case "floating_pod_state" if !reserve && !inserting && matches(row,table,"uuid",request.podUuid) => ()
      case "physical_recovery_locator" if !inserting && matches(row,table,"pod_uuid",request.podUuid) => ()
      case "fp_recovery_reservation" if matches(row,table,"resource_pod_uuid",request.podUuid) &&
          MySqlControlledDebugRows.value(row,table,"resource_kind")==MySqlFormat3Cell.Text("POD") => ()
      case "fp_recovery_operation" if matches(row,table,"pod_uuid",request.podUuid) &&
          matches(row,table,"workflow_uuid",plan.workflowUuid) &&
          ((reserve && inserting && (matches(row,table,"operation_uuid",plan.reserveOperationUuid) ||
            matches(row,table,"operation_uuid",plan.executeOperationUuid))) ||
           (!reserve && !inserting && matches(row,table,"operation_uuid",plan.executeOperationUuid))) => ()
      case _ => fail("changed row is outside fixed inactive request/phase")

  private def allowedColumn(table: String,column: String,phase: MySqlOperationEffectPhase): Boolean =
    val reserve=phase==MySqlOperationEffectPhase.Reserve
    table match
      case "fp_runtime_writer" => column=="last_commit_operation_uuid" || column=="last_commit_fingerprint"
      case "floating_pod_state" if !reserve => column match
        case "state_version" | "world_name" | "pos_x" | "pos_y" | "pos_z" | "yaw" | "pitch" |
            "last_updated" | "last_mutation_operation_uuid" => true
        case _ => false
      case "physical_recovery_locator" =>
        column=="domain_version" || column=="locator_revision" || column=="last_mutation_operation_uuid"
      case "fp_recovery_reservation" => column match
        case "workflow_uuid" | "operation_uuid" if reserve => true
        case "reservation_revision" | "reservation_state" | "last_mutation_operation_uuid" => true
        case _ => false
      case "fp_recovery_operation" if !reserve => column match
        case "operation_state" | "original_context_bytes" | "original_context_sha256" | "preimage_bytes" |
            "preimage_sha256" | "target_bytes" | "target_sha256" | "operation_fingerprint" |
            "commit_witness_bytes" | "commit_witness_sha256" | "result_at" => true
        case _ => false
      case _ => false
  private def matches(row: Vector[MySqlFormat3Cell],table: String,column: String,id: java.util.UUID): Boolean =
    MySqlControlledDebugRows.value(row,table,column) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
      case _ => false
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive exact delta: "+reason)
