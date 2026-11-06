package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 精确 phase 差量门；先比较全20，不先过滤 history 或 unrelated rows。 */
private[mysql] object MySqlControlledDebugDelta:
  def requireAllowedDeploymentReceipt(before: MySqlScriptSchemaSnapshot,target: MySqlScriptSchemaSnapshot,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      activation: Boolean,phase: MySqlOperationEffectPhase,root: MySqlFormat3ReadBudget): Unit =
    if before == null || target == null || request == null || plan == null || phase == null then
      fail("original complete Full26 phase inputs required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    MySqlFormat3QueryProjection.requireDeploymentSource(target,before.inputs,root)
    if before.schema != target.schema || before.connection != target.connection ||
        before.observationScope != target.observationScope || before.maxAllowedPacketBytes != target.maxAllowedPacketBytes then
      fail("exact original Full26 phase context required")
    MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(before,request,plan,root)
    MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(target,request,plan,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child = MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      child.reserveTemporaryMemory(4096L,"controlled Full26 ordered delta scratch")
      var primary: Throwable = null
      try compareRelations(before.relations,target.relations,before.inputs.canonicalOrder,request,plan,activation,phase)
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try child.releaseTemporaryMemory(4096L,"controlled Full26 ordered delta scratch release")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
    }

  def requireAllowed(before: MySqlFormat3FullSnapshot,target: MySqlFormat3FullSnapshot,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      activation: Boolean,phase: MySqlOperationEffectPhase,root: MySqlFormat3ReadBudget,
      sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    if before==null || target==null || request==null || plan==null || phase==null || root==null || root.isClosed ||
      (root.profile ne before.limitProfile) || (root.profile ne target.limitProfile) ||
      !root.sharesAllocationLedger(before.allocationBudget) || !root.sharesAllocationLedger(target.allocationBudget) ||
      before.schema!=target.schema || before.connection!=target.connection || before.observationScope!=target.observationScope ||
      before.maxAllowedPacketBytes!=target.maxAllowedPacketBytes then fail("exact original full20 phase context required")
    MySqlControlledDebugRelations.requireControlledClosure(before,request,plan,root,sourceRoot,continuation)
    MySqlControlledDebugRelations.requireControlledClosure(target,request,plan,root,sourceRoot,continuation)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      // 固定 PK tuple/列查找工作区先占原 ledger；不复制历史 payload。
      val scratch=4096L
      child.reserveTemporaryMemory(scratch,"controlled full20 ordered delta scratch")
      var primary: Throwable=null
      try compareRelations(before,target,request,plan,activation,phase)
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try child.releaseTemporaryMemory(scratch,"controlled full20 ordered delta scratch release")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
    }

  private def compareRelations(before: MySqlFormat3FullSnapshot,target: MySqlFormat3FullSnapshot,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      activation: Boolean,phase: MySqlOperationEffectPhase): Unit =
    compareRelations(before.relations,target.relations,MySqlFormat3RelationShapes.canonicalOrder,request,plan,activation,phase)

  private def compareRelations(before: Vector[MySqlFormat3RelationSnapshot],target: Vector[MySqlFormat3RelationSnapshot],
      shapes: Vector[MySqlFormat3RelationShape],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,activation: Boolean,phase: MySqlOperationEffectPhase): Unit =
      if before.size != shapes.size || target.size != shapes.size then fail("exact complete relation count required")
      var relationIndex=0
      while relationIndex<shapes.size do
        val shape=shapes(relationIndex)
        val oldRows=before(relationIndex).rows
        val newRows=target(relationIndex).rows
        if before(relationIndex).table!=shape.table || target(relationIndex).table!=shape.table then
          fail("exact complete relation order required")
        var inserted=0
        var updated=0
        var oldIndex=0
        var newIndex=0
        while oldIndex<oldRows.size || newIndex<newRows.size do
          if newIndex>=newRows.size then fail("controlled phase cannot delete any raw row")
          val order=if oldIndex>=oldRows.size then 1 else compareKey(oldRows(oldIndex),newRows(newIndex),shape)
          if order<0 then fail("controlled phase cannot remove or replace a raw primary key")
          else if order>0 then
            requireIdentity(shape.table,newRows(newIndex),request,plan,phase,activation,inserting=true)
            inserted=Math.addExact(inserted,1)
            newIndex+=1
          else
            if oldRows(oldIndex)!=newRows(newIndex) then
              requireIdentity(shape.table,newRows(newIndex),request,plan,phase,activation,inserting=false)
              requireImmutableColumns(oldRows(oldIndex),newRows(newIndex),shape,phase)
              updated=Math.addExact(updated,1)
            oldIndex+=1;newIndex+=1
        requireCounts(shape.table,inserted,updated,phase,activation)
        relationIndex+=1

  private def requireCounts(table: String,inserted: Int,updated: Int,phase: MySqlOperationEffectPhase,
      activation: Boolean): Unit =
    val reserve=phase==MySqlOperationEffectPhase.Reserve
    if table=="fp_recovery_reservation" && reserve then
      if inserted+updated!=1 || inserted>1 || updated>1 then fail("reserve requires one exact R delta")
    else
      val expectedInsert=
        if reserve && table=="fp_recovery_operation" then 2
        else if !reserve && !activation then table match
          case "floating_pod_state" | "physical_recovery_footprint" | "physical_recovery_intent" | "physical_recovery_locator" => 1
          case "physical_recovery_footprint_chunk" => 25
          case _ => 0
        else 0
      val expectedUpdate=
        if table=="fp_runtime_writer" then 1
        else if !reserve then table match
          case "fp_recovery_operation" | "fp_recovery_reservation" => 1
          case "floating_pod_state" | "physical_recovery_intent" | "physical_recovery_locator" if activation => 1
          case _ => 0
        else 0
      if inserted!=expectedInsert || updated!=expectedUpdate then fail("phase has an extra, missing or foreign raw delta")

  private def requireIdentity(table: String,row: Vector[MySqlFormat3Cell],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,phase: MySqlOperationEffectPhase,activation: Boolean,inserting: Boolean): Unit =
    val reserve=phase==MySqlOperationEffectPhase.Reserve
    if table=="fp_runtime_writer" then
      if inserting || !matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) then fail("writer delta identity")
    else if !MySqlControlledDebugSelection.includes(table,row,request) then fail("foreign raw row mutation")
    else table match
      case "fp_recovery_operation" =>
        val reserveRow=matches(row,table,"operation_uuid",plan.reserveOperationUuid)
        val executeRow=matches(row,table,"operation_uuid",plan.executeOperationUuid)
        if !matches(row,table,"workflow_uuid",plan.workflowUuid) || !matches(row,table,"attempt_uuid",request.attemptUuid) ||
          !matches(row,table,"footprint_uuid",request.footprintUuid) ||
          (if reserve then !inserting || !(reserveRow || executeRow) else inserting || !executeRow) then
          fail("only original current O may change")
      case "fp_recovery_reservation" =>
        if !matches(row,table,"workflow_uuid",plan.workflowUuid) || !matches(row,table,"operation_uuid",plan.executeOperationUuid) ||
          !matches(row,table,"last_mutation_operation_uuid",if reserve then plan.reserveOperationUuid else plan.executeOperationUuid) ||
          text(row,table,"reservation_state")!=(if reserve then "HELD" else "RELEASED") ||
          value(row,table,"resource_unit_uuid")!=MySqlFormat3Cell.SqlNull then fail("exact immutable workflow R required")
      case "physical_recovery_footprint" | "physical_recovery_footprint_chunk" =>
        if reserve || activation || !inserting || !matches(row,table,"attempt_uuid",request.attemptUuid) ||
          !matches(row,table,"footprint_uuid",request.footprintUuid) then fail("only exact new A/F may be inserted")
      case "physical_recovery_intent" =>
        if reserve || inserting==activation || !matches(row,table,"attempt_uuid",request.attemptUuid) then fail("exact current intent phase delta")
      case "physical_recovery_locator" =>
        if reserve || inserting==activation || !matches(row,table,"current_attempt_uuid",request.attemptUuid) ||
          !matches(row,table,"current_footprint_uuid",request.footprintUuid) then fail("exact current locator phase delta")
      case "floating_pod_state" =>
        if reserve || inserting==activation then fail("exact controlled domain phase delta")
      case _ => fail("table cannot change in controlled debug phase")

  private def requireImmutableColumns(before: Vector[MySqlFormat3Cell],after: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape,phase: MySqlOperationEffectPhase): Unit =
    var index=0
    while index<shape.columns.size do
      val name=shape.columns(index).name
      val allowed=shape.table match
        case "fp_runtime_writer" => name=="last_commit_operation_uuid" || name=="last_commit_fingerprint"
        case "fp_recovery_operation" => name match
          case "operation_state" | "commit_witness_bytes" | "commit_witness_sha256" | "result_at" => true
          case _ => false
        case "fp_recovery_reservation" => name match
          case "reservation_revision" | "reservation_state" | "last_mutation_operation_uuid" => true
          case "workflow_uuid" | "operation_uuid" => phase==MySqlOperationEffectPhase.Reserve
          case _ => false
        case "floating_pod_state" => name match
          case "state_version" | "is_active" | "last_updated" | "last_mutation_operation_uuid" => true
          case _ => false
        case "physical_recovery_intent" => name match
          case "activation_operation_uuid" | "committed_domain_version" | "committed_at" | "committed_world_name" |
              "committed_pos_x" | "committed_pos_y" | "committed_pos_z" | "committed_yaw" | "committed_pitch" |
              "committed_dimension_id" | "phase" | "intent_revision" | "last_mutation_operation_uuid" => true
          case _ => false
        case "physical_recovery_locator" => name match
          case "locator_state" | "reason_code" | "domain_version" | "locator_revision" | "last_mutation_operation_uuid" => true
          case _ => false
        case _ => false
      if !allowed && before(index)!=after(index) then fail("phase changed an immutable raw column")
      index+=1

  private[mysql] def compareKey(left: Vector[MySqlFormat3Cell],right: Vector[MySqlFormat3Cell],shape: MySqlFormat3RelationShape): Int =
    var keyIndex=0
    while keyIndex<shape.primaryKey.size do
      val column=shape.primaryKey(keyIndex)
      val columnIndex=shape.columns.indexWhere(_.name==column)
      if columnIndex<0 || left.size!=shape.columns.size || right.size!=shape.columns.size then
        fail("complete fixed primary-key row shape required")
      val a=left(columnIndex)
      val b=right(columnIndex)
      val order=(a,b) match
        case (MySqlFormat3Cell.Binary(x),MySqlFormat3Cell.Binary(y)) => x.compareTo(y)
        case (MySqlFormat3Cell.SignedInteger(x),MySqlFormat3Cell.SignedInteger(y)) => java.lang.Long.compare(x,y)
        case (MySqlFormat3Cell.DateTime(x),MySqlFormat3Cell.DateTime(y)) => x.compareTo(y)
        case (MySqlFormat3Cell.Text(x),MySqlFormat3Cell.Text(y)) => compareText(x,y)
        case _ => fail("unsupported frozen primary-key kind")
      if order!=0 then return order
      keyIndex+=1
    0
  private def compareText(left: String,right: String): Int =
    var a=0;var b=0
    while a<left.length && b<right.length do
      val x=left.codePointAt(a);val y=right.codePointAt(b)
      val order=Integer.compare(x,y)
      if order!=0 then return order
      a+=Character.charCount(x);b+=Character.charCount(y)
    Integer.compare(left.length-a,right.length-b)
  private def value(row: Vector[MySqlFormat3Cell],table: String,name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  private def matches(row: Vector[MySqlFormat3Cell],table: String,name: String,id: java.util.UUID): Boolean = value(row,table,name) match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
    case _ => false
  private def text(row: Vector[MySqlFormat3Cell],table: String,name: String): String = value(row,table,name) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => fail("required typed text")
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled full20 phase delta: " + reason)
