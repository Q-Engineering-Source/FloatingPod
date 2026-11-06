package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 连接新的 commit plan 前保留原 prepare P/A/F 全历史/原O全文；全局W可由其它合法工作流推进。 */
private[mysql] object MySqlControlledDebugPreparedComparison:
  def requireOriginal(original: MySqlFormat3FullSnapshot,current: MySqlFormat3FullSnapshot,
      request: MySqlControlledDebugPrepareRequest,root: MySqlFormat3ReadBudget,
      sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    MySqlFencedTransaction.requireControlledDebugSource(original,if sourceRoot==null then root else sourceRoot,root,continuation)
    MySqlFencedTransaction.requireControlledDebugSource(current,root,root,None)
    if original==null || current==null || request==null || root==null || root.isClosed || root.isScopedReadBudget ||
      (root.profile ne original.limitProfile) || (root.profile ne current.limitProfile) ||
      !root.sharesAllocationLedger(original.allocationBudget) || !root.sharesAllocationLedger(current.allocationBudget) ||
      original.schema!=current.schema || original.connection!=current.connection || original.observationScope!=current.observationScope ||
      original.maxAllowedPacketBytes!=current.maxAllowedPacketBytes then fail("original prepared same-session graph is unavailable")
    requireRelations(original.relations,current.relations,request,root,MySqlFormat3RelationShapes.canonicalOrder)

  private[mysql] def requireOriginalDeploymentReceipt(original:MySqlScriptSchemaSnapshot,
      current:MySqlScriptSchemaSnapshot,request:MySqlControlledDebugPrepareRequest,root:MySqlFormat3ReadBudget,
      sourceRoot:MySqlFormat3ReadBudget,
      continuation:Option[MySqlFencedTransaction.ControlledDebugContinuationProof]):Unit =
    MySqlFencedTransaction.requireControlledDebugSource(original,sourceRoot,root,continuation)
    MySqlFencedTransaction.requireControlledDebugSource(current,root,root,None)
    if request==null || (original.inputs ne current.inputs) || original.schema!=current.schema ||
        original.connection!=current.connection || original.observationScope!=current.observationScope ||
        original.maxAllowedPacketBytes!=current.maxAllowedPacketBytes then fail("original prepared Full26 same-session graph is unavailable")
    requireRelations(original.relations,current.relations,request,root,original.inputs.canonicalOrder)

  private def requireRelations(original:Vector[MySqlFormat3RelationSnapshot],current:Vector[MySqlFormat3RelationSnapshot],
      request:MySqlControlledDebugPrepareRequest,root:MySqlFormat3ReadBudget,
      shapes:Vector[MySqlFormat3RelationShape]):Unit =
    if original.size!=shapes.size || current.size!=shapes.size then fail("complete prepared relation count differs")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      var relationIndex=0
      while relationIndex<shapes.size do
        val oldRelation=original(relationIndex)
        val newRelation=current(relationIndex)
        if oldRelation.table!=shapes(relationIndex).table || newRelation.table!=shapes(relationIndex).table then fail("complete canonical relation order differs")
        val table=oldRelation.table
        if table!="fp_runtime_writer" && table!="fp_recovery_reservation" then
          val whole=MySqlControlledDebugSelection.isSharedHeader(table) ||
            !MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==table)
          var oldIndex=0
          var newIndex=0
          var finished=false
          while !finished do
            while oldIndex<oldRelation.rows.size && !whole &&
              !MySqlControlledDebugSelection.includes(table,oldRelation.rows(oldIndex),request) do oldIndex+=1
            while newIndex<newRelation.rows.size && !whole &&
              !MySqlControlledDebugSelection.includes(table,newRelation.rows(newIndex),request) do newIndex+=1
            if oldIndex>=oldRelation.rows.size || newIndex>=newRelation.rows.size then
              if oldIndex!=oldRelation.rows.size || newIndex!=newRelation.rows.size then
                fail("prepared authority/history has extra or missing rows")
              finished=true
            else
              if oldRelation.rows(oldIndex)!=newRelation.rows(newIndex) then
                fail("prepared complete typed row or historical raw payload changed")
              oldIndex+=1;newIndex+=1
        relationIndex+=1
    }
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled original prepared graph: "+reason)
