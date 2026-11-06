package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.UUID

/** 请求与健康数据的冲突；不是rollback、资源退休或共享writer健康证明。 */
private[mysql] final class MySqlDbFirstPreparationRejected private[mysql] (
    val reason:MySqlDbFirstPreparationRejected.Reason,detail:String)
    extends SQLDataException("DB-first initial preparation: "+detail)
private[mysql] object MySqlDbFirstPreparationRejected:
  enum Reason:
    case ActiveCapacity, ExistingIdentity

/** schema6初建的完整DATA前置检查；不接受ACTIVATE，不签发计划、SQL ACK或物理放置资格。
  * 原Entry必须先持有before/root/forest holder，失败仍由原持有人承担清理与UNKNOWN责任。
  */
private[mysql] object MySqlDbFirstInitialPreparationPreflight:
  def requireFresh(before: MySqlScriptSchemaSnapshot, request: MySqlControlledDebugPrepareRequest,
      executeOperation: UUID, activeLimit: Int, root: MySqlFormat3ReadBudget,
      forest: MySqlDbFirstForestRead): Unit =
    if before == null || request == null || executeOperation == null || executeOperation == new UUID(0L,0L) ||
        root == null || forest == null || activeLimit < 1 || activeLimit > 256 then
      fail("complete original preparation inputs and active capacity 1..256 required")
    val inputs=before.inputs
    MySqlFormat3QueryProjection.requireDbFirstSource(before,inputs,root)
    forest.requireOwner(root,inputs,request.expectedBinding.bindingUuid)
    MySqlControlledDebugRelations.requireValidDbFirstGraph(before,inputs,root)
    requireFreshRows(before,request,executeOperation,activeLimit)
    // holder消费完整forest/index并核SHA及图关联；失败不关闭或替换原holder。
    forest.validateFreshPod(before,request.podUuid)

  /** 两条DATA路径共享身份/容量规则；不代替完整图及forest验证。 */
  private[mysql] def requireFreshRows(before:MySqlScriptSchemaSnapshot,request:MySqlControlledDebugPrepareRequest,
      executeOperation:UUID,activeLimit:Int):Unit =
    require(activeLimit>=1 && activeLimit<=256,"configured preparation capacity required")
    val inputs=before.inputs
    def value(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
      MySqlDbFirstInitialActivationMaterializer.value(row,table,column,inputs)
    def matches(cell: MySqlFormat3Cell, id: UUID): Boolean =
      MySqlDbFirstInitialActivationMaterializer.uuidEquals(cell,id)
    val bindings=before.relation("physical_recovery_binding").rows
    if bindings.size != 1 || !matches(value(bindings.head,"physical_recovery_binding","binding_uuid"),request.expectedBinding.bindingUuid) ||
        value(bindings.head,"physical_recovery_binding","cutover_state") != MySqlFormat3Cell.Text("CERTIFIED") ||
        value(bindings.head,"physical_recovery_binding","format_version") != MySqlFormat3Cell.SignedInteger(2) then
      fail("original certified binding differs from complete before")
    var active=0
    before.relation("floating_pod_state").rows.foreach { row =>
      value(row,"floating_pod_state","is_active") match
        case MySqlFormat3Cell.SignedInteger(0) => ()
        case MySqlFormat3Cell.SignedInteger(1) => active=Math.addExact(active,1)
        case _ => fail("actual active flag is not 0/1")
    }
    if active >= activeLimit then throw new MySqlDbFirstPreparationRejected(
      MySqlDbFirstPreparationRejected.Reason.ActiveCapacity,"actual global active capacity exhausted")
    // 检查实际全部27表，包含snapshot与历史责任；不先筛成旧20表closure。
    before.relations.foreach { relation =>
      val shape=inputs.forTable(relation.table)
      relation.rows.foreach { row =>
        var index=0
        while index < shape.columns.size do
          val collision=shape.columns(index).name match
            case "uuid" if relation.table == "floating_pod_state" => matches(row(index),request.podUuid)
            case "pod_uuid" | "resource_pod_uuid" => matches(row(index),request.podUuid)
            case "attempt_uuid" | "current_attempt_uuid" | "predecessor_attempt_uuid" => matches(row(index),request.attemptUuid)
            case "footprint_uuid" | "target_footprint_uuid" | "current_footprint_uuid" |
                "predecessor_footprint_uuid" | "from_footprint_uuid" | "to_footprint_uuid" => matches(row(index),request.footprintUuid)
            case "create_operation_uuid" => matches(row(index),request.createOperationUuid)
            case "operation_uuid" if relation.table == "fp_recovery_operation" => matches(row(index),executeOperation)
            case _ => false
          if collision then throw new MySqlDbFirstPreparationRejected(
            MySqlDbFirstPreparationRejected.Reason.ExistingIdentity,
            "proposed identity already has responsibility in " + relation.table + "." + shape.columns(index).name)
          index+=1
      }
    }

  private def fail(reason: String): Nothing =
    throw new SQLDataException("DB-first initial preparation: " + reason)
