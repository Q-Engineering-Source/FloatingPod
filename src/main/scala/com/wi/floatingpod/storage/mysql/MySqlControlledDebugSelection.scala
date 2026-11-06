package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.UUID

/** canonical 行选择，绝不筛掉读取/验证/独立 full20 raw 比较的行。 */
private[mysql] object MySqlControlledDebugSelection:
  def includes(table: String,row: Vector[MySqlFormat3Cell],request: MySqlControlledDebugPrepareRequest): Boolean =
    if table == null || row == null || request == null then fail("complete closure inputs required")
    table match
      case "fp_schema_manifest" | "physical_recovery_binding" | "fp_runtime_writer" | "binding_bootstrap_intent" => true
      case "floating_pod_state" => matches(row,table,"uuid",request.podUuid)
      case "player_entitlement" | "physical_source_unit" | "physical_source_forest" |
          "physical_source_transfer" | "physical_source_write" | "physical_source_obligation" => false
      case "fp_recovery_reservation" =>
        matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) &&
          matches(row,table,"resource_pod_uuid",request.podUuid) &&
          isPodKind(value(row,table,"resource_kind"))
      case "physical_recovery_footprint" | "physical_recovery_footprint_chunk" | "physical_recovery_intent" |
          "physical_recovery_locator" | "fp_recovery_operation" | "physical_recovery_obligation" |
          "fp_lifecycle_completion" | "physical_footprint_transfer" =>
        matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) && matches(row,table,"pod_uuid",request.podUuid)
      case _ => fail("table outside exact full20 closure")

  def isSharedHeader(table: String): Boolean = table match
    case "fp_schema_manifest" | "physical_recovery_binding" | "fp_runtime_writer" | "binding_bootstrap_intent" => true
    case _ => false

  /** fixed proposed identities不能在foreign authority图中重新解释成同一请求。 */
  def requireNoForeignIdentity(table: String,row: Vector[MySqlFormat3Cell],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan): Unit =
    val shape=MySqlFormat3RelationShapes.forTable(table)
    val selected=includes(table,row,request)
    var index=0
    while index<shape.columns.size do
      val name=shape.columns(index).name
      val collision=name match
        case "uuid" if table=="floating_pod_state" => equalsUuid(row(index),request.podUuid)
        case "pod_uuid" | "resource_pod_uuid" => equalsUuid(row(index),request.podUuid)
        case "attempt_uuid" | "current_attempt_uuid" | "predecessor_attempt_uuid" => equalsUuid(row(index),request.attemptUuid)
        case "footprint_uuid" | "target_footprint_uuid" | "current_footprint_uuid" |
            "predecessor_footprint_uuid" | "from_footprint_uuid" | "to_footprint_uuid" => equalsUuid(row(index),request.footprintUuid)
        case "operation_uuid" if table=="fp_recovery_operation" =>
          equalsUuid(row(index),plan.reserveOperationUuid) || equalsUuid(row(index),plan.executeOperationUuid)
        case _ => false
      if collision && !selected then fail("proposed identity conflicts with foreign graph")
      index+=1
    if table=="fp_recovery_operation" &&
      (matches(row,table,"operation_uuid",plan.reserveOperationUuid) || matches(row,table,"operation_uuid",plan.executeOperationUuid)) &&
      (!matches(row,table,"workflow_uuid",plan.workflowUuid) || !matches(row,table,"attempt_uuid",request.attemptUuid) ||
        !matches(row,table,"footprint_uuid",request.footprintUuid)) then fail("current operation identity differs from original plan")
    if selected && !isSharedHeader(table) then
      index=0
      while index<shape.columns.size do
        val name=shape.columns(index).name
        if (name=="source_unit_uuid" || name=="source_forest_uuid" || name=="forest_uuid") &&
          row(index)!=MySqlFormat3Cell.SqlNull then fail("unsupported source history in controlled debug closure")
        index+=1

  private def value(row: Vector[MySqlFormat3Cell],table: String,name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  private def matches(row: Vector[MySqlFormat3Cell],table: String,name: String,id: UUID): Boolean =
    equalsUuid(value(row,table,name),id)
  private def isPodKind(cell: MySqlFormat3Cell): Boolean = cell match
    case MySqlFormat3Cell.Text(kind) => kind=="POD"
    case _ => false
  private def equalsUuid(cell: MySqlFormat3Cell,id: UUID): Boolean = cell match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
    case _ => false
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled canonical closure: " + reason)
