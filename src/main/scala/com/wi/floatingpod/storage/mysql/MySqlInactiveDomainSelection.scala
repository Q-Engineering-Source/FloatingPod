package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.UUID

/** canonical 行选择，绝不筛掉读取/验证/独立 full20 raw 比较的行。 */
private[mysql] object MySqlInactiveDomainSelection:
  def includes(table: String,row: Vector[MySqlFormat3Cell],request: MySqlInactiveDomainUpdateRequest): Boolean =
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

  private def value(row: Vector[MySqlFormat3Cell],table: String,name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  private def matches(row: Vector[MySqlFormat3Cell],table: String,name: String,id: UUID): Boolean =
    value(row,table,name) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
      case _ => false
  private def isPodKind(cell: MySqlFormat3Cell): Boolean = cell match
    case MySqlFormat3Cell.Text(kind) => kind=="POD"
    case _ => false
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive canonical selection: " + reason)
