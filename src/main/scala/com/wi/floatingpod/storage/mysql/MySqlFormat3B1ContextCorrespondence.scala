package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.*
import java.sql.SQLDataException
import java.util.UUID

/** 全量历史仍在full20中；此DATA核验只建立旧组件与真实current/target引用的逐字段对应。 */
private[mysql] object MySqlFormat3B1ContextCorrespondence:
  def requireCorrespondence(common: MySqlB1CommonGraph, snapshot: MySqlFormat3FullSnapshot,
      inputs: MySqlFormat3AuditInputs, root: MySqlFormat3ReadBudget): Unit =
    require(common != null && snapshot != null && inputs != null && root != null && !root.isClosed &&
      (snapshot.limitProfile eq root.profile) && snapshot.allocationBudget.sharesAllocationLedger(root),
      "same original open ledger/profile and complete DATA required")
    // No history copy or filtered replacement snapshot: all rows remain in the supplied full20.
    MySqlFreshFormat3Graph.requireManifestMatches(snapshot, common.manifest, inputs)
    MySqlFreshFormat3Graph.requireBindingMatches(snapshot, common.binding)
    MySqlFreshFormat3Graph.requireBootstrapIntentsMatch(snapshot, common.repository.bootstrapIntents)
    val repo = common.repository
    requireCount(snapshot, "floating_pod_state", repo.pods.size)
    repo.pods.foreach { expected =>
      val row = unique(snapshot, "floating_pod_state", "uuid", expected.uuidBytes)
      binary(row, "floating_pod_state", "uuid", expected.uuidBytes)
      optionalBinary(row, "floating_pod_state", "owner_uuid", expected.ownerUuidBytes)
      integer(row, "floating_pod_state", "state_version", expected.stateVersion)
      text(row, "floating_pod_state", "world_name", expected.worldName)
      floating(row, "floating_pod_state", "pos_x", expected.posX)
      floating(row, "floating_pod_state", "pos_y", expected.posY)
      floating(row, "floating_pod_state", "pos_z", expected.posZ)
      floating(row, "floating_pod_state", "yaw", expected.yaw)
      floating(row, "floating_pod_state", "pitch", expected.pitch)
      integer(row, "floating_pod_state", "is_active", expected.isActiveFlag.toLong)
      integer(row, "floating_pod_state", "last_updated", expected.lastUpdated)
      optionalInteger(row, "floating_pod_state", "dimension_id", expected.dimensionId.map(_.toLong))
      text(row, "floating_pod_state", "operating_mode", expected.operatingModeToken)
    }
    requireCount(snapshot, "player_entitlement", repo.entitlements.size)
    repo.entitlements.foreach { expected =>
      val row = uniqueUuid(snapshot, "player_entitlement", "player_uuid", expected.playerUuid)
      integer(row, "player_entitlement", "registration_limit", expected.registrationLimit.toLong)
      integer(row, "player_entitlement", "simultaneous_active_limit", expected.simultaneousActiveLimit.toLong)
    }
    requireCount(snapshot, "physical_recovery_locator", repo.recovery.locators.size)
    repo.recovery.locators.foreach { expected =>
      val row = uniqueUuid(snapshot, "physical_recovery_locator", "pod_uuid", expected.podUuid)
      uuid(row, "physical_recovery_locator", "current_attempt_uuid", expected.attemptUuid)
      text(row, "physical_recovery_locator", "locator_state", expected.state.databaseValue)
      text(row, "physical_recovery_locator", "reason_code", expected.reasonCode)
      optionalInteger(row, "physical_recovery_locator", "domain_version", expected.domainVersion)
      optionalInteger(row, "physical_recovery_locator", "target_dimension_id", expected.targetDimensionId.map(_.toLong))
      val footprint = cell(row, "physical_recovery_locator", "current_footprint_uuid")
      val actualFootprint = linkedFootprint(snapshot, expected.podUuid, expected.attemptUuid, footprint)
      if cell(actualFootprint, "physical_recovery_footprint", "target_dimension_id") !=
          cell(row, "physical_recovery_locator", "target_dimension_id") then invalid("locator footprint dimension")
      var count = 0
      snapshot.relation("physical_recovery_footprint_chunk").rows.foreach { chunk =>
        if cell(chunk, "physical_recovery_footprint_chunk", "footprint_uuid") == footprint then
          uuid(chunk, "physical_recovery_footprint_chunk", "pod_uuid", expected.podUuid)
          uuid(chunk, "physical_recovery_footprint_chunk", "attempt_uuid", expected.attemptUuid)
          val matches = expected.chunks.count { e =>
            cell(chunk, "physical_recovery_footprint_chunk", "dimension_id") == MySqlFormat3Cell.SignedInteger(e.dimensionId.toLong) &&
            cell(chunk, "physical_recovery_footprint_chunk", "chunk_x") == MySqlFormat3Cell.SignedInteger(e.chunkX.toLong) &&
            cell(chunk, "physical_recovery_footprint_chunk", "chunk_z") == MySqlFormat3Cell.SignedInteger(e.chunkZ.toLong)
          }
          if matches != 1 then invalid("complete selected footprint chunk")
          count += 1
      }
      if count != expected.chunks.size then invalid("selected footprint chunk count")
    }
    // Legacy global chunk inventory must correspond to exactly the selected current locators,
    // not every historical footprint (which remains fully retained in snapshot).
    if repo.recovery.chunks.size != repo.recovery.locators.iterator.map(_.chunks.size).sum ||
      !repo.recovery.chunks.forall(c => repo.recovery.locators.exists(l => l.podUuid == c.podUuid && l.chunks.contains(c.chunk))) then
      invalid("legacy complete current chunk inventory")
    // The legacy component records each explicit attempt identity. Additional full20
    // historical attempts stay in the complete snapshot and are compared separately;
    // their effect/causal issuer cannot be reconstructed from the legacy component.
    repo.recovery.intents.foreach { expected =>
      val row = uniqueUuid(snapshot, "physical_recovery_intent", "attempt_uuid", expected.attemptUuid)
      uuid(row, "physical_recovery_intent", "pod_uuid", expected.podUuid)
      uuid(row, "physical_recovery_intent", "binding_uuid", expected.bindingUuid)
      text(row, "physical_recovery_intent", "origin", expected.origin match
        case UnboundIntentOrigin.DebugItemCreate => "DEBUG_ITEM_CREATE"
        case UnboundIntentOrigin.ObservedArtifactAdoption => "UNBOUND_ADOPTION")
      text(row, "physical_recovery_intent", "phase", expected.phase match
        case MySqlRecoveryIntentPhase.Prepared => "PREPARED"
        case MySqlRecoveryIntentPhase.Committed => "COMMITTED")
      val sourceKind = expected.sourceKind.map {
        case ObservedUnboundSourceKind.FreshArtifact => "FRESH"
        case ObservedUnboundSourceKind.LegacyUnregistered => "LEGACY_UNREGISTERED"
      }
      optionalText(row, "physical_recovery_intent", "source_kind", sourceKind)
      optionalText(row, "physical_recovery_intent", "source_identity", expected.sourceIdentity)
      optionalBinary(row, "physical_recovery_intent", "source_identity_sha256", expected.sourceIdentitySha256)
      optionalUuid(row, "physical_recovery_intent", "catalog_generation", expected.catalogGeneration)
      optionalUuid(row, "physical_recovery_intent", "create_operation_uuid", expected.createOperationUuid)
      integer(row, "physical_recovery_intent", "expected_domain_version", expected.preparedDomainVersion)
      val target = linkedFootprint(snapshot, expected.podUuid, expected.attemptUuid,
        cell(row, "physical_recovery_intent", "target_footprint_uuid"))
      text(target, "physical_recovery_footprint", "target_world_name", expected.targetWorldName)
      floating(target, "physical_recovery_footprint", "target_pos_x", expected.targetPosX)
      floating(target, "physical_recovery_footprint", "target_pos_y", expected.targetPosY)
      floating(target, "physical_recovery_footprint", "target_pos_z", expected.targetPosZ)
      floating(target, "physical_recovery_footprint", "target_yaw", expected.targetYaw)
      floating(target, "physical_recovery_footprint", "target_pitch", expected.targetPitch)
      integer(target, "physical_recovery_footprint", "target_dimension_id", expected.targetDimensionId.toLong)
      integer(target, "physical_recovery_footprint", "center_chunk_x", expected.targetChunkX.toLong)
      integer(target, "physical_recovery_footprint", "center_chunk_z", expected.targetChunkZ.toLong)
      optionalInteger(row, "physical_recovery_intent", "committed_domain_version", expected.committedDomainVersion)
      optionalInteger(row, "physical_recovery_intent", "committed_at", expected.committedAt)
      optionalText(row, "physical_recovery_intent", "committed_world_name", expected.committedWorldName)
      optionalFloating(row, "physical_recovery_intent", "committed_pos_x", expected.committedPosX)
      optionalFloating(row, "physical_recovery_intent", "committed_pos_y", expected.committedPosY)
      optionalFloating(row, "physical_recovery_intent", "committed_pos_z", expected.committedPosZ)
      optionalFloating(row, "physical_recovery_intent", "committed_yaw", expected.committedYaw)
      optionalFloating(row, "physical_recovery_intent", "committed_pitch", expected.committedPitch)
      optionalInteger(row, "physical_recovery_intent", "committed_dimension_id", expected.committedDimensionId.map(_.toLong))
    }

  private def linkedFootprint(s: MySqlFormat3FullSnapshot, pod: UUID, attempt: UUID,
      id: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    var found: Vector[MySqlFormat3Cell] = null
    s.relation("physical_recovery_footprint").rows.foreach { row =>
      if cell(row, "physical_recovery_footprint", "footprint_uuid") == id then
        if found != null then invalid("ambiguous footprint identity")
        uuid(row, "physical_recovery_footprint", "pod_uuid", pod)
        uuid(row, "physical_recovery_footprint", "attempt_uuid", attempt)
        found = row
    }
    if found == null then invalid("missing exact footprint")
    found

  private def cell(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    val index = shape.columns.indexWhere(_.name == column)
    if index < 0 || row.size != shape.columns.size then invalid("frozen field shape " + table + "." + column)
    row(index)
  private def unique(s: MySqlFormat3FullSnapshot, table: String, key: String, bytes: Vector[Byte]): Vector[MySqlFormat3Cell] =
    var result: Vector[MySqlFormat3Cell] = null
    s.relation(table).rows.foreach { row =>
      cell(row, table, key) match
        case MySqlFormat3Cell.Binary(b) if b.equalsVector(bytes) =>
          if result != null then invalid("duplicate legacy key")
          result = row
        case _ => ()
    }
    if result == null then invalid("missing legacy key " + table)
    result
  private def uniqueUuid(s: MySqlFormat3FullSnapshot, table: String, key: String, id: UUID): Vector[MySqlFormat3Cell] =
    var result: Vector[MySqlFormat3Cell] = null
    s.relation(table).rows.foreach { row =>
      cell(row, table, key) match
        case MySqlFormat3Cell.Binary(b) if b.equalsUuid(id) =>
          if result != null then invalid("duplicate legacy UUID key")
          result = row
        case _ => ()
    }
    if result == null then invalid("missing legacy UUID key " + table)
    result
  private def requireCount(s: MySqlFormat3FullSnapshot, table: String, size: Int): Unit =
    if s.relation(table).rows.size != size then invalid("complete legacy count " + table)
  private def expect(row: Vector[MySqlFormat3Cell], t: String, c: String, value: MySqlFormat3Cell): Unit =
    if cell(row, t, c) != value then invalid(t + "." + c)
  private def integer(r: Vector[MySqlFormat3Cell], t: String, c: String, v: Long): Unit = expect(r,t,c,MySqlFormat3Cell.SignedInteger(v))
  private def text(r: Vector[MySqlFormat3Cell], t: String, c: String, v: String): Unit = expect(r,t,c,MySqlFormat3Cell.Text(v))
  private def floating(r: Vector[MySqlFormat3Cell], t: String, c: String, v: Double): Unit = expect(r,t,c,MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(v)))
  private def binary(r: Vector[MySqlFormat3Cell], t: String, c: String, v: Vector[Byte]): Unit = cell(r,t,c) match
    case MySqlFormat3Cell.Binary(b) if b.equalsVector(v) => ()
    case _ => invalid(t + "." + c)
  private def uuid(r: Vector[MySqlFormat3Cell], t: String, c: String, v: UUID): Unit = cell(r,t,c) match
    case MySqlFormat3Cell.Binary(b) if b.equalsUuid(v) => ()
    case _ => invalid(t + "." + c)
  private def optionalInteger(r: Vector[MySqlFormat3Cell],t:String,c:String,v:Option[Long]):Unit = v match
    case Some(x) => integer(r,t,c,x)
    case None => expect(r,t,c,MySqlFormat3Cell.SqlNull)
  private def optionalText(r:Vector[MySqlFormat3Cell],t:String,c:String,v:Option[String]):Unit = v match
    case Some(x) => text(r,t,c,x)
    case None => expect(r,t,c,MySqlFormat3Cell.SqlNull)
  private def optionalFloating(r:Vector[MySqlFormat3Cell],t:String,c:String,v:Option[Double]):Unit = v match
    case Some(x) => floating(r,t,c,x)
    case None => expect(r,t,c,MySqlFormat3Cell.SqlNull)
  private def optionalBinary(r:Vector[MySqlFormat3Cell],t:String,c:String,v:Option[Vector[Byte]]):Unit = v match
    case Some(x) => binary(r,t,c,x)
    case None => expect(r,t,c,MySqlFormat3Cell.SqlNull)
  private def optionalUuid(r:Vector[MySqlFormat3Cell],t:String,c:String,v:Option[UUID]):Unit = v match
    case Some(x) => uuid(r,t,c,x)
    case None => expect(r,t,c,MySqlFormat3Cell.SqlNull)
  private def invalid(stage: String): Nothing = throw new SQLDataException("cold full20/legacy DATA correspondence differs: " + stage)
