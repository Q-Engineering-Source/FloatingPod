package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{OperatingModeChangeOutcome, PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState}

import java.nio.ByteBuffer
import java.util.{Arrays, UUID}
import java.util.concurrent.atomic.AtomicReference
import java.sql.SQLDataException
import MySqlModeOperationEffectCodec.{allocationBudget,limitProfile,observationScope,relation}

/** Actual acknowledged execute + exact same-worker full20 readback. No Main or physical authority. */
final class MySqlCommittedTrackedDriftGraph private (
    val tag: IoOperationTag, val operationPlan: MySqlOwnedRepositoryOperationPlan,
    val request: MySqlTrackedDriftRequest, val state: FloatingPodState,
    val locator: PhysicalRecoveryLocator, val fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot)

object MySqlCommittedTrackedDriftGraph:
  private[mysql] def fromFormat3Readback(tag: IoOperationTag, plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlTrackedDriftRequest, state: FloatingPodState, snapshot: MySqlModeOperationEffectCodec.Image,
      activeLimit: Int, projectionBudget: MySqlFormat3ReadBudget): MySqlCommittedTrackedDriftGraph =
    require(tag != null && plan != null && request != null && state != null && snapshot != null && projectionBudget != null,
      "actual acknowledged drift state/full20/shared projection budget required")
    plan.requireTag(tag)
    plan.requireSingle(request.repositoryOperation, snapshot.limitProfile)
    require(snapshot.observationScope.bootUuid == tag.bootId && snapshot.observationScope.bootGeneration == tag.bootGeneration &&
      snapshot.observationScope.connectionGeneration == tag.connectionGeneration, "exact drift graph generation required")
    require(activeLimit > 0 && activeLimit <= 256, "actual configured active capacity required")
    MySqlCommittedOwnedModeGraph.requireBinding(snapshot,request.binding)
    require(state.uuid == request.podUuid && state.isActive && state.ownerUuid == request.criticalBase.ownerUuid &&
      state.operatingMode == request.criticalBase.operatingMode && state.stateVersion == Math.addExact(request.expectedStateVersion, 1L) &&
      poseOf(state) == request.actualPose, "drift outcome changed critical identity or lacks exact +1 ACK")
    validateDomainRow(snapshot, request, state)
    val activeCount = snapshot.relation("floating_pod_state").rows.count(row =>
      MySqlDriftOperationEffectCodec.value(row, "floating_pod_state", "is_active") match
        case MySqlFormat3Cell.SignedInteger(0L) => false
        case MySqlFormat3Cell.SignedInteger(1L) => true
        case _ => throw new SQLDataException("full20 drift image has invalid active flag"))
    require(activeCount <= activeLimit, "full20 drift image exceeds actual active capacity")
    val locator = locatorFromFormat3(snapshot, request, state)
    new MySqlCommittedTrackedDriftGraph(tag, plan, request, state, locator,
      MySqlCommittedOwnedModeGraph.ownedImage(snapshot,projectionBudget))

  private[mysql] def validateDomainRow(snapshot: MySqlModeOperationEffectCodec.Image, request: MySqlTrackedDriftRequest,
      state: FloatingPodState): Unit =
    val rows = snapshot.relation("floating_pod_state").rows.filter(row =>
      MySqlDriftOperationEffectCodec.uuidEquals(
        MySqlDriftOperationEffectCodec.value(row, "floating_pod_state", "uuid"), request.podUuid))
    if rows.size != 1 then throw new SQLDataException("F3 mode readback has no unique pod domain row")
    val row = rows.head
    def value(name: String): MySqlFormat3Cell = MySqlDriftOperationEffectCodec.value(row, "floating_pod_state", name)
    def sameDouble(name: String, expected: Double): Boolean = value(name) match
      case MySqlFormat3Cell.Floating(bits) => bits == java.lang.Double.doubleToRawLongBits(expected)
      case _ => false
    val ownerMatches = (value("owner_uuid"), state.ownerUuid) match
      case (MySqlFormat3Cell.SqlNull, None) => true
      case (cell: MySqlFormat3Cell.Binary, Some(owner)) => MySqlDriftOperationEffectCodec.uuidEquals(cell, owner)
      case _ => false
    val dimensionMatches = (value("dimension_id"), state.dimensionId) match
      case (MySqlFormat3Cell.SqlNull, None) => true
      case (MySqlFormat3Cell.SignedInteger(actual), Some(expected)) => actual == expected.toLong
      case _ => false
    val exact = MySqlDriftOperationEffectCodec.uuidEquals(value("uuid"), state.uuid) && ownerMatches &&
      value("state_version") == MySqlFormat3Cell.SignedInteger(state.stateVersion) &&
      value("world_name") == MySqlFormat3Cell.Text(state.worldName) &&
      sameDouble("pos_x", state.posX) && sameDouble("pos_y", state.posY) && sameDouble("pos_z", state.posZ) &&
      sameDouble("yaw", state.yaw.toDouble) && sameDouble("pitch", state.pitch.toDouble) &&
      value("is_active") == MySqlFormat3Cell.SignedInteger(if state.isActive then 1L else 0L) &&
      value("last_updated") == MySqlFormat3Cell.SignedInteger(state.lastUpdated) && dimensionMatches &&
      value("operating_mode") == MySqlFormat3Cell.Text(state.operatingMode.databaseValue) &&
      MySqlDriftOperationEffectCodec.uuidEquals(value("binding_uuid"), request.expectedBinding.bindingUuid)
    if !exact then throw new SQLDataException("typed F3 domain row differs from the actual owned-mode outcome")

  private[mysql] def locatorFromFormat3(snapshot: MySqlModeOperationEffectCodec.Image, request: MySqlTrackedDriftRequest,
      state: FloatingPodState): PhysicalRecoveryLocator =
    val locatorRows = snapshot.relation("physical_recovery_locator").rows.filter(row =>
      MySqlDriftOperationEffectCodec.uuidEquals(
        MySqlDriftOperationEffectCodec.value(row, "physical_recovery_locator", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(
          MySqlDriftOperationEffectCodec.value(row, "physical_recovery_locator", "pod_uuid"), request.podUuid))
    if locatorRows.size != 1 then throw new SQLDataException("F3 mode readback has no unique exact locator row")
    val locator = locatorRows.head
    val attempt = uuidValue(snapshot, MySqlDriftOperationEffectCodec.value(locator,
      "physical_recovery_locator", "current_attempt_uuid"), "mode locator attempt")
    val footprintId = uuidValue(snapshot, MySqlDriftOperationEffectCodec.value(locator,
      "physical_recovery_locator", "current_footprint_uuid"), "mode locator footprint")
    val footprintRows = snapshot.relation("physical_recovery_footprint").rows.filter(row =>
      MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
        "physical_recovery_footprint", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint", "pod_uuid"), request.podUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint", "attempt_uuid"), attempt) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint", "footprint_uuid"), footprintId))
    if footprintRows.size != 1 then throw new SQLDataException("F3 locator does not reference one exact footprint")
    val footprint = footprintRows.head
    val seal = MySqlDriftOperationEffectCodec.value(footprint, "physical_recovery_footprint", "seal_state")
    if seal != MySqlFormat3Cell.Text("SEALED") then throw new SQLDataException("F3 mode locator footprint is not sealed")
    val centerX = intValue(footprint, "physical_recovery_footprint", "center_chunk_x")
    val centerZ = intValue(footprint, "physical_recovery_footprint", "center_chunk_z")
    if intValue(footprint, "physical_recovery_footprint", "target_dimension_id") != request.actualPose.dimensionId ||
      centerX != chunk(request.actualPose.posX) || centerZ != chunk(request.actualPose.posZ) then
      throw new SQLDataException("F3 mode footprint is not exact for the actual same-chunk pose")
    val chunkRows = snapshot.relation("physical_recovery_footprint_chunk").rows.filter(row =>
      MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
        "physical_recovery_footprint_chunk", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint_chunk", "pod_uuid"), request.podUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint_chunk", "attempt_uuid"), attempt) &&
        MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(row,
          "physical_recovery_footprint_chunk", "footprint_uuid"), footprintId))
    val chunks = chunkRows.map { row =>
      val dimension = intValue(row, "physical_recovery_footprint_chunk", "dimension_id")
      PhysicalRecoveryChunk(dimension,
        intValue(row, "physical_recovery_footprint_chunk", "chunk_x"),
        intValue(row, "physical_recovery_footprint_chunk", "chunk_z"))
    }.toList
    val expectedChunks = MySqlRepositoryTransitionRules.chunksAt(request.actualPose.dimensionId,
      request.actualPose.posX, request.actualPose.posZ)
    if chunks.size != 25 || chunks.toSet != expectedChunks then
      throw new SQLDataException("F3 mode locator does not reference the complete exact 5x5 footprint")
    val locatorState = textValue(locator, "physical_recovery_locator", "locator_state") match
      case value => PhysicalRecoveryLocatorState.fromDatabase(value)
    val rawReason = textValue(locator, "physical_recovery_locator", "reason_code")
    if rawReason != "ROW_ACTIVE" then
      throw new SQLDataException("F3 mode locator reason is not the exact ROW_ACTIVE value")
    val reason = PhysicalRecoveryLocatorReason.RowActive
    val domainVersion = MySqlDriftOperationEffectCodec.value(locator, "physical_recovery_locator", "domain_version") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.SignedInteger(value) => Some(value)
      case _ => throw new SQLDataException("F3 locator domain version has invalid typed value")
    val targetDimension = intValue(locator, "physical_recovery_locator", "target_dimension_id")
    val result = PhysicalRecoveryLocator(request.podUuid, attempt, locatorState, reason, domainVersion,
      Some(targetDimension), chunks)
    if attempt != request.attemptUuid || result.state != PhysicalRecoveryLocatorState.RowActive ||
      reason != PhysicalRecoveryLocatorReason.RowActive || domainVersion != Some(state.stateVersion) ||
      targetDimension != request.actualPose.dimensionId then
      throw new SQLDataException("F3 locator row differs from the exact active mode command")
    result

  private def uuidValue(snapshot: MySqlModeOperationEffectCodec.Image, cell: MySqlFormat3Cell, label: String): UUID = cell match
    case MySqlFormat3Cell.Binary(value) if value.length == 16 =>
      val bytes = value.copyBytes(snapshot.allocationBudget, label)
      var primary: Throwable = null
      try
        val input = ByteBuffer.wrap(bytes)
        new UUID(input.getLong(), input.getLong())
      catch
        case failure: Throwable => primary = failure; throw failure
      finally
        Arrays.fill(bytes, 0.toByte)
        try snapshot.allocationBudget.releaseRetainedMemory(Math.addExact(bytes.length.toLong, 64L), label + " release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)
    case _ => throw new SQLDataException(label + " is not an exact 16-byte UUID")

  private def textValue(row: Vector[MySqlFormat3Cell], table: String, column: String): String =
    MySqlDriftOperationEffectCodec.value(row, table, column) match
      case MySqlFormat3Cell.Text(value) => value
      case _ => throw new SQLDataException(table + "." + column + " is not typed text")

  private def intValue(row: Vector[MySqlFormat3Cell], table: String, column: String): Int =
    MySqlDriftOperationEffectCodec.value(row, table, column) match
      case MySqlFormat3Cell.SignedInteger(value) if value >= Int.MinValue.toLong && value <= Int.MaxValue.toLong => value.toInt
      case _ => throw new SQLDataException(table + "." + column + " is not an in-range INT")

  private def chunk(value: Double): Int =
    val floored = Math.floor(value / 16D)
    if !java.lang.Double.isFinite(floored) || floored < Int.MinValue.toDouble || floored > Int.MaxValue.toDouble then
      throw new SQLDataException("owned mode pose has an out-of-range chunk coordinate")
    floored.toInt

  private[mysql] def poseOf(state: FloatingPodState): FloatingPodPose =
    FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch,
      state.dimensionId.getOrElse(throw new IllegalArgumentException("active mode state requires an exact dimension")))

