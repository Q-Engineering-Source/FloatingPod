package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{OperatingModeChangeOutcome, PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState}

import java.nio.ByteBuffer
import java.util.{Arrays, UUID}
import java.util.concurrent.atomic.AtomicReference
import java.sql.SQLDataException
import MySqlModeOperationEffectCodec.{allocationBudget, limitProfile, observationScope, relation}

/** MySQL mode 图只暴露 Main 所需的会话身份和绑定值，不冒充旧 F2 recovery inventory。 */
final case class MySqlCommittedOwnedModeRecoveryIdentity(binding: Option[PhysicalRecoveryBinding]):
  require(binding != null && !binding.exists(_ == null), "mode recovery identity option is required")

final case class MySqlCommittedOwnedModeInventoryIdentity(
    bootUuid: UUID,
    bootGeneration: Long,
    connectionGeneration: Long,
    recovery: MySqlCommittedOwnedModeRecoveryIdentity
):
  require(bootUuid != null && bootGeneration > 0L && connectionGeneration > 0L && recovery != null,
    "actual owned mode session identity is required")

/** 固定 owned mode CAS context；Main freeze/body/session资格不包含在此SQL输入中。 */
final case class MySqlOwnedModeCommitRequest(expectedBinding: PhysicalRecoveryBinding, podUuid: UUID,
    authenticatedOwner: UUID, expectedStateVersion: Long, attemptUuid: UUID,
    targetMode: PodOperatingMode, actualPose: FloatingPodPose):
  require(expectedBinding != null && podUuid != null && authenticatedOwner != null && attemptUuid != null &&
    targetMode != null && actualPose != null && expectedStateVersion >= 0L, "complete immutable owned mode CAS request required")
  require(expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified, "certified mode binding required")

  def repositoryOperation: MySqlRepositoryOperation =
    MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(expectedBinding, podUuid, authenticatedOwner,
      expectedStateVersion, attemptUuid, targetMode, actualPose)

/** 实际Changed/AlreadySet ACK与同一 F3 full20 readback；不产生 Main publication 或 Ready 资格。 */
final class MySqlCommittedOwnedModeGraph private (
    val tag: IoOperationTag,
    val request: MySqlOwnedModeCommitRequest,
    val actualCommit: OperatingModeChangeOutcome,
    val state: FloatingPodState,
    val locator: PhysicalRecoveryLocator,
    val inventory: MySqlCommittedOwnedModeInventoryIdentity,
    val fullTwentyRelationSnapshot: Option[MySqlOwnedFormat3Snapshot],
    private[mysql] val legacyInventory: Option[MySqlRecoverySnapshot]
):
  require(tag != null && request != null && actualCommit != null && state != null && locator != null &&
    inventory != null && fullTwentyRelationSnapshot != null && !fullTwentyRelationSnapshot.exists(_ == null) &&
    legacyInventory != null && !legacyInventory.exists(_ == null), "complete owned mode readback graph is required")
  require(fullTwentyRelationSnapshot.isDefined != legacyInventory.isDefined,
    "mode graph must retain exactly one real readback representation")

object MySqlCommittedOwnedModeGraph:
  private[mysql] def requireBinding(snapshot: MySqlModeOperationEffectCodec.Image,
      expected: PhysicalRecoveryBinding): Unit = snapshot match
    case legacy: MySqlFormat3FullSnapshot => MySqlFreshFormat3Graph.requireBindingMatches(legacy, Some(expected))
    case deployment: MySqlScriptSchemaSnapshot =>
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(deployment, expected.cutoverState, expected.bindingUuid)

  private[mysql] def ownedImage(snapshot: MySqlModeOperationEffectCodec.Image,
      root: MySqlFormat3ReadBudget): MySqlOwnedFormat3Snapshot = snapshot match
    case legacy: MySqlFormat3FullSnapshot => MySqlOwnedFormat3Snapshot.from(legacy, root)
    case deployment: MySqlScriptSchemaSnapshot =>
      MySqlOwnedFormat3Snapshot.fromDeploymentReceipt(deployment, deployment.inputs, root)
  private[mysql] def inventoryIdentity(tag: IoOperationTag, binding: Option[PhysicalRecoveryBinding]): MySqlCommittedOwnedModeInventoryIdentity =
    MySqlCommittedOwnedModeInventoryIdentity(tag.bootId, tag.bootGeneration, tag.connectionGeneration,
      MySqlCommittedOwnedModeRecoveryIdentity(binding))

  /** 仅保留供冻结旧 F2 纯样本使用；正式 mode writer 必须走 fromFormat3Readback。 */
  private[mysql] def fromReadback(tag: IoOperationTag, request: MySqlOwnedModeCommitRequest,
      actualCommit: OperatingModeChangeOutcome, inventory: MySqlRecoverySnapshot,
      admission: MySqlOwnedDatabaseAdmission, activeLimit: Int): MySqlCommittedOwnedModeGraph =
    require(request != null && actualCommit != null, "actual owned mode ACK/request required")
    MySqlPreparedUnboundGraph.validatedInventory(tag, request.expectedBinding, inventory, admission, activeLimit)
    val commitState = validateOutcome(request, actualCommit)
    val rows = inventory.pods.filter(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "pod_uuid") == request.podUuid)
    val locators = inventory.recovery.locators.filter(_.podUuid == request.podUuid)
    require(rows.size == 1 && locators.size == 1, "exact actual owned mode row/locator required")
    val state = MySqlStoredRows.materializeGenericState(rows.head)
    val locator = locators.head
    validateStateAndLocator(request, commitState, state, locator)
    new MySqlCommittedOwnedModeGraph(tag, request, actualCommit, state, locator,
      inventoryIdentity(tag, inventory.recovery.binding), None, Some(inventory))

  /** 从真实 F3 readback 构造终端图；不通过旧九表/locator_chunk 兼容路径。 */
  private[mysql] def fromFormat3Readback(
      tag: IoOperationTag,
      request: MySqlOwnedModeCommitRequest,
      actualCommit: OperatingModeChangeOutcome,
      snapshot: MySqlModeOperationEffectCodec.Image,
      activeLimit: Int
  ): MySqlCommittedOwnedModeGraph =
    if snapshot == null then throw new IllegalArgumentException("actual F3 mode readback is required")
    fromFormat3Readback(tag, request, actualCommit, snapshot, activeLimit, snapshot.allocationBudget)

  private[mysql] def fromFormat3Readback(
      tag: IoOperationTag,
      request: MySqlOwnedModeCommitRequest,
      actualCommit: OperatingModeChangeOutcome,
      snapshot: MySqlModeOperationEffectCodec.Image,
      activeLimit: Int,
      projectionBudget: MySqlFormat3ReadBudget
  ): MySqlCommittedOwnedModeGraph =
    require(tag != null && request != null && actualCommit != null && snapshot != null && projectionBudget != null,
      "actual F3 mode readback inputs are required")
    require(activeLimit > 0 && activeLimit <= 256, "mode readback capacity must preserve the global 256 cap")
    require(snapshot.observationScope.bootUuid == tag.bootId &&
      snapshot.observationScope.bootGeneration == tag.bootGeneration &&
      snapshot.observationScope.connectionGeneration == tag.connectionGeneration,
      "F3 mode readback belongs to another exact command/session generation")
    requireBinding(snapshot, request.expectedBinding)
    val state = validateOutcome(request, actualCommit)
    validateDomainRow(snapshot, request, state)
    val activeCount = snapshot.relation("floating_pod_state").rows.count { row =>
      MySqlModeOperationEffectCodec.value(row, "floating_pod_state", "is_active") match
        case MySqlFormat3Cell.SignedInteger(0L) => false
        case MySqlFormat3Cell.SignedInteger(1L) => true
        case _ => throw new SQLDataException("F3 mode readback has an invalid active flag")
    }
    if activeCount > activeLimit || activeCount > 256 then
      throw new SQLDataException("F3 mode readback exceeds the configured/global active capacity")
    val locator = locatorFromFormat3(snapshot, request, state)
    val owned = ownedImage(snapshot, projectionBudget)
    new MySqlCommittedOwnedModeGraph(tag, request, actualCommit, state, locator,
      inventoryIdentity(tag, Some(request.expectedBinding)), Some(owned), None)

  private def validateOutcome(request: MySqlOwnedModeCommitRequest,
      outcome: OperatingModeChangeOutcome): FloatingPodState =
    val state = outcome match
      case OperatingModeChangeOutcome.Changed(value) =>
        require(value != null && value.stateVersion == Math.addExact(request.expectedStateVersion, 1L),
          "Changed must advance the actual expected version exactly once")
        value
      case OperatingModeChangeOutcome.AlreadySet(value) =>
        require(value != null && (value.stateVersion == request.expectedStateVersion ||
          (request.expectedStateVersion < Long.MaxValue && value.stateVersion == request.expectedStateVersion + 1L)),
          "AlreadySet requires actual same-version or exact one-step replay")
        value
      case _ => throw new IllegalArgumentException("rejected mode CAS cannot issue committed graph")
    require(state.uuid == request.podUuid && state.isActive && state.ownerUuid.contains(request.authenticatedOwner) &&
      state.operatingMode == request.targetMode && poseOf(state) == request.actualPose,
      "actual owned mode outcome does not match owner, target mode, and exact observed pose")
    state

  private def validateStateAndLocator(request: MySqlOwnedModeCommitRequest, outcome: FloatingPodState,
      state: FloatingPodState, locator: PhysicalRecoveryLocator): Unit =
    require(state == outcome && state.uuid == request.podUuid && state.isActive &&
      state.ownerUuid.contains(request.authenticatedOwner) && state.operatingMode == request.targetMode &&
      poseOf(state) == request.actualPose, "actual mode ACK/owner/mode/final-pose mismatch")
    require(locator.attemptUuid == request.attemptUuid && locator.state == PhysicalRecoveryLocatorState.RowActive &&
      locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive && locator.domainVersion.contains(state.stateVersion) &&
      locator.targetDimensionId == state.dimensionId && locator.chunks.size == 25 &&
      locator.chunks.toSet == MySqlRepositoryTransitionRules.chunksAt(request.actualPose.dimensionId,
        request.actualPose.posX, request.actualPose.posZ), "complete exact owned mode locator mismatch")

  private[mysql] def validateDomainRow(snapshot: MySqlModeOperationEffectCodec.Image, request: MySqlOwnedModeCommitRequest,
      state: FloatingPodState): Unit =
    val rows = snapshot.relation("floating_pod_state").rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, "floating_pod_state", "uuid"), request.podUuid))
    if rows.size != 1 then throw new SQLDataException("F3 mode readback has no unique pod domain row")
    val row = rows.head
    def value(name: String): MySqlFormat3Cell = MySqlModeOperationEffectCodec.value(row, "floating_pod_state", name)
    def sameDouble(name: String, expected: Double): Boolean = value(name) match
      case MySqlFormat3Cell.Floating(bits) => bits == java.lang.Double.doubleToRawLongBits(expected)
      case _ => false
    val ownerMatches = (value("owner_uuid"), state.ownerUuid) match
      case (MySqlFormat3Cell.SqlNull, None) => true
      case (cell: MySqlFormat3Cell.Binary, Some(owner)) => MySqlModeOperationEffectCodec.uuidEquals(cell, owner)
      case _ => false
    val dimensionMatches = (value("dimension_id"), state.dimensionId) match
      case (MySqlFormat3Cell.SqlNull, None) => true
      case (MySqlFormat3Cell.SignedInteger(actual), Some(expected)) => actual == expected.toLong
      case _ => false
    val exact = MySqlModeOperationEffectCodec.uuidEquals(value("uuid"), state.uuid) && ownerMatches &&
      value("state_version") == MySqlFormat3Cell.SignedInteger(state.stateVersion) &&
      value("world_name") == MySqlFormat3Cell.Text(state.worldName) &&
      sameDouble("pos_x", state.posX) && sameDouble("pos_y", state.posY) && sameDouble("pos_z", state.posZ) &&
      sameDouble("yaw", state.yaw.toDouble) && sameDouble("pitch", state.pitch.toDouble) &&
      value("is_active") == MySqlFormat3Cell.SignedInteger(if state.isActive then 1L else 0L) &&
      value("last_updated") == MySqlFormat3Cell.SignedInteger(state.lastUpdated) && dimensionMatches &&
      value("operating_mode") == MySqlFormat3Cell.Text(state.operatingMode.databaseValue) &&
      MySqlModeOperationEffectCodec.uuidEquals(value("binding_uuid"), request.expectedBinding.bindingUuid)
    if !exact then throw new SQLDataException("typed F3 domain row differs from the actual owned-mode outcome")

  private[mysql] def locatorFromFormat3(snapshot: MySqlModeOperationEffectCodec.Image, request: MySqlOwnedModeCommitRequest,
      state: FloatingPodState): PhysicalRecoveryLocator =
    val locatorRows = snapshot.relation("physical_recovery_locator").rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, "physical_recovery_locator", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(
          MySqlModeOperationEffectCodec.value(row, "physical_recovery_locator", "pod_uuid"), request.podUuid))
    if locatorRows.size != 1 then throw new SQLDataException("F3 mode readback has no unique exact locator row")
    val locator = locatorRows.head
    val attempt = uuidValue(snapshot, MySqlModeOperationEffectCodec.value(locator,
      "physical_recovery_locator", "current_attempt_uuid"), "mode locator attempt")
    val footprintId = uuidValue(snapshot, MySqlModeOperationEffectCodec.value(locator,
      "physical_recovery_locator", "current_footprint_uuid"), "mode locator footprint")
    val footprintRows = snapshot.relation("physical_recovery_footprint").rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
        "physical_recovery_footprint", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
          "physical_recovery_footprint", "pod_uuid"), request.podUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
          "physical_recovery_footprint", "attempt_uuid"), attempt) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
          "physical_recovery_footprint", "footprint_uuid"), footprintId))
    if footprintRows.size != 1 then throw new SQLDataException("F3 locator does not reference one exact footprint")
    val footprint = footprintRows.head
    val seal = MySqlModeOperationEffectCodec.value(footprint, "physical_recovery_footprint", "seal_state")
    if seal != MySqlFormat3Cell.Text("SEALED") then throw new SQLDataException("F3 mode locator footprint is not sealed")
    val centerX = intValue(footprint, "physical_recovery_footprint", "center_chunk_x")
    val centerZ = intValue(footprint, "physical_recovery_footprint", "center_chunk_z")
    if intValue(footprint, "physical_recovery_footprint", "target_dimension_id") != request.actualPose.dimensionId ||
      centerX != chunk(request.actualPose.posX) || centerZ != chunk(request.actualPose.posZ) then
      throw new SQLDataException("F3 mode footprint is not exact for the actual same-chunk pose")
    val chunkRows = snapshot.relation("physical_recovery_footprint_chunk").rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
        "physical_recovery_footprint_chunk", "binding_uuid"), request.expectedBinding.bindingUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
          "physical_recovery_footprint_chunk", "pod_uuid"), request.podUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
          "physical_recovery_footprint_chunk", "attempt_uuid"), attempt) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row,
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
    val domainVersion = MySqlModeOperationEffectCodec.value(locator, "physical_recovery_locator", "domain_version") match
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
    MySqlModeOperationEffectCodec.value(row, table, column) match
      case MySqlFormat3Cell.Text(value) => value
      case _ => throw new SQLDataException(table + "." + column + " is not typed text")

  private def intValue(row: Vector[MySqlFormat3Cell], table: String, column: String): Int =
    MySqlModeOperationEffectCodec.value(row, table, column) match
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

/** 已存在的精确 mode 状态与同次完整 F3 读取；不表示发生了写入或关键提交。 */
final class MySqlOwnedModeAlreadySetReadback private[mysql] (
    val tag: IoOperationTag,
    val operationPlan: MySqlOwnedRepositoryOperationPlan,
    val request: MySqlOwnedModeCommitRequest,
    val state: FloatingPodState,
    val locator: PhysicalRecoveryLocator,
    val inventory: MySqlCommittedOwnedModeInventoryIdentity,
    val fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot
):
  require(tag != null && operationPlan != null && operationPlan.tag == tag && request != null && state != null && locator != null && inventory != null &&
    fullTwentyRelationSnapshot != null, "exact no-change mode readback is required")

object MySqlOwnedModeAlreadySetReadback:
  /** 只由 actual locked planning full20 读取发行；旧版本+1 不能被 mode/pose 相同误认成此计划的结果。 */
  private[mysql] def fromFormat3Readback(
      tag: IoOperationTag,
      operationPlan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlOwnedModeCommitRequest,
      state: FloatingPodState,
      snapshot: MySqlModeOperationEffectCodec.Image,
      projectionBudget: Option[MySqlFormat3ReadBudget] = None
  ): MySqlOwnedModeAlreadySetReadback =
    if tag == null || operationPlan == null || request == null || state == null || snapshot == null then
      throw new IllegalArgumentException("actual no-change F3 planning readback inputs are required")
    operationPlan.requireTag(tag)
    operationPlan.requireSingle(request.repositoryOperation, snapshot.limitProfile)
    if snapshot.observationScope.bootUuid != tag.bootId ||
      snapshot.observationScope.bootGeneration != tag.bootGeneration ||
      snapshot.observationScope.connectionGeneration != tag.connectionGeneration then
      throw new SQLDataException("no-change F3 snapshot belongs to another exact command/session generation")
    MySqlCommittedOwnedModeGraph.requireBinding(snapshot, request.expectedBinding)
    MySqlModeOperationEffectCodec.preflight(snapshot, request.repositoryOperation) match
      case MySqlModePreflight.AlreadySet => ()
      case _ => throw new SQLDataException("full20 snapshot does not prove an exact no-change mode result")
    if state.stateVersion != request.expectedStateVersion || state.uuid != request.podUuid ||
      state.ownerUuid != Some(request.authenticatedOwner) || !state.isActive ||
      state.operatingMode != request.targetMode || MySqlCommittedOwnedModeGraph.poseOf(state) != request.actualPose then
      throw new SQLDataException("no-change state differs from the exact retained owner/version/mode/pose request")
    MySqlCommittedOwnedModeGraph.validateDomainRow(snapshot, request, state)
    val locator = MySqlCommittedOwnedModeGraph.locatorFromFormat3(snapshot, request, state)
    val activeCount = snapshot.relation("floating_pod_state").rows.count { row =>
      MySqlModeOperationEffectCodec.value(row, "floating_pod_state", "is_active") match
        case MySqlFormat3Cell.SignedInteger(0L) => false
        case MySqlFormat3Cell.SignedInteger(1L) => true
        case _ => throw new SQLDataException("no-change full20 snapshot contains an invalid active flag")
    }
    if activeCount > 256 then throw new SQLDataException("no-change full20 snapshot exceeds the global active limit")
    val ownedSnapshot = MySqlCommittedOwnedModeGraph.ownedImage(snapshot,
      projectionBudget.getOrElse(snapshot.allocationBudget))
    new MySqlOwnedModeAlreadySetReadback(tag, operationPlan, request, state, locator,
      MySqlCommittedOwnedModeGraph.inventoryIdentity(tag, Some(request.expectedBinding)), ownedSnapshot)

/** None ACK只代表未观察到结果，不表示未提交/已回滚；Unknown原cause不被改写。 */
final case class MySqlOwnedModeExecutionFailure(tag: IoOperationTag, request: MySqlOwnedModeCommitRequest,
    actualAck: Option[OperatingModeChangeOutcome], cause: Throwable):
  require(tag != null && request != null && actualAck != null && cause != null, "actual mode responsibility required")
  require(actualAck.forall(value => value.isInstanceOf[OperatingModeChangeOutcome.Changed] ||
    value.isInstanceOf[OperatingModeChangeOutcome.AlreadySet]), "only actual successful mode ACK belongs to readback responsibility")

/** 原Throwable有独立checkpoint；此suppressed context不持cause，避免反向循环。 */
final class MySqlOwnedModeObligationNotice(val tag: IoOperationTag, val request: MySqlOwnedModeCommitRequest,
    val actualAck: Option[OperatingModeChangeOutcome])
    extends IllegalStateException("owned mode execution responsibility retained; unobserved ACK is not rollback proof")
