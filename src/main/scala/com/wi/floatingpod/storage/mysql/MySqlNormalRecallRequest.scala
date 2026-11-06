package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState}
import java.util.UUID

/** Main 的不可变输入；它不发行 removal、恢复或 SQL 许可。 */
private[floatingpod] final case class MySqlNormalRecallRequest(
    stopRequest: UUID, expectedBinding: PhysicalRecoveryBinding, criticalBase: FloatingPodState,
    finalPose: FloatingPodPose, dirtyRevision: Long, attemptUuid: UUID, footprintUuid: UUID,
    originalFootprint: Vector[PhysicalRecoveryChunk], source: MySqlNormalRecallSourceIdentity,
    purpose: MySqlNormalRecallPurpose = MySqlNormalRecallPurpose.NormalRecall):
  import MySqlNormalRecallRequest.*
  require(nonZero(stopRequest) && expectedBinding != null &&
    expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified && expectedBinding.formatVersion == 2,
    "recall requires the original Certified binding and stop request")
  require(purpose != null && source != null && criticalBase != null &&
    criticalBase.isActive == (purpose != MySqlNormalRecallPurpose.FinishCommittedInactive) &&
    criticalBase.operatingMode == PodOperatingMode.Normal &&
    criticalBase.ownerUuid.exists(nonZero) && criticalBase.dimensionId.nonEmpty && finalPose != null &&
    finalPose.worldName == criticalBase.worldName && finalPose.dimensionId == criticalBase.dimensionId.get &&
    dirtyRevision >= 0L && nonZero(attemptUuid) && nonZero(footprintUuid),
    "recall requires the frozen original owned Normal active base")
  requireExactFootprint(originalFootprint, finalPose)
  require(chunk(criticalBase.posX) == chunk(finalPose.posX) && chunk(criticalBase.posZ) == chunk(finalPose.posZ),
    "recall cannot discard an unsettled footprint transfer")
  def podUuid: UUID = criticalBase.uuid
  def committedPose: FloatingPodPose = FloatingPodPose(criticalBase.worldName, criticalBase.posX,
    criticalBase.posY, criticalBase.posZ, criticalBase.yaw, criticalBase.pitch, criticalBase.dimensionId.get)

private[floatingpod] object MySqlNormalRecallRequest:
  private[mysql] def nonZero(value: UUID): Boolean =
    value != null && (value.getMostSignificantBits | value.getLeastSignificantBits) != 0L

  private[mysql] def chunk(value: Double): Int =
    require(java.lang.Double.isFinite(value), "finite recall coordinate required")
    val result = Math.floor(value / 16.0)
    require(result >= Int.MinValue.toDouble && result <= Int.MaxValue.toDouble, "recall chunk exceeds Int")
    result.toInt

  private[mysql] def requireExactFootprint(value: Vector[PhysicalRecoveryChunk], pose: FloatingPodPose): Unit =
    require(value != null && pose != null && value.size == 25 && !value.exists(_ == null) && value.distinct.size == 25,
      "recall requires the exact original twenty-five chunks")
    val x = chunk(pose.posX).toLong
    val z = chunk(pose.posZ).toLong
    require(value.forall(c => c.dimensionId == pose.dimensionId &&
      c.chunkX.toLong >= x - 2L && c.chunkX.toLong <= x + 2L &&
      c.chunkZ.toLong >= z - 2L && c.chunkZ.toLong <= z + 2L), "recall footprint differs from frozen pose")

/** 每个提交阶段独立 O；恢复结算不产生旧 boot 的 recall receipt。 */
private[floatingpod] enum MySqlNormalRecallPurpose(val wire: Int):
  case NormalRecall extends MySqlNormalRecallPurpose(1)
  case RestoreCommittedActive extends MySqlNormalRecallPurpose(2)
  case FinishCommittedInactive extends MySqlNormalRecallPurpose(3)

private[mysql] enum MySqlNormalRecallStage(val step: String):
  case Prepare extends MySqlNormalRecallStage("RECALL_PREPARE")
  case Checkpoint extends MySqlNormalRecallStage("RECALL_CHECKPOINT")
  case RecoverySettlement extends MySqlNormalRecallStage("RESERVATION_RELEASE")

/** 完整标量身份只作 DATA；实际 context 仍须从原 command/session 捕获。 */
private[mysql] final case class MySqlNormalRecallHeader(
    request: MySqlNormalRecallRequest, context: MySqlRepositoryOperationContextIdentity,
    previousOperation: Option[UUID], locatorRevision: Long):
  require(request != null && context != null && previousOperation != null &&
    previousOperation.forall(MySqlNormalRecallRequest.nonZero) && locatorRevision >= 0L &&
    context.bindingUuid == request.expectedBinding.bindingUuid &&
    context.tag.operationId != context.reserveOperationUuid &&
    !previousOperation.contains(context.tag.operationId) && !previousOperation.contains(context.reserveOperationUuid),
    "complete distinct recall operation identities required")

/** F14B 映射的完整 DATA 值；构造本值不验证路径、custody或发行Ready。 */
private[floatingpod] final case class MySqlNormalRecallSourceIdentity(worldIdentity: String, sourceIdentity: String):
  private val nativePattern = "FP_WIN32_FILE_ID_V1;volume=[0-9a-f]{16};id=[0-9a-f]{32}"
  require(worldIdentity != null && sourceIdentity != null && worldIdentity.matches(nativePattern) &&
    sourceIdentity.matches(nativePattern) && worldIdentity == sourceIdentity,
    "current native world and inherited full source identities must match exactly")

/** 观察结果值不具有权限。生产值必须由原 Main pending 与 opaque ACK 转录后使用。 */
private[floatingpod] final case class MySqlNormalRecallRemovalFacts(
    tag: IoOperationTag, podUuid: UUID, attemptUuid: UUID, expectedVersion: Long,
    dimensionId: Int, entityId: Int, capturedEnd: Long, requestedEnd: Long, observedEnd: Long):
  require(tag != null && MySqlNormalRecallRequest.nonZero(tag.bootId) &&
    MySqlNormalRecallRequest.nonZero(tag.operationId) && MySqlNormalRecallRequest.nonZero(podUuid) &&
    MySqlNormalRecallRequest.nonZero(attemptUuid) && expectedVersion >= 0L && capturedEnd >= 0L &&
    requestedEnd >= capturedEnd && observedEnd > requestedEnd,
    "recall requires the actual subsequent END, not a returned removal request")

private[floatingpod] final case class MySqlNormalRecallTicketFacts(
    removal: MySqlNormalRecallRemovalFacts, stopRequest: UUID, footprint: Vector[PhysicalRecoveryChunk]):
  require(removal != null && MySqlNormalRecallRequest.nonZero(stopRequest) && footprint != null &&
    footprint.size == 25 && !footprint.exists(_ == null) && footprint.distinct.size == 25 &&
    footprint.forall(_.dimensionId == removal.dimensionId), "exact removal and original ticket footprint required")
  require(footprint.map(_.chunkX.toLong).max - footprint.map(_.chunkX.toLong).min == 4L &&
    footprint.map(_.chunkZ.toLong).max - footprint.map(_.chunkZ.toLong).min == 4L,
    "original ticket must be one exact five-by-five footprint")

/** 只能在两份实际 opaque ACK 已转录后提交的不可变 DATA。权限仍由原 Main owner 保留。 */
private[floatingpod] final case class MySqlNormalRecallCheckpointRequest(
    original: MySqlNormalRecallRequest, removal: MySqlNormalRecallRemovalFacts,
    ticket: MySqlNormalRecallTicketFacts, receiptUuid: UUID):
  require(original != null && original.purpose == MySqlNormalRecallPurpose.NormalRecall && removal != null &&
    ticket != null && ticket.removal == removal && ticket.stopRequest == original.stopRequest &&
    MySqlNormalRecallRequest.nonZero(receiptUuid) && removal.podUuid == original.podUuid &&
    removal.attemptUuid == original.attemptUuid && removal.expectedVersion == original.criticalBase.stateVersion &&
    removal.dimensionId == original.finalPose.dimensionId && ticket.footprint.toSet == original.originalFootprint.toSet,
    "checkpoint requires both physical facts from the exact original Normal request")

/** 完整恢复阶段数据必须保留原 O；UNKNOWN 不能被解释为 active 恢复。 */
private[mysql] enum MySqlNormalRecallCheckpointDecision:
  case Committed
  case DefinitivelyNotCommitted
  case Unknown

private[mysql] object MySqlNormalRecallRecoveryTarget:
  def purpose(decision: MySqlNormalRecallCheckpointDecision): Option[MySqlNormalRecallPurpose] =
    require(decision != null, "original checkpoint decision required")
    decision match
      case MySqlNormalRecallCheckpointDecision.Committed => Some(MySqlNormalRecallPurpose.FinishCommittedInactive)
      case MySqlNormalRecallCheckpointDecision.DefinitivelyNotCommitted => Some(MySqlNormalRecallPurpose.RestoreCommittedActive)
      case MySqlNormalRecallCheckpointDecision.Unknown => None
