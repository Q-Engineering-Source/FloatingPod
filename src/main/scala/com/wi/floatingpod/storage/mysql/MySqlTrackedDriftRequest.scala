package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.util.UUID

/** Main captures a critical base once; later dirty poses do not replace an accepted immutable request. */
final case class MySqlTrackedDriftRequest(binding: PhysicalRecoveryBinding, criticalBase: FloatingPodState,
    desired: FloatingPodState, attemptUuid: UUID):
  MySqlTrackedDriftRequest.requireSameChunkPoseOnly(binding, criticalBase, desired, attemptUuid)
  def expectedBinding: PhysicalRecoveryBinding = binding
  def podUuid: UUID = criticalBase.uuid
  def authenticatedOwner: Option[UUID] = criticalBase.ownerUuid
  def expectedStateVersion: Long = criticalBase.stateVersion
  def targetMode = criticalBase.operatingMode
  def actualPose = com.wi.floatingpod.domain.FloatingPodPose(desired.worldName, desired.posX, desired.posY,
    desired.posZ, desired.yaw, desired.pitch, desired.dimensionId.get)
  def repositoryOperation: MySqlRepositoryOperation =
    MySqlRepositoryOperation.CommitTrackedActiveDrift(binding, criticalBase, desired, attemptUuid)

object MySqlTrackedDriftRequest:
  private[floatingpod] def requireSameChunkPoseOnly(binding: PhysicalRecoveryBinding, base: FloatingPodState,
      desired: FloatingPodState, attempt: UUID): Unit =
    require(binding != null && binding.cutoverState == PhysicalRecoveryCutoverState.Certified && binding.formatVersion == 2 &&
      base != null && desired != null && attempt != null, "exact certified physical2 drift request required")
    require(base.isActive && desired.isActive && base.dimensionId.nonEmpty && base.uuid == desired.uuid &&
      base.ownerUuid == desired.ownerUuid && base.operatingMode == desired.operatingMode &&
      base.stateVersion == desired.stateVersion && base.lastUpdated == desired.lastUpdated &&
      base.dimensionId == desired.dimensionId && base.worldName == desired.worldName,
      "drift may change only the same active critical base's pose")
    require(chunk(base.posX) == chunk(desired.posX) && chunk(base.posZ) == chunk(desired.posZ),
      "tracked drift must retain the exact center chunk")
  private def chunk(value: Double): Int =
    val result = Math.floor(value / 16.0)
    require(result >= Int.MinValue.toDouble && result <= Int.MaxValue.toDouble, "chunk coordinate exceeds Int")
    result.toInt

/** 专用drift业务拒绝，不冒充mode CAS结果或SQL成功。 */
enum MySqlTrackedDriftRejection:
  case BindingMismatch, UnknownPod, Inactive, PhysicalContextMismatch, CrossChunkMovement
  case CriticalBaseMismatch(actualVersion: Long)
