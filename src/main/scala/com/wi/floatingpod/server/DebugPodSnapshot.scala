package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}

import java.util.UUID

/** 当前可由调试入口展示的生命周期阶段；只有 Committed 才表示数据库和 manager 均已接管。 */
enum DebugPodPhase:
  case Prepared, Ticketed, PendingCleanup, Committed, Faulted, PendingRecall

/** 调试入口返回的权威状态快照；缺少运行时确认时必须保留非成功阶段。 */
final case class DebugPodSnapshot(
    podUuid: UUID,
    phase: DebugPodPhase,
    ownerUuid: Option[UUID],
    operatingMode: Option[PodOperatingMode],
    isActive: Boolean,
    stateVersion: Option[Long],
    observedPose: Option[FloatingPodPose],
    entityFrozen: Option[Boolean],
    bindingUuid: Option[UUID],
    attemptUuid: Option[UUID],
    operationUuid: Option[UUID],
    cause: Option[Throwable]
):
  require(podUuid != null && phase != null, "debug pod snapshot identity must not be null")
  require(ownerUuid != null && ownerUuid.forall(_ != null), "ownerUuid option must not be null")
  require(operatingMode != null && operatingMode.forall(_ != null), "operatingMode option must not be null")
  require(stateVersion != null && stateVersion.forall(_ >= 0L), "stateVersion option must not be null or negative")
  require(observedPose != null && observedPose.forall(_ != null), "observedPose option must not be null")
  require(entityFrozen != null, "entityFrozen option must not be null")
  require(bindingUuid != null && bindingUuid.forall(_ != null), "bindingUuid option must not be null")
  require(attemptUuid != null && attemptUuid.forall(_ != null), "attemptUuid option must not be null")
  require(operationUuid != null && operationUuid.forall(_ != null), "operationUuid option must not be null")
  require(cause != null, "cause option must not be null")

  /** Pending、物理尚未确认或提交后映射失败均不能伪装为已初始化。 */
  def isInitialized: Boolean =
    phase == DebugPodPhase.Committed
      && isActive
      && ownerUuid.isEmpty
      && operatingMode.contains(PodOperatingMode.WorldIdle)
      && stateVersion.nonEmpty
      && observedPose.nonEmpty
      && entityFrozen.contains(true)
      && cause.isEmpty

/** 仅受控的新实体路径可创建此句柄；外部观察到的原体必须走 adoption。 */
trait ControlledActivePodEntity extends ActivePodEntity:
  def actualPose: FloatingPodPose
  def isFrozen: Boolean
  def sameBody(other: ControlledActivePodEntity): Boolean
  def hasControlledCreationStamp(receipt: com.wi.floatingpod.storage.UnboundPreparationReceipt): Boolean
  def applyCommittedControl(state: com.wi.floatingpod.domain.FloatingPodState, receipt: com.wi.floatingpod.storage.UnboundPreparationReceipt): Unit
  def applyCommittedMode(state: com.wi.floatingpod.domain.FloatingPodState, attemptUuid: UUID): Unit
  def markControlFaulted(cause: Throwable): Unit

  /** 默认不可用；旧adapter不能伪造operation-specific冻结。 */
  def beginPendingMode(operation: UUID, owner: UUID, version: Long): Unit =
    throw new UnsupportedOperationException("Actual operation-specific mode freeze unavailable")
  def hasPendingMode(operation: UUID, version: Long): Boolean =
    throw new UnsupportedOperationException("Actual operation-specific mode freeze observation unavailable")
  def finishPendingMode(operation: UUID, expectedVersion: Long, state: com.wi.floatingpod.domain.FloatingPodState,
      attemptUuid: UUID): Unit =
    throw new UnsupportedOperationException("Actual operation-specific mode ACK publication unavailable")
