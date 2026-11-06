package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}

import java.util.UUID
import scala.util.Try

/** 提供带迁移兼容检查的恢复读取入口。 */
trait RecoveryPodStore:
  /** 读取所有标记为活动的原始恢复行，包括 V001 遗留的空维度行。 */
  def findDesiredActiveRows(): Try[List[StoredPodRow]]

  /** 按标识读取原始恢复行，供权威回调决定是否补齐维度。 */
  def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]]

  /**
   * 仅在活动行尚无维度时，按同一状态版本写入权威回调提供的维度。
   *
   * 该补齐不改变 `stateVersion`、位置、旋转或时间戳；相同回调可安全重放。
   */
  def backfillMissingActiveDimension(
      uuid: UUID,
      expectedStateVersion: Long,
      authoritativeDimensionId: Int
  ): Try[RecoveryDimensionBackfillOutcome]

  /**
   * 返回可安全恢复的持久状态。
   *
   * V001 遗留的活动空维度行会在此边界明确失败，而不会被误恢复到错误维度。
   */
  def findAllRecoverable(): Try[List[FloatingPodState]]

/**
 * SQLite 恢复行的内部表示，保留 V001 迁移到 V002 后可能出现的活动空维度。
 *
 * 不向生命周期层泄漏该中间形态；转换为领域状态时必须拒绝它。
 */
private[floatingpod] final case class StoredPodRow(
    uuid: UUID,
    ownerUuid: Option[UUID],
    stateVersion: Long,
    worldName: String,
    posX: Double,
    posY: Double,
    posZ: Double,
    yaw: Float,
    pitch: Float,
    isActive: Boolean,
    lastUpdated: Long,
    dimensionId: Option[Int],
    operatingMode: PodOperatingMode = PodOperatingMode.Normal
):
  require(uuid != null, "uuid must not be null")
  require(ownerUuid != null, "ownerUuid must not be null")
  require(dimensionId != null, "dimensionId must not be null")
  require(operatingMode != null, "operatingMode must not be null")

  /** 仅在公开领域不变量满足时构造领域状态。 */
  def toPublicState: FloatingPodState =
    if isActive && dimensionId.isEmpty then
      throw new IllegalStateException(s"Active FloatingPod $uuid has no persisted dimension")
    FloatingPodState(
      uuid,
      ownerUuid,
      stateVersion,
      worldName,
      posX,
      posY,
      posZ,
      yaw,
      pitch,
      isActive,
      lastUpdated,
      dimensionId,
      operatingMode
    )

/** 回填活动维度时的稳定结果。 */
sealed trait RecoveryDimensionBackfillOutcome

object RecoveryDimensionBackfillOutcome:
  final case class Ready(state: FloatingPodState) extends RecoveryDimensionBackfillOutcome
  final case class Rejected(reason: RecoveryDimensionBackfillRejection) extends RecoveryDimensionBackfillOutcome

/** 权威回填的业务拒绝原因。 */
sealed trait RecoveryDimensionBackfillRejection

object RecoveryDimensionBackfillRejection:
  case object UnknownPod extends RecoveryDimensionBackfillRejection
  case object InactivePod extends RecoveryDimensionBackfillRejection
  final case class VersionMismatch(actualVersion: Long) extends RecoveryDimensionBackfillRejection
  final case class DimensionConflict(persistedDimensionId: Int) extends RecoveryDimensionBackfillRejection
