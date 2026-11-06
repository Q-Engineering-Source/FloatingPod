package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.FloatingPodRepository

import java.util.UUID

/** 表示无副作用所有权检查的明确结果，供后续 handler 映射为 bounded protocol reason。 */
sealed trait OwnershipValidation

/** 所有权、版本与未知资源检查通过时携带刚从权威仓储读取的状态。 */
final case class OwnershipAccepted(state: FloatingPodState) extends OwnershipValidation

/** 指定 UUID 尚无任何权威持久化状态。 */
case object UnknownPod extends OwnershipValidation

/** 请求玩家不是刚读取状态的所有者。 */
case object NotOwner extends OwnershipValidation

/** 请求携带的乐观锁版本与刚读取的权威版本不同。 */
final case class VersionMismatch(actualVersion: Long) extends OwnershipValidation

/** 每次请求都直接读取权威 repository 的所有权与版本验证 seam，不缓存且不修改状态。 */
final class OwnershipValidator private (repository: FloatingPodRepository, bootReads: Option[MainPodReadAccess]):
  def this(repository: FloatingPodRepository) = this(repository, None)
  require(bootReads != null && (bootReads.nonEmpty || repository != null), "repository or actual Main Boot read lease required")

  /**
   * 以一次新的 UUID 查询依序确认存在性、所有权与版本；repository 失败会保留原始失败而非伪造拒绝。
   */
  def validate(podUuid: UUID, playerUuid: UUID, expectedStateVersion: Long): OwnershipValidation =
    require(podUuid != null, "podUuid must not be null")
    require(playerUuid != null, "playerUuid must not be null")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    val state = bootReads match
      case Some(projection) => projection.findState(podUuid)
      case None => repository.findByUuid(podUuid).get
    state match
      case None => UnknownPod
      case Some(state) if !state.ownerUuid.contains(playerUuid) => NotOwner
      case Some(state) if state.stateVersion != expectedStateVersion => VersionMismatch(state.stateVersion)
      case Some(state) => OwnershipAccepted(state)

object OwnershipValidator:
  /** 仅 Boot 读预览；最终 lifecycle/critical ACK 仍独立必要。退休时不回退 SQL。 */
  private[floatingpod] def fromMainBoot(projection: MainBootProjection): OwnershipValidator =
    fromMainReads(projection)

  private[floatingpod] def fromMainReads(projection: MainPodReadAccess): OwnershipValidator =
    require(projection != null, "actual claimed Boot read lease required")
    projection.requireCurrentReads()
    new OwnershipValidator(null, Some(projection))
