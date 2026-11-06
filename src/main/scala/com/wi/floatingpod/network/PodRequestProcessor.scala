package com.wi.floatingpod.network

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.server.{ActivePodEntity, AuthoritativePosition, NotOwner, OwnershipAccepted, OwnershipValidator, PodWorld, PositionValidation, ReleasePosition, ReleasePositionPolicy, UnknownPod, VersionMismatch}
import com.wi.floatingpod.storage.{PlayerEntitlementStore, RecoveryPodStore}

import com.wi.floatingpod.server.{MainBootProjection, MainPodReadAccess}
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.MainReadPending

import java.util.UUID
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** 服务端已解析的玩家权威上下文；任何字段均不来自 C2S payload。 */
trait ServerPlayerContext:
  def playerUuid: UUID
  def position: AuthoritativePosition
  def maximumReach: Double
  /** 仅供已验证 Release 调用使用的服务器权威生成世界。 */
  def spawnWorld: PodWorld
  def hasSolidSupport(target: ReleasePosition): Boolean
  def hasTwoBlockHeadroom(target: ReleasePosition): Boolean
  /** 将 manager 中当前活动实体与玩家的服务器维度证据比较。 */
  def isSameDimension(activePod: ActivePodEntity): Boolean

/** Release 权益预览的窄边界；生产实现读取持久权益，测试可显式替换。 */
trait PlayerEntitlementPolicy:
  def mayRelease(player: ServerPlayerContext, state: FloatingPodState): Boolean

/** 仅供纯测试或显式临时装配使用的无条件策略；生产 runtime 不得注入它。 */
object AllowAllPlayerEntitlementPolicy extends PlayerEntitlementPolicy:
  override def mayRelease(player: ServerPlayerContext, state: FloatingPodState): Boolean = true

/** 以持久权益和数据库 desired-active 行完成 Release 的无写入预览。 */
final class PersistedPlayerEntitlementPolicy private (entitlements: PlayerEntitlementStore, activeRows: RecoveryPodStore,
    bootReads: Option[MainPodReadAccess]) extends PlayerEntitlementPolicy:
  def this(entitlements: PlayerEntitlementStore, activeRows: RecoveryPodStore) = this(entitlements, activeRows, None)
  require(bootReads != null && (bootReads.nonEmpty || (entitlements != null && activeRows != null)),
    "stores or actual Main Boot read lease required")

  override def mayRelease(player: ServerPlayerContext, state: FloatingPodState): Boolean =
    require(player != null, "player must not be null")
    val playerUuid = player.playerUuid
    require(playerUuid != null, "playerUuid must not be null")
    val found = bootReads match
      case Some(projection) => projection.findEntitlement(playerUuid)
      case None => entitlements.findByPlayerUuid(playerUuid).get
    val entitlement = found.getOrElse(PlayerEntitlement(playerUuid))
    if entitlement.playerUuid != playerUuid then
      throw new IllegalStateException(s"Entitlement repository returned ${entitlement.playerUuid} for player $playerUuid")
    val activeCount = bootReads match
      case Some(projection) => projection.activeCountFor(playerUuid)
      case None => activeRows.findDesiredActiveRows().get.count(_.ownerUuid.contains(playerUuid))
    activeCount < entitlement.simultaneousActiveLimit

object PersistedPlayerEntitlementPolicy:
  private[floatingpod] def fromMainBoot(projection: MainBootProjection): PersistedPlayerEntitlementPolicy =
    fromMainReads(projection)

  private[floatingpod] def fromMainReads(projection: MainPodReadAccess): PersistedPlayerEntitlementPolicy =
    require(projection != null, "actual claimed Boot read lease required")
    projection.requireCurrentReads()
    new PersistedPlayerEntitlementPolicy(null, null, Some(projection))

/** lifecycle 成功后提供的唯一权威持久化/同步快照。 */
final case class PodLifecycleSnapshot(state: FloatingPodState):
  require(state != null, "state must not be null")

/** 隔离活动实体生命周期；processor 绝不复制其提交、清理或回滚逻辑。 */
trait PodLifecyclePort:
  /** 在 Recall mutation 前从权威运行态检查玩家与 Pod 是否同维度。 */
  def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean]
  def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome]
  def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome]

sealed trait ReleaseLifecycleOutcome
object ReleaseLifecycleOutcome:
  final case class Accepted(snapshot: PodLifecycleSnapshot) extends ReleaseLifecycleOutcome
  final case class Rejected(reason: ReleaseFailureReason) extends ReleaseLifecycleOutcome

sealed trait RecallLifecycleOutcome
object RecallLifecycleOutcome:
  final case class Accepted(snapshot: PodLifecycleSnapshot) extends RecallLifecycleOutcome
  final case class Rejected(reason: RecallFailureReason) extends RecallLifecycleOutcome

/**
 * 请求审计的三种稳定级别；客户端永远不会取得 Throwable 的文字。
 *
 * 实现必须不抛出异常。若违反此契约，异常直接交给调用边界，processor 不会再次映射或重记，
 * 从而保持每个已完成请求至多一次审计尝试。
 */
trait RequestLog:
  def rejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason): Unit
  def success(operation: String, playerUuid: UUID, podUuid: UUID, authoritativeVersion: Long): Unit
  def infrastructureFailure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable): Unit

final case class ReleaseProcessingOutcome(result: S2CReleaseResult, sync: Option[S2CStateSync])
final case class RecallProcessingOutcome(result: S2CRecallResult, sync: Option[S2CStateSync])

/** 按协议固定顺序执行服务端请求验证的深模块。 */
final class PodRequestProcessor(
    ownership: OwnershipValidator,
    capacity: ReleaseCapacityPolicy,
    position: ReleasePositionPolicy,
    entitlement: PlayerEntitlementPolicy,
    lifecycle: PodLifecyclePort,
    log: RequestLog
):
  require(ownership != null, "ownership must not be null")
  require(capacity != null, "capacity must not be null")
  require(position != null, "position must not be null")
  require(entitlement != null, "entitlement must not be null")
  require(lifecycle != null, "lifecycle must not be null")
  require(log != null, "log must not be null")

  def release(request: C2SReleaseRequest, player: ServerPlayerContext): ReleaseProcessingOutcome =
    require(request != null, "request must not be null")
    require(player != null, "player must not be null")
    require(request.podUuid != null, "request podUuid must not be null")
    require(request.targetPos != null, "request targetPos must not be null")
    require(request.expectedOwnerVersion >= 0L, "request expectedOwnerVersion must not be negative")
    val playerUuid = player.playerUuid
    require(playerUuid != null, "playerUuid must not be null")
    val podUuid = request.podUuid

    val ownershipResult =
      try ownership.validate(podUuid, playerUuid, request.expectedOwnerVersion)
      catch
        case _: MainReadPending => return rejectRelease(playerUuid, podUuid, ReleaseFailureReason.Busy)
        case NonFatal(cause) => return infrastructureRelease(playerUuid, podUuid, cause)

    ownershipResult match
      case UnknownPod => rejectRelease(playerUuid, podUuid, ReleaseFailureReason.UnknownPod)
      case NotOwner => rejectRelease(playerUuid, podUuid, ReleaseFailureReason.NotOwner)
      case VersionMismatch(_) => rejectRelease(playerUuid, podUuid, ReleaseFailureReason.VersionMismatch)
      case OwnershipAccepted(state) if state.isActive => rejectRelease(playerUuid, podUuid, ReleaseFailureReason.AlreadyActive)
      case OwnershipAccepted(state) =>
        val hasCapacity =
          try capacity.canRelease
          catch
            case _: MainReadPending => return rejectRelease(playerUuid, podUuid, ReleaseFailureReason.Busy)
            case NonFatal(cause) => return infrastructureRelease(playerUuid, podUuid, cause)
        if !hasCapacity then rejectRelease(playerUuid, podUuid, ReleaseFailureReason.CapacityReached)
        else
          val target = ReleasePosition(request.targetPos.getX, request.targetPos.getY, request.targetPos.getZ)
          val positionResult =
            try position.validate(player.position, target, player.maximumReach, player.hasSolidSupport(target), player.hasTwoBlockHeadroom(target))
            catch case NonFatal(cause) => return infrastructureRelease(playerUuid, podUuid, cause)
          positionResult match
            case PositionValidation.Accepted =>
              val isEntitled =
                try entitlement.mayRelease(player, state)
                catch
                  case _: MainReadPending => return rejectRelease(playerUuid, podUuid, ReleaseFailureReason.Busy)
                  case NonFatal(cause) => return infrastructureRelease(playerUuid, podUuid, cause)
              if !isEntitled then rejectRelease(playerUuid, podUuid, ReleaseFailureReason.EntitlementDenied)
              else
                val releaseResult =
                  try lifecycle.releaseOutcome(state, target, player)
                  catch case NonFatal(cause) => return releaseLifecycleFailure(playerUuid, podUuid, cause)
                releaseResult match
                  case Success(ReleaseLifecycleOutcome.Accepted(snapshot)) => succeedRelease(playerUuid, snapshot)
                  case Success(ReleaseLifecycleOutcome.Rejected(reason)) => rejectRelease(playerUuid, podUuid, reason)
                  case Failure(cause) => releaseLifecycleFailure(playerUuid, podUuid, cause)
            case _ => rejectRelease(playerUuid, podUuid, ReleaseFailureReason.InvalidPosition)

  def recall(request: C2SRecallRequest, player: ServerPlayerContext): RecallProcessingOutcome =
    require(request != null, "request must not be null")
    require(player != null, "player must not be null")
    require(request.podUuid != null, "request podUuid must not be null")
    require(request.expectedStateVersion >= 0L, "request expectedStateVersion must not be negative")
    val playerUuid = player.playerUuid
    require(playerUuid != null, "playerUuid must not be null")
    val podUuid = request.podUuid

    val ownershipResult =
      try ownership.validate(podUuid, playerUuid, request.expectedStateVersion)
      catch
        case _: MainReadPending => return rejectRecall(playerUuid, podUuid, RecallFailureReason.Busy)
        case NonFatal(cause) => return infrastructureRecall(playerUuid, podUuid, cause)

    ownershipResult match
      case UnknownPod => rejectRecall(playerUuid, podUuid, RecallFailureReason.UnknownPod)
      case NotOwner => rejectRecall(playerUuid, podUuid, RecallFailureReason.NotOwner)
      case VersionMismatch(_) => rejectRecall(playerUuid, podUuid, RecallFailureReason.VersionMismatch)
      case OwnershipAccepted(state) if !state.isActive => rejectRecall(playerUuid, podUuid, RecallFailureReason.NotActive)
      case OwnershipAccepted(state) =>
        val dimensionResult =
          try lifecycle.isSameDimension(state, player)
          catch case NonFatal(cause) => return infrastructureRecall(playerUuid, podUuid, cause)
        dimensionResult match
          case Success(false) => rejectRecall(playerUuid, podUuid, RecallFailureReason.WrongDimension)
          case Failure(cause) => infrastructureRecall(playerUuid, podUuid, cause)
          case Success(true) =>
            val recallResult =
              try lifecycle.recallOutcome(state, player)
              catch case NonFatal(cause) => return recallLifecycleFailure(playerUuid, podUuid, cause)
            recallResult match
              case Success(RecallLifecycleOutcome.Accepted(snapshot)) => succeedRecall(playerUuid, snapshot)
              case Success(RecallLifecycleOutcome.Rejected(reason)) => rejectRecall(playerUuid, podUuid, reason)
              case Failure(cause) => recallLifecycleFailure(playerUuid, podUuid, cause)

  private def rejectRelease(playerUuid: UUID, id: UUID, reason: ReleaseFailureReason): ReleaseProcessingOutcome =
    log.rejection("release", playerUuid, id, reason)
    ReleaseProcessingOutcome(S2CReleaseResult.failed(id, reason), None)

  private def rejectRecall(playerUuid: UUID, id: UUID, reason: RecallFailureReason): RecallProcessingOutcome =
    log.rejection("recall", playerUuid, id, reason)
    RecallProcessingOutcome(S2CRecallResult.failed(id, reason), None)

  private def infrastructureRelease(playerUuid: UUID, id: UUID, cause: Throwable): ReleaseProcessingOutcome =
    log.infrastructureFailure("release", playerUuid, id, cause)
    ReleaseProcessingOutcome(S2CReleaseResult.failed(id, ReleaseFailureReason.InfrastructureFailure), None)

  private def infrastructureRecall(playerUuid: UUID, id: UUID, cause: Throwable): RecallProcessingOutcome =
    log.infrastructureFailure("recall", playerUuid, id, cause)
    RecallProcessingOutcome(S2CRecallResult.failed(id, RecallFailureReason.InfrastructureFailure), None)

  private def releaseLifecycleFailure(playerUuid: UUID, id: UUID, cause: Throwable): ReleaseProcessingOutcome = infrastructureRelease(playerUuid, id, cause)

  private def recallLifecycleFailure(playerUuid: UUID, id: UUID, cause: Throwable): RecallProcessingOutcome = infrastructureRecall(playerUuid, id, cause)

  private def succeedRelease(playerUuid: UUID, snapshot: PodLifecycleSnapshot): ReleaseProcessingOutcome =
    log.success("release", playerUuid, snapshot.state.uuid, snapshot.state.stateVersion)
    ReleaseProcessingOutcome(S2CReleaseResult.succeeded(snapshot.state.uuid, snapshot.state.stateVersion), Some(sync(snapshot.state)))

  private def succeedRecall(playerUuid: UUID, snapshot: PodLifecycleSnapshot): RecallProcessingOutcome =
    log.success("recall", playerUuid, snapshot.state.uuid, snapshot.state.stateVersion)
    RecallProcessingOutcome(S2CRecallResult.succeeded(snapshot.state.uuid, snapshot.state.stateVersion), Some(sync(snapshot.state)))

  private def sync(state: FloatingPodState): S2CStateSync =
    new S2CStateSync(state.uuid, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.stateVersion)
