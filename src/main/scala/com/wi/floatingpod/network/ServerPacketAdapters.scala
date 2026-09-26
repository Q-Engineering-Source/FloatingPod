package com.wi.floatingpod.network

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ActivePodEntity, ActivePodEntityImpl, AuthoritativePosition, PodWorld, PodWorldImpl, RecallOutcome, ReleasePosition, SpawnOutcome}
import com.wi.floatingpod.storage.{AdmissionRejection, DeactivationRejection}

import net.minecraft.entity.player.EntityPlayerMP
import net.minecraft.util.EnumFacing
import net.minecraft.util.math.BlockPos
import net.minecraftforge.fml.common.network.simpleimpl.{IMessage, MessageContext}

import org.apache.logging.log4j.{LogManager, Logger}

import java.util.UUID
import scala.util.{Failure, Success, Try}

/** 从已排程的 C2S MessageContext 解析服务端权威玩家上下文的最小边界。 */
trait ServerPlayerContextFactory:
  def resolve(context: MessageContext): ServerPlayerContext

/** 供测试替换消息身份解析、但不改变 endpoint 对 context 使用方式的通用工厂。 */
final class ResolvingServerPlayerContextFactory[P](resolver: MessageContext => P, createContext: P => ServerPlayerContext) extends ServerPlayerContextFactory:
  require(resolver != null, "resolver must not be null")
  require(createContext != null, "createContext must not be null")

  override def resolve(context: MessageContext): ServerPlayerContext =
    require(context != null, "context must not be null")
    val player = resolver(context)
    require(player != null, "resolved player must not be null")
    val resolved = createContext(player)
    require(resolved != null, "resolved player context must not be null")
    resolved

/** 真实 Forge factory 只从 MessageContext 的 server handler 读取玩家身份。 */
object ForgeServerPlayerContextFactory extends ServerPlayerContextFactory:
  override def resolve(context: MessageContext): ServerPlayerContext =
    require(context != null, "context must not be null")
    val handler = context.getServerHandler
    require(handler != null, "server handler must not be null")
    val player = handler.player
    require(player != null, "server player must not be null")
    new ForgeServerPlayerContext(player)

/** 服务端玩家与世界的 Forge 适配，所有位置和方块查询均从已认证玩家当前状态读取。 */
final class ForgeServerPlayerContext(val rawPlayer: EntityPlayerMP) extends ServerPlayerContext:
  require(rawPlayer != null, "rawPlayer must not be null")

  override def playerUuid: UUID = rawPlayer.getUniqueID
  override def position: AuthoritativePosition = AuthoritativePosition(rawPlayer.posX, rawPlayer.posY, rawPlayer.posZ)
  override def maximumReach: Double = rawPlayer.interactionManager.getBlockReachDistance.toDouble
  override def spawnWorld: PodWorld = PodWorldImpl(rawPlayer.world)

  override def hasSolidSupport(target: ReleasePosition): Boolean =
    val support = new BlockPos(target.x, target.y - 1.0D, target.z)
    rawPlayer.world.getBlockState(support).isSideSolid(rawPlayer.world, support, EnumFacing.UP)

  override def hasTwoBlockHeadroom(target: ReleasePosition): Boolean =
    val base = new BlockPos(target.x, target.y, target.z)
    rawPlayer.world.isAirBlock(base) && rawPlayer.world.isAirBlock(base.up())

  override def isSameDimension(activePod: ActivePodEntity): Boolean = activePod match
    case forgePod: ActivePodEntityImpl => rawPlayer.dimension == forgePod.rawEntity.dimension
    case _ => throw new IllegalArgumentException("Forge player context received a non-Forge active pod")

/** 将 processor 的单个有界结果和可选同步消息送回已认证的服务器玩家。 */
trait ServerPacketSender:
  def send(player: ServerPlayerContext, message: IMessage): Unit

/** 生产发送器仅接受真实 Forge 玩家适配，避免从 C2S payload 取得任何目标身份。 */
final class ForgeServerPacketSender(sendToPlayer: (IMessage, EntityPlayerMP) => Unit) extends ServerPacketSender:
  require(sendToPlayer != null, "sendToPlayer must not be null")

  override def send(player: ServerPlayerContext, message: IMessage): Unit =
    require(player != null, "player must not be null")
    require(message != null, "message must not be null")
    player match
      case forgePlayer: ForgeServerPlayerContext => sendToPlayer(message, forgePlayer.rawPlayer)
      case _ => throw new IllegalArgumentException("Forge sender received a non-Forge server player context")

object ForgeServerPacketSender:
  def apply(): ForgeServerPacketSender = new ForgeServerPacketSender(PacketHandler.sendTo)

/** 服务器端的 handler 终点；它只由已经排程到服务端主线程的 C2S task 调用。 */
final class ForgeServerPacketEndpoint(
    playerFactory: ServerPlayerContextFactory,
    processor: PodRequestProcessor,
    sender: ServerPacketSender
) extends ServerPacketEndpoint:
  require(playerFactory != null, "playerFactory must not be null")
  require(processor != null, "processor must not be null")
  require(sender != null, "sender must not be null")

  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit =
    val player = playerFactory.resolve(context)
    val outcome = processor.release(message, player)
    sender.send(player, outcome.result)
    outcome.sync.foreach(sender.send(player, _))

  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit =
    val player = playerFactory.resolve(context)
    val outcome = processor.recall(message, player)
    sender.send(player, outcome.result)
    outcome.sync.foreach(sender.send(player, _))

/** 以 active-manager 的认证主入口实现 processor 所需的单一 lifecycle boundary。 */
final class ActivePodLifecyclePort(manager: ActiveFloatingPodManager, configuredGlobalActiveLimit: Int) extends PodLifecyclePort:
  require(manager != null, "manager must not be null")
  require(configuredGlobalActiveLimit >= 1 && configuredGlobalActiveLimit <= 256, "configuredGlobalActiveLimit must be 1..256")

  override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] = Try {
    require(state != null, "state must not be null")
    require(player != null, "player must not be null")
    val activePod = manager.getActivePod(state.uuid).getOrElse {
      throw new IllegalStateException(s"No active FloatingPod entity exists for ${state.uuid}")
    }
    player.isSameDimension(activePod)
  }

  override def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] =
    require(state != null, "state must not be null")
    require(target != null, "target must not be null")
    require(player != null, "player must not be null")
    manager.spawn(state.uuid, player.playerUuid, state.stateVersion, player.spawnWorld, target.x, target.y, target.z, configuredGlobalActiveLimit).map {
      case SpawnOutcome.Accepted(spawned) => ReleaseLifecycleOutcome.Accepted(PodLifecycleSnapshot(spawned.exactState))
      case SpawnOutcome.Rejected(reason) => ReleaseLifecycleOutcome.Rejected(releaseReason(reason))
    }

  override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] =
    require(state != null, "state must not be null")
    require(player != null, "player must not be null")
    manager.recallIfExpected(state.uuid, Some(player.playerUuid), state.stateVersion).map {
      case RecallOutcome.Accepted(exactState) => RecallLifecycleOutcome.Accepted(PodLifecycleSnapshot(exactState))
      case RecallOutcome.Rejected(reason) => RecallLifecycleOutcome.Rejected(recallReason(reason))
    }

  /** 将 manager 最终持久化拒绝映射为协议的稳定有界原因。 */
  private def releaseReason(reason: AdmissionRejection): ReleaseFailureReason = reason match
    case AdmissionRejection.UnknownPod => ReleaseFailureReason.UnknownPod
    case AdmissionRejection.NotOwner => ReleaseFailureReason.NotOwner
    case _: AdmissionRejection.VersionMismatch => ReleaseFailureReason.VersionMismatch
    case AdmissionRejection.AlreadyActive => ReleaseFailureReason.AlreadyActive
    case AdmissionRejection.GlobalActiveLimitReached => ReleaseFailureReason.CapacityReached
    case AdmissionRejection.PlayerActiveLimitReached => ReleaseFailureReason.EntitlementDenied

  private def recallReason(reason: DeactivationRejection): RecallFailureReason = reason match
    case DeactivationRejection.UnknownPod => RecallFailureReason.UnknownPod
    case DeactivationRejection.NotOwner => RecallFailureReason.NotOwner
    case _: DeactivationRejection.VersionMismatch => RecallFailureReason.VersionMismatch
    case DeactivationRejection.NotActive => RecallFailureReason.NotActive

/** 将 processor 审计级别适配到 Log4j；底层 cause 只写服务端日志，绝不进入协议消息。 */
final class Log4jRequestLog(logger: Logger) extends RequestLog:
  require(logger != null, "logger must not be null")

  override def rejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason): Unit =
    logger.warn(s"Rejected FloatingPod $operation request from $playerUuid for $podUuid: $reason")

  override def success(operation: String, playerUuid: UUID, podUuid: UUID, authoritativeVersion: Long): Unit =
    logger.debug(s"Completed FloatingPod $operation request from $playerUuid for $podUuid at state version $authoritativeVersion")

  override def infrastructureFailure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable): Unit =
    logger.error(s"FloatingPod $operation infrastructure failure from $playerUuid for $podUuid", cause)

object Log4jRequestLog:
  def apply(): Log4jRequestLog = new Log4jRequestLog(LogManager.getLogger(classOf[Log4jRequestLog]))
