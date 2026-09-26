package com.wi.floatingpod.server

import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{FloatingPodRepository, RecoveryPodStore}
import net.minecraft.nbt.NBTTagCompound
import net.minecraft.util.math.ChunkPos
import net.minecraft.world.World
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.apache.logging.log4j.LogManager
import java.util.{ArrayList as JavaArrayList, List as JavaList, UUID}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.math.floor
import scala.util.{Failure, Success, Try}

/** Forge Ticket 的可测试包装器，ownershipKey 必须来自同一原始 Ticket。 */
trait ChunkTicket:
  private[server] def ownershipKey: AnyRef = this

/** 集中 Forge 静态调用的适配边界。 */
trait ChunkTicketGateway:
  /** 登记完整的 Forge 有序恢复回调。 */
  def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit
  /** 为指定运行时世界申请普通票据；配额不足时返回空。 */
  def request(world: PodWorld): Option[ChunkTicket]
  /** 释放票据及其强加载区块。 */
  def release(ticket: ChunkTicket): Unit
  /** 返回 Forge 为票据允许的最大区块深度。 */
  def maxChunkDepth(ticket: ChunkTicket): Int
  /** 设置已验证的票据区块深度。 */
  def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit
  /** 将稳定辅助机 UUID 写入票据数据。 */
  def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit
  /** 读取票据数据中的稳定辅助机 UUID。 */
  def readPodUuid(ticket: ChunkTicket): Option[UUID]
  /** 强加载一个区块。 */
  def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit
  /** 取消强加载一个区块。 */
  def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit
  /** 将 Forge 回调的原始票据包装为项目票据。 */
  def wrapRecovered(ticket: Ticket): ChunkTicket
  /** 还原项目票据持有的 Forge 原始票据。 */
  def unwrap(ticket: ChunkTicket): Option[Ticket]

/** ticket 与 world 只在运行时保存；raw ownershipKey 是回放的权威身份。 */
private[server] final case class ManagedPodTicket(podUuid: UUID, ticket: ChunkTicket, ownershipKey: AnyRef, world: PodWorld):
  require(podUuid != null, "podUuid must not be null")
  require(ticket != null, "ticket must not be null")
  require(ownershipKey != null, "ownershipKey must not be null")
  require(world != null, "world must not be null")

/** 启动恢复或孤儿清理的不可静默失败。 */
sealed trait TicketRecoveryIssue
object TicketRecoveryIssue:
  final case class CandidateRejected(code: String, podUuid: Option[UUID], reason: String) extends TicketRecoveryIssue
  final case class CandidateProcessingFailed(podUuid: Option[UUID], cause: Throwable) extends TicketRecoveryIssue
  final case class OwnershipConflict(podUuid: UUID) extends TicketRecoveryIssue
  final case class CandidateReleaseFailed(podUuid: Option[UUID], cause: Throwable) extends TicketRecoveryIssue
  final case class PendingReleaseRetryFailed(cause: Throwable) extends TicketRecoveryIssue
  final case class OrphanReleaseFailed(podUuid: UUID, cause: Throwable) extends TicketRecoveryIssue
final case class TicketRecoveryReport(issues: List[TicketRecoveryIssue]):
  require(issues != null, "issues must not be null")
  require(issues.forall(_ != null), "issues must not contain null")
  def isClean: Boolean = issues.isEmpty

/** 当前 boot 的启动票据清理观察；仅描述 callback 内存行为，不代表 forcedchunks.dat 已持久化。 */
private[server] sealed trait StartupTicketCleanupObservation
private[server] object StartupTicketCleanupObservation:
  final case class DroppedByOrderedCallback(dimensionId: Int, podUuid: Option[UUID], code: String) extends StartupTicketCleanupObservation:
    require(podUuid != null && code != null && code.trim.nonEmpty, "ordered callback observation values must not be null or blank")
  final case class ReleasedAfterAcceptedRejection(dimensionId: Int, podUuid: Option[UUID]) extends StartupTicketCleanupObservation:
    require(podUuid != null, "podUuid must not be null")
  final case class ReleaseFailed(dimensionId: Int, podUuid: Option[UUID], cause: Throwable) extends StartupTicketCleanupObservation:
    require(podUuid != null && cause != null, "release failure observation values must not be null")
  final case class WorldTicketPassCompleted(dimensionId: Int) extends StartupTicketCleanupObservation

private final case class PendingTicketRelease(ticket: ChunkTicket, ownershipKey: AnyRef)
/** 管理活动辅助机的区块票据，并以 RecoveryPodStore 原始行恢复。 */
final class ChunkLoadingManager(repository: FloatingPodRepository, gateway: ChunkTicketGateway)
    extends ForgeChunkManager.OrderedLoadingCallback:
  private val logger = LogManager.getLogger(getClass)
  private val recoveryStore = repository match
    case store: RecoveryPodStore => Some(store)
    case _ => None
  private val ticketsByPod = mutable.HashMap.empty[UUID, ManagedPodTicket]
  private val ticketsAwaitingRelease = mutable.LinkedHashMap.empty[AnyRef, PendingTicketRelease]
  private var callbackRegistered = false
  private var recoverySessionOpen = false
  private val recoverySessionIssues = mutable.ListBuffer.empty[TicketRecoveryIssue]
  private var lastReport = TicketRecoveryReport(Nil)
  private val startupTicketCleanupObservations = mutable.ListBuffer.empty[StartupTicketCleanupObservation]

  /** 显式幂等登记；构造器绝不登记 Forge callback。 */
  def registerRecoveryCallbackOnce(): Unit = synchronized {
    if !callbackRegistered then
      val priorCallbackRegistered = callbackRegistered
      val priorRecoverySessionOpen = recoverySessionOpen
      val priorRecoverySessionIssues = recoverySessionIssues.toList
      val priorLastReport = lastReport
      val priorCleanupObservations = startupTicketCleanupObservations.toList
      recoverySessionOpen = true
      recoverySessionIssues.clear()
      lastReport = TicketRecoveryReport(Nil)
      startupTicketCleanupObservations.clear()
      try
        gateway.registerCallback(this)
        callbackRegistered = true
      catch
        case problem: Throwable =>
          callbackRegistered = priorCallbackRegistered
          recoverySessionOpen = priorRecoverySessionOpen
          recoverySessionIssues.clear()
          recoverySessionIssues ++= priorRecoverySessionIssues
          lastReport = priorLastReport
          startupTicketCleanupObservations.clear()
          startupTicketCleanupObservations ++= priorCleanupObservations
          throw problem
  }

  /** 申请并初始化票据；中途失败会保留可重试的原始票据释放。 */
  def requestTicket(podUuid: UUID, world: PodWorld, chunkX: Int, chunkZ: Int): Try[ChunkTicket] = Try {
    require(podUuid != null, "podUuid must not be null")
    require(world != null, "world must not be null")
    synchronized {
      retryPendingTicketReleases()
      if ticketsByPod.contains(podUuid) then throw new IllegalStateException(s"Chunk ticket already exists for pod $podUuid")
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.BeforeTicketRequest)
      val ticket = gateway.request(world).getOrElse(throw new IllegalStateException(s"Forge ticket quota exhausted for pod $podUuid"))
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterTicketRequest)
      val managed = ManagedPodTicket(podUuid, ticket, keyOf(ticket), world)
      ticketsByPod.put(podUuid, managed)
      try
        if gateway.maxChunkDepth(ticket) < ChunkLoadingManager.DefaultChunkCount then
          throw new IllegalStateException(s"Forge ticket depth is below required ${ChunkLoadingManager.DefaultChunkCount} for pod $podUuid")
        gateway.setChunkDepth(ticket, ChunkLoadingManager.DefaultChunkCount)
        gateway.writePodUuid(ticket, podUuid)
        forceSquare(ticket, chunkX, chunkZ)
        ticket
      catch
        case primary: Throwable =>
          releaseAfterFailure(podUuid, "ticket initialization", primary)
          throw primary
    }
  }

  /** 使用真实 Forge World 申请并返回原始 Ticket。 */
  def requestTicket(podUuid: UUID, world: World, chunkX: Int, chunkZ: Int): Try[Ticket] =
    requestTicket(podUuid, PodWorldImpl(world), chunkX, chunkZ).flatMap(ticket =>
      gateway.unwrap(ticket).toRight(new IllegalStateException("Forge ticket gateway did not return a raw Ticket")).fold(Failure(_), Success(_))
    )

  /** 严格 API：缺失记录仍失败。 */
  def releaseTicket(podUuid: UUID): Try[Unit] = Try {
    require(podUuid != null, "podUuid must not be null")
    synchronized { releaseManaged(ticketsByPod.getOrElse(podUuid, throw new IllegalStateException(s"No chunk ticket exists for pod $podUuid"))) }
  }

  /** 幂等 API：缺失记录视为 AlreadyAbsent。 */
  def releaseIfPresent(podUuid: UUID): Try[Unit] = Try {
    require(podUuid != null, "podUuid must not be null")
    synchronized { ticketsByPod.get(podUuid).foreach(releaseManaged) }
  }

  /** 对已管理的项目票据强加载区块。 */
  def forceChunk(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Try[Unit] = Try {
    synchronized { requireManaged(ticket); gateway.force(ticket, chunkX, chunkZ) }
  }
  /** 对已管理的真实 Forge Ticket 强加载区块。 */
  def forceChunk(ticket: Ticket, chunkX: Int, chunkZ: Int): Try[Unit] = managedForgeTicket(ticket).flatMap(forceChunk(_, chunkX, chunkZ))
  /** 对已管理的项目票据取消强加载区块。 */
  def unforceChunk(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Try[Unit] = Try {
    synchronized { requireManaged(ticket); gateway.unforce(ticket, chunkX, chunkZ) }
  }
  /** 对已管理的真实 Forge Ticket 取消强加载区块。 */
  def unforceChunk(ticket: Ticket, chunkX: Int, chunkZ: Int): Try[Unit] = managedForgeTicket(ticket).flatMap(unforceChunk(_, chunkX, chunkZ))

  /** 返回 UUID 到项目票据的兼容快照。 */
  def getAllManagedTickets(): Map[UUID, ChunkTicket] = synchronized { ticketsByPod.view.mapValues(_.ticket).toMap }
  /** 返回 UUID 到真实 Forge Ticket 的快照；测试网关不会伪造原始票据。 */
  def getAllTickets(): Map[UUID, Ticket] = synchronized { ticketsByPod.flatMap((uuid, managed) => gateway.unwrap(managed.ticket).map(uuid -> _)).toMap }
  /** 返回完整运行时记录，用于后续 DB、ticket、entity 对账。 */
  def managedTicketRecords(): Map[UUID, ManagedPodTicket] = synchronized { ticketsByPod.toMap }
  /** 返回当前显式恢复 session 中按 callback 观察顺序累计的报告。 */
  def latestRecoveryReport(): TicketRecoveryReport = synchronized { lastReport }
  /** 当前 callback 注册 session 的不可变启动清理观察快照。 */
  private[server] def startupTicketCleanupSnapshot(): List[StartupTicketCleanupObservation] = synchronized { startupTicketCleanupObservations.toList }
  /** 由后续低优先级 WorldEvent.Load listener 标记一个世界的 ticket pass 已完成。 */
  private[server] def markWorldTicketPassCompleted(dimensionId: Int): Unit = synchronized { startupTicketCleanupObservations += StartupTicketCleanupObservation.WorldTicketPassCompleted(dimensionId) }
  private[server] def pendingTicketReleaseCount(): Int = synchronized { ticketsAwaitingRelease.size }
  /** 重试失败释放；失败由调用方保留原始异常处理。 */
  def reconcilePendingTicketReleases(): Try[Unit] = Try { synchronized { retryPendingTicketReleases() } }

  /** Forge 第一回调：按原始恢复行有序筛选，绝不在此阶段强加载。 */
  override def ticketsLoaded(tickets: JavaList[Ticket], world: World, maxTicketCount: Int): JavaList[Ticket] =
    require(tickets != null, "tickets must not be null")
    require(world != null, "world must not be null")
    val selected = selectRecoverableTickets(world.provider.getDimension, tickets.asScala.toList.map(gateway.wrapRecovered), maxTicketCount)
    val result = new JavaArrayList[Ticket]()
    selected.foreach(ticket => gateway.unwrap(ticket).foreach(result.add))
    result

  /** Forge 第二回调：恢复已接管票据并绑定本次 callback World。 */
  override def ticketsLoaded(tickets: JavaList[Ticket], world: World): Unit =
    require(tickets != null, "tickets must not be null")
    require(world != null, "world must not be null")
    recoverAcceptedTickets(tickets.asScala.toList.map(gateway.wrapRecovered), PodWorldImpl(world))

  /** 不转换为 FloatingPodState，因此可接受 V001 legacy 的 active/null-dimension 原始行。 */
  private[floatingpod] def selectRecoverableTickets(dimensionId: Int, candidates: List[ChunkTicket], maxTicketCount: Int): List[ChunkTicket] = synchronized {
    require(candidates != null, "candidates must not be null")
    require(maxTicketCount >= 0, "maxTicketCount must not be negative")
    val issues = mutable.ListBuffer.empty[TicketRecoveryIssue]
    val selected = mutable.ListBuffer.empty[ChunkTicket]
    val seen = mutable.HashSet.empty[UUID]
    candidates.foreach { ticket =>
      readCandidateUuid(ticket) match
        case Left(problem) =>
          issues += TicketRecoveryIssue.CandidateProcessingFailed(None, problem)
          startupTicketCleanupObservations += StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, None, "candidate_processing_failed")
        case Right(None) =>
          issues += TicketRecoveryIssue.CandidateRejected("missing_uuid", None, "Recovered ticket has no valid FloatingPod UUID")
          startupTicketCleanupObservations += StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, None, "missing_uuid")
        case Right(Some(uuid)) if seen.contains(uuid) =>
          issues += TicketRecoveryIssue.CandidateRejected("duplicate_uuid", Some(uuid), "Duplicate recovered ticket in ordered callback")
          startupTicketCleanupObservations += StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, Some(uuid), "duplicate_uuid")
        case Right(Some(uuid)) =>
          desiredActiveRow(uuid) match
            case Right(_) if selected.size < maxTicketCount => seen += uuid; selected += ticket
            case Right(_) =>
              seen += uuid
              issues += TicketRecoveryIssue.CandidateRejected("ticket_limit", Some(uuid), s"Configured ticket limit is $maxTicketCount")
              startupTicketCleanupObservations += StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, Some(uuid), "ticket_limit")
            case Left(problem) =>
              issues += TicketRecoveryIssue.CandidateRejected("recovery_row_rejected", Some(uuid), problem.getMessage)
              startupTicketCleanupObservations += StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, Some(uuid), "recovery_row_rejected")
    }
    appendIssues(issues.toList)
    selected.toList
  }

  /** callbackWorld 直接写入记录；S3 可据此读取确切的服务端 dimension。 */
  private[floatingpod] def recoverAcceptedTickets(accepted: List[ChunkTicket], callbackWorld: PodWorld): Unit = synchronized {
    require(accepted != null, "accepted must not be null")
    require(callbackWorld != null, "callbackWorld must not be null")
    val issues = mutable.ListBuffer.empty[TicketRecoveryIssue]
    try retryPendingTicketReleases()
    catch case problem: Throwable => issues += TicketRecoveryIssue.PendingReleaseRetryFailed(problem)
    accepted.foreach { ticket =>
      readCandidateUuid(ticket) match
        case Left(problem) =>
          logger.error("Rejecting recovered chunk ticket because UUID reading failed", problem)
          issues += TicketRecoveryIssue.CandidateProcessingFailed(None, problem)
          releaseCandidate(ticket, None, callbackWorld.dimensionId, issues)
        case Right(cachedUuid) =>
          try
            val uuid = cachedUuid.getOrElse(throw new IllegalStateException("Recovered ticket has no valid FloatingPod UUID"))
            ticketsByPod.get(uuid) match
              case Some(canonical) if canonical.ownershipKey eq keyOf(ticket) =>
              case Some(_) =>
                issues += TicketRecoveryIssue.OwnershipConflict(uuid)
                releaseCandidate(ticket, Some(uuid), callbackWorld.dimensionId, issues)
              case None =>
                val row = desiredActiveRow(uuid).fold(throw _, identity)
                if gateway.maxChunkDepth(ticket) < ChunkLoadingManager.DefaultChunkCount then
                  throw new IllegalStateException(s"Forge ticket depth is below required ${ChunkLoadingManager.DefaultChunkCount} for recovered pod $uuid")
                gateway.setChunkDepth(ticket, ChunkLoadingManager.DefaultChunkCount)
                forceSquare(ticket, chunkCoordinate(row.posX), chunkCoordinate(row.posZ))
                ticketsByPod.put(uuid, ManagedPodTicket(uuid, ticket, keyOf(ticket), callbackWorld))
          catch
            case problem: Throwable =>
              logger.error(s"Rejecting recovered chunk ticket: ${problem.getMessage}", problem)
              issues += TicketRecoveryIssue.CandidateRejected("accepted_candidate_rejected", cachedUuid, problem.getMessage)
              releaseCandidate(ticket, cachedUuid, callbackWorld.dimensionId, issues)
    }
    appendIssues(issues.toList)
  }

  /** 对 supplied DB desired-active 集合之外的记录做可重试释放。 */
  private[floatingpod] def reconcileOrphanedTickets(desiredActiveUuids: Set[UUID]): TicketRecoveryReport = synchronized {
    require(desiredActiveUuids != null, "desiredActiveUuids must not be null")
    val issues = mutable.ListBuffer.empty[TicketRecoveryIssue]
    ticketsByPod.keys.filterNot(desiredActiveUuids.contains).toList.foreach { uuid =>
      releaseIfPresent(uuid) match
        case Failure(problem) => issues += TicketRecoveryIssue.OrphanReleaseFailed(uuid, problem)
        case Success(_) =>
    }
    appendIssues(issues.toList)
  }

  /** 从 RecoveryPodStore 的原始 desired-active 集合对账；读取失败也追加到当前 session 报告。 */
  private[floatingpod] def reconcileOrphanedTickets(): TicketRecoveryReport = synchronized {
    recoveryStore match
      case Some(store) => store.findDesiredActiveRows() match
        case Success(rows) => reconcileOrphanedTickets(rows.iterator.map(_.uuid).toSet)
        case Failure(problem) => appendIssues(List(TicketRecoveryIssue.CandidateProcessingFailed(None, problem)))
      case None => appendIssues(List(TicketRecoveryIssue.CandidateRejected("missing_recovery_store", None, "Repository does not implement RecoveryPodStore")))
  }

  private def desiredActiveRow(uuid: UUID) = recoveryStore match
    case Some(store) => store.findStoredByUuid(uuid).toEither.flatMap {
      case Some(row) if row.isActive => Right(row)
      case Some(_) => Left(new IllegalStateException(s"Recovered ticket references inactive pod $uuid"))
      case None => Left(new IllegalStateException(s"Recovered ticket references missing pod $uuid"))
    }
    case None => Left(new IllegalStateException("Repository does not implement RecoveryPodStore"))

  private def releaseManaged(managed: ManagedPodTicket): Unit =
    try gateway.release(managed.ticket)
    catch
      case problem: Throwable => queueRelease(managed.ticket, managed.ownershipKey); throw problem
    removeOwnership(managed.ownershipKey)

  private def releaseCandidate(ticket: ChunkTicket, uuid: Option[UUID], dimensionId: Int, issues: mutable.ListBuffer[TicketRecoveryIssue]): Unit =
    try
      gateway.release(ticket)
      removeOwnership(keyOf(ticket))
      startupTicketCleanupObservations += StartupTicketCleanupObservation.ReleasedAfterAcceptedRejection(dimensionId, uuid)
    catch
      case problem: Throwable =>
        queueRelease(ticket, keyOf(ticket))
        issues += TicketRecoveryIssue.CandidateReleaseFailed(uuid, problem)
        startupTicketCleanupObservations += StartupTicketCleanupObservation.ReleaseFailed(dimensionId, uuid, problem)

  private def retryPendingTicketReleases(): Unit =
    var firstFailure: Throwable = null
    ticketsAwaitingRelease.toList.foreach { case (key, pending) =>
      try
        gateway.release(pending.ticket)
        removeOwnership(key)
      catch
        case problem: Throwable =>
          logger.error("Failed to retry pending chunk ticket release", problem)
          if firstFailure == null then firstFailure = problem
    }
    if firstFailure != null then throw firstFailure

  private def releaseAfterFailure(uuid: UUID, operation: String, primary: Throwable): Unit =
    releaseIfPresent(uuid).failed.foreach(problem => logger.error(s"Failed to release ticket for pod $uuid after $operation", problem))
    logger.error(s"Chunk ticket $operation failed for pod $uuid: ${primary.getMessage}", primary)
  private def queueRelease(ticket: ChunkTicket, key: AnyRef): Unit = ticketsAwaitingRelease.put(key, PendingTicketRelease(ticket, key))
  private def removeOwnership(key: AnyRef): Unit =
    ticketsAwaitingRelease.remove(key)
    ticketsByPod.keys.filter(uuid => ticketsByPod(uuid).ownershipKey eq key).toList.foreach(ticketsByPod.remove)
  private def requireManaged(ticket: ChunkTicket): Unit =
    require(ticket != null, "ticket must not be null")
    if !ticketsByPod.values.exists(_.ownershipKey eq keyOf(ticket)) then throw new IllegalStateException("Chunk ticket is not managed by this ChunkLoadingManager")
  private def managedForgeTicket(ticket: Ticket): Try[ChunkTicket] = Try {
    require(ticket != null, "ticket must not be null")
    synchronized { ticketsByPod.values.find(record => gateway.unwrap(record.ticket).exists(_ eq ticket)).map(_.ticket).getOrElse(throw new IllegalStateException("Forge Ticket is not managed by this ChunkLoadingManager")) }
  }
  private def forceSquare(ticket: ChunkTicket, centerX: Int, centerZ: Int): Unit =
    for
      chunkX <- centerX - ChunkLoadingManager.DefaultRadiusChunks to centerX + ChunkLoadingManager.DefaultRadiusChunks
      chunkZ <- centerZ - ChunkLoadingManager.DefaultRadiusChunks to centerZ + ChunkLoadingManager.DefaultRadiusChunks
    do gateway.force(ticket, chunkX, chunkZ)
  private def keyOf(ticket: ChunkTicket): AnyRef =
    require(ticket != null, "ticket must not be null")
    require(ticket.ownershipKey != null, "ticket ownership key must not be null")
    ticket.ownershipKey
  private def readCandidateUuid(ticket: ChunkTicket): Either[Throwable, Option[UUID]] =
    Try(gateway.readPodUuid(ticket)).toEither
  private def appendIssues(issues: List[TicketRecoveryIssue]): TicketRecoveryReport =
    if !recoverySessionOpen then
      recoverySessionOpen = true
      recoverySessionIssues.clear()
    recoverySessionIssues ++= issues
    lastReport = TicketRecoveryReport(recoverySessionIssues.toList)
    lastReport
  private def chunkCoordinate(position: Double): Int = floor(position / ChunkLoadingManager.ChunkSize).toInt

object ChunkLoadingManager:
  val DefaultRadiusChunks = 2
  val DefaultChunkCount = 25
  val ChunkSize = 16.0D
  val PodUuidKey = "floatingpod:pod_uuid"
  def forForge(repository: FloatingPodRepository, modInstance: Object): ChunkLoadingManager =
    new ChunkLoadingManager(repository, new ChunkTicketGatewayImpl(modInstance))

final class ChunkTicketImpl private[server] (val rawTicket: Ticket) extends ChunkTicket:
  override private[server] def ownershipKey: AnyRef = rawTicket

final class ChunkTicketGatewayImpl(modInstance: Object) extends ChunkTicketGateway:
  require(modInstance != null, "modInstance must not be null")
  override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ForgeChunkManager.setForcedChunkLoadingCallback(modInstance, callback)
  override def request(world: PodWorld): Option[ChunkTicket] = world match
    case forgeWorld: PodWorldImpl => Option(ForgeChunkManager.requestTicket(modInstance, forgeWorld.rawWorld, ForgeChunkManager.Type.NORMAL)).map(new ChunkTicketImpl(_))
    case _ => throw new IllegalArgumentException("Forge ticket requests require PodWorldImpl")
  override def release(ticket: ChunkTicket): Unit = ForgeChunkManager.releaseTicket(asForgeTicket(ticket))
  override def maxChunkDepth(ticket: ChunkTicket): Int = asForgeTicket(ticket).getMaxChunkListDepth
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = asForgeTicket(ticket).setChunkListDepth(depth)
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = asForgeTicket(ticket).getModData.setUniqueId(ChunkLoadingManager.PodUuidKey, podUuid)
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] =
    val data: NBTTagCompound = asForgeTicket(ticket).getModData
    if data.hasUniqueId(ChunkLoadingManager.PodUuidKey) then Some(data.getUniqueId(ChunkLoadingManager.PodUuidKey)) else None
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ForgeChunkManager.forceChunk(asForgeTicket(ticket), new ChunkPos(chunkX, chunkZ))
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ForgeChunkManager.unforceChunk(asForgeTicket(ticket), new ChunkPos(chunkX, chunkZ))
  override def wrapRecovered(ticket: Ticket): ChunkTicket = new ChunkTicketImpl(ticket)
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = Some(asForgeTicket(ticket))
  private def asForgeTicket(ticket: ChunkTicket): Ticket = ticket match
    case forgeTicket: ChunkTicketImpl => forgeTicket.rawTicket
    case _ => throw new IllegalArgumentException("Forge gateway received a non-Forge chunk ticket")
