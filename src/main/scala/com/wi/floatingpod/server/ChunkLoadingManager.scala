package com.wi.floatingpod.server

import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{FloatingPodRepository, RecoveryPodStore}
import net.minecraft.nbt.NBTTagCompound
import net.minecraft.util.math.ChunkPos
import net.minecraft.world.World
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.apache.logging.log4j.LogManager
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.mysql.{MySqlNormalRecallRemovalFacts, MySqlNormalRecallTicketFacts}
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
  /** 框架实际登记/强载入/requested集合；旧网关无此能力时不能签发 IO 释放凭证。 */
  private[server] def releaseSnapshot(ticket: ChunkTicket): TicketReleaseSnapshot =
    throw new UnsupportedOperationException("Gateway has no exact ticket release observation")
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
  /** 从实际gateway读取当下requested chunks；默认明确失败，旧gateway不得假完整。 */
  def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] =
    throw new UnsupportedOperationException("Actual ticket requested chunk snapshot is unavailable")
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
/** 仅 Main 保留实际资源与完整原 footprint；不会提交给 worker。 */
final class PendingManagedTicketRelease private[server] (
    private[server] val managerOwner: AnyRef, val tag: IoOperationTag, val attemptUuid: UUID,
    val expectedStateVersion: Long, val expectedEntityId: Int, private[server] val managed: ManagedPodTicket,
    val originalFootprint: Set[com.wi.floatingpod.storage.PhysicalRecoveryChunk]
):
  private[server] var acknowledgement: Option[TicketReleaseAcknowledgement] = None
  private[server] var releaseCause: Option[Throwable] = None
  def failure: Option[Throwable] = releaseCause
/** Main保留的实际allocation/setup责任；None chunks意味着尚不可观察，不是空/完成ACK。 */
private[server] final case class PublicationTicketResponsibility(tag: IoOperationTag, podUuid: UUID,
    world: PodWorld, ticket: Option[ChunkTicket], actualChunks: Option[Set[(Int, Int)]],
    initializationReturned: Boolean, cause: Option[Throwable])
/** 管理活动辅助机的区块票据，并以 RecoveryPodStore 原始行恢复。 */
final class ChunkLoadingManager private (legacyRepository: Option[FloatingPodRepository], gateway: ChunkTicketGateway,
    bootReads: Option[MainBootProjection], dbFirstReads: Option[ProductionDbFirstBootstrapCallbacks]) extends ForgeChunkManager.OrderedLoadingCallback:
  def this(repository: FloatingPodRepository, gateway: ChunkTicketGateway) = this(Some(repository), gateway, None, None)
  private def requireBootReads(): MainBootProjection =
    val reads = bootReads.getOrElse(throw new IllegalStateException("claimed Boot reads are not installed"))
    reads.requireCurrentReads()
    reads
  private val publicationOwner = Thread.currentThread()
  private val logger = LogManager.getLogger(getClass)
  private val recoveryStore = legacyRepository.orNull match
    case store: RecoveryPodStore => Some(store)
    case _ => None
  private val ticketsByPod = mutable.HashMap.empty[UUID, ManagedPodTicket]
  private val ticketsAwaitingRelease = mutable.LinkedHashMap.empty[AnyRef, PendingTicketRelease]
  private val ioReleases = mutable.LinkedHashMap.empty[IoOperationTag, PendingManagedTicketRelease]
  private var callbackRegistered = false
  private var recoverySessionOpen = false
  private val recoverySessionIssues = mutable.ListBuffer.empty[TicketRecoveryIssue]
  private var lastReport = TicketRecoveryReport(Nil)
  private val startupTicketCleanupObservations = mutable.ListBuffer.empty[StartupTicketCleanupObservation]

  private final class PublicationTicketEntry(val tag: IoOperationTag, val podUuid: UUID, val world: PodWorld):
    var ticket = Option.empty[ChunkTicket]
    var returned = false
    var cause = Option.empty[Throwable]
  private val publicationAllocations = mutable.LinkedHashMap.empty[IoOperationTag, PublicationTicketEntry]
  private val completedPublicationAllocations = mutable.HashMap.empty[IoOperationTag, UUID]

  /** 原Main交接前核实际所有票据责任；标量历史不冒充仍持有的Forge ticket。 */
  private[server] def requireDbFirstFinalTicketsReleased(original:ProductionDbFirstBootstrapCallbacks):Unit = synchronized {
    requireReleaseMain()
    require(dbFirstReads.exists(_ eq original) && ticketsByPod.isEmpty && ticketsAwaitingRelease.isEmpty &&
      ioReleases.isEmpty && residentReleases.isEmpty && publicationAllocations.isEmpty,
      "original schema6 tickets/releases/publication allocations remain owned")
  }

  /** 实际释放后的原allocation收缩；不授提交/释放权限，不清失败或foreign来源。 */
  private[server] def compactReleasedPublicationTicket(tag: IoOperationTag, ticket: ChunkTicket): Unit = synchronized {
    requireReleaseMain()
    val entry = publicationAllocations.getOrElse(tag, throw new IllegalArgumentException("original publication allocation required"))
    require(ticket != null && entry.ticket.exists(_ eq ticket) && entry.returned && entry.cause.isEmpty,
      "only exact successfully initialized publication source can compact")
    val key = keyOf(ticket)
    require(!ticketsByPod.values.exists(_.ownershipKey eq key) && !ticketsAwaitingRelease.contains(key) && !hasPendingIoRelease(key),
      "publication source still owns unresolved ticket responsibility")
    ExactTicketReleaseObserver.requireAbsent(key, gateway.releaseSnapshot(ticket))
    completedPublicationAllocations.put(tag, entry.podUuid)
    publicationAllocations.remove(tag)
  }

  private[server] def isPublicationTicketCompacted(tag: IoOperationTag): Boolean =
    requireReleaseMain()
    completedPublicationAllocations.contains(tag)

  private final class ResidentRelease(val permit: ActiveFloatingPodManager#MainPublicationScope#ResidentTicketReleasePermit,
      val managed: ManagedPodTicket, val originalFootprint: Set[com.wi.floatingpod.storage.PhysicalRecoveryChunk],
      val attempt: ResidentTicketReleaseAttempt):
    var acknowledgement = Option.empty[ResidentReleaseAcknowledgement]
  private val residentReleases = mutable.LinkedHashMap.empty[AnyRef, ResidentRelease]
  private val completedResidentReleases = mutable.HashSet.empty[IoOperationTag]
  final class ResidentReleaseAcknowledgement private[ChunkLoadingManager] (private var original: Option[ResidentRelease]):
    private var compactedProof = Option.empty[(IoOperationTag, AnyRef)]
    private[server] def belongsTo(permit: ActiveFloatingPodManager#MainPublicationScope#ResidentTicketReleasePermit): Boolean =
      requireReleaseMain()
      original.exists(value => (value.permit eq permit) && residentReleases.get(value.managed.ownershipKey).exists(_ eq value) &&
        value.acknowledgement.exists(_ eq this))
    private[ChunkLoadingManager] def isCompactedFor(
        permit: ActiveFloatingPodManager#MainPublicationScope#ResidentTicketReleasePermit): Boolean =
      requireReleaseMain()
      original.isEmpty && compactedProof.exists { case (tag, identity) =>
        tag == permit.tag && (identity eq permit.releaseIdentity)
      }
    private[ChunkLoadingManager] def compact(): Unit =
      val value = original.getOrElse(throw new IllegalStateException("resident release ACK already compacted"))
      require(value.acknowledgement.exists(_ eq this) && residentReleases.get(value.managed.ownershipKey).exists(_ eq value),
        "only original successful resident release may compact")
      val sources = publicationAllocations.valuesIterator.filter(_.ticket.exists(t => keyOf(t) eq value.managed.ownershipKey)).toVector
      sources.foreach(entry => require(entry.podUuid == value.managed.podUuid && entry.ticket.exists(_ eq value.managed.ticket) &&
        entry.returned && entry.cause.isEmpty, "resident release has an unsettled original allocation source"))
      sources.foreach(entry => compactReleasedPublicationTicket(entry.tag, value.managed.ticket))
      require(!completedResidentReleases.contains(value.permit.tag) &&
        residentReleases.remove(value.managed.ownershipKey).exists(_ eq value),
        "original resident release ledger must compact exactly once")
      require(completedResidentReleases.add(value.permit.tag), "original resident release tag must complete once")
      compactedProof = Some((value.permit.tag, value.permit.releaseIdentity))
      original = None

  /** compact后以同一原ACK/tag/permit身份和本manager完成账本证明成功，不复用live owner校验。 */
  private[server] def isCompactedResidentRelease(
      ack: ChunkLoadingManager#ResidentReleaseAcknowledgement,
      permit: ActiveFloatingPodManager#MainPublicationScope#ResidentTicketReleasePermit): Boolean = synchronized {
    requireReleaseMain()
    ack != null && permit != null && permit.belongsToManager(this) &&
      completedResidentReleases.contains(permit.tag) && ack.isCompactedFor(permit)
  }

  private[server] def compactResidentRelease(ack: ChunkLoadingManager#ResidentReleaseAcknowledgement): Unit = synchronized {
    requireReleaseMain()
    val owned = residentReleases.valuesIterator.flatMap(_.acknowledgement).find(_ eq ack).getOrElse(
      throw new IllegalArgumentException("resident release ACK belongs to another owner"))
    owned.compact()
  }

  /** WorldIdle没有entity removal；只接受原manager已消费full20并drop/release后的私有许可。 */
  private[server] def releaseResidentForNormalStop(
      permit: ActiveFloatingPodManager#MainPublicationScope#ResidentTicketReleasePermit): Try[ResidentReleaseAcknowledgement] = Try { synchronized {
    requireReleaseMain()
    require(permit != null, "original WorldIdle release permit required")
    permit.requireCurrent(this)
    require(!completedResidentReleases.contains(permit.tag), "resident release operation already completed and compacted")
    val key = keyOf(permit.ticket)
    val retained = residentReleases.get(key) match
      case Some(existing) =>
        require(existing.permit eq permit, "resident ticket already belongs to another stop checkpoint")
        existing
      case None =>
        require(!hasPendingIoRelease(key) && !ticketsAwaitingRelease.contains(key), "ticket has another unresolved release owner")
        val managed = ticketsByPod.getOrElse(permit.pod, throw new IllegalStateException("resident original ticket missing"))
        publicationAllocations.valuesIterator.filter(_.ticket.exists(t => keyOf(t) eq key)).foreach(entry =>
          require(entry.podUuid == managed.podUuid && entry.ticket.exists(_ eq managed.ticket) && entry.returned && entry.cause.isEmpty,
            "resident ticket has an unresolved publication allocation source"))
        val footprint = publicationTicketChunks(permit.pod, permit.world, permit.ticket)
        require(footprint.size == ChunkLoadingManager.DefaultChunkCount, "resident exact25 footprint required")
        val attempt = new ResidentTicketReleaseAttempt(key, new TicketReleaseView:
          override def assertMain(): Unit = requireReleaseMain()
          override def release(): Unit = gateway.release(managed.ticket)
          override def snapshot(): TicketReleaseSnapshot = gateway.releaseSnapshot(managed.ticket)
        )
        val value = new ResidentRelease(permit, managed, footprint, attempt)
        residentReleases.put(key, value)
        value
    retained.acknowledgement match
      case Some(ack) => ack
      case None =>
        require(ticketsByPod.get(permit.pod).exists(_ eq retained.managed), "resident ticket ownership changed")
        retained.attempt.releaseOnce()
        removeOwnership(key)
        val ack = new ResidentReleaseAcknowledgement(Some(retained))
        retained.acknowledgement = Some(ack)
        ack
  } }

  /** 新Main路径不retry旧release、不自动release失败票据。allocation前登记exact操作责任。 */
  private[server] def requestPublicationTicket(tag: IoOperationTag, podUuid: UUID, world: PodWorld,
      chunkX: Int, chunkZ: Int, retainBeforeInitialization: ChunkTicket => Unit): Try[ChunkTicket] = Try {
    require(Thread.currentThread() eq publicationOwner, "publication ticket allocation requires exact Main")
    require(tag != null && podUuid != null && world != null && retainBeforeInitialization != null,
      "exact immutable operation/pod and Main retained resource callback required")
    require(!publicationAllocations.contains(tag) && !completedPublicationAllocations.contains(tag) &&
      !publicationAllocations.values.exists(_.podUuid == podUuid) && !completedPublicationAllocations.values.exists(_ == podUuid) &&
      !ticketsByPod.contains(podUuid) && publicationAllocations.size + completedPublicationAllocations.size < 256 && ticketsByPod.size < 256,
      "duplicate/capacity publication ticket must reject before allocation")
    val entry = new PublicationTicketEntry(tag, podUuid, world)
    publicationAllocations.put(tag, entry)
    try
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.BeforeTicketRequest)
      val ticket = gateway.request(world).getOrElse(throw new IllegalStateException("Actual Forge publication ticket allocation unavailable"))
      entry.ticket = Some(ticket) // 原raw resource先保留，后续key/callback/force抛错也不丢。
      retainBeforeInitialization(ticket)
      val key = keyOf(ticket)
      require(!ticketsByPod.values.exists(_.ownershipKey eq key), "Forge returned a raw ticket already owned by another pod")
      ticketsByPod.put(podUuid, ManagedPodTicket(podUuid, ticket, key, world))
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterTicketRequest)
      require(gateway.maxChunkDepth(ticket) >= ChunkLoadingManager.DefaultChunkCount, "Actual ticket depth below required25")
      gateway.setChunkDepth(ticket, ChunkLoadingManager.DefaultChunkCount)
      gateway.writePodUuid(ticket, podUuid)
      forceSquare(ticket, chunkX, chunkZ)
      entry.returned = true
      ticket
    catch
      case cause: Throwable => entry.cause = Some(cause); throw cause
  }

  private[server] def publicationTicketResponsibility(tag: IoOperationTag): Option[PublicationTicketResponsibility] =
    require(Thread.currentThread() eq publicationOwner, "publication ticket observation requires exact Main")
    publicationAllocations.get(tag).map { entry =>
      val chunks = entry.ticket.flatMap { ticket =>
        try Some(gateway.forcedChunks(ticket))
        catch
          case cause: Throwable =>
            entry.cause match
              case Some(primary) => if (primary ne cause) && !primary.getSuppressed.contains(cause) then primary.addSuppressed(cause)
              case None => entry.cause = Some(cause)
            None
      }
      PublicationTicketResponsibility(entry.tag, entry.podUuid, entry.world, entry.ticket, chunks, entry.returned, entry.cause)
    }

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

  /** Forge Main 从实际 removal pending 取原 tag/本体身份，不能从 ACK DTO 反建捕获步骤。 */
  private[server] def captureReleaseForIo(removal: PendingPodRemoval, world: PodWorld,
      expectedTicket: ChunkTicket,
      expectedFootprint: Set[com.wi.floatingpod.storage.PhysicalRecoveryChunk]): PendingManagedTicketRelease =
    requireReleaseMain()
    require(removal != null, "actual retained removal pending required")
    world match
      case forge: PodWorldImpl => require(forge.rawWorld eq removal.rawWorld, "ticket and captured body worlds differ")
      case _ => throw new IllegalArgumentException("actual Forge removal requires its exact server world")
    val (tag, version) = removal.pending.completionContext.getOrElse(
      throw new IllegalStateException("Removal pending has no retained IO context"))
    val identity = removal.identity
    require((identity.body eq removal.rawEntity) && (identity.world eq removal.rawWorld) &&
      identity.bootUuid == tag.bootId && identity.operationUuid == tag.operationId,
      "captured actual body identity differs from retained IO operation")
    captureReleaseForIo(tag, identity.podUuid, identity.attemptUuid, version, identity.entityId,
      world, expectedTicket, expectedFootprint)

  /** 在任何释放副作用前保留原25区块及操作身份；不能把失败后的残余列表当原 footprint。 */
  private[server] def captureReleaseForIo(tag: IoOperationTag, podUuid: UUID, attemptUuid: UUID,
      expectedStateVersion: Long, expectedEntityId: Int, world: PodWorld, expectedTicket: ChunkTicket,
      expectedFootprint: Set[com.wi.floatingpod.storage.PhysicalRecoveryChunk]): PendingManagedTicketRelease = synchronized {
    requireReleaseMain()
    require(tag != null && podUuid != null && attemptUuid != null && expectedStateVersion >= 0L &&
      world != null && expectedTicket != null && expectedFootprint != null,
      "exact retained recall operation, ticket and footprint required")
    require(!ioReleases.contains(tag), "release operation was already captured")
    val managed = ticketsByPod.getOrElse(podUuid, throw new IllegalStateException("Exact release ticket is absent"))
    require((managed.ticket eq expectedTicket) && managed.world.sameWorld(world) &&
      (managed.ownershipKey eq keyOf(expectedTicket)), "release does not own captured ticket/world")
    require(!residentReleases.contains(managed.ownershipKey) && !ioReleases.values.exists(pending => pending.managed.ownershipKey eq managed.ownershipKey) &&
      !ticketsAwaitingRelease.contains(managed.ownershipKey), "ticket has a previous unresolved release owner")
    val actual = publicationTicketChunks(podUuid, world, expectedTicket)
    require(actual == expectedFootprint && actual.size == ChunkLoadingManager.DefaultChunkCount &&
      actual.forall(_.dimensionId == world.dimensionId), "original exact25 footprint differs from retained recall")
    val pending = new PendingManagedTicketRelease(this, tag, attemptUuid, expectedStateVersion, expectedEntityId, managed, actual)
    ioReleases.put(tag, pending)
    pending
  }

  /** 先消费确切 removal END 凭证，再释放/重观察；失败保留 pending 和原 footprint。 */
  private[server] def releaseForIo(pending: PendingManagedTicketRelease,
      removal: ExactBodyRemovalAcknowledgement): Try[TicketReleaseAcknowledgement] = Try { synchronized {
    requireReleaseMain()
    require(pending != null && (pending.managerOwner eq this) && ioReleases.get(pending.tag).exists(_ eq pending),
      "exact retained manager release required")
    require(removal != null && removal.bootId() == pending.tag.bootId &&
      removal.bootGeneration() == pending.tag.bootGeneration &&
      removal.connectionGeneration() == pending.tag.connectionGeneration && removal.operationId() == pending.tag.operationId &&
      removal.attemptId() == pending.attemptUuid && removal.podId() == pending.managed.podUuid &&
      removal.expectedStateVersion() == pending.expectedStateVersion && removal.entityId() == pending.expectedEntityId &&
      removal.dimensionId() == pending.managed.world.dimensionId,
      "removal acknowledgement differs from retained recall operation")
    pending.acknowledgement match
      case Some(ack) =>
        require(ack.removal() eq removal, "completed release belongs to another removal acknowledgement")
        ack
      case None =>
        require(ticketsByPod.get(pending.managed.podUuid).exists(_ eq pending.managed),
          "captured ticket ownership was replaced or removed")
        try
          gateway.release(pending.managed.ticket)
          ExactTicketReleaseObserver.requireAbsent(pending.managed.ownershipKey, gateway.releaseSnapshot(pending.managed.ticket))
          val original = pending.originalFootprint.toVector.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))
            .map(chunk => new TicketReleaseAcknowledgement.Chunk(chunk.dimensionId, chunk.chunkX, chunk.chunkZ)).asJava
          val ack = TicketReleaseAcknowledgement.issue(removal, original)
          removeOwnership(pending.managed.ownershipKey)
          pending.acknowledgement = Some(ack)
          ack
        catch
          case cause: Throwable =>
            pending.releaseCause = pending.releaseCause match
              case None => Some(cause)
              case Some(primary) =>
                if primary ne cause then primary.addSuppressed(cause)
                Some(primary)
            throw cause
  } }

  /** 原 ACK 已证明 registry/forced/requested absent；此处只转录原 holder，失败历史不能升级成功。 */
  private[server] def normalRecallTicketFacts(pending: PendingManagedTicketRelease,
      acknowledgement: TicketReleaseAcknowledgement, removal: ExactBodyRemovalAcknowledgement,
      facts: MySqlNormalRecallRemovalFacts, stopRequest: UUID): MySqlNormalRecallTicketFacts = synchronized {
    requireReleaseMain()
    require(pending != null && (pending.managerOwner eq this) && ioReleases.get(pending.tag).exists(_ eq pending) &&
      pending.releaseCause.isEmpty && acknowledgement != null && pending.acknowledgement.exists(_ eq acknowledgement) &&
      (acknowledgement.removal() eq removal) && facts != null && facts.tag == pending.tag &&
      facts.podUuid == pending.managed.podUuid && facts.attemptUuid == pending.attemptUuid &&
      facts.expectedVersion == pending.expectedStateVersion && facts.entityId == pending.expectedEntityId &&
      facts.dimensionId == pending.managed.world.dimensionId && facts.observedEnd == removal.observedEndOrdinal(),
      "normal recall ticket facts require original successful holder and exact removal")
    MySqlNormalRecallTicketFacts(facts, stopRequest, pending.originalFootprint.toVector)
  }

  /** C及两个原plan释放ACK之后才撤原ticket holder；不补发release或抹除失败。 */
  private[server] def compactNormalRecallRelease(owner: NormalRecallResponsibility,
      source: ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry,
      pending: PendingManagedTicketRelease, acknowledgement: TicketReleaseAcknowledgement): Unit = synchronized {
    requireReleaseMain()
    require(owner != null && source != null, "original recall owner and source required")
    owner.requireCompleted(source)
    require(pending != null && (pending.managerOwner eq this) && ioReleases.get(pending.tag).exists(_ eq pending) &&
      pending.releaseCause.isEmpty && pending.acknowledgement.exists(_ eq acknowledgement) &&
      (pending.managed.ticket eq source.ticket) && pending.managed.podUuid == source.podUuid,
      "only original successful recall release holder may compact")
    val key = pending.managed.ownershipKey
    require(!ticketsByPod.values.exists(_.ownershipKey eq key) && !ticketsAwaitingRelease.contains(key),
      "recalled ticket ownership remains")
    ExactTicketReleaseObserver.requireAbsent(key, gateway.releaseSnapshot(source.ticket))
    val allocations = publicationAllocations.iterator.filter { (_, entry) => entry.ticket.exists(_ eq source.ticket) }.toVector
    allocations.foreach { (_, entry) => require(entry.returned && entry.cause.isEmpty, "original ticket allocation has unresolved failure") }
    ioReleases.remove(pending.tag)
    allocations.foreach { (original, entry) =>
      completedPublicationAllocations.put(original, entry.podUuid)
      publicationAllocations.remove(original)
    }
  }

  /** DB确认后的原Main局部清理；只撤原成功holder，不制造旧C/absence或物理擦除资格。 */
  private[server] def compactDbFirstRecallRelease(pending: PendingManagedTicketRelease,
      acknowledgement: TicketReleaseAcknowledgement): Unit = synchronized {
    requireReleaseMain()
    require(pending!=null && (pending.managerOwner eq this) && ioReleases.get(pending.tag).exists(_ eq pending) &&
      pending.releaseCause.isEmpty && acknowledgement!=null && pending.acknowledgement.exists(_ eq acknowledgement),
      "DB-first cleanup requires the exact successful ticket holder/ACK")
    val key=pending.managed.ownershipKey
    require(!ticketsByPod.values.exists(_.ownershipKey eq key) && !ticketsAwaitingRelease.contains(key),
      "DB-first original ticket still has local ownership")
    ExactTicketReleaseObserver.requireAbsent(key,gateway.releaseSnapshot(pending.managed.ticket))
    val allocations=publicationAllocations.iterator.filter((_,entry)=>entry.ticket.exists(_ eq pending.managed.ticket)).toVector
    allocations.foreach { (_,entry)=>require(entry.returned && entry.cause.isEmpty,"original ticket allocation has unresolved failure") }
    allocations.foreach { (tag,entry)=>
      completedPublicationAllocations.put(tag,entry.podUuid)
      publicationAllocations.remove(tag)
    }
    ioReleases.remove(pending.tag)
  }

  private def requireReleaseMain(): Unit =
    if Thread.currentThread() ne publicationOwner then throw new IllegalStateException("Ticket release requires actual owner Main")

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
  /** Main publication只读实际ticket权属/UUID/requested chunks；不查repository，不改变票据。 */
  private[server] def publicationTicketChunks(podUuid: UUID, world: PodWorld,
      expectedTicket: ChunkTicket): Set[com.wi.floatingpod.storage.PhysicalRecoveryChunk] = synchronized {
    if Thread.currentThread() ne publicationOwner then throw new IllegalStateException("Ticket publication requires actual owner Main")
    require(podUuid != null && world != null && expectedTicket != null, "exact ticket observation required")
    val managed = ticketsByPod.getOrElse(podUuid, throw new IllegalStateException("Publication ticket is absent"))
    require(managed.world.sameWorld(world) && (managed.ownershipKey eq keyOf(expectedTicket)) &&
      (managed.ticket eq expectedTicket), "publication does not own the exact managed ticket/world")
    require(!ticketsByPod.values.exists(other => other.podUuid != podUuid && (other.ownershipKey eq managed.ownershipKey)),
      "raw ticket ownership is shared by another pod")
    require(gateway.readPodUuid(managed.ticket).contains(podUuid), "actual ticket UUID differs")
    gateway.forcedChunks(managed.ticket).map((x, z) => com.wi.floatingpod.storage.PhysicalRecoveryChunk(world.dimensionId, x, z))
  }
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
    if dbFirstReads.nonEmpty then reconcileOrphanedTickets(dbFirstReads.get.recoveryReadData.desiredActiveRows.map(_.uuid).toSet)
    else if bootReads.nonEmpty then reconcileOrphanedTickets(requireBootReads().desiredActiveRows.map(_.uuid).toSet)
    else recoveryStore match
      case Some(store) => store.findDesiredActiveRows() match
        case Success(rows) => reconcileOrphanedTickets(rows.iterator.map(_.uuid).toSet)
        case Failure(problem) => appendIssues(List(TicketRecoveryIssue.CandidateProcessingFailed(None, problem)))
      case None => appendIssues(List(TicketRecoveryIssue.CandidateRejected("missing_recovery_store", None, "Repository does not implement RecoveryPodStore")))
  }

  private def desiredActiveRow(uuid: UUID): Either[Throwable, com.wi.floatingpod.storage.StoredPodRow] =
    if dbFirstReads.nonEmpty then
      dbFirstReads.get.recoveryReadData.stored(uuid) match
        case Some(row) if row.isActive => Right(row)
        case _ => Left(new IllegalStateException(s"Recovered ticket references missing or inactive schema6 pod $uuid"))
    else if bootReads.nonEmpty then
      requireBootReads().findStored(uuid) match
        case Some(row) if row.isActive => Right(row)
        case _ => Left(new IllegalStateException(s"Recovered ticket references missing or inactive pod $uuid"))
    else legacyDesiredActiveRow(uuid)

  private def legacyDesiredActiveRow(uuid: UUID) = recoveryStore match
    case Some(store) => store.findStoredByUuid(uuid).toEither.flatMap {
      case Some(row) if row.isActive => Right(row)
      case Some(_) => Left(new IllegalStateException(s"Recovered ticket references inactive pod $uuid"))
      case None => Left(new IllegalStateException(s"Recovered ticket references missing pod $uuid"))
    }
    case None => Left(new IllegalStateException("Repository does not implement RecoveryPodStore"))

  private def releaseManaged(managed: ManagedPodTicket): Unit =
    requireOrdinaryReleaseOwner(managed.ownershipKey)
    try gateway.release(managed.ticket)
    catch
      case problem: Throwable => queueRelease(managed.ticket, managed.ownershipKey); throw problem
    removeOwnership(managed.ownershipKey)

  private def releaseCandidate(ticket: ChunkTicket, uuid: Option[UUID], dimensionId: Int, issues: mutable.ListBuffer[TicketRecoveryIssue]): Unit =
    try
      requireOrdinaryReleaseOwner(keyOf(ticket))
      gateway.release(ticket)
      removeOwnership(keyOf(ticket))
      startupTicketCleanupObservations += StartupTicketCleanupObservation.ReleasedAfterAcceptedRejection(dimensionId, uuid)
    catch
      case problem: Throwable =>
        if !hasPendingIoRelease(keyOf(ticket)) then queueRelease(ticket, keyOf(ticket))
        issues += TicketRecoveryIssue.CandidateReleaseFailed(uuid, problem)
        startupTicketCleanupObservations += StartupTicketCleanupObservation.ReleaseFailed(dimensionId, uuid, problem)

  private def retryPendingTicketReleases(): Unit =
    var firstFailure: Throwable = null
    ticketsAwaitingRelease.toList.foreach { case (key, pending) =>
      try
        requireOrdinaryReleaseOwner(key)
        gateway.release(pending.ticket)
        removeOwnership(key)
      catch
        case problem: Throwable =>
          logger.error("Failed to retry pending chunk ticket release", problem)
          if firstFailure == null then firstFailure = problem
    }
    if firstFailure != null then throw firstFailure

  private def releaseAfterFailure(uuid: UUID, operation: String, primary: Throwable): Unit =
    releaseIfPresent(uuid).failed.foreach { problem =>
      // Boot持有原初始化cause；清理失败随原责任一起保留，不只留在独立日志里。
      if (problem ne primary) && !primary.getSuppressed.exists(_ eq problem) then primary.addSuppressed(problem)
    }
    logger.error(s"Chunk ticket $operation failed for pod $uuid: ${primary.getMessage}", primary)
  private def queueRelease(ticket: ChunkTicket, key: AnyRef): Unit = ticketsAwaitingRelease.put(key, PendingTicketRelease(ticket, key))
  private def hasPendingIoRelease(key: AnyRef): Boolean =
    ioReleases.values.exists(pending => pending.acknowledgement.isEmpty && (pending.managed.ownershipKey eq key))
  private def requireOrdinaryReleaseOwner(key: AnyRef): Unit =
    require(!residentReleases.contains(key), "ticket release belongs to original resident stop checkpoint")
    require(!hasPendingIoRelease(key), "ticket release belongs to retained IO lifecycle")
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
  private[floatingpod] def forClaimedBoot(reads: MainBootProjection, gateway: ChunkTicketGateway): ChunkLoadingManager =
    require(reads != null && gateway != null, "actual Boot reads and gateway required")
    reads.requireCurrentReads()
    new ChunkLoadingManager(None, gateway, Some(reads), None)
  private[floatingpod] def forDbFirst(reads: ProductionDbFirstBootstrapCallbacks,
      gateway: ChunkTicketGateway): ChunkLoadingManager =
    require(reads != null && gateway != null, "actual schema6 Main reads and gateway required")
    reads.requireCurrentReads()
    new ChunkLoadingManager(None, gateway, None, Some(reads))
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
  override def release(ticket: ChunkTicket): Unit =
    val raw = asForgeTicket(ticket)
    require(raw.getType == ForgeChunkManager.Type.NORMAL, "owned gateway releases normal tickets only")
    ExactTicketReleaseObserver.release(raw, new TicketReleaseView:
      override def assertMain(): Unit =
        val server = raw.world.getMinecraftServer
        if raw.world.isRemote || server == null || !server.isCallingFromMinecraftThread then
          throw new IllegalStateException("Exact Forge ticket release requires server Main")
      override def release(): Unit = ForgeChunkManager.releaseTicket(raw)
      override def snapshot(): TicketReleaseSnapshot = releaseSnapshot(ticket)
    )
  override private[server] def releaseSnapshot(ticket: ChunkTicket): TicketReleaseSnapshot =
    val raw = asForgeTicket(ticket)
    val server = raw.world.getMinecraftServer
    require(raw.getType == ForgeChunkManager.Type.NORMAL, "owned gateway observes normal tickets only")
    if raw.world.isRemote || server == null || !server.isCallingFromMinecraftThread then
      throw new IllegalStateException("Exact ticket observation requires server Main")
    val registry = com.wi.floatingpod.mixin.ForgeTicketRegistryAccessor.getTickets()
    val registered = Option(registry.get(raw.world)).toVector.flatMap(_.values().asScala.toVector)
    val forced = ForgeChunkManager.getPersistentChunksFor(raw.world).values().asScala.toVector
    TicketReleaseSnapshot(registered.map(ticket => ticket: AnyRef), forced.map(ticket => ticket: AnyRef),
      raw.getChunkList.asScala.iterator.map(chunk => (chunk.x, chunk.z)).toSet)
  override def maxChunkDepth(ticket: ChunkTicket): Int = asForgeTicket(ticket).getMaxChunkListDepth
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = asForgeTicket(ticket).setChunkListDepth(depth)
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = asForgeTicket(ticket).getModData.setUniqueId(ChunkLoadingManager.PodUuidKey, podUuid)
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] =
    val data: NBTTagCompound = asForgeTicket(ticket).getModData
    if data.hasUniqueId(ChunkLoadingManager.PodUuidKey) then Some(data.getUniqueId(ChunkLoadingManager.PodUuidKey)) else None
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ForgeChunkManager.forceChunk(asForgeTicket(ticket), new ChunkPos(chunkX, chunkZ))
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ForgeChunkManager.unforceChunk(asForgeTicket(ticket), new ChunkPos(chunkX, chunkZ))
  override def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] =
    asForgeTicket(ticket).getChunkList.asScala.iterator.map(chunk => (chunk.x, chunk.z)).toSet
  override def wrapRecovered(ticket: Ticket): ChunkTicket = new ChunkTicketImpl(ticket)
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = Some(asForgeTicket(ticket))
  private def asForgeTicket(ticket: ChunkTicket): Ticket = ticket match
    case forgeTicket: ChunkTicketImpl => forgeTicket.rawTicket
    case _ => throw new IllegalArgumentException("Forge gateway received a non-Forge chunk ticket")
