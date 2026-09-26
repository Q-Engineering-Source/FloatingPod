package com.wi.floatingpod.server

import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.storage.{AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryChunk, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore}
import net.minecraft.world.{World, WorldServer}
import org.apache.logging.log4j.LogManager
import java.util.UUID
import scala.collection.mutable
import scala.math.floor
import scala.util.{Failure, Success, Try}

/** 运行中辅助机的最小句柄；UUID 是跨实体重建保持不变的身份。 */
trait ActivePodEntity:
  def entityId: Int
  def podUuid: UUID

/** 与真实服务端世界交互的窄适配边界。 */
trait PodWorld:
  def dimensionId: Int
  def diagnosticWorldName: String
  def findPod(podUuid: UUID): Option[ActivePodEntity]
  def sameWorld(other: PodWorld): Boolean
  def spawnPod(state: FloatingPodState): ActivePodEntity
  def removePod(entity: ActivePodEntity): Unit

/** 真实 Forge 服务端世界适配器；客户端或非 WorldServer 绝不能进入生命周期路径。 */
final class PodWorldImpl private (val rawWorld: WorldServer) extends PodWorld:
  override def dimensionId: Int = rawWorld.provider.getDimension
  override def diagnosticWorldName: String = rawWorld.getWorldInfo.getWorldName
  override def sameWorld(other: PodWorld): Boolean = other match
    case forge: PodWorldImpl => rawWorld.eq(forge.rawWorld)
    case _ => false
  override def findPod(podUuid: UUID): Option[ActivePodEntity] =
    require(podUuid != null, "podUuid must not be null")
    Option(rawWorld.getEntityFromUuid(podUuid)) match
      case None => None
      case Some(entity: FloatingPodEntity) => Some(new ActivePodEntityImpl(entity))
      case Some(other) => throw new IllegalStateException(s"World $diagnosticWorldName already contains non-FloatingPod UUID $podUuid: ${other.getClass.getName}")
  override def spawnPod(state: FloatingPodState): ActivePodEntity =
    require(state != null, "state must not be null")
    findPod(state.uuid).foreach(_ => throw new IllegalStateException(s"World $diagnosticWorldName already contains FloatingPod ${state.uuid}"))
    val entity = new FloatingPodEntity(rawWorld)
    entity.setUniqueId(state.uuid)
    entity.setLocationAndAngles(state.posX, state.posY, state.posZ, state.yaw, state.pitch)
    if !rawWorld.spawnEntity(entity) then throw new IllegalStateException(s"World $diagnosticWorldName refused to spawn FloatingPod ${state.uuid}")
    findPod(state.uuid) match
      case Some(found: ActivePodEntityImpl) if found.rawEntity eq entity => found
      case Some(_) => throw new IllegalStateException(s"World $diagnosticWorldName returned a different entity for spawned FloatingPod ${state.uuid}")
      case None => throw new IllegalStateException(s"World $diagnosticWorldName did not index spawned FloatingPod ${state.uuid}")
  override def removePod(entity: ActivePodEntity): Unit = entity match
    case forgeEntity: ActivePodEntityImpl => rawWorld.removeEntity(forgeEntity.rawEntity)
    case _ => throw new IllegalArgumentException("Forge world received a non-Forge FloatingPod entity")

object PodWorldImpl:
  def apply(world: World): PodWorldImpl =
    require(world != null, "world must not be null")
    world match
      case server: WorldServer => new PodWorldImpl(server)
      case _ => throw new IllegalArgumentException("FloatingPod lifecycle requires a server WorldServer")

/** 真实实体的句柄，直接保留 vanilla UUID，避免从数值 entity ID 推导身份。 */
final class ActivePodEntityImpl private[server] (val rawEntity: FloatingPodEntity) extends ActivePodEntity:
  override def entityId: Int = rawEntity.getEntityId
  override def podUuid: UUID = rawEntity.getUniqueID

sealed trait SpawnOutcome
object SpawnOutcome:
  final case class Accepted(spawned: SpawnedActivePod) extends SpawnOutcome
  final case class Rejected(reason: AdmissionRejection) extends SpawnOutcome

/** 成功发布的活动实体和数据库实际提交的原样状态。 */
final case class SpawnedActivePod(entity: ActivePodEntity, exactState: FloatingPodState):
  require(entity != null, "entity must not be null")
  require(exactState != null && exactState.isActive && exactState.dimensionId.nonEmpty, "spawned state must be active and dimensioned")
  require(entity.podUuid == exactState.uuid, "entity UUID must equal committed state UUID")

sealed trait RecallOutcome
object RecallOutcome:
  final case class Accepted(exactState: FloatingPodState) extends RecallOutcome
  final case class Rejected(reason: DeactivationRejection) extends RecallOutcome

sealed trait RecoveryDisposition
object RecoveryDisposition:
  final case class Attached(spawned: SpawnedActivePod) extends RecoveryDisposition
  final case class Recreated(spawned: SpawnedActivePod) extends RecoveryDisposition
  case object Replayed extends RecoveryDisposition

/** 关服开始时的稳定管理状态；报告只暴露诊断，不暴露可变实体句柄。 */
sealed trait ManagerShutdownStatus
object ManagerShutdownStatus:
  case object Active extends ManagerShutdownStatus
  case object PendingTicketRelease extends ManagerShutdownStatus
  case object PendingCommit extends ManagerShutdownStatus
  case object Faulted extends ManagerShutdownStatus
  case object DurablyActivatedFault extends ManagerShutdownStatus
  case object DurablyDeactivatedFault extends ManagerShutdownStatus

/** 单个辅助机关服推进的不可变结果。 */
final case class ManagerShutdownEntry(podUuid: UUID, initialStatus: ManagerShutdownStatus, cause: Option[Throwable]):
  require(podUuid != null, "podUuid must not be null")
  require(initialStatus != null, "initialStatus must not be null")
  require(cause != null, "cause must not be null")

/** 两阶段停服的完整报告；任何残留状态或原因都不能被表示为成功。 */
final case class ManagerShutdownReport(
    entries: List[ManagerShutdownEntry],
    recalledUuids: List[UUID],
    remaining: Map[UUID, ManagerShutdownStatus],
    pendingTicketReconciliationFailure: Option[Throwable],
    finalFlushFailure: Option[Throwable]
):
  require(entries != null && recalledUuids != null && remaining != null, "shutdown report collections must not be null")
  require(pendingTicketReconciliationFailure != null && finalFlushFailure != null, "shutdown report failures must not be null")
  def isClean: Boolean = entries.forall(_.cause.isEmpty) && remaining.isEmpty && pendingTicketReconciliationFailure.isEmpty && finalFlushFailure.isEmpty

/** 供关服报告读取的阻塞收回快照，不泄漏可变实体记录。 */
sealed trait RecallRecoveryStatus
object RecallRecoveryStatus:
  case object PendingTicketRelease extends RecallRecoveryStatus
  case object PendingCommit extends RecallRecoveryStatus
  final case class Faulted(reason: DeactivationRejection) extends RecallRecoveryStatus

private final case class ManagedActivePod(entity: ActivePodEntity, world: PodWorld, exactState: FloatingPodState, attemptUuid: UUID, entityRemoved: Boolean = false):
  require(attemptUuid != null, "attemptUuid must not be null")
private sealed trait ManagedPodStatus:
  def record: ManagedActivePod
private object ManagedPodStatus:
  final case class Active(record: ManagedActivePod) extends ManagedPodStatus
  final case class RecallPendingTicketRelease(record: ManagedActivePod) extends ManagedPodStatus
  final case class RecallPendingCommit(record: ManagedActivePod) extends ManagedPodStatus
  final case class RecallFaulted(record: ManagedActivePod, reason: DeactivationRejection) extends ManagedPodStatus
  final case class DurablyActivatedFault(record: ManagedActivePod, fault: CommittedSpawnFault) extends ManagedPodStatus
  final case class DurablyDeactivatedFault(record: ManagedActivePod, fault: CommittedDeactivationFault) extends ManagedPodStatus

/** 已跨过数据库提交边界的 spawn 故障快照；后续恢复负责对账而非反向清理。 */
private[server] final case class CommittedSpawnFault(podUuid: UUID, worldName: String, dimensionId: Int, entityId: Int, exactCommittedState: FloatingPodState, cause: Throwable):
  require(podUuid != null, "podUuid must not be null")
  require(worldName != null, "worldName must not be null")
  require(exactCommittedState != null, "exactCommittedState must not be null")
  require(cause != null, "cause must not be null")

/** 已跨过数据库收回提交边界的故障快照；不得再次调用 deactivation transition。 */
private[server] final case class CommittedDeactivationFault(exactCommittedState: FloatingPodState, cause: Throwable):
  require(exactCommittedState != null, "exactCommittedState must not be null")
  require(cause != null, "cause must not be null")

/** UUID 状态表是唯一事实；entityId 查询由 Active 记录即时导出。 */
final class ActiveFloatingPodManager(repository: FloatingPodRepository, chunkLoadingManager: ChunkLoadingManager, clockMillis: () => Long = () => System.currentTimeMillis(), attemptUuidGenerator: () => UUID = () => UUID.randomUUID()):
  private val logger = LogManager.getLogger(getClass)
  private val managedByPod = mutable.HashMap.empty[UUID, ManagedPodStatus]
  private val cachedStates = mutable.HashMap.empty[UUID, FloatingPodState]
  private val dirtyPods = mutable.HashSet.empty[UUID]
  private val failedSpawnQuarantines = mutable.HashMap.empty[UUID, Throwable]

  /** S3 授权释放入口：提交成功前不发布内存映射。 */
  def spawn(podUuid: UUID, authenticatedOwner: UUID, expectedStateVersion: Long, world: PodWorld, posX: Double, posY: Double, posZ: Double, globalActiveLimit: Int): Try[SpawnOutcome] = Try {
    require(podUuid != null, "podUuid must not be null")
    require(authenticatedOwner != null, "authenticatedOwner must not be null")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    require(world != null, "world must not be null")
    require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be 1..256")
    synchronized {
      failedSpawnQuarantines.get(podUuid).foreach(primary => throw new IllegalStateException(s"FloatingPod $podUuid is quarantined after failed spawn cleanup", primary))
      repository.findByUuid(podUuid).get match
        case None => SpawnOutcome.Rejected(AdmissionRejection.UnknownPod)
        case Some(source) if source.uuid != podUuid => throw new IllegalStateException(s"Repository returned mismatched UUID ${source.uuid} for $podUuid")
        case Some(_) if managedByPod.get(podUuid).exists(_.isInstanceOf[ManagedPodStatus.DurablyActivatedFault]) =>
          throw new IllegalStateException(s"FloatingPod $podUuid is blocked by a committed spawn fault")
        case Some(source) if !source.ownerUuid.contains(authenticatedOwner) => SpawnOutcome.Rejected(AdmissionRejection.NotOwner)
        case Some(source) if source.stateVersion != expectedStateVersion => SpawnOutcome.Rejected(AdmissionRejection.VersionMismatch(source.stateVersion))
        case Some(source) if source.isActive || managedByPod.contains(podUuid) => SpawnOutcome.Rejected(AdmissionRejection.AlreadyActive)
        case Some(source) =>
          val physicalCandidate = source.copy(worldName = world.diagnosticWorldName, posX = posX, posY = posY, posZ = posZ, isActive = true, lastUpdated = clockMillis(), dimensionId = Some(world.dimensionId))
          val transitionTarget = physicalCandidate.copy(isActive = false, dimensionId = source.dimensionId)
          val attemptUuid = attemptUuidGenerator()
          require(attemptUuid != null, "attemptUuidGenerator must not return null")
          physicalRecoveryStore.prepareCreateLocator(podUuid, source.stateVersion, attemptUuid, world.dimensionId, recoveryChunks(world.dimensionId, posX, posZ)).get
          chunkLoadingManager.requestTicket(podUuid, world, chunkCoordinate(posX), chunkCoordinate(posZ)).get
          CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.BeforeEntitySpawn)
          var entity: ActivePodEntity = null
          var acceptedState: FloatingPodState = null
          var compensationStarted = false
          try
            entity = world.spawnPod(physicalCandidate)
            validateSpawnedEntity(podUuid, entity)
            ensureNoActiveEntityIdCollision(entity.entityId, podUuid)
            CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterEntitySpawn)
            physicalRecoveryStore.activatePreparedIfAdmissible(transitionTarget, authenticatedOwner, expectedStateVersion, world.dimensionId, globalActiveLimit, attemptUuid).get match
              case AdmissionOutcome.Rejected(reason) =>
                compensationStarted = true
                compensateRejectedSpawn(podUuid, world, entity, reason)
                SpawnOutcome.Rejected(reason)
              case AdmissionOutcome.Accepted(committed) =>
                acceptedState = committed
                try
                  validateCommittedActive(committed, podUuid, world.dimensionId)
                  CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterDbCommitSpawn)
                  managedByPod.put(podUuid, ManagedPodStatus.Active(ManagedActivePod(entity, world, committed, attemptUuid)))
                  cachedStates.put(podUuid, committed)
                  dirtyPods.remove(podUuid)
                  SpawnOutcome.Accepted(SpawnedActivePod(entity, committed))
                catch
                  case crash: CrashPointHarness.CrashPointException => throw crash
                  case failure: Throwable =>
                    recordCommittedSpawnFault(podUuid, world, entity, committed, attemptUuid, failure)
                    throw failure
          catch
            case crash: CrashPointHarness.CrashPointException => throw crash
            case primary: Throwable =>
              if acceptedState != null then throw primary
              if !compensationStarted then compensateInfrastructureSpawn(podUuid, world, entity, primary)
              else throw primary
    }
  }

  /** 先对已发布精确状态做 owner/version 检查，之后才允许物理收回。 */
  def recallIfExpected(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[RecallOutcome] = Try {
    require(podUuid != null, "podUuid must not be null")
    require(expectedOwner != null && expectedOwner.forall(_ != null), "expectedOwner must not be null")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    synchronized {
      managedByPod.get(podUuid) match
        case Some(ManagedPodStatus.Active(record)) => recallActive(podUuid, record, expectedOwner, expectedStateVersion)
        case Some(_: ManagedPodStatus.RecallPendingTicketRelease) => throw new IllegalStateException(s"FloatingPod $podUuid awaits trusted ticket release retry")
        case Some(_: ManagedPodStatus.RecallPendingCommit) => throw new IllegalStateException(s"FloatingPod $podUuid awaits trusted recall commit retry")
        case Some(ManagedPodStatus.RecallFaulted(_, reason)) => throw new IllegalStateException(s"FloatingPod $podUuid is recall-faulted: $reason")
        case Some(_: ManagedPodStatus.DurablyActivatedFault) => throw new IllegalStateException(s"FloatingPod $podUuid is blocked by a committed spawn fault")
        case Some(_: ManagedPodStatus.DurablyDeactivatedFault) => throw new IllegalStateException(s"FloatingPod $podUuid is blocked by a committed deactivation fault")
        case None => RecallOutcome.Rejected(DeactivationRejection.UnknownPod)
    }
  }

  /** 可信兼容入口：pending 只重试数据库收回，不重复实体或 ticket 物理操作。 */
  def recall(podUuid: UUID): Try[FloatingPodState] = Try {
    require(podUuid != null, "podUuid must not be null")
    synchronized {
      val outcome = managedByPod.get(podUuid) match
        case Some(ManagedPodStatus.Active(record)) => recallActive(podUuid, record, record.exactState.ownerUuid, record.exactState.stateVersion)
        case Some(_: ManagedPodStatus.RecallPendingTicketRelease) => resumePendingTicketRelease(podUuid)
        case Some(ManagedPodStatus.RecallPendingCommit(_)) => retryPendingRecall(podUuid).get
        case Some(ManagedPodStatus.RecallFaulted(_, reason)) => throw new IllegalStateException(s"FloatingPod $podUuid is recall-faulted: $reason")
        case Some(_: ManagedPodStatus.DurablyActivatedFault) => throw new IllegalStateException(s"FloatingPod $podUuid is blocked by a committed spawn fault")
        case Some(_: ManagedPodStatus.DurablyDeactivatedFault) => throw new IllegalStateException(s"FloatingPod $podUuid is blocked by a committed deactivation fault")
        case None => throw new IllegalStateException(s"FloatingPod $podUuid is not active")
      outcome match
        case RecallOutcome.Accepted(state) => state
        case RecallOutcome.Rejected(reason) => throw new IllegalStateException(s"Trusted recall rejected for $podUuid: $reason")
    }
  }
  /** package 测试和关服对账入口；绝不重新移除实体或释放 ticket。 */
  private[server] def retryPendingRecall(podUuid: UUID): Try[RecallOutcome] = Try {
    require(podUuid != null, "podUuid must not be null")
    synchronized {
      managedByPod.get(podUuid) match
        case Some(ManagedPodStatus.RecallPendingCommit(record)) => commitRecallOnly(podUuid, record)
        case _ => throw new IllegalStateException(s"FloatingPod $podUuid has no pending recall commit")
    }
  }

  /** 恢复已提交的活动状态，不写数据库或增加版本；重复回放只返回 Replayed。 */
  def recoverCommittedActive(state: FloatingPodState, world: PodWorld): Try[RecoveryDisposition] = Try {
    require(state != null, "state must not be null")
    require(world != null, "world must not be null")
    synchronized {
      if !state.isActive || state.dimensionId.isEmpty then throw new IllegalArgumentException(s"Recovery state ${state.uuid} must be active and dimensioned")
      if state.dimensionId.get != world.dimensionId then throw new IllegalArgumentException(s"Recovery dimension ${state.dimensionId.get} does not match world ${world.dimensionId}")
      val persisted = repository.findByUuid(state.uuid).get.getOrElse(throw new IllegalStateException(s"No committed state exists for ${state.uuid}"))
      if persisted != state then throw new IllegalStateException(s"Recovery state ${state.uuid} is not the exact committed database state")
      val attemptUuid = requireExactActiveLocator(state, world)
      val ticket = chunkLoadingManager.managedTicketRecords().getOrElse(state.uuid, throw new IllegalStateException(s"No managed ticket exists for recovery pod ${state.uuid}"))
      if !ticket.world.sameWorld(world) || ticket.podUuid != state.uuid || ticket.ownershipKey == null then throw new IllegalStateException(s"Managed ticket does not canonically belong to recovery pod ${state.uuid}")
      managedByPod.get(state.uuid) match
        case Some(ManagedPodStatus.Active(record)) if record.world.sameWorld(world) && record.exactState == state && record.attemptUuid == attemptUuid && record.entity.podUuid == state.uuid => RecoveryDisposition.Replayed
        case Some(_) => throw new IllegalStateException(s"FloatingPod ${state.uuid} already has a conflicting manager status")
        case None =>
          world.findPod(state.uuid) match
            case Some(entity) =>
              validateSpawnedEntity(state.uuid, entity); ensureNoActiveEntityIdCollision(entity.entityId, state.uuid); publishRecovered(state, world, entity, attemptUuid)
              RecoveryDisposition.Attached(SpawnedActivePod(entity, state))
            case None =>
              val entity = world.spawnPod(state)
              try
                validateSpawnedEntity(state.uuid, entity); ensureNoActiveEntityIdCollision(entity.entityId, state.uuid); publishRecovered(state, world, entity, attemptUuid)
                RecoveryDisposition.Recreated(SpawnedActivePod(entity, state))
              catch
                case primary: Throwable =>
                  try world.removePod(entity)
                  catch case cleanup: Throwable => primary.addSuppressed(cleanup)
                  throw primary
    }
  }

  def getActivePod(podUuid: UUID): Option[ActivePodEntity] = synchronized { Option(podUuid).flatMap(uuid => managedByPod.get(uuid).collect { case ManagedPodStatus.Active(record) => record.entity }) }
  def getActiveEntity(podUuid: UUID): Option[FloatingPodEntity] = getActivePod(podUuid).collect { case entity: ActivePodEntityImpl => entity.rawEntity }
  def getPodUuidByEntityId(entityId: Int): Option[UUID] = synchronized { managedByPod.collectFirst { case (uuid, ManagedPodStatus.Active(record)) if record.entity.entityId == entityId => uuid } }
  def getAllActivePods(): List[UUID] = synchronized { managedByPod.collect { case (uuid, _: ManagedPodStatus.Active) => uuid }.toList.sortBy(_.toString) }
  private[server] def blockedRecallSnapshot(): Map[UUID, RecallRecoveryStatus] = synchronized {
    managedByPod.collect {
      case (uuid, _: ManagedPodStatus.RecallPendingTicketRelease) => uuid -> RecallRecoveryStatus.PendingTicketRelease
      case (uuid, _: ManagedPodStatus.RecallPendingCommit) => uuid -> RecallRecoveryStatus.PendingCommit
      case (uuid, ManagedPodStatus.RecallFaulted(_, reason)) => uuid -> RecallRecoveryStatus.Faulted(reason)
    }.toMap
  }
  private[server] def committedSpawnFaultSnapshot(): Map[UUID, CommittedSpawnFault] = synchronized {
    managedByPod.collect { case (uuid, ManagedPodStatus.DurablyActivatedFault(_, fault)) => uuid -> fault }.toMap
  }
  private[server] def durablyDeactivatedFaultSnapshot(): Map[UUID, CommittedDeactivationFault] = synchronized {
    managedByPod.collect { case (uuid, ManagedPodStatus.DurablyDeactivatedFault(_, fault)) => uuid -> fault }.toMap
  }

  /** cachedStates 是运行时非关键漂移缓存；关键身份仍以 ManagedActivePod.exactState 为准。 */
  def updateCachedState(podUuid: UUID, state: FloatingPodState): Try[Unit] = Try {
    require(podUuid != null, "podUuid must not be null"); require(state != null, "state must not be null")
    synchronized {
      val record = managedByPod.get(podUuid).collect { case ManagedPodStatus.Active(active) => active }.getOrElse(throw new IllegalStateException(s"Cannot cache state for inactive pod $podUuid"))
      if state.uuid != podUuid then throw new IllegalArgumentException(s"Cached state UUID ${state.uuid} does not match pod UUID $podUuid")
      requireRuntimeDriftOnly(podUuid, record.exactState, state)
      cachedStates.put(podUuid, state); dirtyPods += podUuid
    }
  }
  def flushCachedStates(): Try[Int] = Try {
    synchronized {
      var persistedCount = 0
      dirtyPods.toList.sortBy(_.toString).foreach { podUuid =>
        val active = managedByPod.get(podUuid).collect { case ManagedPodStatus.Active(record) => record }.getOrElse(throw new IllegalStateException(s"Dirty pod $podUuid is not active"))
        val state = cachedStates.getOrElse(podUuid, throw new IllegalStateException(s"Dirty pod $podUuid has no cached state"))
        val persisted = repository.update(state).get
        if persisted.uuid != podUuid || !persisted.isActive || persisted.dimensionId != active.exactState.dimensionId then throw new IllegalStateException(s"Repository returned invalid drift state for $podUuid")
        cachedStates.put(podUuid, persisted); managedByPod.put(podUuid, ManagedPodStatus.Active(active.copy(exactState = persisted))); dirtyPods.remove(podUuid); persistedCount += 1
      }
      persistedCount
    }
  }
  /** 关服可信推进：逐一尝试所有初始状态，任何失败都保留在报告中并继续下一项。 */
  def shutdownWithReport(): ManagerShutdownReport =
    val initial = synchronized { managedByPod.toList.sortBy(_._1.toString).map((uuid, status) => uuid -> shutdownStatus(status)) }
    val entries = initial.map { case (uuid, status) =>
      val failure = recall(uuid).failed.toOption
      ManagerShutdownEntry(uuid, status, failure)
    }
    val pendingFailure = chunkLoadingManager.reconcilePendingTicketReleases().failed.toOption
    val flushFailure = flushCachedStates().failed.toOption
    val remaining = synchronized { managedByPod.view.mapValues(shutdownStatus).toMap }
    ManagerShutdownReport(entries, entries.collect { case entry if entry.cause.isEmpty => entry.podUuid }, remaining, pendingFailure, flushFailure)

  /** 启动恢复失败时只撤销本次内存和实体发布；绝不触碰已接管 ticket 或持久化活动记录。 */
  private[server] def abortRecoveredActive(podUuid: UUID, world: PodWorld): Try[Unit] = Try {
    require(podUuid != null, "podUuid must not be null")
    require(world != null, "world must not be null")
    synchronized {
      try world.findPod(podUuid).foreach(world.removePod)
      finally
        managedByPod.remove(podUuid)
        cachedStates.remove(podUuid)
        dirtyPods.remove(podUuid)
    }
  }

  private def shutdownStatus(status: ManagedPodStatus): ManagerShutdownStatus = status match
    case _: ManagedPodStatus.Active => ManagerShutdownStatus.Active
    case _: ManagedPodStatus.RecallPendingTicketRelease => ManagerShutdownStatus.PendingTicketRelease
    case _: ManagedPodStatus.RecallPendingCommit => ManagerShutdownStatus.PendingCommit
    case _: ManagedPodStatus.RecallFaulted => ManagerShutdownStatus.Faulted
    case _: ManagedPodStatus.DurablyActivatedFault => ManagerShutdownStatus.DurablyActivatedFault
    case _: ManagedPodStatus.DurablyDeactivatedFault => ManagerShutdownStatus.DurablyDeactivatedFault

  private def recallActive(podUuid: UUID, record: ManagedActivePod, expectedOwner: Option[UUID], expectedVersion: Long): RecallOutcome =
    if record.exactState.ownerUuid != expectedOwner then return RecallOutcome.Rejected(DeactivationRejection.NotOwner)
    if record.exactState.stateVersion != expectedVersion then return RecallOutcome.Rejected(DeactivationRejection.VersionMismatch(record.exactState.stateVersion))
    if !record.entityRemoved then
      requireExactActiveLocator(record.exactState, record.world, record.attemptUuid)
      record.world.removePod(record.entity)
      managedByPod.put(podUuid, ManagedPodStatus.RecallPendingTicketRelease(record.copy(entityRemoved = true)))
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterRecallEntityRemoval)
    resumePendingTicketRelease(podUuid)

  /** 释放成功后才进入仅数据库重试阶段；此路径绝不再次移除实体。 */
  private[server] def resumePendingTicketRelease(podUuid: UUID): RecallOutcome =
    val pending = managedByPod.get(podUuid) match
      case Some(ManagedPodStatus.RecallPendingTicketRelease(record)) => record
      case _ => throw new IllegalStateException(s"FloatingPod $podUuid has no pending ticket release")
    chunkLoadingManager.releaseIfPresent(podUuid).get
    managedByPod.put(podUuid, ManagedPodStatus.RecallPendingCommit(pending))
    CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterRecallTicketRelease)
    try commitRecallOnly(podUuid, pending)
    catch
      case primary: Throwable =>
        if !managedByPod.get(podUuid).exists(_.isInstanceOf[ManagedPodStatus.DurablyDeactivatedFault]) then
          managedByPod.put(podUuid, ManagedPodStatus.RecallPendingCommit(pending))
        throw primary
  private def commitRecallOnly(podUuid: UUID, record: ManagedActivePod): RecallOutcome =
    physicalRecoveryStore.deactivateTrackedIfExpectedActive(podUuid, record.exactState.ownerUuid, record.exactState.stateVersion, record.attemptUuid).get match
      case DeactivationOutcome.Accepted(committed) =>
        try
          validateCommittedInactive(committed, podUuid)
          CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterDbCommitRecall)
          managedByPod.remove(podUuid); cachedStates.remove(podUuid); dirtyPods.remove(podUuid); RecallOutcome.Accepted(committed)
        catch
          case failure: Throwable =>
            recordCommittedDeactivationFault(podUuid, record, committed, failure)
            throw failure
      case DeactivationOutcome.Rejected(reason) =>
        managedByPod.put(podUuid, ManagedPodStatus.RecallFaulted(record, reason)); cachedStates.remove(podUuid); dirtyPods.remove(podUuid); RecallOutcome.Rejected(reason)
  private def publishRecovered(state: FloatingPodState, world: PodWorld, entity: ActivePodEntity, attemptUuid: UUID): Unit =
    managedByPod.put(state.uuid, ManagedPodStatus.Active(ManagedActivePod(entity, world, state, attemptUuid))); cachedStates.put(state.uuid, state); dirtyPods.remove(state.uuid)
  private def physicalRecoveryStore: PhysicalRecoveryStore = repository match
    case store: PhysicalRecoveryStore => store
    case _ => throw new IllegalStateException("S3 lifecycle requires a PhysicalRecoveryStore repository")
  private def validateSpawnedEntity(podUuid: UUID, entity: ActivePodEntity): Unit =
    if entity == null then throw new IllegalStateException(s"World returned null entity for $podUuid")
    if entity.podUuid != podUuid then throw new IllegalStateException(s"World returned entity UUID ${entity.podUuid} for $podUuid")
  private def ensureNoActiveEntityIdCollision(entityId: Int, podUuid: UUID): Unit =
    getPodUuidByEntityId(entityId).foreach(existing => if existing != podUuid then throw new IllegalStateException(s"Entity id $entityId is already mapped to $existing"))
  private def validateCommittedActive(state: FloatingPodState, uuid: UUID, dimension: Int): Unit =
    if state == null || state.uuid != uuid || !state.isActive || !state.dimensionId.contains(dimension) then throw new IllegalStateException(s"Transition store returned invalid active state for $uuid")
  private def validateCommittedInactive(state: FloatingPodState, uuid: UUID): Unit =
    if state == null || state.uuid != uuid || state.isActive then throw new IllegalStateException(s"Transition store returned invalid inactive state for $uuid")
  private def requireExactActiveLocator(state: FloatingPodState, world: PodWorld, expectedAttemptUuid: UUID = null): UUID =
    val expectedChunks = recoveryChunks(world.dimensionId, state.posX, state.posZ)
    physicalRecoveryStore.findPhysicalRecoveryLocator(state.uuid).get match
      case Some(locator)
          if locator.podUuid == state.uuid
            && locator.attemptUuid != null
            && (expectedAttemptUuid == null || locator.attemptUuid == expectedAttemptUuid)
            && locator.state == PhysicalRecoveryLocatorState.RowActive
            && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive
            && locator.domainVersion.contains(state.stateVersion)
            && state.dimensionId.contains(world.dimensionId)
            && locator.targetDimensionId == state.dimensionId
            && locator.targetDimensionId.contains(world.dimensionId)
            && locator.chunks.size == expectedChunks.size
            && locator.chunks.toSet == expectedChunks => locator.attemptUuid
      case _ => throw new IllegalStateException(s"No exact active physical recovery locator exists for ${state.uuid}")
  private def compensateRejectedSpawn(podUuid: UUID, world: PodWorld, entity: ActivePodEntity, rejection: AdmissionRejection): Unit =
    val rejectionCause = new IllegalStateException(s"Spawn rejected for $podUuid: $rejection")
    compensationFailures(podUuid, world, entity, rejectionCause) match
      case Nil =>
      case failures =>
        val aggregate = new IllegalStateException(s"Spawn rejection cleanup failed for $podUuid", rejectionCause)
        failures.foreach(aggregate.addSuppressed)
        throw aggregate
  private def compensateInfrastructureSpawn(podUuid: UUID, world: PodWorld, entity: ActivePodEntity, primary: Throwable): Nothing =
    compensationFailures(podUuid, world, entity, primary).foreach(primary.addSuppressed)
    throw primary
  private def compensationFailures(podUuid: UUID, world: PodWorld, entity: ActivePodEntity, primary: Throwable): List[Throwable] =
    val failures = mutable.ListBuffer.empty[Throwable]
    if entity != null then
      cleanSpawnEntity(world, entity, podUuid).foreach { cleanup =>
        quarantineFailedSpawn(podUuid, primary, cleanup)
        failures += cleanup
      }
    cleanSpawnTicket(podUuid).foreach(failures += _)
    failures.toList
  private def recordCommittedSpawnFault(podUuid: UUID, world: PodWorld, entity: ActivePodEntity, exactCommittedState: FloatingPodState, attemptUuid: UUID, cause: Throwable): Unit =
    val record = ManagedActivePod(entity, world, exactCommittedState, attemptUuid)
    val fault = CommittedSpawnFault(podUuid, world.diagnosticWorldName, world.dimensionId, entity.entityId, exactCommittedState, cause)
    managedByPod.put(podUuid, ManagedPodStatus.DurablyActivatedFault(record, fault))
    dirtyPods.remove(podUuid)
  private def recordCommittedDeactivationFault(podUuid: UUID, record: ManagedActivePod, exactCommittedState: FloatingPodState, cause: Throwable): Unit =
    managedByPod.put(podUuid, ManagedPodStatus.DurablyDeactivatedFault(record.copy(exactState = exactCommittedState), CommittedDeactivationFault(exactCommittedState, cause)))
    cachedStates.remove(podUuid)
    dirtyPods.remove(podUuid)
  private def cleanSpawnEntity(world: PodWorld, entity: ActivePodEntity, podUuid: UUID): Option[Throwable] =
    try
      world.removePod(entity)
      None
    catch
      case cleanup: Throwable =>
        logger.error(s"Failed to remove spawned entity for pod $podUuid", cleanup)
        Some(cleanup)
  private def cleanSpawnTicket(podUuid: UUID): Option[Throwable] = chunkLoadingManager.releaseIfPresent(podUuid) match
    case Success(_) => None
    case Failure(cleanup) =>
      logger.error(s"Failed to release ticket for pod $podUuid", cleanup)
      Some(cleanup)
  private def quarantineFailedSpawn(podUuid: UUID, primary: Throwable, cleanup: Throwable): Unit = { failedSpawnQuarantines.put(podUuid, primary); logger.error(s"Quarantined pod $podUuid because spawned entity cleanup failed", cleanup) }
  private def requireRuntimeDriftOnly(podUuid: UUID, cached: FloatingPodState, candidate: FloatingPodState): Unit =
    if candidate.ownerUuid != cached.ownerUuid || candidate.stateVersion != cached.stateVersion || candidate.worldName != cached.worldName || candidate.isActive != cached.isActive || candidate.lastUpdated != cached.lastUpdated || candidate.dimensionId != cached.dimensionId then throw new IllegalArgumentException(s"Cached runtime update for pod $podUuid changes critical state")
  private def chunkCoordinate(position: Double): Int = floor(position / ChunkLoadingManager.ChunkSize).toInt
  private def recoveryChunks(dimensionId: Int, posX: Double, posZ: Double): Set[PhysicalRecoveryChunk] =
    val centerX = chunkCoordinate(posX)
    val centerZ = chunkCoordinate(posZ)
    (for
      chunkX <- centerX - 2 to centerX + 2
      chunkZ <- centerZ - 2 to centerZ + 2
    yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

object ActiveFloatingPodManager:
  def forForge(repository: FloatingPodRepository, modInstance: Object): ActiveFloatingPodManager = new ActiveFloatingPodManager(repository, ChunkLoadingManager.forForge(repository, modInstance))
