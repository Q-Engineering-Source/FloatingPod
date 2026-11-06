package com.wi.floatingpod.server

import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.mixin.{EntityTrackerRemovalViewAccessor, WorldRemovalViewAccessor}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.mysql.MySqlNormalRecallRemovalFacts
import net.minecraft.entity.Entity
import net.minecraft.world.WorldServer
import net.minecraftforge.common.DimensionManager
import net.minecraftforge.fml.common.gameevent.TickEvent

import java.util.UUID
import scala.jdk.CollectionConverters.*

/** 身份引用采用 eq；相同 UUID/ID 的替换本体仍是 foreign。 */
private[server] final case class RemovalEntityView(body: AnyRef, world: AnyRef, entityId: Int, podUuid: UUID)
private[server] final case class RemovalIndexSlot(source: String, entity: RemovalEntityView, idKey: Option[Int] = None, uuidKey: Option[UUID] = None)
private[server] final case class RemovalWorldSnapshot(dimensionId: Int, registeredWorlds: Vector[AnyRef], exactBodyNow: RemovalEntityView, slots: Vector[RemovalIndexSlot])

/** 此 seam 传递真实索引中的实体引用与键，不能以预先判定的 Boolean 代替观察。 */
private[server] trait RemovalWorldView:
  def worldRef: AnyRef
  def dimensionId: Int
  def assertMain(): Unit
  def describeBody(body: AnyRef): RemovalEntityView
  def snapshot(body: AnyRef, entityId: Int, podUuid: UUID): RemovalWorldSnapshot
  def requestRemove(body: AnyRef): Unit

final case class ExactRemovalIdentity private[server] (
    body: AnyRef,
    world: AnyRef,
    dimensionId: Int,
    entityId: Int,
    podUuid: UUID,
    operationUuid: UUID,
    bootUuid: UUID,
    attemptUuid: UUID
)

sealed trait ExactRemovalStatus
object ExactRemovalStatus:
  final case class Pending(identity: ExactRemovalIdentity, retainedSources: Vector[String]) extends ExactRemovalStatus
  final case class IdentityConflict(identity: ExactRemovalIdentity, conflictingSources: Vector[String]) extends ExactRemovalStatus
  final case class Acknowledged(identity: ExactRemovalIdentity, observedEndOrdinal: Long) extends ExactRemovalStatus

/** removeEntity 已请求只表示副作用尝试；义务在后续真实 END 观察确认前保留。 */
private[server] final class RemovalPending private[server] (
    val identity: ExactRemovalIdentity,
    val capturedEndOrdinal: Long,
    private[server] val observerOwner: AnyRef
):
  private[server] var issued = false
  private[server] var requestEndOrdinal = -1L
  private[server] var failure: Throwable = null
  private[server] var conflict: Vector[String] = Vector.empty
  private[server] var completionContext: Option[(IoOperationTag, Long)] = None
  private[server] var completion: Option[ExactBodyRemovalAcknowledgement] = None
  def requestIssued: Boolean = issued
  def requestFailure: Option[Throwable] = Option(failure)

/** 请求/观察共享状态机；不触碰 SQL、ticket、manager mappings 或客户端 ACK。 */
private[server] final class ExactBodyRemovalObserver(view: RemovalWorldView):
  require(view != null, "removal world view is required")

  def capture(body: AnyRef, operationUuid: UUID, bootUuid: UUID, attemptUuid: UUID, currentEndOrdinal: Long): RemovalPending =
    view.assertMain()
    require(body != null && operationUuid != null && bootUuid != null && attemptUuid != null, "exact removal identity is required")
    require(currentEndOrdinal >= 0L, "current END delivery ordinal must not be negative")
    val current = view.describeBody(body)
    require((current.body eq body) && (current.world eq view.worldRef) && current.podUuid != null, "body must belong to exact removal world")
    new RemovalPending(ExactRemovalIdentity(body, view.worldRef, view.dimensionId, current.entityId, current.podUuid, operationUuid, bootUuid, attemptUuid), currentEndOrdinal, this)

  /** 在副作用前绑定实际 retained IO 操作；旧的未绑定观察不能升级成 IO 凭证。 */
  def captureForIo(body: AnyRef, tag: IoOperationTag, attemptUuid: UUID,
      expectedStateVersion: Long, currentEndOrdinal: Long): RemovalPending =
    require(tag != null && expectedStateVersion >= 0L, "exact retained IO tag and state version required")
    val pending = capture(body, tag.operationId, tag.bootId, attemptUuid, currentEndOrdinal)
    pending.completionContext = Some(tag -> expectedStateVersion)
    pending

  /** 不接收外部 ACK DTO；每次从本 observer 的实际索引重新验证，保留首次凭证身份。 */
  def acknowledgeForIo(pending: RemovalPending, endOrdinal: Long): Option[ExactBodyRemovalAcknowledgement] =
    checkOwner(pending)
    val (tag, version) = pending.completionContext.getOrElse(
      throw new IllegalStateException("Removal was not captured for an exact IO operation"))
    observeEnd(pending, endOrdinal) match
      case ExactRemovalStatus.Acknowledged(_, observed) =>
        if pending.completion.isEmpty then
          val identity = pending.identity
          pending.completion = Some(ExactBodyRemovalAcknowledgement.issue(tag.bootId, tag.bootGeneration,
            tag.connectionGeneration, tag.operationId, identity.attemptUuid, identity.podUuid,
            version, identity.dimensionId, identity.entityId, observed))
        pending.completion
      case _ => None

  /** 只有原 observer 的原 pending/原 ACK 可转录三个实际 END ordinal。 */
  private[server] def normalRecallFacts(pending: RemovalPending,
      acknowledgement: ExactBodyRemovalAcknowledgement): MySqlNormalRecallRemovalFacts =
    checkOwner(pending)
    require(acknowledgement != null && pending.completion.exists(_ eq acknowledgement) &&
      pending.issued && pending.failure == null && pending.conflict.isEmpty,
      "normal recall requires the original successful removal acknowledgement")
    val (tag, version) = pending.completionContext.getOrElse(
      throw new IllegalStateException("normal recall removal lost original IO context"))
    val identity = pending.identity
    require(acknowledgement.bootId() == tag.bootId && acknowledgement.bootGeneration() == tag.bootGeneration &&
      acknowledgement.connectionGeneration() == tag.connectionGeneration && acknowledgement.operationId() == tag.operationId &&
      acknowledgement.podId() == identity.podUuid && acknowledgement.attemptId() == identity.attemptUuid &&
      acknowledgement.expectedStateVersion() == version && acknowledgement.dimensionId() == identity.dimensionId &&
      acknowledgement.entityId() == identity.entityId, "normal recall removal context differs")
    MySqlNormalRecallRemovalFacts(tag, identity.podUuid, identity.attemptUuid, version, identity.dimensionId,
      identity.entityId, pending.capturedEndOrdinal, pending.requestEndOrdinal, acknowledgement.observedEndOrdinal())

  /** 先核 foreign/identity，再至多调用一次；异常保留原 cause/stack，token 仍保留不确定义务。 */
  def request(pending: RemovalPending, currentEndOrdinal: Long): ExactRemovalStatus =
    checkOwner(pending)
    require(currentEndOrdinal >= pending.capturedEndOrdinal, "request END delivery ordinal predates capture")
    val observation = view.snapshot(pending.identity.body, pending.identity.entityId, pending.identity.podUuid)
    val conflicts = inspectConflicts(pending.identity, observation)
    if conflicts.nonEmpty then pending.conflict = (pending.conflict ++ conflicts).distinct
    if pending.conflict.nonEmpty then return ExactRemovalStatus.IdentityConflict(pending.identity, pending.conflict)
    if !pending.issued then
      pending.requestEndOrdinal = currentEndOrdinal
      pending.issued = true
      try view.requestRemove(pending.identity.body)
      catch
        case failure: Throwable =>
          pending.failure = failure
          throw failure
    ExactRemovalStatus.Pending(pending.identity, Vector("awaiting-subsequent-END"))

  /** ordinal 来自真实 END delivery clock（包含进行中的 END）；必须晚于实际 request，不能只晚于 capture。 */
  def observeEnd(pending: RemovalPending, endOrdinal: Long): ExactRemovalStatus =
    checkOwner(pending)
    require(endOrdinal >= 0L, "END ordinal must not be negative")
    val observation = view.snapshot(pending.identity.body, pending.identity.entityId, pending.identity.podUuid)
    val conflicts = inspectConflicts(pending.identity, observation)
    if conflicts.nonEmpty then pending.conflict = (pending.conflict ++ conflicts).distinct
    if pending.conflict.nonEmpty then return ExactRemovalStatus.IdentityConflict(pending.identity, pending.conflict)
    if !pending.issued then return ExactRemovalStatus.Pending(pending.identity, Vector("request-not-issued"))
    if endOrdinal <= pending.requestEndOrdinal then return ExactRemovalStatus.Pending(pending.identity, Vector("awaiting-subsequent-END"))
    val retained = observation.slots.collect { case slot if slot.entity.body eq pending.identity.body => slot.source }.distinct
    if retained.nonEmpty then ExactRemovalStatus.Pending(pending.identity, retained)
    else ExactRemovalStatus.Acknowledged(pending.identity, endOrdinal)

  private def checkOwner(pending: RemovalPending): Unit =
    view.assertMain()
    require(pending != null && (pending.observerOwner eq this), "pending removal belongs to another observer")

  private def inspectConflicts(identity: ExactRemovalIdentity, snapshot: RemovalWorldSnapshot): Vector[String] =
    val body = snapshot.exactBodyNow
    val identityChanged = !(body.body eq identity.body) || !(body.world eq identity.world) || body.entityId != identity.entityId || body.podUuid != identity.podUuid
    val worldUnavailable = snapshot.dimensionId != identity.dimensionId || !snapshot.registeredWorlds.exists(_ eq identity.world)
    val foreign = snapshot.slots.collect {
      case slot if (slot.entity.body ne identity.body) &&
          (slot.entity.entityId == identity.entityId || slot.entity.podUuid == identity.podUuid || slot.idKey.contains(identity.entityId) || slot.uuidKey.contains(identity.podUuid)) => slot.source
    }
    (Vector.empty[String] ++ (if identityChanged then Vector("captured-body-identity-changed") else Vector.empty) ++
      (if worldUnavailable then Vector("captured-world-unavailable") else Vector.empty) ++ foreign).distinct

/** manager 保留此 token，先 request，再由真实 ServerTick END 回调 poll；ACK 后清理仍归 manager。 */
final class PendingPodRemoval private[server] (
    val rawEntity: FloatingPodEntity,
    val rawWorld: WorldServer,
    private[server] val pending: RemovalPending
):
  def identity: ExactRemovalIdentity = pending.identity
  def requestIssued: Boolean = pending.requestIssued
  def requestFailure: Option[Throwable] = pending.requestFailure

final class ForgePodRemovalObserver(world: WorldServer):
  require(world != null, "exact WorldServer is required")
  private val observer = new ExactBodyRemovalObserver(new ForgeRemovalWorldView(world))

  /** capture 与 request 分离，调用方能在副作用或异常前保存 PendingRemoval 义务。 */
  def capture(entity: FloatingPodEntity, operationUuid: UUID, bootUuid: UUID, attemptUuid: UUID, currentEndOrdinal: Long): PendingPodRemoval =
    val pending = observer.capture(entity, operationUuid, bootUuid, attemptUuid, currentEndOrdinal)
    new PendingPodRemoval(entity, world, pending)

  def request(pending: PendingPodRemoval, currentEndOrdinal: Long): ExactRemovalStatus =
    require(pending != null && (pending.rawWorld eq world), "pending removal must belong to exact world")
    observer.request(pending.pending, currentEndOrdinal)

  def captureForIo(entity: FloatingPodEntity, tag: IoOperationTag, attemptUuid: UUID,
      expectedStateVersion: Long, currentEndOrdinal: Long): PendingPodRemoval =
    new PendingPodRemoval(entity, world, observer.captureForIo(entity, tag, attemptUuid,
      expectedStateVersion, currentEndOrdinal))

  def acknowledgeForIo(pending: PendingPodRemoval, phase: TickEvent.Phase,
      endOrdinal: Long): Option[ExactBodyRemovalAcknowledgement] =
    require(phase == TickEvent.Phase.END, "removal acknowledgement requires real server END phase")
    require(pending != null && (pending.rawWorld eq world), "pending removal must belong to exact world")
    observer.acknowledgeForIo(pending.pending, endOrdinal)

  private[server] def normalRecallFacts(pending: PendingPodRemoval,
      acknowledgement: ExactBodyRemovalAcknowledgement): MySqlNormalRecallRemovalFacts =
    require(pending != null && (pending.rawWorld eq world), "original normal recall world required")
    observer.normalRecallFacts(pending.pending, acknowledgement)

  def observeEnd(pending: PendingPodRemoval, phase: TickEvent.Phase, endOrdinal: Long): ExactRemovalStatus =
    require(phase == TickEvent.Phase.END, "removal observation requires real server END phase")
    require(pending != null && (pending.rawWorld eq world), "pending removal must belong to exact world")
    observer.observeEnd(pending.pending, endOrdinal)

/** 纯观察 loaded chunks；不 resolve/load/generate chunk，不 untrack 或强制补偿移除。 */
private final class ForgeRemovalWorldView(world: WorldServer) extends RemovalWorldView:
  override def worldRef: AnyRef = world
  override def dimensionId: Int = world.provider.getDimension
  override def assertMain(): Unit =
    val server = world.getMinecraftServer
    if server == null || !server.isCallingFromMinecraftThread then throw new IllegalStateException("Exact body removal requires Minecraft server Main thread")

  override def describeBody(body: AnyRef): RemovalEntityView = body match
    case entity: Entity => RemovalEntityView(entity, entity.world, entity.getEntityId, entity.getUniqueID)
    case _ => throw new IllegalArgumentException("Removal body is not a Minecraft entity")

  override def requestRemove(body: AnyRef): Unit = body match
    case entity: FloatingPodEntity if (entity.world eq world) => world.removeEntity(entity)
    case _ => throw new IllegalArgumentException("Removal request requires exact FloatingPodEntity in captured WorldServer")

  override def snapshot(body: AnyRef, entityId: Int, podUuid: UUID): RemovalWorldSnapshot =
    val loaded = world.loadedEntityList.asScala.toVector.map(entity => RemovalIndexSlot("world-loaded-list", describeBody(entity)))
    val unloaded = world.asInstanceOf[WorldRemovalViewAccessor].floatingpod$getUnloadedEntityList().asScala.toVector
      .map(entity => RemovalIndexSlot("world-unloaded-list", describeBody(entity)))
    val chunks = world.getChunkProvider.getLoadedChunks.asScala.toVector.flatMap { chunk =>
      chunk.getEntityLists.toVector.flatMap(_.asScala.toVector).map(entity => RemovalIndexSlot(s"chunk-list:${chunk.x}:${chunk.z}", describeBody(entity)))
    }
    val idLookup = Option(world.getEntityByID(entityId)).toVector.map(entity => RemovalIndexSlot("world-id-lookup", describeBody(entity), idKey = Some(entityId)))
    val uuidLookup = Option(world.getEntityFromUuid(podUuid)).toVector.map(entity => RemovalIndexSlot("world-uuid-lookup", describeBody(entity), uuidKey = Some(podUuid)))
    val tracker = world.getEntityTracker.asInstanceOf[EntityTrackerRemovalViewAccessor]
    val entries = tracker.floatingpod$getEntries().asScala.toVector.map(entry => RemovalIndexSlot("tracker-entry-set", describeBody(entry.getTrackedEntity)))
    // vanilla insertion/untrack 使用实体 ID；captured slot 加全 entries identity scan 覆盖正常退场。
    // 不宣称扫描 IntHashMap 任意 corrupt 错键，亦不以旧 entry.trackingPlayers 判定服务器 ACK。
    val trackerMap = Option(tracker.floatingpod$getTrackedEntityHashTable().lookup(entityId)).toVector
      .map(entry => RemovalIndexSlot("tracker-id-map", describeBody(entry.getTrackedEntity), idKey = Some(entityId)))
    RemovalWorldSnapshot(dimensionId, DimensionManager.getWorlds().toVector.map(world => world: AnyRef), describeBody(body), loaded ++ unloaded ++ chunks ++ idLookup ++ uuidLookup ++ entries ++ trackerMap)
