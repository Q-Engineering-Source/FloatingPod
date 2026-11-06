package com.wi.floatingpod.server

import com.wi.floatingpod.entity.{FloatingPodEntity,FloatingPodControlSnapshot}
import com.wi.floatingpod.storage.mysql.MySqlDbFirstRecallRequestRecord
import com.wi.floatingpod.debug.{DebugItemEndpoint, MainPodOperationRequest}
import com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.storage.mysql.{MySqlDbFirstCaptureReservation, MySqlDbFirstCheckpointConfirmation, MySqlDbFirstCheckpointFinalReceipt, MySqlDbFirstInitialActivationConfirmation, MySqlDbFirstInitialPublicationReceipt, MySqlDbFirstRecallConfirmation, MySqlDbFirstRecallDropReceipt}
import com.wi.floatingpod.storage.mysql.{MySqlNormalRecallSourceIdentity,MySqlDbFirstBootRecallTakeoverIntent}
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoIdentity, MySqlOwnedIoReply, MySqlCommittedUnboundGraph, MySqlControlledDebugCommittedGraph, MySqlUnboundCommitObligationNotice, MySqlOwnedModeCommitRequest, MySqlOwnedRepositoryOperationPlan, MySqlOwnedModeAlreadySetReadback, MySqlCommittedOwnedModeGraph, MySqlOwnedIoAdapter, MySqlOwnedIoCommand, MySqlTrackedDriftRequest, MySqlTrackedDriftMainRules, MySqlRetainedPlanDropReceipt, MySqlRetainedPlanHandle}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, UnboundCommitOutcome, UnboundIntentOrigin}
import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode,RecallRefusalReason}
import com.wi.floatingpod.entity.{FinalPodSavePayload, FloatingPodControlSnapshot, FloatingPodEntity, PodArtifactIdentity, PodArtifactNbtCodec, FloatingPodEntityRegistry}
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
import net.minecraft.nbt.{NBTBase, NBTTagCompound, NBTTagList}
import net.minecraft.entity.EntityList
import com.wi.floatingpod.mixin.{EntityTrackerRemovalViewAccessor, WorldRemovalViewAccessor}
import net.minecraftforge.common.DimensionManager
import net.minecraft.world.chunk.Chunk
import com.wi.floatingpod.storage.{AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore, UnboundPreparationReceipt}
import net.minecraft.world.{World, WorldServer}
import org.apache.logging.log4j.LogManager
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
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
  /** Adapters without independent physical-residence evidence fail closed and cannot authorize body deletion. */
  def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = false
  /** publication必须读取实际live body及UUID/entity-ID双索引；旧适配器不能猜测通过。 */
  def assertLiveIndexedBody(body: ControlledActivePodEntity): Unit =
    throw new UnsupportedOperationException("Actual live UUID/entity-ID body observation is unavailable")
  def spawnPod(state: FloatingPodState): ActivePodEntity
  /** Records the exact body as soon as this boot's spawn succeeds. */
  def spawnPod(state: FloatingPodState, retainCreatedBody: ActivePodEntity => Unit): ActivePodEntity =
    require(state != null && retainCreatedBody != null, "startup spawn state and receipt callback must not be null")
    val entity = spawnPod(state)
    retainCreatedBody(entity)
    entity
  def removePod(entity: ActivePodEntity): Unit

/** 仅原body的本地索引退场观察；不扫描其他chunk，不产生全域absence或磁盘证明。 */
private[server] final class DbFirstRecallRemovalView(world: PodWorldImpl, body: ActivePodEntityImpl) extends RemovalWorldView:
  private val owner=Thread.currentThread()
  private val raw=body.rawEntity
  private val originalChunkX=raw.chunkCoordX
  private val originalChunkZ=raw.chunkCoordZ
  private val originalChunk=Option(world.rawWorld.getChunkProvider.getLoadedChunk(originalChunkX,originalChunkZ))
    .getOrElse(throw new IllegalStateException("DB-first original body chunk is not loaded"))
  require(raw.addedToChunk && originalChunk.getEntityLists.exists(_.asScala.exists(_ eq raw)),
    "DB-first original body must belong to its actual loaded chunk")
  override def worldRef: AnyRef=world.rawWorld
  override def dimensionId: Int=world.dimensionId
  override def assertMain(): Unit =
    require(Thread.currentThread() eq owner,"DB-first local removal requires original Main")
    val server=world.rawWorld.getMinecraftServer
    require(server!=null && server.isCallingFromMinecraftThread,"DB-first local removal requires actual server Main")
  override def describeBody(value: AnyRef): RemovalEntityView=value match
    case entity: net.minecraft.entity.Entity=>RemovalEntityView(entity,entity.world,entity.getEntityId,entity.getUniqueID)
    case _=>throw new IllegalArgumentException("DB-first removal requires actual Entity identity")
  override def requestRemove(value: AnyRef): Unit =
    assertMain()
    require(value eq raw,"DB-first local removal must use the original body")
    world.removePod(body)
  override def snapshot(value: AnyRef,entityId: Int,pod: UUID): RemovalWorldSnapshot =
    assertMain()
    require(value eq raw,"DB-first local observation must use the original body")
    def relevant(entity: net.minecraft.entity.Entity): Boolean =
      (entity eq raw) || entity.getEntityId==entityId || entity.getUniqueID==pod
    def slots(source: String,entities: scala.collection.Iterable[net.minecraft.entity.Entity]) =
      entities.iterator.filter(relevant).map(e=>RemovalIndexSlot(source,describeBody(e))).toVector
    val loaded=slots("local-world-loaded",world.rawWorld.loadedEntityList.asScala)
    val unloaded=slots("local-world-unloaded",world.rawWorld.asInstanceOf[WorldRemovalViewAccessor]
      .floatingpod$getUnloadedEntityList().asScala)
    val chunk=slots("original-body-chunk",originalChunk.getEntityLists.toVector.flatMap(_.asScala))
    val id=Option(world.rawWorld.getEntityByID(entityId)).toVector.map(e=>RemovalIndexSlot("local-id",describeBody(e),idKey=Some(entityId)))
    val uuid=Option(world.rawWorld.getEntityFromUuid(pod)).toVector.map(e=>RemovalIndexSlot("local-uuid",describeBody(e),uuidKey=Some(pod)))
    val tracker=world.rawWorld.getEntityTracker.asInstanceOf[EntityTrackerRemovalViewAccessor]
    val tracked=tracker.floatingpod$getEntries().asScala.iterator.map(_.getTrackedEntity).filter(relevant)
      .map(e=>RemovalIndexSlot("local-tracker-entry",describeBody(e))).toVector
    val trackedId=Option(tracker.floatingpod$getTrackedEntityHashTable().lookup(entityId)).toVector
      .map(e=>RemovalIndexSlot("local-tracker-id",describeBody(e.getTrackedEntity),idKey=Some(entityId)))
    val registered=Option(DimensionManager.getWorld(dimensionId)).filter(_ eq world.rawWorld).toVector.map(w=>w:AnyRef)
    RemovalWorldSnapshot(dimensionId,registered,describeBody(raw),loaded++unloaded++chunk++id++uuid++tracked++trackedId)

/** 受控创建适配器确保 provenance 与冻结默认值在实体进入世界索引前写入。 */
trait ControlledPodWorld extends PodWorld:
  def spawnControlledPod(
      state: FloatingPodState,
      receipt: UnboundPreparationReceipt,
      retainBeforePublication: ControlledActivePodEntity => Unit
  ): ControlledActivePodEntity

/** 真实 Forge 服务端世界适配器；客户端或非 WorldServer 绝不能进入生命周期路径。 */
final class PodWorldImpl private (val rawWorld: WorldServer) extends ControlledPodWorld:
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
  override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
    case forge: ActivePodEntityImpl =>
      val entity = forge.rawEntity
      val resident = (entity.world eq rawWorld) && !entity.isDead && rawWorld.loadedEntityList.asScala.exists(_ eq entity)
      val uuidIndexed = rawWorld.getEntityFromUuid(entity.getUniqueID)
      val idIndexed = rawWorld.getEntityByID(entity.getEntityId)
      resident && (uuidIndexed == null || (uuidIndexed eq entity)) && (idIndexed == null || (idIndexed eq entity))
    case _ => false
  override def assertLiveIndexedBody(body: ControlledActivePodEntity): Unit = body match
    case forge: ActivePodEntityImpl =>
      val entity = forge.rawEntity
      require(!entity.isDead && (entity.world eq rawWorld) && entity.getUniqueID == body.podUuid &&
        (rawWorld.getEntityFromUuid(body.podUuid) eq entity) &&
        (rawWorld.getEntityByID(entity.getEntityId) eq entity),
        "Publication body is dead, foreign-world or differs from actual UUID/entity-ID indexes")
    case _ => throw new IllegalArgumentException("Forge publication received a non-Forge controlled body")
  override def spawnPod(state: FloatingPodState): ActivePodEntity =
    spawnPod(state, _ => ())
  override def spawnPod(state: FloatingPodState, retainCreatedBody: ActivePodEntity => Unit): ActivePodEntity =
    require(state != null, "state must not be null")
    require(retainCreatedBody != null, "retainCreatedBody must not be null")
    findPod(state.uuid).foreach(_ => throw new IllegalStateException(s"World $diagnosticWorldName already contains FloatingPod ${state.uuid}"))
    val entity = new FloatingPodEntity(rawWorld)
    entity.setUniqueId(state.uuid)
    entity.setLocationAndAngles(state.posX, state.posY, state.posZ, state.yaw, state.pitch)
    if !rawWorld.spawnEntity(entity) then throw new IllegalStateException(s"World $diagnosticWorldName refused to spawn FloatingPod ${state.uuid}")
    val created = new ActivePodEntityImpl(entity)
    retainCreatedBody(created)
    findPod(state.uuid) match
      case Some(found: ActivePodEntityImpl) if (found.rawEntity eq entity) && (rawWorld.getEntityByID(entity.getEntityId) eq entity) =>
        found
      case Some(_) => throw new IllegalStateException(s"World $diagnosticWorldName returned a different entity for spawned FloatingPod ${state.uuid}")
      case None => throw new IllegalStateException(s"World $diagnosticWorldName did not index spawned FloatingPod ${state.uuid}")
  override def spawnControlledPod(
      state: FloatingPodState,
      receipt: UnboundPreparationReceipt,
      retainBeforePublication: ControlledActivePodEntity => Unit
  ): ControlledActivePodEntity =
    require(state != null && receipt != null && retainBeforePublication != null, "controlled spawn values and retention callback must not be null")
    if state.uuid != receipt.podUuid || receipt.origin != com.wi.floatingpod.storage.UnboundIntentOrigin.DebugItemCreate || receipt.createOperationUuid.isEmpty
      || receipt.binding.cutoverState != com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified then
      throw new IllegalArgumentException("Controlled spawn must use the certified preparation receipt UUID")
    if dimensionId != receipt.targetPose.dimensionId || diagnosticWorldName != receipt.targetPose.worldName then
      throw new IllegalArgumentException("Controlled spawn world does not match the exact prepared pose")
    findPod(state.uuid).foreach(_ => throw new IllegalStateException(s"World $diagnosticWorldName already contains FloatingPod ${state.uuid}"))
    val entity = new FloatingPodEntity(rawWorld)
    entity.setUniqueId(state.uuid)
    entity.setLocationAndAngles(receipt.targetPose.posX, receipt.targetPose.posY, receipt.targetPose.posZ, receipt.targetPose.yaw, receipt.targetPose.pitch)
    entity.installArtifactIdentity(new PodArtifactIdentity(
      receipt.binding.bindingUuid,
      receipt.attemptUuid,
      PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE
    ))
    entity.initializeControlledCreation(FloatingPodEntityControl.pending(state, receipt))
    retainBeforePublication(new ActivePodEntityImpl(entity))
    if !rawWorld.spawnEntity(entity) then throw new IllegalStateException(s"World $diagnosticWorldName refused to spawn FloatingPod ${state.uuid}")
    findPod(state.uuid) match
      case Some(found: ActivePodEntityImpl) if found.rawEntity eq entity => found
      case Some(_) => throw new IllegalStateException(s"World $diagnosticWorldName returned a different entity for controlled FloatingPod ${state.uuid}")
      case None => throw new IllegalStateException(s"World $diagnosticWorldName did not index controlled FloatingPod ${state.uuid}")
  override def removePod(entity: ActivePodEntity): Unit = entity match
    case forgeEntity: ActivePodEntityImpl => rawWorld.removeEntity(forgeEntity.rawEntity)
    case _ => throw new IllegalArgumentException("Forge world received a non-Forge FloatingPod entity")

  /** 数据库完整NBT在世界发布前还原；新体保持默认冻结，manager复核后才装committed控制态。 */
  private[server] def spawnDbFirstRecoveryPod(state: FloatingPodState, bindingUuid: UUID, attempt: UUID, nbt: NBTTagCompound,
      retainBeforePublication: ActivePodEntity => Unit): ControlledActivePodEntity =
    require(state != null && bindingUuid != null && attempt != null && nbt != null && retainBeforePublication != null,
      "complete database recovery inputs required")
    require(rawWorld.getMinecraftServer.isCallingFromMinecraftThread && state.isActive &&
      state.dimensionId.contains(dimensionId) && state.worldName == diagnosticWorldName,
      "database recovery requires actual Main and exact committed world")
    require(findPod(state.uuid).isEmpty, "database recovery cannot attach or replace a world-record body")
    val entity = new FloatingPodEntity(rawWorld)
    entity.setUniqueId(state.uuid)
    val body = new ActivePodEntityImpl(entity)
    retainBeforePublication(body)
    try
      require(nbt.hasKey("id", 8) && Option(EntityList.getKey(entity)).exists(_.toString == nbt.getString("id")),
        "database snapshot does not name the registered FloatingPod entity")
      entity.deserializeDbFirstRecovery(nbt)
      require(entity.getUniqueID == state.uuid && entity.dimension == dimensionId &&
        entity.artifactIdentity().isPresent && entity.artifactIdentity().get().bindingUuid() == bindingUuid &&
        entity.artifactIdentity().get().attemptUuid() == attempt &&
        FloatingPodEntityControl.sameActualPose(body.actualPose, state) && body.isFrozen && !entity.isDead,
        "restored NBT conflicts with current database binding/attempt/pose or frozen body")
      require(findPod(state.uuid).isEmpty, "world body appeared during database NBT recovery")
      if !rawWorld.spawnEntity(entity) then throw new IllegalStateException(s"World refused restored FloatingPod ${state.uuid}")
      assertLiveIndexedBody(body)
      body
    catch case cause: Throwable =>
      try entity.markControlFaulted(cause)
      catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
      throw cause

object PodWorldImpl:
  def apply(world: World): PodWorldImpl =
    require(world != null, "world must not be null")
    world match
      case server: WorldServer => new PodWorldImpl(server)
      case _ => throw new IllegalArgumentException("FloatingPod lifecycle requires a server WorldServer")

/** 漂移ACK只改变已提交版本/姿态；保留原受控创建stamp，不借mode路径改身份。 */
private[server] trait TrackedDriftActivePodEntity extends ControlledActivePodEntity:
  def applyCommittedTrackedDrift(expectedBase: FloatingPodState, state: FloatingPodState, attemptUuid: UUID): Unit

private[server] trait NormalStopActivePodEntity extends ControlledActivePodEntity:
  def freezeForNormalStop(request: UUID): Unit
  def requireNormalStopFreeze(request: UUID, base: FloatingPodState, attempt: UUID): Unit

/** 真实实体的句柄，直接保留 vanilla UUID，避免从数值 entity ID 推导身份。 */
final class ActivePodEntityImpl private[server] (val rawEntity: FloatingPodEntity) extends TrackedDriftActivePodEntity, NormalStopActivePodEntity:
  override def entityId: Int = rawEntity.getEntityId
  override def podUuid: UUID = rawEntity.getUniqueID
  override def actualPose: FloatingPodPose = FloatingPodPose(rawEntity.world.getWorldInfo.getWorldName, rawEntity.posX, rawEntity.posY, rawEntity.posZ, rawEntity.rotationYaw, rawEntity.rotationPitch, rawEntity.world.provider.getDimension)
  override def isFrozen: Boolean = rawEntity.isFrozen
  override def freezeForNormalStop(request: UUID): Unit = rawEntity.freezeForNormalStop(request)
  override def requireNormalStopFreeze(request: UUID, base: FloatingPodState, attempt: UUID): Unit =
    rawEntity.requireNormalStopFreeze(request)
    val control = rawEntity.serverControlSnapshot()
    require(control.podUuid() == base.uuid && control.ownerUuid() == base.ownerUuid.orNull &&
      control.operatingMode() == base.operatingMode.toString && control.stateVersion() == base.stateVersion &&
      control.attemptUuid() == attempt && control.active() == base.isActive &&
      control.worldName() == base.worldName && base.dimensionId.contains(control.dimensionId()),
      "normal stop original body control differs from settled manager base")
  override def sameBody(other: ControlledActivePodEntity): Boolean = other match
    case forge: ActivePodEntityImpl => rawEntity.eq(forge.rawEntity)
    case _ => false
  override def hasControlledCreationStamp(receipt: UnboundPreparationReceipt): Boolean =
    receipt != null && rawEntity.matchesControlledCreation(receipt.binding.bindingUuid, receipt.attemptUuid, receipt.createOperationUuid.get)
  override def applyCommittedControl(state: FloatingPodState, receipt: UnboundPreparationReceipt): Unit =
    require(state != null && receipt != null, "committed control values must not be null")
    require(state.uuid == receipt.podUuid && state.ownerUuid.isEmpty && state.operatingMode == PodOperatingMode.WorldIdle && state.isActive, "controlled unbound creation must commit as active ownerless WorldIdle")
    require(hasControlledCreationStamp(receipt), "entity controlled-creation stamp does not match its preparation receipt")
    require(actualPose == receipt.targetPose, "entity pose changed after controlled creation preparation")
    rawEntity.applyCommittedControlledCreation(FloatingPodEntityControl.committed(state, Some(receipt.binding.bindingUuid), Some(receipt.attemptUuid), receipt.createOperationUuid))
  override def applyCommittedMode(state: FloatingPodState, attemptUuid: UUID): Unit =
    require(state != null && state.uuid == podUuid && state.isActive && state.dimensionId.contains(rawEntity.world.provider.getDimension), "committed entity mode state must match the active entity")
    val existing = rawEntity.serverControlSnapshot()
    if existing != null && existing.phase() == FloatingPodControlSnapshot.Phase.COMMITTED && existing.podUuid() == state.uuid
      && existing.ownerUuid() == state.ownerUuid.orNull && existing.operatingMode() == state.operatingMode.toString
      && existing.active() == state.isActive && existing.attemptUuid() == attemptUuid && existing.stateVersion() == state.stateVersion
      && existing.worldName() == state.worldName && existing.posX() == state.posX && existing.posY() == state.posY && existing.posZ() == state.posZ
      && existing.yaw() == state.yaw && existing.pitch() == state.pitch && existing.dimensionId() == state.dimensionId.get
      && rawEntity.hasCommittedSourceIdentity(existing) then ()
    else rawEntity.applyCommittedManagerState(FloatingPodEntityControl.committedMode(state, attemptUuid, existing))
  override def applyCommittedTrackedDrift(expectedBase: FloatingPodState, state: FloatingPodState, attemptUuid: UUID): Unit =
    require(expectedBase != null && state != null && attemptUuid != null && !rawEntity.isDead &&
      rawEntity.world.getMinecraftServer != null && rawEntity.world.getMinecraftServer.isCallingFromMinecraftThread,
      "drift control publication requires exact live server Main body")
    val existing = rawEntity.serverControlSnapshot()
    rawEntity.requireCommittedSourceIdentity()
    val installed = FloatingPodEntityControl.trackedDrift(expectedBase, state, attemptUuid, existing, actualPose)
    if existing.operationUuid() != null then
      require(rawEntity.matchesControlledCreation(existing.bindingUuid(), attemptUuid, existing.operationUuid()),
        "drift must preserve actual controlled creation stamp")
    rawEntity.applyCommittedManagerState(installed)
    require((rawEntity.serverControlSnapshot() eq installed) &&
      FloatingPodEntityControl.sameActualPose(actualPose, state) &&
      rawEntity.hasCommittedSourceIdentity(installed) &&
      (existing.operationUuid() == null || rawEntity.matchesControlledCreation(existing.bindingUuid(), attemptUuid, existing.operationUuid())),
      "actual entity drift control application did not preserve state/pose/original stamp")
  override def beginPendingMode(operation: UUID, owner: UUID, version: Long): Unit =
    rawEntity.beginPendingMode(operation, owner, version)
  override def hasPendingMode(operation: UUID, version: Long): Boolean = rawEntity.hasPendingMode(operation, version)
  override def finishPendingMode(operation: UUID, expectedVersion: Long, state: FloatingPodState, attemptUuid: UUID): Unit =
    val existing = rawEntity.serverControlSnapshot()
    rawEntity.finishPendingMode(operation, expectedVersion,
      FloatingPodEntityControl.committedMode(state, attemptUuid, existing))
  override def markControlFaulted(cause: Throwable): Unit = rawEntity.markControlFaulted(cause)

private[server] object FloatingPodEntityControl:
  /** 真实entity consumer使用的纯检查/构造；本身不授Main/livebody/DB权限或执行实体赋值。 */
  def sameActualPose(actual: FloatingPodPose, state: FloatingPodState): Boolean =
    actual != null && state != null && actual.worldName == state.worldName &&
      state.dimensionId.contains(actual.dimensionId) &&
      java.lang.Double.doubleToRawLongBits(actual.posX) == java.lang.Double.doubleToRawLongBits(state.posX) &&
      java.lang.Double.doubleToRawLongBits(actual.posY) == java.lang.Double.doubleToRawLongBits(state.posY) &&
      java.lang.Double.doubleToRawLongBits(actual.posZ) == java.lang.Double.doubleToRawLongBits(state.posZ) &&
      java.lang.Float.floatToRawIntBits(actual.yaw) == java.lang.Float.floatToRawIntBits(state.yaw) &&
      java.lang.Float.floatToRawIntBits(actual.pitch) == java.lang.Float.floatToRawIntBits(state.pitch)

  def trackedDrift(base: FloatingPodState, state: FloatingPodState, attempt: UUID,
      existing: FloatingPodControlSnapshot, actualPose: FloatingPodPose): FloatingPodControlSnapshot =
    require(base != null && state != null && attempt != null && existing != null &&
      existing.phase() == FloatingPodControlSnapshot.Phase.COMMITTED &&
      existing.podUuid() == base.uuid && existing.ownerUuid() == base.ownerUuid.orNull &&
      existing.operatingMode() == base.operatingMode.toString && existing.active() && base.isActive &&
      existing.attemptUuid() == attempt && existing.stateVersion() == base.stateVersion &&
      existing.worldName() == base.worldName && base.dimensionId.contains(existing.dimensionId()) &&
      state.uuid == base.uuid && state.ownerUuid == base.ownerUuid && state.operatingMode == base.operatingMode &&
      state.isActive && state.worldName == base.worldName && state.dimensionId == base.dimensionId &&
      state.stateVersion == Math.addExact(base.stateVersion, 1L) && sameActualPose(actualPose, state),
      "drift ACK conflicts with committed control identity/version or actual latest pose")
    require(existing.bindingUuid() != null || existing.operationUuid() == null,
      "a controlled creation operation must retain its binding")
    committed(state, Option(existing.bindingUuid()), Some(attempt), Option(existing.operationUuid()))

  /** mode后继保留原实体来源元组；此构造器本身不授予来源权威。 */
  def committedMode(state: FloatingPodState, attempt: UUID,
      existing: FloatingPodControlSnapshot): FloatingPodControlSnapshot =
    require(state != null && attempt != null && state.isActive && state.dimensionId.nonEmpty,
      "committed mode state must be active and dimensioned")
    if existing == null then committed(state, None, Some(attempt), None)
    else
      require(existing.phase() == FloatingPodControlSnapshot.Phase.COMMITTED && existing.podUuid() == state.uuid &&
        existing.attemptUuid() == attempt && (existing.bindingUuid() != null || existing.operationUuid() == null),
        "mode successor must preserve the original committed body source tuple")
      committed(state, Option(existing.bindingUuid()), Some(attempt), Option(existing.operationUuid()))

  def pending(state: FloatingPodState, receipt: UnboundPreparationReceipt): FloatingPodControlSnapshot =
    require(state != null && receipt != null, "pending entity control values must not be null")
    new FloatingPodControlSnapshot(
      FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION,
      state.uuid,
      null,
      PodOperatingMode.WorldIdle.toString,
      false,
      receipt.preparedVersion,
      receipt.targetPose.worldName,
      receipt.targetPose.posX,
      receipt.targetPose.posY,
      receipt.targetPose.posZ,
      receipt.targetPose.yaw,
      receipt.targetPose.pitch,
      receipt.targetPose.dimensionId,
      receipt.binding.bindingUuid,
      receipt.attemptUuid,
      receipt.createOperationUuid.get,
      null
    )
  def committed(state: FloatingPodState, bindingUuid: Option[UUID], attemptUuid: Option[UUID], operationUuid: Option[UUID]): FloatingPodControlSnapshot =
    require(state != null && state.dimensionId.nonEmpty && attemptUuid != null && attemptUuid.nonEmpty, "committed entity control values must be complete")
    new FloatingPodControlSnapshot(
      FloatingPodControlSnapshot.Phase.COMMITTED,
      state.uuid,
      state.ownerUuid.orNull,
      state.operatingMode.toString,
      state.isActive,
      state.stateVersion,
      state.worldName,
      state.posX,
      state.posY,
      state.posZ,
      state.yaw,
      state.pitch,
      state.dimensionId.get,
      bindingUuid.orNull,
      attemptUuid.get,
      operationUuid.orNull,
      null
    )

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

private[server] final case class StartupRecoveryManagerSnapshot(
    entity: ActivePodEntity,
    world: PodWorld,
    exactState: FloatingPodState,
    attemptUuid: UUID
)

/** Opaque, one-boot compensation authority. It is bound to the exact source body and manager snapshot. */
private[server] final class StartupRecoveryRollbackReceipt private[server] (
    private[server] val authority: AnyRef,
    val podUuid: UUID,
    val world: PodWorld,
    val exactState: FloatingPodState,
    private[server] val initialBody: Option[ActivePodEntity],
    private[server] val initialMapping: Option[StartupRecoveryManagerSnapshot],
    private[server] val initialCachedState: Option[FloatingPodState],
    private[server] val initialDirty: Boolean
):
  private[server] var body: Option[ActivePodEntity] = initialBody
  private[server] var attemptUuid: Option[UUID] = initialMapping.map(_.attemptUuid)
  private[server] var bootCreatedArtifactIdentity: Option[PodArtifactIdentity] = None
  private[server] var disposition: Option[RecoveryDisposition] = None
  private[server] var bootCreatedBody = false
  private[server] var managerMappingPublished = false
  private[server] var open = true

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
    finalFlushFailure: Option[Throwable],
    pendingAsyncDrift: Set[UUID] = Set.empty,
    unflushedDirty: Set[UUID] = Set.empty,
    asyncPreStopFinalizerRequired: Boolean = false
):
  require(entries != null && recalledUuids != null && remaining != null, "shutdown report collections must not be null")
  require(pendingTicketReconciliationFailure != null && finalFlushFailure != null, "shutdown report failures must not be null")
  require(pendingAsyncDrift != null && unflushedDirty != null, "actual async/dirty shutdown responsibility required")
  def isClean: Boolean = entries.forall(_.cause.isEmpty) && remaining.isEmpty && pendingTicketReconciliationFailure.isEmpty && finalFlushFailure.isEmpty && pendingAsyncDrift.isEmpty && unflushedDirty.isEmpty && !asyncPreStopFinalizerRequired

/** 供关服报告读取的阻塞收回快照，不泄漏可变实体记录。 */
sealed trait RecallRecoveryStatus
object RecallRecoveryStatus:
  case object PendingTicketRelease extends RecallRecoveryStatus
  case object PendingCommit extends RecallRecoveryStatus
  final case class Faulted(reason: DeactivationRejection) extends RecallRecoveryStatus

private final case class ManagedActivePod(entity: ActivePodEntity, world: PodWorld, exactState: FloatingPodState, attemptUuid: UUID, entityRemoved: Boolean = false):
  require(attemptUuid != null, "attemptUuid must not be null")
private[server] final case class ManagedActiveDetails(entity: ActivePodEntity, world: PodWorld, exactState: FloatingPodState, attemptUuid: UUID)
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
final class ActiveFloatingPodManager private (legacyRepository: Option[FloatingPodRepository], chunkLoadingManager: ChunkLoadingManager,
    clockMillis: () => Long, attemptUuidGenerator: () => UUID):
  def this(repository: FloatingPodRepository, chunkLoadingManager: ChunkLoadingManager,
      clockMillis: () => Long = () => System.currentTimeMillis(), attemptUuidGenerator: () => UUID = () => UUID.randomUUID()) =
    this(Some(repository), chunkLoadingManager, clockMillis, attemptUuidGenerator)
  private def repository: FloatingPodRepository = legacyRepository.getOrElse(
    throw new IllegalStateException("claimed Boot recovery has no legacy synchronous repository authority"))
  private def requireLegacyMutation(): Unit =
    if legacyRepository.isEmpty then throw new IllegalStateException("claimed Boot recovery cannot execute legacy mutation/recall")
  private val publicationMain = Thread.currentThread()
  private var mainPublicationScope = Option.empty[MainPublicationScope]
  private var mainBootReads = Option.empty[MainBootProjection]
  private var mainPodReads = Option.empty[MainPodReadAccess]
  private var dbFirstReadOwner = Option.empty[ProductionDbFirstBootstrapCallbacks]
  private[server] def requireDbFirstFinalMainReleased(original:ProductionDbFirstBootstrapCallbacks):Unit =
    require(Thread.currentThread() eq publicationMain,"original schema6 manager Main required")
    original.requireOriginalMainAdapter(original.adapter)
    require(mainPublicationScope.isEmpty && mainBootReads.isEmpty && mainPodReads.isEmpty && dbFirstReadOwner.isEmpty,
      "original schema6 manager publication/read graph remains owned")
  /** 正式读 consumer 一次安装；退休或失败后不静默回退旧 SQL。 */
  private[floatingpod] def installMainBootReads(projection: MainBootProjection): Unit =
    requirePublicationMain()
    require(projection != null && mainPodReads.isEmpty && mainBootReads.isEmpty && dbFirstReadOwner.isEmpty,
      "actual Main read owner is installed once")
    projection.requireCurrentReads()
    mainPublicationScope.foreach(scope => require(scope.identity == projection.identity &&
      projection.binding.contains(scope.binding) && projection.activeLimit == scope.activeLimit, "manager publication and Boot reads must share exact binding/session"))
    mainBootReads = Some(projection)
    mainPodReads = Some(projection)
  private[floatingpod] def installDbFirstMainReads(callbacks: ProductionDbFirstBootstrapCallbacks): Unit =
    requirePublicationMain()
    require(callbacks != null && mainPodReads.isEmpty && mainBootReads.isEmpty && dbFirstReadOwner.isEmpty &&
      mainPublicationScope.isEmpty, "original schema6 read owner must precede publication")
    callbacks.requireCurrentReads()
    callbacks.requireOriginalMainAdapter(callbacks.adapter)
    mainPodReads = Some(callbacks)
    dbFirstReadOwner = Some(callbacks)
  /** 原回收Entry清理不重新借DATA；即使epoch已变仍须核原Main/adapter引用。 */
  private def requireOriginalMainAdapter(adapter: MySqlOwnedIoAdapter): Unit =
    requirePublicationMain()
    dbFirstReadOwner match
      case Some(callbacks) => callbacks.requireOriginalMainAdapter(adapter)
      case None => mainBootReads.getOrElse(throw new IllegalStateException("original Main read owner missing"))
        .requireOriginalOwnedAdapter(adapter)
  private val logger = LogManager.getLogger(getClass)
  private val managedByPod = mutable.HashMap.empty[UUID, ManagedPodStatus]
  private val cachedStates = mutable.HashMap.empty[UUID, FloatingPodState]
  private val dirtyPods = mutable.HashSet.empty[UUID]
  private val dirtyRevisions = mutable.HashMap.empty[UUID, Long]
  private val startupRecoveryAuthority = new Object
  private val trackedDriftPendingTagsByPod = mutable.HashMap.empty[UUID, IoOperationTag]
  private val periodicFlushRequested = new AtomicBoolean(false)
  private val periodicFlushAdmissionStopped = new AtomicBoolean(false)

  /** scheduler只能合并请求Main捕获；不碰world/repository，不表示持久化成功。 */
  private[floatingpod] def requestPeriodicFlush(): Try[Int] = Try {
    require(!periodicFlushAdmissionStopped.get(), "Main drift producer admission has stopped")
    if periodicFlushRequested.compareAndSet(false, true) then 1 else 0
  }
  private[server] def consumePeriodicFlushRequest(): Boolean =
    requirePublicationMain()
    !periodicFlushAdmissionStopped.get() && periodicFlushRequested.getAndSet(false)
  private[server] def stopPeriodicFlushRequests(): Unit =
    requirePublicationMain()
    periodicFlushAdmissionStopped.set(true)
  private[server] def dirtyRevisionSnapshot: Map[UUID, Long] =
    requirePublicationMain()
    dirtyRevisions.toMap
  private val failedSpawnQuarantines = mutable.HashMap.empty[UUID, Throwable]

  /** 新异步路径只在manager实际构造Main安装一次；不授予Ready或SQL资格。 */
  private[floatingpod] def installMainPublication(identity: MySqlOwnedIoIdentity, runtimeGeneration: Long,
      binding: PhysicalRecoveryBinding, activeLimit: Int): MainPublicationScope =
    requirePublicationMain()
    require(identity != null && runtimeGeneration > 0L && binding != null &&
      binding.cutoverState == PhysicalRecoveryCutoverState.Certified && activeLimit >= 1 && activeLimit <= 256,
      "exact certified runtime publication identity/capacity required")
    mainBootReads.foreach(projection => require(projection.identity == identity && projection.binding.contains(binding) && projection.activeLimit == activeLimit,
      "publication must preserve installed Boot session/binding"))
    dbFirstReadOwner.foreach(_.requireMainPublication(identity,binding,activeLimit))
    require(mainPublicationScope.isEmpty, "manager publication runtime is one-shot")
    val scope = new MainPublicationScope(identity, runtimeGeneration, binding, activeLimit)
    mainPublicationScope = Some(scope)
    dbFirstReadOwner.foreach(_.retainMainPublication(this,scope))
    scope

  /** callback只留存本manager实际安装的同一scope，不接受替代publication。 */
  private[floatingpod] def requireDbFirstPublication(callbacks:ProductionDbFirstBootstrapCallbacks,
      scope:ActiveFloatingPodManager#MainPublicationScope):Unit =
    requirePublicationMain()
    require(dbFirstReadOwner.exists(_ eq callbacks) && mainPublicationScope.exists(_ eq scope),
      "original schema6 manager/callback/publication required")

  /** 原Boot失败亦须封停预装scope；manager字段覆盖安装中途失败的真实责任。 */
  private[server] def stopDbFirstBootPublication(original:ProductionDbFirstBootstrapCallbacks,
      recovery:DbFirstBootActiveRecovery,cause:Throwable):Unit =
    requirePublicationMain()
    require(original!=null && recovery!=null && cause!=null && dbFirstReadOwner.exists(_ eq original),
      "same original schema6 Boot publication failure required")
    original.requireOriginalMainAdapter(original.adapter)
    recovery.requireBootPublicationFailure(this,original)
    // 各步骤的次级失败不能阻止后续封停；原cause仍由runtime/callback保管。
    def attempt(action: => Unit):Unit =
      try action catch case cleanup:Throwable=>
        if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
    mainPublicationScope.foreach { scope=>
      attempt {
        if scope.retainedAsyncResponsibilityCount>0 then
          original.adapter.retainUnsettledStop(scope.stopResponsibilityCause(cause))
      }
      attempt { scope.retire() }
    }
    attempt { original.adapter.quiesce() }

  private def requirePublicationMain(): Unit =
    if Thread.currentThread() ne publicationMain then throw new IllegalStateException("Manager publication requires actual owner Main")

  /** Main本地retained body/ticket/session责任；这些引用从不进入owned worker command。 */
  private[floatingpod] final class MainPublicationScope private[ActiveFloatingPodManager] (
      initialIdentity: MySqlOwnedIoIdentity, val runtimeGeneration: Long,
      val binding: PhysicalRecoveryBinding, val activeLimit: Int):
    private var executionIdentity=initialIdentity
    def identity:MySqlOwnedIoIdentity=executionIdentity
    /** 保留同一scope/native图；只在原Boot真实控制帧且所有旧异步责任已排空时转执行代。 */
    private[floatingpod] def adoptRecoveredExecutionIdentity(callbacks:ProductionDbFirstBootstrapCallbacks,
        adapter:MySqlOwnedIoAdapter,next:MySqlOwnedIoIdentity,actualBinding:PhysicalRecoveryBinding,
        configuredLimit:Int):Unit =
      requireOwner()
      require(!retired && dbFirstReadOwner.exists(_ eq callbacks) && (callbacks.adapter eq adapter) &&
        next!=null && actualBinding==binding && configuredLimit==activeLimit &&
        next.bootId==executionIdentity.bootId && next.bootGeneration==executionIdentity.bootGeneration &&
        next.connectionGeneration>executionIdentity.connectionGeneration && normalStopRequest.isEmpty &&
        normalStopFailure.isEmpty && retainedAsyncResponsibilityCount==0,
        "same live manager scope/binding and fully drained old execution generation required")
      adapter.requireRecoveredPublicationDispatch(callbacks,this,next)
      executionIdentity=next
    private final class Pending(val tag: IoOperationTag, val receipt: UnboundPreparationReceipt,
        val request: MainPodOperationRequest, val session: MainControlledPublicationSession,
        val world: PodWorld, val body: ControlledActivePodEntity, val ticket: ChunkTicket):
      var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
      var projectionRelease = Option.empty[MainControlledProjectionReleaseBarrier]
      var initialReservation=Option.empty[MySqlDbFirstCaptureReservation]
      var initialConfirmation=Option.empty[MySqlDbFirstInitialActivationConfirmation]
      var initialPublication=Option.empty[MySqlDbFirstInitialPublicationReceipt]
      var initialReleaseSubmitted=false
      var status = MainControlledPublicationStatus.Retained
      var cause = Option.empty[Throwable]
      var view = MainControlledPodView(receipt.podUuid, None, PodOperatingMode.WorldIdle, false,
        receipt.preparedVersion, receipt.targetPose, DebugPodPhase.Ticketed, DebugItemEndpoint.Reason.INITIALIZATION_PENDING)
    private val pending = mutable.LinkedHashMap.empty[IoOperationTag, Pending]
    private var retired = false
    private var normalStopRequest = Option.empty[UUID]
    private var normalStopFailure = Option.empty[Throwable]
    private val normalStopBodies = mutable.LinkedHashMap.empty[UUID, NormalStopActivePodEntity]
    private val normalRecallFootprints = mutable.HashMap.empty[UUID, (UUID, UUID)]
    private var normalStopCensus = Option.empty[NormalStopCensus]
    private var originalRecallSource = Option.empty[MySqlNormalRecallSourceIdentity]

    /** 单 Pod 的新收回责任；快照不是提交许可，UNKNOWN/捕获失败都保留原实体和 ticket。 */
    private final class DbFirstRecallResources(val record: ManagedActivePod,val body: ActivePodEntityImpl,val ticket: ChunkTicket):
      var captured=Option.empty[DbFirstPodSnapshot]
      var observer=Option.empty[ExactBodyRemovalObserver]
      var removal=Option.empty[RemovalPending]
      var ticketRelease=Option.empty[PendingManagedTicketRelease]
      var removalAck=Option.empty[ExactBodyRemovalAcknowledgement]
      var ticketAck=Option.empty[TicketReleaseAcknowledgement]
    final class DbFirstRecallEntry private[MainPublicationScope] (
        originalReservation: MySqlDbFirstCaptureReservation, original: ManagedActivePod,
        originalBody: ActivePodEntityImpl, originalTicket: ChunkTicket, val finalPose: FloatingPodPose,
        private[MainPublicationScope] val stopSource:Option[NormalStopEntry],
        private[MainPublicationScope] val bootOwner:Option[DbFirstBootActiveRecovery]):
      private[MainPublicationScope] var reservationOwner:Option[MySqlDbFirstCaptureReservation]=Some(originalReservation)
      def reservation:MySqlDbFirstCaptureReservation=reservationOwner.getOrElse(
        throw new IllegalStateException("Boot Main capture reservation already detached"))
      val tag: IoOperationTag = originalReservation.tag
      val base: FloatingPodState = original.exactState
      val attemptUuid: UUID=original.attemptUuid
      private[MainPublicationScope] var resources: Option[DbFirstRecallResources]=Some(new DbFirstRecallResources(original,originalBody,originalTicket))
      private[MainPublicationScope] var confirmation=Option.empty[MySqlDbFirstRecallConfirmation]
      private[MainPublicationScope] var dropReceipt=Option.empty[MySqlDbFirstRecallDropReceipt]
      private[MainPublicationScope] var releaseSubmitted=false
      private[MainPublicationScope] var lastEnd= -1L
      private[MainPublicationScope] var retainedPhysicalFailure=Option.empty[Throwable]
      private[MainPublicationScope] var completedRemovalAck=Option.empty[ExactBodyRemovalAcknowledgement]
      private[MainPublicationScope] var completedTicketAck=Option.empty[TicketReleaseAcknowledgement]
      private[MainPublicationScope] var cause = Option.empty[Throwable]
      private def retained: DbFirstRecallResources=resources.getOrElse(throw new IllegalStateException("DB-first Main resources already detached"))
      private[MainPublicationScope] def record: ManagedActivePod=retained.record
      private[MainPublicationScope] def body: ActivePodEntityImpl=retained.body
      private[MainPublicationScope] def ticket: ChunkTicket=retained.ticket
      private[MainPublicationScope] def captured: Option[DbFirstPodSnapshot]=retained.captured
      private[MainPublicationScope] def captured_=(value: Option[DbFirstPodSnapshot]):Unit=retained.captured=value
      def snapshot: DbFirstPodSnapshot =
        requireOwner()
        cause.foreach(throw _)
        require(confirmation.isEmpty,"confirmed recall cannot lend its capture again")
        requireDbFirstRecallBody(this)
        captured.getOrElse(throw new IllegalStateException("DB-first recall snapshot capture is incomplete"))

    private val dbFirstRecalls = mutable.LinkedHashMap.empty[IoOperationTag, DbFirstRecallEntry]
    private def hasDbFirstRecall(pod: UUID): Boolean = dbFirstRecalls.valuesIterator.exists(_.base.uuid == pod)

    /** 原Boot在实际恢复完成后的一次观察；不freeze/capture，不伪称Ready或推导玩家维度。 */
    private[server] def observeBootRecallDecision(boot:DbFirstBootActiveRecovery,
        original:ProductionDbFirstBootstrapCallbacks,adapter:MySqlOwnedIoAdapter,
        intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,
        pump:ForgePermanentMainPump,lease:PermanentPumpLease,tick:PermanentEndTick):Option[RecallRefusalReason] =
      requireOwner()
      require(boot!=null && original!=null && intent!=null && reservation!=null &&
        dbFirstReadOwner.exists(_ eq original),"actual Boot original manager decision owner required")
      boot.requireBootDecisionObservation(this,original,intent,reservation,pump,lease,tick)
      currentBootRecallDecision(original,adapter,intent,reservation)

    private def currentBootRecallDecision(original:ProductionDbFirstBootstrapCallbacks,adapter:MySqlOwnedIoAdapter,
        intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Option[RecallRefusalReason] =
      requireOwner()
      requireOriginalMainAdapter(adapter)
      require(!retired && normalStopRequest.isEmpty,"retired/stopping Boot scope is not a business refusal")
      original.requireCurrentReads()
      val request=intent.original
      require(request.bindingUuid==binding.bindingUuid && reservation.tag==intent.executionTag,
        "original Boot Main decision binding/C identity differs")
      dbFirstPoseFailures.get(request.podUuid).foreach(value=>throw value.cause)
      val conflicting=trackedDriftPendingTagsByPod.contains(request.podUuid) || hasDbFirstRecall(request.podUuid) ||
        hasDbFirstCheckpoint(request.podUuid) || pending.valuesIterator.exists(_.receipt.podUuid==request.podUuid) ||
        pendingModes.valuesIterator.exists(_.sqlRequest.podUuid==request.podUuid)
      if conflicting then Some(RecallRefusalReason.Busy)
      else
        val current=original.findState(request.podUuid)
        current match
          case Some(state) if state.isActive=>
            val record=managedByPod.get(request.podUuid) match
              case Some(ManagedPodStatus.Active(value))=>value
              case _=>throw new IllegalStateException("active Boot DATA has no actual restored manager record")
            require(record.exactState==state,"actual Boot manager state differs from current complete DATA")
            val body=record.entity match
              case actual:ActivePodEntityImpl=>actual
              case _=>throw new IllegalStateException("Boot Main decision requires actual Forge body")
            record.world.assertLiveIndexedBody(body)
            val control=body.rawEntity.serverControlSnapshot()
            require(control!=null && control.bindingUuid()==binding.bindingUuid && control.attemptUuid()==record.attemptUuid &&
              control.podUuid()==request.podUuid && control.ownerUuid()==state.ownerUuid.orNull &&
              control.stateVersion()==state.stateVersion && control.operatingMode()==state.operatingMode.toString &&
              control.worldName()==state.worldName && state.dimensionId.contains(control.dimensionId()) &&
              control.active() && body.rawEntity.acceptsBusinessInteraction(),
              "Boot Main decision actual body/control/admission differs")
            val ticket=chunkLoadingManager.managedTicketRecords().getOrElse(request.podUuid,
              throw new IllegalStateException("active Boot DATA has no actual restored ticket")).ticket
            val pose=body.actualPose
            val chunks=chunkLoadingManager.publicationTicketChunks(request.podUuid,record.world,ticket)
            require(chunks.size==25 && chunks==recoveryChunks(pose.dimensionId,pose.posX,pose.posZ),
              "Boot Main decision requires original actual25 ticket")
          case _=>
            require(!managedByPod.contains(request.podUuid) && !chunkLoadingManager.managedTicketRecords().contains(request.podUuid),
              "inactive/missing Boot DATA still has retained manager/ticket responsibility")
        DbFirstPlayerRecallPolicy.originalStateRefusal(request.actorUuid,request.podUuid,request.expectedStateVersion,current,None)
          .map(DbFirstPlayerRecallPolicy.durableReason)

    private[server] def preflightBootRecallCapture(boot:DbFirstBootActiveRecovery,
        original:ProductionDbFirstBootstrapCallbacks,adapter:MySqlOwnedIoAdapter,
        intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit =
      requireOwner()
      boot.requireBootCaptureInvocation(this,intent,reservation,1)
      require(dbFirstReadOwner.exists(_ eq original) && currentBootRecallDecision(original,adapter,intent,reservation).isEmpty,
        "current actual Boot capture body/owner/version/conflicts must still permit positive C")

    private[server] def beginBootDbFirstRecall(boot:DbFirstBootActiveRecovery,
        original:ProductionDbFirstBootstrapCallbacks,adapter:MySqlOwnedIoAdapter,
        intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):DbFirstRecallEntry =
      requireOwner()
      boot.requireBootCaptureInvocation(this,intent,reservation,3)
      require(dbFirstReadOwner.exists(_ eq original) && currentBootRecallDecision(original,adapter,intent,reservation).isEmpty,
        "actual Boot capture body must be rechecked immediately before freeze")
      beginDbFirstRecallOwned(adapter,reservation,None,intent.original.podUuid,intent.original.expectedStateVersion,None,
        Some((boot,intent.original)))

    /** caller 先占原 IO operation；这里只准入实际 Main/body，不发送 SQL 或报告收回成功。 */
    private[server] def beginDbFirstRecall(adapter: MySqlOwnedIoAdapter,
        reservation: MySqlDbFirstCaptureReservation, authenticatedOwner: UUID,
        pod: UUID, expectedVersion: Long): DbFirstRecallEntry =
      beginDbFirstRecallOwned(adapter,reservation,Option(authenticatedOwner),pod,expectedVersion,None)

    private def beginDbFirstRecallForNormalStop(adapter:MySqlOwnedIoAdapter,
        reservation:MySqlDbFirstCaptureReservation,source:NormalStopEntry):DbFirstRecallEntry =
      requireOwner()
      require(dbFirstReadOwner.nonEmpty && dbFirstStopCensus.exists(_.entries.exists(_ eq source)) &&
        !source.retainedResident && normalStopRequest.contains(dbFirstStopCensus.get.request),
        "original schema6 Normal stop census entry required")
      source.body.requireNormalStopFreeze(normalStopRequest.get,source.base,source.attemptUuid)
      beginDbFirstRecallOwned(adapter,reservation,None,source.podUuid,source.base.stateVersion,Some(source))

    /** 玩家与停服分别提供真实资格；只共享原Main capture/确认/退场机制。 */
    private def beginDbFirstRecallOwned(adapter:MySqlOwnedIoAdapter,
        reservation:MySqlDbFirstCaptureReservation,authenticatedOwner:Option[UUID],
        pod:UUID,expectedVersion:Long,stopSource:Option[NormalStopEntry],
        bootSource:Option[(DbFirstBootActiveRecovery,MySqlDbFirstRecallRequestRecord)]=None):DbFirstRecallEntry =
      requireOwner()
      require(adapter != null && reservation != null && reservation.purpose == DbFirstPodSnapshot.Purpose.RECALL,
        "original recall capture adapter/reservation required")
      requireOriginalMainAdapter(adapter)
      adapter.requireDbFirstCaptureReservation(reservation)
      val tag = reservation.tag
      identity.requireTag(tag)
      require(!retired && pod!=null && (if bootSource.nonEmpty then
        stopSource.isEmpty && authenticatedOwner.isEmpty && normalStopRequest.isEmpty
      else stopSource match
        case None=>normalStopRequest.isEmpty && authenticatedOwner.nonEmpty
        case Some(source)=>authenticatedOwner.isEmpty && normalStopRequest.nonEmpty &&
          dbFirstStopCensus.exists(_.entries.exists(_ eq source)) && !source.retainedResident),
        "DB-first recall requires original player or sealed Normal stop owner")
      assertCriticalTransitionAllowed(pod)
      require(!dbFirstRecalls.contains(tag) && !pending.contains(tag) && !pendingModes.contains(tag) &&
        !pendingDrifts.contains(tag) && !pending.valuesIterator.exists(_.receipt.podUuid == pod) &&
        !pendingModes.valuesIterator.exists(_.sqlRequest.podUuid == pod) && dbFirstRecalls.size < activeLimit,
        "DB-first recall conflicts with retained original operation")
      val record = managedByPod.get(pod) match
        case Some(ManagedPodStatus.Active(value)) => value
        case _ => throw new IllegalStateException("DB-first recall requires original active manager record")
      require(record.exactState.isActive && record.exactState.stateVersion==expectedVersion && (if bootSource.nonEmpty then
        record.exactState.ownerUuid.contains(bootSource.get._2.actorUuid)
      else stopSource match
        case None=>record.exactState.ownerUuid==authenticatedOwner
        case Some(source)=>(source.record eq record) && record.exactState.operatingMode==PodOperatingMode.Normal &&
          record.exactState.ownerUuid.nonEmpty && source.finalPose==source.body.actualPose),
        "DB-first recall owner/version or original Normal stop record differs")
      val body = record.entity match
        case actual: ActivePodEntityImpl => actual
        case _ => throw new IllegalStateException("DB-first recall requires actual Forge snapshot owner")
      record.world.assertLiveIndexedBody(body)
      val control = body.rawEntity.serverControlSnapshot()
      require(control != null && control.bindingUuid() == binding.bindingUuid &&
        control.attemptUuid() == record.attemptUuid && control.podUuid() == pod &&
        control.ownerUuid() == record.exactState.ownerUuid.orNull && control.stateVersion() == expectedVersion &&
        control.operatingMode() == record.exactState.operatingMode.toString &&
        control.worldName() == record.exactState.worldName &&
        record.exactState.dimensionId.contains(control.dimensionId()) && control.active() &&
        (stopSource.nonEmpty || body.rawEntity.acceptsBusinessInteraction()),
        "DB-first recall control binding/attempt/admission differs")
      stopSource.foreach { source=>
        require(source.body.sameBody(body) && source.world.sameWorld(record.world),"Normal stop original body/world changed")
        body.rawEntity.requireNormalStopFreeze(normalStopRequest.get)
      }
      val ticket = chunkLoadingManager.managedTicketRecords().getOrElse(pod,
        throw new IllegalStateException("DB-first recall original ticket missing")).ticket
      val pose = body.actualPose
      val chunks = chunkLoadingManager.publicationTicketChunks(pod, record.world, ticket)
      require(chunks.size == 25 && chunks == recoveryChunks(pose.dimensionId, pose.posX, pose.posZ),
        "DB-first recall requires original actual25 ticket")
      stopSource.foreach(source=>require((source.ticket eq ticket) && source.finalPose==pose,
        "Normal stop original ticket/pose changed before recall capture"))
      val entry = new DbFirstRecallEntry(reservation, record, body, ticket, pose,stopSource,bootSource.map(_._1))
      dbFirstRecalls.put(tag, entry) // 所有权先于冻结/序列化；失败仍保留原 operation。
      try
        bootSource.foreach((boot,_)=>boot.registerBootRecallEntry(this,reservation))
        if bootSource.nonEmpty then body.rawEntity.beginDbFirstRecallForBoot(tag.operationId,expectedVersion,bootSource.get._1)
        else stopSource match
          case Some(_)=>body.rawEntity.beginDbFirstRecallForNormalStop(tag.operationId,normalStopRequest.get,expectedVersion)
          case None=>body.rawEntity.beginDbFirstRecall(tag.operationId,authenticatedOwner.get,expectedVersion)
        requireDbFirstRecallBody(entry)
        entry.captured = Some(adapter.captureDbFirstSnapshot(reservation) { allocation =>
          body.rawEntity.captureDbFirstRecall(tag.operationId, expectedVersion, allocation)
        })
        requireDbFirstRecallBody(entry)
        entry
      catch
        case cause: Throwable =>
          dbFirstMainFailed(entry,cause)
          throw cause

    private[server] def requireBootRecallFreeze(boot:DbFirstBootActiveRecovery,entity:FloatingPodEntity,
        tag:IoOperationTag,version:Long,control:FloatingPodControlSnapshot):Unit =
      requireOwner()
      val entry=dbFirstRecalls.getOrElse(tag,throw new IllegalStateException("actual Boot manager Entry missing before freeze"))
      require(entry.bootOwner.exists(_ eq boot) && entry.resources.nonEmpty && entry.confirmation.isEmpty &&
        entry.base.stateVersion==version && (entry.body.rawEntity eq entity) &&
        (entity.serverControlSnapshot() eq control) && entry.base.ownerUuid.contains(control.ownerUuid()) &&
        entry.base.uuid==control.podUuid() && entry.attemptUuid==control.attemptUuid() && control.bindingUuid()==binding.bindingUuid,
        "same registered Boot manager Entry/body/control required before entity freeze")

    private def requireRecallEnd(entry:DbFirstRecallEntry,tick:PermanentEndTick):Unit =
      require(tick!=null,"actual recall END required")
      entry.bootOwner match
        case Some(boot)=>boot.requireBootPositiveEnd(this,entry.tag,tick)
        case None=>require(tick.phase==PermanentPumpPhase.Ready,"ordinary recall requires Ready END")

    private[server] def requireBootRecallMainDetached(boot:DbFirstBootActiveRecovery,tag:IoOperationTag):Unit =
      requireOwner()
      val entry=dbFirstRecalls.getOrElse(tag,throw new IllegalStateException("actual Boot detached Main Entry missing"))
      require(entry.bootOwner.exists(_ eq boot) && entry.resources.isEmpty && entry.confirmation.isEmpty &&
        entry.reservationOwner.isEmpty && entry.dropReceipt.nonEmpty && !entry.releaseSubmitted,
        "actual Boot Main resource/capture graph must detach before final worker submission")

    /** Boot先交drop并撤源图；后续END才走普通C combined successor/final release。 */
    private[server] def prepareBootDbFirstRecallDrop(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry,
        tick:PermanentEndTick):Option[MySqlDbFirstRecallDropReceipt] =
      requireDbFirstEntry(adapter,entry);requireRecallEnd(entry,tick)
      require(entry.bootOwner.nonEmpty && tick.endOrdinal>entry.lastEnd && !entry.releaseSubmitted &&
        entry.confirmation.exists(c=> !c.isRetained),"actual subsequent Boot normal C cleanup END required")
      entry.cause.foreach(throw _);entry.lastEnd=tick.endOrdinal
      try
        if entry.resources.nonEmpty then progressDbFirstResources(entry,tick.endOrdinal)
        if entry.resources.nonEmpty then None
        else
          if entry.dropReceipt.isEmpty then entry.dropReceipt=Some(adapter.recordDbFirstMainDrop(entry.confirmation.get))
          entry.confirmation=None;entry.reservationOwner=None
          entry.dropReceipt
      catch case cause:Throwable=>
        dbFirstMainFailed(entry,cause)
        throw cause

    private[server] def submitBootDbFirstRecallRelease(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry,
        tick:PermanentEndTick):IoSubmission =
      requireDbFirstEntry(adapter,entry);requireRecallEnd(entry,tick)
      val boot=entry.bootOwner.getOrElse(throw new IllegalStateException("actual Boot release owner missing"))
      boot.requireBootPositiveRelease(this,entry.tag,tick)
      requireBootRecallMainDetached(boot,entry.tag)
      require(tick.endOrdinal>entry.lastEnd && entry.dropReceipt.exists(receipt=> !receipt.isRetained),
        "Boot combined final release requires later END after graph detach")
      entry.cause.foreach(throw _);entry.lastEnd=tick.endOrdinal
      val result=adapter.submitDbFirstRecallRelease(entry.dropReceipt.get)
      if result==IoSubmission.Accepted then entry.releaseSubmitted=true
      result

    private def requireDbFirstRecallBody(entry: DbFirstRecallEntry): Unit =
      requireOwner()
      require(!retired && dbFirstRecalls.get(entry.tag).contains(entry) &&
        managedByPod.get(entry.base.uuid).contains(ManagedPodStatus.Active(entry.record)) &&
        entry.record.exactState == entry.base && entry.body.actualPose == entry.finalPose,
        "DB-first recall original record/body/pose changed")
      entry.record.world.assertLiveIndexedBody(entry.body)
      entry.confirmation.filter(_.isRetained) match
        case Some(confirmed)=>
          confirmed.requireOriginal(entry.reservation)
          val original=confirmed.retainedFailure.getOrElse(throw new IllegalStateException("retained confirmation lost original cause"))
          require(entry.cause.exists(_ eq original),"retained Main cause differs from original confirmation")
          entry.body.rawEntity.requireRetainedDbFirstRecall(entry.tag.operationId,entry.base.stateVersion,original)
        case None=>entry.body.rawEntity.requireDbFirstRecall(entry.tag.operationId, entry.base.stateVersion)
      val chunks = chunkLoadingManager.publicationTicketChunks(entry.base.uuid, entry.record.world, entry.ticket)
      require(chunks.size == 25 && chunks == recoveryChunks(entry.finalPose.dimensionId,
        entry.finalPose.posX, entry.finalPose.posZ), "DB-first recall original ticket changed")

    private def requireDbFirstEntry(adapter: MySqlOwnedIoAdapter,entry: DbFirstRecallEntry): Unit =
      requireOwner()
      require(adapter!=null && entry!=null && !retired && dbFirstRecalls.get(entry.tag).exists(_ eq entry),
        "original retained DB-first Main entry required")
      identity.requireTag(entry.tag)
      requireOriginalMainAdapter(adapter)
    private def dbFirstMainFailed(entry: DbFirstRecallEntry,cause: Throwable): Unit =
      entry.cause match
        case Some(primary)=>if primary ne cause then primary.addSuppressed(cause)
        case None=>
          entry.cause=Some(cause)
          logger.error("DB-first recall Main failed for pod "+entry.base.uuid+" operation "+entry.tag.operationId,cause)
      entry.resources.foreach { retained=>
        try retained.body.markControlFaulted(cause)
        catch case diagnostic: Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)
      }

    /** 只接受原adapter消费的实际DB提交确认；原失败由owner撤reply后继续报告，不授普通成功。 */
    private[server] def completeDbFirstRecall(adapter: MySqlOwnedIoAdapter,entry: DbFirstRecallEntry,
        completion: IoCompletion[MySqlOwnedIoReply]): Option[Throwable] =
      requireDbFirstEntry(adapter,entry)
      require(completion!=null && completion.tag==entry.tag && entry.confirmation.isEmpty && entry.resources.nonEmpty,
        "exact one-shot DB-first completion required")
      val retained=completion.result match
        case scala.util.Success(MySqlOwnedIoReply.DbFirstRecallRetained(_))=>true
        case _=>false
      if !retained then entry.cause.foreach(throw _)
      try
        val confirmed=if retained then adapter.claimRetainedDbFirstRecallConfirmation(entry.reservation,completion)
          else adapter.claimDbFirstRecallConfirmation(entry.reservation,completion)
        entry.confirmation=Some(confirmed) // 字段校验/清理失败仍保留实际确认责任。
        confirmed.requireOriginal(entry.reservation)
        if retained then
          require(confirmed.isRetained,"retained completion requires explicit restricted confirmation")
          val original=confirmed.retainedFailure.getOrElse(throw new IllegalStateException("retained confirmation lost original cause"))
          require(entry.cause.forall(_ eq original),"original retained Main cause cannot be replaced")
          dbFirstMainFailed(entry,original) // 同一freeze登记实际fault，不清cause或换operation。
        else require(!confirmed.isRetained,"ordinary completion cannot consume restricted confirmation")
        requireDbFirstRecallBody(entry)
        val captured=entry.captured.getOrElse(throw new IllegalStateException("original capture missing"))
        require(confirmed.tag==entry.tag && confirmed.bindingUuid==binding.bindingUuid && confirmed.podUuid==entry.base.uuid &&
          confirmed.ownerUuid==entry.base.ownerUuid && confirmed.attemptUuid==entry.attemptUuid &&
          confirmed.capturedStateVersion==entry.base.stateVersion && confirmed.committedStateVersion==Math.addExact(entry.base.stateVersion,1L) &&
          confirmed.pose==captured.pose() && confirmed.pose.worldName()==entry.finalPose.worldName &&
          confirmed.pose.dimension()==entry.finalPose.dimensionId && confirmed.pose.x()==entry.finalPose.posX &&
          confirmed.pose.y()==entry.finalPose.posY && confirmed.pose.z()==entry.finalPose.posZ &&
          confirmed.pose.yaw()==entry.finalPose.yaw && confirmed.pose.pitch()==entry.finalPose.pitch,
          "DB-first confirmation differs from original frozen capture")
        // 返回现有原cause容器，不新建资格；caller必须先撤多余reply，再沿同一cause结束失败。
        confirmed.retainedFailure
      catch
        case cause: Throwable=>
          dbFirstMainFailed(entry,cause)
          throw cause

    /** 真END推进原body局部退场；正常延后保持Pending，原remove只请求一次。 */
    private[server] def advanceDbFirstRecall(adapter: MySqlOwnedIoAdapter,entry: DbFirstRecallEntry,
        tick: PermanentEndTick): Option[IoSubmission] =
      requireDbFirstEntry(adapter,entry)
      require(tick!=null && tick.bootUuid==identity.bootId && tick.bootGeneration==identity.bootGeneration &&
        tick.phase==PermanentPumpPhase.Ready && entry.bootOwner.isEmpty && tick.endOrdinal>entry.lastEnd && entry.confirmation.nonEmpty && !entry.releaseSubmitted,
        "DB-first cleanup requires the original subsequent live Main END")
      entry.cause.foreach(throw _)
      entry.lastEnd=tick.endOrdinal
      try
        if entry.resources.nonEmpty then progressDbFirstResources(entry,tick.endOrdinal)
        if entry.resources.nonEmpty then None
        else
          if entry.dropReceipt.isEmpty then entry.dropReceipt=Some(adapter.recordDbFirstMainDrop(entry.confirmation.get))
          val submitted=adapter.submitDbFirstRecallRelease(entry.dropReceipt.get)
          submitted match
            case IoSubmission.Accepted=>entry.releaseSubmitted=true
            case _=>() // 只保留并重交原receipt；不重放实体/ticket或重mint tag。
          Some(submitted)
      catch
        case cause: Throwable=>
          dbFirstMainFailed(entry,cause)
          throw cause

    /** 专用原操作native推进；不发送成功或恢复普通producer，最终仍须原worker退休ACK。 */
    private[server] def advanceRetainedDbFirstRecall(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry,
        tick:PermanentEndTick):Option[MySqlDbFirstRecallDropReceipt] =
      requireDbFirstEntry(adapter,entry);requireRecallEnd(entry,tick)
      require(tick!=null && tick.bootUuid==identity.bootId && tick.bootGeneration==identity.bootGeneration &&
        tick.endOrdinal>entry.lastEnd && !entry.releaseSubmitted &&
        entry.confirmation.exists(_.isRetained),"retained cleanup requires original subsequent live Main END")
      entry.retainedPhysicalFailure.foreach(throw _)
      val confirmed=entry.confirmation.get
      require(entry.cause.nonEmpty && confirmed.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)),
        "retained cleanup must preserve exact original cause")
      entry.lastEnd=tick.endOrdinal
      try
        if entry.resources.nonEmpty then progressDbFirstResources(entry,tick.endOrdinal)
        if entry.resources.nonEmpty then None
        else
          require(entry.completedRemovalAck.nonEmpty && entry.completedTicketAck.exists(ticket=>
            entry.completedRemovalAck.exists(_ eq ticket.removal())),"actual original native ACKs required after resource frame")
          if entry.dropReceipt.isEmpty then
            entry.dropReceipt=Some(adapter.recordRetainedDbFirstMainDrop(confirmed,
              entry.completedRemovalAck.get,entry.completedTicketAck.get))
          Some(entry.dropReceipt.get)
      catch case cause:Throwable=>
        entry.retainedPhysicalFailure match
          case Some(primary)=>if primary ne cause then primary.addSuppressed(cause)
          case None=>entry.retainedPhysicalFailure=Some(cause)
        dbFirstMainFailed(entry,cause)
        throw cause

    private def requireDbFirstPhysicalPermission(entry:DbFirstRecallEntry):Unit =
      entry.confirmation.filter(_.isRetained).foreach { confirmed=>
        entry.retainedPhysicalFailure.foreach(throw _)
        require(confirmed.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)),"retained physical cause changed")
        confirmed.requireOriginal(entry.reservation)
      }

    /** 原owner已撤冗余reply，native完成后的Main撤cap步骤；调用栈返回后才提交worker退休。 */
    private[server] def handoffRetainedDbFirstRecall(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry):Unit =
      requireDbFirstEntry(adapter,entry)
      require(entry.resources.isEmpty && entry.dropReceipt.exists(_.isRetained) &&
        entry.confirmation.exists(_.isRetained) && !entry.releaseSubmitted &&
        entry.completedRemovalAck.nonEmpty && entry.completedTicketAck.exists(ticket=>
          entry.completedRemovalAck.exists(_ eq ticket.removal())),
        "actual original retained native completion and live Main cap required")
      entry.retainedPhysicalFailure.foreach(throw _)
      require(entry.confirmation.get.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)),
        "Main graph handoff preserves original retained failure")
      adapter.handoffRetainedDbFirstMainDrop(entry.dropReceipt.get)
      entry.confirmation=None
      entry.bootOwner.foreach(_=>entry.reservationOwner=None)

    /** 后续真实END复用原slot；拒绝未接受时仅保原receipt，不重放native。 */
    private[server] def submitRetainedDbFirstRecallRelease(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry,
        tick:PermanentEndTick):IoSubmission =
      requireDbFirstEntry(adapter,entry);requireRecallEnd(entry,tick)
      require(tick!=null && tick.bootUuid==identity.bootId && tick.bootGeneration==identity.bootGeneration &&
        tick.endOrdinal>entry.lastEnd && entry.resources.isEmpty &&
        entry.confirmation.isEmpty && entry.dropReceipt.exists(_.isRetained) && !entry.releaseSubmitted,
        "retained release requires actual later END after Main cap handoff")
      entry.bootOwner.foreach(_.requireBootPositiveRelease(this,entry.tag,tick))
      entry.retainedPhysicalFailure.foreach(throw _)
      entry.lastEnd=tick.endOrdinal
      val result=adapter.submitRetainedDbFirstRecallRelease(entry.dropReceipt.get)
      if result==IoSubmission.Accepted then entry.releaseSubmitted=true
      result

    private[server] def finishRetainedDbFirstRecallRelease(adapter:MySqlOwnedIoAdapter,entry:DbFirstRecallEntry,
        completion:IoCompletion[MySqlOwnedIoReply]):Unit =
      requireDbFirstEntry(adapter,entry)
      require(entry.resources.isEmpty && entry.confirmation.isEmpty && entry.releaseSubmitted &&
        entry.dropReceipt.exists(_.isRetained) && completion!=null && completion.tag==entry.tag,
        "actual detached retained Main holder and original ACK required")
      entry.retainedPhysicalFailure.foreach(throw _)
      adapter.finishRetainedDbFirstRecallRelease(entry.dropReceipt.get,completion)
      entry.dropReceipt=None;entry.completedRemovalAck=None;entry.completedTicketAck=None
      dbFirstRecalls.remove(entry.tag)

    /** Unit frame返回后再通知worker drop，避免Main调用栈仍持有bundle中的snapshot/body/ticket。 */
    private def progressDbFirstResources(entry: DbFirstRecallEntry,end: Long): Unit =
      val retained=entry.resources.get
      if retained.removal.isEmpty then
        entry.confirmation.get.requireOriginal(entry.reservation)
        requireDbFirstRecallBody(entry)
        val world=retained.record.world match
          case actual:PodWorldImpl=>actual
          case _=>throw new IllegalStateException("DB-first recall requires original Forge world")
        retained.observer=Some(new ExactBodyRemovalObserver(new DbFirstRecallRemovalView(world,retained.body)))
        retained.removal=Some(retained.observer.get.captureForIo(retained.body.rawEntity,entry.tag,entry.attemptUuid,entry.base.stateVersion,end))
        val footprint=chunkLoadingManager.publicationTicketChunks(entry.base.uuid,world,retained.ticket)
        retained.ticketRelease=Some(chunkLoadingManager.captureReleaseForIo(entry.tag,entry.base.uuid,entry.attemptUuid,
          entry.base.stateVersion,retained.body.entityId,world,retained.ticket,footprint))
      val removal=retained.removal.get
      if !removal.requestIssued then
        requireDbFirstPhysicalPermission(entry)
        retained.observer.get.request(removal,end) match
          case ExactRemovalStatus.IdentityConflict(_,sources)=>throw new IllegalStateException("DB-first original removal identity conflict: "+sources.mkString(","))
          case _=>()
        removal.requestFailure.foreach(throw _)
      else
        retained.removalAck=retained.observer.get.acknowledgeForIo(removal,end)
        if removal.conflict.nonEmpty then throw new IllegalStateException("DB-first local END identity conflict: "+removal.conflict.mkString(","))
        retained.removalAck.foreach { ack=>
          require(managedByPod.get(entry.base.uuid).contains(ManagedPodStatus.Active(retained.record)),"DB-first manager record changed during removal")
          requireDbFirstPhysicalPermission(entry)
          retained.ticketAck=Some(chunkLoadingManager.releaseForIo(retained.ticketRelease.get,ack).get)
          chunkLoadingManager.compactDbFirstRecallRelease(retained.ticketRelease.get,retained.ticketAck.get)
          managedByPod.remove(entry.base.uuid);cachedStates.remove(entry.base.uuid)
          dirtyPods.remove(entry.base.uuid);dirtyRevisions.remove(entry.base.uuid)
          normalStopBodies.remove(entry.base.uuid);normalRecallFootprints.remove(entry.base.uuid)
          entry.stopSource.foreach(_.dropRecallResources())
          entry.completedRemovalAck=retained.removalAck;entry.completedTicketAck=retained.ticketAck
          entry.resources=None
        }

    /** actual worker release ACK消费成功后才退休小holder；失败继续保留原receipt和cause。 */
    private[server] def finishDbFirstRecallRelease(adapter: MySqlOwnedIoAdapter,entry: DbFirstRecallEntry,
        completion: IoCompletion[MySqlOwnedIoReply]): Unit =
      requireDbFirstEntry(adapter,entry)
      require(entry.resources.isEmpty && entry.releaseSubmitted && entry.dropReceipt.nonEmpty && completion!=null && completion.tag==entry.tag,
        "DB-first release ACK requires original detached Main holder")
      entry.cause.foreach(throw _)
      try
        adapter.finishDbFirstRecallRelease(entry.dropReceipt.get,completion)
        entry.dropReceipt=None;entry.confirmation=None
        dbFirstRecalls.remove(entry.tag)
      catch
        case cause: Throwable=>
          dbFirstMainFailed(entry,cause)
          throw cause

    private[server] def dbFirstRecall(tag: IoOperationTag): Option[DbFirstRecallEntry] =
      requireOwner()
      dbFirstRecalls.get(tag)

    /** 完整checkpoint只更新原活动业务；Main责任先于冻结和capture，UNKNOWN不换operation。 */
    final class DbFirstCheckpointEntry private[MainPublicationScope] (
        originalReservation:MySqlDbFirstCaptureReservation,val record:ManagedActivePod,
        val body:ActivePodEntityImpl,val ticket:ChunkTicket,val finalPose:FloatingPodPose):
      val tag:IoOperationTag=originalReservation.tag
      val base:FloatingPodState=record.exactState
      val capturedState:FloatingPodState=base.copy(posX=finalPose.posX,posY=finalPose.posY,posZ=finalPose.posZ,
        yaw=finalPose.yaw,pitch=finalPose.pitch)
      val capturedRevision:Long=dirtyRevisions.getOrElse(base.uuid,0L)
      private[MainPublicationScope] var reservation:Option[MySqlDbFirstCaptureReservation]=Some(originalReservation)
      private[MainPublicationScope] var captured=Option.empty[DbFirstPodSnapshot]
      private[MainPublicationScope] var confirmation=Option.empty[MySqlDbFirstCheckpointConfirmation]
      private[MainPublicationScope] var readReleaseSubmitted=false
      private[MainPublicationScope] var readAcknowledged=false
      private[MainPublicationScope] var published=Option.empty[FloatingPodState]
      private[MainPublicationScope] var finalReceipt=Option.empty[MySqlDbFirstCheckpointFinalReceipt]
      private[MainPublicationScope] var finalSubmitted=false
      private[MainPublicationScope] var cause=Option.empty[Throwable]
      def snapshot:DbFirstPodSnapshot =
        requireOwner();cause.foreach(throw _)
        require(confirmation.isEmpty,"confirmed checkpoint cannot lend its capture again")
        requireDbFirstCheckpointBody(this)
        captured.getOrElse(throw new IllegalStateException("original complete checkpoint capture missing"))

    private val dbFirstCheckpoints=mutable.LinkedHashMap.empty[IoOperationTag,DbFirstCheckpointEntry]
    private def hasDbFirstCheckpoint(pod:UUID):Boolean=dbFirstCheckpoints.valuesIterator.exists(_.base.uuid==pod)

    private[server] def beginDbFirstCheckpoint(adapter:MySqlOwnedIoAdapter,
        reservation:MySqlDbFirstCaptureReservation,pod:UUID,expectedVersion:Long):DbFirstCheckpointEntry =
      requireOwner()
      require(adapter!=null && reservation!=null && pod!=null &&
        reservation.purpose==DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT,"original active checkpoint capture required")
      requireOriginalMainAdapter(adapter);adapter.requireDbFirstCaptureReservation(reservation)
      identity.requireTag(reservation.tag)
      require(!retired && !dbFirstCheckpoints.contains(reservation.tag) && dbFirstCheckpoints.size<activeLimit,
        "original bounded live checkpoint publication required")
      assertCriticalTransitionAllowed(pod)
      require(!pending.contains(reservation.tag) && !pendingModes.contains(reservation.tag) &&
        !pendingDrifts.contains(reservation.tag) && !dbFirstRecalls.contains(reservation.tag) &&
        !pending.valuesIterator.exists(_.receipt.podUuid==pod) &&
        !pendingModes.valuesIterator.exists(_.sqlRequest.podUuid==pod),"checkpoint conflicts with original publication/mode")
      val record=managedByPod.get(pod) match
        case Some(ManagedPodStatus.Active(value))=>value
        case _=>throw new IllegalStateException("checkpoint requires original active manager record")
      require(record.exactState.isActive && record.exactState.stateVersion==expectedVersion,
        "checkpoint requires original active committed state/version")
      val body=record.entity match
        case actual:ActivePodEntityImpl=>actual
        case _=>throw new IllegalStateException("checkpoint requires actual complete Forge snapshot owner")
      record.world.assertLiveIndexedBody(body)
      val control=body.rawEntity.serverControlSnapshot()
      val pose=body.actualPose
      require(control!=null && control.bindingUuid()==binding.bindingUuid && control.podUuid()==pod &&
        control.attemptUuid()==record.attemptUuid && control.ownerUuid()==record.exactState.ownerUuid.orNull &&
        control.stateVersion()==expectedVersion && control.operatingMode()==record.exactState.operatingMode.toString &&
        control.active(),"checkpoint control/binding/attempt/business differs")
      val desired=record.exactState.copy(posX=pose.posX,posY=pose.posY,posZ=pose.posZ,yaw=pose.yaw,pitch=pose.pitch)
      MySqlTrackedDriftRequest.requireSameChunkPoseOnly(binding,record.exactState,desired,record.attemptUuid)
      require(cachedStates.get(pod).contains(desired),"checkpoint actual pose must equal the original Main cache capture")
      val ticket=chunkLoadingManager.managedTicketRecords().getOrElse(pod,
        throw new IllegalStateException("checkpoint original ticket missing")).ticket
      val chunks=chunkLoadingManager.publicationTicketChunks(pod,record.world,ticket)
      require(chunks.size==25 && chunks==recoveryChunks(pose.dimensionId,pose.posX,pose.posZ),
        "checkpoint requires original actual25 ticket")
      val entry=new DbFirstCheckpointEntry(reservation,record,body,ticket,pose)
      dbFirstCheckpoints.put(entry.tag,entry)
      try
        body.rawEntity.beginDbFirstCheckpoint(entry.tag.operationId,expectedVersion,normalStopRequest.orNull)
        requireDbFirstCheckpointBody(entry)
        entry.captured=Some(adapter.captureDbFirstSnapshot(reservation) { allocation=>
          body.rawEntity.captureDbFirstCheckpoint(entry.tag.operationId,expectedVersion,allocation)
        })
        requireDbFirstCheckpointBody(entry)
        entry
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    private def requireCheckpointEntry(adapter:MySqlOwnedIoAdapter,entry:DbFirstCheckpointEntry):Unit =
      requireOwner()
      require(adapter!=null && entry!=null && !retired && dbFirstCheckpoints.get(entry.tag).exists(_ eq entry),
        "original retained checkpoint Main entry required")
      identity.requireTag(entry.tag);requireOriginalMainAdapter(adapter)
    private def requireDbFirstCheckpointBody(entry:DbFirstCheckpointEntry):Unit =
      requireOwner()
      require(!retired && dbFirstCheckpoints.get(entry.tag).exists(_ eq entry) && entry.published.isEmpty &&
        managedByPod.get(entry.base.uuid).contains(ManagedPodStatus.Active(entry.record)) &&
        entry.body.actualPose==entry.finalPose,"original checkpoint body/record/pose changed")
      entry.record.world.assertLiveIndexedBody(entry.body)
      entry.body.rawEntity.requireDbFirstCheckpoint(entry.tag.operationId,entry.base.stateVersion)
      val chunks=chunkLoadingManager.publicationTicketChunks(entry.base.uuid,entry.record.world,entry.ticket)
      require(chunks==recoveryChunks(entry.finalPose.dimensionId,entry.finalPose.posX,entry.finalPose.posZ) &&
        chunks.size==25,"original checkpoint ticket changed")
    private def checkpointMainFailed(entry:DbFirstCheckpointEntry,cause:Throwable):Unit =
      entry.cause match
        case Some(primary)=>if primary ne cause then primary.addSuppressed(cause)
        case None=>
          entry.cause=Some(cause)
          logger.error("DB-first checkpoint Main failed for pod "+entry.base.uuid+" operation "+entry.tag.operationId,cause)
      try entry.body.markControlFaulted(cause)
      catch case diagnostic:Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)

    private[server] def completeDbFirstCheckpoint(adapter:MySqlOwnedIoAdapter,entry:DbFirstCheckpointEntry,
        completion:IoCompletion[MySqlOwnedIoReply]):Unit =
      requireCheckpointEntry(adapter,entry);entry.cause.foreach(throw _)
      require(completion!=null && completion.tag==entry.tag && entry.confirmation.isEmpty,
        "original checkpoint completion is one-shot")
      try
        val reservation=entry.reservation.get
        val confirmed=adapter.claimDbFirstCheckpointConfirmation(reservation,completion)
        entry.confirmation=Some(confirmed)
        confirmed.requireOriginal(reservation);requireDbFirstCheckpointBody(entry)
        require(confirmed.bindingUuid==binding.bindingUuid && confirmed.podUuid==entry.base.uuid &&
          confirmed.ownerUuid==entry.base.ownerUuid && confirmed.attemptUuid==entry.record.attemptUuid &&
          confirmed.capturedStateVersion==entry.base.stateVersion &&
          confirmed.committedStateVersion==Math.addExact(entry.base.stateVersion,1L) &&
          confirmed.pose==entry.captured.get.pose(),"checkpoint confirmation differs from original frozen capture")
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    private[server] def submitDbFirstCheckpointReadRelease(adapter:MySqlOwnedIoAdapter,
        entry:DbFirstCheckpointEntry):IoSubmission =
      requireCheckpointEntry(adapter,entry);entry.cause.foreach(throw _)
      require(!entry.readReleaseSubmitted && !entry.readAcknowledged && entry.confirmation.nonEmpty,
        "original checkpoint read release already accepted or unconfirmed")
      try
        requireDbFirstCheckpointBody(entry)
        val result=adapter.submitCheckpointMainReadRelease(entry.reservation.get,entry.confirmation.get)
        entry.readReleaseSubmitted=result==IoSubmission.Accepted
        result
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    /** 先消费实际worker读代ACK，再从新读代核对完整后继；实体/ticket始终保持原对象。 */
    private[server] def finishDbFirstCheckpointReadRelease(adapter:MySqlOwnedIoAdapter,
        entry:DbFirstCheckpointEntry,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
      requireCheckpointEntry(adapter,entry);entry.cause.foreach(throw _)
      require(entry.readReleaseSubmitted && !entry.readAcknowledged && entry.published.isEmpty,
        "original checkpoint read ACK is one-shot")
      try
        requireDbFirstCheckpointBody(entry)
        val confirmation=entry.confirmation.get
        adapter.finishCheckpointMainReadRelease(entry.reservation.get,confirmation,completion)
        entry.readAcknowledged=true
        val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original checkpoint Main read owner missing"))
        val state=callbacks.findState(entry.base.uuid).getOrElse(throw new IllegalStateException("confirmed checkpoint successor absent"))
        require(state.stateVersion==confirmation.committedStateVersion &&
          state.copy(stateVersion=entry.base.stateVersion,lastUpdated=entry.base.lastUpdated)==entry.capturedState &&
          FloatingPodEntityControl.sameActualPose(entry.body.actualPose,state),
          "checkpoint successor changed active business/mode/owner/pose")
        require(dirtyRevisions.getOrElse(state.uuid,0L)==entry.capturedRevision &&
          cachedStates.get(state.uuid).contains(entry.capturedState),"frozen checkpoint cache/revision changed during IO")
        val original=entry.body.rawEntity.serverControlSnapshot()
        val control=FloatingPodEntityControl.committed(state,Option(original.bindingUuid()),
          Some(entry.record.attemptUuid),Option(original.operationUuid()))
        entry.body.rawEntity.finishDbFirstCheckpoint(entry.tag.operationId,entry.base.stateVersion,control)
        normalStopRequest.foreach(entry.body.freezeForNormalStop)
        entry.record.world.assertLiveIndexedBody(entry.body)
        require(entry.body.actualPose==entry.finalPose,"checkpoint body moved during successor publication")
        managedByPod.put(state.uuid,ManagedPodStatus.Active(entry.record.copy(exactState=state)))
        cachedStates.put(state.uuid,state);dirtyPods.remove(state.uuid)
        entry.published=Some(state)
        // 原entry保留已提交结果；任何后续退休失败都不能回写成旧版本或移除body/ticket。
        entry.finalReceipt=Some(adapter.recordDbFirstCheckpointFinal(confirmation))
        entry.captured=None;entry.reservation=None;entry.confirmation=None
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    private[server] def submitDbFirstCheckpointFinal(adapter:MySqlOwnedIoAdapter,
        entry:DbFirstCheckpointEntry):IoSubmission =
      requireCheckpointEntry(adapter,entry);entry.cause.foreach(throw _)
      require(entry.published.nonEmpty && entry.finalReceipt.nonEmpty && !entry.finalSubmitted &&
        entry.captured.isEmpty && entry.reservation.isEmpty && entry.confirmation.isEmpty,
        "original published checkpoint Main capture must detach before final retirement")
      try
        val result=adapter.submitDbFirstCheckpointFinalRelease(entry.finalReceipt.get)
        entry.finalSubmitted=result==IoSubmission.Accepted
        result
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    private[server] def finishDbFirstCheckpointFinal(adapter:MySqlOwnedIoAdapter,
        entry:DbFirstCheckpointEntry,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
      requireCheckpointEntry(adapter,entry);entry.cause.foreach(throw _)
      require(entry.finalSubmitted && entry.published.nonEmpty && entry.finalReceipt.nonEmpty,
        "original checkpoint final worker ACK required")
      try
        adapter.finishDbFirstCheckpointFinalRelease(entry.finalReceipt.get,completion)
        entry.finalReceipt=None;dbFirstCheckpoints.remove(entry.tag)
      catch case cause:Throwable=>
        checkpointMainFailed(entry,cause)
        throw cause

    private[server] def dbFirstCheckpoint(tag:IoOperationTag):Option[DbFirstCheckpointEntry] =
      requireOwner();dbFirstCheckpoints.get(tag)

    /** Boot 读租约失效前由原装配点留存；停止时不得回借旧 Boot 或从路径重造身份。 */
    private[server] def retainOriginalRecallSource(reads: MainBootProjection): Unit =
      requirePublicationMain()
      require(reads != null && reads.identity == identity && reads.ordinaryPublicationBinding == binding &&
        originalRecallSource.isEmpty && normalStopRequest.isEmpty && !retired,
        "original ordinary publication source must be captured exactly once before stop")
      val facts = reads.ordinaryRecallSourceFacts
      normalRecallFootprints ++= reads.originalActiveAttemptFootprints
      originalRecallSource = Some(facts)

    private[server] def recallSourceIdentity: MySqlNormalRecallSourceIdentity =
      requirePublicationMain()
      originalRecallSource.getOrElse(throw new IllegalStateException("original ordinary source was not captured"))

    /** 这是仍持有物理责任的原Main对象；不是提交/删除/释放或Main closure ACK。 */
    final class NormalStopEntry private[MainPublicationScope] (
        originalRecord: ManagedActivePod, originalBody: NormalStopActivePodEntity,
        originalTicket: ChunkTicket, val finalPose: FloatingPodPose):
      val podUuid: UUID = originalRecord.exactState.uuid
      val retainedResident: Boolean = originalRecord.exactState.operatingMode == PodOperatingMode.WorldIdle
      private var resources = Option((originalRecord, originalBody, originalTicket))
      def record: ManagedActivePod = resources.getOrElse(throw new IllegalStateException("stop source resources dropped"))._1
      def body: NormalStopActivePodEntity = resources.getOrElse(throw new IllegalStateException("stop source resources dropped"))._2
      def ticket: ChunkTicket = resources.getOrElse(throw new IllegalStateException("stop source resources dropped"))._3
      private[server] def base: FloatingPodState = record.exactState
      private[server] def attemptUuid: UUID = record.attemptUuid
      private[server] def world: PodWorld = record.world
      private[server] def recallRequest: com.wi.floatingpod.storage.mysql.MySqlNormalRecallRequest =
        requireOwner()
        require(!retainedResident && normalStopCensus.exists(_.entries.exists(_ eq this)) &&
          managedByPod.get(podUuid).contains(ManagedPodStatus.Active(record)), "original Normal census entry required")
        val (attempt, footprint) = normalRecallFootprints.getOrElse(podUuid,
          throw new IllegalStateException("original committed footprint identity was not retained"))
        require(attempt == attemptUuid, "recall footprint belongs to another attempt")
        com.wi.floatingpod.storage.mysql.MySqlNormalRecallRequest(normalStopRequest.get, binding, base, finalPose,
          dirtyRevisions.getOrElse(podUuid, 0L), attempt, footprint,
          chunkLoadingManager.publicationTicketChunks(podUuid, world, ticket).toVector, recallSourceIdentity)
      private[MainPublicationScope] def dropRecallResources(): Unit =
        require(!retainedResident && resources.nonEmpty, "original recall resources drop once")
        resources = None
      private[MainPublicationScope] def dropResidentResources(): Unit =
        require(retainedResident && resources.nonEmpty, "original resident resources drop once")
        resources = None
    final class NormalStopCensus private[MainPublicationScope] (
        val request: UUID, val entries: Vector[NormalStopEntry])

    private var normalRecalls = Option.empty[NormalRecallResponsibility]
    private[server] def beginNormalRecalls(adapter: MySqlOwnedIoAdapter,
        census: ActiveFloatingPodManager#MainPublicationScope#NormalStopCensus,
        requireUsable: () => Unit): NormalRecallResponsibility =
      requireOwner()
      require(normalRecalls.isEmpty && normalStopCensus.exists(_ eq census) && !retired &&
        (currentDriftRuntime().adapter eq adapter), "original normal-stop owner begins recall exactly once")
      val result = new NormalRecallResponsibility(this, adapter, chunkLoadingManager,
        normalStopCensus.get.entries.filterNot(_.retainedResident), requireUsable)
      normalRecalls = Some(result)
      result

    private[server] def requireRecallBody(source: ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry): Unit =
      requireOwner()
      normalStopFailure.foreach(throw _)
      require(!retired && normalStopCensus.exists(_.entries.exists(_ eq source)) && !source.retainedResident &&
        managedByPod.get(source.podUuid).contains(ManagedPodStatus.Active(source.record)), "original retained recall body required")
      source.body.requireNormalStopFreeze(normalStopRequest.get, source.base, source.attemptUuid)
      require(source.body.isFrozen && source.body.actualPose == source.finalPose, "recall body changed after freeze")
      source.world.assertLiveIndexedBody(source.body)

    private[server] def completeNormalRecall(owner: NormalRecallResponsibility,
        source: ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry): Unit =
      requireOwner()
      require(normalRecalls.exists(_ eq owner) && normalStopCensus.exists(_.entries.exists(_ eq source)),
        "original recall responsibility required for manager cleanup")
      owner.requireCompleted(source)
      require(managedByPod.get(source.podUuid).contains(ManagedPodStatus.Active(source.record)), "recall manager record changed")
      managedByPod.remove(source.podUuid)
      cachedStates.remove(source.podUuid)
      dirtyPods.remove(source.podUuid)
      dirtyRevisions.remove(source.podUuid)
      normalStopBodies.remove(source.podUuid)
      normalRecallFootprints.remove(source.podUuid)
      normalStopCensus.get.entries.find(_ eq source).get.dropRecallResources()

    private final class ResidentCheckpoint(val source: NormalStopEntry, val drift: PendingDrift):
      var releasePermit = Option.empty[ResidentTicketReleasePermit]
      var releaseAck = Option.empty[ChunkLoadingManager#ResidentReleaseAcknowledgement]
      var cleaned = false
      var finalEscrowAmount = 0L
      var finalFactMinted = false
    private val residentCheckpoints = mutable.LinkedHashMap.empty[UUID, ResidentCheckpoint]
    private val completedResidents = mutable.LinkedHashMap.empty[UUID, FloatingPodState]
    private var residentCheckpointsStarted = false

    /** 只由本scope消费原full20完成并收到原projection release后构造；不是可构造的提交DTO。 */
    final class ResidentTicketReleasePermit private (private val legacy:Option[ResidentCheckpoint],
        private val dbFirst:Option[DbFirstStoppedResident]):
      private[MainPublicationScope] def this(retained:ResidentCheckpoint)=this(Some(retained),None)
      private[MainPublicationScope] def this(retained:DbFirstStoppedResident)=this(None,Some(retained))
      private def retained:ResidentCheckpoint=legacy.get
      private[server] val tag: IoOperationTag = dbFirst.map(_.checkpoint.get.tag).getOrElse(retained.drift.tag)
      // ACK compact后只保留此无外链身份，不保留permit及其resident/source图。
      private[server] val releaseIdentity: AnyRef = new Object()
      private[server] def belongsToManager(tickets:ChunkLoadingManager):Boolean = tickets eq chunkLoadingManager
      private[server] def pod: UUID = dbFirst.map(_.source.podUuid).getOrElse(retained.source.record.exactState.uuid)
      private[server] def world: PodWorld = dbFirst.map(_.checkpoint.get.record.world).getOrElse(retained.source.record.world)
      private[server] def ticket: ChunkTicket = dbFirst.map(_.checkpoint.get.ticket).getOrElse(retained.source.ticket)
      private[server] def requireCurrent(tickets: ChunkLoadingManager): Unit =
        requireOwner()
        normalStopFailure.foreach(throw _)
        if dbFirst.nonEmpty then
          val original=dbFirst.get
          val checkpoint=original.checkpoint.get
          checkpoint.cause.foreach(throw _)
          val state=checkpoint.published.get
          require((tickets eq chunkLoadingManager) && !retired &&
            dbFirstStoppedResidents.get(pod).exists(_ eq original) && original.permit.exists(_ eq this) &&
            !original.cleaned && original.finalAcknowledged && !dbFirstCheckpoints.contains(checkpoint.tag) &&
            state.isActive && state.operatingMode==PodOperatingMode.WorldIdle &&
            state.ownerUuid==checkpoint.base.ownerUuid &&
            managedByPod.get(pod).contains(ManagedPodStatus.Active(checkpoint.record.copy(exactState=state))),
            "original schema6 full checkpoint final ACK and resident owner required")
          checkpoint.record.world.assertLiveIndexedBody(checkpoint.body)
          checkpoint.body.requireNormalStopFreeze(normalStopRequest.get,state,checkpoint.record.attemptUuid)
          require(checkpoint.body.isFrozen && FloatingPodEntityControl.sameActualPose(checkpoint.body.actualPose,state),
            "schema6 resident body moved or thawed after commit")
          return
        currentDriftRuntime()
        require((tickets eq chunkLoadingManager) && !retired && residentCheckpoints.get(pod).exists(_ eq retained) &&
          retained.releasePermit.exists(_ eq this) && !retained.cleaned && retained.drift.stopProjectionReleased,
          "original resident checkpoint and projection release owner required")
        val committed = retained.drift.stopCommittedState.getOrElse(
          throw new IllegalStateException("resident checkpoint has no original full20 commit"))
        val source = retained.source
        require(managedByPod.get(pod).contains(ManagedPodStatus.Active(source.record.copy(exactState = committed))) &&
          committed.isActive && committed.operatingMode == PodOperatingMode.WorldIdle &&
          committed.ownerUuid == source.record.exactState.ownerUuid &&
          FloatingPodEntityControl.sameActualPose(source.finalPose, committed),
          "resident checkpoint differs from original settled manager state")
        source.record.world.assertLiveIndexedBody(source.body)
        source.body.requireNormalStopFreeze(normalStopRequest.get, committed, source.record.attemptUuid)
        require(source.body.isFrozen && FloatingPodEntityControl.sameActualPose(source.body.actualPose, committed),
          "resident body moved or thawed after checkpoint")

    /** 复用原F3 same-chunk pose-only事务；所有ID/plan只在原census上分配一次。 */
    private[server] def beginWorldResidentCheckpoints(census: ActiveFloatingPodManager#MainPublicationScope#NormalStopCensus): Unit =
      requireOwner()
      require(!retired && normalStopCensus.exists(_ eq census) && !residentCheckpointsStarted,
        "original captured census begins resident checkpoints once")
      normalStopStep {
        captureNormalStopCensus(normalStopRequest.get)
        val runtime = currentDriftRuntime()
        residentCheckpointsStarted = true
        normalStopCensus.get.entries.filter(_.retainedResident).foreach { source =>
          val base = source.record.exactState
          val body = source.body match
            case actual: TrackedDriftActivePodEntity => actual
            case _ => throw new IllegalStateException("resident checkpoint requires original tracked body adapter")
          val pose = source.finalPose
          val desired = base.copy(posX = pose.posX, posY = pose.posY, posZ = pose.posZ, yaw = pose.yaw, pitch = pose.pitch)
          updateCachedState(base.uuid, desired).get
          val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, UUID.randomUUID())
          val entry = new PendingDrift(tag, UUID.randomUUID(), UUID.randomUUID(),
            MySqlTrackedDriftRequest(binding, base, desired, source.record.attemptUuid), dirtyRevisions(base.uuid),
            source.record.world, body, source.ticket)
          residentCheckpoints.put(base.uuid, new ResidentCheckpoint(source, entry))
          pendingDrifts.put(tag, entry)
          trackedDriftPendingTagsByPod.put(base.uuid, tag)
          entry.plan = Some(runtime.adapter.reserveRepositoryPlan(tag, entry.workflow, entry.reserve, entry.request.repositoryOperation))
        }
      }

    private var finalSaveAdmission: ManagedOrdinarySaveAdmission = null
    private var finalSaveEscrow: ManagedOrdinarySaveAdmission.SourceEscrow = null
    private var finalSaveSource: ActiveFloatingPodManager.FinalSaveSource = null
    private var finalSaveCharge = 0L
    private var finalOldMutationEnvelope = 0L
    private var finalCohortStarted = false
    private var finalMainReferencesDropped = false

    /** 先为本方法扫描/UUID及初始escrow预付；旧票/原IO在pending期间仍保留。 */
    private[server] def tryBeginFinalPreparation(admission: ManagedOrdinarySaveAdmission): Option[UUID] =
      requireOwner()
      normalStopFailure.foreach(throw _)
      require(admission != null && (finalSaveAdmission == null || (finalSaveAdmission eq admission)),
        "original final-save admission required")
      if finalSaveAdmission == null then finalSaveAdmission = admission
      if finalSaveEscrow == null then
        // 原Main独占提交；本次真实观察到new之间没有submit/world callback，worker只移动旧Work。
        val originalIo = dbFirstReadOwner match
          case Some(callbacks) =>
            callbacks.requireOriginalMainAdapter(callbacks.adapter)
            require((callbacks.ordinarySaveAdmission() eq admission) && normalStopRequest.nonEmpty,
              "original schema6 stopped source admission required")
            callbacks.adapter.checkpoint.io
          case None => currentDriftRuntime().adapter.checkpoint.io
        originalIo.fatalCause.foreach(throw _)
        require(originalIo.queued >= 0, "actual original IO queue observation required")
        val envelope = Math.multiplyExact(56L, Math.addExact(originalIo.queued.toLong, 1L))
        val actualEscrow = admission.tryReserveFinalSource(this, Math.addExact(640L + 2296L, envelope))
        if actualEscrow != null then
          finalOldMutationEnvelope = envelope
          finalSaveEscrow = actualEscrow
      if finalSaveEscrow == null then None
      else
        if normalStopRequest.isEmpty then sealForNormalStop(UUID.randomUUID())
        normalStopRequest

    private[floatingpod] def requireFinalSourceOwner(original: Object, escrow: ManagedOrdinarySaveAdmission.SourceEscrow): Unit =
      requireOwner()
      require((original eq this) && finalSaveAdmission != null && finalSaveEscrow != null && (escrow eq finalSaveEscrow) && !finalMainReferencesDropped,
        "original private publication source factory required")
    private[floatingpod] def requireFinalSource(original: Object, escrow: ManagedOrdinarySaveAdmission.SourceEscrow, source: ManagedOrdinarySaveAdmission.FinalSaveValidator): Unit =
      requireFinalSourceOwner(original, escrow)
      require(finalSaveSource != null && (source eq finalSaveSource), "original private factory source required")
    private[server] def finalSourceOwner: Object =
      requireOwner()
      this
    private[server] def finalEscrow: ManagedOrdinarySaveAdmission.SourceEscrow =
      requireOwner()
      require(finalSaveEscrow != null, "prepaid original final escrow required")
      finalSaveEscrow

    /** 全cohort先结SQL/Recall，再统一预付；没有任何ticket副作用。 */
    private[server] def prepareFinalCohort(): Boolean =
      requireOwner()
      normalStopFailure.foreach(throw _)
      require(finalSaveAdmission != null && finalSaveEscrow != null && residentCheckpointsStarted,
        "original final preparation/checkpoints required")
      retainedFailureCauses.headOption.foreach(throw _)
      if normalRecalls.forall(!_.isComplete) || pending.nonEmpty || pendingModes.nonEmpty || pendingDrifts.nonEmpty || dbFirstRecalls.nonEmpty || dbFirstCheckpoints.nonEmpty || dbFirstPeriodicSnapshots.nonEmpty || dbFirstPoseFailures.nonEmpty then false
      else if residentCheckpoints.valuesIterator.exists(value => !value.drift.stopProjectionReleased) then false
      else
        var amount = Math.addExact(640L + 2296L + 48L + 48L + 48L + 64L + 32L, finalOldMutationEnvelope)
        // 640含原有限scratch及Adapter瞬时Some(owner)24；旧Work包络独立保留，不覆盖后续Mutation费。
        var count = 0
        residentCheckpoints.valuesIterator.foreach { retained =>
          retained.drift.cause.foreach(throw _)
          val control = retained.source.body match
            case actual: ActivePodEntityImpl => actual.rawEntity.serverControlSnapshot()
            case _ => throw new IllegalStateException("actual Forge resident body required")
          val fee = ActiveFloatingPodManager.finalResidentCharge(control)
          if retained.finalEscrowAmount != 0L then require(retained.finalEscrowAmount == fee, "resident source charge changed")
          retained.finalEscrowAmount = fee
          amount = Math.addExact(amount, fee)
          count = Math.incrementExact(count)
        }
        amount = Math.addExact(amount, ActiveFloatingPodManager.alignedArray(8L * count) + ActiveFloatingPodManager.alignedArray(count))
        // 一次final token+producer、Flight、MainFlush、所有同chunk KeySet、正常codec/caller临时峰。
        val live = Math.addExact(512L + 56L + 64L + 48L + 568L, Math.multiplyExact(24L, count))
        if !finalSaveAdmission.tryGrowFinalSourceEscrow(finalSaveEscrow, this, amount, live) then false
        else
          if finalSaveSource == null then
            finalSaveCharge = amount
            finalSaveSource = new ActiveFloatingPodManager.FinalSaveSource(count, amount)
          require(finalSaveCharge == amount, "original final cohort source cost changed")
          true

    /** schema6原完整checkpoint已结算；保留body事实直到真实final producer接管。 */
    private[server] def prepareDbFirstFinalCohort():Boolean =
      requireOwner()
      require(dbFirstReadOwner.nonEmpty && dbFirstStopCensus.nonEmpty && finalSaveAdmission!=null &&
        finalSaveEscrow!=null,"original schema6 stopped census and final escrow required")
      normalStopFailure.foreach(throw _)
      retainedFailureCauses.headOption.foreach(throw _)
      if !dbFirstNormalRecallCompleted.contains(true) || pending.nonEmpty || pendingModes.nonEmpty ||
        pendingDrifts.nonEmpty || dbFirstRecalls.nonEmpty || dbFirstCheckpoints.nonEmpty ||
        dbFirstPeriodicSnapshots.nonEmpty || dbFirstPoseFailures.nonEmpty || dbFirstStopPending.nonEmpty then false
      else if dbFirstStoppedResidents.valuesIterator.exists(r=> !r.finalAcknowledged || !r.cleaned) then false
      else
        require(dbFirstStoppedResidents.size==dbFirstStopCensus.get.entries.count(_.retainedResident),
          "original schema6 complete resident cohort required")
        var amount=Math.addExact(640L+2296L+48L+48L+48L+64L+32L,finalOldMutationEnvelope)
        dbFirstStoppedResidents.valuesIterator.foreach { retained=>
          val entry=retained.checkpoint.get
          entry.cause.foreach(throw _)
          require(entry.finalReceipt.isEmpty && !dbFirstCheckpoints.contains(entry.tag),
            "actual checkpoint final ACK required before source preparation")
          amount=Math.addExact(amount,ActiveFloatingPodManager.finalResidentCharge(entry.body.rawEntity.serverControlSnapshot()))
        }
        val count=dbFirstStoppedResidents.size
        amount=Math.addExact(amount,ActiveFloatingPodManager.alignedArray(8L*count)+ActiveFloatingPodManager.alignedArray(count))
        val live=Math.addExact(512L+56L+64L+48L+568L,Math.multiplyExact(24L,count))
        if !finalSaveAdmission.tryGrowFinalSourceEscrow(finalSaveEscrow,this,amount,live) then false
        else
          if finalSaveSource==null then
            finalSaveCharge=amount
            finalSaveSource=new ActiveFloatingPodManager.FinalSaveSource(count,amount)
          require(finalSaveCharge==amount,"original schema6 final cohort charge changed")
          true

    /** ticket已沿原完整checkpoint ACK收束；本方法不重复release或重建SQL。 */
    private[server] def mintDbFirstFinalCohort():ManagedOrdinarySaveAdmission.FinalSaveValidator =
      requireOwner()
      require(dbFirstReadOwner.nonEmpty && finalSaveSource!=null && !finalCohortStarted,
        "original prepaid schema6 final cohort is one-shot")
      finalCohortStarted=true
      normalStopStep {
        var index=0
        dbFirstStoppedResidents.valuesIterator.foreach { retained=>
          val entry=retained.checkpoint.get
          entry.cause.foreach(throw _)
          val state=entry.published.get
          require(retained.cleaned && retained.finalAcknowledged && entry.finalReceipt.isEmpty &&
            !dbFirstCheckpoints.contains(entry.tag) && retained.permit.nonEmpty &&
            retained.releaseAck.exists(ack=>chunkLoadingManager.isCompactedResidentRelease(ack,retained.permit.get)),
            "original schema6 final worker and ticket ACK required")
          val entity=entry.body.rawEntity
          val control=entity.serverControlSnapshot()
          entry.record.world.assertLiveIndexedBody(entry.body)
          entry.body.requireNormalStopFreeze(normalStopRequest.get,state,entry.record.attemptUuid)
          require(state.isActive && state.operatingMode==PodOperatingMode.WorldIdle &&
            control.stateVersion()==state.stateVersion && control.ownerUuid()==state.ownerUuid.orNull &&
            control.attemptUuid()==entry.record.attemptUuid && FloatingPodEntityControl.sameActualPose(entry.body.actualPose,state),
            "original schema6 committed resident fact changed")
          val artifact=entity.artifactIdentity().orElseThrow()
          require(artifact.bindingUuid()==binding.bindingUuid && artifact.attemptUuid()==entry.record.attemptUuid,
            "original schema6 final artifact required")
          finalSaveSource.mint(index,entity,control,artifact,normalStopRequest.get,entry.tag.operationId)
          index+=1
        }
        finalSaveSource.requireComplete()
        finalSaveSource
      }

    /** 调用者已完成真实scheduler/queue/Flight fence；整批在本END完成，不跨tick半激活。 */
    private[server] def releaseAndMintFinalCohort(): ManagedOrdinarySaveAdmission.FinalSaveValidator =
      requireOwner()
      require(finalSaveSource != null && !finalCohortStarted, "original prepaid final cohort is one-shot")
      finalCohortStarted = true
      normalStopStep {
        residentCheckpoints.valuesIterator.foreach { retained =>
          require(retained.drift.stopProjectionReleased, "original projection release ACK required")
          if retained.releasePermit.isEmpty then retained.releasePermit = Some(new ResidentTicketReleasePermit(retained))
          retained.releasePermit.get.requireCurrent(chunkLoadingManager)
        }
        var index = 0
        residentCheckpoints.valuesIterator.foreach { retained =>
          val permit = retained.releasePermit.get
          require(retained.releaseAck.isEmpty && !retained.finalFactMinted, "resident release/fact mint once")
          retained.releaseAck = Some(chunkLoadingManager.releaseResidentForNormalStop(permit).get)
          require(retained.releaseAck.exists(_.belongsTo(permit)), "foreign resident ticket release ACK")
          permit.requireCurrent(chunkLoadingManager)
          val entity = retained.source.body.asInstanceOf[ActivePodEntityImpl].rawEntity
          val control = entity.serverControlSnapshot()
          require(ActiveFloatingPodManager.finalResidentCharge(control) == retained.finalEscrowAmount,
            "resident source fee changed after release")
          val artifact = entity.artifactIdentity().orElseThrow() // caller Optional24由原640预付scratch承担。
          require(artifact.bindingUuid() == binding.bindingUuid && artifact.attemptUuid() == retained.source.record.attemptUuid,
            "original committed artifact binding/attempt required")
          finalSaveSource.mint(index, entity, control, artifact, normalStopRequest.get, retained.drift.tag.operationId)
          retained.finalFactMinted = true
          index += 1
          val pod = permit.pod
          managedByPod.remove(pod);cachedStates.remove(pod);dirtyPods.remove(pod);dirtyRevisions.remove(pod)
          chunkLoadingManager.compactResidentRelease(retained.releaseAck.get)
          normalStopBodies.remove(pod)
          retained.source.dropResidentResources()
          retained.cleaned = true
          completedResidents.put(pod, retained.drift.stopCommittedState.get)
        }
        finalSaveSource.requireComplete()
        finalSaveSource
      }

    private[server] def dropFinalMainReferences(): Unit =
      requireOwner()
      normalStopFailure.foreach(throw _)
      val schema6=dbFirstReadOwner.nonEmpty
      require(!finalMainReferencesDropped && finalCohortStarted && finalSaveAdmission.isFinalActive &&
        (if schema6 then dbFirstNormalRecallCompleted.contains(true) else normalRecalls.exists(_.isComplete)) && pending.isEmpty && pendingModes.isEmpty && pendingDrifts.isEmpty && dbFirstRecalls.isEmpty && dbFirstCheckpoints.isEmpty && dbFirstPeriodicSnapshots.isEmpty && dbFirstPoseFailures.isEmpty && dbFirstStopPending.isEmpty && (if schema6 then dbFirstStoppedResidents.valuesIterator.forall(r=>r.cleaned && r.finalAcknowledged) else dbFirstStoppedResidents.isEmpty) && dbFirstStopNormalPending.isEmpty && dbFirstStopRecalls.isEmpty &&
        residentCheckpoints.valuesIterator.forall(value => value.cleaned && value.finalFactMinted),
        "original Main cohort/Recall/plan releases must finish before graph drop")
      normalRecalls.foreach(_.dropCompletedMainReferences())
      normalRecalls = None
      residentCheckpoints.clear();completedResidents.clear();normalStopCensus = None
      originalRecallSource = None;normalRecallFootprints.clear();normalStopBodies.clear();driftRuntime = None
      mainBootReads = None
      dbFirstReadOwner.foreach(_.dropFinalMainPublication(this))
      dbFirstStoppedResidents.clear();dbFirstStopNormalCompleted.clear()
      dbFirstStopCensus=None;dbFirstStopQueueHandoff=None
      mainPodReads = None;dbFirstReadOwner = None
      require(mainPublicationScope.exists(_ eq this), "original manager publication owner required")
      mainPublicationScope = None
      // final source/escrow由Admission/Scope继续保费；本scope不再保其新payload图。
      finalSaveSource = null
      finalSaveEscrow = null
      finalSaveAdmission = null
      finalMainReferencesDropped = true

    private def advanceWorldResidentReleases(): Unit =
      if residentCheckpointsStarted && finalSaveAdmission == null then normalStopStep {
        residentCheckpoints.valuesIterator.filterNot(_.cleaned).toVector.foreach { retained =>
          retained.drift.cause.foreach(throw _)
          if retained.drift.stopProjectionReleased then
            if retained.releasePermit.isEmpty then retained.releasePermit = Some(new ResidentTicketReleasePermit(retained))
            val permit = retained.releasePermit.get
            permit.requireCurrent(chunkLoadingManager)
            if retained.releaseAck.isEmpty then
              retained.releaseAck = Some(chunkLoadingManager.releaseResidentForNormalStop(permit).get)
            require(retained.releaseAck.exists(_.belongsTo(permit)), "foreign resident ticket release ACK")
            // release可能经Forge回调改变实体；先重核原体/冻结，成功后才清本boot运行期映射。
            permit.requireCurrent(chunkLoadingManager)
            val pod = permit.pod
            managedByPod.remove(pod)
            cachedStates.remove(pod)
            dirtyPods.remove(pod)
            dirtyRevisions.remove(pod)
            chunkLoadingManager.compactResidentRelease(retained.releaseAck.get)
            normalStopBodies.remove(pod)
            retained.source.dropResidentResources()
            retained.cleaned = true
            residentCheckpoints.remove(pod)
            completedResidents.put(pod, retained.drift.stopCommittedState.get)
        }
      }

    private[server] def retainedResidentUuids: Set[UUID] =
      requireOwner()
      completedResidents.keySet.toSet

    private def freezeNormalStopBodies(request: UUID): Unit =
      managedByPod.toVector.sortBy(_._1.toString).foreach {
        case (pod, ManagedPodStatus.Active(record)) if hasDbFirstCheckpoint(pod) || hasDbFirstRecall(pod) =>
          // 已Accepted原capture独占冻结；不覆盖其token。原ACK发布后同栈接停服冻结，收回则完成原退场。
          record.entity match
            case body:ActivePodEntityImpl=>
              record.world.assertLiveIndexedBody(body)
              require(body.isFrozen,"inflight DB-first body lost its original freeze")
            case _=>throw new IllegalStateException("inflight DB-first stop body is not the original Forge owner")
        case (pod, ManagedPodStatus.Active(record)) =>
          val body = record.entity match
            case value: NormalStopActivePodEntity => value
            case _ => throw new IllegalStateException("actual active body lacks normal stop control")
          normalStopBodies.get(pod).foreach(previous => require(previous.sameBody(body), "normal stop body replaced"))
          normalStopBodies.put(pod, body) // freeze/mirror抛错仍保原体，不清零重试。
          record.world.assertLiveIndexedBody(body)
          body.freezeForNormalStop(request)
        case (_, _) => throw new IllegalStateException("original manager has pending/faulted physical responsibility")
      }

    private def normalStopStep[A](body: => A): A =
      normalStopFailure.foreach(throw _)
      try body
      catch
        case cause: Throwable => normalStopFailure = Some(cause); throw cause

    private[server] def sealForNormalStop(request: UUID): Unit =
      requireOwner()
      require(!retired && request != null && normalStopRequest.forall(_ == request), "original live stop request required")
      normalStopRequest = Some(request)
      stopPeriodicFlushRequests()
      if dbFirstReadOwner.nonEmpty && dbFirstStopQueueHandoff.isEmpty then
        dbFirstStopQueueHandoff=Some((request,dbFirstSnapshotQueue.toVector))
        dbFirstSnapshotQueue.clear() // 原未Accepted请求已由同stop holder保留；不是保存/排空确认。
      normalStopStep { freezeNormalStopBodies(request) }

    /** 等原publication/drift/mode release ACK；绝不靠清map或callback返回伪造排空。 */
    private[server] def captureNormalStopCensus(request: UUID): Option[NormalStopCensus] =
      requireOwner()
      require(!retired && normalStopRequest.contains(request), "seal original manager before stop capture")
      normalStopStep {
        retainedFailureCauses.headOption.foreach(throw _)
        freezeNormalStopBodies(request)
        if pending.nonEmpty || pendingModes.nonEmpty || pendingDrifts.nonEmpty || dbFirstRecalls.nonEmpty || dbFirstCheckpoints.nonEmpty || dbFirstPeriodicSnapshots.nonEmpty || dbFirstPoseFailures.nonEmpty then None
        else
          if normalStopCensus.isEmpty then
            require(normalStopBodies.keySet == managedByPod.keySet, "normal stop retained body census changed")
            val entries = managedByPod.toVector.sortBy(_._1.toString).map { case (pod, status) =>
              val record = status match
                case ManagedPodStatus.Active(value) => value
                case _ => throw new IllegalStateException("normal stop census contains unsettled physical status")
              val body = normalStopBodies(pod)
              body.requireNormalStopFreeze(request, record.exactState, record.attemptUuid)
              require(body.isFrozen, "normal stop body did not remain frozen")
              val pose = body.actualPose
              val state = record.exactState
              require(pose.worldName == state.worldName && state.dimensionId.contains(pose.dimensionId),
                "normal stop actual body left original world")
              val ticket = chunkLoadingManager.managedTicketRecords().getOrElse(pod,
                throw new IllegalStateException("normal stop original ticket missing")).ticket
              val chunks = chunkLoadingManager.publicationTicketChunks(pod, record.world, ticket)
              require(chunks.size == 25 && chunks == recoveryChunks(pose.dimensionId, pose.posX, pose.posZ) &&
                chunks == recoveryChunks(pose.dimensionId, state.posX, state.posZ), "normal stop actual25 differs from committed locator center")
              new NormalStopEntry(record, body, ticket, pose)
            }
            normalStopCensus = Some(new NormalStopCensus(request, entries))
          normalStopCensus.foreach(_.entries.foreach { entry =>
            require(managedByPod.get(entry.record.exactState.uuid).contains(ManagedPodStatus.Active(entry.record)) &&
              entry.body.actualPose == entry.finalPose, "frozen stop census changed after capture")
            val chunks = chunkLoadingManager.publicationTicketChunks(entry.record.exactState.uuid, entry.record.world, entry.ticket)
            require(chunks == recoveryChunks(entry.finalPose.dimensionId, entry.finalPose.posX, entry.finalPose.posZ),
              "original stop ticket changed after capture")
          })
          normalStopCensus
      }

    private final class DriftRuntime(val adapter: MySqlOwnedIoAdapter,
        val pump: ForgePermanentMainPump, val lease: PermanentPumpLease)
    private final class PendingDrift(val tag: IoOperationTag, val workflow: UUID, val reserve: UUID,
        val request: MySqlTrackedDriftRequest, val revision: Long,
        val world: PodWorld, val body: TrackedDriftActivePodEntity, val ticket: ChunkTicket):
      var plan = Option.empty[MySqlOwnedRepositoryOperationPlan]
      var accepted = false
      var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
      var cause = Option.empty[Throwable]
      var originalHandle = Option.empty[MySqlRetainedPlanHandle]
      var dropped = Option.empty[MySqlRetainedPlanDropReceipt]
      var publishedAt = Option.empty[Long]
      var releaseSubmitted = false
      var stopCommittedState = Option.empty[FloatingPodState]
      var stopProjectionReleased = false
    private var driftRuntime = Option.empty[DriftRuntime]
    private val pendingDrifts = mutable.LinkedHashMap.empty[IoOperationTag, PendingDrift]

    private[server] def requireOriginalDbFirstMain(adapter:MySqlOwnedIoAdapter):Boolean =
      requireOwner()
      dbFirstReadOwner match
        case Some(callbacks)=>callbacks.requireOriginalMainAdapter(adapter);true
        case None=>false
    private[server] def dbFirstSnapshotDrainPending:Boolean =
      requireOwner()
      dbFirstPeriodicSnapshots.nonEmpty || dbFirstCheckpoints.nonEmpty || dbFirstRecalls.nonEmpty

    /** 返回是否为schema6；资格来自原callback当前读代，非容量/调用者Boolean。 */
    private[server] def requireCurrentDbFirstMain(adapter:MySqlOwnedIoAdapter):Boolean =
      requireOwner()
      dbFirstReadOwner match
        case Some(callbacks)=>
          callbacks.requireOriginalMainAdapter(adapter)
          callbacks.requireCurrentReads()
          true
        case None=>false

    /** 仅真实Ready Main持有原scope时装配；本candidate未由formal factory安装。 */
    private[server] def enableTrackedDrift(adapter: MySqlOwnedIoAdapter,
        pump: ForgePermanentMainPump, lease: PermanentPumpLease): Unit =
      requireOwner()
      require(!retired && driftRuntime.isEmpty && adapter != null && pump != null && lease != null &&
        adapter.currentExecutionIdentity == identity && lease.bootUuid == identity.bootId &&
        lease.bootGeneration == identity.bootGeneration, "one actual Main/pump/session drift installation required")
      pump.assertReadyCurrent(lease)
      dbFirstReadOwner match
        case Some(callbacks)=>
          callbacks.requireOriginalMainAdapter(adapter);callbacks.requireCurrentReads()
        case None=>require(adapter.pollAdmissionMaintenance.isCurrent,"actual admission freshness unavailable")
      driftRuntime = Some(new DriftRuntime(adapter, pump, lease))

    private def currentDriftRuntime(): DriftRuntime =
      requireOwner()
      require(!retired, "retired Main scope cannot capture/publish drift")
      val runtime = driftRuntime.getOrElse(throw new IllegalStateException("tracked drift is not installed"))
      runtime.pump.assertReadyCurrent(runtime.lease)
      require(runtime.adapter.pollAdmissionMaintenance.isCurrent, "actual admission windows expired/faulted")
      runtime

    private def driftPose(state: FloatingPodState): FloatingPodPose =
      FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.dimensionId.get)

    private def assertDriftPhysical(entry: PendingDrift): ManagedActivePod =
      val current = managedByPod.get(entry.request.criticalBase.uuid) match
        case Some(ManagedPodStatus.Active(record)) => record
        case _ => throw new IllegalStateException("drift original pod is no longer Active")
      require(current.world.sameWorld(entry.world) && current.attemptUuid == entry.request.attemptUuid &&
        (current.entity match
          case body: TrackedDriftActivePodEntity => body.sameBody(entry.body)
          case _ => false), "drift body/world/attempt ownership changed")
      entry.world.assertLiveIndexedBody(entry.body)
      val actual = chunkLoadingManager.publicationTicketChunks(entry.request.criticalBase.uuid, entry.world, entry.ticket)
      require(actual.size == 25 && actual == recoveryChunks(entry.world.dimensionId,
        entry.body.actualPose.posX, entry.body.actualPose.posZ), "drift requires original exact actual25 ticket")
      current

    /** END真实body观察先于本轮ACK；inflight后的新pose保留新revision，绝不当作旧ACK已写入。 */
    private def observeActualDriftPoses(isolateDbFirst:Boolean=false): Unit =
      managedByPod.toVector.sortBy(_._1.toString).foreach {
        case (pod, ManagedPodStatus.Active(record)) if !dbFirstPoseFailures.contains(pod) && !hasDbFirstRecall(pod) && !hasDbFirstCheckpoint(pod) && !pendingModes.valuesIterator.exists(e =>
            e.sqlRequest.podUuid == pod && e.status != MainControlledPublicationStatus.Published &&
              e.status != MainControlledPublicationStatus.AlreadySet) =>
          def sample(): Unit =
            val body = record.entity match
              case value: TrackedDriftActivePodEntity => value
              case _ => throw new IllegalStateException("actual Active body has no tracked drift adapter")
            record.world.assertLiveIndexedBody(body)
            val pose = body.actualPose
            require(pose.worldName == record.exactState.worldName &&
              record.exactState.dimensionId.contains(pose.dimensionId), "actual drift body left original world/dimension")
            val candidate = record.exactState.copy(posX = pose.posX, posY = pose.posY, posZ = pose.posZ,
              yaw = pose.yaw, pitch = pose.pitch)
            MySqlTrackedDriftRequest.requireSameChunkPoseOnly(binding, record.exactState, candidate, record.attemptUuid)
            val previous = cachedStates.getOrElse(pod, throw new IllegalStateException("actual Active drift cache missing"))
            if !MySqlTrackedDriftMainRules.samePoseBits(previous, candidate) then updateCachedState(pod, candidate).get
          def observe():Unit = normalRecalls match
            case Some(owner) => owner.observeOrdinaryDrift(record)(sample())
            case None => sample()
          if !isolateDbFirst then observe()
          else
            try observe()
            catch case cause:Throwable=>
              dbFirstPoseFailures.put(pod,new DbFirstPoseFailure(record,cause))
              try record.entity match
                case body:ControlledActivePodEntity=>body.markControlFaulted(cause)
                case _=>throw new IllegalStateException("periodic pose failure original controlled body unavailable")
              catch case diagnostic:Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)
              try logger.error("DB-first periodic pose observation failed for pod "+pod+" attempt "+record.attemptUuid,cause)
              catch case diagnostic:Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)
              if !scala.util.control.NonFatal(cause) then throw cause
        case _ => ()
      }

    /** critical caller在任何实体freeze/remove前核；拒绝不修改body/cache/dirty。 */
    private[server] def canBeginPlayerDbFirstRecall(pod: UUID): Boolean =
      requireOwner()
      pod != null && !retired && normalStopRequest.isEmpty &&
        !trackedDriftPendingTagsByPod.contains(pod) && !hasDbFirstRecall(pod) &&
        !hasDbFirstCheckpoint(pod) && !dbFirstPoseFailures.contains(pod) &&
        !pending.valuesIterator.exists(_.receipt.podUuid == pod) &&
        !pendingModes.valuesIterator.exists(_.sqlRequest.podUuid == pod)

    private[server] def assertCriticalTransitionAllowed(pod: UUID): Unit =
      requireOwner()
      require(pod != null && !trackedDriftPendingTagsByPod.contains(pod) && !hasDbFirstRecall(pod) && !hasDbFirstCheckpoint(pod) && !dbFirstPoseFailures.contains(pod),
        "original drift/DB-first recall is pending/unresolved; critical transition cannot invalidate its preimage")

    /** 由现有Main END caller推进；scheduler只置request标记，不进世界/DB。 */
    private[server] def onTrackedDriftEnd(ordinal: Long, phase: PermanentPumpPhase, max: Int): Unit =
      requireOwner()
      require(ordinal > 0L && max > 0 && max <= activeLimit, "bounded actual Main END required")
      if dbFirstReadOwner.nonEmpty then
        onDbFirstActiveSnapshotEnd(ordinal,phase,max)
        return
      driftRuntime.foreach { runtime =>
        if phase == PermanentPumpPhase.Ready && !retired then
          currentDriftRuntime()
          observeActualDriftPoses()
        val readyReleases = pendingDrifts.valuesIterator.filter(e => e.publishedAt.exists(_ < ordinal) &&
          e.dropped.nonEmpty && !e.releaseSubmitted && e.cause.isEmpty).take(max).toVector
        runtime.adapter.drainTrackedDrift(max).foreach { completion =>
          val entry = pendingDrifts.getOrElse(completion.tag, throw new MainPublicationUnmatchedCompletion(completion))
          if entry.releaseSubmitted then
            completion.result match
              case Success(MySqlOwnedIoReply.DriftProjectionReleased(tag)) if tag == entry.tag =>
                entry.stopProjectionReleased = true
                pendingDrifts.remove(tag)
              case Failure(cause) => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_RELEASE_COMPLETION, entry.cause, cause)
              case Success(MySqlOwnedIoReply.DriftProjectionReleaseFailed(_, cause)) => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_RELEASE_COMPLETION, entry.cause, cause)
              case _ => entry.cause = Some(new IllegalStateException("drift release ACK differs from original responsibility"))
          else
            require(entry.accepted && entry.completion.isEmpty, "drift completion must match one original accepted operation")
            entry.completion = Some(completion)
            try
              if phase != PermanentPumpPhase.Ready then throw new IllegalStateException("late drift completion is retained outside Ready")
              currentDriftRuntime()
              completion.result match
                case Success(MySqlOwnedIoReply.TrackedDriftCommitted(graph)) =>
                  require(graph.tag == entry.tag && entry.plan.exists(_ eq graph.operationPlan) && graph.request == entry.request,
                    "drift full20 ACK differs from original tag/plan/request")
                  val current = assertDriftPhysical(entry)
                  val latest = cachedStates.getOrElse(graph.state.uuid, throw new IllegalStateException("drift cache vanished"))
                  val revision = dirtyRevisions.getOrElse(graph.state.uuid, throw new IllegalStateException("drift revision vanished"))
                  val decision = MySqlTrackedDriftMainRules.acknowledge(current.exactState, latest,
                    entry.request, graph.state, entry.revision, revision)
                  require(FloatingPodEntityControl.sameActualPose(entry.body.actualPose, decision.latestCache), "latest actual body differs from Main cached pose")
                  entry.body.applyCommittedTrackedDrift(current.exactState, decision.latestCache, current.attemptUuid)
                  assertDriftPhysical(entry) // actual body/index/ticket再核；失败保原completion，不发release。
                  managedByPod.put(graph.state.uuid, ManagedPodStatus.Active(current.copy(exactState = decision.durableBase)))
                  cachedStates.put(graph.state.uuid, decision.latestCache)
                  if !decision.keepDirty then dirtyPods.remove(graph.state.uuid)
                  residentCheckpoints.get(graph.state.uuid).filter(_.drift eq entry).foreach { retained =>
                    require(!decision.keepDirty && decision.durableBase.operatingMode == PodOperatingMode.WorldIdle &&
                      FloatingPodEntityControl.sameActualPose(retained.source.finalPose, decision.durableBase),
                      "resident stop ACK must commit the exact frozen final pose")
                    entry.stopCommittedState = Some(decision.durableBase)
                  }
                  val handle = entry.plan.flatMap(_.originalRetainedHandle).getOrElse(
                    throw new IllegalStateException("actual drift plan handle missing"))
                  entry.originalHandle = Some(handle) // drop失败仍有原issuer/slot责任，不靠局部变量或GC。
                  entry.plan = None
                  entry.completion = None
                  entry.dropped = Some(runtime.adapter.recordMainPlanDrop(handle))
                  entry.publishedAt = Some(ordinal)
                  trackedDriftPendingTagsByPod.remove(graph.state.uuid)
                case Failure(cause) => throw cause
                case Success(MySqlOwnedIoReply.TrackedDriftFailed(responsibility)) => throw responsibility.cause
                case _ => throw new IllegalStateException("drift rejected/unmatched result retained; no publication/ACK")
            catch
              case cause: Throwable => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_PUBLICATION, entry.cause, cause)
        }
        if phase == PermanentPumpPhase.Ready && !retired then
          currentDriftRuntime()
          readyReleases.foreach { entry =>
            try
              runtime.adapter.submitDriftProjectionRelease(entry.tag, entry.dropped.get) match
                case IoSubmission.Accepted => entry.releaseSubmitted = true
                case IoSubmission.RejectedCapacity => ()
                case IoSubmission.RejectedQuiescing => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_RELEASE_SUBMIT, entry.cause, new IllegalStateException("drift memory ACK quiesced before acceptance"))
                case IoSubmission.RejectedStopped(cause) => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_RELEASE_SUBMIT, entry.cause, cause.getOrElse(new IllegalStateException("drift memory ACK stopped")))
            catch case cause: Throwable => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_RELEASE_SUBMIT, entry.cause, cause)
          }
          if !periodicFlushAdmissionStopped.get() && consumePeriodicFlushRequest() then
            dirtyPods.toVector.sortBy(_.toString).foreach { pod =>
              if !hasDbFirstRecall(pod) && !hasDbFirstCheckpoint(pod) && !trackedDriftPendingTagsByPod.contains(pod) && pendingDrifts.size < activeLimit &&
                !pendingModes.valuesIterator.exists(e => e.sqlRequest.podUuid == pod &&
                  e.status != MainControlledPublicationStatus.Published && e.status != MainControlledPublicationStatus.AlreadySet) then
                val record = managedByPod.get(pod) match
                  case Some(ManagedPodStatus.Active(value)) => value
                  case _ => throw new IllegalStateException("dirty drift pod lacks Active Main rights")
                val body = record.entity match
                  case actual: TrackedDriftActivePodEntity => actual
                  case _ => throw new IllegalStateException("active body has no actual drift control adapter")
                val latest = cachedStates.getOrElse(pod, throw new IllegalStateException("dirty drift cache missing"))
                val ticket = chunkLoadingManager.managedTicketRecords().getOrElse(pod,
                  throw new IllegalStateException("drift exact original ticket missing")).ticket
                val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, UUID.randomUUID())
                val entry = new PendingDrift(tag, UUID.randomUUID(), UUID.randomUUID(),
                  MySqlTrackedDriftRequest(binding, record.exactState, latest, record.attemptUuid),
                  dirtyRevisions.getOrElse(pod, throw new IllegalStateException("dirty revision missing")), record.world, body, ticket)
                pendingDrifts.put(tag, entry)
                trackedDriftPendingTagsByPod.put(pod, tag) // 原IDs先保留，encode/submit失败不可remint。
                try
                  assertDriftPhysical(entry)
                  require(FloatingPodEntityControl.sameActualPose(body.actualPose, latest), "captured desired pose must be actual Main body pose")
                  entry.plan = Some(runtime.adapter.reserveRepositoryPlan(tag, entry.workflow, entry.reserve, entry.request.repositoryOperation))
                catch case cause: Throwable => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_CAPTURE_RESERVE, entry.cause, cause)
            }
          pendingDrifts.valuesIterator.filter(e => !e.accepted && e.cause.isEmpty && e.plan.nonEmpty && e.publishedAt.isEmpty)
            .take(max).foreach { entry =>
              try
                assertDriftPhysical(entry)
                runtime.adapter.submit(entry.tag, MySqlOwnedIoCommand.CommitTrackedDrift(entry.request, entry.plan.get)) match
                  case IoSubmission.Accepted => entry.accepted = true
                  case IoSubmission.RejectedCapacity => () // 同原plan/IDs，dirty不清；下一END可重试未Accepted请求。
                  case IoSubmission.RejectedQuiescing => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_SUBMIT, entry.cause, new IllegalStateException("drift submission quiesced"))
                  case IoSubmission.RejectedStopped(cause) => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_SUBMIT, entry.cause, cause.getOrElse(new IllegalStateException("drift submission stopped")))
              catch case cause: Throwable => entry.cause = recordMainFailure(entry.tag, entry.request.criticalBase.uuid, MainFailureStage.DRIFT_SUBMIT, entry.cause, cause)
            }
      }
      advanceWorldResidentReleases()

    /** 原永久END的schema6周期保存；只由Main排队，worker接收原完整capture请求。 */
    private final class DbFirstPeriodicSnapshot(val pod:UUID,val tag:IoOperationTag):
      var reservation=Option.empty[MySqlDbFirstCaptureReservation]
      var entry=Option.empty[DbFirstCheckpointEntry]
      var captureSubmitted=false
      var completion=Option.empty[IoCompletion[MySqlOwnedIoReply]]
      var cause=Option.empty[Throwable]
      var stopResident=Option.empty[DbFirstStoppedResident]
    private val dbFirstPeriodicSnapshots=mutable.LinkedHashMap.empty[IoOperationTag,DbFirstPeriodicSnapshot]
    private val dbFirstSnapshotQueue=mutable.LinkedHashSet.empty[UUID]
    private var dbFirstStopQueueHandoff=Option.empty[(UUID,Vector[UUID])]
    private var dbFirstStopCensus=Option.empty[NormalStopCensus]
    private val dbFirstStopNormalPending=mutable.LinkedHashMap.empty[UUID,NormalStopEntry]
    private val dbFirstStopNormalCompleted=mutable.LinkedHashSet.empty[UUID]
    private final class DbFirstStopRecall(val source:NormalStopEntry,val tag:IoOperationTag):
      var reservation=Option.empty[MySqlDbFirstCaptureReservation]
      var entry=Option.empty[DbFirstRecallEntry]
      var captureSubmitted=false
      var completion=Option.empty[IoCompletion[MySqlOwnedIoReply]]
      var cause=Option.empty[Throwable]
      val retainedEnd=new DbFirstRecallRetainedEnd
    private val dbFirstStopRecalls=mutable.LinkedHashMap.empty[IoOperationTag,DbFirstStopRecall]
    private[server] def dbFirstStopRetainedEndPending:Boolean =
      requireOwner();dbFirstStopRecalls.valuesIterator.exists(_.retainedEnd.active)
    private val dbFirstStopPending=mutable.LinkedHashMap.empty[UUID,NormalStopEntry]
    private final class DbFirstStoppedResident(val source:NormalStopEntry):
      var checkpoint=Option.empty[DbFirstCheckpointEntry]
      var finalAcknowledged=false
      var permit=Option.empty[ResidentTicketReleasePermit]
      var releaseAck=Option.empty[ChunkLoadingManager#ResidentReleaseAcknowledgement]
      var cleaned=false
    private val dbFirstStoppedResidents=mutable.LinkedHashMap.empty[UUID,DbFirstStoppedResident]

    /** 原endpoint消费同一stop census；各类保持原资源，只有自身真实ACK才推进。 */
    private[server] def beginDbFirstNormalStopCensus(adapter:MySqlOwnedIoAdapter,
        census:ActiveFloatingPodManager#MainPublicationScope#NormalStopCensus):Unit =
      requireOwner()
      requireCurrentDbFirstMain(adapter)
      require(dbFirstReadOwner.nonEmpty && dbFirstStopCensus.isEmpty && normalStopCensus.exists(_ eq census) &&
        dbFirstStopQueueHandoff.exists(_._1==census.request) && normalStopRequest.contains(census.request),
        "original schema6 sealed census and queued-request handoff required once")
      dbFirstStopCensus=normalStopCensus
      normalStopCensus.get.entries.foreach { source=>
        if source.retainedResident then dbFirstStopPending.put(source.podUuid,source)
        else dbFirstStopNormalPending.put(source.podUuid,source)
      }
    private[server] def dbFirstNormalRecallCompleted:Option[Boolean] =
      requireOwner()
      dbFirstStopCensus.map(census=>dbFirstStopNormalPending.isEmpty && dbFirstStopRecalls.isEmpty &&
        census.entries.filterNot(_.retainedResident).forall(source=>dbFirstStopNormalCompleted.contains(source.podUuid)))

    private[server] def ownsDbFirstStopRecall(tag:IoOperationTag):Boolean =
      requireOwner();dbFirstStopRecalls.contains(tag)
    /** endpoint先保留完整drain batch；只认原tag与空inbox，失败责任也继续收实际完成。 */
    private[server] def retainDbFirstStopRecallBatch(batch:Vector[IoCompletion[MySqlOwnedIoReply]]):Unit =
      requireOwner()
      require(batch!=null && batch.size<=activeLimit && !batch.exists(_==null) &&
        batch.map(_.tag).distinct.size==batch.size && batch.forall(c=>dbFirstStopRecalls.get(c.tag).exists(_.completion.isEmpty)),
        "original bounded shutdown recall completion batch required")
      batch.foreach(c=>dbFirstStopRecalls(c.tag).completion=Some(c))

    private def failDbFirstStopRecall(work:DbFirstStopRecall,cause:Throwable):Unit =
      val first=work.cause.isEmpty
      work.cause=appendMainFailure(work.cause,cause)
      if work.entry.isEmpty then
        work.entry=dbFirstRecalls.get(work.tag)
        if work.entry.nonEmpty then work.reservation=None
      work.entry match
        case Some(entry)=>dbFirstMainFailed(entry,cause)
        case None=>if first then
          logger.error("DB-first Normal stop recall admission failed for pod "+work.source.podUuid+
            " operation "+work.tag.operationId,cause)
          try work.source.body.markControlFaulted(cause)
          catch case diagnostic:Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)

    /** 只消费真实永久END；原complete/physical/release顺序与玩家收回共用，不生成手工tick。 */
    private[server] def onDbFirstNormalStopEnd(adapter:MySqlOwnedIoAdapter,tick:PermanentEndTick):Unit =
      requireOwner()
      if dbFirstStopCensus.isEmpty then return
      requireOriginalMainAdapter(adapter)
      require(tick!=null && tick.bootUuid==identity.bootId && tick.bootGeneration==identity.bootGeneration,
        "original shutdown recall permanent END identity required")
      if tick.phase!=PermanentPumpPhase.Ready || retired then return
      // 先推进已接受原工作，即使其他Pod失败也不丢实际ACK。
      dbFirstStopRecalls.valuesIterator.filter(work=>work.cause.isEmpty || work.retainedEnd.active).toVector.foreach { work=>
        try
          val entry=work.entry.getOrElse(throw new IllegalStateException("original shutdown recall Main entry missing"))
          work.completion.foreach { completion=>
            completion.result.failed.foreach(throw _)
            if work.retainedEnd.awaitingAck || entry.releaseSubmitted then
              if work.retainedEnd.awaitingAck then
                work.retainedEnd.finish(finishRetainedDbFirstRecallRelease(adapter,entry,completion))
              else finishDbFirstRecallRelease(adapter,entry,completion)
              dbFirstStopNormalCompleted.add(work.source.podUuid)
              dbFirstStopRecalls.remove(work.tag)
            else
              require(work.captureSubmitted,"original shutdown recall submit receipt missing")
              val retainedFailure=DbFirstRecallCompletionHandoff.accept(completeDbFirstRecall(adapter,entry,completion)) {
                work.completion=None
              }
              retainedFailure.foreach { original=>
                work.retainedEnd.accepted(tick.endOrdinal)
                failDbFirstStopRecall(work,original)
              }
            work.completion=None
          }
          if dbFirstStopRecalls.contains(work.tag) && work.retainedEnd.active then
            work.retainedEnd.advance(tick.endOrdinal) {
              advanceRetainedDbFirstRecall(adapter,entry,tick).nonEmpty
            } {
              handoffRetainedDbFirstRecall(adapter,entry)
            } {
              submitRetainedDbFirstRecallRelease(adapter,entry,tick)
            }
          else if dbFirstStopRecalls.contains(work.tag) && work.cause.isEmpty && entry.confirmation.nonEmpty && !entry.releaseSubmitted then
            advanceDbFirstRecall(adapter,entry,tick).foreach {
              case IoSubmission.Accepted | IoSubmission.RejectedCapacity=>()
              case IoSubmission.RejectedQuiescing=>throw new IllegalStateException("original shutdown recall release quiesced")
              case IoSubmission.RejectedStopped(cause)=>throw cause.getOrElse(new IllegalStateException("original shutdown recall release stopped"))
            }
        catch case cause:Throwable=>
          work.retainedEnd.stop()
          failDbFirstStopRecall(work,cause)
          if !scala.util.control.NonFatal(cause) then throw cause
      }
      // 非fatal原失败保留并由外层正常关闭gate报告；不为失败work重mint或重放SQL。
      if retainedFailureCauses.isEmpty && dbFirstStopNormalPending.nonEmpty && adapter.canReserveDbFirstCapture then
        val runtime=driftRuntime.getOrElse(throw new IllegalStateException("original shutdown permanent pump owner missing"))
        runtime.pump.assertReadyCurrent(runtime.lease)
        val (pod,source)=dbFirstStopNormalPending.head
        val tag=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,UUID.randomUUID())
        val work=new DbFirstStopRecall(source,tag)
        dbFirstStopRecalls.put(tag,work) // 原census先交caller，再reserve/freeze/capture。
        dbFirstStopNormalPending.remove(pod)
        try
          work.reservation=Some(adapter.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.RECALL))
          work.entry=Some(beginDbFirstRecallForNormalStop(adapter,work.reservation.get,source))
          work.reservation=None
          val result=adapter.submit(tag,MySqlOwnedIoCommand.ExecuteDbFirstRecall(work.entry.get.reservation))
          require(result==IoSubmission.Accepted,"original shutdown recall was not accepted; frozen responsibility retained")
          work.captureSubmitted=true
        catch case cause:Throwable=>
          failDbFirstStopRecall(work,cause)
          if !scala.util.control.NonFatal(cause) then throw cause

    private def advanceDbFirstResidentReleases():Unit =
      dbFirstStoppedResidents.valuesIterator.filter(r=>r.finalAcknowledged && !r.cleaned).toVector.foreach { retained=>
        val checkpoint=retained.checkpoint.get
        checkpoint.cause.foreach(throw _)
        if retained.permit.isEmpty then retained.permit=Some(new ResidentTicketReleasePermit(retained))
        val permit=retained.permit.get
        permit.requireCurrent(chunkLoadingManager)
        if retained.releaseAck.isEmpty then
          retained.releaseAck=Some(chunkLoadingManager.releaseResidentForNormalStop(permit).get)
        require(retained.releaseAck.exists(_.belongsTo(permit)),"foreign schema6 resident release ACK")
        permit.requireCurrent(chunkLoadingManager)
        val state=checkpoint.published.get
        managedByPod.remove(state.uuid);cachedStates.remove(state.uuid)
        dirtyPods.remove(state.uuid);dirtyRevisions.remove(state.uuid)
        chunkLoadingManager.compactResidentRelease(retained.releaseAck.get)
        normalStopBodies.remove(state.uuid)
        retained.source.dropResidentResources()
        retained.cleaned=true
        completedResidents.put(state.uuid,state)
        // 原body/committed checkpoint仍由此stop holder保留，待实际late-save producer接管，不签allDropped。
      }

    /** 原观察失败保留真实body/world/attempt与cause；不迁移ticket、不mint SQL operation。 */
    private final class DbFirstPoseFailure(val record:ManagedActivePod,val cause:Throwable)
    private val dbFirstPoseFailures=mutable.LinkedHashMap.empty[UUID,DbFirstPoseFailure]
    private[server] def observeDbFirstPeriodicPoses():Unit =
      requireOwner()
      observeActualDriftPoses(isolateDbFirst=true)


    private[server] def ownsDbFirstPeriodicSnapshot(tag:IoOperationTag):Boolean =
      requireOwner();dbFirstPeriodicSnapshots.contains(tag)
    /** 先保留整批实际drain结果，再由后续END消费；分支由原提交状态决定，不相信reply自称阶段。 */
    private[server] def retainDbFirstPeriodicSnapshotBatch(batch:Vector[IoCompletion[MySqlOwnedIoReply]]):Unit =
      requireOwner()
      require(batch!=null && batch.size<=activeLimit && !batch.exists(_==null) &&
        batch.map(_.tag).distinct.size==batch.size && batch.forall(c=>
          dbFirstPeriodicSnapshots.get(c.tag).exists(_.completion.isEmpty)),
        "original bounded accepted snapshot completion batch required")
      batch.foreach(c=>dbFirstPeriodicSnapshots(c.tag).completion=Some(c))

    private def failPeriodicSnapshot(work:DbFirstPeriodicSnapshot,cause:Throwable):Unit =
      work.cause=appendMainFailure(work.cause,cause)
      if work.entry.isEmpty then
        // begin失败仍回接已经登记的原Main Entry；分配/验证更早失败则保留原reservation。
        work.entry=dbFirstCheckpoints.get(work.tag)
        if work.entry.nonEmpty then work.reservation=None
      work.entry match
        case Some(entry)=>if entry.cause.isEmpty then checkpointMainFailed(entry,cause)
        case None=>
          logger.error("DB-first periodic snapshot admission/capture failed for pod "+work.pod+" operation "+work.tag.operationId,cause)
          managedByPod.get(work.pod).foreach {
            case ManagedPodStatus.Active(record)=>
              try record.entity match
                case body:ControlledActivePodEntity=>body.markControlFaulted(cause)
                case _=>throw new IllegalStateException("periodic snapshot original controlled body unavailable")
              catch case diagnostic:Throwable=>if diagnostic ne cause then cause.addSuppressed(diagnostic)
            case _=>()
          }

    private def onDbFirstActiveSnapshotEnd(ordinal:Long,phase:PermanentPumpPhase,max:Int):Unit =
      val runtime=driftRuntime.getOrElse(throw new IllegalStateException("original schema6 periodic snapshot runtime missing"))
      requireOriginalMainAdapter(runtime.adapter)
      // 处理已保留的原ACK不回借失效的旧Main读代，也不先要求Ready freshness。
      dbFirstPeriodicSnapshots.valuesIterator.filter(_.cause.isEmpty).take(max).toVector.foreach { work=>
        if work.cause.isEmpty then
          try
            val entry=work.entry.getOrElse(throw new IllegalStateException("periodic snapshot original Main entry missing"))
            work.completion.foreach { completion=>
              completion.result.failed.foreach(throw _)
              if entry.finalSubmitted then
                finishDbFirstCheckpointFinal(runtime.adapter,entry,completion)
                work.stopResident.foreach(_.finalAcknowledged=true)
                dbFirstPeriodicSnapshots.remove(work.tag)
              else if entry.readReleaseSubmitted then finishDbFirstCheckpointReadRelease(runtime.adapter,entry,completion)
              else
                require(work.captureSubmitted,"original snapshot submit receipt missing")
                completeDbFirstCheckpoint(runtime.adapter,entry,completion)
              work.completion=None
            }
            if dbFirstPeriodicSnapshots.contains(work.tag) then
              if entry.finalReceipt.nonEmpty && !entry.finalSubmitted then submitDbFirstCheckpointFinal(runtime.adapter,entry)
              else if entry.confirmation.nonEmpty && !entry.readReleaseSubmitted then
                submitDbFirstCheckpointReadRelease(runtime.adapter,entry)
          catch case cause:Throwable=>
            failPeriodicSnapshot(work,cause)
            if !scala.util.control.NonFatal(cause) then throw cause
      }
      if phase==PermanentPumpPhase.Ready && !retired && !periodicFlushAdmissionStopped.get() then
        runtime.pump.assertReadyCurrent(runtime.lease)
        observeDbFirstPeriodicPoses()
        dbFirstSnapshotQueue.filterInPlace(pod=>managedByPod.get(pod).exists {
          case ManagedPodStatus.Active(_)=> !dbFirstPoseFailures.contains(pod)
          case _=>false
        })
        if consumePeriodicFlushRequest() then
          // 完整NBT/capability可在pose不变时变化，周期覆盖所有活动Pod而不只看dirty pose。
          val active=managedByPod.iterator.collect { case (pod,ManagedPodStatus.Active(_)) if !dbFirstPoseFailures.contains(pod)=>pod }.toVector.sortBy(_.toString)
          require(active.size<=activeLimit,"actual periodic snapshot census exceeds original active limit")
          dbFirstSnapshotQueue.addAll(active)
        // 原Full27只有一个successor读代交接；下一笔须等其实际ACK，容量由原adapter再次核验。
        if dbFirstSnapshotQueue.nonEmpty && dbFirstPeriodicSnapshots.size<activeLimit && runtime.adapter.canReserveDbFirstCapture then
          dbFirstSnapshotQueue.find(pod => !hasDbFirstRecall(pod) && !hasDbFirstCheckpoint(pod) &&
            !trackedDriftPendingTagsByPod.contains(pod) && !pending.valuesIterator.exists(_.receipt.podUuid==pod) &&
            !pendingModes.valuesIterator.exists(_.sqlRequest.podUuid==pod)).foreach { pod=>
            startDbFirstFullSnapshot(runtime,pod,None)
          }
      if phase==PermanentPumpPhase.Ready && !retired && dbFirstStopCensus.nonEmpty then
        runtime.pump.assertReadyCurrent(runtime.lease)
        normalStopStep {
          advanceDbFirstResidentReleases()
          if dbFirstStopPending.nonEmpty && runtime.adapter.canReserveDbFirstCapture then
            val (pod,source)=dbFirstStopPending.head
            source.body.requireNormalStopFreeze(normalStopRequest.get,source.base,source.attemptUuid)
            require(source.body.actualPose==source.finalPose,"original stop resident pose changed")
            updateCachedState(pod,source.base.copy(posX=source.finalPose.posX,posY=source.finalPose.posY,
              posZ=source.finalPose.posZ,yaw=source.finalPose.yaw,pitch=source.finalPose.pitch)).get
            val retained=new DbFirstStoppedResident(source)
            dbFirstStoppedResidents.put(pod,retained)
            dbFirstStopPending.remove(pod) // 原resident holder已登记，再reserve/capture。
            startDbFirstFullSnapshot(runtime,pod,Some(retained))
        }

    /** 两种真实Main producer共用原capture链；caller holder必须先于任何allocation/freeze。 */
    private def startDbFirstFullSnapshot(runtime:DriftRuntime,pod:UUID,
        stopResident:Option[DbFirstStoppedResident]):Unit =
      val tag=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,UUID.randomUUID())
      val work=new DbFirstPeriodicSnapshot(pod,tag)
      work.stopResident=stopResident
      dbFirstPeriodicSnapshots.put(tag,work)
      dbFirstSnapshotQueue.remove(pod)
      try
        work.reservation=Some(runtime.adapter.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT))
        val record=managedByPod(pod) match
          case ManagedPodStatus.Active(value)=>value
          case _=>throw new IllegalStateException("queued snapshot Pod is no longer active")
        work.entry=Some(beginDbFirstCheckpoint(runtime.adapter,work.reservation.get,pod,record.exactState.stateVersion))
        stopResident.foreach(_.checkpoint=work.entry)
        work.reservation=None
        val result=runtime.adapter.submit(tag,MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(work.entry.get.reservation.get))
        require(result==IoSubmission.Accepted,"original checkpoint was not accepted; frozen responsibility retained")
        work.captureSubmitted=true
      catch case cause:Throwable=>
        failPeriodicSnapshot(work,cause)
        stopResident.foreach(_.checkpoint=work.entry)
        if !scala.util.control.NonFatal(cause) then throw cause

    private[server] def hasTrackedDriftRuntime: Boolean =
      requireOwner()
      driftRuntime.nonEmpty
    /** 同Main责任边界一次记录完整原cause；后续cause只附suppressed，不覆盖Entry主因。 */
    private enum MainFailureStage:
      case DRIFT_CAPTURE_RESERVE, DRIFT_SUBMIT, DRIFT_PUBLICATION, DRIFT_RELEASE_SUBMIT,
        DRIFT_RELEASE_COMPLETION, CONTROLLED_PUBLICATION, MODE_PUBLICATION

    private def appendMainFailure(previous: Option[Throwable], cause: Throwable): Option[Throwable] =
      previous match
        case Some(primary) =>
          if (primary ne cause) && !primary.getSuppressed.contains(cause) &&
            !cause.getSuppressed.contains(primary) && (cause.getCause ne primary) then primary.addSuppressed(cause)
          previous
        case None => Some(cause)

    private def recordMainFailure(tag: IoOperationTag, podUuid: UUID, stage: MainFailureStage,
        previous: Option[Throwable], cause: Throwable): Option[Throwable] =
      requireOwner()
      require(tag != null && podUuid != null && stage != null && cause != null, "exact Main Entry failure context required")
      identity.requireTag(tag)
      previous match
        case Some(primary) =>
          if (primary ne cause) && !primary.getSuppressed.contains(cause) &&
            !cause.getSuppressed.contains(primary) && (cause.getCause ne primary) then primary.addSuppressed(cause)
          previous
        case None =>
          try logger.error(s"Main retained failure boot=${tag.bootId} bootGeneration=${tag.bootGeneration} connectionGeneration=${tag.connectionGeneration} operation=${tag.operationId} pod=$podUuid stage=$stage", cause)
          catch
            case diagnosticFailure: Throwable =>
              if diagnosticFailure ne cause then cause.addSuppressed(diagnosticFailure)
          Some(cause)

    private[server] def retainedFailureCauses: Vector[Throwable] =
      requireOwner()
      pending.valuesIterator.flatMap(_.cause).toVector ++ pendingModes.valuesIterator.flatMap(_.cause).toVector ++
        pendingDrifts.valuesIterator.flatMap(_.cause).toVector ++ dbFirstRecalls.valuesIterator.flatMap(_.cause).toVector ++ dbFirstCheckpoints.valuesIterator.flatMap(_.cause).toVector ++ dbFirstPeriodicSnapshots.valuesIterator.filter(_.entry.isEmpty).flatMap(_.cause).toVector ++ dbFirstPoseFailures.valuesIterator.map(_.cause).toVector ++
        dbFirstStopRecalls.valuesIterator.filter(_.entry.isEmpty).flatMap(_.cause).toVector ++ normalStopFailure.toVector

    /** 真实Main Entry原cause优先于stop上下文；只保关联，不清Entry/completion/body/ticket。 */
    private[server] def stopResponsibilityCause(context: Throwable): Throwable =
      requireOwner()
      require(context != null, "actual stop context required")
      val causes = retainedFailureCauses
      val primary = causes.headOption.getOrElse(context)
      causes.drop(1).foreach(cause => appendMainFailure(Some(primary), cause))
      if primary ne context then appendMainFailure(Some(primary), context)
      primary

    private[server] def driftFailureSnapshot: Map[IoOperationTag, Throwable] =
      requireOwner()
      pendingDrifts.valuesIterator.flatMap(entry => entry.cause.map(entry.tag -> _)).toMap
    private[server] def retainedAsyncResponsibilityCount: Int =
      requireOwner()
      pending.size + pendingModes.size + pendingDrifts.size + dbFirstRecalls.size + dbFirstCheckpoints.size + dbFirstPeriodicSnapshots.valuesIterator.count(_.entry.isEmpty) + dbFirstSnapshotQueue.size + dbFirstPoseFailures.size + dbFirstStopPending.size + dbFirstStoppedResidents.size +
        dbFirstStopNormalPending.size + dbFirstStopRecalls.valuesIterator.count(_.entry.isEmpty) +
        dbFirstStopQueueHandoff.filter(_=>dbFirstStopCensus.isEmpty).map(_._2.size).getOrElse(0) + residentCheckpoints.valuesIterator.count(!_.cleaned)
    private[server] def retainedDriftPods: Set[UUID] =
      requireOwner()
      pendingDrifts.valuesIterator.map(_.request.criticalBase.uuid).toSet


    private def requireOwner(): Unit =
      requirePublicationMain()
      require(mainPublicationScope.exists(_ eq this), "foreign manager publication scope")

    def retainControlled(tag: IoOperationTag, receipt: UnboundPreparationReceipt, request: DebugItemEndpoint.Request,
        session: MainControlledPublicationSession, world: PodWorld, body: ControlledActivePodEntity,
        ticket: ChunkTicket, actualRuntimeGeneration: Long): Unit =
      retainControlled(tag,receipt,new MainPodOperationRequest.Player(request),session,world,body,ticket,actualRuntimeGeneration)

    def retainControlled(tag: IoOperationTag, receipt: UnboundPreparationReceipt, request: MainPodOperationRequest,
        session: MainControlledPublicationSession, world: PodWorld, body: ControlledActivePodEntity,
        ticket: ChunkTicket, actualRuntimeGeneration: Long): Unit =
      requireOwner()
      require(!retired && actualRuntimeGeneration == runtimeGeneration, "publication runtime is retired/stale")
      identity.requireTag(tag)
      require(receipt != null && receipt.binding == binding && receipt.origin == UnboundIntentOrigin.DebugItemCreate &&
        receipt.observedIdentity.isEmpty && request != null && request.action() == DebugItemEndpoint.Action.PLACE &&
        request.boot() == identity.bootId && receipt.createOperationUuid.contains(request.operation()),
        "retain requires actual controlled receipt and original operation/session")
      require(request.worldName() == receipt.targetPose.worldName &&
        request.dimension() == receipt.targetPose.dimensionId && request.placement() != null &&
        request.placement().x() == receipt.targetPose.posX && request.placement().y() == receipt.targetPose.posY &&
        request.placement().z() == receipt.targetPose.posZ && request.placement().yaw() == receipt.targetPose.yaw &&
        receipt.targetPose.pitch == 0F, "actual receipt differs from original placement pose/world")
      require(session != null && world != null && body != null && ticket != null, "exact retained physical responsibility required")
      session.assertCurrent(request, world)
      require(!pending.contains(tag) && !pending.values.exists(_.receipt.podUuid == receipt.podUuid) &&
        pending.size < activeLimit, "bounded one-shot pod/operation publication responsibility")
      validatePhysical(receipt, world, body, ticket)
      require(!managedByPod.contains(receipt.podUuid), "prepared pod already has manager rights")
      pending.put(tag, new Pending(tag, receipt, request, session, world, body, ticket))

    /** 原capture已完成后登记Main物理责任；旧Full20 graph不是初建确认来源。 */
    def retainDbFirstInitial(reservation:MySqlDbFirstCaptureReservation,snapshot:DbFirstPodSnapshot,
        receipt:UnboundPreparationReceipt,request:MainPodOperationRequest,
        session:MainControlledPublicationSession,world:PodWorld,body:ControlledActivePodEntity,
        ticket:ChunkTicket,actualRuntimeGeneration:Long):Unit =
      requireOwner()
      require(reservation!=null && snapshot!=null && receipt!=null &&
        reservation.purpose==DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION,
        "original initial capture and prepared physical responsibility required")
      val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main read owner missing"))
      callbacks.requireMainPublication(identity,binding,activeLimit)
      val captured=callbacks.adapter.dbFirstInitialActivationRequest(reservation,snapshot)
      val pose=receipt.targetPose
      require(captured.bindingUuid==binding.bindingUuid && captured.podUuid==receipt.podUuid &&
        captured.attemptUuid==receipt.attemptUuid && receipt.createOperationUuid.contains(captured.creationOperationUuid) &&
        captured.capturedStateVersion==receipt.preparedVersion && captured.ownerUuid.isEmpty &&
        captured.pose.worldName()==pose.worldName && captured.pose.dimension()==pose.dimensionId &&
        captured.pose.x()==pose.posX && captured.pose.y()==pose.posY && captured.pose.z()==pose.posZ &&
        captured.pose.yaw()==pose.yaw && captured.pose.pitch()==pose.pitch,
        "original initial capture differs from retained preparation/body")
      retainControlled(reservation.tag,receipt,request,session,world,body,ticket,actualRuntimeGeneration)
      pending(reservation.tag).initialReservation=Some(reservation)

    /** 仅实际drain返回的完成；重复拒绝，不覆盖原completion/retained责任。 */
    def consume(completion: IoCompletion[MySqlOwnedIoReply], actualRuntimeGeneration: Long): MainControlledPublicationObservation =
      requireOwner()
      require(completion != null, "actual completion required")
      val entry = pending.getOrElse(completion.tag,
        throw new MainPublicationUnmatchedCompletion(completion))
      require(entry.completion.isEmpty, "publication completion was already consumed")
      entry.completion = Some(completion) // consumption责任先retained，即使下面validation失败也不丢。
      if retired || actualRuntimeGeneration != runtimeGeneration then
        entry.status = MainControlledPublicationStatus.Retired
        entry.cause = Some(new IllegalStateException("Late publication belongs to retired runtime generation"))
        completion.result match
          case Success(MySqlOwnedIoReply.ControlledDebugCommitted(graph)) if graph != null && graph.hasPrivateReadbackProof &&
            graph.tag == entry.tag && graph.prepared.receipt == entry.receipt =>
            entry.view = viewOf(graph.state, DebugPodPhase.Faulted, DebugItemEndpoint.Reason.RECOVERY_PENDING)
          case Success(MySqlOwnedIoReply.ControlledUnboundCommitted(graph)) if graph != null && graph.tag == entry.tag && graph.receipt == entry.receipt =>
            entry.view = viewOf(graph.state, DebugPodPhase.Faulted, DebugItemEndpoint.Reason.RECOVERY_PENDING)
          case Success(MySqlOwnedIoReply.ControlledUnboundReadbackFailed(value)) if value.tag == entry.tag && value.receipt == entry.receipt =>
            value.actualCommit match
              case UnboundCommitOutcome.Committed(state) => entry.view = viewOf(state, DebugPodPhase.Faulted, DebugItemEndpoint.Reason.RECOVERY_PENDING)
              case UnboundCommitOutcome.AlreadyCommitted(state) => entry.view = viewOf(state, DebugPodPhase.Faulted, DebugItemEndpoint.Reason.RECOVERY_PENDING)
              case _ => ()
          case _ => entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.RECOVERY_PENDING)
      else
        try
          completion.result match
            case Failure(cause) => throw cause // fatal after-ACK carries typed suppressed obligation, retained unchanged.
            case Success(MySqlOwnedIoReply.ControlledUnboundReadbackFailed(obligation)) =>
              require(obligation.tag == entry.tag && obligation.receipt == entry.receipt, "after-ACK responsibility differs")
              val state = obligation.actualCommit match
                case UnboundCommitOutcome.Committed(value) => value
                case UnboundCommitOutcome.AlreadyCommitted(value) => value
                case _ => throw new IllegalStateException("after-ACK obligation lacks committed result")
              entry.view = viewOf(state, DebugPodPhase.Faulted, DebugItemEndpoint.Reason.RECOVERY_PENDING)
              throw obligation.cause
            case Success(MySqlOwnedIoReply.DbFirstInitialActivationObserved(_)) => publishDbFirstInitial(entry,completion)
            case Success(MySqlOwnedIoReply.ControlledDebugCommitted(graph)) if entry.initialReservation.isEmpty => publishControlledDebug(entry, graph)
            case Success(MySqlOwnedIoReply.ControlledUnboundCommitted(_)) =>
              throw new UnsupportedOperationException("formal F3 publication cannot accept a legacy F2 graph")
            case Success(MySqlOwnedIoReply.ControlledUnboundRejected(_)) =>
              throw new IllegalStateException("Controlled critical commit was rejected; retained physical responsibility is not cleanup ACK")
            case Success(_) => throw new IllegalStateException("Completion is not a controlled committed graph")
        catch
          case cause: Throwable =>
            entry.cause = recordMainFailure(entry.tag, entry.receipt.podUuid, MainFailureStage.CONTROLLED_PUBLICATION, entry.cause, cause)
            entry.status = MainControlledPublicationStatus.Faulted
            entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.FAULTED)
            // 原body保留；不remove、不release ticket、不覆盖其他pod的manager record。
            try entry.body.markControlFaulted(cause)
            catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
      observation(entry)

    private def publish(entry: Pending, graph: MySqlCommittedUnboundGraph): Unit =
      require(graph != null && graph.tag == entry.tag && graph.receipt == entry.receipt, "exact worker graph/tag/receipt mismatch")
      entry.session.assertCurrent(entry.request, entry.world)
      validatePhysical(entry.receipt, entry.world, entry.body, entry.ticket)
      val state = graph.state
      val locator = graph.locator
      require(state.uuid == entry.receipt.podUuid && state.ownerUuid.isEmpty && state.operatingMode == PodOperatingMode.WorldIdle &&
        state.isActive && state.stateVersion == Math.addExact(entry.receipt.preparedVersion, 1L) &&
        locator.podUuid == state.uuid && locator.attemptUuid == entry.receipt.attemptUuid &&
        locator.state == PhysicalRecoveryLocatorState.RowActive && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
        locator.domainVersion.contains(state.stateVersion) && locator.targetDimensionId == state.dimensionId &&
        locator.chunks.size == 25 && locator.chunks.toSet == recoveryChunks(entry.world.dimensionId, state.posX, state.posZ),
        "actual committed row and complete locator mismatch")
      require(graph.inventory.bootUuid == identity.bootId && graph.inventory.bootGeneration == identity.bootGeneration &&
        graph.inventory.connectionGeneration == identity.connectionGeneration && graph.inventory.recovery.binding.contains(binding),
        "worker inventory has foreign runtime identity")
      require(!managedByPod.contains(state.uuid), "manager UUID rights already owned")
      require(managedByPod.size < activeLimit && managedByPod.size < 256, "Main global managed capacity exceeded")
      require(!managedByPod.values.exists(_.record.entity.entityId == entry.body.entityId), "entity ID already owned by manager")
      ensureNoActiveEntityIdCollision(entry.body.entityId, state.uuid)
      require(!managedByPod.values.exists(record => record.record.entity match
        case controlled: ControlledActivePodEntity => controlled.sameBody(entry.body)
        case _ => false), "physical body already owned by manager")
      // 不调用publishRecovered/requireExactActiveLocator：它们仍是旧同步SQL路径。
      entry.body.applyCommittedControl(state, entry.receipt)
      validatePhysical(entry.receipt, entry.world, entry.body, entry.ticket)
      managedByPod.put(state.uuid, ManagedPodStatus.Active(ManagedActivePod(entry.body, entry.world, state, entry.receipt.attemptUuid)))
      cachedStates.put(state.uuid, state)
      dirtyPods.remove(state.uuid)
      entry.status = MainControlledPublicationStatus.Published
      entry.view = viewOf(state, DebugPodPhase.Committed, DebugItemEndpoint.Reason.NONE)

    private def publishDbFirstInitial(entry:Pending,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
      val reservation=entry.initialReservation.getOrElse(throw new IllegalStateException("initial completion has no original retained capture"))
      val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main owner missing"))
      callbacks.requireOriginalMainAdapter(callbacks.adapter)
      entry.session.assertCurrent(entry.request,entry.world)
      validatePhysical(entry.receipt,entry.world,entry.body,entry.ticket)
      val confirmation=callbacks.adapter.claimDbFirstInitialActivationConfirmation(reservation,completion)
      entry.initialConfirmation=Some(confirmation)
      confirmation.requireOriginal(reservation)
      require(confirmation.bindingUuid==binding.bindingUuid && confirmation.podUuid==entry.receipt.podUuid &&
        confirmation.attemptUuid==entry.receipt.attemptUuid &&
        entry.receipt.createOperationUuid.contains(confirmation.creationOperationUuid) &&
        confirmation.ownerUuid.isEmpty && confirmation.capturedStateVersion==0L && confirmation.committedStateVersion==1L,
        "initial confirmation differs from original prepared creation")
      val data=callbacks.adapter.initialPublicationData(reservation,confirmation,completion)
      val state=data.state(confirmation.podUuid).getOrElse(throw new IllegalStateException("confirmed initial domain missing"))
      val locator=data.locators.find(_.podUuid==state.uuid).getOrElse(throw new IllegalStateException("confirmed initial locator missing"))
      val footprint=data.activeAttemptFootprints.getOrElse(state.uuid,throw new IllegalStateException("confirmed initial footprint missing"))
      require(data.binding.contains(binding) && state.isActive && state.ownerUuid.isEmpty &&
        state.operatingMode==PodOperatingMode.WorldIdle && state.stateVersion==confirmation.committedStateVersion &&
        state.worldName==entry.receipt.targetPose.worldName && state.dimensionId.contains(entry.world.dimensionId) &&
        state.posX==entry.receipt.targetPose.posX && state.posY==entry.receipt.targetPose.posY && state.posZ==entry.receipt.targetPose.posZ &&
        state.yaw==entry.receipt.targetPose.yaw && state.pitch==entry.receipt.targetPose.pitch &&
        locator.attemptUuid==confirmation.attemptUuid && locator.state==PhysicalRecoveryLocatorState.RowActive &&
        locator.reasonCode==PhysicalRecoveryLocatorReason.RowActive && locator.domainVersion.contains(state.stateVersion) &&
        locator.targetDimensionId==state.dimensionId && locator.chunks.size==25 &&
        locator.chunks.toSet==recoveryChunks(entry.world.dimensionId,state.posX,state.posZ) && footprint._1==confirmation.attemptUuid,
        "actual initial full readback domain/locator/footprint differs")
      require(!managedByPod.contains(state.uuid) && managedByPod.size<activeLimit && managedByPod.size<256 &&
        data.activeCount<=activeLimit,"initial manager identity or configured capacity unavailable")
      require(!managedByPod.values.exists(_.record.entity.entityId==entry.body.entityId),"initial entity ID already owned by manager")
      ensureNoActiveEntityIdCollision(entry.body.entityId,state.uuid)
      require(!managedByPod.values.exists(record=>record.record.entity match
        case controlled:ControlledActivePodEntity=>controlled.sameBody(entry.body)
        case _=>false),"initial body already owned by manager")
      entry.body.applyCommittedControl(state,entry.receipt)
      validatePhysical(entry.receipt,entry.world,entry.body,entry.ticket)
      normalRecallFootprints.put(state.uuid,footprint)
      managedByPod.put(state.uuid,ManagedPodStatus.Active(ManagedActivePod(entry.body,entry.world,state,confirmation.attemptUuid)))
      cachedStates.put(state.uuid,state)
      dirtyPods.remove(state.uuid)
      entry.status=MainControlledPublicationStatus.Published
      entry.view=viewOf(state,DebugPodPhase.Committed,DebugItemEndpoint.Reason.NONE)

    private def publishControlledDebug(entry: Pending, graph: MySqlControlledDebugCommittedGraph): Unit =
      require(graph != null && graph.tag == entry.tag && graph.hasPrivateReadbackProof && graph.prepared.receipt == entry.receipt, "exact worker graph/tag/receipt mismatch")
      entry.session.assertCurrent(entry.request, entry.world)
      validatePhysical(entry.receipt, entry.world, entry.body, entry.ticket)
      val state = graph.state
      val locator = graph.locator
      require(state.uuid == entry.receipt.podUuid && state.ownerUuid.isEmpty && state.operatingMode == PodOperatingMode.WorldIdle &&
        state.isActive && state.stateVersion == Math.addExact(entry.receipt.preparedVersion, 1L) &&
        locator.podUuid == state.uuid && locator.attemptUuid == entry.receipt.attemptUuid &&
        locator.state == PhysicalRecoveryLocatorState.RowActive && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
        locator.domainVersion.contains(state.stateVersion) && locator.targetDimensionId == state.dimensionId &&
        locator.chunks.size == 25 && locator.chunks.toSet == recoveryChunks(entry.world.dimensionId, state.posX, state.posZ),
        "actual committed row and complete locator mismatch")
      require(graph.prepared.request.expectedBinding == binding &&
        graph.fullTwentyRelationSnapshot.bootUuid == identity.bootId &&
        graph.fullTwentyRelationSnapshot.bootGeneration == identity.bootGeneration &&
        graph.fullTwentyRelationSnapshot.connectionGeneration == identity.connectionGeneration,
        "privately issued full20 graph has foreign runtime identity")
      require(!managedByPod.contains(state.uuid), "manager UUID rights already owned")
      require(managedByPod.size < activeLimit && managedByPod.size < 256, "Main global managed capacity exceeded")
      require(!managedByPod.values.exists(_.record.entity.entityId == entry.body.entityId), "entity ID already owned by manager")
      ensureNoActiveEntityIdCollision(entry.body.entityId, state.uuid)
      require(!managedByPod.values.exists(record => record.record.entity match
        case controlled: ControlledActivePodEntity => controlled.sameBody(entry.body)
        case _ => false), "physical body already owned by manager")
      // 不调用publishRecovered/requireExactActiveLocator：它们仍是旧同步SQL路径。
      entry.body.applyCommittedControl(state, entry.receipt)
      validatePhysical(entry.receipt, entry.world, entry.body, entry.ticket)
      normalRecallFootprints.put(state.uuid, (entry.receipt.attemptUuid, graph.prepared.request.footprintUuid))
      managedByPod.put(state.uuid, ManagedPodStatus.Active(ManagedActivePod(entry.body, entry.world, state, entry.receipt.attemptUuid)))
      cachedStates.put(state.uuid, state)
      dirtyPods.remove(state.uuid)
      entry.status = MainControlledPublicationStatus.Published
      entry.view = viewOf(state, DebugPodPhase.Committed, DebugItemEndpoint.Reason.NONE)

    private final class PendingMode(val tag: IoOperationTag, val request: DebugItemEndpoint.Request,
        val sqlRequest: MySqlOwnedModeCommitRequest, var operationPlan: MySqlOwnedRepositoryOperationPlan,
        val session: MainOwnedModePublicationSession,
        val world: PodWorld, val body: ControlledActivePodEntity, val ticket: ChunkTicket):
      var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
      var completionConsumed = false
      var projectionReleasePrepared = false
      var projectionReleaseSubmitted = false
      var projectionReleaseTerminalCause = Option.empty[Throwable]
      var status = MainControlledPublicationStatus.Retained
      var cause = Option.empty[Throwable]
      var view = viewOf(managedActiveDetails(sqlRequest.podUuid).get.exactState,
        DebugPodPhase.Committed, DebugItemEndpoint.Reason.INITIALIZATION_PENDING)
    private val pendingModes = mutable.LinkedHashMap.empty[IoOperationTag, PendingMode]

    def retainMode(tag: IoOperationTag, request: DebugItemEndpoint.Request,
        sqlRequest: MySqlOwnedModeCommitRequest, operationPlan: MySqlOwnedRepositoryOperationPlan,
        session: MainOwnedModePublicationSession,
        world: PodWorld, body: ControlledActivePodEntity, ticket: ChunkTicket,
        actualRuntimeGeneration: Long): Unit =
      requireOwner(); identity.requireTag(tag)
      require(!retired && actualRuntimeGeneration == runtimeGeneration && request != null &&
        request.action() == DebugItemEndpoint.Action.TOGGLE && sqlRequest != null &&
        operationPlan != null && operationPlan.tag == tag && operationPlan.requestKind == "RUN" &&
        request.identity().boot() == identity.bootId && request.identity().pod() == sqlRequest.podUuid &&
        request.identity().player() == sqlRequest.authenticatedOwner && sqlRequest.expectedBinding == binding &&
        session != null && world != null && body != null && ticket != null, "exact owner mode context required")
      require(!pendingModes.contains(tag) && !pendingModes.values.exists(value =>
        value.sqlRequest.podUuid == sqlRequest.podUuid) &&
        pendingModes.size < activeLimit, "bounded single pending mode per body")
      assertCriticalTransitionAllowed(sqlRequest.podUuid)
      session.assertCurrent(request, world)
      validateModePhysical(request, sqlRequest, world, body, ticket)
      pendingModes.put(tag, new PendingMode(tag, request, sqlRequest, operationPlan, session, world, body, ticket))

    private def validateModePhysical(request: DebugItemEndpoint.Request, sqlRequest: MySqlOwnedModeCommitRequest,
        world: PodWorld, body: ControlledActivePodEntity, ticket: ChunkTicket): Unit =
      val current = managedActiveDetails(sqlRequest.podUuid).getOrElse(throw new IllegalStateException("Mode body lacks Main manager rights"))
      require(current.world.sameWorld(world) && current.entity.entityId == body.entityId &&
        (current.entity match
          case controlled: ControlledActivePodEntity => controlled.sameBody(body)
          case _ => false) && current.attemptUuid == sqlRequest.attemptUuid &&
        current.exactState.ownerUuid.contains(sqlRequest.authenticatedOwner) &&
        current.exactState.stateVersion == sqlRequest.expectedStateVersion &&
        request.identity().entityId() == body.entityId && body.actualPose == sqlRequest.actualPose &&
        body.isFrozen && body.hasPendingMode(request.operation(), sqlRequest.expectedStateVersion),
        "Mode retained exact owner/version/body/attempt/final pose mismatch")
      world.assertLiveIndexedBody(body)
      val chunks = chunkLoadingManager.publicationTicketChunks(sqlRequest.podUuid, world, ticket)
      require(chunks.size == 25 && chunks == recoveryChunks(world.dimensionId,
        sqlRequest.actualPose.posX, sqlRequest.actualPose.posZ), "Mode ticket is not exact actual25 at final pose")

    def consumeMode(completion: IoCompletion[MySqlOwnedIoReply], actualRuntimeGeneration: Long): MainControlledPublicationObservation =
      requireOwner(); require(completion != null, "actual mode completion required")
      val entry = pendingModes.getOrElse(completion.tag, throw new MainPublicationUnmatchedCompletion(completion))
      require(!entry.completionConsumed && entry.completion.isEmpty, "mode completion already consumed")
      entry.completion = Some(completion)
      entry.completionConsumed = true
      if retired || actualRuntimeGeneration != runtimeGeneration then
        entry.status = MainControlledPublicationStatus.Retired
        entry.cause = Some(new IllegalStateException("Late mode completion retained for retired generation"))
        entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.RECOVERY_PENDING)
      else
        try
          completion.result match
            case Success(MySqlOwnedIoReply.OwnedModeAlreadySet(readback)) =>
              publishAlreadySetMode(entry, readback)
            case result =>
              val graph = result match
                case Failure(cause) => throw cause
                case Success(MySqlOwnedIoReply.OwnedModeCommitted(value)) => value
                case Success(MySqlOwnedIoReply.OwnedModeReadbackFailed(value)) =>
                  require(value.tag == entry.tag && value.request == entry.sqlRequest, "actual mode failure context differs")
                  throw value.cause
                case Success(MySqlOwnedIoReply.OwnedModeRejected(_)) =>
                  throw new IllegalStateException("Mode CAS rejected; retained freeze is not a cleanup ACK")
                case Success(_) => throw new IllegalStateException("Mode completion is not an actual committed graph")
              require(graph != null && graph.tag == entry.tag && graph.request == entry.sqlRequest &&
                graph.inventory.bootUuid == identity.bootId && graph.inventory.bootGeneration == identity.bootGeneration &&
                graph.inventory.connectionGeneration == identity.connectionGeneration &&
                graph.inventory.recovery.binding.contains(binding), "Mode graph belongs to foreign command/session")
              entry.session.assertCurrent(entry.request, entry.world)
              validateModePhysical(entry.request, entry.sqlRequest, entry.world, entry.body, entry.ticket)
              val state = graph.state
              require(state.uuid == entry.sqlRequest.podUuid && state.ownerUuid.contains(entry.sqlRequest.authenticatedOwner) &&
                state.isActive && state.operatingMode == entry.sqlRequest.targetMode &&
                (state.stateVersion == entry.sqlRequest.expectedStateVersion ||
                  (entry.sqlRequest.expectedStateVersion < Long.MaxValue && state.stateVersion == entry.sqlRequest.expectedStateVersion + 1L)) &&
                graph.locator.attemptUuid == entry.sqlRequest.attemptUuid && graph.locator.domainVersion.contains(state.stateVersion) &&
                graph.locator.state == PhysicalRecoveryLocatorState.RowActive && graph.locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
                graph.locator.chunks.size == 25 && graph.locator.chunks.toSet == recoveryChunks(entry.world.dimensionId,
                  state.posX, state.posZ), "Mode committed owner/version/locator mismatch")
              // 再次actual索引检查后才解除pending freeze；不调用旧SQL publication。
              entry.world.assertLiveIndexedBody(entry.body)
              entry.body.finishPendingMode(entry.request.operation(), entry.sqlRequest.expectedStateVersion, state, entry.sqlRequest.attemptUuid)
              entry.world.assertLiveIndexedBody(entry.body)
              require(entry.body.actualPose == entry.sqlRequest.actualPose, "Mode pose changed during control publication")
              managedByPod.put(state.uuid, ManagedPodStatus.Active(ManagedActivePod(entry.body, entry.world, state, entry.sqlRequest.attemptUuid)))
              cachedStates.put(state.uuid, state); dirtyPods.remove(state.uuid)
              entry.status = MainControlledPublicationStatus.Published
              entry.view = viewOf(state, DebugPodPhase.Committed, DebugItemEndpoint.Reason.NONE)
              pending.values.filter(_.receipt.podUuid == state.uuid).foreach(_.view = entry.view)
        catch
          case cause: Throwable =>
            entry.cause = recordMainFailure(entry.tag, entry.sqlRequest.podUuid, MainFailureStage.MODE_PUBLICATION, entry.cause, cause); entry.status = MainControlledPublicationStatus.Faulted
            entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.RECOVERY_PENDING)
            // Unknown/afterACK不解冻、不删除原体、不释放ticket；apply后失败也保持实际冻结。
            try entry.body.markControlFaulted(cause)
            catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
      MainControlledPublicationObservation(entry.status, Some(entry.view), entry.cause, true)

    /** No-change 只结束 exact 已有状态请求；不会制造提交回执、改版本或触碰 ticket/mapping。 */
    private def publishAlreadySetMode(entry: PendingMode, readback: MySqlOwnedModeAlreadySetReadback): Unit =
      require(readback != null && readback.tag == entry.tag && readback.request == entry.sqlRequest &&
        readback.operationPlan == entry.operationPlan && entry.operationPlan.tag == entry.tag,
        "AlreadySet readback differs from the retained original tag/request/plan")
      val inventory = readback.inventory
      val full = readback.fullTwentyRelationSnapshot
      require(full != null,"AlreadySet requires its original immutable complete snapshot")
      full.requireCompleteFamily()
      require(inventory.bootUuid == identity.bootId && inventory.bootGeneration == identity.bootGeneration &&
        inventory.connectionGeneration == identity.connectionGeneration && inventory.recovery.binding.contains(binding) &&
        full.bootUuid == identity.bootId &&
        full.bootGeneration == identity.bootGeneration && full.connectionGeneration == identity.connectionGeneration &&
        full.canonicalByteLength > 0L && full.canonicalSha256Hex != null && full.canonicalSha256Hex.nonEmpty,
        "AlreadySet result lacks the exact immutable closed-family session snapshot")
      entry.session.assertCurrent(entry.request, entry.world)
      validateModePhysical(entry.request, entry.sqlRequest, entry.world, entry.body, entry.ticket)
      val state = readback.state
      val current = managedActiveDetails(entry.sqlRequest.podUuid).getOrElse(
        throw new IllegalStateException("AlreadySet body lost its Main manager mapping"))
      require(state == current.exactState && state.uuid == entry.sqlRequest.podUuid &&
        state.ownerUuid.contains(entry.sqlRequest.authenticatedOwner) && state.isActive &&
        state.stateVersion == entry.sqlRequest.expectedStateVersion &&
        state.operatingMode == entry.sqlRequest.targetMode &&
        state.worldName == entry.sqlRequest.actualPose.worldName &&
        state.posX == entry.sqlRequest.actualPose.posX && state.posY == entry.sqlRequest.actualPose.posY &&
        state.posZ == entry.sqlRequest.actualPose.posZ && state.yaw == entry.sqlRequest.actualPose.yaw &&
        state.pitch == entry.sqlRequest.actualPose.pitch &&
        state.dimensionId.contains(entry.sqlRequest.actualPose.dimensionId) &&
        readback.locator.attemptUuid == entry.sqlRequest.attemptUuid &&
        readback.locator.state == PhysicalRecoveryLocatorState.RowActive &&
        readback.locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
        readback.locator.domainVersion.contains(state.stateVersion) && readback.locator.chunks.size == 25 &&
        readback.locator.chunks.toSet == recoveryChunks(entry.world.dimensionId, state.posX, state.posZ),
        "AlreadySet no-change state differs from the retained owner/version/pose/body/ticket")
      entry.world.assertLiveIndexedBody(entry.body)
      entry.body.finishPendingMode(entry.request.operation(), entry.sqlRequest.expectedStateVersion,
        state, entry.sqlRequest.attemptUuid)
      entry.world.assertLiveIndexedBody(entry.body)
      require(entry.body.actualPose == entry.sqlRequest.actualPose,
        "AlreadySet body pose changed during no-change publication")
      entry.status = MainControlledPublicationStatus.AlreadySet
      entry.view = viewOf(state, DebugPodPhase.Committed, DebugItemEndpoint.Reason.NONE)

    def modeResponsibility(tag: IoOperationTag): Option[MainControlledPublicationObservation] =
      requireOwner()
      pendingModes.get(tag).map(entry => MainControlledPublicationObservation(entry.status, Some(entry.view), entry.cause, entry.completionConsumed))

    /** 已活动的body/ticket留在manager；只交接本次DATA并退休原capture Entry。 */
    def prepareDbFirstInitialRetirement(tag:IoOperationTag):MySqlDbFirstInitialPublicationReceipt =
      requireOwner();identity.requireTag(tag)
      val entry=pending.getOrElse(tag,throw new IllegalArgumentException("unknown initial publication operation"))
      require(!retired && entry.status==MainControlledPublicationStatus.Published && entry.cause.isEmpty &&
        !entry.initialReleaseSubmitted,"healthy original initial publication retirement required")
      val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main owner missing"))
      callbacks.requireOriginalMainAdapter(callbacks.adapter)
      try
        if entry.initialPublication.isEmpty then
          val confirmation=entry.initialConfirmation.getOrElse(throw new IllegalStateException("original initial confirmation missing"))
          val reservation=entry.initialReservation.getOrElse(throw new IllegalStateException("original initial capture reference missing"))
          confirmation.requireOriginal(reservation)
          validatePhysical(entry.receipt,entry.world,entry.body,entry.ticket)
          val active=managedByPod.get(entry.receipt.podUuid).collect { case ManagedPodStatus.Active(record)=>record }
            .getOrElse(throw new IllegalStateException("initial active mapping was not published"))
          require(active.world.sameWorld(entry.world) && active.attemptUuid==confirmation.attemptUuid &&
            active.exactState.stateVersion==confirmation.committedStateVersion && (active.entity match
              case body:ControlledActivePodEntity=>body.sameBody(entry.body)
              case _=>false),"original initial active mapping changed before retirement")
          entry.initialPublication=Some(callbacks.adapter.recordDbFirstInitialPublication(confirmation))
          // receipt仅含原tag/core/handle；不再让Main保留capture或worker completion引用。
          entry.initialReservation=None;entry.initialConfirmation=None;entry.completion=None
        entry.initialPublication.get
      catch case cause:Throwable=>
        entry.cause=recordMainFailure(tag,entry.receipt.podUuid,MainFailureStage.CONTROLLED_PUBLICATION,entry.cause,cause)
        entry.status=MainControlledPublicationStatus.Faulted
        throw cause

    def submitDbFirstInitialRetirement(tag:IoOperationTag):IoSubmission =
      val receipt=prepareDbFirstInitialRetirement(tag)
      val entry=pending(tag)
      val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main owner missing"))
      try
        val result=callbacks.adapter.submitDbFirstInitialPublicationRelease(receipt)
        if result==IoSubmission.Accepted then entry.initialReleaseSubmitted=true
        result
      catch case cause:Throwable=>
        entry.cause=recordMainFailure(tag,entry.receipt.podUuid,MainFailureStage.CONTROLLED_PUBLICATION,entry.cause,cause)
        entry.status=MainControlledPublicationStatus.Faulted
        throw cause

    def consumeDbFirstInitialRetirement(completion:IoCompletion[MySqlOwnedIoReply]):MainControlledPublicationObservation =
      requireOwner()
      require(completion!=null,"actual initial retirement completion required")
      val entry=pending.getOrElse(completion.tag,throw new MainPublicationUnmatchedCompletion(completion))
      require(!retired && entry.initialReleaseSubmitted && entry.initialPublication.nonEmpty && entry.cause.isEmpty,
        "original accepted initial retirement required")
      val callbacks=dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main owner missing"))
      callbacks.requireOriginalMainAdapter(callbacks.adapter)
      try
        callbacks.adapter.finishDbFirstInitialPublicationRelease(entry.initialPublication.get,completion)
        val result=observation(entry)
        pending.remove(entry.tag)
        result
      catch case cause:Throwable=>
        entry.cause=recordMainFailure(entry.tag,entry.receipt.podUuid,MainFailureStage.CONTROLLED_PUBLICATION,entry.cause,cause)
        entry.status=MainControlledPublicationStatus.Faulted
        entry.view=entry.view.copy(phase=DebugPodPhase.Faulted,reason=DebugItemEndpoint.Reason.FAULTED)
        throw cause

    /** controlled commit成功后仅撤销Main full20强引用；不触body/ticket/SQL。 */
    def prepareControlledProjectionRelease(tag: IoOperationTag,prepareTag: IoOperationTag): Unit =
      requireOwner();identity.requireTag(tag);identity.requireTag(prepareTag)
      val entry=pending.getOrElse(tag,throw new IllegalArgumentException("unknown controlled publication operation"))
      require(!retired && entry.status==MainControlledPublicationStatus.Published && entry.cause.isEmpty &&
        entry.completion.nonEmpty && entry.projectionRelease.isEmpty,"healthy original publication before graph drop required")
      entry.completion.get.result match
        case Success(MySqlOwnedIoReply.ControlledDebugCommitted(graph)) =>
          require(graph!=null && graph.hasPrivateReadbackProof && graph.tag==tag && graph.prepared.tag==prepareTag &&
            graph.prepared.receipt==entry.receipt,"original prepare/commit worker graph pair required")
        case _ => throw new IllegalStateException("F3 original committed graph required before its release")
      entry.projectionRelease=Some(new MainControlledProjectionReleaseBarrier(Thread.currentThread(),prepareTag,tag))
      entry.completion=None

    private def controlledReleaseEntry(tag: IoOperationTag): Pending =
      requireOwner();identity.requireTag(tag)
      var found: Pending=null
      pending.valuesIterator.foreach { entry =>
        if entry.projectionRelease.exists(_.contains(tag)) then
          require(found==null,"ambiguous controlled release tag");found=entry
      }
      require(found!=null,"ordinary release does not match its original controlled publication")
      found

    /** 只由 endpoint 已消费 exact worker ordinary ACK 调用；两个真实ACK到齐才退出 pending容量。 */
    def acknowledgeControlledProjectionReleased(tag: IoOperationTag): Unit =
      val entry=controlledReleaseEntry(tag)
      require(!retired && entry.status==MainControlledPublicationStatus.Published && entry.cause.isEmpty &&
        entry.completion.isEmpty,"retired/failed/unconsumed controlled responsibility cannot compact")
      if entry.projectionRelease.get.acknowledge(tag) then
        pending.remove(entry.tag)
        // managedByPod/cachedStates已由 critical commit 后的 publish取得；不再触实体/ticket/SQL。

    def recordControlledProjectionReleaseFailure(tag: IoOperationTag,cause: Throwable): Unit =
      val entry=controlledReleaseEntry(tag)
      entry.projectionRelease.get.fail(tag,cause)
      entry.cause=recordMainFailure(entry.tag,entry.receipt.podUuid,MainFailureStage.CONTROLLED_PUBLICATION,entry.cause,cause)
      entry.status=MainControlledPublicationStatus.Faulted
      entry.view=entry.view.copy(phase=DebugPodPhase.Faulted,reason=DebugItemEndpoint.Reason.FAULTED)
    /** 成功 publication 后先清除 Main 侧 full20 completion 引用；worker budget 仍须等待 exact ACK。 */
    def prepareModeProjectionRelease(tag: IoOperationTag): Unit =
      requireOwner(); identity.requireTag(tag)
      val entry = pendingModes.getOrElse(tag, throw new IllegalArgumentException("Unknown mode operation"))
      require((entry.status == MainControlledPublicationStatus.Published || entry.status == MainControlledPublicationStatus.AlreadySet) &&
        entry.completionConsumed && entry.completion.nonEmpty && !entry.projectionReleasePrepared && !retired,
        "only a live successfully published exact mode graph may clear its Main completion reference")
      entry.completion = None
      entry.operationPlan = null // 原Main encoded plan已退出此bounded scope，worker原issuer仍保责任。
      entry.projectionReleasePrepared = true

    def modeProjectionReleaseEligible(tag: IoOperationTag): Boolean =
      requireOwner(); identity.requireTag(tag)
      pendingModes.get(tag).exists(entry => !retired && entry.projectionReleasePrepared &&
        !entry.projectionReleaseSubmitted && entry.projectionReleaseTerminalCause.isEmpty &&
        (entry.status == MainControlledPublicationStatus.Published || entry.status == MainControlledPublicationStatus.AlreadySet))

    def markModeProjectionReleaseSubmitted(tag: IoOperationTag): Unit =
      requireOwner(); identity.requireTag(tag)
      val entry = pendingModes.getOrElse(tag, throw new IllegalArgumentException("Unknown mode operation"))
      require(modeProjectionReleaseEligible(tag), "only a live prepared exact mode projection may submit its release ACK")
      entry.projectionReleaseSubmitted = true

    def recordModeProjectionReleaseFailure(tag: IoOperationTag, cause: Throwable): Unit =
      requireOwner(); identity.requireTag(tag)
      val entry = pendingModes.getOrElse(tag, throw new IllegalArgumentException("Unknown mode operation"))
      require(cause != null && entry.projectionReleasePrepared && entry.projectionReleaseTerminalCause.isEmpty,
        "projection release failure must retain its exact original responsibility")
      entry.projectionReleaseTerminalCause = Some(cause)

    /** Exact accepted memory ACK compacts only after Main dropped every completion graph reference. */
    def acknowledgeModeProjectionReleased(tag: IoOperationTag): Unit =
      requireOwner(); identity.requireTag(tag)
      val entry = pendingModes.getOrElse(tag, throw new IllegalArgumentException("Unknown mode operation"))
      require(entry.projectionReleasePrepared && entry.projectionReleaseSubmitted && entry.completion.isEmpty,
        "only the exact submitted release ACK may compact a graph-free mode responsibility")
      pendingModes.remove(tag)
      ()

    private[server] def modeCompletionRetained(tag: IoOperationTag): Boolean =
      requireOwner(); identity.requireTag(tag)
      pendingModes.get(tag).exists(_.completion.nonEmpty)

    /** pure immutable projection，无world sampling/DB/readback/修复副作用。 */
    def inspect(podUuid: UUID): Option[MainControlledPodView] =
      requireOwner()
      require(podUuid != null, "pod UUID required")
      dbFirstRecalls.values.find(_.base.uuid == podUuid).map(entry =>
        viewOf(entry.base, if entry.cause.nonEmpty then DebugPodPhase.Faulted else DebugPodPhase.PendingRecall,
          if entry.cause.nonEmpty then DebugItemEndpoint.Reason.FAULTED else DebugItemEndpoint.Reason.RECOVERY_PENDING))
        .orElse(dbFirstCheckpoints.values.find(_.base.uuid==podUuid).map(entry=>
          viewOf(entry.published.getOrElse(entry.base),if entry.cause.nonEmpty then DebugPodPhase.Faulted else DebugPodPhase.Committed,
            if entry.cause.nonEmpty then DebugItemEndpoint.Reason.FAULTED else DebugItemEndpoint.Reason.RECOVERY_PENDING)))
        .orElse(pendingModes.values.find(_.sqlRequest.podUuid == podUuid).map(_.view))
        .orElse(pending.values.find(_.receipt.podUuid == podUuid).map(_.view))
        .orElse(managedActiveDetails(podUuid).map(value => viewOf(value.exactState, DebugPodPhase.Committed, DebugItemEndpoint.Reason.NONE)))

    def responsibility(tag: IoOperationTag): Option[MainControlledPublicationObservation] =
      requireOwner()
      pending.get(tag).map(observation)

    def retire(): Unit =
      requireOwner()
      retired = true // 不释放/删除accepted physical或completion责任。
      pendingModes.values.foreach { entry =>
        entry.status = MainControlledPublicationStatus.Retired
        entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.RECOVERY_PENDING)
      }
      pending.values.foreach { entry =>
        entry.status = MainControlledPublicationStatus.Retired
        entry.view = entry.view.copy(phase = DebugPodPhase.Faulted, reason = DebugItemEndpoint.Reason.RECOVERY_PENDING)
      }

    private def observation(entry: Pending): MainControlledPublicationObservation =
      MainControlledPublicationObservation(entry.status, Some(entry.view), entry.cause, entry.completion.nonEmpty || entry.initialPublication.nonEmpty)

    private def viewOf(state: FloatingPodState, phase: DebugPodPhase, reason: DebugItemEndpoint.Reason): MainControlledPodView =
      MainControlledPodView(state.uuid, state.ownerUuid, state.operatingMode, state.isActive, state.stateVersion,
        FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.dimensionId.get), phase, reason)

    private def validatePhysical(receipt: UnboundPreparationReceipt, world: PodWorld,
        body: ControlledActivePodEntity, ticket: ChunkTicket): Unit =
      require(world.dimensionId == receipt.targetPose.dimensionId && world.diagnosticWorldName == receipt.targetPose.worldName &&
        body.podUuid == receipt.podUuid && body.actualPose == receipt.targetPose && body.isFrozen &&
        body.hasControlledCreationStamp(receipt), "retained exact world/body/pose/frozen/stamp mismatch")
      require(world.findPod(receipt.podUuid).exists {
        case indexed: ControlledActivePodEntity => indexed.sameBody(body)
        case _ => false
      }, "retained body is no longer exact indexed body")
      world.assertLiveIndexedBody(body)
      val actual = chunkLoadingManager.publicationTicketChunks(receipt.podUuid, world, ticket)
      require(actual.size == 25 && actual == recoveryChunks(world.dimensionId, receipt.targetPose.posX, receipt.targetPose.posZ),
        "actual ticket is not exact25 requested chunks")

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
                  entity match
                    case controlled: ControlledActivePodEntity => controlled.applyCommittedMode(committed, attemptUuid)
                    case _ => ()
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
    requireLegacyMutation()
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
    requireLegacyMutation()
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
    requireLegacyMutation()
    require(podUuid != null, "podUuid must not be null")
    synchronized {
      managedByPod.get(podUuid) match
        case Some(ManagedPodStatus.RecallPendingCommit(record)) => commitRecallOnly(podUuid, record)
        case _ => throw new IllegalStateException(s"FloatingPod $podUuid has no pending recall commit")
    }
  }

  /** Captures the exact source body/mapping/cache before this boot starts publication. */
  private[server] def beginStartupRecovery(state: FloatingPodState, world: PodWorld): StartupRecoveryRollbackReceipt = synchronized {
    require(state != null && world != null, "startup recovery state and world must not be null")
    if !state.isActive || state.dimensionId.isEmpty then throw new IllegalArgumentException(s"Recovery state ${state.uuid} must be active and dimensioned")
    if state.dimensionId.get != world.dimensionId then throw new IllegalArgumentException(s"Recovery dimension ${state.dimensionId.get} does not match world ${world.dimensionId}")
    val body = world.findPod(state.uuid)
    val mapping = managedByPod.get(state.uuid) match
      case Some(ManagedPodStatus.Active(record)) =>
        if !record.world.sameWorld(world) || record.exactState != state || record.entity.podUuid != state.uuid then
          throw new IllegalStateException(s"FloatingPod ${state.uuid} already has a conflicting manager mapping")
        if !body.exists(found => sameRecoveryBody(found, record.entity)) then
          throw new IllegalStateException(s"FloatingPod ${state.uuid} manager body differs from its world body before startup recovery")
        Some(StartupRecoveryManagerSnapshot(record.entity, record.world, record.exactState, record.attemptUuid))
      case Some(_) => throw new IllegalStateException(s"FloatingPod ${state.uuid} already has a conflicting manager status")
      case None => None
    new StartupRecoveryRollbackReceipt(startupRecoveryAuthority, state.uuid, world, state, body, mapping,
      cachedStates.get(state.uuid), dirtyPods.contains(state.uuid))
  }

  /** Spawn phase for StartupReconciliation; receipt captures the exact created body before publication. */
  private[server] def spawnStartupRecoveryEntity(state: FloatingPodState, receipt: StartupRecoveryRollbackReceipt): Try[Unit] = Try {
    synchronized {
      requireReceipt(receipt)
      requireReceiptState(state, receipt)
      val attemptUuid = validateStartupRecoveryPrerequisites(state, receipt.world)
      receipt.attemptUuid = Some(attemptUuid)
      if receipt.initialBody.nonEmpty || receipt.body.nonEmpty then
        throw new IllegalStateException(s"Startup recovery cannot spawn over an observed body for ${state.uuid}")
      if managedByPod.contains(state.uuid) then throw new IllegalStateException(s"Startup recovery cannot spawn while manager mapping exists for ${state.uuid}")
      if receipt.world.findPod(state.uuid).nonEmpty then throw staleStartupReceipt(state.uuid, "a body appeared after the rollback receipt was captured")
      val entity = spawnStartupBody(state, receipt, attemptUuid)
      validateSpawnedEntity(state.uuid, entity)
      if !receipt.body.exists(body => sameRecoveryBody(body, entity)) || !receipt.bootCreatedBody then
        throw new IllegalStateException(s"Startup recovery did not retain the exact created body for ${state.uuid}")
      ensureNoActiveEntityIdCollision(entity.entityId, state.uuid)
    }
  }

  private def spawnStartupBody(state: FloatingPodState, receipt: StartupRecoveryRollbackReceipt,
      attempt: UUID): ActivePodEntity = dbFirstReadOwner match
    case Some(reads) =>
      val world = receipt.world match
        case actual: PodWorldImpl => actual
        case _ => throw new IllegalArgumentException("schema6 recovery requires the actual server world")
      val binding = requireDbFirstStartupRecoveryBinding(state, receipt.world, attempt)
      var restored: ActivePodEntity = null
      reads.consumeRecoverySnapshot(state, attempt, nbt =>
        restored = world.spawnDbFirstRecoveryPod(state, binding.bindingUuid, attempt, nbt,
          body => retainBootCreatedBody(receipt, body)))
      require(restored != null, "schema6 complete NBT consumer did not return the retained body")
      val actual = restored match
        case body: ActivePodEntityImpl => body.rawEntity.artifactIdentity().orElse(null)
        case _ => throw new IllegalStateException("schema6 recovery must retain the actual FloatingPod body")
      require(actual != null && actual.bindingUuid() == binding.bindingUuid && actual.attemptUuid() == attempt,
        "schema6 recovery receipt requires the actual artifact binding and attempt")
      receipt.bootCreatedArtifactIdentity match
        case None => receipt.bootCreatedArtifactIdentity = Some(actual)
        case Some(existing) if existing == actual => ()
        case Some(_) => throw staleStartupReceipt(state.uuid, "boot-created artifact identity changed during recovery")
      restored
    case None => receipt.world.spawnPod(state, body => retainBootCreatedBody(receipt, body))

  private def requireDbFirstStartupRecoveryBinding(state: FloatingPodState, world: PodWorld,
      attempt: UUID): PhysicalRecoveryBinding =
    val reads = dbFirstReadOwner.getOrElse(throw new IllegalStateException("original schema6 Main reads are required"))
    val data = reads.recoveryReadData
    val binding = data.binding.getOrElse(throw new IllegalStateException("schema6 DB recovery binding is absent"))
    reads.requireMainPublication(reads.adapter.identity, binding, reads.activeLimit)
    require(binding.cutoverState == PhysicalRecoveryCutoverState.Certified &&
      data.state(state.uuid).contains(state) &&
      requireStartupActiveLocator(state, world) == attempt,
      "schema6 recovery must preserve current committed state, certified binding and original active attempt")
    binding

  /** 单Pod恢复失败保原体/票和cause；只隔离本boot新建体，不把世界旧壳登记成活动业务。 */
  private[server] def retainDbFirstStartupRecoveryFailure(receipt: StartupRecoveryRollbackReceipt,
      cause: Throwable): Unit = synchronized {
    requireReceipt(receipt)
    require(dbFirstReadOwner.nonEmpty && cause != null, "original schema6 startup failure required")
    if receipt.bootCreatedBody then
      val body = receipt.body.getOrElse(throw new IllegalStateException("original created recovery body missing"))
      val attempt = receipt.attemptUuid.getOrElse(throw new IllegalStateException("original recovery attempt missing"))
      managedByPod.get(receipt.podUuid).foreach { status =>
        require(sameRecoveryBody(status.record.entity, body) && status.record.world.sameWorld(receipt.world),
          "cannot quarantine another manager body's responsibility")
      }
      recordCommittedSpawnFault(receipt.podUuid, receipt.world, body, receipt.exactState, attempt, cause)
      cachedStates.remove(receipt.podUuid)
    else receipt.body.foreach {
      case body: ControlledActivePodEntity => body.markControlFaulted(cause)
      case _ => ()
    }
  }

  /** Publishes an active row using the receipt established before any startup side effect. */
  private[server] def publishStartupRecovery(state: FloatingPodState, receipt: StartupRecoveryRollbackReceipt): Try[RecoveryDisposition] = Try {
    synchronized {
      requireReceipt(receipt)
      requireReceiptState(state, receipt)
      if dbFirstReadOwner.nonEmpty && receipt.initialBody.nonEmpty && receipt.initialMapping.isEmpty then
        throw new IllegalStateException("schema6 database recovery cannot attach a world-record body")
      val attemptUuid = validateStartupRecoveryPrerequisites(state, receipt.world)
      receipt.attemptUuid = Some(attemptUuid)
      managedByPod.get(state.uuid) match
        case Some(ManagedPodStatus.Active(record)) =>
          val expected = receipt.initialMapping.getOrElse(throw staleStartupReceipt(state.uuid, "manager mapping appeared after the rollback receipt was captured"))
          if !managerRecordMatches(record, expected) || record.attemptUuid != attemptUuid then
            throw new IllegalStateException(s"Recovery manager mapping for ${state.uuid} is not the exact pre-existing mapping")
          val current = receipt.world.findPod(state.uuid).getOrElse(throw staleStartupReceipt(state.uuid, "pre-existing manager body disappeared"))
          if !sameRecoveryBody(current, record.entity) then throw staleStartupReceipt(state.uuid, "pre-existing manager body was replaced")
          receipt.body = Some(record.entity)
          receipt.disposition = Some(RecoveryDisposition.Replayed)
          RecoveryDisposition.Replayed
        case Some(_) => throw new IllegalStateException(s"FloatingPod ${state.uuid} already has a conflicting manager status")
        case None =>
          val entity =
            if receipt.bootCreatedBody then
              val captured = receipt.body.getOrElse(throw new IllegalStateException(s"Created body receipt is missing for ${state.uuid}"))
              if captured.podUuid == state.uuid then
                val current = receipt.world.findPod(state.uuid).getOrElse(throw staleStartupReceipt(state.uuid, "created body disappeared before publication"))
                if !sameRecoveryBody(current, captured) then throw staleStartupReceipt(state.uuid, "created body was replaced before publication")
              captured
            else receipt.initialBody match
              case Some(initial) =>
                val current = receipt.world.findPod(state.uuid).getOrElse(throw staleStartupReceipt(state.uuid, "attached source body disappeared before publication"))
                if !sameRecoveryBody(current, initial) then throw staleStartupReceipt(state.uuid, "attached source body was replaced before publication")
                receipt.body = Some(initial)
                initial
              case None =>
                if receipt.world.findPod(state.uuid).nonEmpty then throw staleStartupReceipt(state.uuid, "a body appeared after the rollback receipt was captured")
                val created = spawnStartupBody(state, receipt, attemptUuid)
                if !receipt.body.exists(body => sameRecoveryBody(body, created)) || !receipt.bootCreatedBody then
                  throw new IllegalStateException(s"Startup recovery did not retain the exact created body for ${state.uuid}")
                created
          validateSpawnedEntity(state.uuid, entity)
          ensureNoActiveEntityIdCollision(entity.entityId, state.uuid)
          val disposition = if receipt.bootCreatedBody then RecoveryDisposition.Recreated(SpawnedActivePod(entity, state))
          else RecoveryDisposition.Attached(SpawnedActivePod(entity, state))
          publishRecovered(state, receipt.world, entity, attemptUuid, () => receipt.managerMappingPublished = true, startupReceipt = Some(receipt))
          receipt.disposition = Some(disposition)
          disposition
    }
  }

  /** Commits receipts only after the complete startup inventory has converged. */
  private[server] def completeStartupRecoveries(receipts: List[StartupRecoveryRollbackReceipt]): Unit = synchronized {
    require(receipts != null && receipts.forall(_ != null), "startup recovery receipts must not be null")
    if receipts.map(_.podUuid).distinct.size != receipts.size then throw new IllegalArgumentException("Startup recovery completion contains duplicate pod receipts")
    receipts.foreach { receipt =>
      requireReceipt(receipt)
      validateReceiptCurrentState(receipt)
      if receipt.disposition.isEmpty then throw new IllegalStateException(s"Startup recovery receipt for ${receipt.podUuid} did not complete publication")
    }
    receipts.foreach(_.open = false)
  }

  /** Compatibility recovery boundary; failures compensate only the receipt created by this invocation. */
  def recoverCommittedActive(state: FloatingPodState, world: PodWorld): Try[RecoveryDisposition] = Try {
    val receipt = beginStartupRecovery(state, world)
    try
      val disposition = publishStartupRecovery(state, receipt).get
      completeStartupRecoveries(List(receipt))
      disposition
    catch
      case primary: Throwable =>
        val terminal =
          try
            abortStartupRecovery(receipt).get
            primary
          catch case cleanup: Throwable => StartupRecoveryFailures.retain(primary, cleanup)
        throw terminal
  }

  /** Removes only the exact body created by this receipt, then unpublishes only its own manager/cache state. */
  private[server] def abortStartupRecovery(receipt: StartupRecoveryRollbackReceipt): Try[Unit] = Try {
    synchronized {
      requireReceipt(receipt)
      receipt.open = false // stale receipts are one-shot; they can never regain deletion authority after an ABA change.
      validateReceiptCurrentState(receipt)
      if !receipt.disposition.contains(RecoveryDisposition.Replayed) then
        var cleanupFailure = Option.empty[Throwable]
        if receipt.bootCreatedBody then
          val body = receipt.body.getOrElse(throw staleStartupReceipt(receipt.podUuid, "created body receipt is missing"))
          try receipt.world.removePod(body)
          catch case failure: Throwable => cleanupFailure = Some(failure)
        if receipt.managerMappingPublished then
          try restoreStartupManagerSnapshot(receipt)
          catch
            case failure: Throwable => cleanupFailure match
              case Some(primary) => cleanupFailure = Some(StartupRecoveryFailures.retain(primary, failure))
              case None => cleanupFailure = Some(failure)
        cleanupFailure.foreach(throw _)
    }
  }

  /** 恢复已提交的活动状态，不写数据库或增加版本；重复回放只返回 Replayed。 */
  def recoverCommittedActiveForStartup(state: FloatingPodState, world: PodWorld, receipt: StartupRecoveryRollbackReceipt): Try[RecoveryDisposition] = Try {
    require(world != null && receipt != null, "startup recovery world and receipt must not be null")
    if !receipt.world.sameWorld(world) then throw new IllegalArgumentException(s"Startup recovery world does not match the exact receipt for ${receipt.podUuid}")
    publishStartupRecovery(state, receipt).get
  }

  def getActivePod(podUuid: UUID): Option[ActivePodEntity] = synchronized { Option(podUuid).flatMap(uuid => managedByPod.get(uuid).collect { case ManagedPodStatus.Active(record) => record.entity }) }
  def getActiveEntity(podUuid: UUID): Option[FloatingPodEntity] = getActivePod(podUuid).collect { case entity: ActivePodEntityImpl => entity.rawEntity }
  def getPodUuidByEntityId(entityId: Int): Option[UUID] = synchronized { managedByPod.collectFirst { case (uuid, ManagedPodStatus.Active(record)) if record.entity.entityId == entityId => uuid } }
  def getAllActivePods(): List[UUID] = synchronized { managedByPod.collect { case (uuid, _: ManagedPodStatus.Active) => uuid }.toList.sortBy(_.toString) }
  private[server] def managedActiveDetails(podUuid: UUID): Option[ManagedActiveDetails] = synchronized {
    Option(podUuid).flatMap(uuid => managedByPod.get(uuid).collect { case ManagedPodStatus.Active(record) => ManagedActiveDetails(record.entity, record.world, record.exactState, record.attemptUuid) })
  }
  private[server] def findPersistedState(podUuid: UUID): Option[FloatingPodState] =
    mainPodReads match
      case Some(projection) => projection.findState(podUuid)
      case None => repository.findByUuid(podUuid).get
  private[server] def persistedActivePodCount: Int =
    mainPodReads match
      case Some(projection) => projection.activeCount
      case None => repository.findAllActive().get.size
  private[server] def physicalRecoveryStoreForRuntime: PhysicalRecoveryStore = physicalRecoveryStore

  /** 数据库已提交后才发布 manager 映射；失败保留持久活动义务和实体/ticket。 */
  private[server] def publishControlledUnboundCreate(receipt: UnboundPreparationReceipt, world: PodWorld, entity: ControlledActivePodEntity, committed: FloatingPodState): Unit = synchronized {
    require(receipt != null && world != null && entity != null && committed != null, "controlled create publication values must not be null")
    try
      if receipt.origin != com.wi.floatingpod.storage.UnboundIntentOrigin.DebugItemCreate || receipt.createOperationUuid.isEmpty then throw new IllegalArgumentException("Only a DEBUG_ITEM_CREATE receipt can publish through controlled creation")
      if receipt.binding.cutoverState != com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified then throw new IllegalArgumentException("Controlled creation requires a certified physical recovery binding")
      validateCommittedActive(committed, receipt.podUuid, receipt.targetPose.dimensionId)
      if committed.ownerUuid.nonEmpty || committed.operatingMode != PodOperatingMode.WorldIdle || committed.stateVersion != receipt.preparedVersion + 1L then throw new IllegalStateException("Controlled create returned an invalid ownerless WorldIdle state")
      if entity.podUuid != receipt.podUuid || entity.actualPose != receipt.targetPose || !entity.hasControlledCreationStamp(receipt) then throw new IllegalStateException("Controlled create entity does not match its exact receipt and pose")
      requireExactActiveLocator(committed, world, receipt.attemptUuid)
      ensureNoActiveEntityIdCollision(entity.entityId, receipt.podUuid)
      managedByPod.get(receipt.podUuid) match
        case Some(ManagedPodStatus.Active(record)) if record.world.sameWorld(world) && record.entity.podUuid == receipt.podUuid && record.exactState == committed && record.attemptUuid == receipt.attemptUuid => ()
        case Some(_) => throw new IllegalStateException(s"FloatingPod ${receipt.podUuid} already has a conflicting manager status")
        case None =>
          entity.applyCommittedControl(committed, receipt)
          publishRecovered(committed, world, entity, receipt.attemptUuid)
    catch
      case failure: Throwable =>
        if committed.isActive then recordCommittedSpawnFault(receipt.podUuid, world, entity, committed, receipt.attemptUuid, failure)
        throw failure
  }
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
    requirePublicationMain()
    require(podUuid != null, "podUuid must not be null"); require(state != null, "state must not be null")
    synchronized {
      val record = managedByPod.get(podUuid).collect { case ManagedPodStatus.Active(active) => active }.getOrElse(throw new IllegalStateException(s"Cannot cache state for inactive pod $podUuid"))
      if state.uuid != podUuid then throw new IllegalArgumentException(s"Cached state UUID ${state.uuid} does not match pod UUID $podUuid")
      requireRuntimeDriftOnly(podUuid, record.exactState, state)
      val revision = Math.addExact(dirtyRevisions.getOrElse(podUuid, 0L), 1L)
      cachedStates.put(podUuid, state); dirtyRevisions.put(podUuid, revision); dirtyPods += podUuid
    }
  }
  def flushCachedStates(): Try[Int] = Try {
    requireLegacyMutation()
    if mainPublicationScope.exists(_.hasTrackedDriftRuntime) then
      requirePublicationMain()
      requestPeriodicFlush()
      0 // 请求数不是durable完成数；shutdown报告仍保dirty/accepted/ACK责任。
    else synchronized {
      var persistedCount = 0
      dirtyPods.toList.sortBy(_.toString).foreach { podUuid =>
        val active = managedByPod.get(podUuid).collect { case ManagedPodStatus.Active(record) => record }.getOrElse(throw new IllegalStateException(s"Dirty pod $podUuid is not active"))
        val state = cachedStates.getOrElse(podUuid, throw new IllegalStateException(s"Dirty pod $podUuid has no cached state"))
        val persisted = physicalRecoveryStore.updateTrackedActiveDrift(state, active.attemptUuid).get
        if persisted.uuid != podUuid || !persisted.isActive || persisted.dimensionId != active.exactState.dimensionId then throw new IllegalStateException(s"Repository returned invalid drift state for $podUuid")
        cachedStates.put(podUuid, persisted); managedByPod.put(podUuid, ManagedPodStatus.Active(active.copy(exactState = persisted))); dirtyPods.remove(podUuid); persistedCount += 1
      }
      persistedCount
    }
  }
  /** 关服可信推进：逐一尝试所有初始状态，任何失败都保留在报告中并继续下一项。 */
  def shutdownWithReport(): ManagerShutdownReport =
    val initial = synchronized { managedByPod.toList.sortBy(_._1.toString).map((uuid, status) => uuid -> shutdownStatus(status)) }
    val asyncInstalled = synchronized { mainPublicationScope.nonEmpty || legacyRepository.isEmpty }
    val requiredFinalizer = if asyncInstalled then Some(new IllegalStateException(
      "actual async Main pre-stop pump/B2 finalizer remains required; no legacy recall/reconcile/SQL ACK")) else None
    val entries = initial.map { case (uuid, status) =>
      val failure = requiredFinalizer.orElse(recall(uuid).failed.toOption)
      ManagerShutdownEntry(uuid, status, failure)
    }
    // async body/ticket/Unknown责任不进入旧release retry；本切片不实现完整B2 stop。
    val pendingFailure = requiredFinalizer.orElse(chunkLoadingManager.reconcilePendingTicketReleases().failed.toOption)
    val flushFailure = if asyncInstalled then None else flushCachedStates().failed.toOption
    val remaining = synchronized { managedByPod.view.mapValues(shutdownStatus).toMap }
    ManagerShutdownReport(entries, entries.collect { case entry if entry.cause.isEmpty => entry.podUuid }, remaining, pendingFailure, flushFailure,
      synchronized { trackedDriftPendingTagsByPod.keySet.toSet ++ mainPublicationScope.toSet.flatMap(_.retainedDriftPods) }, synchronized { dirtyPods.toSet }, asyncInstalled)

  /** 启动恢复失败时只撤销本次内存和实体发布；绝不触碰已接管 ticket 或持久化活动记录。 */
  private def requireReceipt(receipt: StartupRecoveryRollbackReceipt): Unit =
    if receipt == null || !(receipt.authority eq startupRecoveryAuthority) then
      throw new IllegalArgumentException("Startup recovery rollback receipt belongs to a different manager")
    if !receipt.open then throw new IllegalStateException(s"Startup recovery rollback receipt for ${receipt.podUuid} is already consumed")

  private def requireReceiptState(state: FloatingPodState, receipt: StartupRecoveryRollbackReceipt): Unit =
    require(state != null && receipt != null, "startup recovery state and receipt must not be null")
    if state != receipt.exactState || state.uuid != receipt.podUuid then
      throw new IllegalArgumentException(s"Startup recovery state does not match the exact receipt for ${receipt.podUuid}")

  private def retainBootCreatedBody(receipt: StartupRecoveryRollbackReceipt, body: ActivePodEntity): Unit =
    requireReceipt(receipt)
    if body == null then throw new IllegalStateException(s"World supplied a null created body for ${receipt.podUuid}")
    if receipt.initialBody.nonEmpty then throw staleStartupReceipt(receipt.podUuid, "cannot claim a pre-existing source body as boot-created")
    receipt.body match
      case Some(existing) if receipt.bootCreatedBody && sameRecoveryBody(existing, body) => ()
      case Some(_) => throw staleStartupReceipt(receipt.podUuid, "more than one body was reported for one boot spawn")
      case None =>
        receipt.body = Some(body)
        receipt.bootCreatedBody = true

  private def validateStartupRecoveryPrerequisites(state: FloatingPodState, world: PodWorld): UUID =
    if !state.isActive || state.dimensionId.isEmpty then throw new IllegalArgumentException(s"Recovery state ${state.uuid} must be active and dimensioned")
    if state.dimensionId.get != world.dimensionId then throw new IllegalArgumentException(s"Recovery dimension ${state.dimensionId.get} does not match world ${world.dimensionId}")
    val persisted = findPersistedState(state.uuid).getOrElse(throw new IllegalStateException(s"No committed state exists for ${state.uuid}"))
    if persisted != state then throw new IllegalStateException(s"Recovery state ${state.uuid} is not the exact committed database state")
    val attemptUuid = requireStartupActiveLocator(state, world)
    val ticket = chunkLoadingManager.managedTicketRecords().getOrElse(state.uuid, throw new IllegalStateException(s"No managed ticket exists for recovery pod ${state.uuid}"))
    if !ticket.world.sameWorld(world) || ticket.podUuid != state.uuid || ticket.ownershipKey == null then
      throw new IllegalStateException(s"Managed ticket does not canonically belong to recovery pod ${state.uuid}")
    attemptUuid

  private def validateReceiptCurrentState(receipt: StartupRecoveryRollbackReceipt): Unit =
    val expectedMapping =
      if receipt.managerMappingPublished then
        val body = receipt.body.getOrElse(throw staleStartupReceipt(receipt.podUuid, "published manager receipt has no body"))
        val attempt = receipt.attemptUuid.getOrElse(throw staleStartupReceipt(receipt.podUuid, "published manager receipt has no attempt UUID"))
        Some(StartupRecoveryManagerSnapshot(body, receipt.world, receipt.exactState, attempt))
      else receipt.initialMapping
    expectedMapping match
      case Some(expected) =>
        managedByPod.get(receipt.podUuid) match
          case Some(ManagedPodStatus.Active(record)) if managerRecordMatches(record, expected) => ()
          case _ => throw staleStartupReceipt(receipt.podUuid, "manager mapping no longer matches the receipt")
      case None =>
        if managedByPod.contains(receipt.podUuid) then throw staleStartupReceipt(receipt.podUuid, "manager mapping appeared after the receipt was captured")

    if receipt.bootCreatedBody then
      if receipt.initialBody.nonEmpty then throw staleStartupReceipt(receipt.podUuid, "boot-created body conflicts with the captured source body")
      val expectedBody = receipt.body.getOrElse(throw staleStartupReceipt(receipt.podUuid, "boot-created body receipt is missing"))
      if !receipt.world.isExactBodyResidentAndUnreplaced(expectedBody) then
        throw staleStartupReceipt(receipt.podUuid, "boot-created body is no longer resident or an identity index points to a replacement")
      if expectedBody.podUuid != receipt.podUuid && receipt.world.findPod(receipt.podUuid).nonEmpty then
        throw staleStartupReceipt(receipt.podUuid, "target UUID was replaced while the boot-created body used a different UUID")
    else receipt.initialBody match
      case Some(expectedBody) =>
        val currentBody = receipt.world.findPod(receipt.podUuid).getOrElse(throw staleStartupReceipt(receipt.podUuid, "captured source body disappeared"))
        if !sameRecoveryBody(currentBody, expectedBody) then throw staleStartupReceipt(receipt.podUuid, "captured source body was replaced")
      case None =>
        if receipt.world.findPod(receipt.podUuid).nonEmpty then throw staleStartupReceipt(receipt.podUuid, "a target body appeared after the receipt was captured")

  private def managerRecordMatches(record: ManagedActivePod, expected: StartupRecoveryManagerSnapshot): Boolean =
    record.world.sameWorld(expected.world) && record.exactState == expected.exactState && record.attemptUuid == expected.attemptUuid &&
      sameRecoveryBody(record.entity, expected.entity)

  private def restoreStartupManagerSnapshot(receipt: StartupRecoveryRollbackReceipt): Unit =
    managedByPod.remove(receipt.podUuid)
    receipt.initialCachedState match
      case Some(state) => cachedStates.put(receipt.podUuid, state)
      case None => cachedStates.remove(receipt.podUuid)
    if receipt.initialDirty then dirtyPods += receipt.podUuid else dirtyPods -= receipt.podUuid

  private def sameRecoveryBody(left: ActivePodEntity, right: ActivePodEntity): Boolean =
    if left == null || right == null || left.podUuid != right.podUuid || left.entityId != right.entityId then false
    else (left, right) match
      case (leftForge: ActivePodEntityImpl, rightForge: ActivePodEntityImpl) => leftForge.rawEntity eq rightForge.rawEntity
      case _ => left.eq(right)

  private def staleStartupReceipt(podUuid: UUID, detail: String): IllegalStateException =
    new IllegalStateException(s"Startup recovery receipt for $podUuid is stale: $detail")

  private def suppressCleanup(primary: Throwable, cleanup: Throwable): Unit =
    if cleanup ne primary then primary.addSuppressed(cleanup)

  private def shutdownStatus(status: ManagedPodStatus): ManagerShutdownStatus = status match
    case _: ManagedPodStatus.Active => ManagerShutdownStatus.Active
    case _: ManagedPodStatus.RecallPendingTicketRelease => ManagerShutdownStatus.PendingTicketRelease
    case _: ManagedPodStatus.RecallPendingCommit => ManagerShutdownStatus.PendingCommit
    case _: ManagedPodStatus.RecallFaulted => ManagerShutdownStatus.Faulted
    case _: ManagedPodStatus.DurablyActivatedFault => ManagerShutdownStatus.DurablyActivatedFault
    case _: ManagedPodStatus.DurablyDeactivatedFault => ManagerShutdownStatus.DurablyDeactivatedFault

  private def recallActive(podUuid: UUID, record: ManagedActivePod, expectedOwner: Option[UUID], expectedVersion: Long): RecallOutcome =
    requireLegacyMutation()
    if mainPublicationScope.exists(_.hasTrackedDriftRuntime) then
      throw new IllegalStateException("installed owned runtime requires the pending B2 pre-stop pump/finalizer; Main recall cannot execute legacy JDBC")
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
  private def publishRecovered(state: FloatingPodState, world: PodWorld, entity: ActivePodEntity, attemptUuid: UUID, onManagerMappingPublished: () => Unit = () => (), startupReceipt: Option[StartupRecoveryRollbackReceipt] = None): Unit =
    entity match
      case controlled: ControlledActivePodEntity =>
        val dbFirstRecoveryReceipt = startupReceipt.filter(_ => dbFirstReadOwner.nonEmpty)
        dbFirstRecoveryReceipt match
          case Some(receipt) =>
            requireReceipt(receipt)
            requireReceiptState(state, receipt)
            require(receipt.initialBody.isEmpty && receipt.initialMapping.isEmpty && receipt.bootCreatedBody &&
              receipt.body.exists(body => sameRecoveryBody(body, entity)),
              "schema6 source control requires this boot's exact newly-created body receipt")
            val binding = requireDbFirstStartupRecoveryBinding(state, world, attemptUuid)
            val body = entity match
              case actual: ActivePodEntityImpl => actual
              case _ => throw new IllegalStateException("schema6 recovery source requires the actual FloatingPod entity")
            val artifact = body.rawEntity.artifactIdentity().orElse(null)
            require(artifact != null && receipt.bootCreatedArtifactIdentity.contains(artifact) &&
              artifact.bindingUuid() == binding.bindingUuid && artifact.attemptUuid() == attemptUuid,
              "schema6 source control requires the exact receipt-bound artifact identity")
            world match
              case actual: PodWorldImpl => actual.assertLiveIndexedBody(body)
              case _ => throw new IllegalArgumentException("schema6 recovery requires the actual server world")
            val committed = FloatingPodEntityControl.committed(state, Some(binding.bindingUuid), Some(attemptUuid), None)
            body.rawEntity.applyCommittedDbFirstRecoveryControl(committed)
            val readback = body.rawEntity.serverControlSnapshot()
            require((readback eq committed) && body.rawEntity.hasCommittedSourceIdentity(readback) &&
              readback.bindingUuid() == binding.bindingUuid && readback.attemptUuid() == attemptUuid &&
              readback.operationUuid() == null && readback.podUuid() == state.uuid &&
              readback.ownerUuid() == state.ownerUuid.orNull && readback.operatingMode() == state.operatingMode.toString &&
              readback.active() == state.isActive && readback.stateVersion() == state.stateVersion &&
              readback.worldName() == state.worldName && readback.dimensionId() == state.dimensionId.get &&
              java.lang.Double.doubleToRawLongBits(readback.posX()) == java.lang.Double.doubleToRawLongBits(state.posX) &&
              java.lang.Double.doubleToRawLongBits(readback.posY()) == java.lang.Double.doubleToRawLongBits(state.posY) &&
              java.lang.Double.doubleToRawLongBits(readback.posZ()) == java.lang.Double.doubleToRawLongBits(state.posZ) &&
              java.lang.Float.floatToRawIntBits(readback.yaw()) == java.lang.Float.floatToRawIntBits(state.yaw) &&
              java.lang.Float.floatToRawIntBits(readback.pitch()) == java.lang.Float.floatToRawIntBits(state.pitch) &&
              FloatingPodEntityControl.sameActualPose(body.actualPose, state) &&
              body.rawEntity.artifactIdentity().orElse(null) == artifact,
              "schema6 installed control failed complete body/artifact identity readback")
          case None =>
            val attempt = startupReceipt match
              case Some(receipt) =>
                requireReceipt(receipt)
                requireReceiptState(state, receipt)
                requireStartupActiveLocator(state, world)
              case None => requireExactActiveLocator(state, world)
            controlled.applyCommittedMode(state, attempt)
      case _ if startupReceipt.exists(_ => dbFirstReadOwner.nonEmpty) =>
        throw new IllegalStateException("schema6 startup recovery requires a controlled actual FloatingPod body")
      case _ => ()
    managedByPod.put(state.uuid, ManagedPodStatus.Active(ManagedActivePod(entity, world, state, attemptUuid)))
    onManagerMappingPublished()
    cachedStates.put(state.uuid, state); dirtyPods.remove(state.uuid)
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
    requireLegacyMutation()
    validateActiveLocator(state, world, expectedAttemptUuid, physicalRecoveryStore.findPhysicalRecoveryLocator(state.uuid).get)

  private def requireStartupActiveLocator(state: FloatingPodState, world: PodWorld): UUID =
    val locator = dbFirstReadOwner match
      case Some(reads) =>
        val matches = reads.recoveryReadData.locators.filter(_.podUuid == state.uuid)
        if matches.size != 1 then throw new IllegalStateException(s"No unique schema6 Boot locator for ${state.uuid}")
        matches.headOption
      case None => mainBootReads match
        case Some(reads) =>
          val matches = reads.recoveryLocators.filter(_.podUuid == state.uuid)
          if matches.size != 1 then throw new IllegalStateException(s"No unique Boot locator for ${state.uuid}")
          matches.headOption
        case None => physicalRecoveryStore.findPhysicalRecoveryLocator(state.uuid).get
    validateActiveLocator(state, world, null, locator)

  private def validateActiveLocator(state: FloatingPodState, world: PodWorld, expectedAttemptUuid: UUID,
      locator: Option[PhysicalRecoveryLocator]): UUID =
    val expectedChunks = recoveryChunks(world.dimensionId, state.posX, state.posZ)
    locator match
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
    entity match
      case controlled: ControlledActivePodEntity =>
        try controlled.markControlFaulted(cause)
        catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
      case _ => ()
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
    if candidate.ownerUuid != cached.ownerUuid || candidate.stateVersion != cached.stateVersion || candidate.worldName != cached.worldName || candidate.isActive != cached.isActive || candidate.lastUpdated != cached.lastUpdated || candidate.dimensionId != cached.dimensionId || candidate.operatingMode != cached.operatingMode then throw new IllegalArgumentException(s"Cached runtime update for pod $podUuid changes critical state")
  private def chunkCoordinate(position: Double): Int = floor(position / ChunkLoadingManager.ChunkSize).toInt
  private def recoveryChunks(dimensionId: Int, posX: Double, posZ: Double): Set[PhysicalRecoveryChunk] =
    val centerX = chunkCoordinate(posX)
    val centerZ = chunkCoordinate(posZ)
    (for
      chunkX <- centerX - 2 to centerX + 2
      chunkZ <- centerZ - 2 to centerZ + 2
    yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

object ActiveFloatingPodManager:
  private[floatingpod] def forDbFirst(callbacks: ProductionDbFirstBootstrapCallbacks, tickets: ChunkLoadingManager): ActiveFloatingPodManager =
    require(callbacks != null && tickets != null, "original schema6 callbacks and tickets required")
    val manager = new ActiveFloatingPodManager(None, tickets, () => System.currentTimeMillis(), () => UUID.randomUUID())
    manager.installDbFirstMainReads(callbacks)
    manager
  private[floatingpod] def forClaimedBoot(reads: MainBootProjection, tickets: ChunkLoadingManager): ActiveFloatingPodManager =
    require(reads != null && tickets != null, "actual Boot reads and tickets required")
    reads.requireCurrentReads()
    val manager = new ActiveFloatingPodManager(None, tickets, () => System.currentTimeMillis(), () => UUID.randomUUID())
    manager.installMainBootReads(reads)
    manager
  def forForge(repository: FloatingPodRepository, modInstance: Object): ActiveFloatingPodManager = new ActiveFloatingPodManager(repository, ChunkLoadingManager.forForge(repository, modInstance))

  /** 原publication私有factory的静态事实；不捕获manager/scope/Boot/root。 */
  private final class FinalResident(val entity: FloatingPodEntity, val world: World,
      val control: FloatingPodControlSnapshot, val artifact: PodArtifactIdentity,
      val request: UUID, val checkpoint: UUID):
    val most: Long = entity.getUniqueID.getMostSignificantBits
    val least: Long = entity.getUniqueID.getLeastSignificantBits
    val x: Int = math.floor(control.posX()).toInt >> 4
    val z: Int = math.floor(control.posZ()).toInt >> 4
    def requireCurrent(): Unit =
      require(world.getMinecraftServer != null && world.getMinecraftServer.isCallingFromMinecraftThread &&
        !entity.isDead && (entity.world eq world) && (world.getEntityByID(entity.getEntityId) eq entity) &&
        (entity.serverControlSnapshot() eq control) && entity.getUniqueID.getMostSignificantBits == most &&
        entity.getUniqueID.getLeastSignificantBits == least && entity.posX == control.posX() &&
        entity.posY == control.posY() && entity.posZ == control.posZ() &&
        entity.rotationYaw == control.yaw() && entity.rotationPitch == control.pitch(),
        "original final resident body/control/pose changed")
      entity.requireCommittedSourceIdentity()
      entity.requireNormalStopFreeze(request)

  private final class FinalSaveSource(initialCount: Int, val retainedCharge: Long)
      extends ManagedOrdinarySaveAdmission.FinalSaveValidator:
    private val residents = new Array[FinalResident](initialCount)
    private val observed = new Array[Byte](initialCount)
    private def count: Int = residents.length
    private var closed = false
    def mint(index: Int, entity: FloatingPodEntity, control: FloatingPodControlSnapshot,
        artifact: PodArtifactIdentity, request: UUID, checkpoint: UUID): Unit =
      if entity != null then entity.requireCommittedSourceIdentity()
      require(!closed && index >= 0 && index < count && residents(index) == null &&
        entity != null && control != null && artifact != null && request != null && checkpoint != null &&
        (entity.serverControlSnapshot() eq control) && control.phase() == FloatingPodControlSnapshot.Phase.COMMITTED &&
        control.active() && control.operatingMode() == "WorldIdle" && control.podUuid() == entity.getUniqueID &&
        artifact.attemptUuid() == control.attemptUuid() &&
        (control.bindingUuid() == null || control.bindingUuid() == artifact.bindingUuid()),
        "original prepaid committed resident identity required")
      val registered = EntityList.getKey(entity)
      require(registered != null && registered.getNamespace == com.wi.floatingpod.FloatingPod.MOD_ID &&
        registered.getPath == FloatingPodEntityRegistry.ENTITY_NAME, "original registered FloatingPod entity required")
      val fact = new FinalResident(entity, entity.world, control, artifact, request, checkpoint)
      residents(index) = fact // 正常返回先归原责任槽；后检失败保留partial fact/ACK/费用，不重mint。
      fact.requireCurrent()
    override def endMainConsumers(): Unit =
      require(!closed,"original final source ends once")
      var i=0
      while i<count do
        residents(i)=null
        observed(i)=0
        i+=1
      closed=true
    override def requireComplete(): Unit =
      require(!closed, "final source closed")
      var i = 0
      while i < count do
        require(residents(i) != null, "final resident cohort incomplete")
        i += 1
    private def actualChunk(raw:Object,x:Int,z:Int):Chunk =
      val chunk=raw match
        case value:Chunk=>value
        case _=>throw new IllegalStateException("actual original final chunk required")
      require(chunk.x==x && chunk.z==z && chunk.getWorld.getMinecraftServer!=null &&
        chunk.getWorld.getMinecraftServer.isCallingFromMinecraftThread,"actual final Main chunk required")
      chunk
    private def foreachPod(chunk:Chunk)(use:FloatingPodEntity=>Unit):Unit =
      val sections=chunk.getEntityLists
      var section=0
      while section<sections.length do
        val iterator=sections(section).iterator()
        while iterator.hasNext do
          iterator.next() match
            case pod:FloatingPodEntity=>use(pod)
            case _=>()
        section+=1
    private def matches(body:NBTTagCompound,pod:FloatingPodEntity):Boolean =
      FinalPodSavePayload.matches(body,pod.getUniqueID,pod.posX,pod.posY,pod.posZ,
        pod.rotationYaw,pod.rotationPitch,pod.artifactIdentity().orElse(null),pod.diagnosticIdentity().orElse(null))
    private def samePhysical(a:FloatingPodEntity,b:FloatingPodEntity):Boolean =
      a.getUniqueID==b.getUniqueID && a.posX==b.posX && a.posY==b.posY && a.posZ==b.posZ &&
        a.rotationYaw==b.rotationYaw && a.rotationPitch==b.rotationPitch &&
        a.artifactIdentity().orElse(null)==b.artifactIdentity().orElse(null) &&
        a.diagnosticIdentity().orElse(null)==b.diagnosticIdentity().orElse(null)
    override def requireMainSave(rawWorld:Object,x:Int,z:Int,rawChunk:Object):Unit =
      requireComplete()
      val chunk=actualChunk(rawChunk,x,z)
      val world=chunk.getWorld
      require(world eq rawWorld,"original final chunk/world reference required")
      var i=0
      while i<count do
        val fact=residents(i)
        if (fact.world eq world) && fact.x==x && fact.z==z then
          fact.requireCurrent()
          var occurrences=0
          foreachPod(chunk)(pod=>if pod eq fact.entity then occurrences+=1)
          require(occurrences==1,"original final resident must remain in actual chunk exactly once")
        i+=1
      foreachPod(chunk) { pod=>
        require((pod.world eq world) && !pod.isDead && (world.getEntityByID(pod.getEntityId) eq pod) &&
          java.lang.Double.isFinite(pod.posX) && java.lang.Double.isFinite(pod.posY) && java.lang.Double.isFinite(pod.posZ) &&
          java.lang.Float.isFinite(pod.rotationYaw) && java.lang.Float.isFinite(pod.rotationPitch) &&
          (math.floor(pod.posX).toInt>>4)==x && (math.floor(pod.posZ).toInt>>4)==z,
          "actual final FloatingPod world/index/chunk/pose changed")
        var known=false
        var j=0
        while j<count && !known do
          known=residents(j).entity eq pod
          j+=1
        if !known then pod.requireInertDiagnosticSave()
      }
    /** 物理诊断壳不占活动256名额；实际数量只用于原B内的临时key-set费用。 */
    override def expectedArtifacts(rawChunk:Object,loader:Object,x:Int,z:Int,root:Object):Int =
      val chunk=actualChunk(rawChunk,x,z)
      requireMainSave(chunk.getWorld,x,z,chunk)
      var result=0
      foreachPod(chunk)(_=>result=Math.incrementExact(result))
      result
    override def requireMainPayload(rawChunk:Object,loader:Object,x:Int,z:Int,rawRoot:Object):Unit =
      val chunk=actualChunk(rawChunk,x,z)
      requireMainSave(chunk.getWorld,x,z,chunk) // 保存事件之后重核，不能把写前检查当写入许可。
      val root=rawRoot match
        case value:NBTTagCompound=>value
        case _=>throw new IllegalStateException("actual final root compound required")
      require(root.getTagId("Level")==10,"exact final Level compound required")
      val level=root.getCompoundTag("Level")
      require(level.getTagId("xPos")==3 && level.getTagId("zPos")==3 && level.getInteger("xPos")==x &&
        level.getInteger("zPos")==z && level.getTagId("Entities")==9,"actual final chunk coordinates/entities required")
      val entities=level.getTag("Entities").asInstanceOf[NBTTagList]
      require(entities.isEmpty || entities.getTagType==10,"exact final entity compounds required")
      var physicalCount=0
      foreachPod(chunk)(_=>physicalCount=Math.incrementExact(physicalCount))
      java.util.Arrays.fill(observed,0.toByte)
      var savedCount=0
      var n=0
      while n<entities.tagCount() do
        val body=entities.getCompoundTagAt(n)
        val pod=body.getTagId("id")==8 && body.getString("id")==FinalPodId
        if pod || body.getTag(PodArtifactNbtCodec.ROOT_KEY)!=null then
          savedCount=Math.incrementExact(savedCount)
          require(savedCount<=physicalCount,"final payload contains additional FloatingPod")
          FinalPodSavePayload.requireValid(body,FinalPodId)
          var matching=false
          foreachPod(chunk)(actual=>if matches(body,actual) then matching=true)
          require(matching,"final payload contains foreign FloatingPod UUID/artifact/pose")
          var i=0
          while i<count do
            val fact=residents(i)
            if (fact.world eq chunk.getWorld) && fact.x==x && fact.z==z && matches(body,fact.entity) then observed(i)=1
            i+=1
        n+=1
      require(savedCount==physicalCount,"final payload omitted actual FloatingPod")
      // 不以UUID唯一性抹掉合法诊断副本；逐等价类核对实际body与payload的完整重数。
      // 直接使用原Chunk/List，O(P*(P+N))，不建立与诊断数量增长的新集合或事实数组。
      foreachPod(chunk) { actual=>
        var physicalCopies=0
        foreachPod(chunk)(other=>if samePhysical(actual,other) then physicalCopies+=1)
        var savedCopies=0
        var j=0
        while j<entities.tagCount() do
          val body=entities.getCompoundTagAt(j)
          if body.getTagId("id")==8 && body.getString("id")==FinalPodId && matches(body,actual) then savedCopies+=1
          j+=1
        require(savedCopies==physicalCopies,"final FloatingPod identity multiplicity changed")
      }
      var i=0
      while i<count do
        val fact=residents(i)
        if (fact.world eq chunk.getWorld) && fact.x==x && fact.z==z then
          require(observed(i)==1,"expected final resident absent from payload")
        i+=1

  private val FinalPodId = com.wi.floatingpod.FloatingPod.MOD_ID + ":" + FloatingPodEntityRegistry.ENTITY_NAME

  private def alignedArray(payload: Long): Long = Math.multiplyExact(Math.floorDiv(Math.addExact(31L, payload), 8L), 8L)
  private def finalStringCharge(value: String): Long =
    if value == null then 0L else Math.addExact(32L, alignedArray(Math.multiplyExact(2L, value.length.toLong)))
  private def finalResidentCharge(control: FloatingPodControlSnapshot): Long =
    require(control != null && control.phase() == FloatingPodControlSnapshot.Phase.COMMITTED,
      "actual committed final control snapshot required")
    // fact88 + control144(133align136的保守包络) + artifact40 + 最多9个UUID32。
    Math.addExact(560L, Math.addExact(finalStringCharge(control.operatingMode()),
      Math.addExact(finalStringCharge(control.worldName()), finalStringCharge(control.faultType()))))
