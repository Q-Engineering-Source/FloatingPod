package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, UnboundCommitOutcome, UnboundCommitRejection, UnboundIntentOrigin, UnboundPreparationReceipt}

import java.util.UUID
import scala.collection.mutable
import scala.math.floor
import scala.util.Try

/** 受控新建与后续调试 inspect 的服务端入口；不接管外部世界实体的 adoption。 */
final class UnboundPodInitializationCoordinator(
    manager: ActiveFloatingPodManager,
    chunkLoadingManager: ChunkLoadingManager,
    expectedBinding: PhysicalRecoveryBinding
):
  require(manager != null && chunkLoadingManager != null && expectedBinding != null, "unbound initialization dependencies must not be null")
  require(expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified, "unbound initialization requires a certified physical recovery binding")

  private val store = manager.physicalRecoveryStoreForRuntime
  private val runtimeByPod = mutable.HashMap.empty[UUID, RuntimeInitialization]
  private val podByOperation = mutable.HashMap.empty[UUID, UUID]

  /**
   * 按 ticket → 同 UUID 冻结实体 → SQLite critical commit → manager mapping 顺序创建 ownerless WorldIdle。
   * 只在全部四步完成后返回 Committed inspect 快照。
   */
  def createDebugPod(
      proposedPodUuid: UUID,
      proposedAttemptUuid: UUID,
      operationUuid: UUID,
      targetPose: FloatingPodPose,
      world: ControlledPodWorld,
      globalActiveLimit: Int
  ): Try[DebugPodSnapshot] = Try {
    require(proposedPodUuid != null && proposedAttemptUuid != null && operationUuid != null, "controlled creation identities must not be null")
    require(targetPose != null && world != null, "controlled creation pose and world must not be null")
    require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be 1..256")
    if world.dimensionId != targetPose.dimensionId || world.diagnosticWorldName != targetPose.worldName then
      throw new IllegalArgumentException("Controlled creation target pose does not belong to the supplied server world")

    synchronized {
      podByOperation.get(operationUuid) match
        case Some(existingPodUuid) =>
          val existing = runtimeByPod.getOrElse(existingPodUuid, throw new IllegalStateException(s"Operation $operationUuid has no retained runtime status"))
          if existing.receipt.binding != expectedBinding || existing.receipt.targetPose != targetPose then
            throw new IllegalStateException(s"Operation $operationUuid was replayed with a different binding or target pose")
          if existing.phase == DebugPodPhase.Committed then
            val verified = verifyCommittedRuntime(existing)
            runtimeByPod.put(existing.receipt.podUuid, verified)
            if verified.phase == DebugPodPhase.Committed then snapshot(verified)
            else throw new IllegalStateException(s"Committed pod ${existing.receipt.podUuid} no longer passes runtime verification", verified.cause.orNull)
          else throw new IllegalStateException(s"Operation $operationUuid is ${existing.phase}; inspect the retained pod before retrying", existing.cause.orNull)
        case None => initializeFreshOrPersistedReplay(proposedPodUuid, proposedAttemptUuid, operationUuid, targetPose, world, globalActiveLimit)
    }
  }

  /** 返回领域行、当前受管实体、冻结镜像与原始失败；PREPARED/Faulted 均不是初始化成功。 */
  def inspect(podUuid: UUID): Try[DebugPodSnapshot] = Try {
    require(podUuid != null, "podUuid must not be null")
    synchronized {
      runtimeByPod.get(podUuid).map { runtime =>
        val current = if runtime.phase == DebugPodPhase.Committed then verifyCommittedRuntime(runtime) else runtime
        runtimeByPod.put(podUuid, current)
        snapshot(current)
      }.getOrElse(inspectPersisted(podUuid))
    }
  }

  private def initializeFreshOrPersistedReplay(
      proposedPodUuid: UUID,
      proposedAttemptUuid: UUID,
      operationUuid: UUID,
      targetPose: FloatingPodPose,
      world: ControlledPodWorld,
      globalActiveLimit: Int
  ): DebugPodSnapshot =
    val receipt = store.prepareUnboundCreate(expectedBinding, proposedPodUuid, proposedAttemptUuid, targetPose, operationUuid).get
    validateReceipt(receipt, targetPose, operationUuid)
    podByOperation.put(operationUuid, receipt.podUuid)
    runtimeByPod.get(receipt.podUuid) match
      case Some(existing) if existing.receipt == receipt && existing.phase == DebugPodPhase.Committed => snapshot(existing)
      case Some(existing) => throw new IllegalStateException(s"Prepared pod ${receipt.podUuid} already has retained ${existing.phase} responsibility", existing.cause.orNull)
      case None =>
        val persisted = manager.findPersistedState(receipt.podUuid).getOrElse(throw new IllegalStateException(s"No domain row exists for prepared pod ${receipt.podUuid}"))
        if persisted.isActive then
          val details = manager.managedActiveDetails(receipt.podUuid).getOrElse(throw new IllegalStateException(s"Committed controlled pod ${receipt.podUuid} requires startup recovery; refusing another ticket or body"))
          details.entity match
            case entity: ControlledActivePodEntity if details.world.sameWorld(world) && details.exactState == persisted && details.attemptUuid == receipt.attemptUuid && entity.hasControlledCreationStamp(receipt) && entity.actualPose == receipt.targetPose =>
              val recovered = verifyCommittedRuntime(RuntimeInitialization(receipt, world, Some(entity), DebugPodPhase.Committed, Some(persisted), None))
              runtimeByPod.put(receipt.podUuid, recovered)
              if recovered.phase == DebugPodPhase.Committed then snapshot(recovered)
              else throw new IllegalStateException(s"Committed pod ${receipt.podUuid} no longer passes runtime verification", recovered.cause.orNull)
            case _ => throw new IllegalStateException(s"Active pod ${receipt.podUuid} does not match the exact controlled creation receipt")
        else
          validatePreparedState(receipt, persisted)
          validatePreparedLocator(receipt)
          if chunkLoadingManager.managedTicketRecords().contains(receipt.podUuid) then
            throw new IllegalStateException(s"Prepared pod ${receipt.podUuid} already has a runtime ticket outside this coordinator; refusing duplicate allocation")
          world.findPod(receipt.podUuid) match
            case Some(_) => throw new IllegalStateException(s"Prepared pod UUID ${receipt.podUuid} already exists in the target world; refusing to adopt it as a new controlled create")
            case None => ()

          var current = RuntimeInitialization(receipt, world, None, DebugPodPhase.Prepared, Some(persisted), None)
          runtimeByPod.put(receipt.podUuid, current)
          var publishedEntity: ControlledActivePodEntity = null
          try
            val currentActiveCount = manager.persistedActivePodCount
            if currentActiveCount >= globalActiveLimit then
              throw new IllegalStateException(s"Controlled pod ${receipt.podUuid} remains prepared because active capacity is full ($currentActiveCount/$globalActiveLimit)")
            chunkLoadingManager.requestTicket(receipt.podUuid, world, chunkCoordinate(targetPose.posX), chunkCoordinate(targetPose.posZ)).get
            current = current.copy(phase = DebugPodPhase.Ticketed)
            runtimeByPod.put(receipt.podUuid, current)

            val spawned = world.spawnControlledPod(persisted, receipt, entity =>
              publishedEntity = entity
              current = current.copy(entity = Option(entity), phase = DebugPodPhase.PendingCleanup)
              runtimeByPod.put(receipt.podUuid, current)
              ()
            )
            if publishedEntity == null then
              throw new IllegalStateException(s"Controlled world returned pod ${receipt.podUuid} without retaining its pre-publication entity handle")
            if !spawned.sameBody(publishedEntity) then
              throw new IllegalStateException(s"Controlled world returned a different body than its retained pre-publication handle for ${receipt.podUuid}")
            validatePublishedEntity(receipt, world, spawned)

            val committed = store.commitUnboundCreate(
              expectedBinding,
              receipt.podUuid,
              receipt.attemptUuid,
              receipt.preparedVersion,
              spawned.actualPose,
              operationUuid,
              globalActiveLimit
            ).get match
              case UnboundCommitOutcome.Committed(state) => state
              case UnboundCommitOutcome.AlreadyCommitted(state) => state
              case UnboundCommitOutcome.Rejected(UnboundCommitRejection.CapacityReached(activeCount, limit)) =>
                throw new IllegalStateException(s"Controlled pod ${receipt.podUuid} remains prepared after global active capacity rejection ($activeCount/$limit)")
              case UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch) =>
                throw new IllegalStateException(s"Controlled pod ${receipt.podUuid} moved away from its exact prepared pose before commit")

            validateCommittedState(receipt, spawned.actualPose, committed)
            manager.publishControlledUnboundCreate(receipt, world, spawned, committed)
            current = current.copy(phase = DebugPodPhase.Committed, state = Some(committed), cause = None)
            current = verifyCommittedRuntime(current)
            runtimeByPod.put(receipt.podUuid, current)
            if current.phase != DebugPodPhase.Committed then
              throw new IllegalStateException(s"Controlled pod ${receipt.podUuid} failed immediate committed runtime verification", current.cause.orNull)
            snapshot(current)
          catch
            case primary: Throwable =>
              var exactReadback = Option.empty[FloatingPodState]
              var stateReadbackSucceeded = false
              try
                exactReadback = manager.findPersistedState(receipt.podUuid)
                stateReadbackSucceeded = true
              catch case secondary: Throwable => addSuppressed(primary, secondary)
              var indexedEntity = Option.empty[ControlledActivePodEntity]
              try
                indexedEntity = world.findPod(receipt.podUuid).collect { case controlled: ControlledActivePodEntity => controlled }
              catch case secondary: Throwable => addSuppressed(primary, secondary)
              val latestEntity = Option(publishedEntity).orElse(indexedEntity)
              val committedReadback = exactReadback.exists(state => state.isActive && state.uuid == receipt.podUuid && state.ownerUuid.isEmpty && state.operatingMode == PodOperatingMode.WorldIdle)
              val faultedPhase =
                if committedReadback || !stateReadbackSucceeded && current.phase != DebugPodPhase.Prepared then DebugPodPhase.Faulted
                else if current.phase == DebugPodPhase.PendingCleanup || latestEntity.nonEmpty then DebugPodPhase.PendingCleanup
                else current.phase
              val failed = current.copy(entity = latestEntity, phase = faultedPhase, state = exactReadback.orElse(current.state), cause = Some(primary))
              runtimeByPod.put(receipt.podUuid, failed)
              throw primary

  private def inspectPersisted(podUuid: UUID): DebugPodSnapshot =
    val prepared = store.findUnboundPreparation(podUuid).get
    val state = manager.findPersistedState(podUuid)
    manager.managedActiveDetails(podUuid) match
      case Some(details) =>
        val controlled = details.entity match
          case entity: ControlledActivePodEntity => Some(entity)
          case _ => None
        val failure = state match
          case Some(row) =>
            try
              validateManagedRuntime(details, row, prepared)
              None
            catch case problem: Throwable => Some(problem)
          case None => Some(new IllegalStateException(s"No persisted state exists for managed pod $podUuid"))
        val phase = if failure.isEmpty then DebugPodPhase.Committed else DebugPodPhase.Faulted
        val receipt = prepared
        DebugPodSnapshot(
          podUuid,
          phase,
          state.flatMap(_.ownerUuid),
          state.map(_.operatingMode),
          state.exists(_.isActive),
          state.map(_.stateVersion),
          controlled.map(_.actualPose),
          controlled.map(_.isFrozen),
          receipt.map(_.binding.bindingUuid).orElse(store.findPhysicalRecoveryBinding().get.map(_.bindingUuid)),
          Some(details.attemptUuid),
          receipt.flatMap(_.createOperationUuid),
          failure
        )
      case None =>
        val row = state.getOrElse(throw new IllegalArgumentException(s"Unknown FloatingPod $podUuid"))
        val receipt = prepared
        val phase = if !row.isActive && receipt.nonEmpty then DebugPodPhase.Prepared else DebugPodPhase.Faulted
        DebugPodSnapshot(
          podUuid,
          phase,
          row.ownerUuid,
          Some(row.operatingMode),
          row.isActive,
          Some(row.stateVersion),
          None,
          None,
          receipt.map(_.binding.bindingUuid),
          receipt.map(_.attemptUuid),
          receipt.flatMap(_.createOperationUuid),
          if row.isActive then Some(new IllegalStateException(s"Active FloatingPod $podUuid has not been attached to the runtime manager")) else None
        )

  private def verifyCommittedRuntime(runtime: RuntimeInitialization): RuntimeInitialization =
    try
      val receipt = runtime.receipt
      val state = manager.findPersistedState(receipt.podUuid).getOrElse(throw new IllegalStateException(s"Committed pod ${receipt.podUuid} has no persisted row"))
      val details = manager.managedActiveDetails(receipt.podUuid).getOrElse(throw new IllegalStateException(s"Committed pod ${receipt.podUuid} is absent from the active manager"))
      val entity = runtime.entity.getOrElse(throw new IllegalStateException(s"Committed pod ${receipt.podUuid} has no retained entity handle"))
      validateManagedRuntime(details, state, Some(receipt))
      if !runtime.world.sameWorld(details.world) then throw new IllegalStateException(s"Committed pod ${receipt.podUuid} manager world changed")
      if details.attemptUuid != receipt.attemptUuid || details.exactState != state then throw new IllegalStateException(s"Committed pod ${receipt.podUuid} manager version or attempt changed")
      details.entity match
        case managed: ControlledActivePodEntity if managed.sameBody(entity) => ()
        case _ => throw new IllegalStateException(s"Committed pod ${receipt.podUuid} manager entity changed")
      runtime.world.findPod(receipt.podUuid) match
        case Some(indexed: ControlledActivePodEntity) if indexed.sameBody(entity) => ()
        case _ => throw new IllegalStateException(s"Committed pod ${receipt.podUuid} is no longer the exact indexed world entity")
      runtime.copy(phase = DebugPodPhase.Committed, state = Some(state), cause = None)
    catch
      case failure: Throwable => runtime.copy(phase = DebugPodPhase.Faulted, cause = Some(failure))

  private def validateManagedRuntime(
      details: ManagedActiveDetails,
      state: FloatingPodState,
      receipt: Option[UnboundPreparationReceipt]
  ): Unit =
    if details.exactState != state || !state.isActive || state.dimensionId.isEmpty || details.entity.podUuid != state.uuid then
      throw new IllegalStateException(s"Persisted state for ${state.uuid} is not the exact active manager state")
    val entity = details.entity match
      case controlled: ControlledActivePodEntity => controlled
      case _ => throw new IllegalStateException(s"Managed pod ${state.uuid} does not expose verifiable runtime control state")
    if details.world.dimensionId != state.dimensionId.get || entity.actualPose != poseOf(state) || entity.isFrozen != (state.operatingMode == PodOperatingMode.WorldIdle) then
      throw new IllegalStateException(s"Managed pod ${state.uuid} runtime pose or frozen mirror differs from committed state")
    receipt.foreach { prepared =>
      if prepared.podUuid != state.uuid || prepared.binding != expectedBinding || prepared.attemptUuid != details.attemptUuid then
        throw new IllegalStateException(s"Managed pod ${state.uuid} does not match its exact unbound preparation receipt")
      if prepared.origin == UnboundIntentOrigin.DebugItemCreate && (!entity.hasControlledCreationStamp(prepared) || entity.actualPose != prepared.targetPose) then
        throw new IllegalStateException(s"Managed pod ${state.uuid} lacks its exact controlled-creation stamp or pose")
    }
    details.world.findPod(state.uuid) match
      case Some(indexed: ControlledActivePodEntity) if indexed.sameBody(entity) => ()
      case _ => throw new IllegalStateException(s"Managed pod ${state.uuid} is not the exact indexed world entity")
    chunkLoadingManager.managedTicketRecords().get(state.uuid) match
      case Some(ticket) if ticket.podUuid == state.uuid && ticket.world.sameWorld(details.world) && ticket.ownershipKey != null => ()
      case _ => throw new IllegalStateException(s"Managed pod ${state.uuid} has no exact world-owned ticket")
    val binding = store.findPhysicalRecoveryBinding().get.getOrElse(throw new IllegalStateException("Physical recovery binding is missing during inspect"))
    if binding != expectedBinding || binding.cutoverState != PhysicalRecoveryCutoverState.Certified then
      throw new IllegalStateException("Current physical recovery binding no longer matches the certified runtime binding")
    validateActiveLocator(state, details.world, details.attemptUuid)

  private def validateActiveLocator(state: FloatingPodState, world: PodWorld, attemptUuid: UUID): Unit =
    val expectedChunks = recoveryChunks(poseOf(state))
    store.findPhysicalRecoveryLocator(state.uuid).get match
      case Some(locator)
          if locator.podUuid == state.uuid
            && locator.attemptUuid == attemptUuid
            && locator.state == PhysicalRecoveryLocatorState.RowActive
            && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive
            && locator.domainVersion.contains(state.stateVersion)
            && locator.targetDimensionId == state.dimensionId
            && locator.targetDimensionId.contains(world.dimensionId)
            && locator.chunks.size == expectedChunks.size
            && locator.chunks.toSet == expectedChunks => ()
      case _ => throw new IllegalStateException(s"Active pod ${state.uuid} has no exact ROW_ACTIVE locator")

  private def snapshot(runtime: RuntimeInitialization): DebugPodSnapshot =
    val entity = runtime.entity
    val state = runtime.state.orElse(manager.findPersistedState(runtime.receipt.podUuid))
    DebugPodSnapshot(
      runtime.receipt.podUuid,
      runtime.phase,
      state.flatMap(_.ownerUuid),
      state.map(_.operatingMode),
      state.exists(_.isActive),
      state.map(_.stateVersion),
      entity.map(_.actualPose),
      entity.map(_.isFrozen),
      Some(runtime.receipt.binding.bindingUuid),
      Some(runtime.receipt.attemptUuid),
      runtime.receipt.createOperationUuid,
      runtime.cause
    )

  private def validateReceipt(receipt: UnboundPreparationReceipt, targetPose: FloatingPodPose, operationUuid: UUID): Unit =
    if receipt == null || receipt.binding != expectedBinding || receipt.origin != UnboundIntentOrigin.DebugItemCreate
      || receipt.targetPose != targetPose || receipt.createOperationUuid != Some(operationUuid) || receipt.observedIdentity.nonEmpty then
      throw new IllegalStateException("Storage returned a controlled-create receipt that does not match the exact operation")

  private def validatePreparedState(receipt: UnboundPreparationReceipt, state: FloatingPodState): Unit =
    if state.uuid != receipt.podUuid || state.ownerUuid.nonEmpty || state.operatingMode != PodOperatingMode.WorldIdle
      || state.isActive || state.stateVersion != receipt.preparedVersion || state.dimensionId != Some(receipt.targetPose.dimensionId)
      || poseOf(state) != receipt.targetPose then
      throw new IllegalStateException(s"Prepared row for ${receipt.podUuid} is not the exact inactive ownerless WorldIdle state")

  private def validatePreparedLocator(receipt: UnboundPreparationReceipt): Unit =
    val expectedChunks = recoveryChunks(receipt.targetPose)
    store.findPhysicalRecoveryLocator(receipt.podUuid).get match
      case Some(locator)
          if locator.podUuid == receipt.podUuid
            && locator.attemptUuid == receipt.attemptUuid
            && locator.state == PhysicalRecoveryLocatorState.DebugItemCreatePrepared
            && locator.reasonCode == PhysicalRecoveryLocatorReason.DebugItemCreatePrepared
            && locator.domainVersion.contains(receipt.preparedVersion)
            && locator.targetDimensionId.contains(receipt.targetPose.dimensionId)
            && locator.chunks.size == expectedChunks.size
            && locator.chunks.toSet == expectedChunks => ()
      case _ => throw new IllegalStateException(s"No exact DEBUG_ITEM_CREATE_PREPARED locator exists for ${receipt.podUuid}")

  private def validatePublishedEntity(receipt: UnboundPreparationReceipt, world: ControlledPodWorld, entity: ControlledActivePodEntity): Unit =
    if entity == null || entity.podUuid != receipt.podUuid || entity.actualPose != receipt.targetPose || !entity.isFrozen || !entity.hasControlledCreationStamp(receipt) then
      throw new IllegalStateException(s"Published entity ${receipt.podUuid} does not match its prepared receipt, exact pose, or frozen control")
    world.findPod(receipt.podUuid) match
      case Some(found: ControlledActivePodEntity) if found.sameBody(entity) => ()
      case Some(_) => throw new IllegalStateException(s"World ${world.diagnosticWorldName} indexed a different entity for controlled pod ${receipt.podUuid}")
      case None => throw new IllegalStateException(s"World ${world.diagnosticWorldName} did not index controlled pod ${receipt.podUuid}")

  private def validateCommittedState(receipt: UnboundPreparationReceipt, actualPose: FloatingPodPose, state: FloatingPodState): Unit =
    if state.uuid != receipt.podUuid || state.ownerUuid.nonEmpty || state.operatingMode != PodOperatingMode.WorldIdle || !state.isActive
      || state.stateVersion != receipt.preparedVersion + 1L || state.dimensionId != Some(receipt.targetPose.dimensionId)
      || actualPose != receipt.targetPose || poseOf(state) != actualPose then
      throw new IllegalStateException(s"Storage returned an invalid committed state for controlled pod ${receipt.podUuid}")

  private def poseOf(state: FloatingPodState): FloatingPodPose =
    FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.dimensionId.getOrElse(throw new IllegalStateException(s"Pod ${state.uuid} has no persisted dimension")))

  private def recoveryChunks(pose: FloatingPodPose): Set[PhysicalRecoveryChunk] =
    val centerX = chunkCoordinate(pose.posX)
    val centerZ = chunkCoordinate(pose.posZ)
    (for
      chunkX <- centerX - 2 to centerX + 2
      chunkZ <- centerZ - 2 to centerZ + 2
    yield PhysicalRecoveryChunk(pose.dimensionId, chunkX, chunkZ)).toSet

  private def addSuppressed(primary: Throwable, secondary: Throwable): Unit =
    if primary ne secondary then primary.addSuppressed(secondary)

  private def chunkCoordinate(position: Double): Int = floor(position / ChunkLoadingManager.ChunkSize).toInt

  private final case class RuntimeInitialization(
      receipt: UnboundPreparationReceipt,
      world: ControlledPodWorld,
      entity: Option[ControlledActivePodEntity],
      phase: DebugPodPhase,
      state: Option[FloatingPodState],
      cause: Option[Throwable]
  )
