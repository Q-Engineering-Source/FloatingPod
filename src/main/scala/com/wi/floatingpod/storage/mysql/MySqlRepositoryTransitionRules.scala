package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{
  AdmissionOutcome,
  AdmissionRejection,
  DeactivationOutcome,
  DeactivationRejection,
  OperatingModeChangeOutcome,
  OperatingModeChangeRejection,
  OptimisticLockException,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryCutoverState,
  PhysicalRecoveryLocator,
  PhysicalRecoveryLocatorReason,
  PhysicalRecoveryLocatorState,
  RecoveryDimensionBackfillOutcome,
  RecoveryDimensionBackfillRejection,
  StoredPodRow
}

import java.util.{HexFormat, UUID}

/** 对锁定全图作纯内存状态转换；所有 DML 由外层一次冻结的 typed delta 执行。 */
private[mysql] object MySqlRepositoryTransitionRules:
  def insert(snapshot: MySqlFullRepositorySnapshot, state: FloatingPodState): (MySqlFullRepositorySnapshot, FloatingPodState) =
    require(state != null, "state must not be null")
    if state.isActive then throw new IllegalArgumentException("Generic insert only accepts inactive FloatingPodState")
    val key = MySqlStoredRows.encodeUuid(state.uuid)
    if snapshot.pods.exists(_.uuidBytes == key) then throw new IllegalStateException(s"FloatingPodState already exists for UUID ${state.uuid}")
    if snapshot.recovery.locators.exists(_.podUuid == state.uuid) || snapshot.recovery.chunks.exists(_.podUuid == state.uuid) ||
      snapshot.recovery.intents.exists(_.podUuid == state.uuid) then
      throw new IllegalStateException(s"Physical recovery history already exists for pod ${state.uuid}")
    val raw = toRaw(state)
    (snapshot.copy(pods = snapshot.pods :+ raw), state)

  def update(snapshot: MySqlFullRepositorySnapshot, requested: FloatingPodState): (MySqlFullRepositorySnapshot, FloatingPodState) =
    require(requested != null, "state must not be null")
    val current = findPod(snapshot, requested.uuid).getOrElse(throw new OptimisticLockException(requested.uuid, requested.stateVersion))
    if current.stateVersion != requested.stateVersion then throw new OptimisticLockException(requested.uuid, requested.stateVersion)
    val currentState = MySqlStoredRows.materializeRawRow(current)
    if currentState.operatingMode != requested.operatingMode then
      throw new IllegalArgumentException(s"Generic update cannot change operating mode for pod ${requested.uuid}")
    val nextVersion = Math.addExact(current.stateVersion, 1L)
    val updatedAt = System.currentTimeMillis()
    val updated = toRaw(requested.copy(
      stateVersion = nextVersion,
      lastUpdated = updatedAt,
      isActive = current.isActiveFlag == 1,
      dimensionId = current.dimensionId,
      operatingMode = currentState.operatingMode
    ))
    val locator = findLocator(snapshot, requested.uuid)
    val nextRecovery = locator match
      case None => snapshot.recovery
      case Some(value) if value.state == PhysicalRecoveryLocatorState.RowActive =>
        requireCurrentActiveLocator(snapshot, current, value, value.attemptUuid)
        if !requireSameChunk(current.posX, requested.posX) || !requireSameChunk(current.posZ, requested.posZ) then
          throw new IllegalStateException(s"Generic update crossed a chunk boundary for pod ${requested.uuid}")
        snapshot.recovery.copy(locators = snapshot.recovery.locators.map { row =>
          if row.podUuid == requested.uuid then row.copy(domainVersion = Some(nextVersion)) else row
        })
      case Some(_) =>
        throw new IllegalStateException(s"Generic update cannot change a pod with a non-active physical recovery obligation: ${requested.uuid}")
    val next = replacePod(snapshot.copy(recovery = nextRecovery), updated)
    (next, MySqlStoredRows.materializeGenericState(updated))

  def delete(snapshot: MySqlFullRepositorySnapshot, uuid: UUID): (MySqlFullRepositorySnapshot, Boolean) =
    require(uuid != null, "uuid must not be null")
    findPod(snapshot, uuid) match
      case None => (snapshot, false)
      case Some(row) if row.isActiveFlag == 1 =>
        throw new IllegalStateException(s"Cannot delete active FloatingPod ${uuid}")
      case Some(_) if snapshot.recovery.locators.exists(_.podUuid == uuid) ||
          snapshot.recovery.chunks.exists(_.podUuid == uuid) || snapshot.recovery.intents.exists(_.podUuid == uuid) =>
        throw new IllegalStateException(s"Cannot delete FloatingPod ${uuid} with physical recovery history")
      case Some(_) =>
        (snapshot.copy(pods = snapshot.pods.filterNot(_.uuidBytes == MySqlStoredRows.encodeUuid(uuid))), true)

  def prepareCreateLocator(
      snapshot: MySqlFullRepositorySnapshot,
      podUuid: UUID,
      expectedDomainVersion: Long,
      attemptUuid: UUID,
      targetDimensionId: Int,
      chunks: Set[PhysicalRecoveryChunk]
  ): (MySqlFullRepositorySnapshot, PhysicalRecoveryLocator) =
    require(podUuid != null && attemptUuid != null && chunks != null, "locator identity and chunks are required")
    require(expectedDomainVersion >= 0L, "expectedDomainVersion must not be negative")
    require(chunks.size == 25 && chunks.forall(_.dimensionId == targetDimensionId) && isExactSquare(chunks), "CREATE_PREPARED requires an exact 5x5 chunk square")
    val pod = findPod(snapshot, podUuid).getOrElse(throw new IllegalStateException(s"Cannot prepare locator for unknown pod ${podUuid}"))
    if pod.isActiveFlag == 1 then throw new IllegalStateException(s"Cannot prepare locator for active pod ${podUuid}")
    if pod.stateVersion != expectedDomainVersion then throw new OptimisticLockException(podUuid, expectedDomainVersion)
    if snapshot.recovery.intents.exists(_.podUuid == podUuid) then throw new IllegalStateException(s"Unbound history already exists for pod ${podUuid}")
    val orderedChunks = chunks.toList.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ))
    val desired = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared,
      PhysicalRecoveryLocatorReason.CreatePrepared, Some(expectedDomainVersion), Some(targetDimensionId), orderedChunks)
    findLocator(snapshot, podUuid) match
      case Some(existing) if existing == desired => (snapshot, existing)
      case Some(_) => throw new IllegalStateException(s"Physical recovery locator already conflicts for pod ${podUuid}")
      case None =>
        val rawChunks = snapshot.recovery.chunks ++ orderedChunks.map(c => MySqlRecoveryChunkRow(podUuid, c))
        val next = snapshot.copy(recovery = snapshot.recovery.copy(locators = snapshot.recovery.locators :+ desired, chunks = rawChunks))
        (next, desired)

  def activate(
      snapshot: MySqlFullRepositorySnapshot,
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      attemptUuid: UUID
  ): (MySqlFullRepositorySnapshot, AdmissionOutcome) =
    require(attemptUuid != null, "activation attempt UUID is required")
    activateWithAttempt(snapshot, target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, Some(attemptUuid))

  def activateUsingPreparedLocator(
      snapshot: MySqlFullRepositorySnapshot,
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): (MySqlFullRepositorySnapshot, AdmissionOutcome) =
    activateWithAttempt(snapshot, target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, None)

  private def activateWithAttempt(
      snapshot: MySqlFullRepositorySnapshot,
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      requestedAttempt: Option[UUID]
  ): (MySqlFullRepositorySnapshot, AdmissionOutcome) =
    require(target != null && authenticatedOwner != null && requestedAttempt != null, "activation target, owner, and attempt option are required")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be between 1 and 256")
    if target.isActive then throw new IllegalArgumentException("target must describe an inactive state before activation")
    val stored = findPod(snapshot, target.uuid)
    val rejected = stored match
      case None => Some(AdmissionRejection.UnknownPod)
      case Some(row) if MySqlStoredRows.decodeNullableOwnerUuid(row.ownerUuidBytes) != Some(authenticatedOwner) => Some(AdmissionRejection.NotOwner)
      case Some(row) if row.stateVersion != expectedStateVersion => Some(AdmissionRejection.VersionMismatch(row.stateVersion))
      case Some(row) if row.isActiveFlag == 1 => Some(AdmissionRejection.AlreadyActive)
      case Some(_) if snapshot.pods.count(_.isActiveFlag == 1) >= globalActiveLimit => Some(AdmissionRejection.GlobalActiveLimitReached)
      case Some(_) if activeCountFor(snapshot, authenticatedOwner) >= entitlementFor(snapshot, authenticatedOwner).simultaneousActiveLimit =>
        Some(AdmissionRejection.PlayerActiveLimitReached)
      case _ => None
    rejected match
      case Some(reason) => (snapshot, AdmissionOutcome.Rejected(reason))
      case None =>
        val row = stored.get
        val prior = MySqlStoredRows.materializeRawRow(row)
        if prior.operatingMode != target.operatingMode then
          throw new IllegalArgumentException(s"Activation cannot change operating mode for pod ${target.uuid}")
        val locator = findLocator(snapshot, target.uuid).getOrElse(
          throw new IllegalStateException(s"CREATE_PREPARED locator is required to activate pod ${target.uuid}")
        )
        val expectedChunks = chunksAt(dimensionId, target.posX, target.posZ)
        if requestedAttempt.exists(_ != locator.attemptUuid) || locator.state != PhysicalRecoveryLocatorState.CreatePrepared ||
          locator.reasonCode != PhysicalRecoveryLocatorReason.CreatePrepared ||
          locator.domainVersion != Some(expectedStateVersion) || locator.targetDimensionId != Some(dimensionId) ||
          locator.chunks.toSet != expectedChunks || locator.chunks.size != 25 then
          throw new IllegalStateException(s"No exact CREATE_PREPARED physical locator exists for pod ${target.uuid}")
        val nextVersion = Math.addExact(row.stateVersion, 1L)
        val activeState = target.copy(
          uuid = prior.uuid,
          ownerUuid = prior.ownerUuid,
          stateVersion = nextVersion,
          isActive = true,
          lastUpdated = System.currentTimeMillis(),
          dimensionId = Some(dimensionId),
          operatingMode = prior.operatingMode
        )
        val activeRaw = toRaw(activeState)
        val activeLocator = locator.copy(state = PhysicalRecoveryLocatorState.RowActive,
          reasonCode = PhysicalRecoveryLocatorReason.RowActive, domainVersion = Some(nextVersion))
        val next = replaceLocator(replacePod(snapshot, activeRaw), activeLocator)
        (next, AdmissionOutcome.Accepted(MySqlStoredRows.materializeGenericState(activeRaw)))

  def deactivate(
      snapshot: MySqlFullRepositorySnapshot,
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long,
      requiredAttempt: Option[UUID]
  ): (MySqlFullRepositorySnapshot, DeactivationOutcome) =
    require(podUuid != null && expectedOwner != null, "deactivation identity is required")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    val current = findPod(snapshot, podUuid)
    val rejected = current match
      case None => Some(DeactivationRejection.UnknownPod)
      case Some(row) if MySqlStoredRows.decodeNullableOwnerUuid(row.ownerUuidBytes) != expectedOwner => Some(DeactivationRejection.NotOwner)
      case Some(row) if row.stateVersion != expectedStateVersion => Some(DeactivationRejection.VersionMismatch(row.stateVersion))
      case Some(row) if row.isActiveFlag != 1 => Some(DeactivationRejection.NotActive)
      case _ => None
    rejected match
      case Some(reason) => (snapshot, DeactivationOutcome.Rejected(reason))
      case None =>
        val row = current.get
        val nextVersion = Math.addExact(row.stateVersion, 1L)
        val locator = findLocator(snapshot, podUuid)
        val nextLocator = locator.map { value =>
          requireCurrentActiveLocator(snapshot, row, value, requiredAttempt.getOrElse(value.attemptUuid))
          if value.state != PhysicalRecoveryLocatorState.RowActive || value.reasonCode != PhysicalRecoveryLocatorReason.RowActive then
            throw new IllegalStateException(s"Exact active physical locator is required for pod ${podUuid}")
          if requiredAttempt.exists(_ != value.attemptUuid) then
            throw new IllegalStateException(s"Physical recovery attempt mismatch for pod ${podUuid}")
          value.copy(state = PhysicalRecoveryLocatorState.RowInactivePending,
            reasonCode = PhysicalRecoveryLocatorReason.RowInactivePending, domainVersion = Some(nextVersion))
        }
        if requiredAttempt.nonEmpty && locator.isEmpty then
          throw new IllegalStateException(s"Exact active physical locator is required for pod ${podUuid}")
        val nextRaw = row.copy(isActiveFlag = 0, stateVersion = nextVersion, lastUpdated = System.currentTimeMillis())
        val withPod = replacePod(snapshot, nextRaw)
        val next = nextLocator.map(replaceLocator(withPod, _)).getOrElse(withPod)
        (next, DeactivationOutcome.Accepted(MySqlStoredRows.materializeGenericState(nextRaw)))

  def trackedDrift(snapshot: MySqlFullRepositorySnapshot, requested: FloatingPodState, attemptUuid: UUID): (MySqlFullRepositorySnapshot, FloatingPodState) =
    require(requested != null && attemptUuid != null, "tracked drift values are required")
    val current = findPod(snapshot, requested.uuid).getOrElse(throw new OptimisticLockException(requested.uuid, requested.stateVersion))
    if current.stateVersion != requested.stateVersion then throw new OptimisticLockException(requested.uuid, requested.stateVersion)
    if current.isActiveFlag != 1 || !requested.isActive then throw new IllegalStateException(s"Tracked active drift requires an active domain row for pod ${requested.uuid}")
    val prior = MySqlStoredRows.materializeRawRow(current)
    if prior.ownerUuid != requested.ownerUuid || prior.operatingMode != requested.operatingMode ||
      current.worldName != requested.worldName || current.lastUpdated != requested.lastUpdated ||
      current.dimensionId != requested.dimensionId then
      throw new IllegalStateException(s"Tracked active drift cannot change critical metadata for pod ${requested.uuid}")
    val dimension = current.dimensionId.getOrElse(throw new IllegalStateException(s"Active row has no dimension for pod ${requested.uuid}"))
    val locator = findLocator(snapshot, requested.uuid).getOrElse(throw new IllegalStateException(s"Active locator is missing for pod ${requested.uuid}"))
    requireCurrentActiveLocator(snapshot, current, locator, attemptUuid)
    if locator.state != PhysicalRecoveryLocatorState.RowActive || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive ||
      locator.targetDimensionId != Some(dimension) || locator.domainVersion != Some(current.stateVersion) then
      throw new IllegalStateException(s"Exact ROW_ACTIVE locator is required for pod ${requested.uuid}")
    if chunk(requested.posX) != chunk(current.posX) || chunk(requested.posZ) != chunk(current.posZ) then
      throw new IllegalStateException(s"Tracked active drift crossed a chunk boundary for pod ${requested.uuid}")
    val nextVersion = Math.addExact(current.stateVersion, 1L)
    val updatedAt = System.currentTimeMillis()
    val nextRaw = toRaw(requested.copy(stateVersion = nextVersion, lastUpdated = updatedAt, operatingMode = prior.operatingMode))
    val nextLocator = locator.copy(domainVersion = Some(nextVersion))
    val next = replaceLocator(replacePod(snapshot, nextRaw), nextLocator)
    (next, MySqlStoredRows.materializeGenericState(nextRaw))

  def setMode(
      snapshot: MySqlFullRepositorySnapshot,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): (MySqlFullRepositorySnapshot, OperatingModeChangeOutcome) =
    require(expectedBinding != null && podUuid != null && authenticatedOwner != null && attemptUuid != null &&
      targetMode != null && actualPose != null, "mode CAS inputs are required")
    val certified = snapshot.recovery.binding.contains(expectedBinding) &&
      expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified
    if !certified then (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.BindingMismatch))
    else
      findPod(snapshot, podUuid) match
        case None => (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.UnknownPod))
        case Some(row) if MySqlStoredRows.decodeNullableOwnerUuid(row.ownerUuidBytes) != Some(authenticatedOwner) =>
          (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.NotOwner))
        case Some(row) if row.isActiveFlag != 1 =>
          (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.Inactive))
        case Some(row) =>
          val state = MySqlStoredRows.materializeRawRow(row)
          val dimension = row.dimensionId.getOrElse(throw new IllegalStateException(s"Active row has no dimension for pod ${podUuid}"))
          val locator = findLocator(snapshot, podUuid)
          val exact = locator.exists(value =>
            value.attemptUuid == attemptUuid &&
              value.state == PhysicalRecoveryLocatorState.RowActive &&
              value.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
              value.domainVersion.contains(row.stateVersion) &&
              value.targetDimensionId.contains(dimension) &&
              value.chunks.toSet == chunksAt(dimension, row.posX, row.posZ) &&
              value.chunks.size == 25
          )
          if !exact then (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch))
          else if row.stateVersion != expectedStateVersion &&
            !(expectedStateVersion < Long.MaxValue && row.stateVersion == expectedStateVersion + 1L &&
              state.operatingMode == targetMode && poseMatches(row, actualPose)) then
            (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.VersionMismatch(row.stateVersion)))
          else if actualPose.worldName != row.worldName || actualPose.dimensionId != dimension ||
            chunk(actualPose.posX) != chunk(row.posX) || chunk(actualPose.posZ) != chunk(row.posZ) then
            (snapshot, OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.CrossChunkMovement))
          else if state.operatingMode == targetMode && poseMatches(row, actualPose) then
            (snapshot, OperatingModeChangeOutcome.AlreadySet(state.toPublicState))
          else
            val nextVersion = Math.addExact(row.stateVersion, 1L)
            val updatedAt = System.currentTimeMillis()
            val updatedRaw = row.copy(
              stateVersion = nextVersion,
              worldName = actualPose.worldName,
              posX = actualPose.posX,
              posY = actualPose.posY,
              posZ = actualPose.posZ,
              yaw = actualPose.yaw.toDouble,
              pitch = actualPose.pitch.toDouble,
              lastUpdated = updatedAt,
              operatingModeToken = targetMode.databaseValue
            )
            val nextLocator = locator.get.copy(domainVersion = Some(nextVersion))
            val next = replaceLocator(replacePod(snapshot, updatedRaw), nextLocator)
            (next, OperatingModeChangeOutcome.Changed(MySqlStoredRows.materializeGenericState(updatedRaw)))

  def backfillDimension(
      snapshot: MySqlFullRepositorySnapshot,
      uuid: UUID,
      expectedVersion: Long,
      dimensionId: Int
  ): (MySqlFullRepositorySnapshot, com.wi.floatingpod.storage.RecoveryDimensionBackfillOutcome) =
    import com.wi.floatingpod.storage.RecoveryDimensionBackfillOutcome
    import com.wi.floatingpod.storage.RecoveryDimensionBackfillRejection
    require(uuid != null && expectedVersion >= 0L, "dimension backfill identity/version is invalid")
    findPod(snapshot, uuid) match
      case None => (snapshot, RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.UnknownPod))
      case Some(row) if row.isActiveFlag != 1 => (snapshot, RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.InactivePod))
      case Some(row) if row.stateVersion != expectedVersion =>
        (snapshot, RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.VersionMismatch(row.stateVersion)))
      case Some(row) =>
        row.dimensionId match
          case Some(existing) if existing != dimensionId =>
            (snapshot, RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.DimensionConflict(existing)))
          case Some(_) => (snapshot, RecoveryDimensionBackfillOutcome.Ready(MySqlStoredRows.materializeGenericState(row)))
          case None =>
            if findLocator(snapshot, uuid).nonEmpty then throw new IllegalStateException("Active NULL-dimension row cannot carry a locator obligation")
            val updated = row.copy(dimensionId = Some(dimensionId))
            val next = replacePod(snapshot, updated)
            (next, RecoveryDimensionBackfillOutcome.Ready(MySqlStoredRows.materializeGenericState(updated)))

  def verifyInactiveAbsence(
      snapshot: MySqlFullRepositorySnapshot,
      uuid: UUID,
      expectedVersion: Long,
      attemptUuid: UUID
  ): (MySqlFullRepositorySnapshot, PhysicalRecoveryLocator) =
    require(uuid != null && attemptUuid != null && expectedVersion >= 0L, "absence verification identity/version is invalid")
    val row = findPod(snapshot, uuid).getOrElse(throw new IllegalStateException(s"Cannot verify absence of unknown pod ${uuid}"))
    if row.isActiveFlag == 1 then throw new IllegalStateException(s"Cannot verify absence of active pod ${uuid}")
    if row.stateVersion != expectedVersion then throw new OptimisticLockException(uuid, expectedVersion)
    val dimension = row.dimensionId.getOrElse(throw new IllegalStateException(s"Inactive pod has no persisted dimension ${uuid}"))
    val locator = findLocator(snapshot, uuid).getOrElse(throw new IllegalStateException(s"Inactive locator is missing for pod ${uuid}"))
    if locator.attemptUuid != attemptUuid || locator.domainVersion != Some(expectedVersion) ||
      locator.targetDimensionId != Some(dimension) || locator.chunks.size != 25 ||
      locator.chunks.toSet != chunksAt(dimension, row.posX, row.posZ) then
      throw new IllegalStateException(s"Exact inactive physical recovery locator is required for pod ${uuid}")
    locator.state match
      case PhysicalRecoveryLocatorState.RowInactivePending =>
        val verified = locator.copy(state = PhysicalRecoveryLocatorState.RowInactiveVerified,
          reasonCode = PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified)
        (replaceLocator(snapshot, verified), verified)
      case PhysicalRecoveryLocatorState.RowInactiveVerified if locator.reasonCode == PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified =>
        (snapshot, locator)
      case _ => throw new IllegalStateException(s"Unexpected exact inactive physical recovery locator ${locator}")

  def findPod(snapshot: MySqlFullRepositorySnapshot, uuid: UUID): Option[MySqlStoredRowData] =
    val key = MySqlStoredRows.encodeUuid(uuid)
    snapshot.pods.find(_.uuidBytes == key)

  def findLocator(snapshot: MySqlFullRepositorySnapshot, uuid: UUID): Option[PhysicalRecoveryLocator] =
    snapshot.recovery.locators.find(_.podUuid == uuid)

  def replacePod(snapshot: MySqlFullRepositorySnapshot, row: MySqlStoredRowData): MySqlFullRepositorySnapshot =
    val key = row.uuidBytes
    snapshot.copy(pods = snapshot.pods.filterNot(_.uuidBytes == key) :+ row)

  def replaceLocator(snapshot: MySqlFullRepositorySnapshot, locator: PhysicalRecoveryLocator): MySqlFullRepositorySnapshot =
    snapshot.copy(recovery = snapshot.recovery.copy(
      locators = snapshot.recovery.locators.filterNot(_.podUuid == locator.podUuid) :+ locator
    ))

  def toRaw(state: FloatingPodState): MySqlStoredRowData =
    MySqlStoredRowData(
      MySqlStoredRows.encodeUuid(state.uuid),
      state.ownerUuid.map(MySqlStoredRows.encodeUuid),
      state.stateVersion,
      state.worldName,
      state.posX,
      state.posY,
      state.posZ,
      state.yaw.toDouble,
      state.pitch.toDouble,
      if state.isActive then 1 else 0,
      state.lastUpdated,
      state.dimensionId,
      state.operatingMode.databaseValue
    )

  def toStored(row: MySqlStoredRowData): StoredPodRow = MySqlStoredRows.materializeRawRow(row)
  def toPublic(row: MySqlStoredRowData): FloatingPodState = MySqlStoredRows.materializeGenericState(row)

  def toPose(row: MySqlStoredRowData, dimension: Int): FloatingPodPose =
    FloatingPodPose(row.worldName, row.posX, row.posY, row.posZ, exactFloat(row.yaw), exactFloat(row.pitch), dimension)

  def chunksAt(dimension: Int, x: Double, z: Double): Set[PhysicalRecoveryChunk] =
    val cx = chunk(x)
    val cz = chunk(z)
    (for x0 <- cx - 2 to cx + 2; z0 <- cz - 2 to cz + 2 yield PhysicalRecoveryChunk(dimension, x0, z0)).toSet

  def chunk(position: Double): Int =
    val block = Math.floor(position)
    if !java.lang.Double.isFinite(block) || block < Int.MinValue.toDouble || block > Int.MaxValue.toDouble then
      throw new IllegalArgumentException("position cannot be represented as a Minecraft chunk coordinate")
    block.toInt >> 4

  def isExactSquare(chunks: Set[PhysicalRecoveryChunk]): Boolean =
    if chunks == null || chunks.size != 25 then false
    else
      val xs = chunks.map(_.chunkX).toVector.sorted
      val zs = chunks.map(_.chunkZ).toVector.sorted
      xs.size == 5 && zs.size == 5 &&
        xs.indices.drop(1).forall(i => xs(i).toLong == xs(i - 1).toLong + 1L) &&
        zs.indices.drop(1).forall(i => zs(i).toLong == zs(i - 1).toLong + 1L)

  def activeCount(snapshot: MySqlFullRepositorySnapshot): Int = snapshot.pods.count(_.isActiveFlag == 1)

  private def activeCountFor(snapshot: MySqlFullRepositorySnapshot, owner: UUID): Int =
    snapshot.pods.count(row => row.isActiveFlag == 1 && row.ownerUuidBytes.contains(MySqlStoredRows.encodeUuid(owner)))

  private def entitlementFor(snapshot: MySqlFullRepositorySnapshot, owner: UUID): com.wi.floatingpod.domain.PlayerEntitlement =
    snapshot.entitlements.find(_.playerUuid == owner).getOrElse(com.wi.floatingpod.domain.PlayerEntitlement(owner))

  private def requireCurrentActiveLocator(
      snapshot: MySqlFullRepositorySnapshot,
      row: MySqlStoredRowData,
      locator: PhysicalRecoveryLocator,
      attempt: UUID
  ): Unit =
    val dimension = row.dimensionId.getOrElse(throw new IllegalStateException("Active row has no dimension"))
    if row.isActiveFlag != 1 || locator.attemptUuid != attempt ||
      locator.state != PhysicalRecoveryLocatorState.RowActive ||
      locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive ||
      locator.domainVersion != Some(row.stateVersion) || locator.targetDimensionId != Some(dimension) ||
      locator.chunks.toSet != chunksAt(dimension, row.posX, row.posZ) || locator.chunks.size != 25 then
      throw new IllegalStateException(s"Exact ROW_ACTIVE physical locator is required for pod ${locator.podUuid}")

  private def requireSameChunk(oldPosition: Double, newPosition: Double): Boolean =
    chunk(oldPosition) == chunk(newPosition)

  private def poseMatches(row: MySqlStoredRowData, pose: FloatingPodPose): Boolean =
    row.worldName == pose.worldName && sameDouble(row.posX, pose.posX) && sameDouble(row.posY, pose.posY) &&
      sameDouble(row.posZ, pose.posZ) && sameDouble(row.yaw, pose.yaw.toDouble) &&
      sameDouble(row.pitch, pose.pitch.toDouble) && row.dimensionId.contains(pose.dimensionId)

  private def sameDouble(a: Double, b: Double): Boolean =
    java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)

  private def exactFloat(value: Double): Float =
    val converted = value.toFloat
    if !java.lang.Double.isFinite(value) || !java.lang.Float.isFinite(converted) || converted.toDouble != value then
      throw new IllegalStateException("Stored pose cannot be represented exactly by the FloatingPod domain")
    converted
