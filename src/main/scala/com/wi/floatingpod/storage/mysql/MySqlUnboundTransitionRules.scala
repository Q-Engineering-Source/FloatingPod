package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{
  ObservedUnboundIdentity,
  ObservedUnboundPod,
  ObservedUnboundSourceKind,
  ObservedUnboundUniquenessReceipt,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryCutoverState,
  PhysicalRecoveryLocator,
  PhysicalRecoveryLocatorReason,
  PhysicalRecoveryLocatorState,
  UnboundCommitOutcome,
  UnboundCommitRejection,
  UnboundIntentOrigin,
  UnboundPreparationReceipt
}

import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.util.UUID

/** Unbound prepare/commit 的内存转换；不把调用方目录输入提升为全球唯一性证明。 */
private[mysql] object MySqlUnboundTransitionRules:
  def prepareObserved(
      snapshot: MySqlFullRepositorySnapshot,
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessReceipt: ObservedUnboundUniquenessReceipt
  ): (MySqlFullRepositorySnapshot, UnboundPreparationReceipt) =
    require(observation != null && expectedBinding != null && attemptUuid != null && uniquenessReceipt != null,
      "observed unbound preparation inputs are required")
    require(uniquenessReceipt.bindingUuid == expectedBinding.bindingUuid && uniquenessReceipt.observation == observation,
      "unbound observation receipt does not match the supplied pod and binding")
    requireCertifiedBinding(snapshot, expectedBinding)
    val digest = sourceHash(observation.identity.sourceIdentity)
    val sameAttempt = snapshot.recovery.intents.find(_.attemptUuid == attemptUuid)
    sameAttempt match
      case Some(existing) =>
        val existingReceipt = receipt(snapshot, existing)
        if sameObservation(existingReceipt, observation, expectedBinding) then (snapshot, existingReceipt)
        else conflict("physical recovery attempt UUID already belongs to a different intent")
      case None =>
        val bySource = snapshot.recovery.intents.find { existing =>
          existing.bindingUuid == expectedBinding.bindingUuid &&
            existing.origin == UnboundIntentOrigin.ObservedArtifactAdoption &&
            existing.sourceKind.contains(observation.identity.sourceKind) &&
            existing.catalogGeneration.contains(observation.identity.catalogGeneration) &&
            existing.sourceIdentitySha256.contains(digest)
        }
        bySource match
          case Some(existing) =>
            val existingReceipt = receipt(snapshot, existing)
            if sameObservation(existingReceipt, observation, expectedBinding) then (snapshot, existingReceipt)
            else conflict(s"observed source identity conflicts with existing history for pod ${observation.podUuid}")
          case None =>
            requireNoPodHistory(snapshot, observation.podUuid)
            val requested = UnboundPreparationReceipt(
              expectedBinding,
              observation.podUuid,
              attemptUuid,
              UnboundIntentOrigin.ObservedArtifactAdoption,
              observation.pose,
              Some(observation.identity),
              None,
              0L
            )
            (insertPreparation(snapshot, requested, digest), requested)

  def prepareCreate(
      snapshot: MySqlFullRepositorySnapshot,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): (MySqlFullRepositorySnapshot, UnboundPreparationReceipt) =
    require(expectedBinding != null && podUuid != null && attemptUuid != null && targetPose != null && operationUuid != null,
      "controlled-create preparation inputs are required")
    requireCertifiedBinding(snapshot, expectedBinding)
    snapshot.recovery.intents.find(_.attemptUuid == attemptUuid) match
      case Some(existing) =>
        val old = receipt(snapshot, existing)
        if old.binding == expectedBinding && old.podUuid == podUuid &&
          old.origin == UnboundIntentOrigin.DebugItemCreate && old.targetPose == targetPose &&
          old.createOperationUuid.contains(operationUuid) then (snapshot, old)
        else conflict("physical recovery attempt UUID already belongs to another intent")
      case None =>
        snapshot.recovery.intents.find(i => i.bindingUuid == expectedBinding.bindingUuid && i.createOperationUuid.contains(operationUuid)) match
          case Some(existing) =>
            val old = receipt(snapshot, existing)
            if old.podUuid == podUuid && old.binding == expectedBinding &&
              old.origin == UnboundIntentOrigin.DebugItemCreate && old.targetPose == targetPose then (snapshot, old)
            else conflict("controlled-create operation conflicts with existing intent")
          case None =>
            requireNoPodHistory(snapshot, podUuid)
            val requested = UnboundPreparationReceipt(
              expectedBinding,
              podUuid,
              attemptUuid,
              UnboundIntentOrigin.DebugItemCreate,
              targetPose,
              None,
              Some(operationUuid),
              0L
            )
            (insertPreparation(snapshot, requested, Vector.empty), requested)

  def commitObserved(
      snapshot: MySqlFullRepositorySnapshot,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): (MySqlFullRepositorySnapshot, UnboundCommitOutcome) =
    commit(snapshot, expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, None, globalActiveLimit,
      UnboundIntentOrigin.ObservedArtifactAdoption)

  def commitCreate(
      snapshot: MySqlFullRepositorySnapshot,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): (MySqlFullRepositorySnapshot, UnboundCommitOutcome) =
    require(operationUuid != null, "controlled-create operation UUID is required")
    commit(snapshot, expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, Some(operationUuid),
      globalActiveLimit, UnboundIntentOrigin.DebugItemCreate)

  def findPreparation(snapshot: MySqlFullRepositorySnapshot, podUuid: UUID): Option[UnboundPreparationReceipt] =
    require(podUuid != null, "pod UUID is required")
    snapshot.recovery.intents.find(_.podUuid == podUuid).map(receipt(snapshot, _))

  private def commit(
      snapshot: MySqlFullRepositorySnapshot,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      createOperationUuid: Option[UUID],
      globalActiveLimit: Int,
      origin: UnboundIntentOrigin
  ): (MySqlFullRepositorySnapshot, UnboundCommitOutcome) =
    require(expectedBinding != null && podUuid != null && attemptUuid != null && actualPose != null,
      "unbound commit inputs are required")
    require(createOperationUuid != null, "create operation option is required")
    require(expectedPreparedVersion >= 0L, "prepared domain version must not be negative")
    require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be between 1 and 256")
    requireCertifiedBinding(snapshot, expectedBinding)

    val intent = snapshot.recovery.intents.find(_.podUuid == podUuid).getOrElse(
      throw new IllegalStateException(s"Unbound intent is missing for pod ${podUuid}")
    )
    if intent.bindingUuid != expectedBinding.bindingUuid || intent.attemptUuid != attemptUuid ||
      intent.origin != origin || intent.createOperationUuid != createOperationUuid ||
      expectedPreparedVersion != intent.preparedDomainVersion then
      conflict(s"Unbound intent identity does not match commit request for pod ${podUuid}")
    val preparedReceipt = receipt(snapshot, intent)
    if !poseEquals(preparedReceipt.targetPose, actualPose) then
      (snapshot, UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch))
    else
      intent.phase match
        case MySqlRecoveryIntentPhase.Committed =>
          val current = MySqlRepositoryTransitionRules.findPod(snapshot, podUuid).getOrElse(
            throw new IllegalStateException(s"Committed unbound pod row is missing for ${podUuid}")
          )
          val committedVersion = intent.committedDomainVersion.getOrElse(
            throw new IllegalStateException(s"Committed unbound intent has no version for ${podUuid}")
          )
          val exactReplay = committedVersion == expectedPreparedVersion + 1L &&
            current.stateVersion == committedVersion && current.isActiveFlag == 1 &&
            current.ownerUuidBytes.isEmpty && current.operatingModeToken == "WORLD_IDLE" &&
            poseEquals(rawPose(current), actualPose) &&
            MySqlRepositoryTransitionRules.findLocator(snapshot, podUuid).exists { locator =>
              locator.state == PhysicalRecoveryLocatorState.RowActive &&
                locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive &&
                locator.attemptUuid == attemptUuid && locator.domainVersion.contains(committedVersion) &&
                locator.targetDimensionId.contains(actualPose.dimensionId) &&
                locator.chunks.toSet == MySqlRepositoryTransitionRules.chunksAt(actualPose.dimensionId, actualPose.posX, actualPose.posZ)
            }
          if !exactReplay then conflict(s"Committed unbound history no longer matches exact replay for pod ${podUuid}")
          (snapshot, UnboundCommitOutcome.AlreadyCommitted(MySqlStoredRows.materializeGenericState(current)))
        case MySqlRecoveryIntentPhase.Prepared =>
          val row = MySqlRepositoryTransitionRules.findPod(snapshot, podUuid).getOrElse(
            throw new IllegalStateException(s"Prepared unbound domain row is missing for ${podUuid}")
          )
          if row.stateVersion != expectedPreparedVersion || row.isActiveFlag != 0 ||
            row.ownerUuidBytes.nonEmpty || row.operatingModeToken != "WORLD_IDLE" then
            conflict(s"Prepared unbound domain row changed for pod ${podUuid}")
          val activeCount = MySqlRepositoryTransitionRules.activeCount(snapshot)
          if activeCount >= globalActiveLimit then
            return (snapshot, UnboundCommitOutcome.Rejected(UnboundCommitRejection.CapacityReached(activeCount, globalActiveLimit)))

          val locator = MySqlRepositoryTransitionRules.findLocator(snapshot, podUuid).getOrElse(
            throw new IllegalStateException(s"Prepared unbound locator is missing for pod ${podUuid}")
          )
          val expectedLocatorState =
            if origin == UnboundIntentOrigin.ObservedArtifactAdoption then PhysicalRecoveryLocatorState.UnboundAdoptionPrepared
            else PhysicalRecoveryLocatorState.DebugItemCreatePrepared
          val expectedReason =
            if origin == UnboundIntentOrigin.ObservedArtifactAdoption then PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared
            else PhysicalRecoveryLocatorReason.DebugItemCreatePrepared
          val expectedChunks = MySqlRepositoryTransitionRules.chunksAt(actualPose.dimensionId, actualPose.posX, actualPose.posZ)
          if locator.state != expectedLocatorState || locator.reasonCode != expectedReason ||
            locator.attemptUuid != attemptUuid || locator.domainVersion != Some(expectedPreparedVersion) ||
            locator.targetDimensionId != Some(actualPose.dimensionId) || locator.chunks.size != 25 ||
            locator.chunks.toSet != expectedChunks then
            conflict(s"Prepared unbound locator is not exact for pod ${podUuid}")

          val committedAt = System.currentTimeMillis()
          val committedVersion = Math.addExact(expectedPreparedVersion, 1L)
          val active = row.copy(
            stateVersion = committedVersion,
            worldName = actualPose.worldName,
            posX = actualPose.posX,
            posY = actualPose.posY,
            posZ = actualPose.posZ,
            yaw = actualPose.yaw.toDouble,
            pitch = actualPose.pitch.toDouble,
            isActiveFlag = 1,
            lastUpdated = committedAt,
            dimensionId = Some(actualPose.dimensionId),
            operatingModeToken = "WORLD_IDLE"
          )
          val activeLocator = locator.copy(
            state = PhysicalRecoveryLocatorState.RowActive,
            reasonCode = PhysicalRecoveryLocatorReason.RowActive,
            domainVersion = Some(committedVersion)
          )
          val committedIntent = intent.copy(
            phase = MySqlRecoveryIntentPhase.Committed,
            committedDomainVersion = Some(committedVersion),
            committedAt = Some(committedAt),
            committedWorldName = Some(actualPose.worldName),
            committedPosX = Some(actualPose.posX),
            committedPosY = Some(actualPose.posY),
            committedPosZ = Some(actualPose.posZ),
            committedYaw = Some(actualPose.yaw.toDouble),
            committedPitch = Some(actualPose.pitch.toDouble),
            committedDimensionId = Some(actualPose.dimensionId)
          )
          val withPod = MySqlRepositoryTransitionRules.replacePod(snapshot, active)
          val withLocator = MySqlRepositoryTransitionRules.replaceLocator(withPod, activeLocator)
          val nextRecovery = withLocator.recovery.copy(intents =
            withLocator.recovery.intents.map(i => if i.podUuid == podUuid then committedIntent else i))
          val next = withLocator.copy(recovery = nextRecovery)
          (next, UnboundCommitOutcome.Committed(MySqlStoredRows.materializeGenericState(active)))

  private def insertPreparation(
      snapshot: MySqlFullRepositorySnapshot,
      receipt: UnboundPreparationReceipt,
      identityDigest: Vector[Byte]
  ): MySqlFullRepositorySnapshot =
    requireNoPodHistory(snapshot, receipt.podUuid)
    if snapshot.recovery.binding != Some(receipt.binding) || receipt.binding.cutoverState != PhysicalRecoveryCutoverState.Certified then
      conflict("unbound preparation requires the exact certified binding")
    val pose = receipt.targetPose
    val seed = FloatingPodState(
      receipt.podUuid,
      None,
      0L,
      pose.worldName,
      pose.posX,
      pose.posY,
      pose.posZ,
      pose.yaw,
      pose.pitch,
      false,
      System.currentTimeMillis(),
      Some(pose.dimensionId),
      PodOperatingMode.WorldIdle
    )
    val row = MySqlRepositoryTransitionRules.toRaw(seed)
    val chunks = MySqlRepositoryTransitionRules.chunksAt(pose.dimensionId, pose.posX, pose.posZ)
      .toVector.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ))
    val locatorState =
      if receipt.origin == UnboundIntentOrigin.ObservedArtifactAdoption then PhysicalRecoveryLocatorState.UnboundAdoptionPrepared
      else PhysicalRecoveryLocatorState.DebugItemCreatePrepared
    val reason =
      if receipt.origin == UnboundIntentOrigin.ObservedArtifactAdoption then PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared
      else PhysicalRecoveryLocatorReason.DebugItemCreatePrepared
    val locator = PhysicalRecoveryLocator(receipt.podUuid, receipt.attemptUuid, locatorState, reason,
      Some(receipt.preparedVersion), Some(pose.dimensionId), chunks.toList)
    val intent = MySqlRecoveryIntentData(
      receipt.podUuid,
      receipt.attemptUuid,
      receipt.binding.bindingUuid,
      receipt.origin,
      MySqlRecoveryIntentPhase.Prepared,
      receipt.observedIdentity.map(_.sourceKind),
      receipt.observedIdentity.map(_.sourceIdentity),
      receipt.observedIdentity.map(_ => identityDigest),
      receipt.observedIdentity.map(_.catalogGeneration),
      receipt.createOperationUuid,
      receipt.preparedVersion,
      pose.worldName,
      pose.posX,
      pose.posY,
      pose.posZ,
      pose.yaw.toDouble,
      pose.pitch.toDouble,
      pose.dimensionId,
      MySqlRepositoryTransitionRules.chunk(pose.posX),
      MySqlRepositoryTransitionRules.chunk(pose.posZ),
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    val rawChunks = snapshot.recovery.chunks ++ chunks.map(chunk => MySqlRecoveryChunkRow(receipt.podUuid, chunk))
    snapshot.copy(
      pods = snapshot.pods :+ row,
      recovery = snapshot.recovery.copy(
        locators = snapshot.recovery.locators :+ locator,
        chunks = rawChunks,
        intents = snapshot.recovery.intents :+ intent
      )
    )

  private def receipt(snapshot: MySqlFullRepositorySnapshot, intent: MySqlRecoveryIntentData): UnboundPreparationReceipt =
    val binding = snapshot.recovery.binding.filter(_.bindingUuid == intent.bindingUuid).getOrElse(
      throw new IllegalStateException(s"Unbound intent binding is missing for pod ${intent.podUuid}")
    )
    val identity =
      if intent.sourceKind.isEmpty && intent.sourceIdentity.isEmpty && intent.sourceIdentitySha256.isEmpty &&
        intent.catalogGeneration.isEmpty && intent.createOperationUuid.nonEmpty then None
      else
        val kind = intent.sourceKind.getOrElse(throw new IllegalStateException("Observed intent source kind is missing"))
        val text = intent.sourceIdentity.getOrElse(throw new IllegalStateException("Observed intent source identity is missing"))
        val generation = intent.catalogGeneration.getOrElse(throw new IllegalStateException("Observed intent catalog generation is missing"))
        val digest = intent.sourceIdentitySha256.getOrElse(throw new IllegalStateException("Observed intent source digest is missing"))
        if !MessageDigest.isEqual(digest.toArray, sourceHash(text).toArray) then
          throw new IllegalStateException("Observed intent source digest does not match the stored source identity")
        Some(ObservedUnboundIdentity(kind, generation, text))
    UnboundPreparationReceipt(
      binding,
      intent.podUuid,
      intent.attemptUuid,
      intent.origin,
      FloatingPodPose(intent.targetWorldName, intent.targetPosX, intent.targetPosY, intent.targetPosZ,
        exactFloat(intent.targetYaw), exactFloat(intent.targetPitch), intent.targetDimensionId),
      identity,
      intent.createOperationUuid,
      intent.preparedDomainVersion
    )

  private def requireCertifiedBinding(snapshot: MySqlFullRepositorySnapshot, expected: PhysicalRecoveryBinding): Unit =
    if expected.cutoverState != PhysicalRecoveryCutoverState.Certified || !snapshot.recovery.binding.contains(expected) then
      conflict("unbound operations require the exact certified physical binding")

  private def requireNoPodHistory(snapshot: MySqlFullRepositorySnapshot, podUuid: UUID): Unit =
    if MySqlRepositoryTransitionRules.findPod(snapshot, podUuid).nonEmpty ||
      MySqlRepositoryTransitionRules.findLocator(snapshot, podUuid).nonEmpty ||
      snapshot.recovery.chunks.exists(_.podUuid == podUuid) ||
      snapshot.recovery.intents.exists(_.podUuid == podUuid) then
      conflict(s"unbound operation cannot reset existing domain or physical history for pod ${podUuid}")

  private def sameObservation(existing: UnboundPreparationReceipt, observation: ObservedUnboundPod, binding: PhysicalRecoveryBinding): Boolean =
    existing.binding == binding && existing.podUuid == observation.podUuid &&
      existing.origin == UnboundIntentOrigin.ObservedArtifactAdoption &&
      existing.observedIdentity.contains(observation.identity) && poseEquals(existing.targetPose, observation.pose)

  private def rawPose(row: MySqlStoredRowData): FloatingPodPose =
    FloatingPodPose(row.worldName, row.posX, row.posY, row.posZ, exactFloat(row.yaw), exactFloat(row.pitch),
      row.dimensionId.getOrElse(throw new IllegalStateException("Stored row has no dimension")))

  private def poseEquals(left: FloatingPodPose, right: FloatingPodPose): Boolean =
    left.worldName == right.worldName && left.dimensionId == right.dimensionId &&
      sameDouble(left.posX, right.posX) && sameDouble(left.posY, right.posY) && sameDouble(left.posZ, right.posZ) &&
      java.lang.Float.floatToRawIntBits(left.yaw) == java.lang.Float.floatToRawIntBits(right.yaw) &&
      java.lang.Float.floatToRawIntBits(left.pitch) == java.lang.Float.floatToRawIntBits(right.pitch)

  private def sourceHash(value: String): Vector[Byte] =
    require(value != null && value.trim.nonEmpty, "observed source identity is required")
    val encoded = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(CharBuffer.wrap(value))
    val bytes = new Array[Byte](encoded.remaining())
    encoded.get(bytes)
    MessageDigest.getInstance("SHA-256").digest(bytes).toVector

  private def exactFloat(value: Double): Float =
    val result = value.toFloat
    if !java.lang.Double.isFinite(value) || !java.lang.Float.isFinite(result) || result.toDouble != value then
      throw new IllegalStateException("Stored unbound intent rotation cannot be represented exactly")
    result

  private def sameDouble(left: Double, right: Double): Boolean =
    java.lang.Double.doubleToRawLongBits(left) == java.lang.Double.doubleToRawLongBits(right)

  private def conflict(message: String): Nothing =
    throw new IllegalStateException(message)
