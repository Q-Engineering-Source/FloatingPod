package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.storage.AdmissionOutcome.Accepted as AdmissionAccepted
import com.wi.floatingpod.storage.UnboundCommitOutcome.{AlreadyCommitted, Committed, Rejected as CommitRejected}
import com.wi.floatingpod.storage.UnboundCommitRejection.CapacityReached
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.nio.file.Files
import java.sql.DriverManager
import java.util.UUID

/** V006 的外部 adoption 与受控创建使用真实 SQLite 文件和公开 store seam。 */
class UnboundPhysicalRecoveryStoreTest:
  @Test
  def persistsObservedUnboundPrepareAndCommitsExactOwnerlessWorldIdleAcrossReopen(): Unit =
    val database = Files.createTempDirectory("floatingpod-unbound-adoption").resolve("pods.db")
    val podUuid = uuid("00000000-0000-0000-0000-000000000101")
    val firstAttempt = uuid("00000000-0000-0000-0000-000000000102")
    val replayAttempt = uuid("00000000-0000-0000-0000-000000000103")
    val observation = observed(podUuid, "LegacyUnregistered", "region-overworld.12.-7", 0x104)
    val preparedReceipt =
      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        val binding = certifiedBinding(repository, 0x105)
        val store = repository: PhysicalRecoveryStore

        val first = store.prepareObservedUnbound(observation, binding, firstAttempt, uniquenessReceipt(binding, observation)).get
        assertEquals(UnboundIntentOrigin.ObservedArtifactAdoption, first.origin)
        assertEquals(Some(observation.identity), first.observedIdentity)
        assertEquals(None, first.createOperationUuid)
        assertEquals(0L, first.preparedVersion)
        assertEquals(first, repository.findUnboundPreparation(podUuid).get.get)
        assertEquals(first, store.prepareObservedUnbound(observation, binding, replayAttempt, uniquenessReceipt(binding, observation)).get)

        val prepared = repository.findByUuid(podUuid).get.get
        assertEquals(None, prepared.ownerUuid)
        assertEquals(PodOperatingMode.WorldIdle, prepared.operatingMode)
        assertFalse(prepared.isActive)
        assertEquals(0L, prepared.stateVersion)
        assertEquals(Some(observation.pose.dimensionId), prepared.dimensionId)
        val locator = repository.findPhysicalRecoveryLocator(podUuid).get.get
        assertEquals(PhysicalRecoveryLocatorState.UnboundAdoptionPrepared, locator.state)
        assertEquals(PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared, locator.reasonCode)
        assertEquals(Some(0L), locator.domainVersion)
        assertEquals(exactChunks(observation.pose), locator.chunks.toSet)
        assertEquals("PREPARED", intentPhase(database.toString, podUuid))
        first
      finally repository.close()

    val committedState =
      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        val store = repository: PhysicalRecoveryStore
        val result = store.commitObservedUnbound(
          preparedReceipt.binding,
          podUuid,
          preparedReceipt.attemptUuid,
          preparedReceipt.preparedVersion,
          preparedReceipt.targetPose,
          256
        ).get
        val state = result match
          case Committed(value) => value
          case other => throw new AssertionError(s"Expected exact unbound adoption commit, got $other")

        assertEquals(None, state.ownerUuid)
        assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)
        assertTrue(state.isActive)
        assertEquals(1L, state.stateVersion)
        assertEquals(Some(preparedReceipt.targetPose.dimensionId), state.dimensionId)
        assertEquals(preparedReceipt.targetPose, pose(state))
        assertEquals(Some(state), repository.findByUuid(podUuid).get)
        val activeLocator = repository.findPhysicalRecoveryLocator(podUuid).get.get
        assertEquals(PhysicalRecoveryLocatorState.RowActive, activeLocator.state)
        assertEquals(PhysicalRecoveryLocatorReason.RowActive, activeLocator.reasonCode)
        assertEquals(Some(state.stateVersion), activeLocator.domainVersion)
        assertEquals(exactChunks(preparedReceipt.targetPose), activeLocator.chunks.toSet)
        assertEquals("COMMITTED", intentPhase(database.toString, podUuid))
        state
      finally repository.close()

    val replay = new SQLiteFloatingPodRepository(database.toString)
    try
      val store = replay: PhysicalRecoveryStore
      assertEquals(Some(preparedReceipt), store.findUnboundPreparation(podUuid).get)
      assertEquals(Some(committedState), replay.findByUuid(podUuid).get)
      assertEquals(AlreadyCommitted(committedState), store.commitObservedUnbound(
        preparedReceipt.binding,
        podUuid,
        preparedReceipt.attemptUuid,
        preparedReceipt.preparedVersion,
        preparedReceipt.targetPose,
        256
      ).get)
      assertEquals(Some(committedState), replay.findByUuid(podUuid).get)
      assertEquals(Some(committedState.stateVersion), replay.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
    finally replay.close()

  @Test
  def keepsControlledCreateOperationSeparateAndReplaysTheOriginalUuidAndAttempt(): Unit =
    val database = Files.createTempDirectory("floatingpod-unbound-create").resolve("pods.db")
    val firstPod = uuid("00000000-0000-0000-0000-000000000111")
    val replayPod = uuid("00000000-0000-0000-0000-000000000112")
    val attempt = uuid("00000000-0000-0000-0000-000000000113")
    val replayAttempt = uuid("00000000-0000-0000-0000-000000000114")
    val operation = uuid("00000000-0000-0000-0000-000000000115")
    val target = pose(0x116)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val binding = certifiedBinding(repository, 0x117)
      val store = repository: PhysicalRecoveryStore
      val prepared = store.prepareUnboundCreate(binding, firstPod, attempt, target, operation).get

      assertEquals(UnboundIntentOrigin.DebugItemCreate, prepared.origin)
      assertEquals(None, prepared.observedIdentity)
      assertEquals(Some(operation), prepared.createOperationUuid)
      assertEquals(prepared, store.prepareUnboundCreate(binding, replayPod, replayAttempt, target, operation).get)
      assertEquals(None, repository.findByUuid(replayPod).get)
      assertEquals(1, rowCount(database.toString, "physical_recovery_intent"))
      assertEquals(PhysicalRecoveryLocatorState.DebugItemCreatePrepared, repository.findPhysicalRecoveryLocator(firstPod).get.get.state)

      store.commitUnboundCreate(binding, firstPod, prepared.attemptUuid, 0L, target, operation, 256).get match
        case Committed(state) =>
          assertEquals(firstPod, state.uuid)
          assertEquals(None, state.ownerUuid)
          assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)
          assertTrue(state.isActive)
          assertEquals(1L, state.stateVersion)
        case other => throw new AssertionError(s"Expected controlled ownerless create commit, got $other")
      assertEquals("DEBUG_ITEM_CREATE", intentOrigin(database.toString, firstPod))
      assertEquals("COMMITTED", intentPhase(database.toString, firstPod))
      assertTrue(repository.findByPlayerUuid(uuid("11111111-1111-1111-1111-111111111111")).get.isEmpty)
    finally repository.close()

  @Test
  def rejectsReceiptIdentityPoseBindingAttemptAndCapacityConflictsWithoutLosingPreparedIntent(): Unit =
    val database = Files.createTempDirectory("floatingpod-unbound-rejections").resolve("pods.db")
    val podUuid = uuid("00000000-0000-0000-0000-000000000121")
    val attempt = uuid("00000000-0000-0000-0000-000000000122")
    val ownerUuid = uuid("11111111-1111-1111-1111-111111111123")
    val ownerPodUuid = uuid("00000000-0000-0000-0000-000000000124")
    val ownerAttempt = uuid("00000000-0000-0000-0000-000000000125")
    val existingPodUuid = uuid("00000000-0000-0000-0000-00000000012A")
    val observation = observed(podUuid, "FreshArtifact", "artifact-token-1", 0x126)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val binding = certifiedBinding(repository, 0x127)
      val store = repository: PhysicalRecoveryStore

      val existingState = ownedInactiveState(existingPodUuid, ownerUuid, observation.pose)
      repository.insert(existingState).get
      val existingObservation = observed(existingPodUuid, "FreshArtifact", "already-in-database", 0x12B)
      assertTrue(store.prepareObservedUnbound(existingObservation, binding, uuid("00000000-0000-0000-0000-00000000012C"), uniquenessReceipt(binding, existingObservation)).isFailure)
      assertEquals(Some(existingState), repository.findByUuid(existingPodUuid).get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(existingPodUuid).get)

      val wrongReceipt = uniquenessReceipt(binding, observation.copy(identity = observation.identity.copy(sourceIdentity = "different-token")))
      assertTrue(store.prepareObservedUnbound(observation, binding, attempt, wrongReceipt).isFailure)
      assertEquals(None, repository.findByUuid(podUuid).get)

      val prepared = store.prepareObservedUnbound(observation, binding, attempt, uniquenessReceipt(binding, observation)).get
      val originalState = repository.findByUuid(podUuid).get.get
      val originalLocator = repository.findPhysicalRecoveryLocator(podUuid).get.get
      val genericActiveInsert = com.wi.floatingpod.domain.FloatingPodState(
        podUuid, None, 0L, observation.pose.worldName, observation.pose.posX, observation.pose.posY,
        observation.pose.posZ, observation.pose.yaw, observation.pose.pitch, true, 1L,
        Some(observation.pose.dimensionId), PodOperatingMode.WorldIdle
      )
      assertTrue(repository.insert(genericActiveInsert).isFailure)

      val changedSource = observation.copy(identity = observation.identity.copy(sourceIdentity = "replacement-token"))
      assertTrue(store.prepareObservedUnbound(changedSource, binding, uuid("00000000-0000-0000-0000-000000000128"), uniquenessReceipt(binding, changedSource)).isFailure)
      assertTrue(store.commitObservedUnbound(binding, podUuid, uuid("00000000-0000-0000-0000-000000000129"), 0L, observation.pose, 256).isFailure)
      assertTrue(store.commitObservedUnbound(binding.copy(bindingUuid = uuid("00000000-0000-0000-0000-000000000130")), podUuid, attempt, 0L, observation.pose, 256).isFailure)
      assertTrue(store.commitObservedUnbound(binding, podUuid, attempt, 1L, observation.pose, 256).isFailure)

      val movedPose = observation.pose.copy(posX = observation.pose.posX + 1D)
      assertEquals(CommitRejected(UnboundCommitRejection.ActualPoseMismatch), store.commitObservedUnbound(binding, podUuid, attempt, 0L, movedPose, 256).get)
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)

      val ownedInactive = ownedInactiveState(ownerPodUuid, ownerUuid, observation.pose)
      repository.insert(ownedInactive).get
      repository.prepareCreateLocator(ownerPodUuid, 0L, ownerAttempt, observation.pose.dimensionId, exactChunks(observation.pose)).get
      store.activatePreparedIfAdmissible(ownedInactive, ownerUuid, 0L, observation.pose.dimensionId, 256, ownerAttempt).get match
        case AdmissionAccepted(_) => ()
        case other => throw new AssertionError(s"Expected one existing active owner pod, got $other")

      assertEquals(CapacityReached(1, 1), store.commitObservedUnbound(binding, podUuid, attempt, 0L, observation.pose, 1).get match
        case CommitRejected(reason) => reason
        case other => throw new AssertionError(s"Expected normal capacity refusal, got $other"))
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals("PREPARED", intentPhase(database.toString, podUuid))

      (store.deactivateTrackedIfExpectedActive(ownerPodUuid, Some(ownerUuid), 1L, ownerAttempt).get) match
        case com.wi.floatingpod.storage.DeactivationOutcome.Accepted(_) => ()
        case other => throw new AssertionError(s"Expected the capacity fixture to release its active slot, got $other")
      val committedState = store.commitObservedUnbound(binding, podUuid, attempt, 0L, observation.pose, 1).get match
        case Committed(state) =>
          assertEquals(None, state.ownerUuid)
          assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)
          assertTrue(state.isActive)
          assertEquals(1L, state.stateVersion)
          state
        case other => throw new AssertionError(s"Expected retained preparation to commit after capacity became available, got $other")

      val genericOwnerMutation = repository.update(originalState.copy(ownerUuid = Some(ownerUuid)))
      assertTrue(genericOwnerMutation.isFailure)
      assertTrue(repository.delete(podUuid).isFailure)
      assertEquals(Some(committedState), repository.findByUuid(podUuid).get)
      assertEquals(prepared, store.findUnboundPreparation(podUuid).get.get)
    finally repository.close()

  @Test
  def rollsBackDomainLocatorAndIntentTogetherWhenTheIntentCommitGuardFails(): Unit =
    val database = Files.createTempDirectory("floatingpod-unbound-rollback").resolve("pods.db")
    val podUuid = uuid("00000000-0000-0000-0000-000000000141")
    val attempt = uuid("00000000-0000-0000-0000-000000000142")
    val observation = observed(podUuid, "LegacyUnregistered", "legacy-record-141", 0x143)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val binding = certifiedBinding(repository, 0x144)
      val store = repository: PhysicalRecoveryStore
      store.prepareObservedUnbound(observation, binding, attempt, uniquenessReceipt(binding, observation)).get

      val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
      try
        val statement = connection.createStatement()
        try statement.execute("CREATE TRIGGER test_abort_unbound_intent_commit BEFORE UPDATE ON physical_recovery_intent WHEN OLD.phase = 'PREPARED' AND NEW.phase = 'COMMITTED' BEGIN SELECT RAISE(ABORT, 'injected intent commit guard'); END")
        finally statement.close()
      finally connection.close()

      val failure = store.commitObservedUnbound(binding, podUuid, attempt, 0L, observation.pose, 256)
      assertTrue(failure.isFailure)
      assertTrue(failure.failed.get.getMessage.contains("injected intent commit guard"))
      val state = repository.findByUuid(podUuid).get.get
      val locator = repository.findPhysicalRecoveryLocator(podUuid).get.get
      assertFalse(state.isActive)
      assertEquals(0L, state.stateVersion)
      assertEquals(PhysicalRecoveryLocatorState.UnboundAdoptionPrepared, locator.state)
      assertEquals(Some(0L), locator.domainVersion)
      assertEquals("PREPARED", intentPhase(database.toString, podUuid))
    finally repository.close()

  private def certifiedBinding(repository: SQLiteFloatingPodRepository, salt: Int): PhysicalRecoveryBinding =
    val bindingUuid = uuid(f"00000000-0000-0000-0000-${salt}%012x")
    repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
    repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get

  private def observed(podUuid: UUID, sourceKind: String, sourceIdentity: String, salt: Int): ObservedUnboundPod =
    val kind = sourceKind match
      case "FreshArtifact" => ObservedUnboundSourceKind.FreshArtifact
      case "LegacyUnregistered" => ObservedUnboundSourceKind.LegacyUnregistered
      case other => throw new IllegalArgumentException(s"Unknown source kind $other")
    ObservedUnboundPod(podUuid, ObservedUnboundIdentity(kind, uuid(f"00000000-0000-0000-0000-${salt}%012x"), sourceIdentity), pose(0x107))

  private def uniquenessReceipt(binding: PhysicalRecoveryBinding, observation: ObservedUnboundPod): ObservedUnboundUniquenessReceipt =
    ObservedUnboundUniquenessReceipt(binding.bindingUuid, observation)

  private def pose(salt: Int): FloatingPodPose =
    FloatingPodPose("overworld", 200D, 64D, -104D, 45F, 12F, 7)

  private def exactChunks(pose: FloatingPodPose): Set[PhysicalRecoveryChunk] =
    val centerX = Math.floor(pose.posX).toInt >> 4
    val centerZ = Math.floor(pose.posZ).toInt >> 4
    (for chunkX <- centerX - 2 to centerX + 2; chunkZ <- centerZ - 2 to centerZ + 2 yield PhysicalRecoveryChunk(pose.dimensionId, chunkX, chunkZ)).toSet

  private def ownedInactiveState(podUuid: UUID, ownerUuid: UUID, pose: FloatingPodPose) =
    com.wi.floatingpod.domain.FloatingPodState(podUuid, Some(ownerUuid), 0L, pose.worldName, pose.posX, pose.posY, pose.posZ, pose.yaw, pose.pitch, false, 1L, None)

  private def pose(state: com.wi.floatingpod.domain.FloatingPodState): FloatingPodPose =
    FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.dimensionId.get)

  private def intentPhase(database: String, podUuid: UUID): String = intentValue(database, podUuid, "phase")
  private def intentOrigin(database: String, podUuid: UUID): String = intentValue(database, podUuid, "origin")

  private def intentValue(database: String, podUuid: UUID, column: String): String =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database}")
    try
      val statement = connection.prepareStatement(s"SELECT $column FROM physical_recovery_intent WHERE pod_uuid = ?")
      try
        statement.setBytes(1, uuidBytes(podUuid))
        val resultSet = statement.executeQuery()
        try
          assertTrue(resultSet.next())
          resultSet.getString(1)
        finally resultSet.close()
      finally statement.close()
    finally connection.close()

  private def rowCount(database: String, table: String): Int =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database}")
    try
      val statement = connection.createStatement()
      try
        val resultSet = statement.executeQuery(s"SELECT COUNT(*) FROM $table")
        try
          assertTrue(resultSet.next())
          resultSet.getInt(1)
        finally resultSet.close()
      finally statement.close()
    finally connection.close()

  private def uuid(value: String): UUID = UUID.fromString(value)

  private def uuidBytes(value: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
