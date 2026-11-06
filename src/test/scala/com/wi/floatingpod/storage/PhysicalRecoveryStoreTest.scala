package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.AdmissionOutcome.Accepted as AdmissionAccepted
import com.wi.floatingpod.storage.DeactivationOutcome.Accepted as DeactivationAccepted
import com.wi.floatingpod.storage.OperatingModeChangeOutcome.{AlreadySet as ModeAlreadySet, Changed as ModeChanged, Rejected as ModeRejected}
import com.wi.floatingpod.storage.OperatingModeChangeRejection
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.sql.DriverManager
import java.util.UUID

/** CREATE_PREPARED locator 的首个持久化 tracer。 */
class PhysicalRecoveryStoreTest:
  @Test
  def verifiesInactiveAbsenceAfterReopenWithoutMutatingDomainRowAndReplaysVerifiedLocator(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000981")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000982")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000983")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val target = inactive.copy(worldName = "the_end", posX = 128D, posY = 80D, posZ = -48D, yaw = 90F, pitch = -15F)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val expectedChunks = chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))
    val expectedPending = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.RowInactivePending, "row_inactive_pending", Some(2L), Some(7), expectedChunks)
    val (capturedInactive, capturedPending) =
      val bootstrap = new SQLiteFloatingPodRepository(database.toString)
      try
        assertTrue(bootstrap.insert(inactive).isSuccess)
        assertTrue(bootstrap.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).isSuccess)
        (bootstrap: PhysicalRecoveryStore).activatePreparedIfAdmissible(target, ownerUuid, 0L, 7, 256, attemptUuid).get match
          case AdmissionAccepted(_) => ()
          case other => throw new AssertionError(s"Expected accepted activation, got $other")
        (bootstrap: PhysicalRecoveryStore).deactivateTrackedIfExpectedActive(podUuid, Some(ownerUuid), 1L, attemptUuid).get match
          case DeactivationAccepted(_) => ()
          case other => throw new AssertionError(s"Expected accepted deactivation, got $other")
        (bootstrap.findByUuid(podUuid).get.get, bootstrap.findPhysicalRecoveryLocator(podUuid).get.get)
      finally bootstrap.close()

    assertEquals(expectedPending, capturedPending)
    val expectedVerified = capturedPending.copy(state = PhysicalRecoveryLocatorState.RowInactiveVerified, reasonCode = "fresh_boot_absence_verified")
    val reopened = new SQLiteFloatingPodRepository(database.toString)
    try
      val verified = (reopened: PhysicalRecoveryStore).verifyInactiveAbsence(podUuid, 2L, attemptUuid).get

      assertEquals(expectedVerified, verified)
      assertEquals(Some(capturedInactive), reopened.findByUuid(podUuid).get)
      assertEquals(Some(expectedVerified), reopened.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals(expectedVerified, (reopened: PhysicalRecoveryStore).verifyInactiveAbsence(podUuid, 2L, attemptUuid).get)
      assertEquals(Some(capturedInactive), reopened.findByUuid(podUuid).get)
      assertEquals(Some(expectedVerified), reopened.findPhysicalRecoveryLocator(podUuid).get)
    finally reopened.close()

  @Test
  def rejectsGenericDeleteWhenPreparedLocatorWouldBeOrphanedAndRetainsDomainRowAndChunks(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000971")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000972")
    val state = FloatingPodState(podUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000973")), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val expectedLocator = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(state).isSuccess)
      assertEquals(expectedLocator, repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get)

      val deletion = repository.delete(podUuid)

      assertTrue(deletion.isFailure)
      assertTrue(Option(deletion.failed.get.getMessage).exists(_.trim.nonEmpty))
      assertEquals(Some(state), repository.findByUuid(podUuid).get)
      assertEquals(Some(expectedLocator), repository.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals(expectedLocator.chunks, repository.findPhysicalRecoveryLocator(podUuid).get.get.chunks)
    finally repository.close()

  @Test
  def listsCompletePreparedLocatorsExactlyOnceInStableUuidStringOrderWithoutMutatingDomainRows(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val highPodUuid = UUID.fromString("00000000-0000-0000-0000-000000000962")
    val lowPodUuid = UUID.fromString("00000000-0000-0000-0000-000000000961")
    val highAttemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000963")
    val lowAttemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000964")
    val highState = FloatingPodState(highPodUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000965")), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val lowState = FloatingPodState(lowPodUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000966")), 0L, "the_nether", -32D, 72D, 48D, 180F, 10F, false, 100L, None)
    val highChunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val lowChunks = (for chunkX <- -14 to -10; chunkZ <- 5 to 9 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val expectedHigh = PhysicalRecoveryLocator(highPodUuid, highAttemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), highChunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
    val expectedLow = PhysicalRecoveryLocator(lowPodUuid, lowAttemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), lowChunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(highState).isSuccess)
      assertTrue(repository.insert(lowState).isSuccess)
      assertEquals(expectedHigh, repository.prepareCreateLocator(highPodUuid, 0L, highAttemptUuid, 7, highChunks).get)
      assertEquals(expectedLow, repository.prepareCreateLocator(lowPodUuid, 0L, lowAttemptUuid, 7, lowChunks).get)

      val locators = (repository: PhysicalRecoveryStore).findAllPhysicalRecoveryLocators().get

      assertEquals(List(expectedLow, expectedHigh), locators)
      assertEquals(locators.map(_.podUuid.toString).sorted, locators.map(_.podUuid.toString))
      assertEquals(Some(highState), repository.findByUuid(highPodUuid).get)
      assertEquals(Some(lowState), repository.findByUuid(lowPodUuid).get)
    finally repository.close()

  @Test
  def deactivatesExactActiveLocatorAndDomainRowIntoInactivePendingInOneTransition(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000941")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000942")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000943")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val target = inactive.copy(worldName = "the_end", posX = 128D, posY = 80D, posZ = -48D, yaw = 90F, pitch = -15F)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val expectedChunks = chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(inactive).isSuccess)
      assertTrue(repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).isSuccess)
      (repository: PhysicalRecoveryStore).activatePreparedIfAdmissible(target, ownerUuid, 0L, 7, 256, attemptUuid).get match
        case AdmissionAccepted(_) => ()
        case other => throw new AssertionError(s"Expected accepted activation, got $other")

      val outcome = (repository: PhysicalRecoveryStore).deactivateTrackedIfExpectedActive(podUuid, Some(ownerUuid), 1L, attemptUuid).get

      outcome match
        case DeactivationAccepted(deactivated) =>
          assertEquals(podUuid, deactivated.uuid)
          assertEquals(Some(ownerUuid), deactivated.ownerUuid)
          assertEquals(2L, deactivated.stateVersion)
          assertEquals("the_end", deactivated.worldName)
          assertEquals(128D, deactivated.posX)
          assertEquals(80D, deactivated.posY)
          assertEquals(-48D, deactivated.posZ)
          assertEquals(90F, deactivated.yaw)
          assertEquals(-15F, deactivated.pitch)
          assertFalse(deactivated.isActive)
          assertEquals(Some(7), deactivated.dimensionId)
          assertEquals(Some(deactivated), repository.findByUuid(podUuid).get)
        case other => throw new AssertionError(s"Expected accepted deactivation, got $other")

      assertEquals(Some(PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.RowInactivePending, "row_inactive_pending", Some(2L), Some(7), expectedChunks)), repository.findPhysicalRecoveryLocator(podUuid).get)
    finally repository.close()

  @Test
  def activatesExactPreparedLocatorAndDomainRowInOneAdmissibleTransition(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000931")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000932")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000933")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val target = inactive.copy(worldName = "the_end", posX = 128D, posY = 80D, posZ = -48D, yaw = 90F, pitch = -15F)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(inactive).isSuccess)
      assertEquals(PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))), repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get)

      val outcome = (repository: PhysicalRecoveryStore).activatePreparedIfAdmissible(target, ownerUuid, 0L, 7, 256, attemptUuid).get

      outcome match
        case AdmissionAccepted(activated) =>
          assertEquals(podUuid, activated.uuid)
          assertEquals(Some(ownerUuid), activated.ownerUuid)
          assertEquals(1L, activated.stateVersion)
          assertEquals("the_end", activated.worldName)
          assertEquals(128D, activated.posX)
          assertEquals(80D, activated.posY)
          assertEquals(-48D, activated.posZ)
          assertEquals(90F, activated.yaw)
          assertEquals(-15F, activated.pitch)
          assertTrue(activated.isActive)
          assertEquals(Some(7), activated.dimensionId)
          assertEquals(Some(activated), repository.findByUuid(podUuid).get)
        case other => throw new AssertionError(s"Expected accepted activation, got $other")

      assertEquals(Some(PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.RowActive, "row_active", Some(1L), Some(7), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))), repository.findPhysicalRecoveryLocator(podUuid).get)
    finally repository.close()

  @Test
  def sameChunkTrackedDriftKeepsDomainAndLocatorVersionsSynchronizedAcrossReopen(): Unit =
    val database = Files.createTempDirectory("floatingpod-active-drift").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000921")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000922")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000923")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000924")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val drifted =
      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
        repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
        repository.insert(inactive).get
        repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
        val active = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
          case AdmissionAccepted(state) => state
          case other => throw new AssertionError(s"Expected activation, got $other")

        val persistedDrift = repository.updateTrackedActiveDrift(active.copy(posX = 203D, posY = 65D, posZ = -107D, yaw = 45F, pitch = 12F), attemptUuid).get
        assertEquals(2L, persistedDrift.stateVersion)
        assertEquals(Some(2L), repository.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
        assertEquals(PhysicalRecoveryLocatorState.RowActive, repository.findPhysicalRecoveryLocator(podUuid).get.get.state)
        assertEquals(chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)), repository.findPhysicalRecoveryLocator(podUuid).get.get.chunks)
        persistedDrift
      finally repository.close()

    val reopened = new SQLiteFloatingPodRepository(database.toString)
    try
      assertEquals(Some(drifted), reopened.findByUuid(podUuid).get)
      assertEquals(Some(2L), reopened.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
      (reopened: PhysicalRecoveryStore).deactivateTrackedIfExpectedActive(podUuid, Some(ownerUuid), drifted.stateVersion, attemptUuid).get match
        case DeactivationAccepted(deactivated) =>
          assertEquals(3L, deactivated.stateVersion)
          assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, reopened.findPhysicalRecoveryLocator(podUuid).get.get.state)
          assertEquals(Some(3L), reopened.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
        case other => throw new AssertionError(s"Expected deactivation after drift, got $other")
    finally reopened.close()

  @Test
  def rejectsWrongAttemptStaleVersionOwnerChangeAndCrossChunkDriftWithoutMutation(): Unit =
    val database = Files.createTempDirectory("floatingpod-active-drift-rejections").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000931")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000932")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000933")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000934")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val wrongAttemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000935")
    val otherOwnerUuid = UUID.fromString("00000000-0000-0000-0000-000000000936")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
      repository.insert(inactive).get
      repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
      val active = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
        case AdmissionAccepted(state) => state
        case other => throw new AssertionError(s"Expected activation, got $other")
      val originalState = repository.findByUuid(podUuid).get.get
      val originalLocator = repository.findPhysicalRecoveryLocator(podUuid).get.get

      val wrongAttempt = repository.updateTrackedActiveDrift(active.copy(yaw = 15F), wrongAttemptUuid)
      assertTrue(wrongAttempt.isFailure)
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)

      val staleVersion = repository.updateTrackedActiveDrift(active.copy(stateVersion = active.stateVersion - 1L, yaw = 15F), attemptUuid)
      assertTrue(staleVersion.isFailure)
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)

      val ownerChange = repository.updateTrackedActiveDrift(active.copy(ownerUuid = Some(otherOwnerUuid), yaw = 15F), attemptUuid)
      assertTrue(ownerChange.isFailure)
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)

      val crossChunk = repository.updateTrackedActiveDrift(active.copy(posX = 216D, yaw = 15F), attemptUuid)
      assertTrue(crossChunk.isFailure)
      assertEquals(Some(originalState), repository.findByUuid(podUuid).get)
      assertEquals(Some(originalLocator), repository.findPhysicalRecoveryLocator(podUuid).get)
    finally repository.close()

  @Test
  def rollsBackDomainDriftWhenLocatorVersionUpdateFails(): Unit =
    val database = Files.createTempDirectory("floatingpod-active-drift-rollback").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000941")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000942")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000943")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000944")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val initialized = new SQLiteFloatingPodRepository(database.toString)
    val active =
      try
        initialized.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
        initialized.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
        initialized.insert(inactive).get
        initialized.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
        initialized.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
          case AdmissionAccepted(state) => state
          case other => throw new AssertionError(s"Expected activation, got $other")
      finally initialized.close()

    val rawConnection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = rawConnection.createStatement()
      try
        statement.execute(
          "CREATE TRIGGER reject_tracked_active_locator_update " +
            "BEFORE UPDATE OF domain_version ON physical_recovery_locator " +
            "WHEN OLD.locator_state = 'ROW_ACTIVE' BEGIN " +
            "SELECT RAISE(ABORT, 'forced locator update rejection'); END"
        )
      finally statement.close()
    finally rawConnection.close()

    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val beforeLocator = repository.findPhysicalRecoveryLocator(podUuid).get.get
      val update = repository.updateTrackedActiveDrift(active.copy(yaw = 45F), attemptUuid)

      assertTrue(update.isFailure)
      assertTrue(Option(update.failed.get.getMessage).exists(_.contains("forced locator update rejection")))
      assertEquals(Some(active), repository.findByUuid(podUuid).get)
      assertEquals(Some(beforeLocator), repository.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals(Some(active.stateVersion), beforeLocator.domainVersion)
    finally repository.close()

  @Test
  def changesOwnedModeWithPoseAndLocatorCasAndKeepsWorldIdleInsideTheOwnerSlot(): Unit =
    val database = Files.createTempDirectory("floatingpod-mode-cas").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000951")
    val secondPodUuid = UUID.fromString("00000000-0000-0000-0000-000000000952")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000953")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000954")
    val secondAttemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000955")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000956")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val secondInactive = inactive.copy(uuid = secondPodUuid)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val pose = FloatingPodPose("overworld", 203D, 65D, -107D, 45F, 12F, 7)
    var repository = new SQLiteFloatingPodRepository(database.toString)
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      val binding = repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
      repository.insert(inactive).get
      repository.insert(secondInactive).get
      repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
      val active = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
        case AdmissionAccepted(state) => state
        case other => throw new AssertionError(s"Expected activation, got $other")

      val changed = (repository: PhysicalRecoveryStore).setOperatingModeTrackedIfOwned(
        binding, podUuid, ownerUuid, active.stateVersion, attemptUuid, PodOperatingMode.WorldIdle, pose
      ).get match
        case ModeChanged(state) => state
        case other => throw new AssertionError(s"Expected mode change, got $other")
      assertEquals(2L, changed.stateVersion)
      assertEquals(PodOperatingMode.WorldIdle, changed.operatingMode)
      assertEquals(pose.posX, changed.posX)
      assertEquals(pose.posY, changed.posY)
      assertEquals(pose.posZ, changed.posZ)
      assertEquals(pose.yaw, changed.yaw)
      assertEquals(pose.pitch, changed.pitch)
      assertEquals(Some(2L), repository.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
      assertEquals(PhysicalRecoveryLocatorState.RowActive, repository.findPhysicalRecoveryLocator(podUuid).get.get.state)

      repository.close()
      repository = new SQLiteFloatingPodRepository(database.toString)
      assertEquals(Some(changed), repository.findByUuid(podUuid).get)
      assertEquals(Some(2L), repository.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))

      (repository: PhysicalRecoveryStore).setOperatingModeTrackedIfOwned(
        binding, podUuid, ownerUuid, active.stateVersion, attemptUuid, PodOperatingMode.WorldIdle, pose
      ).get match
        case ModeAlreadySet(state) => assertEquals(changed, state)
        case other => throw new AssertionError(s"Expected idempotent mode result, got $other")

      val staleCachedDrift = repository.updateTrackedActiveDrift(changed.copy(operatingMode = PodOperatingMode.Normal, yaw = 30F), attemptUuid)
      assertTrue(staleCachedDrift.isFailure)
      assertEquals(Some(changed), repository.findByUuid(podUuid).get)
      assertEquals(Some(2L), repository.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))

      repository.prepareCreateLocator(secondPodUuid, 0L, secondAttemptUuid, 7, chunks).get
      (repository: PhysicalRecoveryStore).activatePreparedIfAdmissible(secondInactive, ownerUuid, 0L, 7, 256, secondAttemptUuid).get match
        case com.wi.floatingpod.storage.AdmissionOutcome.Rejected(com.wi.floatingpod.storage.AdmissionRejection.PlayerActiveLimitReached) => ()
        case other => throw new AssertionError(s"WorldIdle must retain its owned active slot, got $other")
      assertEquals(Some(changed), repository.findByUuid(podUuid).get)
    finally repository.close()

  @Test
  def rejectsModeCasForWrongOwnerVersionAttemptBindingOrPhysicalMovementWithoutMutation(): Unit =
    val database = Files.createTempDirectory("floatingpod-mode-rejections").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000961")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000962")
    val otherOwner = UUID.fromString("00000000-0000-0000-0000-000000000963")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000964")
    val wrongAttempt = UUID.fromString("00000000-0000-0000-0000-000000000965")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000966")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val pose = FloatingPodPose("overworld", 200D, 64D, -104D, 0F, 0F, 7)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      val binding = repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
      repository.insert(inactive).get
      repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
      val active = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
        case AdmissionAccepted(state) => state
        case other => throw new AssertionError(s"Expected activation, got $other")
      val originalState = repository.findByUuid(podUuid).get.get
      val originalLocator = repository.findPhysicalRecoveryLocator(podUuid).get.get
      val store: PhysicalRecoveryStore = repository

      assertEquals(ModeRejected(OperatingModeChangeRejection.NotOwner), store.setOperatingModeTrackedIfOwned(binding, podUuid, otherOwner, active.stateVersion, attemptUuid, PodOperatingMode.WorldIdle, pose).get)
      assertEquals(ModeRejected(OperatingModeChangeRejection.VersionMismatch(active.stateVersion)), store.setOperatingModeTrackedIfOwned(binding, podUuid, ownerUuid, active.stateVersion - 1L, attemptUuid, PodOperatingMode.WorldIdle, pose).get)
      assertEquals(ModeRejected(OperatingModeChangeRejection.RecoveryObligationMismatch), store.setOperatingModeTrackedIfOwned(binding, podUuid, ownerUuid, active.stateVersion, wrongAttempt, PodOperatingMode.WorldIdle, pose).get)
      assertEquals(ModeRejected(OperatingModeChangeRejection.BindingMismatch), store.setOperatingModeTrackedIfOwned(binding.copy(formatVersion = 2), podUuid, ownerUuid, active.stateVersion, attemptUuid, PodOperatingMode.WorldIdle, pose).get)
      assertEquals(ModeRejected(OperatingModeChangeRejection.CrossChunkMovement), store.setOperatingModeTrackedIfOwned(binding, podUuid, ownerUuid, active.stateVersion, attemptUuid, PodOperatingMode.WorldIdle, pose.copy(posX = 216D)).get)
      assertEquals(originalState, repository.findByUuid(podUuid).get.get)
      assertEquals(originalLocator, repository.findPhysicalRecoveryLocator(podUuid).get.get)

      assertEquals(
        ModeRejected(OperatingModeChangeRejection.UnknownPod),
        store.setOperatingModeTrackedIfOwned(binding, UUID.fromString("00000000-0000-0000-0000-000000000967"), ownerUuid, 0L, attemptUuid, PodOperatingMode.WorldIdle, pose).get
      )

      (repository: PhysicalRecoveryStore).deactivateTrackedIfExpectedActive(podUuid, Some(ownerUuid), active.stateVersion, attemptUuid).get match
        case DeactivationAccepted(_) => ()
        case other => throw new AssertionError(s"Expected deactivation before testing inactive mode change, got $other")
      assertEquals(
        ModeRejected(OperatingModeChangeRejection.Inactive),
        store.setOperatingModeTrackedIfOwned(binding, podUuid, ownerUuid, active.stateVersion + 1L, attemptUuid, PodOperatingMode.WorldIdle, pose).get
      )
    finally repository.close()

  @Test
  def rollsBackModeWhenLocatorVersionUpdateFails(): Unit =
    val database = Files.createTempDirectory("floatingpod-mode-rollback").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000971")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000000972")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000973")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000974")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val active =
      try
        repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
        val binding = repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
        repository.insert(inactive).get
        repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
        val value = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get.asInstanceOf[AdmissionAccepted].state
        (binding, value)
      catch
        case failure: Throwable =>
          repository.close()
          throw failure
    val (binding, activated) = active
    repository.close()

    val rawConnection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = rawConnection.createStatement()
      try statement.execute("CREATE TRIGGER reject_mode_locator_update BEFORE UPDATE OF domain_version ON physical_recovery_locator WHEN OLD.locator_state = 'ROW_ACTIVE' BEGIN SELECT RAISE(ABORT, 'forced mode locator update rejection'); END")
      finally statement.close()
    finally rawConnection.close()

    val reopened = new SQLiteFloatingPodRepository(database.toString)
    try
      val beforeLocator = reopened.findPhysicalRecoveryLocator(podUuid).get.get
      val result = (reopened: PhysicalRecoveryStore).setOperatingModeTrackedIfOwned(
        binding, podUuid, ownerUuid, activated.stateVersion, attemptUuid, PodOperatingMode.WorldIdle,
        FloatingPodPose("overworld", 203D, 65D, -107D, 45F, 12F, 7)
      )
      assertTrue(result.isFailure)
      assertTrue(Option(result.failed.get.getMessage).exists(_.contains("forced mode locator update rejection")))
      assertEquals(Some(activated), reopened.findByUuid(podUuid).get)
      assertEquals(Some(beforeLocator), reopened.findPhysicalRecoveryLocator(podUuid).get)
    finally reopened.close()

  @Test
  def createsAndRetainsLegacyUncertifiedSingletonBindingWithoutCreatingDomainRowsOrLocators(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000891")
    val expected = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 1)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertEquals(None, repository.findPhysicalRecoveryBinding().get)

      assertEquals(expected, repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get)
      assertEquals(Some(expected), repository.findPhysicalRecoveryBinding().get)
      assertEquals(expected, repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get)
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  @Test
  def certifiesLegacyUncertifiedSingletonBindingWithoutCreatingDomainRowsOrLocators(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000881")
    val expected = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 1)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertEquals(PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 1), repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get)

      assertEquals(expected, repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get)
      assertEquals(Some(expected), repository.findPhysicalRecoveryBinding().get)
      assertEquals(expected, repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get)
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  @Test
  def preparesCreateLocatorForExistingInactiveRowWithoutMutatingDomainState(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000901")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000902")
    val state = FloatingPodState(podUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000903")), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 0 to 4; chunkZ <- -4 to 0 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(state).isSuccess)

      val prepared = repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get

      assertEquals(podUuid, prepared.podUuid)
      assertEquals(attemptUuid, prepared.attemptUuid)
      assertEquals(PhysicalRecoveryLocatorState.CreatePrepared, prepared.state)
      assertEquals(PhysicalRecoveryLocatorReason.CreatePrepared, prepared.reasonCode)
      assertEquals(Some(0L), prepared.domainVersion)
      assertEquals(Some(7), prepared.targetDimensionId)
      assertEquals(chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)), prepared.chunks)
      assertEquals(Some(prepared), repository.findPhysicalRecoveryLocator(podUuid).get)

      val unchanged = repository.findByUuid(podUuid).get.get
      assertFalse(unchanged.isActive)
      assertEquals(0L, unchanged.stateVersion)
      assertEquals(state.worldName, unchanged.worldName)
      assertEquals(state.lastUpdated, unchanged.lastUpdated)
    finally repository.close()

  @Test
  def rejectsCreateLocatorChunksFromAnotherDimensionWithoutPersistingLocatorOrMutatingDomainRow(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000911")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000912")
    val state = FloatingPodState(podUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000913")), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val chunks = (for chunkX <- 0 to 4; chunkZ <- -4 to 0 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet - PhysicalRecoveryChunk(7, 0, -4) + PhysicalRecoveryChunk(8, 0, -4)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(state).isSuccess)

      val result = repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks)

      assertTrue(result.isFailure)
      assertEquals(None, repository.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals(Some(state), repository.findByUuid(podUuid).get)
    finally repository.close()

  @Test
  def rejectsCreateLocatorChunksOutsideFiveByFiveSquareWithoutPersistingLocatorOrMutatingDomainRow(): Unit =
    val database = Files.createTempDirectory("floatingpod-physical-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000921")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000922")
    val state = FloatingPodState(podUuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000923")), 0L, "overworld", 32D, 64D, -16D, 0F, 0F, false, 99L, None)
    val chunks = (0 to 24).map(chunkX => PhysicalRecoveryChunk(7, chunkX, 0)).toSet
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      assertTrue(repository.insert(state).isSuccess)

      val result = repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks)

      assertTrue(result.isFailure)
      assertEquals(None, repository.findPhysicalRecoveryLocator(podUuid).get)
      assertEquals(Some(state), repository.findByUuid(podUuid).get)
    finally repository.close()
