package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.AdmissionOutcome.Accepted as AdmissionAccepted
import com.wi.floatingpod.storage.DeactivationOutcome.Accepted as DeactivationAccepted
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
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
