package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore}
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertTrue}
import org.junit.jupiter.api.Test
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** S3 只经公开 manager seam 验证身份、提交顺序、补偿和恢复。 */
class ActiveFloatingPodManagerTest:
  @Test
  def authenticatedSpawnReturnsExactCommittedState(): Unit =
    val fixture = Fixture(1)
    val result = fixture.spawn()
    assertTrue(result.isSuccess)
    val accepted = result.get.asInstanceOf[SpawnOutcome.Accepted]
    assertEquals(17L, accepted.spawned.exactState.stateVersion)
    assertEquals(101L, accepted.spawned.exactState.lastUpdated)
    assertEquals(Some(fixture.world.dimensionId), accepted.spawned.exactState.dimensionId)
    assertEquals(fixture.repository.states(fixture.uuid), accepted.spawned.exactState)
    assertEquals(List("find", "prepare", "request", "spawn", "activate"), fixture.events.toList)
    val expectedChunks = (for chunkX <- -2 to 2; chunkZ <- -2 to 2 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    assertEquals(Some(PhysicalRecoveryLocator(fixture.uuid, fixture.attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), expectedChunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))), fixture.repository.preparedLocator)
    assertEquals(Some(fixture.attemptUuid), fixture.repository.lastTrackedActivationAttempt)
    assertEquals(0, fixture.repository.legacyActivationCalls)

  @Test
  def transitionStoreReceivesInactiveCandidateAndAuthoritativeDimension(): Unit =
    val fixture = Fixture(18)
    assertTrue(fixture.spawn().isSuccess)
    assertFalse(fixture.repository.lastActivationTarget.get.isActive)
    assertEquals(None, fixture.repository.lastActivationTarget.get.dimensionId)
    assertEquals(Some(fixture.world.dimensionId), fixture.repository.lastActivationDimension)

  @Test
  def typedLateAdmissionRejectionCleansEntityAndTicketExactlyOnce(): Unit =
    val fixture = Fixture(2)
    fixture.repository.activateResult = Some(Success(AdmissionOutcome.Rejected(AdmissionRejection.GlobalActiveLimitReached)))
    val result = fixture.spawn()
    assertEquals(Success(SpawnOutcome.Rejected(AdmissionRejection.GlobalActiveLimitReached)), result)
    assertEquals(1, fixture.world.removeCalls)
    assertEquals(1, fixture.gateway.releaseCalls)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  @Test
  def rejectedSpawnWithTicketCleanupFailureSurfacesFailureAndKeepsTicketActionable(): Unit =
    val fixture = Fixture(20)
    fixture.repository.activateResult = Some(Success(AdmissionOutcome.Rejected(AdmissionRejection.GlobalActiveLimitReached)))
    fixture.gateway.releaseFailuresRemaining = 1
    val result = fixture.spawn()
    assertTrue(result.isFailure)
    assertEquals(1, fixture.world.removeCalls)
    assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
    assertTrue(result.failed.get.getCause.getMessage.contains("Spawn rejected"))

  @Test
  def infrastructureSpawnFailurePreservesPrimaryAndEntityCleanupFailure(): Unit =
    val fixture = Fixture(21)
    fixture.repository.activateResult = Some(Failure(new IllegalStateException("database unavailable")))
    fixture.world.failRemoval = true
    val result = fixture.spawn()
    assertTrue(result.isFailure)
    assertEquals("database unavailable", result.failed.get.getMessage)
    assertEquals(1, result.failed.get.getSuppressed.length)
    assertTrue(fixture.spawn().isFailure)
    assertEquals(1, fixture.gateway.requestCalls)

  @Test
  def invalidAcceptedStateIsCommittedSpawnFaultWithoutCompensation(): Unit =
    val fixture = Fixture(22)
    fixture.repository.activateResult = Some(Success(AdmissionOutcome.Accepted(stateFor(fixture.uuid, fixture.owner))))
    fixture.repository.storedAfterActivation = Some(stateFor(fixture.uuid, fixture.owner).copy(isActive = true, dimensionId = Some(7), stateVersion = 55L))
    assertTrue(fixture.spawn().isFailure)
    assertEquals(0, fixture.world.removeCalls)
    assertEquals(0, fixture.gateway.releaseCalls)
    assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
    assertTrue(fixture.manager.committedSpawnFaultSnapshot().contains(fixture.uuid))
    val fault = fixture.manager.committedSpawnFaultSnapshot()(fixture.uuid)
    assertEquals(0L, fault.exactCommittedState.stateVersion)
    assertEquals(1L, fault.exactCommittedState.lastUpdated)
    assertEquals(None, fault.exactCommittedState.dimensionId)
    assertTrue(fixture.spawn().isFailure)

  @Test
  def invalidDeactivationAcceptanceIsDurablyFaultedWithoutAnotherStoreCall(): Unit =
    val fixture = Fixture(25)
    assertTrue(fixture.spawn().isSuccess)
    val exactCommitted = fixture.repository.states(fixture.uuid).copy(uuid = uuidOf(250), isActive = false, stateVersion = 91L, lastUpdated = 909L)
    fixture.repository.deactivateResult = Some(Success(DeactivationOutcome.Accepted(exactCommitted)))
    assertTrue(fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L).isFailure)
    assertEquals(1, fixture.repository.trackedDeactivationCalls)
    val fault = fixture.manager.durablyDeactivatedFaultSnapshot()(fixture.uuid)
    assertEquals(exactCommitted, fault.exactCommittedState)
    assertTrue(fixture.manager.recall(fixture.uuid).isFailure)
    assertEquals(1, fixture.repository.trackedDeactivationCalls)

  @Test
  def afterRecallCommitCrashpointIsDurablyFaultedWithoutRetry(): Unit =
    val fixture = Fixture(26)
    assertTrue(fixture.spawn().isSuccess)
    val harness = CrashPointHarness.getInstance
    harness.disable()
    harness.enable()
    harness.register(CrashPointHarness.AfterDbCommitRecall)
    try
      assertTrue(fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L).isFailure)
    finally harness.disable()
    assertEquals(1, fixture.repository.trackedDeactivationCalls)
    assertEquals(19L, fixture.manager.durablyDeactivatedFaultSnapshot()(fixture.uuid).exactCommittedState.stateVersion)
    assertTrue(fixture.manager.recall(fixture.uuid).isFailure)
    assertEquals(1, fixture.repository.trackedDeactivationCalls)

  @Test
  def afterRecallEntityRemovalCrashLeavesPendingTicketReleaseWithoutCommit(): Unit =
    val fixture = Fixture(261)
    assertTrue(fixture.spawn().isSuccess)
    val exactActive = fixture.repository.states(fixture.uuid)
    fixture.events.clear()
    val harness = CrashPointHarness.getInstance
    harness.disable()
    try
      harness.enable()
      harness.reset()
      harness.register(CrashPointHarness.AfterRecallEntityRemoval)
      val recalled = fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L)
      assertTrue(recalled.isFailure)
      assertEquals(classOf[CrashPointHarness.CrashPointException], recalled.failed.get.getClass)
      assertEquals(List("remove"), fixture.events.toList)
      assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
      assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
      assertEquals(exactActive, fixture.repository.states(fixture.uuid))
      assertTrue(fixture.repository.states(fixture.uuid).isActive)
      assertEquals(17L, fixture.repository.states(fixture.uuid).stateVersion)
      assertEquals(Map(fixture.uuid -> RecallRecoveryStatus.PendingTicketRelease), fixture.manager.blockedRecallSnapshot())
      assertEquals(0, fixture.repository.trackedDeactivationCalls)
      assertEquals(0, fixture.repository.deactivateCalls)
    finally harness.disable()

  @Test
  def afterRecallTicketReleaseCrashLeavesPendingCommitWithoutDeactivation(): Unit =
    val fixture = Fixture(262)
    assertTrue(fixture.spawn().isSuccess)
    val exactActive = fixture.repository.states(fixture.uuid)
    fixture.events.clear()
    val harness = CrashPointHarness.getInstance
    harness.disable()
    try
      harness.enable()
      harness.reset()
      harness.register(CrashPointHarness.AfterRecallTicketRelease)
      val recalled = fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L)
      assertTrue(recalled.isFailure)
      assertEquals(classOf[CrashPointHarness.CrashPointException], recalled.failed.get.getClass)
      assertEquals(List("remove", "release"), fixture.events.toList)
      assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
      assertTrue(fixture.chunkManager.getAllManagedTickets().isEmpty)
      assertEquals(exactActive, fixture.repository.states(fixture.uuid))
      assertTrue(fixture.repository.states(fixture.uuid).isActive)
      assertEquals(17L, fixture.repository.states(fixture.uuid).stateVersion)
      assertEquals(Map(fixture.uuid -> RecallRecoveryStatus.PendingCommit), fixture.manager.blockedRecallSnapshot())
      assertEquals(0, fixture.repository.trackedDeactivationCalls)
      assertEquals(0, fixture.repository.deactivateCalls)
    finally harness.disable()

  @Test
  def ownerOrVersionRejectionDoesNotAllocatePhysicalResources(): Unit =
    val fixture = Fixture(3)
    assertEquals(Success(SpawnOutcome.Rejected(AdmissionRejection.NotOwner)), fixture.manager.spawn(fixture.uuid, uuidOf(99), 0L, fixture.world, 0D, 64D, 0D, 8))
    assertEquals(0, fixture.gateway.requestCalls)
    assertEquals(0, fixture.world.spawnCalls)
    assertEquals(Success(SpawnOutcome.Rejected(AdmissionRejection.VersionMismatch(0L))), fixture.manager.spawn(fixture.uuid, fixture.owner, 8L, fixture.world, 0D, 64D, 0D, 8))

  @Test
  def ticketQuotaFailureRefusesSpawnWithoutCreatingEntity(): Unit =
    val fixture = Fixture(4)
    fixture.gateway.ticketAvailable = false
    assertTrue(fixture.spawn().isFailure)
    assertEquals(1, fixture.gateway.requestCalls)
    assertEquals(0, fixture.world.spawnCalls)

  @Test
  def entityUuidMismatchRollsBackTicketAndDoesNotPublish(): Unit =
    val fixture = Fixture(5)
    fixture.world.nextUuid = uuidOf(500)
    assertTrue(fixture.spawn().isFailure)
    assertEquals(1, fixture.world.removeCalls)
    assertEquals(1, fixture.gateway.releaseCalls)
    assertTrue(fixture.manager.getAllActivePods().isEmpty)

  @Test
  def failedSpawnCleanupQuarantinesUuidBeforeAnotherAllocation(): Unit =
    val fixture = Fixture(51)
    fixture.world.nextUuid = uuidOf(510)
    fixture.world.failRemoval = true
    assertTrue(fixture.spawn().isFailure)
    fixture.world.nextUuid = null
    fixture.world.failRemoval = false
    assertTrue(fixture.spawn().isFailure)
    assertEquals(1, fixture.gateway.requestCalls)
    assertEquals(1, fixture.world.spawnCalls)

  @Test
  def failedRollbackReleaseIsRetriedBeforeAnotherTicketOrEntityAllocation(): Unit =
    val fixture = Fixture(52)
    fixture.repository.activateResult = Some(Failure(new IllegalStateException("commit unavailable")))
    fixture.gateway.releaseFailuresRemaining = 1
    assertTrue(fixture.spawn().isFailure)
    assertEquals(1, fixture.gateway.requestCalls)
    assertEquals(1, fixture.world.spawnCalls)
    assertTrue(fixture.spawn().isFailure)
    assertEquals(2, fixture.gateway.requestCalls)
    assertEquals(2, fixture.world.spawnCalls)

  @Test
  def entityIdCollisionRollsBackNewPhysicalAllocation(): Unit =
    val first = Fixture(6)
    assertTrue(first.spawn().isSuccess)
    first.repository.states.put(uuidOf(7), stateFor(uuidOf(7), first.owner))
    first.world.nextId = 6
    assertTrue(first.manager.spawn(uuidOf(7), first.owner, 0L, first.world, 2D, 64D, 0D, 8).isFailure)
    assertEquals(1, first.world.removeCalls)
    assertTrue(first.manager.getActivePod(first.uuid).nonEmpty)

  @Test
  def recallUsesFrozenPhysicalThenCriticalCommitOrder(): Unit =
    val fixture = Fixture(8)
    assertTrue(fixture.spawn().isSuccess)
    fixture.events.clear()
    val recalled = fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L)
    assertTrue(recalled.isSuccess)
    assertEquals(List("remove", "release", "deactivate"), fixture.events.toList)
    assertEquals(19L, recalled.get.asInstanceOf[RecallOutcome.Accepted].exactState.stateVersion)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  @Test
  def recallUsesTrackedDeactivationWithTheExactSpawnAttempt(): Unit =
    val fixture = Fixture(81)
    assertTrue(fixture.spawn().isSuccess)
    fixture.events.clear()

    val recalled = fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L)

    assertTrue(recalled.isSuccess)
    assertEquals(List("remove", "release", "deactivate"), fixture.events.toList)
    assertEquals(1, fixture.repository.trackedDeactivationCalls)
    assertEquals(0, fixture.repository.deactivateCalls)
    assertEquals(Some(fixture.attemptUuid), fixture.repository.lastTrackedDeactivationAttempt)
    assertEquals(19L, recalled.get.asInstanceOf[RecallOutcome.Accepted].exactState.stateVersion)
    assertFalse(recalled.get.asInstanceOf[RecallOutcome.Accepted].exactState.isActive)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  @Test
  def transientRecallCommitRetryDoesNotRepeatPhysicalStages(): Unit =
    val fixture = Fixture(9)
    assertTrue(fixture.spawn().isSuccess)
    fixture.repository.deactivateResult = Some(Failure(new IllegalStateException("locked")))
    assertTrue(fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L).isFailure)
    assertEquals(1, fixture.world.removeCalls)
    assertEquals(1, fixture.gateway.releaseCalls)

  @Test
  def ticketReleaseFailureHidesActivePodAndTrustedRetryDoesNotRemoveTwice(): Unit =
    val fixture = Fixture(23)
    assertTrue(fixture.spawn().isSuccess)
    fixture.gateway.releaseFailuresRemaining = 1
    assertTrue(fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L).isFailure)
    assertEquals(1, fixture.world.removeCalls)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.getPodUuidByEntityId(fixture.world.nextId).isEmpty)
    assertEquals(Map(fixture.uuid -> RecallRecoveryStatus.PendingTicketRelease), fixture.manager.blockedRecallSnapshot())
    assertTrue(fixture.manager.recall(fixture.uuid).isSuccess)
    assertEquals(1, fixture.world.removeCalls)
    assertTrue(fixture.manager.blockedRecallSnapshot().isEmpty)
    assertEquals(2, fixture.gateway.releaseCalls)

  @Test
  def typedRecallConflictFaultsAndBlocksOrdinaryAccess(): Unit =
    val fixture = Fixture(10)
    assertTrue(fixture.spawn().isSuccess)
    fixture.repository.deactivateResult = Some(Success(DeactivationOutcome.Rejected(DeactivationRejection.VersionMismatch(44L))))
    assertEquals(Success(RecallOutcome.Rejected(DeactivationRejection.VersionMismatch(44L))), fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L))
    assertEquals(Map(fixture.uuid -> RecallRecoveryStatus.Faulted(DeactivationRejection.VersionMismatch(44L))), fixture.manager.blockedRecallSnapshot())
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 17L).isFailure)

  @Test
  def recallOwnerAndVersionPrecheckDoesNotRemoveEntity(): Unit =
    val fixture = Fixture(11)
    assertTrue(fixture.spawn().isSuccess)
    assertEquals(Success(RecallOutcome.Rejected(DeactivationRejection.NotOwner)), fixture.manager.recallIfExpected(fixture.uuid, Some(uuidOf(12)), 17L))
    assertEquals(0, fixture.world.removeCalls)
    assertEquals(Success(RecallOutcome.Rejected(DeactivationRejection.VersionMismatch(17L))), fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), 1L))
    assertEquals(0, fixture.gateway.releaseCalls)

  @Test
  def cacheFlushUsesTrackedActiveDriftAndReturnedVersion(): Unit =
    val fixture = Fixture(13)
    assertTrue(fixture.spawn().isSuccess)
    val cached = fixture.repository.states(fixture.uuid).copy(posX = 12D)
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, cached).isSuccess)
    assertEquals(1, fixture.manager.flushCachedStates().get)
    assertEquals(0, fixture.repository.genericUpdateCalls)
    assertEquals(1, fixture.repository.trackedDriftCalls)
    assertEquals(0, fixture.manager.flushCachedStates().get)

  @Test
  def failedTrackedCacheFlushDoesNotAdvanceExactStateOrRecallPhysicalPod(): Unit =
    val fixture = Fixture(131)
    assertTrue(fixture.spawn().isSuccess)
    val active = fixture.repository.states(fixture.uuid)
    fixture.repository.trackedDriftResult = Some(Failure(new IllegalStateException("locator update rejected")))
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, active.copy(yaw = 15F)).isSuccess)

    val flush = fixture.manager.flushCachedStates()
    val recall = fixture.manager.recallIfExpected(fixture.uuid, Some(fixture.owner), active.stateVersion + 1L)

    assertTrue(flush.isFailure)
    assertEquals("locator update rejected", flush.failed.get.getMessage)
    assertEquals(Success(RecallOutcome.Rejected(DeactivationRejection.VersionMismatch(active.stateVersion))), recall)
    assertEquals(1, fixture.repository.trackedDriftCalls)
    assertEquals(0, fixture.repository.genericUpdateCalls)
    assertEquals(0, fixture.repository.trackedDeactivationCalls)
    assertEquals(0, fixture.world.removeCalls)
    assertEquals(0, fixture.gateway.releaseCalls)

  @Test
  def cacheRejectsCriticalIdentityDrift(): Unit =
    val fixture = Fixture(14)
    assertTrue(fixture.spawn().isSuccess)
    val baseline = fixture.repository.states(fixture.uuid)
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, baseline.copy(dimensionId = Some(99))).isFailure)
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, baseline.copy(isActive = false)).isFailure)
    assertEquals(0, fixture.manager.flushCachedStates().get)

  @Test
  def cacheCannotChangeOperatingModeWithoutTheCriticalTransition(): Unit =
    val fixture = Fixture(141)
    assertTrue(fixture.spawn().isSuccess)
    val baseline = fixture.repository.states(fixture.uuid)
    val changed = baseline.copy(operatingMode = PodOperatingMode.WorldIdle)

    assertTrue(fixture.manager.updateCachedState(fixture.uuid, changed).isFailure)
    assertEquals(0, fixture.manager.flushCachedStates().get)
    assertEquals(baseline, fixture.repository.states(fixture.uuid))
    assertEquals(0, fixture.repository.trackedDriftCalls)
    assertEquals(0, fixture.repository.genericUpdateCalls)

  @Test
  def recoveryAttachesExistingCanonicalTicketAndEntityWithoutWrite(): Unit =
    val fixture = Fixture(15, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val entity = fixture.world.spawnPod(active)
    val outcome = fixture.manager.recoverCommittedActive(active, fixture.world)
    assertTrue(outcome.get.isInstanceOf[RecoveryDisposition.Attached])
    assertSame(entity, fixture.manager.getActivePod(fixture.uuid).get)
    assertEquals(0, fixture.repository.genericUpdateCalls)

  @Test
  def recoveryRecreatesMissingEntityAndReplayIsIdempotent(): Unit =
    val fixture = Fixture(16, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    assertTrue(fixture.manager.recoverCommittedActive(active, fixture.world).get.isInstanceOf[RecoveryDisposition.Recreated])
    assertEquals(1, fixture.world.spawnCalls)
    assertEquals(RecoveryDisposition.Replayed, fixture.manager.recoverCommittedActive(active, fixture.world).get)
    assertEquals(0, fixture.repository.genericUpdateCalls)

  @Test
  def abortOfAttachedRecoveryKeepsSourceBodyAndOnlyRemovesBootPublication(): Unit =
    val fixture = Fixture(161, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val sourceBody = fixture.world.seed(fixture.uuid)
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)

    assertTrue(fixture.manager.recoverCommittedActiveForStartup(active, fixture.world, receipt).get.isInstanceOf[RecoveryDisposition.Attached])
    fixture.manager.abortStartupRecovery(receipt).get

    assertSame(sourceBody, fixture.world.findPod(fixture.uuid).get)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, active).isFailure)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(active, fixture.repository.states(fixture.uuid))

  @Test
  def abortOfRecreatedRecoveryRemovesOnlyTheCapturedBootBody(): Unit =
    val fixture = Fixture(162, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)

    assertTrue(fixture.manager.recoverCommittedActiveForStartup(active, fixture.world, receipt).get.isInstanceOf[RecoveryDisposition.Recreated])
    val bootBody = fixture.world.findPod(fixture.uuid).get
    fixture.manager.abortStartupRecovery(receipt).get

    assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(active, fixture.repository.states(fixture.uuid))
    assertTrue(bootBody.podUuid == fixture.uuid)

  @Test
  def staleBodyReplacementRejectsAbortWithoutDeletingOrUnpublishing(): Unit =
    val fixture = Fixture(163, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)
    fixture.manager.recoverCommittedActiveForStartup(active, fixture.world, receipt).get
    val capturedBody = fixture.world.lastSpawnedBody.get
    val replacement = fixture.world.installUuidIndexReplacementKeepingResident(capturedBody)

    val failure = fixture.manager.abortStartupRecovery(receipt).failed.get

    assertTrue(failure.getMessage.contains("stale"))
    assertSame(replacement, fixture.world.findPod(fixture.uuid).get)
    assertTrue(fixture.world.bodyIsResident(capturedBody))
    assertTrue(fixture.world.bodyIsResident(replacement))
    assertEquals(0, fixture.world.removeCalls)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).nonEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))

  @Test
  def startupReceiptCannotBeReusedWithAStaleWorld(): Unit =
    val fixture = Fixture(166, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)
    val otherWorld = new TestWorld(fixture.events, fixture.world.dimensionId, 1660, new Object)

    val failure = fixture.manager.recoverCommittedActiveForStartup(active, otherWorld, receipt).failed.get

    assertTrue(failure.getMessage.contains("world does not match"))
    fixture.manager.abortStartupRecovery(receipt).get
    assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))

  @Test
  def staleManagerMappingRejectsReplayReceiptWithoutDeletingOrUnpublishing(): Unit =
    val fixture = Fixture(165, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    fixture.manager.recoverCommittedActive(active, fixture.world).get
    val sourceBody = fixture.world.findPod(fixture.uuid).get
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)
    assertEquals(RecoveryDisposition.Replayed, fixture.manager.recoverCommittedActiveForStartup(active, fixture.world, receipt).get)
    val drifted = active.copy(posX = active.posX + 1D)
    assertTrue(fixture.manager.updateCachedState(fixture.uuid, drifted).isSuccess)
    assertEquals(1, fixture.manager.flushCachedStates().get)
    val committedDrift = fixture.repository.states(fixture.uuid)
    assertEquals(drifted.copy(stateVersion = drifted.stateVersion + 1L, lastUpdated = drifted.lastUpdated + 1L), committedDrift)

    val failure = fixture.manager.abortStartupRecovery(receipt).failed.get

    assertTrue(failure.getMessage.contains("stale"))
    assertSame(sourceBody, fixture.world.findPod(fixture.uuid).get)
    assertSame(sourceBody, fixture.manager.getActivePod(fixture.uuid).get)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(committedDrift, fixture.repository.states(fixture.uuid))

  @Test
  def abortOfRepeatedReplayIsNonDestructive(): Unit =
    val fixture = Fixture(164, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    fixture.manager.recoverCommittedActive(active, fixture.world).get
    val sourceBody = fixture.world.findPod(fixture.uuid).get
    val receipt = fixture.manager.beginStartupRecovery(active, fixture.world)

    assertEquals(RecoveryDisposition.Replayed, fixture.manager.recoverCommittedActiveForStartup(active, fixture.world, receipt).get)
    fixture.manager.abortStartupRecovery(receipt).get

    assertSame(sourceBody, fixture.world.findPod(fixture.uuid).get)
    assertSame(sourceBody, fixture.manager.getActivePod(fixture.uuid).get)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))

  @Test
  def recoveryAcceptsFreshWrapperForTheSameCanonicalWorld(): Unit =
    val fixture = Fixture(24, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val freshWrapper = new TestWorld(fixture.events, fixture.world.dimensionId, 240, fixture.world.logicalWorldKey)
    assertTrue(fixture.manager.recoverCommittedActive(active, freshWrapper).isSuccess)

  @Test
  def recoveryRejectsWorldOrCommittedStateConflict(): Unit =
    val fixture = Fixture(17, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    assertTrue(fixture.manager.recoverCommittedActive(active.copy(stateVersion = 9L), fixture.world).isFailure)
    assertTrue(fixture.manager.recoverCommittedActive(active, new TestWorld(fixture.events, 99, 18)).isFailure)

  @Test
  def recreatedRecoveryEntityIsLocallyRemovedWhenIdentityValidationFails(): Unit =
    val fixture = Fixture(18, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val wrongUuid = uuidOf(9999)
    fixture.world.nextUuid = wrongUuid

    assertTrue(fixture.manager.recoverCommittedActive(active, fixture.world).isFailure)
    assertTrue(fixture.world.findPod(wrongUuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  @Test
  def failureAfterCapturedPartialSpawnRetainsPrimaryAndSuppressesRemovalFailure(): Unit =
    val fixture = Fixture(181, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val wrongUuid = uuidOf(9981)
    fixture.world.nextUuid = wrongUuid
    fixture.world.failRemoval = true

    val primary = fixture.manager.recoverCommittedActive(active, fixture.world).failed.get

    assertTrue(primary.getMessage.contains(s"World returned entity UUID $wrongUuid"))
    assertEquals(1, primary.getSuppressed.length)
    assertEquals("remove failed", primary.getSuppressed.head.getMessage)
    assertTrue(fixture.world.findPod(wrongUuid).nonEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))

  @Test
  def capturedResidentBodyIsRemovedAfterSpawnUuidIndexValidationFails(): Unit =
    val fixture = Fixture(182, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val unrelatedUuid = uuidOf(9182)
    val unrelatedBody = fixture.world.seed(unrelatedUuid)
    val indexFailure = new IllegalStateException(s"World ${fixture.world.diagnosticWorldName} did not index spawned FloatingPod ${fixture.uuid}")
    fixture.world.dropUuidIndexAfterCapture = Some(fixture.uuid)
    fixture.world.uuidIndexFailure = Some(indexFailure)

    val failure = fixture.manager.recoverCommittedActive(active, fixture.world).failed.get
    val capturedBody = fixture.world.lastSpawnedBody.get

    assertSame(indexFailure, failure)
    assertFalse(fixture.world.bodyIsResident(capturedBody))
    assertTrue(fixture.world.uuidIndexFailureObservedExactResidentBody)
    assertTrue(fixture.world.uuidIndexFailureObservedExactEntityIdIndex)
    assertFalse(fixture.world.bodyIsIndexedByEntityId(capturedBody))
    assertEquals(1, fixture.world.removeCalls)
    assertSame(unrelatedBody, fixture.world.findPod(unrelatedUuid).get)
    assertTrue(fixture.world.bodyIsResident(unrelatedBody))
    assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(0, fixture.gateway.releaseCalls)
    assertEquals(active, fixture.repository.states(fixture.uuid))

  @Test
  def capturedBodyRemovalFailureIsSuppressedOnOriginalMissingUuidIndexFailure(): Unit =
    val fixture = Fixture(183, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    val indexFailure = new IllegalStateException(s"World ${fixture.world.diagnosticWorldName} did not index spawned FloatingPod ${fixture.uuid}")
    val removalFailure = new IllegalStateException("captured-body removal failed")
    fixture.world.dropUuidIndexAfterCapture = Some(fixture.uuid)
    fixture.world.uuidIndexFailure = Some(indexFailure)
    fixture.world.failRemoval = true
    fixture.world.removalFailure = Some(removalFailure)

    val failure = fixture.manager.recoverCommittedActive(active, fixture.world).failed.get
    val capturedBody = fixture.world.lastSpawnedBody.get

    assertSame(indexFailure, failure)
    assertTrue(failure.getSuppressed.exists(_ eq removalFailure))
    assertTrue(fixture.world.bodyIsResident(capturedBody))
    assertTrue(fixture.world.bodyIsIndexedByEntityId(capturedBody))
    assertEquals(1, fixture.world.removeCalls)
    assertTrue(fixture.world.findPod(fixture.uuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(0, fixture.gateway.releaseCalls)
    assertEquals(active, fixture.repository.states(fixture.uuid))

  @Test
  def compatibilityRecoveryRetainsOriginalFailureWhenRemovalIsFatal(): Unit =
    val original = new IllegalStateException("captured body indexing failed")
    val cleanup = new LinkageError("captured body fatal removal")
    assertCompatibilityRecoveryEscape(original, cleanup, cleanup)

  @Test
  def compatibilityRecoveryRetainsOriginalFailureWhenRemovalIsInterrupted(): Unit =
    val original = new IllegalStateException("captured body indexing failed")
    val cleanup = new InterruptedException("captured body interrupted removal")
    assertCompatibilityRecoveryEscape(original, cleanup, cleanup)

  @Test
  def compatibilityRecoveryPreservesOriginalFatalWhenRemovalAlsoFails(): Unit =
    val original = new LinkageError("captured body fatal indexing")
    assertCompatibilityRecoveryEscape(original, new IllegalStateException("captured body removal failed"), original)

  private def assertCompatibilityRecoveryEscape(original: Throwable, cleanup: Throwable, expectedEscape: Throwable): Unit =
    val fixture = Fixture(184, active = true)
    val active = fixture.repository.states(fixture.uuid)
    fixture.chunkManager.requestTicket(fixture.uuid, fixture.world, 0, 0).get
    fixture.world.dropUuidIndexAfterCapture = Some(fixture.uuid)
    fixture.world.uuidIndexFailure = Some(original)
    fixture.world.failRemoval = true
    fixture.world.removalFailure = Some(cleanup)

    var escaped = Option.empty[Throwable]
    var returned = false
    try
      fixture.manager.recoverCommittedActive(active, fixture.world)
      returned = true
    catch case failure: Throwable => escaped = Some(failure)

    assertFalse(returned, "fatal/interruption must escape the Try boundary")
    assertSame(expectedEscape, escaped.orNull)
    val secondary = if expectedEscape eq original then cleanup else original
    assertTrue(expectedEscape.getSuppressed.exists(_ eq secondary))
    val capturedBody = fixture.world.lastSpawnedBody.get
    assertTrue(fixture.world.bodyIsResident(capturedBody))
    assertTrue(fixture.world.bodyIsIndexedByEntityId(capturedBody))
    assertEquals(1, fixture.world.removeCalls)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)
    assertTrue(fixture.chunkManager.managedTicketRecords().contains(fixture.uuid))
    assertEquals(0, fixture.gateway.releaseCalls)
    assertEquals(active, fixture.repository.states(fixture.uuid))

  /** 关服报告必须保留单项失败，并继续收回其他可推进状态。 */
  @Test
  def shutdownReportKeepsRecallFailureInsteadOfReportingFalseSuccess(): Unit =
    val fixture = Fixture(27)
    assertTrue(fixture.spawn().isSuccess)
    fixture.repository.deactivateResult = Some(Failure(new IllegalStateException("locked")))

    val report = fixture.manager.shutdownWithReport()

    assertFalse(report.isClean)
    assertEquals(List(fixture.uuid), report.entries.map(_.podUuid))
    assertTrue(report.entries.head.cause.nonEmpty)
    assertTrue(report.remaining.contains(fixture.uuid))

  private def uuidOf(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")
  private def stateFor(uuid: UUID, owner: UUID, active: Boolean = false): FloatingPodState = FloatingPodState(uuid, Some(owner), 0L, "stored", 0D, 64D, 0D, 0F, 0F, active, 1L, if active then Some(7) else None)

  private final case class Fixture(uuidValue: Int, active: Boolean = false):
    val uuid: UUID = uuidOf(uuidValue)
    val owner: UUID = uuidOf(uuidValue + 1000)
    val attemptUuid: UUID = uuidOf(uuidValue + 2000)
    val events = mutable.ListBuffer.empty[String]
    val repository = new TestRepository(stateFor(uuid, owner, active), events, attemptUuid)
    val gateway = new TestGateway(events)
    val world = new TestWorld(events, 7, uuidValue)
    val chunkManager = new ChunkLoadingManager(repository, gateway)
    val manager = new ActiveFloatingPodManager(repository, chunkManager, () => 101L, () => attemptUuid)
    def spawn(): Try[SpawnOutcome] = manager.spawn(uuid, owner, 0L, world, 0D, 64D, 0D, 8)

  private final class TestEntity(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity
  private final class TestWorld(val events: mutable.ListBuffer[String], override val dimensionId: Int, var nextId: Int, val logicalWorldKey: AnyRef = new Object) extends PodWorld:
    private val entitiesByUuid = mutable.HashMap.empty[UUID, TestEntity]
    private val entitiesById = mutable.HashMap.empty[Int, TestEntity]
    private val residentBodies = mutable.ArrayBuffer.empty[TestEntity]
    var nextUuid: UUID = null
    var dropUuidIndexAfterCapture = Option.empty[UUID]
    var uuidIndexFailure = Option.empty[Throwable]
    var lastSpawnedBody = Option.empty[TestEntity]
    var uuidIndexFailureObservedExactResidentBody = false
    var uuidIndexFailureObservedExactEntityIdIndex = false
    var spawnCalls = 0
    var removeCalls = 0
    var failRemoval = false
    var removalFailure = Option.empty[Throwable]
    override def diagnosticWorldName: String = s"world-$dimensionId"
    override def sameWorld(other: PodWorld): Boolean = other match
      case test: TestWorld => logicalWorldKey.eq(test.logicalWorldKey)
      case _ => false
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = entitiesByUuid.get(podUuid)
    override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
      case test: TestEntity => residentBodies.exists(_ eq test) && entitiesByUuid.get(test.podUuid).forall(_ eq test) && entitiesById.get(test.entityId).forall(_ eq test)
      case _ => false
    override def spawnPod(state: FloatingPodState): ActivePodEntity =
      events += "spawn"; spawnCalls += 1
      val entity = new TestEntity(Option(nextUuid).getOrElse(state.uuid), nextId)
      nextId += 1
      residentBodies += entity
      entitiesByUuid.put(entity.podUuid, entity)
      entitiesById.put(entity.entityId, entity)
      lastSpawnedBody = Some(entity)
      entity
    override def spawnPod(state: FloatingPodState, retainCreatedBody: ActivePodEntity => Unit): ActivePodEntity =
      require(retainCreatedBody != null, "retainCreatedBody must not be null")
      val entity = spawnPod(state)
      retainCreatedBody(entity)
      if dropUuidIndexAfterCapture.contains(entity.podUuid) then
        entitiesByUuid.remove(entity.podUuid)
        uuidIndexFailureObservedExactResidentBody = residentBodies.exists(_ eq entity)
        uuidIndexFailureObservedExactEntityIdIndex = entitiesById.get(entity.entityId).exists(_ eq entity)
        throw uuidIndexFailure.getOrElse(new IllegalStateException(s"World $diagnosticWorldName did not index spawned FloatingPod ${state.uuid}"))
      entity
    def seed(podUuid: UUID): ActivePodEntity =
      val entity = new TestEntity(podUuid, nextId)
      nextId += 1
      residentBodies += entity
      entitiesByUuid.put(podUuid, entity)
      entitiesById.put(entity.entityId, entity)
      entity
    def installUuidIndexReplacementKeepingResident(capturedBody: ActivePodEntity): ActivePodEntity =
      val captured = capturedBody match
        case test: TestEntity => test
        case _ => throw new IllegalArgumentException("test world received a foreign captured body")
      val entity = new TestEntity(captured.podUuid, nextId)
      nextId += 1
      residentBodies += entity
      entitiesByUuid.put(captured.podUuid, entity)
      entitiesById.put(entity.entityId, entity)
      entity
    def bodyIsResident(body: ActivePodEntity): Boolean = body match
      case test: TestEntity => residentBodies.exists(_ eq test)
      case _ => false
    def bodyIsIndexedByEntityId(body: ActivePodEntity): Boolean = body match
      case test: TestEntity => entitiesById.get(test.entityId).exists(_ eq test)
      case _ => false
    private def removeResidentAndIndexes(entity: TestEntity): Unit =
      val index = residentBodies.indexWhere(_ eq entity)
      if index >= 0 then residentBodies.remove(index)
      entitiesById.get(entity.entityId).filter(_ eq entity).foreach(_ => entitiesById.remove(entity.entityId))
    override def removePod(entity: ActivePodEntity): Unit =
      events += "remove"; removeCalls += 1
      if failRemoval then throw removalFailure.getOrElse(new IllegalStateException("remove failed"))
      entity match
        case test: TestEntity =>
          removeResidentAndIndexes(test)
          entitiesByUuid.get(test.podUuid).filter(_ eq test).foreach(_ => entitiesByUuid.remove(test.podUuid))
        case _ => throw new IllegalArgumentException("test world received a foreign body type")

  private final class TestGateway(events: mutable.ListBuffer[String]) extends ChunkTicketGateway:
    final class TestTicket extends ChunkTicket
    var ticketAvailable = true
    var requestCalls = 0
    var releaseCalls = 0
    var releaseFailuresRemaining = 0
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = { events += "request"; requestCalls += 1; if ticketAvailable then Some(new TestTicket) else None }
    override def release(ticket: ChunkTicket): Unit =
      events += "release"; releaseCalls += 1
      if releaseFailuresRemaining > 0 then
        releaseFailuresRemaining -= 1
        throw new IllegalStateException("release failed")
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ()
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = None
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class TestRepository(initial: FloatingPodState, events: mutable.ListBuffer[String], recoveryAttemptUuid: UUID) extends FloatingPodRepository, ActivityTransitionStore, PhysicalRecoveryStore:
    val states = mutable.HashMap(initial.uuid -> initial)
    var activateResult: Option[Try[AdmissionOutcome]] = None
    var deactivateResult: Option[Try[DeactivationOutcome]] = None
    var deactivateCalls = 0
    var trackedDeactivationCalls = 0
    var genericUpdateCalls = 0
    var trackedDriftCalls = 0
    var trackedDriftResult: Option[Try[FloatingPodState]] = None
    var lastActivationTarget: Option[FloatingPodState] = None
    var lastActivationDimension: Option[Int] = None
    var lastTrackedActivationAttempt: Option[UUID] = None
    var lastTrackedDeactivationAttempt: Option[UUID] = None
    var legacyActivationCalls = 0
    var preparedLocator: Option[PhysicalRecoveryLocator] = None
    private var activeLocator: Option[PhysicalRecoveryLocator] =
      if initial.isActive then Some(activeLocatorFor(initial, recoveryAttemptUuid)) else None
    var storedAfterActivation: Option[FloatingPodState] = None
    override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] =
      legacyActivationCalls += 1
      activate(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit)
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = Success(None)
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = Failure(new UnsupportedOperationException("not used by manager spawn"))
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = Failure(new UnsupportedOperationException("not used by manager spawn"))
    override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] =
      events += "prepare"
      val prepared = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(expectedDomainVersion), Some(targetDimensionId), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
      preparedLocator = Some(prepared)
      Success(prepared)
    override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] =
      lastTrackedActivationAttempt = Some(attemptUuid)
      val outcome = activate(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit)
      outcome.foreach {
        case AdmissionOutcome.Accepted(committed) =>
          preparedLocator.foreach { prepared =>
            if prepared.podUuid == committed.uuid && prepared.attemptUuid == attemptUuid then
              activeLocator = Some(prepared.copy(state = PhysicalRecoveryLocatorState.RowActive, reasonCode = PhysicalRecoveryLocatorReason.RowActive, domainVersion = Some(committed.stateVersion)))
          }
        case _ => ()
      }
      outcome
    override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] =
      trackedDeactivationCalls += 1
      lastTrackedDeactivationAttempt = Some(attemptUuid)
      deactivate(podUuid, expectedOwner, expectedStateVersion)
    override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] =
      trackedDriftCalls += 1
      trackedDriftResult.getOrElse {
        val persisted = state.copy(stateVersion = state.stateVersion + 1L, lastUpdated = state.lastUpdated + 1L)
        states.put(state.uuid, persisted)
        activeLocator = activeLocator.map(_.copy(domainVersion = Some(persisted.stateVersion)))
        Success(persisted)
      }
    override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = Success(activeLocator.orElse(preparedLocator).filter(_.podUuid == podUuid))
    private def activate(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] =
      events += "activate"
      require(!target.isActive, "store target must be inactive before the transition commits")
      require(target.dimensionId == initial.dimensionId, "store target must preserve prior persisted dimension")
      require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "global limit must be 1..256")
      val stored = states.getOrElse(target.uuid, throw new IllegalStateException("missing pod"))
      require(stored.ownerUuid.contains(authenticatedOwner), "store owner must match")
      require(stored.stateVersion == expectedStateVersion, "store version must match")
      require(!stored.isActive, "store pod must be inactive")
      lastActivationTarget = Some(target)
      lastActivationDimension = Some(dimensionId)
      activateResult match
        case Some(result) =>
          storedAfterActivation.foreach(state => states.put(target.uuid, state))
          result
        case None =>
          val committed = target.copy(isActive = true, dimensionId = Some(dimensionId), stateVersion = 17L, lastUpdated = 101L)
          states.put(target.uuid, committed)
          Success(AdmissionOutcome.Accepted(committed))
    override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] =
      deactivateCalls += 1
      deactivate(podUuid, expectedOwner, expectedStateVersion)
    private def deactivate(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] =
      events += "deactivate"
      val outcome = deactivateResult.getOrElse {
        val current = states(podUuid)
        val committed = current.copy(isActive = false, stateVersion = 19L, lastUpdated = 202L)
        states.put(podUuid, committed)
        Success(DeactivationOutcome.Accepted(committed))
      }
      outcome.foreach {
        case DeactivationOutcome.Accepted(committed) =>
          activeLocator.foreach(locator => activeLocator = Some(locator.copy(state = PhysicalRecoveryLocatorState.RowInactivePending, reasonCode = PhysicalRecoveryLocatorReason.RowInactivePending, domainVersion = Some(committed.stateVersion))))
        case _ => ()
      }
      outcome
    private def activeLocatorFor(state: FloatingPodState, attemptUuid: UUID): PhysicalRecoveryLocator =
      val chunks = (for
        chunkX <- math.floor(state.posX / ChunkLoadingManager.ChunkSize).toInt - 2 to math.floor(state.posX / ChunkLoadingManager.ChunkSize).toInt + 2
        chunkZ <- math.floor(state.posZ / ChunkLoadingManager.ChunkSize).toInt - 2 to math.floor(state.posZ / ChunkLoadingManager.ChunkSize).toInt + 2
      yield PhysicalRecoveryChunk(state.dimensionId.get, chunkX, chunkZ)).toList
      PhysicalRecoveryLocator(state.uuid, attemptUuid, PhysicalRecoveryLocatorState.RowActive, PhysicalRecoveryLocatorReason.RowActive, Some(state.stateVersion), state.dimensionId, chunks)
    override def insert(state: FloatingPodState): Try[FloatingPodState] = { states.put(state.uuid, state); Success(state) }
    override def update(state: FloatingPodState): Try[FloatingPodState] = { genericUpdateCalls += 1; val persisted = state.copy(stateVersion = state.stateVersion + 1); states.put(state.uuid, persisted); Success(persisted) }
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = { events += "find"; Success(states.get(uuid)) }
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.filter(_.isActive).toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(states.remove(uuid).nonEmpty)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()
