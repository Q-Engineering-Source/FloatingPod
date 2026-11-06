package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{
  AdmissionOutcome,
  DeactivationOutcome,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryCutoverState,
  PhysicalRecoveryLocator,
  PhysicalRecoveryLocatorReason,
  PhysicalRecoveryLocatorState
}

import java.util.UUID
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

final class MySqlRepositoryStagingTest:
  private val podUuid = UUID.fromString("91000000-0000-0000-0000-000000000001")
  private val attemptUuid = UUID.fromString("92000000-0000-0000-0000-000000000002")
  private val bindingUuid = UUID.fromString("93000000-0000-0000-0000-000000000003")

  @Test
  def transactionFacadeProvidesReadYourWritesAndFreezesOneCompleteSnapshotDelta(): Unit =
    val initial = MySqlFullRepositorySnapshot.empty
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())
    val inserted = inactive()

    assertTrue(stage.insert(inserted).isSuccess)
    assertEquals(Some(inserted), stage.findByUuid(podUuid).get)
    assertEquals(List(inserted), stage.findAll().get)

    val frozen = stage.closeAndFreeze().get
    assertFalse(initial.exactlyEquals(frozen))
    val delta = MySqlFullRepositoryDelta.freeze(initial, frozen)
    assertFalse(delta.isEmpty)
    assertEquals(1, frozen.pods.size)
    assertEquals(podUuid, MySqlStoredRows.decodeUuid(frozen.pods.head.uuidBytes, "uuid"))

  @Test
  def swallowedStagingFailureRemainsStickyAndCannotPublishLaterChanges(): Unit =
    val initial = MySqlFullRepositorySnapshot.empty
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())
    val activeWithoutPreparation = inactive().copy(isActive = true, operatingMode = PodOperatingMode.WorldIdle)

    val firstFailure = stage.insert(activeWithoutPreparation).failed.get
    val swallowedResult = stage.insert(inactive())
    assertSame(firstFailure, swallowedResult.failed.get)
    assertTrue(stage.snapshot.exactlyEquals(initial))
    assertSame(firstFailure, stage.closeAndFreeze().failed.get)

  @Test
  def trackedDriftFreezesDomainAndLocatorAtTheSameNewVersion(): Unit =
    val before = trackedActiveSnapshot()
    val current = MySqlStoredRows.materializeGenericState(before.pods.head)
    val requested = current.copy(posX = 9.0d, lastUpdated = current.lastUpdated)

    val (after, returned) = MySqlRepositoryTransitionRules.trackedDrift(before, requested, attemptUuid)
    after.validate()
    assertEquals(1L, returned.stateVersion)
    assertEquals(1L, after.pods.head.stateVersion)
    assertEquals(Some(1L), after.recovery.locators.head.domainVersion)
    assertEquals(before.recovery.chunks, after.recovery.chunks)
    assertNotEquals(before.canonicalSha256, after.canonicalSha256)
    MySqlFullRepositoryDelta.freeze(before, after)

  @Test
  def trackedDriftRejectsCrossChunkMovementWithoutChangingThePriorSnapshot(): Unit =
    val before = trackedActiveSnapshot()
    val current = MySqlStoredRows.materializeGenericState(before.pods.head)
    val requested = current.copy(posX = 32.0d)

    org.junit.jupiter.api.Assertions.assertThrows(
      classOf[IllegalStateException],
      () => MySqlRepositoryTransitionRules.trackedDrift(before, requested, attemptUuid)
    )
    assertEquals(0L, before.pods.head.stateVersion)
    assertEquals(Some(0L), before.recovery.locators.head.domainVersion)

  @Test
  def stagedPublicOperationsKeepTheCompletePreparedActiveModeStopAndAbsenceGraphConsistent(): Unit =
    val owner = UUID.fromString("94000000-0000-0000-0000-000000000004")
    val binding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)
    val start = inactive().copy(ownerUuid = Some(owner))
    val chunks = (for x <- -2 to 2; z <- -2 to 2 yield PhysicalRecoveryChunk(7, x, z)).toSet

    val certifiedBaseline = MySqlFullRepositorySnapshot(
      Vector.empty,
      Vector.empty,
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector.empty),
      Vector.empty
    )
    certifiedBaseline.validate()
    val prepare = new MySqlRepositoryStaging(certifiedBaseline, Thread.currentThread())
    assertEquals(start, prepare.insert(start).get)
    assertEquals(attemptUuid, prepare.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get.attemptUuid)
    val prepared = freezeAndCheckDelta(certifiedBaseline, prepare)

    val activate = new MySqlRepositoryStaging(prepared, Thread.currentThread())
    val accepted = activate.activatePreparedIfAdmissible(start, owner, 0L, 7, 256, attemptUuid).get
    val active = accepted match
      case AdmissionOutcome.Accepted(state) => state
      case other => throw new AssertionError("expected prepared activation, received " + other)
    assertEquals(1L, active.stateVersion)
    assertEquals(Some(1L), activate.findPhysicalRecoveryLocator(podUuid).get.get.domainVersion)
    val activeSnapshot = freezeAndCheckDelta(prepared, activate)

    val pose = FloatingPodPose("world", active.posX, active.posY, active.posZ, active.yaw, active.pitch, 7)
    val changeMode = new MySqlRepositoryStaging(activeSnapshot, Thread.currentThread())
    val mode = changeMode.setOperatingModeTrackedIfOwned(
      binding,
      podUuid,
      owner,
      1L,
      attemptUuid,
      PodOperatingMode.WorldIdle,
      pose
    ).get
    val idled = mode match
      case com.wi.floatingpod.storage.OperatingModeChangeOutcome.Changed(state) => state
      case other => throw new AssertionError("expected owner mode CAS, received " + other)
    assertEquals(2L, idled.stateVersion)
    assertEquals(Some(2L), changeMode.findPhysicalRecoveryLocator(podUuid).get.get.domainVersion)
    val idledSnapshot = freezeAndCheckDelta(activeSnapshot, changeMode)

    val stop = new MySqlRepositoryStaging(idledSnapshot, Thread.currentThread())
    val stopped = stop.deactivateTrackedIfExpectedActive(podUuid, Some(owner), 2L, attemptUuid).get
    val inactiveState = stopped match
      case DeactivationOutcome.Accepted(state) => state
      case other => throw new AssertionError("expected tracked deactivation, received " + other)
    assertEquals(3L, inactiveState.stateVersion)
    assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, stop.findPhysicalRecoveryLocator(podUuid).get.get.state)
    val stoppedSnapshot = freezeAndCheckDelta(idledSnapshot, stop)

    val verify = new MySqlRepositoryStaging(stoppedSnapshot, Thread.currentThread())
    val verified = verify.verifyInactiveAbsence(podUuid, 3L, attemptUuid).get
    assertEquals(PhysicalRecoveryLocatorState.RowInactiveVerified, verified.state)
    assertEquals(3L, verify.findByUuid(podUuid).get.get.stateVersion)
    val frozen = freezeAndCheckDelta(stoppedSnapshot, verify)
    frozen.validate()
    val locator = frozen.recovery.locators.head
    assertEquals(25, locator.chunks.size)
    assertEquals(PhysicalRecoveryLocatorState.RowInactiveVerified, locator.state)

  @Test
  def orderedStagingAcceptsInactiveInsertThenVersionZeroUpdateAsSeparateTransitions(): Unit =
    val initial = MySqlFullRepositorySnapshot.empty
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())
    val inserted = inactive()
    assertEquals(inserted, stage.insert(inserted).get)

    val updated = stage.update(inserted.copy(worldName = "world-after-insert")).get
    assertEquals(1L, updated.stateVersion)
    assertEquals("world-after-insert", updated.worldName)

    val plan = stage.closeAndFreezePlan().get
    assertEquals(2, plan.steps.size)
    assertTrue(plan.steps.head.before.exactlyEquals(initial))
    assertEquals(0L, plan.steps.head.after.pods.head.stateVersion)
    assertEquals(0L, plan.steps(1).before.pods.head.stateVersion)
    assertEquals(1L, plan.steps(1).after.pods.head.stateVersion)
    assertTrue(plan.finalSnapshot.exactlyEquals(plan.steps.last.after))
    val effect = MySqlBusinessMutation.ReplaceFullRepositorySnapshot(initial, plan.finalSnapshot, plan.steps)
    assertTrue(effect.desiredRepositorySnapshot.exists(_.exactlyEquals(plan.finalSnapshot)))

  @Test
  def orderedStagingAcceptsTwoSequentialVersionCasUpdates(): Unit =
    val initialRow = MySqlRepositoryTransitionRules.toRaw(inactive())
    val initial = MySqlFullRepositorySnapshot(
      Vector(initialRow),
      Vector.empty,
      MySqlRecoveryGraph(None, Vector.empty, Vector.empty, Vector.empty),
      Vector.empty
    )
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())
    val versionOne = stage.update(inactive().copy(stateVersion = 0L, worldName = "world-one")).get
    assertEquals(1L, versionOne.stateVersion)
    val versionTwo = stage.update(versionOne.copy(stateVersion = 1L, worldName = "world-two")).get
    assertEquals(2L, versionTwo.stateVersion)

    val plan = stage.closeAndFreezePlan().get
    assertEquals(2, plan.steps.size)
    assertEquals(0L, plan.steps(0).before.pods.head.stateVersion)
    assertEquals(1L, plan.steps(0).after.pods.head.stateVersion)
    assertEquals(1L, plan.steps(1).before.pods.head.stateVersion)
    assertEquals(2L, plan.steps(1).after.pods.head.stateVersion)
    assertEquals("world-two", plan.finalSnapshot.pods.head.worldName)

  @Test
  def swallowedFailureInLaterStagedStepPreventsFreezingAnyTransactionPlan(): Unit =
    val stage = new MySqlRepositoryStaging(MySqlFullRepositorySnapshot.empty, Thread.currentThread())
    val inserted = inactive()
    stage.insert(inserted).get
    stage.update(inserted.copy(worldName = "world-after-insert")).get

    val firstFailure = stage.update(inserted.copy(worldName = "stale-step")).failed.get
    assertSame(firstFailure, stage.update(inserted.copy(worldName = "another-stale-step")).failed.get)
    assertEquals(1L, stage.snapshot.pods.head.stateVersion)
    assertSame(firstFailure, stage.failure.get)
    assertSame(firstFailure, stage.closeAndFreezePlan().failed.get)

  @Test
  def orderedInsertThenDeleteRetainsCommitIdentityWhenTheFinalFullGraphEqualsTheInitialGraph(): Unit =
    val initial = certifiedEmptySnapshot()
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())
    val row = inactive()

    assertEquals(row, stage.insert(row).get)
    assertTrue(stage.delete(podUuid).get)

    val staged = stage.closeAndFreezePlan().get
    assertEquals(2, staged.steps.size)
    assertTrue(initial.exactlyEquals(staged.finalSnapshot))
    assertTrue(staged.steps.head.before.exactlyEquals(initial))
    assertEquals(1, staged.steps.head.after.pods.size)
    assertEquals(0, staged.steps(1).after.pods.size)
    assertNetZeroChainHasExactCommitEvidence(initial, staged)
    assertThrows(
      classOf[java.sql.SQLTransactionRollbackException],
      () => MySqlRepositoryStagedPlan(initial, initial.copy(pods = Vector(MySqlRepositoryTransitionRules.toRaw(row))), Vector.empty).validate()
    )

  @Test
  def orderedDeleteThenReinsertRetainsCommitIdentityWhenTheOriginalRowIsRestored(): Unit =
    val original = inactive()
    val initial = certifiedEmptySnapshot().copy(pods = Vector(MySqlRepositoryTransitionRules.toRaw(original)))
    initial.validate()
    val stage = new MySqlRepositoryStaging(initial, Thread.currentThread())

    assertTrue(stage.delete(podUuid).get)
    assertEquals(original, stage.insert(original).get)

    val staged = stage.closeAndFreezePlan().get
    assertEquals(2, staged.steps.size)
    assertTrue(initial.exactlyEquals(staged.finalSnapshot))
    assertEquals(0, staged.steps.head.after.pods.size)
    assertEquals(1, staged.steps(1).after.pods.size)
    assertNetZeroChainHasExactCommitEvidence(initial, staged)

  @Test
  def normalSnapshotDeltaCannotChangeBootstrapBindingOrIntentEvidence(): Unit =
    val intent = MySqlBootstrapIntentData(
      1,
      UUID.fromString("95000000-0000-0000-0000-000000000005"),
      UUID.fromString("96000000-0000-0000-0000-000000000006"),
      UUID.fromString("97000000-0000-0000-0000-000000000007"),
      bindingUuid,
      2,
      UUID.fromString("98000000-0000-0000-0000-000000000008"),
      "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=00000000000000000000000000000001",
      Vector.fill(32)(1.toByte),
      MySqlBootstrapIntentStage.Prepared,
      None
    )
    val legacy = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 2)
    val before = MySqlFullRepositorySnapshot(
      Vector.empty,
      Vector.empty,
      MySqlRecoveryGraph(Some(legacy), Vector.empty, Vector.empty, Vector.empty),
      Vector(intent)
    )
    before.validate()
    val after = before.copy(
      recovery = before.recovery.copy(
        binding = Some(PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2))
      ),
      bootstrapIntents = Vector(intent.copy(
        stage = MySqlBootstrapIntentStage.Complete,
        markerAckSha256 = Some(Vector.fill(32)(2.toByte))
      ))
    )
    after.validate()
    assertThrows(classOf[IllegalStateException], () => MySqlFullRepositoryDelta.freeze(before, after))

  @Test
  def unknownEffectReconciliationRequiresTheWholeDesiredGraph(): Unit =
    val before = MySqlFullRepositorySnapshot.empty
    val after = MySqlFullRepositorySnapshot(Vector(MySqlRepositoryTransitionRules.toRaw(inactive())), Vector.empty,
      before.recovery, before.bootstrapIntents)
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.ReplaceFullRepositorySnapshot(before, after)))

    MySqlBusinessUnknownProofRules.requireCompleteEffects(
      plan,
      MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty, Some(after))
    )
    org.junit.jupiter.api.Assertions.assertThrows(
      classOf[java.sql.SQLException],
      () => MySqlBusinessUnknownProofRules.requireCompleteEffects(
        plan,
        MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty, Some(before))
      )
    )

  private def inactive(): FloatingPodState =
    FloatingPodState(
      podUuid,
      None,
      0L,
      "world",
      8.0d,
      64.0d,
      8.0d,
      0.0f,
      0.0f,
      false,
      10L,
      Some(7),
      com.wi.floatingpod.domain.PodOperatingMode.Normal
    )

  private def certifiedEmptySnapshot(): MySqlFullRepositorySnapshot =
    val binding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)
    MySqlFullRepositorySnapshot(
      Vector.empty,
      Vector.empty,
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector.empty),
      Vector.empty
    )

  /** 最终图相等不能抹去已暂存的步骤与固定写入回执槽。 */
  private def assertNetZeroChainHasExactCommitEvidence(
      initial: MySqlFullRepositorySnapshot,
      staged: MySqlRepositoryStagedPlan
  ): Unit =
    assertTrue(initial.exactlyEquals(staged.finalSnapshot))
    assertTrue(staged.steps.nonEmpty)
    val mutation = MySqlBusinessMutation.ReplaceFullRepositorySnapshot(initial, staged.finalSnapshot, staged.steps)
    val effects = MySqlBusinessEffectPlan(Vector(mutation))
    val bootUuid = UUID.fromString("99000000-0000-0000-0000-000000000009")
    val operationUuid = UUID.fromString("9a000000-0000-0000-8000-00000000000a")
    val context = effects.operationContext(bootUuid, 5L, bindingUuid, operationUuid)
    assertEquals(operationUuid, context.operationUuid)
    assertEquals(1, context.expectedEffects.size)
    assertEquals(32, context.expectedEffects.head.desiredEffectFingerprintSha256.size)

    val receipt = MySqlCommitReceipt(
      MySqlCommitReceiptSlot(bootUuid, 5L, bindingUuid, operationUuid),
      context.canonicalFingerprintSha256
    )
    assertEquals(MySqlCommitReceiptVerification.Confirmed(receipt), MySqlCommitReceipts.verifyReadback(context, Some(receipt)))
    val otherOperation = effects.operationContext(
      bootUuid,
      5L,
      bindingUuid,
      UUID.fromString("9b000000-0000-0000-8000-00000000000b")
    )
    assertEquals(MySqlCommitReceiptVerification.Unproven, MySqlCommitReceipts.verifyReadback(otherOperation, Some(receipt)))

    MySqlBusinessUnknownProofRules.requireCompleteEffects(
      effects,
      MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty, Some(staged.finalSnapshot))
    )
    assertThrows(
      classOf[java.sql.SQLException],
      () => MySqlBusinessUnknownProofRules.requireCompleteEffects(
        effects,
        MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty)
      )
    )

  private def freezeAndCheckDelta(before: MySqlFullRepositorySnapshot, stage: MySqlRepositoryStaging): MySqlFullRepositorySnapshot =
    val after = stage.closeAndFreeze().get
    after.validate()
    MySqlFullRepositoryDelta.freeze(before, after)
    after

  private def trackedActiveSnapshot(): MySqlFullRepositorySnapshot =
    val binding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)
    val row = MySqlRepositoryTransitionRules.toRaw(inactive().copy(
      isActive = true,
      operatingMode = com.wi.floatingpod.domain.PodOperatingMode.WorldIdle
    ))
    val chunks = (for x <- -2 to 2; z <- -2 to 2 yield PhysicalRecoveryChunk(7, x, z)).toVector
    val locator = PhysicalRecoveryLocator(
      podUuid,
      attemptUuid,
      PhysicalRecoveryLocatorState.RowActive,
      PhysicalRecoveryLocatorReason.RowActive,
      Some(0L),
      Some(7),
      chunks.toList
    )
    val graph = MySqlRecoveryRows.assembleGraph(
      Some(binding),
      Vector(locator),
      chunks.map(chunk => MySqlRecoveryChunkRow(podUuid, chunk)),
      Vector.empty
    )
    val snapshot = MySqlFullRepositorySnapshot(Vector(row), Vector.empty, graph, Vector.empty)
    snapshot.validate()
    snapshot
