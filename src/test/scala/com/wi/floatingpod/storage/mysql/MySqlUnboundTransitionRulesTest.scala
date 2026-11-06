package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodPose
import com.wi.floatingpod.storage.{
  ObservedUnboundIdentity,
  ObservedUnboundPod,
  ObservedUnboundSourceKind,
  ObservedUnboundUniquenessReceipt,
  PhysicalRecoveryBinding,
  PhysicalRecoveryCutoverState,
  UnboundCommitOutcome,
  UnboundCommitRejection,
  UnboundIntentOrigin
}

import java.util.UUID
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

final class MySqlUnboundTransitionRulesTest:
  private val binding = PhysicalRecoveryBinding(
    UUID.fromString("22000000-0000-0000-0000-000000000002"),
    PhysicalRecoveryCutoverState.Certified,
    2
  )
  private val pod = UUID.fromString("11000000-0000-0000-0000-000000000001")
  private val attempt = UUID.fromString("33000000-0000-0000-0000-000000000003")
  private val generation = UUID.fromString("44000000-0000-0000-0000-000000000004")
  private val operation = UUID.fromString("55000000-0000-0000-0000-000000000005")
  private val pose = FloatingPodPose("world", 8.0d, 64.0d, 8.0d, 0.0f, 0.0f, 7)

  @Test
  def observedPrepareCreatesOnlyInactiveWorldIdleIntentAndExactLocatorAndReplaysOldAttempt(): Unit =
    val observed = ObservedUnboundPod(
      pod,
      ObservedUnboundIdentity(ObservedUnboundSourceKind.LegacyUnregistered, generation, "entity-source-44"),
      pose
    )
    val uniqueness = ObservedUnboundUniquenessReceipt(binding.bindingUuid, observed)
    val (prepared, receipt) = MySqlUnboundTransitionRules.prepareObserved(empty, observed, binding, attempt, uniqueness)
    prepared.validate()
    assertEquals(attempt, receipt.attemptUuid)
    assertEquals(UnboundIntentOrigin.ObservedArtifactAdoption, receipt.origin)
    assertEquals(0, prepared.pods.head.isActiveFlag)
    assertEquals(None, prepared.pods.head.ownerUuidBytes)
    assertEquals("WORLD_IDLE", prepared.pods.head.operatingModeToken)
    assertEquals(0, prepared.recovery.locators.head.chunks.size - 25)
    assertEquals(MySqlRecoveryIntentPhase.Prepared, prepared.recovery.intents.head.phase)

    val newCallerAttempt = UUID.fromString("66000000-0000-0000-0000-000000000006")
    val (replayed, oldReceipt) = MySqlUnboundTransitionRules.prepareObserved(
      prepared,
      observed,
      binding,
      newCallerAttempt,
      uniqueness
    )
    assertTrue(replayed.exactlyEquals(prepared))
    assertEquals(attempt, oldReceipt.attemptUuid)

  @Test
  def wrongPoseAndCapacityRejectWithoutChangingPreparedSnapshot(): Unit =
    val observed = observation()
    val prepared = MySqlUnboundTransitionRules.prepareObserved(
      empty,
      observed,
      binding,
      attempt,
      ObservedUnboundUniquenessReceipt(binding.bindingUuid, observed)
    )._1
    val wrongPose = pose.copy(posX = pose.posX + 1.0d)
    val poseResult = MySqlUnboundTransitionRules.commitObserved(
      prepared,
      binding,
      pod,
      attempt,
      0L,
      wrongPose,
      256
    )
    assertEquals(UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch), poseResult._2)
    assertTrue(poseResult._1.exactlyEquals(prepared))

    val otherActive = MySqlRepositoryTransitionRules.toRaw(
      com.wi.floatingpod.domain.FloatingPodState(
        UUID.fromString("77000000-0000-0000-0000-000000000007"),
        None,
        1L,
        "world",
        100.0d,
        64.0d,
        100.0d,
        0.0f,
        0.0f,
        true,
        1L,
        Some(7),
        com.wi.floatingpod.domain.PodOperatingMode.WorldIdle
      )
    )
    val full = prepared.copy(pods = prepared.pods :+ otherActive)
    full.validate()
    val capacity = MySqlUnboundTransitionRules.commitObserved(full, binding, pod, attempt, 0L, pose, 1)
    assertEquals(UnboundCommitOutcome.Rejected(UnboundCommitRejection.CapacityReached(1, 1)), capacity._2)
    assertTrue(capacity._1.exactlyEquals(full))

  @Test
  def observedCommitUpdatesDomainLocatorAndIntentTogetherAndExactReplayDoesNotIncrementAgain(): Unit =
    val observed = observation()
    val prepared = MySqlUnboundTransitionRules.prepareObserved(
      empty,
      observed,
      binding,
      attempt,
      ObservedUnboundUniquenessReceipt(binding.bindingUuid, observed)
    )._1
    val (committed, first) = MySqlUnboundTransitionRules.commitObserved(prepared, binding, pod, attempt, 0L, pose, 256)
    committed.validate()
    val state = first match
      case UnboundCommitOutcome.Committed(value) => value
      case other => throw new AssertionError("expected first commit, received " + other)
    assertEquals(1L, state.stateVersion)
    assertEquals(true, state.isActive)
    assertEquals(None, state.ownerUuid)
    assertEquals(com.wi.floatingpod.domain.PodOperatingMode.WorldIdle, state.operatingMode)
    assertEquals(Some(1L), committed.recovery.locators.head.domainVersion)
    assertEquals(MySqlRecoveryIntentPhase.Committed, committed.recovery.intents.head.phase)

    val (replayed, outcome) = MySqlUnboundTransitionRules.commitObserved(committed, binding, pod, attempt, 0L, pose, 256)
    assertTrue(replayed.exactlyEquals(committed))
    assertEquals(UnboundCommitOutcome.AlreadyCommitted(state), outcome)

  @Test
  def controlledCreateHasDistinctOriginAndOperationReplayCannotReplaceHistory(): Unit =
    val (prepared, receipt) = MySqlUnboundTransitionRules.prepareCreate(empty, binding, pod, attempt, pose, operation)
    prepared.validate()
    assertEquals(UnboundIntentOrigin.DebugItemCreate, receipt.origin)
    assertNotEquals(UnboundIntentOrigin.ObservedArtifactAdoption, receipt.origin)
    assertEquals(MySqlRecoveryIntentPhase.Prepared, prepared.recovery.intents.head.phase)

    val sameOperationDifferentAttempt = UUID.fromString("88000000-0000-0000-0000-000000000008")
    val (replayed, oldReceipt) = MySqlUnboundTransitionRules.prepareCreate(
      prepared, binding, pod, sameOperationDifferentAttempt, pose, operation
    )
    assertTrue(replayed.exactlyEquals(prepared))
    assertEquals(attempt, oldReceipt.attemptUuid)
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlUnboundTransitionRules.prepareCreate(
        prepared, binding, pod, sameOperationDifferentAttempt, pose.copy(posX = 9.0d), operation
      )
    )

  private def observation(): ObservedUnboundPod =
    ObservedUnboundPod(
      pod,
      ObservedUnboundIdentity(ObservedUnboundSourceKind.LegacyUnregistered, generation, "entity-source-44"),
      pose
    )

  private def empty: MySqlFullRepositorySnapshot =
    MySqlFullRepositorySnapshot(
      Vector.empty,
      Vector.empty,
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector.empty),
      Vector.empty
    )
