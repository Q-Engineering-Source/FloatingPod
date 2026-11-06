package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.entity.FloatingPodControlSnapshot
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 实际entity consumer共用的检查/构造逻辑；无游戏/SQL/Ready或实际body apply验收。 */
final class TrackedDriftEntityControlTest:
  private val pod = new UUID(0L, 1L)
  private val attempt = new UUID(0L, 2L)
  private val binding = new UUID(0L, 3L)
  private val operation = new UUID(0L, 4L)
  private val base = FloatingPodState(pod, None, 7L, "fixture", 2D, 64D, 2D, 0F, 0F,
    true, 100L, Some(0), PodOperatingMode.WorldIdle)
  private val desired = base.copy(posX = 3D, yaw = 4F, stateVersion = 8L, lastUpdated = 200L)
  private def pose(state: FloatingPodState): FloatingPodPose =
    FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch, state.dimensionId.get)

  @Test def realControlledCreationStampSurvivesDriftSnapshotConstruction(): Unit =
    val original = FloatingPodEntityControl.committed(base, Some(binding), Some(attempt), Some(operation))
    val result = FloatingPodEntityControl.trackedDrift(base, desired, attempt, original, pose(desired))
    assertEquals(binding, result.bindingUuid())
    assertEquals(operation, result.operationUuid())
    assertEquals(attempt, result.attemptUuid())
    assertEquals(8L, result.stateVersion())
    assertEquals(3D, result.posX())
    assertEquals(7L, original.stateVersion())
    assertEquals(2D, original.posX())
    assertEquals(FloatingPodControlSnapshot.Phase.COMMITTED, result.phase())

  @Test def dbRecoveryBindingAndCreationStampSurviveModeAndDriftSuccessors(): Unit =
    val recovered = FloatingPodEntityControl.committed(base, Some(binding), Some(attempt), None)
    val owner = new UUID(0L, 18L)
    val changedMode = base.copy(ownerUuid = Some(owner), operatingMode = PodOperatingMode.Normal,
      stateVersion = 8L, lastUpdated = 200L)
    val mode = FloatingPodEntityControl.committedMode(changedMode, attempt, recovered)
    assertEquals(binding, mode.bindingUuid())
    assertEquals(attempt, mode.attemptUuid())
    assertNull(mode.operationUuid())
    val drift = FloatingPodEntityControl.trackedDrift(base, desired, attempt, recovered, pose(desired))
    assertEquals(binding, drift.bindingUuid())
    assertEquals(attempt, drift.attemptUuid())
    assertNull(drift.operationUuid())

    val created = FloatingPodEntityControl.committed(base, Some(binding), Some(attempt), Some(operation))
    val createdMode = FloatingPodEntityControl.committedMode(changedMode, attempt, created)
    assertEquals(binding, createdMode.bindingUuid())
    assertEquals(operation, createdMode.operationUuid())
    assertThrows(classOf[IllegalArgumentException], () =>
      FloatingPodEntityControl.committedMode(changedMode, new UUID(0L, 19L), recovered))
    val partial = FloatingPodEntityControl.committed(base, None, Some(attempt), Some(operation))
    assertThrows(classOf[IllegalArgumentException], () =>
      FloatingPodEntityControl.trackedDrift(base, desired, attempt, partial, pose(desired)))

  @Test def criticalVersionOwnerModeAndAttemptConflictsRejectBeforeSnapshotApplication(): Unit =
    val original = FloatingPodEntityControl.committed(base, Some(binding), Some(attempt), Some(operation))
    for invalid <- Vector(base.copy(stateVersion = 6L), base.copy(ownerUuid = Some(new UUID(0L, 10L))),
        base.copy(operatingMode = PodOperatingMode.Normal, ownerUuid = Some(new UUID(0L, 11L)))) do
      assertThrows(classOf[IllegalArgumentException], () =>
        FloatingPodEntityControl.trackedDrift(invalid, desired, attempt, original, pose(desired)))
    assertThrows(classOf[IllegalArgumentException], () =>
      FloatingPodEntityControl.trackedDrift(base, desired, new UUID(0L, 12L), original, pose(desired)))
    assertEquals(7L, original.stateVersion())
    assertEquals(operation, original.operationUuid())
    assertEquals(2D, original.posX())

  @Test def actualPoseAndRawSignedZeroCannotBeSubstitutedByDesiredDto(): Unit =
    val original = FloatingPodEntityControl.committed(base, Some(binding), Some(attempt), Some(operation))
    assertThrows(classOf[IllegalArgumentException], () =>
      FloatingPodEntityControl.trackedDrift(base, desired, attempt, original, pose(desired).copy(posX = 4D)))
    assertThrows(classOf[IllegalArgumentException], () =>
      FloatingPodEntityControl.trackedDrift(base, desired, attempt, original, pose(desired).copy(pitch = -0.0F)))
    assertEquals(7L, original.stateVersion())
