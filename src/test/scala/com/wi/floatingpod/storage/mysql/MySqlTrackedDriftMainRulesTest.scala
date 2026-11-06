package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

final class MySqlTrackedDriftMainRulesTest:
  private val binding = PhysicalRecoveryBinding(UUID.randomUUID(), PhysicalRecoveryCutoverState.Certified, 2)
  private val attempt = UUID.randomUUID()
  private val base = FloatingPodState(UUID.randomUUID(), None, 4L, "fixture", 1D, 64D, 1D,
    0F, 0F, true, 100L, Some(0), PodOperatingMode.WorldIdle)
  private val desired = base.copy(posX = 2D, yaw = 12F)
  private val request = MySqlTrackedDriftRequest(binding, base, desired, attempt)
  private val durable = desired.copy(stateVersion = 5L, lastUpdated = 200L)
  @Test def laterPoseRebasesOnlyWhenExactCriticalBaseStillMatches(): Unit =
    val latest = desired.copy(posX = 3D, yaw = 18F)
    val result = MySqlTrackedDriftMainRules.acknowledge(base, latest, request, durable, 1L, 2L)
    assertEquals(durable, result.durableBase)
    assertEquals(latest.copy(stateVersion = 5L, lastUpdated = 200L), result.latestCache)
    assertTrue(result.keepDirty)
  @Test def sameBodyCannotExcuseVersionOrLastUpdatedAdvancement(): Unit =
    for advanced <- Vector(base.copy(stateVersion = 5L), base.copy(lastUpdated = 101L)) do
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlTrackedDriftMainRules.acknowledge(advanced, desired, request, durable, 1L, 2L))
  @Test def exactRevisionClearsOnlyItsCapturedDesired(): Unit =
    val result = MySqlTrackedDriftMainRules.acknowledge(base, desired, request, durable, 1L, 1L)
    assertFalse(result.keepDirty)
    assertEquals(durable, result.latestCache)
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlTrackedDriftMainRules.acknowledge(base, desired.copy(posX = 3D), request, durable, 1L, 1L))
  @Test def wrongAckVersionCriticalIdentityAndCrossChunkAreRejected(): Unit =
    for bad <- Vector(durable.copy(stateVersion = 6L), durable.copy(ownerUuid = Some(UUID.randomUUID())),
        durable.copy(posX = 3D)) do
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlTrackedDriftMainRules.acknowledge(base, desired, request, bad, 1L, 2L))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlTrackedDriftRequest(binding, base, desired.copy(posX = 16D), attempt))

  @Test def fullCriticalBaseAndRequestedPoseRetainSignedZeroBits(): Unit =
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlTrackedDriftMainRules.acknowledge(base.copy(yaw = -0.0F), desired, request, durable, 1L, 2L))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlTrackedDriftMainRules.acknowledge(base, desired, request, durable.copy(pitch = -0.0F), 1L, 2L))
