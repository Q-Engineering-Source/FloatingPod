package com.wi.floatingpod.domain

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

import java.util.UUID

/** 验证公开领域状态不会表示缺失维度的活动实体。 */
class FloatingPodStateTest:
  @Test
  def rejectsActiveStateWithoutDimension(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => state(active = true, dimensionId = None))

  @Test
  def acceptsDimensionedActiveAndInactiveStates(): Unit =
    assertDoesNotThrow(() => state(active = true, dimensionId = Some(-1)))
    assertDoesNotThrow(() => state(active = false, dimensionId = None))

  private def state(active: Boolean, dimensionId: Option[Int]): FloatingPodState =
    FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000401"), None, 0L, "overworld", 0D, 64D, 0D, 0F, 0F, active, 1L, dimensionId)
