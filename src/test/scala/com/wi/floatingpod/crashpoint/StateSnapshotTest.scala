package com.wi.floatingpod.crashpoint

import com.wi.floatingpod.server.{ActiveFloatingPodManager, ChunkLoadingManager}

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

/** 直接验证快照的重复记录、未知实体和所有权冲突判定。 */
class StateSnapshotTest:
  private val harness = CrashPointHarness.getInstance

  @AfterEach
  def resetCrashHarness(): Unit =
    harness.disable()
    harness.reset()

  @Test
  def typedCapturePreservesDuplicatePersistedRowsAndUnknownEntities(): Unit =
    val uuid = CrashPointTestSupport.SpawnUuid
    val repository = new CrashPointTestSupport.MemoryRepository(CrashPointTestSupport.state(uuid, active = false))
    repository.duplicateRows += CrashPointTestSupport.state(uuid, active = false)
    val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, new CrashPointTestSupport.TestGateway))

    val snapshot = StateSnapshot.capture(manager, repository, List(701, 701, 702)).get
    assertEquals(List(uuid, uuid), snapshot.persistedPods)
    assertEquals(Set(uuid), snapshot.duplicatePersistedUuids)
    assertEquals(Set(701), snapshot.duplicateEntityIds)
    assertEquals(Set(701, 702), snapshot.unknownEntityIds)
    assertTrue(snapshot.orphanEntityIds.isEmpty)

  @Test
  def diffSeparatesOwnerMutationFromDuplicateOwnership(): Unit =
    val owner = CrashPointTestSupport.SpawnUuid
    val first = CrashPointTestSupport.RecallUuid
    val second = CrashPointTestSupport.BeforeEntityUuid
    val before = StateSnapshot(
      activePods = List(first, second),
      persistedPods = List(first, second),
      activeInDb = List(first, second),
      entityIds = List(1, 2),
      entityPodMappings = List(1 -> Some(first), 2 -> Some(second)),
      ownershipMappings = List(first -> Some(owner), second -> Some(owner)),
      persistedStates = List(CrashPointTestSupport.state(first, active = true), CrashPointTestSupport.state(second, active = true))
    )
    val after = before.copy(ownershipMappings = List(first -> None, second -> Some(owner)))

    val diff = StateSnapshot.diff(before, after)
    assertEquals(Set(first), diff.ownerConflicts)
    assertTrue(diff.duplicateOwnership.isEmpty)
    assertTrue(diff.duplicatePersistedUuids.isEmpty)
    assertFalse(diff.newActivePods.contains(first))

  @Test
  def diffFlagsDuplicateOwnersForNewPodWithoutCrossSnapshotMutation(): Unit =
    val podUuid = CrashPointTestSupport.AfterEntityUuid
    val firstOwner = CrashPointTestSupport.SpawnUuid
    val secondOwner = CrashPointTestSupport.RecallUuid
    val before = emptySnapshot
    val after = emptySnapshot.copy(
      persistedPods = List(podUuid, podUuid),
      ownershipMappings = List(podUuid -> Some(firstOwner), podUuid -> Some(secondOwner))
    )

    val diff = StateSnapshot.diff(before, after)
    assertEquals(Set(podUuid), diff.duplicateOwnership)
    assertTrue(diff.ownerConflicts.isEmpty)

  @Test
  def diffDetectsReversedDuplicateRowsForExistingPod(): Unit =
    val podUuid = CrashPointTestSupport.BeforeEntityUuid
    val firstOwner = CrashPointTestSupport.SpawnUuid
    val secondOwner = CrashPointTestSupport.RecallUuid
    val before = emptySnapshot.copy(
      persistedPods = List(podUuid),
      ownershipMappings = List(podUuid -> Some(firstOwner))
    )
    val after = before.copy(ownershipMappings = List(podUuid -> Some(secondOwner), podUuid -> Some(firstOwner)))

    val diff = StateSnapshot.diff(before, after)
    assertEquals(Set(podUuid), diff.duplicateOwnership)
    assertEquals(Set(podUuid), diff.ownerConflicts)

  @Test
  def diffAllowsDifferentPodsToShareOneOwner(): Unit =
    val owner = CrashPointTestSupport.SpawnUuid
    val firstPod = CrashPointTestSupport.RecallUuid
    val secondPod = CrashPointTestSupport.BeforeEntityUuid
    val snapshot = emptySnapshot.copy(
      persistedPods = List(firstPod, secondPod),
      ownershipMappings = List(firstPod -> Some(owner), secondPod -> Some(owner))
    )

    val diff = StateSnapshot.diff(emptySnapshot, snapshot)
    assertTrue(diff.duplicateOwnership.isEmpty)
    assertTrue(diff.ownerConflicts.isEmpty)

  private def emptySnapshot: StateSnapshot =
    StateSnapshot(
      activePods = Nil,
      persistedPods = Nil,
      activeInDb = Nil,
      entityIds = Nil,
      entityPodMappings = Nil,
      ownershipMappings = Nil,
      persistedStates = Nil
    )
