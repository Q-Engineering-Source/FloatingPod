package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PodOperatingMode
import com.wi.floatingpod.storage.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** actual pure owned CAS/readback规则，不声称SQL/Main mode publication。 */
class MySqlCommittedOwnedModeGraphTest:
  import MySqlPrepareModeGraphFixture.*

  @Test def actualChangedGraphKeepsRequestOwnerVersionLocatorAndWholeInventory(): Unit =
    val data = mode(); val graph = data.graph
    assertTrue(data.outcome.isInstanceOf[OperatingModeChangeOutcome.Changed])
    assertSame(data.request, graph.request)
    assertSame(data.outcome, graph.actualCommit)
    assertEquals(data.request.expectedStateVersion + 1L, graph.state.stateVersion)
    assertEquals(Some(owner), graph.state.ownerUuid)
    assertEquals(PodOperatingMode.WorldIdle, graph.state.operatingMode)
    assertEquals(Some(graph.state.stateVersion), graph.locator.domainVersion)
    assertEquals(2, graph.legacyInventory.get.pods.size)
    assertTrue(graph.fullTwentyRelationSnapshot.isEmpty)

  @Test def alreadySetSupportsActualSameVersionAndOneStepReplay(): Unit =
    val same = mode(PodOperatingMode.Normal)
    assertTrue(same.outcome.isInstanceOf[OperatingModeChangeOutcome.AlreadySet])
    assertEquals(same.request.expectedStateVersion, same.graph.state.stateVersion)
    val changed = mode(); val r = changed.request
    val (snapshot, result) = MySqlRepositoryTransitionRules.setMode(changed.snapshot, binding, r.podUuid,
      r.authenticatedOwner, r.expectedStateVersion, r.attemptUuid, r.targetMode, r.actualPose)
    assertTrue(result.isInstanceOf[OperatingModeChangeOutcome.AlreadySet])
    assertEquals(r.expectedStateVersion + 1L, changed.copy(snapshot = snapshot, outcome = result).graph.state.stateVersion)

  @Test def rejectedOwnerAndVersionRemainRejectedNotCommittedProof(): Unit =
    val (snapshot, state, attempt) = activeOwned
    val (same, rejected) = MySqlRepositoryTransitionRules.setMode(snapshot, binding, state.uuid,
      id(999), state.stateVersion, attempt, PodOperatingMode.WorldIdle, pose)
    assertEquals(OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.NotOwner), rejected)
    val request = MySqlOwnedModeCommitRequest(binding, state.uuid, id(999), state.stateVersion, attempt, PodOperatingMode.WorldIdle, pose)
    assertThrows(classOf[IllegalArgumentException], () => MySqlCommittedOwnedModeGraph.fromReadback(tag, request,
      rejected, inventory(same), admission, 256))
    val data = mode()
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(expectedStateVersion = 0L)).graph)

  @Test def wrongOwnerModeFinalPoseAndAttemptCannotIssueGraph(): Unit =
    val data = mode()
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(authenticatedOwner = id(999))).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(targetMode = PodOperatingMode.Normal)).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(actualPose = pose.copy(posX = 9D))).graph)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(request = data.request.copy(attemptUuid = id(999))).graph)

  @Test def staleAdmissionAndDuplicateUnrelatedHistoryFailFullGraphValidation(): Unit =
    val data = mode(); val raw = inventory(data.snapshot)
    assertThrows(classOf[IllegalArgumentException], () => MySqlCommittedOwnedModeGraph.fromReadback(tag, data.request,
      data.outcome, raw, admission.copy(writerEpoch = 5L), 256))
    val duplicate = data.snapshot.copy(pods = data.snapshot.pods ++ data.snapshot.pods.filter(_.uuidBytes == MySqlStoredRows.encodeUuid(id(99))))
    assertThrows(classOf[IllegalStateException], () => data.copy(snapshot = duplicate).graph)

  @Test def mismatchAckAndReadbackCannotIssueGraph(): Unit =
    val data = mode()
    val wrong = data.state.copy(lastUpdated = data.state.lastUpdated + 1L)
    assertThrows(classOf[IllegalArgumentException], () => data.copy(outcome = OperatingModeChangeOutcome.Changed(wrong)).graph)
    val graph = data.snapshot.recovery
    val changedLocator = graph.copy(locators = graph.locators.map(value => value.copy(attemptUuid = id(777))))
    assertThrows(classOf[IllegalArgumentException], () => data.copy(snapshot = data.snapshot.copy(recovery = changedLocator)).graph)
