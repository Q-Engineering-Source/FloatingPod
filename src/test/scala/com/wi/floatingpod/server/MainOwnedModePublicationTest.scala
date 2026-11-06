package com.wi.floatingpod.server

import com.wi.floatingpod.debug.DebugItemEndpoint
import com.wi.floatingpod.domain.{FloatingPodState, FloatingPodPose}
import com.wi.floatingpod.storage.*
import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoCompletion, IoSubmission}
import com.wi.floatingpod.config.SourceLimitProfile
import java.util.UUID
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.collection.mutable
import scala.util.{Success, Failure, Try}

/** real manager/graph rules，game ports为明确stand-in；setup旧恢复只建立测试已有mapping。
  * 进入新mode seam后repository所有方法均禁止，不声明SQL/Forge/Ready/GPU。
  */
class MainOwnedModePublicationTest:
  @Test def residentCheckpointCannotUseCensusWithoutOriginalReadyRuntime(): Unit =
    val base = MySqlPrepareModeGraphFixture.activeOwned._2.copy(
      operatingMode = com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
    val f = new Fixture(Some(base))
    val request = UUID.randomUUID()
    f.scope.sealForNormalStop(request)
    val census = f.scope.captureNormalStopCensus(request).get
    val original = assertThrows(classOf[IllegalStateException], () => f.scope.beginWorldResidentCheckpoints(census))
    assertTrue(original.getMessage.contains("tracked drift is not installed"))
    assertSame(original, assertThrows(classOf[IllegalStateException], () => f.scope.beginWorldResidentCheckpoints(census)))
    assertTrue(f.scope.retainedResidentUuids.isEmpty)
    assertTrue(f.chunks.releaseResidentForNormalStop(null).isFailure)
    assertEquals(0, f.world.removed)
    assertEquals(0, f.gateway.released)
    assertEquals(0, f.repository.newPathCalls)
    assertSame(f.body, f.manager.managedActiveDetails(base.uuid).get.entity)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(base.uuid))

  @Test def schema6StopCannotBorrowLegacyCensusOrInventCompletedNormalRecall():Unit =
    val original=MySqlPrepareModeGraphFixture.activeOwned._2
    for state<-Vector(original.copy(operatingMode=com.wi.floatingpod.domain.PodOperatingMode.WorldIdle),
        original.copy(ownerUuid=None,operatingMode=com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)) do
      val f=new Fixture(Some(state))
      val request=UUID.randomUUID()
      f.scope.sealForNormalStop(request)
      val census=f.scope.captureNormalStopCensus(request).get
      assertThrows(classOf[IllegalArgumentException],()=>f.scope.beginDbFirstNormalStopCensus(null,census))
      assertTrue(f.scope.dbFirstNormalRecallCompleted.isEmpty)
      assertTrue(f.scope.retainedResidentUuids.isEmpty)
      assertSame(f.body,f.manager.managedActiveDetails(state.uuid).get.entity)
      assertSame(f.ticket,f.chunks.getAllManagedTickets()(state.uuid))
      assertEquals(0,f.gateway.released)
      assertEquals(0,f.world.removed)
      assertEquals(0,f.repository.newPathCalls)

  @Test def schema6FinalSourceCannotBorrowLegacyPublicationOrReleaseUnconfirmedResident():Unit =
    val state=MySqlPrepareModeGraphFixture.activeOwned._2.copy(
      operatingMode=com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)
    val f=new Fixture(Some(state))
    val request=UUID.randomUUID()
    f.scope.sealForNormalStop(request)
    f.scope.captureNormalStopCensus(request).get
    assertThrows(classOf[IllegalArgumentException],()=>f.manager.requireDbFirstPublication(null,f.scope))
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.prepareDbFirstFinalCohort())
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.mintDbFirstFinalCohort())
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.requireFinalSourceOwner(f.scope,null))
    assertSame(f.body,f.manager.managedActiveDetails(state.uuid).get.entity)
    assertSame(f.ticket,f.chunks.getAllManagedTickets()(state.uuid))
    assertEquals(0,f.gateway.released)
    assertEquals(0,f.world.removed)
    assertEquals(0,f.repository.newPathCalls)

  @Test def foreignCensusCannotStartAnotherManagersResidentCheckpoint(): Unit =
    val f = new Fixture
    val foreign = new Fixture
    val request = UUID.randomUUID()
    f.scope.sealForNormalStop(request)
    foreign.scope.sealForNormalStop(request)
    val other = foreign.scope.captureNormalStopCensus(request).get
    assertThrows(classOf[IllegalArgumentException], () => f.scope.beginWorldResidentCheckpoints(other))
    assertTrue(f.scope.retainedResidentUuids.isEmpty)
    assertEquals(0, f.gateway.released)

  @Test def normalStopCapturesThreeCategoriesWithoutDeletingOrReleasing(): Unit =
    val original = MySqlPrepareModeGraphFixture.activeOwned._2
    for state <- Vector(original.copy(operatingMode = com.wi.floatingpod.domain.PodOperatingMode.Normal),
        original.copy(operatingMode = com.wi.floatingpod.domain.PodOperatingMode.WorldIdle),
        original.copy(ownerUuid = None, operatingMode = com.wi.floatingpod.domain.PodOperatingMode.WorldIdle)) do
      val f = new Fixture(Some(state))
      val request = UUID.randomUUID()
      f.scope.sealForNormalStop(request)
      val census = f.scope.captureNormalStopCensus(request).get
      assertSame(census, f.scope.captureNormalStopCensus(request).get)
      assertEquals(1, census.entries.size)
      val entry = census.entries.head
      assertSame(f.body, entry.body)
      assertSame(f.ticket, entry.ticket)
      assertEquals(state.ownerUuid, entry.record.exactState.ownerUuid)
      assertEquals(f.body.actualPose, entry.finalPose)
      assertEquals(state.operatingMode == com.wi.floatingpod.domain.PodOperatingMode.WorldIdle, entry.retainedResident)
      assertTrue(f.body.isFrozen)
      assertEquals(0, f.gateway.released)
      assertEquals(0, f.world.removed)
      assertEquals(0, f.repository.newPathCalls)
      assertTrue(f.manager.requestPeriodicFlush().isFailure)
      assertThrows(classOf[IllegalArgumentException], () => f.scope.sealForNormalStop(UUID.randomUUID()))

  @Test def stopCaptureWaitsForOriginalModeProjectionReleaseAndPreservesFreeze(): Unit =
    val f = new Fixture
    f.retain()
    val request = UUID.randomUUID()
    f.scope.sealForNormalStop(request)
    assertTrue(f.scope.captureNormalStopCensus(request).isEmpty)
    assertEquals(MainControlledPublicationStatus.Published, f.scope.consumeMode(f.success(), 1L).status)
    assertTrue(f.body.isFrozen)
    assertTrue(f.scope.captureNormalStopCensus(request).isEmpty, "method return/publication is not memory release ACK")
    completeMemoryReleaseAck(f.scope, f.graph.tag)
    val entry = f.scope.captureNormalStopCensus(request).get.entries.head
    assertEquals(f.graph.state, entry.record.exactState)
    assertEquals(0, f.repository.newPathCalls)

  @Test def stopFreezeFailureAndPostCaptureDriftRetainOriginalFailure(): Unit =
    val f = new Fixture
    val request = UUID.randomUUID()
    val original = new IllegalStateException("freeze mirror failed after state retained")
    f.body.freezeFailure = original
    assertSame(original, assertThrows(classOf[IllegalStateException], () => f.scope.sealForNormalStop(request)))
    f.body.freezeFailure = null
    assertSame(original, assertThrows(classOf[IllegalStateException], () => f.scope.captureNormalStopCensus(request)))
    assertEquals(1, f.body.freezeCalls)
    assertEquals(0, f.world.removed)
    assertEquals(0, f.gateway.released)
    val changed = new Fixture
    changed.scope.sealForNormalStop(request)
    changed.scope.captureNormalStopCensus(request).get
    changed.body.state = changed.body.state.copy(posX = changed.body.state.posX + 1D)
    val failure = assertThrows(classOf[IllegalArgumentException], () => changed.scope.captureNormalStopCensus(request))
    assertSame(failure, assertThrows(classOf[IllegalArgumentException], () => changed.scope.captureNormalStopCensus(request)))

  @Test def stopCaptureRejectsLostTicketAndOffMainAndRetiredScope(): Unit =
    val f = new Fixture
    val request = UUID.randomUUID()
    f.scope.sealForNormalStop(request)
    val offMain = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val worker = new Thread(() => { try f.scope.captureNormalStopCensus(request) catch case cause: Throwable => offMain.set(cause); () })
    worker.start(); worker.join(3000L)
    assertFalse(worker.isAlive)
    assertInstanceOf(classOf[IllegalStateException], offMain.get())
    f.gateway.chunks.remove((0, 0))
    assertThrows(classOf[IllegalArgumentException], () => f.scope.captureNormalStopCensus(request))
    val retired = new Fixture
    retired.scope.retire()
    assertThrows(classOf[IllegalArgumentException], () => retired.scope.sealForNormalStop(request))

  @Test def actualModeGraphPublishesOnlyAfterExactFrozenBodyAndTicket(): Unit =
    val f = new Fixture
    f.retain()
    assertTrue(f.body.frozen)
    assertEquals(f.initial.operatingMode, f.body.state.operatingMode, "temporary freeze changed committed mode")
    val completion = f.success()
    val result = f.scope.consumeMode(completion, 1L)
    assertEquals(MainControlledPublicationStatus.Published, result.status)
    assertEquals(f.graph.state, f.manager.managedActiveDetails(f.initial.uuid).get.exactState)
    assertEquals(f.graph.state, f.body.state)
    assertEquals(1, f.body.finished)
    assertTrue(result.completionConsumed)
    assertEquals(0, f.repository.newPathCalls)
    assertEquals(0, f.world.removed)
    assertEquals(0, f.gateway.released)
    completeMemoryReleaseAck(f.scope, f.graph.tag)
    assertEquals(f.graph.state.operatingMode, f.scope.inspect(f.initial.uuid).get.operatingMode)

  @Test def deadBodyUuidStillIndexedRejectsAndKeepsOriginalTicketCompletion(): Unit =
    val f = new Fixture
    f.retain(); f.world.dead = true
    val result = f.scope.consumeMode(f.success(), 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertTrue(result.completionConsumed)
    assertTrue(f.body.frozen)
    assertEquals(0, f.body.finished)
    assertEquals(f.initial, f.manager.managedActiveDetails(f.initial.uuid).get.exactState)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.initial.uuid))
    assertEquals(0, f.world.removed)
    assertEquals(0, f.gateway.released)
    assertEquals(0, f.repository.newPathCalls)

  @Test def entityIdReplacementRejectsWithOriginalUuidStillIndexed(): Unit =
    val f = new Fixture
    f.retain(); f.world.idIndex = new Body(f.initial)
    val result = f.scope.consumeMode(f.success(), 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertTrue(f.body.frozen)
    assertEquals(0, f.body.finished)
    assertSame(f.body, f.world.uuidIndex)
    assertTrue(f.scope.modeResponsibility(f.graph.tag).get.completionConsumed)

  @Test def unknownFailureKeepsOriginalCausePendingFreezeAndMappedState(): Unit =
    val f = new Fixture
    f.retain()
    val original = new IllegalStateException("commit result unknown")
    val completion = IoCompletion[MySqlOwnedIoReply](f.graph.tag, Failure(original))
    val result = f.scope.consumeMode(completion, 1L)
    assertSame(original, result.cause.get)
    assertTrue(f.body.frozen)
    assertEquals(f.initial, f.manager.managedActiveDetails(f.initial.uuid).get.exactState)
    assertTrue(result.completionConsumed)
    assertEquals(0, f.repository.newPathCalls)
    assertEquals(0, f.gateway.released)

  @Test def staleRuntimeNeverUnfreezesEvenWithActualCommittedGraph(): Unit =
    val f = new Fixture
    f.retain()
    val result = f.scope.consumeMode(f.success(), 2L)
    assertEquals(MainControlledPublicationStatus.Retired, result.status)
    assertTrue(f.body.frozen)
    assertEquals(0, f.body.finished)
    assertTrue(result.completionConsumed)
    assertEquals(f.initial, f.manager.managedActiveDetails(f.initial.uuid).get.exactState)

  @Test def sessionLossAndTicketChunkLossCannotApplyGraph(): Unit =
    val f = new Fixture
    f.retain()
    val original = new IllegalStateException("original connection lost")
    f.session.failure = original
    val result = f.scope.consumeMode(f.success(), 1L)
    assertSame(original, result.cause.get)
    assertTrue(f.body.frozen)
    val lost = new Fixture
    lost.retain(); lost.gateway.chunks.remove((0, 0))
    assertEquals(MainControlledPublicationStatus.Faulted, lost.scope.consumeMode(lost.success(), 1L).status)
    assertEquals(0, lost.body.finished)
    assertEquals(0, lost.repository.newPathCalls)

  @Test def duplicateModeGraphCannotApplyControlTwice(): Unit =
    val f = new Fixture
    f.retain()
    val completion = f.success()
    f.scope.consumeMode(completion, 1L)
    assertThrows(classOf[IllegalArgumentException], () => f.scope.consumeMode(completion, 1L))
    assertEquals(1, f.body.finished)

  @Test def exactAlreadySetConsumesRequestWithoutVersionOrPhysicalResourceMutation(): Unit =
    val f = new Fixture
    f.retain(f.noChangeRequest, f.noChangePlan)
    val readback = new MySqlOwnedFormat3SnapshotTest().exactAlreadySetReadback(
      f.graph.tag, f.noChangePlan, f.noChangeRequest, f.initial)
    val forcedBefore = f.gateway.chunks.toSet
    val completion = IoCompletion[MySqlOwnedIoReply](f.graph.tag, Success(MySqlOwnedIoReply.OwnedModeAlreadySet(readback)))

    val result = f.scope.consumeMode(completion, 1L)

    assertEquals(MainControlledPublicationStatus.AlreadySet, result.status)
    assertTrue(result.completionConsumed)
    assertTrue(result.cause.isEmpty)
    assertEquals(f.initial, f.manager.managedActiveDetails(f.initial.uuid).get.exactState)
    assertEquals(f.initial, f.body.state)
    assertEquals(1, f.body.finished, "only the pending freeze is released against the exact unchanged state")
    assertFalse(f.body.frozen, "the exact existing Normal state resumes")
    assertSame(f.body, f.world.uuidIndex)
    assertSame(f.body, f.world.idIndex)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.initial.uuid))
    assertEquals(forcedBefore, f.gateway.chunks.toSet)
    assertEquals(0, f.world.removed)
    assertEquals(0, f.gateway.released)
    assertEquals(0, f.repository.newPathCalls)
    completeMemoryReleaseAck(f.scope, f.graph.tag)

  @Test def exactModeProjectionMemoryAckRequiresSuccessfulMainCompactionAndCannotRepeat(): Unit =
    val f = new Fixture
    f.retain()
    val published = f.scope.consumeMode(f.success(), 1L)
    assertEquals(MainControlledPublicationStatus.Published, published.status)
    assertTrue(f.scope.modeCompletionRetained(f.graph.tag))

    val staleTag = f.graph.tag.copy(operationId = MySqlPrepareModeGraphFixture.id(799))
    assertThrows(classOf[IllegalArgumentException], () => f.scope.prepareModeProjectionRelease(staleTag))
    assertTrue(f.scope.modeCompletionRetained(f.graph.tag), "stale tag cannot drop the actual Main graph")

    f.scope.prepareModeProjectionRelease(f.graph.tag)
    assertFalse(f.scope.modeCompletionRetained(f.graph.tag), "Main must drop its completion graph before sending the worker ACK")
    assertThrows(classOf[IllegalArgumentException], () => f.scope.prepareModeProjectionRelease(f.graph.tag))
    assertThrows(classOf[IllegalArgumentException], () => f.scope.acknowledgeModeProjectionReleased(f.graph.tag))
    f.scope.markModeProjectionReleaseSubmitted(f.graph.tag)
    assertThrows(classOf[IllegalArgumentException], () => f.scope.markModeProjectionReleaseSubmitted(f.graph.tag))
    f.scope.acknowledgeModeProjectionReleased(f.graph.tag)
    assertThrows(classOf[IllegalArgumentException], () => f.scope.acknowledgeModeProjectionReleased(f.graph.tag))
    assertEquals(f.graph.state, f.manager.managedActiveDetails(f.initial.uuid).get.exactState,
      "memory accounting ACK does not change the durable mode state")

  @Test def failedOrRetiredModeCompletionCannotPrepareMemoryRelease(): Unit =
    val failed = new Fixture
    failed.retain()
    val cause = new IllegalStateException("mode commit remains unknown")
    val failedResult = failed.scope.consumeMode(IoCompletion(failed.graph.tag, Failure(cause)), 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, failedResult.status)
    assertTrue(failed.scope.modeCompletionRetained(failed.graph.tag))
    assertThrows(classOf[IllegalArgumentException], () => failed.scope.prepareModeProjectionRelease(failed.graph.tag))
    assertTrue(failed.scope.modeCompletionRetained(failed.graph.tag))

    val retired = new Fixture
    retired.retain()
    val retiredResult = retired.scope.consumeMode(retired.success(), 2L)
    assertEquals(MainControlledPublicationStatus.Retired, retiredResult.status)
    assertTrue(retired.scope.modeCompletionRetained(retired.graph.tag))
    assertThrows(classOf[IllegalArgumentException], () => retired.scope.prepareModeProjectionRelease(retired.graph.tag))
    assertTrue(retired.body.frozen)

  @Test def retiredPublicationCannotArmAnUnsubmittedProjectionRelease(): Unit =
    val f = new Fixture
    f.retain()
    assertEquals(MainControlledPublicationStatus.Published, f.scope.consumeMode(f.success(), 1L).status)
    val identity = MySqlOwnedIoIdentity(f.graph.tag.bootId, f.graph.tag.bootGeneration,
      f.graph.tag.connectionGeneration)
    val releases = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 1)
    releases.reserve(f.graph.tag)
    f.scope.prepareModeProjectionRelease(f.graph.tag)
    assertFalse(f.scope.modeCompletionRetained(f.graph.tag))
    f.scope.retire()
    assertFalse(f.scope.modeProjectionReleaseEligible(f.graph.tag),
      "retirement before exact worker submission cannot free worker accounting")
    assertTrue(releases.readyTags.isEmpty,
      "an unarmed tag remains retained when its generation retires")
    assertEquals(1, releases.pendingCount)

  @Test def alreadySetCannotUseAnotherRetainedPlanOrStalePhysicalSession(): Unit =
    val differentPlan = new Fixture
    differentPlan.retain(differentPlan.noChangeRequest, differentPlan.noChangePlan)
    val otherPlan = MySqlOwnedRepositoryOperationPlan.single(differentPlan.graph.tag,
      MySqlPrepareModeGraphFixture.id(810), MySqlPrepareModeGraphFixture.id(811),
      differentPlan.noChangeRequest.repositoryOperation, new MySqlOwnedFormat3SnapshotTest().limitProfileForFixture)
    val wrongPlanReadback = new MySqlOwnedFormat3SnapshotTest().exactAlreadySetReadback(
      differentPlan.graph.tag, otherPlan, differentPlan.noChangeRequest, differentPlan.initial)
    val wrongPlanCompletion: IoCompletion[MySqlOwnedIoReply] = IoCompletion(differentPlan.graph.tag,
      Success(MySqlOwnedIoReply.OwnedModeAlreadySet(wrongPlanReadback)))
    val wrongPlan = differentPlan.scope.consumeMode(wrongPlanCompletion, 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, wrongPlan.status)
    assertTrue(wrongPlan.cause.nonEmpty)
    assertTrue(differentPlan.body.frozen)
    assertEquals(0, differentPlan.body.finished)
    assertEquals(differentPlan.initial, differentPlan.manager.managedActiveDetails(differentPlan.initial.uuid).get.exactState)
    assertSame(differentPlan.ticket, differentPlan.chunks.getAllManagedTickets()(differentPlan.initial.uuid))
    assertEquals(0, differentPlan.world.removed)
    assertEquals(0, differentPlan.gateway.released)

    val staleSession = new Fixture
    staleSession.retain(staleSession.noChangeRequest, staleSession.noChangePlan)
    val original = new IllegalStateException("original mode session expired")
    staleSession.session.failure = original
    val exact = new MySqlOwnedFormat3SnapshotTest().exactAlreadySetReadback(
      staleSession.graph.tag, staleSession.noChangePlan, staleSession.noChangeRequest, staleSession.initial)
    val staleCompletion: IoCompletion[MySqlOwnedIoReply] = IoCompletion(staleSession.graph.tag,
      Success(MySqlOwnedIoReply.OwnedModeAlreadySet(exact)))
    val stale = staleSession.scope.consumeMode(staleCompletion, 1L)
    assertSame(original, stale.cause.get)
    assertEquals(MainControlledPublicationStatus.Faulted, stale.status)
    assertTrue(staleSession.body.frozen)
    assertEquals(0, staleSession.body.finished)
    assertEquals(staleSession.initial, staleSession.manager.managedActiveDetails(staleSession.initial.uuid).get.exactState)
    assertSame(staleSession.ticket, staleSession.chunks.getAllManagedTickets()(staleSession.initial.uuid))
    assertEquals(0, staleSession.world.removed)
    assertEquals(0, staleSession.gateway.released)

    val ticketDrift = new Fixture
    ticketDrift.retain(ticketDrift.noChangeRequest, ticketDrift.noChangePlan)
    ticketDrift.gateway.chunks.remove((0, 0))
    val exactButTicketDrifted = new MySqlOwnedFormat3SnapshotTest().exactAlreadySetReadback(
      ticketDrift.graph.tag, ticketDrift.noChangePlan, ticketDrift.noChangeRequest, ticketDrift.initial)
    val driftedCompletion: IoCompletion[MySqlOwnedIoReply] = IoCompletion(ticketDrift.graph.tag,
      Success(MySqlOwnedIoReply.OwnedModeAlreadySet(exactButTicketDrifted)))
    val drifted = ticketDrift.scope.consumeMode(driftedCompletion, 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, drifted.status)
    assertTrue(drifted.cause.nonEmpty)
    assertTrue(ticketDrift.body.frozen)
    assertEquals(0, ticketDrift.body.finished)
    assertEquals(ticketDrift.initial, ticketDrift.manager.managedActiveDetails(ticketDrift.initial.uuid).get.exactState)
    assertSame(ticketDrift.ticket, ticketDrift.chunks.getAllManagedTickets()(ticketDrift.initial.uuid))
    assertEquals(0, ticketDrift.world.removed)
    assertEquals(0, ticketDrift.gateway.released)

  private def completeMemoryReleaseAck(scope: ActiveFloatingPodManager#MainPublicationScope, tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    val identity = MySqlOwnedIoIdentity(tag.bootId, tag.bootGeneration, tag.connectionGeneration)
    val release = new MainModeProjectionReleaseLedger(Thread.currentThread(), identity, 1)
    release.reserve(tag)
    assertTrue(release.readyTags.isEmpty, "Main cannot enqueue release before clearing its graphs and compacting the business ledger")
    scope.prepareModeProjectionRelease(tag)
    assertFalse(scope.modeCompletionRetained(tag))
    release.arm(tag) // Mirrors finish() only after MainDebugOperationLedger.acknowledged/pending.remove.
    assertEquals(Vector(tag), release.readyTags)
    release.submitted(tag, IoSubmission.Accepted)
    scope.markModeProjectionReleaseSubmitted(tag)
    val exactAck = IoCompletion[MySqlOwnedIoReply](tag, Success(MySqlOwnedIoReply.ModeProjectionReleased(tag)))
    assertEquals(MainModeProjectionReleaseResult.Released(tag), release.consume(exactAck))
    scope.acknowledgeModeProjectionReleased(tag)

  /** 真实Main逐Pod观察入口；无SQL或游戏运行，验证坏body不阻断健康缓存捕获。 */
  @Test def dbFirstPeriodicPoseFailureRetainsOriginalBodyTicketAndContinuesHealthyPod():Unit =
    for brokenIndex <- Vector(false,true) do
      val data=MySqlPrepareModeGraphFixture.mode()
      val (_,a,attempt)=MySqlPrepareModeGraphFixture.activeOwned
      val b=a.copy(uuid=MySqlPrepareModeGraphFixture.id(990))
      val locator=MySqlPrepareModeGraphFixture.inventory(MySqlPrepareModeGraphFixture.activeOwned._1).recovery.locators.head
      val repository=new SetupRepository(a,locator,Some((b,locator.copy(podUuid=b.uuid))))
      val gateway=new Gateway
      val chunks=new ChunkLoadingManager(repository,gateway)
      val manager=new ActiveFloatingPodManager(repository,chunks)
      val bodyA=new Body(a)
      val bodyB=new Body(b,92)
      val worldA=new TestWorld(bodyA)
      val worldB=new TestWorld(bodyB)
      val ticketA=chunks.requestTicket(a.uuid,worldA,0,0).get
      manager.recoverCommittedActive(a,worldA).get
      chunks.requestTicket(b.uuid,worldB,0,0).get
      manager.recoverCommittedActive(b,worldB).get
      val scope=manager.installMainPublication(MySqlOwnedIoIdentity(data.graph.tag.bootId,1L,1L),1L,data.request.expectedBinding,256)
      repository.forbid=true
      if brokenIndex then worldA.dead=true else bodyA.state=a.copy(posX=32D)
      bodyB.state=b.copy(posX=9D)
      scope.observeDbFirstPeriodicPoses()
      val original=scope.retainedFailureCauses.head
      assertEquals(1,scope.retainedFailureCauses.size)
      assertTrue(bodyA.frozen)
      assertEquals(1L,manager.dirtyRevisionSnapshot(b.uuid))
      assertFalse(manager.dirtyRevisionSnapshot.contains(a.uuid))
      assertSame(ticketA,chunks.getAllManagedTickets()(a.uuid))
      assertTrue(manager.getActivePod(a.uuid).nonEmpty)
      assertTrue(manager.getActivePod(b.uuid).nonEmpty)
      assertEquals(0,gateway.released)
      assertEquals(0,worldA.removed)
      assertEquals(0,worldB.removed)
      assertThrows(classOf[IllegalArgumentException],()=>scope.assertCriticalTransitionAllowed(a.uuid))
      scope.assertCriticalTransitionAllowed(b.uuid)
      bodyB.state=b.copy(posX=10D)
      scope.observeDbFirstPeriodicPoses()
      assertSame(original,scope.retainedFailureCauses.head)
      assertEquals(1,scope.retainedFailureCauses.size)
      assertEquals(2L,manager.dirtyRevisionSnapshot(b.uuid))
      assertEquals(0,repository.newPathCalls)

  private final class Fixture(initialOverride: Option[FloatingPodState] = None):
    val data = MySqlPrepareModeGraphFixture.mode()
    val graph = data.graph
    val (_, defaultInitial, attempt) = MySqlPrepareModeGraphFixture.activeOwned
    val initial = initialOverride.getOrElse(defaultInitial)
    val body = new Body(initial)
    val world = new TestWorld(body)
    val repository = new SetupRepository(initial, MySqlPrepareModeGraphFixture.inventory(MySqlPrepareModeGraphFixture.activeOwned._1).recovery.locators.head)
    val gateway = new Gateway
    val chunks = new ChunkLoadingManager(repository, gateway)
    val manager = new ActiveFloatingPodManager(repository, chunks)
    val ticket = chunks.requestTicket(initial.uuid, world, 0, 0).get
    manager.recoverCommittedActive(initial, world).get // unit-only existing mapping setup; not new endpoint path。
    repository.forbid = true
    val scope = manager.installMainPublication(MySqlOwnedIoIdentity(graph.tag.bootId, 1L, 1L), 1L,
      data.request.expectedBinding, 256)
    val request = new DebugItemEndpoint.Request(new DebugItemEndpoint.Identity(graph.tag.bootId,
      data.request.authenticatedOwner, MySqlPrepareModeGraphFixture.id(700), MySqlPrepareModeGraphFixture.id(701),
      world.diagnosticWorldName, 7, body.entityId, body.podUuid, MySqlPrepareModeGraphFixture.id(702)),
      1L, graph.tag.operationId, DebugItemEndpoint.Action.TOGGLE, null)
    val session = new Session(request, world)
    private val format3Test = new MySqlOwnedFormat3SnapshotTest()
    val modePlan = MySqlOwnedRepositoryOperationPlan.single(graph.tag,
      MySqlPrepareModeGraphFixture.id(703), MySqlPrepareModeGraphFixture.id(704),
      data.request.repositoryOperation, format3Test.limitProfileForFixture)
    val noChangeRequest = data.request.copy(targetMode = initial.operatingMode, actualPose = body.actualPose)
    val noChangePlan = MySqlOwnedRepositoryOperationPlan.single(graph.tag,
      MySqlPrepareModeGraphFixture.id(706), MySqlPrepareModeGraphFixture.id(707),
      noChangeRequest.repositoryOperation, format3Test.limitProfileForFixture)
    def retain(sqlRequest: MySqlOwnedModeCommitRequest = data.request,
        operationPlan: MySqlOwnedRepositoryOperationPlan = modePlan): Unit =
      body.beginPendingMode(request.operation(), sqlRequest.authenticatedOwner, sqlRequest.expectedStateVersion)
      scope.retainMode(graph.tag, request, sqlRequest, operationPlan, session, world, body, ticket, 1L)
    def success(): IoCompletion[MySqlOwnedIoReply] = IoCompletion(graph.tag, Success(MySqlOwnedIoReply.OwnedModeCommitted(graph)))

  private final class Session(expected: DebugItemEndpoint.Request, world: PodWorld) extends MainOwnedModePublicationSession:
    var failure: Throwable = null
    override def assertCurrent(request: DebugItemEndpoint.Request, actual: PodWorld): Unit =
      if failure != null then throw failure
      require(request == expected && (actual eq world), "stand-in original session mismatch")
  private final class Body(var state: FloatingPodState, id:Int=91) extends NormalStopActivePodEntity, TrackedDriftActivePodEntity:
    var frozen = false
    var operation: UUID = null
    var expectedVersion = -1L
    var finished = 0
    var stopRequest: UUID = null
    var freezeFailure: Throwable = null
    var freezeCalls = 0
    override def freezeForNormalStop(request: UUID): Unit =
      freezeCalls += 1
      require(stopRequest == null || stopRequest == request)
      stopRequest = request
      frozen = true
      if freezeFailure != null then throw freezeFailure
    override def requireNormalStopFreeze(request: UUID, base: FloatingPodState, attempt: UUID): Unit =
      require(stopRequest == request && state.uuid == base.uuid && state.stateVersion == base.stateVersion && frozen)
    override def podUuid: UUID = state.uuid
    override def entityId: Int = id
    override def actualPose: FloatingPodPose = FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ,
      state.yaw, state.pitch, state.dimensionId.get)
    override def isFrozen: Boolean = frozen
    override def sameBody(other: ControlledActivePodEntity): Boolean = other eq this
    override def hasControlledCreationStamp(receipt: UnboundPreparationReceipt): Boolean = false
    override def applyCommittedControl(value: FloatingPodState, receipt: UnboundPreparationReceipt): Unit =
      throw new AssertionError("mode tests do not use controlled create")
    override def applyCommittedTrackedDrift(base:FloatingPodState,value:FloatingPodState,attempt:UUID):Unit = state=value
    override def applyCommittedMode(value: FloatingPodState, attempt: UUID): Unit = state = value
    override def markControlFaulted(cause: Throwable): Unit = frozen = true
    override def beginPendingMode(op: UUID, owner: UUID, version: Long): Unit =
      require(operation == null && state.ownerUuid.contains(owner) && state.stateVersion == version, "exact stand-in operation guard")
      operation = op; expectedVersion = version; frozen = true
    override def hasPendingMode(op: UUID, version: Long): Boolean = operation == op && expectedVersion == version
    override def finishPendingMode(op: UUID, version: Long, value: FloatingPodState, attempt: UUID): Unit =
      require(hasPendingMode(op, version), "pending operation differs")
      state = value; operation = null; frozen = stopRequest != null || value.operatingMode == com.wi.floatingpod.domain.PodOperatingMode.WorldIdle; finished += 1
  private final class TestWorld(body: Body) extends PodWorld:
    var uuidIndex: ControlledActivePodEntity = body
    var idIndex: ControlledActivePodEntity = body
    var dead = false
    var removed = 0
    override def dimensionId: Int = 7
    override def diagnosticWorldName: String = MySqlPrepareModeGraphFixture.pose.worldName
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(uuid: UUID): Option[ActivePodEntity] = Option(uuidIndex).filter(_.podUuid == uuid)
    override def assertLiveIndexedBody(actual: ControlledActivePodEntity): Unit =
      require(!dead && (actual eq uuidIndex) && (actual eq idIndex), "actual stand-in dual index/live body mismatch")
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new AssertionError("mode cannot spawn")
    override def removePod(entity: ActivePodEntity): Unit = { removed += 1; throw new AssertionError("mode cannot remove") }
  private final class Gateway extends ChunkTicketGateway:
    final class TestTicket extends ChunkTicket
    var pod = Option.empty[UUID]
    val chunks = mutable.HashSet.empty[(Int, Int)]
    var released = 0
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = Some(new TestTicket)
    override def release(ticket: ChunkTicket): Unit = { released += 1 }
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, uuid: UUID): Unit = pod = Some(uuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = pod
    override def force(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunks.add((x, z)); () }
    override def unforce(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunks.remove((x, z)); () }
    override def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] = chunks.toSet
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class SetupRepository(initial: FloatingPodState, locator: PhysicalRecoveryLocator,
      additional:Option[(FloatingPodState,PhysicalRecoveryLocator)]=None)
      extends FloatingPodRepository, PhysicalRecoveryStore:
    var forbid = false
    var newPathCalls = 0
    private def setupOnly(): Unit =
      if forbid then { newPathCalls += 1; throw new AssertionError("new Main mode seam accessed repository") }
    private def unused[A](): Try[A] = { setupOnly(); Failure(new UnsupportedOperationException("unit setup does not implement SQL")) }
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = { setupOnly(); Success((Some(initial).toVector++additional.map(_._1)).find(_.uuid==uuid)) }
    override def findPhysicalRecoveryLocator(uuid: UUID): Try[Option[PhysicalRecoveryLocator]] = { setupOnly(); Success((Some(locator).toVector++additional.map(_._2)).find(_.podUuid==uuid)) }
    override def insert(state: FloatingPodState): Try[FloatingPodState] = unused()
    override def update(state: FloatingPodState): Try[FloatingPodState] = unused()
    override def findByOwner(uuid: UUID): Try[List[FloatingPodState]] = unused()
    override def findAllActive(): Try[List[FloatingPodState]] = unused()
    override def findAll(): Try[List[FloatingPodState]] = unused()
    override def delete(uuid: UUID): Try[Boolean] = unused()
    override def transaction[T](fn: => T): Try[T] = unused()
    override def close(): Unit = { setupOnly(); throw new AssertionError("no SQL close in unit setup") }
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = unused()
    override def createOrRetainLegacyUncertifiedBinding(uuid: UUID, version: Int): Try[PhysicalRecoveryBinding] = unused()
    override def certifyPhysicalRecoveryBinding(uuid: UUID, version: Int): Try[PhysicalRecoveryBinding] = unused()
    override def prepareCreateLocator(uuid: UUID, version: Long, attempt: UUID, dimension: Int,
        chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = unused()
    override def activatePreparedIfAdmissible(state: FloatingPodState, owner: UUID, version: Long,
        dimension: Int, limit: Int, attempt: UUID): Try[AdmissionOutcome] = unused()
    override def deactivateTrackedIfExpectedActive(uuid: UUID, owner: Option[UUID], version: Long,
        attempt: UUID): Try[DeactivationOutcome] = unused()
