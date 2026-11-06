package com.wi.floatingpod.server

import com.wi.floatingpod.debug.DebugItemEndpoint
import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState}
import com.wi.floatingpod.storage.*
import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoCompletion, OwnedSerialIo, IoSubmission}
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.{Success, Failure, Try}

/** 真manager/IO线程/graph规则；只替代游戏gateway与SQL输入，不声称MySQL/native/Ready。 */
class MainControlledPublicationTest:
  private val runners = mutable.ArrayBuffer.empty[MySqlControlledDebugRunnerFixture]
  @org.junit.jupiter.api.AfterEach def closeActualRunners(): Unit =
    var primary: Throwable = null
    runners.reverseIterator.foreach { runner =>
      try runner.closeAfterMainDrop()
      catch case cause: Throwable =>
        if primary == null then primary = cause else if primary ne cause then primary.addSuppressed(cause)
    }
    runners.clear()
    if primary != null then throw primary

  @Test def publishesActualWorkerCompletionAndPureInspectionNeverTouchesRepository(): Unit =
    val f = new Fixture()
    f.retain()
    val completed = new CountDownLatch(1)
    val io = new OwnedSerialIo[MySqlControlledDebugRunnerFixture, MySqlOwnedIoReply](1, Thread.currentThread(), "main-publication-test", None, Some(completed))(
      data => MySqlOwnedIoReply.ControlledDebugCommitted(data.graph))
    var primary: Throwable = null
    try
      assertEquals(IoSubmission.Accepted, io.submit(f.data.tag, f.data))
      assertTrue(completed.await(3, TimeUnit.SECONDS), "actual worker did not execute")
      assertEquals(1, io.checkpoint.completions, "actual completion was not enqueued")
      val completion = io.drain(1).head
      val result = f.scope.consume(completion, 1L)
      assertEquals(MainControlledPublicationStatus.Published, result.status)
      assertTrue(result.completionConsumed)
      assertEquals(List(f.data.receipt.podUuid), f.manager.getAllActivePods())
      assertEquals(f.data.state, f.manager.managedActiveDetails(f.data.receipt.podUuid).get.exactState)
      val calls = f.world.findCalls
      assertEquals(DebugPodPhase.Committed, f.scope.inspect(f.data.receipt.podUuid).get.phase)
      assertEquals(calls, f.world.findCalls, "inspect must not sample/repair world")
      assertEquals(0, f.repository.calls, "new Main path invoked repository")
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      var cleanup: Throwable = null
      try io.quiesce()
      catch case cause: Throwable => cleanup = cause
      try awaitExit(io)
      catch case cause: Throwable =>
        if cleanup == null then cleanup = cause else if cleanup ne cause then cleanup.addSuppressed(cause)
      if cleanup != null then
        if primary != null then primary.addSuppressed(cleanup) else throw cleanup

  @Test def initialCompletionCannotPublishThroughLegacyPreparationOrMissingCapture():Unit =
    val f=new Fixture()
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.retainDbFirstInitial(null,null,f.data.receipt,
      new com.wi.floatingpod.debug.MainPodOperationRequest.Player(f.request),f.session,f.world,f.body,f.ticket,1L))
    f.retain()
    val result=f.scope.consume(IoCompletion(f.data.tag,Success(MySqlOwnedIoReply.DbFirstInitialActivationObserved(null))),1L)
    assertEquals(MainControlledPublicationStatus.Faulted,result.status)
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.submitDbFirstInitialRetirement(f.data.tag))
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.consumeDbFirstInitialRetirement(null))
    assertThrows(classOf[IllegalArgumentException],()=>f.scope.consumeDbFirstInitialRetirement(
      IoCompletion(f.data.tag,Success(MySqlOwnedIoReply.DbFirstInitialPublicationReleased(null)))))
    assertTrue(result.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertEquals(0,f.body.applyCalls)
    assertEquals(0,f.repository.calls)
    assertTrue(f.world.findPod(f.data.receipt.podUuid).exists(_.podUuid==f.body.podUuid))

  @Test def duplicateCompletionDoesNotApplyControlTwice(): Unit =
    val f = new Fixture()
    f.retain()
    val completion = f.success()
    assertEquals(MainControlledPublicationStatus.Published, f.scope.consume(completion, 1L).status)
    assertThrows(classOf[IllegalArgumentException], () => f.scope.consume(completion, 1L))
    assertEquals(1, f.body.applyCalls)
    assertEquals(0, f.repository.calls)

  @Test def staleGenerationAndRetiredScopeKeepCompletionWithoutPublication(): Unit =
    val f = new Fixture()
    f.retain()
    val result = f.scope.consume(f.success(), 2L)
    assertEquals(MainControlledPublicationStatus.Retired, result.status)
    assertTrue(result.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertEquals(0, f.body.applyCalls)
    val other = new Fixture(200L)
    other.retain()
    other.scope.retire()
    assertEquals(MainControlledPublicationStatus.Retired, other.scope.consume(other.success(), 1L).status)
    assertEquals(1, other.chunks.getAllManagedTickets().size)

  @Test def foreignTagExceptionRetainsExactUnmatchedCompletion(): Unit =
    val f = new Fixture()
    f.retain()
    val value = f.success().copy(tag = f.data.tag.copy(connectionGeneration = 2L))
    val failure = assertThrows(classOf[MainPublicationUnmatchedCompletion], () => f.scope.consume(value, 1L))
    assertSame(value, failure.completion)
    assertFalse(f.scope.responsibility(f.data.tag).get.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)

  @Test def replacedBodyWrongPoseAndActualTicketChunkLossCannotPublish(): Unit =
    val replaced = new Fixture()
    replaced.retain()
    replaced.world.indexed = new TestBody(replaced.data.receipt)
    assertEquals(MainControlledPublicationStatus.Faulted, replaced.scope.consume(replaced.success(), 1L).status)
    assertEquals(0, replaced.body.applyCalls)
    assertEquals(0, replaced.world.removeCalls)
    val moved = new Fixture(200L)
    moved.retain()
    moved.body.pose = moved.body.pose.copy(posX = 9D)
    assertEquals(MainControlledPublicationStatus.Faulted, moved.scope.consume(moved.success(), 1L).status)
    val lost = new Fixture(300L)
    lost.retain()
    lost.gateway.chunkSet.remove((0, 0))
    assertEquals(MainControlledPublicationStatus.Faulted, lost.scope.consume(lost.success(), 1L).status)
    assertEquals(0, lost.repository.calls)
    assertEquals(0, lost.gateway.releaseCalls)

  @Test def deadBodyStillInUuidIndexCannotPublishAndRetainsActualCompletion(): Unit =
    val f = new Fixture()
    f.retain()
    f.body.dead = true // UUID lookup still returns the exact original body.
    val completion = f.success()
    val result = f.scope.consume(completion, 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertTrue(result.completionConsumed)
    assertTrue(f.scope.responsibility(f.data.tag).get.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertEquals(0, f.body.applyCalls)
    assertEquals(1, f.chunks.getAllManagedTickets().size)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.data.receipt.podUuid))
    assertSame(f.body, f.world.indexed)
    assertEquals(f.data.receipt.podUuid, result.view.get.podUuid)
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.world.removeCalls)
    assertEquals(0, f.repository.calls)

  @Test def replacedEntityIdIndexCannotPublishEvenWithOriginalUuidIndex(): Unit =
    val f = new Fixture()
    f.retain()
    f.world.idIndexed = new TestBody(f.data.receipt)
    val result = f.scope.consume(f.success(), 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertTrue(result.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertEquals(0, f.body.applyCalls)
    assertEquals(1, f.chunks.getAllManagedTickets().size)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.data.receipt.podUuid))
    assertSame(f.body, f.world.indexed)
    assertEquals(f.data.receipt.podUuid, result.view.get.podUuid)
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.world.removeCalls)
    assertEquals(0, f.repository.calls)

  @Test def actualIndexObservationFailureRetainsOriginalCauseAndCompletion(): Unit =
    val f = new Fixture()
    f.retain()
    val original = new IllegalStateException("actual entity index observation failed")
    f.world.indexFailure = original
    val result = f.scope.consume(f.success(), 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertSame(original, result.cause.get)
    assertTrue(result.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.data.receipt.podUuid))
    assertSame(f.body, f.world.indexed)
    assertEquals(0, f.body.applyCalls)
    assertEquals(0, f.repository.calls)

  @Test def foreignGraphAndLostSessionAreFaultedWithoutMapping(): Unit =
    val f = new Fixture()
    f.retain()
    val foreign = new MySqlControlledDebugRunnerFixture(200L, FloatingPodPose("publication-world", 8.5D, 64D, 8.5D, 0F, 0F, 7))
    runners += foreign
    val other = foreign.graph
    val wrong = IoCompletion(f.data.tag, Success(MySqlOwnedIoReply.ControlledDebugCommitted(other)))
    assertEquals(MainControlledPublicationStatus.Faulted, f.scope.consume(wrong, 1L).status)
    val expired = new Fixture(300L)
    expired.retain()
    val original = new IllegalStateException("actual session replaced")
    expired.session.failure = original
    val result = expired.scope.consume(expired.success(), 1L)
    assertSame(original, result.cause.get)
    assertTrue(expired.manager.getAllActivePods().isEmpty)
    assertEquals(1, expired.chunks.getAllManagedTickets().size)

  @Test def afterAckReadbackAndPublicationFailureRetainOriginalCauseAndResources(): Unit =
    val f = new Fixture()
    f.retain()
    val original = new IllegalStateException("readback failed after actual ACK")
    val failed = MySqlUnboundAfterCommitReadbackFailure(f.data.tag, f.data.receipt, f.data.outcome, original)
    val result = f.scope.consume(IoCompletion(f.data.tag, Success(MySqlOwnedIoReply.ControlledUnboundReadbackFailed(failed))), 1L)
    assertSame(original, result.cause.get)
    assertTrue(result.view.get.active)
    assertEquals(DebugPodPhase.Faulted, result.view.get.phase)
    assertEquals(1, f.chunks.getAllManagedTickets().size)
    assertEquals(0, f.world.removeCalls)
    val mirror = new Fixture(200L)
    mirror.retain()
    mirror.body.applyFailure = original
    val cleanup = new IllegalStateException("fault mirror also failed")
    mirror.body.faultFailure = cleanup
    assertSame(original, mirror.scope.consume(mirror.success(), 1L).cause.get)
    assertTrue(original.getSuppressed.contains(cleanup))
    assertTrue(mirror.manager.getAllActivePods().isEmpty)

  @Test def wrongThreadRejectsInspectionBeforeRepository(): Unit =
    val f = new Fixture()
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val other = new Thread(() =>
      try
        f.scope.inspect(f.data.receipt.podUuid)
        ()
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown(), "foreign-publication-test")
    other.start()
    try
      assertTrue(done.await(3, TimeUnit.SECONDS), "foreign actor timed out")
      assertTrue(failure.get().isInstanceOf[IllegalStateException])
      assertEquals(0, f.repository.calls)
    finally
      other.join(3000)
      assertFalse(other.isAlive, "foreign actor leaked")

  @Test def managerRuntimeAndPendingPodRightsAreOneShot(): Unit =
    val f = new Fixture()
    assertThrows(classOf[IllegalArgumentException], () => f.manager.installMainPublication(
      MySqlOwnedIoIdentity(f.data.tag.bootId, 1L, 1L), 2L, f.data.receipt.binding, 256))
    f.retain()
    assertThrows(classOf[IllegalArgumentException], () => f.retain())
    assertEquals(0, f.repository.calls)
    assertEquals(1, f.chunks.getAllManagedTickets().size)

  @Test def retainRejectsObservedAdoptionAndUnknownTicketWithoutDeletingBody(): Unit =
    val f = new Fixture()
    val receipt = f.data.receipt.copy(origin = UnboundIntentOrigin.ObservedArtifactAdoption,
      observedIdentity = Some(ObservedUnboundIdentity(ObservedUnboundSourceKind.FreshArtifact, MainPublicationGraphFixture.id(9), "original")), createOperationUuid = None)
    assertThrows(classOf[IllegalArgumentException], () => f.scope.retainControlled(f.data.tag, receipt, f.request,
      f.session, f.world, f.body, f.ticket, 1L))
    f.gateway.uuid = None
    assertThrows(classOf[IllegalArgumentException], () => f.retain())
    assertEquals(0, f.world.removeCalls)
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.repository.calls)

  @Test def criticalCacheUpdateFailsWithoutChangingPublishedBodyMappingOrDirtyRevision(): Unit =
    val f = new Fixture()
    f.retain()
    assertEquals(MainControlledPublicationStatus.Published, f.scope.consume(f.success(), 1L).status)
    val original = f.manager.managedActiveDetails(f.data.state.uuid).get.exactState
    f.manager.updateCachedState(original.uuid, original.copy(posX = original.posX + 1D)).get
    val dirty = f.manager.dirtyRevisionSnapshot
    val calls = f.body.applyCalls
    val rejected = f.manager.updateCachedState(original.uuid, original.copy(stateVersion = original.stateVersion + 1L))
    assertTrue(rejected.isFailure)
    assertEquals(dirty, f.manager.dirtyRevisionSnapshot)
    assertEquals(original, f.manager.managedActiveDetails(original.uuid).get.exactState)
    assertEquals(calls, f.body.applyCalls)
    assertEquals(0, f.repository.calls)
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.world.removeCalls)

  @Test def schedulerRequestsCoalesceWithoutWorldDatabaseOrMainDirtyMutation(): Unit =
    val f = new Fixture()
    val observed = new AtomicReference[Throwable]()
    val helper = new Thread(() =>
      try
        assertEquals(1, f.manager.requestPeriodicFlush().get)
        assertEquals(0, f.manager.requestPeriodicFlush().get)
        assertThrows(classOf[IllegalStateException], () => f.manager.consumePeriodicFlushRequest())
      catch case cause: Throwable => observed.set(cause)
      (), "scheduler-capture-request")
    var primary: Throwable = null
    try
      helper.start()
      helper.join(4000L)
      assertFalse(helper.isAlive, "scheduler test actor did not exit")
      if observed.get() != null then throw observed.get()
      assertTrue(f.manager.consumePeriodicFlushRequest())
      assertFalse(f.manager.consumePeriodicFlushRequest())
      assertTrue(f.manager.dirtyRevisionSnapshot.isEmpty)
      assertEquals(0, f.repository.calls)
      assertEquals(0, f.world.findCalls)
      f.manager.stopPeriodicFlushRequests()
      assertTrue(f.manager.requestPeriodicFlush().isFailure)
      assertFalse(f.manager.consumePeriodicFlushRequest())
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try
        if helper.isAlive then helper.interrupt()
        helper.join(4000L)
        assertFalse(helper.isAlive, "scheduler test actor leaked")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  @Test def pendingAsyncDirtyOrSchedulerTailCannotClaimCleanShutdown(): Unit =
    val pod = MainPublicationGraphFixture.id(999)
    val clean = ManagerShutdownReport(List.empty, List.empty, Map.empty, None, None)
    assertTrue(clean.isClean)
    assertFalse(clean.copy(pendingAsyncDrift = Set(pod)).isClean)
    assertFalse(clean.copy(unflushedDirty = Set(pod)).isClean)
    assertFalse(clean.copy(asyncPreStopFinalizerRequired = true).isClean)
    assertFalse(com.wi.floatingpod.server.persistence.PersistenceStoppingReport(None, clean, true).isClean)

  @Test def installedAsyncShutdownRetainsPhysicalAndCompletionWithoutLegacySqlOrTicketRelease(): Unit =
    val f = new Fixture()
    f.retain()
    f.scope.consume(f.success(), 1L)
    val heldBefore = f.scope.retainedAsyncResponsibilityCount
    assertTrue(heldBefore > 0)
    val report = f.manager.shutdownWithReport()
    assertTrue(report.asyncPreStopFinalizerRequired)
    assertFalse(report.isClean)
    assertTrue(report.pendingTicketReconciliationFailure.nonEmpty)
    assertEquals(heldBefore, f.scope.retainedAsyncResponsibilityCount)
    assertEquals(0, f.repository.calls)
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.world.removeCalls)
    assertEquals(25, f.gateway.forcedChunks(f.ticket).size)

  @Test def actualMainFailureIsPrimaryAtStopAndOriginalCompletionBodyTicketRemainHeld(): Unit =
    val f = new Fixture()
    f.retain()
    val original = new IllegalStateException("controlled actual Main body apply failure")
    val stack = original.getStackTrace
    f.body.applyFailure = original
    val completion = f.success()
    val result = f.scope.consume(completion, 1L)
    assertEquals(MainControlledPublicationStatus.Faulted, result.status)
    assertSame(original, result.cause.get)
    val context = new IllegalStateException("stop has unsettled Main responsibility")
    val retained = f.scope.stopResponsibilityCause(context)
    assertSame(original, retained)
    assertTrue(stack.sameElements(retained.getStackTrace))
    assertTrue(retained.getSuppressed.contains(context))
    assertTrue(f.scope.retainedFailureCauses.exists(_ eq original))
    assertTrue(f.scope.responsibility(f.data.tag).get.completionConsumed)
    assertTrue(f.manager.getAllActivePods().isEmpty)
    assertSame(f.body, f.world.indexed)
    assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.data.receipt.podUuid))
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(0, f.repository.calls)

  @Test def actualMainFailureLogHasExactEntryTagPodStageAndOriginalThrowableOnce(): Unit =
    val f = new Fixture()
    val events = mutable.ArrayBuffer.empty[(String, Throwable)]
    val appender = new AbstractAppender("tracked-main-entry-diagnostic", null,
        PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY):
      override def append(event: LogEvent): Unit = { events += ((event.getMessage.getFormattedMessage, event.getThrown)); () }
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val logger = context.getLogger(classOf[ActiveFloatingPodManager].getName)
    appender.start()
    logger.addAppender(appender)
    val original = new IllegalStateException("actual Main apply diagnostic cause")
    val stack = original.getStackTrace
    try
      f.retain()
      f.body.applyFailure = original
      val result = f.scope.consume(f.success(), 1L)
      assertEquals(MainControlledPublicationStatus.Faulted, result.status)
      assertSame(original, result.cause.get)
      f.scope.stopResponsibilityCause(new IllegalStateException("stop context only"))
      val actual = events.filter(_._2 eq original)
      assertEquals(1, actual.size, "one authority diagnostic for the first original Entry failure")
      val tag = f.data.tag
      assertEquals(s"Main retained failure boot=${tag.bootId} bootGeneration=${tag.bootGeneration} connectionGeneration=${tag.connectionGeneration} operation=${tag.operationId} pod=${f.data.receipt.podUuid} stage=CONTROLLED_PUBLICATION",
        actual.head._1)
      assertSame(original, actual.head._2)
      assertTrue(stack.sameElements(actual.head._2.getStackTrace))
      assertTrue(f.scope.responsibility(tag).get.completionConsumed)
      assertSame(f.ticket, f.chunks.getAllManagedTickets()(f.data.receipt.podUuid))
      assertEquals(0, f.repository.calls)
      assertEquals(0, f.gateway.releaseCalls)
    finally
      logger.removeAppender(appender)
      appender.stop()

  /** 真实 manager 装配与 END 容量边界；完整 Minecraft endpoint/factory 仍需集成验证。 */
  @Test def actualManagerFirstReadyEndAcceptsLegalActiveLimits1_31_32(): Unit =
    List(1, 31, 32).foreach { limit =>
      val f = new Fixture(seed = 900L + limit, activeLimit = limit)
      assertEquals(limit, f.scope.activeLimit)
      val boundedBatch = Math.min(32, limit)
      f.scope.onTrackedDriftEnd(7L, PermanentPumpPhase.Ready, boundedBatch)
      f.scope.onTrackedDriftEnd(8L, PermanentPumpPhase.Ready, boundedBatch)
      assertThrows(classOf[IllegalArgumentException], () =>
        f.scope.onTrackedDriftEnd(9L, PermanentPumpPhase.Ready, limit + 1))
      if limit < 32 then assertThrows(classOf[IllegalArgumentException], () =>
        f.scope.onTrackedDriftEnd(10L, PermanentPumpPhase.Ready, 32))
      assertEquals(0, f.repository.calls)
    }

  private def awaitExit(io: OwnedSerialIo[MySqlControlledDebugRunnerFixture, MySqlOwnedIoReply]): Unit =
    val result = new AtomicReference[java.lang.Boolean]()
    val failure = new AtomicReference[Throwable]()
    val helper = new Thread(() =>
      try result.set(io.awaitExitOffMain(3, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      (), "publication-exit-observer")
    helper.start()
    try
      helper.join(4000)
      assertFalse(helper.isAlive, "off-Main exit observer leaked")
      if failure.get() != null then throw failure.get()
      assertEquals(java.lang.Boolean.TRUE, result.get(), "actual owned worker did not exit")
    finally
      if helper.isAlive then helper.interrupt()
      helper.join(4000)
      assertFalse(helper.isAlive, "exit observer cleanup failed")

  private final class Fixture(seed: Long = 100L, activeLimit: Int = 256):
    val data = new MySqlControlledDebugRunnerFixture(seed, FloatingPodPose("publication-world", 8.5D, 64D, 8.5D, 0F, 0F, 7), activeLimit)
    runners += data
    val repository = new NoMainRepository
    val gateway = new TestGateway
    val chunks = new ChunkLoadingManager(repository, gateway)
    val manager = new ActiveFloatingPodManager(repository, chunks)
    val scope = manager.installMainPublication(MySqlOwnedIoIdentity(data.tag.bootId, 1L, 1L), 1L, data.receipt.binding, activeLimit)
    val body = new TestBody(data.receipt)
    val world = new TestWorld(body)
    val ticket = chunks.requestTicket(data.receipt.podUuid, world, 0, 0).get
    val request = new DebugItemEndpoint.Request(new DebugItemEndpoint.Identity(data.tag.bootId,
      MainPublicationGraphFixture.id(10), MainPublicationGraphFixture.id(11), MainPublicationGraphFixture.id(12),
      data.receipt.targetPose.worldName, 7, -1, null, null), 1L, data.receipt.createOperationUuid.get,
      DebugItemEndpoint.Action.PLACE, new DebugItemEndpoint.Position(8.5D, 64D, 8.5D, 0F),
      new DebugItemEndpoint.PlacementContext(8, 63, 8, DebugItemEndpoint.PlacementFace.UP))
    val session = new TestSession(request, world)
    def retain(): Unit = scope.retainControlled(data.tag, data.receipt, request, session, world, body, ticket, 1L)
    def success(): IoCompletion[MySqlOwnedIoReply] = data.completion

  private final class TestSession(expected: DebugItemEndpoint.Request, world: PodWorld) extends MainControlledPublicationSession:
    var failure: Throwable = null
    override def assertCurrent(request: com.wi.floatingpod.debug.MainPodOperationRequest, expectedWorld: PodWorld): Unit =
      if failure != null then throw failure
      require(request.playerRequest() == expected && (expectedWorld eq world), "exact controlled session request/world")

  private final class TestBody(receipt: UnboundPreparationReceipt) extends ControlledActivePodEntity:
    var pose = receipt.targetPose
    var dead = false
    var applyCalls = 0
    var applyFailure: Throwable = null
    var faultFailure: Throwable = null
    override def podUuid: UUID = receipt.podUuid
    override def entityId: Int = 77
    override def actualPose: FloatingPodPose = pose
    override def isFrozen: Boolean = true
    override def sameBody(other: ControlledActivePodEntity): Boolean = other eq this
    override def hasControlledCreationStamp(other: UnboundPreparationReceipt): Boolean = other == receipt
    override def applyCommittedControl(state: FloatingPodState, other: UnboundPreparationReceipt): Unit =
      if applyFailure != null then throw applyFailure
      require(other == receipt && state.uuid == receipt.podUuid && state.isActive, "actual graph identity")
      applyCalls += 1
    override def applyCommittedMode(state: FloatingPodState, attempt: UUID): Unit = throw new AssertionError("mode path not authorized")
    override def markControlFaulted(cause: Throwable): Unit = if faultFailure != null then throw faultFailure

  private final class TestWorld(initial: ControlledActivePodEntity) extends PodWorld:
    var indexed: ControlledActivePodEntity = initial
    var idIndexed: ControlledActivePodEntity = initial
    var indexFailure: Throwable = null
    var findCalls = 0
    var removeCalls = 0
    override def dimensionId: Int = 7
    override def diagnosticWorldName: String = "publication-world"
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(uuid: UUID): Option[ActivePodEntity] =
      findCalls += 1
      Option(indexed).filter(_.podUuid == uuid)
    override def assertLiveIndexedBody(body: ControlledActivePodEntity): Unit =
      if indexFailure != null then throw indexFailure
      body match
        case test: TestBody => require(!test.dead && (indexed eq body) && (idIndexed eq body), "actual stand-in live dual-index body differs")
        case _ => throw new IllegalArgumentException("foreign test body")
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new AssertionError("publication must not spawn")
    override def removePod(entity: ActivePodEntity): Unit = { removeCalls += 1; throw new AssertionError("publication must not delete") }

  private final class TestGateway extends ChunkTicketGateway:
    private final class TestTicket extends ChunkTicket
    val chunkSet = mutable.HashSet.empty[(Int, Int)]
    var uuid = Option.empty[UUID]
    var releaseCalls = 0
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = Some(new TestTicket)
    override def release(ticket: ChunkTicket): Unit = { releaseCalls += 1 }
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, value: UUID): Unit = uuid = Some(value)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = uuid
    override def force(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunkSet.add((x, z)); () }
    override def unforce(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunkSet.remove((x, z)); () }
    override def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] = chunkSet.toSet
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class NoMainRepository extends FloatingPodRepository:
    var calls = 0
    private def forbidden[A](): A = { calls += 1; throw new AssertionError("Main repository access") }
    override def insert(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def update(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = forbidden()
    override def findByOwner(uuid: UUID): Try[List[FloatingPodState]] = forbidden()
    override def findAllActive(): Try[List[FloatingPodState]] = forbidden()
    override def findAll(): Try[List[FloatingPodState]] = forbidden()
    override def delete(uuid: UUID): Try[Boolean] = forbidden()
    override def transaction[T](fn: => T): Try[T] = forbidden()
    override def close(): Unit = forbidden()
