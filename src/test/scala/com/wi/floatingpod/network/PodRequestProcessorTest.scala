package com.wi.floatingpod.network

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ActivePodEntity, AuthoritativePosition, ChunkLoadingManager, ChunkTicket, ChunkTicketGateway, OwnershipValidator, PodWorld, PositionValidation, PositionValidator, ReleasePosition, ReleasePositionPolicy}
import com.wi.floatingpod.storage.{FloatingPodRepository, PlayerEntitlementStore, RecoveryDimensionBackfillOutcome, RecoveryPodStore, StoredPodRow}

import net.minecraft.util.math.BlockPos
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** M4 request processor 的公开行为 tracer。 */
class PodRequestProcessorTest:
  @Test
  def wrongDimensionRejectsBeforeRecallMutation(): Unit =
    val pod = state(active = true)
    val lifecycle = new TraceLifecycle(sameDimension = false)
    val outcome = processorForDimensionRejection(pod, lifecycle).recall(new C2SRecallRequest(pod.uuid, pod.stateVersion), player)

    assertFalse(outcome.result.success)
    assertEquals(Some(RecallFailureReason.WrongDimension), outcome.result.reason)
    assertEquals(0, lifecycle.recallCalls)

  @Test
  def unknownPodRejectsBeforeAnyRecallMutation(): Unit =
    val fixture = recallFixture(repositoryResult = Success(None))

    assertRecallRejected(fixture.processor.recall(recallRequest, player), RecallFailureReason.UnknownPod, fixture, List("ownership"))

  @Test
  def wrongOwnerRejectsBeforeAnyRecallMutation(): Unit =
    val fixture = recallFixture(initialState = state(owner = otherPlayerId, active = true))

    assertRecallRejected(fixture.processor.recall(recallRequest, player), RecallFailureReason.NotOwner, fixture, List("ownership"))

  @Test
  def staleVersionRejectsBeforeAnyRecallMutation(): Unit =
    val fixture = recallFixture(initialState = state(version = 8L, active = true))

    assertRecallRejected(fixture.processor.recall(recallRequest, player), RecallFailureReason.VersionMismatch, fixture, List("ownership"))

  @Test
  def inactivePodRejectsBeforeDimensionOrRecallMutation(): Unit =
    val fixture = recallFixture(initialState = state(active = false))

    assertRecallRejected(fixture.processor.recall(recallRequest, player), RecallFailureReason.NotActive, fixture, List("ownership"))

  @Test
  def wrongDimensionRejectsBeforeRecallMutationWithOneBoundedLog(): Unit =
    val fixture = recallFixture(sameDimension = false)

    assertRecallRejected(fixture.processor.recall(recallRequest, player), RecallFailureReason.WrongDimension, fixture, List("ownership", "dimension"))

  @Test
  def dimensionQueryFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("dimension secret must not reach clients")
    val fixture = recallFixture(dimensionResult = Failure(cause))

    assertRecallInfrastructureFailure(fixture.processor.recall(recallRequest, player), cause, fixture, List("ownership", "dimension"), expectedRecallCalls = 0)

  @Test
  def recallFailureMapsOnceToInfrastructureFailureWithoutMutation(): Unit =
    val cause = new IllegalStateException("recall secret must not reach clients")
    val fixture = recallFixture(recallResult = Failure(cause))

    assertRecallInfrastructureFailure(fixture.processor.recall(recallRequest, player), cause, fixture, List("ownership", "dimension", "recall"), expectedRecallCalls = 1)

  @Test
  def successfulRecallUsesOnlyLifecycleAuthoritativeStateForResultAndSync(): Unit =
    val authoritative = state(version = 42L, x = 19.5D, y = 72.0D, z = -6.25D, yaw = -50.0F, pitch = 12.0F, active = false)
    val fixture = recallFixture(recallResult = Success(PodLifecycleSnapshot(authoritative)))

    val outcome = fixture.processor.recall(recallRequest, player)

    assertTrue(outcome.result.success)
    assertEquals(authoritative.uuid, outcome.result.podUuid)
    assertEquals(Some(42L), outcome.result.newStateVersion)
    assertEquals(None, outcome.result.reason)
    assertAuthoritativeSync(outcome.sync, authoritative)
    assertEquals(List("ownership", "dimension", "recall"), fixture.trace.toList)
    assertEquals(1, fixture.lifecycle.recallCalls)
    assertEquals(1, fixture.lifecycle.recallMutations)
    assertEquals(List(LogSuccess("recall", playerId, authoritative.uuid, 42L)), fixture.log.events.toList)

  @Test
  def successfulRecallRunsTheWholeAuthoritativeValidationOrderExactlyOnce(): Unit =
    val fixture = recallFixture()

    val outcome = fixture.processor.recall(recallRequest, player)

    assertTrue(outcome.result.success)
    assertEquals(List("ownership", "dimension", "recall"), fixture.trace.toList)
    assertEquals(1, fixture.lifecycle.recallCalls)
    assertEquals(1, fixture.lifecycle.recallMutations)
    assertEquals(1, fixture.log.events.size)

  @Test
  def throwingRecallRequestLogPropagatesWithoutASecondMappedLogAttempt(): Unit =
    val log = new ThrowingLog()
    val fixture = recallFixture(repositoryResult = Success(None), log = log)

    assertThrows(classOf[IllegalStateException], () => fixture.processor.recall(recallRequest, player))
    assertEquals(1, log.rejectionCalls)
    assertEquals(0, log.infrastructureCalls)
    assertEquals(0, fixture.lifecycle.recallCalls)

  @Test
  def recallRejectsNullOrInvalidRequestFieldsBeforeDependencies(): Unit =
    val fixture = recallFixture()

    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.recall(null, player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.recall(new C2SRecallRequest(null, 3L), player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.recall(new C2SRecallRequest(podId, -1L), player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.recall(recallRequest, null))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.recall(recallRequest, new TestPlayer(null)))
    assertEquals(Nil, fixture.trace.toList)
    assertEquals(Nil, fixture.log.events.toList)

  @Test
  def unknownPodRejectsBeforeAnyReleaseMutation(): Unit =
    val fixture = releaseFixture(repositoryResult = Success(None))

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.UnknownPod, fixture, List("ownership"))

  @Test
  def wrongOwnerRejectsBeforeAnyReleaseMutation(): Unit =
    val fixture = releaseFixture(initialState = state(owner = otherPlayerId))

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.NotOwner, fixture, List("ownership"))

  @Test
  def staleVersionRejectsBeforeAnyReleaseMutation(): Unit =
    val fixture = releaseFixture(initialState = state(version = 8L))

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.VersionMismatch, fixture, List("ownership"))

  @Test
  def alreadyActiveRejectsBeforeCapacityOrReleaseMutation(): Unit =
    val fixture = releaseFixture(initialState = state(active = true))

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.AlreadyActive, fixture, List("ownership"))

  @Test
  def reachedCapacityRejectsBeforePositionOrReleaseMutation(): Unit =
    val fixture = releaseFixture(capacityResult = false)

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.CapacityReached, fixture, List("ownership", "capacity"))

  @Test
  def invalidPositionRejectsBeforeEntitlementOrReleaseMutation(): Unit =
    val fixture = releaseFixture(positionResult = PositionValidation.OutsideReach)

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.InvalidPosition, fixture, List("ownership", "capacity", "position"))

  @Test
  def deniedEntitlementRejectsBeforeReleaseMutation(): Unit =
    val fixture = releaseFixture(entitlementResult = false)

    assertRejected(fixture.processor.release(request, player), ReleaseFailureReason.EntitlementDenied, fixture, List("ownership", "capacity", "position", "entitlement"))

  @Test
  def repositoryFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("repository secret must not reach clients")
    val fixture = releaseFixture(repositoryResult = Failure(cause))

    assertInfrastructureFailure(fixture.processor.release(request, player), cause, fixture, List("ownership"), expectedReleaseCalls = 0)

  @Test
  def capacityFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("capacity failed")
    val fixture = releaseFixture(capacityResult = throw cause)

    assertInfrastructureFailure(fixture.processor.release(request, player), cause, fixture, List("ownership", "capacity"), expectedReleaseCalls = 0)

  @Test
  def positionFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("position failed")
    val fixture = releaseFixture(positionResult = throw cause)

    assertInfrastructureFailure(fixture.processor.release(request, player), cause, fixture, List("ownership", "capacity", "position"), expectedReleaseCalls = 0)

  @Test
  def playerEnvironmentFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("environment failed")
    val fixture = releaseFixture()
    val failingPlayer = new TestPlayer(playerId, _ => throw cause)

    assertInfrastructureFailure(fixture.processor.release(request, failingPlayer), cause, fixture, List("ownership", "capacity"), expectedReleaseCalls = 0)

  @Test
  def entitlementFailureMapsOnceToInfrastructureFailureWithOriginalCause(): Unit =
    val cause = new IllegalStateException("entitlement failed")
    val fixture = releaseFixture(entitlementResult = throw cause)

    assertInfrastructureFailure(fixture.processor.release(request, player), cause, fixture, List("ownership", "capacity", "position", "entitlement"), expectedReleaseCalls = 0)

  @Test
  def lifecycleFailureMapsOnceToInfrastructureFailureWithoutMutation(): Unit =
    val cause = new IllegalStateException("ticket secret must not reach clients")
    val fixture = releaseFixture(lifecycleResult = Failure(cause))

    assertInfrastructureFailure(fixture.processor.release(request, player), cause, fixture, List("ownership", "capacity", "position", "entitlement", "release"), expectedReleaseCalls = 1)

  @Test
  def fatalLifecycleErrorPropagatesWithoutInfrastructureMappingOrAudit(): Unit =
    val trace = mutable.ListBuffer.empty[String]
    val log = new RecordingLog()
    val lifecycle = new FatalLifecycle(new TestFatalError())
    val processor = new PodRequestProcessor(
      new OwnershipValidator(new Repository(Success(Some(state())), trace)),
      new TraceCapacityPolicy(true, trace),
      new TracePositionPolicy(PositionValidation.Accepted, trace),
      new TraceEntitlementPolicy(true, trace),
      lifecycle,
      log
    )

    assertThrows(classOf[TestFatalError], () => processor.release(request, player))
    assertEquals(List("ownership", "capacity", "position", "entitlement"), trace.toList)
    assertEquals(Nil, log.events.toList)

  @Test
  def successfulReleaseUsesOnlyLifecycleAuthoritativeStateForResultAndSync(): Unit =
    val authoritative = state(version = 41L, x = 17.25D, y = 70.5D, z = -4.75D, yaw = 35.0F, pitch = -10.0F, active = true)
    val fixture = releaseFixture(lifecycleResult = Success(PodLifecycleSnapshot(authoritative)))

    val outcome = fixture.processor.release(request, player)

    assertTrue(outcome.result.success)
    assertEquals(authoritative.uuid, outcome.result.podUuid)
    assertEquals(Some(41L), outcome.result.newStateVersion)
    assertEquals(None, outcome.result.reason)
    val sync = outcome.sync.getOrElse(throw new AssertionError("Expected authoritative state sync"))
    assertEquals(authoritative.uuid, sync.podUuid)
    assertEquals(17.25D, sync.x)
    assertEquals(70.5D, sync.y)
    assertEquals(-4.75D, sync.z)
    assertEquals(35.0F, sync.yaw)
    assertEquals(-10.0F, sync.pitch)
    assertEquals(41L, sync.stateVersion)
    assertEquals(List("ownership", "capacity", "position", "entitlement", "release"), fixture.trace.toList)
    assertEquals(1, fixture.lifecycle.releaseCalls)
    assertEquals(1, fixture.lifecycle.releaseMutations)
    assertEquals(List(LogSuccess("release", playerId, authoritative.uuid, 41L)), fixture.log.events.toList)

  @Test
  def successfulReleaseRunsTheWholeAuthoritativeValidationOrderExactlyOnce(): Unit =
    val fixture = releaseFixture()

    val outcome = fixture.processor.release(request, player)

    assertTrue(outcome.result.success)
    assertEquals(List("ownership", "capacity", "position", "entitlement", "release"), fixture.trace.toList)
    assertEquals(1, fixture.lifecycle.releaseCalls)
    assertEquals(1, fixture.lifecycle.releaseMutations)
    assertEquals(1, fixture.log.events.size)

  @Test
  def persistedEntitlementDefaultsWithoutWritingAndCountsRawOwnerDesiredActiveRows(): Unit =
    val store = new EntitlementStore(None, Nil)
    val policy = new PersistedPlayerEntitlementPolicy(store, store)

    assertTrue(policy.mayRelease(player, state()))
    assertEquals(1, store.entitlementReads)
    assertEquals(1, store.activeReads)

    store.rows = List(desiredRow(playerId, dimensionId = None))
    assertFalse(policy.mayRelease(player, state()))

  @Test
  def persistedEntitlementRejectsAtTheConfiguredOwnerLimitAndPreservesStoreFailures(): Unit =
    val store = new EntitlementStore(Some(PlayerEntitlement(playerId, 99, 2)), List(desiredRow(playerId, Some(0))))
    val policy = new PersistedPlayerEntitlementPolicy(store, store)

    assertTrue(policy.mayRelease(player, state()))
    store.rows = store.rows :+ desiredRow(playerId, Some(0))
    assertFalse(policy.mayRelease(player, state()))
    store.rows = Nil
    store.entitlement = Some(PlayerEntitlement(playerId, 1, 0))
    assertFalse(policy.mayRelease(player, state()))
    store.entitlement = Some(PlayerEntitlement(otherPlayerId, 1, 2))
    assertThrows(classOf[IllegalStateException], () => policy.mayRelease(player, state()))
    val cause = new IllegalStateException("entitlement read failed")
    store.failure = Some(cause)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => policy.mayRelease(player, state())))

  @Test
  def typedLateLifecycleRejectionsMapToBoundedResultsWithoutSync(): Unit =
    val globalFixture = typedProcessor(
      ReleaseLifecycleOutcome.Rejected(ReleaseFailureReason.CapacityReached),
      RecallLifecycleOutcome.Rejected(RecallFailureReason.VersionMismatch),
      active = false
    )
    val global = globalFixture.processor.release(request, player)
    assertEquals(Some(ReleaseFailureReason.CapacityReached), global.result.reason)
    assertEquals(None, global.sync)
    assertEquals(1, globalFixture.lifecycle.releaseOutcomeCalls)
    assertEquals(List(LogRejection("release", playerId, podId, ReleaseFailureReason.CapacityReached)), globalFixture.log.events.toList)

    val playerFixture = typedProcessor(
      ReleaseLifecycleOutcome.Rejected(ReleaseFailureReason.EntitlementDenied),
      RecallLifecycleOutcome.Rejected(RecallFailureReason.VersionMismatch),
      active = false
    )
    val playerLimit = playerFixture.processor.release(request, player)
    assertEquals(Some(ReleaseFailureReason.EntitlementDenied), playerLimit.result.reason)
    assertEquals(None, playerLimit.sync)
    assertEquals(1, playerFixture.lifecycle.releaseOutcomeCalls)
    assertEquals(List(LogRejection("release", playerId, podId, ReleaseFailureReason.EntitlementDenied)), playerFixture.log.events.toList)

    val recallFixture = typedProcessor(
      ReleaseLifecycleOutcome.Rejected(ReleaseFailureReason.EntitlementDenied),
      RecallLifecycleOutcome.Rejected(RecallFailureReason.VersionMismatch),
      active = true
    )
    val recall = recallFixture.processor.recall(recallRequest, player)
    assertEquals(Some(RecallFailureReason.VersionMismatch), recall.result.reason)
    assertEquals(None, recall.sync)
    assertEquals(1, recallFixture.lifecycle.recallOutcomeCalls)
    assertEquals(List(LogRejection("recall", playerId, podId, RecallFailureReason.VersionMismatch)), recallFixture.log.events.toList)

  @Test
  def throwingRequestLogPropagatesWithoutASecondMappedLogAttempt(): Unit =
    val log = new ThrowingLog()
    val fixture = releaseFixture(repositoryResult = Success(None), log = log)

    assertThrows(classOf[IllegalStateException], () => fixture.processor.release(request, player))
    assertEquals(1, log.rejectionCalls)
    assertEquals(0, log.infrastructureCalls)
    assertEquals(0, fixture.lifecycle.releaseCalls)

  @Test
  def releaseRejectsNullOrInvalidRequestFieldsBeforeDependencies(): Unit =
    val fixture = releaseFixture()

    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.release(null, player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.release(new C2SReleaseRequest(null, target, 3L), player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.release(new C2SReleaseRequest(podId, null, 3L), player))
    assertThrows(classOf[IllegalArgumentException], () => fixture.processor.release(new C2SReleaseRequest(podId, target, -1L), player))
    assertEquals(Nil, fixture.trace.toList)
    assertEquals(Nil, fixture.log.events.toList)

  private val podId = UUID.fromString("00000000-0000-0000-0000-000000000001")
  private val playerId = UUID.fromString("00000000-0000-0000-0000-000000000002")
  private val otherPlayerId = UUID.fromString("00000000-0000-0000-0000-000000000003")
  private val target = new BlockPos(3, 64, 2)
  private val request = new C2SReleaseRequest(podId, target, 3L)
  private val recallRequest = new C2SRecallRequest(podId, 3L)
  private val player = new TestPlayer(playerId)

  private def processorForDimensionRejection(pod: FloatingPodState, lifecycle: TraceLifecycle): PodRequestProcessor =
    new PodRequestProcessor(
      new OwnershipValidator(new Repository(Success(Some(pod)), mutable.ListBuffer.empty)),
      new ActiveCapacityPolicy(new EmptyRecoveryStore, 1),
      new PositionValidator(),
      AllowAllPlayerEntitlementPolicy,
      lifecycle,
      new RecordingLog()
    )

  private def releaseFixture(
      initialState: FloatingPodState = state(),
      repositoryResult: Try[Option[FloatingPodState]] = null,
      capacityResult: => Boolean = true,
      positionResult: => PositionValidation = PositionValidation.Accepted,
      entitlementResult: => Boolean = true,
      lifecycleResult: Try[PodLifecycleSnapshot] = null,
      log: RecordingLog = null
  ): ReleaseFixture =
    val trace = mutable.ListBuffer.empty[String]
    val resolvedRepository = if repositoryResult == null then Success(Some(initialState)) else repositoryResult
    val resolvedLifecycle = if lifecycleResult == null then Success(PodLifecycleSnapshot(state(version = 4L, active = true))) else lifecycleResult
    val resolvedLog = if log == null then new RecordingLog() else log
    val lifecycle = new TraceLifecycle(true, resolvedLifecycle, trace)
    val processor = new PodRequestProcessor(
      new OwnershipValidator(new Repository(resolvedRepository, trace)),
      new TraceCapacityPolicy(capacityResult, trace),
      new TracePositionPolicy(positionResult, trace),
      new TraceEntitlementPolicy(entitlementResult, trace),
      lifecycle,
      resolvedLog
    )
    ReleaseFixture(processor, lifecycle, resolvedLog, trace)

  private def recallFixture(
      initialState: FloatingPodState = state(active = true),
      repositoryResult: Try[Option[FloatingPodState]] = null,
      sameDimension: Boolean = true,
      dimensionResult: Try[Boolean] = null,
      recallResult: Try[PodLifecycleSnapshot] = null,
      log: RecordingLog = null
  ): RecallFixture =
    val trace = mutable.ListBuffer.empty[String]
    val resolvedRepository = if repositoryResult == null then Success(Some(initialState)) else repositoryResult
    val resolvedDimension = if dimensionResult == null then Success(sameDimension) else dimensionResult
    val resolvedRecall = if recallResult == null then Success(PodLifecycleSnapshot(state(version = 4L, active = false))) else recallResult
    val resolvedLog = if log == null then new RecordingLog() else log
    val lifecycle = new TraceLifecycle(sameDimension, trace = trace, dimensionResult = resolvedDimension, recallResult = resolvedRecall)
    val processor = new PodRequestProcessor(
      new OwnershipValidator(new Repository(resolvedRepository, trace)),
      new TraceCapacityPolicy(true, trace),
      new TracePositionPolicy(PositionValidation.Accepted, trace),
      new TraceEntitlementPolicy(true, trace),
      lifecycle,
      resolvedLog
    )
    RecallFixture(processor, lifecycle, resolvedLog, trace)

  private def assertRejected(
      outcome: ReleaseProcessingOutcome,
      reason: ReleaseFailureReason,
      fixture: ReleaseFixture,
      expectedTrace: List[String]
  ): Unit =
    assertFalse(outcome.result.success)
    assertEquals(Some(reason), outcome.result.reason)
    assertEquals(None, outcome.sync)
    assertEquals(expectedTrace, fixture.trace.toList)
    assertEquals(0, fixture.lifecycle.releaseCalls)
    assertEquals(0, fixture.lifecycle.releaseMutations)
    assertEquals(List(LogRejection("release", playerId, podId, reason)), fixture.log.events.toList)

  private def assertInfrastructureFailure(
      outcome: ReleaseProcessingOutcome,
      cause: Throwable,
      fixture: ReleaseFixture,
      expectedTrace: List[String],
      expectedReleaseCalls: Int
  ): Unit =
    assertFalse(outcome.result.success)
    assertEquals(Some(ReleaseFailureReason.InfrastructureFailure), outcome.result.reason)
    assertEquals(None, outcome.sync)
    assertEquals(expectedTrace, fixture.trace.toList)
    assertEquals(expectedReleaseCalls, fixture.lifecycle.releaseCalls)
    assertEquals(0, fixture.lifecycle.releaseMutations)
    assertEquals(1, fixture.log.events.size)
    fixture.log.events.head match
      case LogInfrastructure("release", auditedPlayer, uuid, actualCause) =>
        assertEquals(playerId, auditedPlayer)
        assertEquals(podId, uuid)
        assertSame(cause, actualCause)
      case unexpected => throw new AssertionError(s"Expected infrastructure log but got $unexpected")

  private def assertRecallRejected(
      outcome: RecallProcessingOutcome,
      reason: RecallFailureReason,
      fixture: RecallFixture,
      expectedTrace: List[String]
  ): Unit =
    assertFalse(outcome.result.success)
    assertEquals(Some(reason), outcome.result.reason)
    assertEquals(None, outcome.sync)
    assertEquals(expectedTrace, fixture.trace.toList)
    assertEquals(0, fixture.lifecycle.recallCalls)
    assertEquals(0, fixture.lifecycle.recallMutations)
    assertEquals(List(LogRejection("recall", playerId, podId, reason)), fixture.log.events.toList)

  private def assertRecallInfrastructureFailure(
      outcome: RecallProcessingOutcome,
      cause: Throwable,
      fixture: RecallFixture,
      expectedTrace: List[String],
      expectedRecallCalls: Int
  ): Unit =
    assertFalse(outcome.result.success)
    assertEquals(Some(RecallFailureReason.InfrastructureFailure), outcome.result.reason)
    assertEquals(None, outcome.sync)
    assertEquals(expectedTrace, fixture.trace.toList)
    assertEquals(expectedRecallCalls, fixture.lifecycle.recallCalls)
    assertEquals(0, fixture.lifecycle.recallMutations)
    assertEquals(1, fixture.log.events.size)
    fixture.log.events.head match
      case LogInfrastructure("recall", auditedPlayer, uuid, actualCause) =>
        assertEquals(playerId, auditedPlayer)
        assertEquals(podId, uuid)
        assertSame(cause, actualCause)
      case unexpected => throw new AssertionError(s"Expected recall infrastructure log but got $unexpected")

  private def assertAuthoritativeSync(sync: Option[S2CStateSync], authoritative: FloatingPodState): Unit =
    val received = sync.getOrElse(throw new AssertionError("Expected authoritative state sync"))
    assertEquals(authoritative.uuid, received.podUuid)
    assertEquals(authoritative.posX, received.x)
    assertEquals(authoritative.posY, received.y)
    assertEquals(authoritative.posZ, received.z)
    assertEquals(authoritative.yaw, received.yaw)
    assertEquals(authoritative.pitch, received.pitch)
    assertEquals(authoritative.stateVersion, received.stateVersion)

  private def state(
      owner: UUID = playerId,
      version: Long = 3L,
      x: Double = 0.0D,
      y: Double = 64.0D,
      z: Double = 0.0D,
      yaw: Float = 0.0F,
      pitch: Float = 0.0F,
      active: Boolean = false
  ): FloatingPodState =
    FloatingPodState(podId, Some(owner), version, "overworld", x, y, z, yaw, pitch, active, 0L, if active then Some(0) else None)

  private def desiredRow(owner: UUID, dimensionId: Option[Int]): StoredPodRow =
    StoredPodRow(UUID.randomUUID(), Some(owner), 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, isActive = true, 0L, dimensionId)

  private def typedProcessor(release: ReleaseLifecycleOutcome, recall: RecallLifecycleOutcome, active: Boolean): TypedFixture =
    val lifecycle = new TypedLifecycle(release, recall)
    val log = new RecordingLog()
    val processor = new PodRequestProcessor(
      new OwnershipValidator(new Repository(Success(Some(state(active = active))), mutable.ListBuffer.empty)),
      new TraceCapacityPolicy(true, mutable.ListBuffer.empty),
      new TracePositionPolicy(PositionValidation.Accepted, mutable.ListBuffer.empty),
      AllowAllPlayerEntitlementPolicy,
      lifecycle,
      log
    )
    TypedFixture(processor, lifecycle, log)

private final case class ReleaseFixture(
    processor: PodRequestProcessor,
    lifecycle: TraceLifecycle,
    log: RecordingLog,
    trace: mutable.ListBuffer[String]
)

private final case class RecallFixture(
    processor: PodRequestProcessor,
    lifecycle: TraceLifecycle,
    log: RecordingLog,
    trace: mutable.ListBuffer[String]
)

private final case class TypedFixture(processor: PodRequestProcessor, lifecycle: TypedLifecycle, log: RecordingLog)

private final class TestPlayer(val playerUuid: UUID, solidSupport: ReleasePosition => Boolean = _ => true) extends ServerPlayerContext:
  override val position = AuthoritativePosition(0.0D, 64.0D, 0.0D)
  override val maximumReach = 8.0D
  override def spawnWorld: PodWorld = throw new UnsupportedOperationException("Test player has no spawn world")
  override def hasSolidSupport(target: ReleasePosition): Boolean = solidSupport(target)
  override def hasTwoBlockHeadroom(target: ReleasePosition): Boolean = true
  override def isSameDimension(activePod: ActivePodEntity): Boolean = true

private final class TraceLifecycle(
    sameDimension: Boolean,
    releaseResult: Try[PodLifecycleSnapshot] = Success(PodLifecycleSnapshot(FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000001"), None, 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, false, 0L))),
    trace: mutable.ListBuffer[String] = mutable.ListBuffer.empty,
    dimensionResult: Try[Boolean] = null,
    recallResult: Try[PodLifecycleSnapshot] = null
) extends PodLifecyclePort:
  var recallCalls = 0
  var releaseCalls = 0
  var releaseMutations = 0
  var recallMutations = 0
  private val resolvedDimension = if dimensionResult == null then Success(sameDimension) else dimensionResult
  private val resolvedRecall = if recallResult == null then Success(PodLifecycleSnapshot(FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000001"), None, 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, false, 0L))) else recallResult
  override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] =
    trace += "dimension"
    resolvedDimension
  override def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] =
    trace += "release"
    releaseCalls += 1
    releaseResult match
      case Success(snapshot) =>
        releaseMutations += 1
        Success(ReleaseLifecycleOutcome.Accepted(snapshot))
      case Failure(cause) => Failure(cause)

  override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] =
    trace += "recall"
    recallCalls += 1
    resolvedRecall match
      case Success(snapshot) =>
        recallMutations += 1
        Success(RecallLifecycleOutcome.Accepted(snapshot))
      case Failure(cause) => Failure(cause)

private final class TypedLifecycle(release: ReleaseLifecycleOutcome, recall: RecallLifecycleOutcome) extends PodLifecyclePort:
  var releaseOutcomeCalls = 0
  var recallOutcomeCalls = 0
  override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] = Success(true)
  override def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] =
    releaseOutcomeCalls += 1
    Success(release)
  override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] =
    recallOutcomeCalls += 1
    Success(recall)

private final class TraceCapacityPolicy(result: => Boolean, trace: mutable.ListBuffer[String]) extends ReleaseCapacityPolicy:
  override def canRelease: Boolean =
    trace += "capacity"
    result

private final class TracePositionPolicy(result: => PositionValidation, trace: mutable.ListBuffer[String]) extends ReleasePositionPolicy:
  override def validate(
      player: AuthoritativePosition,
      target: ReleasePosition,
      maximumReach: Double,
      hasSolidSupport: Boolean,
      hasTwoBlockHeadroom: Boolean
  ): PositionValidation =
    trace += "position"
    result

private final class TraceEntitlementPolicy(result: => Boolean, trace: mutable.ListBuffer[String]) extends PlayerEntitlementPolicy:
  override def mayRelease(player: ServerPlayerContext, state: FloatingPodState): Boolean =
    trace += "entitlement"
    result

private sealed trait LogEvent
private final case class LogRejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason) extends LogEvent
private final case class LogSuccess(operation: String, playerUuid: UUID, podUuid: UUID, version: Long) extends LogEvent
private final case class LogInfrastructure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable) extends LogEvent

private class RecordingLog extends RequestLog:
  val events = mutable.ListBuffer.empty[LogEvent]
  override def rejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason): Unit = events += LogRejection(operation, playerUuid, podUuid, reason)
  override def success(operation: String, playerUuid: UUID, podUuid: UUID, authoritativeVersion: Long): Unit = events += LogSuccess(operation, playerUuid, podUuid, authoritativeVersion)
  override def infrastructureFailure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable): Unit = events += LogInfrastructure(operation, playerUuid, podUuid, cause)

private final class ThrowingLog extends RecordingLog:
  var rejectionCalls = 0
  var infrastructureCalls = 0
  override def rejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason): Unit =
    rejectionCalls += 1
    throw new IllegalStateException("log failure")
  override def infrastructureFailure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable): Unit = infrastructureCalls += 1

private final class TestFatalError extends LinkageError

private final class FatalLifecycle(problem: Error) extends PodLifecyclePort:
  override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] = Success(true)
  override def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] = throw problem
  override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] = throw problem

private final class NoopGateway extends ChunkTicketGateway:
  override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
  override def request(world: PodWorld): Option[ChunkTicket] = None
  override def release(ticket: ChunkTicket): Unit = ()
  override def maxChunkDepth(ticket: ChunkTicket): Int = 25
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ()
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] = None
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new UnsupportedOperationException()
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

private final class EmptyRecoveryStore extends RecoveryPodStore:
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] = Success(Nil)
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(None)
  override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = throw new UnsupportedOperationException
  override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(Nil)

private final class EntitlementStore(
    var entitlement: Option[PlayerEntitlement],
    var rows: List[StoredPodRow]
) extends PlayerEntitlementStore, RecoveryPodStore:
  var failure: Option[Throwable] = None
  var entitlementReads = 0
  var activeReads = 0
  override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] =
    entitlementReads += 1
    failure.map(Failure(_)).getOrElse(Success(entitlement))
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] =
    activeReads += 1
    Success(rows)
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(None)
  override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = throw new UnsupportedOperationException
  override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(Nil)

private final class Repository(result: Try[Option[FloatingPodState]], trace: mutable.ListBuffer[String]) extends FloatingPodRepository:
  override def insert(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def update(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] =
    trace += "ownership"
    result
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(Nil)
  override def findAllActive(): Try[List[FloatingPodState]] = Success(Nil)
  override def findAll(): Try[List[FloatingPodState]] = Success(Nil)
  override def delete(uuid: UUID): Try[Boolean] = Success(false)
  override def transaction[T](fn: => T): Try[T] = Try(fn)
  override def close(): Unit = ()
