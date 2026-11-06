package com.wi.floatingpod.network

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ActivePodEntity, AuthoritativePosition, ChunkLoadingManager, ChunkTicket, ChunkTicketGateway, OwnershipValidator, PodWorld, PositionValidator, ReleasePosition}
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore}

import net.minecraft.util.math.BlockPos
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import net.minecraftforge.fml.common.network.simpleimpl.{ControlledMessageContext, IMessage, MessageContext}

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import io.netty.buffer.Unpooled

import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** M5A 的 server runtime、Forge-facing seam 与实际 lifecycle adapter 行为 tracer。 */
class ServerPacketAdaptersTest:
  private val playerId = UUID.fromString("00000000-0000-0000-0000-000000000101")
  private val podId = UUID.fromString("00000000-0000-0000-0000-000000000202")
  private val recoveryAttemptId = UUID.fromString("00000000-0000-0000-0000-000000000303")

  @Test
  def dynamicRuntimeFailsClosedAfterUninstallAndRejectsDuplicateInstall(): Unit =
    ServerPacketRuntime.uninstall()
    val endpoint = new RecordingEndpoint()
    ServerPacketRuntime.install(endpoint)
    assertThrows(classOf[IllegalStateException], () => ServerPacketRuntime.install(new RecordingEndpoint()))
    ServerPacketRuntime.receiveRecall(new C2SRecallRequest(podId, 0L), null)
    assertEquals(1, endpoint.recallCalls)

    ServerPacketRuntime.uninstall()
    assertThrows(classOf[IllegalStateException], () => ServerPacketRuntime.receiveRecall(new C2SRecallRequest(podId, 0L), null))

  @Test
  def resolverFactoryPassesTheScheduledContextIdentityToItsResolver(): Unit =
    val scheduled = new ControlledMessageContext()
    val identity = new Object()
    var seenContext: MessageContext = null
    var seenIdentity: Object = null
    val player = new TestPlayerContext(playerId, new TestWorld(9))
    val factory = new ResolvingServerPlayerContextFactory[Object](
      context => { seenContext = context; identity },
      resolved => { seenIdentity = resolved; player }
    )

    assertSame(player, factory.resolve(scheduled))
    assertSame(scheduled, seenContext)
    assertSame(identity, seenIdentity)

  @Test
  def releaseEndpointSendsBoundedResultThenAuthoritativeSyncToResolvedServerPlayer(): Unit =
    val authoritative = state(active = true, version = 7L, x = 12.0D)
    val player = new TestPlayerContext(playerId, new TestWorld(10))
    val context = new ControlledMessageContext()
    val factory = new CapturingFactory(player)
    val sender = new RecordingSender()
    val endpoint = new ForgeServerPacketEndpoint(factory, releaseProcessor(authoritative), sender)

    endpoint.receiveRelease(new C2SReleaseRequest(podId, new BlockPos(2, 64, 0), 3L), context)

    assertSame(context, factory.context)
    assertEquals(2, sender.messages.size)
    assertTrue(sender.messages.head.isInstanceOf[S2CReleaseResult])
    assertTrue(sender.messages(1).isInstanceOf[S2CStateSync])
    assertEquals(7L, sender.messages.head.asInstanceOf[S2CReleaseResult].newStateVersion.get)

  @Test
  def recallEndpointSendsBoundedResultThenAuthoritativeSyncToResolvedServerPlayer(): Unit =
    val authoritative = state(active = false, version = 8L, x = -4.0D)
    val player = new TestPlayerContext(playerId, new TestWorld(11))
    val sender = new RecordingSender()
    val endpoint = new ForgeServerPacketEndpoint(new CapturingFactory(player), recallProcessor(authoritative), sender)

    endpoint.receiveRecall(new C2SRecallRequest(podId, 3L), new ControlledMessageContext())

    assertEquals(2, sender.messages.size)
    assertTrue(sender.messages.head.isInstanceOf[S2CRecallResult])
    assertTrue(sender.messages(1).isInstanceOf[S2CStateSync])
    assertEquals(8L, sender.messages.head.asInstanceOf[S2CRecallResult].newStateVersion.get)

  @Test
  def processorRejectionSendsOnlyItsBoundedResultWithoutSync(): Unit =
    val player = new TestPlayerContext(playerId, new TestWorld(12))
    val sender = new RecordingSender()
    val endpoint = new ForgeServerPacketEndpoint(new CapturingFactory(player), rejectingReleaseProcessor(), sender)

    endpoint.receiveRelease(new C2SReleaseRequest(podId, new BlockPos(2, 64, 0), 3L), new ControlledMessageContext())

    assertEquals(1, sender.messages.size)
    assertTrue(sender.messages.head.isInstanceOf[S2CReleaseResult])
    assertFalse(sender.messages.head.asInstanceOf[S2CReleaseResult].success)

  @Test
  def senderFailurePropagatesAndPreventsTheOptionalSyncFromBeingSent(): Unit =
    val player = new TestPlayerContext(playerId, new TestWorld(13))
    val sender = new FailingSender()
    val endpoint = new ForgeServerPacketEndpoint(new CapturingFactory(player), releaseProcessor(state(active = true, version = 7L)), sender)

    assertThrows(classOf[IllegalStateException], () => endpoint.receiveRelease(
      new C2SReleaseRequest(podId, new BlockPos(2, 64, 0), 3L),
      new ControlledMessageContext()
    ))
    assertEquals(1, sender.attempts)

  @Test
  def sameReleaseEndpointProcessesNextLegalRequestAfterNonFatalDependencyFailure(): Unit =
    withActualRequestLog { (log, events) =>
      val cause = new IllegalStateException("private release dependency detail")
      val originalStack = cause.getStackTrace
      val authoritative = state(active = true, version = 17L, x = 12.25D)
        .copy(posY = 70.5D, posZ = -3.75D, yaw = 35F, pitch = -10F)
      val player = new TestPlayerContext(playerId, new TestWorld(18))
      val sender = new RecordingSender()
      var capacityCalls = 0
      val capacity = new ReleaseCapacityPolicy:
        override def canRelease: Boolean =
          capacityCalls += 1
          if capacityCalls == 1 then throw cause
          true
      var mutations = 0
      val lifecycle = new PodLifecyclePort:
        override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] = Success(true)
        override def releaseOutcome(state: FloatingPodState, target: ReleasePosition,
            player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] =
          mutations += 1
          Success(ReleaseLifecycleOutcome.Accepted(PodLifecycleSnapshot(authoritative)))
        override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] =
          throw new AssertionError("release request must not invoke recall")
      val processor = new PodRequestProcessor(new OwnershipValidator(new RequestRepository(Some(state(active = false)))),
        capacity, new PositionValidator(), AllowAllPlayerEntitlementPolicy, lifecycle, log)
      val endpoint = new ForgeServerPacketEndpoint(new CapturingFactory(player), processor, sender)
      endpoint.receiveRelease(new C2SReleaseRequest(podId, new BlockPos(2, 64, 0), 3L), new ControlledMessageContext())
      assertEquals(0, mutations)
      assertEquals(1, sender.messages.size, "failed request must send no partial sync")
      val failed = sender.messages.head.asInstanceOf[S2CReleaseResult]
      val encoded = Unpooled.buffer()
      try
        failed.toBytes(encoded)
        // Release: version + success + UUID + absent state version + present reason + reason ID.
        assertEquals(1 + 1 + 16 + 1 + 1 + 1, encoded.readableBytes())
        val decoded = new S2CReleaseResult()
        decoded.fromBytes(encoded)
        assertFalse(encoded.isReadable)
        assertFalse(decoded.success)
        assertEquals(podId, decoded.podUuid)
        assertEquals(None, decoded.newStateVersion)
        assertEquals(Some(ReleaseFailureReason.InfrastructureFailure), decoded.reason)
      finally encoded.release()
      assertOriginalRequestFailure(events.toVector, "release", cause, originalStack)

      endpoint.receiveRelease(new C2SReleaseRequest(podId, new BlockPos(3, 64, 1), 3L), new ControlledMessageContext())
      assertEquals(2, capacityCalls)
      assertEquals(1, mutations)
      assertEquals(3, sender.messages.size)
      val succeeded = sender.messages(1).asInstanceOf[S2CReleaseResult]
      assertTrue(succeeded.success)
      assertEquals(podId, succeeded.podUuid)
      assertEquals(Some(authoritative.stateVersion), succeeded.newStateVersion)
      assertEquals(None, succeeded.reason)
      assertExactSync(sender.messages(2), authoritative)
      assertOriginalRequestFailure(events.toVector, "release", cause, originalStack)
      assertEquals(Vector(Level.ERROR, Level.DEBUG), events.map(_.getLevel).toVector)
    }

  @Test
  def sameRecallEndpointProcessesNextLegalRequestAfterNonFatalDependencyFailure(): Unit =
    withActualRequestLog { (log, events) =>
      val cause = new IllegalStateException("private recall dependency detail")
      val originalStack = cause.getStackTrace
      val authoritative = state(active = false, version = 18L, x = -8.25D)
        .copy(posY = 72.5D, posZ = 6.75D, yaw = -40F, pitch = 15F)
      val player = new TestPlayerContext(playerId, new TestWorld(19))
      val sender = new RecordingSender()
      var dimensionCalls = 0
      var mutations = 0
      val lifecycle = new PodLifecyclePort:
        override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] =
          dimensionCalls += 1
          if dimensionCalls == 1 then Failure(cause) else Success(true)
        override def releaseOutcome(state: FloatingPodState, target: ReleasePosition,
            player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] =
          throw new AssertionError("recall request must not invoke release")
        override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] =
          mutations += 1
          Success(RecallLifecycleOutcome.Accepted(PodLifecycleSnapshot(authoritative)))
      val processor = new PodRequestProcessor(new OwnershipValidator(new RequestRepository(Some(state(active = true)))),
        new FixedCapacity(true), new PositionValidator(), AllowAllPlayerEntitlementPolicy, lifecycle, log)
      val endpoint = new ForgeServerPacketEndpoint(new CapturingFactory(player), processor, sender)
      endpoint.receiveRecall(new C2SRecallRequest(podId, 3L), new ControlledMessageContext())
      assertEquals(0, mutations)
      assertEquals(1, sender.messages.size, "failed request must send no partial sync")
      val failed = sender.messages.head.asInstanceOf[S2CRecallResult]
      val encoded = Unpooled.buffer()
      try
        failed.toBytes(encoded)
        // Recall independently uses the bounded optional-version/optional-reason result codec.
        assertEquals(1 + 1 + 16 + 1 + 1 + 1, encoded.readableBytes())
        val decoded = new S2CRecallResult()
        decoded.fromBytes(encoded)
        assertFalse(encoded.isReadable)
        assertFalse(decoded.success)
        assertEquals(podId, decoded.podUuid)
        assertEquals(None, decoded.newStateVersion)
        assertEquals(Some(RecallFailureReason.InfrastructureFailure), decoded.reason)
      finally encoded.release()
      assertOriginalRequestFailure(events.toVector, "recall", cause, originalStack)

      endpoint.receiveRecall(new C2SRecallRequest(podId, 3L), new ControlledMessageContext())
      assertEquals(2, dimensionCalls)
      assertEquals(1, mutations)
      assertEquals(3, sender.messages.size)
      val succeeded = sender.messages(1).asInstanceOf[S2CRecallResult]
      assertTrue(succeeded.success)
      assertEquals(podId, succeeded.podUuid)
      assertEquals(Some(authoritative.stateVersion), succeeded.newStateVersion)
      assertEquals(None, succeeded.reason)
      assertExactSync(sender.messages(2), authoritative)
      assertOriginalRequestFailure(events.toVector, "recall", cause, originalStack)
      assertEquals(Vector(Level.ERROR, Level.DEBUG), events.map(_.getLevel).toVector)
    }

  private def withActualRequestLog(body: (RequestLog, mutable.ArrayBuffer[LogEvent]) => Unit): Unit =
    val events = mutable.ArrayBuffer.empty[LogEvent]
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val logger = context.getLogger(classOf[Log4jRequestLog].getName)
    val originalLevel = logger.getLevel
    val originalAdditivity = logger.isAdditive
    val appender = new AbstractAppender("request-isolation-" + UUID.randomUUID(), null,
        PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY):
      override def append(event: LogEvent): Unit = { events += event.toImmutable; () }
    appender.start()
    try
      logger.addAppender(appender)
      logger.setLevel(Level.ALL)
      logger.setAdditive(false)
      body(new Log4jRequestLog(logger), events)
    finally
      logger.removeAppender(appender)
      logger.setLevel(originalLevel)
      logger.setAdditive(originalAdditivity)
      appender.stop()

  private def assertOriginalRequestFailure(events: Vector[LogEvent], operation: String,
      cause: Throwable, originalStack: Array[StackTraceElement]): Unit =
    val errors = events.filter(_.getLevel == Level.ERROR)
    assertEquals(1, errors.size)
    assertEquals(s"FloatingPod $operation infrastructure failure from $playerId for $podId",
      errors.head.getMessage.getFormattedMessage)
    assertSame(cause, errors.head.getThrown)
    assertTrue(originalStack.sameElements(errors.head.getThrown.getStackTrace))

  private def assertExactSync(message: IMessage, authoritative: FloatingPodState): Unit =
    val sync = message.asInstanceOf[S2CStateSync]
    assertEquals(authoritative.uuid, sync.podUuid)
    assertEquals(authoritative.stateVersion, sync.stateVersion)
    assertEquals(authoritative.posX, sync.x)
    assertEquals(authoritative.posY, sync.y)
    assertEquals(authoritative.posZ, sync.z)
    assertEquals(authoritative.yaw, sync.yaw)
    assertEquals(authoritative.pitch, sync.pitch)

  @Test
  def lifecycleReleaseUsesPrimaryAuthenticatedSpawnAndItsExactCommittedStateWithoutReread(): Unit =
    val repository = new LifecycleRepository(state(active = false))
    val world = new TestWorld(14)
    val manager = managerFor(repository)
    val port = new ActivePodLifecyclePort(manager, 1)
    val player = new TestPlayerContext(playerId, world)

    val result = port.releaseOutcome(repository.current.get, ReleasePosition(19.0D, 70.0D, -3.0D), player)

    assertTrue(result.isSuccess)
    val snapshot = result.get.asInstanceOf[ReleaseLifecycleOutcome.Accepted].snapshot
    assertTrue(snapshot.state.isActive)
    assertEquals(4L, snapshot.state.stateVersion)
    assertEquals(19.0D, snapshot.state.posX)
    assertEquals(70.0D, snapshot.state.posY)
    assertEquals(-3.0D, snapshot.state.posZ)
    assertEquals(1, world.spawnCalls)
    assertEquals(0, world.removeCalls)
    assertEquals(1, repository.findCalls)
    assertEquals(Some(playerId), repository.activateOwner)
    assertEquals(Some(3L), repository.activateExpectedVersion)
    assertEquals(Some(1), repository.activateGlobalLimit)

  @Test
  def lifecycleRecallDelegatesToManagerExactlyOnceAndReturnsItsInactiveState(): Unit =
    val repository = new LifecycleRepository(state(active = false))
    val world = new TestWorld(15)
    val manager = managerFor(repository)
    val player = new TestPlayerContext(playerId, world)
    assertTrue(manager.spawn(podId, playerId, 3L, world, 2.0D, 64.0D, 0.0D, 1).isSuccess)
    val port = new ActivePodLifecyclePort(manager, 1)

    val result = port.recallOutcome(repository.current.get, player)

    assertTrue(result.isSuccess)
    assertFalse(result.get.asInstanceOf[RecallLifecycleOutcome.Accepted].snapshot.state.isActive)
    assertEquals(1, world.removeCalls)
    assertTrue(manager.getActivePod(podId).isEmpty)

  @Test
  def lifecycleSameDimensionUsesTheManagerActiveEntityAsContextEvidence(): Unit =
    val repository = new LifecycleRepository(state(active = false))
    val world = new TestWorld(16)
    val manager = managerFor(repository)
    assertTrue(manager.spawn(podId, playerId, 3L, world, 0.0D, 64.0D, 0.0D, 1).isSuccess)
    val player = new TestPlayerContext(playerId, world, sameDimension = false)

    val result = new ActivePodLifecyclePort(manager, 1).isSameDimension(repository.current.get, player)

    assertEquals(Success(false), result)
    assertEquals(1, player.dimensionEvidenceCalls)
    assertEquals(16, player.evidencedEntityId)

  @Test
  def lifecycleReturnsExactManagerStateEvenWhenAnySecondaryReadWouldFail(): Unit =
    val repository = new LifecycleRepository(state(active = false))
    repository.failAfterFirstRead = true
    val world = new TestWorld(17)
    val port = new ActivePodLifecyclePort(managerFor(repository), 1)

    val result = port.releaseOutcome(repository.current.get, ReleasePosition(1.0D, 64.0D, 1.0D), new TestPlayerContext(playerId, world))

    assertTrue(result.isSuccess)
    assertEquals(1, world.spawnCalls)
    assertEquals(0, world.removeCalls)

  private def releaseProcessor(authoritative: FloatingPodState): PodRequestProcessor =
    processor(new LifecycleStub(Success(PodLifecycleSnapshot(authoritative)), Success(PodLifecycleSnapshot(authoritative))))

  private def recallProcessor(authoritative: FloatingPodState): PodRequestProcessor =
    new PodRequestProcessor(
      new OwnershipValidator(new RequestRepository(Some(state(active = true, version = 3L)))),
      new FixedCapacity(true),
      new PositionValidator(),
      AllowAllPlayerEntitlementPolicy,
      new LifecycleStub(Success(PodLifecycleSnapshot(authoritative)), Success(PodLifecycleSnapshot(authoritative))),
      NoopRequestLog
    )

  private def rejectingReleaseProcessor(): PodRequestProcessor =
    val repository = new RequestRepository(None)
    new PodRequestProcessor(
      new OwnershipValidator(repository),
      new FixedCapacity(true),
      new PositionValidator(),
      AllowAllPlayerEntitlementPolicy,
      new LifecycleStub(Success(PodLifecycleSnapshot(state(active = true))), Success(PodLifecycleSnapshot(state(active = false)))),
      NoopRequestLog
    )

  private def processor(lifecycle: LifecycleStub): PodRequestProcessor =
    new PodRequestProcessor(
      new OwnershipValidator(new RequestRepository(Some(state(active = false, version = 3L)))),
      new FixedCapacity(true),
      new PositionValidator(),
      AllowAllPlayerEntitlementPolicy,
      lifecycle,
      NoopRequestLog
    )

  private def managerFor(repository: LifecycleRepository): ActiveFloatingPodManager =
    new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, new LifecycleGateway()), () => 20L, () => recoveryAttemptId)

  private def state(active: Boolean, version: Long = 3L, x: Double = 0.0D): FloatingPodState =
    FloatingPodState(podId, Some(playerId), version, "overworld", x, 64.0D, 0.0D, 0.0F, 0.0F, active, 1L, if active then Some(0) else None)

private object NoopRequestLog extends RequestLog:
  override def rejection(operation: String, playerUuid: UUID, podUuid: UUID, reason: FailureReason): Unit = ()
  override def success(operation: String, playerUuid: UUID, podUuid: UUID, authoritativeVersion: Long): Unit = ()
  override def infrastructureFailure(operation: String, playerUuid: UUID, podUuid: UUID, cause: Throwable): Unit = ()

private final class CapturingFactory(player: ServerPlayerContext) extends ServerPlayerContextFactory:
  var context: MessageContext = null
  override def resolve(scheduled: MessageContext): ServerPlayerContext = { context = scheduled; player }

private final class RecordingSender extends ServerPacketSender:
  val messages = mutable.ListBuffer.empty[IMessage]
  override def send(player: ServerPlayerContext, message: IMessage): Unit = messages += message

private final class FailingSender extends ServerPacketSender:
  var attempts = 0
  override def send(player: ServerPlayerContext, message: IMessage): Unit =
    attempts += 1
    throw new IllegalStateException("send failed")

private final class RecordingEndpoint extends ServerPacketEndpoint:
  var recallCalls = 0
  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit = ()
  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit = recallCalls += 1

private final class FixedCapacity(value: Boolean) extends ReleaseCapacityPolicy:
  override def canRelease: Boolean = value

private final class LifecycleStub(releaseResult: Try[PodLifecycleSnapshot], recallResult: Try[PodLifecycleSnapshot]) extends PodLifecyclePort:
  override def isSameDimension(state: FloatingPodState, player: ServerPlayerContext): Try[Boolean] = Success(true)
  override def releaseOutcome(state: FloatingPodState, target: ReleasePosition, player: ServerPlayerContext): Try[ReleaseLifecycleOutcome] = releaseResult.map(ReleaseLifecycleOutcome.Accepted.apply)
  override def recallOutcome(state: FloatingPodState, player: ServerPlayerContext): Try[RecallLifecycleOutcome] = recallResult.map(RecallLifecycleOutcome.Accepted.apply)

private final class TestEntity(val entityId: Int, override val podUuid: UUID) extends ActivePodEntity

private final class TestWorld(entityId: Int, override val dimensionId: Int = 0) extends PodWorld:
  var spawnCalls = 0
  var removeCalls = 0
  private var active: Option[TestEntity] = None
  override def diagnosticWorldName: String = "adapter-test-world"
  override def sameWorld(other: PodWorld): Boolean = this.eq(other)
  override def findPod(podUuid: UUID): Option[ActivePodEntity] = active.filter(_.podUuid == podUuid)
  override def spawnPod(state: FloatingPodState): ActivePodEntity =
    spawnCalls += 1
    val entity = new TestEntity(entityId, state.uuid)
    active = Some(entity)
    entity
  override def removePod(entity: ActivePodEntity): Unit =
    removeCalls += 1
    active = None

private final class TestPlayerContext(
    override val playerUuid: UUID,
    override val spawnWorld: PodWorld,
    sameDimension: Boolean = true
) extends ServerPlayerContext:
  var dimensionEvidenceCalls = 0
  var evidencedEntityId = -1
  override val position = AuthoritativePosition(0.0D, 64.0D, 0.0D)
  override val maximumReach = 8.0D
  override def hasSolidSupport(target: ReleasePosition): Boolean = true
  override def hasTwoBlockHeadroom(target: ReleasePosition): Boolean = true
  override def isSameDimension(activePod: ActivePodEntity): Boolean =
    dimensionEvidenceCalls += 1
    evidencedEntityId = activePod.entityId
    sameDimension

private final class RequestRepository(value: Option[FloatingPodState]) extends FloatingPodRepository:
  override def insert(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def update(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(value)
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(Nil)
  override def findAllActive(): Try[List[FloatingPodState]] = Success(Nil)
  override def findAll(): Try[List[FloatingPodState]] = Success(Nil)
  override def delete(uuid: UUID): Try[Boolean] = Success(false)
  override def transaction[T](fn: => T): Try[T] = Try(fn)
  override def close(): Unit = ()

private final class LifecycleRepository(initial: FloatingPodState) extends FloatingPodRepository, ActivityTransitionStore, PhysicalRecoveryStore:
  var current: Option[FloatingPodState] = Some(initial)
  var findCalls = 0
  var failAfterFirstRead = false
  var activateOwner: Option[UUID] = None
  var activateExpectedVersion: Option[Long] = None
  var activateGlobalLimit: Option[Int] = None
  var legacyActivateCalls = 0
  var legacyDeactivateCalls = 0
  private var locator: Option[PhysicalRecoveryLocator] = None
  override def insert(state: FloatingPodState): Try[FloatingPodState] = { current = Some(state); Success(state) }
  override def update(state: FloatingPodState): Try[FloatingPodState] =
    val persisted = state.copy(stateVersion = state.stateVersion + 1L)
    current = Some(persisted)
    Success(persisted)
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] =
    findCalls += 1
    if failAfterFirstRead && findCalls > 1 then Failure(new IllegalStateException("secondary read is forbidden"))
    else Success(current.filter(_.uuid == uuid))
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(current.toList)
  override def findAllActive(): Try[List[FloatingPodState]] = Success(current.filter(_.isActive).toList)
  override def findAll(): Try[List[FloatingPodState]] = Success(current.toList)
  override def delete(uuid: UUID): Try[Boolean] = Success(false)
  override def transaction[T](fn: => T): Try[T] = Try(fn)
  override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] =
    legacyActivateCalls += 1
    activate(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit)
  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = Failure(new UnsupportedOperationException("physical recovery binding is not exercised by packet adapter tests"))
  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = Failure(new UnsupportedOperationException("physical recovery binding is not exercised by packet adapter tests"))
  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = Failure(new UnsupportedOperationException("physical recovery binding is not exercised by packet adapter tests"))
  override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = Try {
    val prepared = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(expectedDomainVersion), Some(targetDimensionId), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
    locator = Some(prepared)
    prepared
  }
  override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] =
    locator match
      case Some(prepared)
          if prepared.podUuid == target.uuid
            && prepared.attemptUuid == attemptUuid
            && prepared.state == PhysicalRecoveryLocatorState.CreatePrepared
            && prepared.reasonCode == PhysicalRecoveryLocatorReason.CreatePrepared
            && prepared.domainVersion.contains(expectedStateVersion)
            && prepared.targetDimensionId.contains(dimensionId) =>
        activate(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit).map {
          case AdmissionOutcome.Accepted(committed) =>
            locator = Some(prepared.copy(state = PhysicalRecoveryLocatorState.RowActive, reasonCode = PhysicalRecoveryLocatorReason.RowActive, domainVersion = Some(committed.stateVersion), targetDimensionId = committed.dimensionId))
            AdmissionOutcome.Accepted(committed)
          case rejected: AdmissionOutcome.Rejected => rejected
        }
      case _ => Failure(new IllegalStateException("tracked activation requires the exact prepared physical recovery locator"))
  override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] =
    locator match
      case Some(active)
          if active.podUuid == podUuid
            && active.attemptUuid == attemptUuid
            && active.state == PhysicalRecoveryLocatorState.RowActive
            && active.reasonCode == PhysicalRecoveryLocatorReason.RowActive
            && active.domainVersion.contains(expectedStateVersion) =>
        deactivate(podUuid, expectedOwner, expectedStateVersion).map {
          case DeactivationOutcome.Accepted(committed) =>
            locator = Some(active.copy(state = PhysicalRecoveryLocatorState.RowInactivePending, reasonCode = PhysicalRecoveryLocatorReason.RowInactivePending, domainVersion = Some(committed.stateVersion), targetDimensionId = committed.dimensionId))
            DeactivationOutcome.Accepted(committed)
          case rejected: DeactivationOutcome.Rejected => rejected
        }
      case _ => Failure(new IllegalStateException("tracked deactivation requires the exact active physical recovery locator"))
  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = Success(locator.filter(_.podUuid == podUuid))
  private def activate(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] = Try {
    activateOwner = Some(authenticatedOwner)
    activateExpectedVersion = Some(expectedStateVersion)
    activateGlobalLimit = Some(globalActiveLimit)
    current match
      case Some(source) if source.ownerUuid.contains(authenticatedOwner) && source.stateVersion == expectedStateVersion && !source.isActive =>
        val committed = target.copy(ownerUuid = source.ownerUuid, stateVersion = source.stateVersion + 1L, isActive = true, dimensionId = Some(dimensionId))
        current = Some(committed)
        AdmissionOutcome.Accepted(committed)
      case Some(source) if source.stateVersion != expectedStateVersion => AdmissionOutcome.Rejected(AdmissionRejection.VersionMismatch(source.stateVersion))
      case Some(_) => AdmissionOutcome.Rejected(AdmissionRejection.NotOwner)
      case None => AdmissionOutcome.Rejected(AdmissionRejection.UnknownPod)
  }
  override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] =
    legacyDeactivateCalls += 1
    deactivate(podUuid, expectedOwner, expectedStateVersion)
  private def deactivate(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] = Try {
    current match
      case Some(source) if source.ownerUuid == expectedOwner && source.stateVersion == expectedStateVersion && source.isActive =>
        val committed = source.copy(stateVersion = source.stateVersion + 1L, isActive = false)
        current = Some(committed)
        DeactivationOutcome.Accepted(committed)
      case Some(source) if source.stateVersion != expectedStateVersion => DeactivationOutcome.Rejected(DeactivationRejection.VersionMismatch(source.stateVersion))
      case Some(source) if !source.isActive => DeactivationOutcome.Rejected(DeactivationRejection.NotActive)
      case Some(_) => DeactivationOutcome.Rejected(DeactivationRejection.NotOwner)
      case None => DeactivationOutcome.Rejected(DeactivationRejection.UnknownPod)
  }
  override def close(): Unit = ()

private final class LifecycleGateway extends ChunkTicketGateway:
  private final class TestTicket extends ChunkTicket
  override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
  override def request(world: PodWorld): Option[ChunkTicket] = Some(new TestTicket)
  override def release(ticket: ChunkTicket): Unit = ()
  override def maxChunkDepth(ticket: ChunkTicket): Int = 25
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ()
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] = None
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = None
