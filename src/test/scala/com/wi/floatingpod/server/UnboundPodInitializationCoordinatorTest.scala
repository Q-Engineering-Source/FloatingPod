package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.entity.FloatingPodControlSnapshot
import com.wi.floatingpod.storage.{AdmissionOutcome, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, PhysicalRecoveryStore, UnboundCommitOutcome, UnboundPreparationReceipt}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.sql.DriverManager
import java.util.UUID
import scala.collection.mutable

/** controlled create 的真实 manager/SQLite 文件路径；世界只替代 Forge 适配边界。 */
class UnboundPodInitializationCoordinatorTest:
  @Test
  def frozenMirrorIsDerivedFromServerSnapshotAndPendingOrFaultedDefaultsFrozen(): Unit =
    val pod = uuid(390)
    val binding = uuid(391)
    val attempt = uuid(392)
    val operation = uuid(393)
    val pending = new FloatingPodControlSnapshot(
      FloatingPodControlSnapshot.Phase.PENDING_INITIALIZATION, pod, null, "WorldIdle", false, 0L,
      "control-world", 1D, 64D, 2D, 0F, 0F, 7, binding, attempt, operation, null
    )
    val normal = new FloatingPodControlSnapshot(
      FloatingPodControlSnapshot.Phase.COMMITTED, pod, uuid(394), "Normal", true, 1L,
      "control-world", 1D, 64D, 2D, 0F, 0F, 7, binding, attempt, operation, null
    )

    assertTrue(pending.frozen())
    assertFalse(normal.frozen())
    assertTrue(normal.asFaulted(new IllegalStateException("mirror failure")).frozen())

  @Test
  def createsOwnerlessWorldIdleOnceAndReturnsExactInspectSnapshot(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-runtime").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val fixture = Fixture(repository, 401)
      val result = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256).get

      assertTrue(result.isInitialized)
      assertEquals(fixture.podUuid, result.podUuid)
      assertEquals(None, result.ownerUuid)
      assertEquals(Some(PodOperatingMode.WorldIdle), result.operatingMode)
      assertEquals(Some(fixture.pose), result.observedPose)
      assertEquals(Some(true), result.entityFrozen)
      assertEquals(Some(1L), result.stateVersion)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(List(fixture.podUuid), fixture.manager.getAllActivePods())
      val stored = repository.findByUuid(fixture.podUuid).get.get
      assertEquals(None, stored.ownerUuid)
      assertEquals(PodOperatingMode.WorldIdle, stored.operatingMode)
      assertTrue(stored.isActive)
      assertEquals(1L, stored.stateVersion)
      assertEquals(PhysicalRecoveryLocatorState.RowActive, repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)

      val replay = fixture.coordinator.createDebugPod(uuid(499), uuid(498), fixture.operationUuid, fixture.pose, fixture.world, 256).get
      assertEquals(result, replay)
      val stateBeforeInspect = repository.findByUuid(fixture.podUuid).get
      val locatorBeforeInspect = repository.findPhysicalRecoveryLocator(fixture.podUuid).get
      assertEquals(result, fixture.coordinator.inspect(fixture.podUuid).get)
      assertEquals(stateBeforeInspect, repository.findByUuid(fixture.podUuid).get)
      assertEquals(locatorBeforeInspect, repository.findPhysicalRecoveryLocator(fixture.podUuid).get)
      assertTrue(fixture.coordinator.createDebugPod(uuid(497), uuid(496), fixture.operationUuid, fixture.pose.copy(posX = fixture.pose.posX + 1D), fixture.world, 256).isFailure)
      assertTrue(fixture.coordinator.createDebugPod(fixture.podUuid, uuid(495), uuid(494), fixture.pose, fixture.world, 256).isFailure)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.world.spawnCalls)
    finally repository.close()

  @Test
  def failedCriticalCommitKeepsPublishedBodyAndTicketAsPendingCleanup(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-commit-failure").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      withConnection(database.toString) { connection =>
        val statement = connection.createStatement()
        try statement.execute("CREATE TRIGGER reject_controlled_commit BEFORE UPDATE OF phase ON physical_recovery_intent WHEN NEW.phase = 'COMMITTED' BEGIN SELECT RAISE(ABORT, 'forced controlled commit failure'); END")
        finally statement.close()
      }
      val fixture = Fixture(repository, 411)
      fixture.world.failFindOnCall = 3

      val failed = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256)
      assertTrue(failed.isFailure)
      assertTrue(failed.failed.get.getSuppressed.exists(_.getMessage == "forced world-index readback failure"))
      val pending = fixture.coordinator.inspect(fixture.podUuid).get
      assertEquals(DebugPodPhase.PendingCleanup, pending.phase)
      assertFalse(pending.isInitialized)
      assertTrue(pending.cause.nonEmpty)
      assertEquals(Some(true), pending.entityFrozen)
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
      assertTrue(fixture.manager.getActivePod(fixture.podUuid).isEmpty)
      assertFalse(repository.findByUuid(fixture.podUuid).get.get.isActive)
      assertEquals(PhysicalRecoveryLocatorState.DebugItemCreatePrepared, repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)

      assertTrue(fixture.coordinator.createDebugPod(uuid(419), uuid(418), fixture.operationUuid, fixture.pose, fixture.world, 256).isFailure)
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(DebugPodPhase.PendingCleanup, fixture.coordinator.inspect(fixture.podUuid).get.phase)
    finally repository.close()

  @Test
  def spawnThatPublishesThenThrowsKeepsItsExactBodyAndTicketWhenFallbackLookupAlsoFails(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-spawn-ack-loss").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val fixture = Fixture(repository, 413)
      val spawnFailure = new IllegalStateException("forced spawn acknowledgement failure after publication")
      val lookupFailure = new IllegalStateException("forced fallback world lookup failure")
      fixture.world.failAfterPublication = spawnFailure
      fixture.world.failFindOnCall = 2
      fixture.world.findFailure = lookupFailure

      val failed = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256)

      assertTrue(failed.isFailure)
      assertTrue(failed.failed.get eq spawnFailure)
      assertTrue(spawnFailure.getSuppressed.toList.contains(lookupFailure))
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(0, fixture.world.removeCalls)
      assertTrue(fixture.world.hasIndexedBody(fixture.podUuid))
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
      assertFalse(repository.findByUuid(fixture.podUuid).get.get.isActive)
      assertEquals(PhysicalRecoveryLocatorState.DebugItemCreatePrepared, repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)

      val pending = fixture.coordinator.inspect(fixture.podUuid).get
      assertEquals(DebugPodPhase.PendingCleanup, pending.phase)
      assertEquals(Some(fixture.pose), pending.observedPose)
      assertEquals(Some(true), pending.entityFrozen)
      assertFalse(pending.isInitialized)
      assertTrue(pending.cause.contains(spawnFailure))

      assertTrue(fixture.coordinator.createDebugPod(uuid(419), uuid(418), fixture.operationUuid, fixture.pose, fixture.world, 256).isFailure)
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(1, fixture.gateway.requestCalls)
    finally repository.close()

  @Test
  def readbackAndWorldLookupFailuresDoNotReplaceTheOriginalSqliteCommitFailure(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-readback-failure").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      withConnection(database.toString) { connection =>
        val statement = connection.createStatement()
        try statement.execute("CREATE TRIGGER reject_controlled_commit BEFORE UPDATE OF phase ON physical_recovery_intent WHEN NEW.phase = 'COMMITTED' BEGIN SELECT RAISE(ABORT, 'forced controlled commit failure'); END")
        finally statement.close()
      }
      val wrapped = new ReadbackFailingRepository(repository)
      val fixture = Fixture(repository, 416, wrapped)
      fixture.world.failFindOnCall = 3

      val failed = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256)

      assertTrue(failed.isFailure)
      assertTrue(wrapped.lastCommitFailure.nonEmpty)
      assertTrue(failed.failed.get eq wrapped.lastCommitFailure.get)
      assertTrue(failed.failed.get.getSuppressed.exists(_.getMessage == "forced repository readback failure"))
      assertTrue(failed.failed.get.getSuppressed.exists(_.getMessage == "forced world-index readback failure"))
      val pending = fixture.coordinator.inspect(fixture.podUuid).get
      assertEquals(DebugPodPhase.Faulted, pending.phase)
      assertFalse(pending.isInitialized)
      assertTrue(pending.cause.contains(failed.failed.get))
      assertEquals(1, fixture.world.spawnCalls)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
    finally repository.close()

  @Test
  def fullActiveCapacityRefusesBeforeTicketOrEntityAllocation(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-capacity").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val fixture = Fixture(repository, 431)
      val store = repository: PhysicalRecoveryStore
      val ownerUuid = uuid(438)
      val existingUuid = uuid(439)
      val existingAttempt = uuid(440)
      val inactive = FloatingPodState(existingUuid, Some(ownerUuid), 0L, "capacity-world", 0D, 64D, 0D, 0F, 0F, false, 1L)
      repository.insert(inactive).get
      val chunks = (for chunkX <- -2 to 2; chunkZ <- -2 to 2 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
      store.prepareCreateLocator(existingUuid, 0L, existingAttempt, 7, chunks).get
      store.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, existingAttempt).get match
        case AdmissionOutcome.Accepted(_) => ()
        case other => throw new AssertionError(s"Expected fixture activation before capacity check, got $other")

      val refused = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 1)

      assertTrue(refused.isFailure)
      val snapshot = fixture.coordinator.inspect(fixture.podUuid).get
      assertEquals(DebugPodPhase.Prepared, snapshot.phase)
      assertFalse(snapshot.isInitialized)
      assertTrue(snapshot.cause.nonEmpty)
      assertEquals(0, fixture.gateway.requestCalls)
      assertEquals(0, fixture.world.spawnCalls)
      assertFalse(repository.findByUuid(fixture.podUuid).get.get.isActive)
      assertEquals(PhysicalRecoveryLocatorState.DebugItemCreatePrepared, repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally repository.close()

  @Test
  def postCommitControlFailureRemainsDurablyActiveAndIsNotReportedInitialized(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-postcommit-failure").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val fixture = Fixture(repository, 421)
      fixture.world.failCommittedControl = true

      assertTrue(fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256).isFailure)
      val failed = fixture.coordinator.inspect(fixture.podUuid).get
      assertEquals(DebugPodPhase.Faulted, failed.phase)
      assertFalse(failed.isInitialized)
      assertTrue(failed.cause.nonEmpty)
      assertTrue(repository.findByUuid(fixture.podUuid).get.get.isActive)
      assertEquals(PhysicalRecoveryLocatorState.RowActive, repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertEquals(1, fixture.chunkManager.getAllManagedTickets().size)
      assertTrue(fixture.manager.committedSpawnFaultSnapshot().contains(fixture.podUuid))
      assertTrue(fixture.manager.getActivePod(fixture.podUuid).isEmpty)
    finally repository.close()

  @Test
  def inspectAndOperationReplayFaultWhenTheSameIndexedBodyDriftsFromCommittedPose(): Unit =
    val database = Files.createTempDirectory("floatingpod-controlled-inspect-drift").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val fixture = Fixture(repository, 451)
      val created = fixture.coordinator.createDebugPod(fixture.podUuid, fixture.attemptUuid, fixture.operationUuid, fixture.pose, fixture.world, 256).get
      assertTrue(created.isInitialized)
      fixture.world.reportedPose = Some(fixture.pose.copy(posX = fixture.pose.posX + 1D))

      val inspected = fixture.coordinator.inspect(fixture.podUuid).get

      assertEquals(DebugPodPhase.Faulted, inspected.phase)
      assertFalse(inspected.isInitialized)
      assertTrue(inspected.cause.nonEmpty)
      assertTrue(fixture.coordinator.createDebugPod(uuid(459), uuid(458), fixture.operationUuid, fixture.pose, fixture.world, 256).isFailure)
      assertEquals(1, fixture.gateway.requestCalls)
      assertEquals(1, fixture.world.spawnCalls)
    finally repository.close()

  private def withConnection[A](database: String)(body: java.sql.Connection => A): A =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:$database")
    try body(connection)
    finally connection.close()

  private def uuid(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")

  private final case class Fixture(repository: SQLiteFloatingPodRepository, suffix: Int, runtimeRepository: FloatingPodRepository & PhysicalRecoveryStore = null):
    val podUuid: UUID = uuid(suffix)
    val attemptUuid: UUID = uuid(suffix + 1)
    val operationUuid: UUID = uuid(suffix + 2)
    val pose: FloatingPodPose = FloatingPodPose("controlled-world", 200D, 65D, -104D, 45F, 0F, 7)
    val bindingUuid: UUID = uuid(suffix + 3)
    val binding: PhysicalRecoveryBinding =
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
    val gateway = new TestGateway
    val lifecycleRepository = Option(runtimeRepository).getOrElse(repository)
    val chunkManager = new ChunkLoadingManager(lifecycleRepository, gateway)
    val manager = new ActiveFloatingPodManager(lifecycleRepository, chunkManager)
    val world = new TestWorld(pose)
    val coordinator = new UnboundPodInitializationCoordinator(manager, chunkManager, binding)

  private final class TestWorld(targetPose: FloatingPodPose) extends ControlledPodWorld:
    private val entities = mutable.HashMap.empty[UUID, TestEntity]
    var spawnCalls = 0
    var removeCalls = 0
    var failAfterPublication: Throwable = null
    var failCommittedControl = false
    var reportedPose = Option.empty[FloatingPodPose]
    var failFindOnCall = -1
    var findFailure: Throwable = new IllegalStateException("forced world-index readback failure")
    private var findCalls = 0
    override def dimensionId: Int = targetPose.dimensionId
    override def diagnosticWorldName: String = targetPose.worldName
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(podUuid: UUID): Option[ActivePodEntity] =
      findCalls += 1
      if findCalls == failFindOnCall then throw findFailure
      entities.get(podUuid)
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new UnsupportedOperationException("This test requires the controlled spawn path")
    override def spawnControlledPod(
        state: FloatingPodState,
        receipt: UnboundPreparationReceipt,
        retainBeforePublication: ControlledActivePodEntity => Unit
    ): ControlledActivePodEntity =
      spawnCalls += 1
      val entity = new TestEntity(receipt.podUuid, spawnCalls, receipt)
      retainBeforePublication(entity)
      entities.put(entity.podUuid, entity)
      if failAfterPublication != null then throw failAfterPublication
      entity
    def hasIndexedBody(podUuid: UUID): Boolean = entities.contains(podUuid)
    override def removePod(entity: ActivePodEntity): Unit = { removeCalls += 1; entities.remove(entity.podUuid) }
    private final class TestEntity(override val podUuid: UUID, override val entityId: Int, receipt: UnboundPreparationReceipt) extends ControlledActivePodEntity:
      private var frozen = true
      override def actualPose: FloatingPodPose = reportedPose.getOrElse(targetPose)
      override def isFrozen: Boolean = frozen
      override def sameBody(other: ControlledActivePodEntity): Boolean = this.eq(other)
      override def hasControlledCreationStamp(candidate: UnboundPreparationReceipt): Boolean =
        candidate.binding == receipt.binding && candidate.podUuid == receipt.podUuid && candidate.attemptUuid == receipt.attemptUuid && candidate.createOperationUuid == receipt.createOperationUuid
      override def applyCommittedControl(state: FloatingPodState, candidate: UnboundPreparationReceipt): Unit =
        if failCommittedControl then throw new IllegalStateException("controlled state mirror rejected committed snapshot")
        if !hasControlledCreationStamp(candidate) || !state.isActive || state.ownerUuid.nonEmpty || state.operatingMode != PodOperatingMode.WorldIdle then
          throw new IllegalStateException("invalid committed controlled state")
        frozen = state.operatingMode == PodOperatingMode.WorldIdle
      override def applyCommittedMode(state: FloatingPodState, attempt: UUID): Unit = frozen = state.operatingMode == PodOperatingMode.WorldIdle
      override def markControlFaulted(cause: Throwable): Unit = frozen = true

  private final class TestGateway extends ChunkTicketGateway:
    private final class TestTicket extends ChunkTicket
    var requestCalls = 0
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = { requestCalls += 1; Some(new TestTicket) }
    override def release(ticket: ChunkTicket): Unit = ()
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ()
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = None
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class ReadbackFailingRepository(delegate: SQLiteFloatingPodRepository) extends FloatingPodRepository, PhysicalRecoveryStore:
    var lastCommitFailure: Option[Throwable] = None
    private var failReadback = false
    override def insert(state: FloatingPodState): scala.util.Try[FloatingPodState] = delegate.insert(state)
    override def update(state: FloatingPodState): scala.util.Try[FloatingPodState] = delegate.update(state)
    override def findByUuid(uuid: UUID): scala.util.Try[Option[FloatingPodState]] =
      if failReadback then scala.util.Failure(new IllegalStateException("forced repository readback failure")) else delegate.findByUuid(uuid)
    override def findByOwner(ownerUuid: UUID): scala.util.Try[List[FloatingPodState]] = delegate.findByOwner(ownerUuid)
    override def findAllActive(): scala.util.Try[List[FloatingPodState]] = delegate.findAllActive()
    override def findAll(): scala.util.Try[List[FloatingPodState]] = delegate.findAll()
    override def delete(uuid: UUID): scala.util.Try[Boolean] = delegate.delete(uuid)
    override def transaction[T](fn: => T): scala.util.Try[T] = delegate.transaction(fn)
    override def close(): Unit = delegate.close()
    override def findPhysicalRecoveryBinding(): scala.util.Try[Option[PhysicalRecoveryBinding]] = delegate.findPhysicalRecoveryBinding()
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): scala.util.Try[PhysicalRecoveryBinding] = delegate.createOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion)
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): scala.util.Try[PhysicalRecoveryBinding] = delegate.certifyPhysicalRecoveryBinding(expectedUuid, expectedFormatVersion)
    override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): scala.util.Try[PhysicalRecoveryLocator] = delegate.prepareCreateLocator(podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks)
    override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): scala.util.Try[AdmissionOutcome] = delegate.activatePreparedIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid)
    override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): scala.util.Try[DeactivationOutcome] = delegate.deactivateTrackedIfExpectedActive(podUuid, expectedOwner, expectedStateVersion, attemptUuid)
    override def prepareUnboundCreate(binding: PhysicalRecoveryBinding, podUuid: UUID, attemptUuid: UUID, targetPose: FloatingPodPose, operationUuid: UUID): scala.util.Try[UnboundPreparationReceipt] = delegate.prepareUnboundCreate(binding, podUuid, attemptUuid, targetPose, operationUuid)
    override def commitUnboundCreate(binding: PhysicalRecoveryBinding, podUuid: UUID, attemptUuid: UUID, expectedPreparedVersion: Long, actualPose: FloatingPodPose, operationUuid: UUID, globalActiveLimit: Int): scala.util.Try[UnboundCommitOutcome] =
      val result = delegate.commitUnboundCreate(binding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit)
      result.failed.foreach { failure => lastCommitFailure = Some(failure); failReadback = true }
      result
    override def findUnboundPreparation(podUuid: UUID): scala.util.Try[Option[UnboundPreparationReceipt]] = delegate.findUnboundPreparation(podUuid)
    override def findPhysicalRecoveryLocator(podUuid: UUID): scala.util.Try[Option[PhysicalRecoveryLocator]] = delegate.findPhysicalRecoveryLocator(podUuid)
