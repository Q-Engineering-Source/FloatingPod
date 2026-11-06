package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.*
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertAll, assertEquals, assertFalse, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.util.{Success, Try}

/** S4 恢复只经公开恢复边界验证 DB、ticket、manager 与世界的身份集合。 */
class ServerStartupRecoveryTest:
  @Test
  def desiredTicketSetMismatchFailsBeforeManagerPublication(): Unit =
    val fixture = RecoveryFixture(activeRow(1, Some(7)))
    fixture.tickets.requestTicket(uuid(2), fixture.world, 0, 0).get

    val report = fixture.recovery.recover()

    assertFalse(report.isClean)
    assertTrue(report.failure.nonEmpty)
    assertTrue(fixture.manager.getAllActivePods().isEmpty)

  @Test
  def legacyActiveRowBackfillsCanonicalTicketWorldWithoutVersionOrTimestampChurn(): Unit =
    val fixture = RecoveryFixture(activeRow(3, None))
    fixture.tickets.requestTicket(uuid(3), fixture.world, 0, 0).get

    val report = fixture.recovery.recover()

    assertTrue(report.isClean)
    val recovered = fixture.repository.states(uuid(3))
    assertEquals(Some(7), recovered.dimensionId)
    assertEquals(11L, recovered.stateVersion)
    assertEquals(22L, recovered.lastUpdated)
    assertEquals(List(uuid(3)), report.recoveredUuids)

  @Test
  def nonEmptyManagerBlocksRecoveryBeforeItCanPublishAnotherUuid(): Unit =
    val fixture = RecoveryFixture(activeRow(4, Some(7)))
    val state = fixture.repository.states(uuid(4))
    fixture.tickets.requestTicket(uuid(4), fixture.world, 0, 0).get
    fixture.manager.recoverCommittedActive(state, fixture.world).get

    val report = fixture.recovery.recover()

    assertFalse(report.isClean)
    assertTrue(report.failure.get.getMessage.contains("not empty"))

  @Test
  def exactCleanSetReconstructsManagerAndWorldAndReportsUuid(): Unit =
    val fixture = RecoveryFixture(activeRow(5, Some(7)))
    fixture.tickets.requestTicket(uuid(5), fixture.world, 0, 0).get

    val report = fixture.recovery.recover()

    assertTrue(report.isClean)
    assertEquals(Set(uuid(5)), fixture.manager.getAllActivePods().toSet)
    assertTrue(fixture.world.findPod(uuid(5)).nonEmpty)
    assertEquals(List(uuid(5)), report.recoveredUuids)

  @Test
  def partialRecoveryRollsBackPublishedEntityAndManagerButRetainsDesiredRowsAndTickets(): Unit =
    val first = activeRow(6, Some(7))
    val second = activeRow(7, Some(8))
    val fixture = MultiRecoveryFixture(List(first, second), None)
    fixture.tickets.requestTicket(first.uuid, fixture.world, 0, 0).get
    fixture.tickets.requestTicket(second.uuid, fixture.world, 0, 0).get

    val report = fixture.recovery.recover()

    assertTrue(report.failure.exists(_.getMessage.contains(second.uuid.toString)))
    assertEquals(Some(first), fixture.repository.raw(first.uuid))
    assertEquals(Some(second), fixture.repository.raw(second.uuid))
    assertTrue(fixture.tickets.managedTicketRecords().contains(first.uuid))
    assertTrue(fixture.world.findPod(first.uuid).isEmpty)
    assertTrue(fixture.manager.getActivePod(first.uuid).isEmpty)

  @Test
  def partialRecoveryRetainsAttachedSourceBodyWhileRemovingOnlyBootMapping(): Unit =
    val first = activeRow(61, Some(7))
    val second = activeRow(62, Some(8))
    val fixture = MultiRecoveryFixture(List(first, second), None)
    fixture.tickets.requestTicket(first.uuid, fixture.world, 0, 0).get
    fixture.tickets.requestTicket(second.uuid, fixture.world, 0, 0).get
    val sourceBody = fixture.world.seed(first.uuid)

    val report = fixture.recovery.recover()

    assertTrue(report.failure.exists(_.getMessage.contains("No server world exists")))
    assertSame(sourceBody, fixture.world.findPod(first.uuid).get)
    assertTrue(fixture.manager.getActivePod(first.uuid).isEmpty)
    assertTrue(fixture.tickets.managedTicketRecords().contains(first.uuid))
    assertEquals(Some(first), fixture.repository.raw(first.uuid))
    assertEquals(Some(second), fixture.repository.raw(second.uuid))

  @Test
  def partialRecoveryCleanupFailureIsSuppressedWithoutLeavingManagerPublication(): Unit =
    val first = activeRow(8, Some(7))
    val second = activeRow(9, Some(8))
    val removalFailure = new IllegalStateException("remove failed")
    val fixture = MultiRecoveryFixture(List(first, second), Some(removalFailure))
    fixture.tickets.requestTicket(first.uuid, fixture.world, 0, 0).get
    fixture.tickets.requestTicket(second.uuid, fixture.world, 0, 0).get

    val report = fixture.recovery.recover()

    assertTrue(report.failure.exists(_.getMessage.contains(second.uuid.toString)))
    assertTrue(report.failure.exists(_.getSuppressed.contains(removalFailure)))
    assertTrue(fixture.manager.getActivePod(first.uuid).isEmpty)
    assertTrue(fixture.tickets.managedTicketRecords().contains(first.uuid))

  @Test
  def fatalRollbackRetainsOriginalFailureAndContinuesOlderReceiptCleanup(): Unit =
    assertRollbackEscape(new LinkageError("actual latest receipt fatal removal"))

  @Test
  def interruptedRollbackRetainsOriginalFailureAndContinuesOlderReceiptCleanup(): Unit =
    assertRollbackEscape(new InterruptedException("actual latest receipt interrupted removal"))

  @Test
  def originalFatalRecoveryEscapesAfterRemainingReceiptCleanup(): Unit =
    val original = new LinkageError("original third world resolution fatal")
    assertRollbackEscape(original, new IllegalStateException("latest receipt removal failure"), original)

  @Test
  def originalInterruptedRecoveryEscapesAfterRemainingReceiptCleanup(): Unit =
    val original = new InterruptedException("original third world resolution interrupted")
    assertRollbackEscape(original, new IllegalStateException("latest receipt removal failure"), original)

  private def assertRollbackEscape(cleanup: Throwable): Unit =
    assertRollbackEscape(new IllegalStateException("original third world resolution failure"), cleanup, cleanup)

  private def assertRollbackEscape(original: Throwable, cleanup: Throwable, expectedEscape: Throwable): Unit =
    val first = activeRow(81, Some(7))
    val second = activeRow(82, Some(7))
    val third = activeRow(83, Some(8))
    val repository = new RecoveryRepository(List(first, second, third))
    val world = new RecoveryWorld(Some(cleanup), Some(second.uuid))
    val tickets = new ChunkLoadingManager(repository, new RecoveryGateway)
    val manager = new ActiveFloatingPodManager(repository, tickets)
    List(first, second, third).foreach(row => tickets.requestTicket(row.uuid, world, 0, 0).get)
    var originalReached = false
    val recovery = new ServerStartupRecovery(repository, tickets, manager, new ServerWorldResolver:
      override def resolve(dimensionId: Int): Option[PodWorld] =
        if dimensionId == 8 then
          assertEquals(Set(first.uuid, second.uuid), manager.getAllActivePods().toSet)
          assertTrue(world.findPod(first.uuid).nonEmpty)
          assertTrue(world.findPod(second.uuid).nonEmpty)
          originalReached = true
          throw original
        Some(world)
    )
    var escaped = Option.empty[Throwable]
    var returned = Option.empty[StartupRecoveryReport]
    try returned = Some(recovery.recover())
    catch case cause: Throwable => escaped = Some(cause)

    def reaches(start: Throwable, target: Throwable): Boolean =
      val seen = new java.util.IdentityHashMap[Throwable, java.lang.Boolean]()
      def visit(current: Throwable): Boolean =
        if current == null || seen.put(current, java.lang.Boolean.TRUE) != null then false
        else (current eq target) || visit(current.getCause) || current.getSuppressed.exists(visit)
      visit(start)

    val observed = s"escaped=${escaped.map(_.getClass.getName)} returnedReport=${returned.nonEmpty} " +
      s"removals=${world.removalAttempts.toList} remainingManager=${manager.getAllActivePods()}"
    assertAll(observed,
      () => assertTrue(originalReached, "both original receipts must be published before the third failure"),
      () => assertTrue(returned.isEmpty, "fatal/interruption must not become an ordinary recovery report"),
      () => assertSame(expectedEscape, escaped.orNull, "the original fatal/interruption object must escape"),
      () => assertTrue(escaped.exists(reaches(_, original)), "original recovery failure identity must remain reachable"),
      () => assertTrue(escaped.exists(reaches(_, cleanup)), "actual fatal/interruption identity must remain reachable"),
      () => assertEquals(List(second.uuid, first.uuid), world.removalAttempts.toList,
        "fatal latest receipt must not prevent older receipt cleanup"),
      () => assertTrue(manager.getActivePod(first.uuid).isEmpty, "older receipt manager publication must be rolled back"),
      () => assertTrue(manager.getActivePod(second.uuid).isEmpty, "latest receipt manager publication must be rolled back"),
      () => assertTrue(world.findPod(first.uuid).isEmpty, "older receipt body must be removed"),
      () => assertTrue(world.findPod(second.uuid).nonEmpty, "failed removal must not claim that its body was deleted"),
      () => assertTrue(List(first, second, third).forall(row => repository.raw(row.uuid).contains(row)),
        "rollback must retain original desired rows"),
      () => assertEquals(Set(first.uuid, second.uuid, third.uuid), tickets.managedTicketRecords().keySet,
        "rollback must retain original tickets"))

  @Test
  def dirtyTicketRecoveryCarriesExactCauseAndSkipsLegacyBackfillAndPublication(): Unit =
    val legacy = activeRow(10, None)
    val orphan = uuid(11)
    val releaseFailure = new IllegalStateException("orphan release failed")
    val repository = new RecoveryRepository(List(legacy))
    val world = new RecoveryWorld
    val tickets = new ChunkLoadingManager(repository, new RecoveryGateway(Some(releaseFailure)))
    val manager = new ActiveFloatingPodManager(repository, tickets)
    val recovery = new ServerStartupRecovery(repository, tickets, manager, new ServerWorldResolver:
      override def resolve(dimensionId: Int): Option[PodWorld] = Some(world)
    )
    tickets.requestTicket(legacy.uuid, world, 0, 0).get
    tickets.requestTicket(orphan, world, 0, 0).get

    val report = recovery.recover()

    val failure = report.failure.get.asInstanceOf[TicketRecoveryFailure]
    assertEquals(report.ticketReport, failure.report)
    assertSame(releaseFailure, failure.originalCause.get)
    assertEquals(0, repository.backfillCalls)
    assertTrue(manager.getAllActivePods().isEmpty)
    assertTrue(world.findPod(legacy.uuid).isEmpty)

  @Test
  def failedPrePublicationVerificationLeavesPreExistingWorldEntityUntouched(): Unit =
    val row = activeRow(12, Some(7))
    val fixture = RecoveryFixture(row)
    fixture.tickets.requestTicket(row.uuid, fixture.world, 0, 0).get
    fixture.world.seed(row.uuid)
    fixture.repository.states.put(row.uuid, row.toPublicStateOption.copy(stateVersion = 99L))

    val report = fixture.recovery.recover()

    assertTrue(report.failure.exists(_.getMessage.contains("exact committed database state")))
    assertTrue(fixture.world.findPod(row.uuid).nonEmpty)
    assertTrue(fixture.manager.getActivePod(row.uuid).isEmpty)

  private def uuid(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")
  private def activeRow(value: Int, dimension: Option[Int]): StoredPodRow =
    StoredPodRow(uuid(value), Some(uuid(value + 100)), 11L, "recovery", 0D, 64D, 0D, 0F, 0F, true, 22L, dimension)

  private final case class RecoveryFixture(row: StoredPodRow):
    val repository = new RecoveryRepository(List(row))
    val gateway = new RecoveryGateway
    val world = new RecoveryWorld
    val tickets = new ChunkLoadingManager(repository, gateway)
    val manager = new ActiveFloatingPodManager(repository, tickets)
    val recovery = new ServerStartupRecovery(repository, tickets, manager, new ServerWorldResolver:
      override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == world.dimensionId then Some(world) else None
    )

  private final case class MultiRecoveryFixture(rows: List[StoredPodRow], removalFailure: Option[Throwable]):
    val repository = new RecoveryRepository(rows)
    val gateway = new RecoveryGateway
    val world = new RecoveryWorld(removalFailure)
    val tickets = new ChunkLoadingManager(repository, gateway)
    val manager = new ActiveFloatingPodManager(repository, tickets)
    val recovery = new ServerStartupRecovery(repository, tickets, manager, new ServerWorldResolver:
      override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == world.dimensionId then Some(world) else None
    )

  private final class RecoveryEntity(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity
  private final class RecoveryWorld(removalFailure: Option[Throwable] = None,
      removalFailurePod: Option[UUID] = None) extends PodWorld:
    private val entities = mutable.HashMap.empty[UUID, RecoveryEntity]
    val removalAttempts = mutable.ListBuffer.empty[UUID]
    override val dimensionId: Int = 7
    override def diagnosticWorldName: String = "recovery-world"
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = entities.get(podUuid)
    override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
      case entity: RecoveryEntity => entities.get(entity.podUuid).exists(_ eq entity)
      case _ => false
    override def sameWorld(other: PodWorld): Boolean = this.eq(other)
    def seed(podUuid: UUID): ActivePodEntity =
      val entity = new RecoveryEntity(podUuid, entities.size + 1)
      entities.put(podUuid, entity)
      entity
    override def spawnPod(state: FloatingPodState): ActivePodEntity =
      val entity = new RecoveryEntity(state.uuid, entities.size + 1)
      entities.put(state.uuid, entity)
      entity
    override def removePod(entity: ActivePodEntity): Unit =
      removalAttempts += entity.podUuid
      if removalFailurePod.forall(_ == entity.podUuid) then removalFailure.foreach(throw _)
      entities.remove(entity.podUuid)

  private final class RecoveryGateway(releaseFailure: Option[Throwable] = None) extends ChunkTicketGateway:
    private final class RecoveryTicket extends ChunkTicket:
      var uuid: Option[UUID] = None
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = Some(new RecoveryTicket)
    override def release(ticket: ChunkTicket): Unit = releaseFailure.foreach(throw _)
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[RecoveryTicket].uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = ticket.asInstanceOf[RecoveryTicket].uuid
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new UnsupportedOperationException
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class RecoveryRepository(initialRows: List[StoredPodRow]) extends FloatingPodRepository, ActivityTransitionStore, RecoveryPodStore, PhysicalRecoveryStore:
    val states = mutable.HashMap.empty[UUID, FloatingPodState]
    private var rows = mutable.HashMap.from(initialRows.map(row => row.uuid -> row))
    private val locators = mutable.HashMap.from(initialRows.collect { case row if row.isActive => row.uuid -> rowActiveLocator(row) })
    initialRows.foreach(row => row.dimensionId.foreach(_ => states.put(row.uuid, row.toPublicStateOption)))
    var backfillCalls = 0
    def raw(uuid: UUID): Option[StoredPodRow] = rows.get(uuid)
    override def insert(state: FloatingPodState): Try[FloatingPodState] = Success(state)
    override def update(state: FloatingPodState): Try[FloatingPodState] = Success(state)
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(states.get(uuid))
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(Nil)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(false)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()
    override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] = throw new UnsupportedOperationException
    override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] = throw new UnsupportedOperationException
    override def findDesiredActiveRows(): Try[List[StoredPodRow]] = Success(rows.values.toList)
    override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(rows.get(uuid))
    override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, dimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = Try {
      backfillCalls += 1
      val row = rows(uuid)
      if row.stateVersion != expectedStateVersion then RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.VersionMismatch(row.stateVersion))
      else row.dimensionId match
        case Some(existing) if existing != dimensionId => RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.DimensionConflict(existing))
        case Some(_) => RecoveryDimensionBackfillOutcome.Ready(states(uuid))
        case None =>
          val state = FloatingPodState(row.uuid, row.ownerUuid, row.stateVersion, row.worldName, row.posX, row.posY, row.posZ, row.yaw, row.pitch, row.isActive, row.lastUpdated, Some(dimensionId), row.operatingMode)
          rows.put(uuid, row.copy(dimensionId = Some(dimensionId))); states.put(uuid, state); RecoveryDimensionBackfillOutcome.Ready(state)
    }
    override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = throw new UnsupportedOperationException("recovery fixture does not exercise physical recovery bindings")
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("recovery fixture does not create physical recovery bindings")
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("recovery fixture does not certify physical recovery bindings")
    override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = throw new UnsupportedOperationException("recovery fixture does not prepare physical recovery locators")
    override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] = throw new UnsupportedOperationException("recovery fixture does not activate prepared physical recovery locators")
    override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] = throw new UnsupportedOperationException("recovery fixture does not deactivate tracked physical recovery locators")
    override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = Success(locators.get(podUuid))

    private def rowActiveLocator(row: StoredPodRow): PhysicalRecoveryLocator =
      val dimensionId = row.dimensionId.getOrElse(7)
      val centerChunkX = math.floor(row.posX / 16D).toInt
      val centerChunkZ = math.floor(row.posZ / 16D).toInt
      val chunks = (for
        chunkX <- centerChunkX - 2 to centerChunkX + 2
        chunkZ <- centerChunkZ - 2 to centerChunkZ + 2
      yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toList
      PhysicalRecoveryLocator(
        row.uuid,
        UUID.nameUUIDFromBytes(s"row-active:${row.uuid}".getBytes(StandardCharsets.UTF_8)),
        PhysicalRecoveryLocatorState.RowActive,
        PhysicalRecoveryLocatorReason.RowActive,
        Some(row.stateVersion),
        Some(dimensionId),
        chunks
      )

  extension (row: StoredPodRow)
    private def toPublicStateOption: FloatingPodState =
      FloatingPodState(row.uuid, row.ownerUuid, row.stateVersion, row.worldName, row.posX, row.posY, row.posZ, row.yaw, row.pitch, row.isActive, row.lastUpdated, row.dimensionId, row.operatingMode)
