package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{AdmissionOutcome, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable

/** 真實 SQLite cutover 後，production reconciliation adapter 的首個重開 tracer。 */
class StartupReconciliationRuntimeTest:
  @Test
  def reopenedCommittedActiveRowPublishesThroughProductionAdapterWithoutDomainMutation(): Unit =
    val directory = Files.createTempDirectory("floatingpod-startup-reconciliation-runtime")
    val database = directory.resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000001001")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000001002")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000001003")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "runtime-reconciliation", 32D, 64D, -16D, 0F, 0F, false, 123L, None)
    val chunks = recoveryChunks(7, inactive.posX, inactive.posZ)

    try
      val bootstrap = new SQLiteFloatingPodRepository(database.toString)
      val committed = try
        assertTrue(bootstrap.insert(inactive).isSuccess)
        assertTrue(bootstrap.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).isSuccess)
        bootstrap.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get match
          case AdmissionOutcome.Accepted(active) => active
          case other => throw new AssertionError(s"Expected committed active bootstrap, got $other")
      finally bootstrap.close()

      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        val events = mutable.ListBuffer.empty[String]
        val gateway = new RuntimeGateway(events)
        val world = new RuntimeWorld(events)
        val tickets = new ChunkLoadingManager(repository, gateway)
        val manager = new ActiveFloatingPodManager(repository, tickets)
        world.managerUuids = () => manager.getAllActivePods().toSet
        val worlds = new ServerWorldResolver:
          override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == 7 then Some(world) else None

        assertTrue(tickets.managedTicketRecords().isEmpty)
        assertTrue(world.entityUuids.isEmpty)
        assertTrue(manager.getAllActivePods().isEmpty)

        val decision = StartupReconciliation.forServer(repository, tickets, manager, worlds).decidePublication()

        assertEquals(StartupPublicationDecision.Permit(List(podUuid)), decision)
        assertEquals(List("ticket-request", s"entity:$podUuid"), events.toList)
        assertEquals(Set(podUuid), tickets.managedTicketRecords().keySet.toSet)
        assertEquals(Set(podUuid), world.entityUuids)
        assertEquals(List(podUuid), manager.getAllActivePods())
        assertEquals(Some(committed), repository.findByUuid(podUuid).get)
        assertEquals(committed.stateVersion, repository.findByUuid(podUuid).get.get.stateVersion)
        assertEquals(committed.lastUpdated, repository.findByUuid(podUuid).get.get.lastUpdated)
      finally repository.close()
    finally
      Files.deleteIfExists(database.resolveSibling("pods.db-shm"))
      Files.deleteIfExists(database.resolveSibling("pods.db-wal"))
      Files.deleteIfExists(database)
      Files.deleteIfExists(directory)

  @Test
  def failedMultiRowPublicationKeepsAttachedBodyAndRollsBackOnlyBootMapping(): Unit =
    val directory = Files.createTempDirectory("floatingpod-startup-reconciliation-abort")
    val database = directory.resolve("pods.db")
    val firstUuid = UUID.fromString("00000000-0000-0000-0000-000000001021")
    val secondUuid = UUID.fromString("00000000-0000-0000-0000-000000001022")
    val firstOwner = UUID.fromString("00000000-0000-0000-0000-000000001023")
    val secondOwner = UUID.fromString("00000000-0000-0000-0000-000000001024")
    val firstAttempt = UUID.fromString("00000000-0000-0000-0000-000000001025")
    val secondAttempt = UUID.fromString("00000000-0000-0000-0000-000000001026")
    val inactiveRows = List(
      FloatingPodState(firstUuid, Some(firstOwner), 0L, "runtime-reconciliation", 32D, 64D, -16D, 0F, 0F, false, 123L, None),
      FloatingPodState(secondUuid, Some(secondOwner), 0L, "runtime-reconciliation", 48D, 64D, -16D, 0F, 0F, false, 124L, None)
    )

    try
      val bootstrap = new SQLiteFloatingPodRepository(database.toString)
      val committed = try
        inactiveRows.zip(List(firstAttempt, secondAttempt)).foreach { case (inactive, attempt) =>
          assertTrue(bootstrap.insert(inactive).isSuccess)
          assertTrue(bootstrap.prepareCreateLocator(inactive.uuid, inactive.stateVersion, attempt, 7, recoveryChunks(7, inactive.posX, inactive.posZ)).isSuccess)
          assertTrue(bootstrap.activatePreparedIfAdmissible(inactive, inactive.ownerUuid.get, inactive.stateVersion, 7, 256, attempt).get.isInstanceOf[AdmissionOutcome.Accepted])
        }
        inactiveRows.map(state => bootstrap.findByUuid(state.uuid).get.get)
      finally bootstrap.close()

      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        val events = mutable.ListBuffer.empty[String]
        val gateway = new RuntimeGateway(events)
        val world = new RuntimeWorld(events)
        world.failSpawnUuid = Some(secondUuid)
        val tickets = new ChunkLoadingManager(repository, gateway)
        val manager = new ActiveFloatingPodManager(repository, tickets)
        world.managerUuids = () => manager.getAllActivePods().toSet
        val sourceBody = world.seed(firstUuid)
        val worlds = new ServerWorldResolver:
          override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == 7 then Some(world) else None

        val failure = try
          StartupReconciliation.forServer(repository, tickets, manager, worlds).decidePublication()
          throw new AssertionError("Expected the second row spawn to fail")
        catch case expected: IllegalStateException => expected

        assertTrue(failure.getMessage.contains(secondUuid.toString))
        assertSame(sourceBody, world.findPod(firstUuid).get)
        assertEquals(Set(firstUuid), world.entityUuids)
        assertTrue(manager.getAllActivePods().isEmpty)
        assertEquals(Set(firstUuid, secondUuid), tickets.managedTicketRecords().keySet.toSet)
        assertEquals(committed, inactiveRows.map(state => repository.findByUuid(state.uuid).get.get))
      finally repository.close()
    finally
      Files.deleteIfExists(database.resolveSibling("pods.db-shm"))
      Files.deleteIfExists(database.resolveSibling("pods.db-wal"))
      Files.deleteIfExists(database)
      Files.deleteIfExists(directory)

  @Test
  def committedInactivePhysicalArtifactsBlockWithoutDestructiveEffectsBeforeCertifiedRecovery(): Unit =
    val directory = Files.createTempDirectory("floatingpod-startup-reconciliation-inactive")
    val database = directory.resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000001011")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000001012")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000001013")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "runtime-reconciliation", 48D, 64D, -16D, 0F, 0F, false, 456L, Some(7))
    val chunks = recoveryChunks(7, inactive.posX, inactive.posZ)

    try
      val bootstrap = new SQLiteFloatingPodRepository(database.toString)
      val committed = try
        assertTrue(bootstrap.insert(inactive).isSuccess)
        assertEquals(
          PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(0L), Some(7), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))),
          bootstrap.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
        )
        bootstrap.findByUuid(podUuid).get.get
      finally bootstrap.close()

      val repository = new SQLiteFloatingPodRepository(database.toString)
      try
        val events = mutable.ListBuffer.empty[String]
        val gateway = new RuntimeGateway(events)
        val world = new RuntimeWorld(events)
        val tickets = new ChunkLoadingManager(repository, gateway)
        val manager = new ActiveFloatingPodManager(repository, tickets)
        world.managerUuids = () => manager.getAllActivePods().toSet
        val worlds = new ServerWorldResolver:
          override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == 7 then Some(world) else None
        tickets.requestTicket(podUuid, world, 3, -1).get
        world.seed(podUuid)
        events.clear()

        val decision = StartupReconciliation.forServer(repository, tickets, manager, worlds).decidePublication()

        decision match
          case StartupPublicationDecision.Block(reason) => assertTrue(reason.nonEmpty)
          case other => throw new AssertionError(s"Expected safety block for inactive physical artifacts, got $other")
        assertTrue(events.isEmpty)
        assertEquals(Set(podUuid), tickets.managedTicketRecords().keySet.toSet)
        assertEquals(Set(podUuid), world.entityUuids)
        assertTrue(manager.getAllActivePods().isEmpty)
        assertEquals(Some(committed), repository.findByUuid(podUuid).get)
        assertEquals(committed.stateVersion, repository.findByUuid(podUuid).get.get.stateVersion)
        assertEquals(committed.lastUpdated, repository.findByUuid(podUuid).get.get.lastUpdated)
      finally repository.close()
    finally
      Files.deleteIfExists(database.resolveSibling("pods.db-shm"))
      Files.deleteIfExists(database.resolveSibling("pods.db-wal"))
      Files.deleteIfExists(database)
      Files.deleteIfExists(directory)

  private def recoveryChunks(dimensionId: Int, posX: Double, posZ: Double): Set[PhysicalRecoveryChunk] =
    val centerX = math.floor(posX / 16D).toInt
    val centerZ = math.floor(posZ / 16D).toInt
    (for
      chunkX <- centerX - 2 to centerX + 2
      chunkZ <- centerZ - 2 to centerZ + 2
    yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

  private final class RuntimeEntity(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity

  private final class RuntimeWorld(events: mutable.ListBuffer[String]) extends PodWorld:
    private val entities = mutable.HashMap.empty[UUID, RuntimeEntity]
    var managerUuids: () => Set[UUID] = () => Set.empty
    var failSpawnUuid = Option.empty[UUID]

    override val dimensionId: Int = 7
    override def diagnosticWorldName: String = "runtime-reconciliation-world"
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = entities.get(podUuid)
    override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
      case entity: RuntimeEntity => entities.get(entity.podUuid).exists(_ eq entity)
      case _ => false
    override def sameWorld(other: PodWorld): Boolean = this.eq(other)
    override def spawnPod(state: FloatingPodState): ActivePodEntity =
      if failSpawnUuid.contains(state.uuid) then throw new IllegalStateException(s"Injected startup spawn failure for ${state.uuid}")
      if events.toList != List("ticket-request") then throw new AssertionError(s"ticket must be requested before entity spawn: $events")
      if managerUuids().nonEmpty then throw new AssertionError("manager must not publish before entity spawn")
      val entity = new RuntimeEntity(state.uuid, entities.size + 1)
      entities.put(state.uuid, entity)
      events += s"entity:${state.uuid}"
      entity
    override def removePod(entity: ActivePodEntity): Unit =
      events += s"entity-remove:${entity.podUuid}"
      entities.remove(entity.podUuid)

    def seed(podUuid: UUID): ActivePodEntity =
      val entity = new RuntimeEntity(podUuid, entities.size + 1)
      entities.put(podUuid, entity)
      entity
    def entityUuids: Set[UUID] = entities.keySet.toSet

  private final class RuntimeGateway(events: mutable.ListBuffer[String]) extends ChunkTicketGateway:
    private final class RuntimeTicket extends ChunkTicket:
      var uuid: Option[UUID] = None

    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] =
      events += "ticket-request"
      Some(new RuntimeTicket)
    override def release(ticket: ChunkTicket): Unit = events += "ticket-release"
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[RuntimeTicket].uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = ticket.asInstanceOf[RuntimeTicket].uuid
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new UnsupportedOperationException("runtime adapter test does not wrap recovered Forge tickets")
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None
