package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.FloatingPodRepository
import java.util.UUID
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.collection.mutable
import scala.util.Try

/** real ChunkLoadingManager publication allocation；gateway只模拟资源接口故障，不声明Forge/B2。 */
class MainPublicationTicketTest:
  private def id(n: Long): UUID = new UUID(0L, n)
  private def tag(n: Long): IoOperationTag = IoOperationTag(id(1), 1L, 1L, id(n))

  @Test def releasedOriginalAllocationCompactsResourcesButKeepsReplayIdentity(): Unit =
    val f = new Fixture
    val original = f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => ()).get
    assertThrows(classOf[IllegalArgumentException], () => f.manager.compactReleasedPublicationTicket(tag(10), original))
    f.manager.releaseTicket(id(10)).get
    f.manager.compactReleasedPublicationTicket(tag(10), original)
    assertTrue(f.manager.isPublicationTicketCompacted(tag(10)))
    assertTrue(f.manager.publicationTicketResponsibility(tag(10)).isEmpty)
    assertTrue(f.manager.getAllManagedTickets().isEmpty)
    assertTrue(f.manager.requestPublicationTicket(tag(10), id(11), f.world, 0, 0, _ => ()).isFailure)
    assertTrue(f.manager.requestPublicationTicket(tag(11), id(10), f.world, 0, 0, _ => ()).isFailure)
    assertEquals(1, f.gateway.requestCalls)
    assertEquals(1, f.gateway.releaseCalls)

  @Test def failedAllocationAndObservableReleaseResidueNeverCompact(): Unit =
    val f = new Fixture
    val originalCause = new IllegalStateException("partial original initialization")
    f.gateway.forceFailure = Some(1 -> originalCause)
    assertTrue(f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => ()).isFailure)
    val ticket = f.manager.publicationTicketResponsibility(tag(10)).get.ticket.get
    f.manager.releaseTicket(id(10)).get
    assertThrows(classOf[IllegalArgumentException], () => f.manager.compactReleasedPublicationTicket(tag(10), ticket))
    assertFalse(f.manager.isPublicationTicketCompacted(tag(10)))
    assertSame(originalCause, f.manager.publicationTicketResponsibility(tag(10)).get.cause.get)
    val retained = new Fixture
    val other = retained.manager.requestPublicationTicket(tag(11), id(11), retained.world, 0, 0, _ => ()).get
    retained.gateway.retainOnRelease = true
    retained.manager.releaseTicket(id(11)).get
    assertThrows(classOf[IllegalStateException], () => retained.manager.compactReleasedPublicationTicket(tag(11), other))
    assertFalse(retained.manager.isPublicationTicketCompacted(tag(11)))
    assertSame(other, retained.manager.publicationTicketResponsibility(tag(11)).get.ticket.get)

  @Test def partialForceFailureRetainsExactRawTicketOriginalCauseAndActualPartialChunks(): Unit =
    val f = new Fixture
    val original = new IllegalStateException("actual force failed")
    f.gateway.forceFailure = Some(3 -> original)
    var retained: ChunkTicket = null
    val result = f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, ticket => retained = ticket)
    assertSame(original, result.failed.get)
    val observation = f.manager.publicationTicketResponsibility(tag(10)).get
    assertSame(retained, observation.ticket.get)
    assertSame(original, observation.cause.get)
    assertFalse(observation.initializationReturned)
    assertEquals(2, observation.actualChunks.get.size)
    assertSame(retained, f.manager.getAllManagedTickets()(id(10)))
    assertEquals(0, f.gateway.releaseCalls)
    assertEquals(2, f.manager.publicationTicketChunks(id(10), f.world, retained).size, "actual partial chunks must not become25")

  @Test def duplicatePodOrOperationRejectsBeforeAnyNewAllocation(): Unit =
    val f = new Fixture
    f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => ()).get
    assertTrue(f.manager.requestPublicationTicket(tag(11), id(10), f.world, 0, 0, _ => ()).isFailure)
    assertTrue(f.manager.requestPublicationTicket(tag(10), id(11), f.world, 0, 0, _ => ()).isFailure)
    assertEquals(1, f.gateway.requestCalls)
    assertEquals(0, f.gateway.releaseCalls)

  @Test def foreignRawTicketCannotOverwriteExistingOtherPodMap(): Unit =
    val f = new Fixture
    val original = f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => ()).get
    f.gateway.replay = Some(original)
    val result = f.manager.requestPublicationTicket(tag(11), id(11), f.world, 0, 0, _ => ())
    assertTrue(result.isFailure)
    assertSame(original, f.manager.getAllManagedTickets()(id(10)))
    assertFalse(f.manager.getAllManagedTickets().contains(id(11)))
    assertSame(original, f.manager.publicationTicketResponsibility(tag(11)).get.ticket.get)
    assertEquals(0, f.gateway.releaseCalls)

  @Test def newPublicationRequestNeverRetriesLegacyPendingRelease(): Unit =
    val f = new Fixture
    f.gateway.forceFailure = Some(1 -> new IllegalStateException("old initialization failed"))
    f.gateway.releaseFailure = Some(new IllegalStateException("old release failed"))
    assertTrue(f.manager.requestTicket(id(9), f.world, 0, 0).isFailure)
    val oldReleaseCalls = f.gateway.releaseCalls
    assertEquals(1, oldReleaseCalls)
    f.gateway.forceFailure = None
    f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => ()).get
    assertEquals(oldReleaseCalls, f.gateway.releaseCalls, "publication retried legacy release")
    val observed = f.manager.publicationTicketResponsibility(tag(10)).get
    assertTrue(observed.initializationReturned)
    assertEquals(25, observed.actualChunks.get.size)

  @Test def resourceRetentionPrecedesCallbackFailureAndNeverAutomaticallyReleases(): Unit =
    val f = new Fixture
    val original = new IllegalStateException("Main retained callback failed")
    val result = f.manager.requestPublicationTicket(tag(10), id(10), f.world, 0, 0, _ => throw original)
    assertSame(original, result.failed.get)
    val observed = f.manager.publicationTicketResponsibility(tag(10)).get
    assertTrue(observed.ticket.nonEmpty)
    assertFalse(observed.initializationReturned)
    assertSame(original, observed.cause.get)
    assertEquals(0, observed.actualChunks.get.size)
    assertEquals(0, f.gateway.releaseCalls)

  private final class Fixture:
    val gateway = new Gateway
    val manager = new ChunkLoadingManager(new ForbiddenRepository, gateway)
    val world = new TestWorld
  private final class TestWorld extends PodWorld:
    override def dimensionId: Int = 7
    override def diagnosticWorldName: String = "ticket-world"
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(uuid: UUID): Option[ActivePodEntity] = throw new AssertionError("ticket cannot sample entities")
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new AssertionError("ticket cannot spawn")
    override def removePod(body: ActivePodEntity): Unit = throw new AssertionError("ticket cannot remove")
  private final class Gateway extends ChunkTicketGateway:
    final class TestTicket extends ChunkTicket
    private val sets = mutable.HashMap.empty[ChunkTicket, mutable.Set[(Int, Int)]]
    private val uuids = mutable.HashMap.empty[ChunkTicket, UUID]
    var forceFailure = Option.empty[(Int, Throwable)]
    var releaseFailure = Option.empty[Throwable]
    var replay = Option.empty[ChunkTicket]
    var requestCalls = 0
    var releaseCalls = 0
    var forces = 0
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] =
      requestCalls += 1
      replay.orElse {
        val ticket = new TestTicket
        sets.put(ticket, mutable.HashSet.empty[(Int, Int)])
        Some(ticket)
      }
    override def release(ticket: ChunkTicket): Unit =
      releaseCalls += 1
      releaseFailure.foreach(cause => throw cause)
      if !retainOnRelease then sets.remove(ticket)
    var retainOnRelease = false
    override private[server] def releaseSnapshot(ticket: ChunkTicket): TicketReleaseSnapshot =
      if sets.contains(ticket) then TicketReleaseSnapshot(Vector(ticket), Vector(ticket), sets(ticket).toSet)
      else TicketReleaseSnapshot(Vector.empty, Vector.empty, Set.empty)
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, uuid: UUID): Unit = { uuids.put(ticket, uuid); () }
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = uuids.get(ticket)
    override def force(ticket: ChunkTicket, x: Int, z: Int): Unit =
      forces += 1
      forceFailure.filter(_._1 == forces).foreach(value => throw value._2)
      sets(ticket).add((x, z)); ()
    override def unforce(ticket: ChunkTicket, x: Int, z: Int): Unit = { sets(ticket).remove((x, z)); () }
    override def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] = sets.get(ticket).map(_.toSet).getOrElse(Set.empty)
    override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new AssertionError("no recovered ticket")
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None
  private final class ForbiddenRepository extends FloatingPodRepository:
    private def forbidden[A](): A = throw new AssertionError("Main publication ticket accessed repository")
    override def insert(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def update(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = forbidden()
    override def findByOwner(uuid: UUID): Try[List[FloatingPodState]] = forbidden()
    override def findAllActive(): Try[List[FloatingPodState]] = forbidden()
    override def findAll(): Try[List[FloatingPodState]] = forbidden()
    override def delete(uuid: UUID): Try[Boolean] = forbidden()
    override def transaction[T](fn: => T): Try[T] = forbidden()
    override def close(): Unit = forbidden()
