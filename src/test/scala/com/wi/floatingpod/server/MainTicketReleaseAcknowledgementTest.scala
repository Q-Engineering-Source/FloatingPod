package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{FloatingPodRepository, PhysicalRecoveryChunk}
import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.util.Try

class MainTicketReleaseAcknowledgementTest:
  private val pod = new UUID(1L, 2L)
  private val attempt = new UUID(3L, 4L)
  private val tag = IoOperationTag(new UUID(5L, 6L), 7L, 8L, new UUID(9L, 10L))

  @Test
  def exactReleaseCarriesOriginal25AndReusesSameAcknowledgement(): Unit =
    val f = new Fixture
    val pending = f.capture()
    val bodyAck = removal()
    val ack = f.manager.releaseForIo(pending, bodyAck).get
    assertSame(bodyAck, ack.removal())
    assertEquals(25, ack.originalFootprint().size())
    assertTrue(f.manager.managedTicketRecords().isEmpty)
    assertSame(ack, f.manager.releaseForIo(pending, bodyAck).get)
    assertEquals(1, f.gateway.releaseCalls)
    assertThrows(classOf[UnsupportedOperationException], () => { ack.originalFootprint().clear(); () })
    assertTrue(f.manager.releaseForIo(pending, removal()).isFailure)

  @Test
  def everyOperationIdentityMismatchFailsBeforeTicketSideEffects(): Unit =
    Vector(
      removal(boot = new UUID(100L, 1L)), removal(bootGeneration = 99L),
      removal(connectionGeneration = 99L), removal(operation = new UUID(100L, 2L)),
      removal(attemptId = new UUID(100L, 3L)), removal(podId = new UUID(100L, 4L)),
      removal(version = 99L), removal(dimension = 99), removal(entityId = 99)
    ).foreach { wrong =>
      val f = new Fixture
      assertTrue(f.manager.releaseForIo(f.capture(), wrong).isFailure)
      assertEquals(0, f.gateway.releaseCalls)
      assertTrue(f.manager.managedTicketRecords().contains(pod))
    }

  @Test
  def partialReleaseRetryRetainsFullOriginalFootprintAndFailureIdentity(): Unit =
    val f = new Fixture
    val pending = f.capture()
    val bodyAck = removal()
    val primary = new IllegalStateException("actual listener failed after requested removal")
    f.gateway.onRelease = () => { f.gateway.chunks.clear(); throw primary }
    assertSame(primary, f.manager.releaseForIo(pending, bodyAck).failed.get)
    assertSame(primary, pending.failure.get)
    assertEquals(25, pending.originalFootprint.size)
    assertTrue(f.manager.managedTicketRecords().contains(pod))
    f.gateway.onRelease = () => { f.gateway.registry.clear(); () }
    assertTrue(f.manager.releaseForIo(pending, bodyAck).isFailure)
    assertSame(primary, pending.failure.get)
    assertEquals(1, primary.getSuppressed.length)
    assertTrue(f.manager.managedTicketRecords().contains(pod))
    f.gateway.onRelease = () => f.gateway.clear()
    val ack = f.manager.releaseForIo(pending, bodyAck).get
    assertEquals(25, ack.originalFootprint().size())
    assertSame(primary, pending.failure.get)
    assertEquals(3, f.gateway.releaseCalls)

  @Test
  def ordinaryReleaseAndDuplicateCaptureCannotStealRetainedLifecycle(): Unit =
    val f = new Fixture
    f.capture()
    assertTrue(f.manager.releaseIfPresent(pod).isFailure)
    assertThrows(classOf[IllegalArgumentException], () => { f.capture(); () })
    assertThrows(classOf[IllegalArgumentException], () => {
      f.manager.captureReleaseForIo(tag.copy(operationId = new UUID(11L, 12L)), pod, attempt, 13L, 42,
        f.world, f.ticket, f.footprint)
      ()
    })
    assertEquals(0, f.gateway.releaseCalls)

  @Test
  def wrongFootprintAndOffMainNeverCaptureOrRelease(): Unit =
    val f = new Fixture
    assertThrows(classOf[IllegalArgumentException], () => {
      f.manager.captureReleaseForIo(tag, pod, attempt, 13L, 42, f.world, f.ticket, f.footprint.take(24))
      ()
    })
    val pending = f.capture()
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() => { failure.set(f.manager.releaseForIo(pending, removal()).failed.get); () })
    worker.start()
    worker.join()
    assertEquals(classOf[IllegalStateException], failure.get().getClass)
    assertEquals(0, f.gateway.releaseCalls)

  @Test
  def recoveryUuidFailureAndOrdinaryRetryCannotStealPendingRelease(): Unit =
    val f = new Fixture
    val pending = f.capture()
    val primary = new IllegalStateException("actual recovered ticket UUID read failed")
    f.gateway.uuidReadFailure = Some(primary)
    f.manager.recoverAcceptedTickets(List(f.ticket), f.world)
    assertEquals(0, f.gateway.releaseCalls)
    assertTrue(f.manager.managedTicketRecords().contains(pod))
    assertEquals(25, pending.originalFootprint.size)
    assertEquals(0, f.manager.pendingTicketReleaseCount())
    assertTrue(f.manager.latestRecoveryReport().issues.exists {
      case TicketRecoveryIssue.CandidateProcessingFailed(_, cause) => cause eq primary
      case _ => false
    })
    assertTrue(f.manager.reconcilePendingTicketReleases().isSuccess)
    assertEquals(0, f.gateway.releaseCalls)
    f.gateway.uuidReadFailure = None
    assertEquals(25, f.manager.releaseForIo(pending, removal()).get.originalFootprint().size())

  // 只测试消费字段匹配；实际签发另由 ForgePodRemovalObserverTest 验证完整索引/END seam。
  private def removal(boot: UUID = tag.bootId, bootGeneration: Long = tag.bootGeneration,
      connectionGeneration: Long = tag.connectionGeneration, operation: UUID = tag.operationId,
      attemptId: UUID = attempt, podId: UUID = pod, version: Long = 13L, dimension: Int = 7, entityId: Int = 42)
      : ExactBodyRemovalAcknowledgement =
    ExactBodyRemovalAcknowledgement.issue(boot, bootGeneration, connectionGeneration, operation,
      attemptId, podId, version, dimension, entityId, 20L)

  private final class Fixture:
    val gateway = new Gateway
    val world = new TestWorld
    val manager = new ChunkLoadingManager(new ForbiddenRepository, gateway)
    val ticket = manager.requestTicket(pod, world, 0, 0).get
    val footprint = gateway.chunks.iterator.map((x, z) => PhysicalRecoveryChunk(7, x, z)).toSet
    def capture(): PendingManagedTicketRelease = manager.captureReleaseForIo(tag, pod, attempt, 13L, 42, world, ticket, footprint)

  private final class TestWorld extends PodWorld:
    override def dimensionId: Int = 7
    override def diagnosticWorldName: String = "release-world"
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(uuid: UUID): Option[ActivePodEntity] = throw new AssertionError("no entity query")
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new AssertionError("no entity spawn")
    override def removePod(body: ActivePodEntity): Unit = throw new AssertionError("no entity removal")

  private final class Gateway extends ChunkTicketGateway:
    val ticket = new ChunkTicket {}
    val registry = mutable.ArrayBuffer[AnyRef](ticket)
    val forced = mutable.ArrayBuffer[AnyRef](ticket)
    val chunks = mutable.Set.empty[(Int, Int)]
    var uuid = Option.empty[UUID]
    var releaseCalls = 0
    var uuidReadFailure = Option.empty[Throwable]
    var onRelease: () => Unit = () => clear()
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = Some(ticket)
    override def release(ticket: ChunkTicket): Unit = { releaseCalls += 1; onRelease() }
    override private[server] def releaseSnapshot(ticket: ChunkTicket): TicketReleaseSnapshot =
      TicketReleaseSnapshot(registry.toVector, forced.toVector, chunks.toSet)
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] =
      uuidReadFailure.foreach(cause => throw cause)
      uuid
    override def force(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunks.add((x, z)); () }
    override def unforce(ticket: ChunkTicket, x: Int, z: Int): Unit = { chunks.remove((x, z)); () }
    override def forcedChunks(ticket: ChunkTicket): Set[(Int, Int)] = chunks.toSet
    override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new AssertionError("no recovery")
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None
    def clear(): Unit = { registry.clear(); forced.clear(); chunks.clear() }

  private final class ForbiddenRepository extends FloatingPodRepository:
    private def forbidden[A](): A = throw new AssertionError("Main release accessed repository")
    override def insert(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def update(state: FloatingPodState): Try[FloatingPodState] = forbidden()
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = forbidden()
    override def findByOwner(uuid: UUID): Try[List[FloatingPodState]] = forbidden()
    override def findAllActive(): Try[List[FloatingPodState]] = forbidden()
    override def findAll(): Try[List[FloatingPodState]] = forbidden()
    override def delete(uuid: UUID): Try[Boolean] = forbidden()
    override def transaction[T](fn: => T): Try[T] = forbidden()
    override def close(): Unit = forbidden()
