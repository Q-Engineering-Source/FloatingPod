package com.wi.floatingpod.crashpoint

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ActivePodEntity, ChunkLoadingManager, ChunkTicket, ChunkTicketGateway, PodWorld, ServerStartupRecovery, ServerWorldResolver}
import com.wi.floatingpod.server.persistence.PeriodicFlushService
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, DeactivationOutcome, FloatingPodRepository}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}

import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** 直接验证全局崩溃注入点与 SQLite 重启边界。 */
class CrashPointHarnessTest:
  private val harness = CrashPointHarness.getInstance

  @AfterEach
  def resetCrashHarness(): Unit =
    harness.disable()
    harness.reset()

  @Test
  def harnessIsDisabledByDefaultAndValidatesPointNames(): Unit =
    harness.disable()
    harness.reset()
    assertFalse(harness.isEnabled)
    assertThrows(classOf[IllegalArgumentException], () => harness.register(null))
    assertThrows(classOf[IllegalArgumentException], () => harness.register("  "))
    harness.register("disabled-point")
    harness.checkCrashPoint("disabled-point")
    harness.enable()
    assertThrows(classOf[IllegalArgumentException], () => harness.checkCrashPoint(""))
    harness.checkCrashPoint("disabled-point")

  @Test
  def registeredPointThrowsOnlyWhenEnabledAndResetClearsIt(): Unit =
    harness.enable()
    harness.register("direct-point")
    val exception = assertThrows(classOf[CrashPointHarness.CrashPointException], () => harness.checkCrashPoint("direct-point"))
    assertTrue(exception.getMessage.contains("direct-point"))
    harness.reset()
    harness.checkCrashPoint("direct-point")
    assertTrue(harness.isEnabled)

  @Test
  def spawnCrashAfterDbCommitLeavesCommittedStateForFreshRuntime(): Unit =
    withDatabase { path =>
      val uuid = CrashPointTestSupport.SpawnUuid
      val world = new CrashPointTestSupport.TestWorld(101)
      val tickets = new CrashPointTestSupport.TicketRestartModel
      withRepository(path) { repository =>
        val gateway = new CrashPointTestSupport.TestGateway(tickets)
        val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, gateway), () => CrashPointTestSupport.FixedTimestamp)
        repository.insert(CrashPointTestSupport.state(uuid, active = false)).get
        harness.enable()
        harness.register(CrashPointHarness.AfterDbCommitSpawn)

        assertThrows(classOf[CrashPointHarness.CrashPointException], () => manager.spawn(uuid, CrashPointTestSupport.OwnerUuid, 0L, world, 0.0D, 64.0D, 0.0D, 256).get)
        assertEquals(0, gateway.releaseCalls)
        assertEquals(0, world.removeCalls)
        assertEquals(List(101), world.entityIds)
        assertTrue(repository.findByUuid(uuid).get.exists(_.isActive))
        assertTrue(manager.getAllActivePods().isEmpty)
      }

      withRepository(path) { reopened =>
        val freshGateway = new CrashPointTestSupport.TestGateway(tickets)
        val freshChunks = new ChunkLoadingManager(reopened, freshGateway)
        val freshManager = new ActiveFloatingPodManager(reopened, freshChunks)
        val freshWorld = world.freshRuntime()
        assertEquals(1, tickets.runOrderedRecovery(freshChunks, freshGateway, freshWorld).size)
        assertTrue(freshChunks.getAllManagedTickets().contains(uuid))
        val recovery = new ServerStartupRecovery(reopened, freshChunks, freshManager, new ServerWorldResolver:
          override def resolve(dimensionId: Int): Option[PodWorld] = if dimensionId == freshWorld.dimensionId then Some(freshWorld) else None
        )
        val recoveryReport = recovery.recover()
        assertTrue(recoveryReport.isClean)
        assertEquals(List(uuid), recoveryReport.recoveredUuids)
        val snapshot = StateSnapshot.capture(freshManager, reopened, freshWorld.entityIds).get
        assertEquals(List(uuid), snapshot.persistedPods)
        assertEquals(List(uuid), snapshot.activeInDb)
        assertEquals(List(uuid), snapshot.activePods)
        assertEquals(List(101), snapshot.entityIds)
        assertTrue(snapshot.duplicatePersistedUuids.isEmpty)
      }
    }

  @Test
  def recallCrashAfterDbCommitLeavesInactiveRecordForFreshRuntime(): Unit =
    withDatabase { path =>
      val uuid = CrashPointTestSupport.RecallUuid
      val world = new CrashPointTestSupport.TestWorld(102)
      val tickets = new CrashPointTestSupport.TicketRestartModel
      withRepository(path) { repository =>
        val gateway = new CrashPointTestSupport.TestGateway(tickets)
        val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, gateway), () => CrashPointTestSupport.FixedTimestamp)
        repository.insert(CrashPointTestSupport.state(uuid, active = false)).get
        assertTrue(manager.spawn(uuid, CrashPointTestSupport.OwnerUuid, 0L, world, 0.0D, 64.0D, 0.0D, 256).isSuccess)
        harness.enable()
        harness.register(CrashPointHarness.AfterDbCommitRecall)

        assertThrows(classOf[CrashPointHarness.CrashPointException], () => manager.recall(uuid).get)
        assertEquals(1, world.removeCalls)
        assertTrue(world.entityIds.isEmpty)
        assertEquals(1, gateway.releaseCalls)
        assertEquals(0, tickets.liveTicketCount)
        assertTrue(manager.getActivePod(uuid).isEmpty)
        assertTrue(repository.findByUuid(uuid).get.exists(!_.isActive))
      }

      withRepository(path) { reopened =>
        val freshGateway = new CrashPointTestSupport.TestGateway(tickets)
        val freshChunks = new ChunkLoadingManager(reopened, freshGateway)
        val freshManager = new ActiveFloatingPodManager(reopened, freshChunks)
        val freshWorld = world.freshRuntime()
        assertTrue(tickets.runOrderedRecovery(freshChunks, freshGateway, freshWorld).isEmpty)
        val snapshot = StateSnapshot.capture(freshManager, reopened, freshWorld.entityIds).get
        assertEquals(List(uuid), snapshot.persistedPods)
        assertTrue(snapshot.activeInDb.isEmpty)
        assertTrue(snapshot.activePods.isEmpty)
        assertTrue(snapshot.entityIds.isEmpty)
      }
    }

  @Test
  def crashBeforeEntitySpawnLeavesTicketAtBoundaryAndFreshRuntimeEmpty(): Unit =
    withDatabase { path =>
      val uuid = CrashPointTestSupport.BeforeEntityUuid
      val world = new CrashPointTestSupport.TestWorld(103)
      val tickets = new CrashPointTestSupport.TicketRestartModel
      withRepository(path) { repository =>
        val gateway = new CrashPointTestSupport.TestGateway(tickets)
        val chunkManager = new ChunkLoadingManager(repository, gateway)
        val manager = new ActiveFloatingPodManager(repository, chunkManager, () => CrashPointTestSupport.FixedTimestamp)
        repository.insert(CrashPointTestSupport.state(uuid, active = false)).get
        harness.enable()
        harness.register(CrashPointHarness.BeforeEntitySpawn)

        assertThrows(classOf[CrashPointHarness.CrashPointException], () => manager.spawn(uuid, CrashPointTestSupport.OwnerUuid, 0L, world, 0.0D, 64.0D, 0.0D, 256).get)
        assertEquals(1, gateway.requestCalls)
        assertEquals(0, gateway.releaseCalls)
        assertEquals(1, chunkManager.getAllManagedTickets().size)
        assertTrue(world.entityIds.isEmpty)
        assertTrue(repository.findByUuid(uuid).get.exists(!_.isActive))
      }

      withRepository(path) { reopened =>
        val freshGateway = new CrashPointTestSupport.TestGateway(tickets)
        val freshChunks = new ChunkLoadingManager(reopened, freshGateway)
        val freshManager = new ActiveFloatingPodManager(reopened, freshChunks)
        val freshWorld = world.freshRuntime()
        assertTrue(tickets.runOrderedRecovery(freshChunks, freshGateway, freshWorld).isEmpty)
        assertEquals(1, tickets.recoveryCallbackCalls)
        assertEquals(0, tickets.liveTicketCount)
        assertTrue(freshChunks.getAllManagedTickets().isEmpty)
        assertTrue(freshChunks.requestTicket(uuid, freshWorld, 0, 0).isSuccess)
        assertTrue(freshChunks.releaseTicket(uuid).isSuccess)
        val snapshot = StateSnapshot.capture(freshManager, reopened, freshWorld.entityIds).get
        assertTrue(snapshot.activePods.isEmpty)
        assertTrue(snapshot.entityIds.isEmpty)
        assertTrue(reopened.findByUuid(uuid).get.exists(!_.isActive))
      }
    }

  @Test
  def crashAfterEntitySpawnLeavesEntityAndTicketToRestartDiscard(): Unit =
    withDatabase { path =>
      val uuid = CrashPointTestSupport.AfterEntityUuid
      val world = new CrashPointTestSupport.TestWorld(104)
      val tickets = new CrashPointTestSupport.TicketRestartModel
      withRepository(path) { repository =>
        val gateway = new CrashPointTestSupport.TestGateway(tickets)
        val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, gateway), () => CrashPointTestSupport.FixedTimestamp)
        repository.insert(CrashPointTestSupport.state(uuid, active = false)).get
        harness.enable()
        harness.register(CrashPointHarness.AfterEntitySpawn)

        assertThrows(classOf[CrashPointHarness.CrashPointException], () => manager.spawn(uuid, CrashPointTestSupport.OwnerUuid, 0L, world, 0.0D, 64.0D, 0.0D, 256).get)
        assertEquals(1, world.spawnCalls)
        assertEquals(List(104), world.entityIds)
        assertEquals(0, world.removeCalls)
        assertEquals(0, gateway.releaseCalls)
        assertTrue(repository.findByUuid(uuid).get.exists(!_.isActive))
      }

      withRepository(path) { reopened =>
        val freshGateway = new CrashPointTestSupport.TestGateway(tickets)
        val freshChunks = new ChunkLoadingManager(reopened, freshGateway)
        val freshManager = new ActiveFloatingPodManager(reopened, freshChunks)
        val freshWorld = world.freshRuntime()
        assertTrue(tickets.runOrderedRecovery(freshChunks, freshGateway, freshWorld).isEmpty)
        assertEquals(0, tickets.liveTicketCount)
        val snapshot = StateSnapshot.capture(freshManager, reopened, freshWorld.entityIds).get
        assertTrue(snapshot.activePods.isEmpty)
        assertTrue(snapshot.activeInDb.isEmpty)
        assertTrue(snapshot.entityIds.isEmpty)
        assertTrue(snapshot.duplicatePersistedUuids.isEmpty)
      }
    }

  @Test
  def ticketBoundaryChecksReachGatewayBeforeAndAfterRequest(): Unit =
    val repository = new CrashPointTestSupport.MemoryRepository(CrashPointTestSupport.state(CrashPointTestSupport.TicketUuid, active = true))
    val restartModel = new CrashPointTestSupport.TicketRestartModel
    val beforeGateway = new CrashPointTestSupport.TestGateway(restartModel)
    val beforeManager = new ChunkLoadingManager(repository, beforeGateway)
    harness.enable()
    harness.register(CrashPointHarness.BeforeTicketRequest)
    assertThrows(classOf[CrashPointHarness.CrashPointException], () => beforeManager.requestTicket(CrashPointTestSupport.TicketUuid, new CrashPointTestSupport.TestWorld(105), 0, 0).get)
    assertEquals(0, beforeGateway.requestCalls)
    assertTrue(beforeManager.getAllManagedTickets().isEmpty)
    val beforeFreshGateway = new CrashPointTestSupport.TestGateway(restartModel)
    val beforeFreshManager = new ChunkLoadingManager(repository, beforeFreshGateway)
    assertTrue(restartModel.runOrderedRecovery(beforeFreshManager, beforeFreshGateway, new CrashPointTestSupport.TestWorld(105)).isEmpty)
    harness.disable()
    harness.reset()
    assertTrue(beforeFreshManager.requestTicket(CrashPointTestSupport.TicketUuid, new CrashPointTestSupport.TestWorld(105), 0, 0).isSuccess)
    assertTrue(beforeFreshManager.releaseTicket(CrashPointTestSupport.TicketUuid).isSuccess)

    harness.reset()
    val afterGateway = new CrashPointTestSupport.TestGateway(restartModel)
    val afterManager = new ChunkLoadingManager(repository, afterGateway)
    harness.enable()
    harness.register(CrashPointHarness.AfterTicketRequest)
    assertThrows(classOf[CrashPointHarness.CrashPointException], () => afterManager.requestTicket(CrashPointTestSupport.TicketUuid, new CrashPointTestSupport.TestWorld(106), 0, 0).get)
    assertEquals(1, afterGateway.requestCalls)
    assertNotNull(afterGateway.lastTicket)
    assertEquals(0, afterGateway.releaseCalls)
    assertTrue(afterManager.getAllManagedTickets().isEmpty)
    assertEquals(1, restartModel.liveTicketCount)
    val rawTicket = afterGateway.lastTicket
    val freshGateway = new CrashPointTestSupport.TestGateway(restartModel)
    val freshManager = new ChunkLoadingManager(repository, freshGateway)
    assertTrue(restartModel.runOrderedRecovery(freshManager, freshGateway, new CrashPointTestSupport.TestWorld(106)).isEmpty)
    assertEquals(2, restartModel.recoveryCallbackCalls)
    assertTrue(freshManager.getAllManagedTickets().isEmpty)
    assertEquals(0, restartModel.liveTicketCount)
    assertFalse(restartModel.isRegistered(rawTicket))
    assertEquals(1, freshGateway.releaseCalls)
    harness.disable()
    harness.reset()
    assertTrue(freshManager.requestTicket(CrashPointTestSupport.TicketUuid, new CrashPointTestSupport.TestWorld(106), 0, 0).isSuccess)
    assertTrue(freshManager.releaseTicket(CrashPointTestSupport.TicketUuid).isSuccess)

  @Test
  def periodicFlushBeforeCrashDoesNotPersistDirtyState(): Unit =
    runPeriodicCrash(CrashPointHarness.BeforePeriodicFlush, expectedPosition = 0.0D)

  @Test
  def periodicFlushAfterCrashKeepsSuccessfulPersistence(): Unit =
    runPeriodicCrash(CrashPointHarness.AfterPeriodicFlush, expectedPosition = 9.0D)

  private def runPeriodicCrash(point: String, expectedPosition: Double): Unit =
    withDatabase { path =>
      val uuid = CrashPointTestSupport.PeriodicUuid
      val oldWorld = new CrashPointTestSupport.TestWorld(107)
      withRepository(path) { repository =>
        val gateway = new CrashPointTestSupport.TestGateway
        val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, gateway), () => CrashPointTestSupport.FixedTimestamp)
        val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000817")
        repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
        repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
        repository.insert(CrashPointTestSupport.state(uuid, active = false)).get
        assertTrue(manager.spawn(uuid, CrashPointTestSupport.OwnerUuid, 0L, oldWorld, 0.0D, 64.0D, 0.0D, 256).isSuccess)
        assertEquals(List(107), oldWorld.entityIds)
        val dirty = repository.findByUuid(uuid).get.get.copy(posX = 9.0D)
        assertTrue(manager.updateCachedState(uuid, dirty).isSuccess)
        val executor = new CrashPointTestSupport.CapturingExecutor
        withCleanup({
          val service = PeriodicFlushService.forTesting(() => manager.flushCachedStates(), 30, () => executor)
          withCleanup({
            harness.enable()
            harness.register(point)
            assertTrue(service.start().isSuccess)
            assertNotNull(executor.command)
            assertThrows(classOf[CrashPointHarness.CrashPointException], () => executor.command.run())
            assertEquals(expectedPosition, repository.findByUuid(uuid).get.get.posX)
          }) {
            if service.isRunning then service.stop().get
          }
        }) {
          executor.cleanup()
        }
      }

      withRepository(path) { reopened =>
        val freshWorld = oldWorld.freshRuntime()
        val freshManager = new ActiveFloatingPodManager(reopened, new ChunkLoadingManager(reopened, new CrashPointTestSupport.TestGateway))
        val freshExecutor = new CrashPointTestSupport.CapturingExecutor
        withCleanup({
          val freshService = PeriodicFlushService.forTesting(() => freshManager.flushCachedStates(), 30, () => freshExecutor)
          withCleanup({
            harness.disable()
            harness.reset()
            assertTrue(freshService.start().isSuccess)
            assertNotNull(freshExecutor.command)
            freshExecutor.command.run()
            assertEquals(1L, freshService.getFlushCount)
            assertTrue(freshService.stop().isSuccess)
            val freshSnapshot = StateSnapshot.capture(freshManager, reopened, freshWorld.entityIds).get
            assertEquals(expectedPosition, reopened.findByUuid(uuid).get.get.posX)
            assertTrue(freshSnapshot.activePods.isEmpty)
            assertTrue(freshSnapshot.entityIds.isEmpty)
          }) {
            if freshService.isRunning then freshService.stop().get
          }
        }) {
          freshExecutor.cleanup()
        }
      }
    }

  private def withRepository[T](path: String)(test: SQLiteFloatingPodRepository => T): T =
    val repository = new SQLiteFloatingPodRepository(path)
    withCleanup(test(repository)) {
      repository.close()
    }

  private def withDatabase[T](test: String => T): T =
    val path = Files.createTempFile("floatingpod-crashpoint-", ".db")
    withCleanup(test(path.toString)) {
      deleteDatabaseFiles(path)
    }

  private def withCleanup[T](body: => T)(cleanup: => Unit): T =
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try cleanup
      catch
        case cleanupFailure: Throwable =>
          if primary != null then primary.addSuppressed(cleanupFailure)
          else throw cleanupFailure

  private def deleteDatabaseFiles(path: Path): Unit =
    var firstFailure: Throwable = null
    List(path, Path.of(path.toString + "-wal"), Path.of(path.toString + "-shm")).foreach { file =>
      try Files.deleteIfExists(file)
      catch
        case cleanupFailure: Throwable =>
          if firstFailure == null then firstFailure = cleanupFailure
          else firstFailure.addSuppressed(cleanupFailure)
    }
    if firstFailure != null then throw firstFailure

/** 共享 crashpoint 测试替身；所有边界均通过正式接口访问。 */
object CrashPointTestSupport:
  val FixedTimestamp = 1700000000000L
  val SpawnUuid = UUID.fromString("00000000-0000-0000-0000-000000000801")
  val RecallUuid = UUID.fromString("00000000-0000-0000-0000-000000000802")
  val BeforeEntityUuid = UUID.fromString("00000000-0000-0000-0000-000000000803")
  val AfterEntityUuid = UUID.fromString("00000000-0000-0000-0000-000000000804")
  val TicketUuid = UUID.fromString("00000000-0000-0000-0000-000000000805")
  val PeriodicUuid = UUID.fromString("00000000-0000-0000-0000-000000000806")
  val OwnerUuid = UUID.fromString("00000000-0000-0000-0000-000000000899")

  def state(uuid: UUID, active: Boolean): FloatingPodState =
    FloatingPodState(uuid, Some(OwnerUuid), 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, active, FixedTimestamp, if active then Some(0) else None)

  final class TestEntity(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity

  final class TestWorld(val nextEntityId: Int) extends PodWorld:
    var spawnCalls = 0
    var removeCalls = 0
    private val entities = mutable.LinkedHashMap.empty[UUID, TestEntity]
    override def dimensionId: Int = 0
    override def diagnosticWorldName: String = "crashpoint-test"
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = entities.get(podUuid)
    override def sameWorld(other: PodWorld): Boolean = this.eq(other)
    override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
      case entity: TestEntity => entities.get(entity.podUuid).exists(_ eq entity)
      case _ => false

    override def spawnPod(state: FloatingPodState): ActivePodEntity =
      spawnCalls += 1
      val entity = new TestEntity(state.uuid, nextEntityId)
      entities.put(state.uuid, entity)
      entity

    override def removePod(entity: ActivePodEntity): Unit =
      removeCalls += 1
      entities.remove(entity.podUuid)

    def entityIds: List[Int] = entities.valuesIterator.map(_.entityId).toList

    /** 创建不携带旧世界实体的确定性新运行时模型。 */
    def freshRuntime(): TestWorld = new TestWorld(nextEntityId)

  final class TestTicket extends ChunkTicket:
    var uuid: Option[UUID] = None

  /** 模拟 Forge 原始 ticket 的跨进程登记，并按有序回调返回值处置未接管票据。 */
  final class TicketRestartModel:
    private val liveTickets = mutable.LinkedHashSet.empty[ChunkTicket]
    private var callbackCalls = 0

    def register(ticket: ChunkTicket): Unit = liveTickets += ticket

    def release(ticket: ChunkTicket): Unit = liveTickets -= ticket

    def liveTicketCount: Int = liveTickets.size

    def recoveryCallbackCalls: Int = callbackCalls

    def isRegistered(ticket: ChunkTicket): Boolean = liveTickets.exists(_ eq ticket)

    /** 按 Forge 有序回调的选择结果保留全局 ticket，再执行产品的第二恢复回调。 */
    def runOrderedRecovery(manager: ChunkLoadingManager, gateway: ChunkTicketGateway, callbackWorld: PodWorld): List[ChunkTicket] =
      val selected = manager.selectRecoverableTickets(callbackWorld.dimensionId, liveTickets.toList, ChunkLoadingManager.DefaultChunkCount)
      liveTickets.filter(ticket => !selected.exists(_ eq ticket)).toList.foreach(gateway.release)
      callbackCalls += 1
      manager.recoverAcceptedTickets(selected, callbackWorld)
      selected

  final class TestGateway(restartModel: TicketRestartModel = new TicketRestartModel) extends ChunkTicketGateway:
    var requestCalls = 0
    var releaseCalls = 0
    var lastTicket: ChunkTicket = null
    val forced = mutable.Set.empty[(Int, Int)]

    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] =
      requestCalls += 1
      lastTicket = new TestTicket
      restartModel.register(lastTicket)
      Some(lastTicket)
    override def release(ticket: ChunkTicket): Unit =
      releaseCalls += 1
      restartModel.release(ticket)
    override def maxChunkDepth(ticket: ChunkTicket): Int = 25
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[TestTicket].uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = ticket.asInstanceOf[TestTicket].uuid
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = forced += ((chunkX, chunkZ))
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = forced -= ((chunkX, chunkZ))
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  final class MemoryRepository(initial: FloatingPodState) extends FloatingPodRepository, ActivityTransitionStore:
    val states = mutable.LinkedHashMap(initial.uuid -> initial)
    val duplicateRows = mutable.ListBuffer.empty[FloatingPodState]
    override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] =
      val stored = states.getOrElse(target.uuid, throw new IllegalStateException("missing pod"))
      if stored.ownerUuid != Some(authenticatedOwner) || stored.stateVersion != expectedStateVersion || stored.isActive then throw new IllegalStateException("invalid activation request")
      val committed = target.copy(isActive = true, dimensionId = Some(dimensionId), stateVersion = target.stateVersion + 1L)
      states.put(target.uuid, committed)
      Success(AdmissionOutcome.Accepted(committed))
    override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] =
      val stored = states.getOrElse(podUuid, throw new IllegalStateException("missing pod"))
      if stored.ownerUuid != expectedOwner || stored.stateVersion != expectedStateVersion || !stored.isActive then throw new IllegalStateException("invalid deactivation request")
      val committed = stored.copy(isActive = false, stateVersion = stored.stateVersion + 1L)
      states.put(podUuid, committed)
      Success(DeactivationOutcome.Accepted(committed))
    override def insert(state: FloatingPodState): Try[FloatingPodState] =
      states.put(state.uuid, state)
      Success(state)
    override def update(state: FloatingPodState): Try[FloatingPodState] =
      val persisted = state.copy(stateVersion = state.stateVersion + 1L)
      states.put(state.uuid, persisted)
      Success(persisted)
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(states.get(uuid))
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(states.values.filter(_.ownerUuid.contains(ownerUuid)).toList)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.filter(_.isActive).toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList ++ duplicateRows.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(states.remove(uuid).nonEmpty)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()

  final class CapturingExecutor extends ScheduledThreadPoolExecutor(1):
    @volatile var command: Runnable = null

    setExecuteExistingDelayedTasksAfterShutdownPolicy(false)

    override def scheduleAtFixedRate(command: Runnable, initialDelay: Long, period: Long, unit: TimeUnit): ScheduledFuture[?] =
      this.command = command
      super.scheduleAtFixedRate(command, 1L, 1L, TimeUnit.DAYS)

    def cleanup(): Unit =
      super.shutdownNow()
      try
        if !super.awaitTermination(3L, TimeUnit.SECONDS) then
          throw new IllegalStateException("Capturing executor did not terminate")
      catch
        case interruption: InterruptedException =>
          Thread.currentThread().interrupt()
          throw interruption
