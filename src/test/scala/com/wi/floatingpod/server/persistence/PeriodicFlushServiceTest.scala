package com.wi.floatingpod.server.persistence

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ActivePodEntity, ChunkLoadingManager, ChunkTicket, ChunkTicketGateway, PodWorld}
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, DeactivationOutcome, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore}

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import java.util.{List => JavaList}
import java.util.concurrent.{CountDownLatch, RejectedExecutionException, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** 直接验证独立定时刷盘的生命周期、计数、失败续跑和空闲写入边界。 */
class PeriodicFlushServiceTest:
  /** 非正间隔必须在构造时失败，避免零延迟忙循环。 */
  @Test
  def rejectsNonPositiveIntervals(): Unit =
    assertThrows(
      classOf[IllegalArgumentException],
      () => PeriodicFlushService.forTesting(() => Success(0), 0)
    )

  /** 启动后发布运行状态，正常停止后清除运行状态。 */
  @Test
  def startsAndStopsWithConsistentRunningState(): Unit =
    val service = PeriodicFlushService.forTesting(() => Success(0), 1)

    assertTrue(service.start().isSuccess)
    assertTrue(service.isRunning)
    assertTrue(service.stop().isSuccess)
    assertFalse(service.isRunning)

  /** 正常停止后使用新 executor 重启，避免复用已关闭 executor。 */
  @Test
  def restartsAfterNormalStop(): Unit =
    val service = PeriodicFlushService.forTesting(() => Success(0), 1)

    assertTrue(service.start().isSuccess)
    assertTrue(service.stop().isSuccess)
    assertTrue(service.start().isSuccess)
    assertTrue(service.isRunning)
    assertTrue(service.stop().isSuccess)

  /** 重复启动或停止属于无效生命周期转换，必须直接返回失败。 */
  @Test
  def rejectsInvalidLifecycleTransitions(): Unit =
    val service = PeriodicFlushService.forTesting(() => Success(0), 1)

    assertTrue(service.start().isSuccess)
    assertTrue(service.start().isFailure)
    assertTrue(service.stop().isSuccess)
    assertTrue(service.stop().isFailure)

  /** 使用一秒测试间隔直接观察两个固定频率周期。 */
  @Test
  def executesFlushAtConfiguredOneSecondInterval(): Unit =
    val cycles = new AtomicInteger(0)
    val reachedTwoCycles = new CountDownLatch(1)
    val service = PeriodicFlushService.forTesting(
      () =>
        if cycles.incrementAndGet() >= 2 then reachedTwoCycles.countDown()
        Success(0),
      1
    )

    try
      assertTrue(service.start().isSuccess)
      assertTrue(reachedTwoCycles.await(4L, TimeUnit.SECONDS))
      assertTrue(awaitCondition(service.getFlushCount >= 2L, 2L))
    finally
      stopIfRunning(service)

  /** 每次成功周期精确增加成功计数，不增加失败计数。 */
  @Test
  def incrementsFlushCountOnSuccess(): Unit =
    val completed = new CountDownLatch(1)
    val calls = new AtomicInteger(0)
    val service = PeriodicFlushService.forTesting(
      () =>
        calls.incrementAndGet()
        completed.countDown()
        Success(0),
      1
    )

    try
      assertTrue(service.start().isSuccess)
      assertTrue(completed.await(3L, TimeUnit.SECONDS))
    finally
      stopIfRunning(service)
    assertEquals(calls.get().toLong, service.getFlushCount)
    assertEquals(0L, service.getFailureCount)

  /** 单次失败计数后，后续成功周期仍继续执行而不会被 ScheduledExecutorService 压制。 */
  @Test
  def recordsFailureAndContinuesWithLaterCycles(): Unit =
    val calls = new AtomicInteger(0)
    val continued = new CountDownLatch(1)
    val service = PeriodicFlushService.forTesting(
      () =>
        if calls.incrementAndGet() == 1 then Failure(new IllegalStateException("database unavailable"))
        else
          continued.countDown()
          Success(0),
      1
    )

    try
      assertTrue(service.start().isSuccess)
      assertTrue(continued.await(4L, TimeUnit.SECONDS))
      assertEquals(1L, service.getFailureCount)
      assertTrue(awaitCondition(service.getFlushCount >= 1L, 2L))
      assertTrue(service.isRunning)
    finally
      stopIfRunning(service)

  /** 直接抛出 Throwable 的刷盘周期也必须计数，并保持后续调度存活。 */
  @Test
  def catchesThrownThrowableAndContinuesWithLaterCycles(): Unit =
    val calls = new AtomicInteger(0)
    val continued = new CountDownLatch(1)
    val service = PeriodicFlushService.forTesting(
      () =>
        if calls.incrementAndGet() == 1 then throw new AssertionError("unexpected flush throwable")
        else
          continued.countDown()
          Success(0),
      1
    )

    try
      assertTrue(service.start().isSuccess)
      assertTrue(continued.await(4L, TimeUnit.SECONDS))
      assertEquals(1L, service.getFailureCount)
      assertTrue(awaitCondition(service.getFlushCount >= 1L, 2L))
      assertTrue(service.isRunning)
    finally
      stopIfRunning(service)

  /** stop 在已开始的刷盘释放闸门前保持等待，避免停服与在途写入并发。 */
  @Test
  def waitsForInFlightFlushOnStop(): Unit =
    val enteredFlush = new CountDownLatch(1)
    val releaseFlush = new CountDownLatch(1)
    val stopReturned = new CountDownLatch(1)
    val stopResult = new AtomicReference[Try[Unit]]()
    val service = PeriodicFlushService.forTesting(
      () =>
        enteredFlush.countDown()
        if !releaseFlush.await(3L, TimeUnit.SECONDS) then throw new IllegalStateException("test flush was not released")
        Success(1),
      1
    )

    assertTrue(service.start().isSuccess)
    assertTrue(enteredFlush.await(3L, TimeUnit.SECONDS))
    val stopper = new Thread(
      () =>
        stopResult.set(service.stop())
        stopReturned.countDown(),
      "periodic-flush-stop-test"
    )
    stopper.start()
    assertFalse(stopReturned.await(200L, TimeUnit.MILLISECONDS))
    releaseFlush.countDown()
    assertTrue(stopReturned.await(3L, TimeUnit.SECONDS))
    assertTrue(stopResult.get().isSuccess)
    assertFalse(service.isRunning)

  /** 调度被拒绝时不发布运行状态，且候选 executor 已关闭。 */
  @Test
  def schedulingFailureDoesNotPublishRunningStateOrLeakExecutor(): Unit =
    val rejectedExecutor = new ControlledExecutor(scheduleFailure = Some(new RejectedExecutionException("schedule rejected")))
    val service = PeriodicFlushService.forTesting(
      () => Success(0),
      1,
      () => rejectedExecutor
    )

    try
      assertFalse(rejectedExecutor.isShutdown)
      assertTrue(service.start().isFailure)
      assertFalse(service.isRunning)
      assertEquals(1, rejectedExecutor.shutdownNowCalls.get())
      assertTrue(rejectedExecutor.isShutdown)
    finally
      rejectedExecutor.cleanup()

  /** awaitTermination 超时必须强制关闭，并在 executor 仍存活时拒绝重启。 */
  @Test
  def timeoutStopForcesShutdownAndRejectsRestartWhileExecutorRemainsAlive(): Unit =
    val controlledExecutor = new ControlledExecutor(awaitResult = false, holdTermination = true)
    val service = PeriodicFlushService.forTesting(
      () => Success(0),
      1,
      () => controlledExecutor
    )

    try
      assertTrue(service.start().isSuccess)
      val stopResult = service.stop()
      assertTrue(stopResult.isFailure)
      assertEquals(1, controlledExecutor.shutdownCalls.get())
      assertEquals(1, controlledExecutor.shutdownNowCalls.get())
      assertFalse(service.isRunning)
      assertTrue(service.start().isFailure)
    finally
      controlledExecutor.markTerminated()
      controlledExecutor.cleanup()

  /** awaitTermination 中断必须保留原始失败、恢复中断标志并强制关闭。 */
  @Test
  def interruptedStopForcesShutdownAndRestoresInterruptFlag(): Unit =
    val interruption = new InterruptedException("stop interrupted")
    val controlledExecutor = new ControlledExecutor(awaitFailure = Some(interruption))
    val service = PeriodicFlushService.forTesting(
      () => Success(0),
      1,
      () => controlledExecutor
    )
    val stopResult = new AtomicReference[Try[Unit]]()
    val interruptRestored = new AtomicBoolean(false)
    val stopper = new Thread(
      () =>
        stopResult.set(service.stop())
        interruptRestored.set(Thread.currentThread().isInterrupted),
      "periodic-flush-interrupted-stop-test"
    )

    try
      assertTrue(service.start().isSuccess)
      stopper.start()
      stopper.join(3000L)
      assertFalse(stopper.isAlive)
      assertTrue(stopResult.get().isFailure)
      assertTrue(stopResult.get().failed.get eq interruption)
      assertEquals(1, controlledExecutor.shutdownNowCalls.get())
      assertTrue(interruptRestored.get())
      assertFalse(service.isRunning)
    finally
      if stopper.isAlive then
        stopper.interrupt()
        stopper.join(3000L)
      controlledExecutor.cleanup()

  /** 真实空闲 manager 周期只获得零脏状态结果，绝不调用 repository.update。 */
  @Test
  def cleanIdleManagerCyclePerformsNoRepositoryUpdates(): Unit =
    val repository = new PeriodicFlushTestSupport.Repository()
    val manager = PeriodicFlushTestSupport.idleManager(repository)
    val service = new PeriodicFlushService(manager, 1)

    try
      assertTrue(service.start().isSuccess)
      assertTrue(awaitCondition(service.getFlushCount >= 1L, 3L))
      assertEquals(0, repository.updateCalls)
    finally
      stopIfRunning(service)

  private def stopIfRunning(service: PeriodicFlushService): Unit =
    if service.isRunning then assertTrue(service.stop().isSuccess)

  private def awaitCondition(condition: => Boolean, timeoutSeconds: Long): Boolean =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds)
    while !condition && System.nanoTime() < deadline do Thread.sleep(20L)
    condition

/** 通过公开 executor API 控制调度与停机分支，避免反射和墙钟超时。 */
private final class ControlledExecutor(
    scheduleFailure: Option[Throwable] = None,
    awaitFailure: Option[InterruptedException] = None,
    awaitResult: Boolean = true,
    holdTermination: Boolean = false
) extends ScheduledThreadPoolExecutor(1):
  val shutdownCalls = new AtomicInteger(0)
  val shutdownNowCalls = new AtomicInteger(0)
  private val logicalTermination = new AtomicBoolean(false)

  override def scheduleAtFixedRate(
      command: Runnable,
      initialDelay: Long,
      period: Long,
      unit: TimeUnit
  ): ScheduledFuture[?] =
    scheduleFailure match
      case Some(problem) => throw problem
      case None => super.scheduleAtFixedRate(command, initialDelay, period, unit)

  override def shutdown(): Unit =
    shutdownCalls.incrementAndGet()
    super.shutdown()

  override def shutdownNow(): JavaList[Runnable] =
    shutdownNowCalls.incrementAndGet()
    super.shutdownNow()

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean =
    awaitFailure match
      case Some(interruption) => throw interruption
      case None =>
        if awaitResult then super.awaitTermination(timeout, unit)
        else false

  override def isTerminated: Boolean =
    if holdTermination then logicalTermination.get()
    else super.isTerminated

  def markTerminated(): Unit =
    logicalTermination.set(true)
    super.shutdownNow()

  def cleanup(): Unit =
    super.shutdownNow()
    try super.awaitTermination(3L, TimeUnit.SECONDS)
    catch
      case _: InterruptedException => Thread.currentThread().interrupt()

/** 提供两个 persistence 测试类共享的真实 manager、repository、Forge gateway 与世界替身。 */
object PeriodicFlushTestSupport:
  /** 创建没有活动或脏状态的真实 manager。 */
  def idleManager(repository: Repository): ActiveFloatingPodManager =
    new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, new Gateway()))

  /** 创建已生成一台活动辅助机的真实 manager，用于验证服务器停止顺序。 */
  def activeManager(removeStarted: CountDownLatch): ActiveFixture =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000701")
    val repository = new Repository(Some(stateFor(uuid, active = false)))
    val gateway = new Gateway()
    val world = new WorldStub(removeStarted)
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000703")
    val manager = new ActiveFloatingPodManager(repository, new ChunkLoadingManager(repository, gateway), () => 7L, () => attemptUuid)
    require(manager.spawn(uuid, stateFor(uuid, active = false).ownerUuid.get, 0L, world, 0.0D, 64.0D, 0.0D, 1).isSuccess, "test pod spawn must succeed")
    ActiveFixture(manager, repository, world, uuid)

  /** 测试用活动 manager 的组成快照。 */
  final case class ActiveFixture(
      manager: ActiveFloatingPodManager,
      repository: Repository,
      world: WorldStub,
      uuid: UUID
  )

  /** 记录 update 调用并为活动 manager 提供内存状态的仓储替身。 */
  final class Repository(initial: Option[FloatingPodState] = None) extends FloatingPodRepository, ActivityTransitionStore, PhysicalRecoveryStore:
    val states = mutable.HashMap.empty[UUID, FloatingPodState]
    initial.foreach(state => states.put(state.uuid, state))
    var updateCalls = 0
    private val locators = mutable.HashMap.empty[UUID, PhysicalRecoveryLocator]

    override def insert(state: FloatingPodState): Try[FloatingPodState] =
      states.put(state.uuid, state)
      Success(state)

    override def update(state: FloatingPodState): Try[FloatingPodState] =
      updateCalls += 1
      val persisted = state.copy(stateVersion = state.stateVersion + 1L)
      states.put(state.uuid, persisted)
      Success(persisted)

    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(states.get(uuid))
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.filter(_.isActive).toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(states.remove(uuid).nonEmpty)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()
    override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] =
      Failure(new UnsupportedOperationException("legacy activation is not used by this test adapter"))
    override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] =
      Failure(new UnsupportedOperationException("legacy deactivation is not used by this test adapter"))
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] =
      Failure(new UnsupportedOperationException("physical recovery binding is not used by this test adapter"))
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] =
      Failure(new UnsupportedOperationException("physical recovery binding is not used by this test adapter"))
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] =
      Failure(new UnsupportedOperationException("physical recovery binding is not used by this test adapter"))
    override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = Try {
      val locator = PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(expectedDomainVersion), Some(targetDimensionId), chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
      locators.put(podUuid, locator)
      locator
    }
    override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] =
      for
        locator <- Try(locators.getOrElse(target.uuid, throw new IllegalStateException("missing prepared locator")))
        _ <- Try {
          if locator.attemptUuid != attemptUuid || locator.state != PhysicalRecoveryLocatorState.CreatePrepared || locator.reasonCode != PhysicalRecoveryLocatorReason.CreatePrepared || locator.domainVersion != Some(expectedStateVersion) || locator.targetDimensionId != Some(dimensionId) then throw new IllegalStateException("unexpected test tracked activation locator")
        }
        outcome <- activate(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit)
      yield outcome match
        case accepted @ AdmissionOutcome.Accepted(committed) =>
          locators.put(target.uuid, locator.copy(state = PhysicalRecoveryLocatorState.RowActive, reasonCode = PhysicalRecoveryLocatorReason.RowActive, domainVersion = Some(committed.stateVersion)))
          accepted
        case rejected => rejected
    override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] =
      for
        locator <- Try(locators.getOrElse(podUuid, throw new IllegalStateException("missing active locator")))
        _ <- Try {
          if locator.attemptUuid != attemptUuid || locator.state != PhysicalRecoveryLocatorState.RowActive || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive || locator.domainVersion != Some(expectedStateVersion) then throw new IllegalStateException("unexpected test tracked deactivation locator")
        }
        outcome <- deactivate(podUuid, expectedOwner, expectedStateVersion)
      yield outcome match
        case accepted @ DeactivationOutcome.Accepted(committed) =>
          locators.put(podUuid, locator.copy(state = PhysicalRecoveryLocatorState.RowInactivePending, reasonCode = PhysicalRecoveryLocatorReason.RowInactivePending, domainVersion = Some(committed.stateVersion)))
          accepted
        case rejected => rejected
    override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = Success(locators.get(podUuid))
    private def activate(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] = Try {
      val current = states.getOrElse(target.uuid, throw new IllegalStateException("missing pod"))
      if !current.ownerUuid.contains(authenticatedOwner) || current.stateVersion != expectedStateVersion || current.isActive then throw new IllegalStateException("unexpected test activation")
      val committed = target.copy(isActive = true, dimensionId = Some(dimensionId), stateVersion = current.stateVersion + 1L)
      states.put(committed.uuid, committed)
      AdmissionOutcome.Accepted(committed)
    }
    private def deactivate(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] = Try {
      val current = states.getOrElse(podUuid, throw new IllegalStateException("missing pod"))
      if !current.isActive || current.ownerUuid != expectedOwner || current.stateVersion != expectedStateVersion then throw new IllegalStateException("unexpected test deactivation")
      val committed = current.copy(isActive = false, stateVersion = current.stateVersion + 1L)
      states.put(podUuid, committed)
      DeactivationOutcome.Accepted(committed)
    }

  /** 不依赖 Forge 原始 Ticket 构造器的测试票据。 */
  final class TestTicket extends ChunkTicket:
    var uuid: Option[UUID] = None

  /** 支持 spawn、recall 和空闲 manager 构造的最小票据网关。 */
  final class Gateway extends ChunkTicketGateway:
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
    override def request(world: PodWorld): Option[ChunkTicket] = Some(new TestTicket)
    override def release(ticket: ChunkTicket): Unit = ()
    override def maxChunkDepth(ticket: ChunkTicket): Int = ChunkLoadingManager.DefaultChunkCount
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[TestTicket].uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] = ticket.asInstanceOf[TestTicket].uuid
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  /** 在 removePod 时通知测试，直接暴露服务器停止已进入 recall 阶段。 */
  final class WorldStub(removeStarted: CountDownLatch) extends PodWorld:
    var spawnCalls = 0
    var removeCalls = 0
    private val entities = mutable.HashMap.empty[UUID, EntityStub]
    override val dimensionId: Int = 7
    override def diagnosticWorldName: String = "overworld"
    override def sameWorld(other: PodWorld): Boolean = other eq this
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = entities.get(podUuid)

    override def spawnPod(state: FloatingPodState): ActivePodEntity =
      spawnCalls += 1
      val entity = new EntityStub(state.uuid, 701)
      entities.put(entity.podUuid, entity)
      entity

    override def removePod(entity: ActivePodEntity): Unit =
      removeCalls += 1
      removeStarted.countDown()
      entities.remove(entity.podUuid)

  /** 提供稳定数值 ID 的活动实体替身。 */
  final class EntityStub(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity

  private def stateFor(uuid: UUID, active: Boolean): FloatingPodState =
    FloatingPodState(uuid, Some(UUID.fromString("00000000-0000-0000-0000-000000000702")), 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, active, 1L, None)
