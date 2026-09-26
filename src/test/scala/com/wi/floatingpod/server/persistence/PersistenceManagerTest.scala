package com.wi.floatingpod.server.persistence

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.util.Try

/** 直接验证 Forge 服务器生命周期中刷盘器与活动辅助机回收的顺序。 */
class PersistenceManagerTest:
  /** 启动事件只启动独立刷盘器，未创建额外持久化工作。 */
  @Test
  def startsFlushServiceOnServerStart(): Unit =
    val repository = new PeriodicFlushTestSupport.Repository()
    val manager = PeriodicFlushTestSupport.idleManager(repository)
    val service = new PeriodicFlushService(manager)
    val persistenceManager = new PersistenceManager(manager, service)

    try
      assertTrue(persistenceManager.onServerStarting(null).isSuccess)
      assertTrue(service.isRunning)
      assertEquals(0, repository.updateCalls)
    finally
      if service.isRunning then assertTrue(service.stop().isSuccess)

  /** 停服先观察到服务停止，再进入真实 active manager 的实体收回。 */
  @Test
  def stopsFlushBeforeRecallingActivePods(): Unit =
    val removeStarted = new CountDownLatch(1)
    val fixture = PeriodicFlushTestSupport.activeManager(removeStarted)
    val service = new PeriodicFlushService(fixture.manager)
    val persistenceManager = new PersistenceManager(fixture.manager, service)
    val result = new AtomicReference[Try[Int]]()

    assertTrue(persistenceManager.onServerStarting(null).isSuccess)
    val stoppingThread = new Thread(
      () => result.set(persistenceManager.onServerStopping(null)),
      "persistence-manager-stop-test"
    )
    stoppingThread.start()
    assertTrue(removeStarted.await(3L, TimeUnit.SECONDS))
    assertFalse(service.isRunning)
    stoppingThread.join(3000L)
    assertFalse(stoppingThread.isAlive)
    assertTrue(result.get().isSuccess)
    assertEquals(1, result.get().get)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  /** 即使刷盘器未启动导致 stop 失败，停服仍继续回收真实活动辅助机。 */
  @Test
  def continuesGracefulShutdownWhenFlushServiceStopFails(): Unit =
    val fixture = PeriodicFlushTestSupport.activeManager(new CountDownLatch(1))
    val service = new PeriodicFlushService(fixture.manager)
    val persistenceManager = new PersistenceManager(fixture.manager, service)

    val result = persistenceManager.onServerStopping(null)

    assertTrue(result.isSuccess)
    assertEquals(1, result.get)
    assertTrue(fixture.manager.getActivePod(fixture.uuid).isEmpty)

  /** 结构化停服结果不能把刷盘失败折叠成成功。 */
  @Test
  def stoppingReportRetainsFlushFailure(): Unit =
    val fixture = PeriodicFlushTestSupport.activeManager(new CountDownLatch(1))
    val service = new PeriodicFlushService(fixture.manager)
    val persistenceManager = new PersistenceManager(fixture.manager, service)

    val report = persistenceManager.stopWithReport(null)

    assertFalse(report.isClean)
    assertTrue(report.flushStopFailure.nonEmpty)
