package com.wi.floatingpod.server.persistence

import com.wi.floatingpod.crashpoint.CrashPointHarness
import com.wi.floatingpod.server.ActiveFloatingPodManager

import org.apache.logging.log4j.LogManager

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.util.{Failure, Success, Try}

/**
 * 独立于 Minecraft tick 的脏状态定时刷盘器，避免非关键位置状态长期只停留在内存中。
 */
final class PeriodicFlushService private (
    private val flushOperation: () => Try[Int],
    val intervalSeconds: Int,
    private val executorFactory: () => ScheduledExecutorService
):
  require(flushOperation != null, "flushOperation must not be null")
  require(executorFactory != null, "executorFactory must not be null")
  require(intervalSeconds > 0, "intervalSeconds must be positive")

  private val logger = LogManager.getLogger(getClass)
  private val lifecycleLock = new Object
  private val running = new AtomicBoolean(false)
  private val flushCount = new AtomicLong(0L)
  private val failureCount = new AtomicLong(0L)
  private var executor: ScheduledExecutorService = null

  /** 使用活动辅助机的脏缓存作为唯一写入准入条件创建生产刷盘器。 */
  def this(manager: ActiveFloatingPodManager, intervalSeconds: Int) =
    this(PeriodicFlushService.flushOperationFor(manager), intervalSeconds, PeriodicFlushService.newExecutor)

  /** 使用 30 秒生产默认间隔创建刷盘器。 */
  def this(manager: ActiveFloatingPodManager) =
    this(manager, PeriodicFlushService.DefaultIntervalSeconds)

  /**
   * 启动固定频率刷盘。调度失败时不会发布运行状态，并同步释放刚创建的 executor。
   *
   * @return 调度完成后的成功或原始调度失败。
   */
  def start(): Try[Unit] = Try {
    lifecycleLock.synchronized {
      if running.get() then throw new IllegalStateException("Periodic flush service is already running")
      if executor != null && !executor.isTerminated then
        throw new IllegalStateException("Previous periodic flush executor has not terminated")
      executor = null

      val candidate = executorFactory()
      if candidate == null then throw new IllegalStateException("Periodic flush executor factory returned null")
      try
        candidate.scheduleAtFixedRate(
          new Runnable:
            override def run(): Unit = executeFlush(),
          intervalSeconds.toLong,
          intervalSeconds.toLong,
          TimeUnit.SECONDS
        )
        executor = candidate
        flushCount.set(0L)
        failureCount.set(0L)
        running.set(true)
        logger.info(s"Periodic flush service started (interval: ${intervalSeconds}s)")
      catch
        case schedulingFailure: Throwable =>
          try candidate.shutdownNow()
          catch
            case cleanupFailure: Throwable =>
              schedulingFailure.addSuppressed(cleanupFailure)
              logger.error("Failed to release periodic flush executor after scheduling failure", cleanupFailure)
          throw schedulingFailure
    }
  }

  /**
   * 停止后续调度并等待当前刷盘至多十秒；超时会强制中断 executor 并报告失败。
   *
   * @return 当前任务终止后的成功或原始生命周期失败。
   */
  def stop(): Try[Unit] =
    try
      Try {
        lifecycleLock.synchronized {
          if !running.compareAndSet(true, false) then
            throw new IllegalStateException("Periodic flush service is not running")
          val activeExecutor = executor
          if activeExecutor == null then
            throw new IllegalStateException("Periodic flush service has no active executor")

          activeExecutor.shutdown()
          try
            if !activeExecutor.awaitTermination(PeriodicFlushService.StopTimeoutSeconds, TimeUnit.SECONDS) then
              logger.warn("Periodic flush service did not terminate within 10 seconds, forcing shutdown")
              activeExecutor.shutdownNow()
              if !activeExecutor.isTerminated then
                throw new IllegalStateException("Periodic flush executor did not terminate after forced shutdown")
            executor = null
            logger.info("Periodic flush service stopped")
          catch
            case interruption: InterruptedException =>
              try activeExecutor.shutdownNow()
              catch
                case cleanupFailure: Throwable =>
                  interruption.addSuppressed(cleanupFailure)
                  logger.error("Failed to force periodic flush executor shutdown after interruption", cleanupFailure)
              Thread.currentThread.interrupt()
              logger.error("Interrupted while waiting for periodic flush service to stop", interruption)
              throw interruption
        }
      }
    catch
      case interruption: InterruptedException => Failure(interruption)

  /** 返回当前是否仍接受并调度周期刷盘。 */
  def isRunning: Boolean = running.get()

  /** 返回本次启动以来成功完成的刷盘周期数。 */
  def getFlushCount: Long = flushCount.get()

  /** 返回本次启动以来失败的刷盘周期数。 */
  def getFailureCount: Long = failureCount.get()

  private def executeFlush(): Unit =
    try
      CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.BeforePeriodicFlush)
      flushOperation() match
        case Success(persistedCount) =>
          CrashPointHarness.getInstance.checkCrashPoint(CrashPointHarness.AfterPeriodicFlush)
          flushCount.incrementAndGet()
          logger.info(s"Periodic flush completed: $persistedCount dirty states persisted")
        case Failure(problem) =>
          failureCount.incrementAndGet()
          logger.error(s"Periodic flush failed: ${problem.getMessage}", problem)
    catch
      case crash: CrashPointHarness.CrashPointException => throw crash
      case problem: Throwable =>
        failureCount.incrementAndGet()
        logger.error(s"Unexpected error during periodic flush: ${problem.getMessage}", problem)

object PeriodicFlushService:
  /** 生产环境中非关键状态的默认最大回退周期。 */
  val DefaultIntervalSeconds = 30

  /** 优雅停服等待当前刷盘任务的最长时间。 */
  val StopTimeoutSeconds = 10L

  private[floatingpod] def forTesting(
      flushOperation: () => Try[Int],
      intervalSeconds: Int,
      executorFactory: () => ScheduledExecutorService = newExecutor
  ): PeriodicFlushService =
    new PeriodicFlushService(flushOperation, intervalSeconds, executorFactory)

  private def flushOperationFor(manager: ActiveFloatingPodManager): () => Try[Int] =
    require(manager != null, "manager must not be null")
    () => manager.flushCachedStates()

  private def newExecutor(): ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
