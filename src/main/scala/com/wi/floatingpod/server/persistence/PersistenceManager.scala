package com.wi.floatingpod.server.persistence

import com.wi.floatingpod.server.{ActiveFloatingPodManager, ManagerShutdownReport}

import net.minecraftforge.fml.common.event.{FMLServerStartingEvent, FMLServerStoppingEvent}

import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}

/**
 * 将独立刷盘器接入 Forge 服务器生命周期，确保停服时先阻止后台写入再收回活动辅助机。
 */
final class PersistenceManager(manager: ActiveFloatingPodManager, flushService: PeriodicFlushService):
  require(manager != null, "manager must not be null")
  require(flushService != null, "flushService must not be null")

  private val logger = LogManager.getLogger(getClass)

  /**
   * 在服务器启动阶段只启动独立刷盘器，不触发实体恢复或关键状态写入。
   *
   * @param event Forge 服务器启动事件。
   * @return 刷盘器的原始启动结果。
   */
  def onServerStarting(event: FMLServerStartingEvent): Try[Unit] =
    val result = flushService.start()
    result match
      case Success(_) => logger.info("Persistence manager initialized")
      case Failure(problem) => logger.error(s"Failed to start periodic flush service: ${problem.getMessage}", problem)
    result

  /**
   * 在停服阶段先停止后台刷盘，再继续活动辅助机回收；刷盘停止失败不会阻断资源回收。
   *
   * @param event Forge 服务器停止事件。
   * @return 活动辅助机回收的原始结果。
   */
  def stopWithReport(event: FMLServerStoppingEvent): PersistenceStoppingReport =
    val flushStopFailure = flushService.stop().failed.toOption
    flushStopFailure.foreach(problem => logger.warn(s"Failed to stop periodic flush service: ${problem.getMessage}", problem))
    val managerReport = manager.shutdownWithReport()
    if managerReport.isClean then logger.info(s"Graceful shutdown completed, ${managerReport.recalledUuids.size} pods recalled")
    else logger.error("Graceful shutdown completed with unresolved FloatingPod resources")
    PersistenceStoppingReport(flushStopFailure, managerReport)

  /** 启动中止只停止后台刷盘；不得把尚未成功启动的 runtime 误当正常停服而召回辅助机。 */
  def abortStarted(): Unit = flushService.stop().get

  /** 兼容旧调用方；新 runtime 使用 stopWithReport 保留所有停服原因。 */
  def onServerStopping(event: FMLServerStoppingEvent): Try[Int] = Try(stopWithReport(event).managerReport.recalledUuids.size)

/** 持久化阶段的结构化停服结果。 */
final case class PersistenceStoppingReport(flushStopFailure: Option[Throwable], managerReport: ManagerShutdownReport):
  require(flushStopFailure != null, "flushStopFailure must not be null")
  require(managerReport != null, "managerReport must not be null")
  def isClean: Boolean = flushStopFailure.isEmpty && managerReport.isClean
