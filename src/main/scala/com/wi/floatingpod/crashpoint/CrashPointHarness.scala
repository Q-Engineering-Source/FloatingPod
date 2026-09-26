package com.wi.floatingpod.crashpoint

import org.apache.logging.log4j.LogManager

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/** 崩溃点注入接口；默认关闭，只有测试显式启用后才会抛出模拟进程崩溃异常。 */
trait CrashPointHarness:
  /** 启用崩溃点检查。 */
  def enable(): Unit

  /** 禁用崩溃点检查并清除已注册点。 */
  def disable(): Unit

  /** 注册一个待触发的崩溃点。 */
  def register(name: String): Unit

  /** 检查指定崩溃点，已注册时抛出异常。 */
  def checkCrashPoint(name: String): Unit

  /** 清除所有已注册崩溃点，但保留启用状态。 */
  def reset(): Unit

  /** 返回当前是否启用崩溃点检查。 */
  def isEnabled: Boolean

object CrashPointHarness:
  /** 数据库提交后的 spawn 边界。 */
  val AfterDbCommitSpawn = "after-db-commit-spawn"

  /** 数据库提交后的 recall 边界。 */
  val AfterDbCommitRecall = "after-db-commit-recall"

  /** 实体移除后的 recall 边界。 */
  val AfterRecallEntityRemoval = "after-recall-entity-removal"

  /** ticket 释放后的 recall 边界。 */
  val AfterRecallTicketRelease = "after-recall-ticket-release"

  /** 实体生成前边界。 */
  val BeforeEntitySpawn = "before-entity-spawn"

  /** 实体生成后边界。 */
  val AfterEntitySpawn = "after-entity-spawn"

  /** Forge ticket 申请前边界。 */
  val BeforeTicketRequest = "before-ticket-request"

  /** Forge ticket 申请后边界。 */
  val AfterTicketRequest = "after-ticket-request"

  /** 周期刷盘调用前边界。 */
  val BeforePeriodicFlush = "before-periodic-flush"

  /** 周期刷盘成功后的边界。 */
  val AfterPeriodicFlush = "after-periodic-flush"

  /** 全局崩溃点实现，确保生产路径只共享一个开关和注册表。 */
  private val instance: CrashPointHarness = new CrashPointHarnessImpl

  /** 返回全局崩溃点接口。 */
  def getInstance: CrashPointHarness = instance

  /** 表示模拟进程突然丢失，不属于普通业务回滚。 */
  final class CrashPointException(name: String)
      extends RuntimeException(s"Crash point triggered: $name")

/** 线程安全的崩溃点注册实现；关闭路径不访问注册表。 */
private final class CrashPointHarnessImpl extends CrashPointHarness:
  private val logger = LogManager.getLogger(getClass)
  private val enabled = new AtomicBoolean(false)
  private val registeredPoints = ConcurrentHashMap.newKeySet[String]()

  override def enable(): Unit =
    if enabled.compareAndSet(false, true) then
      logger.warn("CrashPointHarness enabled - DO NOT USE IN PRODUCTION")

  override def disable(): Unit =
    if enabled.compareAndSet(true, false) then
      registeredPoints.clear()
      logger.info("CrashPointHarness disabled")

  override def register(name: String): Unit =
    validateName(name)
    if enabled.get() then
      registeredPoints.add(name)
      logger.debug(s"Registered crash point: $name")

  override def checkCrashPoint(name: String): Unit =
    if !enabled.get() then return
    validateName(name)
    if registeredPoints.contains(name) then
      logger.warn(s"Triggering crash point: $name")
      throw new CrashPointHarness.CrashPointException(name)

  override def reset(): Unit =
    registeredPoints.clear()
    logger.debug("Reset all crash points")

  override def isEnabled: Boolean = enabled.get()

  private def validateName(name: String): Unit =
    require(name != null && name.trim.nonEmpty, "crash point name must not be blank")
