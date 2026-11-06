package com.wi.floatingpod.server

import java.util.UUID
import com.wi.floatingpod.server.spatial.SpatialEndScope

private[floatingpod] object PermanentMainPumpCore:
  /** 构造权只在 core；闭包保留原调用身份，不接受外部自填校验器。 */
  private[server] final class SpatialScopePermit private[PermanentMainPumpCore] (
      private val check: () => PermanentPumpLease,
      private val recordFailure: Throwable => Unit
  ):
    private[server] def validate(): Unit = { check(); () }
    private[server] def validateExactLease(expected: PermanentPumpLease): Unit =
      require(expected != null, "original spatial consumer lease is required")
      val actual = check()
      require(actual eq expected, "spatial scope requires the exact original consumer lease")
    private[server] def failed(cause: Throwable): Unit = recordFailure(cause)

private[floatingpod] enum PermanentPumpPhase:
  case Recovering, Ready, Quiescing

/** END ordinal 属于该 boot 的真实已接收 END，与 vanilla int tickCounter 分离。 */
private[floatingpod] final case class PermanentEndTick(
    bootUuid: UUID,
    bootGeneration: Long,
    endOrdinal: Long,
    vanillaEndCounter: Int,
    phase: PermanentPumpPhase
)

/** exact 引用 lease 阻止旧 boot 的 retire 操作撤销当前 callback。 */
private[floatingpod] final class PermanentPumpLease private[server] (
    val bootUuid: UUID,
    val bootGeneration: Long
)

/** Main 独占状态；不持 monitor，也不访问 JDBC、world 或 IO。 */
private[floatingpod] final class PermanentMainPumpCore(serverIdentity: AnyRef, mainThread: Thread):
  require(serverIdentity != null && mainThread != null, "exact server and Main thread are required")
  private var current: PermanentPumpLease = null
  private var callback: PermanentEndTick => Unit = null
  private var phase = PermanentPumpPhase.Recovering
  private var lastGeneration = 0L
  private var ordinal = 0L
  private var startedCounter: Option[Int] = None
  private var previousEnd: Option[Int] = None
  private var dispatching = false
  private var callbackFailure: Throwable = null
  private var currentDispatchToken: PermanentEndTick = null
  private var spatialScopeOpened = false
  private var activeSpatialScope: SpatialEndScope = null
  private var spatialFailure: Throwable = null

  private[server] def assertMain(): Unit =
    if Thread.currentThread() ne mainThread then throw new IllegalStateException("Permanent pump requires Main thread")

  private def checkServer(actual: AnyRef): Unit =
    assertMain()
    if actual ne serverIdentity then throw new IllegalStateException("Permanent pump server identity changed")

  def install(boot: UUID, generation: Long, next: PermanentEndTick => Unit): PermanentPumpLease =
    assertMain()
    require(boot != null && next != null && generation > lastGeneration, "fresh boot generation and callback are required")
    if dispatching then throw new IllegalStateException("Permanent pump cannot install during callback")
    if current != null then throw new IllegalStateException("Permanent pump already owns a boot")
    val lease = new PermanentPumpLease(boot, generation)
    current = lease
    callback = next
    phase = PermanentPumpPhase.Recovering
    lastGeneration = generation
    ordinal = 0L
    startedCounter = None
    previousEnd = None
    callbackFailure = null
    lease

  def changePhase(lease: PermanentPumpLease, next: PermanentPumpPhase): Unit =
    assertMain()
    require(next != null, "pump phase is required")
    if lease == null || (lease ne current) then throw new IllegalStateException("Permanent pump lease is stale")
    if callbackFailure != null then throw new IllegalStateException("Permanent pump callback failed", callbackFailure)
    if next.ordinal < phase.ordinal then throw new IllegalStateException("Permanent pump phase cannot regress")
    phase = next

  private[server] def fail(cause: Throwable): Unit =
    assertMain()
    require(cause != null, "pump failure is required")
    callbackFailure = cause

  def isCurrent(lease: PermanentPumpLease): Boolean =
    assertMain()
    lease != null && (lease eq current)

  /** package-owned endpoint只观察actual当前lease/phase；不能由caller填Ready。 */
  private[server] def assertReadyCurrent(lease: PermanentPumpLease): Unit =
    assertMain()
    require(lease != null && (lease eq current) && phase == PermanentPumpPhase.Ready,
      "Debug endpoint requires current actual Ready pump lease")
    if callbackFailure != null then throw new IllegalStateException("Permanent pump callback failed", callbackFailure)

  /** 内部Boot装配只承认实际正在分派的原Recovering END，不授予业务执行。 */
  private[server] def assertRecoveringDispatch(lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    assertMain()
    require(lease!=null && (lease eq current) && phase==PermanentPumpPhase.Recovering &&
      dispatching && tick!=null && (tick eq currentDispatchToken) && tick.phase==PermanentPumpPhase.Recovering,
      "Boot publication requires original current Recovering END dispatch")
    if callbackFailure!=null then throw new IllegalStateException("Permanent pump callback failed",callbackFailure)

  private def assertSpatialDispatch(lease: PermanentPumpLease, tick: PermanentEndTick): Unit =
    assertReadyCurrent(lease)
    require(dispatching && tick != null && (tick eq currentDispatchToken) &&
      tick.phase == PermanentPumpPhase.Ready, "spatial scope requires the original current Ready END callback")
    if spatialFailure != null then throw new IllegalStateException("Spatial END scope failed", spatialFailure)

  private def recordSpatialFailure(cause: Throwable): Unit =
    assertMain()
    if spatialFailure == null then spatialFailure = cause
    else if spatialFailure ne cause then spatialFailure.addSuppressed(cause)

  /** 每个真实 Ready END 至多开一次；返回、异常或原 callback 结束都使外泄 scope 失效。 */
  private[server] def withSpatialEndScope[A](lease: PermanentPumpLease, tick: PermanentEndTick)(
      consumer: SpatialEndScope => A
  ): A =
    assertSpatialDispatch(lease, tick)
    require(consumer != null, "spatial scope consumer is required")
    if spatialScopeOpened then throw new IllegalStateException("Spatial scope already opened for this END")
    spatialScopeOpened = true
    val permit = new PermanentMainPumpCore.SpatialScopePermit(
      () => {
        assertSpatialDispatch(lease, tick)
        if activeSpatialScope == null then throw new IllegalStateException("Spatial scope consumption has ended")
        lease
      },
      cause => recordSpatialFailure(cause)
    )
    val scope = SpatialEndScope.create(permit, tick)
    activeSpatialScope = scope
    try consumer(scope)
    catch
      case cause: Throwable =>
        recordSpatialFailure(cause)
        throw cause
    finally activeSpatialScope = null

  /** False 是旧 lease 的明确无副作用结果，不会对新 boot 调用 unregister。 */
  def retire(lease: PermanentPumpLease): Boolean =
    assertMain()
    if !isCurrent(lease) then false
    else
      current = null
      callback = null
      startedCounter = None
      true

  def start(actual: AnyRef, counter: Int): Unit =
    checkServer(actual)
    if current == null then return
    if dispatching || startedCounter.nonEmpty then throw new IllegalStateException("Permanent pump duplicate or reentrant START")
    if callbackFailure != null then throw new IllegalStateException("Permanent pump callback failed", callbackFailure)
    previousEnd.foreach { end =>
      if counter != end then throw new IllegalStateException("Permanent pump missed a vanilla tick")
    }
    startedCounter = Some(counter)

  def end(actual: AnyRef, counter: Int): Unit =
    checkServer(actual)
    if current == null then return
    if dispatching then throw new IllegalStateException("Permanent pump reentrant END")
    if callbackFailure != null then throw new IllegalStateException("Permanent pump callback failed", callbackFailure)
    val start = startedCounter.getOrElse(throw new IllegalStateException("Permanent pump END has no unique START"))
    // actual patched START occurs before vanilla increments its int tickCounter.
    if counter != start + 1 then throw new IllegalStateException("Permanent pump START/END counter mismatch")
    val lease = current
    val next = callback
    ordinal = Math.incrementExact(ordinal)
    startedCounter = None
    previousEnd = Some(counter)
    val token = PermanentEndTick(lease.bootUuid, lease.bootGeneration, ordinal, counter, phase)
    dispatching = true
    currentDispatchToken = token
    spatialScopeOpened = false
    spatialFailure = null
    try
      next(token)
      // 即使上层误吞 scope 内异常，也不能把失败 END 当健康结束。
      if spatialFailure != null then throw spatialFailure
    catch
      case cause: Throwable =>
        val original = if spatialFailure == null then cause else spatialFailure
        if original ne cause then original.addSuppressed(cause)
        callbackFailure = original
        throw original
    finally
      activeSpatialScope = null
      currentDispatchToken = null
      dispatching = false
