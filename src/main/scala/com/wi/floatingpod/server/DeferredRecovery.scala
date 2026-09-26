package com.wi.floatingpod.server

/** deferred recovery 可推进的服务端 tick 阶段；当前不绑定 Forge 事件。 */
private[floatingpod] sealed trait ServerRecoveryTickPhase

private[floatingpod] object ServerRecoveryTickPhase:
  case object Start extends ServerRecoveryTickPhase
  case object End extends ServerRecoveryTickPhase

/** 延迟恢复的可观察步骤；只有 Terminal(Permit) 可以穿过 publication fence。 */
private[floatingpod] sealed trait RecoveryStep

private[floatingpod] object RecoveryStep:
  case object AwaitingTick extends RecoveryStep
  final case class Terminal(decision: StartupPublicationDecision) extends RecoveryStep:
    require(decision != null, "decision must not be null")

/** recovery 在生命周期中的唯一终止原因。 */
private[floatingpod] sealed trait RecoveryExit

private[floatingpod] object RecoveryExit:
  case object Published extends RecoveryExit
  case object PlannedRemediation extends RecoveryExit
  case object Blocked extends RecoveryExit
  case object AbortStartup extends RecoveryExit
  case object ExternalStopWhileRecovering extends RecoveryExit
  case object StoppedWithoutStopping extends RecoveryExit

/** runtime lifecycle 的终止证据；与 finish exit 分离以保留领域 payload 和原始 cause。 */
private[floatingpod] sealed trait RuntimeRecoveryTermination

private[floatingpod] object RuntimeRecoveryTermination:
  case object Published extends RuntimeRecoveryTermination
  final case class PlannedRemediation(podUuid: java.util.UUID) extends RuntimeRecoveryTermination:
    require(podUuid != null, "podUuid must not be null")
  final case class Blocked(reason: String, cause: Option[Throwable]) extends RuntimeRecoveryTermination:
    require(reason != null && reason.nonEmpty && cause != null, "blocked termination values must not be null or blank")
  final case class Aborted(cause: Throwable) extends RuntimeRecoveryTermination:
    require(cause != null, "cause must not be null")
  case object ExternalStopWhileRecovering extends RuntimeRecoveryTermination
  case object StoppedWithoutStopping extends RuntimeRecoveryTermination
  case object PreparedStopBeforeStarting extends RuntimeRecoveryTermination

/** 终止时已观察到的 cleanup 故障；clean 只表示 cleanup 没有失败。 */
private[floatingpod] final case class RecoveryFinishReport(exit: RecoveryExit, cleanupFailures: List[Throwable]):
  require(exit != null && cleanupFailures != null && cleanupFailures.forall(_ != null), "finish report values must not be null")
  def isClean: Boolean = cleanupFailures.isEmpty

/** 不实现实际恢复算法的 deferred state-machine 边界。 */
private[floatingpod] trait DeferredRecovery:
  def begin(): RecoveryStep
  def advance(phase: ServerRecoveryTickPhase): RecoveryStep
  def finish(exit: RecoveryExit): RecoveryFinishReport

/** 注册/解除 deferred tick callback；实现只请求 callback，不持有 durable state。 */
private[floatingpod] trait RuntimeRecoveryEvents:
  def register(callback: ServerRecoveryTickPhase => Unit): Unit
  def unregister(): Unit

/** 受控停服只发出请求，绝不承担持久化或实际停服。 */
private[floatingpod] trait RuntimeControlledStop:
  def requestStop(): Unit

/** ManagedServerRuntime 的一个可选 deferred recovery 依赖包。 */
private[floatingpod] final case class DeferredRecoveryBundle(recovery: DeferredRecovery, events: RuntimeRecoveryEvents, controlledStop: RuntimeControlledStop):
  require(recovery != null && events != null && controlledStop != null, "deferred recovery bundle values must not be null")
