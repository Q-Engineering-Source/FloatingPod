package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.storage.mysql.*
import java.time.Duration
import java.util.concurrent.TimeUnit

/** 真实 adapter 的退出观察；Terminated 不等于 JDBC/native/Boot root 已结算。 */
private[floatingpod] final class ClaimedBootstrapExit(adapter: MySqlOwnedIoAdapter, main: Thread, limit: Duration):
  require(adapter != null && main != null && limit != null && !limit.isNegative && !limit.isZero,
    "original adapter/Main and bounded exit limit required")
  @volatile private var primary: Throwable = null
  @volatile private var actualWait: Option[MySqlOwnedIoWorkerTerminationWait] = None
  @volatile private var actualCheckpoint: Option[MySqlOwnedIoCheckpoint] = None
  private var attempted = false
  private var tail: Thread = null // retained even if Thread.start fails.
  def failure: Option[Throwable] = Option(primary)
  def waitObservation: Option[MySqlOwnedIoWorkerTerminationWait] = actualWait
  def checkpoint: Option[MySqlOwnedIoCheckpoint] = actualCheckpoint
  private def suppress(cause: Throwable): Unit = synchronized {
    require(cause != null, "original cause required")
    if primary == null then primary = cause
    else if (primary ne cause) && !primary.getSuppressed.contains(cause) then primary.addSuppressed(cause)
  }
  def retainFailure(cause: Throwable): Unit =
    require(Thread.currentThread() eq main, "original Main responsibility required")
    suppress(cause)
  def begin(cause: Throwable): Unit =
    require(Thread.currentThread() eq main, "original Main quiesce required")
    require(!attempted, "claimed exit is one-shot")
    attempted = true
    suppress(cause)
    // Boot input/custody is still retained: normal writer release is not authorized by lifecycle events.
    try adapter.retainUnsettledStop(cause)
    catch case failed: Throwable => suppress(failed)
    try adapter.quiesce()
    catch case failed: Throwable => suppress(failed)
    observeExitOffMain()
  private[floatingpod] def beginCertifiedFresh(callbacks: ProductionDedicatedBootstrapCallbacks,
      drop: com.wi.floatingpod.server.MainBootProjection.FreshReadDrop): Unit =
    require(Thread.currentThread() eq main, "original Main Fresh completion required")
    require(!attempted, "claimed exit is one-shot")
    attempted = true
    try adapter.quiesceForFreshCompletion(callbacks, drop)
    catch
      case cause: Throwable =>
        suppress(cause)
        try adapter.retainUnsettledStop(cause)
        catch case cleanup: Throwable => suppress(cleanup)
        try adapter.quiesce()
        catch case cleanup: Throwable => suppress(cleanup)
    observeExitOffMain()
  private[floatingpod] def beginCertifiedOrdinary(callbacks: ProductionDedicatedBootstrapCallbacks,
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    if (Thread.currentThread() ne main) || attempted then throw new IllegalStateException("original Main Ordinary exit is one-shot")
    attempted=true
    try adapter.quiesceForOrdinaryCompletion(callbacks,runtime,receipt)
    catch
      case cause: Throwable =>
        suppress(cause)
        try adapter.retainUnsettledStop(cause)
        catch case cleanup: Throwable => suppress(cleanup)
        try adapter.quiesce()
        catch case cleanup: Throwable => suppress(cleanup)
    observeExitOffMain(true)

  private[floatingpod] def beginCertifiedDbFirst(callbacks:ProductionDbFirstBootstrapCallbacks,
      runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    require((Thread.currentThread() eq main) && !attempted,"original Main schema6 exit is one-shot")
    attempted=true
    try adapter.quiesceForDbFirstCompletion(callbacks,runtime,receipt)
    catch
      case cause:Throwable=>
        suppress(cause)
        try adapter.retainUnsettledStop(cause)
        catch case cleanup:Throwable=>suppress(cleanup)
        try adapter.quiesce()
        catch case cleanup:Throwable=>suppress(cleanup)
    observeExitOffMain(true)

  /** 实际observer退栈是Main正常许可的必要条件；worker Terminated本身不够。 */
  private[floatingpod] def requireOrdinaryObservationReturned(): Unit =
    requireObservationReturned()
  private[floatingpod] def requireFreshObservationReturned():Unit =
    requireObservationReturned()
  private def requireObservationReturned():Unit =
    if (Thread.currentThread() ne main) || !attempted || tail == null || tail.getState != Thread.State.TERMINATED || tail.isAlive then
      throw new IllegalStateException("actual Ordinary observation thread has not returned")
    if primary != null then throw primary
    actualWait match
      case Some(MySqlOwnedIoWorkerTerminationWait.Terminated(proof)) => adapter.requireOwnWorkerTerminationProof(proof)
      case _ => throw new IllegalStateException("actual Ordinary worker termination proof required")

  private[floatingpod] def ordinaryObservationPending: Boolean =
    if Thread.currentThread() ne main then throw new IllegalStateException("original Main observation owner required")
    if primary != null then throw primary
    tail == null || tail.getState != Thread.State.TERMINATED || tail.isAlive
  private[floatingpod] def freshObservationPending:Boolean =ordinaryObservationPending

  private def observeExitOffMain(ordinary: Boolean = false): Unit =
    val work: Runnable = () => {
      try
        val result = adapter.awaitWorkerTerminationOffMain(limit.toNanos, TimeUnit.NANOSECONDS)
        actualWait = Some(result)
        result match
          case MySqlOwnedIoWorkerTerminationWait.Terminated(proof) => adapter.requireOwnWorkerTerminationProof(proof)
          case MySqlOwnedIoWorkerTerminationWait.TimedOut => suppress(new IllegalStateException("claimed worker exit timed out; responsibility retained"))
          case MySqlOwnedIoWorkerTerminationWait.Interrupted(failed) => suppress(failed)
      catch case failed: Throwable => suppress(failed)
      finally
        try
          val observed = adapter.checkpoint
          actualCheckpoint = Some(observed)
          observed.io.fatalCause.foreach(suppress)
          observed.io.exitFailure.foreach(suppress)
        catch case failed: Throwable => suppress(failed)
    }
    tail =
      if ordinary then Thread.ofPlatform().name("FloatingPod Ordinary exit observation").inheritInheritableThreadLocals(false).unstarted(work)
      else new Thread(work, "FloatingPod claimed bootstrap exit observation")
    try tail.start()
    catch case failed: Throwable => suppress(failed)
