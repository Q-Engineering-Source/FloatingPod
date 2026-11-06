package com.wi.floatingpod.server

import net.minecraft.server.MinecraftServer

/** 只推进原 deferred 算法；Permit 是物理恢复结论，不发行 Ready 或业务资格。 */
private[server] final class ClaimedRecoveryProgress(recovery: DeferredRecovery):
  private var begun = false
  private var stopped = false
  private var finishAttempted = false
  private var decision = Option.empty[StartupPublicationDecision]
  private var failure = Option.empty[Throwable]
  def result: Option[StartupPublicationDecision] = decision
  private def accept(step: RecoveryStep): Unit = step match
    case RecoveryStep.AwaitingTick => ()
    case RecoveryStep.Terminal(value) => decision = Some(value)
  def begin(): Unit =
    require(!begun && !stopped, "claimed recovery begins exactly once")
    begun = true
    try accept(recovery.begin())
    catch
      case cause: Throwable =>
        failure = Some(cause)
        throw cause
  def advance(phase: ServerRecoveryTickPhase): Unit =
    require(begun && !stopped, "claimed recovery is not active")
    failure.foreach(throw _)
    if decision.isEmpty then
      try accept(recovery.advance(phase))
      catch
        case cause: Throwable =>
          failure = Some(cause)
          throw cause
  private def finishOnce(exit: RecoveryExit): Unit =
    require(!finishAttempted, "original recovery finish is one-shot")
    finishAttempted = true // finish may throw after its side effect; never issue another exit.
    val report = recovery.finish(exit)
    if report.cleanupFailures.nonEmpty then
      val primary = report.cleanupFailures.head
      report.cleanupFailures.tail.foreach { secondary =>
        if (secondary ne primary) && !primary.getSuppressed.contains(secondary) then primary.addSuppressed(secondary)
      }
      throw primary
    require(report.exit == exit && report.isClean, "original recovery finish report must match its exit")
  def published(unregister: () => Unit): Unit =
    require(begun && !stopped && !finishAttempted && unregister != null &&
      decision.exists(_.isInstanceOf[StartupPublicationDecision.Permit]),
      "only original physical Permit may finish Published once")
    stopped = true // failed Published never falls back to ExternalStopWhileRecovering.
    var primary = Option.empty[Throwable]
    try finishOnce(RecoveryExit.Published)
    catch case cause: Throwable => primary = Some(cause)
    try unregister()
    catch case cleanup: Throwable => primary match
      case Some(original) => if (cleanup ne original) && !original.getSuppressed.contains(cleanup) then original.addSuppressed(cleanup)
      case None => primary = Some(cleanup)
    primary.foreach { cause => failure = Some(cause); throw cause }
  def stop(): Unit =
    if !stopped then
      stopped = true
      if begun && !finishAttempted then finishOnce(RecoveryExit.ExternalStopWhileRecovering)

/** 原 Boot 的 Main owner；真实 START 来自 Forge，END 由同一永久 pump 消费。 */
private[floatingpod] final class ClaimedBootActiveRecovery private (
    server: MinecraftServer, reads: MainBootProjection,
    val tickets: ChunkLoadingManager, val manager: ActiveFloatingPodManager,
    recovery: DeferredRecovery, events: RuntimeRecoveryEvents, onFailure: Throwable => Unit
):
  private val main = server.getServerThread
  private val progress = new ClaimedRecoveryProgress(recovery)
  private var installed = false
  private var started = false
  private var stopping = false
  private var publicationAttempted = false
  private var unregisterAttempted = false
  private var observedEnd = Option.empty[Int]
  private var lastEndOrdinal = 0L
  private var blocked = Option.empty[StartupPublicationDecision]
  private def current(): Unit =
    require(Thread.currentThread() eq main, "claimed recovery requires actual dedicated Main")
    reads.requireCurrentReads()
  def result: Option[StartupPublicationDecision] = { current(); blocked.orElse(progress.result) }
  def install(): Unit =
    current()
    require(!installed && !stopping, "claimed recovery installation is one-shot")
    installed = true // retain registration responsibility even if the bus throws after its side effect.
    LiveCrashRecovery.claimedRecoveryBlock(reads) match
      case Some(reason) => blocked = Some(StartupPublicationDecision.Block(reason))
      case None =>
        events.register(phase => {
          try
            current()
            if started && !stopping && progress.result.isEmpty then phase match
              case ServerRecoveryTickPhase.Start => progress.advance(phase)
              case ServerRecoveryTickPhase.End => observedEnd = Some(server.getTickCounter)
          catch
            case cause: Throwable =>
              onFailure(cause)
              throw cause
        })
        tickets.registerRecoveryCallbackOnce()
  def start(): Unit =
    current()
    require(installed && !started && !stopping, "claimed recovery requires its original installation")
    started = true
    if blocked.isEmpty then progress.begin()
  def onEnd(tick: PermanentEndTick): Unit =
    current()
    require(started && !stopping && !publicationAttempted && tick != null && tick.bootUuid == reads.identity.bootId &&
      tick.bootGeneration == reads.identity.bootGeneration && tick.phase == PermanentPumpPhase.Recovering &&
      tick.endOrdinal == lastEndOrdinal + 1L, "claimed recovery requires original ordered Recovering END")
    lastEndOrdinal = tick.endOrdinal
    if blocked.isEmpty && progress.result.isEmpty then
      require(observedEnd.contains(tick.vanillaEndCounter), "claimed END has no matching actual Forge observation")
      observedEnd = None
      progress.advance(ServerRecoveryTickPhase.End)
    result.foreach {
      case StartupPublicationDecision.Failed(_, cause) => throw cause
      case _ => () // Block/Permit remain retained; neither publishes Ready.
    }
  private def unregisterOnce(): Unit =
    if !unregisterAttempted then
      unregisterAttempted = true
      events.unregister()
  /** 保留原 manager/tickets；成功 finish 只结束 deferred 算法与原事件订阅。 */
  def published(): Unit =
    current()
    require(started && !stopping && !publicationAttempted && result.exists(_.isInstanceOf[StartupPublicationDecision.Permit]),
      "original claimed recovery Permit required")
    publicationAttempted = true
    progress.published(() => unregisterOnce())
  def stop(): Unit =
    require(Thread.currentThread() eq main, "claimed recovery stop requires actual Main")
    if !stopping then
      stopping = true
      var primary = Option.empty[Throwable]
      try unregisterOnce()
      catch case cause: Throwable => primary = Some(cause)
      try progress.stop()
      catch case cause: Throwable => primary match
        case Some(original) => if original ne cause then original.addSuppressed(cause)
        case None => primary = Some(cause)
      primary.foreach(throw _)

private[floatingpod] object ClaimedBootActiveRecovery:
  def create(server: MinecraftServer, modInstance: Object, reads: MainBootProjection,
      onFailure: Throwable => Unit): ClaimedBootActiveRecovery =
    require(server != null && modInstance != null && reads != null && onFailure != null,
      "original dedicated server/mod/Boot dependencies required")
    reads.requireCurrentReads()
    val tickets = ChunkLoadingManager.forClaimedBoot(reads, new ChunkTicketGatewayImpl(modInstance))
    val manager = ActiveFloatingPodManager.forClaimedBoot(reads, tickets)
    val worlds = new ServerWorldResolver:
      override def resolve(dimensionId: Int): Option[PodWorld] =
        Option(server.getWorld(dimensionId)).map(PodWorldImpl(_))
    val recovery = LiveCrashRecovery.forClaimedBoot(reads, tickets, manager, worlds,
      ExistingWorldInventory.withServerSupplier(() => server))
    new ClaimedBootActiveRecovery(server, reads, tickets, manager, recovery,
      new ForgeRuntimeRecoveryEvents(tickets), onFailure)
