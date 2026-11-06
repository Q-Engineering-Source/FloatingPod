package com.wi.floatingpod.network

import com.wi.floatingpod.bootstrap.{ProductionBootstrapInstallation, ProductionDedicatedBootstrapCallbacks, ClaimedBootstrapExit}
import com.wi.floatingpod.server.{MainBootProjection, ForgePermanentMainPump, PermanentPumpLease, PermanentPumpPhase, MainOwnedRequestReadBridge, ClaimedBootActiveRecovery}
import com.wi.floatingpod.debug.PodDebugRuntimeRegistry
import com.wi.floatingpod.server.{MainDebugItemEndpoint, StartupPublicationDecision}
import net.minecraft.world.chunk.storage.AnvilSaveHandler
import net.minecraft.server.MinecraftServer
import net.minecraftforge.fml.common.event.*
import com.wi.floatingpod.deployment.command.MainDeploymentOriginalOperationIndex
import com.wi.floatingpod.server.io.{IoOperationTag,IoSubmission}
import java.util.UUID

/** 原 claimed OrdinaryCertified Boot 的恢复、一次发布与永久 END owner；无同步 repository。 */
private[floatingpod] final class DedicatedProjectionRuntime private (
    server: MinecraftServer, callbacks: ProductionDedicatedBootstrapCallbacks, modInstance: Object
) extends ServerRuntime:
  private val main = server.getServerThread
  private var projection = Option.empty[MainBootProjection]
  private var pump = Option.empty[ForgePermanentMainPump]
  private var lease = Option.empty[PermanentPumpLease]
  private var readBridge = Option.empty[MainOwnedRequestReadBridge]
  private var activeRecovery = Option.empty[ClaimedBootActiveRecovery]
  private var recallRecovery = Option.empty[com.wi.floatingpod.server.NormalRecallRecoveryResponsibility]
  private var endpoint = Option.empty[MainDebugItemEndpoint]
  private var registryLease = Option.empty[PodDebugRuntimeRegistry.Lease]
  private var publicationAttempted = false
  private var registryInstallAttempted = false
  private var registryUninstallAttempted = false
  private var endpointStopAttempted = false
  private var published = false
  private var started = false
  private var freshPreparation = false
  private var freshStopRequest = Option.empty[com.wi.floatingpod.bootstrap.DedicatedNormalStopBridge.Request]
  private var freshShutdownInvoked = false
  private var readBarrierCompletedAt = 0L
  private var stopping = false
  private var primary = Option.empty[Throwable]
  private var exit = Option.empty[ClaimedBootstrapExit]
  private var ordinaryMainPhase = 0
  private var ordinaryMainLastEnd = 0L
  private var ordinaryMainDropEnd = 0L
  private var ordinaryMainReceipt: DedicatedProjectionRuntime.MainConsumersEnded = null
  private def owner(): Unit = require(Thread.currentThread() eq main, "actual dedicated Main required")
  private def fail(cause: Throwable): Unit =
    owner()
    primary match
      case Some(original) => if (original ne cause) && !original.getSuppressed.contains(cause) then original.addSuppressed(cause)
      case None => primary = Some(cause)
    cleanupPublication(primary.get)
    suppress(primary.get)(activeRecovery.foreach(_.stop()))
    suppress(primary.get)(recallRecovery.foreach(_.stop()))
    suppress(primary.get)(readBridge.foreach(_.stop()))
    // 最后producer已返回后只保原失败；已撤的observer不得因最终清图失败被重建。
    if ordinaryMainPhase >= 11 then
      callbacks.observedClaimedExit match
        case Some(original) => original.retainFailure(primary.get)
        case None => ()
    else
      try exit = Some(callbacks.stopClaimed(server, primary.get))
      catch case cleanup: Throwable =>
        if (cleanup ne primary.get) && !primary.get.getSuppressed.contains(cleanup) then primary.get.addSuppressed(cleanup)
  private def suppress(primary: Throwable)(cleanup: => Unit): Unit =
    try cleanup
    catch case cause: Throwable =>
      if (cause ne primary) && !primary.getSuppressed.contains(cause) then primary.addSuppressed(cause)
  /** registry 先撤销；所有 attempted 在副作用前记录，失败也不重新发布。 */
  private def cleanupPublication(cause: Throwable): Unit =
    endpoint.foreach(value => suppress(cause)(value.sealNewRequestsForPreStop()))
    if registryInstallAttempted && registryLease.isEmpty then
      suppress(cause) {
        val installed = PodDebugRuntimeRegistry.current(server)
        if installed != null && endpoint.exists(value => installed.endpoint() eq value) then
          registryLease = Some(installed)
      }
    if !registryUninstallAttempted then registryLease.foreach { original =>
      registryUninstallAttempted = true
      suppress(cause)(PodDebugRuntimeRegistry.uninstall(original))
    }
    if !endpointStopAttempted then endpoint.foreach { value =>
      endpointStopAttempted = true
      suppress(cause)(value.stop())
    }
  private def requirePublicationInputs(actual: ForgePermanentMainPump, original: PermanentPumpLease): Unit =
    val reads = bootReads
    reads.ordinaryPublicationBinding
    require((ProductionBootstrapInstallation.requireClaimedCallbacks(server) eq callbacks) &&
      original.bootUuid == reads.identity.bootId && original.bootGeneration == reads.identity.bootGeneration &&
      callbacks.adapter.identity == reads.identity, "original claimed callbacks/pump/session required")
    callbacks.verifyClosedBootSaveScopeUse(server)
    callbacks.ordinarySaveAdmission().requireHealthyAfterFlush()
    require(server.worlds != null && server.worlds.exists(_ != null), "actual managed worlds required")
    server.worlds.filter(_ != null).foreach { world =>
      require(world.getMinecraftServer eq server, "actual enrolled world server required")
      world.getSaveHandler match
        case handler: AnvilSaveHandler => ProductionBootstrapInstallation.requireOrdinarySaveHealthy(handler)
        case _ => throw new IllegalStateException("Actual managed Anvil save handler required for publication")
    }
    // current() above validates exact original admission/session and healthy accepting IO.
    require(pump.contains(actual) && lease.exists(_ eq original), "original retained pump/lease required")
  private def publishOnce(recovery: ClaimedBootActiveRecovery): Unit =
    require(!publicationAttempted && !published && !stopping, "claimed debug publication is one-shot")
    publicationAttempted = true
    val actual = pump.getOrElse(throw new IllegalStateException("original pump missing"))
    val original = lease.getOrElse(throw new IllegalStateException("original pump lease missing"))
    requirePublicationInputs(actual, original)
    endpoint = Some(MainDebugItemEndpoint.prepare(server, callbacks.adapter, recovery, bootReads, actual, original))
    recovery.published() // finish Published + original event unregister must both succeed before Ready.
    requirePublicationInputs(actual, original)
    actual.changePhase(original, PermanentPumpPhase.Ready)
    registryInstallAttempted = true
    registryLease = Some(PodDebugRuntimeRegistry.install(server, original.bootUuid, endpoint.get))
    published = true
    activeRecovery = None // published已真实finish+unregister，Endpoint仅保原manager/tickets。
  def bootReads: MainBootProjection =
    owner()
    primary.foreach(cause => throw cause)
    projection.getOrElse(throw new IllegalStateException("claimed Boot projection not consumed"))
  private[floatingpod] def requestReads: MainOwnedRequestReadBridge =
    owner()
    require(started && !stopping, "original claimed request bridge unavailable")
    readBridge.getOrElse(throw new IllegalStateException("original request bridge not installed"))
  /** 安装的同一Main桥接真实调用者原index/entry；R1不创建网络请求或mutation责任。 */
  private[floatingpod] def submitOriginalDeploymentRead(actor:MainDeploymentActorContext,
      index:MainDeploymentOriginalOperationIndex,entry:MainDeploymentOriginalOperationIndex#Entry,
      tag:IoOperationTag,workflow:UUID,reserve:UUID):IoSubmission =
    owner()
    primary.foreach(throw _)
    requestReads.submitOriginal(actor,index,entry,tag,workflow,reserve)
  private[floatingpod] def consumeOriginalDeploymentRead(tag:IoOperationTag):Option[Option[MainDeploymentOriginalOperationIndex.LookupMatch]] =
    owner()
    primary.foreach(throw _)
    requestReads.consumeOriginal(tag)
  def retainedFailure: Option[Throwable] = { owner(); primary }
  def exitObservation: Option[ClaimedBootstrapExit] = { owner(); exit }
  private[floatingpod] def requireFreshPreparationCompletionOwner(
      originalCallbacks: ProductionDedicatedBootstrapCallbacks, originalProjection: MainBootProjection): Unit =
    owner()
    require((originalCallbacks eq callbacks) && projection.exists(_ eq originalProjection) &&
      (ProductionBootstrapInstallation.requireClaimedCallbacks(server) eq callbacks) && freshPreparation &&
      started && !stopping && !published && !registryInstallAttempted && primary.isEmpty &&
      endpoint.isEmpty && activeRecovery.isEmpty && readBridge.isEmpty && freshStopRequest.nonEmpty,
      "original unpublished Fresh preparation runtime and stop request required")
  /** 原Installation显式Main入口；本片不安装offMain/Stopping producer。 */
  private[floatingpod] def beginOrdinaryMainPreparation(actualServer: MinecraftServer,
      originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    owner()
    primary.foreach(throw _)
    require((actualServer eq server) && (originalCallbacks eq callbacks) &&
      (ProductionBootstrapInstallation.requireClaimedCallbacks(server) eq callbacks) && started && published &&
      !stopping && !freshPreparation && recallRecovery.isEmpty && ordinaryMainPhase == 0 &&
      endpoint.nonEmpty && readBridge.nonEmpty && projection.nonEmpty,
      "original published Ordinary Main preparation is one-shot; RecallRecovering unsupported")
    ordinaryMainPhase = 1 // 副作用前保留一次责任；失败不建立新owner。
    try
      endpoint.get.sealNewRequestsForPreStop()
      readBridge.get.sealNewRequestsForPreStop()
      endpoint.get.beginOrdinaryFinalPreparation(callbacks.ordinarySaveAdmission())
      registryLease.foreach { original =>
        require(!registryUninstallAttempted, "original registry removal once")
        registryUninstallAttempted = true
        PodDebugRuntimeRegistry.uninstall(original)
        registryLease = None
      }
    catch
      case cause: Throwable =>
        fail(cause)
        throw cause
  private[floatingpod] def requireOrdinaryFinalSourceOwner(originalCallbacks: ProductionDedicatedBootstrapCallbacks,
      sourceOwner: Object, escrow: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow): Unit =
    owner()
    primary.foreach(throw _)
    require((originalCallbacks eq callbacks) && ordinaryMainPhase == 2 && !stopping,
      "original runtime final-source preparation owner required")
    endpoint.getOrElse(throw new IllegalStateException("original source endpoint missing")).requireOriginalFinalSourceOwner(sourceOwner, escrow)
  private[floatingpod] def requireOrdinaryFinalSource(originalCallbacks: ProductionDedicatedBootstrapCallbacks,
      sourceOwner: Object, escrow: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow, source: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator): Unit =
    requireOrdinaryFinalSourceOwner(originalCallbacks, sourceOwner, escrow)
    endpoint.get.requireOriginalFinalSource(sourceOwner, escrow, source)
  private[floatingpod] def requireOrdinaryMainDropOwner(originalCallbacks: ProductionDedicatedBootstrapCallbacks,
      originalProjection: MainBootProjection): Unit =
    owner()
    primary.foreach(throw _)
    require((originalCallbacks eq callbacks) && ordinaryMainPhase == 2 && projection.exists(_ eq originalProjection) &&
      endpoint.nonEmpty && readBridge.nonEmpty && registryLease.isEmpty && !stopping,
      "original Main drop phase/projection/registry owner required")
    readBridge.get.requirePreStopReadDrainSettled()
  private[floatingpod] def requireOrdinaryApplicationRetirementOwner(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    owner()
    primary.foreach(throw _)
    require((originalCallbacks eq callbacks) && ordinaryMainPhase == 3 && !stopping &&
      ordinaryMainLastEnd > ordinaryMainDropEnd && projection.isEmpty && endpoint.isEmpty && readBridge.isEmpty &&
      activeRecovery.isEmpty && recallRecovery.isEmpty && registryLease.isEmpty,
      "original later-END application retirement owner required")
  private[floatingpod] def requireOrdinaryMainEndedReceipt(originalCallbacks: ProductionDedicatedBootstrapCallbacks,
      receipt: DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    owner()
    primary.foreach(throw _)
    require((originalCallbacks eq callbacks) && ordinaryMainPhase == 4 && (ordinaryMainReceipt eq receipt) &&
      ordinaryMainLastEnd > ordinaryMainDropEnd && projection.isEmpty && endpoint.isEmpty && readBridge.isEmpty &&
      activeRecovery.isEmpty && recallRecovery.isEmpty && registryLease.isEmpty,
      "original later-END Main-only receipt required")
  private[floatingpod] def requireOrdinaryWorkerHandoffOwner(originalCallbacks: ProductionDedicatedBootstrapCallbacks,
      receipt: DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    owner()
    if primary.nonEmpty then throw primary.get
    if (originalCallbacks ne callbacks) || ordinaryMainPhase < 5 || (ordinaryMainReceipt ne receipt) ||
        !published || freshPreparation || projection.nonEmpty || endpoint.nonEmpty || readBridge.nonEmpty ||
        activeRecovery.nonEmpty || recallRecovery.nonEmpty || registryLease.nonEmpty then
      throw new IllegalStateException("original phase5 Ordinary Main handoff required")

  private def advanceOrdinaryMainPreparation(tick: com.wi.floatingpod.server.PermanentEndTick): Unit =
    owner()
    val originalLease = lease.getOrElse(throw new IllegalStateException("original pump lease missing"))
    require(tick.bootUuid == originalLease.bootUuid && tick.bootGeneration == originalLease.bootGeneration &&
      tick.phase == PermanentPumpPhase.Ready && tick.endOrdinal > ordinaryMainLastEnd,
      "original permanent Ready END required for Main preparation")
    ordinaryMainLastEnd = tick.endOrdinal
    if ordinaryMainPhase == 1 then
      val actualEndpoint = endpoint.get
      val reads = readBridge.get
      readBarrierCompletedAt = MainDebugItemEndpoint.advanceReadFirstEnd(
        tick.endOrdinal, true, readBarrierCompletedAt)(
        actualEndpoint.onEnd(tick), actualEndpoint.onReadDrainEnd(tick), reads.onEnd(tick),
        reads.preStopReadDrainPending, reads.requirePreStopReadDrainSettled())
      if readBarrierCompletedAt > 0L then ordinaryMainPhase = 2
    else if ordinaryMainPhase == 2 then
      // 与Q一致：切换END不再dispatch；下一原END只调用endpoint一次。
      endpoint.get.onEnd(tick)
      readBridge.get.onEnd(tick)
      readBridge.get.requirePreStopReadDrainSettled()
      if endpoint.get.advanceOrdinaryFinalPreparation(callbacks) then
        val originalProjection = projection.get
        callbacks.dropOrdinaryMainInputs(this, originalProjection)
        projection = None
        endpoint = None
        readBridge = None
        require(activeRecovery.isEmpty && recallRecovery.isEmpty && registryLease.isEmpty,
          "Main recovery/registry roots remain retained")
        ordinaryMainDropEnd = tick.endOrdinal
        ordinaryMainPhase = 3
    else if ordinaryMainPhase == 3 then
      require(tick.endOrdinal > ordinaryMainDropEnd, "actual later END must retire previous callback stack")
      callbacks.requireOrdinaryMainInputsDropped()
      if !callbacks.tryRetireOrdinaryApplication(this) then return
      val identity = callbacks.adapter.identity
      ordinaryMainReceipt = new DedicatedProjectionRuntime.MainConsumersEnded(identity.bootId.getMostSignificantBits,
        identity.bootId.getLeastSignificantBits, identity.bootGeneration, identity.connectionGeneration, tick.endOrdinal)
      ordinaryMainPhase = 4
      callbacks.registerOrdinaryMainEnded(this, ordinaryMainReceipt)
      ordinaryMainPhase = 5
      exit=Some(callbacks.beginCertifiedOrdinaryCompletion(this,ordinaryMainReceipt))
    else if ordinaryMainPhase == 5 then
      if callbacks.ordinaryExitObservationPending(this,ordinaryMainReceipt) then return
      callbacks.requireCertifiedOrdinaryExitCompleted(this,ordinaryMainReceipt)
      ordinaryMainPhase=6 // ISSUED；原Main同栈消费，不能把Main回执直接当许可。
      ProductionBootstrapInstallation.invokeCertifiedOrdinaryShutdown(server,this,callbacks)
    // phase5及以后只走原控制分支，不读取已撤业务holder。

  /** 以下阶段只由原Installation同server/Main调用；不由phase5或自由布尔值签发。 */
  private[floatingpod] def consumeOrdinaryShutdownInvocation(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 6 then throw new IllegalStateException("original Ordinary stop permit is not ISSUED")
    callbacks.requireCertifiedOrdinaryExitCompleted(this,ordinaryMainReceipt)
    ordinaryMainPhase=7 // CONSUMED_IN_FLIGHT，先于实际initiateShutdown。

  private[floatingpod] def consumeOrdinaryShutdownEntry(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 7 then throw new IllegalStateException("original Ordinary stop entry is one-shot")
    ordinaryMainPhase=8 // ENTRY_CONSUMED。

  private[floatingpod] def ordinaryShutdownReturned(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 8 then throw new IllegalStateException("original Ordinary stop entry did not consume")
    ordinaryMainPhase=9 // 仅原调用正常返回。

  private[floatingpod] def requireOrdinaryStopServerEntry(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 9 then throw new IllegalStateException("original Ordinary initiateShutdown has not returned")

  private[floatingpod] def ordinaryStopServerReturned(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryStopServerEntry(originalCallbacks)
    ordinaryMainPhase=10

  private[floatingpod] def requireOrdinaryStoppedDispatchEntry(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 10 then throw new IllegalStateException("original stopServer did not return normally")

  private[floatingpod] def ordinaryStoppedDispatchReturned(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryStoppedDispatchEntry(originalCallbacks)
    ordinaryMainPhase=11
    ProductionBootstrapInstallation.finishOrdinaryFinalSave(server,this,callbacks)
    ordinaryMainPhase=12
    ordinaryMainReceipt=null
    exit=None

  private[floatingpod] def requireOrdinaryFinalSaveEndOwner(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryWorkerHandoffOwner(originalCallbacks,ordinaryMainReceipt)
    if ordinaryMainPhase != 11 then throw new IllegalStateException("actual full ServerStopped dispatch normal return required")

  private[floatingpod] def detachOrdinaryExitObservation(originalCallbacks: ProductionDedicatedBootstrapCallbacks): Unit =
    requireOrdinaryFinalSaveEndOwner(originalCallbacks)
    exit=None

  private[floatingpod] def retainOrdinaryInvocationFailure(originalCallbacks: ProductionDedicatedBootstrapCallbacks,cause: Throwable): Unit =
    owner()
    if (originalCallbacks ne callbacks) || cause == null then throw new IllegalStateException("original Ordinary invocation failure required")
    fail(cause)

  private[floatingpod] def ordinaryInvocationInFlight: Boolean = ordinaryMainPhase == 7
  private[floatingpod] def ordinaryShutdownWasReturned: Boolean = primary.isEmpty && ordinaryMainPhase >= 9 && ordinaryMainPhase <= 11
  private[floatingpod] def ordinaryStopServerPending: Boolean = primary.isEmpty && ordinaryMainPhase == 9
  private[floatingpod] def ordinaryStoppedDispatchPending: Boolean = primary.isEmpty && ordinaryMainPhase == 10

  private def advanceFreshCompletion(): Unit =
    owner()
    if freshStopRequest.isEmpty then
      freshStopRequest = Option(ProductionBootstrapInstallation.pollFreshShutdown(server, this))
    if freshStopRequest.isEmpty then bootReads.requireCurrentReads()
    else
      if exit.isEmpty then
        callbacks.beginCertifiedFreshCompletion(server, this, bootReads).foreach { observed =>
          exit = Some(observed)
          projection = None // callbacks 已实际 drop；本 runtime 不再保留可读投影。
        }
      exit.foreach { original =>
        original.failure.foreach(throw _)
        if !freshShutdownInvoked && !original.freshObservationPending && original.checkpoint.nonEmpty &&
            original.waitObservation.exists(_.isInstanceOf[com.wi.floatingpod.storage.mysql.MySqlOwnedIoWorkerTerminationWait.Terminated]) then
          freshShutdownInvoked = true // 失败也保留一次调用责任，不能重发。
          ProductionBootstrapInstallation.invokeCertifiedFreshShutdown(server, this, callbacks, freshStopRequest.get)
      }
  override def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit =
    owner()
    require(event != null && (event.getServer eq server) && projection.isEmpty, "exact one-shot AboutToStart required")
    try
      val reads = MainBootProjection.consumeClaimed(server)
      projection = Some(reads)
      freshPreparation = reads.isCertifiedFreshPreparation
      if reads.recallRecoveryLink.nonEmpty then
        recallRecovery = Some(com.wi.floatingpod.server.NormalRecallRecoveryResponsibility.retain(reads,server))
      else if !freshPreparation then
        val recovery = ClaimedBootActiveRecovery.create(server, modInstance, reads, fail)
        activeRecovery = Some(recovery)
        recovery.install()
    catch
      case cause: Throwable =>
        fail(cause)
        throw cause
  override def onServerStarting(event: FMLServerStartingEvent): Unit =
    owner()
    require(event != null && (event.getServer eq server) && !started && !stopping, "exact one-shot Starting required")
    try
      bootReads.requireCurrentReads()
      if !freshPreparation && recallRecovery.isEmpty then
        activeRecovery.getOrElse(throw new IllegalStateException("claimed recovery owner missing")).start()
      val actual = new ForgePermanentMainPump(server, () => server)
      pump = Some(actual)
      if !freshPreparation && recallRecovery.isEmpty then readBridge = Some(new MainOwnedRequestReadBridge(server, callbacks.adapter))
      val original = actual.register(callbacks.adapter.identity.bootId, callbacks.adapter.identity.bootGeneration, tick => {
        try
          primary.foreach(throw _)
          if !freshPreparation && ordinaryMainPhase == 0 && ProductionBootstrapInstallation.ordinaryShutdownRequested(server,this,callbacks) then
            if !published || recallRecovery.nonEmpty then throw new java.util.concurrent.CancellationException("Ordinary shutdown requested before published business readiness")
            ProductionBootstrapInstallation.beginOrdinaryMainPreparation(server,this,callbacks)
          if ordinaryMainPhase != 0 then advanceOrdinaryMainPreparation(tick)
          else tick.phase match
            case PermanentPumpPhase.Recovering =>
              if freshPreparation then
                // 已认证 Fresh 的专用准备阶段；真实 END 继续核原 owner，不创建业务恢复/发布 holder。
                // 正常完成仍须独立的实际资源收口与 FreshCompletionExitPermit。
                advanceFreshCompletion()
              else if recallRecovery.nonEmpty then
                recallRecovery.get.onEnd(tick)
              else
                val recovery = activeRecovery.getOrElse(throw new IllegalStateException("claimed recovery owner missing"))
                recovery.onEnd(tick)
                readBridge.foreach(_.onEnd(tick))
                recovery.result.foreach {
                  case _: StartupPublicationDecision.Permit => publishOnce(recovery)
                  case _ => () // pending/source/legacy Block never becomes Ready.
                }
            case PermanentPumpPhase.Ready =>
              require(published, "actual debug publication required for Ready END")
              val actualEndpoint = endpoint.getOrElse(throw new IllegalStateException("original debug endpoint missing"))
              val actualReads = readBridge.getOrElse(throw new IllegalStateException("original read bridge missing"))
              val requested = actualReads.preStopReadDrainRequested
              if requested then actualEndpoint.sealNewRequestsForPreStop()
              readBarrierCompletedAt = MainDebugItemEndpoint.advanceReadFirstEnd(
                tick.endOrdinal, requested, readBarrierCompletedAt)(
                actualEndpoint.onEnd(tick), actualEndpoint.onReadDrainEnd(tick), actualReads.onEnd(tick),
                actualReads.preStopReadDrainPending, actualReads.requirePreStopReadDrainSettled())
            case PermanentPumpPhase.Quiescing =>
              endpoint.foreach(_.onEnd(tick))
              readBridge.foreach(_.onEnd(tick))
        catch
          case cause: Throwable =>
            fail(cause)
            throw cause
      })
      lease = Some(original)
      started = true // first real Recovering END owns the later publication transition.
    catch
      case registration: com.wi.floatingpod.server.PermanentPumpRegistrationFailure =>
        lease = Some(registration.lease)
        fail(registration)
        throw registration
      case cause: Throwable => fail(cause); throw cause
  override def onServerStopping(event: FMLServerStoppingEvent): Unit =
    owner()
    if !stopping then
      stopping = true
      if ordinaryMainPhase == 9 then
        try callbacks.requireCertifiedOrdinaryExitCompleted(this,ordinaryMainReceipt)
        catch
          case cause: Throwable =>
            fail(cause)
            throw cause
        for actual <- pump; original <- lease do actual.changePhase(original,PermanentPumpPhase.Quiescing)
        return
      if freshShutdownInvoked then
        try
          ProductionBootstrapInstallation.requireCertifiedFreshShutdownInvocation(server, this, freshStopRequest.get)
          callbacks.requireCertifiedFreshExitCompleted(server)
        catch
          case cause: Throwable =>
            fail(cause)
            throw cause
        for actual <- pump; original <- lease do actual.changePhase(original, PermanentPumpPhase.Quiescing)
        return
      val stopCause = primary.getOrElse(new IllegalStateException("claimed Boot/custody remains unsettled at stop; no finalizer ACK"))
      if primary.isEmpty then primary = Some(stopCause)
      cleanupPublication(stopCause)
      try activeRecovery.foreach(_.stop())
      catch case cause: Throwable => fail(cause)
      try readBridge.foreach(_.stop())
      catch case cause: Throwable => fail(cause)
      fail(primary.getOrElse(stopCause))
      for actual <- pump; original <- lease do
        try actual.changePhase(original, PermanentPumpPhase.Quiescing)
        catch case cause: Throwable => fail(cause)
  override def onServerStopped(event: FMLServerStoppedEvent): Unit =
    owner()
    if !stopping then onServerStopping(null)
    if freshShutdownInvoked && primary.isEmpty then
      try callbacks.requireCertifiedFreshExitCompleted(server)
      catch case cause: Throwable => fail(cause)
    for actual <- pump; original <- lease do
      try actual.retire(original) // only event-bus responsibility; not SQL/native/quiesce ACK.
      catch case cause: Throwable => fail(cause)
    // projection/callbacks/exit are deliberately retained by Installation's original Binding.

private[floatingpod] object DedicatedProjectionRuntime:
  final class MainConsumersEnded private[DedicatedProjectionRuntime] (
      private val most: Long, private val least: Long, private val bootGeneration: Long,
      private val connectionGeneration: Long, private val endOrdinal: Long):
    private var consumed = false
    private[floatingpod] def consumeOriginal(boot: java.util.UUID, generation: Long, connection: Long, laterEnd: Long): Unit =
      require(!consumed && boot.getMostSignificantBits == most && boot.getLeastSignificantBits == least &&
        generation == bootGeneration && connection == connectionGeneration && laterEnd == endOrdinal,
        "original Main-only receipt consumes once")
      consumed = true
    /** 登记后的同回执只验证，不再消费；worker资格仍由原exit hook与Session共同核验。 */
    private[floatingpod] def requireRegisteredIdentity(boot: java.util.UUID, generation: Long, connection: Long): Unit =
      if !consumed || boot == null || boot.getMostSignificantBits != most || boot.getLeastSignificantBits != least ||
          generation != bootGeneration || connection != connectionGeneration then
        throw new IllegalStateException("exact consumed Main-only receipt registration required")
    private[floatingpod] def actualEndOrdinal: Long = endOrdinal
  def claimed(server: MinecraftServer, modInstance: Object): DedicatedProjectionRuntime =
    require(server != null && server.isDedicatedServer && (Thread.currentThread() eq server.getServerThread),
      "actual claimed dedicated Main required")
    val callbacks = ProductionBootstrapInstallation.requireClaimedCallbacks(server) match
      case value: ProductionDedicatedBootstrapCallbacks => value
      case _ => throw new IllegalStateException("actual production callbacks required")
    require(modInstance != null, "original FML mod instance required")
    val runtime = new DedicatedProjectionRuntime(server, callbacks, modInstance)
    ProductionBootstrapInstallation.retainClaimedRuntime(server, runtime)
    runtime
