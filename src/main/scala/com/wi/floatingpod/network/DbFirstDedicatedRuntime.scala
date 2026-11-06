package com.wi.floatingpod.network

import com.wi.floatingpod.bootstrap.{ProductionBootstrapInstallation,ProductionDbFirstBootstrapCallbacks}
import com.wi.floatingpod.debug.PodDebugRuntimeRegistry
import com.wi.floatingpod.storage.mysql.MySqlOwnedIoAdapter
import com.wi.floatingpod.server.{DbFirstBootActiveRecovery,DbFirstMainRequestReadBridge,ForgePermanentMainPump,MainDebugItemEndpoint,PermanentEndTick,PermanentPumpLease,PermanentPumpPhase,PermanentPumpRegistrationFailure}
import java.util.UUID
import net.minecraft.server.MinecraftServer
import net.minecraft.world.chunk.storage.AnvilSaveHandler
import net.minecraftforge.fml.common.event.*
import org.apache.logging.log4j.LogManager

/** 原schema6生产Main的恢复、唯一永久END及read-first停服source交接。 */
private[floatingpod] final class DbFirstDedicatedRuntime private (
    server:MinecraftServer,callbacks:ProductionDbFirstBootstrapCallbacks,modInstance:Object) extends ServerRuntime:
  private val main=server.getServerThread
  private val stopInvocation=new com.wi.floatingpod.bootstrap.DbFirstStopInvocation(server,main)
  private var stoppedEventSeen=false
  private val logger=LogManager.getLogger(getClass)
  private var publicationBlockedByRecovery=false
  private var recovery=Option.empty[DbFirstBootActiveRecovery]
  private var pump=Option.empty[ForgePermanentMainPump]
  private var lease=Option.empty[PermanentPumpLease]
  private var reads=Option.empty[DbFirstMainRequestReadBridge]
  private var endpoint=Option.empty[MainDebugItemEndpoint]
  private var registry=Option.empty[PodDebugRuntimeRegistry.Lease]
  private var packets=Option.empty[ServerPacketEndpoint]
  private var aboutAttempted=false
  private var startingAttempted=false
  private var publicationAttempted=false
  private var registryUninstallAttempted=false
  private var started=false
  private var published=false
  private var stopping=false
  private var primary=Option.empty[Throwable]
  private var stopRequest=Option.empty[UUID]
  private var stopPhase=0
  private val preStopRecallDrain=new DbFirstPreStopRecallDrain
  private var lastEnd=0L
  private var readBarrierAt=0L
  private var sourceDropEnd=0L
  private var mainEnded:DbFirstDedicatedRuntime.MainConsumersEnded=null
  private var exit=Option.empty[com.wi.floatingpod.bootstrap.ClaimedBootstrapExit]
  private def owner():Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread,"actual schema6 runtime Main required")
    callbacks.requireOriginalMainAdapter(callbacks.adapter)
    ProductionBootstrapInstallation.requireOriginalDbFirstRuntime(server,callbacks,this)
  private def suppress(original:Throwable)(action: => Unit):Unit =
    try action catch case secondary:Throwable=>
      MySqlOwnedIoAdapter.suppressExitCleanup(original,secondary)
  private def uninstallRegistry():Unit =
    packets.foreach { original =>
      ServerPacketRuntime.uninstall(original)
      packets=None
    }
    registry.foreach { actual=>
      require(!registryUninstallAttempted,"schema6 registry removal once")
      registryUninstallAttempted=true
      PodDebugRuntimeRegistry.uninstall(actual)
      registry=None
    }
  private def fail(cause:Throwable):Unit =
    owner();require(cause!=null,"original schema6 runtime failure required")
    primary match
      case None=>primary=Some(cause)
      case Some(original)=>if (original ne cause) && !original.getSuppressed.contains(cause) then original.addSuppressed(cause)
    val original=primary.get
    suppress(original)(stopInvocation.retainFailure(server,original))
    // 记录原失败并封Boot推进；此方法不quiesce，Main census必须先真实撤借用。
    suppress(original)(recovery.foreach(_.retainJournalTakeoverFailure(original)))
    suppress(original)(uninstallRegistry())
    suppress(original)(reads.foreach(_.sealNewRequestsForPreStop()))
    suppress(original)(endpoint.foreach(_.sealNewRequestsForPreStop()))
    // 不调用endpoint.stop/stopInternalPublication：两者会quiesce worker。
    // 前面已撤外部registry并seal request bridge；现在先撤无在途SQL/native工作的Main census。
    suppress(original)(recovery.foreach(_.detachStoppedBootCensusForFailure()))
    // Boot census和原Main DATA借用都必须在worker停止前撤除；失败由原cause保管，不标成功。
    suppress(original)(callbacks.prepareClaimedFailureMainDrop(server,original))
    // 真实Main借用撤除或因责任不明而保留后，才进入既有quiesce/错误退出路径。
    if endpoint.isEmpty then suppress(original)(recovery.foreach(_.stopInternalPublication(callbacks,original)))
    else suppress(original)(endpoint.foreach(_.stop()))
    suppress(original)(callbacks.retainClaimedFailure(server,original))
  private def requirePublicationInputs():Unit =
    owner()
    require((ProductionBootstrapInstallation.requireClaimedCallbacks(server) eq callbacks) &&
      pump.nonEmpty && lease.exists(l=>l.bootUuid==callbacks.adapter.identity.bootId &&
        l.bootGeneration==callbacks.adapter.identity.bootGeneration) && reads.nonEmpty && !stopping,
      "original schema6 claimed runtime/pump/reads required")
    callbacks.requireCurrentReads()
    recovery.get.requireRecallRequestsSettled()
    callbacks.verifyClosedBootSaveScopeUse(server)
    callbacks.ordinarySaveAdmission().requireHealthyAfterFlush()
    require(server.worlds!=null && server.worlds.exists(_!=null),"actual schema6 managed worlds required")
    server.worlds.filter(_!=null).foreach { world=>
      require(world.getMinecraftServer eq server,"actual schema6 managed world/server required")
      world.getSaveHandler match
        case handler:AnvilSaveHandler=>ProductionBootstrapInstallation.requireOrdinarySaveHealthy(handler)
        case _=>throw new IllegalStateException("actual schema6 managed Anvil save handler required")
    }
  private def publishOnce():Unit =
    require(!publicationAttempted && !published,"schema6 publication once")
    publicationAttempted=true
    requirePublicationInputs()
    val original=recovery.get
    original.requirePublicationOwner(callbacks)
    endpoint=Some(MainDebugItemEndpoint.prepareDbFirst(callbacks,original,pump.get,lease.get))
    requirePublicationInputs()
    pump.get.changePhase(lease.get,PermanentPumpPhase.Ready)
    registry=Some(PodDebugRuntimeRegistry.install(server,lease.get.bootUuid,endpoint.get))
    val originalPackets=endpoint.get.playerPackets(reads.get,recovery.get)
    ServerPacketRuntime.install(originalPackets)
    packets=Some(originalPackets)
    published=true
    logger.info("FloatingPod schema6 Ready: boot={}, generation={}",lease.get.bootUuid,Long.box(lease.get.bootGeneration))
    // failed局部槽及原receipt继续由recovery持有；遍历结束不把失败记录改为成功。
  override def onServerAboutToStart(event:FMLServerAboutToStartEvent):Unit =
    owner();require(event!=null && (event.getServer eq server) && !aboutAttempted,"original schema6 AboutToStart once")
    aboutAttempted=true
    try
      val original=DbFirstBootActiveRecovery.prepare(callbacks,modInstance)
      recovery=Some(original)
      original.install()
    catch case cause:Throwable=>
      fail(cause)
      throw cause
  override def onServerStarting(event:FMLServerStartingEvent):Unit =
    owner();require(event!=null && (event.getServer eq server) && aboutAttempted && recovery.nonEmpty &&
      !startingAttempted && !stopping,"original schema6 Starting once")
    startingAttempted=true
    try
      callbacks.requireCurrentReads()
      reads=Some(new DbFirstMainRequestReadBridge(callbacks))
      val actual=new ForgePermanentMainPump(server,()=>server)
      pump=Some(actual)
      lease=Some(actual.register(callbacks.adapter.identity.bootId,callbacks.adapter.identity.bootGeneration,onEnd))
      started=true
    catch
      case registration:PermanentPumpRegistrationFailure=>lease=Some(registration.lease);fail(registration);throw registration
      case cause:Throwable=>fail(cause);throw cause
  private def onEnd(tick:PermanentEndTick):Unit =
    owner()
    try
      primary.foreach(throw _)
      require(started && tick!=null && lease.exists(l=>l.bootUuid==tick.bootUuid && l.bootGeneration==tick.bootGeneration) &&
        tick.endOrdinal>lastEnd,"original monotonic schema6 permanent END required")
      lastEnd=tick.endOrdinal
      tick.phase match
        case PermanentPumpPhase.Recovering=>
          val original=recovery.get
          // mutation接受后旧DATA已失效；先凭原Main身份完成在途C回复/后继读ACK/跨END退休。
          if !original.advancePendingPositive(pump.get,lease.get,tick) &&
              !original.advancePendingRefusal(pump.get,lease.get,tick) then
            val unresolved=callbacks.recoveryReadData.sourceProtection.unresolved
            if unresolved.nonEmpty || (!original.completedPass && !original.mayRestoreOriginalPods) then
              if !publicationBlockedByRecovery then
                logger.warn("schema6 restore/publication blocked by unresolved original recovery responsibility; sources={}, recallRequests={}",
                  Int.box(unresolved.size),Int.box(original.unresolvedRecallRequestCount))
              publicationBlockedByRecovery=true
            else
              if !original.completedPass then original.restoreLoadedWorlds(callbacks,pump.get,lease.get,tick)
              original.settleRecoveredResults()
              if original.settleInitialTerminalResults(pump.get,lease.get,tick) then
                original.prepareInternalPublication(callbacks,pump.get,lease.get,tick)
                original.advanceJournalTakeover(pump.get,lease.get,tick)
              if original.unresolvedRecallRequestCount==0 then
                publicationBlockedByRecovery=false
                publishOnce()
              else if !publicationBlockedByRecovery then
                logger.warn("schema6 publication waits for actual original refusal Pod recovery; recallRequests={}",
                  Int.box(original.unresolvedRecallRequestCount))
                publicationBlockedByRecovery=true
        case PermanentPumpPhase.Ready=>
          require(published && !stopping,"original published schema6 END required")
          // 原 mutation 接纳后，旧 Main DATA 必须暂拒读取；原 END 仍须消费完成并交接 successor。
          // owner() 已核原 adapter/runtime；不能用新读取资格阻断使它恢复的 ACK 流程。
          if stopPhase==0 && ProductionBootstrapInstallation.dbFirstShutdownRequested(server,this,callbacks) then
            beginNormalStop(UUID.randomUUID())
          if stopPhase==0 then endpoint.get.onEnd(tick)
          else if stopPhase==1 then
            endpoint.get.onReadDrainEnd(tick)
            if !reads.get.preStopReadScopesPending then
              reads.get.preStopReadFailureAfterScopesEnded // 只核原scope确实退出/Fatal；不声明健康drain。
              readBarrierAt=tick.endOrdinal
              stopPhase=2
          else if stopPhase==2 then
            require(tick.endOrdinal>readBarrierAt,"schema6 original recall drain starts on later END after actual read scopes ended")
            val retainedReadFailure=reads.get.preStopReadFailureAfterScopesEnded
            val actual=endpoint.get
            val sourcePrepared=preStopRecallDrain.onEnd(tick.endOrdinal) {
              actual.onPreStopRecallEnd(tick)
            } {
              actual.preStopPlayerRecallSettled
            } {
              retainedReadFailure.foreach(throw _)
              reads.get.requirePreStopReadDrainSettled() // 新stop freeze/source仍须原健康读屏障，不以scope退出代替。
              actual.beginNormalStop(stopRequest.get)
            } {
              actual.onEnd(tick)
              actual.advanceDbFirstFinalPreparation(callbacks)
            }
            if sourcePrepared then
              endpoint=None
              reads=None
              sourceDropEnd=tick.endOrdinal
              stopPhase=3 // 只表示source已实际交接；最终worker/native关闭与stop permit仍须原owner。
          else if stopPhase==3 then
            require(tick.endOrdinal>sourceDropEnd,"schema6 original source handoff stack must return before Main retirement")
            if callbacks.dbFirstApplicationControlEnded then
              recovery.foreach(_.requireFinalMainRelease(callbacks))
              recovery=None
              callbacks.retireDbFirstApplication(this)
              mainEnded=new DbFirstDedicatedRuntime.MainConsumersEnded(callbacks.adapter.identity,tick.endOrdinal)
              stopPhase=4
              callbacks.registerDbFirstMainEnded(this,mainEnded)
              stopPhase=5
              exit=Some(callbacks.beginCertifiedDbFirstCompletion(this,mainEnded))
          else if stopPhase==5 then
            if !callbacks.dbFirstExitObservationPending(this,mainEnded) then
              callbacks.requireCertifiedDbFirstExitCompleted(this,mainEnded)
              stopPhase=6
              stopInvocation.issue(server) // 原worker/observer已证实；原Main同栈消费，仍未结束final-save。
              ProductionBootstrapInstallation.invokeCertifiedDbFirstShutdown(server,this,callbacks)
        case PermanentPumpPhase.Quiescing=>
          require(stopping && stopPhase==9,"original schema6 stopping invocation required")
    catch case cause:Throwable=>
      fail(cause)
      throw cause
  private[floatingpod] def beginNormalStop(request:UUID):Unit =
    owner();primary.foreach(throw _)
    require(request!=null && started && published && !stopping && stopRequest.isEmpty && stopPhase==0,
      "original published schema6 normal-stop request once")
    stopRequest=Some(request);stopPhase=1
    try
      recovery.get.sealRecallResults()
      endpoint.get.sealNewRequestsForPreStop()
      reads.get.sealNewRequestsForPreStop()
      endpoint.get.beginOrdinaryFinalPreparation(callbacks.ordinarySaveAdmission())
      uninstallRegistry()
    catch case cause:Throwable=>
      fail(cause)
      throw cause
  private[floatingpod] def requestReads:DbFirstMainRequestReadBridge =
    owner();primary.foreach(throw _)
    require(published && !stopping && stopPhase==0,"original live schema6 request-read bridge required")
    reads.get
  private[floatingpod] def finalSourcePrepared:Boolean =
    owner();primary.foreach(throw _);stopPhase>=3
  private[floatingpod] def requireApplicationRetirementOwner(original:ProductionDbFirstBootstrapCallbacks):Unit =
    owner();primary.foreach(throw _)
    require((original eq callbacks) && stopPhase==3 && lastEnd>sourceDropEnd && recovery.isEmpty &&
      endpoint.isEmpty && reads.isEmpty && registry.isEmpty && packets.isEmpty && !stopping,
      "original later-END schema6 Main application retirement required")
  private[floatingpod] def requireMainEndedOwner(original:ProductionDbFirstBootstrapCallbacks,
      receipt:DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    owner();primary.foreach(throw _)
    require((original eq callbacks) && stopPhase==4 && (mainEnded eq receipt) && lastEnd>sourceDropEnd &&
      recovery.isEmpty && endpoint.isEmpty && reads.isEmpty && registry.isEmpty,
      "original schema6 Main-only later-END receipt required")
  private[floatingpod] def requireWorkerHandoffOwner(original:ProductionDbFirstBootstrapCallbacks,
      receipt:DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    owner();primary.foreach(throw _)
    require((original eq callbacks) && stopPhase>=5 && receipt!=null && (mainEnded eq receipt) &&
      recovery.isEmpty && endpoint.isEmpty && reads.isEmpty && registry.isEmpty,
      "original registered schema6 Main-only worker handoff required")
  private[floatingpod] def consumeShutdownInvocation(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireWorkerHandoffOwner(original,mainEnded)
    require(stopPhase==6,"original schema6 completed observer stop permit required")
    callbacks.requireCertifiedDbFirstExitCompleted(this,mainEnded)
    stopInvocation.begin(server);stopPhase=7
  private[floatingpod] def shutdownInvocationInFlight:Boolean =
    owner();primary.isEmpty && stopPhase==7 && stopInvocation.inFlight(server)
  private[floatingpod] def consumeShutdownEntry(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireWorkerHandoffOwner(original,mainEnded)
    require(stopPhase==7,"original schema6 in-flight shutdown required")
    stopInvocation.consumeOriginalEntry(server);stopPhase=8
  private[floatingpod] def shutdownReturned(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireWorkerHandoffOwner(original,mainEnded)
    require(stopPhase==8,"original schema6 shutdown HEAD must consume before normal return")
    stopInvocation.invocationReturned(server);stopPhase=9
  private[floatingpod] def stopServerPending:Boolean =
    owner();primary.isEmpty && stopPhase==9 && stopInvocation.stopServerPending(server)
  private[floatingpod] def requireStopServerEntry(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireWorkerHandoffOwner(original,mainEnded)
    require(stopping && stopPhase==9,"original schema6 Stopping dispatch before stopServer required")
    stopInvocation.requireStopServerEntry(server)
    callbacks.requireCertifiedDbFirstExitCompleted(this,mainEnded)
  private[floatingpod] def stopServerReturned(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireStopServerEntry(original)
    stopInvocation.stopServerReturned(server);stopPhase=10
  private[floatingpod] def stoppedDispatchPending:Boolean =
    owner();primary.isEmpty && stopPhase==10 && stopInvocation.stoppedDispatchPending(server)
  private[floatingpod] def requireStoppedDispatchEntry(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireWorkerHandoffOwner(original,mainEnded)
    require(stopping && stopPhase==10,"original schema6 stopServer normal return required")
    stopInvocation.requireStoppedDispatchEntry(server)
  private[floatingpod] def stoppedDispatchReturned(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireStoppedDispatchEntry(original)
    require(stoppedEventSeen,"actual schema6 ServerStopped event within full dispatch required")
    stopInvocation.stoppedDispatchReturned(server);stopPhase=11
    // full dispatch已返回；先撤实际pump回调，再结原scheduler/loader/observer和有限B。
    require(pump.get.retire(lease.get),"original schema6 permanent pump retirement required")
    ProductionBootstrapInstallation.finishDbFirstFinalSave(server,this,callbacks)
    stopInvocation.finalGenerationEnded(server);stopPhase=12
    mainEnded=null;exit=None;pump=None;lease=None
    logger.info("schema6 normal stop completed after full ServerStopped dispatch and final-save retirement; boot={}, generation={}, request={}",
      callbacks.adapter.identity.bootId,Long.box(callbacks.adapter.identity.bootGeneration),stopRequest.get)
  private[floatingpod] def requireFinalSaveEndOwner(original:ProductionDbFirstBootstrapCallbacks):Unit =
    owner();primary.foreach(throw _)
    require((original eq callbacks) && stopping && stoppedEventSeen && stopPhase==11 && mainEnded!=null &&
      endpoint.isEmpty && reads.isEmpty && recovery.isEmpty && registry.isEmpty,
      "original full schema6 ServerStopped dispatch normal return required")
    stopInvocation.requireFinalGenerationEnd(server)
  private[floatingpod] def detachExitObservation(original:ProductionDbFirstBootstrapCallbacks):Unit =
    requireFinalSaveEndOwner(original)
    exit=None
  private[floatingpod] def retainInvocationFailure(original:ProductionDbFirstBootstrapCallbacks,cause:Throwable):Unit =
    owner();require(original eq callbacks,"original schema6 invocation callback required")
    fail(cause)
  private[floatingpod] def shutdownWasReturned:Boolean =
    owner();primary.isEmpty && stopPhase>=9 && stopPhase<=11
  override def onServerStopping(event:FMLServerStoppingEvent):Unit =
    owner();require(event!=null,"actual schema6 Stopping event required")
    try
      primary.foreach(throw _)
      require(!stopping && stopPhase==9,"schema6 Stopping requires original completed shutdown invocation")
      callbacks.requireCertifiedDbFirstExitCompleted(this,mainEnded)
      stopping=true
      pump.get.changePhase(lease.get,PermanentPumpPhase.Quiescing)
    catch case cause:Throwable=>
      fail(cause)
      throw primary.get
  override def onServerStopped(event:FMLServerStoppedEvent):Unit =
    owner()
    try
      require(event!=null && !stoppedEventSeen,"actual schema6 ServerStopped event once")
      requireStoppedDispatchEntry(callbacks)
      callbacks.requireCertifiedDbFirstExitCompleted(this,mainEnded)
      stoppedEventSeen=true // 事件handler返回不等于完整dispatch返回；这里不结束final-save代。
    catch case cause:Throwable=>
      fail(cause)
      throw primary.get

private[floatingpod] object DbFirstDedicatedRuntime:
  /** 只由原runtime在实际later-END撤Main后构造；不是worker或正常停服许可。 */
  final class MainConsumersEnded private[DbFirstDedicatedRuntime](
      private val identity:com.wi.floatingpod.storage.mysql.MySqlOwnedIoIdentity,val actualEndOrdinal:Long):
    private var consumed=false
    private[floatingpod] def consumeOriginal(original:com.wi.floatingpod.storage.mysql.MySqlOwnedIoIdentity):Unit =
      require((identity eq original) && actualEndOrdinal>0L && !consumed,"original schema6 Main-only receipt once")
      consumed=true
    private[floatingpod] def requireRegisteredIdentity(original:com.wi.floatingpod.storage.mysql.MySqlOwnedIoIdentity):Unit =
      require((identity eq original) && consumed,"original registered schema6 Main-only identity required")
  def claimed(server:MinecraftServer,modInstance:Object):DbFirstDedicatedRuntime =
    require(server!=null && server.isDedicatedServer && (Thread.currentThread() eq server.getServerThread) &&
      modInstance!=null,"actual claimed schema6 dedicated Main/mod required")
    val callbacks=ProductionBootstrapInstallation.requireClaimedCallbacks(server) match
      case original:ProductionDbFirstBootstrapCallbacks=>original
      case _=>throw new IllegalStateException("original schema6 production callbacks required")
    val actual=new DbFirstDedicatedRuntime(server,callbacks,modInstance)
    ProductionBootstrapInstallation.retainClaimedRuntime(server,actual)
    actual
