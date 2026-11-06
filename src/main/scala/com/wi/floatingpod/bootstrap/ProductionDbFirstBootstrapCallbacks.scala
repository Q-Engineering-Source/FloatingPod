package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.{ActiveFloatingPodManager,DedicatedWorldContext,MainPodReadAccess,NormalRecallRecoverySourceCustody}
import com.wi.floatingpod.domain.{FloatingPodState,PlayerEntitlement,RecallRequestNamespace}
import com.wi.floatingpod.storage.PhysicalRecoveryBinding
import java.util.UUID
import com.wi.floatingpod.server.source.SourceForestAllocationBudget
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.server.io.IoOwnerTransfer
import com.wi.floatingpod.storage.mysql.*
import java.util.IdentityHashMap
import java.util.concurrent.{CompletionStage, ExecutionException, TimeUnit}
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import net.minecraft.world.chunk.Chunk
import net.minecraft.world.gen.ChunkProviderServer

/** schema6独立启动责任：原两阶段completion与Full27 Main借用；不发行旧Boot reply或Ready。 */
final class ProductionDbFirstBootstrapCallbacks private (
    val actualServer:MinecraftServer, val control:DedicatedBootstrapBridge,
    initialApprovedControl:ProductionBootstrapControl.DbFirstControl,
    val worldContext:DedicatedWorldContext,
    val dependencies:EarlyDependencyResolver.VerifiedDependencies,
    val adapter:MySqlOwnedIoAdapter, val bootTag:IoOperationTag,
    private var preparationBacking:MySqlOwnedIoAdapter#BootstrapPreparation,
    val bootstrapPolicy:ProductionBootstrapControl.Policy,
    private val managedRequest:Option[MySqlManagedBootstrapRequest]
) extends ManagedBootSaveCallbacks with ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy with MainPodReadAccess:
  private val originalControl=Thread.currentThread()
  @volatile private var approvedControlBacking=initialApprovedControl
  private def approvedControl:ProductionBootstrapControl.DbFirstControl =
    Option(approvedControlBacking).getOrElse(throw new IllegalStateException(
      "schema6 managed control is unavailable until the exact owned-IO preparation completes"))
  private def activePolicy:ProductionBootstrapControl.Policy =
    Option(approvedControlBacking).map(_.policy()).getOrElse(bootstrapPolicy)
  private var prepared=false
  @volatile private var reply:MySqlOwnedIoReply.DbFirstSqlWriterAcquired=null
  @volatile private var originalReceipt:IoOwnerTransfer=null
  @volatile private var claimedMain:Thread=null
  @volatile private var failureReport:DedicatedBootstrapBridge.FailureReport=null
  @volatile private var finalCheckpoint:MySqlOwnedIoCheckpoint=null
  @volatile private var finalPreparation:Option[IoPreparationObservation]=None
  private var borrowed:MySqlDbFirstMainReadBacking=null
  private var consumed=false
  private var dropped=false
  private var mainFailure:Throwable=null
  private var claimedExit:ClaimedBootstrapExit=null
  private var failureSourceStopAttempted=false
  private var failureMainDropAttempted=false
  private var failureAdmissionUnknownAttempted=false
  private var sourceCustody:NormalRecallRecoverySourceCustody=null
  private var sourceCustodyAttempted=false
  private var sourceCustodyFailure:Throwable=null
  private var saveIssuerAttempted=false
  private var saveAdmission:ManagedOrdinarySaveAdmission=null
  private var resumingTag:IoOperationTag=null
  private var resumingRecoveredIdentity:MySqlOwnedIoIdentity=null
  private var mainPublication:ActiveFloatingPodManager#MainPublicationScope=null
  private var publicationRetained=false
  private var applicationRetired=false
  private var mainEndedReceipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded=null
  private var finalExitDetached=false

  private def preparation:adapter.BootstrapPreparation =
    require(preparationBacking!=null,"schema6 application preparation retired")
    preparationBacking.asInstanceOf[adapter.BootstrapPreparation]
  private def owner():Unit = require(Thread.currentThread() eq originalControl,"exact schema6 bootstrap control required")
  private def requireReply(value:Object):Unit =
    require(reply!=null && (reply eq value),"original schema6 acquisition completion required")
  private def receipt(value:Object):IoOwnerTransfer =
    require(originalReceipt!=null && (originalReceipt eq value),"original schema6 transfer receipt required")
    originalReceipt
  private def mainIdentity(server:MinecraftServer):Unit =
    require((server eq actualServer) && claimedMain!=null && (Thread.currentThread() eq claimedMain) &&
      (claimedMain eq actualServer.getServerThread) && control.phase()==DedicatedBootstrapBridge.Phase.CLAIMED,
      "exact successfully claimed schema6 Main required")

  override def prepare():CompletionStage[Object] =
    owner();require(!prepared,"schema6 production preparation is one-shot");prepared=true
    preparation.prepare(activePolicy.limits().preparation()).thenApply[Object] { value=>
      value match
        case acquired:MySqlOwnedIoReply.DbFirstSqlWriterAcquired=>
          managedRequest.foreach { request =>
            val actual=request.preparedControl()
            require(actual!=null,"managed bootstrap preparation must expose its exact real control after success")
            if approvedControlBacking==null then approvedControlBacking=actual
            require(approvedControlBacking eq actual,"managed request/control identity changed after preparation")
          }
          require(acquired.value.identity eq adapter.identity,"original schema6 worker acquisition identity required")
          reply=acquired
          acquired
        case _=>throw new IllegalStateException("schema6 production preparation returned another completion family")
    }
  override def issueTransfer(value:Object,target:Thread):Object =
    owner();requireReply(value)
    require(target!=null && (target eq actualServer.getServerThread) && target.getState==Thread.State.NEW,
      "actual original NEW server Main required")
    worldContext.pinBeforeOriginalStart(actualServer)
    val issued=adapter.issueOwnerTransfer(target,bootTag)
    originalReceipt=issued
    issued
  override def claim(value:Object,exactReceipt:Object):Unit =
    requireReply(value)
    require(Thread.currentThread() eq actualServer.getServerThread,"exact original schema6 server Main required")
    adapter.claimOwnerTransfer(receipt(exactReceipt))
    claimedMain=Thread.currentThread()
  override def abortTransfer(value:Object,exactReceipt:Object,cause:Throwable):Unit =
    requireReply(value);require(cause!=null,"original schema6 failure required")
    adapter.abortOwnerTransfer(receipt(exactReceipt),cause)

  /** run-HEAD可借用同一原backing；AboutToStart只消费一次，不复制或重建读责任。 */
  private[floatingpod] def borrowClaimedMainReads(server:MinecraftServer):MySqlDbFirstMainReadBacking =
    mainIdentity(server)
    require(!dropped,"schema6 Main reads already dropped")
    if mainFailure!=null then throw mainFailure
    try
      worldContext.validateAboutToStart(server)
      val actual=adapter.claimedDbFirstMainReads
      if borrowed==null then borrowed=actual
      else require(borrowed eq actual,"schema6 original Main backing changed")
      actual
    catch case cause:Throwable=>
      mainFailure=cause
      throw cause
  // consumer保留原callback而非某一代DATA；successor替换后每次查询仍走原registry/current校验。
  /** 清理原Entry时仅核引用身份，不把当前DATA读资格变成清理前置。 */
  private[floatingpod] def requireOriginalMainAdapter(actual:MySqlOwnedIoAdapter):Unit =
    mainIdentity(actualServer)
    require(actual!=null && (actual eq adapter),"original schema6 Main adapter reference required")
  private[floatingpod] def requireMainPublication(identity:MySqlOwnedIoIdentity,binding:PhysicalRecoveryBinding,
      configuredLimit:Int):Unit =
    val current=currentRequestData
    require((identity eq adapter.currentExecutionIdentity) && current.binding.contains(binding) && configuredLimit==activeLimit,
      "schema6 publication must preserve original current session/binding/capacity")
  /** 只由manager实际安装的原scope登记；不发行runtime或停服许可。 */
  private[floatingpod] def retainMainPublication(manager:ActiveFloatingPodManager,
      scope:ActiveFloatingPodManager#MainPublicationScope):Unit =
    mainIdentity(actualServer)
    require(manager!=null && scope!=null && !publicationRetained,"schema6 original publication retained once")
    manager.requireDbFirstPublication(this,scope)
    mainPublication=scope;publicationRetained=true
  private[floatingpod] def requireFinalSourceOwner(owner:Object,escrow:ManagedOrdinarySaveAdmission.SourceEscrow):Unit =
    mainIdentity(actualServer)
    require(mainPublication!=null,"original schema6 publication source owner required")
    mainPublication.requireFinalSourceOwner(owner,escrow)
  private[floatingpod] def requireFinalSource(owner:Object,escrow:ManagedOrdinarySaveAdmission.SourceEscrow,
      source:ManagedOrdinarySaveAdmission.FinalSaveValidator):Unit =
    requireFinalSourceOwner(owner,escrow)
    mainPublication.requireFinalSource(owner,escrow,source)
  private[floatingpod] def dropFinalMainPublication(scope:ActiveFloatingPodManager#MainPublicationScope):Unit =
    mainIdentity(actualServer)
    require(mainPublication!=null && (mainPublication eq scope) && saveAdmission!=null && saveAdmission.isFinalActive,
      "original schema6 final source must be active before publication detach")
    mainPublication=null

  private def currentRequestBacking:MySqlDbFirstMainReadBacking =
    mainIdentity(actualServer)
    // 暂拒发生在borrow的sticky failure边界之前，不触碰已经失效的旧backing。
    if saveAdmission!=null then saveAdmission.requireMainReadAvailable()
    borrowClaimedMainReads(actualServer)
  private def currentRequestData:MySqlBootMainReadData = currentRequestBacking.data
  private[floatingpod] def recallRequestNamespace(authenticatedActor:UUID):RecallRequestNamespace =
    currentRequestBacking.recallNamespace(authenticatedActor)
  private[floatingpod] def lookupRecallClientRequest(authenticatedActor:UUID,requested:RecallRequestNamespace,
      request:UUID):MySqlDbFirstRecallRequestLookup =
    currentRequestBacking.lookupRecallClientRequest(authenticatedActor,requested,request)
  private[floatingpod] def lookupRecallOriginalOperation(authenticatedActor:UUID,requested:RecallRequestNamespace,
      operation:UUID):MySqlDbFirstRecallRequestLookup =
    currentRequestBacking.lookupRecallOriginalOperation(authenticatedActor,requested,operation)
  override private[floatingpod] def requireCurrentReads():Unit =
    currentRequestData
    ()
  override def findState(id:UUID):Option[FloatingPodState] = currentRequestData.state(id)
  private[floatingpod] def recoveryReadData:MySqlBootMainReadData = currentRequestData
  private[floatingpod] def claimBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Vector[MySqlDbFirstRecallRequestRecord] =
    mainIdentity(actualServer)
    adapter.claimBootRecallCensus(owner)
  private[floatingpod] def detachBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit =
    mainIdentity(actualServer)
    adapter.detachBootRecallCensus(owner)
  /** 只由已封停的原Boot Main撤失败停止借用；Adapter按稳定登记的真实Session定位census。 */
  private[floatingpod] def detachStoppedBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit =
    mainIdentity(actualServer)
    require(owner!=null,"actual failed-stop Boot census owner required")
    adapter.detachStoppedBootRecallCensus(owner)

  private[floatingpod] def currentSourceLimitProfile:com.wi.floatingpod.config.SourceLimitProfile =
    borrowClaimedMainReads(actualServer).sourceAllocationForMain.exactProfile()

  private[floatingpod] def consumeRecoverySnapshot(state:FloatingPodState,attempt:UUID,
      use:net.minecraft.nbt.NBTTagCompound=>Unit):Unit =
    requireCurrentReads()
    borrowClaimedMainReads(actualServer).consumeRecoverySnapshot(state,attempt,use)
  override def findEntitlement(id:UUID):Option[PlayerEntitlement] = currentRequestData.entitlement(id)
  override def activeCount:Int = currentRequestData.activeCount
  override def activeCountFor(id:UUID):Int = currentRequestData.activeCountFor(id)
  override def activeLimit:Int =
    requireCurrentReads()
    approvedControl.metadata().deploymentSemantics().canonicalFields().get("activeLimit").toInt

  private[floatingpod] def consumeClaimedMainReads(server:MinecraftServer):MySqlDbFirstMainReadBacking =
    mainIdentity(server);require(!consumed,"schema6 Main preparation is consumed once");consumed=true
    borrowClaimedMainReads(server)
  /** 读资格失效后仍由原Main履行drop；不把drop当成实体、保存或SQL已完成。 */
  private[floatingpod] def dropClaimedMainReads(server:MinecraftServer):Unit =
    mainIdentity(server)
    require(!dropped,"schema6 Main reads already dropped")
    // claim后第一次borrow亦可能因freshness/world失效失败；原registry仍持有必须drop的backing。
    adapter.dropClaimedDbFirstMainReads()
    borrowed=null;dropped=true

  private[floatingpod] def dbFirstApplicationControlEnded:Boolean =
    mainIdentity(actualServer)
    control.ordinaryApplicationControlEnded(this,originalReceipt)

  private[floatingpod] def requireDbFirstApplicationRetirement(actual:MySqlOwnedIoAdapter,
      runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      prepared:MySqlOwnedIoAdapter#BootstrapPreparation,
      original:MySqlOwnedIoReply.DbFirstSqlWriterAcquired,receipt:Object):Unit =
    mainIdentity(actualServer)
    runtime.requireApplicationRetirementOwner(this)
    require((actual eq adapter) && (prepared eq preparationBacking) && (original eq reply) &&
      (receipt eq originalReceipt) && dropped && borrowed==null && mainPublication==null &&
      sourceCustody==null && mainFailure==null && !applicationRetired && saveAdmission.isFinalActive,
      "original dropped schema6 Main and two-stage application required")
    saveAdmission.requireHealthyAfterFlush()
    control.requireOrdinaryApplicationPreparation(this,originalReceipt,reply)

  private[floatingpod] def retireDbFirstApplication(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime):Unit =
    mainIdentity(actualServer)
    runtime.requireApplicationRetirementOwner(this)
    require(dbFirstApplicationControlEnded && !applicationRetired && mainPublication==null &&
      sourceCustody==null && mainFailure==null,"original schema6 control/application retirement required")
    dropClaimedMainReads(actualServer)
    val original=preparation
    adapter.retireDbFirstApplicationPreparation(this,runtime,original,reply,originalReceipt)
    preparationBacking=null;reply=null;applicationRetired=true
    control.requireOrdinaryApplicationPreparationRetired(this,originalReceipt)

  private[floatingpod] def registerDbFirstMainEnded(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    mainIdentity(actualServer)
    runtime.requireMainEndedOwner(this,receipt)
    require(applicationRetired && dropped && borrowed==null && reply==null && preparationBacking==null &&
      mainEndedReceipt==null && claimedExit==null,"original schema6 Main-only receipt registration once")
    receipt.consumeOriginal(adapter.identity)
    mainEndedReceipt=receipt

  private[floatingpod] def requireDbFirstWorkerHandoff(actual:MySqlOwnedIoAdapter,
      runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    mainIdentity(actualServer)
    runtime.requireWorkerHandoffOwner(this,receipt)
    require((actual eq adapter) && receipt!=null && (mainEndedReceipt eq receipt) && applicationRetired &&
      dropped && borrowed==null && mainPublication==null && sourceCustody==null && mainFailure==null,
      "original schema6 registered Main-only handoff required")
    receipt.requireRegisteredIdentity(adapter.identity)
    control.requireOrdinaryApplicationPreparationRetired(this,originalReceipt)
    saveAdmission.requireHealthyAfterFlush()

  private[floatingpod] def beginCertifiedDbFirstCompletion(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):ClaimedBootstrapExit =
    requireDbFirstWorkerHandoff(adapter,runtime,receipt)
    require(claimedExit==null,"original schema6 finalizer one-shot")
    val actual=new ClaimedBootstrapExit(adapter,claimedMain,approvedControl.policy().limits().finalization())
    claimedExit=actual
    actual.beginCertifiedDbFirst(this,runtime,receipt)
    actual

  private[floatingpod] def dbFirstExitObservationPending(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Boolean =
    requireDbFirstWorkerHandoff(adapter,runtime,receipt)
    require(claimedExit!=null,"original schema6 finalizer required")
    claimedExit.ordinaryObservationPending

  private[floatingpod] def requireCertifiedDbFirstExitCompleted(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    requireDbFirstWorkerHandoff(adapter,runtime,receipt)
    require(claimedExit!=null,"original schema6 finalizer required")
    claimedExit.requireOrdinaryObservationReturned()
    DbFirstFinalizationObservation.requireClosed(claimedExit.checkpoint.getOrElse(
      throw new IllegalStateException("actual schema6 finalizer checkpoint missing")))

  private[floatingpod] def detachDbFirstExitObservation(runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime):Unit =
    runtime.requireFinalSaveEndOwner(this)
    requireCertifiedDbFirstExitCompleted(runtime,mainEndedReceipt)
    require(!finalExitDetached,"original schema6 observer detaches once")
    finalCheckpoint=claimedExit.checkpoint.get
    finalExitDetached=true
    claimedExit=null;mainEndedReceipt=null

  private[floatingpod] def retainClaimedFailure(server:MinecraftServer,cause:Throwable):Unit =
    mainIdentity(server);require(cause!=null,"original claimed schema6 failure required")
    retainClaimedFailureCause(cause)
    if finalExitDetached then return // 原worker已真实关闭；final-save提交失败由原Admission/Scope保留，不重开退出线程。
    if claimedExit==null then
      val exit=new ClaimedBootstrapExit(adapter,claimedMain,approvedControl.policy().limits().finalization())
      claimedExit=exit
      prepareClaimedFailureMainDrop(server,cause)
      exit.begin(mainFailure)
    else claimedExit.retainFailure(cause)
  private def retainClaimedFailureCause(cause:Throwable):Unit =
    if mainFailure==null then mainFailure=cause
    else MySqlOwnedIoAdapter.suppressExitCleanup(mainFailure,cause)
  /** Main必须先封source并drop实际backing，再由后续exit helper quiesce worker。 */
  private[floatingpod] def prepareClaimedFailureMainDrop(server:MinecraftServer,cause:Throwable):Unit =
    mainIdentity(server);require(cause!=null,"original claimed schema6 failure required")
    retainClaimedFailureCause(cause)
    if saveAdmission!=null && !failureAdmissionUnknownAttempted then
      failureAdmissionUnknownAttempted=true
      try saveAdmission.unknownAtBoundary(mainFailure)
      catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(mainFailure,cleanup)
    if finalExitDetached then return
    if sourceCustody!=null && !failureSourceStopAttempted then
      failureSourceStopAttempted=true
      try sourceCustody.beginStoppingSourceWork()
      catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(mainFailure,cleanup)
    if !dropped && !failureMainDropAttempted then
      failureMainDropAttempted=true
      try dropClaimedMainReads(server)
      catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(mainFailure,cleanup)
  private[floatingpod] def observedClaimedExit:Option[ClaimedBootstrapExit]=Option(claimedExit)

  /** custody的公开签发点必须回核原callback已领用的同一backing/root/world。 */
  private[floatingpod] def requireOriginalSourceCustodyRetention(backing:MySqlDbFirstMainReadBacking,
      root:SourceForestAllocationBudget.Root,server:MinecraftServer,context:DedicatedWorldContext):Unit =
    mainIdentity(server)
    require(sourceCustodyAttempted && sourceCustody==null && sourceCustodyFailure==null &&
      backing!=null && (borrowed eq backing) && (context eq worldContext) &&
      (backing.sourceAllocationForMain eq root),"original schema6 callback/backing/source custody issuance required")
    worldContext.validateAboutToStart(server)
  private def retainSourceCustody(server:MinecraftServer):NormalRecallRecoverySourceCustody =
    mainIdentity(server)
    if sourceCustodyFailure!=null then throw sourceCustodyFailure
    if sourceCustodyAttempted then
      require(sourceCustody!=null,"original schema6 source custody issuance incomplete")
      sourceCustody.requireAllocationResponsibility()
      return sourceCustody
    val backing=borrowClaimedMainReads(server)
    val root=backing.sourceAllocationForMain
    sourceCustodyAttempted=true
    try
      sourceCustody=NormalRecallRecoverySourceCustody.retainDbFirst(this,backing,server,worldContext,root)
      sourceCustody.retainDbFirstProtection(backing)
      sourceCustody
    catch case cause:Throwable=>
      sourceCustodyFailure=cause
      throw cause
  override def verifyClosedBootSaveScopeIssuer(server:MinecraftServer):Unit =
    mainIdentity(server);require(!saveIssuerAttempted,"schema6 closed save issuer is one-shot");saveIssuerAttempted=true
    // 常态保存只领用原Full27读资格与ordinary owner；不为所有loader重建已退出的forensic生产链。
    borrowClaimedMainReads(server).data
    val maximumLive=Math.toIntExact(math.min(Int.MaxValue.toLong,approvedControl.sourceLimitProfile().maxRetainedBytes()/512L))
    saveAdmission=new ManagedOrdinarySaveAdmission(claimedMain,maximumLive,this)
    adapter.installOrdinarySaveAdmission(saveAdmission)
    adapter.installDbFirstReadCallbacks(this)
  override def verifyClosedBootSaveScopeUse(server:MinecraftServer):Unit =
    mainIdentity(server);worldContext.validateAboutToStart(server)
  override private[floatingpod] def retainedRecallSourceCustodyForClosedScope(server:MinecraftServer):Option[NormalRecallRecoverySourceCustody] =
    verifyClosedBootSaveScopeUse(server)
    require(saveIssuerAttempted && saveAdmission!=null,"original schema6 ordinary save owner missing")
    if sourceCustodyFailure!=null then throw sourceCustodyFailure
    Option(sourceCustody).map { actual=>actual.requireAllocationResponsibility();actual }
  override def retainTemporary(bytes:Long,purpose:String):AutoCloseable =
    mainIdentity(actualServer)
    if saveAdmission!=null && saveAdmission.isFinalActive then
      return saveAdmission.retainFinalProviderTemporary(bytes,purpose)
    require(borrowed!=null && !dropped && mainFailure==null,"original provider allocation holder required")
    borrowed.retainOrdinaryProviderTemporary(bytes,purpose)
  override def decide(provider:ChunkProviderServer,world:WorldServer,chunk:Chunk,
      operation:ManagedOrdinarySaveAdmission.ProviderOperation):ManagedOrdinarySaveAdmission.ProviderDecision =
    decideManagedSourceProviderChunk(actualServer,provider,world,chunk,operation)
  private[floatingpod] def detachSupersededMainRead(previous:MySqlDbFirstMainReadBacking):Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && sourceCustody==null && (borrowed eq previous),
      "original unrestricted borrowed Main generation required before actual drop")
    previous.drop()
    borrowed=null
  private[floatingpod] def attachSuccessorMainRead(next:MySqlDbFirstMainReadBacking):Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && borrowed==null && (adapter.claimedDbFirstMainReads eq next),
      "original registered current successor required")
    borrowed=next
  /** 新服务控制交接只恢复callback实际借用；普通save/read仍由旧hold封住。 */
  private[floatingpod] def attachRecoveredMainRead(next:MySqlDbFirstMainReadBacking):Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && sourceCustody==null && borrowed==null && next!=null,
      "actual detached unrestricted Main callback required before recovered backing attach")
    adapter.requireRecoveredMainReadAttach(this,next)
    borrowed=next
  private[floatingpod] def adoptRecoveredMainPublication(scope:ActiveFloatingPodManager#MainPublicationScope,
      next:MySqlDbFirstMainReadBacking,identity:MySqlOwnedIoIdentity,binding:PhysicalRecoveryBinding):Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && sourceCustody==null && next!=null && (borrowed eq next) &&
      publicationRetained && (mainPublication eq scope),"actual original callback/backing/publication required")
    adapter.requireRecoveredPublicationDispatch(this,scope,identity)
    val configuredLimit=approvedControl.metadata().deploymentSemantics().canonicalFields().get("activeLimit").toInt
    require(binding==scope.binding && configuredLimit==scope.activeLimit,
      "actual recovered backing binding and approved configuration must match the retained publication scope")
    scope.adoptRecoveredExecutionIdentity(this,adapter,identity,binding,configuredLimit)
  private[floatingpod] def resumeRecoveredService(next:MySqlDbFirstMainReadBacking,
      identity:MySqlOwnedIoIdentity):Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && next!=null && (borrowed eq next) &&
      resumingTag==null && resumingRecoveredIdentity==null,"actual original recovered save callback required")
    adapter.requireRecoveredSaveResume(this,next,identity)
    resumingRecoveredIdentity=identity
    try saveAdmission.resumeConfirmedSuccessor(this)
    finally resumingRecoveredIdentity=null
  private[floatingpod] def resumeOrdinarySuccessor(tag:IoOperationTag):Unit =
    mainIdentity(actualServer)
    require(resumingTag==null && resumingRecoveredIdentity==null,"ordinary successor resume is not reentrant")
    resumingTag=tag
    try saveAdmission.resumeConfirmedSuccessor(this)
    finally resumingTag=null
  override def requireConfirmedSuccessor():Unit =
    mainIdentity(actualServer)
    require(mainFailure==null && !dropped && borrowed!=null,"actual live successor callback required")
    if resumingRecoveredIdentity!=null then
      require(resumingTag==null,"recovered service cannot impersonate an ordinary successor tag")
      adapter.requireRecoveredSaveResume(this,borrowed,resumingRecoveredIdentity)
    else
      require(resumingTag!=null,"actual ordinary successor resume boundary required")
      adapter.requireCurrentDbFirstSaveSuccessor(this,resumingTag)
      require(adapter.claimedDbFirstMainReads eq borrowed,"original callback successor binding required")
  override def ordinarySaveAdmission():ManagedOrdinarySaveAdmission =
    require(saveAdmission!=null,"original schema6 save admission unavailable")
    saveAdmission
  private[floatingpod] def captureCurrentSourceDomain(server:MinecraftServer):Unit =
    verifyClosedBootSaveScopeUse(server)
    require(sourceCustody!=null,"original schema6 source custody required")
    sourceCustody.captureCurrentDomain()
  override private[floatingpod] def decideManagedSourceProviderChunk(server:MinecraftServer,
      provider:ChunkProviderServer,world:WorldServer,chunk:Chunk,
      operation:ManagedOrdinarySaveAdmission.ProviderOperation):ManagedOrdinarySaveAdmission.ProviderDecision =
    verifyClosedBootSaveScopeUse(server)
    require(provider!=null && world!=null && chunk!=null && operation!=null &&
      (world.getMinecraftServer eq server) && (world.getChunkProvider eq provider) && (provider.world eq world) &&
      (chunk.getWorld eq world) && world.provider!=null && saveAdmission!=null,
      "actual original schema6 source provider/world/chunk required")
    if saveAdmission.isFinalActive then
      // 已激活的原final source承担完整物理cohort/诊断壳核对；不再回借退休前的保护DATA。
      saveAdmission.requireFinalMainSave(world,chunk.x,chunk.z,chunk)
      return ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW
    val protection=if sourceCustody!=null then
      sourceCustody.classifyDbFirstProtection(world.provider.getDimension,chunk.x,chunk.z)
    else borrowClaimedMainReads(server).data.sourceProtection.classify(world.provider.getDimension,chunk.x,chunk.z)
    protection match
      case _:MySqlBootSourceProtectionIndex.Protected | _:MySqlBootSourceProtectionIndex.Unknown=>
        ManagedOrdinarySaveAdmission.ProviderDecision.HELD
      case MySqlBootSourceProtectionIndex.Unprotected=>
        // 已实际领用的restricted source责任仍需其原证明；普通保存继续走原token/queue/flight校验。
        if sourceCustody!=null then ManagedOrdinarySaveAdmission.ProviderDecision.DEFER
        else ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW
  override def requireOrdinarySaveCurrent(server:MinecraftServer,dimension:Int,x:Int,z:Int,chunk:Chunk):Unit =
    require(chunk!=null && chunk.x==x && chunk.z==z && chunk.getWorld.isInstanceOf[WorldServer],
      "actual schema6 save chunk required")
    val world=chunk.getWorld.asInstanceOf[WorldServer]
    require(world.provider!=null && world.provider.getDimension==dimension && world.getChunkProvider.isInstanceOf[ChunkProviderServer],
      "actual schema6 save dimension/provider required")
    if saveAdmission!=null && saveAdmission.isFinalActive then
      verifyClosedBootSaveScopeUse(server)
      require(world.getMinecraftServer eq server,"original schema6 final save world required")
      saveAdmission.requireFinalMainSave(world,x,z,chunk)
      return
    decideManagedSourceProviderChunk(server,world.getChunkProvider.asInstanceOf[ChunkProviderServer],world,chunk,
      ManagedOrdinarySaveAdmission.ProviderOperation.SAVE) match
      case ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW=>()
      case ManagedOrdinarySaveAdmission.ProviderDecision.HELD=>
        throw new IllegalStateException("schema6 source save is held by original protected or UNKNOWN responsibility")
      case ManagedOrdinarySaveAdmission.ProviderDecision.DEFER=>
        throw new IllegalStateException("schema6 source save lacks actual outside-unchanged admission")

  override def finalizeFailure(value:Object,exactReceipt:Object,cause:Throwable):CompletionStage[Void] =
    owner();require(cause!=null && claimedMain==null,"old control cannot finalize claimed schema6 resources")
    try finalPreparation=Some(preparation.finalizePreparation(activePolicy.limits().finalization()))
    catch case cleanup:Throwable=>
      finalPreparation=Some(preparation.observation)
      if cleanup ne cause then cause.addSuppressed(cleanup)
    try adapter.quiesce()
    catch case cleanup:Throwable=>if cleanup ne cause then cause.addSuppressed(cleanup)
    ProductionDedicatedBootstrapCallbacks.finalizeOffOwner(adapter,activePolicy.limits().finalization(),cause,
      checkpoint=>finalCheckpoint=checkpoint)
  override def failedStartupTermination(report:DedicatedBootstrapBridge.FailureReport):Unit =
    owner();require(report!=null && (report.actualServer() eq actualServer),"actual schema6 startup failure required")
    failureReport=report
    ProductionDedicatedBootstrapCallbacks.safeFailure(report.originalCause(),new IdentityHashMap(),"schema6-startup")
    ProductionBootstrapInstallation.invokeStartupFailureShutdown(actualServer,this,report)
  def requireOriginalStartupFailure(report:DedicatedBootstrapBridge.FailureReport):Unit =
    owner()
    require(report!=null && (report eq failureReport) && (report.actualServer() eq actualServer) && claimedMain==null,
      "original unclaimed schema6 failure report required")
  def observedFailure:Option[DedicatedBootstrapBridge.FailureReport]=Option(failureReport)
  def observedFinalCheckpoint:Option[MySqlOwnedIoCheckpoint]=Option(finalCheckpoint)
  def observedFinalPreparation:Option[IoPreparationObservation]=finalPreparation

object ProductionDbFirstBootstrapCallbacks:
  // Prepare的原Entry须保留到initial activation退休；两者同时存活，不是活动Pod配额。
  private val InitialPublicationWorkCapacity = 2
  /** 由正式factory在其对应Main保存/恢复生命周期接齐后选择；此入口不重新发现配置或推导Fresh模式。 */
  def open(actual:Object,control:DedicatedBootstrapBridge,input:ProductionBootstrapControl.DbFirstControl,
      dependencies:EarlyDependencyResolver.VerifiedDependencies):DedicatedBootstrapBridge.Callbacks =
    require(control!=null && input!=null && dependencies!=null &&
      (Thread.currentThread() eq control.controlThread()) && control.phase()==DedicatedBootstrapBridge.Phase.PREPARING,
      "exact preparing schema6 bootstrap control required")
    val server=actual match
      case value:MinecraftServer if value.isDedicatedServer=>value
      case _=>throw new IllegalArgumentException("actual dedicated MinecraftServer required")
    require(dependencies.businessBackend()=="MYSQL","verified MYSQL dependencies required")
    val world=DedicatedWorldContext.capture(server)
    require(world.worldDirectory().toAbsolutePath.normalize()==input.worldPath(),"schema6 actual world differs from approved path")
    val ids=input.identity()
    val identity=MySqlOwnedIoIdentity(ids.boot(),ids.bootGeneration(),ids.connectionGeneration())
    val tag=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,ids.acquireOperation())
    val adapter=new MySqlOwnedIoAdapter(InitialPublicationWorkCapacity,Thread.currentThread(),
      "FloatingPod schema6 production owned IO",identity,enableAdmissionMaintenance=true)
    try
      val preparation=adapter.installDbFirstBootstrapPreparation(control,input)
      val callbacks=new ProductionDbFirstBootstrapCallbacks(server,control,input,world,dependencies,adapter,tag,preparation,
        input.policy(),None)
      ProductionBootstrapInstallation.retain(server,control,callbacks)
      callbacks
    catch case original:Throwable=>
      try adapter.quiesce()
      catch case cleanup:Throwable=>if cleanup ne original then original.addSuppressed(cleanup)
      try ProductionDedicatedBootstrapCallbacks.finalizeOffOwner(adapter,input.policy().limits().finalization(),original,_=>())
        .toCompletableFuture.get(input.policy().limits().finalization().toNanos,TimeUnit.NANOSECONDS)
      catch case cleanup:Throwable=>
        val cause=cleanup match
          case e:ExecutionException if e.getCause!=null=>e.getCause
          case other=>other
        if cause ne original then original.addSuppressed(cause)
      throw original

  /** Managed mode uses the same world pin, actual adapter, preparation, observe/acquire and transfer lifecycle. */
  def openManaged(actual:Object,control:DedicatedBootstrapBridge,configuration:ManagedDatabaseConfiguration,
      limits:DedicatedBootstrapBridge.Limits,dependencies:EarlyDependencyResolver.VerifiedDependencies):DedicatedBootstrapBridge.Callbacks =
    require(control!=null && configuration!=null && limits!=null && dependencies!=null &&
      (Thread.currentThread() eq control.controlThread()) && control.phase()==DedicatedBootstrapBridge.Phase.PREPARING,
      "exact preparing managed schema6 bootstrap holder required")
    val server=actual match
      case value:MinecraftServer if value.isDedicatedServer=>value
      case _=>throw new IllegalArgumentException("actual dedicated MinecraftServer required")
    require(dependencies.businessBackend()=="MYSQL","verified MYSQL release dependencies required")
    require(ManagedBootstrapPolicy.limits(configuration.initializationTimeoutMillis())==limits,
      "managed configuration and selected bridge limits differ")
    val world=DedicatedWorldContext.capture(server)
    val ids=ProductionBootstrapControl.DbFirstIdentity(nonzeroUuid(),1L,1L,nonzeroUuid(),nonzeroUuid())
    val ownedIdentity=MySqlOwnedIoIdentity(ids.boot(),ids.bootGeneration(),ids.connectionGeneration())
    val tag=IoOperationTag(ownedIdentity.bootId,ownedIdentity.bootGeneration,ownedIdentity.connectionGeneration,ids.acquireOperation())
    val policy=ManagedBootstrapPolicy.policy(configuration.initializationTimeoutMillis())
    val request=new MySqlManagedBootstrapRequest(configuration,world.worldDirectory().toAbsolutePath.normalize(),
      ids,policy,ManagedReleaseDependencies.expectedPins())
    val adapter=new MySqlOwnedIoAdapter(InitialPublicationWorkCapacity,Thread.currentThread(),
      "FloatingPod managed schema6 owned IO",ownedIdentity,enableAdmissionMaintenance=true)
    try
      val preparation=adapter.installManagedDbFirstBootstrapPreparation(control,request)
      val callbacks=new ProductionDbFirstBootstrapCallbacks(server,control,null,world,dependencies,adapter,tag,
        preparation,policy,Some(request))
      ProductionBootstrapInstallation.retain(server,control,callbacks)
      callbacks
    catch case original:Throwable=>
      try adapter.quiesce()
      catch case cleanup:Throwable=>if cleanup ne original then original.addSuppressed(cleanup)
      try ProductionDedicatedBootstrapCallbacks.finalizeOffOwner(adapter,limits.finalization(),original,_=>())
        .toCompletableFuture.get(limits.finalization().toNanos,TimeUnit.NANOSECONDS)
      catch case cleanup:Throwable=>
        val cause=cleanup match
          case e:ExecutionException if e.getCause!=null=>e.getCause
          case other=>other
        if cause ne original then original.addSuppressed(cause)
      throw original

  private def nonzeroUuid():UUID =
    var value=UUID.randomUUID()
    while value==new UUID(0L,0L) do value=UUID.randomUUID()
    value
