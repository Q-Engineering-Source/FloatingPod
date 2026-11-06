package com.wi.floatingpod.server

import com.wi.floatingpod.entity.{FloatingPodEntity,FloatingPodControlSnapshot}
import com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks
import java.util.UUID
import com.wi.floatingpod.domain.{RecallRequestNamespace,RecallRefusalReason}
import com.wi.floatingpod.network.{DurableRecallPhase,S2CDurableRecallStatus}
import com.wi.floatingpod.storage.mysql.{MySqlBootMainReadData,MySqlDbFirstRecallRequestPhase,MySqlDbFirstRecallRequestRecord,MySqlOwnedIoAdapter}
import org.apache.logging.log4j.LogManager
import scala.collection.mutable
import scala.math.floor
import scala.util.control.NonFatal
import com.wi.floatingpod.server.io.{IoCompletion,IoOperationTag,IoSubmission}
import com.wi.floatingpod.storage.mysql.{MySqlDbFirstBootRecallMainDecision,MySqlDbFirstBootRecallTakeoverIntent,MySqlDbFirstCaptureReservation,MySqlDbFirstRecallRefusalConfirmation,MySqlDbFirstRecallRefusalFinalReceipt,MySqlOwnedIoCommand,MySqlOwnedIoReply}

/** schema6原Main的实际恢复责任；每个Pod独立收口，不要求世界旧壳全域清零，也不发行Ready。 */
private[floatingpod] final class DbFirstBootActiveRecovery private (
    callbacks: ProductionDbFirstBootstrapCallbacks,
    val tickets: ChunkLoadingManager,
    val manager: ActiveFloatingPodManager
):
  private val server = callbacks.actualServer
  private val main = server.getServerThread
  private val logger = LogManager.getLogger(getClass)
  private var started = false
  private var installationAttempted = false
  private var installed = false
  private var finished = false
  private var originalRecallRequests=Option.empty[DbFirstBootRecallRequestCustody]
  private var podRestoration=Option.empty[DbFirstBootActiveRecovery.PodRestorationEntry]
  private var resultPassAttempted=false
  private var resultPass=Option.empty[DbFirstBootActiveRecovery.TerminalRecoveryPass]
  private var initialTerminalHandoffAttempted=false
  private var initialTerminalHandoffCompleted=false
  private var initialTerminalHandoffCause=Option.empty[Throwable]
  private var restoringTerminal:MySqlDbFirstRecallRequestRecord=null
  private var resultsSealed=false
  private var finalMainReleased=false
  private var censusDetached=false
  private var failureStopCause:Throwable=null
  private var stoppedCensusDetachAttempted=false
  private var stoppedCensusDetachDispatch=false
  private var failurePublicationStopAttempted=false
  private var publicationInstallationAttempted=false
  private var publicationFailureStopped=false
  private var internalPublication=Option.empty[ActiveFloatingPodManager#MainPublicationScope]
  private var publicationPump=Option.empty[ForgePermanentMainPump]
  private var publicationLease=Option.empty[PermanentPumpLease]
  private enum RefusalStage:
    case New, Sql, SubmitRead, ReadAck, Detach
  private final class JournalTakeover(val intent:MySqlDbFirstBootRecallTakeoverIntent):
    var submissionAttempted=false
    var submission=Option.empty[IoSubmission]
    val controlCompletion=new DbFirstRecallJournalCompletion(main,intent.controlTag)
    var reservation=Option.empty[MySqlDbFirstCaptureReservation]
    var cause=Option.empty[Throwable]
    var mainDecisionAttempted=false
    var mainDecision=Option.empty[MySqlDbFirstBootRecallMainDecision]
    var refusalStage=RefusalStage.New
    val refusalCompletion=new DbFirstRecallJournalCompletion(main,intent.executionTag)
    var refusalConfirmation=Option.empty[MySqlDbFirstRecallRefusalConfirmation]
  /** 撤下整个takeover后仅保独立原census record、枚举与最终metadata；不再持source/intent/reservation。 */
  private final class RefusalFinal(val original:MySqlDbFirstRecallRequestRecord,val reason:RecallRefusalReason,
      val receipt:MySqlDbFirstRecallRefusalFinalReceipt,val detachedEnd:Long):
    val completion=new DbFirstRecallJournalCompletion(main,receipt.tag)
    var submitted=false
    var acknowledged=false
  private enum PositiveStage:
    case Sql, Physical, Detached, FinalAck, RetainedStopped
  private final class PositiveRecall(val original:MySqlDbFirstRecallRequestRecord,val tag:IoOperationTag,
      val scope:ActiveFloatingPodManager#MainPublicationScope):
    var stage=PositiveStage.Sql
    var entryRegistered=false
    var awaiting=false
    var completion=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var graphDetached=false
    var detachedEnd=0L
    var acknowledged=false
    var serviceMainAcknowledged=false
    var servicePublicationAcknowledged=false
    var serviceExecutionAcknowledged=false
    var cause=Option.empty[Throwable]
    val retainedEnd=new DbFirstRecallRetainedEnd
  private var positiveRecall=Option.empty[PositiveRecall]
  private var issuingCapture:JournalTakeover=null
  private var captureStage=0
  private var detachingPositive:JournalTakeover=null
  private var completingCommitted:PositiveRecall=null
  private var completingRetainedCommitted:PositiveRecall=null
  private var positiveDispatch:PositiveRecall=null
  private var positiveTick:PermanentEndTick=null
  private var lastPositiveEnd=0L
  private var refusalFinal=Option.empty[RefusalFinal]
  private var detachingTakeover:JournalTakeover=null
  private var completingRefusal:RefusalFinal=null
  private var lastRefusalEnd=0L
  private var issuingMainDecision:JournalTakeover=null
  private var mainDecisionStage=0
  private var observedMainReason=Option.empty[Option[RecallRefusalReason]]
  private var journalTakeover=Option.empty[JournalTakeover]
  private var issuingTakeover:MySqlDbFirstRecallRequestRecord=null
  private var issuingControl:IoOperationTag=null
  private var issuingExecution:IoOperationTag=null
  private var unmatchedBootCompletions=Vector.empty[IoCompletion[MySqlOwnedIoReply]]
  private val recovered = mutable.LinkedHashSet.empty[UUID]
  private final case class FailedPod(receipt: Option[StartupRecoveryRollbackReceipt], cause: Throwable)
  private val failed = mutable.LinkedHashMap.empty[UUID, FailedPod]
  private def cleanupOwner():Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread,
      "schema6 boot recovery requires the actual claimed server Main")
    callbacks.requireOriginalMainAdapter(callbacks.adapter)
  private def owner():Unit =
    cleanupOwner()
    callbacks.requireCurrentReads()

  /** runtime须先保存本owner再调用；登记可能部分成功，attempted先记且不重新登记。 */
  def install(): Unit =
    owner()
    require(!installationAttempted && !started, "original schema6 recovery installation is one-shot")
    installationAttempted = true
    originalRecallRequests=Some(new DbFirstBootRecallRequestCustody(main,callbacks.claimBootRecallCensus(this)))
    tickets.registerRecoveryCallbackOnce()
    installed = true

  private[floatingpod] def requireBootCensusClaim(adapter:MySqlOwnedIoAdapter):Unit =
    owner()
    require((adapter eq callbacks.adapter) && installationAttempted && !installed && !started &&
      originalRecallRequests.isEmpty && !censusDetached,"actual original Boot census installation required")
  private[floatingpod] def requireBootCensusDetached(adapter:MySqlOwnedIoAdapter):Unit =
    require(adapter eq callbacks.adapter,"original Boot metadata adapter required")
    requireBootCensusDetachedForRetention()
  private[floatingpod] def requireBootCensusDetachedForRetention():Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread &&
      censusDetached && finalMainReleased && resultsSealed && originalRecallRequests.isEmpty && resultPass.isEmpty &&
      internalPublication.isEmpty && publicationPump.isEmpty && publicationLease.isEmpty && podRestoration.isEmpty &&
      journalTakeover.isEmpty && refusalFinal.isEmpty && positiveRecall.isEmpty && detachingTakeover==null && completingRefusal==null && restoringTerminal==null &&
      issuingCapture==null && detachingPositive==null && completingCommitted==null && completingRetainedCommitted==null && positiveDispatch==null && positiveTick==null,
      "actual Boot Main metadata graph must be detached")
  private[server] def requireBootCensusCustodyDetach(actual:DbFirstBootRecallRequestCustody):Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread &&
      finalMainReleased && resultsSealed && !censusDetached && (actual eq requestCustody),
      "same final released Boot custody may detach once")
  private[server] def requireStoppedBootCensusCustodyDetach(actual:DbFirstBootRecallRequestCustody):Unit =
    cleanupOwner()
    require(failureStopCause!=null && stoppedCensusDetachDispatch && stoppedCensusDetachAttempted &&
      !censusDetached && (originalRecallRequests.contains(actual)),
      "same actual failed-stop Boot custody may detach without settling requests")

  /** worlds与Forge ticket回调实际完成后，由原runtime调用一次；不装第二个tick pump。 */
  def restoreLoadedWorlds(original:ProductionDbFirstBootstrapCallbacks,pump:ForgePermanentMainPump,
      lease:PermanentPumpLease,tick:PermanentEndTick): Unit =
    owner()
    require((original eq callbacks) && pump!=null && lease!=null && installed && !started && !finished &&
      podRestoration.isEmpty && server.worlds != null,"schema6 boot recovery is one-shot after world load")
    pump.assertRecoveringDispatch(lease,tick)
    val identity=callbacks.adapter.identity
    require(lease.bootUuid==identity.bootId && lease.bootGeneration==identity.bootGeneration,
      "same actual Boot Pod recovery pump required")
    require(mayRestoreOriginalPods,"actual original recovery cohort must permit Pod restoration")
    val binding=callbacks.recoveryReadData.binding.get.bindingUuid
    // Session census holder已独立预付512B；先保原entry，再开始任何ticket/entity恢复。
    podRestoration=Some(new DbFirstBootActiveRecovery.PodRestorationEntry(requestCustody,
      requestCustody.originalRequests,main,binding,identity.bootId,pump,lease))
    started = true
    val rows = callbacks.recoveryReadData.desiredActiveRows
    rows.foreach { row =>
      // raw desired-active行转换本身也可失败；该责任仍可按原UUID保留，不能静默略过。
      var receipt = Option.empty[StartupRecoveryRollbackReceipt]
      try
        owner()
        val state = row.toPublicState
        val rawWorld = Option(server.getWorld(state.dimensionId.get)).getOrElse(
          throw new IllegalStateException("No loaded world for active database Pod " + row.uuid))
        val world = PodWorldImpl(rawWorld)
        require(world.diagnosticWorldName == state.worldName, "active database Pod belongs to another world")
        val original = manager.beginStartupRecovery(state, world)
        receipt = Some(original)
        require(original.initialBody.isEmpty, "world-record body remains isolated; it cannot restore active business")
        if !tickets.managedTicketRecords().contains(row.uuid) then
          tickets.requestTicket(row.uuid, world,
            floor(state.posX / ChunkLoadingManager.ChunkSize).toInt,
            floor(state.posZ / ChunkLoadingManager.ChunkSize).toInt).get
        manager.spawnStartupRecoveryEntity(state, original).get
        manager.publishStartupRecovery(state, original).get
        manager.completeStartupRecoveries(List(original))
        recovered += row.uuid
      catch case NonFatal(cause) =>
        receipt.foreach { original =>
          try manager.retainDbFirstStartupRecoveryFailure(original, cause)
          catch case NonFatal(cleanup) => if cleanup ne cause then cause.addSuppressed(cleanup)
        }
        failed.put(row.uuid, FailedPod(receipt, cause))
        logger.error(s"schema6 database Pod recovery failed; pod=${row.uuid}, world=${row.worldName}; original body/ticket responsibility retained", cause)
    }
    // 本轮遍历结束并不表示失败Pod已修复、未知操作已协调或产品已Ready。
    owner()
    finished = true
    settleRecoveredResults()

  private def requestCustody:DbFirstBootRecallRequestCustody =
    originalRecallRequests.getOrElse(throw new IllegalStateException("original Boot request census not retained"))
  /** 完整原DATA只选择cohort；真实恢复仍须原runtime的Recovering dispatch和一次性entry。 */
  private[floatingpod] def mayRestoreOriginalPods:Boolean =
    owner()
    val data=callbacks.recoveryReadData
    val binding=data.binding.getOrElse(throw new IllegalStateException("actual Boot binding missing"))
    require(!started && podRestoration.isEmpty,"original Pod restoration admission is only selected before its attempt")
    (data.recallRequestCensus eq requestCustody.originalRequests) && data.sourceProtection.unresolved.isEmpty &&
      requestCustody.selectsPodRestoration(binding.bindingUuid)

  private def requireCompletedPodRestoration(pump:ForgePermanentMainPump,lease:PermanentPumpLease):Unit =
    owner()
    require(installed && started && finished && !resultsSealed && !finalMainReleased && !publicationFailureStopped,
      "same completed original Pod restoration required")
    val entry=podRestoration.getOrElse(throw new IllegalStateException("actual original Pod restoration entry missing"))
    val data=callbacks.recoveryReadData
    entry.requireOriginal(requestCustody,requestCustody.originalRequests,main,data.binding.get.bindingUuid,
      callbacks.adapter.identity.bootId,pump,lease)
    require(data.sourceProtection.unresolved.isEmpty,"shared unresolved recovery responsibility blocks internal publication")

  private[floatingpod] def mayRestoreOriginalRecallRequests:Boolean =
    owner()
    val binding=callbacks.recoveryReadData.binding.getOrElse(throw new IllegalStateException("actual Boot binding missing"))
    requestCustody.mayRestoreResults(binding.bindingUuid)
  private def resultPodRecovered(data:MySqlBootMainReadData,request:MySqlDbFirstRecallRequestRecord):Boolean =
    if failed.contains(request.podUuid) then false else data.state(request.podUuid) match
      case Some(state) if state.isActive=>
        recovered.contains(request.podUuid) && !failed.contains(request.podUuid) &&
          manager.managedActiveDetails(request.podUuid).exists(_.exactState==state) &&
          tickets.managedTicketRecords().contains(request.podUuid)
      case _=>manager.managedActiveDetails(request.podUuid).isEmpty &&
        !tickets.managedTicketRecords().contains(request.podUuid)
  private def resultPodsRecovered(data:MySqlBootMainReadData):Boolean =
    requestCustody.originalRequests.forall(resultPodRecovered(data,_))

  /** 混合cohort的原terminal在第一条J变更前交接；J-only仍须各自实际final ACK。
    * 初始完整DATA的eq门禁只用于本次恢复交接，完成后不借successor重解释旧结果。
    */
  private[floatingpod] def settleInitialTerminalResults(pump:ForgePermanentMainPump,
      lease:PermanentPumpLease,tick:PermanentEndTick):Boolean =
    owner();pump.assertRecoveringDispatch(lease,tick)
    initialTerminalHandoffCause.foreach(throw _)
    if initialTerminalHandoffCompleted then return true
    requireCompletedPodRestoration(pump,lease)
    val records=requestCustody.originalRequests
    if !records.exists(_.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted) then return true
    val data=callbacks.recoveryReadData
    val binding=data.binding.getOrElse(throw new IllegalStateException("initial terminal recovery binding missing"))
    require(!initialTerminalHandoffAttempted && journalTakeover.isEmpty && refusalFinal.isEmpty &&
      (data.recallRequestCensus eq records) && requestCustody.selectsPodRestoration(binding.bindingUuid) &&
      data.sourceProtection.unresolved.isEmpty,"initial complete cohort must precede first original J mutation")
    // 实际失败Pod保其原slot；未交接全部必要terminal时，不启动首条J。
    if !records.forall(record=>DbFirstBootRecallRequestCustody.terminalResult(record).isEmpty || resultPodRecovered(data,record)) then
      return false
    initialTerminalHandoffAttempted=true
    try
      records.foreach { record=>
        if DbFirstBootRecallRequestCustody.terminalResult(record).nonEmpty then
          restoringTerminal=record
          try requestCustody.completeRestoredTerminal(record,this)
          finally restoringTerminal=null
      }
      initialTerminalHandoffCompleted=true
      true
    catch case cause:Throwable=>
      initialTerminalHandoffCause=Some(cause)
      throw cause

  /** 普通JVM可见方法也必须回核实际issuer私有同步登记期，不能只持metadata/owner引用。 */
  private[server] def requireCompletingRestoredTerminal(actual:DbFirstBootRecallRequestCustody,
      original:MySqlDbFirstRecallRequestRecord):Unit =
    owner()
    require(initialTerminalHandoffAttempted && !initialTerminalHandoffCompleted && initialTerminalHandoffCause.isEmpty &&
      (restoringTerminal eq original) && original!=null && (actual eq requestCustody) &&
      requestCustody.originalRequests.exists(_ eq original) && journalTakeover.isEmpty && refusalFinal.isEmpty &&
      installed && started && finished && !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "actual private initial terminal restoration invocation required")
    val data=callbacks.recoveryReadData
    require((data.recallRequestCensus eq requestCustody.originalRequests) && data.binding.exists(_.bindingUuid==original.bindingUuid) &&
      data.sourceProtection.unresolved.isEmpty && DbFirstBootRecallRequestCustody.terminalResult(original).nonEmpty &&
      data.recallRequestCensus.exists(current=>DbFirstBootRecallRequestCustody.matchesResult(original,current,
        original.phase,original.refusalReason)) && resultPodRecovered(data,original),
      "same strict initial terminal request and actual restored Pod required")

  /** 不重放原C或旧final release；只在本次真实Pod恢复成功后发行一次typed交接。 */
  private[floatingpod] def settleRecoveredResults():Unit =
    owner()
    if requestCustody.unresolvedCount>0 && finished && !resultPassAttempted && mayRestoreOriginalRecallRequests then
      val data=callbacks.recoveryReadData
      require(data.recallRequestCensus eq requestCustody.originalRequests,
        "same original Boot census must remain current through recovery handoff")
      if data.sourceProtection.unresolved.isEmpty && resultPodsRecovered(data) then
        resultPassAttempted=true
        val pass=new DbFirstBootActiveRecovery.TerminalRecoveryPass(requestCustody,requestCustody.originalRequests,
          main,data.binding.get.bindingUuid,callbacks.adapter.identity.bootId)
        resultPass=Some(pass) // 原owner先保真实交接，构造/交付异常不丢责任或重新发行。
        requestCustody.completeResults(pass,this)

  /** Scala限定private不构成JVM权限边界；只承认本真实owner已登记的同一实例。 */
  private[server] def requireIssuedResultPass(pass:DbFirstBootActiveRecovery.TerminalRecoveryPass,
      actual:DbFirstBootRecallRequestCustody,records:Vector[MySqlDbFirstRecallRequestRecord],thread:Thread):Unit =
    owner()
    require(pass!=null && !resultsSealed && installed && started && finished && resultPassAttempted &&
      resultPass.exists(_ eq pass) && (actual eq requestCustody) &&
      (records eq requestCustody.originalRequests) && (thread eq main),
      "same actual Boot issuer registered terminal-result pass required")
    val data=callbacks.recoveryReadData
    require((data.recallRequestCensus eq records) && data.binding.exists(_.bindingUuid==pass.bindingUuid) &&
      callbacks.adapter.identity.bootId==pass.bootUuid && data.sourceProtection.unresolved.isEmpty &&
      resultPodsRecovered(data),"registered terminal-result pass must retain current original completed recovery")

  private[floatingpod] def sealRecallResults():Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread,"actual Boot result Main required")
    resultsSealed=true
  private[server] def requireResultSource(actualServer:net.minecraft.server.MinecraftServer,adapter:MySqlOwnedIoAdapter):Unit =
    owner();require((actualServer eq server) && (adapter eq callbacks.adapter) && !resultsSealed && installed && started && finished,
      "same live completed original Boot result owner required")
    requestCustody.requireSettled() // 新业务可改变当前desired-active；不重解释已完成的原Boot cohort。
  /** 历史结果继续由已计费census和本次真实pass保管；同步query回复不另占live结果槽。 */
  private[server] def recoveredResult(namespace:RecallRequestNamespace,current:MySqlDbFirstRecallRequestRecord):Option[S2CDurableRecallStatus] =
    requireResultSource(server,callbacks.adapter)
    require(namespace!=null && current!=null,"authenticated namespace and current original record required")
    val binding=callbacks.recoveryReadData.binding.get
    if namespace.bindingUuid!=binding.bindingUuid then None
    else requestCustody.originalRequests.find(record=>record.originalTag==current.originalTag &&
        record.bindingUuid==namespace.bindingUuid && record.actorUuid==namespace.actorUuid).flatMap { record=>
      val outcome=requestCustody.resultFor(record).getOrElse(throw new IllegalStateException("original Boot result custody missing"))
      require(DbFirstBootRecallRequestCustody.matchesResult(record,current,outcome._1,outcome._2),
        "current durable record differs from recovered original result")
      // 这里只回复原历史结果；当前Pod可能已被后续合法操作重新激活，不回放旧pose/version同步。
      record.clientRequestUuid.map { request=>
        val (phase,reason)=outcome._1 match
          case MySqlDbFirstRecallRequestPhase.RecallCommitted=>(DurableRecallPhase.Committed,None)
          case MySqlDbFirstRecallRequestPhase.RefusedCommitted=>
            (DurableRecallPhase.Refused,Some(DbFirstPlayerRecallPolicy.networkReason(outcome._2.get)))
          case _=>throw new IllegalStateException("actual completed Boot result cohort changed phase")
        new S2CDurableRecallStatus(namespace,request,record.podUuid,record.expectedStateVersion,
          Some(record.originalTag.operationId),phase,reason)
      }
    }

  /** 实际恢复后预装内部scope；不装endpoint、不发行Ready，也不执行原J/C。 */
  private[floatingpod] def prepareInternalPublication(original:ProductionDbFirstBootstrapCallbacks,
      pump:ForgePermanentMainPump,lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    owner()
    require((original eq callbacks) && pump!=null && lease!=null && installed && started && finished &&
      !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "original completed Boot Pod recovery required before internal publication")
    pump.assertRecoveringDispatch(lease,tick)
    requireCompletedPodRestoration(pump,lease)
    val identity=callbacks.adapter.identity
    require(lease.bootUuid==identity.bootId && lease.bootGeneration==identity.bootGeneration,
      "original Boot publication pump identity required")
    if publicationInstallationAttempted then
      require(internalPublication.nonEmpty && publicationPump.exists(_ eq pump) && publicationLease.exists(_ eq lease),
        "same successfully installed Boot publication required; no retry after partial installation")
    else
      publicationInstallationAttempted=true
      publicationPump=Some(pump);publicationLease=Some(lease)
      val binding=callbacks.recoveryReadData.binding.getOrElse(throw new IllegalStateException("actual Boot publication binding missing"))
      internalPublication=Some(manager.installMainPublication(identity,lease.bootGeneration,binding,callbacks.activeLimit))

  /** 正式endpoint只能领原scope；原settled gate保留，不再次安装。 */
  private[server] def publicationForEndpoint(original:ProductionDbFirstBootstrapCallbacks,
      pump:ForgePermanentMainPump,lease:PermanentPumpLease):ActiveFloatingPodManager#MainPublicationScope =
    requirePublicationOwner(original)
    require(!publicationFailureStopped && publicationPump.exists(_ eq pump) && publicationLease.exists(_ eq lease),
      "endpoint requires exact preinstalled Boot pump/lease")
    val scope=internalPublication.getOrElse(throw new IllegalStateException("actual Boot publication not installed"))
    manager.requireDbFirstPublication(callbacks,scope)
    callbacks.requireMainPublication(scope.identity,scope.binding,scope.activeLimit)
    scope

  /** endpoint尚未存在时封停同一scope；保留原图及cause，不伪造final-source或退款。 */
  private[floatingpod] def stopInternalPublication(original:ProductionDbFirstBootstrapCallbacks,cause:Throwable):Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread &&
      (original eq callbacks) && cause!=null,"actual original Boot publication failure required")
    callbacks.requireOriginalMainAdapter(callbacks.adapter)
    if publicationInstallationAttempted && !failurePublicationStopAttempted then
      publicationFailureStopped=true
      failurePublicationStopAttempted=true
      manager.stopDbFirstBootPublication(callbacks,this,cause)

  /** 先封住Boot恢复/发布入口并保留原cause；不提前quiesce，失败停止需先撤实际Main借用。 */
  private[floatingpod] def beginFailedStop(cause:Throwable):Unit =
    cleanupOwner()
    require(cause!=null,"actual original Boot failure cause required")
    if failureStopCause==null then failureStopCause=cause
    else if (failureStopCause ne cause) && !failureStopCause.getSuppressed.contains(cause) then
      failureStopCause.addSuppressed(cause)
    publicationFailureStopped=true

  /** 失败停服只允许撤已完全静止的Boot请求图；不制造settled、ACK或成功final-release状态。 */
  private[floatingpod] def detachStoppedBootCensusForFailure():Unit =
    cleanupOwner()
    require(failureStopCause!=null && publicationFailureStopped && !stoppedCensusDetachAttempted &&
      !censusDetached,"same actual failed Boot stop and undropped census required")
    // install失败发生在claim之前时Session仍保管未claim census，worker可按原未claim规则释放。
    if originalRecallRequests.isEmpty then return
    val positiveIsQuiescent=positiveRecall.forall(positiveRecallIsStoppedEmpty)
    require((!started || finished) && (!initialTerminalHandoffAttempted || initialTerminalHandoffCompleted) &&
      journalTakeover.isEmpty && refusalFinal.isEmpty && positiveIsQuiescent &&
      detachingTakeover==null && completingRefusal==null && restoringTerminal==null &&
      issuingCapture==null && captureStage==0 && detachingPositive==null && completingCommitted==null &&
      completingRetainedCommitted==null && positiveDispatch==null && positiveTick==null &&
      issuingMainDecision==null && mainDecisionStage==0 && observedMainReason.isEmpty &&
      issuingTakeover==null && unmatchedBootCompletions.isEmpty,
      "actual stopped Boot census cannot detach while SQL/native or partial Main responsibility remains")
    stoppedCensusDetachAttempted=true
    stoppedCensusDetachDispatch=true
    val custody=requestCustody
    try
      custody.detachForStoppedCensus(this)
      // 先撤除付费请求向量的全部Main引用，再交付实际census。
      originalRecallRequests=None
      resultPass=None;podRestoration=None;internalPublication=None;publicationPump=None;publicationLease=None
      positiveRecall=None;refusalFinal=None;journalTakeover=None
      callbacks.detachStoppedBootRecallCensus(this)
      censusDetached=true
    catch case cause:Throwable=>
      if failureStopCause ne cause then
        if !failureStopCause.getSuppressed.contains(cause) then failureStopCause.addSuppressed(cause)
      throw cause
    finally stoppedCensusDetachDispatch=false

  /** 只认可原C最终ACK后真实空Entry的停止图；服务发布/恢复ACK与请求结算不是空图证明。 */
  private def positiveRecallIsStoppedEmpty(state:PositiveRecall):Boolean =
    state!=null && state.stage==PositiveStage.RetainedStopped && state.graphDetached && state.detachedEnd>0L &&
      !state.awaiting && state.completion.isEmpty && !state.retainedEnd.active &&
      !state.retainedEnd.awaitingAck && state.cause.nonEmpty && state.scope.dbFirstRecall(state.tag).isEmpty

  /** 本地仅跳过早于Main撤借用的冻结；Adapter稍后还须核真实census、DATA、IO与SQL空图。 */
  private def stoppedPositiveIsOnlyBootResponsibility:Boolean =
    positiveRecall.exists(positiveRecallIsStoppedEmpty) && (!started || finished) &&
      (!initialTerminalHandoffAttempted || initialTerminalHandoffCompleted) &&
      journalTakeover.isEmpty && refusalFinal.isEmpty && detachingTakeover==null && completingRefusal==null &&
      restoringTerminal==null && issuingCapture==null && captureStage==0 && detachingPositive==null &&
      completingCommitted==null && completingRetainedCommitted==null && positiveDispatch==null && positiveTick==null &&
      issuingMainDecision==null && mainDecisionStage==0 && observedMainReason.isEmpty && issuingTakeover==null &&
      unmatchedBootCompletions.isEmpty

  /** Adapter在真实Main detach栈内调用；只接受本Recovery已撤除全部付费Main别名的一次性dispatch。 */
  private[floatingpod] def requireStoppedBootCensusDetached(adapter:MySqlOwnedIoAdapter):Unit =
    cleanupOwner()
    require((adapter eq callbacks.adapter) && failureStopCause!=null && stoppedCensusDetachDispatch &&
      stoppedCensusDetachAttempted && !censusDetached && originalRecallRequests.isEmpty && resultPass.isEmpty &&
      podRestoration.isEmpty && internalPublication.isEmpty && publicationPump.isEmpty && publicationLease.isEmpty &&
      journalTakeover.isEmpty && refusalFinal.isEmpty && positiveRecall.isEmpty,
      "actual original Boot Main references must be detached before census retirement")

  /** Census本身在同一Main dispatch内退休；它仍要求所有takeover已真实detach。 */
  private[floatingpod] def requireStoppedBootCensusDetachedForRetention():Unit =
    cleanupOwner()
    require(failureStopCause!=null && stoppedCensusDetachDispatch && stoppedCensusDetachAttempted &&
      !censusDetached && originalRecallRequests.isEmpty && resultPass.isEmpty && podRestoration.isEmpty &&
      internalPublication.isEmpty && publicationPump.isEmpty && publicationLease.isEmpty &&
      journalTakeover.isEmpty && refusalFinal.isEmpty && positiveRecall.isEmpty,
      "stopped Boot Main census references must end before worker fee retirement")

  private[server] def requireBootPublicationFailure(actual:ActiveFloatingPodManager,
      original:ProductionDbFirstBootstrapCallbacks):Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread && (actual eq manager) &&
      (original eq callbacks) && publicationInstallationAttempted && publicationFailureStopped && !finalMainReleased,
      "same actual Boot publication failure owner required")

  private def requireCaptureIdentity(scope:ActiveFloatingPodManager#MainPublicationScope,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,stage:Int):Unit =
    cleanupOwner()
    require(issuingCapture!=null && captureStage==stage && journalTakeover.exists(_ eq issuingCapture) &&
      (issuingCapture.intent eq intent) && issuingCapture.reservation.exists(_ eq reservation) &&
      issuingCapture.mainDecision.exists(_.refusalReason.isEmpty) && positiveRecall.exists(state=>
        (state.scope eq scope) && (state.original eq intent.original) && (positiveDispatch eq state)) &&
      positiveTick!=null && !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "actual private positive Boot capture invocation required")
  private[server] def requireBootCaptureInvocation(scope:ActiveFloatingPodManager#MainPublicationScope,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,stage:Int):Unit =
    requireCaptureIdentity(scope,intent,reservation,stage)
    owner()
    require(internalPublication.exists(_ eq scope),"same actual Boot capture publication required")
  /** core锁内只核私有同步身份，不回借DATA或世界。 */
  private[floatingpod] def requireActivatingBootCapture(decision:MySqlDbFirstBootRecallMainDecision,
      reservation:MySqlDbFirstCaptureReservation):Unit =
    val state=positiveRecall.getOrElse(throw new IllegalArgumentException("actual Boot positive owner missing"))
    require(decision!=null,"actual positive Boot decision required")
    requireCaptureIdentity(state.scope,decision.intent,reservation,2)
    require(issuingCapture.mainDecision.exists(_ eq decision) && decision.refusalReason.isEmpty,
      "same registered positive Main decision required for activation")
  private[server] def registerBootRecallEntry(scope:ActiveFloatingPodManager#MainPublicationScope,
      reservation:MySqlDbFirstCaptureReservation):Unit =
    val retained=issuingCapture
    require(retained!=null,"actual Boot capture invocation missing before Main entry")
    requireCaptureIdentity(scope,retained.intent,reservation,3)
    val state=positiveRecall.get
    require(!state.entryRegistered && scope.dbFirstRecall(state.tag).nonEmpty,"one actual registered Boot manager Entry required")
    state.entryRegistered=true
  /** 实体factory回核真实manager已保管的同一body/control；持久actor不冒充玩家认证。 */
  private[floatingpod] def requireBootEntityFreeze(entity:FloatingPodEntity,operation:UUID,version:Long,
      control:FloatingPodControlSnapshot):Unit =
    val state=positiveRecall.getOrElse(throw new IllegalArgumentException("actual Boot entity freeze owner missing"))
    require(issuingCapture!=null,"actual Boot freeze invocation missing")
    requireCaptureIdentity(state.scope,issuingCapture.intent,issuingCapture.reservation.get,3)
    require(state.entryRegistered && entity!=null && control!=null && operation==state.tag.operationId &&
      version==state.original.expectedStateVersion,"actual Boot entry must precede one entity freeze")
    state.scope.requireBootRecallFreeze(this,entity,state.tag,version,control)

  private[server] def requireBootPositiveEnd(scope:ActiveFloatingPodManager#MainPublicationScope,
      tag:IoOperationTag,tick:PermanentEndTick):Unit =
    cleanupOwner()
    val state=positiveRecall.getOrElse(throw new IllegalArgumentException("actual Boot positive END owner missing"))
    require((positiveDispatch eq state) && (positiveTick eq tick) && tick!=null &&
      tick.phase==PermanentPumpPhase.Recovering && (state.scope eq scope) && state.tag==tag &&
      internalPublication.exists(_ eq scope) && !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "same actual Boot positive Recovering dispatch required")
  private[server] def requireBootPositiveRelease(scope:ActiveFloatingPodManager#MainPublicationScope,
      tag:IoOperationTag,tick:PermanentEndTick):Unit =
    requireBootPositiveEnd(scope,tag,tick)
    val state=positiveRecall.get
    require(state.graphDetached && journalTakeover.isEmpty && tick.endOrdinal>state.detachedEnd,
      "Boot Main/census source graph and previous END stack must end before final worker")

  private def detachPositiveGraph(state:PositiveRecall,tick:PermanentEndTick):Unit =
    val retained=journalTakeover.getOrElse(throw new IllegalStateException("original positive Boot takeover missing before detach"))
    state.scope.requireBootRecallMainDetached(this,state.tag)
    state.detachedEnd=tick.endOrdinal
    detachingPositive=retained
    try callbacks.adapter.detachBootRecallTakeover(this,retained.intent,retained.reservation.get)
    finally detachingPositive=null
    journalTakeover=None
    state.graphDetached=true

  private def drainPositive(state:PositiveRecall):Unit =
    if state.awaiting && state.completion.isEmpty then
      val batch=callbacks.adapter.drain(1)
      batch.foreach { actual=>
        if actual.tag!=state.tag then
          unmatchedBootCompletions=batch
          throw new IllegalStateException("Boot positive received unrelated completion; actual batch retained")
        state.completion=Some(actual)
      }
  private def submitPositiveRelease(state:PositiveRecall,tick:PermanentEndTick,retained:Boolean):IoSubmission =
    require(!state.awaiting && state.completion.isEmpty,"one original Boot C retirement completion at a time")
    val entry=state.scope.dbFirstRecall(state.tag).getOrElse(throw new IllegalStateException("original Boot release Main Entry missing"))
    state.awaiting=true // throw/UNKNOWN保原等待；明确未接收仅重交同一metadata receipt。
    val result=if retained then state.scope.submitRetainedDbFirstRecallRelease(callbacks.adapter,entry,tick)
      else state.scope.submitBootDbFirstRecallRelease(callbacks.adapter,entry,tick)
    if result!=IoSubmission.Accepted then state.awaiting=false
    result

  /** 先于任何当前DATA admission；正常C最终ACK同时核旧DATA退休，retained不恢复ordinary。 */
  private[floatingpod] def advancePendingPositive(pump:ForgePermanentMainPump,lease:PermanentPumpLease,
      tick:PermanentEndTick):Boolean =
    cleanupOwner();pump.assertRecoveringDispatch(lease,tick)
    val pending=positiveRecall.nonEmpty || journalTakeover.exists(_.mainDecision.exists(_.refusalReason.isEmpty))
    if !pending then return false
    require(installed && started && finished && !resultsSealed && !publicationFailureStopped && !finalMainReleased &&
      publicationPump.exists(_ eq pump) && publicationLease.exists(_ eq lease) && tick.endOrdinal>lastPositiveEnd,
      "same monotonic actual Boot positive dispatch required")
    lastPositiveEnd=tick.endOrdinal
    if positiveRecall.isEmpty then
      val original=journalTakeover.get
      positiveRecall=Some(new PositiveRecall(original.intent.original,original.intent.executionTag,internalPublication.get))
    val state=positiveRecall.get
    positiveDispatch=state;positiveTick=tick
    try
      if !state.entryRegistered then
        owner();requireCompletedPodRestoration(pump,lease)
        val retained=journalTakeover.get
        retained.controlCompletion.requireSuccessfulWorkerResult()
        require(issuingCapture==null && captureStage==0 && !state.awaiting,"Boot positive capture is one-shot")
        issuingCapture=retained;captureStage=1
        try
          state.scope.preflightBootRecallCapture(this,callbacks,callbacks.adapter,retained.intent,retained.reservation.get)
          captureStage=2
          callbacks.adapter.activateBootRecallCapture(this,retained.mainDecision.get,retained.reservation.get)
          captureStage=3
          state.scope.beginBootDbFirstRecall(this,callbacks,callbacks.adapter,retained.intent,retained.reservation.get)
          require(state.entryRegistered,"actual manager Entry must be retained before capture submission")
          state.awaiting=true
          val entry=state.scope.dbFirstRecall(state.tag).get
          requireAcceptedRefusal(callbacks.adapter.submit(state.tag,MySqlOwnedIoCommand.ExecuteDbFirstRecall(entry.reservation)),
            "Boot original positive C")
        finally { issuingCapture=null;captureStage=0 }
      else state.stage match
        case PositiveStage.Sql=>
          drainPositive(state)
          state.completion.foreach { actual=>
            actual.result.failed.foreach(throw _)
            val entry=state.scope.dbFirstRecall(state.tag).get
            val retained=DbFirstRecallCompletionHandoff.accept(state.scope.completeDbFirstRecall(callbacks.adapter,entry,actual)) {
              state.completion=None;state.awaiting=false
            }
            retained.foreach { cause=>
              state.cause=Some(cause);state.retainedEnd.accepted(tick.endOrdinal)
            }
            state.stage=PositiveStage.Physical
          }
        case PositiveStage.Physical=>
          val entry=state.scope.dbFirstRecall(state.tag).get
          if state.retainedEnd.active then
            state.retainedEnd.advance(tick.endOrdinal) {
              state.scope.advanceRetainedDbFirstRecall(callbacks.adapter,entry,tick).nonEmpty
            } {
              state.scope.handoffRetainedDbFirstRecall(callbacks.adapter,entry)
              detachPositiveGraph(state,tick)
            } {
              submitPositiveRelease(state,tick,true)
            }
            if state.retainedEnd.awaitingAck then state.stage=PositiveStage.FinalAck
          else
            state.cause.foreach(throw _)
            if state.scope.prepareBootDbFirstRecallDrop(callbacks.adapter,entry,tick).nonEmpty then
              detachPositiveGraph(state,tick)
              state.stage=PositiveStage.Detached
        case PositiveStage.Detached=>
          submitPositiveRelease(state,tick,false) match
            case IoSubmission.Accepted=>state.stage=PositiveStage.FinalAck
            case IoSubmission.RejectedCapacity=>()
            case other=>requireAcceptedRefusal(other,"Boot original C combined final release")
        case PositiveStage.FinalAck=>
          drainPositive(state)
          state.completion.foreach { actual=>
            actual.result.failed.foreach(throw _)
            val entry=state.scope.dbFirstRecall(state.tag).get
            if state.retainedEnd.awaitingAck then
              state.retainedEnd.finish(state.scope.finishRetainedDbFirstRecallRelease(callbacks.adapter,entry,actual))
              state.completion=None;state.awaiting=false
              state.stage=PositiveStage.RetainedStopped // 原shared fault/两份DATA保管，不settle/发下一J/Ready。
            else
              state.scope.finishDbFirstRecallRelease(callbacks.adapter,entry,actual)
              state.acknowledged=true;state.completion=None;state.awaiting=false
              completingCommitted=state
              try requestCustody.completeCommitted(state.original,this)
              finally completingCommitted=null
              positiveRecall=None
          }
        case PositiveStage.RetainedStopped=>
          if !state.serviceMainAcknowledged then
            state.serviceMainAcknowledged=callbacks.adapter.advanceRetainedRecallServiceMain(this,state.tag)
          if state.serviceMainAcknowledged && !state.servicePublicationAcknowledged then
            state.servicePublicationAcknowledged=callbacks.adapter.advanceRecoveredServicePublication(this,state.tag,state.scope)
          if state.servicePublicationAcknowledged && !state.serviceExecutionAcknowledged then
            state.serviceExecutionAcknowledged=callbacks.adapter.finishRecoveredServiceExecution(this,state.tag,state.scope,state.original)
            if state.serviceExecutionAcknowledged then positiveRecall=None
          // 原C严格结算、同服务save与业务admission全部发布后，才撤当前责任并在后续END处理下一J。
      true
    catch case cause:Throwable=>
      state.retainedEnd.stop()
      state.cause match
        case Some(original)=>if original ne cause then original.addSuppressed(cause)
        case None=>state.cause=Some(cause)
      throw cause
    finally { positiveDispatch=null;positiveTick=null }

  private[floatingpod] def requireRetainedServiceMainDispatch(actualAdapter:MySqlOwnedIoAdapter,
      actualCallbacks:ProductionDbFirstBootstrapCallbacks,tag:IoOperationTag,cause:Throwable):Unit =
    cleanupOwner()
    val state=positiveDispatch
    require(state!=null && positiveRecall.exists(_ eq state) && positiveTick!=null &&
      (actualAdapter eq callbacks.adapter) && (actualCallbacks eq callbacks) && state.tag==tag &&
      state.stage==PositiveStage.RetainedStopped && state.graphDetached && state.detachedEnd>0L &&
      !state.awaiting && state.completion.isEmpty && !state.acknowledged &&
      state.cause.exists(_ eq cause) && state.scope.dbFirstRecall(tag).isEmpty,
      "actual original monotonic Boot retained dispatch/native graph detach/final ACK required")

  private[server] def requireCompletingBootCommitted(actual:DbFirstBootRecallRequestCustody,
      original:MySqlDbFirstRecallRequestRecord):Unit =
    owner()
    val state=completingCommitted
    require(state!=null && positiveRecall.exists(_ eq state) && state.acknowledged && state.cause.isEmpty &&
      !state.awaiting && state.completion.isEmpty && state.graphDetached && journalTakeover.isEmpty &&
      (actual eq requestCustody) && (state.original eq original) && original.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted &&
      original.refusalReason.isEmpty && !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "same actual Boot normal C combined final ACK required for original Committed settlement")
    val data=callbacks.recoveryReadData
    require(data.binding.exists(_.bindingUuid==original.bindingUuid) && data.sourceProtection.unresolved.isEmpty &&
      data.recallRequestCensus.exists(current=>DbFirstBootRecallRequestCustody.matchesResult(original,current,
        MySqlDbFirstRecallRequestPhase.RecallCommitted,None)),"actual current strict original committed result required")

  private[floatingpod] def completeRetainedServiceCommitted(adapter:MySqlOwnedIoAdapter,
      original:MySqlDbFirstRecallRequestRecord):Unit =
    cleanupOwner()
    val state=positiveDispatch
    require(state!=null && positiveRecall.exists(_ eq state) && (adapter eq callbacks.adapter) &&
      (state.original eq original) && state.serviceMainAcknowledged && state.servicePublicationAcknowledged &&
      !state.serviceExecutionAcknowledged && completingRetainedCommitted==null,
      "actual original retained Boot service completion dispatch required")
    adapter.requireRecoveredOriginalCommit(this,original)
    completingRetainedCommitted=state
    try requestCustody.completeRetainedCommitted(original,this)
    finally completingRetainedCommitted=null

  private[server] def requireCompletingRetainedCommitted(actual:DbFirstBootRecallRequestCustody,
      original:MySqlDbFirstRecallRequestRecord):Unit =
    cleanupOwner()
    val state=completingRetainedCommitted
    require(state!=null && (state eq positiveDispatch) && positiveRecall.exists(_ eq state) &&
      (actual eq requestCustody) && (state.original eq original) && state.cause.nonEmpty &&
      !state.acknowledged && state.serviceMainAcknowledged && state.servicePublicationAcknowledged &&
      state.graphDetached && state.detachedEnd>0L && !state.awaiting && state.completion.isEmpty &&
      state.stage==PositiveStage.RetainedStopped && journalTakeover.isEmpty &&
      original.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted && original.refusalReason.isEmpty &&
      !resultsSealed && !publicationFailureStopped && !finalMainReleased,
      "actual retained final ACK/service restoration and original failed C history required")
    callbacks.adapter.requireRecoveredOriginalCommit(this,original)

  private def requireAcceptedRefusal(result:IoSubmission,operation:String):Unit = result match
    case IoSubmission.Accepted=>()
    case IoSubmission.RejectedStopped(Some(cause))=>throw cause
    case other=>throw new IllegalStateException(operation+" not accepted; original responsibility retained: "+other)

  private def drainRefusal(completion:DbFirstRecallJournalCompletion):Unit =
    if completion.expectedTag.nonEmpty && completion.current.isEmpty then
      val batch=callbacks.adapter.drain(1)
      batch.foreach { actual=>
        if !completion.expectedTag.contains(actual.tag) then
          unmatchedBootCompletions=batch
          throw new IllegalStateException("Boot refusal received unrelated completion; actual batch retained")
        completion.retain(actual)
      }

  /** 先于任何DATA admission；accepted mutation会使旧读代失效，清理仍须原Main/adapter/pump。
    * true表示本END用于在途原责任，调用方不得在提交同栈借回旧DATA或启动下一J。
    */
  private[floatingpod] def advancePendingRefusal(pump:ForgePermanentMainPump,lease:PermanentPumpLease,
      tick:PermanentEndTick):Boolean =
    cleanupOwner();pump.assertRecoveringDispatch(lease,tick)
    require(installed && !resultsSealed && !finalMainReleased && !publicationFailureStopped,
      "same live Boot refusal cleanup dispatch required")
    val pending=refusalFinal.nonEmpty || journalTakeover.exists(_.mainDecision.exists(_.refusalReason.nonEmpty))
    if !pending then return false
    require(publicationPump.exists(_ eq pump) && publicationLease.exists(_ eq lease),
      "same preinstalled Boot refusal pump/lease required")
    require(tick.endOrdinal>lastRefusalEnd,"strictly later actual Boot refusal END required")
    lastRefusalEnd=tick.endOrdinal
    try
      refusalFinal match
        case Some(finalState)=>
          finalState.completion.requireHealthy()
          require(tick.endOrdinal>finalState.detachedEnd,"Boot source detach stack must return before final worker retirement")
          if !finalState.submitted then
            finalState.submitted=true
            finalState.completion.beginSubmission()
            requireAcceptedRefusal(callbacks.adapter.submitRefusalFinalRelease(finalState.receipt),"Boot refusal final retirement")
          else
            drainRefusal(finalState.completion)
            finalState.completion.current.foreach { actual=>
              finalState.completion.requireSuccessfulWorkerResult()
              callbacks.adapter.finishRefusalFinalRelease(finalState.receipt,actual)
              finalState.acknowledged=true // 只在实际exact final ACK成功后登记。
              finalState.completion.consumed()
              completingRefusal=finalState
              try requestCustody.completeRefusal(finalState.original,finalState.reason,this)
              finally completingRefusal=null
              refusalFinal=None // custody已接结果才撤最后receipt；下一END才可接下一J。
            }
        case None=>
          val retained=journalTakeover.get
          retained.refusalCompletion.requireHealthy()
          retained.refusalStage match
            case RefusalStage.New=>
              owner();requireCompletedPodRestoration(pump,lease)
              retained.controlCompletion.requireSuccessfulWorkerResult()
              val source=retained.mainDecision.get
              require(source.refusalReason.nonEmpty && retained.reservation.nonEmpty,"same actual Boot refused decision required")
              retained.refusalStage=RefusalStage.Sql
              retained.refusalCompletion.beginSubmission() // UNKNOWN/throw后不重投，不换C。
              requireAcceptedRefusal(callbacks.adapter.submit(source.routeTag,MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(source)),
                "Boot original C refusal")
            case RefusalStage.Sql=>
              drainRefusal(retained.refusalCompletion)
              retained.refusalCompletion.current.foreach { actual=>
                retained.refusalCompletion.requireSuccessfulWorkerResult()
                retained.refusalConfirmation=Some(callbacks.adapter.claimRecallRefusalConfirmation(retained.mainDecision.get,actual))
                retained.refusalCompletion.consumed()
                retained.refusalStage=RefusalStage.SubmitRead
              }
            case RefusalStage.SubmitRead=>
              retained.refusalCompletion.beginSubmission()
              requireAcceptedRefusal(callbacks.adapter.submitRefusalMainReadRelease(retained.mainDecision.get,
                retained.refusalConfirmation.get),"Boot refusal actual successor/read handoff")
              retained.refusalStage=RefusalStage.ReadAck
            case RefusalStage.ReadAck=>
              drainRefusal(retained.refusalCompletion)
              retained.refusalCompletion.current.foreach { actual=>
                retained.refusalCompletion.requireSuccessfulWorkerResult()
                callbacks.adapter.finishRefusalMainReadRelease(retained.mainDecision.get,retained.refusalConfirmation.get,actual)
                retained.refusalCompletion.consumed()
                retained.refusalStage=RefusalStage.Detach
              }
            case RefusalStage.Detach=>
              owner() // 只有真实read ACK完成后，此处才允许当前DATA门禁。
              val source=retained.mainDecision.get
              val receipt=callbacks.adapter.recordRefusalFinal(source,retained.refusalConfirmation.get)
              refusalFinal=Some(new RefusalFinal(retained.intent.original,source.refusalReason.get,receipt,tick.endOrdinal))
              detachingTakeover=retained
              try callbacks.adapter.detachBootRecallTakeover(this,retained.intent,retained.reservation.get)
              finally detachingTakeover=null
              journalTakeover=None // 整个control envelope/source/intent/reservation/confirmation图撤下。
      true
    catch case cause:Throwable=>
      journalTakeover.foreach { retained=>retained.cause=Some(cause);retained.refusalCompletion.fail(cause) }
      refusalFinal.foreach(_.completion.fail(cause))
      throw cause

  /** 两种source各核真实私有撤图期；positive不能借已失效的旧DATA进入清理。 */
  private[floatingpod] def requireDetachingBootTakeover(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit =
    cleanupOwner()
    if detachingPositive!=null then
      val state=positiveRecall.getOrElse(throw new IllegalStateException("actual positive Boot detach owner missing"))
      require(journalTakeover.exists(_ eq detachingPositive) && (detachingPositive.intent eq intent) &&
        detachingPositive.reservation.exists(_ eq reservation) && (state.original eq intent.original) &&
        (positiveDispatch eq state) && positiveTick!=null && !state.graphDetached,
        "actual private Boot positive post-native detach invocation required")
      state.scope.requireBootRecallMainDetached(this,state.tag)
    else
      owner()
      require(detachingTakeover!=null && journalTakeover.exists(_ eq detachingTakeover) &&
        (detachingTakeover.intent eq intent) && detachingTakeover.reservation.exists(_ eq reservation) &&
        detachingTakeover.refusalStage==RefusalStage.Detach && refusalFinal.exists(_.original eq intent.original),
        "actual private Boot post-read-ACK takeover detach invocation required")

  /** 逐请求结果不替换初始census，也不放宽原TerminalRecoveryPass。 */
  private[server] def requireCompletingBootRefusal(actual:DbFirstBootRecallRequestCustody,
      original:MySqlDbFirstRecallRequestRecord,reason:RecallRefusalReason):Unit =
    owner()
    val finalState=completingRefusal
    require(finalState!=null && refusalFinal.exists(_ eq finalState) && finalState.acknowledged &&
      finalState.completion.current.isEmpty && (actual eq requestCustody) && (finalState.original eq original) &&
      finalState.reason==reason && original.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted &&
      original.refusalReason.isEmpty && !resultsSealed && !finalMainReleased && !publicationFailureStopped,
      "same actual Boot final ACK and original immutable request required for settlement")
    val receipt=finalState.receipt
    require(receipt.originalTag==original.originalTag && receipt.bindingUuid==original.bindingUuid &&
      receipt.actorUuid==original.actorUuid && receipt.podUuid==original.podUuid &&
      receipt.clientRequestUuid==original.clientRequestUuid,"actual final receipt must retain full original identity")
    val data=callbacks.recoveryReadData
    require(data.binding.exists(_.bindingUuid==original.bindingUuid) && data.sourceProtection.unresolved.isEmpty &&
      data.recallRequestCensus.exists(current=>DbFirstBootRecallRequestCustody.matchesResult(original,current,
        MySqlDbFirstRecallRequestPhase.RefusedCommitted,Some(reason))),
      "actual current strict successor result must match final ACK original request")

  /** 只登记一次原J-only接管；控制UUID不替换原C UUID，完成仍为inactive、未settled。 */
  private[floatingpod] def advanceJournalTakeover(pump:ForgePermanentMainPump,lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    owner();pump.assertRecoveringDispatch(lease,tick)
    require(installed && !resultsSealed && !finalMainReleased && !publicationFailureStopped &&
      callbacks.recoveryReadData.sourceProtection.unresolved.isEmpty,"actual Boot original takeover admission required")
    val identity=callbacks.adapter.currentExecutionIdentity
    require(lease.bootUuid==identity.bootId && lease.bootGeneration==identity.bootGeneration,"same actual Boot takeover pump required")
    journalTakeover.foreach { retained=>
      retained.controlCompletion.requireHealthy()
      if retained.submission.contains(IoSubmission.Accepted) && retained.controlCompletion.current.isEmpty then
        val batch=callbacks.adapter.drain(1)
        batch.foreach { actual=>
          if actual.tag!=retained.intent.controlTag then
            unmatchedBootCompletions=batch
            throw new IllegalStateException("Boot takeover received unrelated original completion; batch retained")
          retained.controlCompletion.retain(actual) // 先保真实envelope；control slot空不表示C责任消失。
          try
            retained.controlCompletion.requireSuccessfulWorkerResult()
            retained.reservation=Some(callbacks.adapter.claimBootRecallTakeover(this,retained.intent,actual))
          catch case cause:Throwable=>
            retained.cause=Some(cause)
            retained.controlCompletion.fail(cause)
            throw cause
        }
    }
    // 先交付一个实际inactive Entry；后续capture/settlement接线前不扩散未激活责任。
    if journalTakeover.isEmpty && refusalFinal.isEmpty && positiveRecall.isEmpty then
      require(!requestCustody.originalRequests.exists(_.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted) ||
        initialTerminalHandoffCompleted,"all initial terminal results must hand off before first original J")
      requestCustody.originalRequests.find(record=>record.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted && !requestCustody.isSettled(record)).foreach { record=>
        val execution=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,record.originalTag.operationId)
        val control=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,UUID.randomUUID())
        issuingTakeover=record;issuingControl=control;issuingExecution=execution
        val intent=try callbacks.adapter.beginBootRecallTakeover(this,record,control,execution)
          finally { issuingTakeover=null;issuingControl=null;issuingExecution=null }
        val retained=new JournalTakeover(intent)
        journalTakeover=Some(retained)
        retained.submissionAttempted=true
        retained.controlCompletion.beginSubmission()
        try
          val result=callbacks.adapter.submit(control,MySqlOwnedIoCommand.TakeoverBootRecall(intent))
          retained.submission=Some(result)
          require(result==IoSubmission.Accepted,"original Boot takeover control capacity rejected; no replacement or retry")
        catch case cause:Throwable=>
            retained.cause=Some(cause)
            retained.controlCompletion.fail(cause)
            throw cause
      }
    journalTakeover.foreach { retained=>
      if retained.reservation.nonEmpty && !retained.mainDecisionAttempted then
        advanceMainDecision(retained,pump,lease,tick)
    }

  private def advanceMainDecision(retained:JournalTakeover,pump:ForgePermanentMainPump,
      lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    owner();pump.assertRecoveringDispatch(lease,tick)
    requireCompletedPodRestoration(pump,lease)
    require(journalTakeover.exists(_ eq retained) && retained.reservation.nonEmpty && !retained.mainDecisionAttempted &&
      retained.mainDecision.isEmpty && issuingMainDecision==null,"same original Boot inactive Main decision required once")
    retained.controlCompletion.requireHealthy();retained.controlCompletion.requireSuccessfulWorkerResult()
    failed.get(retained.intent.original.podUuid).foreach(value=>throw value.cause)
    val scope=internalPublication.getOrElse(throw new IllegalStateException("actual Boot internal publication missing"))
    retained.mainDecisionAttempted=true
    issuingMainDecision=retained;mainDecisionStage=1
    try
      callbacks.adapter.beginBootRecallMainDecision(this,retained.intent,retained.reservation.get)
      mainDecisionStage=2
      val reason=scope.observeBootRecallDecision(this,callbacks,callbacks.adapter,retained.intent,
        retained.reservation.get,pump,lease,tick)
      observedMainReason=Some(reason);mainDecisionStage=3
      retained.mainDecision=Some(callbacks.adapter.finishBootRecallMainDecision(this,retained.intent,retained.reservation.get,reason))
    catch case cause:Throwable=>
      retained.cause=Some(cause);retained.controlCompletion.fail(cause)
      throw cause
    finally
      issuingMainDecision=null;mainDecisionStage=0;observedMainReason=None

  /** 仅核本issuer私有同步调用期；不回借DATA，避免在core锁内切入世界或SQL。 */
  private def requireMainDecisionInvocation(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation,stage:Int):Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread &&
      !resultsSealed && !finalMainReleased && !publicationFailureStopped && mainDecisionStage==stage &&
      issuingMainDecision!=null && journalTakeover.exists(_ eq issuingMainDecision) &&
      (issuingMainDecision.intent eq intent) && issuingMainDecision.reservation.exists(_ eq reservation) &&
      issuingMainDecision.mainDecisionAttempted && issuingMainDecision.mainDecision.isEmpty,
      "actual original Boot private Main decision invocation required")
  private[floatingpod] def requireRegisteringBootMainDecision(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit =
    requireMainDecisionInvocation(intent,reservation,1)
  private[floatingpod] def requireCompletingBootMainDecision(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation,reason:Option[RecallRefusalReason]):Unit =
    requireMainDecisionInvocation(intent,reservation,3)
    require(observedMainReason.contains(reason),"same actual Main observation reason required")
  private[server] def requireBootDecisionObservation(scope:ActiveFloatingPodManager#MainPublicationScope,
      original:ProductionDbFirstBootstrapCallbacks,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation,pump:ForgePermanentMainPump,lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    owner();pump.assertRecoveringDispatch(lease,tick)
    requireMainDecisionInvocation(intent,reservation,2)
    requireCompletedPodRestoration(pump,lease)
    require((original eq callbacks) && internalPublication.exists(_ eq scope) &&
      requestCustody.originalRequests.exists(_ eq intent.original),"same original Boot internal decision scope required")
    failed.get(intent.original.podUuid).foreach(value=>throw value.cause)

  private[floatingpod] def requireIssuingBootTakeover(record:MySqlDbFirstRecallRequestRecord,
      control:IoOperationTag,execution:IoOperationTag):Unit =
    owner()
    require(installed && !resultsSealed && !finalMainReleased && journalTakeover.isEmpty &&
      (issuingTakeover eq record) && (issuingControl eq control) && (issuingExecution eq execution) &&
      record!=null && record.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted && record.refusalReason.isEmpty &&
      requestCustody.originalRequests.exists(_ eq record) && control!=null && execution!=null &&
      control.operationId!=execution.operationId && control.operationId!=record.journalTag.operationId &&
      execution.operationId==record.originalTag.operationId && execution!=record.originalTag,
      "actual original Boot one-shot J/C intent required")
  private[floatingpod] def retainJournalTakeoverFailure(cause:Throwable):Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread && cause!=null,"actual Boot failure Main required")
    beginFailedStop(cause)
    val retainedResponsibility=journalTakeover.nonEmpty || refusalFinal.nonEmpty ||
      positiveRecall.exists(state=> !positiveRecallIsStoppedEmpty(state)) || issuingTakeover!=null || unmatchedBootCompletions.nonEmpty
    if retainedResponsibility && !stoppedPositiveIsOnlyBootResponsibility then
      callbacks.adapter.retainUnsettledStop(cause)

  /** 原遍历的每行都已恢复或由原局部失败槽持有；不把局部失败改写为成功。 */
  private[floatingpod] def requirePublicationOwner(original:ProductionDbFirstBootstrapCallbacks):Unit =
    owner();requireRecallRequestsSettled()
    require((original eq callbacks) && installed && started && finished &&
      callbacks.recoveryReadData.desiredActiveRows.forall(row=>recovered.contains(row.uuid) != failed.contains(row.uuid)),
      "original completed schema6 recovery cohort custody required")

  private[floatingpod] def unresolvedRecallRequestCount:Int =
    owner()
    requestCustody.unresolvedCount
  private[floatingpod] def requireRecallRequestsSettled():Unit =
    owner()
    requestCustody.requireSettled()

  def recoveredPods: Vector[UUID] =
    owner(); require(finished, "original schema6 recovery pass has not returned")
    recovered.toVector
  def failedPods: Map[UUID, Throwable] =
    owner(); failed.iterator.map((id, entry) => id -> entry.cause).toMap
  def completedPass: Boolean =
    owner(); finished

  private[floatingpod] def requireFinalMainRelease(original:ProductionDbFirstBootstrapCallbacks):Unit =
    owner();requireRecallRequestsSettled()
    require((original eq callbacks) && finished,"original completed schema6 recovery owner required")
    failed.headOption.foreach(entry=>throw entry._2.cause)
    manager.requireDbFirstFinalMainReleased(callbacks)
    tickets.requireDbFirstFinalTicketsReleased(callbacks)
    require(!finalMainReleased && resultsSealed,"sealed original Boot final Main release is one-shot")
    finalMainReleased=true
    internalPublication=None;publicationPump=None;publicationLease=None
    journalTakeover=None;refusalFinal=None;positiveRecall=None;detachingTakeover=null;completingRefusal=null;restoringTerminal=null;
    issuingCapture=null;captureStage=0;detachingPositive=null;completingCommitted=null;positiveDispatch=null;positiveTick=null;unmatchedBootCompletions=Vector.empty
    issuingMainDecision=null;mainDecisionStage=0;observedMainReason=None
    requestCustody.detach(this)
    resultPass=None;podRestoration=None;originalRecallRequests=None;censusDetached=true
    callbacks.detachBootRecallCensus(this)

private[floatingpod] object DbFirstBootActiveRecovery:
  /** 只保一次真实Pod恢复责任；不持DATA/RAW，不发行原C或terminal结果资格。 */
  private[server] final class PodRestorationEntry private[DbFirstBootActiveRecovery] (
      private val custody:DbFirstBootRecallRequestCustody,private val census:Vector[MySqlDbFirstRecallRequestRecord],
      private val main:Thread,private val binding:UUID,private val boot:UUID,
      private val originalPump:ForgePermanentMainPump,private val originalLease:PermanentPumpLease):
    private[server] def requireOriginal(actual:DbFirstBootRecallRequestCustody,
        records:Vector[MySqlDbFirstRecallRequestRecord],thread:Thread,currentBinding:UUID,currentBoot:UUID,
        pump:ForgePermanentMainPump,lease:PermanentPumpLease):Unit =
      require((actual eq custody) && (records eq census) && (thread eq main) && (Thread.currentThread() eq main) &&
        currentBinding==binding && currentBoot==boot && (pump eq originalPump) && (lease eq originalLease),
        "same actual Pod restoration custody/binding/boot/pump required")

  /** 仅携metadata；资格由真实issuer登记实例核验，不能依赖Scala可见性。不是旧final ACK。 */
  private[server] final class TerminalRecoveryPass private[DbFirstBootActiveRecovery] (
      private val custody:DbFirstBootRecallRequestCustody,private val census:Vector[MySqlDbFirstRecallRequestRecord],
      private val main:Thread,val bindingUuid:UUID,val bootUuid:UUID):
    private[server] def requireOriginal(actual:DbFirstBootRecallRequestCustody,
        records:Vector[MySqlDbFirstRecallRequestRecord],thread:Thread):Unit =
      require((actual eq custody) && (records eq census) && (thread eq main) && (Thread.currentThread() eq main) &&
        bootUuid!=null && bootUuid!=new UUID(0L,0L),"exact actual Boot terminal recovery handoff required")
  /** AboutToStart消费原Full27一次；返回后runtime先保owner，再install真实Forge回调。 */
  def prepare(callbacks: ProductionDbFirstBootstrapCallbacks, modInstance: Object): DbFirstBootActiveRecovery =
    require(callbacks != null && modInstance != null, "original schema6 callbacks and actual mod instance required")
    callbacks.consumeClaimedMainReads(callbacks.actualServer)
    val tickets = ChunkLoadingManager.forDbFirst(callbacks, new ChunkTicketGatewayImpl(modInstance))
    val manager = ActiveFloatingPodManager.forDbFirst(callbacks, tickets)
    new DbFirstBootActiveRecovery(callbacks, tickets, manager)
