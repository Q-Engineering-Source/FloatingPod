package com.wi.floatingpod.server

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.debug.{DebugItemEndpoint, MainDebugRequestContext, MainPodOperationRequest, MainPodRequestContext, MainConsoleCreateContext, ConsolePodEndpoint}
import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.*
import net.minecraft.server.MinecraftServer
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success}
import org.apache.logging.log4j.LogManager

/** 真实Main endpoint；由原 claimed runtime 装配，不接受caller Ready boolean。
  * 所有游戏引用仅在Main retained context；worker收到固定immutable commands。
  */
private[floatingpod] final class MainDebugItemEndpoint private[server] (
    server: MinecraftServer, adapter: MySqlOwnedIoAdapter,
    manager: ActiveFloatingPodManager, chunks: ChunkLoadingManager,
    publication: ActiveFloatingPodManager#MainPublicationScope,
    pump: ForgePermanentMainPump, pumpLease: PermanentPumpLease, actualSourceLimitProfile: SourceLimitProfile,
    ledgerCapacity: Int = 256, drainBound: Int = 32,
    dbFirstCallbacks:Option[com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks]=None) extends DebugItemEndpoint with ConsolePodEndpoint:
  require(server != null && adapter != null && manager != null && chunks != null && publication != null &&
    pump != null && pumpLease != null && actualSourceLimitProfile != null && ledgerCapacity >= 1 && ledgerCapacity <= 256 &&
    drainBound >= 1 && drainBound <= ledgerCapacity, "bounded actual Main dependencies required")
  private val logger = LogManager.getLogger(getClass)
  private val main = Thread.currentThread()
  private val identity = adapter.currentExecutionIdentity
  require(server.isCallingFromMinecraftThread && publication.identity == identity &&
    pumpLease.bootUuid == identity.bootId && pumpLease.bootGeneration == identity.bootGeneration,
    "actual Main/pump/publication/owned identity required")
  private val ledger = new MainDebugOperationLedger(identity, main, ledgerCapacity, actualSourceLimitProfile)
  private val modeProjectionReleases = new MainModeProjectionReleaseLedger(main, identity, ledgerCapacity)
  private var unmatchedModeProjectionReleaseCompletions = Vector.empty[IoCompletion[MySqlOwnedIoReply]]
  private final class Pending(val entry: ledger.Entry, val context: MainPodRequestContext):
    val workflowUuid: UUID = UUID.randomUUID() // 单独一次mint，UI/domain create UUID不变。
    var world = Option.empty[ControlledPodWorld]
    var body = Option.empty[ControlledActivePodEntity]
    var ticket = Option.empty[ChunkTicket]
    var prepare = Option.empty[MySqlControlledDebugPreparedGraph]
    var dbFirstPrepared = Option.empty[MySqlDbFirstInitialPreparationDataView]
    var initialPublished = Option.empty[MainControlledPublicationObservation]
    var mode = Option.empty[MySqlOwnedModeCommitRequest]
    var commitTag = Option.empty[IoOperationTag]
    var failureFeedbackAttempted = false
    var failureLogged = false
  private val pending = mutable.LinkedHashMap.empty[UUID, Pending]
  private var stopped = false
  private var businessAdmissionSealed = false
  private var lastOrdinal = 0L
  private var normalRecall = Option.empty[NormalRecallResponsibility]
  private var playerRecalls = Option.empty[DbFirstPlayerRecallOwner]

  private[floatingpod] def playerPackets(reads: DbFirstMainRequestReadBridge,recovery:DbFirstBootActiveRecovery): com.wi.floatingpod.network.ServerPacketEndpoint =
    owner()
    require(playerRecalls.isEmpty && publication.requireOriginalDbFirstMain(adapter), "original schema6 player endpoint once")
    require(recovery!=null && (recovery.manager eq manager) && (recovery.tickets eq chunks),
      "same actual Boot recovery owner required for player results")
    recovery.requireResultSource(server,adapter)
    val original = new DbFirstPlayerRecallOwner(server, adapter, manager, publication, reads,recovery,
      () => !businessAdmissionSealed && isReady(), ledgerCapacity)
    playerRecalls = Some(original)
    new com.wi.floatingpod.network.ServerPacketEndpoint:
      override def receiveRelease(message: com.wi.floatingpod.network.C2SReleaseRequest,
          context: net.minecraftforge.fml.common.network.simpleimpl.MessageContext): Unit = original.receiveRelease(message, context)
      override def receiveRecall(message: com.wi.floatingpod.network.C2SRecallRequest,
          context: net.minecraftforge.fml.common.network.simpleimpl.MessageContext): Unit = original.receiveRecall(message, context)
      override def receiveRecallOperation(message: com.wi.floatingpod.network.C2SRecallOperationRequest,
          context: net.minecraftforge.fml.common.network.simpleimpl.MessageContext): Unit = original.receiveRecallOperation(message, context)

  private def owner(): Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread,
      "endpoint requires exact actual server Main")
  private[floatingpod] def sealNewRequestsForPreStop(): Unit =
    owner()
    businessAdmissionSealed = true

  private[floatingpod] def beginNormalStop(request: UUID): Unit =
    owner()
    if publication.requireOriginalDbFirstMain(adapter) then
      pump.assertReadyCurrent(pumpLease)
      val io=adapter.checkpoint.io
      io.fatalCause.foreach(throw _)
      require(io.accepting,"original schema6 stop drain requires accepting owned IO")
    else usable()
    sealNewRequestsForPreStop()
    publication.sealForNormalStop(request)

  private[floatingpod] def captureNormalStopCensus(request: UUID): Option[ActiveFloatingPodManager#MainPublicationScope#NormalStopCensus] =
    owner()
    require(businessAdmissionSealed && !stopped, "original endpoint must keep completion drain alive")
    publication.retainedFailureCauses.headOption.foreach(throw _)
    playerRecalls.toVector.flatMap(_.failures).headOption.foreach(throw _)
    if playerRecalls.exists(_.retainedCount != 0) then return None
    if publication.dbFirstSnapshotDrainPending then return None
    usable()
    if pending.nonEmpty || ledger.retainedCount != 0 || modeProjectionReleases.pendingCount != 0 ||
      unmatchedCompletionResponsibility.nonEmpty then None
    else publication.captureNormalStopCensus(request)

  private[floatingpod] def beginWorldResidentCheckpoints(census: ActiveFloatingPodManager#MainPublicationScope#NormalStopCensus): Unit =
    owner()
    require(businessAdmissionSealed && !stopped, "original normal stop drain required")
    usable()
    if publication.requireCurrentDbFirstMain(adapter) then
      publication.beginDbFirstNormalStopCensus(adapter,census)
    else
      publication.beginWorldResidentCheckpoints(census)
      normalRecall = Some(publication.beginNormalRecalls(adapter, census, () => usable()))
  private[floatingpod] def normalRecallCompleted: Boolean =
    owner()
    publication.dbFirstNormalRecallCompleted.getOrElse(normalRecall.exists(_.isComplete))
  private var finalPreparationStage = 0
  private[floatingpod] def beginOrdinaryFinalPreparation(admission: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission): Unit =
    owner()
    require(finalPreparationStage == 0 && businessAdmissionSealed && !stopped, "original endpoint final preparation once")
    admission.beginFinalSealing()
    finalPreparationStage = 1
  private[floatingpod] def requireOriginalFinalSourceOwner(original: Object, escrow: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow): Unit =
    owner()
    require(finalPreparationStage == 1, "original endpoint source preparation phase required")
    publication.requireFinalSourceOwner(original, escrow)
  private[floatingpod] def requireOriginalFinalSource(original: Object, escrow: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.SourceEscrow,
      source: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission.FinalSaveValidator): Unit =
    requireOriginalFinalSourceOwner(original, escrow)
    publication.requireFinalSource(original, escrow, source)
  /** 原END已推进completion/Recall/drift；本方法不启动第二pump或改变Recall票序。 */
  private[floatingpod] def advanceOrdinaryFinalPreparation(
      callbacks: com.wi.floatingpod.bootstrap.ProductionDedicatedBootstrapCallbacks): Boolean =
    owner()
    require(finalPreparationStage == 1 && businessAdmissionSealed && !stopped, "original Main final preparation phase required")
    (ledger.retainedFailureCauses ++ modeProjectionReleases.retainedFailureCauses ++ publication.retainedFailureCauses).headOption.foreach(throw _)
    require(unmatchedCompletionResponsibility.isEmpty, "unmatched Main completion cannot become pending success")
    if pending.nonEmpty || ledger.retainedCount != 0 || modeProjectionReleases.pendingCount != 0 then false
    else
      ledger.requireMainDrainSettled()
      val admission = callbacks.ordinarySaveAdmission()
      publication.tryBeginFinalPreparation(admission) match
        case None => false
        case Some(request) =>
          if normalRecall.isEmpty then
            captureNormalStopCensus(request).foreach(beginWorldResidentCheckpoints)
          if normalRecall.isEmpty || !publication.prepareFinalCohort() then false
          else
            val original = publication.finalSourceOwner
            val escrow = publication.finalEscrow
            if !com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.tryFenceOrdinaryFinalSource(server, callbacks, escrow, original) then false
            else
              val source = publication.releaseAndMintFinalCohort()
              com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.installOrdinaryFinalSource(server, callbacks, escrow, original, source)
              // 原factory/source仍由Admission/Scope持有并保费；这里仅撤原Main consumer图。
              publication.dropFinalMainReferences()
              normalRecall = None
              ledger.dropSettledMainReferences()
              finalPreparationStage = 2
              true

  /** 原runtime必须先完成read屏障；本入口只交接实际final-save source，不签最终停服许可。 */
  private[floatingpod] def advanceDbFirstFinalPreparation(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks):Boolean =
    owner()
    require(finalPreparationStage==1 && businessAdmissionSealed && !stopped &&
      publication.requireOriginalDbFirstMain(adapter),"original schema6 final preparation phase required")
    callbacks.requireOriginalMainAdapter(adapter)
    // 原受限操作尚有真实END/worker ACK责任；先等原owner，不把保留故障升级成停止其pump。
    if playerRecalls.exists(_.retainedEndPending) || publication.dbFirstStopRetainedEndPending then return false
    playerRecalls.toVector.flatMap(_.failures).headOption.foreach(throw _)
    if playerRecalls.exists(_.retainedCount != 0) then return false
    (ledger.retainedFailureCauses ++ modeProjectionReleases.retainedFailureCauses ++ publication.retainedFailureCauses).headOption.foreach(throw _)
    require(unmatchedCompletionResponsibility.isEmpty,"unmatched schema6 completion cannot become final success")
    if pending.nonEmpty || ledger.retainedCount!=0 || modeProjectionReleases.pendingCount!=0 then false
    else
      ledger.requireMainDrainSettled()
      publication.tryBeginFinalPreparation(callbacks.ordinarySaveAdmission()) match
        case None=>false
        case Some(request)=>
          if publication.dbFirstNormalRecallCompleted.isEmpty then
            captureNormalStopCensus(request).foreach(beginWorldResidentCheckpoints)
          if publication.dbFirstNormalRecallCompleted.isEmpty || !publication.prepareDbFirstFinalCohort() then false
          else
            val original=publication.finalSourceOwner
            val escrow=publication.finalEscrow
            if !com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.tryFenceDbFirstFinalSource(server,callbacks,escrow,original) then false
            else
              val source=publication.mintDbFirstFinalCohort()
              com.wi.floatingpod.bootstrap.ProductionBootstrapInstallation.installDbFirstFinalSource(server,callbacks,escrow,original,source)
              publication.dropFinalMainReferences()
              ledger.dropSettledMainReferences()
              finalPreparationStage=2
              true

  private def usable(): Unit =
    owner(); require(!stopped && !ledger.isRetired, "endpoint is stopped/retired")
    pump.assertReadyCurrent(pumpLease)
    if !publication.requireCurrentDbFirstMain(adapter) then
      val observation = adapter.pollAdmissionMaintenance
      observation.failure.foreach(cause => throw cause)
      require(observation.isCurrent, "actual owned admission freshness is unavailable/expired/failed")
      val value = observation.latest.get
      require(value.bootId == identity.bootId && value.bootGeneration == identity.bootGeneration &&
        value.connectionGeneration == identity.connectionGeneration && value.bindingId == publication.binding.bindingUuid,
        "actual freshness belongs to another boot/connection/binding")
    val ioState = adapter.checkpoint.io
    ioState.fatalCause.foreach(cause => throw cause)
    require(ioState.accepting && ioState.fatalCause.isEmpty,
      "owned IO no longer accepts healthy business commands")

  override def isReady(): Boolean =
    owner()
    if stopped || ledger.isRetired || businessAdmissionSealed then return false
    try
      usable()
      true
    catch case _:IllegalArgumentException | _:IllegalStateException=>false

  override def inspect(request: DebugItemEndpoint.Request): DebugItemEndpoint.CachedPod =
    owner()
    if !isReady() || request == null || request.identity().pod() == null then return null
    val context = MainDebugRequestContext.capture(server, request)
    context.assertCurrent()
    publication.inspect(request.identity().pod()).map { view =>
      new DebugItemEndpoint.CachedPod(request.identity(), view.ownerUuid.orNull,
        view.operatingMode.toString, view.active, view.phase.toString, view.reason)
    }.orNull

  override def submit(request: DebugItemEndpoint.Request): DebugItemEndpoint.Reply =
    owner()
    if businessAdmissionSealed then return reply(DebugItemEndpoint.Status.UNAVAILABLE)
    if !isReady() then return reply(DebugItemEndpoint.Status.UNAVAILABLE)
    require(request != null && request.action() != DebugItemEndpoint.Action.INSPECT, "fixed business request required")
    val context = MainDebugRequestContext.capture(server, request)
    if request.action() == DebugItemEndpoint.Action.PLACE then
      return submitPlacement(new MainPodOperationRequest.Player(request),context)
    if request.action() == DebugItemEndpoint.Action.TOGGLE && pending.values.exists(value =>
      value.entry.request.action() == DebugItemEndpoint.Action.TOGGLE &&
      value.entry.request.playerRequest().identity().pod() == request.identity().pod()) then
      return reply(DebugItemEndpoint.Status.REJECTED)
    val reserved = ledger.reserve(request)
    if reserved.isEmpty then return reply(DebugItemEndpoint.Status.REJECTED)
    val entry = reserved.get
    val retained = new Pending(entry, context)
    pending.put(request.operation(), retained) // 先保留，freeze/spawn/actualsubmit抛错也不丢责任。
    try
      request.action() match
        case DebugItemEndpoint.Action.TOGGLE =>
          val details = manager.managedActiveDetails(request.identity().pod()).getOrElse(
            throw new IllegalStateException("Owned body has no committed Main mapping"))
          require(details.exactState.ownerUuid.contains(request.identity().player()), "actual owner required")
          val body = details.entity match
            case value: ControlledActivePodEntity => value
            case _ => throw new IllegalStateException("Actual mode-control body adapter unavailable")
          val actual = new ActivePodEntityImpl(context.pod())
          require(body.sameBody(actual) && details.world.sameWorld(PodWorldImpl(context.world())), "original mapped body/world differ")
          retained.world = Some(PodWorldImpl(context.world())); retained.body = Some(body)
          val ticket = chunks.getAllManagedTickets().getOrElse(body.podUuid,
            throw new IllegalStateException("Original managed ticket unavailable"))
          retained.ticket = Some(ticket)
          publication.assertCriticalTransitionAllowed(body.podUuid)
          body.beginPendingMode(request.operation(), request.identity().player(), details.exactState.stateVersion)
          val mode = if details.exactState.operatingMode == PodOperatingMode.Normal then PodOperatingMode.WorldIdle else PodOperatingMode.Normal
          val command = MySqlOwnedModeCommitRequest(publication.binding, body.podUuid, request.identity().player(),
            details.exactState.stateVersion, details.attemptUuid, mode, body.actualPose)
          retained.mode = Some(command)
          val tag = operationTag(UUID.randomUUID()); retained.commitTag = Some(tag)
          val plan = ledger.reserveOwnedRepositoryPlan(adapter, entry, MainDebugStage.ChangingMode, retained.workflowUuid,
            tag, UUID.randomUUID(), command.repositoryOperation)
          publication.retainMode(tag, request, command, plan, modeSession(context), retained.world.get,
            body, ticket, publication.runtimeGeneration)
          ledger.requirePlannedSubmission(entry, tag, MainDebugStage.ChangingMode)
          val result = adapter.submit(tag, MySqlOwnedIoCommand.CommitOwnedMode(command, plan))
          if ledger.submitted(entry, tag, result, MainDebugStage.ChangingMode) then reply(DebugItemEndpoint.Status.PENDING)
          else
            ledger.fail(entry, new IllegalStateException("Owned mode was not accepted; retained body remains frozen"))
            reply(DebugItemEndpoint.Status.REJECTED)
        case _ => throw new IllegalArgumentException("Unsupported debug business action")
    catch
      case cause: Throwable => failRetained(retained, cause); throw cause

  override def submitConsole(actualServer: MinecraftServer, sender: net.minecraft.command.ICommandSender,
      operation: UUID, dimension: Int, position: DebugItemEndpoint.Position): DebugItemEndpoint.Reply =
    owner()
    require((actualServer eq server) && (sender eq server), "actual local console required")
    if !isReady() || !publication.requireOriginalDbFirstMain(adapter) then return reply(DebugItemEndpoint.Status.UNAVAILABLE)
    val context=MainConsoleCreateContext.capture(server,sender,this,operation,dimension,position)
    submitPlacement(context.request(),context)

  override def inspectConsole(actualServer: MinecraftServer, sender: net.minecraft.command.ICommandSender,
      pod: UUID): ConsolePodEndpoint.State =
    owner()
    require((actualServer eq server) && (sender eq server), "actual local console required")
    require(isReady(), "actual Ready console endpoint required")
    publication.inspect(pod).map { view =>
      new ConsolePodEndpoint.State(view.podUuid,view.ownerUuid.orNull,view.operatingMode.toString,
        view.active,view.phase.toString,view.stateVersion,view.pose.dimensionId,
        view.pose.posX,view.pose.posY,view.pose.posZ)
    }.orNull

  override def resumeConsole(actualServer:MinecraftServer,sender:net.minecraft.command.ICommandSender,
      originalCreate:UUID):DebugItemEndpoint.Reply =
    owner()
    require((actualServer eq server) && (sender eq server) && originalCreate!=null,"actual local console and original create required")
    if businessAdmissionSealed || !isReady() || !publication.requireOriginalDbFirstMain(adapter) ||
      !adapter.canReserveDbFirstInitialPreparation then return reply(DebugItemEndpoint.Status.UNAVAILABLE)
    val callbacks=dbFirstCallbacks.getOrElse(throw new IllegalStateException("actual schema6 console read owner missing"))
    callbacks.requireOriginalMainAdapter(adapter)
    val prepared=callbacks.recoveryReadData.preparedInitial(originalCreate).getOrElse(
      return reply(DebugItemEndpoint.Status.REJECTED))
    val pose=prepared.receipt.targetPose
    val context=MainConsoleCreateContext.capture(server,sender,this,originalCreate,pose.dimensionId,
      new DebugItemEndpoint.Position(pose.posX,pose.posY,pose.posZ,pose.yaw))
    require(context.request().worldName()==pose.worldName,"original prepared console world differs")
    val entry=ledger.reserve(context.request()).getOrElse(return reply(DebugItemEndpoint.Status.REJECTED))
    val retained=new Pending(entry,context)
    pending.put(originalCreate,retained)
    try
      activatePrepared(retained,prepared,MainDebugPreparationSource.RecoveredPreparation)
      reply(DebugItemEndpoint.Status.PENDING)
    catch
      case cause:Throwable =>
        failRetained(retained,cause)
        throw cause

  /** 两个真实入口共用唯一 Prepare → physical → critical commit → 原责任退休流程。 */
  private def submitPlacement(request: MainPodOperationRequest, context: MainPodRequestContext): DebugItemEndpoint.Reply =
    context.assertCurrent()
    if !adapter.canReserveDbFirstInitialPreparation then return reply(DebugItemEndpoint.Status.UNAVAILABLE)
    val reserved=ledger.reserve(request)
    if reserved.isEmpty then return reply(DebugItemEndpoint.Status.REJECTED)
    val entry=reserved.get
    val retained=new Pending(entry,context)
    pending.put(request.operation(),retained)
    try
      val position = request.placement()
      val pose = FloatingPodPose(request.worldName(), position.x(), position.y(), position.z(),
        position.yaw(), 0F, request.dimension())
      val command = MySqlControlledDebugPrepareRequest(publication.binding,
        UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), pose, request.operation())
      val tag = operationTag(UUID.randomUUID())
      ledger.reserveDbFirstPreparation(adapter,entry,tag,command,publication.activeLimit)
      val submitted=adapter.submit(tag,ledger.dbFirstCommandForSubmission(entry,tag,MainDebugStage.Preparing))
      if ledger.submitted(entry,tag,submitted,MainDebugStage.Preparing) then reply(DebugItemEndpoint.Status.PENDING)
      else
        // 原issuer的Rejected Entry尚待专用退休；不得走旧plan drop或丢弃原tag。
        reply(DebugItemEndpoint.Status.REJECTED)
    catch
      case cause: Throwable => failRetained(retained,cause); throw cause

  /** 正式永久pump callback调用；不注册第二listener，不等待IO、不调用manager旧SQL接口。
    * batch的全部completion先retain，再逐项publication；异常不会遗失batch余项。
    */
  private[floatingpod] def onEnd(tick: PermanentEndTick): Unit =
    beginEnd(tick)
    if tick.phase == PermanentPumpPhase.Ready && !publication.hasTrackedDriftRuntime then
      publication.enableTrackedDrift(adapter, pump, pumpLease)
    MainDebugItemEndpoint.advancePhysicalEnd(
      publication.onTrackedDriftEnd(tick.endOrdinal, tick.phase, drainBound),
      {
        publication.onDbFirstNormalStopEnd(adapter,tick)
        playerRecalls.foreach(_.onEnd(tick))
        normalRecall.filterNot(_.isComplete).foreach(_.onEnd(tick))
      })
    // 必须在本轮retain之前取旧END已准备的释放；本轮新图不能同栈发行receipt。
    val preparationRetries=pending.values.filter(value=>ledger.preparationReadRetryReady(value.entry)).toVector
    val initialRetirements=pending.values.filter(value=>value.initialPublished.nonEmpty && ledger.initialRetirementReady(value.entry)).toVector
    val preparationFinals=pending.values.filter(value=>ledger.preparationFinalReady(value.entry)).toVector
    val settledRejections=pending.values.filter(value=>ledger.settledRejectionReady(value.entry)).toVector
    val rejectedPreparations=pending.values.filter(value=>ledger.rejectedPreparationReady(value.entry)).toVector
    val projectionReleasesReadyBeforeDrain = modeProjectionReleases.snapshotReadyAtEndStart()
    val rejectedDropsBeforeDrain = modeProjectionReleases.rejectedDropsAtEndStart()
    if tick.phase == PermanentPumpPhase.Quiescing && !stopped then stop()
    retainArrivedCompletions()
    normalRecall.foreach(_.consumeRetained())
    if ledger.isRetired && !stopped then stop()
    consumePendingBusinessCompletions()
    if !stopped && tick.phase == PermanentPumpPhase.Ready then
      preparationRetries.foreach { value=>
        if ledger.preparationReadRetryReady(value.entry) then
          try ledger.submitPreparationReadHandoff(adapter,value.entry)
          catch case cause:Throwable=>failRetained(value,cause)
      }
      initialRetirements.foreach { value=>
        if ledger.initialRetirementReady(value.entry) then
          try
            val receipt=publication.prepareDbFirstInitialRetirement(value.commitTag.get)
            ledger.detachInitialPayload(value.entry,receipt)
            if ledger.isRecoveredPreparation(value.entry) then value.dbFirstPrepared=None
            val result=publication.submitDbFirstInitialRetirement(value.commitTag.get)
            ledger.initialRetirementSubmitted(value.entry,receipt,result)
          catch case cause:Throwable=>failRetained(value,cause)
      }
      preparationFinals.foreach { value=>
        if ledger.preparationFinalReady(value.entry) then
          try
            val receipt=ledger.detachPreparationPayload(adapter,value.entry)
            value.dbFirstPrepared=None // 原回执已保留；worker关闭Entry root前放最后的Main DATA引用。
            val result=adapter.submitDbFirstPreparationFinalRelease(receipt)
            ledger.preparationFinalSubmitted(value.entry,receipt,result)
          catch case cause:Throwable=>failRetained(value,cause)
      }
      settledRejections.foreach { value=>
        if ledger.settledRejectionReady(value.entry) then
          try
            require(value.body.isEmpty && value.ticket.isEmpty && value.dbFirstPrepared.isEmpty,
              "settled preparation rejection retains unexpected physical effects")
            ledger.submitSettledRejectionRetirement(adapter,value.entry)
          catch case cause:Throwable=>failRetained(value,cause)
      }
      rejectedPreparations.foreach { value=>
        if ledger.rejectedPreparationReady(value.entry) then
          try
            require(value.body.isEmpty && value.ticket.isEmpty && value.dbFirstPrepared.isEmpty,
              "rejected preparation unexpectedly retains physical effects")
            ledger.settleRejectedPreparation(adapter,value.entry)
            pending.remove(value.entry.request.operation())
          catch case cause:Throwable=>failRetained(value,cause)
      }
      submitReadyModeProjectionReleases(projectionReleasesReadyBeforeDrain)
      rejectedDropsBeforeDrain.foreach(tag => modeProjectionReleases.settleRejectedDrop(adapter, tag))

  /** 原read屏障期间只接管完成及已提交释放ACK，不推进物理动作或新写入。 */
  private[floatingpod] def onReadDrainEnd(tick: PermanentEndTick): Unit =
    beginEnd(tick)
    require(businessAdmissionSealed && !stopped && tick.phase == PermanentPumpPhase.Ready,
      "original sealed Ready endpoint read drain required")
    retainArrivedCompletions()

  /** runtime已核实际read词法作用域退出；原读失败保管，不授新producer或停服冻结资格。 */
  private[floatingpod] def onPreStopRecallEnd(tick:PermanentEndTick):Unit =
    beginEnd(tick)
    require(businessAdmissionSealed && !stopped && tick.phase==PermanentPumpPhase.Ready &&
      publication.requireOriginalDbFirstMain(adapter),"original sealed schema6 pre-stop recall END required")
    playerRecalls.foreach(_.onEnd(tick))
    retainArrivedCompletions() // 本END新到reply仍由后续END消费，其他已接受owner继续保管完整inbox。

  private[floatingpod] def preStopPlayerRecallSettled:Boolean =
    owner()
    require(businessAdmissionSealed && !stopped,"original sealed endpoint required for player recall drain")
    if playerRecalls.exists(_.retainedEndPending) then false
    else
      playerRecalls.toVector.flatMap(_.failures).headOption.foreach(throw _)
      !playerRecalls.exists(_.retainedCount!=0)

  private def beginEnd(tick: PermanentEndTick): Unit =
    owner()
    require(tick != null && tick.bootUuid == identity.bootId && tick.bootGeneration == identity.bootGeneration &&
      tick.endOrdinal > lastOrdinal, "exact monotonic permanent END required")
    lastOrdinal = tick.endOrdinal

  private var interruptedDrainBatch=Option.empty[Vector[IoCompletion[MySqlOwnedIoReply]]]
  private def retainArrivedCompletions(): Unit =
    require(interruptedDrainBatch.isEmpty,"previous original drain batch still retains unresolved partition responsibility")
    val drained = adapter.drain(drainBound)
    interruptedDrainBatch=Some(drained) // 所有分区先拥有完整batch，任一retain失败不能丢失其余完成。
    val (playerCompletions,afterPlayers)=drained.partition(value=>playerRecalls.exists(_.owns(value.tag)))
    playerRecalls.foreach(_.retain(playerCompletions))
    val (stopRecallCompletions,afterStopRecall)=afterPlayers.partition(value=>publication.ownsDbFirstStopRecall(value.tag))
    publication.retainDbFirstStopRecallBatch(stopRecallCompletions)
    val (snapshotCompletions,remaining)=afterStopRecall.partition(value=>publication.ownsDbFirstPeriodicSnapshot(value.tag))
    publication.retainDbFirstPeriodicSnapshotBatch(snapshotCompletions)
    val (recallCompletions, otherCompletions) = remaining.partition(value => normalRecall.exists(_.owns(value)))
    normalRecall.foreach(_.retainBatch(recallCompletions))
    val (releaseCompletions, businessCompletions) = otherCompletions.partition(isModeProjectionReleaseCompletion)
    releaseCompletions.foreach(consumeModeProjectionRelease)
    ledger.retainBatch(businessCompletions)
    interruptedDrainBatch=None

  private def consumePendingBusinessCompletions(): Unit =
    ledger.pendingCompletionEntries(drainBound).foreach { entry =>
      val value = pending.getOrElse(entry.request.operation(), throw new IllegalStateException("Retained Main context missing"))
      if stopped then () // 全部实际完成已保留；不进入物理/feedback publication。
      else
        try
          val completion = ledger.claimPendingCompletion(entry)
          entry.stage match
            case MainDebugStage.Preparing =>
              ledger.submitPreparationReadHandoff(adapter,entry)
            case MainDebugStage.PreparationReadHandoff =>
              ledger.consumePreparationReadHandoff(adapter,entry,completion)
              if entry.stage==MainDebugStage.Prepared then placeDbFirstPrepared(value)
            case MainDebugStage.RejectedPreparationRetiring =>
              val original=ledger.consumeSettledRejectionRetirement(adapter,entry,completion)
              // 原确认只接受固定容量/身份拒绝；受控内部日志保留具体首因，公共反馈不暴露内部列。
              logger.info("FP_POD_PREPARATION_REJECTED operation="+entry.request.operation(),original)
              // 正常容量/身份拒绝不是业务成功；最终退休后才发送失败并解除Main占用。
              require(!value.failureFeedbackAttempted,"settled rejection feedback is one-shot")
              value.failureFeedbackAttempted=true
              try value.context.feedbackFailed()
              catch case feedback:Throwable=>
                if feedback ne original then original.addSuppressed(feedback)
                throw original
              ledger.acknowledgedSettledRejection(entry)
              pending.remove(entry.request.operation())
            case MainDebugStage.Committing =>
              completion.result match
                case Success(MySqlOwnedIoReply.ControlledDebugCommitted(_)) => usable()
                case _ => () // 原failure/afterACK责任无需健康Ready才能保留consume。
              finish(value, publication.consume(completion, publication.runtimeGeneration))
            case MainDebugStage.InitialRetiring =>
              value.initialPublished=Some(ledger.consumeInitialRetirement(entry,completion) {
                publication.consumeDbFirstInitialRetirement(completion)
              })
              if ledger.isRecoveredPreparation(entry) then finishDbFirstRetired(value)
            case MainDebugStage.PreparationRetiring =>
              ledger.consumePreparationFinal(adapter,entry,completion)
              finishDbFirstRetired(value)
            case MainDebugStage.ChangingMode =>
              completion.result match
                case Success(MySqlOwnedIoReply.OwnedModeCommitted(_)) => usable()
                case Success(MySqlOwnedIoReply.OwnedModeAlreadySet(_)) => usable()
                case _ => ()
              finish(value, publication.consumeMode(completion, publication.runtimeGeneration))
            case _ => throw new IllegalStateException("Actual completion arrived for an invalid ledger stage")
        catch case cause: Throwable => failRetained(value, cause)
    }

  private def isModeProjectionReleaseCompletion(completion: IoCompletion[MySqlOwnedIoReply]): Boolean =
    // 原实际Submitted control Entry分类，不相信reply自称release。
    modeProjectionReleases.isSubmitted(completion.tag)

  private def consumeModeProjectionRelease(completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    val mode = modeProjectionReleases.isModeProjection(completion.tag)
    modeProjectionReleases.consume(completion) match
      case MainModeProjectionReleaseResult.Released(tag) =>
        try
          if mode then publication.acknowledgeModeProjectionReleased(tag)
          else publication.acknowledgeControlledProjectionReleased(tag)
        catch
          case cause: Throwable =>
            unmatchedModeProjectionReleaseCompletions :+= completion
            logger.error("Mode projection was released by the owned worker but Main could not compact the exact ACK", cause)
      case MainModeProjectionReleaseResult.Failed(tag, cause) =>
        try
          if mode then publication.recordModeProjectionReleaseFailure(tag, cause)
          else publication.recordControlledProjectionReleaseFailure(tag, cause)
        catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
        logger.error("Mode projection memory release failed; retained worker accounting remains unresolved", cause)
      case MainModeProjectionReleaseResult.Unmatched(value) =>
        unmatchedModeProjectionReleaseCompletions :+= value
        logger.error("Unmatched or duplicate mode projection release completion retained for diagnosis",
          new IllegalStateException("exact mode projection release tag is not awaiting this ACK"))

  private def submitReadyModeProjectionReleases(readyAtStart: Vector[IoOperationTag]): Unit =
    readyAtStart.foreach { tag =>
      val mode = modeProjectionReleases.isModeProjection(tag)
      if !mode || publication.modeProjectionReleaseEligible(tag) then
        try
          val receipt = modeProjectionReleases.actualReceipt(tag)
          val result = if modeProjectionReleases.isModeProjection(tag) then
            adapter.submitModeProjectionRelease(tag, receipt)
          else adapter.submitRetainedPlanRelease(tag, receipt)
          modeProjectionReleases.submitted(tag, result)
          result match
            case IoSubmission.Accepted => if mode then publication.markModeProjectionReleaseSubmitted(tag)
            case IoSubmission.RejectedCapacity => () // exact tag remains ready for a later END.
            case IoSubmission.RejectedQuiescing =>
              val cause = new IllegalStateException("owned IO quiesced before the exact mode memory ACK was accepted")
              if mode then publication.recordModeProjectionReleaseFailure(tag, cause)
              else publication.recordControlledProjectionReleaseFailure(tag, cause)
            case IoSubmission.RejectedStopped(cause) =>
              val failure = cause.getOrElse(new IllegalStateException("owned IO stopped before the exact mode memory ACK was accepted"))
              if mode then publication.recordModeProjectionReleaseFailure(tag, failure)
              else publication.recordControlledProjectionReleaseFailure(tag, failure)
        catch
          case cause: Throwable =>
            try modeProjectionReleases.submissionFailed(tag, cause)
            catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
            try
              if mode then publication.recordModeProjectionReleaseFailure(tag, cause)
              else publication.recordControlledProjectionReleaseFailure(tag, cause)
            catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
            logger.error("Mode projection release submission outcome is unresolved; the exact tag remains retained", cause)
    }

  /** 读代交接完成后只使用原confirmation解码DATA；先占初建capture资格再创建物理对象。 */
  private def placeDbFirstPrepared(value:Pending):Unit =
    activatePrepared(value,ledger.dbFirstPreparedData(value.entry),MainDebugPreparationSource.NewPreparation)

  private def activatePrepared(value:Pending,prepared:MySqlDbFirstInitialPreparationDataView,
      source:MainDebugPreparationSource):Unit =
    val entry=value.entry
    require(prepared.receipt.createOperationUuid.contains(entry.request.operation()) &&
      prepared.receipt.binding==publication.binding,"original prepared placement identity required")
    usable();value.context.assertCurrent()
    val tag=operationTag(UUID.randomUUID());value.commitTag=Some(tag)
    val reservation=source match
      case MainDebugPreparationSource.NewPreparation=>ledger.reserveDbFirstInitialCapture(adapter,entry,tag,publication.activeLimit)
      case MainDebugPreparationSource.RecoveredPreparation=>
        ledger.reserveRecoveredInitialCapture(adapter,entry,tag,publication.activeLimit,prepared)
    value.dbFirstPrepared=Some(prepared) // recovered DATA已先计入真实capture root，才能跨读代保留。
    val world=PodWorldImpl(value.context.world());value.world=Some(world)
    val x=Math.floor(prepared.receipt.targetPose.posX/16D).toInt
    val z=Math.floor(prepared.receipt.targetPose.posZ/16D).toInt
    val ticket=chunks.requestPublicationTicket(tag,prepared.receipt.podUuid,world,x,z,
      actual=>value.ticket=Some(actual)) match
      case Success(actual)=>actual
      case Failure(cause)=>throw cause
    value.ticket=Some(ticket)
    val body=world.spawnControlledPod(prepared.state,prepared.receipt,actual=>
      value.body=Some(actual)
      actual match
        case forge:ActivePodEntityImpl=>value.context.retainCreatedBody(forge.rawEntity)
        case _=>throw new IllegalStateException("actual Forge body adapter missing"))
    require(value.body.exists(_.sameBody(body)),"spawn returned another retained body")
    value.context.assertCurrent()
    val snapshot=adapter.captureDbFirstSnapshot(reservation) { allocation=>
      body match
        case forge:ActivePodEntityImpl=>forge.rawEntity.captureDbFirstInitialActivation(tag.operationId,allocation)
        case _=>throw new IllegalStateException("actual Forge initial capture body missing")
    }
    publication.retainDbFirstInitial(reservation,snapshot,prepared.receipt,entry.request,
      placeSession(value.context,entry.request),world,body,ticket,publication.runtimeGeneration)
    val result=adapter.submit(tag,ledger.dbFirstCommandForSubmission(entry,tag,MainDebugStage.Committing))
    if !ledger.submitted(entry,tag,result,MainDebugStage.Committing) then
      throw new IllegalStateException("initial activation not accepted; original preparation/capture/body/ticket remain retained")

  private def finishDbFirstRetired(value:Pending):Unit =
    val view=value.initialPublished.flatMap(_.view).getOrElse(
      throw new IllegalStateException("original published initial view missing after actual retirement ACK"))
    value.context.feedbackCommitted(new DebugItemEndpoint.View(view.podUuid,view.ownerUuid.orNull,
      view.operatingMode.toString,view.active,view.phase.toString,view.reason))
    ledger.acknowledged(value.entry)
    value.initialPublished=None;value.dbFirstPrepared=None;value.commitTag=None
    pending.remove(value.entry.request.operation())

  private def finish(value: Pending, observation: MainControlledPublicationObservation): Unit =
    require(observation != null, "Actual Main publication observation missing")
    observation.cause.foreach(cause => throw cause)
    require((observation.status == MainControlledPublicationStatus.Published ||
      (value.mode.nonEmpty && observation.status == MainControlledPublicationStatus.AlreadySet)) &&
      observation.completionConsumed, "Actual Main publication failed/retired; obligations are not success")
    val view = observation.view.getOrElse(throw new IllegalStateException("Committed projection missing"))
    if value.dbFirstPrepared.nonEmpty then
      ledger.published(value.entry)
      value.initialPublished=Some(observation)
      return // 保留已发布映射及全部原Entry，待专用最终退休后才反馈/compact。
    ledger.published(value.entry)
    // 此临时值只保handle/tag；不把original encoded plan/full20 graph带入release Entry。
    val releases = value.entry.planSnapshot.map { reserved =>
      val handle = reserved.plan.flatMap(_.originalRetainedHandle).getOrElse(
        throw new IllegalStateException("published stage lacks actual original issuer handle"))
      (reserved.tag, handle, value.mode.nonEmpty && value.commitTag.contains(reserved.tag))
    }
    val reservedReleases = mutable.ArrayBuffer.empty[IoOperationTag]
    var ledgerCompacted = false
    try
      releases.foreach { (tag, handle, mode) =>
        modeProjectionReleases.reserveOwned(tag, handle, mode)
        reservedReleases += tag
      }
      value.context.feedbackCommitted(new DebugItemEndpoint.View(view.podUuid, view.ownerUuid.orNull,
        view.operatingMode.toString, view.active, view.phase.toString, view.reason))
      if value.mode.nonEmpty then
        publication.prepareModeProjectionRelease(value.commitTag.get)
      else publication.prepareControlledProjectionRelease(value.commitTag.get,value.prepare.get.tag)
      ledger.acknowledged(value.entry) // 清空其plans/completions；failed/retired不能进入此门。
      ledgerCompacted = true
      value.prepare = None
      value.mode = None
      value.commitTag = None
      pending.remove(value.entry.request.operation())
      // 本轮new Ready tags不在END-start snapshot，actual worker ACK最早下一END提交。
      reservedReleases.foreach(tag => modeProjectionReleases.armDropped(adapter, tag))
    catch
      case cause: Throwable =>
        if !ledgerCompacted then
          reservedReleases.foreach { tag =>
            try modeProjectionReleases.cancelUnsubmitted(tag)
            catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)
          }
        throw cause

  private def failRetained(value: Pending, cause: Throwable): Unit =
    ledger.fail(value.entry, cause)
    if !value.failureLogged then
      value.failureLogged=true // ledger先保cause不等于边界已经记录；完整原异常只记一次。
      logger.error("FloatingPod Main debug operation failed: action=" + value.entry.request.action() +
        " operation=" + value.entry.request.operation() + " stage=" + value.entry.stage, value.entry.cause.getOrElse(cause))
    if !value.failureFeedbackAttempted then
      value.failureFeedbackAttempted = true
      try value.context.feedbackFailed()
      catch case secondary: Throwable => if secondary ne cause then cause.addSuppressed(secondary)

  private def placeSession(context: MainPodRequestContext, original: MainPodOperationRequest): MainControlledPublicationSession = new MainControlledPublicationSession:
    override def assertCurrent(request: MainPodOperationRequest, world: PodWorld): Unit =
      require((request eq original) && request.action() == DebugItemEndpoint.Action.PLACE && world.sameWorld(PodWorldImpl(context.world())), "original PLACE world required")
      context.assertCurrent()
  private def modeSession(context: MainDebugRequestContext): MainOwnedModePublicationSession = new MainOwnedModePublicationSession:
    override def assertCurrent(request: DebugItemEndpoint.Request, world: PodWorld): Unit =
      require(request.action() == DebugItemEndpoint.Action.TOGGLE && world.sameWorld(PodWorldImpl(context.world())), "original MODE world required")
      context.assertCurrent()
  private def operationTag(operation: UUID): IoOperationTag =
    IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, operation)
  private def reply(status: DebugItemEndpoint.Status): DebugItemEndpoint.Reply = DebugItemEndpoint.Reply.of(status)
  private[floatingpod] def stop(): Unit =
    owner()
    if !stopped then
      val held = ledger.retainedCount + modeProjectionReleases.pendingCount +
        playerRecalls.map(_.retainedCount).getOrElse(0) +
        publication.retainedAsyncResponsibilityCount + unmatchedModeProjectionReleaseCompletions.size
      if held > 0 then
        val context = new IllegalStateException("actual Main Entry/completion/plan/body/ticket responsibility remains at stop; no release ACK")
        val causes = publication.retainedFailureCauses ++ ledger.retainedFailureCauses ++ modeProjectionReleases.retainedFailureCauses ++ playerRecalls.toVector.flatMap(_.failures)
        val original = publication.stopResponsibilityCause(causes.headOption.getOrElse(context))
        (causes :+ context).foreach { cause =>
          if (cause ne original) && !original.getSuppressed.contains(cause) &&
            !cause.getSuppressed.contains(original) && (cause.getCause ne original) then original.addSuppressed(cause)
        }
        adapter.retainUnsettledStop(original)
      stopped = true; ledger.retire(); publication.retire(); adapter.quiesce()
  private[server] def retainedResponsibilityCount: Int = ledger.retainedCount + modeProjectionReleases.pendingCount + playerRecalls.map(_.retainedCount).getOrElse(0)
  private[server] def unmatchedCompletionResponsibility: Vector[IoCompletion[MySqlOwnedIoReply]] =
    ledger.unmatchedCompletions ++ modeProjectionReleases.unmatched ++ unmatchedModeProjectionReleaseCompletions

  private[server] def responsibilitySnapshot: Vector[MainDebugResponsibility] =
    owner()
    pending.values.map(value => MainDebugResponsibility(value.entry.request, value.entry.stage,
      value.entry.completionSnapshot, value.entry.cause, value.body.nonEmpty, value.ticket.nonEmpty,
      value.prepare, value.mode, value.entry.planSnapshot,value.dbFirstPrepared,value.initialPublished)).toVector

/** 装配仍在 server package；不绕过 endpoint 对实际 Ready pump/admission 的检查。 */
private[floatingpod] object MainDebugItemEndpoint:
  /** 仅调度真实caller；完成ordinal不是closure/Ready/停止许可。 */
  private[floatingpod] def advanceReadFirstEnd(ordinal: Long, requested: Boolean, completedAt: Long)(
      ordinary: => Unit, retainOnly: => Unit, reads: => Unit,
      pending: => Boolean, strict: => Unit): Long =
    require(ordinal > 0L && completedAt >= 0L && (completedAt == 0L || requested), "original read drain schedule required")
    if !requested || completedAt > 0L then
      require(completedAt == 0L || ordinal > completedAt, "read drain resumes only on a later END")
      ordinary
      reads
      completedAt
    else
      retainOnly
      reads
      if pending then 0L
      else
        strict
        ordinal

  /** 保持原END顺序；drift与recall各自在原owner内校验，不由此函数提供Ready资格。 */
  private[server] def advancePhysicalEnd(drift: => Unit, recall: => Unit): Unit =
    drift
    recall

  def prepare(server: MinecraftServer, adapter: MySqlOwnedIoAdapter,
      recovery: ClaimedBootActiveRecovery, reads: MainBootProjection,
      pump: ForgePermanentMainPump, lease: PermanentPumpLease): MainDebugItemEndpoint =
    reads.requireCurrentReads()
    val identity = adapter.identity
    require(identity == reads.identity && lease.bootUuid == identity.bootId &&
      lease.bootGeneration == identity.bootGeneration, "original Boot/pump identity required")
    val binding = reads.ordinaryPublicationBinding
    val publication = recovery.manager.installMainPublication(identity, lease.bootGeneration, binding, reads.activeLimit)
    publication.retainOriginalRecallSource(reads)
    new MainDebugItemEndpoint(server, adapter, recovery.manager, recovery.tickets,
      publication, pump, lease, reads.limitProfile,
      drainBound = Math.min(32, reads.activeLimit))

  /** 原schema6恢复owner与callback装配；复用唯一permanent pump，不借旧Boot投影。 */
  def prepareDbFirst(callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      recovery:DbFirstBootActiveRecovery,pump:ForgePermanentMainPump,lease:PermanentPumpLease):MainDebugItemEndpoint =
    require(callbacks!=null && recovery!=null && pump!=null && lease!=null,"original schema6 endpoint dependencies required")
    recovery.requirePublicationOwner(callbacks)
    val adapter=callbacks.adapter
    val identity=adapter.currentExecutionIdentity
    require(lease.bootUuid==identity.bootId && lease.bootGeneration==identity.bootGeneration,
      "original schema6 pump/session identity required")
    val limit=callbacks.activeLimit
    val publication=recovery.publicationForEndpoint(callbacks,pump,lease)
    new MainDebugItemEndpoint(callbacks.actualServer,adapter,recovery.manager,recovery.tickets,publication,pump,lease,
      callbacks.currentSourceLimitProfile,drainBound=Math.min(32,limit),dbFirstCallbacks=Some(callbacks))
