package com.wi.floatingpod.server

import com.wi.floatingpod.debug.{DebugItemEndpoint, MainPodOperationRequest}
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoAdapter, MySqlOwnedIoIdentity, MySqlOwnedIoReply, MySqlOwnedRepositoryOperationPlan, MySqlRepositoryOperation}
import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoCommand, MySqlControlledDebugPrepareRequest, MySqlDbFirstInitialPreparationReservation, MySqlDbFirstCaptureReservation, MySqlDbFirstInitialPreparationConfirmation, MySqlDbFirstInitialPreparationDataView, MySqlDbFirstInitialPublicationReceipt, MySqlDbFirstPreparationFinalReceipt, MySqlDbFirstRejectedPreparationReceipt}
import java.util.UUID
import scala.collection.mutable
import scala.util.Success
import com.wi.floatingpod.storage.mysql.MySqlDbFirstPreparationRejectionConfirmation

private[server] enum MainDebugStage:
  case Reserved, Preparing, PreparationReadHandoff, RejectedPreparationReady, RejectedPreparationRetiring, Prepared, Committing, ChangingMode, Published, InitialRetiring, PreparationRetiring, RetainedFailure, Retired, NotAccepted

private[server] enum MainDebugPreparationSource:
  case NewPreparation,RecoveredPreparation

/** 已mint的Main数据责任；None仅表示原plan编码失败，绝不允许入队。 */
private[server] final case class MainDebugPlanReservation(stage: MainDebugStage, workflowUuid: UUID,
    tag: IoOperationTag, reserveOperationUuid: UUID, operation: MySqlRepositoryOperation,
    plan: Option[MySqlOwnedRepositoryOperationPlan])

/** Main有界责任账；只在实际submit Accepted后进入pending；completion先保留再由publication消费。 */
private[server] final class MainDebugOperationLedger(identity: MySqlOwnedIoIdentity, main: Thread, capacity: Int, initialProfile: SourceLimitProfile):
  private var actualProfile = initialProfile
  private var mainReferencesDropped = false
  require(identity != null && main != null && actualProfile != null && capacity >= 1 && capacity <= 256, "bounded exact Main ledger required")
  final class Entry private[MainDebugOperationLedger] (val request: MainPodOperationRequest):
    private[MainDebugOperationLedger] val plans = mutable.ArrayBuffer.empty[MainDebugPlanReservation]
    def planSnapshot: Vector[MainDebugPlanReservation] =
      owner(); plans.toVector
    private[MainDebugOperationLedger] val dbFirstStages = mutable.ArrayBuffer.empty[(MainDebugStage,IoOperationTag,Option[MySqlOwnedIoCommand])]
    def dbFirstCommandSnapshot:Vector[(MainDebugStage,IoOperationTag,Option[MySqlOwnedIoCommand])] =
      owner(); dbFirstStages.toVector
    private[MainDebugOperationLedger] var preparationRejection=Option.empty[MySqlDbFirstPreparationRejectionConfirmation]
    private[MainDebugOperationLedger] var rejectionDetached=false
    private[MainDebugOperationLedger] var preparationConfirmation=Option.empty[MySqlDbFirstInitialPreparationConfirmation]
    private[MainDebugOperationLedger] var preparationSource=MainDebugPreparationSource.NewPreparation
    private[MainDebugOperationLedger] var recoveredPreparedData=Option.empty[MySqlDbFirstInitialPreparationDataView]
    private[MainDebugOperationLedger] var initialReceipt=Option.empty[MySqlDbFirstInitialPublicationReceipt]
    private[MainDebugOperationLedger] var initialRetired=false
    private[MainDebugOperationLedger] var preparationFinal=Option.empty[MySqlDbFirstPreparationFinalReceipt]
    private[MainDebugOperationLedger] var preparationRetired=false
    private[MainDebugOperationLedger] var rejectedPreparation=Option.empty[MySqlDbFirstRejectedPreparationReceipt]
    var stage = MainDebugStage.Reserved
    var cause = Option.empty[Throwable]
    private[server] val completions = mutable.ArrayBuffer.empty[IoCompletion[MySqlOwnedIoReply]]
    private[server] var awaiting = Option.empty[IoOperationTag]
    private[MainDebugOperationLedger] var pendingArrival = 0L
    def completionSnapshot: Vector[IoCompletion[MySqlOwnedIoReply]] =
      owner(); completions.toVector
  private val entries = mutable.LinkedHashMap.empty[java.util.UUID, Entry]
  private val awaiting = mutable.HashMap.empty[IoOperationTag, Entry]
  private var retired = false
  private var nextArrival = 0L
  private var unmatched = Vector.empty[IoCompletion[MySqlOwnedIoReply]]
  private val completedByOrigin = mutable.LinkedHashMap.empty[MainPodOperationRequest.Origin, java.util.UUID]

  private def owner(): Unit =
    require(Thread.currentThread() eq main, "debug ledger requires exact Main owner")
  def reserve(request: DebugItemEndpoint.Request): Option[Entry] =
    reserve(new MainPodOperationRequest.Player(request))

  def reserve(request: MainPodOperationRequest): Option[Entry] =
    owner()
    require(request != null && request.boot() == identity.bootId, "exact boot request required")
    if retired || entries.size >= capacity || entries.contains(request.operation()) ||
      completedByOrigin.get(request.origin()).contains(request.operation()) then None
    else
      val value = new Entry(request)
      entries.put(request.operation(), value)
      Some(value)

  private def requireStage(entry: Entry, stage: MainDebugStage): Unit =
    require((stage == MainDebugStage.Preparing && entry.stage == MainDebugStage.Reserved) ||
      (stage == MainDebugStage.Committing && entry.stage == MainDebugStage.Prepared) ||
      (stage == MainDebugStage.ChangingMode && entry.stage == MainDebugStage.Reserved),
      "fixed one-shot stage transition required; no skipped prepare/retry")

  /** IDs和原operation先保留；canonical编码失败也不可重mint或遗失。 */
  /** 原纯DATA carrier入口不具session reservation，实际Endpoint不使用此路径。 */
  def reserveRepositoryPlan(entry: Entry, stage: MainDebugStage, workflow: UUID, tag: IoOperationTag,
      reserveOperation: UUID, operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    reservePlan(entry, stage, workflow, tag, reserveOperation, operation, None)

  private var actualOwnedAdapter = Option.empty[MySqlOwnedIoAdapter]

  def reserveOwnedRepositoryPlan(adapter: MySqlOwnedIoAdapter,
      entry: Entry, stage: MainDebugStage, workflow: UUID, tag: IoOperationTag,
      reserveOperation: UUID, operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    owner()
    require(adapter != null && adapter.identity == identity && actualOwnedAdapter.forall(_ eq adapter),
      "exact original owned adapter required; equal DTO identity cannot switch issuer")
    actualOwnedAdapter = Some(adapter)
    reservePlan(entry, stage, workflow, tag, reserveOperation, operation, Some(adapter))

  /** 先保存原stage/tag，再请求实际issuer；失败的None槽不可重mint或换旧plan。 */
  private def reserveDbFirstCommand(adapter:MySqlOwnedIoAdapter,entry:Entry,stage:MainDebugStage,
      tag:IoOperationTag)(create: => MySqlOwnedIoCommand):MySqlOwnedIoCommand =
    owner();owned(entry);identity.requireTag(tag);requireStage(entry,stage)
    require(adapter!=null && adapter.identity==identity && actualOwnedAdapter.forall(_ eq adapter),
      "exact original owned adapter required")
    require(!retired && entry.cause.isEmpty && entry.awaiting.isEmpty && entry.plans.isEmpty &&
      entry.request.action()==DebugItemEndpoint.Action.PLACE && entry.dbFirstStages.size<2 &&
      !entry.dbFirstStages.exists(value=>value._1==stage || value._2==tag),
      "one-shot DB-first placement stage cannot mix legacy plans or remint")
    actualOwnedAdapter=Some(adapter)
    entry.dbFirstStages += ((stage,tag,None))
    try
      val command=create
      entry.dbFirstStages(entry.dbFirstStages.size-1)=(stage,tag,Some(command))
      requirePlannedSubmission(entry,tag,stage)
      command
    catch case cause:Throwable=>
      fail(entry,cause)
      throw cause

  def reserveDbFirstPreparation(adapter:MySqlOwnedIoAdapter,entry:Entry,tag:IoOperationTag,
      request:MySqlControlledDebugPrepareRequest,activeLimit:Int):MySqlDbFirstInitialPreparationReservation =
    owner();owned(entry)
    require(request!=null && request.createOperationUuid==entry.request.operation(),
      "original debug request creation identity required")
    reserveDbFirstCommand(adapter,entry,MainDebugStage.Preparing,tag) {
      MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(adapter.reserveDbFirstInitialPreparation(tag,request,activeLimit))
    } match
      case MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(reservation)=>reservation
      case _=>throw new IllegalStateException("original preparation command missing")

  def reserveDbFirstInitialCapture(adapter:MySqlOwnedIoAdapter,entry:Entry,tag:IoOperationTag,
      activeLimit:Int):MySqlDbFirstCaptureReservation =
    owner();owned(entry)
    require(entry.preparationSource==MainDebugPreparationSource.NewPreparation && entry.preparationConfirmation.nonEmpty,
      "original same-boot preparation confirmation required")
    reserveInitialCapture(adapter,entry,tag,activeLimit,None)

  /** 真当前读代派生DATA进入新capture；没有本boot Prepare command/confirmation/退休责任。 */
  def reserveRecoveredInitialCapture(adapter:MySqlOwnedIoAdapter,entry:Entry,tag:IoOperationTag,
      activeLimit:Int,data:MySqlDbFirstInitialPreparationDataView):MySqlDbFirstCaptureReservation =
    owner();owned(entry)
    require(!retired && entry.cause.isEmpty && entry.stage==MainDebugStage.Reserved &&
      entry.awaiting.isEmpty && entry.plans.isEmpty && entry.dbFirstStages.isEmpty &&
      entry.preparationConfirmation.isEmpty && entry.recoveredPreparedData.isEmpty && data!=null &&
      entry.request.origin().kind()==MainPodOperationRequest.OriginKind.LOCAL_CONSOLE_BOOT &&
      data.receipt.createOperationUuid.contains(entry.request.operation()) &&
      adapter!=null && adapter.identity==identity && adapter.canReserveDbFirstInitialPreparation,
      "actual empty new-boot console capture and original committed-create DATA required")
    entry.preparationSource=MainDebugPreparationSource.RecoveredPreparation
    entry.stage=MainDebugStage.Prepared
    reserveInitialCapture(adapter,entry,tag,activeLimit,Some(data))

  private def reserveInitialCapture(adapter:MySqlOwnedIoAdapter,entry:Entry,tag:IoOperationTag,
      activeLimit:Int,recovered:Option[MySqlDbFirstInitialPreparationDataView]):MySqlDbFirstCaptureReservation =
    require(recovered.nonEmpty==(entry.preparationSource==MainDebugPreparationSource.RecoveredPreparation),
      "actual preparation origin and capture metadata must agree")
    require(activeLimit>=1 && activeLimit<=256,"explicit configured initial active limit required")
    reserveDbFirstCommand(adapter,entry,MainDebugStage.Committing,tag) {
      val reservation=adapter.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      recovered.foreach(data=>entry.recoveredPreparedData=Some(adapter.retainRecoveredInitialData(reservation,data)))
      MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,activeLimit)
    } match
      case MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,_)=>reservation
      case _=>throw new IllegalStateException("original initial capture command missing")

  def dbFirstCommandForSubmission(entry:Entry,tag:IoOperationTag,stage:MainDebugStage):MySqlOwnedIoCommand =
    requirePlannedSubmission(entry,tag,stage)
    entry.dbFirstStages.find(value=>value._1==stage && value._2==tag).flatMap(_._3).getOrElse(
      throw new IllegalStateException("original DB-first stage command missing"))

  private def preparationReservation(entry:Entry):MySqlDbFirstInitialPreparationReservation =
    entry.dbFirstStages.find(_._1==MainDebugStage.Preparing).flatMap(_._3) match
      case Some(MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(reservation))=>reservation
      case _=>throw new IllegalStateException("original retained preparation reservation missing")

  def preparationReadRetryReady(entry:Entry):Boolean =
    owner();owned(entry)
    !retired && entry.cause.isEmpty && entry.stage==MainDebugStage.PreparationReadHandoff &&
      entry.awaiting.isEmpty && entry.pendingArrival==0L && entry.completions.size==1 &&
      (entry.preparationConfirmation.nonEmpty || entry.preparationRejection.nonEmpty)

  /** 原业务完成已claim后才发同tag读代交接；Rejected可重提控制，UNKNOWN不可重发。 */
  def submitPreparationReadHandoff(adapter:MySqlOwnedIoAdapter,entry:Entry):Boolean =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && !retired && entry.cause.isEmpty &&
      entry.awaiting.isEmpty && entry.pendingArrival==0L && entry.completions.size==1 &&
      (entry.stage==MainDebugStage.Preparing || entry.stage==MainDebugStage.PreparationReadHandoff),
      "original claimed preparation completion required before read handoff")
    val reservation=preparationReservation(entry)
    val tag=reservation.tag
    require(entry.completions.head.tag==tag && !awaiting.contains(tag),"original preparation completion tag required")
    try
      if entry.preparationConfirmation.isEmpty && entry.preparationRejection.isEmpty then
        entry.completions.head.result match
          case Success(MySqlOwnedIoReply.DbFirstInitialPreparationRejected(_))=>
            entry.preparationRejection=Some(adapter.claimPreparationRejection(reservation,entry.completions.head))
          case _=>entry.preparationConfirmation=Some(adapter.claimDbFirstInitialPreparationConfirmation(reservation,entry.completions.head))
      entry.stage=MainDebugStage.PreparationReadHandoff
      val result=entry.preparationRejection match
        case Some(rejected)=>adapter.submitRejectedPreparationReadRelease(reservation,rejected)
        case None=>adapter.submitPreparationMainReadRelease(reservation,entry.preparationConfirmation.get)
      if result==IoSubmission.Accepted then
        entry.awaiting=Some(tag);awaiting.put(tag,entry);true
      else false
    catch case cause:Throwable=>
      fail(entry,cause);throw cause

  /** 同tag第二次实际完成由原awaiting槽匹配，不能跳过worker退休或拿业务完成充当ACK。 */
  def consumePreparationReadHandoff(adapter:MySqlOwnedIoAdapter,entry:Entry,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && !retired && entry.cause.isEmpty &&
      entry.stage==MainDebugStage.PreparationReadHandoff && entry.awaiting.isEmpty && entry.pendingArrival==0L &&
      entry.completions.size==2 && (entry.completions.last eq completion) &&
      (entry.preparationConfirmation.nonEmpty || entry.preparationRejection.nonEmpty),
      "original claimed preparation read handoff ACK required")
    try
      completion.result.failed.foreach(throw _) // 原worker失败优先保留，不能被缺ACK资格错误覆盖。
      entry.preparationRejection match
        case Some(rejected)=>
          adapter.finishRejectedPreparationReadRelease(preparationReservation(entry),rejected,completion)
          entry.stage=MainDebugStage.RejectedPreparationReady
        case None=>
          adapter.finishPreparationMainReadRelease(preparationReservation(entry),entry.preparationConfirmation.get,completion)
          entry.stage=MainDebugStage.Prepared
    catch case cause:Throwable=>
      fail(entry,cause);throw cause

  def settledRejectionReady(entry:Entry):Boolean =
    owner();owned(entry)
    !retired && entry.cause.isEmpty && entry.awaiting.isEmpty && entry.pendingArrival==0L &&
      entry.preparationRejection.nonEmpty &&
      (entry.stage==MainDebugStage.RejectedPreparationReady || entry.stage==MainDebugStage.RejectedPreparationRetiring)

  def submitSettledRejectionRetirement(adapter:MySqlOwnedIoAdapter,entry:Entry):Unit =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && settledRejectionReady(entry),"original settled rejection retirement required")
    val confirmation=entry.preparationRejection.get
    require(!awaiting.contains(confirmation.tag),"original rejection control already pending")
    try
      if !entry.rejectionDetached then
        adapter.detachRejectedPreparation(confirmation)
        entry.rejectionDetached=true
        entry.dbFirstStages.clear();entry.completions.clear()
      entry.stage=MainDebugStage.RejectedPreparationRetiring
      val result=adapter.submitRejectedPreparationRetirement(confirmation)
      if result==IoSubmission.Accepted then
        entry.awaiting=Some(confirmation.tag);awaiting.put(confirmation.tag,entry)
    catch case cause:Throwable=>
      fail(entry,cause)
      throw cause

  def consumeSettledRejectionRetirement(adapter:MySqlOwnedIoAdapter,entry:Entry,
      completion:IoCompletion[MySqlOwnedIoReply]):Throwable =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && !retired && entry.cause.isEmpty && entry.rejectionDetached &&
      entry.stage==MainDebugStage.RejectedPreparationRetiring && entry.awaiting.isEmpty && entry.pendingArrival==0L &&
      entry.completions.size==1 && (entry.completions.head eq completion),"original rejected final completion required")
    try
      completion.result.failed.foreach(throw _)
      val confirmation=entry.preparationRejection.get
      adapter.finishRejectedPreparationRetirement(confirmation,completion)
      entry.stage=MainDebugStage.RejectedPreparationReady
      confirmation.cause
    catch case cause:Throwable=>
      fail(entry,cause)
      throw cause

  def acknowledgedSettledRejection(entry:Entry):Unit =
    owner();owned(entry)
    require(!retired && entry.cause.isEmpty && entry.stage==MainDebugStage.RejectedPreparationReady &&
      entry.rejectionDetached && entry.awaiting.isEmpty && entry.pendingArrival==0L && entry.completions.size==1 &&
      entry.dbFirstStages.isEmpty,"only retired rejected failure feedback may compact")
    completedByOrigin.put(entry.request.origin(),entry.request.operation())
    while completedByOrigin.size>capacity do completedByOrigin.remove(completedByOrigin.head._1)
    entry.preparationRejection=None;entry.completions.clear();entries.remove(entry.request.operation())
    ()

  def dbFirstPreparedData(entry:Entry):MySqlDbFirstInitialPreparationDataView =
    owner();owned(entry)
    require(!retired && entry.cause.isEmpty && entry.stage==MainDebugStage.Prepared &&
      entry.awaiting.isEmpty && entry.preparationSource==MainDebugPreparationSource.NewPreparation &&
      entry.preparationConfirmation.nonEmpty,"completed original preparation read handoff required")
    entry.preparationConfirmation.get.preparedData(preparationReservation(entry))

  def isRecoveredPreparation(entry:Entry):Boolean =
    owner();owned(entry);entry.preparationSource==MainDebugPreparationSource.RecoveredPreparation

  def initialRetirementReady(entry:Entry):Boolean =
    owner();owned(entry)
    !retired && entry.cause.isEmpty && !entry.initialRetired && entry.awaiting.isEmpty &&
      entry.pendingArrival==0L && (entry.preparationConfirmation.nonEmpty || isRecoveredPreparation(entry)) &&
      (entry.stage==MainDebugStage.Published || entry.stage==MainDebugStage.InitialRetiring)

  /** manager先发行原发布回执；再放下ledger持有的capture与worker completion，才能提交退休。 */
  def detachInitialPayload(entry:Entry,receipt:MySqlDbFirstInitialPublicationReceipt):Unit =
    owner();owned(entry)
    require(initialRetirementReady(entry) && receipt!=null,"healthy original published initial retirement required")
    if entry.initialReceipt.nonEmpty then
      require(entry.initialReceipt.exists(_ eq receipt),"original initial receipt cannot change")
    else
      require(entry.dbFirstStages.exists { case (stage,tag,command)=>
        stage==MainDebugStage.Committing && tag==receipt.tag && command.exists {
          case MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,_)=>reservation.tag==receipt.tag
          case _=>false
        }
      },"original published capture stage required")
      entry.initialReceipt=Some(receipt)
      entry.dbFirstStages.filterInPlace(_._1!=MainDebugStage.Committing)
      entry.completions.filterInPlace(_.tag!=receipt.tag)
      entry.recoveredPreparedData=None
    entry.stage=MainDebugStage.InitialRetiring

  def initialRetirementSubmitted(entry:Entry,receipt:MySqlDbFirstInitialPublicationReceipt,result:IoSubmission):Unit =
    owner();owned(entry)
    require(initialRetirementReady(entry) && entry.stage==MainDebugStage.InitialRetiring &&
      entry.initialReceipt.exists(_ eq receipt) && result!=null && !awaiting.contains(receipt.tag),
      "original detached initial retirement submission required")
    if result==IoSubmission.Accepted then
      entry.awaiting=Some(receipt.tag);awaiting.put(receipt.tag,entry)

  def consumeInitialRetirement(entry:Entry,completion:IoCompletion[MySqlOwnedIoReply])
      (consume: => MainControlledPublicationObservation):MainControlledPublicationObservation =
    owner();owned(entry)
    require(!retired && entry.cause.isEmpty && entry.stage==MainDebugStage.InitialRetiring &&
      entry.awaiting.isEmpty && entry.pendingArrival==0L && entry.initialReceipt.exists(_.tag==completion.tag) &&
      entry.completions.lastOption.exists(_ eq completion),"original claimed initial retirement completion required")
    try
      val result=consume
      require(result!=null && result.cause.isEmpty && result.status==MainControlledPublicationStatus.Published,
        "actual initial retirement publication result required")
      entry.completions.filterInPlace(_.tag!=completion.tag)
      entry.initialRetired=true;entry.stage=MainDebugStage.Published
      result
    catch case cause:Throwable=>
      fail(entry,cause);throw cause

  def preparationFinalReady(entry:Entry):Boolean =
    owner();owned(entry)
    !retired && entry.cause.isEmpty && entry.initialRetired && !entry.preparationRetired &&
      entry.preparationSource==MainDebugPreparationSource.NewPreparation &&
      entry.awaiting.isEmpty && entry.pendingArrival==0L &&
      (entry.stage==MainDebugStage.Published || entry.stage==MainDebugStage.PreparationRetiring)

  def detachPreparationPayload(adapter:MySqlOwnedIoAdapter,entry:Entry):MySqlDbFirstPreparationFinalReceipt =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && preparationFinalReady(entry),
      "original initial-retired preparation final boundary required")
    if entry.preparationFinal.isEmpty then
      val confirmation=entry.preparationConfirmation.getOrElse(throw new IllegalStateException("original preparation confirmation missing"))
      val receipt=adapter.recordDbFirstPreparationFinal(confirmation)
      entry.preparationFinal=Some(receipt)
      entry.dbFirstStages.clear();entry.completions.clear();entry.preparationConfirmation=None
    entry.stage=MainDebugStage.PreparationRetiring
    entry.preparationFinal.get

  def preparationFinalSubmitted(entry:Entry,receipt:MySqlDbFirstPreparationFinalReceipt,result:IoSubmission):Unit =
    owner();owned(entry)
    require(preparationFinalReady(entry) && entry.stage==MainDebugStage.PreparationRetiring &&
      entry.preparationFinal.exists(_ eq receipt) && result!=null && !awaiting.contains(receipt.tag),
      "original detached preparation final submission required")
    if result==IoSubmission.Accepted then
      entry.awaiting=Some(receipt.tag);awaiting.put(receipt.tag,entry)

  def consumePreparationFinal(adapter:MySqlOwnedIoAdapter,entry:Entry,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && !retired && entry.cause.isEmpty &&
      entry.stage==MainDebugStage.PreparationRetiring && entry.initialRetired && entry.awaiting.isEmpty &&
      entry.pendingArrival==0L && entry.preparationFinal.exists(_.tag==completion.tag) &&
      entry.completions.lastOption.exists(_ eq completion),"original claimed preparation final ACK required")
    try
      adapter.finishDbFirstPreparationFinalRelease(entry.preparationFinal.get,completion)
      entry.completions.clear();entry.preparationRetired=true;entry.stage=MainDebugStage.Published
    catch case cause:Throwable=>
      fail(entry,cause);throw cause

  def rejectedPreparationReady(entry:Entry):Boolean =
    owner();owned(entry)
    !retired && entry.cause.isEmpty && entry.stage==MainDebugStage.NotAccepted && entry.awaiting.isEmpty &&
      entry.completions.isEmpty && entry.plans.isEmpty &&
      (entry.rejectedPreparation.nonEmpty || entry.dbFirstStages.exists(_._1==MainDebugStage.Preparing))

  def settleRejectedPreparation(adapter:MySqlOwnedIoAdapter,entry:Entry):Unit =
    owner();owned(entry)
    require(actualOwnedAdapter.exists(_ eq adapter) && rejectedPreparationReady(entry),
      "original unaccepted preparation Entry required")
    try
      if entry.rejectedPreparation.isEmpty then
        val receipt=adapter.recordRejectedPreparationDrop(preparationReservation(entry))
        entry.rejectedPreparation=Some(receipt)
        entry.dbFirstStages.clear() // 回执先保存，原request/canonical backing的最后Main持有先撤下。
      adapter.cancelRejectedPreparation(entry.rejectedPreparation.get)
      entries.remove(entry.request.operation())
      ()
    catch case cause:Throwable=>
      fail(entry,cause);throw cause

  private def reservePlan(entry: Entry, stage: MainDebugStage, workflow: UUID, tag: IoOperationTag,
      reserveOperation: UUID, operation: MySqlRepositoryOperation,
      adapter: Option[MySqlOwnedIoAdapter]): MySqlOwnedRepositoryOperationPlan =
    owner(); owned(entry); identity.requireTag(tag); requireStage(entry, stage)
    require(!retired && entry.cause.isEmpty && entry.dbFirstStages.isEmpty && entry.awaiting.isEmpty && workflow != null && reserveOperation != null &&
      operation != null && reserveOperation != tag.operationId, "fixed original stage IDs and operation required")
    require(entry.plans.size < (if entry.request.action() == DebugItemEndpoint.Action.PLACE then 2 else 1) &&
      !entry.plans.exists(_.stage == stage), "bounded stage is already reserved; no remint")
    require(entry.plans.forall(value => value.workflowUuid == workflow && value.tag.operationId != tag.operationId &&
      value.reserveOperationUuid != reserveOperation && value.tag.operationId != reserveOperation &&
      value.reserveOperationUuid != tag.operationId), "workflow must remain fixed and stage reserve/execute IDs distinct")
    val original = MainDebugPlanReservation(stage, workflow, tag, reserveOperation, operation, None)
    entry.plans += original
    val plan = adapter match
      case Some(actual) => actual.reserveRepositoryPlan(tag, workflow, reserveOperation, operation)
      case None => MySqlOwnedRepositoryOperationPlan.single(tag, workflow, reserveOperation, operation, actualProfile)
    entry.plans(entry.plans.size - 1) = original.copy(plan = Some(plan))
    requirePlannedSubmission(entry, tag, stage)
    plan

  /** Endpoint必须在实际adapter.submit求值前调用；本检查本身无IO。 */
  def requirePlannedSubmission(entry: Entry, tag: IoOperationTag, stage: MainDebugStage): Unit =
    owner(); owned(entry); identity.requireTag(tag); requireStage(entry, stage)
    require(!retired && entry.awaiting.isEmpty && !awaiting.contains(tag), "one exact outstanding planned command required")
    require(entry.cause.isEmpty,"failed original stage cannot be submitted")
    if entry.dbFirstStages.nonEmpty then
      require(entry.plans.isEmpty,"DB-first stage cannot borrow legacy plan")
      val command=entry.dbFirstStages.find(value=>value._1==stage && value._2==tag).flatMap(_._3).getOrElse(
        throw new IllegalStateException("failed or absent original DB-first stage cannot be submitted"))
      command match
        case MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(reservation)=>
          require(stage==MainDebugStage.Preparing && reservation!=null && reservation.tag==tag,
            "exact original preparation stage required")
        case MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,_)=>
          require(stage==MainDebugStage.Committing && reservation!=null && reservation.tag==tag &&
            reservation.purpose==DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION,"exact original initial stage required")
        case _=>throw new IllegalStateException("unsupported DB-first debug stage")
    else
      val original = entry.plans.find(value => value.stage == stage && value.tag == tag).getOrElse(
        throw new IllegalStateException("stage plan must be retained before actual submission"))
      val plan = original.plan.getOrElse(throw new IllegalStateException("failed original plan cannot be submitted"))
      plan.requireTag(tag)
      plan.requireSingle(original.operation, actualProfile)

  /** 调用者必须先reserve，再actualsubmit；capacity拒绝不是Accepted。 */
  def submitted(entry: Entry, tag: IoOperationTag, result: IoSubmission, stage: MainDebugStage): Boolean =
    requirePlannedSubmission(entry, tag, stage)
    require(result != null && entry.awaiting.isEmpty && !awaiting.contains(tag), "one outstanding exact command per entry")
    result match
      case IoSubmission.Accepted =>
        require(!retired, "retired ledger cannot submit")
        entry.awaiting = Some(tag); entry.stage = stage; awaiting.put(tag, entry); true
      case _ =>
        entry.stage = if entry.completions.nonEmpty || stage != MainDebugStage.Preparing then MainDebugStage.RetainedFailure
          else MainDebugStage.NotAccepted
        false

  /** 完整drain batch先一次retain，不能中途publication异常丢失后面的实际完成。 */
  def retainBatch(batch: Vector[IoCompletion[MySqlOwnedIoReply]]): Vector[Entry] =
    owner()
    require(batch != null && batch.forall(_ != null), "actual immutable completion batch required")
    val retained = Vector.newBuilder[Entry]
    batch.foreach { completion =>
      awaiting.get(completion.tag) match
        case Some(entry) =>
          require(entry.awaiting.contains(completion.tag), "exact outstanding completion mismatch")
          require(entry.pendingArrival == 0L, "previous completion must be claimed before another arrival")
          val arrival = Math.incrementExact(nextArrival)
          awaiting.remove(completion.tag)
          entry.awaiting = None
          entry.completions += completion
          entry.pendingArrival = arrival
          nextArrival = arrival
          if retired then entry.stage = MainDebugStage.Retired
          retained += entry
        case None =>
          // 异常完成保留原对象并停止新业务；不丢弃或伪当已处理。
          unmatched :+= completion
          retire() // 同步撤销全部Entry的publication/ACK资格，不能只设置全局flag。
    }
    require(unmatched.size <= capacity, "unmatched completion responsibility exceeded owned mailbox bound")
    retained.result()

  /** 只快照有界原Entry引用；实际到达顺序不等于请求预留顺序。 */
  def pendingCompletionEntries(limit: Int): Vector[Entry] =
    owner(); require(limit > 0 && limit <= capacity, "bounded completion dispatch required")
    entries.valuesIterator.filter(_.pendingArrival > 0L).toVector.sortBy(_.pendingArrival).take(limit)

  /** 只撤一次派发资格；原history/plan继续由原状态与ACK持有。 */
  def claimPendingCompletion(entry: Entry): IoCompletion[MySqlOwnedIoReply] =
    owner(); owned(entry)
    require(!retired && entry.cause.isEmpty && entry.pendingArrival > 0L && entry.awaiting.isEmpty &&
      entry.completions.nonEmpty, "exact healthy retained completion required once")
    val result = entry.completions.last
    entry.pendingArrival = 0L
    result

  def prepared(entry: Entry): Unit =
    owner(); owned(entry)
    require(entry.dbFirstStages.isEmpty && entry.stage == MainDebugStage.Preparing && entry.awaiting.isEmpty && entry.completions.size == 1,
      "actual prepare completion must already be consumed")
    entry.stage = MainDebugStage.Prepared
  def requireRecovery(entry: Entry): Unit =
    owner(); owned(entry)
    require(entry.awaiting.isEmpty && entry.completions.nonEmpty, "actual completed history must be retained")
    entry.stage = MainDebugStage.RetainedFailure // 正常已提交replay需要recovery；没有伪造异常/cause。
  def fail(entry: Entry, cause: Throwable): Unit =
    owner(); owned(entry); require(cause != null, "original failure required")
    entry.pendingArrival = 0L
    if entry.cause.isEmpty then entry.cause = Some(cause)
    else if entry.cause.get ne cause then entry.cause.get.addSuppressed(cause)
    entry.stage = if retired then MainDebugStage.Retired else MainDebugStage.RetainedFailure
  def published(entry: Entry): Unit =
    owner(); owned(entry)
    require(!retired && entry.awaiting.isEmpty && entry.cause.isEmpty && entry.completions.nonEmpty &&
      (entry.stage == MainDebugStage.Committing || entry.stage == MainDebugStage.ChangingMode),
      "publication requires consumed healthy final command completion")
    entry.stage = MainDebugStage.Published
  /** 仅publication与原connection feedback均返回后compact；失败及late绝不移除。 */
  def acknowledged(entry: Entry): Unit =
    owner(); owned(entry); require(!retired && entry.dbFirstStages.isEmpty && entry.stage == MainDebugStage.Published &&
      (entry.initialReceipt.isEmpty || (entry.initialRetired &&
        (entry.preparationRetired || isRecoveredPreparation(entry)))),
      "only non-retired published response may compact")
    completedByOrigin.put(entry.request.origin(), entry.request.operation())
    while completedByOrigin.size > capacity do completedByOrigin.remove(completedByOrigin.head._1)
    entry.plans.clear()
    entry.completions.clear()
    entries.remove(entry.request.operation())
  def releaseNotAccepted(entry: Entry): Unit =
    owner(); owned(entry); require(entry.dbFirstStages.isEmpty && entry.stage == MainDebugStage.NotAccepted && entry.completions.isEmpty,
      "only no-physical prepare rejection has no accepted responsibility")
    entry.plans.clear()
    entry.completions.clear()
    entries.remove(entry.request.operation())
  private[server] def requireMainDrainSettled(): Unit =
    owner()
    require(!retired && !mainReferencesDropped, "original live Main ledger required")
    retainedFailureCauses.headOption.foreach(throw _)
    require(entries.isEmpty && awaiting.isEmpty && unmatched.isEmpty, "original Main Entry/awaiting/completion obligations remain")
  private[server] def dropSettledMainReferences(): Unit =
    requireMainDrainSettled()
    completedByOrigin.clear()
    actualOwnedAdapter = None
    actualProfile = null
    mainReferencesDropped = true
    retired = true
  def retire(): Unit =
    owner(); retired = true; entries.values.foreach(_.stage = MainDebugStage.Retired)
  def isRetired: Boolean = { owner(); retired }
  def retainedFailureCauses: Vector[Throwable] = { owner(); entries.valuesIterator.flatMap(_.cause).toVector }
  def retainedCount: Int = { owner(); entries.size }
  def unmatchedCompletions: Vector[IoCompletion[MySqlOwnedIoReply]] = { owner(); unmatched }
  private def owned(entry: Entry): Unit =
    require(entry != null && entries.get(entry.request.operation()).exists(_ eq entry), "foreign/compacted ledger entry")
