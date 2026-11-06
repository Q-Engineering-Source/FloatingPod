package com.wi.floatingpod.server

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.domain.{FloatingPodState,RecallRefusalReason,RecallRequestNamespace}
import com.wi.floatingpod.network.*
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
import com.wi.floatingpod.storage.mysql.*
import java.util.UUID
import net.minecraft.entity.player.EntityPlayerMP
import net.minecraft.server.MinecraftServer
import net.minecraftforge.fml.common.network.simpleimpl.{IMessage, MessageContext}
import org.apache.logging.log4j.LogManager
import scala.collection.mutable
import scala.util.control.NonFatal

/** 原Main DATA预览只决定正常拒绝；Matched本身不发行mutation或成功资格。 */
private[server] object DbFirstPlayerRecallPolicy:
  /** 网络错误ID不进入持久格式；仅允许实际pre-capture业务拒绝，基础设施失败仍保UNKNOWN责任。 */
  def durableReason(reason:RecallFailureReason):RecallRefusalReason = reason match
    case RecallFailureReason.NotOwner=>RecallRefusalReason.NotOwner
    case RecallFailureReason.VersionMismatch=>RecallRefusalReason.VersionMismatch
    case RecallFailureReason.NotActive=>RecallRefusalReason.NotActive
    case RecallFailureReason.WrongDimension=>RecallRefusalReason.WrongDimension
    case RecallFailureReason.UnknownPod=>RecallRefusalReason.UnknownPod
    case RecallFailureReason.Busy=>RecallRefusalReason.Busy
    case _=>throw new IllegalArgumentException("only actual pre-capture business refusal can be durable")
  def networkReason(reason:RecallRefusalReason):RecallFailureReason =
    require(reason!=null,"actual durable refusal reason required")
    reason match
      case RecallRefusalReason.NotOwner=>RecallFailureReason.NotOwner
      case RecallRefusalReason.VersionMismatch=>RecallFailureReason.VersionMismatch
      case RecallRefusalReason.NotActive=>RecallFailureReason.NotActive
      case RecallRefusalReason.WrongDimension=>RecallFailureReason.WrongDimension
      case RecallRefusalReason.UnknownPod=>RecallFailureReason.UnknownPod
      case RecallRefusalReason.Busy=>RecallFailureReason.Busy
  /** 原J等待后的当前Main标量拒绝；None仍不是freeze/capture或SQL资格。 */
  def afterJournal(actor:UUID,pod:UUID,version:Long,current:Option[FloatingPodState],
      ready:Boolean,mayBegin:Boolean,connectedDimension:Option[Int]):Option[RecallFailureReason] =
    require(actor!=null && pod!=null && version>=0L && current!=null && connectedDimension!=null,
      "actual original journal identity and current Main observations required")
    current.foreach(state=>require(state.uuid==pod,"foreign current Main pod"))
    if !ready || !mayBegin then Some(RecallFailureReason.Busy)
    else originalStateRefusal(actor,pod,version,current,connectedDimension)
  /** 原请求的当前标量业务条件；不代替Ready、实际Main/body或SQL资格。 */
  def originalStateRefusal(actor:UUID,pod:UUID,version:Long,current:Option[FloatingPodState],
      connectedDimension:Option[Int]):Option[RecallFailureReason] =
    require(actor!=null && pod!=null && version>=0L && current!=null && connectedDimension!=null,
      "actual original identity and current state required")
    current.foreach(state=>require(state.uuid==pod,"foreign current Main pod"))
    current match
      case None=>Some(RecallFailureReason.UnknownPod)
      case Some(state) if !state.ownerUuid.contains(actor)=>Some(RecallFailureReason.NotOwner)
      case Some(state) if state.stateVersion!=version=>Some(RecallFailureReason.VersionMismatch)
      case Some(state) if !state.isActive=>Some(RecallFailureReason.NotActive)
      case Some(state) if connectedDimension.exists(dimension => !state.dimensionId.contains(dimension))=>
        Some(RecallFailureReason.WrongDimension)
      case _=>None
  def refusal(decision: MySqlRequestReadDecision): Option[RecallFailureReason] =
    require(decision != null, "actual current read decision required")
    decision.ownership match
      case MySqlRequestOwnershipPreview.Missing => Some(RecallFailureReason.UnknownPod)
      case MySqlRequestOwnershipPreview.NotOwner => Some(RecallFailureReason.NotOwner)
      case MySqlRequestOwnershipPreview.VersionMismatch(_) => Some(RecallFailureReason.VersionMismatch)
      case MySqlRequestOwnershipPreview.Matched(_, false) => Some(RecallFailureReason.NotActive)
      case MySqlRequestOwnershipPreview.Matched(_, true) => None

/** 玩家收回的原Main责任；worker仅得到原reservation，实体/连接不越过Main边界。 */
private[server] final class DbFirstPlayerRecallOwner(
    server: MinecraftServer, adapter: MySqlOwnedIoAdapter,
    manager: ActiveFloatingPodManager, val publication: ActiveFloatingPodManager#MainPublicationScope,
    reads: DbFirstMainRequestReadBridge, boot:DbFirstBootActiveRecovery,ready: () => Boolean, capacity: Int):
  require(server != null && adapter != null && manager != null && publication != null && reads != null &&
    boot!=null && ready != null && capacity > 0 && capacity <= 256, "original bounded player recall dependencies required")
  boot.requireResultSource(server,adapter)
  private val main = server.getServerThread
  private val logger = LogManager.getLogger(getClass)
  private val sender = ForgeServerPacketSender()
  private val results=new MainRecallResultCustody(main,capacity)
  private final class Pending(val tag: IoOperationTag, originalPlayer: EntityPlayerMP,
      val pod: UUID, val expectedVersion: Long, originalRequest: Option[UUID],
      val durableNamespace:Option[RecallRequestNamespace]=None):
    val actor: UUID = originalPlayer.getUniqueID
    val feedback = new MainRecallFeedbackCorrelation(main, actor, pod, expectedVersion, tag.operationId, originalRequest)
    private var feedbackPlayer = originalPlayer
    private var feedbackConnection = originalPlayer.connection
    def player: EntityPlayerMP = feedbackPlayer
    def attachFeedback(authenticatedPlayer: EntityPlayerMP, request: UUID): Boolean =
      if !feedback.attachAuthenticated(authenticatedPlayer.getUniqueID, pod, expectedVersion, request) then false
      else
        feedbackPlayer = authenticatedPlayer
        feedbackConnection = authenticatedPlayer.connection
        true
    var resultSlot=Option.empty[results.Slot]
    var reservation = Option.empty[MySqlDbFirstCaptureReservation]
    var entry = Option.empty[publication.DbFirstRecallEntry]
    var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var journal=Option.empty[DbFirstRecallJournalMain]
    // 无UUID兼容路径直接拥有C；带UUID的新旧协议都须先取得原J真实ACK才可暴露C。
    var mayExposeOperation=durableNamespace.isEmpty && originalRequest.isEmpty
    var refusalReason=Option.empty[RecallFailureReason]
    var captured = false
    var confirmed = false
    var releaseSubmitted = false
    val retainedEnd = new DbFirstRecallRetainedEnd
    var cause = Option.empty[Throwable]
    var failureFeedbackAttempted = false
    def connected: Boolean = (player.getServer eq server) && player.getUniqueID == actor &&
      (player.connection eq feedbackConnection) && (server.getPlayerList.getPlayerByUUID(actor) eq player)
  private val pending = mutable.LinkedHashMap.empty[IoOperationTag, Pending]
  private def owner(): Unit =
    require((Thread.currentThread() eq main) && server.isCallingFromMinecraftThread, "original player recall Main required")
  private def authenticated(context: MessageContext): EntityPlayerMP =
    owner()
    require(context != null && context.getServerHandler != null, "actual C2S server handler required")
    val handler = context.getServerHandler
    val player = handler.player
    require(player != null && (player.getServer eq server) && (player.connection eq handler) &&
      (server.getPlayerList.getPlayerByUUID(player.getUniqueID) eq player), "original authenticated C2S player required")
    player
  private def send(player: EntityPlayerMP, message: IMessage): Unit = sender.send(new ForgeServerPlayerContext(player), message)
  private def refuse(player: EntityPlayerMP, pod: UUID, reason: RecallFailureReason): Unit =
    send(player, S2CRecallResult.failed(pod, reason))

  /** Release尚未接schema6 mutation；明确未接受，绝不转入旧同步SQLite/SQL路径。 */
  def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit =
    val player = authenticated(context)
    require(message != null && message.podUuid != null, "decoded release request required")
    send(player, S2CReleaseResult.failed(message.podUuid, ReleaseFailureReason.Busy))

  def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit =
    val player = authenticated(context)
    require(message != null && message.podUuid != null && message.expectedStateVersion >= 0L,
      "decoded recall request required")
    receiveChecked(message, player, None)

  def receiveRecallOperation(message: C2SRecallOperationRequest, context: MessageContext): Unit =
    val player = authenticated(context)
    require(message != null && message.requestUuid != null && message.podUuid != null && message.expectedStateVersion >= 0L,
      "decoded correlated recall request required")
    receiveChecked(new C2SRecallRequest(message.podUuid, message.expectedStateVersion), player, Some(message.requestUuid))

  /** 新消息仍未注册；发布只取同一当前Main binding与实际handler认证actor。 */
  def receiveRecallNamespace(message:C2SRecallNamespaceRequest,context:MessageContext):Unit =
    val player=authenticated(context)
    require(message!=null,"decoded recall namespace query required")
    send(player,new S2CRecallNamespace(reads.recallNamespace(player)))

  /** 只释放已收到的标量结果；实际认证namespace之前不信任客户端回执。 */
  def receiveRecallResultReceived(message:C2SRecallResultReceived,context:MessageContext):Unit =
    val player=authenticated(context)
    require(message!=null,"decoded result receipt required");message.validate()
    if reads.recallNamespace(player)==message.result.namespace && results.received(message.result) then
      send(player,new S2CRecallResultReleased(message.result))

  /** 显式原UUID只读观察：不reserve、capture、推进END、重发或更换回馈owner。 */
  def receiveRecallResume(message:C2SRecallResumeQuery,context:MessageContext):Unit =
    val player=authenticated(context)
    require(message!=null,"decoded original recall query required");message.validate()
    val actualNamespace=reads.recallNamespace(player)
    if actualNamespace==message.namespace then
      val terminal=results.original(actualNamespace,message.operationUuid)
      terminal.foreach(slot=>send(player,slot.status))
      if terminal.nonEmpty then return
    reads.lookupRecallOriginalOperation(player,message.namespace,message.operationUuid) match
      case MySqlDbFirstRecallRequestLookup.NamespaceMismatch(actual)=>
        send(player,new S2CRecallResumeResult(actual,message.operationUuid,RecallResumeLookup.NamespaceMismatch))
      case MySqlDbFirstRecallRequestLookup.NotFound(actual)=>
        send(player,new S2CRecallResumeResult(actual,message.operationUuid,RecallResumeLookup.NotFound))
      case MySqlDbFirstRecallRequestLookup.Found(actual,original)=>
        send(player,new S2CRecallResumeResult(actual,message.operationUuid,
          RecallResumeLookup.Located(original.podUuid,original.expectedStateVersion,original.clientRequestUuid)))
        results.original(actual,original.originalTag.operationId) match
          case Some(slot)=>
            require(slot.tag==original.originalTag && slot.pod==original.podUuid &&
              slot.version==original.expectedStateVersion && original.clientRequestUuid.contains(slot.request),
              "durable metadata differs from actual final ACK custody")
            send(player,slot.status)
          case None=>()
        val restored=boot.recoveredResult(actual,original)
        restored.foreach(status=>send(player,status))
        if restored.nonEmpty then return
        // 只读原live owner的实际Main观察，不据持久phase发terminal，也不retarget原连接。
        pending.get(original.originalTag).filter(work=>work.actor==player.getUniqueID &&
          work.durableNamespace.contains(actual) && work.pod==original.podUuid &&
          work.expectedVersion==original.expectedStateVersion).foreach { work=>
          original.clientRequestUuid.filter(work.feedback.originalRequest.contains).foreach { request=>
            send(player,durableStatus(work,request))
          }
        }

  /** 独立durable消息入口，尚未注册到PacketHandler；namespace/客户端恢复及Boot原owner前置仍未闭合。 */
  def receiveDurableRecall(message:C2SDurableRecallRequest,context:MessageContext):Unit =
    val player=authenticated(context)
    require(message!=null,"decoded durable recall required");message.validate()
    val lookup=reads.lookupRecallClientRequest(player,message.namespace,message.requestUuid)
    def reject(actual:RecallRequestNamespace,reason:RecallFailureReason):Unit =
      send(player,new S2CDurableRecallStatus(actual,message.requestUuid,message.podUuid,message.expectedStateVersion,
        None,DurableRecallPhase.NotAccepted,Some(reason)))
    def attach(work:Pending):Unit =
      require(work.actor==player.getUniqueID && work.durableNamespace.contains(message.namespace) &&
        work.feedback.originalRequest.contains(message.requestUuid) && work.attachFeedback(player,message.requestUuid),
        "exact authenticated original durable Pending required")
      deliverProcessing(work)
    // 先核真实namespace；final ACK custody不是持久phase推导，也不重挂旧连接。
    val sameNamespace=lookup match
      case MySqlDbFirstRecallRequestLookup.NamespaceMismatch(_)=>false
      case _=>true
    if sameNamespace then
      results.client(message.namespace,message.requestUuid).foreach { slot=>
        require(slot.pod==message.podUuid && slot.version==message.expectedStateVersion,
          "original durable request result target changed")
        send(player,slot.status)
      }
      if results.client(message.namespace,message.requestUuid).nonEmpty then return
    lookup match
      case MySqlDbFirstRecallRequestLookup.NamespaceMismatch(actual)=>reject(actual,RecallFailureReason.Busy)
      case MySqlDbFirstRecallRequestLookup.Found(actual,original)=>
        val restored=boot.recoveredResult(actual,original)
        restored.foreach { status=>
          require(status.podUuid==message.podUuid && status.expectedStateVersion==message.expectedStateVersion,
            "original recovered request target changed")
          send(player,status)
        }
        if restored.nonEmpty then return
        // durable metadata不证明物理清理/最终ACK；原owner缺席时明确失败，不伪造终局或重执行。
        val work=pending.get(original.originalTag).getOrElse(throw new IllegalStateException(
          "Original durable recall owner unavailable for operation "+original.originalTag.operationId))
        require(work.pod==original.podUuid && work.expectedVersion==original.expectedStateVersion &&
          work.durableNamespace.contains(actual),"durable index and original Main Pending differ")
        attach(work)
      case MySqlDbFirstRecallRequestLookup.NotFound(actual)=>
        // J未耐久前也复用原Pending；未命中本身不是fresh SQL或absence许可。
        pending.valuesIterator.find(work=>work.actor==player.getUniqueID && work.durableNamespace.contains(actual) &&
          work.feedback.originalRequest.contains(message.requestUuid)) match
          case Some(work)=>attach(work)
          case None=>
            if !ready() || pending.size+results.completedCount>=capacity || !results.available || pending.valuesIterator.exists(work=>
              work.pod==message.podUuid || work.feedback.references(message.requestUuid)) ||
              !publication.canBeginPlayerDbFirstRecall(message.podUuid) then
              reject(actual,RecallFailureReason.Busy);return
            val decision=reads.preview(player,message.podUuid,message.expectedStateVersion)
            DbFirstPlayerRecallPolicy.refusal(decision) match
              case Some(reason)=>reject(actual,reason);return
              case None=>()
            val record=manager.managedActiveDetails(message.podUuid).getOrElse(
              throw new IllegalStateException("Matched durable recall DATA has no actual manager record"))
            if !record.exactState.dimensionId.contains(player.dimension) then
              reject(actual,RecallFailureReason.WrongDimension);return
            if !adapter.canReserveDbFirstCapture then
              reject(actual,RecallFailureReason.Busy);return
            val id=adapter.currentExecutionIdentity
            require(id==publication.identity,"fresh durable recall identity must equal the actual current publication generation")
            val original=IoOperationTag(id.bootId,id.bootGeneration,id.connectionGeneration,UUID.randomUUID())
            var journalOperation=UUID.randomUUID()
            while journalOperation==original.operationId do journalOperation=UUID.randomUUID()
            val journal=original.copy(operationId=journalOperation)
            val work=new Pending(original,player,message.podUuid,message.expectedStateVersion,Some(message.requestUuid),Some(actual))
            pending.put(original,work) // 在结果槽/J初始化和副作用之前保管partial责任。
            try
              work.resultSlot=Some(results.reserve(actual,message.requestUuid,work.pod,work.expectedVersion,original))
              work.journal=Some(new DbFirstRecallJournalMain(main,adapter,journal,original,actual,
                work.pod,work.expectedVersion,Some(message.requestUuid)))
              work.journal.get.submitJournal()
            catch case cause:Throwable=>
              fail(work,cause)
              if !NonFatal(cause) then throw cause
            if work.cause.isEmpty then deliverProcessing(work)

  private def refuseRequest(player: EntityPlayerMP, message: C2SRecallRequest, request: Option[UUID], reason: RecallFailureReason): Unit =
    request match
      case Some(id) => send(player, new S2CRecallOperationStatus(id, message.podUuid, message.expectedStateVersion, None,
        RecallOperationPhase.Refused, Some(reason)))
      case None => refuse(player, message.podUuid, reason)

  private def retainedFailure(player:EntityPlayerMP,request:UUID,tag:IoOperationTag,pod:UUID,version:Long):Unit =
    require(player!=null && request!=null && tag!=null && pod!=null && version>=0L,
      "actual original operation retained-failure identity required")
    send(player,new S2CRecallOperationStatus(request,pod,version,Some(tag.operationId),
      RecallOperationPhase.RetainedFailure,Some(RecallFailureReason.InfrastructureFailure)))

  private def sendOriginalOperationState(player:EntityPlayerMP,request:UUID,tag:IoOperationTag,
      pod:UUID,version:Long,failed:Boolean):Unit =
    send(player,new S2CRecallOperationStatus(request,pod,version,Some(tag.operationId),
      if failed then RecallOperationPhase.RetainedFailure else RecallOperationPhase.Processing,
      if failed then Some(RecallFailureReason.InfrastructureFailure) else None))

  /** 只有Boot原custody实际交付的终态可跨新Owner回放；持久phase本身不够。 */
  private def sendRecoveredBootTerminal(player:EntityPlayerMP,request:UUID,namespace:RecallRequestNamespace,
      original:MySqlDbFirstRecallRequestRecord):Boolean =
    boot.recoveredResult(namespace,original) match
      case None=>false
      case Some(status)=>
        require(status.namespace==namespace && status.requestUuid==request &&
          status.podUuid==original.podUuid && status.expectedStateVersion==original.expectedStateVersion &&
          status.operationUuid.contains(original.originalTag.operationId),
          "actual Boot terminal handoff differs from the original request")
        status.phase match
          case DurableRecallPhase.Committed=>
            require(status.reason.isEmpty && status.expectedStateVersion<Long.MaxValue,
              "actual Boot committed terminal must retain its original version")
            send(player,new S2CRecallOperationStatus(request,original.podUuid,original.expectedStateVersion,
              Some(original.originalTag.operationId),RecallOperationPhase.Committed,None))
            true
          case DurableRecallPhase.Refused=>
            require(status.reason.nonEmpty,"actual Boot refused terminal requires its durable reason")
            send(player,new S2CRecallOperationStatus(request,original.podUuid,original.expectedStateVersion,
              None,RecallOperationPhase.Refused,status.reason))
            true
          case _=>false

  private def receiveChecked(message: C2SRecallRequest, player: EntityPlayerMP, request: Option[UUID]): Unit =
    try receiveAuthenticatedRecall(message, player, request)
    catch case NonFatal(cause) =>
      logger.error("Authenticated recall admission failed for pod " + message.podUuid + " actor " + player.getUniqueID, cause)
      if request.isEmpty then
        try refuseRequest(player, message, request, RecallFailureReason.InfrastructureFailure)
        catch case delivery: Throwable =>
          if delivery ne cause then cause.addSuppressed(delivery)
          throw cause
      else throw cause // 无法核实重复请求时不能伪造未接纳的terminal refusal。

  private def durableStatus(work:Pending,request:UUID):S2CDurableRecallStatus =
    val phase=if !work.mayExposeOperation then DurableRecallPhase.Pending
      else if work.cause.nonEmpty then DurableRecallPhase.RetainedFailure else DurableRecallPhase.Processing
    new S2CDurableRecallStatus(work.durableNamespace.get,request,work.pod,work.expectedVersion,
      if work.mayExposeOperation then Some(work.tag.operationId) else None,phase,
      if work.cause.nonEmpty then Some(RecallFailureReason.InfrastructureFailure) else None)

  private def progress(work: Pending): Unit =
    work.feedback.currentRequest.foreach { request =>
      work.durableNamespace match
        case Some(_)=>send(work.player,durableStatus(work,request))
        case None=>
          if work.mayExposeOperation then
            sendOriginalOperationState(work.player,request,work.tag,work.pod,work.expectedVersion,work.cause.nonEmpty)
    }

  private def deliverProcessing(work: Pending): Unit =
    try progress(work)
    catch case NonFatal(delivery) =>
      logger.error("Retained player recall processing feedback failed for pod " + work.pod + " operation " + work.tag.operationId, delivery)

  private def receiveAuthenticatedRecall(message: C2SRecallRequest, player: EntityPlayerMP, request: Option[UUID]): Unit =
    val pod = message.podUuid
    // 先排除其他Pod的实际在用request；同Pod重连只接原pending的回馈，不创建新operation。
    val crossPod=request.flatMap(id=>pending.valuesIterator.find(work=>work.pod!=pod && work.feedback.references(id)).map(work=>(id,work)))
    crossPod match
      case Some((id,work))=>
        if work.actor==player.getUniqueID && work.feedback.originalRequest.contains(id) then
          if work.mayExposeOperation then
            sendOriginalOperationState(player,id,work.tag,work.pod,work.expectedVersion,work.cause.nonEmpty)
        else refuseRequest(player,message,request,RecallFailureReason.Busy)
        return
      case None=>()
    val retained = pending.valuesIterator.find(_.pod == pod)
    if retained.nonEmpty then
      val original = retained.get
      request match
        case Some(id) if original.actor==player.getUniqueID && original.feedback.originalRequest.contains(id) &&
            original.durableNamespace.isEmpty && original.expectedVersion==message.expectedStateVersion =>
          if original.attachFeedback(player,id) then deliverProcessing(original)
          else retainedFailure(player,id,original.tag,original.pod,original.expectedVersion)
        case Some(id) if original.actor==player.getUniqueID && original.feedback.originalRequest.contains(id) &&
            original.durableNamespace.isEmpty =>
          if original.mayExposeOperation then
            sendOriginalOperationState(player,id,original.tag,original.pod,original.expectedVersion,original.cause.nonEmpty)
        case Some(id) if original.actor==player.getUniqueID && original.feedback.originalRequest.contains(id) &&
            original.expectedVersion==message.expectedStateVersion =>
          if original.mayExposeOperation then
            sendOriginalOperationState(player,id,original.tag,original.pod,original.expectedVersion,original.cause.nonEmpty)
        case Some(id) if original.actor==player.getUniqueID && original.feedback.originalRequest.contains(id) =>
          if original.mayExposeOperation then
            sendOriginalOperationState(player,id,original.tag,original.pod,original.expectedVersion,original.cause.nonEmpty)
        case _=>refuseRequest(player,message,request,RecallFailureReason.Busy)
      return

    // 关联请求UUID先在真实binding+认证actor namespace内做实际Full27判重；不得按phase直接重跑。
    val priorLookup=request.map { requestId=>
      val namespace=reads.recallNamespace(player)
      (requestId,namespace,reads.lookupRecallClientRequest(player,namespace,requestId))
    }
    priorLookup match
      case Some((_,_,MySqlDbFirstRecallRequestLookup.NamespaceMismatch(_)))=>
        refuseRequest(player,message,request,RecallFailureReason.Busy);return
      case Some((_,namespace,MySqlDbFirstRecallRequestLookup.NotFound(actual)))=>
        require(actual==namespace,"actual recall lookup namespace changed during the Main request")
      case Some((requestId,namespace,MySqlDbFirstRecallRequestLookup.Found(actual,original)))=>
        require(actual==namespace,"actual recall lookup namespace changed during the Main request")
        pending.get(original.originalTag) match
          case Some(work) if work.actor==player.getUniqueID && work.feedback.originalRequest.contains(requestId) =>
            if work.pod==pod && work.expectedVersion==message.expectedStateVersion && work.durableNamespace.isEmpty then
              if work.attachFeedback(player,requestId) then deliverProcessing(work)
              else throw new IllegalStateException("exact original player recall feedback attachment failed")
            else if work.mayExposeOperation then
              sendOriginalOperationState(player,requestId,work.tag,work.pod,work.expectedVersion,work.cause.nonEmpty)
          case _=>
            if original.podUuid!=pod || original.expectedStateVersion!=message.expectedStateVersion then
              retainedFailure(player,requestId,original.originalTag,original.podUuid,original.expectedStateVersion)
            else if !sendRecoveredBootTerminal(player,requestId,actual,original) then
              retainedFailure(player,requestId,original.originalTag,original.podUuid,original.expectedStateVersion)
        return
      case None=>()
    if !ready() || pending.size+results.completedCount >= capacity || pending.valuesIterator.exists(_.pod == pod) ||
      request.exists(id => pending.valuesIterator.exists(_.feedback.references(id))) ||
      !publication.canBeginPlayerDbFirstRecall(pod) then
      refuseRequest(player, message, request, RecallFailureReason.Busy)
      return
    val decision = try reads.preview(player, pod, message.expectedStateVersion) catch
      case _: ManagedOrdinarySaveAdmission.MainReadPending | _: MainDbFirstReadAdmission.Sealed =>
        refuseRequest(player, message, request, RecallFailureReason.Busy)
        return
    DbFirstPlayerRecallPolicy.refusal(decision) match
      case Some(reason) => refuseRequest(player, message, request, reason); return
      case None => ()
    val record = manager.managedActiveDetails(pod).getOrElse(
      throw new IllegalStateException("Matched active Main DATA has no original manager record"))
    if !record.exactState.dimensionId.contains(player.dimension) then
      refuseRequest(player, message, request, RecallFailureReason.WrongDimension)
      return
    if !adapter.canReserveDbFirstCapture then
      refuseRequest(player, message, request, RecallFailureReason.Busy)
      return
    val id = adapter.currentExecutionIdentity
    require(id==publication.identity,"fresh recall identity must equal the actual current publication generation")
    val tag = IoOperationTag(id.bootId, id.bootGeneration, id.connectionGeneration, UUID.randomUUID())
    val work = new Pending(tag, player, pod, message.expectedStateVersion, request)
    pending.put(tag, work) // 原operation先保留，reserve/freeze/capture/submit失败不丢责任。
    try
      request match
        case Some(requestId)=>
          val namespace=priorLookup.map(_._2).getOrElse(
            throw new IllegalStateException("actual correlated recall namespace missing"))
          var journalOperation=UUID.randomUUID()
          while journalOperation==tag.operationId do journalOperation=UUID.randomUUID()
          val journalTag=IoOperationTag(id.bootId,id.bootGeneration,id.connectionGeneration,journalOperation)
          work.journal=Some(new DbFirstRecallJournalMain(main,adapter,journalTag,tag,namespace,
            pod,message.expectedStateVersion,Some(requestId)))
          work.journal.get.submitJournal()
        case None=>
          work.reservation = Some(adapter.reserveDbFirstCapture(tag, DbFirstPodSnapshot.Purpose.RECALL))
          captureAndSubmit(work)
    catch case cause: Throwable =>
      fail(work, cause)
      if !NonFatal(cause) then throw cause
    if work.journal.isEmpty && work.captured && work.cause.isEmpty && work.feedback.currentRequest.nonEmpty then
      deliverProcessing(work)

  def owns(tag: IoOperationTag): Boolean =
    owner();pending.valuesIterator.exists(work=>work.journal match
      case Some(journal)=>journal.expectedCompletionTag.contains(tag)
      case None=>work.tag==tag)
  def retain(batch: Vector[IoCompletion[MySqlOwnedIoReply]]): Unit =
    owner()
    require(batch != null && batch.size <= capacity && !batch.exists(_ == null) &&
      batch.map(_.tag).distinct.size == batch.size,
      "exact player recall completion batch required")
    val assignments=batch.map { completion=>
      val matches=pending.valuesIterator.filter(work=>work.journal match
        case Some(journal)=>journal.expectedCompletionTag.contains(completion.tag)
        case None=>work.tag==completion.tag).toVector
      require(matches.size==1 && matches.head.completion.isEmpty,"one exact original player completion owner required")
      (matches.head,completion)
    }
    assignments.foreach((work,completion)=>work.completion=Some(completion))
  def retainedCount: Int =
    owner(); pending.size
  def failures: Vector[Throwable] =
    owner(); pending.valuesIterator.flatMap(_.cause).toVector
  def retainedEndPending: Boolean =
    owner(); pending.valuesIterator.exists(_.retainedEnd.active)

  private def fail(work: Pending, cause: Throwable): Unit =
    val first = work.cause.isEmpty
    work.cause match
      case None => work.cause = Some(cause)
      case Some(original) => if (original ne cause) && !original.getSuppressed.contains(cause) then original.addSuppressed(cause)
    if work.entry.isEmpty then work.entry = publication.dbFirstRecall(work.tag)
    if work.entry.nonEmpty then work.reservation = None
    if first then logger.error("Authenticated player recall failed for pod " + work.pod + " operation " + work.tag.operationId, cause)
    if !work.failureFeedbackAttempted then
      work.failureFeedbackAttempted = true
      if work.connected then
        try
          if work.feedback.currentRequest.nonEmpty then progress(work)
          else refuse(work.player, work.pod, RecallFailureReason.InfrastructureFailure)
        catch case delivery: Throwable =>
          if delivery ne cause then cause.addSuppressed(delivery)
          if !NonFatal(delivery) then throw delivery

  private def captureAndSubmit(work:Pending):Unit =
    work.entry=Some(publication.beginDbFirstRecall(adapter,work.reservation.get,work.actor,work.pod,work.expectedVersion))
    work.reservation=None
    val result=adapter.submit(work.tag,MySqlOwnedIoCommand.ExecuteDbFirstRecall(work.entry.get.reservation))
    require(result==IoSubmission.Accepted,"player recall capture not accepted; original frozen responsibility retained")
    work.captured=true

  /** J确认期间实际Main状态可变；此处只选正常拒绝，beginDbFirstRecall仍重核真实body/控制状态。 */
  private def currentDurableRefusal(work:Pending):Option[RecallFailureReason] =
    DbFirstPlayerRecallPolicy.afterJournal(work.actor,work.pod,work.expectedVersion,
      manager.managedActiveDetails(work.pod).map(_.exactState),ready(),publication.canBeginPlayerDbFirstRecall(work.pod),
      if work.connected then Some(work.player.dimension) else None)

  private def advanceJournal(work:Pending,tick:PermanentEndTick):Unit =
    val journal=work.journal.get
    work.completion.foreach { actual=>
      journal.retain(actual)
      work.completion=None // 子owner接管后父Pending也须撤reply，不能跨到worker RAW退休。
    }
    journal.advance(tick.endOrdinal)
    if journal.awaitingCaptureDecision then
      work.mayExposeOperation=true // 原J确认和实际Main读代ACK均已完成。
      currentDurableRefusal(work) match
        case Some(reason)=>
          work.refusalReason=Some(reason)
          // 既有exclusion API要求同一cause载体；这是正常拒绝上下文，不抛出/打印ERROR。
          journal.prepareRefusal(tick.endOrdinal,new IllegalStateException("Current pre-capture admission refused: "+reason),
            Some(DbFirstPlayerRecallPolicy.durableReason(reason)))
        case None=>journal.prepareCaptureTransfer(tick.endOrdinal)
      if work.connected && (work.durableNamespace.nonEmpty || work.refusalReason.isEmpty) then
        deliverProcessing(work)
    if journal.captureAvailable then
      work.reservation=Some(journal.takeCapture())
      work.journal=None // 原C reservation来自实际transfer ACK，绝不再次reserve fresh C。
      captureAndSubmit(work)
    else if journal.refusalAvailable then
      val receipt=journal.takeRefusal()
      require(receipt.originalTag==work.tag && receipt.podUuid==work.pod && receipt.actorUuid==work.actor,
        "exact original refused player recall required")
      val reason=work.refusalReason.getOrElse(throw new IllegalStateException("original refusal reason missing"))
      work.durableNamespace match
        case Some(_)=>
          val slot=work.resultSlot.getOrElse(throw new IllegalStateException("original durable result reservation missing"))
          results.refused(slot,reason)
          pending.remove(work.tag)
          work.journal=None
          if work.connected then send(work.player,slot.status)
        case None=>
          val request=work.feedback.originalRequest.getOrElse(
            throw new IllegalStateException("journal refusal has no original correlated request"))
          pending.remove(work.tag)
          work.journal=None
          if work.connected then
            send(work.player,new S2CRecallOperationStatus(request,work.pod,work.expectedVersion,None,
              RecallOperationPhase.Refused,Some(reason)))

  /** 唯一永久END及原ACK驱动；不从网络调用栈推进physical cleanup。 */
  def onEnd(tick: PermanentEndTick): Unit =
    owner()
    if tick.phase != PermanentPumpPhase.Ready then return
    pending.valuesIterator.filter(work => work.cause.isEmpty || work.retainedEnd.active).toVector.foreach { work =>
      try
        if work.journal.nonEmpty then advanceJournal(work,tick)
        if pending.contains(work.tag) && work.journal.isEmpty then
          val entry = work.entry.getOrElse(throw new IllegalStateException("original player recall entry missing"))
          work.completion.foreach { completion =>
            completion.result.failed.foreach(throw _)
            if work.releaseSubmitted || work.retainedEnd.awaitingAck then
              if work.retainedEnd.awaitingAck then
                work.retainedEnd.finish(publication.finishRetainedDbFirstRecallRelease(adapter, entry, completion))
              else publication.finishDbFirstRecallRelease(adapter, entry, completion)
              work.resultSlot.foreach(results.committed) // 实际final ACK先交纯metadata，再撤Pending/发送。
              pending.remove(work.tag)
              work.completion = None
              if work.connected then
                val version = Math.addExact(work.expectedVersion, 1L)
                val pose = entry.finalPose
                work.feedback.currentRequest match
                  case Some(request) => work.durableNamespace match
                    case Some(_)=>send(work.player,work.resultSlot.get.status)
                    case None=>send(work.player, new S2CRecallOperationStatus(request, work.pod, work.expectedVersion,
                      Some(work.tag.operationId), RecallOperationPhase.Committed, None))
                  case None => send(work.player, S2CRecallResult.succeeded(work.pod, version))
                send(work.player, new S2CStateSync(work.pod, pose.posX, pose.posY, pose.posZ, pose.yaw, pose.pitch, version))
            else
              require(work.captured, "original player recall capture submit receipt required")
              val retainedFailure = DbFirstRecallCompletionHandoff.accept(publication.completeDbFirstRecall(adapter, entry, completion)) {
                work.completion = None
              }
              retainedFailure match
                case Some(original) =>
                  work.retainedEnd.accepted(tick.endOrdinal)
                  fail(work, original)
                case None => work.confirmed = true
          }
          if pending.contains(work.tag) && work.retainedEnd.active then
            work.retainedEnd.advance(tick.endOrdinal) {
              publication.advanceRetainedDbFirstRecall(adapter, entry, tick).nonEmpty
            } {
              publication.handoffRetainedDbFirstRecall(adapter, entry)
            } {
              publication.submitRetainedDbFirstRecallRelease(adapter, entry, tick)
            }
          else if pending.contains(work.tag) && work.cause.isEmpty && work.confirmed && !work.releaseSubmitted then
            publication.advanceDbFirstRecall(adapter, entry, tick).foreach {
              case IoSubmission.Accepted => work.releaseSubmitted = true
              case IoSubmission.RejectedCapacity => ()
              case IoSubmission.RejectedQuiescing => throw new IllegalStateException("original player recall release quiesced")
              case IoSubmission.RejectedStopped(cause) => throw cause.getOrElse(new IllegalStateException("original player recall release stopped"))
            }
      catch case cause: Throwable =>
        work.retainedEnd.stop()
        // ACK已退休后的网络失败不重建operation或保留已释放的游戏引用。
        if pending.contains(work.tag) then fail(work, cause)
        else logger.error("Final player recall feedback failed for pod " + work.pod + " operation " + work.tag.operationId, cause)
        if !NonFatal(cause) then throw cause
    }
