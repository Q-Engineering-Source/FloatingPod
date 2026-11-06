package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{RecallRefusalReason,RecallRequestNamespace}
import com.wi.floatingpod.server.io.{IoCompletion,IoOperationTag,IoSubmission}
import com.wi.floatingpod.storage.mysql.*
import java.util.UUID
import scala.util.control.NonFatal

/** 提交不确定后仍接收原J迟到completion；保管不恢复健康或推进资格。 */
private[server] final class DbFirstRecallJournalCompletion(main:Thread,tag:IoOperationTag):
  require(main!=null && tag!=null,"original journal completion owner required")
  private var awaiting=false
  private var retained=Option.empty[IoCompletion[MySqlOwnedIoReply]]
  private var cause=Option.empty[Throwable]
  private def owner():Unit=require(Thread.currentThread() eq main,"original journal completion Main required")
  def failure:Option[Throwable] =
    owner();cause
  def requireHealthy():Unit =
    owner();cause.foreach(throw _)
  def fail(original:Throwable):Unit =
    owner();require(original!=null,"actual journal failure required")
    cause match
      case None=>cause=Some(original)
      case Some(first)=>if (first ne original) && !first.getSuppressed.contains(original) then first.addSuppressed(original)
  def beginSubmission():Unit =
    requireHealthy();require(!awaiting && retained.isEmpty,"one original journal/control submission at a time")
    awaiting=true // 必须在实际submit前登记，throw/UNKNOWN不得撤去迟到路由。
  def expectedTag:Option[IoOperationTag] =
    owner();if awaiting then Some(tag) else None
  def current:Option[IoCompletion[MySqlOwnedIoReply]] =
    owner();retained
  def retain(actual:IoCompletion[MySqlOwnedIoReply]):Unit =
    owner()
    require(awaiting && actual!=null && actual.tag==tag && retained.isEmpty,"one exact original awaited J completion required")
    retained=Some(actual)
  /** 实际worker失败直接沿原Throwable传播；不得先进入只接受成功结果的adapter门禁。 */
  def requireSuccessfulWorkerResult():MySqlOwnedIoReply =
    requireHealthy()
    val actual=retained.getOrElse(throw new IllegalStateException("original worker completion missing"))
    actual.result match
      case scala.util.Success(reply)=>reply
      case scala.util.Failure(original)=>
        fail(original)
        throw original
  /** 只在调用方已完成实际adapter confirmation/ACK后清除；失败不允许消费或重开。 */
  def consumed():Unit =
    requireHealthy();require(awaiting && retained.nonEmpty,"actual original J completion required before consumption")
    retained=None;awaiting=false

/** 原Main持有的J到C capture或拒绝终局责任；调用方必须先保管本对象，再调用submitJournal。
  * 不负责认证、fresh判重或产品接纳；这些前置不能由namespace或本对象代替。
  * 所有SQL确认、读代和退休资格均由实际adapter核验，不从DATA phase推定。
  */
private[server] final class DbFirstRecallJournalMain(
    main:Thread,adapter:MySqlOwnedIoAdapter,val journalTag:IoOperationTag,val originalTag:IoOperationTag,
    namespace:RecallRequestNamespace,val pod:UUID,val expectedVersion:Long,client:Option[UUID]):
  require(main!=null && adapter!=null && journalTag!=null && originalTag!=null && namespace!=null &&
    pod!=null && pod!=new UUID(0L,0L) && expectedVersion>=0L && client!=null &&
    client.forall(id=>id!=null && id!=new UUID(0L,0L)),"original Main journal dependencies and identities required")
  require(journalTag.operationId!=originalTag.operationId && journalTag.bootId==originalTag.bootId &&
    journalTag.bootGeneration==originalTag.bootGeneration && journalTag.connectionGeneration==originalTag.connectionGeneration,
    "distinct original J/C in the same actual executor required")
  // 新业务请求属于已完成恢复发布的当前执行代；Boot与历史receipt继续绑定原adapter identity。
  adapter.currentExecutionIdentity.requireTag(journalTag)
  adapter.currentExecutionIdentity.requireTag(originalTag)

  private enum Stage:
    case New, Journal, SubmitRead, ReadAck, Detach, SubmitTransfer, TransferAck, Capture, Taken, Failed
    case SubmitRefusal, Refusal, SubmitRefusalRead, RefusalReadAck, DetachRefusal, SubmitRefusalFinal, RefusalFinalAck, Refused, RefusalTaken
  private var stage=Stage.New
  private var reservation=Option.empty[MySqlDbFirstRecallJournalReservation]
  private var confirmation=Option.empty[MySqlDbFirstRecallJournalConfirmation]
  private var transfer=Option.empty[MySqlDbFirstRecallJournalCaptureTransfer]
  private var capture=Option.empty[MySqlDbFirstCaptureReservation]
  private var exclusion=Option.empty[MySqlDbFirstRecallPreCaptureExclusion]
  private var refusalConfirmation=Option.empty[MySqlDbFirstRecallRefusalConfirmation]
  private var refusalFinal=Option.empty[MySqlDbFirstRecallRefusalFinalReceipt]
  private val completions=new DbFirstRecallJournalCompletion(main,journalTag)
  private var lastEnd=0L
  private def owner():Unit=require(Thread.currentThread() eq main,"original journal Main required")
  private def healthy():Unit =
    owner();completions.requireHealthy()
  private def guarded[A](body: => A):A =
    healthy()
    try body catch case failure:Throwable=>
      completions.fail(failure)
      stage=Stage.Failed
      throw failure
  private def requireAccepted(result:IoSubmission,operation:String):Unit = result match
    case IoSubmission.Accepted=>()
    // 原core control有one-shot提交责任；明确拒收也不能在此自行重发。
    case IoSubmission.RejectedStopped(Some(failure))=>throw failure
    case other=>throw new IllegalStateException(operation+" was not accepted: "+other)

  def failure:Option[Throwable] =
    owner();completions.failure
  def expectedCompletionTag:Option[IoOperationTag] =
    owner()
    completions.expectedTag
  def captureAvailable:Boolean =
    owner();stage==Stage.Capture && completions.failure.isEmpty

  /** reserve或submit抛出时本对象仍留在原Pending；不得换J/C或再次调用。 */
  def submitJournal():Unit =
    healthy();require(stage==Stage.New,"original journal submission is one-shot")
    guarded {
      stage=Stage.Journal
      reservation=Some(adapter.reserveDbFirstRecallJournal(journalTag,originalTag,namespace.bindingUuid,
        namespace.actorUuid,pod,expectedVersion,client))
      completions.beginSubmission()
      requireAccepted(adapter.submit(journalTag,MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(reservation.get)),
        "original journal")
    }

  def retain(actual:IoCompletion[MySqlOwnedIoReply]):Unit =
    owner();completions.retain(actual)

  /** 由原永久END调用；同一END不跨阶段，撤RAW的调用栈退出后才提交worker退休。 */
  def advance(end:Long):Unit =
    healthy();require(end>0L && end>=lastEnd,"actual ordered journal END required")
    if end==lastEnd then return
    guarded {
      lastEnd=end
      stage match
        case Stage.Journal=>completions.current.foreach { actual=>
          val confirmed=adapter.claimDbFirstRecallJournalConfirmation(reservation.get,actual)
          confirmation=Some(confirmed)
          require(confirmed.originalTag==originalTag && confirmed.bindingUuid==namespace.bindingUuid &&
            confirmed.actorUuid==namespace.actorUuid && confirmed.podUuid==pod &&
            confirmed.expectedStateVersion==expectedVersion && confirmed.clientRequestUuid==client,
            "exact original journal Main namespace required")
          completions.consumed()
          stage=Stage.SubmitRead
        }
        case Stage.SubmitRead=>
          completions.beginSubmission()
          requireAccepted(adapter.submitJournalMainReadRelease(reservation.get,confirmation.get),"journal Main read handoff")
          stage=Stage.ReadAck
        case Stage.ReadAck=>completions.current.foreach { actual=>
          adapter.finishJournalMainReadRelease(reservation.get,confirmation.get,actual)
          completions.consumed()
          stage=Stage.Detach
        }
        case Stage.SubmitTransfer=>
          completions.beginSubmission()
          requireAccepted(adapter.submitJournalCaptureTransfer(transfer.get),"journal/C retirement")
          stage=Stage.TransferAck
        case Stage.TransferAck=>completions.current.foreach { actual=>
          val next=adapter.finishJournalCaptureTransfer(transfer.get,actual)
          capture=Some(next)
          require(next.tag==originalTag,"original C capture reservation required")
          completions.consumed()
          transfer=None
          stage=Stage.Capture
        }
        case Stage.SubmitRefusal=>
          completions.beginSubmission()
          requireAccepted(adapter.submit(journalTag,MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(exclusion.get)),
            "original pre-capture refusal")
          stage=Stage.Refusal
        case Stage.Refusal=>completions.current.foreach { actual=>
          val confirmed=adapter.claimRecallRefusalConfirmation(exclusion.get,actual)
          refusalConfirmation=Some(confirmed)
          require(confirmed.tag==journalTag && confirmed.originalTag==originalTag &&
            confirmed.bindingUuid==namespace.bindingUuid && confirmed.actorUuid==namespace.actorUuid &&
            confirmed.podUuid==pod && confirmed.clientRequestUuid==client,"exact original refusal confirmation required")
          completions.consumed()
          stage=Stage.SubmitRefusalRead
        }
        case Stage.SubmitRefusalRead=>
          completions.beginSubmission()
          requireAccepted(adapter.submitRefusalMainReadRelease(exclusion.get,refusalConfirmation.get),
            "refusal Main read handoff")
          stage=Stage.RefusalReadAck
        case Stage.RefusalReadAck=>completions.current.foreach { actual=>
          adapter.finishRefusalMainReadRelease(exclusion.get,refusalConfirmation.get,actual)
          completions.consumed()
          stage=Stage.DetachRefusal
        }
        case Stage.DetachRefusal=>
          val receipt=adapter.recordRefusalFinal(exclusion.get,refusalConfirmation.get)
          refusalFinal=Some(receipt)
          require(receipt.tag==journalTag && receipt.originalTag==originalTag &&
            receipt.bindingUuid==namespace.bindingUuid && receipt.actorUuid==namespace.actorUuid &&
            receipt.podUuid==pod && receipt.clientRequestUuid==client,"exact original refusal final receipt required")
          reservation=None
          confirmation=None
          exclusion=None
          refusalConfirmation=None
          // 当前调用栈/旧completion必须先退出；下一END才让worker清双RAW/root。
          stage=Stage.SubmitRefusalFinal
        case Stage.SubmitRefusalFinal=>
          completions.beginSubmission()
          requireAccepted(adapter.submitRefusalFinalRelease(refusalFinal.get),"original refusal final retirement")
          stage=Stage.RefusalFinalAck
        case Stage.RefusalFinalAck=>completions.current.foreach { actual=>
          adapter.finishRefusalFinalRelease(refusalFinal.get,actual)
          completions.consumed()
          stage=Stage.Refused
        }
        // Main必须重新核当前状态并显式选capture；不在J耐久后自动越过拒绝分流点。
        case Stage.New | Stage.Detach | Stage.Capture | Stage.Taken | Stage.Refused | Stage.RefusalTaken=>()
        case Stage.Failed=>throw completions.failure.getOrElse(new IllegalStateException("original journal failure missing"))
    }

  def awaitingCaptureDecision:Boolean =
    owner();stage==Stage.Detach && completions.failure.isEmpty

  /** 原Main重新核当前ownership/version/entity/conflict后显式选择；另一分支仍须实际refusal链。
    * 此调用只准备transfer。RAW撤离后的下一END才由advance提交退休，不能同栈退休。
    */
  def prepareCaptureTransfer(end:Long):Unit =
    healthy();require(stage==Stage.Detach && end>0L && end>=lastEnd,"actual journal read ACK and current END required")
    guarded {
      lastEnd=end
      val next=adapter.prepareJournalCaptureTransfer(reservation.get,confirmation.get)
      transfer=Some(next)
      require(next.tag==journalTag && next.originalTag==originalTag && next.bindingUuid==namespace.bindingUuid &&
        next.actorUuid==namespace.actorUuid && next.podUuid==pod && next.expectedStateVersion==expectedVersion,
        "exact original J/C transfer required")
      reservation=None
      confirmation=None
      stage=Stage.SubmitTransfer
    }

  /** 仅在尚未选transfer/capture的决策点，按真实当前拒绝原因封锁原C。
    * 这不是终局：还必须沿原J完成SQL/readback、Main读代、双RAW退休和最终ACK。
    */
  def prepareRefusal(end:Long,reason:Throwable,resultReason:Option[RecallRefusalReason]=None):Unit =
    healthy();require(stage==Stage.Detach && end>0L && end>=lastEnd && reason!=null && resultReason!=null && resultReason.forall(_!=null),
      "actual original journal read ACK, current END and refusal reason required")
    if !NonFatal(reason) then throw reason
    guarded {
      lastEnd=end
      val refused=adapter.excludeRecallBeforeCapture(reservation.get,reason,resultReason)
      exclusion=Some(refused)
      require(refused.journalTag==journalTag && refused.originalTag==originalTag && (refused.cause eq reason),
        "exact original C exclusion and cause required")
      stage=Stage.SubmitRefusal
    }

  def refusalAvailable:Boolean =
    owner();stage==Stage.Refused && completions.failure.isEmpty

  /** 只有实际worker退休及精确final ACK后交一次metadata；父Pending随后才可反馈。 */
  def takeRefusal():MySqlDbFirstRecallRefusalFinalReceipt =
    healthy();require(stage==Stage.Refused,"actual original refusal final ACK required")
    val result=refusalFinal.get
    refusalFinal=None
    stage=Stage.RefusalTaken
    result

  /** 只交出实际最终ACK兑换的原C；不freeze/capture、不创建新reservation。 */
  def takeCapture():MySqlDbFirstCaptureReservation =
    healthy();require(stage==Stage.Capture,"actual original journal/C final ACK required")
    val result=capture.get
    capture=None
    stage=Stage.Taken
    result
