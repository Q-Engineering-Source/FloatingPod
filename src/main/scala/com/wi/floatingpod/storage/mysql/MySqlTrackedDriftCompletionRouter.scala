package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import scala.collection.mutable
import scala.util.{Failure, Success}

private[mysql] enum MySqlCompletionChannel:
  case Ordinary, Drift, RequestRead

/** 原Accepted slot跨worker/partition/Main保留；业务slot直到原责任的纯内存ACK才归还。 */
private[mysql] final class MySqlTrackedDriftCompletionRouter(private var identity: MySqlOwnedIoIdentity, capacity: Int):
  require(identity != null && capacity > 0, "exact shared bounded completion routing required")
  private final class Entry(val channel: MySqlCompletionChannel, val retainedPlan: Boolean):
    var accepted = false
    var consumed = false
    var controlSubmission = false
    var submissionUnknown = Option.empty[Throwable]
    var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var checkpointReadAck=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var refusalContinuation=Option.empty[MySqlDbFirstRecallRefusalSource]
    var refusalReadAck=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var refusalReadAcknowledged=false
    var refusalFinal=Option.empty[MySqlDbFirstRecallRefusalFinalReceipt]
    var refusalFinalSubmissionAttempted=false
    var journalReadAck=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var journalCaptureAck=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var journalCaptureContinuation=Option.empty[MySqlDbFirstRecallJournalCaptureTransfer]
    var journalCaptureSubmissionAttempted=false
    var preparationReadAck=Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var dbFirstRetirementAck = Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var dbFirstRetirementFailure = Option.empty[IoCompletion[MySqlOwnedIoReply]]
  private val entries = mutable.LinkedHashMap.empty[IoOperationTag, Entry]
  private var primary = Option.empty[Throwable]
  // 只保真实OwnedIO一次bounded drain的原batch；没有外部envelope ingress/额外quarantine预算。
  private var invariantBatch = Option.empty[Vector[IoCompletion[MySqlOwnedIoReply]]]

  /** 调用方须先由原IO hook和Main登记回执证明最后producer已退出；此处仅严格查原槽。 */
  private[mysql] def requireTerminalEmpty(): Unit =
    primary match
      case Some(cause) => throw cause
      case None => ()
    if entries.nonEmpty || invariantBatch.nonEmpty then
      throw new IllegalStateException("actual completion router retains original responsibility")

  /** 同一个已排空router原位转代，不复制credit或清理未决Entry/首因。 */
  private[mysql] def adoptRecoveredExecution(adapter:MySqlOwnedIoAdapter,next:MySqlOwnedIoIdentity):Unit =
    require(adapter!=null && next!=null,"actual recovered execution adapter/identity required")
    adapter.requireRecoveredExecutionRouter(this,next)
    requireTerminalEmpty()
    require(next.bootId==identity.bootId && next.bootGeneration==identity.bootGeneration &&
      next.connectionGeneration>identity.connectionGeneration,"actual increasing same-boot router generation required")
    identity=next

  def begin(tag: IoOperationTag, channel: MySqlCompletionChannel, retainsPlan: Boolean): Boolean =
    identity.requireTag(tag)
    require(channel != null && !entries.contains(tag), "exact completion tag already retains responsibility")
    primary.foreach(throw _)
    if entries.size >= capacity then false
    else
      entries.put(tag, new Entry(channel, retainsPlan))
      true

  /** 已消费业务slot承担自身结算控制命令，不需要另一普通容量credit。 */
  def beginRelease(tag: IoOperationTag, channel: MySqlCompletionChannel): Unit =
    identity.requireTag(tag); primary.foreach(throw _)
    val entry = entries.getOrElse(tag, throw new IllegalStateException("release lost original accepted slot"))
    require(entry.channel == channel && entry.retainedPlan && entry.consumed &&
      !entry.accepted && entry.completion.isEmpty && entry.submissionUnknown.isEmpty && entry.dbFirstRetirementAck.isEmpty && entry.preparationReadAck.isEmpty && entry.checkpointReadAck.isEmpty && entry.journalReadAck.isEmpty && entry.journalCaptureAck.isEmpty && entry.refusalReadAck.isEmpty,
      "release must reuse the consumed original business slot once")
    entry.controlSubmission = true
    entry.consumed = false

  /** live复用已消费J槽；Boot首次正常占原C槽，core Entry既存不代表router已有credit。 */
  def beginRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource):Boolean =
    require(exclusion!=null,"actual refusal source route required")
    identity.requireTag(exclusion.routeTag);identity.requireTag(exclusion.executionTag)
    exclusion match
      case live:MySqlDbFirstRecallPreCaptureExclusion=>
        val entry=entries.getOrElse(live.routeTag,throw new IllegalStateException("original J slot missing"))
        require(entry.refusalContinuation.isEmpty && entry.refusalFinal.isEmpty && entry.journalCaptureContinuation.isEmpty &&
          live.originalTag!=live.journalTag,"original live refusal continuation is one-shot")
        beginRelease(live.routeTag,MySqlCompletionChannel.Ordinary)
        entry.refusalContinuation=Some(live);entry.controlSubmission=false
        true
      case boot:MySqlDbFirstBootRecallMainDecision=>
        primary.foreach(throw _)
        require(boot.refusalReason.nonEmpty && boot.routeTag==boot.executionTag,"actual typed Boot C refusal route required")
        if !begin(boot.routeTag,MySqlCompletionChannel.Ordinary,true) then false
        else
          entries(boot.routeTag).refusalContinuation=Some(boot)
          true
      case _=>throw new IllegalArgumentException("actual live or Boot refusal route required")

  def requireRefusalFinalPreparation(exclusion:MySqlDbFirstRecallRefusalSource):Unit =
    require(exclusion!=null,"original refusal route required")
    identity.requireTag(exclusion.routeTag);primary.foreach(throw _)
    val entry=entries.getOrElse(exclusion.routeTag,throw new IllegalStateException("original refusal J slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.consumed &&
      !entry.accepted && !entry.controlSubmission && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      entry.refusalReadAck.isEmpty && entry.refusalReadAcknowledged &&
      entry.refusalContinuation.exists(_ eq exclusion) && entry.refusalFinal.isEmpty,
      "actual original refusal read ACK before final metadata detach required")

  /** 在返回Main前撤原RAW route；当前调用栈/旧Main引用仍须跨END消失后才能提交worker。 */
  def prepareRefusalFinal(exclusion:MySqlDbFirstRecallRefusalSource,
      receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    requireRefusalFinalPreparation(exclusion)
    require(receipt!=null && receipt.tag==exclusion.routeTag && receipt.originalTag==exclusion.originalTag,
      "original J/C final metadata required")
    val entry=entries(receipt.tag)
    entry.refusalFinal=Some(receipt)
    entry.refusalContinuation=None

  def beginRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    require(receipt!=null,"original refusal final metadata required")
    identity.requireTag(receipt.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(receipt.tag,throw new IllegalStateException("original refusal final J slot missing"))
    require(entry.refusalFinal.exists(_ eq receipt) && entry.refusalContinuation.isEmpty &&
      entry.refusalReadAcknowledged && !entry.refusalFinalSubmissionAttempted,
      "original detached refusal final submission is one-shot")
    beginRelease(receipt.tag,MySqlCompletionChannel.Ordinary)
    entry.refusalFinalSubmissionAttempted=true

  def submitted(tag: IoOperationTag, result: IoSubmission): Unit =
    val entry = entries.getOrElse(tag, throw new IllegalStateException("submission was not reserved"))
    require(!entry.accepted && entry.submissionUnknown.isEmpty && result != null, "submission receipt cannot replay")
    if result == IoSubmission.Accepted then entry.accepted = true
    else if entry.controlSubmission || entry.journalCaptureContinuation.nonEmpty || entry.refusalContinuation.nonEmpty then entry.consumed = true
    else entries.remove(tag)
    ()

  def submissionFailed(tag: IoOperationTag, cause: Throwable): Unit =
    require(cause != null, "actual submission uncertainty required")
    val entry = entries.getOrElse(tag, throw new IllegalStateException("unknown submission lost reservation"))
    entry.submissionUnknown = Some(cause)
    primary = MySqlOwnedSessionLifecycle.combine(primary, cause)

  def routableSlots(max: Int): Int =
    require(max > 0, "positive routing bound required")
    if primary.nonEmpty then 0
    else Math.min(max, entries.values.count(e => e.accepted && !e.consumed && e.completion.isEmpty))

  /** 分类仅来自原Accepted commandkind；任何真实内部未知tag保整批并冻结，不释放普通slot。 */
  def routeBatch(batch: Vector[IoCompletion[MySqlOwnedIoReply]]): Unit =
    require(batch != null && batch.size <= capacity && !batch.exists(_ == null), "actual bounded OwnedIO batch required")
    primary.foreach(throw _)
    val valid = batch.map(_.tag).distinct.size == batch.size && batch.forall { completion =>
      entries.get(completion.tag).exists { e=>
        e.accepted && !e.consumed && e.completion.isEmpty && e.refusalFinal.forall { receipt=>
          e.controlSubmission && e.refusalFinalSubmissionAttempted && (completion.result match
            case Success(MySqlOwnedIoReply.RefusalFinalReleased(actual))=>actual eq receipt
            case Success(MySqlOwnedIoReply.RefusalFinalRetirementFailed(failure))=>
              failure!=null && (failure.receipt eq receipt)
            case Failure(_)=>true
            case _=>false)
        } && (e.controlSubmission || e.refusalContinuation.forall { exclusion=>
          completion.result match
            case Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observation))=>
              observation!=null && observation.routeTag==completion.tag && observation.journalTag==exclusion.journalTag && observation.tag==exclusion.executionTag
            case Failure(_)=>true
            case _=>false
        })
      }
    }
    if !valid then
      val cause = new IllegalStateException("actual OwnedIO completion batch violates original accepted slot identity")
      invariantBatch = Some(batch)
      primary = Some(cause)
      throw cause
    batch.foreach(completion => entries(completion.tag).completion = Some(completion))

  def take(channel: MySqlCompletionChannel, max: Int): Vector[IoCompletion[MySqlOwnedIoReply]] =
    require(channel != null && max > 0, "bounded exact partition consumer required")
    val selected = entries.iterator.filter((_, e) => e.channel == channel && e.completion.nonEmpty)
      .take(max).map((tag, e) => tag -> e.completion.get).toVector
    selected.foreach { (tag, _) =>
      val entry = entries(tag)
      if !entry.retainedPlan then entries.remove(tag)
      else if entry.controlSubmission then
        val completed = entry.completion.get
        val acknowledged = completed.result match
          case Success(MySqlOwnedIoReply.RejectedPreparationMainReadReleased(confirmation)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag
          case Success(MySqlOwnedIoReply.RejectedPreparationRetired(confirmation)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag
          case Success(MySqlOwnedIoReply.CheckpointMainReadReleased(confirmation)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag
          case Success(MySqlOwnedIoReply.DbFirstCheckpointFinalReleased(receipt)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && receipt!=null && receipt.tag==tag
          case Success(MySqlOwnedIoReply.RefusalFinalReleased(receipt))=>
            entry.channel==MySqlCompletionChannel.Ordinary && receipt!=null && receipt.tag==tag &&
              entry.refusalFinal.exists(_ eq receipt) && entry.refusalContinuation.isEmpty
          case Success(MySqlOwnedIoReply.RefusalMainReadReleased(confirmation))=>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag &&
              entry.refusalContinuation.exists(e=>e.originalTag==confirmation.originalTag && e.routeTag==tag)
          case Success(MySqlOwnedIoReply.JournalMainReadReleased(confirmation)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag
          case Success(MySqlOwnedIoReply.JournalRetiredForCapture(transfer)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && transfer!=null && transfer.tag==tag
          case Success(MySqlOwnedIoReply.PreparationMainReadReleased(confirmation)) =>
            entry.channel==MySqlCompletionChannel.Ordinary && confirmation!=null && confirmation.tag==tag
          case Success(MySqlOwnedIoReply.DbFirstPreparationFinalReleased(receipt)) =>
            entry.channel == MySqlCompletionChannel.Ordinary && receipt != null && receipt.tag == tag
          case Success(MySqlOwnedIoReply.DbFirstInitialPublicationReleased(receipt)) =>
            entry.channel == MySqlCompletionChannel.Ordinary && receipt != null && receipt.tag == tag
          case Success(MySqlOwnedIoReply.DbFirstRecallReleased(receipt)) =>
            entry.channel == MySqlCompletionChannel.Ordinary && receipt != null && receipt.tag == tag
          case Success(MySqlOwnedIoReply.RetainedPlanReleased(original)) =>
            (entry.channel == MySqlCompletionChannel.Ordinary || entry.channel == MySqlCompletionChannel.RequestRead) && original == tag
          case Success(MySqlOwnedIoReply.ModeProjectionReleased(original)) =>
            entry.channel == MySqlCompletionChannel.Ordinary && original == tag
          case Success(MySqlOwnedIoReply.DriftProjectionReleased(original)) =>
            entry.channel == MySqlCompletionChannel.Drift && original == tag
          case _ => false
        if acknowledged then
          completed.result match
            case Success(MySqlOwnedIoReply.JournalRetiredForCapture(_))=>
              entry.journalCaptureAck=Some(completed)
              entry.completion=None;entry.accepted=false;entry.consumed=true
            case Success(MySqlOwnedIoReply.CheckpointMainReadReleased(_)) =>
              entry.checkpointReadAck=Some(completed)
              entry.completion=None;entry.accepted=false;entry.consumed=true
            case Success(MySqlOwnedIoReply.RefusalMainReadReleased(_))=>
              entry.refusalReadAck=Some(completed)
              entry.completion=None;entry.accepted=false;entry.consumed=true
            case Success(MySqlOwnedIoReply.JournalMainReadReleased(_))=>
              entry.journalReadAck=Some(completed)
              entry.completion=None;entry.accepted=false;entry.consumed=true
            case Success(MySqlOwnedIoReply.PreparationMainReadReleased(_)) |
                Success(MySqlOwnedIoReply.RejectedPreparationMainReadReleased(_)) =>
              entry.preparationReadAck=Some(completed)
              entry.completion=None;entry.accepted=false;entry.consumed=true
            case Success(MySqlOwnedIoReply.RefusalFinalReleased(_)) |
                Success(MySqlOwnedIoReply.DbFirstCheckpointFinalReleased(_)) |
                Success(MySqlOwnedIoReply.RejectedPreparationRetired(_)) |
                Success(MySqlOwnedIoReply.DbFirstRecallReleased(_)) | Success(MySqlOwnedIoReply.DbFirstInitialPublicationReleased(_)) |
                Success(MySqlOwnedIoReply.DbFirstPreparationFinalReleased(_)) =>
              entry.dbFirstRetirementAck = Some(completed)
              entry.completion = None
              entry.accepted = false
              entry.consumed = true
            case _ => entries.remove(tag)
        else
          val local = completed.result match
            case Success(MySqlOwnedIoReply.RefusalFinalRetirementFailed(failure))
                if failure!=null && failure.receipt!=null && failure.receipt.tag==tag &&
                  entry.refusalFinal.exists(_ eq failure.receipt) && entry.channel==MySqlCompletionChannel.Ordinary=>Some(failure)
            case Success(MySqlOwnedIoReply.JournalCaptureRetirementFailed(failure))
                if failure!=null && failure.transfer!=null && failure.transfer.tag==tag &&
                  entry.channel==MySqlCompletionChannel.Ordinary=>Some(failure)
            case Success(MySqlOwnedIoReply.DbFirstCheckpointFinalRetirementFailed(failure))
                if failure != null && failure.receipt != null && failure.receipt.tag == tag &&
                  entry.channel == MySqlCompletionChannel.Ordinary => Some(failure)
            case Success(MySqlOwnedIoReply.DbFirstPreparationFinalRetirementFailed(failure))
                if failure != null && failure.receipt != null && failure.receipt.tag == tag &&
                  entry.channel == MySqlCompletionChannel.Ordinary => Some(failure)
            case Success(MySqlOwnedIoReply.DbFirstInitialPublicationRetirementFailed(failure))
                if failure != null && failure.receipt != null && failure.receipt.tag == tag &&
                  entry.channel == MySqlCompletionChannel.Ordinary => Some(failure)
            case Success(MySqlOwnedIoReply.DbFirstRecallRetirementFailed(failure))
                if failure != null && failure.receipt != null && failure.receipt.tag == tag &&
                  entry.channel == MySqlCompletionChannel.Ordinary => Some(failure)
            case _ => None
          val cause = completed.result match
            case Success(MySqlOwnedIoReply.RefusalFinalRetirementFailed(failure)) if local.nonEmpty=>failure.getCause
            case Success(MySqlOwnedIoReply.JournalCaptureRetirementFailed(failure)) if local.nonEmpty=>failure.getCause
            case Success(MySqlOwnedIoReply.DbFirstCheckpointFinalRetirementFailed(failure)) if local.nonEmpty => failure.getCause
            case Success(MySqlOwnedIoReply.DbFirstPreparationFinalRetirementFailed(failure)) if local.nonEmpty => failure.getCause
            case Success(MySqlOwnedIoReply.DbFirstInitialPublicationRetirementFailed(failure)) if local.nonEmpty => failure.getCause
            case Success(MySqlOwnedIoReply.DbFirstRecallRetirementFailed(failure)) if local.nonEmpty => failure.getCause
            case Failure(original) => original
            case Success(MySqlOwnedIoReply.RetainedPlanReleaseFailed(_, original)) => original
            case Success(MySqlOwnedIoReply.ModeProjectionReleaseFailed(_, original)) => original
            case Success(MySqlOwnedIoReply.DriftProjectionReleaseFailed(_, original)) => original
            case _ => new IllegalStateException("actual control completion did not acknowledge original slot")
          if local.nonEmpty then entry.dbFirstRetirementFailure=Some(completed)
          retainControlFailure(tag,cause,if local.nonEmpty then MySqlDbFirstRetirementFailure.Disposition.Local
            else MySqlDbFirstRetirementFailure.Disposition.Shared)
      else
        entry.completion = None
        entry.accepted = false
        entry.consumed = true
    }
    selected.map(_._2)

  /** 内存责任策略；不发行SQL/释放资格。生产调用须先核原worker的分类证据。 */
  private[mysql] def retainControlFailure(tag: IoOperationTag, cause: Throwable,
      disposition: MySqlDbFirstRetirementFailure.Disposition): Unit =
    identity.requireTag(tag)
    val entry=entries.getOrElse(tag,throw new IllegalStateException("control failure lost original slot"))
    require(cause != null && disposition != null && entry.retainedPlan && entry.controlSubmission &&
      entry.accepted && !entry.consumed && entry.completion.nonEmpty,"actual accepted control completion required")
    entry.submissionUnknown=Some(cause)
    entry.accepted=false
    entry.consumed=true
    entry.completion=None
    if disposition == MySqlDbFirstRetirementFailure.Disposition.Shared then
      primary=MySqlOwnedSessionLifecycle.combine(primary,cause)

  /** Main真正消费原worker退休ACK后归还slot；相等重造completion不被接受。 */
  def finishDbFirstRecallRelease(receipt: MySqlDbFirstRecallDropReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(receipt != null && completion != null && receipt.tag == completion.tag,
      "original DB-first retirement receipt and completion required")
    identity.requireTag(receipt.tag)
    primary.foreach(throw _)
    val entry = entries.getOrElse(receipt.tag, throw new IllegalStateException("DB-first retirement lost original slot"))
    if entry.dbFirstRetirementFailure.exists(_ eq completion) then
      completion.result match
        case Success(MySqlOwnedIoReply.DbFirstRecallRetirementFailed(local)) if local.receipt eq receipt =>
          throw local.getCause
        case _ => throw new IllegalArgumentException("DB-first local failure differs from original receipt")
    require(entry.channel == MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      entry.dbFirstRetirementAck.exists(_ eq completion), "DB-first retirement requires actual taken worker ACK")
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstRecallReleased(actual)) if actual eq receipt => ()
      case _ => throw new IllegalArgumentException("DB-first retirement ACK differs from original receipt")
    entry.dbFirstRetirementAck = None
    entries.remove(receipt.tag)
    ()

  def finishDbFirstPreparationFinalRelease(receipt: MySqlDbFirstPreparationFinalReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(receipt != null && completion != null && receipt.tag == completion.tag,
      "original DB-first retirement receipt and completion required")
    identity.requireTag(receipt.tag)
    primary.foreach(throw _)
    val entry = entries.getOrElse(receipt.tag, throw new IllegalStateException("DB-first retirement lost original slot"))
    if entry.dbFirstRetirementFailure.exists(_ eq completion) then
      completion.result match
        case Success(MySqlOwnedIoReply.DbFirstPreparationFinalRetirementFailed(local)) if local.receipt eq receipt =>
          throw local.getCause
        case _ => throw new IllegalArgumentException("DB-first local failure differs from original receipt")
    require(entry.channel == MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      entry.dbFirstRetirementAck.exists(_ eq completion), "DB-first retirement requires actual taken worker ACK")
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstPreparationFinalReleased(actual)) if actual eq receipt => ()
      case _ => throw new IllegalArgumentException("DB-first retirement ACK differs from original receipt")
    entry.dbFirstRetirementAck = None
    entries.remove(receipt.tag)
    ()

  def finishDbFirstCheckpointFinalRelease(receipt: MySqlDbFirstCheckpointFinalReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(receipt != null && completion != null && receipt.tag == completion.tag,
      "original DB-first retirement receipt and completion required")
    identity.requireTag(receipt.tag)
    primary.foreach(throw _)
    val entry = entries.getOrElse(receipt.tag, throw new IllegalStateException("DB-first retirement lost original slot"))
    if entry.dbFirstRetirementFailure.exists(_ eq completion) then
      completion.result match
        case Success(MySqlOwnedIoReply.DbFirstCheckpointFinalRetirementFailed(local)) if local.receipt eq receipt =>
          throw local.getCause
        case _ => throw new IllegalArgumentException("DB-first local failure differs from original receipt")
    require(entry.channel == MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      entry.dbFirstRetirementAck.exists(_ eq completion), "DB-first retirement requires actual taken worker ACK")
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstCheckpointFinalReleased(actual)) if actual eq receipt => ()
      case _ => throw new IllegalArgumentException("DB-first retirement ACK differs from original receipt")
    entry.dbFirstRetirementAck = None
    entries.remove(receipt.tag)
    ()

  def finishDbFirstInitialPublicationRelease(receipt: MySqlDbFirstInitialPublicationReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(receipt != null && completion != null && receipt.tag == completion.tag,
      "original DB-first retirement receipt and completion required")
    identity.requireTag(receipt.tag)
    primary.foreach(throw _)
    val entry = entries.getOrElse(receipt.tag, throw new IllegalStateException("DB-first retirement lost original slot"))
    if entry.dbFirstRetirementFailure.exists(_ eq completion) then
      completion.result match
        case Success(MySqlOwnedIoReply.DbFirstInitialPublicationRetirementFailed(local)) if local.receipt eq receipt =>
          throw local.getCause
        case _ => throw new IllegalArgumentException("DB-first local failure differs from original receipt")
    require(entry.channel == MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      entry.dbFirstRetirementAck.exists(_ eq completion), "DB-first retirement requires actual taken worker ACK")
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstInitialPublicationReleased(actual)) if actual eq receipt => ()
      case _ => throw new IllegalArgumentException("DB-first retirement ACK differs from original receipt")
    entry.dbFirstRetirementAck = None
    entries.remove(receipt.tag)
    ()

  def requireJournalCaptureAck(transfer:MySqlDbFirstRecallJournalCaptureTransfer,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(transfer!=null && completion!=null && transfer.tag==completion.tag,"original J/C retirement ACK required")
    identity.requireTag(transfer.tag);identity.requireTag(transfer.originalTag);primary.foreach(throw _)
    val entry=entries.getOrElse(transfer.tag,throw new IllegalStateException("original J/C slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.journalCaptureAck.exists(_ eq completion) && !entries.contains(transfer.originalTag),
      "actual taken original J/C worker ACK and unoccupied C required")
    completion.result match
      case Success(MySqlOwnedIoReply.JournalRetiredForCapture(actual)) if actual eq transfer=>()
      case _=>throw new IllegalArgumentException("original J/C transfer differs from worker ACK")

  /** 同一Entry先登记C再移除J；没有释放容量、重建业务槽或允许重放J。 */
  def finishJournalCaptureAck(transfer:MySqlDbFirstRecallJournalCaptureTransfer,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireJournalCaptureAck(transfer,completion)
    val entry=entries(transfer.tag)
    try
      entries.put(transfer.originalTag,entry)
      require(entries.remove(transfer.tag).exists(_ eq entry),"original J slot changed during C handoff")
      entry.journalCaptureAck=None;entry.controlSubmission=false
      entry.journalCaptureContinuation=Some(transfer)
    catch case cause:Throwable=>
      entry.submissionUnknown=MySqlOwnedSessionLifecycle.combine(entry.submissionUnknown,cause)
      primary=MySqlOwnedSessionLifecycle.combine(primary,cause)
      throw cause

  def beginJournalCapture(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit =
    require(transfer!=null,"original C continuation required")
    identity.requireTag(transfer.originalTag);primary.foreach(throw _)
    val entry=entries.getOrElse(transfer.originalTag,throw new IllegalStateException("original C slot missing"))
    require(entry.journalCaptureContinuation.exists(_ eq transfer) && entry.consumed && !entry.accepted &&
      !entry.controlSubmission && entry.completion.isEmpty && entry.submissionUnknown.isEmpty &&
      !entry.journalCaptureSubmissionAttempted,"original C business continuation is one-shot")
    entry.journalCaptureSubmissionAttempted=true;entry.consumed=false

  def requireRefusalReadAck(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,"original refusal read ACK required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original refusal J slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.refusalContinuation.exists(e=>e.originalTag==confirmation.originalTag && e.routeTag==confirmation.tag) &&
      entry.refusalReadAck.exists(_ eq completion),"actual taken refusal worker ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.RefusalMainReadReleased(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original refusal confirmation differs")

  def finishRefusalReadAck(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireRefusalReadAck(confirmation,completion)
    val entry=entries(confirmation.tag)
    entry.refusalReadAck=None;entry.controlSubmission=false;entry.refusalReadAcknowledged=true
    // 原J/C槽及exclusion仍保留；不把读代ACK当终局退休或允许第二次refusal。

  def requireRefusalFinalAck(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(receipt!=null && completion!=null && receipt.tag==completion.tag,"original refusal final ACK required")
    identity.requireTag(receipt.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(receipt.tag,throw new IllegalStateException("original refusal final J slot missing"))
    require(entry.refusalFinal.exists(_ eq receipt) && entry.refusalContinuation.isEmpty,
      "exact detached original refusal final receipt required")
    if entry.dbFirstRetirementFailure.exists(_ eq completion) then
      completion.result match
        case Success(MySqlOwnedIoReply.RefusalFinalRetirementFailed(local)) if local.receipt eq receipt=>throw local.getCause
        case _=>throw new IllegalArgumentException("original refusal local failure receipt differs")
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.refusalFinalSubmissionAttempted && entry.consumed && !entry.accepted && entry.completion.isEmpty &&
      entry.submissionUnknown.isEmpty && entry.dbFirstRetirementAck.exists(_ eq completion),
      "actual taken original refusal final worker ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.RefusalFinalReleased(actual)) if actual eq receipt=>()
      case _=>throw new IllegalArgumentException("original refusal final receipt differs")

  def finishRefusalFinalAck(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireRefusalFinalAck(receipt,completion)
    entries.remove(receipt.tag)
    ()

  def requireJournalReadAck(confirmation:MySqlDbFirstRecallJournalConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,
      "original journal read acknowledgement required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original journal slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.journalReadAck.exists(_ eq completion),"actual taken journal worker ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.JournalMainReadReleased(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original journal confirmation differs")

  def finishJournalReadAck(confirmation:MySqlDbFirstRecallJournalConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireJournalReadAck(confirmation,completion)
    val entry=entries(confirmation.tag)
    entry.journalReadAck=None
    entry.controlSubmission=false // 保留原业务槽，后续最终Entry退休仍使用此槽。

  def requirePreparationReadAck(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,
      "original preparation read acknowledgement required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original preparation slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.preparationReadAck.exists(_ eq completion),"actual taken preparation worker ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.PreparationMainReadReleased(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original preparation confirmation differs")

  def finishPreparationReadAck(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requirePreparationReadAck(confirmation,completion)
    val entry=entries(confirmation.tag)
    entry.preparationReadAck=None
    entry.controlSubmission=false // 保留原业务槽，后续最终Entry退休仍使用此槽。

  def requireCheckpointReadAck(confirmation:MySqlDbFirstCheckpointConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,
      "original checkpoint read acknowledgement required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original checkpoint slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.checkpointReadAck.exists(_ eq completion),"actual taken checkpoint worker ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.CheckpointMainReadReleased(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original checkpoint confirmation differs")

  def finishCheckpointReadAck(confirmation:MySqlDbFirstCheckpointConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireCheckpointReadAck(confirmation,completion)
    val entry=entries(confirmation.tag)
    entry.checkpointReadAck=None
    entry.controlSubmission=false // 保留原业务槽，后续最终Entry退休仍使用此槽。

  def requireRejectedPreparationReadAck(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,"original rejected read ACK required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original rejected slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.preparationReadAck.exists(_ eq completion),"actual taken rejected worker read ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.RejectedPreparationMainReadReleased(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original rejected confirmation differs")
  def finishRejectedPreparationReadAck(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    requireRejectedPreparationReadAck(confirmation,completion)
    val entry=entries(confirmation.tag)
    entry.preparationReadAck=None;entry.controlSubmission=false
  def finishRejectedPreparationRetirement(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(confirmation!=null && completion!=null && confirmation.tag==completion.tag,"original rejected final ACK required")
    identity.requireTag(confirmation.tag);primary.foreach(throw _)
    val entry=entries.getOrElse(confirmation.tag,throw new IllegalStateException("original rejected final slot missing"))
    require(entry.channel==MySqlCompletionChannel.Ordinary && entry.retainedPlan && entry.controlSubmission &&
      entry.consumed && !entry.accepted && entry.submissionUnknown.isEmpty && entry.completion.isEmpty &&
      entry.dbFirstRetirementAck.exists(_ eq completion),"actual taken rejected final ACK required")
    completion.result match
      case Success(MySqlOwnedIoReply.RejectedPreparationRetired(actual)) if actual eq confirmation=>()
      case _=>throw new IllegalArgumentException("original rejected final confirmation differs")
    entries.remove(confirmation.tag)
    ()

  def retainedCount: Int = entries.size
  def failure: Option[Throwable] = primary
  def actualInvariantBatch: Option[Vector[IoCompletion[MySqlOwnedIoReply]]] = invariantBatch
