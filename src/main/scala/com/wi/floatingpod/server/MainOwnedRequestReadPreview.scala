package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.*
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success}

private[floatingpod] enum MainRequestReadPoll:
  case Pending
  case Failed(cause: Throwable)
  case Decision(value: MySqlRequestReadDecision)
  case OriginalDeployment(value: Option[MySqlOriginalDeploymentOperation])

/** 真生产Main状态机亦使用的bounded DATA core；不发行session plan或JDBC资格。 */
private[floatingpod] final class MainRequestReadLedger(
    val identity: MySqlOwnedIoIdentity, main: Thread, capacity: Int):
  require(identity != null && main != null && capacity > 0 && capacity <= 256, "bounded exact Main ledger required")
  private[floatingpod] final class Entry(val tag: IoOperationTag,
      val request: MySqlRequestReadPreview | MySqlRepositoryOperation.FindOriginalDeploymentOperation, val epoch: Long):
    var accepted = false
    var submissionObserved = false
    var decisionConsumed = false
    var releasing = false
    var workerReleaseAcknowledged = false
    var staleEpoch = Option.empty[Long]
    var consumedAt = Option.empty[Long]
    var completion = Option.empty[IoCompletion[MySqlOwnedIoReply]]
    var decision = Option.empty[MySqlRequestReadDecision]
    var originalResult = Option.empty[Option[MySqlOriginalDeploymentOperation]]
    var failure = Option.empty[Throwable]
  private val entries = mutable.LinkedHashMap.empty[IoOperationTag, Entry]
  private var unmatched = Vector.empty[IoCompletion[MySqlOwnedIoReply]]
  private var retired = false
  private var ordinal = 0L
  private def owner(): Unit = require(Thread.currentThread() eq main, "request-read ledger requires original Main")
  def begin(tag: IoOperationTag,
      request: MySqlRequestReadPreview | MySqlRepositoryOperation.FindOriginalDeploymentOperation, epoch: Long): Option[Entry] =
    owner(); identity.requireTag(tag)
    require(request != null && epoch >= 0L && !entries.contains(tag), "one immutable read-preview tag required")
    if retired || entries.size >= capacity then None
    else
      val entry = new Entry(tag, request, epoch)
      entries.put(tag, entry)
      Some(entry)
  def submitted(entry: Entry, result: IoSubmission): Unit =
    owner(); owned(entry); require(result != null && !entry.submissionObserved, "one actual submission receipt required")
    entry.submissionObserved = true
    result match
      case IoSubmission.Accepted => entry.accepted = true
      case IoSubmission.RejectedStopped(cause) => cause.foreach(fail(entry, _))
      case _ => ()
  def fail(entry: Entry, cause: Throwable): Unit =
    owner(); owned(entry); require(cause != null, "original failure required")
    entry.failure match
      case None => entry.failure = Some(cause)
      case Some(primary) => if (primary ne cause) && !primary.getSuppressed.contains(cause) then primary.addSuppressed(cause)
  /** 每个END及每个不可逆边界共享的真实epoch门；不恢复已drop/已释放的引用。 */
  def requireCurrentEpoch(entry: Entry, epoch: Long): Boolean =
    owner(); owned(entry); require(epoch >= 0L, "actual submission epoch required")
    if entry.accepted && epoch != entry.epoch && entry.staleEpoch.isEmpty then
      entry.staleEpoch = Some(epoch)
      fail(entry, new IllegalStateException("request-read epoch changed; actual stage retained without further memory release"))
    entry.failure.isEmpty && !retired
  def observeEndEpoch(epoch: Long): Unit =
    owner(); require(epoch >= 0L, "actual END submission epoch required")
    entries.values.foreach(entry => if entry.accepted then requireCurrentEpoch(entry, epoch))
  def end(currentOrdinal: Long, epoch: Long, batch: Vector[IoCompletion[MySqlOwnedIoReply]]): Unit =
    owner()
    require(currentOrdinal > ordinal && epoch >= 0L && batch != null && batch.size <= capacity &&
      batch.forall(_ != null), "bounded strictly advancing Main END required")
    ordinal = currentOrdinal
    observeEndEpoch(epoch)
    // 整批先保留；任何解析/publication异常不丢后面的完成。
    batch.foreach { completion =>
      entries.get(completion.tag) match
        case Some(entry) if entry.accepted && entry.completion.isEmpty => entry.completion = Some(completion)
        case _ =>
          unmatched :+= completion
          retired = true
    }
    require(unmatched.size <= capacity, "unmatched real completion responsibility exceeds mailbox bound")
    entries.values.toVector.foreach { entry =>
      if entry.accepted && !entry.releasing && entry.consumedAt.isEmpty && entry.completion.nonEmpty then
        entry.consumedAt = Some(ordinal)
        if retired || epoch != entry.epoch then
          fail(entry, new IllegalStateException("stale request-read completion retained; no publication or memory ACK"))
        else
          entry.completion.get.result match
            case Failure(cause) => fail(entry, cause)
            case Success(MySqlOwnedIoReply.Transaction(replies)) =>
              try entry.request match
                case request:MySqlRequestReadPreview=>entry.decision=Some(MySqlRequestReadPreview.decide(request,replies))
                case _=>throw new IllegalStateException("original S-read received preview reply")
              catch case cause: Throwable => fail(entry, cause)
            case Success(MySqlOwnedIoReply.Operation(MySqlRepositoryReply.OriginalDeploymentOperation(result)))=>
              entry.request match
                case request:MySqlRepositoryOperation.FindOriginalDeploymentOperation=>
                  if result.exists(row=>row.actorUuid!=request.actorUuid || row.originalOperationId!=request.originalOperationId) then
                    fail(entry,new IllegalStateException("original S-read returned foreign actor/operation"))
                  else entry.originalResult=Some(result)
                case _=>fail(entry,new IllegalStateException("preview received original S-read reply"))
            case _ => fail(entry, new IllegalStateException("foreign request-read completion kind"))
    }
  def releaseCandidates: Vector[Entry] =
    owner()
    entries.valuesIterator.filter(e => !retired && e.accepted && !e.releasing && e.failure.isEmpty && e.decisionConsumed &&
      (e.decision.nonEmpty || e.originalResult.nonEmpty) && e.consumedAt.exists(_ < ordinal)).toVector
  def dropped(entry: Entry): Unit =
    owner(); owned(entry)
    require(releaseCandidates.exists(_ eq entry), "only healthy consumed earlier-END preview may drop")
    entry.completion = None // full reply/data strong references drop BEFORE original issuer receipt.
    entry.originalResult=None
    entry.releasing = true
  def released(entry: Entry, completion: IoCompletion[MySqlOwnedIoReply]): Boolean =
    owner(); owned(entry)
    require(entry.releasing && completion != null && completion.tag == entry.tag, "exact original release completion required")
    completion.result match
      case Success(MySqlOwnedIoReply.RetainedPlanReleased(tag)) if tag == entry.tag =>
        entry.workerReleaseAcknowledged = true // Worker/root release is already physical; stale ACK cannot undo it.
        if entry.failure.nonEmpty || retired then
          entry.completion = Some(completion)
          false
        else
          entries.remove(entry.tag)
          true
      case Failure(cause) =>
        entry.completion = Some(completion)
        fail(entry, cause)
        false
      case Success(MySqlOwnedIoReply.RetainedPlanReleaseFailed(_, cause)) =>
        entry.completion = Some(completion)
        fail(entry, cause)
        false
      case _ =>
        entry.completion = Some(completion)
        fail(entry, new IllegalStateException("request-read release lacks original worker ACK"))
        false
  def result(tag: IoOperationTag, currentEpoch: Long): Option[MySqlRequestReadDecision] =
    owner(); identity.requireTag(tag); require(currentEpoch >= 0L, "actual submission epoch required")
    if retired then None
    else entries.get(tag).flatMap { entry =>
      if currentEpoch != entry.epoch then
        fail(entry, new IllegalStateException("request-read preview epoch retired before Main consumption"))
      if entry.failure.nonEmpty then None
      else
        entry.decision.foreach(_ => entry.decisionConsumed = true)
        entry.decision
    }
  /** exact tag 原失败观察；失败不消费completion，也不产生drop/release资格。 */
  def poll(tag: IoOperationTag, currentEpoch: Long): MainRequestReadPoll =
    owner(); identity.requireTag(tag)
    entries.get(tag) match
      case Some(entry)=>entry.request match
        case _:MySqlRepositoryOperation.FindOriginalDeploymentOperation=>
          if !requireCurrentEpoch(entry,currentEpoch) then
            return MainRequestReadPoll.Failed(entry.failure.getOrElse(new IllegalStateException("original S-read retired")))
          entry.originalResult.foreach(_=>entry.decisionConsumed=true)
          return entry.originalResult.map(MainRequestReadPoll.OriginalDeployment.apply).getOrElse(MainRequestReadPoll.Pending)
        case _=>()
      case None=>()
    val decision = result(tag, currentEpoch)
    entries.get(tag).flatMap(_.failure) match
      case Some(cause) => MainRequestReadPoll.Failed(cause)
      case None => decision.map(MainRequestReadPoll.Decision.apply).getOrElse(MainRequestReadPoll.Pending)
  def discardNotAccepted(entry: Entry): Unit =
    owner(); owned(entry)
    require(entry.submissionObserved && !entry.accepted && entry.failure.isEmpty && entry.completion.isEmpty,
      "only actual nonAccepted reservation may cancel")
    entries.remove(entry.tag)
    ()
  /** entries只由原未接受取消或真实release ACK移除；失败/unmatched不变空成功。 */
  def requireSettledForPreStop(): Unit =
    require(!pendingForPreStop, "request-read ledger retains original completion/release responsibilities")
  def pendingForPreStop: Boolean =
    owner()
    entries.valuesIterator.flatMap(_.failure).take(1).foreach(throw _)
    unmatched.iterator.map(_.result).collectFirst { case Failure(cause) => cause }.foreach(throw _)
    require(!retired && unmatched.isEmpty, "request-read ledger retired or unmatched before drain")
    entries.nonEmpty
  def retire(): Unit = { owner(); retired = true }
  def retained: Int = { owner(); entries.size }
  def unresolved: Vector[Throwable] = { owner(); entries.valuesIterator.flatMap(_.failure).toVector }
  def unmatchedCompletions: Vector[IoCompletion[MySqlOwnedIoReply]] = { owner(); unmatched }
  private def owned(entry: Entry): Unit =
    require(entry != null && entries.get(entry.tag).exists(_ eq entry), "foreign or released request-read entry")

/** Main的真实owned command/complete入口；下一async Release/Recall必须消费此标量预览并重新critical CAS。
  * 没有同步store、第二Session、Ready、网络Accepted或caller callback进入worker。
  */
private[floatingpod] final class MainOwnedRequestReadPreview(
    adapter: MySqlOwnedIoAdapter, main: Thread, capacity: Int = 256, drainBound: Int = 32):
  require(adapter != null && main != null && capacity > 0 && capacity <= 256 &&
    drainBound > 0 && drainBound <= capacity, "bounded original adapter/Main required")
  private val identity = adapter.identity
  private val ledger = new MainRequestReadLedger(identity, main, capacity)
  private final class Owned(val entry: ledger.Entry):
    var admission = Option.empty[MySqlOwnedAdmissionFreshness]
    var plan = Option.empty[MySqlOwnedRepositoryOperationPlan]
    var handle = Option.empty[MySqlRetainedPlanHandle]
    var receipt = Option.empty[MySqlRetainedPlanDropReceipt]
    var rejectedAt = Option.empty[Long]
    var releaseAccepted = false
    var droppedAt = Option.empty[Long]
  private val owned = mutable.LinkedHashMap.empty[IoOperationTag, Owned]
  private var ordinal = 0L
  private var lastBatch = Vector.empty[IoCompletion[MySqlOwnedIoReply]]
  private def owner(): Unit = require(Thread.currentThread() eq main, "read-preview requires actual Main owner")
  private def fresh(): MySqlOwnedAdmissionFreshness =
    owner()
    val observation = adapter.pollAdmissionMaintenance
    observation.failure.foreach(cause => throw cause)
    val checkpoint = adapter.checkpoint.io
    checkpoint.fatalCause.foreach(cause => throw cause)
    require(observation.isCurrent && checkpoint.accepting, "actual request-read admission unavailable")
    val current = observation.latest.get
    require(current.bootId == identity.bootId && current.bootGeneration == identity.bootGeneration &&
      current.connectionGeneration == identity.connectionGeneration, "foreign request-read generation")
    current
  private def sameAdmission(original: MySqlOwnedAdmissionFreshness, current: MySqlOwnedAdmissionFreshness): Unit =
    require(original != null && current != null && original.serverId == current.serverId &&
      original.schema == current.schema && original.bindingId == current.bindingId &&
      original.writerEpoch == current.writerEpoch && original.profile == current.profile &&
      original.physicalFormat == current.physicalFormat, "request-read admission incarnation changed")
  def submit(tag: IoOperationTag, request: MySqlRequestReadPreview,
      workflow: UUID, reserveOperation: UUID): IoSubmission =
    submitRead(tag,request,workflow,reserveOperation)
  def submitOriginal(tag:IoOperationTag,request:MySqlRepositoryOperation.FindOriginalDeploymentOperation,
      workflow:UUID,reserveOperation:UUID):IoSubmission =
    submitRead(tag,request,workflow,reserveOperation)
  private def submitRead(tag:IoOperationTag,
      request:MySqlRequestReadPreview | MySqlRepositoryOperation.FindOriginalDeploymentOperation,
      workflow:UUID,reserveOperation:UUID):IoSubmission =
    owner(); identity.requireTag(tag)
    require(workflow != null && reserveOperation != null, "fixed original planning IDs required")
    val originalAdmission = fresh()
    val reserved = ledger.begin(tag, request, adapter.repositorySubmissionEpoch)
    if reserved.isEmpty then return IoSubmission.RejectedCapacity
    val value = new Owned(reserved.get)
    value.admission = Some(originalAdmission)
    owned.put(tag, value) // 原IDs/Entry先保留，reserve编码或enqueue失败仍有责任。
    try
      val plan = request match
        case preview:MySqlRequestReadPreview=>adapter.reserveRepositoryTransactionPlan(tag,workflow,reserveOperation,preview.operations)
        case original:MySqlRepositoryOperation.FindOriginalDeploymentOperation=>adapter.reserveRepositoryPlan(tag,workflow,reserveOperation,original)
      value.plan = Some(plan)
      value.handle = plan.originalRetainedHandle
      val command=request match
        case preview:MySqlRequestReadPreview=>MySqlOwnedIoCommand.ReadRequestPreview(preview,plan)
        case original:MySqlRepositoryOperation.FindOriginalDeploymentOperation=>MySqlOwnedIoCommand.ReadOriginalDeployment(original,plan)
      val result = adapter.submit(tag,command)
      ledger.submitted(value.entry, result)
      if result != IoSubmission.Accepted then
        value.plan = None
        value.rejectedAt = Some(ordinal)
      result
    catch
      case cause: Throwable =>
        ledger.fail(value.entry, cause)
        throw cause
  def onEnd(endOrdinal: Long): Unit =
    owner(); require(endOrdinal > ordinal, "strictly advancing original pump END required")
    ordinal = endOrdinal
    try
      val current = fresh()
      owned.values.foreach(value => value.admission.foreach(sameAdmission(_, current)))
    catch
      case cause: Throwable =>
        owned.values.foreach(value => ledger.fail(value.entry, cause))
        ledger.retire()
        throw cause
    ledger.observeEndEpoch(adapter.repositorySubmissionEpoch)
    // 上一END drop的plan/完整reply已退出调用栈；先处理旧release，再新drain。
    owned.values.toVector.foreach { value =>
      if value.rejectedAt.exists(_ < ordinal) && value.entry.failure.isEmpty && ledger.requireCurrentEpoch(value.entry, adapter.repositorySubmissionEpoch) then
        try
          adapter.cancelExplicitlyRejectedPlan(value.handle.getOrElse(
            throw new IllegalStateException("Rejected original handle missing")))
          ledger.discardNotAccepted(value.entry)
          owned.remove(value.entry.tag)
        catch
          case cause: Throwable =>
            ledger.fail(value.entry, cause)
            throw cause
      else if value.droppedAt.exists(_ < ordinal) && value.receipt.isEmpty && value.entry.failure.isEmpty && ledger.requireCurrentEpoch(value.entry, adapter.repositorySubmissionEpoch) then
        try value.receipt = Some(adapter.recordMainPlanDrop(value.handle.getOrElse(
          throw new IllegalStateException("consumed original plan handle missing"))))
        catch
          case cause: Throwable =>
            ledger.fail(value.entry, cause)
            throw cause
      else if value.receipt.nonEmpty && !value.releaseAccepted && value.entry.failure.isEmpty && ledger.requireCurrentEpoch(value.entry, adapter.repositorySubmissionEpoch) then
        try
          val result = adapter.submitRequestReadRelease(value.entry.tag, value.receipt.get)
          if result == IoSubmission.Accepted then value.releaseAccepted = true
          else result match
            case IoSubmission.RejectedCapacity => () // 同receipt未Accepted，可下次END重试。
            case IoSubmission.RejectedStopped(cause) =>
              throw cause.getOrElse(new IllegalStateException("request-read memory release stopped"))
            case _ => throw new IllegalStateException("request-read memory release quiesced")
        catch
          case cause: Throwable =>
            ledger.fail(value.entry, cause)
            throw cause
    }
    lastBatch = adapter.drainRequestRead(drainBound)
    val (releases, reads) = lastBatch.partition(c => owned.get(c.tag).exists(_.releaseAccepted))
    releases.foreach { completion =>
      val value = owned(completion.tag)
      ledger.requireCurrentEpoch(value.entry, adapter.repositorySubmissionEpoch)
      if ledger.released(value.entry, completion) then owned.remove(completion.tag)
    }
    ledger.end(ordinal, adapter.repositorySubmissionEpoch, reads)
    // 只标量decision可外逃，snapshot/state/raw/plan不发布。清强引用再原issuer drop。
    ledger.releaseCandidates.foreach { entry =>
      val value = owned(entry.tag)
      if ledger.requireCurrentEpoch(entry, adapter.repositorySubmissionEpoch) then
        ledger.dropped(entry)
        value.plan = None
        value.droppedAt = Some(ordinal) // 本END局部reads/原batch退出后，下一END才发行drop receipt。
    }
    lastBatch = Vector.empty
  def poll(tag: IoOperationTag): MainRequestReadPoll =
    owner()
    try
      val current = fresh()
      owned.get(tag).foreach(value => value.admission.foreach(sameAdmission(_, current)))
      ledger.poll(tag, adapter.repositorySubmissionEpoch)
    catch
      case cause: Throwable =>
        owned.get(tag).foreach(value => ledger.fail(value.entry, cause))
        ledger.retire()
        throw cause
  def decision(tag: IoOperationTag): Option[MySqlRequestReadDecision] =
    owner()
    try
      val current = fresh()
      owned.get(tag).foreach(value => value.admission.foreach(sameAdmission(_, current)))
      ledger.result(tag, adapter.repositorySubmissionEpoch)
    catch
      case cause: Throwable =>
        owned.get(tag).foreach(value => ledger.fail(value.entry, cause))
        ledger.retire()
        throw cause
  private[floatingpod] def requireSettledForPreStop(): Unit =
    require(!pendingForPreStop, "request-read owner still retains original graph")
  private[floatingpod] def pendingForPreStop: Boolean =
    owner()
    val ledgerPending = ledger.pendingForPreStop
    ledgerPending || owned.nonEmpty || lastBatch.nonEmpty
  def retire(): Unit = { owner(); ledger.retire() }
  def retainedCount: Int = { owner(); owned.size }
  def retainedCauses: Vector[Throwable] = { owner(); ledger.unresolved }
