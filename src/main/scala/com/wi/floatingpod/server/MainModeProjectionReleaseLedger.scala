package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoIdentity, MySqlOwnedIoReply, MySqlRetainedPlanDropReceipt, MySqlRetainedPlanHandle, MySqlOwnedIoAdapter}
import scala.collection.mutable
import scala.util.{Failure, Success}

private[server] enum MainModeProjectionReleaseResult:
  case Released(tag: IoOperationTag)
  case Failed(tag: IoOperationTag, cause: Throwable)
  case Unmatched(completion: IoCompletion[MySqlOwnedIoReply])

/** Main retains only exact operation tags while worker-owned allocation leases await acknowledgement. */
private[server] final class MainModeProjectionReleaseLedger(
    ownerThread: Thread,
    identity: MySqlOwnedIoIdentity,
    capacity: Int
):
  require(ownerThread != null && identity != null && capacity >= 1 && capacity <= 256,
    "bounded exact Main projection-release ledger is required")

  private enum State:
    case Prepared, Ready, RejectedDropped, Submitted, Terminal

  private final class Entry(val modeProjection: Boolean):
    var state = State.Prepared
    var receipt = Option.empty[MySqlRetainedPlanDropReceipt]
    var originalHandle = Option.empty[MySqlRetainedPlanHandle]
    var failure = Option.empty[Throwable]
  private val entries = mutable.LinkedHashMap.empty[IoOperationTag, Entry]
  private var unmatchedValues = Vector.empty[IoCompletion[MySqlOwnedIoReply]]

  def reserve(tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(!entries.contains(tag) && entries.size < capacity, "bounded one-shot mode projection release tag required")
    entries.put(tag, new Entry(true))

  def reserveOwned(tag: IoOperationTag, handle: MySqlRetainedPlanHandle, modeProjection: Boolean): Unit =
    requireOwner(); identity.requireTag(tag)
    require(handle != null && handle.tag == tag && !entries.contains(tag) && entries.size < capacity,
      "bounded exact original handle must be retained before Main graph drop")
    val entry = new Entry(modeProjection)
    entry.originalHandle = Some(handle)
    entries.put(tag, entry)

  /** 明确Rejected的无执行责任仍占同一个bounded Entry；本轮旧plan局部引用退出后才取消。 */
  def retainRejectedDrop(tag: IoOperationTag, handle: MySqlRetainedPlanHandle): Unit =
    reserveOwned(tag, handle, false)
    entries(tag).state = State.RejectedDropped

  def rejectedDropsAtEndStart(): Vector[IoOperationTag] =
    requireOwner()
    entries.iterator.collect { case (tag, entry) if entry.state == State.RejectedDropped => tag }.toVector

  def settleRejectedDrop(adapter: MySqlOwnedIoAdapter, tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(adapter != null && adapter.identity == identity &&
      entries.get(tag).exists(_.state == State.RejectedDropped), "same actual adapter and Rejected drop Entry required")
    val entry = entries(tag)
    try
      adapter.cancelExplicitlyRejectedPlan(entry.originalHandle.get)
      entries.remove(tag)
      ()
    catch
      case cause: Throwable =>
        entry.failure = Some(cause)
        entry.state = State.Terminal
        throw cause

  /** 实际Entry/owner引用先保留；graph/plan refs撤销后由原Adapter issuer一次发行。 */
  def armDropped(adapter: MySqlOwnedIoAdapter, tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(adapter != null && adapter.identity == identity && entries.get(tag).exists(_.state == State.Prepared),
      "original actual adapter and prepared Entry required")
    val entry = entries(tag)
    val handle = entry.originalHandle.getOrElse(throw new IllegalStateException("actual original issuer handle missing"))
    try arm(tag, adapter.recordMainPlanDrop(handle))
    catch
      case cause: Throwable =>
        entry.failure = Some(cause)
        entry.state = State.Terminal
        throw cause

  def reserveOrdinary(tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(!entries.contains(tag) && entries.size < capacity, "bounded original ordinary release required")
    entries.put(tag, new Entry(false))

  def arm(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): Unit =
    requireOwner(); identity.requireTag(tag)
    require(receipt != null && receipt.tag == tag && entries.get(tag).exists(_.state == State.Prepared),
      "exact actual Main drop receipt must match original bounded Entry")
    entries(tag).receipt = Some(receipt)
    entries(tag).state = State.Ready

  def actualReceipt(tag: IoOperationTag): MySqlRetainedPlanDropReceipt =
    requireOwner(); identity.requireTag(tag)
    entries.getOrElse(tag, throw new IllegalArgumentException("unknown original release Entry")).receipt.getOrElse(
      throw new IllegalStateException("DATA-only release state has no issued drop receipt"))

  def isModeProjection(tag: IoOperationTag): Boolean =
    requireOwner(); identity.requireTag(tag)
    entries.getOrElse(tag, throw new IllegalArgumentException("unknown original release Entry")).modeProjection

  /** 纯state seam；没有actualReceipt不能向actual Adapter提交结算。production调用receipt overload。 */
  def arm(tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(entries.get(tag).exists(_.state == State.Prepared), "only a prepared exact release may be armed")
    entries(tag).state = State.Ready

  def cancelUnsubmitted(tag: IoOperationTag): Unit =
    requireOwner(); identity.requireTag(tag)
    require(entries.get(tag).exists(_.state == State.Prepared), "only an unarmed projection release can be cancelled")
    entries.remove(tag)
    ()

  def readyTags: Vector[IoOperationTag] =
    requireOwner()
    entries.iterator.collect { case (tag, entry) if entry.state == State.Ready => tag }.toVector

  /** drain/publication 前捕获；本轮稍后 arm 的 tag 必须等待下一次 END。 */
  def retainedFailureCauses: Vector[Throwable] = { requireOwner(); entries.valuesIterator.flatMap(_.failure).toVector }
  def snapshotReadyAtEndStart(): Vector[IoOperationTag] = readyTags

  def submitted(tag: IoOperationTag, result: IoSubmission): Unit =
    requireOwner(); identity.requireTag(tag)
    require(result != null && entries.get(tag).exists(_.state == State.Ready), "release submission must match one prepared exact tag")
    result match
      case IoSubmission.Accepted => entries(tag).state = State.Submitted
      case IoSubmission.RejectedCapacity => ()
      case IoSubmission.RejectedQuiescing => entries(tag).state = State.Terminal
      case IoSubmission.RejectedStopped(_) => entries(tag).state = State.Terminal

  def submissionFailed(tag: IoOperationTag, cause: Throwable): Unit =
    requireOwner(); identity.requireTag(tag)
    require(cause != null && entries.get(tag).exists(_.state == State.Ready),
      "submission failure must retain the original unsubmitted release responsibility")
    entries(tag).state = State.Terminal

  def isSubmitted(tag: IoOperationTag): Boolean =
    requireOwner()
    entries.get(tag).exists(_.state == State.Submitted)

  def consume(completion: IoCompletion[MySqlOwnedIoReply]): MainModeProjectionReleaseResult =
    requireOwner()
    require(completion != null, "actual immutable projection-release completion is required")
    val result = entries.get(completion.tag) match
      case Some(entry) if entry.state == State.Submitted => completion.result match
        case Success(MySqlOwnedIoReply.ModeProjectionReleased(tag)) if tag == completion.tag && entry.modeProjection =>
          entries.remove(tag)
          MainModeProjectionReleaseResult.Released(tag)
        case Success(MySqlOwnedIoReply.RetainedPlanReleased(tag)) if tag == completion.tag && !entry.modeProjection =>
          entries.remove(tag)
          MainModeProjectionReleaseResult.Released(tag)
        case Success(MySqlOwnedIoReply.RetainedPlanReleaseFailed(tag, cause))
            if tag == completion.tag && cause != null && !entry.modeProjection =>
          entries(tag).state = State.Terminal
          MainModeProjectionReleaseResult.Failed(tag, cause)
        case Success(MySqlOwnedIoReply.ModeProjectionReleaseFailed(tag, cause))
            if tag == completion.tag && cause != null =>
          entries(tag).state = State.Terminal
          MainModeProjectionReleaseResult.Failed(tag, cause)
        case Failure(cause) =>
          entries(completion.tag).state = State.Terminal
          entries(completion.tag).failure = Some(cause)
          MainModeProjectionReleaseResult.Failed(completion.tag, cause)
        case _ => MainModeProjectionReleaseResult.Unmatched(completion)
      case _ => MainModeProjectionReleaseResult.Unmatched(completion)
    result match
      case MainModeProjectionReleaseResult.Unmatched(value) =>
        unmatchedValues :+= value
        require(unmatchedValues.size <= capacity, "unmatched projection-release completion exceeded Main bound")
      case _ => ()
    result

  def unmatched: Vector[IoCompletion[MySqlOwnedIoReply]] =
    requireOwner()
    unmatchedValues

  def pendingCount: Int =
    requireOwner()
    entries.size

  private def requireOwner(): Unit =
    require(Thread.currentThread() eq ownerThread, "projection-release ledger requires exact Main owner")
