package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import scala.collection.mutable

/** format3 模式投影的 worker 保留账；budget 不得随只读图逃逸。 */
private[mysql] final class MySqlModeProjectionBudgetRegistry(
    ownerThread: Thread,
    bootId: UUID,
    bootGeneration: Long,
    connectionGeneration: Long
):
  require(ownerThread != null && bootId != null && bootGeneration > 0L && connectionGeneration > 0L,
    "exact owned worker identity is required for mode projection retention")

  private final class Retained(val root: MySqlFormat3ReadBudget):
    val children = mutable.ArrayBuffer.empty[MySqlFormat3ReadBudget]
    val releaseAttempt = new MySqlModeProjectionReleaseAttempt(ownerThread)
    def releaseAttempted: Boolean = releaseAttempt.hasAttempted
    var witnessDraft: MySqlModeOriginalWitnessDraft = null

  private val retained = mutable.LinkedHashMap.empty[IoOperationTag, Retained]

  def retainRoot(tag: IoOperationTag, root: MySqlFormat3ReadBudget): Unit =
    requireCurrent(tag)
    require(root != null && !root.isScopedReadBudget && !root.isClosed,
      "mode projection root must be the exact open workflow budget")
    require(!retained.contains(tag), "exact mode operation already owns a retained budget")
    retained.put(tag, new Retained(root))

  def retainScoped(tag: IoOperationTag, child: MySqlFormat3ReadBudget): Unit =
    requireCurrent(tag)
    val entry = retained.getOrElse(tag,
      throw new IllegalStateException("mode projection has no worker-retained workflow root"))
    require(child != null && child.isScopedReadBudget && !child.isClosed &&
      entry.children.forall(_ ne child) && entry.root.sharesAllocationLedger(child) && child.isCanonicalChildOf(entry.root),
      "mode projection child must be a distinct open scope on the exact workflow allocation ledger")
    require(!entry.releaseAttempted, "mode projection budget release already started")
    entry.children += child

  def retainWitnessDraft(tag: IoOperationTag, draft: MySqlModeOriginalWitnessDraft): Unit =
    requireCurrent(tag)
    val entry = retained.getOrElse(tag, throw new IllegalStateException("mode witness has no original retained root"))
    require(!entry.releaseAttempted && entry.witnessDraft == null && draft != null && draft.tag == tag &&
      (draft.originalRoot eq entry.root), "exact original tag/root requires one registered witness draft")
    draft.requireOwner()
    entry.witnessDraft = draft

  private[mysql] def retainedWitnessDraft(tag: IoOperationTag): Option[MySqlModeOriginalWitnessDraft] =
    requireCurrent(tag)
    retained.get(tag).flatMap(entry => Option(entry.witnessDraft))

  def release(tag: IoOperationTag): Unit =
    requireCurrent(tag)
    val entry = retained.getOrElse(tag,
      throw new IllegalArgumentException("no retained mode projection budget exists for this exact tag"))
    entry.releaseAttempt.requireNotAttempted()
    if entry.witnessDraft != null then entry.witnessDraft.requireDroppable()
    entry.releaseAttempt.run(
      () => {
        if entry.witnessDraft != null then entry.witnessDraft.dropForExactProjectionRelease()
        entry.root.endProjectionBorrowGroup()
      },
      () => entry.children.foreach(_.closeScopedReadBudget()),
      () => entry.root.closeRootReadBudget())
    retained.remove(tag)
    ()

  private[mysql] def retainedReleaseFailure(tag: IoOperationTag): Option[Throwable] =
    requireCurrent(tag)
    retained.get(tag).flatMap(entry => entry.releaseAttempt.retainedFailure)

  def contains(tag: IoOperationTag): Boolean =
    requireOwner()
    retained.contains(tag)

  def pendingCount: Int =
    requireOwner()
    retained.size

  private def requireCurrent(tag: IoOperationTag): Unit =
    requireOwner()
    require(tag != null && tag.bootId == bootId && tag.bootGeneration == bootGeneration &&
      tag.connectionGeneration == connectionGeneration, "mode projection release tag differs from exact owned worker generation")

  private def requireOwner(): Unit =
    require(Thread.currentThread() eq ownerThread, "format3 mode projection budgets are owned by the actual IO thread")

/** 纯 checked-close 责任 seam。实际registry只传自身draft/children/root；不开放替换业务/JDBC入口。 */
private[mysql] final class MySqlModeProjectionReleaseAttempt(ownerThread: Thread):
  private var attempted = false
  private var failure: Throwable = null
  private def requireOwner(): Unit =
    require(Thread.currentThread() eq ownerThread, "exact projection release owner required")
  def hasAttempted: Boolean =
    requireOwner()
    attempted
  def retainedFailure: Option[Throwable] =
    requireOwner()
    Option(failure)
  def requireNotAttempted(): Unit =
    requireOwner()
    if failure != null then
      throw new IllegalStateException("mode projection checked-close failure remains retained; no second close", failure)
    require(!attempted, "duplicate mode projection release acknowledgement")
  def run(dropRefs: () => Unit, closeChildren: () => Unit, closeRoot: () => Unit): Unit =
    requireNotAttempted()
    require(dropRefs != null && closeChildren != null && closeRoot != null, "actual finite release actions required")
    attempted = true
    try
      dropRefs()
      closeChildren()
      closeRoot()
    catch
      case cause: Throwable =>
        failure = cause
        throw cause
