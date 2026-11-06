package com.wi.floatingpod.server

/** Main 本地的实际框架集合快照；引用不能跨 IO，也不能转换成磁盘完成证明。 */
private[server] final case class TicketReleaseSnapshot(
    registeredTickets: Vector[AnyRef], forcedTickets: Vector[AnyRef], requestedChunks: Set[(Int, Int)])

private[server] trait TicketReleaseView:
  def assertMain(): Unit
  def release(): Unit
  def snapshot(): TicketReleaseSnapshot

/** Forge release 的 void 返回不是 ACK；完整观察通过后调用方才可移除本地权属。 */
private[server] object ExactTicketReleaseObserver:
  def release(ticket: AnyRef, view: TicketReleaseView): Unit =
    require(ticket != null && view != null, "exact ticket release view required")
    view.assertMain()
    view.release()
    view.assertMain()
    requireAbsent(ticket, view.snapshot())

  def requireAbsent(ticket: AnyRef, after: TicketReleaseSnapshot): Unit =
    require(ticket != null && after != null, "exact actual ticket observation required")
    if after.registeredTickets.exists(_ eq ticket) || after.forcedTickets.exists(_ eq ticket) ||
        after.requestedChunks.nonEmpty then
      throw new IllegalStateException("Exact Forge ticket release retains registry, forced mapping or requested chunks")
