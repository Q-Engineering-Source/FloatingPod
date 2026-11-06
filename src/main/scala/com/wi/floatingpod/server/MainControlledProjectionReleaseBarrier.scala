package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.IoOperationTag

/** Main 的两原tag DATA收口；资格仍来自 actual publication/worker release ledger，不能由本值发行。 */
private[server] final class MainControlledProjectionReleaseBarrier(owner: Thread,
    val prepareTag: IoOperationTag,val commitTag: IoOperationTag):
  require(owner!=null && prepareTag!=null && commitTag!=null && prepareTag!=commitTag &&
    prepareTag.bootId==commitTag.bootId && prepareTag.bootGeneration==commitTag.bootGeneration &&
    prepareTag.connectionGeneration==commitTag.connectionGeneration,"same-session distinct original release tags required")
  private var prepareReleased=false
  private var commitReleased=false
  private var failure=Option.empty[Throwable]
  private def requireOwner(): Unit = require(Thread.currentThread() eq owner,"exact publication Main required")
  def contains(tag: IoOperationTag): Boolean =
    requireOwner();tag==prepareTag || tag==commitTag
  def acknowledge(tag: IoOperationTag): Boolean =
    requireOwner();require(failure.isEmpty && contains(tag),"failed or foreign controlled release ACK")
    if tag==prepareTag then
      require(!prepareReleased,"duplicate prepare release ACK");prepareReleased=true
    else
      require(!commitReleased,"duplicate commit release ACK");commitReleased=true
    prepareReleased && commitReleased
  def fail(tag: IoOperationTag,cause: Throwable): Unit =
    requireOwner();require(contains(tag) && cause!=null,"exact controlled release failure required")
    failure match
      case None => failure=Some(cause)
      case Some(primary) =>
        if (primary ne cause) && !primary.getSuppressed.contains(cause) then primary.addSuppressed(cause)
