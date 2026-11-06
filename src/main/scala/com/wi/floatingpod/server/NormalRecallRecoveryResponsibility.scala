package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.MySqlOwnedIoAdapter

/** 原Main只保留恢复路由与END责任。未完成C决策/physical/P2/G时没有publication出口。 */
private[floatingpod] final class NormalRecallRecoveryResponsibility private (
    owner: Thread, server:net.minecraft.server.MinecraftServer,
    link: MySqlOwnedIoAdapter.RecallRecoveryLink,
    projection:MainBootProjection,
    continuation:Option[MainBootProjection.SourceContinuationLease],
    source:Option[NormalRecallRecoverySourceCustody]):
  private var stopped = false
  private var primary = Option.empty[Throwable]
  private var lastEnd = -1L
  private var initialSourceObservation=Option.empty[MainBootProjection.SourceContinuationObservation]
  private def current(): Unit =
    require(Thread.currentThread() eq owner, "original Recall recovery Main required")
    primary.foreach(throw _)
    require(!stopped, "original Recall recovery holder stopped")
    continuation match
      case Some(lease)=>lease.requireResponsibilityOwner(server,link)
      case None=>
        projection.requireCurrentReads()
        require(projection.recallRecoveryLink.exists(_ eq link), "original restricted Boot link changed")

  def onEnd(tick: PermanentEndTick): Unit =
    try
      current()
      require(tick != null && tick.phase == PermanentPumpPhase.Recovering &&
        tick.bootUuid == link.identity.bootId && tick.bootGeneration == link.identity.bootGeneration && tick.endOrdinal > lastEnd,
        "restricted Recall requires original Recovering END")
      // 后续C/P2/G工作只可接入这个原holder；当前不把有限inventory当行动许可。
      require(link.references.nonEmpty, "restricted Recall lost its original workflow inventory")
      source.foreach { custody=>
        custody.captureCurrentDomain()
        continuation.foreach { lease=>
          if initialSourceObservation.isEmpty then
            initialSourceObservation=Some(lease.observeInitialSourceDomain(custody.currentDomainObservation()))
          else custody.requireCurrentDomain()
        }
      }
      lastEnd = tick.endOrdinal
    catch
      case cause: Throwable =>
        if primary.isEmpty then primary = Some(cause)
        else if primary.get ne cause then primary.get.addSuppressed(cause)
        throw primary.get

  def stop(): Unit =
    require(Thread.currentThread() eq owner, "original Recall stop owner required")
    stopped = true

private[floatingpod] object NormalRecallRecoveryResponsibility:
  def retain(projection: MainBootProjection,server:net.minecraft.server.MinecraftServer): NormalRecallRecoveryResponsibility =
    require(projection != null, "actual claimed Boot projection required")
    require(server != null, "actual dedicated Main server required")
    val link = projection.recallRecoveryLink.getOrElse(
      throw new IllegalStateException("original restricted Recall Boot required"))
    val source=projection.retainRecallSourceCustody(server)
    val continuation=projection.retainAvailableRecallSourceContinuation(server)
    continuation.foreach(_.requireResponsibilityOwner(server,link))
    require(source.isDefined==continuation.isDefined,
      "source custody and source continuation must retain the same restricted Boot allocation")
    new NormalRecallRecoveryResponsibility(Thread.currentThread(),server,link,projection,continuation,source)
