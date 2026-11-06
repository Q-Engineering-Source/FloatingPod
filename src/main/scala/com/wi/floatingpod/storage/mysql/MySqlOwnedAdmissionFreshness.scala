package com.wi.floatingpod.storage.mysql

import java.time.Duration
import java.util.UUID

/** 原实际观察的窗口；projection 没有延长/续约方法，也不是业务 admission token。 */
final case class MySqlAdmissionWindow(observedAtNanos: Long, validityNanos: Long):
  require(validityNanos > 0L && validityNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos,
    "admission window must be positive and bounded")
  private[mysql] def remainingAt(now: Long): Long =
    val elapsed = now - observedAtNanos
    if elapsed < 0L || elapsed >= validityNanos then 0L else validityNanos - elapsed

/** 不可变只读 facts；只能由 package 的已验证 receipt 投影，不携带 JDBC/token/holder。
  * Main 只能观察过期，不能把 projection 用作 SQL、physical custody 或 GameReady 权限。
  */
final class MySqlOwnedAdmissionFreshness private[mysql] (
    val bootId: UUID, val bootGeneration: Long, val connectionGeneration: Long,
    val serverId: UUID, val schema: String, val bindingId: UUID, val writerEpoch: Long,
    val profile: MySqlSchemaAuditProfile, val physicalFormat: MySqlExpectedPhysicalFormat,
    val renewSequence: Long, val fenceLease: MySqlAdmissionWindow,
    val receiptLease: MySqlAdmissionWindow, val audit: MySqlAdmissionWindow
):
  require(bootId != null && serverId != null && schema != null && schema.nonEmpty && bindingId != null &&
    bootGeneration > 0L && connectionGeneration > 0L && writerEpoch > 0L && renewSequence >= 0L &&
    profile != null && physicalFormat != null && fenceLease != null && receiptLease != null && audit != null,
    "exact immutable admission projection required")
  def isCurrent: Boolean = isCurrentAt(System.nanoTime())
  private[mysql] def isCurrentAt(now: Long): Boolean = remainingAt(now) > 0L
  private[mysql] def remainingAt(now: Long): Long =
    math.min(fenceLease.remainingAt(now), math.min(receiptLease.remainingAt(now), audit.remainingAt(now)))
  private[mysql] def nextMaintenanceAfter(now: Long): Duration =
    val remaining = remainingAt(now)
    require(remaining / 2L >= 1000000L, "admission projection expired or renewal margin is below 1ms; window is not extended")
    Duration.ofNanos(remaining / 2L)
  override def toString: String = "MySqlOwnedAdmissionFreshness[observed windows; not Ready]"

/** capacity-one 最新成功 facts + sticky 原 failure；普通 completions/reservation 独立保留。 */
final case class MySqlOwnedMaintenanceObservation(
    latest: Option[MySqlOwnedAdmissionFreshness], failure: Option[Throwable], stopped: Boolean = false
):
  def isCurrent: Boolean = !stopped && failure.isEmpty && latest.exists(_.isCurrent)

private[mysql] final class MySqlOwnedMaintenanceMailbox(identity: MySqlOwnedIoIdentity):
  require(identity != null, "exact owned mailbox identity required")
  private var latest = Option.empty[MySqlOwnedAdmissionFreshness]
  private var failure = Option.empty[Throwable]
  private var stopped = false

  def publish(value: MySqlOwnedAdmissionFreshness): Boolean = synchronized {
    require(value != null && value.bootId == identity.bootId && value.bootGeneration == identity.bootGeneration &&
      value.connectionGeneration == identity.connectionGeneration, "maintenance projection has wrong owned generation")
    if failure.nonEmpty || stopped then false
    else latest match
      case Some(old) =>
        require(value.serverId == old.serverId && value.schema == old.schema && value.bindingId == old.bindingId &&
          value.writerEpoch == old.writerEpoch && value.profile == old.profile && value.physicalFormat == old.physicalFormat,
          "renewal must preserve exact database/profile/epoch identity")
        if value.renewSequence <= old.renewSequence then false
        else
          require(value.isCurrent, "expired admission windows cannot replace current maintenance facts")
          latest = Some(value)
          true
      case None =>
        require(value.renewSequence == 0L, "initial admission publication must have sequence zero")
        require(value.isCurrent, "initial admission windows expired before publication")
        latest = Some(value)
        true
  }

  /** 只读工作消费原窗口，不把同sequence重新发布成一次续租。 */
  private[mysql] def requireCurrentRead(value:MySqlOwnedAdmissionFreshness):Unit = synchronized {
    val old=latest.getOrElse(throw new IllegalStateException("actual current maintenance windows missing"))
    require(!stopped && failure.isEmpty && old.isCurrent && value!=null && value.isCurrent &&
      value.bootId==identity.bootId && value.bootGeneration==identity.bootGeneration &&
      value.connectionGeneration==identity.connectionGeneration && value.serverId==old.serverId &&
      value.schema==old.schema && value.bindingId==old.bindingId && value.writerEpoch==old.writerEpoch &&
      value.profile==old.profile && value.physicalFormat==old.physicalFormat &&
      value.renewSequence==old.renewSequence && value.fenceLease==old.fenceLease &&
      value.receiptLease==old.receiptLease && value.audit==old.audit,
      "read-only work requires exact current generation/writer/sequence/windows without renewal")
  }

  /** 实际新服务owner的首个窗口可已续租；只向私有登记source取值，不能由caller伪造或归零sequence。 */
  private[mysql] def initializeRecoveredDbFirst(source:MySqlDbFirstSession.RetiredRecallWriterRecovery):Unit = synchronized {
    require(source!=null && latest.isEmpty && failure.isEmpty && !stopped,
      "actual registered service source and unused new mailbox required")
    val value=source.currentFreshnessForServiceMailbox(this,identity)
    require(value.isCurrent,"actual service mailbox initial windows expired")
    latest=Some(value)
  }

  def fail(cause: Throwable): Unit = synchronized {
    require(cause != null, "original maintenance failure required")
    failure = MySqlOwnedSessionLifecycle.combine(failure, cause)
  }
  def stop(): Unit = synchronized { stopped = true }
  /** 原producer结束后撤latest强边；诊断cause仍保留，不签业务或退出成功。 */
  private[mysql] def retireAfterTerminalProducer(): Unit = synchronized {
    if !stopped then throw new IllegalStateException("original maintenance mailbox was not stopped")
    latest=None
  }
  def poll: MySqlOwnedMaintenanceObservation = synchronized { MySqlOwnedMaintenanceObservation(latest, failure, stopped) }
