package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.B1ResumeRecord
import java.sql.{SQLNonTransientConnectionException, SQLTransactionRollbackException}
import java.time.LocalDateTime
import java.util.Arrays
import scala.jdk.CollectionConverters.*

enum MySqlFreshPreparePhase:
  case New, Planning, Planned, PublishingGenesis, GenesisDurable, Executing, SqlAcknowledged, ReadbackConfirmed, PreparedConfirmed, Frozen

/** 仅实际actor调用事实；不是SQL/native/custody能力，关闭连接不清除其责任。 */
final case class MySqlFreshPrepareAttemptObservation(
    phase: MySqlFreshPreparePhase, plan: Option[MySqlFreshPrepareCapabilities.PlannedFreshPrepare],
    genesis: Option[MySqlFreshPrepareCapabilities.DurableFreshGenesis],
    genesisPublicationAttempted: Boolean, executionAttempted: Boolean,
    sqlCommitAcknowledged: Boolean, fullReadbackConfirmed: Boolean, nativeConfirmationAcknowledged: Boolean,
    failure: Option[Throwable], draftRecord: Option[B1ResumeRecord] = None, failureStage: Option[String] = None
):
  def retainedResponsibility: Boolean = plan.nonEmpty || draftRecord.nonEmpty || genesisPublicationAttempted || executionAttempted
  override def toString: String = "MySqlFreshPrepareAttemptObservation[retained; not Ready or settlement]"

/** 唯一attempt状态，行为测试直接核验；状态boolean从不签发三项opaque能力。 */
private[mysql] final class MySqlFreshPrepareLifecycle(owner: Thread):
  require(owner != null, "actual owned IO owner required")
  private var phase = MySqlFreshPreparePhase.New
  private var primary: Throwable = null
  private var failureStage: Option[String] = None
  private var genesisAttempted = false
  private var executionAttempted = false
  private var commitAck = false
  private var readback = false
  private var confirmation = false
  private def owned(): Unit = require(Thread.currentThread() eq owner, "exact Fresh prepare owner required")
  private def next(expected: MySqlFreshPreparePhase, target: MySqlFreshPreparePhase): Unit =
    owned()
    if primary != null then throw primary
    if phase != expected then throw new SQLNonTransientConnectionException("Fresh prepare phase cannot be retried or skipped")
    phase = target
  def beginPlanning(): Unit = next(MySqlFreshPreparePhase.New, MySqlFreshPreparePhase.Planning)
  def planningCleaned(): Unit = next(MySqlFreshPreparePhase.Planning, MySqlFreshPreparePhase.Planned)
  def beginGenesis(): Unit =
    next(MySqlFreshPreparePhase.Planned, MySqlFreshPreparePhase.PublishingGenesis); genesisAttempted = true
  def genesisDurable(): Unit = next(MySqlFreshPreparePhase.PublishingGenesis, MySqlFreshPreparePhase.GenesisDurable)
  def beginExecution(): Unit =
    next(MySqlFreshPreparePhase.GenesisDurable, MySqlFreshPreparePhase.Executing); executionAttempted = true
  def sqlAcknowledged(): Unit =
    next(MySqlFreshPreparePhase.Executing, MySqlFreshPreparePhase.SqlAcknowledged); commitAck = true
  def readbackConfirmed(): Unit =
    next(MySqlFreshPreparePhase.SqlAcknowledged, MySqlFreshPreparePhase.ReadbackConfirmed); readback = true
  def nativeConfirmed(): Unit =
    next(MySqlFreshPreparePhase.ReadbackConfirmed, MySqlFreshPreparePhase.PreparedConfirmed); confirmation = true
  def fail(failure: Throwable): Unit = fail("owned-prepare-boundary", failure)
  def fail(stage: String, failure: Throwable): Unit =
    owned(); require(stage != null && stage.nonEmpty && failure != null, "exact stage and original Fresh failure required")
    if primary == null then
      primary = failure
      failureStage = Some(stage)
    else if primary ne failure then MySqlFencedTransactionSupport.addSuppressed(primary, failure)
    phase = MySqlFreshPreparePhase.Frozen
  def observe(plan: Option[MySqlFreshPrepareCapabilities.PlannedFreshPrepare],
      genesis: Option[MySqlFreshPrepareCapabilities.DurableFreshGenesis], draftRecord: Option[B1ResumeRecord] = None): MySqlFreshPrepareAttemptObservation =
    owned()
    MySqlFreshPrepareAttemptObservation(phase, plan, genesis, genesisAttempted, executionAttempted,
      commitAck, readback, confirmation, Option(primary), draftRecord, failureStage)

private[mysql] object MySqlFreshPrepareRules:
  def writer(row: MySqlBusinessWriterRow): MySqlFreshPrepareCapabilities.WriterTuple =
    new MySqlFreshPrepareCapabilities.WriterTuple(row.bindingUuid, row.writerEpoch, row.ownerBootUuid.orNull,
      row.leaseUntil, row.lastOperationUuid.orNull, row.lastFingerprint.map(_.toArray).orNull)
  def requireWriter(expected: MySqlFreshPrepareCapabilities.WriterTuple, actual: MySqlBusinessWriterRow): Unit =
    if !expected.exactlyEquals(writer(actual)) then throw new SQLTransactionRollbackException("Exact six-field planned W tuple changed")
  def requireUnexpired(lease: LocalDateTime, databaseNow: LocalDateTime): Unit =
    if !lease.isAfter(databaseNow) then throw new SQLTransactionRollbackException("Original planned prepare lease expired; native pending retained")
  def exactBytes(expected: Array[Byte], actual: Array[Byte]): Unit =
    if expected == null || actual == null || !Arrays.equals(expected, actual) then
      throw new SQLNonTransientConnectionException("Complete planned native record differs from actual ACK bytes")
  def operation(context: MySqlOperationContext): B1ResumeRecord.OperationAssertions =
    new B1ResumeRecord.OperationAssertions(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid,
      context.expectedEffects.map(e => new B1ResumeRecord.EffectAssertions(e.targetIdentity, e.expectedVersion, e.desiredEffectFingerprintSha256.toArray)).asJava,
      context.canonicalFingerprintSha256.toArray)

/** 两个明确的清理动作，不接Connection或任意SQL；production adapter持有actual同owner会话。 */
private[mysql] trait MySqlFreshPrepareCleanupActions:
  def rollback(): Unit
  def restoreAndObserveIdle(): Unit

private[mysql] object MySqlFreshPrepareReadOnlyCleanup:
  def finish(actions: MySqlFreshPrepareCleanupActions, bodyFailure: Option[Throwable]): Either[Throwable, Unit] =
    require(actions != null && bodyFailure != null, "actual readonly cleanup actions required")
    var primary = bodyFailure.orNull
    var rollbackClean = false
    try
      actions.rollback(); rollbackClean = true
    catch case cleanup: Throwable =>
      if primary == null then primary = cleanup else MySqlFencedTransactionSupport.addSuppressed(primary, cleanup)
    if !rollbackClean then Left(primary)
    else
      try
        actions.restoreAndObserveIdle()
        Right(())
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlFencedTransactionSupport.addSuppressed(primary, cleanup)
        Left(primary)
