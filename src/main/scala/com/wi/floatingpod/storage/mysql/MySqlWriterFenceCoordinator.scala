package com.wi.floatingpod.storage.mysql

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Duration
import java.time.LocalDateTime
import java.util.HexFormat
import java.util.UUID

/** Fence 变更只能由获得该不可变 token 的协调器实例继续操作。 */
final class MySqlWriterFenceToken private[mysql] (
    private[mysql] val verified: VerifiedMySqlConnection,
    private[mysql] val scope: MySqlAuditBootScope,
    private[mysql] val qualification: VerifiedSchemaQualification,
    private[mysql] val fence: MySqlWriterFenceExpectation
):
  val bootUuid: UUID = scope.bootUuid
  val bootGeneration: Long = scope.bootGeneration
  val connectionGeneration: Long = scope.connectionGeneration
  val bindingUuid: UUID = fence.bindingUuid
  val writerEpoch: Long = fence.epoch
  val leaseUntil: LocalDateTime = fence.leaseUntil

  override def toString: String =
    "MySqlWriterFenceToken(boot=" + bootUuid + ", epoch=" + writerEpoch + ", connectionGeneration=" + connectionGeneration + ")"

/** 只有 writer fence 与同一代 post-acquire auditor receipt 同时有效时才可写业务表。 */
final class MySqlBusinessAdmission private[mysql] (
    private[mysql] val token: MySqlWriterFenceToken,
    private[mysql] val receipt: RuntimeSchemaAuditReceipt
):
  val bootUuid: UUID = token.bootUuid
  val writerEpoch: Long = token.writerEpoch
  val bindingUuid: UUID = token.bindingUuid

  override def toString: String =
    "MySqlBusinessAdmission(boot=" + bootUuid + ", epoch=" + writerEpoch + ", connectionGeneration=" + token.connectionGeneration + ")"

enum MySqlWriterFenceReconciliation:
  case ConfirmedCurrent
  case ConfirmedReleased
  case ConfirmedExpired
  case Unproven
  case Conflict

private[mysql] enum MySqlWriterFenceOperation:
  case Acquire
  case Renew
  case Release

private[mysql] final case class MySqlWriterFenceRow(
    bindingUuid: UUID,
    epoch: Long,
    ownerBootUuid: Option[UUID],
    leaseUntil: LocalDateTime,
    lastCommitOperationUuid: Option[UUID],
    lastCommitFingerprint: Option[Vector[Byte]]
):
  require(bindingUuid != null && epoch >= 0L && ownerBootUuid != null && leaseUntil != null, "writer row identity is malformed")
  require(lastCommitOperationUuid != null && lastCommitFingerprint != null, "writer receipt slot is malformed")
  require(lastCommitOperationUuid.isDefined == lastCommitFingerprint.isDefined, "writer receipt slot must be complete or empty")
  lastCommitFingerprint.foreach(hash => require(hash.size == 32, "writer receipt fingerprint must contain 32 bytes"))

private[mysql] object MySqlWriterFenceRules:
  def businessAdmission(phase: MySqlWriterFencePhase): Option[MySqlBusinessAdmission] =
    phase match
      case MySqlWriterFenceAdmitted(admission) => Some(admission)
      case _ => None

  def hasDistinctExpectedTarget(
      expected: Option[MySqlWriterFenceRow],
      previous: Option[MySqlWriterFenceRow]
  ): Boolean =
    expected.exists(candidate => !previous.contains(candidate))

  def matchesTokenRow(
      current: MySqlWriterFenceRow,
      binding: UUID,
      epoch: Long,
      owner: UUID,
      leaseUntil: LocalDateTime
  ): Boolean =
    current.bindingUuid == binding &&
      current.epoch == epoch &&
      current.ownerBootUuid.contains(owner) &&
      current.leaseUntil == leaseUntil

  def nextEpoch(current: Long): Long =
    require(current >= 0L, "writer epoch must not be negative")
    Math.addExact(current, 1L)

  def acquire(
      current: MySqlWriterFenceRow,
      owner: UUID,
      databaseNow: LocalDateTime,
      newLeaseUntil: LocalDateTime
  ): MySqlWriterFenceRow =
    require(owner != null && databaseNow != null && newLeaseUntil != null, "writer acquire inputs are required")
    require(current.ownerBootUuid.isEmpty, "an owned writer row is occupied even when its lease has expired")
    require(!current.leaseUntil.isAfter(databaseNow), "unowned writer lease must already be expired")
    require(newLeaseUntil.isAfter(databaseNow), "new writer lease must be in the future")
    current.copy(epoch = nextEpoch(current.epoch), ownerBootUuid = Some(owner), leaseUntil = newLeaseUntil)

  def renew(
      current: MySqlWriterFenceRow,
      owner: UUID,
      databaseNow: LocalDateTime,
      newLeaseUntil: LocalDateTime
  ): MySqlWriterFenceRow =
    require(owner != null && databaseNow != null && newLeaseUntil != null, "writer renew inputs are required")
    require(current.ownerBootUuid.contains(owner), "writer row belongs to a different boot")
    require(current.leaseUntil.isAfter(databaseNow), "writer lease has expired")
    require(newLeaseUntil.isAfter(databaseNow), "renewed writer lease must be in the future")
    require(newLeaseUntil.isAfter(current.leaseUntil), "renewal must strictly extend the exact current lease")
    current.copy(leaseUntil = newLeaseUntil)

  def release(
      current: MySqlWriterFenceRow,
      owner: UUID,
      databaseNow: LocalDateTime
  ): MySqlWriterFenceRow =
    require(owner != null && databaseNow != null, "writer release inputs are required")
    require(current.ownerBootUuid.contains(owner), "writer row belongs to a different boot")
    require(current.leaseUntil.isAfter(databaseNow), "writer lease has expired")
    current.copy(ownerBootUuid = None, leaseUntil = databaseNow.minusNanos(1000L))

private[mysql] sealed trait MySqlWriterFencePhase
private[mysql] case object MySqlWriterFenceUnclaimed extends MySqlWriterFencePhase
private[mysql] case object MySqlWriterFenceRetiredRecall extends MySqlWriterFencePhase
private[mysql] final case class MySqlWriterFenceAwaitingAudit(token: MySqlWriterFenceToken) extends MySqlWriterFencePhase
private[mysql] final case class MySqlWriterFenceAdmitted(admission: MySqlBusinessAdmission) extends MySqlWriterFencePhase
private[mysql] final case class MySqlWriterFenceRenewing(token: MySqlWriterFenceToken, admission: MySqlBusinessAdmission) extends MySqlWriterFencePhase
private[mysql] final case class MySqlWriterFenceUnknown(
    operation: MySqlWriterFenceOperation,
    expectedRow: Option[MySqlWriterFenceRow],
    previousRow: Option[MySqlWriterFenceRow],
    oldScope: MySqlAuditBootScope,
    failure: Throwable
) extends MySqlWriterFencePhase
private[mysql] final class MySqlWriterFenceBusinessUnknown(
    val context: MySqlOperationContext,
    val plan: Option[MySqlBusinessEffectPlan],
    val failure: MySqlCommitUnknownException,
    val previous: MySqlWriterFencePhase,
    val oldAdmission: Option[MySqlBusinessAdmission],
    val oldScope: MySqlAuditBootScope
) extends MySqlWriterFencePhase

/** 只供一次新连接上的完整业务效果核验使用；coordinator 仍以冻结状态身份校验。 */
private[mysql] final class MySqlBusinessUnknownSnapshot(
    private[mysql] val unknownState: MySqlWriterFenceBusinessUnknown,
    val context: MySqlOperationContext,
    val plan: MySqlBusinessEffectPlan,
    val oldAdmission: MySqlBusinessAdmission,
    val oldScope: MySqlAuditBootScope,
    val verified: VerifiedMySqlConnection,
    val scope: MySqlAuditBootScope,
    val qualification: VerifiedSchemaQualification
)

private[mysql] final case class MySqlWriterFenceMutation(
    row: MySqlWriterFenceRow,
    databaseNow: LocalDateTime,
    monotonicAnchorNanos: Long
)
/**
 * 单一 MySQL writer fence 的 owned-IO 协调器。它只操作既有 READY/CERTIFIED fixture，
 * 不执行 schema bootstrap、marker/world IO，也不代表 world custody 或 server Ready。
 */
final class MySqlWriterFenceCoordinator(
    expectation: MySqlSchemaAuditExpectation,
    auditor: MySqlSchemaAuditor,
    initialScope: MySqlAuditBootScope,
    leaseDuration: Duration
):
  require(expectation != null && auditor != null && initialScope != null, "MySQL writer fence dependencies are required")
  require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero, "writer lease duration must be positive")
  private val leaseNanos = leaseDuration.toNanos
  require(leaseNanos > 0L && leaseNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos, "writer lease duration exceeds the bounded receipt policy")
  require(leaseNanos % 1000L == 0L, "writer lease duration must have DATETIME(6) precision")
  require(MySqlSchemaAuditor.SchemaIdentifier.matches(expectation.schema), "writer schema identifier is unsafe")
  private val bootUuid = initialScope.bootUuid
  private val bootGeneration = initialScope.bootGeneration
  private val ownerThread = initialScope.ownerThread
  private var currentScope = initialScope
  private var dbFirstAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var dbFirstPostAuditCompleted=false
  private var phase: MySqlWriterFencePhase = MySqlWriterFenceUnclaimed
  private var pendingBusinessUnknownRecovery: Option[(MySqlBusinessUnknownSnapshot, MySqlWriterFenceToken)] = None
  private var modeWorkflowResponsibility = Option.empty[MySqlModeWorkflowResponsibility]
  private var normalRecallWorkflowResponsibility = Option.empty[MySqlNormalRecallWorkflowResponsibility]
  private var controlledDebugWorkflowResponsibility = Option.empty[MySqlControlledDebugWorkflowResponsibility]
  private var driftWorkflowResponsibility = Option.empty[MySqlDriftWorkflowResponsibility]
  private var inactiveDomainWorkflowResponsibility = Option.empty[MySqlInactiveDomainWorkflowResponsibility]
  private sealed trait CertifiedOrdinaryResponsibility { def actor: MySqlCertifiedRecoveryInspectionActor }
  private final case class AwaitingHandoffAudit(actor: MySqlCertifiedRecoveryInspectionActor)
      extends CertifiedOrdinaryResponsibility
  private final case class AwaitingNativeHandoff(actor: MySqlCertifiedRecoveryInspectionActor)
      extends CertifiedOrdinaryResponsibility
  private final case class HandoffFaulted(actor: MySqlCertifiedRecoveryInspectionActor, cause: Throwable)
      extends CertifiedOrdinaryResponsibility
  private var certifiedOrdinaryResponsibility = Option.empty[CertifiedOrdinaryResponsibility]

  /** 返回现有 token 供诊断或 post-audit 使用；AwaitingAudit 不具备业务写权限。 */
  def currentToken: Option[MySqlWriterFenceToken] = synchronized {
    if modeWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty || certifiedOrdinaryResponsibility.nonEmpty then None
    else
      phase match
        case MySqlWriterFenceAwaitingAudit(token) => Some(token)
        case MySqlWriterFenceAdmitted(admission) => Some(admission.token)
        case MySqlWriterFenceRenewing(token, _) => Some(token)
        case _ => None
  }

  /** Dedicated ordinary path owns this coordinator before native inspection or any W SQL. */
  private[mysql] def beginCertifiedOrdinaryHandoff(
      actor: MySqlCertifiedRecoveryInspectionActor,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit = synchronized {
    if actor == null then MySqlSchemaAuditor.reject("registered ordinary recovery actor is required")
    if certifiedOrdinaryResponsibility.nonEmpty || phase != MySqlWriterFenceUnclaimed ||
        modeWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty ||
        controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then
      MySqlSchemaAuditor.reject("ordinary HANDOFF requires an unclaimed coordinator with no retained writer workflow")
    validateScope(verified, scope, allowReconnect = false)
    certifiedOrdinaryResponsibility = Some(AwaitingHandoffAudit(actor))
  }

  private[mysql] def certifiedOrdinaryLeaseDeadline(
      actor: MySqlCertifiedRecoveryInspectionActor,
      databaseNow: LocalDateTime
  ): LocalDateTime = synchronized {
    requireOrdinaryPhase(actor, awaitingNative = false)
    leaseDeadline(databaseNow)
  }

  /** Builds the exact same-connection token and issues the real post-commit schema audit receipt. */
  private[mysql] def auditCommittedCertifiedHandoff(
      actor: MySqlCertifiedRecoveryInspectionActor,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      qualification: VerifiedSchemaQualification,
      targetWriter: MySqlBusinessWriterRow
  ): (MySqlWriterFenceToken, RuntimeSchemaAuditReceipt) = synchronized {
    requireOrdinaryPhase(actor, awaitingNative = false)
    validateScope(verified, scope, allowReconnect = false, ordinaryActor = Some(actor))
    requireQualification(qualification, verified, scope)
    if targetWriter == null || targetWriter.bindingUuid != expectation.bindingUuid ||
        !targetWriter.ownerBootUuid.contains(bootUuid) || !targetWriter.leaseUntil.isAfter(targetWriter.databaseNow) then
      MySqlSchemaAuditor.reject("actual committed ordinary W target row is not current and occupied")
    if !verified.connection.getAutoCommit then
      MySqlSchemaAuditor.reject("ordinary post-commit schema audit requires the exact idle auto-commit connection")
    val row = MySqlWriterFenceRow(targetWriter.bindingUuid, targetWriter.writerEpoch, targetWriter.ownerBootUuid,
      targetWriter.leaseUntil, targetWriter.lastOperationUuid, targetWriter.lastFingerprint)
    val mutation = MySqlWriterFenceMutation(row, targetWriter.databaseNow, System.nanoTime())
    val token = tokenFor(mutation, qualification, verified, scope)
    val observation = auditor.readCurrentObjectManifest(verified, scope)
    val receipt = auditor.auditAfterWriterAcquire(observation, qualification, scope, token.fence)
    validateToken(token, verified, scope)
    validateReceipt(token, receipt, verified, scope)
    certifiedOrdinaryResponsibility = Some(AwaitingNativeHandoff(actor))
    (token, receipt)
  }

  private[mysql] def failCertifiedOrdinaryHandoff(
      actor: MySqlCertifiedRecoveryInspectionActor,
      cause: Throwable
  ): Unit = synchronized {
    if cause == null then MySqlSchemaAuditor.reject("ordinary HANDOFF failure cause is required")
    certifiedOrdinaryResponsibility match
      case Some(current) if current.actor eq actor =>
        current match
          case HandoffFaulted(_, retained) if retained ne cause =>
            MySqlOwnedSessionLifecycle.suppress(retained, cause)
          case _: HandoffFaulted => ()
          case _ => certifiedOrdinaryResponsibility = Some(HandoffFaulted(actor, cause))
      case _ => ()
  }

  /** F3 mode operation 正式开始前冻结 Main 原 plan；本状态不证明 SQL 写入。 */
  private[mysql] def beginModeWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  ): MySqlModeWorkflowResponsibility = synchronized {
    ensureNotFrozen()
    if driftWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then MySqlSchemaAuditor.reject("other exact workflow responsibility excludes mode")
    modeWorkflowResponsibility match
      case Some(retained) if retained.phase == MySqlModeWorkflowPhase.PreparedForReserve &&
          retained.sameWorkflow(commandScope, operation, materialization) => retained
      case Some(_) => MySqlSchemaAuditor.reject("an exact format3 mode workflow responsibility is already retained")
      case None =>
        val admission = requireModeCommandAdmission(commandScope, operation, materialization)
        val responsibility = MySqlModeWorkflowResponsibility.issue(commandScope, operation, materialization,
          admission, MySqlModeWorkflowPhase.PreparedForReserve, None)
        modeWorkflowResponsibility = Some(responsibility)
        responsibility
  }

  /** 只允许同一原 scope/plan/materialization 推进；普通 admission 在责任存在时一直关闭。 */
  private[mysql] def requireModeWorkflowAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      expectedPhase: MySqlModeWorkflowPhase
  ): MySqlBusinessAdmission = synchronized {
    val retained = modeWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("no exact retained format3 mode workflow is available")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("format3 mode continuation differs from the retained phase or exact Main workflow")
    val admission = requireModeCommandAdmission(commandScope, operation, materialization)
    if admission ne retained.admission then
      MySqlSchemaAuditor.reject("format3 mode continuation admission differs from the original retained reference")
    admission
  }

  /** Reserve commit ACK 已观察；在新的完整 F3 post-commit 读回前仍不得 execute。 */
  private[mysql] def markModeReserveAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireModeWorkflow(commandScope, operation, materialization, MySqlModeWorkflowPhase.PreparedForReserve)
    modeWorkflowResponsibility = Some(MySqlModeWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback, None))
  }

  private[mysql] def markModeReserveReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      primary: Throwable
  ): MySqlModeWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original reserve readback failure is required")
    val retained = requireModeWorkflow(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback)
    val blocked = MySqlModeWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlModeWorkflowPhase.ReserveCommittedReadbackUnknown, Some(primary))
    modeWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** Reserve 已由完整 post-commit F3 读回确认，保留 execute obligation；不得释放 reservation。 */
  private[mysql] def recordModeReserveAcknowledged(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireModeWorkflow(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback)
    modeWorkflowResponsibility = Some(MySqlModeWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlModeWorkflowPhase.ReserveCommittedExecutePending, None))
  }

  /** reserve 回复未知时保留完整两阶段原件；不能调用 F2 slot reconciliation 清除此状态。 */
  private[mysql] def markModeReserveCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      primary: Throwable
  ): MySqlModeWorkflowBlockedException = synchronized {
    markModeWorkflowUnknown(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.PreparedForReserve, MySqlModeWorkflowPhase.ReserveCommitUnknown, primary)
  }

  /** execute 回复未知时保留已经确认的 reserve 与 exact execute phase。 */
  private[mysql] def markModeExecuteCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      primary: Throwable
  ): MySqlModeWorkflowBlockedException = synchronized {
    markModeWorkflowUnknown(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ReserveCommittedExecutePending, MySqlModeWorkflowPhase.ExecuteCommitUnknown, primary)
  }

  private[mysql] def markModeExecuteAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireModeWorkflow(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ReserveCommittedExecutePending)
    modeWorkflowResponsibility = Some(MySqlModeWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback, None))
  }

  private[mysql] def markModeExecuteReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      primary: Throwable
  ): MySqlModeWorkflowBlockedException = synchronized {
    val retained = requireModeWorkflow(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback)
    val blocked = MySqlModeWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlModeWorkflowPhase.ExecuteCommittedReadbackUnknown, Some(primary))
    modeWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** 只有 runner 私有 issuer 在 execute full20/W 精确 readback 后能产生该 proof。 */
  private[mysql] def completeModeWorkflow(
      proof: MySqlFencedTransaction.ModePostCommitReadback
  ): Unit = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("exact full20 mode post-commit readback proof is required")
    val retained = modeWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("there is no retained format3 mode workflow to complete")
    )
    if retained.phase != MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback ||
      !retained.sameWorkflow(proof.commandScope, proof.operation, proof.materialization) then
      MySqlSchemaAuditor.reject("mode completion proof differs from the exact retained workflow")
    val auditScope = proof.commandScope.exactAuditScope()
    proof.commandScope.requireCurrent(auditScope, proof.commandScope.operationPlan())
    val expectedSnapshotScope = MySqlFormat3ObservationScope(auditScope.bootUuid,
      auditScope.bootGeneration, auditScope.connectionGeneration)
    if proof.snapshotScope != expectedSnapshotScope then
      MySqlSchemaAuditor.reject("format3 mode completion proof belongs to a different owned session generation")
    modeWorkflowResponsibility = None
  }

  /** 已确认 reserve 后的 execute 失败保持原 R/plan；此方法仅记录失败，不变更 DB responsibility。 */
  private[mysql] def recordModeExecuteFailureAfterReserve(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      primary: Throwable
  ): Throwable = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original post-reserve failure is required")
    val retained = requireModeWorkflow(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.ReserveCommittedExecutePending)
    val replacement = MySqlModeWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlModeWorkflowPhase.ExecuteFailedAfterReserve, Some(primary))
    modeWorkflowResponsibility = Some(replacement)
    replacement.blockedFailure.get
  }

  /**
   * Execute 的 admission 与连接前置核验也属于 reserve ACK 后的固定阶段。
   * 仅当同一 retained workflow 仍处于 ExecutePending 时记录为已知失败；
   * commit/readback 未知态不被改写。
   */
  private[mysql] def captureModeExecuteFailureAfterReserve[A](
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  )(executePhase: => A): A =
    MySqlModeExecuteFailureBoundary.afterReserve(primary => {
      synchronized {
        modeWorkflowResponsibility.exists(retained =>
          retained.sameWorkflow(commandScope, operation, materialization) &&
            MySqlModeExecuteFailureBoundary.shouldRecordKnownFailure(
              retained.phase == MySqlModeWorkflowPhase.ReserveCommittedExecutePending, primary))
      }
    })(executePhase) { primary =>
      recordModeExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    }

  private def markModeWorkflowUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      expectedPhase: MySqlModeWorkflowPhase,
      nextPhase: MySqlModeWorkflowPhase,
      primary: Throwable
  ): MySqlModeWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original mode commit uncertainty cause is required")
    modeWorkflowResponsibility match
      case Some(existing) if existing.sameWorkflow(commandScope, operation, materialization) && existing.phase == nextPhase =>
        val duplicate = new MySqlModeWorkflowBlockedException(nextPhase, primary)
        MySqlSchemaAuditor.addSuppressed(existing.blockedFailure.get, duplicate)
        existing.blockedFailure.get
      case _ =>
        val retained = requireModeWorkflow(commandScope, operation, materialization, expectedPhase)
        val blocked = MySqlModeWorkflowResponsibility.issue(commandScope, operation, materialization,
          retained.admission, nextPhase, Some(primary))
        modeWorkflowResponsibility = Some(blocked)
        blocked.blockedFailure.get
  }

  private def requireModeWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      expectedPhase: MySqlModeWorkflowPhase
  ): MySqlModeWorkflowResponsibility =
    val retained = modeWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("exact F3 mode workflow responsibility is missing")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("F3 mode workflow phase or original immutable inputs changed")
    retained

  private def requireModeCommandAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization
  ): MySqlBusinessAdmission =
    if commandScope == null || operation == null || materialization == null then
      MySqlSchemaAuditor.reject("exact F3 mode command inputs are required")
    val commandPlan = commandScope.operationPlan()
    materialization.requireBoundTo(commandScope, operation)
    val scope = commandScope.exactAuditScope()
    commandScope.requireCurrent(scope, commandPlan)
    commandScope.requireHeldSessionBarrier(scope, commandPlan)
    val verified = scope.verifiedConnection.getOrElse(
      MySqlSchemaAuditor.reject("F3 mode scope lacks its exact verified connection")
    )
    validateScope(verified, scope, allowReconnect = false)
    val fromCommand = commandScope.requireBusinessAdmission(scope, commandPlan)
    val fromCoordinator =
      if modeWorkflowResponsibility.isEmpty then requireBusinessAdmission(verified, scope)
      else
        phase match
          case MySqlWriterFenceAdmitted(admission) =>
            validateToken(admission.token, verified, scope)
            validateReceipt(admission.token, admission.receipt, verified, scope)
            admission
          case _ => MySqlSchemaAuditor.reject("F3 mode continuation lost its exact admitted writer fence")
    if (fromCoordinator ne fromCommand) then
      MySqlSchemaAuditor.reject("F3 mode command and coordinator admission references differ")
    fromCoordinator


  private[mysql] def beginNormalRecallWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  ): MySqlNormalRecallWorkflowResponsibility = synchronized {
    ensureNotFrozen()
    if driftWorkflowResponsibility.nonEmpty || modeWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then MySqlSchemaAuditor.reject("other exact workflow responsibility excludes normal recall")
    normalRecallWorkflowResponsibility match
      case Some(retained) if retained.phase == MySqlNormalRecallWorkflowPhase.PreparedForReserve &&
          retained.sameWorkflow(commandScope, operation, materialization) => retained
      case Some(_) => MySqlSchemaAuditor.reject("an exact format3 normal recall workflow responsibility is already retained")
      case None =>
        val admission = requireNormalRecallCommandAdmission(commandScope, operation, materialization)
        val responsibility = MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation, materialization,
          admission, MySqlNormalRecallWorkflowPhase.PreparedForReserve, None)
        normalRecallWorkflowResponsibility = Some(responsibility)
        responsibility
  }

  /** 只允许同一原 scope/plan/materialization 推进；普通 admission 在责任存在时一直关闭。 */
  private[mysql] def requireNormalRecallWorkflowAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      expectedPhase: MySqlNormalRecallWorkflowPhase
  ): MySqlBusinessAdmission = synchronized {
    val retained = normalRecallWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("no exact retained format3 normal recall workflow is available")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("format3 normal recall continuation differs from the retained phase or exact Main workflow")
    val admission = requireNormalRecallCommandAdmission(commandScope, operation, materialization)
    if admission ne retained.admission then
      MySqlSchemaAuditor.reject("format3 normal recall continuation admission differs from the original retained reference")
    admission
  }

  /** Reserve commit ACK 已观察；在新的完整 F3 post-commit 读回前仍不得 execute。 */
  private[mysql] def markNormalRecallReserveAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  ): Unit = synchronized {
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization, MySqlNormalRecallWorkflowPhase.PreparedForReserve)
    normalRecallWorkflowResponsibility = Some(MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlNormalRecallWorkflowPhase.ReserveCommittedAwaitingReadback, None))
  }

  private[mysql] def markNormalRecallReserveReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      primary: Throwable
  ): MySqlNormalRecallWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original reserve readback failure is required")
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ReserveCommittedAwaitingReadback)
    val blocked = MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlNormalRecallWorkflowPhase.ReserveCommittedReadbackUnknown, Some(primary))
    normalRecallWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** Reserve 已由完整 post-commit F3 读回确认，保留 execute obligation；不得释放 reservation。 */
  private[mysql] def recordNormalRecallReserveAcknowledged(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  ): Unit = synchronized {
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ReserveCommittedAwaitingReadback)
    normalRecallWorkflowResponsibility = Some(MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending, None))
  }

  /** reserve 回复未知时保留完整两阶段原件；不能调用 F2 slot reconciliation 清除此状态。 */
  private[mysql] def markNormalRecallReserveCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      primary: Throwable
  ): MySqlNormalRecallWorkflowBlockedException = synchronized {
    markNormalRecallWorkflowUnknown(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.PreparedForReserve, MySqlNormalRecallWorkflowPhase.ReserveCommitUnknown, primary)
  }

  /** execute 回复未知时保留已经确认的 reserve 与 exact execute phase。 */
  private[mysql] def markNormalRecallExecuteCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      primary: Throwable
  ): MySqlNormalRecallWorkflowBlockedException = synchronized {
    markNormalRecallWorkflowUnknown(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending, MySqlNormalRecallWorkflowPhase.ExecuteCommitUnknown, primary)
  }

  private[mysql] def markNormalRecallExecuteAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  ): Unit = synchronized {
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending)
    normalRecallWorkflowResponsibility = Some(MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback, None))
  }

  private[mysql] def markNormalRecallExecuteReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      primary: Throwable
  ): MySqlNormalRecallWorkflowBlockedException = synchronized {
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback)
    val blocked = MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlNormalRecallWorkflowPhase.ExecuteCommittedReadbackUnknown, Some(primary))
    normalRecallWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** 只有 runner 私有 issuer 在 execute full20/W 精确 readback 后能产生该 proof。 */
  private[mysql] def completeNormalRecallWorkflow(
      proof: MySqlFencedTransaction.NormalRecallPostCommitReadback
  ): Unit = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("exact full20 mode post-commit readback proof is required")
    val retained = normalRecallWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("there is no retained format3 normal recall workflow to complete")
    )
    if retained.phase != MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback ||
      !retained.sameWorkflow(proof.commandScope, proof.operation, proof.materialization) then
      MySqlSchemaAuditor.reject("mode completion proof differs from the exact retained workflow")
    val auditScope = proof.commandScope.exactAuditScope()
    proof.commandScope.requireCurrent(auditScope, proof.commandScope.operationPlan())
    val expectedSnapshotScope = MySqlFormat3ObservationScope(auditScope.bootUuid,
      auditScope.bootGeneration, auditScope.connectionGeneration)
    if proof.snapshotScope != expectedSnapshotScope then
      MySqlSchemaAuditor.reject("format3 mode completion proof belongs to a different owned session generation")
    normalRecallWorkflowResponsibility = None
  }

  /** 已确认 reserve 后的 execute 失败保持原 R/plan；此方法仅记录失败，不变更 DB responsibility。 */
  private[mysql] def recordNormalRecallExecuteFailureAfterReserve(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      primary: Throwable
  ): Throwable = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original post-reserve failure is required")
    val retained = requireNormalRecallWorkflow(commandScope, operation, materialization,
      MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending)
    val replacement = MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlNormalRecallWorkflowPhase.ExecuteFailedAfterReserve, Some(primary))
    normalRecallWorkflowResponsibility = Some(replacement)
    replacement.blockedFailure.get
  }

  /**
   * Execute 的 admission 与连接前置核验也属于 reserve ACK 后的固定阶段。
   * 仅当同一 retained workflow 仍处于 ExecutePending 时记录为已知失败；
   * commit/readback 未知态不被改写。
   */
  private[mysql] def captureNormalRecallExecuteFailureAfterReserve[A](
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  )(executePhase: => A): A =
    MySqlNormalRecallExecuteFailureBoundary.afterReserve(primary => {
      synchronized {
        normalRecallWorkflowResponsibility.exists(retained =>
          retained.sameWorkflow(commandScope, operation, materialization) &&
            MySqlNormalRecallExecuteFailureBoundary.shouldRecordKnownFailure(
              retained.phase == MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending, primary))
      }
    })(executePhase) { primary =>
      recordNormalRecallExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    }

  private def markNormalRecallWorkflowUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      expectedPhase: MySqlNormalRecallWorkflowPhase,
      nextPhase: MySqlNormalRecallWorkflowPhase,
      primary: Throwable
  ): MySqlNormalRecallWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original mode commit uncertainty cause is required")
    normalRecallWorkflowResponsibility match
      case Some(existing) if existing.sameWorkflow(commandScope, operation, materialization) && existing.phase == nextPhase =>
        val duplicate = new MySqlNormalRecallWorkflowBlockedException(nextPhase, primary)
        MySqlSchemaAuditor.addSuppressed(existing.blockedFailure.get, duplicate)
        existing.blockedFailure.get
      case _ =>
        val retained = requireNormalRecallWorkflow(commandScope, operation, materialization, expectedPhase)
        val blocked = MySqlNormalRecallWorkflowResponsibility.issue(commandScope, operation, materialization,
          retained.admission, nextPhase, Some(primary))
        normalRecallWorkflowResponsibility = Some(blocked)
        blocked.blockedFailure.get
  }

  private def requireNormalRecallWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      expectedPhase: MySqlNormalRecallWorkflowPhase
  ): MySqlNormalRecallWorkflowResponsibility =
    val retained = normalRecallWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("exact F3 normal recall workflow responsibility is missing")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("F3 normal recall workflow phase or original immutable inputs changed")
    retained

  private def requireNormalRecallCommandAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization
  ): MySqlBusinessAdmission =
    if commandScope == null || operation == null || materialization == null then
      MySqlSchemaAuditor.reject("exact F3 normal recall command inputs are required")
    val commandPlan = commandScope.operationPlan()
    materialization.requireBoundTo(commandScope, operation)
    val scope = commandScope.exactAuditScope()
    commandScope.requireCurrent(scope, commandPlan)
    commandScope.requireHeldSessionBarrier(scope, commandPlan)
    val verified = scope.verifiedConnection.getOrElse(
      MySqlSchemaAuditor.reject("F3 mode scope lacks its exact verified connection")
    )
    validateScope(verified, scope, allowReconnect = false)
    val fromCommand = commandScope.requireBusinessAdmission(scope, commandPlan)
    val fromCoordinator =
      if normalRecallWorkflowResponsibility.isEmpty then requireBusinessAdmission(verified, scope)
      else
        phase match
          case MySqlWriterFenceAdmitted(admission) =>
            validateToken(admission.token, verified, scope)
            validateReceipt(admission.token, admission.receipt, verified, scope)
            admission
          case _ => MySqlSchemaAuditor.reject("F3 normal recall continuation lost its exact admitted writer fence")
    if (fromCoordinator ne fromCommand) then
      MySqlSchemaAuditor.reject("F3 normal recall command and coordinator admission references differ")
    fromCoordinator



  private[mysql] def beginControlledDebugWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  ): MySqlControlledDebugWorkflowResponsibility = synchronized {
    ensureNotFrozen()
    if driftWorkflowResponsibility.nonEmpty || modeWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then MySqlSchemaAuditor.reject("other exact workflow responsibility excludes controlled debug")
    controlledDebugWorkflowResponsibility match
      case Some(retained) if retained.phase == MySqlControlledDebugWorkflowPhase.PreparedForReserve &&
          retained.sameWorkflow(commandScope, operation, materialization) => retained
      case Some(_) => MySqlSchemaAuditor.reject("an exact format3 controlled debug workflow responsibility is already retained")
      case None =>
        val admission = requireControlledDebugCommandAdmission(commandScope, operation, materialization)
        val responsibility = MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation, materialization,
          admission, MySqlControlledDebugWorkflowPhase.PreparedForReserve, None)
        controlledDebugWorkflowResponsibility = Some(responsibility)
        responsibility
  }

  /** 只允许同一原 scope/plan/materialization 推进；普通 admission 在责任存在时一直关闭。 */
  private[mysql] def requireControlledDebugWorkflowAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      expectedPhase: MySqlControlledDebugWorkflowPhase
  ): MySqlBusinessAdmission = synchronized {
    val retained = controlledDebugWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("no exact retained format3 controlled debug workflow is available")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("format3 controlled debug continuation differs from the retained phase or exact Main workflow")
    val admission = requireControlledDebugCommandAdmission(commandScope, operation, materialization)
    if admission ne retained.admission then
      MySqlSchemaAuditor.reject("format3 controlled debug continuation admission differs from the original retained reference")
    admission
  }

  /** Reserve commit ACK 已观察；在新的完整 F3 post-commit 读回前仍不得 execute。 */
  private[mysql] def markControlledDebugReserveAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization, MySqlControlledDebugWorkflowPhase.PreparedForReserve)
    controlledDebugWorkflowResponsibility = Some(MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlControlledDebugWorkflowPhase.ReserveCommittedAwaitingReadback, None))
  }

  private[mysql] def markControlledDebugReserveReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      primary: Throwable
  ): MySqlControlledDebugWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original reserve readback failure is required")
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ReserveCommittedAwaitingReadback)
    val blocked = MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlControlledDebugWorkflowPhase.ReserveCommittedReadbackUnknown, Some(primary))
    controlledDebugWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** Reserve 已由完整 post-commit F3 读回确认，保留 execute obligation；不得释放 reservation。 */
  private[mysql] def recordControlledDebugReserveAcknowledged(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ReserveCommittedAwaitingReadback)
    controlledDebugWorkflowResponsibility = Some(MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending, None))
  }

  /** reserve 回复未知时保留完整两阶段原件；不能调用 F2 slot reconciliation 清除此状态。 */
  private[mysql] def markControlledDebugReserveCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      primary: Throwable
  ): MySqlControlledDebugWorkflowBlockedException = synchronized {
    markControlledDebugWorkflowUnknown(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.PreparedForReserve, MySqlControlledDebugWorkflowPhase.ReserveCommitUnknown, primary)
  }

  /** execute 回复未知时保留已经确认的 reserve 与 exact execute phase。 */
  private[mysql] def markControlledDebugExecuteCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      primary: Throwable
  ): MySqlControlledDebugWorkflowBlockedException = synchronized {
    markControlledDebugWorkflowUnknown(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending, MySqlControlledDebugWorkflowPhase.ExecuteCommitUnknown, primary)
  }

  private[mysql] def markControlledDebugExecuteAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending)
    controlledDebugWorkflowResponsibility = Some(MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlControlledDebugWorkflowPhase.ExecuteCommittedAwaitingReadback, None))
  }

  private[mysql] def markControlledDebugExecuteReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      primary: Throwable
  ): MySqlControlledDebugWorkflowBlockedException = synchronized {
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ExecuteCommittedAwaitingReadback)
    val blocked = MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlControlledDebugWorkflowPhase.ExecuteCommittedReadbackUnknown, Some(primary))
    controlledDebugWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** 只有 runner 私有 issuer 在 execute full20/W 精确 readback 后能产生该 proof。 */
  private[mysql] def completeControlledDebugWorkflow(
      proof: MySqlFencedTransaction.ControlledDebugPostCommitReadback
  ): Unit = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("exact full20 mode post-commit readback proof is required")
    val retained = controlledDebugWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("there is no retained format3 controlled debug workflow to complete")
    )
    if retained.phase != MySqlControlledDebugWorkflowPhase.ExecuteCommittedAwaitingReadback ||
      !retained.sameWorkflow(proof.commandScope, proof.operation, proof.materialization) then
      MySqlSchemaAuditor.reject("mode completion proof differs from the exact retained workflow")
    val auditScope = proof.commandScope.exactAuditScope()
    proof.commandScope.requireCurrent(auditScope, proof.commandScope.operationPlan())
    val expectedSnapshotScope = MySqlFormat3ObservationScope(auditScope.bootUuid,
      auditScope.bootGeneration, auditScope.connectionGeneration)
    if proof.snapshotScope != expectedSnapshotScope then
      MySqlSchemaAuditor.reject("format3 mode completion proof belongs to a different owned session generation")
    controlledDebugWorkflowResponsibility = None
  }

  /** 已确认 reserve 后的 execute 失败保持原 R/plan；此方法仅记录失败，不变更 DB responsibility。 */
  private[mysql] def recordControlledDebugExecuteFailureAfterReserve(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      primary: Throwable
  ): Throwable = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original post-reserve failure is required")
    val retained = requireControlledDebugWorkflow(commandScope, operation, materialization,
      MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending)
    val replacement = MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlControlledDebugWorkflowPhase.ExecuteFailedAfterReserve, Some(primary))
    controlledDebugWorkflowResponsibility = Some(replacement)
    replacement.blockedFailure.get
  }

  /**
   * Execute 的 admission 与连接前置核验也属于 reserve ACK 后的固定阶段。
   * 仅当同一 retained workflow 仍处于 ExecutePending 时记录为已知失败；
   * commit/readback 未知态不被改写。
   */
  private[mysql] def captureControlledDebugExecuteFailureAfterReserve[A](
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  )(executePhase: => A): A =
    MySqlControlledDebugExecuteFailureBoundary.afterReserve(primary => {
      synchronized {
        controlledDebugWorkflowResponsibility.exists(retained =>
          retained.sameWorkflow(commandScope, operation, materialization) &&
            MySqlControlledDebugExecuteFailureBoundary.shouldRecordKnownFailure(
              retained.phase == MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending, primary))
      }
    })(executePhase) { primary =>
      recordControlledDebugExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    }

  private def markControlledDebugWorkflowUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      expectedPhase: MySqlControlledDebugWorkflowPhase,
      nextPhase: MySqlControlledDebugWorkflowPhase,
      primary: Throwable
  ): MySqlControlledDebugWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original mode commit uncertainty cause is required")
    controlledDebugWorkflowResponsibility match
      case Some(existing) if existing.sameWorkflow(commandScope, operation, materialization) && existing.phase == nextPhase =>
        val duplicate = new MySqlControlledDebugWorkflowBlockedException(nextPhase, primary)
        MySqlSchemaAuditor.addSuppressed(existing.blockedFailure.get, duplicate)
        existing.blockedFailure.get
      case _ =>
        val retained = requireControlledDebugWorkflow(commandScope, operation, materialization, expectedPhase)
        val blocked = MySqlControlledDebugWorkflowResponsibility.issue(commandScope, operation, materialization,
          retained.admission, nextPhase, Some(primary))
        controlledDebugWorkflowResponsibility = Some(blocked)
        blocked.blockedFailure.get
  }

  private def requireControlledDebugWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      expectedPhase: MySqlControlledDebugWorkflowPhase
  ): MySqlControlledDebugWorkflowResponsibility =
    val retained = controlledDebugWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("exact F3 controlled debug workflow responsibility is missing")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("F3 controlled debug workflow phase or original immutable inputs changed")
    retained

  private def requireControlledDebugCommandAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization
  ): MySqlBusinessAdmission =
    if commandScope == null || operation == null || materialization == null then
      MySqlSchemaAuditor.reject("exact F3 controlled debug command inputs are required")
    val commandPlan = commandScope.operationPlan()
    materialization.requireBoundTo(commandScope, operation)
    val scope = commandScope.exactAuditScope()
    commandScope.requireCurrent(scope, commandPlan)
    commandScope.requireHeldSessionBarrier(scope, commandPlan)
    val verified = scope.verifiedConnection.getOrElse(
      MySqlSchemaAuditor.reject("F3 mode scope lacks its exact verified connection")
    )
    validateScope(verified, scope, allowReconnect = false)
    val fromCommand = commandScope.requireBusinessAdmission(scope, commandPlan)
    val fromCoordinator =
      if controlledDebugWorkflowResponsibility.isEmpty then requireBusinessAdmission(verified, scope)
      else
        phase match
          case MySqlWriterFenceAdmitted(admission) =>
            validateToken(admission.token, verified, scope)
            validateReceipt(admission.token, admission.receipt, verified, scope)
            admission
          case _ => MySqlSchemaAuditor.reject("F3 controlled debug continuation lost its exact admitted writer fence")
    if (fromCoordinator ne fromCommand) then
      MySqlSchemaAuditor.reject("F3 controlled debug command and coordinator admission references differ")
    fromCoordinator


  /** F3 drift operation 正式开始前冻结 Main 原 plan；本状态不证明 SQL 写入。 */
  private[mysql] def beginDriftWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  ): MySqlDriftWorkflowResponsibility = synchronized {
    ensureNotFrozen()
    if modeWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then MySqlSchemaAuditor.reject("other exact workflow responsibility excludes drift")
    driftWorkflowResponsibility match
      case Some(retained) if retained.phase == MySqlDriftWorkflowPhase.PreparedForReserve &&
          retained.sameWorkflow(commandScope, operation, materialization) => retained
      case Some(_) => MySqlSchemaAuditor.reject("an exact format3 drift workflow responsibility is already retained")
      case None =>
        val admission = requireDriftCommandAdmission(commandScope, operation, materialization)
        val responsibility = MySqlDriftWorkflowResponsibility.issue(commandScope, operation, materialization,
          admission, MySqlDriftWorkflowPhase.PreparedForReserve, None)
        driftWorkflowResponsibility = Some(responsibility)
        responsibility
  }

  /** 只允许同一原 scope/plan/materialization 推进；普通 admission 在责任存在时一直关闭。 */
  private[mysql] def requireDriftWorkflowAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      expectedPhase: MySqlDriftWorkflowPhase
  ): MySqlBusinessAdmission = synchronized {
    val retained = driftWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("no exact retained format3 drift workflow is available")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("format3 drift continuation differs from the retained phase or exact Main workflow")
    val admission = requireDriftCommandAdmission(commandScope, operation, materialization)
    if admission ne retained.admission then
      MySqlSchemaAuditor.reject("format3 drift continuation admission differs from the original retained reference")
    admission
  }

  /** Reserve commit ACK 已观察；在新的完整 F3 post-commit 读回前仍不得 execute。 */
  private[mysql] def markDriftReserveAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireDriftWorkflow(commandScope, operation, materialization, MySqlDriftWorkflowPhase.PreparedForReserve)
    driftWorkflowResponsibility = Some(MySqlDriftWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback, None))
  }

  private[mysql] def markDriftReserveReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      primary: Throwable
  ): MySqlDriftWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original reserve readback failure is required")
    val retained = requireDriftWorkflow(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback)
    val blocked = MySqlDriftWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlDriftWorkflowPhase.ReserveCommittedReadbackUnknown, Some(primary))
    driftWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** Reserve 已由完整 post-commit F3 读回确认，保留 execute obligation；不得释放 reservation。 */
  private[mysql] def recordDriftReserveAcknowledged(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireDriftWorkflow(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback)
    driftWorkflowResponsibility = Some(MySqlDriftWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlDriftWorkflowPhase.ReserveCommittedExecutePending, None))
  }

  /** reserve 回复未知时保留完整两阶段原件；不能调用 F2 slot reconciliation 清除此状态。 */
  private[mysql] def markDriftReserveCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      primary: Throwable
  ): MySqlDriftWorkflowBlockedException = synchronized {
    markDriftWorkflowUnknown(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.PreparedForReserve, MySqlDriftWorkflowPhase.ReserveCommitUnknown, primary)
  }

  /** execute 回复未知时保留已经确认的 reserve 与 exact execute phase。 */
  private[mysql] def markDriftExecuteCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      primary: Throwable
  ): MySqlDriftWorkflowBlockedException = synchronized {
    markDriftWorkflowUnknown(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ReserveCommittedExecutePending, MySqlDriftWorkflowPhase.ExecuteCommitUnknown, primary)
  }

  private[mysql] def markDriftExecuteAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireDriftWorkflow(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ReserveCommittedExecutePending)
    driftWorkflowResponsibility = Some(MySqlDriftWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback, None))
  }

  private[mysql] def markDriftExecuteReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      primary: Throwable
  ): MySqlDriftWorkflowBlockedException = synchronized {
    val retained = requireDriftWorkflow(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback)
    val blocked = MySqlDriftWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlDriftWorkflowPhase.ExecuteCommittedReadbackUnknown, Some(primary))
    driftWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** 只有 runner 私有 issuer 在 execute full20/W 精确 readback 后能产生该 proof。 */
  private[mysql] def completeDriftWorkflow(
      proof: MySqlFencedTransaction.DriftPostCommitReadback
  ): Unit = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("exact full20 drift post-commit readback proof is required")
    val retained = driftWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("there is no retained format3 drift workflow to complete")
    )
    if retained.phase != MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback ||
      !retained.sameWorkflow(proof.commandScope, proof.operation, proof.materialization) then
      MySqlSchemaAuditor.reject("drift completion proof differs from the exact retained workflow")
    val auditScope = proof.commandScope.exactAuditScope()
    proof.commandScope.requireCurrent(auditScope, proof.commandScope.operationPlan())
    val expectedSnapshotScope = MySqlFormat3ObservationScope(auditScope.bootUuid,
      auditScope.bootGeneration, auditScope.connectionGeneration)
    if proof.snapshotScope != expectedSnapshotScope then
      MySqlSchemaAuditor.reject("format3 drift completion proof belongs to a different owned session generation")
    driftWorkflowResponsibility = None
  }

  /** 已确认 reserve 后的 execute 失败保持原 R/plan；此方法仅记录失败，不变更 DB responsibility。 */
  private[mysql] def recordDriftExecuteFailureAfterReserve(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      primary: Throwable
  ): Throwable = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original post-reserve failure is required")
    val retained = requireDriftWorkflow(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.ReserveCommittedExecutePending)
    val replacement = MySqlDriftWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlDriftWorkflowPhase.ExecuteFailedAfterReserve, Some(primary))
    driftWorkflowResponsibility = Some(replacement)
    replacement.blockedFailure.get
  }

  /**
   * Execute 的 admission 与连接前置核验也属于 reserve ACK 后的固定阶段。
   * 仅当同一 retained workflow 仍处于 ExecutePending 时记录为已知失败；
   * commit/readback 未知态不被改写。
   */
  private[mysql] def captureDriftExecuteFailureAfterReserve[A](
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  )(executePhase: => A): A =
    MySqlDriftExecuteFailureBoundary.afterReserve(primary => {
      synchronized {
        driftWorkflowResponsibility.exists(retained =>
          retained.sameWorkflow(commandScope, operation, materialization) &&
            MySqlDriftExecuteFailureBoundary.shouldRecordKnownFailure(
              retained.phase == MySqlDriftWorkflowPhase.ReserveCommittedExecutePending, primary))
      }
    })(executePhase) { primary =>
      recordDriftExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    }

  private def markDriftWorkflowUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      expectedPhase: MySqlDriftWorkflowPhase,
      nextPhase: MySqlDriftWorkflowPhase,
      primary: Throwable
  ): MySqlDriftWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original drift commit uncertainty cause is required")
    driftWorkflowResponsibility match
      case Some(existing) if existing.sameWorkflow(commandScope, operation, materialization) && existing.phase == nextPhase =>
        val duplicate = new MySqlDriftWorkflowBlockedException(nextPhase, primary)
        MySqlSchemaAuditor.addSuppressed(existing.blockedFailure.get, duplicate)
        existing.blockedFailure.get
      case _ =>
        val retained = requireDriftWorkflow(commandScope, operation, materialization, expectedPhase)
        val blocked = MySqlDriftWorkflowResponsibility.issue(commandScope, operation, materialization,
          retained.admission, nextPhase, Some(primary))
        driftWorkflowResponsibility = Some(blocked)
        blocked.blockedFailure.get
  }

  private def requireDriftWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      expectedPhase: MySqlDriftWorkflowPhase
  ): MySqlDriftWorkflowResponsibility =
    val retained = driftWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("exact F3 drift workflow responsibility is missing")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("F3 drift workflow phase or original immutable inputs changed")
    retained

  private def requireDriftCommandAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization
  ): MySqlBusinessAdmission =
    if commandScope == null || operation == null || materialization == null then
      MySqlSchemaAuditor.reject("exact F3 drift command inputs are required")
    val commandPlan = commandScope.operationPlan()
    materialization.requireBoundTo(commandScope, operation)
    val scope = commandScope.exactAuditScope()
    commandScope.requireCurrent(scope, commandPlan)
    commandScope.requireHeldSessionBarrier(scope, commandPlan)
    val verified = scope.verifiedConnection.getOrElse(
      MySqlSchemaAuditor.reject("F3 drift scope lacks its exact verified connection")
    )
    validateScope(verified, scope, allowReconnect = false)
    val fromCommand = commandScope.requireBusinessAdmission(scope, commandPlan)
    val fromCoordinator =
      if driftWorkflowResponsibility.isEmpty then requireBusinessAdmission(verified, scope)
      else
        phase match
          case MySqlWriterFenceAdmitted(admission) =>
            validateToken(admission.token, verified, scope)
            validateReceipt(admission.token, admission.receipt, verified, scope)
            admission
          case _ => MySqlSchemaAuditor.reject("F3 drift continuation lost its exact admitted writer fence")
    if (fromCoordinator ne fromCommand) then
      MySqlSchemaAuditor.reject("F3 drift command and coordinator admission references differ")
    fromCoordinator

  /** F3 inactiveDomain operation 正式开始前冻结 Main 原 plan；本状态不证明 SQL 写入。 */
  private[mysql] def beginInactiveDomainWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  ): MySqlInactiveDomainWorkflowResponsibility = synchronized {
    ensureNotFrozen()
    if modeWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty then MySqlSchemaAuditor.reject("other exact workflow responsibility excludes inactive domain")
    inactiveDomainWorkflowResponsibility match
      case Some(retained) if retained.phase == MySqlInactiveDomainWorkflowPhase.PreparedForReserve &&
          retained.sameWorkflow(commandScope, operation, materialization) => retained
      case Some(_) => MySqlSchemaAuditor.reject("an exact format3 inactiveDomain workflow responsibility is already retained")
      case None =>
        val admission = requireInactiveDomainCommandAdmission(commandScope, operation, materialization)
        val responsibility = MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation, materialization,
          admission, MySqlInactiveDomainWorkflowPhase.PreparedForReserve, None)
        inactiveDomainWorkflowResponsibility = Some(responsibility)
        responsibility
  }

  /** 只允许同一原 scope/plan/materialization 推进；普通 admission 在责任存在时一直关闭。 */
  private[mysql] def requireInactiveDomainWorkflowAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      expectedPhase: MySqlInactiveDomainWorkflowPhase
  ): MySqlBusinessAdmission = synchronized {
    val retained = inactiveDomainWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("no exact retained format3 inactiveDomain workflow is available")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("format3 inactiveDomain continuation differs from the retained phase or exact Main workflow")
    val admission = requireInactiveDomainCommandAdmission(commandScope, operation, materialization)
    if admission ne retained.admission then
      MySqlSchemaAuditor.reject("format3 inactiveDomain continuation admission differs from the original retained reference")
    admission
  }

  /** Reserve commit ACK 已观察；在新的完整 F3 post-commit 读回前仍不得 execute。 */
  private[mysql] def markInactiveDomainReserveAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization, MySqlInactiveDomainWorkflowPhase.PreparedForReserve)
    inactiveDomainWorkflowResponsibility = Some(MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlInactiveDomainWorkflowPhase.ReserveCommittedAwaitingReadback, None))
  }

  private[mysql] def markInactiveDomainReserveReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      primary: Throwable
  ): MySqlInactiveDomainWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original reserve readback failure is required")
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ReserveCommittedAwaitingReadback)
    val blocked = MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlInactiveDomainWorkflowPhase.ReserveCommittedReadbackUnknown, Some(primary))
    inactiveDomainWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** Reserve 已由完整 post-commit F3 读回确认，保留 execute obligation；不得释放 reservation。 */
  private[mysql] def recordInactiveDomainReserveAcknowledged(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ReserveCommittedAwaitingReadback)
    inactiveDomainWorkflowResponsibility = Some(MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending, None))
  }

  /** reserve 回复未知时保留完整两阶段原件；不能调用 F2 slot reconciliation 清除此状态。 */
  private[mysql] def markInactiveDomainReserveCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      primary: Throwable
  ): MySqlInactiveDomainWorkflowBlockedException = synchronized {
    markInactiveDomainWorkflowUnknown(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.PreparedForReserve, MySqlInactiveDomainWorkflowPhase.ReserveCommitUnknown, primary)
  }

  /** execute 回复未知时保留已经确认的 reserve 与 exact execute phase。 */
  private[mysql] def markInactiveDomainExecuteCommitUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      primary: Throwable
  ): MySqlInactiveDomainWorkflowBlockedException = synchronized {
    markInactiveDomainWorkflowUnknown(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending, MySqlInactiveDomainWorkflowPhase.ExecuteCommitUnknown, primary)
  }

  private[mysql] def markInactiveDomainExecuteAcknowledgedAwaitingReadback(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  ): Unit = synchronized {
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending)
    inactiveDomainWorkflowResponsibility = Some(MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation,
      materialization, retained.admission, MySqlInactiveDomainWorkflowPhase.ExecuteCommittedAwaitingReadback, None))
  }

  private[mysql] def markInactiveDomainExecuteReadbackUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      primary: Throwable
  ): MySqlInactiveDomainWorkflowBlockedException = synchronized {
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ExecuteCommittedAwaitingReadback)
    val blocked = MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlInactiveDomainWorkflowPhase.ExecuteCommittedReadbackUnknown, Some(primary))
    inactiveDomainWorkflowResponsibility = Some(blocked)
    blocked.blockedFailure.get
  }

  /** 只有 runner 私有 issuer 在 execute full20/W 精确 readback 后能产生该 proof。 */
  private[mysql] def completeInactiveDomainWorkflow(
      proof: MySqlFencedTransaction.InactiveDomainPostCommitReadback
  ): Unit = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("exact full20 inactiveDomain post-commit readback proof is required")
    val retained = inactiveDomainWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("there is no retained format3 inactiveDomain workflow to complete")
    )
    if retained.phase != MySqlInactiveDomainWorkflowPhase.ExecuteCommittedAwaitingReadback ||
      !retained.sameWorkflow(proof.commandScope, proof.operation, proof.materialization) then
      MySqlSchemaAuditor.reject("inactiveDomain completion proof differs from the exact retained workflow")
    val auditScope = proof.commandScope.exactAuditScope()
    proof.commandScope.requireCurrent(auditScope, proof.commandScope.operationPlan())
    val expectedSnapshotScope = MySqlFormat3ObservationScope(auditScope.bootUuid,
      auditScope.bootGeneration, auditScope.connectionGeneration)
    if proof.snapshotScope != expectedSnapshotScope then
      MySqlSchemaAuditor.reject("format3 inactiveDomain completion proof belongs to a different owned session generation")
    inactiveDomainWorkflowResponsibility = None
  }

  /** 已确认 reserve 后的 execute 失败保持原 R/plan；此方法仅记录失败，不变更 DB responsibility。 */
  private[mysql] def recordInactiveDomainExecuteFailureAfterReserve(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      primary: Throwable
  ): Throwable = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original post-reserve failure is required")
    val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization,
      MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending)
    val replacement = MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation, materialization,
      retained.admission, MySqlInactiveDomainWorkflowPhase.ExecuteFailedAfterReserve, Some(primary))
    inactiveDomainWorkflowResponsibility = Some(replacement)
    replacement.blockedFailure.get
  }

  /**
   * Execute 的 admission 与连接前置核验也属于 reserve ACK 后的固定阶段。
   * 仅当同一 retained workflow 仍处于 ExecutePending 时记录为已知失败；
   * commit/readback 未知态不被改写。
   */
  private[mysql] def captureInactiveDomainExecuteFailureAfterReserve[A](
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  )(executePhase: => A): A =
    MySqlInactiveDomainExecuteFailureBoundary.afterReserve(primary => {
      synchronized {
        inactiveDomainWorkflowResponsibility.exists(retained =>
          retained.sameWorkflow(commandScope, operation, materialization) &&
            MySqlInactiveDomainExecuteFailureBoundary.shouldRecordKnownFailure(
              retained.phase == MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending, primary))
      }
    })(executePhase) { primary =>
      recordInactiveDomainExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    }

  private def markInactiveDomainWorkflowUnknown(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      expectedPhase: MySqlInactiveDomainWorkflowPhase,
      nextPhase: MySqlInactiveDomainWorkflowPhase,
      primary: Throwable
  ): MySqlInactiveDomainWorkflowBlockedException = synchronized {
    if primary == null then MySqlSchemaAuditor.reject("original inactiveDomain commit uncertainty cause is required")
    inactiveDomainWorkflowResponsibility match
      case Some(existing) if existing.sameWorkflow(commandScope, operation, materialization) && existing.phase == nextPhase =>
        val duplicate = new MySqlInactiveDomainWorkflowBlockedException(nextPhase, primary)
        MySqlSchemaAuditor.addSuppressed(existing.blockedFailure.get, duplicate)
        existing.blockedFailure.get
      case _ =>
        val retained = requireInactiveDomainWorkflow(commandScope, operation, materialization, expectedPhase)
        val blocked = MySqlInactiveDomainWorkflowResponsibility.issue(commandScope, operation, materialization,
          retained.admission, nextPhase, Some(primary))
        inactiveDomainWorkflowResponsibility = Some(blocked)
        blocked.blockedFailure.get
  }

  private def requireInactiveDomainWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      expectedPhase: MySqlInactiveDomainWorkflowPhase
  ): MySqlInactiveDomainWorkflowResponsibility =
    val retained = inactiveDomainWorkflowResponsibility.getOrElse(
      MySqlSchemaAuditor.reject("exact F3 inactiveDomain workflow responsibility is missing")
    )
    if retained.phase != expectedPhase || !retained.sameWorkflow(commandScope, operation, materialization) then
      MySqlSchemaAuditor.reject("F3 inactiveDomain workflow phase or original immutable inputs changed")
    retained

  private def requireInactiveDomainCommandAdmission(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization
  ): MySqlBusinessAdmission =
    if commandScope == null || operation == null || materialization == null then
      MySqlSchemaAuditor.reject("exact F3 inactiveDomain command inputs are required")
    val commandPlan = commandScope.operationPlan()
    materialization.requireBoundTo(commandScope, operation)
    val scope = commandScope.exactAuditScope()
    commandScope.requireCurrent(scope, commandPlan)
    commandScope.requireHeldSessionBarrier(scope, commandPlan)
    val verified = scope.verifiedConnection.getOrElse(
      MySqlSchemaAuditor.reject("F3 inactiveDomain scope lacks its exact verified connection")
    )
    validateScope(verified, scope, allowReconnect = false)
    val fromCommand = commandScope.requireBusinessAdmission(scope, commandPlan)
    val fromCoordinator =
      if inactiveDomainWorkflowResponsibility.isEmpty then requireBusinessAdmission(verified, scope)
      else
        phase match
          case MySqlWriterFenceAdmitted(admission) =>
            validateToken(admission.token, verified, scope)
            validateReceipt(admission.token, admission.receipt, verified, scope)
            admission
          case _ => MySqlSchemaAuditor.reject("F3 inactiveDomain continuation lost its exact admitted writer fence")
    if (fromCoordinator ne fromCommand) then
      MySqlSchemaAuditor.reject("F3 inactiveDomain command and coordinator admission references differ")
    fromCoordinator


  /** 调用端每次业务事务开始前必须经过此 gate，并在事务内再次锁定 writer 行。 */
  def requireBusinessAdmission(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlBusinessAdmission = synchronized {
    ensureNotFrozen()
    validateScope(verified, scope, allowReconnect = false)
    MySqlWriterFenceRules.businessAdmission(phase) match
      case Some(admission) =>
        validateToken(admission.token, verified, scope)
        validateReceipt(admission.token, admission.receipt, verified, scope)
        admission
      case None =>
        phase match
          case MySqlWriterFenceAwaitingAudit(_) =>
            MySqlSchemaAuditor.reject("MySQL writer fence is waiting for its exact post-acquire schema audit")
          case MySqlWriterFenceUnclaimed =>
            MySqlSchemaAuditor.reject("MySQL writer fence has not been acquired")
          case _ =>
            MySqlSchemaAuditor.reject("MySQL writer fence coordinator is frozen or renewal is pending")
  }

  /** 原实际fault owner先永久撤原coordinator；不把新连接或旧lease包装成原admission。 */
  private[mysql] def sealTerminatedDbFirstRecall(proof:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      token:MySqlWriterFenceToken,admission:MySqlBusinessAdmission):Duration = synchronized {
    require(proof!=null,"actual registered retired writer recovery owner required")
    proof.requireOriginalCoordinator(this,token,admission)
    ensureNotFrozen()
    phase match
      case MySqlWriterFenceAdmitted(actual) if (actual eq admission) && (actual.token eq token) &&
          (currentScope eq token.scope)=>()
      case _=>MySqlSchemaAuditor.reject("original terminated writer cannot seal a foreign/unknown admission")
    phase=MySqlWriterFenceRetiredRecall
    leaseDuration
  }
  private[mysql] def dbFirstWriterRetiredRecall:Boolean = synchronized {
    expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && phase==MySqlWriterFenceRetiredRecall
  }

  /** 只退休已终止的同原owner；占用行不能走generic acquire，旧有效lease要求也不改义。 */
  private[mysql] def retireTerminatedDbFirstRecall(proof:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):MySqlWriterFenceMutation = synchronized {
    require(proof!=null && audit!=null && expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit==null,
      "actual registered retired writer and independent retirement audit required")
    proof.requireRetirementCoordinator(this,verified,scope)
    ensureUnclaimed()
    validateScope(verified,scope,allowReconnect=false)
    dbFirstAudit=audit
    val (observation,qualification)=freshQualification(verified,scope)
    requireQualification(qualification,verified,scope)
    val mutation=runMutation(MySqlWriterFenceOperation.Release,verified,scope,observation,qualification,phase) {
      (current,databaseNow)=>
        proof.requireCurrentWriterRow(current)
        current.copy(ownerBootUuid=None,leaseUntil=databaseNow.minusNanos(1000L))
    } { (connection,current,target,databaseNow)=>updateTerminatedRecallRelease(connection,current,target) }
    phase=MySqlWriterFenceUnclaimed
    mutation // 只含实际W commit/精确读回；dbFirstAudit仍占用，不能直接acquire。
  }
  private[mysql] def detachTerminatedDbFirstRecallAudit(proof:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      audit:MySqlSchemaAuditor.DbFirstAudit):Unit = synchronized {
    require(proof!=null && audit!=null && (dbFirstAudit eq audit) && phase==MySqlWriterFenceUnclaimed,
      "confirmed retired writer audit required before local cleanup")
    proof.requireAuditDetach(this,audit)
    dbFirstAudit=null
  }

  private[mysql] def acquireOwnedDbFirst(verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):MySqlWriterFenceToken = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && audit != null && dbFirstAudit == null,
      "one original schema6 owned fence audit required")
    dbFirstAudit=audit
    acquire(verified,scope)
  }
  private[mysql] def auditOwnedDbFirstPostAcquire(token:MySqlWriterFenceToken,
      audit:MySqlSchemaAuditor.DbFirstAudit):MySqlBusinessAdmission = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit != null && audit != null,
      "original schema6 pre/post fence audits required")
    requireAwaiting(token)
    val receipt=audit.postAcquire(this,token)
    dbFirstPostAuditCompleted=true
    confirmPostAcquireAudit(token,receipt)
  }
  private var dbFirstRecallJournalResponsibility:MySqlDbFirstRecallJournalTransaction=null
  private[mysql] def finishConfirmedDbFirstRecallJournalSql(runner:MySqlDbFirstRecallJournalTransaction):Unit = synchronized {
    require(runner!=null && (dbFirstRecallJournalResponsibility eq runner) && runner.resourcesRetired,
      "original journal must actually retire confirmed SQL resources")
    runner.requireRegisteredWorker()
    dbFirstRecallJournalResponsibility=null
  }
  private var dbFirstRecallRefusalResponsibility:MySqlDbFirstRecallRefusalTransaction=null
  private[mysql] def finishConfirmedDbFirstRecallRefusalSql(runner:MySqlDbFirstRecallRefusalTransaction):Unit = synchronized {
    require(runner!=null && (dbFirstRecallRefusalResponsibility eq runner) && runner.resourcesRetired,
      "original refusal must actually retire confirmed SQL resources")
    runner.requireRegisteredWorker()
    dbFirstRecallRefusalResponsibility=null
  }
  private[mysql] def beginDbFirstRecallRefusal(runner:MySqlDbFirstRecallRefusalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && runner!=null &&
      dbFirstRecallJournalResponsibility==null && dbFirstRecallRefusalResponsibility==null && dbFirstRecallResponsibility==null &&
      dbFirstCheckpointResponsibility==null && dbFirstInitialActivationResponsibility==null &&
      dbFirstInitialPreparationResponsibility==null,"one original refusal SQL responsibility required")
    runner.requireRegisteredWorker()
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original refusal admission required")
    require(runner.bindingUuid==admission.token.bindingUuid,"original refusal writer binding required")
    dbFirstRecallRefusalResponsibility=runner
  }
  private[mysql] def requireDbFirstRecallRefusal(runner:MySqlDbFirstRecallRefusalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner!=null && (dbFirstRecallRefusalResponsibility eq runner),"original registered refusal SQL responsibility required")
    runner.requireRegisteredWorker()
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission=>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _=>MySqlSchemaAuditor.reject("refusal lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstRecallRefusal(runner:MySqlDbFirstRecallRefusalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstRecallRefusal(runner,admission,verified,scope)
    require(audit!=null,"registered owned refusal transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire!=Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire!=admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("refusal audit changed original writer owner/epoch")
    qualified
  }
  private[mysql] def beginDbFirstRecallJournal(runner:MySqlDbFirstRecallJournalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && runner!=null &&
      dbFirstRecallRefusalResponsibility==null && dbFirstRecallJournalResponsibility==null && dbFirstRecallResponsibility==null &&
      dbFirstCheckpointResponsibility==null && dbFirstInitialActivationResponsibility==null &&
      dbFirstInitialPreparationResponsibility==null,"one original journal SQL responsibility required")
    runner.requireRegisteredWorker()
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original journal admission required")
    require(runner.request.bindingUuid==admission.token.bindingUuid,"original journal writer binding required")
    dbFirstRecallJournalResponsibility=runner
  }
  private[mysql] def requireDbFirstRecallJournal(runner:MySqlDbFirstRecallJournalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner!=null && (dbFirstRecallJournalResponsibility eq runner),"original registered journal SQL responsibility required")
    runner.requireRegisteredWorker()
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission=>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _=>MySqlSchemaAuditor.reject("journal lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstRecallJournal(runner:MySqlDbFirstRecallJournalTransaction,
      admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstRecallJournal(runner,admission,verified,scope)
    require(audit!=null,"registered owned journal transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire!=Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire!=admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("journal audit changed original writer owner/epoch")
    qualified
  }
  private var dbFirstRecallResponsibility:MySqlDbFirstRecallTransaction=null
  /** 只核原保留责任，不把失效writer lease当成新的执行资格。 */
  private[mysql] def requireFailedDbFirstRecallObservation(runner:MySqlDbFirstRecallTransaction):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner != null &&
      (dbFirstRecallResponsibility eq runner),"original unresolved schema6 recall responsibility required")
    runner.requireFailedCommitObservation()
  }
  private[mysql] def finishConfirmedDbFirstSql(runner:MySqlDbFirstRecallTransaction):Unit = synchronized {
    require(runner != null && (dbFirstRecallResponsibility eq runner) && runner.resourcesRetired,
      "exact runner must actually retire all confirmed SQL resources")
    // 原Entry/Main的每Pod责任继续保留，但已关闭SQL图不能继续占共享writer槽。
    dbFirstRecallResponsibility=null
  }
  /** 只释放实际关闭的失败SQL槽；不重新审计、续租、恢复writer或确认原业务。 */
  private[mysql] def finishFailedDbFirstRecallSql(runner:MySqlDbFirstRecallTransaction):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner!=null &&
      (dbFirstRecallResponsibility eq runner) && runner.failedSqlResourcesRetired,
      "exact original failed recall must actually retire its local SQL resources")
    runner.requireFailedCommitObservation()
    dbFirstRecallResponsibility=null
  }
  private[mysql] def beginDbFirstRecall(runner:MySqlDbFirstRecallTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner != null && dbFirstRecallResponsibility == null && dbFirstCheckpointResponsibility == null && dbFirstInitialActivationResponsibility == null && dbFirstInitialPreparationResponsibility == null && dbFirstRecallRefusalResponsibility == null && dbFirstRecallJournalResponsibility == null,
      "one exact schema6 recall responsibility required")
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original schema6 recall admission required")
    dbFirstRecallResponsibility=runner // ACK亦不清空；世界cleanup/UNKNOWN结算尚未接线。
  }
  private[mysql] def requireDbFirstRecall(runner:MySqlDbFirstRecallTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner != null && (dbFirstRecallResponsibility eq runner),"original registered schema6 transaction required")
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission =>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _ => MySqlSchemaAuditor.reject("schema6 transaction lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstRecall(runner:MySqlDbFirstRecallTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstRecall(runner,admission,verified,scope)
    require(audit != null,"registered owned schema6 transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire != admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("schema6 transaction audit changed exact writer owner/epoch")
    qualified
  }
  private var dbFirstCheckpointResponsibility:MySqlDbFirstCheckpointTransaction=null
  private[mysql] def finishConfirmedDbFirstCheckpointSql(runner:MySqlDbFirstCheckpointTransaction):Unit = synchronized {
    require(runner != null && (dbFirstCheckpointResponsibility eq runner) && runner.resourcesRetired,
      "exact runner must actually retire all confirmed SQL resources")
    // 原Entry/Main的每Pod责任继续保留，但已关闭SQL图不能继续占共享writer槽。
    dbFirstCheckpointResponsibility=null
  }
  private[mysql] def beginDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner != null && dbFirstCheckpointResponsibility == null && dbFirstRecallResponsibility == null && dbFirstInitialActivationResponsibility == null && dbFirstInitialPreparationResponsibility == null && dbFirstRecallRefusalResponsibility == null && dbFirstRecallJournalResponsibility == null,
      "one exact schema6 checkpoint responsibility required")
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original schema6 checkpoint admission required")
    dbFirstCheckpointResponsibility=runner // ACK亦不清空；世界cleanup/UNKNOWN结算尚未接线。
  }
  private[mysql] def requireDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner != null && (dbFirstCheckpointResponsibility eq runner),"original registered schema6 transaction required")
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission =>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _ => MySqlSchemaAuditor.reject("schema6 transaction lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstCheckpoint(runner,admission,verified,scope)
    require(audit != null,"registered owned schema6 transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire != admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("schema6 transaction audit changed exact writer owner/epoch")
    qualified
  }
  private var dbFirstInitialActivationResponsibility:MySqlDbFirstInitialActivationTransaction=null
  private[mysql] def finishConfirmedDbFirstInitialSql(runner:MySqlDbFirstInitialActivationTransaction):Unit = synchronized {
    require(runner != null && (dbFirstInitialActivationResponsibility eq runner) && runner.resourcesRetired,
      "exact runner must actually retire all confirmed SQL resources")
    // 原Entry/Main的每Pod责任继续保留，但已关闭SQL图不能继续占共享writer槽。
    dbFirstInitialActivationResponsibility=null
  }
  private[mysql] def beginDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner != null && dbFirstInitialActivationResponsibility == null && dbFirstRecallResponsibility == null && dbFirstCheckpointResponsibility == null && dbFirstInitialPreparationResponsibility == null && dbFirstRecallRefusalResponsibility == null && dbFirstRecallJournalResponsibility == null,
      "one exact schema6 recall responsibility required")
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original schema6 recall admission required")
    dbFirstInitialActivationResponsibility=runner // ACK亦不清空；世界cleanup/UNKNOWN结算尚未接线。
  }
  private[mysql] def requireDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner != null && (dbFirstInitialActivationResponsibility eq runner),"original registered schema6 transaction required")
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission =>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _ => MySqlSchemaAuditor.reject("schema6 transaction lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstInitialActivation(runner,admission,verified,scope)
    require(audit != null,"registered owned schema6 transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire != admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("schema6 transaction audit changed exact writer owner/epoch")
    qualified
  }
  private var dbFirstInitialPreparationResponsibility:MySqlDbFirstInitialPreparationTransaction=null
  private[mysql] def finishConfirmedDbFirstPreparationSql(runner:MySqlDbFirstInitialPreparationTransaction):Unit = synchronized {
    require(runner != null && (dbFirstInitialPreparationResponsibility eq runner) && runner.resourcesRetired,
      "exact preparation runner must actually retire SQL resources")
    dbFirstInitialPreparationResponsibility=null
  }
  private[mysql] def finishRejectedDbFirstPreparationSql(runner:MySqlDbFirstInitialPreparationTransaction,
      cause:MySqlDbFirstPreparationRejected,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    ensureNotFrozen()
    requireDbFirstInitialPreparation(runner,admission,verified,scope)
    runner.requireRejectedResourcesRetired(cause)
    dbFirstInitialPreparationResponsibility=null
  }
  private[mysql] def beginDbFirstInitialPreparation(runner:MySqlDbFirstInitialPreparationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && runner != null && dbFirstInitialPreparationResponsibility == null && dbFirstRecallResponsibility == null && dbFirstCheckpointResponsibility == null && dbFirstInitialActivationResponsibility == null && dbFirstRecallRefusalResponsibility == null && dbFirstRecallJournalResponsibility == null,
      "one exact schema6 recall responsibility required")
    val actual=requireBusinessAdmission(verified,scope)
    require(actual eq admission,"original schema6 recall admission required")
    dbFirstInitialPreparationResponsibility=runner // ACK亦不清空；世界cleanup/UNKNOWN结算尚未接线。
  }
  private[mysql] def requireDbFirstInitialPreparation(runner:MySqlDbFirstInitialPreparationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit = synchronized {
    require(runner != null && (dbFirstInitialPreparationResponsibility eq runner),"original registered schema6 transaction required")
    validateScope(verified,scope,allowReconnect=false)
    phase match
      case MySqlWriterFenceAdmitted(current) if current eq admission =>
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _ => MySqlSchemaAuditor.reject("schema6 transaction lost its exact original writer admission")
  }
  private[mysql] def auditDbFirstInitialPreparation(runner:MySqlDbFirstInitialPreparationTransaction,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification = synchronized {
    requireDbFirstInitialPreparation(runner,admission,verified,scope)
    require(audit != null,"registered owned schema6 transaction audit required")
    val (_,qualified)=audit.preAcquire(this,verified,scope)
    requireQualification(qualified,verified,scope)
    if qualified.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) ||
      qualified.writerEpochObservedBeforeAcquire != admission.token.writerEpoch then
      MySqlSchemaAuditor.reject("schema6 transaction audit changed exact writer owner/epoch")
    qualified
  }
  private[mysql] def beginOwnedDbFirstRenewal(admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit != null,
      "original schema6 owned renewal required")
    ensureNotFrozen()
    phase match
      case MySqlWriterFenceAdmitted(current) if admission != null && (current eq admission) =>
        // 在新的audit配置或barrier IO之前撤销旧业务admission；保留原W/token供失败清理。
        phase=MySqlWriterFenceRenewing(current.token,current)
        dbFirstPostAuditCompleted=false
        validateScope(verified,scope,allowReconnect=false)
        validateToken(current.token,verified,scope)
        validateReceipt(current.token,current.receipt,verified,scope)
      case _ => MySqlSchemaAuditor.reject("schema6 renewal requires its exact current admission")
  }
  private[mysql] def renewOwnedDbFirst(admission:MySqlBusinessAdmission,verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,audit:MySqlSchemaAuditor.DbFirstAudit):MySqlWriterFenceToken = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit != null && audit != null,
      "original schema6 owned renewal audit required")
    dbFirstAudit=audit
    dbFirstPostAuditCompleted=false
    renew(admission,verified,scope)
  }
  private[mysql] def requireOwnedDbFirstAuditRetirement(previousToken:MySqlWriterFenceToken,
      previousAdmission:MySqlBusinessAdmission,actualToken:MySqlWriterFenceToken,
      actualAdmission:MySqlBusinessAdmission,previousPre:MySqlSchemaAuditor.DbFirstAudit,
      previousPost:MySqlSchemaAuditor.DbFirstAudit):Unit = synchronized {
    ensureNotFrozen()
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && pendingBusinessUnknownRecovery.isEmpty,
      "schema6 audit retirement cannot retain unknown business consumers")
    phase match
      case MySqlWriterFenceAdmitted(current) if (current eq actualAdmission) && (current.token eq actualToken) &&
          previousToken != null && previousAdmission != null && (previousToken ne actualToken) &&
          (previousAdmission ne actualAdmission) && (previousToken.qualification ne actualToken.qualification) &&
          (previousAdmission.receipt ne actualAdmission.receipt) && (dbFirstAudit ne previousPre) &&
          (dbFirstAudit ne previousPost) => ()
      case _ => MySqlSchemaAuditor.reject("superseded schema6 audit still has a current token/admission/provider consumer")
  }
  private[mysql] def requireCurrentDbFirstAuditConsumers(token:MySqlWriterFenceToken,admission:MySqlBusinessAdmission,
      pre:MySqlSchemaAuditor.DbFirstAudit,post:MySqlSchemaAuditor.DbFirstAudit):Unit = synchronized {
    ensureNotFrozen()
    require(expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && dbFirstPostAuditCompleted &&
      pre!=null && post!=null && (dbFirstAudit eq pre) && pendingBusinessUnknownRecovery.isEmpty,
      "actual completed current schema6 audit consumers required")
    phase match
      case MySqlWriterFenceAdmitted(current) if (current eq admission) && (current.token eq token)=>
        pre.requireCurrentPreConsumer(this,token)
        post.requireCurrentPostConsumer(this,admission)
      case _=>MySqlSchemaAuditor.reject("actual current token/admission do not own these audit consumers")
  }
  private[mysql] def releaseOwnedDbFirst(token:MySqlWriterFenceToken,verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,audit:MySqlSchemaAuditor.DbFirstAudit):Unit = synchronized {
    require(expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit != null && audit != null,
      "original schema6 owned release audit required")
    dbFirstAudit=audit
    release(token,verified,scope)
  }
  /** 未转管的新writer沿原Recovery停止；返回本次真实commit结果供独立Full27核对。 */
  private[mysql] def releaseOwnedDbFirstForStoppedRecovery(proof:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      token:MySqlWriterFenceToken,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      audit:MySqlSchemaAuditor.DbFirstAudit):MySqlWriterFenceMutation = synchronized {
    require(proof!=null && expectation.profile==MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit!=null && audit!=null,
      "actual stopped Recovery and independent new-writer release audit required")
    proof.requireStoppedWriterRelease(this,token,verified,scope,audit)
    dbFirstAudit=audit
    releaseConfirmed(token,verified,scope,proof)
  }
  private[mysql] def detachStoppedRecoveredReleaseAudit(proof:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      audit:MySqlSchemaAuditor.DbFirstAudit):Unit = synchronized {
    require(proof!=null && audit!=null && (dbFirstAudit eq audit) && phase==MySqlWriterFenceUnclaimed,
      "actual confirmed stopped-writer release audit required before detach")
    proof.requireStoppedReleaseAuditDetach(this,audit)
    dbFirstAudit=null
  }
  private[mysql] def dbFirstWriterUnclaimed:Boolean = synchronized {
    expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && phase == MySqlWriterFenceUnclaimed
  }
  /** 先完整重新读取 schema，再在同一 verified connection 的事务中 claim 空闲且未持有的 writer 行。 */
  def acquire(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlWriterFenceToken = synchronized {
    ensureNotFrozen()
    ensureUnclaimed()
    validateScope(verified, scope, allowReconnect = false)
    val (observation, qualification) = freshQualification(verified, scope)
    requireQualification(qualification, verified, scope)
    if qualification.writerOwnerBootObservedBeforeAcquire.nonEmpty then
      throw new IllegalStateException("MySQL writer fence is occupied; an owned row is never stolen on lease expiry")
    val mutation = runMutation(
      MySqlWriterFenceOperation.Acquire,
      verified,
      scope,
      observation,
      qualification,
      phase
    ) { (current, databaseNow) =>
      val deadline = leaseDeadline(databaseNow)
      MySqlWriterFenceRules.acquire(current, bootUuid, databaseNow, deadline)
    } { (connection, current, target, databaseNow) =>
      updateAcquire(connection, current, target, databaseNow)
    }
    val token = tokenFor(mutation, qualification, verified, scope)
    phase = MySqlWriterFenceAwaitingAudit(token)
    currentScope = scope
    token
  }

  /** 通过真实 auditor 执行新的完整 schema CALL，再打开业务 admission。 */
  def auditAndConfirmPostAcquire(token: MySqlWriterFenceToken): MySqlBusinessAdmission = synchronized {
    if expectation.profile == MySqlSchemaAuditProfile.DbFirst1 then
      MySqlSchemaAuditor.reject("schema6 requires its registered owned post-acquire audit")
    requireAwaiting(token)
    if !token.verified.connection.getAutoCommit then
      MySqlSchemaAuditor.reject("post-acquire schema audit requires an idle auto-commit connection")
    val observation = auditor.readCurrentObjectManifest(token.verified, token.scope)
    val receipt = auditor.auditAfterWriterAcquire(observation, token.qualification, token.scope, token.fence)
    confirmPostAcquireAudit(token, receipt)
  }

  /** 仅接受当前 token、相同 owned scope 和它对应的 fresh receipt。 */
  def confirmPostAcquireAudit(
      token: MySqlWriterFenceToken,
      receipt: RuntimeSchemaAuditReceipt
  ): MySqlBusinessAdmission = synchronized {
    ensureNotFrozen()
    requireAwaiting(token)
    if expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && !dbFirstPostAuditCompleted then
      MySqlSchemaAuditor.reject("schema6 receipt requires actual registered owned post-acquire audit")
    if receipt == null then MySqlSchemaAuditor.reject("post-acquire MySQL schema audit receipt is required")
    validateScope(token.verified, token.scope, allowReconnect = false)
    validateToken(token, token.verified, token.scope)
    validateReceipt(token, receipt, token.verified, token.scope)
    val admission = new MySqlBusinessAdmission(token, receipt)
    phase = MySqlWriterFenceAdmitted(admission)
    admission
  }

  /** 只接受 Fresh B1 service 持有的 opaque proof；普通 acquire 仍要求现成 CERTIFIED binding。 */
  private[mysql] def admitFreshWorldBootstrap(
      proof: MySqlFreshWorldBootstrap.CompletionProof
  ): MySqlBusinessAdmission = synchronized {
    ensureNotFrozen()
    ensureUnclaimed()
    if proof == null then MySqlSchemaAuditor.reject("opaque completed Fresh bootstrap proof is required")
    val verified = proof.verified
    val scope = proof.scope
    validateScope(verified, scope, allowReconnect = false)
    proof.validateFor(expectation, this)
    validateToken(proof.token, verified, scope)
    validateReceipt(proof.token, proof.receipt, verified, scope)
    val admission = new MySqlBusinessAdmission(proof.token, proof.receipt)
    phase = MySqlWriterFenceAdmitted(admission)
    currentScope = scope
    admission
  }

  /** 同actor新planned Fresh proof；保留normal scope/token/receipt admission全部校验。 */
  private[mysql] def admitPlannedFreshWorldBootstrap(
      proof: MySqlFreshCertifyCapabilities.CompletionProof
  ): MySqlBusinessAdmission = synchronized {
    ensureNotFrozen(); ensureUnclaimed()
    if proof == null then MySqlSchemaAuditor.reject("opaque current planned Fresh completion proof required")
    val verified = proof.verified(); val scope = proof.scope()
    validateScope(verified, scope, allowReconnect = false)
    proof.owner().validatePlannedCompletionProof(proof, expectation, this)
    validateToken(proof.token(), verified, scope)
    validateReceipt(proof.token(), proof.receipt(), verified, scope)
    val admission = new MySqlBusinessAdmission(proof.token(), proof.receipt())
    phase = MySqlWriterFenceAdmitted(admission)
    currentScope = scope
    admission
  }

  /** Ordinary HANDOFF never re-acquires W; only its same-actor post-audit/native completion can be adopted. */
  private[mysql] def admitDeploymentReceiptHandoff(
      proof:MySqlDeploymentReceiptSession.OrdinaryAdoption5
  ):MySqlBusinessAdmission = synchronized {
    ensureNotFrozen();ensureUnclaimed()
    if proof==null then MySqlSchemaAuditor.reject("original schema5 HANDOFF completion proof required")
    proof.consume(expectation,this) { (verified,scope,token,receipt)=>
      validateScope(verified,scope,allowReconnect=false)
      validateToken(token,verified,scope);validateReceipt(token,receipt,verified,scope)
      val admission=new MySqlBusinessAdmission(token,receipt)
      phase=MySqlWriterFenceAdmitted(admission);currentScope=scope
      admission
    }
  }

  /** schema5 Recall 保持 Unclaimed；同一原 keeper 只可验证和续租，不能发布 business admission。 */
  private var deploymentRecall:MySqlDeploymentReceiptSession.RestrictedRecall5=null
  private[mysql] def adoptDeploymentReceiptRecall(proof:MySqlDeploymentReceiptSession.RestrictedRecall5):Unit = synchronized {
    ensureNotFrozen();ensureUnclaimed()
    if proof==null || deploymentRecall!=null then MySqlSchemaAuditor.reject("one original schema5 restricted Recall keeper required")
    proof.withCurrent(expectation,this) { (verified,scope,token,receipt)=>
      validateScope(verified,scope,allowReconnect=false)
      validateToken(token,verified,scope);validateReceipt(token,receipt,verified,scope)
      currentScope=scope
      deploymentRecall=proof
    }
  }
  private[mysql] def renewDeploymentReceiptRecall(proof:MySqlDeploymentReceiptSession.RestrictedRecall5):Unit = synchronized {
    ensureNotFrozen();ensureUnclaimed()
    if proof==null || (proof ne deploymentRecall) then MySqlSchemaAuditor.reject("original schema5 restricted Recall renewal required")
    proof.withCurrent(expectation,this) { (verified,scope,oldToken,oldReceipt)=>
      validateScope(verified,scope,allowReconnect=false)
      validateToken(oldToken,verified,scope);validateReceipt(oldToken,oldReceipt,verified,scope)
      val (observation,qualification)=freshQualification(verified,scope)
      requireQualification(qualification,verified,scope)
      if qualification.writerEpochObservedBeforeAcquire!=oldToken.writerEpoch ||
          qualification.writerOwnerBootObservedBeforeAcquire!=Some(bootUuid) then
        MySqlSchemaAuditor.reject("restricted schema5 Recall pre-renew audit lost original writer")
      val mutation=runMutation(MySqlWriterFenceOperation.Renew,verified,scope,observation,qualification,MySqlWriterFenceUnclaimed) {
        (current,databaseNow)=>
          if !MySqlWriterFenceRules.matchesTokenRow(current,oldToken.bindingUuid,oldToken.writerEpoch,bootUuid,oldToken.leaseUntil) then
            MySqlSchemaAuditor.reject("restricted schema5 Recall exact W changed before renewal")
          MySqlWriterFenceRules.renew(current,bootUuid,databaseNow,leaseDeadline(databaseNow))
      } { (connection,current,target,databaseNow)=>updateRenew(connection,current,target,databaseNow) }
      try
        proof.retainPendingRenewal(this,mutation)
        val token=tokenFor(mutation,qualification,verified,scope)
        val receipt=auditor.auditAfterWriterAcquire(auditor.readCurrentObjectManifest(verified,scope),qualification,scope,token.fence)
        validateToken(token,verified,scope);validateReceipt(token,receipt,verified,scope)
        proof.retainRenewed(this,token,receipt)
      catch case cause:Throwable=>
        phase=MySqlWriterFenceUnknown(MySqlWriterFenceOperation.Renew,Some(mutation.row),None,scope,cause)
        throw operationFailure(MySqlWriterFenceOperation.Renew,cause)
    }
  }

  private[mysql] def admitPlannedCertifiedHandoff(
      proof: MySqlCertifiedOrdinaryCapabilities.CompletedCertifiedHandoff
  ): MySqlBusinessAdmission = synchronized {
    if proof == null then MySqlSchemaAuditor.reject("opaque completed Certified HANDOFF proof is required")
    val actor = proof.actor
    requireOrdinaryPhase(actor, awaitingNative = true)
    try {
      if phase != MySqlWriterFenceUnclaimed then
        MySqlSchemaAuditor.reject("ordinary completion cannot adopt a writer coordinator already used by a generic path")
      val verified = proof.verified
      val scope = proof.scope
      validateScope(verified, scope, allowReconnect = false, ordinaryActor = Some(actor))
      proof.validateFor(expectation, this)
      validateToken(proof.token, verified, scope)
      validateReceipt(proof.token, proof.receipt, verified, scope)
      val admission = new MySqlBusinessAdmission(proof.token, proof.receipt)
      phase = MySqlWriterFenceAdmitted(admission)
      currentScope = scope
      certifiedOrdinaryResponsibility = None
      admission
    } catch {
      case cause: Throwable =>
        certifiedOrdinaryResponsibility = Some(HandoffFaulted(actor, cause))
        throw cause
    }
  }

  /** 原受限actor的维护通道；coordinator保持Unclaimed，绝不临时构造business admission。 */
  private[mysql] def requireRecallRecovery(
      proof: MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission): Unit = synchronized {
    require(proof != null, "original restricted Recall capability required")
    requireOrdinaryPhase(proof.actor, awaitingNative = true)
    require(phase == MySqlWriterFenceUnclaimed, "restricted Recall cannot borrow generic writer state")
    proof.requireCurrent()
    val completed = proof.completed
    validateScope(completed.verified, completed.scope, allowReconnect = false, ordinaryActor = Some(proof.actor))
    validateToken(proof.token, completed.verified, completed.scope)
    validateReceipt(proof.token, proof.receipt, completed.verified, completed.scope)
  }

  private[mysql] def renewRecallRecovery(
      proof: MySqlCertifiedOrdinaryCapabilities.RecallRecoveryAdmission
  ): (MySqlWriterFenceToken, RuntimeSchemaAuditReceipt) = synchronized {
    requireRecallRecovery(proof)
    val verified = proof.completed.verified
    val scope = proof.completed.scope
    val oldToken = proof.token
    try
      val (observation, qualification) = freshQualification(verified, scope)
      requireQualification(qualification, verified, scope)
      require(qualification.writerEpochObservedBeforeAcquire == oldToken.writerEpoch &&
        qualification.writerOwnerBootObservedBeforeAcquire == Some(bootUuid),
        "restricted Recall pre-renew audit lost its original writer")
      val mutation = runMutation(MySqlWriterFenceOperation.Renew, verified, scope, observation,
        qualification, MySqlWriterFenceUnclaimed) { (current, databaseNow) =>
        require(MySqlWriterFenceRules.matchesTokenRow(current, oldToken.bindingUuid, oldToken.writerEpoch,
          bootUuid, oldToken.leaseUntil), "restricted Recall exact W changed before renewal")
        MySqlWriterFenceRules.renew(current, bootUuid, databaseNow, leaseDeadline(databaseNow))
      } { (connection, current, target, databaseNow) =>
        updateRenew(connection, current, target, databaseNow)
      }
      val token = tokenFor(mutation, qualification, verified, scope)
      val receipt = auditor.auditAfterWriterAcquire(auditor.readCurrentObjectManifest(verified, scope),
        qualification, scope, token.fence)
      validateToken(token, verified, scope)
      validateReceipt(token, receipt, verified, scope)
      (token, receipt)
    catch
      case cause: Throwable =>
        failCertifiedOrdinaryHandoff(proof.actor, cause)
        throw cause
  }

  /** Renew 需要 fresh prequalification；成功后旧 admission 立即失效，并须再次 fresh post-audit 才可写。 */
  def renew(
      admission: MySqlBusinessAdmission,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlWriterFenceToken = synchronized {
    ensureNotFrozen()
    validateScope(verified, scope, allowReconnect = false)
    val oldToken = phase match
      case MySqlWriterFenceAdmitted(currentAdmission) if admission != null && (admission eq currentAdmission) =>
        validateToken(currentAdmission.token, verified, scope)
        validateReceipt(currentAdmission.token, currentAdmission.receipt, verified, scope)
        phase = MySqlWriterFenceRenewing(currentAdmission.token, currentAdmission)
        currentAdmission.token
      case MySqlWriterFenceRenewing(token, currentAdmission) if admission != null && (admission eq currentAdmission) =>
        validateToken(token, verified, scope)
        token
      case _ =>
        MySqlSchemaAuditor.reject("MySQL writer admission is stale or belongs to a different coordinator")
    val (observation, qualification) = freshQualification(verified, scope)
    requireQualification(qualification, verified, scope)
    if qualification.writerEpochObservedBeforeAcquire != oldToken.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) then
      MySqlSchemaAuditor.reject("fresh pre-renew audit does not observe the current boot and writer epoch")
    val mutation = runMutation(
      MySqlWriterFenceOperation.Renew,
      verified,
      scope,
      observation,
      qualification,
      phase
    ) { (current, databaseNow) =>
      if !MySqlWriterFenceRules.matchesTokenRow(
        current,
        oldToken.bindingUuid,
        oldToken.writerEpoch,
        bootUuid,
        oldToken.leaseUntil
      ) then
        MySqlSchemaAuditor.reject("writer fence owner, epoch, binding, or exact token lease changed before renewal")
      MySqlWriterFenceRules.renew(current, bootUuid, databaseNow, leaseDeadline(databaseNow))
    } { (connection, current, target, databaseNow) =>
      updateRenew(connection, current, target, databaseNow)
    }
    val token = tokenFor(mutation, qualification, verified, scope)
    phase = MySqlWriterFenceAwaitingAudit(token)
    token
  }

  /** 只释放仍属本 boot、epoch、binding 且 DB lease 有效的精确 token。 */
  def release(
      token: MySqlWriterFenceToken,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit = synchronized {
    releaseConfirmed(token,verified,scope,null)
    ()
  }

  private def releaseConfirmed(token:MySqlWriterFenceToken,verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,stoppedRecovery:MySqlDbFirstSession.RetiredRecallWriterRecovery):MySqlWriterFenceMutation =
    ensureNotFrozen()
    validateScope(verified, scope, allowReconnect = false)
    requireCurrentToken(token, verified, scope)
    val (observation, qualification) = freshQualification(verified, scope)
    requireQualification(qualification, verified, scope)
    if qualification.writerEpochObservedBeforeAcquire != token.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(bootUuid) then
      MySqlSchemaAuditor.reject("fresh pre-release audit does not observe the current boot and writer epoch")
    val mutation=runMutation(
      MySqlWriterFenceOperation.Release,
      verified,
      scope,
      observation,
      qualification,
      phase
    ) { (current, databaseNow) =>
      if !MySqlWriterFenceRules.matchesTokenRow(
        current,
        token.bindingUuid,
        token.writerEpoch,
        bootUuid,
        token.leaseUntil
      ) then
        MySqlSchemaAuditor.reject("writer fence owner, epoch, binding, or exact token lease changed before release")
      if stoppedRecovery!=null then stoppedRecovery.recordStoppedReleaseBefore(this,current)
      MySqlWriterFenceRules.release(current, bootUuid, databaseNow)
    } { (connection, current, target, databaseNow) =>
      updateRelease(connection, current, target, databaseNow)
    }
    phase = MySqlWriterFenceUnclaimed
    mutation

  /** 业务 commit 结果未知时冻结整个 coordinator；此 slice 不允许只凭 writer slot 读回解除冻结。 */
  def markBusinessCommitUnknown(
      context: MySqlOperationContext,
      primary: Throwable
  ): MySqlCommitUnknownException = markBusinessCommitUnknown(context, None, primary)

  /** 完整 plan 才允许之后的业务效果 reconciliation；旧无 plan API 会永久保持不可解冻。 */
  private[mysql] def markBusinessCommitUnknown(
      context: MySqlOperationContext,
      plan: MySqlBusinessEffectPlan,
      primary: Throwable
  ): MySqlCommitUnknownException = markBusinessCommitUnknown(context, Option(plan), primary)

  private def markBusinessCommitUnknown(
      context: MySqlOperationContext,
      plan: Option[MySqlBusinessEffectPlan],
      primary: Throwable
  ): MySqlCommitUnknownException = synchronized {
    require(context != null && plan != null && primary != null, "business commit unknown requires its exact context, plan option, and primary cause")
    require(plan.forall(value => value != null && context.expectedEffects == value.expectedEffects), "business unknown plan must exactly match its operation context")
    phase match
      case existing: MySqlWriterFenceBusinessUnknown =>
        val duplicate = MySqlCommitUnknownException(context, primary)
        MySqlSchemaAuditor.addSuppressed(existing.failure, duplicate)
        existing.failure
      case previous =>
        val failure = MySqlCommitUnknownException(context, primary)
        val admission = previous match
          case MySqlWriterFenceAdmitted(current) => Some(current)
          case MySqlWriterFenceRenewing(_, current) => Some(current)
          case _ => None
        val oldScope = admission.map(_.token.scope).getOrElse(currentScope)
        phase = new MySqlWriterFenceBusinessUnknown(context, plan, failure, previous, admission, oldScope)
        pendingBusinessUnknownRecovery = None
        failure
  }

  /** 建立重连核验快照；不改变 frozen 状态，且旧的无 plan unknown 永久拒绝。 */
  private[mysql] def beginBusinessUnknownReconciliation(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlBusinessUnknownSnapshot = synchronized {
    if modeWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then
      MySqlSchemaAuditor.reject("F3 mode workflow responsibility cannot be cleared by F2 business reconciliation")
    val unknown = phase match
      case value: MySqlWriterFenceBusinessUnknown => value
      case _ => MySqlSchemaAuditor.reject("there is no frozen business commit to reconcile")
    pendingBusinessUnknownRecovery = None
    val (plan, oldAdmission) = MySqlBusinessUnknownProofRules.requireFrozenResponsibility(unknown.plan, unknown.oldAdmission)
    validateScope(verified, scope, allowReconnect = true)
    if scope.connectionGeneration <= unknown.oldScope.connectionGeneration ||
      (verified.connection eq unknown.oldScope.connectionToken) then
      MySqlSchemaAuditor.reject("business reconciliation requires a different verified connection at a newer generation")
    val oldToken = oldAdmission.token
    if oldToken.bootUuid != unknown.context.bootUuid || oldToken.writerEpoch != unknown.context.writerEpoch ||
      oldToken.bindingUuid != unknown.context.bindingUuid || unknown.context.expectedEffects != plan.expectedEffects then
      MySqlSchemaAuditor.reject("captured business plan, context, and original admission do not identify the same operation")
    val (_, qualification) = freshQualification(verified, scope)
    requireQualification(qualification, verified, scope)
    if qualification.writerEpochObservedBeforeAcquire != oldToken.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(oldToken.bootUuid) then
      MySqlSchemaAuditor.reject("fresh reconnect qualification does not observe the original boot and writer epoch")
    new MySqlBusinessUnknownSnapshot(unknown, unknown.context, plan, oldAdmission, unknown.oldScope, verified, scope, qualification)
  }

  /** 只在密封 SQL proof 精确匹配后为同一 epoch/lease 产生一个待 post-audit token。 */
  private[mysql] def issueBusinessUnknownRecoveryToken(
      snapshot: MySqlBusinessUnknownSnapshot,
      proof: MySqlBusinessUnknownProof
  ): MySqlWriterFenceToken = synchronized {
    requireBusinessUnknownSnapshot(snapshot)
    MySqlBusinessUnknownProofRules.validate(snapshot, proof, expectation)
    requireQualification(snapshot.qualification, snapshot.verified, snapshot.scope)
    val row = proof.writerRow
    val mutation = MySqlWriterFenceMutation(
      MySqlWriterFenceRow(
        row.bindingUuid,
        row.writerEpoch,
        row.ownerBootUuid,
        row.leaseUntil,
        row.lastOperationUuid,
        row.lastFingerprint
      ),
      row.databaseNow,
      proof.monotonicAnchorNanos
    )
    val token = tokenFor(mutation, snapshot.qualification, snapshot.verified, snapshot.scope)
    pendingBusinessUnknownRecovery = Some(snapshot -> token)
    token
  }

  /** 只有同一冻结 operation 的完整 effects proof 与 fresh post-audit 才重新开放 business admission。 */
  private[mysql] def confirmBusinessUnknownReconciliation(
      snapshot: MySqlBusinessUnknownSnapshot,
      proof: MySqlBusinessUnknownProof,
      token: MySqlWriterFenceToken,
      receipt: RuntimeSchemaAuditReceipt
  ): MySqlBusinessAdmission = synchronized {
    requireBusinessUnknownSnapshot(snapshot)
    MySqlBusinessUnknownProofRules.validate(snapshot, proof, expectation)
    val pending = pendingBusinessUnknownRecovery.getOrElse(
      MySqlSchemaAuditor.reject("business reconciliation has no exact coordinator-issued post-audit token")
    )
    if (pending._1 ne snapshot) || (pending._2 ne token) then
      MySqlSchemaAuditor.reject("business reconciliation token does not match the exact pending proof and snapshot")
    validateScope(snapshot.verified, snapshot.scope, allowReconnect = true)
    validateToken(token, snapshot.verified, snapshot.scope)
    validateReceipt(token, receipt, snapshot.verified, snapshot.scope)
    val admission = new MySqlBusinessAdmission(token, receipt)
    currentScope = snapshot.scope
    phase = MySqlWriterFenceAdmitted(admission)
    pendingBusinessUnknownRecovery = None
    admission
  }

  /** 重连核验错误被抛给调用方，同时附在最初 unknown cause 上；phase 不变。 */
  private[mysql] def recordBusinessUnknownReconciliationFailure(
      snapshot: Option[MySqlBusinessUnknownSnapshot],
      primary: Throwable
  ): Unit = synchronized {
    require(primary != null, "business reconciliation failure is required")
    phase match
      case unknown: MySqlWriterFenceBusinessUnknown if snapshot.forall(value => value.unknownState eq unknown) =>
        snapshot.foreach { value =>
          pendingBusinessUnknownRecovery match
            case Some((pendingSnapshot, _)) if pendingSnapshot eq value => pendingBusinessUnknownRecovery = None
            case _ => ()
        }
        MySqlSchemaAuditor.addSuppressed(unknown.failure, primary)
      case _ => ()
  }

  private def requireBusinessUnknownSnapshot(snapshot: MySqlBusinessUnknownSnapshot): Unit =
    if snapshot == null then MySqlSchemaAuditor.reject("business unknown reconciliation snapshot is required")
    phase match
      case unknown: MySqlWriterFenceBusinessUnknown if snapshot.unknownState eq unknown =>
        if !unknown.plan.contains(snapshot.plan) || !unknown.oldAdmission.contains(snapshot.oldAdmission) ||
          (unknown.oldScope ne snapshot.oldScope) then
          MySqlSchemaAuditor.reject("business unknown snapshot differs from the exact frozen plan or prior admission")
      case _ => MySqlSchemaAuditor.reject("business unknown snapshot is stale or no longer frozen")

  /** 只可核验 fence acquire/renew/release 的未知结果。 */
  def reconcileUnknown(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlWriterFenceReconciliation = synchronized {
    if modeWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then
      MySqlSchemaAuditor.reject("F3 mode workflow responsibility cannot be cleared by writer-slot reconciliation")
    val unknown = phase match
      case value: MySqlWriterFenceUnknown => value
      case _: MySqlWriterFenceBusinessUnknown =>
        MySqlSchemaAuditor.reject("business commit unknown remains frozen until exact effects readback is implemented")
      case _ =>
        MySqlSchemaAuditor.reject("there is no unknown MySQL writer fence operation to reconcile")
    validateScope(verified, scope, allowReconnect = true)
    if scope.connectionGeneration <= unknown.oldScope.connectionGeneration ||
      (verified.connection eq unknown.oldScope.connectionToken) then
      MySqlSchemaAuditor.reject("fence reconciliation requires a different verified connection at a newer generation")
    val (_, qualification) = freshQualification(verified, scope)
    requireQualification(qualification, verified, scope)
    verified.readmitSessionBarrier(expectation, scope)
    val readStarted = System.nanoTime()
    val (actual, databaseNow) = readWriterWithDatabaseTime(verified.connection, forUpdate = false)
    val expected = unknown.expectedRow
    if !MySqlWriterFenceRules.hasDistinctExpectedTarget(expected, unknown.previousRow) then
      phase = unknown
      MySqlWriterFenceReconciliation.Unproven
    else if actual == expected.get then
      unknown.operation match
        case MySqlWriterFenceOperation.Release =>
          if actual.ownerBootUuid.isEmpty && !actual.leaseUntil.isAfter(databaseNow) then
            phase = MySqlWriterFenceUnclaimed
            currentScope = scope
            MySqlWriterFenceReconciliation.ConfirmedReleased
          else
            phase = unknown
            MySqlWriterFenceReconciliation.ConfirmedExpired
        case MySqlWriterFenceOperation.Acquire | MySqlWriterFenceOperation.Renew =>
          if actual.ownerBootUuid.contains(bootUuid) &&
            qualification.writerEpochObservedBeforeAcquire == actual.epoch &&
            qualification.writerOwnerBootObservedBeforeAcquire == Some(bootUuid) then
            if actual.leaseUntil.isAfter(databaseNow) then
              val mutation = MySqlWriterFenceMutation(actual, databaseNow, readStarted)
              val token = tokenFor(mutation, qualification, verified, scope)
              phase = MySqlWriterFenceAwaitingAudit(token)
              currentScope = scope
              MySqlWriterFenceReconciliation.ConfirmedCurrent
            else
              phase = unknown
              MySqlWriterFenceReconciliation.ConfirmedExpired
          else
            phase = unknown
            MySqlWriterFenceReconciliation.Conflict
    else if unknown.previousRow.contains(actual) then
      phase = unknown
      MySqlWriterFenceReconciliation.Unproven
    else
      phase = unknown
      MySqlWriterFenceReconciliation.Conflict
  }

  /** 保留的冻结原因供权威边界记录原始 cause 与 suppressed 清理失败。 */
  def frozenFailure: Option[Throwable] = synchronized {
    normalRecallWorkflowResponsibility.flatMap(value => value.blockedFailure.map(cause => cause: Throwable))
      .orElse(controlledDebugWorkflowResponsibility.flatMap(value => value.blockedFailure.map(cause => cause: Throwable)))
      .orElse(modeWorkflowResponsibility.flatMap(value => value.blockedFailure.map(cause => cause: Throwable)))
      .orElse(driftWorkflowResponsibility.flatMap(value => value.blockedFailure.map(cause => cause: Throwable))).orElse(inactiveDomainWorkflowResponsibility.flatMap(value => value.blockedFailure.map(cause => cause: Throwable))).orElse {
      phase match
        case value: MySqlWriterFenceUnknown => Some(value.failure)
        case value: MySqlWriterFenceBusinessUnknown => Some(value.failure)
        case _ => None
    }
  }
  private def runMutation(
      operation: MySqlWriterFenceOperation,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      observation: UnverifiedMySqlSchemaObservation,
      qualification: VerifiedSchemaQualification,
      priorPhase: MySqlWriterFencePhase
  )(
      transition: (MySqlWriterFenceRow, LocalDateTime) => MySqlWriterFenceRow
  )(
      update: (Connection, MySqlWriterFenceRow, MySqlWriterFenceRow, LocalDateTime) => Unit
  ): MySqlWriterFenceMutation =
    val connection = verified.connection
    val callerBefore = if dbFirstAudit == null then auditor.readCallerScopeBeforeCall(verified, scope)
      else dbFirstAudit.caller(this,verified,scope)
    validateCallerSnapshot(callerBefore, observation)
    var transactionAttempted = false
    var commitAttempted = false
    var previousRow: Option[MySqlWriterFenceRow] = None
    var expectedRow: Option[MySqlWriterFenceRow] = None
    try
      if !connection.getAutoCommit then MySqlSchemaAuditor.reject("writer fence requires an idle auto-commit JDBC connection")
      transactionAttempted = true
      connection.setAutoCommit(false)
      verified.readmitSessionBarrier(expectation, scope)
      lockAndValidateSchema(connection, qualification)
      val (current, databaseNow) = readWriterWithDatabaseTime(connection, forUpdate = true)
      previousRow = Some(current)
      expectedRow = Some(current)
      if current.epoch != qualification.writerEpochObservedBeforeAcquire ||
        current.ownerBootUuid != qualification.writerOwnerBootObservedBeforeAcquire then
        MySqlSchemaAuditor.reject("writer row changed after the complete pre-operation audit")
      val target = transition(current, databaseNow)
      expectedRow = Some(target)
      update(connection, current, target, databaseNow)
      val (beforeCommit, beforeCommitTime) = readWriterWithDatabaseTime(connection, forUpdate = true)
      if beforeCommit != target then MySqlSchemaAuditor.reject("writer row differs from the exact in-transaction target")
      if operation == MySqlWriterFenceOperation.Release then
        if beforeCommit.leaseUntil.isAfter(beforeCommitTime) then
          MySqlSchemaAuditor.reject("released writer lease is not in the past before commit")
      else if !beforeCommit.leaseUntil.isAfter(beforeCommitTime) then
        MySqlSchemaAuditor.reject("writer lease expired before transaction commit")
      val callerInTransaction = if dbFirstAudit == null then auditor.readCallerScopeAfterCall(verified, scope)
        else dbFirstAudit.caller(this,verified,scope)
      if callerInTransaction != callerBefore then MySqlSchemaAuditor.reject("MySQL caller scope changed during writer fence transaction")
      requireQualification(qualification, verified, scope)
      commitAttempted = true
      connection.commit()
      connection.setAutoCommit(true)
      transactionAttempted = false
      verified.readmitSessionBarrier(expectation, scope)
      val readStarted = System.nanoTime()
      val (afterCommit, afterCommitTime) = readWriterWithDatabaseTime(connection, forUpdate = false)
      if afterCommit != target then MySqlSchemaAuditor.reject("post-commit writer readback differs from the exact committed target")
      if operation == MySqlWriterFenceOperation.Release then
        if afterCommit.leaseUntil.isAfter(afterCommitTime) then
          MySqlSchemaAuditor.reject("released writer lease is not in the past after commit")
      else if !afterCommit.leaseUntil.isAfter(afterCommitTime) then
        MySqlSchemaAuditor.reject("writer lease expired before exact post-commit readback")
      val callerAfterCommit = if dbFirstAudit == null then auditor.readCallerScopeAfterCall(verified, scope)
        else dbFirstAudit.caller(this,verified,scope)
      if callerAfterCommit != callerBefore then MySqlSchemaAuditor.reject("MySQL caller scope changed across writer fence commit")
      MySqlWriterFenceMutation(afterCommit, afterCommitTime, readStarted)
    catch
      case primary: Throwable =>
        if commitAttempted then
          phase = MySqlWriterFenceUnknown(operation, expectedRow, previousRow, scope, primary)
          throw operationFailure(operation, primary)
        else if transactionAttempted then
          var cleanupFailed = false
          try connection.rollback()
          catch
            case rollbackFailure: Throwable =>
              cleanupFailed = true
              MySqlSchemaAuditor.addSuppressed(primary, rollbackFailure)
          if !cleanupFailed then
            try connection.setAutoCommit(true)
            catch
              case restoreFailure: Throwable =>
                cleanupFailed = true
                MySqlSchemaAuditor.addSuppressed(primary, restoreFailure)
          if cleanupFailed then
            phase = MySqlWriterFenceUnknown(operation, expectedRow, previousRow, scope, primary)
          else
            phase = priorPhase
          throw operationFailure(operation, primary)
        else
          throw operationFailure(operation, primary)

  private def lockAndValidateSchema(
      connection: Connection,
      qualification: VerifiedSchemaQualification
  ): Unit =
    val manifest = readManifest(connection, forUpdate = false)
    val binding = readPhysicalBinding(connection, forUpdate = true)
    validateManifest(manifest, qualification)
    if binding._1 != expectation.bindingUuid || binding._2 != "CERTIFIED" ||
      binding._3 != expectation.expectedPhysicalFormat.version then
      MySqlSchemaAuditor.reject("writer fence requires the exact approved CERTIFIED physical binding")

  private final case class ManifestRow(
      format: Int,
      ddlHash: Vector[Byte],
      provisioningUuid: UUID,
      state: String,
      serverUuid: UUID,
      bindingUuid: UUID,
      importManifestHash: Option[Vector[Byte]],
      importState: String,
      importOperationUuid: Option[UUID],
      importSnapshotHash: Option[Vector[Byte]],
      importReceiptHash: Option[Vector[Byte]],
      guardManifestHash: Option[Vector[Byte]]
  )
  private def readManifest(connection: Connection, forUpdate: Boolean): ManifestRow =
    val suffix = if forUpdate then " FOR UPDATE" else " FOR SHARE"
    val sql = "SELECT singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state," +
      "expected_server_uuid,expected_binding_uuid,import_manifest_sha256,import_state,import_operation_uuid," +
      "import_snapshot_sha256,import_receipt_sha256,guard_manifest_sha256 FROM " + table("fp_schema_manifest") +
      " WHERE singleton_fixed1=1" + suffix
    withPrepared(connection, sql) { statement =>
      withResult(statement.executeQuery()) { result =>
        if !result.next() then MySqlSchemaAuditor.reject("READY schema manifest singleton is missing")
        val singleton = readInt(result, 1, "manifest singleton")
        val backend = readText(result, 2, "manifest backend")
        val format = readInt(result, 3, "manifest format")
        val row = ManifestRow(
          format,
          readBytes(result, 4, "manifest DDL hash", 32).toVector,
          readUuid(result, 5, "manifest provisioning UUID"),
          readText(result, 6, "manifest state"),
          readUuid(result, 7, "manifest server UUID"),
          readUuid(result, 8, "manifest binding UUID"),
          readOptionalBytes(result, 9, "manifest import hash", 32),
          readText(result, 10, "manifest import state"),
          readOptionalUuid(result, 11, "manifest import operation"),
          readOptionalBytes(result, 12, "manifest import snapshot hash", 32),
          readOptionalBytes(result, 13, "manifest import receipt hash", 32),
          readOptionalBytes(result, 14, "manifest guard hash", 32)
        )
        if result.next() then MySqlSchemaAuditor.reject("schema manifest singleton returned more than one row")
        if singleton != 1 || backend != "MYSQL" then MySqlSchemaAuditor.reject("schema manifest singleton or backend is invalid")
        row
      }
    }

  private def validateManifest(row: ManifestRow, qualification: VerifiedSchemaQualification): Unit =
    val expectedImport = expectedImportTuple
    if row.format != expectation.profile.schemaFormat || row.ddlHash != hexBytes(expectation.ddlSha256) ||
      row.provisioningUuid != expectation.provisioningUuid || row.state != "READY" ||
      row.serverUuid != expectation.serverUuid || row.bindingUuid != expectation.bindingUuid ||
      row.guardManifestHash != Some(hexBytes(expectation.objects.canonicalSha256)) ||
      row.importState != expectedImport._1 || row.importOperationUuid != expectedImport._2 ||
      row.importManifestHash != expectedImport._3 || row.importSnapshotHash != expectedImport._4 ||
      row.importReceiptHash != expectedImport._5 then
      MySqlSchemaAuditor.reject("locked MySQL READY manifest differs from the approved schema/import qualification")
    if qualification.profile != expectation.profile ||
      qualification.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      qualification.provisioningUuid != expectation.provisioningUuid ||
      qualification.ddlSha256 != expectation.ddlSha256 ||
      qualification.objectManifestSha256 != expectation.objects.canonicalSha256 ||
      qualification.importState != row.importState ||
      qualification.importOperationUuid != row.importOperationUuid ||
      qualification.importManifestSha256 != expectedImport._6 ||
      qualification.importSnapshotSha256 != expectedImport._4.map(bytesHex) ||
      qualification.importReceiptSha256 != expectedImport._5.map(bytesHex) then
      MySqlSchemaAuditor.reject("schema qualification no longer matches the locked MySQL manifest")

  private def expectedImportTuple:
      (String, Option[UUID], Option[Vector[Byte]], Option[Vector[Byte]], Option[Vector[Byte]], Option[String]) =
    expectation.expectedImport match
      case MySqlExpectedNoImport =>
        ("NONE", None, None, None, None, None)
      case imported: MySqlExpectedCommittedImport =>
        (
          "COMMITTED",
          Some(imported.operationUuid),
          Some(hexBytes(imported.manifestSha256)),
          Some(hexBytes(imported.snapshotSha256)),
          Some(hexBytes(imported.receiptSha256)),
          Some(imported.manifestSha256.toLowerCase(java.util.Locale.ROOT))
        )

  private def readPhysicalBinding(connection: Connection, forUpdate: Boolean): (UUID, String, Int) =
    val suffix = if forUpdate then " FOR UPDATE" else ""
    val sql = "SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM " +
      table("physical_recovery_binding") + " WHERE singleton_fixed1=1" + suffix
    withPrepared(connection, sql) { statement =>
      withResult(statement.executeQuery()) { result =>
        if !result.next() then MySqlSchemaAuditor.reject("physical recovery binding singleton is missing")
        val singleton = readInt(result, 1, "binding singleton")
        val binding = readUuid(result, 2, "physical binding UUID")
        val state = readText(result, 3, "physical binding state")
        val format = readInt(result, 4, "physical binding format")
        if result.next() then MySqlSchemaAuditor.reject("physical recovery binding returned more than one row")
        if singleton != 1 then MySqlSchemaAuditor.reject("physical recovery binding singleton is invalid")
        (binding, state, format)
      }
    }

  private def readWriterWithDatabaseTime(
      connection: Connection,
      forUpdate: Boolean
  ): (MySqlWriterFenceRow, LocalDateTime) =
    val suffix = if forUpdate then " FOR UPDATE" else ""
    val sql = "SELECT singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until," +
      "last_commit_operation_uuid,last_commit_fingerprint,UTC_TIMESTAMP(6) FROM " +
      table("fp_runtime_writer") + " WHERE singleton_fixed1=1" + suffix
    withPrepared(connection, sql) { statement =>
      withResult(statement.executeQuery()) { result =>
        if !result.next() then MySqlSchemaAuditor.reject("runtime writer singleton is missing")
        val singleton = readInt(result, 1, "writer singleton")
        val binding = readUuid(result, 2, "writer binding UUID")
        val epoch = readLong(result, 3, "writer epoch")
        val owner = readOptionalUuid(result, 4, "writer owner boot UUID")
        val lease = readDateTime(result, 5, "writer lease")
        val operation = readOptionalUuid(result, 6, "last commit operation UUID")
        val fingerprint = readOptionalBytes(result, 7, "last commit fingerprint", 32).map(_.toVector)
        val databaseNow = readDateTime(result, 8, "database UTC time")
        if result.next() then MySqlSchemaAuditor.reject("runtime writer singleton returned more than one row")
        if singleton != 1 then MySqlSchemaAuditor.reject("runtime writer singleton value is invalid")
        val row = MySqlWriterFenceRow(binding, epoch, owner, lease, operation, fingerprint)
        if row.bindingUuid != expectation.bindingUuid then MySqlSchemaAuditor.reject("runtime writer binding differs from the approved binding")
        (row, databaseNow)
      }
    }
  private def updateAcquire(
      connection: Connection,
      current: MySqlWriterFenceRow,
      target: MySqlWriterFenceRow,
      databaseNow: LocalDateTime
  ): Unit =
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET writer_epoch=?,owner_boot_uuid=?,lease_until=? WHERE singleton_fixed1=1 AND binding_uuid=? " +
      "AND writer_epoch=? AND owner_boot_uuid IS NULL AND lease_until=? AND lease_until<=?"
    withPrepared(connection, sql) { statement =>
      statement.setLong(1, target.epoch)
      statement.setBytes(2, uuidBytes(bootUuid))
      statement.setObject(3, target.leaseUntil)
      statement.setBytes(4, uuidBytes(current.bindingUuid))
      statement.setLong(5, current.epoch)
      statement.setObject(6, current.leaseUntil)
      statement.setObject(7, databaseNow)
      executeExactlyOne(statement, "writer acquire")
    }

  private def updateRenew(
      connection: Connection,
      current: MySqlWriterFenceRow,
      target: MySqlWriterFenceRow,
      databaseNow: LocalDateTime
  ): Unit =
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET lease_until=? WHERE singleton_fixed1=1 AND binding_uuid=? AND writer_epoch=? " +
      "AND owner_boot_uuid=? AND lease_until=? AND lease_until>? " +
      "AND last_commit_operation_uuid <=> ? AND last_commit_fingerprint <=> ?"
    withPrepared(connection, sql) { statement =>
      statement.setObject(1, target.leaseUntil)
      statement.setBytes(2, uuidBytes(current.bindingUuid))
      statement.setLong(3, current.epoch)
      statement.setBytes(4, uuidBytes(bootUuid))
      statement.setObject(5, current.leaseUntil)
      statement.setObject(6, databaseNow)
      setOptionalUuid(statement, 7, current.lastCommitOperationUuid)
      setOptionalBytes(statement, 8, current.lastCommitFingerprint)
      executeExactlyOne(statement, "writer renew")
    }

  private def updateTerminatedRecallRelease(connection:Connection,current:MySqlWriterFenceRow,
      target:MySqlWriterFenceRow):Unit =
    val sql="UPDATE "+table("fp_runtime_writer")+
      " SET owner_boot_uuid=NULL,lease_until=? WHERE singleton_fixed1=1 AND binding_uuid=? AND writer_epoch=? "+
      "AND owner_boot_uuid=? AND lease_until=? AND last_commit_operation_uuid <=> ? AND last_commit_fingerprint <=> ? "+
      "AND NOT EXISTS(SELECT 1 FROM "+table("fp_recovery_operation")+
      " WHERE binding_uuid=? AND boot_uuid=? AND fence_epoch=? AND operation_state='RESERVED') "+
      "AND NOT EXISTS(SELECT 1 FROM "+table("binding_bootstrap_intent")+" WHERE binding_uuid=? AND stage='PREPARED')"
    withPrepared(connection,sql) { statement=>
      statement.setObject(1,target.leaseUntil)
      statement.setBytes(2,uuidBytes(current.bindingUuid));statement.setLong(3,current.epoch)
      statement.setBytes(4,uuidBytes(bootUuid));statement.setObject(5,current.leaseUntil)
      setOptionalUuid(statement,6,current.lastCommitOperationUuid);setOptionalBytes(statement,7,current.lastCommitFingerprint)
      statement.setBytes(8,uuidBytes(current.bindingUuid));statement.setBytes(9,uuidBytes(bootUuid))
      statement.setLong(10,current.epoch);statement.setBytes(11,uuidBytes(current.bindingUuid))
      executeExactlyOne(statement,"terminated original recall writer retirement")
    }

  private def updateRelease(
      connection: Connection,
      current: MySqlWriterFenceRow,
      target: MySqlWriterFenceRow,
      databaseNow: LocalDateTime
  ): Unit =
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET owner_boot_uuid=NULL,lease_until=? WHERE singleton_fixed1=1 AND binding_uuid=? AND writer_epoch=? " +
      "AND owner_boot_uuid=? AND lease_until=? AND lease_until>? " +
      "AND last_commit_operation_uuid <=> ? AND last_commit_fingerprint <=> ?"
    withPrepared(connection, sql) { statement =>
      statement.setObject(1, target.leaseUntil)
      statement.setBytes(2, uuidBytes(current.bindingUuid))
      statement.setLong(3, current.epoch)
      statement.setBytes(4, uuidBytes(bootUuid))
      statement.setObject(5, current.leaseUntil)
      statement.setObject(6, databaseNow)
      setOptionalUuid(statement, 7, current.lastCommitOperationUuid)
      setOptionalBytes(statement, 8, current.lastCommitFingerprint)
      executeExactlyOne(statement, "writer release")
    }

  private def executeExactlyOne(statement: PreparedStatement, operation: String): Unit =
    val count = statement.executeUpdate()
    if count != 1 then
      MySqlSchemaAuditor.reject("MySQL " + operation + " affected " + count + " rows; exact singleton CAS required")

  private def freshQualification(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): (UnverifiedMySqlSchemaObservation, VerifiedSchemaQualification) =
    if !verified.connection.getAutoCommit then MySqlSchemaAuditor.reject("schema qualification requires an idle auto-commit connection")
    if expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && dbFirstAudit == null then
      MySqlSchemaAuditor.reject("schema6 writer qualification requires its registered allocation owner")
    if dbFirstAudit != null then dbFirstAudit.preAcquire(this,verified,scope)
    else
      val observation = auditor.readCurrentObjectManifest(verified, scope)
      val qualification = auditor.auditBeforeWriterAcquire(observation, scope)
      (observation, qualification)

  private def requireQualification(
      qualification: VerifiedSchemaQualification,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit =
    if qualification == null || !qualification.isCurrent(
      bootUuid,
      bootGeneration,
      scope.connectionGeneration,
      expectation.serverUuid,
      expectation.schema,
      expectation.bindingUuid,
      expectation.profile,
      expectation.expectedPhysicalFormat,
      verified,
      ownerThread,
      System.nanoTime()
    ) then MySqlSchemaAuditor.reject("verified MySQL schema qualification is stale or belongs to a different scope")
    if qualification.profile != expectation.profile ||
      qualification.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      qualification.serverUuid != expectation.serverUuid || qualification.schema != expectation.schema ||
      qualification.bindingUuid != expectation.bindingUuid ||
      qualification.physicalBinding != MySqlCertifiedPhysicalBinding(expectation.bindingUuid, expectation.expectedPhysicalFormat.version) ||
      qualification.provisioningUuid != expectation.provisioningUuid ||
      qualification.ddlSha256 != expectation.ddlSha256 ||
      qualification.objectManifestSha256 != expectation.objects.canonicalSha256 then
      MySqlSchemaAuditor.reject("writer fence requires the exact independently approved READY/CERTIFIED schema qualification")
    val expectedImport = expectedImportTuple
    if qualification.importState != expectedImport._1 ||
      qualification.importOperationUuid != expectedImport._2 ||
      qualification.importManifestSha256 != expectedImport._6 ||
      qualification.importSnapshotSha256 != expectedImport._4.map(bytesHex) ||
      qualification.importReceiptSha256 != expectedImport._5.map(bytesHex) then
      MySqlSchemaAuditor.reject("writer fence qualification has different import provenance")
  private def validateCallerSnapshot(
      actual: MySqlCallerScope,
      observation: UnverifiedMySqlSchemaObservation
  ): Unit =
    if observation == null || observation.before != observation.after ||
      actual != observation.after ||
      actual.serverUuid != expectation.serverUuid ||
      actual.defaultSchema != expectation.schema ||
      actual.effectivePrincipal != expectation.effectiveUser ||
      actual.effectiveRoles != expectation.callerRoles ||
      actual.connectionId <= 0L then
      MySqlSchemaAuditor.reject("writer fence caller scope changed or differs from the approved MySQL principal")

  private def validateScope(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      allowReconnect: Boolean,
      ordinaryActor: Option[MySqlCertifiedRecoveryInspectionActor] = None
  ): Unit =
    certifiedOrdinaryResponsibility.foreach { retained =>
      val dedicated = ordinaryActor.exists(_ eq retained.actor) && (retained match
        case _: AwaitingHandoffAudit | _: AwaitingNativeHandoff => true
        case _: HandoffFaulted => false)
      if !dedicated then
        MySqlSchemaAuditor.reject("ordinary HANDOFF responsibility blocks generic SQL qualification, reconnect, token, and admission paths")
    }
    if verified == null || scope == null || !scope.verifiedConnection.contains(verified) ||
      (verified.connection ne scope.connectionToken) || (verified.identity ne scope.connectionIdentity) then
      MySqlSchemaAuditor.reject("MySQL writer fence requires the exact verified connection bound to its boot scope")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) then
      MySqlSchemaAuditor.reject("MySQL writer fence SQL must run on its explicit owned IO thread")
    if scope.bootUuid != bootUuid || scope.bootGeneration != bootGeneration then
      MySqlSchemaAuditor.reject("MySQL writer fence scope belongs to a different boot generation")
    if verified.identity.serverUuid != expectation.serverUuid ||
      verified.identity.serverVersion != expectation.serverVersion ||
      verified.identity.catalog != expectation.schema ||
      verified.identity.effectiveUser != expectation.effectiveUser then
      MySqlSchemaAuditor.reject("verified MySQL connection identity differs from the approved writer target")
    if allowReconnect then
      if scope.connectionGeneration <= currentScope.connectionGeneration then
        MySqlSchemaAuditor.reject("reconnected MySQL writer scope must have a strictly newer generation")
    else if scope.connectionGeneration != currentScope.connectionGeneration ||
      (scope.connectionToken ne currentScope.connectionToken) then
      MySqlSchemaAuditor.reject("MySQL writer fence operation used a stale connection generation")

  private def validateToken(
      token: MySqlWriterFenceToken,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit =
    if token == null || (token.scope ne scope) || (token.verified ne verified) ||
      (token.scope.connectionToken ne verified.connection) ||
      token.bootUuid != bootUuid || token.bootGeneration != bootGeneration ||
      token.connectionGeneration != scope.connectionGeneration ||
      token.bindingUuid != expectation.bindingUuid ||
      token.fence.ownerBootUuid != bootUuid || token.fence.epoch != token.writerEpoch ||
      !token.fence.isCurrentAt(System.nanoTime()) then
      MySqlSchemaAuditor.reject("MySQL writer fence token is stale, released, expired, or belongs to another scope")
    requireQualification(token.qualification, verified, scope)

  private def validateReceipt(
      token: MySqlWriterFenceToken,
      receipt: RuntimeSchemaAuditReceipt,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit =
    validateToken(token, verified, scope)
    if receipt == null then MySqlSchemaAuditor.reject("runtime schema audit receipt is required")
    receipt.assertCurrent(
      bootUuid,
      bootGeneration,
      scope.connectionGeneration,
      expectation.serverUuid,
      expectation.schema,
      expectation.bindingUuid,
      expectation.profile,
      expectation.expectedPhysicalFormat,
      verified,
      Some(token.fence),
      System.nanoTime()
    )
    if receipt.profile != expectation.profile ||
      receipt.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      receipt.bootUuid != bootUuid || receipt.bootGeneration != bootGeneration ||
      receipt.connectionGeneration != scope.connectionGeneration ||
      receipt.serverUuid != expectation.serverUuid || receipt.schema != expectation.schema ||
      receipt.bindingUuid != expectation.bindingUuid || receipt.writerEpoch != token.writerEpoch ||
      receipt.provisioningUuid != expectation.provisioningUuid ||
      receipt.ddlSha256 != expectation.ddlSha256 ||
      receipt.objectManifestSha256 != expectation.objects.canonicalSha256 ||
      receipt.coverage != expectation.profile.coverage ||
      receipt.importState != token.qualification.importState ||
      receipt.importOperationUuid != token.qualification.importOperationUuid ||
      receipt.importManifestSha256 != token.qualification.importManifestSha256 ||
      receipt.importSnapshotSha256 != token.qualification.importSnapshotSha256 ||
      receipt.importReceiptSha256 != token.qualification.importReceiptSha256 then
      MySqlSchemaAuditor.reject("runtime schema audit receipt does not match the exact current writer token and qualification")

  private def tokenFor(
      mutation: MySqlWriterFenceMutation,
      qualification: VerifiedSchemaQualification,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): MySqlWriterFenceToken =
    val fence = MySqlWriterFenceExpectation.fromAcquiredFence(
      expectation.bindingUuid,
      bootUuid,
      mutation.row.epoch,
      mutation.row.leaseUntil,
      mutation.databaseNow,
      mutation.monotonicAnchorNanos
    )
    new MySqlWriterFenceToken(verified, scope, qualification, fence)

  private def requireAwaiting(token: MySqlWriterFenceToken): Unit =
    ensureNotFrozen()
    phase match
      case MySqlWriterFenceAwaitingAudit(current) if current eq token => ()
      case _ => MySqlSchemaAuditor.reject("post-acquire audit token is stale or no longer current")

  private def requireCurrentToken(
      token: MySqlWriterFenceToken,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Unit =
    phase match
      case MySqlWriterFenceAwaitingAudit(current) if current eq token => validateToken(token, verified, scope)
      case MySqlWriterFenceAdmitted(admission) if admission.token eq token => validateToken(token, verified, scope)
      case MySqlWriterFenceRenewing(current, _) if current eq token => validateToken(token, verified, scope)
      case _ => MySqlSchemaAuditor.reject("MySQL writer fence token is stale, released, or no longer current")

  private def ensureUnclaimed(): Unit =
    ensureNotFrozen()
    phase match
      case MySqlWriterFenceUnclaimed => ()
      case _ => MySqlSchemaAuditor.reject("MySQL writer fence coordinator is already claimed or frozen")

  private def ensureNotFrozen(): Unit =
    if dbFirstRecallRefusalResponsibility != null || dbFirstRecallJournalResponsibility != null || dbFirstRecallResponsibility != null || dbFirstCheckpointResponsibility != null || dbFirstInitialActivationResponsibility != null || dbFirstInitialPreparationResponsibility != null then
      MySqlSchemaAuditor.reject("MySQL writer is held by its exact schema6 recall transaction/cleanup responsibility")
    if modeWorkflowResponsibility.nonEmpty || driftWorkflowResponsibility.nonEmpty || controlledDebugWorkflowResponsibility.nonEmpty || normalRecallWorkflowResponsibility.nonEmpty || inactiveDomainWorkflowResponsibility.nonEmpty then
      MySqlSchemaAuditor.reject("MySQL writer is blocked by an exact retained format3 mode workflow obligation")
    if certifiedOrdinaryResponsibility.nonEmpty then
      certifiedOrdinaryResponsibility.get match
        case AwaitingHandoffAudit(_) =>
          MySqlSchemaAuditor.reject("MySQL writer is blocked while the dedicated ordinary HANDOFF audit path is pending")
        case AwaitingNativeHandoff(_) =>
          MySqlSchemaAuditor.reject("MySQL writer is blocked while the dedicated ordinary native HANDOFF continuation is pending")
        case HandoffFaulted(_, cause) =>
          MySqlSchemaAuditor.reject("MySQL writer is frozen after the ordinary HANDOFF responsibility failed", cause)
    phase match
      case MySqlWriterFenceRetiredRecall=>
        MySqlSchemaAuditor.reject("original MySQL writer coordinator was permanently retired under actual recall fault custody")
      case _: MySqlWriterFenceUnknown | _: MySqlWriterFenceBusinessUnknown =>
        MySqlSchemaAuditor.reject("MySQL writer fence coordinator is frozen after an unknown or faulted operation")
      case _ => ()

  private def requireOrdinaryPhase(actor: MySqlCertifiedRecoveryInspectionActor, awaitingNative: Boolean): Unit =
    if actor == null then MySqlSchemaAuditor.reject("exact registered ordinary actor is required")
    val allowed = certifiedOrdinaryResponsibility.exists {
      case AwaitingHandoffAudit(current) => !awaitingNative && (current eq actor)
      case AwaitingNativeHandoff(current) => awaitingNative && (current eq actor)
      case HandoffFaulted(_, cause) => MySqlSchemaAuditor.reject("ordinary HANDOFF is faulted and cannot resume", cause)
    }
    if !allowed then MySqlSchemaAuditor.reject("ordinary HANDOFF phase/actor does not match the exact continuation")

  private def leaseDeadline(databaseNow: LocalDateTime): LocalDateTime =
    val result =
      try databaseNow.plusNanos(leaseNanos)
      catch case failure: ArithmeticException => MySqlSchemaAuditor.reject("writer lease deadline overflows DATETIME", failure)
    if !result.isAfter(databaseNow) || result.getNano % 1000 != 0 then
      MySqlSchemaAuditor.reject("writer lease deadline is not a future DATETIME(6) value")
    result

  private def table(name: String): String =
    if !MySqlSchemaAuditor.SchemaIdentifier.matches(expectation.schema) then
      MySqlSchemaAuditor.reject("approved MySQL schema identifier is malformed")
    "`" + expectation.schema + "`.`" + name + "`"

  private def operationFailure(operation: MySqlWriterFenceOperation, cause: Throwable): IllegalStateException =
    new IllegalStateException("MySQL writer fence " + operation.toString.toLowerCase(java.util.Locale.ROOT) + " failed", cause)
  private def readInt(result: ResultSet, index: Int, label: String): Int =
    val value = result.getInt(index)
    if result.wasNull() then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
    value

  private def readLong(result: ResultSet, index: Int, label: String): Long =
    val value = result.getLong(index)
    if result.wasNull() then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
    value

  private def readText(result: ResultSet, index: Int, label: String): String =
    val value = result.getString(index)
    if value == null || result.wasNull() then MySqlSchemaAuditor.reject("MySQL " + label + " is unavailable")
    value

  private def readBytes(result: ResultSet, index: Int, label: String, length: Int): Array[Byte] =
    val value = result.getBytes(index)
    val wasNull = result.wasNull()
    if value == null then
      if wasNull then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
      else MySqlSchemaAuditor.reject("MySQL " + label + " could not be decoded")
    if wasNull || value.length != length then MySqlSchemaAuditor.reject("MySQL " + label + " has an invalid binary length")
    value.clone()

  private def readOptionalBytes(result: ResultSet, index: Int, label: String, length: Int): Option[Vector[Byte]] =
    val value = result.getBytes(index)
    val wasNull = result.wasNull()
    if value == null then
      if wasNull then None else MySqlSchemaAuditor.reject("MySQL " + label + " could not be decoded")
    else if wasNull || value.length != length then
      MySqlSchemaAuditor.reject("MySQL " + label + " has an invalid binary length")
    else Some(value.clone().toVector)

  private def readUuid(result: ResultSet, index: Int, label: String): UUID =
    MySqlSchemaAuditor.uuidFromBytes(readBytes(result, index, label, 16), label)

  private def readOptionalUuid(result: ResultSet, index: Int, label: String): Option[UUID] =
    readOptionalBytes(result, index, label, 16).map(bytes => MySqlSchemaAuditor.uuidFromBytes(bytes.toArray, label))

  private def readDateTime(result: ResultSet, index: Int, label: String): LocalDateTime =
    val value = result.getObject(index, classOf[LocalDateTime])
    val wasNull = result.wasNull()
    if value == null then
      if wasNull then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
      else MySqlSchemaAuditor.reject("MySQL " + label + " could not be decoded")
    if wasNull || value.getNano % 1000 != 0 then MySqlSchemaAuditor.reject("MySQL " + label + " is not a valid DATETIME(6)")
    value

  private def setOptionalUuid(statement: PreparedStatement, index: Int, value: Option[UUID]): Unit =
    value match
      case Some(uuid) => statement.setBytes(index, uuidBytes(uuid))
      case None => statement.setNull(index, Types.VARBINARY)

  private def setOptionalBytes(statement: PreparedStatement, index: Int, value: Option[Vector[Byte]]): Unit =
    value match
      case Some(bytes) => statement.setBytes(index, bytes.toArray)
      case None => statement.setNull(index, Types.VARBINARY)

  private def uuidBytes(uuid: UUID): Array[Byte] =
    ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .array()

  private def hexBytes(hex: String): Vector[Byte] =
    if hex == null || !MySqlSchemaAuditor.Sha256Hex.matches(hex) then
      MySqlSchemaAuditor.reject("approved MySQL SHA-256 is malformed")
    try HexFormat.of().parseHex(hex).toVector
    catch case failure: IllegalArgumentException => MySqlSchemaAuditor.reject("approved MySQL SHA-256 is malformed", failure)

  private def bytesHex(bytes: Vector[Byte]): String =
    HexFormat.of().formatHex(bytes.toArray)

  private def withPrepared[A](connection: Connection, sql: String)(body: PreparedStatement => A): A =
    var primary: Throwable = null
    val statement = connection.prepareStatement(sql)
    try body(statement)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(statement, primary)

  private def withResult[A](result: ResultSet)(body: ResultSet => A): A =
    var primary: Throwable = null
    try body(result)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(result, primary)

  private def closePreserving(resource: AutoCloseable, primary: Throwable): Unit =
    if primary == null then resource.close()
    else
      try resource.close()
      catch case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)
