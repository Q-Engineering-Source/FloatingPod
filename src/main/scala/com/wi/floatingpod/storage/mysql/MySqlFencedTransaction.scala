package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{OperatingModeChangeOutcome, PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import java.sql.{
  Connection,
  PreparedStatement,
  ResultSet,
  SQLDataException,
  SQLException,
  SQLNonTransientConnectionException,
  SQLTransactionRollbackException,
  Types
}
import java.io.DataOutputStream
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import scala.util.{Failure, Success, Try}
import MySqlModeOperationEffectCodec.{allocationBudget, relation, schema, connection, observationScope, maxAllowedPacketBytes}

private[mysql] final case class MySqlLockedManifest(
    singleton: Int,
    backend: String,
    formatVersion: Int,
    ddlSha256: Vector[Byte],
    provisioningUuid: UUID,
    bootstrapState: String,
    serverUuid: UUID,
    bindingUuid: UUID,
    importManifestSha256: Option[Vector[Byte]],
    importState: String,
    importOperationUuid: Option[UUID],
    importSnapshotSha256: Option[Vector[Byte]],
    importReceiptSha256: Option[Vector[Byte]],
    guardManifestSha256: Option[Vector[Byte]]
)

private[mysql] final case class MySqlLockedBinding(
    singleton: Int,
    bindingUuid: UUID,
    cutoverState: String,
    formatVersion: Int
)

private[mysql] final case class MySqlBusinessWriterRow(
    bindingUuid: UUID,
    writerEpoch: Long,
    ownerBootUuid: Option[UUID],
    leaseUntil: LocalDateTime,
    lastOperationUuid: Option[UUID],
    lastFingerprint: Option[Vector[Byte]],
    databaseNow: LocalDateTime
):
  require(bindingUuid != null && writerEpoch >= 0L, "writer row identity is malformed")
  require(ownerBootUuid != null && leaseUntil != null && databaseNow != null, "writer row lease is malformed")
  require(lastOperationUuid != null && lastFingerprint != null, "writer receipt option is malformed")
  require(lastOperationUuid.isDefined == lastFingerprint.isDefined, "writer receipt slot must be complete or empty")
  require(lastFingerprint.forall(_.size == 32), "writer receipt fingerprint must contain 32 bytes")

  def receipt: Option[MySqlCommitReceipt] =
    lastOperationUuid.map { operation =>
      val boot = ownerBootUuid.getOrElse(throw new IllegalStateException("owned writer boot is required for a receipt"))
      MySqlCommitReceipt(
        MySqlCommitReceiptSlot(boot, writerEpoch, bindingUuid, operation),
        lastFingerprint.get
      )
    }

/** 一个同连接、同事务内完成的 MySQL 业务写入结果。 */
enum MySqlFencedTransactionOutcome:
  case Committed
  case Replayed

private[mysql] enum MySqlInactiveDomainWorkflowPlanning:
  case Rejected(outcome: MySqlInactiveDomainUpdateRejection)
  case Execute(materialization: MySqlInactiveDomainWorkflowMaterialization)

private[mysql] enum MySqlDriftWorkflowPlanning:
  case Rejected(outcome: MySqlTrackedDriftRejection)
  case Execute(materialization: MySqlDriftWorkflowMaterialization)

private[mysql] enum MySqlTrackedDriftExecution:
  case Rejected(outcome: MySqlTrackedDriftRejection)
  case Committed(graph: MySqlCommittedTrackedDriftGraph)

private[mysql] enum MySqlModeWorkflowPlanning:
  case Rejected(outcome: com.wi.floatingpod.storage.OperatingModeChangeOutcome.Rejected)
  case AlreadySet(readback: MySqlOwnedModeAlreadySetReadback)
  case Execute(materialization: MySqlModeWorkflowMaterialization)

private[mysql] enum MySqlOwnedModeExecution:
  case Rejected(outcome: com.wi.floatingpod.storage.OperatingModeChangeOutcome.Rejected)
  case AlreadySet(readback: MySqlOwnedModeAlreadySetReadback)
  case Committed(graph: MySqlCommittedOwnedModeGraph)

private[mysql] enum MySqlControlledDebugExecution:
  case Prepared(graph: MySqlControlledDebugPreparedGraph)
  case Committed(graph: MySqlControlledDebugCommittedGraph)

private[mysql] final case class MySqlModeCriticalCleanupDecision(
    rollback: Boolean,
    restoreAutoCommit: Boolean,
    restoreIsolation: Boolean,
    connectionUncertain: Boolean
)

/** 只有 runner 已接管事务或空闲连接会话后，失败清理才能修改 JDBC 状态。 */
private[mysql] object MySqlModeCriticalCleanupPolicy:
  def decide(
      transactionAttempted: Boolean,
      idleAutocommitObserved: Boolean,
      rollbackAcknowledged: Boolean,
      autocommitRestored: Boolean,
      isolationChangeAttempted: Boolean,
      isolationRead: Boolean,
      cleanupFailed: Boolean
  ): MySqlModeCriticalCleanupDecision =
    val restoreAutoCommit = transactionAttempted && rollbackAcknowledged
    val restoreIsolation = isolationChangeAttempted && isolationRead &&
      (if transactionAttempted then rollbackAcknowledged && autocommitRestored else idleAutocommitObserved)
    MySqlModeCriticalCleanupDecision(
      rollback = transactionAttempted,
      restoreAutoCommit = restoreAutoCommit,
      restoreIsolation = restoreIsolation,
      connectionUncertain = cleanupFailed ||
        (transactionAttempted && (!rollbackAcknowledged || !autocommitRestored))
    )

final class MySqlCommittedButUnverifiedException(
    val operationContext: MySqlOperationContext,
    cause: Throwable
) extends SQLException("MySQL business commit was acknowledged but its committed state could not be verified", cause)

final class MySqlTransactionCleanupUncertainException(
    val operationContext: MySqlOperationContext,
    cause: Throwable
) extends SQLException("MySQL business transaction cleanup left the owned connection uncertain", cause)

private[mysql] final class MySqlPlanningReadCleanupUncertainException(cause: Throwable)
    extends SQLNonTransientConnectionException("MySQL planning read rollback or session restoration is uncertain", cause)

/** 真实 fenced transaction 中的 format3 full-snapshot read capability。构造入口只在 M/B/W 行已真实锁定并核验后。 */
object MySqlFencedTransaction:
  /** 仅实际 runner 经 commit ACK 与全20读回后构造；Main仍须核原request/plan/holder。 */
  private[floatingpod] final class NormalRecallCommitted private[MySqlFencedTransaction] (
      private[mysql] val materialization: MySqlNormalRecallMaterialization,
      private[mysql] val proof: NormalRecallPostCommitReadback,
      private[mysql] val actual: MySqlModeOperationEffectCodec.Image):
    val tag: IoOperationTag = materialization.plan.tag
    val request: MySqlNormalRecallRequest = materialization.header.request
    val checkpoint: Boolean = materialization.exactOperation.isInstanceOf[MySqlRepositoryOperation.CheckpointNormalRecall]
    private[mysql] def requireRetained(): Unit =
      if materialization.root.isClosed || actual.allocationBudget.isClosed ||
        (proof.materialization ne materialization) then
        throw new SQLNonTransientConnectionException("Normal Recall committed projection was revoked")

  /** Exact current parent evidence from the restricted schema5 Session; it carries no SQL lock or write authority. */
  private[mysql] final class RestrictedSourceParent5 private[MySqlFencedTransaction] (
      private[mysql] val issuer: MySqlFencedTransaction,
      private[mysql] val session: MySqlDeploymentReceiptSession,
      private[mysql] val proof: MySqlDeploymentReceiptSession.RestrictedRecall5,
      private[mysql] val publication: MySqlDeploymentReceiptSession.RestrictedPublication5,
      private[mysql] val readerLease: MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      private[mysql] val originalRoot: MySqlFormat3ReadBudget,
      private[mysql] val actualLedger: MySqlFormat3AllocationLedger,
      private[mysql] val link: MySqlOwnedIoAdapter.RecallRecoveryLink,
      private[mysql] val reference: MySqlNormalRecallRecoveryReference,
      private[mysql] val currentFull26: MySqlScriptSchemaSnapshot,
      private[mysql] val anchorFull26: MySqlScriptSchemaSnapshot,
      private[mysql] val reader: MySqlDeploymentReceiptReadOwner,
      private[mysql] val history: MySqlNormalRecallRecovery.ReceiptHistoryData,
      val binding: UUID,
      val parentRecallWorkflow: UUID,
      val parentP2Operation: UUID,
      val parentPod: UUID
  ):
    private[mysql] def requireRetained(): Unit = issuer.requireRetainedRestrictedSourceParent(this)

  /** 原始 root 的唯一 DATA 子账户及精确注册计划谱系；调用者不能提供替代预算。 */
  private[mysql] final class SourceReservePlanAllocation private[MySqlFencedTransaction] (
      private var issuerOwner: MySqlFencedTransaction,
      private var parentOwner: RestrictedSourceParent5,
      private var originalRootOwner: MySqlFormat3ReadBudget,
      private var actualLedgerOwner: MySqlFormat3AllocationLedger,
      private var workerOwner: Thread,
      private var intentOwner: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      private var childOwner: MySqlFormat3ReadBudget
  ):
    private var revoked = false
    private var childCloseAttempted = false
    private var childClosed = false
    private var childCloseFailure: Throwable = null

    /** 校验已登记的所有者，并只向同步 DATA builder 借出精确子账户。 */
    private[mysql] def withPlanDataBudget[A](
        exactParent: RestrictedSourceParent5,
        exactRoot: MySqlFormat3ReadBudget,
        exactLedger: MySqlFormat3AllocationLedger,
        exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent
    )(body: MySqlFormat3ReadBudget => A): A =
      if body == null then throw new IllegalArgumentException("source RESERVE plan DATA builder required")
      val (exactIssuer, exactChild) = this.synchronized {
        if issuerOwner == null || parentOwner == null || originalRootOwner == null || actualLedgerOwner == null ||
            workerOwner == null || intentOwner == null || childOwner == null || revoked || childCloseAttempted || childClosed then
          throw new SQLNonTransientConnectionException("source RESERVE plan allocation holder is closed or revoked")
        if (Thread.currentThread() ne workerOwner) then
          throw new SQLNonTransientConnectionException("original registered source RESERVE plan worker required")
        if (parentOwner ne exactParent) || (originalRootOwner ne exactRoot) ||
            (actualLedgerOwner ne exactLedger) || (intentOwner ne exactIntent) ||
            exactRoot.isClosed || exactRoot.isScopedReadBudget || childOwner.isClosed ||
            !childOwner.isCanonicalChildOf(exactRoot) || !childOwner.sharesAllocationLedger(exactRoot) then
          throw new SQLNonTransientConnectionException("exact original-root source RESERVE plan DATA child required")
        (issuerOwner, childOwner)
      }
      exactIssuer.requireSourceReservePlanAllocationForUse(this, exactParent, exactRoot, exactLedger, exactIntent)
      body(exactChild)

    /** 仅在原计划已settle后，向纯DATA读取借用仍登记的同一子账户。 */
    private[mysql] def withRetainedPlanDataBudget[A](
        exactParent: RestrictedSourceParent5,
        exactRoot: MySqlFormat3ReadBudget,
        exactLedger: MySqlFormat3AllocationLedger,
        exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
        exactData: MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData
    )(body: MySqlFormat3ReadBudget => A): A =
      if body == null then throw new IllegalArgumentException("retained source RESERVE plan DATA reader required")
      val (exactIssuer, exactChild) = this.synchronized {
        if issuerOwner == null || parentOwner == null || originalRootOwner == null || actualLedgerOwner == null ||
            workerOwner == null || intentOwner == null || childOwner == null || revoked || childCloseAttempted || childClosed then
          throw new SQLNonTransientConnectionException("retained source RESERVE plan allocation holder is closed or revoked")
        if (Thread.currentThread() ne workerOwner) then
          throw new SQLNonTransientConnectionException("original registered source RESERVE plan worker required")
        if exactData == null || (parentOwner ne exactParent) || (originalRootOwner ne exactRoot) ||
            (actualLedgerOwner ne exactLedger) || (intentOwner ne exactIntent) ||
            exactRoot.isClosed || exactRoot.isScopedReadBudget || childOwner.isClosed ||
            (childOwner.profile ne exactRoot.profile) || !childOwner.isCanonicalChildOf(exactRoot) ||
            !childOwner.sharesAllocationLedger(exactRoot) then
          throw new SQLNonTransientConnectionException("exact original-root retained source RESERVE DATA child required")
        (issuerOwner, childOwner)
      }
      exactIssuer.requireRetainedSourceReservePlanAllocationForUse(
        this, exactParent, exactRoot, exactLedger, exactIntent, exactData)
      body(exactChild)

    /** 仅由精确注册的 active R0 scope 在原 RR 事务中借用同一 DATA 子账户。 */
    private[mysql] def withActiveRestrictedSourceR0DataBudget[A](
        exactPlan:MySqlFencedTransaction.SourceReservePlan5,
        freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5
    )(body:MySqlFormat3ReadBudget=>A):A =
      if body==null then throw new IllegalArgumentException("active restricted SOURCE R0 DATA consumer required")
      var exactParent:RestrictedSourceParent5=null
      var exactRoot:MySqlFormat3ReadBudget=null
      var exactLedger:MySqlFormat3AllocationLedger=null
      var exactIntent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent=null
      var exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData=null
      var exactIssuer:MySqlFencedTransaction=null
      val exactChild=this.synchronized {
        if issuerOwner==null || parentOwner==null || originalRootOwner==null || actualLedgerOwner==null ||
            workerOwner==null || intentOwner==null || childOwner==null || revoked ||
            childCloseAttempted || childClosed then
          throw new SQLNonTransientConnectionException("active restricted SOURCE R0 DATA holder is closed or revoked")
        if (Thread.currentThread() ne workerOwner) || exactPlan==null || freshScope==null ||
            (exactPlan.issuer ne issuerOwner) || (exactPlan.parent ne parentOwner) ||
            (exactPlan.exactRoot ne originalRootOwner) || (exactPlan.actualLedger ne actualLedgerOwner) ||
            (exactPlan.intent ne intentOwner) || (exactPlan.allocation ne this) || !exactPlan.isIssued ||
            exactPlan.exactRoot.isClosed || exactPlan.exactRoot.isScopedReadBudget ||
            childOwner.isClosed || (childOwner.profile ne exactPlan.exactRoot.profile) ||
            !childOwner.isCanonicalChildOf(exactPlan.exactRoot) || !childOwner.sharesAllocationLedger(exactPlan.exactRoot) ||
            (freshScope.retainedDataBudget ne childOwner) then
          throw new SQLNonTransientConnectionException("active restricted SOURCE R0 scope must borrow this exact registered plan DATA child")
        exactParent=parentOwner
        exactRoot=originalRootOwner
        exactLedger=actualLedgerOwner
        exactIntent=intentOwner
        exactData=exactPlan.data
        exactIssuer=issuerOwner
        childOwner
      }
      exactIssuer.requireActiveRestrictedSourceReservePlanAllocationForUse(
        this,exactParent,exactRoot,exactLedger,exactIntent,exactData,exactPlan,freshScope,exactChild)
      body(exactChild)

    /** 仅在原 R0 已获真实 JDBC ACK、原写事务已完全离开后借用同一 DATA 子账户读回。 */
    private[mysql] def withAcknowledgedRestrictedSourceR0DataBudget[A](
        exactPlan:MySqlFencedTransaction.SourceReservePlan5,
        exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome
    )(body:MySqlFormat3ReadBudget=>A):A =
      if body==null then throw new IllegalArgumentException("acknowledged restricted SOURCE R0 readback DATA consumer required")
      var exactParent:RestrictedSourceParent5=null
      var exactRoot:MySqlFormat3ReadBudget=null
      var exactLedger:MySqlFormat3AllocationLedger=null
      var exactIntent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent=null
      var exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData=null
      var exactIssuer:MySqlFencedTransaction=null
      val exactChild=this.synchronized {
        if issuerOwner==null || parentOwner==null || originalRootOwner==null || actualLedgerOwner==null ||
            workerOwner==null || intentOwner==null || childOwner==null || revoked ||
            childCloseAttempted || childClosed then
          throw new SQLNonTransientConnectionException("acknowledged restricted SOURCE R0 DATA holder is closed or revoked")
        if (Thread.currentThread() ne workerOwner) || exactPlan==null || exactOutcome==null ||
            (exactPlan.issuer ne issuerOwner) || (exactPlan.parent ne parentOwner) ||
            (exactPlan.exactRoot ne originalRootOwner) || (exactPlan.actualLedger ne actualLedgerOwner) ||
            (exactPlan.intent ne intentOwner) || (exactPlan.allocation ne this) || !exactPlan.isIssued ||
            (exactOutcome.plan ne exactPlan) || exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitAcknowledged ||
            exactPlan.exactRoot.isClosed || exactPlan.exactRoot.isScopedReadBudget || childOwner.isClosed ||
            (childOwner.profile ne exactPlan.exactRoot.profile) || !childOwner.isCanonicalChildOf(exactPlan.exactRoot) ||
            !childOwner.sharesAllocationLedger(exactPlan.exactRoot) ||
            (exactOutcome.dataBudget ne childOwner) ||
            exactOutcome.postCommitReadbackOwner==null ||
            (exactOutcome.postCommitReadbackOwner.plan ne exactPlan) ||
            (exactOutcome.postCommitReadbackOwner.outcome ne exactOutcome) then
          throw new SQLNonTransientConnectionException("exact acknowledged restricted SOURCE R0 plan/outcome DATA child required")
        exactParent=parentOwner
        exactRoot=originalRootOwner
        exactLedger=actualLedgerOwner
        exactIntent=intentOwner
        exactData=exactPlan.data
        exactIssuer=issuerOwner
        childOwner
      }
      exactIssuer.requireAcknowledgedRestrictedSourceReservePlanAllocationForUse(
        this,exactParent,exactRoot,exactLedger,exactIntent,exactData,exactPlan,exactOutcome,exactChild)
      exactOutcome.postCommitReadbackOwner.session.requireAcknowledgedRestrictedSourceR0Readback(
        exactPlan,exactOutcome,exactIssuer,exactOutcome.postCommitReadbackOwner)
      body(exactChild)

    /** UNKNOWN inspection borrows the original DATA child without reopening parent/current business authority. */
    private[mysql] def withUnknownInspectionR0DataBudget[A](
        exactPlan:MySqlFencedTransaction.SourceReservePlan5,
        exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
        exactOwner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5
    )(body:MySqlFormat3ReadBudget=>A):A =
      if body==null then throw new IllegalArgumentException("restricted SOURCE R0 UNKNOWN DATA consumer required")
      var exactParent:RestrictedSourceParent5=null
      var exactRoot:MySqlFormat3ReadBudget=null
      var exactLedger:MySqlFormat3AllocationLedger=null
      var exactIntent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent=null
      var exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData=null
      var exactIssuer:MySqlFencedTransaction=null
      val exactChild=this.synchronized {
        if issuerOwner==null || parentOwner==null || originalRootOwner==null || actualLedgerOwner==null ||
            workerOwner==null || intentOwner==null || childOwner==null || revoked || childCloseAttempted || childClosed then
          throw new SQLNonTransientConnectionException("retained restricted SOURCE R0 UNKNOWN DATA holder is closed or revoked")
        if (Thread.currentThread() ne workerOwner) || exactPlan==null || exactOutcome==null || exactOwner==null ||
            (exactPlan.issuer ne issuerOwner) || (exactPlan.parent ne parentOwner) ||
            (exactPlan.exactRoot ne originalRootOwner) || (exactPlan.actualLedger ne actualLedgerOwner) ||
            (exactPlan.intent ne intentOwner) || (exactPlan.allocation ne this) || !exactPlan.isIssued ||
            (exactOutcome.plan ne exactPlan) ||
            exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitUnknown ||
            (exactOutcome.unknownInspectionOwner ne exactOwner) || (exactOwner.plan ne exactPlan) ||
            (exactOwner.outcome ne exactOutcome) || (exactOwner.dataBudget ne childOwner) ||
            (exactOwner.root ne exactPlan.exactRoot) || (exactOwner.ledger ne exactPlan.actualLedger) ||
            (exactOwner.session ne parentOwner.session) || exactPlan.exactRoot.isClosed ||
            exactPlan.exactRoot.isScopedReadBudget || childOwner.isClosed ||
            (childOwner.profile ne exactPlan.exactRoot.profile) ||
            !childOwner.isCanonicalChildOf(exactPlan.exactRoot) ||
            !childOwner.sharesAllocationLedger(exactPlan.exactRoot) ||
            exactOwner.auditAccount==null || exactOwner.auditAccount.isClosed ||
            !exactOwner.auditAccount.isCanonicalComparisonChildOf(exactPlan.exactRoot) ||
            !exactOwner.auditAccount.sharesAllocationLedger(exactPlan.exactRoot) then
          throw new SQLNonTransientConnectionException("exact retained restricted SOURCE R0 UNKNOWN plan/outcome/owner DATA child required")
        exactParent=parentOwner
        exactRoot=originalRootOwner
        exactLedger=actualLedgerOwner
        exactIntent=intentOwner
        exactData=exactPlan.data
        exactIssuer=issuerOwner
        childOwner
      }
      exactIssuer.requireUnknownInspectionRestrictedSourceReservePlanAllocationForUse(
        this,exactParent,exactRoot,exactLedger,exactIntent,exactData,exactPlan,exactOutcome,exactOwner,exactChild)
      body(exactChild)

    private[mysql] def requireDropBinding(exactIssuer: MySqlFencedTransaction,
        exactParent: RestrictedSourceParent5, exactRoot: MySqlFormat3ReadBudget,
        exactLedger: MySqlFormat3AllocationLedger, exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
        exactWorker: Thread): Unit = this.synchronized {
      if (issuerOwner ne exactIssuer) || (parentOwner ne exactParent) ||
          (originalRootOwner ne exactRoot) || (actualLedgerOwner ne exactLedger) ||
          (intentOwner ne exactIntent) || (workerOwner ne exactWorker) || childOwner == null ||
          revoked || childClosed || exactRoot == null || exactRoot.isClosed || exactRoot.isScopedReadBudget ||
          childOwner.isClosed || !childOwner.isCanonicalChildOf(exactRoot) ||
          !childOwner.sharesAllocationLedger(exactRoot) then
        throw new SQLNonTransientConnectionException("exact live source RESERVE plan allocation holder required")
    }

    private[mysql] def childForReceipt(exactIssuer: MySqlFencedTransaction,
        exactParent: RestrictedSourceParent5, exactRoot: MySqlFormat3ReadBudget,
        exactLedger: MySqlFormat3AllocationLedger, exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
        exactWorker: Thread): MySqlFormat3ReadBudget = this.synchronized {
      requireDropBinding(exactIssuer, exactParent, exactRoot, exactLedger, exactIntent, exactWorker)
      childOwner
    }

    private[mysql] def markRevoked(exactReceipt: SourceReservePlanDropReceipt): Unit = this.synchronized {
      if exactReceipt == null || (exactReceipt.holderOwner ne this) || childOwner == null || childClosed then
        throw new SQLNonTransientConnectionException("exact source RESERVE plan drop receipt required to revoke its DATA child")
      if revoked then return
      revoked = true
    }

    private[mysql] def closeChildForDrop(exactReceipt: SourceReservePlanDropReceipt): Unit = this.synchronized {
      if exactReceipt == null || (exactReceipt.holderOwner ne this) || !revoked || childOwner == null ||
          !exactReceipt.planRevoked || !exactReceipt.graphDetached || !exactReceipt.readerRetired then
        throw new SQLNonTransientConnectionException("revoked exact source RESERVE allocation holder required for child close")
      if childClosed then return
      if childCloseFailure != null then throw childCloseFailure
      if childCloseAttempted then
        throw new SQLNonTransientConnectionException("source RESERVE child close remains unresolved", childCloseFailure)
      childCloseAttempted = true
      try
        childOwner.closeScopedReadBudget()
        childClosed = true
      catch
        case cause: Throwable =>
          childCloseFailure = cause
          throw cause
    }

    private[mysql] def clearAfterDrop(exactReceipt: SourceReservePlanDropReceipt): Unit = this.synchronized {
      if exactReceipt == null || (exactReceipt.holderOwner ne this) || !revoked || !childClosed ||
          !exactReceipt.planRevoked || !exactReceipt.graphDetached || !exactReceipt.readerRetired then
        throw new SQLNonTransientConnectionException("successfully closed exact source RESERVE child required before holder detach")
      issuerOwner = null
      parentOwner = null
      originalRootOwner = null
      actualLedgerOwner = null
      workerOwner = null
      intentOwner = null
      childOwner = null
    }

    private[mysql] def isOpen: Boolean = this.synchronized {
      issuerOwner != null && parentOwner != null && originalRootOwner != null && actualLedgerOwner != null &&
        workerOwner != null && intentOwner != null && childOwner != null && !revoked && !childClosed && !childOwner.isClosed
    }

  /** Main/Adapter 为一次精确计划所有者退休签发的不透明能力。 */
  private[floatingpod] final class SourceReservePlanDropReceipt private[MySqlFencedTransaction] (
      private[MySqlFencedTransaction] var issuerOwner: MySqlFencedTransaction,
      private[MySqlFencedTransaction] var planOwner: SourceReservePlan5,
      private[MySqlFencedTransaction] var holderOwner: SourceReservePlanAllocation,
      private[MySqlFencedTransaction] var parentOwner: RestrictedSourceParent5,
      private[MySqlFencedTransaction] var intentOwner: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      private[MySqlFencedTransaction] var originalRootOwner: MySqlFormat3ReadBudget,
      private[MySqlFencedTransaction] var actualLedgerOwner: MySqlFormat3AllocationLedger,
      private[MySqlFencedTransaction] var childOwner: MySqlFormat3ReadBudget,
      private[MySqlFencedTransaction] var readerLeaseOwner: MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      private[MySqlFencedTransaction] var readerOwner: MySqlDeploymentReceiptReadOwner,
      private[MySqlFencedTransaction] var adapterOwner: MySqlOwnedIoAdapter,
      private[MySqlFencedTransaction] var registrationOwner: MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration,
      private[MySqlFencedTransaction] var viewsRevokedOwner: com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked,
      private[MySqlFencedTransaction] val readTag: IoOperationTag,
      private[MySqlFencedTransaction] var planRevoked: Boolean = false,
      private[MySqlFencedTransaction] var graphDetached: Boolean = false,
      private[MySqlFencedTransaction] var readerRetired: Boolean = false,
      private[MySqlFencedTransaction] var childClosed: Boolean = false,
      private[MySqlFencedTransaction] var metadataDetached: Boolean = false,
      private[MySqlFencedTransaction] var irreversibleStarted: Boolean = false,
      private[MySqlFencedTransaction] var originalFailure: Throwable = null,
      private[MySqlFencedTransaction] var completedAck: SourceReservePlanDropAck = null,
      private[MySqlFencedTransaction] var running: Boolean = false
  ):
    private[floatingpod] def requireFor(exactPlan: SourceReservePlan5): Unit =
      if exactPlan == null || (planOwner ne exactPlan) || issuerOwner == null || holderOwner == null ||
          parentOwner == null || intentOwner == null || originalRootOwner == null || actualLedgerOwner == null ||
          childOwner == null || readerLeaseOwner == null || readerOwner == null || adapterOwner == null ||
          registrationOwner == null || viewsRevokedOwner == null || readTag == null ||
          (exactPlan.receiptForReplay ne this) || (exactPlan.allocation ne holderOwner) ||
          (exactPlan.parent ne parentOwner) || (exactPlan.intent ne intentOwner) ||
          (exactPlan.exactRoot ne originalRootOwner) || (exactPlan.actualLedger ne actualLedgerOwner) ||
          (exactPlan.actualReader ne readerOwner) then
        throw new SQLNonTransientConnectionException("exact issued source RESERVE drop receipt required")

    /** 真实 worker ACK 后释放大型 owner 引用；ACK 与轻量 tombstone 仍绑定本 receipt。 */
    private[MySqlFencedTransaction] def clearPayloadAfterAck(): Unit = synchronized {
      if completedAck == null || !planRevoked || !graphDetached || !readerRetired || !childClosed || !metadataDetached then
        throw new IllegalStateException("completed source RESERVE plan drop required before receipt payload detach")
      holderOwner = null
      parentOwner = null
      intentOwner = null
      originalRootOwner = null
      actualLedgerOwner = null
      childOwner = null
      readerLeaseOwner = null
      readerOwner = null
      adapterOwner = null
      registrationOwner = null
      viewsRevokedOwner = null
    }

  /** 证明原 worker 已完成此计划所有者的每个退休步骤。 */
  private[floatingpod] final class SourceReservePlanDropAck private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val exactReceipt: SourceReservePlanDropReceipt
  ):
    private[floatingpod] def requireFor(actualReceipt: SourceReservePlanDropReceipt): Unit =
      if issuer == null || exactReceipt == null || (actualReceipt ne exactReceipt) ||
          (exactReceipt.issuerOwner ne issuer) || (exactReceipt.completedAck ne this) ||
          exactReceipt.planOwner == null || (exactReceipt.planOwner.receiptForReplay ne exactReceipt) ||
          !exactReceipt.planRevoked || !exactReceipt.graphDetached || !exactReceipt.readerRetired ||
          !exactReceipt.childClosed || !exactReceipt.metadataDetached || exactReceipt.originalFailure != null then
        throw new IllegalStateException("exact completed source RESERVE plan drop ACK required")

  /** Main-visible closure DATA; it deliberately exposes no Full26 rows, SQL roots, or witness handles. */
  private[floatingpod] final class SourceReserveEffectClosure5 private[MySqlFencedTransaction] (
      initialIssuer: MySqlFencedTransaction,
      initialPlan: SourceReservePlan5,
      initialRetainedRoot: MySqlFormat3ReadBudget,
      initialExactProfile: SourceLimitProfile,
      initialData: MySqlNormalRecallSourceWorkflow.SourceChildReserveEffectClosureData
  ):
    private var issuerOwner = initialIssuer
    private var planOwner = initialPlan
    private var retainedRootOwner = initialRetainedRoot
    private var exactProfileOwner = initialExactProfile
    private var dataOwner = initialData
    require(issuerOwner != null && planOwner != null && retainedRootOwner != null &&
      exactProfileOwner != null && dataOwner != null,
      "retained source RESERVE effect-closure DATA and exact issuer lineage required")
    private[mysql] def issuer: MySqlFencedTransaction = issuerOwner
    private[mysql] def plan: SourceReservePlan5 = planOwner
    private[mysql] def retainedRoot: MySqlFormat3ReadBudget = retainedRootOwner
    private[mysql] def exactProfile: SourceLimitProfile = exactProfileOwner
    private[mysql] def data: MySqlNormalRecallSourceWorkflow.SourceChildReserveEffectClosureData = dataOwner
    private def requireAvailable(): Unit =
      if issuerOwner == null || planOwner == null || retainedRootOwner == null ||
          exactProfileOwner == null || dataOwner == null || !planOwner.isIssued || retainedRootOwner.isClosed then
        throw new SQLNonTransientConnectionException("source RESERVE effect closure is not retained")
    private[mysql] def requireRetained(): Unit = { requireAvailable(); plan.requireRetained() }
    private[floatingpod] def belongsTo(exactPlan: SourceReservePlan5, exactIssuer: MySqlFencedTransaction,
        exactRoot: MySqlFormat3ReadBudget, profile: SourceLimitProfile): Boolean =
      if exactPlan == null || exactIssuer == null || exactRoot == null || profile == null ||
          (planOwner ne exactPlan) || (issuerOwner ne exactIssuer) || (retainedRootOwner ne exactRoot) ||
          (exactProfileOwner ne profile) then
        false
      else
        requireAvailable()
        true
    private[floatingpod] def globalUnresolved: Boolean = { requireAvailable(); data.globalUnresolved }
    private[floatingpod] def parentPod: UUID = { requireAvailable(); data.parent.pod }
    private[floatingpod] def parentRecallWorkflow: UUID = { requireAvailable(); data.parent.recallWorkflow }
    private[floatingpod] def parentPrepare: UUID = { requireAvailable(); data.parent.prepare }
    private[floatingpod] def parentCheckpoint: Option[UUID] = { requireAvailable(); data.parent.checkpoint }
    private[floatingpod] def parentP2: UUID = { requireAvailable(); data.parent.parentP2 }
    private[floatingpod] def parentPodStateVersion: Long = { requireAvailable(); data.parent.podStateVersion }
    private[floatingpod] def parentP2Step: String = { requireAvailable(); data.parent.p2Step }
    private[floatingpod] def parentP2State: String = { requireAvailable(); data.parent.p2State }
    private[floatingpod] def parentPodReservationRevision: Long = { requireAvailable(); data.parent.podReservationRevision }
    private[floatingpod] def sourceUnitCount: Int = { requireAvailable(); data.sourceUnits.size }
    private[floatingpod] def sourceUnitBinding(index: Int): UUID = { requireAvailable(); data.sourceUnits(index).binding }
    private[floatingpod] def sourceUnitId(index: Int): UUID = { requireAvailable(); data.sourceUnits(index).unit }
    private[floatingpod] def sourceUnitWorldIdentity(index: Int): String = { requireAvailable(); data.sourceUnits(index).worldIdentity }
    private[floatingpod] def sourceUnitSaveIdentity(index: Int): String = { requireAvailable(); data.sourceUnits(index).saveIdentity }
    private[floatingpod] def sourceUnitLoaderIdentity(index: Int): String = { requireAvailable(); data.sourceUnits(index).loaderIdentity }
    private[floatingpod] def sourceUnitCatalogGeneration(index: Int): UUID = { requireAvailable(); data.sourceUnits(index).catalogGeneration }
    private[floatingpod] def sourceUnitReadGeneration(index: Int): Long = { requireAvailable(); data.sourceUnits(index).readGeneration }
    private[floatingpod] def sourceUnitRevision(index: Int): Long = { requireAvailable(); data.sourceUnits(index).unitRevision }
    private[floatingpod] def sourceUnitState(index: Int): String = { requireAvailable(); data.sourceUnits(index).state }
    private[floatingpod] def sourceUnitCurrentEpoch(index: Int): Option[Long] = { requireAvailable(); data.sourceUnits(index).currentEpoch }
    private[floatingpod] def sourceUnitCurrentForest(index: Int): Option[UUID] = { requireAvailable(); data.sourceUnits(index).currentForest }
    private[floatingpod] def carrierCount: Int = { requireAvailable(); data.carriers.size }
    private[floatingpod] def carrierBinding(index: Int): UUID = { requireAvailable(); data.carriers(index).carrier.binding }
    private[floatingpod] def carrierUnit(index: Int): UUID = { requireAvailable(); data.carriers(index).carrier.unit }
    private[floatingpod] def carrierForest(index: Int): UUID = { requireAvailable(); data.carriers(index).carrier.forest }
    private[floatingpod] def carrierWorldIdentity(index: Int): String = { requireAvailable(); data.carriers(index).carrier.worldIdentity }
    private[floatingpod] def carrierSaveIdentity(index: Int): String = { requireAvailable(); data.carriers(index).carrier.saveIdentity }
    private[floatingpod] def carrierLoaderIdentity(index: Int): String = { requireAvailable(); data.carriers(index).carrier.loaderIdentity }
    private[floatingpod] def carrierRegionIdentity(index: Int): String = { requireAvailable(); data.carriers(index).carrier.regionIdentity }
    private[floatingpod] def carrierDimension(index: Int): Int = { requireAvailable(); data.carriers(index).carrier.chunk.dimension }
    private[floatingpod] def carrierChunkX(index: Int): Int = { requireAvailable(); data.carriers(index).carrier.chunk.x }
    private[floatingpod] def carrierChunkZ(index: Int): Int = { requireAvailable(); data.carriers(index).carrier.chunk.z }
    private[floatingpod] def carrierGeneration(index: Int): Long = { requireAvailable(); data.carriers(index).carrier.generation }
    private[floatingpod] def carrierStatus(index: Int): String = { requireAvailable(); data.carriers(index).status }
    private[floatingpod] def carrierLocalReasonCount(index: Int): Int = { requireAvailable(); data.carriers(index).localReasons.size }
    private[floatingpod] def carrierLocalReason(index: Int, reason: Int): String =
      { requireAvailable(); data.carriers(index).localReasons(reason) }
    private[floatingpod] def footprintChunkCount: Int = { requireAvailable(); data.oldFootprintChunks.size }
    private[floatingpod] def footprintBinding(index: Int): UUID = { requireAvailable(); data.oldFootprintChunks(index).binding }
    private[floatingpod] def footprintPod(index: Int): UUID = { requireAvailable(); data.oldFootprintChunks(index).pod }
    private[floatingpod] def footprintAttempt(index: Int): UUID = { requireAvailable(); data.oldFootprintChunks(index).attempt }
    private[floatingpod] def footprintId(index: Int): UUID = { requireAvailable(); data.oldFootprintChunks(index).footprint }
    private[floatingpod] def footprintDimension(index: Int): Int = { requireAvailable(); data.oldFootprintChunks(index).dimension }
    private[floatingpod] def footprintChunkX(index: Int): Int = { requireAvailable(); data.oldFootprintChunks(index).chunkX }
    private[floatingpod] def footprintChunkZ(index: Int): Int = { requireAvailable(); data.oldFootprintChunks(index).chunkZ }
    private[floatingpod] def unresolvedCount: Int = { requireAvailable(); data.unresolved.size }
    private[floatingpod] def unresolvedCode(index: Int): String = { requireAvailable(); data.unresolved(index).code }
    private[floatingpod] def unresolvedTable(index: Int): String = { requireAvailable(); data.unresolved(index).table }
    private[floatingpod] def containsChunk(dimension: Int, x: Int, z: Int): Boolean =
      requireAvailable()
      data.containsChunk(dimension, x, z)
    private[floatingpod] def containsExactCarrier(locator: MySqlBootSourceProtectionIndex.Carrier): Boolean =
      requireAvailable()
      locator != null && data.containsCarrier(locator)

    private[floatingpod] def detachAfterDrop(exactPlan: SourceReservePlan5, exactReceipt: SourceReservePlanDropReceipt): Unit =
      if exactPlan == null || exactReceipt == null || (planOwner ne exactPlan) ||
          (exactReceipt.planOwner ne exactPlan) || !exactReceipt.planRevoked then
        throw new SQLNonTransientConnectionException("exact revoked source RESERVE closure required for detach")
      issuerOwner = null
      planOwner = null
      retainedRootOwner = null
      exactProfileOwner = null
      dataOwner = null

  /** Private worker plan retained after the fresh read transaction has fully settled. */
  private[floatingpod] final class SourceReservePlan5 private[MySqlFencedTransaction] (
      initialIssuer: MySqlFencedTransaction,
      initialParent: RestrictedSourceParent5,
      initialIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      initialActualReader: MySqlDeploymentReceiptReadOwner,
      initialExactRoot: MySqlFormat3ReadBudget,
      initialActualLedger: MySqlFormat3AllocationLedger,
      initialAllocation: SourceReservePlanAllocation,
      initialData: MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData
  ):
    private var issuerOwner = initialIssuer
    private var parentOwner = initialParent
    private var intentOwner = initialIntent
    private var actualReaderOwner = initialActualReader
    private var exactRootOwner = initialExactRoot
    private var actualLedgerOwner = initialActualLedger
    private var allocationOwner = initialAllocation
    private var dataOwner = initialData
    private var dropReceiptOwner: SourceReservePlanDropReceipt = null
    require(issuerOwner != null && parentOwner != null && intentOwner != null && actualReaderOwner != null && exactRootOwner != null &&
      actualLedgerOwner != null && allocationOwner != null && dataOwner != null,
      "exact settled source-child read plan and parent lineage required")
    require((parentOwner.issuer eq issuerOwner) && (parentOwner.originalRoot eq exactRootOwner) &&
      (parentOwner.actualLedger eq actualLedgerOwner) && (intentOwner.exactLink eq parentOwner.link) &&
      (intentOwner.parentReference eq parentOwner.reference) && !exactRootOwner.isClosed &&
      !exactRootOwner.isScopedReadBudget,
      "source-child plan differs from its exact retained parent, root, or original link")
    require(allocationOwner.isOpen,
      "source-child plan requires its exact open original-root DATA holder")
    private var retainedEffectClosure: SourceReserveEffectClosure5 = null
    @volatile private var issued = false
    @volatile private var revoked = false
    private[mysql] def issuer: MySqlFencedTransaction = issuerOwner
    private[mysql] def parent: RestrictedSourceParent5 = parentOwner
    private[mysql] def intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent = intentOwner
    private[mysql] def actualReader: MySqlDeploymentReceiptReadOwner = actualReaderOwner
    private[mysql] def exactRoot: MySqlFormat3ReadBudget = exactRootOwner
    private[mysql] def actualLedger: MySqlFormat3AllocationLedger = actualLedgerOwner
    private[mysql] def allocation: SourceReservePlanAllocation = allocationOwner
    private[mysql] def data: MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData = dataOwner
    private[floatingpod] def isIssued: Boolean = issued && !revoked && issuerOwner != null && parentOwner != null &&
      intentOwner != null && actualReaderOwner != null && exactRootOwner != null && actualLedgerOwner != null &&
      allocationOwner != null && allocationOwner.isOpen && !exactRootOwner.isClosed
    private[mysql] def markIssued(): Unit =
      if issued || revoked || retainedEffectClosure == null || !allocationOwner.isOpen then
        throw new IllegalStateException("source RESERVE plan issue requires one attached live closure and DATA holder")
      issued = true
    private[mysql] def attachEffectClosure(actual: SourceReserveEffectClosure5): Unit =
      if actual == null || retainedEffectClosure != null || (actual.plan ne this) || (actual.issuer ne issuer) ||
          (actual.retainedRoot ne exactRoot) || (actual.exactProfile ne exactRoot.profile) || !allocationOwner.isOpen then
        throw new IllegalStateException("exact one-shot source RESERVE closure binding required")
      retainedEffectClosure = actual
    private[mysql] def exactEffectClosure: SourceReserveEffectClosure5 = retainedEffectClosure
    private[floatingpod] def effectClosure(): SourceReserveEffectClosure5 =
      if !isIssued then throw new SQLNonTransientConnectionException("source RESERVE effect closure is not issued")
      Option(retainedEffectClosure).getOrElse(throw new IllegalStateException("source RESERVE closure is not retained"))
    private[mysql] def exactRequest: MySqlNormalRecallCanonical.SourceChildReserveRequestData = requireData().request
    private[mysql] def exactBefore: MySqlScriptSchemaSnapshot = requireData().before
    private[mysql] def exactHistory: MySqlNormalRecallRecovery.ReceiptHistoryData = requireData().history
    private def requireData(): MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData =
      if !isIssued || dataOwner == null then throw new SQLNonTransientConnectionException("source RESERVE plan DATA is detached")
      dataOwner
    private[mysql] def requireRetained(): Unit =
      if !isIssued || issuerOwner == null then throw new SQLNonTransientConnectionException("source RESERVE plan is revoked or detached")
      issuerOwner.requireRetainedSourceReservePlan(this)
    private[floatingpod] def issueSourceReservePlanDropReceipt(
        exactAdapter: MySqlOwnedIoAdapter,
        exactRegistration: MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration,
        exactViewsRevoked: com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked
    ): SourceReservePlanDropReceipt =
      if issuerOwner == null then throw new SQLNonTransientConnectionException("source RESERVE plan is no longer retained")
      issuerOwner.issueSourceReservePlanDropReceipt(this, exactAdapter, exactRegistration, exactViewsRevoked)
    private[mysql] def receiptForReplay: SourceReservePlanDropReceipt = synchronized { dropReceiptOwner }
    private[mysql] def requireDropIssueState(exactIssuer: MySqlFencedTransaction): Unit = synchronized {
      if (exactIssuer == null || (issuerOwner ne exactIssuer) || !isIssued || parentOwner == null || intentOwner == null ||
          actualReaderOwner == null || exactRootOwner == null || actualLedgerOwner == null || allocationOwner == null ||
          dataOwner == null || retainedEffectClosure == null || (retainedEffectClosure.plan ne this) ||
          (retainedEffectClosure.issuer ne exactIssuer) || (retainedEffectClosure.retainedRoot ne exactRootOwner) ||
          (retainedEffectClosure.data ne dataOwner.effectClosure)) then
        throw new SQLNonTransientConnectionException("exact live issued source RESERVE plan required for owner drop")
    }
    private[mysql] def retainDropReceipt(actualReceipt: SourceReservePlanDropReceipt): Unit = synchronized {
      if actualReceipt == null || (actualReceipt.planOwner ne this) || dropReceiptOwner != null then
        throw new IllegalStateException("one exact source RESERVE plan drop receipt may be retained")
      dropReceiptOwner = actualReceipt
    }
    private[mysql] def markRevokedForDrop(exactReceipt: SourceReservePlanDropReceipt): Unit = synchronized {
      if exactReceipt == null || (exactReceipt.planOwner ne this) || !issued || revoked ||
          dataOwner == null || retainedEffectClosure == null then
        throw new SQLNonTransientConnectionException("exact active source RESERVE plan is required for revoke")
      revoked = true
    }
    private[mysql] def detachGraphForDrop(exactReceipt: SourceReservePlanDropReceipt): Unit = synchronized {
      if exactReceipt == null || (exactReceipt.planOwner ne this) || !revoked || dataOwner == null ||
          retainedEffectClosure == null then
        throw new SQLNonTransientConnectionException("revoked exact source RESERVE plan is required for DATA detach")
      retainedEffectClosure.detachAfterDrop(this, exactReceipt)
      retainedEffectClosure = null
      dataOwner = null
    }
    private[mysql] def detachOwnerAfterDrop(exactReceipt: SourceReservePlanDropReceipt): Unit = synchronized {
      if exactReceipt == null || (exactReceipt.planOwner ne this) || !revoked || dataOwner != null ||
          retainedEffectClosure != null || exactReceipt.childClosed != true || !exactReceipt.readerRetired then
        throw new SQLNonTransientConnectionException("fully retired source RESERVE plan required for owner detach")
      issuerOwner = null
      parentOwner = null
      intentOwner = null
      actualReaderOwner = null
      exactRootOwner = null
      actualLedgerOwner = null
      allocationOwner = null
      issued = false
    }

  private[mysql] def requireNormalRecallPostCommitRetainedTag(tag: Option[IoOperationTag]): Unit =
    requireModePostCommitRetainedTag(tag)
  private[mysql] enum NormalRecallWriteReadImage:
    case Before, Target

  /** Only the runner can issue completion after an acknowledged execute and exact fresh full20 read. */
  private[mysql] final class NormalRecallPostCommitReadback private[MySqlFencedTransaction] (
      val commandScope: MySqlRepositoryCommandScope,
      val operation: MySqlRepositoryOperation,
      val materialization: MySqlNormalRecallMaterialization,
      val snapshotSchema: String,
      val snapshotConnection: MySqlFormat3ConnectionObservation,
      val snapshotScope: MySqlFormat3ObservationScope,
      val snapshotCanonicalSha256Hex: String,
      private[mysql] val deploymentSource:Option[MySqlScriptSchemaSnapshot]=None
  ):
    require(commandScope != null && operation != null && materialization != null &&
      snapshotSchema != null && snapshotConnection != null && snapshotScope != null &&
      snapshotCanonicalSha256Hex != null && snapshotCanonicalSha256Hex.matches("[0-9A-F]{64}"),
      "complete immutable normal-recall post-commit readback proof is required")
    require(materialization.exactOperation == operation && (commandScope.operationPlan() eq materialization.exactPlan),
      "normal-recall post-commit proof differs from the fixed Main operation plan")

  private[MySqlFencedTransaction] def issueNormalRecallPostCommitReadback(
      issuer: MySqlFencedTransaction,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      fullSnapshot: MySqlFormat3FullSnapshot
  ): NormalRecallPostCommitReadback =
    if issuer == null || commandScope == null || operation == null || materialization == null || fullSnapshot == null ||
      (issuer.ownerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("normal-recall completion proof requires the actual owned F3 runner readback")
    materialization.requireBoundTo(commandScope, operation)
    materialization.requireObserved(MySqlOperationEffectPhase.Execute,fullSnapshot,true)
    val expectedScope = commandScope.exactAuditScope()
    if fullSnapshot.schema != issuer.expectation.schema ||
      fullSnapshot.connection != MySqlFormat3ConnectionObservation.from(issuer.verified.identity, "REPEATABLE-READ") ||
      fullSnapshot.observationScope != MySqlFormat3ObservationScope(expectedScope.bootUuid,
        expectedScope.bootGeneration, expectedScope.connectionGeneration) then
      throw new SQLNonTransientConnectionException("normal-recall completion readback belongs to a different F3 connection generation")
    fullSnapshot.allocationBudget.reserveRetainedMemory(1024L,"private normal recall readback proof")
    new NormalRecallPostCommitReadback(commandScope, operation, materialization, fullSnapshot.schema,
      fullSnapshot.connection, fullSnapshot.observationScope, fullSnapshot.canonicalSha256Hex)

  private[MySqlFencedTransaction] def issueNormalRecallDeploymentPostCommitReadback(issuer:MySqlFencedTransaction,
      commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlNormalRecallMaterialization,snapshot:MySqlScriptSchemaSnapshot,
      owner:NormalRecallWriteReadOwner):NormalRecallPostCommitReadback =
    if issuer==null || owner==null || (issuer.activeNormalRecallWriteReadOwner ne owner) ||
      Thread.currentThread()!=issuer.ownerThread then
      throw new SQLNonTransientConnectionException("Full26 recall proof requires original active fresh readback owner")
    owner.requirePostCommitExecuteReadback(commandScope,operation,materialization,snapshot)
    owner.originalPurposeRoot.reserveRetainedMemory(1024L,"actual Full26 recall post-commit proof")
    new NormalRecallPostCommitReadback(commandScope,operation,materialization,snapshot.schema,snapshot.connection,
      snapshot.observationScope,snapshot.canonicalSha256Hex,Some(snapshot))
  /** Exact same-transaction full20 reader owner issued after a normal-recall phase's M→B→W locks. */
  private[mysql] final class NormalRecallWriteReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operation: MySqlRepositoryOperation,
      private val materialization: MySqlNormalRecallMaterialization,
      private val workflowPhase: MySqlNormalRecallWorkflowPhase,
      private val effect: MySqlNormalRecallPhase,
      private val image: NormalRecallWriteReadImage,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ) extends DeploymentReceiptPurposeReadOwner5(issuer,verified,scope,materialization.exactPlan,gateOwner,rollbackOnly):
    override protected def requirePurposeCurrent():Unit =
      requireCurrent(verified,scope,expectation.schema,issuer.exactPlanningProfile,verified.connection)
    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (readerSchema != expectation.schema) ||
        (ownerThread ne Thread.currentThread()) || (scope.ownerThread ne ownerThread) ||
        (commandScope.exactAuditScope() ne scope) ||
        operation != materialization.exactOperation || workflowPhase == null || image == null ||
        (readerProfile ne issuer.exactPlanningProfile) then
        throw new SQLNonTransientConnectionException("normal-recall-write reader owner differs from the exact Main and verified session scope")

      issuer.requireActiveNormalRecallWriteReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
        throw new SQLNonTransientConnectionException("normal-recall-write reader requires the exact approved format3/physical2 inputs")
      if readerConnection.isClosed || readerConnection.getAutoCommit ||
        readerConnection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("normal-recall-write reader requires the active same-connection REPEATABLE READ transaction")

      val plan = materialization.exactPlan
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
        throw new SQLNonTransientConnectionException("normal-recall-write reader lost the exact held-session barrier")
      if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
        coordinator.requireNormalRecallWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
        throw new SQLNonTransientConnectionException("normal-recall-write reader lost the exact business admission")
      verified.readmitSessionBarrier(expectation, scope)
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      materialization.requireBoundTo(commandScope, operation)

    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null then throw new SQLDataException("normal-recall-write reader returned no complete full20 snapshot")
      val inputs = expectation.format3Inputs
        .getOrElse(throw new SQLNonTransientConnectionException("normal-recall-write snapshot has no exact format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, expectation.expectedPhysicalFormat.version)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)
      image match
        case NormalRecallWriteReadImage.Before => materialization.requireObserved(effect.phase,snapshot,false)
        case NormalRecallWriteReadImage.Target => materialization.requireObserved(effect.phase,snapshot,true)

    private[mysql] def requireRetainedRoot(root: MySqlFormat3ReadBudget): Unit =
      val plan=materialization.exactPlan
      val actual=plan.retainedHandle.getOrElse(
        throw new SQLNonTransientConnectionException("normal recall read has no actual Main reservation")).issuer.rootForOriginal(plan)
      if root==null || (root ne actual) || (root ne materialization.originalRoot) || root.isClosed then
        throw new SQLNonTransientConnectionException("normal recall read lost the exact original reserved root")

    private[mysql] def requireSnapshot(snapshot:MySqlScriptSchemaSnapshot):Unit =
      requirePurposeCurrent()
      val root=originalPurposeRoot
      requireDeploymentReceiptSession(issuer.deploymentReceiptSession,verified,scope,root)
      MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,snapshot.inputs,root)
      if snapshot.connection!=MySqlFormat3ConnectionObservation.from(verified.identity,"REPEATABLE-READ") ||
        snapshot.observationScope!=MySqlFormat3ObservationScope(scope.bootUuid,scope.bootGeneration,scope.connectionGeneration) then
        throw new SQLTransactionRollbackException("Full26 recall readback lost exact locked scope")
      MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expectation,root)
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(snapshot,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
      MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(snapshot,writer)
      materialization.requireObserved(effect.phase,snapshot,image==NormalRecallWriteReadImage.Target)

    private[MySqlFencedTransaction] def requirePostCommitExecuteReadback(actualCommand:MySqlRepositoryCommandScope,
        actualOperation:MySqlRepositoryOperation,actualMaterialization:MySqlNormalRecallMaterialization,
        snapshot:MySqlScriptSchemaSnapshot):Unit =
      if (actualCommand ne commandScope) || actualOperation!=operation ||
        (actualMaterialization ne materialization) || image!=NormalRecallWriteReadImage.Target ||
        workflowPhase!=MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback ||
        (effect ne materialization.execute) then
        throw new SQLNonTransientConnectionException("Full26 recall proof lost original execute readback phase")
      requireSnapshot(snapshot)
    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)

  private[MySqlFencedTransaction] def afterNormalRecallWriteLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      workflowPhase: MySqlNormalRecallWorkflowPhase,
      effect: MySqlNormalRecallPhase,
      image: NormalRecallWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): NormalRecallWriteReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operation == null || materialization == null || workflowPhase == null || effect == null || image == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("normal-recall-write reader owner requires the exact locked M/B/W and Main inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) || !scope.verifiedConnection.contains(verified) ||
      (scope.connectionIdentity ne verified.identity) || (issuer.connection ne verified.connection) ||
      (issuer.ownerThread ne ownerThread) || (issuer.expectation ne expectation) ||
      (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("normal-recall-write reader owner issuer differs from the actual owned connection scope")
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("normal-recall-write reader owner requires approved format3/physical2 inputs")
    materialization.requireBoundTo(commandScope, operation)
    if !materialization.effects.exists(_ eq effect) then
      throw new SQLNonTransientConnectionException("normal-recall-write reader effect is outside the exact two-phase materialization")
    val plan = materialization.exactPlan
    commandScope.requireCurrent(scope, plan)
    if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("normal-recall-write reader requires the exact retained session barrier")
    if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
      coordinator.requireNormalRecallWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
      throw new SQLNonTransientConnectionException("normal-recall-write reader requires the exact retained business admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new NormalRecallWriteReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operation, materialization, workflowPhase, effect, image, heldBarrier, admission, rollbackOnly, ownerThread, manifest, binding, writer)

  /** 仅实际 runner 在原 registry/issuer/admission 门后发行；回调退出立即失效。 */
  private[mysql] final class ControlledDebugContinuationProof private[MySqlFencedTransaction] (
      private val sourceRoot: MySqlFormat3ReadBudget,
      private val comparisonRoot: MySqlFormat3ReadBudget,
      private val original: MySqlControlledDebugWorkflowMaterialization,
      private val revalidate: () => Unit):
    private val thread = Thread.currentThread()
    private var active = true
    private[mysql] def requireSource(snapshot: MySqlFormat3FullSnapshot,
        expectedSource: MySqlFormat3ReadBudget, expectedComparison: MySqlFormat3ReadBudget): Unit =
      if !active || (Thread.currentThread() ne thread) || (expectedSource ne sourceRoot) ||
        (expectedComparison ne comparisonRoot) ||
        ((snapshot ne original.preimage) && (snapshot ne original.reserveTarget) && (snapshot ne original.executeTarget)) then
        throw new MySqlControlledDebugNoProofException("original lexical continuation/snapshot identity is unavailable")
      revalidate()
    private[mysql] def requireSource(snapshot:MySqlScriptSchemaSnapshot,
        expectedSource:MySqlFormat3ReadBudget,expectedComparison:MySqlFormat3ReadBudget):Unit =
      if !active || (Thread.currentThread() ne thread) || (expectedSource ne sourceRoot) ||
          (expectedComparison ne comparisonRoot) ||
          ((snapshot ne original.deploymentPreimage) && (snapshot ne original.deploymentReserveTarget) &&
            (snapshot ne original.deploymentExecuteTarget)) then
        throw new MySqlControlledDebugNoProofException("original Full26 lexical continuation/snapshot identity is unavailable")
      revalidate()
    private[MySqlFencedTransaction] def retire(): Unit = active = false

  /** DATA 来源门不以 same-ledger 推导旧/new workflow 权限。 */
  private[mysql] def requireControlledDebugSource(snapshot: MySqlFormat3FullSnapshot,
      sourceRoot: MySqlFormat3ReadBudget, comparisonRoot: MySqlFormat3ReadBudget,
      continuation: Option[ControlledDebugContinuationProof]): Unit =
    require(snapshot != null && sourceRoot != null && comparisonRoot != null && continuation != null &&
      !sourceRoot.isClosed && !sourceRoot.isScopedReadBudget && !comparisonRoot.isClosed && !comparisonRoot.isScopedReadBudget &&
      snapshot.allocationBudget != null && !snapshot.allocationBudget.isClosed &&
      (snapshot.limitProfile eq sourceRoot.profile) && (comparisonRoot.profile eq sourceRoot.profile) &&
      comparisonRoot.sharesAllocationLedger(sourceRoot) &&
      ((snapshot.allocationBudget eq sourceRoot) || snapshot.allocationBudget.isSnapshotReadChildOf(sourceRoot)),
      "exact original workflow/snapshot read source and comparison root required")
    if sourceRoot ne comparisonRoot then
      val issued=continuation.getOrElse(throw new MySqlControlledDebugNoProofException("cross-workflow comparison requires actual runner continuation"))
      if issued==null then throw new MySqlControlledDebugNoProofException("null continuation is not a runner capability")
      issued.requireSource(snapshot,sourceRoot,comparisonRoot)

  /** 生产 post-commit 路径使用此窄校验；None 表示不保留投影，Some(null)/null Option 均无效。 */
  private[mysql] def requireControlledDebugSource(snapshot:MySqlScriptSchemaSnapshot,
      sourceRoot:MySqlFormat3ReadBudget,comparisonRoot:MySqlFormat3ReadBudget,
      continuation:Option[ControlledDebugContinuationProof]):Unit =
    if snapshot==null || sourceRoot==null || comparisonRoot==null || comparisonRoot.isClosed || comparisonRoot.isScopedReadBudget ||
        continuation==null || (comparisonRoot.profile ne sourceRoot.profile) ||
        !comparisonRoot.sharesAllocationLedger(sourceRoot) then
      throw new MySqlControlledDebugNoProofException("exact original Full26 source/comparison roots required")
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,snapshot.inputs,sourceRoot)
    if sourceRoot ne comparisonRoot then
      continuation.getOrElse(throw new MySqlControlledDebugNoProofException("Full26 cross-workflow comparison requires actual runner continuation"))
        .requireSource(snapshot,sourceRoot,comparisonRoot)

  /** 生产 post-commit 路径使用此窄校验；None 表示不保留投影，Some(null)/null Option 均无效。 */
  private[mysql] def requireModePostCommitRetainedTag(retainedTag: Option[IoOperationTag]): Unit =
    if retainedTag == null then
      throw new IllegalArgumentException("post-commit retained tag option must not be null")
    retainedTag match
      case Some(tag) if tag == null =>
        throw new IllegalArgumentException("post-commit retained tag must not be null")
      case _ => ()

  private[mysql] enum ModeWriteReadImage:
    case Before, Target

  /** Only the runner can issue completion after an acknowledged execute and exact fresh full20 read. */
  private[mysql] final class ModePostCommitReadback private[MySqlFencedTransaction] (
      val commandScope: MySqlRepositoryCommandScope,
      val operation: MySqlRepositoryOperation,
      val materialization: MySqlModeWorkflowMaterialization,
      val snapshotSchema: String,
      val snapshotConnection: MySqlFormat3ConnectionObservation,
      val snapshotScope: MySqlFormat3ObservationScope,
      val snapshotCanonicalSha256Hex: String,
      private[mysql] val deploymentSource:Option[MySqlScriptSchemaSnapshot]=None
  ):
    require(commandScope != null && operation != null && materialization != null &&
      snapshotSchema != null && snapshotConnection != null && snapshotScope != null &&
      snapshotCanonicalSha256Hex != null && snapshotCanonicalSha256Hex.matches("[0-9a-f]{64}"),
      "complete immutable mode post-commit readback proof is required")
    require(materialization.exactOperation == operation && (commandScope.operationPlan() eq materialization.exactPlan),
      "mode post-commit proof differs from the fixed Main operation plan")

  private[MySqlFencedTransaction] def issueModePostCommitReadback(
      issuer: MySqlFencedTransaction,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      fullSnapshot: MySqlFormat3FullSnapshot
  ): ModePostCommitReadback =
    if issuer == null || commandScope == null || operation == null || materialization == null || fullSnapshot == null ||
      (issuer.ownerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("mode completion proof requires the actual owned F3 runner readback")
    materialization.requireBoundTo(commandScope, operation)
    materialization.effects(1).verifyReadback(fullSnapshot)
    val expectedScope = commandScope.exactAuditScope()
    if fullSnapshot.schema != issuer.expectation.schema ||
      fullSnapshot.connection != MySqlFormat3ConnectionObservation.from(issuer.verified.identity, "REPEATABLE-READ") ||
      fullSnapshot.observationScope != MySqlFormat3ObservationScope(expectedScope.bootUuid,
        expectedScope.bootGeneration, expectedScope.connectionGeneration) then
      throw new SQLNonTransientConnectionException("mode completion readback belongs to a different F3 connection generation")
    new ModePostCommitReadback(commandScope, operation, materialization, fullSnapshot.schema,
      fullSnapshot.connection, fullSnapshot.observationScope, fullSnapshot.canonicalSha256Hex)

  private[MySqlFencedTransaction] def issueModeDeploymentPostCommitReadback(issuer:MySqlFencedTransaction,
      commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlModeWorkflowMaterialization,snapshot:MySqlScriptSchemaSnapshot,
      owner:ModeWriteReadOwner):ModePostCommitReadback =
    if issuer==null || owner==null || (issuer.activeModeWriteReadOwner ne owner) ||
      Thread.currentThread()!=issuer.ownerThread then
      throw new SQLNonTransientConnectionException("Full26 mode proof requires original active fresh readback owner")
    owner.requirePostCommitExecuteReadback(commandScope,operation,materialization,snapshot)
    owner.originalPurposeRoot.reserveRetainedMemory(512L,"actual Full26 mode post-commit proof")
    new ModePostCommitReadback(commandScope,operation,materialization,snapshot.schema,snapshot.connection,
      snapshot.observationScope,snapshot.canonicalSha256Hex,Some(snapshot))

  /** Exact same-transaction full20 reader owner issued after a mode phase's M→B→W locks. */
  private[mysql] final class ModeWriteReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operation: MySqlRepositoryOperation,
      private val materialization: MySqlModeWorkflowMaterialization,
      private val workflowPhase: MySqlModeWorkflowPhase,
      private val effect: MySqlModeOperationEffect,
      private val image: ModeWriteReadImage,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ) extends DeploymentReceiptPurposeReadOwner5(issuer,verified,scope,materialization.exactPlan,gateOwner,rollbackOnly):
    override protected def requirePurposeCurrent():Unit =
      requireCurrent(verified,scope,expectation.schema,issuer.exactPlanningProfile,verified.connection)
    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (readerSchema != expectation.schema) ||
        (ownerThread ne Thread.currentThread()) || (scope.ownerThread ne ownerThread) ||
        (commandScope.exactAuditScope() ne scope) ||
        operation != materialization.exactOperation || workflowPhase == null || image == null ||
        (readerProfile ne issuer.exactPlanningProfile) then
        throw new SQLNonTransientConnectionException("mode-write reader owner differs from the exact Main and verified session scope")

      issuer.requireActiveModeWriteReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
        throw new SQLNonTransientConnectionException("mode-write reader requires the exact approved format3/physical2 inputs")
      if readerConnection.isClosed || readerConnection.getAutoCommit ||
        readerConnection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode-write reader requires the active same-connection REPEATABLE READ transaction")

      val plan = materialization.exactPlan
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact held-session barrier")
      if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
        coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact business admission")
      verified.readmitSessionBarrier(expectation, scope)
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      materialization.requireBoundTo(commandScope, operation)

    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null then throw new SQLDataException("mode-write reader returned no complete full20 snapshot")
      val inputs = expectation.format3Inputs
        .getOrElse(throw new SQLNonTransientConnectionException("mode-write snapshot has no exact format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, expectation.expectedPhysicalFormat.version)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)
      image match
        case ModeWriteReadImage.Before => effect.verifyPreimage(snapshot)
        case ModeWriteReadImage.Target => effect.verifyReadback(snapshot)

    private[mysql] def requireSnapshot(snapshot:MySqlScriptSchemaSnapshot):Unit =
      val root=originalPurposeRoot
      requireDeploymentReceiptSession(issuer.deploymentReceiptSession,verified,scope,root)
      MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,snapshot.inputs,root)
      if snapshot.connection!=MySqlFormat3ConnectionObservation.from(verified.identity,"REPEATABLE-READ") ||
        snapshot.observationScope!=MySqlFormat3ObservationScope(scope.bootUuid,scope.bootGeneration,scope.connectionGeneration) then
        throw new SQLTransactionRollbackException("Full26 mode readback lost exact locked scope")
      MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expectation,root)
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(snapshot,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
      MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(snapshot,writer)
      image match
        case ModeWriteReadImage.Before=>effect.verifyDeploymentReceiptPreimage(snapshot,root)
        case ModeWriteReadImage.Target=>effect.verifyDeploymentReceiptReadback(snapshot,root)

    private[MySqlFencedTransaction] def requirePostCommitExecuteReadback(actualCommand:MySqlRepositoryCommandScope,
        actualOperation:MySqlRepositoryOperation,actualMaterialization:MySqlModeWorkflowMaterialization,
        snapshot:MySqlScriptSchemaSnapshot):Unit =
      if (actualCommand ne commandScope) || actualOperation!=operation ||
        (actualMaterialization ne materialization) || image!=ModeWriteReadImage.Target ||
        workflowPhase!=MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback ||
        (effect ne materialization.effects(1)) then
        throw new SQLNonTransientConnectionException("Full26 mode proof lost original execute readback phase")
      requireSnapshot(snapshot)

    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)

  private[MySqlFencedTransaction] def afterModeWriteLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      workflowPhase: MySqlModeWorkflowPhase,
      effect: MySqlModeOperationEffect,
      image: ModeWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): ModeWriteReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operation == null || materialization == null || workflowPhase == null || effect == null || image == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires the exact locked M/B/W and Main inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) || !scope.verifiedConnection.contains(verified) ||
      (scope.connectionIdentity ne verified.identity) || (issuer.connection ne verified.connection) ||
      (issuer.ownerThread ne ownerThread) || (issuer.expectation ne expectation) ||
      (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("mode-write reader owner issuer differs from the actual owned connection scope")
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires approved format3/physical2 inputs")
    materialization.requireBoundTo(commandScope, operation)
    if !materialization.effects.exists(_ eq effect) then
      throw new SQLNonTransientConnectionException("mode-write reader effect is outside the exact two-phase materialization")
    val plan = materialization.exactPlan
    commandScope.requireCurrent(scope, plan)
    if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained session barrier")
    if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
      coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained business admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new ModeWriteReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operation, materialization, workflowPhase, effect, image, heldBarrier, admission, rollbackOnly, ownerThread, manifest, binding, writer)

  private[mysql] def requireInactiveDomainPostCommitRetainedTag(tag: Option[IoOperationTag]): Unit =
    requireModePostCommitRetainedTag(tag)

  private[mysql] def requireDriftPostCommitRetainedTag(tag: Option[IoOperationTag]): Unit =
    requireModePostCommitRetainedTag(tag)

  private[mysql] enum DriftWriteReadImage:
    case Before, Target

  private[mysql] enum InactiveDomainWriteReadImage:
    case Before, Target

  /** Only the runner can issue completion after an acknowledged execute and exact fresh full20 read. */
  private[mysql] enum ControlledDebugWriteReadImage:
    case Before, Target

  /** Only the runner can issue completion after an acknowledged execute and exact fresh full20 read. */
  private[mysql] final class ControlledDebugPostCommitReadback private[MySqlFencedTransaction] (
      val commandScope: MySqlRepositoryCommandScope,
      val operation: MySqlRepositoryOperation,
      val materialization: MySqlControlledDebugWorkflowMaterialization,
      val snapshotSchema: String,
      val snapshotConnection: MySqlFormat3ConnectionObservation,
      val snapshotScope: MySqlFormat3ObservationScope,
      val snapshotCanonicalSha256Hex: String,
      private[mysql] val deploymentSource:Option[MySqlScriptSchemaSnapshot]=None,
      private[mysql] val deploymentExpectation:Option[MySqlSchemaAuditExpectation]=None
  ):
    require(commandScope != null && operation != null && materialization != null &&
      snapshotSchema != null && snapshotConnection != null && snapshotScope != null &&
      snapshotCanonicalSha256Hex != null && snapshotCanonicalSha256Hex.matches("[0-9A-F]{64}"),
      "complete immutable controlled-debug post-commit readback proof is required")
    require(materialization.exactOperation == operation && (commandScope.operationPlan() eq materialization.exactPlan),
      "controlled-debug post-commit proof differs from the fixed Main operation plan")

  private[MySqlFencedTransaction] def issueControlledDebugPostCommitReadback(
      issuer: MySqlFencedTransaction,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      fullSnapshot: MySqlFormat3FullSnapshot
  ): ControlledDebugPostCommitReadback =
    if issuer == null || commandScope == null || operation == null || materialization == null || fullSnapshot == null ||
      (issuer.ownerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("controlled-debug completion proof requires the actual owned F3 runner readback")
    materialization.requireBoundTo(commandScope, operation)
    materialization.requireObserved(MySqlOperationEffectPhase.Execute,fullSnapshot,true)
    val expectedScope = commandScope.exactAuditScope()
    if fullSnapshot.schema != issuer.expectation.schema ||
      fullSnapshot.connection != MySqlFormat3ConnectionObservation.from(issuer.verified.identity, "REPEATABLE-READ") ||
      fullSnapshot.observationScope != MySqlFormat3ObservationScope(expectedScope.bootUuid,
        expectedScope.bootGeneration, expectedScope.connectionGeneration) then
      throw new SQLNonTransientConnectionException("controlled-debug completion readback belongs to a different F3 connection generation")
    fullSnapshot.allocationBudget.reserveRetainedMemory(1024L,"private controlled readback proof")
    new ControlledDebugPostCommitReadback(commandScope, operation, materialization, fullSnapshot.schema,
      fullSnapshot.connection, fullSnapshot.observationScope, fullSnapshot.canonicalSha256Hex)

  private[MySqlFencedTransaction] def issueControlledDebugDeploymentPostCommitReadback(issuer:MySqlFencedTransaction,
      commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlControlledDebugWorkflowMaterialization,snapshot:MySqlScriptSchemaSnapshot,
      actualReadOwner:ControlledDebugWriteReadOwner):ControlledDebugPostCommitReadback =
    if issuer==null || commandScope==null || materialization==null || snapshot==null || actualReadOwner==null ||
        (issuer.activeControlledDebugWriteReadOwner ne actualReadOwner) || issuer.ownerThread!=Thread.currentThread() then
      throw new SQLNonTransientConnectionException("Full26 controlled completion requires actual active post-commit read owner")
    materialization.requireBoundTo(commandScope,operation)
    actualReadOwner.requireSnapshot(snapshot)
    actualReadOwner.requirePostCommitExecuteReadback(commandScope,materialization,snapshot)
    materialization.originalRoot.reserveRetainedMemory(1024L,"private Full26 controlled actual readback proof")
    new ControlledDebugPostCommitReadback(commandScope,operation,materialization,snapshot.schema,snapshot.connection,
      snapshot.observationScope,snapshot.canonicalSha256Hex,Some(snapshot),Some(issuer.expectation))

  /** Exact same-transaction full20 reader owner issued after a controlled-debug phase's M→B→W locks. */
  private[mysql] final class ControlledDebugWriteReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operation: MySqlRepositoryOperation,
      private val materialization: MySqlControlledDebugWorkflowMaterialization,
      private val workflowPhase: MySqlControlledDebugWorkflowPhase,
      private val effect: MySqlControlledDebugPhase,
      private val image: ControlledDebugWriteReadImage,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ) extends DeploymentReceiptPurposeReadOwner5(issuer,verified,scope,materialization.exactPlan,gateOwner,rollbackOnly):
    override protected def requirePurposeCurrent():Unit =
      requireCurrent(verified,scope,expectation.schema,issuer.exactPlanningProfile,verified.connection)

    private[MySqlFencedTransaction] def requirePostCommitExecuteReadback(command:MySqlRepositoryCommandScope,
        materialized:MySqlControlledDebugWorkflowMaterialization,snapshot:MySqlScriptSchemaSnapshot):Unit =
      requirePurposeCurrent()
      if (command ne commandScope) || (materialized ne materialization) ||
          workflowPhase!=MySqlControlledDebugWorkflowPhase.ExecuteCommittedAwaitingReadback ||
          image!=ControlledDebugWriteReadImage.Target || effect.phase!=MySqlOperationEffectPhase.Execute then
        throw new SQLNonTransientConnectionException("exact execute-ACK fresh Full26 readback owner required")
      materialization.requireObservedDeploymentReceipt(MySqlOperationEffectPhase.Execute,snapshot,true)
    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (readerSchema != expectation.schema) ||
        (ownerThread ne Thread.currentThread()) || (scope.ownerThread ne ownerThread) ||
        (commandScope.exactAuditScope() ne scope) ||
        operation != materialization.exactOperation || workflowPhase == null || image == null ||
        (readerProfile ne issuer.exactPlanningProfile) then
        throw new SQLNonTransientConnectionException("controlled-debug-write reader owner differs from the exact Main and verified session scope")

      issuer.requireActiveControlledDebugWriteReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      if (expectation.profile != MySqlSchemaAuditProfile.Format3 && expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1) ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
        throw new SQLNonTransientConnectionException("controlled-debug-write reader requires its closed approved schema3/schema5 physical2 inputs")
      if readerConnection.isClosed || readerConnection.getAutoCommit ||
        readerConnection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("controlled-debug-write reader requires the active same-connection REPEATABLE READ transaction")

      val plan = materialization.exactPlan
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
        throw new SQLNonTransientConnectionException("controlled-debug-write reader lost the exact held-session barrier")
      if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
        coordinator.requireControlledDebugWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
        throw new SQLNonTransientConnectionException("controlled-debug-write reader lost the exact business admission")
      verified.readmitSessionBarrier(expectation, scope)
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      materialization.requireBoundTo(commandScope, operation)

    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null then throw new SQLDataException("controlled-debug-write reader returned no complete full20 snapshot")
      val inputs = expectation.format3Inputs
        .getOrElse(throw new SQLNonTransientConnectionException("controlled-debug-write snapshot has no exact format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, expectation.expectedPhysicalFormat.version)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)
      image match
        case ControlledDebugWriteReadImage.Before => materialization.requireObserved(effect.phase,snapshot,false)
        case ControlledDebugWriteReadImage.Target => materialization.requireObserved(effect.phase,snapshot,true)

    private[mysql] def requireRetainedRoot(root: MySqlFormat3ReadBudget): Unit =
      val plan=materialization.exactPlan
      val actual=plan.retainedHandle.getOrElse(
        throw new SQLNonTransientConnectionException("controlled read has no actual Main reservation")).issuer.rootForOriginal(plan)
      if root==null || (root ne actual) || (root ne materialization.originalRoot) || root.isClosed then
        throw new SQLNonTransientConnectionException("controlled read lost the exact original reserved root")

    private[mysql] def requireSnapshot(snapshot:MySqlScriptSchemaSnapshot):Unit =
      requirePurposeCurrent()
      val root=materialization.originalRoot
      requireDeploymentReceiptSession(issuer.deploymentReceiptSession,verified,scope,root)
      MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,snapshot.inputs,root)
      MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expectation,root)
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(snapshot,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
      MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(snapshot,writer)
      image match
        case ControlledDebugWriteReadImage.Before=>materialization.requireObservedDeploymentReceipt(effect.phase,snapshot,false)
        case ControlledDebugWriteReadImage.Target=>materialization.requireObservedDeploymentReceipt(effect.phase,snapshot,true)

    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)

  private[MySqlFencedTransaction] def afterControlledDebugWriteLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      workflowPhase: MySqlControlledDebugWorkflowPhase,
      effect: MySqlControlledDebugPhase,
      image: ControlledDebugWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): ControlledDebugWriteReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operation == null || materialization == null || workflowPhase == null || effect == null || image == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader owner requires the exact locked M/B/W and Main inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) || !scope.verifiedConnection.contains(verified) ||
      (scope.connectionIdentity ne verified.identity) || (issuer.connection ne verified.connection) ||
      (issuer.ownerThread ne ownerThread) || (issuer.expectation ne expectation) ||
      (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader owner issuer differs from the actual owned connection scope")
    if (expectation.profile != MySqlSchemaAuditProfile.Format3 && expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader owner requires closed approved schema3/schema5 physical2 inputs")
    materialization.requireBoundTo(commandScope, operation)
    if !materialization.effects.exists(_ eq effect) then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader effect is outside the exact two-phase materialization")
    val plan = materialization.exactPlan
    commandScope.requireCurrent(scope, plan)
    if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader requires the exact retained session barrier")
    if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
      coordinator.requireControlledDebugWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
      throw new SQLNonTransientConnectionException("controlled-debug-write reader requires the exact retained business admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new ControlledDebugWriteReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operation, materialization, workflowPhase, effect, image, heldBarrier, admission, rollbackOnly, ownerThread, manifest, binding, writer)

  private[mysql] def requireControlledDebugPostCommitRetainedTag(tag: Option[IoOperationTag]): Unit =
    requireModePostCommitRetainedTag(tag)

  /** Only the runner can issue completion after an acknowledged execute and exact fresh full20 read. */

  private[mysql] final class InactiveDomainPostCommitReadback private[MySqlFencedTransaction] (
      val commandScope: MySqlRepositoryCommandScope,
      val operation: MySqlRepositoryOperation,
      val materialization: MySqlInactiveDomainWorkflowMaterialization,
      val snapshotSchema: String,
      val snapshotConnection: MySqlFormat3ConnectionObservation,
      val snapshotScope: MySqlFormat3ObservationScope,
      val snapshotCanonicalSha256Hex: String
  ):
    require(commandScope != null && operation != null && materialization != null &&
      snapshotSchema != null && snapshotConnection != null && snapshotScope != null &&
      snapshotCanonicalSha256Hex != null && snapshotCanonicalSha256Hex.matches("[0-9a-f]{64}"),
      "complete immutable mode post-commit readback proof is required")
    require(materialization.exactOperation == operation && (commandScope.operationPlan() eq materialization.exactPlan),
      "mode post-commit proof differs from the fixed Main operation plan")

  private[MySqlFencedTransaction] def issueInactiveDomainPostCommitReadback(
      issuer: MySqlFencedTransaction,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      fullSnapshot: MySqlFormat3FullSnapshot
  ): InactiveDomainPostCommitReadback =
    if issuer == null || commandScope == null || operation == null || materialization == null || fullSnapshot == null ||
      (issuer.ownerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("mode completion proof requires the actual owned F3 runner readback")
    materialization.requireBoundTo(commandScope, operation)
    materialization.effects(1).verifyReadback(fullSnapshot)
    val expectedScope = commandScope.exactAuditScope()
    if fullSnapshot.schema != issuer.expectation.schema ||
      fullSnapshot.connection != MySqlFormat3ConnectionObservation.from(issuer.verified.identity, "REPEATABLE-READ") ||
      fullSnapshot.observationScope != MySqlFormat3ObservationScope(expectedScope.bootUuid,
        expectedScope.bootGeneration, expectedScope.connectionGeneration) then
      throw new SQLNonTransientConnectionException("mode completion readback belongs to a different F3 connection generation")
    new InactiveDomainPostCommitReadback(commandScope, operation, materialization, fullSnapshot.schema,
      fullSnapshot.connection, fullSnapshot.observationScope, fullSnapshot.canonicalSha256Hex)

  /** Exact same-transaction full20 reader owner issued after a mode phase's M→B→W locks. */
  private[mysql] final class InactiveDomainWriteReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operation: MySqlRepositoryOperation,
      private val materialization: MySqlInactiveDomainWorkflowMaterialization,
      private val workflowPhase: MySqlInactiveDomainWorkflowPhase,
      private val effect: MySqlInactiveDomainPhase,
      private val image: InactiveDomainWriteReadImage,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ):
    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (readerSchema != expectation.schema) ||
        (ownerThread ne Thread.currentThread()) || (scope.ownerThread ne ownerThread) ||
        (commandScope.exactAuditScope() ne scope) ||
        operation != materialization.exactOperation || workflowPhase == null || image == null ||
        (readerProfile ne expectation.format3Inputs.map(_.limitProfile).orNull) then
        throw new SQLNonTransientConnectionException("mode-write reader owner differs from the exact Main and verified session scope")

      issuer.requireActiveInactiveDomainWriteReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || expectation.format3Inputs.isEmpty then
        throw new SQLNonTransientConnectionException("mode-write reader requires the exact approved format3/physical2 inputs")
      if readerConnection.isClosed || readerConnection.getAutoCommit ||
        readerConnection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode-write reader requires the active same-connection REPEATABLE READ transaction")

      val plan = materialization.exactPlan
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact held-session barrier")
      if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
        coordinator.requireInactiveDomainWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact business admission")
      verified.readmitSessionBarrier(expectation, scope)
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      materialization.requireBoundTo(commandScope, operation)

    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null then throw new SQLDataException("mode-write reader returned no complete full20 snapshot")
      val inputs = expectation.format3Inputs
        .getOrElse(throw new SQLNonTransientConnectionException("mode-write snapshot has no exact format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, expectation.expectedPhysicalFormat.version)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)
      image match
        case InactiveDomainWriteReadImage.Before => effect.verifyPreimage(snapshot)
        case InactiveDomainWriteReadImage.Target => effect.verifyReadback(snapshot)

    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)

  private[MySqlFencedTransaction] def afterInactiveDomainWriteLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      workflowPhase: MySqlInactiveDomainWorkflowPhase,
      effect: MySqlInactiveDomainPhase,
      image: InactiveDomainWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): InactiveDomainWriteReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operation == null || materialization == null || workflowPhase == null || effect == null || image == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires the exact locked M/B/W and Main inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) || !scope.verifiedConnection.contains(verified) ||
      (scope.connectionIdentity ne verified.identity) || (issuer.connection ne verified.connection) ||
      (issuer.ownerThread ne ownerThread) || (issuer.expectation ne expectation) ||
      (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("mode-write reader owner issuer differs from the actual owned connection scope")
    if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || expectation.format3Inputs.isEmpty then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires approved format3/physical2 inputs")
    materialization.requireBoundTo(commandScope, operation)
    if !materialization.effects.exists(_ eq effect) then
      throw new SQLNonTransientConnectionException("mode-write reader effect is outside the exact two-phase materialization")
    val plan = materialization.exactPlan
    commandScope.requireCurrent(scope, plan)
    if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained session barrier")
    if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
      coordinator.requireInactiveDomainWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained business admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new InactiveDomainWriteReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operation, materialization, workflowPhase, effect, image, heldBarrier, admission, rollbackOnly, ownerThread, manifest, binding, writer)

  private[mysql] final class DriftPostCommitReadback private[MySqlFencedTransaction] (
      val commandScope: MySqlRepositoryCommandScope,
      val operation: MySqlRepositoryOperation,
      val materialization: MySqlDriftWorkflowMaterialization,
      val snapshotSchema: String,
      val snapshotConnection: MySqlFormat3ConnectionObservation,
      val snapshotScope: MySqlFormat3ObservationScope,
      val snapshotCanonicalSha256Hex: String,
      private[mysql] val deploymentSource:Option[MySqlScriptSchemaSnapshot]=None
  ):
    require(commandScope != null && operation != null && materialization != null &&
      snapshotSchema != null && snapshotConnection != null && snapshotScope != null &&
      snapshotCanonicalSha256Hex != null && snapshotCanonicalSha256Hex.matches("[0-9a-f]{64}"),
      "complete immutable mode post-commit readback proof is required")
    require(materialization.exactOperation == operation && (commandScope.operationPlan() eq materialization.exactPlan),
      "mode post-commit proof differs from the fixed Main operation plan")

  private[MySqlFencedTransaction] def issueDriftPostCommitReadback(
      issuer: MySqlFencedTransaction,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      fullSnapshot: MySqlFormat3FullSnapshot
  ): DriftPostCommitReadback =
    if issuer == null || commandScope == null || operation == null || materialization == null || fullSnapshot == null ||
      (issuer.ownerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("mode completion proof requires the actual owned F3 runner readback")
    materialization.requireBoundTo(commandScope, operation)
    materialization.effects(1).verifyReadback(fullSnapshot)
    val expectedScope = commandScope.exactAuditScope()
    if fullSnapshot.schema != issuer.expectation.schema ||
      fullSnapshot.connection != MySqlFormat3ConnectionObservation.from(issuer.verified.identity, "REPEATABLE-READ") ||
      fullSnapshot.observationScope != MySqlFormat3ObservationScope(expectedScope.bootUuid,
        expectedScope.bootGeneration, expectedScope.connectionGeneration) then
      throw new SQLNonTransientConnectionException("mode completion readback belongs to a different F3 connection generation")
    new DriftPostCommitReadback(commandScope, operation, materialization, fullSnapshot.schema,
      fullSnapshot.connection, fullSnapshot.observationScope, fullSnapshot.canonicalSha256Hex)

  private[MySqlFencedTransaction] def issueDriftDeploymentPostCommitReadback(issuer:MySqlFencedTransaction,
      commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlDriftWorkflowMaterialization,snapshot:MySqlScriptSchemaSnapshot,
      owner:DriftWriteReadOwner):DriftPostCommitReadback =
    if issuer==null || owner==null || (issuer.activeDriftWriteReadOwner ne owner) ||
      Thread.currentThread()!=issuer.ownerThread then
      throw new SQLNonTransientConnectionException("Full26 mode proof requires original active fresh readback owner")
    owner.requirePostCommitExecuteReadback(commandScope,operation,materialization,snapshot)
    owner.originalPurposeRoot.reserveRetainedMemory(512L,"actual Full26 mode post-commit proof")
    new DriftPostCommitReadback(commandScope,operation,materialization,snapshot.schema,snapshot.connection,
      snapshot.observationScope,snapshot.canonicalSha256Hex,Some(snapshot))

  /** Exact same-transaction full20 reader owner issued after a mode phase's M→B→W locks. */
  private[mysql] final class DriftWriteReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operation: MySqlRepositoryOperation,
      private val materialization: MySqlDriftWorkflowMaterialization,
      private val workflowPhase: MySqlDriftWorkflowPhase,
      private val effect: MySqlDriftOperationEffect,
      private val image: DriftWriteReadImage,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ) extends DeploymentReceiptPurposeReadOwner5(issuer,verified,scope,materialization.exactPlan,gateOwner,rollbackOnly):
    override protected def requirePurposeCurrent():Unit =
      requireCurrent(verified,scope,expectation.schema,issuer.exactPlanningProfile,verified.connection)
    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (readerSchema != expectation.schema) ||
        (ownerThread ne Thread.currentThread()) || (scope.ownerThread ne ownerThread) ||
        (commandScope.exactAuditScope() ne scope) ||
        operation != materialization.exactOperation || workflowPhase == null || image == null ||
        (readerProfile ne issuer.exactPlanningProfile) then
        throw new SQLNonTransientConnectionException("mode-write reader owner differs from the exact Main and verified session scope")

      issuer.requireActiveDriftWriteReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
        throw new SQLNonTransientConnectionException("mode-write reader requires the exact approved format3/physical2 inputs")
      if readerConnection.isClosed || readerConnection.getAutoCommit ||
        readerConnection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode-write reader requires the active same-connection REPEATABLE READ transaction")

      val plan = materialization.exactPlan
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact held-session barrier")
      if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
        coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
        throw new SQLNonTransientConnectionException("mode-write reader lost the exact business admission")
      verified.readmitSessionBarrier(expectation, scope)
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      materialization.requireBoundTo(commandScope, operation)

    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null then throw new SQLDataException("mode-write reader returned no complete full20 snapshot")
      val inputs = expectation.format3Inputs
        .getOrElse(throw new SQLNonTransientConnectionException("mode-write snapshot has no exact format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, expectation.expectedPhysicalFormat.version)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)
      image match
        case DriftWriteReadImage.Before => effect.verifyPreimage(snapshot)
        case DriftWriteReadImage.Target => effect.verifyReadback(snapshot)

    private[mysql] def requireSnapshot(snapshot:MySqlScriptSchemaSnapshot):Unit =
      val root=originalPurposeRoot
      requireDeploymentReceiptSession(issuer.deploymentReceiptSession,verified,scope,root)
      MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,snapshot.inputs,root)
      if snapshot.connection!=MySqlFormat3ConnectionObservation.from(verified.identity,"REPEATABLE-READ") ||
        snapshot.observationScope!=MySqlFormat3ObservationScope(scope.bootUuid,scope.bootGeneration,scope.connectionGeneration) then
        throw new SQLTransactionRollbackException("Full26 mode readback lost exact locked scope")
      MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expectation,root)
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(snapshot,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
      MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(snapshot,writer)
      image match
        case DriftWriteReadImage.Before=>effect.verifyPreimage(snapshot)
        case DriftWriteReadImage.Target=>effect.verifyReadback(snapshot)

    private[MySqlFencedTransaction] def requirePostCommitExecuteReadback(actualCommand:MySqlRepositoryCommandScope,
        actualOperation:MySqlRepositoryOperation,actualMaterialization:MySqlDriftWorkflowMaterialization,
        snapshot:MySqlScriptSchemaSnapshot):Unit =
      if (actualCommand ne commandScope) || actualOperation!=operation ||
        (actualMaterialization ne materialization) || image!=DriftWriteReadImage.Target ||
        workflowPhase!=MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback ||
        (effect ne materialization.effects(1)) then
        throw new SQLNonTransientConnectionException("Full26 mode proof lost original execute readback phase")
      requireSnapshot(snapshot)

    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)

  private[MySqlFencedTransaction] def afterDriftWriteLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      workflowPhase: MySqlDriftWorkflowPhase,
      effect: MySqlDriftOperationEffect,
      image: DriftWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): DriftWriteReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operation == null || materialization == null || workflowPhase == null || effect == null || image == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires the exact locked M/B/W and Main inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) || !scope.verifiedConnection.contains(verified) ||
      (scope.connectionIdentity ne verified.identity) || (issuer.connection ne verified.connection) ||
      (issuer.ownerThread ne ownerThread) || (issuer.expectation ne expectation) ||
      (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("mode-write reader owner issuer differs from the actual owned connection scope")
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("mode-write reader owner requires approved format3/physical2 inputs")
    materialization.requireBoundTo(commandScope, operation)
    if !materialization.effects.exists(_ eq effect) then
      throw new SQLNonTransientConnectionException("mode-write reader effect is outside the exact two-phase materialization")
    val plan = materialization.exactPlan
    commandScope.requireCurrent(scope, plan)
    if commandScope.requireHeldSessionBarrier(scope, plan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained session barrier")
    if commandScope.requireBusinessAdmission(scope, plan).ne(admission) ||
      coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization, workflowPhase).ne(admission) then
      throw new SQLNonTransientConnectionException("mode-write reader requires the exact retained business admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new DriftWriteReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operation, materialization, workflowPhase, effect, image, heldBarrier, admission, rollbackOnly, ownerThread, manifest, binding, writer)

  private[mysql] final class LockedReadOwner private[MySqlFencedTransaction] (
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val admission: MySqlBusinessAdmission,
      private val operationContext: MySqlOperationContext,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow
  ):
    private[mysql] def requireCurrent(
        currentVerified: VerifiedMySqlConnection,
        currentScope: MySqlAuditBootScope,
        currentExpectation: MySqlSchemaAuditExpectation,
        currentOperationContext: MySqlOperationContext,
        currentRollbackOnly: MySqlRollbackOnlyState,
        currentOwnerThread: Thread
    ): Unit =
      if (currentVerified ne verified) || (currentScope ne scope) ||
        (currentExpectation ne expectation) || (currentOperationContext ne operationContext) ||
        (currentRollbackOnly ne rollbackOnly) || (currentOwnerThread ne ownerThread) ||
        (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
        (verified.connection ne scope.connectionToken) ||
        !scope.verifiedConnection.contains(verified) || (scope.connectionIdentity ne verified.identity) then
        throw new IllegalStateException("format3 full-snapshot owner differs from the exact fenced transaction scope")

      MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)

      if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
        expectation.format3Inputs.isEmpty then
        throw new IllegalStateException("format3 full-snapshot owner requires an exact format3/physical2 expectation")

      val expectedDdl = HexFormat.of().parseHex(expectation.ddlSha256).toVector
      val expectedGuard = HexFormat.of().parseHex(expectation.objects.canonicalSha256).toVector
      if manifest.singleton != 1 || manifest.backend != "MYSQL" ||
        manifest.formatVersion != expectation.profile.schemaFormat || manifest.bootstrapState != "READY" ||
        manifest.ddlSha256 != expectedDdl || manifest.provisioningUuid != expectation.provisioningUuid ||
        manifest.serverUuid != expectation.serverUuid || manifest.bindingUuid != expectation.bindingUuid ||
        manifest.guardManifestSha256 != Some(expectedGuard) ||
        !MySqlFencedTransactionSupport.importMatches(manifest, expectation.expectedImport) then
        throw new IllegalStateException("format3 full-snapshot owner no longer matches its locked manifest row")
      if binding.singleton != 1 || binding.bindingUuid != expectation.bindingUuid ||
        binding.cutoverState != "CERTIFIED" || binding.formatVersion != expectation.expectedPhysicalFormat.version then
        throw new IllegalStateException("format3 full-snapshot owner no longer matches its locked physical binding row")

      val token = admission.token
      if (token.verified ne verified) || (token.scope ne scope) ||
        operationContext.bootUuid != admission.bootUuid || operationContext.writerEpoch != admission.writerEpoch ||
        operationContext.bindingUuid != admission.bindingUuid ||
        admission.bindingUuid != expectation.bindingUuid || admission.bootUuid != scope.bootUuid ||
        writer.bindingUuid != token.bindingUuid || writer.writerEpoch != token.writerEpoch ||
        writer.ownerBootUuid != Some(token.bootUuid) || writer.leaseUntil != token.leaseUntil ||
        !writer.leaseUntil.isAfter(writer.databaseNow) || !token.fence.isCurrentAt(System.nanoTime()) then
        throw new IllegalStateException("format3 full-snapshot owner no longer matches its locked writer admission")

  private def afterLockedRows(
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      admission: MySqlBusinessAdmission,
      operationContext: MySqlOperationContext,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): LockedReadOwner =
    new LockedReadOwner(gateOwner, verified, scope, expectation, admission, operationContext,
      rollbackOnly, ownerThread, manifest, binding, writer)

  /** 只在真实 M→B→W 行已锁定、且同一 IO 事务仍活动时发行 planning owner。 */
  private[MySqlFencedTransaction] def afterPlanningLockedRows(
      issuer: MySqlFencedTransaction,
      gateOwner: MySqlConnectionTransactionGate.Owner,
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation,
      coordinator: MySqlWriterFenceCoordinator,
      commandScope: MySqlRepositoryCommandScope,
      operationPlan: MySqlOwnedRepositoryOperationPlan,
      contextIdentity: MySqlRepositoryOperationContextIdentity,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      ownerThread: Thread,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): PlanningReadOwner =
    if issuer == null || gateOwner == null || verified == null || scope == null || expectation == null ||
      coordinator == null || commandScope == null || operationPlan == null || contextIdentity == null ||
      heldBarrier == null || admission == null || rollbackOnly == null || ownerThread == null ||
      manifest == null || binding == null || writer == null then
      throw new SQLNonTransientConnectionException("planning-read owner requires the exact locked M/B/W and Main session inputs")
    if (Thread.currentThread() ne ownerThread) || (scope.ownerThread ne ownerThread) ||
      (verified.connection ne scope.connectionToken) ||
      !scope.verifiedConnection.contains(verified) || (scope.connectionIdentity ne verified.identity) ||
      (issuer.connection ne verified.connection) || (issuer.ownerThread ne ownerThread) ||
      (issuer.expectation ne expectation) || (issuer.coordinator ne coordinator) || (issuer.scope ne scope) then
      throw new SQLNonTransientConnectionException("planning-read owner issuer differs from the actual owned connection scope")
    commandScope.requireCurrent(scope, operationPlan)
    if commandScope.requireHeldSessionBarrier(scope, operationPlan) ne heldBarrier then
      throw new SQLNonTransientConnectionException("planning-read owner requires the exact retained held-session barrier")
    if commandScope.requireBusinessAdmission(scope, operationPlan) ne admission then
      throw new SQLNonTransientConnectionException("planning-read owner requires the exact retained business admission")
    operationPlan.requireTag(commandScope.tag())
    if contextIdentity.tag != operationPlan.tag || contextIdentity.workflowUuid != operationPlan.workflowUuid ||
      contextIdentity.reserveOperationUuid != operationPlan.reserveOperationUuid ||
      contextIdentity.bindingUuid != expectation.bindingUuid || contextIdentity.writerEpoch != admission.writerEpoch ||
      contextIdentity.fixedLeaseUntil != admission.token.leaseUntil ||
      contextIdentity.tag.connectionGeneration != scope.connectionGeneration ||
      contextIdentity.exactLimitProfile != issuer.exactPlanningProfile then
      throw new SQLNonTransientConnectionException("planning-read context differs from the exact retained Main plan and admission")
    issuer.validateManifest(manifest, admission)
    issuer.validateBinding(binding, admission)
    issuer.validateWriterIdentityAndLease(writer, admission)
    if writer.databaseNow == null || writer.databaseNow.getNano % 1000 != 0 ||
      writer.databaseNow.getYear < 1000 || writer.databaseNow.getYear > 9999 ||
      writer.leaseUntil != admission.token.leaseUntil ||
      !writer.leaseUntil.isAfter(writer.databaseNow) then
      throw new SQLNonTransientConnectionException("planning-read timestamp must come from the live locked W UTC_TIMESTAMP(6) row")
    MySqlConnectionTransactionGate.requireCurrent(gateOwner, verified.connection, rollbackOnly)
    rollbackOnly.cause.foreach(throw _)
    new PlanningReadOwner(issuer, gateOwner, verified, scope, expectation, coordinator, commandScope,
      operationPlan, contextIdentity, heldBarrier, admission, rollbackOnly, ownerThread, manifest,
      binding, writer, writer.databaseNow)

  /** 封闭到原 planning/write owner 的同交易 Full26 借用；不发行独立 SQL 权限。 */
  private[mysql] sealed abstract class DeploymentReceiptPurposeReadOwner5 private[MySqlFencedTransaction] (
      purposeIssuer:MySqlFencedTransaction,purposeVerified:VerifiedMySqlConnection,
      purposeScope:MySqlAuditBootScope,purposePlan:MySqlOwnedRepositoryOperationPlan,
      purposeGate:MySqlConnectionTransactionGate.Owner,purposeRollbackOnly:MySqlRollbackOnlyState):
    protected def requirePurposeCurrent():Unit
    private var purposeReadSettled=false
    private[mysql] def originalPurposeRoot:MySqlFormat3ReadBudget =
      purposePlan.retainedHandle.getOrElse(throw new SQLNonTransientConnectionException("Full26 purpose has no original plan handle"))
        .issuer.rootForOriginal(purposePlan)
    private[MySqlFencedTransaction] def settlePurposeReadAfterTransactionEnd():Unit =
      if purposeIssuer.expectation.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 then
        if purposeReadSettled || purposeIssuer.activePlanningReadOwner!=null ||
            purposeIssuer.activeModeWriteReadOwner!=null ||
            purposeIssuer.activeDriftWriteReadOwner!=null || purposeIssuer.activeNormalRecallWriteReadOwner!=null ||
            purposeIssuer.activeControlledDebugWriteReadOwner!=null || !purposeVerified.connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("Full26 purpose read requires actual completed original transaction")
        purposeRollbackOnly.cause.foreach(throw _)
        purposeReadSettled=true
        purposeIssuer.deploymentReceiptSession.settlePurposeRead(this)
    private[mysql] def belongsToPurposeHandle(actual:MySqlRetainedPlanHandle):Boolean =
      purposePlan.retainedHandle.exists(_ eq actual)
    private[mysql] def requirePurposeReadSettled(actual:MySqlDeploymentReceiptSession):Unit =
      if !purposeReadSettled || (purposeIssuer.deploymentReceiptSession ne actual) ||
          Thread.currentThread()!=purposeScope.ownerThread then
        throw new SQLNonTransientConnectionException("exact completed Full26 purpose owner required")
    private[mysql] def transferPurposeImageToOriginalProjection(actual:MySqlDeploymentReceiptSession,
        child:MySqlFormat3ReadBudget,originalRoot:MySqlFormat3ReadBudget):Unit =
      requirePurposeReadSettled(actual)
      if child==null || child.isClosed || !child.isSnapshotReadChildOf(originalRoot) ||
          (originalPurposeRoot ne originalRoot) then
        throw new SQLNonTransientConnectionException("exact original Full26 projection child/root required")
      purposeIssuer.retainedModeProjectionBudgets.retainScoped(purposePlan.tag,child)
    private[mysql] def hasOriginalPurposeProjection(actual:MySqlDeploymentReceiptSession):Boolean =
      requirePurposeReadSettled(actual)
      purposeIssuer.retainedModeProjectionBudgets.contains(purposePlan.tag)
    private[mysql] def requireDeploymentReceiptSession(actual:MySqlDeploymentReceiptSession,
        currentVerified:VerifiedMySqlConnection,currentScope:MySqlAuditBootScope,
        originalRoot:MySqlFormat3ReadBudget):Unit =
      requirePurposeCurrent()
      if purposeIssuer.expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 ||
          (purposeIssuer.deploymentReceiptSession ne actual) || (currentVerified ne purposeVerified) ||
          (currentScope ne purposeScope) || (originalPurposeRoot ne originalRoot) then
        throw new SQLNonTransientConnectionException("Full26 purpose requires exact Session and original retained root")
    private[mysql] def deploymentReceiptReadTransaction(actual:MySqlDeploymentReceiptSession,
        currentVerified:VerifiedMySqlConnection,currentScope:MySqlAuditBootScope,originalRoot:MySqlFormat3ReadBudget)
        :(MySqlConnectionTransactionGate.Owner,MySqlRollbackOnlyState) =
      requireDeploymentReceiptSession(actual,currentVerified,currentScope,originalRoot)
      (purposeGate,purposeRollbackOnly)

  /** 尚无 operation context 时，仅由本 runner 在真实 M→B→W 锁后签发的只读 owner。 */
  private[mysql] final class PlanningReadOwner private[MySqlFencedTransaction] (
      private val issuer: MySqlFencedTransaction,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val coordinator: MySqlWriterFenceCoordinator,
      private val commandScope: MySqlRepositoryCommandScope,
      private val operationPlan: MySqlOwnedRepositoryOperationPlan,
      private val contextIdentity: MySqlRepositoryOperationContextIdentity,
      private val heldBarrier: HeldSessionBarrier,
      private val admission: MySqlBusinessAdmission,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: MySqlLockedBinding,
      private val writer: MySqlBusinessWriterRow,
      val plannedAt: LocalDateTime
  ) extends DeploymentReceiptPurposeReadOwner5(issuer,verified,scope,operationPlan,gateOwner,rollbackOnly):
    override protected def requirePurposeCurrent():Unit =
      requireCurrent(verified,scope,expectation.schema,issuer.exactPlanningProfile,verified.connection)

    private[mysql] def requireCurrent(
        readerVerified: VerifiedMySqlConnection,
        readerScope: MySqlAuditBootScope,
        readerSchema: String,
        readerProfile: SourceLimitProfile,
        readerConnection: java.sql.Connection
    ): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (ownerThread ne Thread.currentThread()) ||
        (scope.ownerThread ne ownerThread) || (readerSchema != expectation.schema) ||
        (readerProfile ne issuer.exactPlanningProfile) ||
        (commandScope.exactAuditScope() ne scope) || (operationPlan ne commandScope.operationPlan()) then
        throw new SQLNonTransientConnectionException("planning-read owner differs from the exact Main and verified session scope")

      issuer.requireActivePlanningReadOwner(this)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      commandScope.requireCurrent(scope, operationPlan)
      if (commandScope.requireHeldSessionBarrier(scope, operationPlan) ne heldBarrier) ||
        (commandScope.requireBusinessAdmission(scope, operationPlan) ne admission) then
        throw new SQLNonTransientConnectionException("planning-read owner lost its exact held barrier or admission")
      verified.readmitSessionBarrier(expectation, scope)
      val currentAdmission = coordinator.requireBusinessAdmission(verified, scope)
      if (currentAdmission ne admission) || (admission.token.verified ne verified) ||
        (admission.token.scope ne scope) || contextIdentity.tag != operationPlan.tag ||
        contextIdentity.workflowUuid != operationPlan.workflowUuid ||
        contextIdentity.reserveOperationUuid != operationPlan.reserveOperationUuid ||
        contextIdentity.bindingUuid != expectation.bindingUuid ||
        contextIdentity.writerEpoch != admission.writerEpoch ||
        contextIdentity.fixedLeaseUntil != admission.token.leaseUntil ||
        contextIdentity.tag.connectionGeneration != scope.connectionGeneration then
        throw new SQLNonTransientConnectionException("planning-read identity no longer matches the Main plan and exact admission")
      issuer.validateManifest(manifest, admission)
      issuer.validateBinding(binding, admission)
      issuer.validateWriterIdentityAndLease(writer, admission)
      if writer.databaseNow != plannedAt || !writer.leaseUntil.isAfter(plannedAt) then
        throw new SQLNonTransientConnectionException("planning timestamp is not the locked W UTC_TIMESTAMP(6) value")
      val recaptured = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
      if !recaptured.canonical.exactlyEquals(contextIdentity.canonical) then
        throw new SQLNonTransientConnectionException("operation context identity changed during locked planning")

    /** 当前真实 held planning owner 才能取得原已保留 plan root。 */
    private[mysql] def originalRetainedRoot: Option[MySqlFormat3ReadBudget] =
      requireCurrent(verified, scope, expectation.schema,
        issuer.exactPlanningProfile, verified.connection)
      operationPlan.retainedHandle.map(_.issuer.rootForOriginal(operationPlan))

    private[mysql] def requireNormalRecallMaterializationInputs(
        currentVerified: VerifiedMySqlConnection,currentScope: MySqlAuditBootScope,currentSchema: String,
        currentProfile: SourceLimitProfile,currentConnection: Connection,currentPlan: MySqlOwnedRepositoryOperationPlan,
        currentOperation: MySqlRepositoryOperation,currentContext: MySqlRepositoryOperationContextIdentity,
        currentPlannedAt: LocalDateTime,currentSnapshot: MySqlFormat3FullSnapshot): Unit =
      requireCurrent(currentVerified,currentScope,currentSchema,currentProfile,currentConnection)
      requireSnapshot(currentSnapshot)
      if (currentPlan ne operationPlan) || (currentContext ne contextIdentity) || currentPlannedAt!=plannedAt then
        throw new SQLTransactionRollbackException("normal recall materialization differs from its actual locked planning inputs")
      val original=originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("normal recall planning requires its actual reserved Main root"))
      if currentSnapshot.allocationBudget ne original then
        throw new SQLNonTransientConnectionException("normal recall planning full20 image has a different original root")
      operationPlan.requireSingle(currentOperation,currentProfile)

    private[mysql] def requireControlledMaterializationInputs(
        currentVerified: VerifiedMySqlConnection,currentScope: MySqlAuditBootScope,currentSchema: String,
        currentProfile: SourceLimitProfile,currentConnection: Connection,currentPlan: MySqlOwnedRepositoryOperationPlan,
        currentOperation: MySqlRepositoryOperation,currentContext: MySqlRepositoryOperationContextIdentity,
        currentPlannedAt: LocalDateTime,currentSnapshot: MySqlFormat3FullSnapshot): Unit =
      requireCurrent(currentVerified,currentScope,currentSchema,currentProfile,currentConnection)
      requireSnapshot(currentSnapshot)
      if (currentPlan ne operationPlan) || (currentContext ne contextIdentity) || currentPlannedAt!=plannedAt then
        throw new SQLTransactionRollbackException("controlled materialization differs from its actual locked planning inputs")
      val original=originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("controlled planning requires its actual reserved Main root"))
      if currentSnapshot.allocationBudget ne original then
        throw new SQLNonTransientConnectionException("controlled planning full20 image has a different original root")
      operationPlan.requireSingle(currentOperation,currentProfile)

    private[mysql] def requireControlledDeploymentReceiptMaterializationInputs(
        currentPlan:MySqlOwnedRepositoryOperationPlan,currentOperation:MySqlRepositoryOperation,
        currentContext:MySqlRepositoryOperationContextIdentity,currentSnapshot:MySqlScriptSchemaSnapshot):Unit =
      val original=originalRetainedRoot.getOrElse(throw new SQLNonTransientConnectionException("Full26 planning has no original retained root"))
      requireDeploymentReceiptSession(issuer.deploymentReceiptSession,verified,scope,original)
      MySqlFormat3QueryProjection.requireDeploymentSource(currentSnapshot,currentSnapshot.inputs,original)
      if (currentPlan ne operationPlan) || (currentContext ne contextIdentity) ||
          currentSnapshot.connection!=MySqlFormat3ConnectionObservation.from(verified.identity,"REPEATABLE-READ") ||
          currentSnapshot.observationScope!=MySqlFormat3ObservationScope(scope.bootUuid,scope.bootGeneration,scope.connectionGeneration) then
        throw new SQLTransactionRollbackException("Full26 controlled snapshot lost its exact locked planning inputs")
      MySqlDeploymentReceiptReadOwner.requireManifestData(currentSnapshot,expectation,original)
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(currentSnapshot,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
      MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(currentSnapshot,writer)
      operationPlan.requireSingle(currentOperation,issuer.exactPlanningProfile)

    private[mysql] def requireMaterializationInputs(
        currentVerified: VerifiedMySqlConnection,
        currentScope: MySqlAuditBootScope,
        currentSchema: String,
        currentProfile: SourceLimitProfile,
        currentConnection: java.sql.Connection,
        currentPlan: MySqlOwnedRepositoryOperationPlan,
        currentOperation: MySqlRepositoryOperation,
        currentContext: MySqlRepositoryOperationContextIdentity,
        currentPlannedAt: LocalDateTime,
        currentSnapshot: MySqlFormat3FullSnapshot,
        currentLocatorRevision: Long
    ): Unit =
      requireCurrent(currentVerified, currentScope, currentSchema, currentProfile, currentConnection)
      requireSnapshot(currentSnapshot)
      if (currentPlan ne operationPlan) || (currentContext ne contextIdentity) || currentPlannedAt != plannedAt ||
        currentLocatorRevision < 0L then
        throw new SQLTransactionRollbackException("mode materialization differs from its exact one-shot locked planning inputs")
      operationPlan.requireSingle(currentOperation, currentProfile)

    private[mysql] def requireInactiveMaterializationInputs(
        currentVerified: VerifiedMySqlConnection, currentScope: MySqlAuditBootScope,
        currentSchema: String, currentProfile: SourceLimitProfile, currentConnection: java.sql.Connection,
        currentPlan: MySqlOwnedRepositoryOperationPlan, request: MySqlInactiveDomainUpdateRequest,
        currentContext: MySqlRepositoryOperationContextIdentity, currentPlannedAt: LocalDateTime,
        currentSnapshot: MySqlFormat3FullSnapshot
    ): Unit =
      requireCurrent(currentVerified, currentScope, currentSchema, currentProfile, currentConnection)
      requireSnapshot(currentSnapshot)
      if request == null || (currentPlan ne operationPlan) || (currentContext ne contextIdentity) ||
        currentPlannedAt != plannedAt then
        throw new SQLTransactionRollbackException("inactive materialization differs from its exact locked planning inputs")
      operationPlan.requireSingle(request.repositoryOperation, currentProfile)
    private[mysql] def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null || snapshot.schema != expectation.schema ||
        snapshot.connection != MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ") ||
        snapshot.observationScope != MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration) ||
        (snapshot.limitProfile ne expectation.format3Inputs.map(_.limitProfile).orNull) then
        throw new SQLTransactionRollbackException("planning snapshot identity differs from its locked owner")
      val inputs = expectation.format3Inputs.getOrElse(
        throw new SQLNonTransientConnectionException("planning snapshot has no independently approved format3 inputs"))
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, Some(PhysicalRecoveryBinding(
        binding.bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)))
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)

    private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)
    override def toString: String = "MySqlFencedTransaction.PlanningReadOwner[active full20 read]"

/** 受限的同连接业务事务 runner；不取得世界、物理 locator 或 runtime Ready 权限。 */
final class MySqlFencedTransaction private[mysql] (
    private val expectation: MySqlSchemaAuditExpectation,
    private val coordinator: MySqlWriterFenceCoordinator,
    private val verified: VerifiedMySqlConnection,
    private val scope: MySqlAuditBootScope,
    suppliedAllocationLedger: Option[MySqlFormat3AllocationLedger]
):
  /** 原四参数ABI保留；session五参数路径只转发纯allocation ledger。 */
  def this(expectation: MySqlSchemaAuditExpectation, coordinator: MySqlWriterFenceCoordinator,
      verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope) =
    this(expectation, coordinator, verified, scope, None)

  private enum DriftSqlPhase:
    case Reserve, Execute

  private enum InactiveDomainSqlPhase:
    case Reserve, Execute

  private enum ModeSqlPhase:
    case Reserve, Execute

  private enum NormalRecallSqlPhase:
    case Reserve, Execute

  private enum ControlledDebugSqlPhase:
    case Reserve, Execute

  require(expectation != null && coordinator != null && verified != null && scope != null, "MySQL transaction dependencies are required")
  require(verified.connection != null && verified.identity != null, "verified MySQL connection is incomplete")
  require(expectation.schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(expectation.schema), "fixed MySQL schema identifier is unsafe")
  require(scope.ownerThread eq Thread.currentThread(), "MySQL transaction runner must be created on its owned IO thread")
  private val connection = verified.connection
  private val ownerThread = Thread.currentThread()
  private val quotedSchema = MySqlFencedTransactionSupport.quote(expectation.schema)
  private val businessRows = new MySqlBusinessRows(connection, expectation.schema)
  private var activeDriftWriteReadOwner: MySqlFencedTransaction.DriftWriteReadOwner = null
  private var activeInactiveDomainWriteReadOwner: MySqlFencedTransaction.InactiveDomainWriteReadOwner = null
  private var activeModeWriteReadOwner: MySqlFencedTransaction.ModeWriteReadOwner = null
  private var activeControlledDebugWriteReadOwner: MySqlFencedTransaction.ControlledDebugWriteReadOwner = null
  private var activeNormalRecallWriteReadOwner: MySqlFencedTransaction.NormalRecallWriteReadOwner = null
  private val retainedNormalRecallMaterializations = scala.collection.mutable.LinkedHashMap.empty[IoOperationTag, MySqlNormalRecallMaterialization]
  private val retainedNormalRecallCommitted = scala.collection.mutable.LinkedHashMap.empty[IoOperationTag, MySqlFencedTransaction.NormalRecallCommitted]
  private val completedNormalRecallPrepares = scala.collection.mutable.HashSet.empty[IoOperationTag]
  private val retainedRequestReadTags=scala.collection.mutable.HashSet.empty[IoOperationTag]
  private final case class RestrictedSourceSessionBinding(
      session: MySqlDeploymentReceiptSession,
      proof: MySqlDeploymentReceiptSession.RestrictedRecall5,
      publication: MySqlDeploymentReceiptSession.RestrictedPublication5,
      originalRoot: MySqlFormat3ReadBudget,
      actualLedger: MySqlFormat3AllocationLedger,
      readerLease: MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      link: MySqlOwnedIoAdapter.RecallRecoveryLink
  )
  private var restrictedSourceSessionBinding: RestrictedSourceSessionBinding = null
  private var partialRestrictedSourceParent: MySqlFencedTransaction.RestrictedSourceParent5 = null
  private val retainedRestrictedSourceParents = new java.util.IdentityHashMap[
    MySqlFencedTransaction.RestrictedSourceParent5, java.lang.Boolean]()
  private val retainedRestrictedSourceParentsByReference = new java.util.IdentityHashMap[
    MySqlNormalRecallRecoveryReference, MySqlFencedTransaction.RestrictedSourceParent5]()
  private final class SourceReservePlanAttempt(
      var exactParent: MySqlFencedTransaction.RestrictedSourceParent5,
      var exactRoot: MySqlFormat3ReadBudget,
      var actualLedger: MySqlFormat3AllocationLedger,
      var exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      var allocation: MySqlFencedTransaction.SourceReservePlanAllocation
  ):
    var building = true
    var plan: MySqlFencedTransaction.SourceReservePlan5 = null
    var data: MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData = null
    var reader: MySqlDeploymentReceiptReadOwner = null
    var failure: Throwable = null
  private val sourceReservePlanAttempts = new java.util.IdentityHashMap[
    MySqlOwnedIoAdapter.RestrictedSourceReserveIntent, SourceReservePlanAttempt]()
  private def sourceReservePlanAttempt(intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent): SourceReservePlanAttempt =
    this.synchronized { if (intent == null) null else sourceReservePlanAttempts.get(intent) }
  private def registerSourceReservePlanAttempt(intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      attempt: SourceReservePlanAttempt): Unit = this.synchronized {
    if (intent == null || attempt == null || sourceReservePlanAttempts.containsKey(intent)) then
      throw new IllegalStateException("unique source RESERVE plan attempt registration required")
    sourceReservePlanAttempts.put(intent, attempt)
  }
  private def removeSourceReservePlanAttempt(intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      attempt: SourceReservePlanAttempt): Unit = this.synchronized {
    if (intent == null || attempt == null || (sourceReservePlanAttempts.get(intent) ne attempt)) then
      throw new IllegalStateException("exact source RESERVE plan attempt required for retirement")
    sourceReservePlanAttempts.remove(intent)
  }
  private def retainSourceReservePlan(plan: MySqlFencedTransaction.SourceReservePlan5): Unit = this.synchronized {
    if (plan == null || retainedSourceReservePlans.containsKey(plan)) then
      throw new IllegalStateException("unique source RESERVE plan retention required")
    retainedSourceReservePlans.put(plan, java.lang.Boolean.TRUE)
  }
  private def sourceReservePlanIsRetained(plan: MySqlFencedTransaction.SourceReservePlan5): Boolean =
    this.synchronized { plan != null && retainedSourceReservePlans.containsKey(plan) }
  private def retireSourceReservePlan(plan: MySqlFencedTransaction.SourceReservePlan5): Unit = this.synchronized {
    if (plan == null || !retainedSourceReservePlans.containsKey(plan)) then
      throw new IllegalStateException("exact retained source RESERVE plan required for retirement")
    retainedSourceReservePlans.remove(plan)
  }
  private var partialSourceReservePlanAttempt: SourceReservePlanAttempt = null
  private var partialSourceReservePlanDataChild: MySqlFormat3ReadBudget = null
  private var partialSourceReservePlan: MySqlFencedTransaction.SourceReservePlan5 = null
  private var sourceReservePlanningFailure: Throwable = null
  private val retainedSourceReservePlans = new java.util.IdentityHashMap[
    MySqlFencedTransaction.SourceReservePlan5, java.lang.Boolean]()

  /** 原操作 S-read 使用专用单行 root；实际 reader 的一次尝试权不与 Full26 preview 混用。 */
  private[mysql] def executeOriginalDeploymentRead(command:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation.FindOriginalDeploymentOperation):Try[Option[MySqlOriginalDeploymentOperation]] =
    val rollbackOnly=new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
      case Some(nested)=>Failure(nested)
      case None=>
        var primary:Throwable=null
        var cleanupFailed=false
        var transactionAttempted=false
        var rolledBack=false
        var oldIsolation=Option.empty[Int]
        var oldReadOnly=Option.empty[Boolean]
        var result=Option.empty[Option[MySqlOriginalDeploymentOperation]]
        def cleanup(body: =>Unit):Unit =
          try body
          catch case cause:Throwable=>
            cleanupFailed=true
            if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
        try
          if expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 || command==null || operation==null ||
              Thread.currentThread()!=ownerThread || !connection.getAutoCommit then
            throw new SQLNonTransientConnectionException("original deployment S-read requires original idle schema5 worker")
          val plan=command.operationPlan()
          command.requireCurrent(scope,plan)
          plan.requireSingle(operation,exactPlanningProfile)
          val admission=coordinator.requireBusinessAdmission(verified,scope)
          if command.requireBusinessAdmission(scope,plan) ne admission then
            throw new SQLNonTransientConnectionException("original deployment S-read lost exact admission")
          val held=command.requireHeldSessionBarrier(scope,plan)
          held.readmit(verified,expectation,scope)
          val root=plan.retainedHandle.getOrElse(throw new SQLDataException("original S-read has no retained handle"))
            .issuer.rootForOriginal(plan)
          root.requireDeploymentOriginalReadLayout()
          oldIsolation=Some(connection.getTransactionIsolation)
          oldReadOnly=Some(connection.isReadOnly)
          connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
          connection.setReadOnly(true)
          transactionAttempted=true
          connection.setAutoCommit(false)
          val reader=new MySqlDeploymentOriginalReadJdbc(verified,scope,expectation.schema,exactPlanningProfile)
          result=Some(reader.readWithinOwnedTransaction(operation,root))
          held.readmit(verified,expectation,scope)
          if coordinator.requireBusinessAdmission(verified,scope) ne admission then
            throw new SQLNonTransientConnectionException("original deployment S-read admission changed")
        catch case cause:Throwable=>primary=cause
        if transactionAttempted then cleanup { connection.rollback();rolledBack=true }
        else rolledBack=true
        if rolledBack then
          if transactionAttempted then cleanup {
            connection.setAutoCommit(true)
            if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("original S-read auto-commit restoration failed")
          }
          // 每个setter即使抛错也可能已改变连接；以原getter值恢复并确认，不假定未修改。
          oldReadOnly.foreach { previous=>cleanup {
            connection.setReadOnly(previous)
            if connection.isReadOnly!=previous then throw new SQLNonTransientConnectionException("original S-read read-only restoration failed")
          }}
          oldIsolation.foreach { previous=>cleanup {
            connection.setTransactionIsolation(previous)
            if connection.getTransactionIsolation!=previous then throw new SQLNonTransientConnectionException("original S-read isolation restoration failed")
          }}
        cleanup { MySqlConnectionTransactionGate.leave(connection,rollbackOnly) }
        if cleanupFailed then Failure(new MySqlPlanningReadCleanupUncertainException(primary))
        else if primary!=null then Failure(primary)
        else result match
          case Some(value)=>Success(value)
          case None=>Failure(new SQLDataException("original deployment S-read produced no result"))

  /** 固定三个preview查询；同原command/plan/root只捕获一次完整26，不开放generic运行器。 */
  private[mysql] def executeRequestReadPreview(command:MySqlRepositoryCommandScope,
      request:MySqlRequestReadPreview):Try[Vector[MySqlRepositoryReply]] =
    val rollbackOnly=new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
      case Some(nested)=>Failure(nested)
      case None=>
        var owner:MySqlFencedTransaction.PlanningReadOwner=null
        var primary:Throwable=null
        var cleanupFailed=false
        var attempted=false
        var rolledBack=false
        var restored=false
        var isolationRead=false
        var oldIsolation=Connection.TRANSACTION_READ_COMMITTED
        var result=Option.empty[Vector[MySqlRepositoryReply]]
        def cleanup(body: =>Unit):Unit =
          try body
          catch case cause:Throwable=>
            cleanupFailed=true
            if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
        try
          if expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 || request==null ||
              command==null || Thread.currentThread()!=ownerThread || !connection.getAutoCommit then
            throw new SQLNonTransientConnectionException("fixed Full26 preview requires original idle purpose command")
          val plan=command.operationPlan()
          val profile=exactPlanningProfile
          command.requireCurrent(scope,plan)
          plan.requireTransaction(request.operations,profile)
          MySqlFormat3ReadDispatcher.requireReads(request.operations)
          val context=MySqlRepositoryOperationContextIdentity.captureCurrent(command,verified,expectation)
          val admission=coordinator.requireBusinessAdmission(verified,scope)
          if command.requireBusinessAdmission(scope,plan) ne admission then
            throw new SQLNonTransientConnectionException("Full26 preview lost original command admission")
          val held=command.requireHeldSessionBarrier(scope,plan)
          oldIsolation=connection.getTransactionIsolation;isolationRead=true
          connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
          attempted=true
          connection.setAutoCommit(false)
          if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
            throw new SQLNonTransientConnectionException("Full26 preview did not establish original RR transaction")
          held.readmit(verified,expectation,scope)
          val manifest=lockManifest();validateManifest(manifest,admission)
          val binding=lockBinding();validateBinding(binding,admission)
          val writer=lockWriter();validateWriterIdentityAndLease(writer,admission)
          owner=MySqlFencedTransaction.afterPlanningLockedRows(this,
            MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
            command,plan,context,held,admission,rollbackOnly,ownerThread,manifest,binding,writer)
          activePlanningReadOwner=owner
          val root=owner.originalPurposeRoot
          root.reserveRetainedMemory(256L,"actual Full26 preview registry and fixed reply references")
          if retainedRequestReadTags.contains(plan.tag) then throw new SQLDataException("duplicate original Full26 preview plan")
          retainedModeProjectionBudgets.retainRoot(plan.tag,root)
          retainedRequestReadTags.add(plan.tag)
          val image=deploymentReceiptSession.capturePurposePlanningImage(owner)
          owner.requireCurrent(verified,scope,expectation.schema,profile,connection)
          MySqlDeploymentReceiptReadOwner.requireManifestData(image,expectation,root)
          MySqlDeploymentReceiptBootstrapGraph.requireBinding(image,PhysicalRecoveryCutoverState.Certified,binding.bindingUuid)
          MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(image,writer)
          val projection=MySqlFormat3QueryProjection.fromDeploymentReceipt(image,image.inputs,root,expectation,admission)
          result=Some(MySqlFormat3ReadDispatcher.execute(request.operations,projection))
          held.readmit(verified,expectation,scope)
          if coordinator.requireBusinessAdmission(verified,scope) ne admission then
            throw new SQLNonTransientConnectionException("Full26 preview admission changed across complete capture")
        catch case cause:Throwable=>primary=cause
        finally activePlanningReadOwner=null
        var outcome:Try[Vector[MySqlRepositoryReply]]=null
        try
          if attempted then cleanup { connection.rollback();rolledBack=true }
          else
            rolledBack=true
            restored=true
          if rolledBack && attempted then cleanup {
            connection.setAutoCommit(true)
            if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("Full26 preview auto-commit restoration failed")
            restored=true
          }
          if restored && isolationRead then cleanup {
            connection.setTransactionIsolation(oldIsolation)
            if connection.getTransactionIsolation!=oldIsolation then throw new SQLNonTransientConnectionException("Full26 preview isolation restoration failed")
          }
          // gate 退出也属于交易清理；成功交接 child 前必须确认它已正常返回。
          cleanup { MySqlConnectionTransactionGate.leave(connection,rollbackOnly) }
          outcome=if cleanupFailed then Failure(new MySqlPlanningReadCleanupUncertainException(primary))
          else if primary!=null then Failure(primary)
          else if owner==null || result.isEmpty then Failure(new SQLDataException("Full26 preview produced no original result"))
          else
            Try { owner.settlePurposeReadAfterTransactionEnd();result.get }
          outcome
        finally ()

  private[mysql] def releaseRetainedRequestRead(tag:IoOperationTag):Unit =
    if Thread.currentThread()!=ownerThread || tag==null then
      throw new IllegalStateException("original Full26 request-read release worker/tag required")
    if retainedRequestReadTags.contains(tag) then
      retainedModeProjectionBudgets.release(tag)
      retainedRequestReadTags.remove(tag)
  private val retainedInactiveDomainMaterializations = scala.collection.mutable.LinkedHashMap.empty[IoOperationTag,MySqlInactiveDomainWorkflowMaterialization]
  private val retainedControlledDebugMaterializations = scala.collection.mutable.LinkedHashMap.empty[IoOperationTag,MySqlControlledDebugWorkflowMaterialization]
  private val retainedControlledDebugPreparedGraphs = scala.collection.mutable.LinkedHashMap.empty[IoOperationTag,MySqlControlledDebugPreparedGraph]
  private var activePlanningReadOwner: MySqlFencedTransaction.PlanningReadOwner = null
  private var deploymentReceiptSession:MySqlDeploymentReceiptSession=null

  private[mysql] def bindDeploymentReceiptSession(actual:MySqlDeploymentReceiptSession,
      ledger:MySqlFormat3AllocationLedger):Unit =
    if deploymentReceiptSession!=null || actual==null || expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 ||
        !suppliedAllocationLedger.exists(_ eq ledger) then
      throw new IllegalStateException("one original schema5 Session runner binding required")
    actual.requirePurposeRunner(this,verified,scope,coordinator,ledger)
    deploymentReceiptSession=actual

  private def exactPlanningProfile:SourceLimitProfile = expectation.profile match
    case MySqlSchemaAuditProfile.Format3=>expectation.format3Inputs.map(_.limitProfile).getOrElse(
      throw new SQLNonTransientConnectionException("original format3 planning inputs missing"))
    case MySqlSchemaAuditProfile.DeploymentReceipt1=>expectation.deploymentReceiptInputs.map(_.limitProfile).getOrElse(
      throw new SQLNonTransientConnectionException("original schema5 planning inputs missing"))
    case _=>throw new SQLNonTransientConnectionException("unsupported business planning family")
  // All simultaneously reachable B2/drift roots use the same actual session allocation cap.
  require(suppliedAllocationLedger != null && !suppliedAllocationLedger.exists(_ == null), "actual optional allocation ledger required")
  private val sharedFormat3AllocationLedger = suppliedAllocationLedger.orElse(
    expectation.format3Inputs.map(inputs => new MySqlFormat3AllocationLedger(inputs.limitProfile.maxRetainedBytes())))
  private val retainedModeProjectionBudgets = new MySqlModeProjectionBudgetRegistry(
    ownerThread, scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)

  private def retainedDriftProjectionBudgets = retainedModeProjectionBudgets
  private def retainedInactiveDomainProjectionBudgets = retainedModeProjectionBudgets
  private def retainedControlledDebugProjectionBudgets = retainedModeProjectionBudgets
  private def retainedNormalRecallProjectionBudgets = retainedModeProjectionBudgets

  private def requireOriginalControlledPreparation(graph: MySqlControlledDebugPreparedGraph,
      actual: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,currentRoot: MySqlFormat3ReadBudget,
      commandScope: MySqlRepositoryCommandScope): Unit =
    if graph==null || commandScope==null || currentRoot==null || (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("controlled commit requires its exact owned private preparation")
    val retained=retainedControlledDebugPreparedGraphs.getOrElse(graph.tag,
      throw new MySqlControlledDebugNoProofException("prepared graph has no retained actual runner issuer"))
    val original=retainedControlledDebugMaterializations.getOrElse(graph.tag,
      throw new MySqlControlledDebugNoProofException("original prepared materialization was lost; no cold restore authority"))
    if (retained ne graph) || (graph.issuerProof.materialization ne original) || original.activation ||
      (graph.originalPreparePlan ne original.plan) || (graph.originalPrepareContext ne original.context) ||
      (graph.request ne original.request) || (graph.issuerProof.commandScope.exactAuditScope() ne scope) ||
      original.plan.workflowUuid!=commandScope.operationPlan().workflowUuid then
      throw new SQLNonTransientConnectionException("foreign or forged prepared graph/ACK lineage")
    val oldHandle=original.plan.retainedHandle.getOrElse(
      throw new MySqlControlledDebugNoProofException("original prepare plan lacks its actual reserved issuer"))
    val currentHandle=commandScope.operationPlan().retainedHandle.getOrElse(
      throw new MySqlControlledDebugNoProofException("current commit plan lacks its actual reserved issuer"))
    if (oldHandle.issuer ne currentHandle.issuer) ||
      (oldHandle.issuer.rootForOriginal(original.plan) ne original.originalRoot) ||
      (currentHandle.issuer.rootForOriginal(commandScope.operationPlan()) ne currentRoot) ||
      original.originalRoot.isClosed || currentRoot.isClosed ||
      !currentRoot.sharesAllocationLedger(original.originalRoot) then
      throw new MySqlControlledDebugNoProofException("original/current plans lost exact same-session retained roots")
    commandScope.requireCurrent(scope,commandScope.operationPlan())
    commandScope.requireHeldSessionBarrier(scope,commandScope.operationPlan()).readmit(verified,expectation,scope)
    if coordinator.requireBusinessAdmission(verified,scope) ne commandScope.requireBusinessAdmission(scope,commandScope.operationPlan()) then
      throw new SQLNonTransientConnectionException("prepared graph continuation lost actual business admission")
    val currentPlan=commandScope.operationPlan()
    val heldBarrier=commandScope.requireHeldSessionBarrier(scope,currentPlan)
    val admission=commandScope.requireBusinessAdmission(scope,currentPlan)
    currentRoot.reserveTemporaryMemory(2048L,"controlled continuation owner and revalidation closure")
    var continuation: MySqlFencedTransaction.ControlledDebugContinuationProof=null
    var continuationFailure: Throwable=null
    try
      continuation=new MySqlFencedTransaction.ControlledDebugContinuationProof(original.originalRoot,currentRoot,original,() => {
        if (Thread.currentThread() ne ownerThread) ||
          !retainedControlledDebugPreparedGraphs.get(graph.tag).exists(_ eq graph) ||
          !retainedControlledDebugMaterializations.get(graph.tag).exists(_ eq original) ||
          (graph.issuerProof.materialization ne original) || (graph.originalPreparePlan ne original.plan) ||
          (graph.originalPrepareContext ne original.context) || (graph.request ne original.request) ||
          (graph.issuerProof.commandScope.exactAuditScope() ne scope) || original.activation ||
          (commandScope.operationPlan() ne currentPlan) || original.plan.workflowUuid!=currentPlan.workflowUuid ||
          (oldHandle.issuer ne currentHandle.issuer) ||
          (oldHandle.issuer.rootForOriginal(original.plan) ne original.originalRoot) ||
          (currentHandle.issuer.rootForOriginal(currentPlan) ne currentRoot) ||
          original.originalRoot.isClosed || currentRoot.isClosed ||
          (original.originalRoot.profile ne currentRoot.profile) || !currentRoot.sharesAllocationLedger(original.originalRoot) then
          throw new MySqlControlledDebugNoProofException("original runner continuation lineage was revoked")
        commandScope.requireCurrent(scope,currentPlan)
        if (commandScope.requireHeldSessionBarrier(scope,currentPlan) ne heldBarrier) ||
          (commandScope.requireBusinessAdmission(scope,currentPlan) ne admission) ||
          (coordinator.requireBusinessAdmission(verified,scope) ne admission) then
          throw new SQLNonTransientConnectionException("runner continuation lost its actual barrier/admission")
      })
      actual match
        case legacy:MySqlFormat3FullSnapshot=>
          original.verifyUsingRoot(currentRoot,Some(continuation))
          MySqlControlledDebugRelations.requireControlledClosure(legacy,graph.request,currentPlan,currentRoot)
          MySqlControlledDebugPreparedComparison.requireOriginal(original.executeTarget,legacy,graph.request,currentRoot,
            original.originalRoot,Some(continuation))
        case deployment:MySqlScriptSchemaSnapshot=>
          original.verify()
          MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(deployment,graph.request,currentPlan,currentRoot)
          MySqlControlledDebugPreparedComparison.requireOriginalDeploymentReceipt(original.deploymentExecuteTarget,
            deployment,graph.request,currentRoot,original.originalRoot,Some(continuation))

    catch
      case primary: Throwable =>
        continuationFailure=primary
        throw primary
    finally
      if continuation != null then continuation.retire()
      try currentRoot.releaseTemporaryMemory(2048L,"controlled continuation owner and revalidation closure")
      catch
        case cleanup: Throwable =>
          if continuationFailure==null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(continuationFailure,cleanup)

  /** 规划输入只在锁定读取到必需的回滚与会话恢复之间有效；单次物化不会返回或保留活动 owner。 */
  private final class InactiveDomainPlanningInputs private[MySqlFencedTransaction] (
      owner: MySqlFencedTransaction.PlanningReadOwner, plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlInactiveDomainUpdateRequest, contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, preimage: MySqlFormat3FullSnapshot
  ):
    private var consumed = false
    def materialize(): MySqlInactiveDomainWorkflowMaterialization =
      if consumed then throw new SQLTransactionRollbackException("inactive locked planning inputs are one-shot")
      consumed = true
      def requireExact(): Unit =
        owner.requireInactiveMaterializationInputs(verified, scope, expectation.schema,
          expectation.format3Inputs.map(_.limitProfile).orNull, connection, plan, request,
          contextIdentity, plannedAt, preimage)
      requireExact()
      val result = MySqlInactiveDomainWorkflowMaterializer.materialize(request, plan, contextIdentity,
        plannedAt, preimage, preimage.allocationBudget)
      if (result.preimage ne preimage) then
        throw new SQLTransactionRollbackException("inactive materialization replaced its actual locked full20 preimage")
      result.verify()
      requireExact()
      result

  private def issueInactiveDomainPlanningInputs(
      owner: MySqlFencedTransaction.PlanningReadOwner, plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlInactiveDomainUpdateRequest, contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, preimage: MySqlFormat3FullSnapshot
  ): InactiveDomainPlanningInputs =
    if owner == null || (activePlanningReadOwner ne owner) || (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("inactive inputs require the active owner from this locked call")
    new InactiveDomainPlanningInputs(owner, plan, request, contextIdentity, plannedAt, preimage)
  private final class DriftExecutionPlanningInputs private[MySqlFencedTransaction] (
      owner: MySqlFencedTransaction.PlanningReadOwner,
      plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime,
      preimage: MySqlModeOperationEffectCodec.Image,
      locatorRevision: Long
  ):
    private var consumed = false

    def materialize(): MySqlDriftWorkflowMaterialization =
      if consumed then throw new SQLTransactionRollbackException("locked mode planning inputs are one-shot")
      consumed = true
      requireInputs()
      val result=preimage match
        case legacy:MySqlFormat3FullSnapshot=>MySqlDriftWorkflowMaterializer.materialize(plan,operation,contextIdentity,
          plannedAt,legacy,locatorRevision)
        case deployment:MySqlScriptSchemaSnapshot=>
          val root=owner.originalRetainedRoot.getOrElse(
            throw new SQLNonTransientConnectionException("Full26 drift planning lost original root"))
          MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,contextIdentity,
            plannedAt,deployment,locatorRevision,root)
      if result.completePreimage ne preimage then
        throw new SQLTransactionRollbackException("mode materialization replaced the actual locked full20 preimage")
      result.verify()
      requireInputs()
      result

    private def requireInputs():Unit=preimage match
      case legacy:MySqlFormat3FullSnapshot=>
        owner.requireMaterializationInputs(verified,scope,expectation.schema,exactPlanningProfile,connection,
          plan,operation,contextIdentity,plannedAt,legacy,locatorRevision)
      case deployment:MySqlScriptSchemaSnapshot=>
        owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,deployment)
        if plannedAt!=owner.plannedAt || locatorRevision<0L then
          throw new SQLTransactionRollbackException("Full26 drift lost original planning timestamp/revision")

  private def issueDriftExecutionPlanningInputs(
      owner: MySqlFencedTransaction.PlanningReadOwner,
      plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime,
      preimage: MySqlModeOperationEffectCodec.Image,
      locatorRevision: Long
  ): DriftExecutionPlanningInputs =
    if owner == null || (activePlanningReadOwner ne owner) || (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("planning inputs require the active owner from this locked call")
    new DriftExecutionPlanningInputs(owner, plan, operation, contextIdentity, plannedAt, preimage, locatorRevision)

  private final class ExecutionPlanningInputs private[MySqlFencedTransaction] (
      owner: MySqlFencedTransaction.PlanningReadOwner,
      plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime,
      preimage: MySqlModeOperationEffectCodec.Image,
      locatorRevision: Long
  ):
    private var consumed = false

    def materialize(): MySqlModeWorkflowMaterialization =
      if consumed then throw new SQLTransactionRollbackException("locked mode planning inputs are one-shot")
      consumed = true
      requireInputs()
      val originalRoot = owner.originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("mode witness requires its actual session-issued original planning root"))
      originalRoot.reserveRetainedMemory(512L, "registered original mode witness draft")
      val draft = new MySqlModeOriginalWitnessDraft(plan, operation, contextIdentity, originalRoot)
      retainedModeProjectionBudgets.retainWitnessDraft(plan.tag, draft)
      try
        val result = preimage match
          case legacy:MySqlFormat3FullSnapshot=>MySqlModeWorkflowMaterializer.materialize(plan,operation,contextIdentity,
            plannedAt,legacy,locatorRevision,draft)
          case deployment:MySqlScriptSchemaSnapshot=>MySqlModeWorkflowMaterializer.materializeDeploymentReceipt(plan,operation,
            contextIdentity,plannedAt,deployment,locatorRevision,originalRoot,draft)
        result.originalLocatorRevision = locatorRevision
        result.originalWitnessDraft = draft
        if result.completePreimage ne preimage then
          throw new SQLTransactionRollbackException("mode materialization replaced the actual locked full20 preimage")
        result.verify()
        requireInputs()
        result
      catch
        case cause: Throwable =>
          draft.recordFailure(cause)
          throw cause

    private def requireInputs():Unit = preimage match
      case legacy:MySqlFormat3FullSnapshot=>
        owner.requireMaterializationInputs(verified,scope,expectation.schema,exactPlanningProfile,connection,
          plan,operation,contextIdentity,plannedAt,legacy,locatorRevision)
      case deployment:MySqlScriptSchemaSnapshot=>
        owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,deployment)
        if plannedAt!=owner.plannedAt || locatorRevision<0L then
          throw new SQLTransactionRollbackException("Full26 mode materialization lost original planning timestamp/revision")

  private def issueExecutionPlanningInputs(
      owner: MySqlFencedTransaction.PlanningReadOwner,
      plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime,
      preimage: MySqlModeOperationEffectCodec.Image,
      locatorRevision: Long
  ): ExecutionPlanningInputs =
    if owner == null || (activePlanningReadOwner ne owner) || (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("planning inputs require the active owner from this locked call")
    new ExecutionPlanningInputs(owner, plan, operation, contextIdentity, plannedAt, preimage, locatorRevision)

  /**
   * 在 Main 已保留的原命令身份下取得一次只读 full20 planning 输入。该路径不提交或改写数据库；
   * 它只在实际 M→B→W 锁与完整观察一致后，用同一次 W 锁的 UTC_TIMESTAMP(6) 生成固定计划。
   */
  private[mysql] def planModeWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation
  ): Try[MySqlModeWorkflowPlanning] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("mode planning must run on its captured owned IO thread"))
          else if commandScope == null || operation == null then
            Failure(new IllegalArgumentException("exact command scope and original mode request are required"))
          else
            try Success(planModeWorkflowOwned(commandScope, operation, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private[mysql] def requireActivePlanningReadOwner(owner: MySqlFencedTransaction.PlanningReadOwner): Unit =
    if owner == null || (activePlanningReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("format3 planning-read owner is stale or not issued by this locked call")

  private[mysql] def requireActiveModeWriteReadOwner(owner: MySqlFencedTransaction.ModeWriteReadOwner): Unit =
    if owner == null || (activeModeWriteReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("format3 mode-write read owner is stale or not issued by this locked phase")

  private[mysql] def requireActiveNormalRecallWriteReadOwner(owner: MySqlFencedTransaction.NormalRecallWriteReadOwner): Unit =
    if owner == null || (activeNormalRecallWriteReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("normal recall debug read owner is stale or foreign")

  /** Bind only the parallel, original schema5 restricted runner and its true locked Full26 reader. */
  private[mysql] def bindRestrictedRecallSession(
      session: MySqlDeploymentReceiptSession,
      proof: MySqlDeploymentReceiptSession.RestrictedRecall5,
      publication: MySqlDeploymentReceiptSession.RestrictedPublication5,
      originalRoot: MySqlFormat3ReadBudget,
      actualLedger: MySqlFormat3AllocationLedger,
      readerLease: MySqlDeploymentReceiptSession.RestrictedSourceReadLease5
  ): Unit =
    if (Thread.currentThread() ne ownerThread) || expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1 ||
        session == null || proof == null || publication == null || originalRoot == null || actualLedger == null || readerLease == null then
      throw new SQLNonTransientConnectionException("exact original restricted schema5 runner binding is required")
    if restrictedSourceSessionBinding != null then
      throw new SQLNonTransientConnectionException("restricted source runner binding is one-shot")
    if sharedFormat3AllocationLedger.forall(_ ne actualLedger) || originalRoot.isClosed ||
        (originalRoot.profile ne exactPlanningProfile) then
      throw new SQLNonTransientConnectionException("restricted source runner lost the exact original Full26 root/profile/ledger")
    originalRoot.requireLedgerIdentity(actualLedger)
    publication.requireCurrent()
    val link = publication.recovery
    proof.withCurrent(expectation, coordinator) { (actualVerified, actualScope, token, receipt) =>
      if (actualVerified ne verified) || (actualScope ne scope) || token == null || receipt == null ||
          link == null || link.identity.bootId != actualScope.bootUuid ||
          link.identity.bootGeneration != actualScope.bootGeneration ||
          link.identity.connectionGeneration != actualScope.connectionGeneration ||
          link.bindingId != token.bindingUuid || link.writerEpoch != token.writerEpoch ||
          receipt.bindingUuid != token.bindingUuid || receipt.writerEpoch != token.writerEpoch ||
          publication.tag == null || publication.repositoryEpochAtSubmission < 0L then
        throw new SQLNonTransientConnectionException("restricted source binding differs from the exact original Session/fence/link")
      session.requireRestrictedSourceRunner(this, proof, publication, originalRoot, actualLedger, readerLease)
    }
    originalRoot.reserveRetainedMemory(512L, "restricted source Session, Full26 reader lease and original lineage BEFORE retention")
    try
      restrictedSourceSessionBinding = RestrictedSourceSessionBinding(
        session, proof, publication, originalRoot, actualLedger, readerLease, link)
    catch
      case cause: Throwable =>
        try originalRoot.releaseRetainedMemory(512L, "failed restricted source Session binding allocation")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  /**
   * Re-read under the true Session Full26 lock and retain only a history-checked parent P2. A caller
   * never supplies a boolean, snapshot, writer row or parent operation identity as qualification.
   */
  private[mysql] def captureRestrictedSourceParent(
      link: MySqlOwnedIoAdapter.RecallRecoveryLink,
      reference: MySqlNormalRecallRecoveryReference
  ): MySqlFencedTransaction.RestrictedSourceParent5 =
    if (Thread.currentThread() ne ownerThread) || link == null || reference == null then
      throw new SQLNonTransientConnectionException("original restricted source Session and exact retained parent reference required")
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    if (bound.link ne link) || !link.references.exists(_ eq reference) then
      throw new SQLNonTransientConnectionException("foreign original Recall link/reference cannot qualify a source parent")
    val retained = retainedRestrictedSourceParentsByReference.get(reference)
    if retained != null then
      retained.requireRetained()
      return retained
    bound.publication.requireCurrent()
    bound.originalRoot.requireLedgerIdentity(bound.actualLedger)
    bound.session.requireRestrictedSourceRunner(this, bound.proof, bound.publication,
      bound.originalRoot, bound.actualLedger, bound.readerLease)
    bound.proof.withCurrent(expectation, coordinator) { (actualVerified, actualScope, token, receipt) =>
      if (actualVerified ne verified) || (actualScope ne scope) || receipt == null ||
          link.bindingId != token.bindingUuid || link.writerEpoch != token.writerEpoch ||
          link.identity.bootId != token.bootUuid || link.identity.bootGeneration != token.bootGeneration ||
          link.identity.connectionGeneration != token.connectionGeneration then
        throw new SQLNonTransientConnectionException("restricted source parent lost its exact Session/fence lineage")
      bound.readerLease.withLockedFull26(this, bound.originalRoot, bound.actualLedger) {
        (reader, current, anchor, writer) =>
          if reader == null || current == null || anchor == null || writer == null ||
              current.inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
              (current.inputs.sourceLimitProfile ne bound.originalRoot.profile) ||
              !current.allocationBudget.sharesAllocationLedger(bound.originalRoot) ||
              !anchor.allocationBudget.sharesAllocationLedger(bound.originalRoot) ||
              current.schema != expectation.schema || current.connection.serverUuid != expectation.serverUuid ||
              current.connection.serverVersion != expectation.serverVersion || current.connection.catalog != expectation.schema ||
              current.observationScope.bootUuid != actualScope.bootUuid ||
              current.observationScope.bootGeneration != actualScope.bootGeneration ||
              current.observationScope.connectionGeneration != actualScope.connectionGeneration ||
              writer.bindingUuid != token.bindingUuid || writer.writerEpoch != token.writerEpoch ||
              !writer.ownerBootUuid.contains(token.bootUuid) || writer.leaseUntil != token.leaseUntil ||
              !writer.leaseUntil.isAfter(writer.databaseNow) then
            throw new SQLNonTransientConnectionException("locked source-parent Full26 changed schema, root, W, or lease identity")
          MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(current, writer)
          val history = MySqlNormalRecallRecovery.verifyCompleteReceiptHistoryData(current, current, anchor, bound.originalRoot)
          val inventories = MySqlNormalRecallRecovery.discover(current, token.bindingUuid, Some(bound.originalRoot))
          val matches = inventories.filter(_.workflow == reference.workflow)
          if matches.size != 1 then
            throw new SQLDataException("restricted source parent workflow is absent or ambiguous in original Full26")
          val inventory = matches.head
          if inventory.pod != reference.pod || inventory.prepare != reference.prepare ||
              inventory.checkpoint != reference.checkpoint then
            throw new SQLDataException("restricted source parent no longer matches its exact retained P/C reference")
          val parentP2 = inventory.cursor match
            case MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical(operation) => operation.operationUuid
            case _ => throw new SQLTransactionRollbackException("restricted source parent has no current committed P2 or has an unresolved successor")
          val operationTable = current.relation("fp_recovery_operation").rows
          val p2Rows = operationTable.filter(row => MySqlModeOperationEffectCodec.uuidEquals(
            MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "binding_uuid"), token.bindingUuid) &&
            MySqlModeOperationEffectCodec.uuidEquals(
              MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "workflow_uuid"), reference.workflow) &&
            MySqlModeOperationEffectCodec.uuidEquals(
              MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "operation_uuid"), parentP2))
          if p2Rows.size != 1 then throw new SQLDataException("current parent P2 row is absent or ambiguous")
          val p2 = p2Rows.head
          def p2Value(column: String): MySqlFormat3Cell =
            MySqlModeOperationEffectCodec.value(p2, "fp_recovery_operation", column)
          if p2Value("step") != MySqlFormat3Cell.Text("RECALL_PREPARE") ||
              p2Value("operation_state") != MySqlFormat3Cell.Text("COMMITTED") ||
              !MySqlModeOperationEffectCodec.uuidEquals(p2Value("pod_uuid"), reference.pod) then
            throw new SQLDataException("current parent P2 is not its exact committed original Recall prepare")
          val podReservations = current.relation("fp_recovery_reservation").rows.filter { row =>
            MySqlModeOperationEffectCodec.value(row, "fp_recovery_reservation", "resource_kind") == MySqlFormat3Cell.Text("POD") &&
              MySqlModeOperationEffectCodec.uuidEquals(
                MySqlModeOperationEffectCodec.value(row, "fp_recovery_reservation", "binding_uuid"), token.bindingUuid) &&
              MySqlModeOperationEffectCodec.uuidEquals(
                MySqlModeOperationEffectCodec.value(row, "fp_recovery_reservation", "resource_pod_uuid"), reference.pod)
          }
          if podReservations.size != 1 then throw new SQLDataException("current parent POD reservation is absent or ambiguous")
          val podReservation = podReservations.head
          if MySqlModeOperationEffectCodec.value(podReservation, "fp_recovery_reservation", "reservation_state") !=
                MySqlFormat3Cell.Text("HELD") ||
              !MySqlModeOperationEffectCodec.uuidEquals(
                MySqlModeOperationEffectCodec.value(podReservation, "fp_recovery_reservation", "workflow_uuid"), reference.workflow) ||
              !MySqlModeOperationEffectCodec.uuidEquals(
                MySqlModeOperationEffectCodec.value(podReservation, "fp_recovery_reservation", "operation_uuid"), parentP2) ||
              !MySqlModeOperationEffectCodec.uuidEquals(
                MySqlModeOperationEffectCodec.value(podReservation, "fp_recovery_reservation", "last_mutation_operation_uuid"), parentP2) then
            throw new SQLDataException("original parent POD reservation is no longer HELD at exact P2")
          if retainedRestrictedSourceParentsByReference.containsKey(reference) then
            throw new SQLNonTransientConnectionException("same original Recall reference already has its exact retained source parent")
          bound.originalRoot.reserveRetainedMemory(512L, "history-qualified restricted source parent evidence BEFORE retention")
          var evidence: MySqlFencedTransaction.RestrictedSourceParent5 = null
          try
            evidence = new MySqlFencedTransaction.RestrictedSourceParent5(this, bound.session, bound.proof,
              bound.publication, bound.readerLease, bound.originalRoot, bound.actualLedger, link, reference,
              current, anchor, reader, history, token.bindingUuid, reference.workflow, parentP2, reference.pod)
            partialRestrictedSourceParent = evidence
            retainedRestrictedSourceParents.put(evidence, java.lang.Boolean.TRUE)
            retainedRestrictedSourceParentsByReference.put(reference, evidence)
            partialRestrictedSourceParent = null
            evidence
          catch
            case cause: Throwable =>
              if partialRestrictedSourceParent == null then
                try bound.originalRoot.releaseRetainedMemory(512L, "failed restricted source parent allocation")
                catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
              throw cause
      }
    }

  private[mysql] def requireRetainedRestrictedSourceParent(
      parent: MySqlFencedTransaction.RestrictedSourceParent5
  ): Unit =
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    if (Thread.currentThread() ne ownerThread) || parent == null ||
        (parent.issuer ne this) || !retainedRestrictedSourceParents.containsKey(parent) ||
        (parent.session ne bound.session) || (parent.proof ne bound.proof) ||
        (parent.publication ne bound.publication) || (parent.readerLease ne bound.readerLease) ||
        (parent.originalRoot ne bound.originalRoot) || (parent.actualLedger ne bound.actualLedger) ||
        (parent.link ne bound.link) then
      throw new SQLNonTransientConnectionException("foreign, released or stale restricted source parent proof")
    parent.publication.requireCurrent()
    parent.session.requireRestrictedSourceRunner(this, parent.proof, parent.publication,
      parent.originalRoot, parent.actualLedger, parent.readerLease)
    parent.proof.withCurrent(expectation, coordinator) { (actualVerified, actualScope, token, _) =>
      if (actualVerified ne verified) || (actualScope ne scope) || token.bindingUuid != parent.binding ||
          token.bootUuid != scope.bootUuid || token.writerEpoch <= 0L then
        throw new SQLNonTransientConnectionException("restricted source parent no longer matches the current fenced Session")
    }

  /**
   * Build one immutable source-child RESERVE plan from a second, fresh locked Full26 read. The
   * reader lease settles its rollback/session restoration before this method issues a usable plan.
   */
  private[mysql] def prepareRestrictedSourceReservePlan(
      parent: MySqlFencedTransaction.RestrictedSourceParent5,
      intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      originalRoot: MySqlFormat3ReadBudget
  ): MySqlFencedTransaction.SourceReservePlan5 =
    if (Thread.currentThread() ne ownerThread) || parent == null || intent == null || originalRoot == null then
      throw new SQLNonTransientConnectionException("original worker, restricted parent and source RESERVE intent required")
    intent.requireSourcePlanDropNotIssued()
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    parent.requireRetained()
    if (parent.issuer ne this) || (parent.originalRoot ne originalRoot) ||
        (parent.actualLedger ne bound.actualLedger) || (parent.readerLease ne bound.readerLease) ||
        (intent.exactLink ne parent.link) || (intent.parentReference ne parent.reference) ||
        (bound.link ne parent.link) || (bound.originalRoot ne originalRoot) || originalRoot.isClosed ||
        originalRoot.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("source RESERVE intent differs from the exact retained parent/root/session")
    originalRoot.requireLedgerIdentity(bound.actualLedger)

    val exactDomain = intent.exactDomainObservation
    val expectedSourceObservation = intent.expectedSourceObservation
    val loadedRoot = intent.loadedRootPlan
    val locator = intent.sourceCarrierLocator
    val protectionData = intent.sourceProtectionData()
    intent.requireOriginalReservePlanBinding(parent.link, parent.reference, originalRoot,
      exactDomain, expectedSourceObservation)
    if (exactDomain == null || loadedRoot == null || locator == null || protectionData == null ||
        !loadedRoot.loaded() || locator.chunk == null || locator.chunk.dimension != loadedRoot.dimension() ||
        !exactDomain.roots().stream().anyMatch(root => root eq loadedRoot) || !protectionData.containsExactCarrier(locator) ||
        locator.binding != parent.binding || locator.generation < 0L ||
        !MySqlNormalRecallRequest.nonZero(intent.childWorkflow) ||
        !MySqlNormalRecallRequest.nonZero(intent.reserveOperation) ||
        !MySqlNormalRecallRequest.nonZero(intent.plannedWrite) || intent.writeGeneration < 0L) then
      throw new SQLDataException("source RESERVE locator/loaded root/parent binding is incomplete or foreign")

    val existing = sourceReservePlanAttempt(intent)
    if (existing != null) then
      if (existing.exactParent ne parent) then
        throw new SQLNonTransientConnectionException("source RESERVE intent was already bound to another exact parent")
      if (existing.failure != null) then throw existing.failure
      if (existing.plan != null) then
        existing.plan.requireRetained()
        return existing.plan
      if (existing.building) then
        throw new SQLNonTransientConnectionException("exact source RESERVE read-plan request is already in progress")
    if (sourceReservePlanningFailure != null) then throw sourceReservePlanningFailure
    var attempt: SourceReservePlanAttempt = null
    try
      val dataChild = originalRoot.newCanonicalDataScope()
      partialSourceReservePlanDataChild = dataChild
      // 原有 512+1280 逻辑 metadata 预留整体转为此 child 的计划所有者费用。
      dataChild.reserveRetainedMemory(1792L,
        "exact source RESERVE plan attempt/map/Plan5/Closure5 partial metadata BEFORE allocation")
      val allocation = new MySqlFencedTransaction.SourceReservePlanAllocation(
        this, parent, originalRoot, bound.actualLedger, ownerThread, intent, dataChild)
      attempt = new SourceReservePlanAttempt(parent, originalRoot, bound.actualLedger, intent, allocation)
      partialSourceReservePlanAttempt = attempt
      registerSourceReservePlanAttempt(intent, attempt)
      partialSourceReservePlanDataChild = null
    catch
      case cause: Throwable =>
        if (sourceReservePlanningFailure == null) sourceReservePlanningFailure = cause
        else if (sourceReservePlanningFailure ne cause) MySqlOwnedSessionLifecycle.suppress(sourceReservePlanningFailure, cause)
        throw cause

    try
      parent.publication.requireCurrent()
      parent.session.requireRestrictedSourceRunner(this, parent.proof, parent.publication,
        originalRoot, bound.actualLedger, bound.readerLease)
      val settledData = bound.proof.withCurrent(expectation, coordinator) {
        (actualVerified, actualScope, token, receipt) =>
          if ((actualVerified ne verified) || (actualScope ne scope) || receipt == null ||
              parent.binding != token.bindingUuid || parent.link.bindingId != token.bindingUuid ||
              parent.link.writerEpoch != token.writerEpoch || parent.link.identity.bootId != token.bootUuid ||
              parent.link.identity.bootGeneration != token.bootGeneration ||
              parent.link.identity.connectionGeneration != token.connectionGeneration) then
            throw new SQLNonTransientConnectionException("source RESERVE lost its exact original writer/session lineage")
          bound.readerLease.withLockedFull26(this, originalRoot, bound.actualLedger) {
            (reader, freshCurrent, anchor, freshWriter) =>
              attempt.reader = reader
              if (reader == null || freshCurrent == null || anchor == null || freshWriter == null ||
                  freshCurrent.inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
                  (freshCurrent.inputs.sourceLimitProfile ne originalRoot.profile) ||
                  !freshCurrent.allocationBudget.sharesAllocationLedger(originalRoot) ||
                  !anchor.allocationBudget.sharesAllocationLedger(originalRoot) ||
                  freshCurrent.schema != expectation.schema ||
                  freshCurrent.connection.serverUuid != expectation.serverUuid ||
                  freshCurrent.connection.serverVersion != expectation.serverVersion ||
                  freshCurrent.connection.catalog != expectation.schema ||
                  freshCurrent.observationScope.bootUuid != actualScope.bootUuid ||
                  freshCurrent.observationScope.bootGeneration != actualScope.bootGeneration ||
                  freshCurrent.observationScope.connectionGeneration != actualScope.connectionGeneration ||
                  freshWriter.bindingUuid != token.bindingUuid || freshWriter.writerEpoch != token.writerEpoch ||
                  !freshWriter.ownerBootUuid.contains(token.bootUuid) || freshWriter.leaseUntil != token.leaseUntil ||
                  !freshWriter.leaseUntil.isAfter(freshWriter.databaseNow)) then
                throw new SQLNonTransientConnectionException("fresh source RESERVE Full26 changed schema/root/W/lease identity")
              MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(freshCurrent, freshWriter)
              val freshHistory = MySqlNormalRecallRecovery.verifyCompleteReceiptHistoryData(
                freshCurrent, freshCurrent, anchor, originalRoot, attempt.allocation, parent, bound.actualLedger, intent)
              val actualPlan = MySqlNormalRecallSourceWorkflow.prepareRestrictedSourceReservePlan(
                intent, parent, freshCurrent, anchor, freshWriter, freshHistory, freshCurrent.inputs,
                originalRoot, attempt.allocation)
              if (actualPlan == null || actualPlan.request == null || actualPlan.before == null ||
                  actualPlan.history == null || actualPlan.effectClosure == null ||
                  (actualPlan.before ne freshCurrent) || !actualPlan.before.allocationBudget.sharesAllocationLedger(originalRoot) ||
                  (actualPlan.history ne freshHistory) ||
                  !actualPlan.effectClosure.containsCarrier(locator) ||
                  actualPlan.effectClosure.sourceProtectionIndex == null ||
                  !actualPlan.effectClosure.sourceProtectionIndex.carriers.contains(locator) ||
                  actualPlan.effectClosure.parent.pod != parent.parentPod ||
                  actualPlan.effectClosure.parent.recallWorkflow != parent.parentRecallWorkflow ||
                  actualPlan.effectClosure.parent.prepare != parent.reference.prepare ||
                  actualPlan.effectClosure.parent.checkpoint != parent.reference.checkpoint ||
                  actualPlan.effectClosure.parent.parentP2 != parent.parentP2Operation) then
                throw new SQLDataException("fresh source RESERVE worker produced incomplete request/before/history/closure DATA")
              val request = actualPlan.request
              if (request.binding != parent.binding || request.childWorkflow != intent.childWorkflow ||
                  request.parentRecallWorkflow != parent.parentRecallWorkflow ||
                  request.parentP2Operation != parent.parentP2Operation ||
                  request.reserveOperation != intent.reserveOperation || request.plannedWrite != intent.plannedWrite ||
                  request.writeGeneration != intent.writeGeneration || request.carrierIdentity == null ||
                  request.carrierIdentity.resourceKey == null ||
                  (request.carrierIdentity.resourceKey.exactProfile ne originalRoot.profile)) then
                throw new SQLDataException("fresh source RESERVE request differs from its exact intent/parent/profile")
              attempt.data = actualPlan
              actualPlan
          }
      }
      if (settledData == null || (settledData ne attempt.data) || attempt.reader == null) then
        throw new SQLNonTransientConnectionException("fresh source RESERVE read-plan was not retained after reader settlement")
      attempt.allocation.withPlanDataBudget(parent, originalRoot, bound.actualLedger, intent) { planDataBudget =>
        if planDataBudget.isClosed || !planDataBudget.isCanonicalChildOf(originalRoot) ||
            !planDataBudget.sharesAllocationLedger(originalRoot) then
          throw new SQLNonTransientConnectionException("registered open source RESERVE plan DATA child required")
        val issuedPlan = new MySqlFencedTransaction.SourceReservePlan5(this, parent, intent,
          attempt.reader, originalRoot, bound.actualLedger, attempt.allocation, settledData)
        partialSourceReservePlan = issuedPlan
        val issuedClosure = new MySqlFencedTransaction.SourceReserveEffectClosure5(this, issuedPlan,
          originalRoot, originalRoot.profile, settledData.effectClosure)
        issuedPlan.attachEffectClosure(issuedClosure)
        retainSourceReservePlan(issuedPlan)
        issuedPlan.markIssued()
        attempt.plan = issuedPlan
        attempt.building = false
        partialSourceReservePlan = null
        partialSourceReservePlanAttempt = null
        issuedPlan
      }
    catch
      case cause: Throwable =>
        attempt.failure = cause
        attempt.building = false
        if (sourceReservePlanningFailure == null) sourceReservePlanningFailure = cause
        else if (sourceReservePlanningFailure ne cause) MySqlOwnedSessionLifecycle.suppress(sourceReservePlanningFailure, cause)
        throw cause

  private[mysql] def requireSourceReservePlanAllocationForUse(
      allocation: MySqlFencedTransaction.SourceReservePlanAllocation,
      parent: MySqlFencedTransaction.RestrictedSourceParent5,
      originalRoot: MySqlFormat3ReadBudget,
      actualLedger: MySqlFormat3AllocationLedger,
      intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent
  ): Unit =
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt = sourceReservePlanAttempt(intent)
    if (Thread.currentThread() ne ownerThread) || allocation == null || parent == null || intent == null ||
        originalRoot == null || actualLedger == null || (actualLedger ne bound.actualLedger) ||
        (originalRoot ne bound.originalRoot) || (originalRoot ne parent.originalRoot) ||
        (parent.issuer ne this) || (parent.actualLedger ne actualLedger) ||
        (intent.exactLink ne parent.link) || (intent.parentReference ne parent.reference) ||
        attempt == null || !attempt.building || (attempt.exactParent ne parent) ||
        (attempt.exactRoot ne originalRoot) || (attempt.actualLedger ne actualLedger) ||
        (attempt.exactIntent ne intent) || (attempt.allocation ne allocation) || attempt.plan != null ||
        attempt.failure != null then
      throw new SQLNonTransientConnectionException("unregistered, foreign, or settled source RESERVE DATA allocation holder")
    allocation.requireDropBinding(this, parent, originalRoot, actualLedger, intent, ownerThread)

  /** settled plan 的纯 DATA 借用只核原登记图，不复用已消费的 read-current/mutation 资格。 */
  private[MySqlFencedTransaction] def requireRetainedSourceReservePlanAllocationForUse(
      allocation: MySqlFencedTransaction.SourceReservePlanAllocation,
      parent: MySqlFencedTransaction.RestrictedSourceParent5,
      originalRoot: MySqlFormat3ReadBudget,
      actualLedger: MySqlFormat3AllocationLedger,
      intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      exactData: MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData
  ): Unit =
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt = sourceReservePlanAttempt(intent)
    if (Thread.currentThread() ne ownerThread) || allocation == null || parent == null || intent == null ||
        originalRoot == null || actualLedger == null || exactData == null ||
        (actualLedger ne bound.actualLedger) || (originalRoot ne bound.originalRoot) ||
        (originalRoot ne parent.originalRoot) || (parent.issuer ne this) ||
        (parent.actualLedger ne actualLedger) || (parent.readerLease ne bound.readerLease) ||
        (intent.exactLink ne parent.link) || (intent.parentReference ne parent.reference) ||
        attempt == null || attempt.building || attempt.failure != null ||
        (attempt.exactParent ne parent) || (attempt.exactRoot ne originalRoot) ||
        (attempt.actualLedger ne actualLedger) || (attempt.exactIntent ne intent) ||
        (attempt.allocation ne allocation) || attempt.plan == null || attempt.reader == null ||
        (attempt.data ne exactData) then
      throw new SQLNonTransientConnectionException("exact settled source RESERVE plan DATA registration required")

    parent.requireRetained()
    originalRoot.requireLedgerIdentity(actualLedger)
    bound.session.requireRestrictedSourceReader(parent.readerLease, attempt.reader)

    val plan = attempt.plan
    if !plan.isIssued || (plan.issuer ne this) || !sourceReservePlanIsRetained(plan) ||
        (plan.parent ne parent) || (plan.intent ne intent) || (plan.exactRoot ne originalRoot) ||
        (plan.actualLedger ne actualLedger) || (plan.allocation ne allocation) ||
        (plan.actualReader ne attempt.reader) || (plan.data ne exactData) ||
        (plan.receiptForReplay ne null) || exactData.effectClosure == null then
      throw new SQLNonTransientConnectionException("source RESERVE plan is foreign, detached, revoked, or entering drop")

    val closure = plan.effectClosure()
    if !closure.belongsTo(plan, this, originalRoot, originalRoot.profile) ||
        (closure.data ne exactData.effectClosure) then
      throw new SQLNonTransientConnectionException("exact retained source RESERVE plan closure DATA required")

  /** 原 Session active scope 是事务内借用此 retained DATA 子账户的唯一通道。 */
  private[mysql] def requireActiveRestrictedSourceReservePlanAllocationForUse(
      allocation:MySqlFencedTransaction.SourceReservePlanAllocation,
      parent:MySqlFencedTransaction.RestrictedSourceParent5,
      originalRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,
      intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData,
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      exactChild:MySqlFormat3ReadBudget):Unit =
    val bound=Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt=sourceReservePlanAttempt(intent)
    if (Thread.currentThread() ne ownerThread) || allocation==null || parent==null || originalRoot==null ||
        actualLedger==null || intent==null || exactData==null || exactPlan==null || freshScope==null || exactChild==null ||
        (actualLedger ne bound.actualLedger) || (originalRoot ne bound.originalRoot) ||
        (parent.issuer ne this) || !retainedRestrictedSourceParents.containsKey(parent) ||
        (parent.session ne bound.session) || (parent.proof ne bound.proof) ||
        (parent.publication ne bound.publication) || (parent.readerLease ne bound.readerLease) ||
        (parent.originalRoot ne originalRoot) || (parent.actualLedger ne actualLedger) ||
        (parent.link ne bound.link) || (intent.exactLink ne parent.link) ||
        (intent.parentReference ne parent.reference) || attempt==null || attempt.building || attempt.failure!=null ||
        (attempt.exactParent ne parent) || (attempt.exactRoot ne originalRoot) ||
        (attempt.actualLedger ne actualLedger) || (attempt.exactIntent ne intent) ||
        (attempt.allocation ne allocation) || (attempt.plan ne exactPlan) || (attempt.reader ne exactPlan.actualReader) ||
        (attempt.data ne exactData) || (exactPlan.issuer ne this) || !exactPlan.isIssued ||
        (exactPlan.parent ne parent) || (exactPlan.intent ne intent) || (exactPlan.exactRoot ne originalRoot) ||
        (exactPlan.actualLedger ne actualLedger) || (exactPlan.allocation ne allocation) ||
        (exactPlan.data ne exactData) || (exactPlan.receiptForReplay ne null) ||
        !sourceReservePlanIsRetained(exactPlan) || exactChild.isClosed || exactData.effectClosure==null then
      throw new SQLNonTransientConnectionException("exact registered active restricted SOURCE R0 DATA child/plan scope required")
    originalRoot.requireLedgerIdentity(actualLedger)
    intent.requireSourcePlanDropNotIssued()
    intent.requireOriginalReservePlanBinding(parent.link,parent.reference,originalRoot,
      intent.exactDomainObservation,intent.expectedSourceObservation)
    val closure=exactPlan.effectClosure()
    if closure==null || (exactPlan.exactEffectClosure ne closure) ||
        !closure.belongsTo(exactPlan,this,originalRoot,originalRoot.profile) ||
        (closure.data ne exactData.effectClosure) then
      throw new SQLNonTransientConnectionException("exact active restricted SOURCE R0 issued effect closure DATA required")
    bound.session.requireRestrictedSourceFreshWriteExecutionScope(exactPlan,freshScope,this)

  /**
   * ACK readback borrows only the original plan DATA child. Its write scope is already settled, so this
   * guard checks the retained registration graph directly and deliberately does not refresh the parent.
   */
  private[mysql] def requireAcknowledgedRestrictedSourceReservePlanAllocationForUse(
      allocation:MySqlFencedTransaction.SourceReservePlanAllocation,
      parent:MySqlFencedTransaction.RestrictedSourceParent5,
      originalRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,
      intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData,
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
      exactChild:MySqlFormat3ReadBudget):Unit =
    val bound=Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt=sourceReservePlanAttempt(intent)
    if (Thread.currentThread() ne ownerThread) || allocation==null || parent==null || intent==null ||
        originalRoot==null || actualLedger==null || exactData==null || exactPlan==null ||
        exactOutcome==null || exactChild==null ||
        (actualLedger ne bound.actualLedger) || (originalRoot ne bound.originalRoot) ||
        (parent.issuer ne this) || !retainedRestrictedSourceParents.containsKey(parent) ||
        (retainedRestrictedSourceParentsByReference.get(parent.reference) ne parent) ||
        (parent.session ne bound.session) || (parent.proof ne bound.proof) ||
        (parent.publication ne bound.publication) || (parent.readerLease ne bound.readerLease) ||
        (parent.originalRoot ne originalRoot) || (parent.actualLedger ne actualLedger) ||
        (parent.link ne bound.link) || (intent.exactLink ne parent.link) ||
        (intent.parentReference ne parent.reference) || attempt==null || attempt.building || attempt.failure!=null ||
        (attempt.exactParent ne parent) || (attempt.exactRoot ne originalRoot) ||
        (attempt.actualLedger ne actualLedger) || (attempt.exactIntent ne intent) ||
        (attempt.allocation ne allocation) || (attempt.plan ne exactPlan) || (attempt.reader ne exactPlan.actualReader) ||
        (attempt.data ne exactData) || (exactPlan.issuer ne this) || !exactPlan.isIssued ||
        !sourceReservePlanIsRetained(exactPlan) || (exactPlan.parent ne parent) ||
        (exactPlan.intent ne intent) || (exactPlan.exactRoot ne originalRoot) ||
        (exactPlan.actualLedger ne actualLedger) || (exactPlan.allocation ne allocation) ||
        (exactPlan.data ne exactData) || (exactPlan.receiptForReplay ne null) ||
        (exactOutcome.plan ne exactPlan) || exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitAcknowledged ||
        !exactOutcome.verifiedPrecommit || exactOutcome.committedCleanupFailure!=null ||
        exactOutcome.freshScope==null || (exactOutcome.freshScope.plan ne exactPlan) ||
        exactOutcome.postCommitReadbackOwner==null || (exactOutcome.postCommitReadbackOwner.plan ne exactPlan) ||
        (exactOutcome.postCommitReadbackOwner.outcome ne exactOutcome) ||
        (exactOutcome.dataBudget ne exactChild) ||
        exactPlan.exactRoot.isClosed || exactPlan.exactRoot.isScopedReadBudget || exactChild.isClosed ||
        (exactChild.profile ne originalRoot.profile) || !exactChild.isCanonicalChildOf(originalRoot) ||
        !exactChild.sharesAllocationLedger(originalRoot) || exactData.effectClosure==null then
      throw new SQLNonTransientConnectionException("exact retained acknowledged restricted SOURCE R0 plan/attempt/closure DATA registration required")
    originalRoot.requireLedgerIdentity(actualLedger)
    intent.requireSourcePlanDropNotIssued()
    if allocation.childForReceipt(this,parent,originalRoot,actualLedger,intent,ownerThread) ne exactChild then
      throw new SQLNonTransientConnectionException("acknowledged restricted SOURCE R0 DATA child changed")
    val closure=exactPlan.exactEffectClosure
    if closure==null || !closure.belongsTo(exactPlan,this,originalRoot,originalRoot.profile) ||
        (closure.data ne exactData.effectClosure) then
      throw new SQLNonTransientConnectionException("exact acknowledged restricted SOURCE R0 effect closure DATA required")
    bound.session.requireRestrictedSourceReader(parent.readerLease,attempt.reader)
    bound.session.requireRestrictedSourceReader(parent.readerLease,parent.reader)

  private[mysql] def requireUnknownInspectionRestrictedSourceReservePlanAllocationForUse(
      allocation:MySqlFencedTransaction.SourceReservePlanAllocation,
      parent:MySqlFencedTransaction.RestrictedSourceParent5,
      originalRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,
      intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      exactData:MySqlNormalRecallSourceWorkflow.SourceChildReservePlanData,
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
      exactOwner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5,
      exactChild:MySqlFormat3ReadBudget):Unit =
    val bound=Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt=sourceReservePlanAttempt(intent)
    if (Thread.currentThread() ne ownerThread) || allocation==null || parent==null || intent==null ||
        originalRoot==null || actualLedger==null || exactData==null || exactPlan==null ||
        exactOutcome==null || exactOwner==null || exactChild==null ||
        (actualLedger ne bound.actualLedger) || (originalRoot ne bound.originalRoot) ||
        (parent.issuer ne this) || !retainedRestrictedSourceParents.containsKey(parent) ||
        (retainedRestrictedSourceParentsByReference.get(parent.reference) ne parent) ||
        (parent.session ne bound.session) || (parent.proof ne bound.proof) ||
        (parent.publication ne bound.publication) || (parent.readerLease ne bound.readerLease) ||
        (parent.originalRoot ne originalRoot) || (parent.actualLedger ne actualLedger) ||
        (parent.link ne bound.link) || (intent.exactLink ne parent.link) ||
        (intent.parentReference ne parent.reference) || attempt==null || attempt.building || attempt.failure!=null ||
        (attempt.exactParent ne parent) || (attempt.exactRoot ne originalRoot) ||
        (attempt.actualLedger ne actualLedger) || (attempt.exactIntent ne intent) ||
        (attempt.allocation ne allocation) || (attempt.plan ne exactPlan) || (attempt.reader ne exactPlan.actualReader) ||
        (attempt.data ne exactData) || (exactPlan.issuer ne this) || !exactPlan.isIssued ||
        !sourceReservePlanIsRetained(exactPlan) || (exactPlan.parent ne parent) ||
        (exactPlan.intent ne intent) || (exactPlan.exactRoot ne originalRoot) ||
        (exactPlan.actualLedger ne actualLedger) || (exactPlan.allocation ne allocation) ||
        (exactPlan.data ne exactData) || (exactPlan.receiptForReplay ne null) ||
        (exactOutcome.plan ne exactPlan) ||
        exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitUnknown ||
        !exactOutcome.verifiedPrecommit || exactOutcome.commitUnknown==null ||
        exactOutcome.freshScope==null || (exactOutcome.freshScope.plan ne exactPlan) ||
        !exactOutcome.freshScope.commitWasAttempted || exactOutcome.freshScope.commitWasAcknowledged ||
        (exactOutcome.freshScope.commitUnknownCause ne exactOutcome.commitUnknown) ||
        (exactOutcome.unknownInspectionOwner ne exactOwner) || exactOwner.failure!=null ||
        (exactOwner.session ne bound.session) || (exactOwner.plan ne exactPlan) ||
        (exactOwner.outcome ne exactOutcome) || (exactOwner.freshScope ne exactOutcome.freshScope) ||
        (exactOwner.readerLease ne parent.readerLease) || (exactOwner.root ne originalRoot) ||
        (exactOwner.ledger ne actualLedger) || (exactOwner.dataBudget ne exactChild) ||
        exactPlan.exactRoot.isClosed || exactPlan.exactRoot.isScopedReadBudget || exactChild.isClosed ||
        (exactChild.profile ne originalRoot.profile) || !exactChild.isCanonicalChildOf(originalRoot) ||
        !exactChild.sharesAllocationLedger(originalRoot) || exactData.effectClosure==null then
      throw new SQLNonTransientConnectionException("exact retained restricted SOURCE R0 UNKNOWN plan/attempt/closure DATA registration required")
    originalRoot.requireLedgerIdentity(actualLedger)
    intent.requireSourcePlanDropNotIssued()
    bound.session.requireRestrictedSourceUnknownInspection(exactOwner)
    if allocation.childForReceipt(this,parent,originalRoot,actualLedger,intent,ownerThread) ne exactChild then
      throw new SQLNonTransientConnectionException("restricted SOURCE R0 UNKNOWN DATA child changed")
    val closure=exactPlan.exactEffectClosure
    if closure==null || !closure.belongsTo(exactPlan,this,originalRoot,originalRoot.profile) ||
        (closure.data ne exactData.effectClosure) then
      throw new SQLNonTransientConnectionException("exact restricted SOURCE R0 UNKNOWN effect closure DATA required")

  private[mysql] def requireRetainedSourceReservePlan(
      plan: MySqlFencedTransaction.SourceReservePlan5
  ): Unit =
    requireRetainedSourceReservePlan(plan, null)

  /** drop 专用核验以精确签发的 Adapter 责任取代已消费的计划路由。 */
  private def requireRetainedSourceReservePlanForDrop(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      exactReceipt: MySqlFencedTransaction.SourceReservePlanDropReceipt
  ): Unit =
    if exactReceipt == null then
      throw new IllegalArgumentException("exact source RESERVE plan drop receipt required")
    requireRetainedSourceReservePlan(plan, exactReceipt)

  /** 计划图共享核验；仅 drop 提供精确 receipt 并使用专属 Adapter validator。 */
  private def requireRetainedSourceReservePlan(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      exactDropReceipt: MySqlFencedTransaction.SourceReservePlanDropReceipt
  ): Unit =
    val bound = Option(restrictedSourceSessionBinding).getOrElse(
      throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
    val attempt = if plan == null then null else sourceReservePlanAttempt(plan.intent)
    if (Thread.currentThread() ne ownerThread) || plan == null || !plan.isIssued || (plan.issuer ne this) ||
        !sourceReservePlanIsRetained(plan) || plan.exactRoot.isClosed || (plan.exactRoot ne bound.originalRoot) ||
        (plan.actualLedger ne bound.actualLedger) || attempt == null || (attempt.plan ne plan) ||
        (attempt.data ne plan.data) || (attempt.exactParent ne plan.parent) then
      throw new SQLNonTransientConnectionException("foreign, unsettled, or released source RESERVE plan")
    plan.exactRoot.requireLedgerIdentity(plan.actualLedger)
    plan.parent.requireRetained()
    if exactDropReceipt == null then
      plan.intent.requireOriginalReservePlanBinding(plan.parent.link, plan.parent.reference,
        plan.exactRoot, plan.intent.exactDomainObservation, plan.intent.expectedSourceObservation)
    else
      if (exactDropReceipt.issuerOwner ne this) || (exactDropReceipt.planOwner ne plan) ||
          (exactDropReceipt.intentOwner ne plan.intent) || exactDropReceipt.originalFailure != null then
        throw new SQLNonTransientConnectionException("drop-only source responsibility differs from its exact retained plan")
      exactDropReceipt.requireFor(plan)
      val exactRegistration = Option(exactDropReceipt.registrationOwner).getOrElse(
        throw new SQLNonTransientConnectionException("exact Adapter drop registration is absent from receipt"))
      plan.intent.requireOriginalReservePlanDropRoute(plan, exactDropReceipt, exactRegistration)
    val closure = plan.exactEffectClosure
    if (closure == null || (closure.plan ne plan) || (closure.issuer ne this) ||
        (closure.retainedRoot ne plan.exactRoot) || (closure.exactProfile ne plan.exactRoot.profile) ||
        (closure.data ne plan.data.effectClosure)) then
      throw new SQLNonTransientConnectionException("source RESERVE plan lost its exact issued effect closure")

  /** Main/Adapter 先撤销唯一只读 view，再由原 plan issuer 发行一次性 worker drop receipt。 */
  private[mysql] def issueSourceReservePlanDropReceipt(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      exactAdapter: MySqlOwnedIoAdapter,
      exactRegistration: MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration,
      exactViewsRevoked: com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked
  ): MySqlFencedTransaction.SourceReservePlanDropReceipt =
    if plan == null || exactAdapter == null || exactRegistration == null || exactViewsRevoked == null then
      throw new IllegalArgumentException("exact Main-revoked source RESERVE plan and Adapter registration required")
    plan.synchronized {
      val existing = plan.receiptForReplay
      if existing != null then
        if existing.originalFailure != null then throw existing.originalFailure
        existing.requireFor(plan)
        if (existing.adapterOwner ne exactAdapter) || (existing.registrationOwner ne exactRegistration) ||
            (existing.viewsRevokedOwner ne exactViewsRevoked) then
          throw new SQLNonTransientConnectionException("source RESERVE drop receipt is already bound to another Adapter registration")
        return existing

      plan.requireDropIssueState(this)
      val exactParent = plan.parent
      val exactIntent = plan.intent
      val exactRoot = plan.exactRoot
      val exactLedger = plan.actualLedger
      val exactHolder = plan.allocation
      val exactReader = plan.actualReader
      val exactClosure = plan.exactEffectClosure
      val bound = Option(restrictedSourceSessionBinding).getOrElse(
        throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
      val attempt = sourceReservePlanAttempt(exactIntent)
      if (plan.issuer ne this) || !plan.isIssued || exactParent == null || exactIntent == null ||
          exactRoot == null || exactLedger == null || exactHolder == null || exactReader == null || exactClosure == null ||
          (exactRoot ne bound.originalRoot) || (exactLedger ne bound.actualLedger) ||
          attempt == null || (exactParent ne attempt.exactParent) || (exactRoot ne attempt.exactRoot) ||
          (exactLedger ne attempt.actualLedger) || (exactIntent ne attempt.exactIntent) ||
          (exactHolder ne attempt.allocation) || (attempt.plan ne plan) || attempt.building ||
          attempt.failure != null || (attempt.data ne plan.data) || (attempt.reader ne exactReader) ||
          (exactParent.readerLease ne bound.readerLease) || exactRoot.isClosed || exactRoot.isScopedReadBudget ||
          !exactHolder.isOpen then
        throw new SQLNonTransientConnectionException("exact live registered source RESERVE plan owners required for drop")
      exactRoot.requireLedgerIdentity(exactLedger)
      exactHolder.requireDropBinding(this, exactParent, exactRoot, exactLedger, exactIntent, ownerThread)
      val exactChild = exactHolder.childForReceipt(this, exactParent, exactRoot, exactLedger, exactIntent, ownerThread)
      val exactReadTag = exactRegistration.readTag
      if exactReadTag == null then
        throw new SQLNonTransientConnectionException("exact registered source RESERVE drop read tag required")
      val receipt = new MySqlFencedTransaction.SourceReservePlanDropReceipt(
        this, plan, exactHolder, exactParent, exactIntent, exactRoot, exactLedger, exactChild,
        bound.readerLease, exactReader, exactAdapter, exactRegistration, exactViewsRevoked, exactReadTag)
      try
        exactRegistration.requireWorkerDropEligible(exactAdapter, plan, exactViewsRevoked)
        plan.retainDropReceipt(receipt)
        exactIntent.recordSourcePlanDropReceipt(plan, receipt)
        exactRegistration.recordReceiptIssued(receipt)
        exactViewsRevoked.recordReceiptIssued(receipt)
        receipt
      catch
        case cause: Throwable =>
          if plan.receiptForReplay == null then
            try plan.retainDropReceipt(receipt)
            catch
              case cleanup: Throwable =>
                if cleanup ne cause then MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          receipt.originalFailure = cause
          throw cause
    }

  /** 原 worker 按撤销、DATA 脱图、第二 reader 退休、child 关闭的顺序真实释放计划。 */
  private[mysql] def dropRestrictedSourceReservePlan(
      receipt: MySqlFencedTransaction.SourceReservePlanDropReceipt
  ): MySqlFencedTransaction.SourceReservePlanDropAck =
    if (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("original restricted source worker required for plan drop")
    if receipt == null then throw new IllegalArgumentException("exact source RESERVE plan drop receipt required")
    receipt.synchronized {
      if receipt.completedAck != null then
        receipt.completedAck.requireFor(receipt)
        return receipt.completedAck
      if receipt.originalFailure != null then throw receipt.originalFailure
      if receipt.running then
        throw new SQLNonTransientConnectionException("exact source RESERVE plan drop is already in progress")
      receipt.running = true
      try
        val plan = receipt.planOwner
        val exactHolder = receipt.holderOwner
        val exactParent = receipt.parentOwner
        val exactIntent = receipt.intentOwner
        val exactRoot = receipt.originalRootOwner
        val exactLedger = receipt.actualLedgerOwner
        val exactChild = receipt.childOwner
        val exactReaderLease = receipt.readerLeaseOwner
        val exactReader = receipt.readerOwner
        val exactAdapter = receipt.adapterOwner
        val exactRegistration = receipt.registrationOwner
        val exactViewsRevoked = receipt.viewsRevokedOwner
        val bound = Option(restrictedSourceSessionBinding).getOrElse(
          throw new SQLNonTransientConnectionException("restricted source runner has not been bound"))
        val attempt = sourceReservePlanAttempt(exactIntent)
        if (receipt.issuerOwner ne this) || plan == null || exactHolder == null || exactParent == null ||
            exactIntent == null || exactRoot == null || exactLedger == null || exactChild == null ||
            exactReaderLease == null || exactReader == null || exactAdapter == null || exactRegistration == null ||
            exactViewsRevoked == null || receipt.readTag == null || (receipt.readTag ne exactRegistration.readTag) ||
            (plan.issuer ne this) || (plan.parent ne exactParent) || (plan.intent ne exactIntent) ||
            (plan.exactRoot ne exactRoot) || (plan.actualLedger ne exactLedger) ||
            (plan.allocation ne exactHolder) || (plan.actualReader ne exactReader) ||
            (exactParent.readerLease ne exactReaderLease) || (bound.readerLease ne exactReaderLease) ||
            (bound.originalRoot ne exactRoot) || (bound.actualLedger ne exactLedger) ||
            (exactChild ne exactHolder.childForReceipt(this, exactParent, exactRoot, exactLedger, exactIntent, ownerThread)) ||
            attempt == null || attempt.building || (attempt.plan ne plan) ||
            (attempt.exactParent ne exactParent) || (attempt.exactRoot ne exactRoot) ||
            (attempt.actualLedger ne exactLedger) || (attempt.exactIntent ne exactIntent) ||
            (attempt.allocation ne exactHolder) || (attempt.data ne plan.data) || (attempt.reader ne exactReader) then
          throw new SQLNonTransientConnectionException("drop receipt differs from the exact retained source RESERVE plan owner graph")
        requireRetainedSourceReservePlanForDrop(plan, receipt)
        plan.requireDropIssueState(this)
        exactHolder.requireDropBinding(this, exactParent, exactRoot, exactLedger, exactIntent, ownerThread)
        bound.session.requireRestrictedSourceReader(exactReaderLease, exactReader)

        receipt.irreversibleStarted = true
        exactHolder.markRevoked(receipt)
        plan.markRevokedForDrop(receipt)
        receipt.planRevoked = true
        plan.detachGraphForDrop(receipt)
        receipt.graphDetached = true
        exactReaderLease.retireReader(this, exactRoot, exactLedger, exactReader)
        receipt.readerRetired = true
        exactHolder.closeChildForDrop(receipt)
        receipt.childClosed = true
        exactHolder.clearAfterDrop(receipt)
        plan.detachOwnerAfterDrop(receipt)
        retireSourceReservePlan(plan)
        removeSourceReservePlanAttempt(exactIntent, attempt)
        attempt.exactParent = null
        attempt.exactRoot = null
        attempt.actualLedger = null
        attempt.exactIntent = null
        attempt.allocation = null
        attempt.plan = null
        attempt.data = null
        attempt.reader = null
        attempt.failure = null
        attempt.building = false
        if (partialSourceReservePlanAttempt eq attempt) then partialSourceReservePlanAttempt = null
        if (partialSourceReservePlan eq plan) then partialSourceReservePlan = null
        receipt.metadataDetached = true
        val ack = new MySqlFencedTransaction.SourceReservePlanDropAck(this, receipt)
        receipt.completedAck = ack
        receipt.clearPayloadAfterAck()
        ack.requireFor(receipt)
        ack
      catch
        case cause: Throwable =>
          if receipt.completedAck == null && receipt.irreversibleStarted then
            if receipt.originalFailure == null then receipt.originalFailure = cause
            else if receipt.originalFailure ne cause then MySqlOwnedSessionLifecycle.suppress(receipt.originalFailure, cause)
          throw cause
      finally
        receipt.running = false
    }

  /** 仅实际 Session 的 scoped command 可以进入；普通 F2 staging 不消费本入口。 */
  private[mysql] def requireActiveControlledDebugWriteReadOwner(owner: MySqlFencedTransaction.ControlledDebugWriteReadOwner): Unit =
    if owner == null || (activeControlledDebugWriteReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("controlled debug read owner is stale or foreign")

  /** 仅实际 Session 的 scoped command 可以进入；普通 F2 staging 不消费本入口。 */
  private def requireOriginalNormalRecallPreparation(graph: MySqlFencedTransaction.NormalRecallCommitted,
      commandScope: MySqlRepositoryCommandScope, currentRoot: MySqlFormat3ReadBudget): Unit =
    if graph == null || commandScope == null || currentRoot == null || (Thread.currentThread() ne ownerThread) then
      throw new SQLNonTransientConnectionException("checkpoint requires its original owned P projection")
    graph.requireRetained()
    val original = retainedNormalRecallMaterializations.getOrElse(graph.tag,
      throw new SQLDataException("original P materialization was not retained by this runner"))
    if !retainedNormalRecallCommitted.get(graph.tag).exists(_ eq graph) || graph.checkpoint ||
      (graph.materialization ne original) || (graph.proof.commandScope.exactAuditScope() ne scope) ||
      original.plan.workflowUuid != commandScope.operationPlan().workflowUuid || completedNormalRecallPrepares.contains(graph.tag) then
      throw new SQLDataException("foreign, revoked or already completed P projection")
    val oldHandle = original.plan.retainedHandle.getOrElse(throw new SQLDataException("P has no original retained issuer"))
    val currentPlan = commandScope.operationPlan()
    val currentHandle = currentPlan.retainedHandle.getOrElse(throw new SQLDataException("C has no original retained issuer"))
    if (oldHandle.issuer ne currentHandle.issuer) ||
      (oldHandle.issuer.rootForOriginal(original.plan) ne original.root) ||
      (currentHandle.issuer.rootForOriginal(currentPlan) ne currentRoot) ||
      currentRoot.isClosed || !original.root.sharesAllocationLedger(currentRoot) ||
      (original.root.profile ne currentRoot.profile) then
      throw new SQLDataException("P/C lost original same-session roots and issuer")
    commandScope.requireCurrent(scope, currentPlan)
    commandScope.requireHeldSessionBarrier(scope, currentPlan).readmit(verified, expectation, scope)
    if coordinator.requireBusinessAdmission(verified, scope) ne commandScope.requireBusinessAdmission(scope, currentPlan) then
      throw new SQLNonTransientConnectionException("P continuation lost the current original admission")

  private[mysql] def executeNormalRecallWorkflow(commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation, prepared: Option[MySqlFencedTransaction.NormalRecallCommitted]
  ): Try[MySqlFencedTransaction.NormalRecallCommitted] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nested) => Failure(nested)
      case None =>
        try
          if (Thread.currentThread() ne ownerThread) || commandScope == null || operation == null || prepared == null then
            throw new SQLNonTransientConnectionException("normal recall requires the exact owned command and worker")
          val m = planNormalRecallWorkflowOwned(commandScope, operation, prepared, rollbackOnly)
          m.requireBoundTo(commandScope, operation)
          coordinator.beginNormalRecallWorkflow(commandScope, operation, m)
          runNormalRecallCriticalPhase(commandScope, operation, m, NormalRecallSqlPhase.Reserve, rollbackOnly)
          try
            if m.isDeploymentReceipt then
              readNormalRecallDeploymentPostCommit(commandScope,operation,m,m.reserve,rollbackOnly,false)((_,_)=>())
            else readNormalRecallPostCommit(m.reserve, m)(_ => ())
          catch case primary: Throwable => throw coordinator.markNormalRecallReserveReadbackUnknown(commandScope, operation, m, primary)
          coordinator.recordNormalRecallReserveAcknowledged(commandScope, operation, m)
          coordinator.captureNormalRecallExecuteFailureAfterReserve(commandScope, operation, m) {
            runNormalRecallCriticalPhase(commandScope, operation, m, NormalRecallSqlPhase.Execute, rollbackOnly)
          }
          try
            def retain(actual:MySqlModeOperationEffectCodec.Image,proof:MySqlFencedTransaction.NormalRecallPostCommitReadback)
                :MySqlFencedTransaction.NormalRecallCommitted =
              if retainedNormalRecallCommitted.contains(commandScope.tag()) then throw new SQLDataException("duplicate normal recall projection")
              m.root.reserveRetainedMemory(2048L,"normal recall committed projection and registry")
              val graph=new MySqlFencedTransaction.NormalRecallCommitted(m,proof,actual)
              retainedNormalRecallCommitted.put(commandScope.tag(),graph)
              graph
            val result = if m.isDeploymentReceipt then
              readNormalRecallDeploymentPostCommit(commandScope,operation,m,m.execute,rollbackOnly,true) { (actual,owner) =>
                retain(actual,MySqlFencedTransaction.issueNormalRecallDeploymentPostCommitReadback(this,commandScope,operation,m,actual,owner))
              }
            else readNormalRecallPostCommit(m.execute,m,Some(commandScope.tag())) { actual =>
              retain(actual,MySqlFencedTransaction.issueNormalRecallPostCommitReadback(this,commandScope,operation,m,actual))
            }
            prepared.foreach(graph => completedNormalRecallPrepares.add(graph.tag))
            coordinator.completeNormalRecallWorkflow(result.proof)
            Success(result)
          catch case primary: Throwable => Failure(coordinator.markNormalRecallExecuteReadbackUnknown(commandScope, operation, m, primary))
        catch case primary: Throwable => Failure(primary)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private[mysql] def hasRetainedNormalRecallProjection(tag: IoOperationTag): Boolean =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("recall retention is owned IO only")
    retainedNormalRecallMaterializations.contains(tag)

  private[mysql] def requireNormalRecallProjectionRelease(tag: IoOperationTag): Unit =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("recall release is owned IO only")
    val graph = retainedNormalRecallCommitted.getOrElse(tag, throw new SQLDataException("recall has no completed readback to release"))
    graph.requireRetained()
    if !graph.checkpoint && !completedNormalRecallPrepares.contains(tag) then
      throw new SQLDataException("original P is still responsible for checkpoint/recovery")
    coordinator.requireBusinessAdmission(verified, scope)

  /** 原 exact Main drop已核后调用；P必须等原C提交并读回，UNKNOWN不能释放。 */
  private[mysql] def releaseRetainedNormalRecallProjection(tag: IoOperationTag): Unit =
    requireNormalRecallProjectionRelease(tag)
    retainedNormalRecallCommitted.remove(tag)
    retainedNormalRecallMaterializations.remove(tag)
    completedNormalRecallPrepares.remove(tag)
    retainedNormalRecallProjectionBudgets.release(tag)

  private[mysql] def executeControlledDebugWorkflow(commandScope: MySqlRepositoryCommandScope,
      request: MySqlControlledDebugPrepareRequest, prepared: Option[MySqlControlledDebugPreparedGraph],
      activeLimit: Int): Try[MySqlControlledDebugExecution] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("controlled debug command left its owned IO thread"))
          else if commandScope == null || request == null || prepared == null || activeLimit < 1 || activeLimit > 256 then
            Failure(new IllegalArgumentException("exact controlled command, original preparation and capacity required"))
          else
            try
              val activation = prepared.nonEmpty
              val operation = prepared match
                case None => request.repositoryOperation
                case Some(graph) =>
                  if graph.request ne request then
                    throw new SQLNonTransientConnectionException("commit lost its original prepared request reference")
                  graph.commitOperation(request.targetPose, activeLimit)
              val m = planControlledDebugWorkflowOwned(commandScope, operation, request, activation,
                activeLimit, prepared, rollbackOnly)
              m.requireBoundTo(commandScope, operation)
              coordinator.beginControlledDebugWorkflow(commandScope, operation, m)
              runControlledDebugCriticalPhase(commandScope, operation, m, ControlledDebugSqlPhase.Reserve, rollbackOnly)
              try
                if m.isDeploymentReceipt then
                  readControlledDebugDeploymentPostCommit(commandScope,operation,m,m.reserve,rollbackOnly,false)((_,_)=>())
                else readControlledDebugPostCommit(m.reserve,m)(_=>())
              catch case primary: Throwable =>
                throw coordinator.markControlledDebugReserveReadbackUnknown(commandScope, operation, m, primary)
              coordinator.recordControlledDebugReserveAcknowledged(commandScope, operation, m)
              coordinator.captureControlledDebugExecuteFailureAfterReserve(commandScope, operation, m) {
                coordinator.requireControlledDebugWorkflowAdmission(commandScope, operation, m,
                  MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending)
                runControlledDebugCriticalPhase(commandScope, operation, m, ControlledDebugSqlPhase.Execute, rollbackOnly)
              }
              try
                def resultFrom(actual:MySqlControlledDebugGraph.Image,budget:MySqlFormat3ReadBudget,
                    proof:MySqlFencedTransaction.ControlledDebugPostCommitReadback):MySqlControlledDebugExecution =
                  val result = prepared match
                    case None =>
                      val graph = MySqlControlledDebugGraph.prepared(commandScope.tag(), request, m.plan,
                        m.context, m.plan.executeOperationUuid, actual, budget, proof)
                      if retainedControlledDebugPreparedGraphs.contains(commandScope.tag()) then
                        throw new SQLNonTransientConnectionException("controlled preparation tag was already retained")
                      m.originalRoot.reserveRetainedMemory(1024L,"controlled prepared registry entry")
                      retainedControlledDebugPreparedGraphs.put(commandScope.tag(), graph)
                      MySqlControlledDebugExecution.Prepared(graph)
                    case Some(graph) =>
                      MySqlControlledDebugExecution.Committed(MySqlControlledDebugGraph.committed(commandScope.tag(),
                        graph, m.plan, m.context, m.plan.executeOperationUuid, actual, activeLimit,
                        budget, proof))
                  result
                val (result,proof)=if m.isDeploymentReceipt then
                  readControlledDebugDeploymentPostCommit(commandScope,operation,m,m.execute,rollbackOnly,true) { (actual,owner)=>
                    val proof=MySqlFencedTransaction.issueControlledDebugDeploymentPostCommitReadback(this,
                      commandScope,operation,m,actual,owner)
                    (resultFrom(actual,m.originalRoot,proof),proof)
                  }
                else readControlledDebugPostCommit(m.execute,m,Some(commandScope.tag())) { actual=>
                  val proof=MySqlFencedTransaction.issueControlledDebugPostCommitReadback(this,commandScope,operation,m,actual)
                  (resultFrom(actual,actual.allocationBudget,proof),proof)
                }
                coordinator.completeControlledDebugWorkflow(proof)
                Success(result)
              catch case primary: Throwable =>
                Failure(coordinator.markControlledDebugExecuteReadbackUnknown(commandScope, operation, m, primary))
            catch case primary: Throwable => Failure(primary)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private[mysql] def hasRetainedControlledDebugProjection(tag: IoOperationTag): Boolean =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("controlled retention is owned IO only")
    retainedControlledDebugMaterializations.contains(tag)

  /** 调用前 Session 必须已经验证 exact drop receipt；先撤强引用，再释放共享原 root。 */
  private[mysql] def releaseRetainedControlledDebugProjection(tag: IoOperationTag): Unit =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("controlled release is owned IO only")
    retainedControlledDebugPreparedGraphs.remove(tag)
    retainedControlledDebugMaterializations.remove(tag)
    retainedControlledDebugProjectionBudgets.release(tag)

  private def planModeWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlModeWorkflowPlanning =
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("mode workflow planning requires the approved format3/physical2 target")
    if Thread.currentThread() ne ownerThread then
      throw new SQLNonTransientConnectionException("mode workflow planning left its owned IO thread")
    val plan = commandScope.operationPlan()
    val commandScopeValue = commandScope.exactAuditScope()
    if (commandScopeValue ne scope) then
      throw new SQLNonTransientConnectionException("Main command scope differs from the F3 runner session")
    commandScope.requireCurrent(scope, plan)
    val profile = exactPlanningProfile
    plan.requireSingle(operation, profile)
    MySqlModeOperationEffectCodec.modeRequest(operation)
    val contextIdentity = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
    val commandAdmission = commandScope.requireBusinessAdmission(scope, plan)
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    if (admission ne commandAdmission) then
      throw new SQLNonTransientConnectionException("Main command admission differs from the current writer fence")
    if !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("mode planning requires an idle auto-commit owned connection")

    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var beginAttempted = false
    var rollbackAcknowledged = false
    var autocommitRestored = false
    var cleanupFailed = false
    var primary: Throwable = null
    var owner: MySqlFencedTransaction.PlanningReadOwner = null
    var lockedBefore: MySqlFormat3FullSnapshot = null
    var retainLockedBeforeBudget = false
    var result: MySqlModeWorkflowPlanning = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode planning could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode planning transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if writer.lastOperationUuid.exists(uuid => uuid == plan.reserveOperationUuid || uuid == plan.executeOperationUuid) then
        throw new SQLTransactionRollbackException("fixed mode operation already occupies W; settlement must use its retained plan")
      if coordinator.requireBusinessAdmission(verified, scope) ne admission then
        throw new SQLNonTransientConnectionException("writer admission changed after M→B→W locks")

      val heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      owner = MySqlFencedTransaction.afterPlanningLockedRows(
        this,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
        verified,
        scope,
        expectation,
        coordinator,
        commandScope,
        plan,
        contextIdentity,
        heldBarrier,
        admission,
        rollbackOnly,
        ownerThread,
        manifest,
        binding,
        writer
      )
      activePlanningReadOwner = owner
      val originalRoot=owner.originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("mode planning lost original reserved root"))
      val before:MySqlModeOperationEffectCodec.Image=expectation.profile match
        case MySqlSchemaAuditProfile.Format3=>
          val reader=new MySqlFormat3FullSnapshotReader(verified,scope,expectation.schema,profile,sharedFormat3AllocationLedger)
          lockedBefore=reader.readWithinPlanningTransaction(owner)
          lockedBefore
        case MySqlSchemaAuditProfile.DeploymentReceipt1=>
          val deployment=deploymentReceiptSession.capturePurposePlanningImage(owner)
          owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,deployment)
          deployment
        case _=>throw new SQLNonTransientConnectionException("unsupported mode planning image family")
      owner.requireCurrent(verified, scope, expectation.schema, profile, connection)
      MySqlModeOperationEffectCodec.preflight(before, operation) match
        case MySqlModePreflight.Rejected(reason) =>
          result = MySqlModeWorkflowPlanning.Rejected(
            com.wi.floatingpod.storage.OperatingModeChangeOutcome.Rejected(reason))
        case MySqlModePreflight.AlreadySet =>
          val request = MySqlModeOperationEffectCodec.modeRequest(operation)
          val state = modeStateFromSnapshot(before, operation, Set(request.expectedStateVersion))
          val commitRequest = MySqlOwnedModeCommitRequest(request.binding, request.podUuid, request.owner,
            request.expectedStateVersion, request.attemptUuid, request.targetMode, request.actualPose)
          val readback = MySqlOwnedModeAlreadySetReadback.fromFormat3Readback(
            commandScope.tag(), plan, commitRequest, state, before,Some(originalRoot))
          result = MySqlModeWorkflowPlanning.AlreadySet(readback)
          retainedModeProjectionBudgets.retainRoot(commandScope.tag(), originalRoot)
          retainLockedBeforeBudget = true
        case MySqlModePreflight.Execute =>
          retainedModeProjectionBudgets.retainRoot(commandScope.tag(),
            owner.originalRetainedRoot.getOrElse(throw new SQLNonTransientConnectionException(
              "mode planning lacks its actual original retained root")))
          retainLockedBeforeBudget = true
          val inputs = issueExecutionPlanningInputs(owner, plan, operation, contextIdentity,
            writer.databaseNow, before, lockModeLocatorRevision(before, operation))
          val materialization = inputs.materialize()
          result = MySqlModeWorkflowPlanning.Execute(materialization)
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        primary = failure
    finally
      activePlanningReadOwner = null

    rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("mode planning did not restore auto-commit after rollback")
        autocommitRestored = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted && isolationRead then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("mode planning did not restore the prior transaction isolation")
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if lockedBefore != null && !retainLockedBeforeBudget && plan.retainedHandle.isEmpty then
      try lockedBefore.allocationBudget.closeRootReadBudget()
      catch case failure: Throwable => recordCleanupFailure(failure)

    if cleanupFailed then
      val cause = if primary != null then primary else new SQLException("mode planning cleanup failed without a reported cause")
      throw new MySqlPlanningReadCleanupUncertainException(cause)
    if primary != null then throw primary
    if result == null then throw new SQLNonTransientConnectionException("mode planning returned no complete full20 materialization")
    if owner!=null then owner.settlePurposeReadAfterTransactionEnd()
    result

  private def planNormalRecallWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      prepared: Option[MySqlFencedTransaction.NormalRecallCommitted],
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlNormalRecallMaterialization =
    if (expectation.profile != MySqlSchemaAuditProfile.Format3 && expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("normal recall workflow planning requires the approved format3/physical2 target")
    if Thread.currentThread() ne ownerThread then
      throw new SQLNonTransientConnectionException("normal recall workflow planning left its owned IO thread")
    val plan = commandScope.operationPlan()
    val commandScopeValue = commandScope.exactAuditScope()
    if (commandScopeValue ne scope) then
      throw new SQLNonTransientConnectionException("Main command scope differs from the F3 runner session")
    commandScope.requireCurrent(scope, plan)
    val profile = exactPlanningProfile
    plan.requireSingle(operation, profile)
    val contextIdentity = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
    val commandAdmission = commandScope.requireBusinessAdmission(scope, plan)
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    if (admission ne commandAdmission) then
      throw new SQLNonTransientConnectionException("Main command admission differs from the current writer fence")
    if !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("normal recall planning requires an idle auto-commit owned connection")

    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var beginAttempted = false
    var rollbackAcknowledged = false
    var autocommitRestored = false
    var cleanupFailed = false
    var primary: Throwable = null
    var owner: MySqlFencedTransaction.PlanningReadOwner = null
    var lockedBefore: MySqlModeOperationEffectCodec.Image = null
    var retainLockedBeforeBudget = false
    var result: MySqlNormalRecallMaterialization = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("normal recall planning could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("normal recall planning transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if writer.lastOperationUuid.exists(uuid => uuid == plan.reserveOperationUuid || uuid == plan.executeOperationUuid) then
        throw new SQLTransactionRollbackException("fixed mode operation already occupies W; settlement must use its retained plan")
      if coordinator.requireBusinessAdmission(verified, scope) ne admission then
        throw new SQLNonTransientConnectionException("writer admission changed after M→B→W locks")

      val heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      owner = MySqlFencedTransaction.afterPlanningLockedRows(
        this,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
        verified,
        scope,
        expectation,
        coordinator,
        commandScope,
        plan,
        contextIdentity,
        heldBarrier,
        admission,
        rollbackOnly,
        ownerThread,
        manifest,
        binding,
        writer
      )
      activePlanningReadOwner = owner
      lockedBefore = if expectation.profile == MySqlSchemaAuditProfile.DeploymentReceipt1 then
        deploymentReceiptSession.capturePurposePlanningImage(owner)
      else
        val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, profile, sharedFormat3AllocationLedger)
        reader.readWithinPlanningTransaction(owner)
      owner.requireCurrent(verified, scope, expectation.schema, profile, connection)
      lockedBefore match
        case legacy: MySqlFormat3FullSnapshot =>
          owner.requireNormalRecallMaterializationInputs(verified,scope,expectation.schema,profile,connection,
            plan,operation,contextIdentity,writer.databaseNow,legacy)
        case deployment: MySqlScriptSchemaSnapshot =>
          owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,deployment)
      val originalRoot=owner.originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("normal recall planning lost original reserved root"))
      retainedNormalRecallProjectionBudgets.retainRoot(commandScope.tag(),originalRoot)
      val materialization = operation match
        case MySqlRepositoryOperation.PrepareNormalRecall(request) =>
          if prepared.nonEmpty then throw new SQLDataException("initial recall cannot borrow a prepared graph")
          MySqlNormalRecallWorkflowMaterializer.prepareInitial(request, plan, contextIdentity,
            writer.databaseNow, lockedBefore, originalRoot)
        case MySqlRepositoryOperation.CheckpointNormalRecall(request) =>
          val original = prepared.getOrElse(throw new SQLDataException("checkpoint needs its original private P graph"))
          requireOriginalNormalRecallPreparation(original, commandScope, originalRoot)
          if request.original ne original.request then throw new SQLDataException("checkpoint replaced its original request")
          MySqlNormalRecallWorkflowMaterializer.checkpoint(request, original.materialization, plan, contextIdentity,
            writer.databaseNow, lockedBefore, originalRoot)
        case _ => throw new SQLDataException("operation is outside normal recall P/C planning")
      result=materialization
      originalRoot.reserveRetainedMemory(1024L,"actual normal recall materialization and projection registry entries")
      if retainedNormalRecallMaterializations.contains(commandScope.tag()) then
        throw new SQLNonTransientConnectionException("normal recall command already retains its original materialization")
      retainedNormalRecallMaterializations.put(commandScope.tag(),materialization)
      retainLockedBeforeBudget=true
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        primary = failure
    finally
      activePlanningReadOwner = null

    rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("normal recall planning did not restore auto-commit after rollback")
        autocommitRestored = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted && isolationRead then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("normal recall planning did not restore the prior transaction isolation")
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if lockedBefore != null && !retainLockedBeforeBudget && plan.retainedHandle.isEmpty then
      try lockedBefore.allocationBudget.closeRootReadBudget()
      catch case failure: Throwable => recordCleanupFailure(failure)

    if cleanupFailed then
      val cause = if primary != null then primary else new SQLException("normal recall planning cleanup failed without a reported cause")
      throw new MySqlPlanningReadCleanupUncertainException(cause)
    if primary != null then throw primary
    if owner != null && expectation.profile == MySqlSchemaAuditProfile.DeploymentReceipt1 then owner.settlePurposeReadAfterTransactionEnd()
    if result == null then throw new SQLNonTransientConnectionException("normal recall planning returned no complete full20 materialization")
    result


  private def planControlledDebugWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      request: MySqlControlledDebugPrepareRequest,
      activation: Boolean,
      activeLimit: Int,
      prepared: Option[MySqlControlledDebugPreparedGraph],
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlControlledDebugWorkflowMaterialization =
    if (expectation.profile != MySqlSchemaAuditProfile.Format3 &&
        expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("controlled debug planning requires its approved closed schema3/schema5 physical2 target")
    if Thread.currentThread() ne ownerThread then
      throw new SQLNonTransientConnectionException("controlled debug workflow planning left its owned IO thread")
    val plan = commandScope.operationPlan()
    val commandScopeValue = commandScope.exactAuditScope()
    if (commandScopeValue ne scope) then
      throw new SQLNonTransientConnectionException("Main command scope differs from the F3 runner session")
    commandScope.requireCurrent(scope, plan)
    val profile = exactPlanningProfile
    plan.requireSingle(operation, profile)
    if request == null then throw new IllegalArgumentException("actual fixed controlled request required")
    val contextIdentity = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
    val commandAdmission = commandScope.requireBusinessAdmission(scope, plan)
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    if (admission ne commandAdmission) then
      throw new SQLNonTransientConnectionException("Main command admission differs from the current writer fence")
    if !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("controlled debug planning requires an idle auto-commit owned connection")

    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var beginAttempted = false
    var rollbackAcknowledged = false
    var autocommitRestored = false
    var cleanupFailed = false
    var primary: Throwable = null
    var owner: MySqlFencedTransaction.PlanningReadOwner = null
    var lockedBefore: MySqlFormat3FullSnapshot = null
    var retainLockedBeforeBudget = false
    var result: MySqlControlledDebugWorkflowMaterialization = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("controlled debug planning could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("controlled debug planning transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if writer.lastOperationUuid.exists(uuid => uuid == plan.reserveOperationUuid || uuid == plan.executeOperationUuid) then
        throw new SQLTransactionRollbackException("fixed mode operation already occupies W; settlement must use its retained plan")
      if coordinator.requireBusinessAdmission(verified, scope) ne admission then
        throw new SQLNonTransientConnectionException("writer admission changed after M→B→W locks")

      val heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      owner = MySqlFencedTransaction.afterPlanningLockedRows(
        this,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
        verified,
        scope,
        expectation,
        coordinator,
        commandScope,
        plan,
        contextIdentity,
        heldBarrier,
        admission,
        rollbackOnly,
        ownerThread,
        manifest,
        binding,
        writer
      )
      activePlanningReadOwner = owner
      val originalRoot=owner.originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("controlled planning lost original reserved root"))
      val materialization=expectation.profile match
        case MySqlSchemaAuditProfile.Format3=>
          val reader=new MySqlFormat3FullSnapshotReader(verified,scope,expectation.schema,profile,sharedFormat3AllocationLedger)
          lockedBefore=reader.readWithinPlanningTransaction(owner)
          owner.requireControlledMaterializationInputs(verified,scope,expectation.schema,profile,connection,
            plan,operation,contextIdentity,writer.databaseNow,lockedBefore)
          if activation then requireOriginalControlledPreparation(prepared.getOrElse(
            throw new SQLNonTransientConnectionException("actual privately issued prepared graph required")),lockedBefore,originalRoot,commandScope)
          MySqlControlledDebugWorkflowMaterializer.materialize(request,plan,operation,contextIdentity,
            writer.databaseNow,lockedBefore,activation,activeLimit,originalRoot)
        case MySqlSchemaAuditProfile.DeploymentReceipt1=>
          val before=deploymentReceiptSession.capturePurposePlanningImage(owner)
          owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,before)
          if activation then requireOriginalControlledPreparation(prepared.getOrElse(
            throw new SQLNonTransientConnectionException("actual privately issued Full26 preparation required")),before,originalRoot,commandScope)
          MySqlControlledDebugWorkflowMaterializer.materializeDeploymentReceipt(request,plan,operation,contextIdentity,
            writer.databaseNow,before,activation,activeLimit,originalRoot)
        case _=>throw new SQLNonTransientConnectionException("unsupported controlled planning family")
      result=materialization
      originalRoot.reserveRetainedMemory(1024L,"actual controlled materialization and projection registry entries")
      if retainedControlledDebugMaterializations.contains(commandScope.tag()) then
        throw new SQLNonTransientConnectionException("controlled command already retains its original materialization")
      retainedControlledDebugProjectionBudgets.retainRoot(commandScope.tag(),originalRoot)
      retainedControlledDebugMaterializations.put(commandScope.tag(),materialization)
      retainLockedBeforeBudget=true
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        primary = failure
    finally
      activePlanningReadOwner = null

    rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("controlled debug planning did not restore auto-commit after rollback")
        autocommitRestored = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted && isolationRead then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("controlled debug planning did not restore the prior transaction isolation")
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if lockedBefore != null && !retainLockedBeforeBudget && plan.retainedHandle.isEmpty then
      try lockedBefore.allocationBudget.closeRootReadBudget()
      catch case failure: Throwable => recordCleanupFailure(failure)

    if cleanupFailed then
      val cause = if primary != null then primary else new SQLException("controlled debug planning cleanup failed without a reported cause")
      throw new MySqlPlanningReadCleanupUncertainException(cause)
    if primary != null then throw primary
    if result == null then throw new SQLNonTransientConnectionException("controlled debug planning returned no complete full20 materialization")
    if owner!=null then owner.settlePurposeReadAfterTransactionEnd()
    result


  private def lockModeLocatorRevision(
      snapshot: MySqlModeOperationEffectCodec.Image,
      operation: MySqlRepositoryOperation
  ): Long =
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    val relation = snapshot.relation("physical_recovery_locator")
    val shape = MySqlFormat3RelationShapes.forTable("physical_recovery_locator")
    val podIndex = shape.columns.indexWhere(_.name == "pod_uuid")
    val bindingIndex = shape.columns.indexWhere(_.name == "binding_uuid")
    val revisionIndex = shape.columns.indexWhere(_.name == "locator_revision")
    if podIndex < 0 || bindingIndex < 0 || revisionIndex < 0 then
      throw new SQLDataException("format3 locator shape lacks the exact mode resource columns")
    val matches = relation.rows.filter(row =>
      row(bindingIndex) match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.binding.bindingUuid)
        case _ => false
    ).filter(row => row(podIndex) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.podUuid)
      case _ => false
    )
    matches match
      case Vector(row) => row(revisionIndex) match
        case MySqlFormat3Cell.SignedInteger(value) if value >= 0L => value
        case _ => throw new SQLDataException("locked current mode locator revision has an invalid typed value")
      case _ => throw new SQLTransactionRollbackException("locked full20 snapshot has no unique exact mode locator")

  /**
   * context 与 plan 必须逐效果相同。回调只能调用受限的 typed mutation；其异常或 Try Failure
   * 会 sticky 标为 rollback-only，即使回调吞掉失败也不能提交。
   */
  /** 原 Session/plan 唯一入口；完整 ACK 后仍保留 worker references，直到 exact Ordinary drop。 */
  private[mysql] def executeInactiveDomainWorkflow(commandScope: MySqlRepositoryCommandScope,
      request: MySqlInactiveDomainUpdateRequest): Try[MySqlInactiveDomainUpdateOutcome] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("inactive command left its captured owned IO thread"))
          else if commandScope == null || request == null then
            Failure(new IllegalArgumentException("exact inactive command and original scope required"))
          else
            try
              val operation = request.repositoryOperation
              planInactiveDomainWorkflowOwned(commandScope, operation, rollbackOnly) match
                case MySqlInactiveDomainWorkflowPlanning.Rejected(reason) =>
                  Success(MySqlInactiveDomainUpdateOutcome.Rejected(reason))
                case MySqlInactiveDomainWorkflowPlanning.Execute(m) =>
                  m.requireBoundTo(commandScope, operation)
                  if retainedInactiveDomainMaterializations.contains(commandScope.tag()) then
                    throw new SQLNonTransientConnectionException("inactive original tag was already retained")
                  m.originalRoot.reserveRetainedMemory(512L,"inactive original materialization registry entry")
                  retainedInactiveDomainMaterializations.put(commandScope.tag(),m)
                  coordinator.beginInactiveDomainWorkflow(commandScope,operation,m)
                  runInactiveDomainCriticalPhase(commandScope,operation,m,InactiveDomainSqlPhase.Reserve,rollbackOnly)
                  try readInactiveDomainPostCommit(m.reserve,m)(_ => ())
                  catch case primary: Throwable =>
                    throw coordinator.markInactiveDomainReserveReadbackUnknown(commandScope,operation,m,primary)
                  coordinator.recordInactiveDomainReserveAcknowledged(commandScope,operation,m)
                  coordinator.captureInactiveDomainExecuteFailureAfterReserve(commandScope,operation,m) {
                    runInactiveDomainCriticalPhase(commandScope,operation,m,InactiveDomainSqlPhase.Execute,rollbackOnly)
                  }
                  try
                    val (outcome,proof)=readInactiveDomainPostCommit(m.execute,m,Some(commandScope.tag())) { actual =>
                      // 完整 target 与独立 cycle 验证已在真实 readback seam 完成；这里只构造不可变 DATA 回复。
                      actual.allocationBudget.reserveRetainedMemory(512L,"inactive committed immutable state and reply")
                      val state=request.desired.copy(stateVersion=Math.addExact(request.expectedVersion,1L),
                        lastUpdated=m.plannedAt.toInstant(java.time.ZoneOffset.UTC).toEpochMilli)
                      val proof=MySqlFencedTransaction.issueInactiveDomainPostCommitReadback(this,commandScope,operation,m,actual)
                      (MySqlInactiveDomainUpdateOutcome.Updated(state),proof)
                    }
                    coordinator.completeInactiveDomainWorkflow(proof)
                    Success(outcome)
                  catch case primary: Throwable =>
                    Failure(coordinator.markInactiveDomainExecuteReadbackUnknown(commandScope,operation,m,primary))
            catch case primary: Throwable => Failure(primary)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
  private[mysql] def planInactiveDomainWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation
  ): Try[MySqlInactiveDomainWorkflowPlanning] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("inactive domain planning must run on its captured owned IO thread"))
          else if commandScope == null || operation == null then
            Failure(new IllegalArgumentException("exact command scope and original mode request are required"))
          else
            try Success(planInactiveDomainWorkflowOwned(commandScope, operation, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private def planInactiveDomainWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlInactiveDomainWorkflowPlanning =
    if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("inactive domain workflow planning requires the approved format3/physical2 target")
    if Thread.currentThread() ne ownerThread then
      throw new SQLNonTransientConnectionException("inactive domain workflow planning left its owned IO thread")
    val plan = commandScope.operationPlan()
    val commandScopeValue = commandScope.exactAuditScope()
    if (commandScopeValue ne scope) then
      throw new SQLNonTransientConnectionException("Main command scope differs from the F3 runner session")
    commandScope.requireCurrent(scope, plan)
    val profile = expectation.format3Inputs
      .map(_.limitProfile)
      .getOrElse(throw new SQLNonTransientConnectionException("format3 planning has no exact source limit profile"))
    plan.requireSingle(operation, profile)
    val request = operation match
      case MySqlRepositoryOperation.UpdateInactiveDomain(value) => value
      case _ => throw new IllegalArgumentException("only exact inactive domain command may use this planning route")
    val contextIdentity = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
    val commandAdmission = commandScope.requireBusinessAdmission(scope, plan)
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    if (admission ne commandAdmission) then
      throw new SQLNonTransientConnectionException("Main command admission differs from the current writer fence")
    if !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("inactive domain planning requires an idle auto-commit owned connection")

    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var beginAttempted = false
    var rollbackAcknowledged = false
    var autocommitRestored = false
    var cleanupFailed = false
    var primary: Throwable = null
    var owner: MySqlFencedTransaction.PlanningReadOwner = null
    var lockedBefore: MySqlFormat3FullSnapshot = null
    var retainLockedBeforeBudget = false
    var result: MySqlInactiveDomainWorkflowPlanning = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("inactive domain planning could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("inactive domain planning transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if writer.lastOperationUuid.exists(uuid => uuid == plan.reserveOperationUuid || uuid == plan.executeOperationUuid) then
        throw new SQLTransactionRollbackException("fixed inactive operation already occupies W; settlement must use its retained plan")
      if coordinator.requireBusinessAdmission(verified, scope) ne admission then
        throw new SQLNonTransientConnectionException("writer admission changed after M→B→W locks")

      val heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      owner = MySqlFencedTransaction.afterPlanningLockedRows(
        this,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
        verified,
        scope,
        expectation,
        coordinator,
        commandScope,
        plan,
        contextIdentity,
        heldBarrier,
        admission,
        rollbackOnly,
        ownerThread,
        manifest,
        binding,
        writer
      )
      activePlanningReadOwner = owner
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, profile, sharedFormat3AllocationLedger)
      lockedBefore = reader.readWithinPlanningTransaction(owner)
      owner.requireCurrent(verified, scope, expectation.schema, profile, connection)
      MySqlInactiveDomainUpdatePreflight.rejection(lockedBefore, request, lockedBefore.allocationBudget) match
        case Some(reason) =>
          result = MySqlInactiveDomainWorkflowPlanning.Rejected(reason)
        case None =>
          val inputs = issueInactiveDomainPlanningInputs(owner, plan, request, contextIdentity,
            writer.databaseNow, lockedBefore)
          val materialization = inputs.materialize()
          result = MySqlInactiveDomainWorkflowPlanning.Execute(materialization)
          retainedInactiveDomainProjectionBudgets.retainRoot(commandScope.tag(), materialization.preimage.allocationBudget)
          retainLockedBeforeBudget = true
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        primary = failure
    finally
      activePlanningReadOwner = null

    rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("inactive domain planning did not restore auto-commit after rollback")
        autocommitRestored = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted && isolationRead then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("inactive domain planning did not restore the prior transaction isolation")
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if lockedBefore != null && !retainLockedBeforeBudget && plan.retainedHandle.isEmpty then
      try lockedBefore.allocationBudget.closeRootReadBudget()
      catch case failure: Throwable => recordCleanupFailure(failure)

    if cleanupFailed then
      val cause = if primary != null then primary else new SQLException("inactive domain planning cleanup failed without a reported cause")
      throw new MySqlPlanningReadCleanupUncertainException(cause)
    if primary != null then throw primary
    if result == null then throw new SQLNonTransientConnectionException("inactive domain planning returned no complete full20 materialization")
    result


  private[mysql] def planDriftWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation
  ): Try[MySqlDriftWorkflowPlanning] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("mode planning must run on its captured owned IO thread"))
          else if commandScope == null || operation == null then
            Failure(new IllegalArgumentException("exact command scope and original mode request are required"))
          else
            try Success(planDriftWorkflowOwned(commandScope, operation, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private[mysql] def requireActiveInactiveDomainWriteReadOwner(owner: MySqlFencedTransaction.InactiveDomainWriteReadOwner): Unit =
    if owner == null || (activeInactiveDomainWriteReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("inactive read owner is stale or not issued by this locked phase")
  private[mysql] def requireActiveDriftWriteReadOwner(owner: MySqlFencedTransaction.DriftWriteReadOwner): Unit =
    if owner == null || (activeDriftWriteReadOwner ne owner) then
      throw new SQLNonTransientConnectionException("format3 mode-write read owner is stale or not issued by this locked phase")

  private def planDriftWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlDriftWorkflowPlanning =
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("mode workflow planning requires the approved format3/physical2 target")
    if Thread.currentThread() ne ownerThread then
      throw new SQLNonTransientConnectionException("mode workflow planning left its owned IO thread")
    val plan = commandScope.operationPlan()
    val commandScopeValue = commandScope.exactAuditScope()
    if (commandScopeValue ne scope) then
      throw new SQLNonTransientConnectionException("Main command scope differs from the F3 runner session")
    commandScope.requireCurrent(scope, plan)
    val profile = exactPlanningProfile
    plan.requireSingle(operation, profile)
    MySqlDriftOperationEffectCodec.driftRequest(operation)
    val contextIdentity = MySqlRepositoryOperationContextIdentity.captureCurrent(commandScope, verified, expectation)
    val commandAdmission = commandScope.requireBusinessAdmission(scope, plan)
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    if (admission ne commandAdmission) then
      throw new SQLNonTransientConnectionException("Main command admission differs from the current writer fence")
    if !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("mode planning requires an idle auto-commit owned connection")

    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var beginAttempted = false
    var rollbackAcknowledged = false
    var autocommitRestored = false
    var cleanupFailed = false
    var primary: Throwable = null
    var owner: MySqlFencedTransaction.PlanningReadOwner = null
    var lockedBefore: MySqlFormat3FullSnapshot = null
    var retainLockedBeforeBudget = false
    var result: MySqlDriftWorkflowPlanning = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode planning could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode planning transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if writer.lastOperationUuid.exists(uuid => uuid == plan.reserveOperationUuid || uuid == plan.executeOperationUuid) then
        throw new SQLTransactionRollbackException("fixed mode operation already occupies W; settlement must use its retained plan")
      if coordinator.requireBusinessAdmission(verified, scope) ne admission then
        throw new SQLNonTransientConnectionException("writer admission changed after M→B→W locks")

      val heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      owner = MySqlFencedTransaction.afterPlanningLockedRows(
        this,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
        verified,
        scope,
        expectation,
        coordinator,
        commandScope,
        plan,
        contextIdentity,
        heldBarrier,
        admission,
        rollbackOnly,
        ownerThread,
        manifest,
        binding,
        writer
      )
      activePlanningReadOwner = owner
      val originalRoot=owner.originalRetainedRoot.getOrElse(
        throw new SQLNonTransientConnectionException("drift planning lost original retained root"))
      val before:MySqlModeOperationEffectCodec.Image=expectation.profile match
        case MySqlSchemaAuditProfile.Format3=>
          val reader=new MySqlFormat3FullSnapshotReader(verified,scope,expectation.schema,profile,sharedFormat3AllocationLedger)
          lockedBefore=reader.readWithinPlanningTransaction(owner)
          lockedBefore
        case MySqlSchemaAuditProfile.DeploymentReceipt1=>
          val actual=deploymentReceiptSession.capturePurposePlanningImage(owner)
          owner.requireControlledDeploymentReceiptMaterializationInputs(plan,operation,contextIdentity,actual)
          actual
        case _=>throw new SQLNonTransientConnectionException("unsupported drift planning image family")
      owner.requireCurrent(verified, scope, expectation.schema, profile, connection)
      MySqlDriftOperationEffectCodec.preflight(before, operation) match
        case MySqlDriftPreflight.Rejected(reason) =>
          result = MySqlDriftWorkflowPlanning.Rejected(reason)
        case MySqlDriftPreflight.Execute =>
          retainedDriftProjectionBudgets.retainRoot(commandScope.tag(),originalRoot)
          retainLockedBeforeBudget = true
          val inputs = issueDriftExecutionPlanningInputs(owner, plan, operation, contextIdentity,
            writer.databaseNow, before, lockDriftLocatorRevision(before, operation))
          val materialization = inputs.materialize()
          result = MySqlDriftWorkflowPlanning.Execute(materialization)
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        primary = failure
    finally
      activePlanningReadOwner = null

    rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          throw new SQLNonTransientConnectionException("mode planning did not restore auto-commit after rollback")
        autocommitRestored = true
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted && isolationRead then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("mode planning did not restore the prior transaction isolation")
      catch
        case failure: Throwable => recordCleanupFailure(failure)

    if lockedBefore != null && !retainLockedBeforeBudget && plan.retainedHandle.isEmpty then
      try lockedBefore.allocationBudget.closeRootReadBudget()
      catch case failure: Throwable => recordCleanupFailure(failure)

    if cleanupFailed then
      val cause = if primary != null then primary else new SQLException("mode planning cleanup failed without a reported cause")
      throw new MySqlPlanningReadCleanupUncertainException(cause)
    if primary != null then throw primary
    if result == null then throw new SQLNonTransientConnectionException("mode planning returned no complete full20 materialization")
    if owner!=null then owner.settlePurposeReadAfterTransactionEnd()
    result

  private def lockDriftLocatorRevision(
      snapshot: MySqlModeOperationEffectCodec.Image,
      operation: MySqlRepositoryOperation
  ): Long =
    val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
    val relation = snapshot.relation("physical_recovery_locator")
    val shape = MySqlFormat3RelationShapes.forTable("physical_recovery_locator")
    val podIndex = shape.columns.indexWhere(_.name == "pod_uuid")
    val bindingIndex = shape.columns.indexWhere(_.name == "binding_uuid")
    val revisionIndex = shape.columns.indexWhere(_.name == "locator_revision")
    if podIndex < 0 || bindingIndex < 0 || revisionIndex < 0 then
      throw new SQLDataException("format3 locator shape lacks the exact mode resource columns")
    val matches = relation.rows.filter(row =>
      row(bindingIndex) match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.binding.bindingUuid)
        case _ => false
    ).filter(row => row(podIndex) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.podUuid)
      case _ => false
    )
    matches match
      case Vector(row) => row(revisionIndex) match
        case MySqlFormat3Cell.SignedInteger(value) if value >= 0L => value
        case _ => throw new SQLDataException("locked current mode locator revision has an invalid typed value")
      case _ => throw new SQLTransactionRollbackException("locked full20 snapshot has no unique exact mode locator")

  /**
   * context 与 plan 必须逐效果相同。回调只能调用受限的 typed mutation；其异常或 Try Failure
   * 会 sticky 标为 rollback-only，即使回调吞掉失败也不能提交。
   */
  def execute(
      context: MySqlOperationContext,
      plan: MySqlBusinessEffectPlan
  )(body: MySqlBusinessTransaction => Try[Unit]): Try[MySqlFencedTransactionOutcome] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            val failure = new IllegalStateException("MySQL business transaction must run on its captured owned IO thread")
            rollbackOnly.mark(failure)
            Failure(failure)
          else if context == null || plan == null || body == null then
            val failure = new IllegalArgumentException("MySQL operation context, effect plan, and transaction body are required")
            rollbackOnly.mark(failure)
            Failure(failure)
          else
            try
              plan.requirePrimitiveExecution()
              Success(executeOwned(context, plan, body, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  /** Execute the exact Main-retained mode plan through two independent F3 critical commits. */
  private[mysql] def executeModeWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      activeLimit: Int
  ): Try[MySqlOwnedModeExecution] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("mode execution must run on its captured owned IO thread"))
          else if commandScope == null || operation == null || materialization == null ||
            activeLimit <= 0 || activeLimit > 256 then
            Failure(new IllegalArgumentException("exact mode command, full20 materialization and bounded active limit are required"))
          else
            try Success(executeModeWorkflowOwned(commandScope, operation, materialization, activeLimit, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private def executeModeWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      activeLimit: Int,
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlOwnedModeExecution =
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("owned mode SQL execution requires the approved format3/physical2 target")
    materialization.requireBoundTo(commandScope, operation)
    if commandScope.exactAuditScope() ne scope then
      throw new SQLNonTransientConnectionException("Main mode command scope differs from the exact owned F3 session")
    coordinator.beginModeWorkflow(commandScope, operation, materialization)
    coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization,
      MySqlModeWorkflowPhase.PreparedForReserve)

    runModeCriticalPhase(commandScope, operation, materialization, ModeSqlPhase.Reserve, rollbackOnly)
    val reserveEffect = materialization.effects(0)
    try
      if materialization.isDeploymentReceipt then
        readModeDeploymentPostCommit(commandScope,operation,materialization,reserveEffect,rollbackOnly,false)((_,_)=>())
      else readModePostCommit(reserveEffect, materialization)(_ => ())
    catch
      case primary: Throwable =>
        val blocked = coordinator.markModeReserveReadbackUnknown(commandScope, operation, materialization, primary)
        throw blocked
    coordinator.recordModeReserveAcknowledged(commandScope, operation, materialization)

    coordinator.captureModeExecuteFailureAfterReserve(commandScope, operation, materialization) {
      coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization,
        MySqlModeWorkflowPhase.ReserveCommittedExecutePending)
      runModeCriticalPhase(commandScope, operation, materialization, ModeSqlPhase.Execute, rollbackOnly)
    }
    val executeEffect = materialization.effects(1)
    try
      def resultFrom(executeReadback:MySqlModeOperationEffectCodec.Image,
          budget:MySqlFormat3ReadBudget):MySqlOwnedModeExecution =
        val state = modeStateFromSnapshot(executeReadback, operation,
          Set(Math.addExact(MySqlModeOperationEffectCodec.modeRequest(operation).expectedStateVersion, 1L)))
        val outcome = OperatingModeChangeOutcome.Changed(state)
        val modeRequest = MySqlModeOperationEffectCodec.modeRequest(operation)
        val commitRequest = MySqlOwnedModeCommitRequest(modeRequest.binding, modeRequest.podUuid, modeRequest.owner,
          modeRequest.expectedStateVersion, modeRequest.attemptUuid, modeRequest.targetMode, modeRequest.actualPose)
        val graph = MySqlCommittedOwnedModeGraph.fromFormat3Readback(commandScope.tag(), commitRequest,
          outcome, executeReadback, activeLimit, budget)
        MySqlOwnedModeExecution.Committed(graph)
      val (result,proof)=if materialization.isDeploymentReceipt then
        readModeDeploymentPostCommit(commandScope,operation,materialization,executeEffect,rollbackOnly,true) { (actual,owner)=>
          val proof=MySqlFencedTransaction.issueModeDeploymentPostCommitReadback(this,commandScope,operation,
            materialization,actual,owner)
          (resultFrom(actual,owner.originalPurposeRoot),proof)
        }
      else readModePostCommit(executeEffect,materialization,Some(commandScope.tag())) { actual=>
        val proof=MySqlFencedTransaction.issueModePostCommitReadback(this,commandScope,operation,materialization,actual)
        (resultFrom(actual,actual.allocationBudget),proof)
      }
      coordinator.completeModeWorkflow(proof)
      result
    catch
      case primary: Throwable =>
        val blocked = coordinator.markModeExecuteReadbackUnknown(commandScope, operation, materialization, primary)
        throw blocked
  private def runModeCriticalPhase(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      phase: ModeSqlPhase,
      rollbackOnly: MySqlRollbackOnlyState
  ): Unit =
    val isReserve = phase == ModeSqlPhase.Reserve
    val workflowPhase = if isReserve then MySqlModeWorkflowPhase.PreparedForReserve
      else MySqlModeWorkflowPhase.ReserveCommittedExecutePending
    val effect = materialization.effects(if isReserve then 0 else 1)
    val expectedBefore = if isReserve then materialization.completePreimage else materialization.completeReserveTarget
    val expectedTarget = if isReserve then materialization.completeReserveTarget else materialization.completeExecuteTarget
    val plan = materialization.exactPlan
    var admission: MySqlBusinessAdmission = null
    var heldBarrier: HeldSessionBarrier = null
    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var transactionAttempted = false
    var idleAutocommitObserved = false
    var commitAttempted = false
    var commitAcknowledged = false
    var acknowledgmentRecorded = false
    var cleanupFailed = false
    var primary: Throwable = null
    var modePurposeOwners=List.empty[MySqlFencedTransaction.ModeWriteReadOwner]

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      admission = coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization, workflowPhase)
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireBusinessAdmission(scope, plan) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission differs from exact Main command admission")
      heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      if !connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("mode critical phase requires an idle auto-commit owned connection")
      idleAutocommitObserved = true
      if connection.isClosed then throw new SQLNonTransientConnectionException("mode critical phase connection is closed")

      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode critical phase could not establish REPEATABLE READ")
      transactionAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode critical transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if coordinator.requireModeWorkflowAdmission(commandScope, operation, materialization, workflowPhase) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission changed after M→B→W locks")
      if expectation.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 then
        val (_,owner)=readModeDeploymentLocked(commandScope,operation,materialization,effect,workflowPhase,
          MySqlFencedTransaction.ModeWriteReadImage.Before,heldBarrier,admission,rollbackOnly,manifest,binding,writer)
        modePurposeOwners=owner::modePurposeOwners
      else withScopedModeSnapshot(readModeLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.ModeWriteReadImage.Before, heldBarrier, admission, rollbackOnly,
        manifest, binding, writer)) { actualBefore =>
        if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedBefore,actualBefore) then
          throw new SQLTransactionRollbackException("locked full20 mode preimage differs from the exact retained phase")
      }

      phase match
        case ModeSqlPhase.Reserve =>
          MySqlModeFormat3DeltaWriter.writeReserve(connection, expectation.schema, materialization)
        case ModeSqlPhase.Execute =>
          MySqlModeFormat3DeltaWriter.writeExecute(connection, expectation.schema, materialization)

      val context = effect.exactOperationContext
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, receipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(context.canonicalFingerprintSha256) then
        throw new SQLDataException("mode phase writer receipt did not read back the exact operation and fingerprint")
      if expectation.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 then
        val (_,owner)=readModeDeploymentLocked(commandScope,operation,materialization,effect,workflowPhase,
          MySqlFencedTransaction.ModeWriteReadImage.Target,heldBarrier,admission,rollbackOnly,manifest,binding,writerAfterUpdate)
        modePurposeOwners=owner::modePurposeOwners
      else withScopedModeSnapshot(readModeLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.ModeWriteReadImage.Target, heldBarrier, admission, rollbackOnly,
        manifest, binding, writerAfterUpdate)) { actualTarget =>
        if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedTarget,actualTarget) then
          throw new SQLTransactionRollbackException("locked full20 mode target differs before critical commit")
      }
      rollbackOnly.cause.foreach(throw _)

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      if isReserve then
        coordinator.markModeReserveAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      else
        coordinator.markModeExecuteAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      acknowledgmentRecorded = true
    catch
      case failure: Throwable => primary = failure
    finally
      activeModeWriteReadOwner = null

    if commitAttempted && !commitAcknowledged then
      val cause = if primary != null then primary else new SQLException("mode phase commit reply was not acknowledged")
      val blocked =
        if isReserve then coordinator.markModeReserveCommitUnknown(commandScope, operation, materialization, cause)
        else coordinator.markModeExecuteCommitUnknown(commandScope, operation, materialization, cause)
      throw blocked

    if commitAcknowledged then
      var restored = false
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode commit did not restore auto-commit")
        restored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && isolationChangeAttempted && isolationRead then
        try
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then
            throw new SQLNonTransientConnectionException("mode commit did not restore the previous isolation")
        catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && !cleanupFailed && acknowledgmentRecorded then
        modePurposeOwners.reverse.foreach { owner=>
          try owner.settlePurposeReadAfterTransactionEnd()
          catch case failure:Throwable=>recordCleanupFailure(failure)
        }
      if cleanupFailed || !acknowledgmentRecorded then
        val cause = if primary != null then primary else new SQLNonTransientConnectionException("mode commit acknowledgement cleanup is uncertain")
        val blocked =
          if isReserve then coordinator.markModeReserveReadbackUnknown(commandScope, operation, materialization, cause)
          else coordinator.markModeExecuteReadbackUnknown(commandScope, operation, materialization, cause)
        throw blocked
      return

    var rollbackAcknowledged = !transactionAttempted
    val initialCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, false, isolationChangeAttempted, isolationRead, cleanupFailed)
    if initialCleanup.rollback then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    // A preflight rejection owns no transaction. Never mutate a caller's existing JDBC transaction.
    var autocommitRestored = !transactionAttempted && idleAutocommitObserved
    val afterRollback = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterRollback.restoreAutoCommit then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode rollback did not restore auto-commit")
        autocommitRestored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    val afterAutoCommit = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterAutoCommit.restoreIsolation then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("mode rollback did not restore the previous isolation")
      catch case failure: Throwable => recordCleanupFailure(failure)

    if primary == null then primary = new SQLException("mode phase failed without preserving its original cause")
    val finalCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if finalCleanup.connectionUncertain then
      val wrapped = new MySqlTransactionCleanupUncertainException(effect.exactOperationContext, primary)
      throw wrapped
    if !isReserve && !commitAttempted then
      throw coordinator.recordModeExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    throw primary

  private def runNormalRecallCriticalPhase(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      phase: NormalRecallSqlPhase,
      rollbackOnly: MySqlRollbackOnlyState
  ): Unit =
    val isReserve = phase == NormalRecallSqlPhase.Reserve
    val workflowPhase = if isReserve then MySqlNormalRecallWorkflowPhase.PreparedForReserve
      else MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending
    val effect = materialization.effects(if isReserve then 0 else 1)
    val expectedBefore = if isReserve then materialization.completePreimage else materialization.completeReserveTarget
    val expectedTarget = if isReserve then materialization.completeReserveTarget else materialization.completeExecuteTarget
    val plan = materialization.exactPlan
    var admission: MySqlBusinessAdmission = null
    var heldBarrier: HeldSessionBarrier = null
    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var transactionAttempted = false
    var idleAutocommitObserved = false
    var commitAttempted = false
    var commitAcknowledged = false
    var acknowledgmentRecorded = false
    var cleanupFailed = false
    var primary: Throwable = null
    val purposeOwners=scala.collection.mutable.ArrayBuffer.empty[MySqlFencedTransaction.NormalRecallWriteReadOwner]

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      admission = coordinator.requireNormalRecallWorkflowAdmission(commandScope, operation, materialization, workflowPhase)
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireBusinessAdmission(scope, plan) ne admission then
        throw new SQLNonTransientConnectionException("normal recall phase admission differs from exact Main command admission")
      heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      if !connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("normal recall critical phase requires an idle auto-commit owned connection")
      idleAutocommitObserved = true
      if connection.isClosed then throw new SQLNonTransientConnectionException("normal recall critical phase connection is closed")

      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("normal recall critical phase could not establish REPEATABLE READ")
      transactionAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("normal recall critical transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if coordinator.requireNormalRecallWorkflowAdmission(commandScope, operation, materialization, workflowPhase) ne admission then
        throw new SQLNonTransientConnectionException("normal recall phase admission changed after M→B→W locks")
      if materialization.isDeploymentReceipt then
        val (actualBefore,actualOwner)=readNormalRecallDeploymentLocked(commandScope,operation,materialization,effect,
          workflowPhase,MySqlFencedTransaction.NormalRecallWriteReadImage.Before,heldBarrier,admission,rollbackOnly,
          manifest,binding,writer)
        purposeOwners += actualOwner
        MySqlNormalRecallOperationEffectCodec.requireExactReadback(expectedBefore,actualBefore,materialization.originalRoot)
      else
        withScopedNormalRecallSnapshot(readNormalRecallLocked(commandScope, operation, materialization, effect,
          workflowPhase, MySqlFencedTransaction.NormalRecallWriteReadImage.Before, heldBarrier, admission, rollbackOnly,
          manifest, binding, writer)) { actualBefore =>
          MySqlNormalRecallOperationEffectCodec.requireExactReadback(expectedBefore,actualBefore,materialization.originalRoot)
        }

      phase match
        case NormalRecallSqlPhase.Reserve =>
          MySqlNormalRecallDeltaWriter.writeReserve(connection, expectation.schema, materialization)
        case NormalRecallSqlPhase.Execute =>
          MySqlNormalRecallDeltaWriter.writeExecute(connection, expectation.schema, materialization)

      val context = effect.operationContext
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, receipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(context.canonicalFingerprintSha256) then
        throw new SQLDataException("normal recall phase writer receipt did not read back the exact operation and fingerprint")
      if materialization.isDeploymentReceipt then
        val (actualTarget,actualOwner)=readNormalRecallDeploymentLocked(commandScope,operation,materialization,effect,
          workflowPhase,MySqlFencedTransaction.NormalRecallWriteReadImage.Target,heldBarrier,admission,rollbackOnly,
          manifest,binding,writerAfterUpdate)
        purposeOwners += actualOwner
        MySqlNormalRecallOperationEffectCodec.requireExactReadback(expectedTarget,actualTarget,materialization.originalRoot)
      else
        withScopedNormalRecallSnapshot(readNormalRecallLocked(commandScope, operation, materialization, effect,
          workflowPhase, MySqlFencedTransaction.NormalRecallWriteReadImage.Target, heldBarrier, admission, rollbackOnly,
          manifest, binding, writerAfterUpdate)) { actualTarget =>
          MySqlNormalRecallOperationEffectCodec.requireExactReadback(expectedTarget,actualTarget,materialization.originalRoot)
        }
      rollbackOnly.cause.foreach(throw _)

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      if isReserve then
        coordinator.markNormalRecallReserveAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      else
        coordinator.markNormalRecallExecuteAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      acknowledgmentRecorded = true
    catch
      case failure: Throwable => primary = failure
    finally
      activeNormalRecallWriteReadOwner = null

    if commitAttempted && !commitAcknowledged then
      val cause = if primary != null then primary else new SQLException("normal recall phase commit reply was not acknowledged")
      val blocked =
        if isReserve then coordinator.markNormalRecallReserveCommitUnknown(commandScope, operation, materialization, cause)
        else coordinator.markNormalRecallExecuteCommitUnknown(commandScope, operation, materialization, cause)
      throw blocked

    if commitAcknowledged then
      var restored = false
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("normal recall commit did not restore auto-commit")
        restored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && isolationChangeAttempted && isolationRead then
        try
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then
            throw new SQLNonTransientConnectionException("normal recall commit did not restore the previous isolation")
        catch case failure: Throwable => recordCleanupFailure(failure)
      if cleanupFailed || !acknowledgmentRecorded then
        val cause = if primary != null then primary else new SQLNonTransientConnectionException("normal recall commit acknowledgement cleanup is uncertain")
        val blocked =
          if isReserve then coordinator.markNormalRecallReserveReadbackUnknown(commandScope, operation, materialization, cause)
          else coordinator.markNormalRecallExecuteReadbackUnknown(commandScope, operation, materialization, cause)
        throw blocked
      try purposeOwners.reverseIterator.foreach(_.settlePurposeReadAfterTransactionEnd())
      catch case failure:Throwable=>
        if isReserve then throw coordinator.markNormalRecallReserveReadbackUnknown(commandScope,operation,materialization,failure)
        else throw coordinator.markNormalRecallExecuteReadbackUnknown(commandScope,operation,materialization,failure)
      return

    var rollbackAcknowledged = !transactionAttempted
    val initialCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, false, isolationChangeAttempted, isolationRead, cleanupFailed)
    if initialCleanup.rollback then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    // A preflight rejection owns no transaction. Never mutate a caller's existing JDBC transaction.
    var autocommitRestored = !transactionAttempted && idleAutocommitObserved
    val afterRollback = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterRollback.restoreAutoCommit then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("normal recall rollback did not restore auto-commit")
        autocommitRestored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    val afterAutoCommit = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterAutoCommit.restoreIsolation then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("normal recall rollback did not restore the previous isolation")
      catch case failure: Throwable => recordCleanupFailure(failure)

    if primary == null then primary = new SQLException("normal recall phase failed without preserving its original cause")
    val finalCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if finalCleanup.connectionUncertain then
      val wrapped = new MySqlTransactionCleanupUncertainException(effect.operationContext, primary)
      throw wrapped
    if !isReserve && !commitAttempted then
      throw coordinator.recordNormalRecallExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    throw primary


  private def runControlledDebugCriticalPhase(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      phase: ControlledDebugSqlPhase,
      rollbackOnly: MySqlRollbackOnlyState
  ): Unit =
    val isReserve = phase == ControlledDebugSqlPhase.Reserve
    val workflowPhase = if isReserve then MySqlControlledDebugWorkflowPhase.PreparedForReserve
      else MySqlControlledDebugWorkflowPhase.ReserveCommittedExecutePending
    val effect = materialization.effects(if isReserve then 0 else 1)
    var deploymentReaders=List.empty[MySqlFencedTransaction.ControlledDebugWriteReadOwner]
    val plan = materialization.exactPlan
    var admission: MySqlBusinessAdmission = null
    var heldBarrier: HeldSessionBarrier = null
    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var transactionAttempted = false
    var idleAutocommitObserved = false
    var commitAttempted = false
    var commitAcknowledged = false
    var acknowledgmentRecorded = false
    var cleanupFailed = false
    var primary: Throwable = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      admission = coordinator.requireControlledDebugWorkflowAdmission(commandScope, operation, materialization, workflowPhase)
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireBusinessAdmission(scope, plan) ne admission then
        throw new SQLNonTransientConnectionException("controlled debug phase admission differs from exact Main command admission")
      heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      if !connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("controlled debug critical phase requires an idle auto-commit owned connection")
      idleAutocommitObserved = true
      if connection.isClosed then throw new SQLNonTransientConnectionException("controlled debug critical phase connection is closed")

      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("controlled debug critical phase could not establish REPEATABLE READ")
      transactionAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("controlled debug critical transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if coordinator.requireControlledDebugWorkflowAdmission(commandScope, operation, materialization, workflowPhase) ne admission then
        throw new SQLNonTransientConnectionException("controlled debug phase admission changed after M→B→W locks")
      if materialization.isDeploymentReceipt then
        val (actualBefore,reader)=readControlledDebugDeploymentLocked(commandScope,operation,materialization,effect,
          workflowPhase,MySqlFencedTransaction.ControlledDebugWriteReadImage.Before,heldBarrier,admission,rollbackOnly,manifest,binding,writer)
        deploymentReaders=reader::deploymentReaders
        MySqlControlledDebugFullComparison.requireExactDeploymentReceiptReadback(
          if isReserve then materialization.deploymentPreimage else materialization.deploymentReserveTarget,
          actualBefore,materialization.originalRoot)
      else
        withScopedControlledDebugSnapshot(readControlledDebugLocked(commandScope, operation, materialization, effect,
          workflowPhase, MySqlFencedTransaction.ControlledDebugWriteReadImage.Before, heldBarrier, admission, rollbackOnly,
          manifest, binding, writer)) { actualBefore =>
          MySqlControlledDebugFullComparison.requireExactReadback(
            if isReserve then materialization.preimage else materialization.reserveTarget,actualBefore,materialization.originalRoot)
        }

      phase match
        case ControlledDebugSqlPhase.Reserve =>
          MySqlControlledDebugDeltaWriter.writeReserve(connection, expectation.schema, materialization)
        case ControlledDebugSqlPhase.Execute =>
          MySqlControlledDebugDeltaWriter.writeExecute(connection, expectation.schema, materialization)

      val context = effect.exactOperationContext
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, receipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(context.canonicalFingerprintSha256) then
        throw new SQLDataException("controlled debug phase writer receipt did not read back the exact operation and fingerprint")
      if materialization.isDeploymentReceipt then
        val (actualTarget,reader)=readControlledDebugDeploymentLocked(commandScope,operation,materialization,effect,
          workflowPhase,MySqlFencedTransaction.ControlledDebugWriteReadImage.Target,heldBarrier,admission,rollbackOnly,manifest,binding,writerAfterUpdate)
        deploymentReaders=reader::deploymentReaders
        MySqlControlledDebugFullComparison.requireExactDeploymentReceiptReadback(
          if isReserve then materialization.deploymentReserveTarget else materialization.deploymentExecuteTarget,
          actualTarget,materialization.originalRoot)
      else
        withScopedControlledDebugSnapshot(readControlledDebugLocked(commandScope, operation, materialization, effect,
          workflowPhase, MySqlFencedTransaction.ControlledDebugWriteReadImage.Target, heldBarrier, admission, rollbackOnly,
          manifest, binding, writerAfterUpdate)) { actualTarget =>
          MySqlControlledDebugFullComparison.requireExactReadback(
            if isReserve then materialization.reserveTarget else materialization.executeTarget,actualTarget,materialization.originalRoot)
        }
      rollbackOnly.cause.foreach(throw _)

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      if isReserve then
        coordinator.markControlledDebugReserveAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      else
        coordinator.markControlledDebugExecuteAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      acknowledgmentRecorded = true
    catch
      case failure: Throwable => primary = failure
    finally
      activeControlledDebugWriteReadOwner = null

    if commitAttempted && !commitAcknowledged then
      val cause = if primary != null then primary else new SQLException("controlled debug phase commit reply was not acknowledged")
      val blocked =
        if isReserve then coordinator.markControlledDebugReserveCommitUnknown(commandScope, operation, materialization, cause)
        else coordinator.markControlledDebugExecuteCommitUnknown(commandScope, operation, materialization, cause)
      throw blocked

    if commitAcknowledged then
      var restored = false
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("controlled debug commit did not restore auto-commit")
        restored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && isolationChangeAttempted && isolationRead then
        try
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then
            throw new SQLNonTransientConnectionException("controlled debug commit did not restore the previous isolation")
        catch case failure: Throwable => recordCleanupFailure(failure)
      if !cleanupFailed && acknowledgmentRecorded then
        try deploymentReaders.foreach(_.settlePurposeReadAfterTransactionEnd())
        catch case failure:Throwable=>recordCleanupFailure(failure)
      if cleanupFailed || !acknowledgmentRecorded then
        val cause = if primary != null then primary else new SQLNonTransientConnectionException("controlled debug commit acknowledgement cleanup is uncertain")
        val blocked =
          if isReserve then coordinator.markControlledDebugReserveReadbackUnknown(commandScope, operation, materialization, cause)
          else coordinator.markControlledDebugExecuteReadbackUnknown(commandScope, operation, materialization, cause)
        throw blocked
      return

    var rollbackAcknowledged = !transactionAttempted
    val initialCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, false, isolationChangeAttempted, isolationRead, cleanupFailed)
    if initialCleanup.rollback then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    // A preflight rejection owns no transaction. Never mutate a caller's existing JDBC transaction.
    var autocommitRestored = !transactionAttempted && idleAutocommitObserved
    val afterRollback = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterRollback.restoreAutoCommit then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("controlled debug rollback did not restore auto-commit")
        autocommitRestored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    val afterAutoCommit = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterAutoCommit.restoreIsolation then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("controlled debug rollback did not restore the previous isolation")
      catch case failure: Throwable => recordCleanupFailure(failure)

    if primary == null then primary = new SQLException("controlled debug phase failed without preserving its original cause")
    val finalCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if finalCleanup.connectionUncertain then
      val wrapped = new MySqlTransactionCleanupUncertainException(effect.exactOperationContext, primary)
      throw wrapped
    if !isReserve && !commitAttempted then
      throw coordinator.recordControlledDebugExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    throw primary


  private[mysql] def releaseRetainedModeProjection(tag: IoOperationTag): Unit =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("mode projection budget release requires its exact owned IO thread")
    retainedModeProjectionBudgets.release(tag)

  private[mysql] def releaseRetainedDriftProjection(tag:IoOperationTag):Unit =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("drift projection release requires its exact owned IO thread")
    retainedDriftProjectionBudgets.release(tag)

  private[mysql] def retainedModeProjectionCount: Int =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("mode projection budget inspection requires its exact owned IO thread")
    retainedModeProjectionBudgets.pendingCount

  private def readModeLocked(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization,
      effect: MySqlModeOperationEffect,
      workflowPhase: MySqlModeWorkflowPhase,
      image: MySqlFencedTransaction.ModeWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    val owner = MySqlFencedTransaction.afterModeWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), verified, scope, expectation,
      coordinator, commandScope, operation, materialization, workflowPhase, effect, image, heldBarrier, admission,
      rollbackOnly, ownerThread, manifest, binding, writer)
    activeModeWriteReadOwner = owner
    try
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new SQLNonTransientConnectionException("mode full20 reader has no approved source limit profile")))
      reader.readWithinModeWriteTransaction(owner, materialization.preimage.allocationBudget)
    finally activeModeWriteReadOwner = null

  private def readModeDeploymentLocked(commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlModeWorkflowMaterialization,effect:MySqlModeOperationEffect,
      workflowPhase:MySqlModeWorkflowPhase,image:MySqlFencedTransaction.ModeWriteReadImage,
      heldBarrier:HeldSessionBarrier,admission:MySqlBusinessAdmission,rollbackOnly:MySqlRollbackOnlyState,
      manifest:MySqlLockedManifest,binding:MySqlLockedBinding,writer:MySqlBusinessWriterRow)
      :(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.ModeWriteReadOwner) =
    val owner=MySqlFencedTransaction.afterModeWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
      commandScope,operation,materialization,workflowPhase,effect,image,heldBarrier,admission,rollbackOnly,
      ownerThread,manifest,binding,writer)
    activeModeWriteReadOwner=owner
    try
      val actual=deploymentReceiptSession.capturePurposeModeWriteImage(owner)
      owner.requireSnapshot(actual)
      (actual,owner)
    finally activeModeWriteReadOwner=null

  private def readModeDeploymentPostCommit[A](commandScope:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation,materialization:MySqlModeWorkflowMaterialization,
      effect:MySqlModeOperationEffect,rollbackOnly:MySqlRollbackOnlyState,retainsProjection:Boolean)
      (consume:(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.ModeWriteReadOwner)=>A):A =
    if !materialization.isDeploymentReceipt || effect==null || consume==null || !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("idle original Full26 mode post-commit read required")
    val workflowPhase=if effect.phase==MySqlOperationEffectPhase.Reserve then
      MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback
    else MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback
    val admission=coordinator.requireModeWorkflowAdmission(commandScope,operation,materialization,workflowPhase)
    commandScope.requireCurrent(scope,materialization.exactPlan)
    if commandScope.requireBusinessAdmission(scope,materialization.exactPlan) ne admission then
      throw new SQLNonTransientConnectionException("Full26 post-commit read lost original command admission")
    val held=commandScope.requireHeldSessionBarrier(scope,materialization.exactPlan)
    val oldIsolation=connection.getTransactionIsolation
    var attempted=false
    var restored=false
    var cleanupFailed=false
    var primary:Throwable=null
    var result=Option.empty[A]
    var owner:MySqlFencedTransaction.ModeWriteReadOwner=null
    def cleanup(action: =>Unit):Unit =
      try action
      catch case cause:Throwable=>
        cleanupFailed=true
        if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
    try
      attempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("Full26 mode post-commit RR establishment failed")
      held.readmit(verified,expectation,scope)
      val manifest=lockManifest();validateManifest(manifest,admission)
      val binding=lockBinding();validateBinding(binding,admission)
      val writer=lockWriter();validateWriterIdentityAndLease(writer,admission)
      owner=MySqlFencedTransaction.afterModeWriteLockedRows(this,
        MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
        commandScope,operation,materialization,workflowPhase,effect,MySqlFencedTransaction.ModeWriteReadImage.Target,
        held,admission,rollbackOnly,ownerThread,manifest,binding,writer)
      activeModeWriteReadOwner=owner
      val snapshot=if retainsProjection then deploymentReceiptSession.capturePurposeModeProjectionImage(owner)
        else deploymentReceiptSession.capturePurposeModeWriteImage(owner)
      owner.requireSnapshot(snapshot)
      result=Some(consume(snapshot,owner))
    catch case cause:Throwable=>primary=cause
    finally activeModeWriteReadOwner=null
    var rolledBack = !attempted
    if attempted then cleanup { connection.rollback();rolledBack=true }
    if rolledBack then cleanup {
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("Full26 post-commit read did not restore auto-commit")
      restored=true
    }
    if restored then cleanup {
      connection.setTransactionIsolation(oldIsolation)
      if connection.getTransactionIsolation!=oldIsolation then throw new SQLNonTransientConnectionException("Full26 post-commit read isolation restoration differs")
    }
    if cleanupFailed then throw new MySqlPlanningReadCleanupUncertainException(primary)
    if primary!=null then throw primary
    if owner==null || result.isEmpty then throw new SQLNonTransientConnectionException("Full26 post-commit read produced no actual result")
    owner.settlePurposeReadAfterTransactionEnd()
    result.get

  private def readModePostCommit[A](effect: MySqlModeOperationEffect,
      materialization: MySqlModeWorkflowMaterialization,
      retainedTag: Option[IoOperationTag] = None)(consume: MySqlFormat3FullSnapshot => A): A =
    if effect == null || materialization == null || consume == null then
      throw new IllegalArgumentException("exact F3 post-commit effect, materialization and consumer are required")
    MySqlFencedTransaction.requireModePostCommitRetainedTag(retainedTag)
    val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
      expectation.format3Inputs.map(_.limitProfile).getOrElse(
        throw new SQLNonTransientConnectionException("mode post-commit reader has no approved source limit profile")))
    val snapshot = reader.readWithinModeWorkflow(materialization.preimage.allocationBudget)
    retainedTag match
      case None => withScopedModeSnapshot(snapshot) { actual =>
        effect.verifyReadback(actual)
        consume(actual)
      }
      case Some(tag) =>
        val budget = snapshot.allocationBudget
        var retained = false
        try
          effect.verifyReadback(snapshot)
          val result = consume(snapshot)
          retainedModeProjectionBudgets.retainScoped(tag, budget)
          retained = true
          result
        catch
          case primary: Throwable =>
            if !retained then
              try budget.closeScopedReadBudget()
              catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
            throw primary

  /** Read images live only through their exact comparison/projection; release their share before the next phase. */
  private def withScopedModeSnapshot[A](snapshot: MySqlFormat3FullSnapshot)(consume: MySqlFormat3FullSnapshot => A): A =
    if snapshot == null || consume == null || snapshot.allocationBudget == null ||
      !snapshot.allocationBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("mode full20 image must carry its exact scoped workflow allocation")
    var primary: Throwable = null
    try consume(snapshot)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def readNormalRecallDeploymentLocked(commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlNormalRecallMaterialization,effect:MySqlNormalRecallPhase,
      workflowPhase:MySqlNormalRecallWorkflowPhase,image:MySqlFencedTransaction.NormalRecallWriteReadImage,
      heldBarrier:HeldSessionBarrier,admission:MySqlBusinessAdmission,rollbackOnly:MySqlRollbackOnlyState,
      manifest:MySqlLockedManifest,binding:MySqlLockedBinding,writer:MySqlBusinessWriterRow)
      :(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.NormalRecallWriteReadOwner) =
    val owner=MySqlFencedTransaction.afterNormalRecallWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
      commandScope,operation,materialization,workflowPhase,effect,image,heldBarrier,admission,rollbackOnly,
      ownerThread,manifest,binding,writer)
    activeNormalRecallWriteReadOwner=owner
    try
      val actual=deploymentReceiptSession.capturePurposeNormalRecallWriteImage(owner)
      owner.requireSnapshot(actual)
      (actual,owner)
    finally activeNormalRecallWriteReadOwner=null

  private def readNormalRecallDeploymentPostCommit[A](commandScope:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation,materialization:MySqlNormalRecallMaterialization,
      effect:MySqlNormalRecallPhase,rollbackOnly:MySqlRollbackOnlyState,retainsProjection:Boolean)
      (consume:(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.NormalRecallWriteReadOwner)=>A):A =
    if !materialization.isDeploymentReceipt || effect==null || consume==null || !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("idle original Full26 mode post-commit read required")
    val workflowPhase=if effect.phase==MySqlOperationEffectPhase.Reserve then
      MySqlNormalRecallWorkflowPhase.ReserveCommittedAwaitingReadback
    else MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback
    val admission=coordinator.requireNormalRecallWorkflowAdmission(commandScope,operation,materialization,workflowPhase)
    commandScope.requireCurrent(scope,materialization.exactPlan)
    if commandScope.requireBusinessAdmission(scope,materialization.exactPlan) ne admission then
      throw new SQLNonTransientConnectionException("Full26 post-commit read lost original command admission")
    val held=commandScope.requireHeldSessionBarrier(scope,materialization.exactPlan)
    val oldIsolation=connection.getTransactionIsolation
    var attempted=false
    var restored=false
    var cleanupFailed=false
    var primary:Throwable=null
    var result=Option.empty[A]
    var owner:MySqlFencedTransaction.NormalRecallWriteReadOwner=null
    def cleanup(action: =>Unit):Unit =
      try action
      catch case cause:Throwable=>
        cleanupFailed=true
        if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
    try
      attempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("Full26 mode post-commit RR establishment failed")
      held.readmit(verified,expectation,scope)
      val manifest=lockManifest();validateManifest(manifest,admission)
      val binding=lockBinding();validateBinding(binding,admission)
      val writer=lockWriter();validateWriterIdentityAndLease(writer,admission)
      owner=MySqlFencedTransaction.afterNormalRecallWriteLockedRows(this,
        MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
        commandScope,operation,materialization,workflowPhase,effect,MySqlFencedTransaction.NormalRecallWriteReadImage.Target,
        held,admission,rollbackOnly,ownerThread,manifest,binding,writer)
      activeNormalRecallWriteReadOwner=owner
      val snapshot=if retainsProjection then deploymentReceiptSession.capturePurposeNormalRecallProjectionImage(owner)
        else deploymentReceiptSession.capturePurposeNormalRecallWriteImage(owner)
      owner.requireSnapshot(snapshot)
      result=Some(consume(snapshot,owner))
    catch case cause:Throwable=>primary=cause
    finally activeNormalRecallWriteReadOwner=null
    var rolledBack = !attempted
    if attempted then cleanup { connection.rollback();rolledBack=true }
    if rolledBack then cleanup {
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("Full26 post-commit read did not restore auto-commit")
      restored=true
    }
    if restored then cleanup {
      connection.setTransactionIsolation(oldIsolation)
      if connection.getTransactionIsolation!=oldIsolation then throw new SQLNonTransientConnectionException("Full26 post-commit read isolation restoration differs")
    }
    if cleanupFailed then throw new MySqlPlanningReadCleanupUncertainException(primary)
    if primary!=null then throw primary
    if owner==null || result.isEmpty then throw new SQLNonTransientConnectionException("Full26 post-commit read produced no actual result")
    owner.settlePurposeReadAfterTransactionEnd()
    result.get

  private def readNormalRecallLocked(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization,
      effect: MySqlNormalRecallPhase,
      workflowPhase: MySqlNormalRecallWorkflowPhase,
      image: MySqlFencedTransaction.NormalRecallWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    val owner = MySqlFencedTransaction.afterNormalRecallWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), verified, scope, expectation,
      coordinator, commandScope, operation, materialization, workflowPhase, effect, image, heldBarrier, admission,
      rollbackOnly, ownerThread, manifest, binding, writer)
    activeNormalRecallWriteReadOwner = owner
    try
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new SQLNonTransientConnectionException("normal recall full20 reader has no approved source limit profile")))
      reader.readWithinNormalRecallWriteTransaction(owner, materialization.preimage.allocationBudget)
    finally activeNormalRecallWriteReadOwner = null

  private def readNormalRecallPostCommit[A](effect: MySqlNormalRecallPhase,
      materialization: MySqlNormalRecallMaterialization,
      retainedTag: Option[IoOperationTag] = None)(consume: MySqlFormat3FullSnapshot => A): A =
    if effect == null || materialization == null || consume == null then
      throw new IllegalArgumentException("exact F3 post-commit effect, materialization and consumer are required")
    MySqlFencedTransaction.requireNormalRecallPostCommitRetainedTag(retainedTag)
    val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
      expectation.format3Inputs.map(_.limitProfile).getOrElse(
        throw new SQLNonTransientConnectionException("normal recall post-commit reader has no approved source limit profile")))
    val snapshot = reader.readWithinNormalRecallWorkflow(materialization.preimage.allocationBudget)
    retainedTag match
      case None => withScopedNormalRecallSnapshot(snapshot) { actual =>
        materialization.requireObserved(effect.phase, actual, true)
        consume(actual)
      }
      case Some(tag) =>
        val budget = snapshot.allocationBudget
        var retained = false
        try
          materialization.requireObserved(effect.phase, snapshot, true)
          val result = consume(snapshot)
          retainedNormalRecallProjectionBudgets.retainScoped(tag, budget)
          retained = true
          result
        catch
          case primary: Throwable =>
            if !retained then
              try budget.closeScopedReadBudget()
              catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
            throw primary

  /** Read images live only through their exact comparison/projection; release their share before the next phase. */
  private def withScopedNormalRecallSnapshot[A](snapshot: MySqlFormat3FullSnapshot)(consume: MySqlFormat3FullSnapshot => A): A =
    if snapshot == null || consume == null || snapshot.allocationBudget == null ||
      !snapshot.allocationBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("normal recall full20 image must carry its exact scoped workflow allocation")
    var primary: Throwable = null
    try consume(snapshot)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)


  private def readControlledDebugLocked(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      effect: MySqlControlledDebugPhase,
      workflowPhase: MySqlControlledDebugWorkflowPhase,
      image: MySqlFencedTransaction.ControlledDebugWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    val owner = MySqlFencedTransaction.afterControlledDebugWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), verified, scope, expectation,
      coordinator, commandScope, operation, materialization, workflowPhase, effect, image, heldBarrier, admission,
      rollbackOnly, ownerThread, manifest, binding, writer)
    activeControlledDebugWriteReadOwner = owner
    try
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new SQLNonTransientConnectionException("controlled debug full20 reader has no approved source limit profile")))
      reader.readWithinControlledDebugWriteTransaction(owner, materialization.preimage.allocationBudget)
    finally activeControlledDebugWriteReadOwner = null

  private def readControlledDebugDeploymentLocked(commandScope:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation,materialization:MySqlControlledDebugWorkflowMaterialization,
      effect:MySqlControlledDebugPhase,workflowPhase:MySqlControlledDebugWorkflowPhase,
      image:MySqlFencedTransaction.ControlledDebugWriteReadImage,heldBarrier:HeldSessionBarrier,
      admission:MySqlBusinessAdmission,rollbackOnly:MySqlRollbackOnlyState,manifest:MySqlLockedManifest,
      binding:MySqlLockedBinding,writer:MySqlBusinessWriterRow)
      :(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.ControlledDebugWriteReadOwner) =
    val owner=MySqlFencedTransaction.afterControlledDebugWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
      commandScope,operation,materialization,workflowPhase,effect,image,heldBarrier,admission,rollbackOnly,ownerThread,
      manifest,binding,writer)
    activeControlledDebugWriteReadOwner=owner
    try
      val snapshot=deploymentReceiptSession.capturePurposeControlledWriteImage(owner)
      owner.requireSnapshot(snapshot)
      (snapshot,owner)
    finally activeControlledDebugWriteReadOwner=null

  private def readControlledDebugDeploymentPostCommit[A](commandScope:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation,materialization:MySqlControlledDebugWorkflowMaterialization,
      effect:MySqlControlledDebugPhase,rollbackOnly:MySqlRollbackOnlyState,retainsProjection:Boolean)
      (consume:(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.ControlledDebugWriteReadOwner)=>A):A =
    if !materialization.isDeploymentReceipt || effect==null || consume==null || !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("idle original Full26 controlled post-commit read required")
    val workflowPhase=if effect.phase==MySqlOperationEffectPhase.Reserve then
      MySqlControlledDebugWorkflowPhase.ReserveCommittedAwaitingReadback
    else MySqlControlledDebugWorkflowPhase.ExecuteCommittedAwaitingReadback
    val admission=coordinator.requireControlledDebugWorkflowAdmission(commandScope,operation,materialization,workflowPhase)
    commandScope.requireCurrent(scope,materialization.exactPlan)
    if commandScope.requireBusinessAdmission(scope,materialization.exactPlan) ne admission then
      throw new SQLNonTransientConnectionException("Full26 post-commit read lost original command admission")
    val held=commandScope.requireHeldSessionBarrier(scope,materialization.exactPlan)
    val oldIsolation=connection.getTransactionIsolation
    var attempted=false
    var restored=false
    var cleanupFailed=false
    var primary:Throwable=null
    var result=Option.empty[A]
    var owner:MySqlFencedTransaction.ControlledDebugWriteReadOwner=null
    def cleanup(action: =>Unit):Unit =
      try action
      catch case cause:Throwable=>
        cleanupFailed=true
        if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
    try
      attempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("Full26 controlled post-commit RR establishment failed")
      held.readmit(verified,expectation,scope)
      val manifest=lockManifest();validateManifest(manifest,admission)
      val binding=lockBinding();validateBinding(binding,admission)
      val writer=lockWriter();validateWriterIdentityAndLease(writer,admission)
      owner=MySqlFencedTransaction.afterControlledDebugWriteLockedRows(this,
        MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
        commandScope,operation,materialization,workflowPhase,effect,MySqlFencedTransaction.ControlledDebugWriteReadImage.Target,
        held,admission,rollbackOnly,ownerThread,manifest,binding,writer)
      activeControlledDebugWriteReadOwner=owner
      val snapshot=if retainsProjection then deploymentReceiptSession.capturePurposeControlledProjectionImage(owner)
        else deploymentReceiptSession.capturePurposeControlledWriteImage(owner)
      owner.requireSnapshot(snapshot)
      result=Some(consume(snapshot,owner))
    catch case cause:Throwable=>primary=cause
    finally activeControlledDebugWriteReadOwner=null
    var rolledBack = !attempted
    if attempted then cleanup { connection.rollback();rolledBack=true }
    if rolledBack then cleanup {
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("Full26 post-commit read did not restore auto-commit")
      restored=true
    }
    if restored then cleanup {
      connection.setTransactionIsolation(oldIsolation)
      if connection.getTransactionIsolation!=oldIsolation then throw new SQLNonTransientConnectionException("Full26 post-commit read isolation restoration differs")
    }
    if cleanupFailed then throw new MySqlPlanningReadCleanupUncertainException(primary)
    if primary!=null then throw primary
    if owner==null || result.isEmpty then throw new SQLNonTransientConnectionException("Full26 post-commit read produced no actual result")
    owner.settlePurposeReadAfterTransactionEnd()
    result.get

  private def readControlledDebugPostCommit[A](effect: MySqlControlledDebugPhase,
      materialization: MySqlControlledDebugWorkflowMaterialization,
      retainedTag: Option[IoOperationTag] = None)(consume: MySqlFormat3FullSnapshot => A): A =
    if effect == null || materialization == null || consume == null then
      throw new IllegalArgumentException("exact F3 post-commit effect, materialization and consumer are required")
    MySqlFencedTransaction.requireControlledDebugPostCommitRetainedTag(retainedTag)
    val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
      expectation.format3Inputs.map(_.limitProfile).getOrElse(
        throw new SQLNonTransientConnectionException("controlled debug post-commit reader has no approved source limit profile")))
    val snapshot = reader.readWithinControlledDebugWorkflow(materialization.preimage.allocationBudget)
    retainedTag match
      case None => withScopedControlledDebugSnapshot(snapshot) { actual =>
        materialization.requireObserved(effect.phase, actual, true)
        consume(actual)
      }
      case Some(tag) =>
        val budget = snapshot.allocationBudget
        var retained = false
        try
          materialization.requireObserved(effect.phase, snapshot, true)
          val result = consume(snapshot)
          retainedControlledDebugProjectionBudgets.retainScoped(tag, budget)
          retained = true
          result
        catch
          case primary: Throwable =>
            if !retained then
              try budget.closeScopedReadBudget()
              catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
            throw primary

  /** Read images live only through their exact comparison/projection; release their share before the next phase. */
  private def withScopedControlledDebugSnapshot[A](snapshot: MySqlFormat3FullSnapshot)(consume: MySqlFormat3FullSnapshot => A): A =
    if snapshot == null || consume == null || snapshot.allocationBudget == null ||
      !snapshot.allocationBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("controlled debug full20 image must carry its exact scoped workflow allocation")
    var primary: Throwable = null
    try consume(snapshot)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)


  private def modeStateFromSnapshot(snapshot: MySqlModeOperationEffectCodec.Image,
      operation: MySqlRepositoryOperation, acceptedVersions: Set[Long]): FloatingPodState =
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    val shape = MySqlFormat3RelationShapes.forTable("floating_pod_state")
    val rows = snapshot.relation(shape.table).rows.filter(row =>
      row(shape.columns.indexWhere(_.name == "uuid")) match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.podUuid)
        case _ => false)
    if rows.size != 1 then throw new SQLDataException("post-commit full20 mode image has no unique exact pod row")
    val row = rows.head
    def cell(name: String): MySqlFormat3Cell = row(shape.columns.indexWhere(_.name == name))
    def signed(name: String): Long = cell(name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException("post-commit mode row has invalid integer " + name)
    def floating(name: String): Double = cell(name) match
      case MySqlFormat3Cell.Floating(raw) => java.lang.Double.longBitsToDouble(raw)
      case _ => throw new SQLDataException("post-commit mode row has invalid DOUBLE " + name)
    cell("uuid") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.equalsUuid(request.podUuid) => ()
      case _ => throw new SQLDataException("post-commit mode row UUID is invalid")
    val owner = cell("owner_uuid") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(bytes) =>
        val copy = bytes.copyBytes(snapshot.allocationBudget, "mode outcome owner UUID")
        var primary: Throwable = null
        try
          val buffer = java.nio.ByteBuffer.wrap(copy)
          Some(new UUID(buffer.getLong(), buffer.getLong()))
        catch
          case failure: Throwable => primary = failure; throw failure
        finally
          java.util.Arrays.fill(copy, 0.toByte)
          try snapshot.allocationBudget.releaseRetainedMemory(Math.addExact(copy.length.toLong, 64L), "mode outcome owner UUID release")
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else if cleanup ne primary then primary.addSuppressed(cleanup)
      case _ => throw new SQLDataException("post-commit mode row owner UUID is invalid")
    val dimension = cell("dimension_id") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.SignedInteger(value) if value >= Int.MinValue.toLong && value <= Int.MaxValue.toLong => Some(value.toInt)
      case _ => throw new SQLDataException("post-commit mode row dimension is invalid")
    val active = signed("is_active")
    if active < Int.MinValue.toLong || active > Int.MaxValue.toLong then
      throw new SQLDataException("post-commit mode row active flag is outside INT range")
    val data = MySqlStoredRowData(
      MySqlStoredRows.encodeUuid(request.podUuid), owner.map(MySqlStoredRows.encodeUuid), signed("state_version"),
      cell("world_name") match
        case MySqlFormat3Cell.Text(value) => value
        case _ => throw new SQLDataException("post-commit mode row world is invalid"),
      floating("pos_x"), floating("pos_y"), floating("pos_z"), floating("yaw"), floating("pitch"),
      active.toInt, signed("last_updated"), dimension,
      cell("operating_mode") match
        case MySqlFormat3Cell.Text(value) => value
        case _ => throw new SQLDataException("post-commit mode row operating mode is invalid"))
    val state = MySqlStoredRows.materializeGenericState(data)
    if state.ownerUuid != Some(request.owner) || !state.isActive || state.operatingMode != request.targetMode ||
      !acceptedVersions(state.stateVersion) ||
      state.worldName != request.actualPose.worldName || state.posX != request.actualPose.posX ||
      state.posY != request.actualPose.posY || state.posZ != request.actualPose.posZ ||
      state.yaw != request.actualPose.yaw || state.pitch != request.actualPose.pitch ||
      state.dimensionId != Some(request.actualPose.dimensionId) then
      throw new SQLDataException("post-commit mode state differs from exact owned one-version transition")
    state

  /**
   * 在 M→B→W 锁与同连接事务内读取完整图，闭包只暂存不可变状态；冻结后才执行业务 DML。
   * 自动分配且仅使用一次 operation UUID；空效果回滚只读事务，不占用 writer receipt。
   */
  private[mysql] def executeDriftWorkflow(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      activeLimit: Int
  ): Try[MySqlTrackedDriftExecution] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            Failure(new SQLNonTransientConnectionException("mode execution must run on its captured owned IO thread"))
          else if commandScope == null || operation == null || materialization == null ||
            activeLimit <= 0 || activeLimit > 256 then
            Failure(new IllegalArgumentException("exact mode command, full20 materialization and bounded active limit are required"))
          else
            try Success(executeDriftWorkflowOwned(commandScope, operation, materialization, activeLimit, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private def executeDriftWorkflowOwned(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      activeLimit: Int,
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlTrackedDriftExecution =
    if !Set(MySqlSchemaAuditProfile.Format3,MySqlSchemaAuditProfile.DeploymentReceipt1).contains(expectation.profile) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("owned mode SQL execution requires the approved format3/physical2 target")
    materialization.requireBoundTo(commandScope, operation)
    if commandScope.exactAuditScope() ne scope then
      throw new SQLNonTransientConnectionException("Main mode command scope differs from the exact owned F3 session")
    coordinator.beginDriftWorkflow(commandScope, operation, materialization)
    coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization,
      MySqlDriftWorkflowPhase.PreparedForReserve)

    runDriftCriticalPhase(commandScope, operation, materialization, DriftSqlPhase.Reserve, rollbackOnly)
    val reserveEffect = materialization.effects(0)
    try
      if materialization.isDeploymentReceipt then
        readDriftDeploymentPostCommit(commandScope,operation,materialization,reserveEffect,rollbackOnly,false)((_,_)=>())
      else readDriftPostCommit(reserveEffect, materialization)(_ => ())
    catch
      case primary: Throwable =>
        val blocked = coordinator.markDriftReserveReadbackUnknown(commandScope, operation, materialization, primary)
        throw blocked
    coordinator.recordDriftReserveAcknowledged(commandScope, operation, materialization)

    coordinator.captureDriftExecuteFailureAfterReserve(commandScope, operation, materialization) {
      coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization,
        MySqlDriftWorkflowPhase.ReserveCommittedExecutePending)
      runDriftCriticalPhase(commandScope, operation, materialization, DriftSqlPhase.Execute, rollbackOnly)
    }
    val executeEffect = materialization.effects(1)
    try
      def resultFrom(executeReadback:MySqlModeOperationEffectCodec.Image,
          root:MySqlFormat3ReadBudget):MySqlTrackedDriftExecution =
        val state = driftStateFromSnapshot(executeReadback, operation,
          Set(Math.addExact(MySqlDriftOperationEffectCodec.driftRequest(operation).expectedStateVersion, 1L)))
        val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
        val commitRequest = MySqlTrackedDriftRequest(request.binding, request.criticalBase,
          request.criticalBase.copy(posX = request.actualPose.posX, posY = request.actualPose.posY,
            posZ = request.actualPose.posZ, yaw = request.actualPose.yaw, pitch = request.actualPose.pitch), request.attemptUuid)
        val graph = MySqlCommittedTrackedDriftGraph.fromFormat3Readback(commandScope.tag(),
          commandScope.operationPlan(), commitRequest, state, executeReadback, activeLimit, root)
        MySqlTrackedDriftExecution.Committed(graph)
      val (result,proof)=if materialization.isDeploymentReceipt then
        readDriftDeploymentPostCommit(commandScope,operation,materialization,executeEffect,rollbackOnly,true) { (actual,owner)=>
          val proof=MySqlFencedTransaction.issueDriftDeploymentPostCommitReadback(this,commandScope,operation,materialization,actual,owner)
          (resultFrom(actual,owner.originalPurposeRoot),proof)
        }
      else readDriftPostCommit(executeEffect,materialization,Some(commandScope.tag())) { actual=>
        val proof=MySqlFencedTransaction.issueDriftPostCommitReadback(this,commandScope,operation,materialization,actual)
        (resultFrom(actual,materialization.preimage.allocationBudget),proof)
      }
      coordinator.completeDriftWorkflow(proof)
      result
    catch
      case primary: Throwable =>
        val blocked = coordinator.markDriftExecuteReadbackUnknown(commandScope, operation, materialization, primary)
        throw blocked
  private def runInactiveDomainCriticalPhase(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      phase: InactiveDomainSqlPhase,
      rollbackOnly: MySqlRollbackOnlyState
  ): Unit =
    val isReserve = phase == InactiveDomainSqlPhase.Reserve
    val workflowPhase = if isReserve then MySqlInactiveDomainWorkflowPhase.PreparedForReserve
      else MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending
    val effect = materialization.effects(if isReserve then 0 else 1)
    val expectedBefore = if isReserve then materialization.preimage else materialization.reserveTarget
    val expectedTarget = if isReserve then materialization.reserveTarget else materialization.executeTarget
    val plan = materialization.exactPlan
    var admission: MySqlBusinessAdmission = null
    var heldBarrier: HeldSessionBarrier = null
    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var transactionAttempted = false
    var idleAutocommitObserved = false
    var commitAttempted = false
    var commitAcknowledged = false
    var acknowledgmentRecorded = false
    var cleanupFailed = false
    var primary: Throwable = null

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      admission = coordinator.requireInactiveDomainWorkflowAdmission(commandScope, operation, materialization, workflowPhase)
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireBusinessAdmission(scope, plan) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission differs from exact Main command admission")
      heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      if !connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("mode critical phase requires an idle auto-commit owned connection")
      idleAutocommitObserved = true
      if connection.isClosed then throw new SQLNonTransientConnectionException("mode critical phase connection is closed")

      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode critical phase could not establish REPEATABLE READ")
      transactionAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode critical transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if coordinator.requireInactiveDomainWorkflowAdmission(commandScope, operation, materialization, workflowPhase) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission changed after M→B→W locks")
      withScopedInactiveDomainSnapshot(readInactiveDomainLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.InactiveDomainWriteReadImage.Before, heldBarrier, admission, rollbackOnly,
        manifest, binding, writer)) { actualBefore =>
        if !expectedBefore.exactlyEquals(actualBefore) then
          throw new SQLTransactionRollbackException("locked full20 mode preimage differs from the exact retained phase")
      }

      phase match
        case InactiveDomainSqlPhase.Reserve =>
          MySqlInactiveDomainDeltaWriter.writeReserve(connection, expectation.schema, materialization)
        case InactiveDomainSqlPhase.Execute =>
          MySqlInactiveDomainDeltaWriter.writeExecute(connection, expectation.schema, materialization)

      val context = effect.exactOperationContext
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, receipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(context.canonicalFingerprintSha256) then
        throw new SQLDataException("mode phase writer receipt did not read back the exact operation and fingerprint")
      withScopedInactiveDomainSnapshot(readInactiveDomainLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.InactiveDomainWriteReadImage.Target, heldBarrier, admission, rollbackOnly,
        manifest, binding, writerAfterUpdate)) { actualTarget =>
        if !expectedTarget.exactlyEquals(actualTarget) then
          throw new SQLTransactionRollbackException("locked full20 mode target differs before critical commit")
      }
      rollbackOnly.cause.foreach(throw _)

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      if isReserve then
        coordinator.markInactiveDomainReserveAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      else
        coordinator.markInactiveDomainExecuteAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      acknowledgmentRecorded = true
    catch
      case failure: Throwable => primary = failure
    finally
      activeInactiveDomainWriteReadOwner = null

    if commitAttempted && !commitAcknowledged then
      val cause = if primary != null then primary else new SQLException("mode phase commit reply was not acknowledged")
      val blocked =
        if isReserve then coordinator.markInactiveDomainReserveCommitUnknown(commandScope, operation, materialization, cause)
        else coordinator.markInactiveDomainExecuteCommitUnknown(commandScope, operation, materialization, cause)
      throw blocked

    if commitAcknowledged then
      var restored = false
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode commit did not restore auto-commit")
        restored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && isolationChangeAttempted && isolationRead then
        try
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then
            throw new SQLNonTransientConnectionException("mode commit did not restore the previous isolation")
        catch case failure: Throwable => recordCleanupFailure(failure)
      if cleanupFailed || !acknowledgmentRecorded then
        val cause = if primary != null then primary else new SQLNonTransientConnectionException("mode commit acknowledgement cleanup is uncertain")
        val blocked =
          if isReserve then coordinator.markInactiveDomainReserveReadbackUnknown(commandScope, operation, materialization, cause)
          else coordinator.markInactiveDomainExecuteReadbackUnknown(commandScope, operation, materialization, cause)
        throw blocked
      return

    var rollbackAcknowledged = !transactionAttempted
    val initialCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, false, isolationChangeAttempted, isolationRead, cleanupFailed)
    if initialCleanup.rollback then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    // A preflight rejection owns no transaction. Never mutate a caller's existing JDBC transaction.
    var autocommitRestored = !transactionAttempted && idleAutocommitObserved
    val afterRollback = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterRollback.restoreAutoCommit then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode rollback did not restore auto-commit")
        autocommitRestored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    val afterAutoCommit = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterAutoCommit.restoreIsolation then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("mode rollback did not restore the previous isolation")
      catch case failure: Throwable => recordCleanupFailure(failure)

    if primary == null then primary = new SQLException("mode phase failed without preserving its original cause")
    val finalCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if finalCleanup.connectionUncertain then
      val wrapped = new MySqlTransactionCleanupUncertainException(effect.exactOperationContext, primary)
      throw wrapped
    if !isReserve && !commitAttempted then
      throw coordinator.recordInactiveDomainExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    throw primary

  private[mysql] def hasRetainedInactiveDomainProjection(tag: IoOperationTag): Boolean =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("inactive retention is owned IO only")
    retainedInactiveDomainMaterializations.contains(tag)

  private[mysql] def releaseRetainedInactiveDomainProjection(tag: IoOperationTag): Unit =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("mode projection budget release requires its exact owned IO thread")
    retainedInactiveDomainMaterializations.remove(tag)
    retainedInactiveDomainProjectionBudgets.release(tag)

  private[mysql] def retainedInactiveDomainProjectionCount: Int =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("mode projection budget inspection requires its exact owned IO thread")
    retainedInactiveDomainProjectionBudgets.pendingCount

  private def readInactiveDomainLocked(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      effect: MySqlInactiveDomainPhase,
      workflowPhase: MySqlInactiveDomainWorkflowPhase,
      image: MySqlFencedTransaction.InactiveDomainWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    val owner = MySqlFencedTransaction.afterInactiveDomainWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), verified, scope, expectation,
      coordinator, commandScope, operation, materialization, workflowPhase, effect, image, heldBarrier, admission,
      rollbackOnly, ownerThread, manifest, binding, writer)
    activeInactiveDomainWriteReadOwner = owner
    try
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new SQLNonTransientConnectionException("mode full20 reader has no approved source limit profile")))
      reader.readWithinInactiveDomainWriteTransaction(owner, materialization.preimage.allocationBudget)
    finally activeInactiveDomainWriteReadOwner = null

  private def readInactiveDomainPostCommit[A](effect: MySqlInactiveDomainPhase,
      materialization: MySqlInactiveDomainWorkflowMaterialization,
      retainedTag: Option[IoOperationTag] = None)(consume: MySqlFormat3FullSnapshot => A): A =
    if effect == null || materialization == null || consume == null then
      throw new IllegalArgumentException("exact F3 post-commit effect, materialization and consumer are required")
    MySqlFencedTransaction.requireInactiveDomainPostCommitRetainedTag(retainedTag)
    val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
      expectation.format3Inputs.map(_.limitProfile).getOrElse(
        throw new SQLNonTransientConnectionException("mode post-commit reader has no approved source limit profile")))
    val snapshot = reader.readWithinInactiveDomainWorkflow(materialization.preimage.allocationBudget)
    retainedTag match
      case None => withScopedInactiveDomainSnapshot(snapshot) { actual =>
        effect.verifyReadback(actual)
        consume(actual)
      }
      case Some(tag) =>
        val budget = snapshot.allocationBudget
        var retained = false
        try
          effect.verifyReadback(snapshot)
          val result = consume(snapshot)
          retainedInactiveDomainProjectionBudgets.retainScoped(tag, budget)
          retained = true
          result
        catch
          case primary: Throwable =>
            if !retained then
              try budget.closeScopedReadBudget()
              catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
            throw primary

  /** Read images live only through their exact comparison/projection; release their share before the next phase. */
  private def withScopedInactiveDomainSnapshot[A](snapshot: MySqlFormat3FullSnapshot)(consume: MySqlFormat3FullSnapshot => A): A =
    if snapshot == null || consume == null || snapshot.allocationBudget == null ||
      !snapshot.allocationBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("mode full20 image must carry its exact scoped workflow allocation")
    var primary: Throwable = null
    try consume(snapshot)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def runDriftCriticalPhase(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      phase: DriftSqlPhase,
      rollbackOnly: MySqlRollbackOnlyState
  ): Unit =
    val isReserve = phase == DriftSqlPhase.Reserve
    val workflowPhase = if isReserve then MySqlDriftWorkflowPhase.PreparedForReserve
      else MySqlDriftWorkflowPhase.ReserveCommittedExecutePending
    val effect = materialization.effects(if isReserve then 0 else 1)
    val expectedBefore = if isReserve then materialization.completePreimage else materialization.completeReserveTarget
    val expectedTarget = if isReserve then materialization.completeReserveTarget else materialization.completeExecuteTarget
    val plan = materialization.exactPlan
    var admission: MySqlBusinessAdmission = null
    var heldBarrier: HeldSessionBarrier = null
    var oldIsolation = Connection.TRANSACTION_READ_COMMITTED
    var isolationRead = false
    var isolationChangeAttempted = false
    var transactionAttempted = false
    var idleAutocommitObserved = false
    var commitAttempted = false
    var commitAcknowledged = false
    var acknowledgmentRecorded = false
    var cleanupFailed = false
    var primary: Throwable = null
    var modePurposeOwners=List.empty[MySqlFencedTransaction.DriftWriteReadOwner]

    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if (failure ne primary) && !primary.getSuppressed.contains(failure) then primary.addSuppressed(failure)

    try
      admission = coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization, workflowPhase)
      commandScope.requireCurrent(scope, plan)
      if commandScope.requireBusinessAdmission(scope, plan) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission differs from exact Main command admission")
      heldBarrier = commandScope.requireHeldSessionBarrier(scope, plan)
      if !connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("mode critical phase requires an idle auto-commit owned connection")
      idleAutocommitObserved = true
      if connection.isClosed then throw new SQLNonTransientConnectionException("mode critical phase connection is closed")

      oldIsolation = connection.getTransactionIsolation
      isolationRead = true
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("mode critical phase could not establish REPEATABLE READ")
      transactionAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode critical transaction did not start")
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)
      if coordinator.requireDriftWorkflowAdmission(commandScope, operation, materialization, workflowPhase) ne admission then
        throw new SQLNonTransientConnectionException("mode phase admission changed after M→B→W locks")
      if expectation.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 then
        val (_,owner)=readDriftDeploymentLocked(commandScope,operation,materialization,effect,workflowPhase,
          MySqlFencedTransaction.DriftWriteReadImage.Before,heldBarrier,admission,rollbackOnly,manifest,binding,writer)
        modePurposeOwners=owner::modePurposeOwners
      else withScopedDriftSnapshot(readDriftLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.DriftWriteReadImage.Before, heldBarrier, admission, rollbackOnly,
        manifest, binding, writer)) { actualBefore =>
        if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedBefore,actualBefore) then
          throw new SQLTransactionRollbackException("locked full20 mode preimage differs from the exact retained phase")
      }

      phase match
        case DriftSqlPhase.Reserve =>
          MySqlDriftFormat3DeltaWriter.writeReserve(connection, expectation.schema, materialization)
        case DriftSqlPhase.Execute =>
          MySqlDriftFormat3DeltaWriter.writeExecute(connection, expectation.schema, materialization)

      val context = effect.exactOperationContext
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, receipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(context.canonicalFingerprintSha256) then
        throw new SQLDataException("mode phase writer receipt did not read back the exact operation and fingerprint")
      if expectation.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 then
        val (_,owner)=readDriftDeploymentLocked(commandScope,operation,materialization,effect,workflowPhase,
          MySqlFencedTransaction.DriftWriteReadImage.Target,heldBarrier,admission,rollbackOnly,manifest,binding,writerAfterUpdate)
        modePurposeOwners=owner::modePurposeOwners
      else withScopedDriftSnapshot(readDriftLocked(commandScope, operation, materialization, effect,
        workflowPhase, MySqlFencedTransaction.DriftWriteReadImage.Target, heldBarrier, admission, rollbackOnly,
        manifest, binding, writerAfterUpdate)) { actualTarget =>
        if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedTarget,actualTarget) then
          throw new SQLTransactionRollbackException("locked full20 mode target differs before critical commit")
      }
      rollbackOnly.cause.foreach(throw _)

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      if isReserve then
        coordinator.markDriftReserveAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      else
        coordinator.markDriftExecuteAcknowledgedAwaitingReadback(commandScope, operation, materialization)
      acknowledgmentRecorded = true
    catch
      case failure: Throwable => primary = failure
    finally
      activeDriftWriteReadOwner = null

    if commitAttempted && !commitAcknowledged then
      val cause = if primary != null then primary else new SQLException("mode phase commit reply was not acknowledged")
      val blocked =
        if isReserve then coordinator.markDriftReserveCommitUnknown(commandScope, operation, materialization, cause)
        else coordinator.markDriftExecuteCommitUnknown(commandScope, operation, materialization, cause)
      throw blocked

    if commitAcknowledged then
      var restored = false
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode commit did not restore auto-commit")
        restored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && isolationChangeAttempted && isolationRead then
        try
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then
            throw new SQLNonTransientConnectionException("mode commit did not restore the previous isolation")
        catch case failure: Throwable => recordCleanupFailure(failure)
      if restored && !cleanupFailed && acknowledgmentRecorded then
        modePurposeOwners.reverse.foreach { owner=>
          try owner.settlePurposeReadAfterTransactionEnd()
          catch case failure:Throwable=>recordCleanupFailure(failure)
        }
      if cleanupFailed || !acknowledgmentRecorded then
        val cause = if primary != null then primary else new SQLNonTransientConnectionException("mode commit acknowledgement cleanup is uncertain")
        val blocked =
          if isReserve then coordinator.markDriftReserveReadbackUnknown(commandScope, operation, materialization, cause)
          else coordinator.markDriftExecuteReadbackUnknown(commandScope, operation, materialization, cause)
        throw blocked
      return

    var rollbackAcknowledged = !transactionAttempted
    val initialCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, false, isolationChangeAttempted, isolationRead, cleanupFailed)
    if initialCleanup.rollback then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    // A preflight rejection owns no transaction. Never mutate a caller's existing JDBC transaction.
    var autocommitRestored = !transactionAttempted && idleAutocommitObserved
    val afterRollback = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterRollback.restoreAutoCommit then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("mode rollback did not restore auto-commit")
        autocommitRestored = true
      catch case failure: Throwable => recordCleanupFailure(failure)
    val afterAutoCommit = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if afterAutoCommit.restoreIsolation then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("mode rollback did not restore the previous isolation")
      catch case failure: Throwable => recordCleanupFailure(failure)

    if primary == null then primary = new SQLException("mode phase failed without preserving its original cause")
    val finalCleanup = MySqlModeCriticalCleanupPolicy.decide(transactionAttempted, idleAutocommitObserved,
      rollbackAcknowledged, autocommitRestored, isolationChangeAttempted, isolationRead, cleanupFailed)
    if finalCleanup.connectionUncertain then
      val wrapped = new MySqlTransactionCleanupUncertainException(effect.exactOperationContext, primary)
      throw wrapped
    if !isReserve && !commitAttempted then
      throw coordinator.recordDriftExecuteFailureAfterReserve(commandScope, operation, materialization, primary)
    throw primary

  private def readDriftLocked(
      commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization,
      effect: MySqlDriftOperationEffect,
      workflowPhase: MySqlDriftWorkflowPhase,
      image: MySqlFencedTransaction.DriftWriteReadImage,
      heldBarrier: HeldSessionBarrier,
      admission: MySqlBusinessAdmission,
      rollbackOnly: MySqlRollbackOnlyState,
      manifest: MySqlLockedManifest,
      binding: MySqlLockedBinding,
      writer: MySqlBusinessWriterRow
  ): MySqlFormat3FullSnapshot =
    val owner = MySqlFencedTransaction.afterDriftWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), verified, scope, expectation,
      coordinator, commandScope, operation, materialization, workflowPhase, effect, image, heldBarrier, admission,
      rollbackOnly, ownerThread, manifest, binding, writer)
    activeDriftWriteReadOwner = owner
    try
      val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new SQLNonTransientConnectionException("mode full20 reader has no approved source limit profile")))
      reader.readWithinDriftWriteTransaction(owner, materialization.preimage.allocationBudget)
    finally activeDriftWriteReadOwner = null

  private def readDriftDeploymentLocked(commandScope:MySqlRepositoryCommandScope,operation:MySqlRepositoryOperation,
      materialization:MySqlDriftWorkflowMaterialization,effect:MySqlDriftOperationEffect,
      workflowPhase:MySqlDriftWorkflowPhase,image:MySqlFencedTransaction.DriftWriteReadImage,
      heldBarrier:HeldSessionBarrier,admission:MySqlBusinessAdmission,rollbackOnly:MySqlRollbackOnlyState,
      manifest:MySqlLockedManifest,binding:MySqlLockedBinding,writer:MySqlBusinessWriterRow)
      :(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.DriftWriteReadOwner) =
    val owner=MySqlFencedTransaction.afterDriftWriteLockedRows(this,
      MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
      commandScope,operation,materialization,workflowPhase,effect,image,heldBarrier,admission,rollbackOnly,
      ownerThread,manifest,binding,writer)
    activeDriftWriteReadOwner=owner
    try
      val actual=deploymentReceiptSession.capturePurposeDriftWriteImage(owner)
      owner.requireSnapshot(actual)
      (actual,owner)
    finally activeDriftWriteReadOwner=null

  private def readDriftDeploymentPostCommit[A](commandScope:MySqlRepositoryCommandScope,
      operation:MySqlRepositoryOperation,materialization:MySqlDriftWorkflowMaterialization,
      effect:MySqlDriftOperationEffect,rollbackOnly:MySqlRollbackOnlyState,retainsProjection:Boolean)
      (consume:(MySqlScriptSchemaSnapshot,MySqlFencedTransaction.DriftWriteReadOwner)=>A):A =
    if !materialization.isDeploymentReceipt || effect==null || consume==null || !connection.getAutoCommit then
      throw new SQLNonTransientConnectionException("idle original Full26 mode post-commit read required")
    val workflowPhase=if effect.phase==MySqlOperationEffectPhase.Reserve then
      MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback
    else MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback
    val admission=coordinator.requireDriftWorkflowAdmission(commandScope,operation,materialization,workflowPhase)
    commandScope.requireCurrent(scope,materialization.exactPlan)
    if commandScope.requireBusinessAdmission(scope,materialization.exactPlan) ne admission then
      throw new SQLNonTransientConnectionException("Full26 post-commit read lost original command admission")
    val held=commandScope.requireHeldSessionBarrier(scope,materialization.exactPlan)
    val oldIsolation=connection.getTransactionIsolation
    var attempted=false
    var restored=false
    var cleanupFailed=false
    var primary:Throwable=null
    var result=Option.empty[A]
    var owner:MySqlFencedTransaction.DriftWriteReadOwner=null
    def cleanup(action: =>Unit):Unit =
      try action
      catch case cause:Throwable=>
        cleanupFailed=true
        if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
    try
      attempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("Full26 mode post-commit RR establishment failed")
      held.readmit(verified,expectation,scope)
      val manifest=lockManifest();validateManifest(manifest,admission)
      val binding=lockBinding();validateBinding(binding,admission)
      val writer=lockWriter();validateWriterIdentityAndLease(writer,admission)
      owner=MySqlFencedTransaction.afterDriftWriteLockedRows(this,
        MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly),verified,scope,expectation,coordinator,
        commandScope,operation,materialization,workflowPhase,effect,MySqlFencedTransaction.DriftWriteReadImage.Target,
        held,admission,rollbackOnly,ownerThread,manifest,binding,writer)
      activeDriftWriteReadOwner=owner
      val snapshot=if retainsProjection then deploymentReceiptSession.capturePurposeDriftProjectionImage(owner)
        else deploymentReceiptSession.capturePurposeDriftWriteImage(owner)
      owner.requireSnapshot(snapshot)
      result=Some(consume(snapshot,owner))
    catch case cause:Throwable=>primary=cause
    finally activeDriftWriteReadOwner=null
    var rolledBack = !attempted
    if attempted then cleanup { connection.rollback();rolledBack=true }
    if rolledBack then cleanup {
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("Full26 post-commit read did not restore auto-commit")
      restored=true
    }
    if restored then cleanup {
      connection.setTransactionIsolation(oldIsolation)
      if connection.getTransactionIsolation!=oldIsolation then throw new SQLNonTransientConnectionException("Full26 post-commit read isolation restoration differs")
    }
    if cleanupFailed then throw new MySqlPlanningReadCleanupUncertainException(primary)
    if primary!=null then throw primary
    if owner==null || result.isEmpty then throw new SQLNonTransientConnectionException("Full26 post-commit read produced no actual result")
    owner.settlePurposeReadAfterTransactionEnd()
    result.get

  private def readDriftPostCommit[A](effect: MySqlDriftOperationEffect,
      materialization: MySqlDriftWorkflowMaterialization,
      retainedTag: Option[IoOperationTag] = None)(consume: MySqlFormat3FullSnapshot => A): A =
    if effect == null || materialization == null || consume == null then
      throw new IllegalArgumentException("exact F3 post-commit effect, materialization and consumer are required")
    MySqlFencedTransaction.requireDriftPostCommitRetainedTag(retainedTag)
    val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
      expectation.format3Inputs.map(_.limitProfile).getOrElse(
        throw new SQLNonTransientConnectionException("mode post-commit reader has no approved source limit profile")))
    val snapshot = reader.readWithinDriftWorkflow(materialization.preimage.allocationBudget)
    retainedTag match
      case None => withScopedDriftSnapshot(snapshot) { actual =>
        effect.verifyReadback(actual)
        consume(actual)
      }
      case Some(tag) =>
        val budget = snapshot.allocationBudget
        var retained = false
        try
          effect.verifyReadback(snapshot)
          val result = consume(snapshot)
          retainedDriftProjectionBudgets.retainScoped(tag, budget)
          retained = true
          result
        catch
          case primary: Throwable =>
            if !retained then
              try budget.closeScopedReadBudget()
              catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
            throw primary

  /** Read images live only through their exact comparison/projection; release their share before the next phase. */
  private def withScopedDriftSnapshot[A](snapshot: MySqlFormat3FullSnapshot)(consume: MySqlFormat3FullSnapshot => A): A =
    if snapshot == null || consume == null || snapshot.allocationBudget == null ||
      !snapshot.allocationBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("mode full20 image must carry its exact scoped workflow allocation")
    var primary: Throwable = null
    try consume(snapshot)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def driftStateFromSnapshot(snapshot: MySqlModeOperationEffectCodec.Image,
      operation: MySqlRepositoryOperation, acceptedVersions: Set[Long]): FloatingPodState =
    val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
    val shape = MySqlFormat3RelationShapes.forTable("floating_pod_state")
    val rows = snapshot.relation(shape.table).rows.filter(row =>
      row(shape.columns.indexWhere(_.name == "uuid")) match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.podUuid)
        case _ => false)
    if rows.size != 1 then throw new SQLDataException("post-commit full20 mode image has no unique exact pod row")
    val row = rows.head
    def cell(name: String): MySqlFormat3Cell = row(shape.columns.indexWhere(_.name == name))
    def signed(name: String): Long = cell(name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException("post-commit mode row has invalid integer " + name)
    def floating(name: String): Double = cell(name) match
      case MySqlFormat3Cell.Floating(raw) => java.lang.Double.longBitsToDouble(raw)
      case _ => throw new SQLDataException("post-commit mode row has invalid DOUBLE " + name)
    cell("uuid") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.equalsUuid(request.podUuid) => ()
      case _ => throw new SQLDataException("post-commit mode row UUID is invalid")
    val owner = cell("owner_uuid") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(bytes) =>
        val copy = bytes.copyBytes(snapshot.allocationBudget, "mode outcome owner UUID")
        var primary: Throwable = null
        try
          val buffer = java.nio.ByteBuffer.wrap(copy)
          Some(new UUID(buffer.getLong(), buffer.getLong()))
        catch
          case failure: Throwable => primary = failure; throw failure
        finally
          java.util.Arrays.fill(copy, 0.toByte)
          try snapshot.allocationBudget.releaseRetainedMemory(Math.addExact(copy.length.toLong, 64L), "mode outcome owner UUID release")
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else if cleanup ne primary then primary.addSuppressed(cleanup)
      case _ => throw new SQLDataException("post-commit mode row owner UUID is invalid")
    val dimension = cell("dimension_id") match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.SignedInteger(value) if value >= Int.MinValue.toLong && value <= Int.MaxValue.toLong => Some(value.toInt)
      case _ => throw new SQLDataException("post-commit mode row dimension is invalid")
    val active = signed("is_active")
    if active < Int.MinValue.toLong || active > Int.MaxValue.toLong then
      throw new SQLDataException("post-commit mode row active flag is outside INT range")
    val data = MySqlStoredRowData(
      MySqlStoredRows.encodeUuid(request.podUuid), owner.map(MySqlStoredRows.encodeUuid), signed("state_version"),
      cell("world_name") match
        case MySqlFormat3Cell.Text(value) => value
        case _ => throw new SQLDataException("post-commit mode row world is invalid"),
      floating("pos_x"), floating("pos_y"), floating("pos_z"), floating("yaw"), floating("pitch"),
      active.toInt, signed("last_updated"), dimension,
      cell("operating_mode") match
        case MySqlFormat3Cell.Text(value) => value
        case _ => throw new SQLDataException("post-commit mode row operating mode is invalid"))
    val state = MySqlStoredRows.materializeGenericState(data)
    if state.ownerUuid != request.owner || !state.isActive || state.operatingMode != request.targetMode ||
      !acceptedVersions(state.stateVersion) ||
      state.worldName != request.actualPose.worldName || state.posX != request.actualPose.posX ||
      state.posY != request.actualPose.posY || state.posZ != request.actualPose.posZ ||
      state.yaw != request.actualPose.yaw || state.pitch != request.actualPose.pitch ||
      state.dimensionId != Some(request.actualPose.dimensionId) then
      throw new SQLDataException("post-commit mode state differs from exact owned one-version transition")
    state

  /**
   * 在 M→B→W 锁与同连接事务内读取完整图，闭包只暂存不可变状态；冻结后才执行业务 DML。
   * 自动分配且仅使用一次 operation UUID；空效果回滚只读事务，不占用 writer receipt。
   */
  def executeStaged[A](body: MySqlRepositoryStaging => Try[A]): Try[A] =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => Failure(nestedFailure)
      case None =>
        try
          if Thread.currentThread() ne ownerThread then
            val failure = new IllegalStateException("MySQL staged transaction must run on its captured owned IO thread")
            rollbackOnly.mark(failure)
            Failure(failure)
          else if body == null then
            val failure = new IllegalArgumentException("MySQL staged transaction body is required")
            rollbackOnly.mark(failure)
            Failure(failure)
          else
            try Success(executeStagedOwned(body, rollbackOnly))
            catch case failure: Throwable => Failure(failure)
        finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private def executeStagedOwned[A](
      body: MySqlRepositoryStaging => Try[A],
      rollbackOnly: MySqlRollbackOnlyState
  ): A =
    val operationUuid = UUID.randomUUID()
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    val validationContext = stagedContext(admission, operationUuid)
    validateAdmission(validationContext, admission)
    if !connection.getAutoCommit then
      MySqlSchemaAuditor.reject("MySQL full repository transaction requires an idle auto-commit connection")

    var transactionAttempted = false
    var cleanupComplete = false
    var commitAttempted = false
    var commitAcknowledged = false
    var staging: MySqlRepositoryStaging = null
    var operationContext = validationContext
    var effectPlan: MySqlBusinessEffectPlan = null

    try
      transactionAttempted = true
      connection.setAutoCommit(false)
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriterIdentityAndLease(writer, admission)

      val before = MySqlFullSnapshotJdbc.read(connection, expectation.schema, lockRows = true)
      staging = new MySqlRepositoryStaging(before, ownerThread)
      val bodyResult =
        try body(staging)
        catch case failure: Throwable => Failure(failure)
      if bodyResult == null then
        val failure = new IllegalStateException("MySQL staged transaction body returned a null Try")
        rollbackOnly.mark(failure)
        throw failure
      bodyResult match
        case Failure(failure) =>
          rollbackOnly.mark(failure)
          throw rollbackOnly.cause.getOrElse(failure)
        case Success(_) => ()

      rollbackOnly.cause.foreach(failure => throw failure)
      val stagedResult = bodyResult.get
      val stagedPlan = staging.closeAndFreezePlan() match
        case Success(value) => value
        case Failure(failure) => throw failure
      staging = null
      rollbackOnly.cause.foreach(failure => throw failure)
      val after = stagedPlan.finalSnapshot
      if stagedPlan.steps.isEmpty then
        connection.rollback()
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          MySqlSchemaAuditor.reject("read-only MySQL staged transaction did not restore auto-commit")
        cleanupComplete = true
        return stagedResult

      val snapshotMutation = MySqlBusinessMutation.ReplaceFullRepositorySnapshot(before, after, stagedPlan.steps)
      effectPlan = MySqlBusinessEffectPlan(Vector(snapshotMutation))
      operationContext = effectPlan.operationContext(admission.bootUuid, admission.writerEpoch, admission.bindingUuid, operationUuid)
      validateAdmission(operationContext, admission)
      MySqlCommitReceipts.verifyReadback(operationContext, writer.receipt) match
        case MySqlCommitReceiptVerification.Conflict(reason) =>
          MySqlSchemaAuditor.reject("new MySQL operation UUID conflicts with the exact writer receipt slot: " + reason.toString)
        case MySqlCommitReceiptVerification.Confirmed(_) =>
          MySqlSchemaAuditor.reject("new MySQL operation UUID unexpectedly matches an existing receipt")
        case MySqlCommitReceiptVerification.Unproven => ()

      stagedPlan.steps.foreach { step =>
        step.delta.apply(verified, scope, expectation)
        val stepReadback = MySqlFullSnapshotJdbc.read(connection, expectation.schema, lockRows = true)
        if !stepReadback.exactlyEquals(step.after) then
          MySqlFencedTransactionSupport.rejectConflict(
            "ordered MySQL staged helper transition differs after its exact guarded writes: " + step.operation
          )
      }
      val inTransactionReadback = MySqlFullSnapshotJdbc.read(connection, expectation.schema, lockRows = true)
      if !inTransactionReadback.exactlyEquals(after) then
        MySqlFencedTransactionSupport.rejectConflict("complete MySQL repository snapshot differs before receipt commit")

      val targetReceipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(operationContext.bootUuid, operationContext.writerEpoch, operationContext.bindingUuid, operationContext.operationUuid),
        operationContext.canonicalFingerprintSha256
      )
      updateWriterReceipt(writer, targetReceipt)
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(operationContext.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(targetReceipt.effectFingerprintSha256) then
        MySqlSchemaAuditor.reject("MySQL writer receipt did not read back the complete full-snapshot operation slot")

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then
        MySqlSchemaAuditor.reject("committed MySQL full repository transaction did not restore auto-commit")
      cleanupComplete = true

      verifyPostCommitWriter(writerAfterUpdate, operationContext, admission)
      val postCommitSnapshot = MySqlFullSnapshotJdbc.read(connection, expectation.schema, lockRows = false)
      if !postCommitSnapshot.exactlyEquals(after) then
        MySqlFencedTransactionSupport.rejectConflict("complete MySQL repository snapshot differs after acknowledged commit")
      stagedResult
    catch
      case primary: Throwable =>
        if staging != null then
          try staging.abort()
          catch case closeFailure: Throwable => MySqlFencedTransactionSupport.addSuppressed(primary, closeFailure)
        if commitAttempted && !commitAcknowledged then
          throw freezeUnknown(operationContext, Option(effectPlan), primary)
        else if commitAcknowledged then
          val plan = Option(effectPlan).getOrElse(throw primary)
          throw freezeCommittedUnverified(operationContext, plan, primary)
        else if transactionAttempted && !cleanupComplete then
          throw rollbackAndRestoreStaged(operationContext, Option(effectPlan), primary)
        else
          throw primary

  private def stagedContext(admission: MySqlBusinessAdmission, operationUuid: UUID): MySqlOperationContext =
    val fingerprint = Vector.fill[Byte](MySqlExpectedEffect.FingerprintLength)(0)
    val effect = MySqlExpectedEffect("repository/staged-admission", 0L, fingerprint)
    MySqlOperationContext(admission.bootUuid, admission.writerEpoch, admission.bindingUuid, operationUuid, Vector(effect))

  private def rollbackAndRestoreStaged(
      context: MySqlOperationContext,
      plan: Option[MySqlBusinessEffectPlan],
      primary: Throwable
  ): Throwable =
    var cleanupFailed = false
    try connection.rollback()
    catch
      case rollbackFailure: Throwable =>
        MySqlFencedTransactionSupport.addSuppressed(primary, rollbackFailure)
        cleanupFailed = true
    if !cleanupFailed then
      try
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          MySqlSchemaAuditor.reject("MySQL staged transaction rollback did not restore auto-commit")
      catch
        case restoreFailure: Throwable =>
          MySqlFencedTransactionSupport.addSuppressed(primary, restoreFailure)
          cleanupFailed = true
    if !cleanupFailed then primary
    else
      val frozen = plan match
        case Some(value) => coordinator.markBusinessCommitUnknown(context, value, primary)
        case None => coordinator.markBusinessCommitUnknown(context, primary)
      if frozen ne primary then frozen
      else primary

  private def executeOwned(
      context: MySqlOperationContext,
      plan: MySqlBusinessEffectPlan,
      body: MySqlBusinessTransaction => Try[Unit],
      rollbackOnly: MySqlRollbackOnlyState
  ): MySqlFencedTransactionOutcome =
    require(context.expectedEffects == plan.expectedEffects, "operation context effects differ from the immutable typed plan")
    val admission = coordinator.requireBusinessAdmission(verified, scope)
    validateAdmission(context, admission)
    if !connection.getAutoCommit then
      MySqlSchemaAuditor.reject("MySQL business transaction requires an idle auto-commit connection")

    var beginAttempted = false
    var cleanupStarted = false
    var commitAttempted = false
    var commitAcknowledged = false
    var transaction: MySqlBusinessTransaction = null

    try
      beginAttempted = true
      connection.setAutoCommit(false)
      verified.readmitSessionBarrier(expectation, scope)

      val manifest = lockManifest()
      validateManifest(manifest, admission)
      val binding = lockBinding()
      validateBinding(binding, admission)
      val writer = lockWriter()
      validateWriter(writer, context, admission)

      MySqlCommitReceipts.verifyReadback(context, writer.receipt) match
        case MySqlCommitReceiptVerification.Confirmed(_) =>
          verifyDesiredReadback(plan)
          cleanupStarted = true
          connection.rollback()
          connection.setAutoCommit(true)
          if !connection.getAutoCommit then
            MySqlSchemaAuditor.reject("MySQL replay readback left auto-commit disabled")
          return MySqlFencedTransactionOutcome.Replayed
        case MySqlCommitReceiptVerification.Conflict(reason) =>
          MySqlSchemaAuditor.reject("MySQL operation UUID conflicts with the exact current writer receipt: " + reason.toString)
        case MySqlCommitReceiptVerification.Unproven => ()

      lockAndVerifyPreimages(plan)
      val lockedReadOwner =
        if expectation.profile == MySqlSchemaAuditProfile.Format3 then
          Some(MySqlFencedTransaction.afterLockedRows(
            MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly),
            verified,
            scope,
            expectation,
            admission,
            context,
            rollbackOnly,
            ownerThread,
            manifest,
            binding,
            writer
          ))
        else None
      transaction = new MySqlBusinessTransaction(
        verified,
        scope,
        expectation,
        quotedSchema,
        plan,
        context,
        ownerThread,
        rollbackOnly,
        lockedReadOwner,
        sharedFormat3AllocationLedger
      )
      val bodyResult =
        try body(transaction)
        catch
          case failure: Throwable => Failure(failure)
      if bodyResult == null then
        val failure = new IllegalStateException("MySQL transaction body returned a null Try")
        transaction.markFailure(failure)
        throw failure
      bodyResult match
        case Failure(failure) =>
          transaction.markFailure(failure)
          throw transaction.rollbackOnly.cause.getOrElse(failure)
        case Success(_) => ()

      transaction.rollbackOnly.cause.foreach(failure => throw failure)
      if !transaction.hasExactlyAppliedPlan then
        MySqlSchemaAuditor.reject("MySQL transaction body did not apply every planned effect exactly once")
      transaction.close()
      transaction = null

      verifyDesiredReadback(plan)
      val targetFingerprint = context.canonicalFingerprintSha256
      updateWriterReceipt(
        writer,
        MySqlCommitReceipt(
          MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
          targetFingerprint
        )
      )
      val writerAfterUpdate = lockWriter()
      validateWriterIdentityAndLease(writerAfterUpdate, admission)
      if writerAfterUpdate.lastOperationUuid != Some(context.operationUuid) ||
        writerAfterUpdate.lastFingerprint != Some(targetFingerprint) then
        MySqlSchemaAuditor.reject("MySQL writer receipt did not read back the complete target slot before commit")

      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      cleanupStarted = true
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then
        MySqlSchemaAuditor.reject("MySQL committed connection did not restore auto-commit")

      verifyPostCommitWriter(writerAfterUpdate, context, admission)
      verifyDesiredReadback(plan)
      MySqlFencedTransactionOutcome.Committed
    catch
      case primary: Throwable =>
        if transaction != null then transaction.close()
        if commitAttempted && !commitAcknowledged then
          throw freezeUnknown(context, plan, primary)
        else if commitAcknowledged then
          throw freezeCommittedUnverified(context, plan, primary)
        else if cleanupStarted then
          throw freezeCleanupUncertain(context, plan, primary)
        else if beginAttempted then
          throw rollbackAndRestore(context, plan, primary)
        else
          throw primary

  private def validateAdmission(context: MySqlOperationContext, admission: MySqlBusinessAdmission): Unit =
    val token = admission.token
    val qualification = token.qualification
    val receipt = admission.receipt
    val identity = verified.identity
    if (token.verified ne verified) || (token.scope ne scope) ||
      context.bootUuid != admission.bootUuid || context.writerEpoch != admission.writerEpoch ||
      context.bindingUuid != admission.bindingUuid ||
      context.bootUuid != scope.bootUuid ||
      identity.serverUuid != expectation.serverUuid || identity.serverVersion != expectation.serverVersion ||
      identity.catalog != expectation.schema || identity.effectiveUser != expectation.effectiveUser ||
      qualification.profile != expectation.profile || qualification.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      qualification.serverUuid != expectation.serverUuid || qualification.schema != expectation.schema ||
      qualification.bindingUuid != expectation.bindingUuid || qualification.provisioningUuid != expectation.provisioningUuid ||
      !qualification.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) ||
      !qualification.objectManifestSha256.equalsIgnoreCase(expectation.objects.canonicalSha256) ||
      qualification.coverage != expectation.profile.coverage ||
      qualification.importState != MySqlFencedTransactionSupport.expectedImportState(expectation.expectedImport) ||
      qualification.importOperationUuid != MySqlFencedTransactionSupport.expectedImportOperation(expectation.expectedImport) ||
      qualification.importManifestSha256 != MySqlFencedTransactionSupport.expectedImportManifest(expectation.expectedImport) ||
      qualification.importSnapshotSha256 != MySqlFencedTransactionSupport.expectedImportSnapshot(expectation.expectedImport) ||
      qualification.importReceiptSha256 != MySqlFencedTransactionSupport.expectedImportReceipt(expectation.expectedImport) ||
      token.fence.bindingUuid != context.bindingUuid || token.fence.ownerBootUuid != context.bootUuid ||
      token.fence.epoch != context.writerEpoch then
      MySqlSchemaAuditor.reject("MySQL business admission differs from the exact operation, server, schema, or approved qualification")

    if !qualification.isCurrent(
      context.bootUuid,
      scope.bootGeneration,
      scope.connectionGeneration,
      expectation.serverUuid,
      expectation.schema,
      expectation.bindingUuid,
      expectation.profile,
      expectation.expectedPhysicalFormat,
      verified,
      scope.ownerThread,
      System.nanoTime()
    ) then
      MySqlSchemaAuditor.reject("MySQL schema qualification is stale for this business transaction")
    receipt.assertCurrent(
      context.bootUuid,
      scope.bootGeneration,
      scope.connectionGeneration,
      expectation.serverUuid,
      expectation.schema,
      context.bindingUuid,
      expectation.profile,
      expectation.expectedPhysicalFormat,
      verified,
      Some(token.fence),
      System.nanoTime()
    )
    if receipt.writerEpoch != context.writerEpoch || receipt.provisioningUuid != expectation.provisioningUuid ||
      !receipt.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) ||
      !receipt.objectManifestSha256.equalsIgnoreCase(expectation.objects.canonicalSha256) ||
      receipt.coverage != expectation.profile.coverage ||
      receipt.importState != MySqlFencedTransactionSupport.expectedImportState(expectation.expectedImport) ||
      receipt.importOperationUuid != MySqlFencedTransactionSupport.expectedImportOperation(expectation.expectedImport) ||
      receipt.importManifestSha256 != MySqlFencedTransactionSupport.expectedImportManifest(expectation.expectedImport) ||
      receipt.importSnapshotSha256 != MySqlFencedTransactionSupport.expectedImportSnapshot(expectation.expectedImport) ||
      receipt.importReceiptSha256 != MySqlFencedTransactionSupport.expectedImportReceipt(expectation.expectedImport) then
      MySqlSchemaAuditor.reject("MySQL post-acquire audit receipt differs from the exact business transaction scope")

  private def lockManifest(): MySqlLockedManifest =
    val sql =
      "SELECT singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state," +
        "expected_server_uuid,expected_binding_uuid,import_manifest_sha256,import_state,import_operation_uuid," +
        "import_snapshot_sha256,import_receipt_sha256,guard_manifest_sha256 FROM " +
        table("fp_schema_manifest") + " WHERE singleton_fixed1=1 FOR SHARE"
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "approved manifest") { row =>
          MySqlLockedManifest(
            singleton = MySqlFencedTransactionSupport.requiredInt(row, 1, "manifest singleton"),
            backend = MySqlFencedTransactionSupport.requiredText(row, 2, "manifest backend"),
            formatVersion = MySqlFencedTransactionSupport.requiredInt(row, 3, "manifest format version"),
            ddlSha256 = MySqlFencedTransactionSupport.requiredBytes(row, 4, "manifest DDL SHA-256", 32).toVector,
            provisioningUuid = MySqlFencedTransactionSupport.requiredUuid(row, 5, "manifest provisioning UUID"),
            bootstrapState = MySqlFencedTransactionSupport.requiredText(row, 6, "manifest bootstrap state"),
            serverUuid = MySqlFencedTransactionSupport.requiredUuid(row, 7, "manifest server UUID"),
            bindingUuid = MySqlFencedTransactionSupport.requiredUuid(row, 8, "manifest binding UUID"),
            importManifestSha256 = MySqlFencedTransactionSupport.optionalHash(row, 9, "manifest import hash"),
            importState = MySqlFencedTransactionSupport.requiredText(row, 10, "manifest import state"),
            importOperationUuid = MySqlFencedTransactionSupport.optionalUuid(row, 11, "manifest import operation UUID"),
            importSnapshotSha256 = MySqlFencedTransactionSupport.optionalHash(row, 12, "manifest import snapshot hash"),
            importReceiptSha256 = MySqlFencedTransactionSupport.optionalHash(row, 13, "manifest import receipt hash"),
            guardManifestSha256 = MySqlFencedTransactionSupport.optionalHash(row, 14, "manifest guard hash")
          )
        }
      }
    }

  private def validateManifest(row: MySqlLockedManifest, admission: MySqlBusinessAdmission): Unit =
    val expectedGuard = HexFormat.of().parseHex(expectation.objects.canonicalSha256).toVector
    val expectedDdl = HexFormat.of().parseHex(expectation.ddlSha256).toVector
    if row.singleton != 1 || row.backend != "MYSQL" || row.formatVersion != expectation.profile.schemaFormat ||
      row.bootstrapState != "READY" || row.ddlSha256 != expectedDdl ||
      row.provisioningUuid != expectation.provisioningUuid || row.serverUuid != expectation.serverUuid ||
      row.bindingUuid != expectation.bindingUuid || row.guardManifestSha256 != Some(expectedGuard) ||
      !MySqlFencedTransactionSupport.importMatches(row, expectation.expectedImport) then
      MySqlSchemaAuditor.reject("MySQL manifest is not the exact approved READY server/schema/import/guard tuple")

    val qualification = admission.token.qualification
    if qualification.profile != expectation.profile || qualification.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      qualification.importState != row.importState ||
      qualification.importOperationUuid != row.importOperationUuid ||
      qualification.importManifestSha256 != row.importManifestSha256.map(MySqlFencedTransactionSupport.hex) ||
      qualification.importSnapshotSha256 != row.importSnapshotSha256.map(MySqlFencedTransactionSupport.hex) ||
      qualification.importReceiptSha256 != row.importReceiptSha256.map(MySqlFencedTransactionSupport.hex) then
      MySqlSchemaAuditor.reject("locked MySQL manifest tuple changed after schema qualification")

  private def lockBinding(): MySqlLockedBinding =
    val sql = "SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM " +
      table("physical_recovery_binding") + " WHERE singleton_fixed1=1 FOR UPDATE"
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "physical binding") { row =>
          MySqlLockedBinding(
            MySqlFencedTransactionSupport.requiredInt(row, 1, "binding singleton"),
            MySqlFencedTransactionSupport.requiredUuid(row, 2, "physical binding UUID"),
            MySqlFencedTransactionSupport.requiredText(row, 3, "physical binding state"),
            MySqlFencedTransactionSupport.requiredInt(row, 4, "physical binding format")
          )
        }
      }
    }

  private def validateBinding(row: MySqlLockedBinding, admission: MySqlBusinessAdmission): Unit =
    val qualifiedMatch = admission.token.qualification.physicalBinding match
      case MySqlCertifiedPhysicalBinding(bindingUuid, formatVersion) =>
        bindingUuid == expectation.bindingUuid && formatVersion == expectation.expectedPhysicalFormat.version
      case _ => false
    if row.singleton != 1 || row.bindingUuid != expectation.bindingUuid || row.cutoverState != "CERTIFIED" ||
      row.formatVersion != expectation.expectedPhysicalFormat.version || !qualifiedMatch then
      MySqlSchemaAuditor.reject("MySQL physical recovery binding is not the exact certified qualified target")

  private def lockWriter(): MySqlBusinessWriterRow = readWriter(lock = true)

  private def readWriter(lock: Boolean): MySqlBusinessWriterRow =
    val lockClause = if lock then " FOR UPDATE" else ""
    val sql =
      "SELECT singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until,last_commit_operation_uuid," +
        "last_commit_fingerprint,UTC_TIMESTAMP(6) FROM " + table("fp_runtime_writer") +
        " WHERE singleton_fixed1=1" + lockClause
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "runtime writer") { row =>
          val singleton = MySqlFencedTransactionSupport.requiredInt(row, 1, "writer singleton")
          if singleton != 1 then MySqlSchemaAuditor.reject("MySQL runtime writer singleton key is not one")
          MySqlBusinessWriterRow(
            bindingUuid = MySqlFencedTransactionSupport.requiredUuid(row, 2, "writer binding UUID"),
            writerEpoch = MySqlFencedTransactionSupport.requiredLong(row, 3, "writer epoch"),
            ownerBootUuid = MySqlFencedTransactionSupport.optionalUuid(row, 4, "writer owner boot UUID"),
            leaseUntil = MySqlFencedTransactionSupport.requiredLocalDateTime(row, 5, "writer lease"),
            lastOperationUuid = MySqlFencedTransactionSupport.optionalUuid(row, 6, "writer operation UUID"),
            lastFingerprint = MySqlFencedTransactionSupport.optionalHash(row, 7, "writer effect fingerprint"),
            databaseNow = MySqlFencedTransactionSupport.requiredLocalDateTime(row, 8, "database UTC time")
          )
        }
      }
    }

  private def validateWriter(row: MySqlBusinessWriterRow, context: MySqlOperationContext, admission: MySqlBusinessAdmission): Unit =
    validateWriterIdentityAndLease(row, admission)
    MySqlCommitReceipts.verifyReadback(context, row.receipt) match
      case MySqlCommitReceiptVerification.Conflict(reason) =>
        MySqlSchemaAuditor.reject("MySQL current writer receipt conflicts with this operation: " + reason.toString)
      case _ => ()

  private def validateWriterIdentityAndLease(row: MySqlBusinessWriterRow, admission: MySqlBusinessAdmission): Unit =
    val token = admission.token
    if row.bindingUuid != expectation.bindingUuid || row.bindingUuid != token.bindingUuid ||
      row.writerEpoch != token.writerEpoch || row.ownerBootUuid != Some(token.bootUuid) ||
      row.leaseUntil != token.leaseUntil then
      MySqlSchemaAuditor.reject("locked MySQL writer row differs from the exact current admission token")
    if !row.leaseUntil.isAfter(row.databaseNow) then
      MySqlSchemaAuditor.reject("MySQL writer lease expired according to database UTC time")

  private def lockAndVerifyPreimages(plan: MySqlBusinessEffectPlan): Unit =
    plan.mutations.foreach { mutation =>
      if mutation.expectedPod.nonEmpty || mutation.desiredPod.nonEmpty then
        val observed = lockPod(MySqlBusinessMutation.podUuid(mutation))
        if !MySqlBusinessMutation.samePodOption(observed, mutation.expectedPod) then
          MySqlFencedTransactionSupport.rejectConflict("inactive MySQL pod preimage differs from the exact expected row")
      else
        val observed = lockEntitlement(MySqlBusinessMutation.entitlementUuid(mutation))
        if observed != mutation.expectedEntitlement then
          MySqlFencedTransactionSupport.rejectConflict("MySQL player entitlement preimage differs from the exact expected row")
    }

  private def lockPod(uuid: UUID): Option[MySqlStoredRowData] =
    val sql = "SELECT " + MySqlFencedTransactionSupport.PodColumns + " FROM " + table("floating_pod_state") +
      " WHERE uuid=? FOR UPDATE"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then None
        else
          val row = MySqlBusinessRows.readPod(result)
          if result.next() then MySqlSchemaAuditor.reject("MySQL pod primary key returned multiple rows")
          Some(row)
      }
    }

  private def lockEntitlement(uuid: UUID): Option[PlayerEntitlement] =
    val sql = "SELECT player_uuid,registration_limit,simultaneous_active_limit FROM " + table("player_entitlement") +
      " WHERE player_uuid=? FOR SHARE"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then None
        else
          val row = MySqlBusinessRows.readEntitlement(result)
          if result.next() then MySqlSchemaAuditor.reject("MySQL player entitlement primary key returned multiple rows")
          Some(row)
      }
    }

  private def verifyDesiredReadback(plan: MySqlBusinessEffectPlan): Unit =
    plan.mutations.foreach { mutation =>
      if mutation.expectedPod.nonEmpty || mutation.desiredPod.nonEmpty then
        val observed = businessRows.findPod(MySqlBusinessMutation.podUuid(mutation))
        if !MySqlBusinessMutation.samePodOption(observed, mutation.desiredPod) then
          MySqlFencedTransactionSupport.rejectConflict("complete MySQL inactive pod effect did not exactly read back")
      else
        val observed = businessRows.findEntitlement(MySqlBusinessMutation.entitlementUuid(mutation))
        if observed != mutation.desiredEntitlement then
          MySqlFencedTransactionSupport.rejectConflict("complete MySQL entitlement effect did not exactly read back")
    }

  private def updateWriterReceipt(before: MySqlBusinessWriterRow, receipt: MySqlCommitReceipt): Unit =
    updateWriterReceipt(before, receipt, None)

  /** The restricted SOURCE wrapper supplies its already-precharged DATA child for exact bind copies. */
  private def updateWriterReceipt(before: MySqlBusinessWriterRow, receipt: MySqlCommitReceipt,
      restrictedSourceBudget: Option[MySqlFormat3ReadBudget]): Unit =
    val charge = 1024L
    restrictedSourceBudget.foreach(_.reserveTemporaryMemory(charge,
      "restricted SOURCE R0 W CAS bind/statement workspace BEFORE allocation"))
    val arrays = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var primary: Throwable = null
    try
      def copy(value: Array[Byte]): Array[Byte] =
        arrays += value
        value
      def bindOptionalUuid(statement: PreparedStatement, index: Int, value: Option[UUID]): Unit =
        value match
          case None => statement.setNull(index, Types.VARBINARY)
          case Some(uuid) => statement.setBytes(index, copy(MySqlStoredRows.encodeUuid(uuid).toArray))
      def bindOptionalBytes(statement: PreparedStatement, index: Int, value: Option[Vector[Byte]]): Unit =
        value match
          case None => statement.setNull(index, Types.VARBINARY)
          case Some(bytes) => statement.setBytes(index, copy(bytes.toArray))
      val sql =
        "UPDATE " + table("fp_runtime_writer") +
          " SET last_commit_operation_uuid=?,last_commit_fingerprint=? WHERE singleton_fixed1=1 AND binding_uuid=? " +
          "AND writer_epoch=? AND owner_boot_uuid=? AND lease_until=? " +
          "AND last_commit_operation_uuid <=> ? AND last_commit_fingerprint <=> ?"
      withStatement(sql) { statement =>
        statement.setBytes(1, copy(MySqlStoredRows.encodeUuid(receipt.slot.operationUuid).toArray))
        statement.setBytes(2, copy(receipt.effectFingerprintSha256.toArray))
        statement.setBytes(3, copy(MySqlStoredRows.encodeUuid(before.bindingUuid).toArray))
        statement.setLong(4, before.writerEpoch)
        statement.setBytes(5, copy(MySqlStoredRows.encodeUuid(before.ownerBootUuid.get).toArray))
        statement.setObject(6, before.leaseUntil)
        bindOptionalUuid(statement, 7, before.lastOperationUuid)
        bindOptionalBytes(statement, 8, before.lastFingerprint)
        val changed = statement.executeUpdate()
        if changed != 1 then MySqlSchemaAuditor.reject("MySQL writer receipt CAS did not update exactly one locked row")
      }
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      var cleanupFailure: Throwable = null
      arrays.foreach { bytes =>
        try java.util.Arrays.fill(bytes, 0.toByte)
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure = cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure, cleanup)
      }
      arrays.clear()
      restrictedSourceBudget.foreach { budget =>
        try budget.releaseTemporaryMemory(charge,
          "restricted SOURCE R0 W CAS bind/statement workspace complete")
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure = cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure, cleanup)
      }
      if cleanupFailure != null then
        if primary == null then throw cleanupFailure
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanupFailure)

  /** Narrow R0 bridge: only the original active Session scope may invoke the existing W CAS. */
  private[mysql] def executeRestrictedSourceR0Dml(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      freshScope: MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
      expectedTarget: MySqlScriptSchemaSnapshot,
      actualBefore: MySqlScriptSchemaSnapshot,
      beforeWriter: MySqlBusinessWriterRow
  ): Unit =
    require(plan != null && freshScope != null && witness != null && expectedTarget != null &&
      actualBefore != null && beforeWriter != null && (plan.issuer eq this),
      "exact retained SOURCE R0 plan, active Session scope, A witness, B target and locked B/W required")
    val session = plan.parent.session
    session.requireRestrictedSourceFreshWriteExecutionScope(plan, freshScope, this)
    if !plan.exactBefore.exactlyEquals(actualBefore) ||
        (expectedTarget.inputs ne plan.exactBefore.inputs) ||
        (expectedTarget.allocationBudget ne witness.dataBudget) ||
        (witness.root ne plan.exactRoot) then
      throw new SQLDataException("restricted SOURCE R0 active before/target/witness differs from the exact retained plan child")
    freshScope.requireLockedBefore(actualBefore, beforeWriter)
    freshScope.requireCurrentFence()
    val root = plan.exactRoot
    val ledger = plan.actualLedger
    root.requireLedgerIdentity(ledger)
    plan.allocation.withActiveRestrictedSourceR0DataBudget(plan, freshScope) { dataBudget =>
      if (dataBudget ne witness.dataBudget) || dataBudget.isClosed ||
          !dataBudget.isCanonicalChildOf(root) || !dataBudget.sharesAllocationLedger(root) then
        throw new SQLNonTransientConnectionException("exact live SOURCE R0 original-plan DATA child required for DML")
      val context = witness.originalContext.context
      if context == null || context.operationUuid != plan.exactRequest.reserveOperation ||
          context.bindingUuid != plan.exactRequest.binding || context.writerEpoch != beforeWriter.writerEpoch ||
          freshScope.expectedFixedLeaseUntil!=beforeWriter.leaseUntil ||
          context.bootUuid != freshScope.expectedContextTagBootId || context.canonicalFingerprintSha256 == null ||
          context.canonicalFingerprintSha256.size != 32 then
        throw new SQLDataException("restricted SOURCE R0 actual context does not bind exact original W and request")
      val inputs = actualBefore.inputs
      val operationShape = inputs.forTable("fp_recovery_operation")
      val reservationShape = inputs.forTable("fp_recovery_reservation")
      val operationRow = restrictedSourceR0OperationRow(plan, expectedTarget, operationShape)
      freshScope.markDmlAttempted()
      executeRestrictedSourceR0Insert(operationShape, operationRow, plan, dataBudget,
        "SOURCE R0 exact fp_recovery_operation INSERT")
      plan.exactRequest.resources.foreach { entry =>
        if entry == null || entry.resource == null || entry.before == null then
          throw new SQLDataException("restricted SOURCE R0 exact request resource expectation is incomplete")
        val targetRow = restrictedSourceR0ReservationRow(plan, expectedTarget, reservationShape, entry, dataBudget)
        entry.before match
          case None =>
            if actualBefore.relation("fp_recovery_reservation").rows.exists(row =>
                restrictedSourceR0SameReservationKey(row, targetRow, reservationShape, dataBudget)) then
              throw new SQLDataException("restricted SOURCE R0 absent reservation appeared in exact locked B")
            executeRestrictedSourceR0Insert(reservationShape, targetRow, plan, dataBudget,
              "SOURCE R0 exact absent fp_recovery_reservation INSERT")
          case Some(beforeRow) =>
            if beforeRow.size != reservationShape.columns.size ||
                !actualBefore.relation("fp_recovery_reservation").rows.exists(_ == beforeRow) then
              throw new SQLDataException("restricted SOURCE R0 released reservation preimage differs from exact locked B")
            executeRestrictedSourceR0Update(reservationShape, targetRow, beforeRow, plan, dataBudget,
              "SOURCE R0 exact RELEASED-to-HELD fp_recovery_reservation full-before CAS")
      }
      val receipt = MySqlCommitReceipt(
        MySqlCommitReceiptSlot(context.bootUuid, context.writerEpoch, context.bindingUuid, context.operationUuid),
        context.canonicalFingerprintSha256)
      updateRestrictedSourceR0WriterReceipt(plan, freshScope, beforeWriter, receipt, context, dataBudget)
      freshScope.markDmlReturned()
    }

  /** Actual same-connection H1 database time sample for the original accepted R0 route. */
  private[mysql] def sampleRestrictedSourceR0DatabaseTime(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      purpose: String
  ): LocalDateTime =
    if plan == null || purpose == null || (plan.issuer ne this) then
      throw new IllegalArgumentException("exact original retained SOURCE R0 plan and database-time purpose required")
    plan.requireRetained()
    val root = plan.exactRoot
    val ledger = plan.actualLedger
    val parent = plan.parent
    val intent = plan.intent
    val data = plan.data
    root.requireLedgerIdentity(ledger)
    plan.allocation.withRetainedPlanDataBudget(parent, root, ledger, intent, data) { dataBudget =>
      val sql = "SELECT UTC_TIMESTAMP(6)"
      val packetBound = Math.min(plan.exactBefore.maxAllowedPacketBytes, root.profile.maxSqlPacketBytes())
      val packetBytes = Math.addExact(MySqlFormat3Utf8.byteLength(sql), 16L)
      if packetBound <= 0L || packetBytes > packetBound then
        throw new SQLDataException("restricted SOURCE R0 database-time query exceeds actual packet/profile bound")
      dataBudget.reserveTemporaryMemory(1024L, purpose + " statement/result workspace BEFORE JDBC")
      var primary: Throwable = null
      try
        MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
          MySqlBusinessRows.using(statement.executeQuery()) { result =>
            if !result.next() then throw new SQLDataException("restricted SOURCE R0 database time is missing")
            val value = result.getObject(1, classOf[LocalDateTime])
            if value == null || value.getYear < 1000 || value.getYear > 9999 || value.getNano % 1000 != 0 || result.next() then
              throw new SQLDataException("restricted SOURCE R0 database time is not one valid DATETIME(6) row")
            value
          }
        }
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try dataBudget.releaseTemporaryMemory(1024L, purpose + " statement/result workspace complete")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)
    }

  /** Commit is the only uncertainty boundary; its failure is retained with the exact original context. */
  private[mysql] def commitRestrictedSourceR0(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      freshScope: MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      context: MySqlOperationContext
  ): Unit =
    if plan == null || freshScope == null || context == null || (plan.issuer ne this) then
      throw new IllegalArgumentException("exact restricted SOURCE R0 commit owner and context required")
    plan.parent.session.requireRestrictedSourceFreshWriteExecutionScope(plan, freshScope, this)
    freshScope.requireVerifiedTarget()
    freshScope.rollbackOnly.cause.foreach(throw _)
    val sql = "SELECT UTC_TIMESTAMP(6)"
    val packetBound = Math.min(plan.exactBefore.maxAllowedPacketBytes, plan.exactRoot.profile.maxSqlPacketBytes())
    val packetBytes = Math.addExact(MySqlFormat3Utf8.byteLength(sql), 16L)
    if packetBound <= 0L || packetBytes > packetBound then
      throw new SQLDataException("restricted SOURCE R0 final database-time query exceeds actual packet/profile bound")
    val dataBudget = freshScope.retainedDataBudget
    if dataBudget == null || dataBudget.isClosed then
      throw new SQLNonTransientConnectionException("restricted SOURCE R0 final fence lacks the original live DATA child")
    dataBudget.reserveTemporaryMemory(1024L, "restricted SOURCE R0 final database-time statement/result workspace BEFORE JDBC")
    var primary: Throwable = null
    try
      val now = MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { result =>
          if !result.next() then throw new SQLDataException("restricted SOURCE R0 final database time is missing")
          val value = result.getObject(1, classOf[LocalDateTime])
          if value == null || value.getYear < 1000 || value.getYear > 9999 || value.getNano % 1000 != 0 || result.next() then
            throw new SQLDataException("restricted SOURCE R0 final database time is not one valid DATETIME(6) row")
          value
        }
      }
      if !freshScope.expectedFixedLeaseUntil.isAfter(now) then
        throw new SQLNonTransientConnectionException("restricted SOURCE R0 writer lease expired at final database fence")
      freshScope.rollbackOnly.cause.foreach(throw _)
      freshScope.markCommitAttempted()
      try
        connection.commit()
        freshScope.markCommitAcknowledged()
      catch
        case cause: Throwable =>
          val unknown = MySqlCommitUnknownException(context, cause)
          freshScope.markCommitUnknown(unknown)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(1024L, "restricted SOURCE R0 final database-time statement/result workspace complete")
      catch
        case cleanup: Throwable =>
          if primary == null then
            if freshScope.commitWasAcknowledged then freshScope.markCommittedCleanupFailure(cleanup)
            else if freshScope.commitWasAttempted then freshScope.markCommitUnknownCleanupFailure(cleanup)
            else throw cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)

  private def updateRestrictedSourceR0WriterReceipt(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      freshScope: MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      before: MySqlBusinessWriterRow,
      receipt: MySqlCommitReceipt,
      context: MySqlOperationContext,
      dataBudget: MySqlFormat3ReadBudget
  ): Unit =
    plan.parent.session.requireRestrictedSourceFreshWriteExecutionScope(plan, freshScope, this)
    freshScope.requireCurrentFence()
    if before == null || receipt == null || dataBudget == null || dataBudget.isClosed ||
        context == null || !receipt.slot.exactlyMatches(context) ||
        (receipt.effectFingerprintSha256 != context.canonicalFingerprintSha256) then
      throw new SQLDataException("restricted SOURCE R0 narrow W-receipt bridge lost exact plan/context/DATA ownership")
    updateWriterReceipt(before, receipt, Some(dataBudget))

  private def restrictedSourceR0OperationRow(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      expectedTarget: MySqlScriptSchemaSnapshot,
      shape: MySqlFormat3RelationShape
  ): Vector[MySqlFormat3Cell] =
    if shape == null || shape.table != "fp_recovery_operation" || expectedTarget == null then
      throw new SQLDataException("restricted SOURCE R0 expected operation target shape is unavailable")
    val request = plan.exactRequest
    var selected: Vector[MySqlFormat3Cell] = null
    var matches = 0
    expectedTarget.relation(shape.table).rows.foreach { row =>
      if row.size == shape.columns.size &&
          MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row, shape.table, "binding_uuid"), request.binding) &&
          MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row, shape.table, "operation_uuid"), request.reserveOperation) then
        matches = Math.addExact(matches, 1)
        selected = row
    }
    if matches != 1 then
      throw new SQLDataException("restricted SOURCE R0 B target must contain one exact new O(binding,R0) row")
    if plan.exactBefore.relation(shape.table).rows.exists(row =>
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row, shape.table, "binding_uuid"), request.binding) &&
        MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row, shape.table, "operation_uuid"), request.reserveOperation)) then
      throw new SQLDataException("restricted SOURCE R0 exact O(binding,R0) already exists in B")
    selected

  private def restrictedSourceR0ReservationRow(
      plan: MySqlFencedTransaction.SourceReservePlan5,
      expectedTarget: MySqlScriptSchemaSnapshot,
      shape: MySqlFormat3RelationShape,
      entry: MySqlNormalRecallCanonical.SourceChildReserveResourceExpectation,
      dataBudget: MySqlFormat3ReadBudget
  ): Vector[MySqlFormat3Cell] =
    if shape == null || shape.table != "fp_recovery_reservation" || expectedTarget == null || entry == null then
      throw new SQLDataException("restricted SOURCE R0 exact request resource target shape is unavailable")
    val resource = entry.resource
    val keyBytes = resource.copyCanonicalBytes(dataBudget, "restricted SOURCE R0 exact full reservation key match")
    var primary: Throwable = null
    try
      var selected: Vector[MySqlFormat3Cell] = null
      var matchCount = 0
      expectedTarget.relation(shape.table).rows.foreach { row =>
        if row.size == shape.columns.size then
          val kind = MySqlModeOperationEffectCodec.value(row, shape.table, "resource_kind")
          val hash = MySqlModeOperationEffectCodec.value(row, shape.table, "resource_key_hash")
          val key = MySqlModeOperationEffectCodec.value(row, shape.table, "resource_key")
          val pod = MySqlModeOperationEffectCodec.value(row, shape.table, "resource_pod_uuid")
          val unit = MySqlModeOperationEffectCodec.value(row, shape.table, "resource_unit_uuid")
          val matches = MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(row, shape.table, "binding_uuid"), plan.exactRequest.binding) &&
            kind == MySqlFormat3Cell.Text(resource.resourceKind) &&
            (hash match
              case MySqlFormat3Cell.Binary(value) => value.equalsVector(resource.keyHashSha256)
              case _ => false) &&
            (key match
              case MySqlFormat3Cell.Binary(value) => value.equalsBytes(keyBytes)
              case _ => false) &&
            (resource.podUuid match
              case Some(uuid) => MySqlModeOperationEffectCodec.uuidEquals(pod, uuid)
              case None => pod == MySqlFormat3Cell.SqlNull) &&
            (resource.sourceUnitUuid match
              case Some(uuid) => MySqlModeOperationEffectCodec.uuidEquals(unit, uuid)
              case None => unit == MySqlFormat3Cell.SqlNull)
          if matches then
            matchCount = Math.addExact(matchCount, 1)
            selected = row
      }
      if matchCount != 1 then
        throw new SQLDataException("restricted SOURCE R0 B target must contain one exact full-key resource row per request expectation")
      selected
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      java.util.Arrays.fill(keyBytes, 0.toByte)
      try dataBudget.releaseRetainedMemory(Math.addExact(resource.canonicalLength.toLong, 64L),
        "restricted SOURCE R0 exact full reservation key match")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)

  private def restrictedSourceR0SameReservationKey(
      left: Vector[MySqlFormat3Cell],
      right: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape,
      dataBudget:MySqlFormat3ReadBudget
  ): Boolean =
    if shape==null || left==null || right==null || dataBudget==null ||
        left.size!=shape.columns.size || right.size!=shape.columns.size then
      throw new SQLDataException("restricted SOURCE R0 reservation key comparison requires exact frozen-shape rows")
    dataBudget.reserveTemporaryMemory(512L,"restricted SOURCE R0 exact reservation primary-key comparison workspace BEFORE allocation")
    var primary:Throwable=null
    try
      val binding=shape.columns.indexWhere(_.name=="binding_uuid")
      val kind=shape.columns.indexWhere(_.name=="resource_kind")
      val hash=shape.columns.indexWhere(_.name=="resource_key_hash")
      val key=shape.columns.indexWhere(_.name=="resource_key")
      val pod=shape.columns.indexWhere(_.name=="resource_pod_uuid")
      val unit=shape.columns.indexWhere(_.name=="resource_unit_uuid")
      if Vector(binding,kind,hash,key,pod,unit).exists(_<0) then
        throw new SQLDataException("restricted SOURCE R0 reservation frozen primary-key columns are incomplete")
      left(binding)==right(binding) && left(kind)==right(kind) && left(hash)==right(hash) &&
        left(key)==right(key) && left(pod)==right(pod) && left(unit)==right(unit)
    catch
      case cause:Throwable =>
        primary=cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(512L,"restricted SOURCE R0 exact reservation primary-key comparison workspace complete")
      catch case cleanup:Throwable=>
        if primary==null then throw cleanup
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)

  private def executeRestrictedSourceR0Insert(
      shape: MySqlFormat3RelationShape,
      row: Vector[MySqlFormat3Cell],
      plan: MySqlFencedTransaction.SourceReservePlan5,
      dataBudget: MySqlFormat3ReadBudget,
      purpose: String
  ): Unit =
    if shape == null || row == null || row.size != shape.columns.size || purpose == null then
      throw new SQLDataException("restricted SOURCE R0 INSERT requires one exact frozen-shape row")
    val sqlCharge = Math.addExact(1024L, Math.multiplyExact(shape.columns.size.toLong, 96L))
    dataBudget.reserveTemporaryMemory(sqlCharge, purpose + " fixed SQL text workspace BEFORE allocation")
    var primary: Throwable = null
    try
      val columnNames = shape.columns.map(column => restrictedSourceR0QuoteColumn(column.name))
      val sql = "INSERT INTO " + table(shape.table) + " (" + columnNames.mkString(",") + ") VALUES (" +
        shape.columns.map(_ => "?").mkString(",") + ")"
      executeRestrictedSourceR0RowStatement(sql, shape, row, Vector.empty, plan, dataBudget, purpose)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(sqlCharge, purpose + " fixed SQL text workspace complete")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)

  private def executeRestrictedSourceR0Update(
      shape: MySqlFormat3RelationShape,
      target: Vector[MySqlFormat3Cell],
      before: Vector[MySqlFormat3Cell],
      plan: MySqlFencedTransaction.SourceReservePlan5,
      dataBudget: MySqlFormat3ReadBudget,
      purpose: String
  ): Unit =
    if shape == null || target == null || before == null || target.size != shape.columns.size ||
        before.size != shape.columns.size || purpose == null then
      throw new SQLDataException("restricted SOURCE R0 UPDATE requires exact same-shape B/target rows")
    val sqlCharge = Math.addExact(1024L, Math.multiplyExact(shape.columns.size.toLong, 96L))
    dataBudget.reserveTemporaryMemory(sqlCharge, purpose + " fixed SQL text workspace BEFORE allocation")
    var primary: Throwable = null
    try
      val assignments = shape.columns.map(column => restrictedSourceR0QuoteColumn(column.name) + "=?").mkString(",")
      val predicates = shape.columns.map(column => restrictedSourceR0QuoteColumn(column.name) + " <=> ?").mkString(" AND ")
      val sql = "UPDATE " + table(shape.table) + " SET " + assignments + " WHERE " + predicates
      executeRestrictedSourceR0RowStatement(sql, shape, target, before, plan, dataBudget, purpose)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(sqlCharge, purpose + " fixed SQL text workspace complete")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)

  private def executeRestrictedSourceR0RowStatement(
      sql: String,
      shape: MySqlFormat3RelationShape,
      setRow: Vector[MySqlFormat3Cell],
      beforeRow: Vector[MySqlFormat3Cell],
      plan: MySqlFencedTransaction.SourceReservePlan5,
      dataBudget: MySqlFormat3ReadBudget,
      purpose: String
  ): Unit =
    val valueCount = Math.addExact(setRow.size, beforeRow.size)
    val cellCopyBound=(setRow.iterator ++ beforeRow.iterator).foldLeft(0L) { (sum,cell) =>
      val cellBound=cell match
        case MySqlFormat3Cell.Text(value)=>Math.addExact(Math.multiplyExact(value.length.toLong,4L),64L)
        case MySqlFormat3Cell.Binary(value)=>Math.addExact(value.length.toLong,64L)
        case _=>48L
      Math.addExact(sum,cellBound)
    }
    val scratch = Math.addExact(2048L,Math.addExact(Math.multiplyExact(valueCount.toLong,48L),cellCopyBound))
    dataBudget.reserveTemporaryMemory(scratch, purpose + " statement/binding workspace BEFORE allocation")
    val arrays = scala.collection.mutable.ArrayBuffer.empty[(Array[Byte], Long)]
    var primary: Throwable = null
    try
      var packetBytes = MySqlFormat3Utf8.byteLength(sql)
      (setRow.iterator ++ beforeRow.iterator).foreach(cell =>
        packetBytes = Math.addExact(packetBytes, restrictedSourceR0PacketCellBytes(cell)))
      val maximum = Math.min(plan.exactBefore.maxAllowedPacketBytes, plan.exactRoot.profile.maxSqlPacketBytes())
      if maximum <= 0L || packetBytes > maximum then
        throw new SQLDataException(purpose + " exceeds observed/profile max_allowed_packet")
      withStatement(sql) { statement =>
        var parameter = 1
        def bind(row: Vector[MySqlFormat3Cell]): Unit =
          var index = 0
          while index < row.size do
            val cell = row(index)
            val column = shape.columns(index)
            cell match
              case MySqlFormat3Cell.SqlNull => statement.setNull(parameter, column.jdbcType)
              case MySqlFormat3Cell.Text(value) => statement.setString(parameter, value)
              case MySqlFormat3Cell.Binary(value) =>
                val bytes = value.copyBytes(dataBudget, purpose + " SQL binary bind")
                val charge = Math.addExact(bytes.length.toLong, 64L)
                arrays += ((bytes, charge))
                statement.setBytes(parameter, bytes)
              case MySqlFormat3Cell.SignedInteger(value) =>
                if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                  if value < Int.MinValue.toLong || value > Int.MaxValue.toLong then
                    throw new SQLDataException(purpose + " INT binding overflow")
                  statement.setInt(parameter, value.toInt)
                else statement.setLong(parameter, value)
              case MySqlFormat3Cell.Floating(bits) => statement.setDouble(parameter, java.lang.Double.longBitsToDouble(bits))
              case MySqlFormat3Cell.DateTime(value) => statement.setObject(parameter, value)
            parameter += 1
            index += 1
        bind(setRow)
        bind(beforeRow)
        val changed = statement.executeUpdate()
        if changed != 1 then
          throw new SQLDataException(purpose + " did not affect exactly one row")
      }
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      var cleanupFailure: Throwable = null
      arrays.foreach { (bytes, charge) =>
        try java.util.Arrays.fill(bytes, 0.toByte)
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure = cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure, cleanup)
        try dataBudget.releaseRetainedMemory(charge, purpose + " SQL binary bind complete")
        catch case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure = cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure, cleanup)
      }
      arrays.clear()
      try dataBudget.releaseTemporaryMemory(scratch, purpose + " statement/binding workspace complete")
      catch case cleanup: Throwable =>
        if cleanupFailure == null then cleanupFailure = cleanup
        else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure, cleanup)
      if cleanupFailure != null then
        if primary == null then throw cleanupFailure
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanupFailure)

  private def restrictedSourceR0PacketCellBytes(cell: MySqlFormat3Cell): Long = cell match
    case MySqlFormat3Cell.SqlNull => 1L
    case MySqlFormat3Cell.Text(value) => Math.addExact(MySqlFormat3Utf8.byteLength(value), 16L)
    case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong, 16L)
    case _ => 32L

  private def restrictedSourceR0QuoteColumn(name: String): String =
    if name == null || !name.matches("[a-z][a-z0-9_]{0,63}") then
      throw new SQLDataException("restricted SOURCE R0 frozen Full26 column name is unsafe")
    MySqlFencedTransactionSupport.quote(name)

  private def verifyPostCommitWriter(
      committedWriter: MySqlBusinessWriterRow,
      context: MySqlOperationContext,
      admission: MySqlBusinessAdmission
  ): Unit =
    val actual = readWriter(lock = false)
    validateWriterIdentityAndLease(actual, admission)
    if actual.lastOperationUuid != Some(context.operationUuid) ||
      actual.lastFingerprint != Some(context.canonicalFingerprintSha256) ||
      actual.bindingUuid != committedWriter.bindingUuid || actual.writerEpoch != committedWriter.writerEpoch ||
      actual.ownerBootUuid != committedWriter.ownerBootUuid || actual.leaseUntil != committedWriter.leaseUntil then
      MySqlSchemaAuditor.reject("committed MySQL writer tuple differs from the exact operation receipt")

  private def table(name: String): String =
    if !name.matches("[a-z][a-z0-9_]{0,63}") then MySqlSchemaAuditor.reject("internal MySQL table name is unsafe")
    quotedSchema + "." + MySqlFencedTransactionSupport.quote(name)

  private def withStatement[A](sql: String)(body: PreparedStatement => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql))(body)

  private def withResultSet[A](result: ResultSet)(body: ResultSet => A): A =
    MySqlBusinessRows.using(result)(body)

  private def rollbackAndRestore(context: MySqlOperationContext, plan: MySqlBusinessEffectPlan, primary: Throwable): Throwable =
    var cleanupFailed = false
    try connection.rollback()
    catch
      case rollbackFailure: Throwable =>
        MySqlFencedTransactionSupport.addSuppressed(primary, rollbackFailure)
        cleanupFailed = true
    if !cleanupFailed then
      try connection.setAutoCommit(true)
      catch
        case restoreFailure: Throwable =>
          MySqlFencedTransactionSupport.addSuppressed(primary, restoreFailure)
          cleanupFailed = true
    if cleanupFailed then freezeCleanupUncertain(context, plan, primary)
    else primary

  private def freezeUnknown(context: MySqlOperationContext, plan: MySqlBusinessEffectPlan, primary: Throwable): Throwable =
    freezeUnknown(context, Some(plan), primary)

  private def freezeUnknown(
      context: MySqlOperationContext,
      plan: Option[MySqlBusinessEffectPlan],
      primary: Throwable
  ): Throwable =
    try
      plan match
        case Some(value) => coordinator.markBusinessCommitUnknown(context, value, primary)
        case None => coordinator.markBusinessCommitUnknown(context, primary)
    catch
      case freezeFailure: Throwable =>
        MySqlFencedTransactionSupport.addSuppressed(primary, freezeFailure)
        primary

  private def freezeCleanupUncertain(context: MySqlOperationContext, plan: MySqlBusinessEffectPlan, primary: Throwable): Throwable =
    val frozen = freezeUnknown(context, plan, primary)
    if frozen eq primary then
      val report = new MySqlTransactionCleanupUncertainException(context, primary)
      MySqlFencedTransactionSupport.addSuppressed(report, primary)
      report
    else frozen

  private def freezeCommittedUnverified(context: MySqlOperationContext, plan: MySqlBusinessEffectPlan, primary: Throwable): Throwable =
    val frozen = freezeUnknown(context, plan, primary)
    val report = new MySqlCommittedButUnverifiedException(context, primary)
    if frozen ne report then MySqlFencedTransactionSupport.addSuppressed(report, frozen)
    report

/** 事务期间仅可执行已登记计划中的 typed mutation。 */
final class MySqlBusinessTransaction private[mysql] (
    verified: VerifiedMySqlConnection,
    scope: MySqlAuditBootScope,
    expectation: MySqlSchemaAuditExpectation,
    quotedSchema: String,
    plan: MySqlBusinessEffectPlan,
    operationContext: MySqlOperationContext,
    ownerThread: Thread,
    rollbackOnlyState: MySqlRollbackOnlyState,
    lockedReadOwner: Option[MySqlFencedTransaction.LockedReadOwner],
    ownedAllocationLedger: Option[MySqlFormat3AllocationLedger]
):
  require(verified != null && scope != null, "exact owned transaction session required")
  private val connection = verified.connection
  require(operationContext != null && lockedReadOwner != null, "fenced transaction identity and locked reader owner option are required")
  require(rollbackOnlyState != null, "shared transaction rollback-only state is required")
  require(ownedAllocationLedger != null && !ownedAllocationLedger.exists(_ == null), "actual fenced issuer allocation ledger option required")
  private val invoked = scala.collection.mutable.HashSet.empty[String]
  private var open = true

  private[mysql] def rollbackOnly: MySqlRollbackOnlyState = rollbackOnlyState

  private[mysql] def readFormat3FullSnapshot(): Try[MySqlFormat3FullSnapshot] =
    rollbackOnlyState.cause match
      case Some(failure) => Failure(failure)
      case None =>
        try
          if expectation.profile != MySqlSchemaAuditProfile.Format3 then
            throw new IllegalStateException("full format3 snapshot reads require a format3 fenced transaction")
          val profile = expectation.format3Inputs
            .map(_.limitProfile)
            .getOrElse(throw new IllegalStateException("format3 transaction has no exact source limit profile"))
          val owner = lockedReadOwner
            .getOrElse(throw new IllegalStateException("format3 fenced transaction did not retain its locked reader owner"))
          if ownedAllocationLedger.isEmpty then
            throw new IllegalStateException("format3 transaction requires its actual fenced issuer shared allocation ledger")
          val reader = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, profile, ownedAllocationLedger)
          Success(reader.readWithinFencedTransaction(owner, this))
        catch
          case failure: Throwable =>
            rollbackOnlyState.mark(failure)
            Failure(failure)

  private[mysql] def requireFormat3ReaderAccess(
      owner: MySqlFencedTransaction.LockedReadOwner,
      readerVerified: VerifiedMySqlConnection,
      readerScope: MySqlAuditBootScope,
      readerSchema: String,
      readerProfile: SourceLimitProfile
  ): Unit =
    if !lockedReadOwner.exists(_ eq owner) || (readerVerified ne verified) || (readerScope ne scope) ||
      readerSchema != expectation.schema || expectation.profile != MySqlSchemaAuditProfile.Format3 then
      throw new IllegalStateException("format3 snapshot reader does not belong to this exact transaction facade")
    val configuredProfile = expectation.format3Inputs
      .map(_.limitProfile)
      .getOrElse(throw new IllegalStateException("format3 transaction has no exact source limit profile"))
    if readerProfile ne configuredProfile then
      throw new IllegalStateException("format3 snapshot reader profile differs from the approved transaction profile")
    owner.requireCurrent(verified, scope, expectation, operationContext, rollbackOnlyState, ownerThread)
    if connection.isClosed then throw new SQLNonTransientConnectionException("format3 fenced snapshot connection is already closed")
    ensureOpen()

  def insertInactivePod(desired: MySqlStoredRowData): Try[Unit] =
    buildAndApply(MySqlBusinessMutation.InsertInactivePod(desired))

  def replaceInactivePod(expected: MySqlStoredRowData, desired: MySqlStoredRowData): Try[Unit] =
    buildAndApply(MySqlBusinessMutation.ReplaceInactivePod(expected, desired))

  def deleteInactivePod(expected: MySqlStoredRowData): Try[Unit] =
    buildAndApply(MySqlBusinessMutation.DeleteInactivePod(expected))

  def changeEntitlement(expected: Option[PlayerEntitlement], desired: Option[PlayerEntitlement]): Try[Unit] =
    buildAndApply(MySqlBusinessMutation.ChangeEntitlement(expected, desired))

  private def buildAndApply(mutation: => MySqlBusinessMutation): Try[Unit] =
    rollbackOnlyState.cause match
      case Some(failure) => Failure(failure)
      case None =>
        try applyMutation(mutation)
        catch
          case failure: Throwable =>
            rollbackOnlyState.mark(failure)
            Failure(failure)

  private def applyMutation(mutation: MySqlBusinessMutation): Try[Unit] =
    rollbackOnlyState.cause match
      case Some(failure) => Failure(failure)
      case None =>
        try
          ensureOpen()
          if !plan.mutations.contains(mutation) then
            throw new IllegalArgumentException("MySQL mutation is not part of the immutable operation plan")
          if invoked.contains(mutation.targetIdentity) then
            throw new IllegalStateException("a typed MySQL effect may be applied only once per target")
          executeMutation(mutation)
          invoked += mutation.targetIdentity
          Success(())
        catch
          case failure: Throwable =>
            rollbackOnlyState.mark(failure)
            Failure(failure)

  private def executeMutation(mutation: MySqlBusinessMutation): Unit =
    mutation match
      case value: MySqlBusinessMutation.InsertInactivePod =>
        val sql =
          "INSERT INTO " + table("floating_pod_state") +
            " (uuid,owner_uuid,state_version,world_name,pos_x,pos_y,pos_z,yaw,pitch,is_active,last_updated,dimension_id,operating_mode)" +
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        withStatement(sql) { statement =>
          MySqlBusinessTransaction.bindPodInsert(statement, value.desired)
          requireExactlyOne(statement.executeUpdate(), "inactive pod insert")
        }
      case value: MySqlBusinessMutation.ReplaceInactivePod =>
        val sql =
          "UPDATE " + table("floating_pod_state") +
            " SET owner_uuid=?,state_version=?,world_name=?,pos_x=?,pos_y=?,pos_z=?,yaw=?,pitch=?,is_active=?," +
            "last_updated=?,dimension_id=?,operating_mode=? WHERE uuid=? AND state_version=?"
        withStatement(sql) { statement =>
          MySqlBusinessTransaction.bindPodFields(statement, value.desired, 1)
          statement.setBytes(13, value.expected.uuidBytes.toArray)
          statement.setLong(14, value.expected.stateVersion)
          requireExactlyOne(statement.executeUpdate(), "inactive pod update")
        }
      case value: MySqlBusinessMutation.DeleteInactivePod =>
        val sql = "DELETE FROM " + table("floating_pod_state") + " WHERE uuid=? AND state_version=?"
        withStatement(sql) { statement =>
          statement.setBytes(1, value.expected.uuidBytes.toArray)
          statement.setLong(2, value.expected.stateVersion)
          requireExactlyOne(statement.executeUpdate(), "inactive pod delete")
        }
      case value: MySqlBusinessMutation.ChangeEntitlement =>
        executeEntitlement(value)
      case _: MySqlBusinessMutation.ReplaceFullRepositorySnapshot =>
        throw new IllegalArgumentException("full repository snapshot plans require executeStaged")

  private def executeEntitlement(effect: MySqlBusinessMutation.ChangeEntitlement): Unit =
    (effect.expected, effect.desired) match
      case (None, Some(desired)) =>
        val sql = "INSERT INTO " + table("player_entitlement") +
          " (player_uuid,registration_limit,simultaneous_active_limit) VALUES (?,?,?)"
        withStatement(sql) { statement =>
          statement.setBytes(1, MySqlStoredRows.encodeUuid(desired.playerUuid).toArray)
          statement.setInt(2, desired.registrationLimit)
          statement.setInt(3, desired.simultaneousActiveLimit)
          requireExactlyOne(statement.executeUpdate(), "entitlement insert")
        }
      case (Some(expected), Some(desired)) =>
        val sql = "UPDATE " + table("player_entitlement") +
          " SET registration_limit=?,simultaneous_active_limit=? WHERE player_uuid=? AND registration_limit=? AND simultaneous_active_limit=?"
        withStatement(sql) { statement =>
          statement.setInt(1, desired.registrationLimit)
          statement.setInt(2, desired.simultaneousActiveLimit)
          statement.setBytes(3, MySqlStoredRows.encodeUuid(expected.playerUuid).toArray)
          statement.setInt(4, expected.registrationLimit)
          statement.setInt(5, expected.simultaneousActiveLimit)
          requireExactlyOne(statement.executeUpdate(), "entitlement update")
        }
      case (Some(expected), None) =>
        val sql = "DELETE FROM " + table("player_entitlement") +
          " WHERE player_uuid=? AND registration_limit=? AND simultaneous_active_limit=?"
        withStatement(sql) { statement =>
          statement.setBytes(1, MySqlStoredRows.encodeUuid(expected.playerUuid).toArray)
          statement.setInt(2, expected.registrationLimit)
          statement.setInt(3, expected.simultaneousActiveLimit)
          requireExactlyOne(statement.executeUpdate(), "entitlement delete")
        }
      case _ =>
        throw new IllegalArgumentException("entitlement effect must insert, update, or delete exactly one player row")

  private def table(name: String): String = quotedSchema + "." + MySqlFencedTransactionSupport.quote(name)

  private def withStatement[A](sql: String)(body: PreparedStatement => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql))(body)

  private def requireExactlyOne(actual: Int, operation: String): Unit =
    if actual != 1 then
      throw new SQLTransactionRollbackException("MySQL " + operation + " affected " + actual + " rows instead of one")

  private def ensureOpen(): Unit =
    if !open then throw new IllegalStateException("MySQL transaction facade is no longer open")
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("MySQL transaction facade is confined to its owned IO thread")
    verified.readmitSessionBarrier(expectation, scope)
    if connection.getAutoCommit then throw new IllegalStateException("typed MySQL mutation requires an active admitted transaction")

  private[mysql] def markFailure(failure: Throwable): Unit = rollbackOnlyState.mark(failure)

  private[mysql] def hasExactlyAppliedPlan: Boolean =
    open && invoked.toSet == plan.mutations.map(_.targetIdentity).toSet

  private[mysql] def close(): Unit = open = false

private object MySqlBusinessTransaction:
  private[mysql] def bindPodInsert(statement: PreparedStatement, row: MySqlStoredRowData): Unit =
    statement.setBytes(1, row.uuidBytes.toArray)
    bindPodFields(statement, row, 2)

  private[mysql] def bindPodFields(statement: PreparedStatement, row: MySqlStoredRowData, start: Int): Unit =
    setOptionalBytes(statement, start, row.ownerUuidBytes)
    statement.setLong(start + 1, row.stateVersion)
    statement.setString(start + 2, row.worldName)
    statement.setDouble(start + 3, row.posX)
    statement.setDouble(start + 4, row.posY)
    statement.setDouble(start + 5, row.posZ)
    statement.setDouble(start + 6, row.yaw)
    statement.setDouble(start + 7, row.pitch)
    statement.setInt(start + 8, row.isActiveFlag)
    statement.setLong(start + 9, row.lastUpdated)
    setOptionalInt(statement, start + 10, row.dimensionId)
    statement.setString(start + 11, row.operatingModeToken)

  private def setOptionalBytes(statement: PreparedStatement, index: Int, value: Option[Vector[Byte]]): Unit =
    value match
      case None => statement.setNull(index, Types.VARBINARY)
      case Some(bytes) => statement.setBytes(index, bytes.toArray)

  private def setOptionalInt(statement: PreparedStatement, index: Int, value: Option[Int]): Unit =
    value match
      case None => statement.setNull(index, Types.INTEGER)
      case Some(number) => statement.setInt(index, number)

/** 只读 manifest / binding / W 核验及 JDBC 类型解码辅助函数。 */
private[mysql] object MySqlFencedTransactionSupport:
  val PodColumns = "uuid,owner_uuid,state_version,world_name,pos_x,pos_y,pos_z,yaw,pitch,is_active,last_updated,dimension_id,operating_mode"

  def quote(identifier: String): String =
    val quoteCharacter = 96.toChar.toString
    quoteCharacter + identifier + quoteCharacter

  def expectedImportState(value: MySqlExpectedImportState): String = value match
    case MySqlExpectedNoImport => "NONE"
    case _: MySqlExpectedCommittedImport => "COMMITTED"

  def expectedImportOperation(value: MySqlExpectedImportState): Option[UUID] = value match
    case MySqlExpectedNoImport => None
    case imported: MySqlExpectedCommittedImport => Some(imported.operationUuid)

  def expectedImportManifest(value: MySqlExpectedImportState): Option[String] = value match
    case MySqlExpectedNoImport => None
    case imported: MySqlExpectedCommittedImport => Some(imported.manifestSha256.toLowerCase(java.util.Locale.ROOT))

  def expectedImportSnapshot(value: MySqlExpectedImportState): Option[String] = value match
    case MySqlExpectedNoImport => None
    case imported: MySqlExpectedCommittedImport => Some(imported.snapshotSha256.toLowerCase(java.util.Locale.ROOT))

  def expectedImportReceipt(value: MySqlExpectedImportState): Option[String] = value match
    case MySqlExpectedNoImport => None
    case imported: MySqlExpectedCommittedImport => Some(imported.receiptSha256.toLowerCase(java.util.Locale.ROOT))

  def importMatches(row: MySqlLockedManifest, expected: MySqlExpectedImportState): Boolean =
    expected match
      case MySqlExpectedNoImport =>
        row.importState == "NONE" &&
          row.importManifestSha256.isEmpty && row.importOperationUuid.isEmpty &&
          row.importSnapshotSha256.isEmpty && row.importReceiptSha256.isEmpty
      case imported: MySqlExpectedCommittedImport =>
        row.importState == "COMMITTED" &&
          row.importOperationUuid.contains(imported.operationUuid) &&
          row.importManifestSha256.exists(hash => MessageDigest.isEqual(hash.toArray, HexFormat.of().parseHex(imported.manifestSha256))) &&
          row.importSnapshotSha256.exists(hash => MessageDigest.isEqual(hash.toArray, HexFormat.of().parseHex(imported.snapshotSha256))) &&
          row.importReceiptSha256.exists(hash => MessageDigest.isEqual(hash.toArray, HexFormat.of().parseHex(imported.receiptSha256)))

  def hex(value: Vector[Byte]): String = HexFormat.of().formatHex(value.toArray)

  def rejectConflict(message: String): Nothing = throw new SQLTransactionRollbackException(message)

  def addSuppressed(primary: Throwable, secondary: Throwable): Unit =
    if primary != null && secondary != null && (primary ne secondary) &&
      !primary.getSuppressed.contains(secondary) then primary.addSuppressed(secondary)

  def setOptionalUuid(statement: PreparedStatement, index: Int, value: Option[UUID]): Unit =
    value match
      case None => statement.setNull(index, Types.VARBINARY)
      case Some(uuid) => statement.setBytes(index, MySqlStoredRows.encodeUuid(uuid).toArray)

  def setOptionalBytes(statement: PreparedStatement, index: Int, value: Option[Vector[Byte]]): Unit =
    value match
      case None => statement.setNull(index, Types.VARBINARY)
      case Some(bytes) => statement.setBytes(index, bytes.toArray)

  def one[A](result: ResultSet, label: String)(read: ResultSet => A): A =
    if !result.next() then MySqlSchemaAuditor.reject("MySQL " + label + " singleton row is missing")
    val value = read(result)
    if result.next() then MySqlSchemaAuditor.reject("MySQL " + label + " singleton query returned multiple rows")
    value

  def requiredInt(result: ResultSet, index: Int, label: String): Int =
    val value = result.getInt(index)
    if result.wasNull() then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
    value

  def requiredLong(result: ResultSet, index: Int, label: String): Long =
    val value = result.getLong(index)
    if result.wasNull() then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
    value

  def requiredText(result: ResultSet, index: Int, label: String): String =
    val value = result.getString(index)
    if result.wasNull() || value == null then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL")
    value

  def requiredBytes(result: ResultSet, index: Int, label: String, length: Int): Array[Byte] =
    val value = result.getBytes(index)
    if value == null || value.length != length then MySqlSchemaAuditor.reject("MySQL " + label + " must contain exactly " + length + " bytes")
    value

  def optionalHash(result: ResultSet, index: Int, label: String): Option[Vector[Byte]] =
    optionalBytes(result, index, label, 32)

  private def optionalBytes(result: ResultSet, index: Int, label: String, length: Int): Option[Vector[Byte]] =
    val value = result.getBytes(index)
    if value == null then None
    else if value.length != length then MySqlSchemaAuditor.reject("MySQL " + label + " must contain exactly " + length + " bytes when present")
    else Some(value.toVector)

  def optionalUuid(result: ResultSet, index: Int, label: String): Option[UUID] =
    optionalBytes(result, index, label, 16).map(bytes => MySqlStoredRows.decodeUuid(bytes, label))

  def requiredUuid(result: ResultSet, index: Int, label: String): UUID =
    optionalUuid(result, index, label).getOrElse(MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL"))

  def requiredLocalDateTime(result: ResultSet, index: Int, label: String): LocalDateTime =
    val value = result.getObject(index, classOf[LocalDateTime])
    if result.wasNull() || value == null then MySqlSchemaAuditor.reject("MySQL " + label + " is SQL NULL or not DATETIME")
    value

/** Recall固定写入DAG；只能由同一真实M/B/W锁事务的私有runner调用。 */
private[mysql] object MySqlNormalRecallDeltaWriter:
  import MySqlControlledDebugRows.*
  def writeReserve(connection: Connection, schema: String, m: MySqlNormalRecallMaterialization): Unit =
    requireInputs(connection, schema, m)
    m.reserve.requireObserved(m.completeReserveTarget, true)
    insert(connection, schema, m, Operation, operation(m.completeReserveTarget, m, m.plan.reserveOperationUuid))
    insert(connection, schema, m, Operation, operation(m.completeReserveTarget, m, m.plan.executeOperationUuid))
    val held = reservation(m.completeReserveTarget, m)
    val old = findByKey(m.completePreimage, Reservation, held)
    if old == null then insert(connection, schema, m, Reservation, held)
    else update(connection, schema, m, Reservation, old, held)

  def writeExecute(connection: Connection, schema: String, m: MySqlNormalRecallMaterialization): Unit =
    requireInputs(connection, schema, m)
    m.execute.requireObserved(m.completeExecuteTarget, true)
    m.exactOperation match
      case MySqlRepositoryOperation.PrepareNormalRecall(_) => ()
      case MySqlRepositoryOperation.CheckpointNormalRecall(request) =>
        def selected(table: String, key: String, id: UUID): Vector[MySqlFormat3Cell] =
          one(m.completeExecuteTarget, table, row =>
            uuidMatches(row, table, "binding_uuid", request.original.expectedBinding.bindingUuid) &&
              uuidMatches(row, table, key, id))
        updateFromImages(connection, schema, m, Domain, selected(Domain, "uuid", request.original.podUuid))
        updateFromImages(connection, schema, m, Locator, selected(Locator, "pod_uuid", request.original.podUuid))
        val obligations = m.completeExecuteTarget.relation("physical_recovery_obligation").rows
        obligations.foreach { after =>
          val before = findByKey(m.completeReserveTarget, "physical_recovery_obligation", after)
          if before == null then insert(connection, schema, m, "physical_recovery_obligation", after)
          else if before != after then fail("checkpoint cannot mutate an existing obligation")
        }
        // DDL completion guard observes exact C still RESERVED and original R still HELD.
        insert(connection, schema, m, "fp_lifecycle_completion",
          selected("fp_lifecycle_completion", "receipt_uuid", request.receiptUuid))
      case _ => fail("operation is outside initial P/checkpoint C")
    update(connection, schema, m, Operation, operation(m.completeReserveTarget, m, m.plan.executeOperationUuid),
      operation(m.completeExecuteTarget, m, m.plan.executeOperationUuid))
    update(connection, schema, m, Reservation, reservation(m.completeReserveTarget, m), reservation(m.completeExecuteTarget, m))

  private def requireInputs(connection: Connection, schema: String, m: MySqlNormalRecallMaterialization): Unit =
    if connection == null || m == null || schema == null || !schema.matches("[a-z][a-z0-9_]{0,63}") ||
      schema != m.completePreimage.schema || connection.isClosed || connection.getAutoCommit ||
      connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      fail("active original same-connection RR transaction required")
    m.verify()
  private def one(image:MySqlModeOperationEffectCodec.Image,table:String,
      predicate:Vector[MySqlFormat3Cell]=>Boolean):Vector[MySqlFormat3Cell]=image match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugImages.one(legacy,table,predicate)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.one(deployment,table,predicate)
  private def operation(snapshot: MySqlModeOperationEffectCodec.Image,m: MySqlNormalRecallMaterialization,id: UUID): Vector[MySqlFormat3Cell] =
    one(snapshot,Operation,row=>uuidMatches(row,Operation,"binding_uuid",m.header.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,Operation,"operation_uuid",id))
  private def reservation(snapshot: MySqlModeOperationEffectCodec.Image,m: MySqlNormalRecallMaterialization): Vector[MySqlFormat3Cell] =
    one(snapshot,Reservation,row=>uuidMatches(row,Reservation,"binding_uuid",m.header.request.expectedBinding.bindingUuid) &&
      value(row,Reservation,"resource_kind")==text("POD") && uuidMatches(row,Reservation,"resource_pod_uuid",m.header.request.podUuid))
  private def uuidMatches(row: Vector[MySqlFormat3Cell],table: String,column: String,id: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(value(row,table,column),id)

  private def findByKey(snapshot: MySqlModeOperationEffectCodec.Image,table: String,row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
    val shape=MySqlFormat3RelationShapes.forTable(table)
    val rows=snapshot.relation(table).rows
    var result: Vector[MySqlFormat3Cell]=null
    var index=0
    while index<rows.size do
      if MySqlControlledDebugDelta.compareKey(rows(index),row,shape)==0 then
        if result!=null then fail("duplicate actual fixed primary key")
        result=rows(index)
      index+=1
    result

  private def updateFromImages(connection: Connection,schema: String,m: MySqlNormalRecallMaterialization,
      table: String,after: Vector[MySqlFormat3Cell]): Unit =
    val before=findByKey(m.completeReserveTarget,table,after)
    if before==null then fail("normal recall CAS lost its original row")
    update(connection,schema,m,table,before,after)

  private def insert(connection: Connection,schema: String,m: MySqlNormalRecallMaterialization,
      table: String,row: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val shape=MySqlFormat3RelationShapes.forTable(table)
      val columns=shape.columns.map(c=>"`"+c.name+"`").mkString(",")
      val placeholders=shape.columns.map(_=>"?").mkString(",")
      val sql="INSERT INTO `"+schema+"`.`"+table+"` ("+columns+") VALUES ("+placeholders+")"
      execute(connection,m,shape,sql,shape.columns.indices.map(index=>(index,row(index))).toVector,budget)
    }

  private def update(connection: Connection,schema: String,m: MySqlNormalRecallMaterialization,
      table: String,before: Vector[MySqlFormat3Cell],after: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val shape=MySqlFormat3RelationShapes.forTable(table)
      val changed=shape.columns.indices.filter(index => !shape.primaryKey.contains(shape.columns(index).name) && before(index)!=after(index)).toVector
      if changed.isEmpty then fail("normal recall CAS has no changed field")
      val guards=table match
        case Domain => Vector("state_version","is_active","last_mutation_operation_uuid")
        case Locator => Vector("locator_revision","current_attempt_uuid","current_footprint_uuid","last_mutation_operation_uuid")
        case Operation => Vector("operation_state","operation_fingerprint")
        case Reservation => Vector("reservation_revision","reservation_state","workflow_uuid","operation_uuid","last_mutation_operation_uuid")
        case _ => fail("table is outside fixed normal recall update DAG")
      val predicates=shape.primaryKey++guards
      val sql="UPDATE `"+schema+"`.`"+table+"` SET "+changed.map(i=>"`"+shape.columns(i).name+"`=?").mkString(",")+
        " WHERE "+predicates.map(name=>"`"+name+"` <=> ?").mkString(" AND ")
      val values=changed.map(index=>(index,after(index)))++predicates.map { name =>
        val index=shape.columns.indexWhere(_.name==name)
        if index<0 then fail("fixed CAS predicate column disappeared")
        (index,before(index))
      }
      execute(connection,m,shape,sql,values,budget)
    }

  private def withStatementBudget(m: MySqlNormalRecallMaterialization,table: String)
      (body: MySqlFormat3ReadBudget=>Unit): Unit =
    MySqlRecoveryCanonicalBytes.withComparisonBudget(m.originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(m.originalRoot.profile)
      child.reserveTemporaryMemory(65536L,"normal recall fixed SQL text/index/binding containers "+table)
      var primary: Throwable = null
      try body(child)
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try child.releaseTemporaryMemory(65536L,"normal recall fixed SQL containers released")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    }

  private def execute(connection: Connection,m: MySqlNormalRecallMaterialization,shape: MySqlFormat3RelationShape,
      sql: String,values: Vector[(Int,MySqlFormat3Cell)],budget: MySqlFormat3ReadBudget): Unit =
    var packet=MySqlFormat3Utf8.byteLength(sql)
    var index=0
    while index<values.size do
      val bytes=values(index)._2 match
        case MySqlFormat3Cell.SqlNull => 1L
        case MySqlFormat3Cell.Text(value) => Math.addExact(MySqlFormat3Utf8.byteLength(value),16L)
        case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong,16L)
        case _ => 32L
      packet=Math.addExact(packet,bytes);index+=1
    if packet>Math.min(m.completePreimage.maxAllowedPacketBytes,budget.profile.maxSqlPacketBytes()) then
      fail("fixed write framing exceeds actual observed/profile packet bound")
    val arrays=scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var primary: Throwable=null
    try
      MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
        var index=0
        while index<values.size do
          val (columnIndex,cell)=values(index)
          val parameter=index+1
          val column=shape.columns(columnIndex)
          cell match
            case MySqlFormat3Cell.SqlNull => statement.setNull(parameter,column.jdbcType)
            case MySqlFormat3Cell.Text(value) => statement.setString(parameter,value)
            case MySqlFormat3Cell.Binary(value) =>
              val copy=value.copyBytes(budget,"normal recall SQL binary binding")
              arrays+=copy;statement.setBytes(parameter,copy)
            case MySqlFormat3Cell.SignedInteger(value) =>
              if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                if value<Int.MinValue.toLong || value>Int.MaxValue.toLong then fail("fixed INT binding overflow")
                statement.setInt(parameter,value.toInt)
              else statement.setLong(parameter,value)
            case MySqlFormat3Cell.Floating(bits) => statement.setDouble(parameter,java.lang.Double.longBitsToDouble(bits))
            case MySqlFormat3Cell.DateTime(value) => statement.setObject(parameter,value)
          index+=1
        if statement.executeUpdate()!=1 then fail("exact normal recall guarded statement did not change one row")
      }
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally
      try
        arrays.foreach(bytes=>java.util.Arrays.fill(bytes,0.toByte))
        arrays.clear()
      catch case cleanup: Throwable =>
        if primary==null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)
      // Copies were charged to lexical child and lose all references before its close。

  private def fail(reason: String): Nothing = throw new SQLDataException("normal recall fixed F3 SQL DAG: "+reason)
