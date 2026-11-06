package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRefusalReason

import com.wi.floatingpod.entity.{DbFirstCaptureAllocation, DbFirstPodSnapshot}
import com.wi.floatingpod.server.{ExactBodyRemovalAcknowledgement,TicketReleaseAcknowledgement}

import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.server.io.{IoCheckpoint, IoCompletion, IoOperationTag, IoOwnerTransfer, IoSubmission, IoWorkerMaintenance, IoMaintenanceDeadline, OwnedSerialIo}
import com.wi.floatingpod.bootstrap.{DedicatedBootstrapBridge, OwnedIoPreparation, IoPreparationObservation, ProductionBootstrapControl}
import com.wi.floatingpod.server.source.SourceDomainCapture
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CompletionStage, CountDownLatch, TimeUnit}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import scala.util.{Failure, Success}

final case class MySqlOwnedIoIdentity(bootId: UUID, bootGeneration: Long, connectionGeneration: Long):
  require(bootId != null && bootGeneration > 0L && connectionGeneration > 0L, "exact owned IO identity required")
  def requireTag(tag: IoOperationTag): Unit =
    require(tag != null && tag.bootId == bootId && tag.bootGeneration == bootGeneration &&
      tag.connectionGeneration == connectionGeneration, "command tag differs from owned database boot/connection")
/** 只观察 exact adapter/identity 的 IO worker 已终止；不代表 JDBC close、DB fence 或 server barrier。 */
enum MySqlOwnedIoWorkerTerminationWait:
  case Terminated(proof: MySqlOwnedIoWorkerTerminationProof)
  case TimedOut
  case Interrupted(cause: InterruptedException)

/** opaque adapter binding；接收者必须再次以实际 adapter、IO、identity 与 issuer references 验证。 */
final class MySqlOwnedIoWorkerTerminationProof private[mysql] (
    private val adapter: AnyRef,
    private val identity: MySqlOwnedIoIdentity,
    private val ownedIo: AnyRef,
    private val issuer: AnyRef,
    private val workerProof: com.wi.floatingpod.server.io.IoWorkerTerminationProof):
  private[mysql] def requireBinding(actualAdapter: AnyRef, actualIdentity: MySqlOwnedIoIdentity,
      actualIo: AnyRef, actualIssuer: AnyRef): com.wi.floatingpod.server.io.IoWorkerTerminationProof =
    require((adapter eq actualAdapter) && (identity eq actualIdentity) && (ownedIo eq actualIo) &&
      (issuer eq actualIssuer) && workerProof != null, "worker termination proof belongs to another adapter generation")
    workerProof
  def requireExactIdentity(expected: MySqlOwnedIoIdentity): Unit =
    require(expected != null && (identity eq expected), "worker termination proof belongs to another MySQL IO identity reference")
  override def toString: String = "MySqlOwnedIoWorkerTerminationProof[IO-only]"

private[mysql] object MySqlOwnedIoWorkerTerminationProof:
  def issue(adapter: AnyRef, identity: MySqlOwnedIoIdentity, ownedIo: AnyRef, issuer: AnyRef,
      workerProof: com.wi.floatingpod.server.io.IoWorkerTerminationProof): MySqlOwnedIoWorkerTerminationProof =
    require(adapter != null && identity != null && ownedIo != null && issuer != null && workerProof != null,
      "actual adapter IO worker termination references required")
    new MySqlOwnedIoWorkerTerminationProof(adapter, identity, ownedIo, issuer, workerProof)

enum MySqlOwnedIoCommand:
  /** schema6原worker初始化观察；不发行Ready、writer fence或repository plan。 */
  case ObserveDbFirst(metadata: MySqlRuntimeMetadata.DbFirstStatic, sourceLimitProfile: SourceLimitProfile,
      auditPolicy: MySqlRuntimeAuditPolicy)
  /** 正式静态control进入原worker；语义digest在首次SQL前核对。 */
  case ObserveApprovedDbFirst(control: ProductionBootstrapControl.DbFirstControl)
  /** Managed初始化只在原owned IO worker执行，完成后立即沿同一worker执行原schema6观察。 */
  case ObserveManagedDbFirst(request: MySqlManagedBootstrapRequest)
  /** 由已完成观测的原schema6 Session取得SQL fence；不发布world恢复或Ready资格。 */
  case AcquireDbFirstWriter(leaseDuration: Duration, barrierWait: Duration)
  /** 原schema6 Session续租其既有SQL fence；不更换operation或发布Ready。 */
  case RenewDbFirstWriter
  case TakeoverBootRecall(intent:MySqlDbFirstBootRecallTakeoverIntent)
  /** 只消费原capture Entry；SQL确认仍不代表Main已移除实体/ticket。 */
  case ExecuteDbFirstRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource)
  case ExecuteDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation)
  case ExecuteDbFirstRecall(reservation: MySqlDbFirstCaptureReservation)
  case ExecuteDbFirstCheckpoint(reservation: MySqlDbFirstCaptureReservation)
  case ExecuteDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation)
  case ExecuteDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,activeLimit:Int)
  case ReleaseDbFirstRecall(receipt: MySqlDbFirstRecallDropReceipt)
  case ReleaseRetainedDbFirstRecall(receipt:MySqlDbFirstRecallDropReceipt)
  case ReleaseRejectedPreparationMainRead(confirmation:MySqlDbFirstPreparationRejectionConfirmation)
  case RetireRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation)
  case ReleaseCheckpointMainRead(confirmation:MySqlDbFirstCheckpointConfirmation)
  case ReleaseDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt)
  case ReleaseRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt)
  case ReleaseRefusalMainRead(confirmation:MySqlDbFirstRecallRefusalConfirmation)
  case ReleaseJournalMainRead(confirmation:MySqlDbFirstRecallJournalConfirmation)
  case RetireJournalForCapture(transfer:MySqlDbFirstRecallJournalCaptureTransfer)
  case ReleasePreparationMainRead(confirmation:MySqlDbFirstInitialPreparationConfirmation)
  case ReleaseDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt)
  case ReleaseDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt)
  case Boot(metadata: MySqlRuntimeMetadata, auditPolicy: MySqlRuntimeAuditPolicy, leaseDuration: Duration)
  case BootFresh(metadata: MySqlRuntimeMetadata, auditPolicy: MySqlRuntimeAuditPolicy, leaseDuration: Duration, request: MySqlFreshBootRequest)
  case BootNormalPreGenesis(metadata: MySqlRuntimeMetadata, auditPolicy: MySqlRuntimeAuditPolicy, leaseDuration: Duration, request: MySqlNormalBootRequest)
  case BootCertifiedInspection(metadata: MySqlRuntimeMetadata, auditPolicy: MySqlRuntimeAuditPolicy, request: MySqlCertifiedRestoreRequest)
  case BootOrdinaryCertified(metadata: MySqlRuntimeMetadata, auditPolicy: MySqlRuntimeAuditPolicy,
      leaseDuration: Duration, request: MySqlOrdinaryCertifiedBootRequest)
  case BootFreshV3(control: ProductionBootstrapControl.StaticV3, request: MySqlFreshBootRequest)
  case BootOrdinaryV3(control: ProductionBootstrapControl.StaticV3, request: MySqlOrdinaryCertifiedBootRequest)
  case BootFresh5(control: ProductionBootstrapControl.DeploymentReceiptControl, worldPath: java.nio.file.Path,
      plan: com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData,
      boot: com.wi.floatingpod.server.WindowsWorldProvisioner.BootIdentity, controlled: MySqlFreshControlledMetadata)
  case BootOrdinary5(control: ProductionBootstrapControl.DeploymentReceiptControl, worldPath: java.nio.file.Path,
      current: ProductionBootstrapControl.OrdinaryIdentity, original: ProductionBootstrapControl.OriginalSourcePins,
      controlled: MySqlFreshControlledMetadata)
  case Renew
  case ReadRequestPreview(request: MySqlRequestReadPreview, plan: MySqlOwnedRepositoryOperationPlan)
  case ReadOriginalDeployment(operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation, plan: MySqlOwnedRepositoryOperationPlan)
  case ReleaseRequestRead(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt)
  case PrepareNormalRecall(request: MySqlNormalRecallRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case CheckpointNormalRecall(prepared: MySqlFencedTransaction.NormalRecallCommitted,
      request: MySqlNormalRecallCheckpointRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case PrepareControlledDebug(request: MySqlControlledDebugPrepareRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case CommitControlledDebug(prepared: MySqlControlledDebugPreparedGraph, actualPose: com.wi.floatingpod.domain.FloatingPodPose, globalActiveLimit: Int, plan: MySqlOwnedRepositoryOperationPlan)
  case CommitControlledUnbound(receipt: com.wi.floatingpod.storage.UnboundPreparationReceipt, actualPose: com.wi.floatingpod.domain.FloatingPodPose, globalActiveLimit: Int, plan: MySqlOwnedRepositoryOperationPlan)
  case Run(operation: MySqlRepositoryOperation, plan: MySqlOwnedRepositoryOperationPlan)
  case Transaction(operations: Vector[MySqlRepositoryOperation], plan: MySqlOwnedRepositoryOperationPlan)
  case PrepareControlledUnbound(request: MySqlControlledUnboundPrepareRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case CommitOwnedMode(request: MySqlOwnedModeCommitRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case CommitTrackedDrift(request: MySqlTrackedDriftRequest, plan: MySqlOwnedRepositoryOperationPlan)
  case ReleaseDriftProjection(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt)
  case ReleaseModeProjection(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt)
  case ReleaseRetainedPlan(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt)
  case PrepareRestrictedSourceReservePlan(intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent)
  case ExecuteRestrictedSourceReserveR0(backing:MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking,
      planAck:MySqlOwnedIoAdapter.RestrictedSourcePlanAck)
  case DropRestrictedSourceReservePlan(receipt: MySqlFencedTransaction.SourceReservePlanDropReceipt)

  /** 纯请求/plan 匹配；profile 必须由实际worker session getter提供，不授予SQL权限。 */
  def requireRepositoryPlan(tag: IoOperationTag, actualProfile: SourceLimitProfile): MySqlOwnedRepositoryOperationPlan =
    def single(operation: MySqlRepositoryOperation, plan: MySqlOwnedRepositoryOperationPlan): MySqlOwnedRepositoryOperationPlan =
      require(plan != null, "mandatory Main-retained repository plan required")
      plan.requireTag(tag)
      plan.requireSingle(operation, actualProfile)
      plan
    this match
      case PrepareNormalRecall(request, plan) => single(MySqlRepositoryOperation.PrepareNormalRecall(request), plan)
      case CheckpointNormalRecall(prepared, request, plan) =>
        require(prepared != null && !prepared.checkpoint && request != null && (request.original eq prepared.request),
          "checkpoint requires the original private P and original request")
        single(MySqlRepositoryOperation.CheckpointNormalRecall(request), plan)
      case Run(operation, plan) => single(operation, plan)
      case ReadOriginalDeployment(operation, plan) => single(operation, plan)
      case ReadRequestPreview(request, plan) =>
        require(request != null && plan != null, "fixed original request-read plan required")
        plan.requireTag(tag)
        plan.requireTransaction(request.operations, actualProfile)
        plan
      case Transaction(operations, plan) =>
        require(plan != null, "mandatory Main-retained transaction plan required")
        plan.requireTag(tag)
        plan.requireTransaction(operations, actualProfile)
        plan
      case PrepareControlledDebug(request, plan) =>
        require(request != null, "fixed F3 controlled preparation required")
        single(request.repositoryOperation, plan)
      case CommitControlledDebug(prepared, pose, limit, plan) =>
        require(prepared != null && prepared.hasPrivateReadbackProof, "original privately issued F3 preparation required")
        single(prepared.commitOperation(pose, limit), plan)
      case PrepareControlledUnbound(request, plan) =>
        require(request != null, "immutable prepare request required")
        single(request.repositoryOperation, plan)
      case CommitTrackedDrift(request, plan) =>
        require(request != null, "immutable drift request required")
        single(request.repositoryOperation, plan)
      case CommitOwnedMode(request, plan) =>
        require(request != null, "immutable mode request required")
        single(request.repositoryOperation, plan)
      case CommitControlledUnbound(receipt, pose, limit, plan) =>
        single(MySqlOwnedIoCommand.controlledCommitOperation(receipt, pose, limit), plan)
      case _ => throw new IllegalArgumentException("Boot/renew does not carry a repository operation plan")

object MySqlOwnedIoCommand:
  /** 专用部署读取只走固定command；在plan/router/epoch/SQL副作用前拒绝通用途径。 */
  private[mysql] def requireGenericRoute(command: MySqlOwnedIoCommand): Unit = command match
    case MySqlOwnedIoCommand.ReleaseRetainedDbFirstRecall(_) =>
      throw new IllegalArgumentException("retained recall release requires its exact original owner route")
    case MySqlOwnedIoCommand.Run(operation, _) => MySqlImmutableCommandDispatcher.requireGenericOperation(operation)
    case MySqlOwnedIoCommand.Transaction(operations, _) => MySqlImmutableCommandDispatcher.requireGenericOperations(operations)
    case MySqlOwnedIoCommand.PrepareRestrictedSourceReservePlan(_) =>
      throw new IllegalArgumentException("restricted source plan requires its exact readonly owner route")
    case MySqlOwnedIoCommand.ExecuteRestrictedSourceReserveR0(_, _) =>
      throw new IllegalArgumentException("restricted source R0 requires its exact retained plan/accepted-owner route")
    case MySqlOwnedIoCommand.DropRestrictedSourceReservePlan(_) =>
      throw new IllegalArgumentException("restricted source plan drop requires its exact readonly owner route")
    case _ => ()

  /** 完全使用实际receipt字段；不重新提议domain UUID，不声明已提交。 */
  def controlledCommitOperation(receipt: com.wi.floatingpod.storage.UnboundPreparationReceipt,
      pose: com.wi.floatingpod.domain.FloatingPodPose, limit: Int): MySqlRepositoryOperation =
    require(receipt != null && pose != null && receipt.origin == com.wi.floatingpod.storage.UnboundIntentOrigin.DebugItemCreate &&
      receipt.createOperationUuid.nonEmpty && receipt.observedIdentity.isEmpty, "controlled commit requires exact DEBUG_ITEM_CREATE receipt")
    MySqlRepositoryOperation.CommitUnboundCreate(receipt.binding, receipt.podUuid, receipt.attemptUuid,
      receipt.preparedVersion, pose, receipt.createOperationUuid.get, limit)

enum MySqlOwnedIoReply:
  case DbFirstObserved(value: MySqlDbFirstSession.DatabaseObservation)
  case DbFirstSqlWriterAcquired(value: MySqlDbFirstSession.SqlWriterObservation)
  case DbFirstSqlWriterRenewed(value: MySqlDbFirstSession.SqlWriterObservation)
  case BootRecallTakenOver(intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,epoch:Long)
  case DbFirstRecallRefusalObserved(value:MySqlDbFirstSession.DbFirstRecallRefusalObservation)
  case DbFirstRecallJournalObserved(value:MySqlDbFirstSession.DbFirstRecallJournalObservation)
  case DbFirstRecallObserved(value: MySqlDbFirstSession.DbFirstRecallObservation)
  case DbFirstRecallRetained(qualification:MySqlDbFirstSession.FailedRecallCleanupQualification)
  case DbFirstCheckpointObserved(value: MySqlDbFirstSession.DbFirstCheckpointObservation)
  case DbFirstInitialPreparationObserved(value:MySqlDbFirstSession.DbFirstInitialPreparationObservation)
  case DbFirstInitialPreparationRejected(value:MySqlDbFirstSession.DbFirstInitialPreparationRejection)
  case DbFirstInitialActivationObserved(value:MySqlDbFirstSession.DbFirstInitialActivationObservation)
  case DbFirstRecallReleased(receipt: MySqlDbFirstRecallDropReceipt)
  case DbFirstRecallRetirementFailed(failure: MySqlDbFirstLocalRetirementException)
  case RejectedPreparationMainReadReleased(confirmation:MySqlDbFirstPreparationRejectionConfirmation)
  case RejectedPreparationRetired(confirmation:MySqlDbFirstPreparationRejectionConfirmation)
  case CheckpointMainReadReleased(confirmation:MySqlDbFirstCheckpointConfirmation)
  case DbFirstCheckpointFinalReleased(receipt:MySqlDbFirstCheckpointFinalReceipt)
  case DbFirstCheckpointFinalRetirementFailed(failure:MySqlDbFirstCheckpointLocalRetirementException)
  case RefusalFinalReleased(receipt:MySqlDbFirstRecallRefusalFinalReceipt)
  case RefusalFinalRetirementFailed(failure:MySqlDbFirstRefusalFinalLocalRetirementException)
  case RefusalMainReadReleased(confirmation:MySqlDbFirstRecallRefusalConfirmation)
  case JournalMainReadReleased(confirmation:MySqlDbFirstRecallJournalConfirmation)
  case JournalRetiredForCapture(transfer:MySqlDbFirstRecallJournalCaptureTransfer)
  case JournalCaptureRetirementFailed(failure:MySqlDbFirstJournalCaptureLocalRetirementException)
  case PreparationMainReadReleased(confirmation:MySqlDbFirstInitialPreparationConfirmation)
  case DbFirstPreparationFinalReleased(receipt:MySqlDbFirstPreparationFinalReceipt)
  case DbFirstInitialPublicationReleased(receipt:MySqlDbFirstInitialPublicationReceipt)
  case DbFirstPreparationFinalRetirementFailed(failure:MySqlDbFirstPreparationLocalRetirementException)
  case DbFirstInitialPublicationRetirementFailed(failure:MySqlDbFirstInitialLocalRetirementException)
  case DeploymentReceiptBooted(owned: MySqlProductionBootReply.DeploymentReceiptBacking)
  case NormalRecallPrepared(graph: MySqlFencedTransaction.NormalRecallCommitted)
  case NormalRecallCheckpointed(graph: MySqlFencedTransaction.NormalRecallCommitted)
  case ControlledDebugPrepared(graph: MySqlControlledDebugPreparedGraph)
  case ControlledDebugCommitted(graph: MySqlControlledDebugCommittedGraph)
  case Booted(databaseAdmission: MySqlOwnedDatabaseAdmission, fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot, actualSourceLimitProfile: SourceLimitProfile)
  case FreshBooted(databaseAdmission: MySqlOwnedDatabaseAdmission, fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot, facts: MySqlFreshBootFacts, actualSourceLimitProfile: SourceLimitProfile)
  case NormalPreGenesisBooted(databaseAdmission: MySqlOwnedDatabaseAdmission, fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot, facts: MySqlNormalBootFacts, actualSourceLimitProfile: SourceLimitProfile)
  case OrdinaryCertifiedBooted(owned: MySqlOwnedIoAdapter.OrdinaryBootResultBacking)
  case DeploymentReceiptRecallRecoveringBooted(owned:MySqlProductionBootReply.RestrictedRecallBacking)
  case RecallRecoveringBooted(recovery: MySqlOwnedIoAdapter.RecallRecoveryLink,
      fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot, ordinaryFacts: MySqlOrdinaryFacts,
      actualSourceLimitProfile: SourceLimitProfile)
  case CertifiedInspectionObserved(observation: MySqlCertifiedInspectionObservation)
  case Renewed(databaseAdmission: MySqlOwnedDatabaseAdmission)
  case ControlledUnboundCommitted(graph: MySqlCommittedUnboundGraph)
  case ControlledUnboundReadbackFailed(responsibility: MySqlUnboundAfterCommitReadbackFailure)
  case ControlledUnboundRejected(outcome: com.wi.floatingpod.storage.UnboundCommitOutcome.Rejected)
  case Operation(result: MySqlRepositoryReply)
  case Transaction(results: Vector[MySqlRepositoryReply])
  case ControlledUnboundPreparation(graph: MySqlPreparedUnboundGraph)
  case ControlledUnboundPrepareReadbackFailed(responsibility: MySqlUnboundAfterPrepareReadbackFailure)
  case OwnedModeCommitted(graph: MySqlCommittedOwnedModeGraph)
  case OwnedModeReadbackFailed(responsibility: MySqlOwnedModeExecutionFailure)
  case OwnedModeRejected(outcome: com.wi.floatingpod.storage.OperatingModeChangeOutcome.Rejected)
  case OwnedModeAlreadySet(readback: MySqlOwnedModeAlreadySetReadback)
  case TrackedDriftCommitted(graph: MySqlCommittedTrackedDriftGraph)
  case TrackedDriftRejected(outcome: MySqlTrackedDriftRejection)
  case TrackedDriftFailed(responsibility: MySqlTrackedDriftExecutionFailure)
  case DriftProjectionReleased(tag: IoOperationTag)
  case DriftProjectionReleaseFailed(tag: IoOperationTag, cause: Throwable)
  case RetainedPlanReleased(tag: IoOperationTag)
  case RetainedPlanReleaseFailed(tag: IoOperationTag, cause: Throwable)
  case RestrictedSourceReservePlanPrepared(owned: MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking)
  case RestrictedSourceR0OutcomeRetained(outcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome)
  case RestrictedSourceR0WorkerFailed(cause:Throwable)
  case RestrictedSourceReservePlanDropped(ack: MySqlFencedTransaction.SourceReservePlanDropAck)
  case ModeProjectionReleased(tag: IoOperationTag)
  case ModeProjectionReleaseFailed(tag: IoOperationTag, cause: Throwable)

private[floatingpod] enum MySqlRestrictedSourceReservePlanPoll:
  case Pending
  case Ready(ack: MySqlOwnedIoAdapter.RestrictedSourcePlanAck)
  case Rejected(ack: MySqlOwnedIoAdapter.RestrictedSourcePlanRejectionAck)
  case Failed(ack: MySqlOwnedIoAdapter.RestrictedSourcePlanFailureAck)
  case Unknown(cause: Throwable)

/** A normal Main/provider conflict or an already-retained plan consumes no new tag/intent slot. */
private[floatingpod] enum MySqlRestrictedSourceReservePlanSubmission:
  case Deferred
  case Submitted(value: IoSubmission)

private[floatingpod] enum MySqlRestrictedSourceR0ExecutionPoll:
  case Pending
  case SubmissionRejected(value:IoSubmission)
  case SubmissionUnknown(cause:Throwable,lateCompletion:Option[IoCompletion[MySqlOwnedIoReply]])
  case SubmissionUnknownHandoffFailure(stage:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage,
      cause:Throwable,handoffFailure:Throwable,lateCompletion:Option[IoCompletion[MySqlOwnedIoReply]])
  // `submissionReturned=false` 与无 submission 等价；true 时 `submission` 保留原返回值，避免新建 Option 容器。
  case PreEnqueueRetainedFailure(stage:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage,
      chargeAttempted:Boolean,chargeReturned:Boolean,submissionReturned:Boolean,submission:IoSubmission,
      acceptedRepositoryEpoch:Long,cause:Throwable,secondaryCause:Throwable)
  case HandoffBookkeepingFailure(stage:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage,
      submissionReturned:Boolean,submission:IoSubmission,acceptedRepositoryEpoch:Long,cause:Throwable,secondaryCause:Throwable,
      lateCompletion:Option[IoCompletion[MySqlOwnedIoReply]])
  case RoutingFailure(cause:Throwable)
  case RoutingFailureWithSecondary(cause:Throwable,secondaryCause:Throwable)
  case WorkerFailed(cause:Throwable)
  case OutcomeRetained(outcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome)

private[floatingpod] enum MySqlRestrictedSourceR0ExecutionSubmission:
  case Submitted(value:IoSubmission)
  case Unknown(cause:Throwable)

private[floatingpod] enum MySqlRestrictedSourcePlanDropPoll:
  case Pending
  case Ready(ack: MySqlFencedTransaction.SourceReservePlanDropAck)
  case Rejected(value: IoSubmission)
  case Unknown(cause: Throwable)
  case Failed(cause: Throwable)

private[floatingpod] enum MySqlRestrictedSourcePlanDropSubmission:
  case Submitted(value: IoSubmission)
  case AlreadyPending

enum MySqlOwnedIoResourceObservation:
  case DbFirstObserved(value: MySqlDbFirstSession.ResourceObservation)
  case DbFirstAssemblyObserved(stage: String, failure: Option[Throwable], retained: Boolean)
  case DeploymentReceiptObserved(failure: Option[Throwable], retained: Boolean)
  case DeploymentReceiptAssemblyObserved(stage: String, failure: Option[Throwable], retained: Boolean)
  case NoVerifiedSessionOwned
  case StaticV3AssemblyObserved(failure: Option[Throwable], unsettled: Boolean)
  case PartialInitializationObserved(value: MySqlOwnedSessionCloseObservation)
  case SessionObserved(value: MySqlOwnedSessionCloseObservation)

final case class MySqlOwnedIoCheckpoint(io: IoCheckpoint, resources: Option[MySqlOwnedIoResourceObservation],
    controlledAfterAckFailure: Option[MySqlUnboundAfterCommitReadbackFailure] = None,
    preparedAfterAckFailure: Option[MySqlUnboundAfterPrepareReadbackFailure] = None,
    ownedModeExecutionFailure: Option[MySqlOwnedModeExecutionFailure] = None,
    driftExecutionFailure: Option[MySqlTrackedDriftExecutionFailure] = None,
    retainedAllocation: Option[MySqlRetainedAllocationObservation] = None)

/** 唯一实际worker装配；Main从未收到Connection/session/view/function或游戏对象。
  * 所有constructor refs先于OwnedSerialIo分配；不存在command时worker不能initialize数据库。
  */
final class MySqlOwnedIoAdapter(capacity: Int, initialMain: Thread, workerName: String, val identity: MySqlOwnedIoIdentity,
                               enableAdmissionMaintenance: Boolean = false):
  require(identity != null, "owned database identity required")
  private final case class Work(tag: IoOperationTag, command: MySqlOwnedIoCommand, repositoryEpochAtSubmission: Long, saveBarrier: Option[com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#Mutation] = None)
  private val ordinarySaveAdmission = new AtomicReference[com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission]()
  private[floatingpod] def installOrdinarySaveAdmission(owner: com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && owner != null,
      "exact claimed Main and save owner required")
    require(ordinarySaveAdmission.compareAndSet(null, owner), "ordinary save admission is installed once")
  private enum SessionSlot:
    case Legacy(value: MySqlOwnedDatabaseSession)
    case DeploymentReceipt(value: MySqlDeploymentReceiptSession)
    case DbFirst(value: MySqlDbFirstSession)
  private val sessionRef = new AtomicReference[SessionSlot]()
  private val dbFirstInputsRef = new AtomicReference[MySqlDbFirstSessionInputs]()
  private val managedBootstrapRequestRef = new AtomicReference[MySqlManagedBootstrapRequest]()
  private var dbFirstStage: String = null
  private var dbFirstFatal: Throwable = null
  private val deploymentInputsRef = new AtomicReference[MySqlDeploymentReceiptSessionInputs]()
  private val deploymentFailedExitRef = new AtomicReference[MySqlDeploymentReceiptSession.FailedInitializationExit]()
  private val deploymentTerminalSettlementRef = new AtomicReference[MySqlDeploymentReceiptSession.NormalTerminalSettlement5]()
  private var deploymentStage: String = null
  private var deploymentFatal: Throwable = null
  private def installLegacySession(value: MySqlOwnedDatabaseSession): Unit =
    require(value != null && sessionRef.compareAndSet(null, SessionSlot.Legacy(value)), "exactly one owned Session may install")
  private def installDeploymentSession(value: MySqlDeploymentReceiptSession): Unit =
    require(value != null && sessionRef.compareAndSet(null, SessionSlot.DeploymentReceipt(value)), "exactly one owned Session may install")
  private def legacySessionOrNull(): MySqlOwnedDatabaseSession = sessionRef.get() match
    case null => null
    case SessionSlot.Legacy(value) => value
    case SessionSlot.DeploymentReceipt(_) => throw new IllegalStateException("schema5 cannot use legacy Session")
    case SessionSlot.DbFirst(_) => throw new IllegalStateException("schema6 observation cannot use legacy Session")
  private def requiredDeploymentSession(): MySqlDeploymentReceiptSession = sessionRef.get() match
    case SessionSlot.DeploymentReceipt(value) => value
    case _ => throw new IllegalStateException("exact schema5 Session unavailable")
  // 这些固定引用槽随Adapter存在；worker bootstrap的ledger/root对象不倒收为root自己分配。
  private val v3LedgerRef=new AtomicReference[MySqlFormat3AllocationLedger]()
  private val v3RootRef=new AtomicReference[MySqlFormat3ReadBudget]()
  private val v3AssemblyRef=new AtomicReference[MySqlRuntimeInputs.AssemblyOwner]()
  private val v3PendingLeaseRef=new AtomicReference[MySqlRuntimeInputs.ConsumerLease]()
  private val freshCompletionHandoff = new AtomicReference[MySqlOwnedIoAdapter.FreshCompletionHandoff | com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded]()
  @volatile private var originalClaimedOwner=Option.empty[(Thread,IoOwnerTransfer)]
  private val dbFirstMainReadRef=new AtomicReference[MySqlDbFirstMainReadBacking]()
  private val dbFirstPendingMainReadRef=new AtomicReference[MySqlDbFirstMainReadBacking]()
  private var dbFirstReadCallbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks=null
  private[floatingpod] def installDbFirstReadCallbacks(value:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && value!=null && (value.adapter eq this) &&
      dbFirstReadCallbacks==null && ordinarySaveAdmission.get()!=null,"original claimed schema6 callback registration required")
    dbFirstReadCallbacks=value
  private def prepareDbFirstMainReadRelease(tag:IoOperationTag):Unit =
    val next=dbFirstPendingMainReadRef.get()
    if next!=null then
      require(dbFirstReadCallbacks!=null && next.repositoryEpoch==acceptedRepositoryEpoch,
        "original callback and exact accepted successor epoch required")
      val previous=dbFirstMainReadRef.get()
      if previous ne next then
        next.requireSuccessorFor(previous,tag)
        dbFirstReadCallbacks.detachSupersededMainRead(previous)
        require(dbFirstMainReadRef.compareAndSet(previous,next),"original Main backing changed during replacement")
        val (main,transfer)=originalClaimedOwner.getOrElse(throw new IllegalStateException("original Main transfer missing"))
        next.claimMain(main,transfer)
        dbFirstReadCallbacks.attachSuccessorMainRead(next)
  private[floatingpod] def requireCurrentDbFirstSaveSuccessor(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,tag:IoOperationTag):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && (dbFirstReadCallbacks eq callbacks),
      "original Main/save callback required")
    val next=dbFirstPendingMainReadRef.get()
    require(next!=null && (dbFirstMainReadRef.get() eq next),"original published successor required")
    next.requireRetiredSuccessor(tag)

  private var dbFirstApprovedControl: ProductionBootstrapControl.DbFirstControl = null // only original worker
  private val retainedPlanIssuerRef = new AtomicReference[MySqlOwnedRetainedPlanIssuer]()
  private val ioRef = new AtomicReference[OwnedSerialIo[Work, MySqlOwnedIoReply]]()
  private val workerTerminationProofIssuer = new Object
  private val resourceObservation = new AtomicReference[MySqlOwnedIoResourceObservation]()
  private val controlledAfterAckFailure = new AtomicReference[MySqlUnboundAfterCommitReadbackFailure]()
  private val preparedAfterAckFailure = new AtomicReference[MySqlUnboundAfterPrepareReadbackFailure]()
  private val ownedModeExecutionFailure = new AtomicReference[MySqlOwnedModeExecutionFailure]()
  private val completionRouter = new MySqlTrackedDriftCompletionRouter(identity, capacity)
  private var currentMainOwner = Option(initialMain)
  private val driftExecutionFailure = new AtomicReference[MySqlTrackedDriftExecutionFailure]()
  private val driftProjectionReleaseTags = ConcurrentHashMap.newKeySet[IoOperationTag]()
  private val completedDriftProjectionReleaseTags = mutable.LinkedHashSet.empty[IoOperationTag]
  private val modeProjectionReleaseTags = ConcurrentHashMap.newKeySet[IoOperationTag]()
  private val completedModeProjectionReleaseTags = mutable.LinkedHashSet.empty[IoOperationTag]
  private val preparationSignal = new CountDownLatch(1)
  private val maintenanceMailbox = new MySqlOwnedMaintenanceMailbox(identity)
  private val recallMaintenanceMailbox = new MySqlOwnedMaintenanceMailbox(identity)
  private val dbFirstMaintenanceMailbox = new MySqlOwnedMaintenanceMailbox(identity)
  private val retainedDbFirstRecallRef = new AtomicReference[MySqlDbFirstSession.FailedRecallCleanupQualification]()
  private val retiredDbFirstRecallHoldRef = new AtomicReference[MySqlDbFirstRecallDropReceipt]()
  // 只由真实Main ACK/路由消费后写入；worker不跨线程窥读Main私有router。
  private val retiredRecallReleaseAckRef=new AtomicReference[MySqlDbFirstRecallDropReceipt]()
  private val retiredRecallProducerDrainRef=new AtomicReference[MySqlDbFirstRecallDropReceipt]()
  // 只保管真实新Main DATA/mailbox的actual Recovery；此登记不改变current backing/普通身份。
  private enum RecoveredServiceMainStage:
    case Prepared, PreviousDropped, Registered, Claimed, Attached, Acknowledged, WorkerRetiring, WorkerRetired, ServiceRegistered, ServiceOwned, Closing, Failed
  /** 真实预付控制责任；只在实际worker装配完成后发布，Main不收到Session/JDBC。 */
  private final class RecoveredServiceMainTransfer(
      var source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      var previous:MySqlDbFirstMainReadBacking,val backing:MySqlDbFirstMainReadBacking,
      val mailbox:MySqlOwnedMaintenanceMailbox,val nextIdentity:MySqlOwnedIoIdentity,
      val receipt:MySqlDbFirstRecallDropReceipt,val epoch:Long):
    val stage=new AtomicReference[RecoveredServiceMainStage](RecoveredServiceMainStage.Prepared)
    val failure=new AtomicReference[Throwable]()
    var service:MySqlDbFirstSession=null // 同一预付control持有实际runtime owner；不向Main暴露。
    var predecessor:MySqlDbFirstSession=null
    val cleanupFailure=new AtomicReference[Throwable]()
    val workerCleanupComplete=new AtomicBoolean(false)
    var publicationDispatch:com.wi.floatingpod.server.ActiveFloatingPodManager#MainPublicationScope=null
    var publicationAcknowledged=false
    var executionDispatch=false
    var originalCommitted=false
    val executionPublished=new AtomicBoolean(false)
    val businessReleased=new AtomicBoolean(false)
  private val recoveredDbFirstServiceRef=new AtomicReference[RecoveredServiceMainTransfer]()
  // 只导航同一已收费census；Session转管不改变Main持有的原claim，也不重claim。
  private val claimedBootCensusRef=new AtomicReference[MySqlDbFirstBootRecallCensus]()
  private val recoveredMainStopAcknowledged=new AtomicBoolean(false)
  private val recoveredMainDataDropped=new AtomicBoolean(false)
  private val retainedServiceFailureRef=new AtomicReference[Throwable]()
  @volatile private var adapterStopping=false
  /** 构造identity继续证明同一IO/Boot历史；业务只消费私有发布后的实际执行代。 */
  private[floatingpod] def currentExecutionIdentity:MySqlOwnedIoIdentity =
    val actual=recoveredDbFirstServiceRef.get()
    if actual!=null && actual.executionPublished.get() then actual.nextIdentity else identity
  private def blockingRetiredRecall:MySqlDbFirstRecallDropReceipt =
    val hold=retiredDbFirstRecallHoldRef.get()
    val actual=recoveredDbFirstServiceRef.get()
    if hold!=null && actual!=null && (actual.receipt eq hold) && actual.businessReleased.get() &&
        !adapterStopping && actual.stage.get()==RecoveredServiceMainStage.ServiceOwned &&
        actual.failure.get()==null && actual.cleanupFailure.get()==null && actual.mailbox.poll.isCurrent then null else hold
  private def currentDbFirstMailbox:MySqlOwnedMaintenanceMailbox =
    val actual=recoveredDbFirstServiceRef.get()
    if actual!=null && actual.executionPublished.get() then actual.mailbox else dbFirstMaintenanceMailbox
  private def stopRecoveredMailbox():Unit =
    val actual=recoveredDbFirstServiceRef.get()
    if actual!=null then actual.mailbox.stop()

  private val recallRecoveryLink = new AtomicReference[MySqlOwnedIoAdapter.RecallRecoveryLink]()
  private final case class RestrictedSourceRegistration(
      projection:com.wi.floatingpod.server.MainBootProjection,
      lease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      server:net.minecraft.server.MinecraftServer,
      reply:MySqlProductionBootReply,
      link:MySqlOwnedIoAdapter.RecallRecoveryLink,
      root:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      custody:com.wi.floatingpod.server.NormalRecallRecoverySourceCustody,
      profile:SourceLimitProfile,
      originalEpoch:Long,
      sourceProtectionData:MySqlBootSourceProtectionIndex.RetainedData)
  private enum RestrictedSourcePlanState:
    case Submitting, Accepted, CompletionRetained, Acknowledging
    case Rejected(value: IoSubmission)
    case Unknown(cause: Throwable)
    case Failed(cause: Throwable)
    case Issued
  private enum RestrictedSourceR0State:
    case Submitting, Accepted, CompletionRetained
    case Rejected(value:IoSubmission)
    case SubmissionUnknown(cause:Throwable)
    case HandoffBookkeepingFailure(value:IoSubmission,cause:Throwable)
  private final class RestrictedSourceR0Pending(
      val tag:IoOperationTag,
      val command:MySqlOwnedIoCommand,
      val backing:MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking,
      val planAck:MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
      val plan:MySqlFencedTransaction.SourceReservePlan5,
      val expectedSourceEpoch:Long,
      val nextAcceptedEpoch:Long,
      val protocolChargeRoot:MySqlFormat3ReadBudget,
      val preparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue):
    @volatile private var currentState=RestrictedSourceR0State.Submitting
    @volatile private var retainedCompletion:IoCompletion[MySqlOwnedIoReply]=null
    @volatile private var retainedRoutingFailure:Throwable=null
    @volatile private var retainedRoutingSecondaryFailure:Throwable=null
    private var exactWork:Work=null
    def state:RestrictedSourceR0State=currentState
    def completion:Option[IoCompletion[MySqlOwnedIoReply]]=Option(retainedCompletion)
    def routingFailure:Throwable=retainedRoutingFailure
    def routingSecondaryFailure:Throwable=retainedRoutingSecondaryFailure
    def retainRoutingFailure(cause:Throwable):Unit=synchronized {
      require(cause!=null,"original completion-routing failure required")
      if retainedRoutingFailure==null then retainedRoutingFailure=cause
      else if (retainedRoutingFailure ne cause) && retainedRoutingSecondaryFailure==null then
        retainedRoutingSecondaryFailure=cause
    }
    def retainRoutingSecondaryFailure(cause:Throwable):Unit=synchronized {
      require(cause!=null,"secondary completion-routing failure required")
      if retainedRoutingFailure==null then retainedRoutingFailure=cause
      else if (retainedRoutingFailure ne cause) && retainedRoutingSecondaryFailure==null then
        retainedRoutingSecondaryFailure=cause
    }
    def installWork(value:Work):Unit=synchronized {
      require(currentState==RestrictedSourceR0State.Submitting && value!=null && exactWork==null &&
        value.tag==tag && (value.command eq command) && value.repositoryEpochAtSubmission==expectedSourceEpoch,
        "exact restricted SOURCE R0 accepted Work must bind its original command/tag/epoch")
      exactWork=value
    }
    def requireWorkerWork(value:Work):Unit=synchronized {
      require(value!=null && (exactWork eq value) && value.tag==tag && (value.command eq command) &&
        value.repositoryEpochAtSubmission==expectedSourceEpoch,
        "restricted SOURCE R0 worker must retain the exact accepted Work and command")
      preparation.requireWorkerEntry(this,value)
      currentState match
        case RestrictedSourceR0State.Submitting | RestrictedSourceR0State.Accepted |
            RestrictedSourceR0State.SubmissionUnknown(_) => ()
        case RestrictedSourceR0State.HandoffBookkeepingFailure(IoSubmission.Accepted,_)=>()
        case _=>throw new IllegalStateException("restricted SOURCE R0 worker reached an unsubmitted or settled owner")
    }
    def setSubmission(value:IoSubmission):Unit=synchronized {
      require(currentState==RestrictedSourceR0State.Submitting && value!=null,
        "restricted SOURCE R0 submission outcome is recorded once")
      currentState=value match
        case IoSubmission.Accepted=>
          if retainedCompletion==null then RestrictedSourceR0State.Accepted
          else RestrictedSourceR0State.CompletionRetained
        case rejected=>
          require(retainedCompletion==null,
            "actual rejected R0 submission cannot own a worker completion")
          RestrictedSourceR0State.Rejected(rejected)
    }
    def setUnknown(cause:Throwable):Unit=synchronized {
      require(currentState==RestrictedSourceR0State.Submitting && cause!=null,
        "restricted SOURCE R0 submission UNKNOWN retains the exact original owner")
      currentState=RestrictedSourceR0State.SubmissionUnknown(cause)
    }
    def setHandoffBookkeepingFailure(value:IoSubmission,cause:Throwable):Unit=synchronized {
      require(preparation.submissionReturned && cause!=null && preparation.rawSubmission==value,
        "known restricted SOURCE R0 submission result and bookkeeping cause are required")
      currentState=RestrictedSourceR0State.HandoffBookkeepingFailure(value,cause)
    }
    def retainCompletion(value:IoCompletion[MySqlOwnedIoReply]):Unit=synchronized {
      require(value!=null && value.tag==tag && retainedCompletion==null && preparation.mayRetainCompletion,
        "only the exact restricted SOURCE R0 completion may be retained")
      currentState match
        case RestrictedSourceR0State.Accepted=>
          retainedCompletion=value
          currentState=RestrictedSourceR0State.CompletionRetained
        case RestrictedSourceR0State.SubmissionUnknown(_) => retainedCompletion=value
        case RestrictedSourceR0State.HandoffBookkeepingFailure(IoSubmission.Accepted,_)=>retainedCompletion=value
        case RestrictedSourceR0State.Submitting=>retainedCompletion=value
        case _=>throw new IllegalStateException("restricted SOURCE R0 completion arrived without its accepted/unknown route")
    }
    def needsCompletionSlot:Boolean=preparation.needsCompletionSlot(retainedCompletion==null)
    def protocolRoot:MySqlFormat3ReadBudget=protocolChargeRoot

  private final class RestrictedSourcePlanPending(
      val tag:IoOperationTag,
      val command:MySqlOwnedIoCommand,
      val intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      barrier:com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier):
    private var pendingBarrier=barrier
    @volatile private var currentState=RestrictedSourcePlanState.Submitting
    @volatile private var retainedCompletion:IoCompletion[MySqlOwnedIoReply]=null
    private var successAck:MySqlOwnedIoAdapter.RestrictedSourcePlanAck=null
    private var rejectionAck:MySqlOwnedIoAdapter.RestrictedSourcePlanRejectionAck=null
    private var failureAck:MySqlOwnedIoAdapter.RestrictedSourcePlanFailureAck=null
    def state:RestrictedSourcePlanState=currentState
    def completion:Option[IoCompletion[MySqlOwnedIoReply]]=Option(retainedCompletion)
    def setSubmission(result:IoSubmission):Unit=synchronized {
      require(result!=null && currentState==RestrictedSourcePlanState.Submitting,
        "restricted source submission outcome is recorded once")
      currentState=result match
        case IoSubmission.Accepted=>RestrictedSourcePlanState.Accepted
        case rejected=>RestrictedSourcePlanState.Rejected(rejected)
    }
    def setUnknown(cause:Throwable):Unit=synchronized {
      require(cause!=null && currentState==RestrictedSourcePlanState.Submitting,
        "restricted source UNKNOWN retains its original submitting responsibility")
      currentState=RestrictedSourcePlanState.Unknown(cause)
    }
    def retainCompletion(value:IoCompletion[MySqlOwnedIoReply]):Unit=synchronized {
      val completionMayArrive=currentState==RestrictedSourcePlanState.Accepted ||
        (currentState match
          case RestrictedSourcePlanState.Unknown(_) => true
          case _ => false)
      require(value!=null && value.tag==tag && retainedCompletion==null && completionMayArrive,
        "only the exact accepted restricted source completion may be retained")
      retainedCompletion=value
      currentState=RestrictedSourcePlanState.CompletionRetained
    }
    def readyAck(adapter:MySqlOwnedIoAdapter):MySqlOwnedIoAdapter.RestrictedSourcePlanAck=synchronized {
      if successAck==null then
        val completionValue=Option(retainedCompletion).getOrElse(throw new IllegalStateException("source plan completion is not retained"))
        completionValue.result match
          case Success(MySqlOwnedIoReply.RestrictedSourceReservePlanPrepared(owned)) =>
            successAck=new MySqlOwnedIoAdapter.RestrictedSourcePlanAck(adapter,tag,command,intent,completionValue,owned)
          case _=>throw new IllegalStateException("restricted source plan success ACK requires its exact typed reply")
      successAck
    }
    def rejectionAckFor(adapter:MySqlOwnedIoAdapter,value:IoSubmission):MySqlOwnedIoAdapter.RestrictedSourcePlanRejectionAck=synchronized {
      require(currentState==RestrictedSourcePlanState.Rejected(value),"exact restricted source rejection is not pending")
      if rejectionAck==null then rejectionAck=new MySqlOwnedIoAdapter.RestrictedSourcePlanRejectionAck(adapter,tag,command,intent,value)
      rejectionAck
    }
    def failureAckFor(adapter:MySqlOwnedIoAdapter,cause:Throwable):MySqlOwnedIoAdapter.RestrictedSourcePlanFailureAck=synchronized {
      val firstFailureAck=currentState==RestrictedSourcePlanState.CompletionRetained && retainedCompletion!=null
      val sameAcknowledgedFailure=currentState match
        case RestrictedSourcePlanState.Failed(previous)=>
          (previous eq cause) && failureAck!=null && (failureAck.cause eq cause) && retainedCompletion!=null
        case _=>false
      require(cause!=null && (firstFailureAck || sameAcknowledgedFailure),
        "exact settled restricted source failure completion is required")
      if failureAck==null then failureAck=new MySqlOwnedIoAdapter.RestrictedSourcePlanFailureAck(adapter,tag,command,intent,retainedCompletion,cause)
      failureAck
    }
    def beginSuccessAck(ack:MySqlOwnedIoAdapter.RestrictedSourcePlanAck):Unit=synchronized {
      require((successAck eq ack) && currentState==RestrictedSourcePlanState.CompletionRetained && retainedCompletion!=null,
        "exact restricted source completion ACK is one-shot")
      currentState=RestrictedSourcePlanState.Acknowledging
    }
    def barrierForLeaseTransfer:com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier=synchronized {
      require(currentState==RestrictedSourcePlanState.Acknowledging && pendingBarrier!=null,
        "exact live restricted source barrier is required for ACK ownership transfer")
      pendingBarrier
    }
    def relinquishTransferredBarrier(exactBarrier:com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#SourceScopedBarrier):Unit=synchronized {
      require(currentState==RestrictedSourcePlanState.Acknowledging && exactBarrier!=null &&
        (pendingBarrier eq exactBarrier),
        "only the exact barrier retained by the ACKed lease closure may leave the pending slot")
      pendingBarrier=null
    }
    /** Close only an untransferred barrier after a definitive settled outcome. */
    def closeUntransferredBarrier():Unit =
      val exactBarrier=synchronized {
        require(pendingBarrier!=null,"restricted source pending slot no longer owns a closeable barrier")
        pendingBarrier
      }
      exactBarrier.close()
      synchronized {
        require(pendingBarrier eq exactBarrier,"restricted source barrier ownership changed during settlement")
        pendingBarrier=null
      }
    def markFailed(cause:Throwable):Unit=synchronized {
      require(cause!=null, "original restricted source failure cause required")
      currentState=RestrictedSourcePlanState.Failed(cause)
    }
    def markIssued():Unit=synchronized {
      require(currentState==RestrictedSourcePlanState.Acknowledging,"exact plan must finish ACK before issue")
      currentState=RestrictedSourcePlanState.Issued
    }
    def isAckInProgress:Boolean=currentState==RestrictedSourcePlanState.Acknowledging && retainedCompletion!=null
  private enum RestrictedSourcePlanDropState:
    case ReadyToSubmit, Submitting, Accepted, CompletionRetained, Acknowledged
    case Rejected(value:IoSubmission)
    case Unknown(cause:Throwable)
    case Failed(cause:Throwable)
  private final class RestrictedSourcePlanDropPending(
      val tag:IoOperationTag,
      val proof:com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked,
      val registrationIdentity:AnyRef,
      var intentOwner:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      var planAckOwner:MySqlOwnedIoAdapter.RestrictedSourcePlanAck,
      var backingOwner:MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking,
      var overlayOwner:com.wi.floatingpod.server.MainBootProjection.SourceChunkEffectClosure,
      var planOwner:MySqlFencedTransaction.SourceReservePlan5,
      var protocolCharge:AutoCloseable):
    private var registrationOwner:MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration=null
    private var receiptOwner:MySqlFencedTransaction.SourceReservePlanDropReceipt=null
    private var commandOwner:MySqlOwnedIoCommand=null
    @volatile private var currentState=RestrictedSourcePlanDropState.ReadyToSubmit
    @volatile private var retainedCompletion:IoCompletion[MySqlOwnedIoReply]=null
    private var successAck:MySqlFencedTransaction.SourceReservePlanDropAck=null
    def state:RestrictedSourcePlanDropState=currentState
    def receipt:MySqlFencedTransaction.SourceReservePlanDropReceipt=receiptOwner
    def command:MySqlOwnedIoCommand=commandOwner
    def registration:MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration=registrationOwner
    def completion:Option[IoCompletion[MySqlOwnedIoReply]]=Option(retainedCompletion)
    def transferProtocolCharge():AutoCloseable=synchronized {
      val exact=Option(protocolCharge).getOrElse(
        throw new IllegalStateException("charged source-plan drop protocol envelope is not retained"))
      protocolCharge=null
      exact
    }
    def beginAttempt():Boolean=synchronized {
      currentState match
        case RestrictedSourcePlanDropState.ReadyToSubmit | RestrictedSourcePlanDropState.Rejected(IoSubmission.RejectedCapacity)=>
          currentState=RestrictedSourcePlanDropState.Submitting
          true
        case _=>false
    }
    def installRegistration(value:MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.Submitting && value!=null && registrationOwner==null,
        "one exact adapter-issued source-plan drop registration required")
      registrationOwner=value
    }
    def installReceipt(exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.Submitting && exactReceipt!=null,
        "exact Fenced source-plan drop receipt required before worker submission")
      if receiptOwner==null then receiptOwner=exactReceipt
      else require(receiptOwner eq exactReceipt,"source-plan drop retry must reuse the original Fenced receipt")
      if commandOwner==null then commandOwner=MySqlOwnedIoCommand.DropRestrictedSourceReservePlan(exactReceipt)
      clearPlanAnchors()
    }
    private def clearPlanAnchors():Unit =
      intentOwner=null
      planAckOwner=null
      backingOwner=null
      overlayOwner=null
      planOwner=null
    def failBeforeQueue(cause:Throwable):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.Submitting && cause!=null,
        "exact source-plan receipt/submission failure required")
      currentState=RestrictedSourcePlanDropState.Failed(cause)
    }
    def setSubmission(result:IoSubmission):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.Submitting && result!=null,
        "source-plan drop submission outcome is recorded once")
      currentState=result match
        case IoSubmission.Accepted=>RestrictedSourcePlanDropState.Accepted
        case rejected=>RestrictedSourcePlanDropState.Rejected(rejected)
    }
    def setUnknown(cause:Throwable):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.Submitting && cause!=null,
        "original source-plan drop submission uncertainty required")
      currentState=RestrictedSourcePlanDropState.Unknown(cause)
    }
    def retainCompletion(value:IoCompletion[MySqlOwnedIoReply]):Unit=synchronized {
      require(value!=null && value.tag==tag && retainedCompletion==null,
        "only the exact source-plan drop completion may be retained")
      currentState match
        case RestrictedSourcePlanDropState.Accepted=>
          retainedCompletion=value
          currentState=RestrictedSourcePlanDropState.CompletionRetained
        case RestrictedSourcePlanDropState.Unknown(_) => retainedCompletion=value // UNKNOWN始终保留原责任，不能被升级为成功。
        case _=>throw new IllegalStateException("source-plan drop completion arrived without its exact accepted route")
    }
    def readyAck():MySqlFencedTransaction.SourceReservePlanDropAck=synchronized {
      val completed=Option(retainedCompletion).getOrElse(
        throw new IllegalStateException("source-plan drop completion is not retained"))
      completed.result match
        case Success(MySqlOwnedIoReply.RestrictedSourceReservePlanDropped(ack))=>
          require(ack!=null,"typed Fenced source-plan drop ACK required")
          if successAck==null then successAck=ack
          else require(successAck eq ack,"source-plan drop completion ACK identity changed")
          successAck
        case _=>throw new IllegalStateException("source-plan drop completion is not the exact typed Fenced ACK")
    }
    def markFailed(cause:Throwable):Unit=synchronized {
      require(cause!=null,"original source-plan drop failure cause required")
      currentState match
        case RestrictedSourcePlanDropState.Failed(original)=>
          if original ne cause then MySqlOwnedIoAdapter.suppressExitCleanup(original,cause)
        case _=>currentState=RestrictedSourcePlanDropState.Failed(cause)
    }
    def acknowledge(exactAck:MySqlFencedTransaction.SourceReservePlanDropAck):Unit=synchronized {
      require(currentState==RestrictedSourcePlanDropState.CompletionRetained &&
        successAck!=null && (successAck eq exactAck) && protocolCharge==null,
        "exact observed source-plan drop ACK and transferred protocol charge required")
      currentState=RestrictedSourcePlanDropState.Acknowledged
      retainedCompletion=null
      registrationOwner=null
      receiptOwner=null
      commandOwner=null
      clearPlanAnchors()
    }
  @volatile private var restrictedSourceRegistration=Option.empty[RestrictedSourceRegistration]
  private var restrictedSourceSettledEpoch=Option.empty[Long]
  private val restrictedSourcePlanPending=new AtomicReference[RestrictedSourcePlanPending]()
  private val restrictedSourceR0Pending=new AtomicReference[RestrictedSourceR0Pending]()
  // Adapter 原有字段是 P1 首个保留锚点；poll 可早于任一新 holder。
  @volatile private var restrictedSourceR0ChargeRoot:MySqlFormat3ReadBudget=null
  @volatile private var restrictedSourceR0ChargeAmount=0L
  @volatile private var restrictedSourceR0ChargeAttempted=false
  @volatile private var restrictedSourceR0ChargeReturned=false
  @volatile private var restrictedSourceR0AnchorStage:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage=null
  @volatile private var restrictedSourceR0AnchorCause:Throwable=null
  @volatile private var restrictedSourceR0AnchorSecondaryCause:Throwable=null
  @volatile private var restrictedSourceR0Preparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue=null
  @volatile private var restrictedSourcePlan=Option.empty[(MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
    MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking,
    com.wi.floatingpod.server.MainBootProjection.SourceChunkEffectClosure)]
  @volatile private var restrictedSourcePlanAck:MySqlOwnedIoAdapter.RestrictedSourcePlanAck=null
  private val restrictedSourcePlanDropPending=new AtomicReference[RestrictedSourcePlanDropPending]()
  @volatile private var completedRestrictedSourcePlanDropAck:MySqlFencedTransaction.SourceReservePlanDropAck=null
  @volatile private var completedRestrictedSourcePlanDropReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt=null
  @volatile private var completedRestrictedSourcePlanDropTag:IoOperationTag=null
  @volatile private var completedRestrictedSourcePlanDropRegistrationIdentity:AnyRef=null
  @volatile private var completedRestrictedSourcePlanDropCharge:AutoCloseable=null
  @volatile private var completedRestrictedSourcePlanDropChargeReleaseAttempted=false
  @volatile private var completedRestrictedSourcePlanDropChargeFailure:Throwable=null
  @volatile private var failedUnpublishedRestrictedSourcePlanDropCharge:AutoCloseable=null
  @volatile private var failedUnpublishedRestrictedSourcePlanDropChargeCause:Throwable=null
  @volatile private var restrictedSourceFailure:Throwable=null
  @volatile private var restrictedSourceUnexpectedRequestRead:Vector[IoCompletion[MySqlOwnedIoReply]]=null
  private val maintenance = new IoWorkerMaintenance:
    override def maintain(): IoMaintenanceDeadline = maintainResources()
  private var bootstrapPreparation = Option.empty[BootstrapPreparation]
  private var certifiedOrdinaryConnectionSourceForAssembly = Option.empty[() => VerifiedMySqlConnection]
  private var freshConnectionSourceForAssembly = Option.empty[() => VerifiedMySqlConnection]
  private var ownerSubmitted = false
  private var ordinarySubmissionAttempted = false
  private var acceptedRepositoryEpoch = 0L
  /** 只表示该 Boot 数据之后有潜在写入被真正接受，不是 DB version 或提交 ACK。 */
  private[floatingpod] def repositorySubmissionEpoch: Long =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "Boot data epoch requires exact current Main")
    acceptedRepositoryEpoch
  private var bootAttempted = false
  private var activeLimit = 0
  private val io = new OwnedSerialIo[Work, MySqlOwnedIoReply](capacity, initialMain, workerName,
    Some(() => exitResources()), Some(preparationSignal),
    if enableAdmissionMaintenance then Some(maintenance) else None)(execute)
  ioRef.set(io) // constructor返回前发布；无public submit前不能有command或verified session。

  private[floatingpod] def reserveDbFirstRecallJournal(tag:IoOperationTag,original:IoOperationTag,
      binding:UUID,actor:UUID,pod:UUID,version:Long,client:Option[UUID]):MySqlDbFirstRecallJournalReservation =
    requireDbFirstCaptureOwner()
    retainedPlanIssuerRef.get().reserveDbFirstRecallJournal(tag,original,binding,actor,pod,version,client)

  private[floatingpod] def reserveDbFirstInitialPreparation(tag:IoOperationTag,
      request:MySqlControlledDebugPrepareRequest,activeLimit:Int):MySqlDbFirstInitialPreparationReservation =
    requireDbFirstCaptureOwner()
    require(canReserveDbFirstInitialPreparation,"preparation requires two free responsibility slots and current reads")
    retainedPlanIssuerRef.get().reserveDbFirstInitialPreparation(tag,request,activeLimit)

  /** 新Prepare独占空闲入口，给其后续initial capture留下第二个Entry；不是提交或成功凭证。 */
  private[floatingpod] def canReserveDbFirstInitialPreparation:Boolean =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"preparation availability requires exact current Main")
    if retainedDbFirstRecallRef.get()!=null || blockingRetiredRecall!=null then return false
    requireDbFirstCaptureOwner()
    val checkpoint=io.checkpoint
    capacity>=2 && completionRouter.retainedCount==0 && checkpoint.reserved==0 &&
      !Option(ordinarySaveAdmission.get()).exists(_.awaitingOrdinarySuccessor()) &&
      retainedPlanIssuerRef.get().observation.plans.isEmpty

  /** 仅供原Main延后周期分配；不是reservation、SQL或成功凭证，真实分配/submit仍逐次核验。 */
  private[floatingpod] def canReserveDbFirstCapture:Boolean =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "capture availability requires exact current Main")
    // 已知共享故障只封新producer探测，不截断原owner的END/实际完成drain；false不是新资格。
    if retainedDbFirstRecallRef.get()!=null || blockingRetiredRecall!=null then return false
    requireDbFirstCaptureOwner()
    val checkpoint=io.checkpoint
    completionRouter.retainedCount<capacity && checkpoint.reserved<capacity &&
      !Option(ordinarySaveAdmission.get()).exists(_.awaitingOrdinarySuccessor()) &&
      retainedPlanIssuerRef.get().observation.plans.size<capacity

  private[floatingpod] def reserveDbFirstCapture(tag: IoOperationTag,
      purpose: DbFirstPodSnapshot.Purpose = DbFirstPodSnapshot.Purpose.RECALL): MySqlDbFirstCaptureReservation =
    requireDbFirstCaptureOwner()
    retainedPlanIssuerRef.get().reserveDbFirstCapture(tag, purpose)

  private[floatingpod] def retainRecoveredInitialData(reservation:MySqlDbFirstCaptureReservation,
      data:MySqlDbFirstInitialPreparationDataView):MySqlDbFirstInitialPreparationDataView =
    requireDbFirstCaptureOwner()
    retainedPlanIssuerRef.get().retainRecoveredInitialData(reservation,data)

  private[floatingpod] def requireDbFirstCaptureReservation(reservation: MySqlDbFirstCaptureReservation): Unit =
    requireDbFirstCaptureOwner()
    val issuer = retainedPlanIssuerRef.get()
    require(issuer != null, "capture issuer was retired")
    issuer.requireDbFirstCaptureReservation(reservation)

  private[floatingpod] def captureDbFirstSnapshot(reservation: MySqlDbFirstCaptureReservation)
      (capture: DbFirstCaptureAllocation => DbFirstPodSnapshot): DbFirstPodSnapshot =
    requireDbFirstCaptureOwner()
    retainedPlanIssuerRef.get().captureDbFirstSnapshot(reservation)(capture)

  private[floatingpod] def dbFirstInitialActivationRequest(reservation: MySqlDbFirstCaptureReservation,
      snapshot: DbFirstPodSnapshot): MySqlDbFirstInitialActivationRequest =
    requireDbFirstCaptureOwner()
    retainedPlanIssuerRef.get().dbFirstInitialActivationRequest(reservation,snapshot)

  private[floatingpod] def excludeRecallBeforeCapture(reservation:MySqlDbFirstRecallJournalReservation,
      cause:Throwable,reason:Option[RecallRefusalReason]=None):MySqlDbFirstRecallPreCaptureExclusion =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"original refusal Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(reservation!=null && issuer!=null && (reservation.handle.issuer eq issuer),"original refusal issuer required")
    issuer.excludeRecallBeforeCapture(reservation,cause,reason)

  /** source明确区分live J路由与Boot首次C路由；历史J身份不解释成当前路由。 */
  private[floatingpod] def claimRecallRefusalConfirmation(exclusion:MySqlDbFirstRecallRefusalSource,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstRecallRefusalConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"original refusal Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(exclusion!=null && completion!=null && completion.tag==exclusion.routeTag && issuer!=null &&
      (exclusion.handle.issuer eq issuer),"original refusal J route/issuer required")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observation)) if observation!=null &&
          observation.routeTag==exclusion.routeTag && observation.journalTag==exclusion.journalTag && observation.tag==exclusion.executionTag=>
        issuer.claimRecallRefusalConfirmation(exclusion,observation)
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("original refusal C result on exact J route required")

  /** 原J数据库事实只兑换其一次SQL确认，不允许转换成C的清理或capture资格。 */
  private[floatingpod] def claimDbFirstRecallJournalConfirmation(reservation:MySqlDbFirstRecallJournalReservation,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstRecallJournalConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"journal confirmation requires original Main")
    val issuer=retainedPlanIssuerRef.get()
    require(reservation!=null && completion!=null && completion.tag==reservation.tag && issuer!=null &&
      (reservation.handle.issuer eq issuer),"original journal issuer/reservation/completion required")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstRecallJournalObserved(observation))=>
        issuer.claimDbFirstRecallJournalConfirmation(reservation,observation)
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("original journal readback completion required")

  /** 只认原runner缓存的实际观察对象；相等DATA或hash不能兑换Main清理资格。 */
  private[floatingpod] def claimDbFirstRecallConfirmation(reservation: MySqlDbFirstCaptureReservation,
      completion: IoCompletion[MySqlOwnedIoReply]): MySqlDbFirstRecallConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first confirmation requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(reservation != null && completion != null && completion.tag == reservation.tag && issuer != null &&
      (reservation.handle.issuer eq issuer), "DB-first confirmation requires original capture, issuer and completion tag")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstRecallObserved(observation)) =>
        issuer.claimDbFirstRecallConfirmation(reservation, observation)
      case Success(MySqlOwnedIoReply.DbFirstRecallRetained(qualification)) =>
        throw issuer.retainedDbFirstRecallFailure(reservation,qualification)
      case Failure(cause) => throw cause
      case _ => throw new IllegalArgumentException("DB-first confirmation requires its original recall observation")

  /** 显式领取原操作受限确认；正常claimer仍将Retained报告为原失败。 */
  private[floatingpod] def claimRetainedDbFirstRecallConfirmation(reservation:MySqlDbFirstCaptureReservation,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstRecallConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"retained confirmation requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(reservation!=null && completion!=null && completion.tag==reservation.tag && issuer!=null &&
      (reservation.handle.issuer eq issuer),"retained confirmation requires original reservation/issuer/completion")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstRecallRetained(qualification))=>
        require(retainedDbFirstRecallRef.get() eq qualification,"original adapter retained qualification required")
        issuer.claimRetainedDbFirstRecallConfirmation(reservation,qualification)
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("only actual original Retained completion may claim restricted confirmation")

  private[floatingpod] def claimDbFirstCheckpointConfirmation(reservation: MySqlDbFirstCaptureReservation,
      completion: IoCompletion[MySqlOwnedIoReply]): MySqlDbFirstCheckpointConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first confirmation requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(reservation != null && completion != null && completion.tag == reservation.tag && issuer != null &&
      (reservation.handle.issuer eq issuer), "DB-first confirmation requires original capture, issuer and completion tag")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstCheckpointObserved(observation)) =>
        issuer.claimDbFirstCheckpointConfirmation(reservation, observation)
      case Failure(cause) => throw cause
      case _ => throw new IllegalArgumentException("DB-first confirmation requires its original checkpoint observation")

  private[floatingpod] def claimDbFirstInitialPreparationConfirmation(reservation: MySqlDbFirstInitialPreparationReservation,
      completion: IoCompletion[MySqlOwnedIoReply]): MySqlDbFirstInitialPreparationConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first confirmation requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(reservation != null && completion != null && completion.tag == reservation.tag && issuer != null &&
      (reservation.handle.issuer eq issuer), "DB-first confirmation requires original capture, issuer and completion tag")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstInitialPreparationObserved(observation)) =>
        issuer.claimDbFirstInitialPreparationConfirmation(reservation, observation)
      case Success(MySqlOwnedIoReply.DbFirstInitialPreparationRejected(observation)) =>
        require(observation!=null && observation.tag==reservation.tag && observation.podUuid==reservation.request.podUuid &&
          observation.cause!=null,"original preparation rejection identity required")
        throw observation.cause
      case Failure(cause) => throw cause
      case _ => throw new IllegalArgumentException("DB-first confirmation requires its original preparation observation")

  private[floatingpod] def claimPreparationRejection(reservation:MySqlDbFirstInitialPreparationReservation,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstPreparationRejectionConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"original rejection Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(reservation!=null && completion!=null && completion.tag==reservation.tag && issuer!=null &&
      (reservation.handle.issuer eq issuer),"original rejection reservation/issuer/tag required")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstInitialPreparationRejected(observation))=>
        issuer.claimPreparationRejection(reservation,observation)
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("actual rejected preparation completion required")

  private[floatingpod] def claimDbFirstInitialActivationConfirmation(reservation: MySqlDbFirstCaptureReservation,
      completion: IoCompletion[MySqlOwnedIoReply]): MySqlDbFirstInitialActivationConfirmation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first confirmation requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(reservation != null && completion != null && completion.tag == reservation.tag && issuer != null &&
      (reservation.handle.issuer eq issuer), "DB-first confirmation requires original capture, issuer and completion tag")
    currentExecutionIdentity.requireTag(completion.tag)
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstInitialActivationObserved(observation)) =>
        issuer.claimDbFirstInitialActivationConfirmation(reservation, observation)
      case Failure(cause) => throw cause
      case _ => throw new IllegalArgumentException("DB-first confirmation requires its original recall observation")

  /** 原Main初建发布只借本次pending DATA；不能使旧read epoch重新有效。 */
  private[floatingpod] def initialPublicationData(reservation:MySqlDbFirstCaptureReservation,
      confirmation:MySqlDbFirstInitialActivationConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlBootMainReadData =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"initial publication DATA requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(reservation!=null && confirmation!=null && completion!=null && issuer!=null &&
      (reservation.handle.issuer eq issuer) && (confirmation.handle.issuer eq issuer) &&
      completion.tag==reservation.tag && confirmation.tag==reservation.tag,
      "original initial publication reservation/confirmation/completion required")
    confirmation.requireOriginal(reservation)
    val next=dbFirstPendingMainReadRef.get()
    require(next!=null && dbFirstReadCallbacks!=null && next.repositoryEpoch==acceptedRepositoryEpoch,
      "original initial pending successor and callback required")
    completion.result match
      case Success(MySqlOwnedIoReply.DbFirstInitialActivationObserved(observation))=>
        next.initialPublicationData(this,dbFirstMainReadRef.get(),observation)
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("original initial completion required")

  private[floatingpod] def recordDbFirstMainDrop(confirmation: MySqlDbFirstRecallConfirmation): MySqlDbFirstRecallDropReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first Main detach requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(confirmation != null && issuer != null && (confirmation.handle.issuer eq issuer),
      "DB-first detach requires its original confirmation issuer")
    issuer.recordDbFirstMainDrop(confirmation)

  private[floatingpod] def recordRetainedDbFirstMainDrop(confirmation:MySqlDbFirstRecallConfirmation,
      removal:ExactBodyRemovalAcknowledgement,ticket:TicketReleaseAcknowledgement):MySqlDbFirstRecallDropReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"retained drop requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(confirmation!=null && confirmation.isRetained && issuer!=null &&
      (confirmation.handle.issuer eq issuer),"original retained confirmation issuer required")
    issuer.recordRetainedDbFirstMainDrop(confirmation,removal,ticket)

  private def requireDbFirstDropReceipt(receipt: MySqlDbFirstRecallDropReceipt): Unit =
    val issuer = retainedPlanIssuerRef.get()
    require(receipt != null && !receipt.isRetained && issuer != null && (receipt.handle.issuer eq issuer),
      "ordinary DB-first release requires its original non-retained Main drop receipt")
    currentExecutionIdentity.requireTag(receipt.tag)

  private[floatingpod] def submitRejectedPreparationReadRelease(reservation:MySqlDbFirstInitialPreparationReservation,
      confirmation:MySqlDbFirstPreparationRejectionConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original rejection Main required")
    confirmation.requireOriginal(reservation)
    require(confirmation.handle.issuer eq retainedPlanIssuerRef.get(),"original rejection issuer required")
    prepareDbFirstMainReadRelease(confirmation.tag)
    submit(confirmation.tag,MySqlOwnedIoCommand.ReleaseRejectedPreparationMainRead(confirmation))

  private[floatingpod] def finishRejectedPreparationReadRelease(reservation:MySqlDbFirstInitialPreparationReservation,
      confirmation:MySqlDbFirstPreparationRejectionConfirmation,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original rejection Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original rejection issuer required")
    issuer.requireRejectedPreparationReadReleased(confirmation,reservation)
    completionRouter.requireRejectedPreparationReadAck(confirmation,completion)
    requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,confirmation.tag)
    dbFirstReadCallbacks.resumeOrdinarySuccessor(confirmation.tag)
    require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original rejected successor changed")
    completionRouter.finishRejectedPreparationReadAck(confirmation,completion)

  private[floatingpod] def detachRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null &&
      (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original rejection Main/issuer required")
    retainedPlanIssuerRef.get().detachRejectedPreparation(confirmation)

  private[floatingpod] def submitRejectedPreparationRetirement(confirmation:MySqlDbFirstPreparationRejectionConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original rejection Main required")
    submit(confirmation.tag,MySqlOwnedIoCommand.RetireRejectedPreparation(confirmation))

  private[floatingpod] def finishRejectedPreparationRetirement(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original rejection Main required")
    completionRouter.finishRejectedPreparationRetirement(confirmation,completion)

  private[floatingpod] def submitRefusalMainReadRelease(exclusion:MySqlDbFirstRecallRefusalSource,
      confirmation:MySqlDbFirstRecallRefusalConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original refusal Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original refusal issuer required")
    issuer.requireRecallRefusalConfirmation(confirmation,exclusion)
    prepareDbFirstMainReadRelease(confirmation.tag)
    submit(confirmation.tag,MySqlOwnedIoCommand.ReleaseRefusalMainRead(confirmation))

  private[floatingpod] def finishRefusalMainReadRelease(exclusion:MySqlDbFirstRecallRefusalSource,
      confirmation:MySqlDbFirstRecallRefusalConfirmation,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original refusal Main ACK required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original refusal issuer required")
    issuer.requireRefusalReadReleased(confirmation,exclusion)
    completionRouter.requireRefusalReadAck(confirmation,completion)
    requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,confirmation.tag)
    dbFirstReadCallbacks.resumeOrdinarySuccessor(confirmation.tag)
    require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original refusal successor changed")
    completionRouter.finishRefusalReadAck(confirmation,completion)
    issuer.acknowledgeRefusalReadReleased(confirmation,exclusion)

  private[floatingpod] def submitJournalMainReadRelease(reservation:MySqlDbFirstRecallJournalReservation,
      confirmation:MySqlDbFirstRecallJournalConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main journal read release required")
    confirmation.requireOriginal(reservation)
    require(confirmation.handle.issuer eq retainedPlanIssuerRef.get(),"original journal issuer required")
    prepareDbFirstMainReadRelease(confirmation.tag)
    submit(confirmation.tag,MySqlOwnedIoCommand.ReleaseJournalMainRead(confirmation))

  private[floatingpod] def finishJournalMainReadRelease(reservation:MySqlDbFirstRecallJournalReservation,
      confirmation:MySqlDbFirstRecallJournalConfirmation,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main journal read ACK required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original journal issuer required")
    issuer.requireJournalReadReleased(confirmation,reservation)
    completionRouter.requireJournalReadAck(confirmation,completion)
    requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,confirmation.tag)
    dbFirstReadCallbacks.resumeOrdinarySuccessor(confirmation.tag)
    require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original journal successor changed")
    completionRouter.finishJournalReadAck(confirmation,completion)
    issuer.acknowledgeJournalReadReleased(confirmation,reservation)

  /** 返回后Main须把旧J/exclusion/confirmation/completion断开，并跨END后才提交退休。 */
  private[floatingpod] def recordRefusalFinal(exclusion:MySqlDbFirstRecallRefusalSource,
      confirmation:MySqlDbFirstRecallRefusalConfirmation):MySqlDbFirstRecallRefusalFinalReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"refusal final detach requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(exclusion!=null && confirmation!=null && issuer!=null &&
      (confirmation.handle.issuer eq issuer),"original refusal final issuer required")
    completionRouter.requireRefusalFinalPreparation(exclusion)
    val receipt=issuer.recordRefusalFinal(confirmation,exclusion)
    completionRouter.prepareRefusalFinal(exclusion,receipt)
    receipt

  private def requireRefusalFinalReceipt(receipt:MySqlDbFirstRecallRefusalFinalReceipt):MySqlOwnedRetainedPlanIssuer =
    val issuer=retainedPlanIssuerRef.get()
    require(receipt!=null && issuer!=null && (receipt.handle.issuer eq issuer),"original refusal final receipt issuer required")
    currentExecutionIdentity.requireTag(receipt.tag)
    issuer

  private[floatingpod] def submitRefusalFinalRelease(receipt:MySqlDbFirstRecallRefusalFinalReceipt):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"refusal final release requires exact current Main")
    requireRefusalFinalReceipt(receipt)
    submit(receipt.tag,MySqlOwnedIoCommand.ReleaseRefusalFinal(receipt))

  private[floatingpod] def finishRefusalFinalRelease(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"refusal final ACK requires exact current Main")
    val issuer=requireRefusalFinalReceipt(receipt)
    // 两边先核，随后只做纯内存ACK；等值重造completion不获得清槽资格。
    completionRouter.requireRefusalFinalAck(receipt,completion)
    issuer.requireRefusalFinalRetired(receipt)
    issuer.acknowledgeRefusalFinal(receipt)
    completionRouter.finishRefusalFinalAck(receipt,completion)

  private[floatingpod] def prepareJournalCaptureTransfer(reservation:MySqlDbFirstRecallJournalReservation,
      confirmation:MySqlDbFirstRecallJournalConfirmation):MySqlDbFirstRecallJournalCaptureTransfer =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,"original journal Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original journal issuer required")
    issuer.prepareJournalCaptureTransfer(reservation,confirmation)

  /** 调用前Main owner须将原J reservation/completion引用替换为transfer；不借出原payload。 */
  private[floatingpod] def submitJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):IoSubmission =
    require(transfer!=null,"original journal/C transfer required")
    submit(transfer.tag,MySqlOwnedIoCommand.RetireJournalForCapture(transfer))

  private[floatingpod] def finishJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer,
      completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstCaptureReservation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && transfer!=null,"original journal/C Main required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (transfer.handle.issuer eq issuer),"original journal/C issuer required")
    issuer.requireJournalCaptureTransferRetired(transfer)
    completionRouter.requireJournalCaptureAck(transfer,completion)
    try
      issuer.publishJournalCaptureTransfer(transfer)
      completionRouter.finishJournalCaptureAck(transfer,completion)
      issuer.activateJournalCaptureTransfer(transfer)
    catch case cause:Throwable=>
      try issuer.failJournalCaptureTransfer(transfer,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

  private[floatingpod] def submitPreparationMainReadRelease(reservation:MySqlDbFirstInitialPreparationReservation,
      confirmation:MySqlDbFirstInitialPreparationConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main preparation read release required")
    confirmation.requireOriginal(reservation)
    require(confirmation.handle.issuer eq retainedPlanIssuerRef.get(),"original preparation issuer required")
    prepareDbFirstMainReadRelease(confirmation.tag)
    submit(confirmation.tag,MySqlOwnedIoCommand.ReleasePreparationMainRead(confirmation))

  private[floatingpod] def finishPreparationMainReadRelease(reservation:MySqlDbFirstInitialPreparationReservation,
      confirmation:MySqlDbFirstInitialPreparationConfirmation,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main preparation read ACK required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original preparation issuer required")
    issuer.requirePreparationReadReleased(confirmation,reservation)
    completionRouter.requirePreparationReadAck(confirmation,completion)
    requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,confirmation.tag)
    dbFirstReadCallbacks.resumeOrdinarySuccessor(confirmation.tag)
    require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original preparation successor changed")
    completionRouter.finishPreparationReadAck(confirmation,completion)

  private[floatingpod] def submitCheckpointMainReadRelease(reservation:MySqlDbFirstCaptureReservation,
      confirmation:MySqlDbFirstCheckpointConfirmation):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main checkpoint read release required")
    confirmation.requireOriginal(reservation)
    require(confirmation.handle.issuer eq retainedPlanIssuerRef.get(),"original checkpoint issuer required")
    prepareDbFirstMainReadRelease(confirmation.tag)
    submit(confirmation.tag,MySqlOwnedIoCommand.ReleaseCheckpointMainRead(confirmation))

  private[floatingpod] def finishCheckpointMainReadRelease(reservation:MySqlDbFirstCaptureReservation,
      confirmation:MySqlDbFirstCheckpointConfirmation,completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && confirmation!=null,
      "original Main checkpoint read ACK required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (confirmation.handle.issuer eq issuer),"original checkpoint issuer required")
    issuer.requireCheckpointReadReleased(confirmation,reservation)
    completionRouter.requireCheckpointReadAck(confirmation,completion)
    requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,confirmation.tag)
    dbFirstReadCallbacks.resumeOrdinarySuccessor(confirmation.tag)
    require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original checkpoint successor changed")
    completionRouter.finishCheckpointReadAck(confirmation,completion)
    issuer.acknowledgeCheckpointReadReleased(confirmation,reservation)

  private[floatingpod] def submitDbFirstRecallRelease(receipt: MySqlDbFirstRecallDropReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release requires exact current Main")
    requireDbFirstDropReceipt(receipt)
    require(!receipt.isRetained,"retained drop requires its dedicated worker retirement route")
    prepareDbFirstMainReadRelease(receipt.tag)
    submit(receipt.tag, MySqlOwnedIoCommand.ReleaseDbFirstRecall(receipt))

  /** 只撤Main cap图引用，不排worker命令、不清adapter原资格或费用。 */
  private[floatingpod] def handoffRetainedDbFirstMainDrop(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"retained graph handoff requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(receipt!=null && receipt.isRetained && issuer!=null && (receipt.handle.issuer eq issuer),
      "original retained native drop issuer required")
    val qualification=retainedDbFirstRecallRef.get()
    require(qualification!=null && (receipt.retainedConfirmation.retainedQualification eq qualification),
      "actual adapter original retained qualification required")
    issuer.handoffRetainedDbFirstMainDrop(receipt,qualification)

  private[floatingpod] def submitRetainedDbFirstRecallRelease(receipt:MySqlDbFirstRecallDropReceipt):IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"retained release requires exact current Main")
    val issuer=retainedPlanIssuerRef.get()
    require(receipt!=null && receipt.isRetained && issuer!=null && (receipt.handle.issuer eq issuer) &&
      retiredDbFirstRecallHoldRef.get()==null,"original retained release issuer and unused worker hold required")
    issuer.requireRetainedDbFirstReleaseSubmission(receipt,retainedDbFirstRecallRef.get())
    completionRouter.beginRelease(receipt.tag,MySqlCompletionChannel.Ordinary)
    try
      // 控制work不新建O、不触SQL/save barrier，也不增减原accepted repository epoch。
      val result=io.submit(receipt.tag,Work(receipt.tag,MySqlOwnedIoCommand.ReleaseRetainedDbFirstRecall(receipt),
        acceptedRepositoryEpoch,None))
      completionRouter.submitted(receipt.tag,result)
      result
    catch case cause:Throwable=>
      completionRouter.submissionFailed(receipt.tag,cause)
      throw cause

  private[floatingpod] def finishRetainedDbFirstRecallRelease(receipt:MySqlDbFirstRecallDropReceipt,
      completion:IoCompletion[MySqlOwnedIoReply]):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"retained release ACK requires exact current Main")
    require(receipt!=null && receipt.isRetained && receipt.retainedEntryRetired &&
      (retiredDbFirstRecallHoldRef.get() eq receipt),"actual original retained worker retirement required")
    completionRouter.finishDbFirstRecallRelease(receipt,completion)
    require(retiredRecallReleaseAckRef.compareAndSet(null,receipt),"original retained release ACK already recorded")
    observeRetiredRecallProducerDrain()
    // Shared fault/两份Session DATA继续保管；不发布successor或恢复ordinary producer。

  /** Main已消费原ACK、全部旧路由及IO队列为空；hold仍封住新submit/capture，不发行普通许可。 */
  private def observeRetiredRecallProducerDrain():Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"actual Main drain observer required")
    val receipt=retiredRecallReleaseAckRef.get()
    if receipt!=null && (retiredDbFirstRecallHoldRef.get() eq receipt) &&
        retiredRecallProducerDrainRef.get()==null && restrictedSourceR0Pending.get()==null &&
        completionRouter.retainedCount==0 && completionRouter.failure.isEmpty then
      val actual=io.checkpoint
      if actual.accepting && !actual.stopped && actual.fatalCause.isEmpty && actual.exitFailure.isEmpty &&
          actual.queued==0 && !actual.running && actual.completions==0 && actual.reserved==0 then
        completionRouter.requireTerminalEmpty()
        require(retiredRecallProducerDrainRef.compareAndSet(null,receipt),"actual old producer drain already recorded")

  private[mysql] def retiredRecallIssuerTransferReady(receipt:MySqlDbFirstRecallDropReceipt):Boolean =
    receipt!=null && (retiredRecallProducerDrainRef.get() eq receipt)

  private[mysql] def requireRetiredRecallCleanupNavigation(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    require(session!=null && receipt!=null && receipt.retainedEntryRetired &&
      (retiredDbFirstRecallHoldRef.get() eq receipt) && (sessionRef.get() match
        case SessionSlot.DbFirst(actual)=>actual eq session
        case _=>false),"actual retained recall Session and receipt required for cleanup navigation")

  private[mysql] def requireRetiredRecallIssuerTransfer(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt,issuer:MySqlOwnedRetainedPlanIssuer):Unit =
    val exact=sessionRef.get() match
      case SessionSlot.DbFirst(actual)=>actual eq session
      case _=>false
    val actual=io.checkpoint
    require(exact && receipt!=null && receipt.retainedEntryRetired &&
      (retiredDbFirstRecallHoldRef.get() eq receipt) && (retiredRecallReleaseAckRef.get() eq receipt) &&
      (retiredRecallProducerDrainRef.get() eq receipt) && retainedDbFirstRecallRef.get()==null &&
      issuer!=null && (retainedPlanIssuerRef.get() eq issuer) &&
      actual.accepting && !actual.stopped && actual.fatalCause.isEmpty && actual.exitFailure.isEmpty &&
      actual.maintenanceRunning && !actual.running && actual.queued==0 && actual.completions==0 && actual.reserved==0,
      "actual held adapter/Main ACK/drained old producer and maintenance worker required before issuer transfer")

  private[mysql] def retiredRecallMainReadEpoch(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt,issuer:MySqlOwnedRetainedPlanIssuer):Long =
    requireRetiredRecallIssuerTransfer(session,receipt,issuer)
    acceptedRepositoryEpoch // 原Main drain的Atomic发布之后读取；hold禁止任何后续普通提交。

  private[mysql] def registerRecoveredDbFirstService(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      previous:MySqlDbFirstMainReadBacking,next:MySqlDbFirstMainReadBacking,mailbox:MySqlOwnedMaintenanceMailbox,
      nextIdentity:MySqlOwnedIoIdentity,receipt:MySqlDbFirstRecallDropReceipt,epoch:Long):Unit =
    require(source!=null,"actual service Recovery required")
    source.requireServicePreparation(this,previous,next,mailbox,nextIdentity,receipt,epoch)
    val actual=new RecoveredServiceMainTransfer(source,previous,next,mailbox,nextIdentity,receipt,epoch)
    require(recoveredDbFirstServiceRef.compareAndSet(null,actual),"actual new service preparation already registered")

  private[mysql] def recoveredServiceMainStage(source:MySqlDbFirstSession.RetiredRecallWriterRecovery):Option[String] =
    Option(recoveredDbFirstServiceRef.get()).filter(_.source eq source).map(_.stage.get().toString)

  /** 仅Main引用切换与关闭宣告共用短锁；任何JDBC/资源清理都在锁外。 */
  private[mysql] def beginRecoveredDbFirstServiceClose(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      registered:Boolean):Unit = synchronized {
    source.requireServiceClosingRegistry(this)
    val actual=recoveredDbFirstServiceRef.get()
    if registered then
      require(actual!=null && (actual.source eq source),"actual registered service close owner required")
      actual.stage.set(RecoveredServiceMainStage.Closing)
    else require(actual==null,"unpublished service cannot close another actual transfer")
  }

  private[mysql] def detachRecoveredDbFirstService(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actualService:MySqlDbFirstSession,registered:Boolean):Unit = synchronized {
    source.requireClosedServicePreparation(this,actualService)
    val actual=recoveredDbFirstServiceRef.get()
    if registered then
      require(actual!=null && (actual.source eq source) && actual.stage.get()==RecoveredServiceMainStage.Closing &&
        recoveredDbFirstServiceRef.compareAndSet(actual,null),"actual service preparation registry changed during close")
    else require(actual==null,"unpublished service cannot detach another actual owner")
  }

  private[floatingpod] def requireRecoveredMainReadAttach(
      callback:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      next:MySqlDbFirstMainReadBacking):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && (dbFirstReadCallbacks eq callback) &&
      actual!=null && actual.stage.get()==RecoveredServiceMainStage.Claimed &&
      (actual.backing eq next) && (dbFirstMainReadRef.get() eq next),
      "actual controlled claimed service Main backing/callback required")
    val (main,transfer)=originalClaimedOwner.getOrElse(throw new IllegalStateException("original Main transfer missing"))
    next.requireRecoveredMainClaim(this,main,transfer)
  }

  /** 原pump实际drop/claim/attach的专用ACK；不消费业务C、不发普通freshness/Ready。 */
  private[floatingpod] def advanceRetainedRecallServiceMain(
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,tag:IoOperationTag):Boolean = synchronized {
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && owner!=null && tag!=null,
      "actual Main Boot recovery owner and original operation required")
    val actual=recoveredDbFirstServiceRef.get()
    val recoveryFailure=retainedServiceFailureRef.get()
    if recoveryFailure!=null then
      val receipt=retiredDbFirstRecallHoldRef.get()
      require(receipt!=null && receipt.tag==tag,"actual failed original retained service required")
      owner.requireRetainedServiceMainDispatch(this,dbFirstReadCallbacks,tag,
        receipt.retainedConfirmation.retainedFailure.get)
      throw recoveryFailure
    if actual==null then return false
    require(actual.receipt.tag==tag && (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
      (retiredRecallReleaseAckRef.get() eq actual.receipt) && (retiredRecallProducerDrainRef.get() eq actual.receipt),
      "actual original retired release and completed Main drain required")
    owner.requireRetainedServiceMainDispatch(this,dbFirstReadCallbacks,tag,
      actual.receipt.retainedConfirmation.retainedFailure.get)
    if actual.stage.get()==RecoveredServiceMainStage.Acknowledged ||
        actual.stage.get()==RecoveredServiceMainStage.WorkerRetiring ||
        actual.stage.get()==RecoveredServiceMainStage.WorkerRetired ||
        actual.stage.get()==RecoveredServiceMainStage.ServiceRegistered ||
        actual.stage.get()==RecoveredServiceMainStage.ServiceOwned then return true
    require(actual.stage.get()==RecoveredServiceMainStage.Prepared && actual.failure.get()==null,
      "partial service Main handoff cannot retry or claim ordinary readiness")
    completionRouter.requireTerminalEmpty()
    val state=io.checkpoint
    require(state.accepting && !state.stopped && state.fatalCause.isEmpty && state.exitFailure.isEmpty &&
      state.queued==0 && !state.running && state.completions==0 && state.reserved==0 &&
      actual.epoch==acceptedRepositoryEpoch && actual.nextIdentity.bootId==identity.bootId &&
      actual.nextIdentity.bootGeneration==identity.bootGeneration &&
      actual.nextIdentity.connectionGeneration>identity.connectionGeneration &&
      (dbFirstMainReadRef.get() eq actual.previous),"actual held old Main generation/empty IO and new service generation required")
    if !actual.mailbox.poll.isCurrent then return false // 未做任何drop；等待实际续租，不能延长投影窗口。
    try
      dbFirstReadCallbacks.detachSupersededMainRead(actual.previous)
      actual.stage.set(RecoveredServiceMainStage.PreviousDropped)
      require(dbFirstMainReadRef.compareAndSet(actual.previous,actual.backing),"actual service Main registry changed")
      actual.stage.set(RecoveredServiceMainStage.Registered)
      val (main,transfer)=originalClaimedOwner.getOrElse(throw new IllegalStateException("original Main transfer missing"))
      actual.backing.claimMain(main,transfer)
      actual.stage.set(RecoveredServiceMainStage.Claimed)
      dbFirstReadCallbacks.attachRecoveredMainRead(actual.backing)
      actual.stage.set(RecoveredServiceMainStage.Attached)
      actual.backing.requireRecoveredMainClaim(this,main,transfer)
      require(actual.mailbox.poll.isCurrent,"actual service windows expired during Main control handoff")
      actual.stage.set(RecoveredServiceMainStage.Acknowledged)
      true
    catch case cause:Throwable=>
      actual.failure.compareAndSet(null,cause)
      actual.stage.set(RecoveredServiceMainStage.Failed)
      throw cause
  }

  /** 专用Main ACK只在原maintenance worker上消费；资源close不持adapter锁。 */
  private[mysql] def beginRecoveredOldDataRetirement(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallDropReceipt,
      previous:MySqlDbFirstMainReadBacking,next:MySqlDbFirstMainReadBacking,
      issuer:MySqlOwnedRetainedPlanIssuer):Boolean = synchronized {
    requireRetiredRecallIssuerTransfer(session,receipt,issuer)
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.source eq source) && (actual.receipt eq receipt) &&
      (actual.previous eq previous) && (actual.backing eq next) &&
      actual.failure.get()==null,"actual private service transfer/held worker and exact DATA required")
    if actual.stage.get()==RecoveredServiceMainStage.Prepared then
      require(dbFirstMainReadRef.get() eq previous,"actual prepared transfer must retain original Main DATA")
      return false
    require(actual.stage.get()==RecoveredServiceMainStage.Acknowledged && (dbFirstMainReadRef.get() eq next),
      "partial Main/worker transfer cannot retire or retry old DATA")
    actual.stage.set(RecoveredServiceMainStage.WorkerRetiring)
    true
  }

  private[mysql] def detachRetiredRecoveredPrevious(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      previous:MySqlDbFirstMainReadBacking):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.source eq source) && (actual.previous eq previous) &&
      actual.stage.get()==RecoveredServiceMainStage.WorkerRetiring && actual.failure.get()==null,
      "actual worker retirement must retain exact previous backing until resource retirement")
    previous.requireRetiredOnWorker()
    actual.previous=null // holder退款前撤控制记录的旧RAW责任边。
  }

  private[mysql] def finishRecoveredOldDataRetirement(source:MySqlDbFirstSession.RetiredRecallWriterRecovery):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.source eq source) && actual.previous==null &&
      actual.stage.get()==RecoveredServiceMainStage.WorkerRetiring && actual.failure.get()==null,
      "actual worker retirement phase and detached previous backing required")
    source.requireRetiredOldData()
    actual.stage.set(RecoveredServiceMainStage.WorkerRetired)
  }

  private[mysql] def failRecoveredOldDataRetirement(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      cause:Throwable):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.source eq source) && cause!=null,
      "actual failed worker retirement record required")
    actual.failure.compareAndSet(null,cause)
    actual.stage.set(RecoveredServiceMainStage.Failed)
  }

  private[mysql] def registerRecoveredServiceRuntime(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      service:MySqlDbFirstSession,inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,
      next:MySqlOwnedIoIdentity):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && source!=null && (actual.source eq source) && actual.service==null &&
      actual.stage.get()==RecoveredServiceMainStage.WorkerRetired && actual.failure.get()==null &&
      (actual.nextIdentity eq next) && (dbFirstMainReadRef.get() eq actual.backing),
      "actual privately retained completed worker transfer required for runtime ownership")
    source.requireServiceRuntimeRegistration(this,service,inputs,root,next)
    actual.service=service
    actual.stage.set(RecoveredServiceMainStage.ServiceRegistered)
  }

  /** 只核实际私有runtime记录；无需回到已退休inspection/Work或原Failed业务资格。 */
  private[mysql] def requireRegisteredRecoveredService(service:MySqlDbFirstSession,
      inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(service!=null && inputs!=null && root!=null && next!=null && actual!=null &&
      (actual.service eq service) && (actual.nextIdentity eq next) &&
      (actual.stage.get()==RecoveredServiceMainStage.ServiceRegistered ||
        actual.stage.get()==RecoveredServiceMainStage.ServiceOwned || actual.stage.get()==RecoveredServiceMainStage.Closing),
      "actual registered recovered Session/root/input identity required")
    service.requireRegisteredRuntimeBinding(this,inputs,root,next)
  }

  /** 真实旧slot实例CAS；旧hold继续封业务，维护及stop改走实际新owner。 */
  private[mysql] def takeOverRecoveredService(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      previous:MySqlDbFirstSession,service:MySqlDbFirstSession,mailbox:MySqlOwnedMaintenanceMailbox,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    val actual=recoveredDbFirstServiceRef.get()
    val oldSlot=sessionRef.get()
    synchronized {
      require(source!=null && previous!=null && service!=null && actual!=null && (actual.source eq source) &&
        (actual.service eq service) && (actual.mailbox eq mailbox) && (actual.receipt eq receipt) &&
        actual.stage.get()==RecoveredServiceMainStage.ServiceRegistered && actual.failure.get()==null &&
        actual.predecessor==null && (retiredDbFirstRecallHoldRef.get() eq receipt),
        "actual privately registered service and original held predecessor required")
      oldSlot match
        case SessionSlot.DbFirst(value)=>require(value eq previous,"actual original worker Session slot required")
        case _=>throw new IllegalStateException("actual original schema6 slot missing")
      source.requireRunningServiceTransfer(this,previous,service,mailbox,receipt)
      actual.predecessor=previous
      source.moveRunningServiceCleanup(this,service)
      require(sessionRef.compareAndSet(oldSlot,SessionSlot.DbFirst(service)),"actual worker Session slot changed during service transfer")
      actual.stage.set(RecoveredServiceMainStage.ServiceOwned)
    }
    // 资源/holder退款在锁外；失败保同一旧holder，不回滚新slot、不重新提交旧C。
    try
      source.retireTransferredRecovery(this,service)
      previous.retireTransferredParent(this)
      synchronized {
        require((recoveredDbFirstServiceRef.get() eq actual) &&
          actual.stage.get()==RecoveredServiceMainStage.ServiceOwned && actual.source==null &&
          actual.predecessor==null && actual.cleanupFailure.get()==null,
          "actual old holders must finish refund before Main publication ACK")
        service.requireOwnedServiceCleanup(this,mailbox)
        actual.workerCleanupComplete.set(true)
      }
    catch case cause:Throwable=>
      actual.cleanupFailure.compareAndSet(null,cause)
      throw cause

  /** 原Boot END内协调同一scope；不更换router、不撤旧业务hold、不结算原C。 */
  private[floatingpod] def advanceRecoveredServicePublication(
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,tag:IoOperationTag,
      scope:com.wi.floatingpod.server.ActiveFloatingPodManager#MainPublicationScope):Boolean = synchronized {
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && owner!=null && tag!=null && scope!=null,
      "actual original Main Boot publication dispatch required")
    val actual=recoveredDbFirstServiceRef.get()
    if actual==null then return false
    owner.requireRetainedServiceMainDispatch(this,dbFirstReadCallbacks,tag,
      actual.receipt.retainedConfirmation.retainedFailure.get)
    require(actual.receipt.tag==tag && (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
      (retiredRecallReleaseAckRef.get() eq actual.receipt) && (retiredRecallProducerDrainRef.get() eq actual.receipt),
      "same actual original C hold/final ACK/drain required")
    if !actual.workerCleanupComplete.get() then
      if actual.cleanupFailure.get()!=null then throw actual.cleanupFailure.get()
      if actual.failure.get()!=null then throw actual.failure.get()
      return false
    require(actual.stage.get()==RecoveredServiceMainStage.ServiceOwned && actual.failure.get()==null &&
      actual.cleanupFailure.get()==null && actual.source==null && actual.predecessor==null && actual.previous==null &&
      actual.service!=null && (dbFirstMainReadRef.get() eq actual.backing) && actual.epoch==acceptedRepositoryEpoch,
      "actual fully retired predecessor and current recovered service required")
    require(sessionRef.get() match
      case SessionSlot.DbFirst(value)=>value eq actual.service
      case _=>false,"actual new worker Session slot required for Main publication")
    completionRouter.requireTerminalEmpty()
    val state=io.checkpoint
    require(state.accepting && !state.stopped && state.fatalCause.isEmpty && state.exitFailure.isEmpty &&
      state.queued==0 && !state.running && state.completions==0 && state.reserved==0,
      "actual sealed empty original IO route required for Main publication")
    if !actual.mailbox.poll.isCurrent then return false
    if actual.publicationAcknowledged then
      require(scope.identity eq actual.nextIdentity,"actual acknowledged scope identity changed")
      return true
    require(actual.publicationDispatch==null,"recovered Main publication is not reentrant")
    actual.publicationDispatch=scope
    try
      val binding=actual.backing.recoveredPublicationBinding(this)
      dbFirstReadCallbacks.adoptRecoveredMainPublication(scope,actual.backing,actual.nextIdentity,binding)
      require(actual.mailbox.poll.isCurrent,"actual service windows expired during Main publication")
      actual.publicationAcknowledged=true
      true
    catch case cause:Throwable=>
      actual.failure.compareAndSet(null,cause)
      actual.stage.set(RecoveredServiceMainStage.Failed)
      throw cause
    finally actual.publicationDispatch=null
  }

  private[floatingpod] def requireRecoveredPublicationDispatch(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      scope:com.wi.floatingpod.server.ActiveFloatingPodManager#MainPublicationScope,
      next:MySqlOwnedIoIdentity):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && (dbFirstReadCallbacks eq callbacks) &&
      actual!=null && (actual.publicationDispatch eq scope) && scope!=null &&
      (actual.nextIdentity eq next) && actual.workerCleanupComplete.get() && !actual.publicationAcknowledged &&
      actual.stage.get()==RecoveredServiceMainStage.ServiceOwned && actual.failure.get()==null &&
      actual.cleanupFailure.get()==null && (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
      (dbFirstMainReadRef.get() eq actual.backing) && actual.mailbox.poll.isCurrent,
      "actual private recovered Main publication frame required")
  }

  private[mysql] def requireRecoveredPublicationBacking(backing:MySqlDbFirstMainReadBacking):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.backing eq backing),"actual recovered publication backing required")
    requireRecoveredPublicationDispatch(dbFirstReadCallbacks,actual.publicationDispatch,actual.nextIdentity)
    val (main,transfer)=originalClaimedOwner.getOrElse(throw new IllegalStateException("actual original Main claim missing"))
    backing.requireRecoveredMainClaim(this,main,transfer)
  }

  /** 完整原C收口边界：旧路由已空、原请求严格结算、保存恢复后，最后才开放业务。 */
  private[floatingpod] def finishRecoveredServiceExecution(
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,tag:IoOperationTag,
      scope:com.wi.floatingpod.server.ActiveFloatingPodManager#MainPublicationScope,
      original:MySqlDbFirstRecallRequestRecord):Boolean = synchronized {
    require(owner!=null && scope!=null && original!=null && currentMainOwner.exists(_ eq Thread.currentThread()),
      "actual original Boot retained service completion required")
    val actual=recoveredDbFirstServiceRef.get()
    if actual==null then return false
    owner.requireRetainedServiceMainDispatch(this,dbFirstReadCallbacks,tag,
      actual.receipt.retainedConfirmation.retainedFailure.get)
    require(actual.publicationAcknowledged && (scope.identity eq actual.nextIdentity) &&
      actual.workerCleanupComplete.get() && actual.stage.get()==RecoveredServiceMainStage.ServiceOwned &&
      actual.failure.get()==null && actual.cleanupFailure.get()==null && actual.source==null &&
      actual.previous==null && actual.predecessor==null && actual.receipt.tag==tag &&
      (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
      (retiredRecallReleaseAckRef.get() eq actual.receipt) && (retiredRecallProducerDrainRef.get() eq actual.receipt) &&
      (dbFirstMainReadRef.get() eq actual.backing) && actual.epoch==acceptedRepositoryEpoch &&
      !adapterStopping && !actual.executionPublished.get() && !actual.businessReleased.get() && !actual.executionDispatch,
      "actual published service and unused original C completion boundary required")
    require(sessionRef.get() match
      case SessionSlot.DbFirst(value)=>value eq actual.service
      case _=>false,"actual recovered worker owner must remain current")
    completionRouter.requireTerminalEmpty()
    val state=io.checkpoint
    require(state.accepting && !state.stopped && state.fatalCause.isEmpty && state.exitFailure.isEmpty &&
      state.queued==0 && !state.running && state.completions==0 && state.reserved==0,
      "actual empty sealed IO required before original C settlement")
    if !actual.mailbox.poll.isCurrent then return false
    actual.executionDispatch=true
    try
      actual.backing.requireRecoveredCommitted(this,original)
      completionRouter.adoptRecoveredExecution(this,actual.nextIdentity)
      actual.executionPublished.set(true)
      owner.completeRetainedServiceCommitted(this,original)
      actual.originalCommitted=true
      dbFirstReadCallbacks.resumeRecoveredService(actual.backing,actual.nextIdentity)
      require(!adapterStopping && actual.stage.get()==RecoveredServiceMainStage.ServiceOwned &&
        actual.failure.get()==null && actual.cleanupFailure.get()==null && actual.mailbox.poll.isCurrent,
        "actual recovered service stopped, failed or expired before admission publication")
      actual.businessReleased.set(true) // release发布发生在结算与save恢复之后；receipt/cause历史继续保留。
      true
    catch case cause:Throwable=>
      actual.failure.compareAndSet(null,cause);actual.stage.set(RecoveredServiceMainStage.Failed)
      throw cause
    finally actual.executionDispatch=false
  }

  private[mysql] def requireRecoveredExecutionRouter(router:MySqlTrackedDriftCompletionRouter,
      next:MySqlOwnedIoIdentity):Unit = synchronized {
    require(router!=null && (router eq completionRouter),"actual original shared router required")
    requireRecoveredExecutionFrame(dbFirstReadCallbacks,next,false)
  }
  private def requireRecoveredExecutionFrame(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      next:MySqlOwnedIoIdentity,settled:Boolean):Unit =
    val actual=recoveredDbFirstServiceRef.get()
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && actual!=null && actual.executionDispatch &&
      (dbFirstReadCallbacks eq callbacks) && (actual.nextIdentity eq next) && actual.publicationAcknowledged &&
      actual.workerCleanupComplete.get() && actual.stage.get()==RecoveredServiceMainStage.ServiceOwned &&
      actual.failure.get()==null && actual.cleanupFailure.get()==null && !actual.businessReleased.get() &&
      (!settled || actual.originalCommitted && actual.executionPublished.get()) &&
      (dbFirstMainReadRef.get() eq actual.backing) && (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
      actual.mailbox.poll.isCurrent,"actual private retained service execution/settlement frame required")

  private[floatingpod] def requireRecoveredOriginalCommit(
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,original:MySqlDbFirstRecallRequestRecord):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && owner!=null && original!=null,"actual original retained commit owner required")
    requireRecoveredExecutionFrame(dbFirstReadCallbacks,actual.nextIdentity,false)
    owner.requireRetainedServiceMainDispatch(this,dbFirstReadCallbacks,actual.receipt.tag,
      actual.receipt.retainedConfirmation.retainedFailure.get)
    actual.backing.requireRecoveredCommitted(this,original)
  }
  private[mysql] def requireRecoveredCommittedBacking(backing:MySqlDbFirstMainReadBacking):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.backing eq backing),"actual strict recovered backing required")
    requireRecoveredExecutionFrame(dbFirstReadCallbacks,actual.nextIdentity,false)
    val (main,transfer)=originalClaimedOwner.getOrElse(throw new IllegalStateException("actual original Main claim missing"))
    backing.requireRecoveredMainClaim(this,main,transfer)
  }
  private[floatingpod] def requireRecoveredSaveResume(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      backing:MySqlDbFirstMainReadBacking,next:MySqlOwnedIoIdentity):Unit = synchronized {
    requireRecoveredExecutionFrame(callbacks,next,true)
    require(recoveredDbFirstServiceRef.get().backing eq backing,"actual settled recovered save backing required")
    completionRouter.requireTerminalEmpty()
  }

  private[mysql] def detachRetiringRecoveredSource(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      previous:MySqlDbFirstSession):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.source eq source) && (actual.predecessor eq previous) &&
      (actual.stage.get()==RecoveredServiceMainStage.ServiceOwned || stoppedRecoveredCleanup(actual)) &&
      actual.service!=null && actual.service.hasAdapterCleanup(this),
      "actual running service and exact old Recovery retirement required")
    actual.source=null
  }

  private[mysql] def retainFailedRecoveredSource(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,cause:Throwable):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && actual.source==null && source!=null && actual.predecessor!=null,
      "failed Recovery refund must keep exact cleanup owner")
    actual.source=source;actual.cleanupFailure.compareAndSet(null,cause)
  }

  private[mysql] def detachRetiringRecoveredParent(previous:MySqlDbFirstSession):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && actual.source==null && (actual.predecessor eq previous) &&
      (actual.stage.get()==RecoveredServiceMainStage.ServiceOwned || stoppedRecoveredCleanup(actual)),
      "actual Recovery retirement precedes old parent refund")
    previous.requireRetiringParentHolder()
    sessionRef.get() match
      case SessionSlot.DbFirst(value)=>require((value eq actual.service) ||
        stoppedRecoveredCleanup(actual) && (value eq previous),"actual new worker service slot required")
      case _=>throw new IllegalStateException("actual new worker service slot missing")
    actual.predecessor=null
  }

  private[mysql] def retainFailedRecoveredParent(previous:MySqlDbFirstSession,cause:Throwable):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && actual.predecessor==null && previous!=null,"failed parent refund retains exact cleanup holder")
    actual.predecessor=previous;actual.cleanupFailure.compareAndSet(null,cause)
  }

  private[mysql] def beginOwnedRecoveredServiceClose(service:MySqlDbFirstSession):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.service eq service) && service.hasAdapterCleanup(this),
      "actual privately owned recovered service close required")
    actual.stage.set(RecoveredServiceMainStage.Closing)
  }

  private[mysql] def detachOwnedRecoveredServiceBeforeInputs(service:MySqlDbFirstSession):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(actual!=null && (actual.service eq service) && actual.stage.get()==RecoveredServiceMainStage.Closing,
      "actual recovered service closing registry required")
    service.requireOwnedServiceResourcesBeforeInputs(this)
    if actual.cleanupFailure.get()!=null then throw actual.cleanupFailure.get()
    require(actual.source==null && actual.predecessor==null,"old cleanup holders remain charged and cannot be discarded")
    service.detachOwnedServiceInputs(this)
    require(recoveredDbFirstServiceRef.compareAndSet(actual,null),"actual owned service registry changed during close")
  }

  private def ownedRecoveredService:Option[(MySqlDbFirstSession,MySqlOwnedMaintenanceMailbox)] =
    Option(recoveredDbFirstServiceRef.get()).filter(value=>value.service!=null && value.service.hasAdapterCleanup(this))
      .map(value=>(value.service,value.mailbox))

  private def dropRetainedRecallReferenceOnWorker(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    val qualification=retainedDbFirstRecallRef.get()
    require(retiredDbFirstRecallHoldRef.get() eq receipt,"actual original retained worker hold required")
    session.requireRetainedQualificationDetach(this,receipt,qualification)
    require(retainedDbFirstRecallRef.compareAndSet(qualification,null),"original adapter qualification holder changed")

  private[mysql] def requireRetainedRecallReferenceDetached(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    val exact=sessionRef.get() match
      case SessionSlot.DbFirst(actual)=>actual eq session
      case _=>false
    require(exact && retainedDbFirstRecallRef.get()==null && (retiredDbFirstRecallHoldRef.get() eq receipt),
      "actual adapter qualification must detach before original root/fee retirement")

  private[floatingpod] def finishDbFirstRecallRelease(receipt: MySqlDbFirstRecallDropReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release ACK requires exact current Main")
    requireDbFirstDropReceipt(receipt)
    if dbFirstPendingMainReadRef.get()!=null then
      // 真worker DATA退休事实独立于可封闭的Entry preflight失败；不能凭调用方构造的completion恢复。
      requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,receipt.tag)
      dbFirstReadCallbacks.resumeOrdinarySuccessor(receipt.tag)
      require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original successor handoff changed")
    completionRouter.finishDbFirstRecallRelease(receipt, completion)

  private[floatingpod] def recordDbFirstPreparationFinal(confirmation: MySqlDbFirstInitialPreparationConfirmation): MySqlDbFirstPreparationFinalReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first Main detach requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(confirmation != null && issuer != null && (confirmation.handle.issuer eq issuer),
      "DB-first detach requires its original confirmation issuer")
    issuer.recordDbFirstPreparationFinal(confirmation)

  private def requireDbFirstPreparationFinalReceipt(receipt: MySqlDbFirstPreparationFinalReceipt): Unit =
    val issuer = retainedPlanIssuerRef.get()
    require(receipt != null && issuer != null && (receipt.handle.issuer eq issuer),
      "DB-first release requires its original Main drop receipt")
    currentExecutionIdentity.requireTag(receipt.tag)

  private[floatingpod] def submitDbFirstPreparationFinalRelease(receipt: MySqlDbFirstPreparationFinalReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release requires exact current Main")
    requireDbFirstPreparationFinalReceipt(receipt)
    submit(receipt.tag, MySqlOwnedIoCommand.ReleaseDbFirstPreparationFinal(receipt))

  private[floatingpod] def finishDbFirstPreparationFinalRelease(receipt: MySqlDbFirstPreparationFinalReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release ACK requires exact current Main")
    requireDbFirstPreparationFinalReceipt(receipt)
    completionRouter.finishDbFirstPreparationFinalRelease(receipt, completion)

  private[floatingpod] def recordDbFirstCheckpointFinal(confirmation: MySqlDbFirstCheckpointConfirmation): MySqlDbFirstCheckpointFinalReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first Main detach requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(confirmation != null && issuer != null && (confirmation.handle.issuer eq issuer),
      "DB-first detach requires its original confirmation issuer")
    issuer.recordDbFirstCheckpointFinal(confirmation)

  private def requireDbFirstCheckpointFinalReceipt(receipt: MySqlDbFirstCheckpointFinalReceipt): Unit =
    val issuer = retainedPlanIssuerRef.get()
    require(receipt != null && issuer != null && (receipt.handle.issuer eq issuer),
      "DB-first release requires its original Main drop receipt")
    currentExecutionIdentity.requireTag(receipt.tag)

  private[floatingpod] def submitDbFirstCheckpointFinalRelease(receipt: MySqlDbFirstCheckpointFinalReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release requires exact current Main")
    requireDbFirstCheckpointFinalReceipt(receipt)
    submit(receipt.tag, MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(receipt))

  private[floatingpod] def finishDbFirstCheckpointFinalRelease(receipt: MySqlDbFirstCheckpointFinalReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release ACK requires exact current Main")
    requireDbFirstCheckpointFinalReceipt(receipt)
    completionRouter.finishDbFirstCheckpointFinalRelease(receipt, completion)

  private[floatingpod] def recordDbFirstInitialPublication(confirmation: MySqlDbFirstInitialActivationConfirmation): MySqlDbFirstInitialPublicationReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first Main detach requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(confirmation != null && issuer != null && (confirmation.handle.issuer eq issuer),
      "DB-first detach requires its original confirmation issuer")
    issuer.recordDbFirstInitialPublication(confirmation)

  private def requireDbFirstInitialPublicationReceipt(receipt: MySqlDbFirstInitialPublicationReceipt): Unit =
    val issuer = retainedPlanIssuerRef.get()
    require(receipt != null && issuer != null && (receipt.handle.issuer eq issuer),
      "DB-first release requires its original Main drop receipt")
    currentExecutionIdentity.requireTag(receipt.tag)

  private[floatingpod] def submitDbFirstInitialPublicationRelease(receipt: MySqlDbFirstInitialPublicationReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release requires exact current Main")
    requireDbFirstInitialPublicationReceipt(receipt)
    prepareDbFirstMainReadRelease(receipt.tag)
    submit(receipt.tag, MySqlOwnedIoCommand.ReleaseDbFirstInitialPublication(receipt))

  private[floatingpod] def finishDbFirstInitialPublicationRelease(receipt: MySqlDbFirstInitialPublicationReceipt,
      completion: IoCompletion[MySqlOwnedIoReply]): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "DB-first release ACK requires exact current Main")
    requireDbFirstInitialPublicationReceipt(receipt)
    if dbFirstPendingMainReadRef.get()!=null then
      // 真worker DATA退休事实独立于可封闭的Entry preflight失败；不能凭调用方构造的completion恢复。
      requireCurrentDbFirstSaveSuccessor(dbFirstReadCallbacks,receipt.tag)
      dbFirstReadCallbacks.resumeOrdinarySuccessor(receipt.tag)
      require(dbFirstPendingMainReadRef.compareAndSet(dbFirstMainReadRef.get(),null),"original successor handoff changed")
    completionRouter.finishDbFirstInitialPublicationRelease(receipt, completion)

  /** Main只在当前实际Boot issuer/freshness上先预留再编码；不接触session/JDBC。 */
  private[floatingpod] def reserveRepositoryPlan(tag: IoOperationTag, workflow: UUID, reserve: UUID,
      operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    requirePlanReservationOwner()
    retainedPlanIssuerRef.get().reserveSingle(tag, workflow, reserve, operation)

  private[floatingpod] def reserveRepositoryTransactionPlan(tag: IoOperationTag, workflow: UUID, reserve: UUID,
      operations: Vector[MySqlRepositoryOperation]): MySqlOwnedRepositoryOperationPlan =
    requirePlanReservationOwner()
    retainedPlanIssuerRef.get().reserveTransaction(tag, workflow, reserve, operations)

  private def requirePlanReservationOwner(): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "reservation requires exact current Main")
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully),
      "unconsumed bootstrap cannot issue business plans")
    val observed = maintenanceMailbox.poll
    require(observed.isCurrent && io.checkpoint.accepting && io.checkpoint.fatalCause.isEmpty && retainedPlanIssuerRef.get() != null,
      "actual Boot/session admission is unavailable or expired; no new plan")

  private[mysql] def requireOriginalDbFirstSession(actual:MySqlDbFirstSession):Unit =
    require(sessionRef.get() match
      case SessionSlot.DbFirst(original)=>original eq actual
      case _=>false,"original adapter schema6 Session required")
  private[mysql] def requireRecordedOwnerClaim(actual:Thread,receipt:IoOwnerTransfer):Unit =
    require(actual!=null && receipt!=null && currentMainOwner.exists(_ eq actual) &&
      originalClaimedOwner.exists((owner,original)=>(owner eq actual) && (original eq receipt)),
      "actual original successful IO owner claim required")
  private[mysql] def requireRegisteredDbFirstMainRead(actual:MySqlDbFirstMainReadBacking):Unit =
    require(actual!=null && (dbFirstMainReadRef.get() eq actual),"original registered schema6 Main backing required")
  private[mysql] def requireCurrentDbFirstMainRead(actual:MySqlDbFirstMainReadBacking):Unit =
    requireDbFirstCaptureOwner()
    require(actual!=null && (dbFirstMainReadRef.get() eq actual) && actual.repositoryEpoch==acceptedRepositoryEpoch,
      "original schema6 Main image retired by repository mutation or another owner")
  private[floatingpod] def claimedDbFirstMainReads:MySqlDbFirstMainReadBacking =
    val value=dbFirstMainReadRef.get()
    requireCurrentDbFirstMainRead(value)
    value.relationNames // 同时核实际claimed Main；bootstrap control不能提前领用。
    value
  private val bootTakeoverTaken=mutable.LinkedHashMap.empty[MySqlDbFirstBootRecallTakeoverIntent,IoCompletion[MySqlOwnedIoReply]]
  private[floatingpod] def beginBootRecallTakeover(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      record:MySqlDbFirstRecallRequestRecord,control:IoOperationTag,execution:IoOperationTag):MySqlDbFirstBootRecallTakeoverIntent =
    require(owner!=null,"actual Boot takeover issuer required")
    claimedDbFirstMainReads.beginBootTakeoverIntent(owner,record,control,execution)
  private[floatingpod] def claimBootRecallTakeover(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,completion:IoCompletion[MySqlOwnedIoReply]):MySqlDbFirstCaptureReservation =
    require(owner!=null && intent!=null && completion!=null && currentMainOwner.exists(_ eq Thread.currentThread()) &&
      bootTakeoverTaken.get(intent).exists(_ eq completion) && completion.tag==intent.controlTag,
      "exact actually taken Boot control completion required")
    val reservation=completion.result match
      case Success(MySqlOwnedIoReply.BootRecallTakenOver(actual,value,epoch))=>
        require((actual eq intent) && value!=null && value.tag==intent.executionTag && epoch==acceptedRepositoryEpoch,
          "actual original Boot C/epoch result required")
        value
      case Failure(cause)=>throw cause
      case _=>throw new IllegalArgumentException("unexpected original Boot takeover result")
    claimedDbFirstMainReads.claimBootTakeoverResult(owner,intent,reservation)
    bootTakeoverTaken.remove(intent)
    reservation
  private[floatingpod] def beginBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit =
    require(owner!=null,"actual Boot decision issuer required")
    claimedDbFirstMainReads.beginBootMainDecision(owner,intent,reservation)
  private[floatingpod] def finishBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,
      reason:Option[com.wi.floatingpod.domain.RecallRefusalReason]):MySqlDbFirstBootRecallMainDecision =
    require(owner!=null,"actual Boot decision issuer required")
    claimedDbFirstMainReads.finishBootMainDecision(owner,intent,reservation,reason)
  private[floatingpod] def activateBootRecallCapture(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      decision:MySqlDbFirstBootRecallMainDecision,reservation:MySqlDbFirstCaptureReservation):Unit =
    requireDbFirstCaptureOwner()
    require(owner!=null && decision!=null,"actual Boot positive capture issuer required")
    claimedDbFirstMainReads.activateBootRecallCapture(owner,decision,reservation)
  private[floatingpod] def detachBootRecallTakeover(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit =
    require(owner!=null,"actual Boot detach issuer required")
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"original Boot cleanup Main required")
    val original=dbFirstMainReadRef.get()
    require(original!=null,"original registered Boot cleanup backing required")
    original.detachBootTakeover(owner,intent,reservation)
  private[floatingpod] def claimBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Vector[MySqlDbFirstRecallRequestRecord] =
    require(owner!=null,"actual Boot metadata owner required")
    owner.requireBootCensusClaim(this)
    require(claimedBootCensusRef.get()==null,"original Boot census already registered")
    val records=claimedDbFirstMainReads.claimBootRecallCensus(owner)
    val session=sessionRef.get() match
      case SessionSlot.DbFirst(actual)=>actual
      case _=>throw new IllegalStateException("actual original Boot census Session missing")
    val census=session.claimedBootRecallCensus(owner)
    require(claimedBootCensusRef.compareAndSet(null,census),"original Boot census registration changed")
    records
  private[floatingpod] def detachBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit =
    require(owner!=null && currentMainOwner.exists(_ eq Thread.currentThread()),"actual Boot metadata cleanup Main required")
    owner.requireBootCensusDetached(this)
    val census=claimedBootCensusRef.get()
    require(census!=null,"original registered Boot census required")
    census.detach(owner)
    require(claimedBootCensusRef.compareAndSet(census,null),"actual Boot census changed during detach")
    recoveredMainStopAcknowledged.set(true)
  /** 失败停止只撤原Main借用；不结算请求、不发行正常final release。 */
  private[floatingpod] def detachStoppedBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit = synchronized {
    require(owner!=null && currentMainOwner.exists(_ eq Thread.currentThread()),"actual stopped Boot cleanup Main required")
    owner.requireStoppedBootCensusDetached(this)
    val census=claimedBootCensusRef.get()
    require(census!=null && !recoveredMainStopAcknowledged.get(),"actual undropped Boot census required")
    census.detachForStoppedMain(owner)
    require(claimedBootCensusRef.compareAndSet(census,null),"actual stopped Boot census changed during detach")
    recoveredMainStopAcknowledged.set(true)
  }
  private[mysql] def requireStoppedRecoveredCleanup(source:MySqlDbFirstSession.RetiredRecallWriterRecovery):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(source!=null && adapterStopping && recoveredMainStopAcknowledged.get() &&
      recoveredMainDataDropped.get() && claimedBootCensusRef.get()==null,
      "actual stopped original service cleanup requires Main census and DATA drop")
    if actual!=null then
      require((actual.source eq source) &&
        (retiredDbFirstRecallHoldRef.get() eq actual.receipt) &&
        (retiredRecallReleaseAckRef.get() eq actual.receipt) && actual.receipt.retainedEntryRetired,
        "actual registered stopped service and retained final ACK required")
    else
      val receipt=retiredDbFirstRecallHoldRef.get()
      require(receipt!=null && (retiredRecallReleaseAckRef.get() eq receipt) && receipt.retainedEntryRetired,
        "actual unregistered stopped service retained final ACK required")
      sessionRef.get() match
        case SessionSlot.DbFirst(parent)=>parent.requireStoppedRecoveryOwner(source,this,receipt)
        case _=>throw new IllegalStateException("actual unregistered stopped service parent missing")
  }
  private def stoppedRecoveredCleanup(actual:RecoveredServiceMainTransfer):Boolean =
    actual!=null && adapterStopping && recoveredMainStopAcknowledged.get() &&
      recoveredMainDataDropped.get() && claimedBootCensusRef.get()==null
  private[mysql] def requireStoppedRecoveredParentCleanup(previous:MySqlDbFirstSession):Unit = synchronized {
    val actual=recoveredDbFirstServiceRef.get()
    require(stoppedRecoveredCleanup(actual) && previous!=null && (actual.predecessor eq previous) &&
      actual.source==null && actual.service!=null && actual.service.hasAdapterCleanup(this),
      "actual stopped service must retain exact old metadata parent")
  }
  /** 原Main撤销读责任不依赖仍然有效的freshness、world或未改变的repository epoch。 */
  private[floatingpod] def dropClaimedDbFirstMainReads():Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"exact claimed schema6 Main drop required")
    val value=dbFirstMainReadRef.get()
    require(value!=null,"original registered schema6 Main backing required for drop")
    value.drop()
    recoveredMainDataDropped.set(true)

  private def requireDbFirstCaptureOwner(): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "capture requires exact current Main")
    require(!adapterStopping,"stopped adapter cannot issue schema6 capture")
    require(!dbFirstApplicationRetired,"schema6 application has retired")
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully),
      "unconsumed bootstrap cannot issue schema6 capture")
    require(blockingRetiredRecall==null,"original retained C must settle before ordinary Main DATA/capture")
    val observed=currentDbFirstMailbox.poll
    require(observed.isCurrent && observed.latest.exists(_.profile == MySqlSchemaAuditProfile.DbFirst1) &&
      io.checkpoint.accepting && io.checkpoint.fatalCause.isEmpty && retainedPlanIssuerRef.get()!=null,
      "original schema6 capture owner is unavailable, expired or stopped")

  private def installCertifiedOrdinaryConnectionSourceForAssembly(
      source: () => VerifiedMySqlConnection
  ): Unit =
    require(source != null, "ordinary verified-connection assembly source is required")
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && !ownerSubmitted && !bootAttempted &&
      bootstrapPreparation.isEmpty && certifiedOrdinaryConnectionSourceForAssembly.isEmpty && freshConnectionSourceForAssembly.isEmpty,
      "ordinary connection assembly source must be installed once before bootstrap preparation or submission")
    certifiedOrdinaryConnectionSourceForAssembly = Some(source)

  private def installFreshConnectionSourceForAssembly(
      source: () => VerifiedMySqlConnection
  ): Unit =
    require(source != null, "Fresh verified-connection assembly source is required")
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && !ownerSubmitted && !bootAttempted &&
      bootstrapPreparation.isEmpty && freshConnectionSourceForAssembly.isEmpty &&
      certifiedOrdinaryConnectionSourceForAssembly.isEmpty,
      "Fresh connection assembly source must be installed once before bootstrap preparation or submission")
    freshConnectionSourceForAssembly = Some(source)

  private def originalCommandPlan(command: MySqlOwnedIoCommand): Option[MySqlOwnedRepositoryOperationPlan] = command match
    case MySqlOwnedIoCommand.PrepareNormalRecall(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.CheckpointNormalRecall(_, _, plan) => Some(plan)
    case MySqlOwnedIoCommand.PrepareControlledDebug(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.CommitControlledDebug(_, _, _, plan) => Some(plan)
    case MySqlOwnedIoCommand.ReadRequestPreview(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.ReadOriginalDeployment(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.Run(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.Transaction(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.PrepareControlledUnbound(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.CommitControlledUnbound(_, _, _, plan) => Some(plan)
    case MySqlOwnedIoCommand.CommitOwnedMode(_, plan) => Some(plan)
    case MySqlOwnedIoCommand.CommitTrackedDrift(_, plan) => Some(plan)
    case _ => None

  def submit(tag: IoOperationTag, command: MySqlOwnedIoCommand): IoSubmission =
    currentExecutionIdentity.requireTag(tag)
    require(command != null, "immutable owned command required")
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully),
      "bootstrap preparation has not been successfully consumed")
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "submit requires exact current adapter Main")
    if adapterStopping then return IoSubmission.RejectedStopped(io.checkpoint.fatalCause.orElse(io.checkpoint.exitFailure))
    require(restrictedSourceRegistration.isEmpty,
      "restricted Boot recovery owns the adapter command route until its source responsibility settles")
    val retainedRecall=retainedDbFirstRecallRef.get()
    if retainedRecall!=null then throw new IllegalStateException(
      "original retained recall seals ordinary command submission",retainedRecall.originalFailure)
    val retiredRecall=blockingRetiredRecall
    if retiredRecall!=null then throw new IllegalStateException(
      "retired original recall retains shared-fault admission hold",retiredRecall.retainedConfirmation.retainedFailure.get)
    MySqlOwnedIoCommand.requireGenericRoute(command)
    command match
      case MySqlOwnedIoCommand.TakeoverBootRecall(intent)=>
        require(intent!=null && intent.controlTag==tag,"Boot read control tag must differ from original C execution")
      case _=>()
    ordinarySubmissionAttempted = true // dedicated bootstrap does not use submit; rejected/UNKNOWN attempts stay visible.
    val dbFirstRecall = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstRecall(reservation) =>
        val issuer = retainedPlanIssuerRef.get()
        require(reservation != null && reservation.tag == tag && issuer != null &&
          (reservation.handle.issuer eq issuer), "DB-first recall submission requires its exact original Session capture and tag")
        Some((issuer, reservation))
      case _ => None
    val journalCaptureContinuation=dbFirstRecall.flatMap((issuer,reservation)=>issuer.journalCaptureTransferFor(reservation))
    val refusalFinalRetirement=command match
      case MySqlOwnedIoCommand.ReleaseRefusalFinal(receipt)=>
        val issuer=requireRefusalFinalReceipt(receipt)
        require(receipt.tag==tag,"refusal final must use original J route")
        Some((issuer,receipt))
      case _=>None
    val journalCaptureRetirement=command match
      case MySqlOwnedIoCommand.RetireJournalForCapture(transfer)=>
        val issuer=retainedPlanIssuerRef.get()
        require(transfer!=null && transfer.tag==tag && issuer!=null && (transfer.handle.issuer eq issuer),
          "journal/C retirement requires original Session transfer/tag")
        Some((issuer,transfer))
      case _=>None
    val dbFirstCheckpoint = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(reservation) =>
        val issuer = retainedPlanIssuerRef.get()
        require(reservation != null && reservation.tag == tag && issuer != null &&
          (reservation.handle.issuer eq issuer), "DB-first checkpoint submission requires its exact original Session capture and tag")
        Some((issuer, reservation))
      case _ => None
    val dbFirstInitial = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,activeLimit) =>
        require(activeLimit>=1 && activeLimit<=256,"explicit configured initial active limit required")
        val issuer = retainedPlanIssuerRef.get()
        require(reservation != null && reservation.tag == tag && issuer != null &&
          (reservation.handle.issuer eq issuer), "DB-first initial activation submission requires its exact original Session capture and tag")
        Some((issuer, reservation))
      case _ => None
    val dbFirstRefusal = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(exclusion)=>
        val issuer=retainedPlanIssuerRef.get()
        require(exclusion!=null && exclusion.routeTag==tag && issuer!=null &&
          (exclusion.handle.issuer eq issuer),"refusal requires original J routing slot and issuer")
        Some((issuer,exclusion))
      case _=>None
    val dbFirstJournal = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(reservation)=>
        val issuer=retainedPlanIssuerRef.get()
        require(reservation!=null && reservation.tag==tag && issuer!=null &&
          (reservation.handle.issuer eq issuer),"journal submission requires original Session reservation/tag")
        Some((issuer,reservation))
      case _=>None
    val dbFirstPreparation = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(reservation) =>
        val issuer=retainedPlanIssuerRef.get()
        require(reservation!=null && reservation.tag==tag && issuer!=null &&
          (reservation.handle.issuer eq issuer), "DB-first preparation submission requires its exact original Session reservation and tag")
        Some((issuer,reservation))
      case _ => None
    val originalPlan = originalCommandPlan(command)
    originalPlan.foreach { plan =>
      require(plan != null && plan.tag == tag && plan.originalRetainedHandle.exists(handle =>
        handle.issuer eq retainedPlanIssuerRef.get()), "business submit requires its actual session-issued original plan")
    }
    command match
      case MySqlOwnedIoCommand.ReleaseDbFirstRecall(receipt) =>
        requireDbFirstDropReceipt(receipt)
        require(receipt.tag == tag, "DB-first release must reuse original operation tag")
      case MySqlOwnedIoCommand.ReleaseRejectedPreparationMainRead(confirmation) =>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original rejected preparation control required")
      case MySqlOwnedIoCommand.RetireRejectedPreparation(confirmation) =>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original rejected preparation retirement required")
      case MySqlOwnedIoCommand.ReleaseRefusalMainRead(confirmation)=>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original refusal read control required")
      case MySqlOwnedIoCommand.ReleaseJournalMainRead(confirmation)=>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original journal read control required")
      case MySqlOwnedIoCommand.ReleasePreparationMainRead(confirmation) =>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original preparation read control required")
      case MySqlOwnedIoCommand.ReleaseDbFirstPreparationFinal(receipt) =>
        requireDbFirstPreparationFinalReceipt(receipt)
        require(receipt.tag==tag,"initial retirement must reuse original operation tag")
      case MySqlOwnedIoCommand.ReleaseCheckpointMainRead(confirmation) =>
        require(confirmation!=null && confirmation.tag==tag &&
          (confirmation.handle.issuer eq retainedPlanIssuerRef.get()),"original checkpoint read control required")
      case MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(receipt) =>
        requireDbFirstCheckpointFinalReceipt(receipt)
        require(receipt.tag==tag,"initial retirement must reuse original operation tag")
      case MySqlOwnedIoCommand.ReleaseDbFirstInitialPublication(receipt) =>
        requireDbFirstInitialPublicationReceipt(receipt)
        require(receipt.tag==tag,"initial retirement must reuse original operation tag")
      case MySqlOwnedIoCommand.ReleaseModeProjection(original, receipt) => requireOriginalDropReceipt(original, receipt)
      case MySqlOwnedIoCommand.ReleaseDriftProjection(original, receipt) => requireOriginalDropReceipt(original, receipt)
      case MySqlOwnedIoCommand.ReleaseRequestRead(original, receipt) => requireOriginalDropReceipt(original, receipt)
      case MySqlOwnedIoCommand.ReleaseRetainedPlan(original, receipt) => requireOriginalDropReceipt(original, receipt)
      case _ => ()
    val channel = command match
      case MySqlOwnedIoCommand.ReadRequestPreview(_, _) | MySqlOwnedIoCommand.ReadOriginalDeployment(_, _) |
          MySqlOwnedIoCommand.ReleaseRequestRead(_, _) => MySqlCompletionChannel.RequestRead
      case MySqlOwnedIoCommand.CommitTrackedDrift(_, _) | MySqlOwnedIoCommand.ReleaseDriftProjection(_, _) => MySqlCompletionChannel.Drift
      case _ => MySqlCompletionChannel.Ordinary
    val retainsPlan = command match
      case MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(_) | MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(_) | MySqlOwnedIoCommand.ExecuteDbFirstRecall(_) | MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(_) | MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(_,_) |
          MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(_) => true
      case MySqlOwnedIoCommand.PrepareNormalRecall(_, _) | MySqlOwnedIoCommand.CheckpointNormalRecall(_, _, _) => true
      case MySqlOwnedIoCommand.PrepareControlledDebug(_, _) | MySqlOwnedIoCommand.CommitControlledDebug(_, _, _, _) |
          MySqlOwnedIoCommand.ReadRequestPreview(_, _) | MySqlOwnedIoCommand.ReadOriginalDeployment(_, _) |
          MySqlOwnedIoCommand.Run(_, _) | MySqlOwnedIoCommand.Transaction(_, _) |
          MySqlOwnedIoCommand.PrepareControlledUnbound(_, _) | MySqlOwnedIoCommand.CommitControlledUnbound(_, _, _, _) |
          MySqlOwnedIoCommand.CommitOwnedMode(_, _) | MySqlOwnedIoCommand.CommitTrackedDrift(_, _) => true
      case _ => false
    val releaseOriginal = command match
      case MySqlOwnedIoCommand.ReleaseDbFirstRecall(receipt) => Some(receipt.tag)
      case MySqlOwnedIoCommand.ReleaseRejectedPreparationMainRead(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.RetireRejectedPreparation(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.ReleaseCheckpointMainRead(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(receipt) => Some(receipt.tag)
      case MySqlOwnedIoCommand.ReleaseRefusalFinal(receipt) => Some(receipt.tag)
      case MySqlOwnedIoCommand.ReleaseRefusalMainRead(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.ReleaseJournalMainRead(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.RetireJournalForCapture(transfer)=>Some(transfer.tag)
      case MySqlOwnedIoCommand.ReleasePreparationMainRead(confirmation) => Some(confirmation.tag)
      case MySqlOwnedIoCommand.ReleaseDbFirstPreparationFinal(receipt) => Some(receipt.tag)
      case MySqlOwnedIoCommand.ReleaseDbFirstInitialPublication(receipt) => Some(receipt.tag)
      case MySqlOwnedIoCommand.ReleaseModeProjection(original, _) => Some(original)
      case MySqlOwnedIoCommand.ReleaseRequestRead(original, _) => Some(original)
      case MySqlOwnedIoCommand.ReleaseRetainedPlan(original, _) => Some(original)
      case MySqlOwnedIoCommand.ReleaseDriftProjection(original, _) => Some(original)
      case _ => None
    // 溢出在入队前拒绝；Accepted 后即使 router 后处理失败也不能继续冒认旧 Boot 数据。
    val nextEpoch = if MySqlBootReadEpoch.mayChangeRepository(command) then
      Math.addExact(acceptedRepositoryEpoch, 1L) else acceptedRepositoryEpoch
    dbFirstRecall.foreach((issuer, reservation) => issuer.requireDbFirstRecallSubmission(reservation)); dbFirstCheckpoint.foreach((issuer, reservation) => issuer.requireDbFirstCheckpointSubmission(reservation))
    dbFirstInitial.foreach((issuer, reservation) => issuer.requireDbFirstInitialActivationSubmission(reservation))
    dbFirstJournal.foreach((issuer,reservation)=>issuer.requireDbFirstRecallJournalSubmission(reservation))
    dbFirstRefusal.foreach((issuer,exclusion)=>issuer.requireRecallRefusalSubmission(exclusion))
    refusalFinalRetirement.foreach((issuer,receipt)=>issuer.requireRefusalFinalSubmission(receipt))
    journalCaptureRetirement.foreach((issuer,transfer)=>issuer.requireJournalCaptureTransferSubmission(transfer))
    dbFirstPreparation.foreach((issuer, reservation) => issuer.requireDbFirstInitialPreparationSubmission(reservation))
    val reserved = try
      if journalCaptureContinuation.nonEmpty then
        completionRouter.beginJournalCapture(journalCaptureContinuation.get)
        if Option(ordinarySaveAdmission.get()).exists(_.awaitingOrdinarySuccessor()) then
          completionRouter.submitted(tag,IoSubmission.RejectedCapacity)
          false
        else true
      else if MySqlBootReadEpoch.mayChangeRepository(command) &&
          Option(ordinarySaveAdmission.get()).exists(_.awaitingOrdinarySuccessor()) then false
      else if dbFirstRefusal.nonEmpty then
        completionRouter.beginRecallRefusal(dbFirstRefusal.get._2)
      else if refusalFinalRetirement.nonEmpty then
        completionRouter.beginRefusalFinal(refusalFinalRetirement.get._2)
        true
      else releaseOriginal match
        case Some(original) =>
          require(original == tag, "projection release reuses its exact original accepted operation slot")
          completionRouter.beginRelease(tag, channel)
          true
        case None => completionRouter.begin(tag, channel, retainsPlan)
    catch
      case cause: Throwable =>
        try refusalFinalRetirement.foreach((issuer,receipt)=>issuer.submissionUnknownRefusalFinal(receipt,cause))
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        try journalCaptureRetirement.foreach((issuer,transfer)=>issuer.submissionUnknownJournalCaptureTransfer(transfer,cause))
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        try { dbFirstRecall.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstRecall(reservation, cause)); dbFirstCheckpoint.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstCheckpoint(reservation, cause)); dbFirstInitial.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstInitialActivation(reservation, cause)); dbFirstJournal.foreach((issuer,reservation)=>issuer.submissionUnknownDbFirstRecallJournal(reservation,cause));; dbFirstRefusal.foreach((issuer,exclusion)=>issuer.submissionUnknownRecallRefusal(exclusion,cause)); dbFirstPreparation.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstInitialPreparation(reservation, cause)) }
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause
    if !reserved then
      originalPlan.foreach(plan => retainedPlanIssuerRef.get().submitted(plan, IoSubmission.RejectedCapacity))
      dbFirstRecall.foreach((issuer, reservation) => issuer.submittedDbFirstRecall(reservation, IoSubmission.RejectedCapacity)); dbFirstCheckpoint.foreach((issuer, reservation) => issuer.submittedDbFirstCheckpoint(reservation, IoSubmission.RejectedCapacity))
      dbFirstInitial.foreach((issuer, reservation) => issuer.submittedDbFirstInitialActivation(reservation, IoSubmission.RejectedCapacity))
      dbFirstJournal.foreach((issuer,reservation)=>issuer.submittedDbFirstRecallJournal(reservation,IoSubmission.RejectedCapacity)); dbFirstRefusal.foreach((issuer,exclusion)=>issuer.submittedRecallRefusal(exclusion,IoSubmission.RejectedCapacity))
      refusalFinalRetirement.foreach((issuer,receipt)=>issuer.submittedRefusalFinal(receipt,IoSubmission.RejectedCapacity))
      journalCaptureRetirement.foreach((issuer,transfer)=>issuer.submittedJournalCaptureTransfer(transfer,IoSubmission.RejectedCapacity))
      dbFirstPreparation.foreach((issuer, reservation) => issuer.submittedDbFirstInitialPreparation(reservation, IoSubmission.RejectedCapacity))
      IoSubmission.RejectedCapacity
    else
      var barrier = Option.empty[com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission#Mutation]
      var submitResolved = false
      try
        if MySqlBootReadEpoch.mayChangeRepository(command) then
          barrier = Option(ordinarySaveAdmission.get()).map(_.reserveMutation())
        val result = io.submit(tag, Work(tag, command, nextEpoch, barrier))
        barrier.foreach(m => ordinarySaveAdmission.get().submitted(m, result == IoSubmission.Accepted))
        submitResolved = true
        if result == IoSubmission.Accepted then acceptedRepositoryEpoch = nextEpoch
        completionRouter.submitted(tag, result)
        originalPlan.foreach(plan => retainedPlanIssuerRef.get().submitted(plan, result))
        dbFirstRecall.foreach((issuer, reservation) => issuer.submittedDbFirstRecall(reservation, result)); dbFirstCheckpoint.foreach((issuer, reservation) => issuer.submittedDbFirstCheckpoint(reservation, result))
        dbFirstInitial.foreach((issuer, reservation) => issuer.submittedDbFirstInitialActivation(reservation, result))
        dbFirstJournal.foreach((issuer,reservation)=>issuer.submittedDbFirstRecallJournal(reservation,result)); dbFirstRefusal.foreach((issuer,exclusion)=>issuer.submittedRecallRefusal(exclusion,result))
        refusalFinalRetirement.foreach((issuer,receipt)=>issuer.submittedRefusalFinal(receipt,result))
        journalCaptureRetirement.foreach((issuer,transfer)=>issuer.submittedJournalCaptureTransfer(transfer,result))
        dbFirstPreparation.foreach((issuer, reservation) => issuer.submittedDbFirstInitialPreparation(reservation, result))
        ownerSubmitted = true
        result
      catch
        case cause: Throwable =>
          try
            if !submitResolved then barrier.foreach(m => ordinarySaveAdmission.get().submissionUnknown(m, cause))
          catch case cleanup: Throwable =>
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(cause, cleanup)
          try refusalFinalRetirement.foreach((issuer,receipt)=>issuer.submissionUnknownRefusalFinal(receipt,cause))
          catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          try journalCaptureRetirement.foreach((issuer,transfer)=>issuer.submissionUnknownJournalCaptureTransfer(transfer,cause))
          catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          try { dbFirstRecall.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstRecall(reservation, cause)); dbFirstCheckpoint.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstCheckpoint(reservation, cause)); dbFirstInitial.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstInitialActivation(reservation, cause)); dbFirstJournal.foreach((issuer,reservation)=>issuer.submissionUnknownDbFirstRecallJournal(reservation,cause));; dbFirstRefusal.foreach((issuer,exclusion)=>issuer.submissionUnknownRecallRefusal(exclusion,cause)); dbFirstPreparation.foreach((issuer, reservation) => issuer.submissionUnknownDbFirstInitialPreparation(reservation, cause)) }
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          try completionRouter.submissionFailed(tag, cause)
          catch case cleanup: Throwable =>
            com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(cause, cleanup)
          throw cause

  def drain(max: Int): Vector[IoCompletion[MySqlOwnedIoReply]] =
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully),
      "bootstrap holder exclusively owns its unconsumed completion")
    routeAvailableCompletions(max)
    completionRouter.take(MySqlCompletionChannel.Ordinary, max).map { completion =>
      completion.result match
        case Success(MySqlOwnedIoReply.BootRecallTakenOver(intent,reservation,epoch))=>
          require(intent!=null && completion.tag==intent.controlTag && reservation!=null && !bootTakeoverTaken.contains(intent),
            "original Boot control result may be taken once")
          bootTakeoverTaken.put(intent,completion)
        case _=>()
      if modeProjectionReleaseTags.remove(completion.tag) then
        completedModeProjectionReleaseTags += completion.tag
        while completedModeProjectionReleaseTags.size > math.max(1, capacity) do
          completedModeProjectionReleaseTags -= completedModeProjectionReleaseTags.head
        completion.result match
          case Failure(cause) =>
            IoCompletion(completion.tag, Success(MySqlOwnedIoReply.ModeProjectionReleaseFailed(completion.tag, cause)))
          case _ => completion
      else completion
    }
  /** 同原共享router，只有专用request-read消费者取得自己的Accepted completion。 */
  private[floatingpod] def drainRequestRead(max: Int): Vector[IoCompletion[MySqlOwnedIoReply]] =
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully),
      "unconsumed bootstrap owns completion")
    routeAvailableCompletions(max)
    val completions=completionRouter.take(MySqlCompletionChannel.RequestRead, max)
    captureRestrictedSourceCompletions(completions)

  private[floatingpod] def submitRequestReadRelease(tag: IoOperationTag,
      receipt: MySqlRetainedPlanDropReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "release requires exact actual Main")
    currentExecutionIdentity.requireTag(tag)
    requireOriginalDropReceipt(tag, receipt)
    submit(tag, MySqlOwnedIoCommand.ReleaseRequestRead(tag, receipt))

  private def routeAvailableCompletions(max: Int): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "completion router requires exact current Main")
    if max<=0 then throw new IllegalArgumentException("positive completion routing bound required")
    val sourcePending=restrictedSourceR0Pending.get()
    val sourceSlots=if sourcePending!=null && sourcePending.needsCompletionSlot then 1 else 0
    val routedSlots=completionRouter.routableSlots(max)
    val totalSlots=Math.addExact(sourceSlots,routedSlots)
    if totalSlots>capacity then
      throw new IllegalStateException("exact completion routing owners exceed the original bounded IO capacity")
    if totalSlots>0 then
      val partitionCharge=if sourcePending==null then 0L
        else Math.addExact(256L,Math.multiplyExact(totalSlots.toLong,64L))
      if partitionCharge>0L then sourcePending.protocolRoot.reserveTemporaryMemory(partitionCharge,
        "restricted SOURCE R0 exact-tag completion partition workspace BEFORE drain/allocation")
      var primary:Throwable=null
      try
        val batch=io.drain(totalSlots)
        if sourcePending==null then completionRouter.routeBatch(batch)
        else
          val (source,other)=batch.partition(_.tag==sourcePending.tag)
          if source.nonEmpty then source.foreach(sourcePending.retainCompletion)
          if other.nonEmpty then completionRouter.routeBatch(other)
      catch
        case cause:Throwable=>primary=cause;throw cause
      finally
        if partitionCharge>0L then
          try sourcePending.protocolRoot.releaseTemporaryMemory(partitionCharge,
            "restricted SOURCE R0 exact-tag completion partition workspace complete")
          catch case cleanup:Throwable=>
            if primary==null then throw cleanup
            else
              sourcePending.retainRoutingFailure(primary)
              sourcePending.retainRoutingSecondaryFailure(cleanup)
    observeRetiredRecallProducerDrain()
  private[floatingpod] def drainTrackedDrift(max: Int): Vector[IoCompletion[MySqlOwnedIoReply]] =
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.bootstrapCompletionConsumedSuccessfully), "unconsumed bootstrap completion owns its mailbox")
    routeAvailableCompletions(max)
    completionRouter.take(MySqlCompletionChannel.Drift, max).map { completion =>
      if driftProjectionReleaseTags.remove(completion.tag) then
        completedDriftProjectionReleaseTags += completion.tag
        while completedDriftProjectionReleaseTags.size > math.max(1, capacity) do
          completedDriftProjectionReleaseTags -= completedDriftProjectionReleaseTags.head
        completion.result match
          case Failure(cause) =>
            IoCompletion(completion.tag, Success(MySqlOwnedIoReply.DriftProjectionReleaseFailed(completion.tag, cause)))
          case _ => completion
      else completion
    }
  private[floatingpod] def retainedCompletionCount: Int =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "completion count requires current Main")
    completionRouter.retainedCount

  /** Main 仅能提交原不可变 tag；adapter 核对其 boot/worker generation 与一次性状态后入队。 */
  /** 仅下一Main END在Entry真实drop后结算明确Rejected；无SQL/无OwnedIO control slot。 */
  private[floatingpod] def recordRejectedPreparationDrop(reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstRejectedPreparationReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && reservation!=null,
      "original Main rejected preparation required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (reservation.handle.issuer eq issuer),"original preparation issuer required")
    issuer.recordRejectedPreparationDrop(reservation)

  private[floatingpod] def cancelRejectedPreparation(receipt:MySqlDbFirstRejectedPreparationReceipt):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && receipt!=null,
      "original Main rejected preparation cleanup required")
    val issuer=retainedPlanIssuerRef.get()
    require(issuer!=null && (receipt.handle.issuer eq issuer),"original preparation issuer required")
    issuer.cancelRejectedPreparation(receipt)

  private[floatingpod] def cancelExplicitlyRejectedPlan(handle: MySqlRetainedPlanHandle): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "Rejected settlement requires actual current Main")
    require(handle != null && retainedPlanIssuerRef.get() != null &&
      (handle.issuer eq retainedPlanIssuerRef.get()), "original actual issuer handle required")
    retainedPlanIssuerRef.get().cancelExplicitlyRejected(handle)

  private[floatingpod] def recordMainPlanDrop(handle: MySqlRetainedPlanHandle): MySqlRetainedPlanDropReceipt =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "drop requires exact current Main")
    val issuer = retainedPlanIssuerRef.get()
    require(issuer != null && handle != null && (handle.issuer eq issuer), "actual original issuer handle required")
    issuer.recordMainDrop(handle)

  private def requireOriginalDropReceipt(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): Unit =
    require(receipt != null && receipt.tag == tag && retainedPlanIssuerRef.get() != null &&
      (receipt.handle.issuer eq retainedPlanIssuerRef.get()), "release lacks original actual Main drop receipt")

  private[floatingpod] def submitRetainedPlanRelease(tag: IoOperationTag,
      receipt: MySqlRetainedPlanDropReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "release requires exact current Main")
    currentExecutionIdentity.requireTag(tag)
    requireOriginalDropReceipt(tag, receipt)
    submit(tag, MySqlOwnedIoCommand.ReleaseRetainedPlan(tag, receipt))

  private[floatingpod] def submitModeProjectionRelease(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "projection release submission requires the adapter's exact current Main thread")
    currentExecutionIdentity.requireTag(tag)
    requireOriginalDropReceipt(tag, receipt)
    require(!completedModeProjectionReleaseTags.contains(tag), "completed mode projection release tag cannot be reused")
    require(modeProjectionReleaseTags.add(tag), "mode projection release was already submitted for this exact operation")
    try
      val result = submit(tag, MySqlOwnedIoCommand.ReleaseModeProjection(tag, receipt))
      if result != IoSubmission.Accepted then modeProjectionReleaseTags.remove(tag)
      else ownerSubmitted = true
      result
    catch
      case cause: Throwable => throw cause // enqueue outcome is retained as unknown; do not mint or retry a new tag.

  private[floatingpod] def submitDriftProjectionRelease(tag: IoOperationTag, receipt: MySqlRetainedPlanDropReceipt): IoSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "projection release submission requires the adapter's exact Main thread")
    currentExecutionIdentity.requireTag(tag)
    requireOriginalDropReceipt(tag, receipt)
    require(!completedDriftProjectionReleaseTags.contains(tag), "completed mode projection release tag cannot be reused")
    require(driftProjectionReleaseTags.add(tag), "mode projection release was already submitted for this exact operation")
    try
      val result = submit(tag, MySqlOwnedIoCommand.ReleaseDriftProjection(tag, receipt))
      if result != IoSubmission.Accepted then driftProjectionReleaseTags.remove(tag)
      else ownerSubmitted = true
      result
    catch
      case cause: Throwable => throw cause // enqueue outcome is retained as unknown; do not mint or retry a new tag.

  def issueOwnerTransfer(target: Thread, bootTag: IoOperationTag): IoOwnerTransfer =
    identity.requireTag(bootTag)
    require(bootstrapPreparation.isEmpty || bootstrapPreparation.exists(_.liveTransferPermitted),
      "bootstrap application payload must be live before transfer")
    val receipt = io.issueOwnerTransfer(target, bootTag)
    retainedPlanIssuerRef.synchronized { Option(retainedPlanIssuerRef.get()).foreach(_.suspendMain()) }
    sessionRef.get() match
      case SessionSlot.DeploymentReceipt(value) => value.suspendRecallSourceMain()
      case _ => ()
    currentMainOwner = None
    receipt
  def claimOwnerTransfer(receipt: IoOwnerTransfer): Unit =
    io.claimOwnerTransfer(receipt)
    retainedPlanIssuerRef.synchronized { Option(retainedPlanIssuerRef.get()).foreach(_.claimMain(Thread.currentThread())) }
    sessionRef.get() match
      case SessionSlot.DeploymentReceipt(value) => value.claimRecallSourceMain()
      case _ => ()
    currentMainOwner = Some(Thread.currentThread())
    originalClaimedOwner=Some((Thread.currentThread(),receipt))
    Option(dbFirstMainReadRef.get()).foreach(_.claimMain(Thread.currentThread(),receipt))
  def abortOwnerTransfer(receipt: IoOwnerTransfer, originalCause: Throwable): Unit =
    io.abortOwnerTransfer(receipt, originalCause)
    sessionRef.get() match
      case SessionSlot.DeploymentReceipt(value) => value.abortRecallSourceMain()
      case _ => ()
    currentMainOwner = Some(initialMain)
    retainedPlanIssuerRef.synchronized { Option(retainedPlanIssuerRef.get()).foreach(_.stop()) }
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()
  def ownerTransferAbortCause: Option[Throwable] = io.ownerTransferAbortCause
  /** Main仍持真实未结算引用时冻结原issuer；不签stop/物理完成或DB release资格。 */
  private[floatingpod] def retainUnsettledStop(originalCause: Throwable): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "stop responsibility requires exact current Main")
    require(originalCause != null, "original unsettled stop cause required")
    retainedPlanIssuerRef.synchronized {
      val issuer=Option(retainedPlanIssuerRef.get()).getOrElse(
        throw new IllegalStateException("actual retained issuer unavailable for unsettled stop"))
      val state=io.checkpoint
      val mainDropped=recoveredMainStopAcknowledged.get() && recoveredMainDataDropped.get() && claimedBootCensusRef.get()==null
      // 这里只核无command责任并封空issuer；maintenance真实返回由唯一worker exit顺序证明。
      val noCommandResponsibility= !state.running && state.queued==0 && state.completions==0 && state.reserved==0
      if !(mainDropped && noCommandResponsibility && issuer.stopEmptyDbFirstAfterFailure()) then issuer.freeze(originalCause)
    }

  def quiesce(): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"operation requires current owner Main thread")
    synchronized { adapterStopping=true }
    io.quiesce()
    retainedPlanIssuerRef.synchronized { Option(retainedPlanIssuerRef.get()).foreach(_.stop()) }
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()
    stopRecoveredMailbox()
  /** 原提交历史的必要条件；不证明 Fresh certification、资源关闭或签发退出许可。 */
  private[floatingpod] def requireNoPostBootstrapWork(): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "Fresh submission history requires original Main")
    require(!ordinarySubmissionAttempted, "Fresh completion cannot follow post-bootstrap work")
  private[floatingpod] def quiesceForFreshCompletion(
      callbacks: com.wi.floatingpod.bootstrap.ProductionDedicatedBootstrapCallbacks,
      drop: com.wi.floatingpod.server.MainBootProjection.FreshReadDrop): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && callbacks != null && drop != null,
      "original Main Fresh completion producer required")
    callbacks.requireFreshMainInputReleased(this, drop)
    requireNoPostBootstrapWork()
    require(repositorySubmissionEpoch == drop.epoch && io.checkpoint.fatalCause.isEmpty,
      "Fresh completion cannot follow post-bootstrap work or worker failure")
    retainedPlanIssuerRef.synchronized {
      Option(retainedPlanIssuerRef.get()).getOrElse(
        throw new IllegalStateException("original Fresh retained-plan issuer missing")).sealUnusedForFreshCompletion()
    }
    val handoff = new MySqlOwnedIoAdapter.FreshCompletionHandoff(drop.snapshot, drop.tag, drop.epoch)
    require(freshCompletionHandoff.compareAndSet(null, handoff), "Fresh completion handoff is one-shot")
    quiesce()
  /** 原Main登记已消费回执并已进入Runtime phase5后，先stop issuer再唤醒退出worker。 */
  private[floatingpod] def quiesceForOrdinaryCompletion(callbacks: com.wi.floatingpod.bootstrap.ProductionDedicatedBootstrapCallbacks,
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime,
      receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded): Unit =
    if currentMainOwner.isEmpty || (currentMainOwner.get ne Thread.currentThread()) then throw new IllegalStateException("actual claimed Ordinary Main required")
    callbacks.requireOrdinaryWorkerHandoff(this,runtime,receipt)
    retainedPlanIssuerRef.synchronized {
      val issuer=retainedPlanIssuerRef.get()
      if issuer == null then throw new IllegalStateException("original Ordinary issuer missing")
      if !freshCompletionHandoff.compareAndSet(null,receipt) then throw new IllegalStateException("original terminal handoff is one-shot")
      issuer.stop()
    }
    io.quiesce()
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()

  private var dbFirstApplicationRetired=false
  /** 两阶段holder/Future只在原control退栈及Main DATA撤销后一起退休，不借旧Ordinary结果协议。 */
  private[floatingpod] def retireDbFirstApplicationPreparation(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      actualPreparation:BootstrapPreparation,original:MySqlOwnedIoReply.DbFirstSqlWriterAcquired,
      receipt:Object):Unit =
    callbacks.requireDbFirstApplicationRetirement(this,runtime,actualPreparation,original,receipt)
    require(!dbFirstApplicationRetired && currentMainOwner.exists(_ eq Thread.currentThread()) &&
      bootstrapPreparation.exists(_ eq actualPreparation) && io.ownerTransferAbortCause.isEmpty &&
      actualPreparation.isDbFirstPreparation && actualPreparation.bootstrapCompletionConsumedSuccessfully &&
      (actualPreparation.requireApplicationValue(callbacks.control) eq original),
      "original successful two-stage schema6 preparation required")
    callbacks.control.requireOrdinaryApplicationPreparation(callbacks,receipt,original)
    callbacks.control.retireOrdinaryApplicationPreparation(callbacks,receipt,original)
    bootstrapPreparation=None
    dbFirstApplicationRetired=true

  private[floatingpod] def quiesceForDbFirstCompletion(
      callbacks:com.wi.floatingpod.bootstrap.ProductionDbFirstBootstrapCallbacks,
      runtime:com.wi.floatingpod.network.DbFirstDedicatedRuntime,
      receipt:com.wi.floatingpod.network.DbFirstDedicatedRuntime.MainConsumersEnded):Unit =
    callbacks.requireDbFirstWorkerHandoff(this,runtime,receipt)
    require(dbFirstApplicationRetired && bootstrapPreparation.isEmpty &&
      currentMainOwner.exists(_ eq Thread.currentThread()),"original retired schema6 application required")
    synchronized { adapterStopping=true }
    retainedPlanIssuerRef.synchronized {
      val issuer=retainedPlanIssuerRef.get()
      require(issuer!=null,"original schema6 issuer missing at final handoff")
      issuer.stop()
    }
    io.quiesce()
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()
    stopRecoveredMailbox()

  def awaitExitOffMain(timeout: Long, unit: TimeUnit): Boolean = io.awaitExitOffMain(timeout, unit)
  /** 仅在 exact owned worker join 后返回 IO-generation worker termination proof，不声明其外部资源已恢复或清理。 */
  def awaitWorkerTerminationOffMain(timeout: Long, unit: TimeUnit): MySqlOwnedIoWorkerTerminationWait =
    io.awaitWorkerTerminationOffMain(timeout, unit) match
      case com.wi.floatingpod.server.io.IoWorkerTerminationWait.Terminated(workerProof) =>
        val proof = MySqlOwnedIoWorkerTerminationProof.issue(this, identity, io, workerTerminationProofIssuer, workerProof)
        requireOwnWorkerTerminationProof(proof)
        MySqlOwnedIoWorkerTerminationWait.Terminated(proof)
      case com.wi.floatingpod.server.io.IoWorkerTerminationWait.TimedOut => MySqlOwnedIoWorkerTerminationWait.TimedOut
      case com.wi.floatingpod.server.io.IoWorkerTerminationWait.Interrupted(cause) =>
        MySqlOwnedIoWorkerTerminationWait.Interrupted(cause)

  /** 验证证明属于此 exact adapter incarnation；不会关闭 session 或清除其他责任。 */
  def requireOwnWorkerTerminationProof(proof: MySqlOwnedIoWorkerTerminationProof): Unit =
    require(proof != null, "owned IO worker termination proof required")
    val workerProof = proof.requireBinding(this, identity, io, workerTerminationProofIssuer)
    io.requireWorkerTerminationProof(workerProof)
  def checkpoint: MySqlOwnedIoCheckpoint = MySqlOwnedIoCheckpoint(io.checkpoint, Option(resourceObservation.get()), Option(controlledAfterAckFailure.get()),
    Option(preparedAfterAckFailure.get()), Option(ownedModeExecutionFailure.get()), Option(driftExecutionFailure.get()),
    retainedPlanIssuerRef.synchronized { Option(retainedPlanIssuerRef.get()).map(_.observation) })
  /** 任意 tick 可 nonblocking poll；不能 renew、延长期限或取得 SQL/physical/GameReady 权限。 */
  def pollAdmissionMaintenance: MySqlOwnedMaintenanceObservation = maintenanceMailbox.poll

  /** 原Boot link只选择其独立mailbox；业务mailbox及plan issuer在恢复期间保持未发行。 */
  private[floatingpod] def pollRecallMaintenance(
      original: MySqlOwnedIoAdapter.RecallRecoveryLink): MySqlOwnedMaintenanceObservation =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && original != null &&
      (recallRecoveryLink.get() eq original) && (original.adapter eq this),
      "exact claimed Main and original restricted Boot link required")
    recallMaintenanceMailbox.poll

  /** 原Projection/Callbacks keeper在首个END前登记唯一受限来源路由。 */
  private[floatingpod] def registerRestrictedSourceContinuation(
      projection:com.wi.floatingpod.server.MainBootProjection,
      lease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      server:net.minecraft.server.MinecraftServer,reply:MySqlProductionBootReply,
      exactLink:MySqlOwnedIoAdapter.RecallRecoveryLink,
      exactRoot:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      exactCustody:com.wi.floatingpod.server.NormalRecallRecoverySourceCustody,
      exactProfile:SourceLimitProfile,originalEpoch:Long):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && projection!=null && lease!=null &&
      server!=null && (server.getServerThread eq Thread.currentThread()) && reply!=null && exactLink!=null &&
      (exactLink.adapter eq this) && (exactLink.identity eq identity) && exactRoot!=null && exactCustody!=null &&
      exactProfile!=null && originalEpoch>=0L,
      "exact claimed Main and original restricted source continuation chain required")
    require(recallRecoveryLink.get() eq exactLink, "restricted source continuation link is not the adapter's original link")
    require(exactLink.sourceAllocation.exists(_ eq exactRoot), "restricted source continuation lost its original allocation root")
    val (bootId,bootGeneration,connectionGeneration,bindingId,writerEpoch)=
      MySqlProductionBootReply.sessionIdentity(reply)
    require(bootId==identity.bootId && bootGeneration==identity.bootGeneration &&
      connectionGeneration==identity.connectionGeneration && bindingId==exactLink.bindingId &&
      writerEpoch==exactLink.writerEpoch && (MySqlProductionBootReply.actualSourceLimitProfile(reply) eq exactProfile),
      "restricted source continuation differs from the exact original Boot/fence/profile")
    require(repositorySubmissionEpoch==originalEpoch && !ordinarySubmissionAttempted &&
      retainedPlanIssuerRef.get()==null && completionRouter.retainedCount==0 && completionRouter.failure.isEmpty,
      "restricted source continuation must register before any unrelated accepted/rejected IO responsibility")
    val checkpoint=io.checkpoint
    require(checkpoint.accepting && !checkpoint.stopped && checkpoint.fatalCause.isEmpty && checkpoint.exitFailure.isEmpty,
      "restricted source adapter is not accepting its original continuation")
    lease.requireResponsibilityOwner(server,exactLink)
    require(restrictedSourceRegistration.isEmpty && restrictedSourceSettledEpoch.isEmpty,
      "restricted source continuation registration is one-shot")
    val sourceProtectionData=lease.sourceProtectionData()
    require(sourceProtectionData!=null,"exact retained source protection view is required")
    restrictedSourceRegistration=Some(RestrictedSourceRegistration(
      projection,lease,server,reply,exactLink,exactRoot,exactCustody,exactProfile,originalEpoch,
      sourceProtectionData))
    restrictedSourceSettledEpoch=Some(originalEpoch)

  /** 主线程提交严格locator的只读Full26请求；已有计划或Admission scope冲突时无槽位DEFER。 */
  private[floatingpod] def submitRestrictedSourceReservePlan(
      tag:IoOperationTag,
      exactParentReference:MySqlNormalRecallRecoveryReference,
      exactCarrier:MySqlBootSourceProtectionIndex.Carrier,
      exactDomainObservation:SourceDomainCapture.CatalogDomainObservation,
      loadedRootPlan:SourceDomainCapture.RootPlan,
      childWorkflow:UUID,
      reserveOperation:UUID,
      plannedWrite:UUID,
      writeGeneration:Long,
      expectedSourceObservation:com.wi.floatingpod.server.MainBootProjection.SourceContinuationObservation
  ):MySqlRestrictedSourceReservePlanSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"restricted source plan requires exact claimed Main")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    Option(restrictedSourceFailure).foreach(throw _)
    if restrictedSourcePlanPending.get()!=null || restrictedSourcePlan.nonEmpty ||
        restrictedSourcePlanDropPending.get()!=null then
      return MySqlRestrictedSourceReservePlanSubmission.Deferred
    requireRestrictedSourceActionNoUnresolved(registered.link,registered.originalEpoch)
    identity.requireTag(tag)
    require(exactParentReference!=null && registered.link.references.exists(_ eq exactParentReference),
      "source reserve plan must retain one exact linked original parent reference")
    require(exactCarrier!=null && exactDomainObservation!=null && loadedRootPlan!=null &&
      expectedSourceObservation!=null && childWorkflow!=null && reserveOperation!=null && plannedWrite!=null &&
      (childWorkflow.getMostSignificantBits | childWorkflow.getLeastSignificantBits)!=0L &&
      (reserveOperation.getMostSignificantBits | reserveOperation.getLeastSignificantBits)!=0L &&
      (plannedWrite.getMostSignificantBits | plannedWrite.getLeastSignificantBits)!=0L && writeGeneration>=0L,
      "exact source locator, nonzero workflow/write identities, and nonnegative generation required")
    registered.lease.requireExactDomainObservation(expectedSourceObservation,exactDomainObservation)
    require(expectedSourceObservation.settledRepositoryEpoch==registered.originalEpoch &&
      repositorySubmissionEpoch==registered.originalEpoch && restrictedSourceSettledEpoch.contains(registered.originalEpoch),
      "readonly source planning requires the original still-settled restricted Boot epoch")
    var hasExactRootPlan=false
    val exactRootPlans=exactDomainObservation.roots().iterator()
    while exactRootPlans.hasNext && !hasExactRootPlan do hasExactRootPlan=exactRootPlans.next() eq loadedRootPlan
    val sourceCarrierLocator=registered.lease.sourceCarrierLocator(expectedSourceObservation,exactDomainObservation,
      exactCarrier.chunk.dimension,exactCarrier.chunk.x,exactCarrier.chunk.z,exactCarrier)
    require((sourceCarrierLocator.requireFor(registered.lease,expectedSourceObservation,exactDomainObservation) eq exactCarrier) &&
      (registered.sourceProtectionData eq registered.lease.sourceProtectionData()) &&
      registered.sourceProtectionData.containsExactCarrier(exactCarrier) &&
      exactCarrier.binding==registered.link.bindingId && exactCarrier.chunk.dimension==loadedRootPlan.dimension() &&
      loadedRootPlan.loaded() && hasExactRootPlan,
      "source carrier must be an exact retained protection member in the Main-selected loaded root")
    val exactSqlRoot=v3RootRef.get()
    val actualLedger=v3LedgerRef.get()
    require(exactSqlRoot!=null && actualLedger!=null && !exactSqlRoot.isClosed &&
      (exactSqlRoot.profile eq registered.profile),"exact live original SQL root and allocation ledger required")
    exactSqlRoot.requireLedgerIdentity(actualLedger)
    require(registered.root.exactProfile() eq registered.profile,
      "restricted source allocation root/profile identity changed")
    val barrier=registered.lease.tryBeginManagedSourceScopedBarrier() match
      case None=>return MySqlRestrictedSourceReservePlanSubmission.Deferred
      case Some(exact)=>exact
    val intent=new MySqlOwnedIoAdapter.RestrictedSourceReserveIntent(this,registered.lease,registered.server,
      registered.root,exactSqlRoot,actualLedger,sourceCarrierLocator,registered.sourceProtectionData,
      registered.link,exactParentReference,exactCarrier,exactDomainObservation,loadedRootPlan,
      childWorkflow,reserveOperation,plannedWrite,writeGeneration,expectedSourceObservation)
    val command=MySqlOwnedIoCommand.PrepareRestrictedSourceReservePlan(intent)
    val pending=new RestrictedSourcePlanPending(tag,command,intent,barrier)
    require(restrictedSourcePlanPending.compareAndSet(null,pending),
      "restricted source plan pending slot changed during Main admission")
    try intent.requireOriginalReservePlanBinding(registered.link,exactParentReference,exactSqlRoot,
      exactDomainObservation,expectedSourceObservation)
    catch
      case cause:Throwable=>
        restrictedSourceFailure=cause
        pending.markFailed(cause)
        throw cause
    val reserved=try completionRouter.begin(tag,MySqlCompletionChannel.RequestRead,retainsPlan=false)
    catch
      case cause:Throwable=>
        restrictedSourceFailure=cause
        try
          pending.closeUntransferredBarrier()
          restrictedSourcePlanPending.compareAndSet(pending,null)
        catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
        throw cause
    if !reserved then
      pending.setSubmission(IoSubmission.RejectedCapacity)
      MySqlRestrictedSourceReservePlanSubmission.Submitted(IoSubmission.RejectedCapacity)
    else
      try
        ownerSubmitted=true
        val result=io.submit(tag,Work(tag,command,acceptedRepositoryEpoch,None))
        completionRouter.submitted(tag,result)
        pending.setSubmission(result)
        MySqlRestrictedSourceReservePlanSubmission.Submitted(result)
      catch
        case cause:Throwable=>
          pending.setUnknown(cause)
          try completionRouter.submissionFailed(tag,cause)
          catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
          throw cause

  /** 原RequestRead消费者也会拦截来源计划的completion，防止ACK落入其他mailbox。 */
  private def captureRestrictedSourceCompletions(
      completions:Vector[IoCompletion[MySqlOwnedIoReply]]):Vector[IoCompletion[MySqlOwnedIoReply]] =
    val pending=restrictedSourcePlanPending.get()
    val dropPending=restrictedSourcePlanDropPending.get()
    var unmatched=completions
    if pending!=null then
      val (source,other)=unmatched.partition(_.tag==pending.tag)
      source.foreach(pending.retainCompletion)
      unmatched=other
    if dropPending!=null then
      val (source,other)=unmatched.partition(_.tag==dropPending.tag)
      source.foreach(dropPending.retainCompletion)
      unmatched=other
    if unmatched.nonEmpty && (pending!=null || dropPending!=null) then
      restrictedSourceUnexpectedRequestRead=unmatched
      val cause=new IllegalStateException("restricted source RequestRead router returned a foreign exact tag")
      restrictedSourceFailure=cause
      throw cause
    if pending==null && dropPending==null then completions else Vector.empty

  /** 轮询只接收实际OwnedIO completion；UNKNOWN保留原cause与barrier，不重发或刷新intent。 */
  private[floatingpod] def pollRestrictedSourceReservePlan(
      max:Int):MySqlRestrictedSourceReservePlanPoll =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && max>0,
      "restricted source plan polling requires exact Main and a positive bound")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    val pending=restrictedSourcePlanPending.get()
    if pending==null then
      throw new IllegalStateException("no restricted source reserve plan is awaiting poll or ACK")
    pending.state match
      case RestrictedSourcePlanState.Unknown(cause)=>MySqlRestrictedSourceReservePlanPoll.Unknown(cause)
      case RestrictedSourcePlanState.Rejected(value)=>
        MySqlRestrictedSourceReservePlanPoll.Rejected(pending.rejectionAckFor(this,value))
      case RestrictedSourcePlanState.Failed(cause)=>
        MySqlRestrictedSourceReservePlanPoll.Failed(pending.failureAckFor(this,cause))
      case RestrictedSourcePlanState.Acknowledging=>MySqlRestrictedSourceReservePlanPoll.Pending
      case RestrictedSourcePlanState.Issued=>throw new IllegalStateException("issued plan cannot remain in pending slot")
      case _=>
        routeAvailableCompletions(max)
        val other=captureRestrictedSourceCompletions(
          completionRouter.take(MySqlCompletionChannel.RequestRead,max))
        if other.nonEmpty then throw new IllegalStateException("unexpected RequestRead completion escaped source route")
        pending.state match
          case RestrictedSourcePlanState.Unknown(cause)=>MySqlRestrictedSourceReservePlanPoll.Unknown(cause)
          case RestrictedSourcePlanState.Rejected(value)=>
            MySqlRestrictedSourceReservePlanPoll.Rejected(pending.rejectionAckFor(this,value))
          case RestrictedSourcePlanState.Failed(cause)=>
            MySqlRestrictedSourceReservePlanPoll.Failed(pending.failureAckFor(this,cause))
          case RestrictedSourcePlanState.CompletionRetained=>
            val completion=pending.completion.get
            completion.result match
              case Success(MySqlOwnedIoReply.RestrictedSourceReservePlanPrepared(_)) =>
                MySqlRestrictedSourceReservePlanPoll.Ready(pending.readyAck(this))
              case Failure(cause)=>
                MySqlRestrictedSourceReservePlanPoll.Failed(pending.failureAckFor(this,cause))
              case Success(otherReply)=>
                val cause=new IllegalStateException("restricted source plan returned a foreign completion reply: "+otherReply.getClass.getSimpleName)
                MySqlRestrictedSourceReservePlanPoll.Failed(pending.failureAckFor(this,cause))
          case RestrictedSourcePlanState.Accepted | RestrictedSourcePlanState.Submitting =>
            MySqlRestrictedSourceReservePlanPoll.Pending
          case RestrictedSourcePlanState.Acknowledging=>MySqlRestrictedSourceReservePlanPoll.Pending
          case RestrictedSourcePlanState.Issued=>throw new IllegalStateException("issued plan cannot remain in pending slot")

  /** 只有精确Main ACK真实typed plan后，closure才进入同一lease的chunk admission overlay。 */
  private[floatingpod] def acknowledgeRestrictedSourceReservePlan(
      ack:MySqlOwnedIoAdapter.RestrictedSourcePlanAck):com.wi.floatingpod.server.MainBootProjection.SourceChunkEffectClosure =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && ack!=null && (ack.adapter eq this),
      "exact Main and adapter-issued restricted source plan ACK required")
    val pending=Option(restrictedSourcePlanPending.get()).getOrElse(
      throw new IllegalStateException("restricted source plan ACK lost its original pending slot"))
    require((pending.tag==ack.tag) && (pending.command eq ack.command) && (pending.intent eq ack.intent) &&
      (pending.completion.contains(ack.completion)) &&
      ack.completion.result.toOption.exists {
        case MySqlOwnedIoReply.RestrictedSourceReservePlanPrepared(actual)=>actual eq ack.backing
        case _=>false
      },"restricted source ACK differs from its exact command, completion, intent, or backing")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    pending.beginSuccessAck(ack)
    try
      val closure=ack.backing.requireFor(this,ack.tag,ack.command,ack.intent)
      val plan=ack.backing.exactPlan
      val barrier=pending.barrierForLeaseTransfer
      val overlay=registered.lease.retainRestrictedSourcePlanClosure(ack.intent.expectedSourceObservation,
        ack.intent.exactDomainObservation,ack.intent.carrierLocator(),ack,plan,barrier,closure)
      pending.relinquishTransferredBarrier(barrier)
      pending.markIssued()
      restrictedSourcePlan=Some((ack.intent,ack.backing,overlay))
      restrictedSourcePlanAck=ack
      require(restrictedSourcePlanPending.compareAndSet(pending,null),
        "restricted source plan ACK could not release its exact pending slot")
      overlay
    catch
      case cause:Throwable=>
        restrictedSourceFailure=cause
        pending.markFailed(cause)
        throw cause

  /** Submit one real R0 only from the exact Main-ACKed plan and its still-settled source epoch. */
  private[floatingpod] def submitRestrictedSourceReserveR0():MySqlRestrictedSourceR0ExecutionSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),
      "restricted SOURCE R0 submission requires exact claimed Main")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    require(restrictedSourcePlanDropPending.get()==null,
      "restricted SOURCE R0 cannot race an original source-plan drop route")
    val (intent,backing,overlay)=restrictedSourcePlan.getOrElse(
      throw new IllegalStateException("exact Main-ACKed restricted source plan is required for R0"))
    val planAck=Option(restrictedSourcePlanAck).getOrElse(
      throw new IllegalStateException("exact restricted source plan ACK is unavailable for R0"))
    require((intent eq planAck.intent) && (backing eq planAck.backing) && overlay!=null &&
      (planAck.adapter eq this) && (intent.exactLink eq registered.link),
      "restricted SOURCE R0 must reuse the exact ACKed intent/backing/overlay route")
    // Main 只核原 ACK 与不可变 backing；完整 Fenced retained-plan 核验仍由 worker 执行。
    val plan=backing.requireR0SubmissionFor(this,planAck,intent)
    val observation=intent.expectedSourceObservation
    val expectedEpoch=observation.settledRepositoryEpoch
    require(acceptedRepositoryEpoch==expectedEpoch && restrictedSourceSettledEpoch.contains(expectedEpoch),
      "restricted SOURCE R0 requires the last Main-settled source epoch before accepted submission")
    registered.lease.requireActionCurrent(observation)
    requireRestrictedSourceActionNoUnresolved(registered.link,expectedEpoch)
    require(restrictedSourceR0Pending.get()==null && restrictedSourceR0Preparation==null &&
      !restrictedSourceR0ChargeAttempted && restrictedSourceR0ChargeRoot==null,
      "restricted SOURCE R0 has one immutable pending execution slot; retry/resend is forbidden")
    val nextEpoch=Math.addExact(acceptedRepositoryEpoch,1L)
    val root=plan.exactRoot
    require(root!=null,"exact restricted SOURCE R0 allocation root required")
    restrictedSourceR0ChargeRoot=root
    restrictedSourceR0ChargeAmount=MySqlOwnedIoAdapter.RestrictedSourceR0ProtocolChargeBytes
    restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.ChargeAttempt
    restrictedSourceR0ChargeAttempted=true
    restrictedSourceR0ChargeReturned=false
    restrictedSourceR0AnchorCause=null
    restrictedSourceR0AnchorSecondaryCause=null
    try root.reserveRetainedMemory(restrictedSourceR0ChargeAmount,
      "restricted SOURCE R0 fixed 3072+4 named 256-byte protocol slots BEFORE new holders/work")
    catch
      case cause:Throwable=>
        restrictedSourceR0AnchorCause=cause
        throw cause
    restrictedSourceR0ChargeReturned=true
    var preparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue=null
    var installed:RestrictedSourceR0Pending=null
    var mainPending:com.wi.floatingpod.server.MainBootProjection.SourceSubmissionPending=null
    try
      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.PreparationCreation
      preparation=new MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue(this,registered.lease,registered.server,
        registered.link,intent,backing,planAck,plan,root,expectedEpoch,nextEpoch)
      restrictedSourceR0Preparation=preparation
      preparation.markRegistered()

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.TagCreation
      preparation.requireTagBindAvailable()
      val tag=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,
        plan.exactRequest.reserveOperation)
      preparation.bindTag(tag)
      identity.requireTag(tag)

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.CommandCreation
      preparation.requireCommandBindAvailable()
      val command=MySqlOwnedIoCommand.ExecuteRestrictedSourceReserveR0(backing,planAck)
      preparation.bindCommand(command)

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.PendingCreation
      preparation.requireR0PendingBindAvailable()
      installed=new RestrictedSourceR0Pending(tag,command,backing,planAck,plan,expectedEpoch,nextEpoch,root,preparation)
      preparation.bindR0Pending(installed)

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.WorkInstallation
      preparation.requireWorkBindAvailable()
      val work=Work(tag,command,expectedEpoch,None)
      preparation.bindWork(work)
      installed.installWork(work)
      preparation.markWorkInstalled()

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.AdapterPendingRegistration
      preparation.markAdapterPendingInstallAttempted()
      require(restrictedSourceR0Pending.compareAndSet(null,installed),
        "restricted SOURCE R0 pending slot changed during exact Main admission")
      preparation.markAdapterPendingInstalled()

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainPendingRegistration
      mainPending=registered.lease.registerRestrictedSourceR0Pending(preparation)
      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.BootReadRetired
      preparation.requireBidirectionalPending(installed,mainPending)

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.EnqueueReadinessCheck
      registered.lease.requireRestrictedSourceR0EnqueueReady(preparation)
      preparation.markPreEnqueueReady()

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.SubmitCall
      preparation.markSubmitCallEntered()
      ownerSubmitted=true
      val result=io.submit(tag,work)
      preparation.retainRawSubmission(result)
      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.AdapterEpochAccounting
      require(preparation.submitCallEntered && result!=null,
        "OwnedSerialIo returned a valid result only after the exact restricted SOURCE R0 submit call")

      result match
        case IoSubmission.Accepted=>
          require(acceptedRepositoryEpoch==expectedEpoch,
            "known Accepted restricted SOURCE R0 must advance from its exact original epoch")
          acceptedRepositoryEpoch=nextEpoch
        case _=>
          require(acceptedRepositoryEpoch==expectedEpoch,
            "known rejected restricted SOURCE R0 must retain its exact original epoch")
      preparation.markAdapterEpochRecorded(acceptedRepositoryEpoch)

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.PendingOutcomeRecording
      installed.setSubmission(result)
      preparation.markPendingOutcomeRecorded()

      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording
      preparation.markMainOutcomeRecordAttempted()
      registered.lease.recordSubmissionOutcome(this,mainPending,preparation,result)
      preparation.markMainOutcomeRecorded()
      restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.Complete
      MySqlRestrictedSourceR0ExecutionSubmission.Submitted(result)
    catch
      case cause:Throwable=>
        if preparation==null then
          restrictedSourceR0AnchorCause=cause
          throw cause
        else if !preparation.submitCallEntered then
          val failureStage=if restrictedSourceR0AnchorStage==
              MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainPendingRegistration &&
              preparation.phase==MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.BootReadRetirement then
            MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.BootReadRetirement
          else restrictedSourceR0AnchorStage
          restrictedSourceR0AnchorStage=failureStage
          restrictedSourceR0AnchorCause=cause
          try preparation.retainPreEnqueueFailure(failureStage,cause)
          catch case recordFailure:Throwable=>restrictedSourceR0AnchorSecondaryCause=recordFailure
          try registered.lease.retainRestrictedSourceR0HandoffFailure(preparation,
            preparation.failureStageOr(restrictedSourceR0AnchorStage),cause)
          catch case secondary:Throwable=>
            if preparation.secondaryCause==null then preparation.retainSecondaryCause(secondary)
            if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=secondary
          throw cause
        else if !preparation.submissionReturned then
          restrictedSourceR0AnchorCause=cause
          try preparation.retainSubmissionUnknown(cause)
          catch case recordFailure:Throwable=>restrictedSourceR0AnchorSecondaryCause=recordFailure
          restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.AdapterEpochAccounting
          try
            preparation.markUnknownAdapterEpochObserved(acceptedRepositoryEpoch)
            restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.PendingOutcomeRecording
            installed.setUnknown(cause)
            preparation.markUnknownPendingOutcomeRecorded()
          catch case secondary:Throwable=>
            if preparation.secondaryCause==null then preparation.retainSecondaryCause(secondary,
              restrictedSourceR0AnchorStage)
            if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=secondary
          restrictedSourceR0AnchorStage=MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording
          try
            preparation.markMainOutcomeRecordAttempted()
            registered.lease.recordSubmissionUnknown(this,mainPending,preparation,cause)
            preparation.markMainOutcomeRecorded()
          catch case secondary:Throwable=>
            if preparation.secondaryCause==null then preparation.retainSecondaryCause(secondary,
              MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording)
            if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=secondary
            try registered.lease.retainRestrictedSourceR0HandoffFailure(preparation,
              preparation.failureStageOr(
                MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording),secondary)
            catch case retainFailure:Throwable=>
              if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=retainFailure
          MySqlRestrictedSourceR0ExecutionSubmission.Unknown(cause)
        else
          val known=preparation.rawSubmission
          restrictedSourceR0AnchorCause=cause
          try preparation.retainHandoffBookkeepingFailure(restrictedSourceR0AnchorStage,cause)
          catch case recordFailure:Throwable=>restrictedSourceR0AnchorSecondaryCause=recordFailure
          try installed.setHandoffBookkeepingFailure(known,cause)
          catch case secondary:Throwable=>
            if preparation.secondaryCause==null then preparation.retainSecondaryCause(secondary)
            if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=secondary
          try registered.lease.retainRestrictedSourceR0HandoffFailure(preparation,
            preparation.failureStageOr(restrictedSourceR0AnchorStage),cause)
          catch case secondary:Throwable=>
            if preparation.secondaryCause==null then preparation.retainSecondaryCause(secondary)
            if restrictedSourceR0AnchorSecondaryCause==null then restrictedSourceR0AnchorSecondaryCause=secondary
          throw cause

  /** Exact settlement poll remains usable after UNKNOWN and never upgrades it into Main/product ACK. */
  private[floatingpod] def pollRestrictedSourceReserveR0(
      max:Int):MySqlRestrictedSourceR0ExecutionPoll =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && max>0,
      "restricted SOURCE R0 settlement polling requires exact Main and a positive bound")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    val preparation=restrictedSourceR0Preparation
    if preparation==null && restrictedSourceR0AnchorCause!=null then
      return MySqlRestrictedSourceR0ExecutionPoll.PreEnqueueRetainedFailure(
        restrictedSourceR0AnchorStage,restrictedSourceR0ChargeAttempted,restrictedSourceR0ChargeReturned,false,null,
        acceptedRepositoryEpoch,restrictedSourceR0AnchorCause,restrictedSourceR0AnchorSecondaryCause)
    val retainedPreEnqueueCause=if preparation==null then null
      else if preparation.preEnqueueFailureCause!=null then preparation.preEnqueueFailureCause
      else if !preparation.submitCallEntered then restrictedSourceR0AnchorCause
      else null
    val retainedSecondary=if preparation==null || preparation.secondaryCause==null then
      restrictedSourceR0AnchorSecondaryCause else preparation.secondaryCause
    if preparation!=null && retainedPreEnqueueCause!=null then
      return MySqlRestrictedSourceR0ExecutionPoll.PreEnqueueRetainedFailure(
        preparation.failureStageOr(restrictedSourceR0AnchorStage),
        restrictedSourceR0ChargeAttempted,restrictedSourceR0ChargeReturned,
        preparation.submissionReturned,preparation.rawSubmission,acceptedRepositoryEpoch,retainedPreEnqueueCause,
        retainedSecondary)
    val handoffFailure=if preparation==null then null
      else if preparation.handoffBookkeepingFailure!=null then preparation.handoffBookkeepingFailure
      else if preparation.submissionReturned then restrictedSourceR0AnchorCause
      else null
    val retainedUnknownCause=if preparation==null then null
      else if preparation.submissionUnknownCause!=null then preparation.submissionUnknownCause
      else if preparation.submitCallEntered && !preparation.submissionReturned then restrictedSourceR0AnchorCause
      else null
    val unknownHandoffFailure=if retainedUnknownCause==null || retainedSecondary==null then null else retainedSecondary
    if handoffFailure!=null && (preparation.rawSubmission match
        case IoSubmission.RejectedCapacity | IoSubmission.RejectedQuiescing | IoSubmission.RejectedStopped(_) => true
        case _=>false) then
      val failedPending=restrictedSourceR0Pending.get()
      val late=if failedPending==null then None else failedPending.completion
      return MySqlRestrictedSourceR0ExecutionPoll.HandoffBookkeepingFailure(
        preparation.failureStageOr(restrictedSourceR0AnchorStage),
        preparation.submissionReturned,preparation.rawSubmission,acceptedRepositoryEpoch,handoffFailure,
        retainedSecondary,late)
    val pending=restrictedSourceR0Pending.get()
    if pending==null || (pending.plan.intent.exactLink ne registered.link) then
      throw new IllegalStateException("exact original restricted SOURCE R0 accepted execution route is unavailable")
    identity.requireTag(pending.tag)
    if preparation!=null && preparation.rawSubmission!=null then
      preparation.rawSubmission match
        case IoSubmission.RejectedCapacity | IoSubmission.RejectedQuiescing | IoSubmission.RejectedStopped(_) =>
          return MySqlRestrictedSourceR0ExecutionPoll.SubmissionRejected(preparation.rawSubmission)
        case IoSubmission.Accepted=>()
    if preparation!=null && !preparation.submitCallEntered then
      return MySqlRestrictedSourceR0ExecutionPoll.Pending
    var routingFailure:Throwable=null
    try routeAvailableCompletions(max)
    catch
      case cause:Throwable=>
        // A different router failure cannot erase or steal this exact pending R0 settlement route.
        pending.retainRoutingFailure(cause)
        routingFailure=if pending.routingFailure==null then cause else pending.routingFailure
    if routingFailure!=null then
      val routingSecondary=pending.routingSecondaryFailure
      if routingSecondary!=null then
        return MySqlRestrictedSourceR0ExecutionPoll.RoutingFailureWithSecondary(routingFailure,routingSecondary)
      else return MySqlRestrictedSourceR0ExecutionPoll.RoutingFailure(routingFailure)
    if handoffFailure!=null then
      return MySqlRestrictedSourceR0ExecutionPoll.HandoffBookkeepingFailure(
        preparation.failureStageOr(restrictedSourceR0AnchorStage),
        preparation.submissionReturned,preparation.rawSubmission,acceptedRepositoryEpoch,
        handoffFailure,retainedSecondary,pending.completion)
    if retainedUnknownCause!=null then
      if unknownHandoffFailure!=null then
        return MySqlRestrictedSourceR0ExecutionPoll.SubmissionUnknownHandoffFailure(
          preparation.failureStageOr(restrictedSourceR0AnchorStage),
          retainedUnknownCause,unknownHandoffFailure,pending.completion)
      return MySqlRestrictedSourceR0ExecutionPoll.SubmissionUnknown(retainedUnknownCause,pending.completion)
    pending.state match
      case RestrictedSourceR0State.Submitting | RestrictedSourceR0State.Accepted=>
        MySqlRestrictedSourceR0ExecutionPoll.Pending
      case RestrictedSourceR0State.Rejected(value)=>
        MySqlRestrictedSourceR0ExecutionPoll.SubmissionRejected(value)
      case RestrictedSourceR0State.SubmissionUnknown(cause)=>
        MySqlRestrictedSourceR0ExecutionPoll.SubmissionUnknown(cause,pending.completion)
      case RestrictedSourceR0State.HandoffBookkeepingFailure(value,cause)=>
        MySqlRestrictedSourceR0ExecutionPoll.HandoffBookkeepingFailure(
          preparation.failureStage,preparation.submissionReturned,value,acceptedRepositoryEpoch,cause,
          preparation.secondaryCause,pending.completion)
      case RestrictedSourceR0State.CompletionRetained=>
        val completion=pending.completion.getOrElse(
          throw new IllegalStateException("restricted SOURCE R0 completion state lost its exact completion"))
        completion.result match
          case Success(MySqlOwnedIoReply.RestrictedSourceR0OutcomeRetained(outcome))
              if outcome!=null && (outcome.plan eq pending.plan) && outcome.tag==pending.tag &&
                outcome.acceptedRepositoryEpochAtSubmission==pending.expectedSourceEpoch &&
                outcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.Running =>
            MySqlRestrictedSourceR0ExecutionPoll.OutcomeRetained(outcome)
          case Success(MySqlOwnedIoReply.RestrictedSourceR0WorkerFailed(cause)) if cause!=null=>
            MySqlRestrictedSourceR0ExecutionPoll.WorkerFailed(cause)
          case Failure(cause)=>MySqlRestrictedSourceR0ExecutionPoll.WorkerFailed(cause)
          case Success(other)=>
            MySqlRestrictedSourceR0ExecutionPoll.WorkerFailed(
              new IllegalStateException("restricted SOURCE R0 returned a foreign typed completion: "+other.getClass.getSimpleName))

  /** 只有明确未入队的容量拒绝可经Main ACK归还barrier；quiescing/stopped保留失败责任。 */
  private[floatingpod] def acknowledgeRestrictedSourceReservePlanRejection(
      ack:MySqlOwnedIoAdapter.RestrictedSourcePlanRejectionAck):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && ack!=null && (ack.adapter eq this),
      "exact Main and adapter-issued restricted source rejection ACK required")
    val pending=Option(restrictedSourcePlanPending.get()).getOrElse(
      throw new IllegalStateException("restricted source rejection ACK lost its original pending slot"))
    require(pending.tag==ack.tag && (pending.command eq ack.command) && (pending.intent eq ack.intent) &&
      pending.state==RestrictedSourcePlanState.Rejected(ack.submission),
      "restricted source rejection ACK differs from its actual submission receipt")
    ack.submission match
      case IoSubmission.RejectedCapacity=>
        try pending.closeUntransferredBarrier()
        catch
          case cause:Throwable=>
            restrictedSourceFailure=cause
            throw cause
      case IoSubmission.RejectedQuiescing=>
        val cause=new IllegalStateException("restricted source read-plan submission was rejected while IO was quiescing")
        if restrictedSourceFailure==null then restrictedSourceFailure=cause
        throw cause
      case IoSubmission.RejectedStopped(cause)=>
        val failure=cause.getOrElse(new IllegalStateException("restricted source read-plan submission was rejected after IO stopped"))
        if restrictedSourceFailure==null then restrictedSourceFailure=failure
        throw failure
      case IoSubmission.Accepted=>throw new IllegalStateException("accepted restricted source submission cannot use rejection ACK")
    require(restrictedSourcePlanPending.compareAndSet(pending,null),
      "restricted source rejection ACK could not release its exact pending slot")

  /** 实际worker失败完成后ACK原cause；范围保持阻塞，不向新source action降级。 */
  private[floatingpod] def acknowledgeRestrictedSourceReservePlanFailure(
      ack:MySqlOwnedIoAdapter.RestrictedSourcePlanFailureAck):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && ack!=null && (ack.adapter eq this),
      "exact Main and adapter-issued restricted source failure ACK required")
    val pending=Option(restrictedSourcePlanPending.get()).getOrElse(
      throw new IllegalStateException("restricted source failure ACK lost its original pending slot"))
    require(pending.tag==ack.tag && (pending.command eq ack.command) && (pending.intent eq ack.intent) &&
      pending.completion.contains(ack.completion) && ack.cause!=null &&
      (pending.failureAckFor(this,ack.cause) eq ack),
      "restricted source failure ACK differs from its actual completion")
    restrictedSourceFailure=ack.cause
    pending.markFailed(ack.cause)

  /** Fenced can revalidate its opaque registration on Main during issuance and on the worker before revoke. */
  private[mysql] def requireCurrentRestrictedSourceDropRegistration(
      exactRegistration:MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration):Unit =
    require(exactRegistration!=null,"exact Adapter-issued source-plan drop registration required")
    require(restrictedSourceR0Pending.get()==null,
      "restricted SOURCE plan cannot be dropped while its exact R0 execution/settlement remains retained")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require(registered.asInstanceOf[AnyRef] eq exactRegistration.registrationIdentity,
      "restricted source drop registration differs from the original Adapter route")
    val pending=restrictedSourcePlanDropPending.get()
    require(pending!=null && (pending.registration eq exactRegistration),
      "exact pending source-plan drop registration required")
    if !exactRegistration.hasIssuedReceipt then
      require(currentMainOwner.exists(_ eq Thread.currentThread()),
        "source-plan drop receipt issuance requires the exact claimed Main")
      val (intent,backing,overlay)=restrictedSourcePlan.getOrElse(
        throw new IllegalStateException("exact current restricted source plan is not retained"))
      require((restrictedSourcePlanAck eq exactRegistration.planAckOwner) &&
        (intent eq exactRegistration.intentOwner) && (backing eq exactRegistration.backingOwner) &&
        (overlay eq exactRegistration.overlayOwner) && (backing.exactPlan eq exactRegistration.planOwner) &&
        (pending.planOwner eq exactRegistration.planOwner) && pending.tag==exactRegistration.dropTag,
        "source-plan drop registration lost its exact current read-plan tuple")
    else
      require(pending.tag==exactRegistration.dropTag && (pending.proof eq exactRegistration.viewsRevokedOwner) &&
        (pending.receipt eq exactRegistration.receipt),
        "worker source-plan drop registration differs from its exact issued receipt")

  /** Fenced只读drop专用责任校验；普通intent-current仍只接受未退休的read route。 */
  private[mysql] def requireRestrictedSourcePlanDropCurrent(
      exactIntent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      actualLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      actualServer:net.minecraft.server.MinecraftServer,
      actualSourceRoot:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      actualSqlRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt,
      exactRegistration:MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration):Unit =
    require(exactIntent!=null && actualLease!=null && actualServer!=null && actualSourceRoot!=null &&
      actualSqlRoot!=null && actualLedger!=null && exactPlan!=null && exactReceipt!=null && exactRegistration!=null,
      "exact original source-plan drop owners required")
    requireCurrentRestrictedSourceDropRegistration(exactRegistration)
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require((exactRegistration.adapter eq this) &&
      (registered.asInstanceOf[AnyRef] eq exactRegistration.registrationIdentity) &&
      (registered.lease eq actualLease) && (registered.server eq actualServer) &&
      (registered.root eq actualSourceRoot) && (registered.link eq exactIntent.exactLink) &&
      (registered.sourceProtectionData eq exactIntent.sourceProtectionData()) &&
      (actualSqlRoot eq v3RootRef.get()) && (actualLedger eq v3LedgerRef.get()) &&
      !actualSqlRoot.isClosed && (actualSqlRoot.profile eq registered.profile) &&
      exactIntent.expectedSourceObservation!=null &&
      (exactPlan.intent eq exactIntent) && (exactPlan.issuer!=null) &&
      exactPlan.parent!=null &&
      (exactPlan.parent.link eq registered.link) &&
      (exactPlan.parent.reference eq exactIntent.parentReference) &&
      (exactPlan.exactRoot eq actualSqlRoot) && (exactPlan.actualLedger eq actualLedger),
      "source-plan drop intent, registration, root, or ledger differs from its original Adapter route")
    require(exactRegistration.hasIssuedReceipt && (exactRegistration.receipt eq exactReceipt) &&
      exactRegistration.readTag!=null && exactRegistration.dropTag!=null,
      "exact one-shot Fenced receipt and original read/drop tags required")
    exactReceipt.requireFor(exactPlan)
    val pending=restrictedSourcePlanDropPending.get()
    require(pending!=null && (pending.registration eq exactRegistration) &&
      (pending.registrationIdentity eq registered.asInstanceOf[AnyRef]) &&
      pending.tag==exactRegistration.dropTag && (pending.proof eq exactRegistration.viewsRevokedOwner) &&
      (pending.receipt eq exactReceipt) && pending.proof.tagForSubmit==pending.tag &&
      (pending.command match
        case MySqlOwnedIoCommand.DropRestrictedSourceReservePlan(receipt)=>receipt eq exactReceipt
        case _=>false),
      "worker source-plan drop lost its exact Main proof, registration, receipt, tag, or command")
    pending.state match
      case RestrictedSourcePlanDropState.Submitting | RestrictedSourcePlanDropState.Accepted |
          RestrictedSourcePlanDropState.Unknown(_) => ()
      case _=>throw new IllegalStateException("source-plan drop worker is outside its submitted responsibility state")

  private def closeUnpublishedRestrictedSourceDropCharge(exactCharge:AutoCloseable,original:Throwable):Unit =
    if exactCharge==null || original==null then throw new IllegalArgumentException("exact unpublished protocol charge and original failure required")
    try exactCharge.close()
    catch
      case cleanup:Throwable=>
        failedUnpublishedRestrictedSourcePlanDropCharge=exactCharge
        failedUnpublishedRestrictedSourcePlanDropChargeCause=original
        if restrictedSourceFailure==null then restrictedSourceFailure=original
        MySqlOwnedIoAdapter.suppressExitCleanup(original,cleanup)

  /** Preserve the prior ACK tombstone until its own original-account charge really refunds. */
  private def releaseCompletedRestrictedSourcePlanDropTombstone():Unit =
    val charge=completedRestrictedSourcePlanDropCharge
    if charge==null then return
    if completedRestrictedSourcePlanDropChargeFailure!=null then
      throw completedRestrictedSourcePlanDropChargeFailure
    if completedRestrictedSourcePlanDropChargeReleaseAttempted then
      val cause=new IllegalStateException("completed source-plan drop tombstone charge refund has an uncertain prior outcome")
      completedRestrictedSourcePlanDropChargeFailure=cause
      throw cause
    completedRestrictedSourcePlanDropChargeReleaseAttempted=true
    try charge.close()
    catch
      case cause:Throwable=>
        completedRestrictedSourcePlanDropChargeFailure=cause
        throw cause
    completedRestrictedSourcePlanDropCharge=null
    completedRestrictedSourcePlanDropAck=null
    completedRestrictedSourcePlanDropReceipt=null
    completedRestrictedSourcePlanDropTag=null
    completedRestrictedSourcePlanDropRegistrationIdentity=null
    completedRestrictedSourcePlanDropChargeReleaseAttempted=false

  /** Main hands off only the exact revoked current plan; UNKNOWN never clears this pending owner. */
  private[floatingpod] def submitRestrictedSourcePlanDrop(
      dropTag:IoOperationTag,
      exactViewsRevoked:com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked
  ):MySqlRestrictedSourcePlanDropSubmission =
    require(currentMainOwner.exists(_ eq Thread.currentThread()),"restricted source plan drop requires exact claimed Main")
    identity.requireTag(dropTag)
    require(exactViewsRevoked!=null && exactViewsRevoked.tagForSubmit==dropTag,
      "exact Main source-plan drop proof/tag required")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    Option(restrictedSourceFailure).foreach(throw _)
    val existing=restrictedSourcePlanDropPending.get()
    require(restrictedSourceR0Pending.get()==null,
      "restricted source plan cannot be dropped while its exact R0 execution/settlement remains retained")
    val pending=if existing!=null then
      require(existing.tag==dropTag && (existing.proof eq exactViewsRevoked) &&
        (existing.registrationIdentity eq registered.asInstanceOf[AnyRef]),
        "source-plan drop retry must preserve its exact tag, proof, and original registration")
      existing
    else
      require(restrictedSourcePlanPending.get()==null && restrictedSourcePlanDropPending.get()==null,
        "restricted source read-plan submission is still pending")
      val (intent,backing,overlay)=restrictedSourcePlan.getOrElse(
        throw new IllegalStateException("exact acknowledged restricted source plan is required before drop"))
      val planAck=Option(restrictedSourcePlanAck).getOrElse(
        throw new IllegalStateException("exact acknowledged restricted source plan proof is required before drop"))
      val plan=backing.exactPlan
      require(plan!=null && planAck.tag!=null && (plan.intent eq intent) &&
        (planAck.intent eq intent) && (planAck.backing eq backing) &&
        (planAck.completion.result.toOption.exists {
          case MySqlOwnedIoReply.RestrictedSourceReservePlanPrepared(actual)=>actual eq backing
          case _=>false
        }),"source-plan drop must retain the exact successful worker plan ACK")
      // 原Main Custody账户先按固定槽账本预付：pending、registration、command、Work、router Entry与MapNode、Fenced receipt、reply、Success、IoCompletion、Fenced ACK、有界tombstone和收费token。
      val protocolCharge=registered.lease.retainAdmissionCharge(intent.expectedSourceObservation,
        MySqlOwnedIoAdapter.RestrictedSourcePlanDropProtocolChargeBytes,
        "restricted source plan drop pending/receipt/ACK/tombstone protocol BEFORE allocation")
      val value=try new RestrictedSourcePlanDropPending(dropTag,exactViewsRevoked,
        registered.asInstanceOf[AnyRef],intent,planAck,backing,overlay,plan,protocolCharge)
      catch
        case cause:Throwable=>
          closeUnpublishedRestrictedSourceDropCharge(protocolCharge,cause)
          throw cause
      if !restrictedSourcePlanDropPending.compareAndSet(null,value) then
        val cause=new IllegalStateException("restricted source plan drop pending slot changed during Main admission")
        if restrictedSourceFailure==null then restrictedSourceFailure=cause
        closeUnpublishedRestrictedSourceDropCharge(value.transferProtocolCharge(),cause)
        throw cause
      value
    if !pending.beginAttempt() then return MySqlRestrictedSourcePlanDropSubmission.AlreadyPending
    val reserved=try completionRouter.begin(dropTag,MySqlCompletionChannel.RequestRead,retainsPlan=false)
    catch
      case cause:Throwable=>
        pending.setUnknown(cause)
        throw cause
    if !reserved then
      pending.setSubmission(IoSubmission.RejectedCapacity)
      return MySqlRestrictedSourcePlanDropSubmission.Submitted(IoSubmission.RejectedCapacity)
    var ioSubmitAttempted=false
    try
      if pending.receipt==null then
        val registration=new MySqlOwnedIoAdapter.RestrictedSourcePlanDropRegistration(this,
          pending.registrationIdentity,dropTag,pending.planAckOwner.tag,registered.lease,pending.intentOwner,
          pending.planAckOwner,pending.backingOwner,pending.overlayOwner,pending.planOwner,pending.proof)
        pending.installRegistration(registration)
        val receipt=pending.planOwner.issueSourceReservePlanDropReceipt(this,registration,pending.proof)
        if receipt==null then throw new IllegalStateException("Fenced source-plan drop issuer returned no receipt")
        pending.installReceipt(receipt)
        restrictedSourcePlan=None
        restrictedSourcePlanAck=null
      val command=Option(pending.command).getOrElse(
        throw new IllegalStateException("exact source-plan drop command was not retained with its receipt"))
      ownerSubmitted=true
      ioSubmitAttempted=true
      val result=io.submit(dropTag,Work(dropTag,command,acceptedRepositoryEpoch,None))
      completionRouter.submitted(dropTag,result)
      pending.setSubmission(result)
      MySqlRestrictedSourcePlanDropSubmission.Submitted(result)
    catch
      case cause:Throwable=>
        if !ioSubmitAttempted then
          if pending.receipt==null && pending.planOwner!=null then
            try
              val retained=pending.planOwner.receiptForReplay
              if retained!=null then
                pending.installReceipt(retained)
                restrictedSourcePlan=None
                restrictedSourcePlanAck=null
            catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
          try completionRouter.submitted(dropTag,IoSubmission.RejectedCapacity)
          catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
          pending.failBeforeQueue(cause)
        else
          pending.setUnknown(cause)
          try completionRouter.submissionFailed(dropTag,cause)
          catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
        throw cause

  /** 只从原 RequestRead router 截取drop completion；同一tag的ACK在Main消费前保持可重复轮询。 */
  private[floatingpod] def pollRestrictedSourcePlanDrop(max:Int):MySqlRestrictedSourcePlanDropPoll =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && max>0,
      "restricted source plan drop polling requires exact Main and a positive bound")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    val pending=Option(restrictedSourcePlanDropPending.get()).getOrElse(
      throw new IllegalStateException("no restricted source plan drop is awaiting worker ACK"))
    pending.state match
      case RestrictedSourcePlanDropState.Unknown(cause)=>MySqlRestrictedSourcePlanDropPoll.Unknown(cause)
      case RestrictedSourcePlanDropState.Failed(cause)=>MySqlRestrictedSourcePlanDropPoll.Failed(cause)
      case RestrictedSourcePlanDropState.Rejected(IoSubmission.RejectedStopped(None))=>
        val cause=new IllegalStateException("restricted source plan drop was rejected after IO stopped without an original cause")
        pending.markFailed(cause)
        MySqlRestrictedSourcePlanDropPoll.Failed(cause)
      case RestrictedSourcePlanDropState.Rejected(value)=>MySqlRestrictedSourcePlanDropPoll.Rejected(value)
      case RestrictedSourcePlanDropState.CompletionRetained=>restrictedSourcePlanDropReady(pending)
      case RestrictedSourcePlanDropState.Acknowledged=>
        throw new IllegalStateException("acknowledged source-plan drop remained in its active pending slot")
      case _=>
        try
          routeAvailableCompletions(max)
          val other=captureRestrictedSourceCompletions(
            completionRouter.take(MySqlCompletionChannel.RequestRead,max))
          if other.nonEmpty then throw new IllegalStateException("unexpected RequestRead completion escaped source drop route")
          pending.state match
            case RestrictedSourcePlanDropState.Unknown(cause)=>MySqlRestrictedSourcePlanDropPoll.Unknown(cause)
            case RestrictedSourcePlanDropState.Failed(cause)=>MySqlRestrictedSourcePlanDropPoll.Failed(cause)
            case RestrictedSourcePlanDropState.Rejected(IoSubmission.RejectedStopped(None))=>
              val cause=new IllegalStateException("restricted source plan drop was rejected after IO stopped without an original cause")
              pending.markFailed(cause)
              MySqlRestrictedSourcePlanDropPoll.Failed(cause)
            case RestrictedSourcePlanDropState.Rejected(value)=>MySqlRestrictedSourcePlanDropPoll.Rejected(value)
            case RestrictedSourcePlanDropState.CompletionRetained=>restrictedSourcePlanDropReady(pending)
            case _=>MySqlRestrictedSourcePlanDropPoll.Pending
        catch
          case cause:Throwable=>
            pending.markFailed(cause)
            throw cause

  private def restrictedSourcePlanDropReady(
      pending:RestrictedSourcePlanDropPending):MySqlRestrictedSourcePlanDropPoll =
    val completion=pending.completion.getOrElse(throw new IllegalStateException("exact source-plan drop completion is not retained"))
    completion.result match
      case Success(MySqlOwnedIoReply.RestrictedSourceReservePlanDropped(ack))=>
        try
          ack.requireFor(pending.receipt)
          MySqlRestrictedSourcePlanDropPoll.Ready(pending.readyAck())
        catch
          case cause:Throwable=>
            pending.markFailed(cause)
            MySqlRestrictedSourcePlanDropPoll.Failed(cause)
      case Failure(cause)=>
        pending.markFailed(cause)
        MySqlRestrictedSourcePlanDropPoll.Failed(cause)
      case Success(otherReply)=>
        val cause=new IllegalStateException("restricted source plan drop returned a foreign completion reply: "+otherReply.getClass.getSimpleName)
        pending.markFailed(cause)
        MySqlRestrictedSourcePlanDropPoll.Failed(cause)

  /** Lease validates and settles its original proof first; this method then clears Adapter refs idempotently. */
  private[floatingpod] def acknowledgeRestrictedSourcePlanDrop(
      exactAck:MySqlFencedTransaction.SourceReservePlanDropAck):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && exactAck!=null,
      "exact Main and Fenced source-plan drop ACK required")
    if completedRestrictedSourcePlanDropAck eq exactAck then return
    val pending=restrictedSourcePlanDropPending.get()
    if pending==null then
      throw new IllegalStateException("source-plan drop ACK lost its pending route and exact completed tombstone")
    require(pending.state==RestrictedSourcePlanDropState.CompletionRetained &&
      (pending.readyAck() eq exactAck) && pending.proof.isTerminalAckSettled,
      "Main must settle the exact original drop proof before Adapter plan-slot release")
    exactAck.requireFor(pending.receipt)
    val exactReceipt=pending.receipt
    val exactDropTag=pending.tag
    val exactRegistrationIdentity=pending.registrationIdentity
    releaseCompletedRestrictedSourcePlanDropTombstone()
    val protocolCharge=pending.transferProtocolCharge()
    try pending.acknowledge(exactAck)
    catch
      case cause:Throwable=>
        failedUnpublishedRestrictedSourcePlanDropCharge=protocolCharge
        failedUnpublishedRestrictedSourcePlanDropChargeCause=cause
        if restrictedSourceFailure==null then restrictedSourceFailure=cause
        throw cause
    completedRestrictedSourcePlanDropAck=exactAck
    completedRestrictedSourcePlanDropReceipt=exactReceipt
    completedRestrictedSourcePlanDropTag=exactDropTag
    completedRestrictedSourcePlanDropRegistrationIdentity=exactRegistrationIdentity
    completedRestrictedSourcePlanDropCharge=protocolCharge
    completedRestrictedSourcePlanDropChargeReleaseAttempted=false
    completedRestrictedSourcePlanDropChargeFailure=null
    require(restrictedSourcePlanDropPending.compareAndSet(pending,null),
      "exact completed source-plan drop pending slot changed during ACK consumption")

  /** Main's original lease calls this before closing its small proof charge; no DATA/reader is exposed. */
  private[floatingpod] def requireRestrictedSourcePlanDropAckFor(
      exactLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      exactProof:com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked,
      exactAck:MySqlFencedTransaction.SourceReservePlanDropAck):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && exactLease!=null &&
      exactProof!=null && exactAck!=null,"exact Main lease, revoked views proof, and worker ACK required")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require(registered.lease eq exactLease,
      "source-plan drop ACK belongs to another original Adapter registration")
    val pending=restrictedSourcePlanDropPending.get()
    require(pending!=null && (pending.proof eq exactProof) &&
      (pending.registrationIdentity eq registered.asInstanceOf[AnyRef]),
      "exact pending Main source-plan drop proof/registration required")
    require(pending.state==RestrictedSourcePlanDropState.CompletionRetained && pending.receipt!=null,
      "source-plan drop ACK differs from the exact pending Main proof/tag")
    if !exactProof.isTerminalAckSettled then
      require(pending.tag==exactProof.tagForSubmit,"source-plan drop ACK tag differs from its exact Main proof")
    val observed=pending.readyAck()
    require(observed eq exactAck,"Main source-plan drop ACK differs from Adapter-observed typed completion")
    exactAck.requireFor(pending.receipt)

  /** Completed duplicate ACKs use only the bounded exact tag/ACK/registration tombstone. */
  private[floatingpod] def requireCompletedRestrictedSourcePlanDropAckFor(
      exactLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      exactDropTag:IoOperationTag,
      exactAck:MySqlFencedTransaction.SourceReservePlanDropAck):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && exactLease!=null &&
      exactDropTag!=null && exactAck!=null,
      "exact Main lease, source-plan drop tag, and completed worker ACK required")
    identity.requireTag(exactDropTag)
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require(registered.lease eq exactLease,
      "duplicate source-plan drop ACK belongs to another original Adapter lease")
    if (registered.asInstanceOf[AnyRef] eq completedRestrictedSourcePlanDropRegistrationIdentity) &&
        completedRestrictedSourcePlanDropTag==exactDropTag &&
        (completedRestrictedSourcePlanDropAck eq exactAck) then
      exactAck.requireFor(completedRestrictedSourcePlanDropReceipt)
    else
      val pending=restrictedSourcePlanDropPending.get()
      require(pending!=null && (pending.registrationIdentity eq registered.asInstanceOf[AnyRef]) &&
        pending.tag==exactDropTag && pending.state==RestrictedSourcePlanDropState.CompletionRetained &&
        pending.proof.isTerminalAckSettled && (pending.readyAck() eq exactAck),
        "duplicate source-plan drop ACK differs from the exact Adapter tombstone or pending ACK")
      exactAck.requireFor(pending.receipt)

  /** Worker绑定只认可当前pending或已ACK的唯一原计划，不接受新构造/已退休的intent。 */
  private[mysql] def retainedRestrictedSourceIntentIsCurrent(
      intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      actualLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      actualServer:net.minecraft.server.MinecraftServer,
      actualSourceRoot:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      actualSqlRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger):Boolean =
    val registration=restrictedSourceRegistration
    val routePending=Option(restrictedSourcePlanPending.get()).exists(pending =>
      (pending.intent eq intent) && (pending.state match
          case RestrictedSourcePlanState.Rejected(_) => false
          case RestrictedSourcePlanState.Failed(_) => false
          case _ => true))
    val routeIssued=restrictedSourcePlan.exists { case (retainedIntent,backing,_) =>
      (retainedIntent eq intent) && backing!=null
    }
    registration.exists(registered => (registered.lease eq actualLease) && (registered.server eq actualServer) &&
      (registered.root eq actualSourceRoot) && (registered.link eq intent.exactLink) &&
      (registered.sourceProtectionData eq intent.sourceProtectionData()) &&
      (actualSqlRoot eq v3RootRef.get()) &&
      (actualLedger eq v3LedgerRef.get()) && actualSqlRoot!=null && actualLedger!=null && !actualSqlRoot.isClosed &&
      (actualSqlRoot.profile eq registered.profile) && (routePending || routeIssued))

  /** 只有来源epoch已ACK且真实completion路由为空，才允许继续动作。 */
  private[floatingpod] def requireRestrictedSourceActionNoUnresolved(
      exactLink:MySqlOwnedIoAdapter.RecallRecoveryLink,settledEpoch:Long):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && exactLink!=null,
      "exact claimed Main and restricted source link required")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require((registered.link eq exactLink) && (exactLink.adapter eq this) && (exactLink.identity eq identity),
      "restricted source action belongs to another original link")
    registered.lease.requireResponsibilityOwner(registered.server,exactLink)
    Option(restrictedSourceFailure).foreach(throw _)
    require(repositorySubmissionEpoch==settledEpoch && restrictedSourceSettledEpoch.contains(settledEpoch),
      "restricted source action does not match its last Main-ACKed repository epoch")
    require(restrictedSourceR0Pending.get()==null && restrictedSourceR0Preparation==null &&
      !restrictedSourceR0ChargeAttempted && restrictedSourceR0ChargeRoot==null &&
      restrictedSourceR0AnchorCause==null && restrictedSourceR0AnchorSecondaryCause==null,
      "restricted source action is held by the original unresolved R0 execution/settlement route")
    require(restrictedSourcePlanDropPending.get()==null,
      "restricted source action waits for its original plan-drop responsibility")
    val pending=Option(restrictedSourcePlanPending.get())
    require(pending.forall(value=>value.isAckInProgress && value.completion.nonEmpty) &&
      completionRouter.retainedCount==0 && completionRouter.failure.isEmpty && restrictedSourceUnexpectedRequestRead==null,
      "restricted source action waits for every prior accepted completion or uncertainty")
    val checkpoint=io.checkpoint
    checkpoint.fatalCause.foreach(throw _)
    checkpoint.exitFailure.foreach(throw _)
    require(checkpoint.accepting && !checkpoint.stopped,
      "restricted source adapter is not accepting its original continuation")

  /** 仅此 R0 在 Main 注册与入队前阶段可使用的精确 role 例外。 */
  private[floatingpod] def requireRestrictedSourceR0PreEnqueue(
      exactPreparation:MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueue,
      exactLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      exactObservation:com.wi.floatingpod.server.MainBootProjection.SourceContinuationObservation):Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && exactPreparation!=null && exactLease!=null &&
      exactObservation!=null,"exact claimed Main and restricted SOURCE R0 preparation required")
    val registered=restrictedSourceRegistration.getOrElse(
      throw new IllegalStateException("original restricted source continuation route is not registered"))
    require((registered.lease eq exactLease) && (registered.link eq exactPreparation.exactLink) &&
      (registered.server eq exactPreparation.exactServer) && (exactPreparation.adapter eq this),
      "restricted SOURCE R0 preparation differs from its original registered Adapter/lease/link/server")
    registered.lease.requireResponsibilityOwner(registered.server,registered.link)
    exactPreparation.requireOriginal(this,exactLease,registered.link,exactObservation)
    if restrictedSourceFailure!=null then throw restrictedSourceFailure
    require((restrictedSourceR0Preparation eq exactPreparation) && restrictedSourceR0ChargeAttempted &&
      restrictedSourceR0ChargeReturned && (restrictedSourceR0ChargeRoot eq exactPreparation.protocolRoot) &&
      restrictedSourceR0ChargeAmount==MySqlOwnedIoAdapter.RestrictedSourceR0ProtocolChargeBytes,
      "exact SOURCE R0 charge and preparation owner must remain registered")
    require(acceptedRepositoryEpoch==exactPreparation.expectedSourceEpoch &&
      restrictedSourceSettledEpoch.contains(exactPreparation.expectedSourceEpoch),
      "restricted SOURCE R0 pre-enqueue epoch differs from its last settled original epoch")
    val exactPending=restrictedSourceR0Pending.get()
    require(exactPending!=null && exactPreparation.ownsR0Pending(exactPending) &&
      exactPending.tag==exactPreparation.exactTag && (exactPending.command eq exactPreparation.exactCommand),
      "restricted SOURCE R0 pre-enqueue requires its exact original installed pending")
    exactPreparation.phase match
      case MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.AdapterPendingInstalled =>
        require(exactPreparation.mainPending==null,
          "Main registration phase cannot reuse an already-bound source pending")
      case MySqlOwnedIoAdapter.RestrictedSourceR0PreEnqueueStage.BootReadRetired =>
        require(exactPreparation.mainPending!=null && exactPreparation.mainPendingBound,
          "pre-enqueue readiness requires the exact bidirectional Main pending")
      case _=>throw new IllegalStateException("restricted SOURCE R0 preparation is outside its two allowed role phases")
    require(restrictedSourcePlanDropPending.get()==null,
      "restricted SOURCE R0 pre-enqueue cannot race its original plan-drop route")
    val planPending=restrictedSourcePlanPending.get()
    require((planPending==null || (planPending.isAckInProgress && planPending.completion.nonEmpty)) && completionRouter.retainedCount==0 &&
      completionRouter.failure.isEmpty && restrictedSourceUnexpectedRequestRead==null,
      "restricted SOURCE R0 pre-enqueue waits for every other original completion or uncertainty")
    val checkpoint=io.checkpoint
    checkpoint.fatalCause.foreach(throw _)
    checkpoint.exitFailure.foreach(throw _)
    require(checkpoint.accepting && !checkpoint.stopped,
      "original restricted source Adapter stopped before SOURCE R0 enqueue")

  /** 仅actual boot-control装配。normal API未安装holder时保持既有行为。 */
  def installBootstrapPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
                                  command: MySqlOwnedIoCommand.Boot): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "only exact initial bootstrap control may install preparation")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted, "preparation must precede every submission")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** 同一个control/preparation gate；只扩大固定Boot类型，不传native实例或游戏对象。 */
  def installFreshBootstrapPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
                                       command: MySqlOwnedIoCommand.BootFresh): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "only exact initial bootstrap control may install Fresh preparation")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted, "Fresh preparation must precede every submission")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** Normal只走真实anchored初始seed；不接普通pending/Certified恢复。 */
  def installNormalPreGenesisBootstrapPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
                                       command: MySqlOwnedIoCommand.BootNormalPreGenesis): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "only exact initial bootstrap control may install Normal pre-genesis preparation")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted, "Normal pre-genesis preparation must precede every submission")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** 专用Certified inspection仍只由实际bootstrap control推进，无普通Main等待入口。 */
  def installCertifiedInspectionBootstrapPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootCertifiedInspection): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "only exact initial bootstrap control may install inspection preparation")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted, "inspection preparation must precede every submission")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** Same one-shot production preparation gate; ordinary source pins are immutable DATA, not native authority. */
  def installOrdinaryCertifiedBootstrapPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootOrdinaryCertified): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "only exact initial ordinary bootstrap control may install preparation")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted,
      "ordinary Certified preparation must precede every submission")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** 仅精确V3 Fresh命令；保留原control/tag/未提交/一次安装gate。 */
  def installFreshBootstrapPreparationV3(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootFreshV3): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain,"only exact initial bootstrap control may install V3 Fresh preparation")
    require(control != null && (control.controlThread() eq initialMain),"actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted,"V3 Fresh preparation must precede every submission")
    val value=new BootstrapPreparation(control,tag,command)
    bootstrapPreparation=Some(value)
    value

  /** 仅精确V3 Ordinary命令；不扩大为任意command入口。 */
  def installOrdinaryCertifiedBootstrapPreparationV3(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootOrdinaryV3): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain,"only exact initial bootstrap control may install V3 Ordinary preparation")
    require(control != null && (control.controlThread() eq initialMain),"actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted,"V3 Ordinary preparation must precede every submission")
    val value=new BootstrapPreparation(control,tag,command)
    bootstrapPreparation=Some(value)
    value

  def installFreshBootstrapPreparation5(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootFresh5): BootstrapPreparation =
    installDeploymentPreparation(control, tag, command)
  def installOrdinaryBootstrapPreparation5(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootOrdinary5): BootstrapPreparation =
    installDeploymentPreparation(control, tag, command)
  private def installDeploymentPreparation(control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand.BootFresh5 | MySqlOwnedIoCommand.BootOrdinary5): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "exact initial schema5 bootstrap control required")
    require(control != null && (control.controlThread() eq initialMain), "actual bridge control identity required")
    identity.requireTag(tag)
    require(command != null && bootstrapPreparation.isEmpty && !ownerSubmitted, "schema5 preparation precedes all submissions once")
    val value = new BootstrapPreparation(control, tag, command)
    bootstrapPreparation = Some(value)
    value

  /** 原控制线程持有两份真实work/completion；第一阶段不授予transfer/capture。 */
  def installDbFirstBootstrapPreparation(control:DedicatedBootstrapBridge,
      approved:ProductionBootstrapControl.DbFirstControl):BootstrapPreparation =
    require(Thread.currentThread() eq initialMain,"exact initial schema6 bootstrap control required")
    require(control != null && (control.controlThread() eq initialMain) && approved != null,
      "actual bridge and approved schema6 control required")
    require(bootstrapPreparation.isEmpty && !ownerSubmitted,"schema6 preparation precedes all submissions once")
    val ids=approved.identity()
    require(ids.boot()==identity.bootId && ids.bootGeneration()==identity.bootGeneration &&
      ids.connectionGeneration()==identity.connectionGeneration,"schema6 control differs from original adapter identity")
    val observe=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,ids.observeOperation())
    val acquire=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,ids.acquireOperation())
    val value=new BootstrapPreparation(control,observe,MySqlOwnedIoCommand.ObserveApprovedDbFirst(approved),
      Some((acquire,MySqlOwnedIoCommand.AcquireDbFirstWriter(approved.policy().writerLease(),approved.barrierWait()))))
    bootstrapPreparation=Some(value)
    value

  /** 普通配置首启仍登记同一 owned-IO preparation；初始化器不是factory.open的副作用。 */
  def installManagedDbFirstBootstrapPreparation(control: DedicatedBootstrapBridge,
      request: MySqlManagedBootstrapRequest): BootstrapPreparation =
    require(Thread.currentThread() eq initialMain, "exact initial managed bootstrap control required")
    require(control != null && (control.controlThread() eq initialMain) && request != null,
      "actual bridge and managed preparation holder required")
    require(bootstrapPreparation.isEmpty && !ownerSubmitted && dbFirstApprovedControl == null,
      "managed schema6 preparation precedes all submissions exactly once")
    val ids=request.identity
    require(ids.boot()==identity.bootId && ids.bootGeneration()==identity.bootGeneration &&
      ids.connectionGeneration()==identity.connectionGeneration, "managed request differs from exact adapter boot identity")
    val holder=request
    if !managedBootstrapRequestRef.compareAndSet(null,holder) then
      throw new IllegalStateException("managed preparation resource holder is already retained")
    val observe=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,ids.observeOperation())
    val acquire=IoOperationTag(identity.bootId,identity.bootGeneration,identity.connectionGeneration,ids.acquireOperation())
    val value=new BootstrapPreparation(control,observe,MySqlOwnedIoCommand.ObserveManagedDbFirst(holder),
      Some((acquire,MySqlOwnedIoCommand.AcquireDbFirstWriter(request.policy.writerLease(),
        com.wi.floatingpod.bootstrap.ManagedBootstrapPolicy.barrierWait()))))
    bootstrapPreparation=Some(value)
    value

  /** 可逃逸的是无JDBC能力的责任holder；所有推进仍由exact control调用。 */
  final class BootstrapPreparation private[MySqlOwnedIoAdapter] (control: DedicatedBootstrapBridge, tag: IoOperationTag,
      command: MySqlOwnedIoCommand,
      dbFirstSecond:Option[(IoOperationTag,MySqlOwnedIoCommand.AcquireDbFirstWriter)]=None):
    private[MySqlOwnedIoAdapter] def isDbFirstPreparation:Boolean=dbFirstSecond.nonEmpty
    private[MySqlOwnedIoAdapter] val repositoryEpochAtSubmission=acceptedRepositoryEpoch
    private[MySqlOwnedIoAdapter] val holder = OwnedIoPreparation.forBootstrap(control, io, preparationSignal, tag, Work(tag, command, repositoryEpochAtSubmission))
    @volatile private var second=Option.empty[OwnedIoPreparation[Work,MySqlOwnedIoReply]]
    @volatile private var startupFailure=Option.empty[Throwable]
    @volatile private var bothConsumed=false
    private var attempted=false
    private val combined=new java.util.concurrent.CompletableFuture[MySqlOwnedIoReply]()
    private val combinedStage=combined.minimalCompletionStage()
    /** 两份原观察都保留；不制造代表整个流程的假tag。 */
    def phaseObservations:Vector[IoPreparationObservation] = Vector(holder.observation)++second.map(_.observation)
    private def retainFailure(cause:Throwable):Unit =
      startupFailure=startupFailure.orElse(Some(cause))
      combined.completeExceptionally(startupFailure.get)
      try io.quiesce()
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(startupFailure.get,cleanup)
    def prepare(limit: Duration): CompletionStage[MySqlOwnedIoReply] =
      if dbFirstSecond.isEmpty then return holder.prepare(limit)
      require(Thread.currentThread() eq control.controlThread(),"original schema6 preparation control required")
      require(!attempted && startupFailure.isEmpty,"schema6 preparation is one-shot")
      require(limit != null && !limit.isNegative && !limit.isZero,"explicit positive schema6 preparation limit required")
      val budget=limit.toNanos
      val started=System.nanoTime()
      attempted=true
      def remaining():Duration =
        val left=budget-(System.nanoTime()-started)
        if left<=0L then throw new java.util.concurrent.TimeoutException("schema6 common preparation deadline expired")
        Duration.ofNanos(left)
      try
        holder.prepare(remaining()).toCompletableFuture().join() match
          case MySqlOwnedIoReply.DbFirstObserved(value) =>
            require(value != null && (value.identity eq identity),"original schema6 observation completion required")
          case _=>throw new IllegalStateException("schema6 first preparation returned another reply")
        require(holder.successfulCompletionConsumed,"schema6 observation completion remains unconsumed")
        remaining() // 不足时不创建或提交第二阶段。
        val signal=io.rotatePreparationCompletionSignal(preparationSignal)
        val (nextTag,nextCommand)=dbFirstSecond.get
        val next=OwnedIoPreparation.forBootstrap(control,io,signal,nextTag,Work(nextTag,nextCommand,repositoryEpochAtSubmission))
        second=Some(next) // 在任何第二阶段提交前保留原holder。
        val result=next.prepare(remaining()).toCompletableFuture().join()
        result match
          case MySqlOwnedIoReply.DbFirstSqlWriterAcquired(value)=>
            require(value != null && (value.identity eq identity) && value.state==MySqlDbFirstSession.State.SqlWriterAcquired,
              "original schema6 acquired completion required")
          case _=>throw new IllegalStateException("schema6 second preparation returned another reply")
        require(next.successfulCompletionConsumed && startupFailure.isEmpty,"schema6 acquisition completion remains unconsumed")
        bothConsumed=true
        combined.complete(result)
      catch
        case wrapped:java.util.concurrent.CompletionException if wrapped.getCause != null => retainFailure(wrapped.getCause)
        case cause:Throwable =>
          retainFailure(cause)
          throw cause
      combinedStage
    def finalizePreparation(limit: Duration): IoPreparationObservation =
      if dbFirstSecond.isEmpty then return holder.finalizePreparation(limit)
      require(Thread.currentThread() eq control.controlThread(),"original schema6 finalization control required")
      retainFailure(startupFailure.getOrElse(new IllegalStateException("schema6 preparation finalized; no further acquisition")))
      // 只收回已经存在的阶段。晚到Observe成功不能创建Acquire，也不能恢复外层成功。
      second.getOrElse(holder).finalizePreparation(limit)
    def stage: CompletionStage[MySqlOwnedIoReply] = if dbFirstSecond.isEmpty then holder.stage else combinedStage
    def observation: IoPreparationObservation = second.getOrElse(holder).observation
    private[MySqlOwnedIoAdapter] def bootstrapCompletionConsumedSuccessfully: Boolean =
      if dbFirstSecond.isEmpty then holder.successfulCompletionConsumed
      else bothConsumed && startupFailure.isEmpty && holder.successfulCompletionConsumed && second.exists(_.successfulCompletionConsumed)
    private[MySqlOwnedIoAdapter] def liveTransferPermitted: Boolean =
      if dbFirstSecond.isEmpty then holder.transferPermitted
      else bootstrapCompletionConsumedSuccessfully && holder.transferPermitted && second.exists(_.transferPermitted)
    private[MySqlOwnedIoAdapter] def requireApplicationValue(actualControl: DedicatedBootstrapBridge): MySqlOwnedIoReply =
      holder.requireOriginalControl(actualControl)
      require(liveTransferPermitted,"original complete preparation required")
      second.getOrElse(holder).requireLiveApplicationValue()
    private[MySqlOwnedIoAdapter] def retireApplicationValue(actualControl: DedicatedBootstrapBridge, callbacks: DedicatedBootstrapBridge.Callbacks, receipt: Object, original: MySqlOwnedIoReply): Unit =
      require(dbFirstSecond.isEmpty,"schema6 preparation cannot retire through legacy Ordinary application protocol")
      holder.requireOriginalControl(actualControl)
      holder.retireConsumedApplicationValue(actualControl, callbacks, receipt, original)

  private[floatingpod] def requireOrdinaryApplicationResult(control: DedicatedBootstrapBridge,
      preparation: BootstrapPreparation): MySqlProductionBootReply.OrdinaryApplicationReply =
    require(currentMainOwner.exists(_ eq Thread.currentThread()), "actual claimed adapter Main required")
    require(bootstrapPreparation.exists(_ eq preparation) && io.ownerTransferAbortCause.isEmpty,
      "exact successful bootstrap preparation required")
    preparation.requireApplicationValue(control) match
      case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ordinary
      case deployment:MySqlOwnedIoReply.DeploymentReceiptBooted
          if deployment.owned.phase==MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted &&
            (deployment.owned.identity eq identity)=>deployment
      case _ => throw new IllegalStateException("only original Ordinary application result can retire")
  private[floatingpod] def retireOrdinaryApplicationPreparation(control: DedicatedBootstrapBridge,
      callbacks: DedicatedBootstrapBridge.Callbacks, receipt: Object, preparation: BootstrapPreparation, original: MySqlProductionBootReply.OrdinaryApplicationReply): Unit =
    require(requireOrdinaryApplicationResult(control, preparation) eq original, "original Ordinary reply differs")
    preparation.retireApplicationValue(control, callbacks, receipt, original)

  private def requireOriginalOrdinaryBacking(preparation: BootstrapPreparation,
      original: MySqlProductionBootReply.OrdinaryApplicationReply): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && bootstrapPreparation.exists(_ eq preparation),
      "actual original preparation/Main required")
    original match
      case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>requiredSession().requireOriginalOrdinaryBootBacking(legacy.owned)
      case deployment:MySqlOwnedIoReply.DeploymentReceiptBooted=>
        require(deployment.owned.phase==MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted &&
          (deployment.owned.identity eq identity),"exact immutable schema5 Ordinary application identity required")
  private[floatingpod] def beginOrdinaryApplicationRetirement(control: DedicatedBootstrapBridge,
      preparation: BootstrapPreparation, original: MySqlProductionBootReply.OrdinaryApplicationReply,
      callbacks: com.wi.floatingpod.bootstrap.ProductionDedicatedBootstrapCallbacks,
      runtime: com.wi.floatingpod.network.DedicatedProjectionRuntime, receipt: Object,
      mode: ProductionBootstrapControl.Mode, limit: Duration): Unit =
    callbacks.requireOrdinaryApplicationRetirementEligibility(runtime, this, control, preparation, original, receipt, mode, limit)
    require(requireOrdinaryApplicationResult(control, preparation) eq original, "original application result required")
    requireOriginalOrdinaryBacking(preparation, original)
    original match
      case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>legacy.owned.beginRetirement(mode,limit)
      case _:MySqlOwnedIoReply.DeploymentReceiptBooted=>
        require(mode==ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED && limit!=null && !limit.isNegative && !limit.isZero,
          "original schema5 application policy required")
  private[floatingpod] def requireOrdinaryApplicationPreparationRetired(preparation: BootstrapPreparation): Unit =
    require(currentMainOwner.exists(_ eq Thread.currentThread()) && bootstrapPreparation.exists(_ eq preparation),
      "actual original preparation/Main required")
    preparation.holder.requireApplicationValueRetired()
  private[floatingpod] def finishOrdinaryApplicationRetirement(preparation: BootstrapPreparation,
      original: MySqlProductionBootReply.OrdinaryApplicationReply): Unit =
    requireOriginalOrdinaryBacking(preparation, original)
    original match
      case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>legacy.owned.finishRetirement()
      case _:MySqlOwnedIoReply.DeploymentReceiptBooted=>requireOrdinaryApplicationPreparationRetired(preparation)
  private[floatingpod] def failOrdinaryApplicationRetirement(preparation: BootstrapPreparation,
      original: MySqlProductionBootReply.OrdinaryApplicationReply, cause: Throwable): Unit =
    requireOriginalOrdinaryBacking(preparation, original)
    original match
      case legacy:MySqlOwnedIoReply.OrdinaryCertifiedBooted=>legacy.owned.retainFailure(cause)
      case _:MySqlOwnedIoReply.DeploymentReceiptBooted=>retainUnsettledStop(cause)

  private def requiredSession(): MySqlOwnedDatabaseSession =
    val session = legacySessionOrNull()
    if session == null then throw new IllegalStateException("normal owned MySQL session is unavailable before successful Boot")
    session

  private def observeApprovedDbFirst(work: Work, approved: ProductionBootstrapControl.DbFirstControl): MySqlOwnedIoReply =
    beginDbFirstObservation()
    observeApprovedDbFirstAfterBegin(work, approved)

  private def observeApprovedDbFirstAfterBegin(work: Work,
      approved: ProductionBootstrapControl.DbFirstControl): MySqlOwnedIoReply =
    dbFirstStage="controlled-observation-identity"
    require(approved != null,"original schema6 production control required")
    val controlled=approved.identity()
    require(controlled.boot()==identity.bootId && controlled.bootGeneration()==identity.bootGeneration &&
      controlled.connectionGeneration()==identity.connectionGeneration && work.tag.operationId==controlled.observeOperation(),
      "schema6 observation must use its exact controlled boot and operation")
    approved.consumeIdentityOnce()
    require(dbFirstApprovedControl==null,"schema6 original production control is one-shot")
    dbFirstApprovedControl=approved
    val policy=approved.policy()
    observeDbFirst(approved.metadata(),approved.sourceLimitProfile(),
      MySqlRuntimeAuditPolicy(policy.callerRoles(),policy.auditDefinerRoles(),MySqlExpectedNoImport,
        MySqlExpectedPhysicalFormat.Format2,policy.receiptFreshness()),Some(approved))

  private def executeRetainedRecallRelease(work:Work,receipt:MySqlDbFirstRecallDropReceipt):MySqlOwnedIoReply =
    val session=sessionRef.get() match
      case SessionSlot.DbFirst(actual)=>actual
      case _=>throw new IllegalStateException("retained original recall Session missing")
    require(receipt!=null && receipt.isRetained && receipt.tag==work.tag && work.saveBarrier.isEmpty &&
      (receipt.handle.issuer eq retainedPlanIssuerRef.get()),"exact original retained worker control required")
    try
      session.detachRetainedRecallEntryGraph(receipt)
      require(retiredDbFirstRecallHoldRef.compareAndSet(null,receipt),"original retired recall hold already occupied")
      dropRetainedRecallReferenceOnWorker(session,receipt)
      // 两个撤图Unit frame均已返回；此处没有qualification/runner局部引用。
      session.finishRetainedRecallRetirement(this,receipt)
      resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
      dbFirstStage="retained-recall-entry-retired-shared-fault-hold"
      MySqlOwnedIoReply.DbFirstRecallReleased(receipt)
    catch case cause:Throwable=>
      session.failRetainedRecallRetirement(cause)
      resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
      dbFirstStage="retained-recall-retirement-failed-resources-held"
      throw cause

  private def execute(work: Work): MySqlOwnedIoReply =
    currentExecutionIdentity.requireTag(work.tag)
    work.command match
      case MySqlOwnedIoCommand.ReleaseRetainedDbFirstRecall(receipt)=>return executeRetainedRecallRelease(work,receipt)
      case _=>()
    // 已接受尾项仍沿原tag交付Failure；不等待普通保存、不执行SQL、不进入普通fatal分类。
    val retiredRecall=blockingRetiredRecall
    if retiredRecall!=null then throw retiredRecall.retainedConfirmation.retainedFailure.get
    val retainedRecall=retainedDbFirstRecallRef.get()
    if retainedRecall!=null then throw retainedRecall.originalFailure

    try
      work.saveBarrier.foreach(_.awaitBeforeRepository())
      work.command match
        case MySqlOwnedIoCommand.ObserveDbFirst(metadata, profile, policy) =>
          beginDbFirstObservation()
          observeDbFirst(metadata,profile,policy,None)
        case MySqlOwnedIoCommand.ObserveApprovedDbFirst(approved) =>
          observeApprovedDbFirst(work,approved)
        case MySqlOwnedIoCommand.ObserveManagedDbFirst(request) =>
          beginDbFirstObservation()
          require(request != null && (request eq managedBootstrapRequestRef.get()),
            "original retained managed initialization request required")
          dbFirstStage="managed-initialization"
          val approved=request.prepareOnOwnedIoThread()
          require(approved != null && (request.preparedControl() eq approved),
            "completed managed request must retain its exact real schema6 control")
          // 在原工作线程、原observe operation内进入被控制的schema6观察；此处没有调用unverified routine。
          observeApprovedDbFirstAfterBegin(work,approved)
        case MySqlOwnedIoCommand.AcquireDbFirstWriter(lease, wait) =>
          if dbFirstApprovedControl != null then
            require(work.tag.operationId==dbFirstApprovedControl.identity().acquireOperation() &&
              lease==dbFirstApprovedControl.policy().writerLease() && wait==dbFirstApprovedControl.barrierWait(),
              "schema6 acquisition differs from original controlled operation or policy")
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("schema6 SQL writer requires its original observed Session")
          dbFirstStage = "sql-writer-acquisition"
          val observed = session.acquireSqlWriter(lease, wait)
          require(retainedPlanIssuerRef.get() eq session.retainedIssuerForOwnershipPublication(),
            "schema6 writer must retain its original published issuer")
          if dbFirstApprovedControl!=null then
            val backing=session.prepareMainRead(this,dbFirstApprovedControl,work.tag,work.repositoryEpochAtSubmission)
            require(dbFirstMainReadRef.compareAndSet(null,backing),"original schema6 Main read publication is one-shot")
            originalClaimedOwner.foreach((main,receipt)=>backing.claimMain(main,receipt))
          publishDbFirstMaintenance(session.currentSqlWriterFreshness())
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage = "sql-writer-acquired-not-ready"
          MySqlOwnedIoReply.DbFirstSqlWriterAcquired(observed)
        case MySqlOwnedIoCommand.TakeoverBootRecall(intent)=>
          require(intent!=null && intent.controlTag==work.tag,"actual original Boot control worker route required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("Boot takeover requires original acquired Session")
          dbFirstStage="boot-recall-takeover-read"
          val reservation=session.executeBootRecallTakeover(intent,work.repositoryEpochAtSubmission)
          currentDbFirstMailbox.requireCurrentRead(session.currentSqlWriterFreshness())
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage="boot-recall-takeover-inactive"
          MySqlOwnedIoReply.BootRecallTakenOver(intent,reservation,work.repositoryEpochAtSubmission)
        case MySqlOwnedIoCommand.RenewDbFirstWriter =>
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("schema6 SQL writer renewal requires its original acquired Session")
          dbFirstStage = "sql-writer-renewal"
          val observed = session.renewSqlWriter()
          publishDbFirstMaintenance(session.currentSqlWriterFreshness())
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage = "sql-writer-renewed-not-ready"
          MySqlOwnedIoReply.DbFirstSqlWriterRenewed(observed)
        case MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(exclusion)=>
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("refusal requires original acquired Session")
          require(exclusion!=null && exclusion.routeTag==work.tag,"original refusal J worker route required")
          dbFirstStage="refusal-transaction"
          val observed=session.executeDbFirstRecallRefusal(exclusion,work.repositoryEpochAtSubmission)
          session.pendingMainReadForCompletion(observed).foreach { pending=>
            require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original refusal successor publication slot occupied")
          }
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage="refusal-sql-retired-main-handoff-pending"
          MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observed)
        case MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(reservation)=>
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("journal requires original acquired Session")
          require(reservation!=null && reservation.tag==work.tag,"original journal worker operation required")
          dbFirstStage="journal-transaction"
          val observed=session.executeDbFirstRecallJournal(reservation,work.repositoryEpochAtSubmission)
          session.pendingMainReadForCompletion(observed).foreach { pending=>
            require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original journal successor publication slot occupied")
          }
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage="journal-sql-retired-main-handoff-pending"
          MySqlOwnedIoReply.DbFirstRecallJournalObserved(observed)
        case MySqlOwnedIoCommand.ExecuteDbFirstRecall(reservation) =>
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first recall requires its original acquired Session")
          require(reservation != null && reservation.tag == work.tag,
            "DB-first worker recall requires its original capture operation")
          dbFirstStage = "recall-transaction"
          try
            val observed = session.executeDbFirstRecall(reservation,work.repositoryEpochAtSubmission)
            session.pendingMainReadForCompletion(observed).foreach { pending=>
              require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original pending successor publication slot occupied")
            }
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage = "recall-sql-observed-main-cleanup-pending"
            MySqlOwnedIoReply.DbFirstRecallObserved(observed)
          catch case scala.util.control.NonFatal(cause)=>
            val qualification=MySqlOwnedIoAdapter.qualifyRetainedRecallFailure(cause) {
              session.retainedRecallQualification(reservation,cause)
            }
            require(retainedDbFirstRecallRef.compareAndSet(null,qualification),"original retained recall slot occupied")
            maintenanceMailbox.fail(cause);recallMaintenanceMailbox.fail(cause);dbFirstMaintenanceMailbox.fail(cause)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage="recall-positive-readback-retained-ordinary-admission-sealed"
            MySqlOwnedIoReply.DbFirstRecallRetained(qualification)
        case MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(reservation) =>
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first checkpoint requires its original acquired Session")
          require(reservation != null && reservation.tag == work.tag,
            "DB-first worker checkpoint requires its original capture operation")
          dbFirstStage = "checkpoint-transaction"
          val observed = session.executeDbFirstCheckpoint(reservation,work.repositoryEpochAtSubmission)
          session.pendingMainReadForCompletion(observed).foreach { pending=>
            require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original pending successor publication slot occupied")
          }
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage = "checkpoint-sql-observed-main-cleanup-pending"
          MySqlOwnedIoReply.DbFirstCheckpointObserved(observed)
        case MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(reservation) =>
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first preparation requires its original acquired Session")
          require(reservation!=null && reservation.tag==work.tag,
            "DB-first worker preparation requires its original reservation operation")
          dbFirstStage="initial-preparation-transaction"
          val observed=session.executeDbFirstInitialPreparation(reservation,work.repositoryEpochAtSubmission)
          observed match
            case accepted:MySqlDbFirstSession.DbFirstInitialPreparationObservation=>
              session.pendingMainReadForCompletion(accepted).foreach { pending=>
                require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original pending successor publication slot occupied")
              }
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage="initial-preparation-sql-retired-main-consumption-pending"
              MySqlOwnedIoReply.DbFirstInitialPreparationObserved(accepted)
            case rejected:MySqlDbFirstSession.DbFirstInitialPreparationRejection=>
              session.pendingMainReadForCompletion(rejected).foreach { pending=>
                require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original rejected successor publication slot occupied")
              }
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage="initial-preparation-rejected-main-handoff-pending"
              MySqlOwnedIoReply.DbFirstInitialPreparationRejected(rejected)
        case MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(reservation,activeLimit) =>
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first recall requires its original acquired Session")
          require(reservation != null && reservation.tag == work.tag,
            "DB-first worker recall requires its original capture operation")
          dbFirstStage = "initial-activation-transaction"
          val observed = session.executeDbFirstInitialActivation(reservation,activeLimit,work.repositoryEpochAtSubmission)
          session.pendingMainReadForCompletion(observed).foreach { pending=>
            require(dbFirstPendingMainReadRef.compareAndSet(null,pending),"original pending successor publication slot occupied")
          }
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          dbFirstStage = "initial-activation-sql-observed-main-publication-pending"
          MySqlOwnedIoReply.DbFirstInitialActivationObserved(observed)
        case MySqlOwnedIoCommand.Boot(metadata, policy, lease) =>
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(metadata != null && policy != null && lease != null, "explicit Boot inputs required")
          val worker = Thread.currentThread()
          val inputs = MySqlRuntimeInputs.assemble(metadata, worker, identity.bootId, identity.bootGeneration, policy)
          val session = MySqlOwnedDatabaseSession.openCertified(inputs, identity.connectionGeneration, lease)
          installLegacySession(session) // before inventory/facts/completion publication; owned exit hook already has its holderref.
          activeLimit = inputs.activeLimit
          val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
          publishAndArmMaintenance(session)
          retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
          MySqlOwnedIoReply.Booted(session.databaseAdmission, inventory, session.actualSourceLimitProfile)
        case MySqlOwnedIoCommand.BootFresh(metadata, policy, lease, request) =>
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(metadata != null && policy != null && lease != null && request != null, "explicit Fresh Boot inputs required")
          val worker = Thread.currentThread()
          val inputs = MySqlRuntimeInputs.assemble(metadata, worker, identity.bootId, identity.bootGeneration, policy,
            Some(request.plan))
          val session = MySqlOwnedDatabaseSession.allocateFresh(inputs, identity.connectionGeneration, lease, request)
          installLegacySession(session) // BEFORE initialize: native/JDBC/partialclose ownerholder cannot escape responsibility。
          freshConnectionSourceForAssembly match
            case Some(source) => session.initializeFreshWithVerifiedConnectionSource(source)
            case None => session.initializeFresh()
          activeLimit = inputs.activeLimit
          val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
          publishAndArmMaintenance(session)
          retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
          MySqlOwnedIoReply.FreshBooted(session.databaseAdmission, inventory, session.freshBootFacts, session.actualSourceLimitProfile)
        case MySqlOwnedIoCommand.BootNormalPreGenesis(metadata, policy, lease, request) =>
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(metadata != null && policy != null && lease != null && request != null, "explicit Normal Boot inputs required")
          val worker = Thread.currentThread()
          val inputs = MySqlRuntimeInputs.assemble(metadata, worker, identity.bootId, identity.bootGeneration, policy,
            Some(request.plan))
          val session = MySqlOwnedDatabaseSession.allocateNormalPreGenesis(inputs, identity.connectionGeneration, lease, request)
          installLegacySession(session) // BEFORE initialize: native/JDBC/partialclose ownerholder cannot escape responsibility。
          session.initializeNormalPreGenesis()
          activeLimit = inputs.activeLimit
          val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
          publishAndArmMaintenance(session)
          retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
          MySqlOwnedIoReply.NormalPreGenesisBooted(session.databaseAdmission, inventory, session.normalBootFacts, session.actualSourceLimitProfile)
        case MySqlOwnedIoCommand.BootCertifiedInspection(metadata, policy, request) =>
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(metadata != null && policy != null && request != null, "explicit certified inspection inputs required")
          val inputs = MySqlRuntimeInputs.assemble(metadata, Thread.currentThread(), identity.bootId,
            identity.bootGeneration, policy, Some(request.plan))
          val session = MySqlOwnedDatabaseSession.allocateCertifiedInspection(inputs, identity.connectionGeneration, request)
          installLegacySession(session) // registered before actual connection/native construction
          val observed = session.initializeCertifiedInspection()
          MySqlOwnedIoReply.CertifiedInspectionObserved(observed) // no admission/maintenance/business issuer
        case MySqlOwnedIoCommand.BootOrdinaryCertified(metadata, policy, lease, request) =>
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(metadata != null && policy != null && lease != null && request != null,
            "explicit ordinary Certified Boot inputs required")
          val inputs = MySqlRuntimeInputs.assembleCertifiedOrdinary(metadata, Thread.currentThread(), identity.bootId,
            identity.bootGeneration, policy, request.approvedOriginalCreation)
          val session = MySqlOwnedDatabaseSession.allocateCertifiedOrdinary(inputs, identity.connectionGeneration, lease, request)
          installLegacySession(session) // The actual IO exit owner retains native/session responsibility before initialization.
          val ordinaryFacts = certifiedOrdinaryConnectionSourceForAssembly match
            case Some(source) => session.initializeCertifiedOrdinaryWithVerifiedConnectionSource(source)
            case None => session.initializeCertifiedOrdinary()
          activeLimit = inputs.activeLimit
          if session.isRecallRecovering then
            val inventory = session.readRecallBootSnapshot(work.tag, work.repositoryEpochAtSubmission)
            val freshness = session.currentRecallFreshness()
            val link = new MySqlOwnedIoAdapter.RecallRecoveryLink(this, identity,
              freshness.bindingId, freshness.writerEpoch, session.recallReferences)
            require(recallRecoveryLink.compareAndSet(null, link), "restricted Boot link is issued once")
            publishRecallMaintenance(freshness)
            MySqlOwnedIoReply.RecallRecoveringBooted(link, inventory, ordinaryFacts, session.actualSourceLimitProfile)
          else
            val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
            publishAndArmMaintenance(session)
            retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
            session.issueOrdinaryBootResult(work.tag, work.repositoryEpochAtSubmission, inventory, ordinaryFacts)
        case MySqlOwnedIoCommand.BootFresh5(control, world, plan, boot, controlled) =>
          deploymentStage="fresh-envelope"
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(control != null && control.mode() == ProductionBootstrapControl.Mode.FRESH &&
            world != null && plan != null && boot != null && controlled != null, "exact schema5 Fresh request required")
          val current = control.fresh()
          require(current.boot() == identity.bootId && current.bootGeneration() == identity.bootGeneration &&
            current.connectionGeneration() == identity.connectionGeneration && current.submitOperation() == work.tag.operationId,
            "schema5 Fresh envelope differs from actual IO identity/tag")
          require(boot.bootId() == current.boot() && boot.generation() == current.bootGeneration() &&
            plan.creationOperationUuid() == current.creation() && plan.bootstrapOperationUuid() == current.bootstrap() &&
            plan.prepareWriteUuid() == current.prepare() && plan.certifyWriteUuid() == current.certify(), "schema5 Fresh phase identity differs")
          requireDeploymentControlled(control, controlled)
          control.requireFreshWorld(world)
          control.consumeFreshIdentityOnce()
          val owner = beginDeploymentOwner(control)
          val graph = MySqlRuntimeInputs.assembleDeploymentReceiptStatic(control.metadata(), owner, deploymentPolicy(control))
          // control pins complete A; graph binds semantic C. Their distinct domains are never compared.
          deploymentStage="fresh-session-inputs"
          val inputs = MySqlRuntimeInputs.assembleSessionInputs(graph, v3RootRef.get(), identity, MySqlDeploymentReceiptSessionInputs.Fresh)
          deploymentInputsRef.set(inputs) // pending real consumer before Session allocation
          require(inputs.sourceLimitProfile eq control.sourceLimitProfile(), "schema5 actual K profile differs from control")
          inputs.requirePlanData(plan)
          val request = MySqlDeploymentReceiptSession.FreshRequest(world, plan, boot, controlled,control.policy().writerLease(),work.tag)
          val session = MySqlDeploymentReceiptSession.allocateFresh(inputs, v3RootRef.get(), identity, request, control.policy().receiptFreshness())
          deploymentStage="fresh-session-install"
          installDeploymentSession(session) // original partial holder precedes every audit/native effect
          deploymentInputsRef.set(null) // exact inputs adopted by registered Session
          session.initializeFresh()
          retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity,initialMain,capacity,v3LedgerRef.get()))
          publishAndArmMaintenance(session.currentFreshness())
          session.issueBootResult(work.tag,work.repositoryEpochAtSubmission)
        case MySqlOwnedIoCommand.BootOrdinary5(control, world, current, original, controlled) =>
          deploymentStage="ordinary-envelope"
          require(!bootAttempted, "owned MySQL Boot is one-shot")
          bootAttempted = true
          require(control != null && control.mode() == ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED &&
            world != null && current != null && original != null && controlled != null, "exact schema5 Ordinary request required")
          require((current eq control.ordinary()) && (original eq control.originalSourcePins()) &&
            current.boot() == identity.bootId && current.bootGeneration() == identity.bootGeneration &&
            current.connectionGeneration() == identity.connectionGeneration && current.submitOperation() == work.tag.operationId,
            "schema5 Ordinary envelope/original pins differ from actual IO identity/tag")
          requireDeploymentControlled(control, controlled)
          control.requireOrdinaryWorld(world)
          control.consumeOrdinaryIdentityOnce()
          val owner = beginDeploymentOwner(control)
          val graph = MySqlRuntimeInputs.assembleDeploymentReceiptStatic(control.metadata(), owner, deploymentPolicy(control))
          deploymentStage="ordinary-session-inputs"
          val inputs = MySqlRuntimeInputs.assembleSessionInputs(graph, v3RootRef.get(), identity, MySqlDeploymentReceiptSessionInputs.Ordinary)
          deploymentInputsRef.set(inputs)
          require(inputs.sourceLimitProfile eq control.sourceLimitProfile(), "schema5 actual K profile differs from control")
          inputs.requirePlanData(original.plan())
          val session = MySqlDeploymentReceiptSession.allocateOrdinary(inputs, v3RootRef.get(), identity, original, control.policy().receiptFreshness(),
            current,controlled,control.policy().writerLease(),work.tag)
          deploymentStage="ordinary-session-install"
          installDeploymentSession(session)
          deploymentInputsRef.set(null)
          session.initializeOrdinary()
          if session.isRestrictedRecall then
            val freshness=session.currentRestrictedRecallFreshness()
            val link=new MySqlOwnedIoAdapter.RecallRecoveryLink(this,identity,freshness.bindingId,freshness.writerEpoch,session.restrictedReferences,Some(session.recallSourceAllocationForMain(initialMain)))
            require(recallRecoveryLink.compareAndSet(null,link),"original schema5 restricted Boot link is issued once")
            publishRecallMaintenance(freshness)
            session.issueRestrictedBoot(work.tag,work.repositoryEpochAtSubmission,link,v3LedgerRef.get())
          else
            retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity,initialMain,capacity,v3LedgerRef.get()))
            publishAndArmMaintenance(session.currentFreshness())
            session.issueBootResult(work.tag,work.repositoryEpochAtSubmission)
        case MySqlOwnedIoCommand.BootFreshV3(control,request) =>
          require(!bootAttempted,"owned MySQL Boot is one-shot")
          bootAttempted=true
          require(control != null && request != null && control.mode()==ProductionBootstrapControl.Mode.FRESH,"explicit Fresh StaticV3 required")
          val current=control.fresh()
          require(current.boot()==identity.bootId && current.bootGeneration()==identity.bootGeneration &&
            current.connectionGeneration()==identity.connectionGeneration && current.submitOperation()==work.tag.operationId,
            "StaticV3 Fresh boot/submit identity differs from actual worker envelope")
          require(request.boot.bootId()==current.boot() && request.boot.generation()==current.bootGeneration() &&
            request.plan.creationOperationUuid()==current.creation() && request.plan.bootstrapOperationUuid()==current.bootstrap() &&
            request.plan.prepareWriteUuid()==current.prepare() && request.plan.certifyWriteUuid()==current.certify(),"StaticV3 Fresh phase identity differs")
          require(request.controlledMetadata.metadataPath.toAbsolutePath.normalize()==control.metadataPath() &&
            request.controlledMetadata.approvedConfigurationSha256.equalsIgnoreCase(control.configurationSha256()),"StaticV3 controlled metadata differs")
          control.requireFreshWorld(request.requestedWorldPath)
          control.consumeFreshIdentityOnce()
          val owner=beginV3Owner(control)
          val policy=v3Policy(control)
          val inputs=MySqlRuntimeInputs.assembleV2(control.metadata(),owner,identity.bootId,identity.bootGeneration,policy,request.plan)
          val lease=owner.borrowForSession(v3LedgerRef.get());v3PendingLeaseRef.set(lease)
          owner.reserveSessionGraph()
          val session=MySqlOwnedDatabaseSession.allocateFreshV2(inputs,identity.connectionGeneration,control.policy().writerLease(),request,lease,v3LedgerRef.get())
          installLegacySession(session)
          v3PendingLeaseRef.set(null) // 同lease已由已登记Session采用；根/owner仍由Adapter保留。
          freshConnectionSourceForAssembly match
            case Some(source) => session.initializeFreshWithVerifiedConnectionSource(source)
            case None => session.initializeFresh()
          activeLimit = inputs.activeLimit
          val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
          publishAndArmMaintenance(session)
          retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
          MySqlOwnedIoReply.FreshBooted(session.databaseAdmission, inventory, session.freshBootFacts, session.actualSourceLimitProfile)
        case MySqlOwnedIoCommand.BootOrdinaryV3(control,request) =>
          require(!bootAttempted,"owned MySQL Boot is one-shot")
          bootAttempted=true
          require(control != null && request != null && control.mode()==ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED,"explicit Ordinary StaticV3 required")
          val current=control.ordinary()
          require(current.boot()==identity.bootId && current.bootGeneration()==identity.bootGeneration &&
            current.connectionGeneration()==identity.connectionGeneration && current.submitOperation()==work.tag.operationId,
            "StaticV3 Ordinary boot/submit identity differs from actual worker envelope")
          require(request.currentIdentity==current && (request.approvedOriginalCreation eq control.originalCreation()),"StaticV3 actual original/current identity required")
          require(request.controlledMetadata.metadataPath.toAbsolutePath.normalize()==control.metadataPath() &&
            request.controlledMetadata.approvedConfigurationSha256.equalsIgnoreCase(control.configurationSha256()),"StaticV3 controlled metadata differs")
          control.requireOrdinaryWorld(request.requestedWorldPath)
          control.consumeOrdinaryIdentityOnce()
          val owner=beginV3Owner(control)
          val policy=v3Policy(control)
          val inputs=MySqlRuntimeInputs.assembleCertifiedOrdinaryV2(control.metadata(),owner,identity.bootId,identity.bootGeneration,policy,request.approvedOriginalCreation)
          val lease=owner.borrowForSession(v3LedgerRef.get());v3PendingLeaseRef.set(lease)
          owner.reserveSessionGraph()
          val session=MySqlOwnedDatabaseSession.allocateCertifiedOrdinaryV2(inputs,identity.connectionGeneration,control.policy().writerLease(),request,lease,v3LedgerRef.get())
          installLegacySession(session)
          v3PendingLeaseRef.set(null)
          val ordinaryFacts = certifiedOrdinaryConnectionSourceForAssembly match
            case Some(source) => session.initializeCertifiedOrdinaryWithVerifiedConnectionSource(source)
            case None => session.initializeCertifiedOrdinary()
          activeLimit = inputs.activeLimit
          if session.isRecallRecovering then
            val inventory = session.readRecallBootSnapshot(work.tag, work.repositoryEpochAtSubmission)
            val freshness = session.currentRecallFreshness()
            val link = new MySqlOwnedIoAdapter.RecallRecoveryLink(this, identity,
              freshness.bindingId, freshness.writerEpoch, session.recallReferences)
            require(recallRecoveryLink.compareAndSet(null, link), "restricted Boot link is issued once")
            publishRecallMaintenance(freshness)
            MySqlOwnedIoReply.RecallRecoveringBooted(link, inventory, ordinaryFacts, session.actualSourceLimitProfile)
          else
            val inventory = session.readFullFormat3Snapshot(work.tag, work.repositoryEpochAtSubmission)
            publishAndArmMaintenance(session)
            retainedPlanIssuerRef.set(session.createRetainedPlanIssuer(identity, initialMain, capacity))
            session.issueOrdinaryBootResult(work.tag, work.repositoryEpochAtSubmission, inventory, ordinaryFacts)
        case MySqlOwnedIoCommand.Renew =>
          sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              val admission=session.renewLease()
              publishAndArmMaintenance(session)
              MySqlOwnedIoReply.Renewed(admission)
            case SessionSlot.DeploymentReceipt(session)=>
              val admission=session.renewLease()
              publishAndArmMaintenance(session.currentFreshness())
              MySqlOwnedIoReply.Renewed(admission)
            case _=>throw new IllegalStateException("original renewal Session missing")
        case MySqlOwnedIoCommand.PrepareNormalRecall(request, plan) =>
          val graph=sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.useRepository(work.tag,plan)(view=>view.normalRecall(plan,MySqlRepositoryOperation.PrepareNormalRecall(request),None)).get
            case SessionSlot.DeploymentReceipt(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.normalRecall(plan,MySqlRepositoryOperation.PrepareNormalRecall(request),None).get
            case _=>throw new IllegalStateException("original normal recall Session missing")
          require(!graph.checkpoint, "P returned a foreign recall phase")
          MySqlOwnedIoReply.NormalRecallPrepared(graph)
        case MySqlOwnedIoCommand.CheckpointNormalRecall(prepared, request, plan) =>
          val graph=sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.useRepository(work.tag,plan)(view=>view.normalRecall(plan,MySqlRepositoryOperation.CheckpointNormalRecall(request),Some(prepared))).get
            case SessionSlot.DeploymentReceipt(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.normalRecall(plan,MySqlRepositoryOperation.CheckpointNormalRecall(request),Some(prepared)).get
            case _=>throw new IllegalStateException("original normal recall Session missing")
          require(graph.checkpoint, "C returned a foreign recall phase")
          MySqlOwnedIoReply.NormalRecallCheckpointed(graph)
        case MySqlOwnedIoCommand.PrepareControlledDebug(request, plan) =>
          val execution=sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.useRepository(work.tag,plan)(view=>view.controlledDebug(plan,request,None,activeLimit)).get
            case SessionSlot.DeploymentReceipt(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.controlledDebug(plan,request,None,activeLimit).get
            case _=>throw new IllegalStateException("original controlled debug Session missing")
          execution match
            case MySqlControlledDebugExecution.Prepared(graph) => MySqlOwnedIoReply.ControlledDebugPrepared(graph)
            case _ => throw new IllegalStateException("prepare returned a foreign controlled phase")
        case MySqlOwnedIoCommand.CommitControlledDebug(prepared, pose, limit, plan) =>
          require(limit > 0 && limit <= activeLimit, "controlled capacity exceeds exact Boot active limit")
          require(MySqlControlledDebugWorkflowMaterializer.exactPose(pose, prepared.request.targetPose), "commit pose drift")
          val execution=sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.useRepository(work.tag,plan)(view=>view.controlledDebug(plan,prepared.request,Some(prepared),limit)).get
            case SessionSlot.DeploymentReceipt(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              session.controlledDebug(plan,prepared.request,Some(prepared),limit).get
            case _=>throw new IllegalStateException("original controlled debug Session missing")
          execution match
            case MySqlControlledDebugExecution.Committed(graph) => MySqlOwnedIoReply.ControlledDebugCommitted(graph)
            case _ => throw new IllegalStateException("commit returned a foreign controlled phase")
        case MySqlOwnedIoCommand.PrepareControlledUnbound(request, plan) =>
          throw new UnsupportedOperationException("formal F3 controlled debug command cannot fall back to F2 staging")
        case MySqlOwnedIoCommand.ReleaseRequestRead(tag, receipt) =>
          require(tag == work.tag, "request-read ACK must reuse original accepted slot")
          try
            sessionRef.get() match
              case SessionSlot.Legacy(session)=>session.releaseRetainedPlan(tag,receipt)
              case SessionSlot.DeploymentReceipt(session)=>session.releaseRetainedPlan(tag,receipt)
              case _=>throw new IllegalStateException("original retained plan Session missing")
            MySqlOwnedIoReply.RetainedPlanReleased(tag)
          catch
            case cause: Throwable if scala.util.control.NonFatal(cause) =>
              MySqlOwnedIoReply.RetainedPlanReleaseFailed(tag, cause)
        case command @ MySqlOwnedIoCommand.PrepareRestrictedSourceReservePlan(intent) =>
          val pending=restrictedSourcePlanPending.get()
          require(pending!=null && pending.tag==work.tag && (pending.command eq command) &&
            (pending.intent eq intent) && work.repositoryEpochAtSubmission==pending.intent.expectedSourceObservation.settledRepositoryEpoch,
            "restricted source plan worker must retain the exact Main submission tag/command/intent/epoch")
          pending.state match
            case RestrictedSourcePlanState.Submitting | RestrictedSourcePlanState.Accepted | RestrictedSourcePlanState.Unknown(_) => ()
            case _=>throw new IllegalStateException("restricted source plan worker reached an unsubmitted or settled route")
          val plan=requiredDeploymentSession().prepareRestrictedSourceReservePlan(intent)
          require(plan!=null && (plan.intent eq intent),"actual Fenced source reserve plan must retain the exact intent")
          MySqlOwnedIoReply.RestrictedSourceReservePlanPrepared(
            new MySqlOwnedIoAdapter.RestrictedSourceReservePlanBacking(this,work.tag,command,intent,plan))
        case command @ MySqlOwnedIoCommand.ExecuteRestrictedSourceReserveR0(backing,planAck) =>
          val pending=restrictedSourceR0Pending.get()
          require(pending!=null && pending.tag==work.tag && (pending.command eq command) &&
            (pending.backing eq backing) && (pending.planAck eq planAck),
            "restricted SOURCE R0 worker must retain its exact accepted pending/tag/command/backing/ACK")
          pending.requireWorkerWork(work)
          val plan=backing.requireR0ExecutionFor(this,planAck,pending.plan.intent)
          require((plan eq pending.plan) && (plan.intent eq backing.exactPlan.intent) &&
            work.repositoryEpochAtSubmission==pending.expectedSourceEpoch &&
            pending.nextAcceptedEpoch==pending.preparation.nextAcceptedEpoch,
            "restricted SOURCE R0 worker plan/work epoch differs from its actual accepted owner")
          try
            val outcome=requiredDeploymentSession().executeRestrictedSourceReserveR0(
              plan,work.tag,work.repositoryEpochAtSubmission)
            require(outcome!=null && outcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.Running,
              "restricted SOURCE R0 Session must retain one typed terminal/settlement outcome")
            MySqlOwnedIoReply.RestrictedSourceR0OutcomeRetained(outcome)
          catch
            case scala.util.control.NonFatal(cause)=>MySqlOwnedIoReply.RestrictedSourceR0WorkerFailed(cause)
        case command @ MySqlOwnedIoCommand.DropRestrictedSourceReservePlan(receipt) =>
          val pending=restrictedSourcePlanDropPending.get()
          require(pending!=null && pending.tag==work.tag && (pending.command eq command) &&
            (pending.receipt eq receipt),
            "restricted source plan drop worker must retain its exact Main tag, command, and Fenced receipt")
          pending.state match
            case RestrictedSourcePlanDropState.Submitting | RestrictedSourcePlanDropState.Accepted |
                RestrictedSourcePlanDropState.Unknown(_) => ()
            case _=>throw new IllegalStateException("restricted source plan drop worker reached an unsubmitted or settled route")
          val ack=requiredDeploymentSession().dropRestrictedSourceReservePlan(receipt)
          if ack==null then throw new IllegalStateException("Fenced source plan drop returned no typed ACK")
          ack.requireFor(receipt)
          MySqlOwnedIoReply.RestrictedSourceReservePlanDropped(ack)
        case MySqlOwnedIoCommand.ReleaseDbFirstRecall(receipt) =>
          require(receipt != null && receipt.tag == work.tag, "DB-first release ACK must reuse original operation")
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first release requires its original Session")
          dbFirstStage = "recall-resource-retirement"
          try
            session.releaseDbFirstRecall(receipt)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage = "recall-resources-retired-main-ack-pending"
            MySqlOwnedIoReply.DbFirstRecallReleased(receipt)
          catch
            case local: MySqlDbFirstLocalRetirementException if local.receipt eq receipt =>
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage = "recall-local-retirement-failed"
              MySqlOwnedIoReply.DbFirstRecallRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseRetainedDbFirstRecall(_) =>
          throw new IllegalStateException("retained recall control bypassed its dedicated worker dispatch")
        case MySqlOwnedIoCommand.ReleaseRejectedPreparationMainRead(confirmation) =>
          require(confirmation!=null && confirmation.tag==work.tag,"original rejected preparation control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original rejected preparation Session required")
          session.releaseRejectedPreparationMainRead(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.RejectedPreparationMainReadReleased(confirmation)
        case MySqlOwnedIoCommand.RetireRejectedPreparation(confirmation) =>
          require(confirmation!=null && confirmation.tag==work.tag,"original rejected retirement control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original rejected retirement Session required")
          session.retireRejectedPreparation(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.RejectedPreparationRetired(confirmation)
        case MySqlOwnedIoCommand.RetireJournalForCapture(transfer)=>
          require(transfer!=null && transfer.tag==work.tag,"original journal/C worker tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original journal/C Session required")
          try
            session.retireJournalForCapture(transfer)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            MySqlOwnedIoReply.JournalRetiredForCapture(transfer)
          catch case local:MySqlDbFirstJournalCaptureLocalRetirementException if local.transfer eq transfer=>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            MySqlOwnedIoReply.JournalCaptureRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseRefusalFinal(receipt)=>
          require(receipt!=null && receipt.tag==work.tag,"original refusal final J route required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original refusal final Session required")
          dbFirstStage="refusal-final-resource-retirement"
          try
            session.releaseRefusalFinal(receipt)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage="refusal-final-resources-retired-main-ack-pending"
            MySqlOwnedIoReply.RefusalFinalReleased(receipt)
          catch case local:MySqlDbFirstRefusalFinalLocalRetirementException if local.receipt eq receipt=>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage="refusal-final-local-retirement-failed"
            MySqlOwnedIoReply.RefusalFinalRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseRefusalMainRead(confirmation)=>
          require(confirmation!=null && confirmation.tag==work.tag,"original refusal J control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original refusal Session required")
          session.releaseRefusalMainRead(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.RefusalMainReadReleased(confirmation)
        case MySqlOwnedIoCommand.ReleaseJournalMainRead(confirmation) =>
          require(confirmation!=null && confirmation.tag==work.tag,"original journal control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original journal Session required")
          session.releaseJournalMainRead(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.JournalMainReadReleased(confirmation)
        case MySqlOwnedIoCommand.ReleasePreparationMainRead(confirmation) =>
          require(confirmation!=null && confirmation.tag==work.tag,"original preparation control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original preparation Session required")
          session.releasePreparationMainRead(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.PreparationMainReadReleased(confirmation)
        case MySqlOwnedIoCommand.ReleaseDbFirstPreparationFinal(receipt) =>
          require(receipt != null && receipt.tag == work.tag, "DB-first release ACK must reuse original operation")
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first release requires its original Session")
          dbFirstStage = "initial-publication-resource-retirement"
          try
            session.releaseDbFirstPreparationFinal(receipt)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage = "initial-publication-resources-retired-main-ack-pending"
            MySqlOwnedIoReply.DbFirstPreparationFinalReleased(receipt)
          catch
            case local: MySqlDbFirstPreparationLocalRetirementException if local.receipt eq receipt =>
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage = "initial-publication-local-retirement-failed"
              MySqlOwnedIoReply.DbFirstPreparationFinalRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseCheckpointMainRead(confirmation) =>
          require(confirmation!=null && confirmation.tag==work.tag,"original checkpoint control tag required")
          val session=sessionRef.get() match
            case SessionSlot.DbFirst(actual)=>actual
            case _=>throw new IllegalStateException("original checkpoint Session required")
          session.releaseCheckpointMainRead(confirmation)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          MySqlOwnedIoReply.CheckpointMainReadReleased(confirmation)
        case MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(receipt) =>
          require(receipt != null && receipt.tag == work.tag, "DB-first release ACK must reuse original operation")
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first release requires its original Session")
          dbFirstStage = "initial-publication-resource-retirement"
          try
            session.releaseDbFirstCheckpointFinal(receipt)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage = "initial-publication-resources-retired-main-ack-pending"
            MySqlOwnedIoReply.DbFirstCheckpointFinalReleased(receipt)
          catch
            case local: MySqlDbFirstCheckpointLocalRetirementException if local.receipt eq receipt =>
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage = "initial-publication-local-retirement-failed"
              MySqlOwnedIoReply.DbFirstCheckpointFinalRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseDbFirstInitialPublication(receipt) =>
          require(receipt != null && receipt.tag == work.tag, "DB-first release ACK must reuse original operation")
          val session = sessionRef.get() match
            case SessionSlot.DbFirst(actual) => actual
            case _ => throw new IllegalStateException("DB-first release requires its original Session")
          dbFirstStage = "initial-publication-resource-retirement"
          try
            session.releaseDbFirstInitialPublication(receipt)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
            dbFirstStage = "initial-publication-resources-retired-main-ack-pending"
            MySqlOwnedIoReply.DbFirstInitialPublicationReleased(receipt)
          catch
            case local: MySqlDbFirstInitialLocalRetirementException if local.receipt eq receipt =>
              resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
              dbFirstStage = "initial-publication-local-retirement-failed"
              MySqlOwnedIoReply.DbFirstInitialPublicationRetirementFailed(local)
        case MySqlOwnedIoCommand.ReleaseRetainedPlan(tag, receipt) =>
          require(tag == work.tag, "retained plan ACK must reuse original accepted slot")
          try
            sessionRef.get() match
              case SessionSlot.Legacy(session)=>session.releaseRetainedPlan(tag,receipt)
              case SessionSlot.DeploymentReceipt(session)=>session.releaseRetainedPlan(tag,receipt)
              case _=>throw new IllegalStateException("original retained plan Session missing")
            MySqlOwnedIoReply.RetainedPlanReleased(tag)
          catch
            case cause: Throwable if scala.util.control.NonFatal(cause) =>
              MySqlOwnedIoReply.RetainedPlanReleaseFailed(tag, cause)
        case MySqlOwnedIoCommand.ReleaseModeProjection(tag, receipt) =>
          require(tag == work.tag, "projection release command must reuse its exact completed operation tag")
          try
            sessionRef.get() match
              case SessionSlot.Legacy(session)=>session.releaseModeProjection(tag,receipt)
              case SessionSlot.DeploymentReceipt(session)=>session.releaseModeProjection(tag,receipt)
              case _=>throw new IllegalStateException("original mode projection Session missing")
            MySqlOwnedIoReply.ModeProjectionReleased(tag)
          catch
            case cause: Throwable if scala.util.control.NonFatal(cause) =>
              MySqlOwnedIoReply.ModeProjectionReleaseFailed(tag, cause)
        case MySqlOwnedIoCommand.ReleaseDriftProjection(tag, receipt) =>
          require(tag == work.tag, "projection release command must reuse its exact completed operation tag")
          try
            sessionRef.get() match
              case SessionSlot.Legacy(session)=>session.releaseDriftProjection(tag,receipt)
              case SessionSlot.DeploymentReceipt(session)=>session.releaseDriftProjection(tag,receipt)
              case _=>throw new IllegalStateException("original drift projection Session missing")
            MySqlOwnedIoReply.DriftProjectionReleased(tag)
          catch
            case cause: Throwable if scala.util.control.NonFatal(cause) =>
              MySqlOwnedIoReply.DriftProjectionReleaseFailed(tag, cause)
        case MySqlOwnedIoCommand.CommitTrackedDrift(request, plan) =>
          try
            val execution=sessionRef.get() match
              case SessionSlot.Legacy(session)=>
                work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
                session.useRepository(work.tag,plan)(view=>view.commitTrackedDrift(plan,request,activeLimit)).get
              case SessionSlot.DeploymentReceipt(session)=>
                work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
                session.commitTrackedDrift(plan,request,activeLimit).get
              case _=>throw new IllegalStateException("original drift Session missing")
            execution match
              case MySqlTrackedDriftExecution.Rejected(outcome) => MySqlOwnedIoReply.TrackedDriftRejected(outcome)
              case MySqlTrackedDriftExecution.Committed(graph) => MySqlOwnedIoReply.TrackedDriftCommitted(graph)
          catch
            case cause: Throwable =>
              val responsibility = MySqlTrackedDriftExecutionFailure(work.tag, request, plan, cause)
              Option(retainedPlanIssuerRef.get()).foreach { issuer =>
                try issuer.markUnknown(plan, cause)
                catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
              }
              driftExecutionFailure.compareAndSet(null, responsibility)
              try sessionRef.get() match
                case SessionSlot.Legacy(session)=>session.markFailed(cause)
                case SessionSlot.DeploymentReceipt(session)=>session.markFailed(cause)
                case _=>throw new IllegalStateException("original failed drift Session missing",cause)
              catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
              maintenanceMailbox.fail(cause)
              if scala.util.control.NonFatal(cause) then MySqlOwnedIoReply.TrackedDriftFailed(responsibility)
              else throw cause
        case MySqlOwnedIoCommand.CommitOwnedMode(request, plan) =>
          require(request != null, "immutable owned mode CAS request required")
          var actualAck = Option.empty[com.wi.floatingpod.storage.OperatingModeChangeOutcome]
          try
            val execution=sessionRef.get() match
              case SessionSlot.Legacy(session)=>
                work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
                session.useRepository(work.tag,plan)(view=>view.commitOwnedMode(plan,request,activeLimit)).get
              case SessionSlot.DeploymentReceipt(session)=>
                work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
                session.commitOwnedMode(plan,request,activeLimit).get
              case _=>throw new IllegalStateException("original mode Session missing")
            require(execution != null, "actual owned mode execution result is required")
            execution match
              case MySqlOwnedModeExecution.Rejected(outcome) => MySqlOwnedIoReply.OwnedModeRejected(outcome)
              case MySqlOwnedModeExecution.AlreadySet(readback) => MySqlOwnedIoReply.OwnedModeAlreadySet(readback)
              case MySqlOwnedModeExecution.Committed(graph) =>
                require(graph != null, "actual F3 changed-mode graph is required")
                actualAck = Some(graph.actualCommit)
                MySqlOwnedIoReply.OwnedModeCommitted(graph)
          catch
            case cause: Throwable =>
              // None是未观察到ACK，可能CommitUnknown；绝不假定无效果或自动重发。
              val responsibility = MySqlOwnedModeExecutionFailure(work.tag, request, actualAck, cause)
              Option(retainedPlanIssuerRef.get()).foreach { issuer =>
                try issuer.markUnknown(plan, cause)
                catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
              }
              ownedModeExecutionFailure.compareAndSet(null, responsibility)
              try sessionRef.get() match
                case SessionSlot.Legacy(session)=>session.markFailed(cause)
                case SessionSlot.DeploymentReceipt(session)=>session.markFailed(cause)
                case _=>throw new IllegalStateException("original failed mode Session missing",cause)
              catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
              maintenanceMailbox.fail(cause)
              cause.addSuppressed(new MySqlOwnedModeObligationNotice(work.tag, request, actualAck))
              if actualAck.nonEmpty && scala.util.control.NonFatal(cause) then
                MySqlOwnedIoReply.OwnedModeReadbackFailed(responsibility)
              else throw cause // 原Unknown/fatal保cause+request，IO保所有completion/reservation。
        case MySqlOwnedIoCommand.CommitControlledUnbound(receipt, pose, limit, plan) =>
          throw new UnsupportedOperationException("formal F3 controlled debug command cannot fall back to F2 staging")
        case MySqlOwnedIoCommand.ReadOriginalDeployment(operation,plan)=>
          val session=requiredDeploymentSession()
          work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
          MySqlOwnedIoReply.Operation(MySqlRepositoryReply.OriginalDeploymentOperation(session.readOriginalDeployment(plan,operation).get))
        case MySqlOwnedIoCommand.ReadRequestPreview(request, plan) =>
          request.operations.foreach(validateOperation)
          sessionRef.get() match
            case SessionSlot.Legacy(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              MySqlOwnedIoReply.Transaction(session.executeQueries(work.tag,plan,request.operations))
            case SessionSlot.DeploymentReceipt(session)=>
              work.command.requireRepositoryPlan(work.tag,session.actualSourceLimitProfile)
              MySqlOwnedIoReply.Transaction(session.requestReadPreview(plan,request).get)
            case _=>throw new IllegalStateException("original request-read Session missing")
        case MySqlOwnedIoCommand.Run(operation, plan) =>
          MySqlOwnedIoCommand.requireGenericRoute(work.command)
          val session = requiredSession()
          work.command.requireRepositoryPlan(work.tag, session.actualSourceLimitProfile)
          validateOperation(operation)
          if MySqlFormat3ReadDispatcher.isRead(operation) then
            MySqlOwnedIoReply.Operation(session.executeQueries(work.tag,plan,Vector(operation)).head)
          else operation match
            case MySqlRepositoryOperation.UpdateInactiveDomain(request) =>
              MySqlOwnedIoReply.Operation(MySqlRepositoryReply.InactiveDomainUpdate(
                session.useRepository(work.tag, plan)(view => view.updateInactiveDomain(plan, request)).get))
            case _ =>
              MySqlOwnedIoReply.Operation(session.useRepository(work.tag, plan)(view => MySqlImmutableCommandDispatcher.execute(operation, view)).get)
        case MySqlOwnedIoCommand.Transaction(operations, plan) =>
          MySqlOwnedIoCommand.requireGenericRoute(work.command)
          val session = requiredSession()
          work.command.requireRepositoryPlan(work.tag, session.actualSourceLimitProfile)
          require(operations != null, "immutable transaction vector required")
          operations.foreach(validateOperation)
          if operations.nonEmpty && operations.forall(MySqlFormat3ReadDispatcher.isRead) then
            MySqlOwnedIoReply.Transaction(session.executeQueries(work.tag,plan,operations))
          else if operations.exists(MySqlFormat3ReadDispatcher.isRead) then
            throw new UnsupportedOperationException("mixed read/mutation vector has no admitted format3 query route")
          else MySqlOwnedIoReply.Transaction(session.useRepository(work.tag, plan)(view => MySqlImmutableCommandDispatcher.transaction(operations, view)).get)
    catch
      case cause: Throwable =>
        if dbFirstStage != null then
          if dbFirstFatal == null then dbFirstFatal = cause
          else MySqlOwnedIoAdapter.suppressExitCleanup(dbFirstFatal, cause)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(dbFirstStage, Some(dbFirstFatal), true))
        if deploymentStage != null then
          if deploymentFatal == null then deploymentFatal=cause
          else MySqlOwnedIoAdapter.suppressExitCleanup(deploymentFatal,cause)
          resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(deploymentStage,Some(deploymentFatal),true))
        originalCommandPlan(work.command).foreach { plan =>
          Option(retainedPlanIssuerRef.get()).foreach { issuer =>
            try issuer.markUnknown(plan, cause)
            catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
          }
        }
        maintenanceMailbox.fail(cause)
        recallMaintenanceMailbox.fail(cause)
        dbFirstMaintenanceMailbox.fail(cause)
        sessionRef.get() match
          case SessionSlot.DbFirst(session) =>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          case SessionSlot.DeploymentReceipt(session) =>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(Some(cause), true))
          case _ =>
            val session = legacySessionOrNull()
            if session != null then
              try session.markFailed(cause)
              catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
              session.closeObservation.foreach(report =>
                resourceObservation.set(MySqlOwnedIoResourceObservation.SessionObserved(report)))
            else
              cause.getSuppressed.collectFirst { case context: MySqlOwnedSessionInitializationObservation => context.closeObservation }
                .foreach(report => resourceObservation.set(MySqlOwnedIoResourceObservation.PartialInitializationObserved(report)))
        throw cause

  private def publishAndArmMaintenance(session: MySqlOwnedDatabaseSession): Unit =
    publishAndArmMaintenance(session.currentFreshness())

  private def publishAndArmMaintenance(value:MySqlOwnedAdmissionFreshness):Unit =
    require(maintenanceMailbox.publish(value), "actual admission publication was stale or mailbox has sticky failure")
    if enableAdmissionMaintenance then
      val now = System.nanoTime()
      ioRef.get().scheduleMaintenanceDeadline(IoMaintenanceDeadline(now, value.nextMaintenanceAfter(now)))

  private def publishRecallMaintenance(value: MySqlOwnedAdmissionFreshness): Unit =
    require(recallMaintenanceMailbox.publish(value), "original Recall maintenance publication unavailable")
    if enableAdmissionMaintenance then
      val now = System.nanoTime()
      ioRef.get().scheduleMaintenanceDeadline(IoMaintenanceDeadline(now, value.nextMaintenanceAfter(now)))

  /** 先锁定一次Boot及原空owner，再改变family路由或消费外部control。 */
  private def beginDbFirstObservation():Unit =
    require(!bootAttempted,"owned MySQL Boot is one-shot")
    bootAttempted=true
    require(sessionRef.get()==null && v3LedgerRef.get()==null && v3RootRef.get()==null && v3AssemblyRef.get()==null,
      "schema6 observation requires the original unused adapter owner")

  private def observeDbFirst(metadata: MySqlRuntimeMetadata.DbFirstStatic, profile: SourceLimitProfile,
      policy: MySqlRuntimeAuditPolicy, approved: Option[ProductionBootstrapControl.DbFirstControl]): MySqlOwnedIoReply =
    dbFirstStage = "static-root-and-owner"
    require(metadata != null && profile != null && policy != null &&
      policy.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && policy.expectedImport == MySqlExpectedNoImport,
      "exact schema6 metadata/profile/ordinary audit policy required")
    require(sessionRef.get() == null && v3LedgerRef.get() == null && v3RootRef.get() == null && v3AssemblyRef.get() == null,
      "schema6 observation requires the original unused adapter owner")
    val ledger = new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    v3LedgerRef.set(ledger)
    val root = MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile, profile.maxSqlPacketBytes(), ledger)
    v3RootRef.set(root)
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    v3AssemblyRef.set(owner)
    dbFirstStage = "static-B-K-C"
    val graph = MySqlRuntimeInputs.assembleDbFirstStatic(metadata, owner, policy)
    if approved.nonEmpty then
      dbFirstStage = "controlled-configuration-identity"
      require(graph.configurationSha256.equalsIgnoreCase(approved.get.expectedConfigurationSha256()),
        "schema6 assembled configuration differs from approved control")
    dbFirstStage = "session-inputs"
    val inputs = MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph, root, identity)
    dbFirstInputsRef.set(inputs)
    dbFirstStage = "session-allocation"
    val session = try MySqlDbFirstSession.allocate(inputs, root, identity, policy.maxReceiptAge, ledger,
      currentMainOwner.getOrElse(throw new IllegalStateException("schema6 actual adapter Main unavailable")), capacity)
    catch
      case retained: MySqlDbFirstSession.AllocationFailure =>
        require(sessionRef.compareAndSet(null, SessionSlot.DbFirst(retained.retainedSession)), "original failed schema6 Session slot changed")
        throw retained
    require(sessionRef.compareAndSet(null, SessionSlot.DbFirst(session)), "exactly one owned schema6 Session may install")
    dbFirstInputsRef.set(null)
    // 先登记原issuer的所有权，确保acquire前的Main移交也覆盖原Session；freshness另行授予。
    require(retainedPlanIssuerRef.compareAndSet(null,session.retainedIssuerForOwnershipPublication()),
      "schema6 original issuer ownership publication is one-shot")
    approved.filter(_.managedBundle().isPresent).foreach(session.registerManagedFirstBinding)
    dbFirstStage = "database-observation"
    session.initialize()
    resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
    dbFirstStage = "database-observed-no-admission"
    MySqlOwnedIoReply.DbFirstObserved(session.observationOnOwnerThread)

  private def publishDbFirstMaintenance(value:MySqlOwnedAdmissionFreshness):Unit =
    require(value.profile == MySqlSchemaAuditProfile.DbFirst1 && currentDbFirstMailbox.publish(value),
      "actual schema6 SQL windows are stale or publication failed")
    if enableAdmissionMaintenance then
      val now=System.nanoTime()
      ioRef.get().scheduleMaintenanceDeadline(IoMaintenanceDeadline(now,value.nextMaintenanceAfter(now)))

  private def maintainResources(): IoMaintenanceDeadline =
    if retainedServiceFailureRef.get()!=null then
      return IoMaintenanceDeadline.after(Duration.ofMinutes(30)) // 等原Main撤借用后quiesce；不重试失败恢复或消费exit hook。
    try
      ownedRecoveredService match
        case Some((service,mailbox))=>
          sessionRef.get() match
            case SessionSlot.DbFirst(value)=>require(value eq service,"actual recovered maintenance slot required")
            case _=>throw new IllegalStateException("actual recovered maintenance slot missing")
          service.requireOwnedServiceCleanup(this,mailbox)
          service.renewSqlWriter()
          val freshness=service.currentSqlWriterFreshness()
          require(mailbox.publish(freshness),"actual recovered service mailbox renewal failed")
          resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(service.resourceObservation))
          val now=System.nanoTime()
          return IoMaintenanceDeadline(now,freshness.nextMaintenanceAfter(now))
        case None=>()
      val retiredRecall=retiredDbFirstRecallHoldRef.get()
      if retiredRecall!=null then
        val session=sessionRef.get() match
          case SessionSlot.DbFirst(actual)=>actual
          case _=>throw new IllegalStateException("retired recall shared-fault Session missing")
        session.bindRetiredRecallCleanupAdapter(this,retiredRecall)
        val deadline=session.maintainRetiredRecallHold(retiredRecall)
        session.transferRetiredRecallIssuerIfDrained(this,retiredRecall)
        val current=sessionRef.get() match
          case SessionSlot.DbFirst(value)=>value
          case _=>throw new IllegalStateException("schema6 service transfer lost current slot")
        resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(current.resourceObservation))
        return deadline // 显式Failed资源保管；不再轮询旧q或进入普通writer renewal。
      val retainedRecall=retainedDbFirstRecallRef.get()
      if retainedRecall!=null then
        val session=sessionRef.get() match
          case SessionSlot.DbFirst(actual)=>actual
          case _=>throw new IllegalStateException("retained original recall Session missing")
        val deadline=session.maintainRetainedRecall(retainedRecall)
        resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
        return deadline // 不续租、不清failure、不发布普通freshness。
      if restrictedSourceR0Pending.get()!=null then
        // Retained R0 execution/settlement owns the original Session; no renewal or second SQL route may run.
        return IoMaintenanceDeadline.after(Duration.ofMinutes(30))
      val (value,mailbox)=sessionRef.get() match
        case SessionSlot.DbFirst(session) =>
          session.renewSqlWriter()
          (session.currentSqlWriterFreshness(),dbFirstMaintenanceMailbox)
        case SessionSlot.DeploymentReceipt(session) =>
          if session.isRestrictedRecall then (session.maintainRestrictedRecall(),recallMaintenanceMailbox)
          else (session.maintainAdmission(),maintenanceMailbox)
        case _ =>
          val session=requiredSession()
          val recovering=session.isRecallRecovering
          (if recovering then session.maintainRecallRecovery() else session.maintainAdmission(),
            if recovering then recallMaintenanceMailbox else maintenanceMailbox)
      require(mailbox.publish(value), "actual renewed admission publication was stale or mailbox has sticky failure")
      val now = System.nanoTime()
      IoMaintenanceDeadline(now, value.nextMaintenanceAfter(now))
    catch
      case cause: Throwable =>
        val deferRetainedRecoveryFailure=synchronized {
          val recovered=recoveredDbFirstServiceRef.get()
          val heldRecovery=retiredDbFirstRecallHoldRef.get()!=null && claimedBootCensusRef.get()!=null &&
            (recovered==null || !recovered.businessReleased.get())
          val publishedService=recovered!=null && recovered.businessReleased.get() && recovered.executionPublished.get() &&
            recovered.originalCommitted && recovered.workerCleanupComplete.get() && recovered.source==null &&
            recovered.predecessor==null && recovered.service!=null && dbFirstReadCallbacks!=null && currentMainOwner.nonEmpty &&
            (sessionRef.get() match
              case SessionSlot.DbFirst(actual)=>actual eq recovered.service
              case _=>false)
          // B的当前runtime在每个Ready END检查同一mailbox，沿原Main fail/drop入口接手。
          val deferFailure=heldRecovery || publishedService
          if deferFailure then retainedServiceFailureRef.compareAndSet(null,cause)
          maintenanceMailbox.fail(cause)
          recallMaintenanceMailbox.fail(cause)
          dbFirstMaintenanceMailbox.fail(cause)
          if recovered!=null then
            recovered.failure.compareAndSet(null,cause)
            recovered.stage.set(RecoveredServiceMainStage.Failed)
            recovered.mailbox.fail(cause)
          deferFailure
        }
        sessionRef.get() match
          case SessionSlot.Legacy(session) =>
            try session.markFailed(cause)
            catch case secondary: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, secondary)
          case SessionSlot.DeploymentReceipt(_) =>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(Some(cause), true))
          case SessionSlot.DbFirst(session) =>
            resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(session.resourceObservation))
          case null => ()
        if deferRetainedRecoveryFailure then
          IoMaintenanceDeadline.after(Duration.ofMinutes(30)) // 原Main观察sticky failure并drop后，worker才运行唯一退出hook。
        else throw cause

  private def validateOperation(operation: MySqlRepositoryOperation): Unit =
    require(operation != null, "immutable operation required")
    val requestedLimit = operation match
      case MySqlRepositoryOperation.ActivateIfAdmissible(_, _, _, _, limit) => Some(limit)
      case MySqlRepositoryOperation.ActivatePreparedIfAdmissible(_, _, _, _, limit, _) => Some(limit)
      case MySqlRepositoryOperation.CommitObservedUnbound(_, _, _, _, _, limit) => Some(limit)
      case MySqlRepositoryOperation.CommitUnboundCreate(_, _, _, _, _, _, limit) => Some(limit)
      case _ => None
    requestedLimit.foreach(limit => require(limit > 0 && limit <= activeLimit, "operation capacity exceeds explicit Boot active limit"))

  private def deploymentPolicy(control: ProductionBootstrapControl.DeploymentReceiptControl): MySqlRuntimeAuditPolicy =
    MySqlRuntimeAuditPolicy(control.policy().callerRoles(), control.policy().auditDefinerRoles(), MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2, control.policy().receiptFreshness())
  private def requireDeploymentControlled(control: ProductionBootstrapControl.DeploymentReceiptControl,
      controlled: MySqlFreshControlledMetadata): Unit =
    require(controlled.metadataPath.toAbsolutePath.normalize() == control.metadataPath() &&
      controlled.approvedConfigurationSha256.equalsIgnoreCase(control.configurationSha256()), "schema5 controlled metadata differs")
  private def beginDeploymentOwner(control: ProductionBootstrapControl.DeploymentReceiptControl): MySqlRuntimeInputs.AssemblyOwner =
    deploymentStage="static-root-and-owner"
    require(v3LedgerRef.get() == null && v3RootRef.get() == null && v3AssemblyRef.get() == null,
      "schema5 bootstrap custody is single-use")
    val profile = control.sourceLimitProfile()
    val ledger = new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    v3LedgerRef.set(ledger)
    val root = MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile, profile.maxSqlPacketBytes(), ledger)
    v3RootRef.set(root)
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    v3AssemblyRef.set(owner)
    deploymentStage="static-B-K-C"
    owner

  private def v3Policy(control: ProductionBootstrapControl.StaticV3): MySqlRuntimeAuditPolicy =
    MySqlRuntimeAuditPolicy(control.policy().callerRoles(),control.policy().auditDefinerRoles(),MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2,control.policy().receiptFreshness())
  private def beginV3Owner(control: ProductionBootstrapControl.StaticV3): MySqlRuntimeInputs.AssemblyOwner =
    require(v3LedgerRef.get()==null && v3RootRef.get()==null && v3AssemblyRef.get()==null,"V3 bootstrap custody is single-use")
    val profile=control.sourceLimitProfile()
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    v3LedgerRef.set(ledger)
    val root=MySqlFormat3ReadBudget.rootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
    v3RootRef.set(root)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    v3AssemblyRef.set(owner)
    owner

  private def exitResources(): Unit =
    if dbFirstStage != null then
      exitDbFirstResources()
      return
    var primary: Throwable=null
    try exitOriginalResources()
    catch
      case failure: Throwable =>
        primary=failure
        throw failure
    finally
      try settleV3StaticResources()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)

  /** schema6退出保留真实JDBC、SQL writer及barrier责任，不套用旧native terminal receipt。 */
  private def exitDbFirstResources(): Unit =
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()
    var primary: Throwable = null
    // 接管后的导航slot可能仍是旧parent；真实registry才决定新资源及残留metadata的责任。
    val recovered=recoveredDbFirstServiceRef.get()
    if recovered!=null && recovered.service!=null && recovered.service.hasAdapterCleanup(this) then
      primary=Option(recovered.failure.get()).orElse(Option(recovered.cleanupFailure.get())).orNull
      def retireMetadata(action: => Unit):Unit =
        try action
        catch case cause:Throwable=>
          if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
      // 退款已throw的holder结果不明，保管而不重试；未尝试的metadata仍沿原owner退休。
      if recovered.cleanupFailure.get()==null then
        if recovered.source!=null then
          retireMetadata(recovered.source.retireTransferredRecoveryForStop(this,recovered.service))
        if recovered.source==null && recovered.predecessor!=null && recovered.cleanupFailure.get()==null then
          retireMetadata(recovered.predecessor.retireTransferredParentForStop(this))
    val exitSlot=ownedRecoveredService.map(value=>SessionSlot.DbFirst(value._1)).getOrElse(sessionRef.get())
    exitSlot match
      case SessionSlot.DbFirst(session) =>
        try session.closeOnOwnerThread()
        catch case cause: Throwable =>
          if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
        val observed = session.resourceObservation
        resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstObserved(observed))
        if !observed.closed then
          if primary != null then throw primary
          throw new IllegalStateException("schema6 Session still retains actual resources", observed.failure.orNull)
        // Session已实际撤销原issuer图；只撤Adapter借用，不以清ref伪装close成功。
        retainedPlanIssuerRef.set(null)
        retiredDbFirstRecallHoldRef.set(null)
        retiredRecallReleaseAckRef.set(null);retiredRecallProducerDrainRef.set(null)
        dbFirstMainReadRef.set(null)
        dbFirstPendingMainReadRef.set(null)
        dbFirstReadCallbacks=null
        sessionRef.set(null)
      case null => ()
      case _ => throw new IllegalStateException("schema6 exit cannot retire another Session family")
    try
      val pending = dbFirstInputsRef.get()
      if pending != null then
        pending.close()
        dbFirstInputsRef.set(null)
      val owner = v3AssemblyRef.get()
      if owner != null then
        owner.close()
        v3AssemblyRef.set(null)
      val root = v3RootRef.get()
      if root != null then
        root.closeRootReadBudget()
        v3RootRef.set(null)
      v3LedgerRef.set(null)
      val managed=managedBootstrapRequestRef.get()
      if managed!=null then
        managed.closeOnOwnedIoThread()
        if managed.resourcesRetained then
          throw new IllegalStateException("managed initializer still retains actual owned resources after worker settlement",
            managed.failure.orNull)
        if !managedBootstrapRequestRef.compareAndSet(managed,null) then
          throw new IllegalStateException("managed initializer resource holder changed during worker settlement")
      dbFirstApprovedControl=null
      resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(dbFirstStage,
        Option(dbFirstFatal).orElse(Option(primary)), false))
    catch
      case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlOwnedIoAdapter.suppressExitCleanup(primary, cleanup)
        resourceObservation.set(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(dbFirstStage, Some(primary), true))
    if primary != null then throw primary

  private def settleV3StaticResources(): Unit =
    val root=v3RootRef.get()
    if root == null then
      if deploymentStage != null then
        resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(deploymentStage,
          Option(deploymentFatal).orElse(Option(ioRef.get()).flatMap(_.checkpoint.fatalCause)),v3AssemblyRef.get() != null || deploymentInputsRef.get() != null))
        if v3AssemblyRef.get() == null && deploymentInputsRef.get() == null then v3LedgerRef.set(null)
      return
    sessionRef.get() match
      case SessionSlot.DeploymentReceipt(session) if deploymentTerminalSettlementRef.get()!=null =>
        val settled=deploymentTerminalSettlementRef.get()
        settled.requireFor(session,root,identity)
        retainedPlanIssuerRef.synchronized {
          val issuer=retainedPlanIssuerRef.get()
          if issuer==null then throw new IllegalStateException("original schema5 terminal issuer disappeared")
          session.detachIssuerForTerminal(settled,issuer,v3LedgerRef.get())
          if !retainedPlanIssuerRef.compareAndSet(issuer,null) then
            throw new IllegalStateException("original schema5 issuer changed during terminal retirement")
        }
        session.retireInputsForTerminal(settled)
        val pending=v3PendingLeaseRef.get()
        if pending!=null then
          pending.close()
          v3PendingLeaseRef.set(null)
        val owner=v3AssemblyRef.get()
        if owner!=null then owner.close()
        root.closeRootReadBudget()
        resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(None,false))
        freshCompletionHandoff.set(null);recallRecoveryLink.set(null)
        sessionRef.set(null);deploymentTerminalSettlementRef.set(null)
        v3AssemblyRef.set(null);v3RootRef.set(null);v3LedgerRef.set(null)
        return
      case SessionSlot.DeploymentReceipt(_) =>
        val observed = deploymentFailedExitRef.get()
        if observed == null || !observed.canRetireStaticAndRoot then
          resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(
            Option(observed).map(_.failure).orElse(Option(deploymentFatal)).orElse(ioRef.get().checkpoint.fatalCause),true))
          return // retained state, never a wrapper whose cause points back into primary
        require(observed.identity eq identity, "schema5 failed-exit observation identity differs")
        sessionRef.set(null) // Session already detached its exact inputs; no native/read/barrier responsibility remains.
      case _ => ()
    val pendingInputs = deploymentInputsRef.get()
    if pendingInputs != null then
      pendingInputs.close()
      deploymentInputsRef.set(null)
    val owner=v3AssemblyRef.get()
    val session=legacySessionOrNull()
    // 未settled Session的borrow不能退；原Hard actor root/pending仍明确保留并HookFailed。
    if session != null then
      // 与checkpoint的取ref/采样互斥；锁中只有真实资格检查与引用撤销，不关闭资源。
      retainedPlanIssuerRef.synchronized {
        val originalIssuer = retainedPlanIssuerRef.get()
        if originalIssuer != null then
          freshCompletionHandoff.get() match
            case _: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded =>
              session.detachOrdinaryIssuerForExit(originalIssuer)
            case _ => session.detachUnusedFreshIssuerForExit(originalIssuer)
          require(retainedPlanIssuerRef.compareAndSet(originalIssuer, null), "original issuer reference changed during retirement")
      }
      session.retireV2GraphForExit()
      // 只在原Session真退出及静态图退休成功后撤销Adapter自己的引用入口。
      retainedPlanIssuerRef.set(null);freshCompletionHandoff.set(null);recallRecoveryLink.set(null)
    val pending=v3PendingLeaseRef.get()
    if pending != null then
      pending.close()
      v3PendingLeaseRef.set(null)
    if owner != null then owner.close()
    root.closeRootReadBudget()
    if session == null && deploymentFailedExitRef.get() == null then
      if deploymentStage != null then resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(
        deploymentStage,Option(deploymentFatal).orElse(Option(ioRef.get()).flatMap(_.checkpoint.fatalCause)).orElse(Option(owner).flatMap(_.failure)),false))
      else resourceObservation.set(MySqlOwnedIoResourceObservation.StaticV3AssemblyObserved(
        if owner == null then Option(ioRef.get()).flatMap(_.checkpoint.fatalCause) else owner.failure,false))
    sessionRef.set(null);v3AssemblyRef.set(null);v3RootRef.set(null);v3LedgerRef.set(null)

  private def exitOriginalResources(): Unit =
    maintenanceMailbox.stop()
    recallMaintenanceMailbox.stop()
    dbFirstMaintenanceMailbox.stop()
    Option(ioRef.get()).flatMap(_.checkpoint.fatalCause).foreach(maintenanceMailbox.fail)
    sessionRef.get() match
      case SessionSlot.DeploymentReceipt(session) =>
        session.beginQuiesce()
        val actualIo=ioRef.get()
        val workerFailure=actualIo.checkpoint.fatalCause
        if workerFailure.isEmpty && freshCompletionHandoff.get()!=null then
          var terminalCloseAttempted=false
          try
            actualIo.requireTerminalHookEmpty()
            completionRouter.requireTerminalEmpty()
            val issuer=retainedPlanIssuerRef.get()
            if issuer==null then throw new IllegalStateException("original schema5 terminal issuer missing")
            val intent=freshCompletionHandoff.get() match
              case handoff:MySqlOwnedIoAdapter.FreshCompletionHandoff =>
                session.prepareFreshTerminal(handoff,issuer,v3LedgerRef.get())
              case receipt:com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded =>
                val preparation=bootstrapPreparation.getOrElse(
                  throw new IllegalStateException("original schema5 Boot preparation missing"))
                session.prepareTerminal(receipt,Some(issuer),v3LedgerRef.get(),preparation.holder.tag,preparation.repositoryEpochAtSubmission)
            maintenanceMailbox.retireAfterTerminalProducer()
            recallMaintenanceMailbox.retireAfterTerminalProducer()
            terminalCloseAttempted=true
            session.closeForWorkerExit(None,intent)
            val settled=session.terminalSettlement.getOrElse(throw new IllegalStateException("schema5 native retirement returned without original settlement"))
            deploymentTerminalSettlementRef.set(settled)
            resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(None,true))
            return // actual IO hook still performs static/root retirement in its finally path.
          catch
            case cause:Throwable =>
              if !terminalCloseAttempted then
                try
                  val observed=session.failInitializationForWorkerExit(Some(cause))
                  deploymentFailedExitRef.set(observed)
                catch case cleanup:Throwable => MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
              resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(Some(cause),true))
              throw cause
        val observed = session.failInitializationForWorkerExit(ioRef.get().checkpoint.fatalCause)
        require(observed.identity eq identity, "schema5 failed-exit observation belongs to another IO generation")
        deploymentFailedExitRef.set(observed)
        resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptObserved(Some(observed.failure), !observed.canRetireStaticAndRoot))
        throw observed.failure // original failure, including legal cleanup suppressed causes; never normal terminal success.
      case _ => ()
    val session = legacySessionOrNull()
    if session != null then
      val actualIo = ioRef.get()
      // 若此不变量意外破坏则明确失败，不伪造None。正常construction不允许published command在此之前出现。
      require(actualIo != null, "actual owned IO reference was not published before database ownership")
      val primaryWorkerFailure = actualIo.checkpoint.fatalCause
      var preparationFailure: Throwable = null
      freshCompletionHandoff.get() match
        case handoff: MySqlOwnedIoAdapter.FreshCompletionHandoff if primaryWorkerFailure.isEmpty =>
          try session.prepareCertifiedFreshCompletion(handoff)
          catch
            case cause: Throwable =>
              preparationFailure=cause
              session.retainCompletionExitFailure(cause)
        case receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded =>
          try
            receipt.requireRegisteredIdentity(identity.bootId,identity.bootGeneration,identity.connectionGeneration)
            actualIo.requireTerminalHookEmpty()
            completionRouter.requireTerminalEmpty()
            retainedPlanIssuerRef.synchronized {
              val issuer=retainedPlanIssuerRef.get()
              if issuer == null then throw new IllegalStateException("original Ordinary issuer missing at actual hook")
              issuer.requireOrdinaryTerminalPrepared()
            }
            maintenanceMailbox.retireAfterTerminalProducer()
            recallMaintenanceMailbox.retireAfterTerminalProducer()
          catch
            case cause: Throwable =>
              preparationFailure=cause
              session.retainCompletionExitFailure(cause)
        case _ => ()
      try
        freshCompletionHandoff.get() match
          case receipt: com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded =>
            // 即使上面Unit预检失败，也继续原Session同一次writer/JDBC/barrier失败收口。
            val originalTag=bootstrapPreparation match
              case Some(value) => value.holder.tag
              case None => null
            session.closeOrdinaryForWorkerExit(primaryWorkerFailure,receipt,originalTag)
          case _ =>
            session.recordedFreshPartialCloseForExit(primaryWorkerFailure) match
              case Some(recorded) => if recorded.failure.nonEmpty then throw recorded.failure.get
              case None => session.closeForWorkerExit(primaryWorkerFailure)
        if preparationFailure != null then throw preparationFailure
      finally session.closeObservation.foreach(report =>
        resourceObservation.set(MySqlOwnedIoResourceObservation.SessionObserved(report)))
    else
      if deploymentStage != null then
        val cause=Option(deploymentFatal).orElse(Option(ioRef.get()).flatMap(_.checkpoint.fatalCause))
        resourceObservation.set(MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(deploymentStage,cause,true))
        cause.foreach(throw _)
      Option(resourceObservation.get()) match
        case Some(MySqlOwnedIoResourceObservation.PartialInitializationObserved(report)) =>
          report.failure.foreach(cause => throw cause) // 已由partialinit同worker收口，禁止第二次close或擦掉失败。
        case _ => resourceObservation.set(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned)

object MySqlOwnedIoAdapter:
  /** 资格核验拒绝保原failure；Fatal从资格边界原样传播。 */
  private[mysql] def qualifyRetainedRecallFailure[A](failure:Throwable)(qualify: => A):A =
    if !scala.util.control.NonFatal(failure) then throw failure
    try qualify
    catch case scala.util.control.NonFatal(diagnostic)=>
      suppressExitCleanup(failure,diagnostic)
      throw failure


  // 仅为逻辑责任预算，不是堆测量：原 command/work/poll/result 为 3072；
  // 槽 1 Preparation 与 charge/stage 日志；槽 2 Main SourceSubmissionPending；
  // 槽 3 双向注册、退役及原 observation 引用；槽 4 有限失败、原结果、epoch 和 Main 记账事实。
  private[mysql] val RestrictedSourceR0ProtocolChargeBytes:Long =
    Math.addExact(3072L,Math.multiplyExact(4L,256L))

  /** 有限的 Main/Adapter 交接阶段；只描述责任，不代表 SQL 成功。 */
  private[floatingpod] enum RestrictedSourceR0PreEnqueueStage:
    case ChargeAttempt
    case PreparationCreation
    case TagCreation
    case CommandCreation
    case PendingCreation
    case WorkInstallation
    case AdapterPendingRegistration
    case AdapterPendingInstalled
    case MainPendingRegistration
    case BootReadRetirement
    case BootReadRetired
    case EnqueueReadinessCheck
    case SubmitCall
    case AdapterEpochAccounting
    case PendingOutcomeRecording
    case MainOutcomeRecording
    case Complete

  /** 由 Adapter 按原身份签发的封闭 owner，用于关联原 R0 Work 与唯一 Main pending。 */
  private[floatingpod] sealed class RestrictedSourceR0PreEnqueue private[MySqlOwnedIoAdapter] (
      private[floatingpod] val adapter:MySqlOwnedIoAdapter,
      private[floatingpod] val lease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      private[floatingpod] val exactServer:net.minecraft.server.MinecraftServer,
      private[floatingpod] val exactLink:RecallRecoveryLink,
      private val exactIntent:RestrictedSourceReserveIntent,
      private val exactBacking:RestrictedSourceReservePlanBacking,
      private val exactPlanAck:RestrictedSourcePlanAck,
      private val exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      private[floatingpod] val protocolRoot:MySqlFormat3ReadBudget,
      val expectedSourceEpoch:Long,
      val nextAcceptedEpoch:Long):
    private[floatingpod] val expectedObservation=exactIntent.expectedSourceObservation
    private[floatingpod] val exactParentReference=exactIntent.parentReference
    private var retainedTag:IoOperationTag=null
    private var retainedCommand:MySqlOwnedIoCommand=null
    private var retainedR0Pending:AnyRef=null
    private var retainedWork:AnyRef=null
    private var retainedMainPending:AnyRef=null
    @volatile private[floatingpod] var phase:RestrictedSourceR0PreEnqueueStage=
      RestrictedSourceR0PreEnqueueStage.PreparationCreation
    @volatile private[floatingpod] var failureStage:RestrictedSourceR0PreEnqueueStage=null
    @volatile private[floatingpod] var secondaryCause:Throwable=null
    @volatile private[floatingpod] var rawSubmission:IoSubmission=null
    @volatile private[floatingpod] var submissionReturned=false
    @volatile private[floatingpod] var submitCallEntered=false
    @volatile private[floatingpod] var adapterEpochFactRecorded=false
    @volatile private[floatingpod] var pendingOutcomeFactRecorded=false
    @volatile private[floatingpod] var adapterEpochAtOutcomeObservation=Long.MinValue
    @volatile private var mainOutcomeRecordAttempted=false
    @volatile private var mainOutcomeRecorded=false
    @volatile private[floatingpod] var mainPendingBound=false
    @volatile private[floatingpod] var preEnqueueReady=false
    @volatile private var retirementAttempted=false
    @volatile private var retirementReturned=false
    @volatile private var previouslyRetiredAckVerified=false
    @volatile private var bootReadWasRetiredBefore=false
    @volatile private[floatingpod] var preEnqueueFailureCause:Throwable=null
    @volatile private[floatingpod] var handoffBookkeepingFailure:Throwable=null
    @volatile private[floatingpod] var submissionUnknownCause:Throwable=null

    private[MySqlOwnedIoAdapter] def markRegistered():Unit =
      require(adapter!=null && lease!=null && exactServer!=null && exactLink!=null && exactIntent!=null &&
        exactBacking!=null && exactPlanAck!=null && exactPlan!=null && protocolRoot!=null &&
        (exactIntent.exactLink eq exactLink) && exactIntent.expectedSourceObservation.belongsToLease(lease) &&
        (exactPlanAck.adapter eq adapter) && (exactPlanAck.backing eq exactBacking) &&
        (exactPlanAck.intent eq exactIntent) && (exactPlan eq exactBacking.exactPlan) &&
        (exactPlan.exactRoot eq protocolRoot) && expectedSourceEpoch>=0L && nextAcceptedEpoch>expectedSourceEpoch,
        "restricted SOURCE R0 preparation must retain the exact original plan/ACK/lease/root/epoch")

    private[MySqlOwnedIoAdapter] def requireTagBindAvailable():Unit =
      require(retainedTag==null && phase==RestrictedSourceR0PreEnqueueStage.PreparationCreation,
        "restricted SOURCE R0 original operation tag slot is available once")

    private[MySqlOwnedIoAdapter] def bindTag(value:IoOperationTag):Unit =
      retainedTag=value
      phase=RestrictedSourceR0PreEnqueueStage.TagCreation

    private[MySqlOwnedIoAdapter] def requireCommandBindAvailable():Unit =
      require(retainedCommand==null && retainedTag!=null &&
        phase==RestrictedSourceR0PreEnqueueStage.TagCreation,
        "restricted SOURCE R0 original command slot is available after its tag")

    private[MySqlOwnedIoAdapter] def bindCommand(value:MySqlOwnedIoCommand):Unit =
      retainedCommand=value
      phase=RestrictedSourceR0PreEnqueueStage.CommandCreation

    private[MySqlOwnedIoAdapter] def requireR0PendingBindAvailable():Unit =
      require(retainedR0Pending==null && retainedCommand!=null &&
        phase==RestrictedSourceR0PreEnqueueStage.CommandCreation,
        "restricted SOURCE R0 original pending slot is available after its command")

    private[MySqlOwnedIoAdapter] def bindR0Pending(value:AnyRef):Unit =
      retainedR0Pending=value
      phase=RestrictedSourceR0PreEnqueueStage.PendingCreation

    private[MySqlOwnedIoAdapter] def requireWorkBindAvailable():Unit =
      require(retainedWork==null && retainedR0Pending!=null &&
        phase==RestrictedSourceR0PreEnqueueStage.PendingCreation,
        "restricted SOURCE R0 exact Work slot is available before pending installation")

    private[MySqlOwnedIoAdapter] def bindWork(value:AnyRef):Unit =
      retainedWork=value
      phase=RestrictedSourceR0PreEnqueueStage.WorkInstallation

    private[MySqlOwnedIoAdapter] def markWorkInstalled():Unit =
      require(retainedWork!=null && retainedR0Pending!=null && phase==RestrictedSourceR0PreEnqueueStage.WorkInstallation,
        "restricted SOURCE R0 Work must be installed in its original pending")

    private[MySqlOwnedIoAdapter] def markAdapterPendingInstallAttempted():Unit =
      require(retainedR0Pending!=null && retainedWork!=null &&
        phase==RestrictedSourceR0PreEnqueueStage.WorkInstallation,
        "restricted SOURCE R0 pending installation requires its exact Work")
      phase=RestrictedSourceR0PreEnqueueStage.AdapterPendingRegistration

    private[MySqlOwnedIoAdapter] def markAdapterPendingInstalled():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.AdapterPendingRegistration && retainedR0Pending!=null,
        "restricted SOURCE R0 Adapter pending must be installed once")
      phase=RestrictedSourceR0PreEnqueueStage.AdapterPendingInstalled

    private[floatingpod] def beginMainPendingRegistration():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.AdapterPendingInstalled && retainedR0Pending!=null,
        "Main pending registration requires the exact installed Adapter pending")
      phase=RestrictedSourceR0PreEnqueueStage.MainPendingRegistration

    private[floatingpod] def requireMainPendingBindAvailable():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.MainPendingRegistration && retainedMainPending==null,
        "exact Main pending slot is available before its holder is created")

    private[floatingpod] def bindMainPending(value:AnyRef):Unit =
      retainedMainPending=value

    private[floatingpod] def confirmMainPendingInstalled(value:AnyRef):Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.MainPendingRegistration && value!=null &&
        (retainedMainPending eq value),"Main lease pending must match the Adapter preparation by identity")
      mainPendingBound=true
      phase=RestrictedSourceR0PreEnqueueStage.BootReadRetirement

    private[floatingpod] def requireBidirectionalPending(exactAdapterPending:AnyRef,
        exactMainPending:com.wi.floatingpod.server.MainBootProjection.SourceSubmissionPending):Unit =
      require(exactAdapterPending!=null && (retainedR0Pending eq exactAdapterPending) &&
        exactMainPending!=null && (retainedMainPending eq exactMainPending) && mainPendingBound &&
        phase==RestrictedSourceR0PreEnqueueStage.BootReadRetired &&
        retainedWork!=null && retainedTag!=null && retainedCommand!=null &&
        expectedObservation.belongsToLease(lease) && (exactIntent.parentReference eq exactParentReference),
        "exact original Adapter/Work/Main pending and parent/observation registrations must match")

    private[floatingpod] def recordBootReadAlreadyRetired():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.BootReadRetirement && mainPendingBound,
        "prior Boot retirement is checked only after both original pending owners are installed")
      bootReadWasRetiredBefore=true
      previouslyRetiredAckVerified=true
      phase=RestrictedSourceR0PreEnqueueStage.BootReadRetired

    private[floatingpod] def markRetirementAttempted():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.BootReadRetirement && mainPendingBound &&
        !bootReadWasRetiredBefore,"Boot retirement must follow bidirectional pending registration once")
      retirementAttempted=true
      phase=RestrictedSourceR0PreEnqueueStage.BootReadRetirement

    private[floatingpod] def markRetirementReturned():Unit =
      require(retirementAttempted && mainPendingBound && phase==RestrictedSourceR0PreEnqueueStage.BootReadRetirement,
        "Boot retirement is recorded only after the original drop call returned")
      retirementReturned=true
      phase=RestrictedSourceR0PreEnqueueStage.BootReadRetired

    private[floatingpod] def markEnqueueReadinessCheck():Unit =
      require(mainPendingBound && (retirementReturned || previouslyRetiredAckVerified),
        "pre-enqueue readiness requires exact pending and completed/ACKed Boot retirement")
      phase=RestrictedSourceR0PreEnqueueStage.EnqueueReadinessCheck

    private[floatingpod] def markPreEnqueueReady():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.EnqueueReadinessCheck && mainPendingBound &&
        (retirementReturned || previouslyRetiredAckVerified),
        "pre-enqueue readiness is irrevocable only after both owners and Boot retirement")
      preEnqueueReady=true
      phase=RestrictedSourceR0PreEnqueueStage.SubmitCall

    private[MySqlOwnedIoAdapter] def markSubmitCallEntered():Unit =
      require(phase==RestrictedSourceR0PreEnqueueStage.SubmitCall && mainPendingBound &&
        preEnqueueReady && (retirementReturned || previouslyRetiredAckVerified) && !submitCallEntered,
        "the exact original R0 submit call is entered once after readiness")
      submitCallEntered=true
      phase=RestrictedSourceR0PreEnqueueStage.SubmitCall

    private[MySqlOwnedIoAdapter] def retainRawSubmission(value:IoSubmission):Unit =
      require(!submissionReturned,"actual restricted SOURCE R0 submit return is retained once")
      submissionReturned=true
      rawSubmission=value
      phase=RestrictedSourceR0PreEnqueueStage.AdapterEpochAccounting

    private[MySqlOwnedIoAdapter] def markAdapterEpochRecorded(actualEpoch:Long):Unit =
      require(submissionReturned && rawSubmission!=null && !adapterEpochFactRecorded,
        "actual restricted SOURCE R0 outcome is retained before exact Adapter epoch accounting")
      adapterEpochAtOutcomeObservation=actualEpoch
      adapterEpochFactRecorded=true
      phase=RestrictedSourceR0PreEnqueueStage.PendingOutcomeRecording

    private[MySqlOwnedIoAdapter] def markUnknownAdapterEpochObserved(actualEpoch:Long):Unit =
      require(submitCallEntered && !submissionReturned && submissionUnknownCause!=null &&
        !adapterEpochFactRecorded,
        "submit UNKNOWN retains the actual Adapter epoch without predicting a result")
      adapterEpochAtOutcomeObservation=actualEpoch
      adapterEpochFactRecorded=true
      phase=RestrictedSourceR0PreEnqueueStage.PendingOutcomeRecording

    private[MySqlOwnedIoAdapter] def markPendingOutcomeRecorded():Unit =
      require(adapterEpochFactRecorded && submissionReturned && rawSubmission!=null && !pendingOutcomeFactRecorded,
        "exact restricted SOURCE R0 pending records the original queue result once")
      pendingOutcomeFactRecorded=true
      phase=RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording

    private[MySqlOwnedIoAdapter] def markUnknownPendingOutcomeRecorded():Unit =
      require(adapterEpochFactRecorded && submissionUnknownCause!=null && !submissionReturned &&
        !pendingOutcomeFactRecorded,
        "exact restricted SOURCE R0 pending records submit UNKNOWN once without inventing a result")
      pendingOutcomeFactRecorded=true
      phase=RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording

    private[MySqlOwnedIoAdapter] def markMainOutcomeRecordAttempted():Unit =
      require(pendingOutcomeFactRecorded && !mainOutcomeRecordAttempted,
        "Main receives a result only after Adapter epoch and pending facts are retained")
      mainOutcomeRecordAttempted=true

    private[MySqlOwnedIoAdapter] def markMainOutcomeRecorded():Unit =
      require(mainOutcomeRecordAttempted && !mainOutcomeRecorded,
        "original Main outcome recording is retained once")
      mainOutcomeRecorded=true
      phase=RestrictedSourceR0PreEnqueueStage.Complete

    private[MySqlOwnedIoAdapter] def retainPreEnqueueFailure(
        stage:RestrictedSourceR0PreEnqueueStage,cause:Throwable):Unit =
      require(!submitCallEntered && stage!=null && cause!=null && preEnqueueFailureCause==null,
        "pre-enqueue failure retains one exact stage and original cause")
      failureStage=stage
      preEnqueueFailureCause=cause
      phase=RestrictedSourceR0PreEnqueueStage.Complete

    private[MySqlOwnedIoAdapter] def retainSubmissionUnknown(cause:Throwable):Unit =
      require(submitCallEntered && !submissionReturned && cause!=null && submissionUnknownCause==null,
        "only a submit-entered call without an actual return is UNKNOWN")
      failureStage=RestrictedSourceR0PreEnqueueStage.SubmitCall
      submissionUnknownCause=cause
      phase=RestrictedSourceR0PreEnqueueStage.Complete

    private[MySqlOwnedIoAdapter] def retainHandoffBookkeepingFailure(
        stage:RestrictedSourceR0PreEnqueueStage,cause:Throwable):Unit =
      require(submissionReturned && stage!=null && cause!=null && handoffBookkeepingFailure==null,
        "known returned R0 result retains a separate bookkeeping failure")
      failureStage=stage
      handoffBookkeepingFailure=cause
      phase=RestrictedSourceR0PreEnqueueStage.Complete

    private[MySqlOwnedIoAdapter] def retainSecondaryCause(cause:Throwable,
        stage:RestrictedSourceR0PreEnqueueStage=null):Unit =
      require(cause!=null,"secondary restricted SOURCE R0 cause is required")
      if secondaryCause==null then secondaryCause=cause
      if stage!=null && (handoffBookkeepingFailure!=null || submissionUnknownCause!=null) then failureStage=stage

    private[floatingpod] def exactTag:IoOperationTag =
      require(retainedTag!=null,"exact original restricted SOURCE R0 tag is not yet retained")
      retainedTag

    private[floatingpod] def exactCommand:MySqlOwnedIoCommand =
      require(retainedCommand!=null,"exact original restricted SOURCE R0 command is not yet retained")
      retainedCommand

    private[floatingpod] def failureStageOr(fallback:RestrictedSourceR0PreEnqueueStage):RestrictedSourceR0PreEnqueueStage =
      if failureStage==null then fallback else failureStage

    private[floatingpod] def mainPending:AnyRef=retainedMainPending

    private[floatingpod] def bootReadRetiredByThisHandoff:Boolean=retirementReturned

    private[floatingpod] def previousBootReadAckVerified:Boolean=previouslyRetiredAckVerified

    private[floatingpod] def ownsR0Pending(value:AnyRef):Boolean =
      value!=null && (retainedR0Pending eq value)

    private[floatingpod] def ownsMainPending(value:AnyRef):Boolean =
      value!=null && (retainedMainPending eq value)

    private[floatingpod] def requireOriginal(actualAdapter:MySqlOwnedIoAdapter,
        actualLease:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
        actualLink:RecallRecoveryLink,
        actualObservation:com.wi.floatingpod.server.MainBootProjection.SourceContinuationObservation):Unit =
      require((actualAdapter eq adapter) && (actualLease eq lease) && (actualLink eq exactLink) &&
        (expectedObservation eq actualObservation) && (exactIntent.exactLink eq exactLink) &&
        (exactIntent.expectedSourceObservation eq expectedObservation) && (exactPlanAck.adapter eq adapter) &&
        (exactPlanAck.backing eq exactBacking) && (exactPlanAck.intent eq exactIntent) &&
        (exactPlan eq exactBacking.exactPlan) && (exactPlan.exactRoot eq protocolRoot) &&
        exactIntent.parentReference!=null && exactIntent.expectedSourceObservation.settledRepositoryEpoch==expectedSourceEpoch,
        "restricted SOURCE R0 role must retain its exact original Adapter/lease/link/intent/parent/observation/plan")

    private[floatingpod] def requireWorkerEntry(exactPending:AnyRef,exactWork:AnyRef):Unit =
      require(exactPending!=null && (retainedR0Pending eq exactPending) && exactWork!=null &&
        (retainedWork eq exactWork) && mainPendingBound && preEnqueueReady && submitCallEntered &&
        mayRetainCompletion &&
        (phase==RestrictedSourceR0PreEnqueueStage.SubmitCall || phase==RestrictedSourceR0PreEnqueueStage.AdapterEpochAccounting ||
          phase==RestrictedSourceR0PreEnqueueStage.PendingOutcomeRecording ||
          phase==RestrictedSourceR0PreEnqueueStage.MainOutcomeRecording || phase==RestrictedSourceR0PreEnqueueStage.Complete),
        "restricted SOURCE R0 worker requires the exact published Work after pre-enqueue readiness and submit entry")

    private[floatingpod] def mayRetainCompletion:Boolean =
      submitCallEntered && (!submissionReturned || rawSubmission==null || rawSubmission==IoSubmission.Accepted)

    private[MySqlOwnedIoAdapter] def needsCompletionSlot(hasNoRetainedCompletion:Boolean):Boolean =
      hasNoRetainedCompletion && mayRetainCompletion

  // 13个固定协议槽各按256字节计入原Custody账户；这是责任预算单位，不是JVM堆测量。
  private[mysql] val RestrictedSourcePlanDropProtocolChargeBytes:Long = 13L*256L
  /** 只附原始且不回指 primary 的 cleanup；retained 状态由 observation 表达。 */
  private[floatingpod] def suppressExitCleanup(primary: Throwable, cleanup: Throwable): Unit =
    def reaches(from: Throwable, target: Throwable): Boolean =
      val seen=new java.util.IdentityHashMap[Throwable,java.lang.Boolean]()
      def visit(value: Throwable): Boolean =
        if value == null || seen.containsKey(value) then false
        else if value eq target then true
        else
          seen.put(value,java.lang.Boolean.TRUE)
          visit(value.getCause) || value.getSuppressed.exists(visit)
      visit(from)
    if primary != null && cleanup != null && !reaches(cleanup,primary) && !reaches(primary,cleanup) then
      primary.addSuppressed(cleanup)
  /** Poll结果只能回到原adapter/slot/command；构造器不向Main开放。 */
  private[floatingpod] final class RestrictedSourcePlanAck private[MySqlOwnedIoAdapter] (
      private[MySqlOwnedIoAdapter] val adapter:MySqlOwnedIoAdapter,
      private[MySqlOwnedIoAdapter] val tag:IoOperationTag,
      private[MySqlOwnedIoAdapter] val command:MySqlOwnedIoCommand,
      private[MySqlOwnedIoAdapter] val intent:RestrictedSourceReserveIntent,
      private[MySqlOwnedIoAdapter] val completion:IoCompletion[MySqlOwnedIoReply],
      private[MySqlOwnedIoAdapter] val backing:RestrictedSourceReservePlanBacking)
  private[floatingpod] final class RestrictedSourcePlanRejectionAck private[MySqlOwnedIoAdapter] (
      private[MySqlOwnedIoAdapter] val adapter:MySqlOwnedIoAdapter,
      private[MySqlOwnedIoAdapter] val tag:IoOperationTag,
      private[MySqlOwnedIoAdapter] val command:MySqlOwnedIoCommand,
      private[MySqlOwnedIoAdapter] val intent:RestrictedSourceReserveIntent,
      private[MySqlOwnedIoAdapter] val submission:IoSubmission)
  private[floatingpod] final class RestrictedSourcePlanFailureAck private[MySqlOwnedIoAdapter] (
      private[MySqlOwnedIoAdapter] val adapter:MySqlOwnedIoAdapter,
      private[MySqlOwnedIoAdapter] val tag:IoOperationTag,
      private[MySqlOwnedIoAdapter] val command:MySqlOwnedIoCommand,
      private[MySqlOwnedIoAdapter] val intent:RestrictedSourceReserveIntent,
      private[MySqlOwnedIoAdapter] val completion:IoCompletion[MySqlOwnedIoReply],
      private[MySqlOwnedIoAdapter] val cause:Throwable)
  /** Exact registration/read tag and revoked Main proof authorizing only one worker plan drop. */
  private[floatingpod] final class RestrictedSourcePlanDropRegistration private[MySqlOwnedIoAdapter] (
      private[MySqlOwnedIoAdapter] val adapter:MySqlOwnedIoAdapter,
      private[MySqlOwnedIoAdapter] val registrationIdentity:AnyRef,
      private[mysql] val dropTag:IoOperationTag,
      private[mysql] val readTag:IoOperationTag,
      private[MySqlOwnedIoAdapter] var continuationOwner:com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      private[MySqlOwnedIoAdapter] var intentOwner:RestrictedSourceReserveIntent,
      private[MySqlOwnedIoAdapter] var planAckOwner:RestrictedSourcePlanAck,
      private[MySqlOwnedIoAdapter] var backingOwner:RestrictedSourceReservePlanBacking,
      private[MySqlOwnedIoAdapter] var overlayOwner:com.wi.floatingpod.server.MainBootProjection.SourceChunkEffectClosure,
      private[MySqlOwnedIoAdapter] var planOwner:MySqlFencedTransaction.SourceReservePlan5,
      private[MySqlOwnedIoAdapter] val viewsRevokedOwner:com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked
  ):
    private var issuerClaimed=false
    private var issuedReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt=null
    require(adapter!=null && registrationIdentity!=null && dropTag!=null && readTag!=null &&
      continuationOwner!=null && intentOwner!=null && planAckOwner!=null && backingOwner!=null &&
      overlayOwner!=null && planOwner!=null && viewsRevokedOwner!=null,
      "exact original restricted source plan drop registration required")
    private[mysql] def intent:RestrictedSourceReserveIntent = synchronized {
      Option(intentOwner).getOrElse(throw new IllegalStateException("source-plan drop registration payload was transferred to its receipt"))
    }
    private[mysql] def plan:MySqlFencedTransaction.SourceReservePlan5 = synchronized {
      Option(planOwner).getOrElse(throw new IllegalStateException("source-plan drop registration plan was transferred to its receipt"))
    }
    private[mysql] def hasIssuedReceipt:Boolean=synchronized { issuedReceipt!=null }
    private[mysql] def receipt:MySqlFencedTransaction.SourceReservePlanDropReceipt=synchronized { issuedReceipt }
    private[mysql] def requireWorkerDropEligible(actualAdapter:MySqlOwnedIoAdapter,
        actualPlan:MySqlFencedTransaction.SourceReservePlan5,
        exactViewsRevoked:com.wi.floatingpod.server.MainBootProjection.SourcePlanViewsRevoked):Unit = synchronized {
      require((actualAdapter eq adapter) && actualPlan!=null && (exactViewsRevoked eq viewsRevokedOwner) &&
        (registrationIdentity ne null) && (adapter ne null),
        "exact original adapter/registration/plan/views proof required for source-plan drop")
      adapter.requireCurrentRestrictedSourceDropRegistration(this)
      if issuedReceipt==null then
        require(!issuerClaimed && continuationOwner!=null && intentOwner!=null && planAckOwner!=null &&
          backingOwner!=null && overlayOwner!=null && (planOwner eq actualPlan) &&
          (intentOwner eq actualPlan.intent) && planAckOwner.tag==readTag &&
          (planAckOwner.intent eq intentOwner) && (planAckOwner.backing eq backingOwner) &&
          (backingOwner.exactPlan eq actualPlan),
          "exact current restricted source plan registration is required before receipt issuance")
        exactViewsRevoked.requireFor(continuationOwner,overlayOwner,planAckOwner,actualPlan)
        issuerClaimed=true
      else
        require(issuerClaimed && (issuedReceipt eq actualPlan.receiptForReplay),
          "worker drop must use the exact one-shot issued receipt")
        issuedReceipt.requireFor(actualPlan)
    }
    /** Fenced calls after retaining the receipt and before releasing its strong plan anchors. */
    private[mysql] def recordReceiptIssued(
        exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt):Unit = synchronized {
      require(issuerClaimed && issuedReceipt==null && exactReceipt!=null && planOwner!=null,
        "exact retained Fenced receipt required before Adapter registration transfer")
      exactReceipt.requireFor(planOwner)
      require(planOwner.receiptForReplay eq exactReceipt,
        "Fenced source-plan drop receipt must already be retained by its exact plan")
      issuedReceipt=exactReceipt
      continuationOwner=null
      intentOwner=null
      planAckOwner=null
      backingOwner=null
      overlayOwner=null
      planOwner=null
    }
    override def toString:String="RestrictedSourcePlanDropRegistration[exact adapter/read/drop lineage]"
  /** 同一结果的所有别名共享此锁；不持锁调用外部 owner，也不在应用退役时退款。 */
  private[floatingpod] final class OrdinaryBootResultBacking private[mysql] (
      admissionInput: MySqlOwnedDatabaseAdmission, snapshotInput: MySqlOwnedFormat3Snapshot,
      factsInput: MySqlOrdinaryFacts, profileInput: SourceLimitProfile):
    private var admission = admissionInput
    private var snapshot = snapshotInput
    private var facts = factsInput
    private var profile = profileInput
    private var failure: Throwable = null
    private var mode: ProductionBootstrapControl.Mode = null
    private var finalization: Duration = null
    private var state = 0 // 0构造、1已发布、2退休中、3已退休、4失败。
    private def requireLive(): Unit =
      if failure != null then throw failure
      require(state == 1, "Ordinary application payload is not live")
    private[mysql] def publish(): Unit = synchronized {
      require(state == 0, "Ordinary result publication once")
      state = 1
    }
    def databaseAdmission: MySqlOwnedDatabaseAdmission = synchronized { requireLive(); admission }
    def fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot = synchronized { requireLive(); snapshot }
    def ordinaryFacts: MySqlOrdinaryFacts = synchronized { requireLive(); facts }
    def actualSourceLimitProfile: SourceLimitProfile = synchronized { requireLive(); profile }
    private[MySqlOwnedIoAdapter] def beginRetirement(actualMode: ProductionBootstrapControl.Mode, limit: Duration): Unit = synchronized {
      requireLive()
      require(actualMode == ProductionBootstrapControl.Mode.ORDINARY_CERTIFIED && limit != null &&
        !limit.isNegative && !limit.isZero, "actual Ordinary mode/finalization required")
      mode = actualMode; finalization = limit; state = 2
    }
    private[MySqlOwnedIoAdapter] def finishRetirement(): Unit = synchronized {
      require(state == 2 && failure == null, "original retiring result required")
      admission = null; snapshot = null; facts = null; profile = null
      state = 3
    }
    private[mysql] def retainFailure(cause: Throwable): Unit = synchronized {
      require(cause != null, "original application failure required")
      if failure == null then failure = cause
      state = 4
    }
    private[floatingpod] def requireRetired(): Unit = synchronized {
      if failure != null then throw failure
      require(state == 3 && admission == null && snapshot == null && facts == null && profile == null,
        "Ordinary application payload remains retained")
    }
    private[floatingpod] def retainedMode: ProductionBootstrapControl.Mode = synchronized {
      require(mode != null && state >= 2, "original mode not retained")
      mode
    }
    private[floatingpod] def retainedFinalizationLimit: Duration = synchronized {
      require(finalization != null && state >= 2, "original finalization not retained")
      finalization
    }
    override def toString: String = "OrdinaryBootResultBacking[opaque; not worker/native closure]"
  /** 只由实际adapter构造并保留；不含actor、connection或native handle，不是SQL/physical许可。 */
  final class RecallRecoveryLink private[MySqlOwnedIoAdapter] (
      private[MySqlOwnedIoAdapter] val adapter: MySqlOwnedIoAdapter,
      val identity: MySqlOwnedIoIdentity, val bindingId: UUID, val writerEpoch: Long,
      private[floatingpod] val references: Vector[MySqlNormalRecallRecoveryReference],
      private[floatingpod] val sourceAllocation:Option[com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root]=None):
    override def toString: String = "RecallRecoveryLink[original Boot routing; not business or physical authority]"
  /** Main-issued locator DATA and exact original lineage; this value carries no SQL or R0 authority. */
  final class RestrictedSourceReserveIntent private[MySqlOwnedIoAdapter] (
      private val adapter: MySqlOwnedIoAdapter,
      private val continuation: com.wi.floatingpod.server.MainBootProjection.SourceContinuationLease,
      private val server: net.minecraft.server.MinecraftServer,
      private val sourceRoot: com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root,
      private val sqlRoot: MySqlFormat3ReadBudget,
      private val sqlLedger: MySqlFormat3AllocationLedger,
      private val carrierLocatorOwner: com.wi.floatingpod.server.MainBootProjection.SourceCarrierLocator,
      private val retainedProtection: MySqlBootSourceProtectionIndex.RetainedData,
      val exactLink: RecallRecoveryLink,
      val parentReference: MySqlNormalRecallRecoveryReference,
      val sourceCarrierLocator: MySqlBootSourceProtectionIndex.Carrier,
      val exactDomainObservation: SourceDomainCapture.CatalogDomainObservation,
      val loadedRootPlan: SourceDomainCapture.RootPlan,
      val childWorkflow: UUID,
      val reserveOperation: UUID,
      val plannedWrite: UUID,
      val writeGeneration: Long,
      val expectedSourceObservation: com.wi.floatingpod.server.MainBootProjection.SourceContinuationObservation):
    private var sourcePlanDropReceiptIssued=false

    private[mysql] def requireSourcePlanDropNotIssued():Unit = synchronized {
      if sourcePlanDropReceiptIssued then
        throw new IllegalStateException("restricted source reserve intent already issued its one-shot drop receipt")
    }

    private[mysql] def recordSourcePlanDropReceipt(
        exactPlan:MySqlFencedTransaction.SourceReservePlan5,
        exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt):Unit = synchronized {
      if sourcePlanDropReceiptIssued then
        throw new IllegalStateException("restricted source reserve intent drop receipt was already issued")
      if exactPlan==null || (exactPlan.intent ne this) || exactPlan.issuer==null || exactReceipt==null then
        throw new IllegalArgumentException("exact Fenced plan and its Main-authorized drop receipt required")
      exactReceipt.requireFor(exactPlan)
      sourcePlanDropReceiptIssued=true
    }

    private def nonZero(value: UUID): Boolean =
      value != null && (value.getMostSignificantBits | value.getLeastSignificantBits) != 0L

    /** Fenced只读worker核验Main签发对象、原SQL root和保护holder的成员身份。 */
    private[mysql] def requireOriginalReservePlanBinding(
        actualLink: RecallRecoveryLink,
        actualParentReference: MySqlNormalRecallRecoveryReference,
        actualRoot: MySqlFormat3ReadBudget,
        actualDomain: SourceDomainCapture.CatalogDomainObservation,
        actualObservation: com.wi.floatingpod.server.MainBootProjection.SourceContinuationObservation): Unit =
      var hasExactRoot = false
      if exactDomainObservation != null && loadedRootPlan != null then
        val iterator = exactDomainObservation.roots().iterator()
        while iterator.hasNext && !hasExactRoot do hasExactRoot = iterator.next() eq loadedRootPlan
      if adapter == null || continuation == null || server == null || sourceRoot == null || sqlRoot == null ||
          sqlLedger == null || carrierLocatorOwner == null || retainedProtection == null ||
          (actualLink ne exactLink) || (actualParentReference ne parentReference) ||
          (actualRoot ne sqlRoot) || (actualDomain ne exactDomainObservation) ||
          (actualObservation ne expectedSourceObservation) || !nonZero(childWorkflow) ||
          !nonZero(reserveOperation) || !nonZero(plannedWrite) || writeGeneration < 0L ||
          !exactLink.references.exists(_ eq parentReference) || !retainedProtection.containsExactCarrier(sourceCarrierLocator) ||
          sourceCarrierLocator.binding != exactLink.bindingId ||
          sourceCarrierLocator.chunk.dimension != loadedRootPlan.dimension() || !loadedRootPlan.loaded() || !hasExactRoot then
        throw new IllegalStateException("exact Main-issued restricted source locator/parent/root plan required")
      if !adapter.retainedRestrictedSourceIntentIsCurrent(this, continuation, server, sourceRoot, sqlRoot, sqlLedger) then
        throw new IllegalStateException("restricted source reserve intent lost its original adapter/lease/root/ledger binding")

    /** 仅worker退休已签发receipt时使用，不授予普通intent-current或新的business mutation资格。 */
    private[mysql] def requireOriginalReservePlanDropRoute(
        exactPlan:MySqlFencedTransaction.SourceReservePlan5,
        exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt,
        exactRegistration:RestrictedSourcePlanDropRegistration):Unit =
      if adapter==null || continuation==null || server==null || sourceRoot==null || sqlRoot==null ||
          sqlLedger==null || exactPlan==null || exactReceipt==null || exactRegistration==null ||
          (exactPlan.intent ne this) || exactPlan.parent==null || (exactPlan.parent.link ne exactLink) ||
          (exactPlan.parent.reference ne parentReference) then
        throw new IllegalStateException("exact original intent/plan/receipt/registration required for source-plan drop")
      exactReceipt.requireFor(exactPlan)
      adapter.requireRestrictedSourcePlanDropCurrent(this,continuation,server,sourceRoot,sqlRoot,sqlLedger,
        exactPlan,exactReceipt,exactRegistration)

    /** 原Main protection-view holder仅供Fenced核验carrier对象成员，不暴露SQL行。 */
    private[mysql] def sourceProtectionData(): MySqlBootSourceProtectionIndex.RetainedData = retainedProtection
    private[mysql] def carrierLocator(): com.wi.floatingpod.server.MainBootProjection.SourceCarrierLocator = carrierLocatorOwner

    override def toString: String = "RestrictedSourceReserveIntent[exact locator DATA; no SQL or R0 authority]"

  /** 只读worker已完全settle后返回Main；仅暴露原Fenced closure DATA。 */
  final class RestrictedSourceReservePlanBacking private[MySqlOwnedIoAdapter] (
      private val adapter: MySqlOwnedIoAdapter,
      private val tag: IoOperationTag,
      private val command: MySqlOwnedIoCommand,
      private val intent: RestrictedSourceReserveIntent,
      private val plan: MySqlFencedTransaction.SourceReservePlan5):
    private[mysql] def requireFor(actualAdapter: MySqlOwnedIoAdapter, actualTag: IoOperationTag,
        actualCommand: MySqlOwnedIoCommand, actualIntent: RestrictedSourceReserveIntent):
        MySqlFencedTransaction.SourceReserveEffectClosure5 =
      if (actualAdapter ne adapter) || tag != actualTag || (command ne actualCommand) ||
          (intent ne actualIntent) || plan == null || (plan.intent ne intent) ||
          plan.issuer == null || plan.exactRoot == null then
        throw new IllegalStateException("exact original restricted source read-plan completion required")
      intent.requireOriginalReservePlanBinding(intent.exactLink, intent.parentReference, plan.exactRoot,
        intent.exactDomainObservation, intent.expectedSourceObservation)
      val closure = plan.effectClosure()
      if closure == null || !closure.belongsTo(plan, plan.issuer, plan.exactRoot, plan.exactRoot.profile) then
        throw new IllegalStateException("Fenced did not retain the exact restricted source effect closure")
      closure
    private[mysql] def exactPlan: MySqlFencedTransaction.SourceReservePlan5 = plan

    /** Revalidates the already Main-ACKed backing for one fixed R0 execution route. */
    private[mysql] def requireR0ExecutionFor(
        actualAdapter:MySqlOwnedIoAdapter,
        exactAck:RestrictedSourcePlanAck,
        exactIntent:RestrictedSourceReserveIntent):MySqlFencedTransaction.SourceReservePlan5 =
      if (actualAdapter ne adapter) || exactAck==null || (exactAck.adapter ne actualAdapter) ||
          (exactAck.backing ne this) || (exactAck.intent ne intent) || (exactIntent ne intent) ||
          (exactAck.tag!=tag) || (exactAck.command ne command) || plan==null ||
          (plan.intent ne intent) || (plan.issuer==null) || (plan.exactRoot==null) then
        throw new IllegalStateException("restricted SOURCE R0 requires the exact original Main-ACKed retained plan backing")
      val closure=requireFor(actualAdapter,tag,command,intent)
      plan.requireRetained()
      if closure==null || (plan.exactEffectClosure ne closure) then
        throw new IllegalStateException("restricted SOURCE R0 lost the exact retained Fenced effect closure")
      intent.requireOriginalReservePlanBinding(intent.exactLink,intent.parentReference,plan.exactRoot,
        intent.exactDomainObservation,intent.expectedSourceObservation)
      plan

    /** Main 只核验实际 ACK/backing，不调用仅原 IO worker 可通过的 retained-plan guard。 */
    private[mysql] def requireR0SubmissionFor(
        actualAdapter:MySqlOwnedIoAdapter,
        exactAck:RestrictedSourcePlanAck,
        exactIntent:RestrictedSourceReserveIntent):MySqlFencedTransaction.SourceReservePlan5 =
      if (actualAdapter ne adapter) || exactAck==null || (exactAck.adapter ne actualAdapter) ||
          (exactAck.backing ne this) || (exactAck.intent ne intent) || (exactIntent ne intent) ||
          (exactAck.tag!=tag) || (exactAck.command ne command) || plan==null ||
          (plan.intent ne intent) || (plan.issuer==null) || (plan.exactRoot==null) || !plan.isIssued then
        throw new IllegalStateException("restricted SOURCE R0 Main submission requires the exact ACKed issued plan backing")
      val closure=requireFor(actualAdapter,tag,command,intent)
      if closure==null || (plan.exactEffectClosure ne closure) then
        throw new IllegalStateException("restricted SOURCE R0 Main submission lost the exact immutable plan closure")
      plan

    override def toString: String = "RestrictedSourceReservePlanBacking[opaque plan; closure DATA only]"

  /** 仅原 adapter 的 Main 收口生产者创建；worker 不接收 server、Main closure 或游戏对象。 */
  final class FreshCompletionHandoff private[MySqlOwnedIoAdapter] (
      private[mysql] val snapshot: MySqlOwnedFormat3Snapshot,
      private[mysql] val tag: IoOperationTag,
      private[mysql] val epoch: Long)

  /** Test-only assembly for a real same-package verifyAndTransfer result; source invocation remains on the owned worker. */
  private[mysql] def withCertifiedOrdinaryConnectionSourceForAssembly(
      capacity: Int,
      initialMain: Thread,
      workerName: String,
      identity: MySqlOwnedIoIdentity,
      source: () => VerifiedMySqlConnection,
      enableAdmissionMaintenance: Boolean = false
  ): MySqlOwnedIoAdapter =
    val adapter = new MySqlOwnedIoAdapter(capacity, initialMain, workerName, identity, enableAdmissionMaintenance)
    adapter.installCertifiedOrdinaryConnectionSourceForAssembly(source)
    adapter

  /** Test-only Fresh assembly using the real same-package verifier after the actual owner holder is published. */
  private[mysql] def withFreshConnectionSourceForAssembly(
      capacity: Int,
      initialMain: Thread,
      workerName: String,
      identity: MySqlOwnedIoIdentity,
      source: () => VerifiedMySqlConnection,
      enableAdmissionMaintenance: Boolean = false
  ): MySqlOwnedIoAdapter =
    val adapter = new MySqlOwnedIoAdapter(capacity, initialMain, workerName, identity, enableAdmissionMaintenance)
    adapter.installFreshConnectionSourceForAssembly(source)
    adapter

/** 固定 command 分类，不根据 reply 自称成功；只读和纯内存释放不更换业务数据。 */
private[mysql] object MySqlBootReadEpoch:
  def mayChangeRepository(command: MySqlOwnedIoCommand): Boolean =
    MySqlOwnedIoCommand.requireGenericRoute(command)
    command match
      case MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(_) | MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(_) | MySqlOwnedIoCommand.ExecuteDbFirstRecall(_) | MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(_) | MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(_,_) |
          MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(_) => true
      case MySqlOwnedIoCommand.PrepareNormalRecall(_, _) | MySqlOwnedIoCommand.CheckpointNormalRecall(_, _, _) => true
      case MySqlOwnedIoCommand.Run(operation, _) => !MySqlFormat3ReadDispatcher.isRead(operation)
      case MySqlOwnedIoCommand.Transaction(operations, _) => operations.exists(operation => !MySqlFormat3ReadDispatcher.isRead(operation))
      case MySqlOwnedIoCommand.PrepareControlledDebug(_, _) | MySqlOwnedIoCommand.CommitControlledDebug(_, _, _, _) |
        MySqlOwnedIoCommand.PrepareControlledUnbound(_, _) | MySqlOwnedIoCommand.CommitControlledUnbound(_, _, _, _) |
        MySqlOwnedIoCommand.CommitOwnedMode(_, _) | MySqlOwnedIoCommand.CommitTrackedDrift(_, _) |
        MySqlOwnedIoCommand.Boot(_, _, _) | MySqlOwnedIoCommand.BootFresh(_, _, _, _) |
        MySqlOwnedIoCommand.BootFreshV3(_,_) | MySqlOwnedIoCommand.BootOrdinaryV3(_,_) |
        MySqlOwnedIoCommand.BootFresh5(_,_,_,_,_) | MySqlOwnedIoCommand.BootOrdinary5(_,_,_,_,_) |
        MySqlOwnedIoCommand.BootNormalPreGenesis(_, _, _, _) | MySqlOwnedIoCommand.BootOrdinaryCertified(_, _, _, _) => true
      case _ => false
