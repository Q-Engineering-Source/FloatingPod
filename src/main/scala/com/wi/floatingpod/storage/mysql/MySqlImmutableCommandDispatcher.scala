package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.storage.{
  ActivityTransitionStore,
  AdmissionOutcome,
  DeactivationOutcome,
  FloatingPodRepository,
  ObservedUnboundPod,
  ObservedUnboundUniquenessReceipt,
  OperatingModeChangeOutcome,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryLocator,
  PhysicalRecoveryStore,
  RecoveryDimensionBackfillOutcome,
  RecoveryPodStore,
  StoredPodRow,
  UnboundCommitOutcome,
  UnboundPreparationReceipt,
  PlayerEntitlementStore
}

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{DeployRequest, RetractRequest, Request, MutationReceipt}
import java.util.UUID
import scala.util.{Failure, Success, Try}

/** 固定不可变 repository 操作；无任意 callback、game object、JDBC 或 owner resource。 */
enum MySqlRepositoryOperation:
  case FindOriginalDeploymentOperation(actorUuid: UUID, originalOperationId: UUID)
  case PrepareNormalRecall(request: MySqlNormalRecallRequest)
  case CheckpointNormalRecall(request: MySqlNormalRecallCheckpointRequest)
  case Insert(state: FloatingPodState)
  case Update(state: FloatingPodState)
  case UpdateInactiveDomain(request: MySqlInactiveDomainUpdateRequest)
  case FindByUuid(uuid: UUID)
  case FindByOwner(ownerUuid: UUID)
  case FindAllActive
  case FindAll
  case Delete(uuid: UUID)
  case FindByPlayerUuid(playerUuid: UUID)
  case FindDesiredActiveRows
  case FindStoredByUuid(uuid: UUID)
  case FindAllRecoverable
  case BackfillMissingActiveDimension(uuid: UUID,       expectedStateVersion: Long,       authoritativeDimensionId: Int)
  case ActivateIfAdmissible(target: FloatingPodState,       authenticatedOwner: UUID,       expectedStateVersion: Long,       dimensionId: Int,       globalActiveLimit: Int)
  case DeactivateIfExpectedActive(podUuid: UUID,       expectedOwner: Option[UUID],       expectedStateVersion: Long)
  case FindPhysicalRecoveryBinding
  case CreateOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int)
  case CertifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int)
  case PrepareObservedUnbound(observation: ObservedUnboundPod,       expectedBinding: PhysicalRecoveryBinding,       attemptUuid: UUID,       uniquenessProof: ObservedUnboundUniquenessReceipt)
  case CommitObservedUnbound(expectedBinding: PhysicalRecoveryBinding,       podUuid: UUID,       attemptUuid: UUID,       expectedPreparedVersion: Long,       actualPose: FloatingPodPose,       globalActiveLimit: Int)
  case PrepareUnboundCreate(expectedBinding: PhysicalRecoveryBinding,       podUuid: UUID,       attemptUuid: UUID,       targetPose: FloatingPodPose,       operationUuid: UUID)
  case CommitUnboundCreate(expectedBinding: PhysicalRecoveryBinding,       podUuid: UUID,       attemptUuid: UUID,       expectedPreparedVersion: Long,       actualPose: FloatingPodPose,       operationUuid: UUID,       globalActiveLimit: Int)
  case PrepareControlledDebugCreate(expectedBinding: PhysicalRecoveryBinding, podUuid: UUID,
      attemptUuid: UUID, footprintUuid: UUID, targetPose: FloatingPodPose, createOperationUuid: UUID)
  case CommitControlledDebugCreate(expectedBinding: PhysicalRecoveryBinding, podUuid: UUID,
      attemptUuid: UUID, footprintUuid: UUID, expectedPreparedVersion: Long,
      actualPose: FloatingPodPose, createOperationUuid: UUID, globalActiveLimit: Int)
  case FindUnboundPreparation(podUuid: UUID)
  case PrepareCreateLocator(podUuid: UUID,       expectedDomainVersion: Long,       attemptUuid: UUID,       targetDimensionId: Int,       chunks: Set[PhysicalRecoveryChunk])
  case ActivatePreparedIfAdmissible(target: FloatingPodState,       authenticatedOwner: UUID,       expectedStateVersion: Long,       dimensionId: Int,       globalActiveLimit: Int,       attemptUuid: UUID)
  case DeactivateTrackedIfExpectedActive(podUuid: UUID,       expectedOwner: Option[UUID],       expectedStateVersion: Long,       attemptUuid: UUID)
  case CommitTrackedActiveDrift(binding: PhysicalRecoveryBinding, criticalBase: FloatingPodState, desired: FloatingPodState, expectedAttemptUuid: UUID)
  case UpdateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID)
  case SetOperatingModeTrackedIfOwned(expectedBinding: PhysicalRecoveryBinding,       podUuid: UUID,       authenticatedOwner: UUID,       expectedStateVersion: Long,       attemptUuid: UUID,       targetMode: PodOperatingMode,       actualPose: FloatingPodPose)
  case FindPhysicalRecoveryLocator(podUuid: UUID)
  case FindAllPhysicalRecoveryLocators
  case VerifyInactiveAbsence(podUuid: UUID,       expectedStateVersion: Long,       attemptUuid: UUID)

/** 历史部署结果的不可变投影；不持有Row、JDBC、预算或执行权限。 */
final class MySqlOriginalDeploymentOperation private[mysql] (
    val actorUuid: UUID,
    val originalOperationId: UUID,
    val request: Request,
    val receipt: MutationReceipt,
    val rowRevision: Long
):
  require(actorUuid != null && originalOperationId != null && request != null && receipt != null && rowRevision >= 0L,
    "complete original deployment result required")
  require(request.isInstanceOf[DeployRequest] || request.isInstanceOf[RetractRequest], "original mutation request required")
  locally {
    val receiptRequest: Request = receipt.commandKind() match
      case DeploymentCommandWireCodec.MutationKind.DEPLOY => new DeployRequest(receipt.operationId(),
        receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())
      case DeploymentCommandWireCodec.MutationKind.RETRACT => new RetractRequest(receipt.operationId(),
        receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())
    require(request == receiptRequest && receipt.operationId() == originalOperationId,
      "receipt differs from original mutation identity")
  }

enum MySqlRepositoryReply:
  case OriginalDeploymentOperation(value: Option[MySqlOriginalDeploymentOperation])
  case InactiveDomainUpdate(value: MySqlInactiveDomainUpdateOutcome)
  case State(value: FloatingPodState)
  case OptionalState(value: Option[FloatingPodState])
  case States(value: List[FloatingPodState])
  case Deleted(value: Boolean)
  case Entitlement(value: Option[PlayerEntitlement])
  case StoredRows(value: List[StoredPodRow])
  case OptionalStoredRow(value: Option[StoredPodRow])
  case Backfill(value: RecoveryDimensionBackfillOutcome)
  case Activation(value: AdmissionOutcome)
  case Deactivation(value: DeactivationOutcome)
  case OptionalBinding(value: Option[PhysicalRecoveryBinding])
  case Binding(value: PhysicalRecoveryBinding)
  case Preparation(value: UnboundPreparationReceipt)
  case UnboundCommit(value: UnboundCommitOutcome)
  case OptionalPreparation(value: Option[UnboundPreparationReceipt])
  case Locator(value: PhysicalRecoveryLocator)
  case ModeChange(value: OperatingModeChangeOutcome)
  case OptionalLocator(value: Option[PhysicalRecoveryLocator])
  case Locators(value: List[PhysicalRecoveryLocator])

/** 直接真实 five-trait 调用；生产仅传active owned view，test controlled traits只核此边界。 */
object MySqlImmutableCommandDispatcher:
  private[mysql] def requireGenericOperation(operation: MySqlRepositoryOperation): Unit =
    require(operation != null, "immutable operation required")
    operation match
      case MySqlRepositoryOperation.FindOriginalDeploymentOperation(_, _) =>
        throw new UnsupportedOperationException("deployment original read requires the dedicated planned owned route")
      case _ => ()

  private[mysql] def requireGenericOperations(operations: Vector[MySqlRepositoryOperation]): Unit =
    require(operations != null, "immutable transaction operations required")
    var index = 0
    while index < operations.size do
      requireGenericOperation(operations(index))
      index += 1

  type Stores = FloatingPodRepository & ActivityTransitionStore & PlayerEntitlementStore & RecoveryPodStore & PhysicalRecoveryStore
  def execute(operation: MySqlRepositoryOperation, stores: Stores): Try[MySqlRepositoryReply] =
    require(operation != null && stores != null, "fixed operation and actual stores are required")
    operation match
      case MySqlRepositoryOperation.FindOriginalDeploymentOperation(_, _) =>
        Failure(new UnsupportedOperationException("deployment original read requires the dedicated planned owned route"))
      case MySqlRepositoryOperation.PrepareNormalRecall(_) | MySqlRepositoryOperation.CheckpointNormalRecall(_) =>
        Failure(new UnsupportedOperationException("normal recall requires the original retained full20 owned route"))
      case MySqlRepositoryOperation.Insert(state) => stores.insert(state).map(MySqlRepositoryReply.State.apply)
      case MySqlRepositoryOperation.Update(state) => stores.update(state).map(MySqlRepositoryReply.State.apply)
      case MySqlRepositoryOperation.UpdateInactiveDomain(_) =>
        Failure(new UnsupportedOperationException("inactive F3 command requires original planned owned route"))
      case MySqlRepositoryOperation.FindByUuid(uuid) => stores.findByUuid(uuid).map(MySqlRepositoryReply.OptionalState.apply)
      case MySqlRepositoryOperation.FindByOwner(ownerUuid) => stores.findByOwner(ownerUuid).map(MySqlRepositoryReply.States.apply)
      case MySqlRepositoryOperation.FindAllActive => stores.findAllActive().map(MySqlRepositoryReply.States.apply)
      case MySqlRepositoryOperation.FindAll => stores.findAll().map(MySqlRepositoryReply.States.apply)
      case MySqlRepositoryOperation.Delete(uuid) => stores.delete(uuid).map(MySqlRepositoryReply.Deleted.apply)
      case MySqlRepositoryOperation.FindByPlayerUuid(playerUuid) => stores.findByPlayerUuid(playerUuid).map(MySqlRepositoryReply.Entitlement.apply)
      case MySqlRepositoryOperation.FindDesiredActiveRows => stores.findDesiredActiveRows().map(MySqlRepositoryReply.StoredRows.apply)
      case MySqlRepositoryOperation.FindStoredByUuid(uuid) => stores.findStoredByUuid(uuid).map(MySqlRepositoryReply.OptionalStoredRow.apply)
      case MySqlRepositoryOperation.FindAllRecoverable => stores.findAllRecoverable().map(MySqlRepositoryReply.States.apply)
      case MySqlRepositoryOperation.BackfillMissingActiveDimension(uuid, expectedStateVersion, authoritativeDimensionId) => stores.backfillMissingActiveDimension(uuid, expectedStateVersion, authoritativeDimensionId).map(MySqlRepositoryReply.Backfill.apply)
      case MySqlRepositoryOperation.ActivateIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit) => stores.activateIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit).map(MySqlRepositoryReply.Activation.apply)
      case MySqlRepositoryOperation.DeactivateIfExpectedActive(podUuid, expectedOwner, expectedStateVersion) => stores.deactivateIfExpectedActive(podUuid, expectedOwner, expectedStateVersion).map(MySqlRepositoryReply.Deactivation.apply)
      case MySqlRepositoryOperation.FindPhysicalRecoveryBinding => stores.findPhysicalRecoveryBinding().map(MySqlRepositoryReply.OptionalBinding.apply)
      case MySqlRepositoryOperation.CreateOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion) => stores.createOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion).map(MySqlRepositoryReply.Binding.apply)
      case MySqlRepositoryOperation.CertifyPhysicalRecoveryBinding(expectedUuid, expectedFormatVersion) => stores.certifyPhysicalRecoveryBinding(expectedUuid, expectedFormatVersion).map(MySqlRepositoryReply.Binding.apply)
      case MySqlRepositoryOperation.PrepareObservedUnbound(observation, expectedBinding, attemptUuid, uniquenessProof) => stores.prepareObservedUnbound(observation, expectedBinding, attemptUuid, uniquenessProof).map(MySqlRepositoryReply.Preparation.apply)
      case MySqlRepositoryOperation.CommitObservedUnbound(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, globalActiveLimit) => stores.commitObservedUnbound(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, globalActiveLimit).map(MySqlRepositoryReply.UnboundCommit.apply)
      case MySqlRepositoryOperation.PrepareUnboundCreate(expectedBinding, podUuid, attemptUuid, targetPose, operationUuid) => stores.prepareUnboundCreate(expectedBinding, podUuid, attemptUuid, targetPose, operationUuid).map(MySqlRepositoryReply.Preparation.apply)
      case MySqlRepositoryOperation.CommitUnboundCreate(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit) => stores.commitUnboundCreate(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit).map(MySqlRepositoryReply.UnboundCommit.apply)
      case MySqlRepositoryOperation.PrepareControlledDebugCreate(_, _, _, _, _, _) |
          MySqlRepositoryOperation.CommitControlledDebugCreate(_, _, _, _, _, _, _, _) =>
        Failure(new UnsupportedOperationException("controlled debug create requires the dedicated retained F3 route"))
      case MySqlRepositoryOperation.FindUnboundPreparation(podUuid) => stores.findUnboundPreparation(podUuid).map(MySqlRepositoryReply.OptionalPreparation.apply)
      case MySqlRepositoryOperation.PrepareCreateLocator(podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks) => stores.prepareCreateLocator(podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks).map(MySqlRepositoryReply.Locator.apply)
      case MySqlRepositoryOperation.ActivatePreparedIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid) => stores.activatePreparedIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid).map(MySqlRepositoryReply.Activation.apply)
      case MySqlRepositoryOperation.DeactivateTrackedIfExpectedActive(podUuid, expectedOwner, expectedStateVersion, attemptUuid) => stores.deactivateTrackedIfExpectedActive(podUuid, expectedOwner, expectedStateVersion, attemptUuid).map(MySqlRepositoryReply.Deactivation.apply)
      case MySqlRepositoryOperation.CommitTrackedActiveDrift(_, _, _, _) =>
        Failure(new UnsupportedOperationException("tracked drift requires the dedicated retained F3 route"))
      case MySqlRepositoryOperation.UpdateTrackedActiveDrift(state, expectedAttemptUuid) => stores.updateTrackedActiveDrift(state, expectedAttemptUuid).map(MySqlRepositoryReply.State.apply)
      case MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(expectedBinding, podUuid, authenticatedOwner, expectedStateVersion, attemptUuid, targetMode, actualPose) => stores.setOperatingModeTrackedIfOwned(expectedBinding, podUuid, authenticatedOwner, expectedStateVersion, attemptUuid, targetMode, actualPose).map(MySqlRepositoryReply.ModeChange.apply)
      case MySqlRepositoryOperation.FindPhysicalRecoveryLocator(podUuid) => stores.findPhysicalRecoveryLocator(podUuid).map(MySqlRepositoryReply.OptionalLocator.apply)
      case MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators => stores.findAllPhysicalRecoveryLocators().map(MySqlRepositoryReply.Locators.apply)
      case MySqlRepositoryOperation.VerifyInactiveAbsence(podUuid, expectedStateVersion, attemptUuid) => stores.verifyInactiveAbsence(podUuid, expectedStateVersion, attemptUuid).map(MySqlRepositoryReply.Locator.apply)

  /** fn 是本模块固定执行逻辑，不是Main传入函数；Failure原cause抛进真实transaction。 */
  def transaction(operations: Vector[MySqlRepositoryOperation], stores: Stores): Try[Vector[MySqlRepositoryReply]] =
    require(operations != null && operations.forall(_ != null), "immutable transaction operations required")
    try requireGenericOperations(operations)
    catch case cause: UnsupportedOperationException => return Failure(cause)
    stores.transaction {
      operations.map(operation => execute(operation, stores).get)
    }
