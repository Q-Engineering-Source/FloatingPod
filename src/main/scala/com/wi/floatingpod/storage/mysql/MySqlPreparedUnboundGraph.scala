package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, UnboundIntentOrigin, UnboundPreparationReceipt}
import java.util.UUID

/** 固定prepare输入；proposal不是实际receipt的权威，也不声明存储会接受不同proposal replay。 */
final case class MySqlControlledUnboundPrepareRequest(expectedBinding: PhysicalRecoveryBinding,
    proposedPodUuid: UUID, proposedAttemptUuid: UUID, targetPose: FloatingPodPose, createOperationUuid: UUID):
  require(expectedBinding != null && proposedPodUuid != null && proposedAttemptUuid != null &&
    targetPose != null && createOperationUuid != null, "complete immutable controlled preparation request required")
  require(expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified, "certified preparation binding required")

  def repositoryOperation: MySqlRepositoryOperation =
    MySqlRepositoryOperation.PrepareUnboundCreate(expectedBinding, proposedPodUuid, proposedAttemptUuid,
      targetPose, createOperationUuid)

/** 同session实际prepare ACK后的完整readback；两个分支绝不能互当新建/Ready资格。 */
sealed trait MySqlPreparedUnboundGraph:
  def tag: IoOperationTag
  def request: MySqlControlledUnboundPrepareRequest
  def receipt: UnboundPreparationReceipt
  def state: FloatingPodState
  def locator: PhysicalRecoveryLocator
  def intent: MySqlRecoveryIntentData
  def inventory: MySqlRecoverySnapshot

object MySqlPreparedUnboundGraph:
  final class PreparedForPhysical private[MySqlPreparedUnboundGraph] (
      val tag: IoOperationTag, val request: MySqlControlledUnboundPrepareRequest,
      val receipt: UnboundPreparationReceipt, val state: FloatingPodState,
      val locator: PhysicalRecoveryLocator, val intent: MySqlRecoveryIntentData,
      val inventory: MySqlRecoverySnapshot) extends MySqlPreparedUnboundGraph

  /** 保留原intent与当前领域/locator历史；Main必须recovery/currentmapping核验，绝不能再spawn。 */
  final class ExistingCommitted private[MySqlPreparedUnboundGraph] (
      val tag: IoOperationTag, val request: MySqlControlledUnboundPrepareRequest,
      val receipt: UnboundPreparationReceipt, val state: FloatingPodState,
      val locator: PhysicalRecoveryLocator, val intent: MySqlRecoveryIntentData,
      val inventory: MySqlRecoverySnapshot) extends MySqlPreparedUnboundGraph

  private[mysql] def fromReadback(tag: IoOperationTag, request: MySqlControlledUnboundPrepareRequest,
      receipt: UnboundPreparationReceipt, inventory: MySqlRecoverySnapshot,
      admission: MySqlOwnedDatabaseAdmission, activeLimit: Int): MySqlPreparedUnboundGraph =
    require(request != null && receipt != null, "actual prepare receipt/request required")
    val snapshot = validatedInventory(tag, request.expectedBinding, inventory, admission, activeLimit)
    require(receipt.binding == request.expectedBinding && receipt.origin == UnboundIntentOrigin.DebugItemCreate &&
      receipt.observedIdentity.isEmpty && receipt.createOperationUuid.contains(request.createOperationUuid) &&
      receipt.targetPose == request.targetPose, "actual receipt differs from controlled operation/binding/pose")
    require(MySqlUnboundTransitionRules.findPreparation(snapshot, receipt.podUuid).contains(receipt),
      "actual original preparation receipt differs from readback")
    val rows = inventory.pods.filter(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "pod_uuid") == receipt.podUuid)
    val locators = inventory.recovery.locators.filter(_.podUuid == receipt.podUuid)
    val intents = inventory.recovery.intents.filter(_.podUuid == receipt.podUuid)
    require(rows.size == 1 && locators.size == 1 && intents.size == 1, "exact actual receipt row/locator/intent required")
    val state = MySqlStoredRows.materializeGenericState(rows.head)
    val locator = locators.head
    val intent = intents.head
    require(locator.attemptUuid == receipt.attemptUuid && intent.attemptUuid == receipt.attemptUuid,
      "actual preparation attempt history mismatch")
    intent.phase match
      case MySqlRecoveryIntentPhase.Prepared =>
        require(!state.isActive && state.ownerUuid.isEmpty && state.operatingMode == PodOperatingMode.WorldIdle &&
          state.stateVersion == receipt.preparedVersion && poseOf(state) == receipt.targetPose &&
          locator.state == PhysicalRecoveryLocatorState.DebugItemCreatePrepared &&
          locator.reasonCode == PhysicalRecoveryLocatorReason.DebugItemCreatePrepared &&
          locator.domainVersion.contains(receipt.preparedVersion) &&
          locator.targetDimensionId.contains(receipt.targetPose.dimensionId) && locator.chunks.size == 25 &&
          locator.chunks.toSet == MySqlRepositoryTransitionRules.chunksAt(receipt.targetPose.dimensionId,
            receipt.targetPose.posX, receipt.targetPose.posZ), "complete exact inactive controlled PREPARED graph required")
        new PreparedForPhysical(tag, request, receipt, state, locator, intent, inventory)
      case MySqlRecoveryIntentPhase.Committed =>
        // Full snapshot验证原COMMITTED fields与current row/locator；后续合法version/recall历史仍保留。
        require(intent.committedDomainVersion.contains(Math.addExact(receipt.preparedVersion, 1L)) &&
          state.stateVersion >= intent.committedDomainVersion.get, "actual committed preparation history required")
        new ExistingCommitted(tag, request, receipt, state, locator, intent, inventory)

  /** 与既有committed graph相同的actual generation/binding/writer门，并验证全部原始业务图。 */
  private[mysql] def validatedInventory(tag: IoOperationTag, binding: PhysicalRecoveryBinding,
      inventory: MySqlRecoverySnapshot, admission: MySqlOwnedDatabaseAdmission, activeLimit: Int): MySqlFullRepositorySnapshot =
    require(tag != null && binding != null && inventory != null && admission != null, "complete actual readback required")
    require(activeLimit >= 1 && activeLimit <= 256 && binding.cutoverState == PhysicalRecoveryCutoverState.Certified,
      "actual certified Boot binding/capacity required")
    require(tag.bootId == admission.bootId && tag.bootGeneration == admission.bootGeneration &&
      tag.connectionGeneration == admission.connectionGeneration && inventory.bootUuid == tag.bootId &&
      inventory.bootGeneration == tag.bootGeneration && inventory.connectionGeneration == tag.connectionGeneration,
      "actual completion/admission/readback generation mismatch")
    require(inventory.state == MySqlRecoveryObservationState.CertifiedSchemaObserved &&
      inventory.writerLease == MySqlObservedWriterLease.OwnedUnexpired && inventory.recovery.binding.contains(binding) &&
      admission.bindingId == binding.bindingUuid && inventory.writer.bindingUuid == admission.bindingId &&
      inventory.writer.epoch == admission.writerEpoch && inventory.writer.ownerBootUuid.contains(tag.bootId) &&
      inventory.databaseTimeAfter.isBefore(inventory.writer.leaseUntil), "actual certified current writer/binding required")
    require(inventory.manifest.bindingUuid == binding.bindingUuid && inventory.manifest.bootstrapState == "READY" &&
      !inventory.databaseTimeAfter.isBefore(inventory.databaseTimeBefore), "actual manifest/DB-time readback mismatch")
    val snapshot = MySqlFullRepositorySnapshot(inventory.pods, inventory.entitlements, inventory.recovery, inventory.bootstrapIntents)
    snapshot.validate()
    require(inventory.pods.count(_.isActiveFlag == 1) <= activeLimit, "actual Boot active capacity exceeded")
    snapshot

  private[mysql] def poseOf(state: FloatingPodState): FloatingPodPose =
    FloatingPodPose(state.worldName, state.posX, state.posY, state.posZ, state.yaw, state.pitch,
      state.dimensionId.getOrElse(throw new IllegalArgumentException("actual readback state has no dimension")))

/** prepare ACK已取得，readback失败不表示没有PREPARED，不允许盲重发。 */
final case class MySqlUnboundAfterPrepareReadbackFailure(tag: IoOperationTag,
    request: MySqlControlledUnboundPrepareRequest, actualReceipt: UnboundPreparationReceipt, cause: Throwable):
  require(tag != null && request != null && actualReceipt != null && cause != null, "actual after-prepare responsibility required")

/** 无cause反向环，原fatal携带exact实际ACK责任上下文。 */
final class MySqlUnboundPrepareObligationNotice(val tag: IoOperationTag,
    val request: MySqlControlledUnboundPrepareRequest, val actualReceipt: UnboundPreparationReceipt)
    extends IllegalStateException("actual preparation ACK retained after readback failure")
