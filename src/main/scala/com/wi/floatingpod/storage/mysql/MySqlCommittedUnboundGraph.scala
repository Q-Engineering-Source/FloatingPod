package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, UnboundCommitOutcome, UnboundIntentOrigin, UnboundPreparationReceipt}

/** 实际 ACK 后同 owned session RR readback 的不可变结果；不是 Ready/custody/IOExitProof。
  * constructor为private companion，producer为private[mysql]；Main资格来自exact completion + retained scope。
  */
final class MySqlCommittedUnboundGraph private (
    val tag: IoOperationTag, val receipt: UnboundPreparationReceipt,
    val actualCommit: UnboundCommitOutcome, val state: FloatingPodState,
    val locator: PhysicalRecoveryLocator, val intent: MySqlRecoveryIntentData,
    val inventory: MySqlRecoverySnapshot
)

/** 已有真实成功 ACK，后续 readback 失败；不允许当作未提交、rollback 或重发资格。 */
final case class MySqlUnboundAfterCommitReadbackFailure(
    tag: IoOperationTag, receipt: UnboundPreparationReceipt,
    actualCommit: UnboundCommitOutcome, cause: Throwable
):
  require(tag != null && receipt != null && cause != null, "after-ACK responsibility is required")
  require(actualCommit.isInstanceOf[UnboundCommitOutcome.Committed] ||
    actualCommit.isInstanceOf[UnboundCommitOutcome.AlreadyCommitted], "actual successful commit outcome required")

object MySqlCommittedUnboundGraph:
  /** 仅 adapter 固定 actualworker command 在真实 commit ACK/readRecoverySnapshot 后调用。
    * 单元可直接核 pure validation，但其 synthetic graph 不构成 SQL 执行证据。
    */
  private[mysql] def fromReadback(tag: IoOperationTag, receipt: UnboundPreparationReceipt,
      actualCommit: UnboundCommitOutcome, inventory: MySqlRecoverySnapshot,
      admission: MySqlOwnedDatabaseAdmission, activeLimit: Int): MySqlCommittedUnboundGraph =
    require(tag != null && receipt != null && inventory != null && admission != null, "complete actual readback required")
    require(activeLimit >= 1 && activeLimit <= 256, "explicit actual Boot capacity required")
    require(receipt.origin == UnboundIntentOrigin.DebugItemCreate && receipt.createOperationUuid.nonEmpty &&
      receipt.observedIdentity.isEmpty, "controlled publication cannot accept observed adoption")
    require(tag.bootId == admission.bootId && tag.bootGeneration == admission.bootGeneration &&
      tag.connectionGeneration == admission.connectionGeneration &&
      inventory.bootUuid == tag.bootId && inventory.bootGeneration == tag.bootGeneration &&
      inventory.connectionGeneration == tag.connectionGeneration, "actual completion/admission/snapshot generation mismatch")
    require(inventory.state == MySqlRecoveryObservationState.CertifiedSchemaObserved &&
      inventory.writerLease == MySqlObservedWriterLease.OwnedUnexpired &&
      inventory.recovery.binding.contains(receipt.binding) && admission.bindingId == receipt.binding.bindingUuid &&
      inventory.writer.bindingUuid == admission.bindingId && inventory.writer.epoch == admission.writerEpoch &&
      inventory.writer.ownerBootUuid.contains(tag.bootId) && inventory.databaseTimeAfter.isBefore(inventory.writer.leaseUntil),
      "actual certified current writer/binding readback required")
    inventory.recovery.validateGraph()
    val commitState = actualCommit match
      case UnboundCommitOutcome.Committed(value) => value
      case UnboundCommitOutcome.AlreadyCommitted(value) => value
      case _ => throw new IllegalArgumentException("rejected commit cannot issue committed graph")
    val rows = inventory.pods.filter(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "pod_uuid") == receipt.podUuid)
    require(rows.size == 1, "actual receipt UUID must have exactly one readback row")
    val state = MySqlStoredRows.materializeGenericState(rows.head)
    require(state == commitState && state.uuid == receipt.podUuid && state.ownerUuid.isEmpty &&
      state.operatingMode == PodOperatingMode.WorldIdle && state.isActive &&
      state.stateVersion == Math.addExact(receipt.preparedVersion, 1L) &&
      state.dimensionId.contains(receipt.targetPose.dimensionId) &&
      state.worldName == receipt.targetPose.worldName && state.posX == receipt.targetPose.posX &&
      state.posY == receipt.targetPose.posY && state.posZ == receipt.targetPose.posZ &&
      state.yaw == receipt.targetPose.yaw && state.pitch == receipt.targetPose.pitch, "ACK/state/receipt readback mismatch")
    require(inventory.pods.count(_.isActiveFlag == 1) <= activeLimit, "actual global active capacity exceeded")
    val locators = inventory.recovery.locators.filter(_.podUuid == receipt.podUuid)
    require(locators.size == 1, "exact active locator required")
    val locator = locators.head
    val chunks = MySqlRepositoryTransitionRules.chunksAt(receipt.targetPose.dimensionId, state.posX, state.posZ)
    require(locator.attemptUuid == receipt.attemptUuid && locator.state == PhysicalRecoveryLocatorState.RowActive &&
      locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive && locator.domainVersion.contains(state.stateVersion) &&
      locator.targetDimensionId == state.dimensionId && locator.chunks.size == 25 && locator.chunks.toSet == chunks,
      "complete exact25 committed locator required")
    val snapshot = MySqlFullRepositorySnapshot(inventory.pods, inventory.entitlements, inventory.recovery, inventory.bootstrapIntents)
    snapshot.validate()
    require(MySqlUnboundTransitionRules.findPreparation(snapshot, receipt.podUuid).contains(receipt), "actual original receipt readback differs")
    val intents = inventory.recovery.intents.filter(_.podUuid == receipt.podUuid)
    require(intents.size == 1, "exact original intent history required")
    val intent = intents.head
    require(intent.phase == MySqlRecoveryIntentPhase.Committed && intent.committedDomainVersion.contains(state.stateVersion) &&
      intent.committedWorldName.contains(state.worldName) && intent.committedPosX.contains(state.posX) &&
      intent.committedPosY.contains(state.posY) && intent.committedPosZ.contains(state.posZ) &&
      intent.committedYaw.contains(state.yaw.toDouble) && intent.committedPitch.contains(state.pitch.toDouble) &&
      intent.committedDimensionId == state.dimensionId && intent.committedAt.nonEmpty, "complete committed intent differs")
    new MySqlCommittedUnboundGraph(tag, receipt, actualCommit, state, locator, intent, inventory)

/** fatal readback 仍按原 worker fatal退出；无cause反向环的suppressed责任上下文。 */
final class MySqlUnboundCommitObligationNotice(val tag: IoOperationTag,
    val receipt: UnboundPreparationReceipt, val actualCommit: UnboundCommitOutcome)
    extends IllegalStateException("actual controlled commit ACK retained after readback failure")
