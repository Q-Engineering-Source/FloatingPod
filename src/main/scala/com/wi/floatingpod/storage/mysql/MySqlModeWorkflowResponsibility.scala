package com.wi.floatingpod.storage.mysql

import java.sql.SQLException

/** F3 mode workflow 的数据库阶段。F2 business receipt reconciliation 不能消费这些阶段。 */
private[mysql] enum MySqlModeWorkflowPhase:
  case PreparedForReserve
  case ReserveCommitUnknown
  case ReserveCommittedAwaitingReadback
  case ReserveCommittedReadbackUnknown
  case ReserveCommittedExecutePending
  case ExecuteFailedAfterReserve
  case ExecuteCommitUnknown
  case ExecuteCommittedAwaitingReadback
  case ExecuteCommittedReadbackUnknown

private[mysql] final class MySqlModeWorkflowBlockedException(
    val phase: MySqlModeWorkflowPhase,
    cause: Throwable
) extends SQLException("format3 owned mode workflow remains responsible at " + phase.toString, cause)

/**
 * 保留 Main 原命令、admission 和两阶段 full20 计划的同一引用。
 * 此对象只表示待履行/未知责任，不自行证明任何 SQL 已提交或已读回。
 */
private[mysql] final class MySqlModeWorkflowResponsibility private[mysql] (
    val commandScope: MySqlRepositoryCommandScope,
    val operation: MySqlRepositoryOperation,
    val materialization: MySqlModeWorkflowMaterialization,
    val admission: MySqlBusinessAdmission,
    val phase: MySqlModeWorkflowPhase,
    val blockedFailure: Option[MySqlModeWorkflowBlockedException]
):
  require(commandScope != null && operation != null && materialization != null && admission != null && phase != null &&
    blockedFailure != null && !blockedFailure.exists(_ == null), "complete F3 mode workflow responsibility is required")
  require(commandScope.hasOperationPlan(), "F3 mode responsibility requires the exact Main-retained operation plan")
  require(commandScope.operationPlan() eq materialization.exactPlan,
    "F3 mode responsibility must retain the plan used to create its full20 images")
  require(materialization.exactOperation == operation,
    "F3 mode responsibility operation differs from its immutable materialization")
  require(operation.isInstanceOf[MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned],
    "F3 mode responsibility is limited to the planned owned-mode operation")
  require(admission.token.scope eq commandScope.exactAuditScope(),
    "F3 mode responsibility admission differs from its exact command scope")
  require((blockedFailure.nonEmpty) == (phase == MySqlModeWorkflowPhase.ReserveCommitUnknown ||
    phase == MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback ||
    phase == MySqlModeWorkflowPhase.ReserveCommittedReadbackUnknown ||
    phase == MySqlModeWorkflowPhase.ExecuteCommitUnknown ||
    phase == MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback ||
    phase == MySqlModeWorkflowPhase.ExecuteCommittedReadbackUnknown ||
    phase == MySqlModeWorkflowPhase.ReserveCommittedExecutePending ||
    phase == MySqlModeWorkflowPhase.ExecuteFailedAfterReserve),
    "F3 blocked phases must retain an explicit failure or obligation")

  private[mysql] def sameWorkflow(scope: MySqlRepositoryCommandScope,
      currentOperation: MySqlRepositoryOperation, currentMaterialization: MySqlModeWorkflowMaterialization): Boolean =
    (commandScope eq scope) && (operation == currentOperation) && (materialization eq currentMaterialization)

private[mysql] object MySqlModeWorkflowResponsibility:
  def issue(commandScope: MySqlRepositoryCommandScope, operation: MySqlRepositoryOperation,
      materialization: MySqlModeWorkflowMaterialization, admission: MySqlBusinessAdmission,
      phase: MySqlModeWorkflowPhase, primary: Option[Throwable]): MySqlModeWorkflowResponsibility =
    if primary == null || primary.exists(_ == null) then
      throw new IllegalArgumentException("F3 responsibility failure option is required")
    val blocking = phase match
      case MySqlModeWorkflowPhase.PreparedForReserve =>
        if primary.nonEmpty then throw new IllegalArgumentException("prepared mode workflow cannot carry a failure")
        None
      case MySqlModeWorkflowPhase.ReserveCommittedExecutePending =>
        Some(new MySqlModeWorkflowBlockedException(phase, primary.getOrElse(
          new IllegalStateException("reserve was acknowledged; the fixed execute operation remains pending"))))
      case MySqlModeWorkflowPhase.ReserveCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("reserve awaiting readback is not a reported failure")
        Some(new MySqlModeWorkflowBlockedException(phase,
          new IllegalStateException("reserve commit was acknowledged; exact full20 readback remains required")))
      case MySqlModeWorkflowPhase.ExecuteCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("awaiting readback is not a reported failure")
        Some(new MySqlModeWorkflowBlockedException(phase,
          new IllegalStateException("execute commit was acknowledged; exact full20 readback remains required")))
      case MySqlModeWorkflowPhase.ReserveCommitUnknown | MySqlModeWorkflowPhase.ReserveCommittedReadbackUnknown |
          MySqlModeWorkflowPhase.ExecuteFailedAfterReserve | MySqlModeWorkflowPhase.ExecuteCommitUnknown |
          MySqlModeWorkflowPhase.ExecuteCommittedReadbackUnknown =>
        val cause = primary.getOrElse(throw new IllegalArgumentException("unknown/unverified phase requires its original cause"))
        Some(new MySqlModeWorkflowBlockedException(phase, cause))
    new MySqlModeWorkflowResponsibility(commandScope, operation, materialization, admission, phase, blocking)

/** 已收到 reserve ACK 后的 execute 边界；已进入 commit/readback 未知态时原样保留未知责任。 */
private[mysql] object MySqlModeExecuteFailureBoundary:
  def shouldRecordKnownFailure(retainedExecutePending: Boolean, primary: Throwable): Boolean =
    if !retainedExecutePending || primary == null || primary.isInstanceOf[MySqlTransactionCleanupUncertainException] then false
    else primary match
      case blocked: MySqlModeWorkflowBlockedException =>
        blocked.phase != MySqlModeWorkflowPhase.ReserveCommitUnknown &&
          blocked.phase != MySqlModeWorkflowPhase.ReserveCommittedReadbackUnknown &&
          blocked.phase != MySqlModeWorkflowPhase.ExecuteCommitUnknown &&
          blocked.phase != MySqlModeWorkflowPhase.ExecuteCommittedReadbackUnknown
      case _ => true
  def afterReserve[A](knownFailureStillRecordable: Throwable => Boolean)(executePhase: => A)(recordKnownFailure: Throwable => Throwable): A =
    if knownFailureStillRecordable == null then
      throw new IllegalArgumentException("post-reserve execute failure classification is required")
    try executePhase
    catch
      case primary: Throwable =>
        val recordable =
          try knownFailureStillRecordable(primary)
          catch
            case checkFailure: Throwable =>
              if checkFailure ne primary then primary.addSuppressed(checkFailure)
              throw primary
        if !recordable then throw primary
        val recorded =
          try recordKnownFailure(primary)
          catch
            case recordFailure: Throwable =>
              if recordFailure ne primary then primary.addSuppressed(recordFailure)
              throw primary
        if recorded == null then
          val missingRecord = new IllegalStateException("post-reserve execute failure recorder returned no retained failure")
          if missingRecord ne primary then primary.addSuppressed(missingRecord)
          throw primary
        throw recorded