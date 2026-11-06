package com.wi.floatingpod.storage.mysql

import java.sql.SQLException

/** F3 tracked drift workflow 的数据库阶段。F2 business receipt reconciliation 不能消费这些阶段。 */
private[mysql] enum MySqlDriftWorkflowPhase:
  case PreparedForReserve
  case ReserveCommitUnknown
  case ReserveCommittedAwaitingReadback
  case ReserveCommittedReadbackUnknown
  case ReserveCommittedExecutePending
  case ExecuteFailedAfterReserve
  case ExecuteCommitUnknown
  case ExecuteCommittedAwaitingReadback
  case ExecuteCommittedReadbackUnknown

private[mysql] final class MySqlDriftWorkflowBlockedException(
    val phase: MySqlDriftWorkflowPhase,
    cause: Throwable
) extends SQLException("format3 owned tracked drift workflow remains responsible at " + phase.toString, cause)

/**
 * 保留 Main 原命令、admission 和两阶段 full20 计划的同一引用。
 * 此对象只表示待履行/未知责任，不自行证明任何 SQL 已提交或已读回。
 */
private[mysql] final class MySqlDriftWorkflowResponsibility private[mysql] (
    val commandScope: MySqlRepositoryCommandScope,
    val operation: MySqlRepositoryOperation,
    val materialization: MySqlDriftWorkflowMaterialization,
    val admission: MySqlBusinessAdmission,
    val phase: MySqlDriftWorkflowPhase,
    val blockedFailure: Option[MySqlDriftWorkflowBlockedException]
):
  require(commandScope != null && operation != null && materialization != null && admission != null && phase != null &&
    blockedFailure != null && !blockedFailure.exists(_ == null), "complete F3 tracked drift workflow responsibility is required")
  require(commandScope.hasOperationPlan(), "F3 mode responsibility requires the exact Main-retained operation plan")
  require(commandScope.operationPlan() eq materialization.exactPlan,
    "F3 mode responsibility must retain the plan used to create its full20 images")
  require(materialization.exactOperation == operation,
    "F3 mode responsibility operation differs from its immutable materialization")
  require(operation.isInstanceOf[MySqlRepositoryOperation.CommitTrackedActiveDrift],
    "F3 mode responsibility is limited to the planned owned-mode operation")
  require(admission.token.scope eq commandScope.exactAuditScope(),
    "F3 mode responsibility admission differs from its exact command scope")
  require((blockedFailure.nonEmpty) == (phase == MySqlDriftWorkflowPhase.ReserveCommitUnknown ||
    phase == MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback ||
    phase == MySqlDriftWorkflowPhase.ReserveCommittedReadbackUnknown ||
    phase == MySqlDriftWorkflowPhase.ExecuteCommitUnknown ||
    phase == MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback ||
    phase == MySqlDriftWorkflowPhase.ExecuteCommittedReadbackUnknown ||
    phase == MySqlDriftWorkflowPhase.ReserveCommittedExecutePending ||
    phase == MySqlDriftWorkflowPhase.ExecuteFailedAfterReserve),
    "F3 blocked phases must retain an explicit failure or obligation")

  private[mysql] def sameWorkflow(scope: MySqlRepositoryCommandScope,
      currentOperation: MySqlRepositoryOperation, currentMaterialization: MySqlDriftWorkflowMaterialization): Boolean =
    (commandScope eq scope) && (operation == currentOperation) && (materialization eq currentMaterialization)

private[mysql] object MySqlDriftWorkflowResponsibility:
  def issue(commandScope: MySqlRepositoryCommandScope, operation: MySqlRepositoryOperation,
      materialization: MySqlDriftWorkflowMaterialization, admission: MySqlBusinessAdmission,
      phase: MySqlDriftWorkflowPhase, primary: Option[Throwable]): MySqlDriftWorkflowResponsibility =
    if primary == null || primary.exists(_ == null) then
      throw new IllegalArgumentException("F3 responsibility failure option is required")
    val blocking = phase match
      case MySqlDriftWorkflowPhase.PreparedForReserve =>
        if primary.nonEmpty then throw new IllegalArgumentException("prepared tracked drift workflow cannot carry a failure")
        None
      case MySqlDriftWorkflowPhase.ReserveCommittedExecutePending =>
        Some(new MySqlDriftWorkflowBlockedException(phase, primary.getOrElse(
          new IllegalStateException("reserve was acknowledged; the fixed execute operation remains pending"))))
      case MySqlDriftWorkflowPhase.ReserveCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("reserve awaiting readback is not a reported failure")
        Some(new MySqlDriftWorkflowBlockedException(phase,
          new IllegalStateException("reserve commit was acknowledged; exact full20 readback remains required")))
      case MySqlDriftWorkflowPhase.ExecuteCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("awaiting readback is not a reported failure")
        Some(new MySqlDriftWorkflowBlockedException(phase,
          new IllegalStateException("execute commit was acknowledged; exact full20 readback remains required")))
      case MySqlDriftWorkflowPhase.ReserveCommitUnknown | MySqlDriftWorkflowPhase.ReserveCommittedReadbackUnknown |
          MySqlDriftWorkflowPhase.ExecuteFailedAfterReserve | MySqlDriftWorkflowPhase.ExecuteCommitUnknown |
          MySqlDriftWorkflowPhase.ExecuteCommittedReadbackUnknown =>
        val cause = primary.getOrElse(throw new IllegalArgumentException("unknown/unverified phase requires its original cause"))
        Some(new MySqlDriftWorkflowBlockedException(phase, cause))
    new MySqlDriftWorkflowResponsibility(commandScope, operation, materialization, admission, phase, blocking)

/** 已收到 reserve ACK 后的 execute 边界；已进入 commit/readback 未知态时原样保留未知责任。 */
private[mysql] object MySqlDriftExecuteFailureBoundary:
  def shouldRecordKnownFailure(retainedExecutePending: Boolean, primary: Throwable): Boolean =
    if !retainedExecutePending || primary == null || primary.isInstanceOf[MySqlTransactionCleanupUncertainException] then false
    else primary match
      case blocked: MySqlDriftWorkflowBlockedException =>
        blocked.phase != MySqlDriftWorkflowPhase.ReserveCommitUnknown &&
          blocked.phase != MySqlDriftWorkflowPhase.ReserveCommittedReadbackUnknown &&
          blocked.phase != MySqlDriftWorkflowPhase.ExecuteCommitUnknown &&
          blocked.phase != MySqlDriftWorkflowPhase.ExecuteCommittedReadbackUnknown
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