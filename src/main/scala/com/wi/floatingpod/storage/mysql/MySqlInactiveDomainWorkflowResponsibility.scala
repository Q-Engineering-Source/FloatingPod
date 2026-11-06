package com.wi.floatingpod.storage.mysql

import java.sql.SQLException

/** F3 inactive domain workflow 的数据库阶段。F2 business receipt reconciliation 不能消费这些阶段。 */
private[mysql] enum MySqlInactiveDomainWorkflowPhase:
  case PreparedForReserve
  case ReserveCommitUnknown
  case ReserveCommittedAwaitingReadback
  case ReserveCommittedReadbackUnknown
  case ReserveCommittedExecutePending
  case ExecuteFailedAfterReserve
  case ExecuteCommitUnknown
  case ExecuteCommittedAwaitingReadback
  case ExecuteCommittedReadbackUnknown

private[mysql] final class MySqlInactiveDomainWorkflowBlockedException(
    val phase: MySqlInactiveDomainWorkflowPhase,
    cause: Throwable
) extends SQLException("format3 owned inactive domain workflow remains responsible at " + phase.toString, cause)

/**
 * 保留 Main 原命令、admission 和两阶段 full20 计划的同一引用。
 * 此对象只表示待履行/未知责任，不自行证明任何 SQL 已提交或已读回。
 */
private[mysql] final class MySqlInactiveDomainWorkflowResponsibility private[mysql] (
    val commandScope: MySqlRepositoryCommandScope,
    val operation: MySqlRepositoryOperation,
    val materialization: MySqlInactiveDomainWorkflowMaterialization,
    val admission: MySqlBusinessAdmission,
    val phase: MySqlInactiveDomainWorkflowPhase,
    val blockedFailure: Option[MySqlInactiveDomainWorkflowBlockedException]
):
  require(commandScope != null && operation != null && materialization != null && admission != null && phase != null &&
    blockedFailure != null && !blockedFailure.exists(_ == null), "complete F3 inactive domain workflow responsibility is required")
  require(commandScope.hasOperationPlan(), "F3 inactive responsibility requires the exact Main-retained operation plan")
  require(commandScope.operationPlan() eq materialization.exactPlan,
    "F3 inactive responsibility must retain the plan used to create its full20 images")
  require(materialization.exactOperation == operation,
    "F3 inactive responsibility operation differs from its immutable materialization")
  require(operation.isInstanceOf[MySqlRepositoryOperation.UpdateInactiveDomain],
    "F3 inactive responsibility is limited to the planned inactive-domain operation")
  require(admission.token.scope eq commandScope.exactAuditScope(),
    "F3 inactive responsibility admission differs from its exact command scope")
  require((blockedFailure.nonEmpty) == (phase == MySqlInactiveDomainWorkflowPhase.ReserveCommitUnknown ||
    phase == MySqlInactiveDomainWorkflowPhase.ReserveCommittedAwaitingReadback ||
    phase == MySqlInactiveDomainWorkflowPhase.ReserveCommittedReadbackUnknown ||
    phase == MySqlInactiveDomainWorkflowPhase.ExecuteCommitUnknown ||
    phase == MySqlInactiveDomainWorkflowPhase.ExecuteCommittedAwaitingReadback ||
    phase == MySqlInactiveDomainWorkflowPhase.ExecuteCommittedReadbackUnknown ||
    phase == MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending ||
    phase == MySqlInactiveDomainWorkflowPhase.ExecuteFailedAfterReserve),
    "F3 blocked phases must retain an explicit failure or obligation")

  private[mysql] def sameWorkflow(scope: MySqlRepositoryCommandScope,
      currentOperation: MySqlRepositoryOperation, currentMaterialization: MySqlInactiveDomainWorkflowMaterialization): Boolean =
    (commandScope eq scope) && (operation == currentOperation) && (materialization eq currentMaterialization)

private[mysql] object MySqlInactiveDomainWorkflowResponsibility:
  def issue(commandScope: MySqlRepositoryCommandScope, operation: MySqlRepositoryOperation,
      materialization: MySqlInactiveDomainWorkflowMaterialization, admission: MySqlBusinessAdmission,
      phase: MySqlInactiveDomainWorkflowPhase, primary: Option[Throwable]): MySqlInactiveDomainWorkflowResponsibility =
    if primary == null || primary.exists(_ == null) then
      throw new IllegalArgumentException("F3 responsibility failure option is required")
    val blocking = phase match
      case MySqlInactiveDomainWorkflowPhase.PreparedForReserve =>
        if primary.nonEmpty then throw new IllegalArgumentException("prepared inactive domain workflow cannot carry a failure")
        None
      case MySqlInactiveDomainWorkflowPhase.ReserveCommittedExecutePending =>
        Some(new MySqlInactiveDomainWorkflowBlockedException(phase, primary.getOrElse(
          new IllegalStateException("reserve was acknowledged; the fixed execute operation remains pending"))))
      case MySqlInactiveDomainWorkflowPhase.ReserveCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("reserve awaiting readback is not a reported failure")
        Some(new MySqlInactiveDomainWorkflowBlockedException(phase,
          new IllegalStateException("reserve commit was acknowledged; exact full20 readback remains required")))
      case MySqlInactiveDomainWorkflowPhase.ExecuteCommittedAwaitingReadback =>
        if primary.nonEmpty then throw new IllegalArgumentException("awaiting readback is not a reported failure")
        Some(new MySqlInactiveDomainWorkflowBlockedException(phase,
          new IllegalStateException("execute commit was acknowledged; exact full20 readback remains required")))
      case MySqlInactiveDomainWorkflowPhase.ReserveCommitUnknown | MySqlInactiveDomainWorkflowPhase.ReserveCommittedReadbackUnknown |
          MySqlInactiveDomainWorkflowPhase.ExecuteFailedAfterReserve | MySqlInactiveDomainWorkflowPhase.ExecuteCommitUnknown |
          MySqlInactiveDomainWorkflowPhase.ExecuteCommittedReadbackUnknown =>
        val cause = primary.getOrElse(throw new IllegalArgumentException("unknown/unverified phase requires its original cause"))
        Some(new MySqlInactiveDomainWorkflowBlockedException(phase, cause))
    new MySqlInactiveDomainWorkflowResponsibility(commandScope, operation, materialization, admission, phase, blocking)

/** 已收到 reserve ACK 后的 execute 边界；已进入 commit/readback 未知态时原样保留未知责任。 */
private[mysql] object MySqlInactiveDomainExecuteFailureBoundary:
  def shouldRecordKnownFailure(retainedExecutePending: Boolean, primary: Throwable): Boolean =
    if !retainedExecutePending || primary == null || primary.isInstanceOf[MySqlTransactionCleanupUncertainException] then false
    else primary match
      case blocked: MySqlInactiveDomainWorkflowBlockedException =>
        blocked.phase != MySqlInactiveDomainWorkflowPhase.ReserveCommitUnknown &&
          blocked.phase != MySqlInactiveDomainWorkflowPhase.ReserveCommittedReadbackUnknown &&
          blocked.phase != MySqlInactiveDomainWorkflowPhase.ExecuteCommitUnknown &&
          blocked.phase != MySqlInactiveDomainWorkflowPhase.ExecuteCommittedReadbackUnknown
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