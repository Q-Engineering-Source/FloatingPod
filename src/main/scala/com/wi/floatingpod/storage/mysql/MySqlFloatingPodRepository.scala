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

import java.util.UUID
import scala.util.{Failure, Success, Try}

/** 仅由 exact Main-retained command scope 调用的 F3 owned-mode 路由。 */
private[mysql] trait MySqlOwnedModePlannedRoute:
  private[mysql] def executeOwnedMode(
      commandScope: MySqlRepositoryCommandScope,
      request: MySqlOwnedModeCommitRequest,
      activeLimit: Int
  ): Try[MySqlOwnedModeExecution]

private[mysql] trait MySqlControlledDebugPlannedRoute:
  private[mysql] def executeControlledDebug(scope: MySqlRepositoryCommandScope,
      request: MySqlControlledDebugPrepareRequest, prepared: Option[MySqlControlledDebugPreparedGraph],
      activeLimit: Int): Try[MySqlControlledDebugExecution]
private[mysql] trait MySqlTrackedDriftPlannedRoute:
  private[mysql] def executeTrackedDrift(scope: MySqlRepositoryCommandScope,
      request: MySqlTrackedDriftRequest, activeLimit: Int): Try[MySqlTrackedDriftExecution]

private[mysql] trait MySqlInactiveDomainPlannedRoute:
  private[mysql] def executeInactiveDomainUpdate(scope: MySqlRepositoryCommandScope,
      request: MySqlInactiveDomainUpdateRequest): Try[MySqlInactiveDomainUpdateOutcome]

/** 持有一条经验证的 MySQL 连接，并将所有业务写入交给同连接 fenced snapshot transaction。 */
final class MySqlFloatingPodRepository private[mysql] (
    expectation: MySqlSchemaAuditExpectation,
    coordinator: MySqlWriterFenceCoordinator,
    verified: VerifiedMySqlConnection,
    scope: MySqlAuditBootScope,
    sharedAllocationLedger: Option[MySqlFormat3AllocationLedger]
) extends FloatingPodRepository
    with ActivityTransitionStore
    with PlayerEntitlementStore
    with RecoveryPodStore
    with PhysicalRecoveryStore
    with MySqlControlledDebugPlannedRoute
    with MySqlNormalRecallPlannedRoute
    with MySqlOwnedModePlannedRoute
    with MySqlTrackedDriftPlannedRoute
    with MySqlInactiveDomainPlannedRoute:
  def this(expectation: MySqlSchemaAuditExpectation, coordinator: MySqlWriterFenceCoordinator,
      verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope) =
    this(expectation, coordinator, verified, scope, None)

  require(sharedAllocationLedger != null && !sharedAllocationLedger.exists(_ == null), "optional shared allocation ledger required")
  require(expectation != null && coordinator != null && verified != null && scope != null,
    "MySQL repository dependencies are required")
  require(verified.connection != null && verified.identity != null, "verified MySQL connection is incomplete")
  require(scope.ownerThread eq Thread.currentThread(), "MySQL repository must be created on its owned IO thread")

  private def queryOnlyLegacy[A](body: MySqlRepositoryStaging => Try[A]): Try[A] =
    if expectation.profile == MySqlSchemaAuditProfile.Format3 then
      Failure(new IllegalStateException("format3 reads require the owned fixed-query full20 route"))
    else run(body)

  private val ownerThread = Thread.currentThread()
  private val transactionRunner = new MySqlFencedTransaction(expectation, coordinator, verified, scope, sharedAllocationLedger)
  private val stagedOnThread = new ThreadLocal[MySqlRepositoryStaging]()
  private var closed = false

  override def insert(state: FloatingPodState): Try[FloatingPodState] = run(_.insert(state))
  override def update(state: FloatingPodState): Try[FloatingPodState] =
    if expectation.profile == MySqlSchemaAuditProfile.Format3 then
      Failure(new UnsupportedOperationException("format3 inactive update requires original scoped exact CAS request"))
    else run(_.update(state))
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = queryOnlyLegacy(_.findByUuid(uuid))
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = queryOnlyLegacy(_.findByOwner(ownerUuid))
  override def findAllActive(): Try[List[FloatingPodState]] = queryOnlyLegacy(_.findAllActive())
  override def findAll(): Try[List[FloatingPodState]] = queryOnlyLegacy(_.findAll())
  override def delete(uuid: UUID): Try[Boolean] = run(_.delete(uuid))

  /** 嵌套 transaction 在取得任何新 SQL 前被 runner 拒绝并 sticky 标记外层 rollback-only。 */
  override def transaction[T](fn: => T): Try[T] =
    if !onOwnerThread then Failure(new IllegalStateException("MySQL repository transaction requires its owned IO thread"))
    else if isClosed then Failure(new IllegalStateException("MySQL repository is closed"))
    else if expectation.profile == MySqlSchemaAuditProfile.Format3 then
      Failure(new UnsupportedOperationException("format3 mutations require their original scoped purpose route"))
    else if stagedOnThread.get() != null then
      transactionRunner.executeStaged(_ => Try(fn))
    else
      transactionRunner.executeStaged { stage =>
        stagedOnThread.set(stage)
        try Try(fn)
        finally stagedOnThread.remove()
      }

  override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] = queryOnlyLegacy(_.findByPlayerUuid(playerUuid))

  override def findDesiredActiveRows(): Try[List[StoredPodRow]] = queryOnlyLegacy(_.findDesiredActiveRows())
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = queryOnlyLegacy(_.findStoredByUuid(uuid))
  override def findAllRecoverable(): Try[List[FloatingPodState]] = queryOnlyLegacy(_.findAllRecoverable())

  override def backfillMissingActiveDimension(
      uuid: UUID,
      expectedStateVersion: Long,
      authoritativeDimensionId: Int
  ): Try[RecoveryDimensionBackfillOutcome] = run(_.backfillMissingActiveDimension(uuid, expectedStateVersion, authoritativeDimensionId))

  override def activateIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): Try[AdmissionOutcome] = run(_.activatePreparedUsingStoredAttempt(
    target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit))

  override def deactivateIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Try[DeactivationOutcome] = run(_.deactivateIfExpectedActive(podUuid, expectedOwner, expectedStateVersion))

  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = queryOnlyLegacy(_.findPhysicalRecoveryBinding())

  /** 普通 repository constructor 不接受世界/marker authority，因此拒绝无证据的 Legacy bootstrap。 */
  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] =
    Failure(new IllegalStateException("MySQL bootstrap requires an opaque fresh-provisioning proof and bootstrap-only admission"))

  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] =
    Failure(new IllegalStateException("MySQL binding certification requires opaque world-custody and durable marker acknowledgement proofs"))

  override def prepareObservedUnbound(
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessProof: ObservedUnboundUniquenessReceipt
  ): Try[UnboundPreparationReceipt] = run(_.prepareObservedUnbound(observation, expectedBinding, attemptUuid, uniquenessProof))

  override def commitObservedUnbound(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] = run(_.commitObservedUnbound(
    expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, globalActiveLimit))

  override def prepareUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): Try[UnboundPreparationReceipt] = run(_.prepareUnboundCreate(
    expectedBinding, podUuid, attemptUuid, targetPose, operationUuid))

  override def commitUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] = run(_.commitUnboundCreate(
    expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit))

  override def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] = queryOnlyLegacy(_.findUnboundPreparation(podUuid))

  override def prepareCreateLocator(
      podUuid: UUID,
      expectedDomainVersion: Long,
      attemptUuid: UUID,
      targetDimensionId: Int,
      chunks: Set[PhysicalRecoveryChunk]
  ): Try[PhysicalRecoveryLocator] = run(_.prepareCreateLocator(
    podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks))

  override def activatePreparedIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      attemptUuid: UUID
  ): Try[AdmissionOutcome] = run(_.activatePreparedIfAdmissible(
    target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid))

  override def deactivateTrackedIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[DeactivationOutcome] = run(_.deactivateTrackedIfExpectedActive(
    podUuid, expectedOwner, expectedStateVersion, attemptUuid))

  override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] =
    run(_.updateTrackedActiveDrift(state, expectedAttemptUuid))

  override def setOperatingModeTrackedIfOwned(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): Try[OperatingModeChangeOutcome] = run(_.setOperatingModeTrackedIfOwned(
    expectedBinding, podUuid, authenticatedOwner, expectedStateVersion, attemptUuid, targetMode, actualPose))

  override private[mysql] def executeNormalRecall(commandScope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation, prepared: Option[MySqlFencedTransaction.NormalRecallCommitted]
  ): Try[MySqlFencedTransaction.NormalRecallCommitted] =
    if !onOwnerThread || isClosed || stagedOnThread.get() != null then
      Failure(new IllegalStateException("normal recall requires its idle live owned repository"))
    else if commandScope == null || operation == null || prepared == null then
      Failure(new IllegalArgumentException("normal recall requires original scope, operation and optional P"))
    else
      try
        commandScope.requireCurrent(scope, commandScope.operationPlan())
        transactionRunner.executeNormalRecallWorkflow(commandScope, operation, prepared)
      catch case primary: Throwable => Failure(primary)

  private[mysql] def hasRetainedNormalRecallProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Boolean =
    if !onOwnerThread || isClosed then throw new IllegalStateException("recall retention requires its live owned repository")
    transactionRunner.hasRetainedNormalRecallProjection(tag)

  private[mysql] def releaseRetainedNormalRecallProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    requireNormalRecallProjectionRelease(tag)
    transactionRunner.releaseRetainedNormalRecallProjection(tag)

  private[mysql] def requireNormalRecallProjectionRelease(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    if !onOwnerThread || isClosed || stagedOnThread.get() != null then
      throw new IllegalStateException("recall release requires its idle live owned repository")
    if tag == null || tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration then
      throw new IllegalArgumentException("recall release tag differs from exact session")
    transactionRunner.requireNormalRecallProjectionRelease(tag)

  override private[mysql] def executeControlledDebug(commandScope: MySqlRepositoryCommandScope,
      request: MySqlControlledDebugPrepareRequest, prepared: Option[MySqlControlledDebugPreparedGraph],
      activeLimit: Int): Try[MySqlControlledDebugExecution] =
    if !onOwnerThread then Failure(new IllegalStateException("controlled debug route requires its owned IO thread"))
    else if isClosed then Failure(new IllegalStateException("MySQL repository is closed"))
    else if commandScope == null || request == null || prepared == null || activeLimit < 1 || activeLimit > 256 then
      Failure(new IllegalArgumentException("exact controlled debug scope and request required"))
    else
      try
        commandScope.requireCurrent(scope, commandScope.operationPlan())
        transactionRunner.executeControlledDebugWorkflow(commandScope, request, prepared, activeLimit)
      catch case primary: Throwable => Failure(primary)

  override private[mysql] def executeInactiveDomainUpdate(
      commandScope: MySqlRepositoryCommandScope,
      request: MySqlInactiveDomainUpdateRequest
  ): Try[MySqlInactiveDomainUpdateOutcome] =
    if !onOwnerThread || isClosed || stagedOnThread.get() != null then
      Failure(new IllegalStateException("inactive update requires its idle live owned repository"))
    else if commandScope == null || request == null then
      Failure(new IllegalArgumentException("original command scope and exact inactive request required"))
    else
      try
        commandScope.requireCurrent(scope, commandScope.operationPlan())
        transactionRunner.executeInactiveDomainWorkflow(commandScope, request)
      catch case primary: Throwable => Failure(primary)
  private[mysql] def hasRetainedInactiveDomainProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Boolean =
    if !onOwnerThread || isClosed then throw new IllegalStateException("inactive retention requires its live owned repository")
    transactionRunner.hasRetainedInactiveDomainProjection(tag)

  private[mysql] def releaseRetainedInactiveDomainProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    if !onOwnerThread || isClosed || stagedOnThread.get() != null then
      throw new IllegalStateException("inactive release requires its idle live owned repository")
    if tag == null || tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration then
      throw new IllegalArgumentException("inactive release tag differs from exact session")
    transactionRunner.releaseRetainedInactiveDomainProjection(tag)

  private[mysql] def hasRetainedControlledDebugProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Boolean =
    if !onOwnerThread || isClosed then throw new IllegalStateException("controlled retention requires its live owned repository")
    transactionRunner.hasRetainedControlledDebugProjection(tag)

  private[mysql] def releaseRetainedControlledDebugProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    if !onOwnerThread || isClosed || stagedOnThread.get() != null then
      throw new IllegalStateException("controlled release requires its idle live owned repository")
    if tag == null || tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration then
      throw new IllegalArgumentException("controlled release tag differs from exact session")
    transactionRunner.releaseRetainedControlledDebugProjection(tag)

  /** Main mode command保留原tag/plan；此route只运行F3两阶段事务，不进入F2 random-operation runner。 */
  override private[mysql] def executeOwnedMode(
      commandScope: MySqlRepositoryCommandScope,
      request: MySqlOwnedModeCommitRequest,
      activeLimit: Int
  ): Try[MySqlOwnedModeExecution] =
    if !onOwnerThread then Failure(new IllegalStateException("MySQL owned mode route requires its owned IO thread"))
    else if isClosed then Failure(new IllegalStateException("MySQL repository is closed"))
    else if commandScope == null || request == null || activeLimit <= 0 || activeLimit > 256 then
      Failure(new IllegalArgumentException("exact owned mode scope/request and bounded active limit are required"))
    else
      try
        val plan = commandScope.operationPlan()
        commandScope.requireCurrent(scope, plan)
        if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
          expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
          throw new java.sql.SQLNonTransientConnectionException(
            "planned owned-mode command requires the verified format3/physical2 repository")
        val profile = expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new java.sql.SQLNonTransientConnectionException("format3 repository has no exact source limit profile"))
        plan.requireSingle(request.repositoryOperation, profile)
        transactionRunner.planModeWorkflow(commandScope, request.repositoryOperation).flatMap {
          case MySqlModeWorkflowPlanning.Rejected(outcome) =>
            Success(MySqlOwnedModeExecution.Rejected(outcome))
          case MySqlModeWorkflowPlanning.AlreadySet(readback) =>
            Success(MySqlOwnedModeExecution.AlreadySet(readback))
          case MySqlModeWorkflowPlanning.Execute(materialization) =>
            transactionRunner.executeModeWorkflow(commandScope, request.repositoryOperation, materialization, activeLimit)
        }
      catch
        case failure: Throwable => Failure(failure)

  override private[mysql] def executeTrackedDrift(
      commandScope: MySqlRepositoryCommandScope,
      request: MySqlTrackedDriftRequest,
      activeLimit: Int
  ): Try[MySqlTrackedDriftExecution] =
    if !onOwnerThread then Failure(new IllegalStateException("MySQL owned mode route requires its owned IO thread"))
    else if isClosed then Failure(new IllegalStateException("MySQL repository is closed"))
    else if commandScope == null || request == null || activeLimit <= 0 || activeLimit > 256 then
      Failure(new IllegalArgumentException("exact owned mode scope/request and bounded active limit are required"))
    else
      try
        val plan = commandScope.operationPlan()
        commandScope.requireCurrent(scope, plan)
        if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
          expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
          throw new java.sql.SQLNonTransientConnectionException(
            "planned owned-mode command requires the verified format3/physical2 repository")
        val profile = expectation.format3Inputs.map(_.limitProfile).getOrElse(
          throw new java.sql.SQLNonTransientConnectionException("format3 repository has no exact source limit profile"))
        plan.requireSingle(request.repositoryOperation, profile)
        transactionRunner.planDriftWorkflow(commandScope, request.repositoryOperation).flatMap {
          case MySqlDriftWorkflowPlanning.Rejected(outcome) =>
            Success(MySqlTrackedDriftExecution.Rejected(outcome))
          case MySqlDriftWorkflowPlanning.Execute(materialization) =>
            transactionRunner.executeDriftWorkflow(commandScope, request.repositoryOperation, materialization, activeLimit)
        }
      catch
        case failure: Throwable => Failure(failure)

  /** Exact Main acknowledgement releases only retained in-memory F3 projection accounting; it performs no SQL or physical action. */
  private[mysql] def releaseRetainedModeProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    if !onOwnerThread then throw new IllegalStateException("mode projection release requires its owned IO thread")
    if isClosed then throw new IllegalStateException("MySQL repository is closed")
    if tag == null || tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration then
      throw new IllegalArgumentException("mode projection release tag differs from this exact repository session")
    if stagedOnThread.get() != null then
      throw new IllegalStateException("mode projection release cannot run inside a database transaction")
    transactionRunner.releaseRetainedModeProjection(tag)

  private[mysql] def releaseRetainedDriftProjection(tag: com.wi.floatingpod.server.io.IoOperationTag): Unit =
    if !onOwnerThread then throw new IllegalStateException("mode projection release requires its owned IO thread")
    if isClosed then throw new IllegalStateException("MySQL repository is closed")
    if tag == null || tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration then
      throw new IllegalArgumentException("mode projection release tag differs from this exact repository session")
    if stagedOnThread.get() != null then
      throw new IllegalStateException("mode projection release cannot run inside a database transaction")
    transactionRunner.releaseRetainedDriftProjection(tag)

  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] =
    queryOnlyLegacy(_.findPhysicalRecoveryLocator(podUuid))

  override def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] = queryOnlyLegacy(_.findAllPhysicalRecoveryLocators())

  override def verifyInactiveAbsence(
      podUuid: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[PhysicalRecoveryLocator] = run(_.verifyInactiveAbsence(podUuid, expectedStateVersion, attemptUuid))

  override def close(): Unit =
    if !onOwnerThread then throw new IllegalStateException("MySQL repository close requires its owned IO thread")
    if !isClosed then
      val active = stagedOnThread.get()
      if active != null then
        val failure = new IllegalStateException("MySQL repository cannot close inside an active staged transaction")
        active.observe("close-during-transaction")(_ => throw failure)
        throw failure
      closed = true
      verified.close()

  private def run[A](body: MySqlRepositoryStaging => Try[A]): Try[A] =
    if !onOwnerThread then Failure(new IllegalStateException("MySQL repository operation requires its owned IO thread"))
    else if isClosed then Failure(new IllegalStateException("MySQL repository is closed"))
    else
      val active = stagedOnThread.get()
      if active != null then body(active)
      else transactionRunner.executeStaged(body)

  private def onOwnerThread: Boolean = Thread.currentThread() eq ownerThread
  private def isClosed: Boolean = synchronized(closed)
