package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
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

/** 每次 borrow 的独立 generation 能力；失败立即通知真实 session，callback Success 不能擦掉它。 */
private[mysql] final class MySqlOwnedRepositoryBorrow(
    owner: Thread, val generation: Long, isCurrent: () => Boolean,
    sharedFailure: () => Option[Throwable], onFailure: Throwable => Unit,
    commandTag: Option[IoOperationTag] = None,
    auditScope: Option[MySqlAuditBootScope] = None,
    heldSessionBarrier: Option[HeldSessionBarrier] = None,
    businessAdmission: Option[MySqlBusinessAdmission] = None
):
  require(owner != null && generation > 0L && isCurrent != null && sharedFailure != null && onFailure != null &&
    commandTag != null && auditScope != null && heldSessionBarrier != null && businessAdmission != null &&
    !heldSessionBarrier.exists(_ == null) && !businessAdmission.exists(_ == null) && commandTag.isDefined == auditScope.isDefined &&
    heldSessionBarrier.isDefined == businessAdmission.isDefined &&
    (heldSessionBarrier.isEmpty || commandTag.isDefined),
    "exact borrow dependencies required")
  private var active = true
  private var primary = Option.empty[Throwable]
  private var issuedCommandScope = Option.empty[MySqlRepositoryCommandScope]

  private def requireOwner(): Unit =
    if Thread.currentThread() ne owner then throw new IllegalStateException("repository view requires exact IO worker")

  private def record(cause: Throwable): Unit =
    primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
    onFailure(cause)

  private def requireActive(): Unit =
    requireOwner()
    if !active || !isCurrent() then
      val cause = new IllegalStateException("repository borrow generation is revoked")
      record(cause)
      throw cause

  def invoke[A](body: => Try[A]): Try[A] =
    requireActive()
    primary.orElse(sharedFailure()) match
      case Some(cause) => Failure(cause)
      case None =>
        try
          val result = body
          if result == null then throw new IllegalStateException("delegate returned null Try")
          result match
            case Failure(cause) => record(cause)
            case Success(_) => ()
          primary.orElse(sharedFailure()).map(Failure(_)).getOrElse(result)
        catch
          case cause: Throwable => record(cause); throw cause

  /** 只把 Main 已提交给 owned session 的原始 tag 以词法参数传给本次调用。 */
  def invokeCommand[A](body: MySqlRepositoryCommandScope => Try[A]): Try[A] =
    invokeCommand(None, body)

  /** Main 在入队时保留的不可变计划随原 tag 进入同一 borrow；worker 不 mint operation ID。 */
  def invokePlannedCommand[A](plan: MySqlOwnedRepositoryOperationPlan,
      body: MySqlRepositoryCommandScope => Try[A]): Try[A] =
    if plan == null then
      requireActive()
      val cause = new IllegalArgumentException("Main-retained repository operation plan is required")
      record(cause)
      Failure(cause)
    else invokeCommand(Some(plan), body)

  private def invokeCommand[A](plan: Option[MySqlOwnedRepositoryOperationPlan],
      body: MySqlRepositoryCommandScope => Try[A]): Try[A] =
    requireActive()
    if body == null then
      val cause = new IllegalArgumentException("owned repository command body is required")
      record(cause)
      Failure(cause)
    else invoke {
      val commandScope = issueCommandScope(plan)
      body(commandScope)
    }

  def throwIfFailed(): Unit =
    requireActive()
    primary.orElse(sharedFailure()).foreach(cause => throw cause)

  def complete[A](result: Try[A]): Try[A] =
    requireActive()
    if result == null then
      val cause = new IllegalStateException("borrow callback returned null Try")
      record(cause)
      throw cause
    result match
      case Failure(cause) => record(cause)
      case Success(_) => ()
    primary.orElse(sharedFailure()).map(Failure(_)).getOrElse(result)

  def closeDenied(): Unit =
    requireActive()
    val cause = new IllegalStateException("borrower cannot close owned repository resources")
    record(cause)
    throw cause

  def revoke(): Unit =
    requireOwner()
    active = false

  /** Java opaque receiver rechecks reference identity against the borrow's cached issuer value. */
  private[mysql] def requireIssuedCommandScope(candidate: MySqlRepositoryCommandScope): Unit =
    requireActive()
    if candidate == null || !issuedCommandScope.exists(_ eq candidate) then
      val cause = new IllegalStateException("repository command scope was not issued by this active borrow")
      record(cause)
      throw cause
    primary.orElse(sharedFailure()).foreach(cause => throw cause)

  private def issueCommandScope(plan: Option[MySqlOwnedRepositoryOperationPlan]): MySqlRepositoryCommandScope =
    requireActive()
    val tag = commandTag.getOrElse {
      throw new IllegalStateException("a mutating repository call requires the exact owned IO operation tag")
    }
    val scope = auditScope.getOrElse {
      throw new IllegalStateException("owned operation tag lacks its verified database scope")
    }
    if tag.bootId != scope.bootUuid || tag.bootGeneration != scope.bootGeneration ||
      tag.connectionGeneration != scope.connectionGeneration || tag.operationId == null then
      throw new IllegalStateException("owned operation tag differs from the exact database session generation")
    plan.foreach(_.requireTag(tag))
    issuedCommandScope match
      case Some(existing) =>
        if plan.exists(value => !existing.hasOperationPlan() || (existing.operationPlan() ne value)) then
          throw new IllegalStateException("a borrow cannot replace its cached Main operation plan")
        existing.requireCurrent(scope)
        existing
      case None =>
        val issued = MySqlRepositoryCommandScope.issue(this, tag, generation, scope,
          heldSessionBarrier.orNull, businessAdmission.orNull, plan.orNull, owner)
        issuedCommandScope = Some(issued)
        issued

/** 可逃逸对象的能力在 finally 撤销；不返回 raw repository/connection/coordinator。 */
final class MySqlOwnedRepositoryView private[mysql] (
    private val delegate: FloatingPodRepository & ActivityTransitionStore & PlayerEntitlementStore & RecoveryPodStore & PhysicalRecoveryStore,
    private val borrow: MySqlOwnedRepositoryBorrow,
    private val query: Option[MySqlFormat3QueryProjection] = None
) extends FloatingPodRepository
    with ActivityTransitionStore
    with PlayerEntitlementStore
    with RecoveryPodStore
    with PhysicalRecoveryStore:
  require(delegate != null && borrow != null, "actual repository and active borrow required")

  /** 由 owned session 在每个 Main command 的 borrow 内词法绑定一次精确 operation scope。 */
  private[mysql] def withCommandScope[A](body: MySqlRepositoryCommandScope => Try[A]): Try[A] =
    borrow.invokeCommand(body)

  /** 只接受 Main 入队前已保存的同一计划对象；operation DTO 会在 SQL 入口重编码比对。 */
  private[mysql] def withCommandScope[A](plan: MySqlOwnedRepositoryOperationPlan)
      (body: MySqlRepositoryCommandScope => Try[A]): Try[A] =
    borrow.invokePlannedCommand(plan, body)

  private[mysql] def normalRecall(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, prepared: Option[MySqlFencedTransaction.NormalRecallCommitted]
  ): Try[MySqlFencedTransaction.NormalRecallCommitted] =
    borrow.invokePlannedCommand(plan, commandScope => delegate match
      case route: MySqlNormalRecallPlannedRoute => route.executeNormalRecall(commandScope, operation, prepared)
      case _ => Failure(new UnsupportedOperationException("owned F3 Normal Recall route required; legacy fallback forbidden")))

  private[mysql] def controlledDebug(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlControlledDebugPrepareRequest, prepared: Option[MySqlControlledDebugPreparedGraph],
      activeLimit: Int): Try[MySqlControlledDebugExecution] =
    borrow.invokePlannedCommand(plan, commandScope =>
      delegate match
        case route: MySqlControlledDebugPlannedRoute => route.executeControlledDebug(commandScope, request, prepared, activeLimit)
        case _ => Failure(new UnsupportedOperationException("planned F3 controlled debug route required; F2 fallback forbidden")))
  /** Mode CAS 只进入 repository 的 exact Main-plan F3 route。 */
  private[mysql] def commitOwnedMode(
      plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlOwnedModeCommitRequest,
      activeLimit: Int
  ): Try[MySqlOwnedModeExecution] =
    borrow.invokePlannedCommand(plan, commandScope =>
      delegate match
        case route: MySqlOwnedModePlannedRoute => route.executeOwnedMode(commandScope, request, activeLimit)
        case _ => Failure(new UnsupportedOperationException(
          "the owned repository has no planned format3 mode route; legacy mode dispatch is forbidden")))

  private[mysql] def commitTrackedDrift(
      plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlTrackedDriftRequest,
      activeLimit: Int
  ): Try[MySqlTrackedDriftExecution] =
    borrow.invokePlannedCommand(plan, commandScope =>
      delegate match
        case route: MySqlTrackedDriftPlannedRoute => route.executeTrackedDrift(commandScope, request, activeLimit)
        case _ => Failure(new UnsupportedOperationException(
          "the owned repository has no planned format3 mode route; legacy mode dispatch is forbidden")))

  override def insert(state: FloatingPodState): Try[FloatingPodState] = borrow.invoke(delegate.insert(state))
  private[mysql] def updateInactiveDomain(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlInactiveDomainUpdateRequest): Try[MySqlInactiveDomainUpdateOutcome] =
    borrow.invokePlannedCommand(plan, scope => delegate match
      case route: MySqlInactiveDomainPlannedRoute => route.executeInactiveDomainUpdate(scope,request)
      case _ => Failure(new UnsupportedOperationException("actual F3 inactive planned route is missing")))
  override def update(state: FloatingPodState): Try[FloatingPodState] = borrow.invoke(delegate.update(state))
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findByUuid(uuid))
    case None => delegate.findByUuid(uuid))
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findByOwner(ownerUuid))
    case None => delegate.findByOwner(ownerUuid))
  override def findAllActive(): Try[List[FloatingPodState]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findAllActive())
    case None => delegate.findAllActive())
  override def findAll(): Try[List[FloatingPodState]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findAll())
    case None => delegate.findAll())
  override def delete(uuid: UUID): Try[Boolean] = borrow.invoke(delegate.delete(uuid))
  override def transaction[T](fn: => T): Try[T] =
    borrow.invoke(delegate.transaction {
      val value = fn
      borrow.throwIfFailed()
      value
    })
  override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findByPlayerUuid(playerUuid))
    case None => delegate.findByPlayerUuid(playerUuid))
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findDesiredActiveRows())
    case None => delegate.findDesiredActiveRows())
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findStoredByUuid(uuid))
    case None => delegate.findStoredByUuid(uuid))
  override def findAllRecoverable(): Try[List[FloatingPodState]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findAllRecoverable())
    case None => delegate.findAllRecoverable())
  override def backfillMissingActiveDimension(
      uuid: UUID,
      expectedStateVersion: Long,
      authoritativeDimensionId: Int
  ): Try[RecoveryDimensionBackfillOutcome] = borrow.invoke(delegate.backfillMissingActiveDimension(uuid, expectedStateVersion, authoritativeDimensionId))
  override def activateIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): Try[AdmissionOutcome] = borrow.invoke(delegate.activateIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit))
  override def deactivateIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Try[DeactivationOutcome] = borrow.invoke(delegate.deactivateIfExpectedActive(podUuid, expectedOwner, expectedStateVersion))
  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findPhysicalRecoveryBinding())
    case None => delegate.findPhysicalRecoveryBinding())
  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = borrow.invoke(delegate.createOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion))
  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = borrow.invoke(delegate.certifyPhysicalRecoveryBinding(expectedUuid, expectedFormatVersion))
  override def prepareObservedUnbound(
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessProof: ObservedUnboundUniquenessReceipt
  ): Try[UnboundPreparationReceipt] = borrow.invoke(delegate.prepareObservedUnbound(observation, expectedBinding, attemptUuid, uniquenessProof))
  override def commitObservedUnbound(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] = borrow.invoke(delegate.commitObservedUnbound(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, globalActiveLimit))
  override def prepareUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): Try[UnboundPreparationReceipt] = borrow.invoke(delegate.prepareUnboundCreate(expectedBinding, podUuid, attemptUuid, targetPose, operationUuid))
  override def commitUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] = borrow.invoke(delegate.commitUnboundCreate(expectedBinding, podUuid, attemptUuid, expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit))
  override def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findUnboundPreparation(podUuid))
    case None => delegate.findUnboundPreparation(podUuid))
  override def prepareCreateLocator(
      podUuid: UUID,
      expectedDomainVersion: Long,
      attemptUuid: UUID,
      targetDimensionId: Int,
      chunks: Set[PhysicalRecoveryChunk]
  ): Try[PhysicalRecoveryLocator] = borrow.invoke(delegate.prepareCreateLocator(podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks))
  override def activatePreparedIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      attemptUuid: UUID
  ): Try[AdmissionOutcome] = borrow.invoke(delegate.activatePreparedIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid))
  override def deactivateTrackedIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[DeactivationOutcome] = borrow.invoke(delegate.deactivateTrackedIfExpectedActive(podUuid, expectedOwner, expectedStateVersion, attemptUuid))
  override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] = borrow.invoke(delegate.updateTrackedActiveDrift(state, expectedAttemptUuid))
  override def setOperatingModeTrackedIfOwned(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): Try[OperatingModeChangeOutcome] = borrow.invoke(delegate.setOperatingModeTrackedIfOwned(expectedBinding, podUuid, authenticatedOwner, expectedStateVersion, attemptUuid, targetMode, actualPose))
  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findPhysicalRecoveryLocator(podUuid))
    case None => delegate.findPhysicalRecoveryLocator(podUuid))
  override def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] = borrow.invoke(query match
    case Some(projection) => Try(projection.findAllPhysicalRecoveryLocators())
    case None => delegate.findAllPhysicalRecoveryLocators())
  override def verifyInactiveAbsence(
      podUuid: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[PhysicalRecoveryLocator] = borrow.invoke(delegate.verifyInactiveAbsence(podUuid, expectedStateVersion, attemptUuid))
  override def close(): Unit = borrow.closeDenied()
