package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
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
  PhysicalRecoveryCutoverState,
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
import com.wi.floatingpod.server.io.IoOperationTag

import java.util.UUID
import scala.util.{Failure, Success, Try}

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Pure borrow boundary, controlled existing traits; no JDBC/open/admission integration. */
class MySqlOwnedRepositoryViewTest:
  private final class ControlledDelegate extends FloatingPodRepository
      with ActivityTransitionStore with PlayerEntitlementStore with RecoveryPodStore with PhysicalRecoveryStore
      with MySqlOwnedModePlannedRoute:
    var calls = 0
    var closes = 0
    var transactions = 0
    var rolledBack = false
    var plannedModeCalls = 0
    var plannedModeScope: MySqlRepositoryCommandScope = null
    var plannedModeRequest: MySqlOwnedModeCommitRequest = null
    var plannedModeActiveLimit = 0
    val original = new IllegalStateException("actual controlled trait Failure")
    val plannedModeFailure = new IllegalStateException("controlled planned F3 route failure")
    override def executeOwnedMode(
        commandScope: MySqlRepositoryCommandScope,
        request: MySqlOwnedModeCommitRequest,
        activeLimit: Int
    ): Try[MySqlOwnedModeExecution] =
      plannedModeCalls += 1
      plannedModeScope = commandScope
      plannedModeRequest = request
      plannedModeActiveLimit = activeLimit
      Failure(plannedModeFailure)
    override def insert(state: FloatingPodState): Try[FloatingPodState] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def update(state: FloatingPodState): Try[FloatingPodState] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findAllActive(): Try[List[FloatingPodState]] = { calls += 1; Success(Nil) }
    override def findAll(): Try[List[FloatingPodState]] = { calls += 1; Failure(original) }
    override def delete(uuid: UUID): Try[Boolean] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def transaction[T](fn: => T): Try[T] =
        transactions += 1
        try Success(fn)
        catch
          case cause: Throwable => rolledBack = true; Failure(cause)
    override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findDesiredActiveRows(): Try[List[StoredPodRow]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findAllRecoverable(): Try[List[FloatingPodState]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def backfillMissingActiveDimension(
        uuid: UUID,
        expectedStateVersion: Long,
        authoritativeDimensionId: Int
    ): Try[RecoveryDimensionBackfillOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def activateIfAdmissible(
        target: FloatingPodState,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        dimensionId: Int,
        globalActiveLimit: Int
    ): Try[AdmissionOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def deactivateIfExpectedActive(
        podUuid: UUID,
        expectedOwner: Option[UUID],
        expectedStateVersion: Long
    ): Try[DeactivationOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def prepareObservedUnbound(
        observation: ObservedUnboundPod,
        expectedBinding: PhysicalRecoveryBinding,
        attemptUuid: UUID,
        uniquenessProof: ObservedUnboundUniquenessReceipt
    ): Try[UnboundPreparationReceipt] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def commitObservedUnbound(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        expectedPreparedVersion: Long,
        actualPose: FloatingPodPose,
        globalActiveLimit: Int
    ): Try[UnboundCommitOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def prepareUnboundCreate(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        targetPose: FloatingPodPose,
        operationUuid: UUID
    ): Try[UnboundPreparationReceipt] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def commitUnboundCreate(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        expectedPreparedVersion: Long,
        actualPose: FloatingPodPose,
        operationUuid: UUID,
        globalActiveLimit: Int
    ): Try[UnboundCommitOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def prepareCreateLocator(
        podUuid: UUID,
        expectedDomainVersion: Long,
        attemptUuid: UUID,
        targetDimensionId: Int,
        chunks: Set[PhysicalRecoveryChunk]
    ): Try[PhysicalRecoveryLocator] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def activatePreparedIfAdmissible(
        target: FloatingPodState,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        dimensionId: Int,
        globalActiveLimit: Int,
        attemptUuid: UUID
    ): Try[AdmissionOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def deactivateTrackedIfExpectedActive(
        podUuid: UUID,
        expectedOwner: Option[UUID],
        expectedStateVersion: Long,
        attemptUuid: UUID
    ): Try[DeactivationOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def setOperatingModeTrackedIfOwned(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        attemptUuid: UUID,
        targetMode: PodOperatingMode,
        actualPose: FloatingPodPose
    ): Try[OperatingModeChangeOutcome] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def verifyInactiveAbsence(
        podUuid: UUID,
        expectedStateVersion: Long,
        attemptUuid: UUID
    ): Try[PhysicalRecoveryLocator] = throw new UnsupportedOperationException("outside borrow-boundary test scenario")
    override def close(): Unit = { closes += 1 }

  private final class Harness:
    val gate = new MySqlOwnedSessionLifecycle(Thread.currentThread())
    gate.beginOpen()
    gate.admit()
    val delegate = new ControlledDelegate
    var current: MySqlOwnedRepositoryBorrow = null
    var generation = 0L
    var onFailureCalls = 0
    var lastFailureCause: Throwable = null
    def open(
        commandTag: Option[IoOperationTag] = None,
        auditScope: Option[MySqlAuditBootScope] = None,
        heldSessionBarrier: Option[HeldSessionBarrier] = None,
        businessAdmission: Option[MySqlBusinessAdmission] = None
    ): (MySqlOwnedRepositoryBorrow, MySqlOwnedRepositoryView) =
      generation = Math.incrementExact(generation)
      var lease: MySqlOwnedRepositoryBorrow = null
      lease = new MySqlOwnedRepositoryBorrow(Thread.currentThread(), generation,
        () => current eq lease, () => gate.failure, cause => {
          onFailureCalls += 1
          lastFailureCause = cause
          gate.markFailed(cause)
        }, commandTag, auditScope, heldSessionBarrier, businessAdmission)
      current = lease
      (lease, new MySqlOwnedRepositoryView(delegate, lease))

  @Test def commandScopeCarriesTheExactMainTagAndIsRevokedWithItsBorrow(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val tag = IoOperationTag(boot, 4L, 9L, UUID.fromString("00000000-0000-0000-0000-000000000002"))
    val scope = auditScope(boot, tag.bootGeneration, tag.connectionGeneration)
    val h = new Harness
    val (lease, view) = h.open(Some(tag), Some(scope))
    var captured: MySqlRepositoryCommandScope = null
    var repeated: MySqlRepositoryCommandScope = null

    try
      val result = view.withCommandScope { commandScope =>
        captured = commandScope
        commandScope.requireCurrent(scope)
        Success(commandScope.tag)
      }
      val secondResult = view.withCommandScope { commandScope =>
        repeated = commandScope
        commandScope.requireCurrent(scope)
        Success(commandScope.tag)
      }
      assertEquals(Success(tag), result)
      assertEquals(Success(tag), secondResult)
      assertSame(tag, captured.tag)
      assertSame(captured, repeated)
      assertEquals(lease.generation, captured.borrowGeneration)
      assertTrue(h.gate.failure.isEmpty)
    finally lease.revoke()

    assertThrows(classOf[IllegalStateException], () => captured.requireCurrent(scope))

  @Test def commandScopeRejectsTagForAnotherDatabaseGenerationBeforeCallingBody(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000011")
    val scope = auditScope(boot, 4L, 9L)
    val tag = IoOperationTag(boot, 4L, 10L, UUID.fromString("00000000-0000-0000-0000-000000000012"))
    val h = new Harness
    val (lease, view) = h.open(Some(tag), Some(scope))
    var bodyCalled = false
    try
      val failure = assertThrows(classOf[IllegalStateException], () => {
        view.withCommandScope { _ =>
          bodyCalled = true
          Success(())
        }
        ()
      })
      assertFalse(bodyCalled)
      assertSame(failure, h.gate.failure.get)
    finally lease.revoke()

  @Test def commandScopeCannotBeMintedWithoutTheExactMainTag(): Unit =
    val h = new Harness
    val (lease, view) = h.open()
    var bodyCalled = false
    try
      val failure = assertThrows(classOf[IllegalStateException], () => {
        view.withCommandScope { _ =>
          bodyCalled = true
          Success(())
        }
        ()
      })
      assertFalse(bodyCalled)
      assertSame(failure, h.gate.failure.get)
    finally lease.revoke()

  @Test def commandScopeRejectsASecondSameBorrowObjectThatWasNotTheCachedIssuerValue(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000021")
    val operation = UUID.fromString("00000000-0000-0000-0000-000000000022")
    val tag = IoOperationTag(boot, 5L, 10L, operation)
    val scope = auditScope(boot, tag.bootGeneration, tag.connectionGeneration)
    val profile = SourceLimitProfile.of(4096L, 4096L, 4096L, 64L, 4096L, 65536L, 65536L, 4096L)
    val state = FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000023"), None, 0L,
      "scope-test", 1.0, 2.0, 3.0, 0.0f, 0.0f, false, 1L)
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag,
      UUID.fromString("00000000-0000-0000-0000-000000000024"),
      UUID.fromString("00000000-0000-0000-0000-000000000025"),
      MySqlRepositoryOperation.Insert(state), profile)
    val h = new Harness
    val (lease, view) = h.open(Some(tag), Some(scope))
    var issued: MySqlRepositoryCommandScope = null
    try
      assertTrue(view.withCommandScope(plan) { value => issued = value; Success(()) }.isSuccess)
      assertNotNull(issued)
      val forgedSibling = MySqlRepositoryCommandScope.issue(lease, tag, lease.generation, scope,
        null, null, plan, Thread.currentThread())
      val failure = assertThrows(classOf[IllegalStateException], () => {
        forgedSibling.requireCurrent(scope, plan)
        ()
      })
      assertSame(failure, h.gate.failure.get)
    finally lease.revoke()

  @Test def commandBorrowCannotReplaceItsCachedMainPlanWithAnEqualButDistinctPlan(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000031")
    val operation = UUID.fromString("00000000-0000-0000-0000-000000000032")
    val tag = IoOperationTag(boot, 5L, 11L, operation)
    val scope = auditScope(boot, tag.bootGeneration, tag.connectionGeneration)
    val profile = SourceLimitProfile.of(4096L, 4096L, 4096L, 64L, 4096L, 65536L, 65536L, 4096L)
    val state = FloatingPodState(UUID.fromString("00000000-0000-0000-0000-000000000033"), None, 0L,
      "plan-test", 1.0, 2.0, 3.0, 0.0f, 0.0f, false, 1L)
    val workflow = UUID.fromString("00000000-0000-0000-0000-000000000034")
    val reserve = UUID.fromString("00000000-0000-0000-0000-000000000035")
    val operationDto = MySqlRepositoryOperation.Insert(state)
    val first = MySqlOwnedRepositoryOperationPlan.single(tag, workflow, reserve, operationDto, profile)
    val equalButDistinct = MySqlOwnedRepositoryOperationPlan.single(tag, workflow, reserve, operationDto, profile)
    assertEquals(first, equalButDistinct)
    assertNotSame(first, equalButDistinct)
    val h = new Harness
    val (lease, view) = h.open(Some(tag), Some(scope))
    try
      assertTrue(view.withCommandScope(first)(_ => Success(())).isSuccess)
      val failure = assertThrows(classOf[IllegalStateException], () => {
        view.withCommandScope(equalButDistinct)(_ => Success(()))
        ()
      })
      assertSame(failure, h.gate.failure.get)
    finally lease.revoke()

  @Test def ownedModeUsesTheExactCachedMainPlanAndF3RouteFailureRemainsPrimary(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000041")
    val tag = IoOperationTag(boot, 8L, 12L, UUID.fromString("00000000-0000-0000-0000-000000000042"))
    val scope = auditScope(boot, tag.bootGeneration, tag.connectionGeneration)
    val profile = SourceLimitProfile.of(4096L, 4096L, 4096L, 64L, 4096L, 65536L, 65536L, 4096L)
    val binding = PhysicalRecoveryBinding(UUID.fromString("00000000-0000-0000-0000-000000000043"),
      PhysicalRecoveryCutoverState.Certified, 2)
    val request = MySqlOwnedModeCommitRequest(binding, UUID.fromString("00000000-0000-0000-0000-000000000044"),
      UUID.fromString("00000000-0000-0000-0000-000000000045"), 7L,
      UUID.fromString("00000000-0000-0000-0000-000000000046"), PodOperatingMode.WorldIdle,
      FloatingPodPose("owned-mode-route", 32D, 64D, 48D, 0F, 0F, 7))
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag,
      UUID.fromString("00000000-0000-0000-0000-000000000047"),
      UUID.fromString("00000000-0000-0000-0000-000000000048"), request.repositoryOperation, profile)
    val h = new Harness
    val (lease, view) = h.open(Some(tag), Some(scope))
    var firstScope: MySqlRepositoryCommandScope = null
    try
      assertTrue(view.withCommandScope(plan) { commandScope =>
        firstScope = commandScope
        Success(())
      }.isSuccess)
      assertSame(h.delegate.plannedModeFailure, view.commitOwnedMode(plan, request, 64).failed.get)
      assertEquals(1, h.delegate.plannedModeCalls)
      assertSame(firstScope, h.delegate.plannedModeScope)
      assertSame(plan, h.delegate.plannedModeScope.operationPlan())
      assertSame(request, h.delegate.plannedModeRequest)
      assertEquals(64, h.delegate.plannedModeActiveLimit)
      assertSame(h.delegate.plannedModeFailure, h.gate.failure.get)
      assertEquals(0, h.delegate.calls, "planned F3 mode route must not fall back to legacy traits")
    finally lease.revoke()

  @Test def swallowedRealDelegatedFailureOverridesCallbackSuccessAndBlocksRelease(): Unit =
    val h = new Harness
    val (lease, view) = h.open()
    try
      val operation = view.findAll()
      assertSame(h.delegate.original, operation.failed.get)
      val callbackResult = lease.complete(Success(42))
      assertSame(h.delegate.original, callbackResult.failed.get)
      assertSame(h.delegate.original, h.gate.failure.get)
      assertEquals(1, h.delegate.calls)
      assertSame(h.delegate.original, view.findAllActive().failed.get)
      assertEquals(1, h.delegate.calls, "failed borrow must not delegate again")
      h.gate.beginClose(None)
      assertFalse(h.gate.releaseEligible)
      h.gate.finishClose()
    finally lease.revoke()

  @Test def escapedViewIsRevokedBeforeAnyFurtherDelegateOrClose(): Unit =
    val h = new Harness
    val (lease, escaped) = h.open()
    assertEquals(Success(Nil), escaped.findAllActive())
    assertTrue(lease.complete(Success(escaped)).isSuccess) // object escape allowed; capability revoked next.
    lease.revoke()
    val failure = assertThrows(classOf[IllegalStateException], () => escaped.findAllActive())
    assertSame(failure, h.gate.failure.get)
    assertThrows(classOf[IllegalStateException], () => escaped.close())
    assertEquals(1, h.delegate.calls)
    assertEquals(0, h.delegate.closes)

  @Test def oldGenerationCannotUseNewBorrowAndSharedFailureStopsCurrentView(): Unit =
    val h = new Harness
    val (oldLease, oldView) = h.open()
    oldLease.revoke()
    val (newLease, current) = h.open()
    try
      val original = assertThrows(classOf[IllegalStateException], () => oldView.findAllActive())
      assertSame(original, current.findAllActive().failed.get)
      assertSame(original, newLease.complete(Success(7)).failed.get)
      assertEquals(0, h.delegate.calls)
    finally newLease.revoke()

  @Test def realTraitTransactionExecutesBodyAndSwallowedHelperFailureThrowsInsideIt(): Unit =
    val h = new Harness
    val (lease, view) = h.open()
    try
      val result = view.transaction {
        view.findAll() // deliberate caller swallowing of actual controlled delegate Failure.
        99
      }
      assertSame(h.delegate.original, result.failed.get)
      assertTrue(h.delegate.rolledBack, "controlled trait transaction must observe original thrown failure")
      assertEquals(1, h.delegate.transactions)
      assertEquals(1, h.delegate.calls)
      assertSame(h.delegate.original, lease.complete(Success(99)).failed.get)
    finally lease.revoke()

  @Test def borrowerCloseIsDeniedAndImmediatelyStickyWithoutDelegateClose(): Unit =
    val h = new Harness
    val (lease, view) = h.open()
    try
      val cause = assertThrows(classOf[IllegalStateException], () => view.close())
      assertSame(cause, h.gate.failure.get)
      assertSame(cause, lease.complete(Success(())).failed.get)
      assertEquals(0, h.delegate.closes)
    finally lease.revoke()

  @Test def wrongThreadCannotInvokeCurrentViewOrTouchDelegate(): Unit =
    val h = new Harness
    val (lease, view) = h.open()
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try
        assertThrows(classOf[IllegalStateException], () => view.findAllActive())
        assertThrows(classOf[IllegalStateException], () => view.close())
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "wrong-repository-borrow-actor")
    var primary: Throwable = null
    try
      wrong.start()
      wrong.join(5000)
      assertFalse(wrong.isAlive, "wrong actor join timeout")
      if failure.get() != null then throw failure.get()
      assertEquals(0, h.delegate.calls)
      assertEquals(0, h.delegate.closes)
      assertTrue(h.gate.failure.isEmpty)
    catch case cause: Throwable => primary = cause
    finally
      lease.revoke()
      try
        wrong.join(5000)
        assertFalse(wrong.isAlive, "wrong actor leaked")
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  @Test def wrongThreadNullPlanCannotPoisonTheBorrowButOwnerNullPlanIsSticky(): Unit =
    val h = new Harness
    val (lease, _) = h.open()
    val bodyCalls = new java.util.concurrent.atomic.AtomicInteger()
    val wrongThreadFailure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val wrongThread = new Thread(() => {
      try lease.invokePlannedCommand[Unit](null, _ => {
        bodyCalls.incrementAndGet()
        Success(())
      })
      catch case cause: Throwable => wrongThreadFailure.set(cause)
      ()
    }, "wrong-thread-null-plan")
    try
      wrongThread.start()
      wrongThread.join(5000)
      assertFalse(wrongThread.isAlive, "wrong-thread null-plan call did not terminate")
      assertTrue(wrongThreadFailure.get().isInstanceOf[IllegalStateException])
      assertTrue(h.gate.failure.isEmpty)
      assertEquals(0, h.onFailureCalls)
      assertEquals(0, bodyCalls.get())
      assertTrue(lease.complete(Success(())).isSuccess, "wrong-thread access must leave owner primary untouched")

      val ownerResult = lease.invokePlannedCommand[Unit](null, _ => {
        bodyCalls.incrementAndGet()
        Success(())
      })
      val ownerFailure = ownerResult.failed.get
      assertTrue(ownerFailure.isInstanceOf[IllegalArgumentException])
      assertEquals("Main-retained repository operation plan is required", ownerFailure.getMessage)
      assertSame(ownerFailure, h.gate.failure.get)
      assertSame(ownerFailure, h.lastFailureCause)
      assertEquals(1, h.onFailureCalls)
      assertEquals(0, bodyCalls.get())
      assertSame(ownerFailure, lease.complete(Success(())).failed.get)
    finally
      lease.revoke()
      if wrongThread.isAlive then
        wrongThread.interrupt()
        wrongThread.join(5000)

  @Test def revokedBorrowNullPlanPropagatesTheRevocationCauseBeforePlanValidation(): Unit =
    val h = new Harness
    val (lease, _) = h.open()
    val bodyCalls = new java.util.concurrent.atomic.AtomicInteger()
    lease.revoke()
    val failure = assertThrows(classOf[IllegalStateException], () => {
      lease.invokePlannedCommand[Unit](null, _ => {
        bodyCalls.incrementAndGet()
        Success(())
      })
      ()
    })
    assertEquals("repository borrow generation is revoked", failure.getMessage)
    assertSame(failure, h.gate.failure.get)
    assertSame(failure, h.lastFailureCause)
    assertEquals(1, h.onFailureCalls)
    assertEquals(0, bodyCalls.get())

  @Test def escapedCommandScopeCannotPassAfterPrimaryOrSharedBorrowFailure(): Unit =
    val boot = UUID.fromString("00000000-0000-0000-0000-000000000041")
    val tag = IoOperationTag(boot, 6L, 14L, UUID.fromString("00000000-0000-0000-0000-000000000042"))
    val scope = auditScope(boot, tag.bootGeneration, tag.connectionGeneration)
    val first = new Harness
    val (firstLease, firstView) = first.open(Some(tag), Some(scope))
    var issued: MySqlRepositoryCommandScope = null
    try
      assertTrue(firstView.withCommandScope { value => issued = value; Success(()) }.isSuccess)
      val primary = firstView.findAll().failed.get
      assertSame(first.delegate.original, primary)
      val rejected = assertThrows(classOf[IllegalStateException], () => {
        issued.requireCurrent(scope)
        ()
      })
      assertSame(primary, rejected)
      assertSame(primary, first.gate.failure.get)
      assertSame(primary, first.lastFailureCause)
      assertEquals(1, first.onFailureCalls)
      assertEquals(1, first.delegate.calls)
    finally firstLease.revoke()

    val second = new Harness
    val (secondLease, secondView) = second.open(Some(tag), Some(scope))
    var sharedScope: MySqlRepositoryCommandScope = null
    try
      assertTrue(secondView.withCommandScope { value => sharedScope = value; Success(()) }.isSuccess)
      val shared = new IllegalStateException("existing shared borrow failure")
      second.gate.markFailed(shared)
      val rejected = assertThrows(classOf[IllegalStateException], () => {
        sharedScope.requireCurrent(scope)
        ()
      })
      assertSame(shared, rejected)
      assertSame(shared, secondLease.complete(Success(())).failed.get)
      assertEquals(0, second.onFailureCalls)
      assertEquals(0, second.delegate.calls)
    finally secondLease.revoke()

  private def auditScope(bootUuid: UUID, bootGeneration: Long, connectionGeneration: Long): MySqlAuditBootScope =
    val identity = new MySqlConnectionIdentity(
      UUID.fromString("00000000-0000-0000-0000-000000000010"), "9.7.2", "fp_scope_test", "test@127.0.0.1",
      "InnoDB", 1, 1L, "READ-COMMITTED", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    new MySqlAuditBootScope(new Object(), identity, None, Thread.currentThread(), bootUuid,
      bootGeneration, connectionGeneration)
