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

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import java.util.UUID
import scala.util.{Failure, Success, Try}

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 固定 dispatcher/controlled five-trait boundary；不是SQL/open/admission integration。 */
class MySqlImmutableCommandDispatcherTest:
  private final class ControlledStores(successReads: Boolean = false) extends FloatingPodRepository
      with ActivityTransitionStore with PlayerEntitlementStore with RecoveryPodStore with PhysicalRecoveryStore:
    val original = new IllegalStateException("controlled existing-trait failure, not JDBC")
    var calls = Vector.empty[String]
    var transactions = 0
    var rolledBack = false
    var modeArguments = Option.empty[(PhysicalRecoveryBinding, UUID, UUID, Long, UUID, PodOperatingMode, FloatingPodPose)]
    override def insert(state: FloatingPodState): Try[FloatingPodState] = { calls :+= "insert"; Failure(original) }
    override def update(state: FloatingPodState): Try[FloatingPodState] = { calls :+= "update"; Failure(original) }
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = { calls :+= "findByUuid"; Failure(original) }
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = { calls :+= "findByOwner"; Failure(original) }
    override def findAllActive(): Try[List[FloatingPodState]] = { calls :+= "findAllActive"; if successReads then Success(Nil) else Failure(original) }
    override def findAll(): Try[List[FloatingPodState]] = { calls :+= "findAll"; Failure(original) }
    override def delete(uuid: UUID): Try[Boolean] = { calls :+= "delete"; Failure(original) }
    override def transaction[T](fn: => T): Try[T] =
      transactions += 1
      try Success(fn)
      catch
        case cause: Throwable => rolledBack = true; Failure(cause)
    override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] = { calls :+= "findByPlayerUuid"; Failure(original) }
    override def findDesiredActiveRows(): Try[List[StoredPodRow]] = { calls :+= "findDesiredActiveRows"; Failure(original) }
    override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = { calls :+= "findStoredByUuid"; Failure(original) }
    override def findAllRecoverable(): Try[List[FloatingPodState]] = { calls :+= "findAllRecoverable"; Failure(original) }
    override def backfillMissingActiveDimension(
        uuid: UUID,
        expectedStateVersion: Long,
        authoritativeDimensionId: Int
    ): Try[RecoveryDimensionBackfillOutcome] = { calls :+= "backfillMissingActiveDimension"; Failure(original) }
    override def activateIfAdmissible(
        target: FloatingPodState,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        dimensionId: Int,
        globalActiveLimit: Int
    ): Try[AdmissionOutcome] = { calls :+= "activateIfAdmissible"; Failure(original) }
    override def deactivateIfExpectedActive(
        podUuid: UUID,
        expectedOwner: Option[UUID],
        expectedStateVersion: Long
    ): Try[DeactivationOutcome] = { calls :+= "deactivateIfExpectedActive"; Failure(original) }
    override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = { calls :+= "findPhysicalRecoveryBinding"; Failure(original) }
    override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = { calls :+= "createOrRetainLegacyUncertifiedBinding"; Failure(original) }
    override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = { calls :+= "certifyPhysicalRecoveryBinding"; Failure(original) }
    override def prepareObservedUnbound(
        observation: ObservedUnboundPod,
        expectedBinding: PhysicalRecoveryBinding,
        attemptUuid: UUID,
        uniquenessProof: ObservedUnboundUniquenessReceipt
    ): Try[UnboundPreparationReceipt] = { calls :+= "prepareObservedUnbound"; Failure(original) }
    override def commitObservedUnbound(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        expectedPreparedVersion: Long,
        actualPose: FloatingPodPose,
        globalActiveLimit: Int
    ): Try[UnboundCommitOutcome] = { calls :+= "commitObservedUnbound"; Failure(original) }
    override def prepareUnboundCreate(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        targetPose: FloatingPodPose,
        operationUuid: UUID
    ): Try[UnboundPreparationReceipt] = { calls :+= "prepareUnboundCreate"; Failure(original) }
    override def commitUnboundCreate(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        attemptUuid: UUID,
        expectedPreparedVersion: Long,
        actualPose: FloatingPodPose,
        operationUuid: UUID,
        globalActiveLimit: Int
    ): Try[UnboundCommitOutcome] = { calls :+= "commitUnboundCreate"; Failure(original) }
    override def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] = { calls :+= "findUnboundPreparation"; Failure(original) }
    override def prepareCreateLocator(
        podUuid: UUID,
        expectedDomainVersion: Long,
        attemptUuid: UUID,
        targetDimensionId: Int,
        chunks: Set[PhysicalRecoveryChunk]
    ): Try[PhysicalRecoveryLocator] = { calls :+= "prepareCreateLocator"; Failure(original) }
    override def activatePreparedIfAdmissible(
        target: FloatingPodState,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        dimensionId: Int,
        globalActiveLimit: Int,
        attemptUuid: UUID
    ): Try[AdmissionOutcome] = { calls :+= "activatePreparedIfAdmissible"; Failure(original) }
    override def deactivateTrackedIfExpectedActive(
        podUuid: UUID,
        expectedOwner: Option[UUID],
        expectedStateVersion: Long,
        attemptUuid: UUID
    ): Try[DeactivationOutcome] = { calls :+= "deactivateTrackedIfExpectedActive"; Failure(original) }
    override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] = { calls :+= "updateTrackedActiveDrift"; Failure(original) }
    override def setOperatingModeTrackedIfOwned(
        expectedBinding: PhysicalRecoveryBinding,
        podUuid: UUID,
        authenticatedOwner: UUID,
        expectedStateVersion: Long,
        attemptUuid: UUID,
        targetMode: PodOperatingMode,
        actualPose: FloatingPodPose
    ): Try[OperatingModeChangeOutcome] = { calls :+= "setOperatingModeTrackedIfOwned"; modeArguments = Some((expectedBinding, podUuid, authenticatedOwner, expectedStateVersion, attemptUuid, targetMode, actualPose)); Failure(original) }
    override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = { calls :+= "findPhysicalRecoveryLocator"; Failure(original) }
    override def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] = { calls :+= "findAllPhysicalRecoveryLocators"; Failure(original) }
    override def verifyInactiveAbsence(
        podUuid: UUID,
        expectedStateVersion: Long,
        attemptUuid: UUID
    ): Try[PhysicalRecoveryLocator] = { calls :+= "verifyInactiveAbsence"; Failure(original) }
    override def close(): Unit = throw new UnsupportedOperationException("owned close is not a dispatcher operation")

  private val id = UUID.randomUUID()
  private val state = FloatingPodState(id, Some(id), 0L, "world", 1, 2, 3, 0f, 0f, false, 0L, Some(0))
  private val pose = FloatingPodPose("world", 1, 2, 3, 0f, 0f, 0)
  private val binding = PhysicalRecoveryBinding(id, com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified, 2)
  private val observed = ObservedUnboundPod(id,
    com.wi.floatingpod.storage.ObservedUnboundIdentity(com.wi.floatingpod.storage.ObservedUnboundSourceKind.FreshArtifact, id, "fixture"), pose)
  private val proof = ObservedUnboundUniquenessReceipt(id, observed)

  @Test def everyFixedOperationCallsItsActualTraitMethodAndPreservesFailure(): Unit =
    val stores = new ControlledStores()
    val cases: Vector[(MySqlRepositoryOperation, String)] = Vector(
      (MySqlRepositoryOperation.Insert(state), "insert"),
      (MySqlRepositoryOperation.Update(state), "update"),
      (MySqlRepositoryOperation.FindByUuid(id), "findByUuid"),
      (MySqlRepositoryOperation.FindByOwner(id), "findByOwner"),
      (MySqlRepositoryOperation.FindAllActive, "findAllActive"),
      (MySqlRepositoryOperation.FindAll, "findAll"),
      (MySqlRepositoryOperation.Delete(id), "delete"),
      (MySqlRepositoryOperation.FindByPlayerUuid(id), "findByPlayerUuid"),
      (MySqlRepositoryOperation.FindDesiredActiveRows, "findDesiredActiveRows"),
      (MySqlRepositoryOperation.FindStoredByUuid(id), "findStoredByUuid"),
      (MySqlRepositoryOperation.FindAllRecoverable, "findAllRecoverable"),
      (MySqlRepositoryOperation.BackfillMissingActiveDimension(id, 0L, 1), "backfillMissingActiveDimension"),
      (MySqlRepositoryOperation.ActivateIfAdmissible(state, id, 0L, 1, 1), "activateIfAdmissible"),
      (MySqlRepositoryOperation.DeactivateIfExpectedActive(id, Some(id), 0L), "deactivateIfExpectedActive"),
      (MySqlRepositoryOperation.FindPhysicalRecoveryBinding, "findPhysicalRecoveryBinding"),
      (MySqlRepositoryOperation.CreateOrRetainLegacyUncertifiedBinding(id, 1), "createOrRetainLegacyUncertifiedBinding"),
      (MySqlRepositoryOperation.CertifyPhysicalRecoveryBinding(id, 1), "certifyPhysicalRecoveryBinding"),
      (MySqlRepositoryOperation.PrepareObservedUnbound(observed, binding, id, proof), "prepareObservedUnbound"),
      (MySqlRepositoryOperation.CommitObservedUnbound(binding, id, id, 0L, pose, 1), "commitObservedUnbound"),
      (MySqlRepositoryOperation.PrepareUnboundCreate(binding, id, id, pose, id), "prepareUnboundCreate"),
      (MySqlRepositoryOperation.CommitUnboundCreate(binding, id, id, 0L, pose, id, 1), "commitUnboundCreate"),
      (MySqlRepositoryOperation.FindUnboundPreparation(id), "findUnboundPreparation"),
      (MySqlRepositoryOperation.PrepareCreateLocator(id, 0L, id, 1, Set(PhysicalRecoveryChunk(0, 0, 0))), "prepareCreateLocator"),
      (MySqlRepositoryOperation.ActivatePreparedIfAdmissible(state, id, 0L, 1, 1, id), "activatePreparedIfAdmissible"),
      (MySqlRepositoryOperation.DeactivateTrackedIfExpectedActive(id, Some(id), 0L, id), "deactivateTrackedIfExpectedActive"),
      (MySqlRepositoryOperation.UpdateTrackedActiveDrift(state, id), "updateTrackedActiveDrift"),
      (MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, id, id, 0L, id, PodOperatingMode.Normal, pose), "setOperatingModeTrackedIfOwned"),
      (MySqlRepositoryOperation.FindPhysicalRecoveryLocator(id), "findPhysicalRecoveryLocator"),
      (MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators, "findAllPhysicalRecoveryLocators"),
      (MySqlRepositoryOperation.VerifyInactiveAbsence(id, 0L, id), "verifyInactiveAbsence")
    )
    cases.foreach { (operation, method) =>
      val before = stores.calls.size
      val result = MySqlImmutableCommandDispatcher.execute(operation, stores)
      assertSame(stores.original, result.failed.get)
      assertEquals(before + 1, stores.calls.size)
      assertEquals(method, stores.calls.last)
    }

  @Test def immutableModeFieldsAreForwardedWithoutIdentityOrPoseRewrite(): Unit =
    val stores = new ControlledStores()
    val pod = UUID.randomUUID(); val owner = UUID.randomUUID(); val attempt = UUID.randomUUID()
    val operation = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, owner, 7L, attempt, PodOperatingMode.WorldIdle, pose)
    assertSame(stores.original, MySqlImmutableCommandDispatcher.execute(operation, stores).failed.get)
    assertEquals(Some((binding, pod, owner, 7L, attempt, PodOperatingMode.WorldIdle, pose)), stores.modeArguments)

  @Test def fixedVectorTransactionStopsAtOriginalFailureAndExecutesRealTraitTransactionBody(): Unit =
    val stores = new ControlledStores(true)
    val result = MySqlImmutableCommandDispatcher.transaction(Vector(
      MySqlRepositoryOperation.FindAllActive, MySqlRepositoryOperation.FindAll, MySqlRepositoryOperation.FindByUuid(id)), stores)
    assertSame(stores.original, result.failed.get)
    assertEquals(Vector("findAllActive", "findAll"), stores.calls)
    assertEquals(1, stores.transactions)
    assertTrue(stores.rolledBack) // controlled trait fn failed; NOT a SQL rollback claim.

  @Test def successfulReplyContainsOnlyImmutableDomainList(): Unit =
    val stores = new ControlledStores(true)
    assertEquals(Success(MySqlRepositoryReply.States(Nil)),
      MySqlImmutableCommandDispatcher.execute(MySqlRepositoryOperation.FindAllActive, stores))
    assertEquals(Success(Vector(MySqlRepositoryReply.States(Nil))),
      MySqlImmutableCommandDispatcher.transaction(Vector(MySqlRepositoryOperation.FindAllActive), stores))

  @Test def actualSerialWorkerExecutesFixedOperationsKeepsTagAndWaitsForHookTerminal(): Unit =
    val stores = new ControlledStores(true)
    val workEntered = new java.util.concurrent.CountDownLatch(1)
    val workRelease = new java.util.concurrent.CountDownLatch(1)
    val hookEntered = new java.util.concurrent.CountDownLatch(1)
    val hookRelease = new java.util.concurrent.CountDownLatch(1)
    val worker = new java.util.concurrent.atomic.AtomicReference[Thread]()
    val hookWorker = new java.util.concurrent.atomic.AtomicReference[Thread]()
    val helpers = scala.collection.mutable.ArrayBuffer.empty[Thread]
    val io = new com.wi.floatingpod.server.io.OwnedSerialIo[MySqlRepositoryOperation, MySqlRepositoryReply](
      2, Thread.currentThread(), "actual-dispatch-owned-worker", Some(() => {
        hookWorker.set(Thread.currentThread())
        hookEntered.countDown()
        assertTrue(hookRelease.await(5, java.util.concurrent.TimeUnit.SECONDS), "hook release timeout")
      }))(operation => {
        worker.set(Thread.currentThread())
        if operation == MySqlRepositoryOperation.FindAllActive then
          workEntered.countDown()
          assertTrue(workRelease.await(5, java.util.concurrent.TimeUnit.SECONDS), "work release timeout")
        MySqlImmutableCommandDispatcher.execute(operation, stores).get
      })
    def waitOff(timeout: Long, expected: Boolean): Unit =
      val result = new java.util.concurrent.atomic.AtomicReference[java.lang.Boolean]()
      val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try result.set(io.awaitExitOffMain(timeout, java.util.concurrent.TimeUnit.MILLISECONDS))
        catch case cause: Throwable => failure.set(cause)
        ()
      }, "dispatcher-offMain-tail")
      helpers += waiter
      waiter.start()
      waiter.join(5000)
      assertFalse(waiter.isAlive, "offMain waiter timeout")
      if failure.get() != null then throw failure.get()
      assertNotNull(result.get())
      assertEquals(expected, result.get().booleanValue())
    var primary: Throwable = null
    try
      val first = com.wi.floatingpod.server.io.IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID())
      val second = first.copy(operationId = UUID.randomUUID())
      assertEquals(com.wi.floatingpod.server.io.IoSubmission.Accepted, io.submit(first, MySqlRepositoryOperation.FindAllActive))
      assertTrue(workEntered.await(5, java.util.concurrent.TimeUnit.SECONDS), "work entered timeout")
      assertEquals(com.wi.floatingpod.server.io.IoSubmission.Accepted, io.submit(second, MySqlRepositoryOperation.FindAll))
      assertEquals(com.wi.floatingpod.server.io.IoSubmission.RejectedCapacity, io.submit(first, MySqlRepositoryOperation.FindAll))
      io.quiesce()
      workRelease.countDown()
      assertTrue(hookEntered.await(5, java.util.concurrent.TimeUnit.SECONDS), "hook entered timeout")
      waitOff(10, false)
      hookRelease.countDown()
      waitOff(5000, true)
      val completions = io.drain(2)
      assertEquals(2, completions.size)
      assertSame(first, completions.head.tag)
      assertSame(second, completions(1).tag)
      assertEquals(Success(MySqlRepositoryReply.States(Nil)), completions.head.result)
      assertSame(stores.original, completions(1).result.failed.get)
      assertSame(worker.get(), hookWorker.get())
      assertEquals(Vector("findAllActive", "findAll"), stores.calls)
      assertEquals(0, io.checkpoint.reserved)
    catch case cause: Throwable => primary = cause
    finally
      workRelease.countDown()
      hookRelease.countDown()
      def cleanup(action: => Unit): Unit =
        try action
        catch
          case cause: Throwable =>
            if primary == null then primary = cause
            else MySqlOwnedSessionLifecycle.suppress(primary, cause)
      cleanup(io.quiesce())
      cleanup(waitOff(5000, true))
      helpers.foreach(t => cleanup { t.join(5000); assertFalse(t.isAlive, "helper leaked") })
      if worker.get() != null then cleanup { worker.get().join(5000); assertFalse(worker.get().isAlive, "worker leaked") }
    if primary != null then throw primary

  @Test def deploymentOriginalGenericExecuteRejectsWithoutStoreCalls(): Unit =
    val stores = new ControlledStores()
    val result = MySqlImmutableCommandDispatcher.execute(MySqlRepositoryOperation.FindOriginalDeploymentOperation(id,id),stores)
    assertTrue(result.failed.get.isInstanceOf[UnsupportedOperationException])
    assertEquals("deployment original read requires the dedicated planned owned route",result.failed.get.getMessage)
    assertTrue(stores.calls.isEmpty)
    assertEquals(0,stores.transactions)

  @Test def deploymentOriginalTransactionPreflightsEntireVectorBeforeAnyMutation(): Unit =
    val stores = new ControlledStores()
    val find = MySqlRepositoryOperation.FindOriginalDeploymentOperation(id,id)
    val result = MySqlImmutableCommandDispatcher.transaction(Vector(MySqlRepositoryOperation.Insert(state),find),stores)
    assertTrue(result.failed.get.isInstanceOf[UnsupportedOperationException])
    assertTrue(stores.calls.isEmpty)
    assertEquals(0,stores.transactions)

  @Test def deploymentOriginalProjectionChecksCompleteMutationIdentity(): Unit =
    val expected = new ExpectedCommandVersions(id,1,id,2,3,4,5)
    def request(retract: Boolean): Request = if retract then new RetractRequest(id,id,id,1,expected) else new DeployRequest(id,id,id,1,expected)
    def receipt(retract: Boolean) = new MutationReceipt(id,id,id,1,if retract then MutationKind.RETRACT else MutationKind.DEPLOY,
      expected,OperationPhase.QUEUED,ReceiptReason.NONE,ActualEffectStatus.NOT_STARTED,
      CriticalCommitStatus.NOT_ATTEMPTED,ReadinessStatus.NOT_EVALUATED,CleanupStatus.NOT_REQUIRED)
    Vector(false,true).foreach { retract =>
      val req=request(retract); val rec=receipt(retract)
      val result=new MySqlOriginalDeploymentOperation(id,id,req,rec,7L)
      assertSame(req,result.request); assertSame(rec,result.receipt)
      assertEquals(id,result.actorUuid); assertEquals(id,result.originalOperationId); assertEquals(7L,result.rowRevision)
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlOriginalDeploymentOperation(id,UUID.randomUUID(),req,rec,7L))
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlOriginalDeploymentOperation(id,id,request(!retract),rec,7L))
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlOriginalDeploymentOperation(id,id,req,rec,-1L))
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlOriginalDeploymentOperation(id,id,new QueryOriginalRequest(id),rec,7L))
      val wrongExpected=new ExpectedCommandVersions(id,1,id,2,3,4,6)
      val wrong: Request=if retract then new RetractRequest(id,id,id,1,wrongExpected) else new DeployRequest(id,id,id,1,wrongExpected)
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlOriginalDeploymentOperation(id,id,wrong,rec,7L))
    }
