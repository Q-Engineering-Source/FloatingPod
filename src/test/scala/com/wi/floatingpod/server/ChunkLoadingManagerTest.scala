package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{FloatingPodRepository, RecoveryDimensionBackfillOutcome, RecoveryPodStore, StoredPodRow}

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable
import scala.util.{Success, Try}

/** 直接验证票据申请、配额、25 坐标、释放以及恢复过滤逻辑。 */
class ChunkLoadingManagerTest:
  @Test
  def registersRecoveryCallbackOnlyWhenExplicitlyRequested(): Unit =
    val uuid = uuidOf(20)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)

    assertEquals(0, gateway.callbackRegistrations)
    manager.registerRecoveryCallbackOnce()
    manager.registerRecoveryCallbackOnce()
    assertEquals(1, gateway.callbackRegistrations)

  @Test
  def registrationPreservesSynchronousOrderedSelectionObservationsAndReport(): Unit =
    val uuid = uuidOf(201)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = false)), gateway)
    gateway.onRegister = _ => manager.selectRecoverableTickets(new TestWorld(9).dimensionId, List(ticket), 1)

    manager.registerRecoveryCallbackOnce()

    assertEquals(List(StartupTicketCleanupObservation.DroppedByOrderedCallback(9, Some(uuid), "recovery_row_rejected")), manager.startupTicketCleanupSnapshot())
    assertTrue(manager.latestRecoveryReport().issues.exists {
      case TicketRecoveryIssue.CandidateRejected("recovery_row_rejected", Some(observedUuid), _) => observedUuid == uuid
      case _ => false
    })

  @Test
  def failedRegistrationRestoresTheExactPriorSessionBeforeAllowingRetry(): Unit =
    val uuid = uuidOf(202)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = false)), gateway)
    manager.selectRecoverableTickets(new TestWorld(10).dimensionId, List(ticket), 1)
    manager.markWorldTicketPassCompleted(10)
    val priorReport = manager.latestRecoveryReport()
    val priorObservations = manager.startupTicketCleanupSnapshot()
    val registrationFailure = new IllegalStateException("registration failed")
    gateway.registrationFailure = registrationFailure

    val failedRegistration = Try(manager.registerRecoveryCallbackOnce())

    assertTrue(failedRegistration.isFailure)
    assertSame(registrationFailure, failedRegistration.failed.get)
    assertEquals(priorReport, manager.latestRecoveryReport())
    assertEquals(priorObservations, manager.startupTicketCleanupSnapshot())
    gateway.registrationFailure = null
    manager.registerRecoveryCallbackOnce()
    assertEquals(2, gateway.callbackRegistrations)
    assertTrue(manager.latestRecoveryReport().isClean)
    assertEquals(Nil, manager.startupTicketCleanupSnapshot())

  @Test
  def requestsAndReleasesTwentyFiveChunks(): Unit =
    val uuid = uuidOf(21)
    val events = mutable.ListBuffer.empty[String]
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(events)
    val manager = new ChunkLoadingManager(repository, gateway)

    val ticket = manager.requestTicket(uuid, new TestWorld, 10, -3).get
    assertEquals(25, gateway.forced.size)
    assertTrue(gateway.forced.contains((8, -5)))
    assertTrue(gateway.forced.contains((12, -1)))
    assertEquals(1, manager.getAllManagedTickets().size)
    assertTrue(manager.forceChunk(ticket, 99, 100).isSuccess)
    assertTrue(manager.unforceChunk(ticket, 99, 100).isSuccess)
    assertTrue(manager.releaseTicket(uuid).isSuccess)
    assertEquals(0, manager.getAllManagedTickets().size)

  @Test
  def rejectsQuotaAndDepthFailures(): Unit =
    val uuid = uuidOf(22)
    val repository = new TestRepository(stateFor(uuid, active = true))
    val quotaGateway = new TestGateway(mutable.ListBuffer.empty, ticketAvailable = false)
    val quotaManager = new ChunkLoadingManager(repository, quotaGateway)
    assertTrue(quotaManager.requestTicket(uuid, new TestWorld, 0, 0).isFailure)

    val depthGateway = new TestGateway(mutable.ListBuffer.empty, maxDepth = 24)
    val depthManager = new ChunkLoadingManager(repository, depthGateway)
    assertTrue(depthManager.requestTicket(uuid, new TestWorld, 0, 0).isFailure)
    assertEquals(1, depthGateway.releaseCalls)

  @Test
  def initializationFailureRetainsExactReleaseFailureOnOriginalCause(): Unit =
    val uuid = uuidOf(270)
    val gateway = new TestGateway(mutable.ListBuffer.empty, maxDepth = 24, releaseFailuresRemaining = 1)
    val cleanup = new IllegalStateException("original Forge release failure")
    gateway.releaseFailure = cleanup
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)

    val primary = manager.requestTicket(uuid, new TestWorld, 0, 0).failed.get

    assertTrue(primary.getMessage.contains("depth is below required"))
    assertEquals(1, primary.getSuppressed.length)
    assertSame(cleanup, primary.getSuppressed.head)
    assertEquals(1, manager.getAllManagedTickets().size)
    assertEquals(1, manager.pendingTicketReleaseCount())
    assertEquals(1, gateway.requestCalls)
    assertEquals(1, gateway.releaseCalls)
    assertTrue(manager.reconcilePendingTicketReleases().isSuccess)
    assertEquals(0, manager.pendingTicketReleaseCount())
    assertEquals(0, manager.getAllManagedTickets().size)
    assertSame(cleanup, primary.getSuppressed.head)

  @Test
  def initializationReleaseFailureRetainsOwnershipUntilRetrySucceedsBeforeAllocation(): Unit =
    val uuid = uuidOf(27)
    val events = mutable.ListBuffer.empty[String]
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(events, maxDepth = 24, releaseFailuresRemaining = 2)
    val manager = new ChunkLoadingManager(repository, gateway)

    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isFailure)
    assertEquals(1, manager.getAllManagedTickets().size)
    assertEquals(1, manager.pendingTicketReleaseCount())
    val eventsBeforeRetry = events.toList

    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isFailure)
    assertEquals(eventsBeforeRetry :+ "release", events.toList)
    assertEquals(1, gateway.requestCalls)
    assertEquals(1, manager.getAllManagedTickets().size)
    assertEquals(1, manager.pendingTicketReleaseCount())

    gateway.maxDepth = 25
    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isSuccess)
    assertEquals(List("release", "request"), events.drop(eventsBeforeRetry.size + 1).toList)
    assertEquals(2, gateway.requestCalls)
    assertEquals(1, manager.getAllManagedTickets().size)
    assertEquals(0, manager.pendingTicketReleaseCount())
    assertTrue(manager.releaseTicket(uuid).isSuccess)
    assertEquals(0, manager.getAllManagedTickets().size)

  @Test
  def retainsTicketWhenReleaseFailsSoItCanBeRetried(): Unit =
    val uuid = uuidOf(26)
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(mutable.ListBuffer.empty, releaseFailuresRemaining = 1)
    val manager = new ChunkLoadingManager(repository, gateway)
    manager.requestTicket(uuid, new TestWorld, 0, 0).get

    assertTrue(manager.releaseTicket(uuid).isFailure)
    assertEquals(1, manager.getAllManagedTickets().size)
    assertTrue(manager.releaseTicket(uuid).isSuccess)
    assertEquals(0, manager.getAllManagedTickets().size)
    assertEquals(2, gateway.releaseCalls)

  @Test
  def rejectedRecoveryReleaseFailureIsRetriedBeforeNewTicketAllocation(): Unit =
    val uuid = uuidOf(28)
    val events = mutable.ListBuffer.empty[String]
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(events, releaseFailuresRemaining = 2)
    val manager = new ChunkLoadingManager(repository, gateway)
    val rejected = new TestTicket

    manager.recoverAcceptedTickets(List(rejected), new TestWorld)
    assertEquals(1, manager.pendingTicketReleaseCount())
    assertEquals(0, manager.getAllManagedTickets().size)
    val eventsBeforeRetry = events.toList

    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isFailure)
    assertEquals(eventsBeforeRetry :+ "release", events.toList)
    assertEquals(0, gateway.requestCalls)
    assertEquals(1, manager.pendingTicketReleaseCount())

    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isSuccess)
    assertEquals(List("release", "request"), events.drop(eventsBeforeRetry.size + 1).toList)
    assertEquals(1, gateway.requestCalls)
    assertEquals(0, manager.pendingTicketReleaseCount())
    assertEquals(1, manager.getAllManagedTickets().size)
    assertTrue(manager.releaseTicket(uuid).isSuccess)
    assertEquals(0, manager.getAllManagedTickets().size)

  @Test
  def recoveryContinuesAfterPendingReleaseRetryFailure(): Unit =
    val uuid = uuidOf(29)
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(mutable.ListBuffer.empty, releaseFailuresRemaining = 3)
    val manager = new ChunkLoadingManager(repository, gateway)
    val rejected = new TestTicket

    manager.recoverAcceptedTickets(List(rejected), new TestWorld)
    assertEquals(1, manager.pendingTicketReleaseCount())

    val accepted = new TestTicket
    accepted.uuid = Some(uuid)
    manager.recoverAcceptedTickets(List(accepted), new TestWorld)

    assertEquals(1, manager.pendingTicketReleaseCount())
    assertTrue(manager.getAllManagedTickets().get(uuid).exists(_ eq accepted))
    assertEquals(25, gateway.forced.size)

  @Test
  def sharedRawTicketRetryRemovesFreshRecoveryWrapperOwnership(): Unit =
    val uuid = uuidOf(30)
    val repository = new TestRepository(stateFor(uuid, active = true))
    val gateway = new TestGateway(mutable.ListBuffer.empty, releaseFailuresRemaining = 2)
    val manager = new ChunkLoadingManager(repository, gateway)
    val sharedRawTicket = new Object
    val rejected = new TestTicket(sharedRawTicket)

    manager.recoverAcceptedTickets(List(rejected), new TestWorld)
    assertEquals(1, manager.pendingTicketReleaseCount())

    val accepted = new TestTicket(sharedRawTicket)
    accepted.uuid = Some(uuid)
    manager.recoverAcceptedTickets(List(accepted), new TestWorld)
    assertTrue(manager.getAllManagedTickets().get(uuid).exists(_ eq accepted))
    assertEquals(1, manager.pendingTicketReleaseCount())

    assertTrue(manager.reconcilePendingTicketReleases().isSuccess)
    assertEquals(0, manager.pendingTicketReleaseCount())
    assertEquals(0, manager.getAllManagedTickets().size)
    assertTrue(manager.requestTicket(uuid, new TestWorld, 0, 0).isSuccess)
    assertEquals(1, gateway.requestCalls)

  @Test
  def recoveryFiltersInvalidInactiveMissingAndDuplicateTicketsDeterministically(): Unit =
    val active = uuidOf(23)
    val inactive = uuidOf(24)
    val missing = uuidOf(25)
    val duplicate = new TestTicket
    val repository = new TestRepository(stateFor(active, active = true))
    repository.states.put(inactive, stateFor(inactive, active = false))
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(repository, gateway)
    val valid = new TestTicket
    valid.uuid = Some(active)
    valid.uuid = Some(active)
    duplicate.uuid = Some(active)
    val bad = new TestTicket
    val inactiveTicket = new TestTicket
    inactiveTicket.uuid = Some(inactive)
    val missingTicket = new TestTicket
    missingTicket.uuid = Some(missing)

    val selected = manager.selectRecoverableTickets(new TestWorld().dimensionId, List(valid, duplicate, bad, inactiveTicket, missingTicket), 10)
    assertEquals(List(valid), selected)
    assertTrue(gateway.forced.isEmpty)
    manager.recoverAcceptedTickets(List(valid), new TestWorld)
    assertEquals(25, gateway.forced.size)

  @Test
  def sameRawRecoveryReplayPreservesCanonicalRecordWithoutRelease(): Unit =
    val uuid = uuidOf(31)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)
    val rawTicket = new Object
    val callbackWorld = new TestWorld
    val first = new TestTicket(rawTicket)
    first.uuid = Some(uuid)
    val replay = new TestTicket(rawTicket)
    replay.uuid = Some(uuid)

    manager.recoverAcceptedTickets(List(first), callbackWorld)
    manager.recoverAcceptedTickets(List(replay), callbackWorld)

    assertEquals(1, manager.managedTicketRecords().size)
    assertSame(first, manager.managedTicketRecords()(uuid).ticket)
    assertEquals(0, gateway.releaseCalls)
    assertTrue(manager.latestRecoveryReport().isClean)

  @Test
  def distinctRawRecoveryDuplicatePreservesCanonicalAndReportsConflict(): Unit =
    val uuid = uuidOf(32)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)
    val canonical = new TestTicket
    canonical.uuid = Some(uuid)
    val duplicate = new TestTicket
    duplicate.uuid = Some(uuid)
    val world = new TestWorld

    manager.recoverAcceptedTickets(List(canonical), world)
    manager.recoverAcceptedTickets(List(duplicate), world)

    assertSame(canonical, manager.managedTicketRecords()(uuid).ticket)
    assertEquals(1, gateway.releaseCalls)
    assertTrue(manager.latestRecoveryReport().issues.exists(_.isInstanceOf[TicketRecoveryIssue.OwnershipConflict]))

  @Test
  def releaseIfPresentIsIdempotent(): Unit =
    val uuid = uuidOf(33)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)
    manager.requestTicket(uuid, new TestWorld, 0, 0).get

    assertTrue(manager.releaseIfPresent(uuid).isSuccess)
    assertTrue(manager.releaseIfPresent(uuid).isSuccess)
    assertEquals(1, gateway.releaseCalls)

  @Test
  def acceptedRecoveryRecordRetainsCallbackWorld(): Unit =
    val uuid = uuidOf(34)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)
    val ticket = new TestTicket
    val callbackWorld = new TestWorld
    ticket.uuid = Some(uuid)

    manager.recoverAcceptedTickets(List(ticket), callbackWorld)

    assertSame(callbackWorld, manager.managedTicketRecords()(uuid).world)

  @Test
  def selectionAcceptsDesiredActiveLegacyRowWithoutPublicStateConversion(): Unit =
    val uuid = uuidOf(35)
    val repository = new TestRepository(stateFor(uuid, active = true))
    repository.rows.put(uuid, storedRow(uuid, active = true, dimensionId = None))
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(repository, gateway)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)

    assertEquals(List(ticket), manager.selectRecoverableTickets(new TestWorld().dimensionId, List(ticket), 1))

  @Test
  def orphanReconciliationReleasesManagedTicketAndReportsFailure(): Unit =
    val retained = uuidOf(36)
    val orphan = uuidOf(37)
    val gateway = new TestGateway(mutable.ListBuffer.empty, releaseFailuresRemaining = 1)
    val repository = new TestRepository(stateFor(retained, active = true))
    repository.states.put(orphan, stateFor(orphan, active = true))
    val manager = new ChunkLoadingManager(repository, gateway)
    manager.requestTicket(retained, new TestWorld, 0, 0).get
    manager.requestTicket(orphan, new TestWorld, 0, 0).get

    val report = manager.reconcileOrphanedTickets(Set(retained))

    assertFalse(report.isClean)
    assertTrue(report.issues.exists(_.isInstanceOf[TicketRecoveryIssue.OrphanReleaseFailed]))
    assertTrue(manager.managedTicketRecords().contains(orphan))
    assertTrue(manager.releaseIfPresent(orphan).isSuccess)

  @Test
  def recoverySessionAccumulatesOrderedAndLaterWorldIssuesWithoutTurningClean(): Unit =
    val selectedUuid = uuidOf(38)
    val laterUuid = uuidOf(39)
    val repository = new TestRepository(stateFor(selectedUuid, active = true))
    repository.rows.put(laterUuid, storedRow(laterUuid, active = true, dimensionId = None))
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(repository, gateway)
    val rejected = new TestTicket
    val selected = new TestTicket
    selected.uuid = Some(selectedUuid)

    manager.registerRecoveryCallbackOnce()
    assertEquals(List(selected), manager.selectRecoverableTickets(new TestWorld().dimensionId, List(rejected, selected), 2))
    manager.recoverAcceptedTickets(List(selected), new TestWorld)
    assertFalse(manager.latestRecoveryReport().isClean)
    assertTrue(manager.latestRecoveryReport().issues.head.isInstanceOf[TicketRecoveryIssue.CandidateRejected])

    val uuidFailure = new IllegalStateException("later world UUID read failed")
    val failedLater = new TestTicket
    val acceptedLater = new TestTicket
    acceptedLater.uuid = Some(laterUuid)
    gateway.uuidReadFailures.put(failedLater, uuidFailure)
    manager.recoverAcceptedTickets(List(failedLater, acceptedLater), new TestWorld)

    val report = manager.latestRecoveryReport()
    assertTrue(report.issues.head.isInstanceOf[TicketRecoveryIssue.CandidateRejected])
    assertTrue(report.issues.exists {
      case TicketRecoveryIssue.CandidateProcessingFailed(None, cause) => cause eq uuidFailure
      case _ => false
    })
    assertTrue(manager.managedTicketRecords().contains(laterUuid))

  @Test
  def uuidReadFailuresAreSingleReadCauseBearingAndDoNotAbortCandidates(): Unit =
    val orderedUuid = uuidOf(40)
    val acceptedUuid = uuidOf(41)
    val repository = new TestRepository(stateFor(orderedUuid, active = true))
    repository.rows.put(acceptedUuid, storedRow(acceptedUuid, active = true, dimensionId = None))
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(repository, gateway)
    val orderedFailure = new IllegalStateException("ordered UUID read failed")
    val brokenOrdered = new TestTicket
    val validOrdered = new TestTicket
    validOrdered.uuid = Some(orderedUuid)
    gateway.uuidReadFailures.put(brokenOrdered, orderedFailure)

    assertEquals(List(validOrdered), manager.selectRecoverableTickets(new TestWorld().dimensionId, List(brokenOrdered, validOrdered), 2))
    assertEquals(1, gateway.uuidReadCount(brokenOrdered))
    assertEquals(1, gateway.uuidReadCount(validOrdered))
    assertTrue(manager.latestRecoveryReport().issues.exists {
      case TicketRecoveryIssue.CandidateProcessingFailed(None, cause) => cause eq orderedFailure
      case _ => false
    })

    val acceptedFailure = new IllegalStateException("accepted UUID read failed")
    val brokenAccepted = new TestTicket
    val validAccepted = new TestTicket
    validAccepted.uuid = Some(acceptedUuid)
    gateway.uuidReadFailures.put(brokenAccepted, acceptedFailure)
    manager.recoverAcceptedTickets(List(brokenAccepted, validAccepted), new TestWorld)

    assertEquals(1, gateway.uuidReadCount(brokenAccepted))
    assertEquals(1, gateway.uuidReadCount(validAccepted))
    assertTrue(manager.latestRecoveryReport().issues.exists {
      case TicketRecoveryIssue.CandidateProcessingFailed(None, cause) => cause eq acceptedFailure
      case _ => false
    })
    assertTrue(manager.managedTicketRecords().contains(acceptedUuid))

  @Test
  def successfulRejectedCandidateReleaseClearsSharedRawOwnership(): Unit =
    val uuid = uuidOf(42)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)
    val sharedRaw = new Object
    val canonical = new TestTicket(sharedRaw)
    canonical.uuid = Some(uuid)
    val rejected = new TestTicket(sharedRaw)

    manager.recoverAcceptedTickets(List(canonical), new TestWorld)
    manager.recoverAcceptedTickets(List(rejected), new TestWorld)

    assertEquals(0, manager.managedTicketRecords().size)
    assertEquals(0, manager.pendingTicketReleaseCount())

  @Test
  def inactiveOrderedCandidateIsObservedAsDroppedWithItsCallbackDimension(): Unit =
    val uuid = uuidOf(43)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = false)), new TestGateway(mutable.ListBuffer.empty))

    assertEquals(Nil, manager.selectRecoverableTickets(new TestWorld(4).dimensionId, List(ticket), 1))

    assertEquals(List(StartupTicketCleanupObservation.DroppedByOrderedCallback(4, Some(uuid), "recovery_row_rejected")), manager.startupTicketCleanupSnapshot())

  @Test
  def acceptedCandidateRejectionReleaseIsObservedWithCallbackWorldDimension(): Unit =
    val uuid = uuidOf(44)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val gateway = new TestGateway(mutable.ListBuffer.empty, maxDepth = 24)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)

    manager.recoverAcceptedTickets(List(ticket), new TestWorld(5))

    assertEquals(1, gateway.releaseCalls)
    assertEquals(List(StartupTicketCleanupObservation.ReleasedAfterAcceptedRejection(5, Some(uuid))), manager.startupTicketCleanupSnapshot())

  @Test
  def failedAcceptedCandidateReleaseRetainsRetryAndExactObservationCause(): Unit =
    val uuid = uuidOf(45)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val releaseFailure = new IllegalStateException("release observation failure")
    val gateway = new TestGateway(mutable.ListBuffer.empty, maxDepth = 24, releaseFailuresRemaining = 1)
    gateway.releaseFailure = releaseFailure
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = true)), gateway)

    manager.recoverAcceptedTickets(List(ticket), new TestWorld(6))

    assertEquals(List(StartupTicketCleanupObservation.ReleaseFailed(6, Some(uuid), releaseFailure)), manager.startupTicketCleanupSnapshot())
    assertEquals(1, manager.pendingTicketReleaseCount())
    assertTrue(manager.reconcilePendingTicketReleases().isSuccess)
    assertEquals(0, manager.pendingTicketReleaseCount())

  @Test
  def cleanupObservationSnapshotPreservesOrderAndResetsAtCallbackRegistration(): Unit =
    val uuid = uuidOf(46)
    val ticket = new TestTicket
    ticket.uuid = Some(uuid)
    val gateway = new TestGateway(mutable.ListBuffer.empty)
    val manager = new ChunkLoadingManager(new TestRepository(stateFor(uuid, active = false)), gateway)

    manager.selectRecoverableTickets(7, List(ticket), 1)
    manager.markWorldTicketPassCompleted(7)
    val beforeLaterMarker = manager.startupTicketCleanupSnapshot()
    manager.markWorldTicketPassCompleted(8)

    assertEquals(
      List(
        StartupTicketCleanupObservation.DroppedByOrderedCallback(7, Some(uuid), "recovery_row_rejected"),
        StartupTicketCleanupObservation.WorldTicketPassCompleted(7)
      ),
      beforeLaterMarker
    )
    assertEquals(beforeLaterMarker :+ StartupTicketCleanupObservation.WorldTicketPassCompleted(8), manager.startupTicketCleanupSnapshot())
    manager.registerRecoveryCallbackOnce()
    assertEquals(Nil, manager.startupTicketCleanupSnapshot())
    assertEquals(
      List(
        StartupTicketCleanupObservation.DroppedByOrderedCallback(7, Some(uuid), "recovery_row_rejected"),
        StartupTicketCleanupObservation.WorldTicketPassCompleted(7)
      ),
      beforeLaterMarker
    )

  @Test
  def dbFirstLocalRemovalWaitsForSubsequentEndThenCompactsOnlyExactTicketAck(): Unit =
    val pod=uuidOf(701)
    val world=new TestWorld
    val gateway=new TestGateway(mutable.ListBuffer.empty, observeRelease=true)
    val manager=new ChunkLoadingManager(new TestRepository(stateFor(pod,active=true)),gateway)
    val ticket=manager.requestTicket(pod,world,0,0).get
    val tag=IoOperationTag(uuidOf(702),1L,2L,uuidOf(703))
    val attempt=uuidOf(704)
    val view=new LocalRemovalView(pod)
    val observer=new ExactBodyRemovalObserver(view)
    val removal=observer.captureForIo(view.body,tag,attempt,3L,10L)
    val footprint=manager.publicationTicketChunks(pod,world,ticket)
    val pending=manager.captureReleaseForIo(tag,pod,attempt,3L,42,world,ticket,footprint)
    observer.request(removal,10L)
    assertTrue(observer.acknowledgeForIo(removal,10L).isEmpty)
    assertTrue(observer.acknowledgeForIo(removal,11L).isEmpty)
    assertEquals(0,gateway.releaseCalls)
    assertEquals(1,view.requests)
    view.indexed=false
    val ack=observer.acknowledgeForIo(removal,12L).get
    val released=manager.releaseForIo(pending,ack).get
    assertEquals(1,gateway.releaseCalls)
    assertEquals(25,pending.originalFootprint.size)
    assertTrue(manager.managedTicketRecords().isEmpty)
    manager.compactDbFirstRecallRelease(pending,released)
    assertThrows(classOf[IllegalArgumentException],()=>manager.compactDbFirstRecallRelease(pending,released))
    assertEquals(1,view.requests)
    assertEquals(1,gateway.releaseCalls)

  @Test
  def dbFirstTicketFailureKeepsOriginalCauseAndCannotCompactOrConsumeForeignAck(): Unit =
    val pod=uuidOf(711)
    val world=new TestWorld
    val gateway=new TestGateway(mutable.ListBuffer.empty,observeRelease=true)
    val manager=new ChunkLoadingManager(new TestRepository(stateFor(pod,active=true)),gateway)
    val ticket=manager.requestTicket(pod,world,0,0).get
    val tag=IoOperationTag(uuidOf(712),1L,2L,uuidOf(713))
    val attempt=uuidOf(714)
    val view=new LocalRemovalView(pod)
    val observer=new ExactBodyRemovalObserver(view)
    val removal=observer.captureForIo(view.body,tag,attempt,3L,10L)
    val pending=manager.captureReleaseForIo(tag,pod,attempt,3L,42,world,ticket,
      manager.publicationTicketChunks(pod,world,ticket))
    observer.request(removal,10L)
    view.indexed=false
    val ack=observer.acknowledgeForIo(removal,11L).get
    val foreignView=new LocalRemovalView(pod)
    val foreignObserver=new ExactBodyRemovalObserver(foreignView)
    val foreign=foreignObserver.captureForIo(foreignView.body,tag.copy(operationId=uuidOf(715)),attempt,3L,10L)
    foreignObserver.request(foreign,10L)
    foreignView.indexed=false
    assertTrue(manager.releaseForIo(pending,foreignObserver.acknowledgeForIo(foreign,11L).get).isFailure)
    assertEquals(0,gateway.releaseCalls)
    val primary=new IllegalStateException("actual ticket release failed",new IllegalArgumentException("release cause"))
    gateway.releaseFailure=primary
    gateway.releaseFailuresRemaining=1
    assertSame(primary,manager.releaseForIo(pending,ack).failed.get)
    assertSame(primary,pending.failure.get)
    assertTrue(manager.managedTicketRecords().get(pod).exists(_.ticket eq ticket))
    assertTrue(pending.acknowledgement.isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>manager.compactDbFirstRecallRelease(pending,null))
    assertEquals(1,gateway.releaseCalls)
    assertEquals(1,view.requests)
    assertEquals(25,pending.originalFootprint.size)

  /** 与生产共享同一observer，集合延后撤销模拟removeEntity后的局部索引等待。 */
  private final class LocalRemovalView(pod: UUID) extends RemovalWorldView:
    val worldRef: AnyRef=new Object
    val body: AnyRef=new Object
    val dimensionId=0
    private val owner=Thread.currentThread()
    var indexed=true
    var requests=0
    def assertMain():Unit=require(Thread.currentThread() eq owner)
    def describeBody(value:AnyRef):RemovalEntityView=
      require(value eq body)
      RemovalEntityView(body,worldRef,42,pod)
    def requestRemove(value:AnyRef):Unit=
      assertMain()
      require(value eq body)
      requests+=1
    def snapshot(value:AnyRef,entityId:Int,podUuid:UUID):RemovalWorldSnapshot=
      assertMain()
      val exact=describeBody(value)
      RemovalWorldSnapshot(0,Vector(worldRef),exact,
        if indexed then Vector(RemovalIndexSlot("original-local-chunk",exact)) else Vector.empty)

  private def uuidOf(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")
  private def stateFor(uuid: UUID, active: Boolean): FloatingPodState =
    FloatingPodState(uuid, None, 0L, "overworld", 160.0D, 64.0D, -48.0D, 0.0F, 0.0F, active, 1L, if active then Some(0) else None, if active then PodOperatingMode.WorldIdle else PodOperatingMode.Normal)

  private def storedRow(uuid: UUID, active: Boolean, dimensionId: Option[Int]): StoredPodRow =
    StoredPodRow(uuid, None, 0L, "overworld", 160.0D, 64.0D, -48.0D, 0.0F, 0.0F, active, 1L, dimensionId, if active then PodOperatingMode.WorldIdle else PodOperatingMode.Normal)

  private final class TestWorld(override val dimensionId: Int = 0) extends PodWorld:
    override def diagnosticWorldName: String = "chunk-loading-test"
    override def findPod(podUuid: UUID): Option[ActivePodEntity] = None
    override def sameWorld(other: PodWorld): Boolean = this.eq(other)
    override def spawnPod(state: FloatingPodState): ActivePodEntity = throw new UnsupportedOperationException()
    override def removePod(entity: ActivePodEntity): Unit = throw new UnsupportedOperationException()

  private final class TestTicket(val rawIdentity: AnyRef = new Object) extends ChunkTicket:
    var uuid: Option[UUID] = None
    override private[server] def ownershipKey: AnyRef = rawIdentity

  private final class TestGateway(
      events: mutable.ListBuffer[String],
      ticketAvailable: Boolean = true,
      var maxDepth: Int = 25,
      var releaseFailuresRemaining: Int = 0,
      observeRelease: Boolean = false
  ) extends ChunkTicketGateway:
    val forced = mutable.Set.empty[(Int, Int)]
    private val registered = mutable.Set.empty[AnyRef]
    var requestCalls = 0
    var releaseCalls = 0
    var callbackRegistrations = 0
    var releaseFailure: Throwable = null
    var registrationFailure: Throwable = null
    var onRegister: ForgeChunkManager.OrderedLoadingCallback => Unit = null
    val uuidReadFailures = mutable.HashMap.empty[ChunkTicket, Throwable]
    private val uuidReads = mutable.HashMap.empty[ChunkTicket, Int]
    override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit =
      callbackRegistrations += 1
      Option(registrationFailure).foreach(throw _)
      Option(onRegister).foreach(_(callback))
    override def request(world: PodWorld): Option[ChunkTicket] =
      events += "request"
      requestCalls += 1
      if ticketAvailable then
        val ticket=new TestTicket
        if observeRelease then registered+=ticket.ownershipKey
        Some(ticket)
      else None
    override def release(ticket: ChunkTicket): Unit =
      events += "release"
      releaseCalls += 1
      if releaseFailuresRemaining > 0 then
        releaseFailuresRemaining -= 1
        throw Option(releaseFailure).getOrElse(new IllegalStateException("release failed"))
      if observeRelease then
        registered-=ticket.ownershipKey
        forced.clear()
    override def forcedChunks(ticket: ChunkTicket): Set[(Int,Int)] =
      if observeRelease then forced.toSet else super.forcedChunks(ticket)
    override private[server] def releaseSnapshot(ticket:ChunkTicket):TicketReleaseSnapshot=
      if observeRelease then TicketReleaseSnapshot(registered.toVector,
        if forced.nonEmpty then Vector(ticket.ownershipKey) else Vector.empty,forced.toSet)
      else super.releaseSnapshot(ticket)
    override def maxChunkDepth(ticket: ChunkTicket): Int = maxDepth
    override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
    override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[TestTicket].uuid = Some(podUuid)
    override def readPodUuid(ticket: ChunkTicket): Option[UUID] =
      uuidReads.update(ticket, uuidReads.getOrElse(ticket, 0) + 1)
      uuidReadFailures.get(ticket).foreach(throw _)
      ticket.asInstanceOf[TestTicket].uuid
    def uuidReadCount(ticket: ChunkTicket): Int = uuidReads.getOrElse(ticket, 0)
    override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = forced += ((chunkX, chunkZ))
    override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = forced -= ((chunkX, chunkZ))
    override def wrapRecovered(ticket: Ticket): ChunkTicket = new TestTicket
    override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

  private final class TestRepository(initial: FloatingPodState) extends FloatingPodRepository, RecoveryPodStore:
    val states = mutable.HashMap(initial.uuid -> initial)
    val rows = mutable.HashMap(initial.uuid -> storedRow(initial.uuid, initial.isActive, initial.dimensionId))
    override def insert(state: FloatingPodState): Try[FloatingPodState] = { states.put(state.uuid, state); Success(state) }
    override def update(state: FloatingPodState): Try[FloatingPodState] = Success(state.copy(stateVersion = state.stateVersion + 1L))
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(states.get(uuid))
    override def findDesiredActiveRows(): Try[List[StoredPodRow]] = Success(rows.values.filter(_.isActive).toList)
    override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(rows.get(uuid))
    override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] =
      throw new UnsupportedOperationException()
    override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(rows.values.map(_.toPublicState).toList)
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.filter(_.isActive).toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(states.remove(uuid).nonEmpty)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()
