package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.storage.StoredPodRow
import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission, OwnedSerialIo}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.util.{Failure, Success}

class MainOwnedRequestReadPreviewTest:
  private val pod = new UUID(1L, 2L)
  private val player = new UUID(3L, 4L)
  private val boot = new UUID(5L, 6L)
  private def identity = MySqlOwnedIoIdentity(boot, 2L, 3L)
  private def tag(n: Long = 1L) = IoOperationTag(boot, 2L, 3L, new UUID(7L, n))
  private def request = MySqlRequestReadPreview(pod, player, 4L)
  private def state(owner: Option[UUID] = Some(player), version: Long = 4L, active: Boolean = false) =
    FloatingPodState(pod, owner, version, "actual", -1.5D, 64D, 2D, -0F, 1F, active, 12345L,
      if active then Some(-1) else None, if owner.isEmpty && active then PodOperatingMode.WorldIdle else PodOperatingMode.Normal)
  private def row(id: UUID = pod, owner: Option[UUID] = Some(player)) =
    StoredPodRow(id, owner, 4L, "actual", 1D, 2D, 3D, 1F, 2F, true, 9L, None,
      if owner.isEmpty then PodOperatingMode.WorldIdle else PodOperatingMode.Normal)
  private def replies(value: Option[FloatingPodState] = Some(state()),
      entitlement: Option[PlayerEntitlement] = None, rows: List[StoredPodRow] = Nil): Vector[MySqlRepositoryReply] =
    Vector(MySqlRepositoryReply.OptionalState(value), MySqlRepositoryReply.Entitlement(entitlement),
      MySqlRepositoryReply.StoredRows(rows))
  private def completion(t: IoOperationTag = tag(), values: Vector[MySqlRepositoryReply] = replies()) =
    IoCompletion(t, Success(MySqlOwnedIoReply.Transaction(values)))
  private def ledger(capacity: Int = 2) = new MainRequestReadLedger(identity, Thread.currentThread(), capacity)

  @Test def originalDeploymentMissingUsesActualReplyAndOriginalDropAck():Unit =
    val core=ledger()
    val lookup: MySqlRepositoryOperation.FindOriginalDeploymentOperation =
      MySqlRepositoryOperation.FindOriginalDeploymentOperation(player,new UUID(41,42))
    val entry=core.begin(tag(),lookup,0L).get
    core.submitted(entry,IoSubmission.Accepted)
    assertEquals(MainRequestReadPoll.Pending,core.poll(tag(),0L))
    core.end(1L,0L,Vector(IoCompletion(tag(),Success(MySqlOwnedIoReply.Operation(
      MySqlRepositoryReply.OriginalDeploymentOperation(None))))))
    assertEquals(MainRequestReadPoll.OriginalDeployment(None),core.poll(tag(),0L))
    assertTrue(core.releaseCandidates.isEmpty)
    core.end(2L,0L,Vector.empty)
    assertEquals(Vector(entry),core.releaseCandidates)
    core.dropped(entry)
    assertTrue(entry.originalResult.isEmpty)
    assertEquals(1,core.retained)
    assertTrue(core.released(entry,IoCompletion(tag(),Success(MySqlOwnedIoReply.RetainedPlanReleased(tag())))))
    assertEquals(0,core.retained)

  @Test def originalDeploymentRejectsPreviewReplyAndRetainsFailure():Unit =
    val core=ledger()
    val entry=core.begin(tag(),MySqlRepositoryOperation.FindOriginalDeploymentOperation(player,new UUID(43,44)),0L).get
    core.submitted(entry,IoSubmission.Accepted)
    core.end(1L,0L,Vector(completion()))
    assertTrue(core.poll(tag(),0L).isInstanceOf[MainRequestReadPoll.Failed])
    assertTrue(entry.originalResult.isEmpty)
    assertTrue(core.releaseCandidates.isEmpty)
    assertEquals(1,core.retained)

  @Test def originalDeploymentChangedEpochDoesNotConvertReplyToMissing():Unit =
    val core=ledger()
    val entry=core.begin(tag(),MySqlRepositoryOperation.FindOriginalDeploymentOperation(player,new UUID(45,46)),0L).get
    core.submitted(entry,IoSubmission.Accepted)
    core.end(1L,1L,Vector(IoCompletion(tag(),Success(MySqlOwnedIoReply.Operation(
      MySqlRepositoryReply.OriginalDeploymentOperation(None))))))
    assertTrue(core.poll(tag(),1L).isInstanceOf[MainRequestReadPoll.Failed])
    assertTrue(entry.originalResult.isEmpty)
    assertFalse(entry.decisionConsumed)
    assertTrue(entry.completion.nonEmpty)

  @Test def fixedThreeQueriesAreImmutableAndOrdered(): Unit =
    assertEquals(Vector(MySqlRepositoryOperation.FindByUuid(pod), MySqlRepositoryOperation.FindByPlayerUuid(player),
      MySqlRepositoryOperation.FindDesiredActiveRows), request.operations)
    assertThrows(classOf[IllegalArgumentException], () => MySqlRequestReadPreview(pod, player, -1L))

  @Test def ownershipOrderPreservesMissingNoneAndVersion(): Unit =
    assertEquals(MySqlRequestOwnershipPreview.Missing, MySqlRequestReadPreview.decide(request, replies(None)).ownership)
    assertEquals(MySqlRequestOwnershipPreview.NotOwner, MySqlRequestReadPreview.decide(request, replies(Some(state(None)))).ownership)
    assertEquals(MySqlRequestOwnershipPreview.VersionMismatch(5L),
      MySqlRequestReadPreview.decide(request, replies(Some(state(version = 5L)))).ownership)
    assertEquals(MySqlRequestOwnershipPreview.Matched(4L, false), MySqlRequestReadPreview.decide(request, replies()).ownership)

  @Test def entitlementAbsentZeroAndWorldIdleRowsDoNotLazyInsert(): Unit =
    val rows = List(row(), row(new UUID(9L, 1L), None))
    val default = MySqlRequestReadPreview.decide(request, replies(rows = rows))
    assertEquals(1, default.activeOwned)
    assertEquals(PlayerEntitlement.DefaultSimultaneousActiveLimit, default.simultaneousLimit)
    val zero = MySqlRequestReadPreview.decide(request, replies(entitlement = Some(PlayerEntitlement(player, 0, 0))))
    assertFalse(zero.belowEntitlementLimit)

  @Test def foreignIdentityDuplicateRowsAndWrongReplyOrderFail(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => MySqlRequestReadPreview.decide(request,
      replies(Some(state().copy(uuid = new UUID(1L, 3L))))))
    assertThrows(classOf[IllegalArgumentException], () => MySqlRequestReadPreview.decide(request,
      replies(entitlement = Some(PlayerEntitlement(new UUID(8L, 1L))))))
    assertThrows(classOf[IllegalArgumentException], () => MySqlRequestReadPreview.decide(request, replies(rows = List(row(), row()))))
    assertThrows(classOf[IllegalStateException], () => MySqlRequestReadPreview.decide(request, replies().reverse))

  @Test def capacityAndSubmissionReceiptAreBoundedOnce(): Unit =
    val q = ledger(1)
    val entry = q.begin(tag(), request, 0L).get
    assertTrue(q.begin(tag(2L), request, 0L).isEmpty)
    q.submitted(entry, IoSubmission.RejectedCapacity)
    assertThrows(classOf[IllegalArgumentException], () => q.submitted(entry, IoSubmission.Accepted))
    q.discardNotAccepted(entry)
    assertEquals(0, q.retained)

  @Test def actualMainMustConsumeBeforeLaterEndDrop(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    q.end(2L, 0L, Vector.empty)
    assertTrue(q.releaseCandidates.isEmpty)
    assertTrue(q.result(tag(), 0L).nonEmpty)
    assertEquals(Vector(entry), q.releaseCandidates)
    q.dropped(entry)
    assertTrue(entry.completion.isEmpty)
    assertTrue(q.released(entry, IoCompletion(tag(), Success(MySqlOwnedIoReply.RetainedPlanReleased(tag())))))
    assertEquals(0, q.retained)

  @Test def sameEndCannotDropAndRetiredCompletionRetainsWholeReply(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    assertTrue(q.result(tag(), 0L).nonEmpty)
    assertTrue(q.releaseCandidates.isEmpty)
    q.retire()
    q.end(2L, 0L, Vector.empty)
    assertTrue(q.result(tag(), 0L).isEmpty)
    assertTrue(q.releaseCandidates.isEmpty)
    assertTrue(entry.completion.nonEmpty)

  @Test def changedRepositoryEpochRetainsOriginalCompletion(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 1L).get
    q.submitted(entry, IoSubmission.Accepted)
    val original = completion()
    q.end(1L, 2L, Vector(original))
    assertSame(original, entry.completion.get)
    assertTrue(q.result(tag(), 2L).isEmpty)
    assertTrue(q.releaseCandidates.isEmpty)
    assertEquals(1, q.retained)

  @Test def epochChangeBetweenEndAndConsumptionCannotPublish(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    assertTrue(q.result(tag(), 1L).isEmpty)
    assertTrue(entry.failure.nonEmpty)
    q.end(2L, 1L, Vector.empty)
    assertTrue(q.releaseCandidates.isEmpty)

  @Test def originalFailureAndOrderedSecondaryStaySameObjects(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    val primary = new IllegalStateException("primary")
    val stack = primary.getStackTrace
    val secondary = new IllegalArgumentException("secondary")
    q.end(1L, 0L, Vector(IoCompletion(tag(), Failure(primary))))
    q.fail(entry, secondary)
    assertSame(primary, entry.failure.get)
    assertSame(secondary, primary.getSuppressed.head)
    assertTrue(stack.sameElements(primary.getStackTrace))
    assertTrue(q.releaseCandidates.isEmpty)

  @Test def fullBatchIsRetainedBeforeOneMalformedDecision(): Unit =
    val q = ledger()
    val first = q.begin(tag(), request, 0L).get
    val second = q.begin(tag(2L), request, 0L).get
    q.submitted(first, IoSubmission.Accepted); q.submitted(second, IoSubmission.Accepted)
    val bad = completion(values = replies().reverse)
    val good = completion(tag(2L))
    q.end(1L, 0L, Vector(bad, good))
    assertSame(bad, first.completion.get)
    assertSame(good, second.completion.get)
    assertTrue(first.failure.nonEmpty)
    assertTrue(q.result(tag(2L), 0L).nonEmpty)

  @Test def unmatchedGenerationRetiresInsteadOfDroppingResources(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    val foreign = completion(tag().copy(connectionGeneration = 4L))
    q.end(1L, 0L, Vector(foreign))
    assertSame(foreign, q.unmatchedCompletions.head)
    assertTrue(q.result(tag(), 0L).isEmpty)
    assertEquals(1, q.retained)

  @Test def wrongThreadCannotConsumeOrRetireMainLedger(): Unit =
    val q = ledger()
    val failure = new AtomicReference[Throwable]()
    val child = new Thread(() => {
      try q.retire()
      catch case cause: Throwable => failure.set(cause)
    }, "request-read-wrong-main")
    child.start(); child.join(5000L)
    assertFalse(child.isAlive)
    assertTrue(failure.get().isInstanceOf[IllegalArgumentException])

  @Test def actualOwnedSerialWorkerDeliversImmutablePreviewBackToOriginalMain(): Unit =
    val main = Thread.currentThread()
    val worker = new AtomicReference[Thread]()
    val done = new CountDownLatch(1)
    val io = new OwnedSerialIo[MySqlRequestReadPreview, MySqlOwnedIoReply](1, main, "preview-data-worker",
      completionSignal = Some(done))(value => {
      worker.set(Thread.currentThread())
      assertNotSame(main, Thread.currentThread())
      assertEquals(request.operations, value.operations)
      MySqlOwnedIoReply.Transaction(replies())
    })
    var primary: Throwable = null
    try
      val q = ledger(1)
      val entry = q.begin(tag(), request, 0L).get
      val submission = io.submit(tag(), request)
      q.submitted(entry, submission)
      assertEquals(IoSubmission.Accepted, submission)
      assertTrue(done.await(5L, TimeUnit.SECONDS))
      q.end(1L, 0L, io.drain(1))
      assertEquals(Some(MySqlRequestReadDecision(MySqlRequestOwnershipPreview.Matched(4L, false), 0, 1)),
        q.result(tag(), 0L))
      assertNotNull(worker.get())
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      io.quiesce()
      val cleanup = new AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try assertTrue(io.awaitExitOffMain(5L, TimeUnit.SECONDS), "actual owned worker did not exit")
        catch case cause: Throwable => cleanup.set(cause)
      }, "preview-data-worker-exit")
      waiter.start(); waiter.join(6000L)
      val cause = if waiter.isAlive then new IllegalStateException("exit observer still owns real worker wait")
        else cleanup.get()
      if cause != null then
        if primary != null then primary.addSuppressed(cause) else throw cause

  @Test def releaseFailurePreservesCompletionCauseAndEntry(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    q.result(tag(), 0L)
    q.end(2L, 0L, Vector.empty)
    q.dropped(entry)
    val cause = new IllegalStateException("actual release failure")
    val failed = IoCompletion(tag(), Success(MySqlOwnedIoReply.RetainedPlanReleaseFailed(tag(), cause)))
    assertFalse(q.released(entry, failed))
    assertSame(failed, entry.completion.get)
    assertSame(cause, entry.failure.get)
    assertEquals(1, q.retained)
  @Test def consumedPreviewNextEmptyEndChecksEpochWithoutDecisionReread(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    val original = completion()
    q.end(1L, 0L, Vector(original))
    assertTrue(q.result(tag(), 0L).nonEmpty)
    q.observeEndEpoch(1L)
    q.end(2L, 1L, Vector.empty)
    assertTrue(q.releaseCandidates.isEmpty)
    assertSame(original, entry.completion.get)
    assertFalse(entry.releasing)
    assertFalse(entry.workerReleaseAcknowledged)
    assertEquals(1, q.retained)
    assertTrue(entry.failure.get.getStackTrace.nonEmpty)

  @Test def epochAfterDropBlocksReceiptAndReleaseWithoutPretendingReplyRestored(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    assertTrue(q.result(tag(), 0L).nonEmpty)
    q.end(2L, 0L, Vector.empty)
    q.dropped(entry)
    assertTrue(entry.releasing)
    assertTrue(entry.completion.isEmpty)
    assertFalse(q.requireCurrentEpoch(entry, 1L))
    val primary = entry.failure.get
    q.observeEndEpoch(1L)
    assertSame(primary, entry.failure.get)
    assertTrue(entry.completion.isEmpty)
    assertFalse(entry.workerReleaseAcknowledged)
    assertEquals(1, q.retained)

  @Test def acceptedReleaseLateStaleAckRetainsActualWorkerReleaseFactAndLineage(): Unit =
    val q = ledger()
    val entry = q.begin(tag(), request, 0L).get
    q.submitted(entry, IoSubmission.Accepted)
    q.end(1L, 0L, Vector(completion()))
    assertTrue(q.result(tag(), 0L).nonEmpty)
    q.end(2L, 0L, Vector.empty)
    q.dropped(entry)
    assertTrue(q.requireCurrentEpoch(entry, 0L))
    assertFalse(q.requireCurrentEpoch(entry, 1L))
    val primary = entry.failure.get
    val ack = IoCompletion(tag(), Success(MySqlOwnedIoReply.RetainedPlanReleased(tag())))
    assertFalse(q.released(entry, ack))
    assertTrue(entry.workerReleaseAcknowledged)
    assertSame(ack, entry.completion.get)
    assertSame(primary, entry.failure.get)
    assertEquals(1, q.retained)
    assertTrue(q.releaseCandidates.isEmpty)
  @Test def preStopSettlementRejectsUnmatchedAndFailedCancelledOrAckedEntries(): Unit =
    val unmatched = ledger()
    unmatched.end(1, 0, Vector(completion()))
    assertThrows(classOf[IllegalArgumentException], () => unmatched.requireSettledForPreStop())
    val failed = ledger()
    val entry = failed.begin(tag(), request, 0).get
    val cause = new IllegalStateException("original rejected cause")
    failed.submitted(entry, IoSubmission.RejectedStopped(Some(cause)))
    assertThrows(classOf[IllegalArgumentException], () => failed.discardNotAccepted(entry))
    assertSame(cause, entry.failure.get)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => failed.requireSettledForPreStop()))
    val cancelled = ledger()
    val original = cancelled.begin(tag(), request, 0).get
    cancelled.submitted(original, IoSubmission.RejectedCapacity)
    assertThrows(classOf[IllegalArgumentException], () => cancelled.requireSettledForPreStop())
    cancelled.discardNotAccepted(original)
    cancelled.requireSettledForPreStop()
    cancelled.retire()
    assertThrows(classOf[IllegalArgumentException], () => cancelled.requireSettledForPreStop())

  @Test def pendingAndStrictShareReadLedgerFailureAndActualAck(): Unit =
    val core = ledger()
    assertFalse(core.pendingForPreStop)
    val entry = core.begin(tag(), request, 0L).get
    core.submitted(entry, IoSubmission.Accepted)
    assertTrue(core.pendingForPreStop)
    val cause = new IllegalStateException("actual retained read failure")
    core.fail(entry, cause)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.pendingForPreStop))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.requireSettledForPreStop()))
    assertEquals(1, core.retained)
