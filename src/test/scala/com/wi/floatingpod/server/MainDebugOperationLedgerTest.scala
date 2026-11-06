package com.wi.floatingpod.server

import com.wi.floatingpod.debug.DebugItemEndpoint
import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.*
import com.wi.floatingpod.storage.mysql.*
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.util.{Success, Failure}

/** 直接production ledger + real OwnedSerialIo；不注入JDBC成功，不声明Ready/Forge/GPU。 */
class MainDebugOperationLedgerTest:
  private def id(n: Long): UUID = new UUID(0L, n)
  private val limits = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, 1048576L, 1048576L, 1048576L)
  private val identity = MySqlOwnedIoIdentity(id(1), 1L, 1L)
  private def request(n: Long): DebugItemEndpoint.Request = new DebugItemEndpoint.Request(
    new DebugItemEndpoint.Identity(id(1), id(2), id(n + 20), id(4), "ledger-world", 7, -1, null, null),
    1L, id(n), DebugItemEndpoint.Action.PLACE, new DebugItemEndpoint.Position(8.5, 64, 8.5, 0F),
    new DebugItemEndpoint.PlacementContext(8, 63, 8, DebugItemEndpoint.PlacementFace.UP))
  private def tag(n: Long): IoOperationTag = IoOperationTag(id(1), 1L, 1L, id(n))
  private def result(n: Long): IoCompletion[MySqlOwnedIoReply] =
    IoCompletion(tag(n), Success(MySqlOwnedIoReply.Transaction(Vector.empty)))

  private def plannedSubmit(ledger: MainDebugOperationLedger)(entry: ledger.Entry,
      tag: IoOperationTag, result: => IoSubmission, stage: MainDebugStage): Boolean =
    val workflow = entry.planSnapshot.headOption.map(_.workflowUuid).getOrElse(UUID.randomUUID())
    ledger.reserveRepositoryPlan(entry, stage, workflow, tag, UUID.randomUUID(), MySqlRepositoryOperation.FindAll)
    ledger.requirePlannedSubmission(entry, tag, stage)
    val actual = result // 实际OwnedSerialIo.submit在所有production检查之后求值。
    ledger.submitted(entry, tag, actual, stage)

  @Test def consoleUsesSameBoundedOperationResponsibilityAndRejectsForeignBoot(): Unit =
    val ledger=new MainDebugOperationLedger(identity,Thread.currentThread(),1,limits)
    val console=new com.wi.floatingpod.debug.MainPodOperationRequest.ConsoleCreate(id(1),id(10),"ledger-world",7,
      new DebugItemEndpoint.Position(8.5,64,8.5,0F))
    val entry=ledger.reserve(console).get
    assertSame(console,entry.request)
    assertTrue(ledger.reserve(request(11)).isEmpty,"same real capacity applies to both inputs")
    assertTrue(plannedSubmit(ledger)(entry,tag(10),IoSubmission.Accepted,MainDebugStage.Preparing))
    val prepared=result(10)
    ledger.retainBatch(Vector(prepared));ledger.claimPendingCompletion(entry);ledger.prepared(entry)
    plannedSubmit(ledger)(entry,tag(11),IoSubmission.Accepted,MainDebugStage.Committing)
    ledger.retainBatch(Vector(result(11)));ledger.published(entry);ledger.acknowledged(entry)
    assertTrue(ledger.reserve(console).isEmpty,"completed console operation cannot replay")
    assertThrows(classOf[IllegalArgumentException],()=>ledger.reserve(
      new com.wi.floatingpod.debug.MainPodOperationRequest.ConsoleCreate(id(999),id(12),"ledger-world",7,console.placement())))

  @Test def actualAcceptedWorkerCompletionReleasesReservationOnlyAtOwnerDrain(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    val signal = new CountDownLatch(1)
    val io = new OwnedSerialIo[Int, MySqlOwnedIoReply](1, Thread.currentThread(), "debug-ledger-real", None, Some(signal))(
      _ => MySqlOwnedIoReply.Transaction(Vector.empty))
    var primary: Throwable = null
    try
      assertTrue(plannedSubmit(ledger)(entry, tag(10), io.submit(tag(10), 1), MainDebugStage.Preparing))
      assertTrue(signal.await(3, TimeUnit.SECONDS), "actual worker completion timeout")
      assertEquals(1, io.checkpoint.reserved)
      val batch = io.drain(1)
      val retained = ledger.retainBatch(batch)
      assertSame(entry, retained.head)
      assertSame(batch.head, entry.completionSnapshot.head)
      assertEquals(0, io.checkpoint.reserved)
      assertEquals(MainDebugStage.Preparing, entry.stage, "Accepted/completion is not committed publication")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally cleanup(io, primary)

  @Test def capacityRejectionHasNoAcceptedPrepareResponsibility(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    assertFalse(plannedSubmit(ledger)(entry, tag(10), IoSubmission.RejectedCapacity, MainDebugStage.Preparing))
    assertEquals(MainDebugStage.NotAccepted, entry.stage)
    ledger.releaseNotAccepted(entry)
    assertEquals(0, ledger.retainedCount)

  @Test def completePrepareThenCommitKeepsBothExactCompletionsUntilFeedback(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    val prepared = result(10)
    ledger.retainBatch(Vector(prepared)); ledger.claimPendingCompletion(entry); ledger.prepared(entry)
    plannedSubmit(ledger)(entry, tag(11), IoSubmission.Accepted, MainDebugStage.Committing)
    val committed = result(11)
    ledger.retainBatch(Vector(committed))
    assertEquals(Vector(prepared, committed), entry.completionSnapshot)
    ledger.published(entry); ledger.acknowledged(entry)
    assertEquals(0, ledger.retainedCount)
    assertTrue(ledger.reserve(request(10)).isEmpty, "exact completed operation replay must not create new business")

  @Test def afterPrepareCommitCapacityRejectionRetainsOriginalReceiptCompletion(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    val completion = result(10)
    ledger.retainBatch(Vector(completion)); ledger.claimPendingCompletion(entry); ledger.prepared(entry)
    assertFalse(plannedSubmit(ledger)(entry, tag(11), IoSubmission.RejectedCapacity, MainDebugStage.Committing))
    assertEquals(MainDebugStage.RetainedFailure, entry.stage)
    assertSame(completion, entry.completionSnapshot.head)
    assertThrows(classOf[IllegalArgumentException], () => ledger.releaseNotAccepted(entry))
    assertTrue(ledger.reserve(request(12)).isEmpty, "unresolved physical/receipt responsibility consumes bound")

  @Test def wholeBatchIsRetainedBeforeAnyPublicationFailure(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 2, limits)
    val a = ledger.reserve(request(10)).get; val b = ledger.reserve(request(11)).get
    plannedSubmit(ledger)(a, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    plannedSubmit(ledger)(b, tag(11), IoSubmission.Accepted, MainDebugStage.Preparing)
    val first = result(10); val second = result(11)
    val entries = ledger.retainBatch(Vector(first, second))
    val original = new IllegalStateException("physical publication failed")
    ledger.fail(entries.head, original)
    assertSame(original, a.cause.get)
    assertSame(second, b.completionSnapshot.head)
    assertEquals(2, ledger.retainedCount)

  @Test def unknownAndDuplicateCompletionRetireAndRetainExactObject(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 2, limits)
    val entry = ledger.reserve(request(10)).get
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    val original = result(10)
    ledger.retainBatch(Vector(original))
    ledger.retainBatch(Vector(original))
    assertTrue(ledger.isRetired)
    assertSame(original, ledger.unmatchedCompletions.head)
    assertTrue(ledger.reserve(request(11)).isEmpty)

  @Test def lateCompletionAfterStopRetainsCauseAndCannotPublish(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    ledger.retire()
    val primary = new IllegalStateException("late failure")
    val completed = IoCompletion[MySqlOwnedIoReply](tag(10), Failure(primary))
    ledger.retainBatch(Vector(completed)); ledger.fail(entry, primary)
    assertEquals(MainDebugStage.Retired, entry.stage)
    assertSame(primary, entry.cause.get)
    assertSame(completed, entry.completionSnapshot.head)
    assertThrows(classOf[IllegalArgumentException], () => ledger.published(entry))

  @Test def originalFailureAndCleanupSuppressedAreNotReplaced(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    val primary = new IllegalStateException("primary"); val secondary = new IllegalStateException("cleanup")
    ledger.fail(entry, primary); ledger.fail(entry, secondary)
    assertSame(primary, entry.cause.get)
    assertSame(secondary, primary.getSuppressed.head)

  @Test def existingCommittedHistoryRetainsWithoutInventingUnexpectedFailure(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    ledger.retainBatch(Vector(result(10))); ledger.requireRecovery(entry)
    assertEquals(MainDebugStage.RetainedFailure, entry.stage)
    assertTrue(entry.cause.isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => ledger.releaseNotAccepted(entry))

  @Test def foreignGenerationNeverAcquiresOutstandingOwnerSlot(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    assertThrows(classOf[IllegalArgumentException], () =>
      plannedSubmit(ledger)(entry, tag(10).copy(connectionGeneration = 2), IoSubmission.Accepted, MainDebugStage.Preparing))
    assertEquals(MainDebugStage.Reserved, entry.stage)

  @Test def wrongThreadCannotReserveOrDrainLedger(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val failure = new AtomicReference[Throwable]()
    val helper = new Thread(() =>
      try ledger.reserve(request(10))
      catch case cause: Throwable => failure.set(cause)
      (), "debug-ledger-wrong-owner")
    helper.start()
    try
      helper.join(3000)
      assertFalse(helper.isAlive, "wrong-owner helper did not terminate")
      assertTrue(failure.get().isInstanceOf[IllegalArgumentException])
      assertEquals(0, ledger.retainedCount)
    finally
      if helper.isAlive then helper.interrupt()
      helper.join(3000)
      assertFalse(helper.isAlive)

  @Test def realFatalWorkerKeepsEveryAcceptedPendingCause(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 2, limits)
    val entered = new CountDownLatch(1); val release = new CountDownLatch(1); val signal = new CountDownLatch(1)
    val original = new LinkageError("actual worker fatal")
    val io = new OwnedSerialIo[Int, MySqlOwnedIoReply](2, Thread.currentThread(), "debug-ledger-fatal", None, Some(signal))(
      _ => { entered.countDown(); if !release.await(3, TimeUnit.SECONDS) then throw new AssertionError("release timeout"); throw original })
    var primary: Throwable = null
    try
      val a = ledger.reserve(request(10)).get; val b = ledger.reserve(request(11)).get
      plannedSubmit(ledger)(a, tag(10), io.submit(tag(10), 1), MainDebugStage.Preparing)
      assertTrue(entered.await(3, TimeUnit.SECONDS))
      plannedSubmit(ledger)(b, tag(11), io.submit(tag(11), 2), MainDebugStage.Preparing)
      release.countDown(); assertTrue(signal.await(3, TimeUnit.SECONDS))
      ledger.retainBatch(io.drain(2))
      assertSame(original, a.completionSnapshot.head.result.failed.get)
      assertSame(original, b.completionSnapshot.head.result.failed.get)
      assertEquals(2, ledger.retainedCount)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      release.countDown()
      cleanup(io, primary)

  @Test def commitCannotSkipPrepareAndPrepareCompletionCannotBePublication(): Unit =
    val ledger = new MainDebugOperationLedger(identity, Thread.currentThread(), 1, limits)
    val entry = ledger.reserve(request(10)).get
    assertThrows(classOf[IllegalArgumentException], () =>
      plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Committing))
    plannedSubmit(ledger)(entry, tag(10), IoSubmission.Accepted, MainDebugStage.Preparing)
    ledger.retainBatch(Vector(result(10)))
    assertThrows(classOf[IllegalArgumentException], () => ledger.published(entry))
    assertEquals(MainDebugStage.Preparing, entry.stage)

  private def cleanup(io: OwnedSerialIo[Int, MySqlOwnedIoReply], primary: Throwable): Unit =
    var problem: Throwable = null
    try io.quiesce()
    catch case cause: Throwable => problem = cause
    val failure = new AtomicReference[Throwable](); val exited = new AtomicReference[java.lang.Boolean]()
    val observer = new Thread(() =>
      try exited.set(io.awaitExitOffMain(3, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      (), "debug-ledger-exit-observer")
    try
      observer.start(); observer.join(4000)
      assertFalse(observer.isAlive, "off-Main observer leaked")
      if failure.get() != null then throw failure.get()
      assertEquals(java.lang.Boolean.TRUE, exited.get(), "owned worker did not actually exit")
    catch case cause: Throwable => if problem == null then problem = cause else problem.addSuppressed(cause)
    finally
      if observer.isAlive then observer.interrupt()
      observer.join(4000)
      if observer.isAlive then
        val leak = new AssertionError("exit observer cleanup failed")
        if problem == null then problem = leak else problem.addSuppressed(leak)
    if problem != null then
      if primary != null then primary.addSuppressed(problem) else throw problem

  @Test def deferredArrivalsResumeInActualOrderWithBoundedOneShotClaims(): Unit =
    val l = new MainDebugOperationLedger(identity, Thread.currentThread(), 3, limits)
    val first = l.reserve(request(101)).get
    val second = l.reserve(request(102)).get
    val third = l.reserve(request(103)).get
    plannedSubmit(l)(first, tag(101), IoSubmission.Accepted, MainDebugStage.Preparing)
    plannedSubmit(l)(second, tag(102), IoSubmission.Accepted, MainDebugStage.Preparing)
    plannedSubmit(l)(third, tag(103), IoSubmission.Accepted, MainDebugStage.Preparing)
    l.retainBatch(Vector(result(102), result(101)))
    l.retainBatch(Vector(result(103)))
    assertEquals(Vector(second), l.pendingCompletionEntries(1))
    assertEquals(tag(102), l.claimPendingCompletion(second).tag)
    assertEquals(1, second.completionSnapshot.size)
    assertEquals(1, second.planSnapshot.size)
    assertThrows(classOf[IllegalArgumentException], () => l.claimPendingCompletion(second))
    assertEquals(Vector(first, third), l.pendingCompletionEntries(2))
    l.claimPendingCompletion(first)
    val cause = new IllegalStateException("actual deferred failure")
    l.fail(third, cause)
    assertTrue(l.pendingCompletionEntries(3).isEmpty)
    assertSame(cause, third.cause.get)
    assertEquals(3, l.retainedCount)

  @Test def claimedPrepareCanReceiveItsOriginalCommitWithoutReplayingPrepare(): Unit =
    val l = new MainDebugOperationLedger(identity, Thread.currentThread(), 2, limits)
    val entry = l.reserve(request(104)).get
    plannedSubmit(l)(entry, tag(104), IoSubmission.Accepted, MainDebugStage.Preparing)
    l.retainBatch(Vector(result(104)))
    l.claimPendingCompletion(entry)
    l.prepared(entry)
    plannedSubmit(l)(entry, tag(105), IoSubmission.Accepted, MainDebugStage.Committing)
    l.retainBatch(Vector(result(105)))
    assertEquals(Vector(entry), l.pendingCompletionEntries(1))
    assertEquals(tag(105), l.claimPendingCompletion(entry).tag)
    assertEquals(2, entry.completionSnapshot.size)
    assertTrue(l.pendingCompletionEntries(1).isEmpty)

  @Test def settledMainLedgerDropIsOneShotAndDoesNotAcknowledgeAnOwnedEntry(): Unit =
    val occupied = new MainDebugOperationLedger(identity, Thread.currentThread(), 4, limits)
    occupied.reserve(request(990L)).get
    assertThrows(classOf[IllegalArgumentException], () => occupied.dropSettledMainReferences())
    assertFalse(occupied.isRetired)
    assertEquals(1, occupied.retainedCount)
    val empty = new MainDebugOperationLedger(identity, Thread.currentThread(), 4, limits)
    empty.dropSettledMainReferences()
    assertTrue(empty.isRetired)
    assertTrue(empty.reserve(request(991L)).isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => empty.dropSettledMainReferences())

  @Test def dbFirstReservationFailureRetainsOriginalStageAndDoesNotPoisonSibling():Unit =
    val l=new MainDebugOperationLedger(identity,Thread.currentThread(),2,limits)
    val entry=l.reserve(request(1200)).get
    val adapter=new MySqlOwnedIoAdapter(2,Thread.currentThread(),"ledger-schema6-preboot-rejection",identity)
    var primary:Throwable=null
    try
      val binding=com.wi.floatingpod.storage.PhysicalRecoveryBinding(id(1300),
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val prepare=MySqlControlledDebugPrepareRequest(binding,id(1301),id(1302),id(1303),
        com.wi.floatingpod.domain.FloatingPodPose("ledger-world",8.5,64,8.5,0F,0F,7),id(1200))
      val cause=assertThrows(classOf[IllegalArgumentException],()=>
        l.reserveDbFirstPreparation(adapter,entry,tag(1201),prepare,256))
      assertSame(cause,entry.cause.get)
      assertEquals(MainDebugStage.RetainedFailure,entry.stage)
      assertFalse(l.preparationReadRetryReady(entry))
      assertFalse(l.initialRetirementReady(entry))
      assertFalse(l.preparationFinalReady(entry))
      assertFalse(l.rejectedPreparationReady(entry))
      assertThrows(classOf[IllegalArgumentException],()=>l.settleRejectedPreparation(adapter,entry))
      assertThrows(classOf[IllegalArgumentException],()=>l.detachPreparationPayload(adapter,entry))
      assertThrows(classOf[IllegalArgumentException],()=>l.detachInitialPayload(entry,null))
      assertThrows(classOf[IllegalArgumentException],()=>l.submitPreparationReadHandoff(adapter,entry))
      assertThrows(classOf[IllegalArgumentException],()=>l.consumePreparationReadHandoff(adapter,entry,result(1201)))
      assertThrows(classOf[IllegalArgumentException],()=>l.dbFirstPreparedData(entry))
      assertEquals(Vector((MainDebugStage.Preparing,tag(1201),None)),entry.dbFirstCommandSnapshot)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertThrows(classOf[IllegalArgumentException],()=>l.reserveDbFirstPreparation(adapter,entry,tag(1202),prepare,256))
      assertThrows(classOf[IllegalArgumentException],()=>l.dbFirstCommandForSubmission(entry,tag(1201),MainDebugStage.Preparing))
      assertThrows(classOf[IllegalArgumentException],()=>l.releaseNotAccepted(entry))
      assertThrows(classOf[IllegalArgumentException],()=>l.reserveRepositoryPlan(entry,MainDebugStage.Preparing,
        id(1400),tag(1401),id(1402),MySqlRepositoryOperation.FindAll))
      val sibling=l.reserve(request(1500)).get
      assertTrue(plannedSubmit(l)(sibling,tag(1501),IoSubmission.Accepted,MainDebugStage.Preparing))
      l.retainBatch(Vector(result(1501)));l.claimPendingCompletion(sibling);l.prepared(sibling)
      assertEquals(MainDebugStage.Prepared,sibling.stage)
      assertFalse(l.preparationReadRetryReady(sibling))
      assertFalse(l.initialRetirementReady(sibling))
      assertThrows(classOf[IllegalArgumentException],()=>l.submitPreparationReadHandoff(adapter,sibling))
      assertThrows(classOf[IllegalArgumentException],()=>l.dbFirstPreparedData(sibling))
      assertEquals(MainDebugStage.Prepared,sibling.stage,"legacy DATA cannot grant schema6 handoff")
      assertFalse(l.isRetired)
      assertSame(cause,entry.cause.get)
    catch case cause:Throwable=>primary=cause
    finally
      try
        adapter.quiesce()
        val failure=new AtomicReference[Throwable]()
        val waiter=new Thread(()=>{
          try assertTrue(adapter.awaitExitOffMain(5,TimeUnit.SECONDS))
          catch case cause:Throwable=>failure.set(cause)
        },"ledger-schema6-exit")
        waiter.start();waiter.join(6000)
        assertFalse(waiter.isAlive)
        if failure.get()!=null then throw failure.get()
      catch case cleanup:Throwable=>
        if primary==null then primary=cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)
    if primary!=null then throw primary
