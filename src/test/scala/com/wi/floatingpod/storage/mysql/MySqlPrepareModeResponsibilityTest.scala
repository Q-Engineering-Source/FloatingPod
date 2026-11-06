package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.*
import com.wi.floatingpod.config.SourceLimitProfile
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

/** 真实adapter preBoot拒绝/owned线程和immutable责任；没有fake JDBC admission/SQL故障注入。 */
class MySqlPrepareModeResponsibilityTest:
  import MySqlPrepareModeGraphFixture.*
  private val limits = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, 1048576L, 1048576L, 1048576L)
  private def plan(tag: IoOperationTag, operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    MySqlOwnedRepositoryOperationPlan.single(tag, UUID.randomUUID(), UUID.randomUUID(), operation, limits)

  private def observeExit(wait: () => Boolean): Unit =
    val failure = new AtomicReference[Throwable]()
    val helper = new Thread(() =>
      try assertTrue(wait(), "actual worker exit deadline expired")
      catch case cause: Throwable => failure.set(cause)
      (), "prepare-mode-offMain-exit")
    var primary: Throwable = null
    try
      helper.start()
      helper.join(6000)
      assertFalse(helper.isAlive, "offMain exit observer deadline expired")
      if failure.get() != null then throw failure.get()
    catch case cause: Throwable => primary = cause
    finally
      if helper.isAlive then helper.interrupt()
      try
        helper.join(6000)
        assertFalse(helper.isAlive, "offMain exit observer leaked")
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  private def managed(quiesce: () => Unit, exit: () => Boolean, release: () => Unit = () => ())
      (body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      try release()
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      try quiesce()
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      try observeExit(exit)
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  @Test def actualAdapterFixedCommandsRejectPreBootWithExactCompletionReservations(): Unit =
    val identity = MySqlOwnedIoIdentity(tag.bootId, 1L, 1L)
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "prepare-mode-actual-adapter", identity)
    managed(() => adapter.quiesce(), () => adapter.awaitExitOffMain(5, TimeUnit.SECONDS)) {
      val prepare = prepared().request; val modeRequest = mode().request
      val first = tag; val second = tag.copy(operationId = id(500))
      // public single 只是 DATA plan；没有实际 Boot/session issuer，业务不得进入 owned queue。
      val foreign = first.copy(operationId = id(499))
      for candidateTag <- Vector(first, foreign) do
        val prepareFailure = assertThrows(classOf[IllegalArgumentException], () =>
          adapter.submit(first, MySqlOwnedIoCommand.PrepareControlledUnbound(prepare, plan(candidateTag, prepare.repositoryOperation))))
        assertEquals("requirement failed: business submit requires its actual session-issued original plan", prepareFailure.getMessage)
        assertNull(prepareFailure.getCause)
        assertTrue(prepareFailure.getStackTrace.nonEmpty)
        val modeFailure = assertThrows(classOf[IllegalArgumentException], () =>
          adapter.submit(first, MySqlOwnedIoCommand.CommitOwnedMode(modeRequest, plan(candidateTag, modeRequest.repositoryOperation))))
        assertEquals("requirement failed: business submit requires its actual session-issued original plan", modeFailure.getMessage)
        assertNull(modeFailure.getCause)
        assertTrue(modeFailure.getStackTrace.nonEmpty)
      val reserveFailure = assertThrows(classOf[IllegalArgumentException], () =>
        adapter.reserveRepositoryPlan(first, UUID.randomUUID(), UUID.randomUUID(), prepare.repositoryOperation))
      assertEquals("requirement failed: actual Boot/session admission is unavailable or expired; no new plan", reserveFailure.getMessage)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertEquals(0, adapter.checkpoint.io.queued)
      assertEquals(0, adapter.checkpoint.io.completions)
      assertTrue(adapter.drain(2).isEmpty)
      assertTrue(adapter.checkpoint.resources.isEmpty)
      assertTrue(adapter.checkpoint.retainedAllocation.isEmpty)
      // Renew 是合法非业务 immutable command；真实 worker 在 preBoot 以 unavailable 结束它。
      assertEquals(IoSubmission.Accepted, adapter.submit(first, MySqlOwnedIoCommand.Renew))
      assertEquals(IoSubmission.Accepted, adapter.submit(second, MySqlOwnedIoCommand.Renew))
      assertEquals(2, adapter.checkpoint.io.reserved)
      assertEquals(IoSubmission.RejectedCapacity, adapter.submit(tag.copy(operationId = id(501)), MySqlOwnedIoCommand.Renew))
      adapter.quiesce()
      observeExit(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(2)
      assertEquals(2, completions.size)
      assertSame(first, completions.head.tag)
      assertSame(second, completions(1).tag)
      assertTrue(completions.forall(_.result.isFailure))
      completions.foreach(value => assertEquals("normal owned MySQL session is unavailable before successful Boot", value.result.failed.get.getMessage))
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.checkpoint.io.stopped)
      assertTrue(adapter.checkpoint.io.fatalCause.isEmpty)
      assertTrue(adapter.checkpoint.retainedAllocation.isEmpty)
      assertTrue(adapter.checkpoint.preparedAfterAckFailure.isEmpty)
      assertTrue(adapter.checkpoint.ownedModeExecutionFailure.isEmpty)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
    }

  @Test def actualImmutableAckResponsibilityAndUnknownCauseAreNeverReplaced(): Unit =
    val data = prepared(); val changed = mode()
    val original = new IllegalStateException("readback failure")
    val preparation = MySqlUnboundAfterPrepareReadbackFailure(tag, data.request, data.receipt, original)
    assertSame(data.receipt, preparation.actualReceipt)
    assertSame(original, preparation.cause)
    val observed = MySqlOwnedModeExecutionFailure(tag, changed.request, Some(changed.outcome), original)
    assertSame(changed.outcome, observed.actualAck.get)
    assertSame(changed.request, observed.request)
    val context = MySqlOperationContext(tag.bootId, 4L, binding.bindingUuid, tag.operationId,
      Vector(MySqlExpectedEffect("mode", changed.request.expectedStateVersion, Vector.fill(32)(1.toByte))))
    val unknown = MySqlCommitUnknownException(context, original) // only actual exception contract, no SQL execution.
    val unobserved = MySqlOwnedModeExecutionFailure(tag, changed.request, None, unknown)
    assertTrue(unobserved.actualAck.isEmpty)
    assertSame(unknown, unobserved.cause)
    assertSame(original, unobserved.cause.getCause)
    val notice = new MySqlOwnedModeObligationNotice(tag, changed.request, None)
    unknown.addSuppressed(notice)
    assertNull(notice.getCause)
    assertSame(notice, unknown.getSuppressed()(0))
    assertThrows(classOf[IllegalArgumentException], () => MySqlOwnedModeExecutionFailure(tag, changed.request,
      Some(com.wi.floatingpod.storage.OperatingModeChangeOutcome.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.NotOwner)), original))

  @Test def actualOwnedFatalRetainsAckContextAndEveryAcceptedPendingCompletion(): Unit =
    val data = prepared()
    val entered = new CountDownLatch(1); val release = new CountDownLatch(1)
    val responsibility = new AtomicReference[MySqlUnboundAfterPrepareReadbackFailure]()
    val original = new LinkageError("actual test worker fatal")
    val io = new OwnedSerialIo[MySqlControlledUnboundPrepareRequest, MySqlOwnedIoReply](2,
      Thread.currentThread(), "prepare-mode-fatal-worker")(_ =>
      entered.countDown()
      assertTrue(release.await(3, TimeUnit.SECONDS), "fatal worker input release deadline expired")
      val retained = MySqlUnboundAfterPrepareReadbackFailure(tag, data.request, data.receipt, original)
      responsibility.set(retained)
      original.addSuppressed(new MySqlUnboundPrepareObligationNotice(tag, data.request, data.receipt))
      throw original)
    managed(() => io.quiesce(), () => io.awaitExitOffMain(5, TimeUnit.SECONDS), () => release.countDown()) {
      assertEquals(IoSubmission.Accepted, io.submit(tag, data.request))
      assertTrue(entered.await(3, TimeUnit.SECONDS), "actual worker did not enter")
      val second = tag.copy(operationId = id(600))
      assertEquals(IoSubmission.Accepted, io.submit(second, data.request))
      assertEquals(IoSubmission.RejectedCapacity, io.submit(tag.copy(operationId = id(601)), data.request))
      release.countDown()
      observeExit(() => io.awaitExitOffMain(5, TimeUnit.SECONDS))
      assertSame(original, io.checkpoint.fatalCause.get)
      assertSame(original, responsibility.get().cause)
      assertSame(data.receipt, responsibility.get().actualReceipt)
      val completed = io.drain(2)
      assertEquals(Vector(tag, second), completed.map(_.tag))
      completed.foreach(value => assertSame(original, value.result.failed.get))
      assertEquals(0, io.checkpoint.reserved)
      assertEquals(IoSubmission.RejectedStopped(Some(original)), io.submit(tag.copy(operationId = id(602)), data.request))
    }
