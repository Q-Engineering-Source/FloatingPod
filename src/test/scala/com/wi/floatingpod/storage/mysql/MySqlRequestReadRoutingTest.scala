package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.util.Success

/** 真实router/command结构和真实adapter无issuer拒绝；不构造私有SQL/native能力。 */
class MySqlRequestReadRoutingTest:
  private val identity = MySqlOwnedIoIdentity(new UUID(1L, 1L), 2L, 3L)
  private def tag(n: Long) = IoOperationTag(identity.bootId, 2L, 3L, new UUID(2L, n))
  private def request = MySqlRequestReadPreview(new UUID(3L, 1L), new UUID(4L, 1L), 0L)
  private def profile = SourceLimitProfile.of(8388608L, 1048576L, 8388608L, 64L, 256L,
    8388608L, 1048576L, 8388608L)

  @Test def sharedRouterPartitionsReadsWithoutStealingOrdinaryCompletion(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 2)
    val read = tag(1L); val ordinary = tag(2L)
    assertTrue(router.begin(read, MySqlCompletionChannel.RequestRead, true))
    assertTrue(router.begin(ordinary, MySqlCompletionChannel.Ordinary, false))
    router.submitted(read, IoSubmission.Accepted); router.submitted(ordinary, IoSubmission.Accepted)
    val readCompletion = IoCompletion(read, Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    val ordinaryCompletion = IoCompletion(ordinary, Success(MySqlOwnedIoReply.Operation(MySqlRepositoryReply.Deleted(false))))
    router.routeBatch(Vector(readCompletion, ordinaryCompletion))
    assertEquals(Vector(ordinaryCompletion), router.take(MySqlCompletionChannel.Ordinary, 2))
    assertEquals(Vector(readCompletion), router.take(MySqlCompletionChannel.RequestRead, 2))
    assertEquals(1, router.retainedCount)
    router.beginRelease(read, MySqlCompletionChannel.RequestRead)
    router.submitted(read, IoSubmission.Accepted)
    val released = IoCompletion(read, Success(MySqlOwnedIoReply.RetainedPlanReleased(read)))
    router.routeBatch(Vector(released))
    assertEquals(Vector(released), router.take(MySqlCompletionChannel.RequestRead, 1))
    assertEquals(0, router.retainedCount)

  @Test def releaseCannotSwitchPartitionOrForgeOriginalTag(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    val original = tag(1L)
    assertTrue(router.begin(original, MySqlCompletionChannel.RequestRead, true))
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion(original, Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
    router.take(MySqlCompletionChannel.RequestRead, 1)
    assertThrows(classOf[IllegalArgumentException], () => router.beginRelease(original, MySqlCompletionChannel.Ordinary))
    router.beginRelease(original, MySqlCompletionChannel.RequestRead)
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion(original, Success(MySqlOwnedIoReply.RetainedPlanReleased(tag(2L))))))
    router.take(MySqlCompletionChannel.RequestRead, 1)
    assertEquals(1, router.retainedCount)
    assertTrue(router.failure.nonEmpty)

  @Test def readCommandPreservesExactThreeVectorAndDoesNotRetireBootEpoch(): Unit =
    val actual = profile
    val plan = MySqlOwnedRepositoryOperationPlan.transaction(tag(1L), new UUID(5L, 1L),
      new UUID(6L, 1L), request.operations, actual)
    val command = MySqlOwnedIoCommand.ReadRequestPreview(request, plan)
    assertSame(plan, command.requireRepositoryPlan(tag(1L), actual))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(command))
    val wrong = MySqlOwnedRepositoryOperationPlan.transaction(tag(1L), new UUID(5L, 1L),
      new UUID(6L, 1L), request.operations.reverse, actual)
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlOwnedIoCommand.ReadRequestPreview(request, wrong).requireRepositoryPlan(tag(1L), actual))

  @Test def actualAdapterRejectsPublicDataPlanBeforeQueueOrJdbc(): Unit =
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "preview-unissued-plan", identity)
    var primary: Throwable = null
    try
      val plan = MySqlOwnedRepositoryOperationPlan.transaction(tag(1L), new UUID(5L, 1L),
        new UUID(6L, 1L), request.operations, profile)
      assertThrows(classOf[IllegalArgumentException], () => adapter.submit(tag(1L),
        MySqlOwnedIoCommand.ReadRequestPreview(request, plan)))
      assertEquals(0, adapter.checkpoint.io.queued)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertEquals(0, adapter.retainedCompletionCount)
      assertTrue(adapter.checkpoint.resources.isEmpty)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      adapter.quiesce()
      val failure = new AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try assertTrue(adapter.awaitExitOffMain(5L, TimeUnit.SECONDS))
        catch case cause: Throwable => failure.set(cause)
      }, "preview-unissued-plan-exit")
      waiter.start()
      waiter.join(6000L)
      val cause = if waiter.isAlive then new IllegalStateException("actual exit waiter retains IO responsibility")
        else failure.get()
      if cause != null then
        if primary == null then throw cause else primary.addSuppressed(cause)

  @Test def releaseWithoutActualDropReceiptCannotBorrowQuerySlot(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    assertThrows(classOf[IllegalStateException], () => router.beginRelease(tag(1L), MySqlCompletionChannel.RequestRead))
    assertEquals(0, router.retainedCount)