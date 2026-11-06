package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoOperationTag, IoCompletion, IoSubmission}
import scala.util.{Failure, Success}
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class MainOwnedRequestReadBridgeTest:
  private def tag = IoOperationTag(new UUID(1, 2), 1, 2, new UUID(3, 4))
  private def decision(value: MySqlRequestOwnershipPreview) = MySqlRequestReadDecision(value, 0, 1)
  @Test def normalScalarRejectionsRemainLocalAndConsumable(): Unit =
    Vector(MySqlRequestOwnershipPreview.Missing, MySqlRequestOwnershipPreview.NotOwner,
      MySqlRequestOwnershipPreview.VersionMismatch(7)).foreach { value =>
      val core = new MainRequestReadDelivery(Thread.currentThread(), 2)
      val entry = core.begin(tag, 3).get
      core.publish(entry, 3, decision(value))
      assertEquals(Some(decision(value)), core.consume(entry, 3))
      assertEquals(0, core.retainedCount)
    }
  @Test def epochChangedBeforeConsumptionRetainsOriginal(): Unit =
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = core.begin(tag, 0).get
    core.publish(entry, 0, decision(MySqlRequestOwnershipPreview.Missing))
    assertThrows(classOf[IllegalStateException], () => core.consume(entry, 1))
    assertEquals(1, core.retainedCount)
    assertTrue(entry.decision.nonEmpty)
    assertFalse(entry.consumed)
  @Test def stopDoesNotAcknowledgeOrClearUnconsumedDecision(): Unit =
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = core.begin(tag, 0).get
    core.publish(entry, 0, decision(MySqlRequestOwnershipPreview.NotOwner))
    core.retire()
    assertThrows(classOf[IllegalStateException], () => core.consume(entry, 0))
    assertEquals(1, core.retainedCount)
    assertFalse(entry.consumed)
  @Test def primaryAndSecondaryKeepOriginalIdentity(): Unit =
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = core.begin(tag, 0).get
    val primary = new IllegalStateException("primary")
    val secondary = new IllegalArgumentException("secondary")
    core.fail(entry, primary)
    core.fail(entry, secondary)
    val actual = assertThrows(classOf[IllegalStateException], () => core.consume(entry, 0))
    assertSame(primary, actual)
    assertSame(secondary, actual.getSuppressed()(0))
    assertEquals(1, core.retainedCount)
  @Test def boundedAndExactEntryLifecycle(): Unit =
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = core.begin(tag, 0).get
    val other = tag.copy(operationId = new UUID(8, 9))
    assertTrue(core.begin(other, 0).isEmpty)
    core.publish(entry, 0, decision(MySqlRequestOwnershipPreview.Missing))
    core.consume(entry, 0)
    assertTrue(core.begin(other, 0).nonEmpty)
    assertThrows(classOf[IllegalArgumentException], () => core.consume(entry, 0))

  @Test def actualCompletionFailurePollReachesBridgeWithOriginalCause(): Unit =
    val q = new MainRequestReadLedger(MySqlOwnedIoIdentity(tag.bootId, 1, 2), Thread.currentThread(), 2)
    val request = MySqlRequestReadPreview(new UUID(5, 6), new UUID(7, 8), 0)
    val entry = q.begin(tag, request, 0).get
    q.submitted(entry, IoSubmission.Accepted)
    val primary = new java.sql.SQLException("controlled test failure")
    val originalStack = primary.getStackTrace
    val completion = IoCompletion[MySqlOwnedIoReply](tag, Failure(primary))
    q.end(1, 0, Vector(completion))
    val core = new MainRequestReadDelivery(Thread.currentThread(), 2)
    val delivered = core.begin(tag, 0).get
    core.observe(delivered, 0, q.poll(tag, 0))
    val actual = assertThrows(classOf[java.sql.SQLException], () => core.consume(delivered, 0))
    assertSame(primary, actual)
    assertTrue(originalStack.sameElements(actual.getStackTrace))
    assertSame(completion, entry.completion.get)
    assertFalse(entry.decisionConsumed)
    assertFalse(entry.releasing)
    assertTrue(q.releaseCandidates.isEmpty)
    assertEquals(1, core.retainedCount)

  @Test def malformedDecisionFailurePollRetainsExactCompletionAndTag(): Unit =
    val q = new MainRequestReadLedger(MySqlOwnedIoIdentity(tag.bootId, 1, 2), Thread.currentThread(), 2)
    val request = MySqlRequestReadPreview(new UUID(5, 6), new UUID(7, 8), 0)
    val entry = q.begin(tag, request, 0).get
    q.submitted(entry, IoSubmission.Accepted)
    val completion = IoCompletion[MySqlOwnedIoReply](tag, Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    q.end(1, 0, Vector(completion))
    val core = new MainRequestReadDelivery(Thread.currentThread(), 2)
    val delivered = core.begin(tag, 0).get
    core.observe(delivered, 0, q.poll(tag, 0))
    val actual = assertThrows(classOf[IllegalStateException], () => core.consume(delivered, 0))
    assertSame(entry.failure.get, actual)
    assertSame(completion, entry.completion.get)
    val other = tag.copy(operationId = new UUID(9, 10))
    assertEquals(MainRequestReadPoll.Pending, q.poll(other, 0))
    assertTrue(q.releaseCandidates.isEmpty)

  @Test def staleFailurePollCannotConsumeOrReleaseHealthyScalar(): Unit =
    val q = new MainRequestReadLedger(MySqlOwnedIoIdentity(tag.bootId, 1, 2), Thread.currentThread(), 1)
    val request = MySqlRequestReadPreview(new UUID(5, 6), new UUID(7, 8), 0)
    val entry = q.begin(tag, request, 0).get
    q.submitted(entry, IoSubmission.Accepted)
    val replies = Vector(MySqlRepositoryReply.OptionalState(None), MySqlRepositoryReply.Entitlement(None),
      MySqlRepositoryReply.StoredRows(Nil))
    q.end(1, 0, Vector(IoCompletion(tag, Success(MySqlOwnedIoReply.Transaction(replies)))))
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val delivered = core.begin(tag, 0).get
    core.observe(delivered, 1, q.poll(tag, 1))
    assertThrows(classOf[IllegalStateException], () => core.consume(delivered, 1))
    assertFalse(entry.decisionConsumed)
    assertTrue(entry.completion.nonEmpty)
    assertTrue(q.releaseCandidates.isEmpty)

  @Test def healthyPollKeepsOriginalLaterEndReleaseContract(): Unit =
    val q = new MainRequestReadLedger(MySqlOwnedIoIdentity(tag.bootId, 1, 2), Thread.currentThread(), 1)
    val request = MySqlRequestReadPreview(new UUID(5, 6), new UUID(7, 8), 0)
    val entry = q.begin(tag, request, 0).get
    q.submitted(entry, IoSubmission.Accepted)
    val replies = Vector(MySqlRepositoryReply.OptionalState(None), MySqlRepositoryReply.Entitlement(None),
      MySqlRepositoryReply.StoredRows(Nil))
    q.end(1, 0, Vector(IoCompletion(tag, Success(MySqlOwnedIoReply.Transaction(replies)))))
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val delivered = core.begin(tag, 0).get
    core.observe(delivered, 0, q.poll(tag, 0))
    assertEquals(MySqlRequestOwnershipPreview.Missing, core.consume(delivered, 0).get.ownership)
    assertTrue(q.releaseCandidates.isEmpty)
    q.end(2, 0, Vector.empty)
    assertSame(entry, q.releaseCandidates.head)
    q.dropped(entry)
    assertTrue(q.released(entry, IoCompletion(tag, Success(MySqlOwnedIoReply.RetainedPlanReleased(tag)))))
    assertEquals(0, q.retained)

  @Test def preStopConsumesActualScalarWithoutPlayerDeliveryAndWaitsForOriginalRelease(): Unit =
    val q = new MainRequestReadLedger(MySqlOwnedIoIdentity(tag.bootId, 1, 2), Thread.currentThread(), 1)
    val request = MySqlRequestReadPreview(new UUID(5, 6), new UUID(7, 8), 0)
    val original = q.begin(tag, request, 3).get
    q.submitted(original, IoSubmission.Accepted)
    val delivery = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = delivery.begin(tag, 3).get
    assertFalse(delivery.consumeForPreStop(entry, 3, q.poll(tag, 3)))
    val replies = Vector(MySqlRepositoryReply.OptionalState(None), MySqlRepositoryReply.Entitlement(None),
      MySqlRepositoryReply.StoredRows(Nil))
    q.end(1, 3, Vector(IoCompletion(tag, Success(MySqlOwnedIoReply.Transaction(replies)))))
    assertTrue(delivery.consumeForPreStop(entry, 3, q.poll(tag, 3)))
    assertTrue(entry.consumed)
    assertTrue(entry.decision.isEmpty)
    delivery.requireSettledForPreStop()
    assertThrows(classOf[IllegalArgumentException], () => q.requireSettledForPreStop())
    assertTrue(q.releaseCandidates.isEmpty)
    q.end(2, 3, Vector.empty)
    assertSame(original, q.releaseCandidates.head)
    q.dropped(original)
    assertThrows(classOf[IllegalArgumentException], () => q.requireSettledForPreStop())
    assertTrue(q.released(original, IoCompletion(tag, Success(MySqlOwnedIoReply.RetainedPlanReleased(tag)))))
    q.requireSettledForPreStop()
    assertThrows(classOf[IllegalArgumentException], () => delivery.consumeForPreStop(entry, 3, MainRequestReadPoll.Pending))

  @Test def preStopFailureStaleAndRetiredDeliveryKeepOriginalResponsibilities(): Unit =
    val primary = new IllegalStateException("original pre-stop read failure")
    val delivery = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val entry = delivery.begin(tag, 3).get
    val stack = primary.getStackTrace
    val actual = assertThrows(classOf[IllegalStateException],
      () => delivery.consumeForPreStop(entry, 3, MainRequestReadPoll.Failed(primary)))
    assertSame(primary, actual)
    assertTrue(stack.sameElements(actual.getStackTrace))
    assertFalse(entry.consumed)
    assertSame(primary, assertThrows(classOf[IllegalStateException], () => delivery.requireSettledForPreStop()))
    val stale = new MainRequestReadDelivery(Thread.currentThread(), 1)
    val staleEntry = stale.begin(tag, 3).get
    assertThrows(classOf[IllegalStateException], () => stale.consumeForPreStop(staleEntry, 4,
      MainRequestReadPoll.Decision(decision(MySqlRequestOwnershipPreview.Missing))))
    assertFalse(staleEntry.consumed)
    assertEquals(1, stale.retainedCount)
    val retired = new MainRequestReadDelivery(Thread.currentThread(), 1)
    retired.retire()
    assertThrows(classOf[IllegalArgumentException], () => retired.requireSettledForPreStop())

  @Test def pendingAndStrictShareDeliveryFailureWithoutDiscardingEntry(): Unit =
    val core = new MainRequestReadDelivery(Thread.currentThread(), 1)
    assertFalse(core.pendingForPreStop)
    val entry = core.begin(tag, 3L).get
    assertTrue(core.pendingForPreStop)
    val cause = new IllegalStateException("actual delivery failure")
    core.fail(entry, cause)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.pendingForPreStop))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.requireSettledForPreStop()))
    assertEquals(1, core.retainedCount)
