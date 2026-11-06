package com.wi.floatingpod.server

import com.wi.floatingpod.network.*
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable

class MainRecallFeedbackCorrelationTest:
  private val actor = UUID.randomUUID()
  private val pod = UUID.randomUUID()
  private val operation = UUID.randomUUID()
  private val first = UUID.randomUUID()
  private def correlation(original: Option[UUID] = Some(first)) =
    new MainRecallFeedbackCorrelation(Thread.currentThread(), actor, pod, 4L, operation, original)

  @Test def reconnectedRequestChangesOnlyFeedbackAliasAndPreservesOriginalIdentity(): Unit =
    val pending = correlation()
    val next = UUID.randomUUID()
    assertTrue(pending.attachAuthenticated(actor, pod, 4L, next))
    assertEquals(Some(next), pending.currentRequest)
    assertEquals(Some(first), pending.originalRequest)
    assertEquals(operation, pending.operation)
    assertTrue(pending.references(first)); assertTrue(pending.references(next))
    assertTrue(pending.attachAuthenticated(actor, pod, 4L, next))
    assertEquals(operation, pending.operation)

  @Test def foreignOwnerPodVersionOrNullRequestCannotReplaceFeedback(): Unit =
    val pending = correlation()
    assertFalse(pending.attachAuthenticated(UUID.randomUUID(), pod, 4L, UUID.randomUUID()))
    assertFalse(pending.attachAuthenticated(actor, UUID.randomUUID(), 4L, UUID.randomUUID()))
    assertFalse(pending.attachAuthenticated(actor, pod, 5L, UUID.randomUUID()))
    assertFalse(pending.attachAuthenticated(actor, pod, 4L, null))
    assertEquals(Some(first), pending.currentRequest)
    assertEquals(operation, pending.operation)

  @Test def originalLegacyPendingCanGainCorrelatedFeedbackWithoutChangingOperation(): Unit =
    val pending = correlation(None)
    assertTrue(pending.currentRequest.isEmpty)
    assertTrue(pending.attachAuthenticated(actor, pod, 4L, first))
    assertTrue(pending.originalRequest.isEmpty)
    assertEquals(Some(first), pending.currentRequest)
    assertEquals(operation, pending.operation)

  @Test def replacementClientRequestCanObserveSameRetainedFailureWithoutTimeoutUnlock(): Unit =
    var now = 0L
    val sent = mutable.ArrayBuffer.empty[C2SRecallOperationRequest]
    val client = new ClientRecallController(() => now, sent += _)
    val originalConnection = new Object
    assertTrue(client.submit(originalConnection, pod, 4L))
    val original = sent.head
    val pending = correlation(Some(original.requestUuid))
    client.receive(originalConnection, new S2CRecallOperationStatus(original.requestUuid, pod, 4L,
      Some(operation), RecallOperationPhase.Processing, None))
    val reconnected = new Object
    assertTrue(client.submit(reconnected, pod, 4L))
    val replacement = sent.last
    assertNotEquals(original.requestUuid, replacement.requestUuid)
    assertTrue(pending.attachAuthenticated(actor, pod, 4L, replacement.requestUuid))
    client.receive(reconnected, new S2CRecallOperationStatus(pending.currentRequest.get, pod, 4L,
      Some(pending.operation), RecallOperationPhase.RetainedFailure, Some(RecallFailureReason.InfrastructureFailure)))
    assertTrue(client.receive(reconnected, new S2CRecallOperationStatus(original.requestUuid, pod, 4L,
      Some(operation), RecallOperationPhase.Committed, None)).isEmpty)
    now = 999999999999L
    assertFalse(client.submit(reconnected, pod, 4L))
    assertEquals(Some(operation), client.view(pod).get.operationUuid)
    assertFalse(client.view(pod).get.completed)

  @Test def foreignThreadCannotReadOrRetargetOriginalMainFeedback(): Unit =
    val pending = correlation()
    val failure = new AtomicReference[Throwable]()
    val thread = Thread.ofPlatform().unstarted(() => {
      try pending.attachAuthenticated(actor, pod, 4L, UUID.randomUUID())
      catch case cause: Throwable => failure.set(cause)
      ()
    })
    thread.start(); thread.join(2000L)
    assertFalse(thread.isAlive)
    assertInstanceOf(classOf[IllegalArgumentException], failure.get())
    assertEquals(Some(first), pending.currentRequest)
