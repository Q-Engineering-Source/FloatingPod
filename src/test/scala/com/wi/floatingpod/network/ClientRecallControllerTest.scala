package com.wi.floatingpod.network

import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable

class ClientRecallControllerTest:
  private class Fixture:
    var now = 0L
    val sent = mutable.ArrayBuffer.empty[C2SRecallOperationRequest]
    val connection = new Object
    val pod = UUID.randomUUID()
    val operation = UUID.randomUUID()
    val controller = new ClientRecallController(() => now, sent += _)
    def submit(): Boolean = controller.submit(connection, pod, 4L)
    def result(phase: RecallOperationPhase, op: Option[UUID] = Some(operation),
        reason: Option[RecallFailureReason] = None): S2CRecallOperationStatus =
      new S2CRecallOperationStatus(sent.head.requestUuid, pod, 4L, op, phase, reason)
    def receive(phase: RecallOperationPhase): Unit =
      controller.receive(connection, result(phase))

  @Test def resultStartsTwoSecondBufferRatherThanFirstClickOrProcessingAck(): Unit =
    val f = new Fixture
    assertTrue(f.submit())
    f.now = 9000000000L
    assertFalse(f.submit())
    f.receive(RecallOperationPhase.Processing)
    f.now = 20000000000L
    assertFalse(f.submit())
    f.receive(RecallOperationPhase.Committed)
    assertTrue(f.controller.view(f.pod).get.completed)
    f.now += 1999999999L
    assertFalse(f.submit())
    f.now += 1L
    assertTrue(f.submit())
    assertEquals(2, f.sent.size)
    assertNotEquals(f.sent.head.requestUuid, f.sent.last.requestUuid)

  @Test def duplicateAndDelayedOldRepliesDoNotExtendBufferOrUnlockNewRequest(): Unit =
    val f = new Fixture
    f.submit(); val terminal = f.result(RecallOperationPhase.Committed)
    f.controller.receive(f.connection, terminal)
    f.now = 1999999999L
    assertTrue(f.controller.receive(f.connection, terminal).isEmpty)
    f.now += 1L
    assertTrue(f.submit())
    assertTrue(f.controller.receive(f.connection, terminal).isEmpty)
    assertFalse(f.submit())
    assertFalse(f.controller.view(f.pod).get.completed)

  @Test def retainedFailureKeepsOriginalRequestAndOperationBeyondBufferTime(): Unit =
    val f = new Fixture
    f.submit(); f.receive(RecallOperationPhase.Processing)
    f.controller.receive(f.connection, f.result(RecallOperationPhase.RetainedFailure,
      reason = Some(RecallFailureReason.InfrastructureFailure)))
    f.now = 999999999999L
    assertFalse(f.submit())
    val view = f.controller.view(f.pod).get
    assertEquals(Some(f.operation), view.operationUuid)
    assertFalse(view.completed)
    assertThrows(classOf[IllegalArgumentException], () => f.receive(RecallOperationPhase.Processing))
    assertEquals(Some(RecallOperationPhase.RetainedFailure), f.controller.view(f.pod).get.phase)
    f.receive(RecallOperationPhase.Committed)
    assertFalse(f.submit())

  @Test def originalOperationCannotBeReplacedOrTurnedIntoUnacceptedRefusal(): Unit =
    val f = new Fixture
    f.submit(); f.receive(RecallOperationPhase.Processing)
    assertThrows(classOf[IllegalArgumentException], () => f.controller.receive(f.connection,
      f.result(RecallOperationPhase.Committed, Some(UUID.randomUUID()))))
    assertThrows(classOf[IllegalArgumentException], () => f.controller.receive(f.connection,
      f.result(RecallOperationPhase.Refused, None, Some(RecallFailureReason.Busy))))
    assertFalse(f.controller.view(f.pod).get.completed)
    assertEquals(Some(f.operation), f.controller.view(f.pod).get.operationUuid)

  @Test def unacceptedBusyGetsResultBufferAndConnectionChangeDropsOnlyClientState(): Unit =
    val f = new Fixture
    f.submit()
    f.controller.receive(f.connection, f.result(RecallOperationPhase.Refused, None, Some(RecallFailureReason.Busy)))
    assertFalse(f.submit())
    val replacement = new Object
    f.controller.bindConnection(replacement)
    assertTrue(f.controller.view(f.pod).isEmpty)
    assertTrue(f.controller.receive(replacement, f.result(RecallOperationPhase.Committed)).isEmpty)
    assertTrue(f.controller.submit(replacement, f.pod, 4L))
    f.controller.disconnect()
    assertTrue(f.controller.view(f.pod).isEmpty)

  @Test def ambiguousSendFailureCannotAutomaticallyMintAnotherRequest(): Unit =
    val cause = new IllegalStateException("actual send failed")
    var calls = 0
    val controller = new ClientRecallController(() => 0L, _ => { calls += 1; throw cause })
    val connection = new Object; val pod = UUID.randomUUID()
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => controller.submit(connection, pod, 4L)))
    assertFalse(controller.submit(connection, pod, 4L))
    assertEquals(1, calls)
