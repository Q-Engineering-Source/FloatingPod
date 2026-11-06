package com.wi.floatingpod.network

import io.netty.buffer.Unpooled
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RecallOperationMessagesTest:
  private val request = UUID.randomUUID()
  private val pod = UUID.randomUUID()
  private val operation = UUID.randomUUID()
  @Test def requestPreservesCorrelationAndRejectsTrailingOrTruncatedPayload(): Unit =
    val buffer = Unpooled.buffer()
    try
      new C2SRecallOperationRequest(request, pod, 4L).toBytes(buffer)
      val decoded = new C2SRecallOperationRequest()
      decoded.fromBytes(buffer.duplicate())
      assertEquals(request, decoded.requestUuid); assertEquals(pod, decoded.podUuid)
      assertEquals(4L, decoded.expectedStateVersion)
      val short = buffer.duplicate(); short.writerIndex(short.writerIndex() - 1)
      assertThrows(classOf[IndexOutOfBoundsException], () => new C2SRecallOperationRequest().fromBytes(short))
      buffer.writeByte(0)
      assertThrows(classOf[IllegalArgumentException], () => new C2SRecallOperationRequest().fromBytes(buffer.duplicate()))
    finally buffer.release()

  @Test def allFourStatusShapesRoundTripWithoutChangingOriginalOperation(): Unit =
    RecallOperationPhase.values.foreach { phase =>
      val op = if phase == RecallOperationPhase.Refused then None else Some(operation)
      val reason = phase match
        case RecallOperationPhase.Refused => Some(RecallFailureReason.Busy)
        case RecallOperationPhase.RetainedFailure => Some(RecallFailureReason.InfrastructureFailure)
        case _ => None
      val source = new S2CRecallOperationStatus(request, pod, 4L, op, phase, reason)
      val buffer = Unpooled.buffer()
      try
        source.toBytes(buffer)
        val decoded = new S2CRecallOperationStatus(); decoded.fromBytes(buffer)
        assertEquals(request, decoded.requestUuid); assertEquals(pod, decoded.podUuid)
        assertEquals(4L, decoded.expectedStateVersion); assertEquals(op, decoded.operationUuid)
        assertEquals(phase, decoded.phase); assertEquals(reason, decoded.reason)
      finally buffer.release()
    }

  @Test def invalidStatusDoesNotOverwritePreviouslyDecodedIdentity(): Unit =
    val valid = new S2CRecallOperationStatus(request, pod, 4L, Some(operation), RecallOperationPhase.Processing, None)
    val buffer = Unpooled.buffer()
    try
      valid.toBytes(buffer)
      val decoded = new S2CRecallOperationStatus(); decoded.fromBytes(buffer.duplicate())
      buffer.setByte(58, 99) // 固定header/两个UUID/version/optional UUID后的phase byte。
      assertThrows(classOf[IllegalArgumentException], () => decoded.fromBytes(buffer.duplicate()))
      assertEquals(request, decoded.requestUuid); assertEquals(Some(operation), decoded.operationUuid)
      assertEquals(RecallOperationPhase.Processing, decoded.phase)
      assertThrows(classOf[IllegalArgumentException], () => new S2CRecallOperationStatus(request,pod,4L,None,
        RecallOperationPhase.Processing,None).validate())
      assertThrows(classOf[IllegalArgumentException], () => new S2CRecallOperationStatus(request,pod,4L,Some(operation),
        RecallOperationPhase.Refused,Some(RecallFailureReason.Busy)).validate())
    finally buffer.release()
