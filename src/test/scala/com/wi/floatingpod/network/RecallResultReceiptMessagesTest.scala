package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.Unpooled
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RecallResultReceiptMessagesTest:
  private def status(phase:DurableRecallPhase,reason:Option[RecallFailureReason]=None)=
    new S2CDurableRecallStatus(RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID()),UUID.randomUUID(),
      UUID.randomUUID(),7L,Some(UUID.randomUUID()),phase,reason)

  @Test def exactAcceptedTerminalPayloadRoundTripsInBothReceiptDirections():Unit =
    for original<-Vector(status(DurableRecallPhase.Committed),status(DurableRecallPhase.Refused,Some(RecallFailureReason.Busy))) do
      val reference=Unpooled.buffer();val received=Unpooled.buffer();val released=Unpooled.buffer()
      try
        original.toBytes(reference)
        new C2SRecallResultReceived(original).toBytes(received);new S2CRecallResultReleased(original).toBytes(released)
        assertEquals(reference,received);assertEquals(reference,released)
        val request=new C2SRecallResultReceived();request.fromBytes(received.duplicate())
        val confirmation=new S2CRecallResultReleased();confirmation.fromBytes(released.duplicate())
        for decoded<-Vector(request.result,confirmation.result) do
          assertEquals(original.namespace,decoded.namespace);assertEquals(original.requestUuid,decoded.requestUuid)
          assertEquals(original.operationUuid,decoded.operationUuid);assertEquals(original.podUuid,decoded.podUuid)
          assertEquals(original.expectedStateVersion,decoded.expectedStateVersion)
          assertEquals(original.phase,decoded.phase);assertEquals(original.reason,decoded.reason)
      finally
        reference.release();received.release();released.release()

  @Test def partialMalformedOrNonterminalDecodeCannotReplaceOriginalReceipt():Unit =
    val original=status(DurableRecallPhase.Committed)
    val request=new C2SRecallResultReceived(original);val confirmation=new S2CRecallResultReleased(original)
    val buffer=Unpooled.buffer()
    try
      original.toBytes(buffer)
      val short=buffer.duplicate();short.writerIndex(short.writerIndex()-1)
      assertThrows(classOf[IndexOutOfBoundsException],()=>request.fromBytes(short))
      buffer.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>confirmation.fromBytes(buffer.duplicate()))
      assertSame(original,request.result);assertSame(original,confirmation.result)
      buffer.clear();status(DurableRecallPhase.Processing).toBytes(buffer)
      assertThrows(classOf[IllegalArgumentException],()=>request.fromBytes(buffer.duplicate()))
      assertThrows(classOf[IllegalArgumentException],()=>confirmation.fromBytes(buffer.duplicate()))
      assertSame(original,request.result);assertSame(original,confirmation.result)
      for phase<-Vector(DurableRecallPhase.Pending,DurableRecallPhase.NotAccepted,DurableRecallPhase.RetainedFailure) do
        val invalid=status(phase,Some(RecallFailureReason.InfrastructureFailure))
        if phase!=DurableRecallPhase.RetainedFailure then invalid.operationUuid=None
        assertThrows(classOf[IllegalArgumentException],()=>new C2SRecallResultReceived(invalid).validate())
    finally buffer.release()

  @Test def receiptDirectionsRemainUnregisteredAlongsideDurableFamily():Unit =
    val types=PacketHandler.registrations().map(_.messageType)
    assertFalse(types.contains(classOf[C2SRecallResultReceived]));assertFalse(types.contains(classOf[S2CRecallResultReleased]))
