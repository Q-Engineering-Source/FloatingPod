package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.Unpooled
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DurableRecallMessagesTest:
  private val ns=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID())
  private val request=UUID.randomUUID()
  private val pod=UUID.randomUUID()
  private val operation=UUID.randomUUID()

  @Test def durableRequestRoundTripsNamespaceWithoutChangingLegacyRequestFormat():Unit =
    val buffer=Unpooled.buffer();val legacy=Unpooled.buffer()
    try
      new C2SDurableRecallRequest(ns,request,pod,7L).toBytes(buffer)
      val decoded=new C2SDurableRecallRequest();decoded.fromBytes(buffer.duplicate())
      assertEquals(ns,decoded.namespace);assertEquals(request,decoded.requestUuid)
      assertEquals(pod,decoded.podUuid);assertEquals(7L,decoded.expectedStateVersion)
      new C2SRecallOperationRequest(request,pod,7L).toBytes(legacy)
      assertEquals(74,buffer.readableBytes());assertEquals(41,legacy.readableBytes())
      assertThrows(classOf[RuntimeException],()=>new C2SDurableRecallRequest().fromBytes(legacy.duplicate()))
      val short=buffer.duplicate();short.writerIndex(short.writerIndex()-1)
      assertThrows(classOf[IndexOutOfBoundsException],()=>decoded.fromBytes(short))
      assertEquals(ns,decoded.namespace);assertEquals(request,decoded.requestUuid)
      buffer.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
    finally
      buffer.release();legacy.release()

  @Test def durablePhasesPreserveAcceptedRefusalOriginalCAndPendingHiddenC():Unit =
    for phase<-DurableRecallPhase.values do
      val op=if phase==DurableRecallPhase.Pending || phase==DurableRecallPhase.NotAccepted then None else Some(operation)
      val reason=phase match
        case DurableRecallPhase.Refused | DurableRecallPhase.NotAccepted=>Some(RecallFailureReason.Busy)
        case DurableRecallPhase.RetainedFailure=>Some(RecallFailureReason.InfrastructureFailure)
        case _=>None
      val buffer=Unpooled.buffer()
      try
        new S2CDurableRecallStatus(ns,request,pod,7L,op,phase,reason).toBytes(buffer)
        val decoded=new S2CDurableRecallStatus();decoded.fromBytes(buffer.duplicate())
        assertEquals(ns,decoded.namespace);assertEquals(request,decoded.requestUuid);assertEquals(pod,decoded.podUuid)
        assertEquals(op,decoded.operationUuid);assertEquals(phase,decoded.phase);assertEquals(reason,decoded.reason)
      finally buffer.release()
    new S2CDurableRecallStatus(ns,request,pod,7L,None,DurableRecallPhase.Pending,
      Some(RecallFailureReason.InfrastructureFailure)).validate()

  @Test def invalidDurableShapesCannotDiscloseCBeforeJournalOrEraseAcceptedRefusalC():Unit =
    def status(op:Option[UUID],phase:DurableRecallPhase,reason:Option[RecallFailureReason]) =
      new S2CDurableRecallStatus(ns,request,pod,7L,op,phase,reason)
    assertThrows(classOf[IllegalArgumentException],()=>status(Some(operation),DurableRecallPhase.Pending,None).validate())
    assertThrows(classOf[IllegalArgumentException],()=>status(None,DurableRecallPhase.Refused,Some(RecallFailureReason.Busy)).validate())
    assertThrows(classOf[IllegalArgumentException],()=>status(Some(operation),DurableRecallPhase.NotAccepted,Some(RecallFailureReason.Busy)).validate())
    assertThrows(classOf[IllegalArgumentException],()=>new C2SDurableRecallRequest(ns,new UUID(0L,0L),pod,7L).validate())
    assertThrows(classOf[IllegalArgumentException],()=>status(Some(new UUID(0L,0L)),DurableRecallPhase.Processing,None).validate())
    // 历史Refused语义保持不变，新消息族没有放宽旧wire。
    assertThrows(classOf[IllegalArgumentException],()=>new S2CRecallOperationStatus(request,pod,7L,Some(operation),
      RecallOperationPhase.Refused,Some(RecallFailureReason.Busy)).validate())

  @Test def malformedDurableVersionBooleanAndTrailingPayloadLeaveDecodedIdentityIntact():Unit =
    val buffer=Unpooled.buffer()
    try
      new S2CDurableRecallStatus(ns,request,pod,7L,Some(operation),DurableRecallPhase.Processing,None).toBytes(buffer)
      val decoded=new S2CDurableRecallStatus();decoded.fromBytes(buffer.duplicate())
      buffer.setByte(1,99)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
      buffer.setByte(1,1);buffer.setByte(74,2)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
      buffer.setByte(74,1);buffer.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
      assertEquals(ns,decoded.namespace);assertEquals(request,decoded.requestUuid)
      assertEquals(Some(operation),decoded.operationUuid);assertEquals(DurableRecallPhase.Processing,decoded.phase)
    finally buffer.release()

  @Test def durableMessagesRemainUnregisteredUntilNamespaceResumeAndBootOwnersAreConnected():Unit =
    val registrations=PacketHandler.registrations()
    assertFalse(registrations.exists(_.messageType==classOf[C2SDurableRecallRequest]))
    assertFalse(registrations.exists(_.messageType==classOf[S2CDurableRecallStatus]))
    assertTrue(registrations.exists(_.messageType==classOf[C2SRecallOperationRequest]))
    assertTrue(registrations.exists(_.messageType==classOf[S2CRecallOperationStatus]))
