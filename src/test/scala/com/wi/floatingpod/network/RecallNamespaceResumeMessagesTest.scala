package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.Unpooled
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class RecallNamespaceResumeMessagesTest:
  private val ns=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID())
  private val operation=UUID.randomUUID()
  private val request=UUID.randomUUID()
  private val pod=UUID.randomUUID()

  @Test def namespaceQueryHasNoActorClaimAndPublicationIsStrictAtomicNamespace():Unit =
    val query=Unpooled.buffer();val response=Unpooled.buffer()
    try
      new C2SRecallNamespaceRequest().toBytes(query)
      assertEquals(2,query.readableBytes());new C2SRecallNamespaceRequest().fromBytes(query.duplicate())
      query.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>new C2SRecallNamespaceRequest().fromBytes(query.duplicate()))
      new S2CRecallNamespace(ns).toBytes(response)
      assertEquals(34,response.readableBytes())
      val decoded=new S2CRecallNamespace();decoded.fromBytes(response.duplicate());assertEquals(ns,decoded.namespace)
      response.setByte(1,99)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(response.duplicate()))
      assertEquals(ns,decoded.namespace)
      response.setByte(1,1);response.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(response.duplicate()))
      assertEquals(ns,decoded.namespace)
    finally
      query.release();response.release()

  @Test def explicitResumeOnlyEncodesOriginalOperationAndRejectsInvalidPartialPayload():Unit =
    val buffer=Unpooled.buffer()
    try
      new C2SRecallResumeQuery(ns,operation).toBytes(buffer);assertEquals(50,buffer.readableBytes())
      val decoded=new C2SRecallResumeQuery();decoded.fromBytes(buffer.duplicate())
      assertEquals(ns,decoded.namespace);assertEquals(operation,decoded.operationUuid)
      val short=buffer.duplicate();short.writerIndex(short.writerIndex()-1)
      assertThrows(classOf[IndexOutOfBoundsException],()=>decoded.fromBytes(short))
      buffer.writeByte(0)
      assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
      assertEquals(ns,decoded.namespace);assertEquals(operation,decoded.operationUuid)
      assertThrows(classOf[IllegalArgumentException],()=>new C2SRecallResumeQuery(ns,new UUID(0L,0L)).validate())
    finally buffer.release()

  @Test def readOnlyLookupShapesRoundTripWithoutTerminalPhaseAndMalformedDecodeIsAtomic():Unit =
    val values=Vector(RecallResumeLookup.NamespaceMismatch,RecallResumeLookup.NotFound,
      RecallResumeLookup.Located(pod,7L,Some(request)),RecallResumeLookup.Located(pod,7L,None))
    values.foreach { value=>
      val buffer=Unpooled.buffer()
      try
        new S2CRecallResumeResult(ns,operation,value).toBytes(buffer)
        val decoded=new S2CRecallResumeResult();decoded.fromBytes(buffer.duplicate())
        assertEquals(ns,decoded.namespace);assertEquals(operation,decoded.operationUuid);assertEquals(value,decoded.lookup)
        buffer.setByte(50,99) // namespace header34 + original UUID16。
        assertThrows(classOf[IllegalArgumentException],()=>decoded.fromBytes(buffer.duplicate()))
        assertEquals(value,decoded.lookup);assertEquals(operation,decoded.operationUuid)
      finally buffer.release()
    }
    val buffer=Unpooled.buffer()
    try
      val located=RecallResumeLookup.Located(pod,7L,Some(request))
      new S2CRecallResumeResult(ns,operation,located).toBytes(buffer)
      buffer.setByte(75,2) // result byte + pod UUID + version后的canonical request flag。
      assertThrows(classOf[IllegalArgumentException],()=>new S2CRecallResumeResult().fromBytes(buffer.duplicate()))
      assertThrows(classOf[IllegalArgumentException],()=>new S2CRecallResumeResult(ns,operation,
        RecallResumeLookup.Located(pod,-1L,None)).validate())
    finally buffer.release()

  @Test def newNamespaceAndResumeMessagesStillHaveNoRuntimeRegistrations():Unit =
    val types=PacketHandler.registrations().map(_.messageType)
    for message<-Vector(classOf[C2SRecallNamespaceRequest],classOf[S2CRecallNamespace],
      classOf[C2SRecallResumeQuery],classOf[S2CRecallResumeResult]) do assertFalse(types.contains(message))
