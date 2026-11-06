package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import java.io.IOException
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable.ArrayBuffer

/** 实际客户端关联/时钟逻辑；连接标识仅是纯测试身份，不声称服务器或GPU验收。 */
class DurableClientRecallControllerTest:
  private class Fixture:
    var now=0L
    val namespace=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID())
    val connection=new Object()
    val pod=UUID.randomUUID()
    val operation=UUID.randomUUID()
    val fresh=ArrayBuffer.empty[C2SDurableRecallRequest]
    val queries=ArrayBuffer.empty[C2SRecallResumeQuery]
    val received=ArrayBuffer.empty[C2SRecallResultReceived]
    val publications=ArrayBuffer.empty[C2SRecallNamespaceRequest]
    val client=new DurableClientRecallController(()=>now,m=>{publications+=m;()},m=>{fresh+=m;()},m=>{queries+=m;()},m=>{received+=m;()})
    client.bindConnection(connection)
    def publish(actual:AnyRef=connection,ns:RecallRequestNamespace=namespace):Unit =
      client.bindConnection(actual);assertTrue(client.receiveNamespace(actual,new S2CRecallNamespace(ns)))
    def start():C2SDurableRecallRequest =
      publish();assertTrue(client.submit(connection,pod,7L));fresh.last
    def status(request:C2SDurableRecallRequest,phase:DurableRecallPhase,op:Option[UUID],reason:Option[RecallFailureReason]=None) =
      new S2CDurableRecallStatus(request.namespace,request.requestUuid,request.podUuid,request.expectedStateVersion,op,phase,reason)

  @Test def freshAndResumeWaitForActualCurrentConnectionNamespacePublication():Unit =
    val f=new Fixture
    assertFalse(f.client.submit(f.connection,f.pod,7L))
    assertFalse(f.client.resumeOriginal(f.connection,f.operation))
    assertTrue(f.fresh.isEmpty);assertTrue(f.queries.isEmpty);assertTrue(f.client.currentView(f.pod).isEmpty)
    f.client.requestNamespace(f.connection);assertEquals(1,f.publications.size)
    assertFalse(f.client.receiveNamespace(new Object(),new S2CRecallNamespace(f.namespace)))
    assertTrue(f.client.currentNamespace.isEmpty)
    f.publish();assertTrue(f.client.submit(f.connection,f.pod,7L))
    assertEquals(f.namespace,f.fresh.head.namespace)

  @Test def disconnectedUnknownJournalKeepsOriginalRequestWithoutFreshResend():Unit =
    val f=new Fixture;val request=f.start()
    f.client.receive(f.connection,f.status(request,DurableRecallPhase.Pending,None,Some(RecallFailureReason.InfrastructureFailure)))
    val original=f.client.view(f.namespace,f.pod).get
    f.client.disconnect(f.connection)
    val next=new Object();f.client.bindConnection(next)
    assertFalse(f.client.submit(next,f.pod,7L))
    assertFalse(f.client.receive(f.connection,f.status(request,DurableRecallPhase.Committed,Some(f.operation))).nonEmpty)
    f.publish(next)
    f.now=90000000000L
    assertFalse(f.client.submit(next,f.pod,7L))
    assertEquals(original,f.client.currentView(f.pod).get)
    assertEquals(1,f.fresh.size);assertTrue(f.queries.isEmpty)

  @Test def knownOriginalCReconnectUsesOnlyExplicitReadOnlyQuery():Unit =
    val f=new Fixture;val request=f.start()
    f.client.receive(f.connection,f.status(request,DurableRecallPhase.Processing,Some(f.operation)))
    val old=f.client.currentView(f.pod).get
    f.client.disconnect(f.connection);val next=new Object();f.publish(next)
    assertEquals(old,f.client.currentView(f.pod).get)
    assertTrue(f.queries.isEmpty);assertEquals(1,f.fresh.size)
    assertTrue(f.client.resumeOriginal(next,f.operation))
    assertEquals(f.operation,f.queries.head.operationUuid);assertEquals(f.namespace,f.queries.head.namespace)
    assertEquals(old,f.client.currentView(f.pod).get);assertEquals(1,f.fresh.size)

  @Test def otherBindingAndActorCannotRebindOrUnlockOriginalPending():Unit =
    val f=new Fixture;val first=f.start();val old=f.client.currentView(f.pod).get
    for foreign<-Vector(f.namespace.copy(bindingUuid=UUID.randomUUID()),f.namespace.copy(actorUuid=UUID.randomUUID())) do
      val next=new Object();f.publish(next,foreign)
      assertTrue(f.client.currentView(f.pod).isEmpty)
      assertTrue(f.client.receive(next,f.status(first,DurableRecallPhase.Refused,Some(f.operation),Some(RecallFailureReason.Busy))).isEmpty)
      assertEquals(old,f.client.view(f.namespace,f.pod).get)
      assertTrue(f.client.submit(next,f.pod,7L))
      assertNotEquals(first.requestUuid,f.fresh.last.requestUuid)
    val restored=new Object();f.publish(restored)
    assertFalse(f.client.submit(restored,f.pod,7L));assertEquals(old,f.client.currentView(f.pod).get)

  @Test def acceptedRefusalPreservesCAndStartsExactlyTwoSecondsAtResultReceipt():Unit =
    val f=new Fixture;val request=f.start()
    f.now=100000000000L
    assertFalse(f.client.submit(f.connection,f.pod,7L))
    val result=f.status(request,DurableRecallPhase.Refused,Some(f.operation),Some(RecallFailureReason.Busy))
    assertTrue(f.client.receive(f.connection,result).get.completed)
    assertEquals(Some(f.operation),f.client.currentView(f.pod).get.operationUuid)
    f.now+=1000000000L;assertTrue(f.client.receive(f.connection,result).isEmpty)
    f.now+=999999999L;assertFalse(f.client.submit(f.connection,f.pod,7L))
    f.now+=1L;assertTrue(f.client.submit(f.connection,f.pod,7L))
    assertNotEquals(request.requestUuid,f.fresh.last.requestUuid)
    assertTrue(f.client.receive(f.connection,result).isEmpty)

  @Test def senderExceptionKeepsOriginalPendingAndDoesNotAuthorizeRemint():Unit =
    val ns=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID());val actual=new Object();val pod=UUID.randomUUID()
    val cause=new IOException("send outcome unknown")
    val sent=ArrayBuffer.empty[C2SDurableRecallRequest]
    val client=new DurableClientRecallController(()=>0L,_=>(),message=>{sent+=message;throw cause},_=>(),_=>())
    client.bindConnection(actual);client.receiveNamespace(actual,new S2CRecallNamespace(ns))
    assertSame(cause,assertThrows(classOf[IOException],()=>client.submit(actual,pod,7L)))
    val original=client.currentView(pod).get
    client.disconnect(actual);val next=new Object();client.bindConnection(next)
    client.receiveNamespace(next,new S2CRecallNamespace(ns))
    assertFalse(client.submit(next,pod,7L));assertEquals(1,sent.size)
    assertEquals(original,client.currentView(pod).get)

  @Test def locatedMissingAndMismatchLookupDataNeverChangePendingOrCompletionClock():Unit =
    val f=new Fixture;val request=f.start();val original=f.client.currentView(f.pod).get
    for observation<-Vector(RecallResumeLookup.NotFound,RecallResumeLookup.NamespaceMismatch,
      RecallResumeLookup.Located(f.pod,7L,Some(request.requestUuid))) do
      val response=new S2CRecallResumeResult(f.namespace,f.operation,observation)
      assertEquals(Some(observation),f.client.receiveResume(f.connection,response))
      assertEquals(original,f.client.currentView(f.pod).get)
    f.now=Long.MaxValue/2
    assertFalse(f.client.submit(f.connection,f.pod,7L));assertEquals(1,f.fresh.size)

  @Test def oldConnectionAndRegressiveStatusCannotChangeOriginalOperation():Unit =
    val f=new Fixture;val request=f.start()
    f.client.receive(f.connection,f.status(request,DurableRecallPhase.RetainedFailure,Some(f.operation),Some(RecallFailureReason.InfrastructureFailure)))
    val old=f.client.currentView(f.pod).get
    assertThrows(classOf[IllegalArgumentException],()=>f.client.receive(f.connection,f.status(request,DurableRecallPhase.Processing,Some(f.operation))))
    assertThrows(classOf[IllegalArgumentException],()=>f.client.receive(f.connection,f.status(request,DurableRecallPhase.Committed,Some(UUID.randomUUID()))))
    assertThrows(classOf[IllegalArgumentException],()=>f.client.receive(f.connection,f.status(request,DurableRecallPhase.NotAccepted,None,Some(RecallFailureReason.Busy))))
    val next=new Object();f.publish(next)
    f.client.disconnect(f.connection) // 旧disconnect不得撤新连接的namespace。
    assertEquals(Some(f.namespace),f.client.currentNamespace)
    assertEquals(old,f.client.currentView(f.pod).get)
    assertThrows(classOf[IllegalArgumentException],()=>f.client.receiveNamespace(next,new S2CRecallNamespace(f.namespace.copy(actorUuid=UUID.randomUUID()))))
    assertTrue(f.client.receive(next,f.status(request,DurableRecallPhase.Refused,Some(f.operation),Some(RecallFailureReason.Busy))).get.completed)

  @Test def boundedPendingNeverEvictsOldNamespaceUnknownToMakeRoomForFresh():Unit =
    val f=new Fixture;f.publish()
    val pods=Vector.fill(256)(UUID.randomUUID())
    pods.foreach(pod=>assertTrue(f.client.submit(f.connection,pod,7L)))
    val first=f.client.view(f.namespace,pods.head).get
    val next=new Object();f.publish(next,f.namespace.copy(bindingUuid=UUID.randomUUID()))
    assertFalse(f.client.submit(next,UUID.randomUUID(),7L))
    assertEquals(256,f.fresh.size);assertEquals(first,f.client.view(f.namespace,pods.head).get)

  @Test def receivedTerminalAckRetriesAfterReconnectWithoutBusinessResendOrClockReset():Unit =
    val f=new Fixture;val request=f.start()
    val terminal=f.status(request,DurableRecallPhase.Committed,Some(f.operation))
    assertTrue(f.client.receive(f.connection,terminal).get.completed);assertEquals(1,f.received.size)
    f.now=100L;assertTrue(f.client.receive(f.connection,terminal).isEmpty);assertEquals(2,f.received.size)
    f.client.disconnect(f.connection);val next=new Object();f.publish(next)
    assertEquals(3,f.received.size);assertEquals(1,f.fresh.size)
    assertTrue(f.client.receiveReleased(next,new S2CRecallResultReleased(f.received.last.result)))
    f.client.disconnect(next);f.publish(new Object());assertEquals(3,f.received.size)

  @Test def receiptSenderFailurePreservesTerminalAndPendingAckForRetry():Unit =
    val ns=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID());val connection=new Object()
    val fresh=ArrayBuffer.empty[C2SDurableRecallRequest];var throwAck=true;var attempts=0;var now=0L
    val failure=new IOException("receipt delivery unknown")
    val client=new DurableClientRecallController(()=>now,_=>(),m=>{fresh+=m;()},_=>(),_=>{
      attempts+=1
      if throwAck then throw failure
    })
    client.bindConnection(connection);client.receiveNamespace(connection,new S2CRecallNamespace(ns))
    val pod=UUID.randomUUID();client.submit(connection,pod,7L);val request=fresh.head
    val terminal=new S2CDurableRecallStatus(ns,request.requestUuid,pod,7L,Some(UUID.randomUUID()),DurableRecallPhase.Committed,None)
    assertSame(failure,assertThrows(classOf[IOException],()=>client.receive(connection,terminal)))
    assertTrue(client.currentView(pod).get.completed);assertFalse(client.submit(connection,pod,7L))
    throwAck=false;val next=new Object();client.bindConnection(next)
    client.receiveNamespace(next,new S2CRecallNamespace(ns));assertEquals(2,attempts);assertEquals(1,fresh.size)
    now=ClientRecallController.ResultBufferNanos;assertTrue(client.submit(next,pod,7L))
    assertNotEquals(request.requestUuid,fresh.last.requestUuid)

  @Test def releaseConfirmationNeedsCurrentConnectionNamespaceAndExactTerminal():Unit =
    val f=new Fixture;val request=f.start()
    f.client.receive(f.connection,f.status(request,DurableRecallPhase.Refused,Some(f.operation),Some(RecallFailureReason.Busy)))
    val original=f.received.last.result
    assertFalse(f.client.receiveReleased(new Object(),new S2CRecallResultReleased(original)))
    val wrong=f.status(request,DurableRecallPhase.Refused,Some(f.operation),Some(RecallFailureReason.NotOwner))
    assertFalse(f.client.receiveReleased(f.connection,new S2CRecallResultReleased(wrong)))
    val other=new Object();f.publish(other,f.namespace.copy(bindingUuid=UUID.randomUUID()))
    assertFalse(f.client.receiveReleased(other,new S2CRecallResultReleased(original)))
    val next=new Object();f.publish(next)
    assertEquals(2,f.received.size);assertTrue(f.client.receiveReleased(next,new S2CRecallResultReleased(original)))
    assertEquals(DurableRecallPhase.Refused,f.client.currentView(f.pod).get.phase.get)

  @Test def unconfirmedReceiptSurvivesTwoSecondsAndEntryReplacementWithinSharedBound():Unit =
    val f=new Fixture;var current=f.start()
    for i<-0 until 256 do
      if i>0 then
        f.now+=ClientRecallController.ResultBufferNanos
        assertTrue(f.client.submit(f.connection,f.pod,7L));current=f.fresh.last
      f.client.receive(f.connection,f.status(current,DurableRecallPhase.Refused,Some(UUID.randomUUID()),Some(RecallFailureReason.Busy)))
    f.now+=ClientRecallController.ResultBufferNanos
    assertFalse(f.client.submit(f.connection,f.pod,7L))
    assertTrue(f.client.receiveReleased(f.connection,new S2CRecallResultReleased(f.received.head.result)))
    assertTrue(f.client.submit(f.connection,f.pod,7L))
