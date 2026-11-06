package com.wi.floatingpod.server

import com.wi.floatingpod.domain.RecallRequestNamespace
import com.wi.floatingpod.network.*
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MainRecallResultCustodyTest:
  private val ns=RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID())
  private def tag()=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
  private def reserve(c:MainRecallResultCustody)=c.reserve(ns,UUID.randomUUID(),UUID.randomUUID(),7L,tag())

  @Test def reservationHasNoTerminalAndEarlyClientAckCannotRelease():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),1);val slot=reserve(c)
    assertFalse(c.available);assertEquals(0,c.completedCount)
    assertTrue(c.client(ns,slot.request).isEmpty);assertTrue(c.original(ns,slot.tag.operationId).isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>slot.status)
    assertFalse(c.received(new S2CDurableRecallStatus(ns,slot.request,slot.pod,7L,Some(slot.tag.operationId),
      DurableRecallPhase.Committed,None)))
    assertFalse(c.available)

  @Test def boundedCapacityTransfersFromPendingToResultUntilExactReceipt():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),1);val slot=reserve(c)
    c.committed(slot)
    assertEquals(1,c.completedCount);assertFalse(c.available)
    assertThrows(classOf[IllegalArgumentException],()=>reserve(c))
    val status=slot.status
    assertTrue(c.received(status));assertTrue(c.available);assertEquals(0,c.completedCount)
    assertTrue(c.received(status)) // release确认丢失后，同一ACK可重发；不重消耗业务ACK。
    val next=reserve(c);assertNotEquals(slot.request,next.request)

  @Test def mismatchedResultFieldsNeverFreeExistingSlot():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),1);val slot=reserve(c);c.refused(slot,RecallFailureReason.Busy)
    for change<-Vector[S2CDurableRecallStatus=>Unit](
        _.podUuid=UUID.randomUUID(),_.requestUuid=UUID.randomUUID(),_.expectedStateVersion=8L,
        _.operationUuid=Some(UUID.randomUUID()),_.reason=Some(RecallFailureReason.NotOwner),
        s=>{s.phase=DurableRecallPhase.Committed;s.reason=None}) do
      val status=slot.status;change(status)
      assertFalse(c.received(status));assertFalse(c.available)
    assertTrue(c.received(slot.status))

  @Test def repliesAreCopiesAndNeverContainDeliveryConnectionOrRawReceipt():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),1);val slot=reserve(c);c.committed(slot)
    val sent=slot.status;sent.podUuid=UUID.randomUUID();sent.reason=Some(RecallFailureReason.Busy)
    val retained=c.original(ns,slot.tag.operationId).get.status
    assertEquals(slot.pod,retained.podUuid);assertTrue(retained.reason.isEmpty)
    assertEquals(DurableRecallPhase.Committed,retained.phase)

  @Test def newClientUuidAtSamePodAndVersionDoesNotMatchOldRefusal():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),2)
    val first=reserve(c);c.refused(first,RecallFailureReason.Busy)
    val fresh=UUID.randomUUID()
    assertTrue(c.client(ns,fresh).isEmpty)
    val next=c.reserve(ns,fresh,first.pod,first.version,tag())
    assertTrue(c.client(ns,next.request).isEmpty);assertEquals(1,c.completedCount)
    assertTrue(c.original(ns.copy(actorUuid=UUID.randomUUID()),first.tag.operationId).isEmpty)
    assertTrue(c.original(ns.copy(bindingUuid=UUID.randomUUID()),first.tag.operationId).isEmpty)

  @Test def exactOriginalSlotHasOneFinalTransitionAndRejectsDuplicateIdentity():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),2);val slot=reserve(c)
    c.refused(slot,RecallFailureReason.NotOwner)
    assertThrows(classOf[IllegalArgumentException],()=>c.committed(slot))
    assertThrows(classOf[IllegalArgumentException],()=>c.refused(slot,RecallFailureReason.Busy))
    assertThrows(classOf[IllegalArgumentException],()=>c.reserve(ns,slot.request,slot.pod,7L,tag()))

  @Test def otherThreadCannotReadOrReleaseOriginalResults():Unit =
    val c=new MainRecallResultCustody(Thread.currentThread(),1);val slot=reserve(c);c.committed(slot)
    val status=slot.status;val failed=new AtomicReference[Throwable]()
    val thread=new Thread(()=>{
      try
        c.received(status)
        ()
      catch case cause:Throwable=>failed.set(cause)
    })
    thread.start();thread.join();assertInstanceOf(classOf[IllegalArgumentException],failed.get())
    assertFalse(c.available)
