package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.IoSubmission
import java.io.IOException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable.ArrayBuffer

/** 真实owner共用的调度逻辑；回调只记录调用，不伪造Session/native/SQL资格。 */
class DbFirstRecallRetainedEndTest:
  @Test def acceptanceDropsReplyBeforeArmingAndDoesNotGrantOrdinarySuccess():Unit =
    val state=new DbFirstRecallRetainedEnd
    val original=new IOException("original UNKNOWN")
    var reply=true
    var ordinary=false
    val retained=DbFirstRecallCompletionHandoff.accept(Some(original)) { reply=false }
    retained match
      case Some(cause)=>assertSame(original,cause);assertFalse(reply);state.accepted(4L)
      case None=>ordinary=true
    assertTrue(state.active)
    assertFalse(ordinary)
    assertFalse(state.awaitingAck)

  @Test def failedAcceptanceOrReplyDropNeverArmsRetainedWork():Unit =
    for failAcceptance<-Vector(true,false) do
      val state=new DbFirstRecallRetainedEnd
      val cause=new IOException("actual acceptance/drop failure")
      var reply=true
      val thrown=assertThrows(classOf[IOException],()=>
        val accepted=DbFirstRecallCompletionHandoff.accept {
          if failAcceptance then throw cause
          Some(cause)
        } {
          if !failAcceptance then throw cause
          reply=false
        }
        accepted.foreach(_=>state.accepted(1L)))
      assertSame(cause,thrown)
      assertTrue(reply)
      assertFalse(state.active)

  @Test def physicalHandoffSubmitAndAckRequireSeparateActualEnds():Unit =
    val state=new DbFirstRecallRetainedEnd
    val events=ArrayBuffer.empty[String]
    state.accepted(5L)
    def advance(end:Long,done:Boolean):Unit = state.advance(end) {
      events+="physical";done
    } {
      events+="handoff"
    } {
      events+="submit";IoSubmission.Accepted
    }
    advance(5L,true)
    assertTrue(events.isEmpty,"acceptance END cannot run physical work")
    advance(6L,false)
    assertEquals(Vector("physical"),events.toVector)
    advance(7L,true)
    assertEquals(Vector("physical","physical","handoff"),events.toVector)
    assertFalse(state.awaitingAck)
    advance(7L,true)
    assertEquals(3,events.size,"handoff frame must return before any submit")
    advance(8L,true)
    assertTrue(state.awaitingAck)
    advance(9L,true)
    assertEquals(Vector("physical","physical","handoff","submit"),events.toVector,"accepted work is never resubmitted")
    state.finish { events+="actual ACK" }
    assertFalse(state.active)
    assertThrows(classOf[IllegalArgumentException],()=>state.finish { events+="duplicate ACK" })
    assertEquals("actual ACK",events.last)

  @Test def capacityRefusalOnlyResubmitsOriginalReceiptOnLaterEnd():Unit =
    val state=new DbFirstRecallRetainedEnd
    state.accepted(1L)
    var physical=0;var handoff=0;var submissions=0
    def advance(end:Long):Unit = state.advance(end) { physical+=1;true } { handoff+=1 } {
      submissions+=1
      if submissions==1 then IoSubmission.RejectedCapacity else IoSubmission.Accepted
    }
    advance(2L);advance(3L);advance(3L)
    assertFalse(state.awaitingAck)
    assertEquals(1,submissions)
    advance(4L);advance(5L)
    assertTrue(state.awaitingAck)
    assertEquals(2,submissions)
    assertEquals(1,physical);assertEquals(1,handoff)

  @Test def newFailureStopsOwnerWithoutDroppingOriginalPendingOrRetrying():Unit =
    for phase<-Vector("physical","handoff","submit","ACK") do
      val state=new DbFirstRecallRetainedEnd
      val cause=new IOException(phase)
      state.accepted(1L)
      var pending=true
      var effects=0
      def advance(end:Long):Unit = state.advance(end) {
        effects+=1;if phase=="physical" then throw cause
        true
      } {
        effects+=1;if phase=="handoff" then throw cause
      } {
        effects+=1;if phase=="submit" then throw cause
        IoSubmission.Accepted
      }
      val thrown=assertThrows(classOf[IOException],()=>
        try
          advance(2L);advance(3L)
          state.finish { throw cause }
          pending=false
        catch case failure:Throwable=>
          state.stop()
          throw failure)
      assertSame(cause,thrown)
      assertTrue(pending)
      assertFalse(state.active)
      val before=effects
      assertThrows(classOf[IllegalArgumentException],()=>advance(4L))
      assertEquals(before,effects)

  @Test def inactiveWrongOrderOrUnacceptedReleaseCannotConsumeAck():Unit =
    val state=new DbFirstRecallRetainedEnd
    var ack=false
    assertThrows(classOf[IllegalArgumentException],()=>state.finish { ack=true })
    state.accepted(4L)
    assertThrows(classOf[IllegalArgumentException],()=>state.accepted(5L))
    assertThrows(classOf[IllegalArgumentException],()=>state.advance(3L)(true)(())(IoSubmission.Accepted))
    assertThrows(classOf[IllegalArgumentException],()=>state.finish { ack=true })
    assertFalse(ack)

  @Test def stoppedOrQuiescingReleaseCannotBecomeAccepted():Unit =
    val original=new IOException("actual stopped cause")
    for refusal<-Vector(IoSubmission.RejectedQuiescing,IoSubmission.RejectedStopped(Some(original))) do
      val state=new DbFirstRecallRetainedEnd
      state.accepted(1L)
      state.advance(2L)(true)(())(IoSubmission.Accepted)
      val thrown=assertThrows(classOf[Throwable],()=>
        try state.advance(3L)(true)(())(refusal)
        catch case failure:Throwable=>
          state.stop()
          throw failure)
      if refusal!=IoSubmission.RejectedQuiescing then assertSame(original,thrown)
      assertFalse(state.active)
      assertFalse(state.awaitingAck)
