package com.wi.floatingpod.network

import java.io.IOException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable.ArrayBuffer

/** 验证生产runtime实际使用的pre-stop顺序；不伪造read屏障、native或SQL成功。 */
class DbFirstPreStopRecallDrainTest:
  @Test def acceptedRecallAndItsAckSettleBeforeLaterEndStopFreeze():Unit =
    val drain=new DbFirstPreStopRecallDrain
    val events=ArrayBuffer.empty[String]
    var acceptedPending=true
    var stopReady=false
    def end(n:Long):Boolean = drain.onEnd(n) {
      events+="recall END"
    } {
      events+="actual owner settled query"
      !acceptedPending
    } {
      assertFalse(acceptedPending,"stop freeze cannot assert live body while original removal/ACK is pending")
      events+="stop freeze"
    } {
      events+="normal stop END"
      stopReady
    }
    assertFalse(end(7L))
    assertEquals(Vector("recall END","actual owner settled query"),events.toVector)
    acceptedPending=false // 仅模拟调度输入；不是native/worker ACK证据。
    assertFalse(end(8L))
    assertFalse(events.contains("stop freeze"),"ACK completion stack returns before stop freeze")
    assertFalse(end(9L))
    assertEquals(1,events.count(_=="stop freeze"))
    assertEquals(1,events.count(_=="normal stop END"))
    stopReady=true
    assertTrue(end(10L))
    assertEquals(2,events.count(_=="recall END"))
    assertEquals(1,events.count(_=="stop freeze"))
    assertEquals(2,events.count(_=="normal stop END"))
    assertThrows(classOf[IllegalArgumentException],()=>end(11L))

  @Test def emptyRecallStillRequiresReturningEndBeforeOneStopFreeze():Unit =
    val drain=new DbFirstPreStopRecallDrain
    var freezes=0
    assertFalse(drain.onEnd(1L)(())(true) { freezes+=1 } (true))
    assertEquals(0,freezes)
    assertTrue(drain.onEnd(2L) { fail("drained recall must not be re-entered") } (false) { freezes+=1 } (true))
    assertEquals(1,freezes)

  @Test def nonMonotonicEndCannotAdvanceAnyCallback():Unit =
    val drain=new DbFirstPreStopRecallDrain
    var calls=0
    def end(n:Long):Boolean = drain.onEnd(n) { calls+=1 } (true) { calls+=1 } { calls+=1;true }
    assertThrows(classOf[IllegalArgumentException],()=>end(0L))
    assertFalse(end(3L))
    assertEquals(1,calls)
    assertThrows(classOf[IllegalArgumentException],()=>end(3L))
    assertThrows(classOf[IllegalArgumentException],()=>end(2L))
    assertEquals(1,calls)
    assertTrue(end(4L))
    assertEquals(3,calls)

  @Test def failedRecallQueryFreezeOrSourceCannotBecomeSuccessOrAutomaticRetry():Unit =
    for stage<-Vector("drain","settled","freeze","source") do
      val drain=new DbFirstPreStopRecallDrain
      val original=new IOException("actual "+stage+" failure")
      var effects=0
      def effect(at:String):Unit =
        effects+=1
        if stage==at then throw original
      def end(n:Long):Boolean = drain.onEnd(n) { effect("drain") } { effect("settled");true } {
        effect("freeze")
      } { effect("source");true }
      val thrown=assertThrows(classOf[IOException],()=>
        if !end(1L) then end(2L))
      assertSame(original,thrown)
      val after=effects
      assertSame(original,assertThrows(classOf[IOException],()=>end(3L)))
      assertEquals(after,effects)

  @Test def fatalCallbackRemainsExactAndPreventsFurtherEndEffects():Unit =
    val drain=new DbFirstPreStopRecallDrain
    val fatal=new LinkageError("actual fatal recall owner")
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>drain.onEnd(1L)(throw fatal)(true)(())(true)))
    var entered=false
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>drain.onEnd(2L) { entered=true } (true)(())(true)))
    assertFalse(entered)
