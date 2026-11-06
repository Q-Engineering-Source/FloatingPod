package com.wi.floatingpod.server

import java.io.IOException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.collection.mutable.ArrayBuffer

/** 只验证两实际owner共用的reply交接顺序；不伪造Session资格、SQL提交或native完成。 */
class DbFirstRecallCompletionHandoffTest:
  @Test def retainedCauseEscapesOnlyAfterRedundantReplyHasBeenDropped():Unit =
    val original=new IOException("original retained recall failure")
    val reply=new Object
    var pending:Option[AnyRef]=Some(reply)
    var confirmed=false
    val events=ArrayBuffer.empty[String]
    val thrown=assertThrows(classOf[IOException],()=>
      DbFirstRecallCompletionHandoff.consume {
        events+="Main accepted original qualification"
        Some(original)
      } {
        events+="owner dropped redundant reply"
        pending=None
      }
      confirmed=true)
    assertSame(original,thrown)
    assertTrue(pending.isEmpty,"Main-owned retained qualification must not remain in owner completion")
    assertFalse(confirmed,"original retained failure cannot set ordinary confirmed success")
    assertEquals(Vector("Main accepted original qualification","owner dropped redundant reply"),events.toVector)
    assertTrue(original.getSuppressed.isEmpty)

  @Test def failedAcceptanceKeepsOriginalReplyAndPreservesNonFatalOrFatalCause():Unit =
    for cause<-Vector[Throwable](new IOException("Main acceptance failed"),new LinkageError("fatal Main acceptance")) do
      val reply=new Object
      var pending:Option[AnyRef]=Some(reply)
      val thrown=assertThrows(classOf[Throwable],()=>
        DbFirstRecallCompletionHandoff.consume(throw cause) { pending=None })
      assertSame(cause,thrown)
      assertSame(reply,pending.get,"unaccepted original reply remains with its owner")

  @Test def ordinarySuccessDropsReplyBeforeCallerMarksConfirmed():Unit =
    var pending=true
    var confirmed=false
    DbFirstRecallCompletionHandoff.consume(None) { pending=false }
    assertFalse(pending)
    confirmed=true
    assertTrue(confirmed)
