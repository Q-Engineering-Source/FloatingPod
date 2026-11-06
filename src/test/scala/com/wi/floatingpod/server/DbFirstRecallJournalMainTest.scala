package com.wi.floatingpod.server

import com.wi.floatingpod.domain.RecallRequestNamespace
import com.wi.floatingpod.server.io.{IoCompletion,IoOperationTag}
import com.wi.floatingpod.storage.mysql.{MySqlOwnedIoAdapter,MySqlOwnedIoIdentity}
import com.wi.floatingpod.storage.mysql.MySqlOwnedIoReply
import java.io.IOException
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.util.Failure

/** 实际未Boot adapter的拒绝边界；不伪造SQL确认、Main读代、transfer或最终ACK。 */
class DbFirstRecallJournalMainTest:
  @Test def bootReadControlFailurePropagatesExactWorkerCauseAndRetainsEnvelopeWithoutReissue():Unit =
    val control=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
    val custody=new DbFirstRecallJournalCompletion(Thread.currentThread(),control)
    val original=new IOException("actual Boot fresh-read/cleanup failure")
    val actual=IoCompletion[MySqlOwnedIoReply](control,Failure(original))
    custody.beginSubmission();custody.retain(actual)
    assertSame(original,assertThrows(classOf[IOException],()=>custody.requireSuccessfulWorkerResult()))
    assertSame(actual,custody.current.get)
    assertSame(original,custody.failure.get)
    assertEquals(Some(control),custody.expectedTag)
    assertSame(original,assertThrows(classOf[IOException],()=>custody.beginSubmission()))
    assertSame(original,assertThrows(classOf[IOException],()=>custody.consumed()))
    assertSame(actual,custody.current.get)

  @Test def submissionUnknownRetainsLateExactCompletionWithoutHealthyResumeOrReplay():Unit =
    val j=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
    val custody=new DbFirstRecallJournalCompletion(Thread.currentThread(),j)
    val original=new IOException("actual submission outcome unknown")
    custody.beginSubmission()
    custody.fail(original)
    assertEquals(Some(j),custody.expectedTag,"failed owner must retain its original late-completion route")
    val worker=new IOException("late worker failure")
    val late=IoCompletion[MySqlOwnedIoReply](j,Failure(worker))
    custody.retain(late)
    assertSame(late,custody.current.get)
    assertSame(original,custody.failure.get)
    assertSame(original,assertThrows(classOf[IOException],()=>custody.requireHealthy()))
    assertSame(original,assertThrows(classOf[IOException],()=>custody.consumed()))
    assertSame(original,assertThrows(classOf[IOException],()=>custody.beginSubmission()))
    assertThrows(classOf[IllegalArgumentException],()=>custody.retain(late))
    assertSame(late,custody.current.get)
    assertEquals(Some(j),custody.expectedTag)

  @Test def failedJournalCustodyRejectsForeignTagAndThreadWithoutLosingLateRoute():Unit =
    val j=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
    val custody=new DbFirstRecallJournalCompletion(Thread.currentThread(),j)
    val cause=new IOException("original UNKNOWN")
    custody.beginSubmission();custody.fail(cause)
    val foreign=IoCompletion[MySqlOwnedIoReply](j.copy(operationId=UUID.randomUUID()),Failure(new IOException("other operation")))
    assertThrows(classOf[IllegalArgumentException],()=>custody.retain(foreign))
    val late=IoCompletion[MySqlOwnedIoReply](j,Failure(cause))
    val refused=new AtomicReference[Throwable]()
    val other=new Thread(()=>{
      try custody.retain(late)
      catch case failure:Throwable=>refused.set(failure)
    },"foreign-late-completion-owner")
    other.start();other.join(5000L);assertFalse(other.isAlive)
    assertInstanceOf(classOf[IllegalArgumentException],refused.get())
    assertTrue(custody.current.isEmpty);assertEquals(Some(j),custody.expectedTag)
    custody.retain(late)
    assertSame(late,custody.current.get);assertSame(cause,custody.failure.get)

  @Test def consumedActualCompletionClearsRouteBeforeNextControlAndFailureCannotEraseStoredReply():Unit =
    val j=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
    val custody=new DbFirstRecallJournalCompletion(Thread.currentThread(),j)
    val original=new IOException("original failed operation")
    val actual=IoCompletion[MySqlOwnedIoReply](j,Failure(original))
    // 只验证词法保管/清槽；失败DATA不被当作SQL确认，生产caller仍须先经实际adapter。
    custody.beginSubmission();custody.retain(actual)
    custody.consumed()
    assertTrue(custody.current.isEmpty);assertTrue(custody.expectedTag.isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>custody.retain(actual))
    custody.beginSubmission();custody.retain(actual)
    custody.fail(original)
    assertSame(actual,custody.current.get)
    assertSame(original,assertThrows(classOf[IOException],()=>custody.consumed()))

  private def withUnbootedAdapter(body:(MySqlOwnedIoAdapter,IoOperationTag,IoOperationTag)=>Unit):Unit =
    val identity=MySqlOwnedIoIdentity(UUID.randomUUID(),1L,1L)
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"journal-main-unbooted",identity)
    val j=IoOperationTag(identity.bootId,1L,1L,UUID.randomUUID())
    val c=j.copy(operationId=UUID.randomUUID())
    var primary:Throwable=null
    try body(adapter,j,c)
    catch case failure:Throwable=>primary=failure
    finally
      try
        adapter.quiesce()
        val failed=new AtomicReference[Throwable]()
        val waiter=new Thread(()=>{
          try assertTrue(adapter.awaitExitOffMain(5L,TimeUnit.SECONDS),"actual worker exit timeout")
          catch case failure:Throwable=>failed.set(failure)
        },"journal-main-off-thread-exit")
        waiter.start();waiter.join(6000L)
        assertFalse(waiter.isAlive,"off-thread exit observer timeout")
        Option(failed.get()).foreach(throw _)
      catch case cleanup:Throwable=>
        if primary==null then primary=cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)
    if primary!=null then throw primary

  private def owner(adapter:MySqlOwnedIoAdapter,j:IoOperationTag,c:IoOperationTag):DbFirstRecallJournalMain =
    new DbFirstRecallJournalMain(Thread.currentThread(),adapter,j,c,
      RecallRequestNamespace(UUID.randomUUID(),UUID.randomUUID()),UUID.randomUUID(),7L,Some(UUID.randomUUID()))

  @Test def actualPreBootRefusalRetainsOriginalFailureAndCannotRetryOrPublishCapture():Unit =
    withUnbootedAdapter { (adapter,j,c)=>
      val work=owner(adapter,j,c)
      val epoch=adapter.repositorySubmissionEpoch
      val failure=assertThrows(classOf[IllegalArgumentException],()=>work.submitJournal())
      assertSame(failure,work.failure.get)
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.submitJournal()))
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.advance(1L)))
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.prepareCaptureTransfer(2L)))
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.takeCapture()))
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.prepareRefusal(2L,new IOException("later refusal reason"))))
      assertSame(failure,assertThrows(classOf[IllegalArgumentException],()=>work.takeRefusal()))
      assertFalse(work.refusalAvailable)
      assertFalse(work.captureAvailable);assertFalse(work.awaitingCaptureDecision)
      assertTrue(work.expectedCompletionTag.isEmpty)
      assertEquals(epoch,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }

  @Test def unstartedMainCannotInventJournalAckOrCaptureByEndOrForeignThread():Unit =
    withUnbootedAdapter { (adapter,j,c)=>
      val work=owner(adapter,j,c)
      work.advance(1L);work.advance(1L)
      assertFalse(work.awaitingCaptureDecision);assertFalse(work.captureAvailable)
      assertThrows(classOf[IllegalArgumentException],()=>work.retain(null))
      assertThrows(classOf[IllegalArgumentException],()=>work.prepareCaptureTransfer(2L))
      assertThrows(classOf[IllegalArgumentException],()=>work.takeCapture())
      val failed=new AtomicReference[Throwable]()
      val other=new Thread(()=>{
        try work.submitJournal()
        catch case failure:Throwable=>failed.set(failure)
      },"foreign-journal-Main")
      other.start();other.join(5000L);assertFalse(other.isAlive)
      assertInstanceOf(classOf[IllegalArgumentException],failed.get())
      assertTrue(work.failure.isEmpty,"foreign call cannot poison original custody")
      assertTrue(work.expectedCompletionTag.isEmpty)
      assertEquals(0L,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
    }

  @Test def constructorRejectsForeignExecutorAndJAliasedCBeforeAnySubmit():Unit =
    withUnbootedAdapter { (adapter,j,c)=>
      assertThrows(classOf[IllegalArgumentException],()=>owner(adapter,j,j))
      assertThrows(classOf[IllegalArgumentException],()=>owner(adapter,j,c.copy(connectionGeneration=2L)))
      val foreign=j.copy(bootId=UUID.randomUUID())
      assertThrows(classOf[IllegalArgumentException],()=>owner(adapter,foreign,foreign.copy(operationId=c.operationId)))
      assertEquals(0L,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }

  @Test def unconfirmedJournalCannotChooseRefusalOrExposeTerminalByClockOrFailureData():Unit =
    withUnbootedAdapter { (adapter,j,c)=>
      val work=owner(adapter,j,c)
      val reason=new IOException("actual current admission conflict")
      work.advance(1L)
      assertThrows(classOf[IllegalArgumentException],()=>work.prepareRefusal(1L,reason))
      assertThrows(classOf[IllegalArgumentException],()=>work.prepareRefusal(2L,null))
      assertThrows(classOf[IllegalArgumentException],()=>work.takeRefusal())
      assertFalse(work.refusalAvailable)
      assertFalse(work.captureAvailable)
      assertTrue(work.failure.isEmpty,"invalid early decision cannot create accepted refusal or poison original owner")
      assertTrue(work.expectedCompletionTag.isEmpty)
      work.advance(3L)
      assertFalse(work.refusalAvailable)
      assertEquals(0L,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }
