package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission, OwnedSerialIo}
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.util.{Failure, Success}

/** 实际slot/partition逻辑与真实OwnedIO线程；控制reply仅测试DATA，未签SQL/释放资格。 */
final class MySqlTrackedDriftCompletionRouterTest:
  private val identity = MySqlOwnedIoIdentity(UUID.fromString("10000000-0000-0000-0000-000000000001"), 1L, 1L)
  private def tag(): IoOperationTag = IoOperationTag(identity.bootId, 1L, 1L, UUID.randomUUID())

  @Test def callerCannotRotateEmptyRouterWithoutActualServiceCompletionFrame():Unit =
    val router=new MySqlTrackedDriftCompletionRouter(identity,2)
    val next=MySqlOwnedIoIdentity(identity.bootId,identity.bootGeneration,2L)
    assertThrows(classOf[IllegalArgumentException],()=>router.adoptRecoveredExecution(null,next))
    val original=tag()
    assertTrue(router.begin(original,MySqlCompletionChannel.Ordinary,false))
    assertThrows(classOf[IllegalArgumentException],()=>router.begin(
      IoOperationTag(next.bootId,next.bootGeneration,next.connectionGeneration,UUID.randomUUID()),MySqlCompletionChannel.Ordinary,false))
    router.submitted(original,IoSubmission.Accepted)
    val done=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    router.routeBatch(Vector(done))
    assertSame(done,router.take(MySqlCompletionChannel.Ordinary,1).head)
    router.requireTerminalEmpty()

  @Test def rejectedPreparationDataKeepsOriginalSlotAndCauseWithoutPoisoningSibling():Unit =
    val router=new MySqlTrackedDriftCompletionRouter(identity,2)
    val original=tag();val sibling=tag()
    val cause=new MySqlDbFirstPreparationRejected(MySqlDbFirstPreparationRejected.Reason.ActiveCapacity,"capacity DATA")
    val observation=MySqlDbFirstSession.DbFirstInitialPreparationRejection(original,UUID.randomUUID(),cause,"0"*64)
    val completion=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.DbFirstInitialPreparationRejected(observation)))
    assertTrue(router.begin(original,MySqlCompletionChannel.Ordinary,true))
    router.submitted(original,IoSubmission.Accepted)
    router.routeBatch(Vector(completion))
    assertSame(completion,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertSame(cause,observation.cause)
    assertTrue(router.failure.isEmpty)
    assertEquals(1,router.retainedCount)
    assertTrue(router.begin(sibling,MySqlCompletionChannel.Ordinary,false))
    router.submitted(sibling,IoSubmission.Accepted)
    val done=IoCompletion[MySqlOwnedIoReply](sibling,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    router.routeBatch(Vector(done))
    assertSame(done,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertEquals(1,router.retainedCount)
    assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty())
    // DATA回覆只可进入原slot控制交接，不自动归还业务容量或发行SQL恢复资格。
    router.beginRelease(original,MySqlCompletionChannel.Ordinary)
    router.submitted(original,IoSubmission.RejectedCapacity)
    assertEquals(1,router.retainedCount)

  @Test def classifiedLocalRetirementKeepsItsSlotWhileHealthySiblingCanComplete(): Unit =
    val router=new MySqlTrackedDriftCompletionRouter(identity,2)
    val failed=tag();val sibling=tag()
    assertTrue(router.begin(failed,MySqlCompletionChannel.Ordinary,true))
    router.submitted(failed,IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](failed,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
    router.take(MySqlCompletionChannel.Ordinary,1)
    router.beginRelease(failed,MySqlCompletionChannel.Ordinary)
    router.submitted(failed,IoSubmission.Accepted)
    val original=new IllegalStateException("local Entry borrower remains")
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](failed,Failure(original))))
    // 直接验证生产使用的阶段/内存策略；不伪造SQL-confirmed receipt或局部资格。
    val local=MySqlDbFirstRetirementFailure.classify(MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight)
    router.retainControlFailure(failed,original,local)
    assertTrue(router.failure.isEmpty,"local failure must not poison shared router")
    assertEquals(1,router.retainedCount)
    assertTrue(router.begin(sibling,MySqlCompletionChannel.Ordinary,false))
    router.submitted(sibling,IoSubmission.Accepted)
    assertEquals(1,router.routableSlots(2))
    val done=IoCompletion[MySqlOwnedIoReply](sibling,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    router.routeBatch(Vector(done))
    assertSame(done,router.take(MySqlCompletionChannel.Ordinary,2).head)
    assertEquals(1,router.retainedCount)
    assertThrows(classOf[IllegalArgumentException],()=>router.beginRelease(failed,MySqlCompletionChannel.Ordinary))
    assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty())
    assertEquals(MySqlDbFirstRetirementFailure.Disposition.Shared,
      MySqlDbFirstRetirementFailure.classify(MySqlDbFirstRetirementFailure.Stage.Unqualified))
    assertEquals(MySqlDbFirstRetirementFailure.Disposition.Shared,
      MySqlDbFirstRetirementFailure.classify(MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted))

  @Test def missingDbFirstRetirementReceiptCannotReturnOriginalCredit(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    val original = tag()
    assertTrue(router.begin(original, MySqlCompletionChannel.Ordinary, true))
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Failure(new IllegalStateException("original DATA failure")))))
    router.take(MySqlCompletionChannel.Ordinary, 1)
    router.beginRelease(original, MySqlCompletionChannel.Ordinary)
    router.submitted(original, IoSubmission.Accepted)
    val invalid = IoCompletion[MySqlOwnedIoReply](original, Success(MySqlOwnedIoReply.DbFirstRecallReleased(null)))
    router.routeBatch(Vector(invalid))
    assertSame(invalid, router.take(MySqlCompletionChannel.Ordinary, 1).head)
    assertEquals(1, router.retainedCount)
    assertTrue(router.failure.nonEmpty)
    assertThrows(classOf[IllegalArgumentException], () => router.finishDbFirstRecallRelease(null, invalid))
    assertEquals(1, router.retainedCount)
    assertThrows(classOf[IllegalStateException], () => router.beginRelease(original, MySqlCompletionChannel.Ordinary))

  @Test def missingInitialPublicationReceiptCannotReturnOriginalCredit(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    val original = tag()
    assertTrue(router.begin(original, MySqlCompletionChannel.Ordinary, true))
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Failure(new IllegalStateException("original DATA failure")))))
    router.take(MySqlCompletionChannel.Ordinary, 1)
    router.beginRelease(original, MySqlCompletionChannel.Ordinary)
    router.submitted(original, IoSubmission.Accepted)
    val invalid = IoCompletion[MySqlOwnedIoReply](original, Success(MySqlOwnedIoReply.DbFirstInitialPublicationReleased(null)))
    router.routeBatch(Vector(invalid))
    assertSame(invalid, router.take(MySqlCompletionChannel.Ordinary, 1).head)
    assertEquals(1, router.retainedCount)
    assertTrue(router.failure.nonEmpty)
    assertThrows(classOf[IllegalArgumentException], () => router.finishDbFirstInitialPublicationRelease(null, invalid))
    assertEquals(1, router.retainedCount)
    assertThrows(classOf[IllegalStateException], () => router.beginRelease(original, MySqlCompletionChannel.Ordinary))

  @Test def consumedBusinessStillChargesAndItsOwnControlCanProgressAtCapacityOne(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    val original = tag()
    val failure = new IllegalStateException("actual operation failure DATA")
    assertTrue(router.begin(original, MySqlCompletionChannel.Ordinary, true))
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Failure(failure))))
    val consumed = router.take(MySqlCompletionChannel.Ordinary, 1)
    assertSame(failure, consumed.head.result.failed.get)
    assertEquals(1, router.retainedCount)
    assertFalse(router.begin(tag(), MySqlCompletionChannel.Drift, true))
    router.beginRelease(original, MySqlCompletionChannel.Ordinary)
    router.submitted(original, IoSubmission.RejectedCapacity)
    assertEquals(1, router.retainedCount)
    router.beginRelease(original, MySqlCompletionChannel.Ordinary)
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Success(MySqlOwnedIoReply.ModeProjectionReleased(original)))))
    assertEquals(1, router.take(MySqlCompletionChannel.Ordinary, 1).size)
    assertEquals(0, router.retainedCount)
    assertThrows(classOf[IllegalStateException], () => router.beginRelease(original, MySqlCompletionChannel.Ordinary))

  @Test def failedControlPreservesOriginalCauseCreditAndRejectsFurtherAcceptance(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 1)
    val original = tag()
    assertTrue(router.begin(original, MySqlCompletionChannel.Drift, true))
    router.submitted(original, IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Failure(new IllegalStateException("work DATA")))))
    router.take(MySqlCompletionChannel.Drift, 1)
    router.beginRelease(original, MySqlCompletionChannel.Drift)
    router.submitted(original, IoSubmission.Accepted)
    val cause = new IllegalStateException("original release failure")
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original, Failure(cause))))
    assertSame(cause, router.take(MySqlCompletionChannel.Drift, 1).head.result.failed.get)
    assertSame(cause, router.failure.get)
    assertEquals(1, router.retainedCount)
    assertSame(cause, assertThrows(classOf[IllegalStateException],
      () => router.begin(tag(), MySqlCompletionChannel.Ordinary, true)))

  @Test def actualWorkerFatalRetainsBothAcceptedChannelSlotsAndExactCause(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 2)
    val first = tag()
    val second = tag()
    val release = new CountDownLatch(1)
    val entered = new CountDownLatch(1)
    val complete = new CountDownLatch(1)
    val fatal = new LinkageError("controlled real worker fatal")
    val main = Thread.currentThread()
    val io = new OwnedSerialIo[IoOperationTag, MySqlOwnedIoReply](2, main,
      "stagec-router-real-fatal-test", None, Some(complete))(_ =>
      entered.countDown()
      if !release.await(5L, TimeUnit.SECONDS) then throw new AssertionError("worker release deadline")
      throw fatal
    )
    var primary: Throwable = null
    try
      assertTrue(router.begin(first, MySqlCompletionChannel.Ordinary, true))
      router.submitted(first, io.submit(first, first))
      assertTrue(entered.await(5L, TimeUnit.SECONDS))
      assertTrue(router.begin(second, MySqlCompletionChannel.Drift, true))
      router.submitted(second, io.submit(second, second))
      release.countDown()
      assertTrue(complete.await(5L, TimeUnit.SECONDS), "completion enqueue must be observed")
      val actual = io.drain(2)
      assertEquals(2, actual.size)
      router.routeBatch(actual)
      val drift = router.take(MySqlCompletionChannel.Drift, 1)
      val ordinary = router.take(MySqlCompletionChannel.Ordinary, 1)
      assertEquals(second, drift.head.tag)
      assertEquals(first, ordinary.head.tag)
      assertSame(fatal, ordinary.head.result.failed.get)
      assertSame(fatal, drift.head.result.failed.get)
      assertEquals(2, router.retainedCount)
      assertFalse(router.begin(tag(), MySqlCompletionChannel.Ordinary, true))
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      release.countDown()
      try io.quiesce()
      catch
        case cause: Throwable =>
          if primary == null then primary = cause else MySqlOwnedSessionLifecycle.suppress(primary, cause)
      val cleanupFailure = new AtomicReference[Throwable]()
      val waiter = new Thread(() => {
        try
          if !io.awaitExitOffMain(5L, TimeUnit.SECONDS) then throw new AssertionError("actual worker did not exit")
        catch case cause: Throwable => cleanupFailure.set(cause)
        ()
      }, "stagec-router-exit-observer")
      waiter.start()
      waiter.join(6000L)
      val cleanup = if waiter.isAlive then new AssertionError("exit observer did not finish") else cleanupFailure.get()
      if cleanup != null then
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      if primary != null then throw primary

  @Test def retainedOtherPartitionsDoNotBlockOriginalReadReleaseSlot(): Unit =
    val router = new MySqlTrackedDriftCompletionRouter(identity, 3)
    val ordinary = tag(); val drift = tag(); val read = tag()
    Vector(ordinary -> MySqlCompletionChannel.Ordinary, drift -> MySqlCompletionChannel.Drift,
      read -> MySqlCompletionChannel.RequestRead).foreach { (t, channel) =>
      assertTrue(router.begin(t, channel, true)); router.submitted(t, IoSubmission.Accepted)
    }
    def reply(t: IoOperationTag) = IoCompletion[MySqlOwnedIoReply](t, Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    router.routeBatch(Vector(reply(ordinary), reply(drift), reply(read)))
    assertEquals(Vector(read), router.take(MySqlCompletionChannel.RequestRead, 1).map(_.tag))
    router.beginRelease(read, MySqlCompletionChannel.RequestRead)
    router.submitted(read, IoSubmission.Accepted)
    assertEquals(1, router.routableSlots(3))
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](read, Success(MySqlOwnedIoReply.RetainedPlanReleased(read)))))
    assertEquals(Vector(read), router.take(MySqlCompletionChannel.RequestRead, 1).map(_.tag))
    assertEquals(2, router.retainedCount)
    assertEquals(Vector(ordinary), router.take(MySqlCompletionChannel.Ordinary, 1).map(_.tag))
    assertEquals(Vector(drift), router.take(MySqlCompletionChannel.Drift, 1).map(_.tag))

  @Test def checkpointFinalAckMustBeTheExactTakenWorkerCompletionAndKeepsReadStageCredit():Unit =
    // 本测试只检查原router的真实槽状态，不把构造的metadata当SQL确认资格。
    val original=tag()
    val receipt=MySqlDbFirstCheckpointFinalReceipt.issue(null,new MySqlRetainedPlanHandle(null,1L,original,null))
    val router=new MySqlTrackedDriftCompletionRouter(identity,1)
    assertTrue(router.begin(original,MySqlCompletionChannel.Ordinary,true))
    router.submitted(original,IoSubmission.Accepted)
    val work=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))
    router.routeBatch(Vector(work));router.take(MySqlCompletionChannel.Ordinary,1)
    val ack=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.DbFirstCheckpointFinalReleased(receipt)))
    assertThrows(classOf[IllegalArgumentException],()=>router.finishDbFirstCheckpointFinalRelease(receipt,ack))
    router.beginRelease(original,MySqlCompletionChannel.Ordinary)
    router.submitted(original,IoSubmission.RejectedCapacity)
    assertEquals(1,router.retainedCount)
    router.beginRelease(original,MySqlCompletionChannel.Ordinary)
    router.submitted(original,IoSubmission.Accepted)
    router.routeBatch(Vector(ack))
    assertThrows(classOf[IllegalArgumentException],()=>router.finishDbFirstCheckpointFinalRelease(receipt,ack))
    assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertEquals(1,router.retainedCount)
    assertFalse(router.begin(tag(),MySqlCompletionChannel.Ordinary,true))
    assertThrows(classOf[IllegalArgumentException],()=>router.finishDbFirstCheckpointFinalRelease(receipt,
      IoCompletion[MySqlOwnedIoReply](original,ack.result)))
    val counterfeit=MySqlDbFirstCheckpointFinalReceipt.issue(null,new MySqlRetainedPlanHandle(null,1L,original,null))
    assertThrows(classOf[IllegalArgumentException],()=>router.finishDbFirstCheckpointFinalRelease(counterfeit,ack))
    assertEquals(1,router.retainedCount)
    router.finishDbFirstCheckpointFinalRelease(receipt,ack)
    assertEquals(0,router.retainedCount)
    router.requireTerminalEmpty()
    assertThrows(classOf[IllegalStateException],()=>router.finishDbFirstCheckpointFinalRelease(receipt,ack))

  @Test def journalReadReplyWithoutOriginalConfirmationRetainsFailedControlSlot():Unit =
    val original=tag();val router=new MySqlTrackedDriftCompletionRouter(identity,1)
    assertTrue(router.begin(original,MySqlCompletionChannel.Ordinary,true))
    router.submitted(original,IoSubmission.Accepted)
    val observation=MySqlDbFirstSession.DbFirstRecallJournalObservation(original,tag(),UUID.randomUUID(),true,true,true,"0"*64)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.DbFirstRecallJournalObserved(observation)))))
    router.take(MySqlCompletionChannel.Ordinary,1)
    router.beginRelease(original,MySqlCompletionChannel.Ordinary);router.submitted(original,IoSubmission.Accepted)
    val invalid=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.JournalMainReadReleased(null)))
    router.routeBatch(Vector(invalid));assertSame(invalid,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertEquals(1,router.retainedCount)
    val cause=router.failure.get
    assertSame(cause,assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty()))
    assertThrows(classOf[IllegalArgumentException],()=>router.finishJournalReadAck(null,invalid))
    assertSame(cause,assertThrows(classOf[IllegalStateException],()=>router.begin(tag(),MySqlCompletionChannel.Ordinary,true)))

  @Test def checkpointReadReplyWithoutOriginalConfirmationRetainsFailedControlSlot():Unit =
    val original=tag();val router=new MySqlTrackedDriftCompletionRouter(identity,1)
    assertTrue(router.begin(original,MySqlCompletionChannel.Ordinary,true))
    router.submitted(original,IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
    router.take(MySqlCompletionChannel.Ordinary,1)
    router.beginRelease(original,MySqlCompletionChannel.Ordinary);router.submitted(original,IoSubmission.Accepted)
    val invalid=IoCompletion[MySqlOwnedIoReply](original,Success(MySqlOwnedIoReply.CheckpointMainReadReleased(null)))
    router.routeBatch(Vector(invalid));assertSame(invalid,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertEquals(1,router.retainedCount)
    assertTrue(router.failure.nonEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>router.finishCheckpointReadAck(null,invalid))

  @Test def journalCaptureTransfersTheSameCapacityOneSlotOnlyAfterExactTakenAck():Unit =
    // 只验证真实router槽与completion引用；这里的metadata不证明SQL/core转移资格。
    for result<-Vector(IoSubmission.Accepted,IoSubmission.RejectedCapacity) do
      val j=tag();val c=tag();val router=new MySqlTrackedDriftCompletionRouter(identity,1)
      val jh=new MySqlRetainedPlanHandle(null,1L,j,null)
      val ch=new MySqlRetainedPlanHandle(null,2L,c,null)
      val capture=new MySqlDbFirstCaptureReservation(null,ch,null)
      def ticket()=new MySqlDbFirstRecallJournalCaptureTransfer(null,jh,capture,UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),7L)
      val transfer=ticket()
      assertTrue(router.begin(j,MySqlCompletionChannel.Ordinary,true))
      router.submitted(j,IoSubmission.Accepted)
      router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
      router.take(MySqlCompletionChannel.Ordinary,1)
      router.beginRelease(j,MySqlCompletionChannel.Ordinary);router.submitted(j,IoSubmission.Accepted)
      val ack=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.JournalRetiredForCapture(transfer)))
      assertThrows(classOf[IllegalStateException],()=>router.beginJournalCapture(transfer))
      router.routeBatch(Vector(ack))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishJournalCaptureAck(transfer,ack))
      assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
      assertFalse(router.begin(tag(),MySqlCompletionChannel.Ordinary,true))
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRelease(j,MySqlCompletionChannel.Ordinary))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishJournalCaptureAck(transfer,IoCompletion[MySqlOwnedIoReply](j,ack.result)))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishJournalCaptureAck(ticket(),ack))
      router.finishJournalCaptureAck(transfer,ack)
      assertEquals(1,router.retainedCount)
      assertFalse(router.begin(j,MySqlCompletionChannel.Ordinary,true))
      assertThrows(classOf[IllegalStateException],()=>router.finishJournalCaptureAck(transfer,ack))
      assertThrows(classOf[IllegalArgumentException],()=>router.beginJournalCapture(ticket()))
      router.beginJournalCapture(transfer);router.submitted(c,result)
      if result==IoSubmission.Accepted then
        router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](c,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
        assertEquals(Vector(c),router.take(MySqlCompletionChannel.Ordinary,1).map(_.tag))
      assertEquals(1,router.retainedCount)
      assertFalse(router.begin(tag(),MySqlCompletionChannel.Ordinary,true))
      assertThrows(classOf[IllegalArgumentException],()=>router.beginJournalCapture(transfer))
      assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty())

  @Test def journalCaptureNullWorkerReplyRetainsOriginalSlotAndFailure():Unit =
    val j=tag();val router=new MySqlTrackedDriftCompletionRouter(identity,1)
    assertTrue(router.begin(j,MySqlCompletionChannel.Ordinary,true));router.submitted(j,IoSubmission.Accepted)
    router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
    router.take(MySqlCompletionChannel.Ordinary,1)
    router.beginRelease(j,MySqlCompletionChannel.Ordinary);router.submitted(j,IoSubmission.Accepted)
    val ack=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.JournalRetiredForCapture(null)))
    router.routeBatch(Vector(ack));assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
    assertEquals(1,router.retainedCount)
    assertSame(router.failure.get,assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty()))

  /** 仅真实router与既有canonical DATA；null core不构成已注册Main决定或SQL许可。 */
  private def withBootRefusalRoute(body:(MySqlDbFirstBootRecallMainDecision,MySqlDbFirstRecallRefusalConfirmation)=>Unit):Unit =
    val profile=com.wi.floatingpod.config.SourceLimitProfile.of(1048576L,1048576L,1048576L,64L,4096L,8388608L,1048576L,1048576L)
    val root=new MySqlFormat3ReadBudget(profile,1048576L)
    try
      val execution=tag();val old=execution.copy(bootId=UUID.randomUUID())
      val journalTag=old.copy(operationId=UUID.randomUUID())
      val journal=MySqlDbFirstRecallJournalRequest.fresh(journalTag,old,UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),7L,None,root)
      val original=new MySqlDbFirstRecallRequestRecord(journalTag,old,journal.bindingUuid,journal.actorUuid,journal.podUuid,
        7L,None,journal.canonical.hexSha256,MySqlDbFirstRecallRequestPhase.JournalCommitted)
      val intent=new MySqlDbFirstBootRecallTakeoverIntent(tag(),execution,original)
      val handle=new MySqlRetainedPlanHandle(null,1L,execution,null)
      val reason=com.wi.floatingpod.domain.RecallRefusalReason.Busy
      val source=new MySqlDbFirstBootRecallMainDecision(null,intent,Some(reason),handle)
      val request=MySqlDbFirstRecallRefusalRequest.notAppliedRecoveredWithReason(execution,original,reason,root)
      body(source,MySqlDbFirstRecallRefusalConfirmation.issue(null,handle,request,"0"*64))
    finally root.closeRootReadBudget()

  @Test def bootRefusalFirstCSlotKeepsHistoricalJAndRequiresExactReadAndFinalAcks():Unit =
    withBootRefusalRoute { (source,confirmation)=>
      val router=new MySqlTrackedDriftCompletionRouter(identity,1);val c=source.executionTag
      assertTrue(router.beginRecallRefusal(source),"no old J slot or promote required")
      router.submitted(c,IoSubmission.Accepted)
      val observation=MySqlDbFirstSession.DbFirstRecallRefusalObservation(c,source.journalTag,confirmation.podUuid,
        true,true,true,"0"*64,Some(c))
      assertNotEquals(c,observation.journalTag);assertEquals(c,observation.routeTag)
      val observed=IoCompletion[MySqlOwnedIoReply](c,Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observation)))
      router.routeBatch(Vector(observed));assertSame(observed,router.take(MySqlCompletionChannel.Ordinary,1).head)
      router.beginRelease(c,MySqlCompletionChannel.Ordinary);router.submitted(c,IoSubmission.Accepted)
      val readAck=IoCompletion[MySqlOwnedIoReply](c,Success(MySqlOwnedIoReply.RefusalMainReadReleased(confirmation)))
      router.routeBatch(Vector(readAck));router.take(MySqlCompletionChannel.Ordinary,1)
      router.finishRefusalReadAck(confirmation,readAck)
      val receipt=MySqlDbFirstRecallRefusalFinalReceipt.issue(confirmation)
      assertEquals(source.originalTag,receipt.originalTag)
      router.prepareRefusalFinal(source,receipt)
      router.beginRefusalFinal(receipt);router.submitted(c,IoSubmission.Accepted)
      val finalAck=IoCompletion[MySqlOwnedIoReply](c,Success(MySqlOwnedIoReply.RefusalFinalReleased(receipt)))
      router.routeBatch(Vector(finalAck));router.take(MySqlCompletionChannel.Ordinary,1)
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalFinalAck(receipt,IoCompletion[MySqlOwnedIoReply](c,finalAck.result)))
      router.finishRefusalFinalAck(receipt,finalAck);assertEquals(0,router.retainedCount);router.requireTerminalEmpty()
    }

  @Test def bootFirstCRefusalCapacityPositiveDecisionRejectedOrUnknownNeverRemintsRoute():Unit =
    withBootRefusalRoute { (source,_)=>
      val full=new MySqlTrackedDriftCompletionRouter(identity,1);val sibling=tag()
      assertTrue(full.begin(sibling,MySqlCompletionChannel.Ordinary,false));full.submitted(sibling,IoSubmission.Accepted)
      assertFalse(full.beginRecallRefusal(source));assertEquals(1,full.retainedCount)
      val positive=new MySqlDbFirstBootRecallMainDecision(null,source.intent,None,source.handle)
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlTrackedDriftCompletionRouter(identity,1).beginRecallRefusal(positive))
      for unknown<-Vector(false,true) do
        val router=new MySqlTrackedDriftCompletionRouter(identity,1)
        assertTrue(router.beginRecallRefusal(source))
        if unknown then
          val cause=new java.io.IOException("actual submit outcome unknown")
          router.submissionFailed(source.routeTag,cause)
          assertSame(cause,router.failure.get)
        else router.submitted(source.routeTag,IoSubmission.RejectedCapacity)
        assertEquals(1,router.retainedCount)
        if unknown then assertSame(router.failure.get,assertThrows(classOf[java.io.IOException],()=>router.beginRecallRefusal(source)))
        else assertThrows(classOf[IllegalArgumentException],()=>router.beginRecallRefusal(source))
    }

  private def withRefusalRoute(body:(MySqlDbFirstRecallPreCaptureExclusion,MySqlDbFirstRecallRefusalConfirmation)=>Unit):Unit =
    val profile=com.wi.floatingpod.config.SourceLimitProfile.of(1048576L,1048576L,1048576L,64L,4096L,8388608L,1048576L,1048576L)
    val root=new MySqlFormat3ReadBudget(profile,1048576L)
    try
      val j=tag();val c=tag()
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,c,UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),7L,None,root)
      val handle=new MySqlRetainedPlanHandle(null,1L,j,null)
      val reservation=new MySqlDbFirstRecallJournalReservation(null,handle,request)
      val exclusion=new MySqlDbFirstRecallPreCaptureExclusion(null,reservation,new java.io.IOException("routing metadata only"))
      val refusal=MySqlDbFirstRecallRefusalRequest.notApplied(c,request,root)
      val confirmation=MySqlDbFirstRecallRefusalConfirmation.issue(null,handle,refusal,"0"*64)
      body(exclusion,confirmation)
    finally root.closeRootReadBudget()

  @Test def refusalReusesJCapacityOneSlotAndRequiresExactTakenReadAckWithoutTerminalRelease():Unit =
    // 只验证真实router与codec metadata，不签发实际SQL或core确认。
    withRefusalRoute { (exclusion,confirmation)=>
      val j=exclusion.journalTag;val c=exclusion.originalTag
      val router=new MySqlTrackedDriftCompletionRouter(identity,1)
      assertTrue(router.begin(j,MySqlCompletionChannel.Ordinary,true));router.submitted(j,IoSubmission.Accepted)
      router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
      router.take(MySqlCompletionChannel.Ordinary,1)
      router.beginRecallRefusal(exclusion);router.submitted(j,IoSubmission.Accepted)
      val observation=MySqlDbFirstSession.DbFirstRecallRefusalObservation(c,j,confirmation.podUuid,true,true,true,"0"*64)
      val observed=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observation)))
      router.routeBatch(Vector(observed));assertSame(observed,router.take(MySqlCompletionChannel.Ordinary,1).head)
      assertEquals(1,router.retainedCount);assertFalse(router.begin(c,MySqlCompletionChannel.Ordinary,true))
      router.beginRelease(j,MySqlCompletionChannel.Ordinary);router.submitted(j,IoSubmission.Accepted)
      val ack=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.RefusalMainReadReleased(confirmation)))
      router.routeBatch(Vector(ack))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalReadAck(confirmation,ack))
      assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalReadAck(confirmation,IoCompletion[MySqlOwnedIoReply](j,ack.result)))
      router.finishRefusalReadAck(confirmation,ack)
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRecallRefusal(exclusion))
      assertEquals(1,router.retainedCount);assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty())
    }

  @Test def refusalRejectedAndWrongOriginalCReplyRetainTheOriginalJSlot():Unit =
    for rejected<-Vector(false,true) do
      withRefusalRoute { (exclusion,confirmation)=>
        val j=exclusion.journalTag;val router=new MySqlTrackedDriftCompletionRouter(identity,1)
        assertTrue(router.begin(j,MySqlCompletionChannel.Ordinary,true));router.submitted(j,IoSubmission.Accepted)
        router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
        router.take(MySqlCompletionChannel.Ordinary,1);router.beginRecallRefusal(exclusion)
        router.submitted(j,if rejected then IoSubmission.RejectedCapacity else IoSubmission.Accepted)
        if !rejected then
          val foreign=MySqlDbFirstSession.DbFirstRecallRefusalObservation(tag(),j,confirmation.podUuid,true,true,true,"0"*64)
          assertThrows(classOf[IllegalStateException],()=>router.routeBatch(Vector(
            IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(foreign))))))
          assertTrue(router.failure.nonEmpty)
        assertEquals(1,router.retainedCount)
        assertThrows(classOf[IllegalArgumentException],()=>router.beginRecallRefusal(exclusion))
        assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty())
      }

  private def withPreparedRefusalFinalRoute(body:(MySqlTrackedDriftCompletionRouter,
      MySqlDbFirstRecallRefusalFinalReceipt,MySqlDbFirstRecallRefusalFinalReceipt)=>Unit):Unit =
    // 实际router行为测试，synthetic metadata不证明SQL成功或core退休资格。
    withRefusalRoute { (exclusion,confirmation)=>
      val j=exclusion.journalTag;val c=exclusion.originalTag
      val router=new MySqlTrackedDriftCompletionRouter(identity,1)
      assertTrue(router.begin(j,MySqlCompletionChannel.Ordinary,true));router.submitted(j,IoSubmission.Accepted)
      router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
      router.take(MySqlCompletionChannel.Ordinary,1)
      router.beginRecallRefusal(exclusion);router.submitted(j,IoSubmission.Accepted)
      val observed=MySqlDbFirstSession.DbFirstRecallRefusalObservation(c,j,confirmation.podUuid,true,true,true,"0"*64)
      router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.DbFirstRecallRefusalObserved(observed)))))
      router.take(MySqlCompletionChannel.Ordinary,1)
      val receipt=MySqlDbFirstRecallRefusalFinalReceipt.issue(confirmation)
      assertThrows(classOf[IllegalArgumentException],()=>router.prepareRefusalFinal(exclusion,receipt))
      router.beginRelease(j,MySqlCompletionChannel.Ordinary);router.submitted(j,IoSubmission.Accepted)
      val ack=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.RefusalMainReadReleased(confirmation)))
      router.routeBatch(Vector(ack));router.take(MySqlCompletionChannel.Ordinary,1)
      assertThrows(classOf[IllegalArgumentException],()=>router.prepareRefusalFinal(exclusion,receipt))
      router.finishRefusalReadAck(confirmation,ack)
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRefusalFinal(receipt))
      router.prepareRefusalFinal(exclusion,receipt)
      assertThrows(classOf[IllegalArgumentException],()=>router.prepareRefusalFinal(exclusion,receipt))
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRecallRefusal(exclusion))
      body(router,receipt,MySqlDbFirstRecallRefusalFinalReceipt.issue(confirmation))
    }

  @Test def refusalFinalReusesJSlotAndOnlyExactTakenAckReleasesBothRoutingIdentities():Unit =
    withPreparedRefusalFinalRoute { (router,receipt,counterfeit)=>
      val j=receipt.tag
      assertEquals(receipt.originalTag,counterfeit.originalTag)
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRefusalFinal(counterfeit))
      router.beginRefusalFinal(receipt);router.submitted(j,IoSubmission.Accepted)
      val ack=IoCompletion[MySqlOwnedIoReply](j,Success(MySqlOwnedIoReply.RefusalFinalReleased(receipt)))
      assertThrows(classOf[IllegalArgumentException],()=>router.requireRefusalFinalAck(receipt,ack))
      router.routeBatch(Vector(ack))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalFinalAck(receipt,ack))
      assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
      assertEquals(1,router.retainedCount)
      assertFalse(router.begin(receipt.originalTag,MySqlCompletionChannel.Ordinary,true))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalFinalAck(receipt,IoCompletion[MySqlOwnedIoReply](j,ack.result)))
      assertThrows(classOf[IllegalArgumentException],()=>router.finishRefusalFinalAck(counterfeit,ack))
      assertThrows(classOf[IllegalArgumentException],()=>router.beginRefusalFinal(receipt))
      router.finishRefusalFinalAck(receipt,ack)
      assertEquals(0,router.retainedCount);router.requireTerminalEmpty()
      assertThrows(classOf[IllegalStateException],()=>router.finishRefusalFinalAck(receipt,ack))
    }

  @Test def refusalFinalRejectedOrUnknownRetainsOneShotOriginalSlot():Unit =
    for unknown<-Vector(false,true) do
      withPreparedRefusalFinalRoute { (router,receipt,_)=>
        router.beginRefusalFinal(receipt)
        if unknown then
          val cause=new java.io.IOException("original final submission unknown")
          router.submissionFailed(receipt.tag,cause)
          assertSame(cause,router.failure.get)
          assertSame(cause,assertThrows(classOf[java.io.IOException],()=>router.beginRefusalFinal(receipt)))
        else
          router.submitted(receipt.tag,IoSubmission.RejectedCapacity)
          assertThrows(classOf[IllegalArgumentException],()=>router.beginRefusalFinal(receipt))
          assertFalse(router.begin(receipt.originalTag,MySqlCompletionChannel.Ordinary,true))
        assertEquals(1,router.retainedCount)
        assertThrows(classOf[Exception],()=>router.requireTerminalEmpty())
      }

  @Test def refusalFinalCounterfeitOrUnrelatedWorkerReplyRetainsInvariantBatchAndSlot():Unit =
    for wrongKind<-Vector(false,true) do
      withPreparedRefusalFinalRoute { (router,receipt,counterfeit)=>
        router.beginRefusalFinal(receipt);router.submitted(receipt.tag,IoSubmission.Accepted)
        val reply=if wrongKind then MySqlOwnedIoReply.Transaction(Vector.empty)
          else MySqlOwnedIoReply.RefusalFinalReleased(counterfeit)
        assertThrows(classOf[IllegalStateException],()=>router.routeBatch(Vector(IoCompletion[MySqlOwnedIoReply](receipt.tag,Success(reply)))))
        assertEquals(1,router.retainedCount)
        assertSame(router.failure.get,assertThrows(classOf[IllegalStateException],()=>router.requireTerminalEmpty()))
      }
