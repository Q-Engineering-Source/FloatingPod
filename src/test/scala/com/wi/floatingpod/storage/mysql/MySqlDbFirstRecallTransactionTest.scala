package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshotTest
import com.wi.floatingpod.server.io.{IoOperationTag,IoSubmission}
import java.io.IOException
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue,CountDownLatch,TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真实core与runner提交边界；不制造JDBC资格或声称执行SQL。 */
final class MySqlDbFirstRecallTransactionTest:
  @Test def failedCommitObservationRequiresOriginalUnverifiedFailureAndCannotChangeOutcome():Unit =
    val fresh=new MySqlDbFirstRecallTransaction.Progress(Thread.currentThread())
    assertFalse(fresh.failedCommitObservationPending)
    assertThrows(classOf[IllegalArgumentException],()=>fresh.requireFailedCommitObservation())
    fresh.begin();fresh.fail()
    assertFalse(fresh.failedCommitObservationPending)
    assertThrows(classOf[IllegalArgumentException],()=>fresh.requireFailedCommitObservation())
    Vector(false,true).foreach { acknowledged=>
      val progress=new MySqlDbFirstRecallTransaction.Progress(Thread.currentThread())
      progress.begin();progress.attemptCommit()
      if acknowledged then progress.acknowledge()
      assertFalse(progress.failedCommitObservationPending)
      assertThrows(classOf[IllegalArgumentException],()=>progress.requireFailedCommitObservation())
      progress.fail()
      assertTrue(progress.failedCommitObservationPending)
      val original=progress.outcome
      progress.requireFailedCommitObservation();progress.requireFailedCommitObservation()
      assertEquals(original,progress.outcome)
      assertThrows(classOf[IllegalArgumentException],()=>progress.attemptCommit())
      assertThrows(classOf[IllegalArgumentException],()=>progress.verifyReadback())
      val foreign=new AtomicReference[Throwable]()
      val thread=Thread.ofPlatform().unstarted(()=>
        try progress.requireFailedCommitObservation()
        catch case cause:Throwable=>foreign.set(cause)
        ())
      thread.start();thread.join(4000L)
      assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalArgumentException],foreign.get())
      assertEquals(original,progress.outcome)
    }
    val verified=new MySqlDbFirstRecallTransaction.Progress(Thread.currentThread())
    verified.begin();verified.attemptCommit();verified.acknowledge();verified.verifyReadback();verified.fail()
    assertFalse(verified.failedCommitObservationPending)
    assertThrows(classOf[IllegalArgumentException],()=>verified.requireFailedCommitObservation())

  @Test def commitAttemptAckAndReadbackRemainDistinctAndCannotReplay():Unit =
    val unknown=new MySqlDbFirstRecallTransaction.Progress(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    unknown.begin();unknown.attemptCommit();unknown.fail()
    assertTrue(unknown.commitAttempted);assertFalse(unknown.acknowledged)
    assertEquals(MySqlDbFirstSession.DbFirstRecallOutcome.CommitUnknown,unknown.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.begin())
    val ack=new MySqlDbFirstRecallTransaction.Progress(Thread.currentThread())
    ack.begin();ack.attemptCommit();ack.acknowledge();ack.fail()
    assertTrue(ack.commitAttempted);assertTrue(ack.acknowledged)
    assertEquals(MySqlDbFirstSession.DbFirstRecallOutcome.AcknowledgedPending,ack.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>ack.verifyReadback())
    val foreign=new AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try ack.attemptCommit()
      catch case cause:Throwable=>foreign.set(cause))
    thread.start();thread.join(4000L)
    assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalArgumentException],foreign.get())

  @Test def actualRecallObservationBoundaryRetainsOrdinaryFailureButPropagatesFatal():Unit =
    val original=new IOException("original commit reply lost")
    val ordinary=new IOException("actual one-shot observation failed")
    MySqlDbFirstSession.observeRetainingRecallFailure(original)(throw ordinary)
    assertEquals(Vector(ordinary),original.getSuppressed.toVector)
    MySqlDbFirstSession.observeRetainingRecallFailure(original)(throw original)
    assertEquals(Vector(ordinary),original.getSuppressed.toVector,"original cause cannot suppress itself")
    val fatal=new LinkageError("observation fatal must escape unchanged")
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>
      MySqlDbFirstSession.observeRetainingRecallFailure(original)(throw fatal)))
    assertEquals(Vector(ordinary),original.getSuppressed.toVector,"boundary must not downgrade Fatal into suppressed")
    MySqlDbFirstSession.observeRetainingRecallFailure(original)(())
    assertEquals(Vector(ordinary),original.getSuppressed.toVector)

  @Test def originalCoreRetainsOneRunnerAcrossWorkerBeforeAcceptedAndSubmissionUnknown():Unit =
    val base=DeploymentReceiptStaticTestData.defaultProfile()
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(base))
    val profile=inputs.sourceLimitProfile
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val main=Thread.currentThread()
    val queue=new ArrayBlockingQueue[Runnable](1)
    val poison=new Runnable { def run():Unit=() }
    val worker=new Thread(()=>
      var alive=true
      while alive do
        val action=queue.take()
        if action eq poison then alive=false else action.run()
      ())
    val identity=MySqlOwnedIoIdentity(new UUID(0L,1L),1L,1L)
    val core=new MySqlRetainedPlanReservationCore(identity,worker,main,profile,ledger,1,None,None,Some(inputs))
    def onWorker(body: => Unit):Unit =
      val done=new CountDownLatch(1)
      val error=new AtomicReference[Throwable]()
      assertTrue(queue.offer(new Runnable {
        def run():Unit =
          try body
          catch case cause:Throwable=>error.set(cause)
          finally done.countDown()
      },3L,TimeUnit.SECONDS))
      assertTrue(done.await(3L,TimeUnit.SECONDS),"actual core worker did not return")
      Option(error.get()).foreach(throw _)
    worker.start()
    try
      val reservation=core.reserveDbFirstCapture(IoOperationTag(identity.bootId,1L,1L,new UUID(0L,912L)))
      core.captureDbFirstSnapshot(reservation)(allocation=>DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,allocation,100))
      val fake=new MySqlDbFirstCaptureReservation(core,reservation.handle,profile)
      assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstRecallSubmission(fake))
      core.requireDbFirstRecallSubmission(reservation)
      assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstRecallSubmission(reservation))
      var runner:MySqlDbFirstRecallTransaction=null
      onWorker {
        runner=core.beginDbFirstRecall(reservation)
        assertSame(reservation,runner.reservation)
        assertFalse(runner.commitAttempted);assertFalse(runner.acknowledged)
        val retained=ledger.accountedBytes
        assertThrows(classOf[IllegalArgumentException],()=>core.requireFailedDbFirstRecallObservation(runner))
        assertThrows(classOf[IllegalArgumentException],()=>core.beginDbFirstRecall(reservation))
        assertEquals(retained,ledger.accountedBytes)
      }
      core.submittedDbFirstRecall(reservation,IoSubmission.Accepted)
      val observed=core.observation.plans.head
      assertTrue(observed.accepted);assertTrue(observed.workerObserved)
      assertThrows(classOf[IllegalArgumentException],()=>core.submittedDbFirstRecall(reservation,IoSubmission.RejectedCapacity))
      val held=ledger.accountedBytes
      val counterfeitObservation=MySqlDbFirstSession.DbFirstRecallObservation(reservation.tag,runner.request.podUuid,
        true,true,true,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>core.claimDbFirstRecallConfirmation(reservation,counterfeitObservation))
      val counterfeitConfirmation=MySqlDbFirstRecallConfirmation.issue(core,reservation.handle,runner.request)
      assertThrows(classOf[IllegalArgumentException],()=>counterfeitConfirmation.requireOriginal(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>core.recordDbFirstMainDrop(counterfeitConfirmation))
      assertThrows(classOf[IllegalArgumentException],()=>core.recordMainDrop(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>core.cancelExplicitlyRejected(reservation.handle))
      val counterfeitDrop=MySqlDbFirstRecallDropReceipt.issue(core,reservation.handle)
      onWorker {
        assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstDrop(counterfeitDrop))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstLocalRetirementException.fromCore(
          core,counterfeitDrop,new IllegalStateException("unqualified counterfeit failure")))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireFailedSqlResources(null,null))
        assertFalse(runner.failedSqlResourcesRetired)
        assertThrows(classOf[IllegalArgumentException],()=>core.releaseDropped(new MySqlRetainedPlanDropReceipt(reservation.handle)))
        assertFalse(runner.resourcesRetired)
      }
      assertEquals(held,ledger.accountedBytes,"forged/unconfirmed cleanup must leave original resources charged")
      assertTrue(core.observation.failure.isEmpty,"local rejected cleanup must not freeze the shared issuer")
      assertTrue(core.observation.plans.head.cause.isEmpty,"rejected counterfeit must not poison the original Entry")
      val original=new IOException("actual submission response was lost")
      core.submissionUnknownDbFirstRecall(reservation,original)
      onWorker {
        assertThrows(classOf[IllegalArgumentException],()=>core.requireFailedDbFirstRecallObservation(runner))
        val counterfeit=new MySqlDbFirstRecallTransaction(reservation,runner.request,runner.root)
        assertThrows(classOf[IllegalArgumentException],()=>core.requireFailedDbFirstRecallObservation(counterfeit))
      }
      assertSame(original,core.observation.plans.head.cause.get)
      assertSame(original,core.observation.failure.get)
      assertTrue(ledger.accountedBytes>0L,"unresolved original capture/runner must retain actual charges")
      onWorker { assertSame(original,assertThrows(classOf[IOException],()=>core.requireStoppedEmptyForTerminal())) }
    finally
      assertTrue(queue.offer(poison,3L,TimeUnit.SECONDS))
      worker.join(4000L)
      assertFalse(worker.isAlive)
