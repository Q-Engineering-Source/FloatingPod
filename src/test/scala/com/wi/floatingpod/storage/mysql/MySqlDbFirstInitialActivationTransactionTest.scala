package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.{DbFirstPodSnapshot,DbFirstPodSnapshotTest}
import com.wi.floatingpod.server.io.{IoOperationTag,IoSubmission}
import java.io.IOException
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue,CountDownLatch,TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真实core与runner提交边界；不制造JDBC资格或声称执行SQL。 */
final class MySqlDbFirstInitialActivationTransactionTest:
  @Test def commitAttemptAckAndReadbackRemainDistinctAndCannotReplay():Unit =
    val unknown=new MySqlDbFirstInitialActivationTransaction.Progress(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    unknown.begin();unknown.attemptCommit();unknown.fail()
    assertTrue(unknown.commitAttempted);assertFalse(unknown.acknowledged)
    assertEquals(MySqlDbFirstSession.DbFirstInitialActivationOutcome.CommitUnknown,unknown.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.begin())
    val ack=new MySqlDbFirstInitialActivationTransaction.Progress(Thread.currentThread())
    ack.begin();ack.attemptCommit();ack.acknowledge();ack.fail()
    assertTrue(ack.commitAttempted);assertTrue(ack.acknowledged)
    assertEquals(MySqlDbFirstSession.DbFirstInitialActivationOutcome.AcknowledgedPending,ack.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>ack.verifyReadback())
    val foreign=new AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try ack.attemptCommit()
      catch case cause:Throwable=>foreign.set(cause))
    thread.start();thread.join(4000L)
    assertFalse(thread.isAlive);assertInstanceOf(classOf[IllegalArgumentException],foreign.get())

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
      val reservation=core.reserveDbFirstCapture(IoOperationTag(identity.bootId,1L,1L,new UUID(0L,912L)),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      core.captureDbFirstSnapshot(reservation)(allocation=>DbFirstPodSnapshotTest.captureInitialForCore(reservation.tag.operationId,allocation))
      val fake=new MySqlDbFirstCaptureReservation(core,reservation.handle,profile)
      assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstInitialActivationSubmission(fake))
      core.requireDbFirstInitialActivationSubmission(reservation)
      assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstInitialActivationSubmission(reservation))
      var runner:MySqlDbFirstInitialActivationTransaction=null
      onWorker {
        assertThrows(classOf[IllegalArgumentException],()=>core.beginDbFirstInitialActivation(reservation,0))
        runner=core.beginDbFirstInitialActivation(reservation,256)
        assertEquals(256,runner.activeLimit)
        core.requireExecutingDbFirstInitialActivation(runner)
        assertSame(reservation,runner.reservation)
        assertThrows(classOf[IllegalArgumentException],()=>runner.requireConfirmedReadbackDraft(null))
        assertThrows(classOf[IllegalArgumentException],()=>runner.recordConfirmedDraftDetached(null))
        val missingDraft=assertThrows(classOf[IllegalStateException],()=>core.detachConfirmedDbFirstInitialDraft(runner))
        assertEquals("original initial draft missing",missingDraft.getMessage)
        assertFalse(runner.resourcesRetired)
        assertFalse(runner.commitAttempted);assertFalse(runner.acknowledged)
        val retained=ledger.accountedBytes
        assertThrows(classOf[IllegalArgumentException],()=>core.beginDbFirstInitialActivation(reservation,256))
        assertEquals(retained,ledger.accountedBytes)
      }
      core.submittedDbFirstInitialActivation(reservation,IoSubmission.Accepted)
      val observed=core.observation.plans.head
      assertTrue(observed.accepted);assertTrue(observed.workerObserved)
      assertThrows(classOf[IllegalArgumentException],()=>core.submittedDbFirstInitialActivation(reservation,IoSubmission.RejectedCapacity))
      val held=ledger.accountedBytes
      val counterfeitObservation=MySqlDbFirstSession.DbFirstInitialActivationObservation(reservation.tag,runner.request.podUuid,
        true,true,true,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>core.claimDbFirstInitialActivationConfirmation(reservation,counterfeitObservation))
      val counterfeitConfirmation=MySqlDbFirstInitialActivationConfirmation.issue(core,reservation.handle,runner.request)
      assertEquals(runner.request.creationOperationUuid,counterfeitConfirmation.creationOperationUuid)
      assertThrows(classOf[IllegalArgumentException],()=>counterfeitConfirmation.requireOriginal(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>core.recordDbFirstInitialPublication(counterfeitConfirmation))
      assertThrows(classOf[IllegalArgumentException],()=>core.recordMainDrop(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>core.cancelExplicitlyRejected(reservation.handle))
      val counterfeitReceipt=MySqlDbFirstInitialPublicationReceipt.issue(core,reservation.handle)
      onWorker {
        assertThrows(classOf[IllegalArgumentException],()=>core.requireDbFirstInitialPublication(counterfeitReceipt))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstInitialLocalRetirementException.fromCore(core,counterfeitReceipt,new IOException("unqualified local failure")))
        assertThrows(classOf[IllegalArgumentException],()=>runner.requireConfirmedObservation(counterfeitObservation))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>core.releaseDropped(new MySqlRetainedPlanDropReceipt(reservation.handle)))
        assertFalse(runner.resourcesRetired)
      }
      assertEquals(held,ledger.accountedBytes,"forged/unconfirmed cleanup must leave original resources charged")
      assertTrue(core.observation.failure.isEmpty,"local rejected cleanup must not freeze the shared issuer")
      assertTrue(core.observation.plans.head.cause.isEmpty,"rejected counterfeit must not poison the original Entry")
      val original=new IOException("actual submission response was lost")
      core.submissionUnknownDbFirstInitialActivation(reservation,original)
      assertSame(original,core.observation.plans.head.cause.get)
      assertSame(original,core.observation.failure.get)
      assertTrue(ledger.accountedBytes>0L,"unresolved original capture/runner must retain actual charges")
      onWorker { assertSame(original,assertThrows(classOf[IOException],()=>core.requireStoppedEmptyForTerminal())) }
    finally
      assertTrue(queue.offer(poison,3L,TimeUnit.SECONDS))
      worker.join(4000L)
      assertFalse(worker.isAlive)
