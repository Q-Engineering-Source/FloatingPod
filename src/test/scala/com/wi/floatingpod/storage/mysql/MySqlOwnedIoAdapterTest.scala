package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.*
import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

/** 实际adapter拒绝/OwnedSerialIo线程验证；没有Boot数据库成功或fake driver注入。 */
class MySqlOwnedIoAdapterTest:
  @Test def retainedRecallQualificationRefusalKeepsOriginalFailureAndDoesNotDowngradeFatal():Unit =
    val original=new java.io.IOException("original recall UNKNOWN")
    val refused=new IllegalArgumentException("actual qualification not current")
    assertSame(original,assertThrows(classOf[java.io.IOException],()=>
      MySqlOwnedIoAdapter.qualifyRetainedRecallFailure[Unit](original)(throw refused)))
    assertEquals(Vector(refused),original.getSuppressed.toVector)
    val fatal=new LinkageError("qualification boundary fatal")
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>
      MySqlOwnedIoAdapter.qualifyRetainedRecallFailure[Unit](original)(throw fatal)))
    assertEquals(Vector(refused),original.getSuppressed.toVector)
    var entered=false
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>
      MySqlOwnedIoAdapter.qualifyRetainedRecallFailure[Unit](fatal) { entered=true }))
    assertFalse(entered)
    val wrapped=new IllegalStateException("qualification diagnostic retaining original",original)
    assertSame(original,assertThrows(classOf[java.io.IOException],()=>
      MySqlOwnedIoAdapter.qualifyRetainedRecallFailure[Unit](original)(throw wrapped)))
    assertEquals(Vector(refused),original.getSuppressed.toVector,"must not add a reverse edge to original failure")


  @Test def retainedExitObservationDoesNotCreateCauseSuppressedCycle(): Unit =
    val primary=new java.sql.SQLException("original schema5 initialization failure")
    val retainedWrapper=new IllegalStateException("retained observation",primary)
    MySqlOwnedIoAdapter.suppressExitCleanup(primary,retainedWrapper)
    assertEquals(0,primary.getSuppressed.length)
    val cleanup=new java.sql.SQLException("actual independent JDBC close failure")
    MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)
    MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)
    assertEquals(1,primary.getSuppressed.length)
    assertSame(cleanup,primary.getSuppressed.head)
    assertNull(cleanup.getCause)
    val observation=MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved("session-install",Some(primary),true)
    observation match
      case MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(stage,failure,retained) =>
        assertEquals("session-install",stage);assertSame(primary,failure.get);assertTrue(retained)
      case _ => fail("schema5 failure must retain its own family")

  private val limits = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, 1048576L, 1048576L, 1048576L)
  private def plan(tag: IoOperationTag, operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    MySqlOwnedRepositoryOperationPlan.single(tag, UUID.randomUUID(), UUID.randomUUID(), operation, limits)
  private def join(thread: Thread): Unit =
    thread.join(5000)
    assertFalse(thread.isAlive, "bounded thread join timeout")
  private def waitOff(wait: () => Boolean): Unit =
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      try assertTrue(wait(), "off-Main actual worker exit timeout")
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "adapter-off-Main-tail")
    var primary: Throwable = null
    try
      waiter.start()
      join(waiter)
      if failure.get() != null then throw failure.get()
    catch case cause: Throwable => primary = cause
    finally
      try join(waiter)
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary
  private def identity() = MySqlOwnedIoIdentity(UUID.randomUUID(), 1L, 1L)
  private def tag(id: MySqlOwnedIoIdentity) = IoOperationTag(id.bootId, id.bootGeneration, id.connectionGeneration, UUID.randomUUID())
  private def managed(adapter: MySqlOwnedIoAdapter)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      try
        adapter.quiesce()
        waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  @Test def preBootAdapterCannotCreateRecoveredMainControlAckFromCallerTag():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"actual-no-service-control-adapter",id)
    managed(adapter) {
      val before=adapter.checkpoint
      assertThrows(classOf[IllegalArgumentException],()=>adapter.advanceRetainedRecallServiceMain(null,tag(id)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRegisteredRecoveredService(null,null,null,id))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.takeOverRecoveredService(null,null,null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.advanceRecoveredServicePublication(null,tag(id),null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecoveredPublicationDispatch(null,null,id))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishRecoveredServiceExecution(null,tag(id),null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecoveredOriginalCommit(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecoveredSaveResume(null,null,id))
      assertSame(id,adapter.currentExecutionIdentity)
      assertEquals(before.io.reserved,adapter.checkpoint.io.reserved)
      assertEquals(before.io.queued,adapter.checkpoint.io.queued)
      assertEquals(before.io.completions,adapter.checkpoint.io.completions)
      assertTrue(adapter.checkpoint.resources.isEmpty)
    }

  @Test def actualQuiesceRejectsLaterBusinessWithoutClaimingRouterOrWorkerCredit():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"actual-stop-before-business",id)
    managed(adapter) {
      adapter.quiesce()
      val before=adapter.checkpoint.io
      assertInstanceOf(classOf[IoSubmission.RejectedStopped],adapter.submit(tag(id),MySqlOwnedIoCommand.Renew))
      val after=adapter.checkpoint.io
      assertEquals(before.reserved,after.reserved)
      assertEquals(before.queued,after.queued)
      assertEquals(before.completions,after.completions)
      assertSame(id,adapter.currentExecutionIdentity)
    }

  @Test def rejectedForeignQuiesceDoesNotSealActualMainBusinessAdmission():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"actual-foreign-stop-denied",id)
    managed(adapter) {
      val rejected=new AtomicReference[Throwable]()
      val foreign=new Thread(()=>{
        try adapter.quiesce()
        catch case cause:Throwable=>rejected.set(cause)
      },"foreign-stop-caller")
      foreign.start();join(foreign)
      assertInstanceOf(classOf[IllegalArgumentException],rejected.get())
      assertEquals("requirement failed: operation requires current owner Main thread",rejected.get().getMessage)
      assertEquals(IoSubmission.Accepted,adapter.submit(tag(id),MySqlOwnedIoCommand.Renew))
      adapter.quiesce()
      waitOff(()=>adapter.awaitExitOffMain(5,TimeUnit.SECONDS))
      val completed=adapter.drain(1)
      assertEquals(1,completed.size)
      assertEquals("original renewal Session missing",completed.head.result.failed.get.getMessage)
    }

  @Test def actualAdapterPreBootFailuresKeepExactTagsReservationsAndExitObservation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "actual-preBoot-adapter", id)
    managed(adapter) {
      val first = tag(id); val second = tag(id)
      assertThrows(classOf[IllegalArgumentException], () => adapter.submit(first,
        MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.FindAll, plan(first, MySqlRepositoryOperation.FindAll))))
      assertEquals(0, adapter.checkpoint.io.reserved, "unissued DATA plan cannot claim an Accepted slot")
      assertEquals(IoSubmission.Accepted, adapter.submit(first, MySqlOwnedIoCommand.Renew))
      assertEquals(IoSubmission.Accepted, adapter.submit(second, MySqlOwnedIoCommand.Renew))
      val rejected = tag(id)
      assertEquals(IoSubmission.RejectedCapacity, adapter.submit(rejected, MySqlOwnedIoCommand.Renew))
      adapter.quiesce()
      waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(2)
      assertEquals(2, completions.size)
      assertSame(first, completions.head.tag)
      assertSame(second, completions(1).tag)
      completions.foreach(c => assertEquals("original renewal Session missing", c.result.failed.get.getMessage))
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
      assertTrue(adapter.checkpoint.io.stopped)
    }

  @Test def failedBootAttemptCannotRetryEvenWithoutOpeningAnyDatabase(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "actual-failedBoot-adapter", id)
    managed(adapter) {
      adapter.submit(tag(id), MySqlOwnedIoCommand.Boot(null, null, null))
      adapter.submit(tag(id), MySqlOwnedIoCommand.Boot(null, null, null))
      adapter.quiesce()
      waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(2)
      assertEquals(2, completions.size)
      assertTrue(completions.forall(_.result.isFailure))
      assertEquals("requirement failed: explicit Boot inputs required", completions.head.result.failed.get.getMessage)
      assertEquals("requirement failed: owned MySQL Boot is one-shot", completions(1).result.failed.get.getMessage)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
    }

  @Test def schema5InvalidRequestsFailOnSameWorkerAndCannotRetryAsLegacy(): Unit =
    val commands = Vector(
      (MySqlOwnedIoCommand.BootFresh5(null, null, null, null, null), "fresh-envelope"),
      (MySqlOwnedIoCommand.BootOrdinary5(null, null, null, null, null), "ordinary-envelope"))
    commands.foreach { (command, expectedStage) =>
      val id = identity()
      val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "schema5-invalid-request", id)
      managed(adapter) {
        val first = tag(id)
        val retry = tag(id)
        assertEquals(IoSubmission.Accepted, adapter.submit(first, command))
        assertEquals(IoSubmission.Accepted, adapter.submit(retry, MySqlOwnedIoCommand.Boot(null, null, null)))
        adapter.quiesce()
        waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
        val completions = adapter.drain(2)
        assertEquals(2, completions.size)
        assertSame(first, completions.head.tag)
        assertTrue(completions.forall(_.result.isFailure), "internal schema5 WIP must never produce legacy Boot/Ready")
        assertTrue(completions.head.result.failed.get.getMessage.contains("exact schema5"))
        assertEquals("requirement failed: owned MySQL Boot is one-shot", completions(1).result.failed.get.getMessage)
        adapter.checkpoint.resources match
          case Some(MySqlOwnedIoResourceObservation.DeploymentReceiptAssemblyObserved(stage, Some(originalFailure), retained)) =>
            assertEquals(expectedStage, stage)
            assertSame(completions.head.result.failed.get, originalFailure)
            assertFalse(retained)
          case other => fail(s"exact schema5 assembly failure observation required, got $other")
        assertEquals(0, adapter.checkpoint.io.reserved)
      }
    }

  @Test def schema6InvalidObservationKeepsOriginalFailureAndCannotRetryAsLegacy(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "schema6-invalid-observation", id)
    managed(adapter) {
      val first = tag(id)
      val retry = tag(id)
      assertEquals(IoSubmission.Accepted, adapter.submit(first, MySqlOwnedIoCommand.ObserveDbFirst(null, null, null)))
      assertEquals(IoSubmission.Accepted, adapter.submit(retry, MySqlOwnedIoCommand.Boot(null, null, null)))
      adapter.quiesce()
      waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(2)
      assertEquals(2, completions.size)
      assertSame(first, completions.head.tag)
      assertTrue(completions.forall(_.result.isFailure))
      assertTrue(completions.head.result.failed.get.getMessage.contains("exact schema6"))
      assertEquals("requirement failed: owned MySQL Boot is one-shot", completions(1).result.failed.get.getMessage)
      adapter.checkpoint.resources match
        case Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(stage, Some(original), retained)) =>
          assertEquals("static-root-and-owner", stage)
          assertSame(completions.head.result.failed.get, original)
          assertFalse(retained)
        case other => fail(s"exact schema6 assembly failure observation required, got $other")
      assertEquals(0, adapter.checkpoint.io.reserved)
    }

  @org.junit.jupiter.api.io.TempDir var controlledDirectory:java.nio.file.Path = _

  @Test def actualClaimRecordSurvivesUntilLaterSchema6BackingPublication():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"schema6-original-owner-record",id)
    val receipt=new AtomicReference[IoOwnerTransfer]()
    val failure=new AtomicReference[Throwable]()
    val target=new Thread(()=>
      var claimed=false
      try
        adapter.claimOwnerTransfer(receipt.get());claimed=true
        adapter.requireRecordedOwnerClaim(Thread.currentThread(),receipt.get())
        assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecordedOwnerClaim(new Thread(()=>()),receipt.get()))
        assertThrows(classOf[IllegalArgumentException],()=>adapter.claimedDbFirstMainReads)
      catch case cause:Throwable=>failure.set(cause)
      finally if claimed then adapter.quiesce()
      (),"schema6-target-before-acquisition")
    receipt.set(adapter.issueOwnerTransfer(target,tag(id)))
    assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecordedOwnerClaim(target,receipt.get()))
    target.start();target.join(5000L)
    assertFalse(target.isAlive)
    if failure.get()!=null then throw failure.get()
    waitOff(()=>adapter.awaitExitOffMain(5,TimeUnit.SECONDS))
    adapter.requireRecordedOwnerClaim(target,receipt.get()) // 原事实不因backing尚未存在而丢失。
    assertThrows(classOf[IllegalArgumentException],()=>adapter.requireRecordedOwnerClaim(target,null))

  @Test def secondControlledBootCannotChangeOriginalFailureFamily():Unit =
    val id=identity();val first=tag(id);val second=tag(id)
    val adapter=new MySqlOwnedIoAdapter(2,Thread.currentThread(),"schema6-second-boot-family",id)
    managed(adapter) {
      assertEquals(IoSubmission.Accepted,adapter.submit(first,MySqlOwnedIoCommand.Boot(null,null,null)))
      assertEquals(IoSubmission.Accepted,adapter.submit(second,MySqlOwnedIoCommand.ObserveApprovedDbFirst(null)))
      adapter.quiesce();waitOff(()=>adapter.awaitExitOffMain(5,TimeUnit.SECONDS))
      val completions=adapter.drain(2)
      assertEquals(2,completions.size);assertTrue(completions.head.result.isFailure)
      assertEquals("requirement failed: owned MySQL Boot is one-shot",completions(1).result.failed.get.getMessage)
      assertFalse(adapter.checkpoint.resources.exists {
        case _:MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved=>true
        case _:MySqlOwnedIoResourceObservation.DbFirstObserved=>true
        case _=>false
      },"second Boot must not redirect original family cleanup")
      assertEquals(0,adapter.checkpoint.io.reserved)
    }

  @Test def schema6PreparationFailureNeverCreatesAcquireOrPermitsTransfer():Unit =
    import com.wi.floatingpod.bootstrap.{DedicatedBootstrapBridge,IoPreparationState}
    import java.util.concurrent.{CompletableFuture,CompletionStage,CompletionException}
    val f=MySqlDbFirstHistoricalTestFixture.fixture(controlledDirectory,DeploymentReceiptStaticTestData.defaultProfile())
    val id=identity();val observe=tag(id);val acquire=tag(id)
    val approved=MySqlDbFirstHistoricalTestFixture.productionControl(f,id,observe.operationId,acquire.operationId,"0"*64)
    val sentinel=new IllegalStateException("unit preparation complete; no runtime startup")
    val failure=new AtomicReference[Throwable]()
    val factory=new DedicatedBootstrapBridge.Factory {
      override def open(server:Object,control:DedicatedBootstrapBridge):DedicatedBootstrapBridge.Callbacks =
        new DedicatedBootstrapBridge.Callbacks {
          override def prepare():CompletionStage[Object] =
            val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"schema6-two-phase-failure",id)
            managed(adapter) {
              val preparation=adapter.installDbFirstBootstrapPreparation(control,approved)
              val result=preparation.prepare(Duration.ofSeconds(10))
              val wrapped=assertThrows(classOf[CompletionException],()=>result.toCompletableFuture().join())
              val original=wrapped.getCause
              assertEquals("requirement failed: schema6 assembled configuration differs from approved control",original.getMessage)
              assertEquals(1,preparation.phaseObservations.size)
              assertThrows(classOf[IllegalArgumentException],()=>adapter.claimedDbFirstMainReads)
              assertEquals(observe,preparation.phaseObservations.head.tag)
              assertEquals(IoPreparationState.ConsumedFailure(original),preparation.phaseObservations.head.state)
              assertThrows(classOf[IllegalArgumentException],()=>adapter.reserveDbFirstCapture(tag(id)))
              assertThrows(classOf[IllegalArgumentException],()=>adapter.issueOwnerTransfer(new Thread(()=>()),acquire))
              assertThrows(classOf[IllegalArgumentException],()=>preparation.prepare(Duration.ofSeconds(1)))
              preparation.finalizePreparation(Duration.ofSeconds(5))
              assertEquals(1,preparation.phaseObservations.size,"finalization must not create Acquire")
              assertSame(original,assertThrows(classOf[CompletionException],()=>preparation.stage.toCompletableFuture().join()).getCause)
              assertThrows(classOf[IllegalArgumentException],()=>adapter.issueOwnerTransfer(new Thread(()=>()),acquire))
              assertEquals(0,adapter.checkpoint.io.reserved)
            }
            CompletableFuture.failedFuture[Object](sentinel)
          override def issueTransfer(p:Object,t:Thread):Object=throw new UnsupportedOperationException("unit never starts game")
          override def claim(p:Object,r:Object):Unit=throw new UnsupportedOperationException("unit never starts game")
          override def abortTransfer(p:Object,r:Object,c:Throwable):Unit=throw new UnsupportedOperationException("unit never starts game")
          override def finalizeFailure(p:Object,r:Object,c:Throwable):CompletionStage[Void]=CompletableFuture.completedFuture[Void](null)
          override def failedStartupTermination(report:DedicatedBootstrapBridge.FailureReport):Unit =
            if report.originalCause() ne sentinel then failure.set(report.originalCause())
        }
    }
    val limit=Duration.ofSeconds(20)
    val binding=DedicatedBootstrapBridge.install(factory,new DedicatedBootstrapBridge.Limits(limit,limit,limit))
    val bridge=DedicatedBootstrapBridge.create(new Object(),()=>())
    try
      bridge.startDeferred()
      assertTrue(bridge.awaitFinishedOffGame(Duration.ofSeconds(30)))
      if failure.get()!=null then throw failure.get()
    finally
      bridge.requestCancellation(sentinel)
      if bridge.controlThread()!=null then
        bridge.controlThread().join(5000L)
        assertFalse(bridge.controlThread().isAlive)
      DedicatedBootstrapBridge.uninstall(binding)

  @Test def controlledSchema6DigestMismatchFailsBeforeSessionAndPreservesOriginalCause():Unit =
    val f=MySqlDbFirstHistoricalTestFixture.fixture(controlledDirectory,DeploymentReceiptStaticTestData.defaultProfile())
    val id=identity();val observe=tag(id);val acquire=tag(id)
    val control=MySqlDbFirstHistoricalTestFixture.productionControl(f,id,observe.operationId,acquire.operationId,"0"*64)
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"schema6-controlled-digest-mismatch",id)
    managed(adapter) {
      val command=MySqlOwnedIoCommand.ObserveApprovedDbFirst(control)
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(command))
      assertEquals(IoSubmission.Accepted,adapter.submit(observe,command))
      adapter.quiesce()
      waitOff(()=>adapter.awaitExitOffMain(10,TimeUnit.SECONDS))
      val completion=adapter.drain(1).head
      val original=completion.result.failed.get
      assertEquals("requirement failed: schema6 assembled configuration differs from approved control",original.getMessage)
      assertSame(observe,completion.tag)
      adapter.checkpoint.resources match
        case Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(stage,Some(cause),retained))=>
          assertEquals("controlled-configuration-identity",stage);assertSame(original,cause);assertFalse(retained)
        case other=>fail("original pre-SQL assembly failure required: "+other)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertFalse(java.nio.file.Files.exists(java.nio.file.Path.of(f.fields("credentialSourceRef").substring(5))))
      assertThrows(classOf[IllegalArgumentException],()=>control.consumeIdentityOnce())
    }

  @Test def controlledSchema6RejectsForeignOperationBeforeStaticAssembly():Unit =
    val f=MySqlDbFirstHistoricalTestFixture.fixture(controlledDirectory,DeploymentReceiptStaticTestData.defaultProfile())
    val id=identity();val observe=tag(id);val acquire=tag(id)
    val control=MySqlDbFirstHistoricalTestFixture.productionControl(f,id,observe.operationId,acquire.operationId,"0"*64)
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"schema6-controlled-foreign-operation",id)
    managed(adapter) {
      assertEquals(IoSubmission.Accepted,adapter.submit(acquire,MySqlOwnedIoCommand.ObserveApprovedDbFirst(control)))
      adapter.quiesce();waitOff(()=>adapter.awaitExitOffMain(5,TimeUnit.SECONDS))
      val original=adapter.drain(1).head.result.failed.get
      assertEquals("requirement failed: schema6 observation must use its exact controlled boot and operation",original.getMessage)
      adapter.checkpoint.resources match
        case Some(MySqlOwnedIoResourceObservation.DbFirstAssemblyObserved(stage,Some(cause),retained))=>
          assertEquals("controlled-observation-identity",stage);assertSame(original,cause);assertFalse(retained)
        case other=>fail("original identity failure required: "+other)
      control.consumeIdentityOnce() // rejected foreign work did not consume this control's identities.
    }

  @Test def schema6WriterCommandRequiresTheOriginalObservedSession(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-writer-without-session", id)
    managed(adapter) {
      val original = tag(id)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.reserveDbFirstCapture(tag(id)))
      assertEquals(IoSubmission.Accepted, adapter.submit(original,
        MySqlOwnedIoCommand.AcquireDbFirstWriter(Duration.ofSeconds(20), Duration.ZERO)))
      adapter.quiesce()
      waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(1)
      assertEquals(1, completions.size)
      assertSame(original, completions.head.tag)
      assertTrue(completions.head.result.isFailure)
      assertEquals("schema6 SQL writer requires its original observed Session",
        completions.head.result.failed.get.getMessage)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.reserveDbFirstCapture(tag(id)))
    }

  @Test def schema6RenewalCommandRequiresItsOriginalAcquiredSession(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-renew-without-session", id)
    managed(adapter) {
      val original = tag(id)
      assertEquals(IoSubmission.Accepted, adapter.submit(original, MySqlOwnedIoCommand.RenewDbFirstWriter))
      adapter.quiesce()
      waitOff(() => adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      val completions = adapter.drain(1)
      assertEquals(1, completions.size)
      assertSame(original, completions.head.tag)
      assertTrue(completions.head.result.isFailure)
      assertEquals("schema6 SQL writer renewal requires its original acquired Session",
        completions.head.result.failed.get.getMessage)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
    }

  @Test def refusalRejectsMissingOriginalBeforeQueueEpochAndReadMutation():Unit =
    val id=identity();val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"refusal-without-original",id)
    managed(adapter) {
      val j=tag(id);val before=adapter.repositorySubmissionEpoch
      assertThrows(classOf[IllegalArgumentException],()=>adapter.excludeRecallBeforeCapture(null,new java.io.IOException("original")))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(j,MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(null)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.claimRecallRefusalConfirmation(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitRefusalMainReadRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishRefusalMainReadRelease(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(j,MySqlOwnedIoCommand.ReleaseRefusalMainRead(null)))
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstRecallRefusal(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseRefusalMainRead(null)))
      assertEquals(before,adapter.repositorySubmissionEpoch);assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }

  @Test def journalRejectsMissingOriginalBeforeQueueEpochOrMainReadMutation():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"journal-without-original",id)
    managed(adapter) {
      val original=tag(id);val before=adapter.repositorySubmissionEpoch
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(original,MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(null)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.claimDbFirstRecallJournalConfirmation(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitJournalMainReadRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishJournalMainReadRelease(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(original,MySqlOwnedIoCommand.ReleaseJournalMainRead(null)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.prepareJournalCaptureTransfer(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitJournalCaptureTransfer(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishJournalCaptureTransfer(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(original,MySqlOwnedIoCommand.RetireJournalForCapture(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.RetireJournalForCapture(null)))
      assertEquals(before,adapter.repositorySubmissionEpoch);assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstRecallJournal(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseJournalMainRead(null)))
    }

  @Test def dbFirstPreparationRejectsMissingReservationBeforeQueueOrEpochMutation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-preparation-without-reservation", id)
    managed(adapter) {
      val original = tag(id)
      val before = adapter.repositorySubmissionEpoch
      val cause = assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(original, MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(null)))
      assertEquals("requirement failed: DB-first preparation submission requires its exact original Session reservation and tag", cause.getMessage)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.claimDbFirstInitialPreparationConfirmation(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitPreparationMainReadRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.recordDbFirstPreparationFinal(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitDbFirstPreparationFinalRelease(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishDbFirstPreparationFinalRelease(null,null))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseDbFirstPreparationFinal(null)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishPreparationMainReadRelease(null,null,null))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleasePreparationMainRead(null)))
      assertEquals(before, adapter.repositorySubmissionEpoch)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstInitialPreparation(null)))
    }

  @Test def dbFirstRecallRejectsMissingCaptureBeforeQueueOrEpochMutation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-recall-without-capture", id)
    managed(adapter) {
      val original = tag(id)
      val before = adapter.repositorySubmissionEpoch
      val cause = assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(original, MySqlOwnedIoCommand.ExecuteDbFirstRecall(null)))
      assertEquals("requirement failed: DB-first recall submission requires its exact original Session capture and tag", cause.getMessage)
      assertEquals(before, adapter.repositorySubmissionEpoch)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstRecall(null)))
    }

  @Test def dbFirstCheckpointRejectsMissingCaptureBeforeQueueOrEpochMutation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-checkpoint-without-capture", id)
    managed(adapter) {
      val original = tag(id)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.canReserveDbFirstCapture)
      val before = adapter.repositorySubmissionEpoch
      val cause = assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(original, MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(null)))
      assertEquals("requirement failed: DB-first checkpoint submission requires its exact original Session capture and tag", cause.getMessage)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.claimDbFirstCheckpointConfirmation(null,null))
      assertEquals(before, adapter.repositorySubmissionEpoch)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstCheckpoint(null)))
    }

  @Test def dbFirstInitialRejectsMissingCaptureBeforeQueueOrEpochMutation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-recall-without-capture", id)
    managed(adapter) {
      val original = tag(id)
      val before = adapter.repositorySubmissionEpoch
      val cause = assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(original, MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(null,256)))
      assertEquals("requirement failed: DB-first initial activation submission requires its exact original Session capture and tag", cause.getMessage)
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(original,MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(null,0)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(original,MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(null,257)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.claimDbFirstInitialActivationConfirmation(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.initialPublicationData(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.recordDbFirstInitialPublication(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitDbFirstInitialPublicationRelease(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishDbFirstInitialPublicationRelease(null,null))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseDbFirstInitialPublication(null)))
      assertEquals(before, adapter.repositorySubmissionEpoch)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ExecuteDbFirstInitialActivation(null,256)))
    }

  @Test def dbFirstCleanupCannotUseMissingConfirmationOrReceipt(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "schema6-cleanup-without-authority", id)
    managed(adapter) {
      val before = adapter.repositorySubmissionEpoch
      assertThrows(classOf[IllegalArgumentException], () => adapter.claimDbFirstRecallConfirmation(null, null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.recordDbFirstMainDrop(null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.submitDbFirstRecallRelease(null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.finishDbFirstRecallRelease(null, null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.submitRetainedDbFirstRecallRelease(null))
      assertThrows(classOf[IllegalArgumentException], () => adapter.finishRetainedDbFirstRecallRelease(null, null))
      val retainedControl=MySqlOwnedIoCommand.ReleaseRetainedDbFirstRecall(null)
      assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedIoCommand.requireGenericRoute(retainedControl))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlBootReadEpoch.mayChangeRepository(retainedControl))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(tag(id),retainedControl))
      assertEquals(before, adapter.repositorySubmissionEpoch)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseDbFirstRecall(null)))
    }

  @Test def mainWaitWrongActorAndForeignTagFailBeforeAnyDatabaseCommand(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "actual-owner-guard-adapter", id)
    val failure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(tag(id), MySqlOwnedIoCommand.Renew))
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "wrong-adapter-owner")
    managed(adapter) {
      var primary: Throwable = null
      try
        wrong.start()
        join(wrong)
        if failure.get() != null then throw failure.get()
        assertThrows(classOf[IllegalArgumentException], () => adapter.awaitExitOffMain(0, TimeUnit.MILLISECONDS))
        assertThrows(classOf[IllegalArgumentException], () =>
          adapter.submit(IoOperationTag(UUID.randomUUID(), 1, 1, UUID.randomUUID()), MySqlOwnedIoCommand.Renew))
        assertEquals(0, adapter.checkpoint.io.reserved)
      catch case cause: Throwable => primary = cause
      finally
        try join(wrong)
        catch
          case cleanup: Throwable =>
            if primary == null then primary = cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      if primary != null then throw primary
    }


  @Test def deploymentOriginalGuardRejectsRunAndMixedTransactionBeforePlanUse(): Unit =
    val find=MySqlRepositoryOperation.FindOriginalDeploymentOperation(UUID.randomUUID(),UUID.randomUUID())
    val commands=Vector(MySqlOwnedIoCommand.Run(find,null),MySqlOwnedIoCommand.Transaction(Vector(find),null),
      MySqlOwnedIoCommand.Transaction(Vector(MySqlRepositoryOperation.Delete(UUID.randomUUID()),find),null))
    commands.foreach { command =>
      val failure=assertThrows(classOf[UnsupportedOperationException],()=>MySqlOwnedIoCommand.requireGenericRoute(command))
      assertEquals("deployment original read requires the dedicated planned owned route",failure.getMessage)
    }
    MySqlOwnedIoCommand.requireGenericRoute(MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.FindAll,null))
    MySqlOwnedIoCommand.requireGenericRoute(MySqlOwnedIoCommand.Transaction(Vector(MySqlRepositoryOperation.FindAll),null))

  @Test def deploymentOriginalEpochClassificationCannotTreatUnopenedReadAsMutation(): Unit =
    val find=MySqlRepositoryOperation.FindOriginalDeploymentOperation(UUID.randomUUID(),UUID.randomUUID())
    assertThrows(classOf[UnsupportedOperationException],()=>MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Run(find,null)))
    assertThrows(classOf[UnsupportedOperationException],()=>MySqlBootReadEpoch.mayChangeRepository(
      MySqlOwnedIoCommand.Transaction(Vector(MySqlRepositoryOperation.Delete(UUID.randomUUID()),find),null)))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.FindAll,null)))
    assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.Delete(UUID.randomUUID()),null)))

  @Test def dedicatedOriginalReadRequiresExactSinglePlanAndPreservesEpoch():Unit =
    val original: MySqlRepositoryOperation.FindOriginalDeploymentOperation =
      MySqlRepositoryOperation.FindOriginalDeploymentOperation(UUID.randomUUID(),UUID.randomUUID())
    val actualTag=tag(identity())
    val actualPlan=plan(actualTag,original)
    val command=MySqlOwnedIoCommand.ReadOriginalDeployment(original,actualPlan)
    assertSame(actualPlan,command.requireRepositoryPlan(actualTag,limits))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(command))
    MySqlOwnedIoCommand.requireGenericRoute(command)
    assertThrows(classOf[IllegalArgumentException],()=>command.requireRepositoryPlan(
      actualTag.copy(operationId=UUID.randomUUID()),limits))
    val foreign: MySqlRepositoryOperation.FindOriginalDeploymentOperation =
      MySqlRepositoryOperation.FindOriginalDeploymentOperation(original.actorUuid,UUID.randomUUID())
    assertThrows(classOf[IllegalArgumentException],()=>
      MySqlOwnedIoCommand.ReadOriginalDeployment(foreign,actualPlan).requireRepositoryPlan(actualTag,limits))

  @Test def checkpointReadAndFinalRetirementRequireOriginalConfirmationBeforeQueue():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"checkpoint-retirement-no-authority",id)
    managed(adapter) {
      val before=adapter.repositorySubmissionEpoch
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitCheckpointMainReadRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishCheckpointMainReadRelease(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.recordDbFirstCheckpointFinal(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitDbFirstCheckpointFinalRelease(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishDbFirstCheckpointFinalRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(tag(id),MySqlOwnedIoCommand.ReleaseCheckpointMainRead(null)))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(tag(id),MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseCheckpointMainRead(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseDbFirstCheckpointFinal(null)))
      assertEquals(before,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }

  @Test def refusalFinalRequiresOriginalIssuerBeforeQueueAndDoesNotMutateEpoch():Unit =
    val id=identity()
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"refusal-final-no-authority",id)
    managed(adapter) {
      val before=adapter.repositorySubmissionEpoch
      assertThrows(classOf[IllegalArgumentException],()=>adapter.recordRefusalFinal(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submitRefusalFinalRelease(null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.finishRefusalFinalRelease(null,null))
      assertThrows(classOf[IllegalArgumentException],()=>adapter.submit(tag(id),MySqlOwnedIoCommand.ReleaseRefusalFinal(null)))
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.ReleaseRefusalFinal(null)))
      assertEquals(before,adapter.repositorySubmissionEpoch)
      assertEquals(0,adapter.checkpoint.io.reserved)
      assertTrue(adapter.drain(1).isEmpty)
    }
