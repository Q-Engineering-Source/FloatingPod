package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, UnboundCommitOutcome, UnboundPreparationReceipt}
import com.wi.floatingpod.server.io.{IoCheckpoint, IoCompletion, IoOperationTag, IoSubmission, IoWorkerExitHookObservation, OwnedSerialIo}
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Scripted JDBC transport only: actual owned session, retained plans, SQL DAG and private readback issuer. */
final class MySqlControlledDebugRunnerFixture(seed: Long, pose: FloatingPodPose, activeLimit: Int = 256):
  private val main = Thread.currentThread()
  private val root = Files.createTempDirectory("fp-controlled-jdbc-").toAbsolutePath
  MySqlOrdinaryCertifiedHandoffFixture.prepareOrdinaryProcessFixture(root)
  private val boot = UUID.randomUUID()
  private val identity = MySqlOwnedIoIdentity(boot, 1L, 1L)
  private var session: MySqlOwnedDatabaseSession = null
  private var issuer: MySqlOwnedRetainedPlanIssuer = null
  private var serial = 0L
  private val plans = scala.collection.mutable.ArrayBuffer.empty[MySqlOwnedRepositoryOperationPlan]
  private val closeControl = new MySqlOrdinaryCertifiedHandoffFixture.ControlledClose
  private var dropAcknowledged = false
  private var observedClose = Option.empty[MySqlOwnedSessionCloseObservation]
  private val io: OwnedSerialIo[() => Any, Any] = new OwnedSerialIo[() => Any, Any](1, main, "controlled-jdbc-runner",
    Some(() => if session != null then
      try { session.closeForWorkerExit(io.checkpoint.fatalCause); () }
      finally observedClose = session.closeObservation))(_())

  private def nextTag(): IoOperationTag =
    serial += 1
    IoOperationTag(boot, 1L, 1L, new UUID(seed, serial))

  private def execute[A](tag: IoOperationTag, plan: Option[MySqlOwnedRepositoryOperationPlan] = None)(body: => A): A =
    val submission = io.submit(tag, () => body)
    plan.foreach(issuer.submitted(_, submission))
    require(submission == IoSubmission.Accepted, "actual runner command was not accepted")
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
    while io.checkpoint.completions == 0 && System.nanoTime() < deadline do Thread.sleep(1)
    val completion = io.drain(1).headOption.getOrElse(throw new IllegalStateException("controlled runner completion timeout"))
    require(completion.tag == tag, "actual runner completion tag mismatch")
    completion.result.get.asInstanceOf[A]

  private def initializeGraphs(): (MySqlControlledDebugPrepareRequest, MySqlControlledDebugPreparedGraph, IoOperationTag, MySqlControlledDebugCommittedGraph) =
    execute(nextTag()) {
      val (inputs, source) = MySqlOrdinaryCertifiedHandoffFixture.controlledRunnerSource(root, boot, closeControl)
      session = MySqlOwnedDatabaseSession.openCertifiedWithVerifiedConnectionSourceForAssembly(inputs, 1L,
        Duration.ofMinutes(5), source)
      issuer = session.createRetainedPlanIssuer(identity, main, 4)
    }
    val binding = PhysicalRecoveryBinding(UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf"),
      PhysicalRecoveryCutoverState.Certified, 2)
    val request = MySqlControlledDebugPrepareRequest(binding, new UUID(seed, 101), new UUID(seed, 102),
      new UUID(seed, 103), pose, new UUID(seed, 104))
    val prepareTag = nextTag()
    val preparePlan = issuer.reserveSingle(prepareTag, new UUID(seed, 105), new UUID(seed, 106), request.repositoryOperation)
    plans += preparePlan
    val prepared: MySqlControlledDebugPreparedGraph = execute(prepareTag, Some(preparePlan)) {
      session.useRepository(prepareTag, preparePlan)(_.controlledDebug(preparePlan, request, None, activeLimit)).get match
        case MySqlControlledDebugExecution.Prepared(graph) => graph
        case _ => throw new IllegalStateException("actual preparation returned foreign execution")
    }
    val tag = nextTag()
    val commitPlan = issuer.reserveSingle(tag, preparePlan.workflowUuid, new UUID(seed, 108), prepared.commitOperation(pose, activeLimit))
    plans += commitPlan
    val graph: MySqlControlledDebugCommittedGraph = execute(tag, Some(commitPlan)) {
      session.useRepository(tag, commitPlan)(_.controlledDebug(commitPlan, request, Some(prepared), activeLimit)).get match
        case MySqlControlledDebugExecution.Committed(value) => value
        case _ => throw new IllegalStateException("actual activation returned foreign execution")
    }
    (request, prepared, tag, graph)

  private var assembled = try initializeGraphs()
  catch
    case primary: Throwable =>
      try stopWorker()
      catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
      throw primary
  def request: MySqlControlledDebugPrepareRequest = assembled._1
  def prepared: MySqlControlledDebugPreparedGraph = assembled._2
  def tag: IoOperationTag = assembled._3
  def graph: MySqlControlledDebugCommittedGraph = assembled._4
  def receipt: UnboundPreparationReceipt = prepared.receipt
  def state: FloatingPodState = graph.state
  def outcome: UnboundCommitOutcome = UnboundCommitOutcome.Committed(state)
  def completion: IoCompletion[MySqlOwnedIoReply] = IoCompletion(tag, scala.util.Success(MySqlOwnedIoReply.ControlledDebugCommitted(graph)))

  /** Caller has finished all Main assertions and must remove every graph/plan reference before release. */
  def closeAfterMainDrop(): Unit =
    dropMainReferencesAndAck()
    stopWorker()

  def dropMainReferencesAndAck(): Unit =
    require(!dropAcknowledged, "actual Main graph/plan drop ACK cannot replay")
    assembled = null
    val releasedPlans = plans.toVector
    plans.clear()
    releasedPlans.foreach { plan =>
      val receipt = issuer.recordMainDrop(plan.retainedHandle.get)
      execute(nextTag()) { session.releaseRetainedPlan(plan.tag, receipt) }
    }
    dropAcknowledged = true

  def armCloseFaults(close: Option[Throwable], value: Option[Boolean], getter: Option[Throwable]): Unit =
    require(dropAcknowledged, "close faults require every original graph/plan drop ACK")
    closeControl.arm(close, value, getter)

  def closeCalls: Int = closeControl.closeCalls
  def getterCallsAfterClose: Int = closeControl.getterCallsAfterClose

  def failActualSessionAction(cause: Throwable): Unit =
    require(dropAcknowledged, "failure probe requires actual original drop ACK")
    val result = execute(nextTag()) { session.useRepository(_ => scala.util.Failure(cause)) }
    require(result.failed.get eq cause, "session action did not retain original failure")

  def submitFatal(cause: LinkageError): IoSubmission =
    require(dropAcknowledged, "fatal probe requires actual original drop ACK")
    io.submit(nextTag(), () => throw cause)

  def shutdownObservation(): (IoCheckpoint, MySqlOwnedSessionCloseObservation) =
    require(dropAcknowledged, "shutdown probe requires original drop ACK")
    awaitWorkerExit()
    (io.checkpoint, observedClose.getOrElse(throw new IllegalStateException("worker omitted actual close observation")))

  private def stopWorker(): Unit =
    awaitWorkerExit()
    val checkpoint = io.checkpoint
    checkpoint.exitFailure.foreach(throw _)
    checkpoint.workerExitHook match
      case Some(IoWorkerExitHookObservation.HookFailed(cause)) => throw cause
      case Some(IoWorkerExitHookObservation.HookReturned) => ()
      case other => throw new IllegalStateException("controlled runner exit hook was not observed: " + other)

  private def awaitWorkerExit(): Unit =
    io.quiesce()
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val observer = new Thread(() =>
      try require(io.awaitExitOffMain(5, TimeUnit.SECONDS), "controlled runner did not exit")
      catch case cause: Throwable => failure.set(cause)
      (), "controlled-jdbc-exit")
    observer.start()
    observer.join(6000)
    require(!observer.isAlive, "controlled runner exit observer leaked")
    if failure.get() != null then throw failure.get()
