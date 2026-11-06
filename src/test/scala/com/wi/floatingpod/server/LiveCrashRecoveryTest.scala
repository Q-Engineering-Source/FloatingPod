package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.network.{ManagedServerRuntime, RuntimeEndpointRegistry, RuntimePersistence, UnavailableServerPacketEndpoint}
import com.wi.floatingpod.storage.{PhysicalRecoveryChunk, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import net.minecraftforge.fml.common.event.{FMLServerStartingEvent, FMLServerStoppingEvent}
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.{Appender, LogEvent, LoggerContext}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable

/** DeferredRecovery 對 inactive locator 的第一條真實 SQLite tracer。 */
class LiveCrashRecoveryTest:
  @Test
  def pendingOrderedDropRequiresFreshBootWithoutCas(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.selectRecoverableTickets(fixture.dimensionId, List(fixture.gateway.recoveredTicket(fixture.podUuid)), 1)
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)

      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(
        RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(fixture.podUuid)),
        recovery.advance(ServerRecoveryTickPhase.End)
      )
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertEquals(PhysicalRecoveryLocatorReason.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.reasonCode)
    finally fixture.close()

  @Test
  def orderedDropThatAppearsAfterStartRequiresFreshBootWithoutCas(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      fixture.tickets.selectRecoverableTickets(fixture.dimensionId, List(fixture.gateway.recoveredTicket(fixture.podUuid)), 1)

      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(fixture.podUuid)), recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def pendingEntityIsRemovedBeforeTicketReleaseAndRequiresFreshBoot(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val entity = fixture.world.seed(fixture.podUuid)
      fixture.inventoryObservations = List(fixture.observation(fixture.podUuid, entity.entityId))
      assertTrue(fixture.tickets.requestTicket(fixture.podUuid, fixture.world, 10, -3).isSuccess)
      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertFalse(fixture.world.contains(fixture.podUuid))
      assertFalse(fixture.tickets.managedTicketRecords().contains(fixture.podUuid))
      assertEquals(List("ticket-request", s"entity-remove:${fixture.podUuid}", "ticket-release"), fixture.gateway.events.toList)
      fixture.inventoryObservations = Nil
      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(fixture.podUuid)), recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def delayedEntityRemovalStillReleasesTicketAtStartThenRequiresFreshBootAfterTickCompletion(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      fixture.world.delayedRemoval = true
      val entity = fixture.world.seed(fixture.podUuid)
      fixture.inventoryObservations = List(fixture.observation(fixture.podUuid, entity.entityId))
      assertTrue(fixture.tickets.requestTicket(fixture.podUuid, fixture.world, 10, -3).isSuccess)
      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertTrue(fixture.world.contains(fixture.podUuid))
      assertFalse(fixture.tickets.managedTicketRecords().contains(fixture.podUuid))
      assertEquals(List("ticket-request", s"entity-remove:${fixture.podUuid}", "ticket-release"), fixture.gateway.events.toList)
      fixture.world.completeDelayedRemoval()
      fixture.inventoryObservations = Nil
      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(fixture.podUuid)), recovery.advance(ServerRecoveryTickPhase.End))
    finally fixture.close()

  @Test
  def delayedEntityRemovalThatHasNotCompletedByEndBlocks(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      fixture.world.delayedRemoval = true
      val entity = fixture.world.seed(fixture.podUuid)
      fixture.inventoryObservations = List(fixture.observation(fixture.podUuid, entity.entityId))
      assertTrue(fixture.tickets.requestTicket(fixture.podUuid, fixture.world, 10, -3).isSuccess)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))

      assertBlock(recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def pendingArtifactsThatAppearAfterStartBlockWithoutCas(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      fixture.world.seed(fixture.podUuid)
      fixture.inventoryObservations = List(fixture.observation(fixture.podUuid, 32))
      assertTrue(fixture.tickets.requestTicket(fixture.podUuid, fixture.world, 10, -3).isSuccess)

      assertBlock(recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertTrue(fixture.world.contains(fixture.podUuid))
      assertTrue(fixture.tickets.managedTicketRecords().contains(fixture.podUuid))
    finally fixture.close()

  @Test
  def managerMappingThatAppearsBeforeEndBlocksWithoutCas(): Unit =
    val fixture = Fixture.pending()
    try
      val activeUuid = UUID.fromString("00000000-0000-0000-0000-000000002021")
      fixture.addActive(activeUuid)
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      val active = fixture.repository.findByUuid(activeUuid).get.get
      assertTrue(fixture.tickets.requestTicket(activeUuid, fixture.world, 14, -3).isSuccess)
      assertTrue(fixture.manager.recoverCommittedActive(active, fixture.world).isSuccess)

      assertBlock(recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertEquals(List(activeUuid), fixture.manager.getAllActivePods())
    finally fixture.close()

  @Test
  def cleanPendingCasVerifiesLocatorThenPermits(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)), recovery.advance(ServerRecoveryTickPhase.End))
      val locator = fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get
      assertEquals(PhysicalRecoveryLocatorState.RowInactiveVerified, locator.state)
      assertEquals(PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified, locator.reasonCode)
    finally fixture.close()

  @Test
  def missingTicketPassOrIncompleteInventoryBlocksWithoutCas(): Unit =
    val missingPass = Fixture.pending()
    try
      assertBlock(missingPass.recovery().begin())
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, missingPass.repository.findPhysicalRecoveryLocator(missingPass.podUuid).get.get.state)
    finally missingPass.close()

    val incomplete = Fixture.pending()
    try
      incomplete.tickets.markWorldTicketPassCompleted(incomplete.dimensionId)
      incomplete.generatedChunks = incomplete.chunks.tail
      assertBlock(incomplete.recovery().begin())
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, incomplete.repository.findPhysicalRecoveryLocator(incomplete.podUuid).get.get.state)
    finally incomplete.close()

  @Test
  def acceptedRejectionDispositionRequiresFreshBootWithoutCas(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.recoverAcceptedTickets(List(fixture.gateway.recoveredTicket(fixture.podUuid)), fixture.world)
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(fixture.podUuid)), recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def unknownDuplicateAndOutsideLocatorObservationsBlockBeforeMutation(): Unit =
    val unknown = Fixture.pending()
    try
      unknown.tickets.markWorldTicketPassCompleted(unknown.dimensionId)
      unknown.inventoryObservations = List(unknown.observation(UUID.fromString("00000000-0000-0000-0000-000000002099"), 41))
      assertBlock(unknown.recovery().begin())
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, unknown.repository.findPhysicalRecoveryLocator(unknown.podUuid).get.get.state)
    finally unknown.close()

    val duplicate = Fixture.pending()
    try
      duplicate.tickets.markWorldTicketPassCompleted(duplicate.dimensionId)
      duplicate.inventoryObservations = List(duplicate.observation(duplicate.podUuid, 42), duplicate.observation(duplicate.podUuid, 43, chunkX = 11))
      assertBlock(duplicate.recovery().begin())
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, duplicate.repository.findPhysicalRecoveryLocator(duplicate.podUuid).get.get.state)
    finally duplicate.close()

    val outside = Fixture.pending()
    try
      outside.tickets.markWorldTicketPassCompleted(outside.dimensionId)
      outside.inventoryObservations = List(outside.observation(outside.podUuid, 44, chunkX = 999))
      assertBlock(outside.recovery().begin())
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, outside.repository.findPhysicalRecoveryLocator(outside.podUuid).get.get.state)
    finally outside.close()

  @Test
  def inventoryProbeFailureRetainsItsExactCauseAndUsesASafeReason(): Unit =
    val fixture = Fixture.pending()
    val failure = new IllegalStateException("private diagnostic payload")
    try
      fixture.inventoryFailure = failure

      val step = fixture.recovery().begin()

      step match
        case RecoveryStep.Terminal(StartupPublicationDecision.Failed(reason, cause)) =>
          assertEquals("world inventory failed", reason)
          assertSame(failure, cause)
          assertFalse(reason.contains("private diagnostic payload"))
        case other => throw new AssertionError(s"Expected a failed recovery decision, got $other")
    finally fixture.close()

  @Test
  def inventoryFailureRetainsExactCauseAndSuppressedCleanupAtFinalTermination(): Unit =
    val fixture = Fixture.pending()
    val cause = new IllegalStateException("synthetic internal failure marker")
    val unregisterFailure = new IllegalStateException("synthetic unregister failure")
    val lifecycleEvents = mutable.ListBuffer.empty[String]
    val logEvents = mutable.ArrayBuffer.empty[CapturedRecoveryLog]
    val appender = captureRecoveryLog(logEvents, () => lifecycleEvents += "diagnostic")
    val logger = LogManager.getContext(false).asInstanceOf[LoggerContext].getLogger(classOf[ManagedServerRuntime].getName)
    logger.addAppender(appender)
    appender.start()
    try
      fixture.inventoryFailure = cause
      val lifecycle = new RecoveryLifecycleProbe(fixture.repository, fixture.recovery(), lifecycleEvents, unregisterFailure)

      lifecycle.start()
      lifecycle.runtime.onServerStopping(null)

      val report = lifecycle.runtime.latestStoppingReport.get
      assertEquals(Some(RuntimeRecoveryTermination.Blocked("world inventory failed", Some(cause))), report.deferredTermination)
      assertSame(cause, report.deferredFailure.get)
      assertTrue(cause.getSuppressed.contains(unregisterFailure))
      assertEquals(Some(unregisterFailure), report.deferredUnregisterFailure)
      assertTrue(report.controlledStopSucceeded)
      assertEquals(RecoveryExit.Blocked, report.deferredFinishReport.get.exit)
      assertEquals(1, logEvents.size)
      assertEquals("FloatingPod startup recovery failed before endpoint publication: world inventory failed", logEvents.head.event.getMessage.getFormattedMessage)
      val loggedCause = Option(logEvents.head.event.getThrown).orElse(Option(logEvents.head.event.getThrownProxy).map(_.getThrowable))
      assertSame(cause, loggedCause.orNull)
      assertTrue(Option(logEvents.head.event.getThrown).exists(_.getStackTrace.nonEmpty) || Option(logEvents.head.event.getThrownProxy).exists(_.getStackTrace.nonEmpty))
      assertFalse(logEvents.head.event.getMessage.getFormattedMessage.contains(cause.getMessage))
      assertTrue(logEvents.head.rendered.contains("java.lang.IllegalStateException: synthetic internal failure marker"))
      assertTrue(logEvents.head.rendered.contains("Suppressed: java.lang.IllegalStateException: synthetic unregister failure"))
      assertTrue(lifecycleEvents.indexOf("diagnostic") < lifecycleEvents.indexOf("controlled-stop"))
      assertEquals(0, lifecycle.persistenceStartCalls)
      assertEquals(0, lifecycle.endpointInstallCalls)
    finally
      logger.removeAppender(appender)
      appender.stop()
      fixture.close()

  @Test
  def controlledStopFailureLogsCompleteSuppressedCauseAfterThePreStopRecoveryDiagnostic(): Unit =
    val fixture = Fixture.pending()
    val cause = new IllegalStateException("synthetic internal failure marker")
    val stopFailure = new IllegalStateException("synthetic controlled stop failure marker")
    val lifecycleEvents = mutable.ListBuffer.empty[String]
    val logEvents = mutable.ArrayBuffer.empty[CapturedRecoveryLog]
    val appender = captureRecoveryLog(logEvents, () => lifecycleEvents += "diagnostic")
    val logger = LogManager.getContext(false).asInstanceOf[LoggerContext].getLogger(classOf[ManagedServerRuntime].getName)
    logger.addAppender(appender)
    appender.start()
    try
      fixture.inventoryFailure = cause
      val lifecycle = new RecoveryLifecycleProbe(fixture.repository, fixture.recovery(), lifecycleEvents, controlledStopFailure = stopFailure)

      val thrown = assertThrows(classOf[IllegalStateException], () => lifecycle.start())
      lifecycle.runtime.onServerStopping(null)

      val report = lifecycle.runtime.latestStoppingReport.get
      assertSame(cause, thrown)
      assertEquals(1, cause.getSuppressed.length)
      assertSame(stopFailure, cause.getSuppressed.head)
      assertSame(cause, report.deferredFailure.get)
      assertEquals(Some(stopFailure), report.controlledStopFailure)
      assertFalse(report.controlledStopSucceeded)
      assertEquals(2, logEvents.size)
      assertEquals("FloatingPod startup recovery failed before endpoint publication: world inventory failed", logEvents.head.event.getMessage.getFormattedMessage)
      val preStopCause = Option(logEvents.head.event.getThrown).orElse(Option(logEvents.head.event.getThrownProxy).map(_.getThrowable))
      assertSame(cause, preStopCause.orNull)
      assertTrue(logEvents.head.rendered.contains("java.lang.IllegalStateException: synthetic internal failure marker"))
      assertFalse(logEvents.head.rendered.contains(stopFailure.getMessage))
      assertEquals("FloatingPod controlled stop request failed after startup recovery termination", logEvents.last.event.getMessage.getFormattedMessage)
      val finalCause = Option(logEvents.last.event.getThrown).orElse(Option(logEvents.last.event.getThrownProxy).map(_.getThrowable))
      assertSame(cause, finalCause.orNull)
      assertTrue(logEvents.last.rendered.contains("java.lang.IllegalStateException: synthetic internal failure marker"))
      assertTrue(logEvents.last.rendered.contains("Suppressed: java.lang.IllegalStateException: synthetic controlled stop failure marker"))
      val diagnosticIndexes = lifecycleEvents.zipWithIndex.collect { case ("diagnostic", index) => index }
      val stopIndex = lifecycleEvents.indexOf("controlled-stop")
      assertEquals(2, diagnosticIndexes.size)
      assertTrue(diagnosticIndexes.head < stopIndex)
      assertTrue(stopIndex < diagnosticIndexes.last)
      assertEquals(0, lifecycle.persistenceStartCalls)
      assertEquals(0, lifecycle.endpointInstallCalls)
    finally
      logger.removeAppender(appender)
      appender.stop()
      fixture.close()

  @Test
  def normalRecoveryRejectionIsLoggedWithoutUnexpectedCauseOrEndpointPublication(): Unit =
    val fixture = Fixture.pending()
    val unknownUuid = UUID.fromString("00000000-0000-0000-0000-000000002098")
    val lifecycleEvents = mutable.ListBuffer.empty[String]
    val logEvents = mutable.ArrayBuffer.empty[CapturedRecoveryLog]
    val appender = captureRecoveryLog(logEvents, () => lifecycleEvents += "diagnostic")
    val logger = LogManager.getContext(false).asInstanceOf[LoggerContext].getLogger(classOf[ManagedServerRuntime].getName)
    logger.addAppender(appender)
    appender.start()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      fixture.inventoryObservations = List(fixture.observation(unknownUuid, 98))
      val lifecycle = new RecoveryLifecycleProbe(fixture.repository, fixture.recovery(), lifecycleEvents)

      lifecycle.start()
      lifecycle.runtime.onServerStopping(null)

      val report = lifecycle.runtime.latestStoppingReport.get
      val termination = report.deferredTermination.get.asInstanceOf[RuntimeRecoveryTermination.Blocked]
      assertTrue(termination.reason.contains(s"world inventory observed unknown pod $unknownUuid"))
      assertTrue(termination.cause.isEmpty)
      assertTrue(report.deferredFailure.isEmpty)
      assertEquals(1, logEvents.size)
      assertEquals(s"FloatingPod startup recovery blocked before endpoint publication: ${termination.reason}", logEvents.head.event.getMessage.getFormattedMessage)
      assertTrue(logEvents.head.event.getThrown == null)
      assertTrue(lifecycleEvents.indexOf("diagnostic") < lifecycleEvents.indexOf("controlled-stop"))
      assertEquals(0, lifecycle.persistenceStartCalls)
      assertEquals(0, lifecycle.endpointInstallCalls)
    finally
      logger.removeAppender(appender)
      appender.stop()
      fixture.close()

  private def captureRecoveryLog(logEvents: mutable.ArrayBuffer[CapturedRecoveryLog], recordOrder: () => Unit): Appender =
    val layout = PatternLayout.createDefaultLayout()
    new AbstractAppender("live-crash-recovery-test", null, layout, false, Property.EMPTY_ARRAY):
      override def append(event: LogEvent): Unit =
        val snapshot = event.toImmutable
        logEvents += CapturedRecoveryLog(snapshot, layout.toSerializable(snapshot))
        recordOrder()

  private final case class CapturedRecoveryLog(event: LogEvent, rendered: String)

  @Test
  def verifiedLocatorEntityContradictionBlocksWithoutCleanup(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.repository.verifyInactiveAbsence(fixture.podUuid, fixture.inactive.stateVersion, fixture.attemptUuid).get
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      fixture.world.seed(fixture.podUuid)
      fixture.inventoryObservations = List(fixture.observation(fixture.podUuid, 51))

      assertBlock(fixture.recovery().begin())
      assertTrue(fixture.world.contains(fixture.podUuid))
      assertEquals(PhysicalRecoveryLocatorState.RowInactiveVerified, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def rowOrLocatorChangeBetweenBeginAndStartBlocks(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      val stored = fixture.repository.findByUuid(fixture.podUuid).get.get
      fixture.repository.update(stored.copy(lastUpdated = stored.lastUpdated + 1L)).get

      assertBlock(recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
    finally fixture.close()

  @Test
  def mixedActiveAndCleanPendingVerifiesBeforeActiveConvergence(): Unit =
    val fixture = Fixture.pending()
    try
      val activeUuid = UUID.fromString("00000000-0000-0000-0000-000000002011")
      fixture.addActive(activeUuid)
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()

      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryStep.AwaitingTick, recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(RecoveryStep.Terminal(StartupPublicationDecision.Permit(List(activeUuid))), recovery.advance(ServerRecoveryTickPhase.End))
      assertEquals(PhysicalRecoveryLocatorState.RowInactiveVerified, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertEquals(Set(activeUuid), fixture.tickets.managedTicketRecords().keySet.toSet)
      assertEquals(List(activeUuid), fixture.manager.getAllActivePods())
    finally fixture.close()

  @Test
  def finishPreventsAnyFurtherAdvanceWithoutMutation(): Unit =
    val fixture = Fixture.pending()
    try
      fixture.tickets.markWorldTicketPassCompleted(fixture.dimensionId)
      val recovery = fixture.recovery()
      assertEquals(RecoveryStep.AwaitingTick, recovery.begin())
      assertEquals(RecoveryFinishReport(RecoveryExit.ExternalStopWhileRecovering, Nil), recovery.finish(RecoveryExit.ExternalStopWhileRecovering))

      assertThrows(classOf[IllegalStateException], () => recovery.advance(ServerRecoveryTickPhase.Start))
      assertEquals(PhysicalRecoveryLocatorState.RowInactivePending, fixture.repository.findPhysicalRecoveryLocator(fixture.podUuid).get.get.state)
      assertTrue(fixture.gateway.events.isEmpty)
    finally fixture.close()

  @Test
  def ticketEvidenceRequiresExactBidirectionalMultisetPairing(): Unit =
    val fixture = Fixture.pending()
    try
      val drop = StartupTicketCleanupObservation.DroppedByOrderedCallback(fixture.dimensionId, Some(fixture.podUuid), "recovery_row_rejected")
      val issue = TicketRecoveryIssue.CandidateRejected("recovery_row_rejected", Some(fixture.podUuid), "inactive")
      val duplicateIssue = new LiveCrashRecoveryTicketEvidenceSource:
        override def snapshot(): LiveCrashRecoveryTicketEvidence = LiveCrashRecoveryTicketEvidence(TicketRecoveryReport(List(issue, issue)), List(drop, StartupTicketCleanupObservation.WorldTicketPassCompleted(fixture.dimensionId)))
      assertBlock(fixture.recovery(duplicateIssue).begin())

      val duplicateDrop = new LiveCrashRecoveryTicketEvidenceSource:
        override def snapshot(): LiveCrashRecoveryTicketEvidence = LiveCrashRecoveryTicketEvidence(TicketRecoveryReport(List(issue)), List(drop, drop, StartupTicketCleanupObservation.WorldTicketPassCompleted(fixture.dimensionId)))
      assertBlock(fixture.recovery(duplicateDrop).begin())

      val missingCounterpart = new LiveCrashRecoveryTicketEvidenceSource:
        override def snapshot(): LiveCrashRecoveryTicketEvidence = LiveCrashRecoveryTicketEvidence(TicketRecoveryReport(List(issue)), List(StartupTicketCleanupObservation.WorldTicketPassCompleted(fixture.dimensionId)))
      assertBlock(fixture.recovery(missingCounterpart).begin())
    finally fixture.close()

  @Test
  def productionBundleDefersServerSupplierResolutionUntilRecoveryProbes(): Unit =
    val fixture = Fixture.pending()
    try
      var supplierCalls = 0
      val bundle = LiveCrashRecovery.productionBundle(fixture.repository, fixture.tickets, fixture.manager, fixture.worlds, () => { supplierCalls += 1; null })

      assertTrue(bundle.recovery.isInstanceOf[LiveCrashRecovery])
      assertEquals(0, supplierCalls)
    finally fixture.close()

  private def assertBlock(step: RecoveryStep): Unit = step match
    case RecoveryStep.Terminal(StartupPublicationDecision.Block(reason)) => assertTrue(reason.nonEmpty)
    case other => throw new AssertionError(s"Expected recovery block, got $other")

private final class RecoveryLifecycleProbe(
    repository: SQLiteFloatingPodRepository,
    recovery: LiveCrashRecovery,
    events: mutable.ListBuffer[String],
    unregisterFailure: Throwable = null,
    controlledStopFailure: Throwable = null
):
  var persistenceStartCalls = 0
  var endpointInstallCalls = 0
  private var callback: Option[ServerRecoveryTickPhase => Unit] = None

  private val runtimePersistence = new RuntimePersistence:
    override def start(event: FMLServerStartingEvent): Unit = { persistenceStartCalls += 1; events += "persistence-start" }
    override def stop(event: FMLServerStoppingEvent): Unit = events += "persistence-stop"
    override def abortStarted(): Unit = events += "persistence-abort"

  private val endpointRegistry = new RuntimeEndpointRegistry:
    override def install(endpoint: com.wi.floatingpod.network.ServerPacketEndpoint): Unit =
      endpointInstallCalls += 1
      events += "endpoint-install"
    override def uninstall(): Unit = events += "endpoint-uninstall"

  private val recoveryEvents = new RuntimeRecoveryEvents:
    override def register(callback: ServerRecoveryTickPhase => Unit): Unit =
      if RecoveryLifecycleProbe.this.callback.nonEmpty then throw new AssertionError("deferred callback registered twice")
      RecoveryLifecycleProbe.this.callback = Some(callback)
      events += "recovery-register"
    override def unregister(): Unit =
      RecoveryLifecycleProbe.this.callback = None
      events += "recovery-unregister"
      if unregisterFailure != null then throw unregisterFailure

  private val controlledStop = new RuntimeControlledStop:
    override def requestStop(): Unit =
      events += "controlled-stop"
      if controlledStopFailure != null then throw controlledStopFailure

  val runtime = new ManagedServerRuntime(
    repository,
    runtimePersistence,
    UnavailableServerPacketEndpoint,
    endpointRegistry,
    deferredRecovery = Some(DeferredRecoveryBundle(recovery, recoveryEvents, controlledStop))
  )

  def start(): Unit =
    runtime.onServerAboutToStart(null)
    runtime.onServerStarting(null)

private object Fixture:
  val dimensionId = 7

  def pending(): Fixture =
    val database = Files.createTempDirectory("floatingpod-live-crash-recovery").resolve("pods.db")
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000002001")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000002002")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000002003")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "live-crash-recovery", 160D, 64D, -48D, 0F, 0F, false, 1L, None)
    val chunks = square(dimensionId, inactive.posX, inactive.posZ)
    assert(repository.insert(inactive).isSuccess)
    assert(repository.prepareCreateLocator(podUuid, 0L, attemptUuid, dimensionId, chunks).isSuccess)
    assert(repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, dimensionId, 256, attemptUuid).isSuccess)
    assert(repository.deactivateTrackedIfExpectedActive(podUuid, Some(ownerUuid), 1L, attemptUuid).isSuccess)
    new Fixture(database, repository, podUuid, attemptUuid, inactive.copy(stateVersion = 2L, dimensionId = Some(dimensionId)), chunks)

  def square(dimensionId: Int, x: Double, z: Double): Set[PhysicalRecoveryChunk] =
    val centerX = math.floor(x / 16D).toInt
    val centerZ = math.floor(z / 16D).toInt
    (for chunkX <- centerX - 2 to centerX + 2; chunkZ <- centerZ - 2 to centerZ + 2 yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

private final class Fixture(
    database: java.nio.file.Path,
    val repository: SQLiteFloatingPodRepository,
    val podUuid: UUID,
    val attemptUuid: UUID,
    val inactive: FloatingPodState,
    val chunks: Set[PhysicalRecoveryChunk]
):
  val dimensionId: Int = Fixture.dimensionId
  val gateway = new FixtureGateway
  val tickets = new ChunkLoadingManager(repository, gateway)
  val manager = new ActiveFloatingPodManager(repository, tickets)
  val world = new FixtureWorld(dimensionId, gateway.events)
  var inventoryObservations: List[ExistingWorldInventoryObservation] = Nil
  var inventoryFailure: Throwable = null
  var generatedChunks: Set[PhysicalRecoveryChunk] = chunks
  val worlds = new ServerWorldResolver:
    override def resolve(requestedDimensionId: Int): Option[PodWorld] = if requestedDimensionId == dimensionId then Some(world) else None
  val inventory = new ExistingWorldInventory(new ExistingWorldInventoryProbe:
    override def loadIfAlreadyGenerated(chunk: PhysicalRecoveryChunk): Boolean =
      if inventoryFailure != null then throw inventoryFailure
      generatedChunks.contains(chunk)
    override def loadedChunks(): List[ExistingWorldInventoryLoadedChunk] =
      inventoryObservations.groupBy(observation => (observation.dimensionId, observation.chunkX, observation.chunkZ)).toList.map { case ((dimension, chunkX, chunkZ), observations) =>
        ExistingWorldInventoryLoadedChunk(dimension, chunkX, chunkZ, observations)
      }
  )

  def recovery(ticketEvidence: LiveCrashRecoveryTicketEvidenceSource = null): LiveCrashRecovery = new LiveCrashRecovery(repository, tickets, manager, worlds, inventory, ticketEvidence)

  def observation(uuid: UUID, entityId: Int, chunkX: Int = 10, chunkZ: Int = -3): ExistingWorldInventoryObservation =
    ExistingWorldInventoryObservation(uuid, dimensionId, chunkX, chunkZ, entityId)

  def addActive(activeUuid: UUID): Unit =
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-000000002012")
    val attempt = UUID.fromString("00000000-0000-0000-0000-000000002013")
    val inactiveActive = FloatingPodState(activeUuid, Some(ownerUuid), 0L, "live-crash-recovery-active", 224D, 64D, -48D, 0F, 0F, false, 2L, None)
    val activeChunks = Fixture.square(dimensionId, inactiveActive.posX, inactiveActive.posZ)
    assert(repository.insert(inactiveActive).isSuccess)
    assert(repository.prepareCreateLocator(activeUuid, 0L, attempt, dimensionId, activeChunks).isSuccess)
    assert(repository.activatePreparedIfAdmissible(inactiveActive, ownerUuid, 0L, dimensionId, 256, attempt).isSuccess)
    generatedChunks = generatedChunks ++ activeChunks

  def close(): Unit =
    repository.close()
    Files.deleteIfExists(database.resolveSibling("pods.db-shm"))
    Files.deleteIfExists(database.resolveSibling("pods.db-wal"))
    Files.deleteIfExists(database)
    Files.deleteIfExists(database.getParent)

private final class FixtureEntity(override val podUuid: UUID, override val entityId: Int) extends ActivePodEntity

private final class FixtureWorld(override val dimensionId: Int, events: mutable.ListBuffer[String]) extends PodWorld:
  private val entities = mutable.HashMap.empty[UUID, FixtureEntity]
  var delayedRemoval = false
  override def diagnosticWorldName: String = "live-crash-recovery"
  override def sameWorld(other: PodWorld): Boolean = this.eq(other)
  override def isExactBodyResidentAndUnreplaced(body: ActivePodEntity): Boolean = body match
    case entity: FixtureEntity => entities.get(entity.podUuid).exists(_ eq entity)
    case _ => false
  override def findPod(podUuid: UUID): Option[ActivePodEntity] = entities.get(podUuid)
  override def spawnPod(state: FloatingPodState): ActivePodEntity =
    val entity = new FixtureEntity(state.uuid, entities.size + 1)
    entities.put(state.uuid, entity)
    entity
  override def removePod(entity: ActivePodEntity): Unit = { events += s"entity-remove:${entity.podUuid}"; if !delayedRemoval then entities.remove(entity.podUuid) }
  def seed(podUuid: UUID): ActivePodEntity =
    val entity = new FixtureEntity(podUuid, entities.size + 1)
    entities.put(podUuid, entity)
    entity
  def contains(podUuid: UUID): Boolean = entities.contains(podUuid)
  def completeDelayedRemoval(): Unit = entities.clear()

private final class FixtureGateway extends ChunkTicketGateway:
  private final class FixtureTicket extends ChunkTicket:
    var uuid: Option[UUID] = None
  override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit = ()
  val events = mutable.ListBuffer.empty[String]
  override def request(world: PodWorld): Option[ChunkTicket] = { events += "ticket-request"; Some(new FixtureTicket) }
  override def release(ticket: ChunkTicket): Unit = events += "ticket-release"
  override def maxChunkDepth(ticket: ChunkTicket): Int = 25
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ticket.asInstanceOf[FixtureTicket].uuid = Some(podUuid)
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] = ticket.asInstanceOf[FixtureTicket].uuid
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def wrapRecovered(ticket: Ticket): ChunkTicket = new FixtureTicket
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = None
  def recoveredTicket(podUuid: UUID): ChunkTicket =
    val ticket = new FixtureTicket
    ticket.uuid = Some(podUuid)
    ticket
