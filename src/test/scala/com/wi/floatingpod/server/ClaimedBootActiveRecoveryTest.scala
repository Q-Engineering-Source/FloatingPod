package com.wi.floatingpod.server

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.collection.mutable.ListBuffer
import com.wi.floatingpod.storage.{PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, StoredPodRow}
import java.util.UUID

/** 直接验证生产 owner 的 deferred 生命周期；不签发 claimed lease 或 Forge 成功证据。 */
class ClaimedBootActiveRecoveryTest:
  private final class Recovery extends DeferredRecovery:
    val phases = ListBuffer.empty[ServerRecoveryTickPhase]
    val exits = ListBuffer.empty[RecoveryExit]
    var begins = 0
    var outcome: StartupPublicationDecision = StartupPublicationDecision.Permit(Nil)
    var failure: Option[Throwable] = None
    var beginFailure: Option[Throwable] = None
    var cleanupFailures = List.empty[Throwable]
    var finishFailure = Option.empty[Throwable]
    var reportedExit = Option.empty[RecoveryExit]
    override def begin(): RecoveryStep =
      begins += 1
      beginFailure.foreach(throw _)
      RecoveryStep.AwaitingTick
    override def advance(phase: ServerRecoveryTickPhase): RecoveryStep =
      phases += phase
      failure.foreach(throw _)
      if phase == ServerRecoveryTickPhase.End then RecoveryStep.Terminal(outcome) else RecoveryStep.AwaitingTick
    override def finish(exit: RecoveryExit): RecoveryFinishReport =
      exits += exit
      finishFailure.foreach(throw _)
      RecoveryFinishReport(reportedExit.getOrElse(exit), cleanupFailures)

  @Test def physicalPermitIsRetainedWithoutPublishedFinish(): Unit =
    val recovery = new Recovery
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    progress.advance(ServerRecoveryTickPhase.Start)
    progress.advance(ServerRecoveryTickPhase.End)
    assertEquals(Some(recovery.outcome), progress.result)
    progress.advance(ServerRecoveryTickPhase.Start)
    assertEquals(List(ServerRecoveryTickPhase.Start, ServerRecoveryTickPhase.End), recovery.phases.toList)
    progress.stop()
    progress.stop()
    assertEquals(List(RecoveryExit.ExternalStopWhileRecovering), recovery.exits.toList)

  @Test def blockKeepsOriginalPayloadAndDoesNotResume(): Unit =
    val recovery = new Recovery
    recovery.outcome = StartupPublicationDecision.Block("pending source responsibility")
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    progress.advance(ServerRecoveryTickPhase.Start)
    progress.advance(ServerRecoveryTickPhase.End)
    progress.advance(ServerRecoveryTickPhase.End)
    assertSame(recovery.outcome, progress.result.get)
    assertEquals(2, recovery.phases.size)

  @Test def originalUnexpectedFailureAndStackPropagate(): Unit =
    val recovery = new Recovery
    val cause = new IllegalStateException("inventory probe failed")
    val originalStack = cause.getStackTrace
    recovery.failure = Some(cause)
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => progress.advance(ServerRecoveryTickPhase.Start)))
    assertTrue(originalStack.sameElements(cause.getStackTrace))
    assertEquals(None, progress.result)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => progress.advance(ServerRecoveryTickPhase.End)))
    assertEquals(1, recovery.phases.size)

  @Test def beginAndStopRemainOneShot(): Unit =
    val recovery = new Recovery
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    assertThrows(classOf[IllegalArgumentException], () => progress.begin())
    progress.stop()
    assertThrows(classOf[IllegalArgumentException], () => progress.advance(ServerRecoveryTickPhase.Start))
    assertEquals(1, recovery.begins)
    assertEquals(1, recovery.exits.size)

  @Test def beginFailureCannotResumeAtALaterForgeStart(): Unit =
    val recovery = new Recovery
    val cause = new IllegalStateException("begin capture failed")
    val stack = cause.getStackTrace
    recovery.beginFailure = Some(cause)
    val progress = new ClaimedRecoveryProgress(recovery)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => progress.begin()))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => progress.advance(ServerRecoveryTickPhase.Start)))
    assertTrue(recovery.phases.isEmpty)
    assertTrue(stack.sameElements(cause.getStackTrace))
    progress.stop()
    assertEquals(List(RecoveryExit.ExternalStopWhileRecovering), recovery.exits.toList)

  @Test def stopBeforeBeginDoesNotInventARecoveryFinish(): Unit =
    val recovery = new Recovery
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.stop()
    assertTrue(recovery.exits.isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => progress.begin())

  @Test def finishReportCannotHideCleanupFailures(): Unit =
    val recovery = new Recovery
    val primary = new IllegalStateException("finish cleanup failed")
    val secondary = new IllegalStateException("secondary cleanup failed")
    recovery.cleanupFailures = List(primary, secondary)
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    assertSame(primary, assertThrows(classOf[IllegalStateException], () => progress.stop()))
    assertSame(secondary, primary.getSuppressed.head)
    progress.stop()
    assertEquals(1, recovery.exits.size)

  @Test def missingLegacyDimensionBlocksBeforePublicStateConversion(): Unit =
    val row = StoredPodRow(UUID.randomUUID(), None, 1L, "legacy", 0D, 64D, 0D, 0F, 0F, true, 0L, None)
    assertTrue(LiveCrashRecovery.claimedDataBlock(Vector(row), Vector.empty).exists(_.contains("without dimensions")))
    assertThrows(classOf[IllegalStateException], () => row.toPublicState)

  @Test def pendingAndSourceObligationsCannotDisappearInAnEmptyWorld(): Unit =
    val states = List(PhysicalRecoveryLocatorState.RowInactivePending, PhysicalRecoveryLocatorState.CreatePrepared,
      PhysicalRecoveryLocatorState.UnboundAdoptionPrepared, PhysicalRecoveryLocatorState.DebugItemCreatePrepared,
      PhysicalRecoveryLocatorState.AbsentPending, PhysicalRecoveryLocatorState.AbsentVerified,
      PhysicalRecoveryLocatorState.LegacyUncertified, PhysicalRecoveryLocatorState.Quarantined)
    states.foreach { state =>
      val locator = PhysicalRecoveryLocator(UUID.randomUUID(), UUID.randomUUID(), state, "original-debt", None, None, Nil)
      assertTrue(LiveCrashRecovery.claimedDataBlock(Vector.empty, Vector(locator)).nonEmpty, state.toString)
    }

  private def permitted(recovery: Recovery): ClaimedRecoveryProgress =
    val progress = new ClaimedRecoveryProgress(recovery)
    progress.begin()
    progress.advance(ServerRecoveryTickPhase.Start)
    progress.advance(ServerRecoveryTickPhase.End)
    progress

  @Test def publishedFinishesAndUnregistersOnceWithoutExternalStopOrMoreTicks(): Unit =
    val recovery = new Recovery
    val progress = permitted(recovery)
    var unregisters = 0
    progress.published(() => { unregisters += 1; assertEquals(List(RecoveryExit.Published), recovery.exits.toList) })
    progress.stop()
    assertThrows(classOf[IllegalArgumentException], () => progress.published(() => unregisters += 1))
    assertThrows(classOf[IllegalArgumentException], () => progress.advance(ServerRecoveryTickPhase.Start))
    assertEquals(1, unregisters)
    assertEquals(List(RecoveryExit.Published), recovery.exits.toList)
    assertEquals(2, recovery.phases.size)

  @Test def failedPublishedFinishPreservesPrimaryAndUnregisterFailureWithoutSecondExit(): Unit =
    val recovery = new Recovery
    val original = new IllegalStateException("Published finish failed after side effect")
    val cleanup = new IllegalStateException("original event unregister failed")
    val stack = original.getStackTrace
    recovery.finishFailure = Some(original)
    val progress = permitted(recovery)
    var unregisters = 0
    assertSame(original, assertThrows(classOf[IllegalStateException], () => progress.published(() => {
      unregisters += 1
      throw cleanup
    })))
    assertSame(cleanup, original.getSuppressed.head)
    assertTrue(stack.sameElements(original.getStackTrace))
    progress.stop()
    assertThrows(classOf[IllegalArgumentException], () => progress.published(() => unregisters += 1))
    assertEquals(1, unregisters)
    assertEquals(List(RecoveryExit.Published), recovery.exits.toList)

  @Test def unregisterFailureAfterSuccessfulPublishedIsPrimaryAndNotRetriedByStop(): Unit =
    val recovery = new Recovery
    val progress = permitted(recovery)
    val original = new IllegalStateException("unregister failed")
    var unregisters = 0
    assertSame(original, assertThrows(classOf[IllegalStateException], () => progress.published(() => {
      unregisters += 1
      throw original
    })))
    progress.stop()
    assertEquals(1, unregisters)
    assertEquals(List(RecoveryExit.Published), recovery.exits.toList)

  @Test def mismatchedPublishedReportFailsAndCannotFallBackToExternalStop(): Unit =
    val recovery = new Recovery
    recovery.reportedExit = Some(RecoveryExit.ExternalStopWhileRecovering)
    val progress = permitted(recovery)
    var unregisters = 0
    assertThrows(classOf[IllegalArgumentException], () => progress.published(() => unregisters += 1))
    progress.stop()
    assertEquals(1, unregisters)
    assertEquals(List(RecoveryExit.Published), recovery.exits.toList)

  @Test def blockedRecoveryCannotFinishPublishedOrUnregisterItsOwner(): Unit =
    val recovery = new Recovery
    recovery.outcome = StartupPublicationDecision.Block("source obligation")
    val progress = permitted(recovery)
    var unregisters = 0
    assertThrows(classOf[IllegalArgumentException], () => progress.published(() => unregisters += 1))
    assertEquals(0, unregisters)
    assertTrue(recovery.exits.isEmpty)
    progress.stop()
    assertEquals(List(RecoveryExit.ExternalStopWhileRecovering), recovery.exits.toList)
