package com.wi.floatingpod.network

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.server.{ActiveFloatingPodManager, ChunkLoadingManager, ChunkTicket, ChunkTicketGateway, DeferredRecovery, DeferredRecoveryBundle, PodWorld, RecoveryExit, RecoveryFinishReport, RecoveryStep, RuntimeControlledStop, RuntimeRecoveryEvents, RuntimeRecoveryTermination, ServerRecoveryTickPhase, ServerWorldResolver, StartupPublicationDecision}
import com.wi.floatingpod.server.persistence.{PeriodicFlushService, PersistenceManager}
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, DeactivationOutcome, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryStore, PlayerEntitlementStore, RecoveryDimensionBackfillOutcome, RecoveryPodStore, StoredPodRow}

import net.minecraftforge.common.ForgeChunkManager
import net.minecraftforge.common.ForgeChunkManager.Ticket
import net.minecraftforge.fml.common.event.{FMLServerAboutToStartEvent, FMLServerStartingEvent, FMLServerStoppedEvent, FMLServerStoppingEvent}
import net.minecraftforge.fml.common.network.simpleimpl.MessageContext

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

/** M5B 的服务器私有设置、运行时装配与 FML 生命周期行为测试。 */
class ServerRuntimeLifecycleTest:
  /** AboutToStart 是可否决边界，工厂或 callback 故障不得降级成仅记录日志。 */
  @Test
  def aboutToStartFailurePropagatesToTheLifecycleCaller(): Unit =
    val failure = new IllegalStateException("callback failed")
    val lifecycle = new ServerRuntimeLifecycle(new M5bFailingAboutFactory(failure))
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod-about-failure.db"), 1))

    assertSame(failure, assertThrows(classOf[IllegalStateException], () => lifecycle.onServerAboutToStart(null)))

  @Test
  def lifecycleRetainsExactStoppingAndFinalCleanupFailuresAfterRuntimeClears(): Unit =
    val events = ListBuffer.empty[String]
    val endpointFailure = new IllegalStateException("endpoint")
    val persistenceFailure = new IllegalStateException("persistence")
    val closeFailure = new IllegalStateException("close")
    val resolverFailure = new IllegalStateException("resolver")
    val managed = new ManagedServerRuntime(
      new M5bRepository(events, closeFailure),
      new M5bPersistence(events, stopFailure = persistenceFailure),
      new M5bEndpoint,
      new M5bRegistry(events, uninstallFailure = endpointFailure),
      clearStartingEvent = () => throw resolverFailure
    )
    val lifecycle = new ServerRuntimeLifecycle(new M5bRecordingFactory(managed))
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod-report-identities.db"), 1))

    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)
    lifecycle.onServerStopping(null)
    lifecycle.onServerStopped(null)

    assertTrue(lifecycle.latestStoppingReport.exists(_.endpointUninstallFailure.contains(endpointFailure)))
    assertTrue(lifecycle.latestStoppingReport.exists(_.persistenceFailure.contains(persistenceFailure)))
    assertTrue(lifecycle.latestFinalShutdownReport.exists(_.closeFailure.contains(closeFailure)))
    assertTrue(lifecycle.latestFinalShutdownReport.exists(_.resolverClearFailure.contains(resolverFailure)))

  @Test
  def abortClearsStartingBindingOnceAndRetainsExactClearFailure(): Unit =
    val events = ListBuffer.empty[String]
    val startFailure = new IllegalStateException("start")
    val clearFailure = new IllegalStateException("clear")
    var binds = 0
    var clears = 0
    val runtime = new ManagedServerRuntime(
      new M5bRepository(events),
      new M5bPersistence(events, startFailure = startFailure),
      new M5bEndpoint,
      new M5bRegistry(events),
      bindStartingEvent = _ => binds += 1,
      clearStartingEvent = () => { clears += 1; throw clearFailure }
    )

    val thrown = assertThrows(classOf[IllegalStateException], () => runtime.onServerStarting(null))
    runtime.onServerStopping(null)
    runtime.onServerStopped(null)

    assertSame(startFailure, thrown)
    assertEquals(1, binds)
    assertEquals(1, clears)
    assertTrue(thrown.getSuppressed.contains(clearFailure))
    assertTrue(runtime.latestStoppingReport.exists(_.startupAbortFailure.contains(startFailure)))

  /** 缺少 Stopping 时仍只可关闭 repository，不能以虚假的 clean report 补做 null stop。 */
  @Test
  def stoppedWithoutStoppingClosesOnceButReportsDirty(): Unit =
    val events = ListBuffer.empty[String]
    val runtime = new ManagedServerRuntime(new M5bRepository(events), new M5bPersistence(events), new M5bEndpoint, new M5bRegistry(events))

    runtime.onServerStopped(null)
    runtime.onServerStopped(null)

    assertEquals(List("repository-close"), events.toList)
    assertTrue(runtime.latestFinalShutdownReport.nonEmpty)
    assertFalse(runtime.latestFinalShutdownReport.get.isClean)
  /** AboutToStart 必须先建立同一 runtime，才能登记 ticket 恢复回调。 */
  @Test
  def aboutToStartCreatesTheRuntimeBeforeStarting(): Unit =
    val events = ListBuffer.empty[String]
    val runtime = new M5bFourPhaseRuntime(events)
    val lifecycle = new ServerRuntimeLifecycle(new M5bRecordingFactory(runtime))
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod-about.db"), 1))

    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)

    assertEquals(List("create", "about", "start"), events.toList)
  @Test
  def configuredLifecycleStartsPersistenceBeforeInstallingItsEndpoint(): Unit =
    val events = ListBuffer.empty[String]
    val runtime = new M5bRecordingRuntime(events)
    val lifecycle = new ServerRuntimeLifecycle(new M5bRecordingFactory(runtime))

    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod.db"), 256))
    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)

    assertEquals(List("create", "persistence-start", "endpoint-install"), events.toList)

  @Test
  def settingsWriteDefaultServerPrivateSqlitePathOnlyWhenDefaultsAreGenerated(): Unit =
    val configFile = Files.createTempDirectory("floatingpod-m5b-settings").resolve("floatingpod.cfg")
    val configuration = new M5bConfiguration()

    val settings = ServerRuntimeSettings.load(configuration, configFile.toFile)

    assertEquals("sqlite", settings.backend)
    assertEquals(configFile.getParent.resolve("floatingpod").resolve("floatingpod.db").toAbsolutePath.normalize, settings.sqlitePath)
    assertEquals(256, settings.activeLimit)
    assertTrue(configuration.saved)

  @Test
  def settingsNormalizeSqliteAndAcceptConfiguredPathAndCapacity(): Unit =
    val configFile = Files.createTempDirectory("floatingpod-m5b-valid").resolve("floatingpod.cfg")
    val configuration = new M5bConfiguration(
      Map("backend" -> "SQLITE", "sqlitePath" -> "private/pods.db", "activeLimit" -> "17")
    )

    val settings = ServerRuntimeSettings.load(configuration, configFile.toFile)

    assertEquals("sqlite", settings.backend)
    assertEquals(configFile.getParent.resolve("private/pods.db").toAbsolutePath.normalize, settings.sqlitePath)
    assertEquals(17, settings.activeLimit)
    assertFalse(configuration.saved)

  @Test
  def settingsRejectUnsupportedBackendBlankPathAndOutOfRangeCapacity(): Unit =
    assertSettingsFailure("backend", "mysql")
    assertSettingsFailure("sqlitePath", "   ")
    assertSettingsFailure("activeLimit", "0")
    assertSettingsFailure("activeLimit", "257")
    assertSettingsFailure("activeLimit", "not-an-integer")

  @Test
  def lifecycleRejectsStartBeforeConfigureDuplicateConfigureDuplicateStartAndStopBeforeStart(): Unit =
    val events = ListBuffer.empty[String]
    val lifecycle = new ServerRuntimeLifecycle(new M5bRecordingFactory(new M5bRecordingRuntime(events)))

    assertThrows(classOf[IllegalStateException], () => lifecycle.onServerStarting(null))
    assertThrows(classOf[IllegalStateException], () => lifecycle.onServerStopping(null))
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod.db"), 1))
    assertThrows(classOf[IllegalStateException], () => lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/other.db"), 1)))
    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)
    assertThrows(classOf[IllegalStateException], () => lifecycle.onServerStarting(null))

  @Test
  def lifecycleCreatesFreshRuntimeForRestartAfterSuccessfulStop(): Unit =
    val events = ListBuffer.empty[String]
    val factory = new M5bRestartingFactory(events)
    val lifecycle = new ServerRuntimeLifecycle(factory)
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod.db"), 1))

    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)
    lifecycle.onServerStopping(null)
    lifecycle.onServerStopped(null)
    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)
    lifecycle.onServerStopping(null)
    lifecycle.onServerStopped(null)

    assertEquals(2, factory.created)
    assertEquals(List("start-1", "stop-1", "start-2", "stop-2"), events.toList)

  @Test
  def lifecycleReleasesClosedRuntimeAfterStopFailureAndPreservesItsSuppressedCauses(): Unit =
    val events = ListBuffer.empty[String]
    val uninstallFailure = new IllegalStateException("uninstall failed")
    val persistenceFailure = new IllegalStateException("persistence failed")
    val closeFailure = new IllegalStateException("close failed")
    val failedRuntime = new ManagedServerRuntime(
      new M5bRepository(events, closeFailure),
      new M5bPersistence(events, stopFailure = persistenceFailure),
      new M5bEndpoint,
      new M5bRegistry(events, uninstallFailure = uninstallFailure)
    )
    val replacement = new M5bRecordingRuntime(events)
    val factory = new M5bRuntimeSequenceFactory(failedRuntime, replacement)
    val lifecycle = new ServerRuntimeLifecycle(factory)
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/failed-stop.db"), 1))
    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)

    lifecycle.onServerStopping(null)
    lifecycle.onServerStopped(null)
    lifecycle.onServerAboutToStart(null)
    lifecycle.onServerStarting(null)
    assertEquals(2, factory.created)

  @Test
  def managedRuntimeUninstallsBeforeStoppingPersistenceAndClosingRepository(): Unit =
    val events = ListBuffer.empty[String]
    val runtime = new ManagedServerRuntime(new M5bRepository(events), new M5bPersistence(events), new M5bEndpoint, new M5bRegistry(events))

    runtime.onServerStarting(null)
    runtime.onServerStopping(null)

    assertEquals(List("persistence-start", "endpoint-install", "endpoint-uninstall", "persistence-stop"), events.toList)

  @Test
  def endpointInstallFailureAbortsOnlyStartedPersistenceAndDoesNotRunNormalStopping(): Unit =
    val events = ListBuffer.empty[String]
    val installFailure = new IllegalStateException("install failed")
    val registry = new M5bRegistry(events, installFailure = installFailure)
    val runtime = new ManagedServerRuntime(new M5bRepository(events), new M5bPersistence(events), new M5bEndpoint, registry)

    assertSame(installFailure, assertThrows(classOf[IllegalStateException], () => runtime.onServerStarting(null)))
    runtime.onServerStopping(null)
    runtime.onServerStopped(null)
    assertEquals(List("persistence-start", "endpoint-install", "endpoint-uninstall", "persistence-abort", "repository-close"), events.toList)
    assertTrue(runtime.latestStoppingReport.exists(report => !report.isClean))

  @Test
  def stoppingAfterAboutBeforeStartingRecordsStartupAbortWithoutNormalPersistenceShutdown(): Unit =
    val events = ListBuffer.empty[String]
    val runtime = new ManagedServerRuntime(new M5bRepository(events), new M5bPersistence(events), new M5bEndpoint, new M5bRegistry(events))

    runtime.onServerAboutToStart(null)
    runtime.onServerStopping(null)
    runtime.onServerStopped(null)

    assertEquals(List("repository-close"), events.toList)
    assertTrue(runtime.latestStoppingReport.exists(report => !report.isClean && report.startupAbortFailure.nonEmpty))

  @Test
  def managedRuntimeRegistersTicketRecoveryCallbackOnlyOnce(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    val gateway = new M5bGateway
    val runtime = new ManagedServerRuntime(
      repository,
      new M5bPersistence(events),
      new M5bEndpoint,
      new M5bRegistry(events),
      ticketManager = Some(new ChunkLoadingManager(repository, gateway))
    )

    runtime.onServerAboutToStart(null)
    runtime.onServerAboutToStart(null)

    assertEquals(1, gateway.callbackRegistrations)

  @Test
  def managedRuntimeValidatesWorldBindingBeforeRegisteringTicketCallbackOnce(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    val gateway = new M5bGateway(events = events)
    val runtime = new ManagedServerRuntime(
      repository,
      new M5bPersistence(events),
      new M5bEndpoint,
      new M5bRegistry(events),
      ticketManager = Some(new ChunkLoadingManager(repository, gateway)),
      validateWorldBinding = _ => events += "binding-validated"
    )

    runtime.onServerAboutToStart(null)
    runtime.onServerAboutToStart(null)

    assertEquals(List("binding-validated", "callback-registered"), events.toList)
    assertEquals(1, gateway.callbackRegistrations)

  @Test
  def aboutCallbackFailureClosesRepositoryOnceAndRetainsExactCloseFailure(): Unit =
    val events = ListBuffer.empty[String]
    val callbackFailure = new IllegalStateException("callback failed")
    val closeFailure = new IllegalStateException("close failed")
    val failedRuntime = new ManagedServerRuntime(
      new M5bRepository(events, closeFailure),
      new M5bPersistence(events),
      new M5bEndpoint,
      new M5bRegistry(events),
      ticketManager = Some(new ChunkLoadingManager(new M5bRepository(events), new M5bGateway(callbackFailure)))
    )
    val lifecycle = new ServerRuntimeLifecycle(new M5bRuntimeSequenceFactory(failedRuntime, new M5bRecordingRuntime(events)))
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/about-close.db"), 1))

    val thrown = assertThrows(classOf[IllegalStateException], () => lifecycle.onServerAboutToStart(null))
    assertTrue(thrown.getSuppressed.contains(closeFailure))
    lifecycle.onServerStopped(null)

    assertEquals(1, events.count(_ == "repository-close"))
    assertTrue(lifecycle.latestFinalShutdownReport.exists(_.closeFailure.contains(closeFailure)))
    lifecycle.onServerAboutToStart(null)

  @Test
  def stopPreservesPrimaryFailureAndSuppressesLaterCleanupFailures(): Unit =
    val events = ListBuffer.empty[String]
    val uninstallFailure = new IllegalStateException("uninstall failed")
    val persistenceFailure = new IllegalStateException("persistence failed")
    val closeFailure = new IllegalStateException("close failed")
    val runtime = new ManagedServerRuntime(
      new M5bRepository(events, closeFailure),
      new M5bPersistence(events, stopFailure = persistenceFailure),
      new M5bEndpoint,
      new M5bRegistry(events, uninstallFailure = uninstallFailure)
    )
    runtime.onServerStarting(null)

    runtime.onServerStopping(null)
    runtime.onServerStopped(null)
    assertEquals(List("persistence-start", "endpoint-install", "endpoint-uninstall", "persistence-stop", "repository-close"), events.toList)

  @Test
  def productionFactoryComposesRuntimeWithInjectedStorageAndForgeFreeConstructors(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    val registry = new M5bRegistry(events)
    val factory = new ForgeServerRuntimeFactory(
      new Object(),
      repositoryFactory = path => { events += "repository"; repository },
      ticketManagerFactory = (repo, instance) => { events += "tickets"; new ChunkLoadingManager(repo, new M5bGateway) },
      managerFactory = (repo, tickets) => { events += "manager"; new ActiveFloatingPodManager(repo, tickets) },
      flushFactory = manager => { events += "flush"; new PeriodicFlushService(manager) },
      persistenceFactory = (manager, flush) => { events += "persistence"; new PersistenceManager(manager, flush) },
      endpointFactory = processor => { events += "endpoint"; new M5bEndpoint },
      endpointRegistry = registry
    )

    val runtime = factory.create(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/factory.db"), 2))

    assertTrue(runtime.isInstanceOf[ManagedServerRuntime])
    assertEquals(List("repository", "tickets", "manager", "endpoint", "flush", "persistence"), events.toList)

  @Test
  def productionFactoryAssemblesOneDeferredBundleAndKeepsAwaitingPublicationFenceClosed(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    var bundleCalls = 0
    val factory = new ForgeServerRuntimeFactory(
      new Object(),
      repositoryFactory = _ => repository,
      ticketManagerFactory = (repo, _) => new ChunkLoadingManager(repo, new M5bGateway(events = events)),
      managerFactory = (repo, tickets) => new ActiveFloatingPodManager(repo, tickets),
      flushFactory = manager => new PeriodicFlushService(manager),
      persistenceFactory = (manager, flush) => new PersistenceManager(manager, flush),
      endpointFactory = _ => new M5bEndpoint,
      endpointRegistry = new M5bRegistry(events),
      worldBindingGateFactory = _ => _ => events += "binding-validated",
      deferredBundleFactory = (repo, tickets, manager, worlds, supplier) =>
        bundleCalls += 1
        assertSame(repository, repo)
        assertTrue(tickets != null && manager != null && worlds != null)
        assertTrue(supplier() == null)
        DeferredRecoveryBundle(
          new DeferredRecoveryScript(events, RecoveryStep.AwaitingTick, Nil, null, null, null),
          new DeferredRecoveryEvents(events, null, null),
          new DeferredControlledStop(events, null)
        )
    )

    val runtime = factory.create(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/factory-deferred-bundle.db"), 2))
    runtime.onServerAboutToStart(null)
    runtime.onServerStarting(null)

    assertEquals(1, bundleCalls)
    assertEquals(List("binding-validated", "callback-registered", "recovery-register", "begin"), events.toList)
    assertFalse(events.contains("persistence-start"))
    assertFalse(events.contains("endpoint-install"))
    runtime.onServerStopping(null)
    runtime.onServerStopped(null)
    assertTrue(events.contains("recovery-unregister"))
    assertTrue(events.contains("repository-close"))

  @Test
  def productionFactoryInstallsWorldBindingGateBeforeTheTicketCallbackAndOnlyOnce(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    val gateway = new M5bGateway(events = events)
    val factory = new ForgeServerRuntimeFactory(
      new Object(),
      repositoryFactory = _ => repository,
      ticketManagerFactory = (repo, _) => new ChunkLoadingManager(repo, gateway),
      managerFactory = (repo, tickets) => new ActiveFloatingPodManager(repo, tickets),
      flushFactory = manager => new PeriodicFlushService(manager),
      persistenceFactory = (manager, flush) => new PersistenceManager(manager, flush),
      endpointFactory = _ => new M5bEndpoint,
      endpointRegistry = new M5bRegistry(events),
      worldBindingGateFactory = (store: PhysicalRecoveryStore) =>
        assertSame(repository, store)
        (_: FMLServerAboutToStartEvent) => events += "binding-validated",
      deferredBundleFactory = (_, _, _, _, _) => DeferredRecoveryBundle(
        new DeferredRecoveryScript(events, RecoveryStep.AwaitingTick, Nil, null, null, null),
        new DeferredRecoveryEvents(events, null, null),
        new DeferredControlledStop(events, null)
      )
    )

    val runtime = factory.create(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/factory-world-binding-gate.db"), 2))
    runtime.onServerAboutToStart(null)
    runtime.onServerAboutToStart(null)

    assertEquals(List("binding-validated", "callback-registered", "recovery-register"), events.toList)
    assertEquals(1, gateway.callbackRegistrations)

  @Test
  def productionFactoryClosesOpenedRepositoryAndPreservesConstructionFailure(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bRepository(events)
    val constructionFailure = new IllegalStateException("manager construction failed")
    val factory = new ForgeServerRuntimeFactory(
      new Object(),
      repositoryFactory = path => { events += "repository"; repository },
      managerFactory = (repo, tickets) => throw constructionFailure
    )

    val failure = assertThrows(classOf[IllegalStateException], () => factory.create(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/factory-failure.db"), 2)))

    assertSame(constructionFailure, failure)
    assertEquals(List("repository", "repository-close"), events.toList)

  @Test
  def productionFactoryRejectsMissingEntitlementStoreBeforeTicketsAndClosesOnce(): Unit =
    val events = ListBuffer.empty[String]
    val repository = new M5bMissingEntitlementRepository(events)
    val factory = new ForgeServerRuntimeFactory(
      new Object(),
      repositoryFactory = path => { events += "repository"; repository },
      ticketManagerFactory = (_, _) => throw new AssertionError("tickets must not be constructed")
    )

    assertThrows(classOf[IllegalStateException], () => factory.create(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/missing-entitlement.db"), 1)))
    assertEquals(List("repository", "repository-close"), events.toList)

  @Test
  def deferredAwaitingTickReturnsNormallyWithPublicationFenceClosed(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick)

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)

    assertEquals(List("binding-validate", "callback-registered", "recovery-register", "bind", "begin"), harness.events.toList)
    assertEquals(0, harness.recoveryEvents.unregisterCalls)
    assertEquals(0, harness.controlledStop.requests)

  @Test
  def deferredAsyncPermitPublishesOnceInExactOrder(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))))

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.recoveryEvents.tick()
    harness.recoveryEvents.tick()

    assertEquals(List("binding-validate", "callback-registered", "recovery-register", "bind", "begin", "advance", "persistence-start", "endpoint-install", "finish:Published", "recovery-unregister"), harness.events.toList)
    assertEquals(List(RecoveryExit.Published), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(0, harness.controlledStop.requests)

  @Test
  def deferredTickUsesExactStartAndEndPhasesAtTheRecoverySeam(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.AwaitingTick), Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))))

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.recoveryEvents.tick(ServerRecoveryTickPhase.Start)
    harness.recoveryEvents.tick(ServerRecoveryTickPhase.End)

    assertEquals(List(ServerRecoveryTickPhase.Start, ServerRecoveryTickPhase.End), harness.recovery.advancePhases.toList)
    assertEquals(List("binding-validate", "callback-registered", "recovery-register", "bind", "begin", "advance", "advance", "persistence-start", "endpoint-install", "finish:Published", "recovery-unregister"), harness.events.toList)

  @Test
  def deferredPlannedRemediationFinishesUnregistersAndRequestsOneControlledStop(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(1))))))

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.recoveryEvents.tick()
    harness.recoveryEvents.tick()

    assertEquals(List(RecoveryExit.PlannedRemediation), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.controlledStop.requests)
    assertTrue(!harness.events.contains("persistence-start"))
    assertTrue(!harness.events.contains("endpoint-install"))

  @Test
  def deferredAsyncBlockFinishesAndRequestsOneControlledStopWithoutPublication(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Block("unsafe")))))

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.recoveryEvents.tick()

    assertEquals(List(RecoveryExit.Blocked), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.controlledStop.requests)
    assertTrue(!harness.events.contains("persistence-start"))

  @Test
  def deferredAdvanceFailurePreservesCauseAndDoesNotRepeatTerminalActions(): Unit =
    val failure = new IllegalStateException("advance failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Left(failure)))

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    assertSame(failure, assertThrows(classOf[IllegalStateException], () => harness.recoveryEvents.tick()))
    harness.recoveryEvents.tick()
    harness.runtime.onServerStopping(null)

    assertEquals(List(RecoveryExit.AbortStartup), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.controlledStop.requests)
    assertSame(failure, harness.runtime.latestStoppingReport.get.deferredFailure.get)

  @Test
  def deferredAdvanceFailureRethrowsOriginalCauseAfterControlledStopFailure(): Unit =
    val advanceFailure = new IllegalStateException("advance failed")
    val stopFailure = new IllegalStateException("stop failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Left(advanceFailure)), stopFailure = stopFailure)

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    assertSame(advanceFailure, assertThrows(classOf[IllegalStateException], () => harness.recoveryEvents.tick()))
    harness.recoveryEvents.tick()

    assertTrue(advanceFailure.getSuppressed.contains(stopFailure))
    assertEquals(List(RecoveryExit.AbortStartup), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.controlledStop.requests)

  @Test
  def deferredExternalStopWhileRecoveringFinishesWithoutFabricatingPersistenceStop(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick)

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.runtime.onServerStopping(null)

    val report = harness.runtime.latestStoppingReport.get
    assertEquals(Some(RuntimeRecoveryTermination.ExternalStopWhileRecovering), report.deferredTermination)
    assertEquals(List(RecoveryExit.ExternalStopWhileRecovering), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertTrue(!harness.events.contains("persistence-stop"))
    assertFalse(report.isClean)

  @Test
  def deferredExternalStoppingAggregatesBrokenFinishAndStillCloses(): Unit =
    def assertExternalShutdown(harness: DeferredRuntimeHarness): Unit =
      harness.runtime.onServerAboutToStart(null)
      harness.runtime.onServerStarting(null)
      assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerStopping(null))
      assertEquals(Some(RuntimeRecoveryTermination.ExternalStopWhileRecovering), harness.runtime.latestStoppingReport.get.deferredTermination)
      harness.runtime.onServerStopped(null)
      val report = harness.runtime.latestFinalShutdownReport.get
      assertTrue(report.stoppingObserved)
      assertEquals(Some(RuntimeRecoveryTermination.ExternalStopWhileRecovering), report.deferredTermination)
      assertEquals(1, harness.recoveryEvents.unregisterCalls)
      assertEquals(1, harness.events.count(_ == "repository-close"))
      assertEquals(1, harness.events.count(_ == "clear"))

    assertExternalShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishResult = RecoveryFinishReport(RecoveryExit.ExternalStopWhileRecovering, List(new IllegalStateException("cleanup")))))
    assertExternalShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishResult = RecoveryFinishReport(RecoveryExit.Blocked, Nil)))
    assertExternalShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishFailure = new IllegalStateException("finish")))

  @Test
  def deferredPublicationFailureRollsBackFenceAndFinishesAbort(): Unit =
    val failure = new IllegalStateException("endpoint install failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))), installFailure = failure)

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    assertSame(failure, assertThrows(classOf[IllegalStateException], () => harness.recoveryEvents.tick()))
    harness.runtime.onServerStopping(null)

    assertEquals(List(RecoveryExit.AbortStartup), harness.recovery.finishExits)
    assertEquals(Some(RuntimeRecoveryTermination.Aborted(failure)), harness.runtime.latestStoppingReport.get.deferredTermination)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.controlledStop.requests)
    assertEquals(List("persistence-start", "endpoint-install", "endpoint-uninstall", "persistence-abort"), harness.events.toList.filter(event => event.startsWith("persistence") || event.startsWith("endpoint")))

  @Test
  def deferredStoppedWithoutStoppingFinishesUnregistersAndClosesDirtyOnce(): Unit =
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick)

    harness.runtime.onServerAboutToStart(null)
    harness.runtime.onServerStarting(null)
    harness.runtime.onServerStopped(null)
    harness.runtime.onServerStopped(null)

    assertEquals(List(RecoveryExit.StoppedWithoutStopping), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.events.count(_ == "repository-close"))
    assertTrue(harness.runtime.latestFinalShutdownReport.get.dirtyClosed)

  @Test
  def deferredStoppedWithoutStoppingAggregatesBrokenFinishAndPublishesFinalReport(): Unit =
    def assertStoppedShutdown(harness: DeferredRuntimeHarness, expected: Throwable = null): Unit =
      harness.runtime.onServerAboutToStart(null)
      harness.runtime.onServerStarting(null)
      val thrown = assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerStopped(null))
      if expected != null then assertSame(expected, thrown)
      val report = harness.runtime.latestFinalShutdownReport.get
      assertFalse(report.stoppingObserved)
      assertEquals(Some(RuntimeRecoveryTermination.StoppedWithoutStopping), report.deferredTermination)
      assertEquals(1, harness.recoveryEvents.unregisterCalls)
      assertEquals(1, harness.events.count(_ == "repository-close"))
      assertEquals(1, harness.events.count(_ == "clear"))
      harness.runtime.onServerStopped(null)
      assertEquals(1, harness.recoveryEvents.unregisterCalls)
      assertEquals(1, harness.events.count(_ == "repository-close"))

    assertStoppedShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishResult = RecoveryFinishReport(RecoveryExit.StoppedWithoutStopping, List(new IllegalStateException("cleanup")))))
    assertStoppedShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishResult = RecoveryFinishReport(RecoveryExit.Blocked, Nil)))
    val finishFailure = new IllegalStateException("finish")
    assertStoppedShutdown(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, finishFailure = finishFailure), finishFailure)

  @Test
  def deferredBeginFailureRecordsExactAbortedTermination(): Unit =
    val beginFailure = new IllegalStateException("begin failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, beginFailure = beginFailure)

    harness.runtime.onServerAboutToStart(null)
    assertSame(beginFailure, assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerStarting(null)))
    harness.runtime.onServerStopping(null)

    assertEquals(Some(RuntimeRecoveryTermination.Aborted(beginFailure)), harness.runtime.latestStoppingReport.get.deferredTermination)
    assertEquals(List(RecoveryExit.AbortStartup), harness.recovery.finishExits)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)

  @Test
  def deferredBindFailureBeforeBeginNeverFinishesRecoveryButRetainsAbortedEvidence(): Unit =
    val bindFailure = new IllegalStateException("bind failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, bindFailure = bindFailure)

    harness.runtime.onServerAboutToStart(null)
    assertSame(bindFailure, assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerStarting(null)))
    harness.runtime.onServerStopping(null)
    harness.runtime.onServerStopped(null)

    assertFalse(harness.events.contains("begin"))
    assertEquals(Nil, harness.recovery.finishExits.toList)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertTrue(!harness.events.contains("persistence-start"))
    assertEquals(Some(RuntimeRecoveryTermination.Aborted(bindFailure)), harness.runtime.latestStoppingReport.get.deferredTermination)
    assertEquals(Some(RuntimeRecoveryTermination.Aborted(bindFailure)), harness.runtime.latestFinalShutdownReport.get.deferredTermination)

  @Test
  def deferredPreparedDirectStoppedUnregisterFailurePublishesFinalEvidenceThenRethrows(): Unit =
    val unregisterFailure = new IllegalStateException("unregister failed")
    val closeFailure = new IllegalStateException("close failed")
    val resolverFailure = new IllegalStateException("resolver failed")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, unregisterFailure = unregisterFailure, closeFailure = closeFailure, resolverFailure = resolverFailure)

    harness.runtime.onServerAboutToStart(null)
    assertSame(unregisterFailure, assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerStopped(null)))

    val report = harness.runtime.latestFinalShutdownReport.get
    assertSame(unregisterFailure, report.deferredFailure.get)
    assertSame(closeFailure, report.closeFailure.get)
    assertSame(resolverFailure, report.resolverClearFailure.get)
    assertTrue(unregisterFailure.getSuppressed.contains(closeFailure))
    assertTrue(unregisterFailure.getSuppressed.contains(resolverFailure))
    assertFalse(report.isClean)
    assertFalse(report.dirtyClosed)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertEquals(1, harness.events.count(_ == "repository-close"))
    assertEquals(1, harness.events.count(_ == "clear"))

  @Test
  def deferredImmediateRemediationAndBlockPreserveTerminalPayloadAndRequestStop(): Unit =
    val podUuid = uuid(77)
    val remediation = new DeferredRuntimeHarness(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(podUuid)))
    remediation.runtime.onServerAboutToStart(null)
    remediation.runtime.onServerStarting(null)
    remediation.runtime.onServerStopping(null)
    assertEquals(Some(RuntimeRecoveryTermination.PlannedRemediation(podUuid)), remediation.runtime.latestStoppingReport.get.deferredTermination)
    assertEquals(List(RecoveryExit.PlannedRemediation), remediation.recovery.finishExits)
    assertEquals(1, remediation.controlledStop.requests)

    val blocked = new DeferredRuntimeHarness(RecoveryStep.Terminal(StartupPublicationDecision.Block("blocked-now")))
    blocked.runtime.onServerAboutToStart(null)
    blocked.runtime.onServerStarting(null)
    blocked.runtime.onServerStopping(null)
    assertEquals(Some(RuntimeRecoveryTermination.Blocked("blocked-now", None)), blocked.runtime.latestStoppingReport.get.deferredTermination)
    assertEquals(List(RecoveryExit.Blocked), blocked.recovery.finishExits)
    assertEquals(1, blocked.controlledStop.requests)

  @Test
  def deferredPublishedFinishRejectsNonCleanMismatchedAndThrownReports(): Unit =
    def assertPublicationRollback(harness: DeferredRuntimeHarness): Unit =
      assertEquals(List("persistence-start", "endpoint-install", "endpoint-uninstall", "persistence-abort"), harness.events.toList.filter(event => event.startsWith("persistence") || event.startsWith("endpoint")))
      assertEquals(1, harness.recoveryEvents.unregisterCalls)
      assertEquals(1, harness.controlledStop.requests)

    val nonClean = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))), finishResult = RecoveryFinishReport(RecoveryExit.Published, List(new IllegalStateException("cleanup"))))
    nonClean.runtime.onServerAboutToStart(null); nonClean.runtime.onServerStarting(null)
    assertThrows(classOf[IllegalStateException], () => nonClean.recoveryEvents.tick())
    assertEquals(List(RecoveryExit.Published), nonClean.recovery.finishExits)
    assertPublicationRollback(nonClean)

    val mismatched = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))), finishResult = RecoveryFinishReport(RecoveryExit.Blocked, Nil))
    mismatched.runtime.onServerAboutToStart(null); mismatched.runtime.onServerStarting(null)
    assertThrows(classOf[IllegalStateException], () => mismatched.recoveryEvents.tick())
    assertEquals(List(RecoveryExit.Published), mismatched.recovery.finishExits)
    assertPublicationRollback(mismatched)

    val thrown = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))), finishFailure = new IllegalStateException("finish failed"))
    thrown.runtime.onServerAboutToStart(null); thrown.runtime.onServerStarting(null)
    assertThrows(classOf[IllegalStateException], () => thrown.recoveryEvents.tick())
    assertEquals(List(RecoveryExit.Published), thrown.recovery.finishExits)
    assertPublicationRollback(thrown)

    val unregisterFailure = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.Permit(Nil)))), unregisterFailure = new IllegalStateException("unregister"))
    unregisterFailure.runtime.onServerAboutToStart(null); unregisterFailure.runtime.onServerStarting(null)
    assertThrows(classOf[IllegalStateException], () => unregisterFailure.recoveryEvents.tick())
    assertEquals(List(RecoveryExit.Published), unregisterFailure.recovery.finishExits)
    assertPublicationRollback(unregisterFailure)

  @Test
  def deferredPreparedListenerUnregistersBeforeStartingOnStoppingAndStopped(): Unit =
    val stopping = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick)
    stopping.runtime.onServerAboutToStart(null); stopping.runtime.onServerStopping(null); stopping.runtime.onServerStopped(null)
    assertEquals(1, stopping.recoveryEvents.unregisterCalls)
    assertEquals(Some(RuntimeRecoveryTermination.PreparedStopBeforeStarting), stopping.runtime.latestStoppingReport.get.deferredTermination)
    assertTrue(stopping.runtime.latestStoppingReport.get.deferredFinishReport.isEmpty)

    val stopped = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick)
    stopped.runtime.onServerAboutToStart(null); stopped.runtime.onServerStopped(null)
    assertEquals(1, stopped.recoveryEvents.unregisterCalls)
    assertEquals(Some(RuntimeRecoveryTermination.PreparedStopBeforeStarting), stopped.runtime.latestFinalShutdownReport.get.deferredTermination)
    assertTrue(stopped.runtime.latestFinalShutdownReport.get.deferredFinishReport.isEmpty)

  @Test
  def deferredRegisterFailureAfterListenerSideEffectUnregistersAndSuppressesCleanupFailure(): Unit =
    val registrationFailure = new IllegalStateException("register")
    val unregisterFailure = new IllegalStateException("unregister")
    val harness = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, registerFailure = registrationFailure, unregisterFailure = unregisterFailure)

    val thrown = assertThrows(classOf[IllegalStateException], () => harness.runtime.onServerAboutToStart(null))

    assertSame(registrationFailure, thrown)
    assertEquals(1, harness.recoveryEvents.unregisterCalls)
    assertTrue(thrown.getSuppressed.contains(unregisterFailure))

  @Test
  def deferredControlledRemediationClosureTruthTableRequiresEverySuccessfulBoundary(): Unit =
    def planned(harness: DeferredRuntimeHarness, stopping: Boolean): FinalShutdownReport =
      harness.runtime.onServerAboutToStart(null); harness.runtime.onServerStarting(null); harness.recoveryEvents.tick()
      if stopping then harness.runtime.onServerStopping(null)
      harness.runtime.onServerStopped(null)
      harness.runtime.latestFinalShutdownReport.get

    assertTrue(planned(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(81)))))), true).controlledRemediationClosed)
    assertFalse(planned(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(82)))))), false).controlledRemediationClosed)
    assertFalse(planned(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(83))))), closeFailure = new IllegalStateException("close")), true).controlledRemediationClosed)
    assertFalse(planned(new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(84))))), resolverFailure = new IllegalStateException("resolver")), true).controlledRemediationClosed)
    val unregisterFailure = new IllegalStateException("unregister")
    val failedUnregister = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(85))))), unregisterFailure = unregisterFailure)
    failedUnregister.runtime.onServerAboutToStart(null); failedUnregister.runtime.onServerStarting(null)
    assertSame(unregisterFailure, assertThrows(classOf[IllegalStateException], () => failedUnregister.recoveryEvents.tick()))
    failedUnregister.runtime.onServerStopping(null); failedUnregister.runtime.onServerStopped(null)
    assertFalse(failedUnregister.runtime.latestFinalShutdownReport.get.controlledRemediationClosed)
    val stopFailure = new IllegalStateException("stop")
    val failedStop = new DeferredRuntimeHarness(RecoveryStep.AwaitingTick, List(Right(RecoveryStep.Terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(uuid(86))))), stopFailure = stopFailure)
    failedStop.runtime.onServerAboutToStart(null); failedStop.runtime.onServerStarting(null)
    assertSame(stopFailure, assertThrows(classOf[IllegalStateException], () => failedStop.recoveryEvents.tick()))
    failedStop.runtime.onServerStopping(null); failedStop.runtime.onServerStopped(null)
    assertFalse(failedStop.runtime.latestFinalShutdownReport.get.controlledRemediationClosed)

  private def uuid(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")

  private def assertSettingsFailure(key: String, value: String): Unit =
    val configFile = Files.createTempDirectory("floatingpod-m5b-invalid").resolve("floatingpod.cfg")
    assertThrows(classOf[IllegalArgumentException], () => ServerRuntimeSettings.load(new M5bConfiguration(Map(key -> value)), configFile.toFile))

private final class M5bRecordingFactory(runtime: ServerRuntime) extends ServerRuntimeFactory:
  override def create(settings: ServerRuntimeSettings): ServerRuntime =
    runtime match
      case recording: M5bRecordingRuntime => recording.events += "create"
      case fourPhase: M5bFourPhaseRuntime => fourPhase.events += "create"
      case _ => ()
    runtime

private final class M5bFailingAboutFactory(failure: Throwable) extends ServerRuntimeFactory:
  override def create(settings: ServerRuntimeSettings): ServerRuntime =
    new ServerRuntime:
      override def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit = throw failure
      override def onServerStarting(event: FMLServerStartingEvent): Unit = ()

private final class M5bRecordingRuntime(val events: ListBuffer[String]) extends ServerRuntime:
  override def onServerStarting(event: FMLServerStartingEvent): Unit =
    events += "persistence-start"
    events += "endpoint-install"

private final class M5bFourPhaseRuntime(val events: ListBuffer[String]) extends ServerRuntime:
  override def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit = events += "about"
  override def onServerStarting(event: FMLServerStartingEvent): Unit = events += "start"

private final class M5bRestartingFactory(events: ListBuffer[String]) extends ServerRuntimeFactory:
  var created = 0
  override def create(settings: ServerRuntimeSettings): ServerRuntime =
    created += 1
    new ServerRuntime:
      override def onServerStarting(event: FMLServerStartingEvent): Unit = events += s"start-$created"
      override def onServerStopping(event: FMLServerStoppingEvent): Unit = events += s"stop-$created"

private final class M5bRuntimeSequenceFactory(first: ServerRuntime, second: ServerRuntime) extends ServerRuntimeFactory:
  var created = 0
  override def create(settings: ServerRuntimeSettings): ServerRuntime =
    created += 1
    if created == 1 then first else second

private final class M5bPersistence(events: ListBuffer[String], startFailure: Throwable = null, stopFailure: Throwable = null) extends RuntimePersistence:
  override def start(event: FMLServerStartingEvent): Unit =
    events += "persistence-start"
    if startFailure != null then throw startFailure
  override def stop(event: FMLServerStoppingEvent): Unit =
    events += "persistence-stop"
    if stopFailure != null then throw stopFailure
  override def abortStarted(): Unit =
    events += "persistence-abort"

private final class M5bRegistry(events: ListBuffer[String], installFailure: Throwable = null, uninstallFailure: Throwable = null) extends RuntimeEndpointRegistry:
  override def install(endpoint: ServerPacketEndpoint): Unit =
    events += "endpoint-install"
    if installFailure != null then throw installFailure
  override def uninstall(): Unit =
    events += "endpoint-uninstall"
    if uninstallFailure != null then throw uninstallFailure

private final class M5bEndpoint extends ServerPacketEndpoint:
  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit = ()
  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit = ()

private final class M5bRepository(events: ListBuffer[String], closeFailure: Throwable = null) extends FloatingPodRepository, ActivityTransitionStore, RecoveryPodStore, PlayerEntitlementStore, PhysicalRecoveryStore:
  override def insert(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def update(state: FloatingPodState): Try[FloatingPodState] = Success(state)
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = Success(None)
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(Nil)
  override def findAllActive(): Try[List[FloatingPodState]] = Success(Nil)
  override def findAll(): Try[List[FloatingPodState]] = Success(Nil)
  override def delete(uuid: UUID): Try[Boolean] = Success(false)
  override def transaction[T](fn: => T): Try[T] = Try(fn)
  override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] = throw new UnsupportedOperationException
  override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] = throw new UnsupportedOperationException
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] = Success(Nil)
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(None)
  override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = throw new UnsupportedOperationException
  override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(Nil)
  override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] = Success(None)
  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = throw new UnsupportedOperationException("physical recovery binding is not exercised by M5b")
  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("physical recovery binding is not exercised by M5b")
  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = throw new UnsupportedOperationException("physical recovery binding is not exercised by M5b")
  override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = throw new UnsupportedOperationException("physical recovery locator is not exercised by M5b")
  override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] = throw new UnsupportedOperationException("physical recovery activation is not exercised by M5b")
  override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] = throw new UnsupportedOperationException("physical recovery deactivation is not exercised by M5b")
  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = throw new UnsupportedOperationException("physical recovery locator is not exercised by M5b")
  override def close(): Unit =
    events += "repository-close"
    if closeFailure != null then throw closeFailure

private final class M5bMissingEntitlementRepository(events: ListBuffer[String]) extends FloatingPodRepository, ActivityTransitionStore, RecoveryPodStore, PhysicalRecoveryStore:
  private val delegate = new M5bRepository(events)
  override def insert(state: FloatingPodState): Try[FloatingPodState] = delegate.insert(state)
  override def update(state: FloatingPodState): Try[FloatingPodState] = delegate.update(state)
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] = delegate.findByUuid(uuid)
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = delegate.findByOwner(ownerUuid)
  override def findAllActive(): Try[List[FloatingPodState]] = delegate.findAllActive()
  override def findAll(): Try[List[FloatingPodState]] = delegate.findAll()
  override def delete(uuid: UUID): Try[Boolean] = delegate.delete(uuid)
  override def transaction[T](fn: => T): Try[T] = delegate.transaction(fn)
  override def activateIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int): Try[AdmissionOutcome] = delegate.activateIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit)
  override def deactivateIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long): Try[DeactivationOutcome] = delegate.deactivateIfExpectedActive(podUuid, expectedOwner, expectedStateVersion)
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] = delegate.findDesiredActiveRows()
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = delegate.findStoredByUuid(uuid)
  override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = delegate.backfillMissingActiveDimension(uuid, expectedStateVersion, authoritativeDimensionId)
  override def findAllRecoverable(): Try[List[FloatingPodState]] = delegate.findAllRecoverable()
  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] = delegate.findPhysicalRecoveryBinding()
  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] = delegate.createOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion)
  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] = delegate.certifyPhysicalRecoveryBinding(expectedUuid, expectedFormatVersion)
  override def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator] = delegate.prepareCreateLocator(podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks)
  override def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome] = delegate.activatePreparedIfAdmissible(target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid)
  override def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome] = delegate.deactivateTrackedIfExpectedActive(podUuid, expectedOwner, expectedStateVersion, attemptUuid)
  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] = delegate.findPhysicalRecoveryLocator(podUuid)
  override def close(): Unit = delegate.close()

private final class M5bGateway(callbackFailure: Throwable = null, events: ListBuffer[String] = null) extends ChunkTicketGateway:
  var callbackRegistrations = 0
  override def registerCallback(callback: ForgeChunkManager.OrderedLoadingCallback): Unit =
    callbackRegistrations += 1
    if events != null then events += "callback-registered"
    if callbackFailure != null then throw callbackFailure
  override def request(world: PodWorld): Option[ChunkTicket] = None
  override def release(ticket: ChunkTicket): Unit = ()
  override def maxChunkDepth(ticket: ChunkTicket): Int = 25
  override def setChunkDepth(ticket: ChunkTicket, depth: Int): Unit = ()
  override def writePodUuid(ticket: ChunkTicket, podUuid: UUID): Unit = ()
  override def readPodUuid(ticket: ChunkTicket): Option[UUID] = None
  override def force(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def unforce(ticket: ChunkTicket, chunkX: Int, chunkZ: Int): Unit = ()
  override def wrapRecovered(ticket: Ticket): ChunkTicket = throw new UnsupportedOperationException
  override def unwrap(ticket: ChunkTicket): Option[Ticket] = None

private final class M5bConfiguration(values: Map[String, String] = Map.empty) extends ServerRuntimeConfiguration:
  private var changed = false
  var loaded = false
  var saved = false
  override def load(): Unit = loaded = true
  override def string(key: String, category: String, defaultValue: String, comment: String): String =
    values.getOrElse(key, { changed = true; defaultValue })
  override def hasChanged: Boolean = changed
  override def save(): Unit = saved = true

private final class DeferredRuntimeHarness(
    beginStep: RecoveryStep,
    advances: List[Either[Throwable, RecoveryStep]] = Nil,
    beginFailure: Throwable = null,
    bindFailure: Throwable = null,
    installFailure: Throwable = null,
    finishResult: RecoveryFinishReport = null,
    finishFailure: Throwable = null,
    registerFailure: Throwable = null,
    unregisterFailure: Throwable = null,
    stopFailure: Throwable = null,
    closeFailure: Throwable = null,
    resolverFailure: Throwable = null
):
  val events = ListBuffer.empty[String]
  val repository = new M5bRepository(events, closeFailure)
  val tickets = new ChunkLoadingManager(repository, new M5bGateway(events = events))
  val recovery = new DeferredRecoveryScript(events, beginStep, advances, beginFailure, finishResult, finishFailure)
  val recoveryEvents = new DeferredRecoveryEvents(events, registerFailure, unregisterFailure)
  val controlledStop = new DeferredControlledStop(events, stopFailure)
  val runtime = new ManagedServerRuntime(
    repository,
    new M5bPersistence(events),
    new M5bEndpoint,
    new M5bRegistry(events, installFailure),
    Some(tickets),
    None,
    _ => { events += "bind"; if bindFailure != null then throw bindFailure },
    () => { events += "clear"; if resolverFailure != null then throw resolverFailure },
    _ => events += "binding-validate",
    Some(DeferredRecoveryBundle(recovery, recoveryEvents, controlledStop))
  )

private final class DeferredRecoveryScript(
    events: ListBuffer[String],
    beginResult: RecoveryStep,
    advanceResults: List[Either[Throwable, RecoveryStep]],
    beginFailure: Throwable,
    finishResult: RecoveryFinishReport,
    finishFailure: Throwable
) extends DeferredRecovery:
  private var advanceIndex = 0
  val finishExits = ListBuffer.empty[RecoveryExit]
  val advancePhases = ListBuffer.empty[ServerRecoveryTickPhase]

  override def begin(): RecoveryStep =
    events += "begin"
    if beginFailure != null then throw beginFailure
    beginResult
  override def advance(phase: ServerRecoveryTickPhase): RecoveryStep =
    events += "advance"
    advancePhases += phase
    val result = advanceResults.lift(advanceIndex).getOrElse(Right(RecoveryStep.AwaitingTick))
    advanceIndex += 1
    result.fold(throw _, identity)
  override def finish(exit: RecoveryExit): RecoveryFinishReport =
    events += s"finish:$exit"
    finishExits += exit
    if finishFailure != null then throw finishFailure
    if finishResult != null then finishResult else RecoveryFinishReport(exit, Nil)

private final class DeferredRecoveryEvents(events: ListBuffer[String], registerFailure: Throwable, unregisterFailure: Throwable) extends RuntimeRecoveryEvents:
  private var callback: Option[ServerRecoveryTickPhase => Unit] = None
  var unregisterCalls = 0

  override def register(callback: ServerRecoveryTickPhase => Unit): Unit =
    if this.callback.nonEmpty then throw new AssertionError("recovery callback registered twice")
    events += "recovery-register"
    this.callback = Some(callback)
    if registerFailure != null then throw registerFailure
  override def unregister(): Unit =
    unregisterCalls += 1
    events += "recovery-unregister"
    callback = None
    if unregisterFailure != null then throw unregisterFailure
  def tick(phase: ServerRecoveryTickPhase = ServerRecoveryTickPhase.End): Unit = callback.foreach(_(phase))

private final class DeferredControlledStop(events: ListBuffer[String], stopFailure: Throwable) extends RuntimeControlledStop:
  var requests = 0
  override def requestStop(): Unit =
    requests += 1
    events += "controlled-stop"
    if stopFailure != null then throw stopFailure
