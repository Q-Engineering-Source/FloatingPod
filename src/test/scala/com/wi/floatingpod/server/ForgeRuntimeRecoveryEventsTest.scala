package com.wi.floatingpod.server

import net.minecraftforge.fml.common.gameevent.TickEvent
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.mutable

/** 驗證 deferred recovery 與 Forge/Cleanroom runtime event 的最小轉接契約。 */
class ForgeRuntimeRecoveryEventsTest:
  @Test
  def mapsEveryForgeServerTickPhaseInArrivalOrder(): Unit =
    val observed = mutable.ListBuffer.empty[ServerRecoveryTickPhase]
    val events = new ForgeRuntimeRecoveryEvents(ticketManager(), new RecordingEventBus)

    events.register(observed += _)
    events.onServerTick(new TickEvent.ServerTickEvent(TickEvent.Phase.START))
    events.onServerTick(new TickEvent.ServerTickEvent(TickEvent.Phase.END))

    assertEquals(List(ServerRecoveryTickPhase.Start, ServerRecoveryTickPhase.End), observed.toList)

  @Test
  def unregisterIsIdempotentBeforeAndAfterRegistrationAndDisablesTheCallback(): Unit =
    val observed = mutable.ListBuffer.empty[ServerRecoveryTickPhase]
    val bus = new RecordingEventBus
    val events = new ForgeRuntimeRecoveryEvents(ticketManager(), bus)

    events.unregister()
    events.register(observed += _)
    events.onServerTick(new TickEvent.ServerTickEvent(TickEvent.Phase.START))
    events.unregister()
    events.unregister()
    events.onServerTick(new TickEvent.ServerTickEvent(TickEvent.Phase.END))

    assertEquals(1, bus.registerCalls)
    assertEquals(1, bus.unregisterCalls)
    assertEquals(List(ServerRecoveryTickPhase.Start), observed.toList)
    assertTrue(bus.listeners.isEmpty)

  @Test
  def duplicateRegisterFailsFastWithoutASecondBusRegistration(): Unit =
    val bus = new RecordingEventBus
    val events = new ForgeRuntimeRecoveryEvents(ticketManager(), bus)
    events.register(_ => ())

    assertThrows(classOf[IllegalStateException], () => events.register(_ => ()))

    assertEquals(1, bus.registerCalls)

  @Test
  def registerFailureAfterBusSideEffectRemainsCleanableByUnregister(): Unit =
    val registrationFailure = new IllegalStateException("register after listener side effect")
    val bus = new RecordingEventBus(registerFailure = registrationFailure)
    val events = new ForgeRuntimeRecoveryEvents(ticketManager(), bus)

    val thrown = assertThrows(classOf[IllegalStateException], () => events.register(_ => ()))
    assertSame(registrationFailure, thrown)
    assertEquals(1, bus.listeners.size)

    events.unregister()
    bus.registerFailure = null
    events.register(_ => ())

    assertEquals(2, bus.registerCalls)
    assertEquals(1, bus.unregisterCalls)
    assertEquals(1, bus.listeners.size)

  @Test
  def unregisterPropagatesTheExactBusFailureOnlyOnceAndStillDisablesCallback(): Unit =
    val unregisterFailure = new IllegalStateException("unregister")
    val bus = new RecordingEventBus(unregisterFailure = unregisterFailure)
    val observed = mutable.ListBuffer.empty[ServerRecoveryTickPhase]
    val events = new ForgeRuntimeRecoveryEvents(ticketManager(), bus)
    events.register(observed += _)

    val thrown = assertThrows(classOf[IllegalStateException], () => events.unregister())
    assertSame(unregisterFailure, thrown)
    events.unregister()
    events.onServerTick(new TickEvent.ServerTickEvent(TickEvent.Phase.END))

    assertEquals(1, bus.unregisterCalls)
    assertTrue(observed.isEmpty)

  @Test
  def observesExactActiveWorldPassesInOrderButIgnoresThemAfterAnyUnregister(): Unit =
    val manager = ticketManager()
    val events = new ForgeRuntimeRecoveryEvents(manager, new RecordingEventBus)
    events.register(_ => ())

    events.observeServerWorldLoaded(-7)
    events.observeServerWorldLoaded(42)
    val initialSnapshot = manager.startupTicketCleanupSnapshot()
    events.unregister()
    events.observeServerWorldLoaded(63)

    assertEquals(
      List(
        StartupTicketCleanupObservation.WorldTicketPassCompleted(-7),
        StartupTicketCleanupObservation.WorldTicketPassCompleted(42)
      ),
      initialSnapshot
    )
    assertEquals(initialSnapshot, manager.startupTicketCleanupSnapshot())

    val failedManager = ticketManager()
    val unregisterFailure = new IllegalStateException("unregister after listener leak")
    val failedBus = new RecordingEventBus(unregisterFailure = unregisterFailure)
    val failedEvents = new ForgeRuntimeRecoveryEvents(failedManager, failedBus)
    failedEvents.register(_ => ())
    failedEvents.observeServerWorldLoaded(81)
    val beforeFailedUnregister = failedManager.startupTicketCleanupSnapshot()

    assertSame(unregisterFailure, assertThrows(classOf[IllegalStateException], () => failedEvents.unregister()))
    assertEquals(1, failedBus.listeners.size)
    failedEvents.observeServerWorldLoaded(99)

    assertEquals(List(StartupTicketCleanupObservation.WorldTicketPassCompleted(81)), beforeFailedUnregister)
    assertEquals(beforeFailedUnregister, failedManager.startupTicketCleanupSnapshot())

  @Test
  def controlledStopFailsFastWhenNoCurrentMinecraftServerExists(): Unit =
    val controlledStop = new MinecraftServerControlledStop(() => null)

    val thrown = assertThrows(classOf[IllegalStateException], () => controlledStop.requestStop())

    assertTrue(thrown.getMessage.contains("unavailable"))

  private def ticketManager(): ChunkLoadingManager = new ChunkLoadingManager(null, null)

private final class RecordingEventBus(
    var registerFailure: Throwable = null,
    var unregisterFailure: Throwable = null
) extends ForgeRuntimeRecoveryEventBus:
  val listeners = mutable.ListBuffer.empty[AnyRef]
  var registerCalls = 0
  var unregisterCalls = 0

  override def register(listener: AnyRef): Unit =
    registerCalls += 1
    listeners += listener
    if registerFailure != null then throw registerFailure

  override def unregister(listener: AnyRef): Unit =
    unregisterCalls += 1
    if unregisterFailure != null then throw unregisterFailure
    listeners -= listener
