package com.wi.floatingpod.server

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import com.wi.floatingpod.server.spatial.SpatialEndScope
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import scala.collection.mutable.ListBuffer

class PermanentMainPumpCoreTest:
  @Test def bootPublicationRequiresExactRecoveringDispatchAndCannotUseCopiedOrEscapedEnd():Unit =
    val server=new Object
    val core=new PermanentMainPumpCore(server,Thread.currentThread())
    var lease:PermanentPumpLease=null
    var escaped:PermanentEndTick=null
    var calls=0
    lease=core.install(UUID.randomUUID(),1L,tick=>{
      core.assertRecoveringDispatch(lease,tick)
      assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(lease,tick.copy()))
      if escaped!=null then
        assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(lease,escaped))
      escaped=tick;calls+=1
    })
    assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(lease,null))
    core.start(server,0);core.end(server,1)
    assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(lease,escaped))
    core.start(server,1);core.end(server,2)
    assertEquals(2,calls)

  @Test def bootPublicationRejectsForeignLeaseThreadAndReadyPhase():Unit =
    val server=new Object
    val core=new PermanentMainPumpCore(server,Thread.currentThread())
    val old=core.install(UUID.randomUUID(),1L,_=>())
    core.retire(old)
    var lease:PermanentPumpLease=null
    var recovering=true
    lease=core.install(UUID.randomUUID(),2L,tick=>{
      assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(old,tick))
      if recovering then
        val failure=new AtomicReference[Throwable]()
        val foreign=new Thread(()=>{
          try core.assertRecoveringDispatch(lease,tick)
          catch case cause:Throwable=>failure.set(cause)
        })
        foreign.start();foreign.join()
        assertTrue(failure.get().isInstanceOf[IllegalStateException])
        core.assertRecoveringDispatch(lease,tick)
      else assertThrows(classOf[IllegalArgumentException],()=>core.assertRecoveringDispatch(lease,tick))
    })
    core.start(server,0);core.end(server,1)
    recovering=false;core.changePhase(lease,PermanentPumpPhase.Ready)
    core.start(server,1);core.end(server,2)

  @Test def deliversRealCycleOrdinalsAcrossAllRuntimePhases(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val seen = ListBuffer.empty[PermanentEndTick]
    val boot = UUID.randomUUID()
    val lease = core.install(boot, 9L, tick => { seen += tick; () })
    core.start(server, 10); core.end(server, 11)
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 11); core.end(server, 12)
    core.changePhase(lease, PermanentPumpPhase.Quiescing)
    core.start(server, 12); core.end(server, 13)
    assertEquals(List(1L, 2L, 3L), seen.map(_.endOrdinal).toList)
    assertEquals(List(PermanentPumpPhase.Recovering, PermanentPumpPhase.Ready, PermanentPumpPhase.Quiescing), seen.map(_.phase).toList)
    assertTrue(seen.forall(t => t.bootUuid == boot && t.bootGeneration == 9L))
    assertThrows(classOf[IllegalStateException], () => core.changePhase(lease, PermanentPumpPhase.Ready))

  @Test def duplicateWrongServerAndOffMainNeverDispatch(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    var count = 0
    core.install(UUID.randomUUID(), 1L, _ => count += 1)
    assertThrows(classOf[IllegalStateException], () => core.start(new Object, 1))
    core.start(server, 1); core.end(server, 2)
    assertThrows(classOf[IllegalStateException], () => core.end(server, 2))
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() => { try core.start(server, 2) catch case cause: Throwable => failure.set(cause) })
    thread.start(); thread.join()
    assertTrue(failure.get().isInstanceOf[IllegalStateException])
    assertEquals(1, count)

  @Test def oldLeaseCannotRetireNewBootAndCounterWrapRemainsExact(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val old = core.install(UUID.randomUUID(), 1L, _ => ())
    assertTrue(core.retire(old)); assertFalse(core.retire(old))
    val seen = ListBuffer.empty[PermanentEndTick]
    val current = core.install(UUID.randomUUID(), 2L, tick => { seen += tick; () })
    assertFalse(core.retire(old)); assertTrue(core.isCurrent(current))
    core.start(server, Int.MaxValue); core.end(server, Int.MinValue)
    assertEquals(1L, seen.head.endOrdinal)

  @Test def callbackRunsWithoutMonitorAndOriginalFailureIsSticky(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val cause = new IllegalArgumentException("callback boundary")
    var calls = 0
    core.install(UUID.randomUUID(), 1L, _ => { assertFalse(Thread.holdsLock(core)); calls += 1; throw cause })
    core.start(server, 0)
    assertSame(cause, assertThrows(classOf[IllegalArgumentException], () => core.end(server, 1)))
    val sticky = assertThrows(classOf[IllegalStateException], () => core.start(server, 1))
    assertSame(cause, sticky.getCause); assertEquals(1, calls)

  @Test def registrationFailureRetainsExactLeaseAndCanBeRetiredOnce(): Unit =
    val core = new PermanentMainPumpCore(new Object, Thread.currentThread())
    val bus = new RecordingBus
    val cause = new IllegalArgumentException("register after effect")
    bus.registerFailure = cause
    val registration = new PermanentPumpRegistration(core, new Object, bus)
    val failed = assertThrows(classOf[PermanentPumpRegistrationFailure], () => registration.register(UUID.randomUUID(), 1L, _ => ()))
    assertSame(cause, failed.getCause); assertTrue(core.isCurrent(failed.lease))
    assertTrue(registration.retire(failed.lease)); assertFalse(registration.retire(failed.lease))
    assertEquals(1, bus.registerCalls); assertEquals(1, bus.unregisterCalls)

  @Test def failedUnregisterIsNotRetriedOrReportedClean(): Unit =
    val core = new PermanentMainPumpCore(new Object, Thread.currentThread())
    val bus = new RecordingBus
    val registration = new PermanentPumpRegistration(core, new Object, bus)
    val lease = registration.register(UUID.randomUUID(), 1L, _ => ())
    val cause = new IllegalArgumentException("unregister uncertain")
    bus.unregisterFailure = cause
    assertSame(cause, assertThrows(classOf[IllegalArgumentException], () => registration.retire(lease)))
    assertFalse(registration.retire(lease)); assertEquals(1, bus.unregisterCalls)
    assertSame(cause, registration.unresolvedBusFailure.get)
    val blocked = assertThrows(classOf[IllegalStateException], () => registration.register(UUID.randomUUID(), 2L, _ => ()))
    assertSame(cause, blocked.getCause)

  @Test def spatialScopeConsumesOnlyInsideItsOriginalReadyCall(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    var lease: PermanentPumpLease = null
    var escaped: SpatialEndScope = null
    var original: PermanentEndTick = null
    var calls = 0
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      original = tick
      assertThrows(classOf[IllegalArgumentException], () =>
        core.withSpatialEndScope(lease, tick.copy())(_ => ()))
      val result = core.withSpatialEndScope(lease, tick) { scope =>
        escaped = scope
        scope.consume { actual => assertSame(tick, actual); calls += 1; 42 }
      }
      assertEquals(42, result)
      assertThrows(classOf[IllegalStateException], () => escaped.consume(_ => ()))
      assertThrows(classOf[IllegalStateException], () => core.withSpatialEndScope(lease, tick)(_ => ()))
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0); core.end(server, 1)
    assertEquals(1, calls)
    assertThrows(classOf[IllegalArgumentException], () => escaped.consume(_ => ()))
    assertThrows(classOf[IllegalArgumentException], () => core.withSpatialEndScope(lease, original)(_ => ()))

  @Test def spatialScopeRejectsOldLeaseTokenAndScopeDuringANewEnd(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val oldLease = core.install(UUID.randomUUID(), 1L, _ => ())
    core.retire(oldLease)
    var lease: PermanentPumpLease = null
    var oldTick: PermanentEndTick = null
    var oldScope: SpatialEndScope = null
    var calls = 0
    lease = core.install(UUID.randomUUID(), 2L, tick => {
      assertThrows(classOf[IllegalArgumentException], () => core.withSpatialEndScope(oldLease, tick)(_ => ()))
      if oldTick != null then
        assertThrows(classOf[IllegalArgumentException], () => core.withSpatialEndScope(lease, oldTick)(_ => ()))
      core.withSpatialEndScope(lease, tick) { scope =>
        if oldScope != null then assertThrows(classOf[IllegalArgumentException], () => oldScope.consume(_ => ()))
        scope.consume(_ => calls += 1)
        oldScope = scope
      }
      oldTick = tick
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0); core.end(server, 1)
    core.start(server, 1); core.end(server, 2)
    assertEquals(2, calls)

  @Test def spatialScopeRejectsOffMainWithoutConsumingMainQualification(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    var lease: PermanentPumpLease = null
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      core.withSpatialEndScope(lease, tick) { scope =>
        val errors = new AtomicReference[List[Throwable]](Nil)
        val thread = new Thread(() => {
          var seen = List.empty[Throwable]
          try scope.consume(_ => ()) catch case cause: Throwable => seen = cause :: seen
          try core.withSpatialEndScope(lease, tick)(_ => ()) catch case cause: Throwable => seen = cause :: seen
          errors.set(seen)
        })
        thread.start(); thread.join()
        assertEquals(2, errors.get().size)
        assertTrue(errors.get().forall(_.isInstanceOf[IllegalStateException]))
        scope.consume(actual => assertSame(tick, actual))
      }
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0); core.end(server, 1)

  @Test def spatialScopeFailureCannotBeSwallowedOrReused(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val cause = new IllegalArgumentException("actual spatial consumer failure")
    var lease: PermanentPumpLease = null
    var escaped: SpatialEndScope = null
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      core.withSpatialEndScope(lease, tick) { scope =>
        escaped = scope
        assertSame(cause, assertThrows(classOf[IllegalArgumentException], () =>
          scope.consume[Unit](_ => throw cause)))
        val reuse = assertThrows(classOf[IllegalStateException], () => scope.consume(_ => ()))
        assertSame(cause, reuse.getCause)
      }
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0)
    assertSame(cause, assertThrows(classOf[IllegalArgumentException], () => core.end(server, 1)))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.start(server, 1)).getCause)
    assertThrows(classOf[IllegalStateException], () => escaped.consume(_ => ()))

  @Test def spatialOuterFailureClosesScopeAndPreservesCause(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val cause = new IllegalStateException("outer spatial failure")
    var lease: PermanentPumpLease = null
    var escaped: SpatialEndScope = null
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      core.withSpatialEndScope[Unit](lease, tick) { scope => escaped = scope; throw cause }
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.end(server, 1)))
    assertThrows(classOf[IllegalStateException], () => escaped.consume(_ => ()))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => core.start(server, 1)).getCause)

  @Test def spatialScopeRequiresActualReadyAndRevokesWithinCallback(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    var lease: PermanentPumpLease = null
    var calls = 0
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      if tick.phase == PermanentPumpPhase.Recovering then
        assertThrows(classOf[IllegalArgumentException], () => core.withSpatialEndScope(lease, tick)(_ => ()))
      else
        core.withSpatialEndScope(lease, tick) { scope =>
          scope.consume(_ => calls += 1)
          core.changePhase(lease, PermanentPumpPhase.Quiescing)
          assertThrows(classOf[IllegalArgumentException], () => scope.consume(_ => ()))
        }
    })
    core.start(server, 0); core.end(server, 1)
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 1); core.end(server, 2)
    assertEquals(1, calls)

  @Test def spatialPrimarySurvivesALaterCallbackFailure(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val primary = new IllegalArgumentException("original spatial failure")
    val secondary = new IllegalStateException("later callback failure")
    var lease: PermanentPumpLease = null
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      assertSame(primary, assertThrows(classOf[IllegalArgumentException], () =>
        core.withSpatialEndScope[Unit](lease, tick)(_ => throw primary)))
      throw secondary
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0)
    assertSame(primary, assertThrows(classOf[IllegalArgumentException], () => core.end(server, 1)))
    assertEquals(List(secondary), primary.getSuppressed.toList)
    assertSame(primary, assertThrows(classOf[IllegalStateException], () => core.start(server, 1)).getCause)

  @Test def spatialScopeCannotSurviveRetirementOrPromoteARecoveringToken(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    var lease: PermanentPumpLease = null
    lease = core.install(UUID.randomUUID(), 1L, tick => {
      if tick.phase == PermanentPumpPhase.Recovering then
        core.changePhase(lease, PermanentPumpPhase.Ready)
        assertThrows(classOf[IllegalArgumentException], () => core.withSpatialEndScope(lease, tick)(_ => ()))
      else
        core.withSpatialEndScope(lease, tick) { scope =>
          assertThrows(classOf[IllegalStateException], () => core.withSpatialEndScope(lease, tick)(_ => ()))
          assertTrue(core.retire(lease))
          assertThrows(classOf[IllegalArgumentException], () => scope.consume(_ => ()))
        }
    })
    core.start(server, 0); core.end(server, 1)
    core.start(server, 1); core.end(server, 2)
    assertFalse(core.isCurrent(lease))

  @Test def spatialScopeRequiresExactLeaseEvenWhenBootAndGenerationMatch(): Unit =
    val server = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val otherCore = new PermanentMainPumpCore(server, Thread.currentThread())
    val boot = UUID.randomUUID()
    val otherLease = otherCore.install(boot, 1L, _ => ())
    var lease: PermanentPumpLease = null
    var escaped: SpatialEndScope = null
    var calls = 0
    lease = core.install(boot, 1L, tick => {
      core.withSpatialEndScope(lease, tick) { scope =>
        escaped = scope
        val rejected = assertThrows(classOf[IllegalArgumentException], () =>
          scope.consumeFor(otherLease)(_ => calls += 1))
        assertEquals("requirement failed: spatial scope requires the exact original consumer lease", rejected.getMessage)
        assertThrows(classOf[IllegalArgumentException], () => scope.consumeFor(null)(_ => calls += 1))
        scope.consumeFor(lease) { actual => assertSame(tick, actual); calls += 1 }
      }
    })
    core.changePhase(lease, PermanentPumpPhase.Ready)
    core.start(server, 0); core.end(server, 1)
    assertEquals(1, calls)
    assertThrows(classOf[IllegalArgumentException], () => escaped.consumeFor(lease)(_ => calls += 1))
    assertTrue(core.retire(lease))
    assertThrows(classOf[IllegalArgumentException], () => escaped.consumeFor(lease)(_ => calls += 1))
    assertEquals(1, calls)

  @Test def sameMainForeignServerScopeCannotConsumeForOriginalLease(): Unit =
    val server = new Object
    val foreignServer = new Object
    val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val foreignCore = new PermanentMainPumpCore(foreignServer, Thread.currentThread())
    val boot = UUID.randomUUID()
    val originalLease = core.install(boot, 1L, _ => ())
    var foreignLease: PermanentPumpLease = null
    var calls = 0
    foreignLease = foreignCore.install(boot, 1L, tick => {
      foreignCore.withSpatialEndScope(foreignLease, tick) { scope =>
        val rejected = assertThrows(classOf[IllegalArgumentException], () =>
          scope.consumeFor(originalLease)(_ => calls += 1))
        assertEquals("requirement failed: spatial scope requires the exact original consumer lease", rejected.getMessage)
        scope.consumeFor(foreignLease) { actual => assertSame(tick, actual); calls += 1 }
      }
    })
    foreignCore.changePhase(foreignLease, PermanentPumpPhase.Ready)
    foreignCore.start(foreignServer, 0); foreignCore.end(foreignServer, 1)
    assertEquals(1, calls)
    assertTrue(core.isCurrent(originalLease))

  private class RecordingBus extends PermanentPumpEventBus:
    var registerCalls = 0
    var unregisterCalls = 0
    var registerFailure: Throwable = null
    var unregisterFailure: Throwable = null
    override def register(listener: AnyRef): Unit =
      registerCalls += 1
      if registerFailure != null then throw registerFailure
    override def unregister(listener: AnyRef): Unit =
      unregisterCalls += 1
      if unregisterFailure != null then throw unregisterFailure
