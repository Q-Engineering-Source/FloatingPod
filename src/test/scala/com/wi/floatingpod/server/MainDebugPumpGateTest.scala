package com.wi.floatingpod.server

import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** actual pump owner/lease/phase逻辑；没有caller boolean Ready或正式DI运行。 */
class MainDebugPumpGateTest:
  @Test def recoveringAndQuiescingCannotAuthorizeEndpoint(): Unit =
    val server = new Object
    val pump = new PermanentMainPumpCore(server, Thread.currentThread())
    val lease = pump.install(UUID.randomUUID(), 1L, _ => ())
    assertThrows(classOf[IllegalArgumentException], () => pump.assertReadyCurrent(lease))
    pump.changePhase(lease, PermanentPumpPhase.Ready)
    pump.assertReadyCurrent(lease)
    pump.changePhase(lease, PermanentPumpPhase.Quiescing)
    assertThrows(classOf[IllegalArgumentException], () => pump.assertReadyCurrent(lease))
  @Test def foreignAndRetiredLeaseCannotAuthorizeReady(): Unit =
    val first = new PermanentMainPumpCore(new Object, Thread.currentThread())
    val second = new PermanentMainPumpCore(new Object, Thread.currentThread())
    val a = first.install(UUID.randomUUID(), 1L, _ => ())
    val b = second.install(UUID.randomUUID(), 1L, _ => ())
    first.changePhase(a, PermanentPumpPhase.Ready)
    assertThrows(classOf[IllegalArgumentException], () => first.assertReadyCurrent(b))
    first.retire(a)
    assertThrows(classOf[IllegalArgumentException], () => first.assertReadyCurrent(a))
  @Test def actualCallbackFailureRetainsOriginalCauseInsteadOfReady(): Unit =
    val server = new Object
    val pump = new PermanentMainPumpCore(server, Thread.currentThread())
    val original = new IllegalStateException("actual END callback failed")
    val lease = pump.install(UUID.randomUUID(), 1L, _ => throw original)
    pump.changePhase(lease, PermanentPumpPhase.Ready)
    pump.start(server, 10)
    assertSame(original, assertThrows(classOf[IllegalStateException], () => pump.end(server, 11)))
    val failed = assertThrows(classOf[IllegalStateException], () => pump.assertReadyCurrent(lease))
    assertSame(original, failed.getCause)

  @Test def phaseChangeInsideRecoveringCallbackOnlyAffectsNextActualEnd(): Unit =
    val server = new Object
    val pump = new PermanentMainPumpCore(server, Thread.currentThread())
    var lease: PermanentPumpLease = null
    val seen = scala.collection.mutable.ListBuffer.empty[PermanentEndTick]
    lease = pump.install(UUID.randomUUID(), 7L, tick => {
      seen += tick
      if tick.endOrdinal == 3L then
        assertEquals(PermanentPumpPhase.Recovering, tick.phase)
        pump.changePhase(lease, PermanentPumpPhase.Ready)
        pump.assertReadyCurrent(lease)
        assertEquals(PermanentPumpPhase.Recovering, tick.phase)
    })
    (10 to 13).foreach { counter => pump.start(server, counter); pump.end(server, counter + 1) }
    assertEquals(List(1L, 2L, 3L, 4L), seen.map(_.endOrdinal).toList)
    assertEquals(List(PermanentPumpPhase.Recovering, PermanentPumpPhase.Recovering,
      PermanentPumpPhase.Recovering, PermanentPumpPhase.Ready), seen.map(_.phase).toList)

  @Test def actualReadFirstSchedulerRetainsThenSwitchesOnlyOnNextEnd(): Unit =
    val seen = scala.collection.mutable.ArrayBuffer.empty[String]
    def step(n: Long, sealedRead: Boolean, completed: Long, pending: Boolean): Long =
      MainDebugItemEndpoint.advanceReadFirstEnd(n, sealedRead, completed)(
        seen += "ordinary", seen += "retain", seen += "read", pending, seen += "strict")
    assertEquals(0L, step(1L, false, 0L, true))
    assertEquals(Vector("ordinary", "read"), seen.toVector)
    seen.clear()
    assertEquals(0L, step(2L, true, 0L, true))
    assertEquals(Vector("retain", "read"), seen.toVector)
    seen.clear()
    val completed = step(3L, true, 0L, false)
    assertEquals(3L, completed)
    assertEquals(Vector("retain", "read", "strict"), seen.toVector)
    seen.clear()
    assertEquals(3L, step(4L, true, completed, false))
    assertEquals(Vector("ordinary", "read"), seen.toVector)
    assertThrows(classOf[IllegalArgumentException], () => step(3L, true, completed, false))

  @Test def actualReadFirstSchedulerDoesNotTurnFailureIntoPending(): Unit =
    val cause = new IllegalStateException("actual read barrier failure")
    var dispatched = false
    assertSame(cause, assertThrows(classOf[IllegalStateException], () =>
      MainDebugItemEndpoint.advanceReadFirstEnd(1L, true, 0L)(
        { dispatched = true }, (), (), throw cause, ())))
    assertFalse(dispatched)
