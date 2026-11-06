package com.wi.floatingpod.server

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows}
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class ExactTicketReleaseObserverTest:
  @Test
  def completeReleaseChecksAllThreeActualCollections(): Unit =
    val view = new MutableView
    view.onRelease = () => view.clear()
    ExactTicketReleaseObserver.release(view.ticket, view)
    assertEquals(1, view.requests)
    // Forge allows repeated release of already absent tickets; the postcondition still runs.
    ExactTicketReleaseObserver.release(view.ticket, view)
    assertEquals(2, view.snapshots)

  @Test
  def eachIndependentResidualRejectsNormalReturn(): Unit =
    Vector("registry", "forced", "requested").foreach { source =>
      val view = new MutableView
      view.onRelease = () =>
        view.clear()
        source match
          case "registry" => view.registry += view.ticket
          case "forced" => view.forced += view.ticket
          case "requested" => view.chunks += ((1, 2))
      assertThrows(classOf[IllegalStateException], () => ExactTicketReleaseObserver.release(view.ticket, view))
      assertEquals(1, view.snapshots)
    }

  @Test
  def partialUnforceFailureThenNormalRetryStillRejectsStaleForcedMap(): Unit =
    val view = new MutableView
    val primary = new IllegalStateException("actual unforce listener failure")
    view.onRelease = () =>
      view.chunks.clear()
      throw primary
    assertSame(primary, assertThrows(classOf[IllegalStateException], () => ExactTicketReleaseObserver.release(view.ticket, view)))
    assertEquals(0, view.snapshots)
    // The second Forge call skips now-empty requested chunks and only removes registry ownership.
    view.onRelease = () => { view.registry.clear(); () }
    assertThrows(classOf[IllegalStateException], () => ExactTicketReleaseObserver.release(view.ticket, view))
    assertEquals(2, view.requests)
    assertEquals(Vector(view.ticket), view.forced.toVector)

  @Test
  def equalButDifferentTicketDoesNotRetainExactOwnership(): Unit =
    val view = new MutableView
    val other = new EqualTicket
    view.onRelease = () =>
      view.clear()
      view.registry += other
      view.forced += other
    ExactTicketReleaseObserver.release(view.ticket, view)
    assertEquals(1, view.snapshots)

  @Test
  def observationFailurePreservesCauseAndOffMainNeverReleases(): Unit =
    val view = new MutableView
    val primary = new IllegalArgumentException("actual registry unavailable")
    view.onRelease = () => view.clear()
    view.snapshotFailure = primary
    assertSame(primary, assertThrows(classOf[IllegalArgumentException], () => ExactTicketReleaseObserver.release(view.ticket, view)))
    val failure = new AtomicReference[Throwable]()
    val worker = new Thread(() =>
      try ExactTicketReleaseObserver.release(view.ticket, view)
      catch case caught: Throwable => failure.set(caught)
    )
    worker.start()
    worker.join()
    assertEquals(classOf[IllegalStateException], failure.get().getClass)
    assertEquals(1, view.requests)

  private class EqualTicket:
    override def equals(other: Any): Boolean = other.isInstanceOf[EqualTicket]
    override def hashCode(): Int = 1

  private class MutableView extends TicketReleaseView:
    val ticket = new EqualTicket
    val registry = mutable.ArrayBuffer[AnyRef](ticket)
    val forced = mutable.ArrayBuffer[AnyRef](ticket)
    val chunks = mutable.Set((1, 2))
    val owner = Thread.currentThread()
    var requests = 0
    var snapshots = 0
    var onRelease: () => Unit = () => ()
    var snapshotFailure: Throwable = null
    override def assertMain(): Unit =
      if Thread.currentThread() ne owner then throw new IllegalStateException("not Main")
    override def release(): Unit =
      requests += 1
      onRelease()
    override def snapshot(): TicketReleaseSnapshot =
      snapshots += 1
      if snapshotFailure != null then throw snapshotFailure
      TicketReleaseSnapshot(registry.toVector, forced.toVector, chunks.toSet)
    def clear(): Unit =
      registry.clear()
      forced.clear()
      chunks.clear()
