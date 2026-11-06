package com.wi.floatingpod.server

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

final class ResidentTicketReleaseAttemptTest:
  private final class View extends TicketReleaseView:
    val ticket = new Object
    var present = true
    var releaseCalls = 0
    var fail: Throwable = null
    var leaveRegistered = false
    def assertMain(): Unit = ()
    def release(): Unit =
      releaseCalls += 1
      if !leaveRegistered then present = false
      if fail != null then throw fail
    def snapshot(): TicketReleaseSnapshot =
      if present then TicketReleaseSnapshot(Vector(ticket), Vector(ticket), Set((0, 0)))
      else TicketReleaseSnapshot(Vector.empty, Vector.empty, Set.empty)

  @Test def exactAbsenceObservedAfterReleaseIsIdempotent(): Unit =
    val view = new View
    val attempt = new ResidentTicketReleaseAttempt(view.ticket, view)
    attempt.releaseOnce(); attempt.releaseOnce()
    assertEquals(1, view.releaseCalls)
    assertFalse(view.present)

  @Test def returnWithoutAbsenceCannotAcknowledgeOrRetry(): Unit =
    val view = new View
    view.leaveRegistered = true
    val attempt = new ResidentTicketReleaseAttempt(view.ticket, view)
    val cause = assertThrows(classOf[IllegalStateException], () => attempt.releaseOnce())
    view.leaveRegistered = false
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => attempt.releaseOnce()))
    assertEquals(1, view.releaseCalls)
    assertTrue(view.present)

  @Test def partialRemovalFailureKeepsOriginalCauseEvenIfNowAbsent(): Unit =
    val view = new View
    val original = new IllegalStateException("listener failed after release")
    view.fail = original
    val attempt = new ResidentTicketReleaseAttempt(view.ticket, view)
    assertSame(original, assertThrows(classOf[IllegalStateException], () => attempt.releaseOnce()))
    view.fail = null
    assertSame(original, assertThrows(classOf[IllegalStateException], () => attempt.releaseOnce()))
    assertEquals(1, view.releaseCalls)
    assertFalse(view.present)
