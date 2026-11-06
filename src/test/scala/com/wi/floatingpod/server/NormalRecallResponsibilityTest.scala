package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 生产 observer 的直接 seam 验证；手动ordinal不被报告为游戏 END/runtime证据。 */
class NormalRecallResponsibilityTest:
  private val tag = IoOperationTag(new UUID(1, 2), 3L, 4L, new UUID(5, 6))
  private val pod = new UUID(7, 8)
  private val attempt = new UUID(9, 10)
  private class View extends RemovalWorldView:
    val main = Thread.currentThread()
    val worldRef = new Object()
    val body = new Object()
    val dimensionId = -1
    var present = true
    var calls = 0
    var failure: Throwable = null
    var foreign = false
    def assertMain(): Unit = require(Thread.currentThread() eq main)
    def describeBody(value: AnyRef): RemovalEntityView =
      require(value eq body)
      RemovalEntityView(body, worldRef, 99, pod)
    def snapshot(value: AnyRef, id: Int, uuid: UUID): RemovalWorldSnapshot =
      val entity = describeBody(value)
      val slots = if present then Vector(RemovalIndexSlot("tracker-map", entity, Some(99), None)) else Vector.empty
      RemovalWorldSnapshot(dimensionId, Vector(worldRef), entity,
        if foreign then slots :+ RemovalIndexSlot("foreign-id", entity.copy(body = new Object()), Some(99), None) else slots)
    def requestRemove(value: AnyRef): Unit =
      require(value eq body)
      calls += 1
      if failure != null then throw failure

  @Test def endpointPhysicalEndOrderReachesLaterRemovalAckWithoutDroppingOriginalRecord(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    val record = new Object()
    val ordinaryRecord = new Object()
    val retainedManager = scala.collection.mutable.ArrayBuffer(record, ordinaryRecord)
    val custody = new NormalRecallDriftCustody(record, pending)
    var samples = 0
    var ordinarySamples = 0
    var acknowledgement = Option.empty[ExactBodyRemovalAcknowledgement]
    def drift(): Unit = retainedManager.foreach { current =>
      custody.observeOrdinary(current) {
        if current eq record then
          require(view.present, "original live-body check")
          samples += 1
        else ordinarySamples += 1
      }
    }
    MainDebugItemEndpoint.advancePhysicalEnd(drift(), {
      observer.request(pending, 25L)
      view.present = false
      assertTrue(observer.acknowledgeForIo(pending, 25L).isEmpty)
    })
    assertEquals(1, samples, "capture alone cannot remove ordinary sampling")
    MainDebugItemEndpoint.advancePhysicalEnd(drift(), {
      acknowledgement = observer.acknowledgeForIo(pending, 26L)
      observer.normalRecallFacts(pending, acknowledgement.get)
    })
    assertEquals(1, samples, "removed original body must not be live-sampled before later END")
    assertEquals(2, ordinarySamples, "another record remains under ordinary drift")
    assertEquals(26L, acknowledgement.get.observedEndOrdinal())
    assertSame(record, retainedManager.head, "sampling handoff cannot clean original manager")
    assertSame(view.body, pending.identity.body)
    assertEquals(1, view.calls)

  @Test def physicalEndFailureKeepsOriginalRemovalAndDoesNotResumeOrdinarySampling(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    val record = new Object()
    val custody = new NormalRecallDriftCustody(record, pending)
    val cause = new IllegalStateException("partial actual removal")
    view.failure = cause
    assertSame(cause, assertThrows(classOf[IllegalStateException], () =>
      MainDebugItemEndpoint.advancePhysicalEnd(custody.observeOrdinary(record)(()), observer.request(pending, 25L))))
    view.present = false
    assertSame(cause, assertThrows(classOf[IllegalStateException], () =>
      MainDebugItemEndpoint.advancePhysicalEnd(
        custody.observeOrdinary(record)(fail("removed original was sampled")),
        pending.requestFailure.foreach(throw _))))
    assertEquals(1, view.calls)
    assertSame(view.body, pending.identity.body)
    custody.observeOrdinary(new Object())(assertSame(cause, pending.requestFailure.get))

  @Test def samplingHandoffCannotHideLaterForeignIdentityConflict(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    val record = new Object()
    val custody = new NormalRecallDriftCustody(record, pending)
    observer.request(pending, 25L)
    view.present = false
    view.foreign = true
    MainDebugItemEndpoint.advancePhysicalEnd(
      custody.observeOrdinary(record)(fail("removed original was sampled")), {
        assertTrue(observer.acknowledgeForIo(pending, 26L).isEmpty)
        assertTrue(pending.conflict.nonEmpty)
      })
    assertEquals(1, view.calls)
    assertSame(view.body, pending.identity.body)

  @Test def factsRetainOriginalCaptureRequestAndLaterEnd(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    observer.request(pending, 25L)
    view.present = false
    assertTrue(observer.acknowledgeForIo(pending, 25L).isEmpty)
    val ack = observer.acknowledgeForIo(pending, 26L).get
    val facts = observer.normalRecallFacts(pending, ack)
    assertEquals(tag, facts.tag)
    assertEquals(20L, facts.capturedEnd)
    assertEquals(25L, facts.requestedEnd)
    assertEquals(26L, facts.observedEnd)
    assertEquals(1, view.calls)

  @Test def equalValuedForeignAckAndOtherObserverCannotTranscribe(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    observer.request(pending, 25L)
    view.present = false
    val ack = observer.acknowledgeForIo(pending, 26L).get
    val foreign = ExactBodyRemovalAcknowledgement.issue(tag.bootId, tag.bootGeneration, tag.connectionGeneration,
      tag.operationId, attempt, pod, 11L, -1, 99, 26L)
    assertThrows(classOf[IllegalArgumentException], () => observer.normalRecallFacts(pending, foreign))
    assertThrows(classOf[IllegalArgumentException], () => new ExactBodyRemovalObserver(view).normalRecallFacts(pending, ack))

  @Test def requestFailureCannotBecomeNormalRecallSuccessAfterAbsence(): Unit =
    val view = new View
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body, tag, attempt, 11L, 20L)
    val cause = new IllegalStateException("actual removal failed")
    view.failure = cause
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => observer.request(pending, 25L)))
    view.present = false
    val ack = observer.acknowledgeForIo(pending, 26L).get
    assertThrows(classOf[IllegalArgumentException], () => observer.normalRecallFacts(pending, ack))
    assertSame(cause, pending.requestFailure.get)
    assertEquals(1, view.calls)
