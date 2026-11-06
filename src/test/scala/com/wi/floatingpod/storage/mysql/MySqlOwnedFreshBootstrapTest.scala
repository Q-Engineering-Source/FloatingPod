package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{DedicatedBootstrapBridge, IoPreparationState}
import com.wi.floatingpod.server.io.*
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CompletableFuture, CompletionException, CompletionStage, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 实际adapter/bridge/worker的pre-resource拒绝；没有构造fake JDBC或native成功。 */
class MySqlOwnedFreshBootstrapTest:
  private val limit = Duration.ofSeconds(5)
  private def identity() = MySqlOwnedIoIdentity(UUID.randomUUID(), 1, 1)
  private def tag(id: MySqlOwnedIoIdentity) = IoOperationTag(id.bootId, id.bootGeneration, id.connectionGeneration, UUID.randomUUID())
  private def join(t: Thread): Unit =
    t.join(5000)
    assertFalse(t.isAlive, "bounded thread join timeout")
  private def waitOff(adapter: MySqlOwnedIoAdapter): Unit =
    val result = new AtomicReference[java.lang.Boolean]()
    val failure = new AtomicReference[Throwable]()
    val tail = new Thread(() => {
      try result.set(adapter.awaitExitOffMain(5, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "fresh-adapter-unit-tail")
    var primary: Throwable = null
    try
      tail.start()
      join(tail)
      if failure.get() != null then throw failure.get()
      assertEquals(java.lang.Boolean.TRUE, result.get())
    catch case c: Throwable => primary = c
    finally
      try join(tail)
      catch case c: Throwable =>
        if primary == null then primary = c else MySqlOwnedSessionLifecycle.suppress(primary, c)
    if primary != null then throw primary
  private def managed(adapter: MySqlOwnedIoAdapter)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case c: Throwable => primary = c
    finally
      try
        adapter.quiesce()
        waitOff(adapter)
        assertTrue(adapter.checkpoint.io.stopped)
      catch case c: Throwable =>
        if primary == null then primary = c else MySqlOwnedSessionLifecycle.suppress(primary, c)
    if primary != null then throw primary

  @Test def missingFreshInputsFailOnActualWorkerAndExactCompletionStillConsumesReservation(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "fresh-missing-input-unit", id)
    managed(adapter) {
      val operation = tag(id)
      assertEquals(IoSubmission.Accepted, adapter.submit(operation, MySqlOwnedIoCommand.BootFresh(null, null, null, null)))
      assertEquals(IoSubmission.RejectedCapacity, adapter.submit(tag(id), MySqlOwnedIoCommand.Renew))
      adapter.quiesce()
      waitOff(adapter)
      val completion = adapter.drain(1).head
      assertSame(operation, completion.tag)
      assertTrue(completion.result.isFailure)
      assertEquals("requirement failed: explicit Fresh Boot inputs required", completion.result.failed.get.getMessage)
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
    }

  @Test def certifiedAndFreshShareOneAttemptAndCannotFallbackIntoOtherBootMode(): Unit =
    for freshFirst <- Vector(true, false) do
      val id = identity()
      val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "fresh-certified-one-shot-unit", id)
      managed(adapter) {
        val normal = MySqlOwnedIoCommand.Boot(null, null, null)
        val fresh = MySqlOwnedIoCommand.BootFresh(null, null, null, null)
        val first = tag(id); val second = tag(id)
        adapter.submit(first, if freshFirst then fresh else normal)
        adapter.submit(second, if freshFirst then normal else fresh)
        adapter.quiesce()
        waitOff(adapter)
        val messages = adapter.drain(2)
        assertEquals(Vector(first, second), messages.map(_.tag))
        assertTrue(messages.forall(_.result.isFailure))
        assertEquals("requirement failed: owned MySQL Boot is one-shot", messages(1).result.failed.get.getMessage)
        assertEquals(0, adapter.checkpoint.io.reserved)
        assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
      }

  @Test def wrongActorAndForeignGenerationAreRejectedBeforeNativeOrJdbcOwnership(): Unit =
    val id = identity()
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "fresh-owner-unit", id)
    val failure = new AtomicReference[Throwable]()
    val actor = new Thread(() => {
      try assertThrows(classOf[IllegalArgumentException], () =>
        adapter.submit(tag(id), MySqlOwnedIoCommand.BootFresh(null, null, null, null)))
      catch case cause: Throwable => failure.set(cause)
      ()
    })
    managed(adapter) {
      var primary: Throwable = null
      try
        actor.start()
        join(actor)
        if failure.get() != null then throw failure.get()
        assertThrows(classOf[IllegalArgumentException], () =>
          adapter.submit(IoOperationTag(id.bootId, 2, 1, UUID.randomUUID()), MySqlOwnedIoCommand.BootFresh(null, null, null, null)))
        assertEquals(0, adapter.checkpoint.io.reserved)
      catch case cause: Throwable => primary = cause
      finally
        try join(actor)
        catch case c: Throwable =>
          if primary == null then primary = c else MySqlOwnedSessionLifecycle.suppress(primary, c)
      if primary != null then throw primary
    }

  @Test def freshPreparationUsesActualBridgeOwnerAndConsumesActualBootFailureWithoutTransfer(): Unit =
    val failure = new AtomicReference[Throwable]()
    val sentinel = new IllegalStateException("no runtime Fresh factory installed by this unit test")
    val factory = new DedicatedBootstrapBridge.Factory:
      override def open(server: Object, control: DedicatedBootstrapBridge): DedicatedBootstrapBridge.Callbacks =
        new DedicatedBootstrapBridge.Callbacks:
          override def prepare(): CompletionStage[Object] =
            val id = identity()
            val adapter = new MySqlOwnedIoAdapter(1, control.controlThread(), "fresh-prep-no-resource-unit", id)
            managed(adapter) {
              val operation = tag(id)
              val holder = adapter.installFreshBootstrapPreparation(control, operation, MySqlOwnedIoCommand.BootFresh(null, null, null, null))
              assertThrows(classOf[IllegalArgumentException], () => adapter.submit(operation, MySqlOwnedIoCommand.Renew))
              assertThrows(classOf[IllegalArgumentException], () => adapter.drain(1))
              val stage = holder.prepare(limit)
              val original = assertThrows(classOf[CompletionException], () => stage.toCompletableFuture().getNow(null)).getCause
              assertEquals(IoPreparationState.ConsumedFailure(original), holder.observation.state)
              assertEquals(0, adapter.checkpoint.io.reserved)
              val target = new Thread(() => ())
              assertThrows(classOf[IllegalArgumentException], () => adapter.issueOwnerTransfer(target, operation))
            }
            CompletableFuture.failedFuture[Object](sentinel)
          override def issueTransfer(p: Object, t: Thread): Object = throw new UnsupportedOperationException("no actual runtime factory")
          override def claim(p: Object, r: Object): Unit = throw new UnsupportedOperationException("no actual runtime factory")
          override def abortTransfer(p: Object, r: Object, c: Throwable): Unit = throw new UnsupportedOperationException("no actual runtime factory")
          override def finalizeFailure(p: Object, r: Object, c: Throwable): CompletionStage[Void] = CompletableFuture.completedFuture[Void](null)
          override def failedStartupTermination(report: DedicatedBootstrapBridge.FailureReport): Unit =
            if report.originalCause() ne sentinel then failure.set(report.originalCause())
    val binding = DedicatedBootstrapBridge.install(factory, new DedicatedBootstrapBridge.Limits(limit, limit, limit))
    val bridge = DedicatedBootstrapBridge.create(new Object(), () => ())
    var primary: Throwable = null
    try
      bridge.startDeferred()
      assertTrue(bridge.awaitFinishedOffGame(Duration.ofSeconds(15)), "actual bridge control deadline")
      if failure.get() != null then throw failure.get()
    catch case c: Throwable => primary = c
    finally
      try
        bridge.requestCancellation(new IllegalStateException("unit cleanup"))
        if bridge.controlThread() != null then join(bridge.controlThread())
      catch case c: Throwable =>
        if primary == null then primary = c else MySqlOwnedSessionLifecycle.suppress(primary, c)
      finally DedicatedBootstrapBridge.uninstall(binding)
    if primary != null then throw primary
