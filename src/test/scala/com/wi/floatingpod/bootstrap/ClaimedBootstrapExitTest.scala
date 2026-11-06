package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真 preBoot OwnedSerialIo 行为；不冒充已 admitted SQL/Fresh/claimed factory 集成。 */
final class ClaimedBootstrapExitTest:
  private def withAdapter(body: MySqlOwnedIoAdapter => Unit): Unit =
    val id = MySqlOwnedIoIdentity(UUID.randomUUID(), 1L, 1L)
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "claimed exit test IO", id)
    var primary: Throwable = null
    try body(adapter)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try adapter.quiesce()
      catch case cause: Throwable =>
        if primary != null then primary.addSuppressed(cause) else primary = cause
      val ended = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val observer = new Thread(() => {
        try
          adapter.awaitWorkerTerminationOffMain(5L, TimeUnit.SECONDS) match
            case MySqlOwnedIoWorkerTerminationWait.Terminated(proof) => adapter.requireOwnWorkerTerminationProof(proof)
            case other => throw new AssertionError("actual worker did not terminate: " + other)
        catch case cause: Throwable => failure.set(cause)
        finally ended.countDown()
      }, "claimed exit test cleanup")
      observer.start()
      assertTrue(ended.await(6L, TimeUnit.SECONDS), "actual bounded helper timeout is failure")
      observer.join(1000L)
      assertFalse(observer.isAlive)
      if failure.get() != null then
        if primary != null then primary.addSuppressed(failure.get()) else primary = failure.get()
      if primary != null then throw primary

  private def observe(exit: ClaimedBootstrapExit): Unit =
    val ended = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val observer = new Thread(() => {
      try
        val start = System.nanoTime()
        while exit.checkpoint.isEmpty && System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5L) do Thread.onSpinWait()
        assertTrue(exit.checkpoint.nonEmpty, "actual exit checkpoint deadline must fail")
      catch case cause: Throwable => failure.set(cause)
      finally ended.countDown()
    }, "claimed exit observation test")
    observer.start()
    try
      assertTrue(ended.await(6L, TimeUnit.SECONDS))
      if failure.get() != null then throw failure.get()
    finally
      observer.join(1000L)
      assertFalse(observer.isAlive)

  @Test def actualWorkerTerminationPreservesOriginalFailureAndAcceptedMailbox(): Unit = withAdapter { adapter =>
    adapter.requireNoPostBootstrapWork()
    val before = adapter.repositorySubmissionEpoch
    val tag = IoOperationTag(adapter.identity.bootId, 1L, 1L, UUID.randomUUID())
    assertEquals(IoSubmission.Accepted, adapter.submit(tag, MySqlOwnedIoCommand.Renew))
    assertEquals(before, adapter.repositorySubmissionEpoch, "non-repository work must not gain a fabricated mutation epoch")
    assertThrows(classOf[IllegalArgumentException], () => adapter.requireNoPostBootstrapWork())
    val cause = new IllegalStateException("original test failure")
    val stack = cause.getStackTrace
    val exit = new ClaimedBootstrapExit(adapter, Thread.currentThread(), Duration.ofSeconds(3L))
    exit.begin(cause)
    observe(exit)
    assertSame(cause, exit.failure.get)
    assertTrue(stack.sameElements(cause.getStackTrace))
    assertTrue(exit.waitObservation.get.isInstanceOf[MySqlOwnedIoWorkerTerminationWait.Terminated])
    val completion = adapter.drain(2)
    assertEquals(1, completion.size)
    assertEquals(tag, completion.head.tag)
    assertTrue(completion.head.result.isFailure) // preBoot Renew cannot invent admission.
    assertThrows(classOf[IllegalArgumentException], () => adapter.requireNoPostBootstrapWork(),
      "consuming a non-writing completion cannot erase original submission history")
    assertTrue(exit.checkpoint.get.io.stopped)
    assertThrows(classOf[IllegalArgumentException], () => exit.begin(cause))
  }

  @Test def foreignThreadCannotQuiesceOriginalOwnerAndSecondaryDoesNotReplacePrimary(): Unit = withAdapter { adapter =>
    val exit = new ClaimedBootstrapExit(adapter, Thread.currentThread(), Duration.ofSeconds(3L))
    val observed = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try exit.begin(new IllegalStateException("foreign"))
      catch case cause: Throwable => observed.set(cause)
    }, "foreign claimed exit caller")
    wrong.start()
    wrong.join(3000L)
    assertFalse(wrong.isAlive)
    assertNotNull(observed.get())
    assertTrue(adapter.checkpoint.io.accepting)
    val original = new IllegalStateException("primary")
    val secondary = new IllegalStateException("secondary")
    exit.begin(original)
    exit.retainFailure(secondary)
    observe(exit)
    assertSame(original, exit.failure.get)
    assertTrue(original.getSuppressed.exists(_ eq secondary))
  }
