package com.wi.floatingpod.server.io

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.util.Success

class OwnedSerialIoTest:
  private def tag(generation: Long = 1L): IoOperationTag =
    IoOperationTag(UUID.randomUUID(), generation, generation, UUID.randomUUID())

  private def waitFor(latch: CountDownLatch): Unit =
    assertTrue(latch.await(5, TimeUnit.SECONDS), "bounded latch timed out")

  private def exit[C, A](io: OwnedSerialIo[C, A]): Unit =
    val done = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      try assertTrue(io.awaitExitOffMain(5, TimeUnit.SECONDS), "owned worker exit timed out")
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown()
    }, "test-exit-tail")
    waiter.start()
    waitFor(done)
    if failure.get() != null then throw failure.get()

  @Test def serialWorkerReservesCapacityAndPreservesRetiredCompletions(): Unit =
    val main = Thread.currentThread()
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val workerThread = new AtomicReference[Thread]()
    val io = new OwnedSerialIo[Int, Int](2, main, "owned-serial-test")(value => {
      assertFalse(Thread.currentThread() eq main)
      assertFalse(Thread.currentThread().isDaemon)
      val previous = workerThread.getAndSet(Thread.currentThread())
      if previous != null then assertSame(previous, Thread.currentThread())
      if value == 1 then
        entered.countDown()
        assertTrue(release.await(5, TimeUnit.SECONDS), "worker release timed out")
      value
    })
    val oldTag = tag(1)
    val newTag = tag(2)
    try
      assertEquals(IoSubmission.Accepted, io.submit(oldTag, 1))
      waitFor(entered)
      assertEquals(IoSubmission.Accepted, io.submit(newTag, 2))
      assertEquals(IoSubmission.RejectedCapacity, io.submit(tag(), 3))
      assertTrue(io.drain(10).isEmpty)
      io.quiesce()
      assertEquals(IoSubmission.RejectedQuiescing, io.submit(tag(), 4))
      release.countDown()
      exit(io)
      assertEquals(2, io.checkpoint.reserved)
      assertEquals(Vector(IoCompletion(oldTag, Success(1)), IoCompletion(newTag, Success(2))), io.drain(10))
      assertEquals(0, io.checkpoint.reserved)
    finally
      release.countDown()
      io.quiesce()
      exit(io)

  @Test def ordinaryFailureRetainsCauseAndNextWorkExecutes(): Unit =
    val cause = new IllegalStateException("original failure")
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "owned-failure-test")(value => {
      if value == 1 then throw cause
      value
    })
    try
      io.submit(tag(), 1)
      io.submit(tag(), 2)
      io.quiesce()
      exit(io)
      val results = io.drain(2)
      assertSame(cause, results(0).result.failed.get)
      assertTrue(cause.getStackTrace.nonEmpty)
      assertEquals(Success(2), results(1).result)
      assertTrue(io.checkpoint.fatalCause.isEmpty)
    finally
      io.quiesce()
      exit(io)

  @Test def fatalFailureStopsWorkerAndAccountsForUnexecutedAcceptedWork(): Unit =
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val cause = new LinkageError("fatal worker failure")
    val io = new OwnedSerialIo[Int, Int](2, Thread.currentThread(), "owned-fatal-test")(value => {
      entered.countDown()
      assertTrue(release.await(5, TimeUnit.SECONDS))
      throw cause
    })
    try
      io.submit(tag(), 1)
      waitFor(entered)
      io.submit(tag(), 2)
      release.countDown()
      exit(io)
      assertEquals(IoSubmission.RejectedStopped(Some(cause)), io.submit(tag(), 3))
      val results = io.drain(2)
      assertEquals(2, results.size)
      results.foreach(completion => assertSame(cause, completion.result.failed.get))
      assertSame(cause, io.checkpoint.fatalCause.get)
    finally
      release.countDown()
      io.quiesce()
      exit(io)

  @Test def mainCannotBlockInExitTailAndOffMainCannotSubmit(): Unit =
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "owned-boundary-test")(identity)
    try
      assertThrows(classOf[IllegalArgumentException], () => io.awaitExitOffMain(1, TimeUnit.SECONDS))
      val done = new CountDownLatch(1)
      val result = new AtomicReference[Throwable]()
      new Thread(() => {
        try
          io.submit(tag(), 1)
          ()
        catch case cause: Throwable => result.set(cause)
        finally done.countDown()
      }, "wrong-owner-test").start()
      waitFor(done)
      assertTrue(result.get().isInstanceOf[IllegalArgumentException])
    finally
      io.quiesce()
      exit(io)

  @Test def blockedWorkerTimeoutPreservesPendingResponsibility(): Unit =
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val operation = tag()
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "owned-timeout-test")(value => {
      entered.countDown()
      assertTrue(release.await(5, TimeUnit.SECONDS), "worker release timed out")
      value
    })
    try
      assertEquals(IoSubmission.Accepted, io.submit(operation, 7))
      waitFor(entered)
      io.quiesce()
      val observed = new AtomicReference[java.lang.Boolean]()
      val failure = new AtomicReference[Throwable]()
      val done = new CountDownLatch(1)
      new Thread(() => {
        try observed.set(io.awaitExitOffMain(10, TimeUnit.MILLISECONDS))
        catch case cause: Throwable => failure.set(cause)
        finally done.countDown()
      }, "bounded-timeout-tail").start()
      waitFor(done)
      if failure.get() != null then throw failure.get()
      assertEquals(java.lang.Boolean.FALSE, observed.get())
      assertFalse(io.checkpoint.stopped)
      assertTrue(io.checkpoint.running)
      assertEquals(1, io.checkpoint.reserved)
      assertTrue(io.drain(1).isEmpty)
      release.countDown()
      exit(io)
      assertEquals(Vector(IoCompletion(operation, Success(7))), io.drain(1))
      assertEquals(0, io.checkpoint.reserved)
    finally
      release.countDown()
      io.quiesce()
      exit(io)

  @Test def malformedTagIsRejectedBeforeReservingOrExecuting(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => IoOperationTag(null, 1, 1, UUID.randomUUID()))
    assertThrows(classOf[IllegalArgumentException], () => IoOperationTag(UUID.randomUUID(), 0, 1, UUID.randomUUID()))
    val io = new OwnedSerialIo[Int, Int](1, Thread.currentThread(), "owned-tag-test")(identity)
    try
      assertThrows(classOf[IllegalArgumentException], () => io.submit(null, 7))
      assertEquals(0, io.checkpoint.reserved)
      assertTrue(io.drain(1).isEmpty)
    finally
      io.quiesce()
      exit(io)
