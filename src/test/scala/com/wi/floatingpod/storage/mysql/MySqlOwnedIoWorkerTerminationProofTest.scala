package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

class MySqlOwnedIoWorkerTerminationProofTest:
  private def identity(): MySqlOwnedIoIdentity = MySqlOwnedIoIdentity(UUID.randomUUID(), 1L, 1L)

  private def join(thread: Thread): Unit =
    thread.join(6000L)
    assertFalse(thread.isAlive, "test helper thread did not terminate")

  private def awaitOffMain(adapter: MySqlOwnedIoAdapter): MySqlOwnedIoWorkerTerminationWait =
    val result = new AtomicReference[MySqlOwnedIoWorkerTerminationWait]()
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val waiter = new Thread(() => {
      try result.set(adapter.awaitWorkerTerminationOffMain(5L, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown()
    }, "actual-adapter-worker-termination-waiter")
    waiter.start()
    assertTrue(done.await(6L, TimeUnit.SECONDS), "adapter worker did not terminate within the bounded test wait")
    join(waiter)
    if failure.get() != null then throw failure.get()
    assertNotNull(result.get(), "adapter retirement result missing")
    result.get()

  private def requireProof(result: MySqlOwnedIoWorkerTerminationWait): MySqlOwnedIoWorkerTerminationProof = result match
    case MySqlOwnedIoWorkerTerminationWait.Terminated(proof) => proof
    case MySqlOwnedIoWorkerTerminationWait.TimedOut => fail("adapter worker remained alive after bounded wait")
    case MySqlOwnedIoWorkerTerminationWait.Interrupted(cause) => throw cause

  @Test def workerTerminationProofIsBoundToTheExactAdapterIoAndIdentity(): Unit =
    val exactIdentity = identity()
    val first = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "termination-adapter-first", exactIdentity)
    val second = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "termination-adapter-second", exactIdentity)
    val adapters = mutable.ArrayBuffer(first, second)
    var primary: Throwable = null
    try
      assertThrows(classOf[IllegalArgumentException], () => {
        first.awaitWorkerTerminationOffMain(0L, TimeUnit.MILLISECONDS)
        ()
      }, "initial Main cannot wait for worker termination")
      first.quiesce()
      second.quiesce()
      val proof = requireProof(awaitOffMain(first))
      proof.requireExactIdentity(exactIdentity)
      first.requireOwnWorkerTerminationProof(proof)
      assertThrows(classOf[IllegalArgumentException], () => {
        second.requireOwnWorkerTerminationProof(proof)
        ()
      }, "same identity reference does not transfer proof across distinct adapter/IO instances")

      val copiedIdentity = exactIdentity.copy()
      assertThrows(classOf[IllegalArgumentException], () => proof.requireExactIdentity(copiedIdentity),
        "equal identity values do not replace the exact identity reference")
      val copiedIdentityAdapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(),
        "termination-adapter-copy-identity", copiedIdentity)
      adapters += copiedIdentityAdapter
      copiedIdentityAdapter.quiesce()
      assertThrows(classOf[IllegalArgumentException], () => {
        copiedIdentityAdapter.requireOwnWorkerTerminationProof(proof)
        ()
      }, "the adapter accepts only its own exact IO/identity issuer")
      requireProof(awaitOffMain(copiedIdentityAdapter))
    catch case cause: Throwable => primary = cause
    finally
      adapters.reverseIterator.foreach { adapter =>
        try
          adapter.quiesce()
          requireProof(awaitOffMain(adapter))
        catch
          case cleanup: Throwable =>
            if primary == null then primary = cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      }
    if primary != null then throw primary