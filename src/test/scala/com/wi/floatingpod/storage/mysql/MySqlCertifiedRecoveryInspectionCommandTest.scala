package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真OwnedSerialIo fixed-command/owner/one-shot路径；不称SQL/native actor成功。 */
final class MySqlCertifiedRecoveryInspectionCommandTest:
  private def identity = MySqlOwnedIoIdentity(UUID.randomUUID(), 1L, 1L)
  private def exit(adapter: MySqlOwnedIoAdapter): Unit =
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val helper = new Thread(() => {
      try assertTrue(adapter.awaitExitOffMain(5L, TimeUnit.SECONDS), "actual worker did not exit within bound")
      catch
        case cause: Throwable => failure.set(cause)
      finally done.countDown()
    }, "inspection-test-offmain-exit")
    helper.start()
    var primary: Throwable = null
    try
      assertTrue(done.await(6L, TimeUnit.SECONDS), "offMain exit helper deadline")
      Option(failure.get()).foreach(throw _)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try
        helper.join(6000L)
        assertFalse(helper.isAlive, "offMain helper remained alive")
      catch
        case cleanup: Throwable =>
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
          else throw cleanup

  private def managed(adapter: MySqlOwnedIoAdapter)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try
        adapter.quiesce()
        exit(adapter)
      catch
        case cleanup: Throwable =>
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
          else throw cleanup

  @Test def invalidInspectionInputsPreserveExactAcceptedCompletionsAndOneShotFailure(): Unit =
    val id = identity
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "inspection-command-test", id)
    managed(adapter) {
      val first = IoOperationTag(id.bootId, id.bootGeneration, id.connectionGeneration, UUID.randomUUID())
      val second = first.copy(operationId = UUID.randomUUID())
      val command = MySqlOwnedIoCommand.BootCertifiedInspection(null, null, null)
      assertEquals(IoSubmission.Accepted, adapter.submit(first, command))
      assertEquals(IoSubmission.Accepted, adapter.submit(second, command))
      assertEquals(IoSubmission.RejectedCapacity, adapter.submit(first.copy(operationId = UUID.randomUUID()), MySqlOwnedIoCommand.Renew))
      adapter.quiesce()
      exit(adapter)
      val completed = adapter.drain(2)
      assertEquals(Vector(first, second), completed.map(_.tag))
      assertTrue(completed.head.result.isFailure)
      assertEquals("requirement failed: explicit certified inspection inputs required", completed.head.result.failed.get.getMessage)
      assertEquals("requirement failed: owned MySQL Boot is one-shot", completed(1).result.failed.get.getMessage)
      assertTrue(completed.forall(_.result.failed.get.getStackTrace.nonEmpty))
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.checkpoint.io.stopped)
      assertTrue(adapter.checkpoint.retainedAllocation.isEmpty)
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
    }

  @Test def inspectionIsReadOnlyForRealAcceptedRepositoryEpochClassification(): Unit =
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.BootCertifiedInspection(null, null, null)))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Renew))
    assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.BootFresh(null, null, null, null)))

  @Test def foreignThreadCannotSubmitInspectionOrAcquirePreparationResponsibility(): Unit =
    val id = identity
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "inspection-owner-test", id)
    val original = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val foreign = new Thread(() => {
      try
        adapter.submit(IoOperationTag(id.bootId, id.bootGeneration, id.connectionGeneration, UUID.randomUUID()),
          MySqlOwnedIoCommand.BootCertifiedInspection(null, null, null))
        ()
      catch
        case cause: Throwable => original.set(cause)
      finally done.countDown()
    }, "inspection-foreign-test")
    managed(adapter) {
      foreign.start()
      var primary: Throwable = null
      try
        assertTrue(done.await(5L, TimeUnit.SECONDS))
        assertTrue(original.get().isInstanceOf[IllegalArgumentException])
        assertTrue(original.get().getStackTrace.nonEmpty)
        assertEquals(0, adapter.checkpoint.io.reserved)
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try
          foreign.join(6000L)
          assertFalse(foreign.isAlive)
        catch
          case cleanup: Throwable =>
            if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
            else throw cleanup
    }
