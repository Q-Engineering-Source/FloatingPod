package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.*
import com.wi.floatingpod.server.io.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真实 adapter/OwnedSerialIo accepted/capacity/tag/exit；负输入不 OPEN，绝非 Boot admission 正例。 */
class MainBootProjectionEpochTest:
  private def waitOffMain(adapter: MySqlOwnedIoAdapter): Unit =
    val failure=new AtomicReference[Throwable]()
    val observer=new Thread(() =>
      try assertTrue(adapter.awaitExitOffMain(3L,TimeUnit.SECONDS),"owned worker exit deadline expired")
      catch
        case cause: Throwable => failure.set(cause)
      (),"boot-epoch-exit-observer")
    var primary: Throwable=null
    try
      observer.start(); observer.join(5000L)
      assertFalse(observer.isAlive,"exit observer deadline expired")
      if failure.get()!=null then throw failure.get()
    catch
      case cause: Throwable => primary=cause
    finally
      if observer.isAlive then observer.interrupt()
      try
        observer.join(5000L)
        assertFalse(observer.isAlive,"exit observer cleanup deadline expired")
      catch
        case cleanup: Throwable =>
          if primary==null then primary=cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    if primary!=null then throw primary
  private def owned(body: (MySqlOwnedIoAdapter, IoOperationTag) => Unit): Unit =
    val identity=MySqlOwnedIoIdentity(UUID.randomUUID(),1L,1L)
    val adapter=new MySqlOwnedIoAdapter(2,Thread.currentThread(),"boot-epoch-actual-worker",identity)
    val tag=IoOperationTag(identity.bootId,1L,1L,UUID.randomUUID())
    var primary: Throwable=null
    try body(adapter,tag)
    catch
      case cause: Throwable => primary=cause
    finally
      try adapter.quiesce()
      catch
        case cleanup: Throwable =>
          if primary==null then primary=cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
      try waitOffMain(adapter)
      catch
        case cleanup: Throwable =>
          if primary==null then primary=cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    if primary!=null then throw primary
  @Test def acceptedRenewAndCapacityRejectKeepBootReadEpochAndExactReservations(): Unit = owned { (adapter,first) =>
    val second=first.copy(operationId=UUID.randomUUID())
    assertEquals(0L,adapter.repositorySubmissionEpoch)
    assertEquals(IoSubmission.Accepted,adapter.submit(first,MySqlOwnedIoCommand.Renew))
    assertEquals(IoSubmission.Accepted,adapter.submit(second,MySqlOwnedIoCommand.Renew))
    assertEquals(IoSubmission.RejectedCapacity,adapter.submit(first.copy(operationId=UUID.randomUUID()),MySqlOwnedIoCommand.Renew))
    assertEquals(0L,adapter.repositorySubmissionEpoch)
    assertEquals(2,adapter.checkpoint.io.reserved)
    adapter.quiesce(); waitOffMain(adapter)
    val completions=adapter.drain(2)
    assertSame(first,completions.head.tag); assertSame(second,completions(1).tag)
    assertTrue(completions.forall(_.result.isFailure))
    assertEquals(0L,adapter.repositorySubmissionEpoch)
    assertEquals(0,adapter.checkpoint.io.reserved)
    assertTrue(adapter.checkpoint.io.stopped)
    assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned),adapter.checkpoint.resources)
  }
  @Test def acceptedPotentialMutationRetiresInputEvenAfterFailureCompletionsDrain(): Unit = owned { (adapter,first) =>
    val lifetime=new MainBootReadLifetime(Thread.currentThread(),adapter.repositorySubmissionEpoch)
    val second=first.copy(operationId=UUID.randomUUID())
    // 实际 worker 在 explicit-input 守卫拒绝；不创建任何 JDBC/session/native资源。
    val invalid=MySqlOwnedIoCommand.Boot(null,null,null)
    assertEquals(IoSubmission.Accepted,adapter.submit(first,invalid))
    assertEquals(1L,adapter.repositorySubmissionEpoch)
    val retired=assertThrows(classOf[IllegalStateException], () => lifetime.requireCurrent(adapter.repositorySubmissionEpoch))
    assertEquals(IoSubmission.Accepted,adapter.submit(second,invalid))
    assertEquals(2L,adapter.repositorySubmissionEpoch)
    assertEquals(IoSubmission.RejectedCapacity,adapter.submit(first.copy(operationId=UUID.randomUUID()),invalid))
    assertEquals(2L,adapter.repositorySubmissionEpoch)
    adapter.quiesce(); waitOffMain(adapter)
    val completions=adapter.drain(2)
    assertEquals(2,completions.size)
    assertSame(first,completions.head.tag); assertSame(second,completions(1).tag)
    assertTrue(completions.forall(_.result.isFailure))
    assertTrue(completions.head.result.failed.get.getStackTrace.nonEmpty)
    assertEquals(0,adapter.checkpoint.io.reserved)
    assertSame(retired,assertThrows(classOf[IllegalStateException], () => lifetime.requireCurrent(adapter.repositorySubmissionEpoch)))
    assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned),adapter.checkpoint.resources)
  }

  @Test def ordinaryOwnedDropDoesNotReborrowRetiredEpochAndIsOneShot(): Unit =
    val lifetime = new MainBootReadLifetime(Thread.currentThread(), 7L)
    // 未调用旧 current 入口，原 owner 可以结束已完成读者；不重新取得 epoch 资格。
    lifetime.dropOrdinaryOwnedInput()
    assertTrue(lifetime.retainedFailure.isEmpty)
    assertThrows(classOf[IllegalArgumentException], () => lifetime.requireCurrent(8L))
    assertThrows(classOf[IllegalArgumentException], () => lifetime.dropOrdinaryOwnedInput())

  @Test def ordinaryOwnedDropPreservesActualLatchedFailureIdentity(): Unit =
    val lifetime = new MainBootReadLifetime(Thread.currentThread(), 7L)
    val original = assertThrows(classOf[IllegalStateException], () => lifetime.requireCurrent(8L))
    assertSame(original, assertThrows(classOf[IllegalStateException], () => lifetime.dropOrdinaryOwnedInput()))
    assertSame(original, lifetime.retainedFailure.get)
    assertSame(original, assertThrows(classOf[IllegalStateException], () => lifetime.requireCurrent(7L)))
