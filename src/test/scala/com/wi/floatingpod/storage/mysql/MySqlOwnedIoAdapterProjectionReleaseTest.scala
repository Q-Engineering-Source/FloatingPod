package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlOwnedIoAdapterProjectionReleaseTest:
  private def awaitExitOffMain(adapter: MySqlOwnedIoAdapter): Unit =
    val awaitFailure = new AtomicReference[Throwable]()
    val waiter = new Thread(() =>
      try assertTrue(adapter.awaitExitOffMain(5L, TimeUnit.SECONDS), "owned adapter exit timed out")
      catch case cause: Throwable => awaitFailure.set(cause))
    waiter.start()
    waiter.join(6000L)
    assertFalse(waiter.isAlive, "owned adapter exit waiter leaked")
    if awaitFailure.get() != null then throw awaitFailure.get()

  @Test def unissuedPreBootReleaseIsRejectedBeforeSubmitAndCannotClaimAnOriginalSlot(): Unit =
    val identity = MySqlOwnedIoIdentity(UUID.randomUUID(), 1L, 1L)
    val tag = IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, UUID.randomUUID())
    val adapter = new MySqlOwnedIoAdapter(2, Thread.currentThread(), "mode-projection-unissued-rejection", identity)
    var primary: Throwable = null
    try
      val foreignGeneration = tag.copy(connectionGeneration = tag.connectionGeneration + 1L)
      assertThrows(classOf[IllegalArgumentException], () => { adapter.submitModeProjectionRelease(foreignGeneration, null); () })
      val offMainFailure = new AtomicReference[Throwable]()
      val offMain = new Thread(() =>
        try { adapter.submitModeProjectionRelease(tag, null); () }
        catch case cause: Throwable => offMainFailure.set(cause))
      offMain.start()
      offMain.join(3000L)
      assertFalse(offMain.isAlive, "off-main release validation thread leaked")
      assertTrue(offMainFailure.get().isInstanceOf[IllegalArgumentException], "exact actual Main required")
      assertThrows(classOf[IllegalArgumentException], () => { adapter.submitModeProjectionRelease(tag, null); () })
      // package内故意构造未发行DATA对象用于拒绝测试；没有Boot/session/SQL成功模拟。
      val unissued = new MySqlRetainedPlanDropReceipt(new MySqlRetainedPlanHandle(null, 1L, tag, null))
      assertThrows(classOf[IllegalArgumentException], () => { adapter.submitModeProjectionRelease(tag, unissued); () })
      assertThrows(classOf[IllegalArgumentException], () => { adapter.submitRetainedPlanRelease(tag, unissued); () })
      assertEquals(0, adapter.checkpoint.io.reserved, "unissued release must not consume any worker reservation")
      assertEquals(0, adapter.checkpoint.io.queued)
      adapter.quiesce()
      awaitExitOffMain(adapter)
      assertTrue(adapter.drain(1).isEmpty, "submit rejection must not fabricate a worker completion")
    catch
      case cause: Throwable => primary = cause
    finally
      try
        adapter.quiesce()
        awaitExitOffMain(adapter)
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary
