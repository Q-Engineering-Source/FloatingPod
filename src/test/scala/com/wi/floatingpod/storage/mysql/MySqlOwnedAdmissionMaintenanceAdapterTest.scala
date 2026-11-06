package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/** 实际 adapter/worker 的 pre-open failure；绝不 fake JDBC/native 成功。 */
class MySqlOwnedAdmissionMaintenanceAdapterTest:
  private def tail(adapter: MySqlOwnedIoAdapter): Unit =
    val failure = new AtomicReference[Throwable]()
    val helper = new Thread(() => {
      try assertTrue(adapter.awaitExitOffMain(5, TimeUnit.SECONDS), "actual adapter worker exit timed out")
      catch case cause: Throwable => failure.set(cause)
      ()
    }, "maintenance-adapter-off-main-tail")
    helper.start()
    helper.join(6000L)
    assertFalse(helper.isAlive, "off-main helper leaked")
    if failure.get() != null then throw failure.get()

  private def protectedRun(adapter: MySqlOwnedIoAdapter)(body: => Unit): Unit =
    var primary: Throwable = null
    try body
    catch case cause: Throwable => primary = cause
    finally
      try adapter.quiesce()
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else if primary ne cleanup then primary.addSuppressed(cleanup)
      try tail(adapter)
      catch case cleanup: Throwable =>
        if primary == null then primary = cleanup else if primary ne cleanup then primary.addSuppressed(cleanup)
    if primary != null then throw primary

  @Test def explicitlyEnabledMaintenanceStaysDisabledBeforeBootAndBootFailureNeverProducesProof(): Unit =
    val identity = MySqlOwnedIoIdentity(UUID.randomUUID(), 2L, 3L)
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "enabled-admission-pre-open",
      identity, enableAdmissionMaintenance = true)
    protectedRun(adapter) {
      assertTrue(adapter.pollAdmissionMaintenance.latest.isEmpty)
      assertFalse(adapter.checkpoint.io.maintenanceScheduled)
      val operation = IoOperationTag(identity.bootId, 2L, 3L, UUID.randomUUID())
      assertEquals(IoSubmission.Accepted, adapter.submit(operation, MySqlOwnedIoCommand.Boot(null, null, null)))
      adapter.quiesce()
      tail(adapter)
      val completed = adapter.drain(1)
      assertEquals(operation, completed.head.tag)
      val original = completed.head.result.failed.get
      assertTrue(original.isInstanceOf[IllegalArgumentException])
      assertSame(original, adapter.pollAdmissionMaintenance.failure.get)
      assertTrue(adapter.pollAdmissionMaintenance.latest.isEmpty)
      assertFalse(adapter.pollAdmissionMaintenance.isCurrent)
      assertFalse(adapter.checkpoint.io.maintenanceScheduled)
      assertFalse(adapter.checkpoint.io.maintenanceRunning)
      assertTrue(adapter.checkpoint.io.fatalCause.isEmpty, "Boot work failure does not pretend maintenance ran")
      assertEquals(Some(MySqlOwnedIoResourceObservation.NoVerifiedSessionOwned), adapter.checkpoint.resources)
      assertEquals(0, adapter.checkpoint.io.reserved)
    }

  @Test def defaultMaintenanceIsNotProductionWiringAndForeignActorCannotArmOrSubmitThroughPoll(): Unit =
    val identity = MySqlOwnedIoIdentity(UUID.randomUUID(), 2L, 3L)
    val adapter = new MySqlOwnedIoAdapter(1, Thread.currentThread(), "default-maintenance-disabled", identity)
    val wrongFailure = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try
        assertTrue(adapter.pollAdmissionMaintenance.latest.isEmpty)
        val operation = IoOperationTag(identity.bootId, 2L, 3L, UUID.randomUUID())
        assertThrows(classOf[IllegalArgumentException], () => adapter.submit(operation, MySqlOwnedIoCommand.Renew))
        assertFalse(adapter.checkpoint.io.maintenanceScheduled)
      catch case cause: Throwable => wrongFailure.set(cause)
      ()
    }, "wrong-admission-main")
    protectedRun(adapter) {
      var primary: Throwable = null
      try
        wrong.start()
        wrong.join(5000L)
        assertFalse(wrong.isAlive, "wrong actor timed out")
        if wrongFailure.get() != null then throw wrongFailure.get()
      catch case cause: Throwable => primary = cause
      finally
        try
          if wrong.getState != Thread.State.NEW then wrong.join(5000L)
          assertFalse(wrong.isAlive, "wrong actor leaked")
        catch case cleanup: Throwable =>
          if primary == null then primary = cleanup else if primary ne cleanup then primary.addSuppressed(cleanup)
      if primary != null then throw primary
      assertEquals(0, adapter.checkpoint.io.reserved)
      assertTrue(adapter.pollAdmissionMaintenance.latest.isEmpty)
      assertTrue(adapter.pollAdmissionMaintenance.failure.isEmpty)
    }
