package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.WindowsMarkerIo
import java.io.IOException
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 不打开 Kernel32；验证原 Full27 root 对预登记 native holder 的实际费用与归属。 */
class MySqlDbFirstBootstrapNativeBudgetTest:
  private def full27() = MySqlScriptSchemaInputs.createDbFirstFull27(
    MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))

  @Test def uninitializedNativeHolderReturnsItsExactOriginalReservation(): Unit =
    val inputs = full27()
    val root = inputs.newAllocationRoot(inputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(inputs.sourceLimitProfile.maxRetainedBytes()))
    val before = root.accountedMemoryBytes
    val holder = WindowsMarkerIo.SourceIoHolder.reserveDbFirst(root)
    assertEquals(before + 16384L, root.accountedMemoryBytes)
    val allocation = holder.dbFirstAllocation()
    allocation.reserveTemporary(4096L, "actual bootstrap child")
    assertEquals(before + 20480L, root.accountedMemoryBytes)
    assertThrows(classOf[IOException], () => allocation.releaseTemporary(30000L, "foreign refund"))
    assertEquals(before + 20480L, root.accountedMemoryBytes)
    allocation.releaseTemporary(4096L, "actual bootstrap child retired")
    holder.close()
    assertEquals(before, root.accountedMemoryBytes)
    assertThrows(classOf[IOException], () => allocation.reserveTemporary(1L, "retired native realm"))
    assertThrows(classOf[IOException], () => holder.dbFirstAllocation())
    root.closeRootReadBudget()
    assertEquals(0L, root.accountedMemoryBytes)

  @Test def bootstrapAllocationRejectsAnotherThreadAndClosedOriginalRoot(): Unit =
    val inputs = full27()
    val root = inputs.newAllocationRoot(inputs.budget.packetBytes,
      new MySqlFormat3AllocationLedger(inputs.sourceLimitProfile.maxRetainedBytes()))
    val before = root.accountedMemoryBytes
    val holder = WindowsMarkerIo.SourceIoHolder.reserveDbFirst(root)
    val allocation = holder.dbFirstAllocation()
    val thrown = new AtomicReference[Throwable]()
    val other = new Thread(() =>
      try allocation.reserveTemporary(1L, "foreign worker")
      catch case cause: Throwable => thrown.set(cause))
    other.start(); other.join(5000L)
    assertFalse(other.isAlive)
    assertInstanceOf(classOf[IOException], thrown.get())
    assertEquals(before + 16384L, root.accountedMemoryBytes)
    holder.close()
    root.closeRootReadBudget()
    assertThrows(classOf[IOException], () => allocation.reserveTemporary(1L, "closed root"))
    assertThrows(classOf[IOException], () => WindowsMarkerIo.SourceIoHolder.reserveDbFirst(root))
    assertEquals(0L, root.accountedMemoryBytes)
