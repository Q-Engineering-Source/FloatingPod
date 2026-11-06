package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 实际生产计费入口，DATA不发行actor或native/SQL资格。 */
final class MySqlColdInventoryWorkspaceTest:
  private val profile = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 256L,
    8388608L, 1048576L, 1048576L)
  private def root() = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())

  @Test def twoSuccessiveDataReservationsRefundOnlyActualScratch(): Unit =
    val original = root()
    val initial = original.availableMemoryBytes
    for pass <- 1 to 2 do
      val temporary = MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(original, 150)
      assertTrue(temporary > 0L)
      assertEquals(initial - pass * 512L - temporary, original.availableMemoryBytes)
      original.releaseTemporaryMemory(temporary, "test actual DATA workspace dropped")
      assertEquals(initial - pass * 512L, original.availableMemoryBytes)
    original.closeRootReadBudget()

  @Test def competingOriginalLedgerObligationRejectsBeforeWorkspaceAllocation(): Unit =
    val original = root()
    original.reserveTemporaryMemory(original.availableMemoryBytes - 1024L, "actual competing fixture obligation")
    val before = original.availableMemoryBytes
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(original, 150))
    // 512B保守retained登记仍属原root；没有native/actor/OPEN操作。
    assertEquals(before - 512L, original.availableMemoryBytes)
    original.closeRootReadBudget()

  @Test def ScopedOrClosedBudgetCannotBecomeInventoryWorkflowRoot(): Unit =
    val original = root()
    val child = original.newCanonicalDataScope()
    val before = original.availableMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(child, 150))
    assertEquals(before, original.availableMemoryBytes)
    child.closeScopedReadBudget()
    original.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(original, 150))
