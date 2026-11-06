package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.sql.SQLDataException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真实canonical sink/ledger行为；无连接/SQL/Boot admission模拟。 */
final class MySqlSharedCanonicalAllocationTest:
  private def profile(retained: Long): SourceLimitProfile =
    SourceLimitProfile.of(1048576L, 65536L, 1048576L, 32L, 256L, retained, 65536L, 65536L)

  @Test def secondReachableRootCannotBorrowAnotherPrivateCap(): Unit =
    val limits = profile(8000L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val first = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    val second = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    var value = Option(MySqlRecoveryCanonicalBytes.withAllocationBudget(first) {
      MySqlRecoveryCanonicalBytes.encode("TEST_SHARED_ROOT", limits)(_.write(new Array[Byte](3200)))
    })
    assertEquals(3200, value.get.length)
    val retained = shared.accountedBytes
    assertTrue(retained >= 3200L)
    assertThrows(classOf[SQLDataException], () => MySqlRecoveryCanonicalBytes.withAllocationBudget(second) {
      MySqlRecoveryCanonicalBytes.encode("TEST_SHARED_ROOT", limits)(_.write(new Array[Byte](3200)))
    })
    assertEquals(retained, shared.accountedBytes, "failed encode must release only its own temporary/output allocations")
    second.closeRootReadBudget()
    assertEquals(retained, shared.accountedBytes)
    // 这里只丢弃测试拥有的payload引用；不声称GC/SQL/正式Main责任已结算。
    value = None
    first.closeRootReadBudget()
    assertEquals(0L, shared.accountedBytes)

  @Test def originalCauseEscapesAndNestedDifferentRootCannotSwitchTheCharge(): Unit =
    val limits = profile(8000L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val first = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    val second = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    val original = new IllegalStateException("controlled encoder failure")
    val observed = assertThrows(classOf[IllegalStateException], () => MySqlRecoveryCanonicalBytes.withAllocationBudget(first) {
      MySqlRecoveryCanonicalBytes.encode("TEST_SHARED_ROOT", limits) { output =>
        output.write(new Array[Byte](100))
        throw original
      }
    })
    assertSame(original, observed)
    assertEquals(0L, shared.accountedBytes)
    assertThrows(classOf[IllegalStateException], () => MySqlRecoveryCanonicalBytes.withAllocationBudget(first) {
      MySqlRecoveryCanonicalBytes.withAllocationBudget(second) { () }
    })
    first.closeRootReadBudget(); second.closeRootReadBudget()

  @Test def exactProfileReferenceAndClosedRootRejectBeforePayloadAllocation(): Unit =
    val limits = profile(8000L)
    val sameDataForeignProfile = profile(8000L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val root = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    assertThrows(classOf[IllegalArgumentException], () => MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.encode("TEST_SHARED_ROOT", sameDataForeignProfile)(_.write(1))
    })
    assertEquals(0L, shared.accountedBytes)
    root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlRecoveryCanonicalBytes.withAllocationBudget(root) { () })

  @Test def measuredInitialPacketLimitKeepsOriginalChargeAndRejectsLargerRow(): Unit =
    val limits = profile(100000L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val root = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    root.reserveRetainedMemory(256L, "test original retained plan")
    val chargedBeforeRead = shared.accountedBytes
    assertThrows(classOf[SQLDataException], () => root.bindInitialMeasuredPacketLimit(0L))
    assertEquals(chargedBeforeRead, shared.accountedBytes)
    root.bindInitialMeasuredPacketLimit(64L)
    val observed = MySqlFormat3ConnectionObservation(
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"), "9.7.2", "fp_test",
      "test@127.0.0.1", "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ",
      "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val scope = MySqlFormat3ObservationScope(
      java.util.UUID.fromString("00000000-0000-0000-0000-000000000002"), 1L, 1L)
    root.initialize("fp_test", observed, scope, 64L)
    assertTrue(shared.accountedBytes > chargedBeforeRead)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException],
      () => root.bindInitialMeasuredPacketLimit(65536L))
    root.beginRow(1, "test_row", 1L)
    assertThrows(classOf[SQLDataException], () => root.beginFixedCell(80, 80, "actual bounded packet"))
    root.endRow("test_row", 1L)
    val child = root.newScopedReadBudget(64L)
    assertTrue(root.sharesAllocationLedger(child))
    child.reserveRetainedMemory(32L, "test actual child allocation")
    val bothHeld = shared.accountedBytes
    child.closeScopedReadBudget()
    assertEquals(bothHeld - 32L, shared.accountedBytes)
    child.closeScopedReadBudget()
    assertEquals(bothHeld - 32L, shared.accountedBytes)
    root.closeRootReadBudget()
    assertEquals(0L, shared.accountedBytes)

  @Test def projectionChildOnSameSharedLedgerStillMustBelongToOriginalRoot(): Unit =
    val limits = profile(100000L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val first = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    val second = MySqlFormat3ReadBudget.rootOnLedger(limits, limits.maxSqlPacketBytes(), shared)
    val observation = MySqlFormat3ConnectionObservation(new java.util.UUID(0L, 1L), "9.7.2", "fp_test",
      "test@127.0.0.1", "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES",
      "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val scope = MySqlFormat3ObservationScope(new java.util.UUID(0L, 2L), 1L, 1L)
    first.initialize("fp_test", observation, scope, 65536L)
    second.initialize("fp_test", observation, scope, 65536L)
    val foreign = second.newScopedReadBudget(65536L)
    val original = first.newScopedReadBudget(65536L)
    val tag = com.wi.floatingpod.server.io.IoOperationTag(scope.bootUuid, 1L, 1L, new java.util.UUID(0L, 3L))
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), scope.bootUuid, 1L, 1L)
    try
      registry.retainRoot(tag, first)
      assertTrue(first.sharesAllocationLedger(foreign))
      assertThrows(classOf[IllegalArgumentException], () => registry.retainScoped(tag, foreign))
      assertEquals(1, registry.pendingCount)
      registry.retainScoped(tag, original)
      registry.release(tag)
      assertTrue(first.isClosed)
      assertTrue(original.isClosed)
      assertFalse(second.isClosed)
      assertFalse(foreign.isClosed)
      assertThrows(classOf[IllegalArgumentException], () => registry.release(tag))
    finally
      foreign.closeScopedReadBudget()
      second.closeRootReadBudget()
      original.closeScopedReadBudget()
      first.closeRootReadBudget()
    assertEquals(0L, shared.accountedBytes)
