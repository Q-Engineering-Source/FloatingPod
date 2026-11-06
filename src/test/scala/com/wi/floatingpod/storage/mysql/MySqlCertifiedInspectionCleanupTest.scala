package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 实际actor消费的既有readonly cleanup规则；actions仅记录协议顺序，不假JDBC成功。 */
final class MySqlCertifiedInspectionCleanupTest:
  private final class Actions(rollbackFailure: Option[Throwable]) extends MySqlFreshPrepareCleanupActions:
    var rollbacks = 0
    var restores = 0
    def rollback(): Unit =
      rollbacks += 1
      rollbackFailure.foreach(throw _)
    def restoreAndObserveIdle(): Unit = restores += 1

  @Test def rollbackFailureNeverRestoresIdleAndPreservesOriginalBodyFailure(): Unit =
    val primary = new java.sql.SQLDataException("body fixture fault")
    val cleanup = new java.sql.SQLException("rollback fixture fault")
    val stack = primary.getStackTrace
    val actions = new Actions(Some(cleanup))
    val result = MySqlFreshPrepareReadOnlyCleanup.finish(actions, Some(primary))
    assertTrue(result.isLeft)
    assertSame(primary, result.swap.toOption.get)
    assertTrue(stack.sameElements(primary.getStackTrace))
    assertEquals(Vector(cleanup), primary.getSuppressed.toVector)
    assertEquals(1, actions.rollbacks)
    assertEquals(0, actions.restores)

  @Test def rollbackFailureWithoutBodyIsTheOriginalPrimary(): Unit =
    val cleanup = new java.sql.SQLException("rollback fixture fault")
    val stack = cleanup.getStackTrace
    val actions = new Actions(Some(cleanup))
    val result = MySqlFreshPrepareReadOnlyCleanup.finish(actions, None)
    assertTrue(result.isLeft)
    assertSame(cleanup, result.swap.toOption.get)
    assertTrue(stack.sameElements(cleanup.getStackTrace))
    assertEquals(1, actions.rollbacks)
    assertEquals(0, actions.restores)

  @Test def successfulRollbackAllowsExactlyOneRestore(): Unit =
    val actions = new Actions(None)
    assertTrue(MySqlFreshPrepareReadOnlyCleanup.finish(actions, None).isRight)
    assertEquals(1, actions.rollbacks)
    assertEquals(1, actions.restores)
