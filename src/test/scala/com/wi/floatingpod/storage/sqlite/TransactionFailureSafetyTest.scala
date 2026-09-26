package com.wi.floatingpod.storage.sqlite

import com.wi.floatingpod.domain.FloatingPodState

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * 直接注入 transaction control 失败，验证 rollback 失败绝不会通过恢复 auto-commit 隐式提交。
 */
class TransactionFailureSafetyTest:
  /** 验证 repository rollback 失败会保留主异常、关闭连接且不持久化先前写入。 */
  @Test
  def repositoryPoisonsConnectionWhenRollbackFails(): Unit = withDatabase { database =>
    val connection = DriverManager.getConnection(databaseUrl(database))
    val control = new RollbackFailingControl(connection, failCommit = false)
    val repository = new SQLiteFloatingPodRepository(connection, control)
    control.events.clear()
    val primaryFailure = new IllegalStateException("repository body failed")
    val state = sampleState("00000000-0000-0000-0000-000000000201")

    val result = repository.transaction {
      repository.insert(state).get
      throw primaryFailure
    }

    assertTrue(result.isFailure)
    assertTrue(result.failed.get eq primaryFailure)
    assertTrue(primaryFailure.getSuppressed.exists(_ eq control.rollbackFailure))
    assertEquals(List("setAutoCommit:false", "rollback", "close"), control.events.toList)
    assertTrue(connection.isClosed)

    val reopened = new SQLiteFloatingPodRepository(databaseUrl(database))
    try assertEquals(None, reopened.findByUuid(state.uuid).get)
    finally reopened.close()
  }

  /** 验证 migration commit 失败且 rollback 失败时不报告成功、不开启 auto-commit 且不留下 schema。 */
  @Test
  def schemaManagerPoisonsConnectionWhenRollbackFails(): Unit = withDatabase { database =>
    val connection = DriverManager.getConnection(databaseUrl(database))
    val control = new RollbackFailingControl(connection, failCommit = true)

    try
      SchemaManager.applyMigrations(connection, control)
      throw new AssertionError("Expected migration failure")
    catch
      case failure: SQLException =>
        assertTrue(failure eq control.commitFailure)
        assertTrue(failure.getSuppressed.exists(_ eq control.rollbackFailure))

    assertEquals(List("setAutoCommit:false", "commit", "rollback", "close"), control.events.toList)
    assertTrue(connection.isClosed)

    val reopened = DriverManager.getConnection(databaseUrl(database))
    try assertFalse(tableExists(reopened, "floating_pod_state"))
    finally reopened.close()
  }

  /** 验证 BEGIN 已成功但 statement close 失败时仍会 rollback，控制边界保持可复用。 */
  @Test
  def immediateReservationCloseFailureRollsBackAndKeepsControlReusable(): Unit =
    val control = new ImmediateControl(rollbackFails = false)
    var markerRan = false
    var poisoned: Throwable = null
    val statement = new CloseFailingImmediateStatement(() => markerRan = true)

    val failure = assertThrows(
      classOf[SQLException],
      () => TransactionBoundary.executeImmediate(control, cause => poisoned = cause, () => statement)("unreachable")
    )

    assertTrue(failure eq statement.closeFailure)
    assertTrue(markerRan)
    assertEquals(1, control.rollbackCalls)
    assertEquals(null, poisoned)
    assertEquals(
      "reused",
      TransactionBoundary.executeImmediate(control, cause => poisoned = cause, () => new ImmediateStatement)("reused")
    )

  /** 验证 BEGIN 后 close 失败且 rollback 同时失败时控制边界会 poison。 */
  @Test
  def immediateReservationCloseFailurePoisonsWhenRollbackFails(): Unit =
    val control = new ImmediateControl(rollbackFails = true)
    var poisoned: Throwable = null
    val statement = new CloseFailingImmediateStatement(() => ())

    val failure = assertThrows(
      classOf[SQLException],
      () => TransactionBoundary.executeImmediate(control, cause => poisoned = cause, () => statement)("unreachable")
    )

    assertTrue(failure eq statement.closeFailure)
    assertTrue(failure.getSuppressed.exists(_ eq control.rollbackFailure))
    assertTrue(poisoned eq failure)

  private def withDatabase(body: Path => Unit): Unit =
    val database = Files.createTempFile("floatingpod-transaction-safety-", ".db")
    try body(database)
    finally Files.deleteIfExists(database)

  private def databaseUrl(database: Path): String = s"jdbc:sqlite:${database.toAbsolutePath}"

  private def tableExists(connection: Connection, tableName: String): Boolean =
    val statement = connection.prepareStatement("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
    try
      statement.setString(1, tableName)
      val resultSet = statement.executeQuery()
      try
        resultSet.next()
        resultSet.getInt(1) == 1
      finally resultSet.close()
    finally statement.close()

  private def sampleState(uuid: String): FloatingPodState =
    FloatingPodState(
      uuid = UUID.fromString(uuid),
      ownerUuid = None,
      stateVersion = 0L,
      worldName = "overworld",
      posX = 0.0D,
      posY = 64.0D,
      posZ = 0.0D,
      yaw = 0.0F,
      pitch = 0.0F,
      isActive = false,
      lastUpdated = 1L
    )

  /** 记录真实 JDBC 连接上的 transaction 调用顺序，并按测试选择注入 commit 或 rollback 失败。 */
  private final class RollbackFailingControl(connection: Connection, failCommit: Boolean) extends TransactionControl:
    val events: ListBuffer[String] = ListBuffer.empty
    val commitFailure = new SQLException("Injected commit failure")
    val rollbackFailure = new SQLException("Injected rollback failure")
    private var closed = false

    override def autoCommit: Boolean = connection.getAutoCommit

    override def setAutoCommit(enabled: Boolean): Unit =
      events += s"setAutoCommit:$enabled"
      connection.setAutoCommit(enabled)

    override def commit(): Unit =
      events += "commit"
      if failCommit then throw commitFailure
      connection.commit()

    override def rollback(): Unit =
      events += "rollback"
      throw rollbackFailure

    override def close(): Unit =
      if !closed then
        closed = true
        events += "close"
        connection.close()

  /** 不依赖 JDBC proxy 的最小 transaction control，专门验证 immediate reservation 的失败分支。 */
  private final class ImmediateControl(rollbackFails: Boolean) extends TransactionControl:
    val rollbackFailure = new SQLException("Injected immediate rollback failure")
    var rollbackCalls = 0

    override def autoCommit: Boolean = true

    override def setAutoCommit(enabled: Boolean): Unit = ()

    override def commit(): Unit = ()

    override def rollback(): Unit =
      rollbackCalls += 1
      if rollbackFails then throw rollbackFailure

    override def close(): Unit = ()

  /** 以显式 seam 模拟 BEGIN 已执行和随后 statement close 失败。 */
  private final class CloseFailingImmediateStatement(onExecute: () => Unit) extends ImmediateControlStatement:
    val closeFailure = new SQLException("Injected BEGIN statement close failure")
    private var closeAttempts = 0

    override def executeBeginImmediate(): Unit = onExecute()

    override def close(): Unit =
      closeAttempts += 1
      if closeAttempts == 1 then throw closeFailure

  /** 表示可正常完成的 immediate reservation statement。 */
  private final class ImmediateStatement extends ImmediateControlStatement:
    override def executeBeginImmediate(): Unit = ()

    override def close(): Unit = ()
