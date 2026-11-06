package com.wi.floatingpod.storage.sqlite

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.ObservedUnboundIdentity
import com.wi.floatingpod.storage.ObservedUnboundPod
import com.wi.floatingpod.storage.ObservedUnboundSourceKind
import com.wi.floatingpod.storage.ObservedUnboundUniquenessReceipt
import com.wi.floatingpod.storage.UnboundCommitOutcome
import com.wi.floatingpod.storage.OperatingModeChangeOutcome
import com.wi.floatingpod.storage.PhysicalRecoveryStore
import com.wi.floatingpod.storage.PhysicalRecoveryChunk

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
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * 直接注入 transaction control 失败，验证 rollback 失败绝不会通过恢复 auto-commit 隐式提交。
 */
class TransactionFailureSafetyTest:
  @Test
  def insertPreservesExecutionFailureWhenStatementCloseAlsoFails(): Unit =
    withFaultingRepository { (repository, transport) =>
      transport.arm("executeUpdate")
      val failure = repository.insert(sampleState("00000000-0000-0000-0000-000000000301")).failed.get
      assertTrue(failure eq transport.bodyFailure)
      assertTrue(failure.getSuppressed.exists(_ eq transport.statementCloseFailure))
    }

  @Test
  def queryPreservesReadFailureAcrossResultSetAndStatementCloseFailures(): Unit =
    withFaultingRepository { (repository, transport) =>
      transport.arm("next")
      val failure = repository.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000302")).failed.get
      assertTrue(failure eq transport.bodyFailure)
      assertTrue(failure.getSuppressed.exists(_ eq transport.resultCloseFailure))
      assertTrue(failure.getSuppressed.exists(_ eq transport.statementCloseFailure))
    }

  @Test
  def insertReportsStatementCloseFailureWhenExecutionSucceeded(): Unit =
    withFaultingRepository { (repository, transport) =>
      transport.arm("closeOnly")
      val failure = repository.insert(sampleState("00000000-0000-0000-0000-000000000303")).failed.get
      assertTrue(failure eq transport.statementCloseFailure)
    }

  @Test
  def migrationPreservesSqlFailureWhenStatementCloseAlsoFails(): Unit =
    val raw = DriverManager.getConnection("jdbc:sqlite::memory:")
    val transport = new FaultingJdbcTransport(raw)
    try
      transport.arm("execute")
      val failure = assertThrows(classOf[SQLException], () => SchemaManager.applyMigrations(transport.connection))
      assertTrue(failure eq transport.bodyFailure)
      assertTrue(failure.getSuppressed.exists(_ eq transport.statementCloseFailure))
    finally raw.close()

  @Test
  def immediateCommitPreservesExecutionFailureWhenStatementCloseAlsoFails(): Unit =
    val raw = DriverManager.getConnection("jdbc:sqlite::memory:")
    val transport = new FaultingJdbcTransport(raw)
    var poisoned: Throwable = null
    try
      transport.arm("COMMIT")
      val failure = assertThrows(classOf[SQLException], () =>
        TransactionBoundary.executeImmediate(transport.connection, new JdbcTransactionControl(transport.connection), cause => poisoned = cause)("unreachable")
      )
      assertTrue(failure eq transport.bodyFailure)
      assertTrue(failure.getSuppressed.exists(_ eq transport.statementCloseFailure))
      assertEquals(null, poisoned)
      // rollback 必须已结束手动 BEGIN，否则第二次真实 BEGIN 会失败。
      transport.arm("")
      assertEquals("reused", TransactionBoundary.executeImmediate(transport.connection, new JdbcTransactionControl(transport.connection), cause => poisoned = cause)("reused"))
    finally raw.close()

  private def withFaultingRepository(body: (SQLiteFloatingPodRepository, FaultingJdbcTransport) => Unit): Unit =
    val raw = DriverManager.getConnection("jdbc:sqlite::memory:")
    val transport = new FaultingJdbcTransport(raw)
    val repository = new SQLiteFloatingPodRepository(transport.connection, new JdbcTransactionControl(transport.connection))
    try body(repository, transport)
    finally repository.close()

  /** 仅包装真实 JDBC transport；异常身份来自实际 repository/migration 资源边界。 */
  private final class FaultingJdbcTransport(raw: Connection):
    val bodyFailure = new SQLException("Injected SQL body failure")
    val statementCloseFailure = new SQLException("Injected statement close failure")
    val resultCloseFailure = new SQLException("Injected result set close failure")
    private var failurePoint = ""

    def arm(point: String): Unit = failurePoint = point

    val connection: Connection = proxy(classOf[Connection]) { (method, args) =>
      val value = delegate(raw, method, args)
      method.getName match
        case "prepareStatement" if failurePoint.nonEmpty && failurePoint != "execute" && failurePoint != "COMMIT" => statement(value.asInstanceOf[PreparedStatement], classOf[PreparedStatement])
        case "createStatement" if failurePoint == "execute" || failurePoint == "COMMIT" => statement(value.asInstanceOf[Statement], classOf[Statement])
        case _ => value
    }

    private def statement[T <: Statement](rawStatement: T, interfaceType: Class[T]): T =
      var failClose = failurePoint != "COMMIT"
      proxy(interfaceType) { (method, args) =>
        method.getName match
          case "close" if failClose =>
            rawStatement.close()
            throw statementCloseFailure
          case "execute" if failurePoint == "COMMIT" && args.headOption.contains("COMMIT") =>
            failClose = true
            throw bodyFailure
          case name if name == failurePoint => throw bodyFailure
          case "executeQuery" =>
            val result = delegate(rawStatement, method, args).asInstanceOf[ResultSet]
            if failurePoint == "next" then
              proxy(classOf[ResultSet]) { (resultMethod, resultArgs) =>
                resultMethod.getName match
                  case "next" => throw bodyFailure
                  case "close" =>
                    result.close()
                    throw resultCloseFailure
                  case _ => delegate(result, resultMethod, resultArgs)
              }
            else result
          case _ => delegate(rawStatement, method, args)
      }

    private def delegate(target: AnyRef, method: Method, args: Array[Object]): Object =
      try method.invoke(target, args*)
      catch
        case failure: InvocationTargetException => throw failure.getCause

    private def proxy[T](interfaceType: Class[T])(dispatch: (Method, Array[Object]) => Object): T =
      Proxy.newProxyInstance(interfaceType.getClassLoader, Array[Class[?]](interfaceType), new InvocationHandler:
        override def invoke(proxy: Any, method: Method, args: Array[Object]): Object =
          dispatch(method, Option(args).getOrElse(Array.empty[Object]))
      ).asInstanceOf[T]

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

  @Test
  def v005FailureRollsBackModeBackfillAndRestoresV003NullDimensionGuard(): Unit = withDatabase { database =>
    val connection = DriverManager.getConnection(databaseUrl(database))
    val control = new V005FailingControl(connection)
    try
      val failure = assertThrows(classOf[SQLException], () => SchemaManager.applyMigrations(connection, control))
      assertTrue(failure eq control.commitFailure)
      assertEquals(4, currentSchemaVersion(connection))
      assertFalse(columnExists(connection, "floating_pod_state", "operating_mode"))
      assertTrue(triggerExists(connection, "trg_floating_pod_state_reject_active_null_dimension"))
      assertTrue(triggerExists(connection, "trg_floating_pod_state_reject_active_insert"))
      assertTrue(triggerExists(connection, "trg_floating_pod_state_reject_active_delete"))
      assertEquals(1, countRows(connection, "SELECT COUNT(*) FROM floating_pod_state WHERE uuid = X'000000000000000000000000000000A1' AND is_active = 1 AND dimension_id IS NULL"))
      assertThrows(classOf[SQLException], () => {
        val statement = connection.createStatement()
        try statement.executeUpdate("UPDATE floating_pod_state SET world_name = 'blocked' WHERE uuid = X'000000000000000000000000000000A1'")
        finally statement.close()
      })
    finally
      connection.close()
  }

  @Test
  def v006FailureRollsBackLocatorRebuildAndRetainsEveryV005ParentChildRow(): Unit = withDatabase { database =>
    val connection = DriverManager.getConnection(databaseUrl(database))
    val foreignKeys = connection.createStatement()
    try foreignKeys.execute("PRAGMA foreign_keys = ON")
    finally foreignKeys.close()
    val control = new V006FailingControl(connection)
    try
      val failure = assertThrows(classOf[SQLException], () => SchemaManager.applyMigrations(connection, control))
      assertTrue(failure eq control.commitFailure)
      assertEquals(5, currentSchemaVersion(connection))
      assertFalse(tableExists(connection, "physical_recovery_intent"))
      assertFalse(tableExists(connection, "physical_recovery_locator_v5"))
      assertEquals(1, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator WHERE pod_uuid = X'000000000000000000000000000000B1' AND locator_state = 'CREATE_PREPARED' AND reason_code = 'legacy-preserved' AND domain_version = 7 AND target_dimension_id = -3"))
      assertEquals(2, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator_chunk WHERE pod_uuid = X'000000000000000000000000000000B1'"))
      assertEquals("NO ACTION", foreignKeyDeleteAction(connection, "physical_recovery_locator_chunk"))
      assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM pragma_foreign_key_check"))

      SchemaManager.applyMigrations(connection)

      assertEquals(6, currentSchemaVersion(connection))
      assertTrue(tableExists(connection, "physical_recovery_intent"))
      assertEquals(1, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator WHERE pod_uuid = X'000000000000000000000000000000B1' AND locator_state = 'CREATE_PREPARED' AND reason_code = 'legacy-preserved' AND domain_version = 7 AND target_dimension_id = -3"))
      assertEquals(2, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator_chunk WHERE pod_uuid = X'000000000000000000000000000000B1'"))
      assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM pragma_foreign_key_check"))
    finally connection.close()
  }

  @Test
  def resolvesCommitAcknowledgementLossByExactModeCasReadbackAndReopensTheRepository(): Unit = withDatabase { database =>
    val url = databaseUrl(database)
    val connection = DriverManager.getConnection(url)
    val control = new CommitThenReportFailureControl(connection)
    val repository = new SQLiteFloatingPodRepository(url, connection, control)
    val podUuid = UUID.fromString("00000000-0000-0000-0000-0000000002A1")
    val ownerUuid = UUID.fromString("00000000-0000-0000-0000-0000000002A2")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-0000000002A3")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-0000000002A4")
    val inactive = FloatingPodState(podUuid, Some(ownerUuid), 0L, "overworld", 200D, 64D, -104D, 0F, 0F, false, 1L, None)
    val chunks = (for chunkX <- 10 to 14; chunkZ <- -9 to -5 yield PhysicalRecoveryChunk(7, chunkX, chunkZ)).toSet
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      val binding = repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
      repository.insert(inactive).get
      repository.prepareCreateLocator(podUuid, 0L, attemptUuid, 7, chunks).get
      val active = repository.activatePreparedIfAdmissible(inactive, ownerUuid, 0L, 7, 256, attemptUuid).get.asInstanceOf[com.wi.floatingpod.storage.AdmissionOutcome.Accepted].state

      control.failNextCommitAfterPersisting = true
      val result = (repository: PhysicalRecoveryStore).setOperatingModeTrackedIfOwned(
        binding,
        podUuid,
        ownerUuid,
        active.stateVersion,
        attemptUuid,
        PodOperatingMode.WorldIdle,
        FloatingPodPose("overworld", 203D, 65D, -107D, 45F, 12F, 7)
      ).get

      result match
        case OperatingModeChangeOutcome.Changed(state) =>
          assertEquals(2L, state.stateVersion)
          assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)
        case other => throw new AssertionError(s"Expected exact readback to confirm committed mode, got $other")
      assertEquals(Some(PodOperatingMode.WorldIdle), repository.findByUuid(podUuid).get.map(_.operatingMode))
      assertEquals(Some(2L), repository.findPhysicalRecoveryLocator(podUuid).get.flatMap(_.domainVersion))
    finally repository.close()
  }

  @Test
  def resolvesUnboundCommitAcknowledgementLossByExactIntentReadbackWithoutRetrying(): Unit = withDatabase { database =>
    val url = databaseUrl(database)
    val connection = DriverManager.getConnection(url)
    val control = new CommitThenReportFailureControl(connection)
    val repository = new SQLiteFloatingPodRepository(url, connection, control)
    val podUuid = UUID.fromString("00000000-0000-0000-0000-0000000002B1")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-0000000002B2")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-0000000002B3")
    val generation = UUID.fromString("00000000-0000-0000-0000-0000000002B4")
    val observation = ObservedUnboundPod(
      podUuid,
      ObservedUnboundIdentity(ObservedUnboundSourceKind.FreshArtifact, generation, "ack-loss-source"),
      FloatingPodPose("overworld", 200D, 64D, -104D, 45F, 12F, 7)
    )
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      val binding = repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
      val store = repository: PhysicalRecoveryStore
      store.prepareObservedUnbound(observation, binding, attemptUuid, ObservedUnboundUniquenessReceipt(bindingUuid, observation)).get

      control.failNextCommitAfterPersisting = true
      val result = store.commitObservedUnbound(binding, podUuid, attemptUuid, 0L, observation.pose, 256).get

      result match
        case UnboundCommitOutcome.Committed(state) =>
          assertEquals(podUuid, state.uuid)
          assertEquals(None, state.ownerUuid)
          assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)
          assertTrue(state.isActive)
          assertEquals(1L, state.stateVersion)
        case other => throw new AssertionError(s"Expected exact unbound commit readback after acknowledgement loss, got $other")
      assertEquals(Some(1L), repository.findByUuid(podUuid).get.map(_.stateVersion))
      assertEquals("ROW_ACTIVE", repository.findPhysicalRecoveryLocator(podUuid).get.get.state.databaseValue)
      assertEquals("COMMITTED", intentPhase(databaseUrlToPath(url), podUuid))
      assertFalse(control.failNextCommitAfterPersisting)
    finally repository.close()
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

  private def databaseUrlToPath(url: String): String = url.stripPrefix("jdbc:sqlite:")

  private def foreignKeyDeleteAction(connection: Connection, tableName: String): String =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(s"PRAGMA foreign_key_list($tableName)")
      try
        assertTrue(resultSet.next())
        resultSet.getString("on_delete")
      finally resultSet.close()
    finally statement.close()

  private def intentPhase(database: String, podUuid: UUID): String =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:$database")
    try
      val statement = connection.prepareStatement("SELECT phase FROM physical_recovery_intent WHERE pod_uuid = ?")
      try
        statement.setBytes(1, java.nio.ByteBuffer.allocate(16).putLong(podUuid.getMostSignificantBits).putLong(podUuid.getLeastSignificantBits).array())
        val resultSet = statement.executeQuery()
        try
          assertTrue(resultSet.next())
          resultSet.getString(1)
        finally resultSet.close()
      finally statement.close()
    finally connection.close()

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

  private def currentSchemaVersion(connection: Connection): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT MAX(version) FROM schema_version")
      try
        assertTrue(resultSet.next())
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def columnExists(connection: Connection, tableName: String, columnName: String): Boolean =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(s"PRAGMA table_info($tableName)")
      try
        var found = false
        while resultSet.next() do if resultSet.getString("name") == columnName then found = true
        found
      finally resultSet.close()
    finally statement.close()

  private def triggerExists(connection: Connection, triggerName: String): Boolean =
    val statement = connection.prepareStatement("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?")
    try
      statement.setString(1, triggerName)
      val resultSet = statement.executeQuery()
      try
        assertTrue(resultSet.next())
        resultSet.getInt(1) == 1
      finally resultSet.close()
    finally statement.close()

  private def countRows(connection: Connection, sql: String): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(sql)
      try
        assertTrue(resultSet.next())
        resultSet.getInt(1)
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

  private final class V005FailingControl(connection: Connection) extends TransactionControl:
    val commitFailure = new SQLException("Injected V005 commit failure")
    private var commitCount = 0

    override def autoCommit: Boolean = connection.getAutoCommit
    override def setAutoCommit(enabled: Boolean): Unit = connection.setAutoCommit(enabled)
    override def commit(): Unit =
      commitCount += 1
      if commitCount == 2 then
        val statement = connection.createStatement()
        try
          statement.executeUpdate("INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id) VALUES (X'000000000000000000000000000000A1', NULL, 0, 'legacy', 0, 64, 0, 0, 0, 1, 1, NULL)")
        finally statement.close()
      if commitCount == 5 then throw commitFailure
      connection.commit()
    override def rollback(): Unit = connection.rollback()
    override def close(): Unit = connection.close()

  private final class V006FailingControl(connection: Connection) extends TransactionControl:
    val commitFailure = new SQLException("Injected V006 commit failure")
    private var commitCount = 0

    override def autoCommit: Boolean = connection.getAutoCommit
    override def setAutoCommit(enabled: Boolean): Unit = connection.setAutoCommit(enabled)
    override def commit(): Unit =
      commitCount += 1
      if commitCount == 4 then
        val statement = connection.createStatement()
        try
          statement.executeUpdate("INSERT INTO physical_recovery_locator (pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id) VALUES (X'000000000000000000000000000000B1', X'000000000000000000000000000000B2', 'CREATE_PREPARED', 'legacy-preserved', 7, -3)")
          statement.executeUpdate("INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z) VALUES (X'000000000000000000000000000000B1', -3, 12, -8)")
          statement.executeUpdate("INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z) VALUES (X'000000000000000000000000000000B1', -3, 13, -8)")
        finally statement.close()
      if commitCount == 6 then throw commitFailure
      connection.commit()
    override def rollback(): Unit = connection.rollback()
    override def close(): Unit = connection.close()

  private final class CommitThenReportFailureControl(connection: Connection) extends TransactionControl:
    var failNextCommitAfterPersisting = false
    private var closed = false

    override def autoCommit: Boolean = connection.getAutoCommit
    override def setAutoCommit(enabled: Boolean): Unit = connection.setAutoCommit(enabled)
    override def commit(): Unit =
      if connection.getAutoCommit then
        val statement = connection.createStatement()
        try statement.execute("COMMIT")
        finally statement.close()
      else connection.commit()
      if failNextCommitAfterPersisting then
        failNextCommitAfterPersisting = false
        throw new SQLException("Injected lost commit acknowledgement after SQLite persisted the transaction")
    override def rollback(): Unit =
      if connection.getAutoCommit then
        val statement = connection.createStatement()
        try statement.execute("ROLLBACK")
        finally statement.close()
      else connection.rollback()
    override def close(): Unit =
      if !closed then
        closed = true
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
