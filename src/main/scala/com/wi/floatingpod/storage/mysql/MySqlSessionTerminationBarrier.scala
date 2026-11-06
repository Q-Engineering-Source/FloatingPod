package com.wi.floatingpod.storage.mysql

import java.lang.ref.WeakReference
import java.sql.{Connection, ResultSet, SQLNonTransientConnectionException}
import java.time.Duration
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

enum MySqlSessionBarrierAcquisition:
  case Held(value: HeldSessionBarrier)
  case Deferred

/** 不含 JDBC 对象；数字 CID 不是物理 session 终止或 executor 退休证明。 */
final case class MySqlSessionObservation(
    connectionId: Option[Long], serverUuid: UUID, schema: String, effectiveUser: String,
    bootUuid: UUID, bootGeneration: Long, connectionGeneration: Long,
    jdbcIsClosed: Boolean, barrierAttempted: Boolean, barrierHeld: Boolean
):
  override def toString: String = "MySqlSessionObservation[redacted; not session-termination proof]"

/** 仅本 owner 观察的调用事实；物理 close / isClosed 不清除可能仍在服务端持有锁的责任。 */
final case class MySqlSessionBarrierCloseObservation(
    attempted: Boolean, getLockCallAttempted: Boolean, busyReplyConfirmed: Boolean,
    heldReplyConfirmed: Boolean, localCapabilityRevoked: Boolean,
    actualConnectionId: Option[Long], namespace: Option[String], failure: Option[Throwable]
):
  def mayRetainServerLock: Boolean = getLockCallAttempted && !busyReplyConfirmed
  override def toString: String = "MySqlSessionBarrierCloseObservation[redacted; no server-termination proof]"

/** 只由实际 SQL 成功核验签发；不向调用方返回 Connection 或 SQL callback。 */
final class HeldSessionBarrier private[mysql] (
    private val issuer: MySqlSessionTerminationBarrier,
    val connectionGenerationId: UUID,
    val actualConnectionId: Long,
    val namespace: MySqlSessionBarrierNamespace
):
  private[mysql] def readmit(verified: VerifiedMySqlConnection, expectation: MySqlSchemaAuditExpectation, scope: MySqlAuditBootScope): Unit =
    issuer.readmit(this, verified, expectation, scope)
  override def toString: String = "HeldSessionBarrier[redacted; not settlement proof]"

/** 仅固定 SQL 的窄内部 seam；协议状态机测试不冒充 MySQL 行为验收。 */
private[mysql] trait MySqlSessionBarrierQueries:
  def connectionId(): Long
  def acquire(name: String, timeoutSeconds: Long): Option[Int]
  def owner(name: String): (Long, Option[Long])

/** Weak identity keys 不使用 JDBC equals/hashCode；登记值不反向持有 Connection。 */
private[mysql] final class MySqlSessionBarrierOnce:
  private var attempted = false
  def claim(): Unit = synchronized {
    if attempted then throw new SQLNonTransientConnectionException("physical MySQL session already attempted its termination barrier")
    attempted = true
  }

private[mysql] object MySqlSessionBarrierRegistry:
  private val sessions = ArrayBuffer.empty[(WeakReference[Connection], MySqlSessionBarrierOnce)]
  def forConnection(connection: Connection): MySqlSessionBarrierOnce = synchronized {
    sessions.filterInPlace(_._1.get() != null)
    sessions.find(pair => pair._1.get() eq connection) match
      case Some(pair) => pair._2
      case None =>
        val once = new MySqlSessionBarrierOnce
        sessions += ((new WeakReference(connection), once))
        once
  }

/** 无 release/reset 路径；同一次物理 session 的 GET_LOCK 永久只允许一次。 */
private[mysql] final class MySqlSessionBarrierProtocol(
    queries: MySqlSessionBarrierQueries, once: MySqlSessionBarrierOnce, ownerThread: Thread
):
  require(queries != null && once != null && ownerThread != null, "actual barrier dependencies required")
  private var attempted = false
  private var closed = false
  private var held = false
  private var getLockCallAttempted = false
  private var busyReplyConfirmed = false
  private var heldReplyConfirmed = false
  private var cid = 0L
  private var name: String = null
  private var failure: Throwable = null

  private def requireOwner(): Unit =
    require(Thread.currentThread() eq ownerThread, "session barrier requires exact owned IO thread")
  def isAttempted: Boolean = attempted
  def isHeld: Boolean = held && failure == null && !closed
  def capturedCid: Long = cid

  def acquire(namespace: MySqlSessionBarrierNamespace, wait: Duration): Boolean =
    requireOwner()
    require(namespace != null && wait != null && !wait.isNegative && wait.compareTo(Duration.ofSeconds(30)) <= 0,
      "session barrier wait must be explicit and bounded within 0..30 seconds")
    require(wait.getNano == 0, "session barrier wait must use whole seconds")
    if failure != null then throw failure
    if closed || attempted then reject("session barrier acquire is one-shot")
    // Claim before any query: pre-CID/query/cleanup failure cannot enable another wrapper to retry GET_LOCK.
    once.claim()
    attempted = true
    name = namespace.lockName
    try
      val before = queries.connectionId()
      if before <= 0 then reject("session barrier initial CID is unavailable")
      cid = before
      getLockCallAttempted = true
      val acquired = queries.acquire(name, wait.getSeconds)
      acquired match
        case Some(0) => busyReplyConfirmed = true; false
        case Some(1) =>
          val (after, actualOwner) = queries.owner(name)
          if after != before || actualOwner != Some(before) then reject("session barrier acquired response differs from actual CID/owner")
          cid = before
          held = true
          heldReplyConfirmed = true
          true
        case _ => reject("session barrier acquisition is unknown or malformed")
    catch
      case primary: Throwable => invalidate(primary); throw primary

  def readmit(): Unit =
    requireOwner()
    if failure != null then throw failure
    if !isHeld then reject("session barrier is not held for transaction admission")
    try
      val (actualCid, actualOwner) = queries.owner(name)
      if actualCid != cid || actualOwner != Some(cid) then reject("session barrier lost actual CID/lock ownership")
    catch
      case primary: Throwable => invalidate(primary); throw primary

  def invalidate(primary: Throwable): Unit =
    requireOwner()
    require(primary != null, "original session failure required")
    held = false
    if failure == null then failure = primary
    else if failure ne primary then MySqlFencedTransactionSupport.addSuppressed(failure, primary)

  /** Closing permanently revokes local use, but is not proof that the server session ended. */
  def beginPhysicalClose(): Unit =
    requireOwner()
    closed = true
    held = false

  def closeObservation: MySqlSessionBarrierCloseObservation =
    requireOwner()
    MySqlSessionBarrierCloseObservation(attempted, getLockCallAttempted, busyReplyConfirmed,
      heldReplyConfirmed, closed || failure != null, Option.when(cid > 0)(cid), Option(name), Option(failure))

  private def reject(message: String): Nothing = throw new SQLNonTransientConnectionException(message)

private[mysql] final class MySqlJdbcSessionBarrierQueries(connection: Connection) extends MySqlSessionBarrierQueries:
  private def requiredLong(result: ResultSet, column: Int): Long =
    val value = result.getLong(column)
    if result.wasNull() || value <= 0 then throw new SQLNonTransientConnectionException("session barrier CID is unavailable")
    value
  private def query[A](sql: String)(bind: java.sql.PreparedStatement => Unit)(read: ResultSet => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
      // All calls are bounded independently of the driver's finite socket timeout.
      statement.setQueryTimeout(35)
      bind(statement)
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        if !result.next() then throw new SQLNonTransientConnectionException("session barrier query returned no row")
        val value = read(result)
        if result.next() then throw new SQLNonTransientConnectionException("session barrier query returned duplicate rows")
        value
      }
    }
  def connectionId(): Long = query("SELECT CONNECTION_ID()")(_ => ()) { result => requiredLong(result, 1) }
  def acquire(name: String, timeoutSeconds: Long): Option[Int] =
    query("SELECT GET_LOCK(?, ?)") { statement =>
      statement.setString(1, name); statement.setLong(2, timeoutSeconds)
    } { result =>
      val value = result.getInt(1)
      if result.wasNull() then None else Some(value)
    }
  def owner(name: String): (Long, Option[Long]) =
    query("SELECT CONNECTION_ID(), IS_USED_LOCK(?)")(_.setString(1, name)) { result =>
      val cid = requiredLong(result, 1)
      val owner = result.getLong(2)
      val optional = if result.wasNull() then None else Some(owner)
      (cid, optional)
    }

private[mysql] final class MySqlSessionTerminationBarrier(verified: VerifiedMySqlConnection):
  private val owner = Thread.currentThread()
  private val generation = UUID.randomUUID()
  private val protocol = new MySqlSessionBarrierProtocol(
    new MySqlJdbcSessionBarrierQueries(verified.connection), MySqlSessionBarrierRegistry.forConnection(verified.connection), owner)
  private var held: HeldSessionBarrier = null
  private var heldScope: MySqlAuditBootScope = null
  private var heldExpectation: MySqlSchemaAuditExpectation = null

  private def scopeMatches(scope: MySqlAuditBootScope): Unit =
    require(scope != null && (Thread.currentThread() eq owner) && (scope.ownerThread eq owner), "exact owned session scope required")
    require(scope.verifiedConnection.contains(verified) && (scope.connectionToken eq verified.connection) &&
      (scope.connectionIdentity eq verified.identity), "exact verified session object identity required")

  def acquire(expectation: MySqlSchemaAuditExpectation, scope: MySqlAuditBootScope, wait: Duration): MySqlSessionBarrierAcquisition =
    scopeMatches(scope)
    require(expectation != null && verified.identity.serverUuid == expectation.serverUuid &&
      verified.identity.catalog == expectation.schema && verified.identity.effectiveUser == expectation.effectiveUser,
      "barrier primary/schema/principal differ from verified approved target")
    if !verified.connection.getAutoCommit || verified.connection.isClosed then
      throw new SQLNonTransientConnectionException("session barrier acquisition requires an idle open connection")
    val namespace = MySqlSessionBarrierNamespace.forIdentity(expectation.serverUuid, expectation.schema, expectation.provisioningUuid, expectation.bindingUuid)
    if protocol.acquire(namespace, wait) then
      heldScope = scope
      heldExpectation = expectation
      held = new HeldSessionBarrier(this, generation, protocol.capturedCid, namespace)
      MySqlSessionBarrierAcquisition.Held(held)
    else MySqlSessionBarrierAcquisition.Deferred

  def requireHeld(expectation: MySqlSchemaAuditExpectation, scope: MySqlAuditBootScope): Unit =
    if held == null then
      scopeMatches(scope)
      throw new SQLNonTransientConnectionException("session barrier has not been acquired")
    held.readmit(verified, expectation, scope)

  def readmit(value: HeldSessionBarrier, actual: VerifiedMySqlConnection, expectation: MySqlSchemaAuditExpectation, scope: MySqlAuditBootScope): Unit =
    try
      scopeMatches(scope)
      require((value eq held) && (actual eq verified) && (scope eq heldScope) && value.connectionGenerationId == generation,
        "session barrier generation or scope is stale")
      require(expectation != null, "transaction approved target required")
      val expectedNamespace = MySqlSessionBarrierNamespace.forIdentity(expectation.serverUuid, expectation.schema, expectation.provisioningUuid, expectation.bindingUuid)
      MySqlSessionBarrierTargetRules.requireSame(value.namespace, expectedNamespace)
      protocol.readmit()
    catch
      case primary: Throwable =>
        if Thread.currentThread() eq owner then protocol.invalidate(primary)
        throw primary

  def beginPhysicalClose(): Unit = protocol.beginPhysicalClose()
  def closeObservation: MySqlSessionBarrierCloseObservation = protocol.closeObservation
  def observe(scope: MySqlAuditBootScope): MySqlSessionObservation =
    scopeMatches(scope)
    try
      val closed = verified.connection.isClosed
      if closed then protocol.beginPhysicalClose()
      else if protocol.isHeld then requireHeld(heldExpectation, scope)
      val cid = if closed then None else Some(new MySqlJdbcSessionBarrierQueries(verified.connection).connectionId())
      // Identity fields are the source's verified captured facts, not a new primary incarnation proof.
      MySqlSessionObservation(cid, verified.identity.serverUuid, verified.identity.catalog, verified.identity.effectiveUser,
        scope.bootUuid, scope.bootGeneration, scope.connectionGeneration, closed, protocol.isAttempted, protocol.isHeld)
    catch
      case primary: Throwable => protocol.invalidate(primary); throw primary

/** 本切片不伪造缺失的 v3 anchor/genesis issuer。旧 Fresh 不能继续写入。 */
private[mysql] object MySqlB1V3MutationGate:
  def requireImplementedIssuers(): Unit =
    throw new SQLNonTransientConnectionException("B1 v3 mutation blocked: durable source-anchor and genesis issuers are not implemented")

private[mysql] object MySqlSessionBarrierTargetRules:
  def requireSame(actual: MySqlSessionBarrierNamespace, expected: MySqlSessionBarrierNamespace): Unit =
    if actual == null || expected == null || actual.lockName() != expected.lockName() then
      throw new SQLNonTransientConnectionException("session barrier namespace differs from the transaction approved source")
