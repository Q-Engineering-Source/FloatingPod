package com.wi.floatingpod.storage.mysql

import java.sql.{SQLException, SQLNonTransientConnectionException}
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** Calls the real one-shot/freeze protocol. Scripted fixed-query results are not a live SQL acceptance. */
class MySqlSessionTerminationBarrierTest:
  private def namespace = MySqlSessionBarrierNamespace.forIdentity(
    UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "fp_test_20261007_sec07",
    UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14"), UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf"))
  private final class Queries extends MySqlSessionBarrierQueries:
    var initialCid = 71L
    var actualCid = 71L
    var actualOwner: Option[Long] = Some(71L)
    var response: Option[Int] = Some(1)
    var primary: Throwable = null
    var initialFailure: Throwable = null
    var getCalls = 0
    var ownerCalls = 0
    var waits = Vector.empty[Long]
    var names = Vector.empty[String]
    def connectionId(): Long =
      if initialFailure != null then throw initialFailure
      initialCid
    def acquire(name: String, timeoutSeconds: Long): Option[Int] =
      getCalls += 1; waits :+= timeoutSeconds; names :+= name
      if primary != null then throw primary
      response
    def owner(name: String): (Long, Option[Long]) =
      ownerCalls += 1; names :+= name
      (actualCid, actualOwner)
  private def protocol(q: Queries, once: MySqlSessionBarrierOnce = new MySqlSessionBarrierOnce) =
    new MySqlSessionBarrierProtocol(q, once, Thread.currentThread())

  @Test def acquiredBarrierReadmitsByObservationWithoutReenteringGetLock(): Unit =
    val q = new Queries; val p = protocol(q)
    assertTrue(p.acquire(namespace, Duration.ofSeconds(3)))
    p.readmit(); p.readmit()
    assertEquals(1, q.getCalls)
    assertEquals(Vector(3L), q.waits)
    assertEquals(3, q.ownerCalls)
    assertTrue(q.names.forall(_ == namespace.lockName()))
    assertEquals(71L, p.capturedCid)
    assertThrows(classOf[SQLNonTransientConnectionException], () => p.acquire(namespace, Duration.ZERO))
    assertEquals(1, q.getCalls)

  @Test def busyIsDeferredAndCannotRetryOnSameSessionOrNewWrapper(): Unit =
    val q = new Queries; q.response = Some(0)
    val once = new MySqlSessionBarrierOnce; val p = protocol(q, once)
    assertFalse(p.acquire(namespace, Duration.ZERO))
    assertTrue(p.isAttempted); assertFalse(p.isHeld)
    assertFalse(p.closeObservation.mayRetainServerLock)
    assertThrows(classOf[SQLNonTransientConnectionException], () => p.readmit())
    assertThrows(classOf[SQLNonTransientConnectionException], () => protocol(q, once).acquire(namespace, Duration.ZERO))
    assertEquals(1, q.getCalls); assertEquals(0, q.ownerCalls)

  @Test def nullAndMalformedRepliesFreezeWithoutASecondGetLock(): Unit =
    for response <- Vector(None, Some(-1), Some(2)) do
      val q = new Queries; q.response = response
      val p = protocol(q)
      val original = assertThrows(classOf[SQLNonTransientConnectionException], () => p.acquire(namespace, Duration.ZERO))
      assertFalse(p.isHeld)
      assertSame(original, assertThrows(classOf[SQLNonTransientConnectionException], () => p.readmit()))
      assertThrows(classOf[SQLNonTransientConnectionException], () => p.acquire(namespace, Duration.ZERO))
      assertEquals(1, q.getCalls)

  @Test def replyLostKeepsExactPrimaryAndSuppressedCleanupAndPreventsWrapperRetry(): Unit =
    val q = new Queries; val primary = new SQLException("controlled lost reply", "08006", 9001)
    val cleanup = new SQLException("controlled close failure", "08006", 9002)
    primary.addSuppressed(cleanup); q.primary = primary
    val once = new MySqlSessionBarrierOnce; val p = protocol(q, once)
    assertSame(primary, assertThrows(classOf[SQLException], () => p.acquire(namespace, Duration.ZERO)))
    assertSame(primary, assertThrows(classOf[SQLException], () => p.readmit()))
    assertTrue(p.closeObservation.mayRetainServerLock)
    assertSame(primary, p.closeObservation.failure.get)
    assertEquals(Vector(cleanup), primary.getSuppressed.toVector)
    assertThrows(classOf[SQLNonTransientConnectionException], () => protocol(q, once).acquire(namespace, Duration.ZERO))
    assertEquals(1, q.getCalls)

  @Test def failureBeforeGetLockStillSealsPhysicalSessionAttempt(): Unit =
    val q = new Queries; val primary = new SQLException("controlled CID failure", "08006", 9003)
    q.initialFailure = primary
    val once = new MySqlSessionBarrierOnce
    assertSame(primary, assertThrows(classOf[SQLException], () => protocol(q, once).acquire(namespace, Duration.ZERO)))
    q.initialFailure = null
    assertThrows(classOf[SQLNonTransientConnectionException], () => protocol(q, once).acquire(namespace, Duration.ZERO))
    assertEquals(0, q.getCalls)

  @Test def cidChangeOrForeignOwnerCannotIssueHeldAndDoesNotRetry(): Unit =
    for (cid, owner) <- Vector((72L, Some(71L)), (71L, Some(72L)), (71L, None)) do
      val q = new Queries; q.actualCid = cid; q.actualOwner = owner
      val p = protocol(q)
      assertThrows(classOf[SQLNonTransientConnectionException], () => p.acquire(namespace, Duration.ZERO))
      assertFalse(p.isHeld); assertEquals(1, q.getCalls)

  @Test def lostLockIsPermanentlyInvalidEvenIfOwnerLaterLooksEqual(): Unit =
    val q = new Queries; val p = protocol(q)
    assertTrue(p.acquire(namespace, Duration.ZERO))
    q.actualOwner = None
    val original = assertThrows(classOf[SQLNonTransientConnectionException], () => p.readmit())
    q.actualOwner = Some(71L)
    assertSame(original, assertThrows(classOf[SQLNonTransientConnectionException], () => p.readmit()))
    assertFalse(p.isHeld); assertEquals(1, q.getCalls)

  @Test def boundedWaitIsValidatedBeforeAnySessionAttempt(): Unit =
    for wait <- Vector(Duration.ofSeconds(-1), Duration.ofSeconds(31), Duration.ofNanos(1)) do
      val q = new Queries; val p = protocol(q)
      assertThrows(classOf[IllegalArgumentException], () => p.acquire(namespace, wait))
      assertFalse(p.isAttempted); assertEquals(0, q.getCalls)

  @Test def crossThreadUseCannotCallJdbcQueries(): Unit =
    val q = new Queries; val p = protocol(q)
    val failure = new AtomicReference[Throwable]()
    val other = new Thread(() => {
      try p.acquire(namespace, Duration.ZERO)
      catch case c: Throwable => failure.set(c)
      ()
    }, "session-barrier-wrong-owner")
    other.start(); other.join(5000)
    assertFalse(other.isAlive); assertTrue(failure.get().isInstanceOf[IllegalArgumentException])
    assertEquals(0, q.getCalls); assertFalse(p.isAttempted)

  @Test def physicalCloseRevokesUseButNeverReleasesNamedLockInTheProtocol(): Unit =
    val q = new Queries; val p = protocol(q)
    assertTrue(p.acquire(namespace, Duration.ZERO))
    p.beginPhysicalClose()
    assertFalse(p.isHeld)
    assertTrue(p.closeObservation.heldReplyConfirmed)
    assertTrue(p.closeObservation.localCapabilityRevoked)
    assertTrue(p.closeObservation.mayRetainServerLock)
    assertThrows(classOf[SQLNonTransientConnectionException], () => p.readmit())
    assertThrows(classOf[SQLNonTransientConnectionException], () => p.acquire(namespace, Duration.ZERO))
    assertEquals(1, q.getCalls); assertEquals(1, q.ownerCalls)

  @Test def actualResourceCleanupPreservesFailureAtTheFixedQueryBoundary(): Unit =
    val primary = new SQLException("controlled query primary")
    val cleanup = new SQLException("controlled statement close")
    val resource = new AutoCloseable:
      def close(): Unit = throw cleanup
    val actual = assertThrows(classOf[SQLException], () => MySqlBusinessRows.using(resource)(_ => throw primary))
    assertSame(primary, actual); assertEquals(Vector(cleanup), actual.getSuppressed.toVector)

  @Test def oldFreshMutationIsExplicitlyBlockedUntilRealAnchorAndGenesisIssuersExist(): Unit =
    val failure = assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlB1V3MutationGate.requireImplementedIssuers())
    assertEquals("B1 v3 mutation blocked: durable source-anchor and genesis issuers are not implemented", failure.getMessage)

  @Test def differentApprovedBindingCannotUseAnAcquiredNamespaceForItsTransactions(): Unit =
    val same = namespace
    MySqlSessionBarrierTargetRules.requireSame(same, namespace)
    val other = MySqlSessionBarrierNamespace.forIdentity(
      UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "fp_test_20261007_sec07",
      UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14"), UUID.fromString("00000000-0000-0000-0000-000000000001"))
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSessionBarrierTargetRules.requireSame(same, other))

  @Test def sessionClosePredicateRetainsActualProtocolUnknownOrHeldDespiteLocalClosedObservation(): Unit =
    for kind <- Vector("held", "unknown", "busy") do
      val q = new Queries
      if kind == "busy" then q.response = Some(0)
      if kind == "unknown" then q.primary = new SQLException("controlled lost response", "08006", 9004)
      val p = protocol(q)
      if kind == "unknown" then assertThrows(classOf[SQLException], () => p.acquire(namespace, Duration.ZERO))
      else p.acquire(namespace, Duration.ZERO)
      p.beginPhysicalClose()
      // 此 getter observation 是谓词输入，不宣称本测试打开或关闭过真实 JDBC。
      val observation = MySqlOwnedSessionCloseObservation(UUID.randomUUID(), 1L, 1L,
        true, false, MySqlOwnedSessionRelease.NoAcquireAttempt, false, false, true, Some(true),
        None, None, None, Some(p.closeObservation))
      assertEquals(kind != "busy", observation.unresolvedOwnedResponsibility)
      assertEquals(1, q.getCalls)
