package com.wi.floatingpod.storage.mysql

import com.wi.sqljdbcdriverlib.mysql.{MySqlCredentialSnapshot, MySqlCredentialSource, MySqlSecretSnapshot, MySqlSecretSource}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.file.Path
import java.sql.{Connection, ResultSet, SQLException, SQLNonTransientConnectionException, Statement}
import java.time.Duration
import java.util.UUID

class MySqlConnectionSourceTest:
  @Test
  def rejectsUnexpectedServerIdentityAndClosesTheUntransferredConnection(): Unit =
    val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
    val sessionSetCalls = new java.util.concurrent.atomic.AtomicInteger(0)
    val connection = identityConnection(
      "00000000-0000-0000-0000-000000000001",
      closed,
      sessionSetCalls,
      null
    )
    val source = new MySqlConnectionSource(settings())

    val failure = assertThrows(classOf[SQLNonTransientConnectionException], () => source.verifyAndTransfer(connection))

    assertTrue(closed.get())
    assertEquals(0, sessionSetCalls.get())
    assertFalse(failure.getMessage.contains("00000000-0000-0000-0000-000000000001"))
    assertFalse(failure.getMessage.contains("982937bc-bc46-11f1-b957-04421a0aa74d"))

  @Test
  def rejectsWeakServerDurabilityBeforeChangingTheSession(): Unit =
    val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
    val sessionSetCalls = new java.util.concurrent.atomic.AtomicInteger(0)
    val connection = identityConnection(
      "982937bc-bc46-11f1-b957-04421a0aa74d",
      closed,
      sessionSetCalls,
      null,
      innodbFlushLogAtTrxCommit = 2,
      syncBinlog = 1L
    )

    val failure = assertThrows(classOf[SQLNonTransientConnectionException], () => new MySqlConnectionSource(settings()).verifyAndTransfer(connection))

    assertTrue(closed.get())
    assertEquals(0, sessionSetCalls.get())
    assertTrue(failure.getMessage.contains("durability"))

  @Test
  def preservesSqlFailureAndAttachesConnectionCloseFailureDuringPreHandoff(): Unit =
    val primary = new SQLException("forced identity-query failure", "08006", 9001)
    val cleanup = new SQLException("forced connection-close failure", "08006", 9002)
    val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
    val connection = failingQueryConnection(primary, cleanup, closed)
    val source = new MySqlConnectionSource(settings())

    val observed = assertThrows(classOf[SQLException], () => source.verifyAndTransfer(connection))

    assertSame(primary, observed)
    assertEquals(List(cleanup), observed.getSuppressed.toList)
    assertTrue(closed.get())

  @Test
  def failedRawOpenRemainsOwnedByActualSessionCloseObservation(): Unit =
    val root = java.nio.file.Files.createTempDirectory("fp-partial-open-").toAbsolutePath
    MySqlOrdinaryCertifiedHandoffFixture.prepareOrdinaryProcessFixture(root)
    val (inputs, _) = MySqlOrdinaryCertifiedHandoffFixture.controlledRunnerSource(root, UUID.randomUUID())
    val primary = new SQLException("forced pre-admission identity failure")
    val cleanup = new SQLException("forced raw close failure")
    val closeAttempted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val raw = failingQueryConnection(primary, cleanup, closeAttempted)
    val source = new MySqlConnectionSource(inputs.settings, () => raw)
    val observed = assertThrows(classOf[SQLException], () =>
      MySqlOwnedDatabaseSession.openCertifiedWithConnectionSourceForAssembly(inputs, 1L, Duration.ofSeconds(1), source))
    assertSame(primary, observed)
    assertTrue(observed.getSuppressed.contains(cleanup))
    val observation = observed.getSuppressed.collectFirst {
      case value: MySqlOwnedSessionInitializationObservation =>
        assertTrue(value.retainedSession.retainedPartialOpen.hasRawHandle())
        value.closeObservation
    }.getOrElse(throw new AssertionError("actual Session close observation required"))
    assertTrue(observation.connectionOpened, "driver returned raw connection must remain represented")
    assertTrue(observation.jdbcCloseAttempted)
    assertEquals(Some(false), observation.jdbcClosed)
    assertTrue(observation.unresolvedOwnedResponsibility)
    assertTrue(closeAttempted.get())

  @Test
  def successfulRawCloseIsObservedWithoutInventingAdmission(): Unit =
    val (failure, observation) = failedSession(null, true, null)
    assertTrue(observation.connectionOpened)
    assertTrue(observation.jdbcCloseAttempted)
    assertEquals(Some(true), observation.jdbcClosed)
    assertFalse(observation.acquireAttempted)
    assertFalse(observation.unresolvedOwnedResponsibility)
    assertSame(failure, observation.failure.get)

  @Test
  def rawGetterFailurePreservesVerificationAndCleanupCauses(): Unit =
    val cleanup = new SQLException("raw close failed")
    val getter = new SQLException("raw getter failed")
    val (failure, observation) = failedSession(cleanup, false, getter)
    assertTrue(failure.getSuppressed.contains(cleanup))
    assertTrue(failure.getSuppressed.contains(getter))
    assertEquals(None, observation.jdbcClosed)
    assertTrue(observation.connectionOpened && observation.unresolvedOwnedResponsibility)

  @Test
  def driverFailureBeforeReturnDoesNotInventRawOwnership(): Unit =
    val root = java.nio.file.Files.createTempDirectory("fp-driver-failure-").toAbsolutePath
    MySqlOrdinaryCertifiedHandoffFixture.prepareOrdinaryProcessFixture(root)
    val (inputs, _) = MySqlOrdinaryCertifiedHandoffFixture.controlledRunnerSource(root, UUID.randomUUID())
    val primary = new SQLException("driver failed before returning connection")
    val source = new MySqlConnectionSource(inputs.settings, () => throw primary)
    assertSame(primary, assertThrows(classOf[SQLException], () =>
      MySqlOwnedDatabaseSession.openCertifiedWithConnectionSourceForAssembly(inputs, 1L, Duration.ofSeconds(1), source)))
    val observation = primary.getSuppressed.collectFirst {
      case value: MySqlOwnedSessionInitializationObservation => value.closeObservation
    }.get
    assertFalse(observation.connectionOpened)
    assertFalse(observation.jdbcCloseAttempted)
    assertFalse(observation.unresolvedOwnedResponsibility)

  @Test
  def lexicalOpenFailureRetainsItsOriginalPartialOwner(): Unit =
    val primary = new SQLException("lexical identity failure")
    val cleanup = new SQLException("lexical close failure")
    val raw = failingQueryConnection(primary, cleanup, new java.util.concurrent.atomic.AtomicBoolean(false))
    val source = new MySqlConnectionSource(settings(), () => raw)
    assertSame(primary, assertThrows(classOf[SQLException], () => source.open()))
    val owner = primary.getSuppressed.collectFirst {
      case value: MySqlConnectionSource.FailedOpenOwnership => value.retainedOwner()
    }.get
    assertTrue(owner.hasRawHandle())
    assertTrue(owner.closeAttempted())
    assertFalse(owner.closeReturned())
    assertFalse(owner.rawConnectionClosed())
    assertSame(primary, owner.primaryFailure())
    assertThrows(classOf[IllegalStateException], () => owner.openOnce())

  @Test
  def successfulOpenTransfersExactConnectionAndRejectsSecondOpen(): Unit =
    val root = java.nio.file.Files.createTempDirectory("fp-source-transfer-").toAbsolutePath
    MySqlOrdinaryCertifiedHandoffFixture.prepareOrdinaryProcessFixture(root)
    val (inputs, scriptedSource) = MySqlOrdinaryCertifiedHandoffFixture.controlledRunnerSource(root, UUID.randomUUID())
    val raw = scriptedSource().connection
    val holder = new MySqlConnectionSource(inputs.settings, () => raw).newOwnedSessionOpen()
    val verified = holder.openOnce()
    try
      assertSame(raw, verified.connection)
      assertSame(verified, holder.transferredVerified())
      assertFalse(holder.hasRawHandle())
      assertFalse(holder.closeAttempted())
      assertThrows(classOf[IllegalStateException], () => holder.openOnce())
    finally verified.close()

  private def failedSession(cleanup: SQLException, closedResult: Boolean, getter: Throwable):
      (SQLException, MySqlOwnedSessionCloseObservation) =
    val root = java.nio.file.Files.createTempDirectory("fp-failed-session-").toAbsolutePath
    MySqlOrdinaryCertifiedHandoffFixture.prepareOrdinaryProcessFixture(root)
    val (inputs, _) = MySqlOrdinaryCertifiedHandoffFixture.controlledRunnerSource(root, UUID.randomUUID())
    val primary = new SQLException("actual verification failure")
    val raw = failingQueryConnection(primary, cleanup, new java.util.concurrent.atomic.AtomicBoolean(false), closedResult, getter)
    val source = new MySqlConnectionSource(inputs.settings, () => raw)
    assertSame(primary, assertThrows(classOf[SQLException], () =>
      MySqlOwnedDatabaseSession.openCertifiedWithConnectionSourceForAssembly(inputs, 1L, Duration.ofSeconds(1), source)))
    val initialization = primary.getSuppressed.collectFirst {
      case value: MySqlOwnedSessionInitializationObservation => value
    }.get
    assertTrue(initialization.retainedSession.retainedPartialOpen.hasRawHandle())
    (primary, initialization.closeObservation)

  @Test def managedSessionSetsAndReadsBackOnlyResultEncodingBeforeReturningIdentity(): Unit =
    val executed = scala.collection.mutable.ArrayBuffer.empty[String]
    val connection = healthySessionConnection(executed)
    new MySqlConnectionVerification(settings().withUtf8mb4Results).verifySession(connection)
    assertEquals(Vector("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
      "SET SESSION sql_mode = 'STRICT_ALL_TABLES'", "SET SESSION time_zone = '+00:00'",
      "SET SESSION character_set_results = 'utf8mb4'"), executed.toVector)

  @Test def managedSessionRejectsUnappliedResultEncoding(): Unit =
    val connection = healthySessionConnection(scala.collection.mutable.ArrayBuffer.empty[String], "latin1")
    val failure = assertThrows(classOf[SQLNonTransientConnectionException], () =>
      new MySqlConnectionVerification(settings().withUtf8mb4Results).verifySession(connection))
    assertTrue(failure.getMessage.contains("result character set"))

  @Test def externalSessionDoesNotChangeOrReadResultEncoding(): Unit =
    val executed = scala.collection.mutable.ArrayBuffer.empty[String]
    new MySqlConnectionVerification(settings()).verifySession(healthySessionConnection(executed))
    assertEquals(3, executed.size)
    assertFalse(executed.exists(_.contains("character_set")))

  private def healthySessionConnection(executed: scala.collection.mutable.ArrayBuffer[String],
      resultEncoding: String = "utf8mb4"): Connection =
    def result(values: Vector[AnyRef]): ResultSet =
      var available = true
      proxy(classOf[ResultSet]) { (_, method, args) => method.getName match
        case "next" => val next = available; available = false; java.lang.Boolean.valueOf(next)
        case "getString" | "getInt" | "getLong" => values(args(0).asInstanceOf[Integer].intValue() - 1)
        case "wasNull" => java.lang.Boolean.FALSE
        case "close" => null
        case name => throw new UnsupportedOperationException(s"Unexpected result method $name")
      }
    val statement = proxy(classOf[Statement]) { (_, method, args) => method.getName match
      case "execute" => executed += args(0).asInstanceOf[String]; java.lang.Boolean.FALSE
      case "executeQuery" =>
        val sql = args(0).asInstanceOf[String]
        if sql.startsWith("SELECT @@GLOBAL.server_uuid") then
          result(Vector("982937bc-bc46-11f1-b957-04421a0aa74d", "9.7.2", "fp_test", "fp_test_runtime@%",
            "InnoDB", Integer.valueOf(1), java.lang.Long.valueOf(1L)))
        else if sql == "SELECT @@SESSION.transaction_isolation, @@SESSION.sql_mode, @@SESSION.time_zone" then
          result(Vector("READ-COMMITTED", "STRICT_ALL_TABLES", "+00:00"))
        else if sql == "SELECT @@SESSION.character_set_results" then
          assertTrue(executed.contains("SET SESSION character_set_results = 'utf8mb4'"))
          result(Vector(resultEncoding))
        else if sql == "SHOW SESSION STATUS LIKE 'Ssl_version'" then result(Vector("Ssl_version", "TLSv1.3"))
        else if sql == "SHOW SESSION STATUS LIKE 'Ssl_cipher'" then result(Vector("Ssl_cipher", "TLS_AES_256_GCM_SHA384"))
        else throw new UnsupportedOperationException(s"Unexpected session query $sql")
      case "close" => null
      case name => throw new UnsupportedOperationException(s"Unexpected statement method $name")
    }
    proxy(classOf[Connection]) { (_, method, _) => method.getName match
      case "createStatement" => statement
      case name => throw new UnsupportedOperationException(s"Unexpected connection method $name")
    }

  private def settings(): MySqlConnectionSettings =
    val credentialSource = new MySqlCredentialSource:
      override def acquire(): MySqlCredentialSnapshot = throw new AssertionError("test must not open a live MySQL connection")
    val trustSecretSource = new MySqlSecretSource:
      override def acquire(): MySqlSecretSnapshot = throw new AssertionError("test must not acquire a trust-store password")
    MySqlConnectionSettings(
      "mysql-test.example",
      33317,
      "fp_test",
      UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"),
      "fp_test_runtime@%",
      credentialSource,
      Path.of("C:/fp-test/trust.p12"),
      trustSecretSource,
      Duration.ofSeconds(3),
      Duration.ofSeconds(10),
      MySqlDurabilityExpectation(1, 1L)
    )

  private def identityConnection(
      actualServerUuid: String,
      closed: java.util.concurrent.atomic.AtomicBoolean,
      sessionSetCalls: java.util.concurrent.atomic.AtomicInteger,
      closeFailure: SQLException,
      innodbFlushLogAtTrxCommit: Int = 1,
      syncBinlog: Long = 1L
  ): Connection =
    var rowAvailable = true
    val result = proxy(classOf[ResultSet]) { (_, method, args) =>
      method.getName match
        case "next" =>
          val wasAvailable = rowAvailable
          rowAvailable = false
          java.lang.Boolean.valueOf(wasAvailable)
        case "getString" =>
          args(0).asInstanceOf[Integer].intValue() match
            case 1 => actualServerUuid
            case 2 => "9.7.2"
            case 3 => "fp_test"
            case 4 => "fp_test_runtime@%"
            case 5 => "InnoDB"
            case other => throw new UnsupportedOperationException(s"Unexpected identity string column $other")
        case "getInt" => Integer.valueOf(innodbFlushLogAtTrxCommit)
        case "getLong" => java.lang.Long.valueOf(syncBinlog)
        case "wasNull" => java.lang.Boolean.FALSE
        case "close" => null
        case "toString" => "test-result-set"
        case name => throw new UnsupportedOperationException(s"Unexpected ResultSet method $name")
    }
    val statement = proxy(classOf[Statement]) { (_, method, _) =>
      method.getName match
        case "executeQuery" => result
        case "execute" => sessionSetCalls.incrementAndGet(); java.lang.Boolean.FALSE
        case "close" => null
        case "toString" => "test-statement"
        case name => throw new UnsupportedOperationException(s"Unexpected Statement method $name")
    }
    proxy(classOf[Connection]) { (_, method, _) =>
      method.getName match
        case "createStatement" => statement
        case "close" =>
          closed.set(true)
          if closeFailure != null then throw closeFailure
          null
        case "isClosed" => java.lang.Boolean.FALSE
        case "toString" => "test-connection"
        case name => throw new UnsupportedOperationException(s"Unexpected Connection method $name")
    }

  private def failingQueryConnection(
      primary: SQLException,
      closeFailure: SQLException,
      closed: java.util.concurrent.atomic.AtomicBoolean,
      closedResult: Boolean = false, getterFailure: Throwable = null
  ): Connection =
    val statement = proxy(classOf[Statement]) { (_, method, _) =>
      method.getName match
        case "executeQuery" => throw primary
        case "close" => null
        case "toString" => "test-statement"
        case name => throw new UnsupportedOperationException(s"Unexpected Statement method $name")
    }
    proxy(classOf[Connection]) { (_, method, _) =>
      method.getName match
        case "createStatement" => statement
        case "close" =>
          closed.set(true)
          if closeFailure != null then throw closeFailure
          null
        case "isClosed" =>
          if getterFailure != null then throw getterFailure
          java.lang.Boolean.valueOf(closedResult)
        case "toString" => "test-connection"
        case name => throw new UnsupportedOperationException(s"Unexpected Connection method $name")
    }

  private def proxy[T](interfaceType: Class[T])(dispatch: (AnyRef, Method, Array[AnyRef]) => AnyRef): T =
    Proxy.newProxyInstance(
      interfaceType.getClassLoader,
      Array[Class[?]](interfaceType),
      new InvocationHandler:
        override def invoke(proxy: Any, method: Method, args: Array[Object]): Object =
          dispatch(proxy.asInstanceOf[AnyRef], method, Option(args).map(_.asInstanceOf[Array[AnyRef]]).orNull).asInstanceOf[Object]
    ).asInstanceOf[T]
