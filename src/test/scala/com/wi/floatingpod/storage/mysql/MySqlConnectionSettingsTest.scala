package com.wi.floatingpod.storage.mysql

import com.wi.sqljdbcdriverlib.core.BackendId
import com.wi.sqljdbcdriverlib.mysql.{MySqlCredentialSnapshot, MySqlCredentialSource, MySqlSecretSnapshot, MySqlSecretSource}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.UUID

class MySqlConnectionSettingsTest:
  @Test
  def createsAnExplicitMySqlRequestWithPkcs12AndCallerSuppliedSourcesWithoutLeakingInputs(): Unit =
    val store = Files.createTempFile("fp-mysql-trust", ".p12")
    try
      val credentialSource = new MySqlCredentialSource:
        override def acquire(): MySqlCredentialSnapshot = throw new AssertionError("opening was not requested")
        override def toString: String = "credential-source-secret-sentinel"
      val trustSecretSource = new MySqlSecretSource:
        override def acquire(): MySqlSecretSnapshot = throw new AssertionError("opening was not requested")
        override def toString: String = "trust-source-secret-sentinel"
      val expectedServer = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
      val settings = MySqlConnectionSettings(
        "mysql-private.example",
        33317,
        "fp_private_schema",
        expectedServer,
        "fp_runtime@%",
        credentialSource,
        store,
        trustSecretSource,
        Duration.ofSeconds(3),
        Duration.ofSeconds(10),
        MySqlDurabilityExpectation(1, 1L)
      )

      val request = settings.openRequest

      assertEquals(BackendId.MYSQL, request.backend())
      assertEquals("fp_private_schema", request.endpoint().database())
      assertEquals(3000, request.timeouts().connectMillis())
      assertEquals(10000, request.timeouts().socketMillis())
      val diagnostics = settings.toString + request.toString
      List("mysql-private.example", "fp_private_schema", expectedServer.toString, "fp_runtime@%", store.toString, "credential-source-secret-sentinel", "trust-source-secret-sentinel")
        .foreach(value => assertFalse(diagnostics.contains(value)))
    finally Files.deleteIfExists(store)

  @Test
  def rejectsRelativeTrustStoreAndInvalidTimeoutBeforeAnyConnectionCanBeOpened(): Unit =
    val credentialSource = new MySqlCredentialSource:
      override def acquire(): MySqlCredentialSnapshot = throw new AssertionError("invalid settings must not acquire credentials")
    val trustSecretSource = new MySqlSecretSource:
      override def acquire(): MySqlSecretSnapshot = throw new AssertionError("invalid settings must not acquire trust secret")

    val relativePathFailure = assertThrows(
      classOf[IllegalArgumentException],
      () => settings(Path.of("relative.p12"), credentialSource, trustSecretSource)
    )
    assertFalse(relativePathFailure.getMessage.contains("relative.p12"))

    val timeoutFailure = assertThrows(
      classOf[IllegalArgumentException],
      () => settings(Path.of("C:/fp-test/trust.p12"), credentialSource, trustSecretSource, Duration.ofSeconds(31))
    )
    assertTrue(timeoutFailure.getMessage.contains("timeout"))

  @Test
  def rejectsDurabilityExpectationsThatCouldLoseCommittedTransactions(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => MySqlDurabilityExpectation(2, 1L))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDurabilityExpectation(1, 0L))

  private def settings(
      path: Path,
      credentialSource: MySqlCredentialSource,
      trustSecretSource: MySqlSecretSource,
      connectTimeout: Duration = Duration.ofSeconds(3)
  ): MySqlConnectionSettings =
    MySqlConnectionSettings(
      "127.0.0.1",
      33317,
      "fp_test",
      UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"),
      "fp_test_runtime@%",
      credentialSource,
      path,
      trustSecretSource,
      connectTimeout,
      Duration.ofSeconds(10),
      MySqlDurabilityExpectation(1, 1L)
    )
