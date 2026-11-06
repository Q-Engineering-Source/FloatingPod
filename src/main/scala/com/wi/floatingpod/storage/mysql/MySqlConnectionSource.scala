package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, ResultSet, SQLException, SQLNonTransientConnectionException, Statement}
import java.util.UUID

/** 在交给存储层的同一 JDBC 会话上核验的只读信息。 */
final class MySqlConnectionIdentity private[mysql] (
    val serverUuid: UUID,
    val serverVersion: String,
    val catalog: String,
    val effectiveUser: String,
    val defaultStorageEngine: String,
    val innodbFlushLogAtTrxCommit: Int,
    val syncBinlog: Long,
    val transactionIsolation: String,
    val sqlMode: String,
    val timeZone: String,
    val tlsVersion: String,
    val tlsCipher: String
):
  override def toString: String = "MySqlConnectionIdentity[redacted]"

/** 核验仅返回不可变身份；实际连接所有权和移交只由 Java package-private issuer 管理。 */
private[mysql] final class MySqlConnectionVerification(settings: MySqlConnectionSettings):
  require(settings != null, "MySQL connection settings are required")

  def verifySession(connection: Connection): MySqlConnectionIdentity =
    val server = readServerIdentity(connection)
    if server.serverUuid != settings.expectedServerUuid then reject("MySQL server identity does not match the configured target")
    if server.serverVersion != MySqlConnectionSource.SupportedServerVersion then reject("MySQL server version is unsupported")
    if server.catalog != settings.expectedSchema then reject("MySQL selected catalog does not match the configured schema")
    if server.effectiveUser != settings.expectedEffectiveUser then reject("MySQL effective account does not match the configured account")
    if !server.defaultStorageEngine.equalsIgnoreCase("InnoDB") then reject("MySQL default storage engine is not InnoDB")
    if server.innodbFlushLogAtTrxCommit != settings.durabilityExpectation.innodbFlushLogAtTrxCommit
      || server.syncBinlog != settings.durabilityExpectation.syncBinlog then
      reject("MySQL durability settings do not match the explicit connection contract")

    withStatement(connection) { statement =>
      statement.execute(MySqlConnectionVerification.SetReadCommitted)
      statement.execute(MySqlConnectionVerification.SetStrictSqlMode)
      statement.execute(MySqlConnectionVerification.SetUtcSession)
      if settings.utf8mb4ResultsRequired then statement.execute(MySqlConnectionVerification.SetUtf8mb4Results)
    }

    val session = readSessionSettings(connection)
    if session.transactionIsolation != MySqlConnectionSource.ExpectedReadCommitted then
      reject("MySQL session transaction isolation could not be set to READ COMMITTED")
    if session.sqlMode != MySqlConnectionSource.ExpectedStrictSqlMode then
      reject("MySQL session SQL mode could not be set to STRICT_ALL_TABLES")
    if session.timeZone != MySqlConnectionSource.ExpectedUtcTimeZone then
      reject("MySQL session time zone could not be set to UTC")

    if settings.utf8mb4ResultsRequired then
      withStatement(connection) { statement =>
        withResultSet(statement.executeQuery(MySqlConnectionVerification.ReadResultCharacterSet)) { result =>
          if !result.next() || result.getString(1) != "utf8mb4" || result.next() then
            reject("MySQL session result character set could not be set to utf8mb4")
        }
      }

    val tls = readTlsStatus(connection)
    if tls.cipher.isEmpty || !MySqlConnectionVerification.AllowedTlsVersions.contains(tls.version) then
      reject("MySQL session does not have an approved TLS cipher and protocol")

    new MySqlConnectionIdentity(
      server.serverUuid,
      server.serverVersion,
      server.catalog,
      server.effectiveUser,
      server.defaultStorageEngine,
      server.innodbFlushLogAtTrxCommit,
      server.syncBinlog,
      session.transactionIsolation,
      session.sqlMode,
      session.timeZone,
      tls.version,
      tls.cipher
    )

  private def readServerIdentity(connection: Connection): ServerIdentity =
    withStatement(connection) { statement =>
      withResultSet(statement.executeQuery(MySqlConnectionVerification.ReadServerIdentity)) { result =>
        if !result.next() then reject("MySQL server identity query returned no row")
        val serverUuid = parseServerUuid(requiredText(result.getString(1), "server UUID"))
        val serverVersion = requiredText(result.getString(2), "server version")
        val catalog = requiredText(result.getString(3), "selected catalog")
        val effectiveUser = requiredText(result.getString(4), "effective account")
        val storageEngine = requiredText(result.getString(5), "default storage engine")
        val innodbFlushLogAtTrxCommit = result.getInt(6)
        if result.wasNull() then reject("MySQL InnoDB durability setting is unavailable")
        val syncBinlog = result.getLong(7)
        if result.wasNull() then reject("MySQL binary-log durability setting is unavailable")
        if result.next() then reject("MySQL server identity query returned more than one row")
        ServerIdentity(serverUuid, serverVersion, catalog, effectiveUser, storageEngine, innodbFlushLogAtTrxCommit, syncBinlog)
      }
    }

  private def readSessionSettings(connection: Connection): SessionSettings =
    withStatement(connection) { statement =>
      withResultSet(statement.executeQuery(MySqlConnectionVerification.ReadSessionSettings)) { result =>
        if !result.next() then reject("MySQL session settings query returned no row")
        val isolation = requiredText(result.getString(1), "transaction isolation")
        val sqlMode = requiredText(result.getString(2), "SQL mode")
        val timeZone = requiredText(result.getString(3), "time zone")
        if result.next() then reject("MySQL session settings query returned more than one row")
        SessionSettings(normalizeIsolation(isolation), sqlMode, timeZone)
      }
    }

  private def readTlsStatus(connection: Connection): TlsStatus =
    TlsStatus(readSessionStatus(connection, "Ssl_version"), readSessionStatus(connection, "Ssl_cipher"))

  private def readSessionStatus(connection: Connection, name: String): String =
    val sql = s"SHOW SESSION STATUS LIKE '$name'"
    withStatement(connection) { statement =>
      withResultSet(statement.executeQuery(sql)) { result =>
        if !result.next() then reject("MySQL TLS session status is unavailable")
        val statusName = requiredText(result.getString(1), "TLS status name")
        val value = Option(result.getString(2)).getOrElse("").trim
        if result.next() then reject("MySQL TLS session status is ambiguous")
        if !statusName.equalsIgnoreCase(name) then reject("MySQL TLS session status name did not match")
        value
      }
    }

  private def parseServerUuid(value: String): UUID =
    if !MySqlConnectionVerification.CanonicalUuid.matches(value) then reject("MySQL server UUID is malformed")
    try UUID.fromString(value)
    catch case _: IllegalArgumentException => reject("MySQL server UUID is malformed")

  private def normalizeIsolation(value: String): String = value.toUpperCase(java.util.Locale.ROOT).replace('_', '-').replace(' ', '-')

  private def requiredText(value: String, field: String): String =
    if value == null || value.isBlank then reject(s"MySQL $field is unavailable")
    value

  private def withStatement[A](connection: Connection)(body: Statement => A): A =
    withResource(connection.createStatement())(body)

  private def withResultSet[A](result: ResultSet)(body: ResultSet => A): A = withResource(result)(body)

  private def withResource[R <: AutoCloseable, A](resource: R)(body: R => A): A =
    var primary: Throwable = null
    try body(resource)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary == null then resource.close()
      else
        try resource.close()
        catch case cleanup: Throwable => addSuppressed(primary, cleanup)

  private def addSuppressed(primary: Throwable, cleanup: Throwable): Unit =
    if cleanup != null && (cleanup ne primary) then primary.addSuppressed(cleanup)

  private def reject(message: String): Nothing = throw new SQLNonTransientConnectionException(message)

  private final case class ServerIdentity(
      serverUuid: UUID,
      serverVersion: String,
      catalog: String,
      effectiveUser: String,
      defaultStorageEngine: String,
      innodbFlushLogAtTrxCommit: Int,
      syncBinlog: Long
  )
  private final case class SessionSettings(transactionIsolation: String, sqlMode: String, timeZone: String)
  private final case class TlsStatus(version: String, cipher: String)

private[mysql] object MySqlConnectionVerification:
  private val AllowedTlsVersions = Set("TLSv1.2", "TLSv1.3")
  private val CanonicalUuid = "(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}".r
  private val ReadServerIdentity =
    "SELECT @@GLOBAL.server_uuid, @@GLOBAL.version, DATABASE(), CURRENT_USER(), @@GLOBAL.default_storage_engine, @@GLOBAL.innodb_flush_log_at_trx_commit, @@GLOBAL.sync_binlog"
  private val ReadSessionSettings = "SELECT @@SESSION.transaction_isolation, @@SESSION.sql_mode, @@SESSION.time_zone"
  private val SetReadCommitted = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
  private val SetStrictSqlMode = "SET SESSION sql_mode = 'STRICT_ALL_TABLES'"
  private val SetUtcSession = "SET SESSION time_zone = '+00:00'"
  private val SetUtf8mb4Results = "SET SESSION character_set_results = 'utf8mb4'"
  private val ReadResultCharacterSet = "SELECT @@SESSION.character_set_results"
