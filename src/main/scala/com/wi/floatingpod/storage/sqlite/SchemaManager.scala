package com.wi.floatingpod.storage.sqlite

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.util.Objects

/**
 * 按版本顺序执行已登记 schema migration，确保数据库结构只在完整事务成功后前进。
 */
object SchemaManager:
  private val logger: Logger = LogManager.getLogger("floatingpod-storage")

  /** 已登记 migration 的版本与资源路径，后续版本必须保持递增。 */
  private val migrations: List[Migration] = List(
    Migration(1, "Initial schema", "/db/migration/V001__initial_schema.sql"),
    Migration(2, "Activity transition authority", "/db/migration/V002__activity_transition_authority.sql"),
    Migration(3, "Strict activity lifecycle guards", "/db/migration/V003__strict_activity_lifecycle.sql"),
    Migration(4, "Physical recovery obligations", "/db/migration/V004__physical_recovery_obligations.sql")
  )

  /** schema_version 缺失时必须拒绝的既有 FloatingPod 内部对象，避免误把受管数据库当作新库。 */
  private val ManagedObjectNames = List(
    "schema_version",
    "floating_pod_state",
    "idx_floating_pod_state_owner_uuid",
    "idx_floating_pod_state_is_active",
    "idx_floating_pod_state_last_updated",
    "player_entitlement",
    "idx_floating_pod_state_owner_active",
    "trg_floating_pod_state_reject_active_insert",
    "trg_floating_pod_state_reject_active_null_dimension",
    "trg_floating_pod_state_reject_active_delete",
    "physical_recovery_binding",
    "physical_recovery_locator",
    "physical_recovery_locator_chunk",
    "idx_physical_recovery_locator_chunk_dimension"
  )

  /**
   * 对连接应用尚未记录的 migration；单个 migration 失败时回滚其全部 SQL。
   *
   * @param connection 需要升级的 JDBC 连接。
   */
  def applyMigrations(connection: Connection): Unit =
    Objects.requireNonNull(connection, "connection must not be null")
    applyMigrations(connection, JdbcTransactionControl(connection))

  /**
   * 为同包失败注入测试复用 migration transaction 边界，生产路径仍传入同一 JDBC 连接的直接控制器。
   *
   * @param connection 需要升级的 JDBC 连接。
   * @param transactionControl 负责该连接 transaction 状态和关闭动作的控制边界。
   */
  private[sqlite] def applyMigrations(
      connection: Connection,
      transactionControl: TransactionControl
  ): Unit =
    Objects.requireNonNull(connection, "connection must not be null")
    Objects.requireNonNull(transactionControl, "transactionControl must not be null")
    val appliedVersions = readAppliedVersions(connection)
    validateAppliedVersions(appliedVersions)
    migrations.drop(appliedVersions.size).foreach { migration =>
      applyMigration(connection, transactionControl, migration)
    }

  private def readAppliedVersions(connection: Connection): List[Int] =
    if !schemaVersionTableExists(connection) then
      rejectManagedObjectsWithoutSchemaVersion(connection)
      return Nil

    val sql = "SELECT version FROM schema_version ORDER BY version ASC"
    val statement = connection.prepareStatement(sql)
    try
      val resultSet = statement.executeQuery()
      try
        val versions = List.newBuilder[Int]
        while resultSet.next() do versions += resultSet.getInt(1)
        versions.result()
      finally resultSet.close()
    finally statement.close()

  private def schemaVersionTableExists(connection: Connection): Boolean =
    val tableExistsSql = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?"
    val tableExistsStatement = connection.prepareStatement(tableExistsSql)
    try
      tableExistsStatement.setString(1, "schema_version")
      val tableExistsResult = tableExistsStatement.executeQuery()
      try
        if (!tableExistsResult.next()) then
          throw new IllegalStateException("sqlite_master query returned no row")
        tableExistsResult.getInt(1) == 1
      finally tableExistsResult.close()
    finally tableExistsStatement.close()

  private def validateAppliedVersions(appliedVersions: List[Int]): Unit =
    val expectedPrefix = migrations.map(_.version).take(appliedVersions.size)
    if appliedVersions != expectedPrefix then
      throw new IllegalStateException(
        s"Unsupported SQLite schema history: applied=${appliedVersions.mkString(",")}, expectedPrefix=${expectedPrefix.mkString(",")}"
      )

  private def rejectManagedObjectsWithoutSchemaVersion(connection: Connection): Unit =
    val placeholders = List.fill(ManagedObjectNames.size)("?").mkString(", ")
    val sql = s"SELECT name FROM sqlite_master WHERE name IN ($placeholders) ORDER BY name LIMIT 1"
    val statement = connection.prepareStatement(sql)
    try
      ManagedObjectNames.zipWithIndex.foreach { case (name, index) =>
        statement.setString(index + 1, name)
      }
      val resultSet = statement.executeQuery()
      try
        if resultSet.next() then
          throw new IllegalStateException(
            s"SQLite database contains FloatingPod managed object ${resultSet.getString(1)} without schema_version"
          )
      finally resultSet.close()
    finally statement.close()

  private def applyMigration(
      connection: Connection,
      transactionControl: TransactionControl,
      migration: Migration
  ): Unit =
    try
      TransactionBoundary.execute(transactionControl, failure => poisonConnection(transactionControl, failure)) {
        executeSqlScript(connection, loadSql(migration.resourcePath))
      }
      logger.info("Applied SQLite schema migration V{}: {}", Int.box(migration.version), migration.description)
    catch
      case exception: Throwable =>
        logger.error(
          s"SQLite schema migration failed: version=${migration.version}, resource=${migration.resourcePath}",
          exception
        )
        throw exception

  private def poisonConnection(transactionControl: TransactionControl, primaryFailure: Throwable): Unit =
    try transactionControl.close()
    catch
      case closeFailure: Throwable => primaryFailure.addSuppressed(closeFailure)

  private def loadSql(resourcePath: String): String =
    val input = Option(getClass.getResourceAsStream(resourcePath)).getOrElse {
      throw new IllegalStateException(s"Missing SQLite migration resource: $resourcePath")
    }
    try String(input.readAllBytes(), StandardCharsets.UTF_8)
    finally input.close()

  private def executeSqlScript(connection: Connection, script: String): Unit =
    splitSqlStatements(script).foreach { sql =>
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    }

  private def splitSqlStatements(script: String): List[String] =
    val statements = List.newBuilder[String]
    val current = new StringBuilder
    var index = 0
    var quote: Char = 0
    var lineComment = false
    var blockComment = false
    var trigger = false
    while index < script.length do
      val character = script.charAt(index)
      val next = if index + 1 < script.length then script.charAt(index + 1) else 0.toChar
      if lineComment then
        current.append(character)
        if character == '\n' then lineComment = false
      else if blockComment then
        current.append(character)
        if character == '*' && next == '/' then
          current.append(next)
          index += 1
          blockComment = false
      else if quote != 0 then
        current.append(character)
        if character == quote then
          if next == quote then
            current.append(next)
            index += 1
          else quote = 0
      else if character == '-' && next == '-' then
        current.append(character).append(next)
        index += 1
        lineComment = true
      else if character == '/' && next == '*' then
        current.append(character).append(next)
        index += 1
        blockComment = true
      else if character == '\'' || character == '"' then
        current.append(character)
        quote = character
      else if character == ';' then
        val candidate = current.result().trim
        if !trigger && candidate.toLowerCase.contains("create trigger") then trigger = true
        if !trigger || candidate.toUpperCase.endsWith("END") then
          if candidate.nonEmpty then statements += candidate
          current.clear()
          trigger = false
        else current.append(character)
      else current.append(character)
      index += 1
    val tail = current.result().trim
    if tail.nonEmpty then statements += tail
    statements.result()

  /**
   * 记录一个可排序且不可变的 migration 定义，避免运行时扫描引入不受控脚本。
   */
  private final case class Migration(version: Int, description: String, resourcePath: String)
