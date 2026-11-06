package com.wi.floatingpod.storage

import com.wi.floatingpod.storage.sqlite.SchemaManager

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Types
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * 直接验证初始 migration 的版本记录和重复应用行为。
 */
class SchemaManagerTest:
  @Test
  def migratesLegacyOperatingModesWithoutLosingOwnershipOrLegacyDimension(): Unit = withConnection { connection =>
    applyV001Only(connection)
    applyV002Only(connection)
    val ownedActive = "00000000-0000-0000-0000-000000000041"
    val unclaimedActive = "00000000-0000-0000-0000-000000000042"
    val unclaimedInactive = "00000000-0000-0000-0000-000000000043"
    val owner = "11111111-1111-1111-1111-111111111111"
    insertLegacyState(connection, ownedActive, Some(owner))
    insertLegacyState(connection, unclaimedActive, None)
    insertLegacyState(connection, unclaimedInactive, None)
    execute(connection, "UPDATE floating_pod_state SET is_active = 1 WHERE uuid = ?", UUID.fromString(ownedActive))
    execute(connection, "UPDATE floating_pod_state SET is_active = 1 WHERE uuid = ?", UUID.fromString(unclaimedActive))

    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertEquals("NORMAL", operatingMode(connection, UUID.fromString(ownedActive)))
    assertEquals("WORLD_IDLE", operatingMode(connection, UUID.fromString(unclaimedActive)))
    assertEquals("NORMAL", operatingMode(connection, UUID.fromString(unclaimedInactive)))
    assertEquals(1, countRows(connection, "SELECT COUNT(*) FROM floating_pod_state WHERE uuid = ? AND is_active = 1 AND dimension_id IS NULL", UUID.fromString(unclaimedActive)))
  }

  /** 验证空数据库按顺序应用 V001 至 V005 并记录版本。 */
  @Test
  def appliesInitialSchema(): Unit = withConnection { connection =>
    SchemaManager.applyMigrations(connection)
    assertEquals(6, schemaVersionCount(connection))
    assertEquals(6, currentVersion(connection))
    assertTrue(columnExists(connection, "floating_pod_state", "operating_mode"))
  }

  /** 验证已记录版本再次执行时不会重复写入 schema_version。 */
  @Test
  def appliesInitialSchemaIdempotently(): Unit = withConnection { connection =>
    SchemaManager.applyMigrations(connection)
    SchemaManager.applyMigrations(connection)
    assertEquals(6, schemaVersionCount(connection))
  }

  /** 验证 V002 增加维度和默认权益，并仅为既有所有者补齐一次默认记录。 */
  @Test
  def upgradesLegacyOwnersWithDimensionAndDefaultEntitlement(): Unit = withConnection { connection =>
    applyV001Only(connection)
    insertLegacyState(connection, "00000000-0000-0000-0000-000000000021", Some("11111111-1111-1111-1111-111111111111"))
    insertLegacyState(connection, "00000000-0000-0000-0000-000000000022", Some("11111111-1111-1111-1111-111111111111"))
    insertLegacyState(connection, "00000000-0000-0000-0000-000000000023", None)

    SchemaManager.applyMigrations(connection)
    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertEquals(6, schemaVersionCount(connection))
    assertTrue(columnExists(connection, "floating_pod_state", "dimension_id"))
    assertEquals(1, entitlementCount(connection))
    assertEquals((1, 1), entitlementFor(connection, UUID.fromString("11111111-1111-1111-1111-111111111111")))
  }

  /** 验证带缺号或未知版本的历史不会被静默接受为已迁移。 */
  @Test
  def rejectsUnknownSchemaHistory(): Unit = withConnection { connection =>
    val statement = connection.createStatement()
    try
      statement.executeUpdate("CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL, description TEXT NOT NULL)")
      statement.executeUpdate("INSERT INTO schema_version (version, applied_at, description) VALUES (2, 0, 'unknown')")
      assertThrows(classOf[IllegalStateException], () => SchemaManager.applyMigrations(connection))
    finally statement.close()
  }

  /** 验证 schema 在数据库层拒绝不是 16 字节的 UUID BLOB。 */
  @Test
  def rejectsUuidBlobWithWrongLength(): Unit = withConnection { connection =>
    SchemaManager.applyMigrations(connection)
    val sql = "INSERT INTO floating_pod_state (uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    val statement = connection.prepareStatement(sql)
    try
      statement.setBytes(1, Array.fill[Byte](15)(0))
      statement.setLong(2, 0L)
      statement.setString(3, "overworld")
      statement.setDouble(4, 0.0D)
      statement.setDouble(5, 64.0D)
      statement.setDouble(6, 0.0D)
      statement.setDouble(7, 0.0D)
      statement.setDouble(8, 0.0D)
      statement.setInt(9, 0)
      statement.setLong(10, 0L)
      assertThrows(classOf[SQLException], () => statement.executeUpdate())
    finally statement.close()
  }

  /** 验证缺少 schema_version 的既有 FloatingPod 表不会被当作新数据库继续迁移。 */
  @Test
  def rejectsManagedTableWithoutSchemaVersion(): Unit = withConnection { connection =>
    val statement = connection.createStatement()
    try
      statement.executeUpdate("CREATE TABLE floating_pod_state (uuid BLOB PRIMARY KEY NOT NULL)")
      val failure = assertThrows(classOf[IllegalStateException], () => SchemaManager.applyMigrations(connection))
      assertTrue(failure.getMessage.contains("floating_pod_state"))
      assertFalse(tableExists(connection, "schema_version"))
    finally statement.close()
  }

  /** 验证同名 schema_version view 不会被误认为有效版本表或新数据库。 */
  @Test
  def rejectsSchemaVersionViewWithoutSchemaVersionTable(): Unit = withConnection { connection =>
    val statement = connection.createStatement()
    try
      statement.executeUpdate("CREATE VIEW schema_version AS SELECT 1 AS version")
      val failure = assertThrows(classOf[IllegalStateException], () => SchemaManager.applyMigrations(connection))
      assertTrue(failure.getMessage.contains("schema_version"))
      assertFalse(tableExists(connection, "floating_pod_state"))
    finally statement.close()
  }

  /** 验证 V003 保留可由 S4 raw recovery 修复的遗留活动空维度，并约束后续生命周期绕过。 */
  @Test
  def preservesLegacyActiveNullWhileInstallingLifecycleTriggers(): Unit = withConnection { connection =>
    applyV001Only(connection)
    applyV002Only(connection)
    val legacy = UUID.fromString("00000000-0000-0000-0000-000000000024")
    insertLegacyState(connection, legacy.toString, None)
    val activateLegacy = connection.prepareStatement("UPDATE floating_pod_state SET is_active = 1 WHERE uuid = ?")
    try
      activateLegacy.setBytes(1, uuidBytes(legacy))
      activateLegacy.executeUpdate()
    finally activateLegacy.close()

    SchemaManager.applyMigrations(connection)
    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertEquals(1, countRows(connection, "SELECT COUNT(*) FROM floating_pod_state WHERE uuid = ? AND is_active = 1 AND dimension_id IS NULL", legacy))
    assertEquals("WORLD_IDLE", operatingMode(connection, legacy))
    assertThrows(classOf[SQLException], () => executeSql(connection,
      "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode) VALUES (X'00000000000000000000000000000025', X'11111111111111111111111111111111', 0, 'overworld', 0, 64, 0, 0, 0, 1, 1, 7, 'NORMAL')"))
    assertThrows(classOf[SQLException], () => execute(connection, "UPDATE floating_pod_state SET world_name = 'blocked' WHERE uuid = ?", legacy))
    assertThrows(classOf[SQLException], () => execute(connection, "DELETE FROM floating_pod_state WHERE uuid = ?", legacy))
    assertEquals(1, execute(connection, "UPDATE floating_pod_state SET dimension_id = 7 WHERE uuid = ?", legacy))
    assertThrows(classOf[SQLException], () => execute(connection, "UPDATE floating_pod_state SET operating_mode = 'NORMAL' WHERE uuid = ?", legacy))
    assertEquals(1, execute(connection, "UPDATE floating_pod_state SET is_active = 0 WHERE uuid = ?", legacy))
  }

  @Test
  def installsPhysicalRecoveryObligationsWithoutAutoseedingRows(): Unit = withConnection { connection =>
    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertTrue(tableExists(connection, "physical_recovery_binding"))
    assertTrue(tableExists(connection, "physical_recovery_locator"))
    assertTrue(tableExists(connection, "physical_recovery_locator_chunk"))
    assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_binding"))
    assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator"))
    assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM physical_recovery_locator_chunk"))

    assertThrows(classOf[SQLException], () => executeSql(connection, "INSERT INTO physical_recovery_binding (singleton_fixed1, binding_uuid, cutover_state, format_version) VALUES (1, X'00', 'CERTIFIED', 1)"))
    assertThrows(classOf[SQLException], () => executeSql(connection, "INSERT INTO physical_recovery_binding (singleton_fixed1, binding_uuid, cutover_state, format_version) VALUES (1, X'00000000000000000000000000000001', 'UNKNOWN', 1)"))
    assertThrows(classOf[SQLException], () => executeSql(connection, "INSERT INTO physical_recovery_binding (singleton_fixed1, binding_uuid, cutover_state, format_version) VALUES (1, X'00000000000000000000000000000001', 'CERTIFIED', 0)"))

    val pod = UUID.fromString("00000000-0000-0000-0000-000000000031")
    val attempt = UUID.fromString("00000000-0000-0000-0000-000000000032")
    assertThrows(classOf[SQLException], () => insertLocator(connection, pod, attempt, "UNKNOWN", "reason", None, None))
    assertThrows(classOf[SQLException], () => insertLocator(connection, pod, attempt, "ROW_ACTIVE", "   ", None, None))
    assertThrows(classOf[SQLException], () => insertLocator(connection, pod, attempt, "ROW_ACTIVE", "reason", Some(-1L), None))
    insertLocator(connection, pod, attempt, "ROW_INACTIVE_VERIFIED", "fresh_boot", Some(9L), Some(7))
    assertThrows(classOf[SQLException], () => insertLocator(connection, UUID.fromString("00000000-0000-0000-0000-000000000033"), attempt, "ROW_ACTIVE", "duplicate_attempt", None, None))

    executeSql(connection, "PRAGMA foreign_keys = ON")
    insertChunk(connection, pod, 7, 2, -1)
    assertEquals("NO ACTION", foreignKeyDeleteAction(connection, "physical_recovery_locator_chunk"))
    assertThrows(classOf[SQLException], () => execute(connection, "DELETE FROM physical_recovery_locator WHERE pod_uuid = ?", pod))
  }

  @Test
  def v006AddsDurableUnboundIntentStorage(): Unit = withConnection { connection =>
    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertTrue(tableExists(connection, "physical_recovery_intent"))
    assertTrue(tableExists(connection, "physical_recovery_locator_chunk"))
    assertTrue(columnExists(connection, "physical_recovery_intent", "catalog_generation"))
    assertTrue(columnExists(connection, "physical_recovery_intent", "create_operation_uuid"))
  }

  @Test
  def rebuildsV005LocatorParentAndChildWhilePreservingEveryExistingRowAndForeignKey(): Unit = withConnection { connection =>
    createV005LocatorFixture(connection)

    SchemaManager.applyMigrations(connection)

    assertEquals(6, currentVersion(connection))
    assertEquals(
      List(
        "00000000000000000000000000000041|00000000000000000000000000000051|CREATE_PREPARED|create_prepared|4|7",
        "00000000000000000000000000000042|00000000000000000000000000000052|LEGACY_UNCERTIFIED|legacy_source|9|-2147483648"
      ),
      locatorRows(connection)
    )
    assertEquals(
      List(
        "00000000000000000000000000000041|7|-2|3",
        "00000000000000000000000000000041|7|-1|3",
        "00000000000000000000000000000042|-1|8|-4"
      ),
      locatorChunkRows(connection)
    )
    assertEquals("NO ACTION", foreignKeyDeleteAction(connection, "physical_recovery_locator_chunk"))
    assertEquals(0, countRows(connection, "SELECT COUNT(*) FROM pragma_foreign_key_check"))
    assertTrue(indexExists(connection, "idx_physical_recovery_locator_chunk_dimension"))
    assertThrows(classOf[SQLException], () => execute(connection, "DELETE FROM physical_recovery_locator WHERE pod_uuid = ?", UUID.fromString("00000000-0000-0000-0000-000000000041")))
  }

  private def withConnection(body: Connection => Unit): Unit =
    val connection = DriverManager.getConnection("jdbc:sqlite::memory:")
    try body(connection)
    finally connection.close()

  private def schemaVersionCount(connection: Connection): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT COUNT(*) FROM schema_version")
      try
        resultSet.next()
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def currentVersion(connection: Connection): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT MAX(version) FROM schema_version")
      try
        resultSet.next()
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

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

  private def applyV001Only(connection: Connection): Unit =
    val script = new String(
      Option(getClass.getResourceAsStream("/db/migration/V001__initial_schema.sql")).get.readAllBytes(),
      StandardCharsets.UTF_8
    )
    script.split(';').iterator.map(_.trim).filter(_.nonEmpty).foreach { sql =>
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    }

  private def applyV002Only(connection: Connection): Unit =
    val script = new String(
      Option(getClass.getResourceAsStream("/db/migration/V002__activity_transition_authority.sql")).get.readAllBytes(),
      StandardCharsets.UTF_8
    )
    script.split(';').iterator.map(_.trim).filter(_.nonEmpty).foreach { sql =>
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    }

  private def insertLegacyState(connection: Connection, uuid: String, owner: Option[String]): Unit =
    val sql = "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated) VALUES (?, ?, 0, 'overworld', 0, 64, 0, 0, 0, 0, 0)"
    val statement = connection.prepareStatement(sql)
    try
      statement.setBytes(1, uuidBytes(UUID.fromString(uuid)))
      owner match
        case Some(value) => statement.setBytes(2, uuidBytes(UUID.fromString(value)))
        case None => statement.setNull(2, Types.BLOB)
      statement.executeUpdate()
    finally statement.close()

  private def entitlementCount(connection: Connection): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT COUNT(*) FROM player_entitlement")
      try
        resultSet.next()
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def entitlementFor(connection: Connection, playerUuid: UUID): (Int, Int) =
    val statement = connection.prepareStatement("SELECT registration_limit, simultaneous_active_limit FROM player_entitlement WHERE player_uuid = ?")
    try
      statement.setBytes(1, uuidBytes(playerUuid))
      val resultSet = statement.executeQuery()
      try
        assertTrue(resultSet.next())
        (resultSet.getInt(1), resultSet.getInt(2))
      finally resultSet.close()
    finally statement.close()

  private def operatingMode(connection: Connection, podUuid: UUID): String =
    val statement = connection.prepareStatement("SELECT operating_mode FROM floating_pod_state WHERE uuid = ?")
    try
      statement.setBytes(1, uuidBytes(podUuid))
      val resultSet = statement.executeQuery()
      try
        assertTrue(resultSet.next())
        resultSet.getString(1)
      finally resultSet.close()
    finally statement.close()

  private def columnExists(connection: Connection, tableName: String, columnName: String): Boolean =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(s"PRAGMA table_info($tableName)")
      try
        Iterator.continually(resultSet.next()).takeWhile(identity).exists(_ => columnName == resultSet.getString("name"))
      finally resultSet.close()
    finally statement.close()

  private def uuidBytes(uuid: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()

  private def execute(connection: Connection, sql: String, uuid: UUID): Int =
    val statement = connection.prepareStatement(sql)
    try
      statement.setBytes(1, uuidBytes(uuid))
      statement.executeUpdate()
    finally statement.close()

  private def countRows(connection: Connection, sql: String, uuid: UUID): Int =
    val statement = connection.prepareStatement(sql)
    try
      statement.setBytes(1, uuidBytes(uuid))
      val resultSet = statement.executeQuery()
      try
        resultSet.next()
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def countRows(connection: Connection, sql: String): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(sql)
      try
        resultSet.next()
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def executeSql(connection: Connection, sql: String): Int =
    val statement = connection.createStatement()
    try statement.executeUpdate(sql)
    finally statement.close()

  private def insertLocator(connection: Connection, podUuid: UUID, attemptUuid: UUID, state: String, reason: String, domainVersion: Option[Long], dimensionId: Option[Int]): Unit =
    val statement = connection.prepareStatement("INSERT INTO physical_recovery_locator (pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id) VALUES (?, ?, ?, ?, ?, ?)")
    try
      statement.setBytes(1, uuidBytes(podUuid))
      statement.setBytes(2, uuidBytes(attemptUuid))
      statement.setString(3, state)
      statement.setString(4, reason)
      domainVersion.fold(statement.setNull(5, Types.INTEGER))(statement.setLong(5, _))
      dimensionId.fold(statement.setNull(6, Types.INTEGER))(statement.setInt(6, _))
      statement.executeUpdate()
    finally statement.close()

  private def insertChunk(connection: Connection, podUuid: UUID, dimensionId: Int, chunkX: Int, chunkZ: Int): Unit =
    val statement = connection.prepareStatement("INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z) VALUES (?, ?, ?, ?)")
    try
      statement.setBytes(1, uuidBytes(podUuid))
      statement.setInt(2, dimensionId)
      statement.setInt(3, chunkX)
      statement.setInt(4, chunkZ)
      statement.executeUpdate()
    finally statement.close()

  private def foreignKeyDeleteAction(connection: Connection, tableName: String): String =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(s"PRAGMA foreign_key_list($tableName)")
      try
        assertTrue(resultSet.next())
        resultSet.getString("on_delete")
      finally resultSet.close()
    finally statement.close()

  private def createV005LocatorFixture(connection: Connection): Unit =
    executeSql(connection, "PRAGMA foreign_keys = ON")
    executeSql(connection, "CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL, description TEXT NOT NULL)")
    (1 to 5).foreach(version => execute(connection, "INSERT INTO schema_version (version, applied_at, description) VALUES (?, 10, ?)", version, s"V$version"))
    executeSql(connection, "CREATE TABLE floating_pod_state (uuid BLOB PRIMARY KEY NOT NULL, owner_uuid BLOB, state_version INTEGER NOT NULL, world_name TEXT NOT NULL, pos_x REAL NOT NULL, pos_y REAL NOT NULL, pos_z REAL NOT NULL, yaw REAL NOT NULL, pitch REAL NOT NULL, is_active INTEGER NOT NULL, last_updated INTEGER NOT NULL, dimension_id INTEGER, operating_mode TEXT NOT NULL)")
    executeSql(connection, "CREATE TABLE physical_recovery_binding (singleton_fixed1 INTEGER PRIMARY KEY NOT NULL, binding_uuid BLOB NOT NULL UNIQUE, cutover_state TEXT NOT NULL, format_version INTEGER NOT NULL)")
    executeSql(connection, "CREATE TABLE physical_recovery_locator (pod_uuid BLOB PRIMARY KEY NOT NULL, attempt_uuid BLOB NOT NULL UNIQUE, locator_state TEXT NOT NULL, reason_code TEXT NOT NULL, domain_version INTEGER, target_dimension_id INTEGER)")
    executeSql(connection, "CREATE TABLE physical_recovery_locator_chunk (pod_uuid BLOB NOT NULL, dimension_id INTEGER NOT NULL, chunk_x INTEGER NOT NULL, chunk_z INTEGER NOT NULL, PRIMARY KEY(pod_uuid, dimension_id, chunk_x, chunk_z), FOREIGN KEY(pod_uuid) REFERENCES physical_recovery_locator(pod_uuid))")
    executeSql(connection, "CREATE INDEX idx_physical_recovery_locator_chunk_dimension ON physical_recovery_locator_chunk(dimension_id, chunk_x, chunk_z)")
    executeSql(connection, "INSERT INTO physical_recovery_binding (singleton_fixed1, binding_uuid, cutover_state, format_version) VALUES (1, X'00000000000000000000000000000061', 'CERTIFIED', 1)")
    executeSql(connection, "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode) VALUES (X'00000000000000000000000000000041', NULL, 4, 'overworld', 1, 64, 2, 3, 4, 0, 100, 7, 'WORLD_IDLE')")
    executeSql(connection, "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode) VALUES (X'00000000000000000000000000000042', NULL, 9, 'the_end', -1, 80, 8, 90, -15, 0, 200, -2147483648, 'NORMAL')")
    executeSql(connection, "INSERT INTO physical_recovery_locator VALUES (X'00000000000000000000000000000041', X'00000000000000000000000000000051', 'CREATE_PREPARED', 'create_prepared', 4, 7)")
    executeSql(connection, "INSERT INTO physical_recovery_locator VALUES (X'00000000000000000000000000000042', X'00000000000000000000000000000052', 'LEGACY_UNCERTIFIED', 'legacy_source', 9, -2147483648)")
    executeSql(connection, "INSERT INTO physical_recovery_locator_chunk VALUES (X'00000000000000000000000000000041', 7, -2, 3)")
    executeSql(connection, "INSERT INTO physical_recovery_locator_chunk VALUES (X'00000000000000000000000000000041', 7, -1, 3)")
    executeSql(connection, "INSERT INTO physical_recovery_locator_chunk VALUES (X'00000000000000000000000000000042', -1, 8, -4)")

  private def locatorRows(connection: Connection): List[String] =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT lower(hex(pod_uuid)), lower(hex(attempt_uuid)), locator_state, reason_code, domain_version, target_dimension_id FROM physical_recovery_locator ORDER BY pod_uuid")
      try
        val rows = List.newBuilder[String]
        while resultSet.next() do rows += s"${resultSet.getString(1)}|${resultSet.getString(2)}|${resultSet.getString(3)}|${resultSet.getString(4)}|${resultSet.getString(5)}|${resultSet.getString(6)}"
        rows.result()
      finally resultSet.close()
    finally statement.close()

  private def locatorChunkRows(connection: Connection): List[String] =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery("SELECT lower(hex(pod_uuid)), dimension_id, chunk_x, chunk_z FROM physical_recovery_locator_chunk ORDER BY pod_uuid, dimension_id, chunk_x, chunk_z")
      try
        val rows = List.newBuilder[String]
        while resultSet.next() do rows += s"${resultSet.getString(1)}|${resultSet.getInt(2)}|${resultSet.getInt(3)}|${resultSet.getInt(4)}"
        rows.result()
      finally resultSet.close()
    finally statement.close()

  private def indexExists(connection: Connection, indexName: String): Boolean =
    val statement = connection.prepareStatement("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?")
    try
      statement.setString(1, indexName)
      val resultSet = statement.executeQuery()
      try
        resultSet.next()
        resultSet.getInt(1) == 1
      finally resultSet.close()
    finally statement.close()

  private def execute(connection: Connection, sql: String, version: Int, description: String): Int =
    val statement = connection.prepareStatement(sql)
    try
      statement.setInt(1, version)
      statement.setString(2, description)
      statement.executeUpdate()
    finally statement.close()
