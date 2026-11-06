package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement

import java.sql.{Connection, ResultSet, SQLDataException}
import java.util.UUID

/** 仅在调用方已准入的同一连接/线程上读业务行；不取得 fence，不改变事务或连接生命周期。 */
final class MySqlBusinessRows(connection: Connection, schema: String):
  require(connection != null, "caller-owned connection is required")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "fixed schema identifier is unsafe")
  private val ownerThread = Thread.currentThread()
  private val pods = s"`$schema`.`floating_pod_state`"
  private val entitlements = s"`$schema`.`player_entitlement`"
  private val podColumns = "uuid,owner_uuid,state_version,world_name,pos_x,pos_y,pos_z,yaw,pitch,is_active,last_updated,dimension_id,operating_mode"

  def findPod(uuid: UUID): Option[MySqlStoredRowData] =
    MySqlBusinessRows.single(query(s"SELECT $podColumns FROM $pods WHERE uuid=? ORDER BY uuid", Some(uuid))(MySqlBusinessRows.readPod))

  def findPodsByOwner(ownerUuid: UUID): Vector[MySqlStoredRowData] =
    query(s"SELECT $podColumns FROM $pods WHERE owner_uuid=? ORDER BY uuid", Some(ownerUuid))(MySqlBusinessRows.readPod)

  def readAllPods(): Vector[MySqlStoredRowData] =
    query(s"SELECT $podColumns FROM $pods ORDER BY uuid", None)(MySqlBusinessRows.readPod)

  def readActivePods(): Vector[MySqlStoredRowData] =
    query(s"SELECT $podColumns FROM $pods WHERE is_active=1 ORDER BY uuid", None)(MySqlBusinessRows.readPod)

  def findEntitlement(playerUuid: UUID): Option[PlayerEntitlement] =
    MySqlBusinessRows.single(query(s"SELECT player_uuid,registration_limit,simultaneous_active_limit FROM $entitlements WHERE player_uuid=? ORDER BY player_uuid", Some(playerUuid))(MySqlBusinessRows.readEntitlement))

  def readAllEntitlements(): Vector[PlayerEntitlement] =
    query(s"SELECT player_uuid,registration_limit,simultaneous_active_limit FROM $entitlements ORDER BY player_uuid", None)(MySqlBusinessRows.readEntitlement)

  private def query[A](sql: String, parameter: Option[UUID])(decode: ResultSet => A): Vector[A] =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("MySQL business read requires its caller-owned thread")
    parameter.foreach(uuid => require(uuid != null, "query UUID is required"))
    MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
      parameter.foreach(uuid => statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray))
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        val rows = Vector.newBuilder[A]
        while result.next() do rows += decode(result)
        rows.result()
      }
    }

object MySqlBusinessRows:
  /** 不以 LIMIT 1 隐藏主键/连接协议异常。 */
  private[mysql] def single[A](rows: Vector[A]): Option[A] =
    if rows.size > 1 then throw new SQLDataException("MySQL singleton business query returned duplicate rows")
    rows.headOption

  private[mysql] def readPod(row: ResultSet): MySqlStoredRowData =
    MySqlStoredRowData(
      uuidBytes = uuidBytes(row, 1, "uuid", nullable = false).get,
      ownerUuidBytes = uuidBytes(row, 2, "owner_uuid", nullable = true),
      stateVersion = requiredLong(row, 3, "state_version"),
      worldName = requiredText(row, 4, "world_name"),
      posX = requiredDouble(row, 5, "pos_x"),
      posY = requiredDouble(row, 6, "pos_y"),
      posZ = requiredDouble(row, 7, "pos_z"),
      yaw = requiredDouble(row, 8, "yaw"),
      pitch = requiredDouble(row, 9, "pitch"),
      isActiveFlag = requiredInt(row, 10, "is_active"),
      lastUpdated = requiredLong(row, 11, "last_updated"),
      dimensionId = nullableInt(row, 12),
      operatingModeToken = requiredText(row, 13, "operating_mode")
    )

  private[mysql] def readEntitlement(row: ResultSet): PlayerEntitlement =
    val uuid = MySqlStoredRows.decodeUuid(uuidBytes(row, 1, "player_uuid", nullable = false).get, "player_uuid")
    PlayerEntitlement(uuid, requiredInt(row, 2, "registration_limit"), requiredInt(row, 3, "simultaneous_active_limit"))

  private def uuidBytes(row: ResultSet, index: Int, label: String, nullable: Boolean): Option[Vector[Byte]] =
    val bytes = row.getBytes(index)
    // getBytes 的 SQL NULL 契约直接返回 null；wasNull 用于下方 primitive getters。
    // CachedRowSet 的二进制 getter 不更新 wasNull，不能借上一次 getter 的标志判 UUID。
    if bytes == null then
      if !nullable then missing(label)
      None
    else
      if bytes.length != 16 then throw new SQLDataException(s"MySQL column $label requires exactly 16 UUID bytes")
      Some(bytes.toVector)

  private def requiredText(row: ResultSet, index: Int, label: String): String =
    val value = row.getString(index)
    if row.wasNull() || value == null then missing(label)
    value

  private def requiredLong(row: ResultSet, index: Int, label: String): Long =
    val value = row.getLong(index)
    if row.wasNull() then missing(label)
    value

  private def requiredInt(row: ResultSet, index: Int, label: String): Int =
    val value = row.getInt(index)
    if row.wasNull() then missing(label)
    value

  private def nullableInt(row: ResultSet, index: Int): Option[Int] =
    val value = row.getInt(index)
    if row.wasNull() then None else Some(value)

  private def requiredDouble(row: ResultSet, index: Int, label: String): Double =
    val value = row.getDouble(index)
    if row.wasNull() then missing(label)
    value

  private def missing(label: String): Nothing =
    throw new SQLDataException(s"MySQL required business column $label is SQL NULL")

  /** 清理异常从属原始失败；两层 using 保留 ResultSet 与 Statement 的关闭失败。 */
  private[mysql] def using[R <: AutoCloseable, A](resource: R)(body: R => A): A =
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
        catch
          case cleanup: Throwable =>
            if cleanup ne primary then primary.addSuppressed(cleanup)
