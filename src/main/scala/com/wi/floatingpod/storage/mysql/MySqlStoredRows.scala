package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.StoredPodRow

import java.nio.{ByteBuffer, ByteOrder}
import java.util.UUID

/** JDBC 层已按列读出的不可变 scalar snapshot；SQL NULL 由 Option 原样保留。 */
final case class MySqlStoredRowData(
    uuidBytes: Vector[Byte],
    ownerUuidBytes: Option[Vector[Byte]],
    stateVersion: Long,
    worldName: String,
    posX: Double,
    posY: Double,
    posZ: Double,
    yaw: Double,
    pitch: Double,
    isActiveFlag: Int,
    lastUpdated: Long,
    dimensionId: Option[Int],
    operatingModeToken: String
):
  require(uuidBytes != null, "uuidBytes must not be null")
  require(ownerUuidBytes != null && ownerUuidBytes.forall(_ != null), "ownerUuidBytes option must not be null")
  require(dimensionId != null, "dimensionId option must not be null")
  require(worldName != null, "worldName must not be null")
  require(operatingModeToken != null, "operatingModeToken must not be null")

/** 将 MySQL scalar 值转成 raw recovery row 或公开领域状态，不执行 JDBC I/O。 */
object MySqlStoredRows:
  private val UuidByteLength = 16

  /** UUID 使用 MSB→LSB 网络顺序，不进行 MySQL UUID swap。 */
  def encodeUuid(uuid: UUID): Vector[Byte] =
    require(uuid != null, "uuid must not be null")
    val buffer = ByteBuffer.allocate(UuidByteLength).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(uuid.getMostSignificantBits)
    buffer.putLong(uuid.getLeastSignificantBits)
    buffer.array().toVector

  def decodeUuid(bytes: Vector[Byte], column: String): UUID =
    require(bytes != null, "UUID bytes must not be null")
    require(column != null && column.nonEmpty, "UUID column label must not be blank")
    if bytes.size != UuidByteLength then
      throw new IllegalStateException(s"MySQL column $column does not contain a 16-byte UUID")
    val buffer = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.BIG_ENDIAN)
    new UUID(buffer.getLong(), buffer.getLong())

  /** SQL NULL 与全零 UUID 是不同值。 */
  def decodeNullableOwnerUuid(bytes: Option[Vector[Byte]]): Option[UUID] =
    require(bytes != null, "owner UUID option must not be null")
    bytes.map(decodeUuid(_, "owner_uuid"))

  /** 保留 JDBC nullable INT 的 NULL 与包括 Int.MinValue 在内的真实值。 */
  def decodeNullableDimension(value: java.lang.Integer): Option[Int] =
    Option(value).map(_.intValue())

  def materializeRawRow(columns: MySqlStoredRowData): StoredPodRow =
    require(columns != null, "stored row columns must not be null")
    StoredPodRow(
      uuid = decodeUuid(columns.uuidBytes, "uuid"),
      ownerUuid = decodeNullableOwnerUuid(columns.ownerUuidBytes),
      stateVersion = columns.stateVersion,
      worldName = columns.worldName,
      posX = columns.posX,
      posY = columns.posY,
      posZ = columns.posZ,
      yaw = exactDomainFloat(columns.yaw, "yaw"),
      pitch = exactDomainFloat(columns.pitch, "pitch"),
      isActive = activeFlag(columns.isActiveFlag),
      lastUpdated = columns.lastUpdated,
      dimensionId = columns.dimensionId,
      operatingMode = PodOperatingMode.fromDatabase(columns.operatingModeToken)
    )

  def materializeGenericState(columns: MySqlStoredRowData): FloatingPodState =
    materializeRawRow(columns).toPublicState

  /** Raw recovery rows保留活动空维度，供恢复协调器自行作显式决策。 */
  def materializeRecoveryRows(rows: Iterable[MySqlStoredRowData]): List[StoredPodRow] =
    require(rows != null, "stored recovery rows must not be null")
    rows.iterator.map(materializeRawRow).toList

  /** 只有满足领域状态不变量的 raw rows 才能进入常规恢复结果。 */
  def materializeRecoverableStates(rows: Iterable[MySqlStoredRowData]): List[FloatingPodState] =
    materializeRecoveryRows(rows).map(_.toPublicState)

  private def activeFlag(value: Int): Boolean =
    value match
      case 0 => false
      case 1 => true
      case _ => throw new IllegalStateException("MySQL column is_active must be 0 or 1")

  private def exactDomainFloat(value: Double, column: String): Float =
    val converted = value.toFloat
    if !java.lang.Double.isFinite(value) || !java.lang.Float.isFinite(converted) || converted.toDouble != value then
      throw new IllegalStateException(s"MySQL column $column cannot be represented exactly by the FloatingPod domain")
    converted
