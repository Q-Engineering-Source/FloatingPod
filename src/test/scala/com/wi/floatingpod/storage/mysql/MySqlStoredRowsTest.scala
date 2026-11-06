package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PodOperatingMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID

class MySqlStoredRowsTest:
  @Test
  def uuidCodecUsesSixteenBigEndianBytesWithoutSwap(): Unit =
    val uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")
    val expected = List(0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff)
      .map(_.toByte)
      .toVector

    assertEquals(expected, MySqlStoredRows.encodeUuid(uuid))
    assertEquals(uuid, MySqlStoredRows.decodeUuid(expected, "uuid"))
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlStoredRows.decodeUuid(expected.take(15), "uuid")
    )

  @Test
  def nullableOwnerRemainsDistinctFromTheAllZeroUuid(): Unit =
    val unowned = MySqlStoredRows.materializeRawRow(row(ownerUuidBytes = None))
    val ownedByZeroUuid = MySqlStoredRows.materializeRawRow(
      row(ownerUuidBytes = Some(Vector.fill(16)(0.toByte)))
    )

    assertEquals(None, unowned.ownerUuid)
    assertEquals(Some(new UUID(0L, 0L)), ownedByZeroUuid.ownerUuid)

  @Test
  def rawRecoveryRowPreservesActiveNullDimensionAndPublicMaterializationRejectsIt(): Unit =
    val raw = MySqlStoredRows.materializeRawRow(row(isActive = 1, dimensionId = None, operatingModeToken = "WORLD_IDLE"))

    assertTrue(raw.isActive)
    assertEquals(None, raw.dimensionId)
    assertThrows(classOf[IllegalStateException], () => MySqlStoredRows.materializeGenericState(row(isActive = 1, dimensionId = None, operatingModeToken = "WORLD_IDLE")))
    assertThrows(classOf[IllegalStateException], () => MySqlStoredRows.materializeRecoverableStates(List(row(isActive = 1, dimensionId = None, operatingModeToken = "WORLD_IDLE"))))

  @Test
  def unclaimedActiveWorldIdleMaterializesWithoutCreatingAnOwner(): Unit =
    val state = MySqlStoredRows.materializeGenericState(
      row(ownerUuidBytes = None, isActive = 1, dimensionId = Some(Int.MinValue), operatingModeToken = "WORLD_IDLE")
    )

    assertEquals(None, state.ownerUuid)
    assertTrue(state.isActive)
    assertEquals(Some(Int.MinValue), state.dimensionId)
    assertEquals(PodOperatingMode.WorldIdle, state.operatingMode)

  @Test
  def genericStateConversionRejectsAnActiveUnclaimedNormalRow(): Unit =
    val raw = MySqlStoredRows.materializeRawRow(
      row(ownerUuidBytes = None, isActive = 1, dimensionId = Some(0), operatingModeToken = "NORMAL")
    )

    assertEquals(None, raw.ownerUuid)
    assertEquals(PodOperatingMode.Normal, raw.operatingMode)
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlStoredRows.materializeGenericState(row(ownerUuidBytes = None, isActive = 1, dimensionId = Some(0), operatingModeToken = "NORMAL"))
    )

  @Test
  def preservesMillisecondTimeDoubleCoordinatesAndExactDomainFloatPrecision(): Unit =
    val state = MySqlStoredRows.materializeGenericState(
      row(
        posX = 0.10000000000000002D,
        posY = -4096.125D,
        posZ = 300000000.75D,
        yaw = (-72.375F).toDouble,
        pitch = 13.125F.toDouble,
        lastUpdated = 1700000000123L
      )
    )

    assertEquals(0.10000000000000002D, state.posX)
    assertEquals(-4096.125D, state.posY)
    assertEquals(300000000.75D, state.posZ)
    assertEquals(-72.375F, state.yaw)
    assertEquals(13.125F, state.pitch)
    assertEquals(1700000000123L, state.lastUpdated)

  @Test
  def rejectsValuesThatCannotBeRepresentedByExistingFloatDomainAngles(): Unit =
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlStoredRows.materializeRawRow(row(yaw = 0.1D))
    )
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlStoredRows.materializeRawRow(row(pitch = Double.PositiveInfinity))
    )

  @Test
  def validatesActiveFlagAndDatabaseModeTokensExactly(): Unit =
    assertThrows(classOf[IllegalStateException], () => MySqlStoredRows.materializeRawRow(row(isActive = 2)))
    assertThrows(classOf[IllegalStateException], () => MySqlStoredRows.materializeRawRow(row(operatingModeToken = "WORLDIDLE")))
    val normal = MySqlStoredRows.materializeGenericState(row(operatingModeToken = "NORMAL"))
    assertFalse(normal.isActive)
    assertEquals(PodOperatingMode.Normal, normal.operatingMode)

  private def row(
      uuidBytes: Vector[Byte] = MySqlStoredRows.encodeUuid(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")),
      ownerUuidBytes: Option[Vector[Byte]] = Some(MySqlStoredRows.encodeUuid(UUID.fromString("ffeeddcc-bbaa-9988-7766-554433221100"))),
      stateVersion: Long = 9L,
      worldName: String = "overworld",
      posX: Double = -0.125D,
      posY: Double = 73.5D,
      posZ: Double = 4096.25D,
      yaw: Double = 90.0D,
      pitch: Double = -15.0D,
      isActive: Int = 0,
      lastUpdated: Long = 1700000000123L,
      dimensionId: Option[Int] = None,
      operatingModeToken: String = "NORMAL"
  ): MySqlStoredRowData =
    MySqlStoredRowData(
      uuidBytes,
      ownerUuidBytes,
      stateVersion,
      worldName,
      posX,
      posY,
      posZ,
      yaw,
      pitch,
      isActive,
      lastUpdated,
      dimensionId,
      operatingModeToken
    )
  @Test
  def nullableDimensionKeepsSqlNullDistinctFromAllValidIntValues(): Unit =
    assertEquals(None, MySqlStoredRows.decodeNullableDimension(null))
    assertEquals(Some(Int.MinValue), MySqlStoredRows.decodeNullableDimension(Int.box(Int.MinValue)))
    assertEquals(Some(0), MySqlStoredRows.decodeNullableDimension(Int.box(0)))
