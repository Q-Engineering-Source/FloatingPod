package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.sql.{SQLDataException, Types}
import java.util.UUID
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}

class MySqlBusinessRowsTest:
  private val uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")

  @Test
  def jdbcRawPodPreservesNullOwnerDimensionExactPoseAndWorldText(): Unit =
    val row = podRow(None, None)
    try
      val raw = MySqlBusinessRows.readPod(row)
      assertEquals(MySqlStoredRows.encodeUuid(uuid), raw.uuidBytes)
      assertEquals(None, raw.ownerUuidBytes)
      assertEquals(None, raw.dimensionId)
      assertEquals(" world 名称 ", raw.worldName)
      assertEquals(0.10000000000000002D, raw.posX)
      assertEquals(Long.MaxValue, raw.lastUpdated)
      assertEquals("WORLD_IDLE", raw.operatingModeToken)
      assertEquals(None, MySqlStoredRows.materializeRawRow(raw).dimensionId)
      assertThrows(classOf[IllegalStateException], () => MySqlStoredRows.materializeGenericState(raw))
    finally row.close()

  @Test
  def jdbcZeroUuidAndMinimumDimensionAreNotSqlNull(): Unit =
    val row = podRow(Some(new UUID(0L, 0L)), Some(Int.MinValue))
    try
      val raw = MySqlBusinessRows.readPod(row)
      assertEquals(Some(Vector.fill(16)(0.toByte)), raw.ownerUuidBytes)
      assertEquals(Some(Int.MinValue), raw.dimensionId)
    finally row.close()

  @Test
  def jdbcEntitlementKeepsExplicitZeroAndRejectsRequiredNull(): Unit =
    val row = rows(Vector(Types.VARBINARY, Types.INTEGER, Types.INTEGER))
    try
      row.moveToInsertRow()
      row.updateBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      row.updateInt(2, 0)
      row.updateInt(3, 7)
      row.insertRow()
      row.moveToCurrentRow()
      row.beforeFirst()
      assertTrue(row.next())
      val entitlement = MySqlBusinessRows.readEntitlement(row)
      assertEquals(uuid, entitlement.playerUuid)
      assertEquals(0, entitlement.registrationLimit)
      assertEquals(7, entitlement.simultaneousActiveLimit)
      row.updateNull(2)
      assertThrows(classOf[SQLDataException], () => MySqlBusinessRows.readEntitlement(row))
    finally row.close()

  @Test
  def malformedUuidFailsAtJdbcBoundary(): Unit =
    val row = podRow(None, None)
    try
      row.updateBytes(1, Array.fill[Byte](15)(0))
      assertThrows(classOf[SQLDataException], () => MySqlBusinessRows.readPod(row))
    finally row.close()

  @Test
  def duplicateSingletonIsAnExplicitFailure(): Unit =
    assertEquals(None, MySqlBusinessRows.single(Vector.empty[Int]))
    assertEquals(Some(1), MySqlBusinessRows.single(Vector(1)))
    assertThrows(classOf[SQLDataException], () => MySqlBusinessRows.single(Vector(1, 2)))

  @Test
  def resourceCleanupPreservesPrimaryAndBothSuppressedCauses(): Unit =
    val primary = new IllegalStateException("primary")
    val resultClose = new IllegalStateException("result close")
    val statementClose = new IllegalStateException("statement close")
    val thrown = assertThrows(classOf[IllegalStateException], () =>
      MySqlBusinessRows.using(new FailingClose(statementClose)) { _ =>
        MySqlBusinessRows.using(new FailingClose(resultClose)) { _ => throw primary }
      })
    assertSame(primary, thrown)
    assertEquals(Vector(resultClose, statementClose), thrown.getSuppressed.toVector)

  @Test
  def successfulBodyDoesNotHideCloseFailure(): Unit =
    val cleanup = new IllegalStateException("close")
    val thrown = assertThrows(classOf[IllegalStateException], () => MySqlBusinessRows.using(new FailingClose(cleanup))(_ => 1))
    assertSame(cleanup, thrown)

  private final class FailingClose(failure: Throwable) extends AutoCloseable:
    override def close(): Unit = throw failure

  private def rows(types: Vector[Int]): CachedRowSet =
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(types.size)
    types.zipWithIndex.foreach { (kind, index) =>
      metadata.setColumnType(index + 1, kind)
      metadata.setColumnName(index + 1, s"column${index + 1}")
    }
    val row = RowSetProvider.newFactory().createCachedRowSet()
    row.setMetaData(metadata)
    row

  private def podRow(owner: Option[UUID], dimension: Option[Int]): CachedRowSet =
    val row = rows(Vector(Types.VARBINARY, Types.VARBINARY, Types.BIGINT, Types.LONGVARCHAR,
      Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.TINYINT,
      Types.BIGINT, Types.INTEGER, Types.VARCHAR))
    row.moveToInsertRow()
    row.updateBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
    owner match
      case Some(value) => row.updateBytes(2, MySqlStoredRows.encodeUuid(value).toArray)
      case None => row.updateNull(2)
    row.updateLong(3, 1L)
    row.updateString(4, " world 名称 ")
    row.updateDouble(5, 0.10000000000000002D)
    row.updateDouble(6, -10D)
    row.updateDouble(7, 4096.125D)
    row.updateDouble(8, 72.375D)
    row.updateDouble(9, -13.125D)
    row.updateInt(10, 1)
    row.updateLong(11, Long.MaxValue)
    dimension match
      case Some(value) => row.updateInt(12, value)
      case None => row.updateNull(12)
    row.updateString(13, "WORLD_IDLE")
    row.insertRow()
    row.moveToCurrentRow()
    row.beforeFirst()
    assertTrue(row.next())
    row
