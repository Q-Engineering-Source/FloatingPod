package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.{SQLDataException, Types}
import java.util.UUID
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}

class MySqlBootstrapIntentRowsTest:
  private val zero = new UUID(0L, 0L)
  private val certify = new UUID(1L, 2L)
  private val digest = Vector.tabulate[Byte](32)(_.toByte)
  private val source = " v1|目录 原文\n尾端 "

  private def row(): CachedRowSet =
    val types = Vector(Types.TINYINT, Types.VARBINARY, Types.VARBINARY, Types.VARBINARY,
      Types.VARBINARY, Types.INTEGER, Types.VARBINARY, Types.LONGVARCHAR, Types.VARBINARY,
      Types.VARCHAR, Types.VARBINARY)
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(types.size)
    types.zipWithIndex.foreach { (kind, index) =>
      metadata.setColumnType(index + 1, kind)
      metadata.setColumnName(index + 1, s"c${index + 1}")
    }
    val result = RowSetProvider.newFactory().createCachedRowSet()
    result.setMetaData(metadata)
    result.moveToInsertRow()
    result.updateInt(1, 1)
    Vector(2, 3, 5, 7).foreach(index => result.updateBytes(index, MySqlStoredRows.encodeUuid(zero).toArray))
    result.updateBytes(4, MySqlStoredRows.encodeUuid(certify).toArray)
    result.updateInt(6, 2)
    result.updateString(8, source)
    result.updateBytes(9, digest.toArray)
    result.updateString(10, "PREPARED")
    result.updateNull(11)
    result.insertRow()
    result.moveToCurrentRow()
    result.beforeFirst()
    assertTrue(result.next())
    result

  private def withRow(test: CachedRowSet => Unit): Unit =
    val result = row()
    try test(result)
    finally result.close()

  @Test def preparedDecodeRetainsEveryFieldAndZeroUuidWithoutQualification(): Unit = withRow { result =>
    val actual = MySqlBootstrapIntentRows.decode(result)
    assertEquals(MySqlBootstrapIntentData(1, zero, zero, certify, zero, 2, zero, source, digest,
      MySqlBootstrapIntentStage.Prepared, None), actual)
    MySqlBootstrapIntentRows.validateGraph(Vector(actual))
    MySqlBootstrapIntentRows.validateGraph(Vector.empty)
  }

  @Test def completeDecodeRetainsExactMarkerDigest(): Unit = withRow { result =>
    result.updateString(10, "COMPLETE")
    result.updateBytes(11, digest.reverse.toArray)
    val actual = MySqlBootstrapIntentRows.decode(result)
    assertEquals(MySqlBootstrapIntentStage.Complete, actual.stage)
    assertEquals(Some(digest.reverse), actual.markerAckSha256)
    MySqlBootstrapIntentRows.validateGraph(Vector(actual))
  }

  @Test def requiredNullsAreNotZeroUuidOrZeroInt(): Unit =
    Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).foreach { index => withRow { result =>
      result.updateNull(index)
      assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.decode(result))
    }}

  @Test def binaryLengthsAndStageTokensFailAtDecoderWithoutSourcePayload(): Unit =
    Vector(2, 3, 4, 5, 7, 9, 11).foreach { index => withRow { result =>
      result.updateBytes(index, Array[Byte](1, 2, 3))
      val failure = assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.decode(result))
      assertFalse(failure.getMessage.contains(source))
    }}
    Vector("prepared", "COMMITTED", " COMPLETE ", "").foreach { token => withRow { result =>
      result.updateString(10, token)
      assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.decode(result))
    }}

  @Test def ddlNullableStageCombinationsRequireExplicitGraphValidation(): Unit = withRow { result =>
    val prepared = MySqlBootstrapIntentRows.decode(result)
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(prepared.copy(markerAckSha256 = Some(digest)))))
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(prepared.copy(stage = MySqlBootstrapIntentStage.Complete))))
    result.updateString(10, "COMPLETE")
    val completeWithoutAck = MySqlBootstrapIntentRows.decode(result)
    assertEquals(None, completeWithoutAck.markerAckSha256)
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(completeWithoutAck)))
  }

  @Test def graphChecksActualKeysVersionsAndWritePair(): Unit = withRow { result =>
    val actual = MySqlBootstrapIntentRows.decode(result)
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual, actual)))
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(singletonFixed1 = 0))))
    Vector(Int.MinValue, 0, 1, 3).foreach { version =>
      assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(bindingFormatVersion = version))))
    }
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(certifyWriteUuid = actual.prepareWriteUuid))))
    // DDL 不禁止 bootstrap operation 与 prepare write UUID 相等，不凭空增加 unique。
    assertEquals(actual.bootstrapOperationUuid, actual.prepareWriteUuid)
    MySqlBootstrapIntentRows.validateGraph(Vector(actual))
  }

  @Test def graphChecksDigestLengthAndMysqlSpaceTrimWithoutChangingSource(): Unit = withRow { result =>
    val actual = MySqlBootstrapIntentRows.decode(result)
    Vector("", " ", "   ").foreach { value =>
      assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(sourceWorldIdentity = value))))
    }
    MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(sourceWorldIdentity = "\t")))
    assertEquals(source, actual.sourceWorldIdentity)
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(provisionReceiptSha256 = Vector.empty))))
    assertThrows(classOf[SQLDataException], () => MySqlBootstrapIntentRows.validateGraph(Vector(actual.copy(stage = MySqlBootstrapIntentStage.Complete, markerAckSha256 = Some(Vector.empty)))))
  }
