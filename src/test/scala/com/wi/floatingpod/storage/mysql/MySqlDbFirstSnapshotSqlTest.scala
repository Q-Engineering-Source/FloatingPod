package com.wi.floatingpod.storage.mysql

import com.mysql.cj.MysqlType
import java.nio.charset.StandardCharsets
import java.sql.{ResultSetMetaData, SQLDataException, Types}
import javax.sql.rowset.RowSetMetaDataImpl
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlDbFirstSnapshotSqlTest:
  private val schema = "floatingpod"
  @Test def consistentObservationKeepsAllColumnsAndOrderWithoutTakingUncertainTransactionsLocks():Unit =
    MySqlDbFirstSnapshotSql.canonicalOrder.foreach { shape=>
      val locked=MySqlDbFirstSnapshotSql.select(schema,shape.table)
      val observation=MySqlDbFirstSnapshotSql.select(schema,shape.table,true)
      assertEquals(locked.substring(0,locked.lastIndexOf(" FOR ")),observation)
      assertEquals(observation.getBytes(StandardCharsets.UTF_8).length.toLong,
        MySqlDbFirstSnapshotSql.selectUtf8Length(schema,shape.table,true))
      MySqlDbFirstSnapshotSql.validateMetadata(metadata(shape.table),shape.table)
      assertEquals(locked,MySqlDbFirstSnapshotSql.select(schema,shape.table),"observation must not change execution SQL")
    }
    assertEquals("SELECT `binding_uuid`,`pod_uuid`,`owner_uuid`,`attempt_uuid`,`snapshot_format_version`,`nbt_bytes`,`nbt_sha256`,`limit_profile_encoding`,`limit_profile_bytes`,`limit_profile_sha256`,`captured_state_version`,`committed_state_version`,`last_mutation_operation_uuid` FROM `floatingpod`.`fp_pod_snapshot` ORDER BY `binding_uuid`,`pod_uuid`",
      MySqlDbFirstSnapshotSql.select(schema,"fp_pod_snapshot",true))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstSnapshotSql.select("x`;DELETE","fp_pod_snapshot",true))

  private def metadata(table: String): RowSetMetaDataImpl =
    val shape = MySqlDbFirstRelationShapes.forTable(table)
    val result = new RowSetMetaDataImpl()
    result.setColumnCount(shape.columns.size)
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val index = offset + 1
      result.setColumnLabel(index, column.name)
      // 使用实际Connector/J类型映射，覆盖新LONGBLOB与旧MEDIUMBLOB。
      result.setColumnType(index, MysqlType.getByName(column.sqlDeclaration.takeWhile(c => c != ' ' && c != '(')).getJdbcType)
      result.setNullable(index, if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
      if column.kind == MySqlFormat3SqlKind.DateTime then result.setScale(index, 6)
    }
    result

  @Test def fixedFull27QueriesLockRootsFirstAndRetainAllSnapshotColumns(): Unit =
    val names = MySqlDbFirstSnapshotSql.lockedReadOrder.map(_.table)
    assertEquals(Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer"), names.take(3))
    assertEquals(27, names.size)
    assertEquals(MySqlDbFirstSnapshotSql.canonicalOrder.map(_.table).toSet, names.toSet)
    names.foreach { table =>
      val sql = MySqlDbFirstSnapshotSql.select(schema, table)
      assertEquals(sql.getBytes(StandardCharsets.UTF_8).length.toLong, MySqlDbFirstSnapshotSql.selectUtf8Length(schema, table))
      if names.take(3).contains(table) then assertTrue(sql.endsWith("FOR UPDATE"))
      MySqlDbFirstSnapshotSql.validateMetadata(metadata(table), table)
    }
    assertEquals("SELECT `binding_uuid`,`pod_uuid`,`owner_uuid`,`attempt_uuid`,`snapshot_format_version`,`nbt_bytes`,`nbt_sha256`,`limit_profile_encoding`,`limit_profile_bytes`,`limit_profile_sha256`,`captured_state_version`,`committed_state_version`,`last_mutation_operation_uuid` FROM `floatingpod`.`fp_pod_snapshot` ORDER BY `binding_uuid`,`pod_uuid` FOR SHARE",
      MySqlDbFirstSnapshotSql.select(schema, "fp_pod_snapshot"))

  @Test def identifiersAndJdbcShapeDriftRejectBeforeRowsAreConsumed(): Unit =
    Vector("x`;DELETE", "", "Uppercase", "x" * 65).foreach { bad =>
      assertThrows(classOf[IllegalArgumentException], () => MySqlDbFirstSnapshotSql.select(bad, "fp_pod_snapshot"))
      assertThrows(classOf[IllegalArgumentException], () => MySqlDbFirstSnapshotSql.selectUtf8Length(bad, "fp_pod_snapshot"))
    }
    assertThrows(classOf[IllegalArgumentException], () => MySqlDbFirstSnapshotSql.select(schema, "fp_pod_snapshot`"))
    val table = "fp_pod_snapshot"
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(null, table))
    val missing = metadata(table); missing.setColumnCount(12)
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(missing, table))
    val renamed = metadata(table); renamed.setColumnLabel(6, "payload")
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(renamed, table))
    val truncated = metadata(table); truncated.setColumnType(6, Types.VARBINARY)
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(truncated, table))
    val nullable = metadata(table); nullable.setNullable(6, ResultSetMetaData.columnNullable)
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(nullable, table))
    val writer = metadata("fp_runtime_writer")
    val timestamp = MySqlDbFirstRelationShapes.forTable("fp_runtime_writer").columns.indexWhere(_.kind == MySqlFormat3SqlKind.DateTime) + 1
    assertTrue(timestamp > 0)
    writer.setScale(timestamp, 0)
    assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotSql.validateMetadata(writer, "fp_runtime_writer"))
