package com.wi.floatingpod.storage.mysql

import java.sql.{SQLDataException, Types}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 只验证形状及 SQL 单元类型；空关系和构造的行不证明 quota、r03 或 schema Ready。 */
final class MySqlDeploymentReceiptRelationShapesTest:
  private def emptyRelations: Vector[MySqlFormat3RelationSnapshot] =
    MySqlDeploymentReceiptRelationShapes.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, Vector.empty))

  private def validate(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(relations)

  private def typedOperationRow: Vector[MySqlFormat3Cell] =
    MySqlDeploymentReceiptRelationShapes.operation.columns.map { column =>
      if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(0L)
        case MySqlFormat3SqlKind.Binary =>
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](column.maximumLength.getOrElse(0))))
        case other => throw new AssertionError(s"unexpected deployment column kind: $other")
    }

  private def withOperationRow(row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3RelationSnapshot] =
    emptyRelations.map { relation =>
      if relation.table == MySqlDeploymentReceiptSchemaDefinition.OperationTable then
        MySqlFormat3RelationSnapshot(relation.table, Vector(row))
      else relation
    }

  @Test def composesExact26RelationsWithoutRebuildingOriginal24(): Unit =
    val combined = MySqlDeploymentReceiptRelationShapes.canonicalOrder
    assertEquals(26, combined.size)
    assertEquals(467, combined.map(_.columns.size).sum)
    MySqlScriptSchemaInputs.canonicalOrder.zip(combined.take(24)).foreach { (original, reused) =>
      assertSame(original, reused)
    }
    val operation = MySqlDeploymentReceiptRelationShapes.operation
    val quota = MySqlDeploymentReceiptRelationShapes.quota
    assertSame(operation, combined(24))
    assertSame(quota, combined(25))
    assertEquals(31, operation.columns.size)
    assertEquals(4, quota.columns.size)
    assertEquals(Vector("namespace", "operation_uuid"), operation.primaryKey)
    assertEquals(Vector("namespace"), quota.primaryKey)
    assertEquals("FOR SHARE", operation.lockClause)
    assertEquals("FOR SHARE", quota.lockClause)
    operation.columns.filter(_.sqlDeclaration.startsWith("SMALLINT")).foreach { column =>
      assertEquals(Types.SMALLINT, column.jdbcType)
      assertEquals(6, column.integerPacketWidth)
    }
    Vector(("TINYINT NOT NULL", 4), ("INT NOT NULL", 11), ("BIGINT NOT NULL", 20)).foreach { (sql, width) =>
      assertEquals(width, MySqlFormat3ColumnShape("integer_value", sql, MySqlFormat3SqlKind.Integer, false, None).integerPacketWidth)
    }
    val binaryColumn = operation.columns.find(_.name == "operation_uuid").get
    assertThrows(classOf[SQLDataException], () => binaryColumn.requireSignedInteger(0L, "operation_uuid non-integer"))
    assertThrows(classOf[SQLDataException], () => binaryColumn.integerPacketWidth)
    Vector(("namespace", 4), ("operation_uuid", 16), ("request_sha256", 32),
      ("request_bytes", 134), ("receipt_bytes", 142)).foreach { (name, width) =>
      val column = operation.columns.find(_.name == name).get
      assertEquals(Types.VARBINARY, column.jdbcType)
      assertEquals(Some(width), column.maximumLength)
      assertFalse(column.nullable)
    }
    assertTrue(operation.columns.find(_.name == "diagnostic_bytes").get.nullable)

  @Test def fixedLookupRejectsUnknownAndNullTables(): Unit =
    MySqlDeploymentReceiptRelationShapes.canonicalOrder.foreach { shape =>
      assertSame(shape, MySqlDeploymentReceiptRelationShapes.forTable(shape.table))
    }
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptRelationShapes.forTable(null))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptRelationShapes.forTable("fp_unknown"))

  @Test def acceptsEmpty26RelationsAsStructureOnlyAndKeepsOldProfilesClosed(): Unit =
    // 空 quota 与 operation 仅满足结构验证；这不是 provisioned quota 种子或业务有效行。
    validate(emptyRelations)
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotValidation.validateRelations(emptyRelations))
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotValidation.validateScriptPublication1Relations(emptyRelations))

  @Test def rejectsMissingDuplicateOrReorderedRelations(): Unit =
    val relations = emptyRelations
    Vector(relations.take(20), relations.take(24), relations.dropRight(1),
      relations.updated(25, relations(24)),
      relations.updated(24, relations(25)).updated(25, relations(24))).foreach { invalid =>
      assertThrows(classOf[SQLDataException], () => validate(invalid))
    }

  @Test def validatesDeploymentColumnKindsNullabilityAndShortRange(): Unit =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    val row = typedOperationRow
    validate(withOperationRow(row))
    assertThrows(classOf[SQLDataException], () => validate(withOperationRow(row.dropRight(1))))
    val actor = shape.columns.indexWhere(_.name == "actor_uuid")
    assertThrows(classOf[SQLDataException], () => validate(withOperationRow(row.updated(actor, MySqlFormat3Cell.SqlNull))))
    assertThrows(classOf[SQLDataException], () => validate(withOperationRow(row.updated(actor, MySqlFormat3Cell.SignedInteger(0L)))))
    shape.columns.zipWithIndex.filter(_._1.sqlDeclaration.startsWith("SMALLINT")).foreach { (column, index) =>
      // SQL signed 范围与 encoding/step 业务闭集不同；这里只验证真实 typed validator。
      Vector(Short.MinValue.toLong, 0L, Short.MaxValue.toLong).foreach { value =>
        validate(withOperationRow(row.updated(index, MySqlFormat3Cell.SignedInteger(value))))
      }
      Vector(-32769L, 32768L).foreach { value =>
        assertThrows(classOf[SQLDataException], () => validate(withOperationRow(row.updated(index, MySqlFormat3Cell.SignedInteger(value)))))
      }
      if column.nullable then validate(withOperationRow(row.updated(index, MySqlFormat3Cell.SqlNull)))
      else assertThrows(classOf[SQLDataException], () => validate(withOperationRow(row.updated(index, MySqlFormat3Cell.SqlNull))))
    }
