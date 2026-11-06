package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** Full27 自己的封闭关系集合；绝不让旧 Full26 reader 默默遗漏快照表。 */
private[mysql] object MySqlDbFirstRelationShapes:
  val ExpectedRelationCount = 27
  val ExpectedColumnCount = 480

  private def shape(column: MySqlScriptSchemaProfile.Column): MySqlFormat3ColumnShape =
    val (kind, nullable, maximum) = column.declaration match
      case "SMALLINT NOT NULL" | "BIGINT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "VARBINARY(16) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(16))
      case "VARBINARY(16) NULL" => (MySqlFormat3SqlKind.Binary, true, Some(16))
      case "VARBINARY(32) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(32))
      case "LONGBLOB NOT NULL" => (MySqlFormat3SqlKind.Binary, false, None)
      case "VARCHAR(48) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL" =>
        (MySqlFormat3SqlKind.Text, false, Some(48))
      case unsupported => throw new SQLDataException(s"unsupported DB-first snapshot column ${column.name}: $unsupported")
    MySqlFormat3ColumnShape(column.name, column.declaration, kind, nullable, maximum)

  val snapshot: MySqlFormat3RelationShape = MySqlFormat3RelationShape(
    MySqlDbFirstSchemaDefinition.SnapshotTable,
    MySqlDbFirstSchemaDefinition.snapshotColumns.map(shape),
    MySqlDbFirstSchemaDefinition.snapshotPrimaryKey,
    "FOR SHARE")

  val canonicalOrder: Vector[MySqlFormat3RelationShape] =
    val combined = MySqlDeploymentReceiptRelationShapes.canonicalOrder :+ snapshot
    if combined.size != ExpectedRelationCount || combined.map(_.table).distinct.size != ExpectedRelationCount ||
      combined.map(_.columns.size).sum != ExpectedColumnCount then
      throw new SQLDataException("DB-first layout requires exact27 relations and exact480 columns")
    combined

  def forTable(table: String): MySqlFormat3RelationShape =
    require(table != null, "DB-first relation name is required")
    canonicalOrder.find(_.table == table).getOrElse(
      throw new IllegalArgumentException("table is outside the closed DB-first 27-relation layout"))
