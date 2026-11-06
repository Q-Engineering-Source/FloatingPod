package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 仅组合 full26 的列形状；不发行 schema、guard、预算或运行时资格。 */
private[mysql] object MySqlDeploymentReceiptRelationShapes:
  val ExpectedRelationCount = 26
  val ExpectedColumnCount = 467

  private def parseDeploymentColumn(column: MySqlScriptSchemaProfile.Column): MySqlFormat3ColumnShape =
    val (kind, nullable, maximumLength) = column.declaration match
      case "SMALLINT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "SMALLINT NULL" => (MySqlFormat3SqlKind.Integer, true, None)
      case "BIGINT NOT NULL" => (MySqlFormat3SqlKind.Integer, false, None)
      case "VARBINARY(4) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(4))
      case "VARBINARY(16) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(16))
      case "VARBINARY(32) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(32))
      case "VARBINARY(32) NULL" => (MySqlFormat3SqlKind.Binary, true, Some(32))
      case "VARBINARY(134) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(134))
      case "VARBINARY(142) NOT NULL" => (MySqlFormat3SqlKind.Binary, false, Some(142))
      case "LONGBLOB NOT NULL" => (MySqlFormat3SqlKind.Binary, false, None)
      case "LONGBLOB NULL" => (MySqlFormat3SqlKind.Binary, true, None)
      case declaration => throw new SQLDataException(
        s"unsupported deployment receipt SQL declaration for ${column.name}: $declaration")
    MySqlFormat3ColumnShape(column.name, column.declaration, kind, nullable, maximumLength)

  // FOR SHARE 只是既有 shape 类型的读锁字段，不定义 REGISTER 或 writer 的实际锁序。
  val operation: MySqlFormat3RelationShape = MySqlFormat3RelationShape(
    MySqlDeploymentReceiptSchemaDefinition.OperationTable,
    MySqlDeploymentReceiptSchemaDefinition.operationColumns.map(parseDeploymentColumn),
    MySqlDeploymentReceiptSchemaDefinition.operationPrimaryKey,
    "FOR SHARE")

  val quota: MySqlFormat3RelationShape = MySqlFormat3RelationShape(
    MySqlDeploymentReceiptSchemaDefinition.QuotaTable,
    MySqlDeploymentReceiptSchemaDefinition.quotaColumns.map(parseDeploymentColumn),
    MySqlDeploymentReceiptSchemaDefinition.quotaPrimaryKey,
    "FOR SHARE")

  val canonicalOrder: Vector[MySqlFormat3RelationShape] =
    val combined = MySqlScriptSchemaInputs.canonicalOrder ++ Vector(operation, quota)
    if combined.size != ExpectedRelationCount || combined.map(_.table).distinct.size != ExpectedRelationCount ||
      combined.map(_.columns.size).sum != ExpectedColumnCount || operation.columns.size != 31 || quota.columns.size != 4 then
      throw new SQLDataException("deployment receipt layout requires exact26 relations and exact467 columns including 31/4 deployment columns")
    combined

  def forTable(table: String): MySqlFormat3RelationShape =
    if table == null then throw new IllegalArgumentException("deployment receipt table name is required")
    canonicalOrder.find(_.table == table).getOrElse(
      throw new IllegalArgumentException("table is outside the closed deployment receipt 26-relation layout"))
