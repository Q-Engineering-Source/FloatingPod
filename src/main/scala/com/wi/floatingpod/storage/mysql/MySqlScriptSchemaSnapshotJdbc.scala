package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, ResultSetMetaData, SQLDataException, Types}

/** 只消费 Fresh companion 在真实事务与根锁后发行的新 schema owner。 */
private[mysql] final class MySqlScriptSchemaSnapshotJdbc(
    verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
    owner: MySqlFreshWorldBootstrap.ScriptSchemaReadOwner
):
  require(owner != null, "ScriptPublication1 private locked read owner required")
  private val inputs = owner.inputs

  def read(): MySqlScriptSchemaSnapshot =
    owner.requireCurrent(verified, scope)
    val connection = verified.connection
    if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLDataException("ScriptPublication1 requires the original active REPEATABLE READ transaction")
    val packet = MySqlBusinessRows.using(connection.prepareStatement("SELECT @@SESSION.max_allowed_packet")) { statement =>
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        if !result.next() then throw new SQLDataException("ScriptPublication1 max_allowed_packet observation missing")
        val value = result.getLong(1)
        if result.wasNull() || value <= 0 || result.next() then
          throw new SQLDataException("ScriptPublication1 max_allowed_packet observation is not one positive value")
        value
      }
    }
    val observation = MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
    val observationScope = MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
    val budget = owner.root.issueScriptSchemaSnapshotImage(inputs, inputs.schema, observation, observationScope, packet)
    try
      val cells = new MySqlFormat3FullSnapshotReader(verified, scope, inputs.schema, inputs.sourceLimitProfile)
      val byTable = MySqlScriptSchemaInputs.lockedReadOrder.map { shape =>
        owner.requireCurrent(verified, scope)
        shape.table -> cells.readScriptSchemaRelation(owner, shape.table, budget, packet)
      }.toMap
      val relations = MySqlScriptSchemaInputs.canonicalOrder.map(shape => byTable(shape.table))
      val snapshot = MySqlScriptSchemaSnapshot.decoded(inputs.schema, observation, observationScope, packet, inputs, relations, budget)
      owner.requireSnapshot(snapshot)
      snapshot.canonicalSha256
      snapshot
    catch
      case primary: Throwable =>
        owner.markFailure(primary)
        try budget.closeScopedReadBudget()
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
        throw primary

/** 固定 schema/表/列/主键；无法提交任意 shape、SQL 或 identifier。 */
private[mysql] object MySqlScriptSchemaSnapshotSql:
  def select(table: String): String =
    val shape = MySqlScriptSchemaInputs.forTable(table)
    val columns = shape.columns.map(c => "`" + c.name + "`").mkString(",")
    val keys = shape.primaryKey.map(k => "`" + k + "`").mkString(",")
    s"SELECT $columns FROM `${MySqlScriptSchemaProfile.Schema}`.`${shape.table}` ORDER BY $keys ${shape.lockClause}"

  def selectUtf8Length(table: String): Long =
    val shape = MySqlScriptSchemaInputs.forTable(table)
    val columns = shape.columns.foldLeft(0L)((n, c) => Math.addExact(n, c.name.length.toLong + 2L))
    val keys = shape.primaryKey.foldLeft(0L)((n, k) => Math.addExact(n, k.length.toLong + 2L))
    7L + columns + shape.columns.size - 1L + 6L + MySqlScriptSchemaProfile.Schema.length + shape.table.length + 5L +
      10L + keys + shape.primaryKey.size - 1L + 1L + shape.lockClause.length

  def validateMetadata(metadata: ResultSetMetaData, table: String): Unit =
    val shape = MySqlScriptSchemaInputs.forTable(table)
    if metadata == null || metadata.getColumnCount != shape.columns.size then
      throw new SQLDataException(s"$table requires all frozen ScriptPublication1 columns")
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val index = offset + 1
      // Connector/J 26.7.0 MysqlType.MEDIUMBLOB is LONGVARBINARY (-4), not VARBINARY.
      val jdbc = if column.sqlDeclaration.startsWith("MEDIUMBLOB") then Types.LONGVARBINARY else column.jdbcType
      if metadata.getColumnLabel(index) != column.name || metadata.getColumnType(index) != jdbc then
        throw new SQLDataException(s"$table.${column.name} differs from its frozen JDBC column identity/type")
      val nullable = if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls
      if metadata.isNullable(index) != nullable then throw new SQLDataException(s"$table.${column.name} JDBC nullability differs")
      if column.kind == MySqlFormat3SqlKind.DateTime && metadata.getScale(index) != 6 then
        throw new SQLDataException(s"$table.${column.name} JDBC DATETIME precision differs")
    }

private[mysql] object MySqlScriptSchemaSnapshotJdbcValidation:
  def requireManifest(snapshot: MySqlScriptSchemaSnapshot, expectation: MySqlSchemaAuditExpectation): Unit =
    val manifest = snapshot.relation("fp_schema_manifest")
    if manifest.rows.size != 1 then throw new SQLDataException("schema4 requires exactly one manifest")
    val columns = MySqlScriptSchemaInputs.forTable(manifest.table).columns
    def field(name: String): MySqlFormat3Cell = manifest.rows.head(columns.indexWhere(_.name == name))
    def check(name: String, expected: MySqlAuditCell): Unit =
      val matches = (field(name), expected) match
        case (MySqlFormat3Cell.Text(actual), MySqlAuditText(value)) => actual == value
        case (MySqlFormat3Cell.SignedInteger(actual), MySqlAuditText(value)) => actual.toString == value
        case (MySqlFormat3Cell.Binary(actual), value: MySqlAuditBinary) => value.equalsFormat3Binary(actual)
        case (MySqlFormat3Cell.SqlNull, MySqlAuditNull) => true
        case _ => false
      if !matches then throw new SQLDataException(s"schema4 manifest $name differs from independent approval")
    check("singleton_fixed1", MySqlAuditText("1"))
    check("backend", MySqlAuditText("MYSQL"))
    check("format_version", MySqlAuditText("4"))
    check("bootstrap_state", MySqlAuditText("READY"))
    check("ddl_sha256", MySqlAuditBinary(java.util.HexFormat.of().parseHex(expectation.ddlSha256)))
    def uuid(value: java.util.UUID): MySqlAuditBinary =
      MySqlAuditBinary(java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array())
    check("provisioning_uuid", uuid(expectation.provisioningUuid))
    check("expected_server_uuid", uuid(expectation.serverUuid))
    check("expected_binding_uuid", uuid(expectation.bindingUuid))
    check("import_state", MySqlAuditText("NONE"))
    Vector("import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256").foreach(check(_, MySqlAuditNull))
    val security = expectation.scriptSchemaInputs.getOrElse(throw new SQLDataException("independent schema4 security inputs missing"))
    security.manifestLabels.zip(security.expectedManifestCells).foreach((name, value) => check(name, value))
