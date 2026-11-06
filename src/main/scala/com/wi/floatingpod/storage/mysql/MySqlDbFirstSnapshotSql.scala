package com.wi.floatingpod.storage.mysql

import java.sql.{ResultSetMetaData, SQLDataException, Types}
import java.util.{HexFormat, UUID}

/** schema6的完整固定查询；根锁顺序与最终canonical顺序分别保持。 */
private[mysql] object MySqlDbFirstSnapshotSql:
  val canonicalOrder: Vector[MySqlFormat3RelationShape] = MySqlDbFirstRelationShapes.canonicalOrder
  private val roots = Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer")
  val lockedReadOrder: Vector[MySqlFormat3RelationShape] =
    roots.map(MySqlDbFirstRelationShapes.forTable) ++ canonicalOrder.filterNot(s => roots.contains(s.table))

  private def shape(schema: String, table: String): MySqlFormat3RelationShape =
    require(schema != null && schema.matches("[a-z][a-z0-9_]{0,63}"), "safe fixed schema6 identifier required")
    MySqlDbFirstRelationShapes.forTable(table)
  private def lock(table: String, value: MySqlFormat3RelationShape): String =
    if roots.contains(table) then "FOR UPDATE" else value.lockClause

  def selectUtf8Length(schema: String, table: String,consistentObservation:Boolean=false): Long =
    val s = shape(schema, table)
    val columns = s.columns.foldLeft(0L)((n, c) => Math.addExact(n, c.name.length.toLong + 2L))
    val keys = s.primaryKey.foldLeft(0L)((n, k) => Math.addExact(n, k.length.toLong + 2L))
    7L + columns + s.columns.size - 1L + 6L + schema.length + s.table.length + 5L +
      10L + keys + s.primaryKey.size - 1L + (if consistentObservation then 0L else 1L + lock(table, s).length)

  def select(schema: String, table: String,consistentObservation:Boolean=false): String =
    val s = shape(schema, table)
    val capacity = MySqlFormat3JvmArrayLimit.requireLength(selectUtf8Length(schema, table,consistentObservation), "schema6 fixed SELECT")
    val out = new StringBuilder(capacity)
    out.append("SELECT ")
    s.columns.zipWithIndex.foreach { (c, i) =>
      if i != 0 then out.append(',')
      out.append('`').append(c.name).append('`')
    }
    out.append(" FROM `").append(schema).append("`.`").append(table).append("` ORDER BY ")
    s.primaryKey.zipWithIndex.foreach { (key, i) =>
      if i != 0 then out.append(',')
      out.append('`').append(key).append('`')
    }
    // UNKNOWN观测使用同一RR一致性视图；不能再请求原不确定事务可能仍持有的行锁。
    if !consistentObservation then out.append(' ').append(lock(table, s))
    out.toString

  def validateMetadata(metadata: ResultSetMetaData, table: String): Unit =
    val s = MySqlDbFirstRelationShapes.forTable(table)
    if metadata == null || metadata.getColumnCount != s.columns.size then
      throw new SQLDataException(s"$table requires all frozen Full27 columns")
    s.columns.zipWithIndex.foreach { (column, offset) =>
      val index = offset + 1
      val jdbc = if column.sqlDeclaration.startsWith("MEDIUMBLOB") then Types.LONGVARBINARY else column.jdbcType
      val nullable = if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls
      val labelMatches = metadata.getColumnLabel(index) == column.name
      val actualJdbc = metadata.getColumnType(index)
      val actualNullable = metadata.isNullable(index)
      // 固定 Connector/J 26.7.0 的 getScale 对非 decimal 返回0；DATETIME(6)由原完整 schema audit核验。
      val scale = if column.kind == MySqlFormat3SqlKind.DateTime then Some(metadata.getScale(index)) else None
      if !labelMatches || actualJdbc != jdbc || actualNullable != nullable || scale.exists(_ != 0) then
        throw new MySqlDbFirstMetadataMismatch(
          s"table=$table column=${column.name} ordinal=$index labelMatches=$labelMatches " +
          s"jdbcActual=$actualJdbc jdbcExpected=$jdbc nullableActual=$actualNullable nullableExpected=$nullable" +
          scale.map(value => s" scaleActual=$value scaleExpected=0").getOrElse(""))
    }

  /** 用原image计费；此处仅核DB manifest，不发行运行或恢复资格。 */
  def requireManifest(snapshot: MySqlScriptSchemaSnapshot, expectation: MySqlSchemaAuditExpectation): Unit =
    require(snapshot != null && expectation != null && snapshot.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      expectation.profile == MySqlSchemaAuditProfile.DbFirst1 && snapshot.schema == expectation.schema,
      "original schema6 Full27 image and independent approval required")
    val budget = snapshot.allocationBudget
    budget.requireScriptDataInputs(snapshot.inputs)
    val relation = snapshot.relation("fp_schema_manifest")
    if relation.rows.size != 1 then throw new SQLDataException("Full27 requires exactly one READY manifest")
    var fee = 8192L
    relation.rows.head.foreach {
      case MySqlFormat3Cell.Binary(value) => fee = Math.addExact(fee, Math.multiplyExact(value.length.toLong, 4L))
      case _ => ()
    }
    budget.reserveTemporaryMemory(fee, "schema6 manifest defensive copies BEFORE allocation")
    var primary: Throwable = null
    try
      val columns = snapshot.inputs.forTable(relation.table).columns
      def field(name: String): MySqlFormat3Cell = relation.rows.head(columns.indexWhere(_.name == name))
      def check(name: String, expected: MySqlFormat3Cell): Unit =
        if field(name) != expected then throw new SQLDataException(s"schema6 manifest $name differs")
      def uuid(name: String, expected: UUID): Unit = field(name) match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsUuid(expected) => ()
        case _ => throw new SQLDataException(s"schema6 manifest $name differs from approval")
      check("singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
      check("backend", MySqlFormat3Cell.Text("MYSQL"))
      check("format_version", MySqlFormat3Cell.SignedInteger(6L))
      check("bootstrap_state", MySqlFormat3Cell.Text("READY"))
      check("import_state", MySqlFormat3Cell.Text("NONE"))
      uuid("provisioning_uuid", expectation.provisioningUuid)
      uuid("expected_server_uuid", expectation.serverUuid)
      uuid("expected_binding_uuid", expectation.bindingUuid)
      field("ddl_sha256") match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(HexFormat.of().parseHex(expectation.ddlSha256)) => ()
        case _ => throw new SQLDataException("schema6 manifest DDL differs from approval")
      Vector("import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256")
        .foreach(check(_, MySqlFormat3Cell.SqlNull))
      val security = expectation.dbFirstInputs.getOrElse(throw new SQLDataException("schema6 independent C/K/profile missing"))
      val labels = security.manifestLabels
      val cells = labels.map(name => field(name) match
        case MySqlFormat3Cell.Text(value) => MySqlAuditText(value)
        case MySqlFormat3Cell.SignedInteger(value) => MySqlAuditText(value.toString)
        case MySqlFormat3Cell.Binary(value) =>
          val bytes = new Array[Byte](value.length)
          var index = 0
          while index < bytes.length do
            bytes(index) = value.byteAt(index)
            index += 1
          MySqlAuditBinary(bytes)
        case MySqlFormat3Cell.SqlNull => MySqlAuditNull
        case _ => throw new SQLDataException("schema6 manifest C/K/profile cell type differs"))
      val row = MySqlAuditRow(cells)
      val set = MySqlAuditResultSet("manifest", labels.map(n =>
        MySqlAuditColumn(n, Types.VARCHAR, "VARCHAR", 0, 0, ResultSetMetaData.columnNullable, false)), Vector(row))
      security.validateManifest(set, row, budget)
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() =>
        budget.releaseTemporaryMemory(fee, "schema6 manifest comparison detached")))

/** 只携带冻结列标识与数值 metadata；不携带驱动 message、URL、参数或实际 label。 */
private[floatingpod] final class MySqlDbFirstMetadataMismatch private[mysql](val safeDifference: String)
    extends SQLDataException("Full27 JDBC metadata differs from its frozen shape")
