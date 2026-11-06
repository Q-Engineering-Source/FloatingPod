package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.sql.{Connection, ResultSet, SQLDataException, Types}
import java.util.Locale
import scala.util.matching.Regex

/** 首次托管安装只读采集已编译 schema6 中的 INFORMATION_SCHEMA DATA，不执行审计 routine 或取得 Ready。 */
private[mysql] object MySqlManagedCatalogCapture:
  final case class Query private[MySqlManagedCatalogCapture] (
      id: String,
      source: String,
      schemaColumn: String,
      sql: String,
      sha256: String
  )

  private val catalogSpecs = Vector(
    ("RS02_TABLES", "TABLES", "TABLE_SCHEMA"),
    ("RS03_COLUMNS", "COLUMNS", "TABLE_SCHEMA"),
    ("RS04_STATISTICS", "STATISTICS", "TABLE_SCHEMA"),
    ("RS05_TABLE_CONSTRAINTS", "TABLE_CONSTRAINTS", "TABLE_SCHEMA"),
    ("RS06_KEY_COLUMN_USAGE", "KEY_COLUMN_USAGE", "TABLE_SCHEMA"),
    ("RS07_REFERENTIAL_CONSTRAINTS", "REFERENTIAL_CONSTRAINTS", "CONSTRAINT_SCHEMA"),
    ("RS08_CHECK_CONSTRAINTS", "CHECK_CONSTRAINTS", "CONSTRAINT_SCHEMA"),
    ("RS09_TRIGGERS", "TRIGGERS", "TRIGGER_SCHEMA"),
    ("RS10_AUDITSELF", "ROUTINES", "ROUTINE_SCHEMA"),
    ("RS11_AUDITSELF_PARAMETERS", "PARAMETERS", "SPECIFIC_SCHEMA")
  )

  private val securitySpecs = Vector(
    ("RS15_TABLE_PRIVILEGES", "TABLE_PRIVILEGES"),
    ("RS16_COLUMN_PRIVILEGES", "COLUMN_PRIVILEGES"),
    ("RS17_SCHEMA_PRIVILEGES", "SCHEMA_PRIVILEGES"),
    ("RS18_USER_PRIVILEGES", "USER_PRIVILEGES")
  )

  private val markerPattern: Regex = "(?m)^[ \\t]*--[ \\t]+(RS[^ \\t:]+):?(?:[ \\t]+([^\\r\\n]*))?[ \\t]*$".r
  private val forbiddenQueryToken: Regex =
    "(?i)\\b(UNION|JOIN|CALL|INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|INTO|SLEEP|BENCHMARK|GET_LOCK|RELEASE_LOCK)\\b".r
  private val fromSourcePattern: Regex = "(?i)\\bFROM\\s+INFORMATION_SCHEMA\\.([A-Z_]+)\\b".r
  private val temporalJdbcTypes = Set(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE)

  private final case class Marker(id: String, start: Int, end: Int)

  /** 最终 existing-database schema6 中 RS02–RS11 的真实 SQL；每条仍是一个独立单 SELECT。 */
  private[mysql] def directCatalogQueries(rendered: MySqlDbFirstSchemaProfile.Rendered): Vector[Query] =
    validateRendered(rendered)
    extractCatalogQueries(rendered.sql, rendered.inputs.schema)

  /** 最终 compiled routine 的四条原始直接授权查询；不包含 routine 内 mysql.* preflight。 */
  private[mysql] def directSecurityQueries(rendered: MySqlDbFirstSchemaProfile.Rendered): Vector[Query] =
    validateRendered(rendered)
    extractSecurityQueries(rendered.sql, rendered.inputs)

  /** 直接 I_S DATA 捕获，不签发静态批准、baseline、资格或 Ready。 */
  private[mysql] def captureCatalog(
      rendered: MySqlDbFirstSchemaProfile.Rendered,
      owner: ManagedMySqlConnectionOwner,
      root: MySqlFormat3ReadBudget
  ): Captured =
    capture(rendered, owner, root, directCatalogQueries(rendered))

  /** 仅捕获原 routine 的四组 direct-grant I_S DATA；安全 preflight 由其独立责任边界完成。 */
  private[mysql] def captureSecurity(
      owner: ManagedMySqlConnectionOwner,
      rendered: MySqlDbFirstSchemaProfile.Rendered,
      root: MySqlFormat3ReadBudget
  ): Captured =
    capture(rendered, owner, root, directSecurityQueries(rendered))

  private[mysql] final class Captured private[MySqlManagedCatalogCapture] (
      private var retainedResultSets: Vector[MySqlAuditResultSet],
      private var retainedRoutineSource: Option[MySqlAuditResultSet],
      private var childBudget: MySqlFormat3ReadBudget,
      private val worker: Thread
  ) extends AutoCloseable:
    private var closed = false

    /** 原始 JDBC DATA 值保持原顺序；调用者不能将它们视作 audit qualification。 */
    private[mysql] def resultSets: Vector[MySqlAuditResultSet] =
      requireWorker()
      if closed || retainedResultSets == null then throw new IllegalStateException("managed catalog DATA capture is closed")
      retainedResultSets

    private[mysql] def routineSource: Option[MySqlAuditResultSet] =
      requireWorker()
      if closed then throw new IllegalStateException("managed catalog DATA capture is closed")
      retainedRoutineSource

    private def requireWorker(): Unit =
      if Thread.currentThread() ne worker then throw new IllegalStateException("managed catalog capture requires its original worker")

    override def close(): Unit =
      requireWorker()
      if !closed then
        if childBudget == null then throw new IllegalStateException("managed catalog DATA budget owner is unavailable")
        childBudget.closeScopedReadBudget()
        retainedResultSets = null
        retainedRoutineSource = None
        childBudget = null
        closed = true

  /** 原 marker interval parser 供结构测试直接检查实际渲染 SQL；不执行或改写语句。 */
  private[mysql] def extractCatalogQueries(sql: String, schema: String): Vector[Query] =
    if sql == null || schema == null || !schema.matches("[A-Za-z0-9_]{1,64}") then
      throw new SQLDataException("managed catalog capture requires an exact rendered script and safe schema")
    val start = uniqueMarker(sql, "RS02")
    val end = uniqueMarker(sql, "RS12")
    if start.start >= end.start then throw new SQLDataException("managed catalog RS02–RS12 marker interval is reversed")
    val intervalMarkers = markers(sql).filter(marker => marker.start >= start.start && marker.start < end.start)
    val expectedIds = catalogSpecs.map(_._1.take(4))
    if intervalMarkers.map(_.id) != expectedIds then
      throw new SQLDataException("managed catalog markers are unknown, missing, duplicated, or out of order")
    val body = sql.substring(start.end, end.start)
    val framed = MySqlScriptSchemaProfile.statements(body).fold(
      error => throw new SQLDataException("managed catalog SQL framing failed: " + error), identity)
    if framed.size != catalogSpecs.size then
      throw new SQLDataException("managed catalog marker interval does not contain exactly ten SELECTs")
    framed.zip(catalogSpecs).map { case (statement, (id, source, schemaColumn)) =>
      validateSingleSelect(statement, source, Some((schemaColumn, schema)))
      query(id, source, schemaColumn, statement)
    }

  private[mysql] def extractSecurityQueries(
      sql: String,
      inputs: MySqlDeploymentReceiptSchemaProfile.Inputs
  ): Vector[Query] =
    if sql == null || inputs == null || inputs.accounts == null then
      throw new SQLDataException("managed security capture requires the exact rendered script and six account inputs")
    val candidates = markers(sql).filter(_.id == "RS15..18")
    if candidates.size != 1 then throw new SQLDataException("managed security RS15..18 marker is missing or duplicated")
    val start = candidates.head
    // The managed recipe places all 81 trigger END$$ statements after the audit routine; use the first following routine terminator.
    val routineEnd = endRoutineMarkers(sql).find(_.start > start.start).getOrElse(
      throw new SQLDataException("managed security query interval has no following routine terminator"))
    val intervalMarkers = markers(sql).filter(marker => marker.start >= start.start && marker.start < routineEnd.start)
    if intervalMarkers.map(_.id) != Vector("RS15..18") then
      throw new SQLDataException("managed security markers are unknown, duplicated, or out of order")
    val body = sql.substring(start.end, routineEnd.start)
    val framed = MySqlScriptSchemaProfile.statements(body).fold(
      error => throw new SQLDataException("managed security SQL framing failed: " + error), identity)
    if framed.size != securitySpecs.size then
      throw new SQLDataException("managed security marker interval does not contain exactly four SELECTs")
    val expectedSubjects = inputs.accounts.all.map(account => "'" + account.grantee.replace("'", "''") + "'").mkString(",")
    framed.zip(securitySpecs).map { case (statement, (id, source)) =>
      validateSingleSelect(statement, source, None)
      val exactSubjectPredicate = s"GRANTEE IN ($expectedSubjects)"
      if countLiteral(statement, exactSubjectPredicate) != 1 || countLiteral(statement, "TABLE_SCHEMA='") != 0 then
        throw new SQLDataException("managed security query does not cover the six exact subject accounts across grant scopes")
      val hash = sha256(statement)
      Query(id, source, "", statement, hash)
    }

  private def validateRendered(rendered: MySqlDbFirstSchemaProfile.Rendered): Unit =
    if rendered == null || rendered.inputs == null || rendered.mode != MySqlDbFirstSchemaProfile.RenderMode.ExistingDatabaseOnly then
      throw new SQLDataException("managed capture requires the final ExistingDatabaseOnly schema6 renderer")
    val canonical = MySqlDbFirstSchemaProfile.renderExistingDatabase(rendered.inputs)
    if rendered.sql != canonical.sql || rendered.sha256 != canonical.sha256 ||
        rendered.tables != canonical.tables || rendered.guards != canonical.guards ||
        rendered.profileIdentitySha256 != canonical.profileIdentitySha256 ||
        sha256(rendered.sql) != rendered.sha256 then
      throw new SQLDataException("managed capture rendered SQL or Full27 descriptors differ from the canonical existing-database plan")

  private def validateSingleSelect(
      statement: String,
      expectedSource: String,
      schemaScope: Option[(String, String)]
  ): Unit =
    if statement == null || !statement.matches("(?is)^SELECT\\b.*") ||
        countWord(statement, "SELECT") != 1 || countWord(statement, "FROM") != 1 ||
        countWord(statement, "WHERE") != 1 || forbiddenQueryToken.findFirstIn(statement).nonEmpty then
      throw new SQLDataException("managed catalog capture accepts only one closed, read-only SELECT per result set")
    val sources = fromSourcePattern.findAllMatchIn(statement).map(_.group(1).toUpperCase(Locale.ROOT)).toVector
    if sources != Vector(expectedSource) then throw new SQLDataException("managed catalog SELECT source differs from its fixed INFORMATION_SCHEMA object")
    schemaScope.foreach { (column, schema) =>
      val predicate = s"CAST($column AS BINARY)=CAST('$schema' AS BINARY)"
      if countLiteral(statement, predicate) != 1 then
        throw new SQLDataException("managed catalog SELECT is not closed over the exact configured schema")
    }

  private def query(id: String, source: String, schemaColumn: String, statement: String): Query =
    Query(id, source, schemaColumn, statement, sha256(statement))

  private def sha256(value: String): String =
    MySqlScriptSchemaProfile.sha256(value.getBytes(StandardCharsets.UTF_8))

  private def markers(sql: String): Vector[Marker] =
    markerPattern.findAllMatchIn(sql).map { found =>
      Marker(found.group(1), found.start, found.end)
    }.toVector

  private def uniqueMarker(sql: String, expected: String): Marker =
    val found = markers(sql).filter(_.id == expected)
    if found.size != 1 then throw new SQLDataException(s"managed catalog $expected marker is missing or duplicated")
    found.head

  private val endRoutinePattern: Regex = "(?m)^[ \\t]*END\\$\\$[ \\t]*$".r

  private def endRoutineMarkers(sql: String): Vector[Marker] =
    endRoutinePattern.findAllMatchIn(sql).map(found => Marker("END$$", found.start, found.end)).toVector

  private def countWord(sql: String, word: String): Int =
    ("(?i)\\b" + Regex.quote(word) + "\\b").r.findAllIn(sql).length

  private def countLiteral(sql: String, value: String): Int =
    var count = 0
    var from = 0
    var index = sql.indexOf(value, from)
    while index >= 0 do
      count = Math.incrementExact(count)
      from = index + value.length
      index = sql.indexOf(value, from)
    count

  private def capture(
      rendered: MySqlDbFirstSchemaProfile.Rendered,
      owner: ManagedMySqlConnectionOwner,
      root: MySqlFormat3ReadBudget,
      queries: Vector[Query]
  ): Captured =
    if rendered == null || owner == null || root == null || queries == null || queries.isEmpty then
      throw new IllegalArgumentException("managed catalog capture requires rendered SQL, original connection owner, root budget and queries")
    validateRendered(rendered)
    val schema = rendered.inputs.schema
    if root.profile ne rendered.inputs.sourceLimitProfile then
      throw new SQLDataException("managed catalog DATA root uses a different source-limit profile")
    if root.isScopedReadBudget || root.isClosed then
      throw new SQLDataException("managed catalog capture requires the original open workflow root budget")
    if owner.state() != ManagedMySqlConnectionOwner.State.OPEN || !owner.retainsConnection() then
      throw new SQLDataException("managed catalog capture requires the original open installer connection owner")
    val identity = owner.identity()
    if identity.catalog != schema then throw new SQLDataException("managed catalog installer catalog differs from the rendered schema")

    val child = root.newCanonicalDataScope()
    var primary: Throwable = null
    try
      val worker = Thread.currentThread()
      val allocation = new CaptureAllocation(owner, child, worker)
      owner.requireHeldInitializationLock()
      val connection = owner.connection()
      if connection == null || connection.isClosed then
        throw new SQLDataException("managed catalog original JDBC connection is unavailable")
      allocation.retain(512L, "managed catalog statement/result builders BEFORE allocation")
      val sets = Vector.newBuilder[MySqlAuditResultSet]
      allocation.retainScratch(sets, "managed catalog partial result-set builder")
      val budget = new MySqlSchemaAuditor.ReadBudget(Some(root.profile), allocation)
      queries.foreach { query =>
        owner.requireHeldInitializationLock()
        val captured = executeOne(connection, owner, query, budget, allocation)
        sets += captured
        owner.requireHeldInitializationLock()
      }
      allocation.retain(Math.multiplyExact(96L, queries.size.toLong), "managed catalog completed result-set vector BEFORE allocation")
      val completed = sets.result()
      allocation.retainScratch(completed, "managed catalog retained result-set vector")
      owner.requireHeldInitializationLock()
      val routineSource = completed.find(_.id == "RS10_AUDITSELF").filter(_.rows.nonEmpty).map { _ =>
        // 同一受锁保护的 installer connection、同一 DATA scope/总预算，不 CALL 被检查对象。
        owner.requireHeldInitializationLock()
        val name = MySqlDbFirstSchemaDefinition.AuditRoutine
        val show = query(MySqlManagedRoutineSource.ResultId, "ROUTINES", "ROUTINE_SCHEMA",
          s"SHOW CREATE PROCEDURE `$schema`.`$name`")
        val result = executeOne(connection, owner, show, budget, allocation)
        owner.requireHeldInitializationLock()
        result
      }
      new Captured(completed, routineSource, child, worker)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary != null then
        try child.closeScopedReadBudget()
        catch
          case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)

  private def executeOne(
      connection: Connection,
      owner: ManagedMySqlConnectionOwner,
      query: Query,
      budget: MySqlSchemaAuditor.ReadBudget,
      allocation: CaptureAllocation
  ): MySqlAuditResultSet =
    val context = s"managed catalog ${query.id}"
    allocation.retain(384L + Math.multiplyExact(32L, query.sql.length.toLong), s"$context SQL and JDBC statement BEFORE allocation")
    val statement = MySqlSchemaAuditor.jdbcRead(context, "createStatement")(connection.createStatement())
    withResource(statement) { sqlStatement =>
      MySqlSchemaAuditor.jdbcRead(context, "setQueryTimeout")(sqlStatement.setQueryTimeout(owner.queryTimeoutSeconds()))
      val result = MySqlSchemaAuditor.jdbcRead(context, "executeQuery")(sqlStatement.executeQuery(query.sql))
      withResource(result) { rows => decodeResultSet(rows, query, budget, allocation) }
    }

  private def decodeResultSet(
      result: ResultSet,
      query: Query,
      budget: MySqlSchemaAuditor.ReadBudget,
      allocation: CaptureAllocation
  ): MySqlAuditResultSet =
    if result == null then throw new SQLDataException(s"managed catalog ${query.id} returned a missing result set")
    allocation.retain(256L, s"managed catalog ${query.id} JDBC metadata BEFORE acquisition")
    val metadata = MySqlSchemaAuditor.jdbcRead(query.id, "getMetaData")(result.getMetaData)
    if metadata == null then throw new SQLDataException(s"managed catalog ${query.id} has no JDBC metadata")
    val count = MySqlSchemaAuditor.jdbcRead(query.id, "getColumnCount")(metadata.getColumnCount)
    if count <= 0 || count > MySqlSchemaAuditor.MaxColumnsPerResultSet then
      throw new SQLDataException(s"managed catalog ${query.id} has an invalid column count")
    allocation.retain(Math.addExact(512L, Math.multiplyExact(192L, count.toLong)), s"managed catalog ${query.id} column metadata BEFORE allocation")
    val columns = Vector.tabulate(count) { offset =>
      val index = offset + 1
      val label = requiredMetadataText(MySqlSchemaAuditor.jdbcRead(query.id, "getColumnLabel")(metadata.getColumnLabel(index)), query.id, "column label")
      val typeName = requiredMetadataText(MySqlSchemaAuditor.jdbcRead(query.id, "getColumnTypeName")(metadata.getColumnTypeName(index)), query.id, "JDBC type name")
      val labelBytes = MySqlSchemaAuditor.boundedUtf8ByteLength(label, MySqlSchemaAuditor.MaxCellBytes, s"${query.id} column label")
      val typeBytes = MySqlSchemaAuditor.boundedUtf8ByteLength(typeName, MySqlSchemaAuditor.MaxCellBytes, s"${query.id} JDBC type")
      allocation.retain(Math.addExact(96L, Math.multiplyExact(32L, Math.addExact(labelBytes, typeBytes).toLong)),
        s"managed catalog ${query.id} actual column description BEFORE holder allocation")
      budget.addBytes(Math.addExact(32, Math.addExact(labelBytes, typeBytes)), s"${query.id} column framing")
      MySqlAuditColumn(
        label,
        MySqlSchemaAuditor.jdbcRead(query.id, "getColumnType")(metadata.getColumnType(index)),
        typeName,
        MySqlSchemaAuditor.jdbcRead(query.id, "getPrecision")(metadata.getPrecision(index)),
        MySqlSchemaAuditor.jdbcRead(query.id, "getScale")(metadata.getScale(index)),
        MySqlSchemaAuditor.jdbcRead(query.id, "isNullable")(metadata.isNullable(index)),
        MySqlSchemaAuditor.jdbcRead(query.id, "isSigned")(metadata.isSigned(index))
      )
    }
    allocation.withTemporary(Math.addExact(256L, Math.multiplyExact(96L, count.toLong)), s"${query.id} duplicate-label validation") {
      val labels = columns.map(_.label)
      allocation.retainScratch(labels, s"managed catalog ${query.id} column label view")
      if labels.distinct.size != labels.size then throw new SQLDataException(s"managed catalog ${query.id} has duplicate column labels")
    }
    allocation.retain(512L, s"managed catalog ${query.id} row/cell builders BEFORE allocation")
    val rows = Vector.newBuilder[MySqlAuditRow]
    allocation.retainScratch(rows, s"managed catalog ${query.id} partial row builder")
    var rowCount = 0
    while MySqlSchemaAuditor.jdbcRead(query.id, "next")(result.next()) do
      rowCount = Math.incrementExact(rowCount)
      budget.addRow(rowCount)
      allocation.retain(256L, s"managed catalog ${query.id} row cell builder BEFORE allocation")
      val cells = Vector.newBuilder[MySqlAuditCell]
      allocation.retainScratch(cells, s"managed catalog ${query.id} partial cell builder")
      var index = 1
      while index <= columns.size do
        budget.addCell()
        cells += readCell(result, query.id, rowCount, index, columns(index - 1), budget, allocation)
        index += 1
      allocation.retain(Math.multiplyExact(96L, columns.size.toLong), s"managed catalog ${query.id} completed row cells BEFORE allocation")
      val rowCells = cells.result()
      allocation.retainScratch(rowCells, s"managed catalog ${query.id} completed row cell vector")
      allocation.retain(96L, s"managed catalog ${query.id} row holder BEFORE allocation")
      rows += MySqlAuditRow(rowCells)
    allocation.retain(Math.multiplyExact(96L, rowCount.toLong), s"managed catalog ${query.id} completed row vector BEFORE allocation")
    val completedRows = rows.result()
    allocation.retainScratch(completedRows, s"managed catalog ${query.id} retained row vector")
    allocation.retain(256L, s"managed catalog ${query.id} result-set holder BEFORE allocation")
    MySqlAuditResultSet(query.id, columns, completedRows)

  private def readCell(
      result: ResultSet,
      resultSetId: String,
      rowNumber: Int,
      index: Int,
      column: MySqlAuditColumn,
      budget: MySqlSchemaAuditor.ReadBudget,
      allocation: CaptureAllocation
  ): MySqlAuditCell =
    val context = s"$resultSetId row $rowNumber column $index '${column.label}' JDBC ${column.jdbcType}/${column.typeName}"
    if temporalJdbcTypes.contains(column.jdbcType) then
      throw new SQLDataException(s"managed catalog $context has an unsupported temporal type")
    MySqlSchemaAuditor.jdbcValueKind(column.jdbcType, context) match
      case MySqlAuditJdbcValueKind.Timestamp =>
        throw new SQLDataException(s"managed catalog $context has an unsupported temporal type")
      case MySqlAuditJdbcValueKind.Binary =>
        allocation.retain(256L, s"managed catalog $context binary stream owner BEFORE acquisition")
        val stream = MySqlSchemaAuditor.jdbcRead(context, "getBinaryStream")(result.getBinaryStream(index))
        if stream == null then
          if !MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull()) then
            throw new SQLDataException(s"managed catalog $context returned no binary stream for a non-NULL value")
          MySqlAuditNull
        else
          val bytes = MySqlSchemaAuditor.jdbcRead(context, "binary stream decode") {
            withResource(stream)(value => MySqlSchemaAuditor.readBoundedBytesOwned(value, allocation, context))
          }
          budget.addBytes(bytes.length, context)
          allocation.retain(Math.addExact(96L, bytes.length.toLong), s"managed catalog $context retained binary cell BEFORE copy")
          MySqlAuditBinary(bytes)
      case MySqlAuditJdbcValueKind.Numeric =>
        MySqlSchemaAuditor.readOwnedNumericColumn(result, index, context, allocation, budget)
      case MySqlAuditJdbcValueKind.SqlNull =>
        allocation.retain(96L, s"managed catalog $context SQL NULL cell BEFORE JDBC allocation")
        val value = MySqlSchemaAuditor.jdbcRead(context, "getObject")(result.getObject(index))
        val wasNull = MySqlSchemaAuditor.jdbcRead(context, "wasNull")(result.wasNull())
        MySqlSchemaAuditor.decodeJdbcNullCell(value, wasNull, context)
      case MySqlAuditJdbcValueKind.Text =>
        val value = MySqlSchemaAuditor.readOwnedNullableTextColumn(result, index, context, allocation)
        value match
          case Some(text) =>
            val bytes = MySqlSchemaAuditor.boundedUtf8ByteLength(text, MySqlSchemaAuditor.MaxCellBytes, context)
            budget.addBytes(bytes, context)
            MySqlAuditText(text)
          case None => MySqlAuditNull

  private def requiredMetadataText(value: String, resultSetId: String, label: String): String =
    if value == null || value.isEmpty then throw new SQLDataException(s"managed catalog $resultSetId has a missing $label")
    MySqlSchemaAuditor.boundedUtf8ByteLength(value, MySqlSchemaAuditor.MaxCellBytes, s"$resultSetId $label")
    value

  private def withResource[A <: AutoCloseable, B](resource: A)(body: A => B): B =
    if resource == null then throw new SQLDataException("managed catalog JDBC resource was not acquired")
    var primary: Throwable = null
    try body(resource)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary == null then resource.close()
      else
        try resource.close()
        catch
          case cleanup: Throwable => MySqlSchemaAuditor.addSuppressed(primary, cleanup)

  private final class CaptureAllocation(
      owner: ManagedMySqlConnectionOwner,
      account: MySqlFormat3ReadBudget,
      worker: Thread
  ) extends MySqlSchemaAuditor.AuditAllocation(account):
    override protected def requireOwner(): Unit =
      if Thread.currentThread() ne worker then throw new IllegalStateException("managed catalog allocation requires its original worker")
      if account.isClosed then throw new IllegalStateException("managed catalog DATA allocation budget is closed")
      if owner.state() != ManagedMySqlConnectionOwner.State.OPEN || !owner.retainsConnection() then
        throw new IllegalStateException("managed catalog allocation owner no longer holds the original connection")

    override protected def requireObservationOwner(value: UnverifiedMySqlSchemaObservation): Unit =
      throw new IllegalStateException("managed catalog DATA capture cannot issue an audit observation")
