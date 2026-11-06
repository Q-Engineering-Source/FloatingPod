package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.storage.mysql.MySqlDeploymentOperationRows.{ColumnValue, ColumnValues}
import java.lang.{StringBuilder as JavaStringBuilder}
import java.nio.ByteBuffer
import java.sql.{Connection, ResultSet, SQLDataException, SQLNonTransientConnectionException}
import java.util.UUID

/** 只消费调用方已准入的原连接、事务和单列 root；不发行 schema 或业务资格。 */
private[mysql] final class MySqlDeploymentOriginalReadJdbc(
    verified: VerifiedMySqlConnection,
    scope: MySqlAuditBootScope,
    schema: String,
    expectedProfile: SourceLimitProfile
):
  require(verified != null && scope != null && expectedProfile != null,
    "original deployment reader requires verified connection, owned scope and explicit profile")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "unsafe fixed deployment schema")

  /** root 的尝试权先于本次分配、观测和 JDBC；跨 reader 也不产生第二次 bootstrap。 */
  def readWithinOwnedTransaction(
      operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation,
      root: MySqlFormat3ReadBudget
  ): Option[MySqlOriginalDeploymentOperation] =
    require(root != null, "original retained root required")
    root.claimDeploymentOriginalJdbcAttempt()
    try
      require(operation != null && operation.actorUuid != null && operation.originalOperationId != null,
        "exact original actor/operation lookup required")
      require(scope.ownerThread eq Thread.currentThread(), "original deployment read requires its actual IO worker")
      val connection = verified.connection()
      require(scope.verifiedConnection.contains(verified) && (scope.connectionToken eq connection) &&
        (scope.connectionIdentity eq verified.identity), "original deployment read lost its verified connection scope")
      require(schema == verified.identity.catalog && (root.profile eq expectedProfile),
        "original deployment read differs from actual catalog/profile")
      if connection.getAutoCommit || !connection.isReadOnly ||
        connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("original deployment read requires an existing read-only REPEATABLE READ transaction")

      val bootstrap = "SELECT @@SESSION.max_allowed_packet"
      MySqlDeploymentOriginalReadJdbc.requirePacket(bootstrap.length.toLong + 16L, expectedProfile.maxSqlPacketBytes())
      val packet = MySqlDeploymentOriginalReadJdbc.withTemporary(root, 512L, "deployment packet observation workspace") {
        MySqlBusinessRows.using(connection.prepareStatement(bootstrap)) { statement =>
          MySqlBusinessRows.using(statement.executeQuery()) { result =>
            if !result.next() then throw new SQLDataException("deployment max_allowed_packet observation is missing")
            val value = result.getLong(1)
            if result.wasNull() || value <= 0L then throw new SQLDataException("deployment max_allowed_packet observation is not positive")
            if result.next() then throw new SQLDataException("deployment max_allowed_packet observation has multiple rows")
            value
          }
        }
      }
      MySqlDeploymentOriginalReadJdbc.requirePacket(bootstrap.length.toLong + 16L, packet)
      MySqlDeploymentOriginalReadJdbc.withTemporary(root, 512L, "deployment original observation construction") {
        val observation = MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
        val observationScope = MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
        root.initializeDeploymentOriginalRead(schema, observation, observationScope, packet)
      }

      val sqlBytes = MySqlDeploymentOriginalReadJdbc.selectLength(schema)
      // 保守覆盖 prepare/execute 协议、三个 binary 参数的类型/长度头及 4+16+16 payload。
      val wireBytes = Math.addExact(sqlBytes, 164L)
      MySqlDeploymentOriginalReadJdbc.requirePacket(wireBytes, expectedProfile.maxSqlPacketBytes())
      MySqlDeploymentOriginalReadJdbc.requirePacket(wireBytes, packet)
      val workspace = Math.addExact(1024L, Math.multiplyExact(sqlBytes, 4L))
      MySqlDeploymentOriginalReadJdbc.withTemporary(root, workspace, "deployment fixed SELECT, binary parameters and JDBC workspace") {
        val sql = MySqlDeploymentOriginalReadJdbc.selectSql(schema)
        val namespace = Array[Byte](70, 80, 68, 67)
        val actor = uuidBytes(operation.actorUuid)
        val original = uuidBytes(operation.originalOperationId)
        MySqlBusinessRows.using(connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) { statement =>
          statement.setFetchSize(Integer.MIN_VALUE)
          statement.setBytes(1, namespace)
          statement.setBytes(2, actor)
          statement.setBytes(3, original)
          MySqlBusinessRows.using(statement.executeQuery()) { result =>
            MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, operation, root)
          }
        }
      }
    catch
      case primary: Throwable =>
        try root.recordDeploymentOriginalJdbcFailure(primary)
        catch case recording: Throwable => MySqlOwnedSessionLifecycle.suppress(primary, recording)
        throw primary

  private def uuidBytes(value: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()

private[mysql] object MySqlDeploymentOriginalReadJdbc:
  private val table = "fp_deployment_operation"
  private val suffix = "` WHERE namespace=? AND actor_uuid=? AND operation_uuid=?"

  private def withTemporary[A](root: MySqlFormat3ReadBudget, bytes: Long, context: String)(body: => A): A =
    root.reserveTemporaryMemory(bytes, context)
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      try root.releaseTemporaryMemory(bytes, context)
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def selectLength(schema: String): Long =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    var bytes = 7L // SELECT 后的空格。
    var index = 0
    while index < shape.columns.size do
      bytes = Math.addExact(bytes, shape.columns(index).name.length.toLong + 2L)
      index += 1
    bytes = Math.addExact(bytes, shape.columns.size.toLong - 1L)
    Math.addExact(bytes, " FROM `".length.toLong + schema.length.toLong + "`.`".length.toLong + table.length.toLong + suffix.length.toLong)

  private def selectSql(schema: String): String =
    val columns = MySqlDeploymentReceiptRelationShapes.operation.columns
    val builder = new JavaStringBuilder()
    builder.append("SELECT ")
    var index = 0
    while index < columns.size do
      if index > 0 then builder.append(',')
      builder.append('`').append(columns(index).name).append('`')
      index += 1
    builder.append(" FROM `").append(schema).append("`.`").append(table).append(suffix).toString

  private def requirePacket(bytes: Long, maximum: Long): Unit =
    if bytes <= 0L || maximum <= 0L || bytes > maximum then
      throw new SQLDataException("deployment fixed query exceeds the explicit SQL packet limit")

  /** 生产 ResultSet 的同一数据解码接缝；不打开 SQL，不发行 root 或连接资格。资源归调用方关闭。 */
  private[mysql] def decodeResultSet(
      result: ResultSet,
      operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation,
      root: MySqlFormat3ReadBudget
  ): Option[MySqlOriginalDeploymentOperation] =
    require(result != null && operation != null && operation.actorUuid != null && operation.originalOperationId != null && root != null,
      "actual result, original lookup and root required")
    root.requireDeploymentOriginalReadLayout()
    if !root.isInitialized then throw new SQLNonTransientConnectionException("deployment result requires its initialized original root")
    withTemporary(root, 4096L, "deployment metadata, per-cell context and decoder workspace") {
      decodeInitializedResultSet(result, operation, root)
    }

  private def decodeInitializedResultSet(result: ResultSet,
      operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation,
      root: MySqlFormat3ReadBudget): Option[MySqlOriginalDeploymentOperation] =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    if shape.table != table || shape.columns.size != 31 || MySqlDeploymentOperationRows.columnNames.size != 31 then
      throw new SQLDataException("deployment SELECT shape differs from the fixed row decoder")
    var index = 0
    while index < 31 do
      if shape.columns(index).name != MySqlDeploymentOperationRows.columnNames(index) then
        throw new SQLDataException("deployment SELECT column order differs from the fixed row decoder")
      index += 1
    MySqlFormat3FullSnapshotSql.validateMetadata(result.getMetaData, shape)
    if !result.next() then return None
    root.beginRow(31, table, 0L)
    var primary: Throwable = null
    try
      val values = Vector.newBuilder[ColumnValue]
      index = 0
      while index < 31 do
        val column = shape.columns(index)
        val jdbcIndex = index + 1
        val context = s"$table row 0 column ${column.name}"
        column.kind match
          case MySqlFormat3SqlKind.Binary =>
            val stream = result.getBinaryStream(jdbcIndex)
            if stream == null then values += nullValue(column, root, context)
            else values += ColumnValue.Binary(MySqlFormat3CellStreamDecoder.readBinary(stream, column.maximumLength, root, context))
          case MySqlFormat3SqlKind.Integer =>
            val value = result.getLong(jdbcIndex)
            if result.wasNull() then values += nullValue(column, root, context)
            else
              column.requireSignedInteger(value, context)
              root.beginFixedCell(8, column.integerPacketWidth, context)
              values += ColumnValue.Integer(value)
          case _ => throw new SQLDataException("deployment row contains an unsupported frozen SQL kind")
        index += 1
      val row = MySqlDeploymentOperationRows.decode(ColumnValues(values.result()), operation.actorUuid, operation.originalOperationId, root)
      // 第二行只观察存在性，不分配、解码或向 row 预算收费。
      if result.next() then throw new SQLDataException("deployment original lookup returned more than one row")
      root.reserveRetainedMemory(160L, "deployment immutable result projection and Option")
      Some(new MySqlOriginalDeploymentOperation(row.actorUuid, row.operationUuid, row.request, row.receipt, row.rowRevision))
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      // 只有 beginRow 成功后进入此 finally；失败的行也必须结束，保留原 root 的释放路径。
      try root.endRow(table, 0L)
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def nullValue(column: MySqlFormat3ColumnShape, root: MySqlFormat3ReadBudget, context: String): ColumnValue =
    if !column.nullable then throw new SQLDataException(s"$context is SQL NULL but required")
    root.beginNullCell(context)
    ColumnValue.SqlNull
