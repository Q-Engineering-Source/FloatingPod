package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, PreparedStatement, SQLDataException}
import java.util.Arrays
import MySqlModeOperationEffectCodec.{allocationBudget,schema,connection,observationScope,
  maxAllowedPacketBytes,limitProfile,relation,shapes}

/** 仅把已验证的 owned-mode 两阶段 full20 差量写回；不接受任意表、行或字段选择器。 */
private[mysql] object MySqlDriftFormat3DeltaWriter:
  private final case class InsertRow(table: String, row: Vector[MySqlFormat3Cell])
  private final case class UpdateRow(table: String, before: Vector[MySqlFormat3Cell], after: Vector[MySqlFormat3Cell])
  private val ReservationTable = "fp_recovery_reservation"
  private val OperationTable = "fp_recovery_operation"

  def writeReserve(
      connection: Connection,
      schema: String,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit =
    requireInputs(connection, schema, materialization)
    val before = materialization.completePreimage
    val target = materialization.completeReserveTarget
    val effect = materialization.effects(0)
    effect.verifyPreimage(before)
    effect.verifyReadback(target)
    val reservationDelta = exactReserveReservationDelta(before, target, materialization)
    val changes = validateChanges(before, target, Map(
      OperationTable -> (2, 0),
      ReservationTable -> reservationDelta,
      "fp_runtime_writer" -> (0, 1)
    ))
    // FKs require both immutable operation identities before their reservation row.
    changes.inserts.filter(_.table == OperationTable).foreach(insert(connection, schema, before, _))
    changes.inserts.filter(_.table == ReservationTable).foreach(insert(connection, schema, before, _))
    changes.updates.filter(_.table == ReservationTable).foreach(update(connection, schema, before, _))

  def writeExecute(
      connection: Connection,
      schema: String,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit =
    requireInputs(connection, schema, materialization)
    val before = materialization.completeReserveTarget
    val target = materialization.completeExecuteTarget
    val effect = materialization.effects(1)
    effect.verifyPreimage(before)
    effect.verifyReadback(target)
    val changes = validateChanges(before, target, Map(
      "floating_pod_state" -> (0, 1),
      "physical_recovery_locator" -> (0, 1),
      "fp_recovery_operation" -> (0, 1),
      "fp_recovery_reservation" -> (0, 1),
      "fp_runtime_writer" -> (0, 1)
    ))
    // The frozen DDL6 guard order is domain → locator → execute O COMMITTED → R RELEASED.
    Vector("floating_pod_state", "physical_recovery_locator", "fp_recovery_operation",
      "fp_recovery_reservation").foreach { table =>
      changes.updates.filter(_.table == table).foreach(update(connection, schema, before, _))
    }

  private def validateChanges(
      before: MySqlModeOperationEffectCodec.Image,
      target: MySqlModeOperationEffectCodec.Image,
      expected: Map[String, (Int, Int)]
  ): Changes =
    requireSameSnapshotContext(before, target)
    val inserts = Vector.newBuilder[InsertRow]
    val updates = Vector.newBuilder[UpdateRow]
    before.shapes.foreach { shape =>
      val oldRelation = before.relation(shape.table)
      val newRelation = target.relation(shape.table)
      if !expected.contains(shape.table) && oldRelation.rows != newRelation.rows then
        throw new SQLDataException("owned-mode phase changed a relation outside its exact F3 delta allowlist: " + shape.table)
      if expected.contains(shape.table) then
        val (relationInserts, relationUpdates) = diffRelation(shape, oldRelation.rows, newRelation.rows)
        val (expectedInserts, expectedUpdates) = expected(shape.table)
        if relationInserts.size != expectedInserts || relationUpdates.size != expectedUpdates then
          throw new SQLDataException("owned-mode phase row delta differs from the exact frozen F3 transition for " + shape.table)
        inserts ++= relationInserts.map(row => InsertRow(shape.table, row))
        updates ++= relationUpdates.map { case (oldRow, newRow) => UpdateRow(shape.table, oldRow, newRow) }
    }
    Changes(inserts.result(), updates.result())

  private final case class Changes(inserts: Vector[InsertRow], updates: Vector[UpdateRow])

  /** Only create a POD hold or advance one exact terminal RELEASED row; never replace a held workflow. */
  private def exactReserveReservationDelta(
      before: MySqlModeOperationEffectCodec.Image,
      target: MySqlModeOperationEffectCodec.Image,
      materialization: MySqlDriftWorkflowMaterialization
  ): (Int, Int) =
    val request = MySqlDriftOperationEffectCodec.driftRequest(materialization.exactOperation)
    val plan = materialization.exactPlan
    val targetRows = target.relation(ReservationTable).rows.filter { row =>
      cell(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("POD") &&
        MySqlDriftOperationEffectCodec.uuidEquals(cell(row, ReservationTable, "binding_uuid"), request.binding.bindingUuid) &&
        MySqlDriftOperationEffectCodec.uuidEquals(cell(row, ReservationTable, "resource_pod_uuid"), request.podUuid)
    }
    if targetRows.size != 1 then throw new SQLDataException("reserve target must retain one exact POD reservation row")
    val targetRow = targetRows.head
    if cell(targetRow, ReservationTable, "reservation_state") != MySqlFormat3Cell.Text("HELD") ||
      !MySqlDriftOperationEffectCodec.uuidEquals(cell(targetRow, ReservationTable, "workflow_uuid"), plan.workflowUuid) ||
      !MySqlDriftOperationEffectCodec.uuidEquals(cell(targetRow, ReservationTable, "operation_uuid"), plan.executeOperationUuid) ||
      !MySqlDriftOperationEffectCodec.uuidEquals(cell(targetRow, ReservationTable, "last_mutation_operation_uuid"), plan.reserveOperationUuid) then
      throw new SQLDataException("reserve target does not hold the exact Main tracked drift workflow and operation identities")

    val shape = MySqlFormat3RelationShapes.forTable(ReservationTable)
    val keyIndexes = shape.primaryKey.map { name =>
      val index = shape.columns.indexWhere(_.name == name)
      if index < 0 then throw new SQLDataException("frozen reservation primary key disappeared")
      index
    }
    val targetKey = keyIndexes.map(targetRow(_))
    val prior = before.relation(ReservationTable).rows.filter(row => keyIndexes.map(row(_)) == targetKey)
    if prior.size > 1 then throw new SQLDataException("duplicate exact POD reservation primary key in the locked full20 image")
    prior.headOption match
      case None =>
        if integer(targetRow, ReservationTable, "reservation_revision") != 0L then
          throw new SQLDataException("new POD reservation must start at exact revision zero")
        (1, 0)
      case Some(old) =>
        if cell(old, ReservationTable, "reservation_state") != MySqlFormat3Cell.Text("RELEASED") then
          throw new java.sql.SQLTransactionRollbackException("held or unresolved POD reservation keeps its existing owner workflow")
        val oldRevision = integer(old, ReservationTable, "reservation_revision")
        if oldRevision < 0L || integer(targetRow, ReservationTable, "reservation_revision") != Math.addExact(oldRevision, 1L) then
          throw new SQLDataException("reused POD reservation must advance its exact prior revision once")
        Vector("binding_uuid", "resource_kind", "resource_key_hash", "resource_key", "resource_pod_uuid", "resource_unit_uuid")
          .foreach { column =>
            if cell(old, ReservationTable, column) != cell(targetRow, ReservationTable, column) then
              throw new SQLDataException("reused POD reservation changed its fixed resource identity")
          }
        (0, 1)

  private def cell(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    val index = shape.columns.indexWhere(_.name == column)
    if row == null || index < 0 || row.size != shape.columns.size then
      throw new SQLDataException("frozen typed SQL row lacks " + table + "." + column)
    row(index)

  private def integer(row: Vector[MySqlFormat3Cell], table: String, column: String): Long = cell(row, table, column) match
    case MySqlFormat3Cell.SignedInteger(value) => value
    case _ => throw new SQLDataException("frozen typed SQL integer is invalid for " + table + "." + column)

  private def diffRelation(
      shape: MySqlFormat3RelationShape,
      before: Vector[Vector[MySqlFormat3Cell]],
      target: Vector[Vector[MySqlFormat3Cell]]
  ): (Vector[Vector[MySqlFormat3Cell]], Vector[(Vector[MySqlFormat3Cell], Vector[MySqlFormat3Cell])]) =
    val keyIndexes = shape.primaryKey.map(name =>
      shape.columns.indexWhere(_.name == name) match
        case index if index >= 0 => index
        case _ => throw new SQLDataException("frozen mode delta primary key disappeared for " + shape.table))
    val insertRows = Vector.newBuilder[Vector[MySqlFormat3Cell]]
    val updateRows = Vector.newBuilder[(Vector[MySqlFormat3Cell], Vector[MySqlFormat3Cell])]
    var oldIndex = 0
    var newIndex = 0
    while oldIndex < before.size || newIndex < target.size do
      if oldIndex >= before.size then
        insertRows += target(newIndex)
        newIndex += 1
      else if newIndex >= target.size then
        throw new SQLDataException("owned-mode F3 phase cannot delete historical rows from " + shape.table)
      else
        val compared = compareKey(before(oldIndex), target(newIndex), keyIndexes, shape)
        if compared < 0 then
          throw new SQLDataException("owned-mode F3 phase cannot delete historical rows from " + shape.table)
        else if compared > 0 then
          insertRows += target(newIndex)
          newIndex += 1
        else
          if before(oldIndex) != target(newIndex) then updateRows += ((before(oldIndex), target(newIndex)))
          oldIndex += 1
          newIndex += 1
    (insertRows.result(), updateRows.result())

  private def compareKey(
      left: Vector[MySqlFormat3Cell],
      right: Vector[MySqlFormat3Cell],
      indexes: Vector[Int],
      shape: MySqlFormat3RelationShape
  ): Int =
    var index = 0
    while index < indexes.size do
      val columnIndex = indexes(index)
      val compared = (left(columnIndex), right(columnIndex)) match
        case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
        case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
        case _ => throw new SQLDataException("mode delta primary key must remain exact binary/integer for " + shape.table)
      if compared != 0 then return compared
      index += 1
    0

  private def insert(
      connection: Connection,
      schema: String,
      snapshot: MySqlModeOperationEffectCodec.Image,
      row: InsertRow
  ): Unit =
    val shape = MySqlFormat3RelationShapes.forTable(row.table)
    val columns = shape.columns.map(_.name)
    val sql = "INSERT INTO `" + schema + "`.`" + row.table + "` (" +
      columns.map(name => "`" + name + "`").mkString(",") + ") VALUES (" +
      columns.map(_ => "?").mkString(",") + ")"
    bindAndExecute(connection, snapshot, shape, sql,
      row.row.zipWithIndex.map { case (cell, index) => (index, cell) }, expectChangedOne = true)

  private def update(
      connection: Connection,
      schema: String,
      snapshot: MySqlModeOperationEffectCodec.Image,
      change: UpdateRow
  ): Unit =
    val shape = MySqlFormat3RelationShapes.forTable(change.table)
    val keyNames = shape.primaryKey.toSet
    val changedColumns = shape.columns.indices.filter(index =>
      !keyNames(shape.columns(index).name) && change.before(index) != change.after(index))
    if changedColumns.isEmpty then throw new SQLDataException("owned-mode update has no changed non-key columns")
    val setSql = changedColumns.map(index => "`" + shape.columns(index).name + "`=?").mkString(",")
    val whereSql = shape.primaryKey.map(name => "`" + name + "`=?").mkString(" AND ")
    val sql = "UPDATE `" + schema + "`.`" + change.table + "` SET " + setSql + " WHERE " + whereSql
    val values = changedColumns.toVector.map(index => (index, change.after(index))) ++
      shape.primaryKey.map(name =>
        val index = shape.columns.indexWhere(_.name == name)
        (index, change.before(index)))
    bindAndExecute(connection, snapshot, shape, sql, values, expectChangedOne = true)

  private def bindAndExecute(
      connection: Connection,
      snapshot: MySqlModeOperationEffectCodec.Image,
      shape: MySqlFormat3RelationShape,
      sql: String,
      values: Vector[(Int, MySqlFormat3Cell)],
      expectChangedOne: Boolean
  ): Unit =
    if connection == null || snapshot == null || shape == null || sql == null || values == null then
      throw new IllegalArgumentException("tracked drift SQL delta inputs are required")
    val budget = snapshot.allocationBudget
    val packetLimit = Math.min(snapshot.maxAllowedPacketBytes, snapshot.limitProfile.maxSqlPacketBytes())
    var packetBytes = MySqlFormat3FullSnapshotSql.utf8Length(sql)
    values.foreach { case (_, cell) =>
      val valueBytes = cell match
        case MySqlFormat3Cell.SqlNull => 1L
        case MySqlFormat3Cell.Text(value) => Math.addExact(MySqlFormat3Utf8.byteLength(value), 8L)
        case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong, 8L)
        case MySqlFormat3Cell.SignedInteger(_) | MySqlFormat3Cell.Floating(_) => 16L
        case MySqlFormat3Cell.DateTime(_) => 32L
      packetBytes = Math.addExact(packetBytes, Math.addExact(valueBytes, 8L))
    }
    if packetBytes > packetLimit then throw new SQLDataException("tracked drift F3 write exceeds the exact profile or observed MySQL packet budget")

    val allocated = scala.collection.mutable.ArrayBuffer.empty[(Array[Byte], Long)]
    var primary: Throwable = null
    try
      MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
        values.zipWithIndex.foreach { case ((columnIndex, cell), offset) =>
          val column = shape.columns(columnIndex)
          val parameterIndex = offset + 1
          cell match
            case MySqlFormat3Cell.SqlNull => statement.setNull(parameterIndex, column.jdbcType)
            case MySqlFormat3Cell.Text(value) => statement.setString(parameterIndex, value)
            case MySqlFormat3Cell.Binary(value) =>
              val copy = value.copyBytes(budget, "tracked drift F3 SQL parameter " + shape.table + "." + column.name)
              allocated += ((copy, Math.addExact(copy.length.toLong, 64L)))
              statement.setBytes(parameterIndex, copy)
            case MySqlFormat3Cell.SignedInteger(value) =>
              if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                if value < Int.MinValue.toLong || value > Int.MaxValue.toLong then
                  throw new SQLDataException("tracked drift F3 integer parameter exceeds the frozen JDBC integer range")
                statement.setInt(parameterIndex, value.toInt)
              else statement.setLong(parameterIndex, value)
            case MySqlFormat3Cell.Floating(rawBits) => statement.setDouble(parameterIndex, java.lang.Double.longBitsToDouble(rawBits))
            case MySqlFormat3Cell.DateTime(value) => statement.setObject(parameterIndex, value)
        }
        val changed = statement.executeUpdate()
        if expectChangedOne && changed != 1 then
          throw new SQLDataException("tracked drift F3 guarded row write did not affect exactly one row")
      }
    catch
      case failure: Throwable => primary = failure
    finally
      allocated.foreach { case (bytes, charge) =>
        Arrays.fill(bytes, 0.toByte)
        try budget.releaseRetainedMemory(charge, "tracked drift F3 SQL parameter release")
        catch
          case cleanup: Throwable =>
            if primary == null then primary = cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)
      }
    if primary != null then throw primary

  private def requireInputs(
      connection: Connection,
      schema: String,
      materialization: MySqlDriftWorkflowMaterialization
  ): Unit =
    if connection == null || materialization == null || schema == null || !schema.matches("[a-z][a-z0-9_]{0,63}") then
      throw new IllegalArgumentException("exact tracked drift F3 write inputs are required")
    materialization.verify()

  private def requireSameSnapshotContext(before: MySqlModeOperationEffectCodec.Image, target: MySqlModeOperationEffectCodec.Image): Unit =
    if before == null || target == null || before.schema != target.schema || before.connection != target.connection ||
      before.observationScope != target.observationScope || before.limitProfile != target.limitProfile ||
      before.maxAllowedPacketBytes != target.maxAllowedPacketBytes ||
      ((before,target) match
        case (_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot)=>false
        case (a:MySqlScriptSchemaSnapshot,b:MySqlScriptSchemaSnapshot)=>a.inputs ne b.inputs
        case _=>true) then
      throw new SQLDataException("tracked drift full20 phase images do not share the exact scope, connection and budget")
