package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, SQLDataException}
import java.util.{Arrays, UUID}

/** 专用 fixed F3 写入 DAG；只由已实锁 M→B→W 的 inactive runner 调用。 */
private[mysql] object MySqlInactiveDomainDeltaWriter:
  import MySqlControlledDebugRows.*
  def writeReserve(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization): Unit =
    requireInputs(connection,schema,m)
    m.requireObserved(MySqlOperationEffectPhase.Reserve,m.reserveTarget,true)
    val before=m.preimage
    val target=m.reserveTarget
    insert(connection,schema,m,Operation,operation(target,m,m.plan.reserveOperationUuid))
    insert(connection,schema,m,Operation,operation(target,m,m.plan.executeOperationUuid))
    val held=reservation(target,m)
    val old=findByKey(before,Reservation,held)
    if old==null then insert(connection,schema,m,Reservation,held)
    else update(connection,schema,m,Reservation,old,held)
    // 在 execute O RESERVED / POD HELD 后，用原 NULL/revision 全字段 CAS；W 仍指 reserve O。
    before.relation(Locator).rows.foreach { original =>
      if uuidMatches(original,Locator,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
        uuidMatches(original,Locator,"pod_uuid",m.request.podUuid) && value(original,Locator,"domain_version")==MySqlFormat3Cell.SqlNull then
        update(connection,schema,m,Locator,original,pod(target,m,Locator))
    }

  def writeExecute(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization): Unit =
    requireInputs(connection,schema,m)
    m.requireObserved(MySqlOperationEffectPhase.Execute,m.executeTarget,true)
    val before=m.reserveTarget
    val target=m.executeTarget
    updateFromImages(connection,schema,m,Domain,pod(target,m,Domain))
    val locatorBefore=before.relation(Locator).rows.filter(row =>
      uuidMatches(row,Locator,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,Locator,"pod_uuid",m.request.podUuid))
    locatorBefore match
      case Vector() => ()
      case Vector(_) => updateFromImages(connection,schema,m,Locator,pod(target,m,Locator))
      case _ => fail("inactive update has duplicate original locator")
    // DDL6 DOMAIN_MUTATION release requires execute O to be COMMITTED before R becomes RELEASED.
    update(connection,schema,m,Operation,operation(before,m,m.plan.executeOperationUuid),operation(target,m,m.plan.executeOperationUuid))
    update(connection,schema,m,Reservation,reservation(before,m),reservation(target,m))

  private def requireInputs(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization): Unit =
    if connection==null || m==null || schema==null || !schema.matches("[a-z][a-z0-9_]{0,63}") ||
      schema!=m.preimage.schema || connection.isClosed || connection.getAutoCommit ||
      connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
      fail("active exact inactive same-connection RR transaction required")
    m.verify()

  private def pod(snapshot: MySqlFormat3FullSnapshot,m: MySqlInactiveDomainWorkflowMaterialization,table: String): Vector[MySqlFormat3Cell] =
    MySqlControlledDebugImages.one(snapshot,table,row => uuidMatches(row,table,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,table,if table==Domain then "uuid" else "pod_uuid",m.request.podUuid))
  private def operation(snapshot: MySqlFormat3FullSnapshot,m: MySqlInactiveDomainWorkflowMaterialization,id: UUID): Vector[MySqlFormat3Cell] =
    MySqlControlledDebugImages.one(snapshot,Operation,row=>uuidMatches(row,Operation,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,Operation,"operation_uuid",id))
  private def reservation(snapshot: MySqlFormat3FullSnapshot,m: MySqlInactiveDomainWorkflowMaterialization): Vector[MySqlFormat3Cell] =
    MySqlControlledDebugImages.one(snapshot,Reservation,row=>uuidMatches(row,Reservation,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      value(row,Reservation,"resource_kind")==text("POD") && uuidMatches(row,Reservation,"resource_pod_uuid",m.request.podUuid))
  private def uuidMatches(row: Vector[MySqlFormat3Cell],table: String,column: String,id: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(value(row,table,column),id)

  private def findByKey(snapshot: MySqlFormat3FullSnapshot,table: String,row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
    val shape=MySqlFormat3RelationShapes.forTable(table)
    val rows=snapshot.relation(table).rows
    var result: Vector[MySqlFormat3Cell]=null
    var index=0
    while index<rows.size do
      if MySqlControlledDebugDelta.compareKey(rows(index),row,shape)==0 then
        if result!=null then fail("duplicate actual fixed primary key")
        result=rows(index)
      index+=1
    result

  private def updateFromImages(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization,
      table: String,after: Vector[MySqlFormat3Cell]): Unit =
    val before=findByKey(m.reserveTarget,table,after)
    if before==null then fail("inactive CAS lost its original row")
    update(connection,schema,m,table,before,after)

  private def insert(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization,
      table: String,row: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val shape=MySqlFormat3RelationShapes.forTable(table)
      val columns=shape.columns.map(c=>"`"+c.name+"`").mkString(",")
      val placeholders=shape.columns.map(_=>"?").mkString(",")
      val sql="INSERT INTO `"+schema+"`.`"+table+"` ("+columns+") VALUES ("+placeholders+")"
      execute(connection,m,shape,sql,shape.columns.indices.map(index=>(index,row(index))).toVector,budget)
    }

  private def update(connection: Connection,schema: String,m: MySqlInactiveDomainWorkflowMaterialization,
      table: String,before: Vector[MySqlFormat3Cell],after: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val command=guardedUpdate(schema,table,before,after)
      execute(connection,m,command.shape,command.sql,command.values,budget)
    }

  /** 真实 UPDATE 共用的结构 DATA；不执行 JDBC，不发行 admission 或 commit ACK。 */
  private[mysql] final case class GuardedUpdate(shape: MySqlFormat3RelationShape,
      sql: String, values: Vector[(Int,MySqlFormat3Cell)])

  private[mysql] def guardedUpdate(schema: String,table: String,before: Vector[MySqlFormat3Cell],
      after: Vector[MySqlFormat3Cell]): GuardedUpdate =
    if schema==null || !schema.matches("[a-z][a-z0-9_]{0,63}") || table==null || before==null || after==null then
      fail("fixed guarded update inputs required")
    val shape=MySqlFormat3RelationShapes.forTable(table)
    if before.size!=shape.columns.size || after.size!=shape.columns.size then fail("guarded update row width differs")
    val changed=shape.columns.indices.filter(index => !shape.primaryKey.contains(shape.columns(index).name) && before(index)!=after(index)).toVector
    if changed.isEmpty then fail("inactive CAS has no changed field")
    val guards=table match
      case Domain => Vector("owner_uuid","state_version","is_active","last_mutation_operation_uuid")
      case Locator => Vector("domain_version","locator_revision","locator_state","reason_code","current_attempt_uuid","current_footprint_uuid","last_mutation_operation_uuid")
      case Operation => Vector("operation_state","operation_fingerprint")
      case Reservation => Vector("reservation_revision","reservation_state","workflow_uuid","operation_uuid","last_mutation_operation_uuid")
      case _ => fail("table is outside fixed inactive update DAG")
    val predicates=shape.primaryKey++guards
    val sql="UPDATE `"+schema+"`.`"+table+"` SET "+changed.map(i=>"`"+shape.columns(i).name+"`=?").mkString(",")+
      " WHERE "+predicates.map(name=>"`"+name+"` <=> ?").mkString(" AND ")
    val values=changed.map(index=>(index,after(index)))++predicates.map { name =>
      val index=shape.columns.indexWhere(_.name==name)
      if index<0 then fail("fixed CAS predicate column disappeared")
      (index,before(index))
    }
    GuardedUpdate(shape,sql,values)

  private def withStatementBudget(m: MySqlInactiveDomainWorkflowMaterialization,table: String)
      (body: MySqlFormat3ReadBudget=>Unit): Unit =
    MySqlRecoveryCanonicalBytes.withComparisonBudget(m.originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(m.originalRoot.profile)
      child.reserveTemporaryMemory(65536L,"inactive fixed SQL text/index/binding containers "+table)
      var primary: Throwable = null
      try body(child)
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try child.releaseTemporaryMemory(65536L,"inactive fixed SQL containers released")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    }

  private def execute(connection: Connection,m: MySqlInactiveDomainWorkflowMaterialization,shape: MySqlFormat3RelationShape,
      sql: String,values: Vector[(Int,MySqlFormat3Cell)],budget: MySqlFormat3ReadBudget): Unit =
    var packet=MySqlFormat3Utf8.byteLength(sql)
    var index=0
    while index<values.size do
      val bytes=values(index)._2 match
        case MySqlFormat3Cell.SqlNull => 1L
        case MySqlFormat3Cell.Text(value) => Math.addExact(MySqlFormat3Utf8.byteLength(value),16L)
        case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong,16L)
        case _ => 32L
      packet=Math.addExact(packet,bytes);index+=1
    if packet>Math.min(m.preimage.maxAllowedPacketBytes,budget.profile.maxSqlPacketBytes()) then
      fail("fixed write framing exceeds actual observed/profile packet bound")
    val arrays=scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var primary: Throwable=null
    try
      MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
        var index=0
        while index<values.size do
          val (columnIndex,cell)=values(index)
          val parameter=index+1
          val column=shape.columns(columnIndex)
          cell match
            case MySqlFormat3Cell.SqlNull => statement.setNull(parameter,column.jdbcType)
            case MySqlFormat3Cell.Text(value) => statement.setString(parameter,value)
            case MySqlFormat3Cell.Binary(value) =>
              val copy=value.copyBytes(budget,"inactive SQL binary binding")
              arrays+=copy;statement.setBytes(parameter,copy)
            case MySqlFormat3Cell.SignedInteger(value) =>
              if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                if value<Int.MinValue.toLong || value>Int.MaxValue.toLong then fail("fixed INT binding overflow")
                statement.setInt(parameter,value.toInt)
              else statement.setLong(parameter,value)
            case MySqlFormat3Cell.Floating(bits) => statement.setDouble(parameter,java.lang.Double.longBitsToDouble(bits))
            case MySqlFormat3Cell.DateTime(value) => statement.setObject(parameter,value)
          index+=1
        if statement.executeUpdate()!=1 then fail("exact inactive guarded statement did not change one row")
      }
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally
      try
        arrays.foreach(bytes=>Arrays.fill(bytes,0.toByte))
        arrays.clear()
      catch case cleanup: Throwable =>
        if primary==null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)
      // Copies were charged to lexical child and lose all references before its close。

  private def fail(reason: String): Nothing = throw new SQLDataException("inactive fixed F3 SQL DAG: "+reason)
