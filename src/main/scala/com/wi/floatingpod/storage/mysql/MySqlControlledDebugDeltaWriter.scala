package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, SQLDataException}
import java.util.{Arrays, UUID}

/** 专用 fixed F3 写入 DAG；只由已实锁 M→B→W 的 controlled runner 调用。 */
private[mysql] object MySqlControlledDebugDeltaWriter:
  import MySqlControlledDebugRows.*
  private type Image=MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot
  private def relation(snapshot:Image,table:String):MySqlFormat3RelationSnapshot=snapshot match
    case legacy:MySqlFormat3FullSnapshot=>legacy.relation(table)
    case deployment:MySqlScriptSchemaSnapshot=>deployment.relation(table)
  private def one(snapshot:Image,table:String,predicate:Vector[MySqlFormat3Cell]=>Boolean):Vector[MySqlFormat3Cell]=snapshot match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugImages.one(legacy,table,predicate)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.one(deployment,table,predicate)
  private def packetLimit(m:MySqlControlledDebugWorkflowMaterialization):Long=m.completePreimage match
    case legacy:MySqlFormat3FullSnapshot=>legacy.maxAllowedPacketBytes
    case deployment:MySqlScriptSchemaSnapshot=>deployment.maxAllowedPacketBytes
  private def imageSchema(m:MySqlControlledDebugWorkflowMaterialization):String=m.completePreimage match
    case legacy:MySqlFormat3FullSnapshot=>legacy.schema
    case deployment:MySqlScriptSchemaSnapshot=>deployment.schema
  def writeReserve(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization): Unit =
    requireInputs(connection,schema,m)
    if m.isDeploymentReceipt then m.requireObservedDeploymentReceipt(MySqlOperationEffectPhase.Reserve,m.deploymentReserveTarget,true)
    else m.requireObserved(MySqlOperationEffectPhase.Reserve,m.reserveTarget,true)
    val before=m.completePreimage
    val target=m.completeReserveTarget
    insert(connection,schema,m,Operation,operation(target,m,m.plan.reserveOperationUuid))
    insert(connection,schema,m,Operation,operation(target,m,m.plan.executeOperationUuid))
    val held=reservation(target,m)
    val old=findByKey(before,Reservation,held)
    if old==null then insert(connection,schema,m,Reservation,held)
    else update(connection,schema,m,Reservation,old,held)

  def writeExecute(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization): Unit =
    requireInputs(connection,schema,m)
    if m.isDeploymentReceipt then m.requireObservedDeploymentReceipt(MySqlOperationEffectPhase.Execute,m.deploymentExecuteTarget,true)
    else m.requireObserved(MySqlOperationEffectPhase.Execute,m.executeTarget,true)
    val before=m.completeReserveTarget
    val target=m.completeExecuteTarget
    if m.activation then
      updateFromImages(connection,schema,m,Domain,pod(target,m,Domain))
      updateFromImages(connection,schema,m,Intent,intent(target,m))
      updateFromImages(connection,schema,m,Locator,pod(target,m,Locator))
    else
      insert(connection,schema,m,Domain,pod(target,m,Domain))
      val sealedRow=footprint(target,m)
      m.originalRoot.reserveRetainedMemory(2048L,"controlled intermediate SQL DRAFT footprint map")
      val draft=MySqlControlledDebugRows.update(sealedRow,Footprint,Map("seal_state"->text("DRAFT")),m.originalRoot)
      insert(connection,schema,m,Footprint,draft)
      val chunks=relation(target,Chunk).rows
      var index=0
      var written=0
      while index<chunks.size do
        val candidate=chunks(index)
        if exactFootprint(candidate,m,Chunk) then
          insert(connection,schema,m,Chunk,candidate)
          written=Math.addExact(written,1)
        index+=1
      if written!=25 then fail("fixed footprint did not write exactly its retained25 chunks")
      update(connection,schema,m,Footprint,draft,sealedRow)
      insert(connection,schema,m,Intent,intent(target,m))
      insert(connection,schema,m,Locator,pod(target,m,Locator))
    // DDL7 窄 release guard 要求 exact execute O 已 COMMITTED，不能用 reserve O 代替。
    update(connection,schema,m,Operation,operation(before,m,m.plan.executeOperationUuid),operation(target,m,m.plan.executeOperationUuid))
    update(connection,schema,m,Reservation,reservation(before,m),reservation(target,m))

  private def requireInputs(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization): Unit =
    if connection==null || m==null || schema==null || !schema.matches("[a-z][a-z0-9_]{0,63}") ||
      schema!=imageSchema(m) || connection.isClosed || connection.getAutoCommit ||
      connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
      fail("active exact controlled same-connection RR transaction required")
    m.verify()

  private def pod(snapshot: Image,m: MySqlControlledDebugWorkflowMaterialization,table: String): Vector[MySqlFormat3Cell] =
    one(snapshot,table,row => uuidMatches(row,table,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,table,if table==Domain then "uuid" else "pod_uuid",m.request.podUuid))
  private def intent(snapshot: Image,m: MySqlControlledDebugWorkflowMaterialization): Vector[MySqlFormat3Cell] =
    one(snapshot,Intent,row => uuidMatches(row,Intent,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,Intent,"pod_uuid",m.request.podUuid) && uuidMatches(row,Intent,"attempt_uuid",m.request.attemptUuid))
  private def footprint(snapshot: Image,m: MySqlControlledDebugWorkflowMaterialization): Vector[MySqlFormat3Cell] =
    one(snapshot,Footprint,row=>exactFootprint(row,m,Footprint))
  private def exactFootprint(row: Vector[MySqlFormat3Cell],m: MySqlControlledDebugWorkflowMaterialization,table: String): Boolean =
    uuidMatches(row,table,"binding_uuid",m.request.expectedBinding.bindingUuid) && uuidMatches(row,table,"pod_uuid",m.request.podUuid) &&
      uuidMatches(row,table,"attempt_uuid",m.request.attemptUuid) && uuidMatches(row,table,"footprint_uuid",m.request.footprintUuid)
  private def operation(snapshot: Image,m: MySqlControlledDebugWorkflowMaterialization,id: UUID): Vector[MySqlFormat3Cell] =
    one(snapshot,Operation,row=>uuidMatches(row,Operation,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      uuidMatches(row,Operation,"operation_uuid",id))
  private def reservation(snapshot: Image,m: MySqlControlledDebugWorkflowMaterialization): Vector[MySqlFormat3Cell] =
    one(snapshot,Reservation,row=>uuidMatches(row,Reservation,"binding_uuid",m.request.expectedBinding.bindingUuid) &&
      value(row,Reservation,"resource_kind")==text("POD") && uuidMatches(row,Reservation,"resource_pod_uuid",m.request.podUuid))
  private def uuidMatches(row: Vector[MySqlFormat3Cell],table: String,column: String,id: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(value(row,table,column),id)

  private def findByKey(snapshot: Image,table: String,row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
    val shape=MySqlFormat3RelationShapes.forTable(table)
    val rows=relation(snapshot,table).rows
    var result: Vector[MySqlFormat3Cell]=null
    var index=0
    while index<rows.size do
      if MySqlControlledDebugDelta.compareKey(rows(index),row,shape)==0 then
        if result!=null then fail("duplicate actual fixed primary key")
        result=rows(index)
      index+=1
    result

  private def updateFromImages(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization,
      table: String,after: Vector[MySqlFormat3Cell]): Unit =
    val before=findByKey(m.completeReserveTarget,table,after)
    if before==null then fail("controlled CAS lost its original row")
    update(connection,schema,m,table,before,after)

  private def insert(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization,
      table: String,row: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val shape=MySqlFormat3RelationShapes.forTable(table)
      val columns=shape.columns.map(c=>"`"+c.name+"`").mkString(",")
      val placeholders=shape.columns.map(_=>"?").mkString(",")
      val sql="INSERT INTO `"+schema+"`.`"+table+"` ("+columns+") VALUES ("+placeholders+")"
      execute(connection,m,shape,sql,shape.columns.indices.map(index=>(index,row(index))).toVector,budget)
    }

  private def update(connection: Connection,schema: String,m: MySqlControlledDebugWorkflowMaterialization,
      table: String,before: Vector[MySqlFormat3Cell],after: Vector[MySqlFormat3Cell]): Unit =
    withStatementBudget(m,table) { budget =>
      val shape=MySqlFormat3RelationShapes.forTable(table)
      val changed=shape.columns.indices.filter(index => !shape.primaryKey.contains(shape.columns(index).name) && before(index)!=after(index)).toVector
      if changed.isEmpty then fail("controlled CAS has no changed field")
      val guards=table match
        case Domain => Vector("state_version","is_active","last_mutation_operation_uuid")
        case Footprint => Vector("seal_state","created_operation_uuid")
        case Intent => Vector("intent_revision","phase","last_mutation_operation_uuid")
        case Locator => Vector("locator_revision","current_attempt_uuid","current_footprint_uuid","last_mutation_operation_uuid")
        case Operation => Vector("operation_state","operation_fingerprint")
        case Reservation => Vector("reservation_revision","reservation_state","workflow_uuid","operation_uuid","last_mutation_operation_uuid")
        case _ => fail("table is outside fixed controlled update DAG")
      val predicates=shape.primaryKey++guards
      val sql="UPDATE `"+schema+"`.`"+table+"` SET "+changed.map(i=>"`"+shape.columns(i).name+"`=?").mkString(",")+
        " WHERE "+predicates.map(name=>"`"+name+"` <=> ?").mkString(" AND ")
      val values=changed.map(index=>(index,after(index)))++predicates.map { name =>
        val index=shape.columns.indexWhere(_.name==name)
        if index<0 then fail("fixed CAS predicate column disappeared")
        (index,before(index))
      }
      execute(connection,m,shape,sql,values,budget)
    }

  private def withStatementBudget(m: MySqlControlledDebugWorkflowMaterialization,table: String)
      (body: MySqlFormat3ReadBudget=>Unit): Unit =
    MySqlRecoveryCanonicalBytes.withComparisonBudget(m.originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(m.originalRoot.profile)
      child.reserveTemporaryMemory(65536L,"controlled fixed SQL text/index/binding containers "+table)
      var primary: Throwable = null
      try body(child)
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try child.releaseTemporaryMemory(65536L,"controlled fixed SQL containers released")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
    }

  private def execute(connection: Connection,m: MySqlControlledDebugWorkflowMaterialization,shape: MySqlFormat3RelationShape,
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
    if packet>Math.min(packetLimit(m),budget.profile.maxSqlPacketBytes()) then
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
              val copy=value.copyBytes(budget,"controlled SQL binary binding")
              arrays+=copy;statement.setBytes(parameter,copy)
            case MySqlFormat3Cell.SignedInteger(value) =>
              if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                if value<Int.MinValue.toLong || value>Int.MaxValue.toLong then fail("fixed INT binding overflow")
                statement.setInt(parameter,value.toInt)
              else statement.setLong(parameter,value)
            case MySqlFormat3Cell.Floating(bits) => statement.setDouble(parameter,java.lang.Double.longBitsToDouble(bits))
            case MySqlFormat3Cell.DateTime(value) => statement.setObject(parameter,value)
          index+=1
        if statement.executeUpdate()!=1 then fail("exact controlled guarded statement did not change one row")
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

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled fixed F3 SQL DAG: "+reason)
