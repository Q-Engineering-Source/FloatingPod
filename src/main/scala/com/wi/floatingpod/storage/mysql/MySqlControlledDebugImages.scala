package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 只构造完整 DATA 图像；每次 patch 保留全部未选关系/历史原引用与有序完整行。 */
private[mysql] object MySqlControlledDebugImages:
  def put(snapshot: MySqlScriptSchemaSnapshot,table: String,row: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget,requireExisting: Boolean): MySqlScriptSchemaSnapshot =
    if snapshot == null then fail("original complete Full26 image required")
    val inputs = snapshot.inputs
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    if table == null || row == null then fail("fixed table and complete row required")
    val shape = inputs.forTable(table)
    if row.size != shape.columns.size then fail("complete Full26 row width required")
    val source = snapshot.relation(table).rows
    root.reserveRetainedMemory(Math.addExact(16384L,
      Math.multiplyExact(Math.addExact(source.size.toLong,1L),256L)),"controlled Full26 patch containers")
    var scratch = 16384L
    snapshot.relations.foreach { relation =>
      val perRow = Math.addExact(1024L,Math.multiplyExact(inputs.forTable(relation.table).columns.size.toLong,256L))
      scratch = Math.addExact(scratch,Math.multiplyExact(Math.addExact(relation.rows.size.toLong,1L),perRow))
    }
    root.reserveTemporaryMemory(scratch,"controlled Full26 patch ordering and validation")
    var primary: Throwable = null
    try
      val changed = changedRows(source,row,shape,requireExisting)
      val relations = snapshot.relations.map { relation =>
        if relation.table == table then MySqlFormat3RelationSnapshot(table,changed) else relation
      }
      MySqlScriptSchemaSnapshot.decoded(snapshot.schema,snapshot.connection,snapshot.observationScope,
        snapshot.maxAllowedPacketBytes,inputs,relations,root)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(scratch,"controlled Full26 patch ordering and validation release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  /** H3 history only: the original root validates the source while the registered DATA child owns the rebuilt graph. */
  private[mysql] def put(snapshot: MySqlScriptSchemaSnapshot,table: String,row: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget,requireExisting: Boolean): MySqlScriptSchemaSnapshot =
    if snapshot == null || root == null || root.isClosed || root.isScopedReadBudget || allocation == null ||
      allocation.isClosed || (allocation.profile ne root.profile) || !allocation.isCanonicalChildOf(root) ||
      !allocation.sharesAllocationLedger(root) then fail("exact original Full26 root and same-ledger DATA child required")
    val inputs = snapshot.inputs
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    if table == null || row == null then fail("fixed table and complete row required")
    val shape = inputs.forTable(table)
    if row.size != shape.columns.size then fail("complete Full26 row width required")
    val source = snapshot.relation(table).rows
    allocation.reserveRetainedMemory(Math.addExact(16384L,
      Math.multiplyExact(Math.addExact(source.size.toLong,1L),256L)),"controlled Full26 plan DATA patch containers")
    var scratch = 16384L
    snapshot.relations.foreach { relation =>
      val perRow = Math.addExact(1024L,Math.multiplyExact(inputs.forTable(relation.table).columns.size.toLong,256L))
      scratch = Math.addExact(scratch,Math.multiplyExact(Math.addExact(relation.rows.size.toLong,1L),perRow))
    }
    allocation.reserveTemporaryMemory(scratch,"controlled Full26 plan DATA patch ordering and validation")
    var primary: Throwable = null
    try
      val changed = changedRows(source,row,shape,requireExisting)
      val relations = snapshot.relations.map { relation =>
        if relation.table == table then MySqlFormat3RelationSnapshot(table,changed) else relation
      }
      MySqlScriptSchemaSnapshot.decoded(snapshot.schema,snapshot.connection,snapshot.observationScope,
        snapshot.maxAllowedPacketBytes,inputs,relations,allocation)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try allocation.releaseTemporaryMemory(scratch,"controlled Full26 plan DATA patch ordering and validation release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  def put(snapshot: MySqlFormat3FullSnapshot,table: String,row: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget,requireExisting: Boolean): MySqlFormat3FullSnapshot =
    if snapshot==null || table==null || row==null || root==null || root.isClosed || root.isScopedReadBudget ||
      (snapshot.limitProfile ne root.profile) || !root.sharesAllocationLedger(snapshot.allocationBudget) then
      fail("exact open original full20 root required")
    val shape=MySqlFormat3RelationShapes.forTable(table)
    if row.size!=shape.columns.size then fail("complete row width required")
    val source=snapshot.relation(table).rows
    // 新容器只共享既有 immutable cells；新增 cells 由其 producer 原先计费。
    val retained=Math.addExact(16384L,Math.multiplyExact(Math.addExact(source.size.toLong,1L),256L))
    root.reserveRetainedMemory(retained,"controlled full20 patch containers")
    var scratch=16384L
    snapshot.relations.foreach { relation =>
      val width=MySqlFormat3RelationShapes.forTable(relation.table).columns.size.toLong
      val perRow=Math.addExact(1024L,Math.multiplyExact(width,256L))
      scratch=Math.addExact(scratch,Math.multiplyExact(Math.addExact(relation.rows.size.toLong,1L),perRow))
    }
    root.reserveTemporaryMemory(scratch,"controlled full20 patch ordering and constructor validation")
    var primary: Throwable=null
    try
      val changed=changedRows(source,row,shape,requireExisting)
      val relations=snapshot.relations.map { relation =>
        if relation.table==table then MySqlFormat3RelationSnapshot(table,changed) else relation
      }
      new MySqlFormat3FullSnapshot(snapshot.schema,snapshot.connection,snapshot.observationScope,
        snapshot.maxAllowedPacketBytes,snapshot.limitProfile,relations,root)
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally
      try root.releaseTemporaryMemory(scratch,"controlled full20 patch ordering and constructor validation release")
      catch case cleanup: Throwable =>
        if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  def one(snapshot: MySqlFormat3FullSnapshot,table: String,
      matches: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
    oneRow(snapshot.relation(table).rows,matches)

  def one(snapshot: MySqlScriptSchemaSnapshot,table: String,
      matches: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
    oneRow(snapshot.relation(table).rows,matches)

  private def oneRow(rows: Vector[Vector[MySqlFormat3Cell]],
      matches: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
    var found: Vector[MySqlFormat3Cell]=null
    var index=0
    while index<rows.size do
      if matches(rows(index)) then
        if found!=null then fail("original graph has more than one exact row")
        found=rows(index)
      index+=1
    if found==null then fail("original graph lacks its exact row")
    found

  private def changedRows(source: Vector[Vector[MySqlFormat3Cell]],row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape,requireExisting: Boolean): Vector[Vector[MySqlFormat3Cell]] =
    var matched = -1
    var index = 0
    while index < source.size do
      if MySqlControlledDebugDelta.compareKey(source(index),row,shape) == 0 then
        if matched >= 0 then fail("duplicate complete primary key")
        matched = index
      index += 1
    if requireExisting && matched < 0 then fail("required original row is missing")
    if !requireExisting && matched >= 0 then fail("new row collides with an actual original key")
    if matched >= 0 then source.updated(matched,row)
    else (source :+ row).sortWith((left,right)=>MySqlControlledDebugDelta.compareKey(left,right,shape)<0)

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled complete image: "+reason)
