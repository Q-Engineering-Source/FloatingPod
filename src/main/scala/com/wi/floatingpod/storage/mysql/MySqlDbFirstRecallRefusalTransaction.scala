package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime
import java.util.UUID

/** 原C封锁后的专用O/W拒绝事务。actual confirmed J/read ACK和原Entry先于任何IO。
  * 当前barrier/audit/lease与锁定Full27资格另核；ACK/读回均不自动发行终局反馈或清槽。
  */
private[mysql] final class MySqlDbFirstRecallRefusalTransaction private[mysql] (
    val work:MySqlDbFirstRecallRefusalWork):
  import MySqlDbFirstRecallRefusalTransaction.*
  def tag=work.exclusion.executionTag
  def bindingUuid:UUID=work.exclusion.bindingUuid
  val root=work.root
  def request:MySqlDbFirstRecallRefusalRequest=work.request
  private val workerThread=Thread.currentThread()
  private val progress=new MySqlDbFirstSnapshotTransaction.Progress(workerThread)
  private var first:Throwable=null
  private var cleanupStarted=false
  private var cleanupFailure:Throwable=null
  private var retired=false
  private var entryPayloadRetired=false
  private[mysql] var audit:MySqlSchemaAuditor.DbFirstAudit=null
  private var freshReader:MySqlDbFirstSnapshotJdbc=null
  private var inTransactionReader:MySqlDbFirstSnapshotJdbc=null
  private var committedReader:MySqlDbFirstSnapshotJdbc=null
  private var historyFee=0L
  private var forestFee=0L
  private var history:MySqlDbFirstSession.HistoricalRead=null
  private var forest:MySqlDbFirstForestRead=null
  private var materialization:MySqlDbFirstRecallRefusalMaterialization=null
  private var rollbackOnly:MySqlRollbackOnlyState=null
  private var gate:MySqlConnectionTransactionGate.Owner=null
  private var gateHeld=false
  private var transactionAttempted=false
  private var rollbackAttempted=false
  private var rollbackReturned=false
  private var oldIsolation:Option[Int]=None
  private var isolationAttempted=false
  private var result:MySqlDbFirstSession.DbFirstRecallRefusalObservation=null
  private var sqlBudget:MySqlFormat3ReadBudget=null
  private var sqlFee=0L
  private var statement:PreparedStatement=null
  private var rows:ResultSet=null
  private[mysql] def requireRegisteredWorker():Unit =
    require(Thread.currentThread() eq workerThread,"original refusal worker required")
    work.requireTransaction(this)
  private[mysql] def confirmedMainReadSource(observation:MySqlDbFirstSession.DbFirstRecallRefusalObservation):MySqlScriptSchemaSnapshot =
    require(Thread.currentThread() eq workerThread,"original refusal readback worker required")
    require(observation!=null && (result eq observation) && first==null && cleanupFailure==null &&
      !cleanupStarted && !retired && committedReader!=null && !gateHeld &&
      progress.outcome==MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "original successful live refusal reader required before Main DATA assembly")
    committedReader.snapshot
  private[mysql] def requireConfirmedObservation(observation:MySqlDbFirstSession.DbFirstRecallRefusalObservation):Unit =
    require(observation!=null && (result eq observation) && first==null && cleanupFailure==null && retired &&
      progress.outcome==MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "actual original SQL-retired refusal readback required; equal DATA is not confirmation")
  private[mysql] def requireReadbackForCleanup():Unit =
    require(Thread.currentThread() eq workerThread,"original refusal cleanup worker required")
    require(first==null && cleanupFailure==null && result!=null && materialization!=null &&
      progress.outcome==MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "only actual successful refusal readback permits SQL retirement")
  private[mysql] def resourcesRetired:Boolean=retired
  private[mysql] def retireConfirmedResources():Unit =
    requireReadbackForCleanup()
    work.requireTransaction(this)
    require(!cleanupStarted && !retired,"refusal SQL retirement is one-shot")
    cleanupStarted=true
    try
      require(!gateHeld && statement==null && rows==null,"refusal SQL handles/gate still pending")
      if history!=null then
        history.close();history=null;historyFee=0L
      if forest!=null then
        forest.close();forest=null;forestFee=0L
      work.retirePreflightSource(this)
      if committedReader!=null then
        committedReader.close();committedReader=null
      if inTransactionReader!=null then
        inTransactionReader.close();inTransactionReader=null
      if freshReader!=null then
        freshReader.close();freshReader=null
      if audit!=null then
        audit.close();audit=null
      if sqlBudget!=null then
        sqlBudget.closeScopedReadBudget();sqlBudget=null
      if sqlFee!=0L then
        root.releaseTemporaryMemory(sqlFee,"confirmed refusal SQL child detached");sqlFee=0L
      require(historyFee==0L && forestFee==0L,"refusal temporary reader fees still held")
      rollbackOnly=null;gate=null;oldIsolation=None
      retired=true
    catch case cause:Throwable=>
      cleanupFailure=cause
      if first==null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      throw cause

  private[mysql] def retireRefusalFinalPayload(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    require(Thread.currentThread() eq workerThread,"original refusal payload worker required")
    require(receipt!=null,"original refusal final receipt required")
    receipt.core.requireRefusalFinalPayload(receipt,this)
    require(retired && first==null && cleanupFailure==null && !entryPayloadRetired,
      "original SQL-retired refusal payload detach is one-shot")
    work.retireRefusalFinalPayload(this,receipt)
    materialization=null;result=null
    entryPayloadRetired=true

  private[mysql] def payloadRetired:Boolean=entryPayloadRetired

  def firstFailure:Option[Throwable]=Option(first)
  def readersPending:Boolean=freshReader!=null || inTransactionReader!=null || committedReader!=null || sqlBudget!=null
  def historyPending:Boolean=history!=null || historyFee!=0L
  def forestPending:Boolean=forest!=null || forestFee!=0L
  def auditPending:Boolean=audit!=null
  def gatePending:Boolean=gateHeld
  def commitAttempted:Boolean=progress.commitAttempted
  def acknowledged:Boolean=progress.acknowledged
  def outcome:MySqlDbFirstSession.DbFirstSnapshotOutcome=progress.outcome
  private def withSqlBudget(body:MySqlFormat3ReadBudget=>Unit):Unit =
    require(sqlBudget == null && sqlFee == 0L && statement == null && rows == null,"previous SQL cleanup remains retained")
    root.reserveTemporaryMemory(256L,"DB-first SQL child BEFORE allocation")
    sqlFee=256L
    var primary:Throwable=null
    try
      sqlBudget=root.newCanonicalScope()
      sqlBudget.reserveTemporaryMemory(65536L,"DB-first SQL containers BEFORE allocation")
      body(sqlBudget)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally
      // 未成功close的真实JDBC handle保留原child与全部binding费用，不能伪退款。
      if statement == null && rows == null then
        try
          if sqlBudget != null then
            sqlBudget.closeScopedReadBudget()
            sqlBudget=null
          if sqlFee != 0L then
            root.releaseTemporaryMemory(sqlFee,"DB-first SQL child detached")
            sqlFee=0L
        catch case cleanup:Throwable=>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  private def withStatement(connection:Connection,sql:String)(body:PreparedStatement=>Unit):Unit =
    require(statement == null && rows == null,"original SQL handle is still pending")
    var primary:Throwable=null
    try
      statement=connection.prepareStatement(sql)
      body(statement)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally
      if rows != null then
        try
          rows.close()
          rows=null
        catch case cleanup:Throwable=>
          if primary == null then primary=cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      if statement != null then
        try
          statement.close()
          statement=null
        catch case cleanup:Throwable=>
          if primary == null then primary=cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      if primary != null then throw primary
  private def fail(message:String):Nothing=throw new SQLDataException("schema6 DB-first snapshot: "+message)
  private def reader(inputs:MySqlDbFirstSessionInputs,sessionRoot:MySqlFormat3ReadBudget,identity:MySqlOwnedIoIdentity,
      retain:MySqlDbFirstSnapshotJdbc=>Unit):MySqlDbFirstSnapshotJdbc =
    try
      val value=MySqlDbFirstSnapshotJdbc.create(inputs,sessionRoot,identity)
      retain(value)
      value
    catch case creation:MySqlDbFirstSnapshotJdbc.CreationFailure =>
      retain(creation.retainedHolder)
      throw creation

  private[mysql] def execute(session:MySqlDbFirstSession,inputs:MySqlDbFirstSessionInputs,
      sessionRoot:MySqlFormat3ReadBudget,identity:MySqlOwnedIoIdentity,verified:VerifiedMySqlConnection,
      scope:MySqlAuditBootScope,coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      barrier:HeldSessionBarrier,issuer:MySqlOwnedRetainedPlanIssuer,auditor:MySqlSchemaAuditor):MySqlDbFirstSession.DbFirstRecallRefusalObservation =
    progress.begin()
    val connection=verified.connection
    def current():Unit =
      inputs.requireForSession(sessionRoot,identity)
      issuer.requireExecutingDbFirstRecallRefusal(session,work,inputs.full27Inputs)
      work.requireTransaction(this)
      root.requireScriptDataInputs(inputs.full27Inputs)
      coordinator.requireDbFirstRecallRefusal(this,admission,verified,scope)
      verified.readmitSessionBarrier(inputs.auditExpectation,scope)
      if gateHeld && gate != null then
        MySqlConnectionTransactionGate.requireCurrent(gate,connection,rollbackOnly)
        // 本方法只在ResultSet关闭后调用，避免streaming期间驱动getter执行第二查询。
        if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
          fail("original gate lost its exact REPEATABLE READ transaction")
    def dbNow():LocalDateTime =
      current()
      var value:LocalDateTime=null
      val sql="SELECT UTC_TIMESTAMP(6)"
      if sql.length.toLong+16L>Math.min(root.profile.maxSqlPacketBytes(),inputs.full27Inputs.budget.packetBytes) then
        fail("time query exceeds original packet bound")
      withSqlBudget { _ =>
        withStatement(connection,sql) { statement =>
          rows=statement.executeQuery()
          if !rows.next() then fail("database time missing")
          value=rows.getObject(1,classOf[LocalDateTime])
          if value == null || value.getNano%1000 != 0 || rows.next() then fail("database time is not singleton DATETIME6")
          if !value.isBefore(admission.token.leaseUntil) then fail("original writer lease expired")
        }
      }
      value
    def beginRead():Unit =
      current()
      if !connection.getAutoCommit then fail("original connection is not idle auto-commit")
      oldIsolation=Some(connection.getTransactionIsolation)
      rollbackOnly=new MySqlRollbackOnlyState
      MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
        case Some(cause)=>throw cause
        case None=>gateHeld=true
      session.publishDbFirstRecallRefusal(this)
      gate=MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly)
      isolationAttempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      transactionAttempted=true
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        fail("original transaction did not enter REPEATABLE READ")
    def settleRollback():Unit =
      if transactionAttempted && !rollbackAttempted then
        rollbackAttempted=true
        connection.rollback()
        rollbackReturned=true
      if transactionAttempted && !rollbackReturned then
        throw new SQLNonTransientConnectionException("schema6 original rollback did not return; auto-commit is not restored",first)
      if transactionAttempted then connection.setAutoCommit(true)
      if isolationAttempted then connection.setTransactionIsolation(oldIsolation.get)
      if gateHeld then
        MySqlConnectionTransactionGate.leave(connection,rollbackOnly)
        gateHeld=false;gate=null
    try
      current()
      verified.readmitSessionBarrier(inputs.auditExpectation,scope)
      if barrier == null then fail("original held barrier missing")
      audit=auditor.beginDbFirstWriterAudit(inputs,sessionRoot,verified,scope,session,coordinator)
      val qualified=coordinator.auditDbFirstRecallRefusal(this,admission,verified,scope,audit)
      beginRead()
      freshReader=reader(inputs,sessionRoot,identity,value=>freshReader=value)
      val before=freshReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      if (before ne freshReader.snapshot) || (before.inputs ne inputs.full27Inputs) ||
        before.allocationBudget.isClosed || !before.allocationBudget.isSnapshotReadChildOf(sessionRoot) ||
        !before.allocationBudget.sharesAllocationLedger(root) || (before.allocationBudget.profile ne root.profile) then
        fail("fresh before is not the original live issued Full27 child on the shared ledger")
      // 与原观测一样，既存forest与P/C/DB-first完整历史必须先通过；未决共享责任不冒充可写。
      sessionRoot.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"transaction historical holder BEFORE allocation")
      historyFee=MySqlDbFirstSession.HistoricalRead.HolderBytes
      history=new MySqlDbFirstSession.HistoricalRead(sessionRoot,inputs.full27Inputs)
      history.acquire(inputs.historicalInputs)
      history.validate(before)
      sessionRoot.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"transaction forest holder BEFORE allocation")
      forestFee=MySqlDbFirstForestRead.HolderBytes
      forest=new MySqlDbFirstForestRead(sessionRoot,inputs.full27Inputs,inputs.auditExpectation.bindingUuid)
      forest.validate(before)
      val pendingOperation=before.relation("fp_recovery_operation").rows.exists { row =>
        val state=MySqlDbFirstRecallMaterializer.value(row,"fp_recovery_operation","operation_state",inputs.full27Inputs)
        state == MySqlFormat3Cell.Text("RESERVED") ||
          (state == MySqlFormat3Cell.Text("CANCELLED") &&
            MySqlDbFirstRecallMaterializer.value(row,"fp_recovery_operation","settled_by_operation_uuid",inputs.full27Inputs)==MySqlFormat3Cell.SqlNull)
      }
      if history.result.pendingSettlements != 0L || history.dbFirstRecallSummary.unresolvedReserved != 0L ||
        history.dbFirstRecallSummary.pendingCancelled != 0L || pendingOperation ||
        before.relation("fp_recovery_reservation").rows.exists(row=>
        MySqlDbFirstRecallMaterializer.value(row,"fp_recovery_reservation","reservation_state",inputs.full27Inputs)==MySqlFormat3Cell.Text("HELD")) then
        fail("shared unresolved operation/reservation requires actual reconciliation")
      val planned=dbNow()
      val completed=dbNow()
      // 原Entry root已由J绑定；当前拒绝另复制before，不重置原J image身份。
      val facts=MySqlDbFirstRecallRefusalFacts(barrier.actualConnectionId,barrier.connectionGenerationId,
        admission.token.writerEpoch,admission.token.leaseUntil,planned,completed)
      if bindingUuid!=admission.token.bindingUuid then fail("refusal binding differs from original writer")
      materialization=work.materialize(before,sessionRoot,facts)
      materialization.verify()
      current()
      write(connection,materialization,this,()=>current())
      inTransactionReader=reader(inputs,sessionRoot,identity,value=>inTransactionReader=value)
      val actual=inTransactionReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      materialization.verify() // candidate保持Entry root身份；实际reader用独立exact byte comparison。
      if !materialization.target.exactlyEquals(actual) then fail("complete in-transaction Full27 differs from original target")
      dbNow()
      rollbackOnly.cause.foreach(throw _)
      progress.attemptCommit() // 在driver调用前留原attempt；throw/无ACK绝不rollback/retry。
      session.publishDbFirstRecallRefusal(this)
      connection.commit()
      progress.acknowledge()
      session.publishDbFirstRecallRefusal(this)
      committedReader=reader(inputs,sessionRoot,identity,value=>committedReader=value)
      val committed=committedReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      if !materialization.target.exactlyEquals(committed) then fail("acknowledged commit has different complete Full27 readback")
      MySqlDbFirstRecallRefusalPersistedRead.verifyCommitted(committed,tag.operationId,sessionRoot)
      dbNow()
      settleRollback() // 只结束ACK后的只读读回事务，不代表业务cleanup。
      progress.verifyReadback()
      result=MySqlDbFirstSession.DbFirstRecallRefusalObservation(tag,work.exclusion.journalTag,
        request.podUuid,progress.commitAttempted,progress.acknowledged,true,materialization.target.canonicalSha256Hex,Some(work.exclusion.routeTag))
      result
    catch case cause:Throwable =>
      if first == null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      progress.fail()
      if !progress.commitAttempted && statement == null && rows == null then
        try settleRollback()
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      // commit已尝试或ACK后的失败保原gate/connection/reader；不会rollback或重试提交。
      try work.exclusion.core.failRecallRefusal(work,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

private[mysql] object MySqlDbFirstRecallRefusalTransaction:
  val HolderBytes=8192L
  private val O="fp_recovery_operation"
  private val W="fp_runtime_writer"
  private val writable=Set(O,W)
  /** 固定O/W-only DAG；所有CAS逐列NULL-safe核实际before，exactly-one不接受缺行/批量写。 */
  private def write(connection:Connection,m:MySqlDbFirstRecallRefusalMaterialization,runner:MySqlDbFirstRecallRefusalTransaction,
      current:()=>Unit):Unit =
    val inputs=m.before.inputs
    def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=MySqlDbFirstRecallMaterializer.value(row,table,name,inputs)
    def sameKey(a:Vector[MySqlFormat3Cell],b:Vector[MySqlFormat3Cell],table:String):Boolean =
      inputs.forTable(table).primaryKey.forall(name=>value(a,table,name)==value(b,table,name))
    def target(table:String):Vector[MySqlFormat3Cell]=
      val rows=m.target.relation(table).rows.filter { row =>
        table match
          case O=>MySqlDbFirstRecallMaterializer.uuidEquals(value(row,table,"operation_uuid"),m.request.tag.operationId)
          case W=>true
          case _=>throw new SQLDataException("refusal cannot select non O/W table")
      }
      if rows.size!=1 then throw new SQLDataException("schema6 write target is not singleton: "+table)
      rows.head
    def put(table:String,after:Vector[MySqlFormat3Cell],before:Option[Vector[MySqlFormat3Cell]]):Unit =
      require(writable.contains(table),"fixed DB-first SQL table required")
      current()
      runner.withSqlBudget { budget =>
        var primary:Throwable=null
        try
          val shape=inputs.forTable(table)
          val changed=shape.columns.indices.filter(i=>before.forall(_(i)!=after(i))).toVector
          if changed.isEmpty then throw new SQLDataException("schema6 CAS has no changed columns")
          val qualified="`"+inputs.schema+"`.`"+table+"`"
          val sql=before match
            case None=>"INSERT INTO "+qualified+" ("+shape.columns.map(c=>"`"+c.name+"`").mkString(",")+") VALUES ("+shape.columns.map(_=>"?").mkString(",")+")"
            case Some(_)=>"UPDATE "+qualified+" SET "+changed.map(i=>"`"+shape.columns(i).name+"`=?").mkString(",")+
              " WHERE "+shape.columns.map(c=>"`"+c.name+"` <=> ?").mkString(" AND ")
          val bindings=before match
            case None=>shape.columns.indices.map(i=>(i,after(i))).toVector
            case Some(old)=>changed.map(i=>(i,after(i)))++shape.columns.indices.map(i=>(i,old(i)))
          var packet=MySqlFormat3Utf8.byteLength(sql)+16L
          bindings.foreach { (_,cell)=>
            packet=Math.addExact(packet,cell match
              case MySqlFormat3Cell.Binary(bytes)=>bytes.length.toLong+16L
              case MySqlFormat3Cell.Text(text)=>MySqlFormat3Utf8.byteLength(text)+16L
              case MySqlFormat3Cell.SqlNull=>1L
              case _=>32L)
          }
          if packet>Math.min(m.before.maxAllowedPacketBytes,Math.min(inputs.budget.packetBytes,runner.root.profile.maxSqlPacketBytes())) then
            throw new SQLDataException("schema6 exact statement exceeds observed/profile packet bound")
          runner.withStatement(connection,sql) { statement=>
            bindings.zipWithIndex.foreach { case ((columnIndex,cell),index)=>
              val parameter=index+1
              val column=shape.columns(columnIndex)
              cell match
                case MySqlFormat3Cell.SqlNull=>statement.setNull(parameter,column.jdbcType)
                case MySqlFormat3Cell.Text(text)=>statement.setString(parameter,text)
                case MySqlFormat3Cell.Binary(bytes)=>statement.setBytes(parameter,bytes.copyBytes(budget,"DB-first exact SQL binary binding"))
                case MySqlFormat3Cell.SignedInteger(number)=>
                  if column.sqlDeclaration.startsWith("TINYINT") || column.sqlDeclaration.startsWith("INT ") then
                    if number<Int.MinValue.toLong || number>Int.MaxValue.toLong then throw new SQLDataException("schema6 INT binding overflow")
                    statement.setInt(parameter,number.toInt)
                  else statement.setLong(parameter,number)
                case MySqlFormat3Cell.Floating(bits)=>statement.setDouble(parameter,java.lang.Double.longBitsToDouble(bits))
                case MySqlFormat3Cell.DateTime(time)=>statement.setObject(parameter,time)
            }
            if statement.executeUpdate()!=1 then throw new SQLDataException("schema6 exact guarded write did not affect one row: "+table)
          }
        catch case cause:Throwable=>
          primary=cause
          throw cause
      }
    def upsert(table:String,row:Vector[MySqlFormat3Cell]):Unit =
      val old=m.before.relation(table).rows.filter(sameKey(_,row,table))
      if old.size>1 then throw new SQLDataException("schema6 duplicate original primary key")
      put(table,row,old.headOption)
    put(O,m.reservedOperation,None)
    put(O,target(O),Some(m.reservedOperation))
    upsert(W,target(W))
