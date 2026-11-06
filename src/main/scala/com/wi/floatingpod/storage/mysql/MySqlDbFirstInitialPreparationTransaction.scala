package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime

/** 原Entry在任何IO前持有；ACK不是世界完成，所有DATA/reader/事务残留由原Entry继续保留。 */
private[mysql] final class MySqlDbFirstInitialPreparationTransaction private[mysql] (
    val work:MySqlDbFirstInitialPreparationWork):
  import MySqlDbFirstInitialPreparationTransaction.*
  val reservation=work.reservation
  val root=work.root
  val activeLimit=reservation.activeLimit
  def request:MySqlControlledDebugPrepareRequest=reservation.request
  private val workerThread=Thread.currentThread()
  private val progress=new Progress(workerThread)
  private var first:Throwable=null
  private var cleanupStarted=false
  private var cleanupFailure:Throwable=null
  private var retired=false
  private var rejectedRetired=false
  private var settledRejection:MySqlDbFirstSession.DbFirstInitialPreparationRejection=null
  private[mysql] var audit:MySqlSchemaAuditor.DbFirstAudit=null
  private var freshReader:MySqlDbFirstSnapshotJdbc=null
  private var inTransactionReader:MySqlDbFirstSnapshotJdbc=null
  private var preparationData:MySqlDbFirstInitialPreparationDataView=null
  private var preparationQueryBudget:MySqlFormat3ReadBudget=null
  private var committedReader:MySqlDbFirstSnapshotJdbc=null
  private var historyFee=0L
  private var history:MySqlDbFirstSession.HistoricalRead=null
  private var materialization:MySqlDbFirstInitialPreparationTransition=null
  private var rollbackOnly:MySqlRollbackOnlyState=null
  private var gate:MySqlConnectionTransactionGate.Owner=null
  private var gateHeld=false
  private var transactionAttempted=false
  private var rollbackAttempted=false
  private var rollbackReturned=false
  private var oldIsolation:Option[Int]=None
  private var isolationAttempted=false
  private var result:MySqlDbFirstSession.DbFirstInitialPreparationObservation=null
  private var sqlBudget:MySqlFormat3ReadBudget=null
  private var sqlFee=0L
  private var statement:PreparedStatement=null
  private var rows:ResultSet=null
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
  /** Main只核原缓存对象与真实退休结果，不以caller重建的observation取得确认。 */
  /** 仅原worker在成功读回且reader尚存活时借用；调用方须在关闭reader前登记独立DATA持有人。 */
  private[mysql] def confirmedMainReadSource(observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):MySqlScriptSchemaSnapshot =
    require(Thread.currentThread() eq workerThread,"original confirmed readback worker required")
    require(observation!=null && (result eq observation) && first==null && cleanupFailure==null &&
      !cleanupStarted && !retired && committedReader!=null && !gateHeld &&
      progress.outcome==MySqlDbFirstSession.DbFirstInitialPreparationOutcome.ReadbackVerifiedPending,
      "original successful live committed reader required before successor materialization")
    committedReader.snapshot
  private[mysql] def confirmedData(observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):MySqlDbFirstInitialPreparationDataView =
    requireConfirmedObservation(observation)
    require(preparationData!=null,"original preparation readback DATA missing")
    preparationData

  private[mysql] def requireConfirmedObservation(observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):Unit =
    require(observation != null && (result eq observation) && first==null && cleanupFailure==null && retired &&
      progress.outcome==MySqlDbFirstSession.DbFirstInitialPreparationOutcome.ReadbackVerifiedPending,
      "original SQL-retired preparation readback required")
  private[mysql] def resourcesRetired:Boolean=retired
  private[mysql] def requireReadbackForCleanup():Unit =
    require(Thread.currentThread() eq workerThread,"original preparation cleanup worker required")
    require(first==null && cleanupFailure==null && result!=null && materialization!=null &&
      progress.outcome==MySqlDbFirstSession.DbFirstInitialPreparationOutcome.ReadbackVerifiedPending,
      "only exact successful preparation readback may retire")
  /** 关闭成功才清引用；任一清理失败保原对象、cause和pending，不重发SQL。 */
  private[mysql] def retireConfirmedResources():Unit =
    requireReadbackForCleanup()
    work.requireTransaction(this)
    require(!cleanupStarted && !retired,"preparation SQL retirement is one-shot")
    cleanupStarted=true
    try
      require(!gateHeld && statement==null && rows==null,"preparation JDBC/gate cleanup still pending")
      if history!=null then
        history.close();history=null;historyFee=0L
      work.retirePreflightSource(this)
      if preparationQueryBudget!=null then
        preparationQueryBudget.closeScopedReadBudget();preparationQueryBudget=null
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
        root.releaseTemporaryMemory(sqlFee,"confirmed preparation SQL child detached");sqlFee=0L
      require(historyFee==0L && !work.preflightForestPending,"preparation history/forest fees still held")
      rollbackOnly=null;gate=null;oldIsolation=None
      // 完整target、request与原Entry仍保留；这里只释放SQL与临时读取责任。
      retired=true
    catch case cause:Throwable=>
      cleanupFailure=cause
      if first==null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      throw cause

  def firstFailure:Option[Throwable]=Option(first)
  /** 仅可进入后续失败清理的原worker证据；不表示resourcesRetired或可解除Session hold。 */
  private[mysql] def requireRejectedPrecommitCleanup(cause:MySqlDbFirstPreparationRejected):Unit =
    require(Thread.currentThread() eq workerThread,"original failed preparation worker required")
    require(cause!=null && (first eq cause) && cause.getSuppressed.isEmpty && progress.precommitRollbackSettled &&
      !progress.commitAttempted && result==null && !cleanupStarted && !retired && cleanupFailure==null &&
      statement==null && rows==null && !gateHeld && rollbackReturned,
      "original rejected preparation with fully settled precommit rollback required")

  private[mysql] def rejectedMainReadSource(cause:MySqlDbFirstPreparationRejected):MySqlScriptSchemaSnapshot =
    requireRejectedPrecommitCleanup(cause)
    val source=work.rejectedDataSource(this,cause)
    require(freshReader!=null && (freshReader.snapshot eq source),"original live rejected reader required")
    source

  /** 独立于成功readback资格；原拒绝cause/Entry仍保留，Session恢复另行核验。 */
  private[mysql] def rejectedResourcesRetired:Boolean=rejectedRetired
  /** 只结算原拒绝runner的SQL责任，不清除Entry失败或授予成功readback资格。 */
  private[mysql] def requireRejectedResourcesRetired(cause:MySqlDbFirstPreparationRejected):Unit =
    require(Thread.currentThread() eq workerThread,"original rejected retirement worker required")
    work.requireRejectedTransaction(this,cause)
    require((first eq cause) && cause.getSuppressed.isEmpty && rejectedRetired && !retired &&
      cleanupStarted && cleanupFailure==null && progress.precommitRollbackSettled && !progress.commitAttempted &&
      result==null && materialization==null && preparationData==null && !readersPending && !historyPending &&
      !forestPending && audit==null && statement==null && rows==null && !gateHeld && rollbackReturned &&
      sqlBudget==null && sqlFee==0L,"original fully retired rejected preparation required")

  private[mysql] def recordSettledRejection(observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):Unit =
    require(observation!=null && observation.tag==reservation.tag && observation.podUuid==request.podUuid &&
      settledRejection==null,"original settled rejection registration is one-shot")
    requireRejectedResourcesRetired(observation.cause)
    settledRejection=observation

  /** Main只核原worker登记的同一观测与已经完成的清理，不执行worker账本操作。 */
  private[mysql] def requireSettledRejection(observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):Unit =
    require(observation!=null && (settledRejection eq observation) && (first eq observation.cause) &&
      observation.cause.getSuppressed.isEmpty && rejectedRetired && !retired && cleanupFailure==null &&
      !progress.commitAttempted && result==null,"exact registered settled rejection required")

  private[mysql] def requireRejectedResourceCleanup(cause:MySqlDbFirstPreparationRejected):Unit =
    require(Thread.currentThread() eq workerThread,"original rejected cleanup worker required")
    require(cause!=null && (first eq cause) && cause.getSuppressed.isEmpty && cleanupStarted &&
      cleanupFailure==null && !retired && !rejectedRetired && progress.precommitRollbackSettled &&
      !progress.commitAttempted && result==null && statement==null && rows==null && !gateHeld && rollbackReturned,
      "original rejected cleanup in progress required")

  private[mysql] def retireRejectedResources(cause:MySqlDbFirstPreparationRejected):Unit =
    requireRejectedPrecommitCleanup(cause)
    work.requireRejectedTransaction(this,cause)
    cleanupStarted=true
    try
      if history!=null then
        history.close();history=null;historyFee=0L
      work.retireRejectedPreflightSource(this,cause)
      if preparationQueryBudget!=null then
        preparationQueryBudget.closeScopedReadBudget();preparationQueryBudget=null
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
        root.releaseTemporaryMemory(sqlFee,"rejected preparation SQL child detached");sqlFee=0L
      require(historyFee==0L && !work.preflightForestPending,"rejected preparation history/forest fees still held")
      rollbackOnly=null;gate=null;oldIsolation=None
      rejectedRetired=true
    catch case cleanup:Throwable=>
      cleanupFailure=cleanup
      MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

  private[mysql] def retireRejectedEntryPayload(observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):Unit =
    require(Thread.currentThread() eq workerThread,"original rejected payload worker required")
    requireSettledRejection(observation)
    work.retireRejectedEntryPayload(this,observation.cause)
    settledRejection=null

  private[mysql] def retireEntryPayload():Unit =
    require(Thread.currentThread() eq workerThread,"original Entry retirement worker required")
    require(retired && cleanupFailure==null && first==null,"only SQL-retired preparation payload may detach")
    work.retireEntryPayload(this)
    preparationData=null;materialization=null;result=null

  def readersPending:Boolean=preparationQueryBudget != null || freshReader != null || inTransactionReader != null || committedReader != null
  def historyPending:Boolean=history != null || historyFee != 0L
  def forestPending:Boolean=work.preflightForestPending
  def gatePending:Boolean=gateHeld
  def commitAttempted:Boolean=progress.commitAttempted
  def acknowledged:Boolean=progress.acknowledged
  def outcome:MySqlDbFirstSession.DbFirstInitialPreparationOutcome=progress.outcome
  private def fail(message:String):Nothing=throw new SQLDataException("schema6 DB-first initial preparation: "+message)
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
      barrier:HeldSessionBarrier,issuer:MySqlOwnedRetainedPlanIssuer,auditor:MySqlSchemaAuditor):MySqlDbFirstSession.DbFirstInitialPreparationObservation =
    progress.begin()
    val connection=verified.connection
    def current():Unit =
      inputs.requireForSession(sessionRoot,identity)
      issuer.requireExecutingDbFirstInitialPreparation(session,work,inputs.full27Inputs)
      work.requireTransaction(this)
      root.requireScriptDataInputs(inputs.full27Inputs)
      coordinator.requireDbFirstInitialPreparation(this,admission,verified,scope)
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
      session.publishDbFirstInitialPreparation(this)
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
      val qualified=coordinator.auditDbFirstInitialPreparation(this,admission,verified,scope,audit)
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
      val pendingOperation=before.relation("fp_recovery_operation").rows.exists { row =>
        val state=MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation","operation_state",inputs.full27Inputs)
        state == MySqlFormat3Cell.Text("RESERVED") ||
          (state == MySqlFormat3Cell.Text("CANCELLED") &&
            MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation","settled_by_operation_uuid",inputs.full27Inputs)==MySqlFormat3Cell.SqlNull)
      }
      if history.result.pendingSettlements != 0L || history.dbFirstInitialActivationSummary.unresolvedReserved != 0L ||
        history.dbFirstInitialActivationSummary.pendingCancelled != 0L || pendingOperation ||
        before.relation("fp_recovery_reservation").rows.exists(row=>
        MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_reservation","reservation_state",inputs.full27Inputs)==MySqlFormat3Cell.Text("HELD")) then
        fail("shared unresolved operation/reservation requires actual reconciliation")
      val planned=dbNow()
      val completed=dbNow()
      root.initialize(before.schema,before.connection,before.observationScope,before.maxAllowedPacketBytes)
      work.preflight(before,sessionRoot)
      work.constructDomainRows(planned)
      materialization=work.constructTransition(MySqlDbFirstInitialPreparationFacts(barrier.actualConnectionId,
        barrier.connectionGenerationId,admission.token.writerEpoch,admission.token.leaseUntil,planned,completed))
      materialization.verifyTarget(materialization.target)
      if materialization.facts.writerEpoch != admission.token.writerEpoch ||
        materialization.facts.leaseUntil != admission.token.leaseUntil || request.expectedBinding.bindingUuid != admission.token.bindingUuid then
        fail("locked writer differs from original exact admission owner/epoch/lease")
      current()
      write(connection,materialization,this,()=>current())
      inTransactionReader=reader(inputs,sessionRoot,identity,value=>inTransactionReader=value)
      val actual=inTransactionReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      materialization.verifyTarget(materialization.target) // candidate保持Entry root身份；实际reader用独立exact byte comparison。
      if !materialization.target.exactlyEquals(actual) then fail("complete in-transaction Full27 differs from original target")
      dbNow()
      rollbackOnly.cause.foreach(throw _)
      progress.attemptCommit() // 在driver调用前留原attempt；throw/无ACK绝不rollback/retry。
      session.publishDbFirstInitialPreparation(this)
      connection.commit()
      progress.acknowledge()
      session.publishDbFirstInitialPreparation(this)
      committedReader=reader(inputs,sessionRoot,identity,value=>committedReader=value)
      val committed=committedReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      if !materialization.target.exactlyEquals(committed) then fail("acknowledged commit has different complete Full27 readback")
      MySqlDbFirstInitialPreparationPersistedRead.verifyCommitted(committed,reservation.tag.operationId,sessionRoot)
      preparationQueryBudget=sessionRoot.newCanonicalDataScope()
      preparationData=MySqlDbFirstInitialPreparationDataView.read(request,committed,sessionRoot,preparationQueryBudget,root)
      dbNow()
      settleRollback() // 只结束ACK后的只读读回事务，不代表业务cleanup。
      progress.verifyReadback()
      result=MySqlDbFirstSession.DbFirstInitialPreparationObservation(reservation.tag,request.podUuid,
        progress.commitAttempted,progress.acknowledged,true,materialization.target.canonicalSha256Hex)
      result
    catch case cause:Throwable =>
      if first == null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      progress.fail()
      if !progress.commitAttempted && statement == null && rows == null then
        try
          settleRollback()
          if transactionAttempted && rollbackReturned && !gateHeld then progress.recordPrecommitRollbackSettled()
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      // commit已尝试或ACK后的失败保原gate/connection/reader；不会rollback或重试提交。
      try issuer.failDbFirstInitialPreparation(work,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

private[mysql] object MySqlDbFirstInitialPreparationTransaction:
  val HolderBytes=8192L
  /** 真runner使用的单次commit边界；pure测试不制造JDBC资格或提交成功。 */
  private[mysql] final class Progress(owner:Thread):
    private var begun=false
    private var attempted=false
    private var ack=false
    private var verified=false
    private var fault=false
    private var rollbackSettled=false
    private def check():Unit=require(Thread.currentThread() eq owner,"original initial preparation transaction worker required")
    def begin():Unit =
      check();require(!begun,"initial preparation transaction is one-shot");begun=true
    def attemptCommit():Unit =
      check();require(begun && !attempted && !fault,"original live transaction required before commit")
      attempted=true
    def acknowledge():Unit =
      check();require(attempted && !ack && !fault,"original commit attempt required before ACK");ack=true
    def verifyReadback():Unit =
      check();require(ack && !verified && !fault,"original ACK required before readback");verified=true
    def fail():Unit={check();fault=true}
    def recordPrecommitRollbackSettled():Unit =
      check();require(begun && fault && !attempted && !rollbackSettled,
        "only original failed precommit may record settled rollback once")
      rollbackSettled=true
    def precommitRollbackSettled:Boolean=rollbackSettled
    def commitAttempted:Boolean=attempted
    def acknowledged:Boolean=ack
    def outcome:MySqlDbFirstSession.DbFirstInitialPreparationOutcome =
      import MySqlDbFirstSession.DbFirstInitialPreparationOutcome.*
      if verified then ReadbackVerifiedPending
      else if ack then AcknowledgedPending
      else if attempted then CommitUnknown
      else if fault then FailedBeforeCommit
      else NotAttempted

  private val O="fp_recovery_operation"
  private val R="fp_recovery_reservation"
  private val F="physical_recovery_footprint"
  private val C="physical_recovery_footprint_chunk"
  private val D="floating_pod_state"
  private val L="physical_recovery_locator"
  private val I="physical_recovery_intent"
  private val W="fp_runtime_writer"
  private val writable=Set(O,R,F,C,D,I,L,W)
  /** SQL只选原binding/pod/attempt/footprint；C由精确25行循环写入。 */
  private[mysql] def targetRow(m:MySqlDbFirstInitialPreparationTransition,table:String):Vector[MySqlFormat3Cell] =
    require(m!=null && writable.contains(table),"original initial materialization and fixed SQL table required")
    val inputs=m.draft.before.inputs
    def matches(row:Vector[MySqlFormat3Cell],column:String,id:java.util.UUID)=
      MySqlDbFirstInitialActivationMaterializer.uuidEquals(MySqlDbFirstInitialActivationMaterializer.value(row,table,column,inputs),id)
    var selected:Vector[MySqlFormat3Cell]=null
    m.target.relation(table).rows.foreach { row=>
      val own=table match
        case O=>matches(row,"binding_uuid",m.work.reservation.request.expectedBinding.bindingUuid) && matches(row,"operation_uuid",m.work.reservation.tag.operationId)
        case D=>matches(row,"binding_uuid",m.work.reservation.request.expectedBinding.bindingUuid) && matches(row,"uuid",m.work.reservation.request.podUuid)
        case W=>true
        case I=>matches(row,"binding_uuid",m.work.reservation.request.expectedBinding.bindingUuid) && matches(row,"pod_uuid",m.work.reservation.request.podUuid) &&
          matches(row,"attempt_uuid",m.work.reservation.request.attemptUuid)
        case F | C=>matches(row,"binding_uuid",m.work.reservation.request.expectedBinding.bindingUuid) &&
          matches(row,"pod_uuid",m.work.reservation.request.podUuid) && matches(row,"attempt_uuid",m.work.reservation.request.attemptUuid) &&
          matches(row,"footprint_uuid",m.work.reservation.request.footprintUuid)
        case _=>matches(row,"binding_uuid",m.work.reservation.request.expectedBinding.bindingUuid) &&
          matches(row,if table==R then "resource_pod_uuid" else "pod_uuid",m.work.reservation.request.podUuid)
      if own then
        if selected!=null then throw new SQLDataException("schema6 write target is not singleton: "+table)
        selected=row
    }
    if selected==null then throw new SQLDataException("schema6 write target is absent: "+table)
    selected

  /** 实际DDL声明决定可写列；generated值仍保留在完整target与读回比较中。 */
  private[mysql] def writeColumns(shape:MySqlFormat3RelationShape):Vector[Int] =
    shape.columns.indices.filterNot(i=>shape.columns(i).sqlDeclaration.contains(" GENERATED ALWAYS AS ")).toVector
  private[mysql] def insertSql(schema:String,shape:MySqlFormat3RelationShape):String =
    require(schema != null && schema.matches("[a-z][a-z0-9_]{0,63}") && writable.contains(shape.table),
      "exact preparation schema and fixed table required")
    val columns=writeColumns(shape)
    "INSERT INTO `"+schema+"`.`"+shape.table+"` ("+columns.map(i=>"`"+shape.columns(i).name+"`").mkString(",")+
      ") VALUES ("+columns.map(_=>"?").mkString(",")+")"

  /** 闭合准备八表DAG；所有CAS逐列NULL-safe核实际before，exactly-one不接受缺行/批量写。 */
  private def write(connection:Connection,m:MySqlDbFirstInitialPreparationTransition,runner:MySqlDbFirstInitialPreparationTransaction,
      current:()=>Unit):Unit =
    val inputs=m.draft.before.inputs
    def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=MySqlDbFirstInitialActivationMaterializer.value(row,table,name,inputs)
    def sameKey(a:Vector[MySqlFormat3Cell],b:Vector[MySqlFormat3Cell],table:String):Boolean =
      inputs.forTable(table).primaryKey.forall(name=>value(a,table,name)==value(b,table,name))
    def target(table:String)=targetRow(m,table)
    def put(table:String,after:Vector[MySqlFormat3Cell],before:Option[Vector[MySqlFormat3Cell]]):Unit =
      require(writable.contains(table),"fixed DB-first SQL table required")
      current()
      runner.withSqlBudget { budget =>
        var primary:Throwable=null
        try
          val shape=inputs.forTable(table)
          val changed=writeColumns(shape).filter(i=>before.forall(_(i)!=after(i)))
          if changed.isEmpty then throw new SQLDataException("schema6 CAS has no changed columns")
          val qualified="`"+inputs.schema+"`.`"+table+"`"
          val sql=before match
            case None=>insertSql(inputs.schema,shape)
            case Some(_)=>"UPDATE "+qualified+" SET "+changed.map(i=>"`"+shape.columns(i).name+"`=?").mkString(",")+
              " WHERE "+shape.columns.map(c=>"`"+c.name+"` <=> ?").mkString(" AND ")
          val bindings=before match
            case None=>writeColumns(shape).map(i=>(i,after(i)))
            case Some(old)=>changed.map(i=>(i,after(i)))++shape.columns.indices.map(i=>(i,old(i)))
          var packet=MySqlFormat3Utf8.byteLength(sql)+16L
          bindings.foreach { (_,cell)=>
            packet=Math.addExact(packet,cell match
              case MySqlFormat3Cell.Binary(bytes)=>bytes.length.toLong+16L
              case MySqlFormat3Cell.Text(text)=>MySqlFormat3Utf8.byteLength(text)+16L
              case MySqlFormat3Cell.SqlNull=>1L
              case _=>32L)
          }
          if packet>Math.min(m.draft.before.maxAllowedPacketBytes,Math.min(inputs.budget.packetBytes,runner.root.profile.maxSqlPacketBytes())) then
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
      val old=m.draft.before.relation(table).rows.filter(sameKey(_,row,table))
      if old.size>1 then throw new SQLDataException("schema6 duplicate original primary key")
      put(table,row,old.headOption)
    put(O,m.reservedOperation,None)
    put(R,m.heldReservation,None)
    put(D,target(D),None)
    val sealedFootprint=target(F)
    runner.root.reserveRetainedMemory(2048L,"preparation intermediate DRAFT footprint map BEFORE allocation")
    val draft=MySqlControlledDebugRows.update(sealedFootprint,F,Map("seal_state"->MySqlFormat3Cell.Text("DRAFT")),runner.root)
    put(F,draft,None)
    val request=m.work.reservation.request
    var count=0
    m.target.relation(C).rows.foreach { row=>
      def matches(name:String,id:java.util.UUID)=MySqlDbFirstInitialActivationMaterializer.uuidEquals(value(row,C,name),id)
      if matches("binding_uuid",request.expectedBinding.bindingUuid) && matches("pod_uuid",request.podUuid) &&
          matches("attempt_uuid",request.attemptUuid) && matches("footprint_uuid",request.footprintUuid) then
        put(C,row,None)
        count+=1
    }
    if count!=25 then throw new SQLDataException("preparation requires exactly25 original footprint chunks")
    put(F,sealedFootprint,Some(draft))
    put(I,target(I),None)
    put(L,target(L),None)
    put(O,target(O),Some(m.reservedOperation))
    put(R,target(R),Some(m.heldReservation))
    upsert(W,target(W))
