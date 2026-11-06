package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime
import java.util.UUID

/** 原Entry在任何IO前持有；ACK不是世界完成，所有DATA/reader/事务残留由原Entry继续保留。 */
private[mysql] abstract class MySqlDbFirstSnapshotTransaction[R <: MySqlDbFirstSnapshotMutation,
    O <: MySqlDbFirstSession.SnapshotObservation] private[mysql] (
    val reservation:MySqlDbFirstCaptureReservation,requestInput:R,
    val root:MySqlFormat3ReadBudget):
  import MySqlDbFirstSnapshotTransaction.*
  private val workerThread=Thread.currentThread()
  private var originalRequest=Option(requestInput)
  final def request:R = originalRequest.getOrElse(throw new IllegalStateException("DB-first transaction request retired"))
  protected def requireExecution(issuer:MySqlOwnedRetainedPlanIssuer,session:MySqlDbFirstSession,inputs:MySqlScriptSchemaInputs):Unit
  protected def requireCoordinator(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit
  protected def auditQualification(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification
  protected def materializeOriginal(issuer:MySqlOwnedRetainedPlanIssuer,before:MySqlScriptSchemaSnapshot,
      connectionId:Long,barrier:UUID,plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization
  protected def publishOriginal(session:MySqlDbFirstSession):Unit
  protected def failOriginal(issuer:MySqlOwnedRetainedPlanIssuer,cause:Throwable):Unit
  protected def newObservation(canonicalSha256:String):O
  private val progress=new Progress(workerThread)
  private var cleanupStarted=false
  private var cleanupFailure:Throwable=null
  private var failedGateCleanupAttempted=false
  private var failedGateRetired=false
  private var failedReadbackAttempted=false
  private var failedReadback:Option[MySqlDbFirstSession.FailedRecallReadback]=None
  private var failedReadersCleanupAttempted=false
  private var failedReadersClosed=false
  private var failedSqlResourcesClosed=false
  private var failedEntryPayloadDetached=false
  private var retired=false
  private var confirmedDraftDetached=false
  private var first:Throwable=null
  private[mysql] var audit:MySqlSchemaAuditor.DbFirstAudit=null
  private var freshReader:MySqlDbFirstSnapshotJdbc=null
  private var inTransactionReader:MySqlDbFirstSnapshotJdbc=null
  private var committedReader:MySqlDbFirstSnapshotJdbc=null
  private var historyFee=0L
  private var forestFee=0L
  private var history:MySqlDbFirstSession.HistoricalRead=null
  private var forest:MySqlDbFirstForestRead=null
  private var materialization:MySqlDbFirstRecallMaterialization=null
  private var rollbackOnly:MySqlRollbackOnlyState=null
  private var gate:MySqlConnectionTransactionGate.Owner=null
  private var gateHeld=false
  private var transactionAttempted=false
  private var rollbackAttempted=false
  private var rollbackReturned=false
  private var oldIsolation:Option[Int]=None
  private var isolationAttempted=false
  private var result=Option.empty[O]
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
  private[mysql] def requireConfirmedObservation(observation:O):Unit =
    require(observation != null && result.exists(_ eq observation) && first == null && cleanupFailure == null && retired &&
      progress.outcome == MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "original successful cached DB-first readback observation required; DATA cannot grant confirmation")
  /** 仅原worker在成功读回且reader尚存活时借用；调用方须在关闭reader前登记独立DATA持有人。 */
  private[mysql] def confirmedMainReadSource(observation:O):MySqlScriptSchemaSnapshot =
    require(Thread.currentThread() eq workerThread,"original confirmed readback worker required")
    require(observation!=null && result.exists(_ eq observation) && first==null && cleanupFailure==null &&
      !cleanupStarted && !retired && committedReader!=null && !gateHeld &&
      progress.outcome==MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "original successful live committed reader required before successor materialization")
    committedReader.snapshot
  private[mysql] def resourcesRetired:Boolean=retired
  private[mysql] def requireConfirmedReadbackDraft(draft:MySqlDbFirstRecallDraft):Unit =
    require(Thread.currentThread() eq workerThread,"original confirmed draft worker required")
    require(first==null && result.nonEmpty && !cleanupStarted && !retired && !confirmedDraftDetached &&
      progress.outcome==MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending &&
      materialization!=null && draft!=null && (draft.root eq root) && (draft.result eq materialization) &&
      (draft.before eq materialization.before) && (draft.target eq materialization.target),
      "original verified readback and exact still-live Entry draft required")
  private[mysql] def recordConfirmedDraftDetached(draft:MySqlDbFirstRecallDraft):Unit =
    requireConfirmedReadbackDraft(draft)
    confirmedDraftDetached=true
  /** 只清已确认的本地资源；不发SQL、不重试commit，也不把close失败伪装已退休。 */
  private[mysql] def retireConfirmedResources():Unit =
    require(Thread.currentThread() eq workerThread,"original DB-first retirement worker required")
    if cleanupFailure != null then throw cleanupFailure
    require(!cleanupStarted && !retired && first == null && result.nonEmpty && confirmedDraftDetached &&
      progress.outcome == MySqlDbFirstSession.DbFirstSnapshotOutcome.ReadbackVerifiedPending,
      "only original confirmed readback may retire resources once")
    cleanupStarted=true
    try
      require(!gateHeld && statement == null && rows == null,"confirmed transaction still retains actual JDBC/gate responsibility")
      materialization=null // Entry draft已撤除；不在来源reader退款后保有before/target借用图。
      if history != null then
        history.close();history=null;historyFee=0L
      if forest != null then
        forest.close();forest=null;forestFee=0L
      if committedReader != null then
        committedReader.close();committedReader=null
      if inTransactionReader != null then
        inTransactionReader.close();inTransactionReader=null
      if freshReader != null then
        freshReader.close();freshReader=null
      // 所有reader的qualification引用先detach，之后才可退款owned audit图。
      if audit != null then
        audit.close();audit=null
      if sqlBudget != null then
        sqlBudget.closeScopedReadBudget();sqlBudget=null
      if sqlFee != 0L then
        root.releaseTemporaryMemory(sqlFee,"confirmed DB-first SQL child detached");sqlFee=0L
      require(historyFee == 0L && forestFee == 0L,"confirmed history/forest holder fees remain")
      rollbackOnly=null;gate=null;oldIsolation=None
      // SQL已确定退休；Entry继续持有原capture/request/完整confirmed事实供Main独立cleanup。
      retired=true
    catch case cause:Throwable=>
      cleanupFailure=cause
      if first == null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      throw cause
  private[mysql] def retireEntryPayload():Unit =
    require(Thread.currentThread() eq workerThread,"original Entry retirement worker required")
    require(retired && cleanupFailure == null && first == null,"only actual SQL-retired confirmed Entry may detach")
    materialization=null;result=None;originalRequest=None
  def firstFailure:Option[Throwable]=Option(first)
  def readersPending:Boolean=freshReader != null || inTransactionReader != null || committedReader != null
  def historyPending:Boolean=history != null || historyFee != 0L
  def forestPending:Boolean=forest != null || forestFee != 0L
  def gatePending:Boolean=gateHeld
  private[mysql] def failedSqlGateRetired:Boolean=failedGateRetired
  private[mysql] def failedReadersRetired:Boolean=failedReadersClosed
  private[mysql] def failedSqlResourcesRetired:Boolean=failedSqlResourcesClosed
  /** 仅核对原失败SQL的已验证读回；不改变原commit ACK、outcome或正常确认资格。 */
  private[mysql] def requireFailedRecallSqlSettlement(
      observed:MySqlDbFirstSession.FailedRecallReadback,cause:Throwable):Unit =
    requireFailedCommitObservation()
    require(cause!=null && (first eq cause) && observed!=null && failedReadback.exists(_ eq observed) &&
      failedSqlResourcesClosed && failedGateRetired && failedReadersClosed && cleanupFailure==null,
      "exact original cause, verified readback and successfully retired failed SQL resources required")
  /** 只撤已正向核实且SQL已实际退休的原失败payload，不改原ACK/outcome/first cause。 */
  private[mysql] def retireFailedEntryPayload(observed:MySqlDbFirstSession.FailedRecallReadback,cause:Throwable):Unit =
    require(Thread.currentThread() eq workerThread,"original failed Entry retirement worker required")
    requireFailedRecallSqlSettlement(observed,cause)
    require(!failedEntryPayloadDetached && materialization==null && result.isEmpty &&
      audit==null && freshReader==null && inTransactionReader==null && committedReader==null &&
      history==null && historyFee==0L && forest==null && forestFee==0L &&
      rows==null && statement==null && sqlBudget==null && sqlFee==0L && !gateHeld && gate==null,
      "actual failed SQL graph must be detached before original request")
    originalRequest=None
    failedEntryPayloadDetached=true
  /** 仅结算已物理关闭的原SQL图；原失败、commit ACK与业务UNKNOWN均不改变。 */
  private[mysql] def retireFailedSqlResources(session:MySqlDbFirstSession,
      observed:MySqlDbFirstSession.FailedRecallReadback):Unit =
    requireFailedCommitObservation()
    require(!failedSqlResourcesClosed && failedGateRetired && failedReadersClosed && cleanupFailure==null,
      "original failed SQL resources must close successfully once before local settlement")
    session.requirePreparedFailedRecallReadback(this,observed)
    require(failedReadback.exists(_ eq observed) && materialization==null && audit==null &&
      freshReader==null && inTransactionReader==null && committedReader==null &&
      history==null && historyFee==0L && forest==null && forestFee==0L &&
      rows==null && statement==null && sqlBudget==null && sqlFee==0L &&
      !gateHeld && gate==null && rollbackOnly==null && oldIsolation.isEmpty,
      "actual original JDBC, gate, readers, audit and borrowed DATA must all be detached")
    failedSqlResourcesClosed=true
  /** 原barrier交接下的正向完整DATA证明；只缓存固定scalar，不授予confirmed资格。 */
  private[mysql] def prepareFailedRecallReadback(session:MySqlDbFirstSession,
      image:MySqlScriptSchemaSnapshot):MySqlDbFirstSession.FailedRecallReadback =
    requireFailedCommitObservation()
    require(failedGateRetired && !failedReadbackAttempted && materialization!=null,
      "original gate-retired materialization and one-shot readback preparation required")
    session.requireFailedRecallGateRetirement(this)
    require(session.failedRecallSnapshotOnOwnerThread(reservation) eq image,"actual retained handoff image required")
    failedReadbackAttempted=true
    val steps=materialization.verifyObservedSnapshotHistory(image,root)
    MySqlDbFirstRecallPersistedRead.verifyCommitted(image,request.tag.operationId,root)
    root.reserveRetainedMemory(512L,"failed recall readback scalar and shared fingerprint BEFORE allocation")
    val observed=MySqlDbFirstSession.FailedRecallReadback(request.tag,request.podUuid,
      image.canonicalSha256Hex,steps)
    failedReadback=Some(observed)
    observed
  /** 原正向DATA已独立复制到未发布Main持有人后，才撤原reader；原Entry/cause/coordinator仍保留。 */
  private[mysql] def requireFailedRecallDraft(draft:MySqlDbFirstRecallDraft,
      observed:MySqlDbFirstSession.FailedRecallReadback):Unit =
    requireFailedCommitObservation()
    require(observed!=null && failedReadback.exists(_ eq observed) && materialization!=null &&
      draft!=null && (draft.root eq root) && (draft.result eq materialization) &&
      (draft.before eq materialization.before) && (draft.target eq materialization.target),
      "original prepared readback and exact Entry materialization draft required")
  /** 原正向DATA已独立复制到未发布Main持有人后，才撤原reader；原Entry/cause/coordinator仍保留。 */
  private[mysql] def retireFailedRecallReaders(session:MySqlDbFirstSession,
      observed:MySqlDbFirstSession.FailedRecallReadback):Unit =
    requireFailedCommitObservation()
    require(failedGateRetired && observed!=null && failedReadback.exists(_ eq observed) &&
      !failedReadersCleanupAttempted,"original prepared readback and one-shot reader retirement required")
    session.requirePreparedFailedRecallReadback(this,observed)
    failedReadersCleanupAttempted=true
    try
      require(rows==null && statement==null && !gateHeld && sqlBudget==null && sqlFee==0L,
        "original failed SQL handles/gate must already be detached")
      session.detachPreparedFailedRecallDraft(this,observed)
      // 原before/target借用reader DATA；丢弃materialization引用后才能关闭并退款来源child。
      materialization=null
      if history!=null then
        history.close();history=null;historyFee=0L
      if forest!=null then
        forest.close();forest=null;forestFee=0L
      if committedReader!=null then
        committedReader.close();committedReader=null
      if inTransactionReader!=null then
        inTransactionReader.close();inTransactionReader=null
      if freshReader!=null then
        freshReader.close();freshReader=null
      if audit!=null then
        audit.close();audit=null
      failedReadersClosed=true
    catch case cause:Throwable=>
      cleanupFailure=cause
      MySqlOwnedSessionLifecycle.suppress(first,cause)
      throw cause
  /** 只撤原已关闭连接的JDBC handle/gate；reader/materialization仍为原Entry保留，不能冒充全SQL退休。 */
  private[mysql] def retireFailedSqlGate(session:MySqlDbFirstSession):Unit =
    require(Thread.currentThread() eq workerThread,"original failed SQL retirement worker required")
    requireFailedCommitObservation()
    require(!failedGateCleanupAttempted,"failed SQL gate cleanup is one-shot; no automatic retry")
    val connection=session.requireFailedRecallGateRetirement(this)
    failedGateCleanupAttempted=true
    try
      if rows!=null then
        rows.close();rows=null
      if statement!=null then
        statement.close();statement=null
      if sqlBudget!=null then
        sqlBudget.closeScopedReadBudget();sqlBudget=null
      if sqlFee!=0L then
        root.releaseTemporaryMemory(sqlFee,"failed original SQL handles detached after barrier handoff");sqlFee=0L
      if gateHeld then
        MySqlConnectionTransactionGate.leave(connection,rollbackOnly)
        gateHeld=false;gate=null
      rollbackOnly=null;oldIsolation=None
      failedGateRetired=true
    catch case cause:Throwable=>
      cleanupFailure=cause
      MySqlOwnedSessionLifecycle.suppress(first,cause)
      throw cause
  def commitAttempted:Boolean=progress.commitAttempted
  def acknowledged:Boolean=progress.acknowledged
  def outcome:MySqlDbFirstSession.DbFirstSnapshotOutcome=progress.outcome
  /** 原失败runner只允许另开观测；不清除cause、不重试SQL，也不签发确认。 */
  private[mysql] def failedCommitObservationPending:Boolean =
    progress.failedCommitObservationPending && first!=null && !retired && !cleanupStarted && originalRequest.nonEmpty
  private[mysql] def requireFailedCommitObservation():Unit =
    progress.requireFailedCommitObservation()
    require(first != null && !retired && !cleanupStarted && originalRequest.nonEmpty,
      "original unresolved failed transaction must retain its request and SQL responsibility")
  private[mysql] def compareFailedCommitObservation(candidate:MySqlScriptSchemaSnapshot,
      observationRoot:MySqlFormat3ReadBudget):MySqlDbFirstSession.DbFirstFailedRecallImage =
    requireFailedCommitObservation()
    require(materialization != null && (materialization.request eq request),
      "original failed transaction must retain its exact materialization/request")
    materialization.compareOriginalObservation(candidate,observationRoot)
  private[mysql] def verifyFailedSnapshotReceiptSegment(candidate:MySqlScriptSchemaSnapshot,
      observationRoot:MySqlFormat3ReadBudget):Long =
    requireFailedCommitObservation()
    require(materialization!=null && (materialization.request eq request),"original retained materialization required")
    materialization.verifyObservedSnapshotHistory(candidate,observationRoot)
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
      barrier:HeldSessionBarrier,issuer:MySqlOwnedRetainedPlanIssuer,auditor:MySqlSchemaAuditor):O =
    progress.begin()
    val connection=verified.connection
    def current():Unit =
      inputs.requireForSession(sessionRoot,identity)
      requireExecution(issuer,session,inputs.full27Inputs)
      root.requireScriptDataInputs(inputs.full27Inputs)
      requireCoordinator(coordinator,admission,verified,scope)
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
      publishOriginal(session)
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
      val qualified=auditQualification(coordinator,admission,verified,scope,audit)
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
      materialization=materializeOriginal(issuer,before,barrier.actualConnectionId,
        barrier.connectionGenerationId,planned,completed)
      materialization.verify()
      if materialization.facts.writerEpoch != admission.token.writerEpoch ||
        materialization.facts.leaseUntil != admission.token.leaseUntil || request.bindingUuid != admission.token.bindingUuid then
        fail("locked writer differs from original exact admission owner/epoch/lease")
      current()
      write(connection,materialization,this,()=>current())
      inTransactionReader=reader(inputs,sessionRoot,identity,value=>inTransactionReader=value)
      val actual=inTransactionReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      materialization.verify() // candidate保持Entry root身份；实际reader用独立exact byte comparison。
      if !materialization.target.exactlyEquals(actual) then fail("complete in-transaction Full27 differs from original target")
      dbNow()
      rollbackOnly.cause.foreach(throw _)
      progress.attemptCommit() // 在driver调用前留原attempt；throw/无ACK绝不rollback/retry。
      publishOriginal(session)
      connection.commit()
      progress.acknowledge()
      publishOriginal(session)
      committedReader=reader(inputs,sessionRoot,identity,value=>committedReader=value)
      val committed=committedReader.readLocked(verified,scope,qualified,gate,rollbackOnly)
      if !materialization.target.exactlyEquals(committed) then fail("acknowledged commit has different complete Full27 readback")
      dbNow()
      settleRollback() // 只结束ACK后的只读读回事务，不代表业务cleanup。
      progress.verifyReadback()
      val observed=newObservation(materialization.target.canonicalSha256Hex)
      result=Some(observed)
      observed
    catch case cause:Throwable =>
      if first == null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
      progress.fail()
      if !progress.commitAttempted && statement == null && rows == null then
        try settleRollback()
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      // commit已尝试或ACK后的失败保原gate/connection/reader；不会rollback或重试提交。
      try failOriginal(issuer,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

private[mysql] object MySqlDbFirstSnapshotTransaction:
  val HolderBytes=8192L
  /** 真runner使用的单次commit边界；pure测试不制造JDBC资格或提交成功。 */
  private[mysql] final class Progress(owner:Thread):
    private var begun=false
    private var attempted=false
    private var ack=false
    private var verified=false
    private var fault=false
    private def check():Unit=require(Thread.currentThread() eq owner,"original snapshot transaction worker required")
    def begin():Unit =
      check();require(!begun,"snapshot transaction is one-shot");begun=true
    def attemptCommit():Unit =
      check();require(begun && !attempted && !fault,"original live transaction required before commit")
      attempted=true
    def acknowledge():Unit =
      check();require(attempted && !ack && !fault,"original commit attempt required before ACK");ack=true
    def verifyReadback():Unit =
      check();require(ack && !verified && !fault,"original ACK required before readback");verified=true
    def fail():Unit={check();fault=true}
    def requireFailedCommitObservation():Unit =
      require(failedCommitObservationPending,
        "only original failed commit/ACK-readback uncertainty permits an observation; no execution or confirmation")
    def failedCommitObservationPending:Boolean =
      check()
      begun && attempted && fault && !verified
    def commitAttempted:Boolean=attempted
    def acknowledged:Boolean=ack
    def outcome:MySqlDbFirstSession.DbFirstSnapshotOutcome =
      import MySqlDbFirstSession.DbFirstSnapshotOutcome.*
      if verified then ReadbackVerifiedPending
      else if ack then AcknowledgedPending
      else if attempted then CommitUnknown
      else if fault then FailedBeforeCommit
      else NotAttempted

  private val O="fp_recovery_operation"
  private val R="fp_recovery_reservation"
  private val S=MySqlDbFirstSchemaDefinition.SnapshotTable
  private val D="floating_pod_state"
  private val L="physical_recovery_locator"
  private val W="fp_runtime_writer"
  private val writable=Set(O,R,S,D,L,W)
  /** 闭合六表DAG；所有CAS逐列NULL-safe核实际before，exactly-one不接受缺行/批量写。 */
  private def write(connection:Connection,m:MySqlDbFirstRecallMaterialization,runner:MySqlDbFirstSnapshotTransaction[?,?],
      current:()=>Unit):Unit =
    val inputs=m.before.inputs
    def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=MySqlDbFirstRecallMaterializer.value(row,table,name,inputs)
    def sameKey(a:Vector[MySqlFormat3Cell],b:Vector[MySqlFormat3Cell],table:String):Boolean =
      inputs.forTable(table).primaryKey.forall(name=>value(a,table,name)==value(b,table,name))
    def target(table:String):Vector[MySqlFormat3Cell]=
      val rows=m.target.relation(table).rows.filter { row =>
        table match
          case O=>MySqlDbFirstRecallMaterializer.uuidEquals(value(row,table,"operation_uuid"),m.request.tag.operationId)
          case D=>MySqlDbFirstRecallMaterializer.uuidEquals(value(row,table,"uuid"),m.request.podUuid)
          case W=>true
          case _=>MySqlDbFirstRecallMaterializer.uuidEquals(value(row,table,if table==R then "resource_pod_uuid" else "pod_uuid"),m.request.podUuid)
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
    upsert(R,m.heldReservation)
    upsert(S,target(S))
    upsert(D,target(D))
    upsert(L,target(L))
    put(O,target(O),Some(m.reservedOperation))
    put(R,target(R),Some(m.heldReservation))
    upsert(W,target(W))
