package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRefusalReason
import com.wi.floatingpod.server.io.IoOperationTag
import java.sql.{Connection,SQLDataException,SQLNonTransientConnectionException}
import java.util.UUID

/** 只带已登记的不可变身份；control不是新J/C，也不携带Boot/world/Main对象。 */
private[floatingpod] final class MySqlDbFirstBootRecallTakeoverIntent private[mysql] (
    val controlTag:IoOperationTag,val executionTag:IoOperationTag,
    val original:MySqlDbFirstRecallRequestRecord)

/** 原inactive Entry注册的Main决定；只持metadata，不是capture、SQL或终局结果资格。 */
private[floatingpod] final class MySqlDbFirstBootRecallMainDecision private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,val intent:MySqlDbFirstBootRecallTakeoverIntent,
    val refusalReason:Option[RecallRefusalReason],private[mysql] val handle:MySqlRetainedPlanHandle) extends MySqlDbFirstRecallRefusalSource:
  def tag:IoOperationTag=intent.executionTag
  def originalTag:IoOperationTag=intent.original.originalTag
  def executionTag:IoOperationTag=intent.executionTag
  def journalTag:IoOperationTag=intent.original.journalTag
  def routeTag:IoOperationTag=handle.tag
  def bindingUuid:UUID=intent.original.bindingUuid

/** 原Entry先保管，再进入任何SQL；读资格、partial资源及失败均不借用启动reader槽。 */
private[mysql] final class MySqlDbFirstBootRecallTakeoverWork(
    val intent:MySqlDbFirstBootRecallTakeoverIntent,val root:MySqlFormat3ReadBudget,val worker:Thread):
  private var attempted=false
  private var completed=false
  private var first:Throwable=null
  private var cleanupFailure:Throwable=null
  private var reader:MySqlDbFirstSnapshotJdbc=null
  private var history:MySqlDbFirstSession.HistoricalRead=null
  private var forest:MySqlDbFirstForestRead=null
  private var gate:MySqlConnectionTransactionGate.Owner=null
  private val rollbackOnly=new MySqlRollbackOnlyState
  private var gateHeld=false
  private var gateLeaveAttempted=false
  private var oldIsolation=Option.empty[Int]
  private var isolationAttempted=false
  private var isolationRestoreAttempted=false
  private var isolationRestored=false
  private var transactionAttempted=false
  private var rollbackAttempted=false
  private var rollbackReturned=false
  private var autoCommitRestoreAttempted=false
  private var autoCommitRestored=false
  private var result:MySqlDbFirstRecallJournalLink=null
  private var metadataRetired=false
  def resourcesRetired:Boolean=completed && first==null && cleanupFailure==null &&
    !readersPending && !historyPending && !forestPending && !gatePending
  def retireRefusalMetadata(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    owner();receipt.core.requireBootRefusalFinalPayload(receipt,this)
    require(resourcesRetired && !metadataRetired,"actual Boot takeover metadata retirement is one-shot")
    result=null;metadataRetired=true
  /** 正常C与retained C均核原drop/runner的真实退休阶段；不把metadata当最终ACK。 */
  def retireRecallMetadata(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    owner();require(receipt!=null,"actual original C drop receipt required")
    receipt.core.requireBootRecallFinalPayload(receipt,this)
    require(resourcesRetired && !metadataRetired,"actual Boot takeover metadata retirement is one-shot")
    result=null;metadataRetired=true
  def isMetadataRetired:Boolean=metadataRetired
  def readersPending:Boolean=reader!=null
  def historyPending:Boolean=history!=null
  def forestPending:Boolean=forest!=null
  def gatePending:Boolean=gateHeld
  private def owner():Unit=require(Thread.currentThread() eq worker,"original Boot takeover worker required")
  def requireCompleted():MySqlDbFirstRecallJournalLink =
    owner()
    require(completed && first==null && cleanupFailure==null && !readersPending && !historyPending &&
      !forestPending && !gateHeld && result!=null,"actual fresh-read cleanup must finish before inactive C registration")
    result

  def execute(session:MySqlDbFirstSession,inputs:MySqlDbFirstSessionInputs,sessionRoot:MySqlFormat3ReadBudget,identity:MySqlOwnedIoIdentity,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,qualification:VerifiedSchemaQualification):Unit =
    owner();require(!attempted && session!=null,"Boot takeover read is one-shot")
    attempted=true
    val connection=verified.connection
    def cleanup():Unit =
      // ResultSet/statement由reader自身保管；失败close不能冒认gate已退。
      if history!=null then { history.close();history=null }
      if forest!=null then { forest.close();forest=null }
      if reader!=null then { reader.close();reader=null }
      if gateHeld then
        if transactionAttempted then
          require(!rollbackAttempted,"Boot takeover rollback outcome remains uncertain; no retry")
          rollbackAttempted=true;connection.rollback();rollbackReturned=true
          require(rollbackReturned,"actual rollback return required")
          autoCommitRestoreAttempted=true;connection.setAutoCommit(true)
          autoCommitRestored=connection.getAutoCommit
          if !autoCommitRestored then throw new SQLNonTransientConnectionException("Boot takeover auto-commit not restored")
        if isolationAttempted then
          isolationRestoreAttempted=true;connection.setTransactionIsolation(oldIsolation.get)
          isolationRestored=connection.getTransactionIsolation==oldIsolation.get
          if !isolationRestored then throw new SQLNonTransientConnectionException("Boot takeover isolation not restored")
        require(!gateLeaveAttempted,"Boot takeover original gate exit is one-shot")
        gateLeaveAttempted=true;MySqlConnectionTransactionGate.leave(connection,rollbackOnly)
        gateHeld=false;gate=null
    try
      session.requireBootTakeoverRead(this,inputs,sessionRoot,identity,verified,scope,qualification)
      inputs.requireForSession(sessionRoot,identity)
      require(connection.getAutoCommit,"Boot takeover requires idle actual connection")
      oldIsolation=Some(connection.getTransactionIsolation)
      MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
        case Some(cause)=>throw cause
        case None=>gateHeld=true
      gate=MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly)
      isolationAttempted=true;connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      transactionAttempted=true;connection.setAutoCommit(false)
      require(!connection.getAutoCommit && connection.getTransactionIsolation==Connection.TRANSACTION_REPEATABLE_READ,
        "Boot takeover actual REPEATABLE READ required")
      try reader=MySqlDbFirstSnapshotJdbc.create(inputs,sessionRoot,identity)
      catch case creation:MySqlDbFirstSnapshotJdbc.CreationFailure=>
        reader=creation.retainedHolder
        throw creation
      val image=reader.readLocked(verified,scope,qualification,gate,rollbackOnly)
      sessionRoot.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"Boot takeover history holder BEFORE allocation")
      try history=new MySqlDbFirstSession.HistoricalRead(sessionRoot,inputs.full27Inputs)
      catch case cause:Throwable=>
        try sessionRoot.releaseRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"unallocated Boot history holder")
        catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
        throw cause
      history.acquire(inputs.historicalInputs);history.validate(image)
      sessionRoot.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"Boot takeover forest holder BEFORE allocation")
      try forest=new MySqlDbFirstForestRead(sessionRoot,inputs.full27Inputs,inputs.auditExpectation.bindingUuid)
      catch case cause:Throwable=>
        try sessionRoot.releaseRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"unallocated Boot forest holder")
        catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
        throw cause
      forest.validate(image)
      result=MySqlDbFirstBootRecallTakeoverData.strictLink(image,sessionRoot,root,intent)
      cleanup()
      session.requireBootTakeoverRead(this,inputs,sessionRoot,identity,verified,scope,qualification)
      completed=true
    catch case cause:Throwable=>
      first=cause
      try cleanup()
      catch case secondary:Throwable=>
        cleanupFailure=secondary
        if secondary ne cause then MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

/** strict DATA检查可以独立复测；本对象自身不发行worker资格、Entry或capture许可。 */
private[mysql] object MySqlDbFirstBootRecallTakeoverData:
  def strictLink(image:MySqlScriptSchemaSnapshot,sourceRoot:MySqlFormat3ReadBudget,
      retainedRoot:MySqlFormat3ReadBudget,intent:MySqlDbFirstBootRecallTakeoverIntent):MySqlDbFirstRecallJournalLink =
    var retained:MySqlDbFirstRecallJournalLink=null
    withStrictJournal(image,sourceRoot,retainedRoot,intent) { request=>
      // comparison child退款前独立预付并复制metadata，不保留canonical/raw/child边。
      retainedRoot.reserveRetainedMemory(4096L,"Boot retained V3 identity BEFORE independent metadata copy")
      def id(value:UUID)=new UUID(value.getMostSignificantBits,value.getLeastSignificantBits)
      def tag(value:IoOperationTag)=IoOperationTag(id(value.bootId),value.bootGeneration,value.connectionGeneration,id(value.operationId))
      val link=MySqlDbFirstRecallJournalLink(tag(request.journalTag),id(request.actorUuid),request.clientRequestUuid.map(id),
        Vector.tabulate(32)(request.canonical.sha256(_)),Some(tag(request.originalTag)))
      link.requireJournalIdentity(request,intent.executionTag)
      retained=link
    }
    require(retained!=null,"strict Boot original metadata construction incomplete")
    retained

  /** 新的完整读中构造原C拒绝DATA；显式原因来自调用者，不能从V3 link/phase推导。
    * 此工厂不证明Main拒绝、capture已封锁、barrier有效或可写SQL，不能替代实际Entry许可。
    */
  def strictRefusalRequest(image:MySqlScriptSchemaSnapshot,sourceRoot:MySqlFormat3ReadBudget,
      retainedRoot:MySqlFormat3ReadBudget,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reason:RecallRefusalReason):MySqlDbFirstRecallRefusalRequest =
    require(reason!=null,"actual typed Boot Main refusal reason required; no default")
    withStrictJournal(image,sourceRoot,retainedRoot,intent)(_=>())
    // 不把比较child中的J对象带出；退出child后只从已严格匹配的独立record编码。
    MySqlDbFirstRecallRefusalRequest.notAppliedRecoveredWithReason(intent.executionTag,intent.original,reason,retainedRoot)

  /** 解码J只在比较child存活期间回调；两个消费者均独立计费，禁止泄露原reader/canonical。 */
  private def withStrictJournal(image:MySqlScriptSchemaSnapshot,sourceRoot:MySqlFormat3ReadBudget,
      retainedRoot:MySqlFormat3ReadBudget,intent:MySqlDbFirstBootRecallTakeoverIntent)(
      consume:MySqlDbFirstRecallJournalRequest=>Unit):Unit =
    require(intent!=null && image!=null && sourceRoot!=null && retainedRoot!=null &&
      !retainedRoot.isClosed && retainedRoot.sharesAllocationLedger(sourceRoot),"actual takeover metadata allocation required")
    val execution=intent.executionTag
    val control=intent.controlTag
    require(execution!=null && control!=null && execution.bootId==image.observationScope.bootUuid &&
      execution.bootGeneration==image.observationScope.bootGeneration && execution.connectionGeneration==image.observationScope.connectionGeneration &&
      control.bootId==execution.bootId && control.bootGeneration==execution.bootGeneration && control.connectionGeneration==execution.connectionGeneration &&
      control.operationId!=execution.operationId,"actual current Boot control/execution scope required")
    val original=intent.original
    require(original!=null && original.phase==MySqlDbFirstRecallRequestPhase.JournalCommitted && original.refusalReason.isEmpty &&
      execution.operationId==original.originalTag.operationId,
      "original J-only request identity required")
    val inputs=image.inputs
    val rows=image.relation("fp_recovery_operation").rows
    def v(row:Vector[MySqlFormat3Cell],name:String)=MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation",name,inputs)
    def uuid(row:Vector[MySqlFormat3Cell],name:String,id:UUID)=v(row,name) match
      case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(id)
      case _=>false
    if rows.exists(r=>uuid(r,"operation_uuid",original.originalTag.operationId)) then
      throw new SQLDataException("Boot takeover original C is present; never reinterpret as absence")
    MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(image,original.journalTag.operationId,sourceRoot)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(sourceRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(sourceRoot)
      child.reserveRetainedMemory(256L+32L*rows.size,"Boot original J row selection BEFORE allocation")
      val selected=rows.filter(r=>uuid(r,"operation_uuid",original.journalTag.operationId))
      require(selected.size==1,"actual unique original committed J required")
      val binary=v(selected.head,"request_identity_bytes") match
        case MySqlFormat3Cell.Binary(bytes)=>bytes
        case _=>throw new SQLDataException("Boot takeover original J request bytes missing")
      val request=MySqlDbFirstRecallJournalRequest.decode(binary,child)
      require(request.journalTag==original.journalTag && request.originalTag==original.originalTag &&
        request.bindingUuid==original.bindingUuid && request.actorUuid==original.actorUuid && request.podUuid==original.podUuid &&
        request.expectedStateVersion==original.expectedStateVersion && request.clientRequestUuid==original.clientRequestUuid &&
        request.canonical.hexSha256==original.journalRequestSha256,"Boot original complete J/C identity differs")
      consume(request)
    }
