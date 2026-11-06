package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.{IoOperationTag,IoMaintenanceDeadline}
import java.sql.{Connection, SQLDataException, SQLNonTransientConnectionException}
import java.time.Duration
import java.util.UUID
import scala.util.control.NonFatal

/** 原owned worker的数据库只读观测阶段；未取得fence/native/恢复资格，不代表admitted或Ready。 */
private[mysql] final class MySqlDbFirstSession private (
    private val inputs: MySqlDbFirstSessionInputs,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private val thread: Thread,
    private val auditMaxAge: Duration,
    private val full27: MySqlScriptSchemaInputs,
    private val ledger: MySqlFormat3AllocationLedger,
    private var holderFee: Long,
    private val inspectionOnly: Boolean=false,
    private val inspectionParent: MySqlDbFirstSession=null,
    private var serviceSource: MySqlDbFirstSession.RetiredRecallWriterRecovery=null
):
  import MySqlDbFirstSession.*
  // 原operation/inputs仍用identity关联；独立观测连接的SQL scope不能冒充原连接代。
  private val scopeConnectionGeneration=if inspectionOnly then Math.incrementExact(identity.connectionGeneration)
    else identity.connectionGeneration
  private var state=State.Allocated
  private var started=false
  private var primary: Throwable=null
  private var cleanupFailure: Throwable=null
  private var closeAttempted=false
  private var inputsClosed=false
  private var ownsInputs=false
  private var inputBorrow: AutoCloseable=null
  // 只保旧audit/reader所需引用；实际close责任已转交，不再拥有此连接。
  private var connectionSuccessor:MySqlDbFirstSession=null
  private var retiringInspectionSource:MySqlDbFirstSession.RetiredRecallWriterRecovery=null
  private var serviceAdapter:MySqlOwnedIoAdapter=null
  private var adapterCleanupOwned=false
  private var adapterCleanupEdgesDetached=false
  private var ownedServiceMailbox:MySqlOwnedMaintenanceMailbox=null
  private var ownedServiceMailboxFee=0L
  private var ownedServiceControlFee=0L
  private var retainedIssuer: MySqlOwnedRetainedPlanIssuer=null
  private var retainedIssuerFee=0L
  private var bootTakeoverWork:MySqlDbFirstBootRecallTakeoverWork=null
  private var openHolder: MySqlConnectionSource.SourceUnknownInspectionOpen=null
  private var verified: VerifiedMySqlConnection=null
  private var scope: MySqlAuditBootScope=null
  private var auditor: MySqlSchemaAuditor=null
  private var qualification: VerifiedSchemaQualification=null
  private var auditHolder: MySqlSchemaAuditor.DbFirstAudit=null
  private var reader: MySqlDbFirstSnapshotJdbc=null
  // 每个实际reader预收的4096覆盖其登记节点；固定list头属于本Session holder。
  private val serviceReaders=new java.util.LinkedList[MySqlDbFirstSnapshotJdbc]()
  private var historicalRead: HistoricalRead=null
  private var forestRead: MySqlDbFirstForestRead=null
  private var databaseObservation: DatabaseObservation=null
  private var mainRead:MySqlDbFirstMainReadBacking=null
  private var mainReadFee=0L
  private var bootRecallCensus:MySqlDbFirstBootRecallCensus=null
  private var pendingMainRead:MySqlDbFirstMainReadBacking=null
  private var pendingMainReadFee=0L
  private var pendingMainReadObservation:DbFirstRecallRefusalObservation | DbFirstRecallJournalObservation | DbFirstRecallObservation | DbFirstCheckpointObservation | DbFirstInitialActivationObservation | DbFirstInitialPreparationObservation | DbFirstInitialPreparationRejection=null
  private var refusalTransaction:MySqlDbFirstRecallRefusalTransaction=null
  private var journalTransaction:MySqlDbFirstRecallJournalTransaction=null
  private var recallTransaction:MySqlDbFirstRecallTransaction=null
  private var recallInspectionAttempted=false
  private var recallInspection:MySqlDbFirstSession=null
  private var recallInspectionAllocationFee=0L
  private var closedRecallInspection:Option[ResourceObservation]=None
  private var recallImageObservation:Option[DbFirstFailedRecallImage]=None
  private var recallAcceptedEpoch:Option[Long]=None
  private var recallPreparedReadback:Option[FailedRecallReadback]=None
  private var recallEntrySqlSettlementRecorded=false
  private var recallLeaseObservationAttempted=false
  private var recallObservedLease:Option[MySqlAdmissionWindow]=None
  private var recallObservationAuditAttempted=false
  private var recallObservationAuditCompleted=false
  private var recallBeforeLeaseObservationAttempted=false
  private var recallBeforeObservedLease:Option[MySqlAdmissionWindow]=None
  private var recallBeforeAuditAttempted=false
  private var recallBeforeAuditCompleted=false
  private var recallNotAppliedQualificationAttempted=false
  private var recallNotAppliedQualification:FailedRecallNotAppliedQualification=null
  private var recallNotAppliedQualificationFee=0L
  private var recallCleanupQualificationAttempted=false
  private var recallCleanupQualification:FailedRecallCleanupQualification=null
  private var recallCleanupQualificationFee=0L
  // 固定Session holder内的退休summary槽；保原SQL历史，不伪造正常ACK或释放Session DATA。
  private var retainedRecallRetirementReceipt:MySqlDbFirstRecallDropReceipt=null
  private var retainedRecallPayloadDetached=false
  private var retainedRecallFinishAttempted=false
  private var retiredRecallOutcome:Option[DbFirstRecallOutcome]=None
  private var retiredRecallCommitAttempted=false
  private var retiredRecallAcknowledged=false
  private var retiredRecallObservationAttempted=false
  private var retiredRecallObservation:RetiredRecallObservation=null
  private var retiredRecallObservationAllocationFee=0L
  private var closedRetiredRecallObservation:Option[RetiredRecallObservationState]=None
  private var retiredRecallWriterAttempted=false
  private var retiredRecallWriter:RetiredRecallWriterRecovery=null
  private var retiredRecallWriterAllocationFee=0L
  private var retiredRecallCleanupAdapter:MySqlOwnedIoAdapter=null
  private var closedRetiredRecallWriter:Option[RetiredRecallWriterState]=None
  // 已转管父Session只允许一次退款；结果不明时保留原owner与费用，不重试。
  private var transferredParentHolderRefundAttempted=false
  private var transferredParentHolderRefundReturned=false
  private var recallReadbackAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var recallReadbackQualification:VerifiedSchemaQualification=null
  private var recallSqlRetirementAttempted=false
  private var recallSqlResponsibilityReleased=false
  private var checkpointTransaction:MySqlDbFirstCheckpointTransaction=null
  private var preparationTransaction:MySqlDbFirstInitialPreparationTransaction=null
  private var settledPreparationRejection:DbFirstInitialPreparationRejection=null
  private var initialTransaction:MySqlDbFirstInitialActivationTransaction=null
  private var writerAttempted=false
  private var writerRenewSequence=0L
  private var writerCoordinator:MySqlWriterFenceCoordinator=null
  private var writerPreAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var writerReleaseAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var writerReleaseAttempted=false
  private var lastBarrierObservation:Option[MySqlSessionBarrierCloseObservation]=None
  private var writerPostAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var pendingWriterPreAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var pendingWriterPostAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var retiringWriterPreAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var retiringWriterPostAudit:MySqlSchemaAuditor.DbFirstAudit=null
  private var previousWriterToken:MySqlWriterFenceToken=null
  private var previousWriterAdmission:MySqlBusinessAdmission=null
  private var retiringAuditFailure:Throwable=null
  private var writerToken:MySqlWriterFenceToken=null
  private var writerAdmission:MySqlBusinessAdmission=null
  private var writerFee=0L
  private var barrier:HeldSessionBarrier=null
  private var barrierAttempted=false
  private var barrierHeld=false
  private var barrierMayRetain=false
  private var managedFirstBindingActor:MySqlDbFirstBootstrapActor=null
  private var managedFirstBindingFee=0L

  private var rollbackOnly: MySqlRollbackOnlyState=null
  private var gate: MySqlConnectionTransactionGate.Owner=null
  private var gateHeld=false
  private var gateLeaveAttempted=false
  private var oldIsolation: Option[Int]=None
  private var isolationAttempted=false
  private var isolationRestoreAttempted=false
  private var isolationRestored=false
  private var transactionAttempted=false
  private var rollbackAttempted=false
  private var rollbackReturned=false
  private var rollbackFailure: Throwable=null
  private var autoCommitRestoreAttempted=false
  private var autoCommitRestored=false
  private var jdbcCloseAttempted=false
  private var jdbcCloseReturned=false
  private var jdbcClosedObserved=false
  @volatile private var resources=ResourceObservation(State.Allocated,false,false,false,false,false,true,false,false,
    holderFee,None,None,false,false,false,false,false,false,false,None,false)

  /** 不含JDBC、scope或snapshot引用，可供原adapter跨线程读取。 */
  def resourceObservation: ResourceObservation=resources
  def firstFailure: Option[Throwable]=resources.failure
  private def rawPending: Boolean = openHolder != null && openHolder.hasRawHandle() && !openHolder.closeReturned()
  private def publish(): Unit =
    resources=ResourceObservation(state,state == State.Closed,verified != null || rawPending,rawPending,
      reader != null || !serviceReaders.isEmpty || (bootTakeoverWork!=null && bootTakeoverWork.readersPending) || (refusalTransaction!=null && refusalTransaction.readersPending) || (journalTransaction!=null && journalTransaction.readersPending) || (preparationTransaction != null && preparationTransaction.readersPending) || (initialTransaction != null && initialTransaction.readersPending) || (recallTransaction != null && recallTransaction.readersPending) || (retiredRecallObservation!=null && retiredRecallObservation.readerPending) || (retiredRecallWriter!=null && retiredRecallWriter.readerPending) || (checkpointTransaction != null && checkpointTransaction.readersPending),gateHeld || (bootTakeoverWork!=null && bootTakeoverWork.gatePending) || (refusalTransaction!=null && refusalTransaction.gatePending) || (journalTransaction!=null && journalTransaction.gatePending) || (preparationTransaction != null && preparationTransaction.gatePending) || (initialTransaction != null && initialTransaction.gatePending) || (recallTransaction != null && recallTransaction.gatePending) || (retiredRecallObservation!=null && retiredRecallObservation.gatePending) || (retiredRecallWriter!=null && retiredRecallWriter.gatePending) || (checkpointTransaction != null && checkpointTransaction.gatePending),!inputsClosed,jdbcCloseReturned,jdbcClosedObserved,holderFee,Option(primary),Option(cleanupFailure),auditHolder != null || recallReadbackAudit!=null || (retiredRecallWriter!=null && retiredRecallWriter.resourceObservation.auditPending) || (retiredRecallObservation!=null && retiredRecallObservation.auditPending) || (refusalTransaction!=null && refusalTransaction.auditPending) || (journalTransaction!=null && journalTransaction.auditPending),retainedIssuer != null || retainedIssuerFee != 0L,historicalRead != null || (retiredRecallObservation!=null && retiredRecallObservation.historyPending) || (retiredRecallWriter!=null && retiredRecallWriter.historyPending) || (bootTakeoverWork!=null && bootTakeoverWork.historyPending) || (refusalTransaction!=null && refusalTransaction.historyPending) || (journalTransaction!=null && journalTransaction.historyPending) || (preparationTransaction != null && preparationTransaction.historyPending) || (initialTransaction != null && initialTransaction.historyPending) || (recallTransaction != null && recallTransaction.historyPending) || (checkpointTransaction != null && checkpointTransaction.historyPending),forestRead != null || (retiredRecallObservation!=null && retiredRecallObservation.forestPending) || (retiredRecallWriter!=null && retiredRecallWriter.forestPending) || (bootTakeoverWork!=null && bootTakeoverWork.forestPending) || (refusalTransaction!=null && refusalTransaction.forestPending) || (journalTransaction!=null && journalTransaction.forestPending) || (preparationTransaction != null && preparationTransaction.forestPending) || (initialTransaction != null && initialTransaction.forestPending) || (recallTransaction != null && recallTransaction.forestPending) || (checkpointTransaction != null && checkpointTransaction.forestPending),barrierAttempted,barrierHeld,barrierMayRetain,lastBarrierObservation,writerCoordinator != null || writerFee != 0L,recallTransaction != null,
      (recallTransaction != null && recallTransaction.commitAttempted) || retiredRecallCommitAttempted,
      (recallTransaction != null && recallTransaction.acknowledged) || retiredRecallAcknowledged,
      Option(recallTransaction).map(_.outcome).orElse(retiredRecallOutcome),initialTransaction!=null,Option(initialTransaction).map(_.outcome),preparationTransaction!=null,Option(preparationTransaction).map(_.outcome),checkpointTransaction!=null,
      checkpointTransaction!=null && checkpointTransaction.commitAttempted,checkpointTransaction!=null && checkpointTransaction.acknowledged,Option(checkpointTransaction).map(_.outcome),
      recallInspectionAttempted,Option(recallInspection).map(_.resourceObservation).orElse(closedRecallInspection),recallInspectionAllocationFee,
      recallImageObservation,(recallTransaction!=null && recallTransaction.failedSqlGateRetired) || retainedRecallPayloadDetached,
      recallPreparedReadback,(recallTransaction!=null && recallTransaction.failedReadersRetired) || retainedRecallPayloadDetached,
      recallSqlRetirementAttempted,(recallTransaction!=null && recallTransaction.failedSqlResourcesRetired) || retainedRecallPayloadDetached,
      recallSqlResponsibilityReleased,scopeConnectionGeneration,recallObservedLease,recallLeaseObservationAttempted,
      recallObservationAuditAttempted,recallObservationAuditCompleted,recallCleanupQualificationAttempted,
      recallCleanupQualification!=null,recallCleanupQualificationFee,
      recallBeforeLeaseObservationAttempted,recallBeforeObservedLease,recallBeforeAuditAttempted,recallBeforeAuditCompleted,
      recallNotAppliedQualificationAttempted,recallNotAppliedQualification!=null,recallNotAppliedQualificationFee,
      journalTransaction!=null,journalTransaction!=null && journalTransaction.commitAttempted,
      journalTransaction!=null && journalTransaction.acknowledged,Option(journalTransaction).map(_.outcome),refusalTransaction!=null,refusalTransaction!=null && refusalTransaction.commitAttempted,
      refusalTransaction!=null && refusalTransaction.acknowledged,Option(refusalTransaction).map(_.outcome),
      retiredRecallObservationAttempted,Option(retiredRecallObservation).map(_.resourceObservation).orElse(closedRetiredRecallObservation),
      retiredRecallObservationAllocationFee,retiredRecallWriterAttempted,
      Option(retiredRecallWriter).map(_.resourceObservation).orElse(closedRetiredRecallWriter),retiredRecallWriterAllocationFee,
      connectionSuccessor!=null,adapterCleanupOwned,ownedServiceMailbox!=null,ownedServiceMailboxFee,ownedServiceControlFee,
      Option(managedFirstBindingActor).map(_.resourceObservation),managedFirstBindingFee)
  private def requireThread(): Unit =
    if Thread.currentThread() ne thread then throw new IllegalStateException("schema6 Session requires original owned IO worker")
  private def requireOpen(): Unit =
    requireThread()
    if closeAttempted || state == State.Closed then throw new IllegalStateException("schema6 Session detached or cleanup incomplete",cleanupFailure)
    inputs.requireForSession(root,identity)
  private def retainFailure(cause: Throwable): Unit =
    if recallCleanupQualification!=null then recallCleanupQualification.revoke()
    if primary == null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
    state=State.Failed
    publish()

  private[mysql] def requireDbFirstAuditSource(actualRoot:MySqlFormat3ReadBudget,
      actualInputs:MySqlDbFirstSessionInputs,actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope):Unit =
    requireThread()
    if (actualRoot ne root) || (actualInputs ne inputs) || actualVerified == null || actualScope == null ||
      (actualVerified ne verified) || (actualScope ne scope) || (actualScope.ownerThread ne thread) ||
      (actualScope.connectionToken ne actualVerified.connection) || !actualScope.verifiedConnection.contains(actualVerified) ||
      (actualScope.connectionIdentity ne actualVerified.identity) || actualScope.bootUuid != identity.bootId ||
      actualScope.bootGeneration != identity.bootGeneration || actualScope.connectionGeneration != scopeConnectionGeneration then
      throw new IllegalArgumentException("original schema6 Session audit inputs/root/connection/scope required")
    inputs.requireForSession(root,identity)
  private[mysql] def requireDbFirstAuditBinding(holder:MySqlSchemaAuditor.DbFirstAudit,
      actualRoot:MySqlFormat3ReadBudget,actualInputs:MySqlDbFirstSessionInputs,
      actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope):Unit =
    requireDbFirstAuditSource(actualRoot,actualInputs,actualVerified,actualScope)
    if holder == null || (auditHolder ne holder) && (recallReadbackAudit ne holder) && (writerPreAudit ne holder) && (writerPostAudit ne holder) && (writerReleaseAudit ne holder) &&
        (pendingWriterPreAudit ne holder) && (pendingWriterPostAudit ne holder) &&
        (retiringWriterPreAudit ne holder) && (retiringWriterPostAudit ne holder) &&
        (refusalTransaction == null || (refusalTransaction.audit ne holder)) &&
        (journalTransaction == null || (journalTransaction.audit ne holder)) &&
        (recallTransaction == null || (recallTransaction.audit ne holder)) &&
        (checkpointTransaction == null || (checkpointTransaction.audit ne holder)) &&
        (initialTransaction == null || (initialTransaction.audit ne holder)) &&
        (preparationTransaction == null || (preparationTransaction.audit ne holder)) &&
        !ownsRetiredRecallAudit(holder) && !ownsRetiredWriterAudit(holder) then throw new IllegalArgumentException("registered original schema6 partial audit holder required")

  private[mysql] def requireDbFirstWriterAuditSource(actualRoot:MySqlFormat3ReadBudget,
      actualInputs:MySqlDbFirstSessionInputs,actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope,
      actualCoordinator:MySqlWriterFenceCoordinator):Unit =
    requireDbFirstAuditSource(actualRoot,actualInputs,actualVerified,actualScope)
    val recovery=inspectionParent!=null && inspectionParent.retiredRecallWriter!=null &&
      inspectionParent.retiredRecallWriter.ownsCoordinator(this,actualCoordinator)
    if !recovery && (!writerAttempted || actualCoordinator == null || (actualCoordinator ne writerCoordinator) ||
        inputs.auditExpectation.profile != MySqlSchemaAuditProfile.DbFirst1) then
      throw new IllegalArgumentException("original registered schema6 writer coordinator required")

  private def ownsRetiredWriterAudit(holder:MySqlSchemaAuditor.DbFirstAudit):Boolean =
    inspectionParent!=null && inspectionParent.retiredRecallWriter!=null &&
      inspectionParent.retiredRecallWriter.ownsAudit(this,holder)

  private def ownsRetiredRecallAudit(holder:MySqlSchemaAuditor.DbFirstAudit):Boolean =
    inspectionParent!=null && inspectionParent.retiredRecallObservation!=null &&
      inspectionParent.retiredRecallObservation.ownsAudit(this,holder) ||
      inspectionParent!=null && inspectionParent.retiredRecallWriter!=null &&
        inspectionParent.retiredRecallWriter.ownsReadbackAudit(this,holder)

  /** 只有实际inspection Session能给其reader绑定新SQL连接代；原inputs/operation身份保持不变。 */
  private[mysql] def requireDbFirstObservationReaderSource(actualInputs:MySqlDbFirstSessionInputs,
      actualRoot:MySqlFormat3ReadBudget,actualIdentity:MySqlOwnedIoIdentity):Long =
    requireOpen()
    val stoppedRelease=inspectionParent!=null && inspectionParent.retiredRecallWriter!=null &&
      inspectionParent.retiredRecallWriter.ownsStoppedReleaseObservationFor(this)
    require(inspectionOnly && started && inspectionParent!=null &&
      (inspectionParent.recallInspection eq this) && inspectionParent.recallInspectionAttempted &&
      (inspectionParent.inputs eq inputs) && (inspectionParent.root eq root) &&
      (inspectionParent.identity eq identity) &&
      (inspectionParent.state==State.Failed || stoppedRelease) &&
      inspectionParent.primary!=null && inspectionParent.recallTransaction!=null &&
      inputBorrow!=null && (actualInputs eq inputs) && (actualRoot eq root) &&
      (actualIdentity eq identity) && scopeConnectionGeneration>identity.connectionGeneration,
      "actual independent inspection Session required; no arbitrary generation override")
    requireDbFirstAuditSource(actualRoot,actualInputs,verified,scope)
    scopeConnectionGeneration

  /** 只取得数据库writer资格；不授予native、恢复、业务完成或Ready。 */
  def acquireSqlWriter(leaseDuration:Duration,barrierWait:Duration):SqlWriterObservation =
    requireOpen()
    require(!inspectionOnly,"failed-operation observation cannot acquire a writer or mutation issuer")
    if state != State.DatabaseObserved || databaseObservation == null || writerAttempted then
      throw new IllegalStateException("schema6 SQL writer acquisition requires one completed database observation",primary)
    require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero &&
      leaseDuration.toNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && leaseDuration.toNanos % 1000L == 0L,
      "explicit bounded microsecond writer lease required")
    require(barrierWait != null && !barrierWait.isNegative && barrierWait.compareTo(Duration.ofSeconds(30)) <= 0 &&
      barrierWait.getNano == 0,"explicit whole-second barrier wait within 0..30 required")
    writerAttempted=true
    try
      root.reserveRetainedMemory(8192L,"schema6 coordinator/fence/admission/barrier/summary BEFORE allocation")
      writerFee=8192L
      writerCoordinator=new MySqlWriterFenceCoordinator(inputs.auditExpectation,auditor,scope,leaseDuration)
      publish() // coordinator及原verified内的barrier协议在任何writer SQL前归Session持有。
      val firstBinding=managedFirstBindingActor
      if firstBinding!=null && firstBinding.hasCompletedWithBarrier(verified,scope,barrier) then
        verified.readmitSessionBarrier(inputs.auditExpectation,scope)
      else
        barrierAttempted=true
        publish()
        verified.acquireSessionBarrier(inputs.auditExpectation,scope,barrierWait) match
          case MySqlSessionBarrierAcquisition.Held(actual) => barrier=actual
          case MySqlSessionBarrierAcquisition.Deferred =>
            throw new SQLNonTransientConnectionException("schema6 SQL writer barrier is busy; acquisition not completed")
      refreshBarrierFacts()
      publish()
      writerPreAudit=auditor.beginDbFirstWriterAudit(inputs,root,verified,scope,this,writerCoordinator)
      publish()
      writerToken=writerCoordinator.acquireOwnedDbFirst(verified,scope,writerPreAudit)
      publish()
      writerPostAudit=auditor.beginDbFirstWriterAudit(inputs,root,verified,scope,this,writerCoordinator)
      publish()
      writerAdmission=writerCoordinator.auditOwnedDbFirstPostAcquire(writerToken,writerPostAudit)
      refreshAcquiredDatabaseSnapshot()
      state=State.SqlWriterAcquired
      val result=SqlWriterObservation(identity,full27.schema,writerToken.bindingUuid,writerToken.writerEpoch,
        writerToken.leaseUntil,state)
      publish()
      result
    catch
      case cause:Throwable =>
        retainFailure(cause)
        try refreshBarrierFacts()
        catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        publish()
        throw cause
  /** 沿原固定lease policy续租；新的完整post-audit确认前不恢复SQL admission。 */
  def renewSqlWriter():SqlWriterObservation =
    requireOpen()
    if state != State.SqlWriterAcquired || writerCoordinator == null || writerAdmission == null ||
        pendingWriterPreAudit != null || pendingWriterPostAudit != null ||
        retiringWriterPreAudit != null || retiringWriterPostAudit != null then
      throw new IllegalStateException("schema6 SQL writer renewal requires its original current admission and settled audit holders",primary)
    val nextSequence=Math.incrementExact(writerRenewSequence)
    try
      previousWriterToken=writerToken;previousWriterAdmission=writerAdmission
      state=State.SqlWriterRenewing
      writerCoordinator.beginOwnedDbFirstRenewal(writerAdmission,verified,scope)
      publish()
      verified.readmitSessionBarrier(inputs.auditExpectation,scope)
      pendingWriterPreAudit=auditor.beginDbFirstWriterAudit(inputs,root,verified,scope,this,writerCoordinator)
      publish() // 新一轮holder在首次audit/renew SQL前登记；旧qualification仍被真实token持有。
      writerToken=writerCoordinator.renewOwnedDbFirst(writerAdmission,verified,scope,pendingWriterPreAudit)
      writerAdmission=null
      publish()
      pendingWriterPostAudit=auditor.beginDbFirstWriterAudit(inputs,root,verified,scope,this,writerCoordinator)
      publish()
      writerAdmission=writerCoordinator.auditOwnedDbFirstPostAcquire(writerToken,pendingWriterPostAudit)
      writerCoordinator.requireOwnedDbFirstAuditRetirement(previousWriterToken,previousWriterAdmission,
        writerToken,writerAdmission,writerPreAudit,writerPostAudit)
      // 原phase/provider/unknown槽已核不再消费旧图，Session亦先丢弃旧authority引用。
      previousWriterToken=null;previousWriterAdmission=null
      retiringWriterPreAudit=writerPreAudit;retiringWriterPostAudit=writerPostAudit
      writerPreAudit=pendingWriterPreAudit;writerPostAudit=pendingWriterPostAudit
      pendingWriterPreAudit=null;pendingWriterPostAudit=null
      retireSupersededWriterAudits()
      state=State.SqlWriterAcquired
      writerRenewSequence=nextSequence
      val result=SqlWriterObservation(identity,full27.schema,writerToken.bindingUuid,writerToken.writerEpoch,
        writerToken.leaseUntil,state)
      refreshBarrierFacts()
      publish()
      result
    catch
      case cause:Throwable =>
        retainFailure(cause)
        try refreshBarrierFacts()
        catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        publish()
        throw cause
  /** 来自原post-audit receipt的实际窗口；不把SQL writer观察转换为Main Ready。 */
  private[mysql] def currentSqlWriterFreshness():MySqlOwnedAdmissionFreshness =
    requireOpen()
    if primary != null then throw primary
    if state != State.SqlWriterAcquired || refusalTransaction != null || journalTransaction != null || recallTransaction != null || checkpointTransaction != null || initialTransaction != null || preparationTransaction != null || writerCoordinator == null ||
        writerAdmission == null || (writerCoordinator.requireBusinessAdmission(verified,scope) ne writerAdmission) then
      throw new IllegalStateException("original idle schema6 SQL writer admission required")
    val value=writerAdmission.receipt.ownedFreshness(writerRenewSequence)
    if !value.isCurrent then throw new IllegalStateException("original schema6 SQL writer windows expired")
    value

  /** inspection终端image只由真实parent/child/reader登记链发行；不改普通Session的image身份。 */
  private[mysql] def requireInspectionImageIssuer(holder:MySqlDbFirstSnapshotJdbc,
      actualRoot:MySqlFormat3ReadBudget,actualInputs:MySqlDbFirstSessionInputs,
      actualIdentity:MySqlOwnedIoIdentity,actualVerified:VerifiedMySqlConnection,
      actualScope:MySqlAuditBootScope):Unit =
    requireDbFirstObservationReaderSource(actualInputs,actualRoot,actualIdentity)
    require(state==State.Initializing && primary==null && holder!=null && (reader eq holder) &&
      actualVerified!=null && (actualVerified eq verified) && actualScope!=null && (actualScope eq scope),
      "actual registered initializing inspection reader/connection/scope required")

  /** Adapter借用同一issuer；原Session继续承担ledger/close责任，不另发business权限。 */
  private[mysql] def retainedIssuerForOwnershipPublication():MySqlOwnedRetainedPlanIssuer =
    requireOpen()
    require(retainedIssuer != null,"original schema6 capture issuer missing")
    retainedIssuer.requireDbFirstSession(this,full27,full27.sourceLimitProfile,ledger)
    retainedIssuer
  private[mysql] def publishDbFirstRecall(runner:MySqlDbFirstRecallTransaction):Unit =
    requireThread()
    require(runner != null && (recallTransaction eq runner),"original registered recall observation required")
    publish()
  private def requireFailedRecallObservation(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireOpen()
    require(!inspectionOnly && state == State.Failed && primary != null && recallTransaction != null &&
      reservation != null && (recallTransaction.reservation eq reservation) && retainedIssuer != null &&
      writerCoordinator != null,"original failed Session/recall/reservation/issuer/coordinator required")
    identity.requireTag(reservation.tag)
    retainedIssuer.requireFailedDbFirstRecallObservation(this,recallTransaction,full27)
    if !recallSqlResponsibilityReleased then
      writerCoordinator.requireFailedDbFirstRecallObservation(recallTransaction)
    else
      // 已释放的是SQL槽；原Entry/runner/root仍由上方同一issuer核定，不能恢复业务或重执行。
      recallTransaction.requireFailedCommitObservation()
      require(recallTransaction.failedSqlResourcesRetired,"released original SQL slot requires actual resource retirement")

  /** 原Failed Session先保管唯一子Session，再允许borrow/open/Full27；结果仅是诊断，不结算UNKNOWN。 */
  private[mysql] def observeFailedDbFirstRecall(reservation:MySqlDbFirstCaptureReservation):DatabaseObservation =
    observeFailedDbFirstRecall(reservation,false)

  /** 主动关闭原失败连接后，仅在同namespace独占barrier交接成功之后读取；不退休原SQL/Entry或确认业务。 */
  private[mysql] def observeFailedDbFirstRecallAfterBarrierHandoff(
      reservation:MySqlDbFirstCaptureReservation):DatabaseObservation =
    observeFailedDbFirstRecall(reservation,true)

  /** 原commit失败边界的单次只读推进；before仍为UNKNOWN，正向DATA也不发行成功。 */
  private[mysql] def observeFailedRecallCommitOnce(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallObservation(reservation)
    observeFailedDbFirstRecallAfterBarrierHandoff(reservation)
    compareFailedRecallImageOnOwnerThread(reservation) match
      case DbFirstFailedRecallImage.BeforeObservedStillUnknown=>inspectFailedRecallBefore(reservation)
      case _=>prepareFailedRecallMainRead(reservation)

  /** 原before分支核实际租约和独立审计后，只封存原次SQL未应用证明，不结算请求或退休Entry。 */
  private def inspectFailedRecallBefore(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallBefore(reservation)
    require(writerToken!=null && !recallBeforeLeaseObservationAttempted && recallBeforeObservedLease.isEmpty &&
      !recallBeforeAuditAttempted && !recallBeforeAuditCompleted,
      "original before observation lease/audit is one-shot")
    recallBeforeLeaseObservationAttempted=true
    publish()
    try
      require(writerToken.bootUuid==identity.bootId && writerToken.bindingUuid==recallTransaction.request.bindingUuid,
        "before observation lost original writer identity")
      recallBeforeObservedLease=Some(recallInspection.reader.observedRecallLease(writerToken.bindingUuid,
        writerToken.bootUuid,writerToken.writerEpoch,writerToken.leaseUntil))
      requireFailedRecallBefore(reservation)
      recallBeforeAuditAttempted=true
      publish()
      recallInspection.auditOriginalFailedRecallBefore(this)
      requireFailedRecallBefore(reservation)
      recallInspection.requireCurrentReadbackQualification(this)
      require(recallBeforeObservedLease.exists(_.remainingAt(System.nanoTime())>0L),
        "original before observation lease expired during independent audit")
      recallBeforeAuditCompleted=true
      qualifyFailedRecallNotApplied(reservation)
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 终止后的排他交接和不可回退的完整before共同证明未应用；absence/分类/审计旗标均不能单独发证。 */
  private def requireCurrentFailedRecallNotApplied(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallBefore(reservation)
    require(recallBeforeLeaseObservationAttempted && recallBeforeAuditAttempted && recallBeforeAuditCompleted &&
      writerToken!=null && writerToken.bootUuid==identity.bootId &&
      writerToken.bindingUuid==recallTransaction.request.bindingUuid,
      "original completed before inspection and actual writer identity required")
    recallInspection.requireCurrentReadbackQualification(this)
    require(recallBeforeObservedLease.exists(_.remainingAt(System.nanoTime())>0L),
      "original not-applied observation lease expired")

  private def qualifyFailedRecallNotApplied(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireCurrentFailedRecallNotApplied(reservation)
    require(!recallNotAppliedQualificationAttempted && recallNotAppliedQualification==null &&
      recallNotAppliedQualificationFee==0L,"original not-applied qualification is one-shot")
    recallNotAppliedQualificationAttempted=true
    root.reserveRetainedMemory(FailedRecallNotAppliedQualification.HolderBytes,
      "original failed recall not-applied qualification BEFORE allocation")
    recallNotAppliedQualificationFee=FailedRecallNotAppliedQualification.HolderBytes
    // 原Session先保管；分配或后续核验失败保留原cause、partial holder和预收费，不自动退款重试。
    recallNotAppliedQualification=new FailedRecallNotAppliedQualification(this,recallTransaction,reservation,
      recallBeforeObservedLease.get,primary)
    requireFailedRecallNotAppliedQualification(recallTransaction,recallNotAppliedQualification,primary)

  private[mysql] def requireFailedRecallNotAppliedQualification(runner:MySqlDbFirstRecallTransaction,
      actual:FailedRecallNotAppliedQualification,cause:Throwable):Unit =
    requireThread()
    require(runner!=null && (runner eq recallTransaction) && actual!=null &&
      (actual eq recallNotAppliedQualification) && recallNotAppliedQualificationAttempted &&
      recallNotAppliedQualificationFee==FailedRecallNotAppliedQualification.HolderBytes &&
      cause!=null && (cause eq primary),"actual original live not-applied qualification/cause required")
    requireCurrentFailedRecallNotApplied(runner.reservation)
    actual.requireOriginal(this,runner,runner.reservation,cause)

  private def requireFailedRecallBefore(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallObservation(reservation)
    require(recallImageObservation.contains(DbFirstFailedRecallImage.BeforeObservedStillUnknown) &&
      recallPreparedReadback.isEmpty && pendingMainRead==null && !recallSqlResponsibilityReleased &&
      !recallEntrySqlSettlementRecorded && recallCleanupQualification==null,
      "original unresolved before image cannot borrow positive settlement or Main DATA")
    requireFailedRecallGateRetirement(recallTransaction)
    require(recallTransaction.compareFailedCommitObservation(recallInspection.snapshotOnOwnerThread,root)==
      DbFirstFailedRecallImage.BeforeObservedStillUnknown,
      "actual retained Full27 no longer matches original complete before image")

  private def observeFailedDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,
      handoff:Boolean):DatabaseObservation =
    requireFailedRecallObservation(reservation)
    require(!recallInspectionAttempted && recallInspection == null,"original recall observation is one-shot; no automatic retry")
    if handoff then
      require(verified!=null && scope!=null && barrier!=null && !jdbcCloseAttempted,
        "original failed connection and held barrier required before one-shot handoff")
    recallInspectionAttempted=true
    publish()
    try
      if handoff then
        // 不rollback、不重发commit、不释放named lock；远端终止只能由后续独占取得同namespace佐证。
        jdbcCloseAttempted=true
        publish()
        try
          verified.close()
          jdbcCloseReturned=true
          jdbcClosedObserved=verified.connection.isClosed
          if !jdbcClosedObserved then throw new SQLNonTransientConnectionException("original failed recall connection is not locally closed")
        finally
          refreshBarrierFacts()
          publish()
      root.reserveRetainedMemory(HolderBytes,"failed recall observation Session BEFORE allocation")
      recallInspectionAllocationFee=HolderBytes
      recallInspection=new MySqlDbFirstSession(inputs,root,identity,thread,auditMaxAge,full27,ledger,HolderBytes,true,this)
      recallInspectionAllocationFee=0L // 实际child接管同一预收费；不重复计费或退款。
      publish() // 子holder先可达；后续borrow/open/审计/reader失败均由原Session保管。
      recallInspection.inputBorrow=inputs.borrowForReader()
      recallInspection.publish()
      // ownsInputs保持false；不调用allocateRetainedOwner，不创建mutation issuer。
      recallInspection.initializeForObservation(if handoff then barrier else null)
      requireFailedRecallObservation(reservation)
      if handoff then recallTransaction.retireFailedSqlGate(this)
      recallInspection.observationOnOwnerThread
    catch case cause:Throwable =>
      if recallInspection == null && recallInspectionAllocationFee != 0L then
        try
          root.releaseRetainedMemory(recallInspectionAllocationFee,"unallocated failed recall observation Session")
          recallInspectionAllocationFee=0L
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if recallInspection != null then recallInspection.retainFailure(cause)
      retainFailure(cause) // 原primary/cause不被观测失败覆盖。
      throw cause
    finally publish()

  /** 只供已登记的原runner清理本地gate；必须有实际closed原连接和其后新取得barrier的完整子观测。 */
  private[mysql] def requireFailedRecallGateRetirement(
      runner:MySqlDbFirstSnapshotTransaction[?,?]):Connection =
    require(runner!=null && (recallTransaction eq runner),"original failed recall runner required")
    requireFailedRecallObservation(recallTransaction.reservation)
    require(jdbcCloseAttempted && jdbcCloseReturned && jdbcClosedObserved && verified!=null &&
      verified.connection.isClosed && barrier!=null && recallInspection!=null,
      "actual locally closed original recall connection and retained observation required")
    val child=recallInspection
    child.snapshotOnOwnerThread // 必须已在独占barrier之后完成新的完整RR读取，失败/普通诊断child不合格。
    require(child.inspectionOnly && child.barrier!=null && child.barrierAttempted &&
      child.scopeConnectionGeneration>scopeConnectionGeneration &&
      child.barrier.actualConnectionId!=barrier.actualConnectionId &&
      child.barrier.connectionGenerationId!=barrier.connectionGenerationId,
      "actual distinct barrier-handoff observation required before original gate retirement")
    MySqlSessionBarrierTargetRules.requireSame(child.barrier.namespace,barrier.namespace)
    child.verified.readmitSessionBarrier(inputs.auditExpectation,child.scope)
    verified.connection

  /** 正向完整receipt DATA复制成功后退休原SQL责任；仍保持Failed，不发completion/confirmation。 */
  private[mysql] def prepareFailedRecallMainRead(reservation:MySqlDbFirstCaptureReservation):FailedRecallReadback =
    requireFailedRecallObservation(reservation)
    require(mainRead!=null && pendingMainRead==null && pendingMainReadFee==0L &&
      recallPreparedReadback.isEmpty && recallAcceptedEpoch.nonEmpty,
      "original current Main predecessor and original accepted epoch required")
    try
      val source=failedRecallSnapshotOnOwnerThread(reservation)
      val observed=recallTransaction.prepareFailedRecallReadback(this,source)
      root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"failed recall unclaimed Main successor BEFORE allocation")
      pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
      pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,recallAcceptedEpoch.get)
      pendingMainRead.assemble(source,full27,root)
      pendingMainRead.requireUnpublishedData(this,observed.canonicalSha256)
      recallPreparedReadback=Some(observed)
      // observation仍不登记为普通completion；未闭合原confirmation/ACK时Main无法claim。
      recallTransaction.retireFailedRecallReaders(this,observed)
      retireFailedRecallSql(reservation,observed)
      retainedIssuer.settleFailedDbFirstRecallReadback(this,recallTransaction,observed,full27)
      recallEntrySqlSettlementRecorded=true
      observeFailedRecallLease(reservation)
      auditFailedRecallObservation(reservation)
      observed
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  private[mysql] def requirePreparedFailedRecallReadback(runner:MySqlDbFirstSnapshotTransaction[?,?],
      observed:FailedRecallReadback):Unit =
    requireFailedRecallGateRetirement(runner)
    require(observed!=null && recallPreparedReadback.exists(_ eq observed) && pendingMainRead!=null &&
      pendingMainReadObservation==null,"original prepared unissued readback DATA reference required")
    pendingMainRead.requireUnpublishedData(this,observed.canonicalSha256)

  /** 原Entry正向SQL读回之后才观察原W的有效期；窗口不是副作用或新writer资格。 */
  private[mysql] def observeFailedRecallLease(reservation:MySqlDbFirstCaptureReservation):MySqlAdmissionWindow =
    requireFailedRecallObservation(reservation)
    require(recallEntrySqlSettlementRecorded && recallSqlResponsibilityReleased && !recallLeaseObservationAttempted && recallObservedLease.isEmpty &&
      writerToken!=null,"original settled failed Entry and one-shot lease observation required")
    recallLeaseObservationAttempted=true
    publish()
    val observed=recallPreparedReadback.getOrElse(throw new IllegalStateException("original positive SQL readback missing"))
    requirePreparedFailedRecallReadback(recallTransaction,observed)
    require(writerToken.bootUuid==identity.bootId && writerToken.bindingUuid==recallTransaction.request.bindingUuid,
      "original failed writer scalar identity differs")
    val window=recallInspection.reader.observedRecallLease(writerToken.bindingUuid,writerToken.bootUuid,
      writerToken.writerEpoch,writerToken.leaseUntil)
    recallObservedLease=Some(window)
    publish()
    window

  /** 正向读回与独立Main DATA齐备后结算原SQL责任；仍为Failed，不签普通completion或confirmation。 */
  private[mysql] def auditFailedRecallObservation(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallObservation(reservation)
    require(recallEntrySqlSettlementRecorded && recallObservedLease.exists(_.remainingAt(System.nanoTime())>0L) &&
      !recallObservationAuditAttempted && !recallObservationAuditCompleted,
      "original positive Entry/current observed lease and one-shot readback audit required")
    recallObservationAuditAttempted=true
    publish()
    val observed=recallPreparedReadback.getOrElse(throw new IllegalStateException("original positive readback missing"))
    try
      requirePreparedFailedRecallReadback(recallTransaction,observed)
      recallInspection.auditOriginalFailedRecall(this,observed)
      requirePreparedFailedRecallReadback(recallTransaction,observed)
      recallInspection.requireCurrentReadbackQualification(this)
      require(recallObservedLease.exists(_.remainingAt(System.nanoTime())>0L),"original observed lease expired during readback audit")
      recallObservationAuditCompleted=true
      qualifyFailedRecallCleanup(reservation)
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 只封存原操作的当前只读资格；仍不是Main confirmation、native清理或普通准入。 */
  private[mysql] def qualifyFailedRecallCleanup(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallObservation(reservation)
    require(recallObservationAuditCompleted && !recallCleanupQualificationAttempted && recallCleanupQualification==null &&
      recallCleanupQualificationFee==0L,
      "completed actual readback audit and unused original qualification slot required")
    recallCleanupQualificationAttempted=true
    val observed=recallPreparedReadback.getOrElse(throw new IllegalStateException("original positive readback missing"))
    requireCurrentFailedRecallCleanup(observed)
    root.reserveRetainedMemory(FailedRecallCleanupQualification.HolderBytes,
      "original failed recall bounded cleanup qualification BEFORE allocation")
    recallCleanupQualificationFee=FailedRecallCleanupQualification.HolderBytes
    // 原Session先保管；Entry登记失败也不丢失partial holder或退款。
    recallCleanupQualification=new FailedRecallCleanupQualification(this,recallTransaction,reservation,observed,
      recallObservedLease.get,primary)
    retainedIssuer.recordFailedDbFirstRecallQualification(this,recallTransaction,recallCleanupQualification,full27)

  private def requireCurrentFailedRecallCleanup(observed:FailedRecallReadback):Unit =
    requireFailedRecallObservation(recallTransaction.reservation)
    require(recallObservationAuditCompleted && recallEntrySqlSettlementRecorded,
      "original Entry settlement and completed readback audit required")
    requirePreparedFailedRecallReadback(recallTransaction,observed)
    recallTransaction.requireFailedRecallSqlSettlement(observed,primary)
    recallInspection.requireCurrentReadbackQualification(this)
    require(recallObservedLease.exists(_.remainingAt(System.nanoTime())>0L),
      "original observed lease expired before restricted cleanup qualification")

  private[mysql] def requireFailedRecallCleanupQualification(runner:MySqlDbFirstRecallTransaction,
      qualification:FailedRecallCleanupQualification):Unit =
    requireThread()
    require(runner!=null && (runner eq recallTransaction) && qualification!=null &&
      (qualification eq recallCleanupQualification),"actual original Session qualification holder required")
    requireCurrentFailedRecallCleanup(qualification.readback)
    qualification.requireOriginal(runner,recallPreparedReadback.get,primary)

  /** 仅同一execute失败边界取得原Entry已登记的实际资格；失败仍是原cause。 */
  private[mysql] def retainedRecallQualification(reservation:MySqlDbFirstCaptureReservation,
      cause:Throwable):FailedRecallCleanupQualification =
    requireFailedRecallObservation(reservation)
    require(cause!=null && (cause eq primary),"exact original recall failure required")
    requireFailedRecallCleanupQualification(recallTransaction,recallCleanupQualification)
    retainedIssuer.requireRecordedFailedRecallQualification(recallTransaction,recallCleanupQualification)
    recallCleanupQualification

  /** 原窗口内只维持保管检查，不续租writer或发布普通freshness。 */
  private[mysql] def maintainRetainedRecall(qualification:FailedRecallCleanupQualification):IoMaintenanceDeadline =
    requireFailedRecallCleanupQualification(recallTransaction,qualification)
    retainedIssuer.requireRecordedFailedRecallQualification(recallTransaction,qualification)
    val now=System.nanoTime()
    val remaining=qualification.remainingAt(now)
    require(remaining>0L,"original restricted recall window expired")
    IoMaintenanceDeadline(now,Duration.ofNanos(Math.min(remaining,Duration.ofSeconds(1).toNanos)))

  /** 新连接上的完整只读审计；只由原Failed Session持有的实际child执行，不授予writer。 */
  private def auditOriginalFailedRecall(parent:MySqlDbFirstSession,observed:FailedRecallReadback):Unit =
    requireOpen()
    require(inspectionOnly && parent!=null && (parent.recallInspection eq this) &&
      (parent.inputs eq inputs) && (parent.root eq root) && (parent.identity eq identity) &&
      parent.recallObservationAuditAttempted && parent.recallEntrySqlSettlementRecorded &&
      recallReadbackAudit==null && recallReadbackQualification==null,
      "exact original failed recall observation child required")
    parent.requirePreparedFailedRecallReadback(parent.recallTransaction,observed)
    try
      recallReadbackAudit=auditor.beginDbFirstAudit(inputs,root,verified,scope,this)
      publish() // 原child在任何CALL前持有partial holder和完整审计图。
      recallReadbackQualification=recallReadbackAudit.readAndQualifyOnce()
      requireCurrentReadbackQualification(parent)
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 与正向审计共用原child的partial holder保管；不借用正向Entry settlement前置或授予写资格。 */
  private def auditOriginalFailedRecallBefore(parent:MySqlDbFirstSession):Unit =
    requireOpen()
    require(inspectionOnly && parent!=null && (parent.recallInspection eq this) &&
      (parent.inputs eq inputs) && (parent.root eq root) && (parent.identity eq identity) &&
      parent.recallBeforeAuditAttempted && !parent.recallBeforeAuditCompleted &&
      parent.recallBeforeObservedLease.exists(_.remainingAt(System.nanoTime())>0L) &&
      recallReadbackAudit==null && recallReadbackQualification==null,
      "exact original before observation child and live lease required")
    parent.requireFailedRecallBefore(parent.recallTransaction.reservation)
    try
      recallReadbackAudit=auditor.beginDbFirstAudit(inputs,root,verified,scope,this)
      publish() // CALL前保原partial holder；失败不清ref或退款假成功。
      recallReadbackQualification=recallReadbackAudit.readAndQualifyOnce()
      requireCurrentReadbackQualification(parent)
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  private def requireCurrentReadbackQualification(parent:MySqlDbFirstSession):Unit =
    requireOpen()
    require(inspectionOnly && parent!=null && (parent.recallInspection eq this) &&
      parent.writerToken!=null && recallReadbackAudit!=null && recallReadbackQualification!=null,
      "actual retained original readback audit qualification required")
    val current=recallReadbackQualification
    val expected=inputs.auditExpectation
    require(current.writerOwnerBootObservedBeforeAcquire.contains(parent.writerToken.bootUuid) &&
      current.writerEpochObservedBeforeAcquire==parent.writerToken.writerEpoch &&
      current.isCurrent(identity.bootId,identity.bootGeneration,scopeConnectionGeneration,
        expected.serverUuid,expected.schema,expected.bindingUuid,MySqlSchemaAuditProfile.DbFirst1,
        MySqlExpectedPhysicalFormat.Format2,verified,thread,System.nanoTime()),
      "fresh actual readback qualification lost original writer/security scope")

  /** 正向读回与独立Main DATA齐备后结算原SQL责任；仍为Failed，不签普通completion或confirmation。 */
  private[mysql] def requireFailedRecallSqlSettlement(runner:MySqlDbFirstRecallTransaction,
      observed:FailedRecallReadback):Unit =
    requirePreparedFailedRecallReadback(runner,observed)
    require(recallSqlRetirementAttempted && recallSqlResponsibilityReleased &&
      runner.failedSqlResourcesRetired,"actual original SQL responsibility must be released before Entry settlement")

  /** 正向读回与独立Main DATA齐备后结算原SQL责任；仍为Failed，不签普通completion或confirmation。 */
  private[mysql] def retireFailedRecallSql(reservation:MySqlDbFirstCaptureReservation,
      observed:FailedRecallReadback):Unit =
    requireFailedRecallObservation(reservation)
    require(!recallSqlRetirementAttempted && !recallSqlResponsibilityReleased,
      "original failed recall SQL settlement is one-shot; no automatic retry")
    recallSqlRetirementAttempted=true
    try
      recallTransaction.retireFailedSqlResources(this,observed)
      writerCoordinator.finishFailedDbFirstRecallSql(recallTransaction)
      recallSqlResponsibilityReleased=true
      // 不清recallTransaction/primary，不改pendingMainReadObservation，不恢复SqlWriterAcquired。
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  private[mysql] def detachPreparedFailedRecallDraft(runner:MySqlDbFirstSnapshotTransaction[?,?],
      observed:FailedRecallReadback):Unit =
    requirePreparedFailedRecallReadback(runner,observed)
    // barrier/read核验在issuer/core monitor之外完成；monitor内只撤确切原Entry的借用图。
    retainedIssuer.detachFailedDbFirstRecallDraft(this,recallTransaction,observed,full27)

  /** 只借原子holder仍持有的完整image；latest W/receipt/provenance/原ACK桥另行核定。 */
  private[mysql] def failedRecallSnapshotOnOwnerThread(reservation:MySqlDbFirstCaptureReservation):MySqlScriptSchemaSnapshot =
    requireFailedRecallObservation(reservation)
    require(recallInspection != null,"original failed recall observation was not allocated")
    recallInspection.snapshotOnOwnerThread

  /** 仅比对原观测时刻的完整DATA；不清原失败、不退休SQL、不恢复writer或发行Main ACK。 */
  private[mysql] def compareFailedRecallImageOnOwnerThread(reservation:MySqlDbFirstCaptureReservation):DbFirstFailedRecallImage =
    val image=failedRecallSnapshotOnOwnerThread(reservation)
    try
      val observed=recallTransaction.compareFailedCommitObservation(image,root)
      recallImageObservation=Some(observed)
      observed
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 完整同epoch snapshot receipt区段仍仅返回DATA步数；不记录settled或生成任何ACK。 */
  private[mysql] def verifyFailedRecallSnapshotHistoryOnOwnerThread(reservation:MySqlDbFirstCaptureReservation):Long =
    val image=failedRecallSnapshotOnOwnerThread(reservation)
    try recallTransaction.verifyFailedSnapshotReceiptSegment(image,root)
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  private[mysql] def closeFailedRecallObservationOnOwnerThread(reservation:MySqlDbFirstCaptureReservation):Unit =
    requireFailedRecallObservation(reservation)
    require(recallInspection != null,"original failed recall observation was not allocated")
    if recallCleanupQualification!=null then recallCleanupQualification.revoke()
    if recallNotAppliedQualification!=null then recallNotAppliedQualification.revoke()
    try recallInspection.closeOnOwnerThread()
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally
      detachClosedRecallInspection()
      publish()

  private def detachClosedRecallInspection():Unit =
    if recallInspection != null && recallInspection.resourceObservation.closed then
      closedRecallInspection=Some(recallInspection.resourceObservation)
      recallInspection=null // 不在退款后继续保有已关child的inputs/root/DATA引用。
  private[mysql] def publishDbFirstRecallJournal(runner:MySqlDbFirstRecallJournalTransaction):Unit =
    requireThread()
    require(runner!=null && (journalTransaction eq runner),"original registered journal observation required")
    publish()
  private[mysql] def publishDbFirstRecallRefusal(runner:MySqlDbFirstRecallRefusalTransaction):Unit =
    requireThread()
    require(runner!=null && (refusalTransaction eq runner),"original registered refusal observation required")
    publish()
  /** 只执行已确认J/封锁C的原拒绝次；读回不发布终局反馈，保SQL/Entry待专用交接。 */
  private[mysql] def executeDbFirstRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource,acceptedEpoch:Long):DbFirstRecallRefusalObservation =
    requireOpen()
    require(state==State.SqlWriterAcquired && writerCoordinator!=null && writerAdmission!=null &&
      refusalTransaction==null && journalTransaction==null && recallTransaction==null && checkpointTransaction==null &&
      initialTransaction==null && preparationTransaction==null && retainedIssuer!=null &&
      pendingMainRead==null && pendingMainReadFee==0L,"original idle schema6 refusal slot required")
    require(exclusion!=null && acceptedEpoch>=0L,"original C exclusion and actual accepted epoch required")
    identity.requireTag(exclusion.executionTag);identity.requireTag(exclusion.routeTag)
    var work:MySqlDbFirstRecallRefusalWork=null
    try
      work=retainedIssuer.beginDbFirstRecallRefusal(this,exclusion,full27)
      refusalTransaction=work.beginTransaction()
      state=State.DbFirstRecallRefusalPending
      writerCoordinator.beginDbFirstRecallRefusal(refusalTransaction,writerAdmission,verified,scope)
      publish()
      val result=refusalTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      if mainRead!=null then
        val source=refusalTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"refusal pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        // completion/读代control沿原J槽；实际持久operation仍为result.tag(C)。
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,exclusion.routeTag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      refusalTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstRecallRefusalSql(refusalTransaction)
      refusalTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForRefusalCompletion(this,mainRead,result)
      // 只释放已读回并复制独立DATA的SQL图；原J/C Entry/root仍存活，未发终局反馈。
      publish()
      result
    catch case cause:Throwable=>
      retainFailure(cause)
      if work!=null then
        try exclusion.core.failRecallRefusal(work,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  /** 实际journal-only入口；完整读回后仍保原Entry/SQL槽，待专用Main交接与退休。 */
  private[mysql] def executeDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation,acceptedEpoch:Long):DbFirstRecallJournalObservation =
    requireOpen()
    require(state==State.SqlWriterAcquired && writerCoordinator!=null && writerAdmission!=null &&
      refusalTransaction==null && journalTransaction==null && recallTransaction==null && checkpointTransaction==null && initialTransaction==null &&
      preparationTransaction==null && retainedIssuer!=null && pendingMainRead==null && pendingMainReadFee==0L,
      "original idle schema6 journal slot required")
    require(reservation!=null && acceptedEpoch>=0L,"original journal reservation and accepted epoch required")
    identity.requireTag(reservation.tag);identity.requireTag(reservation.originalTag)
    var work:MySqlDbFirstRecallJournalWork=null
    try
      work=retainedIssuer.beginDbFirstRecallJournal(this,reservation,full27)
      journalTransaction=work.beginTransaction()
      state=State.DbFirstRecallJournalPending
      writerCoordinator.beginDbFirstRecallJournal(journalTransaction,writerAdmission,verified,scope)
      publish()
      val result=journalTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      if mainRead!=null then
        val source=journalTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"journal pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      journalTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstRecallJournalSql(journalTransaction)
      journalTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForJournalCompletion(this,mainRead,result)
      publish()
      result
    catch case cause:Throwable=>
      retainFailure(cause)
      if work!=null then
        try reservation.core.failDbFirstRecallJournal(work,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  /** 原reservation-only worker入口；ACK/readback只报告数据库事实，Entry/freeze仍保留。 */
  def executeDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,acceptedEpoch:Long):DbFirstRecallObservation =
    requireOpen()
    if state != State.SqlWriterAcquired || writerCoordinator == null || writerAdmission == null ||
        refusalTransaction != null || journalTransaction != null || recallTransaction != null || checkpointTransaction != null || initialTransaction != null || preparationTransaction != null || retainedIssuer == null || pendingMainRead!=null || pendingMainReadFee!=0L then
      throw new IllegalStateException("schema6 recall requires original current SQL writer and unused transaction slot",primary)
    require(reservation != null && acceptedEpoch>=0L,"original capture reservation and accepted epoch required")
    identity.requireTag(reservation.tag)
    // 原Entry先登记runner；从此任何失败均不能丢失capture、operation或部分child。
    try
      recallAcceptedEpoch=Some(acceptedEpoch)
      recallTransaction=retainedIssuer.beginDbFirstRecall(this,reservation,full27)
      state=State.DbFirstRecallPending
      writerCoordinator.beginDbFirstRecall(recallTransaction,writerAdmission,verified,scope)
      publish()
      val result=recallTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      // 先登记原Session唯一pending DATA槽；reader关闭后不能再借其snapshot补造successor。
      if mainRead!=null then
        val source=recallTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"schema6 pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      // 在Main收到确认前，真实关闭本操作SQL图；pending DATA不发行成功或Main资格。
      retainedIssuer.detachConfirmedDbFirstSnapshotDraft(this,recallTransaction,full27)
      recallTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstSql(recallTransaction)
      recallTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForRecallCompletion(this,mainRead,result)
      publish()
      result
    catch case cause:Throwable =>
      retainFailure(cause)
      try
        if recallTransaction != null then retainedIssuer.failDbFirstRecall(recallTransaction,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      // 原Entry/cause先登记；只观察已尝试且读回未核定的commit，不重试mutation或普通成功清理。
      if NonFatal(cause) && recallTransaction!=null && recallTransaction.failedCommitObservationPending &&
          !recallInspectionAttempted then
        observeRetainingRecallFailure(cause)(observeFailedRecallCommitOnce(reservation))
      publish()
      throw cause

  private[mysql] def publishDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction):Unit =
    requireThread()
    require(runner != null && (checkpointTransaction eq runner),"original registered checkpoint observation required")
    publish()
  /** 原reservation-only worker入口；ACK/readback只报告数据库事实，Entry/freeze仍保留。 */
  def executeDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation,acceptedEpoch:Long):DbFirstCheckpointObservation =
    requireOpen()
    if state != State.SqlWriterAcquired || writerCoordinator == null || writerAdmission == null ||
        refusalTransaction != null || journalTransaction != null || checkpointTransaction != null || recallTransaction != null || initialTransaction != null || preparationTransaction != null || retainedIssuer == null || pendingMainRead!=null || pendingMainReadFee!=0L then
      throw new IllegalStateException("schema6 checkpoint requires original current SQL writer and unused transaction slot",primary)
    require(reservation != null && acceptedEpoch>=0L,"original capture reservation and accepted epoch required")
    identity.requireTag(reservation.tag)
    // 原Entry先登记runner；从此任何失败均不能丢失capture、operation或部分child。
    try
      checkpointTransaction=retainedIssuer.beginDbFirstCheckpoint(this,reservation,full27)
      state=State.DbFirstCheckpointPending
      writerCoordinator.beginDbFirstCheckpoint(checkpointTransaction,writerAdmission,verified,scope)
      publish()
      val result=checkpointTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      // 先登记原Session唯一pending DATA槽；reader关闭后不能再借其snapshot补造successor。
      if mainRead!=null then
        val source=checkpointTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"schema6 pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      // 在Main收到确认前，真实关闭本操作SQL图；pending DATA不发行成功或Main资格。
      retainedIssuer.detachConfirmedDbFirstSnapshotDraft(this,checkpointTransaction,full27)
      checkpointTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstCheckpointSql(checkpointTransaction)
      checkpointTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForCheckpointCompletion(this,mainRead,result)
      publish()
      result
    catch case cause:Throwable =>
      retainFailure(cause)
      try
        if checkpointTransaction != null then retainedIssuer.failDbFirstCheckpoint(checkpointTransaction,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  private[mysql] def publishDbFirstInitialPreparation(runner:MySqlDbFirstInitialPreparationTransaction):Unit =
    requireThread()
    require(runner != null && (preparationTransaction eq runner),"original registered preparation transaction required")
    publish()
  /** 仅数据库事实；原Work/Entry继续保留，SQL资源确认退休后才恢复共享writer；没有实体放置或最终Entry退休资格。 */
  private[mysql] def executeDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation,acceptedEpoch:Long):DbFirstInitialPreparationObservation | DbFirstInitialPreparationRejection =
    requireOpen()
    require(state==State.SqlWriterAcquired && writerCoordinator!=null && writerAdmission!=null &&
      refusalTransaction==null && journalTransaction==null && recallTransaction==null && checkpointTransaction==null && initialTransaction==null && preparationTransaction==null && retainedIssuer!=null &&
      pendingMainRead==null && pendingMainReadFee==0L,"original idle schema6 preparation slot required")
    require(reservation!=null && acceptedEpoch>=0L,"original preparation reservation and accepted epoch required")
    identity.requireTag(reservation.tag)
    var work:MySqlDbFirstInitialPreparationWork=null
    try
      work=retainedIssuer.beginDbFirstInitialPreparation(this,reservation,full27)
      preparationTransaction=work.beginTransaction()
      state=State.DbFirstInitialPreparationPending
      writerCoordinator.beginDbFirstInitialPreparation(preparationTransaction,writerAdmission,verified,scope)
      publish()
      val result=preparationTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      if mainRead!=null then
        val source=preparationTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"preparation pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      preparationTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstPreparationSql(preparationTransaction)
      preparationTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForInitialPreparationCompletion(this,mainRead,result)
      publish()
      result
    catch case cause:Throwable=>
      cause match
        case rejected:MySqlDbFirstPreparationRejected if preparationTransaction!=null && rejected.getSuppressed.isEmpty=>
          try
            val source=preparationTransaction.rejectedMainReadSource(rejected)
            // 在关闭原reader前保存完整独立DATA；分配/装配失败保留原pending引用及cause。
            work.root.reserveRetainedMemory(256L,"rejected preparation observation BEFORE allocation")
            val observation=DbFirstInitialPreparationRejection(reservation.tag,reservation.request.podUuid,
              rejected,source.canonicalSha256Hex)
            if mainRead!=null then
              require(pendingMainRead==null && pendingMainReadFee==0L,"original rejected successor slot required")
              root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"rejected preparation successor BEFORE allocation")
              pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
              pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
              pendingMainRead.assemble(source,full27,root)
              pendingMainReadObservation=observation
            preparationTransaction.retireRejectedResources(rejected)
            if pendingMainRead!=null then pendingMainRead.readyForRejectedPreparation(this,mainRead,observation)
            writerCoordinator.finishRejectedDbFirstPreparationSql(preparationTransaction,rejected,writerAdmission,verified,scope)
            preparationTransaction.recordSettledRejection(observation)
            settledPreparationRejection=observation
          catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        case _=>()
      // DATA与SQL清理不等于Main交接完成；局部reply/router接齐前仍保原Session hold。
      retainFailure(cause)
      try if work!=null then retainedIssuer.failDbFirstInitialPreparation(work,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      if settledPreparationRejection!=null && (settledPreparationRejection.cause eq cause) && cause.getSuppressed.isEmpty then
        settledPreparationRejection
      else throw cause

  private[mysql] def publishDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction):Unit =
    requireThread()
    require(runner!=null && (initialTransaction eq runner),"original registered initial transaction required")
    publish()

  /** 完整读回先装配pending DATA并退休SQL图；原Entry仍等待Main活动发布，不发行业务成功。 */
  private[mysql] def executeDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,
      activeLimit:Int,acceptedEpoch:Long):DbFirstInitialActivationObservation =
    requireOpen()
    require(state==State.SqlWriterAcquired && writerCoordinator!=null && writerAdmission!=null &&
      refusalTransaction==null && journalTransaction==null && recallTransaction==null && checkpointTransaction==null && initialTransaction==null && preparationTransaction==null && retainedIssuer!=null &&
      pendingMainRead==null && pendingMainReadFee==0L,"initial transaction requires original idle SQL writer slot")
    require(reservation!=null && activeLimit>=1 && activeLimit<=256 && acceptedEpoch>=0L,"original initial reservation, configured limit and accepted epoch required")
    identity.requireTag(reservation.tag)
    try
      initialTransaction=retainedIssuer.beginDbFirstInitialActivation(this,reservation,full27,activeLimit)
      state=State.DbFirstInitialActivationPending
      writerCoordinator.beginDbFirstInitialActivation(initialTransaction,writerAdmission,verified,scope)
      publish()
      val result=initialTransaction.execute(this,inputs,root,identity,verified,scope,writerCoordinator,
        writerAdmission,barrier,retainedIssuer,auditor)
      if mainRead!=null then
        val source=initialTransaction.confirmedMainReadSource(result)
        root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"initial pending Main successor BEFORE allocation")
        pendingMainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
        pendingMainRead=mainRead.newUnclaimedSuccessor(this,reservation.tag,acceptedEpoch)
        pendingMainRead.assemble(source,full27,root)
        pendingMainReadObservation=result
      retainedIssuer.detachConfirmedDbFirstInitialDraft(this,initialTransaction,full27)
      initialTransaction.retireConfirmedResources()
      writerCoordinator.finishConfirmedDbFirstInitialSql(initialTransaction)
      initialTransaction=null
      state=State.SqlWriterAcquired
      if pendingMainRead!=null then pendingMainRead.readyForInitialActivationCompletion(this,mainRead,result)
      publish()
      result
    catch case cause:Throwable=>
      retainFailure(cause)
      try
        if initialTransaction!=null then retainedIssuer.failDbFirstInitialActivation(initialTransaction,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstRecallRefusalObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && refusalTransaction==null,"actual refusal SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original refusal pending observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstRecallJournalObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && refusalTransaction==null && journalTransaction==null,"actual journal SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original journal pending observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstRecallObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && recallTransaction==null,"original SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original pending completion observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstCheckpointObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && checkpointTransaction==null,"original SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original pending completion observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstInitialPreparationRejection):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(observation!=null && (settledPreparationRejection eq observation) && state==State.Failed &&
      (primary eq observation.cause) && preparationTransaction!=null,"original settled rejection required")
    preparationTransaction.requireRejectedResourcesRetired(observation.cause)
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original rejected pending observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstInitialPreparationObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && preparationTransaction==null,"original SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original pending completion observation required")
      Some(pendingMainRead)

  private[mysql] def pendingMainReadForCompletion(observation:DbFirstInitialActivationObservation):Option[MySqlDbFirstMainReadBacking] =
    requireOpen()
    require(state==State.SqlWriterAcquired && initialTransaction==null,"original SQL retirement required before pending publication")
    if pendingMainRead==null then None
    else
      require(pendingMainReadObservation eq observation,"original pending completion observation required")
      Some(pendingMainRead)

  /** 原失败路径的worker预检；通过不表示Main holder已撤除或允许关闭任何root。 */
  private[mysql] def requireRetainedRecallRetirement(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireThread()
    require(receipt!=null && receipt.isRetained && recallTransaction!=null &&
      recallCleanupQualification!=null &&
      receipt.retainedConfirmation.retainedQualification==null,
      "actual original retained recall Session and native drop required")
    // 内含实际barrier/audit与未发布DATA核对，必须在issuer/core monitor之外执行。
    requireFailedRecallCleanupQualification(recallTransaction,recallCleanupQualification)
    val runner=retainedIssuer.requireRetainedDbFirstDrop(this,receipt,recallCleanupQualification,full27)
    require(runner eq recallTransaction,"retained retirement cannot replace original Session runner")

  /** 只由原worker执行；这个Unit frame返回前不退款或关Entry root。 */
  private[mysql] def detachRetainedRecallEntryGraph(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireRetainedRecallRetirement(receipt)
    require(retainedRecallRetirementReceipt==null && !retainedRecallPayloadDetached,
      "original retained graph detach is one-shot")
    retainedIssuer.beginRetainedDbFirstDrop(this,receipt,recallCleanupQualification,full27)
    retainedRecallRetirementReceipt=receipt
    publish()
    retiredRecallOutcome=resources.recallOutcome
    retiredRecallCommitAttempted=resources.recallCommitAttempted
    retiredRecallAcknowledged=resources.recallAcknowledged
    recallCleanupQualification.detachOriginalGraph(this,recallTransaction,recallPreparedReadback.get,primary)
    retainedIssuer.detachRetainedDbFirstPayload(this,receipt,recallCleanupQualification,full27)
    recallTransaction=null
    retainedRecallPayloadDetached=true
    publish()

  private[mysql] def requireRetainedQualificationDetach(adapter:MySqlOwnedIoAdapter,
      receipt:MySqlDbFirstRecallDropReceipt,qualification:FailedRecallCleanupQualification):Unit =
    requireThread()
    require((retainedRecallRetirementReceipt eq receipt) && retainedRecallPayloadDetached && recallTransaction==null &&
      qualification!=null && (recallCleanupQualification eq qualification),
      "actual detached original Session qualification holder required")
    mainRead.requireBinding(this,adapter)
    pendingMainRead.requireBinding(this,adapter)
    qualification.requireDetached(recallPreparedReadback.get,primary)

  private def dropRetainedQualificationHolder(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    require((retainedRecallRetirementReceipt eq receipt) && recallCleanupQualification!=null &&
      recallCleanupQualificationFee==FailedRecallCleanupQualification.HolderBytes,"original qualification fee/holder required")
    recallCleanupQualification.requireDetached(recallPreparedReadback.get,primary)
    recallCleanupQualification=null

  /** adapter/Core/Main已撤原图后，先撤最后Session引用；Unit返回后才退休root/费用。 */
  private[mysql] def finishRetainedRecallRetirement(adapter:MySqlOwnedIoAdapter,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireThread()
    require((retainedRecallRetirementReceipt eq receipt) && retainedRecallPayloadDetached &&
      !retainedRecallFinishAttempted && recallTransaction==null,"actual original detached retirement required once")
    adapter.requireRetainedRecallReferenceDetached(this,receipt)
    retainedRecallFinishAttempted=true
    dropRetainedQualificationHolder(receipt)
    retainedIssuer.retireRetainedDbFirstDrop(this,receipt,full27)
    root.releaseRetainedMemory(recallCleanupQualificationFee,"original retired recall qualification graph detached")
    recallCleanupQualificationFee=0L
    publish() // 保Failed/primary与原UNKNOWN/ACK历史；两Main DATA仍由原Session独立计费保管。

  private[mysql] def failRetainedRecallRetirement(cause:Throwable):Unit =
    requireThread()
    if recallCleanupQualification!=null then recallCleanupQualification.revoke()
    if cleanupFailure==null then cleanupFailure=cause else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure,cause)
    if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
    state=State.Failed
    publish()

  private def requireRetiredRecallHold(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireOpen()
    require(receipt!=null && state==State.Failed && (retainedRecallRetirementReceipt eq receipt) && receipt.retainedEntryRetired &&
      retainedRecallPayloadDetached && retainedRecallFinishAttempted && recallTransaction==null &&
      recallCleanupQualification==null && recallCleanupQualificationFee==0L && primary!=null &&
      receipt.retainedConfirmation.retainedFailure.exists(_ eq primary) &&
      recallPreparedReadback.nonEmpty,
      "actual retired original Entry and shared-fault cause required")
    if mainRead!=null && pendingMainRead!=null then
      pendingMainRead.requireUnpublishedData(this,recallPreparedReadback.get.canonicalSha256)
    else
      require(retiredRecallWriter!=null,"missing actual old-DATA retirement owner")
      retiredRecallWriter.requireRetiredOldData()

  /** 原故障owner先登记独立Work；只读当前Full27，不复用旧runner/qualification或重发原C。 */
  private[mysql] def observeRetiredRecallOnce(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireRetiredRecallHold(receipt)
    require(!retiredRecallObservationAttempted && retiredRecallObservation==null &&
      retiredRecallObservationAllocationFee==0L && recallInspection!=null,
      "actual retired recall current observation is one-shot")
    retiredRecallObservationAttempted=true
    publish()
    try
      root.reserveRetainedMemory(RetiredRecallObservation.HolderBytes,
        "retired recall independent current observation Work BEFORE allocation")
      retiredRecallObservationAllocationFee=RetiredRecallObservation.HolderBytes
      retiredRecallObservation=new RetiredRecallObservation(this,recallInspection,receipt,
        RetiredRecallObservation.HolderBytes)
      retiredRecallObservationAllocationFee=0L
      publish() // 新audit/RR/reader/Full27执行前保管实际Work；失败不清attempt或覆盖旧child。
      retiredRecallObservation.executeOnce()
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 实际终止/当前完整读回责任下只退休旧W；不借旧token续租或接管occupied行。 */
  private[mysql] def retireRetainedRecallWriterOnce(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireRetiredRecallHold(receipt)
    require(!retiredRecallWriterAttempted && retiredRecallWriter==null && retiredRecallWriterAllocationFee==0L &&
      retiredRecallObservation!=null && recallInspection!=null && writerCoordinator!=null &&
      writerToken!=null && writerAdmission!=null,"actual unused retired writer recovery owner required")
    retiredRecallObservation.requireCompletedObservation()
    retiredRecallWriterAttempted=true;publish()
    try
      root.reserveRetainedMemory(RetiredRecallWriterRecovery.HolderBytes,"retired writer recovery owner BEFORE allocation")
      retiredRecallWriterAllocationFee=RetiredRecallWriterRecovery.HolderBytes
      retiredRecallWriter=new RetiredRecallWriterRecovery(this,recallInspection,receipt,retiredRecallObservation,
        RetiredRecallWriterRecovery.HolderBytes)
      retiredRecallWriterAllocationFee=0L
      if retiredRecallCleanupAdapter!=null then
        retiredRecallWriter.bindStoppedCleanupAdapter(retiredRecallCleanupAdapter,receipt)
      publish()
      retiredRecallWriter.executeOnce()
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause
    finally publish()

  /** 完整观测仅一次；后续保持Failed/原cause/两DATA，不续租或发布普通freshness。 */
  private[mysql] def bindRetiredRecallCleanupAdapter(adapter:MySqlOwnedIoAdapter,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireThread()
    require(adapter!=null && receipt!=null,"actual retired recall adapter and receipt required")
    adapter.requireRetiredRecallCleanupNavigation(this,receipt)
    requireRetiredRecallHold(receipt)
    require(retiredRecallCleanupAdapter==null || (retiredRecallCleanupAdapter eq adapter),
      "retired recall cleanup navigation is bound to one actual adapter")
    retiredRecallCleanupAdapter=adapter
    if retiredRecallWriter!=null then retiredRecallWriter.bindStoppedCleanupAdapter(adapter,receipt)

  private[mysql] def requireStoppedRecoveryOwner(source:RetiredRecallWriterRecovery,
      adapter:MySqlOwnedIoAdapter,expectedReceipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireThread()
    require(source!=null && adapter!=null && expectedReceipt!=null &&
      (retiredRecallWriter eq source) && (retainedRecallRetirementReceipt eq expectedReceipt) &&
      (retiredRecallCleanupAdapter eq adapter),
      "actual Session retained stop route and same recall receipt required")
    source.requireStoppedRecoveryOwner(adapter,expectedReceipt)

  private[mysql] def maintainRetiredRecallHold(receipt:MySqlDbFirstRecallDropReceipt):IoMaintenanceDeadline =
    requireRetiredRecallHold(receipt)
    if !retiredRecallObservationAttempted then observeRetiredRecallOnce(receipt)
    if retiredRecallWriter!=null && retiredRecallWriter.oldDataRetired then
      retiredRecallWriter.requireRetiredOldData()
    else
      require(retiredRecallObservation!=null,"retired recall current observation allocation failed; no retry")
      retiredRecallObservation.requireCompletedObservation()
    if !retiredRecallWriterAttempted then retireRetainedRecallWriterOnce(receipt)
    require(retiredRecallWriter!=null,"retired writer allocation failed; no retry")
    val next=try retiredRecallWriter.maintainRecoveredWriter()
      catch case cause:Throwable=>
        retainFailure(cause)
        throw cause
    publish()
    next

  private[mysql] def transferRetiredRecallIssuerIfDrained(adapter:MySqlOwnedIoAdapter,
      receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireRetiredRecallHold(receipt)
    require(retiredRecallWriter!=null,"actual recovered SQL owner required")
    if adapter.retiredRecallIssuerTransferReady(receipt) then
      try
        retiredRecallWriter.transferRetainedIssuerOnce(adapter)
        retiredRecallWriter.prepareServiceMainReadOnce(adapter)
        retiredRecallWriter.retireOldDataAfterMainAck(adapter)
        retiredRecallWriter.transferInputsAfterOldReadersRetired(adapter)
        retiredRecallWriter.retireInspectionAfterServiceAuditRenewal(adapter)
        retiredRecallWriter.registerServiceRuntimeAfterInspectionRetirement(adapter)
        retiredRecallWriter.transferRunningServiceOwner(adapter)
      catch case cause:Throwable=>
        retainFailure(cause)
        throw cause
      finally publish()

  /** Main真实detach的原receipt，只退休确认资源；不创建新O或声称世界cleanup成功。 */
  def releaseDbFirstRecall(receipt:MySqlDbFirstRecallDropReceipt):Unit =
    requireOpen()
    require(retainedIssuer != null,"original retained issuer required")
    val runner=retainedIssuer.requireDbFirstDrop(this,receipt,full27)
    require(runner.resourcesRetired && (recallTransaction ne runner),
      "Main retirement requires the original actually SQL-retired Entry")
    // 旧Main必须真实drop；DATA/root失败属于共享责任，不能交Entry局部失败分类器。
    if pendingMainRead!=null then
      try
        pendingMainRead.requireSuccessorFor(mainRead,receipt.tag)
        mainRead.retireOnWorker(this)
        mainRead=null
        root.releaseRetainedMemory(mainReadFee,"superseded Main backing detached")
        mainReadFee=0L
        pendingMainRead.markPreviousRetired(this,receipt.tag)
        mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
        pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      catch case cause:Throwable=>
        retainFailure(cause)
        throw cause
    try
      retainedIssuer.retireDbFirstDrop(receipt)
      publish()
    catch case cause:Throwable=>
      // 已证实的本Entry preflight阻挡只保局部cause；实际root/shared release失败则保持共享hold。
      var classified:Throwable=cause
      try classified=retainedIssuer.failDbFirstDrop(receipt,cause)
      catch case cleanup:Throwable=>
        MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if !classified.isInstanceOf[MySqlDbFirstLocalRetirementException] then retainFailure(cause)
      publish()
      throw classified

  /** 旧读代已真实ACK后只退休原J；C仍须Main消费实际回执并迁移原槽。 */
  private[mysql] def retireJournalForCapture(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit =
    requireOpen()
    require(retainedIssuer!=null && state==State.SqlWriterAcquired && refusalTransaction==null && journalTransaction==null &&
      pendingMainRead==null,"original healthy journal Session and completed Main read handoff required")
    try
      retainedIssuer.retireJournalCaptureTransfer(this,transfer,full27)
      publish()
    catch case cause:Throwable=>
      var classified:Throwable=cause
      try classified=retainedIssuer.classifyJournalCaptureTransferFailure(transfer,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if !classified.isInstanceOf[MySqlDbFirstJournalCaptureLocalRetirementException] then retainFailure(cause)
      publish()
      throw classified

  def releaseRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    requireOpen()
    try
      require(state==State.SqlWriterAcquired && refusalTransaction==null && journalTransaction==null &&
        pendingMainRead==null && mainRead!=null,"completed original refusal read handoff required before final retirement")
      retainedIssuer.retireRefusalFinal(this,receipt,full27)
      publish()
    catch case cause:Throwable=>
      var classified:Throwable=cause
      try classified=retainedIssuer.failRefusalFinal(receipt,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      if !classified.isInstanceOf[MySqlDbFirstRefusalFinalLocalRetirementException] then retainFailure(cause)
      publish();throw classified

  /** 拒绝确认只退休被替代的Main读代；原J/C Entry/root继续持有，终局另行交接。 */
  def releaseRefusalMainRead(confirmation:MySqlDbFirstRecallRefusalConfirmation):Unit =
    requireOpen()
    retainedIssuer.beginRefusalReadRelease(this,confirmation,full27)
    try
      require(state==State.SqlWriterAcquired && refusalTransaction==null && pendingMainRead!=null &&
        mainRead!=null,"original refusal successor handoff required")
      pendingMainRead.requireSuccessorFor(mainRead,confirmation.tag)
      mainRead.retireOnWorker(this)
      mainRead=null
      root.releaseRetainedMemory(mainReadFee,"superseded refusal Main backing detached")
      mainReadFee=0L
      pendingMainRead.markPreviousRetired(this,confirmation.tag)
      mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
      pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      retainedIssuer.finishRefusalReadRelease(confirmation)
      publish()
    catch case cause:Throwable=>
      retainFailure(cause);publish();throw cause

  /** 原J确认只退休已被真实Main替代的读代；Entry及C身份继续保留。 */
  def releaseJournalMainRead(confirmation:MySqlDbFirstRecallJournalConfirmation):Unit =
    requireOpen()
    retainedIssuer.beginJournalReadRelease(this,confirmation,full27)
    try
      require(state==State.SqlWriterAcquired && refusalTransaction==null && journalTransaction==null && pendingMainRead!=null &&
        mainRead!=null,"original journal successor handoff required")
      pendingMainRead.requireSuccessorFor(mainRead,confirmation.tag)
      mainRead.retireOnWorker(this)
      mainRead=null
      root.releaseRetainedMemory(mainReadFee,"superseded journal Main backing detached")
      mainReadFee=0L
      pendingMainRead.markPreviousRetired(this,confirmation.tag)
      mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
      pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      retainedIssuer.finishJournalReadRelease(confirmation)
      publish()
    catch case cause:Throwable=>
      retainFailure(cause);publish();throw cause

  /** 只退休被替代的Main读代；原准备Entry仍持有业务DATA，不能据此释放业务slot。 */
  def releasePreparationMainRead(confirmation:MySqlDbFirstInitialPreparationConfirmation):Unit =
    requireOpen()
    retainedIssuer.beginPreparationReadRelease(this,confirmation,full27)
    try
      require(state==State.SqlWriterAcquired && preparationTransaction==null && pendingMainRead!=null &&
        mainRead!=null,"original preparation successor handoff required")
      pendingMainRead.requireSuccessorFor(mainRead,confirmation.tag)
      mainRead.retireOnWorker(this)
      mainRead=null
      root.releaseRetainedMemory(mainReadFee,"superseded preparation Main backing detached")
      mainReadFee=0L
      pendingMainRead.markPreviousRetired(this,confirmation.tag)
      mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
      pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      retainedIssuer.finishPreparationReadRelease(confirmation)
      publish()
    catch case cause:Throwable=>
      retainFailure(cause);publish();throw cause

  def releaseCheckpointMainRead(confirmation:MySqlDbFirstCheckpointConfirmation):Unit =
    requireOpen()
    retainedIssuer.beginCheckpointReadRelease(this,confirmation,full27)
    try
      require(state==State.SqlWriterAcquired && checkpointTransaction==null && pendingMainRead!=null &&
        mainRead!=null,"original checkpoint successor handoff required")
      pendingMainRead.requireSuccessorFor(mainRead,confirmation.tag)
      mainRead.retireOnWorker(this)
      mainRead=null
      root.releaseRetainedMemory(mainReadFee,"superseded checkpoint Main backing detached")
      mainReadFee=0L
      pendingMainRead.markPreviousRetired(this,confirmation.tag)
      mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
      pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      retainedIssuer.finishCheckpointReadRelease(confirmation)
      publish()
    catch case cause:Throwable=>
      retainFailure(cause);publish();throw cause

  /** 读代交接已完成后的准备Entry退休；不改变任何世界状态，也不再退休别的successor。 */
  /** 仅恢复已经完整回滚、独立DATA装配、SQL退休的原单次拒绝。任一步失败保留原cause。 */
  def releaseRejectedPreparationMainRead(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit =
    requireOpen()
    require(confirmation!=null && settledPreparationRejection!=null &&
      (settledPreparationRejection eq confirmation.observation) && state==State.Failed &&
      (primary eq confirmation.cause) && primary.getSuppressed.isEmpty && preparationTransaction!=null &&
      pendingMainRead!=null && mainRead!=null,"original isolated settled rejection and successor required")
    preparationTransaction.requireRejectedResourcesRetired(settledPreparationRejection.cause)
    retainedIssuer.beginRejectedPreparationReadRelease(this,confirmation,full27)
    try
      pendingMainRead.requireSuccessorFor(mainRead,confirmation.tag)
      mainRead.retireOnWorker(this);mainRead=null
      root.releaseRetainedMemory(mainReadFee,"superseded rejected preparation Main backing detached");mainReadFee=0L
      pendingMainRead.markPreviousRetired(this,confirmation.tag)
      mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
      pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      retainedIssuer.finishRejectedPreparationReadRelease(confirmation)
      // 原Entry/confirmation继续保存拒绝；这里只解除已被证明局部的Session hold。
      preparationTransaction=null;settledPreparationRejection=null;primary=null;state=State.SqlWriterAcquired
      publish()
    catch case cause:Throwable=>
      MySqlOwnedSessionLifecycle.suppress(confirmation.cause,cause)
      retainFailure(confirmation.cause);throw confirmation.cause

  def retireRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit =
    requireOpen()
    require(state==State.SqlWriterAcquired && primary==null && preparationTransaction==null &&
      settledPreparationRejection==null,"original recovered rejection Session required")
    try
      retainedIssuer.retireRejectedPreparation(this,confirmation,full27)
      publish()
    catch case cause:Throwable=>
      retainFailure(cause)
      throw cause

  def releaseDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt):Unit =
    requireOpen()
    val runner=retainedIssuer.requireDbFirstPreparationFinal(this,receipt,full27)
    require(runner.resourcesRetired && (preparationTransaction ne runner),
      "original preparation SQL retirement required before final Entry release")
    try
      retainedIssuer.retireDbFirstPreparationFinal(receipt)
      publish()
    catch case cause:Throwable=>
      var classified:Throwable=cause
      try classified=retainedIssuer.failDbFirstPreparationFinal(receipt,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if !classified.isInstanceOf[MySqlDbFirstPreparationLocalRetirementException] then retainFailure(cause)
      publish();throw classified

  def releaseDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt):Unit =
    requireOpen()
    val runner=retainedIssuer.requireDbFirstCheckpointFinal(this,receipt,full27)
    require(runner.resourcesRetired && (checkpointTransaction ne runner),
      "original checkpoint SQL retirement required before final Entry release")
    try
      retainedIssuer.retireDbFirstCheckpointFinal(receipt)
      publish()
    catch case cause:Throwable=>
      var classified:Throwable=cause
      try classified=retainedIssuer.failDbFirstCheckpointFinal(receipt,cause)
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if !classified.isInstanceOf[MySqlDbFirstCheckpointLocalRetirementException] then retainFailure(cause)
      publish();throw classified

  def releaseDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt):Unit =
    requireOpen()
    require(retainedIssuer != null,"original retained issuer required")
    val runner=retainedIssuer.requireDbFirstInitialPublication(this,receipt,full27)
    require(runner.resourcesRetired && (initialTransaction ne runner),
      "Main retirement requires the original actually SQL-retired Entry")
    // 旧Main必须真实drop；DATA/root失败属于共享责任，不能交Entry局部失败分类器。
    if pendingMainRead!=null then
      try
        pendingMainRead.requireSuccessorFor(mainRead,receipt.tag)
        mainRead.retireOnWorker(this)
        mainRead=null
        root.releaseRetainedMemory(mainReadFee,"superseded Main backing detached")
        mainReadFee=0L
        pendingMainRead.markPreviousRetired(this,receipt.tag)
        mainRead=pendingMainRead;mainReadFee=pendingMainReadFee
        pendingMainRead=null;pendingMainReadFee=0L;pendingMainReadObservation=null
      catch case cause:Throwable=>
        retainFailure(cause)
        throw cause
    try
      retainedIssuer.retireDbFirstInitialPublication(receipt)
      publish()
    catch case cause:Throwable=>
      // 已证实的本Entry preflight阻挡只保局部cause；实际root/shared release失败则保持共享hold。
      var classified:Throwable=cause
      try classified=retainedIssuer.failDbFirstInitialPublication(receipt,cause)
      catch case cleanup:Throwable=>
        MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      if !classified.isInstanceOf[MySqlDbFirstInitialLocalRetirementException] then retainFailure(cause)
      publish()
      throw classified

  private def retireSupersededWriterAudits():Unit =
    if retiringAuditFailure != null then throw retiringAuditFailure
    try
      if retiringWriterPostAudit != null then
        retiringWriterPostAudit.close()
        retiringWriterPostAudit=null
      if retiringWriterPreAudit != null then
        retiringWriterPreAudit.close()
        retiringWriterPreAudit=null
    catch
      case cause:Throwable =>
        retiringAuditFailure=cause
        throw cause

  private def refreshBarrierFacts():Unit =
    if verified != null && barrierAttempted then
      val observation=verified.sessionBarrierCloseObservation()
      lastBarrierObservation=Some(observation)
      barrierHeld=observation.heldReplyConfirmed && !observation.localCapabilityRevoked && observation.failure.isEmpty
      barrierMayRetain=observation.mayRetainServerLock

  /** 仅原worker的内存责任核验；不向Main发布reservation或admission。 */
  private[mysql] def requireRetainedOwner(): MySqlRetainedAllocationObservation =
    requireOpen()
    if retainedIssuer == null then throw new IllegalStateException("schema6 retained owner unavailable",primary)
    root.requireLedgerIdentity(ledger)
    retainedIssuer.requireDbFirstSession(this,full27,full27.sourceLimitProfile,ledger)
    retainedIssuer.observation

  private def allocateRetainedOwner(actualMain: Thread, capacity: Int): Unit =
    require(!inspectionOnly,"read-only observation must not allocate a retained mutation owner")
    root.requireLedgerIdentity(ledger)
    root.reserveRetainedMemory(RetainedIssuerBytes,"schema6 retained issuer/core BEFORE allocation")
    retainedIssuerFee=RetainedIssuerBytes
    publish()
    retainedIssuer=new MySqlOwnedRetainedPlanIssuer(null,identity,thread,actualMain,
      full27.sourceLimitProfile,ledger,capacity,dbFirstInput=this,dbFirstInputsInput=Some(full27))
    publish() // 在任何可失败的issuer使用前保留原holder。
    retainedIssuer.requireDbFirstSession(this,full27,full27.sourceLimitProfile,ledger)

  /** allocate已返回并由adapter登记后，才允许首次真实connection open。 */
  def initialize(): Unit =
    initializeForObservation(null)

  /** 仅受控托管安装可在原Session打开连接前登记首次绑定owner。 */
  private[mysql] def registerManagedFirstBinding(
      control:com.wi.floatingpod.bootstrap.ProductionBootstrapControl.DbFirstControl):Unit =
    requireOpen()
    require(!inspectionOnly && state==State.Allocated && !started && !closeAttempted &&
      managedFirstBindingActor==null && managedFirstBindingFee==0L,
      "managed first-binding owner requires the original unstarted non-inspection Session")
    require(control!=null && control.managedBundle().isPresent,
      "managed first-binding requires the exact persisted installation bundle")
    require(inputs.managedFirstBindingAllowed,
      "schema6 K does not authorize managed first binding")
    require(control.sourceLimitProfile() eq inputs.sourceLimitProfile,
      "managed first-binding must retain the exact K/root source-limit profile")
    require(control.expectedConfigurationSha256().equalsIgnoreCase(inputs.configurationSha256),
      "managed first-binding control C differs from the original Session K")
    val controlled=control.identity()
    require(controlled.boot()==identity.bootId && controlled.bootGeneration()==identity.bootGeneration &&
      controlled.connectionGeneration()==identity.connectionGeneration,
      "managed first-binding control differs from the original owned IO identity")
    val loaded=control.managedBundle().get()
    val persisted=loaded.identity()
    val expected=inputs.auditExpectation
    // identity.configurationSha256 是托管安装配置的输入指纹，非完整运行配置 C；
    // createManaged 已重读 exact bundle，运行期只与该 bundle 明确携带的 C 同域比较。
    require(loaded.configurationDigestSha256().equalsIgnoreCase(inputs.configurationSha256) &&
      persisted.serverUuid()==expected.serverUuid && persisted.bindingUuid()==expected.bindingUuid &&
      persisted.provisioningUuid()==expected.provisioningUuid && persisted.schema()==expected.schema,
      "managed installation identity differs from the exact schema6 Session K")
    val fee=ManagedFirstBindingHolderBytes
    root.reserveRetainedMemory(fee,"managed first-binding actor/control graph BEFORE allocation")
    managedFirstBindingFee=fee
    publish()
    try
      managedFirstBindingActor=new MySqlDbFirstBootstrapActor(this,inputs,root,identity,control,fee)
      publish()
    catch case cause:Throwable =>
      retainFailure(cause)
      try
        root.releaseRetainedMemory(fee,"unpublished managed first-binding actor holder")
        managedFirstBindingFee=0L
      catch case cleanup:Throwable =>
        if cleanupFailure==null then cleanupFailure=cleanup else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cleanup)
        MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  private def requireManagedFirstBindingActor(actual:MySqlDbFirstBootstrapActor,
      actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope):Unit =
    requireThread()
    require(actual!=null && (managedFirstBindingActor eq actual) && !inspectionOnly &&
      state==State.Initializing && verified!=null && (verified eq actualVerified) &&
      scope!=null && (scope eq actualScope) && auditHolder!=null,
      "exact active managed first-binding actor and original Session JDBC scope required")

  /** 在当前verified连接上一次性取得托管安装的同目标barrier。 */
  private[mysql] def bootstrapAcquireBarrier(actual:MySqlDbFirstBootstrapActor,
      actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope,wait:Duration):HeldSessionBarrier =
    requireManagedFirstBindingActor(actual,actualVerified,actualScope)
    require(wait!=null && !wait.isNegative && wait.compareTo(Duration.ofSeconds(30))<=0 && wait.getNano==0 &&
      actualVerified.connection.getAutoCommit && !gateHeld && reader==null && barrier==null && !barrierAttempted,
      "managed first-binding requires one bounded barrier on its idle original Session")
    barrierAttempted=true
    publish()
    try
      actualVerified.acquireSessionBarrier(inputs.auditExpectation,actualScope,wait) match
        case MySqlSessionBarrierAcquisition.Held(value) =>
          barrier=value
          refreshBarrierFacts()
          publish()
          value
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new SQLNonTransientConnectionException("managed first-binding target barrier is busy")
    catch case cause:Throwable =>
      try refreshBarrierFacts()
      catch case cleanup:Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  /** 丢弃旧资格并以新holder读取实际安装后的安全图。 */
  private[mysql] def bootstrapFreshAudit(actual:MySqlDbFirstBootstrapActor):VerifiedSchemaQualification =
    requireThread()
    require(actual!=null && (managedFirstBindingActor eq actual) && !inspectionOnly &&
      state==State.Initializing && verified!=null && scope!=null && auditHolder!=null &&
      barrier!=null && barrierHeld && !gateHeld && reader==null && verified.connection.getAutoCommit,
      "managed first-binding fresh audit requires its exact held barrier and idle original JDBC Session")
    auditHolder.close()
    auditHolder=null
    qualification=null
    publish()
    val fresh=auditor.beginDbFirstAudit(inputs,root,verified,scope,this)
    auditHolder=fresh
    publish()
    qualification=fresh.readAndQualifyOnce()
    publish()
    qualification

  /** 仅复核本actor已保有的同一held barrier，不再次发送GET_LOCK。 */
  private[mysql] def bootstrapVerifiedBarrier(actual:MySqlDbFirstBootstrapActor,
      actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope):Unit =
    requireManagedFirstBindingActor(actual,actualVerified,actualScope)
    require(barrierAttempted && barrier!=null && barrierHeld && actualVerified.connection.getAutoCommit,
      "managed first-binding requires its original confirmed held barrier")
    actualVerified.readmitSessionBarrier(inputs.auditExpectation,actualScope)
    refreshBarrierFacts()
    require(barrierHeld,"managed first-binding held barrier lost during readmission")
    publish()

  private def initializeForObservation(originalBarrier:HeldSessionBarrier): Unit =
    requireOpen()
    require(originalBarrier==null || inspectionOnly,"barrier handoff is restricted to the original failed-operation observer")
    if started then throw new IllegalStateException("schema6 Session initialization is one-shot",primary)
    started=true;state=State.Initializing;publish()
    try
      val source=new MySqlConnectionSource(inputs.settings)
      openHolder=source.newDbFirstSessionOpen()
      publish() // 在driver open/核验/失败close前保留partial owner。
      verified=openHolder.openOnce()
      publish()
      scope=MySqlAuditBootScope.forOwnedIo(verified,thread,identity.bootId,identity.bootGeneration,scopeConnectionGeneration)
      auditor=new MySqlSchemaAuditor(inputs.auditExpectation,auditMaxAge)
      auditHolder=auditor.beginDbFirstAudit(inputs,root,verified,scope,this)
      publish() // 原partial audit holder在任何caller/grants/CALL读取前登记。
      qualification=auditHolder.readAndQualifyOnce()
      if managedFirstBindingActor!=null then
        qualification=managedFirstBindingActor.ensureCertified(verified,scope,qualification)
        publish()
      if originalBarrier!=null then
        val expected=MySqlSessionBarrierNamespace.forIdentity(inputs.auditExpectation.serverUuid,
          inputs.auditExpectation.schema,inputs.auditExpectation.provisioningUuid,inputs.auditExpectation.bindingUuid)
        MySqlSessionBarrierTargetRules.requireSame(originalBarrier.namespace,expected)
        barrierAttempted=true
        publish() // GET_LOCK之前保留实际attempt责任；busy/未知/读图失败均不自动重试。
        try
          verified.acquireSessionBarrier(inputs.auditExpectation,scope,Duration.ZERO) match
            case MySqlSessionBarrierAcquisition.Held(value)=>
              barrier=value
              require(value.actualConnectionId!=originalBarrier.actualConnectionId &&
                value.connectionGenerationId!=originalBarrier.connectionGenerationId,
                "recall barrier handoff requires a distinct actual session/generation")
            case MySqlSessionBarrierAcquisition.Deferred=>
              throw new SQLNonTransientConnectionException("original failed recall session barrier is still busy; UNKNOWN retained")
        finally
          refreshBarrierFacts()
          publish()
      // 普通诊断不取得barrier；交接观测在完整audit及同namespace独占成功后才开始新RR快照。
      readDatabaseSnapshot(qualification)
      if originalBarrier!=null then verified.readmitSessionBarrier(inputs.auditExpectation,scope)
      state=State.DatabaseObserved
      publish()
    catch case cause: Throwable =>
      retainFailure(cause)
      if gateHeld && rollbackOnly != null then
        try rollbackOnly.mark(cause)
        catch case marking: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,marking)
      try settleReadTransaction()
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      publish()
      throw cause

  /** 原Session承担每次完整read的gate、reader和验证holder；失败保留同一责任槽。 */
  private def readDatabaseSnapshot(readQualification:VerifiedSchemaQualification):Unit =
    val connection=verified.connection
    if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("schema6 observation requires idle auto-commit connection")
    oldIsolation=Some(connection.getTransactionIsolation)
    rollbackOnly=new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
      case Some(cause)=>throw cause
      case None=>gateHeld=true
    publish()
    gate=MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly)
    isolationAttempted=true
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
    transactionAttempted=true
    connection.setAutoCommit(false)
    if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLNonTransientConnectionException("schema6 observation did not enter original REPEATABLE READ transaction")
    try reader=if inspectionOnly then MySqlDbFirstSnapshotJdbc.createForObservation(this,inputs,root,identity)
      else MySqlDbFirstSnapshotJdbc.create(inputs,root,identity)
    catch case creation: MySqlDbFirstSnapshotJdbc.CreationFailure =>
      reader=creation.retainedHolder
      publish()
      throw creation
    publish() // caller-retained reader在任何Full27读取SQL前可达。
    root.reserveRetainedMemory(HistoricalRead.HolderBytes,"schema6 historical read holder BEFORE allocation")
    try historicalRead=new HistoricalRead(root,full27)
    catch case cause: Throwable =>
      try root.releaseRetainedMemory(HistoricalRead.HolderBytes,"unallocated schema6 historical read holder")
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause
    publish() // 在读取或解码前登记；清理失败仍保留原子作用域与借用。
    historicalRead.acquire(inputs.historicalInputs)
    root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"schema6 forest read holder BEFORE allocation")
    try forestRead=new MySqlDbFirstForestRead(root,full27,inputs.auditExpectation.bindingUuid)
    catch case cause: Throwable =>
      try root.releaseRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"unallocated schema6 forest read holder")
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause
    publish() // 原 forest holder 在读取 SQL/解码前可达。
    val snapshot=if inspectionOnly then reader.readConsistentObservation(verified,scope,readQualification,gate,rollbackOnly)
      else reader.readLocked(verified,scope,readQualification,gate,rollbackOnly)
    forestRead.validate(snapshot)
    val forestSummary=forestRead.result
    forestRead.close()
    forestRead=null
    historicalRead.validate(snapshot)
    val phaseSummary=historicalRead.result
    val recallSummary=historicalRead.dbFirstRecallSummary
    val initialSummary=historicalRead.dbFirstInitialActivationSummary
    val preparationSummary=historicalRead.dbFirstInitialPreparationSummary
    val checkpointSummary=historicalRead.dbFirstCheckpointSummary
    historicalRead.close()
    historicalRead=null
    settleReadTransaction()
    reader.detachCompletedReadAuthority()
    databaseObservation=DatabaseObservation(identity,full27.schema,inputs.auditExpectation.bindingUuid,
      snapshot.rowCount,snapshot.canonicalSha256Hex,phaseSummary,forestSummary,recallSummary,initialSummary,preparationSummary,checkpointSummary)

  /** 初次观察只提供诊断；取得writer后重新读取，不把acquire前的image当作恢复基线。 */
  private def refreshAcquiredDatabaseSnapshot():Unit =
    requireOpen()
    require(primary==null && reader!=null && databaseObservation!=null && !gateHeld && gate==null &&
      rollbackReturned && autoCommitRestored && isolationRestored && gateLeaveAttempted &&
      historicalRead==null && forestRead==null && writerToken!=null && writerAdmission!=null,
      "post-acquire read requires the settled original observation and actual writer")
    require(writerCoordinator.requireBusinessAdmission(verified,scope) eq writerAdmission,
      "post-acquire read requires original current admission")
    reader.close() // 旧image尚未向Main发行；失败仍由原槽保留，不能清ref假装退休。
    reader=null
    databaseObservation=null
    rollbackOnly=null;oldIsolation=None
    gateLeaveAttempted=false;isolationAttempted=false;isolationRestoreAttempted=false;isolationRestored=false
    transactionAttempted=false;rollbackAttempted=false;rollbackReturned=false;rollbackFailure=null
    autoCommitRestoreAttempted=false;autoCommitRestored=false
    publish()
    try
      readDatabaseSnapshot(writerToken.qualification)
      require(writerCoordinator.requireBusinessAdmission(verified,scope) eq writerAdmission,
        "post-acquire completed image lost its original admission")
    catch case cause:Throwable=>
      if gateHeld && rollbackOnly!=null then
        try rollbackOnly.mark(cause)
        catch case marking:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,marking)
      try settleReadTransaction()
      catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause

  /** 只在所有ResultSet退出后调用；rollback未确认时绝不setAutoCommit(true)。 */
  private def settleReadTransaction(): Unit =
    if !gateHeld then return
    val connection=verified.connection
    if transactionAttempted then
      if !rollbackAttempted then
        rollbackAttempted=true
        try
          connection.rollback()
          rollbackReturned=true
        catch case cause: Throwable =>
          rollbackFailure=cause
          throw cause
      if !rollbackReturned then throw rollbackFailure
      if !autoCommitRestoreAttempted then
        autoCommitRestoreAttempted=true
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("schema6 auto-commit restoration was not observed")
        autoCommitRestored=true
      if !autoCommitRestored then throw new SQLNonTransientConnectionException("schema6 auto-commit restoration remains uncertain")
    if isolationAttempted then
      if !isolationRestoreAttempted then
        isolationRestoreAttempted=true
        connection.setTransactionIsolation(oldIsolation.get)
        if connection.getTransactionIsolation != oldIsolation.get then
          throw new SQLNonTransientConnectionException("schema6 isolation restoration was not observed")
        isolationRestored=true
      if !isolationRestored then throw new SQLNonTransientConnectionException("schema6 isolation restoration remains uncertain")
    leaveGate()
  private def leaveGate(): Unit =
    if gateHeld then
      if gateLeaveAttempted then throw new IllegalStateException("schema6 original gate exit remains unsettled")
      gateLeaveAttempted=true
      MySqlConnectionTransactionGate.leave(verified.connection,rollbackOnly)
      gateHeld=false;gate=null

  def observationOnOwnerThread: DatabaseObservation =
    requireOpen()
    if (state != State.DatabaseObserved && state != State.SqlWriterAcquired) || databaseObservation == null then
      throw new IllegalStateException("schema6 database observation not completed",primary)
    databaseObservation
  private[mysql] def snapshotOnOwnerThread: MySqlScriptSchemaSnapshot =
    observationOnOwnerThread
    reader.snapshot

  private[mysql] def prepareMainRead(actualAdapter:MySqlOwnedIoAdapter,
      approved:com.wi.floatingpod.bootstrap.ProductionBootstrapControl.DbFirstControl,
      tag:IoOperationTag,epoch:Long):MySqlDbFirstMainReadBacking =
    currentSqlWriterFreshness()
    require(mainRead==null && mainReadFee==0L && approved!=null && tag!=null && epoch>=0L &&
      tag.operationId==approved.identity().acquireOperation(),"original controlled one-shot acquired Main input required")
    identity.requireTag(tag)
    actualAdapter.requireOriginalDbFirstSession(this)
    root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,"schema6 Main/source backing BEFORE allocation")
    mainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
    mainRead=new MySqlDbFirstMainReadBacking(thread,this,actualAdapter,approved,tag,epoch)
    mainRead.assemble(reader.snapshot,full27,root)
    require(bootRecallCensus==null,"Boot metadata retention is initial-acquisition only")
    bootRecallCensus=MySqlDbFirstBootRecallCensus.retain(root,thread,mainRead.unclaimedRecallCensus(this))
    currentSqlWriterFreshness()
    mainRead

  private[mysql] def beginBootTakeoverIntent(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,record:MySqlDbFirstRecallRequestRecord,
      control:IoOperationTag,execution:IoOperationTag):MySqlDbFirstBootRecallTakeoverIntent = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null,"original Boot Session census registration required")
    identity.requireTag(control);identity.requireTag(execution)
    bootRecallCensus.beginTakeover(owner,record,control,execution)
  }
  private[mysql] def claimBootTakeoverResult(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require(bootRecallCensus!=null && retainedIssuer!=null,"original Session takeover registry required")
    retainedIssuer.requireBootRecallTakeoverResult(intent,reservation)
    bootRecallCensus.claimTakeoverOnMain(owner,intent,reservation)
  }
  private[mysql] def beginBootMainDecision(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null && retainedIssuer!=null,"same current Boot decision Session required")
    bootRecallCensus.requireMainClaimedTakeover(owner,intent,reservation)
    retainedIssuer.beginBootRecallMainDecision(owner,intent,reservation)
  }
  private[mysql] def finishBootMainDecision(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation,reason:Option[com.wi.floatingpod.domain.RecallRefusalReason]):MySqlDbFirstBootRecallMainDecision = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null && retainedIssuer!=null,"same current Boot decision Session required")
    bootRecallCensus.requireMainClaimedTakeover(owner,intent,reservation)
    retainedIssuer.finishBootRecallMainDecision(owner,intent,reservation,reason)
  }
  /** 只由实际Session私有Work槽回核；不接受调用方fresh/ended布尔值或自填callback。 */
  private[mysql] def requireBootTakeoverRead(work:MySqlDbFirstBootRecallTakeoverWork,
      actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,actualIdentity:MySqlOwnedIoIdentity,
      actualVerified:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope,qualification:VerifiedSchemaQualification):Unit =
    requireThread()
    require(work!=null && (bootTakeoverWork eq work) && (actualInputs eq inputs) && (actualRoot eq root) &&
      (actualIdentity eq identity) && (actualVerified eq verified) && (actualScope eq scope) && writerToken!=null &&
      (qualification eq writerToken.qualification) && barrier!=null && !inspectionOnly,
      "same actual Session registered Boot Work/read authority required")
    currentSqlWriterFreshness()
    barrier.readmit(verified,inputs.auditExpectation,scope)
  private[mysql] def executeBootRecallTakeover(intent:MySqlDbFirstBootRecallTakeoverIntent,
      epoch:Long):MySqlDbFirstCaptureReservation =
    requireOpen();currentSqlWriterFreshness()
    require(intent!=null && epoch>=0L && bootRecallCensus!=null && bootTakeoverWork==null &&
      retainedIssuer!=null && pendingMainRead==null && barrier!=null && !inspectionOnly,
      "actual idle Boot Session takeover slot required")
    identity.requireTag(intent.controlTag);identity.requireTag(intent.executionTag)
    bootRecallCensus.beginTakeoverOnWorker(intent)
    try
      bootTakeoverWork=retainedIssuer.beginBootRecallTakeover(this,intent,full27)
      publish()
      bootTakeoverWork.execute(this,inputs,root,identity,verified,scope,writerToken.qualification)
      val reservation=retainedIssuer.finishBootRecallTakeover(bootTakeoverWork)
      bootRecallCensus.completeTakeoverOnWorker(intent,reservation)
      bootTakeoverWork=null
      publish()
      reservation
    catch case cause:Throwable=>
      try retainedIssuer.failBootRecallTakeover(intent,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      retainFailure(cause);publish();throw cause

  private[mysql] def activateBootRecallCapture(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,decision:MySqlDbFirstBootRecallMainDecision,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null && retainedIssuer!=null,"actual current Boot capture Session required")
    bootRecallCensus.requireMainClaimedTakeover(owner,decision.intent,reservation)
    retainedIssuer.activateBootRecallCapture(owner,decision,reservation)
  }
  private[mysql] def detachBootTakeover(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null,"actual current successor Session required for Boot detach")
    bootRecallCensus.detachTakeover(owner,intent,reservation)
  }
  private[mysql] def claimBootRecallCensus(backing:MySqlDbFirstMainReadBacking,
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Vector[MySqlDbFirstRecallRequestRecord] = synchronized {
    require((mainRead eq backing) && bootRecallCensus!=null && !inspectionOnly,
      "actual current Session Boot census required")
    bootRecallCensus.claim(owner)
  }
  /** Adapter 在真实 Main claim 后固定同一付费 census 的导航引用；本调用不转移其所有权。 */
  private[mysql] def claimedBootRecallCensus(
      owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):MySqlDbFirstBootRecallCensus = synchronized {
    require(owner!=null && bootRecallCensus!=null,"actual claimed Boot census holder required")
    bootRecallCensus.requireClaimedOwner(owner)
    bootRecallCensus
  }
  private[mysql] def detachBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit = synchronized {
    require(bootRecallCensus!=null,"actual original Session Boot census holder required")
    bootRecallCensus.detach(owner)
  }

  private def requireServiceInputOwner(actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity):Unit =
    if serviceAdapter!=null then serviceAdapter.requireRegisteredRecoveredService(this,actualInputs,actualRoot,actualIdentity)
    else
      require(serviceSource!=null,"actual recovered service input owner required")
      serviceSource.requireServiceInputBinding(this,actualInputs,actualRoot,actualIdentity)

  private[mysql] def requireRegisteredRuntimeBinding(adapter:MySqlOwnedIoAdapter,
      actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity):Unit =
    requireThread()
    require(adapter!=null && (serviceAdapter eq adapter) && (inputs eq actualInputs) && (root eq actualRoot) &&
      !actualRoot.isClosed && (identity eq actualIdentity) && !inspectionOnly && inspectionParent==null &&
      scopeConnectionGeneration==identity.connectionGeneration && ownsInputs,
      "actual new Session privately bound runtime adapter/root/input/true generation required")
    if closeAttempted then require(state==State.Closing || state==State.Failed,
      "actual registered service cleanup must retain its input identity")
    else
      require(primary==null && (state==State.SqlWriterAcquired || state==State.SqlWriterRenewing) &&
        inputBorrow!=null && verified!=null && scope!=null && barrier!=null &&
        scope.verifiedConnection.exists(_ eq verified),"actual live registered service SQL/input responsibility required")
      if pendingMainRead!=null then
        require(pendingMainReadFee==MySqlDbFirstMainReadBacking.HolderBytes,"actual pending service Main DATA fee required")
        pendingMainRead.requireBinding(this,adapter)
      else
        require(mainRead!=null && mainReadFee==MySqlDbFirstMainReadBacking.HolderBytes,
          "actual current service Main DATA owner required")
        mainRead.requireBinding(this,adapter)

  private[mysql] def requireInitialRegisteredRuntimeBinding(adapter:MySqlOwnedIoAdapter,
      actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity,initialBacking:MySqlDbFirstMainReadBacking):Unit =
    requireRegisteredRuntimeBinding(adapter,actualInputs,actualRoot,actualIdentity)
    require(initialBacking!=null && (mainRead eq initialBacking) && mainReadFee==MySqlDbFirstMainReadBacking.HolderBytes,
      "actual initial recovered service Main DATA backing required")
    initialBacking.requireBinding(this,adapter)

  private[MySqlDbFirstSession] def installRegisteredServiceRuntime(source:RetiredRecallWriterRecovery,
      adapter:MySqlOwnedIoAdapter):Unit =
    requireThread()
    require(source!=null && (serviceSource eq source) && serviceAdapter==null,
      "actual cleanup predecessor and unused runtime owner binding required")
    serviceAdapter=adapter
    requireInitialRegisteredRuntimeBinding(adapter,inputs,root,identity,mainRead)
    adapter.requireRegisteredRecoveredService(this,inputs,root,identity)

  private[mysql] def registerServiceSnapshotReader(actual:MySqlDbFirstSnapshotJdbc,
      actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity):Unit =
    requireThread()
    require((serviceSource!=null || adapterCleanupOwned) && actual!=null && !closeAttempted && primary==null &&
      state==State.SqlWriterAcquired && verified!=null && scope!=null && !serviceReaders.contains(actual),
      "actual live service Session and previously unregistered reader required")
    requireServiceInputOwner(actualInputs,actualRoot,actualIdentity)
    actual.requireServiceRegistration(this,actualInputs,actualRoot,actualIdentity)
    serviceReaders.addLast(actual)

  private[mysql] def requireServiceSnapshotReader(actual:MySqlDbFirstSnapshotJdbc):Unit =
    requireThread()
    require((serviceSource!=null || adapterCleanupOwned) && actual!=null && serviceReaders.contains(actual),
      "actual service Session private reader registration required")

  /** caller已完成全部清理及退款，同worker中最后撤实际登记强边；不再调用可失败的资源操作。 */
  private[mysql] def detachServiceSnapshotReader(actual:MySqlDbFirstSnapshotJdbc):Unit =
    actual.requireServiceReaderRetired(this)
    serviceReaders.remove(actual)

  private[mysql] def requireServiceImageIssuer(actual:MySqlDbFirstSnapshotJdbc,
      actualRoot:MySqlFormat3ReadBudget,actualInputs:MySqlDbFirstSessionInputs,
      actualIdentity:MySqlOwnedIoIdentity,actualVerified:VerifiedMySqlConnection,
      actualScope:MySqlAuditBootScope):Unit =
    requireServiceSnapshotReader(actual)
    require(!closeAttempted && primary==null && state==State.SqlWriterAcquired &&
      !inspectionOnly && (verified eq actualVerified) && (scope eq actualScope) &&
      scope.connectionGeneration==identity.connectionGeneration,
      "registered service reader must use the actual new Session SQL connection/scope")
    requireServiceInputOwner(actualInputs,actualRoot,actualIdentity)

  private[mysql] def requireServiceResourcesRetired(source:RetiredRecallWriterRecovery):Unit =
    requireServiceResourcesBeforeInputClose(source)
    require(inputsClosed,"actual service inputs close/borrow settlement required")

  private[mysql] def requireOwnedInputClose(actualInputs:MySqlDbFirstSessionInputs):Unit =
    requireThread()
    require(ownsInputs && (inputs eq actualInputs) &&
      (adapterCleanupOwned || serviceSource!=null && serviceSource.inputsCloseTransferred),
      "actual service owns the transferred final input close")
    requireServiceResourcesBeforeInputClose(serviceSource)

  private[mysql] def requireServiceResourcesBeforeInputClose(source:RetiredRecallWriterRecovery):Unit =
    requireThread()
    require((if adapterCleanupOwned then source==null && serviceSource==null && serviceAdapter!=null
      else source!=null && (serviceSource eq source)) && closeAttempted && state==State.Closing &&
      jdbcCloseReturned && jdbcClosedObserved && verified==null && scope==null && auditor==null && openHolder==null &&
      writerCoordinator==null && writerToken==null && writerAdmission==null && writerFee==0L &&
      writerPreAudit==null && writerPostAudit==null && writerReleaseAudit==null &&
      pendingWriterPreAudit==null && pendingWriterPostAudit==null &&
      retiringWriterPreAudit==null && retiringWriterPostAudit==null &&
      previousWriterToken==null && previousWriterAdmission==null &&
      reader==null && serviceReaders.isEmpty && historicalRead==null && forestRead==null && auditHolder==null && recallReadbackAudit==null &&
      mainRead==null && mainReadFee==0L && pendingMainRead==null && pendingMainReadFee==0L &&
      retainedIssuer==null && retainedIssuerFee==0L &&
      !gateHeld && inputBorrow==null &&
      (inputsClosed || ownsInputs && (adapterCleanupOwned || source.inputsCloseTransferred)) &&
      holderFee==MySqlDbFirstSession.HolderBytes && cleanupFailure==null,
      "actual new service SQL/borrow resources must retire before input/owner edges and holder refund")
    if !inputsClosed then inputs.requireFinalCloseOwner(this)

  private[mysql] def requireStoppedEmptyServiceShell(source:RetiredRecallWriterRecovery,
      exactInputs:MySqlDbFirstSessionInputs,exactRoot:MySqlFormat3ReadBudget,
      exactIdentity:MySqlOwnedIoIdentity):Unit =
    requireThread()
    require(source!=null && (serviceSource eq source) && (inputs eq exactInputs) && (root eq exactRoot) &&
      (identity eq exactIdentity) && !inspectionOnly && inspectionParent==null && !ownsInputs &&
      (closeAttempted && state==State.Closing || !closeAttempted && state==State.Allocated) &&
      !inputsClosed && !adapterCleanupOwned && serviceAdapter==null && serviceReaders.isEmpty &&
      verified==null && scope==null && auditor==null && qualification==null && auditHolder==null &&
      reader==null && historicalRead==null && forestRead==null && databaseObservation==null &&
      mainRead==null && mainReadFee==0L && pendingMainRead==null && pendingMainReadFee==0L &&
      writerCoordinator==null && writerToken==null && writerAdmission==null && writerFee==0L &&
      writerPreAudit==null && writerPostAudit==null && writerReleaseAudit==null &&
      pendingWriterPreAudit==null && pendingWriterPostAudit==null && retiringWriterPreAudit==null &&
      retiringWriterPostAudit==null && previousWriterToken==null && previousWriterAdmission==null &&
      retainedIssuer==null && retainedIssuerFee==0L && openHolder==null && barrier==null && !gateHeld &&
      (if closeAttempted then inputBorrow==null else true) && holderFee==MySqlDbFirstSession.HolderBytes &&
      primary==null && cleanupFailure==null,
      "actual empty untransferred service shell with no SQL/resource ownership required")

  private[mysql] def hasAdapterCleanup(adapter:MySqlOwnedIoAdapter):Boolean =
    requireThread()
    adapterCleanupOwned && (serviceAdapter eq adapter)

  private[mysql] def requireOwnedServiceCleanup(adapter:MySqlOwnedIoAdapter,mailbox:MySqlOwnedMaintenanceMailbox):Unit =
    requireThread()
    require(adapterCleanupOwned && (serviceAdapter eq adapter) && serviceSource==null && ownsInputs &&
      (ownedServiceMailbox eq mailbox) && ownedServiceMailboxFee==1024L && ownedServiceControlFee==1024L,
      "actual privately transferred service mailbox/control/final-close owner required")

  private[MySqlDbFirstSession] def takeAdapterCleanup(source:RetiredRecallWriterRecovery,adapter:MySqlOwnedIoAdapter,
      mailbox:MySqlOwnedMaintenanceMailbox):Unit =
    requireThread()
    require((serviceSource eq source) && (serviceAdapter eq adapter) && !adapterCleanupOwned && ownsInputs &&
      ownedServiceMailbox==null && ownedServiceMailboxFee==0L && ownedServiceControlFee==0L && mailbox!=null,
      "actual unused new service cleanup responsibility required")
    ownedServiceMailbox=mailbox;ownedServiceMailboxFee=1024L;ownedServiceControlFee=1024L
    adapterCleanupOwned=true;serviceSource=null

  private[mysql] def requireOwnedServiceResourcesBeforeInputs(adapter:MySqlOwnedIoAdapter):Unit =
    require(adapterCleanupOwned && (serviceAdapter eq adapter) && ownedServiceMailbox==null && ownedServiceMailboxFee==0L,
      "actual owned service mailbox retirement and cleanup actor required")
    requireServiceResourcesBeforeInputClose(null)

  private[mysql] def detachOwnedServiceInputs(adapter:MySqlOwnedIoAdapter):Unit =
    requireOwnedServiceResourcesBeforeInputs(adapter)
    inputs.detachRegisteredServiceOwner(this,adapter,root,identity)

  /** 仅清理由真实Recovery创建但尚未转入SQL资源的空service壳。 */
  private[mysql] def retireStoppedEmptyServiceShell(source:RetiredRecallWriterRecovery,
      inputBindingReturned:Boolean):Unit =
    requireThread()
    require(source!=null && (serviceSource eq source) && !ownsInputs && !closeAttempted &&
      state==State.Allocated && holderFee==MySqlDbFirstSession.HolderBytes &&
      managedFirstBindingActor==null && managedFirstBindingFee==0L,
      "actual allocated, untransferred service shell required")
    source.requireStoppedServiceShell(this,closingAllowed=false)
    closeAttempted=true;state=State.Closing
    var operationFailure:Throwable=null
    try
      if inputBorrow!=null then { inputBorrow.close();inputBorrow=null }
      inputs.detachStoppedEmptyServiceOwner(source,this,root,identity,inputBindingReturned)
      serviceSource=null
      root.releaseRetainedMemory(holderFee,"actual stopped empty service Session holder detached")
      holderFee=0L;state=State.Closed
    catch case cause:Throwable=>
      operationFailure=cause
      cleanupFailure=cause
      if primary==null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
      state=State.Failed
      throw cause
    finally
      try publish()
      catch case publicationFailure:Throwable=>
        if operationFailure!=null then MySqlOwnedSessionLifecycle.suppress(operationFailure,publicationFailure)
        else
          cleanupFailure=publicationFailure
          if primary==null then primary=publicationFailure else MySqlOwnedSessionLifecycle.suppress(primary,publicationFailure)
          state=State.Failed
          throw publicationFailure

  private[mysql] def requireTransferredParentResources():Unit =
    requireThread()
    require(!ownsInputs && inputBorrow==null && !gateHeld && jdbcCloseAttempted && jdbcCloseReturned && jdbcClosedObserved &&
      recallInspection==null && recallInspectionAllocationFee==0L && retiredRecallObservation==null &&
      retiredRecallObservationAllocationFee==0L && retiredRecallWriterAllocationFee==0L &&
      bootTakeoverWork==null && bootRecallCensus==null && initialTransaction==null && preparationTransaction==null &&
      checkpointTransaction==null && refusalTransaction==null && journalTransaction==null && recallTransaction==null &&
      mainRead==null && mainReadFee==0L && pendingMainRead==null && pendingMainReadFee==0L &&
      retainedIssuer==null && retainedIssuerFee==0L && writerCoordinator==null && writerFee==0L &&
      writerToken==null && writerAdmission==null && writerPreAudit==null && writerPostAudit==null && writerReleaseAudit==null &&
      pendingWriterPreAudit==null && pendingWriterPostAudit==null && retiringWriterPreAudit==null && retiringWriterPostAudit==null &&
      previousWriterToken==null && previousWriterAdmission==null && reader==null && serviceReaders.isEmpty &&
      historicalRead==null && forestRead==null && auditHolder==null && recallReadbackAudit==null &&
      recallCleanupQualification==null && recallCleanupQualificationFee==0L &&
      recallNotAppliedQualification==null && recallNotAppliedQualificationFee==0L &&
      managedFirstBindingActor==null && managedFirstBindingFee==0L && primary!=null,
      "actual transferred old parent must retain no SQL/read/DATA/borrow/issuer work")

  private[mysql] def retireTransferredParent(adapter:MySqlOwnedIoAdapter):Unit =
    requireTransferredParentResources()
    require(!closeAttempted && state==State.Failed && retiredRecallWriter==null,
      "actual failed old parent and retired Recovery required once")
    closeAttempted=true;state=State.Closing
    // 原JDBC已实际关闭，Inputs关闭权已转出；这里不close任何新服务资源。
    verified=null;scope=null;auditor=null;openHolder=null;barrier=null;barrierHeld=false;barrierMayRetain=false
    databaseObservation=null;recallPreparedReadback=None;retainedRecallRetirementReceipt=null;inputsClosed=true
    try
      adapter.detachRetiringRecoveredParent(this)
      transferredParentHolderRefundAttempted=true
      try root.releaseRetainedMemory(holderFee,"actual transferred old parent graph detached")
      catch case cause:Throwable=>
        adapter.retainFailedRecoveredParent(this,cause)
        throw cause
      transferredParentHolderRefundReturned=true
      holderFee=0L;state=State.Closed
    catch case cause:Throwable=>
      cleanupFailure=cause;state=State.Failed
      MySqlOwnedSessionLifecycle.suppress(primary,cause)
      throw cause
    finally publish()

  /** Stop-only continuation of a known pre-refund transferred-parent failure; an uncertain refund is never retried. */
  private[mysql] def retireTransferredParentForStop(adapter:MySqlOwnedIoAdapter):Unit =
    requireThread()
    require(adapter!=null && state==State.Failed && retiredRecallWriter==null &&
      holderFee==MySqlDbFirstSession.HolderBytes && !transferredParentHolderRefundAttempted &&
        !transferredParentHolderRefundReturned &&
        (!closeAttempted || cleanupFailure!=null),
      "exact unrefunded transferred parent stop owner required")
    requireTransferredParentResources()
    adapter.requireStoppedRecoveredParentCleanup(this)
    if !closeAttempted then
      closeAttempted=true
      // 原SQL/read图已验证退休、最终Inputs已属新service；这里只撤旧parent字段，不触碰新service资源。
      verified=null;scope=null;auditor=null;openHolder=null;barrier=null;barrierHeld=false;barrierMayRetain=false
      databaseObservation=null;recallPreparedReadback=None;retainedRecallRetirementReceipt=null;inputsClosed=true
      publish()
    state=State.Closing // 仅未尝试退款的metadata可继续；先前资源与所有权事实已在上方核验。
    try
      adapter.detachRetiringRecoveredParent(this)
      transferredParentHolderRefundAttempted=true
      try root.releaseRetainedMemory(holderFee,"actual stopped transferred old parent graph detached")
      catch case cause:Throwable=>
        adapter.retainFailedRecoveredParent(this,cause)
        throw cause
      transferredParentHolderRefundReturned=true
      holderFee=0L;state=State.Closed
    catch case cause:Throwable=>
      cleanupFailure=cause
      MySqlOwnedSessionLifecycle.suppress(primary,cause)
      throw cause
    finally publish()

  private[mysql] def requireRetiringParentHolder():Unit =
    requireTransferredParentResources()
    require(closeAttempted && state==State.Closing && retiredRecallWriter==null && inputsClosed &&
      verified==null && scope==null && auditor==null && openHolder==null && barrier==null &&
      holderFee==MySqlDbFirstSession.HolderBytes,"actual old parent holder must retire before refund")

  private def retireTransferredInspectionReaders(source:RetiredRecallWriterRecovery):Unit =
    requireThread()
    source.requireInspectionReaderRetirement(this)
    require(inspectionOnly && !closeAttempted && !gateHeld && connectionSuccessor!=null,
      "actual settled inspection read resources required before service close")
    if historicalRead!=null then { historicalRead.close();historicalRead=null }
    if forestRead!=null then { forestRead.close();forestRead=null }
    if reader!=null then { reader.close();reader=null }
    qualification=null;recallReadbackQualification=null
    if recallReadbackAudit!=null then { recallReadbackAudit.close();recallReadbackAudit=null }
    if auditHolder!=null then { auditHolder.close();auditHolder=null }
    databaseObservation=null

  private def ownsInheritedServiceAudit(source:RetiredRecallWriterRecovery,
      actual:MySqlSchemaAuditor.DbFirstAudit):Boolean =
    (serviceSource eq source) && actual!=null &&
      ((writerPreAudit eq actual) || (writerPostAudit eq actual) ||
        (retiringWriterPreAudit eq actual) || (retiringWriterPostAudit eq actual))

  /** 只结算本地JDBC/读取/输入责任；不证明远端session终止，也不发行Ready。 */
  def closeOnOwnerThread(): Unit =
    requireThread()
    if state == State.Closed then return
    if bootTakeoverWork!=null then throw new IllegalStateException("Boot takeover retains original read/Entry responsibility",primary)
    if refusalTransaction != null then throw new IllegalStateException("refusal transaction retains original SQL/Entry responsibility",primary)
    if journalTransaction != null then throw new IllegalStateException("journal transaction retains original SQL/Entry responsibility",primary)
    if preparationTransaction != null then throw new IllegalStateException("preparation transaction retains original SQL/Entry responsibility",primary)
    if closeAttempted then throw new IllegalStateException("schema6 Session cleanup incomplete; no automatic retry",cleanupFailure)
    if recallCleanupQualification!=null then recallCleanupQualification.revoke()
    if recallNotAppliedQualification!=null then recallNotAppliedQualification.revoke()
    closeAttempted=true;state=State.Closing;publish()
    def settle(action: => Unit): Boolean =
      try { action;true }
      catch case cause: Throwable =>
        if cleanupFailure == null then cleanupFailure=cause else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cause)
        if primary == null then primary=cause else MySqlOwnedSessionLifecycle.suppress(primary,cause)
        false
    def detachClosedManagedFirstBindingActor():Unit =
      val actual=managedFirstBindingActor
      if actual!=null then
        val observed=actual.resourceObservation
        if observed.closed && !observed.nativePending && !observed.readersPending && !observed.dmlPending &&
            !observed.gateHeld && !observed.commitUnknown then
          if managedFirstBindingFee==0L then managedFirstBindingActor=null
          else if settle(root.releaseRetainedMemory(managedFirstBindingFee,
              "managed first-binding actor retired after confirmed local owner close")) then
            managedFirstBindingFee=0L
            managedFirstBindingActor=null
    if managedFirstBindingActor!=null && !managedFirstBindingActor.resourceObservation.closed then
      val beforeReturned=settle(managedFirstBindingActor.beforeJdbcClose())
      val actorState=managedFirstBindingActor.resourceObservation
      if beforeReturned && !actorState.started && actorState.failure.isEmpty && !actorState.nativePending &&
          !actorState.readersPending && !actorState.dmlPending && !actorState.gateHeld && !actorState.commitUnknown then
        settle(managedFirstBindingActor.retireUnstarted())
      detachClosedManagedFirstBindingActor()
    if adapterCleanupOwned then
      serviceAdapter.beginOwnedRecoveredServiceClose(this)
      if ownedServiceMailbox!=null && settle {
        ownedServiceMailbox.stop();ownedServiceMailbox.retireAfterTerminalProducer()
      } then ownedServiceMailbox=null
      if ownedServiceMailbox==null && ownedServiceMailboxFee!=0L &&
          settle(root.releaseRetainedMemory(ownedServiceMailboxFee,"actual owned service mailbox retired")) then ownedServiceMailboxFee=0L
    if retiredRecallWriter!=null then
      if !settle(retiredRecallWriter.closeForStoppedService()) then
        state=State.Failed;publish();throw cleanupFailure
      closedRetiredRecallWriter=Some(retiredRecallWriter.resourceObservation)
      retiredRecallWriter=null
      retiredRecallCleanupAdapter=null
    if retiredRecallWriterAllocationFee!=0L then
      if !settle(root.releaseRetainedMemory(retiredRecallWriterAllocationFee,"unallocated retired writer owner detached")) then
        state=State.Failed;publish();throw cleanupFailure
      retiredRecallWriterAllocationFee=0L
    // 新Work的独立RR/reader/audit必须先退出；不能先关其仍借用的旧inspection连接。
    if retiredRecallObservation!=null then
      if !settle(retiredRecallObservation.closeOnOwnerThread()) then
        state=State.Failed;publish();throw cleanupFailure
      closedRetiredRecallObservation=Some(retiredRecallObservation.resourceObservation)
      retiredRecallObservation=null
    if retiredRecallObservationAllocationFee!=0L then
      if !settle(root.releaseRetainedMemory(retiredRecallObservationAllocationFee,
          "unallocated retired observation Work detached")) then
        state=State.Failed;publish();throw cleanupFailure
      retiredRecallObservationAllocationFee=0L
    // 子reader借用未退休时，原Session不能继续撤输入、issuer、writer或root。
    if recallInspection != null && !recallInspection.resourceObservation.closed then
      val closed=settle(recallInspection.closeOnOwnerThread())
      detachClosedRecallInspection()
      if !closed then
        state=State.Failed;publish()
        throw cleanupFailure
      publish()
    if recallInspectionAllocationFee != 0L then
      if !settle(root.releaseRetainedMemory(recallInspectionAllocationFee,"unallocated recall observation holder detached")) then
        state=State.Failed;publish()
        throw cleanupFailure
      recallInspectionAllocationFee=0L
    if pendingMainRead!=null then
      if !settle(pendingMainRead.retireOnWorker(this)) then
        publish()
        throw cleanupFailure
      pendingMainRead=null;pendingMainReadObservation=null
    if pendingMainReadFee!=0L then
      if !settle(root.releaseRetainedMemory(pendingMainReadFee,"schema6 pending Main successor detached")) then
        publish()
        throw cleanupFailure
      pendingMainReadFee=0L
    if mainRead!=null then
      if !settle(mainRead.retireOnWorker(this)) then
        publish()
        throw cleanupFailure // Main仍持有image时不能向下释放reader/root。
      mainRead=null
    if mainReadFee!=0L then
      if !settle(root.releaseRetainedMemory(mainReadFee,"schema6 Main backing detached")) then
        publish()
        throw cleanupFailure
      mainReadFee=0L
    if bootRecallCensus!=null then
      if !settle(bootRecallCensus.retireOnWorker()) then
        publish()
        throw cleanupFailure
      bootRecallCensus=null
    // 先退休新服务reader的真实DATA/借用，再允许释放writer、JDBC、inputs及Session holder。
    while !serviceReaders.isEmpty do
      if !settle(serviceReaders.getFirst.close()) then
        state=State.Failed;publish();throw cleanupFailure
    if retainedIssuer != null && settle {
      retainedIssuer.detachDbFirstReferencesForExit(this,full27,full27.sourceLimitProfile,ledger)
    } then retainedIssuer=null
    if retainedIssuer == null && retainedIssuerFee != 0L &&
      settle(root.releaseRetainedMemory(retainedIssuerFee,"schema6 retained issuer/core detached")) then retainedIssuerFee=0L
    if writerCoordinator != null && retainedIssuer == null && retainedIssuerFee == 0L then
      if !writerCoordinator.dbFirstWriterUnclaimed && !writerCoordinator.dbFirstWriterRetiredRecall && !writerReleaseAttempted && writerToken != null then
        writerReleaseAttempted=true
        settle {
          writerReleaseAudit=auditor.beginDbFirstWriterAudit(inputs,root,verified,scope,this,writerCoordinator)
          publish() // release audit在任何CALL或W release SQL前由Session登记。
          writerCoordinator.releaseOwnedDbFirst(writerToken,verified,scope,writerReleaseAudit)
        }
      if (writerCoordinator.dbFirstWriterUnclaimed || writerCoordinator.dbFirstWriterRetiredRecall) && !(barrier == null && barrierMayRetain) then
        writerAdmission=null;writerToken=null;writerCoordinator=null
    if writerCoordinator == null then
      previousWriterToken=null;previousWriterAdmission=null
      settle(retireSupersededWriterAudits())
      if pendingWriterPostAudit != null && settle(pendingWriterPostAudit.close()) then pendingWriterPostAudit=null
      if pendingWriterPreAudit != null && settle(pendingWriterPreAudit.close()) then pendingWriterPreAudit=null
      if writerReleaseAudit != null && settle(writerReleaseAudit.close()) then writerReleaseAudit=null
      if writerPostAudit != null && settle(writerPostAudit.close()) then writerPostAudit=null
      if writerPreAudit != null && settle(writerPreAudit.close()) then writerPreAudit=null
      if writerReleaseAudit == null && writerPostAudit == null && writerPreAudit == null &&
        pendingWriterPostAudit == null && pendingWriterPreAudit == null &&
        retiringWriterPostAudit == null && retiringWriterPreAudit == null && writerFee != 0L &&
        settle(root.releaseRetainedMemory(writerFee,"schema6 confirmed released writer references detached")) then writerFee=0L
    if gateHeld then settle(settleReadTransaction())
    databaseObservation=null
    if historicalRead != null && settle(historicalRead.close()) then historicalRead=null
    if forestRead != null && settle(forestRead.close()) then forestRead=null
    if historicalRead == null && forestRead == null && reader != null && settle(reader.close()) then reader=null
    if reader == null && writerCoordinator == null then
      qualification=null
      recallReadbackQualification=null
      if recallReadbackAudit!=null && settle(recallReadbackAudit.close()) then recallReadbackAudit=null
      if auditHolder != null && settle(auditHolder.close()) then auditHolder=null
    if connectionSuccessor!=null then
      settle { throw new IllegalStateException("actual service owner still retains inspection connection borrow") }
    if verified != null && connectionSuccessor==null then
      if !jdbcCloseAttempted then
        jdbcCloseAttempted=true
        if settle(verified.close()) then jdbcCloseReturned=true
      settle { jdbcClosedObserved=verified.connection.isClosed }
      if jdbcCloseReturned && jdbcClosedObserved && managedFirstBindingActor!=null &&
          managedFirstBindingActor.resourceObservation.started then
        settle(managedFirstBindingActor.afterJdbcClose())
      detachClosedManagedFirstBindingActor()
      if barrierAttempted then settle(refreshBarrierFacts())
      // close返回和isClosed是本地事实；不以此冒充server termination。
      if jdbcCloseReturned && jdbcClosedObserved && gateHeld && !gateLeaveAttempted then settle(leaveGate())
      if jdbcCloseReturned && jdbcClosedObserved && !gateHeld && reader == null && auditHolder == null && recallReadbackAudit==null && writerCoordinator == null &&
        writerPreAudit == null && writerPostAudit == null && writerReleaseAudit == null &&
        pendingWriterPreAudit == null && pendingWriterPostAudit == null &&
        retiringWriterPreAudit == null && retiringWriterPostAudit == null &&
        managedFirstBindingActor==null && managedFirstBindingFee==0L then
        barrier=null
        verified=null;scope=null;auditor=null;qualification=null;openHolder=null
    else if openHolder != null && !rawPending then
      // 历史partial owner保留成功close的raw引用仅为证据，closeReturned才是本地结算事实。
      openHolder=null
    if barrierAttempted then settle(refreshBarrierFacts())
    if rawPending then settle {
      throw new SQLNonTransientConnectionException("schema6 raw open cleanup still retains actual handle",openHolder.primaryFailure())
    }
    if verified == null && !rawPending && !gateHeld && reader == null && historicalRead == null && forestRead == null && auditHolder == null && recallReadbackAudit==null &&
      retainedIssuer == null && retainedIssuerFee == 0L && writerCoordinator == null && writerFee == 0L &&
      recallCleanupQualification==null && recallCleanupQualificationFee==0L &&
      recallNotAppliedQualification==null && recallNotAppliedQualificationFee==0L &&
      managedFirstBindingActor==null && managedFirstBindingFee==0L then
      if inputBorrow != null && settle(inputBorrow.close()) then inputBorrow=null
      if inputBorrow == null && !inputsClosed then
        val source=serviceSource
        if adapterCleanupOwned then
          if !adapterCleanupEdgesDetached then
            serviceAdapter.detachOwnedRecoveredServiceBeforeInputs(this)
            adapterCleanupEdgesDetached=true
          if ownedServiceControlFee!=0L &&
              settle(root.releaseRetainedMemory(ownedServiceControlFee,"actual owned service control registry detached")) then
            ownedServiceControlFee=0L
          if ownedServiceControlFee==0L && settle(inputs.closeFromTransferredSession(this)) then inputsClosed=true
        else if ownsInputs && source!=null && source.inputsCloseTransferred then
          // 绑定自身指向Session；先核全部SQL/reader退休并撤绑定，才能关闭实际输入。
          if !source.serviceCloseEdgesAreDetached then source.detachServiceCloseEdges(this)
          if settle(inputs.closeFromTransferredSession(this)) then inputsClosed=true
          else source.retainFailedServiceHolderRefund(this)
        else if !ownsInputs || settle(inputs.close()) then inputsClosed=true
      if inputsClosed then retainedRecallRetirementReceipt=null
      if inputsClosed && holderFee != 0L then
        val source=serviceSource
        if source!=null && !source.serviceCloseEdgesAreDetached then source.detachServiceCloseEdges(this)
        val inspectionSource=retiringInspectionSource
        if inspectionSource!=null then inspectionSource.detachRetiringInspectionHolder(this)
        if settle(root.releaseRetainedMemory(holderFee,"schema6 observed Session holder detached")) then
          holderFee=0L
          serviceSource=null;retiringInspectionSource=null;serviceAdapter=null
        else if inspectionSource!=null then inspectionSource.retainFailedInspectionHolder(this)
        else if source!=null then source.retainFailedServiceHolderRefund(this)
    if verified == null && !rawPending && !gateHeld && reader == null && historicalRead == null && forestRead == null && auditHolder == null && recallReadbackAudit==null &&
      retainedIssuer == null && retainedIssuerFee == 0L && writerCoordinator == null && writerFee == 0L &&
      recallCleanupQualification==null && recallCleanupQualificationFee==0L &&
      recallNotAppliedQualification==null && recallNotAppliedQualificationFee==0L &&
      inputBorrow == null && inputsClosed && holderFee == 0L &&
      managedFirstBindingActor==null && managedFirstBindingFee==0L &&
      ownedServiceMailbox==null && ownedServiceMailboxFee==0L && ownedServiceControlFee==0L then state=State.Closed
    else state=State.Failed
    publish()
    if cleanupFailure != null then throw cleanupFailure
    if state != State.Closed then throw new IllegalStateException("schema6 Session retains unsettled local resources",primary)
  override def toString: String = "MySqlDbFirstSession[database observation only; not Ready]"

private[mysql] object MySqlDbFirstSession:
  private val ManagedFirstBindingHolderBytes=524288L
  /** 原cause已归Session/Entry持有；仅普通观测失败附加诊断，Fatal不得降为普通失败。 */
  private[mysql] def observeRetainingRecallFailure(original:Throwable)(observe: => Unit):Unit =
    try observe
    catch case NonFatal(observationFailure)=>MySqlOwnedSessionLifecycle.suppress(original,observationFailure)
  /** 只保留数值 summary；短命 DATA 引用在子作用域退休前不得返回。 */
  private[mysql] final class HistoricalRead(root: MySqlFormat3ReadBudget, current: MySqlScriptSchemaInputs):
    private var inputs: MySqlScriptSchemaInputs=null
    private var borrow: MySqlDbFirstHistoryInputs.PayloadBorrow=null
    private var child: MySqlFormat3ReadBudget=null
    private var childFee=0L
    private var holderFee=HistoricalRead.HolderBytes
    private var summary: MySqlNormalRecallRecovery.PersistedPhaseSummary=null
    private var recallSummary: DbFirstRecallSummary=null
    private var checkpointSummary: DbFirstCheckpointSummary=null
    private var initialSummary: DbFirstInitialActivationSummary=null
    private var preparationSummary: DbFirstInitialPreparationSummary=null
    private var completed=false
    private var started=false
    private var acquired=false
    def acquire(history: Option[MySqlDbFirstHistoryInputs]): Unit =
      require(!acquired && holderFee != 0L,"historical read holder acquisition is one-shot")
      root.requireScriptDataInputs(current)
      acquired=true
      history.foreach { actual =>
        inputs=actual.inputs
        borrow=actual.borrow()
        borrow.requireInputs(root,inputs)
      }
    def result: MySqlNormalRecallRecovery.PersistedPhaseSummary =
      if !completed || summary == null then throw new IllegalStateException("persisted phase DATA observation incomplete")
      summary
    def dbFirstRecallSummary: DbFirstRecallSummary =
      if !completed || recallSummary == null then throw new IllegalStateException("DB-first recall DATA observation incomplete")
      recallSummary
    def dbFirstCheckpointSummary: DbFirstCheckpointSummary =
      if !completed || checkpointSummary == null then throw new IllegalStateException("DB-first checkpoint DATA observation incomplete")
      checkpointSummary
    def dbFirstInitialActivationSummary: DbFirstInitialActivationSummary =
      if !completed || initialSummary == null then throw new IllegalStateException("DB-first initial DATA observation incomplete")
      initialSummary
    def dbFirstInitialPreparationSummary: DbFirstInitialPreparationSummary =
      if !completed || preparationSummary == null then throw new IllegalStateException("DB-first preparation DATA observation incomplete")
      preparationSummary
    def validate(snapshot: MySqlScriptSchemaSnapshot): Unit =
      require(acquired && !started && holderFee != 0L,"historical phase observation is one-shot")
      started=true
      root.requireScriptDataInputs(current)
      require(snapshot != null && (snapshot.inputs eq current),"exact current Full27 image required")
      val imageOwner=snapshot.allocationBudget
      require(!imageOwner.isClosed && imageOwner.sharesAllocationLedger(root) &&
        ((imageOwner eq root) || imageOwner.isSnapshotReadChildOf(root)),
        "historical phase observation requires actual current root or issued snapshot child")
      val phaseSummary=MySqlNormalRecallRecovery.visitDbFirstPersistedPhases(snapshot, root, (row,reserve,cancelled) =>
        if borrow == null then throw new IllegalStateException("Recall phase DATA requires trusted original Full26 history inputs")
        borrow.requireInputs(root,inputs)
        root.reserveTemporaryMemory(256L,"historical comparison child BEFORE allocation")
        childFee=256L
        child=root.newCanonicalScope()
        var primary: Throwable=null
        try MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          child.withHistoricalPayloadInputs(inputs,borrow) {
            MySqlNormalRecallRecovery.verifyDbFirstPersistedPhase(row,reserve,root,child,current,inputs,cancelled)
          }
        }
        catch case cause: Throwable =>
          primary=cause
          throw cause
        finally
          try closeChild()
          catch case cleanup: Throwable =>
            if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      )
      val table="fp_recovery_operation"
      val shape=current.forTable(table)
      def index(column: String): Int =
        val result=shape.columns.indexWhere(_.name == column)
        if result < 0 then throw new IllegalStateException("Full27 operation column absent: "+column)
        result
      val stepIndex=index("step");val stateIndex=index("operation_state");val operationIndex=index("operation_uuid")
      var verified=0L;var reserved=0L;var cancelled=0L
      var checkpointVerified=0L;var checkpointReserved=0L;var checkpointCancelled=0L
      var initialVerified=0L;var initialReserved=0L;var initialCancelled=0L
      var preparationVerified=0L;var preparationReserved=0L;var preparationCancelled=0L
      val scopeIndex=index("scope_bytes")
      snapshot.relation(table).rows.foreach { row =>
        if row(stepIndex)==MySqlFormat3Cell.Text("REARM") &&
            MySqlDbFirstInitialPreparationPersistedRead.hasPreparationScope(row(scopeIndex)) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") =>
              verifySnapshotOperation(snapshot,row(operationIndex),preparation=true)
              preparationVerified=Math.addExact(preparationVerified,1L)
            case MySqlFormat3Cell.Text("RESERVED") => preparationReserved=Math.addExact(preparationReserved,1L)
            case MySqlFormat3Cell.Text("CANCELLED") => preparationCancelled=Math.addExact(preparationCancelled,1L)
            case _ => throw new IllegalStateException("DB-first preparation operation has unsupported persisted state")
        else if row(stepIndex) == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallRefusedStep) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") => verifySnapshotOperation(snapshot,row(operationIndex),refusal=true)
            // 未完成拒绝收据仍属原C责任；没有恢复owner时不可跳过或当成成功收回。
            case _ => throw new SQLDataException("DB-first recall refusal requires committed strict DATA or its explicit recovery owner")
        else if row(stepIndex) == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallJournalStep) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") => verifySnapshotOperation(snapshot,row(operationIndex),journal=true)
            // 尚无J RESERVED/CANCELLED的实际恢复owner；不能忽略或归入旧recall成功计数。
            case _ => throw new SQLDataException("DB-first recall journal requires committed strict DATA or its explicit recovery owner")
        else if row(stepIndex) == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") =>
              verifySnapshotOperation(snapshot,row(operationIndex),true)
              initialVerified=Math.addExact(initialVerified,1L)
            case MySqlFormat3Cell.Text("RESERVED") => initialReserved=Math.addExact(initialReserved,1L)
            case MySqlFormat3Cell.Text("CANCELLED") => initialCancelled=Math.addExact(initialCancelled,1L)
            case _ => throw new IllegalStateException("DB-first initial operation has unsupported persisted state")
        else if row(stepIndex) == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") =>
              verifySnapshotOperation(snapshot,row(operationIndex))
              checkpointVerified=Math.addExact(checkpointVerified,1L)
            case MySqlFormat3Cell.Text("RESERVED") => checkpointReserved=Math.addExact(checkpointReserved,1L)
            case MySqlFormat3Cell.Text("CANCELLED") => checkpointCancelled=Math.addExact(checkpointCancelled,1L)
            case _ => throw new IllegalStateException("DB-first checkpoint operation has unsupported persisted state")
        else if row(stepIndex) == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep) then
          row(stateIndex) match
            case MySqlFormat3Cell.Text("COMMITTED") =>
              verifySnapshotOperation(snapshot,row(operationIndex))
              // 只有完整DATA验证与实际child退休均成功，才计入成功。
              verified=Math.addExact(verified,1L)
            case MySqlFormat3Cell.Text("RESERVED") => reserved=Math.addExact(reserved,1L)
            case MySqlFormat3Cell.Text("CANCELLED") => cancelled=Math.addExact(cancelled,1L)
            case _ => throw new IllegalStateException("DB-first recall operation has unsupported persisted state")
      }
      if child != null || childFee != 0L then throw new IllegalStateException("DB-first recall retains comparison resources")
      val committedSummary=DbFirstRecallSummary(verified,reserved,cancelled)
      summary=phaseSummary
      recallSummary=committedSummary
      checkpointSummary=DbFirstCheckpointSummary(checkpointVerified,checkpointReserved,checkpointCancelled)
      initialSummary=DbFirstInitialActivationSummary(initialVerified,initialReserved,initialCancelled)
      preparationSummary=DbFirstInitialPreparationSummary(preparationVerified,preparationReserved,preparationCancelled)
      completed=true
    private def verifySnapshotOperation(snapshot: MySqlScriptSchemaSnapshot, operation: MySqlFormat3Cell, initial: Boolean=false,
        preparation:Boolean=false,journal:Boolean=false,refusal:Boolean=false): Unit =
      if child != null || childFee != 0L then throw new IllegalStateException("previous historical child still retained")
      root.reserveTemporaryMemory(256L,"DB-first recall comparison child BEFORE allocation")
      childFee=256L
      var primary: Throwable=null
      try
        child=root.newCanonicalScope()
        MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          val bytes=operation match
            case MySqlFormat3Cell.Binary(value) if value != null && value.length == 16 => value
            case _ => throw new IllegalStateException("DB-first recall requires exact persisted operation UUID")
          child.reserveTemporaryMemory(32L,"DB-first recall operation UUID BEFORE allocation")
          var most=0L;var least=0L;var offset=0
          while offset < 8 do
            most=(most << 8) | (bytes.byteAt(offset).toLong & 255L)
            least=(least << 8) | (bytes.byteAt(offset+8).toLong & 255L)
            offset+=1
          val operationUuid=new UUID(most,least)
          if refusal then MySqlDbFirstRecallRefusalPersistedRead.verifyCommitted(snapshot,operationUuid,root)
          else if journal then MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(snapshot,operationUuid,root)
          else if preparation then MySqlDbFirstInitialPreparationPersistedRead.verifyCommitted(snapshot,operationUuid,root)
          else if initial then MySqlDbFirstInitialActivationPersistedRead.verifyCommitted(snapshot,operationUuid,root)
          else MySqlDbFirstRecallPersistedRead.verifyCommitted(snapshot,operationUuid,root)
        }
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally
        try closeChild()
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
    private def closeChild(): Unit =
      if child != null then
        child.closeScopedReadBudget()
        child=null
      if childFee != 0L then
        root.releaseTemporaryMemory(childFee,"historical comparison child detached")
        childFee=0L
    def close(): Unit =
      closeChild()
      if borrow != null then
        borrow.close()
        borrow=null;inputs=null
      if holderFee != 0L then
        root.releaseRetainedMemory(holderFee,"historical read holder detached")
        holderFee=0L
  private[mysql] object HistoricalRead:
    val HolderBytes=1024L
  enum State:
    case Allocated,Initializing,DatabaseObserved,SqlWriterAcquired,SqlWriterRenewing,DbFirstRecallJournalPending,DbFirstRecallRefusalPending,DbFirstRecallPending,DbFirstCheckpointPending,DbFirstInitialActivationPending,DbFirstInitialPreparationPending,Failed,Closing,Closed
  final case class SqlWriterObservation(identity:MySqlOwnedIoIdentity,schema:String,bindingUuid:UUID,
      writerEpoch:Long,leaseUntil:java.time.LocalDateTime,state:State)
  /** 观测时刻的DATA分类，所有分支均不结算UNKNOWN或授予执行/confirmation资格。 */
  enum DbFirstFailedRecallImage:
    case CommittedTargetObserved,CommittedTargetWithRenewedLeaseObserved,BeforeObservedStillUnknown,DifferentObservedStillUnknown

  /** 原handoff下的完整正向DATA scalar；对象本身不是confirmation或Main读资格。 */
  final case class FailedRecallReadback(tag:IoOperationTag,podUuid:UUID,canonicalSha256:String,receiptSteps:Long)

  /** 仅原live SQL mutation未应用；不表示CANCELLED、Main解冻、反馈送达或restart身份可恢复。 */
  private[mysql] final class FailedRecallNotAppliedQualification private[MySqlDbFirstSession] (
      private val session:MySqlDbFirstSession,private val runner:MySqlDbFirstRecallTransaction,
      private val reservation:MySqlDbFirstCaptureReservation,private val window:MySqlAdmissionWindow,
      private val cause:Throwable):
    @volatile private var revoked=false
    private[MySqlDbFirstSession] def revoke():Unit = revoked=true
    private[MySqlDbFirstSession] def requireOriginal(actualSession:MySqlDbFirstSession,
        actualRunner:MySqlDbFirstRecallTransaction,actualReservation:MySqlDbFirstCaptureReservation,
        actualCause:Throwable):Unit =
      require(!revoked && window.remainingAt(System.nanoTime())>0L &&
        (session eq actualSession) && (runner eq actualRunner) && (reservation eq actualReservation) &&
        (actualRunner.reservation eq reservation) && cause!=null && (cause eq actualCause),
        "exact original live not-applied proof revoked, expired or mismatched")
  private[mysql] object FailedRecallNotAppliedQualification:
    val HolderBytes=512L

  /** 实际Session封存的只读资格；可跨线程核身份/原窗口，不持有JDBC操作能力。 */
  private[mysql] final class FailedRecallCleanupQualification private[MySqlDbFirstSession] (
      sessionInput:MySqlDbFirstSession,runnerInput:MySqlDbFirstRecallTransaction,
      reservationInput:MySqlDbFirstCaptureReservation,val readback:FailedRecallReadback,
      private val window:MySqlAdmissionWindow,private val cause:Throwable):
    @volatile private var session=sessionInput
    @volatile private var runner=runnerInput
    @volatile private var reservation=reservationInput
    @volatile private var revoked=false
    private[MySqlDbFirstSession] def revoke():Unit = revoked=true
    private[mysql] def requireDiagnosticOriginal(actual:MySqlDbFirstRecallTransaction,
        observed:FailedRecallReadback,originalCause:Throwable):Unit =
      require(actual!=null && (actual eq runner) && (actual.reservation eq reservation) &&
        (observed eq readback) && cause!=null && (originalCause eq cause),
        "exact original recall qualification diagnostic identity/cause required")
    private[mysql] def requireOriginal(actual:MySqlDbFirstRecallTransaction,
        observed:FailedRecallReadback,originalCause:Throwable):Unit =
      requireDiagnosticOriginal(actual,observed,originalCause)
      require(!revoked && window.remainingAt(System.nanoTime())>0L,
        "original recall qualification revoked or expired")
    private[mysql] def originalFailure:Throwable=cause
    private[mysql] def remainingAt(now:Long):Long=if revoked then 0L else window.remainingAt(now)
    private[mysql] def requireSession(actual:MySqlDbFirstSession):Unit =
      require(actual!=null && (actual eq session),"original qualification Session required")
    private[MySqlDbFirstSession] def detachOriginalGraph(actualSession:MySqlDbFirstSession,
        actualRunner:MySqlDbFirstRecallTransaction,observed:FailedRecallReadback,originalCause:Throwable):Unit =
      actualSession.requireThread()
      requireSession(actualSession)
      requireDiagnosticOriginal(actualRunner,observed,originalCause)
      // 实际原worker已经通过live preflight并消费原退休槽；撤权/撤引用本身不延长窗口。
      revoked=true
      session=null;runner=null;reservation=null
    private[mysql] def requireDetached(observed:FailedRecallReadback,originalCause:Throwable):Unit =
      require(revoked && session==null && runner==null && reservation==null &&
        (observed eq readback) && cause!=null && (cause eq originalCause),
        "actual original qualification graph must detach without losing readback/cause")
  private[mysql] object FailedRecallCleanupQualification:
    val HolderBytes=512L

  enum DbFirstSnapshotOutcome:
    case NotAttempted,FailedBeforeCommit,CommitUnknown,AcknowledgedPending,ReadbackVerifiedPending
  enum DbFirstInitialActivationOutcome:
    case NotAttempted,FailedBeforeCommit,CommitUnknown,AcknowledgedPending,ReadbackVerifiedPending
  enum DbFirstInitialPreparationOutcome:
    case NotAttempted,FailedBeforeCommit,CommitUnknown,AcknowledgedPending,ReadbackVerifiedPending
  /** 明确拒绝及未变更DATA事实；不是成功准备、commit ACK或Main恢复资格。 */
  final case class DbFirstInitialPreparationRejection(tag:IoOperationTag,podUuid:UUID,
      cause:MySqlDbFirstPreparationRejected,canonicalSha256:String)
  final case class DbFirstInitialPreparationObservation(tag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,readbackVerified:Boolean,canonicalSha256:String)
  type DbFirstRecallOutcome=DbFirstSnapshotOutcome
  val DbFirstRecallOutcome=DbFirstSnapshotOutcome
  trait SnapshotObservation:
    def tag:IoOperationTag
    def podUuid:UUID
    def commitAttempted:Boolean
    def acknowledged:Boolean
    def readbackVerified:Boolean
    def canonicalSha256:String
  /** 只证明原checkpoint的SQL事实；不授实体/ticket移除权限。 */
  final case class DbFirstCheckpointObservation(tag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,readbackVerified:Boolean,canonicalSha256:String) extends SnapshotObservation
  /** 只表示原SQL提交/读回事实，尚无Main活动发布资格。 */
  final case class DbFirstInitialActivationObservation(tag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,readbackVerified:Boolean,canonicalSha256:String)
  /** 仅数据库ACK/完整读回事实；不代表entity removal、ticket cleanup或business完成。 */
  final case class DbFirstRecallObservation(tag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,readbackVerified:Boolean,canonicalSha256:String) extends SnapshotObservation
  /** 只报告原J的SQL事实；不属于C的SnapshotObservation，不授予Main capture/freeze资格。 */
  /** 仅原次SQL事实；不是Main确认、终局反馈或Entry退休能力。 */
  final case class DbFirstRecallRefusalObservation(tag:IoOperationTag,journalTag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,sqlReadbackVerified:Boolean,canonicalSha256:String,
      routedTag:Option[IoOperationTag]=None):
    def routeTag:IoOperationTag=routedTag.getOrElse(journalTag)
  final case class DbFirstRecallJournalObservation(tag:IoOperationTag,originalTag:IoOperationTag,podUuid:UUID,
      commitAttempted:Boolean,acknowledged:Boolean,readbackVerified:Boolean,canonicalSha256:String)
  /** RESERVED和CANCELLED只分类为待处理，不证明absence、效果或恢复完成。 */
  final case class DbFirstCheckpointSummary(verifiedCommitted: Long,unresolvedReserved: Long,pendingCancelled: Long)
  final case class DbFirstRecallSummary(verifiedCommitted: Long,unresolvedReserved: Long,pendingCancelled: Long)
  final case class DbFirstInitialActivationSummary(verifiedCommitted: Long,unresolvedReserved: Long,pendingCancelled: Long)
  final case class DbFirstInitialPreparationSummary(verifiedCommitted: Long,unresolvedReserved: Long,pendingCancelled: Long)
  final case class DatabaseObservation(identity: MySqlOwnedIoIdentity,schema: String,bindingUuid: UUID,
      rowCount: Long,canonicalSha256: String,persistedPhases: MySqlNormalRecallRecovery.PersistedPhaseSummary,
      sourceForests: MySqlDbFirstForestRead.Summary,dbFirstRecalls: DbFirstRecallSummary,
      dbFirstInitialActivations: DbFirstInitialActivationSummary,dbFirstInitialPreparations: DbFirstInitialPreparationSummary,
      dbFirstCheckpoints: DbFirstCheckpointSummary)
  /** Immutable local-resource facts only; closed不证明远端session已终止。 */
  final case class ResourceObservation(state: State,closed: Boolean,connectionPending: Boolean,rawOpenPending: Boolean,
      readerPending: Boolean,gatePending: Boolean,inputsPending: Boolean,jdbcCloseReturned: Boolean,
      jdbcClosedObserved: Boolean,holderFee: Long,failure: Option[Throwable],cleanupFailure: Option[Throwable],auditPending: Boolean,retainedOwnerPending: Boolean,
      historicalPending: Boolean,forestPending: Boolean,
      barrierAttempted:Boolean,barrierHeld:Boolean,mayRetainServerLock:Boolean,
      lastBarrierObservation:Option[MySqlSessionBarrierCloseObservation],writerPending:Boolean,
      recallPending:Boolean=false,recallCommitAttempted:Boolean=false,recallAcknowledged:Boolean=false,
      recallOutcome:Option[DbFirstRecallOutcome]=None,initialPending:Boolean=false,
      initialOutcome:Option[DbFirstInitialActivationOutcome]=None,preparationPending:Boolean=false,
      preparationOutcome:Option[DbFirstInitialPreparationOutcome]=None,checkpointPending:Boolean=false,checkpointCommitAttempted:Boolean=false,checkpointAcknowledged:Boolean=false,
      checkpointOutcome:Option[DbFirstSnapshotOutcome]=None,
      recallInspectionAttempted:Boolean=false,recallInspection:Option[ResourceObservation]=None,
      recallInspectionAllocationFee:Long=0L,recallImageObservation:Option[DbFirstFailedRecallImage]=None,
      recallFailedSqlGateRetired:Boolean=false,recallPreparedReadback:Option[FailedRecallReadback]=None,
      recallFailedReadersRetired:Boolean=false,recallSqlRetirementAttempted:Boolean=false,
      recallFailedSqlResourcesRetired:Boolean=false,recallSqlResponsibilityReleased:Boolean=false,
      scopeConnectionGeneration:Long=0L,recallObservedLease:Option[MySqlAdmissionWindow]=None,
      recallLeaseObservationAttempted:Boolean=false,recallObservationAuditAttempted:Boolean=false,
      recallObservationAuditCompleted:Boolean=false,recallCleanupQualificationAttempted:Boolean=false,
      recallCleanupQualificationPending:Boolean=false,recallCleanupQualificationFee:Long=0L,
      recallBeforeLeaseObservationAttempted:Boolean=false,recallBeforeObservedLease:Option[MySqlAdmissionWindow]=None,
      recallBeforeAuditAttempted:Boolean=false,recallBeforeAuditCompleted:Boolean=false,
      recallNotAppliedQualificationAttempted:Boolean=false,recallNotAppliedQualificationPending:Boolean=false,
      recallNotAppliedQualificationFee:Long=0L,journalPending:Boolean=false,
      journalCommitAttempted:Boolean=false,journalAcknowledged:Boolean=false,
      journalOutcome:Option[DbFirstSnapshotOutcome]=None,refusalPending:Boolean=false,
      refusalCommitAttempted:Boolean=false,refusalAcknowledged:Boolean=false,
      refusalOutcome:Option[DbFirstSnapshotOutcome]=None,
      retiredRecallObservationAttempted:Boolean=false,
      retiredRecallObservation:Option[RetiredRecallObservationState]=None,
      retiredRecallObservationAllocationFee:Long=0L,
      retiredRecallWriterAttempted:Boolean=false,retiredRecallWriter:Option[RetiredRecallWriterState]=None,
      retiredRecallWriterAllocationFee:Long=0L,connectionTransferred:Boolean=false,
      adapterCleanupOwned:Boolean=false,ownedServiceMailboxPending:Boolean=false,
      ownedServiceMailboxFee:Long=0L,ownedServiceControlFee:Long=0L,
      managedFirstBindingActor:Option[MySqlDbFirstBootstrapActor.ResourceObservation]=None,
      managedFirstBindingFee:Long=0L)
  /** Immutable observation facts only; completed is neither a writer window nor ordinary success. */
  final case class RetiredRecallObservationState(started:Boolean,completed:Boolean,closed:Boolean,
      scopeConnectionGeneration:Long,barrierReadmitted:Boolean,readerPending:Boolean,gatePending:Boolean,
      auditPending:Boolean,historyPending:Boolean,forestPending:Boolean,holderFee:Long,
      observation:Option[DatabaseObservation],failure:Option[Throwable],cleanupFailure:Option[Throwable])

  /** 原fault owner登记的第二次独立观测；借同一真实barrier连接，绝不重置旧child的RR/初始化事实。 */
  private[mysql] final class RetiredRecallObservation private[MySqlDbFirstSession] (
      private val parent:MySqlDbFirstSession,private val observer:MySqlDbFirstSession,
      private val receipt:MySqlDbFirstRecallDropReceipt,private var holderFee:Long,
      private val writerOwner:RetiredRecallWriterRecovery=null):
    private val originalFailure=parent.primary
    private var started=false
    private var running=false
    private var completed=false
    private var closed=false
    private var closeAttempted=false
    private var first:Throwable=null
    private var cleanupFailure:Throwable=null
    private var barrierReadmitted=false
    private var audit:MySqlSchemaAuditor.DbFirstAudit=null
    private var qualification:VerifiedSchemaQualification=null
    private var reader:MySqlDbFirstSnapshotJdbc=null
    private var history:HistoricalRead=null
    private var forest:MySqlDbFirstForestRead=null
    private var observation:DatabaseObservation=null
    private var rollbackOnly:MySqlRollbackOnlyState=null
    private var gate:MySqlConnectionTransactionGate.Owner=null
    private var gateHeld=false
    private var gateLeaveAttempted=false
    private var oldIsolation:Option[Int]=None
    private var isolationAttempted=false
    private var isolationRestoreAttempted=false
    private var isolationRestored=false
    private var transactionAttempted=false
    private var rollbackAttempted=false
    private var rollbackReturned=false
    private var rollbackFailure:Throwable=null
    private var autoCommitRestoreAttempted=false
    private var autoCommitRestored=false

    def readerPending:Boolean=reader!=null
    def gatePending:Boolean=gateHeld
    def auditPending:Boolean=audit!=null
    def historyPending:Boolean=history!=null
    def forestPending:Boolean=forest!=null
    private[MySqlDbFirstSession] def completedForStoppedCleanup:Boolean=completed
    private[MySqlDbFirstSession] def runningForStoppedCleanup:Boolean=running
    private[MySqlDbFirstSession] def closeAttemptedForStoppedCleanup:Boolean=closeAttempted
    private[MySqlDbFirstSession] def hasStoppedReleaseOwners(actualWriter:RetiredRecallWriterRecovery,
        actualObserver:MySqlDbFirstSession,actualReceipt:MySqlDbFirstRecallDropReceipt):Boolean =
      (writerOwner eq actualWriter) && (observer eq actualObserver) && (receipt eq actualReceipt)
    private[MySqlDbFirstSession] def requireActiveStoppedReleaseRead():Unit =
      require(running && reader!=null && gateHeld && audit!=null && qualification!=null && rollbackOnly!=null,
        "actual active stopped-release Full27 read transaction required")
    def resourceObservation:RetiredRecallObservationState =
      RetiredRecallObservationState(started,completed,closed,observer.scopeConnectionGeneration,
        barrierReadmitted,readerPending,gatePending,auditPending,historyPending,forestPending,holderFee,
        Option(observation),Option(first),Option(cleanupFailure))

    private def registeredOwner:Boolean =
      if writerOwner==null then (parent.retiredRecallObservation eq this) && parent.retiredRecallObservationAttempted
      else writerOwner.ownsReadback(this,observer)

    private def requireRegistered():Unit =
      if writerOwner!=null && writerOwner.ownsStoppedReleaseReadback(this) then
        writerOwner.requireStoppedReleaseReadbackRegistration(this)
      else parent.requireOpen()
      require(!closed && !closeAttempted && holderFee==RetiredRecallObservation.HolderBytes &&
        registeredOwner &&
        (parent.retainedRecallRetirementReceipt eq receipt) && receipt.retainedEntryRetired &&
        parent.retainedRecallPayloadDetached && parent.retainedRecallFinishAttempted &&
        parent.recallTransaction==null && parent.recallCleanupQualification==null &&
        parent.recallCleanupQualificationFee==0L && originalFailure!=null && (parent.primary eq originalFailure) &&
        receipt.retainedConfirmation.retainedFailure.exists(_ eq originalFailure) &&
        parent.mainRead!=null && parent.pendingMainRead!=null && parent.recallPreparedReadback.nonEmpty &&
        (parent.recallInspection eq observer) && observer.inspectionOnly &&
        (observer.inspectionParent eq parent) && (observer.inputs eq parent.inputs) &&
        (observer.root eq parent.root) && (observer.identity eq parent.identity) &&
        observer.state==State.DatabaseObserved && observer.primary==null && observer.inputBorrow!=null &&
        !observer.closeAttempted && observer.verified!=null && observer.scope!=null && observer.barrier!=null &&
        observer.scopeConnectionGeneration>parent.identity.connectionGeneration,
        "actual privately registered retired recall Work/inspection/two-DATA/cause required")

    private[mysql] def ownsAudit(actualObserver:MySqlDbFirstSession,
        actual:MySqlSchemaAuditor.DbFirstAudit):Boolean =
      registeredOwner && (actualObserver eq observer) && actual!=null && (audit eq actual)

    private[mysql] def requireReaderSource(actualInputs:MySqlDbFirstSessionInputs,
        actualRoot:MySqlFormat3ReadBudget,actualIdentity:MySqlOwnedIoIdentity):Long =
      requireRegistered()
      val stoppedRelease=writerOwner!=null && writerOwner.ownsStoppedReleaseReadback(this)
      require(started && running && !completed && (first==null || stoppedRelease) && barrierReadmitted &&
        audit!=null && qualification!=null && gateHeld && gate!=null && rollbackOnly!=null &&
        (actualInputs eq parent.inputs) && (actualRoot eq parent.root) && (actualIdentity eq parent.identity),
        "actual retired observation audit/RR/source required; no metadata generation override")
      observer.requireDbFirstAuditSource(actualRoot,actualInputs,observer.verified,observer.scope)
      observer.scopeConnectionGeneration

    private[mysql] def requireImageIssuer(actualReader:MySqlDbFirstSnapshotJdbc,
        actualRoot:MySqlFormat3ReadBudget,actualInputs:MySqlDbFirstSessionInputs,
        actualIdentity:MySqlOwnedIoIdentity,actualVerified:VerifiedMySqlConnection,
        actualScope:MySqlAuditBootScope):Unit =
      requireReaderSource(actualInputs,actualRoot,actualIdentity)
      require(actualReader!=null && (reader eq actualReader) && (actualVerified eq observer.verified) &&
        (actualScope eq observer.scope),"actual registered retired observation reader/connection/scope required")

    private def requireCurrentQualification():Unit =
      requireRegistered()
      val expected=parent.inputs.auditExpectation
      require(qualification!=null && qualification.isCurrent(parent.identity.bootId,parent.identity.bootGeneration,
        observer.scopeConnectionGeneration,expected.serverUuid,expected.schema,expected.bindingUuid,
        MySqlSchemaAuditProfile.DbFirst1,MySqlExpectedPhysicalFormat.Format2,observer.verified,parent.thread,System.nanoTime()),
        "retired recall independent current audit expired or changed")

    private[MySqlDbFirstSession] def executeOnce():Unit =
      requireRegistered()
      require(!started && !running && !completed && first==null,"retired recall observation is one-shot, including failure")
      started=true;running=true
      var operationFailure:Throwable=null
      try
        parent.publish()
        if writerOwner!=null then writerOwner.requireReadbackWindow(this)
        observer.verified.readmitSessionBarrier(parent.inputs.auditExpectation,observer.scope)
        barrierReadmitted=true
        audit=observer.auditor.beginDbFirstAudit(parent.inputs,parent.root,observer.verified,observer.scope,observer)
        parent.publish() // 新partial audit已在Work登记；不覆盖旧readback audit。
        qualification=audit.readAndQualifyOnce()
        requireCurrentQualification()
        observer.verified.readmitSessionBarrier(parent.inputs.auditExpectation,observer.scope)
        val connection=observer.verified.connection
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("retired recall observation requires idle actual barrier connection")
        oldIsolation=Some(connection.getTransactionIsolation)
        rollbackOnly=new MySqlRollbackOnlyState
        MySqlConnectionTransactionGate.enter(connection,rollbackOnly) match
          case Some(cause)=>throw cause
          case None=>gateHeld=true
        parent.publish()
        gate=MySqlConnectionTransactionGate.currentOwner(connection,rollbackOnly)
        isolationAttempted=true
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
        transactionAttempted=true
        connection.setAutoCommit(false)
        if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
          throw new SQLNonTransientConnectionException("retired recall observation did not enter actual REPEATABLE READ")
        try reader=MySqlDbFirstSnapshotJdbc.createForRetiredObservation(this,parent.inputs,parent.root,parent.identity)
        catch case creation:MySqlDbFirstSnapshotJdbc.CreationFailure=>
          reader=creation.retainedHolder
          throw creation
        parent.publish()
        parent.root.reserveRetainedMemory(HistoricalRead.HolderBytes,"retired recall history holder BEFORE allocation")
        try history=new HistoricalRead(parent.root,parent.full27)
        catch case cause:Throwable=>
          try parent.root.releaseRetainedMemory(HistoricalRead.HolderBytes,"unallocated retired recall history holder")
          catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          throw cause
        parent.publish()
        history.acquire(parent.inputs.historicalInputs)
        parent.root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"retired recall forest holder BEFORE allocation")
        try forest=new MySqlDbFirstForestRead(parent.root,parent.full27,parent.inputs.auditExpectation.bindingUuid)
        catch case cause:Throwable=>
          try parent.root.releaseRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"unallocated retired recall forest holder")
          catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          throw cause
        parent.publish()
        val snapshot=reader.readLocked(observer.verified,observer.scope,qualification,gate,rollbackOnly)
        forest.validate(snapshot)
        val forestSummary=forest.result
        forest.close();forest=null
        history.validate(snapshot)
        val phases=history.result
        val recalls=history.dbFirstRecallSummary
        val initials=history.dbFirstInitialActivationSummary
        val preparations=history.dbFirstInitialPreparationSummary
        val checkpoints=history.dbFirstCheckpointSummary
        history.close();history=null
        // 原C的完整receipt以及旧已验证image到当前image的完整连续段，不以单行存在当作结算。
        MySqlDbFirstRecallPersistedRead.verifyCommitted(snapshot,receipt.tag.operationId,parent.root)
        if writerOwner==null then
          MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(snapshot,observer.snapshotOnOwnerThread,parent.root)
        else writerOwner.verifyWriterReadback(this,snapshot)
        settleReadTransaction()
        reader.detachCompletedReadAuthority()
        requireCurrentQualification()
        observer.verified.readmitSessionBarrier(parent.inputs.auditExpectation,observer.scope)
        if writerOwner!=null && writerOwner.ownsStoppedReleaseReadback(this) then
          writerOwner.requireStoppedReleaseReadbackRegistration(this)
        else
          parent.requireRetiredRecallHold(receipt)
          if writerOwner!=null then writerOwner.requireReadbackWindow(this)
        observation=DatabaseObservation(parent.identity,parent.full27.schema,parent.inputs.auditExpectation.bindingUuid,
          snapshot.rowCount,snapshot.canonicalSha256Hex,phases,forestSummary,recalls,initials,preparations,checkpoints)
        completed=true // 仅本次只读观测完成；不清primary、不变writer/两DATA、不发行Main结果。
      catch case cause:Throwable=>
        operationFailure=cause
        first=cause
        if gateHeld && rollbackOnly!=null then
          try rollbackOnly.mark(cause)
          catch case marking:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,marking)
        try settleReadTransaction()
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
      finally
        running=false
        try parent.publish()
        catch case publicationFailure:Throwable=>
          if operationFailure!=null then MySqlOwnedSessionLifecycle.suppress(operationFailure,publicationFailure)
          else
            first=publicationFailure
            throw publicationFailure

    private def settleReadTransaction():Unit =
      if !gateHeld then return
      val connection=observer.verified.connection
      if transactionAttempted then
        if !rollbackAttempted then
          rollbackAttempted=true
          try { connection.rollback();rollbackReturned=true }
          catch case cause:Throwable=>
            rollbackFailure=cause
            throw cause
        if !rollbackReturned then throw rollbackFailure
        if !autoCommitRestoreAttempted then
          autoCommitRestoreAttempted=true
          connection.setAutoCommit(true)
          if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("retired observation auto-commit restoration not observed")
          autoCommitRestored=true
        if !autoCommitRestored then throw new SQLNonTransientConnectionException("retired observation auto-commit restoration uncertain")
      if isolationAttempted then
        if !isolationRestoreAttempted then
          isolationRestoreAttempted=true
          connection.setTransactionIsolation(oldIsolation.get)
          if connection.getTransactionIsolation!=oldIsolation.get then
            throw new SQLNonTransientConnectionException("retired observation isolation restoration not observed")
          isolationRestored=true
        if !isolationRestored then throw new SQLNonTransientConnectionException("retired observation isolation restoration uncertain")
      if gateLeaveAttempted then throw new IllegalStateException("retired observation gate exit remains unsettled")
      gateLeaveAttempted=true
      MySqlConnectionTransactionGate.leave(connection,rollbackOnly)
      gateHeld=false;gate=null

    private[MySqlDbFirstSession] def currentSnapshotForWriter(actual:RetiredRecallWriterRecovery):MySqlScriptSchemaSnapshot =
      requireCompletedObservation()
      requireCurrentQualification()
      require(actual!=null && (parent.retiredRecallWriter eq actual),"actual registered writer recovery required")
      actual.requireObservation(this)
      reader.snapshot

    private[MySqlDbFirstSession] def requireActivePostSnapshot(actual:RetiredRecallWriterRecovery,
        snapshot:MySqlScriptSchemaSnapshot):Unit =
      requireRegistered()
      require(writerOwner!=null && (writerOwner eq actual) && started && running && !completed &&
        gateHeld && reader!=null && snapshot!=null && (reader.snapshot eq snapshot),
        "actual post-retirement Work must own this exact active reader snapshot")
      if writerOwner.ownsStoppedReleaseReadback(this) then
        writerOwner.requireStoppedReleasePostSnapshot(this)
      else require(first==null,"ordinary retired readback cannot carry a prior failure")

    private[MySqlDbFirstSession] def snapshotForPostRetirement(actual:RetiredRecallWriterRecovery):MySqlScriptSchemaSnapshot =
      requireCompletedObservation()
      require(writerOwner==null && actual!=null && (parent.retiredRecallWriter eq actual),
        "actual original current observation and registered writer owner required")
      actual.requirePreRetirementObservation(this)
      reader.snapshot

    private[MySqlDbFirstSession] def snapshotForAcquisition(actual:RetiredRecallWriterRecovery):MySqlScriptSchemaSnapshot =
      requireCompletedObservation()
      require(writerOwner!=null && (writerOwner eq actual),"actual registered post-retirement source required")
      actual.requireAcquisitionSource(this)
      reader.snapshot

    private[MySqlDbFirstSession] def snapshotForStoppedRelease(actual:RetiredRecallWriterRecovery):MySqlScriptSchemaSnapshot =
      parent.requireThread()
      require(writerOwner!=null && (writerOwner eq actual) && completed && first==null && cleanupFailure==null &&
        !closed && !closeAttempted && holderFee==RetiredRecallObservation.HolderBytes && reader!=null &&
        (parent.retiredRecallWriter eq actual),
        "actual retained complete post-acquire Full27 must remain available as stopped-release preimage")
      actual.requireStoppedReleasePreimage(this)
      reader.snapshot

    private[MySqlDbFirstSession] def snapshotForServiceMainRead(actual:RetiredRecallWriterRecovery):MySqlScriptSchemaSnapshot =
      requireCompletedObservation()
      require(writerOwner!=null && (writerOwner eq actual),"actual acquired service Full27 source required")
      actual.requireServiceMainReadSource(this)
      reader.snapshot

    private[MySqlDbFirstSession] def requireCompletedObservation():Unit =
      parent.requireThread()
      if first!=null then throw first
      requireRegistered()
      require(completed && !running && observation!=null && !gateHeld && gate==null && rollbackReturned &&
        autoCommitRestored && isolationRestored && history==null && forest==null,
        "retired recall independent complete Full27/SQL cleanup required; no retry or ordinary admission")

    private[MySqlDbFirstSession] def requireRetiredObservation():Unit =
      parent.requireThread()
      require(closed && closeAttempted && !running && holderFee==0L && reader==null && history==null &&
        forest==null && audit==null && qualification==null && observation==null && !gateHeld && gate==null &&
        first==null && cleanupFailure==null,"actual closed/refunded observation graph required")

    private[MySqlDbFirstSession] def closeOnOwnerThread():Unit =
      parent.requireThread()
      if closed then return
      require(!running,"retired recall observation is still executing")
      if closeAttempted then throw new IllegalStateException("retired observation cleanup incomplete; no automatic retry",cleanupFailure)
      closeAttempted=true
      try
        settleReadTransaction()
        if history!=null then { history.close();history=null }
        if forest!=null then { forest.close();forest=null }
        if reader!=null then { reader.close();reader=null }
        qualification=null
        if audit!=null then { audit.close();audit=null }
        observation=null
        parent.root.releaseRetainedMemory(holderFee,"retired recall observation Work graph detached")
        holderFee=0L;closed=true
      catch case cause:Throwable=>
        cleanupFailure=cause
        if first==null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
        throw cause

  final case class RetiredRecallWriterState(started:Boolean,originalSealed:Boolean,writerRetired:Boolean,
      holderFee:Long,coordinatorPending:Boolean,auditPending:Boolean,
      failure:Option[Throwable],mutation:Option[MySqlWriterFenceRow],
      postReadbackAttempted:Boolean=false,postReadbackAllocationFee:Long=0L,
      postReadback:Option[RetiredRecallObservationState]=None,
      acquireAttempted:Boolean=false,acquired:Boolean=false,acquireHolderFee:Long=0L,
      recoveredEpoch:Option[Long]=None,renewSequence:Long=0L,
      postAcquireAttempted:Boolean=false,postAcquireAllocationFee:Long=0L,
      postAcquire:Option[RetiredRecallObservationState]=None,postAcquireSequence:Long=0L,
      serviceAttempted:Boolean=false,serviceTransferred:Boolean=false,serviceAllocationFee:Long=0L,
      service:Option[ResourceObservation]=None,serviceMainAttempted:Boolean=false,serviceMainPrepared:Boolean=false,
      serviceMailboxFee:Long=0L,serviceControlFee:Long=0L,serviceMainControlStage:Option[String]=None,
      oldDataRetirementAttempted:Boolean=false,oldDataRetired:Boolean=false,
      oldReadersRetirementAttempted:Boolean=false,oldReadersRetired:Boolean=false,inputsCloseTransferred:Boolean=false,
      inspectionRetirementAttempted:Boolean=false,inspectionRetired:Boolean=false,retiredInspection:Option[ResourceObservation]=None,
       runtimeRegistrationAttempted:Boolean=false,runtimeRegistered:Boolean=false,
       oldReadersRetirementReturned:Boolean=false,transferredRecoveryHolderRefundAttempted:Boolean=false,
      transferredRecoveryHolderRefundReturned:Boolean=false,serviceInputBindingReturned:Boolean=false,
      stoppedReleaseAuditAttempted:Boolean=false,stoppedReleaseAuditPending:Boolean=false,
      stoppedReleaseAttempted:Boolean=false,stoppedReleaseReturned:Boolean=false,
      stoppedReleaseLockedBefore:Option[MySqlWriterFenceRow]=None,
      stoppedReleaseMutation:Option[MySqlWriterFenceMutation]=None,
      stoppedReleaseReadbackAttempted:Boolean=false,stoppedReleaseReadbackReturned:Boolean=false,
      stoppedReleaseReadbackAllocationFee:Long=0L,
      stoppedReleaseReadback:Option[RetiredRecallObservationState]=None,
      serviceAllocationRefundAttempted:Boolean=false,serviceAllocationRefundReturned:Boolean=false,
      acquireHolderRefundAttempted:Boolean=false,acquireHolderRefundReturned:Boolean=false)

  /** 只退休实际已终止的原writer；取得新writer、当前DATA与Main交接是后续独立阶段。 */
  private[mysql] final class RetiredRecallWriterRecovery private[MySqlDbFirstSession] (
      private val parent:MySqlDbFirstSession,private var observer:MySqlDbFirstSession,
      private val receipt:MySqlDbFirstRecallDropReceipt,
      private var current:RetiredRecallObservation,private var holderFee:Long):
    private val originalFailure=parent.primary
    private var started=false
    private var running=false
    private var originalSealed=false
    private var writerRetired=false
    private var first:Throwable=null
    private var closed=false
    private var closing=false
    private var coordinator:MySqlWriterFenceCoordinator=null
    private var retirementAudit:MySqlSchemaAuditor.DbFirstAudit=null
    private var mutation:MySqlWriterFenceMutation=null
    private var beforeMutation:MySqlWriterFenceRow=null
    private var postReadbackAttempted=false
    private var postReadbackAllocationFee=0L
    private var postRetirementReadback:RetiredRecallObservation=null
    private var acquireAttempted=false
    private var acquired=false
    private var acquireHolderFee=0L
    private var acquireHolderRefundAttempted=false
    private var acquireHolderRefundReturned=false
    private var detachingRetirementAudit=false
    private var token:MySqlWriterFenceToken=null
    private var admission:MySqlBusinessAdmission=null
    private var preAudit:MySqlSchemaAuditor.DbFirstAudit=null
    private var postAudit:MySqlSchemaAuditor.DbFirstAudit=null
    private var renewSequence=0L
    private var postAcquireAttempted=false
    private var postAcquireAllocationFee=0L
    private var postAcquireReadback:RetiredRecallObservation=null
    private var postAcquireToken:MySqlWriterFenceToken=null
    private var postAcquireAdmission:MySqlBusinessAdmission=null
    private var postAcquireSequence=0L
    private var serviceAttempted=false
    private var serviceAllocationFee=0L
    private var serviceAllocationRefundAttempted=false
    private var serviceAllocationRefundReturned=false
    private var serviceOwner:MySqlDbFirstSession=null
    private var serviceInputBindingReturned=false
    private var issuerTransferred=false
    private var serviceMainAttempted=false
    private var serviceMainPrepared=false
    private var serviceMailbox:MySqlOwnedMaintenanceMailbox=null
    private var serviceMailboxFee=0L
    private var serviceControlFee=0L
    private var servicePreparationAdapter:MySqlOwnedIoAdapter=null
    private var servicePreparationRegistered=false
    private var stoppedReleaseAuditAttempted=false
    private var stoppedReleaseAudit:MySqlSchemaAuditor.DbFirstAudit=null
    private var stoppedReleaseAttempted=false
    private var stoppedReleaseReturned=false
    private var stoppedReleaseLockedBefore:MySqlWriterFenceRow=null
    private var stoppedReleaseMutation:MySqlWriterFenceMutation=null
    private var stoppedReleaseReadbackAttempted=false
    private var stoppedReleaseReadbackReturned=false
    private var stoppedReleaseReadbackAllocationFee=0L
    private var stoppedReleaseReadback:RetiredRecallObservation=null
    private var closedStoppedReleaseReadback:Option[RetiredRecallObservationState]=None
    private var serviceIdentity:MySqlOwnedIoIdentity=null
    private var serviceTransferred=false
    private var serviceCloseEdgesDetached=false
    private[MySqlDbFirstSession] def serviceCloseEdgesAreDetached:Boolean=serviceCloseEdgesDetached
    private var oldDataRetirementAttempted=false
    private[MySqlDbFirstSession] var oldDataRetired=false
    private var runningServiceTransferAttempted=false
    private var runningServiceTransferred=false
    private var runtimeRegistrationAttempted=false
    private var runtimeRegistered=false
    private var inspectionRetirementAttempted=false
    private var inspectionRetired=false
    private var retiredInspection:Option[ResourceObservation]=None
    private var oldReadersRetirementAttempted=false
    private var oldReadersRetirementReturned=false
    private var oldReadersRetired=false
    private var transferredRecoveryHolderRefundAttempted=false
    private var transferredRecoveryHolderRefundReturned=false
    private var stoppedCleanup=false
    private[MySqlDbFirstSession] var inputsCloseTransferred=false
    private var closedCurrent:Option[RetiredRecallObservationState]=None
    private var closedPostRetirement:Option[RetiredRecallObservationState]=None
    private var closedPostAcquire:Option[RetiredRecallObservationState]=None
    private var closedService:Option[ResourceObservation]=None

    def readerPending:Boolean=postRetirementReadback!=null && postRetirementReadback.readerPending ||
      postAcquireReadback!=null && postAcquireReadback.readerPending ||
      stoppedReleaseReadback!=null && stoppedReleaseReadback.readerPending
    def gatePending:Boolean=postRetirementReadback!=null && postRetirementReadback.gatePending ||
      postAcquireReadback!=null && postAcquireReadback.gatePending ||
      stoppedReleaseReadback!=null && stoppedReleaseReadback.gatePending
    def auditPending:Boolean=retirementAudit!=null || preAudit!=null || postAudit!=null || stoppedReleaseAudit!=null ||
      serviceOwner!=null && (serviceOwner.writerPreAudit!=null || serviceOwner.writerPostAudit!=null ||
        serviceOwner.pendingWriterPreAudit!=null || serviceOwner.pendingWriterPostAudit!=null ||
        serviceOwner.retiringWriterPreAudit!=null || serviceOwner.retiringWriterPostAudit!=null ||
        serviceOwner.writerReleaseAudit!=null) ||
      postRetirementReadback!=null && postRetirementReadback.auditPending ||
      postAcquireReadback!=null && postAcquireReadback.auditPending ||
      stoppedReleaseReadback!=null && stoppedReleaseReadback.auditPending
    def historyPending:Boolean=postRetirementReadback!=null && postRetirementReadback.historyPending ||
      postAcquireReadback!=null && postAcquireReadback.historyPending ||
      stoppedReleaseReadback!=null && stoppedReleaseReadback.historyPending
    def forestPending:Boolean=postRetirementReadback!=null && postRetirementReadback.forestPending ||
      postAcquireReadback!=null && postAcquireReadback.forestPending ||
      stoppedReleaseReadback!=null && stoppedReleaseReadback.forestPending
    def resourceObservation:RetiredRecallWriterState =
      RetiredRecallWriterState(started,originalSealed,writerRetired,holderFee,coordinator!=null,
        auditPending,Option(first),Option(mutation).map(_.row),postReadbackAttempted,
        postReadbackAllocationFee,Option(postRetirementReadback).map(_.resourceObservation).orElse(closedPostRetirement),
        acquireAttempted,acquired,acquireHolderFee,Option(token).map(_.writerEpoch),renewSequence,
        postAcquireAttempted,postAcquireAllocationFee,Option(postAcquireReadback).map(_.resourceObservation).orElse(closedPostAcquire),postAcquireSequence,
        serviceAttempted,serviceTransferred,serviceAllocationFee,Option(serviceOwner).map(_.resourceObservation).orElse(closedService),
        serviceMainAttempted,serviceMainPrepared,serviceMailboxFee,serviceControlFee,
        Option(servicePreparationAdapter).flatMap(_.recoveredServiceMainStage(this)),oldDataRetirementAttempted,oldDataRetired,
        oldReadersRetirementAttempted,oldReadersRetired,inputsCloseTransferred,
        inspectionRetirementAttempted,inspectionRetired,retiredInspection,runtimeRegistrationAttempted,runtimeRegistered,
        oldReadersRetirementReturned,transferredRecoveryHolderRefundAttempted,transferredRecoveryHolderRefundReturned,
        serviceInputBindingReturned,stoppedReleaseAuditAttempted,stoppedReleaseAudit!=null,
        stoppedReleaseAttempted,stoppedReleaseReturned,Option(stoppedReleaseLockedBefore),Option(stoppedReleaseMutation),
        stoppedReleaseReadbackAttempted,stoppedReleaseReadbackReturned,stoppedReleaseReadbackAllocationFee,
        Option(stoppedReleaseReadback).map(_.resourceObservation).orElse(closedStoppedReleaseReadback),
        serviceAllocationRefundAttempted,serviceAllocationRefundReturned,
        acquireHolderRefundAttempted,acquireHolderRefundReturned)
    private def requireRegistered():Unit =
      parent.requireOpen()
      val connectionOwner=if observer!=null then observer else serviceOwner
      require((parent.retiredRecallWriter eq this) && parent.retiredRecallWriterAttempted &&
        holderFee==RetiredRecallWriterRecovery.HolderBytes && originalFailure!=null &&
        (parent.primary eq originalFailure) && (parent.retainedRecallRetirementReceipt eq receipt) &&
        receipt.retainedEntryRetired && parent.retainedRecallPayloadDetached &&
        parent.recallTransaction==null && parent.recallCleanupQualification==null &&
        parent.recallCleanupQualificationFee==0L &&
        (if oldDataRetired then retiredOldDataFacts else parent.mainRead!=null && parent.pendingMainRead!=null) &&
        (parent.retiredRecallObservation eq current) && (parent.recallInspection eq observer) &&
        (if observer!=null then (observer.inspectionParent eq parent) && observer.state==State.DatabaseObserved && observer.primary==null
          else inspectionRetired && inspectionRetirementAttempted && retiredInspection.exists(s=>s.closed && s.holderFee==0L)) &&
        connectionOwner!=null && connectionOwner.verified!=null && connectionOwner.scope!=null && connectionOwner.barrier!=null &&
        parent.verified!=null && parent.scope!=null && parent.barrier!=null &&
        parent.jdbcCloseAttempted && parent.jdbcCloseReturned && parent.jdbcClosedObserved &&
        connectionOwner.scopeConnectionGeneration>parent.scopeConnectionGeneration &&
        connectionOwner.barrier.actualConnectionId!=parent.barrier.actualConnectionId &&
        connectionOwner.barrier.connectionGenerationId!=parent.barrier.connectionGenerationId,
        "actual registered retired writer owner/terminated original/current connection responsibility required")
      MySqlSessionBarrierTargetRules.requireSame(connectionOwner.barrier.namespace,parent.barrier.namespace)

    private[MySqlDbFirstSession] def requireObservation(actual:RetiredRecallObservation):Unit =
      requireRegistered()
      require((current eq actual) && running && !writerRetired && first==null,
        "same registered pre-retirement current Full27 Work required")

    private[mysql] def requireOriginalCoordinator(actual:MySqlWriterFenceCoordinator,
        token:MySqlWriterFenceToken,admission:MySqlBusinessAdmission):Unit =
      requireRegistered()
      require(running && !originalSealed && first==null && actual!=null && (parent.writerCoordinator eq actual) &&
        token!=null && (parent.writerToken eq token) && admission!=null && (parent.writerAdmission eq admission) &&
        (admission.token eq token) && (token.verified eq parent.verified) && (token.scope eq parent.scope),
        "actual terminated original coordinator/token/admission required for permanent sealing")

    private[mysql] def requireRetirementCoordinator(actual:MySqlWriterFenceCoordinator,
        verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit =
      requireRegistered()
      require(running && originalSealed && !writerRetired && first==null && actual!=null &&
        (coordinator eq actual) && (verified eq observer.verified) && (scope eq observer.scope),
        "actual new-scope registered retirement coordinator required")
      current.currentSnapshotForWriter(this) // 当前完整DATA/审计证明；不把旧业务UNKNOWN伪装writer Unknown。

    private[mysql] def ownsCoordinator(actualObserver:MySqlDbFirstSession,
        actual:MySqlWriterFenceCoordinator):Boolean =
      (parent.retiredRecallWriter eq this) && (actualObserver eq observer) && actual!=null &&
        (coordinator eq actual) && ((running || closing) && first==null && (!writerRetired || acquireAttempted) ||
          closing && stoppedCleanup && parent.closeAttempted && parent.state==State.Closing &&
            servicePreparationAdapter!=null && (parent.retiredRecallCleanupAdapter eq servicePreparationAdapter) &&
            serviceAttempted && !serviceTransferred && acquired && writerRetired && !running &&
            token!=null && admission!=null && (admission.token eq token) && observer.connectionSuccessor==null)
    private[mysql] def ownsAudit(actualObserver:MySqlDbFirstSession,
        actual:MySqlSchemaAuditor.DbFirstAudit):Boolean =
      (parent.retiredRecallWriter eq this) && (actualObserver eq observer) && actual!=null &&
        ((retirementAudit eq actual) || (preAudit eq actual) || (postAudit eq actual) ||
          (stoppedReleaseAudit eq actual) ||
          serviceOwner!=null && serviceOwner.ownsInheritedServiceAudit(this,actual))

    private[MySqlDbFirstSession] def ownsReadback(actual:RetiredRecallObservation,
        actualObserver:MySqlDbFirstSession):Boolean =
      (parent.retiredRecallWriter eq this) && (actualObserver eq observer) && actual!=null &&
        !closed && ((postRetirementReadback eq actual) && postReadbackAttempted ||
          (postAcquireReadback eq actual) && postAcquireAttempted ||
          (stoppedReleaseReadback eq actual) && stoppedReleaseReadbackAttempted)

    private[MySqlDbFirstSession] def ownsReadbackAudit(actualObserver:MySqlDbFirstSession,
        actual:MySqlSchemaAuditor.DbFirstAudit):Boolean =
      postRetirementReadback!=null && postRetirementReadback.ownsAudit(actualObserver,actual) ||
        postAcquireReadback!=null && postAcquireReadback.ownsAudit(actualObserver,actual) ||
        stoppedReleaseReadback!=null && stoppedReleaseReadback.ownsAudit(actualObserver,actual)

    private def stoppedPreReleaseObservationsRetired:Boolean =
      def returned(value:Option[RetiredRecallObservationState]):Boolean=value.exists(state=>
        state.closed && state.holderFee==0L && !state.readerPending && !state.gatePending &&
          !state.auditPending && !state.historyPending && !state.forestPending &&
          state.failure.isEmpty && state.cleanupFailure.isEmpty)
      current==null && parent.retiredRecallObservation==null && postRetirementReadback==null &&
        returned(closedCurrent) && returned(closedPostRetirement)

    private def retireStoppedPreReleaseObservations():Unit =
      require(current!=null && (parent.retiredRecallObservation eq current) &&
        current.resourceObservation.completed && current.resourceObservation.failure.isEmpty &&
        current.resourceObservation.cleanupFailure.isEmpty && !current.resourceObservation.closed &&
        !current.closeAttemptedForStoppedCleanup &&
        postRetirementReadback!=null && postRetirementReadback.resourceObservation.completed &&
        postRetirementReadback.resourceObservation.failure.isEmpty &&
        postRetirementReadback.resourceObservation.cleanupFailure.isEmpty &&
        !postRetirementReadback.resourceObservation.closed &&
        !postRetirementReadback.closeAttemptedForStoppedCleanup &&
        closedCurrent.isEmpty && closedPostRetirement.isEmpty,
        "actual completed pre-release observations must remain owned until stop cleanup")
      val retiredReadback=postRetirementReadback
      retiredReadback.closeOnOwnerThread();retiredReadback.requireRetiredObservation()
      closedPostRetirement=Some(retiredReadback.resourceObservation)
      postRetirementReadback=null
      val originalCurrent=current
      originalCurrent.closeOnOwnerThread();originalCurrent.requireRetiredObservation()
      closedCurrent=Some(originalCurrent.resourceObservation)
      require(parent.retiredRecallObservation eq originalCurrent,
        "actual original current observation must remain registered through close")
      parent.closedRetiredRecallObservation=closedCurrent
      parent.retiredRecallObservation=null
      current=null
      require(stoppedPreReleaseObservationsRetired,
        "actual old current/post-retirement Full27 graphs and fees must return before stopped release")

    private[mysql] def requireStoppedWriterRelease(actualCoordinator:MySqlWriterFenceCoordinator,
        actualToken:MySqlWriterFenceToken,actualVerified:VerifiedMySqlConnection,
        actualScope:MySqlAuditBootScope,actualAudit:MySqlSchemaAuditor.DbFirstAudit):Unit =
      parent.requireThread()
      require(stoppedCleanup && closing && parent.closeAttempted && parent.state==State.Closing &&
        servicePreparationAdapter!=null && (parent.retiredRecallCleanupAdapter eq servicePreparationAdapter) &&
        (parent.retiredRecallWriter eq this) && (parent.retainedRecallRetirementReceipt eq receipt) &&
        receipt.retainedEntryRetired && parent.retainedRecallPayloadDetached &&
        parent.retainedRecallFinishAttempted && (parent.primary eq originalFailure) && originalFailure!=null &&
        serviceAttempted && !serviceTransferred && acquired && !running && writerRetired &&
        !runningServiceTransferred,
        "actual stopped untransferred recovered writer release source required")
      require(actualCoordinator!=null && (coordinator eq actualCoordinator) && actualToken!=null &&
        (token eq actualToken) && admission!=null && (admission.token eq actualToken) &&
        actualVerified!=null && (actualVerified eq observer.verified) && actualScope!=null &&
        (actualScope eq observer.scope) && (actualToken.verified eq actualVerified) &&
        (actualToken.scope eq actualScope) && (actualAudit!=null) && (stoppedReleaseAudit eq actualAudit) &&
        stoppedReleaseAuditAttempted && !stoppedReleaseAttempted && !stoppedReleaseReturned &&
        stoppedReleaseLockedBefore==null && stoppedReleaseMutation==null &&
        postAcquireReadback!=null && postAcquireReadback.resourceObservation.completed &&
        postAcquireReadback.resourceObservation.observation.nonEmpty &&
        postAcquireReadback.resourceObservation.failure.isEmpty &&
        postAcquireReadback.resourceObservation.cleanupFailure.isEmpty &&
        !postAcquireReadback.resourceObservation.closed &&
        !postAcquireReadback.closeAttemptedForStoppedCleanup &&
        stoppedPreReleaseObservationsRetired &&
        postAcquireSequence==renewSequence && observer.connectionSuccessor==null &&
        observer.openHolder!=null && observer.scope!=null && observer.verified!=null &&
        observer.writerCoordinator==null && !oldDataRetired &&
        (!oldReadersRetirementAttempted || oldReadersRetired),
        "same stopped actual token/connection/completed pre-release Full27 and unconsumed release required")
      if serviceOwner!=null then requireStoppedServiceShell(serviceOwner,closingAllowed=false)

    private[mysql] def recordStoppedReleaseBefore(actualCoordinator:MySqlWriterFenceCoordinator,
        current:MySqlWriterFenceRow):Unit =
      parent.requireThread()
      require(stoppedCleanup && closing && parent.closeAttempted && parent.state==State.Closing &&
        (coordinator eq actualCoordinator) && actualCoordinator!=null && stoppedReleaseAudit!=null &&
        stoppedReleaseAuditAttempted && !stoppedReleaseAttempted && !stoppedReleaseReturned &&
        stoppedReleaseLockedBefore==null && stoppedReleaseMutation==null &&
        token!=null && current!=null && current.bindingUuid==token.bindingUuid &&
        current.epoch==token.writerEpoch && current.ownerBootUuid.contains(token.bootUuid) &&
        current.leaseUntil==token.leaseUntil && serviceAttempted && !serviceTransferred &&
        acquired && writerRetired && postAcquireReadback!=null &&
        postAcquireReadback.resourceObservation.completed &&
        postAcquireReadback.resourceObservation.observation.nonEmpty &&
        stoppedPreReleaseObservationsRetired && !oldDataRetired &&
        (serviceOwner==null || (serviceOwner.serviceSource eq this)),
        "actual locked current W immediately before the one stopped release required")
      stoppedReleaseAttempted=true
      stoppedReleaseLockedBefore=current

    private[mysql] def requireStoppedReleaseAuditDetach(actualCoordinator:MySqlWriterFenceCoordinator,
        actualAudit:MySqlSchemaAuditor.DbFirstAudit):Unit =
      parent.requireThread()
      val completedReadback=stoppedReleaseReadbackReturned &&
        (stoppedReleaseReadback!=null && stoppedReleaseReadback.completedForStoppedCleanup &&
          !stoppedReleaseReadback.runningForStoppedCleanup ||
          closedStoppedReleaseReadback.exists(value=>value.closed && value.completed &&
            !value.readerPending && !value.gatePending && !value.auditPending &&
            !value.historyPending && !value.forestPending && value.holderFee==0L))
      require(stoppedCleanup && closing && (coordinator eq actualCoordinator) && actualCoordinator!=null &&
        stoppedReleaseReturned && completedReadback &&
        (stoppedReleaseAudit eq actualAudit) && actualAudit!=null && stoppedReleaseAttempted &&
        stoppedReleaseLockedBefore!=null && stoppedReleaseMutation!=null,
        "actual stopped release and complete Full27 acknowledgement required before release audit detach")

    private[mysql] def requireStoppedServiceShell(actual:MySqlDbFirstSession,closingAllowed:Boolean):Unit =
      parent.requireThread()
      require(stoppedCleanup && closing && parent.closeAttempted && parent.state==State.Closing &&
        (parent.retiredRecallWriter eq this) && serviceAttempted && !serviceTransferred &&
        acquired && (!closingAllowed || stoppedReleaseReturned && stoppedReleaseReadbackReturned) &&
        (serviceOwner eq actual) && actual!=null && (serviceIdentity eq actual.identity) &&
        (actual.serviceSource eq this),
        "actual stopped empty service shell route and confirmed release/readback required")
      actual.requireStoppedEmptyServiceShell(this,parent.inputs,parent.root,serviceIdentity)

    private[mysql] def ownsStoppedReleaseReadback(actual:RetiredRecallObservation):Boolean =
      (parent.retiredRecallWriter eq this) && actual!=null && (stoppedReleaseReadback eq actual) &&
        stoppedReleaseReadbackAttempted

    private[mysql] def ownsStoppedReleaseObservationFor(actualObserver:MySqlDbFirstSession):Boolean =
      (parent.retiredRecallWriter eq this) && (observer eq actualObserver) && stoppedCleanup && closing &&
        parent.closeAttempted && parent.state==State.Closing && stoppedReleaseAttempted &&
        stoppedReleaseReturned && stoppedReleaseReadback!=null && stoppedReleaseReadback.runningForStoppedCleanup &&
        stoppedReleaseReadbackAttempted && !stoppedReleaseReadback.completedForStoppedCleanup &&
        observer.connectionSuccessor==null

    private[mysql] def requireStoppedReleaseReadbackRegistration(actual:RetiredRecallObservation):Unit =
      parent.requireThread()
      val preimageState=if postAcquireReadback==null then None else Some(postAcquireReadback.resourceObservation)
      val readbackState=if actual==null then None else Some(actual.resourceObservation)
      require(ownsStoppedReleaseReadback(actual) && stoppedCleanup && closing && parent.closeAttempted &&
        parent.state==State.Closing && stoppedReleaseAttempted && stoppedReleaseReturned &&
        stoppedReleaseLockedBefore!=null && stoppedReleaseMutation!=null && !oldDataRetired &&
        (parent.retiredRecallWriter eq this) && (parent.retainedRecallRetirementReceipt eq receipt) &&
        receipt.retainedEntryRetired && parent.retainedRecallPayloadDetached &&
        parent.retainedRecallFinishAttempted && (parent.primary eq originalFailure) &&
        parent.mainRead!=null && parent.pendingMainRead!=null && (parent.recallInspection eq observer) &&
        observer.inspectionOnly && (observer.inspectionParent eq parent) && observer.inputBorrow!=null &&
        observer.state==State.DatabaseObserved && observer.primary==null &&
        observer.verified!=null && observer.scope!=null && observer.openHolder!=null &&
        observer.connectionSuccessor==null && postAcquireReadback!=null &&
        preimageState.exists(state=>state.completed && state.observation.nonEmpty && state.failure.isEmpty &&
          state.cleanupFailure.isEmpty && !state.closed) && !postAcquireReadback.closeAttemptedForStoppedCleanup &&
        readbackState.exists(state => !state.closed && state.failure.isEmpty && state.cleanupFailure.isEmpty &&
          state.holderFee==RetiredRecallObservation.HolderBytes) &&
        !actual.closeAttemptedForStoppedCleanup &&
        actual.hasStoppedReleaseOwners(this,observer,receipt),
        "actual one-shot stopped-release Full27 readback must remain bound to retained owners")

    private[mysql] def requireStoppedReleaseObservationRegistration(actualObserver:MySqlDbFirstSession):Unit =
      require(ownsStoppedReleaseObservationFor(actualObserver) && (actualObserver eq observer),
        "actual stopped-release reader must use the retained inspection Session")

    private[mysql] def requireStoppedReleasePreimage(actual:RetiredRecallObservation):Unit =
      parent.requireThread()
      require(stoppedCleanup && closing && stoppedReleaseReturned && (postAcquireReadback eq actual) &&
        actual.resourceObservation.completed && actual.resourceObservation.failure.isEmpty &&
        actual.resourceObservation.cleanupFailure.isEmpty && !actual.resourceObservation.closed &&
        !actual.closeAttemptedForStoppedCleanup && actual.resourceObservation.observation.nonEmpty && actual.readerPending &&
        (parent.retiredRecallWriter eq this) && (parent.retainedRecallRetirementReceipt eq receipt),
        "actual retained post-acquire Full27 preimage for stopped release required")

    private[mysql] def requireStoppedReleasePostSnapshot(actual:RetiredRecallObservation):Unit =
      requireStoppedReleaseReadbackRegistration(actual)
      actual.requireActiveStoppedReleaseRead()

    private[mysql] def requireStoppedReleaseReadbackCompleted(actual:RetiredRecallObservation):Unit =
      parent.requireThread()
      val state=actual.resourceObservation
      require(ownsStoppedReleaseReadback(actual) && stoppedCleanup && closing &&
        stoppedReleaseReturned && state.completed && !actual.runningForStoppedCleanup && state.failure.isEmpty &&
        state.cleanupFailure.isEmpty && state.observation.nonEmpty && state.readerPending &&
        !state.gatePending && stoppedReleaseMutation!=null && stoppedReleaseLockedBefore!=null,
        "actual completed single stopped-release Full27 observation required")

    private[MySqlDbFirstSession] def requirePreRetirementObservation(actual:RetiredRecallObservation):Unit =
      requireRegistered()
      require((current eq actual) && running && writerRetired && first==null && mutation!=null && beforeMutation!=null,
        "actual retained pre-retirement Full27 and confirmed mutation required")

    private[MySqlDbFirstSession] def requireReadbackWindow(actual:RetiredRecallObservation):Unit =
      if ownsStoppedReleaseReadback(actual) then
        requireStoppedReleaseReadbackRegistration(actual)
        require(actual.runningForStoppedCleanup && writerRetired && stoppedReleaseReturned && stoppedReleaseMutation!=null &&
          stoppedReleaseLockedBefore!=null && token!=null && admission!=null && (admission.token eq token),
          "single active stop-release Full27 readback with actual W release result required")
      else
        requireRegistered()
        require(ownsReadback(actual,observer) && running && writerRetired && first==null && mutation!=null,
          "actual running registered writer readback phase required")
        if actual eq postRetirementReadback then
          require(!acquireAttempted && !acquired,"post-retirement observation cannot execute after acquire")
        else
          require((actual eq postAcquireReadback) && acquired && token!=null && admission!=null &&
            (postAcquireToken eq token) && (postAcquireAdmission eq admission) && postAcquireSequence==renewSequence,
            "post-acquire read must pin the actual current token/admission/renew sequence")
          requireRecoveredFreshness()

    private[MySqlDbFirstSession] def requireAcquisitionSource(actual:RetiredRecallObservation):Unit =
      requireRegistered()
      require((postRetirementReadback eq actual) && postAcquireReadback!=null,
        "exact retained second Full27 is required as post-acquire source")
      requireReadbackWindow(postAcquireReadback)

    private[MySqlDbFirstSession] def verifyWriterReadback(actual:RetiredRecallObservation,
        after:MySqlScriptSchemaSnapshot):Unit =
      requireReadbackWindow(actual)
      actual.requireActivePostSnapshot(this,after)
      val before=if actual eq stoppedReleaseReadback then postAcquireReadback.snapshotForStoppedRelease(this)
        else if actual eq postRetirementReadback then current.snapshotForPostRetirement(this)
        else postRetirementReadback.snapshotForAcquisition(this)
      val connection=MySqlFormat3ConnectionObservation.from(observer.verified.identity,"REPEATABLE-READ")
      val scope=MySqlFormat3ObservationScope(observer.scope.bootUuid,observer.scope.bootGeneration,observer.scope.connectionGeneration)
      require(after!=null && (before.inputs eq parent.full27) && (after.inputs eq parent.full27) &&
        before.schema==parent.full27.schema && after.schema==before.schema &&
        before.connection==connection && after.connection==connection &&
        before.observationScope==scope && after.observationScope==scope &&
        before.maxAllowedPacketBytes==after.maxAllowedPacketBytes &&
        !before.allocationBudget.isClosed && !after.allocationBudget.isClosed &&
        before.allocationBudget.isSnapshotReadChildOf(parent.root) &&
        after.allocationBudget.isSnapshotReadChildOf(parent.root) &&
        (before.allocationBudget ne after.allocationBudget) &&
        before.allocationBudget.sharesAllocationLedger(parent.root) &&
        after.allocationBudget.sharesAllocationLedger(parent.root),
        "actual independent live Full27 images on the exact inspection connection/scope/root required")
      if actual eq stoppedReleaseReadback then
        require(stoppedReleaseLockedBefore!=null && stoppedReleaseMutation!=null &&
          stoppedReleaseReturned && (postAcquireReadback ne null),
          "actual locked new-W release preimage and returned SQL mutation required")
        MySqlRetiredRecallWriterReadback.requireExactRelations(before.relations,after.relations,parent.full27,
          stoppedReleaseLockedBefore,stoppedReleaseMutation)
      else if actual eq postRetirementReadback then
        require(beforeMutation!=null,"actual SQL-locked retirement preimage required")
        MySqlRetiredRecallWriterReadback.requireExactRelations(before.relations,after.relations,parent.full27,
          beforeMutation,mutation)
      else
        // 预期W仅由实际已ACK退休行和当前实际token约束，不伪造acquire mutation或snapshot。
        val expected=mutation.row.copy(epoch=postAcquireToken.writerEpoch,
          ownerBootUuid=Some(postAcquireToken.bootUuid),leaseUntil=postAcquireToken.leaseUntil)
        MySqlRetiredRecallWriterReadback.requireExactAcquisitionRelations(before.relations,after.relations,
          parent.full27,mutation.row,expected)

    private[mysql] def requireCurrentWriterRow(actual:MySqlWriterFenceRow):Unit =
      requireRetirementCoordinator(coordinator,observer.verified,observer.scope)
      val snapshot=current.currentSnapshotForWriter(this)
      val rows=snapshot.relation("fp_runtime_writer").rows
      require(rows.size==1,"retired writer requires complete current W singleton")
      def v(name:String)=MySqlDbFirstRecallMaterializer.value(rows.head,"fp_runtime_writer",name,parent.full27)
      def uuid(name:String,id:UUID)=MySqlDbFirstRecallMaterializer.uuidEquals(v(name),id)
      def optionalUuid(name:String,id:Option[UUID]):Boolean=id match
        case Some(value)=>uuid(name,value)
        case None=>v(name)==MySqlFormat3Cell.SqlNull
      def fingerprint:Boolean=actual.lastCommitFingerprint match
        case None=>v("last_commit_fingerprint")==MySqlFormat3Cell.SqlNull
        case Some(bytes)=>v("last_commit_fingerprint") match
          case MySqlFormat3Cell.Binary(value) if value!=null && value.length==32L && bytes.size==32=>
            bytes.indices.forall(i=>bytes(i)==value.byteAt(i))
          case _=>false
      val original=parent.writerToken
      require(actual!=null && original!=null && actual.bindingUuid==original.bindingUuid &&
        actual.epoch==original.writerEpoch && actual.ownerBootUuid.contains(parent.identity.bootId) &&
        actual.leaseUntil==original.leaseUntil && uuid("binding_uuid",actual.bindingUuid) &&
        uuid("owner_boot_uuid",parent.identity.bootId) && v("writer_epoch")==MySqlFormat3Cell.SignedInteger(actual.epoch) &&
        v("lease_until")==MySqlFormat3Cell.DateTime(actual.leaseUntil) &&
        optionalUuid("last_commit_operation_uuid",actual.lastCommitOperationUuid) && fingerprint,
        "current locked W differs from actual terminated owner/full verified receipt slot")
      require(beforeMutation==null,"retired writer CAS preimage is one-shot")
      beforeMutation=actual // 实际SQL锁定前像；固定holder预收其metadata，不由DTO提供。

    private[MySqlDbFirstSession] def executeOnce():Unit =
      requireRegistered()
      require(!started && first==null,"retired writer owner is one-shot, including failure")
      started=true;running=true;parent.publish()
      try
        current.requireCompletedObservation()
        observer.verified.readmitSessionBarrier(parent.inputs.auditExpectation,observer.scope)
        require(parent.verified.connection.isClosed,"original writer connection is not actually locally closed")
        current.currentSnapshotForWriter(this)
        val leasePolicy=parent.writerCoordinator.sealTerminatedDbFirstRecall(this,parent.writerToken,parent.writerAdmission)
        originalSealed=true
        parent.publish()
        coordinator=new MySqlWriterFenceCoordinator(parent.inputs.auditExpectation,observer.auditor,observer.scope,leasePolicy)
        parent.publish()
        retirementAudit=observer.auditor.beginDbFirstWriterAudit(parent.inputs,parent.root,
          observer.verified,observer.scope,observer,coordinator)
        parent.publish()
        mutation=coordinator.retireTerminatedDbFirstRecall(this,observer.verified,observer.scope,retirementAudit)
        writerRetired=true // 仅W精确退休ACK/行读回；不发行acquire/post-audit/Main交接。
        postReadbackAttempted=true
        parent.publish()
        parent.root.reserveRetainedMemory(RetiredRecallObservation.HolderBytes,
          "post-retirement independent Full27 observation Work BEFORE allocation")
        postReadbackAllocationFee=RetiredRecallObservation.HolderBytes
        postRetirementReadback=new RetiredRecallObservation(parent,observer,receipt,
          RetiredRecallObservation.HolderBytes,this)
        postReadbackAllocationFee=0L
        parent.publish() // 第二实际owner登记后才建立其独立audit/RR/reader；不重置第一实例。
        postRetirementReadback.executeOnce()
      catch case cause:Throwable=>
        first=cause
        throw cause
      finally { running=false;parent.publish() }

    private[mysql] def requireAuditDetach(actual:MySqlWriterFenceCoordinator,
        audit:MySqlSchemaAuditor.DbFirstAudit):Unit =
      parent.requireThread()
      require((closing || detachingRetirementAudit) && !closed && first==null &&
        (parent.retiredRecallWriter eq this) && writerRetired && mutation!=null &&
        mutation.row.ownerBootUuid.isEmpty && (coordinator eq actual) && (retirementAudit eq audit),
        "actual completed retired writer audit-detach owner required")
      if detachingRetirementAudit then
        require(running && acquireAttempted && !acquired && token==null && admission==null &&
          acquireHolderFee==RetiredRecallWriterRecovery.AcquireHolderBytes &&
          postRetirementReadback!=null,"actual prepaid pre-acquire detach phase required")
        postRetirementReadback.requireCompletedObservation()

    private def acquireWriterOnce():Unit =
      requireRetiredHold()
      require(!acquireAttempted && !acquired && token==null && admission==null && acquireHolderFee==0L,
        "actual recovered writer acquisition is one-shot, including failure")
      acquireAttempted=true;running=true;parent.publish()
      try
        parent.root.reserveRetainedMemory(RetiredRecallWriterRecovery.AcquireHolderBytes,
          "recovered writer token/admission/renewal/release slots BEFORE allocation and SQL")
        acquireHolderFee=RetiredRecallWriterRecovery.AcquireHolderBytes
        observer.verified.readmitSessionBarrier(parent.inputs.auditExpectation,observer.scope)
        detachingRetirementAudit=true
        parent.publish()
        coordinator.detachTerminatedDbFirstRecallAudit(this,retirementAudit)
        detachingRetirementAudit=false
        // coordinator已撤旧audit消费边；实际close失败仍留本字段并停止，不继续acquire。
        retirementAudit.close();retirementAudit=null
        preAudit=observer.auditor.beginDbFirstWriterAudit(parent.inputs,parent.root,
          observer.verified,observer.scope,observer,coordinator)
        parent.publish()
        token=coordinator.acquireOwnedDbFirst(observer.verified,observer.scope,preAudit)
        parent.publish()
        require(token.writerEpoch==Math.incrementExact(mutation.row.epoch) &&
          token.bindingUuid==mutation.row.bindingUuid && token.bootUuid==parent.identity.bootId &&
          (token.verified eq observer.verified) && (token.scope eq observer.scope),
          "actual recovered empty-W acquire changed exact binding/new epoch/scope")
        postAudit=observer.auditor.beginDbFirstWriterAudit(parent.inputs,parent.root,
          observer.verified,observer.scope,observer,coordinator)
        parent.publish()
        admission=coordinator.auditOwnedDbFirstPostAcquire(token,postAudit)
        requireRecoveredFreshness()
        acquired=true // 仅服务writer资格；独立完整读回及Main交接不能被此旗标替代。
        postAcquireAttempted=true;parent.publish()
        parent.root.reserveRetainedMemory(RetiredRecallObservation.HolderBytes,
          "post-acquire independent Full27 Work and pinned authority slots BEFORE allocation")
        postAcquireAllocationFee=RetiredRecallObservation.HolderBytes
        postAcquireToken=token;postAcquireAdmission=admission;postAcquireSequence=renewSequence
        postAcquireReadback=new RetiredRecallObservation(parent,observer,receipt,
          RetiredRecallObservation.HolderBytes,this)
        postAcquireAllocationFee=0L;parent.publish()
        postAcquireReadback.executeOnce()
        requireReadbackWindow(postAcquireReadback)
        // 完整读回/cleanup及窗口再次核验后撤临时authority引用；保留实际image，不阻止下一轮旧audit退休。
        postAcquireToken=null;postAcquireAdmission=null
        prepareServiceOwnerOnce()
      catch case cause:Throwable=>
        first=cause
        throw cause
      finally { running=false;parent.publish() }

    private[mysql] def requireServiceInputBinding(actual:MySqlDbFirstSession,
        actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
        actualIdentity:MySqlOwnedIoIdentity):Unit =
      parent.requireThread()
      require((parent.retiredRecallWriter eq this) && serviceAttempted && actual!=null &&
        (serviceOwner eq actual) && (actual.serviceSource eq this) &&
        (actualInputs eq parent.inputs) && (actualRoot eq parent.root) && !actualRoot.isClosed &&
        (actual.inputs eq actualInputs) && (actual.root eq actualRoot) &&
        actualIdentity!=null && (serviceIdentity eq actualIdentity) && (actual.identity eq actualIdentity) &&
        !actual.inspectionOnly && actual.inspectionParent==null &&
        actualIdentity.bootId==parent.identity.bootId && actualIdentity.bootGeneration==parent.identity.bootGeneration &&
        actualIdentity.connectionGeneration==actual.scopeConnectionGeneration &&
        actualIdentity.connectionGeneration>parent.identity.connectionGeneration,
        "actual privately registered new service Session/root/true SQL generation required")

    private[MySqlDbFirstSession] def requireInspectionReaderRetirement(actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require((closing || oldReadersRetirementAttempted && oldDataRetired) && !closed &&
        (first==null || stoppedCleanup && oldReadersRetired) &&
        (parent.retiredRecallWriter eq this) && (observer eq actual) && serviceTransferred && serviceOwner!=null &&
        (actual.connectionSuccessor eq serviceOwner) && postAcquireReadback==null && postRetirementReadback==null &&
        current==null && parent.retiredRecallObservation==null,
        "actual service close owner must first retire all three observation consumers")

    private[MySqlDbFirstSession] def detachServiceCloseEdges(actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(closing && !closed && (first==null || stoppedCleanup && oldReadersRetired) && !serviceCloseEdgesDetached &&
        (parent.retiredRecallWriter eq this) && (serviceOwner eq actual) &&
        (if observer!=null then (observer.connectionSuccessor eq actual) && observer.reader==null &&
          observer.historicalRead==null && observer.forestRead==null && observer.auditHolder==null &&
          observer.recallReadbackAudit==null && !observer.gateHeld else inspectionRetired && parent.recallInspection==null) &&
        postAcquireReadback==null && postRetirementReadback==null && current==null && parent.retiredRecallObservation==null,
        "actual all-reader/audit retirement required before service close edge detachment")
      actual.requireServiceResourcesBeforeInputClose(this)
      // actual inputs runtime proof必须在控制登记仍存在时撤除；最后close owner另行保留。
      parent.inputs.detachClosedServiceOwner(this,actual,parent.root,serviceIdentity)
      if servicePreparationAdapter!=null then
        servicePreparationAdapter.detachRecoveredDbFirstService(this,actual,servicePreparationRegistered)
        servicePreparationAdapter=null;servicePreparationRegistered=false
      if serviceControlFee!=0L then
        parent.root.releaseRetainedMemory(serviceControlFee,"actual service Main control registry graph detached")
        serviceControlFee=0L
      if observer!=null then
        observer.verified=null;observer.scope=null;observer.auditor=null;observer.qualification=null
        observer.openHolder=null;observer.barrier=null;observer.barrierHeld=false;observer.barrierMayRetain=false
        observer.connectionSuccessor=null
      serviceOwner=null
      serviceCloseEdgesDetached=true // 后续只有本次清理栈持新Session；退款失败会立即重新保管。

    private[MySqlDbFirstSession] def retainFailedServiceHolderRefund(actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(closing && !closed && serviceCloseEdgesDetached && serviceOwner==null && actual!=null &&
        (actual.serviceSource eq this) && actual.holderFee==MySqlDbFirstSession.HolderBytes,
        "failed refund must retain the exact actual service holder")
      serviceOwner=actual

    /** 唯一实际Recovery/旧issuer/core/新Session关系；metadata或替换core不能签交接。 */
    private[mysql] def requireServiceIssuerTransfer(adapter:MySqlOwnedIoAdapter,
        actualIssuer:MySqlOwnedRetainedPlanIssuer,actualCore:MySqlRetainedPlanReservationCore,
        previous:MySqlOwnedIoIdentity,next:MySqlOwnedIoIdentity):Unit =
      requireRetiredHold()
      require(serviceTransferred && serviceOwner!=null && !issuerTransferred && !running && !closing && first==null &&
        (parent.retainedIssuer eq actualIssuer) && parent.retainedIssuerFee==RetainedIssuerBytes &&
        serviceOwner.retainedIssuer==null && serviceOwner.retainedIssuerFee==0L &&
        (previous eq parent.identity) && (next eq serviceIdentity) && (next eq serviceOwner.identity),
        "actual registered old issuer and prepared new service Session required once")
      actualIssuer.requireRecoveryCore(actualCore)
      adapter.requireRetiredRecallIssuerTransfer(parent,receipt,actualIssuer)
      postAcquireReadback.requireCompletedObservation()
      serviceOwner.currentSqlWriterFreshness()

    private[MySqlDbFirstSession] def transferRetainedIssuerOnce(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if issuerTransferred then return
      require(serviceOwner!=null && serviceTransferred,"actual service SQL ownership must precede issuer transfer")
      val actual=parent.retainedIssuer
      require(actual!=null,"original retained issuer missing before service transfer")
      if actual.transferDbFirstServiceOwner(this,adapter,parent,serviceOwner,parent.full27,
          parent.full27.sourceLimitProfile,parent.ledger,parent.identity,serviceIdentity) then
        // 原issuer/core及2048责任整体转移；不新建空core，不重置serial/Main/历史或退款。
        serviceOwner.retainedIssuer=actual;serviceOwner.retainedIssuerFee=parent.retainedIssuerFee
        parent.retainedIssuer=null;parent.retainedIssuerFee=0L
        issuerTransferred=true
        serviceOwner.publish();parent.publish()

    private[mysql] def requireServiceMainReadConstruction(previous:MySqlDbFirstMainReadBacking,
        actualSession:MySqlDbFirstSession,adapter:MySqlOwnedIoAdapter,epoch:Long):Unit =
      requireRetiredHold()
      require(issuerTransferred && serviceTransferred && serviceMainAttempted && !serviceMainPrepared &&
        !closing && !running && (serviceOwner eq actualSession) && (parent.mainRead eq previous) &&
        serviceOwner.mainRead==null && serviceOwner.mainReadFee==MySqlDbFirstMainReadBacking.HolderBytes &&
        epoch==parent.pendingMainRead.repositoryEpoch && epoch>previous.repositoryEpoch &&
        epoch==adapter.retiredRecallMainReadEpoch(parent,receipt,serviceOwner.retainedIssuer),
        "actual old/current DATA, prepaid new service backing and exact accepted epoch required")
      previous.requireBinding(parent,adapter)
      parent.pendingMainRead.requireUnpublishedData(parent,parent.recallPreparedReadback.get.canonicalSha256)
      serviceOwner.currentSqlWriterFreshness()

    private[MySqlDbFirstSession] def requireServiceMainReadSource(actual:RetiredRecallObservation):Unit =
      requireRetiredHold()
      require(issuerTransferred && serviceMainAttempted && !serviceMainPrepared && !closing && !running &&
        (postAcquireReadback eq actual) && serviceOwner!=null && serviceOwner.mainRead!=null,
        "actual third completed Full27 and registered new service backing required")
      serviceOwner.currentSqlWriterFreshness()

    private[mysql] def requireActualServiceMainRead(actual:MySqlDbFirstMainReadBacking,
        actualSession:MySqlDbFirstSession,actualIdentity:MySqlOwnedIoIdentity):Unit =
      requireRetiredHold()
      require(issuerTransferred && serviceMainAttempted && !closing && !running &&
        (serviceOwner eq actualSession) && (serviceIdentity eq actualIdentity) &&
        actual!=null && (serviceOwner.mainRead eq actual) &&
        serviceOwner.mainReadFee==MySqlDbFirstMainReadBacking.HolderBytes,
        "actual privately registered new service Main DATA required")
      serviceOwner.currentSqlWriterFreshness()

    private[mysql] def currentFreshnessForServiceMailbox(actual:MySqlOwnedMaintenanceMailbox,
        actualIdentity:MySqlOwnedIoIdentity):MySqlOwnedAdmissionFreshness =
      requireRetiredHold()
      require(issuerTransferred && serviceMainAttempted && !closing && !running && actual!=null &&
        (serviceMailbox eq actual) && serviceMailboxFee==1024L && (serviceIdentity eq actualIdentity),
        "actual privately registered new service mailbox required")
      serviceOwner.currentSqlWriterFreshness()

    private[mysql] def requireServicePreparation(adapter:MySqlOwnedIoAdapter,
        previous:MySqlDbFirstMainReadBacking,next:MySqlDbFirstMainReadBacking,
        mailbox:MySqlOwnedMaintenanceMailbox,nextIdentity:MySqlOwnedIoIdentity,
        actualReceipt:MySqlDbFirstRecallDropReceipt,epoch:Long):Unit =
      requireRetiredHold()
      require(serviceMainPrepared && serviceOwner!=null && serviceMailbox!=null && serviceControlFee==1024L &&
        (parent.mainRead eq previous) && (serviceOwner.mainRead eq next) && (serviceMailbox eq mailbox) &&
        (serviceIdentity eq nextIdentity) && (receipt eq actualReceipt) && epoch==next.repositoryEpoch &&
        (servicePreparationAdapter eq adapter) && !servicePreparationRegistered &&
        serviceMailbox.poll.isCurrent && serviceMailbox.poll.latest.exists(value=>
          value.connectionGeneration==serviceIdentity.connectionGeneration &&
          value.renewSequence==serviceOwner.writerRenewSequence),
        "actual complete new service DATA/mailbox required before private registry publication")
      adapter.requireRetiredRecallIssuerTransfer(parent,receipt,serviceOwner.retainedIssuer)
      requireActualServiceMainRead(serviceOwner.mainRead,serviceOwner,serviceIdentity)

    private[mysql] def requireServiceClosingRegistry(adapter:MySqlOwnedIoAdapter):Unit =
      parent.requireThread()
      require(closing && !closed && (servicePreparationAdapter eq adapter),"actual service closing registry owner required")

    /** 仅固定Adapter导航引用；停止权限仍由Adapter的真实stop/ACK gate裁定。 */
    private[mysql] def bindStoppedCleanupAdapter(adapter:MySqlOwnedIoAdapter,
        expectedReceipt:MySqlDbFirstRecallDropReceipt):Unit =
      parent.requireThread()
      require(adapter!=null && expectedReceipt!=null && (receipt eq expectedReceipt) &&
        (parent.retainedRecallRetirementReceipt eq expectedReceipt) && (parent.retiredRecallWriter eq this) &&
        (parent.retiredRecallCleanupAdapter eq adapter) && !closed && !closing,
        "actual retained Recovery/receipt adapter navigation required")
      require(servicePreparationAdapter==null || (servicePreparationAdapter eq adapter),
        "Recovery cleanup navigation is bound to one actual adapter")
      servicePreparationAdapter=adapter

    /** 只供Adapter已确认的停止清理；保留原cause，不走健康路径的first guard。 */
    private[mysql] def requireStoppedRecoveryOwner(adapter:MySqlOwnedIoAdapter,
        expectedReceipt:MySqlDbFirstRecallDropReceipt):Unit =
      parent.requireThread()
      val transferredService=serviceAttempted && serviceTransferred && serviceOwner!=null &&
        (serviceOwner.serviceSource eq this) && (serviceIdentity eq serviceOwner.identity) &&
        !serviceOwner.closeAttempted && !runningServiceTransferred
      val untransferredRelease=serviceAttempted && !serviceTransferred && acquired && token!=null &&
        admission!=null && (admission.token eq token) && !oldDataRetired && !runningServiceTransferred &&
        postAcquireReadback!=null && postAcquireReadback.resourceObservation.completed &&
        postAcquireReadback.resourceObservation.observation.nonEmpty &&
        postAcquireReadback.resourceObservation.failure.isEmpty &&
        postAcquireReadback.resourceObservation.cleanupFailure.isEmpty &&
        !postAcquireReadback.resourceObservation.closed && !postAcquireReadback.closeAttemptedForStoppedCleanup &&
        (serviceOwner==null || (serviceOwner.serviceSource eq this))
      require(adapter!=null && expectedReceipt!=null && (receipt eq expectedReceipt) &&
        (parent.retainedRecallRetirementReceipt eq expectedReceipt) &&
        (parent.retiredRecallCleanupAdapter eq adapter) && (servicePreparationAdapter eq adapter) &&
        (parent.retiredRecallWriter eq this) && parent.retiredRecallWriterAttempted &&
        holderFee==RetiredRecallWriterRecovery.HolderBytes && originalFailure!=null &&
        (parent.primary eq originalFailure) && expectedReceipt.retainedEntryRetired &&
        parent.retainedRecallPayloadDetached && parent.retainedRecallFinishAttempted &&
        parent.recallTransaction==null && parent.recallCleanupQualification==null &&
        parent.recallCleanupQualificationFee==0L && parent.recallPreparedReadback.nonEmpty &&
        started && originalSealed && writerRetired && !running && !closing && !closed &&
        mutation!=null && mutation.row.ownerBootUuid.isEmpty &&
        (!oldReadersRetirementAttempted || oldReadersRetired) &&
        parent.closeAttempted && parent.state==State.Closing && (transferredService || untransferredRelease),
        "actual stopped Recovery source/service owner and same retained receipt required")
      if untransferredRelease && serviceOwner!=null then
        serviceOwner.requireStoppedEmptyServiceShell(this,parent.inputs,parent.root,serviceIdentity)

    private[mysql] def requireClosedServicePreparation(adapter:MySqlOwnedIoAdapter,
        actualService:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(closing && !closed && (servicePreparationAdapter eq adapter) && (serviceOwner eq actualService) &&
        serviceMailbox==null && serviceMailboxFee==0L,"actual service mailbox retirement/owner required before registry detach")
      actualService.requireServiceResourcesBeforeInputClose(this)

    private[MySqlDbFirstSession] def prepareServiceMainReadOnce(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if !issuerTransferred || serviceMainAttempted then return
      require(serviceOwner!=null && serviceOwner.mainRead==null && serviceOwner.mainReadFee==0L,
        "actual new service Session has not allocated Main DATA")
      require(adapter!=null && (parent.retiredRecallCleanupAdapter eq adapter) &&
        (servicePreparationAdapter==null || (servicePreparationAdapter eq adapter)),
        "actual retained stop-navigation adapter required before service Main allocation")
      servicePreparationAdapter=adapter
      serviceOwner.currentSqlWriterFreshness()
      val epoch=adapter.retiredRecallMainReadEpoch(parent,receipt,serviceOwner.retainedIssuer)
      serviceMainAttempted=true;parent.publish()
      parent.root.reserveRetainedMemory(MySqlDbFirstMainReadBacking.HolderBytes,
        "actual new service Main/source backing BEFORE allocation")
      serviceOwner.mainReadFee=MySqlDbFirstMainReadBacking.HolderBytes
      serviceOwner.mainRead=parent.mainRead.newUnclaimedServiceSuccessor(this,serviceOwner,adapter,epoch)
      val snapshot=postAcquireReadback.snapshotForServiceMainRead(this)
      serviceOwner.mainRead.assemble(snapshot,parent.full27,parent.root)
      serviceOwner.mainRead.requireUnpublishedServiceData(this,serviceOwner,serviceIdentity,snapshot.canonicalSha256Hex)
      parent.root.reserveRetainedMemory(1024L,"actual new service mailbox and bounded latest windows BEFORE allocation")
      serviceMailboxFee=1024L
      serviceMailbox=new MySqlOwnedMaintenanceMailbox(serviceIdentity)
      serviceMailbox.initializeRecoveredDbFirst(this)
      serviceMainPrepared=true
      parent.root.reserveRetainedMemory(1024L,"actual Main service transfer/control ACK graph BEFORE allocation")
      serviceControlFee=1024L
      adapter.registerRecoveredDbFirstService(this,parent.mainRead,serviceOwner.mainRead,
        serviceMailbox,serviceIdentity,receipt,epoch)
      servicePreparationRegistered=true
      serviceOwner.publish();parent.publish() // 只登记待交接责任；旧两DATA、current backing及ordinary hold不变。

    private def prepareServiceOwnerOnce():Unit =
      requireRegistered()
      require(running && acquired && first==null && !serviceAttempted && serviceOwner==null &&
        postAcquireReadback!=null && postAcquireToken==null && postAcquireAdmission==null,
        "actual completed post-acquire owner required before one-shot service transfer")
      postAcquireReadback.requireCompletedObservation()
      requireRecoveredFreshness()
      serviceAttempted=true;parent.publish()
      parent.root.reserveRetainedMemory(MySqlDbFirstSession.HolderBytes,
        "actual new service Session/true IO identity BEFORE allocation or transfer")
      serviceAllocationFee=MySqlDbFirstSession.HolderBytes
      serviceIdentity=MySqlOwnedIoIdentity(parent.identity.bootId,parent.identity.bootGeneration,observer.scopeConnectionGeneration)
      serviceOwner=new MySqlDbFirstSession(parent.inputs,parent.root,serviceIdentity,parent.thread,
        parent.auditMaxAge,parent.full27,parent.ledger,MySqlDbFirstSession.HolderBytes,
        serviceSource=this)
      serviceAllocationFee=0L;parent.publish()
      parent.inputs.bindServiceOwner(this,serviceOwner,parent.root,serviceIdentity)
      serviceInputBindingReturned=true
      serviceOwner.inputBorrow=parent.inputs.borrowForReader()
      // 新owner已可达且输入已实际绑定；旧observer仅留audit/reader引用，不再close或维护SQL资源。
      require(observer.connectionSuccessor==null && observer.writerCoordinator==null &&
        observer.openHolder!=null && observer.verified!=null && observer.scope!=null && observer.barrier!=null,
        "actual unused inspection connection responsibility required")
      requireRecoveredFreshness()
      val serviceObservation=postAcquireReadback.resourceObservation.observation.get.copy(identity=serviceIdentity)
      require(serviceOwner.state==State.Allocated && !serviceOwner.closeAttempted &&
        serviceOwner.openHolder==null && serviceOwner.verified==null && serviceOwner.scope==null &&
        serviceOwner.writerCoordinator==null && serviceOwner.writerToken==null && serviceOwner.writerAdmission==null &&
        serviceOwner.mainRead==null && serviceOwner.pendingMainRead==null && serviceOwner.inputBorrow!=null,
        "actual empty service shell and completed preflight required before SQL owner transfer")
      val transferredOpenHolder=observer.openHolder
      val transferredVerified=observer.verified
      val transferredScope=observer.scope
      val transferredAuditor=observer.auditor
      val transferredBarrier=observer.barrier
      val transferredCoordinator=coordinator
      val transferredToken=token
      val transferredAdmission=admission
      val transferredPreAudit=preAudit
      val transferredPostAudit=postAudit
      val transferredFee=acquireHolderFee
      // 此短段只搬同一组已预检引用；没有回调或资源操作可在serviceTransferred前失败。
      serviceOwner.openHolder=transferredOpenHolder
      serviceOwner.verified=transferredVerified;serviceOwner.scope=transferredScope;serviceOwner.auditor=transferredAuditor
      serviceOwner.barrier=transferredBarrier;serviceOwner.barrierAttempted=observer.barrierAttempted
      serviceOwner.barrierHeld=observer.barrierHeld;serviceOwner.barrierMayRetain=observer.barrierMayRetain
      serviceOwner.lastBarrierObservation=observer.lastBarrierObservation
      serviceOwner.writerCoordinator=transferredCoordinator;serviceOwner.writerToken=transferredToken
      serviceOwner.writerAdmission=transferredAdmission
      serviceOwner.writerPreAudit=transferredPreAudit;serviceOwner.writerPostAudit=transferredPostAudit
      serviceOwner.writerFee=transferredFee;serviceOwner.writerAttempted=true
      serviceOwner.writerRenewSequence=renewSequence;serviceOwner.started=true
      serviceOwner.databaseObservation=serviceObservation
      serviceOwner.state=State.SqlWriterAcquired
      observer.connectionSuccessor=serviceOwner;observer.openHolder=null
      coordinator=null;token=null;admission=null;preAudit=null;postAudit=null;acquireHolderFee=0L
      serviceTransferred=true
      serviceOwner.publish();observer.publish();parent.publish()

    private def requireRecoveredFreshness():MySqlOwnedAdmissionFreshness =
      requireRegistered()
      require(first==null && token!=null && admission!=null && (admission.token eq token) &&
        (coordinator.requireBusinessAdmission(observer.verified,observer.scope) eq admission),
        "actual recovered writer admission required; parent failure remains unchanged")
      val freshness=admission.receipt.ownedFreshness(renewSequence)
      require(freshness.isCurrent,"recovered writer admission windows expired; no projection extension")
      freshness

    /** 本owner独立维持服务writer，绝不向原Failed Session/mailbox发行普通freshness。 */
    private[MySqlDbFirstSession] def maintainRecoveredWriter():IoMaintenanceDeadline =
      requireRetiredHold()
      if !acquireAttempted then acquireWriterOnce()
      else
        require(serviceTransferred && serviceOwner!=null && acquired && !running && !closing && first==null,
          "unresolved service ownership cannot retry or publish")
        running=true;parent.publish()
        try
          serviceOwner.renewSqlWriter()
          renewSequence=serviceOwner.writerRenewSequence
          if serviceMailbox!=null then
            require(serviceMailbox.publish(serviceOwner.currentSqlWriterFreshness()),
              "actual new service mailbox renewal failed; old mailbox cannot substitute")
        catch case cause:Throwable=>
          first=cause
          throw cause
        finally { running=false;parent.publish() }
      require(serviceTransferred && serviceOwner!=null && (postAcquireReadback!=null || oldDataRetired),
        "actual new SQL/close owner and completed or actually retired Full27 required before maintenance hold")
      if oldDataRetired then requireRetiredOldData() else postAcquireReadback.requireCompletedObservation()
      try IoMaintenanceDeadline.after(serviceOwner.currentSqlWriterFreshness().nextMaintenanceAfter(System.nanoTime()))
      catch case cause:Throwable=>
        first=cause
        throw cause
      finally parent.publish()

    private[mysql] def requireRunningServiceTransfer(adapter:MySqlOwnedIoAdapter,previous:MySqlDbFirstSession,
        actual:MySqlDbFirstSession,mailbox:MySqlOwnedMaintenanceMailbox,actualReceipt:MySqlDbFirstRecallDropReceipt):Unit =
      requireRetiredHold()
      require(runningServiceTransferAttempted && !runningServiceTransferred && runtimeRegistered && !running && !closing &&
        (parent eq previous) && (serviceOwner eq actual) && (serviceMailbox eq mailbox) && (receipt eq actualReceipt) &&
        (servicePreparationAdapter eq adapter) && serviceMailboxFee==1024L && serviceControlFee==1024L &&
        inspectionRetired && observer==null && oldReadersRetired && inputsCloseTransferred &&
        receipt.retainedConfirmation.retainedQualification==null,
        "actual retired original graph and registered live service/opaque old receipt required")
      parent.requireTransferredParentResources()
      actual.currentSqlWriterFreshness()

    private[MySqlDbFirstSession] def transferRunningServiceOwner(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if !runtimeRegistered || runningServiceTransferred then return
      require(!runningServiceTransferAttempted && !running && !closing,"actual running service transfer is one-shot")
      runningServiceTransferAttempted=true;parent.publish()
      try adapter.takeOverRecoveredService(this,parent,serviceOwner,serviceMailbox,receipt)
      catch case cause:Throwable=>
        first=cause
        throw cause

    private[mysql] def moveRunningServiceCleanup(adapter:MySqlOwnedIoAdapter,actual:MySqlDbFirstSession):Unit =
      requireRunningServiceTransfer(adapter,parent,actual,serviceMailbox,receipt)
      actual.takeAdapterCleanup(this,adapter,serviceMailbox)
      serviceMailbox=null;serviceMailboxFee=0L;serviceControlFee=0L;serviceOwner=null
      runningServiceTransferred=true

    private def retireTransferredRecoveryInternal(adapter:MySqlOwnedIoAdapter,actual:MySqlDbFirstSession,
        stopping:Boolean):Unit =
      parent.requireThread()
      require(runningServiceTransferred && runtimeRegistered && !closed && !closing &&
        (first==null || stopping && stoppedCleanup) && !transferredRecoveryHolderRefundAttempted &&
        (parent.retiredRecallWriter eq this) && holderFee==RetiredRecallWriterRecovery.HolderBytes &&
        serviceOwner==null && serviceMailbox==null && serviceMailboxFee==0L && serviceControlFee==0L &&
        observer==null && inspectionRetired && retiredOldDataFacts && (servicePreparationAdapter eq adapter) &&
        coordinator==null && retirementAudit==null && preAudit==null && postAudit==null && token==null && admission==null &&
        acquireHolderFee==0L && serviceAllocationFee==0L && postAcquireToken==null && postAcquireAdmission==null &&
        actual.hasAdapterCleanup(adapter),"actual transferred Recovery retains metadata only")
      if stopping then adapter.requireStoppedRecoveredCleanup(this)
      parent.requireTransferredParentResources()
      adapter.detachRetiringRecoveredSource(this,parent)
      mutation=null;beforeMutation=null;servicePreparationRegistered=false
      parent.retiredRecallWriter=null
      transferredRecoveryHolderRefundAttempted=true
      try parent.root.releaseRetainedMemory(holderFee,"actual transferred Recovery metadata detached")
      catch case cause:Throwable=>
        if first==null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
        try adapter.retainFailedRecoveredSource(this,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        parent.retiredRecallWriter=this
        throw cause
      transferredRecoveryHolderRefundReturned=true
      holderFee=0L;servicePreparationAdapter=null;closed=true

    private[mysql] def retireTransferredRecovery(adapter:MySqlOwnedIoAdapter,actual:MySqlDbFirstSession):Unit =
      require(first==null && !stoppedCleanup,"ordinary Recovery retirement requires a healthy live owner")
      retireTransferredRecoveryInternal(adapter,actual,stopping=false)

    /** 仅Adapter持有真实stop权时，允许完成尚未尝试的metadata退休；未知退款仍保留原费用。 */
    private[mysql] def retireTransferredRecoveryForStop(adapter:MySqlOwnedIoAdapter,actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(adapter!=null && actual!=null && !stoppedCleanup && !transferredRecoveryHolderRefundAttempted &&
        !transferredRecoveryHolderRefundReturned,"exact unattempted transferred Recovery stop holder required")
      adapter.requireStoppedRecoveredCleanup(this)
      stoppedCleanup=true
      retireTransferredRecoveryInternal(adapter,actual,stopping=true)

    private[mysql] def requireServiceRuntimeRegistration(adapter:MySqlOwnedIoAdapter,actual:MySqlDbFirstSession,
        actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit =
      requireRetiredHold()
      require(runtimeRegistrationAttempted && !runtimeRegistered && !running && !closing && first==null &&
        inspectionRetired && observer==null && parent.recallInspection==null && oldReadersRetired && inputsCloseTransferred &&
        (servicePreparationAdapter eq adapter) && servicePreparationRegistered && serviceControlFee==1024L &&
        (serviceOwner eq actual) && actual!=null && actual.ownsInputs && actual.serviceReaders.isEmpty,
        "actual complete old graph retirement and exact new runtime responsibility required")
      requireServiceInputBinding(actual,actualInputs,actualRoot,next)
      actual.currentSqlWriterFreshness()

    private[MySqlDbFirstSession] def registerServiceRuntimeAfterInspectionRetirement(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if !inspectionRetired || runtimeRegistered then return
      require(!runtimeRegistrationAttempted && !running && !closing,"actual runtime registration is one-shot")
      runtimeRegistrationAttempted=true;parent.publish()
      try
        adapter.registerRecoveredServiceRuntime(this,serviceOwner,parent.inputs,parent.root,serviceIdentity)
        serviceOwner.installRegisteredServiceRuntime(this,adapter)
        parent.inputs.bindRegisteredServiceRuntime(this,serviceOwner,adapter,parent.root,serviceIdentity)
        runtimeRegistered=true
      catch case cause:Throwable=>
        first=cause
        try adapter.failRecoveredOldDataRetirement(this,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
      finally parent.publish()

    private[MySqlDbFirstSession] def detachRetiringInspectionHolder(actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(inspectionRetirementAttempted && !inspectionRetired && !closing && first==null &&
        (observer eq actual) && (parent.recallInspection eq actual) && (actual.retiringInspectionSource eq this) &&
        actual.inspectionOnly && actual.closeAttempted && actual.state==State.Closing && actual.inputsClosed &&
        actual.inputBorrow==null && actual.verified==null && actual.scope==null && actual.auditor==null &&
        actual.openHolder==null && actual.barrier==null && actual.connectionSuccessor==null &&
        actual.reader==null && actual.historicalRead==null && actual.forestRead==null && actual.auditHolder==null &&
        actual.recallReadbackAudit==null && actual.writerCoordinator==null && actual.writerFee==0L &&
        actual.retainedIssuer==null && actual.retainedIssuerFee==0L && !actual.gateHeld &&
        actual.holderFee==MySqlDbFirstSession.HolderBytes && actual.cleanupFailure==null,
        "actual old inspection resources and exact holder must retire before reverse-edge detach")
      observer=null;parent.recallInspection=null

    private[MySqlDbFirstSession] def retainFailedInspectionHolder(actual:MySqlDbFirstSession):Unit =
      parent.requireThread()
      require(inspectionRetirementAttempted && !inspectionRetired && observer==null && parent.recallInspection==null &&
        actual!=null && (actual.retiringInspectionSource eq this) && actual.holderFee==MySqlDbFirstSession.HolderBytes,
        "exact failed old inspection refund holder must remain owned")
      observer=actual;parent.recallInspection=actual

    private[MySqlDbFirstSession] def retireInspectionAfterServiceAuditRenewal(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if !inputsCloseTransferred || inspectionRetired then return
      require(!inspectionRetirementAttempted && !running && !closing && observer!=null &&
        (servicePreparationAdapter eq adapter) && oldReadersRetired && oldReaderResourcesRetired &&
        serviceOwner!=null && serviceOwner.ownsInputs && (observer.connectionSuccessor eq serviceOwner),
        "actual transferred input/SQL owner and unused old inspection retirement required")
      val live=serviceOwner
      require(live.writerRenewSequence>0L && live.state==State.SqlWriterAcquired && live.primary==null &&
        live.pendingWriterPreAudit==null && live.pendingWriterPostAudit==null && live.retiringWriterPreAudit==null &&
        live.retiringWriterPostAudit==null && live.previousWriterToken==null && live.previousWriterAdmission==null,
        "actual normal service renewal must settle inherited audit consumers first")
      live.writerPreAudit.requireExclusiveSessionOwner(live)
      live.writerPostAudit.requireExclusiveSessionOwner(live)
      live.writerCoordinator.requireCurrentDbFirstAuditConsumers(live.writerToken,live.writerAdmission,
        live.writerPreAudit,live.writerPostAudit)
      live.currentSqlWriterFreshness()
      require((live.verified eq observer.verified) && (live.scope eq observer.scope) && (live.barrier eq observer.barrier) &&
        live.openHolder!=null && observer.openHolder==null,"actual sole new SQL/barrier close owner required")
      inspectionRetirementAttempted=true;parent.publish()
      val retiring=observer
      try
        // 只撤已转移的借用metadata；实际新SQL连接/lease/barrier继续由live保管。
        retiring.retiringInspectionSource=this
        retiring.verified=null;retiring.scope=null;retiring.auditor=null;retiring.barrier=null
        retiring.barrierHeld=false;retiring.barrierMayRetain=false;retiring.connectionSuccessor=null
        retiring.closeOnOwnerThread()
        require(observer==null && parent.recallInspection==null && retiring.resourceObservation.closed &&
          retiring.holderFee==0L,"actual old inspection close/refund and all reverse edges required")
        retiredInspection=Some(retiring.resourceObservation)
        parent.closedRecallInspection=retiredInspection
        inspectionRetired=true
      catch case cause:Throwable=>
        first=cause
        try adapter.failRecoveredOldDataRetirement(this,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
      finally parent.publish()

    private def oldReaderResourcesRetired:Boolean =
      parent.reader==null && parent.historicalRead==null && parent.forestRead==null &&
        parent.auditHolder==null && parent.recallReadbackAudit==null && parent.qualification==null &&
        parent.recallReadbackQualification==null &&
        parent.writerCoordinator==null && parent.writerToken==null && parent.writerAdmission==null && parent.writerFee==0L &&
        parent.writerPreAudit==null && parent.writerPostAudit==null && parent.writerReleaseAudit==null &&
        parent.pendingWriterPreAudit==null && parent.pendingWriterPostAudit==null &&
        parent.retiringWriterPreAudit==null && parent.retiringWriterPostAudit==null &&
        parent.previousWriterToken==null && parent.previousWriterAdmission==null && parent.inputBorrow==null &&
        observer.reader==null && observer.historicalRead==null && observer.forestRead==null &&
        observer.auditHolder==null && observer.recallReadbackAudit==null && observer.qualification==null &&
        observer.recallReadbackQualification==null && observer.inputBorrow==null &&
        !parent.gateHeld && !observer.gateHeld

    private[mysql] def requireInputCloseTransfer(previous:MySqlDbFirstSession,actual:MySqlDbFirstSession,
        actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit =
      requireRetiredHold()
      require(oldReadersRetirementAttempted && oldReadersRetired && oldReaderResourcesRetired &&
        !inputsCloseTransferred && (previous eq parent) && parent.ownsInputs && !parent.inputsClosed &&
        (actual eq serviceOwner) && !actual.ownsInputs && actual.inputBorrow!=null && actual.serviceReaders.isEmpty &&
        (actualInputs eq parent.inputs) && (actualRoot eq parent.root) && (next eq serviceIdentity),
        "actual retired old consumers and exact sole live new service input borrower required")
      requireServiceInputBinding(actual,actualInputs,actualRoot,next)

    private[MySqlDbFirstSession] def transferInputsAfterOldReadersRetired(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if !oldDataRetired || inputsCloseTransferred then return
      require(!oldReadersRetirementAttempted && !running && !closing && servicePreparationAdapter==adapter &&
        adapter.recoveredServiceMainStage(this).contains("WorkerRetired") &&
        parent.ownsInputs && serviceOwner!=null && !serviceOwner.ownsInputs &&
        parent.bootTakeoverWork==null && parent.initialTransaction==null && parent.preparationTransaction==null &&
        parent.checkpointTransaction==null && parent.refusalTransaction==null && parent.journalTransaction==null &&
        parent.recallTransaction==null && !parent.gateHeld && !observer.gateHeld &&
        parent.retainedIssuer==null && parent.retainedIssuerFee==0L &&
        parent.writerCoordinator!=null && parent.writerCoordinator.dbFirstWriterRetiredRecall,
        "actual drained old producer/sealed writer and unused old-reader retirement required")
      oldReadersRetirementAttempted=true;parent.publish()
      var operationFailure:Throwable=null
      try
        observer.retireTransferredInspectionReaders(this)
        if observer.inputBorrow!=null then { observer.inputBorrow.close();observer.inputBorrow=null }
        // 旧writer已永久seal且原JDBC已实际close；只撤责任引用，绝不对新writer执行release。
        parent.writerAdmission=null;parent.writerToken=null;parent.writerCoordinator=null
        parent.previousWriterToken=null;parent.previousWriterAdmission=null
        parent.retireSupersededWriterAudits()
        if parent.pendingWriterPostAudit!=null then { parent.pendingWriterPostAudit.close();parent.pendingWriterPostAudit=null }
        if parent.pendingWriterPreAudit!=null then { parent.pendingWriterPreAudit.close();parent.pendingWriterPreAudit=null }
        if parent.writerReleaseAudit!=null then { parent.writerReleaseAudit.close();parent.writerReleaseAudit=null }
        if parent.writerPostAudit!=null then { parent.writerPostAudit.close();parent.writerPostAudit=null }
        if parent.writerPreAudit!=null then { parent.writerPreAudit.close();parent.writerPreAudit=null }
        if parent.writerFee!=0L then
          parent.root.releaseRetainedMemory(parent.writerFee,"actual permanently sealed old writer references detached")
          parent.writerFee=0L
        if parent.historicalRead!=null then { parent.historicalRead.close();parent.historicalRead=null }
        if parent.forestRead!=null then { parent.forestRead.close();parent.forestRead=null }
        if parent.reader!=null then { parent.reader.close();parent.reader=null }
        parent.qualification=null;parent.recallReadbackQualification=null
        if parent.recallReadbackAudit!=null then { parent.recallReadbackAudit.close();parent.recallReadbackAudit=null }
        if parent.auditHolder!=null then { parent.auditHolder.close();parent.auditHolder=null }
        parent.databaseObservation=null
        if parent.inputBorrow!=null then { parent.inputBorrow.close();parent.inputBorrow=null }
        require(oldReaderResourcesRetired,"actual old reader/audit/borrow resources remain unsettled")
        oldReadersRetired=true
        val census=parent.bootRecallCensus
        if census!=null then
          require(serviceOwner.bootRecallCensus==null && (serviceOwner.root eq parent.root) &&
            (serviceOwner.inputs eq parent.inputs) && (serviceOwner.thread eq parent.thread),
            "actual new service must receive sole original Boot census custody")
          census.requireWorkerTransfer(parent.root,parent.thread)
        parent.inputs.transferFinalCloseOwner(this,parent,serviceOwner,serviceIdentity)
        // 旧读者/借用均已实际关闭且census预检通过后，才执行无回调的所有权字段切换。
        parent.ownsInputs=false;serviceOwner.ownsInputs=true;inputsCloseTransferred=true
        if census!=null then
          serviceOwner.bootRecallCensus=census
          parent.bootRecallCensus=null
        oldReadersRetirementReturned=true
      catch case cause:Throwable=>
        operationFailure=cause
        first=cause
        try adapter.failRecoveredOldDataRetirement(this,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
      finally
        var publicationFailure:Throwable=null
        def publishSafely(action: => Unit):Unit =
          try action
          catch case cause:Throwable=>
            if operationFailure!=null then MySqlOwnedSessionLifecycle.suppress(operationFailure,cause)
            else if publicationFailure==null then publicationFailure=cause
            else MySqlOwnedSessionLifecycle.suppress(publicationFailure,cause)
        publishSafely(serviceOwner.publish())
        publishSafely(observer.publish())
        publishSafely(parent.publish())
        if operationFailure==null && publicationFailure!=null then
          first=publicationFailure
          throw publicationFailure

    private def retiredOldDataFacts:Boolean =
      oldDataRetirementAttempted && oldDataRetired && parent.mainRead==null && parent.mainReadFee==0L &&
        parent.pendingMainRead==null && parent.pendingMainReadFee==0L && parent.pendingMainReadObservation==null &&
        current==null && parent.retiredRecallObservation==null && postRetirementReadback==null && postAcquireReadback==null &&
        parent.retiredRecallObservationAllocationFee==0L && postReadbackAllocationFee==0L && postAcquireAllocationFee==0L &&
        Seq(closedCurrent,closedPostRetirement,closedPostAcquire).forall(_.exists(s=>
          s.closed && s.holderFee==0L && !s.readerPending && !s.gatePending && !s.auditPending &&
            !s.historyPending && !s.forestPending && s.failure.isEmpty && s.cleanupFailure.isEmpty))

    private[mysql] def requireRetiredOldData():Unit =
      parent.requireThread()
      if first!=null then throw first
      require((parent.retiredRecallWriter eq this) && retiredOldDataFacts && serviceTransferred && issuerTransferred &&
        serviceMainPrepared && servicePreparationRegistered && serviceControlFee==1024L &&
        serviceOwner!=null && serviceOwner.mainRead!=null && serviceMailbox!=null,
        "actual ACKed old DATA/three Work retirement and retained new service resources required")

    private[MySqlDbFirstSession] def retireOldDataAfterMainAck(adapter:MySqlOwnedIoAdapter):Unit =
      requireRetiredHold()
      if oldDataRetired then return
      require(!oldDataRetirementAttempted && !running && !closing && serviceOwner!=null && serviceMainPrepared &&
        (servicePreparationAdapter eq adapter),"actual prepared service and unused worker retirement required")
      if !adapter.beginRecoveredOldDataRetirement(this,parent,receipt,parent.mainRead,serviceOwner.mainRead,
          serviceOwner.retainedIssuer) then return
      oldDataRetirementAttempted=true;parent.publish()
      try
        // 新Main DATA已独立装配且真实claim；先关仍可能核旧DATA的reader/audit消费者。
        postAcquireReadback.closeOnOwnerThread();postAcquireReadback.requireRetiredObservation()
        closedPostAcquire=Some(postAcquireReadback.resourceObservation);postAcquireReadback=null
        postRetirementReadback.closeOnOwnerThread();postRetirementReadback.requireRetiredObservation()
        closedPostRetirement=Some(postRetirementReadback.resourceObservation);postRetirementReadback=null
        current.closeOnOwnerThread();current.requireRetiredObservation()
        closedCurrent=Some(current.resourceObservation);parent.closedRetiredRecallObservation=closedCurrent
        parent.retiredRecallObservation=null;current=null
        parent.pendingMainRead.requireUnpublishedData(parent,parent.recallPreparedReadback.get.canonicalSha256)
        parent.pendingMainRead.retireOnWorker(parent);parent.pendingMainRead.requireRetiredOnWorker()
        parent.pendingMainRead=null;parent.pendingMainReadObservation=null
        parent.root.releaseRetainedMemory(parent.pendingMainReadFee,"actual unpublished old pending DATA retired")
        parent.pendingMainReadFee=0L
        parent.mainRead.retireOnWorker(parent);parent.mainRead.requireRetiredOnWorker()
        adapter.detachRetiredRecoveredPrevious(this,parent.mainRead)
        parent.mainRead=null
        parent.root.releaseRetainedMemory(parent.mainReadFee,"actual Main-dropped old current DATA retired")
        parent.mainReadFee=0L
        oldDataRetired=true
        adapter.finishRecoveredOldDataRetirement(this)
      catch case cause:Throwable=>
        first=cause
        try adapter.failRecoveredOldDataRetirement(this,cause)
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        throw cause
      finally parent.publish()

    private[MySqlDbFirstSession] def closeOnOwnerThread():Unit = closeOnOwnerThread(stopping=false)

    /** 停止清理是独立权限：只关闭已保管的提交资源，不恢复普通业务准入。 */
    private[MySqlDbFirstSession] def closeForStoppedService():Unit =
      closeForStoppedService(servicePreparationAdapter)

    private[MySqlDbFirstSession] def closeForStoppedService(adapter:MySqlOwnedIoAdapter):Unit =
      parent.requireThread()
      require(adapter!=null && (servicePreparationAdapter eq adapter) && !stoppedCleanup,
        "exact registered retained-service stop owner required")
      adapter.requireStoppedRecoveredCleanup(this)
      require(!running && writerRetired && mutation!=null && mutation.row.ownerBootUuid.isEmpty &&
        (!oldReadersRetirementAttempted || oldReadersRetired),
        "actual stopped worker, known retired writer and fully returned old-reader transfer required")
      if runningServiceTransferred then
        require(closed && transferredRecoveryHolderRefundReturned && transferredRecoveryHolderRefundAttempted,
          "adapter-owned transfer metadata must settle or remain retained before Recovery close")
        return
      stoppedCleanup=true
      closeOnOwnerThread(stopping=true)

    private def closeStoppedUntransferredService():Unit =
      require(stoppedCleanup && closing && parent.closeAttempted && parent.state==State.Closing &&
        serviceAttempted && !serviceTransferred && acquired && writerRetired && !running &&
        token!=null && admission!=null && (admission.token eq token) && coordinator!=null &&
        stoppedReleaseAudit==null && !stoppedReleaseAuditAttempted && !stoppedReleaseAttempted &&
        !stoppedReleaseReturned && stoppedReleaseLockedBefore==null && stoppedReleaseMutation==null &&
        !stoppedReleaseReadbackAttempted && stoppedReleaseReadbackAllocationFee==0L &&
        stoppedReleaseReadback==null && serviceMailbox==null && serviceMailboxFee==0L && serviceControlFee==0L &&
        !servicePreparationRegistered && !oldDataRetired && !oldReadersRetirementAttempted &&
        !inputsCloseTransferred && postAcquireReadback!=null && postAcquireReadback.resourceObservation.completed &&
        postAcquireReadback.resourceObservation.observation.nonEmpty &&
        postAcquireReadback.resourceObservation.failure.isEmpty &&
        postAcquireReadback.resourceObservation.cleanupFailure.isEmpty &&
        !postAcquireReadback.resourceObservation.closed && !postAcquireReadback.closeAttemptedForStoppedCleanup &&
        postRetirementReadback!=null && postRetirementReadback.resourceObservation.completed &&
        current!=null && current.completedForStoppedCleanup &&
        closedCurrent.isEmpty && closedPostRetirement.isEmpty &&
        acquireHolderFee==RetiredRecallWriterRecovery.AcquireHolderBytes &&
        preAudit!=null && postAudit!=null && retirementAudit==null && observer!=null &&
        observer.connectionSuccessor==null && observer.openHolder!=null && observer.verified!=null &&
        observer.scope!=null && observer.writerCoordinator==null,
        "actual acquired untransferred W and retained Full27 preimage are required for one stopped release")
      if serviceOwner!=null then requireStoppedServiceShell(serviceOwner,closingAllowed=false)
      retireStoppedPreReleaseObservations()

      stoppedReleaseAuditAttempted=true
      stoppedReleaseAudit=observer.auditor.beginDbFirstWriterAudit(parent.inputs,parent.root,
        observer.verified,observer.scope,observer,coordinator)
      val released=coordinator.releaseOwnedDbFirstForStoppedRecovery(this,token,
        observer.verified,observer.scope,stoppedReleaseAudit)
      stoppedReleaseMutation=released
      stoppedReleaseReturned=true

      stoppedReleaseReadbackAttempted=true
      parent.root.reserveRetainedMemory(RetiredRecallObservation.HolderBytes,
        "stopped new-W release Full27 readback Work BEFORE allocation")
      stoppedReleaseReadbackAllocationFee=RetiredRecallObservation.HolderBytes
      stoppedReleaseReadback=new RetiredRecallObservation(parent,observer,receipt,
        RetiredRecallObservation.HolderBytes,this)
      stoppedReleaseReadbackAllocationFee=0L
      stoppedReleaseReadback.executeOnce()
      requireStoppedReleaseReadbackCompleted(stoppedReleaseReadback)
      stoppedReleaseReadbackReturned=true

      val stoppedReadback=stoppedReleaseReadback
      stoppedReadback.closeOnOwnerThread();stoppedReadback.requireRetiredObservation()
      closedStoppedReleaseReadback=Some(stoppedReadback.resourceObservation)
      stoppedReleaseReadback=null
      val acquiredReadback=postAcquireReadback
      acquiredReadback.closeOnOwnerThread();acquiredReadback.requireRetiredObservation()
      closedPostAcquire=Some(acquiredReadback.resourceObservation);postAcquireReadback=null
      require(stoppedPreReleaseObservationsRetired && postAcquireAllocationFee==0L && postReadbackAllocationFee==0L &&
        stoppedReleaseReadbackAllocationFee==0L && postAcquireReadback==null && postRetirementReadback==null &&
        stoppedReleaseReadback==null && current==null,
        "all actual prior and stopped-release observations must retire before audit consumers")

      coordinator.detachStoppedRecoveredReleaseAudit(this,stoppedReleaseAudit)
      stoppedReleaseAudit.close();stoppedReleaseAudit=null
      preAudit.close();preAudit=null
      postAudit.close();postAudit=null
      token=null;admission=null;coordinator=null;postAcquireToken=null;postAcquireAdmission=null
      stoppedReleaseAuditAttempted=true
      acquireHolderRefundAttempted=true
      parent.root.releaseRetainedMemory(acquireHolderFee,"stopped recovered writer and audit graph detached")
      acquireHolderRefundReturned=true
      acquireHolderFee=0L

      if serviceOwner!=null then
        val shell=serviceOwner
        shell.retireStoppedEmptyServiceShell(this,serviceInputBindingReturned)
        closedService=Some(shell.resourceObservation)
        serviceOwner=null
      else
        require(!serviceInputBindingReturned,"missing service Session cannot retain an input binding")
      serviceIdentity=null
      if serviceAllocationFee!=0L then
        require(serviceOwner==null && !serviceInputBindingReturned,
          "unallocated service holder cannot coexist with a bound Session")
        serviceAllocationRefundAttempted=true
        parent.root.releaseRetainedMemory(serviceAllocationFee,"unallocated stopped service Session holder detached")
        serviceAllocationRefundReturned=true
        serviceAllocationFee=0L
      servicePreparationAdapter=null

    private def closeOnOwnerThread(stopping:Boolean):Unit =
      parent.requireThread()
      if closed then return
      require(!running && !closing && writerRetired && (first==null || stopping && stoppedCleanup) &&
        mutation!=null && mutation.row.ownerBootUuid.isEmpty &&
        (!oldReadersRetirementAttempted || oldReadersRetired),
        "unresolved writer retirement cannot discard its SQL/mutation owner")
      closing=true
      try
        if servicePreparationAdapter!=null then
          servicePreparationAdapter.beginRecoveredDbFirstServiceClose(this,servicePreparationRegistered)
        if stopping && stoppedCleanup && serviceAttempted && !serviceTransferred then
          closeStoppedUntransferredService()
        else
          if serviceMailbox!=null then
            serviceMailbox.stop()
            serviceMailbox.retireAfterTerminalProducer()
            serviceMailbox=null
          if serviceMailboxFee!=0L then
            parent.root.releaseRetainedMemory(serviceMailboxFee,"actual stopped service mailbox windows detached")
            serviceMailboxFee=0L
          if postAcquireReadback!=null then
            postAcquireReadback.closeOnOwnerThread();postAcquireReadback=null
          if postAcquireAllocationFee!=0L then
            parent.root.releaseRetainedMemory(postAcquireAllocationFee,"unallocated post-acquire Work detached")
            postAcquireAllocationFee=0L
          if postRetirementReadback!=null then
            postRetirementReadback.closeOnOwnerThread();postRetirementReadback=null
          if postReadbackAllocationFee!=0L then
            parent.root.releaseRetainedMemory(postReadbackAllocationFee,"unallocated post-retirement Work detached")
            postReadbackAllocationFee=0L
          if serviceAttempted && !runningServiceTransferred then
            require(serviceTransferred && serviceOwner!=null,
              "partial service transfer cannot discard either actual owner")
            if current!=null then
              current.closeOnOwnerThread()
              parent.closedRetiredRecallObservation=Some(current.resourceObservation)
              parent.retiredRecallObservation=null;current=null
            if observer!=null then observer.retireTransferredInspectionReaders(this)
            val retiring=serviceOwner
            retiring.closeOnOwnerThread() // 最后holder退款前实际回调撤inputs/observer/Recovery强边。
            require(retiring.resourceObservation.closed && serviceOwner==null &&
              (observer==null || observer.connectionSuccessor==null) && serviceCloseEdgesDetached,
              "actual service SQL close and strong-edge retirement remain unsettled")
            closedService=Some(retiring.resourceObservation)
          else if serviceAttempted then
            require(stopping && stoppedCleanup && transferredRecoveryHolderRefundAttempted &&
              transferredRecoveryHolderRefundReturned && closed,
              "adapter must retire exact old metadata before stop closes transferred Recovery")
          else require(!acquireAttempted,"unresolved actual acquisition has no discard path")
        if retirementAudit!=null then
          coordinator.detachTerminatedDbFirstRecallAudit(this,retirementAudit)
          coordinator=null
          retirementAudit.close();retirementAudit=null
        mutation=null;beforeMutation=null
        parent.root.releaseRetainedMemory(holderFee,"retired writer owner graph detached")
        holderFee=0L;closed=true
      catch case cause:Throwable=>
        if first==null then first=cause else MySqlOwnedSessionLifecycle.suppress(first,cause)
        throw cause

    private[MySqlDbFirstSession] def requireRetiredHold():Unit =
      parent.requireThread()
      if first!=null then throw first
      requireRegistered()
      require(started && originalSealed && writerRetired && !running && mutation!=null &&
        mutation.row.ownerBootUuid.isEmpty && postReadbackAttempted && (postRetirementReadback!=null || oldDataRetired),
        "exact retired W and completed or actually retired independent readback required")
      if oldDataRetired then requireRetiredOldData() else postRetirementReadback.requireCompletedObservation()

  private object RetiredRecallWriterRecovery:
    val HolderBytes=8192L // 新coordinator/mutation/retirement引用槽；原owner和两DATA仍独立计费。
    val AcquireHolderBytes=16384L // 实际新token/admission及每轮独立audit/退休/失败引用槽，SQL前预收。

  private object RetiredRecallObservation:
    val HolderBytes=8192L // 独立Work/RR/summary/reader/audit/forest/history引用槽；实际子图另行计费。

  final class AllocationFailure private[MySqlDbFirstSession] (
      original: Throwable,val retainedSession: MySqlDbFirstSession)
      extends RuntimeException("schema6 Session allocation retains actual cleanup responsibility",original)
  private val HolderBytes=8192L // Session/审计器/qualification/rollback状态、固定观测与summary槽。
  private val RetainedIssuerBytes=2048L // issuer/core、空索引及固定family引用，先预收再建立。
  def allocate(inputs: MySqlDbFirstSessionInputs,root: MySqlFormat3ReadBudget,
      identity: MySqlOwnedIoIdentity,auditMaxAge: Duration, ledger: MySqlFormat3AllocationLedger,
      actualMain: Thread, capacity: Int): MySqlDbFirstSession =
    require(inputs != null && root != null && identity != null && auditMaxAge != null,"actual schema6 Session inputs/root/identity/audit age required")
    inputs.requireForSession(root,identity)
    root.requireLedgerIdentity(ledger)
    require(actualMain != null && (actualMain ne Thread.currentThread()) && capacity > 0,
      "schema6 retained owner requires actual distinct Main and explicit adapter capacity")
    require(!auditMaxAge.isNegative && !auditMaxAge.isZero && auditMaxAge.toNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos,
      "schema6 audit age must be explicit and bounded")
    val full27=inputs.full27Inputs
    val thread=Thread.currentThread()
    root.reserveRetainedMemory(HolderBytes,"schema6 observed Session holder BEFORE allocation")
    var session: MySqlDbFirstSession=null
    try
      session=new MySqlDbFirstSession(inputs,root,identity,thread,auditMaxAge,full27,ledger,HolderBytes)
      session.inputBorrow=inputs.borrowForReader()
      session.ownsInputs=true
      session.allocateRetainedOwner(actualMain,capacity)
      session.publish()
      session
    catch case cause: Throwable =>
      if session == null then
        try root.releaseRetainedMemory(HolderBytes,"unallocated schema6 Session holder")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      else
        session.retainFailure(cause)
        try session.closeOnOwnerThread()
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        if !session.resourceObservation.closed then throw new AllocationFailure(cause,session)
      throw cause
