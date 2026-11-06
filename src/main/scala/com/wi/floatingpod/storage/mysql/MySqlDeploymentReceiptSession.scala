package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}
import com.wi.floatingpod.bootstrap.ProductionBootstrapControl
import com.wi.floatingpod.server.io.IoOperationTag
import java.nio.file.Path
import java.time.{Duration,LocalDateTime}
import java.sql.{Connection,PreparedStatement,SQLDataException,SQLNonTransientConnectionException,SQLTransientConnectionException,Types}
import scala.util.{Success,Try}

/** schema5 的原 worker partial holder；分配不打开连接，也不发行 Ready。 */
private[mysql] final class MySqlDeploymentReceiptSession private (
    private val inputs: MySqlDeploymentReceiptSessionInputs,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private val mode: MySqlDeploymentReceiptSessionInputs.ModeRequest,
    private val fresh: Option[MySqlDeploymentReceiptSession.FreshRequest],
    private val original: Option[ProductionBootstrapControl.OriginalSourcePins],
    private val auditMaxAge: Duration,
    private val leaseDuration: Duration,
    private val submissionTag: IoOperationTag,
    private val ordinaryIdentity: Option[ProductionBootstrapControl.OrdinaryIdentity],
    private val ordinaryControlled: Option[MySqlFreshControlledMetadata]
):
  private val thread = Thread.currentThread()
  private[mysql] def requireHistoricalReceiptScope(actual:MySqlDeploymentReceiptBootstrapActor,
      data:MySqlNormalRecallRecovery.ReceiptData,held:HeldSessionBarrier):Unit =
    requireActor(actual)
    if held ne barrier then throw new SQLDataException("historical receipt lost original held Session barrier")
    val approved=inputs.auditExpectation
    val deployment=approved.deploymentReceiptInputs.getOrElse(throw new SQLDataException("historical receipt lacks approved schema5 inputs"))
    val s=data.scope.stableSession
    if s.schemaFormat!=5 || s.physicalFormat!=2 || s.provisioning!=approved.provisioningUuid ||
        !s.ddl.equalsIgnoreCase(approved.ddlSha256) || !s.manifest.equalsIgnoreCase(approved.objects.canonicalSha256) ||
        !s.configuration.equalsBytes(deployment.configurationSha256.bytes) ||
        s.provenanceKind!=deployment.sourceProvenanceKind ||
        !s.provenance.equalsBytes(deployment.sourceProvenanceSha256.bytes) || s.namespace!=held.namespace.lockName() then
      throw new SQLDataException("historical receipt differs from original approved schema5 source/configuration/barrier")
  /** 原 HANDOFF 已确认但尚未 business adoption；原 root/native/keeper 继续承担 Recall。 */
  private var restrictedPublication:MySqlDeploymentReceiptSession.RestrictedPublication5=null
  private var restrictedDecisions=Vector.empty[MySqlNormalRecallRecovery.CheckpointDecisionData]
  private var restrictedRecallProof:MySqlDeploymentReceiptSession.RestrictedRecall5=null
  private var restrictedReferencesIssued:Vector[MySqlNormalRecallRecoveryReference]=null
  private var restrictedSourceLink:MySqlOwnedIoAdapter.RecallRecoveryLink=null
  private var restrictedSourceRunner:MySqlFencedTransaction=null
  private var restrictedSourceReaderLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5=null
  private var restrictedSourceLedger:MySqlFormat3AllocationLedger=null
  private var restrictedSourceReaders:List[MySqlDeploymentReceiptReadOwner]=Nil
  private var restrictedSourceFreshWriteScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5=null
  private var restrictedSourceR0ExecutionOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome=null
  private var restrictedSourceR0PostCommitReadback:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5=null
  private var restrictedSourceR0UnknownInspection:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5=null
  private var lastUnknownInspectionConnectionGeneration=identity.connectionGeneration
  private var restrictedSourceFreshWritePendingReader:MySqlDeploymentReceiptReadOwner=null
  private var restrictedSourceFreshWritePendingScopeHolder=false
  private var restrictedRecall: Option[(com.wi.floatingpod.server.WindowsB1V6WitnessRecordStore.DurableV6Ack,
    MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow,Vector[MySqlNormalRecallRecoveryInventory],
    MySqlDeploymentReceiptBootstrapActor.Planned5,MySqlDeploymentReceiptSession.OrdinaryPostAudit5)] = None
  private[mysql] def retainRestrictedRecall(actual:MySqlDeploymentReceiptBootstrapActor,
      ack:com.wi.floatingpod.server.WindowsB1V6WitnessRecordStore.DurableV6Ack,
      after:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,
      inventory:Vector[MySqlNormalRecallRecoveryInventory],plan:MySqlDeploymentReceiptBootstrapActor.Planned5,
      postAudit:MySqlDeploymentReceiptSession.OrdinaryPostAudit5,
      decisions:Vector[MySqlNormalRecallRecovery.CheckpointDecisionData]):Unit =
    requireActor(actual);actual.requireRestrictedRecallHandoff(plan,ack)
    if mode!=MySqlDeploymentReceiptSessionInputs.Ordinary || restrictedRecall.nonEmpty || ack==null ||
        after==null || writer==null || inventory==null || inventory.isEmpty || decisions==null || decisions.size!=inventory.size ||
        !inventory.indices.forall(i=>decisions(i).inventory eq inventory(i)) || plan==null ||
        !readOwners.exists(_.ownsRetainedImage(after)) || postAudit==null || (postAudit ne ordinaryPostAudit) ||
        !writer.ownerBootUuid.contains(identity.bootId) || !writer.leaseUntil.isAfter(writer.databaseNow) ||
        critical!=null || !verified.connection.getAutoCommit then
      throw new SQLDataException("restricted schema5 Recall requires original confirmed HANDOFF and current Full26 readback")
    root.reserveRetainedMemory(768L,"schema5 original restricted Recall holder/proof/coordinator BEFORE allocation")
    restrictedDecisions=decisions
    restrictedRecall=Some((ack,after,writer,inventory,plan,postAudit))
    coordinator=new MySqlWriterFenceCoordinator(inputs.auditExpectation,auditor,scope,leaseDuration)
    restrictedRecallProof=new MySqlDeploymentReceiptSession.RestrictedRecall5(this,actual,plan,ack,postAudit,coordinator)
    coordinator.adoptDeploymentReceiptRecall(restrictedRecallProof)

  private var started = false
  @volatile private var quiescing = false
  @volatile private var primary: Throwable = null
  private var verified: VerifiedMySqlConnection = null
  private var initialOpenHolder: MySqlConnectionSource.SourceUnknownInspectionOpen = null
  private var initialRawClosed = Option.empty[Boolean]
  private def openOriginalConnection(): VerifiedMySqlConnection =
    requireOwner()
    require(initialOpenHolder == null && verified == null, "original schema5 connection open is one-shot")
    initialOpenHolder = new MySqlConnectionSource(inputs.settings).newOwnedSessionOpen()
    initialOpenHolder.openOnce()
  private def hasUntransferredRaw: Boolean =
    initialOpenHolder != null && initialOpenHolder.hasRawHandle()

  private var scope: MySqlAuditBootScope = null
  private var barrier: HeldSessionBarrier = null
  private var auditor: MySqlSchemaAuditor = null
  private var bootstrapQualification:VerifiedSchemaQualification=null
  private var nativeOwner: MySqlDeploymentReceiptFreshNativeOwner = null
  private var readOwner: MySqlDeploymentReceiptReadOwner = null
  private var readOwners: List[MySqlDeploymentReceiptReadOwner] = Nil
  private var critical:MySqlDeploymentReceiptSession.CriticalWriteScope=null
  private var ordinaryPostAudit:MySqlDeploymentReceiptSession.OrdinaryPostAudit5=null
  private var ordinaryAdoption:MySqlDeploymentReceiptSession.OrdinaryAdoption5=null
  private var coordinator:MySqlWriterFenceCoordinator=null
  private var admission:MySqlBusinessAdmission=null
  private var renewSequence=0L
  private var publicationLease:MySqlRuntimeInputs.ConsumerLease=null
  private var publication:MySqlDeploymentReceiptSession.Publication5=null
  private var publishedReply:MySqlOwnedIoReply.DeploymentReceiptBooted=null
  private var failedExit: MySqlDeploymentReceiptSession.FailedInitializationExit = null
  private var retainedPlanIssuer:MySqlOwnedRetainedPlanIssuer=null
  private var purposeRunner:MySqlFencedTransaction=null
  private var purposeLedger:MySqlFormat3AllocationLedger=null
  private var purposeBorrow:MySqlOwnedRepositoryBorrow=null
  private var purposeBorrowGeneration=0L
  private var purposeReads:List[(MySqlFencedTransaction.DeploymentReceiptPurposeReadOwner5,
    MySqlDeploymentReceiptSession.PurposeReadScope5,MySqlDeploymentReceiptReadOwner,MySqlFormat3ReadBudget,Boolean)]=Nil
  private var terminalIntent:MySqlDeploymentReceiptSession.TerminalIntent5=null
  private var nativeRetirement:MySqlDeploymentReceiptSession.NativeRetirementPermit5=null
  private var sqlRetirementReturned=false
  private var normalSettlement:MySqlDeploymentReceiptSession.NormalTerminalSettlement5=null
  private var terminalInputsRetired=false
  private val actor = MySqlDeploymentReceiptBootstrapActor.allocate(this)
  private val freshCapture = new MySqlDeploymentReceiptSession.FreshCapture(this,actor)

  private def requireOwner(): Unit =
    if Thread.currentThread() ne thread then throw new IllegalStateException("exact schema5 Session IO worker required")
    inputs.requireForSession(root, identity, mode)

  private[mysql] def createRetainedPlanIssuer(actualIdentity:MySqlOwnedIoIdentity,actualMain:Thread,
      capacity:Int,ledger:MySqlFormat3AllocationLedger):MySqlOwnedRetainedPlanIssuer =
    requireOwner()
    if quiescing || primary!=null || retainedPlanIssuer!=null || (actualIdentity ne identity) || actualMain==null then
      throw new IllegalStateException("one original live schema5 retained issuer required",primary)
    root.requireLedgerIdentity(ledger)
    val completed=actor.completionFor(this)
    completed.admittedForSession(this)
    if coordinator==null || admission==null || (coordinator.requireBusinessAdmission(verified,scope) ne admission) then
      throw new IllegalStateException("schema5 retained issuer requires exact completed adoption/admission")
    barrier.readmit(verified,inputs.auditExpectation,scope)
    val issuer=new MySqlOwnedRetainedPlanIssuer(null,identity,thread,actualMain,inputs.sourceLimitProfile,ledger,capacity,
      this,Some(inputs.full26Inputs))
    retainedPlanIssuer=issuer
    purposeLedger=ledger
    purposeRunner=new MySqlFencedTransaction(inputs.auditExpectation,coordinator,verified,scope,Some(ledger))
    purposeRunner.bindDeploymentReceiptSession(this,ledger)
    issuer

  private[mysql] def requirePurposeRunner(actual:MySqlFencedTransaction,currentVerified:VerifiedMySqlConnection,
      currentScope:MySqlAuditBootScope,currentCoordinator:MySqlWriterFenceCoordinator,
      currentLedger:MySqlFormat3AllocationLedger):Unit =
    requireOwner()
    if actual==null || (purposeRunner ne actual) || (verified ne currentVerified) || (scope ne currentScope) ||
        (coordinator ne currentCoordinator) || purposeLedger==null || (purposeLedger ne currentLedger) ||
        retainedPlanIssuer==null || quiescing || primary!=null then
      throw new IllegalStateException("exact original schema5 purpose runner/session/ledger required",primary)
    root.requireLedgerIdentity(currentLedger)
    retainedPlanIssuer.requireDeploymentReceiptSession(this,inputs.sourceLimitProfile,currentLedger)

  private def capturePurposeRead(planning:MySqlFencedTransaction.DeploymentReceiptPurposeReadOwner5,
      retainsProjection:Boolean):MySqlDeploymentReceiptReadOwner =
    requirePurposeRunner(purposeRunner,verified,scope,coordinator,purposeLedger)
    if planning==null || purposeReads.exists(_._1 eq planning) then
      throw new IllegalStateException("one original Full26 reader per planning owner required")
    val originalRoot=planning.originalPurposeRoot
    originalRoot.requireScriptDataInputs(inputs.full26Inputs)
    if !root.sharesAllocationLedger(originalRoot) then throw new IllegalStateException("Full26 purpose root lost original Session ledger")
    val (gate,rollbackOnly)=planning.deploymentReceiptReadTransaction(this,verified,scope,originalRoot)
    val readScope=new MySqlDeploymentReceiptSession.PurposeReadScope5(this,actor,verified,planning,originalRoot,rollbackOnly,gate)
    val reader=MySqlDeploymentReceiptReadOwner.allocate(this,actor,verified,scope,inputs.auditExpectation,inputs.full26Inputs,originalRoot)
    readOwners=reader::readOwners
    purposeReads=(planning,readScope,reader,originalRoot,retainsProjection)::purposeReads
    reader.capturePurposeLockedFull26(readScope)
    reader

  private[mysql] def capturePurposePlanningImage(planning:MySqlFencedTransaction.PlanningReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(planning,true)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeControlledWriteImage(owner:MySqlFencedTransaction.ControlledDebugWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,false)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeControlledProjectionImage(owner:MySqlFencedTransaction.ControlledDebugWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,true)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeModeWriteImage(owner:MySqlFencedTransaction.ModeWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,false)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeModeProjectionImage(owner:MySqlFencedTransaction.ModeWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,true)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeDriftWriteImage(owner:MySqlFencedTransaction.DriftWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,false)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeDriftProjectionImage(owner:MySqlFencedTransaction.DriftWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,true)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeNormalRecallWriteImage(owner:MySqlFencedTransaction.NormalRecallWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,false)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def capturePurposeNormalRecallProjectionImage(owner:MySqlFencedTransaction.NormalRecallWriteReadOwner):MySqlScriptSchemaSnapshot =
    val reader=capturePurposeRead(owner,true)
    reader.withLockedImage(actor,inputs)((image,_)=>image)

  private[mysql] def settlePurposeRead(planning:MySqlFencedTransaction.DeploymentReceiptPurposeReadOwner5):Unit =
    requirePurposeRunner(purposeRunner,verified,scope,coordinator,purposeLedger)
    planning.requirePurposeReadSettled(this)
    val matches=purposeReads.filter(_._1 eq planning)
    if matches.size!=1 then throw new IllegalStateException("exact registered Full26 purpose reader required")
    val (_,readScope,reader,originalRoot,retainsProjection)=matches.head
    readScope.settled=true
    if retainsProjection && planning.hasOriginalPurposeProjection(this) then
      val child=reader.purposeImageBudget(actor,originalRoot)
      planning.transferPurposeImageToOriginalProjection(this,child,originalRoot)
      reader.detachPurposeImage(actor,originalRoot,child)
    else
      // 正常拒绝没有 retained projection，不再有 consumer；可直接退休实际 reader child。
      reader.retireForPurpose(actor,originalRoot)
    purposeReads=purposeReads.filterNot(_._3 eq reader)
    readOwners=readOwners.filterNot(_ eq reader)

  private def releasePurposePlan(tag:IoOperationTag,receipt:MySqlRetainedPlanDropReceipt,
      kind:MySqlRetainedPlanReleaseKind):Unit =
    requirePurposeRunner(purposeRunner,verified,scope,coordinator,purposeLedger)
    if purposeBorrow!=null || receipt==null || tag!=receipt.tag || (receipt.handle.issuer ne retainedPlanIssuer) then
      throw new IllegalArgumentException("exact non-nested schema5 Main drop receipt required")
    barrier.readmit(verified,inputs.auditExpectation,scope)
    if coordinator.requireBusinessAdmission(verified,scope) ne admission then
      throw new IllegalStateException("schema5 plan release lost current actual admission")
    retainedPlanIssuer.requireDropReceipt(receipt)
    retainedPlanIssuer.requireReleaseKind(receipt,kind)
    try
      if purposeReads.exists(_._1.belongsToPurposeHandle(receipt.handle)) then
        throw new IllegalStateException("original purpose reader cleanup/transfer has not settled; no release ACK")
      if kind==MySqlRetainedPlanReleaseKind.Ordinary && purposeRunner.hasRetainedNormalRecallProjection(tag) then
        purposeRunner.requireNormalRecallProjectionRelease(tag)
      retainedPlanIssuer.dropWorkerPlanReferences(receipt)
      kind match
        case MySqlRetainedPlanReleaseKind.ModeProjection=>purposeRunner.releaseRetainedModeProjection(tag)
        case MySqlRetainedPlanReleaseKind.DriftProjection=>purposeRunner.releaseRetainedDriftProjection(tag)
        case MySqlRetainedPlanReleaseKind.Ordinary=>
          if purposeRunner.hasRetainedNormalRecallProjection(tag) then purposeRunner.releaseRetainedNormalRecallProjection(tag)
          if purposeRunner.hasRetainedControlledDebugProjection(tag) then purposeRunner.releaseRetainedControlledDebugProjection(tag)
          purposeRunner.releaseRetainedRequestRead(tag)
      retainedPlanIssuer.releaseDropped(receipt)
    catch case cause:Throwable=>
      try retainedPlanIssuer.markReleaseFailed(receipt,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      primary=MySqlOwnedSessionLifecycle.combine(Option(primary),cause).orNull
      throw cause

  private[mysql] def releaseRetainedPlan(tag:IoOperationTag,receipt:MySqlRetainedPlanDropReceipt):Unit =
    releasePurposePlan(tag,receipt,MySqlRetainedPlanReleaseKind.Ordinary)
  private[mysql] def releaseModeProjection(tag:IoOperationTag,receipt:MySqlRetainedPlanDropReceipt):Unit =
    releasePurposePlan(tag,receipt,MySqlRetainedPlanReleaseKind.ModeProjection)
  private[mysql] def releaseDriftProjection(tag:IoOperationTag,receipt:MySqlRetainedPlanDropReceipt):Unit =
    releasePurposePlan(tag,receipt,MySqlRetainedPlanReleaseKind.DriftProjection)

  private[mysql] def requirePurposeReader(actual:MySqlDeploymentReceiptReadOwner,
      actualActor:MySqlDeploymentReceiptBootstrapActor,actualRoot:MySqlFormat3ReadBudget):Unit =
    requireReadOwner(actual,actualActor)
    if !purposeReads.exists { case (_,readScope,reader,originalRoot,_)=>
        (reader eq actual) && (originalRoot eq actualRoot) && readScope.isSettled } then
      throw new IllegalStateException("registered settled original Full26 purpose reader/root required")

  /** 仅下方固定 purpose 入口可用；generic Run/Transaction 不取得本 runner。 */
  private def withPurposeCommand[A](plan:MySqlOwnedRepositoryOperationPlan,
      originalRead:Option[MySqlRepositoryOperation.FindOriginalDeploymentOperation]=None)
      (body:MySqlRepositoryCommandScope=>Try[A]):Try[A] =
    requirePurposeRunner(purposeRunner,verified,scope,coordinator,purposeLedger)
    if purposeBorrow!=null || plan==null then throw new IllegalStateException("non-nested original schema5 purpose command required")
    if plan.exactLimitProfile ne inputs.sourceLimitProfile then throw new IllegalArgumentException("purpose plan profile differs")
    val handle=plan.retainedHandle.getOrElse(throw new IllegalArgumentException("purpose command lacks actual Main reservation"))
    if handle.issuer ne retainedPlanIssuer then throw new IllegalArgumentException("purpose command has a foreign issuer")
    barrier.readmit(verified,inputs.auditExpectation,scope)
    if coordinator.requireBusinessAdmission(verified,scope) ne admission then
      throw new IllegalStateException("purpose command lost exact schema5 business admission")
    val originalRoot=retainedPlanIssuer.beginExecution(plan)
    originalRead match
      case Some(operation)=>
        plan.requireSingle(operation,inputs.sourceLimitProfile)
        originalRoot.requireDeploymentOriginalReadLayout()
      case None=>originalRoot.requireScriptDataInputs(inputs.full26Inputs)
    purposeBorrowGeneration=Math.incrementExact(purposeBorrowGeneration)
    var borrow:MySqlOwnedRepositoryBorrow=null
    borrow=new MySqlOwnedRepositoryBorrow(thread,purposeBorrowGeneration,()=>purposeBorrow eq borrow,
      ()=>Option(primary),cause=>{primary=MySqlOwnedSessionLifecycle.combine(Option(primary),cause).orNull},
      Some(plan.tag),Some(scope),Some(barrier),Some(admission))
    purposeBorrow=borrow
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(originalRoot) {
      val result=borrow.invokePlannedCommand(plan,body)
      borrow.complete(result)
    }
    finally
      borrow.revoke()
      purposeBorrow=null

  private[mysql] def controlledDebug(plan:MySqlOwnedRepositoryOperationPlan,
      request:MySqlControlledDebugPrepareRequest,prepared:Option[MySqlControlledDebugPreparedGraph],
      activeLimit:Int):Try[MySqlControlledDebugExecution] =
    withPurposeCommand(plan)(command=>purposeRunner.executeControlledDebugWorkflow(command,request,prepared,activeLimit))

  private[mysql] def requestReadPreview(plan:MySqlOwnedRepositoryOperationPlan,
      request:MySqlRequestReadPreview):Try[Vector[MySqlRepositoryReply]] =
    require(request!=null,"fixed original schema5 read-preview request required")
    plan.requireTransaction(request.operations,inputs.sourceLimitProfile)
    withPurposeCommand(plan)(command=>purposeRunner.executeRequestReadPreview(command,request))

  private[mysql] def readOriginalDeployment(plan:MySqlOwnedRepositoryOperationPlan,
      operation:MySqlRepositoryOperation.FindOriginalDeploymentOperation):Try[Option[MySqlOriginalDeploymentOperation]] =
    require(operation!=null,"exact original deployment lookup required")
    plan.requireSingle(operation,inputs.sourceLimitProfile)
    withPurposeCommand(plan,Some(operation))(command=>purposeRunner.executeOriginalDeploymentRead(command,operation))

  private[mysql] def normalRecall(plan:MySqlOwnedRepositoryOperationPlan,operation:MySqlRepositoryOperation,
      prepared:Option[MySqlFencedTransaction.NormalRecallCommitted]):Try[MySqlFencedTransaction.NormalRecallCommitted] =
    withPurposeCommand(plan)(command=>purposeRunner.executeNormalRecallWorkflow(command,operation,prepared))

  private[mysql] def commitOwnedMode(plan:MySqlOwnedRepositoryOperationPlan,
      request:MySqlOwnedModeCommitRequest,activeLimit:Int):Try[MySqlOwnedModeExecution] =
    withPurposeCommand(plan) { command=>
      if request==null then throw new IllegalArgumentException("actual schema5 mode request required")
      plan.requireSingle(request.repositoryOperation,inputs.sourceLimitProfile)
      purposeRunner.planModeWorkflow(command,request.repositoryOperation).flatMap {
        case MySqlModeWorkflowPlanning.Rejected(outcome)=>Success(MySqlOwnedModeExecution.Rejected(outcome))
        case MySqlModeWorkflowPlanning.AlreadySet(readback)=>Success(MySqlOwnedModeExecution.AlreadySet(readback))
        case MySqlModeWorkflowPlanning.Execute(materialization)=>
          purposeRunner.executeModeWorkflow(command,request.repositoryOperation,materialization,activeLimit)
      }
    }

  /** 仅核验既有 DATA context；不再构造第二个 retained identity。 */
  private def requireRestrictedSourceExpectedContext(
      plan:MySqlFencedTransaction.SourceReservePlan5,expected:MySqlRepositoryOperationContextIdentity,
      dataBudget:MySqlFormat3ReadBudget):Unit =
    val parent=plan.parent
    val intent=plan.intent
    val data=plan.data
    val request=plan.exactRequest
    val before=plan.exactBefore
    val actualLedger=plan.actualLedger
    val proof=restrictedRecallProof
    val publication=restrictedPublication
    val readerLease=restrictedSourceReaderLease
    if expected==null || parent==null || intent==null || data==null || request==null || before==null ||
        actualLedger==null || proof==null || publication==null || readerLease==null || dataBudget==null ||
        (plan.issuer ne restrictedSourceRunner) || (plan.exactRoot ne root) ||
        (actualLedger ne restrictedSourceLedger) || (data.request ne request) || (data.before ne before) ||
        (parent.session ne this) || (parent.proof ne proof) || (parent.publication ne publication) ||
        (parent.readerLease ne readerLease) || (parent.originalRoot ne root) ||
        (parent.actualLedger ne actualLedger) || (parent.link ne restrictedSourceLink) ||
        (intent.exactLink ne parent.link) || (intent.parentReference ne parent.reference) ||
        restrictedReferencesIssued==null || !restrictedReferencesIssued.exists(_ eq parent.reference) ||
        request.binding!=parent.binding || request.childWorkflow!=intent.childWorkflow ||
        request.parentRecallWorkflow!=parent.parentRecallWorkflow || request.parentP2Operation!=parent.parentP2Operation ||
        request.reserveOperation!=intent.reserveOperation || request.plannedWrite!=intent.plannedWrite ||
        request.writeGeneration!=intent.writeGeneration || request.canonical==null ||
        request.canonical.sha256==null || request.canonical.sha256.size!=32 then
      throw new IllegalStateException("exact original retained restricted source plan/request lineage required",primary)
    root.requireLedgerIdentity(actualLedger)
    root.requireScriptDataInputs(inputs.full26Inputs)
    if (dataBudget.profile ne root.profile) || !dataBudget.isCanonicalChildOf(root) ||
        !dataBudget.sharesAllocationLedger(root) || dataBudget.isClosed then
      throw new IllegalStateException("exact open original source RESERVE DATA child required",primary)
    val expectation=inputs.auditExpectation
    val ioIdentity=identity
    val deployment=expectation.deploymentReceiptInputs.getOrElse(
      throw new SQLDataException("restricted R0 context schema5 inputs are unavailable"))
    val profile=deployment.limitProfile
    val schemaUtf8Bytes=Math.min(expectation.schema.length.toLong,64L)
    val namespaceTextBytes=Math.addExact(47L,schemaUtf8Bytes)
    val namespaceCanonicalBytes=Math.addExact(60L,namespaceTextBytes)
    val namespaceBufferAndCopies=Math.addExact(Math.multiplyExact(2L,namespaceCanonicalBytes),128L)
    val namespaceDigestAndNames=Math.addExact(32L,Math.addExact(43L,Math.addExact(43L,48L)))
    val namespaceTemporaryCharge=Math.addExact(512L,Math.addExact(namespaceBufferAndCopies,
      Math.addExact(namespaceTextBytes,namespaceDigestAndNames)))
    dataBudget.reserveTemporaryMemory(namespaceTemporaryCharge,
      "restricted fresh-write context comparison and SessionBarrierNamespace workspace BEFORE readmit")
    var primaryFailure:Throwable=null
    try
      proof.withCurrent(expectation,coordinator) { (actualVerified,actualScope,token,receipt) =>
        if actualVerified==null || actualScope==null || token==null || receipt==null || barrier==null ||
            ioIdentity==null || (actualVerified ne verified) || (actualScope ne scope) || token.fence==null ||
            token.qualification==null || actualScope.bootUuid!=ioIdentity.bootId ||
            actualScope.bootGeneration!=ioIdentity.bootGeneration ||
            actualScope.connectionGeneration!=ioIdentity.connectionGeneration ||
            (token.verified ne actualVerified) || (token.scope ne actualScope) || token.bootUuid!=ioIdentity.bootId ||
            token.bootGeneration!=ioIdentity.bootGeneration || token.connectionGeneration!=ioIdentity.connectionGeneration ||
            token.bindingUuid!=request.binding || token.writerEpoch<=0L || token.leaseUntil!=token.fence.leaseUntil ||
            token.fence.ownerBootUuid!=ioIdentity.bootId then
          throw new SQLDataException("restricted R0 expected context differs from the exact current Session/fence identities")
        val now=System.nanoTime()
        if !token.fence.isCurrentAt(now) then
          throw new SQLDataException("restricted R0 expected context writer fence is no longer current")
        val qualification=token.qualification
        if !qualification.isCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
            expectation.serverUuid,expectation.schema,expectation.bindingUuid,expectation.profile,
            expectation.expectedPhysicalFormat,actualVerified,Thread.currentThread(),now) then
          throw new SQLDataException("restricted R0 expected context schema qualification is stale or mismatched")
        receipt.assertCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
          expectation.serverUuid,expectation.schema,expectation.bindingUuid,expectation.profile,
          expectation.expectedPhysicalFormat,actualVerified,Some(token.fence),now)
        if expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 ||
            expectation.expectedPhysicalFormat!=MySqlExpectedPhysicalFormat.Format2 then
          throw new SQLDataException("restricted R0 expected context requires the approved schema5/physical2 audit target")
        deployment.requireIdentity(expectation.serverUuid,expectation.serverVersion,expectation.schema,
          expectation.effectiveUser,expectation.callerRoles,expectation.provisioningUuid,expectation.bindingUuid,
          expectation.ddlSha256,expectation.auditDefiner)
        val connectionIdentity=actualVerified.identity
        if profile==null || (profile ne root.profile) || (profile ne plan.exactRoot.profile) ||
            inputs.full26Inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
            (inputs.full26Inputs.sourceLimitProfile ne profile) ||
            before.inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
            (before.inputs.sourceLimitProfile ne profile) || before.schema!=expectation.schema ||
            before.connection.serverUuid!=expectation.serverUuid || before.connection.serverVersion!=expectation.serverVersion ||
            before.connection.catalog!=expectation.schema || before.connection.effectiveUser!=expectation.effectiveUser ||
            before.observationScope.bootUuid!=actualScope.bootUuid ||
            before.observationScope.bootGeneration!=actualScope.bootGeneration ||
            before.observationScope.connectionGeneration!=actualScope.connectionGeneration ||
            connectionIdentity.serverUuid!=expectation.serverUuid || connectionIdentity.serverVersion!=expectation.serverVersion ||
            connectionIdentity.catalog!=expectation.schema || connectionIdentity.effectiveUser!=expectation.effectiveUser then
          throw new SQLDataException("restricted R0 expected context lost its exact schema5 Full26/profile/session identity")
        try barrier.readmit(actualVerified,expectation,actualScope)
        catch
          case cause:Throwable=>
            if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
            throw cause
        val namespace=barrier.namespace.lockName()
        val contextTag=expected.tag
        val expectedMatches=contextTag.bootId==ioIdentity.bootId &&
          contextTag.bootGeneration==ioIdentity.bootGeneration &&
          contextTag.connectionGeneration==ioIdentity.connectionGeneration &&
          contextTag.operationId==request.reserveOperation && expected.workflowUuid==request.childWorkflow &&
          expected.reserveOperationUuid==request.reserveOperation &&
          expected.barrierGenerationUuid==barrier.connectionGenerationId &&
          expected.actualConnectionId==barrier.actualConnectionId && expected.barrierNamespace==namespace &&
          expected.bindingUuid==request.binding && expected.bindingUuid==token.bindingUuid &&
          expected.writerEpoch==token.writerEpoch && expected.fixedLeaseUntil==token.leaseUntil &&
          expected.serverUuid==connectionIdentity.serverUuid && expected.serverVersion==connectionIdentity.serverVersion &&
          expected.schema==connectionIdentity.catalog && expected.effectiveUser==connectionIdentity.effectiveUser &&
          expected.provisioningUuid==expectation.provisioningUuid &&
          expected.schemaFormat==expectation.profile.schemaFormat &&
          expected.physicalFormat==expectation.expectedPhysicalFormat.version && expected.ddlSha256==expectation.ddlSha256 &&
          expected.approvedObjectManifestSha256==expectation.objects.canonicalSha256 &&
          expected.configurationSha256==deployment.configurationSha256.bytes.toVector &&
          expected.sourceProvenanceKind==deployment.sourceProvenanceKind &&
          expected.sourceProvenanceSha256==deployment.sourceProvenanceSha256.bytes.toVector &&
          expected.limitProfileSha256==profile.sha256().toVector && (expected.exactLimitProfile eq profile) &&
          expected.requestKind=="RUN" && expected.requestIdentitySha256==request.canonical.sha256
        if !expectedMatches then
          throw new SQLDataException("restricted R0 expected context differs field-for-field from the current Session/request/deployment")
        val writerRows=before.relation("fp_runtime_writer").rows
        if writerRows==null || writerRows.size!=1 then
          throw new SQLDataException("restricted R0 expected context requires its exact singleton W row")
        val writer=writerRows.head
        def writerCell(name:String):MySqlFormat3Cell=MySqlModeOperationEffectCodec.value(writer,"fp_runtime_writer",name)
        val beforeEpoch=writerCell("writer_epoch") match
          case MySqlFormat3Cell.SignedInteger(value)=>value
          case _=>throw new SQLDataException("restricted R0 expected context retained W epoch is not an integer")
        val beforeLease=writerCell("lease_until") match
          case MySqlFormat3Cell.DateTime(value)=>value
          case _=>throw new SQLDataException("restricted R0 expected context retained W lease is not DATETIME(6)")
        if writerCell("singleton_fixed1")!=MySqlFormat3Cell.SignedInteger(1L) ||
            !MySqlModeOperationEffectCodec.uuidEquals(writerCell("binding_uuid"),token.bindingUuid) ||
            beforeEpoch!=token.writerEpoch ||
            !MySqlModeOperationEffectCodec.uuidEquals(writerCell("owner_boot_uuid"),token.fence.ownerBootUuid) ||
            token.fence.ownerBootUuid!=actualScope.bootUuid || beforeLease!=token.leaseUntil then
          throw new SQLDataException("restricted R0 expected context token binding/epoch/owner/fixed lease differs from retained W")
      }
    catch
      case cause:Throwable=>primaryFailure=cause;throw cause
    finally
      try dataBudget.releaseTemporaryMemory(namespaceTemporaryCharge,
        "restricted fresh-write context comparison and SessionBarrierNamespace workspace complete")
      catch
        case cleanup:Throwable=>
          if primaryFailure==null then
            if primary==null then primary=cleanup else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)
            throw cleanup
          else
            MySqlOwnedIoAdapter.suppressExitCleanup(primaryFailure,cleanup)
            if primary==null then primary=primaryFailure else MySqlOwnedIoAdapter.suppressExitCleanup(primary,primaryFailure)

  /** 仅在原 actor/worker 内借用锁定的 Full26 与 W；Connection 不离开此 bracket。 */
  private def withRestrictedSourceFreshWriteScope(
      plan:MySqlFencedTransaction.SourceReservePlan5,expected:MySqlRepositoryOperationContextIdentity)
      (created:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5=>Unit,
       consume:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5=>Unit):Unit =
    requireOwner()
    if plan==null || expected==null || created==null || consume==null then
      throw new IllegalArgumentException("exact retained source plan, original context and scope consumer required")
    plan.requireRetained()
    val parent=plan.parent
    val intent=plan.intent
    val data=plan.data
    val actualLedger=plan.actualLedger
    val readerLease=restrictedSourceReaderLease
    requireRestrictedSourceRunner(restrictedSourceRunner,restrictedRecallProof,restrictedPublication,
      root,actualLedger,readerLease)
    if (plan.parent ne parent) || (plan.intent ne intent) || (plan.data ne data) ||
        (plan.exactRoot ne root) || (plan.actualLedger ne restrictedSourceLedger) ||
        (parent.actualLedger ne actualLedger) || restrictedSourceFreshWriteScope!=null then
      throw new IllegalStateException("exact idle retained source RESERVE plan and unused Session scope slot required",primary)

    var allocatedReader:MySqlDeploymentReceiptReadOwner=null
    var allocatedTargetReader:MySqlDeploymentReceiptReadOwner=null
    var freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5=null
    var scopeHolderCharged=false
    var captureWorkspaceCharged=false
    var contextValidated=false
    var isolationReadAttempted=false
    val originalReadOwners=readOwners
    var failure:Throwable=null
    try
      plan.allocation.withRetainedPlanDataBudget(parent,root,actualLedger,intent,data) { dataBudget=>
        requireRestrictedSourceExpectedContext(plan,expected,dataBudget)
        contextValidated=true
        isolationReadAttempted=true
        val originalIsolation=verified.connection.getTransactionIsolation
        root.reserveRetainedMemory(1024L,"restricted fresh-write two-reader scope holder BEFORE allocation")
        scopeHolderCharged=true
        allocatedReader=allocateRestrictedSourceReader(readerLease)
        allocatedTargetReader=allocateRestrictedSourceReader(readerLease)
        freshScope=new MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5(
          this,actor,verified,scope,restrictedSourceRunner,restrictedRecallProof,restrictedPublication,
          restrictedSourceLink,readerLease,root,actualLedger,plan,expected,allocatedReader,allocatedTargetReader,dataBudget,
          new MySqlRollbackOnlyState,originalIsolation)
        restrictedSourceFreshWriteScope=freshScope
        scopeHolderCharged=false
        allocatedReader.registerRestrictedSourceFreshWriteScope(freshScope)
        allocatedTargetReader.registerRestrictedSourceFreshTargetReader(freshScope)
        created(freshScope)
        root.reserveTemporaryMemory(8192L,"Full26 transaction/root-lock/control workspace BEFORE gate/JDBC")
        captureWorkspaceCharged=true
        freshScope.markCaptureWorkspacePrecharged()
      }
      captureWorkspaceCharged=false
      val connection=verified.connection
      freshScope.gateEntryAttempted=true
      val gateEntryFailure=MySqlConnectionTransactionGate.enter(connection,freshScope.rollbackOnly)
      freshScope.gateEntryReturned=true
      gateEntryFailure match
        case Some(cause)=>freshScope.gateEntryRejected=true;throw cause
        case None=>freshScope.gateEntrySucceeded=true
      val actualGate=MySqlConnectionTransactionGate.currentOwner(connection,freshScope.rollbackOnly)
      freshScope.gateOwnerReturned=true
      freshScope.gateOwner=actualGate
      freshScope.transactionAttempted=true
      freshScope.isolationChangeAttempted=true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      freshScope.isolationChangeReturned=true
      freshScope.autoCommitChangeAttempted=true
      connection.setAutoCommit(false)
      freshScope.autoCommitChangeReturned=true
      if connection.getAutoCommit || connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLDataException("restricted fresh-write original RR transaction could not be established")
      freshScope.captureAttempted=true
      allocatedReader.captureRestrictedSourceLockedFull26(freshScope)
      freshScope.captureReturned=true
      freshScope.validateFreshFull26()
      freshScope.requireCurrentFence()
      freshScope.consumerAttempted=true
      consume(freshScope)
      freshScope.consumerReturned=true
      if !freshScope.commitAttempted then freshScope.requireCurrentFence()
      freshScope.rethrowStaleIfRecorded()
    catch
      case cause:Throwable=>failure=cause

    if freshScope==null then
      val partialReader:MySqlDeploymentReceiptReadOwner=if allocatedTargetReader!=null then allocatedTargetReader
        else if allocatedReader!=null then allocatedReader
        else if readOwners ne originalReadOwners then readOwners.headOption.orNull
        else null
      if partialReader!=null then
        // Scope construction did not return; keep the exact registered reader/charges under a sticky Session failure.
        restrictedSourceFreshWritePendingReader=partialReader
        restrictedSourceFreshWritePendingScopeHolder=scopeHolderCharged
        scopeHolderCharged=false
      else if scopeHolderCharged then
        try
          root.releaseRetainedMemory(1024L,"failed restricted fresh-write scope holder before reader allocation")
          scopeHolderCharged=false
        catch case cleanup:Throwable=>
          if failure==null then failure=cleanup else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
          restrictedSourceFreshWritePendingScopeHolder=true
      if failure!=null then
        if contextValidated || isolationReadAttempted || partialReader!=null || primary!=null then
          if primary==null then primary=failure else MySqlOwnedIoAdapter.suppressExitCleanup(primary,failure)
        throw failure
      throw new IllegalStateException("restricted fresh-write scope construction returned no issued scope",primary)

    if failure!=null then freshScope.markFailure(failure)
    var cleanupSucceeded=false
    val noOwnedGate=freshScope!=null && !freshScope.transactionAttempted && !freshScope.commitAttempted &&
      (!freshScope.gateEntryAttempted || freshScope.gateEntryRejected)
    if noOwnedGate then
      try
        freshScope.markSettledWithoutTransaction()
        if (captureWorkspaceCharged || freshScope.captureWorkspacePrecharged) &&
            !freshScope.captureWorkspaceRefundReturned then
          freshScope.captureWorkspaceRefundAttempted=true
          root.releaseTemporaryMemory(8192L,"restricted fresh-write pre-gate Full26 workspace retired")
          freshScope.captureWorkspaceRefundReturned=true
          captureWorkspaceCharged=false
        retireRestrictedSourceReaderRegistered(readerLease,restrictedSourceRunner,root,actualLedger,
          allocatedReader,freshScope)
        retireRestrictedSourceReaderRegistered(readerLease,restrictedSourceRunner,root,actualLedger,
          allocatedTargetReader,freshScope)
        if freshScope.readerRetirementComplete then restrictedSourceFreshWriteScope=null
        cleanupSucceeded=true
      catch
        case cleanup:Throwable=>
          if failure==null then failure=cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
    if freshScope.commitAttempted && freshScope.commitUnknown!=null then
      // UNKNOWN is a retained settlement-only state. The original gate, readers, scope, and DATA child stay owned.
      cleanupSucceeded=true
    else if freshScope.commitAcknowledged then
      try
        settleRestrictedSourceFreshWriteAfterCommit(freshScope)
        releaseRestrictedSourceFreshWriteWorkspaces(freshScope)
        cleanupSucceeded=true
      catch
        case cleanup:Throwable=>
          freshScope.markCommittedCleanupFailure(cleanup)
          cleanupSucceeded=true // the ACK remains authoritative; cleanup failure is retained on the exact scope.
    else if freshScope.transactionAttempted then
      try
        settleRestrictedSourceFreshWrite(freshScope)
        if allocatedReader.isSettled then
          freshScope.captureWorkspaceRefundAttempted=true
          root.releaseTemporaryMemory(8192L,"restricted fresh-write Full26 transaction/root-lock/control workspace complete")
          freshScope.captureWorkspaceRefundReturned=true
          if freshScope.targetCaptureWorkspacePrecharged then
            root.releaseTemporaryMemory(8192L,"restricted SOURCE R0 target Full26 capture workspace complete")
            freshScope.targetCaptureWorkspacePrecharged=false
          retireRestrictedSourceReaderRegistered(readerLease,restrictedSourceRunner,root,actualLedger,
            allocatedReader,freshScope)
          retireRestrictedSourceReaderRegistered(readerLease,restrictedSourceRunner,root,actualLedger,
            allocatedTargetReader,freshScope)
          if freshScope.readerRetirementComplete then restrictedSourceFreshWriteScope=null
          cleanupSucceeded=true
      catch
        case cleanup:Throwable=>
          if failure==null then failure=cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
    if failure==null && !cleanupSucceeded then
      failure=new IllegalStateException("restricted fresh-write scope did not confirm rollback/restoration/retirement")
    val readerFailure=allocatedReader.primaryFailure.orElse(Option(allocatedTargetReader).flatMap(_.primaryFailure))
    val stalePreDml=freshScope.isStaleRejection(failure) && !freshScope.barrierCleanupFailureObserved &&
      cleanupSucceeded && readerFailure.isEmpty
    if failure!=null then
      val rejectedNestedGate=freshScope.gateEntryRejected
      if !stalePreDml && (contextValidated || rejectedNestedGate || !cleanupSucceeded || readerFailure.nonEmpty) then
        if primary==null then primary=failure else MySqlOwnedIoAdapter.suppressExitCleanup(primary,failure)
      throw failure

  private def settleRestrictedSourceFreshWrite(
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if freshScope==null || (restrictedSourceFreshWriteScope ne freshScope) || !freshScope.transactionAttempted ||
        !freshScope.gateOwnerReturned || freshScope.isSettled then
      throw new IllegalStateException("exact active restricted fresh-write transaction settlement required",primary)
    val connection=verified.connection
    if freshScope.rollbackAttempted then
      throw new IllegalStateException("restricted fresh-write rollback was already attempted",freshScope.failure.orNull)
    freshScope.rollbackAttempted=true
    try
      connection.rollback()
      freshScope.rollbackReturned=true
      freshScope.autoCommitRestoreAttempted=true
      connection.setAutoCommit(true)
      freshScope.autoCommitRestoreCallReturned=true
      if !connection.getAutoCommit then throw new IllegalStateException("restricted fresh-write rollback did not restore autocommit")
      freshScope.autoCommitRestoreReturned=true
      freshScope.isolationRestoreAttempted=true
      connection.setTransactionIsolation(freshScope.originalIsolation)
      freshScope.isolationRestoreCallReturned=true
      if connection.getTransactionIsolation!=freshScope.originalIsolation then
        throw new IllegalStateException("restricted fresh-write rollback did not restore transaction isolation")
      freshScope.isolationRestoreReturned=true
      freshScope.gateLeaveAttempted=true
      MySqlConnectionTransactionGate.leave(connection,freshScope.rollbackOnly)
      freshScope.gateLeaveReturned=true
      freshScope.markSettled()
    catch
      case cause:Throwable=>
        freshScope.markFailure(cause)
        throw cause

  /** ACKed commit has no rollback path; restore only after the JDBC commit actually returned. */
  private def settleRestrictedSourceFreshWriteAfterCommit(
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if freshScope==null || (restrictedSourceFreshWriteScope ne freshScope) || !freshScope.commitAcknowledged ||
        !freshScope.transactionAttempted || !freshScope.gateOwnerReturned || freshScope.isSettled then
      throw new IllegalStateException("exact ACKed restricted SOURCE R0 transaction settlement required",primary)
    val connection=verified.connection
    freshScope.autoCommitRestoreAttempted=true
    connection.setAutoCommit(true)
    freshScope.autoCommitRestoreCallReturned=true
    if !connection.getAutoCommit then throw new IllegalStateException("ACKed restricted SOURCE R0 did not restore autocommit")
    freshScope.autoCommitRestoreReturned=true
    freshScope.isolationRestoreAttempted=true
    connection.setTransactionIsolation(freshScope.originalIsolation)
    freshScope.isolationRestoreCallReturned=true
    if connection.getTransactionIsolation!=freshScope.originalIsolation then
      throw new IllegalStateException("ACKed restricted SOURCE R0 did not restore transaction isolation")
    freshScope.isolationRestoreReturned=true
    freshScope.gateLeaveAttempted=true
    MySqlConnectionTransactionGate.leave(connection,freshScope.rollbackOnly)
    freshScope.gateLeaveReturned=true
    freshScope.markSettledAfterCommit()

  private def releaseRestrictedSourceFreshWriteWorkspaces(
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if freshScope.captureWorkspacePrecharged && !freshScope.captureWorkspaceRefundReturned then
      if freshScope.captureWorkspaceRefundAttempted then
        throw new IllegalStateException("ACKed restricted SOURCE R0 capture workspace refund has an uncertain prior outcome")
      freshScope.captureWorkspaceRefundAttempted=true
      root.releaseTemporaryMemory(8192L,"ACKed restricted SOURCE R0 before Full26 capture workspace complete")
      freshScope.captureWorkspaceRefundReturned=true
    if freshScope.targetCaptureWorkspacePrecharged then
      root.releaseTemporaryMemory(8192L,"ACKed restricted SOURCE R0 target Full26 capture workspace complete")
      freshScope.targetCaptureWorkspacePrecharged=false

  private def requireRestrictedSourceFreshWriteActive(
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    requireActor(actor)
    if freshScope==null || freshScope.plan==null then
      throw new IllegalStateException("exact active restricted fresh-write Session scope/plan required",primary)
    val plan=freshScope.plan
    val parent=plan.parent
    val intent=plan.intent
    val data=plan.data
    val request=plan.exactRequest
    val actualReader=freshScope.reader
    val actualTargetReader=freshScope.targetReader
    val planReader=plan.actualReader
    val planBefore=if planReader==null then null else plan.exactBefore
    val parentReader=if parent==null then null else parent.reader
    val parentCurrent=if parent==null then null else parent.currentFull26
    val parentAnchor=if parent==null then null else parent.anchorFull26
    val closure=plan.exactEffectClosure
    if (restrictedSourceFreshWriteScope ne freshScope) || freshScope.isSettled ||
        (freshScope.session ne this) || (freshScope.actor ne actor) || (freshScope.verified ne verified) ||
        (freshScope.auditScope ne scope) || (freshScope.runner ne restrictedSourceRunner) ||
        (freshScope.proof ne restrictedRecallProof) || (freshScope.publication ne restrictedPublication) ||
        (freshScope.link ne restrictedSourceLink) || (freshScope.readerLease ne restrictedSourceReaderLease) ||
        (freshScope.root ne root) || (freshScope.ledger ne restrictedSourceLedger) ||
        (freshScope.expectedContext==null) || (freshScope.dataBudget==null) ||
        (freshScope.dataBudget.profile ne root.profile) || !freshScope.dataBudget.isCanonicalChildOf(root) ||
        !freshScope.dataBudget.sharesAllocationLedger(root) || freshScope.dataBudget.isClosed ||
        plan==null || !plan.isIssued || (plan.issuer ne restrictedSourceRunner) ||
        (plan.parent ne parent) || (plan.intent ne intent) || (plan.data ne data) ||
        (plan.exactRoot ne root) || (plan.actualLedger ne restrictedSourceLedger) ||
        !plan.allocation.isOpen ||
        planReader==null || parentReader==null || parentCurrent==null || parentAnchor==null ||
        (planReader eq parentReader) || (planReader eq actualReader) || (parentReader eq actualReader) ||
        !restrictedSourceReaders.exists(_ eq parentReader) || !readOwners.exists(_ eq parentReader) ||
        !parentReader.ownsRetainedImage(parentCurrent) ||
        !readOwners.exists(reader=>reader.ownsRetainedImage(parentAnchor) && {
          reader.requireRestrictedSourceRetirementIdentity(this,actor,root)
          true
        }) ||
        !restrictedSourceReaders.exists(_ eq planReader) || !readOwners.exists(_ eq planReader) ||
        !planReader.ownsRetainedImage(planBefore) || plan.receiptForReplay!=null ||
        closure==null || !closure.belongsTo(plan,restrictedSourceRunner,root,root.profile) ||
        (closure.data ne data.effectClosure) || (intent.exactLink ne parent.link) ||
        (intent.parentReference ne parent.reference) || (parent.session ne this) ||
        (parent.proof ne restrictedRecallProof) || (parent.publication ne restrictedPublication) ||
        (parent.readerLease ne restrictedSourceReaderLease) || (parent.originalRoot ne root) ||
        (parent.actualLedger ne restrictedSourceLedger) || (parent.link ne restrictedSourceLink) ||
        (data.request ne plan.exactRequest) || (data.before ne planBefore) ||
        request.binding!=parent.binding || request.childWorkflow!=intent.childWorkflow ||
        request.parentRecallWorkflow!=parent.parentRecallWorkflow || request.parentP2Operation!=parent.parentP2Operation ||
        request.reserveOperation!=intent.reserveOperation || request.plannedWrite!=intent.plannedWrite ||
        request.writeGeneration!=intent.writeGeneration || actualReader==null || actualTargetReader==null ||
        (actualTargetReader eq actualReader) || (actualTargetReader eq parentReader) || (actualTargetReader eq planReader) ||
        !restrictedSourceReaders.exists(_ eq actualReader) || !readOwners.exists(_ eq actualReader) ||
        !restrictedSourceReaders.exists(_ eq actualTargetReader) || !readOwners.exists(_ eq actualTargetReader) ||
         actualReader.isSettled || actualTargetReader.isSettled || admission!=null || purposeRunner!=null || purposeLedger!=null ||
        quiescing || critical!=null || primary!=null || publicationLease==null ||
        (restrictedPublication.recovery ne restrictedSourceLink) || (restrictedPublication.tag ne submissionTag) then
      throw new IllegalStateException("exact active registered restricted fresh-write scope/plan/reader graph required",primary)
    parentReader.requireRestrictedSourceRetirementIdentity(this,actor,root)
    planReader.requireRestrictedSourceRetirementIdentity(this,actor,root)
    actualReader.requireRestrictedSourceRetirementIdentity(this,actor,root)
    actualTargetReader.requireRestrictedSourceRetirementIdentity(this,actor,root)
    root.requireLedgerIdentity(restrictedSourceLedger)
    root.requireScriptDataInputs(inputs.full26Inputs)
    freshScope.dataBudget.requireScriptDataInputs(inputs.full26Inputs)
    freshScope.readerLease.requireBinding(this,actor,restrictedRecallProof,restrictedPublication,
      restrictedSourceLink,root,restrictedSourceLedger)
    if freshScope.gate==null || !freshScope.transactionAttempted then
      throw new IllegalStateException("restricted fresh-write transaction gate/attempt registration is missing",primary)
    MySqlConnectionTransactionGate.requireCurrent(freshScope.gate,verified.connection,freshScope.rollbackOnly)
    if verified.connection.getAutoCommit ||
        verified.connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLDataException("restricted fresh-write scope lost its active RR transaction")
    freshScope.rollbackOnly.cause.foreach(throw _)
    restrictedRecallProof.withCurrent(inputs.auditExpectation,coordinator) { (actualVerified,actualScope,token,receipt) =>
      val ioIdentity=identity
      val expected=freshScope.expectedContext
      val now=System.nanoTime()
      if (actualVerified ne verified) || (actualScope ne scope) || token==null || receipt==null || token.fence==null ||
          token.qualification==null || (token.scope ne scope) || (token.verified ne verified) ||
          token.bootUuid!=ioIdentity.bootId || token.bootGeneration!=ioIdentity.bootGeneration ||
          token.connectionGeneration!=ioIdentity.connectionGeneration ||
          expected.tag.bootId!=ioIdentity.bootId || expected.tag.bootGeneration!=ioIdentity.bootGeneration ||
          expected.tag.connectionGeneration!=ioIdentity.connectionGeneration ||
          expected.tag.operationId!=plan.exactRequest.reserveOperation || token.bindingUuid!=expected.bindingUuid ||
          token.writerEpoch!=expected.writerEpoch || token.leaseUntil!=expected.fixedLeaseUntil ||
          token.leaseUntil!=token.fence.leaseUntil || token.fence.ownerBootUuid!=ioIdentity.bootId then
        freshScope.rejectStale(new SQLDataException("restricted fresh-write Session/token/context identity is stale"))
      if !token.fence.isCurrentAt(now) then
        freshScope.rejectStale(new SQLDataException("restricted fresh-write writer fence is no longer current"))
      if !token.qualification.isCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
          inputs.auditExpectation.serverUuid,inputs.auditExpectation.schema,inputs.auditExpectation.bindingUuid,
          inputs.auditExpectation.profile,inputs.auditExpectation.expectedPhysicalFormat,
          verified,Thread.currentThread(),now) then
        freshScope.rejectStale(new SQLDataException("restricted fresh-write schema qualification is stale"))
      try receipt.assertCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
        inputs.auditExpectation.serverUuid,inputs.auditExpectation.schema,inputs.auditExpectation.bindingUuid,
        inputs.auditExpectation.profile,inputs.auditExpectation.expectedPhysicalFormat,
        verified,Some(token.fence),now)
      catch case cause:SQLNonTransientConnectionException=>freshScope.rejectStale(cause)
    }

  /** Fenced 的 restricted R0 SQL 只从原 Session 已 gate/锁读的 before/target scope 进入。 */
  private[mysql] def requireRestrictedSourceFreshWriteExecutionScope(
      plan:MySqlFencedTransaction.SourceReservePlan5,
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      actualRunner:MySqlFencedTransaction):Unit =
    requireActor(actor)
    if plan==null || freshScope==null || actualRunner==null || (freshScope.plan ne plan) ||
        (freshScope.runner ne actualRunner) || (plan.issuer ne actualRunner) ||
        !freshScope.transactionAttempted || !freshScope.isolationChangeReturned ||
        !freshScope.autoCommitChangeReturned || !freshScope.gateOwnerReturned || !freshScope.captureReturned ||
        verified.connection.getAutoCommit ||
        verified.connection.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then
      throw new IllegalStateException("exact active original Session accepted R0 plan and RR/gate before scope required",primary)
    requireRestrictedSourceFreshWriteActive(freshScope)

  private def requireRestrictedSourceFreshWriter(
      freshScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      writer:MySqlBusinessWriterRow):Unit =
    requireRestrictedSourceFreshWriteActive(freshScope)
    if writer==null then freshScope.rejectStale(new SQLDataException("restricted fresh-write Full26 W row is absent"))
    restrictedRecallProof.withCurrent(inputs.auditExpectation,coordinator) { (_,_,token,_) =>
      val expected=freshScope.expectedContext
      if writer.bindingUuid!=expected.bindingUuid || writer.writerEpoch!=expected.writerEpoch ||
          !writer.ownerBootUuid.contains(expected.tag.bootId) || writer.leaseUntil!=expected.fixedLeaseUntil ||
          token.bindingUuid!=writer.bindingUuid || token.writerEpoch!=writer.writerEpoch ||
          token.fence.ownerBootUuid!=writer.ownerBootUuid.getOrElse(null) ||
          token.leaseUntil!=writer.leaseUntil then
        freshScope.rejectStale(new SQLDataException("restricted fresh-write locked W differs from exact token binding/epoch/owner/fixed lease"))
      if !writer.leaseUntil.isAfter(writer.databaseNow) then
        freshScope.rejectStale(new SQLDataException("restricted fresh-write locked W lease is not after locked database time"))
    }

  private[mysql] def commitTrackedDrift(plan:MySqlOwnedRepositoryOperationPlan,
      request:MySqlTrackedDriftRequest,activeLimit:Int):Try[MySqlTrackedDriftExecution] =
    withPurposeCommand(plan) { command=>
      if request==null then throw new IllegalArgumentException("actual schema5 drift request required")
      plan.requireSingle(request.repositoryOperation,inputs.sourceLimitProfile)
      purposeRunner.planDriftWorkflow(command,request.repositoryOperation).flatMap {
        case MySqlDriftWorkflowPlanning.Rejected(outcome)=>Success(MySqlTrackedDriftExecution.Rejected(outcome))
        case MySqlDriftWorkflowPlanning.Execute(materialization)=>
          purposeRunner.executeDriftWorkflow(command,request.repositoryOperation,materialization,activeLimit)
      }
    }

  private[mysql] def actualSourceLimitProfile:com.wi.floatingpod.config.SourceLimitProfile =
    requireOwner();inputs.sourceLimitProfile

  private[mysql] def markFailed(cause:Throwable):Unit =
    requireOwner()
    if cause==null then throw new IllegalArgumentException("original schema5 work failure required")
    primary=MySqlOwnedSessionLifecycle.combine(Option(primary),cause).orNull

  private[mysql] def requireRetainedIssuer(actual:MySqlOwnedRetainedPlanIssuer,ledger:MySqlFormat3AllocationLedger):Unit =
    requireOwner()
    if actual==null || (retainedPlanIssuer ne actual) then throw new IllegalStateException("original schema5 retained issuer required")
    root.requireLedgerIdentity(ledger)
    actual.requireDeploymentReceiptSession(this,inputs.sourceLimitProfile,ledger)

  private[mysql] def requireNativeRetirementPermit(actual:MySqlDeploymentReceiptSession.NativeRetirementPermit5,
      owner:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    requireOwner()
    if actual==null || (actual ne nativeRetirement) || (owner ne nativeOwner) || terminalIntent==null ||
        !quiescing || !sqlRetirementReturned || primary!=null then
      throw new IllegalStateException("original Main-ended/release-ACK/SQL-settled schema5 native permit required",primary)

  private[mysql] def requireTerminalHolder(actualActor:MySqlDeploymentReceiptBootstrapActor,
      intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    requireOwner()
    if (actualActor ne actor) || intent==null || (intent ne terminalIntent) || (intent.session ne this) ||
        !quiescing || !sqlRetirementReturned then throw new IllegalStateException("original SQL-settled schema5 terminal holder scope required")
  private[mysql] def requireTerminalReader(actual:MySqlDeploymentReceiptReadOwner,
      actualActor:MySqlDeploymentReceiptBootstrapActor,intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    requireTerminalHolder(actualActor,intent)
    if actual==null || !readOwners.exists(_ eq actual) then throw new IllegalStateException("original registered schema5 terminal reader required")
  private[mysql] def requireTerminalRoot(actualActor:MySqlDeploymentReceiptBootstrapActor,
      intent:MySqlDeploymentReceiptSession.TerminalIntent5,actualRoot:MySqlFormat3ReadBudget):Unit =
    requireTerminalHolder(actualActor,intent)
    if actualRoot ne root then throw new IllegalStateException("original schema5 terminal allocation root required")
  private[mysql] def requireTerminalNativeOwner(actual:MySqlDeploymentReceiptFreshNativeOwner,
      intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    requireTerminalHolder(actor,intent)
    if (actual ne nativeOwner) || primary!=null then throw new IllegalStateException("original settled schema5 native retirement owner required",primary)
  private[mysql] def requireTerminalPreparationActor(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
    requireOwner()
    if (actual ne actor) || !quiescing || primary!=null || terminalIntent!=null then
      throw new IllegalStateException("original quiesced schema5 terminal preparation actor required",primary)

  private[mysql] def prepareTerminal(mainEnded:com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded,
      issuer:Option[MySqlOwnedRetainedPlanIssuer],ledger:MySqlFormat3AllocationLedger,
      tag:IoOperationTag,repositoryEpoch:Long):MySqlDeploymentReceiptSession.TerminalIntent5 =
    requireOwner()
    if mainEnded==null then throw new IllegalArgumentException("actual original Main-ended receipt required")
    mainEnded.requireRegisteredIdentity(identity.bootId,identity.bootGeneration,identity.connectionGeneration)
    prepareTerminalFrom(MySqlDeploymentReceiptSession.TerminalMainDrop5.Ordinary(mainEnded),issuer,ledger,tag,repositoryEpoch)

  private[mysql] def prepareFreshTerminal(handoff:MySqlOwnedIoAdapter.FreshCompletionHandoff,
      issuer:MySqlOwnedRetainedPlanIssuer,ledger:MySqlFormat3AllocationLedger):MySqlDeploymentReceiptSession.TerminalIntent5 =
    requireOwner()
    if handoff==null || publication==null || (handoff.snapshot ne publication.immutableSnapshot) then
      throw new IllegalStateException("original Main Fresh-drop handoff and exact immutable publication required")
    prepareTerminalFrom(MySqlDeploymentReceiptSession.TerminalMainDrop5.Fresh(handoff),Some(issuer),ledger,handoff.tag,handoff.epoch)

  private def prepareTerminalFrom(mainDrop:MySqlDeploymentReceiptSession.TerminalMainDrop5,
      issuer:Option[MySqlOwnedRetainedPlanIssuer],ledger:MySqlFormat3AllocationLedger,
      tag:IoOperationTag,repositoryEpoch:Long):MySqlDeploymentReceiptSession.TerminalIntent5 =
    if !quiescing || primary!=null || terminalIntent!=null || mainDrop==null || issuer==null ||
        issuer.exists(_==null) || publication==null || publicationLease==null || publishedReply==null || critical!=null ||
        purposeBorrow!=null || purposeReads.nonEmpty then
      throw new IllegalStateException("original Main-ended/live publication/settled work required for schema5 terminal preparation",primary)
    identity.requireTag(tag)
    if (tag ne submissionTag) || (publication.tag ne tag) || publication.repositoryEpochAtSubmission!=repositoryEpoch then
      throw new IllegalStateException("schema5 terminal differs from original publication tag/epoch")
    root.requireLedgerIdentity(ledger)
    val completion=actor.completionForTerminal(this)
    val actual=issuer.getOrElse(throw new IllegalStateException("original schema5 retained issuer required"))
    requireRetainedIssuer(actual,ledger)
    mainDrop match
      case MySqlDeploymentReceiptSession.TerminalMainDrop5.Fresh(_) =>
        if !completion.isFreshPreparation then throw new IllegalStateException("Fresh drop cannot retire an Ordinary completion")
        actual.requireFreshCompletionSeal()
      case MySqlDeploymentReceiptSession.TerminalMainDrop5.Ordinary(_) =>
        if completion.isFreshPreparation then throw new IllegalStateException("Fresh preparation requires its actual dedicated Main-drop producer")
        actual.requireOrdinaryTerminalPrepared()
    // Main-ended 是原私有 producer 的真实 drop；此 worker 先完成 publication lease release，才发行 intent。
    publishedReply.owned.retireOriginalPublication(publication)
    publication.retireImmutableInput(this)
    publicationLease.close()
    publicationLease=null;publication=null;publishedReply=null
    terminalIntent=new MySqlDeploymentReceiptSession.TerminalIntent5(this,mainDrop,issuer,ledger,tag,repositoryEpoch)
    terminalIntent

  private[mysql] def requireActor(actual: MySqlDeploymentReceiptBootstrapActor): Unit =
    requireOwner()
    if actual == null || (actual ne actor) || !started || quiescing then
      throw new IllegalStateException("exact registered live schema5 bootstrap actor required", primary)
    if primary != null then throw primary

  private[mysql] def requireRestrictedSourceRunner(actual:MySqlFencedTransaction,
      proof:MySqlDeploymentReceiptSession.RestrictedRecall5,
      publication:MySqlDeploymentReceiptSession.RestrictedPublication5,
      actualRoot:MySqlFormat3ReadBudget,actualLedger:MySqlFormat3AllocationLedger,
      readerLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5):Unit =
    requireActor(actor)
    if actualRoot==null || actualLedger==null then
      throw new IllegalStateException("original restricted source root and allocation ledger required")
    root.requireLedgerIdentity(actualLedger)
    if actual==null || (actual ne restrictedSourceRunner) || proof==null ||
        (proof ne restrictedRecallProof) || publication==null || (publication ne restrictedPublication) ||
        readerLease==null || (readerLease ne restrictedSourceReaderLease) ||
        actualRoot==null || (actualRoot ne root) || (actualLedger ne restrictedSourceLedger) ||
        actualRoot.profile!=inputs.sourceLimitProfile || restrictedSourceLink==null ||
        (publication.recovery ne restrictedSourceLink) || admission!=null || purposeRunner!=null || purposeLedger!=null ||
        quiescing || critical!=null || restrictedSourceFreshWriteScope!=null ||
        restrictedSourceFreshWritePendingReader!=null || restrictedSourceFreshWritePendingScopeHolder ||
        !verified.connection.getAutoCommit then
      throw new IllegalStateException("exact live restricted schema5 source runner/root/ledger/proof/publication required",primary)
    proof.withCurrent(inputs.auditExpectation,coordinator) { (_,_,_,_)=>
      publication.requireCurrent()
      readerLease.requireBinding(this,actor,proof,publication,restrictedSourceLink,root,actualLedger)
    }

  private[mysql] def allocateRestrictedSourceReader(
      actualLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5):MySqlDeploymentReceiptReadOwner =
    requireOwner()
    if actualLease==null || (actualLease ne restrictedSourceReaderLease) || restrictedSourceRunner==null ||
        restrictedSourceLink==null || restrictedRecallProof==null || restrictedPublication==null then
      throw new IllegalStateException("original restricted Full26 reader lease is not installed",primary)
    val reader=allocateReader()
    restrictedSourceReaders=reader::restrictedSourceReaders
    reader

  private[mysql] def requireRestrictedSourceReader(
      actualLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      reader:MySqlDeploymentReceiptReadOwner):Unit =
    requireOwner()
    requireActor(actor)
    if actualLease==null || (actualLease ne restrictedSourceReaderLease) || restrictedSourceRunner==null ||
        restrictedSourceLink==null || restrictedRecallProof==null || restrictedPublication==null then
      throw new IllegalStateException("original restricted Full26 reader lease is not installed",primary)
    actualLease.requireBinding(this,actor,restrictedRecallProof,restrictedPublication,
      restrictedSourceLink,root,restrictedSourceLedger)
    if reader==null || !restrictedSourceReaders.exists(_ eq reader) || !readOwners.exists(_ eq reader) then
      throw new IllegalStateException("exact registered restricted source Full26 reader required",primary)

  private[mysql] def retireRestrictedSourceReader(
      actualLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      actualFenced:MySqlFencedTransaction,actualRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,reader:MySqlDeploymentReceiptReadOwner):Unit =
    requireRestrictedSourceRunner(actualFenced,restrictedRecallProof,restrictedPublication,actualRoot,actualLedger,actualLease)
    retireRestrictedSourceReaderRegistered(actualLease,actualFenced,actualRoot,actualLedger,reader,null)

  /** 活跃 Session 槽仍阻止重入时，供已结算 scope 复用同一 reader 退休流程。 */
  private def retireRestrictedSourceReaderRegistered(
      actualLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      actualFenced:MySqlFencedTransaction,actualRoot:MySqlFormat3ReadBudget,
      actualLedger:MySqlFormat3AllocationLedger,reader:MySqlDeploymentReceiptReadOwner,
      freshWriteScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if reader==null then throw new IllegalStateException("exact restricted source Full26 reader required",primary)
    reader.requireRestrictedSourceRetirementIdentity(this,actor,root)
    if freshWriteScope!=null && ((restrictedSourceFreshWriteScope ne freshWriteScope) ||
        (!freshWriteScope.isBoundReader(reader) && !freshWriteScope.isBoundTargetReader(reader)) ||
        !freshWriteScope.isSettled || !reader.isSettled) then
      throw new IllegalStateException("settled exact restricted fresh-write scope/reader required before retirement",primary)
    if reader.restrictedSourceRetirementFailure!=null then throw reader.restrictedSourceRetirementFailure
    if reader.restrictedSourceSnapshotRetirementReturned && reader.restrictedSourceHolderRefundReturned &&
        reader.restrictedSourceRegistryRemovalReturned && reader.readOwnerRegistryRemovalReturned &&
        (freshWriteScope==null || freshWriteScope.scopeHolderRefundReturned) then return
    requireRestrictedSourceReader(actualLease,reader)
    if !reader.restrictedSourceSnapshotRetirementReturned then
      try
        reader.retireSnapshot(actor)
        reader.restrictedSourceSnapshotRetirementReturned=true
      catch
        case cause:Throwable=>
          reader.restrictedSourceRetirementFailure=cause
          throw cause
    var remainingRestrictedSourceReaders:List[MySqlDeploymentReceiptReadOwner]=null
    var remainingReadOwners:List[MySqlDeploymentReceiptReadOwner]=null
    if !reader.restrictedSourceRegistryRemovalReturned || !reader.readOwnerRegistryRemovalReturned then
      try
        // 在原1024字节reader holder仍计费时构造两份替代列表；分配失败时两份原registry均保持不变。
        remainingRestrictedSourceReaders=restrictedSourceReaders.filterNot(_ eq reader)
        remainingReadOwners=readOwners.filterNot(_ eq reader)
      catch
        case cause:Throwable=>
          reader.restrictedSourceRetirementFailure=cause
          throw cause
    if !reader.restrictedSourceHolderRefundReturned then
      if reader.restrictedSourceHolderRefundAttempted then
        if reader.restrictedSourceRetirementFailure==null then
          reader.restrictedSourceRetirementFailure=
            new IllegalStateException("restricted source reader holder refund has an uncertain prior outcome",primary)
        throw reader.restrictedSourceRetirementFailure
      reader.restrictedSourceHolderRefundAttempted=true
      try
        root.releaseRetainedMemory(1024L,"restricted Full26 parent image and reader holder retired")
        reader.restrictedSourceHolderRefundReturned=true
      catch
        case cause:Throwable=>
          reader.restrictedSourceRetirementFailure=cause
          throw cause
    if !reader.restrictedSourceRegistryRemovalReturned || !reader.readOwnerRegistryRemovalReturned then
      restrictedSourceReaders=remainingRestrictedSourceReaders
      readOwners=remainingReadOwners
      reader.restrictedSourceRegistryRemovalReturned=true
      reader.readOwnerRegistryRemovalReturned=true
    if freshWriteScope!=null then
      val bothReadersRetired=Vector(freshWriteScope.reader,freshWriteScope.targetReader).forall(actual =>
        actual.restrictedSourceHolderRefundReturned && actual.restrictedSourceRegistryRemovalReturned &&
          actual.readOwnerRegistryRemovalReturned)
      if bothReadersRetired && !freshWriteScope.scopeHolderRefundReturned then
        if freshWriteScope.scopeHolderRefundAttempted then
          if reader.restrictedSourceRetirementFailure==null then
            reader.restrictedSourceRetirementFailure=
              new IllegalStateException("restricted two-reader scope holder refund has an uncertain prior outcome",primary)
          throw reader.restrictedSourceRetirementFailure
        freshWriteScope.scopeHolderRefundAttempted=true
        try
          root.releaseRetainedMemory(1024L,"restricted fresh-write before/target scope holder retired")
          freshWriteScope.scopeHolderRefundReturned=true
        catch
          case cause:Throwable=>
            reader.restrictedSourceRetirementFailure=cause
            throw cause
      freshWriteScope.readerRetirementReturned=bothReadersRetired && freshWriteScope.scopeHolderRefundReturned

  private[mysql] def withRestrictedSourceParent[A](link:MySqlOwnedIoAdapter.RecallRecoveryLink,
      reference:MySqlNormalRecallRecoveryReference)
      (action:(MySqlFencedTransaction,MySqlFencedTransaction.RestrictedSourceParent5)=>A):A =
    requireOwner()
    val runner=restrictedSourceRunner
    val proof=restrictedRecallProof
    val publication=restrictedPublication
    val ledger=restrictedSourceLedger
    val readerLease=restrictedSourceReaderLease
    requireRestrictedSourceRunner(runner,proof,publication,root,ledger,readerLease)
    if action==null || link==null || (link ne restrictedSourceLink) ||
        (publication.recovery ne link) || reference==null || restrictedReferencesIssued==null || link.references==null ||
        !restrictedReferencesIssued.exists(_ eq reference) ||
        !link.references.exists(_ eq reference) then
      throw new IllegalStateException("exact original restricted Boot link/reference/action required",primary)
    proof.withCurrent(inputs.auditExpectation,coordinator) { (_,_,_,_)=>
      publication.requireCurrent()
      val parent=runner.captureRestrictedSourceParent(link,reference)
      if parent==null then throw new IllegalStateException("restricted source parent issuer returned no parent")
      action(runner,parent)
    }

  /** 原restricted worker用Main locator取得Parent5，再由第二次fresh Full26签发已settle的只读R0计划。 */
  private[mysql] def prepareRestrictedSourceReservePlan(
      intent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent):MySqlFencedTransaction.SourceReservePlan5 =
    requireOwner()
    if intent==null then throw new IllegalArgumentException("original restricted source RESERVE intent required")
    withRestrictedSourceParent(intent.exactLink,intent.parentReference) { (runner,parent) =>
      intent.requireOriginalReservePlanBinding(intent.exactLink,intent.parentReference,root,
        intent.exactDomainObservation,intent.expectedSourceObservation)
      runner.prepareRestrictedSourceReservePlan(parent,intent,root)
    }

  /** 原 Session 从 retained R0 plan 发行纯 DATA context identity；不表示命令或提交。 */
  private[mysql] def captureRestrictedSourceReserveContextIdentity(
      plan:MySqlFencedTransaction.SourceReservePlan5):MySqlRepositoryOperationContextIdentity =
    requireOwner()
    if plan==null then throw new IllegalArgumentException("exact retained restricted source RESERVE plan required")
    plan.requireRetained()
    val parent=plan.parent
    val intent=plan.intent
    val data=plan.data
    val request=plan.exactRequest
    val before=plan.exactBefore
    val actualLedger=plan.actualLedger
    val proof=restrictedRecallProof
    val publication=restrictedPublication
    val readerLease=restrictedSourceReaderLease
    if parent==null || intent==null || data==null || request==null || before==null || actualLedger==null ||
        proof==null || publication==null || readerLease==null || (plan.issuer ne restrictedSourceRunner) ||
        (plan.exactRoot ne root) || (actualLedger ne restrictedSourceLedger) ||
        (data.request ne request) || (data.before ne before) || (parent.session ne this) ||
        (parent.proof ne proof) || (parent.publication ne publication) || (parent.readerLease ne readerLease) ||
        (parent.originalRoot ne root) || (parent.actualLedger ne actualLedger) || (parent.link ne restrictedSourceLink) ||
        (intent.exactLink ne parent.link) || (intent.parentReference ne parent.reference) ||
        restrictedReferencesIssued==null || !restrictedReferencesIssued.exists(_ eq parent.reference) ||
        request.binding!=parent.binding || request.childWorkflow!=intent.childWorkflow ||
        request.parentRecallWorkflow!=parent.parentRecallWorkflow || request.parentP2Operation!=parent.parentP2Operation ||
        request.reserveOperation!=intent.reserveOperation || request.plannedWrite!=intent.plannedWrite ||
        request.writeGeneration!=intent.writeGeneration || request.canonical==null ||
        request.canonical.sha256==null || request.canonical.sha256.size!=32 then
      throw new IllegalStateException("exact original retained restricted source plan/request lineage required",primary)
    requireRestrictedSourceRunner(restrictedSourceRunner,proof,publication,root,actualLedger,readerLease)
    proof.withCurrent(inputs.auditExpectation,coordinator) { (actualVerified,actualScope,token,receipt) =>
      val expectation=inputs.auditExpectation
      val ioIdentity=identity
      if actualVerified==null || actualScope==null || token==null || receipt==null || barrier==null || ioIdentity==null ||
          (actualVerified ne verified) || (actualScope ne scope) || token.fence==null || token.qualification==null ||
          actualScope.bootUuid!=ioIdentity.bootId || actualScope.bootGeneration!=ioIdentity.bootGeneration ||
          actualScope.connectionGeneration!=ioIdentity.connectionGeneration ||
          (token.verified ne actualVerified) || (token.scope ne actualScope) ||
          token.bootUuid!=ioIdentity.bootId || token.bootGeneration!=ioIdentity.bootGeneration ||
          token.connectionGeneration!=ioIdentity.connectionGeneration || token.bindingUuid!=request.binding ||
          token.writerEpoch<=0L || token.leaseUntil!=token.fence.leaseUntil ||
          token.fence.ownerBootUuid!=ioIdentity.bootId then
        throw new SQLDataException("restricted R0 context differs from the exact current Session/fence identities")
      val now=System.nanoTime()
      if !token.fence.isCurrentAt(now) then
        throw new SQLDataException("restricted R0 context writer fence is no longer current")
      val qualification=token.qualification
      if !qualification.isCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
          expectation.serverUuid,expectation.schema,expectation.bindingUuid,expectation.profile,
          expectation.expectedPhysicalFormat,actualVerified,Thread.currentThread(),now) then
        throw new SQLDataException("restricted R0 context schema qualification is stale or mismatched")
      receipt.assertCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
        expectation.serverUuid,expectation.schema,expectation.bindingUuid,expectation.profile,
        expectation.expectedPhysicalFormat,actualVerified,Some(token.fence),now)
      if expectation.profile!=MySqlSchemaAuditProfile.DeploymentReceipt1 ||
          expectation.expectedPhysicalFormat!=MySqlExpectedPhysicalFormat.Format2 then
        throw new SQLDataException("restricted R0 context requires the approved schema5/physical2 audit target")
      val deployment=expectation.deploymentReceiptInputs.getOrElse(
        throw new SQLDataException("restricted R0 context schema5 inputs are unavailable"))
      deployment.requireIdentity(expectation.serverUuid,expectation.serverVersion,expectation.schema,
        expectation.effectiveUser,expectation.callerRoles,expectation.provisioningUuid,expectation.bindingUuid,
        expectation.ddlSha256,expectation.auditDefiner)
      val profile=deployment.limitProfile
      val connectionIdentity=actualVerified.identity
      if profile==null || (profile ne root.profile) || (profile ne plan.exactRoot.profile) ||
          inputs.full26Inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
          (inputs.full26Inputs.sourceLimitProfile ne profile) ||
          before.inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
          (before.inputs.sourceLimitProfile ne profile) || before.schema!=expectation.schema ||
          before.connection.serverUuid!=expectation.serverUuid || before.connection.serverVersion!=expectation.serverVersion ||
          before.connection.catalog!=expectation.schema || before.connection.effectiveUser!=expectation.effectiveUser ||
          before.observationScope.bootUuid!=actualScope.bootUuid ||
          before.observationScope.bootGeneration!=actualScope.bootGeneration ||
          before.observationScope.connectionGeneration!=actualScope.connectionGeneration ||
          connectionIdentity.serverUuid!=expectation.serverUuid || connectionIdentity.serverVersion!=expectation.serverVersion ||
          connectionIdentity.catalog!=expectation.schema || connectionIdentity.effectiveUser!=expectation.effectiveUser then
        throw new SQLDataException("restricted R0 context lost its exact schema5 Full26/profile/session identity")
      plan.allocation.withRetainedPlanDataBudget(parent,plan.exactRoot,actualLedger,intent,data) { dataBudget =>
        // forIdentity encodes 29-byte DOMAIN_VERSION + 18-byte APP_ID + an ASCII schema of at most 64 bytes,
        // three int32 text lengths and three UUIDs (60 fixed canonical bytes). Reserve for its maximum BAOS
        // growth/copy, all UTF-8/hash/Base64/name byte arrays, plus a fixed JDK wrapper/provider allowance.
        val schemaUtf8Bytes=if expectation.schema==null then 0L else Math.min(expectation.schema.length.toLong,64L)
        val namespaceTextBytes=Math.addExact(47L,schemaUtf8Bytes)
        val namespaceCanonicalBytes=Math.addExact(60L,namespaceTextBytes)
        val namespaceBufferAndCopies=Math.addExact(Math.multiplyExact(2L,namespaceCanonicalBytes),128L)
        // SHA-256, unpadded Base64 bytes, its intermediate String, and the final 48-byte lock-name String.
        val namespaceDigestAndNames=Math.addExact(32L,Math.addExact(43L,Math.addExact(43L,48L)))
        // Bounded allowance for stream/namespace wrappers, object headers, encoder and SHA provider state.
        val namespaceJdkWrapperAllowance=512L
        val namespaceTemporaryCharge=Math.addExact(namespaceJdkWrapperAllowance,Math.addExact(
          namespaceBufferAndCopies,Math.addExact(namespaceTextBytes,namespaceDigestAndNames)))
        var namespaceTemporaryCharged=false
        var namespaceFailure:Throwable=null
        try
          dataBudget.reserveTemporaryMemory(namespaceTemporaryCharge,
            "actual SessionBarrierNamespace canonical/hash/Base64 temporary workspace BEFORE readmit")
          namespaceTemporaryCharged=true
          barrier.readmit(actualVerified,expectation,actualScope)
        catch
          case cause:Throwable=>namespaceFailure=cause;throw cause
        finally
          if namespaceTemporaryCharged then
            try dataBudget.releaseTemporaryMemory(namespaceTemporaryCharge,
              "actual SessionBarrierNamespace canonical/hash/Base64 temporary workspace")
            catch case cleanup:Throwable=>
              if namespaceFailure==null then throw cleanup
              else MySqlOwnedSessionLifecycle.suppress(namespaceFailure,cleanup)
        if barrier.actualConnectionId<=0L || barrier.connectionGenerationId==null || barrier.namespace==null then
          throw new SQLDataException("restricted R0 context actual held-session barrier identity is malformed")
        val writerRows=before.relation("fp_runtime_writer").rows
        if writerRows==null || writerRows.size!=1 then
          throw new SQLDataException("restricted R0 context requires the exact singleton W row from retained Full26")
        val writer=writerRows.head
        def writerCell(name:String):MySqlFormat3Cell = MySqlModeOperationEffectCodec.value(writer,"fp_runtime_writer",name)
        val beforeEpoch=writerCell("writer_epoch") match
          case MySqlFormat3Cell.SignedInteger(value)=>value
          case _=>throw new SQLDataException("restricted R0 retained Full26 W epoch is not an integer")
        val beforeLease=writerCell("lease_until") match
          case MySqlFormat3Cell.DateTime(value)=>value
          case _=>throw new SQLDataException("restricted R0 retained Full26 W lease is not DATETIME(6)")
        if writerCell("singleton_fixed1")!=MySqlFormat3Cell.SignedInteger(1L) ||
            !MySqlModeOperationEffectCodec.uuidEquals(writerCell("binding_uuid"),token.bindingUuid) ||
            beforeEpoch!=token.writerEpoch ||
            !MySqlModeOperationEffectCodec.uuidEquals(writerCell("owner_boot_uuid"),token.fence.ownerBootUuid) ||
            token.fence.ownerBootUuid!=actualScope.bootUuid || beforeLease!=token.leaseUntil then
          throw new SQLDataException("restricted R0 token binding/epoch/owner/fixed lease differs from its retained Full26 W row")
        val retainedCharge=640L
        val temporaryCharge=512L
        var retainedCharged=false
        var constructionSucceeded=false
        var temporaryCharged=false
        var primaryFailure:Throwable=null
        try
          dataBudget.reserveRetainedMemory(retainedCharge,
            "restricted R0 context identity/tag/namespace and three schema5 digest vectors BEFORE allocation")
          retainedCharged=true
          dataBudget.reserveTemporaryMemory(temporaryCharge,
            "restricted R0 configuration/provenance/profile digest vector construction BEFORE allocation")
          temporaryCharged=true
          val tag=IoOperationTag(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,request.reserveOperation)
          ioIdentity.requireTag(tag)
          val barrierNamespace=barrier.namespace.lockName()
          if barrierNamespace==null then throw new SQLDataException("restricted R0 held-session namespace is unavailable")
          val configurationSha256=deployment.configurationSha256.bytes.toVector
          val sourceProvenanceSha256=deployment.sourceProvenanceSha256.bytes.toVector
          val limitProfileSha256=profile.sha256().toVector
          val result=new MySqlRepositoryOperationContextIdentity(tag,request.childWorkflow,request.reserveOperation,
            barrier.connectionGenerationId,barrier.actualConnectionId,barrierNamespace,request.binding,
            token.writerEpoch,token.leaseUntil,connectionIdentity.serverUuid,connectionIdentity.serverVersion,
            connectionIdentity.catalog,connectionIdentity.effectiveUser,expectation.provisioningUuid,
            expectation.profile.schemaFormat,expectation.expectedPhysicalFormat.version,expectation.ddlSha256,
            expectation.objects.canonicalSha256,configurationSha256,deployment.sourceProvenanceKind,
            sourceProvenanceSha256,limitProfileSha256,"RUN",request.canonical.sha256,profile)
          constructionSucceeded=true
          result
        catch
          case cause:Throwable=>primaryFailure=cause;throw cause
        finally
          var cleanupFailure:Throwable=null
          if temporaryCharged then
            try dataBudget.releaseTemporaryMemory(temporaryCharge,"restricted R0 context digest vector construction")
            catch case cleanup:Throwable=>
              if cleanupFailure==null then cleanupFailure=cleanup
              else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cleanup)
          if retainedCharged && (!constructionSucceeded || cleanupFailure!=null) then
            try dataBudget.releaseRetainedMemory(retainedCharge,"failed restricted R0 context identity construction")
            catch case cleanup:Throwable=>
              if cleanupFailure==null then cleanupFailure=cleanup
              else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cleanup)
          if cleanupFailure!=null then
            if primaryFailure==null then throw cleanupFailure
            else MySqlOwnedSessionLifecycle.suppress(primaryFailure,cleanupFailure)
      }
    }

  /** Execute exactly one accepted restricted-source R0 on its original worker and retain its outcome. */
  private[mysql] def executeRestrictedSourceReserveR0(
      plan:MySqlFencedTransaction.SourceReservePlan5,
      exactTag:IoOperationTag,
      acceptedRepositoryEpochAtSubmission:Long):MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome =
    requireOwner()
    if plan==null || exactTag==null || acceptedRepositoryEpochAtSubmission<0L ||
        restrictedSourceR0ExecutionOutcome!=null || restrictedSourceFreshWriteScope!=null ||
        restrictedSourceFreshWritePendingReader!=null || restrictedSourceFreshWritePendingScopeHolder then
      throw new IllegalStateException("one original-worker restricted SOURCE R0 execution slot and exact retained plan are required",primary)
    plan.requireRetained()
    val intent=plan.intent
    val request=plan.exactRequest
    if (plan.issuer ne restrictedSourceRunner) || (plan.exactRoot ne root) ||
        (plan.actualLedger ne restrictedSourceLedger) || (intent.exactLink ne restrictedSourceLink) ||
        (intent.parentReference ne plan.parent.reference) || exactTag.bootId!=identity.bootId ||
        exactTag.bootGeneration!=identity.bootGeneration || exactTag.connectionGeneration!=identity.connectionGeneration ||
        exactTag.operationId!=request.reserveOperation ||
        acceptedRepositoryEpochAtSubmission!=intent.expectedSourceObservation.settledRepositoryEpoch ||
        request.reserveOperation!=intent.reserveOperation then
      throw new IllegalStateException("accepted restricted SOURCE R0 tag/epoch differs from the exact retained intent and plan",primary)
    val parent=plan.parent
    val ledger=plan.actualLedger
    var resultHolder:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome=null
    var holderChargeReturned=false
    plan.allocation.withRetainedPlanDataBudget(parent,root,ledger,intent,plan.data) { dataBudget =>
      dataBudget.reserveRetainedMemory(1024L,"accepted restricted SOURCE R0 retained outcome holder BEFORE allocation")
      var made=false
      try
        resultHolder=new MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome(
          plan,exactTag,acceptedRepositoryEpochAtSubmission,dataBudget)
        made=true
        resultHolder
      finally
        if !made then
          dataBudget.releaseRetainedMemory(1024L,"failed accepted restricted SOURCE R0 outcome holder allocation")
          holderChargeReturned=true
    }
    if holderChargeReturned || resultHolder==null then
      throw new IllegalStateException("accepted restricted SOURCE R0 retained outcome allocation did not complete",primary)
    restrictedSourceR0ExecutionOutcome=resultHolder
    var activeScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5=null
    var failure:Throwable=null
    try
      requireRestrictedSourceRunner(restrictedSourceRunner,restrictedRecallProof,restrictedPublication,
        root,ledger,restrictedSourceReaderLease)
      if !verified.connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("restricted SOURCE R0 plannedAt requires the original idle verified connection")
      // H1: the first real database timestamp precedes context capture on this same verified connection.
      val plannedAt=restrictedSourceRunner.sampleRestrictedSourceR0DatabaseTime(plan,
        "restricted SOURCE R0 plannedAt BEFORE context capture")
      val expectedContext=captureRestrictedSourceReserveContextIdentity(plan)
      if expectedContext.tag!=exactTag || expectedContext.reserveOperationUuid!=request.reserveOperation ||
          expectedContext.workflowUuid!=request.childWorkflow || expectedContext.bindingUuid!=request.binding ||
          expectedContext.writerEpoch<=0L || expectedContext.fixedLeaseUntil==null then
        throw new SQLDataException("restricted SOURCE R0 actual context identity differs from the accepted command and request")
      // The frozen workflow and canonical A/B builders are idle-only. Sample resultAt and derive them before RR/gate.
      val resultAt=restrictedSourceRunner.sampleRestrictedSourceR0DatabaseTime(plan,
        "restricted SOURCE R0 resultAt after context validation and before A")
      if plannedAt.getYear<1000 || plannedAt.getYear>9999 || plannedAt.getNano%1000!=0 ||
          resultAt.getYear<1000 || resultAt.getYear>9999 || resultAt.getNano%1000!=0 ||
          resultAt.isBefore(plannedAt) || !resultAt.isBefore(expectedContext.fixedLeaseUntil) then
        throw new SQLDataException("restricted SOURCE R0 actual database timestamps violate H1 DATETIME(6) ordering or fixed lease")
      val witness=MySqlNormalRecallSourceWorkflow.sourceChildReservePrecommitWitnessData(
        plan.data,expectedContext,plannedAt,resultAt)
      val expectedTarget=MySqlNormalRecallSourceWorkflow.sourceChildReservePrecommitTargetSnapshotData(plan.data,witness)
      var actualBefore:MySqlScriptSchemaSnapshot=null
      var beforeWriter:MySqlBusinessWriterRow=null
      withRestrictedSourceFreshWriteScope(plan,expectedContext)(created=>activeScope=created, freshScope=>
        freshScope.withFreshFull26 { (lockedBefore,lockedWriter) =>
          actualBefore=lockedBefore
          beforeWriter=lockedWriter
        }
        restrictedSourceRunner.executeRestrictedSourceR0Dml(plan,freshScope,witness,expectedTarget,
          actualBefore,beforeWriter)
        freshScope.prechargeTargetCaptureWorkspace()
        freshScope.validateFreshTarget(expectedTarget)
        val operationContext=witness.originalContext.context
        resultHolder.retainPrecommit(freshScope,operationContext,plannedAt,resultAt,witness,expectedTarget,
          actualBefore,beforeWriter)
        restrictedSourceRunner.commitRestrictedSourceR0(plan,freshScope,operationContext))
    catch
      case cause:Throwable=>failure=cause

    val settledScope=activeScope
    if settledScope!=null && settledScope.commitUnknown!=null then
      resultHolder.markCommitUnknown(settledScope)
    else if settledScope!=null && settledScope.commitAcknowledged then
      resultHolder.markCommitAcknowledged(settledScope)
    else if settledScope!=null && settledScope.isSettled && settledScope.readerRetirementComplete &&
        !settledScope.commitAttempted then
      val cause=Option(failure).orElse(settledScope.failure).getOrElse(
        new IllegalStateException("restricted SOURCE R0 scope settled without a commit attempt"))
      resultHolder.markKnownNotCommitted(cause)
    else if settledScope==null && restrictedSourceFreshWritePendingReader==null &&
        !restrictedSourceFreshWritePendingScopeHolder && primary==null then
      val cause=Option(failure).getOrElse(
        new IllegalStateException("restricted SOURCE R0 ended before opening its fresh transaction"))
      resultHolder.markKnownNotCommitted(cause)
    else
      val cause=Option(failure).orElse(Option(settledScope).flatMap(_.failure)).getOrElse(
        new IllegalStateException("restricted SOURCE R0 outcome lacks confirmed commit or complete precommit cleanup",primary))
      resultHolder.markSettlementUncertain(cause)
    resultHolder.committedCleanupFailure=Option(settledScope).flatMap(scope=>Option(scope.committedCleanupFailure)).orNull
    if resultHolder.state==MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitUnknown then
      executeUnknownRestrictedSourceR0Inspection(plan,resultHolder,settledScope)
    else if resultHolder.state==MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitAcknowledged then
      if resultHolder.committedCleanupFailure!=null then
        resultHolder.markPostCommitReadbackBlocked(resultHolder.committedCleanupFailure)
      else
        executeAcknowledgedRestrictedSourceR0Readback(plan,resultHolder,settledScope)
    resultHolder

  /** One original-worker, newly owned UNKNOWN inspection; every failure keeps the original outcome and its UNKNOWN truth. */
  private def executeUnknownRestrictedSourceR0Inspection(
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
      exactScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    var owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5=null
    try
      requireOwner()
      if exactPlan==null || exactOutcome==null || exactScope==null ||
          (restrictedSourceR0ExecutionOutcome ne exactOutcome) || (exactOutcome.plan ne exactPlan) ||
          (exactOutcome.freshScope ne exactScope) ||
          exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitUnknown ||
          exactOutcome.commitUnknown==null || exactOutcome.unknownInspection.nonEmpty ||
          restrictedSourceR0UnknownInspection!=null || !exactOutcome.verifiedPrecommit ||
          !exactScope.commitAttempted || exactScope.commitAcknowledged ||
          (exactScope.commitUnknown ne exactOutcome.commitUnknown) ||
          exactOutcome.context==null || exactOutcome.witness==null || exactOutcome.actualBefore==null ||
          exactOutcome.expectedTarget==null || exactOutcome.beforeWriter==null ||
          (exactScope ne restrictedSourceFreshWriteScope) || (verified==null) || (scope==null) || (barrier==null) then
        throw new IllegalStateException("exact accepted restricted SOURCE R0 UNKNOWN outcome and original Session owners required",Option(primary).orNull)
      root.requireLedgerIdentity(restrictedSourceLedger)
      root.requireScriptDataInputs(inputs.full26Inputs)
      val dataBudget=exactOutcome.dataBudget
      if dataBudget==null || dataBudget.isClosed || !dataBudget.isCanonicalChildOf(root) ||
          !dataBudget.sharesAllocationLedger(root) || (dataBudget.profile ne root.profile) ||
          (exactPlan.exactRoot ne root) || (exactPlan.actualLedger ne restrictedSourceLedger) ||
          ((exactPlan.intent ne restrictedSourceLink) && (exactPlan.intent.exactLink ne restrictedSourceLink)) then
        throw new IllegalStateException("original restricted SOURCE R0 root/ledger/DATA graph changed before UNKNOWN inspection",Option(primary).orNull)
      dataBudget.reserveRetainedMemory(1024L,"restricted SOURCE R0 UNKNOWN inspection responsibility BEFORE allocation")
      var holderMade=false
      var holderFailure:Throwable=null
      try
        owner=new MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5(
          this,exactPlan,exactOutcome,exactScope,verified,scope,barrier,restrictedSourceReaderLease,
          root,restrictedSourceLedger,dataBudget,thread,exactOutcome.commitUnknown)
        holderMade=true
        restrictedSourceR0UnknownInspection=owner
        exactOutcome.beginUnknownInspection(exactScope,owner)
      catch
        case cause:Throwable=>holderFailure=cause;throw cause
      finally
        if !holderMade then
          try dataBudget.releaseRetainedMemory(1024L,"failed restricted SOURCE R0 UNKNOWN responsibility allocation")
          catch
            case cleanup:Throwable=>
              if holderFailure==null then throw cleanup
              else MySqlOwnedIoAdapter.suppressExitCleanup(holderFailure,cleanup)
      dataBudget.reserveRetainedMemory(512L,"UNKNOWN inspection audit-account owner BEFORE allocation")
      val auditAccount=root.newCanonicalScope()
      owner.bindAuditAccount(auditAccount)
      exactPlan.allocation.withUnknownInspectionR0DataBudget(exactPlan,exactOutcome,owner) { exactData =>
        if (exactData ne dataBudget) || exactData.isClosed ||
            !exactData.isCanonicalChildOf(root) || !exactData.sharesAllocationLedger(root) then
          throw new SQLNonTransientConnectionException("exact retained original UNKNOWN DATA child required")
        owner.attemptStarted=true
        owner.oldCloseAttempted=true
        var closeFailure:Throwable=null
        try
          owner.oldVerified.close()
          owner.oldCloseReturned=true
        catch
          case cause:Throwable=>
            closeFailure=cause
            owner.oldCloseFailure=cause
        try owner.oldCloseObservation=owner.oldVerified.sessionBarrierCloseObservation()
        catch
          case cause:Throwable=>
            if closeFailure==null then closeFailure=cause
            else MySqlOwnedIoAdapter.suppressExitCleanup(closeFailure,cause)
        if closeFailure!=null then throw closeFailure

        val nextGeneration=Math.incrementExact(lastUnknownInspectionConnectionGeneration)
        if nextGeneration<=owner.oldScope.connectionGeneration then
          throw new IllegalStateException("UNKNOWN inspection connection generation must advance beyond the original session")
        lastUnknownInspectionConnectionGeneration=nextGeneration
        owner.inspectionGeneration=nextGeneration
        exactData.reserveRetainedMemory(512L,"UNKNOWN inspection partial source-open owner BEFORE allocation")
        val source=new MySqlConnectionSource(inputs.settings)
        val openHolder=source.newSourceUnknownInspectionOpen()
        owner.bindOpenHolder(openHolder)
        val newVerified=openHolder.openOnce()
        owner.dataBudget.reserveRetainedMemory(512L,
          "UNKNOWN fresh audit boot-scope owner BEFORE allocation")
        val newScope=MySqlAuditBootScope.forOwnedIo(newVerified,thread,identity.bootId,
          identity.bootGeneration,nextGeneration)
        owner.bindNewConnection(newVerified,newScope)
        val newBarrier=newVerified.acquireSessionBarrier(inputs.auditExpectation,newScope,Duration.ZERO) match
          case MySqlSessionBarrierAcquisition.Held(value)=>value
          case MySqlSessionBarrierAcquisition.Deferred=>
            throw new SQLTransientConnectionException("restricted SOURCE R0 UNKNOWN inspection session barrier is busy")
        owner.bindBarrier(newBarrier)
        newBarrier.readmit(newVerified,inputs.auditExpectation,newScope)
        auditAccount.reserveRetainedMemory(256L,
          "schema5 UNKNOWN auditor issuer BEFORE allocation")
        val newAuditor=new MySqlSchemaAuditor(inputs.auditExpectation,auditMaxAge)
        val audit=newAuditor.beginSourceUnknownInspectionAudit(newVerified,newScope,root,auditAccount,owner)
        owner.bindAudit(audit)
        val qualification=audit.readAndQualifyOnce()
        owner.bindQualification(qualification)
        MySqlDeploymentReceiptSession.requireFreshQualification(qualification,inputs.auditExpectation,newVerified,newScope)
        newBarrier.readmit(newVerified,inputs.auditExpectation,newScope)
        val reader=allocateRestrictedSourceUnknownInspectionReader(owner)
        reader.captureLockedFull26(None)
        reader.settleSuccessfulRead(actor)
        owner.markReaderTransactionSettled()
        reader.withRetainedBefore(actor) { (actual,actualWriter)=>
          owner.retainObserved(actual,actualWriter)
          validateUnknownInspectionFacts(owner,actual,actualWriter)
          val classification=
            if sameFull26Data(actual,exactOutcome.expectedTarget,owner) then
              MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspectionClass.ExactTarget
            else if sameFull26Data(actual,exactOutcome.actualBefore,owner) then
              MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspectionClass.ExactBefore
            else MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspectionClass.Neither
          owner.bindClassification(classification)
          exactOutcome.retainUnknownInspectionData(owner,reader,actual,actualWriter,classification)
        }
      }
    catch
      case cause:Throwable=>
        if owner==null then
          try exactOutcome.retainUnknownInspectionRegistrationFailure(cause)
          catch case stateFailure:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,stateFailure)
        else
          owner.recordFailure(cause)
          if owner.classification.isEmpty then owner.closeNewForFailure(cause)
          try exactOutcome.retainUnknownInspectionFailure(owner,owner.failure)
          catch case stateFailure:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,stateFailure)
        if !scala.util.control.NonFatal(cause) then throw cause

  private def validateUnknownInspectionFacts(
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5,
      actual:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow):Unit =
    owner.requireCurrent()
    if actual==null || writer==null || (actual.inputs ne inputs.full26Inputs) ||
        actual.inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
        actual.schema!=inputs.auditExpectation.schema || actual.connection.serverUuid!=owner.newVerified.identity.serverUuid ||
        actual.connection.serverVersion!=owner.newVerified.identity.serverVersion ||
        actual.connection.catalog!=owner.newVerified.identity.catalog ||
        actual.connection.effectiveUser!=owner.newVerified.identity.effectiveUser ||
        actual.observationScope.bootUuid!=owner.newScope.bootUuid ||
        actual.observationScope.bootGeneration!=owner.newScope.bootGeneration ||
        actual.observationScope.connectionGeneration!=owner.inspectionGeneration ||
        actual.maxAllowedPacketBytes<=0L ||
        actual.maxAllowedPacketBytes>inputs.sourceLimitProfile.maxSqlPacketBytes() ||
        writer.bindingUuid!=exactR0Context(owner).bindingUuid ||
        writer.writerEpoch!=exactR0Context(owner).writerEpoch ||
        !writer.ownerBootUuid.contains(owner.oldScope.bootUuid) ||
        writer.leaseUntil!=owner.freshScope.expectedFixedLeaseUntil ||
        writer.databaseNow==null || !writer.databaseNow.isBefore(owner.freshScope.expectedFixedLeaseUntil) ||
        owner.qualification.writerEpochObservedBeforeAcquire!=writer.writerEpoch ||
        owner.qualification.writerOwnerBootObservedBeforeAcquire!=writer.ownerBootUuid then
      throw new SQLDataException("qualified UNKNOWN Full26 image, locked writer, fixed lease or new-session provenance differs from the exact original R0")
    MySqlDeploymentReceiptSession.requireFreshQualification(owner.qualification,inputs.auditExpectation,owner.newVerified,owner.newScope)
    owner.newBarrier.readmit(owner.newVerified,inputs.auditExpectation,owner.newScope)

  private def exactR0Context(owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5)
      :MySqlOperationContext =
    if owner==null || owner.outcome==null || owner.outcome.context==null then
      throw new IllegalStateException("exact original restricted SOURCE R0 context required")
    owner.outcome.context

  /** Compare only frozen Full26 DATA cells; connection/generation/packet metadata is validated separately. */
  private def sameFull26Data(
      actual:MySqlScriptSchemaSnapshot,expected:MySqlScriptSchemaSnapshot,
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):Boolean =
    if actual==null || expected==null || owner==null || actual.schema!=expected.schema ||
        actual.schema!=inputs.full26Inputs.schema || (actual.inputs ne inputs.full26Inputs) ||
        (expected.inputs ne inputs.full26Inputs) || actual.inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
        expected.inputs.dataLayout!=MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
        actual.relations.size!=26 || expected.relations.size!=26 || inputs.full26Inputs.canonicalOrder.size!=26 then false
    else
      var relationIndex=0
      var equal=true
      val shapes=inputs.full26Inputs.canonicalOrder
      while relationIndex<26 && equal do
        val shape=shapes(relationIndex)
        val left=actual.relations(relationIndex)
        val right=expected.relations(relationIndex)
        if left.table!=shape.table || right.table!=shape.table || left.rows.size!=right.rows.size then equal=false
        else
          var rowIndex=0
          while rowIndex<left.rows.size && equal do
            val leftRow=left.rows(rowIndex)
            val rightRow=right.rows(rowIndex)
            if leftRow.size!=shape.columns.size || rightRow.size!=shape.columns.size then equal=false
            else
              var columnIndex=0
              while columnIndex<shape.columns.size && equal do
                if !sameFull26Cell(leftRow(columnIndex),rightRow(columnIndex)) then equal=false
                columnIndex+=1
            rowIndex+=1
        relationIndex+=1
      equal

  private def sameFull26Cell(left:MySqlFormat3Cell,right:MySqlFormat3Cell):Boolean = (left,right) match
    case (MySqlFormat3Cell.SqlNull,MySqlFormat3Cell.SqlNull)=>true
    case (MySqlFormat3Cell.Text(a),MySqlFormat3Cell.Text(b))=>a==b
    case (MySqlFormat3Cell.SignedInteger(a),MySqlFormat3Cell.SignedInteger(b))=>a==b
    case (MySqlFormat3Cell.Floating(a),MySqlFormat3Cell.Floating(b))=>a==b
    case (MySqlFormat3Cell.DateTime(a),MySqlFormat3Cell.DateTime(b))=>a==b
    case (MySqlFormat3Cell.Binary(a),MySqlFormat3Cell.Binary(b))=>
      if a.length!=b.length then false
      else
        var index=0
        var equal=true
        while index<a.length && equal do
          if a.byteAt(index)!=b.byteAt(index) then equal=false
          index+=1
        equal
    case _=>false

  /** JDBC commit 已返回 ACK 后，再由独立登记的读回责任完成一次 Full26 读回。 */
  private def executeAcknowledgedRestrictedSourceR0Readback(
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
      exactScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    requireOwner()
    var owner:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5=null
    var registrationChargeReserved=false
    var failure:Throwable=null
    try
      if restrictedSourceR0PostCommitReadback!=null then
        throw new IllegalStateException("one original Session ACK readback responsibility slot is already occupied",primary)
      root.reserveRetainedMemory(1024L,"restricted SOURCE R0 ACK readback responsibility BEFORE allocation")
      registrationChargeReserved=true
      owner=new MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5(
        this,exactPlan,exactOutcome,exactScope,restrictedSourceReaderLease,root,restrictedSourceLedger,exactOutcome.dataBudget)
      restrictedSourceR0PostCommitReadback=owner
      exactOutcome.beginPostCommitReadback(exactScope,owner)
      owner.markAttemptStarted()
    catch
      case cause:Throwable=>failure=cause
    if owner==null then
      if registrationChargeReserved then
        try root.releaseRetainedMemory(1024L,"failed restricted SOURCE R0 ACK readback responsibility allocation")
        catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
      exactOutcome.markPostCommitReadbackBlocked(Option(failure).getOrElse(
        new IllegalStateException("restricted SOURCE R0 ACK readback responsibility was not registered",primary)))
      return
    if failure!=null then
      exactOutcome.markPostCommitReadbackFailure(owner,null,failure)
      return

    try
      exactPlan.allocation.withAcknowledgedRestrictedSourceR0DataBudget(exactPlan,exactOutcome) { dataBudget=>
        val reader=allocateRestrictedSourceReader(owner.readerLease)
        owner.bindReader(reader)
        reader.registerRestrictedSourceAckReadback(owner)
        reader.captureLockedFull26(None)
        owner.markCaptureReturned()
        reader.settleSuccessfulRead(actor)
        owner.markReadTransactionSettled()
        reader.withRetainedBefore(actor) { (actual,actualWriter)=>
          owner.retainObserved(actual,actualWriter)
          requireAcknowledgedRestrictedSourceR0Readback(exactPlan,exactOutcome,exactPlan.issuer,owner)
          val expected=exactScope.expectedContext
          if actual==null || actualWriter==null || (actual.inputs ne inputs.full26Inputs) ||
              actualWriter.bindingUuid!=expected.bindingUuid || actualWriter.writerEpoch!=expected.writerEpoch ||
              !actualWriter.ownerBootUuid.contains(expected.tag.bootId) ||
              actualWriter.leaseUntil!=expected.fixedLeaseUntil ||
              !actualWriter.leaseUntil.isAfter(actualWriter.databaseNow) then
            throw new SQLDataException("restricted SOURCE R0 ACK readback actual Full26 writer/DBnow differs from its exact retained fence")

          owner.markCompareAttempted()
          var compareChargeReserved=false
          var comparisonFailure:Throwable=null
          var mismatch:Throwable=null
          var exactlyTarget=false
          try
            dataBudget.reserveTemporaryMemory(8192L,
              "restricted SOURCE R0 ACK Full26 exact-target comparison workspace BEFORE compare")
            compareChargeReserved=true
            exactlyTarget=actual.exactlyEquals(exactOutcome.expectedTarget)
            if !exactlyTarget then
              mismatch=new SQLDataException(
                "restricted SOURCE R0 ACK Full26 readback differs field-for-field/canonically from the exact expected target")
          catch
            case cause:Throwable=>comparisonFailure=cause
          finally
            if compareChargeReserved then
              try dataBudget.releaseTemporaryMemory(8192L,
                "restricted SOURCE R0 ACK Full26 exact-target comparison workspace complete")
              catch
                case cleanup:Throwable=>
                  if comparisonFailure==null then
                    if mismatch==null then comparisonFailure=cleanup
                    else MySqlOwnedIoAdapter.suppressExitCleanup(mismatch,cleanup)
                  else MySqlOwnedIoAdapter.suppressExitCleanup(comparisonFailure,cleanup)
          if comparisonFailure!=null then throw comparisonFailure
          if mismatch!=null then exactOutcome.retainUnexpectedPostCommitReadback(owner,reader,actual,actualWriter,mismatch)
          else exactOutcome.retainPostCommitReadbackData(owner,reader,actual,actualWriter)
        }
      }
    catch
      case cause:Throwable=>
        if owner.reader!=null && !owner.captureReturned then
          try owner.cleanupSettled=owner.reader.settleFailedRestrictedSourceAckReadback(
            this,actor,owner.readerLease,root,cause)
          catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(cause,cleanup)
        exactOutcome.markPostCommitReadbackFailure(owner,owner.reader,cause)

  /** ACK 专用精确身份与新鲜度检查；不重开已结算写作用域，也不经旧 parent bridge 刷新。 */
  private def withAcknowledgedRestrictedSourceR0NamespaceWorkspace[A](
      exactScope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5,
      exactDataBudget:MySqlFormat3ReadBudget)(body: => A):A =
    if exactScope==null || exactDataBudget==null || (exactScope.dataBudget ne exactDataBudget) then
      throw new IllegalStateException("exact original restricted SOURCE R0 DATA child for ACK namespace workspace required",primary)
    val fee=exactScope.namespaceTemporaryCharge
    var charged=false
    var primaryFailure:Throwable=null
    try
      exactDataBudget.reserveTemporaryMemory(fee,
        "restricted SOURCE R0 ACK SessionBarrierNamespace canonical/hash/Base64 workspace BEFORE readmit")
      charged=true
      body
    catch
      case cause:Throwable=>primaryFailure=cause;throw cause
    finally
      if charged then
        try exactDataBudget.releaseTemporaryMemory(fee,
          "restricted SOURCE R0 ACK SessionBarrierNamespace canonical/hash/Base64 workspace complete")
        catch
          case cleanup:Throwable=>
            if primaryFailure==null then throw cleanup
            else MySqlOwnedIoAdapter.suppressExitCleanup(primaryFailure,cleanup)

  private[mysql] def requireAcknowledgedRestrictedSourceR0Readback(
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactOutcome:MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome,
      actualRunner:MySqlFencedTransaction,
      readback:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5):Unit =
    requireOwner()
    val exactScope=if exactOutcome==null then null else exactOutcome.freshScope
    val expected=if exactScope==null then null else exactScope.expectedContext
    if exactPlan==null || exactOutcome==null || actualRunner==null || readback==null || expected==null ||
        (restrictedSourceR0ExecutionOutcome ne exactOutcome) || (restrictedSourceR0PostCommitReadback ne readback) ||
        exactOutcome.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitAcknowledged ||
        (exactOutcome.plan ne exactPlan) || !exactOutcome.verifiedPrecommit ||
        exactOutcome.committedCleanupFailure!=null || (exactPlan.issuer ne actualRunner) ||
        (actualRunner ne restrictedSourceRunner) || (exactPlan.exactRoot ne root) ||
        (exactPlan.actualLedger ne restrictedSourceLedger) || (exactPlan.parent.session ne this) ||
        (exactPlan.parent.proof ne restrictedRecallProof) ||
        (exactPlan.parent.publication ne restrictedPublication) ||
        (exactPlan.parent.readerLease ne restrictedSourceReaderLease) ||
        (exactPlan.parent.link ne restrictedSourceLink) ||
        (exactPlan.intent.exactLink ne restrictedSourceLink) ||
        (exactPlan.intent.parentReference ne exactPlan.parent.reference) ||
        (exactScope ne restrictedSourceFreshWriteScope) || (exactScope.plan ne exactPlan) ||
        (exactScope.session ne this) || (exactScope.actor ne actor) || (exactScope.verified ne verified) ||
        (exactScope.readerLease ne restrictedSourceReaderLease) || (exactScope.root ne root) ||
        (exactScope.ledger ne restrictedSourceLedger) ||
        !exactScope.commitAcknowledged || exactScope.commitUnknown!=null || !exactScope.commitAttempted ||
        !exactScope.isSettled || !exactScope.transactionAttempted || !exactScope.gateOwnerReturned ||
        !exactScope.autoCommitRestoreReturned || !exactScope.isolationRestoreReturned ||
        !exactScope.gateLeaveReturned || exactScope.rollbackAttempted ||
        !exactScope.captureWorkspaceRefundReturned || exactScope.targetCaptureWorkspacePrecharged ||
        exactScope.committedCleanupFailure!=null || exactScope.failure.nonEmpty ||
        exactScope.rollbackOnly.cause.nonEmpty || !exactScope.hasVerifiedTarget ||
        exactOutcome.tag!=expected.tag || exactOutcome.tag.operationId!=exactPlan.exactRequest.reserveOperation ||
        exactOutcome.acceptedRepositoryEpochAtSubmission!=exactPlan.intent.expectedSourceObservation.settledRepositoryEpoch ||
        exactOutcome.context==null || exactOutcome.witness==null || exactOutcome.plannedAt==null ||
        exactOutcome.resultAt==null || exactOutcome.expectedTarget==null || exactOutcome.actualBefore==null ||
        exactOutcome.beforeWriter==null || (exactOutcome.context ne exactOutcome.witness.originalContext.context) ||
        (readback.session ne this) || (readback.plan ne exactPlan) || (readback.outcome ne exactOutcome) ||
        (readback.freshScope ne exactScope) || (readback.readerLease ne restrictedSourceReaderLease) ||
        (readback.root ne root) || (readback.ledger ne restrictedSourceLedger) ||
        (readback.dataBudget ne exactOutcome.dataBudget) || !readback.attemptStarted ||
        !readback.isAttemptedState(exactOutcome.postCommitReadback) then
      throw new IllegalStateException("exact settled ACKed restricted SOURCE R0 readback responsibility is required",primary)

    root.requireLedgerIdentity(restrictedSourceLedger)
    root.requireScriptDataInputs(inputs.full26Inputs)
    exactOutcome.dataBudget.requireScriptDataInputs(inputs.full26Inputs)
    restrictedSourceReaderLease.requireBinding(this,actor,restrictedRecallProof,restrictedPublication,
      restrictedSourceLink,root,restrictedSourceLedger)
    val readbackReader=readback.reader
    if readbackReader!=null then
      requireRestrictedSourceReader(restrictedSourceReaderLease,readbackReader)
      readbackReader.requireRestrictedSourceRetirementIdentity(this,actor,root)

    val connectionStateCurrent=if readbackReader==null then
      verified.connection.getAutoCommit==true && verified.connection.getTransactionIsolation==exactScope.originalIsolation
    else readbackReader.hasCurrentRestrictedSourceAckReadbackConnectionState(readback,exactScope.originalIsolation)
    if !connectionStateCurrent then
      throw new SQLDataException("restricted SOURCE R0 ACK readback has neither restored idle connection nor its exact active RR read gate")
    if barrier==null then throw new SQLDataException("restricted SOURCE R0 ACK readback lost the verified Session barrier")
    withAcknowledgedRestrictedSourceR0NamespaceWorkspace(exactScope,exactOutcome.dataBudget) {
      barrier.readmit(verified,inputs.auditExpectation,scope)
      if barrier.connectionGenerationId!=expected.barrierGenerationUuid ||
          barrier.actualConnectionId!=expected.actualConnectionId || barrier.namespace==null ||
          barrier.namespace.lockName()!=expected.barrierNamespace then
        throw new SQLDataException("restricted SOURCE R0 ACK readback barrier differs from its original context")
    }

    restrictedRecallProof.withCurrent(inputs.auditExpectation,coordinator) { (actualVerified,actualScope,token,receipt) =>
      val now=System.nanoTime()
      val ioIdentity=identity
      val qualification=if token==null then null else token.qualification
      if (actualVerified ne verified) || (actualScope ne scope) || token==null || receipt==null ||
          token.fence==null || qualification==null || (token.verified ne verified) || (token.scope ne scope) ||
          token.bootUuid!=ioIdentity.bootId || token.bootGeneration!=ioIdentity.bootGeneration ||
          token.connectionGeneration!=ioIdentity.connectionGeneration || expected.tag.bootId!=ioIdentity.bootId ||
          expected.tag.bootGeneration!=ioIdentity.bootGeneration ||
          expected.tag.connectionGeneration!=ioIdentity.connectionGeneration ||
          expected.tag.operationId!=exactPlan.exactRequest.reserveOperation ||
          expected.bindingUuid!=exactPlan.exactRequest.binding || expected.writerEpoch<=0L ||
          expected.fixedLeaseUntil!=token.leaseUntil || token.bindingUuid!=expected.bindingUuid ||
          token.writerEpoch!=expected.writerEpoch || token.fence.ownerBootUuid!=ioIdentity.bootId ||
          token.leaseUntil!=token.fence.leaseUntil || !token.fence.isCurrentAt(now) ||
          !qualification.isCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
            inputs.auditExpectation.serverUuid,inputs.auditExpectation.schema,inputs.auditExpectation.bindingUuid,
            inputs.auditExpectation.profile,inputs.auditExpectation.expectedPhysicalFormat,
            verified,Thread.currentThread(),now) then
        throw new SQLDataException("restricted SOURCE R0 ACK readback original qualification/fence is stale or mismatched")
      receipt.assertCurrent(ioIdentity.bootId,ioIdentity.bootGeneration,ioIdentity.connectionGeneration,
        inputs.auditExpectation.serverUuid,inputs.auditExpectation.schema,inputs.auditExpectation.bindingUuid,
        inputs.auditExpectation.profile,inputs.auditExpectation.expectedPhysicalFormat,
        verified,Some(token.fence),now)
    }

  /** Adapter settlement polling reads only this exact worker-owned result; it never creates a second execution. */
  private[mysql] def retainedRestrictedSourceR0ExecutionOutcome(
      exactPlan:MySqlFencedTransaction.SourceReservePlan5,
      exactTag:IoOperationTag):MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionOutcome =
    requireOwner()
    val result=restrictedSourceR0ExecutionOutcome
    if result==null || (result.plan ne exactPlan) || result.tag!=exactTag then
      throw new IllegalStateException("exact retained restricted SOURCE R0 outcome owner is unavailable",primary)
    result

  /** 精确退役原restricted reader与plan DATA；不关闭provider generation barrier。 */
  private[mysql] def dropRestrictedSourceReservePlan(
      exactReceipt:MySqlFencedTransaction.SourceReservePlanDropReceipt):MySqlFencedTransaction.SourceReservePlanDropAck =
    requireOwner()
    if exactReceipt==null then throw new IllegalArgumentException("exact restricted source plan drop receipt required")
    requireRestrictedSourceRunner(restrictedSourceRunner,restrictedRecallProof,restrictedPublication,
      root,restrictedSourceLedger,restrictedSourceReaderLease)
    restrictedSourceRunner.dropRestrictedSourceReservePlan(exactReceipt)

  private[mysql] def requireReadOwner(actual: MySqlDeploymentReceiptReadOwner,
      actualActor: MySqlDeploymentReceiptBootstrapActor): Unit =
    if actual!=null && actual.unknownInspectionOwner!=null then
      requireRestrictedSourceUnknownReader(actual,actualActor,actual.unknownInspectionOwner)
    else
      requireActor(actualActor)
      if actual == null || !readOwners.exists(_ eq actual) then throw new IllegalStateException("exact registered Full26 read owner required")

  private[mysql] def requireRestrictedSourceUnknownInspection(
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):Unit =
    requireOwner()
    val result=restrictedSourceR0ExecutionOutcome
    if owner==null || (restrictedSourceR0UnknownInspection ne owner) ||
        (owner.session ne this) || (owner.worker ne thread) || (Thread.currentThread() ne thread) ||
        (owner.outcome ne result) || result==null || (result.plan ne owner.plan) ||
        result.state!=MySqlDeploymentReceiptSession.RestrictedSourceR0ExecutionState.CommitUnknown ||
        (result.unknownInspectionOwner ne owner) ||
        !result.unknownInspection.exists {
          case MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspectionState.Pending(actual) => actual eq owner
          case _ => false
        } || (owner.freshScope ne restrictedSourceFreshWriteScope) ||
        (owner.freshScope ne result.freshScope) || (owner.oldVerified ne verified) || (owner.oldScope ne scope) ||
        (owner.oldBarrier ne barrier) || owner.root.isClosed || (owner.root ne root) ||
        (owner.ledger ne restrictedSourceLedger) || (owner.dataBudget ne result.dataBudget) ||
        owner.dataBudget.isClosed || !owner.dataBudget.isCanonicalChildOf(root) ||
        !owner.dataBudget.sharesAllocationLedger(root) || (owner.readerLease ne restrictedSourceReaderLease) ||
        !owner.freshScope.commitAttempted || owner.freshScope.commitAcknowledged ||
        (owner.freshScope.commitUnknown ne result.commitUnknown) || result.commitUnknown==null ||
        !result.verifiedPrecommit || result.context==null || result.witness==null || result.actualBefore==null ||
        result.expectedTarget==null || result.beforeWriter==null || result.plannedAt==null || result.resultAt==null ||
        owner.auditAccount==null || owner.auditAccount.isClosed ||
        !owner.auditAccount.isCanonicalComparisonChildOf(root) || !owner.auditAccount.sharesAllocationLedger(root) ||
        (owner.auditAccount.profile ne root.profile) || quiescing then
      throw new IllegalStateException("exact registered original-worker restricted SOURCE R0 UNKNOWN inspection owner required",
        Option(owner).flatMap(value=>Option(value.failure)).orElse(Option(primary)).orNull)
    root.requireLedgerIdentity(owner.ledger)
    root.requireScriptDataInputs(inputs.full26Inputs)
    owner.readerLease.requireBinding(this,actor,restrictedRecallProof,restrictedPublication,
      restrictedSourceLink,root,restrictedSourceLedger)

  /** Exact read-only audit issuer binding for the newly opened UNKNOWN inspection connection. */
  private[mysql] def requireRestrictedSourceUnknownAuditBinding(
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5,
      expectation:MySqlSchemaAuditExpectation,
      actualVerified:VerifiedMySqlConnection,
      actualScope:MySqlAuditBootScope,
      actualRoot:MySqlFormat3ReadBudget,
      actualAuditAccount:MySqlFormat3ReadBudget):Unit =
    requireRestrictedSourceUnknownInspection(owner)
    if expectation==null || (expectation ne inputs.auditExpectation) ||
        actualVerified==null || (actualVerified ne owner.newVerified) ||
        actualScope==null || (actualScope ne owner.newScope) ||
        (actualScope.verifiedConnection.getOrElse(null) ne actualVerified) ||
        (actualScope.ownerThread ne thread) || actualScope.bootUuid!=owner.oldScope.bootUuid ||
        actualScope.bootGeneration!=owner.oldScope.bootGeneration ||
        actualScope.connectionGeneration!=owner.inspectionGeneration ||
        actualScope.connectionGeneration<=owner.oldScope.connectionGeneration ||
        (actualRoot ne root) || (actualRoot ne owner.root) ||
        (actualAuditAccount ne owner.auditAccount) || actualAuditAccount==null || actualAuditAccount.isClosed ||
        !actualAuditAccount.isCanonicalComparisonChildOf(actualRoot) ||
        !actualAuditAccount.sharesAllocationLedger(actualRoot) ||
        (actualAuditAccount.profile ne actualRoot.profile) || owner.newBarrier==null ||
        owner.newBarrier.actualConnectionId<=0L || owner.newBarrier.connectionGenerationId==null ||
        owner.newBarrier.namespace==null ||
        owner.newBarrier.namespace.lockName()!=owner.oldBarrier.namespace.lockName() then
      throw new IllegalStateException("exact fresh same-root schema5 UNKNOWN inspection audit binding required",
        Option(owner.failure).orElse(Option(primary)).orNull)
    actualRoot.requireLedgerIdentity(owner.ledger)
    actualRoot.requireScriptDataInputs(inputs.full26Inputs)

  private[mysql] def requireRestrictedSourceUnknownReader(
      actual:MySqlDeploymentReceiptReadOwner,
      actualActor:MySqlDeploymentReceiptBootstrapActor,
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):Unit =
    requireRestrictedSourceUnknownInspection(owner)
    if actualActor==null || (actualActor ne actor) || actual==null ||
        !readOwners.exists(_ eq actual) || (owner.reader ne actual) ||
        (actual.unknownInspectionOwner ne owner) then
      throw new IllegalStateException("exact registered UNKNOWN-role Full26 reader required",Option(owner.failure).orNull)

  private[mysql] def allocateRestrictedSourceUnknownInspectionReader(
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):MySqlDeploymentReceiptReadOwner =
    requireRestrictedSourceUnknownInspection(owner)
    if owner.reader!=null || owner.newVerified==null || owner.newScope==null || owner.qualification==null ||
        owner.newBarrier==null then
      throw new IllegalStateException("qualified independent UNKNOWN reader registration is incomplete",Option(owner.failure).orNull)
    val value=MySqlDeploymentReceiptReadOwner.allocateForRestrictedSourceUnknown(
      this,actor,owner.newVerified,owner.newScope,inputs.auditExpectation,inputs.full26Inputs,root,owner)
    owner.bindReader(value)
    readOwners=value::readOwners
    value

  private[mysql] def hasPendingRestrictedSourceFreshWritePreparation(
      reader:MySqlDeploymentReceiptReadOwner):Boolean =
    reader!=null && ((restrictedSourceFreshWritePendingReader eq reader) && restrictedSourceFreshWritePendingScopeHolder ||
      (restrictedSourceFreshWriteScope!=null &&
        (restrictedSourceFreshWriteScope.isBoundReader(reader) || restrictedSourceFreshWriteScope.isBoundTargetReader(reader)) &&
        !restrictedSourceFreshWriteScope.readerRetirementComplete))

  private def allocateReader():MySqlDeploymentReceiptReadOwner =
    val value=MySqlDeploymentReceiptReadOwner.allocate(this,actor,verified,scope,inputs.auditExpectation,inputs.full26Inputs,root)
    readOwners=value::readOwners
    value

  private[mysql] def withFreshCapture[A](actualActor: MySqlDeploymentReceiptBootstrapActor)
      (consume: MySqlDeploymentReceiptSession.FreshCapture => A): A =
    requireActor(actualActor)
    if mode != MySqlDeploymentReceiptSessionInputs.Fresh || readOwner == null || nativeOwner == null then
      throw new IllegalStateException("original schema5 Fresh native/read holders required")
    consume(freshCapture)

  private def start(expected: MySqlDeploymentReceiptSessionInputs.ModeRequest): Unit =
    requireOwner()
    if started || mode != expected then throw new IllegalStateException("schema5 Session initialization is one-shot and mode-specific", primary)
    started = true

  /** 必须先挂入原 adapter 的单一 session 槽；此方法仅由同一 worker 调用。 */
  def initializeFresh(): Unit =
    start(MySqlDeploymentReceiptSessionInputs.Fresh)
    try
      val request = fresh.getOrElse(throw new IllegalStateException("original schema5 Fresh request missing"))
      inputs.requirePlanData(request.plan)
      inputs.requireCompleteConfigurationPin(request.controlled)
      // 同 root 的 schema5 layout/inputs 必须先成立；不在 native 后才发现 assembly/layout 差异。
      root.requireScriptDataInputs(inputs.full26Inputs)
      val expectation = inputs.auditExpectation
      verified = openOriginalConnection()
      scope = MySqlAuditBootScope.forOwnedIo(verified, thread, identity.bootId,
        identity.bootGeneration, identity.connectionGeneration)
      auditor = new MySqlSchemaAuditor(expectation, auditMaxAge)
      val qualification = auditor.auditBeforeWriterAcquire(auditor.readCurrentObjectManifest(verified, scope), scope)
      bootstrapQualification=qualification
      MySqlDeploymentReceiptSession.requireFreshQualification(qualification, expectation, verified, scope)
      barrier = verified.acquireSessionBarrier(expectation, scope, Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value) => value
        case MySqlSessionBarrierAcquisition.Deferred =>
          throw new SQLTransientConnectionException("schema5 original session barrier busy; native not attempted")
      // begin 返回即先登记；createFresh 的任何 native attempt 都已有实际 holder。
      nativeOwner = inputs.beginFreshNative(request.worldPath, request.plan, request.boot, request.controlled)
      nativeOwner.createFresh()
      actor.continueFreshPrepare()
    catch
      case failure: Throwable =>
        primary = failure
        // 未完成 B/C 时保留实际连接、锁、native、图与计费；不能 ordinary-close custody。
        throw failure

  def initializeOrdinary(): Unit =
    start(MySqlDeploymentReceiptSessionInputs.Ordinary)
    try
      val pins=original.getOrElse(throw new IllegalStateException("original schema5 source pins missing"))
      val controlled=ordinaryControlled.getOrElse(throw new IllegalStateException("original Ordinary controlled metadata missing"))
      inputs.requirePlanData(pins.plan());inputs.requireCompleteConfigurationPin(controlled)
      verified=openOriginalConnection()
      scope=MySqlAuditBootScope.forOwnedIo(verified,thread,identity.bootId,identity.bootGeneration,identity.connectionGeneration)
      auditor=new MySqlSchemaAuditor(inputs.auditExpectation,auditMaxAge)
      val qualification=auditor.auditBeforeWriterAcquire(auditor.readCurrentObjectManifest(verified,scope),scope)
      bootstrapQualification=qualification
      barrier=verified.acquireSessionBarrier(inputs.auditExpectation,scope,Duration.ZERO) match
        case MySqlSessionBarrierAcquisition.Held(value)=>value
        case MySqlSessionBarrierAcquisition.Deferred=>throw new SQLTransientConnectionException("schema5 Ordinary original session barrier busy")
      nativeOwner=inputs.beginOrdinaryNative(pins,new WindowsWorldProvisioner.BootIdentity(identity.bootId,identity.bootGeneration),controlled)
      nativeOwner.inspectOrdinary()
      actor.continueOrdinary(qualification)
    catch
      case failure:Throwable=>
        primary=failure
        throw failure

  private[mysql] def withFreshPlanning[A](actual:MySqlDeploymentReceiptBootstrapActor)
      (consume:(MySqlDeploymentReceiptSession.FreshRequest,MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow,
        MySqlScriptSchemaInputs,MySqlFormat3ReadBudget,MySqlAuditBootScope,HeldSessionBarrier,MySqlDeploymentReceiptFreshNativeOwner)=>A):A =
    requireActor(actual)
    if readOwner!=null then throw new IllegalStateException("original Fresh planning read is one-shot")
    readOwner=allocateReader();readOwner.captureLockedFull26()
    readOwner.withCurrentConfiguration(actual,inputs,fresh.get.plan)(_=>())
    readOwner.withLockedImage(actual,inputs) { (before,writer)=>
      consume(fresh.get,before,writer,inputs.full26Inputs,root,scope,barrier,nativeOwner)
    }
  private[mysql] def withOrdinaryPlanning[A](actual:MySqlDeploymentReceiptBootstrapActor)
      (consume:(ProductionBootstrapControl.OriginalSourcePins,ProductionBootstrapControl.OrdinaryIdentity,MySqlScriptSchemaSnapshot,
        MySqlBusinessWriterRow,MySqlScriptSchemaInputs,MySqlFormat3ReadBudget,MySqlAuditBootScope,HeldSessionBarrier,MySqlDeploymentReceiptFreshNativeOwner)=>A):A =
    requireActor(actual)
    if readOwner!=null then throw new IllegalStateException("original Ordinary planning read is one-shot")
    readOwner=allocateReader();readOwner.captureLockedFull26()
    readOwner.withCurrentConfiguration(actual,inputs,original.get.plan())(_=>())
    readOwner.withLockedImage(actual,inputs) { (before,writer)=>
      consume(original.get,ordinaryIdentity.get,before,writer,inputs.full26Inputs,root,scope,barrier,nativeOwner)
    }
  private[mysql] def withOrdinaryNativePreflight[A](actual:MySqlDeploymentReceiptBootstrapActor)
      (consume:(MySqlScriptSchemaInputs,MySqlFormat3ReadBudget,MySqlDeploymentReceiptFreshNativeOwner)=>A):A =
    requireActor(actual)
    if mode!=MySqlDeploymentReceiptSessionInputs.Ordinary || readOwner!=null || critical!=null || !verified.connection.getAutoCommit then
      throw new IllegalStateException("Ordinary native inventory/body preflight requires original idle Session before SQL read")
    consume(inputs.full26Inputs,root,nativeOwner)
  private[mysql] def withFreshNativePreflight[A](actual:MySqlDeploymentReceiptBootstrapActor)
      (consume:(MySqlScriptSchemaInputs,MySqlFormat3ReadBudget,MySqlDeploymentReceiptFreshNativeOwner)=>A):A =
    requireActor(actual)
    if mode!=MySqlDeploymentReceiptSessionInputs.Fresh || readOwner!=null || critical!=null || !verified.connection.getAutoCommit then
      throw new IllegalStateException("Fresh native source preflight requires original idle Session before SQL read")
    consume(inputs.full26Inputs,root,nativeOwner)
  private[mysql] def settlePlanningRead(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
    requireActor(actual);readOwner.settleSuccessfulRead(actual)
  private[mysql] def requireSuccessorInstallBefore(actual:MySqlDeploymentReceiptBootstrapActor,
      before:MySqlScriptSchemaSnapshot,held:HeldSessionBarrier):Unit =
    requireActor(actual)
    if mode!=MySqlDeploymentReceiptSessionInputs.Ordinary || readOwner==null ||
        !readOwner.ownsRetainedImage(before) || (held ne barrier) || critical!=null || !verified.connection.getAutoCommit then
      throw new IllegalStateException("original settled cold before image/barrier and idle Session required for successor installation")
    barrier.readmit(verified,inputs.auditExpectation,scope)
  private[mysql] def requirePreGenesisQualification(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
    requireActor(actual)
    if mode!=MySqlDeploymentReceiptSessionInputs.Ordinary || readOwner!=null then
      throw new IllegalStateException("original idle pre-genesis Ordinary Session required")
    MySqlDeploymentReceiptSession.requireFreshQualification(bootstrapQualification,inputs.auditExpectation,verified,scope)
  private[mysql] def withCommittedImage[A](actual:MySqlDeploymentReceiptBootstrapActor,image:MySqlScriptSchemaSnapshot)
      (consume:(MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow)=>A):A =
    requireActor(actual)
    var found=Option.empty[A]
    readOwners.iterator.takeWhile(_=>found.isEmpty).foreach { owner=>
      if owner.ownsRetainedImage(image) then found=Some(owner.withRetainedBefore(actual)(consume))
    }
    found.getOrElse(throw new IllegalArgumentException("exact Session-owned settled readback image required"))
  private[mysql] def actualLeaseDuration:Duration=leaseDuration
  private[mysql] def actualSubmissionTag:IoOperationTag=submissionTag
  private[mysql] def withNative[A](actual:MySqlDeploymentReceiptBootstrapActor)(consume:MySqlDeploymentReceiptFreshNativeOwner=>A):A =
    requireActor(actual);consume(nativeOwner)

  private[mysql] def auditOrdinaryCompletion(actual:MySqlDeploymentReceiptBootstrapActor,
      qualification:VerifiedSchemaQualification,writer:MySqlBusinessWriterRow):MySqlDeploymentReceiptSession.OrdinaryPostAudit5 =
    requireActor(actual)
    if qualification==null || (qualification ne bootstrapQualification) || ordinaryPostAudit!=null ||
        !writer.ownerBootUuid.contains(scope.bootUuid) || !writer.leaseUntil.isAfter(writer.databaseNow) then
      throw new IllegalStateException("exact original Ordinary committed current writer required")
    val fence=MySqlWriterFenceExpectation.fromAcquiredFence(writer.bindingUuid,scope.bootUuid,writer.writerEpoch,
      writer.leaseUntil,writer.databaseNow,System.nanoTime())
    val receipt=auditor.auditAfterWriterAcquire(auditor.readCurrentObjectManifest(verified,scope),qualification,scope,fence)
    val token=new MySqlWriterFenceToken(verified,scope,qualification,fence)
    ordinaryPostAudit=new MySqlDeploymentReceiptSession.OrdinaryPostAudit5(this,token,receipt)
    ordinaryPostAudit

  private[mysql] def auditFreshPreparation(actual:MySqlDeploymentReceiptBootstrapActor,
      writer:MySqlBusinessWriterRow):MySqlDeploymentReceiptSession.OrdinaryPostAudit5 =
    requireActor(actual)
    if mode!=MySqlDeploymentReceiptSessionInputs.Fresh then throw new IllegalStateException("original Fresh preparation post-audit required")
    auditOrdinaryCompletion(actual,bootstrapQualification,writer)

  private[mysql] def adoptOrdinaryCompletion(actual:MySqlDeploymentReceiptBootstrapActor,
      completion:MySqlDeploymentReceiptBootstrapActor.Completion5,
      postAudit:MySqlDeploymentReceiptSession.OrdinaryPostAudit5):MySqlBusinessAdmission =
    requireActor(actual);completion.requireBootstrapAdoption(actual)
    if postAudit==null || (postAudit ne ordinaryPostAudit) || ordinaryAdoption!=null then
      throw new IllegalStateException("original post-audit and one-shot settled native completion required")
    nativeOwner.withSource(_.validateLive())
    coordinator=new MySqlWriterFenceCoordinator(inputs.auditExpectation,auditor,scope,leaseDuration)
    ordinaryAdoption=new MySqlDeploymentReceiptSession.OrdinaryAdoption5(this,actual,completion,postAudit,coordinator)
    admission=coordinator.admitDeploymentReceiptHandoff(ordinaryAdoption)
    admission

  /** 所有固定SQL仅在此原worker边界执行；计划/ACK不能由DATA构造替代。 */
  private[mysql] def executePlan(actual:MySqlDeploymentReceiptBootstrapActor,plan:MySqlDeploymentReceiptBootstrapActor.Planned5,
      durable:com.wi.floatingpod.server.WindowsB1V6WitnessRecordStore.DurableV6Ack):MySqlScriptSchemaSnapshot =
    requireActor(actual);actual.requirePlan(plan)
    if critical!=null || !verified.connection.getAutoCommit then throw new IllegalStateException("schema5 critical write requires idle exact Session")
    actual.rebarrierBeforeWrite(plan,durable)
    val c=verified.connection
    val rollbackOnly=new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(c,rollbackOnly).foreach(throw _)
    val gate=MySqlConnectionTransactionGate.currentOwner(c,rollbackOnly)
    critical=new MySqlDeploymentReceiptSession.CriticalWriteScope(this,actual,verified,rollbackOnly,gate)
    val transaction=critical
    val isolation=c.getTransactionIsolation
    var begun=false
    var attemptedCommit=false
    var acknowledgedCommit=false
    var failure:Throwable=null
    root.reserveTemporaryMemory(65536L,"schema5 critical fixed SQL/control workspace")
    try
      begun=true;c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);c.setAutoCommit(false)
      if c.getAutoCommit || c.getTransactionIsolation!=Connection.TRANSACTION_REPEATABLE_READ then throw new SQLDataException("schema5 critical RR establishment failed")
      barrier.readmit(verified,inputs.auditExpectation,scope)
      val locked=allocateReader();locked.captureLockedFull26(Some(transaction))
      locked.withLockedImage(actual,inputs) { (before,writer)=>
        if !MySqlDeploymentReceiptFull26RestoreOwner.sameData(before,plan.before) then throw new SQLDataException("schema5 locked complete before changed after durable ACK")
        if !writer.leaseUntil.isAfter(writer.databaseNow) && plan.phase!=com.wi.floatingpod.server.B1ResumeRecord.Phase.PREPARE &&
            plan.phase!=com.wi.floatingpod.server.B1ResumeRecord.Phase.HANDOFF then throw new SQLDataException("schema5 current owned writer lease expired")
        actual.requireDurable(plan,durable,barrier)
        writePlan(plan)
      }
      val after=allocateReader();after.captureLockedFull26(Some(transaction))
      after.withLockedImage(actual,inputs) { (image,writer)=>
        if !MySqlDeploymentReceiptFull26RestoreOwner.sameData(image,plan.target) || !writer.leaseUntil.isAfter(writer.databaseNow) then
          throw new SQLDataException("schema5 exact Full26 target/current lease differs before commit")
      }
      if rollbackOnly.cause.nonEmpty then throw rollbackOnly.cause.get
      attemptedCommit=true;c.commit();acknowledgedCommit=true
      actual.commitAcknowledged(plan)
      c.setAutoCommit(true);c.setTransactionIsolation(isolation)
      if !c.getAutoCommit||c.getTransactionIsolation!=isolation then throw new IllegalStateException("schema5 committed connection restoration failed")
      MySqlConnectionTransactionGate.leave(c,rollbackOnly);transaction.settled=true;critical=null
      val readback=allocateReader();readback.captureLockedFull26()
      var result:MySqlScriptSchemaSnapshot=null
      readback.withLockedImage(actual,inputs) { (image,writer)=>
        if !MySqlDeploymentReceiptFull26RestoreOwner.sameData(image,plan.target) || !writer.leaseUntil.isAfter(writer.databaseNow) then
          throw new SQLDataException("schema5 committed complete Full26 readback differs")
        result=image
      }
      readback.settleSuccessfulRead(actual)
      actual.readbackConfirmed(plan)
      result
    catch
      case cause:Throwable=>
        failure=cause
        if attemptedCommit && !acknowledgedCommit then failure=MySqlCommitUnknownException(plan.context,cause)
        else if acknowledgedCommit then failure=new MySqlBootstrapCommittedUnverifiedException("schema5 critical write/readback",Some(plan.context),cause)
        else if begun then
          try
            c.rollback();c.setAutoCommit(true);c.setTransactionIsolation(isolation)
            if !c.getAutoCommit||c.getTransactionIsolation!=isolation then throw new IllegalStateException("schema5 rollback restoration differs")
            MySqlConnectionTransactionGate.leave(c,rollbackOnly);transaction.settled=true;critical=null
          catch
            case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
        primary=failure
        throw failure
    finally
      if transaction.isSettled then root.releaseTemporaryMemory(65536L,"schema5 critical fixed SQL/control workspace complete")

  private def writePlan(plan:MySqlDeploymentReceiptBootstrapActor.Planned5):Unit =
    val phase=plan.phase
    val changed=if phase==com.wi.floatingpod.server.B1ResumeRecord.Phase.HANDOFF then Vector("fp_runtime_writer")
      else if phase==com.wi.floatingpod.server.B1ResumeRecord.Phase.PREPARE then Vector("physical_recovery_binding","binding_bootstrap_intent","fp_script_catalog","fp_runtime_writer")
      else Vector("physical_recovery_binding","binding_bootstrap_intent","fp_runtime_writer")
    plan.before.relations.foreach { relation=>
      if !changed.contains(relation.table)&&relation!=plan.target.relation(relation.table) then throw new SQLDataException("schema5 fixed phase changed an unrelated complete relation")
    }
    changed.foreach { table=>
      val columns=inputs.full26Inputs.forTable(table).columns
      val before=plan.before.relation(table).rows
      val target=plan.target.relation(table).rows
      if target.size!=1 || before.size>1 then throw new SQLDataException("schema5 fixed B1 target requires singleton rows")
      val quoted=columns.map(c=>"`"+c.name+"`")
      val sql=if before.isEmpty then s"INSERT INTO `${inputs.full26Inputs.schema}`.`$table` (${quoted.mkString(",")}) VALUES (${columns.map(_=>"?").mkString(",")})"
        else s"UPDATE `${inputs.full26Inputs.schema}`.`$table` SET ${quoted.map(_+"=?").mkString(",")} WHERE ${quoted.map(_+" <=> ?").mkString(" AND ")}"
      val values=target.head++before.headOption.getOrElse(Vector.empty)
      val payload=values.foldLeft(0L)((n,cell)=>Math.addExact(n,cell match
        case MySqlFormat3Cell.Binary(value)=>value.length.toLong+16L
        case MySqlFormat3Cell.Text(value)=>Math.multiplyExact(value.length.toLong,4L)+16L
        case _=>64L))
      val packet=Math.addExact(MySqlFormat3FullSnapshotSql.utf8Length(sql),payload)
      if packet>inputs.full26Inputs.effectivePacketLimit(plan.target.maxAllowedPacketBytes) then throw new SQLDataException("schema5 fixed phase DML exceeds actual/profile packet budget")
      root.reserveTemporaryMemory(Math.addExact(4096L,payload),"schema5 fixed DML parameter copies BEFORE binding")
      var primary:Throwable=null
      try
        MySqlBusinessRows.using(verified.connection.prepareStatement(sql)) { statement=>
          values.zipWithIndex.foreach((cell,index)=>bindCell(statement,index+1,cell))
          if statement.executeUpdate()!=1 then throw new SQLDataException("schema5 exact fixed phase DML row count differs")
        }
      catch
        case cause:Throwable=>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(()=>root.releaseTemporaryMemory(Math.addExact(4096L,payload),"schema5 fixed DML parameter copies detached")))
    }
  private def bindCell(statement:PreparedStatement,index:Int,cell:MySqlFormat3Cell):Unit=cell match
    case MySqlFormat3Cell.SqlNull=>statement.setNull(index,Types.NULL)
    case MySqlFormat3Cell.Text(value)=>statement.setString(index,value)
    case MySqlFormat3Cell.Binary(value)=>statement.setBytes(index,Array.tabulate(value.length)(value.byteAt))
    case MySqlFormat3Cell.SignedInteger(value)=>statement.setLong(index,value)
    case MySqlFormat3Cell.Floating(value)=>statement.setDouble(index,java.lang.Double.longBitsToDouble(value))
    case MySqlFormat3Cell.DateTime(value)=>statement.setObject(index,value)

  def beginQuiesce(): Unit =
    requireOwner()
    quiescing = true

  def issueBootResult(tag:IoOperationTag,repositoryEpochAtSubmission:Long):MySqlOwnedIoReply.DeploymentReceiptBooted =
    requireOwner();identity.requireTag(tag)
    if tag!=submissionTag || repositoryEpochAtSubmission<0L || publishedReply!=null || publication!=null then
      throw new IllegalStateException("original one-shot schema5 Boot tag/epoch required")
    val completion=actor.completionFor(this)
    publicationLease=inputs.borrowForPublication() // 实际 lease 先挂入，后续任意分配失败仍留在 Session。
    root.reserveRetainedMemory(512L,"schema5 publication proof/backing/reply BEFORE allocation")
    publication=new MySqlDeploymentReceiptSession.Publication5(this,completion,tag,repositoryEpochAtSubmission,publicationLease)
    publication.initializeImmutableInput()
    publishedReply=MySqlOwnedIoReply.DeploymentReceiptBooted(MySqlProductionBootReply.issueDeploymentReceipt(publication))
    publishedReply

  @volatile private var sourceMain:Thread=null
  @volatile private var sourceStopped=false
  @volatile private var sourceBridge:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root=null
  private[mysql] def suspendRecallSourceMain():Unit =
    val bridge=sourceBridge
    if bridge!=null then bridge.synchronized {
      if sourceStopped || (Thread.currentThread() ne sourceMain) then throw new IllegalStateException("original source Main transfer required")
      sourceMain=null
    }
  private[mysql] def claimRecallSourceMain():Unit =
    val bridge=sourceBridge
    if bridge!=null then bridge.synchronized {
      if sourceStopped || sourceMain!=null then throw new IllegalStateException("original suspended source Main required")
      sourceMain=Thread.currentThread()
    }
  private[mysql] def abortRecallSourceMain():Unit =
    val bridge=sourceBridge
    if bridge!=null then bridge.synchronized {sourceStopped=true;sourceMain=null}
  private[mysql] def recallSourceAllocationForMain(actualMain:Thread):com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root =
    currentRestrictedRecallFreshness()
    if actualMain==null || (actualMain eq thread) || sourceBridge!=null then throw new IllegalArgumentException("original distinct one-shot Recall Main required")
    root.reserveRetainedMemory(256L,"original Recall source allocation bridge BEFORE allocation")
    sourceMain=actualMain
    sourceBridge=new com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root:
      def exactProfile():com.wi.floatingpod.config.SourceLimitProfile=inputs.sourceLimitProfile
      protected def requireLive():Unit =
        if sourceStopped || sourceMain==null || primary!=null || quiescing then throw new IllegalStateException("original active Recall source owner required",primary)
      protected def requireMain():Unit =
        requireLive()
        if Thread.currentThread() ne sourceMain then throw new IllegalStateException("actual claimed Recall Main required")
      protected def reserveRoot(bytes:Long,context:String):Unit = sourceBridge.synchronized {
        requireLive()
        try root.reserveTemporaryMemory(bytes,context)
        catch case cause:java.sql.SQLException=>throw new java.io.IOException("Original Recall source reservation failed",cause)
      }
      protected def releaseRoot(bytes:Long,context:String):Unit = sourceBridge.synchronized {
        requireLive()
        try root.releaseTemporaryMemory(bytes,context)
        catch case cause:java.sql.SQLException=>throw new java.io.IOException("Original Recall source release failed",cause)
      }
    sourceBridge

  private[mysql] def restrictedReferences:Vector[MySqlNormalRecallRecoveryReference] =
    currentRestrictedRecallFreshness()
    if restrictedReferencesIssued!=null then throw new IllegalStateException("original restricted source references are one-shot")
    val fee=Math.addExact(128L,Math.multiplyExact(restrictedDecisions.size.toLong,256L))
    root.reserveRetainedMemory(fee,"schema5 original restricted Main references BEFORE allocation")
    try
      restrictedReferencesIssued=restrictedDecisions.map { decision=>
        val i=decision.inventory
        MySqlNormalRecallRecoveryReference(i.workflow,i.pod,i.prepare,i.checkpoint,decision.observation)
      }
    catch
      case failure:Throwable=>
        try root.releaseRetainedMemory(fee,"failed original restricted Main reference allocation")
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(failure,cleanup)
        throw failure
    restrictedReferencesIssued
  private[mysql] def issueRestrictedBoot(tag:IoOperationTag,epoch:Long,
      link:MySqlOwnedIoAdapter.RecallRecoveryLink,actualLedger:MySqlFormat3AllocationLedger):MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =
    currentRestrictedRecallFreshness();identity.requireTag(tag)
    if tag!=submissionTag || epoch<0L || restrictedPublication!=null || publication!=null || publishedReply!=null ||
        link==null || (link.identity ne identity) || actualLedger==null || restrictedReferencesIssued==null ||
        (link.references ne restrictedReferencesIssued) then throw new IllegalStateException("original one-shot restricted schema5 Boot required")
    root.requireLedgerIdentity(actualLedger)
    val sourceLease=inputs.borrowForPublication()
    try root.reserveRetainedMemory(512L,"schema5 restricted publication/backing/reply BEFORE allocation")
    catch
      case failure:Throwable=>
        try sourceLease.close()
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(failure,cleanup)
        throw failure
    publicationLease=sourceLease
    restrictedPublication=new MySqlDeploymentReceiptSession.RestrictedPublication5(this,restrictedRecallProof,link,tag,epoch)
    restrictedPublication.initialize()
    root.reserveRetainedMemory(512L,"schema5 restricted source Fenced runner/read lease BEFORE allocation")
    try
      val anchor=restrictedRecall.getOrElse(throw new IllegalStateException("original restricted Full26 history anchor missing"))._2
      if !readOwners.exists(_.ownsRetainedImage(anchor)) then throw new IllegalStateException("original restricted Full26 history anchor retired")
      val readerLease=new MySqlDeploymentReceiptSession.RestrictedSourceReadLease5(
        this,actor,restrictedRecallProof,restrictedPublication,link,root,actualLedger,coordinator,anchor)
      val runner=new MySqlFencedTransaction(inputs.auditExpectation,coordinator,verified,scope,Some(actualLedger))
      restrictedSourceLink=link
      restrictedSourceLedger=actualLedger
      restrictedSourceReaderLease=readerLease
      restrictedSourceRunner=runner
      runner.bindRestrictedRecallSession(this,restrictedRecallProof,restrictedPublication,root,actualLedger,readerLease)
    catch
      case failure:Throwable=>
        restrictedSourceRunner=null
        restrictedSourceReaderLease=null
        restrictedSourceLedger=null
        restrictedSourceLink=null
        try root.releaseRetainedMemory(512L,"unpublished restricted source runner/read lease")
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(failure,cleanup)
        throw failure
    MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted(MySqlProductionBootReply.issueRestrictedRecall(restrictedPublication))

  private[mysql] def isRestrictedRecall:Boolean =
    requireOwner();restrictedRecallProof!=null
  private[mysql] def currentRestrictedRecallFreshness():MySqlOwnedAdmissionFreshness =
    requireOwner()
    if primary!=null then throw primary
    if quiescing || critical!=null || restrictedSourceFreshWriteScope!=null ||
        restrictedSourceFreshWritePendingReader!=null || restrictedSourceFreshWritePendingScopeHolder ||
        restrictedRecallProof==null || admission!=null ||
        !verified.connection.getAutoCommit then throw new IllegalStateException("original idle restricted schema5 Recall keeper required")
    val value=restrictedRecallProof.withCurrent(inputs.auditExpectation,coordinator) { (_,_,_,receipt)=>receipt.ownedFreshness(renewSequence) }
    if !value.isCurrent then throw new IllegalStateException("restricted schema5 Recall freshness expired")
    value
  private[mysql] def maintainRestrictedRecall():MySqlOwnedAdmissionFreshness =
    currentRestrictedRecallFreshness()
    try
      barrier.readmit(verified,inputs.auditExpectation,scope)
      val next=Math.incrementExact(renewSequence)
      coordinator.renewDeploymentReceiptRecall(restrictedRecallProof)
      renewSequence=next
      currentRestrictedRecallFreshness()
    catch
      case cause:Throwable=>
        primary=cause
        throw cause

  def maintainAdmission(): MySqlOwnedAdmissionFreshness =
    requireOwner()
    if primary!=null then throw primary
    if quiescing || critical!=null || coordinator==null || admission==null || !verified.connection.getAutoCommit then
      throw new IllegalStateException("schema5 maintenance requires original idle completed adoption")
    try
      barrier.readmit(verified,inputs.auditExpectation,scope)
      val current=coordinator.requireBusinessAdmission(verified,scope)
      if current ne admission then throw new IllegalStateException("schema5 keeper admission reference changed")
      val sequence=Math.incrementExact(renewSequence)
      val token=coordinator.renew(current,verified,scope)
      admission=coordinator.auditAndConfirmPostAcquire(token)
      renewSequence=sequence
      val freshness=admission.receipt.ownedFreshness(renewSequence)
      if !freshness.isCurrent then throw new IllegalStateException("schema5 keeper post-audit freshness expired before publication")
      freshness
    catch
      case cause:Throwable=>primary=cause;throw cause

  private[mysql] def currentFreshness():MySqlOwnedAdmissionFreshness =
    requireOwner()
    if primary!=null then throw primary
    if quiescing || admission==null || coordinator==null ||
        (coordinator.requireBusinessAdmission(verified,scope) ne admission) then
      throw new IllegalStateException("original completed schema5 keeper admission required")
    val value=admission.receipt.ownedFreshness(renewSequence)
    if !value.isCurrent then throw new IllegalStateException("original schema5 keeper freshness expired")
    value

  private[mysql] def renewLease():MySqlOwnedDatabaseAdmission =
    try
      maintainAdmission()
      val current=coordinator.requireBusinessAdmission(verified,scope)
      if current ne admission then throw new IllegalStateException("renewed schema5 admission reference changed")
      MySqlOwnedDatabaseAdmission(current.bootUuid,identity.bootGeneration,identity.connectionGeneration,
        current.bindingUuid,current.writerEpoch)
    catch case cause:Throwable=>
      primary=MySqlOwnedSessionLifecycle.combine(Option(primary),cause).orNull
      throw cause

  def closeForWorkerExit(workerPrimary: Option[Throwable], intent: MySqlDeploymentReceiptSession.TerminalIntent5): Unit =
    if restrictedSourceR0UnknownInspection!=null then
      throw new IllegalStateException("retained restricted SOURCE R0 UNKNOWN inspection still owns its original Session and blocks terminal release",primary)
    requireOwner()
    require(workerPrimary != null && workerPrimary.forall(_ != null), "actual worker failure option required")
    if intent==null || (intent ne terminalIntent) || (intent.session ne this) || !quiescing || normalSettlement!=null then
      throw new IllegalStateException("one original prepared schema5 terminal intent required")
    workerPrimary.foreach { cause=>
      if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
    }
    var jdbcClosed=verified==null && !hasUntransferredRaw
    def settle(action: =>Unit):Unit =
      try action
      catch case cause:Throwable=>
        if primary==null then primary=cause else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cause)
    if primary==null then settle {
      if coordinator==null || admission==null then throw new IllegalStateException("original schema5 keeper/admission missing at terminal")
      val actual=coordinator.requireBusinessAdmission(verified,scope)
      if actual ne admission then throw new IllegalStateException("schema5 terminal admission reference changed")
      coordinator.release(actual.token,verified,scope)
    }
    if verified!=null then
      settle(verified.close())
      settle { jdbcClosed=verified.connection.isClosed }
    if verified==null && hasUntransferredRaw then
      settle { initialRawClosed=Some(initialOpenHolder.rawConnectionClosed()); jdbcClosed=initialRawClosed.contains(true) }
    var barrierRetained=verified!=null
    if verified!=null then settle { barrierRetained=verified.sessionBarrierCloseObservation().mayRetainServerLock }
    if primary!=null then throw primary
    if !jdbcClosed || barrierRetained then
      primary=new IllegalStateException("schema5 actual JDBC/barrier terminal settlement remains uncertain")
      throw primary
    sqlRetirementReturned=true
    nativeOwner.withRetirementIdentity(this,intent) { (source,io)=>
      nativeRetirement=new MySqlDeploymentReceiptSession.NativeRetirementPermit5(this,intent,nativeOwner,source,io)
    }
    settle(actor.retireForTerminal(intent,root))
    readOwners=readOwners.filter { reader=>
      var retired=false
      settle { reader.retireForTerminal(intent);retired=true }
      !retired
    }
    restrictedSourceReaders=restrictedSourceReaders.filter(reader=>readOwners.exists(_ eq reader))
    if readOwners.isEmpty then readOwner=null
    if primary==null then settle(nativeOwner.retireAfterSqlSettlement(nativeRetirement))
    if primary!=null then throw primary
    if nativeOwner.hasUnsettledResources || readOwners.nonEmpty || !actor.terminalRetirementComplete then
      primary=new IllegalStateException("schema5 original terminal holders remain unsettled")
      throw primary
    normalSettlement=new MySqlDeploymentReceiptSession.NormalTerminalSettlement5(this,identity,root,intent)

  private[mysql] def terminalSettlement:Option[MySqlDeploymentReceiptSession.NormalTerminalSettlement5] =
    requireOwner();Option(normalSettlement)
  private[mysql] def requireNormalSettlement(value:MySqlDeploymentReceiptSession.NormalTerminalSettlement5,
      actualRoot:MySqlFormat3ReadBudget,actualIdentity:MySqlOwnedIoIdentity):Unit =
    if Thread.currentThread() ne thread then throw new IllegalStateException("exact schema5 terminal observation worker required")
    if !terminalInputsRetired then requireOwner()
    if value==null || (value ne normalSettlement) || (actualRoot ne root) || (actualIdentity ne identity) ||
        primary!=null || !sqlRetirementReturned || (!terminalInputsRetired && (nativeOwner==null || nativeOwner.hasUnsettledResources)) ||
        readOwners.nonEmpty || restrictedSourceR0UnknownInspection!=null || !actor.terminalRetirementComplete then
      throw new IllegalStateException("actual original schema5 terminal settlement required",primary)
  private[mysql] def detachIssuerForTerminal(value:MySqlDeploymentReceiptSession.NormalTerminalSettlement5,
      expected:MySqlOwnedRetainedPlanIssuer,ledger:MySqlFormat3AllocationLedger):Unit =
    requireNormalSettlement(value,root,identity)
    requireRetainedIssuer(expected,ledger)
    expected.detachDeploymentReceiptReferencesForExit(this,root.profile,ledger)
    retainedPlanIssuer=null;purposeRunner=null;purposeLedger=null
  private[mysql] def retireInputsForTerminal(value:MySqlDeploymentReceiptSession.NormalTerminalSettlement5):Unit =
    requireNormalSettlement(value,root,identity)
    if retainedPlanIssuer!=null then throw new IllegalStateException("original schema5 issuer still retains terminal input graph")
    auditor=null;bootstrapQualification=null;coordinator=null;admission=null;verified=null;initialOpenHolder=null;scope=null;barrier=null
    ordinaryPostAudit=null;ordinaryAdoption=null;nativeOwner=null
    try
      inputs.close()
      terminalInputsRetired=true
    catch
      case cause:Throwable=>
        primary=cause
        throw cause

  def failure: Option[Throwable] = { requireOwner(); Option(primary) }
  def hasPendingResources: Boolean =
    requireOwner()
    verified != null || (hasUntransferredRaw && !initialRawClosed.contains(true)) || nativeOwner != null || readOwner != null || restrictedSourceR0UnknownInspection != null

  /** 仅 failed initialization 的 worker exit；没有正常终止许可，也不尝试普通 native close。 */
  def failInitializationForWorkerExit(workerPrimary: Option[Throwable]): MySqlDeploymentReceiptSession.FailedInitializationExit =
    if Thread.currentThread() ne thread then throw new IllegalStateException("exact schema5 failure cleanup worker required")
    require(workerPrimary != null && workerPrimary.forall(_ != null), "actual worker failure option required")
    if failedExit != null then return failedExit
    inputs.requireForSession(root, identity, mode)
    val cause = workerPrimary.orElse(Option(primary)).getOrElse(
      throw new IllegalStateException("failed initialization cleanup requires original failure"))
    if primary != null && (primary ne cause) then MySqlOwnedSessionLifecycle.suppress(cause, primary)
    primary = cause
    quiescing = true
    if restrictedSourceR0UnknownInspection!=null then
      // The retained UNKNOWN owner is the only authority for these old/new connections and account children.
      // A generic worker-exit cleanup must not repeat an old close or detach the live partial graph.
      failedExit = new MySqlDeploymentReceiptSession.FailedInitializationExit(identity, cause,
        jdbcClosed=false, nativeResponsibilityRetained=nativeOwner!=null,
        readResponsibilityRetained=true, canRetireStaticAndRoot=false)
      return failedExit
    var settled = true
    def close(action: => Unit): Unit =
      try action
      catch case failure: Throwable =>
        settled = false
        MySqlOwnedSessionLifecycle.suppress(cause, failure)
    if readOwners.nonEmpty then
      close {
        readOwners=readOwners.filter { reader=>
          val pending= !reader.abortInitialization(cause)
          if pending then settled=false
          pending
        }
        if readOwners.isEmpty then readOwner=null
      }
    val nativeRetained = nativeOwner != null && nativeOwner.attemptedNativeConstruction
    if nativeOwner != null && !nativeRetained then close { nativeOwner.close(); nativeOwner = null }
    var jdbcClosed = verified == null && !hasUntransferredRaw
    if verified != null then
      close(verified.close())
      close { jdbcClosed = verified.connection.isClosed }
    if verified == null && hasUntransferredRaw then
      if !initialOpenHolder.closeReturned() then settled = false
      close { initialRawClosed = Some(initialOpenHolder.rawConnectionClosed()); jdbcClosed = initialRawClosed.contains(true) }
    var barrierRetained = verified != null
    if verified != null then close { barrierRetained = verified.sessionBarrierCloseObservation().mayRetainServerLock }
    auditor = null
    // native borrow 或任一失败仍引用原 static/root；不能提前 detach inputs 或退费。
    val retire = settled && jdbcClosed && !barrierRetained && !nativeRetained && nativeOwner == null && readOwner == null
    var detached = false
    if retire then
      close { inputs.close(); detached = true }
    // Session/actor 仍在 adapter 槽中；其 2048B 由实际撤槽后的 root 退休结算，不在此提前退费。
    failedExit = new MySqlDeploymentReceiptSession.FailedInitializationExit(identity, cause,
      jdbcClosed, nativeRetained, readOwner != null, detached && settled && !barrierRetained)
    failedExit

private[floatingpod] object MySqlDeploymentReceiptSession:
  private[mysql] enum RestrictedSourceR0ExecutionState:
    case Running
    case CommitAcknowledged
    case KnownNotCommitted
    case CommitUnknown
    case SettlementUncertain

  private[mysql] enum RestrictedSourceR0ReadbackState:
    case Pending
    case Blocked(cause:Throwable)
    case Attempted(owner:RestrictedSourceR0PostCommitReadback5)
    case CompleteData(owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,
      snapshot:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow)
    case Unexpected(owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,
      snapshot:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,cause:Throwable)
    case Failed(owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,cause:Throwable)

  private[mysql] enum RestrictedSourceR0UnknownInspectionClass:
    case ExactTarget
    case ExactBefore
    case Neither

  private[mysql] enum RestrictedSourceR0UnknownInspectionState:
    case RegistrationFailed(cause:Throwable)
    case Pending(owner:RestrictedSourceR0UnknownInspection5)
    case Failed(owner:RestrictedSourceR0UnknownInspection5,cause:Throwable)
    case Data(owner:RestrictedSourceR0UnknownInspection5,reader:MySqlDeploymentReceiptReadOwner,
      snapshot:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,
      classification:RestrictedSourceR0UnknownInspectionClass)

  /** 仅由 Session 签发的一次性 ACK 后 Full26 读回责任。 */
  private[mysql] final class RestrictedSourceR0PostCommitReadback5 private[MySqlDeploymentReceiptSession] (
      private[mysql] val session:MySqlDeploymentReceiptSession,
      private[mysql] val plan:MySqlFencedTransaction.SourceReservePlan5,
      private[mysql] val outcome:RestrictedSourceR0ExecutionOutcome,
      private[mysql] val freshScope:RestrictedSourceFreshWriteScope5,
      private[mysql] val readerLease:RestrictedSourceReadLease5,
      private[mysql] val root:MySqlFormat3ReadBudget,
      private[mysql] val ledger:MySqlFormat3AllocationLedger,
      private[mysql] val dataBudget:MySqlFormat3ReadBudget):
    require(session!=null && plan!=null && outcome!=null && freshScope!=null && readerLease!=null &&
      root!=null && ledger!=null && dataBudget!=null && (outcome.plan eq plan) &&
      (freshScope.plan eq plan) && (freshScope.session eq session) && (freshScope.readerLease eq readerLease) &&
      (freshScope.root eq root) && (freshScope.ledger eq ledger) && (outcome.dataBudget eq dataBudget),
      "exact original Session/plan/outcome/settled write scope and DATA responsibility required")
    private[mysql] var attemptStarted=false
    private[mysql] var reader:MySqlDeploymentReceiptReadOwner=null
    private[mysql] var captureReturned=false
    private[mysql] var readTransactionSettled=false
    private[mysql] var compareAttempted=false
    private[mysql] var observedSnapshot:MySqlScriptSchemaSnapshot=null
    private[mysql] var observedWriter:MySqlBusinessWriterRow=null
    private[mysql] var cleanupSettled=false
    private[mysql] var failure:Throwable=null

    private[MySqlDeploymentReceiptSession] def markAttemptStarted():Unit =
      if attemptStarted || (outcome.postCommitReadbackOwner ne this) then
        throw new IllegalStateException("one registered restricted SOURCE R0 ACK readback attempt required")
      attemptStarted=true

    private[MySqlDeploymentReceiptSession] def bindReader(actual:MySqlDeploymentReceiptReadOwner):Unit =
      if !attemptStarted || actual==null || reader!=null then
        throw new IllegalStateException("one original Session-registered restricted SOURCE R0 ACK reader required")
      reader=actual

    private[MySqlDeploymentReceiptSession] def markCaptureReturned():Unit =
      if !attemptStarted || reader==null || captureReturned || readTransactionSettled then
        throw new IllegalStateException("one completed restricted SOURCE R0 ACK Full26 capture required")
      captureReturned=true

    private[MySqlDeploymentReceiptSession] def markReadTransactionSettled():Unit =
      if !captureReturned || reader==null || readTransactionSettled || !reader.isSettled then
        throw new IllegalStateException("settled restricted SOURCE R0 ACK read transaction required")
      readTransactionSettled=true

    private[MySqlDeploymentReceiptSession] def retainObserved(
        actual:MySqlScriptSchemaSnapshot,actualWriter:MySqlBusinessWriterRow):Unit =
      if !readTransactionSettled || reader==null || actual==null || actualWriter==null ||
          observedSnapshot!=null || observedWriter!=null || !reader.ownsRetainedImage(actual) then
        throw new IllegalStateException("actual settled retained restricted SOURCE R0 ACK Full26 image required")
      observedSnapshot=actual
      observedWriter=actualWriter

    private[MySqlDeploymentReceiptSession] def markCompareAttempted():Unit =
      if !readTransactionSettled || observedSnapshot==null || observedWriter==null || compareAttempted then
        throw new IllegalStateException("one exact settled restricted SOURCE R0 ACK Full26 comparison required")
      compareAttempted=true

    private[mysql] def isAttemptedState(
        actual:Option[RestrictedSourceR0ReadbackState]):Boolean = actual match
      case Some(RestrictedSourceR0ReadbackState.Attempted(owner)) => owner eq this
      case _ => false

  /** One original-worker UNKNOWN inspection owner; its resources remain settlement-only and retained. */
  private[mysql] final class RestrictedSourceR0UnknownInspection5 private[MySqlDeploymentReceiptSession] (
      private[mysql] val session:MySqlDeploymentReceiptSession,
      private[mysql] val plan:MySqlFencedTransaction.SourceReservePlan5,
      private[mysql] val outcome:RestrictedSourceR0ExecutionOutcome,
      private[mysql] val freshScope:RestrictedSourceFreshWriteScope5,
      private[mysql] val oldVerified:VerifiedMySqlConnection,
      private[mysql] val oldScope:MySqlAuditBootScope,
      private[mysql] val oldBarrier:HeldSessionBarrier,
      private[mysql] val readerLease:RestrictedSourceReadLease5,
      private[mysql] val root:MySqlFormat3ReadBudget,
      private[mysql] val ledger:MySqlFormat3AllocationLedger,
      private[mysql] val dataBudget:MySqlFormat3ReadBudget,
      private[mysql] val worker:Thread,
      private[mysql] val primaryUnknown:MySqlCommitUnknownException):
    require(session!=null && plan!=null && outcome!=null && freshScope!=null && oldVerified!=null && oldScope!=null &&
      oldBarrier!=null && readerLease!=null && root!=null && ledger!=null && dataBudget!=null && worker!=null &&
      primaryUnknown!=null && (outcome.plan eq plan) && (outcome.freshScope eq freshScope) &&
      (outcome.commitUnknown eq primaryUnknown) && (freshScope.session eq session) &&
      (freshScope.plan eq plan) && (freshScope.root eq root) && (freshScope.ledger eq ledger) &&
      (outcome.dataBudget eq dataBudget),
      "exact original Session/R0 UNKNOWN scope, verified connection and retained DATA owner required")
    private[mysql] var auditAccount:MySqlFormat3ReadBudget=null
    private[mysql] var audit:MySqlSchemaAuditor.SourceUnknownInspectionAudit=null
    private[mysql] var openHolder:MySqlConnectionSource.SourceUnknownInspectionOpen=null
    private[mysql] var newVerified:VerifiedMySqlConnection=null
    private[mysql] var newScope:MySqlAuditBootScope=null
    private[mysql] var newBarrier:HeldSessionBarrier=null
    private[mysql] var newCloseAttempted=false
    private[mysql] var newCloseReturned=false
    private[mysql] var newCloseFailure:Throwable=null
    private[mysql] var newCloseObservation:MySqlSessionBarrierCloseObservation=null
    private[mysql] var qualification:VerifiedSchemaQualification=null
    private[mysql] var reader:MySqlDeploymentReceiptReadOwner=null
    private[mysql] var actualSnapshot:MySqlScriptSchemaSnapshot=null
    private[mysql] var actualWriter:MySqlBusinessWriterRow=null
    private[mysql] var snapshotImage:MySqlFormat3ReadBudget=null
    private[mysql] var snapshotImageFeeReserved=false
    private[mysql] var classification=Option.empty[RestrictedSourceR0UnknownInspectionClass]
    private[mysql] var oldCloseAttempted=false
    private[mysql] var oldCloseReturned=false
    private[mysql] var oldCloseFailure:Throwable=null
    private[mysql] var oldCloseObservation:MySqlSessionBarrierCloseObservation=null
    private[mysql] var inspectionGeneration=0L
    private[mysql] var readerTransactionSettled=false
    private[mysql] var attemptStarted=false
    private[mysql] var retired=false
    private[mysql] var failure:Throwable=null

    private[mysql] def requireCurrent():Unit =
      session.requireRestrictedSourceUnknownInspection(this)

    private[mysql] def requireAuditBinding(
        expectation:MySqlSchemaAuditExpectation,verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
        exactRoot:MySqlFormat3ReadBudget,audit:MySqlFormat3ReadBudget):Unit =
      session.requireRestrictedSourceUnknownAuditBinding(this,expectation,verified,scope,exactRoot,audit)

    private[mysql] def bindAuditAccount(actual:MySqlFormat3ReadBudget):Unit =
      requireCurrent()
      if actual==null || auditAccount!=null || actual.isClosed ||
          !actual.isCanonicalComparisonChildOf(root) || !actual.sharesAllocationLedger(root) ||
          (actual.profile ne root.profile) then
        throw new IllegalStateException("one exact registered same-root UNKNOWN audit account required",failure)
      root.requireScriptDataInputs(session.inputs.full26Inputs)
      auditAccount=actual

    private[mysql] def bindAudit(actual:MySqlSchemaAuditor.SourceUnknownInspectionAudit):Unit =
      requireCurrent()
      if actual==null || audit!=null then
        throw new IllegalStateException("one registered UNKNOWN owned-audit partial holder required",failure)
      audit=actual
      if auditAccount==null || !actual.isBoundTo(this) then
        throw new IllegalStateException("registered UNKNOWN owned-audit holder differs from its original Session/account",failure)

    private[mysql] def bindOpenHolder(actual:MySqlConnectionSource.SourceUnknownInspectionOpen):Unit =
      requireCurrent()
      if actual==null || openHolder!=null || inspectionGeneration<=oldScope.connectionGeneration then
        throw new IllegalStateException("one pre-registered UNKNOWN source open holder and fresh generation required",failure)
      openHolder=actual

    private[mysql] def bindNewConnection(actual:VerifiedMySqlConnection,actualScope:MySqlAuditBootScope):Unit =
      requireCurrent()
      if actual==null || actualScope==null || newVerified!=null then
        throw new IllegalStateException("one transferred UNKNOWN inspection connection required",failure)
      // Retain the actual transferred handle before validating scope provenance so failure cleanup can close it once.
      newVerified=actual
      newScope=actualScope
      if openHolder==null ||
          (openHolder.transferredVerified() ne actual) || (actualScope.verifiedConnection.getOrElse(null) ne actual) ||
          (actualScope.ownerThread ne worker) || actualScope.bootUuid!=oldScope.bootUuid ||
          actualScope.bootGeneration!=oldScope.bootGeneration || actualScope.connectionGeneration!=inspectionGeneration ||
          inspectionGeneration<=oldScope.connectionGeneration then
        throw new IllegalStateException("exact transferred UNKNOWN inspection connection and fresh same-boot scope required",failure)

    private[mysql] def bindBarrier(actual:HeldSessionBarrier):Unit =
      requireCurrent()
      if actual==null || newBarrier!=null then
        throw new SQLNonTransientConnectionException("one actual UNKNOWN inspection barrier result required")
      // Keep even a rejected Held token reachable; it may still represent a server-side lock responsibility.
      newBarrier=actual
      if newVerified==null || newScope==null ||
          actual.actualConnectionId<=0L || actual.connectionGenerationId==null || actual.namespace==null ||
          actual.namespace.lockName()!=oldBarrier.namespace.lockName() then
        throw new SQLNonTransientConnectionException("same-namespace UNKNOWN inspection barrier was not acquired")

    private[mysql] def bindQualification(actual:VerifiedSchemaQualification):Unit =
      requireCurrent()
      if actual==null || audit==null || qualification!=null || newBarrier==null then
        throw new IllegalStateException("one actual fresh schema5 UNKNOWN inspection qualification required",failure)
      qualification=actual

    private[mysql] def bindReader(actual:MySqlDeploymentReceiptReadOwner):Unit =
      requireCurrent()
      if actual==null || reader!=null || newVerified==null || newScope==null || qualification==null || newBarrier==null then
        throw new IllegalStateException("one registered independent UNKNOWN Full26 reader required",failure)
      reader=actual

    private[mysql] def prechargeSnapshotImage(actual:MySqlDeploymentReceiptReadOwner):Unit =
      requireReader(actual)
      if snapshotImageFeeReserved || snapshotImage!=null then
        throw new IllegalStateException("one UNKNOWN snapshot child fee reservation required",failure)
      dataBudget.reserveRetainedMemory(512L,"UNKNOWN Full26 snapshot child owner registration BEFORE allocation")
      snapshotImageFeeReserved=true

    private[mysql] def bindSnapshotImage(actual:MySqlDeploymentReceiptReadOwner,
        child:MySqlFormat3ReadBudget):Unit =
      requireReader(actual)
      if !snapshotImageFeeReserved || snapshotImage!=null || child==null || child.isClosed ||
          !child.isSnapshotReadChildOf(root) || !child.sharesAllocationLedger(root) ||
          (child.profile ne root.profile) then
        throw new IllegalStateException("exact registered new Full26 snapshot child required",failure)
      snapshotImage=child

    private[mysql] def requireReader(actual:MySqlDeploymentReceiptReadOwner):Unit =
      requireCurrent()
      if (reader ne actual) || actual==null || (actual.unknownInspectionOwner ne this) ||
          (actual.verifiedConnection ne newVerified) || (actual.auditScope ne newScope) then
        throw new IllegalStateException("exact registered UNKNOWN-role reader/current connection required",failure)
      newBarrier.readmit(newVerified,session.inputs.auditExpectation,newScope)
      MySqlDeploymentReceiptSession.requireFreshQualification(qualification,session.inputs.auditExpectation,newVerified,newScope)

    private[mysql] def markReaderTransactionSettled():Unit =
      requireCurrent()
      if reader==null || readerTransactionSettled || !reader.isSettled then
        throw new IllegalStateException("settled independent UNKNOWN read transaction required",failure)
      readerTransactionSettled=true

    private[mysql] def retainObserved(actual:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow):Unit =
      requireCurrent()
      if !readerTransactionSettled || reader==null || actual==null || writer==null ||
          actualSnapshot!=null || actualWriter!=null || !reader.ownsRetainedImage(actual) then
        throw new IllegalStateException("settled retained UNKNOWN Full26 image and writer required",failure)
      actualSnapshot=actual
      actualWriter=writer

    private[mysql] def recordFailure(cause:Throwable):Unit =
      if cause==null then throw new IllegalArgumentException("actual UNKNOWN inspection cause required")
      if failure==null then failure=cause
      else if failure ne cause then MySqlOwnedIoAdapter.suppressExitCleanup(failure,cause)

    private[mysql] def closeNewForFailure(primary:Throwable):Unit =
      if primary==null then throw new IllegalArgumentException("exact UNKNOWN inspection failure required")
      val transferred = if newVerified!=null then newVerified
        else if openHolder!=null then openHolder.transferredVerified()
        else null
      if transferred!=null && !newCloseAttempted then
        newVerified=transferred
        newCloseAttempted=true
        try
          transferred.close()
          newCloseReturned=true
        catch
          case cleanup:Throwable=>
            newCloseFailure=cleanup
            MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)
        try newCloseObservation=transferred.sessionBarrierCloseObservation()
        catch
          case cleanup:Throwable=>
            if newCloseFailure==null then newCloseFailure=cleanup
            MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)

    private[mysql] def bindClassification(value:RestrictedSourceR0UnknownInspectionClass):Unit =
      requireCurrent()
      if value==null || classification.nonEmpty || !readerTransactionSettled || actualSnapshot==null || actualWriter==null ||
          !reader.ownsRetainedImage(actualSnapshot) then
        throw new IllegalStateException("one settled qualified UNKNOWN DATA classification required",failure)
      newBarrier.readmit(newVerified,session.inputs.auditExpectation,newScope)
      MySqlDeploymentReceiptSession.requireFreshQualification(qualification,session.inputs.auditExpectation,newVerified,newScope)
      classification=Some(value)

  /** Exact accepted SOURCE R0 result holder; status is mutable only on the original worker. */
  private[mysql] final class RestrictedSourceR0ExecutionOutcome private[MySqlDeploymentReceiptSession] (
      private[mysql] val plan:MySqlFencedTransaction.SourceReservePlan5,
      private[mysql] val tag:IoOperationTag,
      private[mysql] val acceptedRepositoryEpochAtSubmission:Long,
      private[mysql] val dataBudget:MySqlFormat3ReadBudget):
    private[mysql] var state=RestrictedSourceR0ExecutionState.Running
    private[mysql] var failure:Throwable=null
    private[mysql] var commitUnknown:MySqlCommitUnknownException=null
    private[mysql] var context:MySqlOperationContext=null
    private[mysql] var plannedAt:LocalDateTime=null
    private[mysql] var resultAt:LocalDateTime=null
    private[mysql] var witness:MySqlNormalRecallCanonical.SourceChildReserveWitnessData=null
    private[mysql] var expectedTarget:MySqlScriptSchemaSnapshot=null
    private[mysql] var actualBefore:MySqlScriptSchemaSnapshot=null
    private[mysql] var beforeWriter:MySqlBusinessWriterRow=null
    private[mysql] var freshScope:RestrictedSourceFreshWriteScope5=null
    private[mysql] var postCommitReadback=Option.empty[RestrictedSourceR0ReadbackState]
    private[mysql] var postCommitReadbackOwner:RestrictedSourceR0PostCommitReadback5=null
    private[mysql] var postCommitReadbackFailure:Throwable=null
    private[mysql] var unknownInspectionOwner:RestrictedSourceR0UnknownInspection5=null
    private[mysql] var unknownInspection=Option.empty[RestrictedSourceR0UnknownInspectionState]
    private[mysql] var verifiedPrecommit=false
    private[mysql] var committedCleanupFailure:Throwable=null

    require(plan!=null && tag!=null && acceptedRepositoryEpochAtSubmission>=0L && dataBudget!=null,
      "exact accepted restricted SOURCE R0 result owner required")

    private[mysql] def retainPrecommit(
        exactScope:RestrictedSourceFreshWriteScope5,
        exactContext:MySqlOperationContext,
        exactPlannedAt:LocalDateTime,
        exactResultAt:LocalDateTime,
        exactWitness:MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
        exactTarget:MySqlScriptSchemaSnapshot,
        exactBefore:MySqlScriptSchemaSnapshot,
        exactBeforeWriter:MySqlBusinessWriterRow):Unit =
      if state!=RestrictedSourceR0ExecutionState.Running || exactScope==null || exactContext==null ||
          exactPlannedAt==null || exactResultAt==null || exactWitness==null || exactTarget==null ||
          exactBefore==null || exactBeforeWriter==null || !exactScope.hasVerifiedTarget then
        throw new IllegalStateException("one actual accepted SOURCE R0 verified precommit witness/target result is required")
      freshScope=exactScope
      context=exactContext
      plannedAt=exactPlannedAt
      resultAt=exactResultAt
      witness=exactWitness
      expectedTarget=exactTarget
      actualBefore=exactBefore
      beforeWriter=exactBeforeWriter
      verifiedPrecommit=true
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.Pending)

    private[mysql] def markCommitAcknowledged(exactScope:RestrictedSourceFreshWriteScope5):Unit =
      require(state==RestrictedSourceR0ExecutionState.Running && verifiedPrecommit &&
        (freshScope eq exactScope) && exactScope.commitAcknowledged && exactScope.commitUnknown==null,
        "actual returned restricted SOURCE R0 JDBC commit and verified precommit readback are required")
      committedCleanupFailure=exactScope.committedCleanupFailure
      state=RestrictedSourceR0ExecutionState.CommitAcknowledged

    private[mysql] def beginPostCommitReadback(
        exactScope:RestrictedSourceFreshWriteScope5,owner:RestrictedSourceR0PostCommitReadback5):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitAcknowledged || !verifiedPrecommit || owner==null ||
          (freshScope ne exactScope) || (owner.freshScope ne exactScope) ||
          committedCleanupFailure!=null || postCommitReadbackOwner!=null ||
          !postCommitReadback.contains(RestrictedSourceR0ReadbackState.Pending) then
        throw new IllegalStateException("one cleanly ACKed restricted SOURCE R0 postcommit readback registration required")
      postCommitReadbackOwner=owner
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.Attempted(owner))

    private[mysql] def markPostCommitReadbackBlocked(cause:Throwable):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitAcknowledged || cause==null ||
          postCommitReadbackOwner!=null || !postCommitReadback.contains(RestrictedSourceR0ReadbackState.Pending) then
        throw new IllegalStateException("ACKed restricted SOURCE R0 readback may be blocked only before its one-shot registration",cause)
      postCommitReadbackFailure=cause
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.Blocked(cause))

    private[mysql] def markPostCommitReadbackFailure(
        owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,cause:Throwable):Unit =
      val alreadyAttempted=postCommitReadback.exists {
        case RestrictedSourceR0ReadbackState.Attempted(actual) => actual eq owner
        case _=>false
      }
      val registeredBeforeState=postCommitReadback.contains(RestrictedSourceR0ReadbackState.Pending) &&
        postCommitReadbackOwner==null && owner!=null && owner.outcome==this
      if state!=RestrictedSourceR0ExecutionState.CommitAcknowledged || owner==null || cause==null ||
          (!alreadyAttempted && !registeredBeforeState) ||
          (alreadyAttempted && (postCommitReadbackOwner ne owner)) || (owner.reader ne reader) then
        throw new IllegalStateException("exact attempted restricted SOURCE R0 ACK readback failure state required",cause)
      if registeredBeforeState then
        postCommitReadbackOwner=owner
        postCommitReadback=Some(RestrictedSourceR0ReadbackState.Attempted(owner))
      if owner.failure==null then owner.failure=cause
      else if owner.failure ne cause then MySqlOwnedIoAdapter.suppressExitCleanup(owner.failure,cause)
      postCommitReadbackFailure=owner.failure
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.Failed(owner,reader,owner.failure))

    private[mysql] def retainUnexpectedPostCommitReadback(
        owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,
        actual:MySqlScriptSchemaSnapshot,actualWriter:MySqlBusinessWriterRow,cause:Throwable):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitAcknowledged || owner==null || reader==null ||
          actual==null || actualWriter==null || cause==null || (postCommitReadbackOwner ne owner) ||
          (owner.reader ne reader) || (owner.observedSnapshot ne actual) || (owner.observedWriter ne actualWriter) ||
          !owner.readTransactionSettled || !reader.ownsRetainedImage(actual) ||
          !owner.isAttemptedState(postCommitReadback) then
        throw new IllegalStateException("actual settled unexpected restricted SOURCE R0 ACK Full26 observation required",cause)
      owner.failure=cause
      postCommitReadbackFailure=cause
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.Unexpected(owner,reader,actual,actualWriter,cause))

    private[mysql] def retainPostCommitReadbackData(
        owner:RestrictedSourceR0PostCommitReadback5,reader:MySqlDeploymentReceiptReadOwner,
        actual:MySqlScriptSchemaSnapshot,actualWriter:MySqlBusinessWriterRow):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitAcknowledged || owner==null || reader==null ||
          actual==null || actualWriter==null || (postCommitReadbackOwner ne owner) ||
          (owner.reader ne reader) || (owner.observedSnapshot ne actual) || (owner.observedWriter ne actualWriter) ||
          !owner.readTransactionSettled || !owner.compareAttempted || !reader.ownsRetainedImage(actual) ||
          !owner.isAttemptedState(postCommitReadback) then
        throw new IllegalStateException("exact settled target DATA and retained ACK Full26 image required",owner.failure)
      postCommitReadback=Some(RestrictedSourceR0ReadbackState.CompleteData(owner,reader,actual,actualWriter))

    private[mysql] def markCommitUnknown(exactScope:RestrictedSourceFreshWriteScope5):Unit =
      require(state==RestrictedSourceR0ExecutionState.Running && (freshScope eq exactScope) &&
        exactScope.commitAttempted && !exactScope.commitAcknowledged && exactScope.commitUnknown!=null,
        "actual attempted restricted SOURCE R0 commit without returned ACK is required")
      commitUnknown=exactScope.commitUnknown
      failure=exactScope.commitUnknown
      state=RestrictedSourceR0ExecutionState.CommitUnknown

    private[mysql] def beginUnknownInspection(
        exactScope:RestrictedSourceFreshWriteScope5,owner:RestrictedSourceR0UnknownInspection5):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitUnknown || !verifiedPrecommit || owner==null ||
          (freshScope ne exactScope) || (owner.freshScope ne exactScope) ||
          (owner.outcome ne this) || (owner.plan ne plan) || unknownInspectionOwner!=null ||
          unknownInspection.nonEmpty || !exactScope.commitAttempted || exactScope.commitAcknowledged ||
          exactScope.commitUnknown==null || (commitUnknown ne exactScope.commitUnknown) then
        throw new IllegalStateException("one exact original-worker UNKNOWN inspection registration required",failure)
      unknownInspectionOwner=owner
      unknownInspection=Some(RestrictedSourceR0UnknownInspectionState.Pending(owner))

    private[mysql] def retainUnknownInspectionRegistrationFailure(cause:Throwable):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitUnknown || cause==null ||
          unknownInspectionOwner!=null || unknownInspection.nonEmpty then
        throw new IllegalStateException("UNKNOWN inspection registration failure must precede its owner and side effects",cause)
      unknownInspection=Some(RestrictedSourceR0UnknownInspectionState.RegistrationFailed(cause))

    private[mysql] def retainUnknownInspectionFailure(
        owner:RestrictedSourceR0UnknownInspection5,cause:Throwable):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitUnknown || owner==null || cause==null ||
          (unknownInspectionOwner ne owner) || !unknownInspection.exists {
            case RestrictedSourceR0UnknownInspectionState.Pending(actual) => actual eq owner
            case _ => false
          } then
        throw new IllegalStateException("exact attempted UNKNOWN inspection failure state required",cause)
      owner.recordFailure(cause)
      unknownInspection=Some(RestrictedSourceR0UnknownInspectionState.Failed(owner,owner.failure))

    private[mysql] def retainUnknownInspectionData(
        owner:RestrictedSourceR0UnknownInspection5,reader:MySqlDeploymentReceiptReadOwner,
        actual:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,
        classification:RestrictedSourceR0UnknownInspectionClass):Unit =
      if state!=RestrictedSourceR0ExecutionState.CommitUnknown || owner==null || reader==null || actual==null ||
          writer==null || classification==null || (unknownInspectionOwner ne owner) ||
          (owner.reader ne reader) || (owner.actualSnapshot ne actual) || (owner.actualWriter ne writer) ||
          !owner.readerTransactionSettled || !owner.classification.contains(classification) ||
          !reader.ownsRetainedImage(actual) then
        throw new IllegalStateException("settled qualified UNKNOWN DATA owner required",owner.failure)
      unknownInspection=Some(RestrictedSourceR0UnknownInspectionState.Data(owner,reader,actual,writer,classification))

    private[mysql] def markKnownNotCommitted(cause:Throwable):Unit =
      require(state==RestrictedSourceR0ExecutionState.Running && cause!=null &&
        (freshScope==null || (freshScope.isSettled && freshScope.readerRetirementComplete &&
          !freshScope.commitAttempted && !freshScope.commitAcknowledged && freshScope.commitUnknown==null)),
        "known restricted SOURCE R0 non-commit requires the original cause and confirmed no-commit cleanup")
      failure=cause
      freshScope=null;context=null;plannedAt=null;resultAt=null;witness=null;expectedTarget=null
      actualBefore=null;beforeWriter=null;postCommitReadback=None
      state=RestrictedSourceR0ExecutionState.KnownNotCommitted

    private[mysql] def markSettlementUncertain(cause:Throwable):Unit =
      require(state==RestrictedSourceR0ExecutionState.Running && cause!=null,
        "uncertain restricted SOURCE R0 cleanup requires the original cause")
      failure=cause
      state=RestrictedSourceR0ExecutionState.SettlementUncertain

  final class RestrictedPublication5 private[MySqlDeploymentReceiptSession] (
      session:MySqlDeploymentReceiptSession,proof:RestrictedRecall5,
      val recovery:MySqlOwnedIoAdapter.RecallRecoveryLink,val tag:IoOperationTag,val repositoryEpochAtSubmission:Long):
    private var immutable:MySqlOwnedFormat3Snapshot=null
    private[mysql] def requireCurrent():Unit =
      session.currentRestrictedRecallFreshness()
      if (session.restrictedPublication ne this) || (session.restrictedRecallProof ne proof) ||
          session.publicationLease==null then throw new IllegalStateException("original restricted schema5 publication owner required")
    private[MySqlDeploymentReceiptSession] def initialize():Unit =
      requireCurrent()
      if immutable!=null then throw new IllegalStateException("restricted schema5 immutable input is one-shot")
      immutable=MySqlOwnedFormat3Snapshot.fromRestrictedDeploymentReceiptBoot(session.restrictedRecall.get._2,
        session.inputs.full26Inputs,session.root,session.inputs.auditExpectation,proof,tag,repositoryEpochAtSubmission)
    private[mysql] def snapshot:MySqlOwnedFormat3Snapshot =
      requireCurrent();if immutable==null then throw new IllegalStateException("restricted schema5 immutable input missing")
      immutable
    private[mysql] def originalSourcePins:ProductionBootstrapControl.OriginalSourcePins =
      requireCurrent();session.original.get

  /** 仅实际受限 runner 持有；每次调用都在原Session内新建并登记一次Full26 reader。 */
  final class RestrictedSourceReadLease5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,
      private val actor:MySqlDeploymentReceiptBootstrapActor,
      private val proof:RestrictedRecall5,
      private val publication:RestrictedPublication5,
      private val link:MySqlOwnedIoAdapter.RecallRecoveryLink,
      private val root:MySqlFormat3ReadBudget,
      private val ledger:MySqlFormat3AllocationLedger,
      private val coordinator:MySqlWriterFenceCoordinator,
      private val anchor:MySqlScriptSchemaSnapshot):
    private[mysql] def requireBinding(actualSession:MySqlDeploymentReceiptSession,
        actualActor:MySqlDeploymentReceiptBootstrapActor,actualProof:RestrictedRecall5,
        actualPublication:RestrictedPublication5,actualLink:MySqlOwnedIoAdapter.RecallRecoveryLink,
        actualRoot:MySqlFormat3ReadBudget,actualLedger:MySqlFormat3AllocationLedger):Unit =
      if ((session ne actualSession) || (actor ne actualActor) || (proof ne actualProof) ||
          (publication ne actualPublication) || (link ne actualLink) || (root ne actualRoot) ||
          (ledger ne actualLedger) || (coordinator ne session.coordinator) ||
          (anchor ne session.restrictedRecall.getOrElse(throw new IllegalStateException("restricted Full26 history anchor missing"))._2) ||
          !session.readOwners.exists(_.ownsRetainedImage(anchor))) then
        throw new IllegalStateException("exact original restricted reader/session/history binding required")
      root.requireLedgerIdentity(ledger)

    private[mysql] def withLockedFull26[A](actualFenced:MySqlFencedTransaction,
        actualRoot:MySqlFormat3ReadBudget,actualLedger:MySqlFormat3AllocationLedger)
        (consume:(MySqlDeploymentReceiptReadOwner,MySqlScriptSchemaSnapshot,
          MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow)=>A):A =
      if consume==null then throw new IllegalArgumentException("restricted locked Full26 consumer required")
      session.requireRestrictedSourceRunner(actualFenced,proof,publication,actualRoot,actualLedger,this)
      proof.withCurrent(session.inputs.auditExpectation,coordinator) { (_,_,_,_)=>
        publication.requireCurrent()
        val reader=session.allocateRestrictedSourceReader(this)
        var captured=false
        var primary:Throwable=null
        try
          reader.captureLockedFull26()
          captured=true
          reader.withLockedImage(actor,session.inputs) { (currentSnapshot,writerRow)=>
            session.requireRestrictedSourceReader(this,reader)
            if !session.readOwners.exists(_.ownsRetainedImage(anchor)) then
              throw new IllegalStateException("original restricted HANDOFF Full26 history anchor retired")
            consume(reader,currentSnapshot,anchor,writerRow)
          }
        catch
          case cause:Throwable=>
            primary=cause
            throw cause
        finally
          if captured then MySqlFormat3BudgetCleanup.run(primary,Vector(
            ()=>reader.settleSuccessfulRead(actor)))
      }

    private[mysql] def retireReader(actualFenced:MySqlFencedTransaction,
        actualRoot:MySqlFormat3ReadBudget,actualLedger:MySqlFormat3AllocationLedger,
        reader:MySqlDeploymentReceiptReadOwner):Unit =
      session.retireRestrictedSourceReader(this,actualFenced,actualRoot,actualLedger,reader)

    override def toString:String="RestrictedSourceReadLease5[original Session/actor/root/ledger; no SQL authority]"

  final class Publication5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val completion:MySqlDeploymentReceiptBootstrapActor.Completion5,
      val tag:IoOperationTag,val repositoryEpochAtSubmission:Long,private[mysql] val lease:MySqlRuntimeInputs.ConsumerLease
  ):
    private var immutableInput:MySqlOwnedFormat3Snapshot=null
    private var initialized=false
    private var inputRetired=false
    private[mysql] def initializeImmutableInput():Unit =
      requireCurrent()
      if initialized then throw new IllegalStateException("original Full26 immutable publication is one-shot")
      initialized=true
      immutableInput=MySqlOwnedFormat3Snapshot.fromAcceptedDeploymentReceiptBoot(completion.image,session.inputs.full26Inputs,
        session.root,session.inputs.auditExpectation,completion.admittedForSession(session),tag,repositoryEpochAtSubmission)
    /** Main 只借已由原 worker 解码的不可变值；这里不进入 Session/native/SQL。 */
    private[mysql] def immutableSnapshot:MySqlOwnedFormat3Snapshot = synchronized {
      if !initialized || inputRetired || immutableInput==null then
        throw new IllegalStateException("original immutable schema5 publication is unavailable")
      immutableInput
    }
    private[MySqlDeploymentReceiptSession] def retireImmutableInput(actual:MySqlDeploymentReceiptSession):Unit = synchronized {
      actual.requireOwner()
      if (actual ne session) || (actual.publication ne this) || inputRetired || immutableInput==null then
        throw new IllegalStateException("original schema5 immutable input retirement required once")
      val originalRoot=immutableInput.detachOriginalBootInputForExit(completion.image,tag)
      if originalRoot ne session.root then throw new IllegalStateException("schema5 immutable input lost original root")
      immutableInput=null
      inputRetired=true
    }
    private[mysql] def requireCurrent():Unit =
      session.requireOwner();completion.requireFor(session.actor)
      if (session.publication ne this) || (session.publicationLease ne lease) then throw new IllegalStateException("original schema5 publication lease required")
    private[mysql] def identity:MySqlOwnedIoIdentity={requireCurrent();session.identity}
    private[mysql] def image:MySqlScriptSchemaSnapshot={requireCurrent();completion.image}
    private[mysql] def root:MySqlFormat3ReadBudget={requireCurrent();session.root}
    private[mysql] def profile:com.wi.floatingpod.config.SourceLimitProfile={requireCurrent();session.inputs.sourceLimitProfile}
    private[mysql] def phase:MySqlProductionBootReply.DeploymentReceiptPhase =
      requireCurrent()
      if completion.isFreshPreparation then MySqlProductionBootReply.DeploymentReceiptPhase.FreshCertifiedPreparation
      else MySqlProductionBootReply.DeploymentReceiptPhase.OrdinaryAdmitted
    private[mysql] def binding:java.util.UUID={requireCurrent();session.inputs.auditExpectation.bindingUuid}
    private[mysql] def epoch:Long={requireCurrent();completion.writerEpoch}
    private[mysql] def normalRecallSourceFacts:Option[MySqlNormalRecallSourceIdentity] =
      requireCurrent()
      if completion.isFreshPreparation then None
      else session.withNative(session.actor) { native=>
        native.withSource { source=>
          val pins=session.original.getOrElse(throw new SQLDataException("Ordinary schema5 publication lacks original source pins"))
          source.validateLive()
          if source.sourceWorldIdentity()!=pins.sourceWorldIdentity() ||
              !source.currentOwner().boot().equals(session.identity.bootId) ||
              source.currentOwner().generation()!=session.identity.bootGeneration then
            throw new SQLDataException("Ordinary schema5 recall source differs from original actual native source/boot")
          session.root.reserveRetainedMemory(256L,"actual schema5 immutable recall source facts before publication")
          Some(MySqlNormalRecallSourceIdentity(source.sourceWorldIdentity(),source.sourceWorldIdentity()))
        }
      }
  final class OrdinaryPostAudit5 private[MySqlDeploymentReceiptSession] (
      private[MySqlDeploymentReceiptSession] val session:MySqlDeploymentReceiptSession,
      private[MySqlDeploymentReceiptSession] val token:MySqlWriterFenceToken,
      private[MySqlDeploymentReceiptSession] val receipt:RuntimeSchemaAuditReceipt
  )
  /** 只能由原 HANDOFF 已提交/读回/确认及 post-audit 发行，不携 business admission。 */
  final class RestrictedRecall5 private[MySqlDeploymentReceiptSession] (
      session:MySqlDeploymentReceiptSession,actor:MySqlDeploymentReceiptBootstrapActor,
      plan:MySqlDeploymentReceiptBootstrapActor.Planned5,
      ack:com.wi.floatingpod.server.WindowsB1V6WitnessRecordStore.DurableV6Ack,
      post:OrdinaryPostAudit5,coordinator:MySqlWriterFenceCoordinator):
    private var pendingRenewal:MySqlWriterFenceMutation=null
    private var token=post.token
    private var receipt=post.receipt
    private[mysql] def withCurrent[A](expectation:MySqlSchemaAuditExpectation,actual:MySqlWriterFenceCoordinator)
        (use:(VerifiedMySqlConnection,MySqlAuditBootScope,MySqlWriterFenceToken,RuntimeSchemaAuditReceipt)=>A):A =
      session.requireActor(actor)
      if (session.restrictedRecallProof ne this) || (session.coordinator ne actual) || (coordinator ne actual) ||
          (session.inputs.auditExpectation ne expectation) || session.restrictedRecall.isEmpty ||
          session.admission!=null || session.quiescing || session.primary!=null || pendingRenewal!=null then
        throw new IllegalStateException("original live schema5 restricted Recall keeper required",session.primary)
      actor.requireRestrictedRecallHandoff(plan,ack)
      use(session.verified,session.scope,token,receipt)
    private[mysql] def requireBootSource(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
        root:MySqlFormat3ReadBudget,expectation:MySqlSchemaAuditExpectation):Unit =
      withCurrent(expectation,coordinator) { (_,scope,currentToken,_)=>
        val original=session.restrictedRecall.get
        if (snapshot ne original._2) || (inputs ne session.inputs.full26Inputs) || (root ne session.root) ||
            snapshot.schema!=expectation.schema || snapshot.connection.serverUuid!=expectation.serverUuid ||
            snapshot.connection.catalog!=expectation.schema || snapshot.observationScope.bootUuid!=scope.bootUuid ||
            snapshot.observationScope.bootGeneration!=scope.bootGeneration ||
            snapshot.observationScope.connectionGeneration!=scope.connectionGeneration ||
            original._3.bindingUuid!=currentToken.bindingUuid || original._3.writerEpoch!=currentToken.writerEpoch ||
            !original._3.ownerBootUuid.contains(scope.bootUuid) || original._3.leaseUntil!=currentToken.leaseUntil then
          throw new SQLDataException("restricted schema5 claimed Boot source differs from original HANDOFF/root/session")
        root.requireScriptDataInputs(inputs)
      }
    private[mysql] def retainPendingRenewal(actual:MySqlWriterFenceCoordinator,mutation:MySqlWriterFenceMutation):Unit =
      session.requireOwner()
      if (actual ne coordinator) || mutation==null || pendingRenewal!=null then
        throw new IllegalStateException("original restricted renewal observation slot required")
      pendingRenewal=mutation
    private[mysql] def retainRenewed(actual:MySqlWriterFenceCoordinator,
        nextToken:MySqlWriterFenceToken,nextReceipt:RuntimeSchemaAuditReceipt):Unit =
      if (actual ne coordinator) || nextToken==null || nextReceipt==null then
        throw new IllegalStateException("original restricted schema5 renewal result required")
      token=nextToken;receipt=nextReceipt;pendingRenewal=null

  final class OrdinaryAdoption5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val actor:MySqlDeploymentReceiptBootstrapActor,
      private val completion:MySqlDeploymentReceiptBootstrapActor.Completion5,private val post:OrdinaryPostAudit5,
      private val coordinator:MySqlWriterFenceCoordinator
  ):
    private var consumed=false
    private[mysql] def consume(expectation:MySqlSchemaAuditExpectation,actual:MySqlWriterFenceCoordinator)
        (use:(VerifiedMySqlConnection,MySqlAuditBootScope,MySqlWriterFenceToken,RuntimeSchemaAuditReceipt)=>MySqlBusinessAdmission):MySqlBusinessAdmission =
      session.requireActor(actor);completion.requireBootstrapAdoption(actor)
      if consumed || (actual ne coordinator) || (session.coordinator ne actual) ||
          (session.ordinaryAdoption ne this) || (session.inputs.auditExpectation ne expectation) ||
          (session.ordinaryPostAudit ne post) || (post.session ne session) then
        throw new IllegalStateException("exact one-shot schema5 settled native/post-audit adoption required")
      consumed=true
      use(session.verified,session.scope,post.token,post.receipt)

  /** Session 私有签发的一次 restricted Full26 锁读能力；所有出口仅回滚。 */
  final class RestrictedSourceFreshWriteScope5 private[MySqlDeploymentReceiptSession] (
      private[MySqlDeploymentReceiptSession] val session:MySqlDeploymentReceiptSession,
      private[MySqlDeploymentReceiptSession] val actor:MySqlDeploymentReceiptBootstrapActor,
      private[MySqlDeploymentReceiptSession] val verified:VerifiedMySqlConnection,
      private[MySqlDeploymentReceiptSession] val auditScope:MySqlAuditBootScope,
      private[MySqlDeploymentReceiptSession] val runner:MySqlFencedTransaction,
      private[MySqlDeploymentReceiptSession] val proof:RestrictedRecall5,
      private[MySqlDeploymentReceiptSession] val publication:RestrictedPublication5,
      private[MySqlDeploymentReceiptSession] val link:MySqlOwnedIoAdapter.RecallRecoveryLink,
      private[MySqlDeploymentReceiptSession] val readerLease:RestrictedSourceReadLease5,
      private[MySqlDeploymentReceiptSession] val root:MySqlFormat3ReadBudget,
      private[MySqlDeploymentReceiptSession] val ledger:MySqlFormat3AllocationLedger,
      private[mysql] val plan:MySqlFencedTransaction.SourceReservePlan5,
      private[MySqlDeploymentReceiptSession] val expectedContext:MySqlRepositoryOperationContextIdentity,
      private[MySqlDeploymentReceiptSession] val reader:MySqlDeploymentReceiptReadOwner,
      private[MySqlDeploymentReceiptSession] val targetReader:MySqlDeploymentReceiptReadOwner,
      private[MySqlDeploymentReceiptSession] val dataBudget:MySqlFormat3ReadBudget,
      private[mysql] val rollbackOnly:MySqlRollbackOnlyState,
      private[MySqlDeploymentReceiptSession] val originalIsolation:Int
  ) extends LockedReadScope5:
    require(session!=null && actor!=null && verified!=null && auditScope!=null && runner!=null && proof!=null &&
      publication!=null && link!=null && readerLease!=null && root!=null && ledger!=null && plan!=null &&
      expectedContext!=null && reader!=null && targetReader!=null && (reader ne targetReader) &&
      dataBudget!=null && rollbackOnly!=null,
      "complete original restricted fresh-write Session scope with distinct before/target readers required")
    private[MySqlDeploymentReceiptSession] var gateOwner:MySqlConnectionTransactionGate.Owner=null
    private[MySqlDeploymentReceiptSession] var settled=false
    private[mysql] def gate:MySqlConnectionTransactionGate.Owner=gateOwner
    private[mysql] def isSettled:Boolean=settled
    private[mysql] var captureWorkspacePrecharged=false
    private[mysql] var targetCaptureWorkspacePrecharged=false
    private[mysql] def expectedFixedLeaseUntil:LocalDateTime=expectedContext.fixedLeaseUntil
    private[mysql] def expectedContextTagBootId=expectedContext.tag.bootId
    private[mysql] def retainedDataBudget:MySqlFormat3ReadBudget=dataBudget
    private val schemaUtf8Bytes=Math.min(session.inputs.auditExpectation.schema.length.toLong,64L)
    private val namespaceTextBytes=Math.addExact(47L,schemaUtf8Bytes)
    private val namespaceCanonicalBytes=Math.addExact(60L,namespaceTextBytes)
    private val namespaceBufferAndCopies=Math.addExact(Math.multiplyExact(2L,namespaceCanonicalBytes),128L)
    private val namespaceDigestAndNames=Math.addExact(32L,Math.addExact(43L,Math.addExact(43L,48L)))
    private[MySqlDeploymentReceiptSession] val namespaceTemporaryCharge=Math.addExact(512L,Math.addExact(namespaceBufferAndCopies,
      Math.addExact(namespaceTextBytes,namespaceDigestAndNames)))
    private var staleFailure:Throwable=null
    private var firstFailure:Throwable=null
    private var validatedSnapshot:MySqlScriptSchemaSnapshot=null
    private var validatedWriter:MySqlBusinessWriterRow=null
    private var verifiedTargetSnapshot:MySqlScriptSchemaSnapshot=null
    private var verifiedTargetWriter:MySqlBusinessWriterRow=null
    private var targetCaptureAttempted=false
    private var targetCaptureReturned=false
    private var dmlAttempted=false
    private[mysql] var dmlReturned=false
    private[MySqlDeploymentReceiptSession] var commitAttempted=false
    private[MySqlDeploymentReceiptSession] var commitAcknowledged=false
    private[MySqlDeploymentReceiptSession] var commitUnknown:MySqlCommitUnknownException=null
    private[MySqlDeploymentReceiptSession] var committedCleanupFailure:Throwable=null
    private[MySqlDeploymentReceiptSession] var commitUnknownCleanupFailure:Throwable=null
    private[mysql] def commitWasAttempted:Boolean=commitAttempted
    private[mysql] def commitWasAcknowledged:Boolean=commitAcknowledged
    private[mysql] def commitUnknownCause:MySqlCommitUnknownException=commitUnknown
    private var freshFull26ConsumerClaimed=false
    private[MySqlDeploymentReceiptSession] var gateEntryAttempted=false
    private[MySqlDeploymentReceiptSession] var gateEntryReturned=false
    private[MySqlDeploymentReceiptSession] var gateEntrySucceeded=false
    private[MySqlDeploymentReceiptSession] var gateEntryRejected=false
    private[MySqlDeploymentReceiptSession] var gateOwnerReturned=false
    private[MySqlDeploymentReceiptSession] var transactionAttempted=false
    private[MySqlDeploymentReceiptSession] var isolationChangeAttempted=false
    private[MySqlDeploymentReceiptSession] var isolationChangeReturned=false
    private[MySqlDeploymentReceiptSession] var autoCommitChangeAttempted=false
    private[MySqlDeploymentReceiptSession] var autoCommitChangeReturned=false
    private[MySqlDeploymentReceiptSession] var captureAttempted=false
    private[MySqlDeploymentReceiptSession] var captureReturned=false
    private[MySqlDeploymentReceiptSession] var consumerAttempted=false
    private[MySqlDeploymentReceiptSession] var consumerReturned=false
    private[MySqlDeploymentReceiptSession] var rollbackAttempted=false
    private[MySqlDeploymentReceiptSession] var rollbackReturned=false
    private[MySqlDeploymentReceiptSession] var autoCommitRestoreAttempted=false
    private[MySqlDeploymentReceiptSession] var autoCommitRestoreCallReturned=false
    private[MySqlDeploymentReceiptSession] var autoCommitRestoreReturned=false
    private[MySqlDeploymentReceiptSession] var isolationRestoreAttempted=false
    private[MySqlDeploymentReceiptSession] var isolationRestoreCallReturned=false
    private[MySqlDeploymentReceiptSession] var isolationRestoreReturned=false
    private[MySqlDeploymentReceiptSession] var gateLeaveAttempted=false
    private[MySqlDeploymentReceiptSession] var gateLeaveReturned=false
    private[MySqlDeploymentReceiptSession] var captureWorkspaceRefundAttempted=false
    private[MySqlDeploymentReceiptSession] var captureWorkspaceRefundReturned=false
    private[MySqlDeploymentReceiptSession] var scopeHolderRefundAttempted=false
    private[MySqlDeploymentReceiptSession] var scopeHolderRefundReturned=false
    private[MySqlDeploymentReceiptSession] var readerRetirementReturned=false
    private[mysql] def isBoundReader(actual:MySqlDeploymentReceiptReadOwner):Boolean = (reader eq actual)
    private[mysql] def isBoundTargetReader(actual:MySqlDeploymentReceiptReadOwner):Boolean = (targetReader eq actual)
    private[mysql] def readerRetirementComplete:Boolean = readerRetirementReturned

    private[mysql] def requireLockedBefore(actual:MySqlScriptSchemaSnapshot,
        actualWriter:MySqlBusinessWriterRow):Unit =
      if actual==null || actualWriter==null || validatedSnapshot==null || validatedWriter==null ||
          !validatedSnapshot.exactlyEquals(actual) || validatedWriter!=actualWriter ||
          !plan.exactBefore.exactlyEquals(actual) then
        throw new SQLDataException("restricted SOURCE R0 DML requires the exact one-shot locked B and its original W")

    private[mysql] def prechargeTargetCaptureWorkspace():Unit =
      requireFor(session,actor,verified)
      if !dmlReturned || targetCaptureAttempted || targetCaptureWorkspacePrecharged then
        throw new IllegalStateException("restricted SOURCE R0 target capture is allowed once after returned DML",firstFailure)
      root.reserveTemporaryMemory(8192L,"restricted SOURCE R0 independent Full26 target capture workspace BEFORE allocation")
      targetCaptureWorkspacePrecharged=true

    private[mysql] def validateFreshTarget(expected:MySqlScriptSchemaSnapshot):Unit =
      if !dmlReturned || !targetCaptureWorkspacePrecharged || targetCaptureAttempted ||
          expected==null || verifiedTargetSnapshot!=null || settled then
        throw new IllegalStateException("one registered post-DML SOURCE R0 target capture and exact target are required",firstFailure)
      targetCaptureAttempted=true
      targetReader.captureRestrictedSourceLockedFull26(this)
      targetCaptureReturned=true
      targetReader.withLockedImage(actor,session.inputs) { (actual,writer) =>
        requireFor(session,actor,verified)
        requireCurrentFence()
        if !expected.exactlyEquals(actual) then
          rejectStale(new SQLDataException("restricted SOURCE R0 post-DML Full26 differs field-for-field/canonically from the A-built expected target"))
        verifiedTargetSnapshot=actual
        verifiedTargetWriter=writer
      }

    private[mysql] def requireVerifiedTarget():Unit =
      if !dmlReturned || !targetCaptureReturned || verifiedTargetSnapshot==null || verifiedTargetWriter==null ||
          !targetCaptureWorkspacePrecharged then
        throw new IllegalStateException("exact registered post-DML Full26 target verification is required before commit",firstFailure)
      requireCurrentFence()

    private[mysql] def hasVerifiedTarget:Boolean =
      dmlReturned && targetCaptureReturned && verifiedTargetSnapshot!=null && verifiedTargetWriter!=null

    private[mysql] def markDmlAttempted():Unit =
      requireLockedBefore(validatedSnapshot,validatedWriter)
      if dmlAttempted || commitAttempted then
        throw new IllegalStateException("restricted SOURCE R0 DML has one attempt before commit",firstFailure)
      dmlAttempted=true

    private[mysql] def markDmlReturned():Unit =
      if !dmlAttempted || dmlReturned || commitAttempted then
        throw new IllegalStateException("restricted SOURCE R0 DML return must follow its unique attempt",firstFailure)
      dmlReturned=true

    private[mysql] def markCommitAttempted():Unit =
      if !dmlReturned || !targetCaptureReturned || verifiedTargetSnapshot==null || verifiedTargetWriter==null ||
          commitAttempted || commitAcknowledged || commitUnknown!=null then
        throw new IllegalStateException("restricted SOURCE R0 commit attempt follows verified DML exactly once",firstFailure)
      rollbackOnly.cause.foreach(throw _)
      commitAttempted=true

    private[mysql] def markCommitAcknowledged():Unit =
      if !commitAttempted || commitAcknowledged || commitUnknown!=null then
        val invariant=new IllegalStateException(
          "JDBC commit returned for restricted SOURCE R0 despite an inconsistent attempt/ACK/UNKNOWN marker",firstFailure)
        commitAcknowledged=true // the actual JDBC return is authoritative even if its bookkeeping invariant failed.
        if committedCleanupFailure==null then committedCleanupFailure=invariant
        else MySqlOwnedIoAdapter.suppressExitCleanup(committedCleanupFailure,invariant)
      else commitAcknowledged=true

    private[mysql] def markCommitUnknown(actual:MySqlCommitUnknownException):Unit =
      if !commitAttempted || commitAcknowledged || actual==null || commitUnknown!=null then
        throw new IllegalStateException("restricted SOURCE R0 UNKNOWN requires one attempted commit without returned ACK",firstFailure)
      commitUnknown=actual

    private[mysql] def markCommittedCleanupFailure(actual:Throwable):Unit =
      if !commitAcknowledged || actual==null then
        throw new IllegalStateException("post-ACK cleanup failure must retain DB-committed truth",firstFailure)
      if committedCleanupFailure==null then committedCleanupFailure=actual
      else if committedCleanupFailure ne actual then MySqlOwnedIoAdapter.suppressExitCleanup(committedCleanupFailure,actual)

    private[mysql] def markCommitUnknownCleanupFailure(actual:Throwable):Unit =
      if commitUnknown==null || actual==null then
        throw new IllegalStateException("commit uncertainty cleanup must remain attached to the original UNKNOWN",firstFailure)
      MySqlOwnedIoAdapter.suppressExitCleanup(commitUnknown,actual)
      if commitUnknownCleanupFailure==null then commitUnknownCleanupFailure=actual
      else if commitUnknownCleanupFailure ne actual then MySqlOwnedIoAdapter.suppressExitCleanup(commitUnknownCleanupFailure,actual)

    private[mysql] def requireFor(actualSession:MySqlDeploymentReceiptSession,
        actualActor:MySqlDeploymentReceiptBootstrapActor,actualVerified:VerifiedMySqlConnection):Unit =
      if settled || (actualSession ne session) || (actualActor ne actor) || (actualVerified ne verified) then
        throw new IllegalStateException("exact active restricted fresh-write Session scope required",firstFailure)
      session.requireRestrictedSourceFreshWriteActive(this)

    /** 每次 readmit 前先向原 DATA 子预算登记 namespace 编码、哈希与副本空间。 */
    private[mysql] def readmitCurrentBarrier():Unit =
      session.requireRestrictedSourceFreshWriteActive(this)
      dataBudget.reserveTemporaryMemory(namespaceTemporaryCharge,
        "restricted SessionBarrierNamespace/hash workspace BEFORE active-scope readmit")
      var primary:Throwable=null
      try
        val actualBarrier=session.barrier
        if actualBarrier==null then
          rejectStale(new SQLDataException("restricted fresh-write Session held barrier is absent"))
        actualBarrier.readmit(verified,session.inputs.auditExpectation,auditScope)
        if actualBarrier.connectionGenerationId!=expectedContext.barrierGenerationUuid ||
            actualBarrier.actualConnectionId!=expectedContext.actualConnectionId || actualBarrier.namespace==null ||
            actualBarrier.namespace.lockName()!=expectedContext.barrierNamespace then
          rejectStale(new SQLDataException("restricted fresh-write held Session barrier differs from original DATA context"))
      catch
        case cause:Throwable =>
          primary=cause
          throw cause
      finally
        try dataBudget.releaseTemporaryMemory(namespaceTemporaryCharge,
          "restricted SessionBarrierNamespace/hash workspace after active-scope readmit")
        catch
          case cleanup:Throwable=>
            barrierCleanupFailureObserved=true
            if primary==null then throw cleanup
            MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)
            throw primary

    /** 在借用 gate 仍持有时仅捕获并完整比较一次。 */
    private[MySqlDeploymentReceiptSession] def validateFreshFull26():Unit =
      if !captureReturned || validatedSnapshot!=null || settled then
        throw new IllegalStateException("one captured restricted fresh Full26 validation is required",firstFailure)
      reader.withLockedImage(actor,session.inputs) { (actual,writer) =>
        requireFor(session,actor,verified)
        if !plan.exactBefore.exactlyEquals(actual) then
          rejectStale(new SQLDataException("restricted fresh Full26 differs field-for-field/canonically from the exact retained before"))
        session.requireRestrictedSourceFreshWriter(this,writer)
        validatedSnapshot=actual
        validatedWriter=writer
      }

    /** Unit consumer 可在同一 scope 内检查一次精确重读值；不返回 Connection 或命令。 */
    private[mysql] def withFreshFull26(
        consume:(MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow)=>Unit):Unit =
      if consume==null || validatedSnapshot==null || validatedWriter==null || freshFull26ConsumerClaimed || settled then
        throw new IllegalStateException("one exact validated restricted fresh Full26 consumer is required",firstFailure)
      requireCurrentFence()
      freshFull26ConsumerClaimed=true
      consume(validatedSnapshot,validatedWriter)

    private[mysql] def requireCurrentFence():Unit =
      readmitCurrentBarrier()
      requireFor(session,actor,verified)
      if validatedWriter!=null then session.requireRestrictedSourceFreshWriter(this,validatedWriter)

    private[MySqlDeploymentReceiptSession] def rejectStale(cause:Throwable):Nothing =
      if cause==null then throw new IllegalArgumentException("restricted fresh-write stale cause is required")
      if staleFailure==null then staleFailure=cause
      else if staleFailure ne cause then MySqlOwnedIoAdapter.suppressExitCleanup(staleFailure,cause)
      throw cause

    private[MySqlDeploymentReceiptSession] def rethrowStaleIfRecorded():Unit =
      if staleFailure!=null then throw staleFailure

    private[mysql] def isStaleRejection(cause:Throwable):Boolean =
      staleFailure!=null && (cause eq staleFailure)
    private[MySqlDeploymentReceiptSession] var barrierCleanupFailureObserved=false

    private[MySqlDeploymentReceiptSession] def markFailure(cause:Throwable):Unit =
      if cause==null then throw new IllegalArgumentException("restricted fresh-write original cause is required")
      if firstFailure==null then firstFailure=cause
      else if firstFailure ne cause then MySqlOwnedIoAdapter.suppressExitCleanup(firstFailure,cause)
      rollbackOnly.mark(cause)

    private[MySqlDeploymentReceiptSession] def failure:Option[Throwable]=Option(firstFailure)

    private[MySqlDeploymentReceiptSession] def markSettled():Unit =
      if settled || !transactionAttempted || !gateOwnerReturned || !rollbackReturned ||
          !autoCommitRestoreReturned || !isolationRestoreReturned || !gateLeaveReturned then
        throw new IllegalStateException("restricted fresh-write rollback/restoration/gate settlement stages are incomplete",firstFailure)
      settled=true

    private[MySqlDeploymentReceiptSession] def markSettledAfterCommit():Unit =
      if settled || !commitAttempted || !commitAcknowledged || commitUnknown!=null || !transactionAttempted ||
          !gateOwnerReturned || !autoCommitRestoreReturned || !isolationRestoreReturned || !gateLeaveReturned then
        throw new IllegalStateException("ACKed restricted SOURCE R0 commit/restoration/gate settlement stages are incomplete",firstFailure)
      settled=true

    private[MySqlDeploymentReceiptSession] def markCaptureWorkspacePrecharged():Unit =
      if captureWorkspacePrecharged || settled then
        throw new IllegalStateException("one restricted fresh-write capture workspace precharge is required",firstFailure)
      captureWorkspacePrecharged=true

    private[MySqlDeploymentReceiptSession] def markSettledWithoutTransaction():Unit =
      if settled || transactionAttempted || gateEntrySucceeded || gateOwnerReturned || rollbackAttempted ||
          autoCommitRestoreAttempted || isolationRestoreAttempted || gateLeaveAttempted ||
          (gateEntryAttempted && !gateEntryRejected) then
        throw new IllegalStateException("restricted fresh-write no-gate settlement requires an unstarted transaction",firstFailure)
      settled=true

  private[mysql] sealed trait LockedReadScope5:
    private[mysql] def rollbackOnly:MySqlRollbackOnlyState
    private[mysql] def gate:MySqlConnectionTransactionGate.Owner
    private[mysql] def isSettled:Boolean
    private[mysql] def requireFor(actualSession:MySqlDeploymentReceiptSession,
      actualActor:MySqlDeploymentReceiptBootstrapActor,actualVerified:VerifiedMySqlConnection):Unit

  final class CriticalWriteScope private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val actor:MySqlDeploymentReceiptBootstrapActor,
      private val verified:VerifiedMySqlConnection,
      private[mysql] val rollbackOnly:MySqlRollbackOnlyState,private[mysql] val gate:MySqlConnectionTransactionGate.Owner) extends LockedReadScope5:
    private[MySqlDeploymentReceiptSession] var settled=false
    private[mysql] def isSettled:Boolean=settled
    private[mysql] def requireFor(actualSession:MySqlDeploymentReceiptSession,actualActor:MySqlDeploymentReceiptBootstrapActor,
        actualVerified:VerifiedMySqlConnection):Unit =
      if settled || (actualSession ne session)||(actualActor ne actor)||(actualVerified ne verified)|| (session.critical ne this) then
        throw new IllegalStateException("exact live schema5 critical transaction required")
      session.requireActor(actualActor)
      MySqlConnectionTransactionGate.requireCurrent(gate,verified.connection,rollbackOnly)

  /** 借用原 Fenced planning 交易；没有独立 transaction/commit 权限。 */
  final class PurposeReadScope5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val actor:MySqlDeploymentReceiptBootstrapActor,
      private val verified:VerifiedMySqlConnection,private val planning:MySqlFencedTransaction.DeploymentReceiptPurposeReadOwner5,
      private val originalRoot:MySqlFormat3ReadBudget,
      private[mysql] val rollbackOnly:MySqlRollbackOnlyState,private[mysql] val gate:MySqlConnectionTransactionGate.Owner) extends LockedReadScope5:
    private[MySqlDeploymentReceiptSession] var settled=false
    private[mysql] def isSettled:Boolean=settled
    private[mysql] def requireFor(actualSession:MySqlDeploymentReceiptSession,actualActor:MySqlDeploymentReceiptBootstrapActor,
        actualVerified:VerifiedMySqlConnection):Unit =
      if settled || (actualSession ne session) || (actualActor ne actor) || (actualVerified ne verified) then
        throw new IllegalStateException("exact live schema5 purpose planning transaction required")
      session.requireActor(actor)
      planning.requireDeploymentReceiptSession(session,verified,session.scope,originalRoot)
      MySqlConnectionTransactionGate.requireCurrent(gate,verified.connection,rollbackOnly)

  /** 原actor的受控handoff；不发行写交易/ACK/Ready，也不开放Connection或给Main的graph getter。 */
  final class FreshCapture private[MySqlDeploymentReceiptSession] (
      session: MySqlDeploymentReceiptSession, actor: MySqlDeploymentReceiptBootstrapActor):
    private def requireLive(): FreshRequest =
      session.requireActor(actor)
      val request=session.fresh.getOrElse(throw new IllegalStateException("original Fresh request missing"))
      session.inputs.requirePlanData(request.plan)
      session.root.requireScriptDataInputs(session.inputs.full26Inputs)
      request
    private[mysql] def withNativeState[A](consume: (
        WindowsWorldProvisioner.DeploymentReceiptWorld,
        com.wi.floatingpod.server.WindowsB1SourceAnchorStore.DurableB1SourceAnchor) => A): A =
      val request=requireLive()
      val world=session.nativeOwner.retainedWorld.getOrElse(throw new IllegalStateException("original native5 world missing"))
      val anchor=session.nativeOwner.retainedAnchor.getOrElse(throw new IllegalStateException("original native5 source ACK missing"))
      if world.creationProof().plan() ne request.plan then throw new IllegalStateException("Fresh native proof differs from original request")
      consume(world,anchor)
    private[mysql] def withLockedBefore[A](consume: MySqlScriptSchemaSnapshot => A): A =
      val request=requireLive()
      session.readOwner.withCurrentConfiguration(actor,session.inputs,request.plan)(consume)
    private[mysql] def settleReadTransaction(): Unit =
      requireLive()
      session.readOwner.settleSuccessfulRead(actor)
    private[mysql] def withRetainedBefore[A](consume: (
        MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow,MySqlScriptSchemaInputs,MySqlFormat3ReadBudget) => A): A =
      requireLive()
      session.readOwner.withRetainedBefore(actor) { (before,writer) =>
        consume(before,writer,session.inputs.full26Inputs,session.root)
      }
    private[mysql] def retireBeforeAfterLastConsumer(): Unit =
      requireLive()
      session.readOwner.retireSnapshot(actor)

  final case class FreshRequest(worldPath: Path, plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      boot: WindowsWorldProvisioner.BootIdentity, controlled: MySqlFreshControlledMetadata,
      writerLease:Duration,submissionTag:IoOperationTag):
    require(worldPath != null && worldPath.isAbsolute && worldPath.getParent != null && worldPath.getFileName != null,
      "explicit absolute schema5 world path required")
    require(plan != null && plan.schemaFormat() == 5 && plan.physicalFormat() == 2 && boot != null && controlled != null,
      "complete schema5 PlanData/current boot/controlled metadata required")
    require(!controlled.metadataPath.normalize().startsWith(worldPath.normalize()), "controlled schema5 metadata must be outside world")
    require(writerLease!=null && !writerLease.isZero && !writerLease.isNegative&&writerLease.toNanos<=MySqlSchemaAuditor.MaxReceiptAgeNanos&&
      writerLease.toNanos%1000L==0L&&submissionTag!=null,"actual bounded DATETIME6 lease and original submission tag required")

  /** 后续只由本 Session 的实际终止检查发行；DATA/Boolean 无法构造。 */
  private enum TerminalMainDrop5:
    case Fresh(receipt:MySqlOwnedIoAdapter.FreshCompletionHandoff)
    case Ordinary(receipt:com.wi.floatingpod.network.DedicatedProjectionRuntime.MainConsumersEnded)
  final class TerminalIntent5 private[MySqlDeploymentReceiptSession] (
      private[MySqlDeploymentReceiptSession] val session:MySqlDeploymentReceiptSession,
      private[MySqlDeploymentReceiptSession] val mainDrop:TerminalMainDrop5,
      private[MySqlDeploymentReceiptSession] val issuer:Option[MySqlOwnedRetainedPlanIssuer],
      private[MySqlDeploymentReceiptSession] val ledger:MySqlFormat3AllocationLedger,
      private[MySqlDeploymentReceiptSession] val tag:IoOperationTag,
      private[MySqlDeploymentReceiptSession] val repositoryEpoch:Long)

  final class NormalTerminalSettlement5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val identity:MySqlOwnedIoIdentity,
      private val root:MySqlFormat3ReadBudget,private val intent:TerminalIntent5):
    private[mysql] def requireFor(actual:MySqlDeploymentReceiptSession,actualRoot:MySqlFormat3ReadBudget,
        actualIdentity:MySqlOwnedIoIdentity):Unit =
      if actual ne session then throw new IllegalStateException("original schema5 settled Session required")
      session.requireNormalSettlement(this,actualRoot,actualIdentity)

  /** 只由原 Session 在实际 SQL writer/barrier 结算返回后发行；不接受 DATA/Boolean。 */
  final class NativeRetirementPermit5 private[MySqlDeploymentReceiptSession] (
      private val session:MySqlDeploymentReceiptSession,private val intent:TerminalIntent5,
      private val owner:MySqlDeploymentReceiptFreshNativeOwner,
      private var source:WindowsWorldProvisioner.DeploymentReceiptIoScope,
      private var io:com.wi.floatingpod.server.WindowsMarkerIo):
    private var nativeClosed=false
    private[mysql] def requireOwner(actual:MySqlDeploymentReceiptFreshNativeOwner):Unit =
      session.requireNativeRetirementPermit(this,actual)
      if nativeClosed || (actual ne owner) || (session.terminalIntent ne intent) then throw new IllegalStateException("exact original schema5 native retirement owner required")
    def requireNativeIdentity(actualSource:WindowsWorldProvisioner.DeploymentReceiptIoScope,
        actualIo:com.wi.floatingpod.server.WindowsMarkerIo):Unit =
      requireOwner(owner)
      if (source ne actualSource) || (io ne actualIo) then throw new IllegalStateException("exact original schema5 native source/IO retirement required")
    private[mysql] def detachNativeIdentityAfterClose(actual:MySqlDeploymentReceiptFreshNativeOwner):Unit =
      requireOwner(actual)
      actual.requireActualNativeCloseReturn(this)
      source=null;io=null;nativeClosed=true

  /** 实际调用/观察，不是正常退出、IOExitProof 或 native 结算资格。 */
  final class FailedInitializationExit private[MySqlDeploymentReceiptSession] (
      val identity: MySqlOwnedIoIdentity, val failure: Throwable, val jdbcClosed: Boolean,
      val nativeResponsibilityRetained: Boolean, val readResponsibilityRetained: Boolean,
      val canRetireStaticAndRoot: Boolean
  )

  private[mysql] def allocateFresh(inputs: MySqlDeploymentReceiptSessionInputs, exactRoot: MySqlFormat3ReadBudget,
      actualIoIdentity: MySqlOwnedIoIdentity, request: FreshRequest,
      auditMaxAge: Duration): MySqlDeploymentReceiptSession =
    require(request != null && actualIoIdentity != null && request.boot.bootId() == actualIoIdentity.bootId &&
      request.boot.generation() == actualIoIdentity.bootGeneration, "Fresh current boot differs from actual IO identity")
    actualIoIdentity.requireTag(request.submissionTag)
    allocate(inputs, exactRoot, actualIoIdentity, MySqlDeploymentReceiptSessionInputs.Fresh, Some(request), auditMaxAge,
      request.writerLease,request.submissionTag)

  private[mysql] def allocateOrdinary(inputs: MySqlDeploymentReceiptSessionInputs, exactRoot: MySqlFormat3ReadBudget,
      actualIoIdentity: MySqlOwnedIoIdentity, original: ProductionBootstrapControl.OriginalSourcePins,
      auditMaxAge: Duration,current:ProductionBootstrapControl.OrdinaryIdentity,controlled:MySqlFreshControlledMetadata,
      writerLease:Duration,submissionTag:IoOperationTag): MySqlDeploymentReceiptSession =
    require(original != null, "complete original schema5 source DATA pins required; not native custody")
    require(actualIoIdentity!=null&&current!=null&&controlled!=null&&current.boot()==actualIoIdentity.bootId&&
      current.bootGeneration()==actualIoIdentity.bootGeneration&&current.connectionGeneration()==actualIoIdentity.connectionGeneration,
      "original current Ordinary identity/controlled inputs required")
    actualIoIdentity.requireTag(submissionTag)
    require(current.submitOperation()==submissionTag.operationId,"original Ordinary submission operation required")
    allocate(inputs, exactRoot, actualIoIdentity, MySqlDeploymentReceiptSessionInputs.Ordinary, None, auditMaxAge,
      writerLease,submissionTag,Some(original),Some(current),Some(controlled))

  private def allocate(inputs: MySqlDeploymentReceiptSessionInputs, root: MySqlFormat3ReadBudget,
      identity: MySqlOwnedIoIdentity, mode: MySqlDeploymentReceiptSessionInputs.ModeRequest,
      request: Option[FreshRequest], age: Duration,lease:Duration,tag:IoOperationTag,
      original: Option[ProductionBootstrapControl.OriginalSourcePins] = None,
      current:Option[ProductionBootstrapControl.OrdinaryIdentity]=None,controlled:Option[MySqlFreshControlledMetadata]=None): MySqlDeploymentReceiptSession =
    require(inputs != null && root != null && identity != null && age != null && !age.isNegative && !age.isZero &&
      age.toNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos, "exact schema5 inputs/root/IO identity and bounded audit age required")
    inputs.requireForSession(root, identity, mode)
    require(lease!=null && !lease.isZero && !lease.isNegative&&lease.toNanos<=MySqlSchemaAuditor.MaxReceiptAgeNanos&&lease.toNanos%1000L==0L&&tag!=null,
      "actual bounded schema5 writer lease/submission required")
    root.reserveRetainedMemory(2048L, "schema5 Session/actor partial holders BEFORE allocation")
    try new MySqlDeploymentReceiptSession(inputs, root, identity, mode, request, original, age,lease,tag,current,controlled)
    catch
      case failure: Throwable =>
        try root.releaseRetainedMemory(2048L, "unpublished schema5 Session/actor holders")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(failure, cleanup)
        throw failure

  private[mysql] def requireFreshQualification(q: VerifiedSchemaQualification,
      e: MySqlSchemaAuditExpectation, v: VerifiedMySqlConnection, s: MySqlAuditBootScope): Unit =
    if q == null || e == null || v == null || s == null ||
        e.profile != MySqlSchemaAuditProfile.DeploymentReceipt1 || e.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
        !q.isCurrent(s.bootUuid, s.bootGeneration, s.connectionGeneration, e.serverUuid, e.schema,
          e.bindingUuid, e.profile, e.expectedPhysicalFormat, v, s.ownerThread, System.nanoTime()) ||
        q.physicalBinding != MySqlNoPhysicalBinding || q.importState != "NONE" ||
        q.importOperationUuid.nonEmpty || q.importManifestSha256.nonEmpty || q.importSnapshotSha256.nonEmpty || q.importReceiptSha256.nonEmpty then
      throw new IllegalStateException("current exact schema5 READY/noBinding/physical2/noimport audit required before native")
