package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.ProductionBootstrapControl
import java.util.UUID
import com.wi.floatingpod.domain.RecallRequestNamespace
import com.wi.floatingpod.server.io.{IoOperationTag,IoOwnerTransfer}

/** 原Session发行的Full27 Main读责任；不是SQL、实体恢复或Ready资格。 */
private[floatingpod] final class MySqlDbFirstMainReadBacking private[mysql] (
    private val worker:Thread, private var session:MySqlDbFirstSession,
    private var adapter:MySqlOwnedIoAdapter, private var approved:ProductionBootstrapControl.DbFirstControl,
    val acquisitionTag:IoOperationTag, private[mysql] val repositoryEpoch:Long):
  private var dataAllocation:MySqlFormat3ReadBudget=null
  private var projection:MySqlOwnedFormat3Snapshot=null
  private var domainData:MySqlBootMainReadData=null
  @volatile private var sourceAllocation:MySqlDbFirstSourceAllocation=null
  @volatile private var main:Thread=null
  @volatile private var transfer:IoOwnerTransfer=null
  private var dropped=false
  @volatile private var retired=false
  private var failure:Throwable=null
  private var predecessor:MySqlDbFirstMainReadBacking=null
  private var successorObservation:MySqlDbFirstSession.DbFirstRecallRefusalObservation | MySqlDbFirstSession.DbFirstRecallJournalObservation | MySqlDbFirstSession.DbFirstRecallObservation | MySqlDbFirstSession.DbFirstCheckpointObservation | MySqlDbFirstSession.DbFirstInitialActivationObservation | MySqlDbFirstSession.DbFirstInitialPreparationObservation | MySqlDbFirstSession.DbFirstInitialPreparationRejection=null
  private def successorTag:IoOperationTag=
    if successorObservation==null then throw new IllegalStateException("original successor observation missing")
    successorObservation match
      // 拒绝沿原J保留路由槽；observation.tag仍是持久结果原C，不能混为新业务身份。
      case value:MySqlDbFirstSession.DbFirstRecallRefusalObservation=>value.routeTag
      case value:MySqlDbFirstSession.DbFirstRecallJournalObservation=>value.tag
      case value:MySqlDbFirstSession.DbFirstRecallObservation=>value.tag
      case value:MySqlDbFirstSession.DbFirstCheckpointObservation=>value.tag
      case value:MySqlDbFirstSession.DbFirstInitialActivationObservation=>value.tag
      case value:MySqlDbFirstSession.DbFirstInitialPreparationObservation=>value.tag
      case value:MySqlDbFirstSession.DbFirstInitialPreparationRejection=>value.tag
  private var previousRetired=false
  private[mysql] def assemble(source:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):Unit = synchronized {
    require(Thread.currentThread() eq worker,"original schema6 read assembly worker required")
    require(!retired && projection==null && main==null,"original unissued Full27 backing required")
    // 先登记实际child；部分装配失败仍由原worker在退出路径清偿。
    require(dataAllocation==null,"original Main DATA allocation is one-shot")
    dataAllocation=root.newCanonicalDataScope()
    projection=MySqlOwnedFormat3Snapshot.fromDbFirst(source,inputs,root,dataAllocation)
    domainData=MySqlBootMainReadData.fromDbFirst(source,inputs,root,dataAllocation)
    sourceAllocation=new MySqlDbFirstSourceAllocation(root,worker)
  }
  /** 核对独立持有的未发布DATA，不赋予Main读资格或successor确认。 */
  private[mysql] def requireUnpublishedData(actualSession:MySqlDbFirstSession,sha256:String):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && !retired && !dropped &&
      main==null && predecessor==null && successorObservation==null && failure==null &&
      dataAllocation!=null && !dataAllocation.isClosed && projection!=null && domainData!=null &&
      projection.canonicalSha256Hex==sha256,"actual independently owned unpublished Main DATA required")
  }
  /** 仅原Session worker分配未发布successor；保留原acquire身份，不重签owner transfer。 */
  private[mysql] def newUnclaimedSuccessor(actualSession:MySqlDbFirstSession,
      mutationTag:IoOperationTag,nextEpoch:Long):MySqlDbFirstMainReadBacking = synchronized {
    require((Thread.currentThread() eq worker) && !retired && !dropped && (session eq actualSession) &&
      main!=null && transfer!=null && mutationTag!=null && nextEpoch>repositoryEpoch &&
      mutationTag.bootId==acquisitionTag.bootId && mutationTag.bootGeneration==acquisitionTag.bootGeneration,
      "original claimed backing and later accepted mutation epoch required")
    new MySqlDbFirstMainReadBacking(worker,session,adapter,approved,acquisitionTag,nextEpoch)
  }
  /** acquire tag仅保留原bootstrap来源；新SQL代来自实际Session/独立投影，不能重签旧业务操作。 */
  private[mysql] def newUnclaimedServiceSuccessor(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actualSession:MySqlDbFirstSession,actualAdapter:MySqlOwnedIoAdapter,
      epoch:Long):MySqlDbFirstMainReadBacking = synchronized {
    require((Thread.currentThread() eq worker) && source!=null && !retired && !dropped &&
      main!=null && transfer!=null && (adapter eq actualAdapter),"actual original Main backing required")
    source.requireServiceMainReadConstruction(this,actualSession,actualAdapter,epoch)
    new MySqlDbFirstMainReadBacking(worker,actualSession,adapter,approved,acquisitionTag,epoch)
  }
  private[mysql] def requireUnpublishedServiceData(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actualSession:MySqlDbFirstSession,actualIdentity:MySqlOwnedIoIdentity,sha:String):Unit = synchronized {
    requireUnpublishedData(actualSession,sha)
    source.requireActualServiceMainRead(this,actualSession,actualIdentity)
    require(projection.bootUuid==actualIdentity.bootId && projection.bootGeneration==actualIdentity.bootGeneration &&
      projection.connectionGeneration==actualIdentity.connectionGeneration,
      "actual new service DATA must preserve the true independent SQL observation generation")
  }
  private[mysql] def readyForRefusalCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstRecallRefusalObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed refusal successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForJournalCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstRecallJournalObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForRecallCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstRecallObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForCheckpointCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstCheckpointObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForInitialPreparationCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForRejectedPreparation(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null && observation.cause!=null && observation.cause.getSuppressed.isEmpty &&
      projection.canonicalSha256Hex==observation.canonicalSha256,"original assembled rejected successor required")
    predecessor=previous;successorObservation=observation
  }
  private[mysql] def readyForInitialActivationCompletion(actualSession:MySqlDbFirstSession,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstInitialActivationObservation):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main==null &&
      projection!=null && domainData!=null && predecessor==null && successorObservation==null &&
      previous!=null && observation!=null,"original assembled unclaimed successor required")
    predecessor=previous;successorObservation=observation
  }
  /** 只借原初建completion对应的待发布DATA；不把它登记为当前Main读资格。 */
  private[mysql] def initialPublicationData(actualAdapter:MySqlOwnedIoAdapter,
      previous:MySqlDbFirstMainReadBacking,observation:MySqlDbFirstSession.DbFirstInitialActivationObservation):MySqlBootMainReadData = synchronized {
    require(!retired && !dropped && main==null && (adapter eq actualAdapter) && predecessor!=null &&
      (predecessor eq previous) && observation!=null && (successorObservation eq observation) &&
      domainData!=null && projection!=null,"original unclaimed initial successor DATA required")
    if failure!=null then throw failure
    domainData
  }
  private[mysql] def requireSuccessorFor(previous:MySqlDbFirstMainReadBacking,tag:IoOperationTag):Unit = synchronized {
    require(!retired && !dropped && (predecessor eq previous) && successorObservation!=null &&
      successorTag==tag,"original confirmed successor/predecessor/operation required")
  }
  private[mysql] def markPreviousRetired(actualSession:MySqlDbFirstSession,tag:IoOperationTag):Unit = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actualSession) && main!=null &&
      predecessor!=null && successorObservation!=null && successorTag==tag && !previousRetired,
      "original claimed successor and actual worker retirement required")
    predecessor=null;previousRetired=true
  }
  private[mysql] def requireRetiredSuccessor(tag:IoOperationTag):Unit = synchronized {
    current()
    require(previousRetired && predecessor==null && successorObservation!=null && successorTag==tag,
      "original worker-retired current successor required")
  }
  private[mysql] def claimMain(actual:Thread,receipt:IoOwnerTransfer):Unit = synchronized {
    require(actual!=null && ((Thread.currentThread() eq actual) || (Thread.currentThread() eq worker)) &&
      (actual ne worker) && receipt!=null &&
      projection!=null && domainData!=null && sourceAllocation!=null && !dropped && !retired,"original live Full27 backing required")
    adapter.requireRegisteredDbFirstMainRead(this)
    adapter.requireRecordedOwnerClaim(actual,receipt)
    // worker发布与Main claim可并发到达；仅原始同一移交事实允许重复确认。
    require(main==null || ((main eq actual) && (transfer eq receipt)),
      "Full27 backing already belongs to another Main transfer")
    main=actual;transfer=receipt
    sourceAllocation.bindMain(actual)
  }
  /** 只核真实Main claim/transfer事实；不走ordinary current，不授业务或Ready。 */
  private[mysql] def requireRecoveredMainClaim(actualAdapter:MySqlOwnedIoAdapter,
      actualMain:Thread,actualTransfer:IoOwnerTransfer):Unit = synchronized {
    require((Thread.currentThread() eq actualMain) && (adapter eq actualAdapter) &&
      (main eq actualMain) && (transfer eq actualTransfer) && !retired && !dropped &&
      projection!=null && domainData!=null && sourceAllocation!=null,
      "actual recovered Main claim/source backing and original thread transfer required")
    actualAdapter.requireRegisteredDbFirstMainRead(this)
    actualAdapter.requireRecordedOwnerClaim(actualMain,actualTransfer)
  }
  /** 专用控制帧只核实际不可变binding；普通DATA仍由原hold封住。 */
  private[mysql] def recoveredPublicationBinding(actual:MySqlOwnedIoAdapter):com.wi.floatingpod.storage.PhysicalRecoveryBinding = synchronized {
    actual.requireRecoveredPublicationBacking(this)
    domainData.binding.getOrElse(throw new IllegalStateException("actual recovered publication binding missing"))
  }
  /** 原请求严格读回只在本次实际服务收口帧核验，不向普通DATA借用者开放。 */
  private[mysql] def requireRecoveredCommitted(actual:MySqlOwnedIoAdapter,
      original:MySqlDbFirstRecallRequestRecord):Unit = synchronized {
    actual.requireRecoveredCommittedBacking(this)
    require(original!=null && domainData.binding.exists(_.bindingUuid==original.bindingUuid) &&
      domainData.sourceProtection.unresolved.isEmpty && domainData.recallRequestCensus.exists(current=>
        com.wi.floatingpod.server.DbFirstBootRecallRequestCustody.matchesResult(original,current,
          MySqlDbFirstRecallRequestPhase.RecallCommitted,None)),
      "actual current strict original committed recall result required")
  }
  private def owner():Unit =
    require(main!=null && (Thread.currentThread() eq main) && transfer!=null,"exact claimed schema6 Main required")
  private def current():Unit =
    owner()
    require(!dropped && !retired && projection!=null && domainData!=null,"schema6 Main input was dropped or retired")
    if failure!=null then throw failure
    try adapter.requireCurrentDbFirstMainRead(this)
    catch case cause:Throwable=>
      failure=cause
      throw cause
  def relation(table:String):MySqlOwnedFormat3Relation = synchronized {
    current()
    projection.relations.find(_.table==table).getOrElse(throw new IllegalArgumentException("unknown Full27 relation"))
  }
  def relationNames:Vector[String] = synchronized { current();projection.relations.map(_.table) }
  def canonicalSha256:String = synchronized { current();projection.canonicalSha256Hex }
  /** namespace只取同一当前读代的实际binding；actor必须由调用方认证，不能取客户端声明。 */
  private def currentRecallNamespace(authenticatedActor:UUID):RecallRequestNamespace =
    val binding=domainData.binding.getOrElse(throw new IllegalStateException("current schema6 binding absent"))
    RecallRequestNamespace(binding.bindingUuid,authenticatedActor)
  def recallNamespace(authenticatedActor:UUID):RecallRequestNamespace = synchronized {
    current();currentRecallNamespace(authenticatedActor)
  }
  /** 两类显式只读查询不mint请求/J/C、不reserve/freeze或触发SQL。 */
  def lookupRecallClientRequest(authenticatedActor:UUID,requested:RecallRequestNamespace,
      request:UUID):MySqlDbFirstRecallRequestLookup = synchronized {
    current()
    val actual=currentRecallNamespace(authenticatedActor)
    val index=domainData.recallRequests.getOrElse(throw new IllegalStateException("Full27 durable recall index missing"))
    MySqlDbFirstRecallRequestLookup.client(actual,requested,request,index)
  }
  def lookupRecallOriginalOperation(authenticatedActor:UUID,requested:RecallRequestNamespace,
      operation:UUID):MySqlDbFirstRecallRequestLookup = synchronized {
    current()
    val actual=currentRecallNamespace(authenticatedActor)
    val index=domainData.recallRequests.getOrElse(throw new IllegalStateException("Full27 durable recall index missing"))
    MySqlDbFirstRecallRequestLookup.original(actual,requested,operation,index)
  }
  /** 借用的不可变DATA只在本backing未drop期间有效；不含worker query/root。 */
  def data:MySqlBootMainReadData = synchronized { current();domainData }
  private[mysql] def unclaimedRecallCensus(actual:MySqlDbFirstSession):Vector[MySqlDbFirstRecallRequestRecord] = synchronized {
    require((Thread.currentThread() eq worker) && (session eq actual) && main==null && !retired &&
      domainData!=null,"actual initial unclaimed Boot metadata required")
    domainData.recallRequestCensus
  }
  private[mysql] def beginBootTakeoverIntent(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      record:MySqlDbFirstRecallRequestRecord,control:IoOperationTag,execution:IoOperationTag):MySqlDbFirstBootRecallTakeoverIntent = synchronized {
    current();session.beginBootTakeoverIntent(this,owner,record,control,execution)
  }
  private[mysql] def claimBootTakeoverResult(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    current();session.claimBootTakeoverResult(owner,intent,reservation)
  }
  private[mysql] def beginBootMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    current();session.beginBootMainDecision(this,owner,intent,reservation)
  }
  private[mysql] def finishBootMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,
      reason:Option[com.wi.floatingpod.domain.RecallRefusalReason]):MySqlDbFirstBootRecallMainDecision = synchronized {
    current();session.finishBootMainDecision(this,owner,intent,reservation,reason)
  }
  private[mysql] def activateBootRecallCapture(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      decision:MySqlDbFirstBootRecallMainDecision,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    current();session.activateBootRecallCapture(this,owner,decision,reservation)
  }
  private[mysql] def detachBootTakeover(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    ownerThreadForCleanup();session.detachBootTakeover(this,owner,intent,reservation)
  }
  private[mysql] def claimBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Vector[MySqlDbFirstRecallRequestRecord] = synchronized {
    current()
    session.claimBootRecallCensus(this,owner)
  }
  private[mysql] def detachBootRecallCensus(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery):Unit = synchronized {
    ownerThreadForCleanup()
    session.detachBootRecallCensus(owner)
  }
  private def ownerThreadForCleanup():Unit =
    owner();require(!retired && session!=null,"original Boot census Session already retired")

  /** 原Main在同一当前代际内消费完整快照；NBT仅供同步还原，不授予SQL或Ready。 */
  private[floatingpod] def consumeRecoverySnapshot(state:com.wi.floatingpod.domain.FloatingPodState,
      attempt:java.util.UUID,use:net.minecraft.nbt.NBTTagCompound=>Unit):Unit = synchronized {
    current()
    require(domainData.state(state.uuid).contains(state),"exact current committed restore state required")
    val binding=domainData.binding.getOrElse(throw new IllegalStateException("schema6 restore binding missing"))
    require(domainData.activeAttemptFootprints.get(state.uuid).exists(_._1==attempt),
      "exact current active attempt required")
    MySqlDbFirstRecoveryNbt.requireSettled(relation("fp_recovery_operation"),relation("fp_recovery_reservation"),state.uuid)
    MySqlDbFirstRecoveryNbt.consume(relation(MySqlDbFirstSchemaDefinition.SnapshotTable),state,
      binding.bindingUuid,attempt,projection.limitProfile,
      (bytes,purpose)=>sourceAllocation.retainOrdinaryProvider(bytes,purpose),use)
  }
  /** 仅在原读资格有效时领用；已领用source账户的清偿责任独立于DATA drop。 */
  private[floatingpod] def sourceAllocationForMain:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root = synchronized {
    current();require(sourceAllocation!=null,"original schema6 source allocation bridge missing")
    sourceAllocation
  }
  /** 仅当前真实 Main 读资格可申请；释放原费用不依赖后续 epoch freshness。 */
  private[floatingpod] def retainOrdinaryProviderTemporary(bytes:Long,purpose:String):AutoCloseable = synchronized {
    owner()
    require(!retired && sourceAllocation!=null,"original provider allocation responsibility required")
    sourceAllocation.retainOrdinaryProvider(bytes,purpose)
  }
  /** Root/account持锁时只核已发布责任身份，不能反向取得backing锁或授予新的读资格。 */
  private[floatingpod] def requireSourceAllocationOwner(actual:com.wi.floatingpod.server.source.SourceForestAllocationBudget.Root):Unit =
    require(!retired && main!=null && (Thread.currentThread() eq main) && transfer!=null &&
      actual!=null && (sourceAllocation eq actual),"original claimed schema6 source responsibility required")
  /** 即使epoch/窗口已失效，原Main仍须能明确撤销自己的读责任；不清除原failure。 */
  def drop():Unit = synchronized {
    owner();require(!dropped && !retired,"schema6 Main input drop is one-shot")
    projection=null;domainData=null;dropped=true
  }
  private[mysql] def requireBinding(actualSession:MySqlDbFirstSession,actualAdapter:MySqlOwnedIoAdapter):Unit = synchronized {
    require(!retired && (session eq actualSession) && (adapter eq actualAdapter),"original schema6 read backing binding required")
  }
  private[mysql] def requireRetiredOnWorker():Unit = synchronized {
    require((Thread.currentThread() eq worker) && retired && sourceAllocation==null && dataAllocation==null &&
      projection==null && domainData==null && session==null && adapter==null && main==null && transfer==null &&
      predecessor==null && successorObservation==null && approved==null,
      "actual worker-retired Main backing and all DATA/source/owner edges required")
  }
  private[mysql] def retireOnWorker(actualSession:MySqlDbFirstSession):Unit = synchronized {
    require(Thread.currentThread() eq worker,"original schema6 read retirement worker required")
    require(!retired && (session eq actualSession) && (main==null || dropped),
      "claimed schema6 Main input remains undropped")
    if sourceAllocation!=null then
      sourceAllocation.retireOnWorker()
      sourceAllocation=null
    projection=null;domainData=null
    if dataAllocation!=null then
      dataAllocation.closeScopedReadBudget()
      dataAllocation=null
    predecessor=null;successorObservation=null;session=null;adapter=null;approved=null;transfer=null;main=null;retired=true
  }

private[mysql] object MySqlDbFirstMainReadBacking:
  val HolderBytes=2048L // backing及原source Root/account-book空容器的保守预付；实际账户另计费。
