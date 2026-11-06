package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.time.LocalDateTime

/** 原Main的准备请求占位；与实体快照capture分开，不可转换成旧repository plan。 */
private[floatingpod] final class MySqlDbFirstInitialPreparationReservation private[mysql] (
    private[mysql] val core: MySqlRetainedPlanReservationCore,
    private[mysql] val handle: MySqlRetainedPlanHandle,
    val request: MySqlControlledDebugPrepareRequest, val activeLimit: Int):
  val tag: IoOperationTag=handle.tag

/** 原Entry先登记的worker持有责任；不是事务成功或prepared proof。
  * 后续reader/materializer必须挂在此原持有人下，不能以另一个root替换未决请求。
  */
private[mysql] final class MySqlDbFirstInitialPreparationWork private[mysql] (
    val reservation: MySqlDbFirstInitialPreparationReservation,
    val root: MySqlFormat3ReadBudget, val requestBacking: MySqlRepositoryRequestIdentity):
  private var transaction:MySqlDbFirstInitialPreparationTransaction=null
  private var transactionAttempted=false
  private[mysql] def beginTransaction():MySqlDbFirstInitialPreparationTransaction =
    reservation.core.requireExecutingDbFirstInitialPreparation(this)
    require(!transactionAttempted && !preflightAttempted,"original preparation transaction is one-shot before preflight")
    transactionAttempted=true
    try
      root.reserveRetainedMemory(MySqlDbFirstInitialPreparationTransaction.HolderBytes+MySqlDbFirstInitialPreparationConfirmation.HolderBytes+MySqlDbFirstPreparationFinalReceipt.HolderBytes+MySqlDbFirstPreparationRejectionConfirmation.HolderBytes,"preparation runner/confirmation BEFORE allocation")
      transaction=new MySqlDbFirstInitialPreparationTransaction(this)
      transaction
    catch case cause:Throwable=>
      try reservation.core.failDbFirstInitialPreparation(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause
  private[mysql] def requireTransaction(actual:MySqlDbFirstInitialPreparationTransaction):Unit =
    reservation.core.requireExecutingDbFirstInitialPreparation(this)
    require(actual != null && (transaction eq actual),"exact original registered preparation transaction required")
  /** core在Main领确认时只借原已登记runner；引用本身不能签发成功。 */
  private[mysql] def retainedTransaction:MySqlDbFirstInitialPreparationTransaction =
    if transaction==null then throw new IllegalStateException("original preparation runner absent")
    transaction
  private[mysql] def retirePreflightSource(actual:MySqlDbFirstInitialPreparationTransaction):Unit =
    requireTransaction(actual)
    actual.requireReadbackForCleanup()
    if forest!=null then
      forest.close();forest=null
    before=null;sourceRoot=null;preflightComplete=false

  private[mysql] def requireRejectedTransaction(actual:MySqlDbFirstInitialPreparationTransaction,
      cause:MySqlDbFirstPreparationRejected):Unit =
    reservation.core.requireRejectedPreparationCleanup(this,cause)
    require(actual!=null && (transaction eq actual) && (preflightRejection eq cause),
      "exact original runner and request preflight rejection required")

  /** 拒绝的完整before仅供原runner构造未变更的DATA successor，不授予SQL恢复资格。 */
  private[mysql] def rejectedDataSource(actual:MySqlDbFirstInitialPreparationTransaction,
      cause:MySqlDbFirstPreparationRejected):MySqlScriptSchemaSnapshot =
    requireRejectedTransaction(actual,cause)
    require(before!=null && sourceRoot!=null && forest!=null && !preflightComplete &&
      !draftAttempted && !transitionAttempted,"original rejected preflight source required")
    forest.result
    MySqlFormat3QueryProjection.requireDbFirstSource(before,before.inputs,sourceRoot)
    before

  private[mysql] def retireRejectedPreflightSource(actual:MySqlDbFirstInitialPreparationTransaction,
      cause:MySqlDbFirstPreparationRejected):Unit =
    requireRejectedTransaction(actual,cause)
    actual.requireRejectedResourceCleanup(cause)
    if forest!=null then
      forest.close();forest=null
    before=null;sourceRoot=null;preflightComplete=false

  private[mysql] def retireEntryPayload(actual:MySqlDbFirstInitialPreparationTransaction):Unit =
    require(actual!=null && (actual eq transaction) && actual.resourcesRetired && forest==null && before==null && sourceRoot==null,
      "original SQL-retired preparation payload required")
    draft=null;transition=null;transaction=null

  private[mysql] def retireRejectedEntryPayload(actual:MySqlDbFirstInitialPreparationTransaction,
      cause:MySqlDbFirstPreparationRejected):Unit =
    reservation.core.requireRejectedPayloadRetirement(this,cause)
    require(actual!=null && (actual eq transaction) && (preflightRejection eq cause),"original rejected payload runner required")
    require(actual.rejectedResourcesRetired && forest==null && before==null && sourceRoot==null &&
      draft==null && transition==null,"original fully retired rejected payload required")
    transaction=null;preflightRejection=null

  private[mysql] def preflightForestPending:Boolean=forest != null

  private var preflightAttempted=false
  private var before:MySqlScriptSchemaSnapshot=null
  private var sourceRoot:MySqlFormat3ReadBudget=null
  private var forest:MySqlDbFirstForestRead=null
  private var preflightComplete=false
  private var preflightRejection:MySqlDbFirstPreparationRejected=null
  private var draftAttempted=false
  private var draft:MySqlDbFirstInitialPreparationDraft=null
  private var transitionAttempted=false
  private var transition:MySqlDbFirstInitialPreparationTransition=null

  private[mysql] def ownsTransition(candidate:MySqlDbFirstInitialPreparationTransition):Boolean =
    candidate != null && (transition eq candidate)

  private[mysql] def constructTransition(facts:MySqlDbFirstInitialPreparationFacts):MySqlDbFirstInitialPreparationTransition =
    reservation.core.requireExecutingDbFirstInitialPreparation(this)
    require(draft != null && !transitionAttempted,"original preparation domain draft and one-shot transition required")
    transitionAttempted=true
    try
      root.reserveRetainedMemory(MySqlDbFirstInitialPreparationTransition.HolderBytes,"preparation transition BEFORE allocation")
      transition=new MySqlDbFirstInitialPreparationTransition(this,draft,facts)
      transition.construct()
      transition
    catch case cause:Throwable=>
      try reservation.core.failDbFirstInitialPreparation(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

  private[mysql] def ownsDraft(candidate:MySqlDbFirstInitialPreparationDraft):Boolean =
    candidate != null && (draft eq candidate)

  /** 先将部分构造责任挂到原Work；禁止失败后换root或重造另一个草稿。 */
  private[mysql] def constructDomainRows(plannedAt:LocalDateTime):MySqlDbFirstInitialPreparationDraft =
    val source=validatedDataSource
    require(!draftAttempted,"original preparation draft is one-shot")
    draftAttempted=true
    try
      root.reserveRetainedMemory(MySqlDbFirstInitialPreparationDraft.HolderBytes,"preparation draft BEFORE allocation")
      draft=new MySqlDbFirstInitialPreparationDraft(reservation,root,plannedAt)
      draft.construct(source)
      draft
    catch case cause:Throwable=>
      try reservation.core.failDbFirstInitialPreparation(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

  /** 原worker先持有完整来源与forest holder再验证；失败不替换Entry或丢弃部分构造责任。 */
  private[mysql] def preflight(source:MySqlScriptSchemaSnapshot):Unit =
    preflight(source,root)

  /** JDBC来源仍归原Session root；只跨同ledger保留DATA，不改写reader或Session身份。 */
  private[mysql] def preflight(source:MySqlScriptSchemaSnapshot,actualSourceRoot:MySqlFormat3ReadBudget):Unit =
    reservation.core.requireExecutingDbFirstInitialPreparation(this)
    require(!preflightAttempted,"original preparation preflight is one-shot")
    preflightAttempted=true
    try
      require(source != null,"original complete preparation source required")
      require(actualSourceRoot != null && !actualSourceRoot.isClosed && !actualSourceRoot.isScopedReadBudget &&
        actualSourceRoot.sharesAllocationLedger(root) && (actualSourceRoot.profile eq root.profile),
        "preparation source must retain exact original shared allocation family")
      root.requireScriptDataInputs(source.inputs)
      MySqlFormat3QueryProjection.requireDbFirstSource(source,source.inputs,actualSourceRoot)
      sourceRoot=actualSourceRoot
      before=source
      sourceRoot.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"preparation forest holder BEFORE allocation")
      forest=new MySqlDbFirstForestRead(sourceRoot,source.inputs,reservation.request.expectedBinding.bindingUuid)
      try
        MySqlDbFirstInitialPreparationPreflight.requireFresh(source,reservation.request,reservation.tag.operationId,
          reservation.activeLimit,sourceRoot,forest)
      catch case rejected:MySqlDbFirstPreparationRejected=>
        // 身份/容量拒绝发生在forest扫描前；补全既存forest健康检查后才保存局部候选来源。
        // 历史重建或其他阶段抛出的同类型错误不能取得本次请求的资格。
        try
          forest.validate(source)
          preflightRejection=rejected
        catch case invalid:Throwable=>MySqlOwnedSessionLifecycle.suppress(rejected,invalid)
        throw rejected
      preflightComplete=true
    catch case cause:Throwable=>
      try reservation.core.failDbFirstInitialPreparation(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

  /** 后续DATA物化只能借本次原完整before；此getter不签发SQL或placement资格。 */
  private[mysql] def validatedDataSource:MySqlScriptSchemaSnapshot =
    reservation.core.requireExecutingDbFirstInitialPreparation(this)
    require(preflightComplete && before != null,"original preparation preflight incomplete")
    require(sourceRoot != null && sourceRoot.sharesAllocationLedger(root),"original preparation DATA source root lost")
    MySqlFormat3QueryProjection.requireDbFirstSource(before,before.inputs,sourceRoot)
    before

private[mysql] object MySqlDbFirstInitialPreparationWork:
  val HolderBytes=512L
