package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** J与原C在任何capture前同属一笔Entry；请求DATA不证明认证、durable接纳或freeze许可。 */
private[floatingpod] final class MySqlDbFirstRecallJournalReservation private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,
    private[mysql] val handle:MySqlRetainedPlanHandle,
    private[mysql] val request:MySqlDbFirstRecallJournalRequest):
  val tag:IoOperationTag=handle.tag
  val originalTag:IoOperationTag=request.originalTag

/** 原Main领取的journal SQL确认；仍须实际读代交接，不授予C capture/freeze或Entry退休。 */
private[floatingpod] final class MySqlDbFirstRecallJournalConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val originalTag:IoOperationTag,val bindingUuid:UUID,val actorUuid:UUID,
    val podUuid:UUID,val expectedStateVersion:Long,val clientRequestUuid:Option[UUID],
    val canonicalSha256:String):
  val tag:IoOperationTag=handle.tag
  def requireOriginal(reservation:MySqlDbFirstRecallJournalReservation):Unit=
    core.requireDbFirstRecallJournalConfirmation(this,reservation)
private[mysql] object MySqlDbFirstRecallJournalConfirmation:
  val HolderBytes=512L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstRecallJournalRequest,sha256:String):MySqlDbFirstRecallJournalConfirmation =
    new MySqlDbFirstRecallJournalConfirmation(core,handle,request.originalTag,request.bindingUuid,request.actorUuid,
      request.podUuid,request.expectedStateVersion,request.clientRequestUuid,sha256)

/** 只清实际明确拒收且未被worker观察的原journal占位，不用于UNKNOWN或durable J。 */
private[floatingpod] final class MySqlDbFirstRejectedJournalReceipt private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle):
  val tag:IoOperationTag=handle.tag

/** 原J确认/读代ACK之后的一次C转移；注册、worker退休和Main路由ACK均未完成时不能capture。
  * 仅含独立计费metadata；不借用原J canonical/Full27 payload。
  */
private[floatingpod] final class MySqlDbFirstRecallJournalCaptureTransfer private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    private[mysql] val capture:MySqlDbFirstCaptureReservation,
    val bindingUuid:UUID,val actorUuid:UUID,val podUuid:UUID,val expectedStateVersion:Long):
  val tag:IoOperationTag=handle.tag
  val originalTag:IoOperationTag=capture.tag
private[mysql] object MySqlDbFirstRecallJournalCaptureTransfer:
  val HolderBytes=4096L

/** 原Entry登记的一次worker持有人。来源reader和partial DATA在失败后继续保留，不重试。 */
private[mysql] final class MySqlDbFirstRecallJournalWork private[mysql] (
    reservationInput:MySqlDbFirstRecallJournalReservation,val root:MySqlFormat3ReadBudget):
  private[mysql] var reservation:MySqlDbFirstRecallJournalReservation=reservationInput
  private var transaction:MySqlDbFirstRecallJournalTransaction=null
  private var transactionAttempted=false
  private[mysql] def beginTransaction():MySqlDbFirstRecallJournalTransaction =
    reservation.core.requireExecutingDbFirstRecallJournal(this)
    require(!transactionAttempted && !materializationAttempted,"original journal transaction is one-shot before materialization")
    transactionAttempted=true
    try
      root.reserveRetainedMemory(MySqlDbFirstRecallJournalTransaction.HolderBytes+MySqlDbFirstRecallJournalConfirmation.HolderBytes,"journal runner BEFORE allocation")
      transaction=new MySqlDbFirstRecallJournalTransaction(this)
      transaction
    catch case cause:Throwable=>
      try reservation.core.failDbFirstRecallJournal(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause
  private[mysql] def requireTransaction(actual:MySqlDbFirstRecallJournalTransaction):Unit =
    reservation.core.requireExecutingDbFirstRecallJournal(this)
    require(actual!=null && (transaction eq actual),"exact original registered journal transaction required")
  private[mysql] def retainedTransaction:MySqlDbFirstRecallJournalTransaction =
    if transaction==null then throw new IllegalStateException("original journal runner absent")
    transaction

  /** 原runner完整读回后先撤借用，再允许关闭Session reader；Entry自己的before/target继续保管。 */
  private[mysql] def retirePreflightSource(actual:MySqlDbFirstRecallJournalTransaction):Unit =
    requireTransaction(actual)
    actual.requireReadbackForCleanup()
    source=null;sourceRoot=null

  private[mysql] def retireEntryPayload(actual:MySqlDbFirstRecallJournalTransaction,
      transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit =
    reservation.core.requireJournalCaptureTransferPayload(transfer,actual)
    require(transaction eq actual,"original journal transaction detach required")
    source=null;sourceRoot=null;construction=null;before=null;draft=null;result=null

  /** 专用拒绝终局门禁；不要求仍可执行的旧Work，且不关闭共享root。 */
  private[mysql] def retireRefusalFinalPayload(actual:MySqlDbFirstRecallJournalTransaction,
      receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    reservation.core.requireRefusalFinalPayload(receipt,actual)
    require(transaction eq actual,"original journal runner detach required")
    source=null;sourceRoot=null;construction=null;before=null;draft=null;result=null
    reservation=null

  private[mysql] var materializationAttempted=false
  private[mysql] var source:MySqlScriptSchemaSnapshot=null
  private[mysql] var sourceRoot:MySqlFormat3ReadBudget=null
  private[mysql] var construction:Vector[MySqlFormat3RelationSnapshot]=null
  private[mysql] var before:MySqlScriptSchemaSnapshot=null
  private[mysql] var draft:MySqlDbFirstRecallJournalDraft=null
  private[mysql] var result:MySqlDbFirstRecallJournalMaterialization=null

  private[mysql] def materialize(actual:MySqlScriptSchemaSnapshot,actualRoot:MySqlFormat3ReadBudget,
      facts:MySqlDbFirstRecallJournalFacts):MySqlDbFirstRecallJournalMaterialization =
    reservation.core.requireExecutingDbFirstRecallJournal(this)
    require(!materializationAttempted,"original journal materialization is one-shot")
    materializationAttempted=true
    try
      require(actualRoot!=null && !actualRoot.isClosed && !actualRoot.isScopedReadBudget &&
        actualRoot.sharesAllocationLedger(root) && (actualRoot.profile eq root.profile),
        "original Session source on exact shared ledger required")
      MySqlFormat3QueryProjection.requireDbFirstSource(actual,actual.inputs,actualRoot)
      source=actual;sourceRoot=actualRoot
      root.requireScriptDataInputs(actual.inputs)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        root.reserveRetainedMemory(4096L+256L*actual.rowCount,"journal complete before containers BEFORE allocation")
        construction=actual.relations.map { relation=>
          val rows=relation.rows.map { row=>
            root.reserveRetainedMemory(256L+96L*row.size,"journal original before row BEFORE copy")
            row.map {
              case MySqlFormat3Cell.Binary(bytes)=>MySqlFormat3Cell.Binary(
                MySqlFormat3Binary.takeOwned(bytes.copyBytes(root,"journal original before binary copy"),root))
              case cell @ MySqlFormat3Cell.Text(text)=>
                root.reserveRetainedMemory(MySqlFormat3Utf8.byteLength(text),"journal original before retained text")
                cell
              case cell=>cell
            }
          }
          MySqlFormat3RelationSnapshot(relation.table,rows)
        }
        before=MySqlScriptSchemaSnapshot.captureMaterialized(actual.schema,actual.connection,actual.observationScope,
          actual.maxAllowedPacketBytes,actual.inputs,construction,root)
        draft=MySqlDbFirstRecallJournalMaterializer.allocate(reservation.request,facts,before,root)
        result=MySqlDbFirstRecallJournalMaterializer.materialize(draft)
      }
      result
    catch case cause:Throwable=>
      try reservation.core.failDbFirstRecallJournal(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

private[mysql] object MySqlDbFirstRecallJournalWork:
  val HolderBytes=1024L
