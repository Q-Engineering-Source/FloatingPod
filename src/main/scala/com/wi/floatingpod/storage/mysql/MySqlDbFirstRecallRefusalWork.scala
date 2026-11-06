package com.wi.floatingpod.storage.mysql

/** 原J/C Entry上的一次拒绝worker责任。登记和预收费必须先于请求/完整DATA构造。
  * J原before/target保持不变；当前拒绝另持完整before/target，不能复用旧未应用证明。
  * 本类型不发行barrier、当前not-applied资格或SQL写入权限。
  */
private[mysql] final class MySqlDbFirstRecallRefusalWork private[mysql] (
    exclusionInput:MySqlDbFirstRecallRefusalSource,val root:MySqlFormat3ReadBudget):
  private[mysql] var exclusion:MySqlDbFirstRecallRefusalSource=exclusionInput
  private def core=exclusion.core
  private var transactionAttempted=false
  private var transaction:MySqlDbFirstRecallRefusalTransaction=null
  private[mysql] def beginTransaction():MySqlDbFirstRecallRefusalTransaction =
    core.requireExecutingRecallRefusal(this)
    require(!transactionAttempted && !materializationAttempted,"original refusal transaction is one-shot")
    transactionAttempted=true
    try
      root.reserveRetainedMemory(MySqlDbFirstRecallRefusalTransaction.HolderBytes+MySqlDbFirstRecallRefusalConfirmation.HolderBytes+MySqlDbFirstRecallRefusalFinalReceipt.HolderBytes,"original refusal runner BEFORE allocation")
      transaction=new MySqlDbFirstRecallRefusalTransaction(this)
      transaction
    catch case cause:Throwable=>
      try core.failRecallRefusal(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause
  private[mysql] def retainedTransaction:MySqlDbFirstRecallRefusalTransaction =
    if transaction==null then throw new IllegalStateException("original refusal runner absent")
    transaction
  private[mysql] def requireTransaction(actual:MySqlDbFirstRecallRefusalTransaction):Unit =
    core.requireExecutingRecallRefusal(this)
    require(actual!=null && (transaction eq actual),"exact registered original refusal transaction required")
  private[mysql] def retirePreflightSource(actual:MySqlDbFirstRecallRefusalTransaction):Unit =
    requireTransaction(actual)
    actual.requireReadbackForCleanup()
    source=null;sourceRoot=null
  /** 专用拒绝终局门禁；不要求仍可执行的旧Work，且不关闭共享root。 */
  private[mysql] def retireRefusalFinalPayload(actual:MySqlDbFirstRecallRefusalTransaction,
      receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit =
    core.requireRefusalFinalPayload(receipt,actual)
    require(transaction eq actual,"original refusal runner detach required")
    source=null;sourceRoot=null;construction=null;before=null;draft=null;result=null
    request=null;exclusion=null

  private[mysql] var materializationAttempted=false
  private[mysql] var request:MySqlDbFirstRecallRefusalRequest=null
  private[mysql] var source:MySqlScriptSchemaSnapshot=null
  private[mysql] var sourceRoot:MySqlFormat3ReadBudget=null
  private[mysql] var construction:Vector[MySqlFormat3RelationSnapshot]=null
  private[mysql] var before:MySqlScriptSchemaSnapshot=null
  private[mysql] var draft:MySqlDbFirstRecallRefusalDraft=null
  private[mysql] var result:MySqlDbFirstRecallRefusalMaterialization=null

  private[mysql] def materialize(actual:MySqlScriptSchemaSnapshot,actualRoot:MySqlFormat3ReadBudget,
      facts:MySqlDbFirstRecallRefusalFacts):MySqlDbFirstRecallRefusalMaterialization =
    core.requireExecutingRecallRefusal(this)
    require(!materializationAttempted,"original refusal DATA materialization is one-shot")
    materializationAttempted=true
    try
      require(actualRoot!=null && !actualRoot.isClosed && !actualRoot.isScopedReadBudget &&
        actualRoot.sharesAllocationLedger(root) && (actualRoot.profile eq root.profile),
        "actual refusal source on exact shared profile/ledger required")
      MySqlFormat3QueryProjection.requireDbFirstSource(actual,actual.inputs,actualRoot)
      source=actual;sourceRoot=actualRoot
      root.requireScriptDataInputs(actual.inputs)
      request=exclusion match
        case live:MySqlDbFirstRecallPreCaptureExclusion=>live.refusalReason match
          case Some(reason)=>MySqlDbFirstRecallRefusalRequest.notAppliedWithReason(live.executionTag,live.reservation.request,reason,root)
          case None=>MySqlDbFirstRecallRefusalRequest.notApplied(live.executionTag,live.reservation.request,root)
        case boot:MySqlDbFirstBootRecallMainDecision=>
          MySqlDbFirstBootRecallTakeoverData.strictRefusalRequest(actual,actualRoot,root,boot.intent,boot.refusalReason.get)
        case _=>throw new IllegalArgumentException("actual registered refusal source required")
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        root.reserveRetainedMemory(4096L+256L*actual.rowCount,"refusal complete before containers BEFORE allocation")
        construction=actual.relations.map { relation=>
          val rows=relation.rows.map { row=>
            root.reserveRetainedMemory(256L+96L*row.size,"refusal original before row BEFORE copy")
            row.map {
              case MySqlFormat3Cell.Binary(bytes)=>MySqlFormat3Cell.Binary(
                MySqlFormat3Binary.takeOwned(bytes.copyBytes(root,"refusal original before binary copy"),root))
              case cell @ MySqlFormat3Cell.Text(text)=>
                root.reserveRetainedMemory(MySqlFormat3Utf8.byteLength(text),"refusal original before retained text")
                cell
              case cell=>cell
            }
          }
          MySqlFormat3RelationSnapshot(relation.table,rows)
        }
        before=MySqlScriptSchemaSnapshot.captureMaterialized(actual.schema,actual.connection,actual.observationScope,
          actual.maxAllowedPacketBytes,actual.inputs,construction,root)
        draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
        result=MySqlDbFirstRecallRefusalMaterializer.materialize(draft)
      }
      result
    catch case cause:Throwable=>
      try core.failRecallRefusal(this,cause)
      catch case secondary:Throwable=>MySqlOwnedSessionLifecycle.suppress(cause,secondary)
      throw cause

private[mysql] object MySqlDbFirstRecallRefusalWork:
  val HolderBytes=1024L
