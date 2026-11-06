package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.{MySqlDbFirstRecallRequestPhase,MySqlDbFirstRecallRequestRecord}
import java.util.UUID
import com.wi.floatingpod.domain.RecallRefusalReason
import com.wi.floatingpod.server.io.IoOperationTag

/** 已计费完整原请求metadata；只有真实Boot owner的本次恢复pass能交接，不按phase发行终局。 */
private[floatingpod] final class DbFirstBootRecallRequestCustody(main:Thread,
    private var originals:Vector[MySqlDbFirstRecallRequestRecord]):
  require(main!=null && originals!=null && originals.forall(_!=null),"complete original Boot request census required")
  private var detached=false
  private var completed=Option.empty[DbFirstBootActiveRecovery.TerminalRecoveryPass]
  // 费用由Session独立census root预付；只保原record及枚举结果，不借用successor DATA/receipt。
  private var settled=Map.empty[IoOperationTag,(MySqlDbFirstRecallRequestPhase,Option[RecallRefusalReason])]
  private def owner():Unit=require((Thread.currentThread() eq main) && !detached,"live original Boot request Main required")
  def unresolvedCount:Int =
    owner();if completed.nonEmpty then 0 else originals.size-settled.size
  def originalRequests:Vector[MySqlDbFirstRecallRequestRecord] =
    owner();originals
  /** 只选择可进入真实恢复的cohort；true仍不是恢复完成或Ready资格。 */
  def mayRestoreResults(binding:UUID):Boolean =
    owner();require(binding!=null && binding!=new UUID(0L,0L),"actual current Boot binding required")
    originals.forall(record=>record.bindingUuid==binding && (record.phase match
      case MySqlDbFirstRecallRequestPhase.RefusedCommitted=>record.refusalReason.nonEmpty
      case MySqlDbFirstRecallRequestPhase.RecallCommitted=>record.refusalReason.isEmpty
      case _=>false))
  /** 仅筛选可进入原Boot实际Pod恢复的完整cohort；J-only保持未settled。
    * 此DATA选择不授予恢复、capture、SQL或publication权限，实际issuer须登记一次恢复责任。
    */
  def selectsPodRestoration(binding:UUID):Boolean =
    owner();require(binding!=null && binding!=new UUID(0L,0L),"actual current Boot binding required")
    originals.forall(record=>record.bindingUuid==binding && (record.phase match
      case MySqlDbFirstRecallRequestPhase.JournalCommitted=>record.refusalReason.isEmpty
      case MySqlDbFirstRecallRequestPhase.RefusedCommitted=>record.refusalReason.nonEmpty
      case MySqlDbFirstRecallRequestPhase.RecallCommitted=>record.refusalReason.isEmpty
      case _=>false))
  private[server] def completeResults(pass:DbFirstBootActiveRecovery.TerminalRecoveryPass,issuer:DbFirstBootActiveRecovery):Unit =
    owner();require(pass!=null && originals.nonEmpty && completed.isEmpty,"one actual Boot terminal recovery pass required")
    require(issuer!=null,"actual Boot terminal pass issuer required")
    issuer.requireIssuedResultPass(pass,this,originals,main)
    pass.requireOriginal(this,originals,main)
    require(mayRestoreResults(pass.bindingUuid),"actual complete terminal-result cohort identity required")
    completed=Some(pass)
  private[server] def isSettled(record:MySqlDbFirstRecallRequestRecord):Boolean =
    owner();require(originals.exists(_ eq record),"exact immutable original Boot request required")
    completed.nonEmpty || settled.contains(record.originalTag)
  /** 逐请求结算只接受真实owner同步登记期；J必须先取得实际final ACK，不能凭phase或复制receipt。 */
  private[server] def completeRefusal(original:MySqlDbFirstRecallRequestRecord,
      reason:RecallRefusalReason,issuer:DbFirstBootActiveRecovery):Unit =
    owner()
    require(issuer!=null && original!=null && reason!=null && completed.isEmpty &&
      originals.exists(_ eq original) && !settled.contains(original.originalTag),"one exact actual Boot refusal settlement required")
    issuer.requireCompletingBootRefusal(this,original,reason)
    settled=settled.updated(original.originalTag,(MySqlDbFirstRecallRequestPhase.RefusedCommitted,Some(reason)))
  private[server] def completeCommitted(original:MySqlDbFirstRecallRequestRecord,issuer:DbFirstBootActiveRecovery):Unit =
    owner()
    require(issuer!=null && original!=null && completed.isEmpty && originals.exists(_ eq original) &&
      !settled.contains(original.originalTag),"one exact actual Boot Committed settlement required")
    issuer.requireCompletingBootCommitted(this,original)
    settled=settled.updated(original.originalTag,(MySqlDbFirstRecallRequestPhase.RecallCommitted,None))
  /** retained服务结算有自己的实际确认边界，不能伪造普通ACK或清原cause。 */
  private[server] def completeRetainedCommitted(original:MySqlDbFirstRecallRequestRecord,
      issuer:DbFirstBootActiveRecovery):Unit =
    owner()
    require(issuer!=null && original!=null && completed.isEmpty && originals.exists(_ eq original) &&
      !settled.contains(original.originalTag),"one exact actual retained original Committed settlement required")
    issuer.requireCompletingRetainedCommitted(this,original)
    settled=settled.updated(original.originalTag,(MySqlDbFirstRecallRequestPhase.RecallCommitted,None))
  /** 初始terminal只接本次实际Pod恢复结果，不兑换或伪造旧final ACK。 */
  private[server] def completeRestoredTerminal(original:MySqlDbFirstRecallRequestRecord,
      issuer:DbFirstBootActiveRecovery):Unit =
    owner()
    require(issuer!=null && original!=null && completed.isEmpty && originals.exists(_ eq original) &&
      !settled.contains(original.originalTag),"one exact initial terminal restoration handoff required")
    val result=DbFirstBootRecallRequestCustody.terminalResult(original).getOrElse(
      throw new IllegalArgumentException("only strict initial terminal records may hand off restored results"))
    issuer.requireCompletingRestoredTerminal(this,original)
    settled=settled.updated(original.originalTag,result)
  private[server] def resultFor(original:MySqlDbFirstRecallRequestRecord):Option[(MySqlDbFirstRecallRequestPhase,Option[RecallRefusalReason])] =
    owner();require(originals.exists(_ eq original),"exact immutable original result required")
    if completed.nonEmpty then Some((original.phase,original.refusalReason))
    else settled.get(original.originalTag)
  def requireSettled():Unit =
    owner();require(unresolvedCount==0,"original durable recall Boot lifecycle remains unresolved; count="+unresolvedCount)

  /** 只能由登记了本custody的真实issuer在最终Main收口后撤图，不是原请求完成资格。 */
  private[server] def detach(issuer:DbFirstBootActiveRecovery):Unit =
    owner();require(issuer!=null,"actual Boot census issuer required")
    issuer.requireBootCensusCustodyDetach(this)
    requireSettled()
    completed=None;settled=Map.empty;originals=null;detached=true

  /** 失败停服仅撤真实Main借用；未结算请求继续留在原付费census中，不伪造结果。 */
  private[server] def detachForStoppedCensus(issuer:DbFirstBootActiveRecovery):Unit =
    owner();require(issuer!=null,"actual failed-stop Boot census issuer required")
    issuer.requireStoppedBootCensusCustodyDetach(this)
    completed=None;settled=Map.empty;originals=null;detached=true

private[floatingpod] object DbFirstBootRecallRequestCustody:
  /** DATA分类只返回已记录的结果，不授予settlement或旧ACK资格。 */
  def terminalResult(record:MySqlDbFirstRecallRequestRecord):Option[(MySqlDbFirstRecallRequestPhase,Option[RecallRefusalReason])] =
    require(record!=null,"actual initial terminal record required")
    record.phase match
      case MySqlDbFirstRecallRequestPhase.RecallCommitted if record.refusalReason.isEmpty=>Some((record.phase,None))
      case MySqlDbFirstRecallRequestPhase.RefusedCommitted if record.refusalReason.nonEmpty=>Some((record.phase,record.refusalReason))
      case _=>None
  /** 只核完整历史请求与当前严格index结果；true本身不授结算、ACK或Ready。 */
  def matchesResult(original:MySqlDbFirstRecallRequestRecord,current:MySqlDbFirstRecallRequestRecord,
      phase:MySqlDbFirstRecallRequestPhase,reason:Option[RecallRefusalReason]):Boolean =
    original!=null && current!=null && reason!=null &&
      original.originalTag==current.originalTag && original.journalTag==current.journalTag &&
      original.bindingUuid==current.bindingUuid && original.actorUuid==current.actorUuid &&
      original.podUuid==current.podUuid && original.expectedStateVersion==current.expectedStateVersion &&
      original.clientRequestUuid==current.clientRequestUuid && original.journalRequestSha256==current.journalRequestSha256 &&
      current.phase==phase && current.refusalReason==reason
