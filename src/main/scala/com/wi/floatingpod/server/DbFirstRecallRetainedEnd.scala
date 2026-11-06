package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.IoSubmission

/** 只调度原owner的受限后续END；实际资格、native操作和ACK仍由原Main/adapter逐步核验。 */
private[server] final class DbFirstRecallRetainedEnd:
  private enum Stage:
    case Inactive, Physical, Submit, AwaitingAck, Completed, Stopped
  private var stage=Stage.Inactive
  private var lastEnd=0L

  def active:Boolean = stage==Stage.Physical || stage==Stage.Submit || stage==Stage.AwaitingAck
  def awaitingAck:Boolean = stage==Stage.AwaitingAck

  /** 仅在Main完整接受且owner撤reply之后调用；不从reply类型或cause非空推断资格。 */
  def accepted(end:Long):Unit =
    require(stage==Stage.Inactive && end>0L,"original retained acceptance requires one actual END")
    lastEnd=end
    stage=Stage.Physical

  def advance(end:Long)(physical: => Boolean)(handoff: => Unit)(submit: => IoSubmission):Unit =
    require(active && end>=lastEnd,"original retained END order required")
    if end==lastEnd || awaitingAck then return
    lastEnd=end
    stage match
      case Stage.Physical=>
        if physical then
          handoff
          stage=Stage.Submit // 撤cap调用栈先返回；最早下一个实际END才向worker提交。
      case Stage.Submit=>
        submit match
          case IoSubmission.Accepted=>stage=Stage.AwaitingAck
          case IoSubmission.RejectedCapacity=>() // 未接受：下一END只重交同一receipt。
          case IoSubmission.RejectedQuiescing=>throw new IllegalStateException("original retained recall release quiesced")
          case IoSubmission.RejectedStopped(cause)=>throw cause.getOrElse(new IllegalStateException("original retained recall release stopped"))
      case _=>throw new IllegalStateException("original retained END stage changed")

  def finish(actualAck: => Unit):Unit =
    require(awaitingAck,"original accepted retained release must precede ACK")
    actualAck
    stage=Stage.Completed

  /** 新失败不重放已接受工作；原owner保留completion/receipt/cause用于诊断。 */
  def stop():Unit = stage=Stage.Stopped
