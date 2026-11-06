package com.wi.floatingpod.network

/** 原runtime在实际read屏障之后排空已接受玩家收回；不发行read、SQL、native或停服资格。 */
private[floatingpod] final class DbFirstPreStopRecallDrain:
  private var lastEnd=0L
  private var recallSettledAt=0L
  private var freezeStarted=false
  private var completed=false
  private var failure=Option.empty[Throwable]

  def onEnd(end:Long)(drainRecall: => Unit)(recallSettled: => Boolean)
      (beginStop: => Unit)(advanceStop: => Boolean):Boolean =
    failure.foreach(throw _)
    require(end>lastEnd && !completed,"original monotonic pre-stop END required before source handoff")
    lastEnd=end
    try
      if recallSettledAt==0L then
        drainRecall
        if recallSettled then recallSettledAt=end
        false // 原owner/ACK调用栈先返回；下一真实END才冻结停服census。
      else
        require(end>recallSettledAt,"original recall drain must return before stop freeze")
        if !freezeStarted then
          freezeStarted=true
          beginStop
        completed=advanceStop
        completed
    catch case cause:Throwable=>
      failure=Some(cause)
      throw cause // 部分freeze/source交接失败不重试；runtime继续保原primary/资源。
