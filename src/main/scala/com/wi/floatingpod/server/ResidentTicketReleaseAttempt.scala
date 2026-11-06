package com.wi.floatingpod.server

/** 已验证原owner后的真实ticket释放尝试；partial/throw不允许盲重试，也不抹掉原cause。 */
private[server] final class ResidentTicketReleaseAttempt(key: AnyRef, view: TicketReleaseView):
  require(key != null && view != null, "original ticket and observation boundary required")
  private val main = Thread.currentThread()
  private var attempted = false
  private var acknowledged = false
  private var failure = Option.empty[Throwable]

  def releaseOnce(): Unit =
    require(Thread.currentThread() eq main, "resident ticket release requires original Main")
    view.assertMain()
    failure.foreach(throw _)
    if !acknowledged then
      require(!attempted, "resident release has an incomplete original attempt")
      attempted = true
      try
        ExactTicketReleaseObserver.release(key, view)
        acknowledged = true
      catch
        case cause: Throwable => failure = Some(cause); throw cause
