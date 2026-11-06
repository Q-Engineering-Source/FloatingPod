package com.wi.floatingpod.server

/** Main确认已保管原资格后，原owner撤除多余reply；失败仍沿原cause返回，不产生成功资格。 */
private[server] object DbFirstRecallCompletionHandoff:
  def accept(acceptance: => Option[Throwable])(dropReply: => Unit):Option[Throwable] =
    val retainedFailure=acceptance
    dropReply
    retainedFailure

  def consume(acceptance: => Option[Throwable])(dropReply: => Unit):Unit =
    val retainedFailure=accept(acceptance)(dropReply)
    retainedFailure.foreach(throw _)
