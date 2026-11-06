package com.wi.floatingpod.server

import java.util.UUID

/** 原Main pending的标量回馈关联；不发行认证、mutation、SQL重试或UNKNOWN结算资格。 */
private[server] final class MainRecallFeedbackCorrelation(main: Thread, val actor: UUID,
    val pod: UUID, val expectedVersion: Long, val operation: UUID, val originalRequest: Option[UUID]):
  require(main != null && actor != null && pod != null && expectedVersion >= 0L && operation != null &&
    originalRequest != null && originalRequest.forall(_ != null), "original recall feedback identity required")
  private var deliveryRequest = originalRequest
  private def owner(): Unit = require(Thread.currentThread() eq main, "original recall feedback Main required")
  def currentRequest: Option[UUID] =
    owner(); deliveryRequest
  def references(request: UUID): Boolean =
    owner(); request != null && (originalRequest.contains(request) || deliveryRequest.contains(request))
  /** caller必须先从实际handler/roster认证；这里只有同owner/Pod/版本才可换回馈request别名。 */
  def attachAuthenticated(authenticatedActor: UUID, target: UUID, version: Long, request: UUID): Boolean =
    owner()
    if authenticatedActor != actor || target != pod || version != expectedVersion || request == null then false
    else
      deliveryRequest = Some(request)
      true
