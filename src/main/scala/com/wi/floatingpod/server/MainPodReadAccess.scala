package com.wi.floatingpod.server

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import java.util.UUID

/** Main请求预览的读契约；每次调用必须核对当前原owner和DATA代际，不授予提交或恢复资格。 */
private[floatingpod] trait MainPodReadAccess:
  private[floatingpod] def requireCurrentReads(): Unit
  def findState(id: UUID): Option[FloatingPodState]
  def findEntitlement(id: UUID): Option[PlayerEntitlement]
  def activeCount: Int
  def activeCountFor(id: UUID): Int
  def activeLimit: Int
