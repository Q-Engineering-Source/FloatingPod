package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRefusalReason

import com.wi.floatingpod.server.io.IoOperationTag

/** 同一真实拒绝runner的来源身份；类型/相等DATA不授权限，必须回原core核登记实例。 */
private[floatingpod] trait MySqlDbFirstRecallRefusalSource:
  private[mysql] def core:MySqlRetainedPlanReservationCore
  private[mysql] def handle:MySqlRetainedPlanHandle
  def originalTag:IoOperationTag
  def executionTag:IoOperationTag
  def journalTag:IoOperationTag
  def routeTag:IoOperationTag
  def bindingUuid:java.util.UUID
  def refusalReason:Option[RecallRefusalReason]

/** 原J Entry的一次本地执行封锁；不证明durable J、数据库C缺席或当前SQL写入资格。
  * 必须回原core核确切注册引用；相同tag/原因重建的对象没有权限。
  */
private[floatingpod] final class MySqlDbFirstRecallPreCaptureExclusion private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,
    private[mysql] val reservation:MySqlDbFirstRecallJournalReservation,
    val cause:Throwable,val refusalReason:Option[RecallRefusalReason]=None) extends MySqlDbFirstRecallRefusalSource:
  val journalTag:IoOperationTag=reservation.tag
  val originalTag:IoOperationTag=reservation.originalTag
  private[mysql] def handle:MySqlRetainedPlanHandle=reservation.handle
  def executionTag:IoOperationTag=originalTag
  def routeTag:IoOperationTag=journalTag
  def bindingUuid:java.util.UUID=reservation.request.bindingUuid

private[mysql] object MySqlDbFirstRecallPreCaptureExclusion:
  val HolderBytes=512L
