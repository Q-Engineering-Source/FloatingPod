package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 原Main领取的拒绝SQL确认；J仅作保留路由身份，业务结果仍为原C。
  * 只含独立计费metadata，不持有request/RAW，不发行终局反馈或Entry/root退休资格。
  */
private[floatingpod] final class MySqlDbFirstRecallRefusalConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val originalTag:IoOperationTag,val bindingUuid:UUID,val actorUuid:UUID,val podUuid:UUID,
    val clientRequestUuid:Option[UUID],val canonicalSha256:String):
  val tag:IoOperationTag=handle.tag

private[mysql] object MySqlDbFirstRecallRefusalConfirmation:
  val HolderBytes=1024L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstRecallRefusalRequest,sha256:String):MySqlDbFirstRecallRefusalConfirmation =
    new MySqlDbFirstRecallRefusalConfirmation(core,handle,request.originalTag,request.bindingUuid,
      request.actorUuid,request.podUuid,request.clientRequestUuid,sha256)
