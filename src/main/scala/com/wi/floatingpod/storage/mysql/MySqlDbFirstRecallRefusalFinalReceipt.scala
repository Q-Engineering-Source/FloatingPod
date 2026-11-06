package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 只含已计费metadata的原拒绝终局凭证；不借用confirmation、exclusion或request/RAW。
  * Main仍须跨END断开旧引用，router断开exclusion；实际worker退休ACK之前不得反馈完成。
  */
private[floatingpod] final class MySqlDbFirstRecallRefusalFinalReceipt private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val originalTag:IoOperationTag,val bindingUuid:UUID,val actorUuid:UUID,val podUuid:UUID,
    val clientRequestUuid:Option[UUID],val canonicalSha256:String):
  val tag:IoOperationTag=handle.tag

private[mysql] object MySqlDbFirstRecallRefusalFinalReceipt:
  val HolderBytes=1024L
  def issue(confirmation:MySqlDbFirstRecallRefusalConfirmation):MySqlDbFirstRecallRefusalFinalReceipt =
    new MySqlDbFirstRecallRefusalFinalReceipt(confirmation.core,confirmation.handle,confirmation.originalTag,
      confirmation.bindingUuid,confirmation.actorUuid,confirmation.podUuid,confirmation.clientRequestUuid,
      confirmation.canonicalSha256)
