package com.wi.floatingpod.storage.mysql

import java.util.UUID

/** 文件引用仅为 DATA；没有 native ACK、当前 source 或 SQL 资格。 */
private[mysql] final class MySqlModeWitnessReference private[mysql] (
    val phase: MySqlOperationEffectPhase,
    val workflowUuid: UUID,
    val attemptUuid: UUID,
    val phaseOperationUuid: UUID,
    val beforeImage: Int,
    val targetImage: Int,
    val basename: String,
    val artifactVolume: Long,
    val artifactFileId128: Vector[Byte],
    val fullLength: Long,
    val fullSha256: Vector[Byte],
    val profileSha256: Vector[Byte],
    val sourceAnchorSha256: Vector[Byte],
    val originalReceiptSha256: Vector[Byte]
):
  require(phase != null && workflowUuid != null && attemptUuid != null && phaseOperationUuid != null &&
    fullLength > 0L && Vector(fullSha256, profileSha256, sourceAnchorSha256, originalReceiptSha256)
      .forall(value => value != null && value.size == 32), "complete mode witness DATA reference required")
  require(basename != null && artifactFileId128 != null && artifactFileId128.size == 16 &&
    basename == ".floatingpod-b1-mode-full20-v1-" + attemptUuid + "-" +
      java.util.HexFormat.of().formatHex(fullSha256.toArray) + ".bin",
    "fixed artifact name/full128 identity DATA required")
  require((phase == MySqlOperationEffectPhase.Reserve && beforeImage == 0 && targetImage == 1) ||
    (phase == MySqlOperationEffectPhase.Execute && beforeImage == 1 && targetImage == 2),
    "fixed ordinary mode phase/image mapping required")
  override def toString: String = "ModeWitnessReference[DATA; no native ACK]"
