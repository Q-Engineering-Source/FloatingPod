package com.wi.floatingpod.storage.mysql

import java.security.MessageDigest
import java.util.UUID

/** 写者单槽回执身份；epoch 与 boot 属于当前写者范围，operation UUID 不可跨范围复用。 */
final case class MySqlCommitReceiptSlot(
    bootUuid: UUID,
    writerEpoch: Long,
    bindingUuid: UUID,
    operationUuid: UUID
):
  require(bootUuid != null, "receipt bootUuid must not be null")
  require(writerEpoch >= 0L, "receipt writerEpoch must not be negative")
  require(bindingUuid != null, "receipt bindingUuid must not be null")
  require(operationUuid != null, "receipt operationUuid must not be null")

  private[mysql] def exactlyMatches(context: MySqlOperationContext): Boolean =
    bootUuid == context.bootUuid
      && writerEpoch == context.writerEpoch
      && bindingUuid == context.bindingUuid
      && operationUuid == context.operationUuid

  private[mysql] def sameWriterScope(context: MySqlOperationContext): Boolean =
    bootUuid == context.bootUuid
      && writerEpoch == context.writerEpoch
      && bindingUuid == context.bindingUuid

  override def toString: String = "MySqlCommitReceiptSlot[redacted]"

/** 与 slot 同一事务保存的规范化操作效果摘要。 */
final case class MySqlCommitReceipt(
    slot: MySqlCommitReceiptSlot,
    effectFingerprintSha256: Vector[Byte]
):
  require(slot != null, "receipt slot must not be null")
  require(effectFingerprintSha256 != null, "receipt effect fingerprint must not be null")
  require(effectFingerprintSha256.size == 32, "receipt effect fingerprint must contain 32 bytes")

  override def toString: String = "MySqlCommitReceipt[redacted]"

enum MySqlCommitReceiptConflict:
  case WriterScopeMismatch, OperationSlotMismatch, EffectFingerprintMismatch

sealed trait MySqlCommitReceiptVerification

object MySqlCommitReceiptVerification:
  final case class Confirmed(receipt: MySqlCommitReceipt) extends MySqlCommitReceiptVerification
  case object Unproven extends MySqlCommitReceiptVerification
  final case class Conflict(reason: MySqlCommitReceiptConflict) extends MySqlCommitReceiptVerification

/** 对受控只读回读结果作精确分类；缺少证据永不等价于已知回滚。 */
object MySqlCommitReceipts:
  def verifyReadback(
      expected: MySqlOperationContext,
      observed: Option[MySqlCommitReceipt]
  ): MySqlCommitReceiptVerification =
    require(expected != null, "expected operation context must not be null")
    require(observed != null, "observed receipt option must not be null")
    require(observed.forall(_ != null), "observed receipt must not contain null")

    observed match
      case None => MySqlCommitReceiptVerification.Unproven
      case Some(receipt) if receipt.slot.exactlyMatches(expected) =>
        if MessageDigest.isEqual(receipt.effectFingerprintSha256.toArray, expected.canonicalFingerprintSha256.toArray) then
          MySqlCommitReceiptVerification.Confirmed(receipt)
        else
          MySqlCommitReceiptVerification.Conflict(MySqlCommitReceiptConflict.EffectFingerprintMismatch)
      case Some(receipt) if receipt.slot.operationUuid == expected.operationUuid =>
        MySqlCommitReceiptVerification.Conflict(MySqlCommitReceiptConflict.OperationSlotMismatch)
      case Some(receipt) if !receipt.slot.sameWriterScope(expected) =>
        MySqlCommitReceiptVerification.Conflict(MySqlCommitReceiptConflict.WriterScopeMismatch)
      case Some(_) =>
        MySqlCommitReceiptVerification.Unproven
