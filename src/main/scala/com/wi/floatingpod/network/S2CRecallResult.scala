package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import net.minecraftforge.fml.common.network.simpleimpl.IMessage

import java.util.UUID

/** 回传 Recall 操作的成功状态或有限失败原因，两种形状不可混用。 */
final class S2CRecallResult(
    var success: Boolean,
    var podUuid: UUID,
    var newStateVersion: Option[Long],
    var reason: Option[RecallFailureReason]
) extends IMessage:
  /** 为 Forge 反序列化保留的公开无参构造器。 */
  def this() = this(false, null, None, None)

  /** 写入结果的固定成功标志、UUID、optional version 与 optional bounded reason。 */
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    validate(success, podUuid, newStateVersion, reason)
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeCanonicalBoolean(output, success)
    ProtocolCodec.writeUuid(output, podUuid)
    ProtocolCodec.writeOptionalLong(output, newStateVersion)
    ProtocolCodec.writeOptionalReason(output, reason)

  /** 读取且验证完整结果，使不一致 payload 不能成为可观察状态。 */
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    ProtocolCodec.readHeader(input)
    val decodedSuccess = ProtocolCodec.readCanonicalBoolean(input)
    val decodedUuid = ProtocolCodec.readUuid(input)
    val decodedVersion = ProtocolCodec.readOptionalLong(input)
    val decodedReason = ProtocolCodec.readOptionalReason(input, RecallFailureReason.fromWireId)
    validate(decodedSuccess, decodedUuid, decodedVersion, decodedReason)
    ProtocolCodec.requireFullyConsumed(input)
    success = decodedSuccess
    podUuid = decodedUuid
    newStateVersion = decodedVersion
    reason = decodedReason

  private def validate(
      candidateSuccess: Boolean,
      candidateUuid: UUID,
      candidateVersion: Option[Long],
      candidateReason: Option[RecallFailureReason]
  ): Unit =
    require(candidateUuid != null, "podUuid must not be null")
    require(candidateVersion != null, "newStateVersion must not be null")
    require(candidateReason != null, "reason must not be null")
    if candidateSuccess then
      require(candidateVersion.exists(_ >= 0L), "successful result requires a non-negative newStateVersion")
      require(candidateReason.isEmpty, "successful result must not contain a failure reason")
    else
      require(candidateVersion.isEmpty, "failed result must not contain a newStateVersion")
      require(candidateReason.nonEmpty, "failed result requires a failure reason")

object S2CRecallResult:
  /** 建立成功结果。 */
  def succeeded(podUuid: UUID, newStateVersion: Long): S2CRecallResult =
    new S2CRecallResult(true, podUuid, Some(newStateVersion), None)

  /** 建立含有限原因的失败结果。 */
  def failed(podUuid: UUID, reason: RecallFailureReason): S2CRecallResult =
    new S2CRecallResult(false, podUuid, None, Some(reason))
