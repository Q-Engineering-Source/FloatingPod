package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

enum RecallOperationPhase(val wireId: Int):
  case Processing extends RecallOperationPhase(0)
  case Committed extends RecallOperationPhase(1)
  case Refused extends RecallOperationPhase(2)
  /** 原operation/冻结责任仍在；此结果不能解锁或换ID重发。 */
  case RetainedFailure extends RecallOperationPhase(3)

/** 关联标量只表示原Main观察，不携带SQL、root、entity或客户端业务权威。 */
final class S2CRecallOperationStatus(var requestUuid: UUID, var podUuid: UUID,
    var expectedStateVersion: Long, var operationUuid: Option[UUID],
    var phase: RecallOperationPhase, var reason: Option[RecallFailureReason]) extends IMessage:
  def this() = this(null, null, -1L, None, null, None)
  private[network] def validate(): Unit =
    require(requestUuid != null && podUuid != null && expectedStateVersion >= 0L && operationUuid != null &&
      operationUuid.forall(_ != null) && phase != null && reason != null, "complete recall status required")
    phase match
      case RecallOperationPhase.Processing => require(operationUuid.nonEmpty && reason.isEmpty, "processing original operation required")
      case RecallOperationPhase.Committed => require(operationUuid.nonEmpty && reason.isEmpty && expectedStateVersion < Long.MaxValue, "committed version/operation required")
      case RecallOperationPhase.Refused => require(operationUuid.isEmpty && reason.nonEmpty, "refusal cannot claim an accepted operation")
      case RecallOperationPhase.RetainedFailure => require(operationUuid.nonEmpty && reason.contains(RecallFailureReason.InfrastructureFailure), "retained failure original operation required")
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output required"); validate()
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeUuid(output, requestUuid); ProtocolCodec.writeUuid(output, podUuid)
    output.writeLong(expectedStateVersion)
    ProtocolCodec.writeCanonicalBoolean(output, operationUuid.nonEmpty)
    operationUuid.foreach(ProtocolCodec.writeUuid(output, _))
    output.writeByte(phase.wireId)
    ProtocolCodec.writeOptionalReason(output, reason)
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input required")
    ProtocolCodec.readHeader(input)
    val request = ProtocolCodec.readUuid(input); val pod = ProtocolCodec.readUuid(input)
    val version = input.readLong()
    val operation = if ProtocolCodec.readCanonicalBoolean(input) then Some(ProtocolCodec.readUuid(input)) else None
    val phaseId = input.readUnsignedByte()
    val decodedPhase = RecallOperationPhase.values.find(_.wireId == phaseId).getOrElse(
      throw new IllegalArgumentException("Unknown recall operation phase " + phaseId))
    val decodedReason = ProtocolCodec.readOptionalReason(input, RecallFailureReason.fromWireId)
    ProtocolCodec.requireFullyConsumed(input)
    val decoded = new S2CRecallOperationStatus(request, pod, version, operation, decodedPhase, decodedReason)
    decoded.validate()
    requestUuid = request; podUuid = pod; expectedStateVersion = version
    operationUuid = operation; phase = decodedPhase; reason = decodedReason
