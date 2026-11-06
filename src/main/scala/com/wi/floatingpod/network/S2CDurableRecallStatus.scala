package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

enum DurableRecallPhase(val wireId:Int):
  /** 原client请求仍保留；J尚未确认并完成Main读代交接，不公开C UUID。 */
  case Pending extends DurableRecallPhase(0)
  case Processing extends DurableRecallPhase(1)
  case Committed extends DurableRecallPhase(2)
  /** 已接纳原C的持久拒绝，只能在实际final ACK后报告。 */
  case Refused extends DurableRecallPhase(3)
  /** 尚未创建J/C时的正常拒绝；不能拿来表示已接纳原operation的终局。 */
  case NotAccepted extends DurableRecallPhase(4)
  case RetainedFailure extends DurableRecallPhase(5)

/** 独立namespace关联的durable反馈，不放宽旧S2CRecallOperationStatus的契约。 */
final class S2CDurableRecallStatus(var namespace:RecallRequestNamespace,var requestUuid:UUID,var podUuid:UUID,
    var expectedStateVersion:Long,var operationUuid:Option[UUID],var phase:DurableRecallPhase,
    var reason:Option[RecallFailureReason]) extends IMessage:
  def this()=this(null,null,null,-1L,None,null,None)
  private[floatingpod] def validate():Unit =
    require(namespace!=null && expectedStateVersion>=0L && operationUuid!=null && phase!=null && reason!=null && reason.forall(_!=null),
      "complete durable recall status required")
    DurableRecallCodec.requireId(requestUuid);DurableRecallCodec.requireId(podUuid)
    operationUuid.foreach(DurableRecallCodec.requireId)
    phase match
      case DurableRecallPhase.Pending=>require(operationUuid.isEmpty &&
        (reason.isEmpty || reason.contains(RecallFailureReason.InfrastructureFailure)),"pending J cannot disclose C")
      case DurableRecallPhase.Processing=>require(operationUuid.nonEmpty && reason.isEmpty,"processing original C required")
      case DurableRecallPhase.Committed=>require(operationUuid.nonEmpty && reason.isEmpty && expectedStateVersion<Long.MaxValue,
        "committed original C and successor version required")
      case DurableRecallPhase.Refused=>require(operationUuid.nonEmpty && reason.nonEmpty,"durable refused original C required")
      case DurableRecallPhase.NotAccepted=>require(operationUuid.isEmpty && reason.nonEmpty,"not accepted cannot claim original C")
      case DurableRecallPhase.RetainedFailure=>require(operationUuid.nonEmpty &&
        reason.contains(RecallFailureReason.InfrastructureFailure),"retained original C failure required")
  override def toBytes(output:ByteBuf):Unit =
    validate();DurableRecallCodec.writeNamespace(output,namespace)
    ProtocolCodec.writeUuid(output,requestUuid);ProtocolCodec.writeUuid(output,podUuid);output.writeLong(expectedStateVersion)
    ProtocolCodec.writeCanonicalBoolean(output,operationUuid.nonEmpty)
    operationUuid.foreach(ProtocolCodec.writeUuid(output,_));output.writeByte(phase.wireId)
    ProtocolCodec.writeOptionalReason(output,reason)
  override def fromBytes(input:ByteBuf):Unit =
    val ns=DurableRecallCodec.readNamespace(input)
    val request=ProtocolCodec.readUuid(input);val pod=ProtocolCodec.readUuid(input);val version=input.readLong()
    val operation=if ProtocolCodec.readCanonicalBoolean(input) then Some(ProtocolCodec.readUuid(input)) else None
    val phaseId=input.readUnsignedByte()
    val decodedPhase=DurableRecallPhase.values.find(_.wireId==phaseId).getOrElse(
      throw new IllegalArgumentException("unknown durable recall phase "+phaseId))
    val decodedReason=ProtocolCodec.readOptionalReason(input,RecallFailureReason.fromWireId)
    ProtocolCodec.requireFullyConsumed(input)
    val decoded=new S2CDurableRecallStatus(ns,request,pod,version,operation,decodedPhase,decodedReason);decoded.validate()
    namespace=ns;requestUuid=request;podUuid=pod;expectedStateVersion=version
    operationUuid=operation;phase=decodedPhase;reason=decodedReason
