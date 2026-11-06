package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

enum RecallResumeLookup:
  case NamespaceMismatch, NotFound
  case Located(pod:UUID,expectedVersion:Long,clientRequest:Option[UUID])

/** 只读定位metadata，不包含持久phase或终局许可；缺行不是absence/fresh/重执行资格。 */
final class S2CRecallResumeResult(var namespace:RecallRequestNamespace,var operationUuid:UUID,
    var lookup:RecallResumeLookup) extends IMessage:
  def this()=this(null,null,null)
  private[floatingpod] def validate():Unit =
    require(namespace!=null && lookup!=null,"complete original recall lookup required")
    DurableRecallCodec.requireId(operationUuid)
    lookup match
      case RecallResumeLookup.Located(pod,version,request)=>
        DurableRecallCodec.requireId(pod)
        require(version>=0L && request!=null,"actual located recall metadata required")
        request.foreach(DurableRecallCodec.requireId)
      case _=>()
  override def toBytes(output:ByteBuf):Unit =
    validate();DurableRecallCodec.writeNamespace(output,namespace);ProtocolCodec.writeUuid(output,operationUuid)
    lookup match
      case RecallResumeLookup.NamespaceMismatch=>output.writeByte(0)
      case RecallResumeLookup.NotFound=>output.writeByte(1)
      case RecallResumeLookup.Located(pod,version,request)=>
        output.writeByte(2);ProtocolCodec.writeUuid(output,pod);output.writeLong(version)
        ProtocolCodec.writeCanonicalBoolean(output,request.nonEmpty);request.foreach(ProtocolCodec.writeUuid(output,_))
  override def fromBytes(input:ByteBuf):Unit =
    val ns=DurableRecallCodec.readNamespace(input);val operation=ProtocolCodec.readUuid(input)
    val found=input.readUnsignedByte() match
      case 0=>RecallResumeLookup.NamespaceMismatch
      case 1=>RecallResumeLookup.NotFound
      case 2=>
        val pod=ProtocolCodec.readUuid(input);val version=input.readLong()
        val request=if ProtocolCodec.readCanonicalBoolean(input) then Some(ProtocolCodec.readUuid(input)) else None
        RecallResumeLookup.Located(pod,version,request)
      case other=>throw new IllegalArgumentException("unknown recall lookup result "+other)
    ProtocolCodec.requireFullyConsumed(input)
    val decoded=new S2CRecallResumeResult(ns,operation,found);decoded.validate()
    namespace=ns;operationUuid=operation;lookup=found
