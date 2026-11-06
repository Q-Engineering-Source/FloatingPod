package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 显式原C UUID只读查询；没有pod/version mutation参数，不重发业务请求。 */
final class C2SRecallResumeQuery(var namespace:RecallRequestNamespace,var operationUuid:UUID) extends IMessage:
  def this()=this(null,null)
  private[floatingpod] def validate():Unit =
    require(namespace!=null,"actual resume namespace required");DurableRecallCodec.requireId(operationUuid)
  override def toBytes(output:ByteBuf):Unit =
    validate();DurableRecallCodec.writeNamespace(output,namespace);ProtocolCodec.writeUuid(output,operationUuid)
  override def fromBytes(input:ByteBuf):Unit =
    val actual=DurableRecallCodec.readNamespace(input);val operation=ProtocolCodec.readUuid(input)
    ProtocolCodec.requireFullyConsumed(input)
    val decoded=new C2SRecallResumeQuery(actual,operation);decoded.validate()
    namespace=actual;operationUuid=operation
