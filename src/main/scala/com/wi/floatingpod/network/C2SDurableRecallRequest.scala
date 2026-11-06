package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** namespace必须来自实际服务器发布；声明本身不证明actor认证或fresh接纳。 */
final class C2SDurableRecallRequest(var namespace:RecallRequestNamespace,var requestUuid:UUID,
    var podUuid:UUID,var expectedStateVersion:Long) extends IMessage:
  def this()=this(null,null,null,-1L)
  private[floatingpod] def validate():Unit =
    require(namespace!=null && expectedStateVersion>=0L,"durable namespace and version required")
    DurableRecallCodec.requireId(requestUuid);DurableRecallCodec.requireId(podUuid)
  override def toBytes(output:ByteBuf):Unit =
    validate();DurableRecallCodec.writeNamespace(output,namespace)
    ProtocolCodec.writeUuid(output,requestUuid);ProtocolCodec.writeUuid(output,podUuid)
    output.writeLong(expectedStateVersion)
  override def fromBytes(input:ByteBuf):Unit =
    val ns=DurableRecallCodec.readNamespace(input)
    val request=ProtocolCodec.readUuid(input);val pod=ProtocolCodec.readUuid(input);val version=input.readLong()
    ProtocolCodec.requireFullyConsumed(input)
    val decoded=new C2SDurableRecallRequest(ns,request,pod,version);decoded.validate()
    namespace=ns;requestUuid=request;podUuid=pod;expectedStateVersion=version
