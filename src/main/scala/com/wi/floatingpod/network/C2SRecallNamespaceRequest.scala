package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 只请求实际认证namespace，不提交业务或声明actor。 */
final class C2SRecallNamespaceRequest extends IMessage:
  override def toBytes(output:ByteBuf):Unit=DurableRecallCodec.writeHeader(output)
  override def fromBytes(input:ByteBuf):Unit =
    DurableRecallCodec.readHeader(input);ProtocolCodec.requireFullyConsumed(input)
