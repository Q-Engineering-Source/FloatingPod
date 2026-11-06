package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 仅由实际Main同一当前binding和认证actor发布，不授予客户端业务权威。 */
final class S2CRecallNamespace(var namespace:RecallRequestNamespace) extends IMessage:
  def this()=this(null)
  private[floatingpod] def validate():Unit=require(namespace!=null,"actual recall namespace required")
  override def toBytes(output:ByteBuf):Unit =
    validate();DurableRecallCodec.writeNamespace(output,namespace)
  override def fromBytes(input:ByteBuf):Unit =
    val actual=DurableRecallCodec.readNamespace(input)
    ProtocolCodec.requireFullyConsumed(input);namespace=actual
