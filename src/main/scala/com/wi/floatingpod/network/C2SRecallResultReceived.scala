package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 客户端已实际接收并保留终态；不是SQL或physical ACK。 */
final class C2SRecallResultReceived(var result:S2CDurableRecallStatus) extends IMessage:
  def this()=this(null)
  private[floatingpod] def validate():Unit =
    require(result!=null,"exact received terminal required");result.validate()
    require(result.phase==DurableRecallPhase.Committed || result.phase==DurableRecallPhase.Refused,
      "only accepted final results have custody receipts")
  override def toBytes(output:ByteBuf):Unit =
    validate();result.toBytes(output)
  override def fromBytes(input:ByteBuf):Unit =
    val decoded=new S2CDurableRecallStatus();decoded.fromBytes(input)
    val checked=new C2SRecallResultReceived(decoded);checked.validate()
    result=decoded
