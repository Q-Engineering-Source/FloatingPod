package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 确认服务端结果槽已不占用；不能生成或改写客户端终态。 */
final class S2CRecallResultReleased(var result:S2CDurableRecallStatus) extends IMessage:
  def this()=this(null)
  private[floatingpod] def validate():Unit =
    require(result!=null,"exact received terminal required");result.validate()
    require(result.phase==DurableRecallPhase.Committed || result.phase==DurableRecallPhase.Refused,
      "only accepted final results have custody receipts")
  override def toBytes(output:ByteBuf):Unit =
    validate();result.toBytes(output)
  override def fromBytes(input:ByteBuf):Unit =
    val decoded=new S2CDurableRecallStatus();decoded.fromBytes(input)
    val checked=new S2CRecallResultReleased(decoded);checked.validate()
    result=decoded
