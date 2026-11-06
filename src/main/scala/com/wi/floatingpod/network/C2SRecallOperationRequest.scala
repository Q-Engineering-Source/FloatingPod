package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf
import java.util.UUID
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 独立关联请求；不改变旧C2SRecallRequest及历史wire的字段/含义。 */
final class C2SRecallOperationRequest(var requestUuid: UUID, var podUuid: UUID,
    var expectedStateVersion: Long) extends IMessage:
  def this() = this(null, null, -1L)
  private def validate(): Unit =
    require(requestUuid != null && podUuid != null && expectedStateVersion >= 0L, "fixed correlated recall required")
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output required"); validate()
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeUuid(output, requestUuid)
    ProtocolCodec.writeUuid(output, podUuid)
    output.writeLong(expectedStateVersion)
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input required")
    ProtocolCodec.readHeader(input)
    val request = ProtocolCodec.readUuid(input)
    val pod = ProtocolCodec.readUuid(input)
    val version = input.readLong()
    require(version >= 0L, "non-negative expected version required")
    ProtocolCodec.requireFullyConsumed(input)
    requestUuid = request; podUuid = pod; expectedStateVersion = version
