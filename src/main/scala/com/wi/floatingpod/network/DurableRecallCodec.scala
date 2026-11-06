package com.wi.floatingpod.network

import com.wi.floatingpod.domain.RecallRequestNamespace
import io.netty.buffer.ByteBuf
import java.util.UUID

/** 独立durable消息族格式；不更改旧封包的payload版本或discriminator。 */
private[network] object DurableRecallCodec:
  private val Format=1
  def writeHeader(output:ByteBuf):Unit =
    require(output!=null,"durable recall output required")
    ProtocolCodec.writeHeader(output);output.writeByte(Format)
  def readHeader(input:ByteBuf):Unit =
    require(input!=null,"durable recall input required")
    ProtocolCodec.readHeader(input)
    require(input.readUnsignedByte()==Format,"unsupported durable recall format")
  def writeNamespace(output:ByteBuf,namespace:RecallRequestNamespace):Unit =
    require(output!=null && namespace!=null,"durable recall namespace required")
    writeHeader(output)
    ProtocolCodec.writeUuid(output,namespace.bindingUuid);ProtocolCodec.writeUuid(output,namespace.actorUuid)
  def readNamespace(input:ByteBuf):RecallRequestNamespace =
    readHeader(input)
    RecallRequestNamespace(ProtocolCodec.readUuid(input),ProtocolCodec.readUuid(input))
  def requireId(id:UUID):Unit =
    require(id!=null && id!=new UUID(0L,0L),"nonzero durable recall identity required")
