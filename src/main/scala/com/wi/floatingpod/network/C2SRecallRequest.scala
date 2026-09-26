package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import net.minecraftforge.fml.common.network.simpleimpl.IMessage

import java.util.UUID

/** 请求服务端收回一个当前活动中的 FloatingPod。 */
final class C2SRecallRequest(var podUuid: UUID, var expectedStateVersion: Long) extends IMessage:
  /** 为 Forge 反序列化保留的公开无参构造器。 */
  def this() = this(null, -1L)

  /** 写入带协议版本的固定 UUID 与状态版本字段。 */
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeUuid(output, podUuid)
    output.writeLong(expectedStateVersion)

  /** 读取完整请求，不接纳负版本、截断或尾随数据。 */
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    ProtocolCodec.readHeader(input)
    val decodedUuid = ProtocolCodec.readUuid(input)
    val decodedVersion = input.readLong()
    if decodedVersion < 0L then throw new IllegalArgumentException("expectedStateVersion must not be negative")
    ProtocolCodec.requireFullyConsumed(input)
    podUuid = decodedUuid
    expectedStateVersion = decodedVersion

object C2SRecallRequest:
  /** 以完整字段建立可直接传送的 Recall 请求。 */
  def apply(podUuid: UUID, expectedStateVersion: Long): C2SRecallRequest =
    new C2SRecallRequest(podUuid, expectedStateVersion)
