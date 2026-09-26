package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import net.minecraft.util.math.BlockPos
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

import java.util.UUID

/** 请求服务端在指定方块位置释放一个未活动的 FloatingPod。 */
final class C2SReleaseRequest(
    var podUuid: UUID,
    var targetPos: BlockPos,
    var expectedOwnerVersion: Long
) extends IMessage:
  /** 为 Forge 反序列化保留的公开无参构造器。 */
  def this() = this(null, null, -1L)

  /** 依固定字段顺序写入请求，避免客户端自由格式字符串进入协议。 */
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    require(targetPos != null, "targetPos must not be null")
    require(expectedOwnerVersion >= 0L, "expectedOwnerVersion must not be negative")
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeUuid(output, podUuid)
    output.writeLong(targetPos.toLong)
    output.writeLong(expectedOwnerVersion)

  /** 从唯一受版本保护的二进制布局读取请求，并拒绝截断及尾随数据。 */
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    ProtocolCodec.readHeader(input)
    val decodedUuid = ProtocolCodec.readUuid(input)
    val decodedPosition = BlockPos.fromLong(input.readLong())
    val decodedVersion = input.readLong()
    if decodedVersion < 0L then throw new IllegalArgumentException("expectedOwnerVersion must not be negative")
    ProtocolCodec.requireFullyConsumed(input)
    podUuid = decodedUuid
    targetPos = decodedPosition
    expectedOwnerVersion = decodedVersion

object C2SReleaseRequest:
  /** 以完整字段建立可直接传送的 Release 请求。 */
  def apply(podUuid: UUID, targetPos: BlockPos, expectedOwnerVersion: Long): C2SReleaseRequest =
    new C2SReleaseRequest(podUuid, targetPos, expectedOwnerVersion)
