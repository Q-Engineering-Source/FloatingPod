package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import net.minecraftforge.fml.common.network.simpleimpl.IMessage

import java.lang.{Double as JavaDouble, Float as JavaFloat}
import java.util.UUID

/** 向客户端同步活动 FloatingPod 的连续位置、朝向与可供下次请求使用的状态版本。 */
final class S2CStateSync(
    var podUuid: UUID,
    var x: Double,
    var y: Double,
    var z: Double,
    var yaw: Float,
    var pitch: Float,
    var stateVersion: Long
) extends IMessage:
  /** 为 Forge 反序列化保留的公开无参构造器。 */
  def this() = this(null, 0.0D, 0.0D, 0.0D, 0.0F, 0.0F, -1L)

  /** 写入不含所有权与模组资料的状态快照。 */
  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    requireValid()
    ProtocolCodec.writeHeader(output)
    ProtocolCodec.writeUuid(output, podUuid)
    output.writeDouble(x)
    output.writeDouble(y)
    output.writeDouble(z)
    output.writeFloat(yaw)
    output.writeFloat(pitch)
    output.writeLong(stateVersion)

  /** 读取完整快照，拒绝非有限数值、负版本与未消耗的输入。 */
  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    ProtocolCodec.readHeader(input)
    val decodedUuid = ProtocolCodec.readUuid(input)
    val decodedX = input.readDouble()
    val decodedY = input.readDouble()
    val decodedZ = input.readDouble()
    val decodedYaw = input.readFloat()
    val decodedPitch = input.readFloat()
    val decodedVersion = input.readLong()
    validate(decodedUuid, decodedX, decodedY, decodedZ, decodedYaw, decodedPitch, decodedVersion)
    ProtocolCodec.requireFullyConsumed(input)
    podUuid = decodedUuid
    x = decodedX
    y = decodedY
    z = decodedZ
    yaw = decodedYaw
    pitch = decodedPitch
    stateVersion = decodedVersion

  private def requireValid(): Unit = validate(podUuid, x, y, z, yaw, pitch, stateVersion)

  private def validate(
      candidateUuid: UUID,
      candidateX: Double,
      candidateY: Double,
      candidateZ: Double,
      candidateYaw: Float,
      candidatePitch: Float,
      candidateVersion: Long
  ): Unit =
    require(candidateUuid != null, "podUuid must not be null")
    require(JavaDouble.isFinite(candidateX), "x must be finite")
    require(JavaDouble.isFinite(candidateY), "y must be finite")
    require(JavaDouble.isFinite(candidateZ), "z must be finite")
    require(JavaFloat.isFinite(candidateYaw), "yaw must be finite")
    require(JavaFloat.isFinite(candidatePitch), "pitch must be finite")
    require(candidateVersion >= 0L, "stateVersion must not be negative")

object S2CStateSync:
  /** 以完整状态建立可直接传送的同步封包。 */
  def apply(
      podUuid: UUID,
      x: Double,
      y: Double,
      z: Double,
      yaw: Float,
      pitch: Float,
      stateVersion: Long
  ): S2CStateSync = new S2CStateSync(podUuid, x, y, z, yaw, pitch, stateVersion)
