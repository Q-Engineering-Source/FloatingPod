package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import java.util.UUID

/** 集中协议字段的固定编码，让每个封包以相同方式拒绝不兼容或残留数据。 */
private[network] object ProtocolCodec:
  /** 当前封包payload版本；v2新增Busy拒绝，旧客户端不能误解为已接受处理中。 */
  final val Version: Int = PacketHandler.PROTOCOL_VERSION

  /** 写入每个 payload 必须具备的协议版本前缀。 */
  def writeHeader(output: ByteBuf): Unit = output.writeByte(Version)

  /** 仅以 0 与 1 表示协议布尔值，拒绝 Netty 默认接受的其他非零字节。 */
  def writeCanonicalBoolean(output: ByteBuf, value: Boolean): Unit = output.writeByte(if value then 1 else 0)

  /** 读取唯一合法的协议布尔值，任何其他字节均为格式错误。 */
  def readCanonicalBoolean(input: ByteBuf): Boolean = input.readUnsignedByte() match
    case 0 => false
    case 1 => true
    case received => throw new IllegalArgumentException(s"Non-canonical protocol boolean $received")

  /** 读取并验证协议版本，版本漂移一律拒绝而非尝试兼容解码。 */
  def readHeader(input: ByteBuf): Unit =
    val received = input.readUnsignedByte()
    if received != Version then throw new IllegalArgumentException(s"Unsupported protocol version $received")

  /** 以两个 long 保留 UUID 的完整位表示。 */
  def writeUuid(output: ByteBuf, value: UUID): Unit =
    require(value != null, "uuid must not be null")
    output.writeLong(value.getMostSignificantBits)
    output.writeLong(value.getLeastSignificantBits)

  /** 从完整 UUID 位表示重建稳定标识。 */
  def readUuid(input: ByteBuf): UUID = new UUID(input.readLong(), input.readLong())

  /** 以 presence boolean 后接固定 long 表示 optional state version。 */
  def writeOptionalLong(output: ByteBuf, value: Option[Long]): Unit =
    require(value != null, "optional value must not be null")
    writeCanonicalBoolean(output, value.nonEmpty)
    value.foreach(output.writeLong)

  /** 读取 presence boolean 后的 optional state version。 */
  def readOptionalLong(input: ByteBuf): Option[Long] =
    if readCanonicalBoolean(input) then Some(input.readLong()) else None

  /** 以 presence boolean 后接单一 wire id 表示 optional bounded failure reason。 */
  def writeOptionalReason[A <: FailureReason](output: ByteBuf, value: Option[A]): Unit =
    require(value != null, "optional value must not be null")
    writeCanonicalBoolean(output, value.nonEmpty)
    value.foreach(reason => output.writeByte(reason.wireId))

  /** 读取 presence boolean 后的 bounded failure reason，未知代码由 decoder 拒绝。 */
  def readOptionalReason[A](input: ByteBuf, decode: Int => A): Option[A] =
    if readCanonicalBoolean(input) then Some(decode(input.readUnsignedByte())) else None

  /** 所有 decoder 都必须消耗完 payload，避免把串接数据误认为合法消息。 */
  def requireFullyConsumed(input: ByteBuf): Unit =
    if input.isReadable then throw new IllegalArgumentException("Packet payload contains trailing bytes")
