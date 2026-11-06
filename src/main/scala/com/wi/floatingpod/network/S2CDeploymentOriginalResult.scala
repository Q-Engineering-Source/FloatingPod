package com.wi.floatingpod.network

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{Decoded, QueryOriginalResult, StructuralFailure}
import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 原 FPDC 查询结果的 Forge 载体；不生成回执或把失败转换成 NOT_FOUND。 */
final class S2CDeploymentOriginalResult() extends IMessage:
  private var decodedResult: Option[QueryOriginalResult] = None

  def this(result: QueryOriginalResult) =
    this()
    require(result != null, "query result must not be null")
    decodedResult = Some(result)

  def result: QueryOriginalResult =
    decodedResult.getOrElse(throw new IllegalStateException("original result message is uninitialized"))

  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    output.writeBytes(DeploymentCommandWireCodec.encodeResponse(result))

  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    val length = input.readableBytes()
    if length != 23 && length != 143 then
      throw new IllegalArgumentException(s"original result requires 23 or 143 readable bytes, got $length")
    val payload = new Array[Byte](length)
    input.readBytes(payload)
    val validated = DeploymentCommandWireCodec.decodeResponse(payload) match
      case decoded: Decoded[?] => decoded.value() match
        case query: QueryOriginalResult => query
        case _ => throw new IllegalArgumentException("original result message rejects non-query response kind")
      case failure: StructuralFailure[?] =>
        throw new IllegalArgumentException(s"original result decode failed: ${failure.stage()}/${failure.code()}")
    // 保留完整原结果；不以部分解码值覆盖此前已验证的结果。
    decodedResult = Some(validated)
