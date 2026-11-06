package com.wi.floatingpod.network

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{Decoded, QueryOriginalRequest, StructuralFailure}
import io.netty.buffer.ByteBuf
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

/** 仅承载原 FPDC 查询帧；认证、调度和原操作责任由服务端消费者承担。 */
final class C2SDeploymentOriginalQuery() extends IMessage:
  private var decodedRequest: Option[QueryOriginalRequest] = None

  def this(request: QueryOriginalRequest) =
    this()
    require(request != null, "query request must not be null")
    decodedRequest = Some(request)

  def request: QueryOriginalRequest =
    decodedRequest.getOrElse(throw new IllegalStateException("original query message is uninitialized"))

  override def toBytes(output: ByteBuf): Unit =
    require(output != null, "output must not be null")
    output.writeBytes(DeploymentCommandWireCodec.encodeRequest(request))

  override def fromBytes(input: ByteBuf): Unit =
    require(input != null, "input must not be null")
    val length = input.readableBytes()
    if length != 22 then
      throw new IllegalArgumentException(s"original query requires 22 readable bytes, got $length")
    val payload = new Array[Byte](length)
    input.readBytes(payload)
    val validated = DeploymentCommandWireCodec.decodeRequest(payload) match
      case decoded: Decoded[?] => decoded.value() match
        case query: QueryOriginalRequest => query
        case _ => throw new IllegalArgumentException("original query message rejects non-query request kind")
      case failure: StructuralFailure[?] =>
        throw new IllegalArgumentException(s"original query decode failed: ${failure.stage()}/${failure.code()}")
    // 完整验证后才发布值，失败不得覆盖先前有效消息或伪造默认查询。
    decodedRequest = Some(validated)
