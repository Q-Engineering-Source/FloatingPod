package com.wi.floatingpod.client

import com.wi.floatingpod.network.*
import java.util.UUID
import net.minecraft.client.Minecraft
import net.minecraft.util.text.TextComponentTranslation
import org.apache.logging.log4j.LogManager
import scala.util.control.NonFatal

/** 实际客户端Main上的网络/显示装配；公共network类不解析Minecraft客户端类型。 */
object ClientRecallRequests:
  private val logger = LogManager.getLogger(getClass)
  private val controller = new ClientRecallController(() => System.nanoTime(), PacketHandler.sendRecallOperation)
  private var installed = false
  private def actual(): Minecraft =
    val client = Minecraft.getMinecraft()
    require(client.isCallingFromMinecraftThread, "actual client Main required")
    client
  def install(): Unit =
    actual(); require(!installed, "client recall consumer once")
    ClientNetworkState.installRecallStatusConsumer(receive)
    installed = true
  /** 供终端/GUI使用的普通发送入口；本轮不制造缺失的终端页面或测试手势。 */
  def request(pod: UUID, expectedVersion: Long): Boolean =
    val client = actual()
    require(installed && client.player != null && client.getConnection != null, "live client recall session required")
    val submitted = try controller.submit(client.getConnection, pod, expectedVersion) catch
      case NonFatal(cause) =>
        logger.error("Client recall send failed for pod " + pod + " expected version " + expectedVersion, cause)
        client.player.sendStatusMessage(new TextComponentTranslation("floatingpod.recall.retained_failure"), true)
        throw cause
    val key = if submitted then "waiting" else controller.view(pod) match
      case Some(view) if view.phase.contains(RecallOperationPhase.RetainedFailure) => "retained_failure"
      case Some(view) if !view.completed => "processing"
      case _ => "buffered"
    client.player.sendStatusMessage(new TextComponentTranslation("floatingpod.recall." + key), true)
    submitted
  def view(pod: UUID): Option[ClientRecallController.View] =
    val client = actual()
    if client.getConnection == null then controller.disconnect()
    else controller.bindConnection(client.getConnection)
    controller.view(pod)
  private def receive(status: S2CRecallOperationStatus): Unit =
    val client = actual()
    if client.getConnection == null || client.player == null then
      controller.disconnect()
      return
    val observed = try controller.receive(client.getConnection, status) catch
      case NonFatal(cause) =>
        logger.error("Client recall status rejected for request " + status.requestUuid + " pod " + status.podUuid, cause)
        client.player.sendStatusMessage(new TextComponentTranslation("floatingpod.recall.retained_failure"), true)
        return
    observed.foreach { view =>
      val key = view.phase.get match
        case RecallOperationPhase.Processing => "processing"
        case RecallOperationPhase.Committed => "committed"
        case RecallOperationPhase.Refused => "refused"
        case RecallOperationPhase.RetainedFailure => "retained_failure"
      client.player.sendStatusMessage(new TextComponentTranslation("floatingpod.recall." + key), true)
    }
