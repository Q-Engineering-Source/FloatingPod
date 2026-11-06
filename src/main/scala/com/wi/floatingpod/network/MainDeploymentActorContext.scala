package com.wi.floatingpod.network

import net.minecraft.entity.player.EntityPlayerMP
import net.minecraft.network.NetHandlerPlayServer
import net.minecraft.server.MinecraftServer
import net.minecraftforge.fml.common.network.simpleimpl.{IMessage, MessageContext}

import java.util.UUID

/** 历史回执只绑定原认证连接，不把玩家当前世界或当前 Pod 所有权作为读取资格。 */
private[floatingpod] final class MainDeploymentActorContext private (
    private val originalPlayer: EntityPlayerMP,
    private val originalServer: MinecraftServer,
    private val originalConnection: NetHandlerPlayServer,
    private val originalActorUuid: UUID
):
  private def requireMain(): Unit =
    require(Thread.currentThread() eq originalServer.getServerThread, "original server Main required")
    require(originalServer.isCallingFromMinecraftThread, "original server Main required")

  def actorUuid: UUID =
    requireMain()
    originalActorUuid

  def requireCurrent(): Unit =
    requireMain()
    require(originalPlayer.getServer eq originalServer, "original player server changed")
    require(originalPlayer.getUniqueID == originalActorUuid, "original player UUID changed")
    require(originalPlayer.connection eq originalConnection, "original player connection changed")
    require(originalServer.getPlayerList.getPlayerByUUID(originalActorUuid) eq originalPlayer,
      "original player roster changed")

  def send(message: IMessage): Unit =
    require(message != null, "message must not be null")
    requireCurrent()
    PacketHandler.sendTo(message, originalPlayer)

private[floatingpod] object MainDeploymentActorContext:
  /** 仅由已排程到服务器 Main 的请求入口捕获；不接受 payload 中的 actor 或 player。 */
  def capture(context: MessageContext): MainDeploymentActorContext =
    require(context != null, "context must not be null")
    val handler = context.getServerHandler
    require(handler != null, "server handler must not be null")
    val player = handler.player
    require(player != null, "server player must not be null")
    val server = player.getServer
    require(server != null, "player server must not be null")
    require(Thread.currentThread() eq server.getServerThread, "original server Main required")
    require(server.isCallingFromMinecraftThread, "original server Main required")
    require(player.connection eq handler, "original player connection required")
    val actor = player.getUniqueID
    require(actor != null, "player UUID must not be null")
    require(server.getPlayerList.getPlayerByUUID(actor) eq player, "original player roster required")
    new MainDeploymentActorContext(player, server, handler, actor)
