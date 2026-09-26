package com.wi.floatingpod.server

import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import net.minecraftforge.common.MinecraftForge
import net.minecraftforge.event.world.WorldEvent
import net.minecraftforge.fml.common.eventhandler.{EventPriority, SubscribeEvent}
import net.minecraftforge.fml.common.gameevent.TickEvent

/** 將 Forge 全域 bus 限制在此一可替換邊界，避免測試變更全域 listener。 */
private[floatingpod] trait ForgeRuntimeRecoveryEventBus:
  def register(listener: AnyRef): Unit
  def unregister(listener: AnyRef): Unit

private object ForgeRuntimeRecoveryEventBus:
  val minecraftForge: ForgeRuntimeRecoveryEventBus = new ForgeRuntimeRecoveryEventBus:
    override def register(listener: AnyRef): Unit = MinecraftForge.EVENT_BUS.register(listener)
    override def unregister(listener: AnyRef): Unit = MinecraftForge.EVENT_BUS.unregister(listener)

/** Forge/Cleanroom 的 deferred recovery event adapter；只轉發既有服務端事件，絕不主動載入世界。 */
private[floatingpod] final class ForgeRuntimeRecoveryEvents(
    ticketManager: ChunkLoadingManager,
    eventBus: ForgeRuntimeRecoveryEventBus = ForgeRuntimeRecoveryEventBus.minecraftForge
) extends RuntimeRecoveryEvents:
  require(ticketManager != null, "ticketManager must not be null")
  require(eventBus != null, "eventBus must not be null")

  private var callback: ServerRecoveryTickPhase => Unit = null

  /** 先保留 callback，再進入外部 bus；即使 bus side effect 後丟出，也能由 unregister 清理。 */
  override def register(nextCallback: ServerRecoveryTickPhase => Unit): Unit = synchronized {
    require(nextCallback != null, "recovery callback must not be null")
    if callback != null then throw new IllegalStateException("Recovery callback is already registered")
    callback = nextCallback
    eventBus.register(this)
  }

  /** 清理至多呼叫一次外部 bus；無論其結果均清除 callback，使重試保持冪等。 */
  override def unregister(): Unit = synchronized {
    if callback == null then return
    try eventBus.unregister(this)
    finally callback = null
  }

  /** HIGHEST 的 Forge ticket callback 後，以 NORMAL 觀察已載入的 server world。 */
  @SubscribeEvent(priority = EventPriority.NORMAL)
  def onWorldLoad(event: WorldEvent.Load): Unit = event.getWorld match
    case world: WorldServer => observeServerWorldLoaded(world.provider.getDimension)
    case _ => ()

  /** callback 仍活躍時才記錄既有 WorldServer 的 exact dimension，絕不 resolve 或載入世界。 */
  private[floatingpod] def observeServerWorldLoaded(dimensionId: Int): Unit = synchronized {
    if callback != null then ticketManager.markWorldTicketPassCompleted(dimensionId)
  }

  /** HIGHEST handlers 已先處理的 tick phase 逐一映射，保留 Forge 到達順序。 */
  @SubscribeEvent(priority = EventPriority.NORMAL)
  def onServerTick(event: TickEvent.ServerTickEvent): Unit =
    val recoveryPhase = event.phase match
      case TickEvent.Phase.START => ServerRecoveryTickPhase.Start
      case TickEvent.Phase.END => ServerRecoveryTickPhase.End
    synchronized { if callback != null then callback(recoveryPhase) }

/** 僅向目前 server 請求 shutdown；保存、flush 與 durability 仍屬既有 lifecycle 邊界。 */
private[floatingpod] final class MinecraftServerControlledStop(serverSupplier: () => MinecraftServer) extends RuntimeControlledStop:
  require(serverSupplier != null, "serverSupplier must not be null")

  override def requestStop(): Unit =
    val server = Option(serverSupplier()).getOrElse(throw new IllegalStateException("Minecraft server is unavailable for controlled stop"))
    server.initiateShutdown()
