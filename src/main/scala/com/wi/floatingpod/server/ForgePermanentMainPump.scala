package com.wi.floatingpod.server

import java.util.UUID
import com.wi.floatingpod.server.spatial.SpatialEndScope
import net.minecraft.server.MinecraftServer
import net.minecraftforge.common.MinecraftForge
import net.minecraftforge.fml.common.eventhandler.{EventPriority, SubscribeEvent}
import net.minecraftforge.fml.common.gameevent.TickEvent

private[floatingpod] trait PermanentPumpEventBus:
  def register(listener: AnyRef): Unit
  def unregister(listener: AnyRef): Unit

private[floatingpod] object PermanentPumpEventBus:
  val forge: PermanentPumpEventBus = new PermanentPumpEventBus:
    override def register(listener: AnyRef): Unit = MinecraftForge.EVENT_BUS.register(listener)
    override def unregister(listener: AnyRef): Unit = MinecraftForge.EVENT_BUS.unregister(listener)

/** 注册失败仍提供 exact lease，调用方可显式 retire 保留的 bus 责任。 */
private[floatingpod] final class PermanentPumpRegistrationFailure(
    val lease: PermanentPumpLease,
    cause: Throwable
) extends IllegalStateException("Permanent pump bus registration failed", cause)

/** 与 deferred recovery adapter 分离；Ready 不撤销此 listener。尚未装配 runtime。 */
private[floatingpod] final class ForgePermanentMainPump(
    server: MinecraftServer,
    serverSupplier: () => MinecraftServer,
    eventBus: PermanentPumpEventBus = PermanentPumpEventBus.forge
):
  require(server != null && serverSupplier != null && eventBus != null, "permanent pump dependencies are required")
  private val mainThread = Thread.currentThread()
  if !server.isCallingFromMinecraftThread then throw new IllegalStateException("Permanent pump must be constructed on server Main")
  private val core = new PermanentMainPumpCore(server, mainThread)
  private val registration = new PermanentPumpRegistration(core, this, eventBus)

  private def actualServer(): MinecraftServer =
    core.assertMain()
    val actual = serverSupplier()
    if actual == null || (actual ne server) || !server.isCallingFromMinecraftThread then
      throw new IllegalStateException("Permanent pump actual server/Main identity changed")
    actual

  def register(boot: UUID, generation: Long, callback: PermanentEndTick => Unit): PermanentPumpLease =
    actualServer()
    registration.register(boot, generation, callback)

  def changePhase(lease: PermanentPumpLease, phase: PermanentPumpPhase): Unit =
    actualServer()
    core.changePhase(lease, phase)

  private[server] def assertReadyCurrent(lease: PermanentPumpLease): Unit =
    actualServer()
    core.assertReadyCurrent(lease)

  private[server] def assertRecoveringDispatch(lease:PermanentPumpLease,tick:PermanentEndTick):Unit =
    actualServer()
    core.assertRecoveringDispatch(lease,tick)

  private[server] def withSpatialEndScope[A](lease: PermanentPumpLease, tick: PermanentEndTick)(
      consumer: SpatialEndScope => A
  ): A =
    actualServer()
    core.withSpatialEndScope(lease, tick)(consumer)

  /** 每个 exact lease 的 unregister 至多一次；失败保留原 cause，禁止假清理或再注册。 */
  def retire(lease: PermanentPumpLease): Boolean =
    actualServer()
    registration.retire(lease)

  @SubscribeEvent(priority = EventPriority.NORMAL)
  def onServerTick(event: TickEvent.ServerTickEvent): Unit =
    if event == null then throw new IllegalArgumentException("ServerTickEvent is required")
    val actual = actualServer()
    // FML ServerTickEvent has no server field; only this Main-bound supplier is authoritative.
    event.phase match
      case TickEvent.Phase.START => core.start(actual, actual.getTickCounter)
      case TickEvent.Phase.END => core.end(actual, actual.getTickCounter)

/** 可直接验证 bus 副作用/异常责任；所有调用仍由同一个 Main 独占。 */
private[floatingpod] final class PermanentPumpRegistration(
    core: PermanentMainPumpCore,
    listener: AnyRef,
    eventBus: PermanentPumpEventBus
):
  require(core != null && listener != null && eventBus != null, "registration dependencies are required")
  private var registeredLease: PermanentPumpLease = null
  private var registrationAttempted = false
  private var unregisterAttempted = false
  private var busFailure: Throwable = null

  def register(boot: UUID, generation: Long, callback: PermanentEndTick => Unit): PermanentPumpLease =
    core.assertMain()
    if busFailure != null then throw new IllegalStateException("Permanent pump bus responsibility is unresolved", busFailure)
    if registeredLease != null then throw new IllegalStateException("Permanent pump listener already owns a lease")
    val lease = core.install(boot, generation, callback)
    registeredLease = lease
    registrationAttempted = true
    unregisterAttempted = false
    try eventBus.register(listener)
    catch case cause: Throwable =>
      busFailure = cause
      core.fail(cause)
      throw new PermanentPumpRegistrationFailure(lease, cause)
    lease

  def unresolvedBusFailure: Option[Throwable] =
    core.assertMain()
    Option(busFailure)

  def retire(lease: PermanentPumpLease): Boolean =
    core.assertMain()
    if !core.isCurrent(lease) then return false
    core.retire(lease)
    if registrationAttempted && !unregisterAttempted then
      unregisterAttempted = true
      try eventBus.unregister(listener)
      catch case cause: Throwable =>
        if busFailure != null && (busFailure ne cause) then cause.addSuppressed(busFailure)
        busFailure = cause
        throw cause
    registeredLease = null
    registrationAttempted = false
    busFailure = null
    true
