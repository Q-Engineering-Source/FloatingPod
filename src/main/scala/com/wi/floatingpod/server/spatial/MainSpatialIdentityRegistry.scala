package com.wi.floatingpod.server.spatial

import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.server.{PermanentEndTick, PermanentPumpLease, PermanentPumpPhase}
import java.util.{IdentityHashMap, UUID}
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import scala.jdk.CollectionConverters.*

private[floatingpod] object MainSpatialIdentityRegistry:
  /** 仅标识观察到的实体世代；不授予业务准入，也不持有 world/entity 引用。 */
  final class Identity private[MainSpatialIdentityRegistry] (
      val bootUuid: UUID,
      val bootGeneration: Long,
      val dimension: Int,
      val worldIncarnation: Long,
      val podUuid: UUID,
      val entityId: Int,
      val entityGeneration: Long
  )

/** Main 独占的 live identity 表；生命周期 owner 必须在卸载/关闭时调用 retire。 */
private[floatingpod] final class MainSpatialIdentityRegistry(server: MinecraftServer, lease: PermanentPumpLease):
  import MainSpatialIdentityRegistry.Identity
  require(server != null && lease != null, "actual server and original pump lease are required")

  private final class WorldEntry(val incarnation: Long, val dimension: Int):
    val bodies = new IdentityHashMap[FloatingPodEntity, Identity]()

  private val worlds = new IdentityHashMap[WorldServer, WorldEntry]()
  private var worldSequence = 0L
  private var bodySequence = 0L
  private var retired = false

  private def assertMain(): Unit =
    if (Thread.currentThread() ne server.getServerThread) || !server.isCallingFromMinecraftThread then
      throw new IllegalStateException("Spatial identity registry requires actual server Main")

  private def checkTick(tick: PermanentEndTick): Unit =
    assertMain()
    if retired then throw new IllegalStateException("Spatial identity registry is retired")
    if tick.bootUuid != lease.bootUuid || tick.bootGeneration != lease.bootGeneration ||
        tick.phase != PermanentPumpPhase.Ready then
      throw new IllegalStateException("Spatial identity registry requires its original Ready boot")

  private def loaded(world: WorldServer): Boolean =
    if world == null || world.isRemote || (world.getMinecraftServer ne server) || server.worlds == null then false
    else
      val dimension = world.provider.getDimension
      server.worlds.count(w => w != null && w.provider.getDimension == dimension) == 1 &&
        server.worlds.exists(_ eq world)

  private def live(world: WorldServer, body: FloatingPodEntity): Boolean =
    body != null && !body.isDead && (body.world eq world) &&
      body.getUniqueID != null &&
      (world.getEntityFromUuid(body.getUniqueID) eq body) &&
      (world.getEntityByID(body.getEntityId) eq body) &&
      world.loadedEntityList.asScala.exists(_ eq body)

  /** 不保存历史引用；替换或卸载在下一次消费时摘除，显式 retire 可更早释放。 */
  private def prune(): Unit =
    val it = worlds.entrySet.iterator()
    while it.hasNext do
      val entry = it.next()
      val world = entry.getKey
      if !loaded(world) || world.provider.getDimension != entry.getValue.dimension then it.remove()
      else
        val bodies = entry.getValue.bodies.entrySet.iterator()
        while bodies.hasNext do
          val body = bodies.next()
          val identity = body.getValue
          if !live(world, body.getKey) || body.getKey.getUniqueID != identity.podUuid ||
              body.getKey.getEntityId != identity.entityId then bodies.remove()

  def observe(scope: SpatialEndScope, world: WorldServer, body: FloatingPodEntity): Identity =
    require(scope != null, "original END scope is required")
    scope.consumeFor(lease) { tick =>
      checkTick(tick)
      prune()
      if !loaded(world) || !live(world, body) then
        throw new IllegalStateException("Spatial identity requires loaded world and resident dual-indexed live body")
      var entry = worlds.get(world)
      if entry == null then
        worldSequence = Math.incrementExact(worldSequence)
        entry = new WorldEntry(worldSequence, world.provider.getDimension)
        worlds.put(world, entry)
      var identity = entry.bodies.get(body)
      if identity == null then
        bodySequence = Math.incrementExact(bodySequence)
        identity = new Identity(tick.bootUuid, tick.bootGeneration, entry.dimension,
          entry.incarnation, body.getUniqueID, body.getEntityId, bodySequence)
        entry.bodies.put(body, identity)
      identity
    }

  def requireCurrent(scope: SpatialEndScope, identity: Identity): Unit =
    require(scope != null && identity != null, "original END scope and issued identity are required")
    scope.consumeFor(lease) { tick =>
      checkTick(tick)
      prune()
      if !worlds.values.asScala.exists(_.bodies.values.asScala.exists(_ eq identity)) then
        throw new IllegalStateException("Spatial identity is no longer current")
    }

  /** 精确引用退役，延迟到达的旧实体清理不能撤销替换实体。 */
  def retireBody(body: FloatingPodEntity): Unit =
    assertMain()
    require(body != null, "actual body is required")
    worlds.values.asScala.foreach(_.bodies.remove(body))

  def retireWorld(world: WorldServer): Unit =
    assertMain()
    require(world != null, "actual world is required")
    worlds.remove(world)
    ()

  def retireAll(): Unit =
    assertMain()
    worlds.clear()
    retired = true
