package com.wi.floatingpod.server.spatial

import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.server.{PermanentMainPumpCore, PermanentPumpLease, PermanentPumpPhase}
import java.net.Proxy
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import net.minecraft.init.Bootstrap
import net.minecraft.profiler.Profiler
import net.minecraft.server.MinecraftServer
import net.minecraft.server.management.PlayerList
import net.minecraft.command.ServerCommandManager
import net.minecraft.util.datafix.DataFixer
import net.minecraft.world.{EnumDifficulty, GameType, WorldServer, WorldSettings, WorldType}
import net.minecraft.world.chunk.storage.AnvilSaveHandler
import net.minecraft.world.storage.WorldInfo
import net.minecraftforge.common.DimensionManager
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.parallel.ResourceLock

/** 正常构造 WorldServer 并填充其真实索引；不启动 tick、网络、区块生成或游戏实例。 */
@ResourceLock("forge-dimension-manager")
class MainSpatialIdentityRegistryTest:
  @TempDir var directory: Path = _

  @Test def keepsIdentityAcrossRealEndScopesAndReleasesRetiredBody(): Unit = withFixture { f =>
    val body = f.add(f.world)
    val first = f.end(s => f.registry.observe(s, f.world, body))
    f.end { scope =>
      assertSame(first, f.registry.observe(scope, f.world, body))
      f.registry.requireCurrent(scope, first)
    }
    assertEquals(f.lease.bootUuid, first.bootUuid)
    assertEquals(f.lease.bootGeneration, first.bootGeneration)
    assertEquals(body.getUniqueID, first.podUuid)
    assertEquals(body.getEntityId, first.entityId)
    f.registry.retireBody(body)
    val next = f.end(s => f.registry.observe(s, f.world, body))
    assertTrue(next.entityGeneration > first.entityGeneration)
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.requireCurrent(s, first)))
  }

  @Test def sameUuidReplacementGetsFreshGenerationAndOldCleanupCannotRetireIt(): Unit = withFixture { f =>
    val oldBody = f.add(f.world)
    val first = f.end(s => f.registry.observe(s, f.world, oldBody))
    f.remove(f.world, oldBody)
    val replacement = f.add(f.world, oldBody.getUniqueID)
    val next = f.end(s => f.registry.observe(s, f.world, replacement))
    assertEquals(first.podUuid, next.podUuid)
    assertTrue(next.entityGeneration > first.entityGeneration)
    f.registry.retireBody(oldBody)
    f.end(s => f.registry.requireCurrent(s, next))
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.requireCurrent(s, first)))
  }

  @Test def actualWorldReplacementChangesIncarnationAndInvalidatesOldIdentity(): Unit = withFixture { f =>
    val first = f.end(s => f.registry.observe(s, f.world, f.add(f.world)))
    val replacement = f.newWorld("replacement")
    val next = f.end(s => f.registry.observe(s, replacement, f.add(replacement)))
    assertEquals(first.dimension, next.dimension)
    assertNotEquals(first.worldIncarnation, next.worldIncarnation)
    f.registry.retireWorld(f.world)
    f.end(s => f.registry.requireCurrent(s, next))
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.requireCurrent(s, first)))
  }

  @Test def rejectsUuidIndexThatPointsAtAnotherActualBody(): Unit = withFixture { f =>
    val body = f.add(f.world)
    f.add(f.world, body.getUniqueID)
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
  }

  @Test def rejectsEntityIdIndexThatPointsAtAnotherActualBody(): Unit = withFixture { f =>
    val body = f.add(f.world)
    val replacement = new FloatingPodEntity(f.world)
    replacement.setEntityId(body.getEntityId)
    f.world.loadedEntityList.add(replacement)
    f.world.onEntityAdded(replacement)
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
  }

  @Test def deadBodyInvalidatesIssuedIdentity(): Unit = withFixture { f =>
    val body = f.add(f.world)
    val identity = f.end(s => f.registry.observe(s, f.world, body))
    body.isDead = true
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.requireCurrent(s, identity)))
  }

  @Test def rejectsBodyMissingFromResidentListEvenWhenBothIndexesStillMatch(): Unit = withFixture { f =>
    val body = f.add(f.world)
    f.world.loadedEntityList.remove(body)
    assertSame(body, f.world.getEntityFromUuid(body.getUniqueID))
    assertSame(body, f.world.getEntityByID(body.getEntityId))
    assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
  }

  @Test def rejectsUnloadedWorldAndBodyWithWrongWorldReference(): Unit =
    withFixture { f =>
      val body = f.add(f.world)
      f.server.worlds = Array.empty[WorldServer]
      assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
    }
    withFixture { f =>
      val body = f.add(f.world)
      body.world = f.newWorld("different-body-world")
      f.server.worlds = Array(f.world)
      assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
    }

  @Test def expiredScopeAndOffMainCannotReadOrRetire(): Unit = withFixture { f =>
    val body = f.add(f.world)
    var expired: SpatialEndScope = null
    val identity = f.end { scope =>
      expired = scope
      val failure = new AtomicReference[Throwable]()
      val worker = new Thread(() =>
        try
          f.registry.observe(scope, f.world, body)
          ()
        catch case cause: Throwable => failure.set(cause))
      worker.start()
      worker.join()
      assertTrue(failure.get().isInstanceOf[IllegalStateException])
      f.registry.observe(scope, f.world, body)
    }
    assertThrows(classOf[IllegalArgumentException], () => f.registry.requireCurrent(expired, identity))
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() =>
      try f.registry.retireAll()
      catch case cause: Throwable => failure.set(cause))
    thread.start()
    thread.join()
    assertTrue(failure.get().isInstanceOf[IllegalStateException])
    f.end(s => f.registry.requireCurrent(s, identity))
  }

  @Test def foreignBootCannotBindRegistryAndRetireAllIsTerminal(): Unit =
    withFixture { f =>
      val foreign = new MainSpatialIdentityRegistry(f.server, f.foreignLease())
      val body = f.add(f.world)
      assertThrows(classOf[IllegalArgumentException], () => f.end(s => foreign.observe(s, f.world, body)))
    }
    withFixture { f =>
      val body = f.add(f.world)
      f.end(s => f.registry.observe(s, f.world, body))
      f.registry.retireAll()
      f.registry.retireAll()
      assertThrows(classOf[IllegalStateException], () => f.end(s => f.registry.observe(s, f.world, body)))
    }

  @Test def sameBootValuesOnAnotherCoreCannotObserveOrValidateOriginalIdentity(): Unit = withFixture { f =>
    assertForeignScopeRejected(f, f.server)
  }

  @Test def foreignServerOnSameMainWithSameBootValuesCannotObserveOrValidateOriginalIdentity(): Unit = withFixture { f =>
    val otherServer = new IdentityServer(directory.resolve("foreign-server"))
    assertSame(f.server.getServerThread, otherServer.getServerThread)
    assertForeignScopeRejected(f, otherServer)
  }

  private def assertForeignScopeRejected(f: Fixture, foreignServer: MinecraftServer): Unit =
    val body = f.add(f.world)
    val identity = f.end(scope => f.registry.observe(scope, f.world, body))
    // 两条路径分别使用未失败的真实 Core；world/body 始终是原 registry 的有效对象。
    for validateOnly <- List(false, true) do
      val core = new PermanentMainPumpCore(foreignServer, Thread.currentThread())
      var otherLease: PermanentPumpLease = null
      var entered = false
      otherLease = core.install(f.lease.bootUuid, f.lease.bootGeneration, tick =>
        core.withSpatialEndScope(otherLease, tick) { scope =>
          entered = true
          scope.consume { actual =>
            assertEquals(f.lease.bootUuid, actual.bootUuid)
            assertEquals(f.lease.bootGeneration, actual.bootGeneration)
            assertEquals(PermanentPumpPhase.Ready, actual.phase)
          }
          if validateOnly then f.registry.requireCurrent(scope, identity)
          else
            f.registry.observe(scope, f.world, body)
            ()
        })
      assertTrue(otherLease ne f.lease)
      core.changePhase(otherLease, PermanentPumpPhase.Ready)
      try
        core.start(foreignServer, 0)
        val failure = assertThrows(classOf[IllegalArgumentException], () => core.end(foreignServer, 1))
        assertTrue(failure.getMessage.contains("exact original consumer lease"))
        assertTrue(entered)
      finally core.retire(otherLease)
    // 拒绝外来 scope 不应退役原 registry，原 boot 的下一 END 仍可消费同一身份。
    f.end(scope => f.registry.requireCurrent(scope, identity))

  private def withFixture(body: Fixture => Unit): Unit =
    val fixture = new Fixture(directory)
    try body(fixture)
    finally fixture.close()

  private final class Fixture(directory: Path):
    Bootstrap.register()
    val server = new IdentityServer(directory)
    private val previous = DimensionManager.getWorld(0)
    private val previousWorlds = server.worlds
    private var action: SpatialEndScope => Unit = _ => ()
    private var counter = 0
    private val core = new PermanentMainPumpCore(server, Thread.currentThread())
    val lease: PermanentPumpLease = core.install(UUID.randomUUID(), 1L, tick =>
      core.withSpatialEndScope(lease, tick)(scope => action(scope)))
    core.changePhase(lease, PermanentPumpPhase.Ready)
    val registry = new MainSpatialIdentityRegistry(server, lease)
    val world: WorldServer =
      try newWorld("initial")
      catch
        case cause: Throwable =>
          try restoreWorld()
          catch case cleanup: Throwable => cause.addSuppressed(cleanup)
          throw cause

    def newWorld(name: String): WorldServer =
      val settings = new WorldSettings(1L, GameType.SURVIVAL, false, false, WorldType.DEFAULT)
      new WorldServer(server, new AnvilSaveHandler(directory.toFile, name, false, new DataFixer(1343)),
        new WorldInfo(settings, name), 0, new Profiler())

    def add(world: WorldServer, uuid: UUID = UUID.randomUUID()): FloatingPodEntity =
      val entity = new FloatingPodEntity(world)
      entity.setUniqueId(uuid)
      world.loadedEntityList.add(entity)
      world.onEntityAdded(entity)
      entity

    def remove(world: WorldServer, body: FloatingPodEntity): Unit =
      world.loadedEntityList.remove(body)
      world.onEntityRemoved(body)

    def end[A](consumer: SpatialEndScope => A): A =
      var result: Option[A] = None
      action = scope => result = Some(consumer(scope))
      try
        core.start(server, counter)
        counter += 1
        core.end(server, counter)
        result.get
      finally action = _ => ()

    def foreignLease(): PermanentPumpLease =
      new PermanentMainPumpCore(server, Thread.currentThread()).install(UUID.randomUUID(), 2L, _ => ())

    private def restoreWorld(): Unit =
      val owner = if previous == null then server else previous.getMinecraftServer
      DimensionManager.setWorld(0, previous, owner)
      server.worlds = previousWorlds

    def close(): Unit =
      try registry.retireAll()
      finally
        try core.retire(lease)
        finally restoreWorld()

  /** 只提供构造所需环境；主线程归属固定，未启用的运行入口明确失败。 */
  private final class IdentityServer(directory: Path)
      extends MinecraftServer(directory.toFile, Proxy.NO_PROXY, new DataFixer(1343), null, null, null, null):
    private val mainThread = Thread.currentThread()
    setPlayerList(new PlayerList(this) {})
    override def getServerThread: Thread = mainThread
    override def isCallingFromMinecraftThread: Boolean = Thread.currentThread() eq mainThread
    override def createCommandManager(): ServerCommandManager = null
    override def init(): Boolean = throw new UnsupportedOperationException("Identity test cannot start server")
    override def shareToLAN(gameType: GameType, allowCheats: Boolean): String =
      throw new UnsupportedOperationException("Identity test cannot open LAN")
    override def canStructuresSpawn(): Boolean = false
    override def getGameType(): GameType = GameType.SURVIVAL
    override def getDifficulty(): EnumDifficulty = EnumDifficulty.NORMAL
    override def isHardcore(): Boolean = false
    override def getOpPermissionLevel(): Int = 0
    override def shouldBroadcastRconToOps(): Boolean = false
    override def shouldBroadcastConsoleToOps(): Boolean = false
    override def isDedicatedServer(): Boolean = false
    override def shouldUseNativeTransport(): Boolean = false
    override def isCommandBlockEnabled(): Boolean = false
