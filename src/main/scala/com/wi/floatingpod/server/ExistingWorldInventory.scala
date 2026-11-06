package com.wi.floatingpod.server

import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.storage.{PhysicalRecoveryChunk, PhysicalRecoveryLocator}

import net.minecraft.server.MinecraftServer
import net.minecraft.world.chunk.Chunk
import net.minecraftforge.common.DimensionManager

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** 已加载 chunk 中观察到的一个 FloatingPod 实体；同 UUID 的多次观察保持为独立条目。 */
private[server] final case class ExistingWorldInventoryObservation(podUuid: UUID, dimensionId: Int, chunkX: Int, chunkZ: Int, entityId: Int):
  require(podUuid != null, "podUuid must not be null")

/** 一个当前已加载 chunk 的完整 FloatingPod 观察快照。 */
private[server] final case class ExistingWorldInventoryLoadedChunk(dimensionId: Int, chunkX: Int, chunkZ: Int, observations: List[ExistingWorldInventoryObservation]):
  require(observations != null && observations.forall(_ != null), "observations must not contain null")

/** locator 所需 chunk 与本次安全加载到的 chunk；不完整只表示 unknown，绝不表示 absence。 */
private[server] final case class ExistingWorldInventoryRequiredChunks(podUuid: UUID, requiredChunks: List[PhysicalRecoveryChunk], loadedChunks: List[PhysicalRecoveryChunk]):
  require(podUuid != null && requiredChunks != null && loadedChunks != null, "required chunk values must not be null")
  def complete: Boolean = requiredChunks == loadedChunks

/** 已生成 locator chunk 的安全加载结果和所有当前 loaded-world FloatingPod observations。 */
private[server] final case class ExistingWorldInventoryReport(requiredChunks: List[ExistingWorldInventoryRequiredChunks], observations: List[ExistingWorldInventoryObservation]):
  require(requiredChunks != null && observations != null, "inventory report values must not be null")

/** 隔离 Forge/Cleanroom world 与 chunk APIs；语义层不接触生成路径。 */
private[server] trait ExistingWorldInventoryProbe:
  def loadIfAlreadyGenerated(chunk: PhysicalRecoveryChunk): Boolean
  def loadedChunks(): List[ExistingWorldInventoryLoadedChunk]

/** 对既有物理 world 的只读/非生成 inventory 检查；不裁决 absence。 */
private[server] final class ExistingWorldInventory(probe: ExistingWorldInventoryProbe):
  require(probe != null, "probe must not be null")

  def inspect(requiredLocators: List[PhysicalRecoveryLocator]): Try[ExistingWorldInventoryReport] = Try {
    require(requiredLocators != null && requiredLocators.forall(_ != null), "requiredLocators must not contain null")
    validateUniquePods(requiredLocators)
    val required = requiredLocators.sortBy(_.podUuid.toString).map { locator =>
      val chunks = validateExactLocator(locator)
      ExistingWorldInventoryRequiredChunks(locator.podUuid, chunks, chunks.filter(probe.loadIfAlreadyGenerated))
    }
    ExistingWorldInventoryReport(required, probe.loadedChunks().flatMap(_.observations))
  }

  private def validateUniquePods(locators: List[PhysicalRecoveryLocator]): Unit =
    if locators.map(_.podUuid).distinct.size != locators.size then
      throw new IllegalArgumentException("required locators must have unique pod UUIDs")

  private def validateExactLocator(locator: PhysicalRecoveryLocator): List[PhysicalRecoveryChunk] =
    val targetDimension = locator.targetDimensionId.getOrElse(throw new IllegalArgumentException(s"locator ${locator.podUuid} has no target dimension"))
    val chunks = locator.chunks.toSet
    val chunkXs = chunks.map(_.chunkX).toList.sorted
    val chunkZs = chunks.map(_.chunkZ).toList.sorted
    val exact = (for chunkX <- chunkXs; chunkZ <- chunkZs yield PhysicalRecoveryChunk(targetDimension, chunkX, chunkZ)).toSet
    if locator.chunks.size != 25 || chunks.size != 25 || !chunks.forall(_.dimensionId == targetDimension) || !consecutiveFive(chunkXs) || !consecutiveFive(chunkZs) || chunks != exact then
      throw new IllegalArgumentException(s"locator ${locator.podUuid} does not contain an exact target-dimension 5x5 chunk square")
    chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))

  private def consecutiveFive(coordinates: List[Int]): Boolean = coordinates match
    case first :: second :: third :: fourth :: fifth :: Nil => second == first + 1 && third == second + 1 && fourth == third + 1 && fifth == fourth + 1
    case _ => false

private[server] object ExistingWorldInventory:
  def forServer(server: MinecraftServer): ExistingWorldInventory =
    require(server != null, "server must not be null")
    new ExistingWorldInventory(new ForgeExistingWorldInventoryProbe(server))

  /** 延遲取得當前 server；construction 不讀取 supplier，probe 前缺失 server 直接失敗。 */
  def withServerSupplier(serverSupplier: () => MinecraftServer): ExistingWorldInventory =
    require(serverSupplier != null, "server supplier must not be null")
    new ExistingWorldInventory(new ExistingWorldInventoryProbe:
      private def currentProbe(): ForgeExistingWorldInventoryProbe =
        new ForgeExistingWorldInventoryProbe(Option(serverSupplier()).getOrElse(throw new IllegalStateException("Minecraft server is unavailable for world inventory")))
      override def loadIfAlreadyGenerated(chunk: PhysicalRecoveryChunk): Boolean = currentProbe().loadIfAlreadyGenerated(chunk)
      override def loadedChunks(): List[ExistingWorldInventoryLoadedChunk] = currentProbe().loadedChunks()
    )

private final class ForgeExistingWorldInventoryProbe(server: MinecraftServer) extends ExistingWorldInventoryProbe:
  override def loadIfAlreadyGenerated(chunk: PhysicalRecoveryChunk): Boolean =
    val world = Option(server.getWorld(chunk.dimensionId))
    world.exists { resolved =>
      val provider = resolved.getChunkProvider
      if !provider.isChunkGeneratedAt(chunk.chunkX, chunk.chunkZ) then false
      else
        val loaded = provider.loadChunk(chunk.chunkX, chunk.chunkZ)
        loaded != null && loaded.x == chunk.chunkX && loaded.z == chunk.chunkZ
    }

  override def loadedChunks(): List[ExistingWorldInventoryLoadedChunk] =
    DimensionManager.getWorlds().toList.flatMap { world =>
      world.getChunkProvider.getLoadedChunks.asScala.toList.map(snapshotChunk(world.provider.getDimension, _))
    }

  private def snapshotChunk(dimensionId: Int, chunk: Chunk): ExistingWorldInventoryLoadedChunk =
    val observations = chunk.getEntityLists.toList.flatMap(_.asScala.collect {
      case entity: FloatingPodEntity => ExistingWorldInventoryObservation(entity.sourcePodUuid(), dimensionId, chunk.x, chunk.z, entity.getEntityId)
    })
    ExistingWorldInventoryLoadedChunk(dimensionId, chunk.x, chunk.z, observations)
