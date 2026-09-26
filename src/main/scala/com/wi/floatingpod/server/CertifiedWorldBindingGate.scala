package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryCutoverState, PhysicalRecoveryStore}

import net.minecraftforge.fml.common.event.FMLServerAboutToStartEvent

import java.nio.file.{Files, LinkOption, Path}
import java.util.Objects

/** AboutToStart 只读认证 gate；仅接受已有的已认证 world binding。 */
final class CertifiedWorldBindingGate(
    physicalRecoveryStore: PhysicalRecoveryStore,
    canonicalMarkerResolver: FMLServerAboutToStartEvent => Path = CertifiedWorldBindingGate.productionCanonicalMarker
):
  require(physicalRecoveryStore != null, "physicalRecoveryStore must not be null")
  require(canonicalMarkerResolver != null, "canonicalMarkerResolver must not be null")

  /** 读取既有认证 binding 与其 canonical marker；任一缺失或不匹配均阻止启动继续。 */
  def validate(event: FMLServerAboutToStartEvent): ValidatedWorldBinding =
    Objects.requireNonNull(event, "event must not be null")
    val binding = physicalRecoveryStore.findPhysicalRecoveryBinding().get.getOrElse(
      throw new IllegalStateException("Certified physical recovery binding is required")
    )
    if binding.cutoverState != PhysicalRecoveryCutoverState.Certified then
      throw new IllegalStateException("Physical recovery binding must be certified before server startup")

    val marker = Objects.requireNonNull(canonicalMarkerResolver(event), "canonical marker resolver must not return null").toAbsolutePath.normalize
    val parent = Option(marker.getParent).getOrElse(
      throw new IllegalArgumentException(s"Canonical world marker must have an existing parent directory: $marker")
    )
    if Files.isSymbolicLink(parent) || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) then
      throw new IllegalStateException(s"Canonical world marker parent is not an existing regular directory: $parent")

    WorldBinding.validate(binding, marker).get

object CertifiedWorldBindingGate:
  private val MarkerPath = "data/floatingpod-world-binding.bin"

  /** 从当前 FML server 的 active save converter 读取 canonical marker 路径，不创建文件系统对象。 */
  def productionCanonicalMarker(event: FMLServerAboutToStartEvent): Path =
    Objects.requireNonNull(event, "event must not be null")
    val server = Objects.requireNonNull(event.getServer, "event server must not be null")
    val folderName = Objects.requireNonNull(server.getFolderName, "server folder name must not be null")
    if folderName.trim.isEmpty then throw new IllegalArgumentException("server folder name must not be blank")
    Objects.requireNonNull(server.getActiveAnvilConverter, "active anvil converter must not be null")
    Objects.requireNonNull(
      event.getServer.getActiveAnvilConverter.getFile(event.getServer.getFolderName, MarkerPath),
      "canonical marker file must not be null"
    ).toPath
