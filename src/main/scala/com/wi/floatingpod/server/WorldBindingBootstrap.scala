package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryStore}

import net.minecraftforge.fml.common.event.FMLServerAboutToStartEvent

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.{Objects, UUID}
import scala.util.Try

/**
 * 生产平台的 canonical marker publication acknowledgement adapter。
 *
 * `CanonicalMarkerPublicationBarrier.unsupported` 缺失的正是这一层：把 publication 委派给
 * `WorldBinding.createOrRetainMarker` 已有的耐久路径（CREATE_NEW 临时文件 + force(true) + ATOMIC_MOVE + 读回校验）。
 * 该 seam 只发布调用端提供的 binding 身份，不生成 UUID，也不改动 binding 的 cutover state。
 */
private[server] object FileSystemCanonicalMarkerPublicationBarrier extends CanonicalMarkerPublicationBarrier:
  override def publish(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[Path] =
    WorldBinding.createOrRetainMarker(binding, canonicalMarker)

/** 首次启动时为本世界建立并认证一次 physical recovery binding 的唯一生产入口。 */
object WorldBindingBootstrap:
  /** 与 `WorldBinding` 固定 24-byte marker wire 格式一致的初始版本；既有测试亦使用 1。 */
  val InitialFormatVersion = 1

  /**
   * binding token 由 server folder name 确定性派生（RFC 4122 v3），因此同一 world folder 在任意次重启后
   * 都重放同一 token，不会触发 "Physical recovery binding conflicts with the supplied world binding token"。
   * 每个服务器实例拥有独立的 `config/floatingpod/floatingpod.db`，folder name 已足以唯一标识其 world，
   * 且比绝对路径更能容忍服务器目录搬迁，因此不引入新配置项。
   */
  def bindingTokenFor(folderName: String): UUID =
    UUID.nameUUIDFromBytes(Objects.requireNonNull(folderName, "server folder name must not be null").getBytes(StandardCharsets.UTF_8))

  /**
   * 仅当本世界尚无 binding 时，为其建立并认证一次 cutover；已有 binding 时完全不介入，
   * 交由 `CertifiedWorldBindingGate` 保持原有严格只读语义。
   */
  def ensureCertifiedBinding(physicalRecoveryStore: PhysicalRecoveryStore, event: FMLServerAboutToStartEvent): Unit =
    Objects.requireNonNull(physicalRecoveryStore, "physicalRecoveryStore must not be null")
    val serverEvent = Objects.requireNonNull(event, "event must not be null")
    if physicalRecoveryStore.findPhysicalRecoveryBinding().get.isEmpty then
      val server = Objects.requireNonNull(serverEvent.getServer, "event server must not be null")
      val canonicalMarker = CertifiedWorldBindingGate.productionCanonicalMarker(serverEvent)
      new WorldBindingCoordinator(physicalRecoveryStore, FileSystemCanonicalMarkerPublicationBarrier)
        .prepareAndCertify(bindingTokenFor(server.getFolderName), InitialFormatVersion, canonicalMarker)
        .get
    ()
