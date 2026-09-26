package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryStore}

import java.nio.file.Path
import java.util.UUID
import scala.util.{Failure, Try}

/** canonical marker publication 的平台边界；成功仅表示 adapter 已确认 publication。 */
private[server] trait CanonicalMarkerPublicationBarrier:
  def publish(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[Path]

private[server] object CanonicalMarkerPublicationBarrier:
  val unsupported: CanonicalMarkerPublicationBarrier = new CanonicalMarkerPublicationBarrier:
    override def publish(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[Path] =
      Failure(new UnsupportedOperationException("Canonical marker publication requires a platform durability acknowledgement adapter"))

/** 以调用方提供的固定 token 串联 world marker 与 physical recovery cutover。 */
final class WorldBindingCoordinator private[server] (
    physicalRecoveryStore: PhysicalRecoveryStore,
    canonicalMarkerPublicationBarrier: CanonicalMarkerPublicationBarrier
):
  require(physicalRecoveryStore != null, "physicalRecoveryStore must not be null")
  require(canonicalMarkerPublicationBarrier != null, "canonicalMarkerPublicationBarrier must not be null")

  /** 无平台 durability acknowledgement adapter 时默认 fail closed，且不发布 canonical marker。 */
  def this(physicalRecoveryStore: PhysicalRecoveryStore) = this(physicalRecoveryStore, CanonicalMarkerPublicationBarrier.unsupported)

  /** 不生成 token 或 world；依次保留 legacy binding、等待 publication acknowledgement、认证 binding 并验证固定身份。 */
  def prepareAndCertify(bindingUuid: UUID, formatVersion: Int, canonicalMarker: Path): Try[ValidatedWorldBinding] =
    physicalRecoveryStore.findPhysicalRecoveryBinding().flatMap {
      case None => prepareLegacyAndCertify(bindingUuid, formatVersion, canonicalMarker)
      case Some(existing) if existing.bindingUuid != bindingUuid || existing.formatVersion != formatVersion =>
        Failure(new IllegalStateException("Physical recovery binding conflicts with the supplied world binding token"))
      case Some(existing) if existing.cutoverState == PhysicalRecoveryCutoverState.Certified =>
        WorldBinding.validate(existing, canonicalMarker)
      case Some(_: PhysicalRecoveryBinding) => prepareLegacyAndCertify(bindingUuid, formatVersion, canonicalMarker)
    }

  private def prepareLegacyAndCertify(
      bindingUuid: UUID,
      formatVersion: Int,
      canonicalMarker: Path
  ): Try[ValidatedWorldBinding] =
    for
      legacyBinding <- physicalRecoveryStore.createOrRetainLegacyUncertifiedBinding(bindingUuid, formatVersion)
      marker <- canonicalMarkerPublicationBarrier.publish(legacyBinding, canonicalMarker)
      certifiedBinding <- physicalRecoveryStore.certifyPhysicalRecoveryBinding(bindingUuid, formatVersion)
      validatedBinding <- WorldBinding.validate(certifiedBinding, marker)
    yield validatedBinding
