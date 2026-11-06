package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.WindowsWorldProvisioner
import java.nio.file.Path

/** 固定来源计划DATA；仅真实Session/actor/native检查后才取得观察，不授恢复或写权。 */
final case class MySqlCertifiedRestoreRequest(
    requestedWorldPath: Path, plan: WindowsWorldProvisioner.Plan,
    boot: WindowsWorldProvisioner.BootIdentity, controlledMetadata: MySqlFreshControlledMetadata):
  require(requestedWorldPath != null && requestedWorldPath.isAbsolute && requestedWorldPath.getParent != null &&
    requestedWorldPath.getFileName != null && plan != null && boot != null && controlledMetadata != null,
    "explicit existing world/source/boot and independently controlled metadata required")
  require(!controlledMetadata.metadataPath.normalize().startsWith(requestedWorldPath.normalize()),
    "controlled metadata must be external to the source world")

/** The registered actual recovery actor has exactly two purposes; neither accepts arbitrary DTO data. */
private[mysql] sealed trait CertifiedSourceRequest
private[mysql] object CertifiedSourceRequest:
  final case class Inspection(value: MySqlCertifiedRestoreRequest) extends CertifiedSourceRequest:
    require(value != null, "original certified inspection request required")
  final case class Ordinary(value: MySqlOrdinaryCertifiedBootRequest) extends CertifiedSourceRequest:
    require(value != null, "pinned ordinary Certified request required")

/** 只公开不可变责任事实；Throwable保持内部原始对象，不以toString输出秘密message。 */
final case class MySqlCertifiedInspectionObservation(
    status: String, sourceIdentity: Option[String], originalReceiptSha256: Option[String],
    originalRecordSequence: Option[Long], retainedNativeResponsibility: Boolean,
    originalRootRetained: Boolean, failure: Option[Throwable],
    witnessLengths: Vector[MySqlCertifiedWitnessLengths] = Vector.empty):
  override def toString: String = "MySqlCertifiedInspectionObservation[redacted; historical DATA only, not admission]"

final case class MySqlCertifiedWitnessLengths(contextBytes: Int, beforeFull20Bytes: Int, targetFull20Bytes: Int, envelopeBytes: Int)
