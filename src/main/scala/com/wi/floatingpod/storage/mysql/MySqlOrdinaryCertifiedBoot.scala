package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{ProductionBootstrapControl, ProductionOrdinarySourceApproval}
import com.wi.floatingpod.server.WindowsWorldProvisioner
import java.nio.file.Path
import java.util.UUID

/** Fixed ordinary boot input. It carries approved birth DATA and this launch's independent IDs only. */
final case class MySqlOrdinaryCertifiedBootRequest(
    requestedWorldPath: Path,
    approvedOriginalCreation: ProductionOrdinarySourceApproval.ApprovedOriginalCreation,
    boot: WindowsWorldProvisioner.BootIdentity,
    currentIdentity: ProductionBootstrapControl.OrdinaryIdentity,
    controlledMetadata: MySqlFreshControlledMetadata,
    runtimeContractSha256: String
):
  require(requestedWorldPath != null && requestedWorldPath.isAbsolute && requestedWorldPath.getParent != null &&
    requestedWorldPath.getFileName != null && approvedOriginalCreation != null && boot != null &&
    currentIdentity != null && controlledMetadata != null, "explicit ordinary source and current boot inputs required")
  require(requestedWorldPath.toAbsolutePath.normalize() == approvedOriginalCreation.worldPath(),
    "ordinary world path must equal the independently pinned original source path")
  require(boot.bootId() == currentIdentity.boot() && boot.generation() == currentIdentity.bootGeneration(),
    "ordinary BootIdentity must be the exact current control generation")
  require(boot.bootId() != approvedOriginalCreation.creationBootUuid() &&
    !Set(currentIdentity.boot(), currentIdentity.submitOperation(), currentIdentity.handoffAttempt(),
      currentIdentity.handoffOperation()).contains(approvedOriginalCreation.creationBootUuid()),
    "ordinary current IDs must not reuse original creation boot identity")
  require(controlledMetadata.approvedConfigurationSha256.equalsIgnoreCase(approvedOriginalCreation.configurationSha256()),
    "ordinary controlled metadata must retain the approved original configuration pin")
  require(runtimeContractSha256 != null && runtimeContractSha256.matches("[0-9A-F]{64}") &&
    runtimeContractSha256.equals(approvedOriginalCreation.runtimeContractSha256()),
    "ordinary runtime contract raw pin must equal approved birth source")
  override def toString: String = "MySqlOrdinaryCertifiedBootRequest[redacted; no native or SQL authority]"

/** Actual history record facts exposed to Main as immutable DATA, never as a private completion capability. */
final case class MySqlOrdinaryV5RecordFact(
    sequence: Long,
    containerSha256: String,
    nativeFileIdentity: String,
    referencedArtifactIdentities: Vector[String]
):
  require(sequence > 0 && containerSha256 != null && containerSha256.matches("[0-9A-F]{64}") &&
    nativeFileIdentity != null && nativeFileIdentity.nonEmpty && referencedArtifactIdentities != null &&
    referencedArtifactIdentities.forall(value => value != null && value.nonEmpty),
    "complete actual V5 record data required")

/** Immutable successful source/boot evidence. Private capabilities and raw JDBC/native handles are excluded. */
final case class MySqlOrdinaryFacts(
    originalCreation: ProductionOrdinarySourceApproval.ApprovedOriginalCreation,
    currentBootUuid: UUID,
    currentBootGeneration: Long,
    connectionGeneration: Long,
    writerEpoch: Long,
    namespace: String,
    sourceWorldIdentity: String,
    guardFileIdentity: String,
    receiptFileIdentity: String,
    anchorFileIdentity: String,
    actualReceiptSha256: String,
    actualAnchorSha256: String,
    recordFacts: Vector[MySqlOrdinaryV5RecordFact]
):
  require(originalCreation != null && currentBootUuid != null && currentBootGeneration > 0 &&
    connectionGeneration > 0 && writerEpoch > 0 && namespace != null && namespace.nonEmpty &&
    sourceWorldIdentity != null && guardFileIdentity != null && receiptFileIdentity != null &&
    anchorFileIdentity != null && actualReceiptSha256 != null && actualReceiptSha256.matches("[0-9A-F]{64}") &&
    actualAnchorSha256 != null && actualAnchorSha256.matches("[0-9A-F]{64}") && recordFacts != null && recordFacts.nonEmpty,
    "completed ordinary source and settled V5 facts required")
  override def toString: String = "MySqlOrdinaryFacts[redacted; immutable source evidence only]"
