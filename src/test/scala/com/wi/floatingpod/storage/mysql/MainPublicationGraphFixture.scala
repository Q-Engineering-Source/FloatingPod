package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState}
import com.wi.floatingpod.storage.*
import com.wi.floatingpod.server.io.IoOperationTag
import java.time.LocalDateTime
import java.util.UUID

/** 只用于production pure转换/validation和Main边界；不fake JDBC open，不称SQL/native/Ready。 */
object MainPublicationGraphFixture:
  def id(value: Long): UUID = new UUID(0L, value)
  final case class Data(tag: IoOperationTag, receipt: UnboundPreparationReceipt, outcome: UnboundCommitOutcome,
      inventory: MySqlRecoverySnapshot, admission: MySqlOwnedDatabaseAdmission):
    def graph: MySqlCommittedUnboundGraph =
      MySqlCommittedUnboundGraph.fromReadback(tag, receipt, outcome, inventory, admission, 256)
    def state: FloatingPodState = outcome match
      case UnboundCommitOutcome.Committed(value) => value
      case UnboundCommitOutcome.AlreadyCommitted(value) => value
      case _ => throw new IllegalStateException("fixture has no actual pure commit result")
  def create(seed: Long = 100L, pose: FloatingPodPose = FloatingPodPose("publication-world", 8D, 64D, 8D, 0F, 0F, 7)): Data =
    val binding = PhysicalRecoveryBinding(id(seed), PhysicalRecoveryCutoverState.Certified, 2)
    val empty = MySqlFullRepositorySnapshot(Vector.empty, Vector.empty,
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector.empty), Vector.empty)
    val (prepared, receipt) = MySqlUnboundTransitionRules.prepareCreate(empty, binding, id(seed + 1), id(seed + 2), pose, id(seed + 3))
    val (committed, outcome) = MySqlUnboundTransitionRules.commitCreate(prepared, binding, receipt.podUuid, receipt.attemptUuid,
      receipt.preparedVersion, pose, receipt.createOperationUuid.get, 256)
    val tag = IoOperationTag(id(1), 1L, 1L, id(seed + 4))
    val now = LocalDateTime.of(2026, 10, 7, 12, 0)
    val manifest = MySqlSnapshotManifest(1, "MYSQL", 2, Vector.fill(32)(1.toByte), id(2), "READY", id(3),
      binding.bindingUuid, None, "NONE", None, None, None, Some(Vector.fill(32)(2.toByte)))
    val writer = MySqlSnapshotWriter(1, binding.bindingUuid, 4L, Some(tag.bootId), now.plusMinutes(5), Some(tag.operationId), Some(Vector.fill(32)(3.toByte)))
    val inventory = MySqlRecoverySnapshot(tag.bootId, tag.bootGeneration, tag.connectionGeneration, manifest, writer,
      committed.pods, committed.entitlements, committed.recovery, committed.bootstrapIntents, now, now,
      "A" * 64, MySqlRecoveryObservationState.CertifiedSchemaObserved, MySqlObservedWriterLease.OwnedUnexpired)
    Data(tag, receipt, outcome, inventory, MySqlOwnedDatabaseAdmission(tag.bootId, 1L, 1L, binding.bindingUuid, 4L))
