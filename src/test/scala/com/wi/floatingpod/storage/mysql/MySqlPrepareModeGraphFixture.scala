package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.*
import java.time.LocalDateTime
import java.util.UUID

/** actual pure转换生成raw图；没有JDBC/session成功注入，不是SQL/physical/runtime验收。 */
object MySqlPrepareModeGraphFixture:
  def id(value: Long): UUID = new UUID(0L, value)
  val binding = PhysicalRecoveryBinding(id(20), PhysicalRecoveryCutoverState.Certified, 2)
  val pose = FloatingPodPose("prepared-mode-world", 8D, 64D, 8D, 0F, 0F, 7)
  val tag = IoOperationTag(id(1), 1L, 1L, id(2))
  val admission = MySqlOwnedDatabaseAdmission(tag.bootId, 1L, 1L, binding.bindingUuid, 4L)
  val owner = id(30)
  def empty: MySqlFullRepositorySnapshot =
    val raw = MySqlFullRepositorySnapshot(Vector.empty, Vector(PlayerEntitlement(owner)),
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector.empty), Vector.empty)
    val unrelated = FloatingPodState(id(99), Some(owner), 0L, pose.worldName, 32D, 64D, 32D,
      0F, 0F, false, 1L, Some(7), PodOperatingMode.Normal)
    MySqlRepositoryTransitionRules.insert(raw, unrelated)._1
  def inventory(snapshot: MySqlFullRepositorySnapshot): MySqlRecoverySnapshot =
    val now = LocalDateTime.of(2026, 10, 7, 12, 0)
    val manifest = MySqlSnapshotManifest(1, "MYSQL", 2, Vector.fill(32)(1.toByte), id(3), "READY", id(4),
      binding.bindingUuid, None, "NONE", None, None, None, Some(Vector.fill(32)(2.toByte)))
    val writer = MySqlSnapshotWriter(1, binding.bindingUuid, 4L, Some(tag.bootId), now.plusMinutes(5),
      Some(tag.operationId), Some(Vector.fill(32)(3.toByte)))
    MySqlRecoverySnapshot(tag.bootId, 1L, 1L, manifest, writer, snapshot.pods, snapshot.entitlements,
      snapshot.recovery, snapshot.bootstrapIntents, now, now, "A" * 64,
      MySqlRecoveryObservationState.CertifiedSchemaObserved, MySqlObservedWriterLease.OwnedUnexpired)
  final case class Prepared(request: MySqlControlledUnboundPrepareRequest,
      snapshot: MySqlFullRepositorySnapshot, receipt: UnboundPreparationReceipt):
    def graph: MySqlPreparedUnboundGraph = MySqlPreparedUnboundGraph.fromReadback(tag, request, receipt,
      inventory(snapshot), admission, 256)
  def prepared(): Prepared =
    val request = MySqlControlledUnboundPrepareRequest(binding, id(40), id(41), pose, id(42))
    val (snapshot, receipt) = MySqlUnboundTransitionRules.prepareCreate(empty, binding,
      request.proposedPodUuid, request.proposedAttemptUuid, pose, request.createOperationUuid)
    Prepared(request, snapshot, receipt)
  def committedPreparation(): Prepared =
    val old = prepared()
    val (committed, _) = MySqlUnboundTransitionRules.commitCreate(old.snapshot, binding, old.receipt.podUuid,
      old.receipt.attemptUuid, old.receipt.preparedVersion, pose, old.request.createOperationUuid, 256)
    val (same, receipt) = MySqlUnboundTransitionRules.prepareCreate(committed, binding, old.receipt.podUuid,
      id(43), pose, old.request.createOperationUuid)
    Prepared(old.request.copy(proposedAttemptUuid = id(43)), same, receipt)
  final case class Mode(request: MySqlOwnedModeCommitRequest, snapshot: MySqlFullRepositorySnapshot,
      outcome: OperatingModeChangeOutcome):
    def graph: MySqlCommittedOwnedModeGraph = MySqlCommittedOwnedModeGraph.fromReadback(tag, request,
      outcome, inventory(snapshot), admission, 256)
    def state: FloatingPodState = outcome match
      case OperatingModeChangeOutcome.Changed(value) => value
      case OperatingModeChangeOutcome.AlreadySet(value) => value
      case _ => throw new IllegalStateException("fixture has rejected mode outcome")
  def activeOwned: (MySqlFullRepositorySnapshot, FloatingPodState, UUID) =
    val inactive = FloatingPodState(id(50), Some(owner), 0L, pose.worldName, pose.posX, pose.posY, pose.posZ,
      pose.yaw, pose.pitch, false, 1L, Some(7), PodOperatingMode.Normal)
    val inserted = MySqlRepositoryTransitionRules.insert(empty, inactive)._1
    val attempt = id(51)
    val prepared = MySqlRepositoryTransitionRules.prepareCreateLocator(inserted, inactive.uuid, 0L,
      attempt, 7, MySqlRepositoryTransitionRules.chunksAt(7, pose.posX, pose.posZ))._1
    val (active, outcome) = MySqlRepositoryTransitionRules.activate(prepared, inactive, owner, 0L, 7, 256, attempt)
    val state = outcome match
      case AdmissionOutcome.Accepted(value) => value
      case _ => throw new IllegalStateException("pure fixture did not activate")
    (active, state, attempt)
  def mode(target: PodOperatingMode = PodOperatingMode.WorldIdle): Mode =
    val (active, state, attempt) = activeOwned
    val request = MySqlOwnedModeCommitRequest(binding, state.uuid, owner, state.stateVersion, attempt, target, pose)
    val (changed, outcome) = MySqlRepositoryTransitionRules.setMode(active, binding, state.uuid, owner,
      state.stateVersion, attempt, target, pose)
    Mode(request, changed, outcome)
