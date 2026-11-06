package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.storage.{
  AdmissionOutcome,
  DeactivationOutcome,
  ObservedUnboundPod,
  ObservedUnboundUniquenessReceipt,
  OperatingModeChangeOutcome,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryLocator,
  RecoveryDimensionBackfillOutcome,
  StoredPodRow,
  UnboundCommitOutcome,
  UnboundPreparationReceipt
}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/** 一次成功的 typed repository helper 转换；该步已通过 single-step delta guards。 */
private[mysql] final case class MySqlRepositoryStagedStep(
    operation: String,
    before: MySqlFullRepositorySnapshot,
    after: MySqlFullRepositorySnapshot,
    delta: MySqlFullRepositoryDelta
):
  require(operation != null && operation.nonEmpty, "staged transition operation is required")
  require(before != null && after != null && delta != null, "staged transition snapshots and delta are required")
  require(!before.exactlyEquals(after), "staged transition must change the complete repository graph")
  require(delta.before.exactlyEquals(before) && delta.after.exactlyEquals(after), "staged transition delta must bind its exact before/after snapshots")

/** 有序的 helper 转换链；最终效果仍是完整 initial→final 图。 */
private[mysql] final case class MySqlRepositoryStagedPlan(
    initial: MySqlFullRepositorySnapshot,
    finalSnapshot: MySqlFullRepositorySnapshot,
    steps: Vector[MySqlRepositoryStagedStep]
):
  require(initial != null && finalSnapshot != null && steps != null && steps.forall(_ != null), "staged repository plan is required")

  def validate(): Unit =
    initial.validate()
    var current = initial
    steps.foreach { step =>
      if !current.exactlyEquals(step.before) then
        throw new java.sql.SQLTransactionRollbackException("staged repository transitions are not an ordered exact snapshot chain")
      MySqlFullRepositoryDelta.freeze(step.before, step.after)
      current = step.after
    }
    if !current.exactlyEquals(finalSnapshot) then
      throw new java.sql.SQLTransactionRollbackException("staged repository transition chain does not produce the final snapshot")
    finalSnapshot.validate()
    if steps.isEmpty && !initial.exactlyEquals(finalSnapshot) then
      throw new java.sql.SQLTransactionRollbackException("staged repository transition count disagrees with the final graph")

/** owned IO 线程中的暂存边界；闭包只改不可变 snapshot，不执行任何 JDBC DML。 */
private[mysql] final class MySqlRepositoryStaging(initial: MySqlFullRepositorySnapshot, ownerThread: Thread):
  require(initial != null && ownerThread != null, "staging snapshot and owner thread are required")
  initial.validate()
  private var current = initial
  private val transitions = ArrayBuffer.empty[MySqlRepositoryStagedStep]
  private var open = true
  private var firstFailure: Throwable = null

  def snapshot: MySqlFullRepositorySnapshot =
    ensureOpen()
    current

  def changed: Boolean =
    ensureOpen()
    !initial.exactlyEquals(current)

  def failure: Option[Throwable] = Option(firstFailure)

  def insert(state: FloatingPodState): Try[FloatingPodState] =
    execute("insert")(snapshot => MySqlRepositoryTransitionRules.insert(snapshot, state))

  def update(state: FloatingPodState): Try[FloatingPodState] =
    execute("update")(snapshot => MySqlRepositoryTransitionRules.update(snapshot, state))

  def delete(uuid: UUID): Try[Boolean] =
    execute("delete")(snapshot => MySqlRepositoryTransitionRules.delete(snapshot, uuid))

  def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] =
    observe("findByUuid")(snapshot => MySqlRepositoryTransitionRules.findPod(snapshot, uuid).map(MySqlStoredRows.materializeGenericState))

  def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] =
    observe("findByOwner") { snapshot =>
      val key = MySqlStoredRows.encodeUuid(ownerUuid)
      snapshot.pods.filter(_.ownerUuidBytes.contains(key)).sortBy(row => java.util.HexFormat.of().formatHex(row.uuidBytes.toArray))
        .map(MySqlStoredRows.materializeGenericState).toList
    }

  def findAllActive(): Try[List[FloatingPodState]] =
    observe("findAllActive")(_.pods.filter(_.isActiveFlag == 1)
      .sortBy(row => java.util.HexFormat.of().formatHex(row.uuidBytes.toArray))
      .map(MySqlStoredRows.materializeGenericState).toList)

  def findAll(): Try[List[FloatingPodState]] =
    observe("findAll")(_.pods.sortBy(row => java.util.HexFormat.of().formatHex(row.uuidBytes.toArray))
      .map(MySqlStoredRows.materializeGenericState).toList)

  def findAllRecoverable(): Try[List[FloatingPodState]] =
    observe("findAllRecoverable")(_.pods.sortBy(row => java.util.HexFormat.of().formatHex(row.uuidBytes.toArray))
      .map(MySqlStoredRows.materializeGenericState).toList)

  def findDesiredActiveRows(): Try[List[StoredPodRow]] =
    observe("findDesiredActiveRows")(_.pods.filter(_.isActiveFlag == 1)
      .sortBy(row => java.util.HexFormat.of().formatHex(row.uuidBytes.toArray))
      .map(MySqlStoredRows.materializeRawRow).toList)

  def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] =
    observe("findStoredByUuid")(snapshot => MySqlRepositoryTransitionRules.findPod(snapshot, uuid).map(MySqlStoredRows.materializeRawRow))

  def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] =
    observe("findByPlayerUuid")(_.entitlements.find(_.playerUuid == playerUuid))

  def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] =
    observe("findPhysicalRecoveryBinding")(_.recovery.binding)

  def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] =
    observe("findPhysicalRecoveryLocator")(snapshot => MySqlRepositoryTransitionRules.findLocator(snapshot, podUuid))

  def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] =
    observe("findAllPhysicalRecoveryLocators")(_.recovery.locators.sortBy(row =>
      java.util.HexFormat.of().formatHex(MySqlStoredRows.encodeUuid(row.podUuid).toArray)).toList)

  def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] =
    observe("findUnboundPreparation")(snapshot => MySqlUnboundTransitionRules.findPreparation(snapshot, podUuid))

  def prepareCreateLocator(
      podUuid: UUID,
      expectedDomainVersion: Long,
      attemptUuid: UUID,
      targetDimensionId: Int,
      chunks: Set[PhysicalRecoveryChunk]
  ): Try[PhysicalRecoveryLocator] =
    execute("prepareCreateLocator")(snapshot =>
      MySqlRepositoryTransitionRules.prepareCreateLocator(snapshot, podUuid, expectedDomainVersion, attemptUuid, targetDimensionId, chunks))

  def activatePreparedIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      attemptUuid: UUID
  ): Try[AdmissionOutcome] =
    execute("activatePreparedIfAdmissible")(snapshot =>
      MySqlRepositoryTransitionRules.activate(snapshot, target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit, attemptUuid))

  def activatePreparedUsingStoredAttempt(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): Try[AdmissionOutcome] =
    execute("activatePreparedUsingStoredAttempt")(snapshot =>
      MySqlRepositoryTransitionRules.activateUsingPreparedLocator(
        snapshot, target, authenticatedOwner, expectedStateVersion, dimensionId, globalActiveLimit))

  def deactivateIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Try[DeactivationOutcome] =
    execute("deactivateIfExpectedActive")(snapshot =>
      MySqlRepositoryTransitionRules.deactivate(snapshot, podUuid, expectedOwner, expectedStateVersion, None))

  def deactivateTrackedIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[DeactivationOutcome] =
    execute("deactivateTrackedIfExpectedActive")(snapshot =>
      MySqlRepositoryTransitionRules.deactivate(snapshot, podUuid, expectedOwner, expectedStateVersion, Some(attemptUuid)))

  def updateTrackedActiveDrift(state: FloatingPodState, attemptUuid: UUID): Try[FloatingPodState] =
    execute("updateTrackedActiveDrift")(snapshot => MySqlRepositoryTransitionRules.trackedDrift(snapshot, state, attemptUuid))

  def setOperatingModeTrackedIfOwned(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): Try[OperatingModeChangeOutcome] =
    execute("setOperatingModeTrackedIfOwned")(snapshot =>
      MySqlRepositoryTransitionRules.setMode(snapshot, expectedBinding, podUuid, authenticatedOwner,
        expectedStateVersion, attemptUuid, targetMode, actualPose))

  def verifyInactiveAbsence(podUuid: UUID, expectedStateVersion: Long, attemptUuid: UUID): Try[PhysicalRecoveryLocator] =
    execute("verifyInactiveAbsence")(snapshot =>
      MySqlRepositoryTransitionRules.verifyInactiveAbsence(snapshot, podUuid, expectedStateVersion, attemptUuid))

  def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, dimensionId: Int): Try[RecoveryDimensionBackfillOutcome] =
    execute("backfillMissingActiveDimension")(snapshot =>
      MySqlRepositoryTransitionRules.backfillDimension(snapshot, uuid, expectedStateVersion, dimensionId))

  def prepareObservedUnbound(
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessReceipt: ObservedUnboundUniquenessReceipt
  ): Try[UnboundPreparationReceipt] =
    execute("prepareObservedUnbound")(snapshot =>
      MySqlUnboundTransitionRules.prepareObserved(snapshot, observation, expectedBinding, attemptUuid, uniquenessReceipt))

  def prepareUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): Try[UnboundPreparationReceipt] =
    execute("prepareUnboundCreate")(snapshot =>
      MySqlUnboundTransitionRules.prepareCreate(snapshot, expectedBinding, podUuid, attemptUuid, targetPose, operationUuid))

  def commitObservedUnbound(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    execute("commitObservedUnbound")(snapshot =>
      MySqlUnboundTransitionRules.commitObserved(snapshot, expectedBinding, podUuid, attemptUuid,
        expectedPreparedVersion, actualPose, globalActiveLimit))

  def commitUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    execute("commitUnboundCreate")(snapshot =>
      MySqlUnboundTransitionRules.commitCreate(snapshot, expectedBinding, podUuid, attemptUuid,
        expectedPreparedVersion, actualPose, operationUuid, globalActiveLimit))

  def execute[A](operation: String)(body: MySqlFullRepositorySnapshot => (MySqlFullRepositorySnapshot, A)): Try[A] =
    if Thread.currentThread() ne ownerThread then
      val error = new IllegalStateException("MySQL staged repository is confined to its owned IO thread")
      markFailure(error)
      Failure(error)
    else if !open then
      val error = new IllegalStateException("MySQL staged repository is closed")
      markFailure(error)
      Failure(error)
    else if firstFailure != null then Failure(firstFailure)
    else if operation == null || operation.trim.isEmpty || body == null then
      val error = new IllegalArgumentException("staged operation and body are required")
      markFailure(error)
      Failure(error)
    else
      try
        val before = current
        val (next, value) = body(before)
        if next == null then throw new IllegalStateException("staged operation produced a null snapshot")
        next.validate()
        if !before.exactlyEquals(next) then
          val delta = MySqlFullRepositoryDelta.freeze(before, next)
          transitions += MySqlRepositoryStagedStep(operation, before, next, delta)
        current = next
        Success(value)
      catch
        case error: Throwable =>
          markFailure(error)
          Failure(error)

  def observe[A](operation: String)(body: MySqlFullRepositorySnapshot => A): Try[A] =
    if Thread.currentThread() ne ownerThread then
      val error = new IllegalStateException("MySQL staged repository is confined to its owned IO thread")
      markFailure(error)
      Failure(error)
    else if !open then
      val error = new IllegalStateException("MySQL staged repository is closed")
      markFailure(error)
      Failure(error)
    else if firstFailure != null then Failure(firstFailure)
    else if operation == null || operation.trim.isEmpty || body == null then
      val error = new IllegalArgumentException("staged read and body are required")
      markFailure(error)
      Failure(error)
    else
      try Success(body(current))
      catch
        case error: Throwable =>
          markFailure(error)
          Failure(error)

  def closeAndFreeze(): Try[MySqlFullRepositorySnapshot] = closeAndFreezePlan().map(_.finalSnapshot)

  def closeAndFreezePlan(): Try[MySqlRepositoryStagedPlan] =
    if Thread.currentThread() ne ownerThread then
      val error = new IllegalStateException("MySQL staged repository is confined to its owned IO thread")
      markFailure(error)
      Failure(error)
    else if !open then Failure(new IllegalStateException("MySQL staged repository is already closed"))
    else
      open = false
      if firstFailure != null then Failure(firstFailure)
      else
        try
          val plan = MySqlRepositoryStagedPlan(initial, current, transitions.toVector)
          plan.validate()
          Success(plan)
        catch
          case error: Throwable =>
            markFailure(error)
            Failure(error)

  def abort(): Unit =
    if Thread.currentThread() ne ownerThread then
      val error = new IllegalStateException("MySQL staged repository abort requires its owned IO thread")
      markFailure(error)
      throw error
    open = false

  private def ensureOpen(): Unit =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("MySQL staged repository is confined to its owned IO thread")
    if !open then throw new IllegalStateException("MySQL staged repository is closed")

  private def markFailure(error: Throwable): Unit =
    if firstFailure == null then firstFailure = error
    else if (firstFailure ne error) && !firstFailure.getSuppressed.contains(error) then firstFailure.addSuppressed(error)
