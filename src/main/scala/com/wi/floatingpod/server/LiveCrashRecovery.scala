package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{FloatingPodRepository, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore}

import java.util.UUID

/** deferred recovery 在單一時點讀取的 ticket callback 證據；production source 只包裝現有 manager 快照。 */
private[server] final case class LiveCrashRecoveryTicketEvidence(report: TicketRecoveryReport, cleanup: List[StartupTicketCleanupObservation]):
  require(report != null && cleanup != null && cleanup.forall(_ != null), "ticket evidence values must not be null")

/** 將 ticket callback 證據與 recovery 演算法隔離，測試不需要改寫 ChunkLoadingManager。 */
private[server] trait LiveCrashRecoveryTicketEvidenceSource:
  def snapshot(): LiveCrashRecoveryTicketEvidence

private[server] object LiveCrashRecoveryTicketEvidenceSource:
  def fromManager(tickets: ChunkLoadingManager): LiveCrashRecoveryTicketEvidenceSource =
    require(tickets != null, "ticket manager must not be null")
    new LiveCrashRecoveryTicketEvidenceSource:
      override def snapshot(): LiveCrashRecoveryTicketEvidence = LiveCrashRecoveryTicketEvidence(tickets.latestRecoveryReport(), tickets.startupTicketCleanupSnapshot())

private[server] final case class RecoveryFailure(reason: String, cause: Option[Throwable]):
  require(reason != null && reason.nonEmpty && cause != null, "recovery failure values must not be null or blank")

private[server] object RecoveryFailure:
  def blocked(reason: String): RecoveryFailure = RecoveryFailure(reason, None)
  def unexpected(reason: String, cause: Throwable): RecoveryFailure = RecoveryFailure(reason, Some(cause))

/** 對已提交 ROW_* locator 的服務端 deferred recovery；它只裁決本 boot，絕不自行發布 runtime。 */
private[server] final class LiveCrashRecovery private (
    legacyRepository: Option[FloatingPodRepository],
    tickets: ChunkLoadingManager,
    manager: ActiveFloatingPodManager,
    worlds: ServerWorldResolver,
    inventory: ExistingWorldInventory,
    suppliedTicketEvidence: LiveCrashRecoveryTicketEvidenceSource,
    bootReads: Option[MainBootProjection]
) extends DeferredRecovery:
  def this(repository: FloatingPodRepository, tickets: ChunkLoadingManager, manager: ActiveFloatingPodManager,
      worlds: ServerWorldResolver, inventory: ExistingWorldInventory,
      suppliedTicketEvidence: LiveCrashRecoveryTicketEvidenceSource = null) =
    this(Some(repository), tickets, manager, worlds, inventory, suppliedTicketEvidence, None)
  private def repository: FloatingPodRepository = legacyRepository.getOrElse(
    throw new IllegalStateException("claimed recovery cannot use synchronous repository"))
  import LiveCrashRecovery.{Phase, Snapshot, TicketEvidenceKey}

  require(tickets != null && manager != null && worlds != null && inventory != null, "live crash recovery dependencies must not be null")
  require(legacyRepository.forall(value => value != null && value.isInstanceOf[PhysicalRecoveryStore]), "live crash recovery requires a PhysicalRecoveryStore repository")

  private def store: PhysicalRecoveryStore = repository.asInstanceOf[PhysicalRecoveryStore]
  private val ticketEvidence = Option(suppliedTicketEvidence).getOrElse(LiveCrashRecoveryTicketEvidenceSource.fromManager(tickets))
  private var phase: Phase = Phase.New
  private var finishReport: Option[RecoveryFinishReport] = None

  override def begin(): RecoveryStep = synchronized {
    phase match
      case Phase.New =>
        capture() match
          case Right(snapshot) => phase = Phase.Begun(snapshot); RecoveryStep.AwaitingTick
          case Left(failure) => terminalFailure(failure)
      case _ => throw new IllegalStateException("LiveCrashRecovery.begin may only be called once")
  }

  override def advance(tickPhase: ServerRecoveryTickPhase): RecoveryStep = synchronized {
    require(tickPhase != null, "recovery tick phase must not be null")
    if finishReport.nonEmpty then throw new IllegalStateException("LiveCrashRecovery cannot advance after finish")
    phase match
      case Phase.Begun(plan) if tickPhase == ServerRecoveryTickPhase.Start =>
        capture().flatMap(current => requireSamePlan(plan, current).map(_ => current)) match
          case Left(failure) => terminalFailure(failure)
          case Right(current) =>
            remediatePending(current) match
              case Left(failure) => terminalFailure(failure)
              case Right(remediated) => phase = Phase.Started(plan, plan.bootRemediation ++ remediated); RecoveryStep.AwaitingTick
      case Phase.Started(plan, remediated) if tickPhase == ServerRecoveryTickPhase.End =>
        capture().flatMap(current => requireSamePlan(plan, current).map(_ => current)) match
          case Left(failure) => terminalFailure(failure)
          case Right(current) => completeEnd(plan, current, remediated ++ current.bootRemediation)
      case Phase.Begun(_) => throw new IllegalStateException("LiveCrashRecovery requires Start before End")
      case Phase.Started(_, _) => throw new IllegalStateException("LiveCrashRecovery does not accept a second End tick")
      case _: Phase.Terminal => throw new IllegalStateException("LiveCrashRecovery cannot advance after a terminal decision")
      case Phase.New => throw new IllegalStateException("LiveCrashRecovery.begin must precede advance")
  }

  override def finish(exit: RecoveryExit): RecoveryFinishReport = synchronized {
    require(exit != null, "recovery exit must not be null")
    finishReport match
      case Some(_) => throw new IllegalStateException("LiveCrashRecovery.finish may only be called once")
      case None =>
        phase match
          case Phase.New => throw new IllegalStateException("LiveCrashRecovery.begin must precede finish")
          case _ =>
            val report = RecoveryFinishReport(exit, Nil)
            finishReport = Some(report)
            report
  }

  private def completeEnd(plan: Snapshot, current: Snapshot, remediated: Set[UUID]): RecoveryStep =
    if remediated.nonEmpty then
      pendingArtifactsAbsent(current) match
        case Left(failure) => terminalFailure(failure)
        case Right(_) => terminal(StartupPublicationDecision.RemediationRequiresFreshBoot(remediated.minBy(_.toString)))
    else if current.pendingUuids.nonEmpty then
        pendingArtifactsAbsent(current) match
        case Left(failure) => terminalFailure(failure)
        case Right(_) =>
          verifyFreshAbsence(current) match
            case Left(failure) => terminalFailure(failure)
            case Right(_) =>
              capture() match
                case Left(failure) => terminalFailure(failure)
                case Right(afterCas) =>
                  requireSamePlanAfterVerification(plan, afterCas) match
                    case Left(failure) => terminalFailure(failure)
                    case Right(_) => terminal(reconcileActive())
    else terminal(reconcileActive())

  private def reconcileActive(): StartupPublicationDecision =
    try
      val reconciliation = bootReads match
        case Some(reads) => StartupReconciliation.forClaimedBoot(reads, tickets, manager, worlds)
        case None => StartupReconciliation.forServer(repository, tickets, manager, worlds)
      reconciliation.decidePublication()
    catch case failure: Throwable => StartupPublicationDecision.Failed("active reconciliation failed", failure)

  private def verifyFreshAbsence(snapshot: Snapshot): Either[RecoveryFailure, Unit] =
    snapshot.pendingUuids.toList.sortBy(_.toString).foldLeft[Either[RecoveryFailure, Unit]](Right(())) { (result, uuid) =>
      result.flatMap { _ =>
        val locator = snapshot.locators(uuid)
        try
          val verified = store.verifyInactiveAbsence(uuid, locator.domainVersion.get, locator.attemptUuid).get
          if isExactVerified(snapshot.rows(uuid), locator, verified) then Right(())
          else Left(RecoveryFailure.blocked(s"inactive absence verification returned a non-exact locator for $uuid"))
        catch case failure: Throwable => Left(RecoveryFailure.unexpected("inactive absence verification failed", failure))
      }
    }

  private def remediatePending(snapshot: Snapshot): Either[RecoveryFailure, Set[UUID]] =
    snapshot.pendingUuids.toList.sortBy(_.toString).foldLeft[Either[RecoveryFailure, Set[UUID]]](Right(snapshot.bootRemediation)) { (result, uuid) =>
      result.flatMap { remediated =>
        val observation = snapshot.observationsByUuid.get(uuid)
        val hasTicket = tickets.managedTicketRecords().contains(uuid)
        if observation.isEmpty && !hasTicket then Right(remediated)
        else
          removeObservedEntity(snapshot, uuid, observation).flatMap { removed =>
            try
              tickets.releaseIfPresent(uuid).get
              Right(if removed || hasTicket then remediated + uuid else remediated)
            catch case failure: Throwable => Left(RecoveryFailure.unexpected("ticket remediation failed", failure))
          }
      }
    }

  private def removeObservedEntity(snapshot: Snapshot, uuid: UUID, observation: Option[ExistingWorldInventoryObservation]): Either[RecoveryFailure, Boolean] =
    observation match
      case None => Right(false)
      case Some(observed) =>
        try
          val world = exactWorld(snapshot.locators(uuid))
          world.findPod(uuid) match
            case Some(entity) if entity.podUuid == uuid && entity.entityId == observed.entityId =>
              world.removePod(entity)
              Right(true)
            case Some(entity) => Left(RecoveryFailure.blocked(s"world entity for $uuid does not match the inventory observation"))
            case None => Left(RecoveryFailure.blocked(s"inventory observed $uuid but its exact server world no longer contains it"))
        catch case failure: Throwable => Left(RecoveryFailure.unexpected("entity remediation failed", failure))

  private def pendingArtifactsAbsent(snapshot: Snapshot): Either[RecoveryFailure, Unit] =
    if manager.getAllActivePods().nonEmpty then Left(RecoveryFailure.blocked("manager mapping appeared before active reconciliation"))
    else if snapshot.pendingUuids.exists(snapshot.observationsByUuid.contains) then Left(RecoveryFailure.blocked("pending locator still has an entity observation after remediation"))
    else if snapshot.pendingUuids.exists(tickets.managedTicketRecords().contains) then Left(RecoveryFailure.blocked("pending locator still has a managed ticket after remediation"))
    else Right(())

  private def capture(): Either[RecoveryFailure, Snapshot] =
    try
      bootReads.flatMap(LiveCrashRecovery.claimedRecoveryBlock) match
        case Some(reason) => return Left(RecoveryFailure.blocked(reason))
        case None => ()
      val rows = bootReads match
        case Some(reads) => reads.recoveryRows.map(_.toPublicState).toList.sortBy(_.uuid.toString)
        case None => repository.findAll().get.sortBy(_.uuid.toString)
      val locators = bootReads match
        case Some(reads) => reads.recoveryLocators.toList.sortBy(_.podUuid.toString)
        case None => store.findAllPhysicalRecoveryLocators().get.sortBy(_.podUuid.toString)
      val normalized = validateRowsAndLocators(rows, locators)
      normalized.flatMap { case (rowMap, locatorMap) =>
        if manager.getAllActivePods().nonEmpty then Left(RecoveryFailure.blocked("manager mapping exists before active reconciliation"))
        else
          inventory.inspect(locatorMap.values.toList.sortBy(_.podUuid.toString)).toEither.left.map(failure => RecoveryFailure.unexpected("world inventory failed", failure)).flatMap { report =>
            validateInventory(locatorMap, report).flatMap { observations =>
              validateTickets(rowMap, locatorMap).map { bootRemediation =>
                Snapshot(rowMap, locatorMap, observations, bootRemediation)
              }
            }
          }
      }
    catch case failure: Throwable => Left(RecoveryFailure.unexpected("recovery capture failed", failure))

  private def validateRowsAndLocators(rows: List[FloatingPodState], locators: List[PhysicalRecoveryLocator]): Either[RecoveryFailure, (Map[UUID, FloatingPodState], Map[UUID, PhysicalRecoveryLocator])] =
    if rows.map(_.uuid).distinct.size != rows.size then Left(RecoveryFailure.blocked("recovery rows contain duplicate UUIDs"))
    else if locators.map(_.podUuid).distinct.size != locators.size then Left(RecoveryFailure.blocked("recovery locators contain duplicate UUIDs"))
    else
      val rowMap = rows.map(state => state.uuid -> state).toMap
      val locatorMap = locators.map(locator => locator.podUuid -> canonical(locator)).toMap
      if rowMap.keySet != locatorMap.keySet then Left(RecoveryFailure.blocked("recovery rows and locators do not have an exact UUID pairing"))
      else
        rowMap.keys.toList.sortBy(_.toString).foldLeft[Either[RecoveryFailure, Unit]](Right(())) { (result, uuid) =>
          result.flatMap(_ => validatePair(rowMap(uuid), locatorMap(uuid)))
        }.map(_ => rowMap -> locatorMap)

  private def validatePair(row: FloatingPodState, locator: PhysicalRecoveryLocator): Either[RecoveryFailure, Unit] =
    val expectedChunks = recoveryChunks(locator.targetDimensionId.getOrElse(return Left(RecoveryFailure.blocked(s"locator ${locator.podUuid} has no target dimension"))), row.posX, row.posZ)
    val shared =
      locator.domainVersion.contains(row.stateVersion) &&
        row.dimensionId == locator.targetDimensionId &&
        locator.chunks.toSet == expectedChunks && locator.chunks.size == expectedChunks.size
    if !shared then Left(RecoveryFailure.blocked(s"row and locator are not exact for ${row.uuid}"))
    else if row.isActive then
      if locator.state == PhysicalRecoveryLocatorState.RowActive && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive && row.dimensionId.nonEmpty then Right(())
      else Left(RecoveryFailure.blocked(s"active row ${row.uuid} does not have an exact ROW_ACTIVE locator"))
    else if locator.state == PhysicalRecoveryLocatorState.RowInactivePending && locator.reasonCode == PhysicalRecoveryLocatorReason.RowInactivePending then Right(())
    else if locator.state == PhysicalRecoveryLocatorState.RowInactiveVerified && locator.reasonCode == PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified then Right(())
    else Left(RecoveryFailure.blocked(s"inactive row ${row.uuid} has an unsupported locator state or reason"))

  private def validateInventory(locators: Map[UUID, PhysicalRecoveryLocator], report: ExistingWorldInventoryReport): Either[RecoveryFailure, Map[UUID, ExistingWorldInventoryObservation]] =
    val required = report.requiredChunks
    if required.map(_.podUuid).distinct.size != required.size || required.map(_.podUuid).toSet != locators.keySet then Left(RecoveryFailure.blocked("world inventory report does not have an exact locator pairing"))
    else if required.exists(entry => !entry.complete || entry.requiredChunks != locators(entry.podUuid).chunks) then Left(RecoveryFailure.blocked("world inventory is incomplete or changed"))
    else
      report.observations.foldLeft[Either[RecoveryFailure, Map[UUID, ExistingWorldInventoryObservation]]](Right(Map.empty)) { (result, observed) =>
        result.flatMap { collected =>
          locators.get(observed.podUuid) match
            case None => Left(RecoveryFailure.blocked(s"world inventory observed unknown pod ${observed.podUuid}"))
            case Some(locator) if observed.dimensionId != locator.targetDimensionId.get || !locator.chunks.contains(PhysicalRecoveryChunk(observed.dimensionId, observed.chunkX, observed.chunkZ)) =>
              Left(RecoveryFailure.blocked(s"world inventory observed ${observed.podUuid} outside its exact locator"))
            case Some(locator) if locator.state == PhysicalRecoveryLocatorState.RowInactiveVerified =>
              Left(RecoveryFailure.blocked(s"verified inactive locator ${observed.podUuid} has a physical entity contradiction"))
            case Some(_) if collected.contains(observed.podUuid) => Left(RecoveryFailure.blocked(s"world inventory observed duplicate pod ${observed.podUuid}"))
            case Some(_) => Right(collected.updated(observed.podUuid, observed))
        }
      }

  private def validateTickets(rows: Map[UUID, FloatingPodState], locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, Set[UUID]] =
    val evidence = ticketEvidence.snapshot()
    val observations = evidence.cleanup
    val report = evidence.report
    val dimensions = locators.values.map(_.targetDimensionId.get).toSet
    if !dimensions.forall(dimension => observations.contains(StartupTicketCleanupObservation.WorldTicketPassCompleted(dimension))) then Left(RecoveryFailure.blocked("world ticket pass is incomplete"))
    else
      for
        issueKeys <- collectIssueKeys(report.issues, locators)
        dispositionKeys <- collectDispositionKeys(observations, locators)
        _ <- if issueKeys.sortBy(ticketEvidenceOrder) == dispositionKeys.sortBy(ticketEvidenceOrder) then Right(()) else Left(RecoveryFailure.blocked("ticket report issues and cleanup dispositions do not pair exactly"))
        remediated = dispositionKeys.map(_.podUuid).toSet
        managed = tickets.managedTicketRecords()
        _ <- if !managed.keySet.subsetOf(rows.keySet) then Left(RecoveryFailure.blocked("managed ticket inventory contains an unknown pod"))
        else if managed.exists { case (uuid, record) => record.podUuid != uuid || record.ownershipKey == null || !(record.ticket.ownershipKey eq record.ownershipKey) || !locators(uuid).targetDimensionId.contains(record.world.dimensionId) } then Left(RecoveryFailure.blocked("managed ticket ownership or world does not match its locator"))
        else Right(())
      yield remediated

  private def collectIssueKeys(issues: List[TicketRecoveryIssue], locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, List[TicketEvidenceKey]] =
    issues.foldLeft[Either[RecoveryFailure, List[TicketEvidenceKey]]](Right(Nil)) { (result, issue) =>
      result.flatMap(keys => issueKey(issue, locators).map(key => keys :+ key))
    }

  private def collectDispositionKeys(observations: List[StartupTicketCleanupObservation], locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, List[TicketEvidenceKey]] =
    observations.foldLeft[Either[RecoveryFailure, List[TicketEvidenceKey]]](Right(Nil)) { (result, observation) =>
      result.flatMap { keys =>
        dispositionKey(observation, locators).map {
          case Some(key) => keys :+ key
          case None => keys
        }
      }
    }

  private def issueKey(issue: TicketRecoveryIssue, locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, TicketEvidenceKey] = issue match
    case TicketRecoveryIssue.CandidateRejected("recovery_row_rejected", Some(uuid), _) => evidenceKey("drop", uuid, "recovery_row_rejected", locators)
    case TicketRecoveryIssue.CandidateRejected("accepted_candidate_rejected", Some(uuid), _) => evidenceKey("release", uuid, "accepted_candidate_rejected", locators)
    case _ => Left(RecoveryFailure.blocked("ticket recovery report contains an inadmissible issue"))

  private def dispositionKey(observation: StartupTicketCleanupObservation, locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, Option[TicketEvidenceKey]] = observation match
    case StartupTicketCleanupObservation.WorldTicketPassCompleted(_) => Right(None)
    case StartupTicketCleanupObservation.DroppedByOrderedCallback(dimensionId, Some(uuid), "recovery_row_rejected") => dispositionEvidenceKey("drop", uuid, dimensionId, "recovery_row_rejected", locators).map(Some(_))
    case StartupTicketCleanupObservation.ReleasedAfterAcceptedRejection(dimensionId, Some(uuid)) => dispositionEvidenceKey("release", uuid, dimensionId, "accepted_candidate_rejected", locators).map(Some(_))
    case _: StartupTicketCleanupObservation.ReleaseFailed => Left(RecoveryFailure.blocked("ticket cleanup recorded a release failure"))
    case _ => Left(RecoveryFailure.blocked("ticket cleanup contains an inadmissible disposition"))

  private def evidenceKey(kind: String, uuid: UUID, code: String, locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, TicketEvidenceKey] =
    locators.get(uuid) match
      case Some(locator) if locator.state == PhysicalRecoveryLocatorState.RowInactivePending => Right(TicketEvidenceKey(kind, uuid, locator.targetDimensionId.get, code))
      case _ => Left(RecoveryFailure.blocked(s"ticket evidence references a non-pending or unknown locator $uuid"))

  private def dispositionEvidenceKey(kind: String, uuid: UUID, observedDimensionId: Int, code: String, locators: Map[UUID, PhysicalRecoveryLocator]): Either[RecoveryFailure, TicketEvidenceKey] =
    evidenceKey(kind, uuid, code, locators).flatMap { key =>
      if key.dimensionId == observedDimensionId then Right(key) else Left(RecoveryFailure.blocked(s"ticket cleanup disposition dimension does not match locator for $uuid"))
    }

  private def ticketEvidenceOrder(key: TicketEvidenceKey): (String, String, Int, String) = (key.kind, key.podUuid.toString, key.dimensionId, key.code)

  private def requireSamePlan(expected: Snapshot, current: Snapshot): Either[RecoveryFailure, Unit] =
    if expected.rows == current.rows && expected.locators == current.locators then Right(())
    else Left(RecoveryFailure.blocked("recovery row or locator plan changed during deferred recovery"))

  private def requireSamePlanAfterVerification(before: Snapshot, after: Snapshot): Either[RecoveryFailure, Unit] =
    val rowsMatch = before.rows == after.rows
    val locatorsMatch = before.locators.forall { case (uuid, locator) =>
      after.locators.get(uuid).exists { current =>
        if locator.state == PhysicalRecoveryLocatorState.RowInactivePending then isExactVerified(before.rows(uuid), locator, current)
        else current == locator
      }
    }
    if rowsMatch && locatorsMatch && after.pendingUuids.isEmpty then Right(()) else Left(RecoveryFailure.blocked("post-CAS recovery plan is not exactly verified"))

  private def exactWorld(locator: PhysicalRecoveryLocator): PodWorld =
    val dimension = locator.targetDimensionId.getOrElse(throw new IllegalStateException(s"locator ${locator.podUuid} has no target dimension"))
    worlds.resolve(dimension).filter(_.dimensionId == dimension).getOrElse(throw new IllegalStateException(s"no exact server world exists for locator ${locator.podUuid}"))

  private def isExactVerified(row: FloatingPodState, pending: PhysicalRecoveryLocator, verified: PhysicalRecoveryLocator): Boolean =
    verified.podUuid == pending.podUuid && verified.attemptUuid == pending.attemptUuid && verified.state == PhysicalRecoveryLocatorState.RowInactiveVerified &&
      verified.reasonCode == PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified && verified.domainVersion.contains(row.stateVersion) &&
      verified.targetDimensionId == pending.targetDimensionId && canonical(verified).chunks == pending.chunks

  private def recoveryChunks(dimensionId: Int, x: Double, z: Double): Set[PhysicalRecoveryChunk] =
    val centerX = math.floor(x / ChunkLoadingManager.ChunkSize).toInt
    val centerZ = math.floor(z / ChunkLoadingManager.ChunkSize).toInt
    (for chunkX <- centerX - 2 to centerX + 2; chunkZ <- centerZ - 2 to centerZ + 2 yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

  private def canonical(locator: PhysicalRecoveryLocator): PhysicalRecoveryLocator = locator.copy(chunks = locator.chunks.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))
  private def terminal(decision: StartupPublicationDecision): RecoveryStep = { phase = Phase.Terminal(decision); RecoveryStep.Terminal(decision) }
  private def terminalFailure(failure: RecoveryFailure): RecoveryStep =
    failure.cause match
      case Some(cause) => terminal(StartupPublicationDecision.Failed(failure.reason, cause))
      case None => terminal(StartupPublicationDecision.Block(failure.reason))

private[floatingpod] object LiveCrashRecovery:
  /** 只裁决当前切片资格；未实现的写入/来源责任保持 Block，不能由空世界消除。 */
  private[server] def claimedRecoveryBlock(reads: MainBootProjection): Option[String] =
    reads.requireCurrentReads()
    reads.claimedActiveRecoveryBlock.orElse {
      if !reads.binding.exists(_.cutoverState == com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified) then
        Some("claimed recovery requires the exact Certified binding; certification remains incomplete")
      else claimedDataBlock(reads.recoveryRows, reads.recoveryLocators)
    }

  /** 纯数据资格检查不签发任何 lease、ticket 或成功证明。 */
  private[server] def claimedDataBlock(rows: Vector[com.wi.floatingpod.storage.StoredPodRow],
      locators: Vector[PhysicalRecoveryLocator]): Option[String] =
    if rows.exists(row => row.isActive && row.dimensionId.isEmpty) then
      Some("legacy active rows without dimensions remain unresolved; no synchronous backfill is permitted")
    else if locators.exists(locator => locator.state != PhysicalRecoveryLocatorState.RowActive && locator.state != PhysicalRecoveryLocatorState.RowInactiveVerified) then
      Some("pending/source-write/legacy locator obligations remain unresolved; claimed ROW_ACTIVE recovery is incomplete")
    else None

  private[server] def forClaimedBoot(reads: MainBootProjection, tickets: ChunkLoadingManager,
      manager: ActiveFloatingPodManager, worlds: ServerWorldResolver, inventory: ExistingWorldInventory): LiveCrashRecovery =
    require(reads != null, "original Boot projection required")
    reads.requireCurrentReads()
    new LiveCrashRecovery(None, tickets, manager, worlds, inventory, null, Some(reads))
  /** 生產裝配只保留 deferred 邊界；inventory 與 controlled stop 都在實際 probe/request 時解析 current server。 */
  def productionBundle(
      repository: FloatingPodRepository,
      tickets: ChunkLoadingManager,
      manager: ActiveFloatingPodManager,
      worlds: ServerWorldResolver,
      serverSupplier: () => net.minecraft.server.MinecraftServer
  ): DeferredRecoveryBundle =
    require(repository != null && tickets != null && manager != null && worlds != null && serverSupplier != null, "live crash recovery production dependencies must not be null")
    DeferredRecoveryBundle(
      new LiveCrashRecovery(repository, tickets, manager, worlds, ExistingWorldInventory.withServerSupplier(serverSupplier)),
      new ForgeRuntimeRecoveryEvents(tickets),
      new MinecraftServerControlledStop(serverSupplier)
    )

  private final case class TicketEvidenceKey(kind: String, podUuid: UUID, dimensionId: Int, code: String)

  private final case class Snapshot(
      rows: Map[UUID, FloatingPodState],
      locators: Map[UUID, PhysicalRecoveryLocator],
      observationsByUuid: Map[UUID, ExistingWorldInventoryObservation],
      bootRemediation: Set[UUID]
  ):
    def pendingUuids: Set[UUID] = locators.collect { case (uuid, locator) if locator.state == PhysicalRecoveryLocatorState.RowInactivePending => uuid }.toSet

  private sealed trait Phase
  private object Phase:
    case object New extends Phase
    final case class Begun(plan: Snapshot) extends Phase
    final case class Started(plan: Snapshot, remediated: Set[UUID]) extends Phase
    final case class Terminal(decision: StartupPublicationDecision) extends Phase
