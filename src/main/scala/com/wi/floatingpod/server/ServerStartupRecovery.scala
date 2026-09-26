package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{RecoveryDimensionBackfillOutcome, RecoveryPodStore, StoredPodRow}

import java.util.UUID
import scala.collection.mutable.ListBuffer

/** 将 dimension id 解析为当前服务器的同一世界包装；不得使用客户端状态。 */
trait ServerWorldResolver:
  def resolve(dimensionId: Int): Option[PodWorld]

/** 启动恢复的可观察结果；失败始终携带原始原因。 */
final case class TicketRecoveryFailure(report: TicketRecoveryReport, originalCause: Option[Throwable]) extends RuntimeException(originalCause.orNull):
  require(report != null && originalCause != null, "ticket recovery failure values must not be null")
final case class StartupRecoveryReport(recoveredUuids: List[UUID], ticketReport: TicketRecoveryReport, failure: Option[Throwable], cleanupFailures: List[Throwable] = Nil):
  require(recoveredUuids != null && ticketReport != null && failure != null && cleanupFailures != null, "startup recovery report values must not be null")
  def isClean: Boolean = failure.isEmpty && ticketReport.isClean

/** 仅协调 S2 已接管 ticket、S1 原始行和 S3 manager；不在 Stopped 做任何世界工作。 */
final class ServerStartupRecovery(store: RecoveryPodStore, tickets: ChunkLoadingManager, manager: ActiveFloatingPodManager, worlds: ServerWorldResolver):
  require(store != null && tickets != null && manager != null && worlds != null, "startup recovery dependencies must not be null")

  def recover(): StartupRecoveryReport =
    val attempted = ListBuffer.empty[(UUID, PodWorld)]
    try
      val rows = store.findDesiredActiveRows().get.sortBy(_.uuid.toString)
      tickets.reconcileOrphanedTickets(rows.iterator.map(_.uuid).toSet)
      val ticketReport = tickets.latestRecoveryReport()
      if !ticketReport.isClean then throw TicketRecoveryFailure(ticketReport, firstCause(ticketReport))
      val desiredRawUuids = rows.map(_.uuid).toSet
      val ticketUuids = tickets.managedTicketRecords().keySet
      if ticketUuids != desiredRawUuids then throw new IllegalStateException(s"Desired-active and canonical ticket UUID sets differ: desired=$desiredRawUuids tickets=$ticketUuids")
      val preexistingManagerUuids = manager.getAllActivePods().toSet
      if preexistingManagerUuids.nonEmpty then throw new IllegalStateException(s"Recovery manager is not empty: $preexistingManagerUuids")
      val states = rows.map(toRecoverableState)
      val desiredUuids = states.map(_.uuid).toSet
      states.foreach { state =>
        val world = worlds.resolve(state.dimensionId.getOrElse(throw new IllegalStateException(s"Active pod ${state.uuid} has no dimension"))).getOrElse(throw new IllegalStateException(s"No server world exists for active pod ${state.uuid}"))
        manager.recoverCommittedActive(state, world).get
        attempted += state.uuid -> world
        val ticket = tickets.managedTicketRecords().getOrElse(state.uuid, throw new IllegalStateException(s"Missing canonical ticket for ${state.uuid}"))
        if ticket.podUuid != state.uuid || !ticket.world.sameWorld(world) then throw new IllegalStateException(s"Ticket world mismatch for ${state.uuid}")
        val active = manager.getActivePod(state.uuid).getOrElse(throw new IllegalStateException(s"Manager did not publish ${state.uuid}"))
        val entity = world.findPod(state.uuid).getOrElse(throw new IllegalStateException(s"World did not contain ${state.uuid}"))
        if active.podUuid != state.uuid || entity.podUuid != state.uuid then throw new IllegalStateException(s"Recovery UUID set mismatch for ${state.uuid}")
      }
      val managerUuids = manager.getAllActivePods().toSet
      if managerUuids != desiredUuids then throw new IllegalStateException(s"Desired-active and manager UUID sets differ: desired=$desiredUuids manager=$managerUuids")
      StartupRecoveryReport(states.map(_.uuid), tickets.latestRecoveryReport(), None)
    catch
      case failure: Throwable =>
        val cleanupFailures = attempted.reverse.flatMap { case (uuid, world) => manager.abortRecoveredActive(uuid, world).failed.toOption }.toList
        cleanupFailures.foreach(failure.addSuppressed)
        StartupRecoveryReport(Nil, tickets.latestRecoveryReport(), Some(failure), cleanupFailures)

  private def firstCause(report: TicketRecoveryReport): Option[Throwable] =
    report.issues.collectFirst {
      case TicketRecoveryIssue.CandidateProcessingFailed(_, cause) => cause
      case TicketRecoveryIssue.CandidateReleaseFailed(_, cause) => cause
      case TicketRecoveryIssue.PendingReleaseRetryFailed(cause) => cause
      case TicketRecoveryIssue.OrphanReleaseFailed(_, cause) => cause
    }

  private def toRecoverableState(row: StoredPodRow): FloatingPodState =
    if !row.isActive then throw new IllegalStateException(s"Desired-active query returned inactive pod ${row.uuid}")
    row.dimensionId match
      case Some(_) => row.toPublicState
      case None =>
        val ticket = tickets.managedTicketRecords().getOrElse(row.uuid, throw new IllegalStateException(s"Missing canonical ticket for legacy active pod ${row.uuid}"))
        store.backfillMissingActiveDimension(row.uuid, row.stateVersion, ticket.world.dimensionId).get match
          case RecoveryDimensionBackfillOutcome.Ready(state) => state
          case RecoveryDimensionBackfillOutcome.Rejected(reason) => throw new IllegalStateException(s"Active dimension backfill rejected for ${row.uuid}: $reason")
