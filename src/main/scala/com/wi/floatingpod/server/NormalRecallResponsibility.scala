package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.{IoCompletion, IoOperationTag, IoSubmission}
import com.wi.floatingpod.storage.mysql.*
import net.minecraftforge.fml.common.gameevent.TickEvent
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success}

/** 只移交普通采样责任；原record/pending继续存活，不提供提交、删除或cleanup许可。 */
private[server] final class NormalRecallDriftCustody(originalRecord: AnyRef, originalRemoval: RemovalPending):
  require(originalRecord != null && originalRemoval != null, "original recall record and removal required")
  private val main = Thread.currentThread()
  def observeOrdinary(record: AnyRef)(sample: => Unit): Unit =
    require(Thread.currentThread() eq main, "original recall sampling Main required")
    if (record ne originalRecord) || !originalRemoval.requestIssued then sample

/** 原Main逐pod推进P/物理/C，失败保留原件；不签Normal退出或跨boot恢复许可。 */
private[server] final class NormalRecallResponsibility(
    initialPublication: ActiveFloatingPodManager#MainPublicationScope, adapter: MySqlOwnedIoAdapter,
    tickets: ChunkLoadingManager, initialSources: Vector[ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry],
    initialRequireUsable: () => Unit):
  private var publication = initialPublication
  private var sources = initialSources
  private var requireUsable = initialRequireUsable
  private var mainReferencesDropped = false
  private val main = Thread.currentThread()
  private val identity = adapter.identity
  require(publication != null && adapter != null && tickets != null && sources != null && requireUsable != null &&
    publication.identity == identity && sources.size <= 256 && sources.map(_.podUuid).distinct.size == sources.size &&
    sources.forall(!_.retainedResident), "bounded original Normal stop census required")
  private enum Phase:
    case PrepareReady, PrepareWaiting, Removing, CheckpointReady, CheckpointWaiting, DropPending, Releasing, Complete
  private final class Entry(val source: ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry):
    val request = source.recallRequest
    val workflow = UUID.randomUUID()
    val prepareTag = tag(UUID.randomUUID())
    val reserve = UUID.randomUUID()
    var phase = Phase.PrepareReady
    var preparePlan = Option.empty[MySqlOwnedRepositoryOperationPlan]
    var checkpointPlan = Option.empty[MySqlOwnedRepositoryOperationPlan]
    var prepareGraph = Option.empty[MySqlFencedTransaction.NormalRecallCommitted]
    var checkpointRequest = Option.empty[MySqlNormalRecallCheckpointRequest]
    var checkpointTag = Option.empty[IoOperationTag]
    var observer = Option.empty[ForgePodRemovalObserver]
    var removal = Option.empty[PendingPodRemoval]
    var driftCustody = Option.empty[NormalRecallDriftCustody]
    var ticket = Option.empty[PendingManagedTicketRelease]
    var removalAck = Option.empty[ExactBodyRemovalAcknowledgement]
    var ticketAck = Option.empty[TicketReleaseAcknowledgement]
    var ticketAttempted = false
    var dropAfter = -1L
    val drops = mutable.LinkedHashMap.empty[IoOperationTag, MySqlRetainedPlanDropReceipt]
    val submittedReleases = mutable.HashSet.empty[IoOperationTag]
    val released = mutable.HashSet.empty[IoOperationTag]
    val inbox = mutable.ArrayBuffer.empty[IoCompletion[MySqlOwnedIoReply]]
  private var next = 0
  private var active = Option.empty[Entry]
  private var failure = Option.empty[Throwable]
  private var lastEnd = 0L
  private val completed = mutable.HashSet.empty[UUID]
  private def tag(operation: UUID): IoOperationTag =
    IoOperationTag(identity.bootId, identity.bootGeneration, identity.connectionGeneration, operation)
  private def owner(): Unit = require(Thread.currentThread() eq main, "original Recall Main required")
  private def requireLiveMainReferences(): Unit =
    owner()
    require(!mainReferencesDropped, "original Recall Main references already dropped")
  private def step[A](body: => A): A =
    requireLiveMainReferences()
    failure.foreach(throw _)
    try body
    catch
      case cause: Throwable =>
        failure = Some(cause)
        throw cause
  private[server] def dropCompletedMainReferences(): Unit =
    owner()
    failure.foreach(throw _)
    require(!mainReferencesDropped && isComplete && pendingHandles.isEmpty && completed.size == sources.size,
      "original Recall C/both release ACKs and all sources required before Main drop")
    publication = null
    sources = Vector.empty
    requireUsable = null
    completed.clear()
    mainReferencesDropped = true
  def failureCause: Option[Throwable] = { owner(); failure }
  def isComplete: Boolean = { requireLiveMainReferences(); failure.isEmpty && next == sources.size && active.isEmpty }
  private[server] def observeOrdinaryDrift(record: AnyRef)(sample: => Unit): Unit =
    requireLiveMainReferences()
    active.flatMap(_.driftCustody) match
      case Some(custody) => custody.observeOrdinary(record)(sample)
      case None => sample
  def owns(completion: IoCompletion[MySqlOwnedIoReply]): Boolean =
    requireLiveMainReferences()
    active.exists(e => completion != null && (completion.tag == e.prepareTag || e.checkpointTag.contains(completion.tag)))
  /** 整批先保留；后续任一失败不丢已经从原router取出的完成。 */
  def retainBatch(completions: Vector[IoCompletion[MySqlOwnedIoReply]]): Unit =
    requireLiveMainReferences()
    completions.foreach { completion =>
      require(owns(completion), "completion belongs to another Normal responsibility")
      active.get.inbox += completion
    }
  def consumeRetained(): Unit = step {
    active.foreach { entry =>
      while entry.inbox.nonEmpty do
        val completion = entry.inbox.head
        completion.result match
          case Failure(cause) => throw cause
          case Success(MySqlOwnedIoReply.NormalRecallPrepared(graph)) =>
            requireUsable()
            require(entry.phase == Phase.PrepareWaiting && completion.tag == entry.prepareTag && graph != null &&
              !graph.checkpoint && graph.tag == entry.prepareTag && (graph.request eq entry.request), "foreign/duplicate original P readback")
            publication.requireRecallBody(entry.source)
            entry.prepareGraph = Some(graph)
            val world = entry.source.world match
              case value: PodWorldImpl => value
              case _ => throw new IllegalStateException("recall requires original Forge world")
            val body = entry.source.body match
              case value: ActivePodEntityImpl => value
              case _ => throw new IllegalStateException("recall requires original Forge body")
            val observer = new ForgePodRemovalObserver(world.rawWorld)
            entry.observer = Some(observer)
            val removal = observer.captureForIo(body.rawEntity, entry.prepareTag, entry.request.attemptUuid,
              entry.request.criticalBase.stateVersion, lastEnd)
            entry.removal = Some(removal)
            entry.driftCustody = Some(new NormalRecallDriftCustody(entry.source.record, removal.pending))
            entry.ticket = Some(tickets.captureReleaseForIo(removal, world, entry.source.ticket, entry.request.originalFootprint.toSet))
            entry.phase = Phase.Removing
          case Success(MySqlOwnedIoReply.NormalRecallCheckpointed(graph)) =>
            requireUsable()
            require(entry.phase == Phase.CheckpointWaiting && entry.checkpointTag.contains(completion.tag) && graph != null &&
              graph.checkpoint && graph.tag == completion.tag && (graph.request eq entry.request), "foreign/duplicate original C readback")
            require(entry.removalAck.nonEmpty && entry.ticketAck.nonEmpty, "C cannot replace actual original physical ACKs")
            // 本轮先撤本owner的graph/plan/command引用；真正drop要等下一个END，排除drain局部引用。
            val pHandle = entry.preparePlan.get.originalRetainedHandle.get
            val cHandle = entry.checkpointPlan.get.originalRetainedHandle.get
            pendingHandles.put(entry.prepareTag, pHandle)
            pendingHandles.put(completion.tag, cHandle)
            entry.prepareGraph = None
            entry.preparePlan = None
            entry.checkpointPlan = None
            entry.checkpointRequest = None
            entry.dropAfter = lastEnd
            entry.phase = Phase.DropPending
          case Success(MySqlOwnedIoReply.RetainedPlanReleased(original)) =>
            require(entry.phase == Phase.Releasing && original == completion.tag &&
              entry.submittedReleases.contains(original) && !entry.released.contains(original), "foreign/duplicate recall allocation ACK")
            entry.released.add(original)
          case Success(MySqlOwnedIoReply.RetainedPlanReleaseFailed(original, cause)) =>
            require(original == completion.tag && entry.submittedReleases.contains(original), "foreign recall release failure")
            throw cause
          case Success(_) => throw new IllegalStateException("unexpected original recall completion")
        entry.inbox.remove(0)
    }
  }
  private val pendingHandles = mutable.LinkedHashMap.empty[IoOperationTag, MySqlRetainedPlanHandle]
  /** 原永久pump实际END；容量拒绝仅重交原未执行plan，不mint新ID，不重放物理副作用。 */
  def onEnd(tick: PermanentEndTick): Unit = step {
    require(tick != null && tick.bootUuid == identity.bootId && tick.bootGeneration == identity.bootGeneration &&
      tick.endOrdinal > lastEnd && tick.phase == PermanentPumpPhase.Ready, "recall needs original live Ready END before quiescing")
    lastEnd = tick.endOrdinal
    requireUsable()
    if active.isEmpty && next < sources.size then
      active = Some(new Entry(sources(next)))
      next += 1
    active.foreach { entry =>
      entry.phase match
        case Phase.PrepareReady =>
          publication.requireRecallBody(entry.source)
          if entry.preparePlan.isEmpty then entry.preparePlan = Some(adapter.reserveRepositoryPlan(entry.prepareTag,
            entry.workflow, entry.reserve, MySqlRepositoryOperation.PrepareNormalRecall(entry.request)))
          submit(entry, entry.prepareTag, MySqlOwnedIoCommand.PrepareNormalRecall(entry.request, entry.preparePlan.get), Phase.PrepareWaiting)
        case Phase.Removing =>
          val pending = entry.removal.get
          if !pending.requestIssued then
            publication.requireRecallBody(entry.source)
            entry.observer.get.request(pending, lastEnd) match
              case ExactRemovalStatus.IdentityConflict(_, conflicts) =>
                throw new IllegalStateException("recall removal identity conflict: " + conflicts.mkString(","))
              case _ => ()
            pending.requestFailure.foreach(throw _)
          else
            val observed = entry.observer.get.acknowledgeForIo(pending, TickEvent.Phase.END, lastEnd)
            if pending.pending.conflict.nonEmpty then
              throw new IllegalStateException("recall END identity conflict: " + pending.pending.conflict.mkString(","))
            observed.foreach { acknowledgement =>
              val facts = entry.observer.get.normalRecallFacts(pending, acknowledgement)
              entry.removalAck = Some(acknowledgement)
              require(!entry.ticketAttempted, "ticket release is one-shot after original END")
              entry.ticketAttempted = true
              val released = tickets.releaseForIo(entry.ticket.get, acknowledgement).get
              entry.ticketAck = Some(released)
              val ticketFacts = tickets.normalRecallTicketFacts(entry.ticket.get, released, acknowledgement, facts, entry.request.stopRequest)
              val request = MySqlNormalRecallCheckpointRequest(entry.request, facts, ticketFacts, UUID.randomUUID())
              entry.checkpointRequest = Some(request)
              val cTag = tag(UUID.randomUUID())
              entry.checkpointTag = Some(cTag)
              entry.checkpointPlan = Some(adapter.reserveRepositoryPlan(cTag, entry.workflow, UUID.randomUUID(),
                MySqlRepositoryOperation.CheckpointNormalRecall(request)))
              entry.phase = Phase.CheckpointReady
            }
        case Phase.CheckpointReady =>
          submit(entry, entry.checkpointTag.get, MySqlOwnedIoCommand.CheckpointNormalRecall(entry.prepareGraph.get,
            entry.checkpointRequest.get, entry.checkpointPlan.get), Phase.CheckpointWaiting)
        case Phase.DropPending if lastEnd > entry.dropAfter =>
          Vector(entry.checkpointTag.get, entry.prepareTag).foreach { original =>
            entry.drops.put(original, adapter.recordMainPlanDrop(pendingHandles(original)))
            pendingHandles.remove(original)
          }
          entry.phase = Phase.Releasing
        case Phase.Releasing =>
          entry.drops.foreach { (original, receipt) =>
            if !entry.submittedReleases.contains(original) then
              adapter.submitRetainedPlanRelease(original, receipt) match
                case IoSubmission.Accepted => entry.submittedReleases.add(original)
                case IoSubmission.RejectedCapacity => ()
                case IoSubmission.RejectedQuiescing => throw new IllegalStateException("recall allocation release rejected by quiescing")
                case IoSubmission.RejectedStopped(cause) =>
                  throw cause.getOrElse(new IllegalStateException("recall allocation release rejected by stopped worker"))
          }
          if entry.released.size == 2 then
            entry.phase = Phase.Complete
            tickets.compactNormalRecallRelease(this, entry.source, entry.ticket.get, entry.ticketAck.get)
            publication.completeNormalRecall(this, entry.source)
            completed.add(entry.request.podUuid)
            active = None
        case _ => ()
    }
  }
  private def submit(entry: Entry, original: IoOperationTag, command: MySqlOwnedIoCommand, waiting: Phase): Unit =
    adapter.submit(original, command) match
      case IoSubmission.Accepted => entry.phase = waiting
      case IoSubmission.RejectedCapacity => ()
      case IoSubmission.RejectedQuiescing => throw new IllegalStateException("normal recall command rejected by quiescing")
      case IoSubmission.RejectedStopped(cause) =>
        throw cause.getOrElse(new IllegalStateException("normal recall command rejected by stopped worker"))
  private[server] def requireCompleted(source: ActiveFloatingPodManager#MainPublicationScope#NormalStopEntry): Unit =
    owner()
    failure.foreach(throw _)
    require(active.exists(entry => (entry.source eq source) && entry.phase == Phase.Complete && entry.released.size == 2 &&
      entry.inbox.isEmpty && entry.preparePlan.isEmpty && entry.checkpointPlan.isEmpty && entry.prepareGraph.isEmpty &&
      entry.removalAck.nonEmpty && entry.ticketAck.nonEmpty), "original C and both allocation ACKs must precede cleanup")
