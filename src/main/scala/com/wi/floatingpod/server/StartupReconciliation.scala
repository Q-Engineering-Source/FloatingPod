package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{FloatingPodRepository, PhysicalRecoveryStore}

import java.util.UUID
import scala.collection.mutable.ListBuffer

/** 启动发布的不可变结论；Permit 只表示最终 inventory 已收敛。 */
sealed trait StartupPublicationDecision

object StartupPublicationDecision:
  final case class Permit(recoveredUuids: List[UUID]) extends StartupPublicationDecision:
    require(recoveredUuids != null && recoveredUuids.forall(_ != null), "recoveredUuids must not contain null")
  final case class RemediationRequiresFreshBoot(podUuid: UUID) extends StartupPublicationDecision:
    require(podUuid != null, "podUuid must not be null")
  final case class Block(reason: String) extends StartupPublicationDecision:
    require(reason != null && reason.nonEmpty, "reason must not be blank")
  final case class Failed(reason: String, cause: Throwable) extends StartupPublicationDecision:
    require(reason != null && reason.nonEmpty && cause != null, "failure decision values must not be null or blank")

/** 真实 runtime 与测试替身共享的包内 inventory/动作边界。 */
private[server] trait StartupReconciliationInventory:
  def committedStates: List[FloatingPodState]
  def ticketUuids: Set[UUID]
  def entityUuids: Set[UUID]
  def managerUuids: Set[UUID]
  def destructiveRemediationReady: Boolean = true
  def beginStartupRecovery(active: FloatingPodState): Unit
  def completeStartupRecoveries(podUuids: List[UUID]): Unit
  def abortStartupRecovery(podUuid: UUID): Unit
  def requestTicket(podUuid: UUID): Unit
  def spawnEntity(active: FloatingPodState): Unit
  def publishMapping(active: FloatingPodState): Unit
  def removeEntity(podUuid: UUID): Unit
  def releaseTicket(podUuid: UUID): Unit

/** 第一条启动发布 tracer：仅处理一条已提交 active 状态且初始运行时 inventory 为空的情况。 */
final class StartupReconciliation(inventory: StartupReconciliationInventory):
  require(inventory != null, "inventory must not be null")

  def decidePublication(): StartupPublicationDecision =
    val attempted = ListBuffer.empty[UUID]
    try
      decidePublication(attempted) match
        case permit: StartupPublicationDecision.Permit =>
          inventory.completeStartupRecoveries(attempted.toList)
          permit
        case StartupPublicationDecision.Block(reason) =>
          val failure = new IllegalStateException(s"Startup reconciliation blocked: $reason")
          rollbackStartupRecoveries(attempted, failure)
          if failure.getSuppressed.isEmpty then StartupPublicationDecision.Block(reason)
          else StartupPublicationDecision.Failed(reason, failure)
        case StartupPublicationDecision.Failed(reason, cause) =>
          rollbackStartupRecoveries(attempted, cause)
          StartupPublicationDecision.Failed(reason, cause)
        case remediation: StartupPublicationDecision.RemediationRequiresFreshBoot =>
          if attempted.isEmpty then remediation
          else
            val failure = new IllegalStateException("Startup remediation cannot retain active publication receipts")
            rollbackStartupRecoveries(attempted, failure)
            StartupPublicationDecision.Failed(failure.getMessage, failure)
    catch
      case primary: Throwable =>
        rollbackStartupRecoveries(attempted, primary)
        throw primary

  private def decidePublication(attempted: ListBuffer[UUID]): StartupPublicationDecision =
    val committed = inventory.committedStates
    if committed.size == 1 then
      val state = committed.head
      if !state.isActive then remediateInactive(state)
      else if state.dimensionId.isEmpty then StartupPublicationDecision.Block("committed state is not a complete active state")
      else if inventory.ticketUuids == Set(state.uuid) && inventory.entityUuids == Set(state.uuid) && inventory.managerUuids.isEmpty then
        recoverActive(state, requestMissingTicket = false, spawnMissingEntity = false, attempted = attempted)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids == Set(state.uuid) && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then
        recoverActive(state, requestMissingTicket = false, spawnMissingEntity = true, attempted = attempted)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids.isEmpty && inventory.entityUuids == Set(state.uuid) && inventory.managerUuids.isEmpty then
        recoverActive(state, requestMissingTicket = true, spawnMissingEntity = false, attempted = attempted)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids.nonEmpty || inventory.entityUuids.nonEmpty || inventory.managerUuids.nonEmpty then
        StartupPublicationDecision.Block("runtime inventory is not empty before publication")
      else
        recoverActive(state, requestMissingTicket = true, spawnMissingEntity = true, attempted = attempted)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
    else if committed.isEmpty then
      if inventory.ticketUuids.isEmpty && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then StartupPublicationDecision.Permit(Nil)
      else StartupPublicationDecision.Block("runtime inventory is not empty without committed state")
    else reconcileMultipleCommitted(committed, attempted)

  private def reconcileMultipleCommitted(committed: List[FloatingPodState], attempted: ListBuffer[UUID]): StartupPublicationDecision =
    val committedUuids = committed.map(_.uuid).toSet
    val activeRows = committed.filter(_.isActive)
    val activeUuids = activeRows.map(_.uuid).toSet
    val inactiveUuids = committed.collect { case state if !state.isActive => state.uuid }.toSet
    if committedUuids.size != committed.size then StartupPublicationDecision.Block("committed states contain duplicate UUIDs")
    else if inventory.managerUuids.nonEmpty then StartupPublicationDecision.Block("runtime manager inventory is not empty before publication")
    else if !inventory.ticketUuids.subsetOf(committedUuids) || !inventory.entityUuids.subsetOf(committedUuids) then
      StartupPublicationDecision.Block("runtime inventory contains unexpected UUIDs")
    else if inventory.ticketUuids.intersect(inactiveUuids).nonEmpty || inventory.entityUuids.intersect(inactiveUuids).nonEmpty then
      StartupPublicationDecision.Block("inactive history has physical artifacts")
    else if activeRows.exists(_.dimensionId.isEmpty) then
      StartupPublicationDecision.Block("committed active states are not complete")
    else if activeRows.isEmpty then StartupPublicationDecision.Permit(Nil)
    else
      val ordered = activeRows.sortBy(_.uuid.toString)
      ordered.foreach: state =>
        recoverActive(state, requestMissingTicket = !inventory.ticketUuids.contains(state.uuid),
          spawnMissingEntity = !inventory.entityUuids.contains(state.uuid), attempted = attempted)
      val recoveredUuids = ordered.map(_.uuid)
      if inventory.committedStates == committed && inventory.ticketUuids == activeUuids && inventory.entityUuids == activeUuids && inventory.managerUuids == activeUuids then
        StartupPublicationDecision.Permit(recoveredUuids)
      else StartupPublicationDecision.Block("final inventory did not converge")

  private def remediateInactive(state: FloatingPodState): StartupPublicationDecision =
    val expected = Set(state.uuid)
    if inventory.ticketUuids.isEmpty && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then
      StartupPublicationDecision.Permit(Nil)
    else if !inventory.destructiveRemediationReady then
      StartupPublicationDecision.Block("runtime startup cannot certify destructive inactive remediation")
    else if inventory.managerUuids.nonEmpty || inventory.ticketUuids != expected || inventory.entityUuids != expected then
      StartupPublicationDecision.Block("inactive state does not have exactly one orphan ticket and entity")
    else
      inventory.removeEntity(state.uuid)
      inventory.releaseTicket(state.uuid)
      if inventory.ticketUuids.isEmpty && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then
        StartupPublicationDecision.RemediationRequiresFreshBoot(state.uuid)
      else StartupPublicationDecision.Block("inactive remediation did not clear physical inventory")

  private def recoverActive(state: FloatingPodState, requestMissingTicket: Boolean, spawnMissingEntity: Boolean, attempted: ListBuffer[UUID]): Unit =
    inventory.beginStartupRecovery(state)
    attempted += state.uuid
    if requestMissingTicket then inventory.requestTicket(state.uuid)
    if spawnMissingEntity then inventory.spawnEntity(state)
    inventory.publishMapping(state)

  private def rollbackStartupRecoveries(attempted: ListBuffer[UUID], primary: Throwable): Unit =
    attempted.reverseIterator.foreach { podUuid =>
      try inventory.abortStartupRecovery(podUuid)
      catch
        case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
    }

object StartupReconciliation:
  def forServer(repository: FloatingPodRepository, tickets: ChunkLoadingManager, manager: ActiveFloatingPodManager, worlds: ServerWorldResolver): StartupReconciliation =
    require(repository != null, "repository must not be null")
    require(repository.isInstanceOf[PhysicalRecoveryStore], "startup reconciliation requires a PhysicalRecoveryStore repository")
    require(tickets != null, "tickets must not be null")
    require(manager != null, "manager must not be null")
    require(worlds != null, "worlds must not be null")
    new StartupReconciliation(new ServerStartupReconciliationInventory(() => repository.findAll().get, tickets, manager, worlds))

  private[server] def forClaimedBoot(reads: MainBootProjection, tickets: ChunkLoadingManager,
      manager: ActiveFloatingPodManager, worlds: ServerWorldResolver): StartupReconciliation =
    reads.requireCurrentReads()
    new StartupReconciliation(new ServerStartupReconciliationInventory(
      () => reads.recoveryRows.map(_.toPublicState).toList, tickets, manager, worlds))

  private final class ServerStartupReconciliationInventory(
      rows: () => List[FloatingPodState],
      tickets: ChunkLoadingManager,
      manager: ActiveFloatingPodManager,
      worlds: ServerWorldResolver
  ) extends StartupReconciliationInventory:
    private val startupRecoveryReceipts = scala.collection.mutable.LinkedHashMap.empty[UUID, StartupRecoveryRollbackReceipt]
    override def committedStates: List[FloatingPodState] = rows().sortBy(_.uuid.toString)
    override def ticketUuids: Set[UUID] = tickets.managedTicketRecords().keySet.toSet
    override def managerUuids: Set[UUID] = manager.getAllActivePods().toSet
    override val destructiveRemediationReady = false
    override def beginStartupRecovery(active: FloatingPodState): Unit =
      val state = currentActiveState(active)
      val world = worldFor(state)
      if startupRecoveryReceipts.contains(state.uuid) then throw new IllegalStateException(s"Startup recovery receipt already exists for ${state.uuid}")
      startupRecoveryReceipts.put(state.uuid, manager.beginStartupRecovery(state, world))

    override def completeStartupRecoveries(podUuids: List[UUID]): Unit =
      if podUuids == null || podUuids.distinct.size != podUuids.size then throw new IllegalArgumentException("Startup recovery completion UUIDs must be distinct")
      val receipts = podUuids.map(uuid => startupRecoveryReceipts.getOrElse(uuid, throw new IllegalStateException(s"Missing startup recovery receipt for $uuid")))
      manager.completeStartupRecoveries(receipts)
      podUuids.foreach(startupRecoveryReceipts.remove)

    override def abortStartupRecovery(podUuid: UUID): Unit =
      val receipt = startupRecoveryReceipts.getOrElse(podUuid, throw new IllegalStateException(s"Missing startup recovery receipt for $podUuid"))
      try manager.abortStartupRecovery(receipt).get
      finally startupRecoveryReceipts.remove(podUuid)

    override def entityUuids: Set[UUID] =
      committedStates.flatMap { state =>
        state.dimensionId.toList.flatMap { _ =>
          worldFor(state).findPod(state.uuid).map { entity =>
            if entity.podUuid != state.uuid then throw new IllegalStateException(s"World returned entity UUID ${entity.podUuid} for committed pod ${state.uuid}")
            state.uuid
          }
        }
      }.toSet

    override def requestTicket(podUuid: UUID): Unit =
      val state = activeState(podUuid)
      tickets.requestTicket(podUuid, worldFor(state), chunkCoordinate(state.posX), chunkCoordinate(state.posZ)).get

    override def spawnEntity(active: FloatingPodState): Unit =
      val state = currentActiveState(active)
      val receipt = startupRecoveryReceipts.getOrElse(state.uuid, throw new IllegalStateException(s"Missing startup recovery receipt for ${state.uuid}"))
      manager.spawnStartupRecoveryEntity(state, receipt).get

    override def publishMapping(active: FloatingPodState): Unit =
      val state = currentActiveState(active)
      val receipt = startupRecoveryReceipts.getOrElse(state.uuid, throw new IllegalStateException(s"Missing startup recovery receipt for ${state.uuid}"))
      manager.publishStartupRecovery(state, receipt).get

    override def removeEntity(podUuid: UUID): Unit =
      val state = exactState(podUuid)
      val world = worldFor(state)
      val entity = world.findPod(podUuid).getOrElse(throw new IllegalStateException(s"Expected physical entity for committed pod $podUuid"))
      if entity.podUuid != podUuid then throw new IllegalStateException(s"World returned entity UUID ${entity.podUuid} for committed pod $podUuid")
      world.removePod(entity)

    override def releaseTicket(podUuid: UUID): Unit =
      exactState(podUuid)
      tickets.managedTicketRecords().get(podUuid).getOrElse(throw new IllegalStateException(s"Expected managed ticket for committed pod $podUuid"))
      tickets.releaseTicket(podUuid).get

    private def exactState(podUuid: UUID): FloatingPodState =
      val matches = committedStates.filter(_.uuid == podUuid)
      if matches.size != 1 then throw new IllegalStateException(s"Expected exactly one committed state for $podUuid")
      matches.head

    private def activeState(podUuid: UUID): FloatingPodState =
      val state = exactState(podUuid)
      if !state.isActive || state.dimensionId.isEmpty then throw new IllegalStateException(s"Committed state $podUuid is not complete active")
      state

    private def currentActiveState(active: FloatingPodState): FloatingPodState =
      if active == null then throw new IllegalArgumentException("active state must not be null")
      val state = activeState(active.uuid)
      if state != active then throw new IllegalStateException(s"Committed state changed before publication for ${active.uuid}")
      state

    private def worldFor(state: FloatingPodState): PodWorld =
      val dimensionId = state.dimensionId.getOrElse(throw new IllegalStateException(s"Committed state ${state.uuid} has no dimension"))
      val world = worlds.resolve(dimensionId).getOrElse(throw new IllegalStateException(s"No server world exists for committed pod ${state.uuid} in dimension $dimensionId"))
      if world.dimensionId != dimensionId then throw new IllegalStateException(s"Resolved world dimension ${world.dimensionId} does not match committed pod ${state.uuid} dimension $dimensionId")
      world

    private def chunkCoordinate(position: Double): Int = math.floor(position / 16D).toInt
