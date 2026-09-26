package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{FloatingPodRepository, PhysicalRecoveryStore}

import java.util.UUID

/** 启动发布的不可变结论；Permit 只表示最终 inventory 已收敛。 */
sealed trait StartupPublicationDecision

object StartupPublicationDecision:
  final case class Permit(recoveredUuids: List[UUID]) extends StartupPublicationDecision:
    require(recoveredUuids != null && recoveredUuids.forall(_ != null), "recoveredUuids must not contain null")
  final case class RemediationRequiresFreshBoot(podUuid: UUID) extends StartupPublicationDecision:
    require(podUuid != null, "podUuid must not be null")
  final case class Block(reason: String) extends StartupPublicationDecision:
    require(reason != null && reason.nonEmpty, "reason must not be blank")

/** 真实 runtime 与测试替身共享的包内 inventory/动作边界。 */
private[server] trait StartupReconciliationInventory:
  def committedStates: List[FloatingPodState]
  def ticketUuids: Set[UUID]
  def entityUuids: Set[UUID]
  def managerUuids: Set[UUID]
  def destructiveRemediationReady: Boolean = true
  def requestTicket(podUuid: UUID): Unit
  def spawnEntity(active: FloatingPodState): Unit
  def publishMapping(active: FloatingPodState): Unit
  def removeEntity(podUuid: UUID): Unit
  def releaseTicket(podUuid: UUID): Unit

/** 第一条启动发布 tracer：仅处理一条已提交 active 状态且初始运行时 inventory 为空的情况。 */
final class StartupReconciliation(inventory: StartupReconciliationInventory):
  require(inventory != null, "inventory must not be null")

  def decidePublication(): StartupPublicationDecision =
    val committed = inventory.committedStates
    if committed.size == 1 then
      val state = committed.head
      if !state.isActive then remediateInactive(state)
      else if state.dimensionId.isEmpty then StartupPublicationDecision.Block("committed state is not a complete active state")
      else if inventory.ticketUuids == Set(state.uuid) && inventory.entityUuids == Set(state.uuid) && inventory.managerUuids.isEmpty then
        inventory.publishMapping(state)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids == Set(state.uuid) && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then
        inventory.spawnEntity(state)
        inventory.publishMapping(state)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids.isEmpty && inventory.entityUuids == Set(state.uuid) && inventory.managerUuids.isEmpty then
        inventory.requestTicket(state.uuid)
        inventory.publishMapping(state)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
      else if inventory.ticketUuids.nonEmpty || inventory.entityUuids.nonEmpty || inventory.managerUuids.nonEmpty then
        StartupPublicationDecision.Block("runtime inventory is not empty before publication")
      else
        inventory.requestTicket(state.uuid)
        inventory.spawnEntity(state)
        inventory.publishMapping(state)
        val expected = Set(state.uuid)
        if inventory.committedStates == List(state) && inventory.ticketUuids == expected && inventory.entityUuids == expected && inventory.managerUuids == expected then
          StartupPublicationDecision.Permit(List(state.uuid))
        else StartupPublicationDecision.Block("final inventory did not converge")
    else if committed.isEmpty then
      if inventory.ticketUuids.isEmpty && inventory.entityUuids.isEmpty && inventory.managerUuids.isEmpty then StartupPublicationDecision.Permit(Nil)
      else StartupPublicationDecision.Block("runtime inventory is not empty without committed state")
    else reconcileMultipleCommitted(committed)

  private def reconcileMultipleCommitted(committed: List[FloatingPodState]): StartupPublicationDecision =
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
        if !inventory.ticketUuids.contains(state.uuid) then inventory.requestTicket(state.uuid)
        if !inventory.entityUuids.contains(state.uuid) then inventory.spawnEntity(state)
        inventory.publishMapping(state)
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

object StartupReconciliation:
  def forServer(repository: FloatingPodRepository, tickets: ChunkLoadingManager, manager: ActiveFloatingPodManager, worlds: ServerWorldResolver): StartupReconciliation =
    require(repository != null, "repository must not be null")
    require(repository.isInstanceOf[PhysicalRecoveryStore], "startup reconciliation requires a PhysicalRecoveryStore repository")
    require(tickets != null, "tickets must not be null")
    require(manager != null, "manager must not be null")
    require(worlds != null, "worlds must not be null")
    new StartupReconciliation(new ServerStartupReconciliationInventory(repository, tickets, manager, worlds))

  private final class ServerStartupReconciliationInventory(
      repository: FloatingPodRepository,
      tickets: ChunkLoadingManager,
      manager: ActiveFloatingPodManager,
      worlds: ServerWorldResolver
  ) extends StartupReconciliationInventory:
    override def committedStates: List[FloatingPodState] = repository.findAll().get.sortBy(_.uuid.toString)
    override def ticketUuids: Set[UUID] = tickets.managedTicketRecords().keySet.toSet
    override def managerUuids: Set[UUID] = manager.getAllActivePods().toSet
    override val destructiveRemediationReady = false
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
      val entity = worldFor(state).spawnPod(state)
      if entity == null || entity.podUuid != state.uuid then throw new IllegalStateException(s"World did not spawn exact committed pod ${state.uuid}")

    override def publishMapping(active: FloatingPodState): Unit =
      val state = currentActiveState(active)
      manager.recoverCommittedActive(state, worldFor(state)).get

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
