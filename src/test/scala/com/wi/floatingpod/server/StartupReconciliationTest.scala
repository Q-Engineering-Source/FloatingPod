package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable.ListBuffer

/** 已提交活动行的首个启动发布 tracer。 */
class StartupReconciliationTest:
  @Test
  def completeCommittedActiveInventoryPublishesOnlyAfterTicketEntityAndManagerConverge(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000801"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000802")),
      7L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      99L,
      Some(7)
    )
    val inventory = new CompleteInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(state.uuid)), decision)
    assertEquals(List(s"ticket:${state.uuid}", s"entity:${state.uuid}", s"mapping:${state.uuid}"), inventory.events.toList)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(Set(state.uuid), inventory.ticketUuids)
    assertEquals(Set(state.uuid), inventory.entityUuids)
    assertEquals(Set(state.uuid), inventory.managerUuids)

  @Test
  def completeCommittedActiveRowAttachesMappingToExistingTicketAndEntity(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000807"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000808")),
      10L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      102L,
      Some(7)
    )
    val inventory = new AttachedPhysicalInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(state.uuid)), decision)
    assertEquals(List(s"mapping:${state.uuid}"), inventory.events.toList)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(List(state), inventory.committedStates)
    assertEquals(Set(state.uuid), inventory.ticketUuids)
    assertEquals(Set(state.uuid), inventory.entityUuids)
    assertEquals(Set(state.uuid), inventory.managerUuids)

  @Test
  def completeCommittedActiveRowSpawnsEntityThenAttachesMappingToExistingTicket(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000809"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000810")),
      11L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      103L,
      Some(7)
    )
    val inventory = new TicketOnlyInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(state.uuid)), decision)
    assertEquals(List(s"entity:${state.uuid}", s"mapping:${state.uuid}"), inventory.events.toList)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(List(state), inventory.committedStates)
    assertEquals(Set(state.uuid), inventory.ticketUuids)
    assertEquals(Set(state.uuid), inventory.entityUuids)
    assertEquals(Set(state.uuid), inventory.managerUuids)

  @Test
  def completeCommittedActiveRowRequestsTicketThenAttachesMappingToExistingEntity(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000811"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000812")),
      12L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      104L,
      Some(7)
    )
    val inventory = new EntityOnlyInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(state.uuid)), decision)
    assertEquals(List(s"ticket:${state.uuid}", s"mapping:${state.uuid}"), inventory.events.toList)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(List(state), inventory.committedStates)
    assertEquals(Set(state.uuid), inventory.ticketUuids)
    assertEquals(Set(state.uuid), inventory.entityUuids)
    assertEquals(Set(state.uuid), inventory.managerUuids)

  @Test
  def twoCompleteCommittedActiveRowsPublishInStableUuidOrder(): Unit =
    val low = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000813"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000815")),
      13L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      105L,
      Some(7)
    )
    val high = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000814"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000816")),
      14L,
      "reconciliation-world",
      48D,
      64D,
      -16D,
      0F,
      0F,
      true,
      106L,
      Some(7)
    )
    val inventory = new MultipleActiveRowsInventory(List(high, low))

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(low.uuid, high.uuid)), decision)
    assertEquals(
      List(
        "ticket:00000000-0000-0000-0000-000000000813",
        "entity:00000000-0000-0000-0000-000000000813",
        "mapping:00000000-0000-0000-0000-000000000813",
        "ticket:00000000-0000-0000-0000-000000000814",
        "entity:00000000-0000-0000-0000-000000000814",
        "mapping:00000000-0000-0000-0000-000000000814"
      ),
      inventory.events.toList
    )
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(List(high, low), inventory.committedStates)
    assertEquals(Set(low.uuid, high.uuid), inventory.ticketUuids)
    assertEquals(Set(low.uuid, high.uuid), inventory.entityUuids)
    assertEquals(Set(low.uuid, high.uuid), inventory.managerUuids)

  @Test
  def inactiveHistoryWithoutArtifactsDoesNotBlockActiveRowPublication(): Unit =
    val active = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000001021"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000001023")),
      15L,
      "reconciliation-world",
      32D,
      64D,
      -16D,
      0F,
      0F,
      true,
      107L,
      Some(7)
    )
    val inactive = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000001022"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000001024")),
      16L,
      "reconciliation-world",
      48D,
      64D,
      -16D,
      0F,
      0F,
      false,
      108L,
      Some(7)
    )
    val inventory = new MultipleActiveRowsInventory(List(inactive, active))

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(List(active.uuid)), decision)
    assertEquals(List(s"ticket:${active.uuid}", s"entity:${active.uuid}", s"mapping:${active.uuid}"), inventory.events.toList)
    assertTrue(inventory.events.forall(event => !event.contains(inactive.uuid.toString)))
    assertEquals(0, inventory.databaseWriteAttempts)
    assertEquals(List(inactive, active), inventory.committedStates)
    assertEquals(Set(active.uuid), inventory.ticketUuids)
    assertEquals(Set(active.uuid), inventory.entityUuids)
    assertEquals(Set(active.uuid), inventory.managerUuids)

  @Test
  def emptyCommittedStateListPermitsCleanCertifiedWorldStartup(): Unit =
    val inventory = new EmptyCommittedInventory

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(Nil), decision)
    assertTrue(inventory.events.isEmpty)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertTrue(inventory.ticketUuids.isEmpty)
    assertTrue(inventory.entityUuids.isEmpty)
    assertTrue(inventory.managerUuids.isEmpty)

  @Test
  def committedInactiveRowRemovesEntityThenReleasesTicketAndRequiresFreshBootProof(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000803"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000804")),
      8L,
      "reconciliation-world",
      0D,
      64D,
      0D,
      0F,
      0F,
      false,
      100L,
      None
    )
    val inventory = new InactiveInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.RemediationRequiresFreshBoot(state.uuid), decision)
    assertEquals(List(s"remove:${state.uuid}", s"release:${state.uuid}"), inventory.events.toList)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertTrue(inventory.entityUuids.isEmpty)
    assertTrue(inventory.ticketUuids.isEmpty)
    assertTrue(inventory.managerUuids.isEmpty)

  @Test
  def freshBootWithCommittedInactiveRowAndNoPhysicalInventoryPermitsNoRecoveredPods(): Unit =
    val state = FloatingPodState(
      UUID.fromString("00000000-0000-0000-0000-000000000805"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000806")),
      9L,
      "reconciliation-world",
      0D,
      64D,
      0D,
      0F,
      0F,
      false,
      101L,
      None
    )
    val inventory = new FreshBootInactiveInventory(state)

    val decision = new StartupReconciliation(inventory).decidePublication()

    assertEquals(StartupPublicationDecision.Permit(Nil), decision)
    assertTrue(inventory.events.isEmpty)
    assertEquals(0, inventory.databaseWriteAttempts)
    assertTrue(inventory.ticketUuids.isEmpty)
    assertTrue(inventory.entityUuids.isEmpty)
    assertTrue(inventory.managerUuids.isEmpty)

  private final class CompleteInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var tickets = Set.empty[UUID]
    private var entities = Set.empty[UUID]
    private var mappings = Set.empty[UUID]

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = tickets
    override def entityUuids: Set[UUID] = entities
    override def managerUuids: Set[UUID] = mappings
    override def requestTicket(podUuid: UUID): Unit =
      events += s"ticket:$podUuid"
      tickets += podUuid
    override def spawnEntity(active: FloatingPodState): Unit =
      events += s"entity:${active.uuid}"
      entities += active.uuid
    override def publishMapping(active: FloatingPodState): Unit =
      events += s"mapping:${active.uuid}"
      mappings += active.uuid
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("active publication must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("active publication must not release a ticket")

  private final class InactiveInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var tickets = Set(state.uuid)
    private var entities = Set(state.uuid)

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = tickets
    override def entityUuids: Set[UUID] = entities
    override def managerUuids: Set[UUID] = Set.empty
    override def requestTicket(podUuid: UUID): Unit = throw new AssertionError("inactive remediation must not request a ticket")
    override def spawnEntity(active: FloatingPodState): Unit = throw new AssertionError("inactive remediation must not spawn an entity")
    override def publishMapping(active: FloatingPodState): Unit = throw new AssertionError("inactive remediation must not publish a mapping")
    override def removeEntity(podUuid: UUID): Unit =
      events += s"remove:$podUuid"
      entities -= podUuid
    override def releaseTicket(podUuid: UUID): Unit =
      events += s"release:$podUuid"
      tickets -= podUuid

  private final class AttachedPhysicalInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var mappings = Set.empty[UUID]

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = Set(state.uuid)
    override def entityUuids: Set[UUID] = Set(state.uuid)
    override def managerUuids: Set[UUID] = mappings
    override def requestTicket(podUuid: UUID): Unit = throw new AssertionError("attached physical inventory must not request a ticket")
    override def spawnEntity(active: FloatingPodState): Unit = throw new AssertionError("attached physical inventory must not spawn an entity")
    override def publishMapping(active: FloatingPodState): Unit =
      events += s"mapping:${active.uuid}"
      mappings += active.uuid
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("active publication must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("active publication must not release a ticket")

  private final class TicketOnlyInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var entities = Set.empty[UUID]
    private var mappings = Set.empty[UUID]

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = Set(state.uuid)
    override def entityUuids: Set[UUID] = entities
    override def managerUuids: Set[UUID] = mappings
    override def requestTicket(podUuid: UUID): Unit = throw new AssertionError("ticket-only inventory must not request a ticket")
    override def spawnEntity(active: FloatingPodState): Unit =
      events += s"entity:${active.uuid}"
      entities += active.uuid
    override def publishMapping(active: FloatingPodState): Unit =
      events += s"mapping:${active.uuid}"
      mappings += active.uuid
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("active publication must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("active publication must not release a ticket")

  private final class EntityOnlyInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var tickets = Set.empty[UUID]
    private var mappings = Set.empty[UUID]

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = tickets
    override def entityUuids: Set[UUID] = Set(state.uuid)
    override def managerUuids: Set[UUID] = mappings
    override def requestTicket(podUuid: UUID): Unit =
      events += s"ticket:$podUuid"
      tickets += podUuid
    override def spawnEntity(active: FloatingPodState): Unit = throw new AssertionError("entity-only inventory must not spawn an entity")
    override def publishMapping(active: FloatingPodState): Unit =
      events += s"mapping:${active.uuid}"
      mappings += active.uuid
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("active publication must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("active publication must not release a ticket")

  private final class MultipleActiveRowsInventory(states: List[FloatingPodState]) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0
    private var tickets = Set.empty[UUID]
    private var entities = Set.empty[UUID]
    private var mappings = Set.empty[UUID]

    override def committedStates: List[FloatingPodState] = states
    override def ticketUuids: Set[UUID] = tickets
    override def entityUuids: Set[UUID] = entities
    override def managerUuids: Set[UUID] = mappings
    override def requestTicket(podUuid: UUID): Unit =
      events += s"ticket:$podUuid"
      tickets += podUuid
    override def spawnEntity(active: FloatingPodState): Unit =
      events += s"entity:${active.uuid}"
      entities += active.uuid
    override def publishMapping(active: FloatingPodState): Unit =
      events += s"mapping:${active.uuid}"
      mappings += active.uuid
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("active publication must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("active publication must not release a ticket")

  private final class EmptyCommittedInventory extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0

    override def committedStates: List[FloatingPodState] = Nil
    override def ticketUuids: Set[UUID] = Set.empty
    override def entityUuids: Set[UUID] = Set.empty
    override def managerUuids: Set[UUID] = Set.empty
    override def requestTicket(podUuid: UUID): Unit = events += "ticket"
    override def spawnEntity(active: FloatingPodState): Unit = events += "entity"
    override def publishMapping(active: FloatingPodState): Unit = events += "mapping"
    override def removeEntity(podUuid: UUID): Unit = events += "remove"
    override def releaseTicket(podUuid: UUID): Unit = events += "release"

  private final class FreshBootInactiveInventory(state: FloatingPodState) extends StartupReconciliationInventory:
    val events = ListBuffer.empty[String]
    var databaseWriteAttempts = 0

    override def committedStates: List[FloatingPodState] = List(state)
    override def ticketUuids: Set[UUID] = Set.empty
    override def entityUuids: Set[UUID] = Set.empty
    override def managerUuids: Set[UUID] = Set.empty
    override def requestTicket(podUuid: UUID): Unit = throw new AssertionError("fresh boot inactive state must not request a ticket")
    override def spawnEntity(active: FloatingPodState): Unit = throw new AssertionError("fresh boot inactive state must not spawn an entity")
    override def publishMapping(active: FloatingPodState): Unit = throw new AssertionError("fresh boot inactive state must not publish a mapping")
    override def removeEntity(podUuid: UUID): Unit = throw new AssertionError("fresh boot inactive state must not remove an entity")
    override def releaseTicket(podUuid: UUID): Unit = throw new AssertionError("fresh boot inactive state must not release a ticket")
