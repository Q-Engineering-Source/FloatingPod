package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.SQLDataException
import java.util.UUID

/** 实际生产 decoder/factory/classifier；不把 typed fixture 当 SQL 成功。 */
class MySqlBootMainReadDataTest:
  import MainBootProjectionFixture.{snapshot as rawSnapshot, *}
  private def snapshot(extra: Map[String,Vector[Vector[MySqlFormat3Cell]]] = Map.empty,
      state: String = "READY"): MySqlFormat3FullSnapshot =
    val ids = if extra.contains("physical_recovery_intent") then Set(new UUID(0x0101010101010101L,0x0101010101010101L),id(40))
      else if extra.nonEmpty then Set(new UUID(0x0101010101010101L,0x0101010101010101L)) else Set.empty[UUID]
    rawSnapshot(withCommittedOperationReferences(extra,ids),state)
  @Test def originalBootInputDropRevokesViewsButLeavesRootForActualWorkerClose(): Unit =
    val source = snapshot()
    val scope = source.observationScope
    val tag = com.wi.floatingpod.server.io.IoOperationTag(scope.bootUuid, scope.bootGeneration,
      scope.connectionGeneration, UUID.randomUUID())
    val projected = MySqlOwnedFormat3Snapshot.fromAcceptedBoot(source, tag, 0L)
    val root = source.allocationBudget
    val charged = root.accountedMemoryBytes
    assertSame(root, projected.detachOriginalBootInputForExit(source, tag))
    assertFalse(root.isClosed, "drop cannot close a root before the session drops its own source")
    assertEquals(charged, root.accountedMemoryBytes)
    assertThrows(classOf[IllegalArgumentException], () => projected.requireBootReadData)
    assertThrows(classOf[IllegalArgumentException], () => projected.relations)
    assertThrows(classOf[IllegalArgumentException], () => projected.detachOriginalBootInputForExit(source, tag))

  @Test def foreignSourceOrEqualReplacementTagCannotDropOriginalBootInput(): Unit =
    val source = snapshot()
    val scope = source.observationScope
    val tag = com.wi.floatingpod.server.io.IoOperationTag(scope.bootUuid, scope.bootGeneration,
      scope.connectionGeneration, UUID.randomUUID())
    val projected = MySqlOwnedFormat3Snapshot.fromAcceptedBoot(source, tag, 0L)
    assertThrows(classOf[IllegalArgumentException], () => projected.detachOriginalBootInputForExit(snapshot(), tag))
    assertThrows(classOf[IllegalArgumentException], () => projected.detachOriginalBootInputForExit(source, tag.copy()))
    assertEquals(20, projected.relations.size)
    assertNotNull(projected.requireBootReadData)
    assertFalse(source.allocationBudget.isClosed)

  @Test def fullTwentyFactoryCarriesActualRawAndDecodedData(): Unit =
    val source = snapshot(Map("floating_pod_state" -> Vector(pod(10), pod(11,u(id(0)),active=1))))
    val root = source.allocationBudget
    val before = root.accountedMemoryBytes
    val projected = MySqlOwnedFormat3Snapshot.fromForBoot(source)
    val data = projected.requireBootReadData
    assertEquals(20,projected.relations.size)
    assertEquals(source.canonicalSha256Hex,projected.canonicalSha256Hex)
    assertSame(source.limitProfile,projected.limitProfile)
    assertTrue(root.accountedMemoryBytes > before)
    assertEquals(None,data.stored(id(10)).get.ownerUuid)
    assertEquals(Some(id(0)),data.stored(id(11)).get.ownerUuid)
    assertEquals(1,data.activeCount)
    assertEquals(1,data.activeCountFor(id(0)))
    assertEquals(None,data.entitlement(id(99)))
    assertEquals(None,data.stored(id(99)))
  @Test def rawNullDimensionRecoveryPreservedAndPublicInspectionRejects(): Unit =
    val projected=MySqlOwnedFormat3Snapshot.fromForBoot(snapshot(Map("floating_pod_state" ->
      Vector(pod(active=1,dimension=MySqlFormat3Cell.SqlNull)))))
    val data=projected.requireBootReadData
    assertEquals(None,data.recoveryRows.head.dimensionId)
    assertEquals(1,data.activeCount)
    assertThrows(classOf[IllegalStateException], () => data.state(id(10)))
    assertEquals(20,projected.relations.size)
  @Test def historyRemainsCompleteWhileCurrentReceiptUsesCurrentAttempt(): Unit =
    val source=snapshot(history())
    val projected=MySqlOwnedFormat3Snapshot.fromForBoot(source)
    val data=projected.requireBootReadData
    assertEquals(Some(id(40)),data.receipt(id(10)).get.createOperationUuid)
    assertEquals(id(20),data.locators.head.attemptUuid)
    assertEquals(25,projected.relation("physical_recovery_footprint_chunk").rows.size)
    val historical=MySqlOwnedFormat3Snapshot.fromForBoot(snapshot(history(seal="DRAFT",current=false)))
    assertEquals(3,historical.relation("physical_recovery_footprint_chunk").rows.size)
    assertEquals(None,historical.requireBootReadData.receipt(id(10)))
  @Test def zeroEntitlementAndImmutableBinaryKeepExactValues(): Unit =
    val e=row("player_entitlement","player_uuid"->u(id(10)),"binding_uuid"->u(id(2)),
      "registration_limit"->i(0),"simultaneous_active_limit"->i(0))
    val projected=MySqlOwnedFormat3Snapshot.fromForBoot(snapshot(Map("player_entitlement"->Vector(e))))
    assertEquals(0,projected.requireBootReadData.entitlement(id(10)).get.simultaneousActiveLimit)
    val binary=projected.relation("player_entitlement").rows.head.cells.collectFirst {
      case MySqlOwnedFormat3Value.Binary(value) => value
    }.get
    val expected=binary.bytes
    val escaped=binary.bytes
    java.util.Arrays.fill(escaped,7.toByte)
    assertArrayEquals(expected,binary.bytes)
  @Test def closedForeignBudgetAndMissingIssuerRejectBeforeProjection(): Unit =
    val source=snapshot()
    val foreign=snapshot()
    assertThrows(classOf[IllegalArgumentException], () => MySqlBootMainReadData.from(source,foreign.allocationBudget))
    val plain=MySqlOwnedFormat3Snapshot.from(source)
    assertThrows(classOf[IllegalArgumentException], () => plain.requireBootReadData)
    val child=source.allocationBudget.newCanonicalScope()
    child.closeScopedReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlBootMainReadData.from(source,child))
  @Test def lowRemainingOriginalLedgerRejectsRatherThanPrivateCapacity(): Unit =
    val source=snapshot(Map("floating_pod_state"->Vector(pod())))
    val other=source.allocationBudget.newCanonicalScope()
    other.reserveRetainedMemory(source.limitProfile.maxRetainedBytes()-source.allocationBudget.accountedMemoryBytes-1L,
      "independent same-root retained obligation")
    try assertThrows(classOf[SQLDataException], () => MySqlOwnedFormat3Snapshot.fromForBoot(source))
    finally other.closeScopedReadBudget()
  @Test def bootstrappingAndMalformedScalarDoNotIssueReadData(): Unit =
    assertThrows(classOf[SQLDataException], () => MySqlOwnedFormat3Snapshot.fromForBoot(snapshot(state="BOOTSTRAPPING")))
    assertThrows(classOf[IllegalStateException], () => MySqlOwnedFormat3Snapshot.fromForBoot(snapshot(Map(
      "floating_pod_state"->Vector(pod(active=2))))))
  @Test def fixedCommandsClassifyOnlyPotentialRepositoryMutations(): Unit =
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Renew))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.FindAll,null)))
    assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Transaction(Vector(MySqlRepositoryOperation.FindAll),null)))
    assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.Delete(id(10)),null)))
    assertTrue(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.Transaction(Vector(MySqlRepositoryOperation.FindAll,
      MySqlRepositoryOperation.Delete(id(10))),null)))
