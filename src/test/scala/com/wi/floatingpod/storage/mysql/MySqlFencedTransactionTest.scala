package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement

import java.sql.SQLException
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

final class MySqlFencedTransactionTest:
  @Test
  def primitiveExecutionRejectsFullSnapshotWitnessWithoutChangingItsEffects(): Unit =
    val before = MySqlFullRepositorySnapshot.empty
    val after = before.copy(pods = Vector(row()))
    val plan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.ReplaceFullRepositorySnapshot(before, after)))
    val effects = plan.expectedEffects

    val failure = assertThrows(classOf[IllegalArgumentException], () => plan.requirePrimitiveExecution())
    assertEquals("full repository snapshot plans require executeStaged", failure.getMessage)
    assertEquals(effects, plan.expectedEffects)
    assertEquals(effects, plan.operationContext(UUID.randomUUID(), 1L, UUID.randomUUID(), UUID.randomUUID()).expectedEffects)

  @Test
  def primitiveExecutionAcceptsItsFourSupportedMutations(): Unit =
    val before = row()
    val after = before.copy(stateVersion = 1L)
    val entitlement = PlayerEntitlement(UUID.randomUUID(), 4, 2)
    Vector(
      MySqlBusinessMutation.InsertInactivePod(before),
      MySqlBusinessMutation.ReplaceInactivePod(before, after),
      MySqlBusinessMutation.DeleteInactivePod(before),
      MySqlBusinessMutation.ChangeEntitlement(None, Some(entitlement))
    ).foreach(mutation => MySqlBusinessEffectPlan(Vector(mutation)).requirePrimitiveExecution())

  @Test
  def entitlementUpdateMustKeepItsPlayerIdentity(): Unit =
    val before = PlayerEntitlement(uuid("00000000-0000-0000-0000-000000000001"), 4, 2)
    val differentPlayer = PlayerEntitlement(uuid("00000000-0000-0000-0000-000000000002"), 5, 3)
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlBusinessMutation.ChangeEntitlement(Some(before), Some(differentPlayer))
    )

  @Test
  def entitlementUpdateForTheSamePlayerKeepsThatExactTarget(): Unit =
    val before = PlayerEntitlement(uuid("00000000-0000-0000-0000-000000000003"), 4, 2)
    val desired = PlayerEntitlement(before.playerUuid, 5, 3)
    val mutation = MySqlBusinessMutation.ChangeEntitlement(Some(before), Some(desired))

    assertEquals("entitlement/" + before.playerUuid.toString, mutation.targetIdentity)
    assertEquals(Some(before), mutation.expectedEntitlement)
    assertEquals(Some(desired), mutation.desiredEntitlement)

  @Test
  def inactivePodPlanFingerprintIncludesTheFixedCallerTimestamp(): Unit =
    val operation = uuid("00000000-0000-0000-0000-000000000010")
    val boot = uuid("00000000-0000-0000-0000-000000000011")
    val binding = uuid("00000000-0000-0000-0000-000000000012")
    val firstPlan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(row(lastUpdated = 1700000000001L))))
    val retryPlan = MySqlBusinessEffectPlan(Vector(MySqlBusinessMutation.InsertInactivePod(row(lastUpdated = 1700000000002L))))
    val first = firstPlan.operationContext(boot, 8L, binding, operation)
    val changedRetry = retryPlan.operationContext(boot, 8L, binding, operation)

    assertNotEquals(first.canonicalFingerprintHex, changedRetry.canonicalFingerprintHex)
    assertFalse(first.isExactReplayOf(changedRetry))

  @Test
  def activeRowsCannotEnterThisInactiveMutationSlice(): Unit =
    val active = row(owner = Some(uuid("00000000-0000-0000-0000-000000000021")), isActiveFlag = 1)
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlBusinessMutation.InsertInactivePod(active)
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlBusinessMutation.DeleteInactivePod(active)
    )

  @Test
  def planRejectsTwoEffectsForOnePodTarget(): Unit =
    val before = row(stateVersion = 4L, lastUpdated = 1700000000000L)
    val after = before.copy(stateVersion = 5L, lastUpdated = 1700000000001L)
    val insert = MySqlBusinessMutation.InsertInactivePod(row(lastUpdated = 1700000000002L))
    val replacement = MySqlBusinessMutation.ReplaceInactivePod(before, after)
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlBusinessEffectPlan(Vector(insert, replacement))
    )

  @Test
  def swallowedNestedTransactionFailureMarksTheOuterTransactionRollbackOnly(): Unit =
    val connectionIdentity = new Object()
    val outerRollbackOnly = new MySqlRollbackOnlyState
    val nestedRollbackOnly = new MySqlRollbackOnlyState

    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, outerRollbackOnly).isEmpty)
    val nestedFailure = MySqlConnectionTransactionGate.enter(connectionIdentity, nestedRollbackOnly).get
    val callbackResult = scala.util.Success(()) // 外层回调吞掉 nested execute 的 Failure。
    assertTrue(callbackResult.isSuccess)
    assertSame(nestedFailure, outerRollbackOnly.cause.get)

    val laterFailure = new SQLException("a later mutation failure")
    outerRollbackOnly.mark(laterFailure)
    assertTrue(nestedFailure.getSuppressed.contains(laterFailure))
    MySqlConnectionTransactionGate.leave(connectionIdentity, outerRollbackOnly)

  @Test
  def nestedTransactionGateUsesConnectionIdentityAndCanBeReleased(): Unit =
    val connectionIdentity = new Object()
    val firstState = new MySqlRollbackOnlyState
    val secondState = new MySqlRollbackOnlyState
    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, firstState).isEmpty)
    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, secondState).isDefined)
    MySqlConnectionTransactionGate.leave(connectionIdentity, firstState)
    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, secondState).isEmpty)
    MySqlConnectionTransactionGate.leave(connectionIdentity, secondState)

  @Test
  def activeGateOwnerCapabilityIsBoundToConnectionThreadStateAndLifecycle(): Unit =
    val connectionIdentity = new Object()
    val otherConnection = new Object()
    val rollbackOnly = new MySqlRollbackOnlyState
    val otherState = new MySqlRollbackOnlyState

    assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.currentOwner(connectionIdentity, rollbackOnly))
    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, rollbackOnly).isEmpty)
    val owner = MySqlConnectionTransactionGate.currentOwner(connectionIdentity, rollbackOnly)
    MySqlConnectionTransactionGate.requireCurrent(owner, connectionIdentity, rollbackOnly)

    assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.requireCurrent(owner, otherConnection, rollbackOnly))
    assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.requireCurrent(owner, connectionIdentity, otherState))

    val offThreadFailure = new AtomicReference[Throwable]()
    val runOffThread: Runnable = () =>
      try
        MySqlConnectionTransactionGate.requireCurrent(owner, connectionIdentity, rollbackOnly)
      catch
        case failure: Throwable => offThreadFailure.set(failure)
    val otherThread = new Thread(runOffThread)
    otherThread.start()
    otherThread.join()
    assertTrue(offThreadFailure.get().isInstanceOf[IllegalStateException])

    MySqlConnectionTransactionGate.leave(connectionIdentity, rollbackOnly)
    assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.requireCurrent(owner, connectionIdentity, rollbackOnly))
    assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.currentOwner(connectionIdentity, rollbackOnly))

  @Test
  def swallowedNestedFailureInvalidatesFurtherUseOfTheOuterGateOwner(): Unit =
    val connectionIdentity = new Object()
    val outerRollbackOnly = new MySqlRollbackOnlyState
    val nestedRollbackOnly = new MySqlRollbackOnlyState
    assertTrue(MySqlConnectionTransactionGate.enter(connectionIdentity, outerRollbackOnly).isEmpty)
    val owner = MySqlConnectionTransactionGate.currentOwner(connectionIdentity, outerRollbackOnly)

    val nestedFailure = MySqlConnectionTransactionGate.enter(connectionIdentity, nestedRollbackOnly).get
    val swallowedResult = scala.util.Success(())
    assertTrue(swallowedResult.isSuccess)
    val rejectedRead = assertThrows(classOf[IllegalStateException], () =>
      MySqlConnectionTransactionGate.requireCurrent(owner, connectionIdentity, outerRollbackOnly))
    assertSame(nestedFailure, rejectedRead)
    MySqlConnectionTransactionGate.leave(connectionIdentity, outerRollbackOnly)

  @Test
  def postCommitQualificationAcceptsNoRetainedTagAndRejectsNullOptionAndNullElement(): Unit =
    MySqlFencedTransaction.requireModePostCommitRetainedTag(None)

    val nullElement: Option[com.wi.floatingpod.server.io.IoOperationTag] =
      Some(null.asInstanceOf[com.wi.floatingpod.server.io.IoOperationTag])
    val elementFailure = assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlFencedTransaction.requireModePostCommitRetainedTag(nullElement)
    )
    assertEquals("post-commit retained tag must not be null", elementFailure.getMessage)

    val optionFailure = assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlFencedTransaction.requireModePostCommitRetainedTag(
        null.asInstanceOf[Option[com.wi.floatingpod.server.io.IoOperationTag]]
      )
    )
    assertEquals("post-commit retained tag option must not be null", optionFailure.getMessage)

  @Test
  def modePreflightFailureNeverSchedulesMutationOfAnUnownedJdbcTransaction(): Unit =
    val callerTransaction = MySqlModeCriticalCleanupPolicy.decide(
      transactionAttempted = false,
      idleAutocommitObserved = false,
      rollbackAcknowledged = true,
      autocommitRestored = false,
      isolationChangeAttempted = false,
      isolationRead = false,
      cleanupFailed = false
    )
    assertFalse(callerTransaction.rollback)
    assertFalse(callerTransaction.restoreAutoCommit)
    assertFalse(callerTransaction.restoreIsolation)
    assertFalse(callerTransaction.connectionUncertain)

    val getterFailedBeforeOwnership = MySqlModeCriticalCleanupPolicy.decide(
      transactionAttempted = false,
      idleAutocommitObserved = false,
      rollbackAcknowledged = true,
      autocommitRestored = false,
      isolationChangeAttempted = false,
      isolationRead = false,
      cleanupFailed = false
    )
    assertEquals(callerTransaction, getterFailedBeforeOwnership,
      "an unreadable autocommit preflight also provides no authority to rollback or restore session state")

  @Test
  def modeCriticalCleanupMutatesOnlyAfterOwnedTransactionOrIsolationChange(): Unit =
    val idleIsolationFailure = MySqlModeCriticalCleanupPolicy.decide(
      transactionAttempted = false,
      idleAutocommitObserved = true,
      rollbackAcknowledged = true,
      autocommitRestored = true,
      isolationChangeAttempted = true,
      isolationRead = true,
      cleanupFailed = false
    )
    assertFalse(idleIsolationFailure.rollback)
    assertFalse(idleIsolationFailure.restoreAutoCommit)
    assertTrue(idleIsolationFailure.restoreIsolation)
    assertFalse(idleIsolationFailure.connectionUncertain)

    val uncertainOwnedRollback = MySqlModeCriticalCleanupPolicy.decide(
      transactionAttempted = true,
      idleAutocommitObserved = true,
      rollbackAcknowledged = false,
      autocommitRestored = false,
      isolationChangeAttempted = true,
      isolationRead = true,
      cleanupFailed = true
    )
    assertTrue(uncertainOwnedRollback.rollback)
    assertFalse(uncertainOwnedRollback.restoreAutoCommit)
    assertFalse(uncertainOwnedRollback.restoreIsolation)
    assertTrue(uncertainOwnedRollback.connectionUncertain)

  @Test
  def exactRawReadbackDistinguishesTheDoubleBitPattern(): Unit =
    val positiveZero = row(posX = 0.0d)
    val negativeZero = row(posX = -0.0d)
    assertFalse(MySqlBusinessRowEquality.pod(positiveZero, negativeZero))
    assertTrue(MySqlBusinessRowEquality.pod(positiveZero, positiveZero.copy()))

  private def row(
      stateVersion: Long = 0L,
      lastUpdated: Long = 1700000000000L,
      posX: Double = 10.0d,
      owner: Option[UUID] = None,
      isActiveFlag: Int = 0
  ): MySqlStoredRowData =
    MySqlStoredRowData(
      uuidBytes = MySqlStoredRows.encodeUuid(uuid("00000000-0000-0000-0000-0000000000aa")),
      ownerUuidBytes = owner.map(MySqlStoredRows.encodeUuid),
      stateVersion = stateVersion,
      worldName = "world",
      posX = posX,
      posY = 64.0d,
      posZ = -3.0d,
      yaw = 90.0d,
      pitch = 0.0d,
      isActiveFlag = isActiveFlag,
      lastUpdated = lastUpdated,
      dimensionId = Some(0),
      operatingModeToken = "NORMAL"
    )

  private def uuid(value: String): UUID = UUID.fromString(value)
