package com.wi.floatingpod.server

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.network.ActiveCapacityPolicy
import com.wi.floatingpod.storage.{FloatingPodRepository, RecoveryDimensionBackfillOutcome, RecoveryPodStore, StoredPodRow}

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable
import scala.util.{Success, Try}

/** 从 handler 可直接调用的纯服务端验证 seam 行为测试。 */
class ServerValidationTest:
  @Test
  def ownershipValidatorAcceptsARecentMatchingOwnerAndVersion(): Unit =
    val podUuid = uuidOf(1)
    val playerUuid = uuidOf(2)
    val state = stateFor(podUuid, Some(playerUuid), 3L)
    val repository = new TestRepository(List(state))

    val result = new OwnershipValidator(repository).validate(podUuid, playerUuid, 3L)

    assertEquals(OwnershipAccepted(state), result)
    assertEquals(1, repository.findCalls)

  @Test
  def ownershipValidatorRejectsUnknownWrongOwnerAndStaleVersionFromFreshReads(): Unit =
    val podUuid = uuidOf(3)
    val ownerUuid = uuidOf(4)
    val repository = new TestRepository(List(stateFor(podUuid, Some(ownerUuid), 6L)))
    val validator = new OwnershipValidator(repository)

    assertEquals(UnknownPod, validator.validate(uuidOf(5), ownerUuid, 0L))
    assertEquals(NotOwner, validator.validate(podUuid, uuidOf(6), 6L))
    assertEquals(VersionMismatch(6L), validator.validate(podUuid, ownerUuid, 5L))
    assertEquals(3, repository.findCalls)

  @Test
  def activeCapacityPolicyUsesCurrentDesiredActiveRowsBelowAndAtLimit(): Unit =
    val belowPolicy = new ActiveCapacityPolicy(new DesiredRows(desiredRows(1)), 2)
    val atPolicy = new ActiveCapacityPolicy(new DesiredRows(desiredRows(2)), 2)

    assertEquals(1, belowPolicy.activeCount)
    assertTrue(belowPolicy.canRelease)
    assertEquals(2, atPolicy.activeCount)
    assertFalse(atPolicy.canRelease)

  @Test
  def activeCapacityPolicyRejectsInvalidConfiguredLimits(): Unit =
    val store = new DesiredRows(Nil)

    assertThrows(classOf[IllegalArgumentException], () => new ActiveCapacityPolicy(store, 0))
    assertThrows(classOf[IllegalArgumentException], () => new ActiveCapacityPolicy(store, 257))

  @Test
  def activeCapacityCountsDesiredDatabaseRowsEvenWhenNoRuntimeEntityExists(): Unit =
    val legacyDesiredActive = StoredPodRow(
      uuidOf(77),
      None,
      0L,
      "legacy",
      0.0D,
      64.0D,
      0.0D,
      0.0F,
      0.0F,
      isActive = true,
      0L,
      None
    )
    val policy = new ActiveCapacityPolicy(new DesiredRows(List(legacyDesiredActive)), 1)

    assertEquals(1, policy.activeCount)
    assertFalse(policy.canRelease)

  @Test
  def positionValidatorAcceptsSupportedReachablePosition(): Unit =
    val result = new PositionValidator().validate(
      AuthoritativePosition(0.0D, 64.0D, 0.0D),
      ReleasePosition(3.0D, 64.0D, 4.0D),
      5.0D,
      hasSolidSupport = true,
      hasTwoBlockHeadroom = true
    )

    assertEquals(PositionValidation.Accepted, result)

  @Test
  def positionValidatorRejectsRangeVerticalSupportAndHeadroomFailures(): Unit =
    val validator = new PositionValidator()
    val player = AuthoritativePosition(0.0D, 64.0D, 0.0D)

    assertEquals(PositionValidation.OutsideReach, validator.validate(player, ReleasePosition(6.0D, 64.0D, 0.0D), 5.0D, true, true))
    assertEquals(PositionValidation.OutsideVerticalBounds, validator.validate(player, ReleasePosition(0.0D, 256.0D, 0.0D), 300.0D, true, true))
    assertEquals(PositionValidation.Unsupported, validator.validate(player, ReleasePosition(0.0D, 64.0D, 0.0D), 5.0D, false, true))
    assertEquals(PositionValidation.Obstructed, validator.validate(player, ReleasePosition(0.0D, 64.0D, 0.0D), 5.0D, true, false))

  @Test
  def positionValidatorRejectsNonFinitePositionAndReach(): Unit =
    val validator = new PositionValidator()

    assertEquals(PositionValidation.NonFiniteInput, validator.validate(AuthoritativePosition(Double.NaN, 64.0D, 0.0D), ReleasePosition(0.0D, 64.0D, 0.0D), 5.0D, true, true))
    assertEquals(PositionValidation.NonFiniteInput, validator.validate(AuthoritativePosition(0.0D, 64.0D, 0.0D), ReleasePosition(0.0D, 64.0D, 0.0D), Double.PositiveInfinity, true, true))

  private def uuidOf(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")

  private def stateFor(uuid: UUID, ownerUuid: Option[UUID], version: Long): FloatingPodState =
    FloatingPodState(uuid, ownerUuid, version, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, isActive = false, 0L)

  private def desiredRows(count: Int): List[StoredPodRow] =
    (1 to count).toList.map { index =>
      StoredPodRow(uuidOf(200 + index), None, 0L, "overworld", 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, isActive = true, 0L, Some(0))
    }

  private final class TestRepository(initialStates: List[FloatingPodState]) extends FloatingPodRepository:
    private val states = mutable.HashMap.from(initialStates.map(state => state.uuid -> state))
    var findCalls = 0
    override def insert(state: FloatingPodState): Try[FloatingPodState] =
      states.put(state.uuid, state)
      Success(state)
    override def update(state: FloatingPodState): Try[FloatingPodState] =
      states.put(state.uuid, state)
      Success(state)
    override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] =
      findCalls += 1
      Success(states.get(uuid))
    override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] = Success(states.values.filter(_.ownerUuid.contains(ownerUuid)).toList)
    override def findAllActive(): Try[List[FloatingPodState]] = Success(states.values.filter(_.isActive).toList)
    override def findAll(): Try[List[FloatingPodState]] = Success(states.values.toList)
    override def delete(uuid: UUID): Try[Boolean] = Success(states.remove(uuid).nonEmpty)
    override def transaction[T](fn: => T): Try[T] = Try(fn)
    override def close(): Unit = ()

  private final class DesiredRows(rows: List[StoredPodRow]) extends RecoveryPodStore:
    override def findDesiredActiveRows(): Try[List[StoredPodRow]] = Success(rows)
    override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] = Success(None)
    override def backfillMissingActiveDimension(uuid: UUID, expectedStateVersion: Long, authoritativeDimensionId: Int): Try[RecoveryDimensionBackfillOutcome] = throw new UnsupportedOperationException
    override def findAllRecoverable(): Try[List[FloatingPodState]] = Success(Nil)
