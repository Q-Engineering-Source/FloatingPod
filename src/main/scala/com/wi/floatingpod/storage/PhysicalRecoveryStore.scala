package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.FloatingPodState

import java.util.UUID
import scala.util.{Failure, Try}

/** V004 locator 的持久化状态；本切片只写入 CREATE_PREPARED。 */
sealed trait PhysicalRecoveryLocatorState:
  private[storage] def databaseValue: String

object PhysicalRecoveryLocatorState:
  case object CreatePrepared extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "CREATE_PREPARED"
  case object RowActive extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "ROW_ACTIVE"
  case object RowInactivePending extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "ROW_INACTIVE_PENDING"
  case object RowInactiveVerified extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "ROW_INACTIVE_VERIFIED"
  case object AbsentPending extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "ABSENT_PENDING"
  case object AbsentVerified extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "ABSENT_VERIFIED"
  case object LegacyUncertified extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "LEGACY_UNCERTIFIED"
  case object Quarantined extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "QUARANTINED"

  private[storage] val all: List[PhysicalRecoveryLocatorState] = List(CreatePrepared, RowActive, RowInactivePending, RowInactiveVerified, AbsentPending, AbsentVerified, LegacyUncertified, Quarantined)
  private[storage] def fromDatabase(value: String): PhysicalRecoveryLocatorState =
    all.find(_.databaseValue == value).getOrElse(throw new IllegalStateException(s"Unknown physical recovery locator state $value"))

object PhysicalRecoveryLocatorReason:
  val CreatePrepared = "create_prepared"
  val RowActive = "row_active"
  val RowInactivePending = "row_inactive_pending"
  val FreshBootAbsenceVerified = "fresh_boot_absence_verified"

/** V004 singleton binding 的 cutover 状态；本切片只写入 LegacyUncertified。 */
sealed trait PhysicalRecoveryCutoverState:
  private[storage] def databaseValue: String

object PhysicalRecoveryCutoverState:
  case object LegacyUncertified extends PhysicalRecoveryCutoverState:
    override private[storage] val databaseValue = "LEGACY_UNCERTIFIED"
  case object Certified extends PhysicalRecoveryCutoverState:
    override private[storage] val databaseValue = "CERTIFIED"

  private[storage] val all: List[PhysicalRecoveryCutoverState] = List(LegacyUncertified, Certified)
  private[storage] def fromDatabase(value: String): PhysicalRecoveryCutoverState =
    all.find(_.databaseValue == value).getOrElse(throw new IllegalStateException(s"Unknown physical recovery cutover state $value"))

/** immutable V004 singleton binding；UUID 由调用方提供，storage 绝不自动生成。 */
final case class PhysicalRecoveryBinding(bindingUuid: UUID, cutoverState: PhysicalRecoveryCutoverState, formatVersion: Int):
  require(bindingUuid != null && cutoverState != null, "physical recovery binding values must not be null")
  require(formatVersion >= 1, "physical recovery binding formatVersion must be at least 1")

/** 被强加载 ticket 覆盖的一个精确物理 chunk。 */
final case class PhysicalRecoveryChunk(dimensionId: Int, chunkX: Int, chunkZ: Int)

/** immutable V004 parent row 与其完整 chunk inventory。 */
final case class PhysicalRecoveryLocator(
    podUuid: UUID,
    attemptUuid: UUID,
    state: PhysicalRecoveryLocatorState,
    reasonCode: String,
    domainVersion: Option[Long],
    targetDimensionId: Option[Int],
    chunks: List[PhysicalRecoveryChunk]
):
  require(podUuid != null && attemptUuid != null && state != null && reasonCode != null && reasonCode.trim.nonEmpty && domainVersion != null && targetDimensionId != null && chunks != null, "physical recovery locator values must not be null")
  require(domainVersion.forall(_ >= 0L), "domainVersion must not be negative")

/** V004 的物理恢复存储边界；prepared locator 只能和对应领域激活原子地一同提交。 */
trait PhysicalRecoveryStore:
  def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]]
  def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding]
  def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding]
  def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator]
  def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome]
  def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome]
  def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]]
  def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] =
    Failure(new UnsupportedOperationException("Global physical recovery locator snapshots are not implemented by this store"))
  def verifyInactiveAbsence(podUuid: UUID, expectedStateVersion: Long, attemptUuid: UUID): Try[PhysicalRecoveryLocator] =
    Failure(new UnsupportedOperationException("Inactive absence verification is not implemented by this store"))
