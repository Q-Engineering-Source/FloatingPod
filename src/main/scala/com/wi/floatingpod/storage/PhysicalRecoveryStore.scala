package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}

import java.util.UUID
import scala.util.{Failure, Try}

/** 来源目录中观察到的外部未绑定实体类别；此标签不代表数据库完成了世界清点。 */
enum ObservedUnboundSourceKind:
  case FreshArtifact, LegacyUnregistered

/** 外部实体在某次只读目录代次中的不可变身份。 */
final case class ObservedUnboundIdentity(sourceKind: ObservedUnboundSourceKind, catalogGeneration: UUID, sourceIdentity: String):
  require(sourceKind != null && catalogGeneration != null && sourceIdentity != null && sourceIdentity.trim.nonEmpty, "observed unbound identity must be complete")

/** 外部现有实体由协调器采集的 UUID、身份与实际 pose。 */
final case class ObservedUnboundPod(podUuid: UUID, identity: ObservedUnboundIdentity, pose: FloatingPodPose):
  require(podUuid != null && identity != null && pose != null, "observed unbound pod values must not be null")

/**
 * 协调器在只读目录验证后交给 storage 的不可变输入收据。
 * 本数据层只校验收据与 observation 一致，不把此对象视为全世界唯一性证明。
 */
final case class ObservedUnboundUniquenessReceipt(bindingUuid: UUID, observation: ObservedUnboundPod):
  require(bindingUuid != null && observation != null, "observed unbound uniqueness receipt values must not be null")

/** 持久化未绑定 intent 的独立来源，避免与 owner-authenticated CREATE_PREPARED 混同。 */
enum UnboundIntentOrigin:
  case ObservedArtifactAdoption, DebugItemCreate

/** 与 SQLite PREPARED intent 完全对应的不可变收据，可在 close/reopen 后再次读取。 */
final case class UnboundPreparationReceipt(
    binding: PhysicalRecoveryBinding,
    podUuid: UUID,
    attemptUuid: UUID,
    origin: UnboundIntentOrigin,
    targetPose: FloatingPodPose,
    observedIdentity: Option[ObservedUnboundIdentity],
    createOperationUuid: Option[UUID],
    preparedVersion: Long
):
  require(binding != null && podUuid != null && attemptUuid != null && origin != null && targetPose != null, "unbound preparation receipt identity must not be null")
  require(observedIdentity != null && createOperationUuid != null, "unbound preparation receipt options must not be null")
  require(preparedVersion == 0L, "unbound preparations currently begin at version zero")
  require(
    (origin == UnboundIntentOrigin.ObservedArtifactAdoption && observedIdentity.nonEmpty && createOperationUuid.isEmpty)
      || (origin == UnboundIntentOrigin.DebugItemCreate && observedIdentity.isEmpty && createOperationUuid.nonEmpty),
    "unbound preparation receipt origin fields are inconsistent"
  )

sealed trait UnboundCommitOutcome

object UnboundCommitOutcome:
  final case class Committed(state: FloatingPodState) extends UnboundCommitOutcome
  final case class AlreadyCommitted(state: FloatingPodState) extends UnboundCommitOutcome
  final case class Rejected(reason: UnboundCommitRejection) extends UnboundCommitOutcome

sealed trait UnboundCommitRejection

object UnboundCommitRejection:
  final case class CapacityReached(activeCount: Int, limit: Int) extends UnboundCommitRejection
  case object ActualPoseMismatch extends UnboundCommitRejection

/** 所有者对活动辅助机进行 mode CAS 后的持久化结果。 */
sealed trait OperatingModeChangeOutcome

object OperatingModeChangeOutcome:
  final case class Changed(state: FloatingPodState) extends OperatingModeChangeOutcome
  final case class AlreadySet(state: FloatingPodState) extends OperatingModeChangeOutcome
  final case class Rejected(reason: OperatingModeChangeRejection) extends OperatingModeChangeOutcome

/** mode CAS 的稳定业务拒绝；物理登记矛盾则作为基础设施失败传播。 */
enum OperatingModeChangeRejection:
  case UnknownPod, NotOwner, Inactive, BindingMismatch, RecoveryObligationMismatch, CrossChunkMovement
  case VersionMismatch(actualVersion: Long)

/** V004 locator 的持久化状态；本切片只写入 CREATE_PREPARED。 */
sealed trait PhysicalRecoveryLocatorState:
  private[storage] def databaseValue: String

object PhysicalRecoveryLocatorState:
  case object CreatePrepared extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "CREATE_PREPARED"
  case object UnboundAdoptionPrepared extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "UNBOUND_ADOPTION_PREPARED"
  case object DebugItemCreatePrepared extends PhysicalRecoveryLocatorState:
    override private[storage] val databaseValue = "DEBUG_ITEM_CREATE_PREPARED"
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

  private[storage] val all: List[PhysicalRecoveryLocatorState] = List(CreatePrepared, UnboundAdoptionPrepared, DebugItemCreatePrepared, RowActive, RowInactivePending, RowInactiveVerified, AbsentPending, AbsentVerified, LegacyUncertified, Quarantined)
  private[storage] def fromDatabase(value: String): PhysicalRecoveryLocatorState =
    all.find(_.databaseValue == value).getOrElse(throw new IllegalStateException(s"Unknown physical recovery locator state $value"))

object PhysicalRecoveryLocatorReason:
  val CreatePrepared = "create_prepared"
  val UnboundAdoptionPrepared = "unbound_adoption_prepared"
  val DebugItemCreatePrepared = "debug_item_create_prepared"
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
  /** 将已被只读目录观察到的 ownerless 原体持久化为非成功的 adoption preparation。 */
  def prepareObservedUnbound(
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessProof: ObservedUnboundUniquenessReceipt
  ): Try[UnboundPreparationReceipt] =
    Failure(new UnsupportedOperationException("Observed unbound physical recovery preparation is not implemented by this store"))
  /** 仅在 exact 原位实体仍匹配准备收据且全服容量可用时提交 adoption。 */
  def commitObservedUnbound(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    Failure(new UnsupportedOperationException("Observed unbound physical recovery commit is not implemented by this store"))
  /** 为受控的新实体创建独立 DEBUG_ITEM_CREATE intent；不等价于 adoption。 */
  def prepareUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): Try[UnboundPreparationReceipt] =
    Failure(new UnsupportedOperationException("Controlled unbound creation preparation is not implemented by this store"))
  /** 提交与受控创建 operation 完全匹配的 unbound active 状态。 */
  def commitUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    Failure(new UnsupportedOperationException("Controlled unbound creation commit is not implemented by this store"))
  def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] =
    Failure(new UnsupportedOperationException("Unbound physical recovery intent reads are not implemented by this store"))
  def prepareCreateLocator(podUuid: UUID, expectedDomainVersion: Long, attemptUuid: UUID, targetDimensionId: Int, chunks: Set[PhysicalRecoveryChunk]): Try[PhysicalRecoveryLocator]
  def activatePreparedIfAdmissible(target: FloatingPodState, authenticatedOwner: UUID, expectedStateVersion: Long, dimensionId: Int, globalActiveLimit: Int, attemptUuid: UUID): Try[AdmissionOutcome]
  def deactivateTrackedIfExpectedActive(podUuid: UUID, expectedOwner: Option[UUID], expectedStateVersion: Long, attemptUuid: UUID): Try[DeactivationOutcome]
  /**
   * 原子提交已跟踪活动辅助机的同 chunk 非关键漂移，并同步 domain 与 ROW_ACTIVE locator 版本。
   *
   * @param state 基于当前已提交版本构造的漂移状态；身份、所有者、模式及活动维度不得改变。
   * @param expectedAttemptUuid 当前活动 locator 的精确操作身份。
   */
  def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] =
    Failure(new UnsupportedOperationException("Tracked active drift updates are not implemented by this store"))
  /** 精确绑定 owner 对已跟踪活动实体进行 mode+实际 pose 的单事务 CAS。 */
  def setOperatingModeTrackedIfOwned(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): Try[OperatingModeChangeOutcome] =
    Failure(new UnsupportedOperationException("Tracked operating mode changes are not implemented by this store"))
  def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]]
  def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] =
    Failure(new UnsupportedOperationException("Global physical recovery locator snapshots are not implemented by this store"))
  def verifyInactiveAbsence(podUuid: UUID, expectedStateVersion: Long, attemptUuid: UUID): Try[PhysicalRecoveryLocator] =
    Failure(new UnsupportedOperationException("Inactive absence verification is not implemented by this store"))
