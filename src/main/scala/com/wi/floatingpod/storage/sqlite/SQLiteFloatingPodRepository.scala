package com.wi.floatingpod.storage.sqlite

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, ObservedUnboundIdentity, ObservedUnboundPod, ObservedUnboundSourceKind, ObservedUnboundUniquenessReceipt, OperatingModeChangeOutcome, OperatingModeChangeRejection, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore, PlayerEntitlementStore, RecoveryDimensionBackfillOutcome, RecoveryDimensionBackfillRejection, RecoveryPodStore, StoredPodRow, UnboundCommitOutcome, UnboundCommitRejection, UnboundIntentOrigin, UnboundPreparationReceipt}
import com.wi.floatingpod.storage.OptimisticLockException

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.lang.Math
import java.nio.ByteBuffer
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.util.Objects
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * 使用单一 JDBC 连接实现 SQLite 恢复存储，并以同步访问和乐观锁保护服务端权威写入。
 *
 * @param dbPath SQLite JDBC URL 或数据库路径；`:memory:` 可用于隔离测试。
 */
final class SQLiteFloatingPodRepository private (
    dbPath: String,
    injectedConnection: Option[Connection],
    injectedTransactionControl: Option[TransactionControl]
) extends FloatingPodRepository, ActivityTransitionStore, PlayerEntitlementStore, RecoveryPodStore, PhysicalRecoveryStore:
  require(dbPath != null && dbPath.trim.nonEmpty, "dbPath must not be blank")

  import SQLiteFloatingPodRepository.*

  private val logger: Logger = LogManager.getLogger("floatingpod-storage")
  private var resources: Resources = initializeResources()
  private var connection: Connection = resources.connection
  private var transactionControl: TransactionControl = resources.transactionControl
  private var closed = false

  /**
   * 打开由路径指定的 SQLite 连接，并由同一连接的 JDBC transaction control 管理原子边界。
   *
   * @param dbPath SQLite JDBC URL 或数据库路径；`:memory:` 可用于隔离测试。
   */
  def this(dbPath: String) = this(dbPath, None, None)

  /**
   * 为同包失败注入测试提供真实连接与可控 transaction control，避免使用 reflection。
   *
   * @param connection 已打开的 SQLite JDBC 连接。
   * @param transactionControl 管理该连接 transaction 状态和关闭动作的边界。
   */
  private[sqlite] def this(connection: Connection, transactionControl: TransactionControl) =
    this(
      "injected SQLite connection",
      Some(Objects.requireNonNull(connection, "connection must not be null")),
      Some(Objects.requireNonNull(transactionControl, "transactionControl must not be null"))
    )

  /** 为需要验证文件数据库提交不确定后的真实 reopen 行为提供可控 transaction control。 */
  private[sqlite] def this(dbPath: String, connection: Connection, transactionControl: TransactionControl) =
    this(
      Objects.requireNonNull(dbPath, "dbPath must not be null"),
      Some(Objects.requireNonNull(connection, "connection must not be null")),
      Some(Objects.requireNonNull(transactionControl, "transactionControl must not be null"))
    )

  /**
   * 写入一条新状态；重复 UUID 明确失败，避免恢复记录被隐式覆盖。
   */
  override def insert(state: FloatingPodState): Try[FloatingPodState] =
    guarded("insert", Option(state).map(_.uuid), InsertSql) {
      Objects.requireNonNull(state, "state must not be null")
      if state.isActive then
        throw new IllegalArgumentException("Generic insert only accepts inactive FloatingPodState")
      val statement = connection.prepareStatement(InsertSql)
      JdbcResource.withResource(statement):
        try
          bindInsert(statement, state)
          if (statement.executeUpdate() != 1) then
            throw new IllegalStateException(s"Insert did not affect exactly one row for UUID ${state.uuid}")
          state
        catch
          case exception: SQLException if isUniqueConstraint(exception) =>
            throw new IllegalStateException(s"FloatingPodState already exists for UUID ${state.uuid}", exception)
    }

  /**
   * 用 `uuid` 和当前状态版本执行比较更新，成功后返回原子递增版本和当前时间戳。
   */
  override def update(state: FloatingPodState): Try[FloatingPodState] =
    guarded("update", Option(state).map(_.uuid), UpdateSql) {
      Objects.requireNonNull(state, "state must not be null")
      val current = selectStoredRow(state.uuid).getOrElse(throw new OptimisticLockException(state.uuid, state.stateVersion))
      if current.stateVersion != state.stateVersion then
        throw new OptimisticLockException(state.uuid, state.stateVersion)
      if current.operatingMode != state.operatingMode then
        throw new IllegalArgumentException(s"Generic update cannot change operating mode for pod ${state.uuid}")
      val nextVersion = Math.addExact(state.stateVersion, 1L)
      val updatedAt = System.currentTimeMillis()
      val statement = connection.prepareStatement(UpdateSql)
      JdbcResource.withResource(statement):
        bindUpdate(statement, state, updatedAt)
        if (statement.executeUpdate() != 1) then
          throw new OptimisticLockException(state.uuid, state.stateVersion)
        state.copy(
          stateVersion = nextVersion,
          lastUpdated = updatedAt,
          isActive = current.isActive,
          dimensionId = current.dimensionId,
          operatingMode = current.operatingMode
        )
    }

  /**
   * 读取一个 UUID 的恢复快照，不存在时返回 `None`。
   */
  override def findByUuid(uuid: UUID): Try[Option[FloatingPodState]] =
    guarded("findByUuid", Option(uuid), FindByUuidSql) {
      Objects.requireNonNull(uuid, "uuid must not be null")
      selectSingle(FindByUuidSql, statement => statement.setBytes(1, uuidToBytes(uuid)))
    }

  /**
   * 读取指定所有者拥有的全部状态，结果使用 UUID 稳定排序。
   */
  override def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]] =
    guarded("findByOwner", Option(ownerUuid), FindByOwnerSql) {
      Objects.requireNonNull(ownerUuid, "ownerUuid must not be null")
      selectMany(FindByOwnerSql, statement => statement.setBytes(1, uuidToBytes(ownerUuid)))
    }

  /**
   * 读取已标记为活动的状态，供服务端重建实体前校验。
   */
  override def findAllActive(): Try[List[FloatingPodState]] =
    guarded("findAllActive", None, FindAllActiveSql) {
      selectMany(FindAllActiveSql, _ => ())
    }

  /**
   * 读取全部状态供管理员导出，不按活动状态过滤。
   */
  override def findAll(): Try[List[FloatingPodState]] =
    guarded("findAll", None, FindAllSql) {
      selectMany(FindAllSql, _ => ())
    }

  /**
   * 读取迁移兼容的恢复行，并在向生命周期层暴露前拒绝活动空维度遗留数据。
   */
  override def findAllRecoverable(): Try[List[FloatingPodState]] =
    guarded("findAllRecoverable", None, FindAllSql) {
      selectStoredRows(FindAllSql, _ => ()).map(_.toPublicState)
    }

  /** 读取活动意图的原始行，使遗留活动空维度可以等待权威回调补齐。 */
  override def findDesiredActiveRows(): Try[List[StoredPodRow]] =
    guarded("findDesiredActiveRows", None, FindAllActiveSql) {
      selectStoredRows(FindAllActiveSql, _ => ())
    }

  /** 读取单条原始恢复行，不在此边界强制构造公开领域状态。 */
  override def findStoredByUuid(uuid: UUID): Try[Option[StoredPodRow]] =
    guarded("findStoredByUuid", Option(uuid), FindByUuidSql) {
      Objects.requireNonNull(uuid, "uuid must not be null")
      selectStoredRow(uuid)
    }

  override def findPhysicalRecoveryLocator(podUuid: UUID): Try[Option[PhysicalRecoveryLocator]] =
    guarded("findPhysicalRecoveryLocator", Option(podUuid), FindPhysicalRecoveryLocatorSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      selectPhysicalRecoveryLocator(podUuid)
    }

  override def findAllPhysicalRecoveryLocators(): Try[List[PhysicalRecoveryLocator]] =
    guarded("findAllPhysicalRecoveryLocators", None, FindAllPhysicalRecoveryLocatorPodUuidsSql) {
      selectAllPhysicalRecoveryLocators()
    }

  override def verifyInactiveAbsence(
      podUuid: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[PhysicalRecoveryLocator] =
    guarded("verifyInactiveAbsence", Option(podUuid), VerifyInactiveAbsenceLocatorSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      reservedWrite {
        selectStoredRow(podUuid) match
          case None => throw new IllegalStateException(s"Cannot verify inactive absence for unknown pod $podUuid")
          case Some(stored) if stored.isActive => throw new IllegalStateException(s"Cannot verify inactive absence for active pod $podUuid")
          case Some(stored) if stored.stateVersion != expectedStateVersion => throw new OptimisticLockException(podUuid, expectedStateVersion)
          case Some(stored) =>
            val dimensionId = stored.dimensionId.getOrElse(throw new IllegalStateException(s"Inactive physical recovery row has no dimension for pod $podUuid"))
            requireExactInactiveLocator(podUuid, attemptUuid, expectedStateVersion, dimensionId) match
              case pending @ PhysicalRecoveryLocator(_, _, PhysicalRecoveryLocatorState.RowInactivePending, PhysicalRecoveryLocatorReason.RowInactivePending, _, _, _) =>
                verifyInactiveLocator(podUuid, attemptUuid, expectedStateVersion, dimensionId)
                pending.copy(state = PhysicalRecoveryLocatorState.RowInactiveVerified, reasonCode = PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified)
              case verified @ PhysicalRecoveryLocator(_, _, PhysicalRecoveryLocatorState.RowInactiveVerified, PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified, _, _, _) => verified
              case locator => throw new IllegalStateException(s"Unexpected exact inactive physical recovery locator $locator")
      }
    }

  override def findPhysicalRecoveryBinding(): Try[Option[PhysicalRecoveryBinding]] =
    guarded("findPhysicalRecoveryBinding", None, FindPhysicalRecoveryBindingSql) {
      selectPhysicalRecoveryBinding()
    }

  override def createOrRetainLegacyUncertifiedBinding(bindingUuid: UUID, formatVersion: Int): Try[PhysicalRecoveryBinding] =
    guarded("createOrRetainLegacyUncertifiedBinding", Option(bindingUuid), InsertPhysicalRecoveryBindingSql) {
      Objects.requireNonNull(bindingUuid, "bindingUuid must not be null")
      require(formatVersion >= 1, "formatVersion must be at least 1")
      val expected = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, formatVersion)
      reservedWrite {
        selectPhysicalRecoveryBinding() match
          case Some(existing) if existing == expected => existing
          case Some(_) => throw new IllegalStateException("Physical recovery binding already conflicts with requested legacy binding")
          case None =>
            insertPhysicalRecoveryBinding(expected)
            expected
      }
    }

  override def certifyPhysicalRecoveryBinding(expectedUuid: UUID, expectedFormatVersion: Int): Try[PhysicalRecoveryBinding] =
    guarded("certifyPhysicalRecoveryBinding", Option(expectedUuid), CertifyPhysicalRecoveryBindingSql) {
      Objects.requireNonNull(expectedUuid, "expectedUuid must not be null")
      require(expectedFormatVersion >= 1, "expectedFormatVersion must be at least 1")
      val expected = PhysicalRecoveryBinding(expectedUuid, PhysicalRecoveryCutoverState.Certified, expectedFormatVersion)
      reservedWrite {
        selectPhysicalRecoveryBinding() match
          case None => throw new IllegalStateException("Cannot certify an absent physical recovery binding")
          case Some(existing) if existing.bindingUuid != expectedUuid || existing.formatVersion != expectedFormatVersion =>
            throw new IllegalStateException("Physical recovery binding conflicts with certification request")
          case Some(existing) if existing.cutoverState == PhysicalRecoveryCutoverState.Certified => existing
          case Some(existing) if existing.cutoverState == PhysicalRecoveryCutoverState.LegacyUncertified =>
            certifyPhysicalRecoveryBinding(expected)
            expected
          case Some(existing) =>
            throw new IllegalStateException(s"Unexpected physical recovery binding cutover state ${existing.cutoverState}")
      }
    }

  override def prepareObservedUnbound(
      observation: ObservedUnboundPod,
      expectedBinding: PhysicalRecoveryBinding,
      attemptUuid: UUID,
      uniquenessProof: ObservedUnboundUniquenessReceipt
  ): Try[UnboundPreparationReceipt] =
    guarded("prepareObservedUnbound", Option(observation).map(_.podUuid), InsertUnboundIntentSql) {
      Objects.requireNonNull(observation, "observation must not be null")
      Objects.requireNonNull(expectedBinding, "expectedBinding must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      Objects.requireNonNull(uniquenessProof, "uniquenessProof must not be null")
      require(uniquenessProof.bindingUuid == expectedBinding.bindingUuid && uniquenessProof.observation == observation,
        "Observed unbound receipt does not match the observation and expected binding")
      val receipt = UnboundPreparationReceipt(
        expectedBinding,
        observation.podUuid,
        attemptUuid,
        UnboundIntentOrigin.ObservedArtifactAdoption,
        observation.pose,
        Some(observation.identity),
        None,
        0L
      )
      reservedWrite {
        requireCertifiedBinding(expectedBinding)
        findReplayByAttempt(attemptUuid) match
          case Some(existing) if existing.receipt == receipt => existing.receipt
          case Some(_) => throw new IllegalStateException(s"Physical recovery attempt UUID already belongs to another intent: $attemptUuid")
          case None =>
            findReplayByObservedIdentity(expectedBinding.bindingUuid, observation.identity) match
              case Some(existing)
                  if existing.receipt.podUuid == observation.podUuid
                    && existing.receipt.targetPose == observation.pose
                    && existing.receipt.origin == UnboundIntentOrigin.ObservedArtifactAdoption => existing.receipt
              case Some(_) => throw new IllegalStateException(s"Observed unbound source identity conflicts with existing history for pod ${observation.podUuid}")
              case None =>
                ensureNoUnboundHistory(receipt)
                insertUnboundPreparation(receipt)
                receipt
      }
    }

  override def commitObservedUnbound(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    commitUnboundPreparation(
      "commitObservedUnbound",
      expectedBinding,
      podUuid,
      attemptUuid,
      expectedPreparedVersion,
      actualPose,
      None,
      globalActiveLimit,
      UnboundIntentOrigin.ObservedArtifactAdoption
    )

  override def prepareUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      targetPose: FloatingPodPose,
      operationUuid: UUID
  ): Try[UnboundPreparationReceipt] =
    guarded("prepareUnboundCreate", Option(podUuid), InsertUnboundIntentSql) {
      Objects.requireNonNull(expectedBinding, "expectedBinding must not be null")
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      Objects.requireNonNull(targetPose, "targetPose must not be null")
      Objects.requireNonNull(operationUuid, "operationUuid must not be null")
      val requested = UnboundPreparationReceipt(
        expectedBinding,
        podUuid,
        attemptUuid,
        UnboundIntentOrigin.DebugItemCreate,
        targetPose,
        None,
        Some(operationUuid),
        0L
      )
      reservedWrite {
        requireCertifiedBinding(expectedBinding)
        findReplayByAttempt(attemptUuid) match
          case Some(existing) if existing.receipt == requested => existing.receipt
          case Some(_) => throw new IllegalStateException(s"Physical recovery attempt UUID already belongs to another intent: $attemptUuid")
          case None =>
            findReplayByCreateOperation(expectedBinding.bindingUuid, operationUuid) match
              case Some(existing)
                  if existing.receipt.binding == expectedBinding
                    && existing.receipt.targetPose == targetPose
                    && existing.receipt.origin == UnboundIntentOrigin.DebugItemCreate => existing.receipt
              case Some(_) => throw new IllegalStateException(s"Controlled-create operation conflicts with existing intent: $operationUuid")
              case None =>
                ensureNoUnboundHistory(requested)
                insertUnboundPreparation(requested)
                requested
      }
    }

  override def commitUnboundCreate(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      operationUuid: UUID,
      globalActiveLimit: Int
  ): Try[UnboundCommitOutcome] =
    commitUnboundPreparation(
      "commitUnboundCreate",
      expectedBinding,
      podUuid,
      attemptUuid,
      expectedPreparedVersion,
      actualPose,
      Some(operationUuid),
      globalActiveLimit,
      UnboundIntentOrigin.DebugItemCreate
    )

  override def findUnboundPreparation(podUuid: UUID): Try[Option[UnboundPreparationReceipt]] =
    guarded("findUnboundPreparation", Option(podUuid), FindUnboundIntentSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      selectUnboundIntent(podUuid).map(_.receipt)
    }

  private def commitUnboundPreparation(
      operation: String,
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      createOperationUuid: Option[UUID],
      globalActiveLimit: Int,
      expectedOrigin: UnboundIntentOrigin
  ): Try[UnboundCommitOutcome] =
    guarded(operation, Option(podUuid), CommitUnboundIntentSql) {
      Objects.requireNonNull(expectedBinding, "expectedBinding must not be null")
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      Objects.requireNonNull(actualPose, "actualPose must not be null")
      createOperationUuid.foreach(value => Objects.requireNonNull(value, "createOperationUuid must not be null"))
      require(expectedPreparedVersion >= 0L, "expectedPreparedVersion must not be negative")
      require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be between 1 and 256")
      try
        reservedWrite {
          requireCertifiedBinding(expectedBinding)
          val intent = selectUnboundIntent(podUuid).getOrElse(throw new IllegalStateException(s"No unbound recovery intent exists for pod $podUuid"))
          val receipt = intent.receipt
          if receipt.binding != expectedBinding
            || receipt.podUuid != podUuid
            || receipt.attemptUuid != attemptUuid
            || receipt.origin != expectedOrigin
            || receipt.createOperationUuid != createOperationUuid then
            throw new IllegalStateException(s"Unbound commit identity does not match the persisted intent for pod $podUuid")

          if intent.phase == "COMMITTED" then
            if expectedPreparedVersion != receipt.preparedVersion then
              throw new OptimisticLockException(podUuid, expectedPreparedVersion)
            if actualPose != receipt.targetPose then UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch)
            else
              requireCommittedUnboundState(intent)
              UnboundCommitOutcome.AlreadyCommitted(intent.committedState.getOrElse(throw new IllegalStateException(s"Committed intent has no committed snapshot for pod $podUuid")))
          else if intent.phase != "PREPARED" then
            throw new IllegalStateException(s"Unbound intent has unsupported phase ${intent.phase} for pod $podUuid")
          else if expectedPreparedVersion != receipt.preparedVersion then
            throw new OptimisticLockException(podUuid, expectedPreparedVersion)
          else if actualPose != receipt.targetPose then
            UnboundCommitOutcome.Rejected(UnboundCommitRejection.ActualPoseMismatch)
          else
            val stored = selectStoredRow(podUuid).getOrElse(throw new IllegalStateException(s"Prepared unbound intent has no domain row for pod $podUuid"))
            if stored.ownerUuid.nonEmpty || stored.operatingMode != PodOperatingMode.WorldIdle || stored.isActive
              || stored.stateVersion != expectedPreparedVersion || !poseMatches(stored, actualPose) then
              throw new IllegalStateException(s"Prepared unbound domain row conflicts with its exact intent for pod $podUuid")
            requireExactUnboundPreparedLocator(receipt)
            val active = activeCount()
            if active >= globalActiveLimit then
              UnboundCommitOutcome.Rejected(UnboundCommitRejection.CapacityReached(active, globalActiveLimit))
            else
              val committedAt = System.currentTimeMillis()
              updateUnboundDomainActive(receipt, expectedPreparedVersion, committedAt)
              updateUnboundLocatorActive(receipt, expectedPreparedVersion + 1L)
              updateUnboundIntentCommitted(receipt, expectedPreparedVersion + 1L, committedAt)
              val committed = selectStoredRow(podUuid).getOrElse(throw new IllegalStateException(s"Committed unbound domain row disappeared for pod $podUuid")).toPublicState
              val committedIntent = selectUnboundIntent(podUuid).getOrElse(throw new IllegalStateException(s"Committed unbound intent disappeared for pod $podUuid"))
              if committedIntent.phase != "COMMITTED" || committedIntent.committedState != Some(committed) then
                throw new IllegalStateException(s"Unbound intent commit readback does not match the domain row for pod $podUuid")
              requireCommittedUnboundState(committedIntent)
              UnboundCommitOutcome.Committed(committed)
        }
      catch
        case writeFailure: Throwable =>
          resolveUnboundCommitAfterFailure(
            expectedBinding,
            podUuid,
            attemptUuid,
            expectedPreparedVersion,
            actualPose,
            createOperationUuid,
            expectedOrigin,
            writeFailure
          ).getOrElse(throw writeFailure)
    }

  private def requireCertifiedBinding(expectedBinding: PhysicalRecoveryBinding): Unit =
    selectPhysicalRecoveryBinding() match
      case Some(current) if current == expectedBinding && current.cutoverState == PhysicalRecoveryCutoverState.Certified => ()
      case _ => throw new IllegalStateException("Unbound physical recovery requires the exact certified binding")

  private def ensureNoUnboundHistory(receipt: UnboundPreparationReceipt): Unit =
    if selectStoredRow(receipt.podUuid).nonEmpty then
      throw new IllegalStateException(s"Unbound preparation cannot replace an existing domain row for pod ${receipt.podUuid}")
    if selectPhysicalRecoveryLocator(receipt.podUuid).nonEmpty then
      throw new IllegalStateException(s"Unbound preparation cannot replace physical recovery history for pod ${receipt.podUuid}")

  private def insertUnboundPreparation(receipt: UnboundPreparationReceipt): Unit =
    val target = receipt.targetPose
    val chunks = recoveryChunks(target.dimensionId, target.posX, target.posZ)
    if chunks.size != 25 then throw new IllegalStateException(s"Unbound preparation did not derive an exact 5x5 chunk square for pod ${receipt.podUuid}")
    val state = FloatingPodState(
      receipt.podUuid,
      None,
      receipt.preparedVersion,
      target.worldName,
      target.posX,
      target.posY,
      target.posZ,
      target.yaw,
      target.pitch,
      false,
      System.currentTimeMillis(),
      Some(target.dimensionId),
      PodOperatingMode.WorldIdle
    )
    val domainInsert = connection.prepareStatement(InsertSql)
    JdbcResource.withResource(domainInsert):
      bindInsert(domainInsert, state)
      if domainInsert.executeUpdate() != 1 then throw new IllegalStateException(s"Unbound domain row insert did not affect pod ${receipt.podUuid}")

    val intentInsert = connection.prepareStatement(InsertUnboundIntentSql)
    JdbcResource.withResource(intentInsert):
      intentInsert.setBytes(1, uuidToBytes(receipt.podUuid))
      intentInsert.setBytes(2, uuidToBytes(receipt.attemptUuid))
      intentInsert.setBytes(3, uuidToBytes(receipt.binding.bindingUuid))
      intentInsert.setString(4, originDatabaseValue(receipt.origin))
      intentInsert.setString(5, "PREPARED")
      receipt.observedIdentity match
        case Some(identity) =>
          intentInsert.setString(6, sourceKindDatabaseValue(identity.sourceKind))
          intentInsert.setString(7, identity.sourceIdentity)
          intentInsert.setBytes(8, uuidToBytes(identity.catalogGeneration))
          intentInsert.setNull(9, Types.BLOB)
        case None =>
          intentInsert.setNull(6, Types.VARCHAR)
          intentInsert.setNull(7, Types.VARCHAR)
          intentInsert.setNull(8, Types.BLOB)
          intentInsert.setBytes(9, uuidToBytes(receipt.createOperationUuid.getOrElse(throw new IllegalStateException("Controlled-create intent has no operation UUID"))))
      intentInsert.setLong(10, receipt.preparedVersion)
      intentInsert.setString(11, target.worldName)
      intentInsert.setDouble(12, target.posX)
      intentInsert.setDouble(13, target.posY)
      intentInsert.setDouble(14, target.posZ)
      intentInsert.setDouble(15, target.yaw.toDouble)
      intentInsert.setDouble(16, target.pitch.toDouble)
      intentInsert.setInt(17, target.dimensionId)
      intentInsert.setInt(18, chunkCoordinate(target.posX))
      intentInsert.setInt(19, chunkCoordinate(target.posZ))
      if intentInsert.executeUpdate() != 1 then throw new IllegalStateException(s"Unbound intent insert did not affect pod ${receipt.podUuid}")

    val locatorState = locatorStateFor(receipt.origin)
    val locatorInsert = connection.prepareStatement(InsertPhysicalRecoveryLocatorSql)
    JdbcResource.withResource(locatorInsert):
      locatorInsert.setBytes(1, uuidToBytes(receipt.podUuid))
      locatorInsert.setBytes(2, uuidToBytes(receipt.attemptUuid))
      locatorInsert.setString(3, locatorState.databaseValue)
      locatorInsert.setString(4, locatorReasonFor(receipt.origin))
      locatorInsert.setLong(5, receipt.preparedVersion)
      locatorInsert.setInt(6, target.dimensionId)
      if locatorInsert.executeUpdate() != 1 then throw new IllegalStateException(s"Unbound locator insert did not affect pod ${receipt.podUuid}")
    replacePhysicalRecoveryChunks(receipt.podUuid, chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ)))

  private def requireExactUnboundPreparedLocator(receipt: UnboundPreparationReceipt): PhysicalRecoveryLocator =
    val locator = selectPhysicalRecoveryLocator(receipt.podUuid) match
      case Some(value)
          if value.attemptUuid == receipt.attemptUuid
            && value.state == locatorStateFor(receipt.origin)
            && value.reasonCode == locatorReasonFor(receipt.origin)
            && value.domainVersion.contains(receipt.preparedVersion)
            && value.targetDimensionId.contains(receipt.targetPose.dimensionId) => value
      case _ => throw new IllegalStateException(s"Unbound physical recovery locator is not the exact prepared intent for pod ${receipt.podUuid}")
    if locator.chunks.toSet != recoveryChunks(receipt.targetPose.dimensionId, receipt.targetPose.posX, receipt.targetPose.posZ)
      || locator.chunks.size != 25 then
      throw new IllegalStateException(s"Unbound physical recovery locator is not the exact target 5x5 square for pod ${receipt.podUuid}")
    locator

  private def updateUnboundDomainActive(receipt: UnboundPreparationReceipt, expectedVersion: Long, committedAt: Long): Unit =
    val statement = connection.prepareStatement(CommitUnboundDomainSql)
    JdbcResource.withResource(statement):
      statement.setString(1, receipt.targetPose.worldName)
      statement.setDouble(2, receipt.targetPose.posX)
      statement.setDouble(3, receipt.targetPose.posY)
      statement.setDouble(4, receipt.targetPose.posZ)
      statement.setDouble(5, receipt.targetPose.yaw.toDouble)
      statement.setDouble(6, receipt.targetPose.pitch.toDouble)
      statement.setLong(7, committedAt)
      statement.setInt(8, receipt.targetPose.dimensionId)
      statement.setBytes(9, uuidToBytes(receipt.podUuid))
      statement.setLong(10, expectedVersion)
      statement.setInt(11, receipt.targetPose.dimensionId)
      if statement.executeUpdate() != 1 then throw new OptimisticLockException(receipt.podUuid, expectedVersion)

  private def updateUnboundLocatorActive(receipt: UnboundPreparationReceipt, committedVersion: Long): Unit =
    val statement = connection.prepareStatement(CommitUnboundLocatorSql)
    JdbcResource.withResource(statement):
      statement.setString(1, PhysicalRecoveryLocatorState.RowActive.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.RowActive)
      statement.setLong(3, committedVersion)
      statement.setBytes(4, uuidToBytes(receipt.podUuid))
      statement.setBytes(5, uuidToBytes(receipt.attemptUuid))
      statement.setLong(6, receipt.preparedVersion)
      statement.setInt(7, receipt.targetPose.dimensionId)
      statement.setString(8, locatorStateFor(receipt.origin).databaseValue)
      statement.setString(9, locatorReasonFor(receipt.origin))
      if statement.executeUpdate() != 1 then throw new IllegalStateException(s"Unbound locator changed during activation for pod ${receipt.podUuid}")

  private def updateUnboundIntentCommitted(receipt: UnboundPreparationReceipt, committedVersion: Long, committedAt: Long): Unit =
    val target = receipt.targetPose
    val statement = connection.prepareStatement(CommitUnboundIntentSql)
    JdbcResource.withResource(statement):
      statement.setString(1, "COMMITTED")
      statement.setLong(2, committedVersion)
      statement.setLong(3, committedAt)
      statement.setString(4, target.worldName)
      statement.setDouble(5, target.posX)
      statement.setDouble(6, target.posY)
      statement.setDouble(7, target.posZ)
      statement.setDouble(8, target.yaw.toDouble)
      statement.setDouble(9, target.pitch.toDouble)
      statement.setInt(10, target.dimensionId)
      statement.setBytes(11, uuidToBytes(receipt.podUuid))
      statement.setBytes(12, uuidToBytes(receipt.attemptUuid))
      if statement.executeUpdate() != 1 then throw new IllegalStateException(s"Unbound intent changed during activation for pod ${receipt.podUuid}")

  private def requireCommittedUnboundState(intent: StoredUnboundIntent): Unit =
    val receipt = intent.receipt
    val committed = intent.committedState.getOrElse(throw new IllegalStateException(s"Committed intent has no durable snapshot for pod ${receipt.podUuid}"))
    val current = selectStoredRow(receipt.podUuid).getOrElse(throw new IllegalStateException(s"Committed unbound intent has no domain row for pod ${receipt.podUuid}"))
    if committed.ownerUuid.nonEmpty || !committed.isActive || committed.operatingMode != PodOperatingMode.WorldIdle
      || committed.stateVersion != receipt.preparedVersion + 1L || !poseMatches(committed, receipt.targetPose)
      || current.ownerUuid.nonEmpty || !current.isActive || current.operatingMode != PodOperatingMode.WorldIdle
      || current.stateVersion < committed.stateVersion || current.dimensionId != Some(receipt.targetPose.dimensionId) then
      throw new IllegalStateException(s"Committed unbound state no longer matches its durable intent for pod ${receipt.podUuid}")
    val locator = selectPhysicalRecoveryLocator(receipt.podUuid).getOrElse(throw new IllegalStateException(s"Committed unbound intent has no locator for pod ${receipt.podUuid}"))
    if locator.attemptUuid != receipt.attemptUuid || locator.state != PhysicalRecoveryLocatorState.RowActive
      || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive || locator.domainVersion != Some(current.stateVersion)
      || locator.targetDimensionId != Some(receipt.targetPose.dimensionId)
      || locator.chunks.toSet != recoveryChunks(receipt.targetPose.dimensionId, receipt.targetPose.posX, receipt.targetPose.posZ)
      || locator.chunks.size != 25 then
      throw new IllegalStateException(s"Committed unbound locator does not match its durable intent for pod ${receipt.podUuid}")

  private def resolveUnboundCommitAfterFailure(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      expectedPreparedVersion: Long,
      actualPose: FloatingPodPose,
      createOperationUuid: Option[UUID],
      expectedOrigin: UnboundIntentOrigin,
      primaryFailure: Throwable
  ): Option[UnboundCommitOutcome] =
    val normalizedDbPath = dbPath.toLowerCase
    if dbPath == "injected SQLite connection" || normalizedDbPath.contains(":memory:") || normalizedDbPath.contains("mode=memory") then
      return None

    var probe: SQLiteFloatingPodRepository = null
    var readbackFailure: Throwable = null
    var committed: Option[FloatingPodState] = None
    try
      probe = new SQLiteFloatingPodRepository(dbPath)
      val currentBinding = probe.findPhysicalRecoveryBinding().get
      val observedIntent = probe.synchronized { probe.selectUnboundIntent(podUuid) }
      val currentState = probe.findStoredByUuid(podUuid).get
      val currentLocator = probe.findPhysicalRecoveryLocator(podUuid).get
      committed = for
        binding <- currentBinding if binding == expectedBinding && expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified
        intent <- observedIntent if intent.phase == "COMMITTED"
          && intent.receipt.binding == expectedBinding
          && intent.receipt.podUuid == podUuid
          && intent.receipt.attemptUuid == attemptUuid
          && intent.receipt.origin == expectedOrigin
          && intent.receipt.createOperationUuid == createOperationUuid
          && intent.receipt.preparedVersion == expectedPreparedVersion
          && intent.receipt.targetPose == actualPose
        state <- currentState if state.ownerUuid.isEmpty && state.isActive && state.operatingMode == PodOperatingMode.WorldIdle
          && state.stateVersion == expectedPreparedVersion + 1L && poseMatches(state, actualPose)
        locator <- currentLocator if locator.attemptUuid == attemptUuid
          && locator.state == PhysicalRecoveryLocatorState.RowActive
          && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive
          && locator.domainVersion.contains(state.stateVersion)
          && locator.targetDimensionId.contains(actualPose.dimensionId)
          && locator.chunks.toSet == recoveryChunks(actualPose.dimensionId, actualPose.posX, actualPose.posZ)
          && locator.chunks.size == 25
      yield intent.committedState.getOrElse(throw new IllegalStateException(s"Committed unbound intent has no exact readback snapshot for pod $podUuid"))
    catch
      case exception: Throwable => readbackFailure = exception
    finally
      if probe != null then
        try probe.close()
        catch
          case closeFailure: Throwable =>
            if readbackFailure == null then readbackFailure = closeFailure
            else readbackFailure.addSuppressed(closeFailure)

    if readbackFailure != null then
      if readbackFailure ne primaryFailure then primaryFailure.addSuppressed(readbackFailure)
      None
    else committed.map { state =>
      if closed then
        try
          val replacement = openStandardResources()
          resources = replacement
          connection = replacement.connection
          transactionControl = replacement.transactionControl
          closed = false
        catch
          case reopenFailure: Throwable =>
            primaryFailure.addSuppressed(reopenFailure)
            throw primaryFailure
      logger.warn(s"SQLite unbound commit response was uncertain; exact intent readback confirmed pod $podUuid", primaryFailure)
      UnboundCommitOutcome.Committed(state)
    }

  private def selectUnboundIntent(podUuid: UUID): Option[StoredUnboundIntent] =
    val statement = connection.prepareStatement(FindUnboundIntentSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(podUuid))
      val resultSet = statement.executeQuery()
      val raw = JdbcResource.withResource(resultSet)(if resultSet.next() then Some(readUnboundIntentColumns(resultSet)) else None)
      raw.map { columns =>
        val binding = selectPhysicalRecoveryBinding().filter(_.bindingUuid == columns.bindingUuid)
          .getOrElse(throw new IllegalStateException(s"Unbound intent refers to a missing or changed binding for pod $podUuid"))
        materializeUnboundIntent(columns, binding)
      }

  private def selectUnboundIntentByQuery(sql: String, bind: PreparedStatement => Unit): Option[StoredUnboundIntent] =
    val statement = connection.prepareStatement(sql)
    JdbcResource.withResource(statement):
      bind(statement)
      val resultSet = statement.executeQuery()
      val podUuid = JdbcResource.withResource(resultSet)(if resultSet.next() then Some(bytesToUuid(resultSet.getBytes(1), "pod_uuid")) else None)
      podUuid.flatMap(selectUnboundIntent)

  private def findReplayByAttempt(attemptUuid: UUID): Option[StoredUnboundIntent] =
    selectUnboundIntentByQuery(FindUnboundIntentByAttemptSql, statement => statement.setBytes(1, uuidToBytes(attemptUuid)))

  private def findReplayByObservedIdentity(bindingUuid: UUID, identity: ObservedUnboundIdentity): Option[StoredUnboundIntent] =
    selectUnboundIntentByQuery(FindUnboundIntentByObservedSourceSql, statement => {
      statement.setBytes(1, uuidToBytes(bindingUuid))
      statement.setString(2, sourceKindDatabaseValue(identity.sourceKind))
      statement.setBytes(3, uuidToBytes(identity.catalogGeneration))
      statement.setString(4, identity.sourceIdentity)
    })

  private def findReplayByCreateOperation(bindingUuid: UUID, operationUuid: UUID): Option[StoredUnboundIntent] =
    selectUnboundIntentByQuery(FindUnboundIntentByCreateOperationSql, statement => {
      statement.setBytes(1, uuidToBytes(bindingUuid))
      statement.setBytes(2, uuidToBytes(operationUuid))
    })

  private def locatorStateFor(origin: UnboundIntentOrigin): PhysicalRecoveryLocatorState = origin match
    case UnboundIntentOrigin.ObservedArtifactAdoption => PhysicalRecoveryLocatorState.UnboundAdoptionPrepared
    case UnboundIntentOrigin.DebugItemCreate => PhysicalRecoveryLocatorState.DebugItemCreatePrepared

  private def locatorReasonFor(origin: UnboundIntentOrigin): String = origin match
    case UnboundIntentOrigin.ObservedArtifactAdoption => PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared
    case UnboundIntentOrigin.DebugItemCreate => PhysicalRecoveryLocatorReason.DebugItemCreatePrepared

  private def originDatabaseValue(origin: UnboundIntentOrigin): String = origin match
    case UnboundIntentOrigin.ObservedArtifactAdoption => "UNBOUND_ADOPTION"
    case UnboundIntentOrigin.DebugItemCreate => "DEBUG_ITEM_CREATE"

  private def sourceKindDatabaseValue(kind: ObservedUnboundSourceKind): String = kind match
    case ObservedUnboundSourceKind.FreshArtifact => "FRESH"
    case ObservedUnboundSourceKind.LegacyUnregistered => "LEGACY_UNREGISTERED"

  override def prepareCreateLocator(
      podUuid: UUID,
      expectedDomainVersion: Long,
      attemptUuid: UUID,
      targetDimensionId: Int,
      chunks: Set[PhysicalRecoveryChunk]
  ): Try[PhysicalRecoveryLocator] =
    guarded("prepareCreateLocator", Option(podUuid), InsertPhysicalRecoveryLocatorSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      Objects.requireNonNull(chunks, "chunks must not be null")
      require(expectedDomainVersion >= 0L, "expectedDomainVersion must not be negative")
      require(chunks.size == 25, "CREATE_PREPARED locator requires exactly 25 unique chunks")
      require(chunks.forall(_.dimensionId == targetDimensionId), "CREATE_PREPARED locator chunks must all use the target dimension")
      val chunkXs = chunks.map(_.chunkX)
      val chunkZs = chunks.map(_.chunkZ)
      def areConsecutive(coordinates: Set[Int]): Boolean =
        coordinates.toList.sorted match
          case first :: second :: third :: fourth :: fifth :: Nil =>
            second == first + 1 && third == second + 1 && fourth == third + 1 && fifth == fourth + 1
          case _ => false
      val expectedChunks = (for chunkX <- chunkXs; chunkZ <- chunkZs yield PhysicalRecoveryChunk(targetDimensionId, chunkX, chunkZ)).toSet
      require(areConsecutive(chunkXs) && areConsecutive(chunkZs) && chunks == expectedChunks, "CREATE_PREPARED locator chunks must form an exact 5x5 consecutive square")
      reservedWrite {
        selectStoredRow(podUuid) match
          case None => throw new IllegalStateException(s"Cannot prepare physical recovery locator for unknown pod $podUuid")
          case Some(stored) if stored.isActive => throw new IllegalStateException(s"Cannot prepare physical recovery locator for active pod $podUuid")
          case Some(stored) if stored.stateVersion != expectedDomainVersion => throw new OptimisticLockException(podUuid, expectedDomainVersion)
          case Some(_) =>
            val expectedChunks = chunks.toList.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))
            selectPhysicalRecoveryLocator(podUuid) match
              case Some(existing) if existing.attemptUuid == attemptUuid && existing.state == PhysicalRecoveryLocatorState.CreatePrepared && existing.reasonCode == PhysicalRecoveryLocatorReason.CreatePrepared && existing.domainVersion.contains(expectedDomainVersion) && existing.targetDimensionId.contains(targetDimensionId) && existing.chunks == expectedChunks => existing
              case Some(_) => throw new IllegalStateException(s"Physical recovery locator already conflicts for pod $podUuid")
              case None =>
                insertPhysicalRecoveryLocator(podUuid, attemptUuid, expectedDomainVersion, targetDimensionId)
                replacePhysicalRecoveryChunks(podUuid, expectedChunks)
                PhysicalRecoveryLocator(podUuid, attemptUuid, PhysicalRecoveryLocatorState.CreatePrepared, PhysicalRecoveryLocatorReason.CreatePrepared, Some(expectedDomainVersion), Some(targetDimensionId), expectedChunks)
      }
    }

  override def activatePreparedIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int,
      attemptUuid: UUID
  ): Try[AdmissionOutcome] =
    guarded("activatePreparedIfAdmissible", Option(target).flatMap(state => Option(state.uuid)), ActivateSql) {
      Objects.requireNonNull(target, "target must not be null")
      Objects.requireNonNull(authenticatedOwner, "authenticatedOwner must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be between 1 and 256")
      require(!target.isActive, "target must describe an inactive state before activation")
      reservedWrite {
        admitInactive(selectStoredRow(target.uuid), authenticatedOwner, expectedStateVersion, globalActiveLimit) match
          case Left(rejected) => rejected
          case Right(stored) =>
            require(target.operatingMode == stored.operatingMode, s"Activation cannot change operating mode for pod ${target.uuid}")
            requireExactPreparedLocator(target.uuid, attemptUuid, expectedStateVersion, dimensionId)
            val persisted = target.copy(
              uuid = stored.uuid,
              ownerUuid = stored.ownerUuid,
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = true,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = Some(dimensionId),
              operatingMode = stored.operatingMode
            )
            val statement = connection.prepareStatement(ActivateSql)
            JdbcResource.withResource(statement):
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Activation changed unexpectedly for UUID ${target.uuid}")
            activatePreparedLocator(target.uuid, attemptUuid, expectedStateVersion, dimensionId, persisted.stateVersion)
            AdmissionOutcome.Accepted(persisted)
      }
    }

  override def deactivateTrackedIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long,
      attemptUuid: UUID
  ): Try[DeactivationOutcome] =
    guarded("deactivateTrackedIfExpectedActive", Option(podUuid), DeactivateSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(expectedOwner, "expectedOwner must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      reservedWrite {
        validateExpectedActiveDeactivation(selectStoredRow(podUuid), expectedOwner, expectedStateVersion) match
          case Left(rejected) => rejected
          case Right(stored) =>
            val dimensionId = stored.dimensionId.getOrElse(throw new IllegalStateException(s"Active physical recovery row has no dimension for pod $podUuid"))
            requireExactActiveLocator(podUuid, attemptUuid, expectedStateVersion, dimensionId)
            val persisted = stateForGenericRead(stored).copy(
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = false,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = stored.dimensionId
            )
            val statement = connection.prepareStatement(DeactivateSql)
            JdbcResource.withResource(statement):
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Tracked deactivation changed unexpectedly for UUID $podUuid")
            deactivateActiveLocator(podUuid, attemptUuid, expectedStateVersion, dimensionId, persisted.stateVersion)
            DeactivationOutcome.Accepted(persisted)
      }
    }

  override def updateTrackedActiveDrift(state: FloatingPodState, expectedAttemptUuid: UUID): Try[FloatingPodState] =
    guarded("updateTrackedActiveDrift", Option(state).flatMap(value => Option(value.uuid)), UpdateTrackedActiveDriftSql) {
      Objects.requireNonNull(state, "state must not be null")
      Objects.requireNonNull(expectedAttemptUuid, "expectedAttemptUuid must not be null")
      reservedWrite {
        val binding = selectPhysicalRecoveryBinding().getOrElse(
          throw new IllegalStateException("Certified physical recovery binding is required for tracked active drift")
        )
        if binding.cutoverState != PhysicalRecoveryCutoverState.Certified then
          throw new IllegalStateException("Certified physical recovery binding is required for tracked active drift")

        val stored = selectStoredRow(state.uuid).getOrElse(throw new OptimisticLockException(state.uuid, state.stateVersion))
        if stored.stateVersion != state.stateVersion then
          throw new OptimisticLockException(state.uuid, state.stateVersion)
        if !stored.isActive || !state.isActive then
          throw new IllegalStateException(s"Tracked active drift requires an active domain row for pod ${state.uuid}")
        if stored.ownerUuid != state.ownerUuid then
          throw new IllegalStateException(s"Tracked active drift cannot change owner for pod ${state.uuid}")
        if stored.operatingMode != state.operatingMode then
          throw new IllegalStateException(s"Tracked active drift cannot change operating mode for pod ${state.uuid}")
        if stored.worldName != state.worldName || stored.lastUpdated != state.lastUpdated then
          throw new IllegalStateException(s"Tracked active drift cannot change critical world metadata for pod ${state.uuid}")
        if stored.dimensionId != state.dimensionId then
          throw new IllegalStateException(s"Tracked active drift cannot change dimension for pod ${state.uuid}")

        val dimensionId = stored.dimensionId.getOrElse(throw new IllegalStateException(s"Active physical recovery row has no dimension for pod ${state.uuid}"))
        if chunkCoordinate(stored.posX) != chunkCoordinate(state.posX) || chunkCoordinate(stored.posZ) != chunkCoordinate(state.posZ) then
          throw new IllegalStateException(s"Tracked active drift crossed a chunk boundary for pod ${state.uuid}")

        val locator = selectPhysicalRecoveryLocator(state.uuid).getOrElse(
          throw new IllegalStateException(s"No active physical recovery locator exists for pod ${state.uuid}")
        )
        if locator.attemptUuid != expectedAttemptUuid
          || locator.state != PhysicalRecoveryLocatorState.RowActive
          || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive
          || !locator.domainVersion.contains(stored.stateVersion)
          || !locator.targetDimensionId.contains(dimensionId)
          || locator.chunks.toSet != recoveryChunks(dimensionId, stored.posX, stored.posZ)
          || locator.chunks.size != 25 then
          throw new IllegalStateException(s"No exact current active physical recovery locator exists for pod ${state.uuid}")

        val nextVersion = Math.addExact(stored.stateVersion, 1L)
        val updatedAt = System.currentTimeMillis()
        val statement = connection.prepareStatement(UpdateTrackedActiveDriftSql)
        JdbcResource.withResource(statement):
          statement.setString(1, state.worldName)
          statement.setDouble(2, state.posX)
          statement.setDouble(3, state.posY)
          statement.setDouble(4, state.posZ)
          statement.setDouble(5, state.yaw.toDouble)
          statement.setDouble(6, state.pitch.toDouble)
          statement.setLong(7, updatedAt)
          statement.setBytes(8, uuidToBytes(state.uuid))
          statement.setLong(9, stored.stateVersion)
          if statement.executeUpdate() != 1 then
            throw new OptimisticLockException(state.uuid, stored.stateVersion)

        val locatorUpdate = connection.prepareStatement(UpdateTrackedActiveLocatorSql)
        JdbcResource.withResource(locatorUpdate):
          locatorUpdate.setLong(1, nextVersion)
          locatorUpdate.setBytes(2, uuidToBytes(state.uuid))
          locatorUpdate.setBytes(3, uuidToBytes(expectedAttemptUuid))
          locatorUpdate.setLong(4, stored.stateVersion)
          locatorUpdate.setInt(5, dimensionId)
          if locatorUpdate.executeUpdate() != 1 then
            throw new IllegalStateException(s"Active physical recovery locator changed unexpectedly during drift update for pod ${state.uuid}")

        state.copy(stateVersion = nextVersion, lastUpdated = updatedAt, operatingMode = stored.operatingMode)
      }
    }

  override def setOperatingModeTrackedIfOwned(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose
  ): Try[OperatingModeChangeOutcome] =
    guarded("setOperatingModeTrackedIfOwned", Option(podUuid), SetOperatingModeSql) {
      Objects.requireNonNull(expectedBinding, "expectedBinding must not be null")
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(authenticatedOwner, "authenticatedOwner must not be null")
      Objects.requireNonNull(attemptUuid, "attemptUuid must not be null")
      Objects.requireNonNull(targetMode, "targetMode must not be null")
      Objects.requireNonNull(actualPose, "actualPose must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      try reservedWrite {
        selectPhysicalRecoveryBinding() match
          case Some(binding) if binding == expectedBinding && binding.cutoverState == PhysicalRecoveryCutoverState.Certified =>
            selectStoredRow(podUuid) match
              case None => OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.UnknownPod)
              case Some(stored) if stored.ownerUuid != Some(authenticatedOwner) =>
                OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.NotOwner)
              case Some(stored) if !stored.isActive =>
                OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.Inactive)
              case Some(stored) =>
                val dimensionId = stored.dimensionId.getOrElse(throw new IllegalStateException(s"Active physical recovery row has no dimension for pod $podUuid"))
                selectPhysicalRecoveryLocator(podUuid) match
                  case None => OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
                  case Some(locator) =>
                    if locator.attemptUuid != attemptUuid
                      || locator.state != PhysicalRecoveryLocatorState.RowActive
                      || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive
                      || !locator.domainVersion.contains(stored.stateVersion)
                      || !locator.targetDimensionId.contains(dimensionId)
                      || locator.chunks.toSet != recoveryChunks(dimensionId, stored.posX, stored.posZ)
                      || locator.chunks.size != 25 then
                      OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
                    else if stored.stateVersion != expectedStateVersion
                      && !(expectedStateVersion < Long.MaxValue
                        && stored.stateVersion == expectedStateVersion + 1L
                        && stored.operatingMode == targetMode
                        && poseMatches(stored, actualPose)) then
                      OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.VersionMismatch(stored.stateVersion))
                    else if actualPose.worldName != stored.worldName
                      || actualPose.dimensionId != dimensionId
                      || chunkCoordinate(stored.posX) != chunkCoordinate(actualPose.posX)
                      || chunkCoordinate(stored.posZ) != chunkCoordinate(actualPose.posZ) then
                      OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.CrossChunkMovement)
                    else if stored.operatingMode == targetMode && poseMatches(stored, actualPose) then
                      OperatingModeChangeOutcome.AlreadySet(stored.toPublicState)
                    else
                      val nextVersion = Math.addExact(stored.stateVersion, 1L)
                      val updatedAt = System.currentTimeMillis()
                      val stateUpdate = connection.prepareStatement(SetOperatingModeSql)
                      JdbcResource.withResource(stateUpdate):
                        stateUpdate.setString(1, targetMode.databaseValue)
                        stateUpdate.setString(2, actualPose.worldName)
                        stateUpdate.setDouble(3, actualPose.posX)
                        stateUpdate.setDouble(4, actualPose.posY)
                        stateUpdate.setDouble(5, actualPose.posZ)
                        stateUpdate.setDouble(6, actualPose.yaw.toDouble)
                        stateUpdate.setDouble(7, actualPose.pitch.toDouble)
                        stateUpdate.setLong(8, updatedAt)
                        stateUpdate.setBytes(9, uuidToBytes(podUuid))
                        stateUpdate.setBytes(10, uuidToBytes(authenticatedOwner))
                        stateUpdate.setLong(11, expectedStateVersion)
                        stateUpdate.setString(12, stored.operatingMode.databaseValue)
                        stateUpdate.setInt(13, dimensionId)
                        if stateUpdate.executeUpdate() != 1 then
                          throw new OptimisticLockException(podUuid, expectedStateVersion)

                      val locatorUpdate = connection.prepareStatement(UpdateOperatingModeLocatorSql)
                      JdbcResource.withResource(locatorUpdate):
                        locatorUpdate.setLong(1, nextVersion)
                        locatorUpdate.setBytes(2, uuidToBytes(podUuid))
                        locatorUpdate.setBytes(3, uuidToBytes(attemptUuid))
                        locatorUpdate.setLong(4, expectedStateVersion)
                        locatorUpdate.setInt(5, dimensionId)
                        if locatorUpdate.executeUpdate() != 1 then
                          throw new IllegalStateException(s"Active physical recovery locator changed unexpectedly during mode update for pod $podUuid")

                      OperatingModeChangeOutcome.Changed(
                        stored.toPublicState.copy(
                          stateVersion = nextVersion,
                          worldName = actualPose.worldName,
                          posX = actualPose.posX,
                          posY = actualPose.posY,
                          posZ = actualPose.posZ,
                          yaw = actualPose.yaw,
                          pitch = actualPose.pitch,
                          lastUpdated = updatedAt,
                          operatingMode = targetMode
                        )
                      )
          case _ => OperatingModeChangeOutcome.Rejected(OperatingModeChangeRejection.BindingMismatch)
      }
      catch
        case commitOrWriteFailure: Throwable =>
          resolveOperatingModeWriteAfterFailure(
            expectedBinding,
            podUuid,
            authenticatedOwner,
            expectedStateVersion,
            attemptUuid,
            targetMode,
            actualPose,
            commitOrWriteFailure
          ).getOrElse(throw commitOrWriteFailure)
    }

  /**
   * 仅读取玩家明确持久化的权益；默认值由调用方的内存预览策略决定。
   */
  override def findByPlayerUuid(playerUuid: UUID): Try[Option[PlayerEntitlement]] =
    guarded("findByPlayerUuid", Option(playerUuid), FindEntitlementSql) {
      Objects.requireNonNull(playerUuid, "playerUuid must not be null")
      selectEntitlement(playerUuid)
    }

  /** 按服务器回调补齐遗留活动空维度；同维度重放不写入，冲突维度显式拒绝。 */
  override def backfillMissingActiveDimension(
      uuid: UUID,
      expectedStateVersion: Long,
      authoritativeDimensionId: Int
  ): Try[RecoveryDimensionBackfillOutcome] =
    guarded("backfillMissingActiveDimension", Option(uuid), BackfillDimensionSql) {
      Objects.requireNonNull(uuid, "uuid must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      reservedWrite {
        selectStoredRow(uuid) match
          case None => RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.UnknownPod)
          case Some(stored) if !stored.isActive =>
            RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.InactivePod)
          case Some(stored) if stored.stateVersion != expectedStateVersion =>
            RecoveryDimensionBackfillOutcome.Rejected(RecoveryDimensionBackfillRejection.VersionMismatch(stored.stateVersion))
          case Some(stored) if stored.dimensionId.contains(authoritativeDimensionId) =>
            RecoveryDimensionBackfillOutcome.Ready(stored.toPublicState)
          case Some(stored) if stored.dimensionId.nonEmpty =>
            RecoveryDimensionBackfillOutcome.Rejected(
              RecoveryDimensionBackfillRejection.DimensionConflict(stored.dimensionId.get)
            )
          case Some(stored) =>
            val statement = connection.prepareStatement(BackfillDimensionSql)
            JdbcResource.withResource(statement):
              statement.setInt(1, authoritativeDimensionId)
              statement.setBytes(2, uuidToBytes(uuid))
              statement.setLong(3, expectedStateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Recovery dimension changed unexpectedly for UUID $uuid")
            RecoveryDimensionBackfillOutcome.Ready(stored.copy(dimensionId = Some(authoritativeDimensionId)).toPublicState)
      }
    }

  /**
   * 用写保留事务原子完成 Release 的数据库准入，避免两个独立 repository 连接同时越过最后一个槽位。
   */
  override def activateIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): Try[AdmissionOutcome] =
    guarded("activateIfAdmissible", Option(target).flatMap(state => Option(state.uuid)), ActivateSql) {
      Objects.requireNonNull(target, "target must not be null")
      Objects.requireNonNull(authenticatedOwner, "authenticatedOwner must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      require(globalActiveLimit >= 1 && globalActiveLimit <= 256, "globalActiveLimit must be between 1 and 256")
      require(!target.isActive, "target must describe an inactive state before activation")
      reservedWrite {
        admitInactive(selectStoredRow(target.uuid), authenticatedOwner, expectedStateVersion, globalActiveLimit) match
          case Left(rejected) => rejected
          case Right(stored) =>
            require(target.operatingMode == stored.operatingMode, s"Activation cannot change operating mode for pod ${target.uuid}")
            val persisted = target.copy(
              uuid = stored.uuid,
              ownerUuid = stored.ownerUuid,
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = true,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = Some(dimensionId),
              operatingMode = stored.operatingMode
            )
            val statement = connection.prepareStatement(ActivateSql)
            JdbcResource.withResource(statement):
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Activation changed unexpectedly for UUID ${target.uuid}")
            AdmissionOutcome.Accepted(persisted)
      }
    }

  /** 用写保留事务原子提交 Recall 的收回状态，确保版本与所有者校验在同一写边界内完成。 */
  override def deactivateIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Try[DeactivationOutcome] =
    guarded("deactivateIfExpectedActive", Option(podUuid), DeactivateSql) {
      Objects.requireNonNull(podUuid, "podUuid must not be null")
      Objects.requireNonNull(expectedOwner, "expectedOwner must not be null")
      require(expectedStateVersion >= 0L, "expectedStateVersion must not be negative")
      reservedWrite {
        validateExpectedActiveDeactivation(selectStoredRow(podUuid), expectedOwner, expectedStateVersion) match
          case Left(rejected) => rejected
          case Right(stored) =>
            val persisted = stateForGenericRead(stored).copy(
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = false,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = stored.dimensionId
            )
            val statement = connection.prepareStatement(DeactivateSql)
            JdbcResource.withResource(statement):
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Deactivation changed unexpectedly for UUID $podUuid")
            DeactivationOutcome.Accepted(persisted)
      }
    }

  /**
   * 删除指定状态，并通过布尔结果区分不存在和已删除。
   */
  override def delete(uuid: UUID): Try[Boolean] =
    guarded("delete", Option(uuid), DeleteSql) {
      Objects.requireNonNull(uuid, "uuid must not be null")
      selectStoredRow(uuid) match
        case None => false
        case Some(current) if current.isActive =>
          throw new IllegalStateException(s"Cannot delete active FloatingPodState $uuid")
        case Some(_) =>
          val statement = connection.prepareStatement(DeleteSql)
          JdbcResource.withResource(statement):
            statement.setBytes(1, uuidToBytes(uuid))
            statement.setBytes(2, uuidToBytes(uuid))
            if statement.executeUpdate() == 1 then true
            else
              selectPhysicalRecoveryLocator(uuid) match
                case Some(_) => throw new IllegalStateException(s"Cannot delete FloatingPodState $uuid while a physical recovery locator exists")
                case None =>
                  selectStoredRow(uuid) match
                    case None => false
                    case Some(current) if current.isActive => throw new IllegalStateException(s"Cannot delete active FloatingPodState $uuid")
                    case Some(_) => throw new IllegalStateException(s"Inactive FloatingPodState changed unexpectedly during delete: $uuid")
    }

  /**
   * 在同一连接上执行完整事务；代码块失败时回滚所有已执行的数据库操作。
   */
  override def transaction[T](fn: => T): Try[T] =
    guarded("transaction", None, "TRANSACTION") {
      TransactionBoundary.execute(transactionControl, poisonConnection)(fn)
    }

  /**
   * 显式关闭唯一 JDBC 连接；重复关闭或关闭失败均立即报告，避免后台继续使用失效连接。
   */
  override def close(): Unit = this.synchronized {
    if closed then
      val exception = new IllegalStateException("SQLite repository is already closed")
      logFailure("close", None, "CLOSE CONNECTION", exception)
      throw exception
    closed = true
    try
      transactionControl.close()
    catch
      case exception: Throwable =>
        logFailure("close", None, "CLOSE CONNECTION", exception)
        throw new IllegalStateException("Failed to close SQLite repository", exception)
  }

  private def initializeResources(): Resources =
    var opened: Connection = null
    var control: TransactionControl = null
    try
      opened = injectedConnection.getOrElse(DriverManager.getConnection(toJdbcUrl(dbPath)))
      control = injectedTransactionControl.getOrElse(JdbcTransactionControl(opened))
      configureConnection(opened)
      SchemaManager.applyMigrations(opened, control)
      Resources(opened, control)
    catch
      case exception: Throwable =>
        if control != null then
          try control.close()
          catch
            case closeFailure: Throwable => exception.addSuppressed(closeFailure)
        else if opened != null then
          try opened.close()
          catch
            case closeFailure: Throwable => exception.addSuppressed(closeFailure)
        logFailure("initialize", None, "connection configuration and migrations", exception)
        throw exception

  private def openStandardResources(): Resources =
    var opened: Connection = null
    try
      opened = DriverManager.getConnection(toJdbcUrl(dbPath))
      val control = JdbcTransactionControl(opened)
      configureConnection(opened)
      SchemaManager.applyMigrations(opened, control)
      Resources(opened, control)
    catch
      case exception: Throwable =>
        if opened != null then
          try opened.close()
          catch
            case closeFailure: Throwable => exception.addSuppressed(closeFailure)
        throw exception

  private def resolveOperatingModeWriteAfterFailure(
      expectedBinding: PhysicalRecoveryBinding,
      podUuid: UUID,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      attemptUuid: UUID,
      targetMode: PodOperatingMode,
      actualPose: FloatingPodPose,
      primaryFailure: Throwable
  ): Option[OperatingModeChangeOutcome] =
    val normalizedDbPath = dbPath.toLowerCase
    if dbPath == "injected SQLite connection" || normalizedDbPath.contains(":memory:") || normalizedDbPath.contains("mode=memory") then
      return None

    var probe: SQLiteFloatingPodRepository = null
    var observed: Option[FloatingPodState] = None
    var readbackFailure: Throwable = null
    try
      probe = new SQLiteFloatingPodRepository(dbPath)
      val binding = probe.findPhysicalRecoveryBinding().get
      val state = probe.findByUuid(podUuid).get
      val locator = probe.findPhysicalRecoveryLocator(podUuid).get
      observed = for
        currentBinding <- binding if currentBinding == expectedBinding && currentBinding.cutoverState == PhysicalRecoveryCutoverState.Certified
        currentState <- state if currentState.ownerUuid.contains(authenticatedOwner)
          && currentState.isActive
          && currentState.stateVersion == Math.addExact(expectedStateVersion, 1L)
          && currentState.operatingMode == targetMode
          && poseMatches(currentState, actualPose)
        currentLocator <- locator if currentLocator.attemptUuid == attemptUuid
          && currentLocator.state == PhysicalRecoveryLocatorState.RowActive
          && currentLocator.reasonCode == PhysicalRecoveryLocatorReason.RowActive
          && currentLocator.domainVersion.contains(currentState.stateVersion)
          && currentLocator.targetDimensionId.contains(actualPose.dimensionId)
          && currentLocator.chunks.toSet == recoveryChunks(actualPose.dimensionId, actualPose.posX, actualPose.posZ)
          && currentLocator.chunks.size == 25
      yield currentState
    catch
      case exception: Throwable => readbackFailure = exception
    finally
      if probe != null then
        try probe.close()
        catch
          case closeFailure: Throwable =>
            if readbackFailure == null then readbackFailure = closeFailure
            else readbackFailure.addSuppressed(closeFailure)

    if readbackFailure != null then
      if readbackFailure ne primaryFailure then primaryFailure.addSuppressed(readbackFailure)
      None
    else observed.map { committedState =>
      if closed then
        try
          val replacement = openStandardResources()
          resources = replacement
          connection = replacement.connection
          transactionControl = replacement.transactionControl
          closed = false
        catch
          case reopenFailure: Throwable =>
            primaryFailure.addSuppressed(reopenFailure)
            throw primaryFailure
      logger.warn(s"SQLite operating mode write response was uncertain; exact committed CAS readback confirmed pod $podUuid", primaryFailure)
      OperatingModeChangeOutcome.Changed(committedState)
    }

  private def poisonConnection(primaryFailure: Throwable): Unit =
    closed = true
    try transactionControl.close()
    catch
      case closeFailure: Throwable => primaryFailure.addSuppressed(closeFailure)

  private def guarded[T](operation: String, uuid: Option[UUID], sql: String)(fn: => T): Try[T] =
    Try(this.synchronized {
      ensureOpen()
      fn
    }) match
      case success @ Success(_) => success
      case Failure(exception) =>
        logFailure(operation, uuid, sql, exception)
        Failure(exception)

  private def ensureOpen(): Unit =
    if closed then
      throw new IllegalStateException("SQLite repository is closed")

  private def configureConnection(target: Connection): Unit =
    val statement = target.createStatement()
    JdbcResource.withResource(statement):
      statement.execute("PRAGMA foreign_keys = ON")
      val foreignKeys = statement.executeQuery("PRAGMA foreign_keys")
      JdbcResource.withResource(foreignKeys):
        if !foreignKeys.next() || foreignKeys.getInt(1) != 1 then
          throw new IllegalStateException("SQLite foreign key enforcement is unavailable")

      val journalMode = statement.executeQuery("PRAGMA journal_mode = WAL")
      JdbcResource.withResource(journalMode):
        if !journalMode.next() then
          throw new IllegalStateException("SQLite journal mode query returned no row")
        val effectiveMode = journalMode.getString(1)
        if "wal".equalsIgnoreCase(effectiveMode) then
          logger.info("SQLite journal mode is WAL")
        else
          logger.info("SQLite WAL is unsupported for this connection; effective journal mode={}", effectiveMode)

  private def selectSingle(
      sql: String,
      bind: PreparedStatement => Unit
  ): Option[FloatingPodState] =
    val statement = connection.prepareStatement(sql)
    JdbcResource.withResource(statement):
      bind(statement)
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        if resultSet.next() then Some(readState(resultSet)) else None

  private def selectMany(
      sql: String,
      bind: PreparedStatement => Unit
  ): List[FloatingPodState] =
    val statement = connection.prepareStatement(sql)
    JdbcResource.withResource(statement):
      bind(statement)
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        val states = List.newBuilder[FloatingPodState]
        while resultSet.next() do states += readState(resultSet)
        states.result()

  private def selectStoredRows(
      sql: String,
      bind: PreparedStatement => Unit
  ): List[StoredPodRow] =
    val statement = connection.prepareStatement(sql)
    JdbcResource.withResource(statement):
      bind(statement)
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        val rows = List.newBuilder[StoredPodRow]
        while resultSet.next() do rows += readStoredRow(resultSet)
        rows.result()

  private def selectStoredRow(uuid: UUID): Option[StoredPodRow] =
    val statement = connection.prepareStatement(FindByUuidSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(uuid))
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet)(if resultSet.next() then Some(readStoredRow(resultSet)) else None)

  private def selectPhysicalRecoveryLocator(podUuid: UUID): Option[PhysicalRecoveryLocator] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryLocatorSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(podUuid))
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        if resultSet.next() then
          val attemptUuid = bytesToUuid(resultSet.getBytes("attempt_uuid"), "attempt_uuid")
          val state = PhysicalRecoveryLocatorState.fromDatabase(resultSet.getString("locator_state"))
          val domainVersion = resultSet.getLong("domain_version")
          val version = if resultSet.wasNull() then None else Some(domainVersion)
          val dimension = resultSet.getInt("target_dimension_id")
          val targetDimension = if resultSet.wasNull() then None else Some(dimension)
          Some(PhysicalRecoveryLocator(podUuid, attemptUuid, state, resultSet.getString("reason_code"), version, targetDimension, selectPhysicalRecoveryChunks(podUuid)))
        else None

  private def selectAllPhysicalRecoveryLocators(): List[PhysicalRecoveryLocator] =
    val statement = connection.prepareStatement(FindAllPhysicalRecoveryLocatorPodUuidsSql)
    JdbcResource.withResource(statement):
      val resultSet = statement.executeQuery()
      val podUuids = JdbcResource.withResource(resultSet):
        val values = List.newBuilder[UUID]
        while resultSet.next() do values += bytesToUuid(resultSet.getBytes("pod_uuid"), "pod_uuid")
        values.result()
      podUuids.map { podUuid =>
        selectPhysicalRecoveryLocator(podUuid).getOrElse(throw new IllegalStateException(s"Physical recovery locator disappeared during global read for pod $podUuid"))
      }.sortBy(_.podUuid.toString)

  private def selectPhysicalRecoveryBinding(): Option[PhysicalRecoveryBinding] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryBindingSql)
    JdbcResource.withResource(statement):
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        if resultSet.next() then
          Some(PhysicalRecoveryBinding(
            bytesToUuid(resultSet.getBytes("binding_uuid"), "binding_uuid"),
            PhysicalRecoveryCutoverState.fromDatabase(resultSet.getString("cutover_state")),
            resultSet.getInt("format_version")
          ))
        else None

  private def readUnboundIntentColumns(resultSet: ResultSet): UnboundIntentColumns =
    val catalogBytes = resultSet.getBytes("catalog_generation")
    val operationBytes = resultSet.getBytes("create_operation_uuid")
    val committedVersion = resultSet.getLong("committed_domain_version")
    val hasCommittedVersion = !resultSet.wasNull()
    val committedAt = resultSet.getLong("committed_at")
    val hasCommittedAt = !resultSet.wasNull()
    val committedDimension = resultSet.getInt("committed_dimension_id")
    val hasCommittedDimension = !resultSet.wasNull()
    val phase = resultSet.getString("phase")
    val committedState =
      if phase == "COMMITTED" && hasCommittedVersion && hasCommittedAt && hasCommittedDimension then
        Some(FloatingPodState(
          bytesToUuid(resultSet.getBytes("pod_uuid"), "pod_uuid"),
          None,
          committedVersion,
          resultSet.getString("committed_world_name"),
          resultSet.getDouble("committed_pos_x"),
          resultSet.getDouble("committed_pos_y"),
          resultSet.getDouble("committed_pos_z"),
          resultSet.getFloat("committed_yaw"),
          resultSet.getFloat("committed_pitch"),
          true,
          committedAt,
          Some(committedDimension),
          PodOperatingMode.WorldIdle
        ))
      else None
    UnboundIntentColumns(
      podUuid = bytesToUuid(resultSet.getBytes("pod_uuid"), "pod_uuid"),
      attemptUuid = bytesToUuid(resultSet.getBytes("attempt_uuid"), "attempt_uuid"),
      bindingUuid = bytesToUuid(resultSet.getBytes("binding_uuid"), "binding_uuid"),
      origin = resultSet.getString("origin"),
      phase = phase,
      sourceKind = Option(resultSet.getString("source_kind")),
      sourceIdentity = Option(resultSet.getString("source_identity")),
      catalogGeneration = Option(catalogBytes).map(bytesToUuid(_, "catalog_generation")),
      createOperationUuid = Option(operationBytes).map(bytesToUuid(_, "create_operation_uuid")),
      preparedVersion = resultSet.getLong("prepared_domain_version"),
      targetPose = FloatingPodPose(
        resultSet.getString("target_world_name"),
        resultSet.getDouble("target_pos_x"),
        resultSet.getDouble("target_pos_y"),
        resultSet.getDouble("target_pos_z"),
        resultSet.getFloat("target_yaw"),
        resultSet.getFloat("target_pitch"),
        resultSet.getInt("target_dimension_id")
      ),
      targetChunkX = resultSet.getInt("target_chunk_x"),
      targetChunkZ = resultSet.getInt("target_chunk_z"),
      committedState = committedState
    )

  private def materializeUnboundIntent(columns: UnboundIntentColumns, binding: PhysicalRecoveryBinding): StoredUnboundIntent =
    if binding.bindingUuid != columns.bindingUuid then
      throw new IllegalStateException(s"Unbound intent binding changed for pod ${columns.podUuid}")
    val origin = columns.origin match
      case "UNBOUND_ADOPTION" => UnboundIntentOrigin.ObservedArtifactAdoption
      case "DEBUG_ITEM_CREATE" => UnboundIntentOrigin.DebugItemCreate
      case other => throw new IllegalStateException(s"Unknown unbound intent origin $other")
    val observedIdentity = (columns.sourceKind, columns.sourceIdentity, columns.catalogGeneration) match
      case (Some("FRESH"), Some(identity), Some(generation)) => Some(ObservedUnboundIdentity(ObservedUnboundSourceKind.FreshArtifact, generation, identity))
      case (Some("LEGACY_UNREGISTERED"), Some(identity), Some(generation)) => Some(ObservedUnboundIdentity(ObservedUnboundSourceKind.LegacyUnregistered, generation, identity))
      case (None, None, None) => None
      case _ => throw new IllegalStateException(s"Unbound intent source identity is incomplete for pod ${columns.podUuid}")
    val receipt = UnboundPreparationReceipt(
      binding,
      columns.podUuid,
      columns.attemptUuid,
      origin,
      columns.targetPose,
      observedIdentity,
      columns.createOperationUuid,
      columns.preparedVersion
    )
    origin match
      case UnboundIntentOrigin.ObservedArtifactAdoption if columns.createOperationUuid.nonEmpty =>
        throw new IllegalStateException(s"Observed unbound intent unexpectedly contains a create operation for pod ${columns.podUuid}")
      case UnboundIntentOrigin.DebugItemCreate if columns.createOperationUuid.isEmpty || observedIdentity.nonEmpty =>
        throw new IllegalStateException(s"Controlled-create intent identity is malformed for pod ${columns.podUuid}")
      case UnboundIntentOrigin.ObservedArtifactAdoption if observedIdentity.isEmpty =>
        throw new IllegalStateException(s"Observed unbound intent has no source identity for pod ${columns.podUuid}")
      case _ => ()
    StoredUnboundIntent(receipt, columns.phase, columns.committedState)

  private def insertPhysicalRecoveryBinding(binding: PhysicalRecoveryBinding): Unit =
    val statement = connection.prepareStatement(InsertPhysicalRecoveryBindingSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(binding.bindingUuid))
      statement.setString(2, binding.cutoverState.databaseValue)
      statement.setInt(3, binding.formatVersion)
      if statement.executeUpdate() != 1 then throw new IllegalStateException("Physical recovery binding insert did not affect the singleton row")

  private def certifyPhysicalRecoveryBinding(binding: PhysicalRecoveryBinding): Unit =
    val statement = connection.prepareStatement(CertifyPhysicalRecoveryBindingSql)
    JdbcResource.withResource(statement):
      statement.setString(1, binding.cutoverState.databaseValue)
      statement.setBytes(2, uuidToBytes(binding.bindingUuid))
      statement.setInt(3, binding.formatVersion)
      if statement.executeUpdate() != 1 then throw new IllegalStateException("Physical recovery binding changed unexpectedly during certification")

  private def selectPhysicalRecoveryChunks(podUuid: UUID): List[PhysicalRecoveryChunk] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryChunksSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(podUuid))
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        val chunks = List.newBuilder[PhysicalRecoveryChunk]
        while resultSet.next() do chunks += PhysicalRecoveryChunk(resultSet.getInt("dimension_id"), resultSet.getInt("chunk_x"), resultSet.getInt("chunk_z"))
        chunks.result()

  private def insertPhysicalRecoveryLocator(podUuid: UUID, attemptUuid: UUID, domainVersion: Long, targetDimensionId: Int): Unit =
    val statement = connection.prepareStatement(InsertPhysicalRecoveryLocatorSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(podUuid))
      statement.setBytes(2, uuidToBytes(attemptUuid))
      statement.setString(3, PhysicalRecoveryLocatorState.CreatePrepared.databaseValue)
      statement.setString(4, PhysicalRecoveryLocatorReason.CreatePrepared)
      statement.setLong(5, domainVersion)
      statement.setInt(6, targetDimensionId)
      if statement.executeUpdate() != 1 then throw new IllegalStateException(s"Physical recovery locator insert did not affect pod $podUuid")

  private def replacePhysicalRecoveryChunks(podUuid: UUID, chunks: List[PhysicalRecoveryChunk]): Unit =
    val delete = connection.prepareStatement(DeletePhysicalRecoveryChunksSql)
    JdbcResource.withResource(delete):
      delete.setBytes(1, uuidToBytes(podUuid))
      delete.executeUpdate()
    val insert = connection.prepareStatement(InsertPhysicalRecoveryChunkSql)
    JdbcResource.withResource(insert):
      chunks.foreach { chunk =>
        insert.setBytes(1, uuidToBytes(podUuid))
        insert.setInt(2, chunk.dimensionId)
        insert.setInt(3, chunk.chunkX)
        insert.setInt(4, chunk.chunkZ)
        if insert.executeUpdate() != 1 then throw new IllegalStateException(s"Physical recovery chunk insert did not affect pod $podUuid")
      }

  private def requireExactPreparedLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int): Unit =
    val expectedChunks = selectPhysicalRecoveryLocator(podUuid) match
      case Some(locator)
          if locator.attemptUuid == attemptUuid
            && locator.state == PhysicalRecoveryLocatorState.CreatePrepared
            && locator.reasonCode == PhysicalRecoveryLocatorReason.CreatePrepared
            && locator.domainVersion.contains(expectedDomainVersion)
            && locator.targetDimensionId.contains(targetDimensionId) => locator.chunks
      case _ => throw new IllegalStateException(s"Physical recovery locator is not the exact prepared activation for pod $podUuid")
    val chunks = expectedChunks.toSet
    val chunkXs = chunks.map(_.chunkX)
    val chunkZs = chunks.map(_.chunkZ)
    val areConsecutive = (coordinates: Set[Int]) => coordinates.toList.sorted match
      case first :: second :: third :: fourth :: fifth :: Nil =>
        second == first + 1 && third == second + 1 && fourth == third + 1 && fifth == fourth + 1
      case _ => false
    val exactChunks = (for chunkX <- chunkXs; chunkZ <- chunkZs yield PhysicalRecoveryChunk(targetDimensionId, chunkX, chunkZ)).toSet
    if expectedChunks.size != 25 || chunks.size != 25 || !chunks.forall(_.dimensionId == targetDimensionId) || !areConsecutive(chunkXs) || !areConsecutive(chunkZs) || chunks != exactChunks then
      throw new IllegalStateException(s"Physical recovery locator chunks are not an exact prepared 5x5 square for pod $podUuid")

  private def requireExactActiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int): Unit =
    val expectedChunks = selectPhysicalRecoveryLocator(podUuid) match
      case Some(locator)
          if locator.attemptUuid == attemptUuid
            && locator.state == PhysicalRecoveryLocatorState.RowActive
            && locator.reasonCode == PhysicalRecoveryLocatorReason.RowActive
            && locator.domainVersion.contains(expectedDomainVersion)
            && locator.targetDimensionId.contains(targetDimensionId) => locator.chunks
      case _ => throw new IllegalStateException(s"Physical recovery locator is not the exact active deactivation for pod $podUuid")
    val chunks = expectedChunks.toSet
    val chunkXs = chunks.map(_.chunkX)
    val chunkZs = chunks.map(_.chunkZ)
    val areConsecutive = (coordinates: Set[Int]) => coordinates.toList.sorted match
      case first :: second :: third :: fourth :: fifth :: Nil =>
        second == first + 1 && third == second + 1 && fourth == third + 1 && fifth == fourth + 1
      case _ => false
    val exactChunks = (for chunkX <- chunkXs; chunkZ <- chunkZs yield PhysicalRecoveryChunk(targetDimensionId, chunkX, chunkZ)).toSet
    if expectedChunks.size != 25 || chunks.size != 25 || !chunks.forall(_.dimensionId == targetDimensionId) || !areConsecutive(chunkXs) || !areConsecutive(chunkZs) || chunks != exactChunks then
      throw new IllegalStateException(s"Physical recovery locator chunks are not an exact active 5x5 square for pod $podUuid")

  private def requireExactInactiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int): PhysicalRecoveryLocator =
    val locator = selectPhysicalRecoveryLocator(podUuid) match
      case Some(candidate)
          if candidate.attemptUuid == attemptUuid
            && candidate.domainVersion.contains(expectedDomainVersion)
            && candidate.targetDimensionId.contains(targetDimensionId)
            && ((candidate.state == PhysicalRecoveryLocatorState.RowInactivePending && candidate.reasonCode == PhysicalRecoveryLocatorReason.RowInactivePending)
              || (candidate.state == PhysicalRecoveryLocatorState.RowInactiveVerified && candidate.reasonCode == PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified)) => candidate
      case _ => throw new IllegalStateException(s"Physical recovery locator is not the exact inactive absence verification for pod $podUuid")
    val chunks = locator.chunks.toSet
    val chunkXs = chunks.map(_.chunkX)
    val chunkZs = chunks.map(_.chunkZ)
    val areConsecutive = (coordinates: Set[Int]) => coordinates.toList.sorted match
      case first :: second :: third :: fourth :: fifth :: Nil =>
        second == first + 1 && third == second + 1 && fourth == third + 1 && fifth == fourth + 1
      case _ => false
    val exactChunks = (for chunkX <- chunkXs; chunkZ <- chunkZs yield PhysicalRecoveryChunk(targetDimensionId, chunkX, chunkZ)).toSet
    if locator.chunks.size != 25 || chunks.size != 25 || !chunks.forall(_.dimensionId == targetDimensionId) || !areConsecutive(chunkXs) || !areConsecutive(chunkZs) || chunks != exactChunks then
      throw new IllegalStateException(s"Physical recovery locator chunks are not an exact inactive 5x5 square for pod $podUuid")
    locator

  private def activatePreparedLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int, committedDomainVersion: Long): Unit =
    val statement = connection.prepareStatement(ActivatePreparedLocatorSql)
    JdbcResource.withResource(statement):
      statement.setString(1, PhysicalRecoveryLocatorState.RowActive.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.RowActive)
      statement.setLong(3, committedDomainVersion)
      statement.setBytes(4, uuidToBytes(podUuid))
      statement.setBytes(5, uuidToBytes(attemptUuid))
      statement.setLong(6, expectedDomainVersion)
      statement.setInt(7, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during activation for pod $podUuid")

  private def deactivateActiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int, committedDomainVersion: Long): Unit =
    val statement = connection.prepareStatement(DeactivateActiveLocatorSql)
    JdbcResource.withResource(statement):
      statement.setString(1, PhysicalRecoveryLocatorState.RowInactivePending.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.RowInactivePending)
      statement.setLong(3, committedDomainVersion)
      statement.setBytes(4, uuidToBytes(podUuid))
      statement.setBytes(5, uuidToBytes(attemptUuid))
      statement.setLong(6, expectedDomainVersion)
      statement.setInt(7, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during deactivation for pod $podUuid")

  private def verifyInactiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int): Unit =
    val statement = connection.prepareStatement(VerifyInactiveAbsenceLocatorSql)
    JdbcResource.withResource(statement):
      statement.setString(1, PhysicalRecoveryLocatorState.RowInactiveVerified.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified)
      statement.setBytes(3, uuidToBytes(podUuid))
      statement.setBytes(4, uuidToBytes(attemptUuid))
      statement.setLong(5, expectedDomainVersion)
      statement.setInt(6, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during inactive absence verification for pod $podUuid")

  private def validateExpectedActiveDeactivation(
      stored: Option[StoredPodRow],
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Either[DeactivationOutcome.Rejected, StoredPodRow] =
    stored match
      case None => Left(DeactivationOutcome.Rejected(DeactivationRejection.UnknownPod))
      case Some(row) if row.ownerUuid != expectedOwner => Left(DeactivationOutcome.Rejected(DeactivationRejection.NotOwner))
      case Some(row) if row.stateVersion != expectedStateVersion => Left(DeactivationOutcome.Rejected(DeactivationRejection.VersionMismatch(row.stateVersion)))
      case Some(row) if !row.isActive => Left(DeactivationOutcome.Rejected(DeactivationRejection.NotActive))
      case Some(row) => Right(row)

  private def admitInactive(
      stored: Option[StoredPodRow],
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      globalActiveLimit: Int
  ): Either[AdmissionOutcome.Rejected, StoredPodRow] =
    stored match
      case None => Left(AdmissionOutcome.Rejected(AdmissionRejection.UnknownPod))
      case Some(row) if row.ownerUuid != Some(authenticatedOwner) => Left(AdmissionOutcome.Rejected(AdmissionRejection.NotOwner))
      case Some(row) if row.stateVersion != expectedStateVersion => Left(AdmissionOutcome.Rejected(AdmissionRejection.VersionMismatch(row.stateVersion)))
      case Some(row) if row.isActive => Left(AdmissionOutcome.Rejected(AdmissionRejection.AlreadyActive))
      case Some(row) if activeCount() >= globalActiveLimit => Left(AdmissionOutcome.Rejected(AdmissionRejection.GlobalActiveLimitReached))
      case Some(row) =>
        val entitlement = entitlementFor(authenticatedOwner)
        if activeCountForOwner(authenticatedOwner) >= entitlement.simultaneousActiveLimit then
          Left(AdmissionOutcome.Rejected(AdmissionRejection.PlayerActiveLimitReached))
        else Right(row)

  private def selectEntitlement(playerUuid: UUID): Option[PlayerEntitlement] =
    val statement = connection.prepareStatement(FindEntitlementSql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(playerUuid))
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        if resultSet.next() then
          Some(PlayerEntitlement(playerUuid, resultSet.getInt("registration_limit"), resultSet.getInt("simultaneous_active_limit")))
        else None

  private def bindInsert(statement: PreparedStatement, state: FloatingPodState): Unit =
    statement.setBytes(1, uuidToBytes(state.uuid))
    setOwner(statement, 2, state.ownerUuid)
    statement.setLong(3, state.stateVersion)
    statement.setString(4, state.worldName)
    statement.setDouble(5, state.posX)
    statement.setDouble(6, state.posY)
    statement.setDouble(7, state.posZ)
    statement.setDouble(8, state.yaw.toDouble)
    statement.setDouble(9, state.pitch.toDouble)
    statement.setInt(10, if state.isActive then 1 else 0)
    statement.setLong(11, state.lastUpdated)
    setDimension(statement, 12, state.dimensionId)
    statement.setString(13, state.operatingMode.databaseValue)

  private def bindUpdate(statement: PreparedStatement, state: FloatingPodState, updatedAt: Long): Unit =
    setOwner(statement, 1, state.ownerUuid)
    statement.setString(2, state.worldName)
    statement.setDouble(3, state.posX)
    statement.setDouble(4, state.posY)
    statement.setDouble(5, state.posZ)
    statement.setDouble(6, state.yaw.toDouble)
    statement.setDouble(7, state.pitch.toDouble)
    statement.setLong(8, updatedAt)
    statement.setBytes(9, uuidToBytes(state.uuid))
    statement.setLong(10, state.stateVersion)

  private def bindTransition(statement: PreparedStatement, state: FloatingPodState, expectedVersion: Long): Unit =
    statement.setString(1, state.worldName)
    statement.setDouble(2, state.posX)
    statement.setDouble(3, state.posY)
    statement.setDouble(4, state.posZ)
    statement.setDouble(5, state.yaw.toDouble)
    statement.setDouble(6, state.pitch.toDouble)
    statement.setInt(7, if state.isActive then 1 else 0)
    statement.setLong(8, state.lastUpdated)
    setDimension(statement, 9, state.dimensionId)
    statement.setBytes(10, uuidToBytes(state.uuid))
    statement.setLong(11, expectedVersion)

  private def setOwner(statement: PreparedStatement, index: Int, ownerUuid: Option[UUID]): Unit =
    ownerUuid match
      case Some(value) => statement.setBytes(index, uuidToBytes(value))
      case None => statement.setNull(index, Types.BLOB)

  private def setDimension(statement: PreparedStatement, index: Int, dimensionId: Option[Int]): Unit =
    dimensionId match
      case Some(value) => statement.setInt(index, value)
      case None => statement.setNull(index, Types.INTEGER)

  private def readState(resultSet: ResultSet): FloatingPodState = stateForGenericRead(readStoredRow(resultSet))

  private def readStoredRow(resultSet: ResultSet): StoredPodRow =
    val ownerBytes = resultSet.getBytes("owner_uuid")
    val dimension = resultSet.getInt("dimension_id")
    val dimensionId = if resultSet.wasNull() then None else Some(dimension)
    StoredPodRow(
      uuid = bytesToUuid(resultSet.getBytes("uuid"), "uuid"),
      ownerUuid = Option(ownerBytes).map(bytesToUuid(_, "owner_uuid")),
      stateVersion = resultSet.getLong("state_version"),
      worldName = resultSet.getString("world_name"),
      posX = resultSet.getDouble("pos_x"),
      posY = resultSet.getDouble("pos_y"),
      posZ = resultSet.getDouble("pos_z"),
      yaw = resultSet.getFloat("yaw"),
      pitch = resultSet.getFloat("pitch"),
      isActive = readActiveFlag(resultSet),
      lastUpdated = resultSet.getLong("last_updated"),
      dimensionId = dimensionId,
      operatingMode = PodOperatingMode.fromDatabase(resultSet.getString("operating_mode"))
    )

  private def stateForGenericRead(stored: StoredPodRow): FloatingPodState =
    FloatingPodState(
      stored.uuid,
      stored.ownerUuid,
      stored.stateVersion,
      stored.worldName,
      stored.posX,
      stored.posY,
      stored.posZ,
      stored.yaw,
      stored.pitch,
      stored.isActive,
      stored.lastUpdated,
      stored.dimensionId,
      stored.operatingMode
    )

  private def entitlementFor(ownerUuid: UUID): PlayerEntitlement =
    val insert = connection.prepareStatement(EnsureEntitlementSql)
    JdbcResource.withResource(insert):
      insert.setBytes(1, uuidToBytes(ownerUuid))
      insert.executeUpdate()

    val select = connection.prepareStatement(FindEntitlementSql)
    JdbcResource.withResource(select):
      select.setBytes(1, uuidToBytes(ownerUuid))
      val resultSet = select.executeQuery()
      JdbcResource.withResource(resultSet):
        if !resultSet.next() then
          throw new IllegalStateException(s"Missing entitlement after initialization for player $ownerUuid")
        PlayerEntitlement(ownerUuid, resultSet.getInt("registration_limit"), resultSet.getInt("simultaneous_active_limit"))

  private def activeCountForOwner(ownerUuid: UUID): Int = count(ActiveCountForOwnerSql, ownerUuid)

  private def activeCount(): Int =
    val statement = connection.createStatement()
    JdbcResource.withResource(statement):
      val resultSet = statement.executeQuery(ActiveCountSql)
      JdbcResource.withResource(resultSet):
        if !resultSet.next() then throw new IllegalStateException("Active count query returned no row")
        resultSet.getInt(1)

  private def chunkCoordinate(position: Double): Int = Math.floor(position / PhysicalChunkSize).toInt

  private def poseMatches(state: FloatingPodState, pose: FloatingPodPose): Boolean =
    state.worldName == pose.worldName
      && state.posX == pose.posX
      && state.posY == pose.posY
      && state.posZ == pose.posZ
      && state.yaw == pose.yaw
      && state.pitch == pose.pitch
      && state.dimensionId.contains(pose.dimensionId)

  private def poseMatches(state: StoredPodRow, pose: FloatingPodPose): Boolean =
    state.worldName == pose.worldName
      && state.posX == pose.posX
      && state.posY == pose.posY
      && state.posZ == pose.posZ
      && state.yaw == pose.yaw
      && state.pitch == pose.pitch
      && state.dimensionId.contains(pose.dimensionId)

  private def recoveryChunks(dimensionId: Int, posX: Double, posZ: Double): Set[PhysicalRecoveryChunk] =
    val centerX = chunkCoordinate(posX)
    val centerZ = chunkCoordinate(posZ)
    (for
      chunkX <- centerX - 2 to centerX + 2
      chunkZ <- centerZ - 2 to centerZ + 2
    yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet

  private def count(sql: String, ownerUuid: UUID): Int =
    val statement = connection.prepareStatement(sql)
    JdbcResource.withResource(statement):
      statement.setBytes(1, uuidToBytes(ownerUuid))
      val resultSet = statement.executeQuery()
      JdbcResource.withResource(resultSet):
        if !resultSet.next() then throw new IllegalStateException("Count query returned no row")
        resultSet.getInt(1)

  private def reservedWrite[T](fn: => T): T =
    TransactionBoundary.executeImmediate(connection, transactionControl, poisonConnection)(fn)

  private def uuidToBytes(uuid: UUID): Array[Byte] =
    Objects.requireNonNull(uuid, "uuid must not be null")
    ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()

  private def bytesToUuid(bytes: Array[Byte], column: String): UUID =
    if bytes == null || bytes.length != 16 then
      throw new IllegalStateException(s"SQLite column $column does not contain a 16-byte UUID")
    val buffer = ByteBuffer.wrap(bytes)
    new UUID(buffer.getLong(), buffer.getLong())

  private def readActiveFlag(resultSet: ResultSet): Boolean =
    resultSet.getInt("is_active") match
      case 0 => false
      case 1 => true
      case value => throw new IllegalStateException(s"SQLite column is_active has invalid value $value")

  private def toJdbcUrl(path: String): String =
    if path.startsWith("jdbc:sqlite:") then path else s"jdbc:sqlite:$path"

  private def isUniqueConstraint(exception: SQLException): Boolean =
    exception.getErrorCode == 19 || Option(exception.getSQLState).exists(_.startsWith("23"))
      || Option(exception.getMessage).exists(_.toLowerCase.contains("unique constraint"))

  private def logFailure(operation: String, uuid: Option[UUID], sql: String, exception: Throwable): Unit =
    val uuidContext = uuid.map(_.toString).getOrElse("N/A")
    val sqliteCode = exception match
      case sqlException: SQLException => sqlException.getErrorCode.toString
      case _ => "N/A"
    logger.error(
      s"SQLite operation failed: operation=$operation, uuid=$uuidContext, sql=$sql, sqliteErrorCode=$sqliteCode",
      exception
    )

private[sqlite] object SQLiteFloatingPodRepository:
  private[sqlite] val InsertSql =
    "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  private[sqlite] val UpdateSql =
    "UPDATE floating_pod_state SET owner_uuid = ?, state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, last_updated = ? WHERE uuid = ? AND state_version = ?"
  private[sqlite] val FindByUuidSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode FROM floating_pod_state WHERE uuid = ?"
  private[sqlite] val FindByOwnerSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode FROM floating_pod_state WHERE owner_uuid = ? ORDER BY uuid"
  private[sqlite] val FindAllActiveSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode FROM floating_pod_state WHERE is_active = 1 ORDER BY uuid"
  private[sqlite] val FindAllSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id, operating_mode FROM floating_pod_state ORDER BY uuid"
  private[sqlite] val DeleteSql = "DELETE FROM floating_pod_state WHERE uuid = ? AND is_active = 0 AND NOT EXISTS (SELECT 1 FROM physical_recovery_locator WHERE pod_uuid = ?)"
  private[sqlite] val ActivateSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, is_active = ?, last_updated = ?, dimension_id = ? WHERE uuid = ? AND state_version = ? AND is_active = 0"
  private[sqlite] val DeactivateSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, is_active = ?, last_updated = ?, dimension_id = ? WHERE uuid = ? AND state_version = ? AND is_active = 1"
  private[sqlite] val UpdateTrackedActiveDriftSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, last_updated = ? WHERE uuid = ? AND state_version = ? AND is_active = 1"
  private[sqlite] val SetOperatingModeSql =
    "UPDATE floating_pod_state SET operating_mode = ?, state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, last_updated = ? WHERE uuid = ? AND owner_uuid = ? AND state_version = ? AND operating_mode = ? AND is_active = 1 AND dimension_id = ?"
  private[sqlite] val BackfillDimensionSql =
    "UPDATE floating_pod_state SET dimension_id = ? WHERE uuid = ? AND state_version = ? AND is_active = 1 AND dimension_id IS NULL"
  private[sqlite] val EnsureEntitlementSql =
    "INSERT INTO player_entitlement (player_uuid, registration_limit, simultaneous_active_limit) VALUES (?, 1, 1) ON CONFLICT(player_uuid) DO NOTHING"
  private[sqlite] val FindEntitlementSql =
    "SELECT registration_limit, simultaneous_active_limit FROM player_entitlement WHERE player_uuid = ?"
  private[sqlite] val ActiveCountForOwnerSql = "SELECT COUNT(*) FROM floating_pod_state WHERE owner_uuid = ? AND is_active = 1"
  private[sqlite] val ActiveCountSql = "SELECT COUNT(*) FROM floating_pod_state WHERE is_active = 1"
  private[sqlite] val FindPhysicalRecoveryBindingSql =
    "SELECT binding_uuid, cutover_state, format_version FROM physical_recovery_binding WHERE singleton_fixed1 = 1"
  private[sqlite] val InsertPhysicalRecoveryBindingSql =
    "INSERT INTO physical_recovery_binding (singleton_fixed1, binding_uuid, cutover_state, format_version) VALUES (1, ?, ?, ?)"
  private[sqlite] val CertifyPhysicalRecoveryBindingSql =
    "UPDATE physical_recovery_binding SET cutover_state = ? WHERE singleton_fixed1 = 1 AND binding_uuid = ? AND format_version = ? AND cutover_state = 'LEGACY_UNCERTIFIED'"
  private[sqlite] val FindPhysicalRecoveryLocatorSql =
    "SELECT attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id FROM physical_recovery_locator WHERE pod_uuid = ?"
  private[sqlite] val FindAllPhysicalRecoveryLocatorPodUuidsSql =
    "SELECT pod_uuid FROM physical_recovery_locator"
  private[sqlite] val FindPhysicalRecoveryChunksSql =
    "SELECT dimension_id, chunk_x, chunk_z FROM physical_recovery_locator_chunk WHERE pod_uuid = ? ORDER BY dimension_id, chunk_x, chunk_z"
  private[sqlite] val FindUnboundIntentSql =
    "SELECT pod_uuid, attempt_uuid, binding_uuid, origin, phase, source_kind, source_identity, catalog_generation, create_operation_uuid, prepared_domain_version, target_world_name, target_pos_x, target_pos_y, target_pos_z, target_yaw, target_pitch, target_dimension_id, target_chunk_x, target_chunk_z, committed_domain_version, committed_at, committed_world_name, committed_pos_x, committed_pos_y, committed_pos_z, committed_yaw, committed_pitch, committed_dimension_id FROM physical_recovery_intent WHERE pod_uuid = ?"
  private[sqlite] val FindUnboundIntentByAttemptSql =
    "SELECT pod_uuid FROM physical_recovery_intent WHERE attempt_uuid = ?"
  private[sqlite] val FindUnboundIntentByObservedSourceSql =
    "SELECT pod_uuid FROM physical_recovery_intent WHERE binding_uuid = ? AND source_kind = ? AND catalog_generation = ? AND source_identity = ? AND origin = 'UNBOUND_ADOPTION'"
  private[sqlite] val FindUnboundIntentByCreateOperationSql =
    "SELECT pod_uuid FROM physical_recovery_intent WHERE binding_uuid = ? AND create_operation_uuid = ? AND origin = 'DEBUG_ITEM_CREATE'"
  private[sqlite] val InsertUnboundIntentSql =
    "INSERT INTO physical_recovery_intent (pod_uuid, attempt_uuid, binding_uuid, origin, phase, source_kind, source_identity, catalog_generation, create_operation_uuid, prepared_domain_version, target_world_name, target_pos_x, target_pos_y, target_pos_z, target_yaw, target_pitch, target_dimension_id, target_chunk_x, target_chunk_z) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  private[sqlite] val CommitUnboundDomainSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, is_active = 1, last_updated = ?, dimension_id = ? WHERE uuid = ? AND owner_uuid IS NULL AND state_version = ? AND is_active = 0 AND operating_mode = 'WORLD_IDLE' AND dimension_id = ?"
  private[sqlite] val CommitUnboundLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ?, domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND domain_version = ? AND target_dimension_id = ? AND locator_state = ? AND reason_code = ?"
  private[sqlite] val CommitUnboundIntentSql =
    "UPDATE physical_recovery_intent SET phase = ?, committed_domain_version = ?, committed_at = ?, committed_world_name = ?, committed_pos_x = ?, committed_pos_y = ?, committed_pos_z = ?, committed_yaw = ?, committed_pitch = ?, committed_dimension_id = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND phase = 'PREPARED'"
  private[sqlite] val InsertPhysicalRecoveryLocatorSql =
    "INSERT INTO physical_recovery_locator (pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id) VALUES (?, ?, ?, ?, ?, ?)"
  private[sqlite] val ActivatePreparedLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ?, domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'CREATE_PREPARED' AND reason_code = 'create_prepared' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val DeactivateActiveLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ?, domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'ROW_ACTIVE' AND reason_code = 'row_active' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val UpdateTrackedActiveLocatorSql =
    "UPDATE physical_recovery_locator SET domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'ROW_ACTIVE' AND reason_code = 'row_active' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val UpdateOperatingModeLocatorSql = UpdateTrackedActiveLocatorSql
  private[sqlite] val VerifyInactiveAbsenceLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'ROW_INACTIVE_PENDING' AND reason_code = 'row_inactive_pending' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val DeletePhysicalRecoveryChunksSql = "DELETE FROM physical_recovery_locator_chunk WHERE pod_uuid = ?"
  private[sqlite] val InsertPhysicalRecoveryChunkSql =
    "INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z) VALUES (?, ?, ?, ?)"
  private val PhysicalChunkSize = 16

  private[sqlite] final case class StoredUnboundIntent(
      receipt: UnboundPreparationReceipt,
      phase: String,
      committedState: Option[FloatingPodState]
  )

  private[sqlite] final case class UnboundIntentColumns(
      podUuid: UUID,
      attemptUuid: UUID,
      bindingUuid: UUID,
      origin: String,
      phase: String,
      sourceKind: Option[String],
      sourceIdentity: Option[String],
      catalogGeneration: Option[UUID],
      createOperationUuid: Option[UUID],
      preparedVersion: Long,
      targetPose: FloatingPodPose,
      targetChunkX: Int,
      targetChunkZ: Int,
      committedState: Option[FloatingPodState]
  )

  private[sqlite] final case class Resources(connection: Connection, transactionControl: TransactionControl)

/**
 * 抽象 JDBC transaction 的状态转换，使 failure injection 可直接验证主异常、rollback 和关闭顺序。
 */
private[sqlite] trait TransactionControl:
  /** 读取当前 JDBC auto-commit 状态。 */
  def autoCommit: Boolean

  /** 切换 JDBC auto-commit 状态。 */
  def setAutoCommit(enabled: Boolean): Unit

  /** 提交当前 JDBC transaction。 */
  def commit(): Unit

  /** 回滚当前 JDBC transaction。 */
  def rollback(): Unit

  /** 关闭唯一 JDBC connection，阻止继续写入。 */
  def close(): Unit

/** 资源释放不能替换 SQL/读取的首因；仅释放失败时仍明确失败。 */
private[sqlite] object JdbcResource:
  def withResource[T](resource: AutoCloseable)(body: => T): T =
    var primaryFailure: Throwable = null
    try body
    catch
      case failure: Throwable =>
        primaryFailure = failure
        throw failure
    finally
      if primaryFailure == null then resource.close()
      else
        try resource.close()
        catch
          case closeFailure: Throwable =>
            if closeFailure ne primaryFailure then primaryFailure.addSuppressed(closeFailure)

/**
 * 将 transaction control 操作直接委派给 repository 或 migration 所持有的唯一 JDBC connection。
 */
private[sqlite] final class JdbcTransactionControl(private[sqlite] val connection: Connection) extends TransactionControl:
  override def autoCommit: Boolean = connection.getAutoCommit

  override def setAutoCommit(enabled: Boolean): Unit = connection.setAutoCommit(enabled)

  override def commit(): Unit = connection.commit()

  override def rollback(): Unit = connection.rollback()

  override def close(): Unit = connection.close()

/** 隔离 BEGIN IMMEDIATE statement 的最小资源边界，使 close 失败仍可按已开始事务处理。 */
private[sqlite] trait ImmediateControlStatement:
  /** 执行 SQLite 的 `BEGIN IMMEDIATE`。 */
  def executeBeginImmediate(): Unit

  /** 释放执行 BEGIN 的 statement。 */
  def close(): Unit

/** 生产 JDBC statement 适配器；测试可在同包注入显式失败语义而不使用反射或动态代理。 */
private[sqlite] final class JdbcImmediateControlStatement(connection: Connection) extends ImmediateControlStatement:
  private val statement = connection.createStatement()

  override def executeBeginImmediate(): Unit = statement.execute("BEGIN IMMEDIATE")

  override def close(): Unit = statement.close()

/**
 * 统一处理 transaction 失败：rollback 失败时关闭连接且绝不恢复 auto-commit，避免 JDBC 隐式提交。
 */
private[sqlite] object TransactionBoundary:
  /**
   * 执行一个 transaction；成功时恢复原 auto-commit，失败时保留主异常并附加恢复异常。
   *
   * @param control 当前 JDBC connection 的 transaction 控制器。
   * @param poison 关闭并使连接不可再用的回调。
   * @param fn transaction 内的业务代码。
   * @return 业务代码成功时的结果。
   */
  def execute[T](control: TransactionControl, poison: Throwable => Unit)(fn: => T): T =
    val originalAutoCommit = control.autoCommit
    if !originalAutoCommit then
      throw new IllegalStateException("Nested SQLite transactions are not supported")

    var transactionStarted = false
    var committed = false
    try
      control.setAutoCommit(false)
      transactionStarted = true
      val result = fn
      control.commit()
      committed = true
      control.setAutoCommit(originalAutoCommit)
      result
    catch
      case primaryFailure: Throwable =>
        if transactionStarted && !committed then
          val rollbackSucceeded =
            try
              control.rollback()
              true
            catch
              case rollbackFailure: Throwable =>
                primaryFailure.addSuppressed(rollbackFailure)
                false
          if rollbackSucceeded then
            try control.setAutoCommit(originalAutoCommit)
            catch
              case restoreFailure: Throwable =>
                primaryFailure.addSuppressed(restoreFailure)
                poison(primaryFailure)
          else poison(primaryFailure)
        else poison(primaryFailure)
        throw primaryFailure

  /**
   * 执行 SQLite `BEGIN IMMEDIATE` 写保留事务。
   *
   * 必须在任何容量读取之前取得写锁，避免两个独立连接基于同一旧计数同时提交。
   */
  def executeImmediate[T](
      connection: Connection,
      control: TransactionControl,
      poison: Throwable => Unit
  )(fn: => T): T =
    executeImmediate(control, poison, () => new JdbcImmediateControlStatement(connection))(fn)

  /**
   * 执行可注入的 immediate reservation；仅同包测试可替换 statement 生命周期。
   *
   * 成功执行 BEGIN 后必须先标记事务已开始，再释放 statement，保证 close 失败进入 rollback 路径。
   */
  private[sqlite] def executeImmediate[T](
      control: TransactionControl,
      poison: Throwable => Unit,
      statementFactory: () => ImmediateControlStatement
  )(fn: => T): T =
    val originalAutoCommit = control.autoCommit
    if !originalAutoCommit then
      throw new IllegalStateException("Nested SQLite transactions are not supported")

    var transactionStarted = false
    var committed = false
    var reservation: ImmediateControlStatement = null
    try
      reservation = statementFactory()
      reservation.executeBeginImmediate()
      transactionStarted = true
      reservation.close()
      reservation = null
      val result = fn
      executeControlSql(control, "COMMIT")
      committed = true
      result
    catch
      case primaryFailure: Throwable =>
        if reservation != null then
          try reservation.close()
          catch
            case closeFailure: Throwable if closeFailure ne primaryFailure => primaryFailure.addSuppressed(closeFailure)
        if transactionStarted && !committed then
          val rollbackSucceeded =
            try
              executeControlSql(control, "ROLLBACK")
              true
            catch
              case rollbackFailure: Throwable =>
                primaryFailure.addSuppressed(rollbackFailure)
                false
          if !rollbackSucceeded then poison(primaryFailure)
        else if transactionStarted then poison(primaryFailure)
        throw primaryFailure

  private def executeControlSql(control: TransactionControl, sql: String): Unit = control match
    case jdbc: JdbcTransactionControl =>
      val connection = jdbc.connection
      val statement = connection.createStatement()
      JdbcResource.withResource(statement)(statement.execute(sql))
    case _ if sql == "ROLLBACK" => control.rollback()
    case _ if sql == "COMMIT" => control.commit()
    case _ => throw new IllegalStateException(s"Unsupported immediate transaction command: $sql")
