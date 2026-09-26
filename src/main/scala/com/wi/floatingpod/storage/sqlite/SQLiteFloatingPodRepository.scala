package com.wi.floatingpod.storage.sqlite

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.storage.{ActivityTransitionStore, AdmissionOutcome, AdmissionRejection, DeactivationOutcome, DeactivationRejection, FloatingPodRepository, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorReason, PhysicalRecoveryLocatorState, PhysicalRecoveryStore, PlayerEntitlementStore, RecoveryDimensionBackfillOutcome, RecoveryDimensionBackfillRejection, RecoveryPodStore, StoredPodRow}
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
  private val resources: Resources = initializeResources()
  private val connection: Connection = resources.connection
  private val transactionControl: TransactionControl = resources.transactionControl
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

  /**
   * 写入一条新状态；重复 UUID 明确失败，避免恢复记录被隐式覆盖。
   */
  override def insert(state: FloatingPodState): Try[FloatingPodState] =
    guarded("insert", Option(state).map(_.uuid), InsertSql) {
      Objects.requireNonNull(state, "state must not be null")
      if state.isActive then
        throw new IllegalArgumentException("Generic insert only accepts inactive FloatingPodState")
      val statement = connection.prepareStatement(InsertSql)
      try
        bindInsert(statement, state)
        if (statement.executeUpdate() != 1) then
          throw new IllegalStateException(s"Insert did not affect exactly one row for UUID ${state.uuid}")
        state
      catch
        case exception: SQLException if isUniqueConstraint(exception) =>
          throw new IllegalStateException(s"FloatingPodState already exists for UUID ${state.uuid}", exception)
      finally statement.close()
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
      val nextVersion = Math.addExact(state.stateVersion, 1L)
      val updatedAt = System.currentTimeMillis()
      val statement = connection.prepareStatement(UpdateSql)
      try
        bindUpdate(statement, state, updatedAt)
        if (statement.executeUpdate() != 1) then
          throw new OptimisticLockException(state.uuid, state.stateVersion)
        state.copy(
          stateVersion = nextVersion,
          lastUpdated = updatedAt,
          isActive = current.isActive,
          dimensionId = current.dimensionId
        )
      finally statement.close()
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
            requireExactPreparedLocator(target.uuid, attemptUuid, expectedStateVersion, dimensionId)
            val persisted = target.copy(
              uuid = stored.uuid,
              ownerUuid = stored.ownerUuid,
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = true,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = Some(dimensionId)
            )
            val statement = connection.prepareStatement(ActivateSql)
            try
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Activation changed unexpectedly for UUID ${target.uuid}")
            finally statement.close()
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
            try
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Tracked deactivation changed unexpectedly for UUID $podUuid")
            finally statement.close()
            deactivateActiveLocator(podUuid, attemptUuid, expectedStateVersion, dimensionId, persisted.stateVersion)
            DeactivationOutcome.Accepted(persisted)
      }
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
            try
              statement.setInt(1, authoritativeDimensionId)
              statement.setBytes(2, uuidToBytes(uuid))
              statement.setLong(3, expectedStateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Recovery dimension changed unexpectedly for UUID $uuid")
            finally statement.close()
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
            val persisted = target.copy(
              uuid = stored.uuid,
              ownerUuid = stored.ownerUuid,
              stateVersion = Math.addExact(stored.stateVersion, 1L),
              isActive = true,
              lastUpdated = System.currentTimeMillis(),
              dimensionId = Some(dimensionId)
            )
            val statement = connection.prepareStatement(ActivateSql)
            try
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Activation changed unexpectedly for UUID ${target.uuid}")
            finally statement.close()
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
            try
              bindTransition(statement, persisted, stored.stateVersion)
              if statement.executeUpdate() != 1 then
                throw new IllegalStateException(s"Deactivation changed unexpectedly for UUID $podUuid")
            finally statement.close()
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
          try
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
          finally statement.close()
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
    try
      statement.execute("PRAGMA foreign_keys = ON")
      val foreignKeys = statement.executeQuery("PRAGMA foreign_keys")
      try
        if !foreignKeys.next() || foreignKeys.getInt(1) != 1 then
          throw new IllegalStateException("SQLite foreign key enforcement is unavailable")
      finally foreignKeys.close()

      val journalMode = statement.executeQuery("PRAGMA journal_mode = WAL")
      try
        if !journalMode.next() then
          throw new IllegalStateException("SQLite journal mode query returned no row")
        val effectiveMode = journalMode.getString(1)
        if "wal".equalsIgnoreCase(effectiveMode) then
          logger.info("SQLite journal mode is WAL")
        else
          logger.info("SQLite WAL is unsupported for this connection; effective journal mode={}", effectiveMode)
      finally journalMode.close()
    finally statement.close()

  private def selectSingle(
      sql: String,
      bind: PreparedStatement => Unit
  ): Option[FloatingPodState] =
    val statement = connection.prepareStatement(sql)
    try
      bind(statement)
      val resultSet = statement.executeQuery()
      try
        if resultSet.next() then Some(readState(resultSet)) else None
      finally resultSet.close()
    finally statement.close()

  private def selectMany(
      sql: String,
      bind: PreparedStatement => Unit
  ): List[FloatingPodState] =
    val statement = connection.prepareStatement(sql)
    try
      bind(statement)
      val resultSet = statement.executeQuery()
      try
        val states = List.newBuilder[FloatingPodState]
        while resultSet.next() do states += readState(resultSet)
        states.result()
      finally resultSet.close()
    finally statement.close()

  private def selectStoredRows(
      sql: String,
      bind: PreparedStatement => Unit
  ): List[StoredPodRow] =
    val statement = connection.prepareStatement(sql)
    try
      bind(statement)
      val resultSet = statement.executeQuery()
      try
        val rows = List.newBuilder[StoredPodRow]
        while resultSet.next() do rows += readStoredRow(resultSet)
        rows.result()
      finally resultSet.close()
    finally statement.close()

  private def selectStoredRow(uuid: UUID): Option[StoredPodRow] =
    val statement = connection.prepareStatement(FindByUuidSql)
    try
      statement.setBytes(1, uuidToBytes(uuid))
      val resultSet = statement.executeQuery()
      try if resultSet.next() then Some(readStoredRow(resultSet)) else None
      finally resultSet.close()
    finally statement.close()

  private def selectPhysicalRecoveryLocator(podUuid: UUID): Option[PhysicalRecoveryLocator] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryLocatorSql)
    try
      statement.setBytes(1, uuidToBytes(podUuid))
      val resultSet = statement.executeQuery()
      try
        if resultSet.next() then
          val attemptUuid = bytesToUuid(resultSet.getBytes("attempt_uuid"), "attempt_uuid")
          val state = PhysicalRecoveryLocatorState.fromDatabase(resultSet.getString("locator_state"))
          val domainVersion = resultSet.getLong("domain_version")
          val version = if resultSet.wasNull() then None else Some(domainVersion)
          val dimension = resultSet.getInt("target_dimension_id")
          val targetDimension = if resultSet.wasNull() then None else Some(dimension)
          Some(PhysicalRecoveryLocator(podUuid, attemptUuid, state, resultSet.getString("reason_code"), version, targetDimension, selectPhysicalRecoveryChunks(podUuid)))
        else None
      finally resultSet.close()
    finally statement.close()

  private def selectAllPhysicalRecoveryLocators(): List[PhysicalRecoveryLocator] =
    val statement = connection.prepareStatement(FindAllPhysicalRecoveryLocatorPodUuidsSql)
    try
      val resultSet = statement.executeQuery()
      val podUuids = try
        val values = List.newBuilder[UUID]
        while resultSet.next() do values += bytesToUuid(resultSet.getBytes("pod_uuid"), "pod_uuid")
        values.result()
      finally resultSet.close()
      podUuids.map { podUuid =>
        selectPhysicalRecoveryLocator(podUuid).getOrElse(throw new IllegalStateException(s"Physical recovery locator disappeared during global read for pod $podUuid"))
      }.sortBy(_.podUuid.toString)
    finally statement.close()

  private def selectPhysicalRecoveryBinding(): Option[PhysicalRecoveryBinding] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryBindingSql)
    try
      val resultSet = statement.executeQuery()
      try
        if resultSet.next() then
          Some(PhysicalRecoveryBinding(
            bytesToUuid(resultSet.getBytes("binding_uuid"), "binding_uuid"),
            PhysicalRecoveryCutoverState.fromDatabase(resultSet.getString("cutover_state")),
            resultSet.getInt("format_version")
          ))
        else None
      finally resultSet.close()
    finally statement.close()

  private def insertPhysicalRecoveryBinding(binding: PhysicalRecoveryBinding): Unit =
    val statement = connection.prepareStatement(InsertPhysicalRecoveryBindingSql)
    try
      statement.setBytes(1, uuidToBytes(binding.bindingUuid))
      statement.setString(2, binding.cutoverState.databaseValue)
      statement.setInt(3, binding.formatVersion)
      if statement.executeUpdate() != 1 then throw new IllegalStateException("Physical recovery binding insert did not affect the singleton row")
    finally statement.close()

  private def certifyPhysicalRecoveryBinding(binding: PhysicalRecoveryBinding): Unit =
    val statement = connection.prepareStatement(CertifyPhysicalRecoveryBindingSql)
    try
      statement.setString(1, binding.cutoverState.databaseValue)
      statement.setBytes(2, uuidToBytes(binding.bindingUuid))
      statement.setInt(3, binding.formatVersion)
      if statement.executeUpdate() != 1 then throw new IllegalStateException("Physical recovery binding changed unexpectedly during certification")
    finally statement.close()

  private def selectPhysicalRecoveryChunks(podUuid: UUID): List[PhysicalRecoveryChunk] =
    val statement = connection.prepareStatement(FindPhysicalRecoveryChunksSql)
    try
      statement.setBytes(1, uuidToBytes(podUuid))
      val resultSet = statement.executeQuery()
      try
        val chunks = List.newBuilder[PhysicalRecoveryChunk]
        while resultSet.next() do chunks += PhysicalRecoveryChunk(resultSet.getInt("dimension_id"), resultSet.getInt("chunk_x"), resultSet.getInt("chunk_z"))
        chunks.result()
      finally resultSet.close()
    finally statement.close()

  private def insertPhysicalRecoveryLocator(podUuid: UUID, attemptUuid: UUID, domainVersion: Long, targetDimensionId: Int): Unit =
    val statement = connection.prepareStatement(InsertPhysicalRecoveryLocatorSql)
    try
      statement.setBytes(1, uuidToBytes(podUuid))
      statement.setBytes(2, uuidToBytes(attemptUuid))
      statement.setString(3, PhysicalRecoveryLocatorState.CreatePrepared.databaseValue)
      statement.setString(4, PhysicalRecoveryLocatorReason.CreatePrepared)
      statement.setLong(5, domainVersion)
      statement.setInt(6, targetDimensionId)
      if statement.executeUpdate() != 1 then throw new IllegalStateException(s"Physical recovery locator insert did not affect pod $podUuid")
    finally statement.close()

  private def replacePhysicalRecoveryChunks(podUuid: UUID, chunks: List[PhysicalRecoveryChunk]): Unit =
    val delete = connection.prepareStatement(DeletePhysicalRecoveryChunksSql)
    try
      delete.setBytes(1, uuidToBytes(podUuid))
      delete.executeUpdate()
    finally delete.close()
    val insert = connection.prepareStatement(InsertPhysicalRecoveryChunkSql)
    try
      chunks.foreach { chunk =>
        insert.setBytes(1, uuidToBytes(podUuid))
        insert.setInt(2, chunk.dimensionId)
        insert.setInt(3, chunk.chunkX)
        insert.setInt(4, chunk.chunkZ)
        if insert.executeUpdate() != 1 then throw new IllegalStateException(s"Physical recovery chunk insert did not affect pod $podUuid")
      }
    finally insert.close()

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
    try
      statement.setString(1, PhysicalRecoveryLocatorState.RowActive.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.RowActive)
      statement.setLong(3, committedDomainVersion)
      statement.setBytes(4, uuidToBytes(podUuid))
      statement.setBytes(5, uuidToBytes(attemptUuid))
      statement.setLong(6, expectedDomainVersion)
      statement.setInt(7, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during activation for pod $podUuid")
    finally statement.close()

  private def deactivateActiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int, committedDomainVersion: Long): Unit =
    val statement = connection.prepareStatement(DeactivateActiveLocatorSql)
    try
      statement.setString(1, PhysicalRecoveryLocatorState.RowInactivePending.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.RowInactivePending)
      statement.setLong(3, committedDomainVersion)
      statement.setBytes(4, uuidToBytes(podUuid))
      statement.setBytes(5, uuidToBytes(attemptUuid))
      statement.setLong(6, expectedDomainVersion)
      statement.setInt(7, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during deactivation for pod $podUuid")
    finally statement.close()

  private def verifyInactiveLocator(podUuid: UUID, attemptUuid: UUID, expectedDomainVersion: Long, targetDimensionId: Int): Unit =
    val statement = connection.prepareStatement(VerifyInactiveAbsenceLocatorSql)
    try
      statement.setString(1, PhysicalRecoveryLocatorState.RowInactiveVerified.databaseValue)
      statement.setString(2, PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified)
      statement.setBytes(3, uuidToBytes(podUuid))
      statement.setBytes(4, uuidToBytes(attemptUuid))
      statement.setLong(5, expectedDomainVersion)
      statement.setInt(6, targetDimensionId)
      if statement.executeUpdate() != 1 then
        throw new IllegalStateException(s"Physical recovery locator changed unexpectedly during inactive absence verification for pod $podUuid")
    finally statement.close()

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
    try
      statement.setBytes(1, uuidToBytes(playerUuid))
      val resultSet = statement.executeQuery()
      try
        if resultSet.next() then
          Some(PlayerEntitlement(playerUuid, resultSet.getInt("registration_limit"), resultSet.getInt("simultaneous_active_limit")))
        else None
      finally resultSet.close()
    finally statement.close()

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
      dimensionId = dimensionId
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
      stored.dimensionId
    )

  private def entitlementFor(ownerUuid: UUID): PlayerEntitlement =
    val insert = connection.prepareStatement(EnsureEntitlementSql)
    try
      insert.setBytes(1, uuidToBytes(ownerUuid))
      insert.executeUpdate()
    finally insert.close()

    val select = connection.prepareStatement(FindEntitlementSql)
    try
      select.setBytes(1, uuidToBytes(ownerUuid))
      val resultSet = select.executeQuery()
      try
        if !resultSet.next() then
          throw new IllegalStateException(s"Missing entitlement after initialization for player $ownerUuid")
        PlayerEntitlement(ownerUuid, resultSet.getInt("registration_limit"), resultSet.getInt("simultaneous_active_limit"))
      finally resultSet.close()
    finally select.close()

  private def activeCountForOwner(ownerUuid: UUID): Int = count(ActiveCountForOwnerSql, ownerUuid)

  private def activeCount(): Int =
    val statement = connection.createStatement()
    try
      val resultSet = statement.executeQuery(ActiveCountSql)
      try
        if !resultSet.next() then throw new IllegalStateException("Active count query returned no row")
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

  private def count(sql: String, ownerUuid: UUID): Int =
    val statement = connection.prepareStatement(sql)
    try
      statement.setBytes(1, uuidToBytes(ownerUuid))
      val resultSet = statement.executeQuery()
      try
        if !resultSet.next() then throw new IllegalStateException("Count query returned no row")
        resultSet.getInt(1)
      finally resultSet.close()
    finally statement.close()

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
    "INSERT INTO floating_pod_state (uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  private[sqlite] val UpdateSql =
    "UPDATE floating_pod_state SET owner_uuid = ?, state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, last_updated = ? WHERE uuid = ? AND state_version = ?"
  private[sqlite] val FindByUuidSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id FROM floating_pod_state WHERE uuid = ?"
  private[sqlite] val FindByOwnerSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id FROM floating_pod_state WHERE owner_uuid = ? ORDER BY uuid"
  private[sqlite] val FindAllActiveSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id FROM floating_pod_state WHERE is_active = 1 ORDER BY uuid"
  private[sqlite] val FindAllSql =
    "SELECT uuid, owner_uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id FROM floating_pod_state ORDER BY uuid"
  private[sqlite] val DeleteSql = "DELETE FROM floating_pod_state WHERE uuid = ? AND is_active = 0 AND NOT EXISTS (SELECT 1 FROM physical_recovery_locator WHERE pod_uuid = ?)"
  private[sqlite] val ActivateSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, is_active = ?, last_updated = ?, dimension_id = ? WHERE uuid = ? AND state_version = ? AND is_active = 0"
  private[sqlite] val DeactivateSql =
    "UPDATE floating_pod_state SET state_version = state_version + 1, world_name = ?, pos_x = ?, pos_y = ?, pos_z = ?, yaw = ?, pitch = ?, is_active = ?, last_updated = ?, dimension_id = ? WHERE uuid = ? AND state_version = ? AND is_active = 1"
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
  private[sqlite] val InsertPhysicalRecoveryLocatorSql =
    "INSERT INTO physical_recovery_locator (pod_uuid, attempt_uuid, locator_state, reason_code, domain_version, target_dimension_id) VALUES (?, ?, ?, ?, ?, ?)"
  private[sqlite] val ActivatePreparedLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ?, domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'CREATE_PREPARED' AND reason_code = 'create_prepared' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val DeactivateActiveLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ?, domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'ROW_ACTIVE' AND reason_code = 'row_active' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val VerifyInactiveAbsenceLocatorSql =
    "UPDATE physical_recovery_locator SET locator_state = ?, reason_code = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'ROW_INACTIVE_PENDING' AND reason_code = 'row_inactive_pending' AND domain_version = ? AND target_dimension_id = ?"
  private[sqlite] val DeletePhysicalRecoveryChunksSql = "DELETE FROM physical_recovery_locator_chunk WHERE pod_uuid = ?"
  private[sqlite] val InsertPhysicalRecoveryChunkSql =
    "INSERT INTO physical_recovery_locator_chunk (pod_uuid, dimension_id, chunk_x, chunk_z) VALUES (?, ?, ?, ?)"

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
      try statement.execute(sql)
      finally statement.close()
    case _ if sql == "ROLLBACK" => control.rollback()
    case _ if sql == "COMMIT" => control.commit()
    case _ => throw new IllegalStateException(s"Unsupported immediate transaction command: $sql")
