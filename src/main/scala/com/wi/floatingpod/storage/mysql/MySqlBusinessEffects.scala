package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.util.{IdentityHashMap, UUID}

/** 本 slice 仅允许非活动 pod 与玩家额度表的确定性写操作。 */
sealed trait MySqlBusinessMutation:
  private[mysql] def targetIdentity: String
  private[mysql] def expectedVersion: Long
  private[mysql] def expectedEffect: MySqlExpectedEffect
  private[mysql] def expectedPod: Option[MySqlStoredRowData]
  private[mysql] def desiredPod: Option[MySqlStoredRowData]
  private[mysql] def expectedEntitlement: Option[PlayerEntitlement]
  private[mysql] def desiredEntitlement: Option[PlayerEntitlement]
  private[mysql] def expectedRepositorySnapshot: Option[MySqlFullRepositorySnapshot] = None
  private[mysql] def desiredRepositorySnapshot: Option[MySqlFullRepositorySnapshot] = None

object MySqlBusinessMutation:
  final case class InsertInactivePod(desired: MySqlStoredRowData) extends MySqlBusinessMutation:
    validatePod(desired, "desired")
    require(desired.isActiveFlag == 0, "this MySQL transaction slice only inserts inactive pods")
    require(desired.stateVersion == 0L, "a newly inserted inactive pod must start at version zero")
    private[mysql] val targetIdentity: String = "pod/" + MySqlStoredRows.decodeUuid(desired.uuidBytes, "uuid").toString
    private[mysql] val expectedVersion: Long = 0L
    private[mysql] val expectedEffect: MySqlExpectedEffect = effect(targetIdentity, expectedVersion, desired)
    private[mysql] val expectedPod: Option[MySqlStoredRowData] = None
    private[mysql] val desiredPod: Option[MySqlStoredRowData] = Some(desired)
    private[mysql] val expectedEntitlement: Option[PlayerEntitlement] = None
    private[mysql] val desiredEntitlement: Option[PlayerEntitlement] = None

  final case class ReplaceInactivePod(expected: MySqlStoredRowData, desired: MySqlStoredRowData) extends MySqlBusinessMutation:
    validatePod(expected, "expected")
    validatePod(desired, "desired")
    require(expected.isActiveFlag == 0 && desired.isActiveFlag == 0, "this MySQL transaction slice only replaces inactive pods")
    require(expected.uuidBytes == desired.uuidBytes, "a pod replacement must retain its UUID")
    require(desired.stateVersion == Math.addExact(expected.stateVersion, 1L), "a pod replacement must increment state_version exactly once")
    private[mysql] val targetIdentity: String = "pod/" + MySqlStoredRows.decodeUuid(expected.uuidBytes, "uuid").toString
    private[mysql] val expectedVersion: Long = expected.stateVersion
    private[mysql] val expectedEffect: MySqlExpectedEffect = effect(targetIdentity, expectedVersion, desired)
    private[mysql] val expectedPod: Option[MySqlStoredRowData] = Some(expected)
    private[mysql] val desiredPod: Option[MySqlStoredRowData] = Some(desired)
    private[mysql] val expectedEntitlement: Option[PlayerEntitlement] = None
    private[mysql] val desiredEntitlement: Option[PlayerEntitlement] = None

  final case class DeleteInactivePod(expected: MySqlStoredRowData) extends MySqlBusinessMutation:
    validatePod(expected, "expected")
    require(expected.isActiveFlag == 0, "this MySQL transaction slice only deletes inactive pods")
    private[mysql] val targetIdentity: String = "pod/" + MySqlStoredRows.decodeUuid(expected.uuidBytes, "uuid").toString
    private[mysql] val expectedVersion: Long = expected.stateVersion
    private[mysql] val expectedEffect: MySqlExpectedEffect =
      MySqlExpectedEffect(targetIdentity, expectedVersion, MySqlBusinessEffectFingerprint.absentPod(expected.uuidBytes))
    private[mysql] val expectedPod: Option[MySqlStoredRowData] = Some(expected)
    private[mysql] val desiredPod: Option[MySqlStoredRowData] = None
    private[mysql] val expectedEntitlement: Option[PlayerEntitlement] = None
    private[mysql] val desiredEntitlement: Option[PlayerEntitlement] = None

  final case class ChangeEntitlement(expected: Option[PlayerEntitlement], desired: Option[PlayerEntitlement]) extends MySqlBusinessMutation:
    require(expected != null && desired != null, "entitlement option values are required")
    require(expected.forall(_ != null) && desired.forall(_ != null), "entitlement rows must not contain null")
    require(expected.isDefined || desired.isDefined, "an entitlement mutation must identify a player")
    require(expected != desired, "an entitlement mutation must change the stored row")
    require(expected.zip(desired).forall((before, after) => before.playerUuid == after.playerUuid), "an entitlement update must retain the player UUID")
    private val playerUuid = expected.orElse(desired).get.playerUuid
    private[mysql] val targetIdentity: String = "entitlement/" + playerUuid.toString
    private[mysql] val expectedVersion: Long = 0L
    private[mysql] val expectedEffect: MySqlExpectedEffect =
      MySqlExpectedEffect(targetIdentity, expectedVersion, MySqlBusinessEffectFingerprint.entitlement(desired))
    private[mysql] val expectedPod: Option[MySqlStoredRowData] = None
    private[mysql] val desiredPod: Option[MySqlStoredRowData] = None
    private[mysql] val expectedEntitlement: Option[PlayerEntitlement] = expected
    private[mysql] val desiredEntitlement: Option[PlayerEntitlement] = desired

  /** 一个事务的责任范围是完整业务/恢复/B1 图，不能只把改动行作为未知结果读回。 */
  final case class ReplaceFullRepositorySnapshot(
      expected: MySqlFullRepositorySnapshot,
      desired: MySqlFullRepositorySnapshot,
      transitions: Vector[MySqlRepositoryStagedStep] = Vector.empty
  ) extends MySqlBusinessMutation:
    if transitions == null || transitions.exists(_ == null) then
      throw new IllegalArgumentException("full snapshot transition path must not contain null")
    if transitions.isEmpty then MySqlFullRepositoryDelta.freeze(expected, desired)
    else MySqlRepositoryStagedPlan(expected, desired, transitions).validate()
    private[mysql] val targetIdentity: String = "repository/full-snapshot-v1"
    private[mysql] val expectedVersion: Long = 0L
    private[mysql] val expectedEffect: MySqlExpectedEffect =
      MySqlExpectedEffect(targetIdentity, expectedVersion, MySqlBusinessEffectFingerprint.fullSnapshot(expected, desired))
    private[mysql] val expectedPod: Option[MySqlStoredRowData] = None
    private[mysql] val desiredPod: Option[MySqlStoredRowData] = None
    private[mysql] val expectedEntitlement: Option[PlayerEntitlement] = None
    private[mysql] val desiredEntitlement: Option[PlayerEntitlement] = None
    override private[mysql] val expectedRepositorySnapshot: Option[MySqlFullRepositorySnapshot] = Some(expected)
    override private[mysql] val desiredRepositorySnapshot: Option[MySqlFullRepositorySnapshot] = Some(desired)

  private[mysql] def podUuid(mutation: MySqlBusinessMutation): UUID =
    val row = mutation.expectedPod.orElse(mutation.desiredPod).getOrElse(
      throw new IllegalArgumentException("MySQL effect does not target a pod")
    )
    MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid")

  private[mysql] def entitlementUuid(mutation: MySqlBusinessMutation): UUID =
    val row = mutation.expectedEntitlement.orElse(mutation.desiredEntitlement).getOrElse(
      throw new IllegalArgumentException("MySQL effect does not target an entitlement")
    )
    row.playerUuid

  private[mysql] def samePodOption(left: Option[MySqlStoredRowData], right: Option[MySqlStoredRowData]): Boolean =
    left != null && right != null && left.isDefined == right.isDefined &&
      left.zip(right).forall((a, b) => MySqlBusinessRowEquality.pod(a, b))

  private def validatePod(row: MySqlStoredRowData, label: String): Unit =
    require(row != null, label + " pod row is required")
    require(row.uuidBytes.size == 16, label + " pod UUID must contain 16 bytes")
    require(row.ownerUuidBytes.forall(_.size == 16), label + " owner UUID must contain 16 bytes")
    MySqlStoredRows.materializeRawRow(row)

  private def effect(target: String, version: Long, row: MySqlStoredRowData): MySqlExpectedEffect =
    MySqlExpectedEffect(target, version, MySqlBusinessEffectFingerprint.pod(row))

/** 一个 operation 的完整、不可变、受限写集合；不接受任意 SQL 或 Boolean 成功回呼。 */
final class MySqlBusinessEffectPlan private (val mutations: Vector[MySqlBusinessMutation]):
  require(mutations != null && mutations.nonEmpty, "MySQL business effect plan must not be empty")
  require(mutations.forall(_ != null), "MySQL business effect plan must not contain null")
  require(mutations.map(_.targetIdentity).distinct.size == mutations.size, "MySQL business effect plan must have unique targets")

  val expectedEffects: Vector[MySqlExpectedEffect] =
    mutations.map(_.expectedEffect).sortBy(_.targetIdentity)

  /** 完整图是 staged 路径的效果见证，不允许交给只会写 primitive 行的入口。 */
  private[mysql] def requirePrimitiveExecution(): Unit =
    if mutations.exists(_.isInstanceOf[MySqlBusinessMutation.ReplaceFullRepositorySnapshot]) then
      throw new IllegalArgumentException("full repository snapshot plans require executeStaged")

  def operationContext(bootUuid: UUID, writerEpoch: Long, bindingUuid: UUID, operationUuid: UUID): MySqlOperationContext =
    MySqlOperationContext(bootUuid, writerEpoch, bindingUuid, operationUuid, expectedEffects)

object MySqlBusinessEffectPlan:
  def apply(mutations: Iterable[MySqlBusinessMutation]): MySqlBusinessEffectPlan =
    require(mutations != null, "MySQL business mutations are required")
    new MySqlBusinessEffectPlan(mutations.iterator.toVector.sortBy(_.targetIdentity))

/** 对期望效果使用稳定的二进制编码；调用方提供的 last_updated 原样进入摘要。 */
private[mysql] object MySqlBusinessEffectFingerprint:
  private val Domain = "floatingpod.mysql.business-effect/inactive-pod-entitlement/v1"

  def fullSnapshot(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): Vector[Byte] =
    require(before != null && after != null, "full repository snapshots are required")
    val outputBytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(outputBytes)
    try
      writeText(output, "floatingpod.mysql.business-effect/full-repository-snapshot/v1")
      writeBytes(output, before.canonicalBytes.toArray)
      writeBytes(output, after.canonicalBytes.toArray)
      output.flush()
      MessageDigest.getInstance("SHA-256").digest(outputBytes.toByteArray).toVector
    finally output.close()

  def pod(row: MySqlStoredRowData): Vector[Byte] =
    require(row != null, "pod row is required")
    digest { output =>
      writeText(output, Domain)
      output.writeByte(1)
      writePod(output, row)
    }

  def absentPod(uuidBytes: Vector[Byte]): Vector[Byte] =
    require(uuidBytes != null && uuidBytes.size == 16, "pod UUID bytes are required")
    digest { output =>
      writeText(output, Domain)
      output.writeByte(2)
      writeBytes(output, uuidBytes.toArray)
    }

  def entitlement(row: Option[PlayerEntitlement]): Vector[Byte] =
    require(row != null && row.forall(_ != null), "entitlement option is required")
    digest { output =>
      writeText(output, Domain)
      row match
        case None => output.writeByte(4)
        case Some(value) =>
          output.writeByte(3)
          writeUuid(output, value.playerUuid)
          output.writeInt(value.registrationLimit)
          output.writeInt(value.simultaneousActiveLimit)
    }

  private def digest(write: DataOutputStream => Unit): Vector[Byte] =
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    try
      write(output)
      output.flush()
      MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray).toVector
    finally output.close()

  private def writePod(output: DataOutputStream, row: MySqlStoredRowData): Unit =
    writeBytes(output, row.uuidBytes.toArray)
    writeOptionalBytes(output, row.ownerUuidBytes)
    output.writeLong(row.stateVersion)
    writeText(output, row.worldName)
    output.writeLong(java.lang.Double.doubleToRawLongBits(row.posX))
    output.writeLong(java.lang.Double.doubleToRawLongBits(row.posY))
    output.writeLong(java.lang.Double.doubleToRawLongBits(row.posZ))
    output.writeLong(java.lang.Double.doubleToRawLongBits(row.yaw))
    output.writeLong(java.lang.Double.doubleToRawLongBits(row.pitch))
    output.writeInt(row.isActiveFlag)
    output.writeLong(row.lastUpdated)
    row.dimensionId match
      case None =>
        output.writeByte(0)
      case Some(value) =>
        output.writeByte(1)
        output.writeInt(value)
    writeText(output, row.operatingModeToken)

  private def writeOptionalBytes(output: DataOutputStream, value: Option[Vector[Byte]]): Unit =
    value match
      case None => output.writeByte(0)
      case Some(bytes) =>
        output.writeByte(1)
        writeBytes(output, bytes.toArray)

  private def writeUuid(output: DataOutputStream, uuid: UUID): Unit =
    output.writeLong(uuid.getMostSignificantBits)
    output.writeLong(uuid.getLeastSignificantBits)

  private def writeText(output: DataOutputStream, value: String): Unit =
    require(value != null, "fingerprinted text is required")
    val encoder = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val encoded = encoder.encode(CharBuffer.wrap(value))
    val bytes = new Array[Byte](encoded.remaining())
    encoded.get(bytes)
    writeBytes(output, bytes)

  private def writeBytes(output: DataOutputStream, bytes: Array[Byte]): Unit =
    output.writeInt(bytes.length)
    output.write(bytes)

/** 精确比较 MySQL raw 行，包括 DOUBLE 的 IEEE 位模式。 */
private[mysql] object MySqlBusinessRowEquality:
  def pod(left: MySqlStoredRowData, right: MySqlStoredRowData): Boolean =
    left != null && right != null &&
      left.uuidBytes == right.uuidBytes &&
      left.ownerUuidBytes == right.ownerUuidBytes &&
      left.stateVersion == right.stateVersion &&
      left.worldName == right.worldName &&
      java.lang.Double.doubleToRawLongBits(left.posX) == java.lang.Double.doubleToRawLongBits(right.posX) &&
      java.lang.Double.doubleToRawLongBits(left.posY) == java.lang.Double.doubleToRawLongBits(right.posY) &&
      java.lang.Double.doubleToRawLongBits(left.posZ) == java.lang.Double.doubleToRawLongBits(right.posZ) &&
      java.lang.Double.doubleToRawLongBits(left.yaw) == java.lang.Double.doubleToRawLongBits(right.yaw) &&
      java.lang.Double.doubleToRawLongBits(left.pitch) == java.lang.Double.doubleToRawLongBits(right.pitch) &&
      left.isActiveFlag == right.isActiveFlag &&
      left.lastUpdated == right.lastUpdated &&
      left.dimensionId == right.dimensionId &&
      left.operatingModeToken == right.operatingModeToken

private[mysql] final class MySqlRollbackOnlyState:
  private var firstFailure: Throwable = null

  def mark(failure: Throwable): Unit = synchronized {
    require(failure != null, "rollback-only failure must not be null")
    if firstFailure == null then firstFailure = failure
    else if (firstFailure ne failure) && !firstFailure.getSuppressed.contains(failure) then
      firstFailure.addSuppressed(failure)
  }

  def cause: Option[Throwable] = synchronized(Option(firstFailure))

/** 按 JDBC Connection 对象身份拒绝同一连接上的公共嵌套事务，并发行仅在当前 gate 生命周期内有效的 owner。 */
private[mysql] object MySqlConnectionTransactionGate:
  private final class ActiveTransaction(val rollbackOnly: MySqlRollbackOnlyState, val ownerThread: Thread,
      val owner: Owner)

  final class Owner private[MySqlConnectionTransactionGate] (
      private[MySqlConnectionTransactionGate] val connectionIdentity: AnyRef,
      private[MySqlConnectionTransactionGate] val rollbackOnly: MySqlRollbackOnlyState,
      private[MySqlConnectionTransactionGate] val ownerThread: Thread
  )

  private val active = new IdentityHashMap[AnyRef, ActiveTransaction]()

  def enter(connectionIdentity: AnyRef, rollbackOnly: MySqlRollbackOnlyState): Option[IllegalStateException] = synchronized {
    require(connectionIdentity != null, "transaction connection identity is required")
    require(rollbackOnly != null, "transaction rollback-only state is required")
    val existing = active.get(connectionIdentity)
    if existing != null then
      val failure = new IllegalStateException("nested MySQL transaction on the same connection is rejected before SQL")
      existing.rollbackOnly.mark(failure)
      Some(failure)
    else
      val ownerThread = Thread.currentThread()
      val owner = new Owner(connectionIdentity, rollbackOnly, ownerThread)
      active.put(connectionIdentity, new ActiveTransaction(rollbackOnly, ownerThread, owner))
      None
  }

  def currentOwner(connectionIdentity: AnyRef, rollbackOnly: MySqlRollbackOnlyState): Owner = synchronized {
    require(connectionIdentity != null, "transaction connection identity is required")
    require(rollbackOnly != null, "transaction rollback-only state is required")
    val current = active.get(connectionIdentity)
    if current == null then throw new IllegalStateException("MySQL connection transaction gate is not held")
    if current.rollbackOnly ne rollbackOnly then
      throw new IllegalStateException("MySQL connection transaction gate belongs to a different outer transaction")
    if current.ownerThread ne Thread.currentThread() then
      throw new IllegalStateException("MySQL connection transaction gate belongs to another thread")
    current.rollbackOnly.cause.foreach(throw _)
    current.owner
  }

  def requireCurrent(owner: Owner, connectionIdentity: AnyRef, rollbackOnly: MySqlRollbackOnlyState): Unit = synchronized {
    require(connectionIdentity != null, "transaction connection identity is required")
    require(rollbackOnly != null, "transaction rollback-only state is required")
    if owner == null then throw new IllegalStateException("MySQL connection transaction owner is required")
    val current = active.get(connectionIdentity)
    if (owner.connectionIdentity ne connectionIdentity) ||
      (owner.rollbackOnly ne rollbackOnly) ||
      (owner.ownerThread ne Thread.currentThread()) ||
      (current == null) || (current.owner ne owner) then
      throw new IllegalStateException("MySQL connection transaction owner is stale or belongs to another scope")
    rollbackOnly.cause.foreach(throw _)
  }

  def leave(connectionIdentity: AnyRef, rollbackOnly: MySqlRollbackOnlyState): Unit = synchronized {
    require(connectionIdentity != null, "transaction connection identity is required")
    require(rollbackOnly != null, "transaction rollback-only state is required")
    val current = active.get(connectionIdentity)
    if current == null then
      throw new IllegalStateException("MySQL connection transaction gate was not held")
    if current.rollbackOnly ne rollbackOnly then
      throw new IllegalStateException("MySQL connection transaction gate belongs to a different outer transaction")
    active.remove(connectionIdentity)
  }
