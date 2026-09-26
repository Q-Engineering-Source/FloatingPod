package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.storage.AdmissionOutcome.{Accepted as AdmissionAccepted, Rejected as AdmissionRejected}
import com.wi.floatingpod.storage.AdmissionRejection.{AlreadyActive, GlobalActiveLimitReached, NotOwner, PlayerActiveLimitReached, VersionMismatch as AdmissionVersionMismatch}
import com.wi.floatingpod.storage.DeactivationOutcome.{Accepted as DeactivationAccepted, Rejected as DeactivationRejected}
import com.wi.floatingpod.storage.DeactivationRejection.{NotActive, NotOwner as DeactivationNotOwner, VersionMismatch as DeactivationVersionMismatch}
import com.wi.floatingpod.storage.RecoveryDimensionBackfillOutcome.{Ready as RecoveryReady, Rejected as RecoveryRejected}
import com.wi.floatingpod.storage.RecoveryDimensionBackfillRejection.{DimensionConflict, InactivePod, UnknownPod as RecoveryUnknownPod, VersionMismatch as RecoveryVersionMismatch}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager}
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import scala.util.Try

/** 直接验证 SQLite 权威活动转换，而非以 manager 或源码文本间接断言。 */
class ActivityTransitionStoreTest:
  /** 验证预览查询缺少权益时只返回 None，不能创建默认持久化行。 */
  @Test
  def findsMissingEntitlementWithoutPersistingDefault(): Unit = withDatabase { database =>
    val player = uuid("11111111-1111-1111-1111-111111111128")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val store: PlayerEntitlementStore = repository
      assertEquals(None, store.findByPlayerUuid(player).get)
    finally repository.close()
    assertEquals(0, entitlementCount(database, player))
  }

  /** 验证预览查询按 UUID 返回管理员明确配置的两个权益额度。 */
  @Test
  def findsExactPersistedEntitlement(): Unit = withDatabase { database =>
    val player = uuid("11111111-1111-1111-1111-111111111129")
    val initializer = new SQLiteFloatingPodRepository(database.toString)
    initializer.close()
    setEntitlement(database, player, 4, 2)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val store: PlayerEntitlementStore = repository
      assertEquals(Some(PlayerEntitlement(player, 4, 2)), store.findByPlayerUuid(player).get)
    finally repository.close()
  }

  /** 验证空玩家 UUID 在查询边界立即作为参数错误返回。 */
  @Test
  def rejectsNullPlayerUuidForEntitlementQuery(): Unit = withRepository { repository =>
    val store: PlayerEntitlementStore = repository
    val failure = store.findByPlayerUuid(null).failed.get
    assertTrue(failure.isInstanceOf[NullPointerException])
    assertEquals("playerUuid must not be null", failure.getMessage)
  }

  /** 验证成功准入提交精确的持久版本、时间戳和服务端维度。 */
  @Test
  def activatesWithExactPersistedState(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111111")
    val initial = state("00000000-0000-0000-0000-000000000301", owner)
    repository.insert(initial).get
    val started = System.currentTimeMillis()

    val outcome = repository.activateIfAdmissible(initial.copy(worldName = "nether"), owner, 0L, -1, 2).get

    outcome match
      case AdmissionAccepted(persisted) =>
        assertEquals(1L, persisted.stateVersion)
        assertTrue(persisted.lastUpdated >= started)
        assertEquals("nether", persisted.worldName)
        assertEquals(Some(-1), persisted.dimensionId)
        assertEquals(Some(persisted), repository.findByUuid(initial.uuid).get)
      case other => throw new AssertionError(s"Expected accepted activation, got $other")
  }

  /** 验证所有者、版本和已活动状态是业务拒绝且不产生数据库写入。 */
  @Test
  def rejectsOwnerVersionAndActivityMismatches(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111112")
    val other = uuid("22222222-2222-2222-2222-222222222222")
    val initial = state("00000000-0000-0000-0000-000000000302", owner)
    repository.insert(initial).get

    assertEquals(AdmissionRejected(NotOwner), repository.activateIfAdmissible(initial, other, 0L, 0, 2).get)
    assertEquals(AdmissionRejected(AdmissionVersionMismatch(0L)), repository.activateIfAdmissible(initial, owner, 1L, 0, 2).get)
    assertTrue(repository.activateIfAdmissible(initial, owner, 0L, 0, 2).get.isInstanceOf[AdmissionAccepted])
    assertEquals(AdmissionRejected(AlreadyActive), repository.activateIfAdmissible(initial, owner, 1L, 0, 2).get)
  }

  /** 验证缺少记录时懒创建规范 1/1 权益，且已有显式权益不被默认值覆盖。 */
  @Test
  def initializesMissingEntitlementWithoutOverwritingExistingValue(): Unit = withDatabase { database =>
    val owner = uuid("11111111-1111-1111-1111-111111111113")
    val first = new SQLiteFloatingPodRepository(database.toString)
    try
      val initial = state("00000000-0000-0000-0000-000000000303", owner)
      first.insert(initial).get
      assertTrue(first.activateIfAdmissible(initial, owner, 0L, 0, 2).get.isInstanceOf[AdmissionAccepted])
    finally first.close()

    assertEquals((1, 1), entitlement(database, owner))
    setEntitlement(database, owner, 3, 2)
    val second = new SQLiteFloatingPodRepository(database.toString)
    try
      val inactive = state("00000000-0000-0000-0000-000000000304", owner)
      second.insert(inactive).get
      assertTrue(second.activateIfAdmissible(inactive, owner, 0L, 0, 3).get.isInstanceOf[AdmissionAccepted])
    finally second.close()
    assertEquals((3, 2), entitlement(database, owner))
  }

  /** 验证注册额度只是持久化基础，不会阻止既有辅助机释放；活动和全服额度仍会拒绝。 */
  @Test
  def ignoresRegistrationLimitButRejectsPlayerAndGlobalCapacity(): Unit = withDatabase { database =>
    val owner = uuid("11111111-1111-1111-1111-111111111114")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val first = state("00000000-0000-0000-0000-000000000305", owner)
      val second = state("00000000-0000-0000-0000-000000000306", owner)
      val third = state("00000000-0000-0000-0000-000000000307", owner)
      repository.insert(first).get
      repository.insert(second).get
      repository.insert(third).get
    finally repository.close()

    setEntitlement(database, owner, 0, 2)
    val limited = new SQLiteFloatingPodRepository(database.toString)
    try
      val first = limited.findByUuid(uuid("00000000-0000-0000-0000-000000000305")).get.get
      val second = limited.findByUuid(uuid("00000000-0000-0000-0000-000000000306")).get.get
      val third = limited.findByUuid(uuid("00000000-0000-0000-0000-000000000307")).get.get
      assertTrue(limited.activateIfAdmissible(first, owner, 0L, 0, 3).get.isInstanceOf[AdmissionAccepted])
      assertTrue(limited.activateIfAdmissible(second, owner, 0L, 0, 3).get.isInstanceOf[AdmissionAccepted])
      assertEquals(AdmissionRejected(GlobalActiveLimitReached), limited.activateIfAdmissible(third, owner, 0L, 0, 2).get)
      assertEquals(AdmissionRejected(PlayerActiveLimitReached), limited.activateIfAdmissible(third, owner, 0L, 0, 3).get)
    finally limited.close()

    val anotherOwner = uuid("11111111-1111-1111-1111-111111111115")
    val global = new SQLiteFloatingPodRepository(database.toString)
    try
      val candidate = state("00000000-0000-0000-0000-000000000312", anotherOwner)
      global.insert(candidate).get
      assertEquals(AdmissionRejected(GlobalActiveLimitReached), global.activateIfAdmissible(candidate, anotherOwner, 0L, 0, 2).get)
    finally global.close()
  }

  /** 验证全服活动上限仅接受规范的 1 到 256 范围。 */
  @Test
  def rejectsGlobalActiveLimitOutsideNormativeRange(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111120")
    val initial = state("00000000-0000-0000-0000-000000000313", owner)
    repository.insert(initial).get
    assertTrue(repository.activateIfAdmissible(initial, owner, 0L, 0, 0).failed.get.isInstanceOf[IllegalArgumentException])
    assertTrue(repository.activateIfAdmissible(initial, owner, 0L, 0, 257).failed.get.isInstanceOf[IllegalArgumentException])
  }

  /** 验证两个独立连接争用最后一个全服槽位时恰有一个提交成功。 */
  @Test
  def serializesFinalSlotAcrossSeparateRepositories(): Unit = withDatabase { database =>
    val firstOwner = uuid("11111111-1111-1111-1111-111111111116")
    val secondOwner = uuid("11111111-1111-1111-1111-111111111117")
    val first = new SQLiteFloatingPodRepository(database.toString)
    val second = new SQLiteFloatingPodRepository(database.toString)
    try
      val firstState = state("00000000-0000-0000-0000-000000000308", firstOwner)
      val secondState = state("00000000-0000-0000-0000-000000000309", secondOwner)
      first.insert(firstState).get
      second.insert(secondState).get
      val ready = new CountDownLatch(2)
      val start = new CountDownLatch(1)
      val firstResult = new AtomicReference[Try[AdmissionOutcome]]()
      val secondResult = new AtomicReference[Try[AdmissionOutcome]]()
      val firstUncaught = new AtomicReference[Throwable]()
      val secondUncaught = new AtomicReference[Throwable]()
      val firstThread = new Thread(() => {
        ready.countDown()
        start.await()
        firstResult.set(first.activateIfAdmissible(firstState, firstOwner, 0L, 0, 1))
      })
      val secondThread = new Thread(() => {
        ready.countDown()
        start.await()
        secondResult.set(second.activateIfAdmissible(secondState, secondOwner, 0L, 0, 1))
      })
      firstThread.setUncaughtExceptionHandler((_, failure) => firstUncaught.set(failure))
      secondThread.setUncaughtExceptionHandler((_, failure) => secondUncaught.set(failure))
      firstThread.start()
      secondThread.start()
      ready.await()
      start.countDown()
      firstThread.join(5000L)
      secondThread.join(5000L)

      assertFalse(firstThread.isAlive)
      assertFalse(secondThread.isAlive)
      assertEquals(null, firstUncaught.get())
      assertEquals(null, secondUncaught.get())
      val results = List(firstResult.get(), secondResult.get())
      assertTrue(results.forall(_ != null))
      assertTrue(results.forall(_.isSuccess))
      val outcomes = results.map(_.get)
      assertEquals(1, outcomes.count(_.isInstanceOf[AdmissionAccepted]))
      assertEquals(1, outcomes.count(_ == AdmissionRejected(GlobalActiveLimitReached)))
      val verifier = new SQLiteFloatingPodRepository(database.toString)
      try assertEquals(1, verifier.findAllActive().get.size)
      finally verifier.close()
    finally
      first.close()
      second.close()
  }

  /** 验证写保留锁不可取得时先返回基础设施失败，随后同一 repository 可继续给出业务拒绝。 */
  @Test
  def keepsRepositoryReusableWhenImmediateReservationIsBusy(): Unit = withDatabase { database =>
    val owner = uuid("11111111-1111-1111-1111-111111111124")
    val wrongOwner = uuid("11111111-1111-1111-1111-111111111125")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val raw = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    val reservation = raw.createStatement()
    var lockHeld = false
    try
      val inactive = state("00000000-0000-0000-0000-000000000318", owner)
      repository.insert(inactive).get
      reservation.execute("BEGIN IMMEDIATE")
      lockHeld = true
      val result = new AtomicReference[Try[AdmissionOutcome]]()
      val worker = new Thread(() => result.set(repository.activateIfAdmissible(inactive, wrongOwner, 0L, 0, 2)))
      worker.start()
      worker.join(5000L)
      val completedWhileLocked = !worker.isAlive
      assertTrue(completedWhileLocked)
      assertTrue(result.get() != null)
      assertTrue(result.get().isFailure)
      reservation.execute("ROLLBACK")
      lockHeld = false
      worker.join(5000L)
      assertFalse(worker.isAlive)
      assertEquals(AdmissionRejected(NotOwner), repository.activateIfAdmissible(inactive, wrongOwner, 0L, 0, 2).get)
    finally
      if lockHeld then reservation.execute("ROLLBACK")
      reservation.close()
      raw.close()
      repository.close()
  }

  /** 验证写保留事务内的业务异常会回滚，并允许同一连接继续处理后续转换。 */
  @Test
  def rollsBackImmediateReservationWhenActivationCannotAdvanceVersion(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111126")
    val saturated = state("00000000-0000-0000-0000-000000000319", owner).copy(stateVersion = Long.MaxValue)
    repository.insert(saturated).get
    val failed = repository.activateIfAdmissible(saturated, owner, Long.MaxValue, 0, 2)
    assertTrue(failed.failed.get.isInstanceOf[ArithmeticException])
    assertEquals(Some(saturated), repository.findByUuid(saturated.uuid).get)
    val wrongOwner = uuid("11111111-1111-1111-1111-111111111127")
    assertEquals(AdmissionRejected(NotOwner), repository.activateIfAdmissible(saturated, wrongOwner, Long.MaxValue, 0, 2).get)
  }

  /** 验证收回保留活动行的权威维度；版本或活动状态不匹配均保持业务拒绝。 */
  @Test
  def deactivatesOnlyExpectedActiveState(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111118")
    val initial = state("00000000-0000-0000-0000-000000000310", owner)
    repository.insert(initial).get
    val active = repository.activateIfAdmissible(initial, owner, 0L, 7, 2).get.asInstanceOf[AdmissionAccepted].state

    assertEquals(DeactivationRejected(DeactivationVersionMismatch(1L)), repository.deactivateIfExpectedActive(active.uuid, Some(owner), 0L).get)
    val recalled = repository.deactivateIfExpectedActive(active.uuid, Some(owner), 1L).get match
      case DeactivationAccepted(recalled) =>
        assertFalse(recalled.isActive)
        assertEquals(Some(7), recalled.dimensionId)
        assertEquals(2L, recalled.stateVersion)
        assertEquals(active.worldName, recalled.worldName)
        assertEquals(active.posX, recalled.posX)
        assertEquals(active.posY, recalled.posY)
        assertEquals(active.posZ, recalled.posZ)
        assertEquals(active.yaw, recalled.yaw)
        assertEquals(active.pitch, recalled.pitch)
        recalled
      case other => throw new AssertionError(s"Expected accepted deactivation, got $other")
    assertEquals(DeactivationRejected(NotActive), repository.deactivateIfExpectedActive(active.uuid, Some(owner), 2L).get)
    repository.activateIfAdmissible(recalled, owner, 2L, 8, 2).get match
      case AdmissionAccepted(reactivated) =>
        assertEquals(3L, reactivated.stateVersion)
        assertEquals(Some(8), reactivated.dimensionId)
        assertEquals(Some(reactivated), repository.findByUuid(active.uuid).get)
      case other => throw new AssertionError(s"Expected accepted reactivation, got $other")
  }

  /** 验证 trusted lifecycle 可用精确的 None 所有者收回无主活动行，错误玩家仍被拒绝。 */
  @Test
  def deactivatesOwnerlessActiveStateWithExpectedNoneOwner(): Unit = withDatabase { database =>
    val podUuid = uuid("00000000-0000-0000-0000-000000000317")
    val bootstrap = new SQLiteFloatingPodRepository(database.toString)
    try bootstrap.insert(FloatingPodState(podUuid, None, 0L, "overworld", 4.0D, 70.0D, -3.0D, 5.0F, -1.0F, false, 12L, None)).get
    finally bootstrap.close()
    val raw = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = raw.prepareStatement("UPDATE floating_pod_state SET is_active = 1, dimension_id = 11 WHERE uuid = ?")
      try
        statement.setBytes(1, uuidBytes(podUuid))
        statement.executeUpdate()
      finally statement.close()
    finally raw.close()
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val wrongOwner = uuid("11111111-1111-1111-1111-111111111123")
      assertEquals(DeactivationRejected(DeactivationNotOwner), repository.deactivateIfExpectedActive(podUuid, Some(wrongOwner), 0L).get)
      repository.deactivateIfExpectedActive(podUuid, None, 0L).get match
        case DeactivationAccepted(recalled) =>
          assertFalse(recalled.isActive)
          assertEquals(Some(11), recalled.dimensionId)
          assertEquals(1L, recalled.stateVersion)
        case other => throw new AssertionError(s"Expected ownerless deactivation, got $other")
    finally repository.close()
  }

  /** 验证 V001 遗留活动空维度只能通过 raw recovery row 读取，缺少回调证据时不能构造公开状态。 */
  @Test
  def exposesLegacyActiveNullOnlyThroughRawRecoveryRow(): Unit = withLegacyRepository("00000000-0000-0000-0000-000000000311") { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111119")
    val legacyUuid = uuid("00000000-0000-0000-0000-000000000311")
    val stored = repository.findStoredByUuid(legacyUuid).get.get
    assertTrue(stored.isActive)
    assertEquals(None, stored.dimensionId)
    assertTrue(repository.findDesiredActiveRows().get.contains(stored))
    assertThrows(classOf[IllegalStateException], () => stored.toPublicState)
    assertTrue(repository.findAllRecoverable().isFailure)
  }

  /** 验证回调可回填活动空维度而不改变版本，重放幂等而冲突维度明确拒绝。 */
  @Test
  def backfillsLegacyActiveDimensionFromAuthoritativeCallback(): Unit = withLegacyRepository("00000000-0000-0000-0000-000000000314") { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111121")
    val legacyUuid = uuid("00000000-0000-0000-0000-000000000314")

    repository.backfillMissingActiveDimension(legacyUuid, 0L, 9).get match
      case RecoveryReady(state) =>
        assertEquals(0L, state.stateVersion)
        assertEquals(Some(9), state.dimensionId)
      case other => throw new AssertionError(s"Expected ready recovery backfill, got $other")
    assertEquals(RecoveryReady(repository.findByUuid(legacyUuid).get.get), repository.backfillMissingActiveDimension(legacyUuid, 0L, 9).get)
    assertEquals(RecoveryRejected(DimensionConflict(9)), repository.backfillMissingActiveDimension(legacyUuid, 0L, 10).get)
  }

  /** 验证回填边界以 typed rejection 区分不存在、非活动和版本不匹配。 */
  @Test
  def rejectsInvalidRecoveryDimensionBackfillRequests(): Unit = withRepository { repository =>
    val owner = uuid("11111111-1111-1111-1111-111111111122")
    val inactive = state("00000000-0000-0000-0000-000000000315", owner)
    repository.insert(inactive).get
    assertEquals(RecoveryRejected(RecoveryUnknownPod), repository.backfillMissingActiveDimension(uuid("00000000-0000-0000-0000-000000000316"), 0L, 1).get)
    assertEquals(RecoveryRejected(InactivePod), repository.backfillMissingActiveDimension(inactive.uuid, 0L, 1).get)
    val active = repository.activateIfAdmissible(inactive, owner, 0L, 1, 2).get.asInstanceOf[AdmissionAccepted].state
    assertEquals(RecoveryRejected(RecoveryVersionMismatch(1L)), repository.backfillMissingActiveDimension(active.uuid, 0L, 1).get)
  }

  private def withRepository(body: SQLiteFloatingPodRepository => Unit): Unit =
    val repository = new SQLiteFloatingPodRepository(":memory:")
    try body(repository)
    finally repository.close()

  private def withDatabase(body: Path => Unit): Unit =
    val database = Files.createTempFile("floatingpod-activity-transition-", ".db")
    try body(database)
    finally Files.deleteIfExists(database)

  private def withLegacyRepository(uuidValue: String)(body: SQLiteFloatingPodRepository => Unit): Unit =
    val database = Files.createTempFile("floatingpod-legacy-", ".db")
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      applyLegacyScript(connection, "/db/migration/V001__initial_schema.sql")
      applyLegacyScript(connection, "/db/migration/V002__activity_transition_authority.sql")
      val statement = connection.prepareStatement("INSERT INTO floating_pod_state (uuid, state_version, world_name, pos_x, pos_y, pos_z, yaw, pitch, is_active, last_updated, dimension_id) VALUES (?, 0, 'overworld', 0, 64, 0, 0, 0, 1, 1, NULL)")
      try
        statement.setBytes(1, uuidBytes(uuid(uuidValue)))
        statement.executeUpdate()
      finally statement.close()
    finally connection.close()
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try body(repository)
    finally
      repository.close()
      Files.deleteIfExists(database)

  private def applyLegacyScript(connection: Connection, resource: String): Unit =
    val script = new String(Option(getClass.getResourceAsStream(resource)).get.readAllBytes(), StandardCharsets.UTF_8)
    script.split(';').iterator.map(_.trim).filter(_.nonEmpty).foreach { sql =>
      val statement = connection.createStatement()
      try statement.execute(sql)
      finally statement.close()
    }

  private def state(id: String, owner: UUID, active: Boolean = false): FloatingPodState =
    FloatingPodState(uuid(id), Some(owner), 0L, "overworld", 1.0D, 64.0D, 2.0D, 0.0F, 0.0F, active, 1L)

  private def uuid(value: String): UUID = UUID.fromString(value)

  private def entitlement(database: Path, owner: UUID): (Int, Int) =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = connection.prepareStatement("SELECT registration_limit, simultaneous_active_limit FROM player_entitlement WHERE player_uuid = ?")
      try
        statement.setBytes(1, uuidBytes(owner))
        val resultSet = statement.executeQuery()
        try
          assertTrue(resultSet.next())
          (resultSet.getInt(1), resultSet.getInt(2))
        finally resultSet.close()
      finally statement.close()
    finally connection.close()

  private def entitlementCount(database: Path, owner: UUID): Int =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = connection.prepareStatement("SELECT COUNT(*) FROM player_entitlement WHERE player_uuid = ?")
      try
        statement.setBytes(1, uuidBytes(owner))
        val resultSet = statement.executeQuery()
        try
          assertTrue(resultSet.next())
          resultSet.getInt(1)
        finally resultSet.close()
      finally statement.close()
    finally connection.close()

  private def setEntitlement(database: Path, owner: UUID, registrationLimit: Int, activeLimit: Int): Unit =
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      val statement = connection.prepareStatement("INSERT INTO player_entitlement (player_uuid, registration_limit, simultaneous_active_limit) VALUES (?, ?, ?) ON CONFLICT(player_uuid) DO UPDATE SET registration_limit = excluded.registration_limit, simultaneous_active_limit = excluded.simultaneous_active_limit")
      try
        statement.setBytes(1, uuidBytes(owner))
        statement.setInt(2, registrationLimit)
        statement.setInt(3, activeLimit)
        statement.executeUpdate()
      finally statement.close()
    finally connection.close()

  private def uuidBytes(value: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
