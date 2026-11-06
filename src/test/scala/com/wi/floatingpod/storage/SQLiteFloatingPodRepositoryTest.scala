package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.util.UUID
import java.nio.file.Files
import scala.util.Failure
import scala.util.Try

/**
 * 直接验证 SQLite CRUD、乐观锁和事务行为，避免以源码文本替代实际 JDBC 行为。
 */
class SQLiteFloatingPodRepositoryTest:
  /** 验证新 UUID 可插入且能以相同状态读取。 */
  @Test
  def insertsNewState(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000001")
    assertEquals(state, repository.insert(state).get)
    assertEquals(Some(state), repository.findByUuid(state.uuid).get)
  }

  /** 验证重复 UUID 返回显式失败而非覆盖既有记录。 */
  @Test
  def rejectsDuplicateUuid(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000002")
    repository.insert(state).get
    assertTrue(failureOf(repository.insert(state)).isInstanceOf[IllegalStateException])
  }

  /** 验证更新同时递增版本并刷新时间戳。 */
  @Test
  def updatesWithCorrectVersion(): Unit = withRepository { repository =>
    val inserted = sampleState("00000000-0000-0000-0000-000000000003", lastUpdated = 1L)
    repository.insert(inserted).get
    val updateStartedAt = System.currentTimeMillis()
    val updated = repository.update(inserted.copy(worldName = "nether", lastUpdated = 0L)).get
    assertEquals(1L, updated.stateVersion)
    assertTrue(updated.lastUpdated >= updateStartedAt)
    assertEquals(Some(updated), repository.findByUuid(inserted.uuid).get)
  }

  /** 验证过期版本更新不会影响已提交状态。 */
  @Test
  def rejectsStaleOptimisticUpdate(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000004")
    repository.insert(state).get
    val current = repository.update(state).get
    assertTrue(failureOf(repository.update(state.copy(worldName = "stale"))).isInstanceOf[OptimisticLockException])
    assertEquals(Some(current), repository.findByUuid(state.uuid).get)
  }

  /** 验证 UUID 查询区分存在和不存在的恢复记录。 */
  @Test
  def findsByUuidAndReportsMissing(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000005")
    repository.insert(state).get
    assertEquals(Some(state), repository.findByUuid(state.uuid).get)
    assertEquals(None, repository.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000006")).get)
  }

  /** 验证所有者查询返回同一所有者的全部记录。 */
  @Test
  def findsAllStatesForOwner(): Unit = withRepository { repository =>
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111111")
    val first = sampleState("00000000-0000-0000-0000-000000000007", owner = Some(owner))
    val second = sampleState("00000000-0000-0000-0000-000000000008", owner = Some(owner))
    repository.insert(first).get
    repository.insert(second).get
    repository.insert(sampleState("00000000-0000-0000-0000-000000000009")).get
    assertEquals(List(first, second), repository.findByOwner(owner).get)
  }

  /** 验证活动查询不会返回已收回状态。 */
  @Test
  def findsOnlyActiveStates(): Unit = withRepository { repository =>
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111112")
    val inactive = sampleState("00000000-0000-0000-0000-000000000010", owner = Some(owner))
    val recalled = sampleState("00000000-0000-0000-0000-000000000011", active = false)
    repository.insert(inactive).get
    repository.insert(recalled).get
    val active = repository.activateIfAdmissible(inactive, owner, 0L, 7, 2).get.asInstanceOf[AdmissionOutcome.Accepted].state
    assertEquals(List(active), repository.findAllActive().get)
  }

  /** 验证通用插入不会绕过活动生命周期入口。 */
  @Test
  def rejectsGenericActiveInsert(): Unit = withRepository { repository =>
    val active = sampleState("00000000-0000-0000-0000-000000000018", owner = Some(UUID.fromString("11111111-1111-1111-1111-111111111118")), active = true, dimensionId = Some(1))
    assertTrue(failureOf(repository.insert(active)).isInstanceOf[IllegalArgumentException])
  }

  /** 验证通用更新即使收到伪造生命周期字段，也保留数据库中的活动与维度。 */
  @Test
  def genericUpdatePreservesLifecycleFields(): Unit = withRepository { repository =>
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111113")
    val inactive = sampleState("00000000-0000-0000-0000-000000000019", owner = Some(owner))
    repository.insert(inactive).get
    val active = repository.activateIfAdmissible(inactive, owner, 0L, 8, 2).get.asInstanceOf[AdmissionOutcome.Accepted].state
    val updated = repository.update(active.copy(worldName = "nether", isActive = false, dimensionId = None)).get
    assertTrue(updated.isActive)
    assertEquals(Some(8), updated.dimensionId)
    assertEquals("nether", updated.worldName)
    assertEquals(Some(updated), repository.findByUuid(active.uuid).get)
  }

  @Test
  def genericUpdateCannotChangeOperatingMode(): Unit = withRepository { repository =>
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111127")
    val inactive = sampleState("00000000-0000-0000-0000-000000000027", owner = Some(owner))
    repository.insert(inactive).get
    val active = repository.activateIfAdmissible(inactive, owner, 0L, 7, 256).get.asInstanceOf[AdmissionOutcome.Accepted].state

    val failure = failureOf(repository.update(active.copy(operatingMode = PodOperatingMode.WorldIdle)))

    assertTrue(failure.isInstanceOf[IllegalArgumentException])
    assertEquals(Some(active), repository.findByUuid(active.uuid).get)
  }

  /** 验证活动行不能经通用删除跳过 Recall。 */
  @Test
  def rejectsDeletingActiveState(): Unit = withRepository { repository =>
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111114")
    val inactive = sampleState("00000000-0000-0000-0000-000000000020", owner = Some(owner))
    repository.insert(inactive).get
    val active = repository.activateIfAdmissible(inactive, owner, 0L, 9, 2).get.asInstanceOf[AdmissionOutcome.Accepted].state
    assertTrue(failureOf(repository.delete(active.uuid)).isInstanceOf[IllegalStateException])
    assertEquals(Some(active), repository.findByUuid(active.uuid).get)
  }

  /** 验证另一连接的活动转换会使旧通用更新失败，不能回写生命周期字段。 */
  @Test
  def rejectsGenericUpdateAfterCrossConnectionActivation(): Unit =
    val database = Files.createTempFile("floatingpod-generic-race-", ".db")
    val owner = UUID.fromString("11111111-1111-1111-1111-111111111115")
    val first = new SQLiteFloatingPodRepository(database.toString)
    val second = new SQLiteFloatingPodRepository(database.toString)
    try
      val inactive = sampleState("00000000-0000-0000-0000-000000000021", owner = Some(owner))
      first.insert(inactive).get
      val stale = first.findByUuid(inactive.uuid).get.get
      val active = second.activateIfAdmissible(inactive, owner, 0L, 10, 2).get.asInstanceOf[AdmissionOutcome.Accepted].state
      assertTrue(failureOf(first.update(stale.copy(worldName = "stale", isActive = false, dimensionId = None))).isInstanceOf[OptimisticLockException])
      assertEquals(Some(active), second.findByUuid(inactive.uuid).get)
    finally
      first.close()
      second.close()
      Files.deleteIfExists(database)

  /** 验证删除结果区分实际删除与目标不存在。 */
  @Test
  def deletesExistingAndReportsMissing(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000012")
    repository.insert(state).get
    assertTrue(repository.delete(state.uuid).get)
    assertFalse(repository.delete(state.uuid).get)
  }

  /** 验证事务提交后其中的全部状态均可恢复。 */
  @Test
  def commitsTransaction(): Unit = withRepository { repository =>
    val first = sampleState("00000000-0000-0000-0000-000000000013")
    val second = sampleState("00000000-0000-0000-0000-000000000014")
    repository.transaction {
      repository.insert(first).get
      repository.insert(second).get
      "committed"
    }.get
    assertEquals(Some(first), repository.findByUuid(first.uuid).get)
    assertEquals(Some(second), repository.findByUuid(second.uuid).get)
  }

  /** 验证事务内异常会回滚先前已完成的写入。 */
  @Test
  def rollsBackTransaction(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000015")
    val result = repository.transaction {
      repository.insert(state).get
      throw new IllegalStateException("force rollback")
    }
    assertTrue(failureOf(result).isInstanceOf[IllegalStateException])
    assertEquals(None, repository.findByUuid(state.uuid).get)
  }

  /** 验证拒绝嵌套事务时，外层事务已经执行的写入同样会回滚。 */
  @Test
  def rejectsNestedTransactionsAndRollsBackOuterWrites(): Unit = withRepository { repository =>
    val state = sampleState("00000000-0000-0000-0000-000000000016")
    val result = repository.transaction {
      repository.insert(state).get
      repository.transaction("nested").get
    }
    assertTrue(failureOf(result).isInstanceOf[IllegalStateException])
    assertEquals(None, repository.findByUuid(state.uuid).get)
  }

  /** 验证领域状态拒绝空标识、非法版本、空白世界和非有限位置，避免无效记录进入 JDBC 层。 */
  @Test
  def rejectsInvalidDomainState(): Unit =
    val valid = sampleState("00000000-0000-0000-0000-000000000017")
    assertIllegalArgument(valid.copy(uuid = null))
    assertIllegalArgument(valid.copy(ownerUuid = null))
    assertIllegalArgument(valid.copy(stateVersion = -1L))
    assertIllegalArgument(valid.copy(worldName = "  "))
    assertIllegalArgument(valid.copy(posX = Double.NaN))

  private def withRepository(body: SQLiteFloatingPodRepository => Unit): Unit =
    val repository = new SQLiteFloatingPodRepository(":memory:")
    try body(repository)
    finally repository.close()

  private def failureOf(result: Try[?]): Throwable =
    result match
      case Failure(error) => error
      case _ => throw new AssertionError("Expected a failed Try")

  private def assertIllegalArgument(action: => Any): Unit =
    try
      action
      throw new AssertionError("Expected IllegalArgumentException")
    catch
      case _: IllegalArgumentException => ()

  private def sampleState(
      uuid: String,
      owner: Option[UUID] = None,
      worldName: String = "overworld",
      active: Boolean = false,
      lastUpdated: Long = 100L,
      dimensionId: Option[Int] = None
  ): FloatingPodState =
    FloatingPodState(
      uuid = UUID.fromString(uuid),
      ownerUuid = owner,
      stateVersion = 0L,
      worldName = worldName,
      posX = 1.25D,
      posY = 64.0D,
      posZ = -2.5D,
      yaw = 90.0F,
      pitch = 10.0F,
      isActive = active,
      lastUpdated = lastUpdated,
      dimensionId = dimensionId
    )
