package com.wi.floatingpod.crashpoint

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.entity.FloatingPodEntity
import com.wi.floatingpod.server.ActiveFloatingPodManager
import com.wi.floatingpod.storage.FloatingPodRepository

import net.minecraft.world.World

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** 保留运行时与持久化多重记录的崩溃恢复快照，避免过早折叠掩盖重复项。 */
final case class StateSnapshot(
    activePods: List[UUID],
    persistedPods: List[UUID],
    activeInDb: List[UUID],
    entityIds: List[Int],
    entityPodMappings: List[(Int, Option[UUID])],
    ownershipMappings: List[(UUID, Option[UUID])],
    persistedStates: List[FloatingPodState]
):
  /** 返回重复持久化 UUID，主键异常不会被 Set 转换静默隐藏。 */
  def duplicatePersistedUuids: Set[UUID] = duplicates(persistedPods)

  /** 返回重复实体数值 ID。 */
  def duplicateEntityIds: Set[Int] = duplicates(entityIds)

  /** 返回无法映射到当前 manager 活动 UUID 的实体 ID。 */
  def orphanEntityIds: Set[Int] =
    entityPodMappings.collect { case (entityId, Some(podUuid)) if !activePods.contains(podUuid) => entityId }.toSet

  /** 返回 manager 没有 UUID 关联的未知实体 ID。 */
  def unknownEntityIds: Set[Int] =
    entityPodMappings.collect { case (entityId, None) => entityId }.toSet

  private def duplicates[A](values: List[A]): Set[A] =
    values.groupMapReduce(identity)(_ => 1)(_ + _).collect { case (value, count) if count > 1 => value }.toSet

/** 两个崩溃前后快照之间的可审计差异。 */
final case class SnapshotDiff(
    newActivePods: Set[UUID],
    removedActivePods: Set[UUID],
    newPersistedPods: Set[UUID],
    removedPersistedPods: Set[UUID],
    newActiveInDb: Set[UUID],
    removedActiveInDb: Set[UUID],
    newEntityIds: Set[Int],
    removedEntityIds: Set[Int],
    duplicatePersistedUuids: Set[UUID],
    duplicateEntityIds: Set[Int],
    orphanEntityIds: Set[Int],
    unknownEntityIds: Set[Int],
    ownerConflicts: Set[UUID],
    duplicateOwnership: Set[UUID]
)

object StateSnapshot:
  /** 从真实 Minecraft world 捕获 FloatingPod 实体 ID，并通过 manager 解析其 UUID。 */
  def capture(
      manager: ActiveFloatingPodManager,
      repository: FloatingPodRepository,
      world: World
  ): Try[StateSnapshot] = Try {
    require(manager != null, "manager must not be null")
    require(repository != null, "repository must not be null")
    require(world != null, "world must not be null")
    val entityIds = world.loadedEntityList.asScala.collect { case entity: FloatingPodEntity => entity.getEntityId }.toList
    captureFromEntityIds(manager, repository, entityIds)
  }

  /** 测试入口：使用明确的实体 ID 列表，不构造 Minecraft World，也不使用反射。 */
  def capture(
      manager: ActiveFloatingPodManager,
      repository: FloatingPodRepository,
      entityIds: List[Int]
  ): Try[StateSnapshot] = Try {
    require(entityIds != null, "entityIds must not be null")
    require(manager != null, "manager must not be null")
    require(repository != null, "repository must not be null")
    captureFromEntityIds(manager, repository, entityIds)
  }

  /** 计算保留重复、未知实体和同一 UUID 所有权变化的快照差异。 */
  def diff(before: StateSnapshot, after: StateSnapshot): SnapshotDiff =
    require(before != null, "before snapshot must not be null")
    require(after != null, "after snapshot must not be null")
    val beforeOwners = ownersByPod(before)
    val afterOwners = ownersByPod(after)
    val ownerConflicts = beforeOwners.keySet.intersect(afterOwners.keySet).filter { podUuid =>
      beforeOwners(podUuid) != afterOwners(podUuid)
    }
    SnapshotDiff(
      newActivePods = after.activePods.toSet -- before.activePods.toSet,
      removedActivePods = before.activePods.toSet -- after.activePods.toSet,
      newPersistedPods = after.persistedPods.toSet -- before.persistedPods.toSet,
      removedPersistedPods = before.persistedPods.toSet -- after.persistedPods.toSet,
      newActiveInDb = after.activeInDb.toSet -- before.activeInDb.toSet,
      removedActiveInDb = before.activeInDb.toSet -- after.activeInDb.toSet,
      newEntityIds = after.entityIds.toSet -- before.entityIds.toSet,
      removedEntityIds = before.entityIds.toSet -- after.entityIds.toSet,
      duplicatePersistedUuids = after.duplicatePersistedUuids,
      duplicateEntityIds = after.duplicateEntityIds,
      orphanEntityIds = after.orphanEntityIds,
      unknownEntityIds = after.unknownEntityIds,
      ownerConflicts = ownerConflicts,
      duplicateOwnership = duplicateOwnerPods(afterOwners)
    )

  /** 按辅助机 UUID 聚合非空所有者，避免 Map 折叠掩盖同一 UUID 的多行损坏。 */
  private def ownersByPod(snapshot: StateSnapshot): Map[UUID, Set[UUID]] =
    snapshot.ownershipMappings
      .groupMap { case (podUuid, _) => podUuid } { case (_, ownerUuid) => ownerUuid }
      .view
      .mapValues(_.flatten.toSet)
      .toMap

  /** 返回在单个快照中具有多个不同非空所有者的辅助机 UUID。 */
  private def duplicateOwnerPods(owners: Map[UUID, Set[UUID]]): Set[UUID] =
    owners.collect { case (podUuid, ownerUuids) if ownerUuids.size > 1 => podUuid }.toSet

  private def captureFromEntityIds(
      manager: ActiveFloatingPodManager,
      repository: FloatingPodRepository,
      entityIds: List[Int]
  ): StateSnapshot =
    val persistedStates = repository.findAll().get
    val activePods = manager.getAllActivePods()
    val sortedEntityIds = entityIds.sorted
    StateSnapshot(
      activePods = activePods,
      persistedPods = persistedStates.map(_.uuid),
      activeInDb = persistedStates.filter(_.isActive).map(_.uuid),
      entityIds = sortedEntityIds,
      entityPodMappings = sortedEntityIds.map(entityId => entityId -> manager.getPodUuidByEntityId(entityId)),
      ownershipMappings = persistedStates.map(state => state.uuid -> state.ownerUuid),
      persistedStates = persistedStates
    )
