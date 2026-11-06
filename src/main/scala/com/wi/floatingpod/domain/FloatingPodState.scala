package com.wi.floatingpod.domain

import java.util.UUID
import java.lang.{Double as JavaDouble, Float as JavaFloat}

/**
 * 表示可由持久化层恢复的辅助机不可变状态，避免调用方在提交前后共享可变数据。
 *
 * @param uuid 辅助机的稳定唯一标识。
 * @param ownerUuid 所有者标识；`None` 表示尚未绑定。
 * @param stateVersion 用于乐观锁的非负状态版本。
 * @param worldName 状态所属世界的非空名称。
 * @param posX 世界坐标 X。
 * @param posY 世界坐标 Y。
 * @param posZ 世界坐标 Z。
 * @param yaw 实体水平朝向。
 * @param pitch 实体垂直朝向。
 * @param isActive 是否存在活动实体；`false` 表示已收回。
 * @param lastUpdated 最近一次持久状态变更的 Unix 毫秒时间戳。
 * @param dimensionId 活动实体所在的服务端维度；活动公开状态必须存在维度，旧数据仅能通过 raw recovery row 处理。
 */
final case class FloatingPodState(
    uuid: UUID,
    ownerUuid: Option[UUID],
    stateVersion: Long,
    worldName: String,
    posX: Double,
    posY: Double,
    posZ: Double,
    yaw: Float,
    pitch: Float,
    isActive: Boolean,
    lastUpdated: Long,
    dimensionId: Option[Int] = None,
    operatingMode: PodOperatingMode = PodOperatingMode.Normal
):
  require(uuid != null, "uuid must not be null")
  require(ownerUuid != null, "ownerUuid must not be null")
  require(ownerUuid.forall(_ != null), "ownerUuid must not contain null")
  require(stateVersion >= 0L, "stateVersion must not be negative")
  require(worldName != null && worldName.trim.nonEmpty, "worldName must not be blank")
  require(JavaDouble.isFinite(posX), "posX must be finite")
  require(JavaDouble.isFinite(posY), "posY must be finite")
  require(JavaDouble.isFinite(posZ), "posZ must be finite")
  require(JavaFloat.isFinite(yaw), "yaw must be finite")
  require(JavaFloat.isFinite(pitch), "pitch must be finite")
  require(lastUpdated >= 0L, "lastUpdated must not be negative")
  require(dimensionId != null, "dimensionId must not be null")
  require(operatingMode != null, "operatingMode must not be null")
  require(!isActive || dimensionId.nonEmpty, "active state must have a dimensionId")
  require(!isActive || ownerUuid.nonEmpty || operatingMode == PodOperatingMode.WorldIdle, "active unclaimed state must use WorldIdle")

  /** 仅活动 WorldIdle 状态承担“原位留在世界”的恢复语义。 */
  def isWorldResident: Boolean = isActive && operatingMode == PodOperatingMode.WorldIdle
