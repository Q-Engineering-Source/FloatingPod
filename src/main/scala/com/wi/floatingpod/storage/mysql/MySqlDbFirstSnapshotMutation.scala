package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 仅复用完整快照DATA投影；具体请求类型仍决定独立编码域，不授提交或清理权限。 */
private[floatingpod] trait MySqlDbFirstSnapshotIdentity:
  def canonicalBytes: Vector[Byte]
  def sha256: Vector[Byte]
  def requestKind: String
  def canonicalLength: Int

private[floatingpod] trait MySqlDbFirstSnapshotMutation:
  def tag: IoOperationTag
  def bindingUuid: UUID
  def podUuid: UUID
  def ownerUuid: Option[UUID]
  def attemptUuid: UUID
  def capturedStateVersion: Long
  def snapshotFormatVersion: Int
  def nbtByteLength: Int
  def nbtSha256: Vector[Byte]
  def profileBytes: Vector[Byte]
  def profileSha256: Vector[Byte]
  def pose: DbFirstPodSnapshot.Pose
  def identity: MySqlDbFirstSnapshotIdentity
  final def purpose: DbFirstPodSnapshot.Purpose = this match
    case _: MySqlDbFirstRecallRequest => DbFirstPodSnapshot.Purpose.RECALL
    case _: MySqlDbFirstCheckpointRequest => DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT
    case _ => throw new IllegalArgumentException("unsupported complete snapshot request type")
