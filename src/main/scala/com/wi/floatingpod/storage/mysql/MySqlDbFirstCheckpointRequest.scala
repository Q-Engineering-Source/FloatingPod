package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 完整active checkpoint的不可变请求；与recall类型隔离，不授实体/ticket清理权限。 */
private[floatingpod] final class MySqlDbFirstCheckpointRequest private[mysql] (
    val tag: IoOperationTag, val bindingUuid: UUID, val podUuid: UUID, val ownerUuid: Option[UUID],
    val attemptUuid: UUID, val capturedStateVersion: Long, val snapshotFormatVersion: Int,
    val nbtByteLength: Int, val nbtSha256: Vector[Byte], val profileBytes: Vector[Byte],
    val profileSha256: Vector[Byte], val pose: DbFirstPodSnapshot.Pose,
    val identity: MySqlDbFirstCheckpointIdentity) extends MySqlDbFirstSnapshotMutation

private[floatingpod] final case class MySqlDbFirstCheckpointIdentity private[mysql] (
    canonicalBytes: Vector[Byte], sha256: Vector[Byte], requestKind: String) extends MySqlDbFirstSnapshotIdentity:
  def canonicalLength: Int = canonicalBytes.size

private[mysql] object MySqlDbFirstCheckpointRequest:
  def capture(tag: IoOperationTag, snapshot: DbFirstPodSnapshot, root: MySqlFormat3ReadBudget)(
      retainBacking: MySqlRepositoryRequestIdentity => Unit): MySqlDbFirstCheckpointRequest =
    require(snapshot != null && snapshot.purpose() == DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT &&
      tag != null && snapshot.operationUuid() == tag.operationId && root != null && !root.isClosed &&
      !root.isScopedReadBudget && (snapshot.limitProfile() eq root.profile) && retainBacking != null,
      "original active checkpoint operation/profile/root required")
    root.reserveRetainedMemory(4096L,"checkpoint immutable request fields BEFORE allocation")
    val owner=Option(snapshot.ownerUuid())
    val nbtSha=snapshot.sha256().toVector
    val profileBytes=root.profile.canonicalBytes().toVector
    val profileSha=root.profile.sha256().toVector
    val backing=MySqlRepositoryRequestIdentity.dbFirstCheckpoint(snapshot,tag,root)
    retainBacking(backing)
    val length=backing.canonicalLength
    root.reserveRetainedMemory(Math.addExact(1024L,Math.multiplyExact(length.toLong,16L)),
      "checkpoint detached complete canonical vector BEFORE allocation")
    val bytes=Vector.tabulate(length)(backing.canonical.borrowedBinary.byteAt)
    val identity=MySqlDbFirstCheckpointIdentity(bytes,backing.sha256,backing.requestKind)
    new MySqlDbFirstCheckpointRequest(tag,snapshot.bindingUuid(),snapshot.podUuid(),owner,snapshot.attemptUuid(),
      snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),nbtSha,profileBytes,profileSha,snapshot.pose(),identity)
