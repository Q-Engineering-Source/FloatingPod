package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 原 capture 的一次不可变请求身份；不持有 snapshot、allocation、游戏或 JDBC owner。 */
private[floatingpod] final class MySqlDbFirstInitialActivationRequest private[mysql] (
    val tag: IoOperationTag, val creationOperationUuid: UUID, val bindingUuid: UUID, val podUuid: UUID, val ownerUuid: Option[UUID],
    val attemptUuid: UUID, val capturedStateVersion: Long, val snapshotFormatVersion: Int,
    val nbtByteLength: Int, val nbtSha256: Vector[Byte], val profileBytes: Vector[Byte],
    val profileSha256: Vector[Byte], val pose: DbFirstPodSnapshot.Pose,
    val identity: MySqlDbFirstInitialActivationIdentity)

/** 脱离账本的完整规范 DATA；字段仅为不可变字节容器，不藏 canonical owner。 */
private[floatingpod] final case class MySqlDbFirstInitialActivationIdentity private[mysql] (
    canonicalBytes: Vector[Byte], sha256: Vector[Byte], requestKind: String):
  def canonicalLength: Int = canonicalBytes.size

private[mysql] object MySqlDbFirstInitialActivationRequest:
  def capture(tag: IoOperationTag, snapshot: DbFirstPodSnapshot, root: MySqlFormat3ReadBudget)(
      retainBacking: MySqlRepositoryRequestIdentity => Unit): MySqlDbFirstInitialActivationRequest =
    require(tag != null && snapshot != null && snapshot.purpose() == DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (snapshot.limitProfile() eq root.profile) && snapshot.operationUuid() == tag.operationId && retainBacking != null,
      "original capture operation/profile/root required")
    // 覆盖字段容器、digest/profile 防御副本及 immutable identity 元数据；payload 仍由 canonical sink 精确计费。
    root.reserveRetainedMemory(4096L,"DB-first immutable request fields BEFORE allocation")
    val owner=Option(snapshot.ownerUuid())
    val nbtSha=snapshot.sha256().toVector
    val profileBytes=root.profile.canonicalBytes().toVector
    val profileSha=root.profile.sha256().toVector
    val pose=snapshot.pose()
    val backing=MySqlRepositoryRequestIdentity.dbFirstInitialActivation(snapshot,tag,root)
    // 先把已计费的 backing 交原 Entry 保留，再分配 detached DATA；失败不丢失原 custody。
    retainBacking(backing)
    val length=backing.canonicalLength
    // Vector 树/叶数组/构造器按实际 canonical 长度预收，包含 profile 与可变长 world 的完整编码。
    root.reserveRetainedMemory(Math.addExact(1024L,Math.multiplyExact(length.toLong,16L)),
      "DB-first detached complete canonical vector BEFORE allocation")
    val bytes=Vector.tabulate(length)(backing.canonical.borrowedBinary.byteAt)
    val identity=MySqlDbFirstInitialActivationIdentity(bytes,backing.sha256,backing.requestKind)
    new MySqlDbFirstInitialActivationRequest(tag,snapshot.initialCreationOperationUuid(),snapshot.bindingUuid(),snapshot.podUuid(),owner,snapshot.attemptUuid(),
      snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),nbtSha,profileBytes,profileSha,pose,identity)
