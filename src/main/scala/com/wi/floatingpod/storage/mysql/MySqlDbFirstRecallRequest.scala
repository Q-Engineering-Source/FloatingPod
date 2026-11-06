package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** 原 capture 的一次不可变请求身份；不持有 snapshot、allocation、游戏或 JDBC owner。 */
private[floatingpod] final class MySqlDbFirstRecallRequest private[mysql] (
    val tag: IoOperationTag, val bindingUuid: UUID, val podUuid: UUID, val ownerUuid: Option[UUID],
    val attemptUuid: UUID, val capturedStateVersion: Long, val snapshotFormatVersion: Int,
    val nbtByteLength: Int, val nbtSha256: Vector[Byte], val profileBytes: Vector[Byte],
    val profileSha256: Vector[Byte], val pose: DbFirstPodSnapshot.Pose,
    val identity: MySqlDbFirstRecallIdentity,
    val journalLink:Option[MySqlDbFirstRecallJournalLink]=None) extends MySqlDbFirstSnapshotMutation

/** 原J的不可变关联DATA；单凭此对象不证明durable接纳、认证或C capture许可。 */
private[floatingpod] final case class MySqlDbFirstRecallJournalLink private[mysql] (
    journalTag:IoOperationTag,actorUuid:UUID,clientRequestUuid:Option[UUID],journalRequestSha256:Vector[Byte],
    recoveredOriginal:Option[IoOperationTag]=None):
  /** 原C身份与本次执行scope分别保存；DATA不发行Boot接管或capture资格。 */
  private[mysql] def originalTagFor(execution:IoOperationTag):IoOperationTag = recoveredOriginal.getOrElse(execution)
  private[mysql] def requireFor(execution:IoOperationTag,owner:Option[UUID]):Unit =
    val zero=new UUID(0L,0L)
    require(recoveredOriginal!=null && recoveredOriginal.forall(_!=null),"explicit original C identity option required")
    val original=originalTagFor(execution)
    require(journalTag!=null && original!=null && journalTag.bootId!=null && journalTag.bootId!=zero &&
      journalTag.operationId!=null && journalTag.operationId!=zero && original.operationId!=null && original.operationId!=zero &&
      journalTag.bootId==original.bootId && journalTag.bootGeneration>0L && journalTag.connectionGeneration>0L &&
      journalTag.bootGeneration==original.bootGeneration && journalTag.connectionGeneration==original.connectionGeneration &&
      execution!=null && execution.bootId!=null && execution.bootId!=zero && execution.bootGeneration>0L &&
      execution.connectionGeneration>0L && execution.operationId==original.operationId && recoveredOriginal.forall(_!=execution) &&
      journalTag.operationId!=original.operationId && actorUuid!=null && actorUuid!=zero && owner!=null && owner.contains(actorUuid) &&
      clientRequestUuid!=null && clientRequestUuid.forall(id=>id!=null && id!=zero) &&
      journalRequestSha256!=null && journalRequestSha256.size==32,"exact original J/C namespace, execution, actor and request digest required")
  private[mysql] def requireJournalIdentity(journal:MySqlDbFirstRecallJournalRequest,execution:IoOperationTag):Unit =
    require(journal!=null,"actual original journal DATA required")
    requireFor(execution,Some(journal.actorUuid))
    require(journalTag==journal.journalTag && originalTagFor(execution)==journal.originalTag &&
      actorUuid==journal.actorUuid && clientRequestUuid==journal.clientRequestUuid &&
      journalRequestSha256==journal.canonical.sha256,"complete original journal DATA identity differs")
private[mysql] object MySqlDbFirstRecallJournalLink:
  def from(request:MySqlDbFirstRecallJournalRequest):MySqlDbFirstRecallJournalLink =
    require(request!=null,"original journal DATA required")
    val link=MySqlDbFirstRecallJournalLink(request.journalTag,request.actorUuid,request.clientRequestUuid,request.canonical.sha256)
    link.requireFor(request.originalTag,Some(request.actorUuid))
    link
  /** 显式V3 DATA；真实Boot/core登记、当前锁定Full27和capture资格仍须独立核验。 */
  def forRecovery(request:MySqlDbFirstRecallJournalRequest,execution:IoOperationTag):MySqlDbFirstRecallJournalLink =
    require(request!=null,"original journal DATA required")
    val link=MySqlDbFirstRecallJournalLink(request.journalTag,request.actorUuid,request.clientRequestUuid,
      request.canonical.sha256,Some(request.originalTag))
    link.requireJournalIdentity(request,execution)
    link

/** 脱离账本的完整规范 DATA；字段仅为不可变字节容器，不藏 canonical owner。 */
private[floatingpod] final case class MySqlDbFirstRecallIdentity private[mysql] (
    canonicalBytes: Vector[Byte], sha256: Vector[Byte], requestKind: String) extends MySqlDbFirstSnapshotIdentity:
  def canonicalLength: Int = canonicalBytes.size

private[mysql] object MySqlDbFirstRecallRequest:
  def capture(tag: IoOperationTag, snapshot: DbFirstPodSnapshot, root: MySqlFormat3ReadBudget,
      journalLink:Option[MySqlDbFirstRecallJournalLink]=None)(
      retainBacking: MySqlRepositoryRequestIdentity => Unit): MySqlDbFirstRecallRequest =
    require(tag != null && snapshot != null && snapshot.purpose() == DbFirstPodSnapshot.Purpose.RECALL && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (snapshot.limitProfile() eq root.profile) && snapshot.operationUuid() == tag.operationId && retainBacking != null,
      "original capture operation/profile/root required")
    // 覆盖字段容器、digest/profile 防御副本及 immutable identity 元数据；payload 仍由 canonical sink 精确计费。
    root.reserveRetainedMemory(4096L,"DB-first immutable request fields BEFORE allocation")
    val owner=Option(snapshot.ownerUuid())
    require(journalLink!=null,"explicit journal link option required")
    journalLink.foreach(_.requireFor(tag,owner))
    val nbtSha=snapshot.sha256().toVector
    val profileBytes=root.profile.canonicalBytes().toVector
    val profileSha=root.profile.sha256().toVector
    val pose=snapshot.pose()
    val backing=MySqlRepositoryRequestIdentity.dbFirstRecall(tag,snapshot.bindingUuid(),snapshot.podUuid(),owner,
      snapshot.attemptUuid(),snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),nbtSha,
      profileBytes,profileSha,pose,root,journalLink)
    // 先把已计费的 backing 交原 Entry 保留，再分配 detached DATA；失败不丢失原 custody。
    retainBacking(backing)
    val length=backing.canonicalLength
    // Vector 树/叶数组/构造器按实际 canonical 长度预收，包含 profile 与可变长 world 的完整编码。
    root.reserveRetainedMemory(Math.addExact(1024L,Math.multiplyExact(length.toLong,16L)),
      "DB-first detached complete canonical vector BEFORE allocation")
    val bytes=Vector.tabulate(length)(backing.canonical.borrowedBinary.byteAt)
    val identity=MySqlDbFirstRecallIdentity(bytes,backing.sha256,backing.requestKind)
    new MySqlDbFirstRecallRequest(tag,snapshot.bindingUuid(),snapshot.podUuid(),owner,snapshot.attemptUuid(),
      snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),nbtSha,profileBytes,profileSha,pose,identity,journalLink)
