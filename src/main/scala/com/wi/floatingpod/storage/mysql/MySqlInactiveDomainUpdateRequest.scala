package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.util.UUID

/** 内部存储 CAS 身份；expectedOwner 不签发用户认证或物理权限。 */
final case class MySqlInactiveDomainUpdateRequest(
    expectedBinding: PhysicalRecoveryBinding,
    podUuid: UUID,
    expectedOwner: Option[UUID],
    expectedVersion: Long,
    desired: FloatingPodState):
  require(expectedBinding != null && expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified &&
    expectedBinding.formatVersion == 2, "inactive update requires exact certified physical2 binding")
  require(podUuid != null && expectedOwner != null && expectedOwner.forall(_ != null) &&
    expectedVersion >= 0L && desired != null, "complete immutable inactive CAS request required")
  require(desired.uuid == podUuid && desired.ownerUuid == expectedOwner &&
    desired.stateVersion == expectedVersion && !desired.isActive,
    "inactive update cannot replace pod, owner, expected version, or activate")
  def repositoryOperation: MySqlRepositoryOperation =
    MySqlRepositoryOperation.UpdateInactiveDomain(this)

/** 正常拒绝不修改原图；故障和 CommitUnknown 不转换成这些业务结果。 */
enum MySqlInactiveDomainUpdateRejection:
  case BindingMismatch, UnknownPod, OwnerMismatch, Active, RecoveryPending
  case VersionMismatch(actualVersion: Long)
  case CommitGraphRecoveryPending(podUuid: UUID)

/** 结果为 immutable DATA；只有实际 owned runner 完整 critical readback 后返回 Updated。 */
enum MySqlInactiveDomainUpdateOutcome:
  case Updated(state: FloatingPodState)
  case Rejected(reason: MySqlInactiveDomainUpdateRejection)
