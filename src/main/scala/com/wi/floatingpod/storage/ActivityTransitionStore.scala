package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.FloatingPodState

import java.util.UUID
import scala.util.Try

/** 活动状态准入的稳定结果；业务拒绝不会伪装成基础设施失败。 */
sealed trait AdmissionOutcome

object AdmissionOutcome:
  /** 数据库已原子提交目标活动状态。 */
  final case class Accepted(state: FloatingPodState) extends AdmissionOutcome

  /** 数据库未修改，并返回可映射到协议的稳定拒绝原因。 */
  final case class Rejected(reason: AdmissionRejection) extends AdmissionOutcome

/** 活动准入的业务拒绝原因。 */
sealed trait AdmissionRejection

object AdmissionRejection:
  case object UnknownPod extends AdmissionRejection
  case object NotOwner extends AdmissionRejection
  final case class VersionMismatch(actualVersion: Long) extends AdmissionRejection
  case object AlreadyActive extends AdmissionRejection
  case object PlayerActiveLimitReached extends AdmissionRejection
  case object GlobalActiveLimitReached extends AdmissionRejection

/** 收回状态转换的稳定结果。 */
sealed trait DeactivationOutcome

object DeactivationOutcome:
  final case class Accepted(state: FloatingPodState) extends DeactivationOutcome
  final case class Rejected(reason: DeactivationRejection) extends DeactivationOutcome

/** 收回转换的业务拒绝原因。 */
sealed trait DeactivationRejection

object DeactivationRejection:
  case object UnknownPod extends DeactivationRejection
  case object NotOwner extends DeactivationRejection
  final case class VersionMismatch(actualVersion: Long) extends DeactivationRejection
  case object NotActive extends DeactivationRejection

/**
 * 提供与实体生命周期分离的数据库权威活动状态转换。
 *
 * 泛型 CRUD 继续保留兼容语义；只有新调用方使用本接口时才获得额度与维度原子约束。
 */
trait ActivityTransitionStore:
  /**
   * 在 SQLite 写保留事务中校验所有权、版本、额度和全服容量后，提交指定位置的活动状态。
   *
   * @param target 调用方希望持久化的目标位置与旋转；其所有权和版本以数据库当前记录为准。
   * @param authenticatedOwner 服务端认证后的玩家标识。
   * @param expectedStateVersion 客户端操作所基于的持久版本。
   * @param dimensionId 服务端权威的目标维度。
   * @param globalActiveLimit 全服同时活动的硬上限。
   */
  def activateIfAdmissible(
      target: FloatingPodState,
      authenticatedOwner: UUID,
      expectedStateVersion: Long,
      dimensionId: Int,
      globalActiveLimit: Int
  ): Try[AdmissionOutcome]

  /** 在 SQLite 写保留事务中按预期所有者和版本提交收回状态。 */
  def deactivateIfExpectedActive(
      podUuid: UUID,
      expectedOwner: Option[UUID],
      expectedStateVersion: Long
  ): Try[DeactivationOutcome]
