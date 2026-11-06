package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID

/** Main 在入队前保留的命令身份；该值不签发 session、writer 或世界权限。 */
final class MySqlOwnedRepositoryOperationPlan private (
    val tag: IoOperationTag,
    val workflowUuid: UUID,
    val reserveOperationUuid: UUID,
    private[mysql] val requestIdentity: MySqlRepositoryRequestIdentity,
    private val originalOperations: Vector[MySqlRepositoryOperation],
    private val limitProfile: SourceLimitProfile,
    private[mysql] val retainedHandle: Option[MySqlRetainedPlanHandle] = None
):
  require(tag != null && workflowUuid != null && reserveOperationUuid != null && requestIdentity != null &&
    originalOperations != null && originalOperations.nonEmpty && !originalOperations.exists(_ == null) &&
    limitProfile != null, "complete Main-retained repository operation plan is required")
  require(nonZero(tag.bootId) && nonZero(tag.operationId) && nonZero(workflowUuid) && nonZero(reserveOperationUuid),
    "Main operation, workflow, and reservation UUIDs must be non-zero")
  require(reserveOperationUuid != tag.operationId,
    "reserve and execute operation UUIDs must remain distinct fixed identities")

  require(retainedHandle != null && !retainedHandle.exists(_ == null), "optional exact owned reservation required")
  require(retainedHandle.forall(_.tag == tag), "reservation envelope tag differs")

  val executeOperationUuid: UUID = tag.operationId
  val requestKind: String = requestIdentity.requestKind
  val requestIdentitySha256: Vector[Byte] = requestIdentity.sha256
  val requestIdentitySha256Hex: String = requestIdentity.sha256Hex

  def requireTag(actual: IoOperationTag): Unit =
    if actual == null || actual != tag then
      throw new IllegalArgumentException("repository operation tag differs from its Main-retained plan")

  def requireSingle(operation: MySqlRepositoryOperation, actualProfile: SourceLimitProfile): Unit =
    if requestKind != "RUN" || operation == null || actualProfile == null || actualProfile != limitProfile ||
      originalOperations.size != 1 then
      throw new IllegalArgumentException("repository RUN does not match its Main-retained plan")
    val matches = retainedHandle match
      case Some(handle) =>
        val temporary = handle.reservationCore.validationBudget(handle).newCanonicalScope()
        var primary: Throwable = null
        try MySqlRecoveryCanonicalBytes.withAllocationBudget(temporary) {
          requestIdentity.exactlyEquals(MySqlRepositoryRequestIdentity.single(operation, actualProfile))
        }
        catch
          case cause: Throwable =>
            primary = cause
            throw cause
        finally
          try temporary.closeScopedReadBudget()
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      case None => requestIdentity.exactlyEquals(MySqlRepositoryRequestIdentity.single(operation, actualProfile))
    if !matches then
      throw new IllegalArgumentException("repository RUN request bytes differ from its Main-retained plan")

  def requireTransaction(operations: Vector[MySqlRepositoryOperation], actualProfile: SourceLimitProfile): Unit =
    if requestKind != "TRANSACTION" || operations == null || operations.isEmpty || actualProfile == null ||
      actualProfile != limitProfile || originalOperations.size != operations.size then
      throw new IllegalArgumentException("repository transaction does not match its Main-retained plan")
    val matches = retainedHandle match
      case Some(handle) =>
        val temporary = handle.reservationCore.validationBudget(handle).newCanonicalScope()
        var primary: Throwable = null
        try MySqlRecoveryCanonicalBytes.withAllocationBudget(temporary) {
          requestIdentity.exactlyEquals(MySqlRepositoryRequestIdentity.transaction(operations, actualProfile))
        }
        catch
          case cause: Throwable =>
            primary = cause
            throw cause
        finally
          try temporary.closeScopedReadBudget()
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      case None => requestIdentity.exactlyEquals(MySqlRepositoryRequestIdentity.transaction(operations, actualProfile))
    if !matches then
      throw new IllegalArgumentException("repository transaction request bytes differ from its Main-retained plan")

  private[floatingpod] def originalRetainedHandle: Option[MySqlRetainedPlanHandle] = retainedHandle

  private[mysql] def originalOperationCount: Int = originalOperations.size
  private[mysql] def exactRequestIdentity: MySqlRepositoryRequestIdentity = requestIdentity
  private[mysql] def exactLimitProfile: SourceLimitProfile = limitProfile

  override def equals(other: Any): Boolean = other match
    case value: MySqlOwnedRepositoryOperationPlan =>
      tag == value.tag && workflowUuid == value.workflowUuid && reserveOperationUuid == value.reserveOperationUuid &&
        requestIdentity == value.requestIdentity && limitProfile == value.limitProfile
    case _ => false
  override def hashCode(): Int = (tag, workflowUuid, reserveOperationUuid, requestIdentity, limitProfile).hashCode
  override def toString: String =
    s"MySqlOwnedRepositoryOperationPlan[kind=$requestKind,workflow=$workflowUuid,execute=$executeOperationUuid,reserve=$reserveOperationUuid,requestSha256=$requestIdentitySha256Hex]"

  private def nonZero(value: UUID): Boolean =
    value != null && (value.getMostSignificantBits | value.getLeastSignificantBits) != 0L

object MySqlOwnedRepositoryOperationPlan:
  def single(
      tag: IoOperationTag,
      workflowUuid: UUID,
      reserveOperationUuid: UUID,
      operation: MySqlRepositoryOperation,
      limitProfile: SourceLimitProfile
  ): MySqlOwnedRepositoryOperationPlan =
    if tag == null || operation == null || limitProfile == null then
      throw new IllegalArgumentException("Main RUN tag, original request, and exact limit profile are required")
    val identity = MySqlRepositoryRequestIdentity.single(operation, limitProfile)
    new MySqlOwnedRepositoryOperationPlan(tag, workflowUuid, reserveOperationUuid, identity, Vector(operation), limitProfile)

  def transaction(
      tag: IoOperationTag,
      workflowUuid: UUID,
      reserveOperationUuid: UUID,
      operations: Vector[MySqlRepositoryOperation],
      limitProfile: SourceLimitProfile
  ): MySqlOwnedRepositoryOperationPlan =
    if tag == null || operations == null || operations.isEmpty || limitProfile == null then
      throw new IllegalArgumentException("Main transaction tag, non-empty original request, and exact limit profile are required")
    val identity = MySqlRepositoryRequestIdentity.transaction(operations, limitProfile)
    new MySqlOwnedRepositoryOperationPlan(tag, workflowUuid, reserveOperationUuid, identity, operations, limitProfile)

  /** 包内actual issuer专用；公开single/transaction仍仅DATA，不能通过session执行门。 */
  private[mysql] def singleRetained(tag: IoOperationTag, workflowUuid: UUID, reserveOperationUuid: UUID,
      operation: MySqlRepositoryOperation, profile: SourceLimitProfile,
      handle: MySqlRetainedPlanHandle): MySqlOwnedRepositoryOperationPlan =
    require(handle != null && handle.tag == tag && operation != null && profile != null,
      "actual fixed retained reservation required")
    val identity = MySqlRepositoryRequestIdentity.single(operation, profile)
    new MySqlOwnedRepositoryOperationPlan(tag, workflowUuid, reserveOperationUuid, identity, Vector(operation), profile, Some(handle))

  private[mysql] def transactionRetained(tag: IoOperationTag, workflowUuid: UUID, reserveOperationUuid: UUID,
      operations: Vector[MySqlRepositoryOperation], profile: SourceLimitProfile,
      handle: MySqlRetainedPlanHandle): MySqlOwnedRepositoryOperationPlan =
    require(handle != null && handle.tag == tag && operations != null && operations.nonEmpty &&
      !operations.exists(_ == null) && profile != null, "actual fixed retained transaction reservation required")
    val identity = MySqlRepositoryRequestIdentity.transaction(operations, profile)
    new MySqlOwnedRepositoryOperationPlan(tag, workflowUuid, reserveOperationUuid, identity, operations, profile, Some(handle))
