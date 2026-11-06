package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 原规划 root 下的两阶段 DATA；本类型不发行 JDBC/commit ACK。 */
private[mysql] final class MySqlInactiveDomainWorkflowMaterialization private[mysql] (
    val request: MySqlInactiveDomainUpdateRequest,val plannedAt: LocalDateTime,
    val plan: MySqlOwnedRepositoryOperationPlan,val operation: MySqlRepositoryOperation,
    val context: MySqlRepositoryOperationContextIdentity,val originalRoot: MySqlFormat3ReadBudget,
    val preimage: MySqlFormat3FullSnapshot,val reserveTarget: MySqlFormat3FullSnapshot,
    val executeTarget: MySqlFormat3FullSnapshot,val reserve: MySqlInactiveDomainPhase,
    val execute: MySqlInactiveDomainPhase):
  private[mysql] def exactPlan: MySqlOwnedRepositoryOperationPlan = plan
  private[mysql] def exactOperation: MySqlRepositoryOperation = operation
  private[mysql] val effects: Vector[MySqlInactiveDomainPhase] = Vector(reserve,execute)
  def requireBoundTo(scope: MySqlRepositoryCommandScope,current: MySqlRepositoryOperation): Unit =
    if scope==null || (scope.operationPlan() ne plan) || current!=operation then fail("original scoped command identity differs")
    scope.requireCurrent(scope.exactAuditScope(),plan)
    plan.requireSingle(current,originalRoot.profile)
    verify()

  def verify(): Unit =
    verifyUsingRoot(originalRoot)

  /** 仅 DATA 再验证；实际 caller 必须先证明当前 root 来自同 Session 当前原 commit plan。 */
  private[mysql] def verifyUsingRoot(currentOriginalRoot: MySqlFormat3ReadBudget): Unit =
    if plannedAt==null || plannedAt.getNano%1000!=0 || originalRoot==null || originalRoot.isClosed ||
      currentOriginalRoot==null || (currentOriginalRoot ne originalRoot) || currentOriginalRoot.isClosed || currentOriginalRoot.isScopedReadBudget ||
      (currentOriginalRoot.profile ne originalRoot.profile) || !currentOriginalRoot.sharesAllocationLedger(originalRoot) ||
      reserve==null || execute==null then fail("original held materialization is incomplete or revoked")
    reserve.requireObservedUsingRoot(preimage,false,currentOriginalRoot)
    reserve.requireObservedUsingRoot(reserveTarget,true,currentOriginalRoot)
    execute.requireObservedUsingRoot(reserveTarget,false,currentOriginalRoot)
    execute.requireObservedUsingRoot(executeTarget,true,currentOriginalRoot)

  def requireObserved(phase: MySqlOperationEffectPhase,actual: MySqlFormat3FullSnapshot,targetImage: Boolean): Unit =
    // 包括 reserve 读回中的 stub execute slots；完整 held 两阶段先独立再生成。
    verify()
    phase match
      case MySqlOperationEffectPhase.Reserve => reserve.requireObserved(actual,targetImage)
      case MySqlOperationEffectPhase.Execute => execute.requireObserved(actual,targetImage)

  private def fail(reason: String): Nothing = throw new SQLDataException("inactive held materialization: "+reason)

/** 每次读回独立再生成 cycle 字节，随后比较完整原图；没有 hash-only ACK。 */
private[mysql] final class MySqlInactiveDomainPhase private[mysql] (
    val request: MySqlInactiveDomainUpdateRequest,val plan: MySqlOwnedRepositoryOperationPlan,
    val context: MySqlRepositoryOperationContextIdentity,val plannedAt: LocalDateTime,val phase: MySqlOperationEffectPhase,
    val before: MySqlFormat3FullSnapshot,val target: MySqlFormat3FullSnapshot,val scopeBytes: MySqlRecoveryCanonicalBytes,
    val beforeBytes: MySqlRecoveryCanonicalBytes,val targetBytes: MySqlRecoveryCanonicalBytes,
    val fingerprint: MySqlRecoveryCanonicalBytes,val operationContext: MySqlOperationContext,
    val contextBytes: MySqlRecoveryCanonicalBytes,val witness: MySqlRecoveryCanonicalBytes,
    val originalReserve: Option[MySqlRecoveryOperationRawEnvelope],val root: MySqlFormat3ReadBudget,
    val originalLocatorRevision: Option[Long],
    private val reservedExecute: Option[MySqlInactiveDomainPhase] = None):
  val operationUuid: UUID=if phase==MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid
  private[mysql] def exactOperationContext: MySqlOperationContext = operationContext
  private[mysql] def verifyPreimage(actual: MySqlFormat3FullSnapshot): Unit = requireObserved(actual,false)
  private[mysql] def verifyReadback(actual: MySqlFormat3FullSnapshot): Unit = requireObserved(actual,true)

  def requireObserved(actual: MySqlFormat3FullSnapshot,targetImage: Boolean): Unit =
    requireObservedUsingRoot(actual,targetImage,root)

  private[mysql] def requireObservedUsingRoot(actual: MySqlFormat3FullSnapshot,targetImage: Boolean,
      currentOriginalRoot: MySqlFormat3ReadBudget): Unit =
    if root==null || root.isClosed || currentOriginalRoot==null || (currentOriginalRoot ne root) || currentOriginalRoot.isClosed || currentOriginalRoot.isScopedReadBudget ||
      (currentOriginalRoot.profile ne root.profile) || !currentOriginalRoot.sharesAllocationLedger(root) then
      fail("original held phase and exact shared current allocation root required")
    verifyObserved(actual,targetImage,currentOriginalRoot)

  private def verifyObserved(actual: MySqlFormat3FullSnapshot,targetImage: Boolean,root: MySqlFormat3ReadBudget): Unit =
    if actual==null || root==null || root.isClosed then fail("original phase/root is unavailable")
    MySqlControlledDebugRelations.requireValidFullGraph(actual,root)
    if phase==MySqlOperationEffectPhase.Reserve && targetImage then
      val executePhase=reservedExecute.getOrElse(fail("original independently regenerated execute phase required"))
      if executePhase.phase!=MySqlOperationEffectPhase.Execute || executePhase.plan.ne(plan) ||
        executePhase.request.ne(request) || executePhase.before.ne(target) then fail("original execute phase identity differs")
      executePhase.requireObservedUsingRoot(actual,false,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
    // 从原固定 graph/context/plan 再生成；不从 actual current O 字段学习 expected。
    val regeneratedBefore=MySqlInactiveDomainCanonical.projection(before,request,plan,context,phase,
      MySqlOperationEffectImage.Before,root)
    val regeneratedTarget=MySqlInactiveDomainCanonical.projection(target,request,plan,context,phase,
      MySqlOperationEffectImage.Target,root)
    val regeneratedFingerprint=MySqlInactiveDomainCanonical.fingerprint(request,plan,context,phase,scopeBytes,
      regeneratedBefore,regeneratedTarget,root)
    val regeneratedContext=MySqlInactiveDomainWorkflowMaterializer.standardContext(context,request,operationUuid,regeneratedFingerprint,root)
    val regeneratedContextBytes=MySqlModeOperationEffectCodec.operationContextCanonicalBytes(regeneratedContext,root.profile)
    val regeneratedWitness=MySqlInactiveDomainCanonical.witness(request,plan,phase,plannedAt,scopeBytes,
      regeneratedFingerprint,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,originalReserve,root)
    if !beforeBytes.exactlyEquals(regeneratedBefore) || !targetBytes.exactlyEquals(regeneratedTarget) ||
      !fingerprint.exactlyEquals(regeneratedFingerprint) || !contextBytes.exactlyEquals(regeneratedContextBytes) ||
      !witness.exactlyEquals(regeneratedWitness) then fail("held phase differs from independently regenerated cycles")
    if targetImage then
      MySqlInactiveDomainCycleChecks.requireOperation(operationRow(actual,operationUuid),request,plan,context,operationUuid,
        if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else "DOMAIN_MUTATION",
        scopeBytes,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,regeneratedFingerprint,
        Some(regeneratedWitness),Some(plannedAt),originalLocatorRevision,root)
      MySqlInactiveDomainCycleChecks.requireWriter(actual.relation("fp_runtime_writer").rows.head,
        request.expectedBinding.bindingUuid,operationUuid,regeneratedFingerprint)
      MySqlInactiveDomainDelta.requireAllowed(before,actual,request,plan,phase,root)
    else if phase==MySqlOperationEffectPhase.Execute then
      MySqlInactiveDomainCycleChecks.requireOperation(operationRow(actual,operationUuid),request,plan,context,operationUuid,
        "DOMAIN_MUTATION",scopeBytes,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,
        regeneratedFingerprint,None,None,originalLocatorRevision,root)
    originalReserve.foreach { raw =>
      MySqlControlledDebugFullComparison.requireExactOperationRow(raw.row,operationRow(actual,plan.reserveOperationUuid),root)
      if !raw.exactlyMatches(operationRow(actual,plan.reserveOperationUuid),root.profile) then fail("original reserve full raw row differs")
    }
    MySqlControlledDebugFullComparison.requireExactReadback(if targetImage then target else before,actual,root)
    }

  private def operationRow(image: MySqlFormat3FullSnapshot,id: UUID): Vector[MySqlFormat3Cell] =
    MySqlControlledDebugImages.one(image,"fp_recovery_operation",row =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(row,"fp_recovery_operation","binding_uuid"),request.expectedBinding.bindingUuid) &&
      MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(row,"fp_recovery_operation","operation_uuid"),id))
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive independent phase proof: "+reason)

private[mysql] object MySqlInactiveDomainWorkflowMaterializer:
  import MySqlControlledDebugRows.*
  private[mysql] def standardContext(context: MySqlRepositoryOperationContextIdentity,
      request: MySqlInactiveDomainUpdateRequest,operation: UUID,fingerprint: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlOperationContext =
    val allocation=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    if allocation.isClosed || (allocation.profile ne root.profile) ||
      ((allocation ne root) && !allocation.isCanonicalComparisonChildOf(root)) then
      fail("standard context lost exact original/canonical-comparison allocation owner")
    allocation.reserveRetainedMemory(4096L,"inactive standard context and exact expected effect")
    MySqlOperationContext(context.tag.bootId,context.writerEpoch,request.expectedBinding.bindingUuid,operation,
      Vector(MySqlExpectedEffect("pod/"+request.podUuid.toString,request.expectedVersion,fingerprint.sha256)))

  /** DATA factory：运行时必须先由真实 PlanningReadOwner 验证原 plan/root/DB timestamp。 */
  def materialize(request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,at: LocalDateTime,before: MySqlFormat3FullSnapshot,
      root: MySqlFormat3ReadBudget): MySqlInactiveDomainWorkflowMaterialization =
    if request==null || plan==null || context==null || before==null || at==null || root==null ||
      root.isClosed || root.isScopedReadBudget || (root.profile ne before.limitProfile) ||
      (context.exactLimitProfile ne root.profile) || (before.allocationBudget ne root) ||
      context.tag!=plan.tag || context.bindingUuid!=request.expectedBinding.bindingUuid ||
      context.workflowUuid!=plan.workflowUuid || context.reserveOperationUuid!=plan.reserveOperationUuid ||
      at.getNano%1000!=0 || at.getYear<1000 || at.getYear>9999 then fail("exact original planning inputs and DB DATETIME6 required")
    val operation=request.repositoryOperation
    plan.requireSingle(operation,root.profile)
    MySqlInactiveDomainUpdatePreflight.requireInputGraph(before,root)
    MySqlInactiveDomainUpdatePreflight.rejection(before,request,root).foreach(reason => fail("rejected inactive plan: "+reason))
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(16384L,"inactive materializer/phase wrappers and closures")
      val locatorRevision=originalLocatorRevision(before,request)
      val key=MySqlRecoveryResourceKey.pod(request.podUuid,root.profile)
      val scope=MySqlInactiveDomainCanonical.scope(request,context,key,root)
      val seed=MySqlRecoveryCanonicalBytes.encode("FP_INACTIVE_PLANNING_SEED_BE_V1",root.profile) { output =>
        MySqlRecoveryCanonicalBytes.writeToken(output,"FP_INACTIVE_PLANNING_SEED_V1");output.writeInt(0)
      }
      val reserveBefore=MySqlInactiveDomainCanonical.projection(before,request,plan,context,
        MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Before,root)
      val reserveSeed=MySqlInactiveDomainOperationRows.preimage(
        MySqlInactiveDomainOperationRows.operation(request,plan,context,at,locatorRevision,true,scope,seed,root),reserveBefore,root)
      val executeSeed=MySqlInactiveDomainOperationRows.operation(request,plan,context,at,locatorRevision,false,scope,seed,root)
      val (held,hadReservation)=MySqlInactiveDomainOperationRows.reservation(before,request,plan,key,root)
      var reserveScaffold=MySqlControlledDebugImages.put(before,Operation,reserveSeed,root,false)
      reserveScaffold=MySqlControlledDebugImages.put(reserveScaffold,Operation,executeSeed,root,false)
      reserveScaffold=MySqlControlledDebugImages.put(reserveScaffold,Reservation,held,root,hadReservation)
      // reserve O 已 COMMITTED INSERT；真实 RESERVED execute O 和 HELD POD 同 Tx 授权此窄同步。
      MySqlInactiveDomainUpdatePreflight.normalizedLocatorTarget(before,request,plan.executeOperationUuid,root).foreach { row =>
        reserveScaffold=MySqlControlledDebugImages.put(reserveScaffold,"physical_recovery_locator",row,root,true)
      }
      reserveScaffold=MySqlInactiveDomainOperationRows.writer(reserveScaffold,plan.reserveOperationUuid,seed,root)
      val reserveTargetBytes=MySqlInactiveDomainCanonical.projection(reserveScaffold,request,plan,context,
        MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Target,root)
      val reserveFp=MySqlInactiveDomainCanonical.fingerprint(request,plan,context,MySqlOperationEffectPhase.Reserve,
        scope,reserveBefore,reserveTargetBytes,root)
      val reserveContext=standardContext(context,request,plan.reserveOperationUuid,reserveFp,root)
      val reserveC=MySqlModeOperationEffectCodec.operationContextCanonicalBytes(reserveContext,root.profile)
      val reserveWitness=MySqlInactiveDomainCanonical.witness(request,plan,MySqlOperationEffectPhase.Reserve,at,
        scope,reserveFp,reserveC,reserveBefore,reserveTargetBytes,None,root)
      val reserveFinal=MySqlInactiveDomainOperationRows.commit(MySqlInactiveDomainOperationRows.phase(reserveSeed,
        reserveC,reserveBefore,reserveTargetBytes,reserveFp,root),reserveWitness,at,root)
      var reserveBase=MySqlControlledDebugImages.put(reserveScaffold,Operation,reserveFinal,root,true)
      reserveBase=MySqlInactiveDomainOperationRows.writer(reserveBase,plan.reserveOperationUuid,reserveFp,root)
      val executeBefore=MySqlInactiveDomainCanonical.projection(reserveBase,request,plan,context,
        MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Before,root)
      val domain=MySqlInactiveDomainUpdatePreflight.domainTarget(reserveBase,request,plan.executeOperationUuid,at,root)
      var executeScaffold=MySqlControlledDebugImages.put(reserveBase,Domain,domain,root,true)
      MySqlInactiveDomainUpdatePreflight.locatorTarget(reserveBase,request,plan.executeOperationUuid,root).foreach { locator =>
        executeScaffold=MySqlControlledDebugImages.put(executeScaffold,Locator,locator,root,true)
      }
      executeScaffold=MySqlControlledDebugImages.put(executeScaffold,Reservation,
        MySqlInactiveDomainOperationRows.release(held,plan.executeOperationUuid,root),root,true)
      val executeDraft=MySqlInactiveDomainOperationRows.commit(
        MySqlInactiveDomainOperationRows.preimage(executeSeed,executeBefore,root),seed,at,root)
      executeScaffold=MySqlControlledDebugImages.put(executeScaffold,Operation,executeDraft,root,true)
      executeScaffold=MySqlInactiveDomainOperationRows.writer(executeScaffold,plan.executeOperationUuid,seed,root)
      val executeTargetBytes=MySqlInactiveDomainCanonical.projection(executeScaffold,request,plan,context,
        MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Target,root)
      val executeFp=MySqlInactiveDomainCanonical.fingerprint(request,plan,context,MySqlOperationEffectPhase.Execute,
        scope,executeBefore,executeTargetBytes,root)
      val executeContext=standardContext(context,request,plan.executeOperationUuid,executeFp,root)
      val executeC=MySqlModeOperationEffectCodec.operationContextCanonicalBytes(executeContext,root.profile)
      val executeStub=MySqlInactiveDomainOperationRows.phase(executeSeed,executeC,executeBefore,executeTargetBytes,executeFp,root)
      val reserveTarget=MySqlControlledDebugImages.put(reserveBase,Operation,executeStub,root,true)
      val reserveRaw=MySqlRecoveryOperationRawEnvelopeCodec.encode(reserveFinal,root.profile)
      val executeWitness=MySqlInactiveDomainCanonical.witness(request,plan,MySqlOperationEffectPhase.Execute,at,
        scope,executeFp,executeC,executeBefore,executeTargetBytes,Some(reserveRaw),root)
      val executeFinal=MySqlInactiveDomainOperationRows.commit(MySqlInactiveDomainOperationRows.phase(executeSeed,
        executeC,executeBefore,executeTargetBytes,executeFp,root),executeWitness,at,root)
      var executeTarget=MySqlControlledDebugImages.put(executeScaffold,Operation,executeFinal,root,true)
      executeTarget=MySqlInactiveDomainOperationRows.writer(executeTarget,plan.executeOperationUuid,executeFp,root)
      val executePhase=new MySqlInactiveDomainPhase(request,plan,context,at,MySqlOperationEffectPhase.Execute,
        reserveTarget,executeTarget,scope,executeBefore,executeTargetBytes,executeFp,executeContext,executeC,executeWitness,
        Some(reserveRaw),root,locatorRevision)
      val reservePhase=new MySqlInactiveDomainPhase(request,plan,context,at,MySqlOperationEffectPhase.Reserve,
        before,reserveTarget,scope,reserveBefore,reserveTargetBytes,reserveFp,reserveContext,reserveC,reserveWitness,
        None,root,locatorRevision,Some(executePhase))
      val materialized=new MySqlInactiveDomainWorkflowMaterialization(request,at,plan,operation,context,root,
        before,reserveTarget,executeTarget,reservePhase,executePhase)
      materialized.verify()
      materialized
    }

  private def originalLocatorRevision(snapshot: MySqlFormat3FullSnapshot,request: MySqlInactiveDomainUpdateRequest): Option[Long] =
    var result: Option[Long]=None
    snapshot.relation(Locator).rows.foreach { row =>
      if MySqlModeOperationEffectCodec.uuidEquals(value(row,Locator,"pod_uuid"),request.podUuid) then
        if result.nonEmpty then fail("duplicate current locator")
        value(row,Locator,"locator_revision") match
          case MySqlFormat3Cell.SignedInteger(n) if n>=0 => result=Some(n)
          case _ => fail("exact original locator revision required")
    }
    result
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive two-phase materializer: "+reason)
