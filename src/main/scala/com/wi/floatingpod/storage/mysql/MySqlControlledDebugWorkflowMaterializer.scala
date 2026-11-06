package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 原规划 root 下的两阶段 DATA；本类型不发行 JDBC/commit ACK。 */
private[mysql] final class MySqlControlledDebugWorkflowMaterialization private[mysql] (
    val request: MySqlControlledDebugPrepareRequest,val activation: Boolean,val plannedAt: LocalDateTime,
    val plan: MySqlOwnedRepositoryOperationPlan,val operation: MySqlRepositoryOperation,
    val context: MySqlRepositoryOperationContextIdentity,val originalRoot: MySqlFormat3ReadBudget,
    private val preimageSource: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
    private val reserveSource: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
    private val executeSource: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,val reserve: MySqlControlledDebugPhase,
    val execute: MySqlControlledDebugPhase):
  def preimage:MySqlFormat3FullSnapshot=MySqlControlledDebugWorkflowMaterializer.legacyImage(preimageSource)
  def reserveTarget:MySqlFormat3FullSnapshot=MySqlControlledDebugWorkflowMaterializer.legacyImage(reserveSource)
  def executeTarget:MySqlFormat3FullSnapshot=MySqlControlledDebugWorkflowMaterializer.legacyImage(executeSource)
  private[mysql] def deploymentPreimage:MySqlScriptSchemaSnapshot=MySqlControlledDebugWorkflowMaterializer.deploymentImage(preimageSource)
  private[mysql] def deploymentReserveTarget:MySqlScriptSchemaSnapshot=MySqlControlledDebugWorkflowMaterializer.deploymentImage(reserveSource)
  private[mysql] def deploymentExecuteTarget:MySqlScriptSchemaSnapshot=MySqlControlledDebugWorkflowMaterializer.deploymentImage(executeSource)
  private[mysql] def isDeploymentReceipt:Boolean=preimageSource.isInstanceOf[MySqlScriptSchemaSnapshot]
  private[mysql] def completePreimage:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot=preimageSource
  private[mysql] def completeReserveTarget:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot=reserveSource
  private[mysql] def completeExecuteTarget:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot=executeSource
  private[mysql] def exactPlan: MySqlOwnedRepositoryOperationPlan = plan
  private[mysql] def exactOperation: MySqlRepositoryOperation = operation
  private[mysql] val effects: Vector[MySqlControlledDebugPhase] = Vector(reserve,execute)
  def requireBoundTo(scope: MySqlRepositoryCommandScope,current: MySqlRepositoryOperation): Unit =
    if scope==null || (scope.operationPlan() ne plan) || current!=operation then fail("original scoped command identity differs")
    scope.requireCurrent(scope.exactAuditScope(),plan)
    plan.requireSingle(current,originalRoot.profile)
    verify()

  def verify(): Unit =
    verifyUsingRoot(originalRoot)

  /** 仅 DATA 再验证；实际 caller 必须先证明当前 root 来自同 Session 当前原 commit plan。 */
  private[mysql] def verifyUsingRoot(currentOriginalRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    if plannedAt==null || plannedAt.getNano%1000!=0 || originalRoot==null || originalRoot.isClosed ||
      currentOriginalRoot==null || currentOriginalRoot.isClosed || currentOriginalRoot.isScopedReadBudget ||
      (currentOriginalRoot.profile ne originalRoot.profile) || !currentOriginalRoot.sharesAllocationLedger(originalRoot) ||
      reserve==null || execute==null then fail("original held materialization is incomplete or revoked")
    preimageSource match
      case _:MySqlFormat3FullSnapshot=>
        reserve.requireObservedUsingRoot(preimage,false,currentOriginalRoot,continuation)
        reserve.requireObservedUsingRoot(reserveTarget,true,currentOriginalRoot,continuation)
        execute.requireObservedUsingRoot(reserveTarget,false,currentOriginalRoot,continuation)
        execute.requireObservedUsingRoot(executeTarget,true,currentOriginalRoot,continuation)
      case _:MySqlScriptSchemaSnapshot=>
        if (currentOriginalRoot ne originalRoot) || continuation.nonEmpty then
          fail("Full26 materialization requires its original same-root phase owner")
        reserve.requireObservedDeploymentReceipt(deploymentPreimage,false,currentOriginalRoot)
        reserve.requireObservedDeploymentReceipt(deploymentReserveTarget,true,currentOriginalRoot)
        execute.requireObservedDeploymentReceipt(deploymentReserveTarget,false,currentOriginalRoot)
        execute.requireObservedDeploymentReceipt(deploymentExecuteTarget,true,currentOriginalRoot)

  def requireObserved(phase: MySqlOperationEffectPhase,actual: MySqlFormat3FullSnapshot,targetImage: Boolean): Unit =
    // 包括 reserve 读回中的 stub execute slots；完整 held 两阶段先独立再生成。
    verify()
    phase match
      case MySqlOperationEffectPhase.Reserve => reserve.requireObserved(actual,targetImage)
      case MySqlOperationEffectPhase.Execute => execute.requireObserved(actual,targetImage)

  private[mysql] def requireObservedDeploymentReceipt(phase:MySqlOperationEffectPhase,
      actual:MySqlScriptSchemaSnapshot,targetImage:Boolean):Unit =
    verify()
    phase match
      case MySqlOperationEffectPhase.Reserve=>reserve.requireObservedDeploymentReceipt(actual,targetImage,originalRoot)
      case MySqlOperationEffectPhase.Execute=>execute.requireObservedDeploymentReceipt(actual,targetImage,originalRoot)

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled held materialization: "+reason)

/** 每次读回独立再生成 cycle 字节，随后比较完整原图；没有 hash-only ACK。 */
private[mysql] final class MySqlControlledDebugPhase private[mysql] (
    val request: MySqlControlledDebugPrepareRequest,val activation: Boolean,val plan: MySqlOwnedRepositoryOperationPlan,
    val context: MySqlRepositoryOperationContextIdentity,val plannedAt: LocalDateTime,val phase: MySqlOperationEffectPhase,
    private val beforeSource: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
    private val targetSource: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,val scopeBytes: MySqlRecoveryCanonicalBytes,
    val beforeBytes: MySqlRecoveryCanonicalBytes,val targetBytes: MySqlRecoveryCanonicalBytes,
    val fingerprint: MySqlRecoveryCanonicalBytes,val operationContext: MySqlOperationContext,
    val contextBytes: MySqlRecoveryCanonicalBytes,val witness: MySqlRecoveryCanonicalBytes,
    val originalReserve: Option[MySqlRecoveryOperationRawEnvelope],val root: MySqlFormat3ReadBudget,
    private val reservedExecute: Option[MySqlControlledDebugPhase] = None):
  def before:MySqlFormat3FullSnapshot=MySqlControlledDebugWorkflowMaterializer.legacyImage(beforeSource)
  def target:MySqlFormat3FullSnapshot=MySqlControlledDebugWorkflowMaterializer.legacyImage(targetSource)
  private[mysql] def deploymentBefore:MySqlScriptSchemaSnapshot=MySqlControlledDebugWorkflowMaterializer.deploymentImage(beforeSource)
  private[mysql] def deploymentTarget:MySqlScriptSchemaSnapshot=MySqlControlledDebugWorkflowMaterializer.deploymentImage(targetSource)
  val operationUuid: UUID=if phase==MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid
  private[mysql] def exactOperationContext: MySqlOperationContext = operationContext
  private[mysql] def verifyPreimage(actual: MySqlFormat3FullSnapshot): Unit = requireObserved(actual,false)
  private[mysql] def verifyReadback(actual: MySqlFormat3FullSnapshot): Unit = requireObserved(actual,true)

  def requireObserved(actual: MySqlFormat3FullSnapshot,targetImage: Boolean): Unit =
    requireObservedUsingRoot(actual,targetImage,root)

  private[mysql] def requireObservedUsingRoot(actual: MySqlFormat3FullSnapshot,targetImage: Boolean,
      currentOriginalRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    if root==null || root.isClosed || currentOriginalRoot==null || currentOriginalRoot.isClosed || currentOriginalRoot.isScopedReadBudget ||
      (currentOriginalRoot.profile ne root.profile) || !currentOriginalRoot.sharesAllocationLedger(root) then
      fail("original held phase and exact shared current allocation root required")
    verifyObserved(actual,targetImage,currentOriginalRoot,continuation)

  private[mysql] def requireObservedDeploymentReceipt(actual:MySqlScriptSchemaSnapshot,targetImage:Boolean,
      currentRoot:MySqlFormat3ReadBudget):Unit =
    if currentRoot ne root then fail("Full26 phase requires the original same-root observation")
    verifyObserved(actual,targetImage,currentRoot,None)

  private def verifyObserved(actual: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,targetImage: Boolean,root: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]): Unit =
    if actual==null || root==null || root.isClosed then fail("original phase/root is unavailable")
    actual match
      case legacy:MySqlFormat3FullSnapshot=>
        MySqlControlledDebugWorkflowMaterializer.legacyImage(beforeSource)
        MySqlControlledDebugWorkflowMaterializer.legacyImage(targetSource)
        MySqlControlledDebugRelations.requireControlledClosure(legacy,request,plan,root,this.root,continuation)
      case deployment:MySqlScriptSchemaSnapshot=>
        if (root ne this.root) || continuation.nonEmpty || (deployment.inputs ne deploymentBefore.inputs) ||
            (deployment.inputs ne deploymentTarget.inputs) then fail("Full26 observed phase lost original inputs/root")
        MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(deployment,request,plan,root)
    if phase==MySqlOperationEffectPhase.Reserve && targetImage then
      val executePhase=reservedExecute.getOrElse(fail("original independently regenerated execute phase required"))
      if executePhase.phase!=MySqlOperationEffectPhase.Execute || executePhase.plan.ne(plan) ||
          executePhase.request.ne(request) || executePhase.beforeSource.ne(targetSource) then fail("original execute phase identity differs")
      actual match
        case legacy:MySqlFormat3FullSnapshot=>executePhase.requireObservedUsingRoot(legacy,false,root,continuation)
        case deployment:MySqlScriptSchemaSnapshot=>executePhase.requireObservedDeploymentReceipt(deployment,false,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
    // 从原固定 graph/context/plan 再生成；不从 actual current O 字段学习 expected。
    val regeneratedBefore=projection(beforeSource,MySqlOperationEffectImage.Before,root,continuation)
    val regeneratedTarget=projection(targetSource,MySqlOperationEffectImage.Target,root,continuation)
    val regeneratedFingerprint=MySqlControlledDebugCanonical.fingerprint(request,plan,context,phase,scopeBytes,
      regeneratedBefore,regeneratedTarget,root)
    val regeneratedContext=MySqlControlledDebugWorkflowMaterializer.standardContext(context,request,operationUuid,regeneratedFingerprint,root)
    val regeneratedContextBytes=MySqlModeOperationEffectCodec.operationContextCanonicalBytes(regeneratedContext,root.profile)
    val regeneratedWitness=MySqlControlledDebugCanonical.witness(request,plan,phase,plannedAt,scopeBytes,
      regeneratedFingerprint,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,originalReserve,root)
    if !beforeBytes.exactlyEquals(regeneratedBefore) || !targetBytes.exactlyEquals(regeneratedTarget) ||
      !fingerprint.exactlyEquals(regeneratedFingerprint) || !contextBytes.exactlyEquals(regeneratedContextBytes) ||
      !witness.exactlyEquals(regeneratedWitness) then fail("held phase differs from independently regenerated cycles")
    if targetImage then
      MySqlControlledDebugCycleChecks.requireOperation(operationRow(actual,operationUuid),request,plan,context,operationUuid,
        if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else if activation then "ACTIVATE" else "REARM",
        scopeBytes,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,regeneratedFingerprint,
        Some(regeneratedWitness),Some(plannedAt),root)
      MySqlControlledDebugCycleChecks.requireWriter(relation(actual,"fp_runtime_writer").rows.head,
        request.expectedBinding.bindingUuid,operationUuid,regeneratedContext)
      (beforeSource,actual) match
        case (before:MySqlFormat3FullSnapshot,observed:MySqlFormat3FullSnapshot)=>
          MySqlControlledDebugDelta.requireAllowed(before,observed,request,plan,activation,phase,root,this.root,continuation)
        case (before:MySqlScriptSchemaSnapshot,observed:MySqlScriptSchemaSnapshot)=>
          MySqlControlledDebugDelta.requireAllowedDeploymentReceipt(before,observed,request,plan,activation,phase,root)
        case _=>fail("cross-family phase delta")
    else if phase==MySqlOperationEffectPhase.Execute then
      MySqlControlledDebugCycleChecks.requireOperation(operationRow(actual,operationUuid),request,plan,context,operationUuid,
        if activation then "ACTIVATE" else "REARM",scopeBytes,regeneratedContextBytes,regeneratedBefore,regeneratedTarget,
        regeneratedFingerprint,None,None,root)
    originalReserve.foreach { raw =>
      raw.requireInputs(inputsForImage(actual, root))
      MySqlControlledDebugFullComparison.requireExactOperationRow(raw.row,operationRow(actual,plan.reserveOperationUuid),root)
      if !raw.exactlyMatches(operationRow(actual,plan.reserveOperationUuid),root.profile) then fail("original reserve full raw row differs")
    }
    val expectedSource=if targetImage then targetSource else beforeSource
    (expectedSource,actual) match
      case (expected:MySqlFormat3FullSnapshot,observed:MySqlFormat3FullSnapshot)=>
        MySqlControlledDebugFullComparison.requireExactReadback(expected,observed,root,this.root,continuation)
      case (expected:MySqlScriptSchemaSnapshot,observed:MySqlScriptSchemaSnapshot)=>
        MySqlControlledDebugFullComparison.requireExactDeploymentReceiptReadback(expected,observed,root)
      case _=>fail("cross-family phase readback")
    }

  private def relation(image:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,table:String):MySqlFormat3RelationSnapshot=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy.relation(table)
    case deployment:MySqlScriptSchemaSnapshot=>deployment.relation(table)
  private def projection(image:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
      side:MySqlOperationEffectImage,root:MySqlFormat3ReadBudget,
      continuation:Option[MySqlFencedTransaction.ControlledDebugContinuationProof]):MySqlRecoveryCanonicalBytes=image match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugCanonical.projection(legacy,request,plan,context,activation,phase,side,root,this.root,continuation)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugCanonical.projectionDeploymentReceipt(deployment,deployment.inputs,request,plan,context,activation,phase,side,root)
  private def operationRow(image: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,id: UUID): Vector[MySqlFormat3Cell] =
    def matches(row:Vector[MySqlFormat3Cell]):Boolean =
      MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(row,"fp_recovery_operation","binding_uuid"),request.expectedBinding.bindingUuid) &&
      MySqlModeOperationEffectCodec.uuidEquals(MySqlControlledDebugRows.value(row,"fp_recovery_operation","operation_uuid"),id)
    image match
      case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugImages.one(legacy,"fp_recovery_operation",matches)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.one(deployment,"fp_recovery_operation",matches)
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled independent phase proof: "+reason)

private[mysql] object MySqlControlledDebugWorkflowMaterializer:
  import MySqlControlledDebugRows.*
  private type Image=MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot
  private def imageProfile(image:Image)=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy.limitProfile
    case deployment:MySqlScriptSchemaSnapshot=>deployment.inputs.sourceLimitProfile
  private def imageBudget(image:Image)=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy.allocationBudget
    case deployment:MySqlScriptSchemaSnapshot=>deployment.allocationBudget
  private def imageRelations(image:Image)=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy.relations
    case deployment:MySqlScriptSchemaSnapshot=>deployment.relations
  private def relation(image:Image,table:String)=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy.relation(table)
    case deployment:MySqlScriptSchemaSnapshot=>deployment.relation(table)
  private def one(image:Image,table:String,matches:Vector[MySqlFormat3Cell]=>Boolean)=image match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugImages.one(legacy,table,matches)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.one(deployment,table,matches)
  private def put(image:Image,table:String,row:Vector[MySqlFormat3Cell],root:MySqlFormat3ReadBudget,existing:Boolean):Image=image match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugImages.put(legacy,table,row,root,existing)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.put(deployment,table,row,root,existing)
  private def put(image:Image,table:String,row:Vector[MySqlFormat3Cell],root:MySqlFormat3ReadBudget,
      allocation:MySqlFormat3ReadBudget,existing:Boolean):Image=image match
    case legacy:MySqlFormat3FullSnapshot=>
      if allocation ne root then fail("legacy Full20 patch cannot borrow Full26 plan DATA")
      MySqlControlledDebugImages.put(legacy,table,row,root,existing)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugImages.put(deployment,table,row,root,allocation,existing)
  private def writer(image:Image,id:UUID,fingerprint:Vector[Byte],root:MySqlFormat3ReadBudget):Image=image match
    case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugOperationRows.writer(legacy,id,fingerprint,root)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugOperationRows.writer(deployment,id,fingerprint,root)
  private def writer(image:Image,id:UUID,fingerprint:Vector[Byte],root:MySqlFormat3ReadBudget,
      allocation:MySqlFormat3ReadBudget):Image=image match
    case legacy:MySqlFormat3FullSnapshot=>
      if allocation ne root then fail("legacy Full20 writer cannot borrow Full26 plan DATA")
      MySqlControlledDebugOperationRows.writer(legacy,id,fingerprint,root)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugOperationRows.writer(deployment,id,fingerprint,root,allocation)
  private[mysql] def legacyImage(image:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot):MySqlFormat3FullSnapshot=image match
    case legacy:MySqlFormat3FullSnapshot=>legacy
    case _=>fail("legacy Full20 getter cannot borrow a Full26 phase")
  private[mysql] def deploymentImage(image:MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot):MySqlScriptSchemaSnapshot=image match
    case deployment:MySqlScriptSchemaSnapshot=>deployment
    case _=>fail("Full26 getter cannot borrow a legacy phase")
  private[mysql] def standardContext(context: MySqlRepositoryOperationContextIdentity,
      request: MySqlControlledDebugPrepareRequest,operation: UUID,fingerprint: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlOperationContext =
    standardContext(context,request,operation,fingerprint,root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private[mysql] def standardContext(context: MySqlRepositoryOperationContextIdentity,
      request: MySqlControlledDebugPrepareRequest,operation: UUID,fingerprint: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): MySqlOperationContext =
    if root==null || root.isClosed || root.isScopedReadBudget || allocation==null || allocation.isClosed ||
      (allocation.profile ne root.profile) || !allocation.sharesAllocationLedger(root) ||
      ((allocation ne root) && !allocation.isCanonicalChildOf(root)) then
      fail("standard context lost original root and same-ledger allocation")
    allocation.reserveRetainedMemory(4096L,"controlled standard context and exact single expected effect")
    MySqlOperationContext(context.tag.bootId,context.writerEpoch,request.expectedBinding.bindingUuid,operation,
      Vector(MySqlExpectedEffect("pod/"+request.podUuid.toString,0L,fingerprint.sha256)))

  def materialize(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,context: MySqlRepositoryOperationContextIdentity,
      at: LocalDateTime,before: MySqlFormat3FullSnapshot,activation: Boolean,activeLimit: Int,
      root: MySqlFormat3ReadBudget): MySqlControlledDebugWorkflowMaterialization =
    if context!=null && (context.schemaFormat!=3 || context.physicalFormat!=2) then
      fail("legacy controlled planning requires its original format3/physical2 context")
    materializeImage(request,plan,operation,context,at,before,activation,activeLimit,root,root)

  private[mysql] def materializeDeploymentReceipt(request:MySqlControlledDebugPrepareRequest,
      plan:MySqlOwnedRepositoryOperationPlan,operation:MySqlRepositoryOperation,
      context:MySqlRepositoryOperationContextIdentity,at:LocalDateTime,before:MySqlScriptSchemaSnapshot,
      activation:Boolean,activeLimit:Int,root:MySqlFormat3ReadBudget):MySqlControlledDebugWorkflowMaterialization =
    if before==null || context==null || context.schemaFormat!=5 || context.physicalFormat!=2 then
      fail("actual schema5 controlled planning context required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    materializeImage(request,plan,operation,context,at,before,activation,activeLimit,root,root)

  private[mysql] def materializeDeploymentReceipt(request:MySqlControlledDebugPrepareRequest,
      plan:MySqlOwnedRepositoryOperationPlan,operation:MySqlRepositoryOperation,
      context:MySqlRepositoryOperationContextIdentity,at:LocalDateTime,before:MySqlScriptSchemaSnapshot,
      activation:Boolean,activeLimit:Int,root:MySqlFormat3ReadBudget,
      planDataBudget:MySqlFormat3ReadBudget):MySqlControlledDebugWorkflowMaterialization =
    if before==null || context==null || context.schemaFormat!=5 || context.physicalFormat!=2 then
      fail("actual schema5 controlled planning context required")
    if root==null || root.isClosed || root.isScopedReadBudget || planDataBudget==null || planDataBudget.isClosed ||
      (planDataBudget.profile ne root.profile) || !planDataBudget.isCanonicalChildOf(root) ||
      !planDataBudget.sharesAllocationLedger(root) || (before.allocationBudget ne planDataBudget) then
      fail("exact original Full26 authority root and registered plan DATA image required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      plan.requireSingle(operation,root.profile)
    }
    materializeImage(request,plan,operation,context,at,before,activation,activeLimit,root,planDataBudget,
      validatePlan=false)

  private def materializeImage(request:MySqlControlledDebugPrepareRequest,plan:MySqlOwnedRepositoryOperationPlan,
      operation:MySqlRepositoryOperation,context:MySqlRepositoryOperationContextIdentity,
      at:LocalDateTime,before:Image,activation:Boolean,activeLimit:Int,
      root:MySqlFormat3ReadBudget,allocation:MySqlFormat3ReadBudget,
      validatePlan:Boolean=true):MySqlControlledDebugWorkflowMaterialization =
    if request==null || plan==null || operation==null || context==null || before==null || at==null ||
      root==null || root.isClosed || root.isScopedReadBudget || (root.profile ne imageProfile(before)) ||
      (context.exactLimitProfile ne root.profile) || !root.sharesAllocationLedger(imageBudget(before)) ||
      allocation==null || allocation.isClosed || (allocation.profile ne root.profile) ||
      !allocation.sharesAllocationLedger(root) || ((allocation ne root) && !allocation.isCanonicalChildOf(root)) ||
      (before.isInstanceOf[MySqlScriptSchemaSnapshot] && (imageBudget(before) ne allocation)) ||
      context.tag!=plan.tag || at.getNano%1000!=0 || at.getYear<1000 || at.getYear>9999 ||
      activeLimit<1 || activeLimit>256 then fail("exact original planning inputs and fixed DATETIME6 required")
    if validatePlan then plan.requireSingle(operation,root.profile)
    if !activation && operation!=request.repositoryOperation then fail("prepare operation differs from fixed original request")
    if activation then operation match
      case MySqlRepositoryOperation.CommitControlledDebugCreate(binding,pod,attempt,footprint,version,pose,create,limit)
          if binding==request.expectedBinding && pod==request.podUuid && attempt==request.attemptUuid &&
            footprint==request.footprintUuid && version==0L && create==request.createOperationUuid &&
            limit==activeLimit && exactPose(pose,request.targetPose) => ()
      case _ => fail("activate operation differs from the original prepared tuple/pose")
    before match
      case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugRelations.requireControlledClosure(legacy,request,plan,root)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(deployment,request,plan,root)
    def project(image:Image,phase:MySqlOperationEffectPhase,side:MySqlOperationEffectImage):MySqlRecoveryCanonicalBytes=image match
      case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugCanonical.projection(legacy,request,plan,context,activation,phase,side,root)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugCanonical.projectionDeploymentReceipt(deployment,deployment.inputs,request,plan,context,activation,phase,side,root,allocation)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      allocation.reserveRetainedMemory(16384L,"controlled materializer/phase wrappers and closures")
      requireDomainPreimage(before,request,activation,activeLimit)
      val key=if allocation eq root then MySqlRecoveryResourceKey.pod(request.podUuid,root.profile)
        else MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
          MySqlRecoveryResourceKey.pod(request.podUuid,root.profile)
        }
      val scope=MySqlControlledDebugCanonical.scope(request,context,activation,key,root,allocation)
      val seed=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlRecoveryCanonicalBytes.encode("FP_CONTROLLED_PLANNING_SEED_BE_V1",root.profile) { output =>
          MySqlRecoveryCanonicalBytes.writeToken(output,"FP_CONTROLLED_PLANNING_SEED_V1");output.writeInt(0)
        }
      }
      val reserveBefore=project(before,MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Before)
      val reserveSeed=MySqlControlledDebugOperationRows.preimage(
        MySqlControlledDebugOperationRows.operation(request,plan,context,at,activation,true,scope,seed,root,allocation),reserveBefore,root,allocation)
      val executeSeed=MySqlControlledDebugOperationRows.operation(request,plan,context,at,activation,false,scope,seed,root,allocation)
      val (held,hadReservation)=before match
        case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugOperationRows.reservation(legacy,request,plan,key,root)
        case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugOperationRows.reservation(deployment,request,plan,key,root,allocation)
      var reserveScaffold=put(before,Operation,reserveSeed,root,allocation,false)
      reserveScaffold=put(reserveScaffold,Operation,executeSeed,root,allocation,false)
      reserveScaffold=put(reserveScaffold,Reservation,held,root,allocation,hadReservation)
      reserveScaffold=writer(reserveScaffold,plan.reserveOperationUuid,seed.sha256,root,allocation)
      val reserveTargetBytes=project(reserveScaffold,MySqlOperationEffectPhase.Reserve,MySqlOperationEffectImage.Target)
      val reserveFp=MySqlControlledDebugCanonical.fingerprint(request,plan,context,MySqlOperationEffectPhase.Reserve,
        scope,reserveBefore,reserveTargetBytes,root,allocation)
      val reserveContext=standardContext(context,request,plan.reserveOperationUuid,reserveFp,root,allocation)
      val reserveC=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(reserveContext,root.profile)
      }
      val reserveWitness=MySqlControlledDebugCanonical.witness(request,plan,MySqlOperationEffectPhase.Reserve,at,
        scope,reserveFp,reserveC,reserveBefore,reserveTargetBytes,None,root,allocation)
      val reserveFinal=MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.phase(reserveSeed,
        reserveC,reserveBefore,reserveTargetBytes,reserveFp,root,allocation),reserveWitness,at,root,allocation)
      var reserveBase=put(reserveScaffold,Operation,reserveFinal,root,allocation,true)
      // W 保存事务 receipt 的完整标准 context 摘要；O 仍保存独立 phase fingerprint。
      reserveBase=writer(reserveBase,plan.reserveOperationUuid,reserveContext.canonicalFingerprintSha256,root,allocation)
      val executeBefore=project(reserveBase,MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Before)
      var executeScaffold=domainTarget(reserveBase,request,plan,at,activation,root,allocation)
      executeScaffold=put(executeScaffold,Reservation,
        MySqlControlledDebugOperationRows.release(held,plan.executeOperationUuid,root,allocation),root,allocation,true)
      val executeDraft=MySqlControlledDebugOperationRows.commit(
        MySqlControlledDebugOperationRows.preimage(executeSeed,executeBefore,root,allocation),seed,at,root,allocation)
      executeScaffold=put(executeScaffold,Operation,executeDraft,root,allocation,true)
      executeScaffold=writer(executeScaffold,plan.executeOperationUuid,seed.sha256,root,allocation)
      val executeTargetBytes=project(executeScaffold,MySqlOperationEffectPhase.Execute,MySqlOperationEffectImage.Target)
      val executeFp=MySqlControlledDebugCanonical.fingerprint(request,plan,context,MySqlOperationEffectPhase.Execute,
        scope,executeBefore,executeTargetBytes,root,allocation)
      val executeContext=standardContext(context,request,plan.executeOperationUuid,executeFp,root,allocation)
      val executeC=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(executeContext,root.profile)
      }
      val executeStub=MySqlControlledDebugOperationRows.phase(executeSeed,executeC,executeBefore,executeTargetBytes,executeFp,root,allocation)
      val reserveTarget=put(reserveBase,Operation,executeStub,root,allocation,true)
      val reserveRaw=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlRecoveryOperationRawEnvelopeCodec.encode(reserveFinal,root.profile,inputsForImage(before,root))
      }
      val executeWitness=MySqlControlledDebugCanonical.witness(request,plan,MySqlOperationEffectPhase.Execute,at,
        scope,executeFp,executeC,executeBefore,executeTargetBytes,Some(reserveRaw),root,allocation)
      val executeFinal=MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.phase(executeSeed,
        executeC,executeBefore,executeTargetBytes,executeFp,root,allocation),executeWitness,at,root,allocation)
      var executeTarget=put(executeScaffold,Operation,executeFinal,root,allocation,true)
      executeTarget=writer(executeTarget,plan.executeOperationUuid,executeContext.canonicalFingerprintSha256,root,allocation)
      val executePhase=new MySqlControlledDebugPhase(request,activation,plan,context,at,MySqlOperationEffectPhase.Execute,
        reserveTarget,executeTarget,scope,executeBefore,executeTargetBytes,executeFp,executeContext,executeC,executeWitness,Some(reserveRaw),root)
      val reservePhase=new MySqlControlledDebugPhase(request,activation,plan,context,at,MySqlOperationEffectPhase.Reserve,
        before,reserveTarget,scope,reserveBefore,reserveTargetBytes,reserveFp,reserveContext,reserveC,reserveWitness,None,root,Some(executePhase))
      val materialized=new MySqlControlledDebugWorkflowMaterialization(request,activation,at,plan,operation,context,root,
        before,reserveTarget,executeTarget,reservePhase,executePhase)
      materialized.verify()
      materialized
    }

  private def requireDomainPreimage(before: Image,request: MySqlControlledDebugPrepareRequest,
      activation: Boolean,activeLimit: Int): Unit =
    val rows=relation(before,Domain).rows
    var proposed=0
    var active=0
    var index=0
    while index<rows.size do
      val row=rows(index)
      value(row,Domain,"is_active") match
        case MySqlFormat3Cell.SignedInteger(0) => ()
        case MySqlFormat3Cell.SignedInteger(1) => active=Math.addExact(active,1)
        case _ => fail("actual global active flag differs from 0/1")
      if MySqlModeOperationEffectCodec.uuidEquals(value(row,Domain,"uuid"),request.podUuid) then
        proposed+=1
        if !activation || value(row,Domain,"state_version")!=signed(0) || value(row,Domain,"is_active")!=signed(0) ||
          value(row,Domain,"owner_uuid")!=Null || value(row,Domain,"operating_mode")!=text("WORLD_IDLE") then
          fail("controlled preimage is not the original inactive unclaimed v0 row")
      index+=1
    if proposed!=(if activation then 1 else 0) then fail("fresh absence and actual prepared presence differ")
    if !activation then
      var relationIndex=0
      val relations=imageRelations(before)
      while relationIndex<relations.size do
        val relation=relations(relationIndex)
        if MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==relation.table) &&
            !MySqlControlledDebugSelection.isSharedHeader(relation.table) then
          var rowIndex=0
          while rowIndex<relation.rows.size do
            if MySqlControlledDebugSelection.includes(relation.table,relation.rows(rowIndex),request) then
              fail("fresh controlled UUID already has managed or historical responsibility")
            rowIndex+=1
        relationIndex+=1
    if active>=activeLimit then fail("actual global active capacity is exhausted")

  private def domainTarget(before: Image,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,at: LocalDateTime,activation: Boolean,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Image =
    def podRow(table: String)=one(before,table,row =>
      MySqlModeOperationEffectCodec.uuidEquals(value(row,table,if table==Domain then "uuid" else "pod_uuid"),request.podUuid) &&
      MySqlModeOperationEffectCodec.uuidEquals(value(row,table,"binding_uuid"),request.expectedBinding.bindingUuid))
    var target=before
    if activation then
      val domain=activatedDomain(podRow(Domain),plan.executeOperationUuid,at,allocation)
      target=put(target,Domain,domain,root,allocation,true)
      val intent=one(before,Intent,row =>
        MySqlModeOperationEffectCodec.uuidEquals(value(row,Intent,"binding_uuid"),request.expectedBinding.bindingUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(value(row,Intent,"pod_uuid"),request.podUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(value(row,Intent,"attempt_uuid"),request.attemptUuid))
      target=put(target,Intent,activatedIntent(intent,domain,plan.executeOperationUuid,allocation),root,allocation,true)
      target=put(target,Locator,activatedLocator(podRow(Locator),plan.executeOperationUuid,allocation),root,allocation,true)
    else
      val binding=request.expectedBinding.bindingUuid
      target=put(target,Domain,preparedDomain(binding,request.podUuid,request.targetPose,plan.executeOperationUuid,at,allocation),root,allocation,false)
      target=put(target,Footprint,preparedFootprint(binding,request.podUuid,request.attemptUuid,
        request.footprintUuid,request.targetPose,plan.executeOperationUuid,allocation),root,allocation,false)
      val chunks=preparedChunks(binding,request.podUuid,request.attemptUuid,request.footprintUuid,request.targetPose,allocation)
      var index=0
      while index<chunks.size do
        target=put(target,Chunk,chunks(index),root,allocation,false);index+=1
      target=put(target,Intent,preparedIntent(binding,request.podUuid,request.attemptUuid,
        request.footprintUuid,plan.executeOperationUuid,request.createOperationUuid,allocation),root,allocation,false)
      target=put(target,Locator,preparedLocator(binding,request.podUuid,request.attemptUuid,
        request.footprintUuid,request.targetPose.dimensionId,plan.executeOperationUuid,allocation),root,allocation,false)
    target

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled two-phase materializer: "+reason)
  private[mysql] def exactPose(left: com.wi.floatingpod.domain.FloatingPodPose,right: com.wi.floatingpod.domain.FloatingPodPose): Boolean =
    left!=null && right!=null && left.worldName==right.worldName && left.dimensionId==right.dimensionId &&
      java.lang.Double.doubleToRawLongBits(left.posX)==java.lang.Double.doubleToRawLongBits(right.posX) &&
      java.lang.Double.doubleToRawLongBits(left.posY)==java.lang.Double.doubleToRawLongBits(right.posY) &&
      java.lang.Double.doubleToRawLongBits(left.posZ)==java.lang.Double.doubleToRawLongBits(right.posZ) &&
      java.lang.Float.floatToRawIntBits(left.yaw)==java.lang.Float.floatToRawIntBits(right.yaw) &&
      java.lang.Float.floatToRawIntBits(left.pitch)==java.lang.Float.floatToRawIntBits(right.pitch)
