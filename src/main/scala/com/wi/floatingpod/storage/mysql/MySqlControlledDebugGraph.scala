package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, PhysicalRecoveryLocatorReason, UnboundPreparationReceipt, UnboundIntentOrigin}
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import java.util.UUID

/** 原 held graph/plan 丢失时的显式责任拒绝；不能从 DATA/JSON 冷恢复 ACK。 */
private[mysql] final class MySqlControlledDebugNoProofException(reason: String)
    extends SQLNonTransientConnectionException("controlled debug NoProof: " + reason)

/** 原Main首次生成的四UUID与pose；本值是请求，不能发行数据库或物理资格。 */
final case class MySqlControlledDebugPrepareRequest(expectedBinding: PhysicalRecoveryBinding,
    podUuid: UUID, attemptUuid: UUID, footprintUuid: UUID, targetPose: FloatingPodPose,
    createOperationUuid: UUID):
  require(expectedBinding != null && expectedBinding.cutoverState == PhysicalRecoveryCutoverState.Certified && expectedBinding.formatVersion == 2,
    "actual certified controlled binding required")
  require(Vector(podUuid,attemptUuid,footprintUuid,createOperationUuid).forall(id => id != null && id != new UUID(0L,0L)) &&
    targetPose != null, "fixed original controlled identities and pose required")
  def repositoryOperation: MySqlRepositoryOperation =
    MySqlRepositoryOperation.PrepareControlledDebugCreate(expectedBinding,podUuid,attemptUuid,footprintUuid,targetPose,createOperationUuid)

/** 仅供实际F3专用runner完成commit与完整readback后装配；Main不能用receipt构造图。 */
final class MySqlControlledDebugPreparedGraph private[mysql] (
    val tag: IoOperationTag, val request: MySqlControlledDebugPrepareRequest,
    val receipt: UnboundPreparationReceipt, val state: FloatingPodState,
    val locator: PhysicalRecoveryLocator, val fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot,
    private[mysql] val prepareExecuteOperationUuid: UUID,
    private[mysql] val originalPreparePlan: MySqlOwnedRepositoryOperationPlan,
    private[mysql] val originalPrepareContext: MySqlRepositoryOperationContextIdentity,
    private[mysql] val issuerProof: MySqlFencedTransaction.ControlledDebugPostCommitReadback):
  require(issuerProof!=null && !issuerProof.materialization.activation &&
    (issuerProof.commandScope.operationPlan() eq originalPreparePlan) && issuerProof.commandScope.tag()==tag,
    "actual private controlled prepare readback issuer required")
  private[floatingpod] def hasPrivateReadbackProof: Boolean = issuerProof!=null
  def commitOperation(actualPose: FloatingPodPose,activeLimit: Int): MySqlRepositoryOperation =
    require(MySqlControlledDebugWorkflowMaterializer.exactPose(actualPose, receipt.targetPose) && activeLimit > 0 && activeLimit <= 256,
      "original prepared target and global capacity required")
    MySqlRepositoryOperation.CommitControlledDebugCreate(request.expectedBinding,request.podUuid,
      request.attemptUuid,request.footprintUuid,receipt.preparedVersion,actualPose,request.createOperationUuid,activeLimit)

final class MySqlControlledDebugCommittedGraph private[mysql] (
    val tag: IoOperationTag, val prepared: MySqlControlledDebugPreparedGraph,
    val state: FloatingPodState, val locator: PhysicalRecoveryLocator,
    val fullTwentyRelationSnapshot: MySqlOwnedFormat3Snapshot,
    private[mysql] val originalCommitPlan: MySqlOwnedRepositoryOperationPlan,
    private[mysql] val originalCommitContext: MySqlRepositoryOperationContextIdentity,
    private[mysql] val issuerProof: MySqlFencedTransaction.ControlledDebugPostCommitReadback):
  require(issuerProof!=null && issuerProof.materialization.activation &&
    (issuerProof.commandScope.operationPlan() eq originalCommitPlan) && issuerProof.commandScope.tag()==tag,
    "actual private controlled commit readback issuer required")
  private[floatingpod] def hasPrivateReadbackProof: Boolean = issuerProof!=null && prepared.hasPrivateReadbackProof

private[mysql] object MySqlControlledDebugGraph:
  private[mysql] type Image=MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot
  private def relations(snapshot:Image,table:String):MySqlFormat3RelationSnapshot=snapshot match
    case legacy:MySqlFormat3FullSnapshot=>legacy.relation(table)
    case deployment:MySqlScriptSchemaSnapshot=>deployment.relation(table)
  private def profile(snapshot:Image)=snapshot match
    case legacy:MySqlFormat3FullSnapshot=>legacy.limitProfile
    case deployment:MySqlScriptSchemaSnapshot=>deployment.inputs.sourceLimitProfile
  private def owned(snapshot:Image,budget:MySqlFormat3ReadBudget):MySqlOwnedFormat3Snapshot=snapshot match
    case legacy:MySqlFormat3FullSnapshot=>MySqlOwnedFormat3Snapshot.from(legacy,budget)
    case deployment:MySqlScriptSchemaSnapshot=>MySqlOwnedFormat3Snapshot.fromDeploymentReceipt(deployment,deployment.inputs,budget)
  private val I="physical_recovery_intent"
  private val F="physical_recovery_footprint"
  private val L="physical_recovery_locator"
  private val O="fp_recovery_operation"
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled F3 readback: " + reason)
  private def cell(row: Vector[MySqlFormat3Cell],table: String,name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  private def matches(row: Vector[MySqlFormat3Cell],table: String,name: String,id: UUID): Boolean =
    cell(row,table,name) match
      case MySqlFormat3Cell.Binary(value) => value.equalsUuid(id)
      case _ => false
  private def one(snapshot: Image,table: String,predicate: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
    var found: Vector[MySqlFormat3Cell] = null
    relations(snapshot,table).rows.foreach { row =>
      if predicate(row) then
        if found != null then fail("duplicate exact " + table + " identity")
        found=row
    }
    if found == null then fail("missing exact " + table + " identity")
    found
  private def common(tag: IoOperationTag,request: MySqlControlledDebugPrepareRequest,
      snapshot: Image,budget: MySqlFormat3ReadBudget,
      proof:MySqlFencedTransaction.ControlledDebugPostCommitReadback): MySqlFormat3QueryProjection = snapshot match
    case deployment:MySqlScriptSchemaSnapshot=>
      if tag==null || request==null || proof==null || !proof.deploymentSource.exists(_ eq deployment) ||
          deployment.observationScope!=MySqlFormat3ObservationScope(tag.bootId,tag.bootGeneration,tag.connectionGeneration) then
        fail("exact private Full26 completion source/generation required")
      MySqlDeploymentReceiptBootstrapGraph.requireBinding(deployment,PhysicalRecoveryCutoverState.Certified,request.expectedBinding.bindingUuid)
      val command=proof.commandScope
      MySqlFormat3QueryProjection.fromDeploymentReceipt(deployment,deployment.inputs,budget,
        proof.deploymentExpectation.getOrElse(fail("actual schema5 readback expectation missing")),
        command.requireBusinessAdmission(command.exactAuditScope(),command.operationPlan()))
    case legacy:MySqlFormat3FullSnapshot=>commonLegacy(tag,request,legacy,budget)

  private def commonLegacy(tag:IoOperationTag,request:MySqlControlledDebugPrepareRequest,
      snapshot:MySqlFormat3FullSnapshot,budget:MySqlFormat3ReadBudget):MySqlFormat3QueryProjection =
    if tag == null || request == null || snapshot == null || budget == null ||
      !budget.sharesAllocationLedger(snapshot.allocationBudget) || (budget.profile ne snapshot.limitProfile) || budget.isClosed then fail("exact full20 readback root required")
    if snapshot.observationScope.bootUuid != tag.bootId || snapshot.observationScope.bootGeneration != tag.bootGeneration ||
      snapshot.observationScope.connectionGeneration != tag.connectionGeneration then fail("completion generation")
    MySqlFreshFormat3Graph.requireBindingMatches(snapshot,Some(request.expectedBinding))
    MySqlFormat3QueryProjection.from(snapshot,budget)
  private def exactIntent(snapshot: Image,request: MySqlControlledDebugPrepareRequest): Vector[MySqlFormat3Cell] =
    val row=one(snapshot,I,r => matches(r,I,"binding_uuid",request.expectedBinding.bindingUuid) &&
      matches(r,I,"pod_uuid",request.podUuid) && matches(r,I,"attempt_uuid",request.attemptUuid))
    if cell(row,I,"origin") != MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE") ||
      !matches(row,I,"target_footprint_uuid",request.footprintUuid) ||
      !matches(row,I,"create_operation_uuid",request.createOperationUuid) then fail("original debug intent")
    row
  private def exactFootprint(snapshot: Image,request: MySqlControlledDebugPrepareRequest,
      prepareExecute: UUID): Vector[MySqlFormat3Cell] =
    val row=one(snapshot,F,r => matches(r,F,"binding_uuid",request.expectedBinding.bindingUuid) &&
      matches(r,F,"pod_uuid",request.podUuid) && matches(r,F,"attempt_uuid",request.attemptUuid) &&
      matches(r,F,"footprint_uuid",request.footprintUuid))
    if cell(row,F,"seal_state") != MySqlFormat3Cell.Text("SEALED") ||
      cell(row,F,"purpose") != MySqlFormat3Cell.Text("CREATE_TARGET") ||
      cell(row,F,"origin_version") != MySqlFormat3Cell.SignedInteger(0L) ||
      !matches(row,F,"created_operation_uuid",prepareExecute) then fail("original sealed footprint")
    row
  private def exactLocator(snapshot: Image,request: MySqlControlledDebugPrepareRequest): Unit =
    val row=one(snapshot,L,r => matches(r,L,"binding_uuid",request.expectedBinding.bindingUuid) && matches(r,L,"pod_uuid",request.podUuid))
    if !matches(row,L,"current_attempt_uuid",request.attemptUuid) ||
      !matches(row,L,"current_footprint_uuid",request.footprintUuid) then fail("current locator identity")
  private def terminalOperation(snapshot: Image,request: MySqlControlledDebugPrepareRequest,
      execute: UUID,step: String): Unit =
    val row=one(snapshot,O,r=>matches(r,O,"operation_uuid",execute))
    if !matches(row,O,"binding_uuid",request.expectedBinding.bindingUuid) ||
      !matches(row,O,"pod_uuid",request.podUuid) || !matches(row,O,"attempt_uuid",request.attemptUuid) ||
      !matches(row,O,"footprint_uuid",request.footprintUuid) || cell(row,O,"step") != MySqlFormat3Cell.Text(step) ||
      cell(row,O,"operation_state") != MySqlFormat3Cell.Text("COMMITTED") then fail("original terminal operation")
  def prepared(tag: IoOperationTag,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,
      prepareExecute: UUID,snapshot: Image,budget: MySqlFormat3ReadBudget,
      proof: MySqlFencedTransaction.ControlledDebugPostCommitReadback): MySqlControlledDebugPreparedGraph =
    requireProof(proof,tag,plan,snapshot,false)
    if plan == null || context == null || context.tag != tag || plan.executeOperationUuid != prepareExecute ||
      context.workflowUuid != plan.workflowUuid || context.reserveOperationUuid != plan.reserveOperationUuid then
      fail("original prepare plan/context identity")
    plan.requireSingle(request.repositoryOperation,profile(snapshot))
    val query=common(tag,request,snapshot,budget,proof)
    val intent=exactIntent(snapshot,request)
    exactFootprint(snapshot,request,prepareExecute); exactLocator(snapshot,request)
    terminalOperation(snapshot,request,prepareExecute,"REARM")
    if cell(intent,I,"phase") != MySqlFormat3Cell.Text("PREPARED") ||
      !matches(intent,I,"prepare_operation_uuid",prepareExecute) then fail("prepared intent phase")
    val receipt=query.findUnboundPreparation(request.podUuid).getOrElse(fail("preparation receipt"))
    val state=query.findByUuid(request.podUuid).getOrElse(fail("inactive prepared domain"))
    val locator=query.findPhysicalRecoveryLocator(request.podUuid).getOrElse(fail("prepared locator"))
    if receipt.origin != UnboundIntentOrigin.DebugItemCreate || receipt.observedIdentity.nonEmpty ||
      receipt.binding != request.expectedBinding || receipt.attemptUuid != request.attemptUuid ||
      !receipt.createOperationUuid.contains(request.createOperationUuid) || !MySqlControlledDebugWorkflowMaterializer.exactPose(receipt.targetPose, request.targetPose) ||
      receipt.preparedVersion != 0L || state.stateVersion != 0L || state.isActive || state.ownerUuid.nonEmpty ||
      state.operatingMode != PodOperatingMode.WorldIdle || !MySqlControlledDebugWorkflowMaterializer.exactPose(MySqlPreparedUnboundGraph.poseOf(state), request.targetPose) ||
      locator.state != PhysicalRecoveryLocatorState.DebugItemCreatePrepared ||
      locator.reasonCode != PhysicalRecoveryLocatorReason.DebugItemCreatePrepared then fail("complete target-zero prepared graph")
    budget.reserveRetainedMemory(1024L,"privately issued controlled prepared graph")
    new MySqlControlledDebugPreparedGraph(tag,request,receipt,state,locator,
      owned(snapshot,budget),prepareExecute,plan,context,proof)
  def committed(tag: IoOperationTag,prepared: MySqlControlledDebugPreparedGraph,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,execute: UUID,
      snapshot: Image,activeLimit: Int,budget: MySqlFormat3ReadBudget,
      proof: MySqlFencedTransaction.ControlledDebugPostCommitReadback): MySqlControlledDebugCommittedGraph =
    requireProof(proof,tag,plan,snapshot,true)
    if prepared == null || activeLimit < 1 || activeLimit > 256 then fail("original privately issued preparation and capacity required")
    val request=prepared.request
    if plan == null || context == null || context.tag != tag || plan.executeOperationUuid != execute ||
      context.workflowUuid != plan.workflowUuid || context.reserveOperationUuid != plan.reserveOperationUuid then
      fail("original commit plan/context identity")
    if plan.workflowUuid != prepared.originalPreparePlan.workflowUuid || tag.bootId != prepared.tag.bootId ||
      tag.bootGeneration != prepared.tag.bootGeneration || tag.connectionGeneration != prepared.tag.connectionGeneration then
      fail("foreign prepared workflow/session")
    plan.requireSingle(prepared.commitOperation(request.targetPose,activeLimit),profile(snapshot))
    val query=common(tag,request,snapshot,budget,proof)
    val intent=exactIntent(snapshot,request)
    exactFootprint(snapshot,request,prepared.prepareExecuteOperationUuid); exactLocator(snapshot,request)
    terminalOperation(snapshot,request,execute,"ACTIVATE")
    if cell(intent,I,"phase") != MySqlFormat3Cell.Text("ACTIVATED") ||
      !matches(intent,I,"prepare_operation_uuid",prepared.prepareExecuteOperationUuid) ||
      !matches(intent,I,"activation_operation_uuid",execute) then fail("original activated intent")
    val state=query.findByUuid(request.podUuid).getOrElse(fail("active domain"))
    val locator=query.findPhysicalRecoveryLocator(request.podUuid).getOrElse(fail("active locator"))
    if !state.isActive || state.ownerUuid.nonEmpty || state.operatingMode != PodOperatingMode.WorldIdle ||
      state.stateVersion != 1L || !MySqlControlledDebugWorkflowMaterializer.exactPose(MySqlPreparedUnboundGraph.poseOf(state), request.targetPose) ||
      locator.state != PhysicalRecoveryLocatorState.RowActive || locator.reasonCode != PhysicalRecoveryLocatorReason.RowActive ||
      !locator.domainVersion.contains(1L) || query.findAllActive().size > activeLimit then fail("committed unclaimed world-idle graph")
    budget.reserveRetainedMemory(1024L,"privately issued controlled committed graph")
    new MySqlControlledDebugCommittedGraph(tag,prepared,state,locator,owned(snapshot,budget),plan,context,proof)

  private def requireProof(proof: MySqlFencedTransaction.ControlledDebugPostCommitReadback,tag: IoOperationTag,
      plan: MySqlOwnedRepositoryOperationPlan,snapshot: Image,activation: Boolean): Unit =
    if proof==null || snapshot==null || plan==null || proof.commandScope.tag()!=tag ||
      (proof.commandScope.operationPlan() ne plan) || proof.materialization.activation!=activation ||
      snapshotMismatch(snapshot,proof) then
      fail("graph requires the exact private fresh readback proof")

  private def snapshotMismatch(snapshot:Image,proof:MySqlFencedTransaction.ControlledDebugPostCommitReadback):Boolean =
    snapshot match
      case legacy:MySqlFormat3FullSnapshot=>proof.deploymentSource.nonEmpty || proof.snapshotSchema!=legacy.schema ||
        proof.snapshotConnection!=legacy.connection || proof.snapshotScope!=legacy.observationScope ||
        proof.snapshotCanonicalSha256Hex!=legacy.canonicalSha256Hex
      case deployment:MySqlScriptSchemaSnapshot => !proof.deploymentSource.exists(_ eq deployment) ||
        proof.snapshotSchema!=deployment.schema || proof.snapshotConnection!=deployment.connection ||
        proof.snapshotScope!=deployment.observationScope || proof.snapshotCanonicalSha256Hex!=deployment.canonicalSha256Hex
