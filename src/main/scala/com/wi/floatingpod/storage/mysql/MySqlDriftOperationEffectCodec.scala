package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, EOFException, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.{DigestOutputStream, MessageDigest}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{Arrays, UUID}
import scala.collection.mutable
import MySqlModeOperationEffectCodec.{Image, allocationBudget, schema, connection, observationScope,
  maxAllowedPacketBytes, limitProfile, relations, relation, rowCount, shapes, originalImageRoot}

private[mysql] enum MySqlDriftPreflight:
  case Rejected(reason: MySqlTrackedDriftRejection)
  case Execute

private[mysql] final class MySqlDriftOperationEffect private[mysql] (
    val phase: MySqlOperationEffectPhase,
    val preimage: MySqlRecoveryCanonicalBytes,
    val target: MySqlRecoveryCanonicalBytes,
    val fingerprint: MySqlRecoveryCanonicalBytes,
    val preimageRawOperationEnvelopes: Vector[MySqlRecoveryOperationRawEnvelope],
    val rawOperationEnvelopes: Vector[MySqlRecoveryOperationRawEnvelope],
    private val plan: MySqlOwnedRepositoryOperationPlan,
    private val operation: MySqlRepositoryOperation,
    private val contextIdentity: MySqlRepositoryOperationContextIdentity,
    private val locatorRevision: Long,
    private val profile: SourceLimitProfile,
    private val expectedPreimage: Image,
    private val expectedTarget: Image,
    private val validatedOperationContext: MySqlOperationContext,
    private val authorityRoot: Option[MySqlFormat3ReadBudget]
):
  def fingerprintSha256: Vector[Byte] = fingerprint.sha256
  def fingerprintHex: String = fingerprint.hexSha256

  /** 返回已由最终full20逐byte核验的同一不可变context；不重编码、不读取时钟或数据库。 */
  private[mysql] def exactOperationContext: MySqlOperationContext =
    validatedOperationContext

  private def comparisonRoot(snapshot: Image): MySqlFormat3ReadBudget =
    authorityRoot match
      case Some(root) => root
      case None => MySqlModeOperationEffectCodec.originalImageRoot(snapshot,plan)

  private def requireAuthority(snapshot: Image): Unit = authorityRoot.foreach { root =>
    if snapshot == null || root.isClosed || root.isScopedReadBudget || (snapshot.limitProfile ne root.profile) then
      throw new SQLDataException("tracked drift effect lost its exact original authority root")
    snapshot match
      case deployment:MySqlScriptSchemaSnapshot =>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)
      case legacy:MySqlFormat3FullSnapshot =>
        if legacy.allocationBudget ne root then
          throw new SQLDataException("tracked legacy drift effect lost its exact original authority root")
  }

  /** 此阶段的完整 context 在 projection 后构造，以免 effects hash 回指自身。 */
  def requireOperationContext(context: MySqlOperationContext): Unit =
    requireAuthority(expectedTarget)
    MySqlDriftOperationEffectCodec.withGraphComparison(expectedTarget,Some(comparisonRoot(expectedTarget))) {
      if context == null then throw new IllegalArgumentException("exact MySQL operation context is required")
      val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
      val operationUuid = phase match
        case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
        case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
      if context.bootUuid != contextIdentity.tag.bootId || context.bindingUuid != request.binding.bindingUuid ||
        context.operationUuid != operationUuid || context.writerEpoch != contextIdentity.writerEpoch then
        throw new SQLDataException("operation context differs from this fixed mode phase")
      if context.expectedEffects.size != 1 ||
        context.expectedEffects.head.targetIdentity != ("pod/" + request.podUuid.toString) ||
        context.expectedEffects.head.expectedVersion != request.expectedStateVersion ||
        context.expectedEffects.head.desiredEffectFingerprintSha256 != fingerprint.sha256 then
        throw new SQLDataException("operation context does not bind the exact pod effect fingerprint")
      val currentUuid = phase match
        case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
        case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
      val envelope = rawOperationEnvelopes.find(_.operationUuid == currentUuid)
        .getOrElse(throw new SQLDataException("current phase operation raw envelope is missing"))
      MySqlDriftOperationEffectCodec.requireSuppliedOperationContext(context, envelope.row, profile, expectedTarget.allocationBudget)
    }

  /** 完整二十关系先逐值核对，再独立核受影响图 projection 和原始 O 封套。 */
  def verifyReadback(actual: Image): Unit =
    if actual == null then
      throw new SQLDataException("full format3 readback differs from the fixed mode target")
    requireAuthority(actual)
    MySqlDriftOperationEffectCodec.withGraphComparison(expectedTarget,Some(comparisonRoot(expectedTarget))) {
      if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedTarget,actual) then
        throw new SQLDataException("full format3 readback differs from the fixed mode target")
    }
    MySqlDriftOperationEffectCodec.requireProjectionPlan(plan, operation, contextIdentity, locatorRevision, actual,authorityRoot)
    MySqlDriftOperationEffectCodec.withGraphComparison(expectedTarget,Some(comparisonRoot(expectedTarget))) {
      val projected = MySqlDriftOperationEffectCodec.projectValidatedImage(actual, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Target,None,authorityRoot)
      if !target.exactlyEquals(projected) then throw new SQLDataException("mode target projection readback differs")
      requireRawOperationEnvelopes(actual, rawOperationEnvelopes)
    }

  def verifyPreimage(actual: Image): Unit =
    if actual == null then
      throw new SQLDataException("full format3 preimage differs from its exact fixed snapshot")
    requireAuthority(actual)
    MySqlDriftOperationEffectCodec.withGraphComparison(expectedPreimage,Some(comparisonRoot(expectedPreimage))) {
      if !MySqlModeOperationEffectCodec.sameCompleteImage(expectedPreimage,actual) then
        throw new SQLDataException("full format3 preimage differs from its exact fixed snapshot")
    }
    MySqlDriftOperationEffectCodec.requireProjectionPlan(plan, operation, contextIdentity, locatorRevision, actual,authorityRoot)
    MySqlDriftOperationEffectCodec.withGraphComparison(expectedPreimage,Some(comparisonRoot(expectedPreimage))) {
      val projected = MySqlDriftOperationEffectCodec.projectValidatedImage(actual, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Before,None,authorityRoot)
      if !preimage.exactlyEquals(projected) then throw new SQLDataException("mode preimage projection differs")
      requireRawOperationEnvelopes(actual, preimageRawOperationEnvelopes)
    }

  private def requireRawOperationEnvelopes(actual: Image,
      expected: Vector[MySqlRecoveryOperationRawEnvelope]): Unit =
    val rows = actual.relation("fp_recovery_operation").rows.filter(row =>
      MySqlDriftOperationEffectCodec.uuidEquals(MySqlDriftOperationEffectCodec.value(
        row, "fp_recovery_operation", "binding_uuid"), MySqlDriftOperationEffectCodec.driftRequest(operation).binding.bindingUuid))
    expected.foreach { envelope =>
      envelope.requireInputs(inputsForImage(actual, comparisonRoot(actual)))
      val row = rows.find(value => MySqlDriftOperationEffectCodec.rowOperationUuid(value) == envelope.operationUuid)
        .getOrElse(throw new SQLDataException("a fixed raw operation row is missing from full readback"))
      if !envelope.exactlyMatches(row, profile) then
        throw new SQLDataException("raw full operation envelope differs from its fixed bytes")
    }

  override def toString: String =
    "MySqlDriftOperationEffect[phase=" + phase + ",fingerprint=" + fingerprintHex + "]"

/** 单个阶段在 targets 已确定后的一次性规范值；不持有 JDBC 或提交权限。 */
private[mysql] final case class MySqlDriftPhaseCanonical(
    preimage: MySqlRecoveryCanonicalBytes,
    target: MySqlRecoveryCanonicalBytes,
    scope: MySqlRecoveryCanonicalBytes,
    fingerprint: MySqlRecoveryCanonicalBytes,
    operationContext: MySqlOperationContext,
    operationContextBytes: MySqlRecoveryCanonicalBytes
)

private[mysql] object MySqlDriftOperationEffectCodec:
  private val ProjectionEncoding = "FP_MYSQL_TRACKED_DRIFT_EFFECT_PROJECTION_BE_V1"
  private val FingerprintEncoding = "FP_MYSQL_TRACKED_DRIFT_EFFECT_FINGERPRINT_BE_V1"
  private val ProjectionDomain = "com.wi.floatingpod.mysql-operation-effect-projection/v1"
  private val FingerprintDomain = "com.wi.floatingpod.mysql-operation-effect-fingerprint/v1"
  private val ManifestTable = "fp_schema_manifest"
  private val BindingTable = "physical_recovery_binding"
  private val BootstrapIntentTable = "binding_bootstrap_intent"
  private val WriterTable = "fp_runtime_writer"
  private val DomainTable = "floating_pod_state"
  private val LocatorTable = "physical_recovery_locator"
  private val FootprintTable = "physical_recovery_footprint"
  private val ChunkTable = "physical_recovery_footprint_chunk"
  private val IntentTable = "physical_recovery_intent"
  private val ObligationTable = "physical_recovery_obligation"
  private val CompletionTable = "fp_lifecycle_completion"
  private val MoveTable = "physical_footprint_transfer"
  private val OperationTable = "fp_recovery_operation"
  private val ReservationTable = "fp_recovery_reservation"
  private val SourceUnitTable = "physical_source_unit"
  private val SourceForestTable = "physical_source_forest"
  private val SourceTransferTable = "physical_source_transfer"
  private val SourceWriteTable = "physical_source_write"
  private val SourceObligationTable = "physical_source_obligation"
  private[mysql] final case class Request(binding: PhysicalRecoveryBinding, podUuid: UUID, owner: Option[UUID],
      expectedStateVersion: Long, attemptUuid: UUID, targetMode: PodOperatingMode, actualPose: FloatingPodPose, criticalBase: com.wi.floatingpod.domain.FloatingPodState)

  private def sameSnapshotContext(left: Image, right: Image): Unit =
    if left == null || right == null || left.schema != right.schema ||
      left.connection != right.connection || left.observationScope != right.observationScope ||
      left.maxAllowedPacketBytes != right.maxAllowedPacketBytes || left.limitProfile != right.limitProfile ||
      ((left,right) match
        case (_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot)=>false
        case (a:MySqlScriptSchemaSnapshot,b:MySqlScriptSchemaSnapshot)=>a.inputs ne b.inputs
        case _=>true) then
      fail("full20 images must keep the exact connection, scope, schema, packet observation and limit profile")

  /** 固定验证程序的Map/key/vector/zip元数据先按实际完整图行宽计上界。
    * 原typed cells/opaque payload只读，不复制；比较产生的canonical bytes在同ledger child内计费。
    */
  private[mysql] def withGraphComparison(snapshot: Image,
      rootOverride:Option[MySqlFormat3ReadBudget]=None)(body: => Unit): Unit =
    if snapshot == null then throw new IllegalArgumentException("actual full20 snapshot required for drift comparison")
    val root = rootOverride.getOrElse(snapshot.allocationBudget)
    snapshot match
      case deployment:MySqlScriptSchemaSnapshot=>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)
      case legacy:MySqlFormat3FullSnapshot=>
        if legacy.allocationBudget ne root then fail("drift comparison lost exact legacy image root")
    var structures = 4096L
    var relationIndex = 0
    while relationIndex < snapshot.relations.size do
      val rows = snapshot.relations(relationIndex).rows
      var rowIndex = 0
      while rowIndex < rows.size do
        structures = Math.addExact(structures,
          Math.addExact(256L, Math.multiplyExact(rows(rowIndex).size.toLong, 96L)))
        rowIndex += 1
      relationIndex += 1
    root.reserveTemporaryMemory(structures, "drift graph comparison structures")
    var primary: Throwable = null
    try MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { body }
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try root.releaseTemporaryMemory(structures, "drift graph comparison structures release")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  def buildWorkflow(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      reservePreimage: Image, reserveTarget: Image,
      executeTarget: Image): Vector[MySqlDriftOperationEffect] =
    buildWorkflowInternal(plan, operation, contextIdentity, locatorRevision, reservePreimage,
      reserveTarget, executeTarget, None, None, None, None, None)

  /** 只供同materializer已有canonical值复用；仍以最终真实full20重新编码逐byte核验。
    * 不接SQL/Main authority，不根据caller boolean跳过验证，也不签提交确认。
    */
  private[mysql] def buildMaterializedWorkflow(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, reservePreimage: Image,
      reserveTarget: Image, executeTarget: Image,
      reserveCanonical: MySqlDriftPhaseCanonical, executeCanonical: MySqlDriftPhaseCanonical,
      existingReserveEnvelope: MySqlRecoveryOperationRawEnvelope): Vector[MySqlDriftOperationEffect] =
    if reserveCanonical == null || executeCanonical == null || existingReserveEnvelope == null then
      throw new IllegalArgumentException("same-materialization canonical values and raw reserve envelope required")
    buildWorkflowInternal(plan, operation, contextIdentity, locatorRevision, reservePreimage,
      reserveTarget, executeTarget, Some(reserveCanonical), Some(executeCanonical), Some(existingReserveEnvelope), None, None)

  /** H3历史专用：authority root核完整来源，registered DATA child核本次物化产物。 */
  private[mysql] def buildMaterializedWorkflow(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, reservePreimage: Image,
      reserveTarget: Image, executeTarget: Image,
      reserveCanonical: MySqlDriftPhaseCanonical, executeCanonical: MySqlDriftPhaseCanonical,
      existingReserveEnvelope: MySqlRecoveryOperationRawEnvelope,
      authorityRoot:MySqlFormat3ReadBudget,planDataBudget:MySqlFormat3ReadBudget):Vector[MySqlDriftOperationEffect] =
    if reserveCanonical == null || executeCanonical == null || existingReserveEnvelope == null then
      throw new IllegalArgumentException("same-materialization canonical values and raw reserve envelope required")
    buildWorkflowInternal(plan, operation, contextIdentity, locatorRevision, reservePreimage,
      reserveTarget, executeTarget, Some(reserveCanonical), Some(executeCanonical), Some(existingReserveEnvelope),
      Some(authorityRoot), Some(planDataBudget))

  private def buildWorkflowInternal(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      reservePreimage: Image, reserveTarget: Image,
      executeTarget: Image, reserveCanonical: Option[MySqlDriftPhaseCanonical],
      executeCanonical: Option[MySqlDriftPhaseCanonical],
      existingReserveEnvelope: Option[MySqlRecoveryOperationRawEnvelope],
      authorityRootOverride:Option[MySqlFormat3ReadBudget],
      planDataBudget:Option[MySqlFormat3ReadBudget]): Vector[MySqlDriftOperationEffect] =
    validatePlan(plan, operation, contextIdentity, locatorRevision, reservePreimage, authorityRootOverride)
    sameSnapshotContext(reservePreimage, reserveTarget)
    sameSnapshotContext(reserveTarget, executeTarget)
    // H3可能由DATA child解码历史图；只有普通入口才允许从图/plan推导原root。
    val root = authorityRootOverride match
      case Some(explicitRoot) => explicitRoot
      case None => originalImageRoot(reservePreimage,plan)
    val outputBudget = planDataBudget.getOrElse(root)
    if authorityRootOverride.isDefined != planDataBudget.isDefined then
      fail("H3 drift effects require the original root and plan DATA child together")
    if root == null || root.isClosed || root.isScopedReadBudget || (root.profile ne reservePreimage.limitProfile) then
      fail("drift effects require the explicit exact original workflow authority root")
    planDataBudget match
      case None =>
        if (reserveTarget.allocationBudget ne root) || (executeTarget.allocationBudget ne root) then
          fail("drift effects must retain the exact original workflow allocation root")
      case Some(dataBudget) =>
        if dataBudget == null || dataBudget.isClosed || (dataBudget.profile ne root.profile) ||
          !dataBudget.isCanonicalChildOf(root) || !dataBudget.sharesAllocationLedger(root) ||
          dataBudget.isCanonicalComparisonChildOf(root) ||
          !reservePreimage.isInstanceOf[MySqlScriptSchemaSnapshot] ||
          ((reservePreimage.allocationBudget ne root) && (reservePreimage.allocationBudget ne dataBudget)) ||
          !reserveTarget.isInstanceOf[MySqlScriptSchemaSnapshot] || !executeTarget.isInstanceOf[MySqlScriptSchemaSnapshot] ||
          (reserveTarget.allocationBudget ne dataBudget) || (executeTarget.allocationBudget ne dataBudget) then
          fail("H3 drift effects require a root-linked preimage and exact same-ledger plan DATA target graphs")
        val source = reservePreimage.asInstanceOf[MySqlScriptSchemaSnapshot]
        MySqlFormat3QueryProjection.requireDeploymentSource(source,source.inputs,root)
    withGraphComparison(reservePreimage,Some(root)) {
      validatePreimage(reservePreimage, operation, contextIdentity, locatorRevision)
      validateReserveShape(plan, operation, reservePreimage, reserveTarget)
      validateExecuteShape(plan, operation, reserveTarget, executeTarget)
    }
    val rowCapacity = Math.addExact(Math.addExact(reservePreimage.relation(OperationTable).rows.size,
      reserveTarget.relation(OperationTable).rows.size), executeTarget.relation(OperationTable).rows.size)
    outputBudget.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(rowCapacity.toLong, 64L)),
      "drift workflow raw-envelope reference cache")
    // 唯一局部cache按已锁定三个完整图的实际O行数上界先保留；无全局registry/私有cap。
    val envelopes = new mutable.ArrayBuffer[MySqlRecoveryOperationRawEnvelope](rowCapacity)
    existingReserveEnvelope.foreach { envelope =>
      if !reserveTarget.relation(OperationTable).rows.exists(row => row eq envelope.row) then
        fail("same-materialization reserve envelope has no exact final row reference")
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        envelope.requireInputs(inputsForImage(reservePreimage, root))
        if !envelope.exactlyMatches(envelope.row, reservePreimage.limitProfile) then
          fail("same-materialization reserve envelope differs from its actual immutable row")
      }
      envelopes += envelope
    }
    val reserve = buildPhase(plan, operation, contextIdentity, locatorRevision,
      MySqlOperationEffectPhase.Reserve, reservePreimage, reserveTarget, reserveCanonical, envelopes, root,
      outputBudget, authorityRootOverride)
    val execute = buildPhase(plan, operation, contextIdentity, locatorRevision,
      MySqlOperationEffectPhase.Execute, reserveTarget, executeTarget, executeCanonical, envelopes, root,
      outputBudget, authorityRootOverride)
    withGraphComparison(reserveTarget,Some(root)) {
      val comparisonBudget=authorityRootOverride.map(_ => MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile))
      requireEnvelopeSetExactly(reserve.rawOperationEnvelopes, execute.preimageRawOperationEnvelopes,
        reservePreimage.limitProfile, "all historical closure O rows must remain raw exact at execute preimage")
      validatePlannedExecuteOperation(plan, operation, contextIdentity, locatorRevision,
        reserveTarget, execute, reservePreimage.limitProfile,comparisonBudget,authorityRootOverride)
    }
    Vector(reserve, execute)

  private def buildPhase(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, before: Image,
      target: Image, existingCanonical: Option[MySqlDriftPhaseCanonical],
      cache: mutable.ArrayBuffer[MySqlRecoveryOperationRawEnvelope],root:MySqlFormat3ReadBudget,
      outputBudget:MySqlFormat3ReadBudget,authorityRoot:Option[MySqlFormat3ReadBudget]): MySqlDriftOperationEffect =
    if authorityRoot.isEmpty && (root ne originalImageRoot(before,plan)) then
      fail("drift phase lost its exact original authority root")
    val phaseCanonical = existingCanonical match
      case None => canonicalPhase(plan, operation, contextIdentity, locatorRevision, phase, before, target)
      case Some(retained) =>
        withGraphComparison(target,Some(root)) {
          val actual = canonicalPhaseValidated(plan, operation, contextIdentity, locatorRevision, phase, before, target,
            None,authorityRoot)
          if !retained.preimage.exactlyEquals(actual.preimage) || !retained.target.exactlyEquals(actual.target) ||
            !retained.scope.exactlyEquals(actual.scope) || !retained.fingerprint.exactlyEquals(actual.fingerprint) ||
            !retained.operationContextBytes.exactlyEquals(actual.operationContextBytes) ||
            retained.operationContext != actual.operationContext then fail("retained canonical phase differs from final full20 graph")
        }
        retained
    val preimage = phaseCanonical.preimage
    val projectedTarget = phaseCanonical.target
    val request = driftRequest(operation)
    def encodeRows(snapshot: Image): Vector[MySqlRecoveryOperationRawEnvelope] =
      val rows = driftClosureOperationRows(snapshot, request.binding.bindingUuid, request.podUuid,
        Some(outputBudget),Some(root))
      outputBudget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(rows.size.toLong, 64L)),
        "drift immutable phase raw-envelope references")
      rows.map { row =>
        cache.find(envelope => envelope.row eq row) match
          case Some(existing) =>
            existing.requireInputs(inputsForImage(snapshot, root))
            existing
          case None =>
            val metadata = Math.addExact(2048L, Math.multiplyExact(row.size.toLong, 128L))
            outputBudget.reserveTemporaryMemory(metadata, "drift raw-envelope row encoding metadata")
            var primary: Throwable = null
            val encoded =
              try MySqlRecoveryCanonicalBytes.withAllocationBudget(outputBudget) {
                MySqlRecoveryOperationRawEnvelopeCodec.encode(row, snapshot.limitProfile, inputsForImage(snapshot, root))
              }
              catch
                case cause: Throwable =>
                  primary = cause
                  throw cause
              finally
                try outputBudget.releaseTemporaryMemory(metadata, "drift raw-envelope row encoding metadata release")
                catch
                  case cleanup: Throwable =>
                    if primary == null then throw cleanup
                    else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)
            cache += encoded
            encoded
      }
    val beforeEnvelopes = encodeRows(before)
    val targetEnvelopes = encodeRows(target)
    withGraphComparison(target,Some(root)) {
      val comparisonBudget=authorityRoot.map(_ => MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile))
      validateCurrentPhaseOperation(plan, operation, contextIdentity, locatorRevision, phase, target,
        preimage, projectedTarget, phaseCanonical.scope, phaseCanonical.fingerprint, before.limitProfile,
        comparisonBudget,authorityRoot)
    }
    new MySqlDriftOperationEffect(phase, preimage, projectedTarget, phaseCanonical.fingerprint, beforeEnvelopes, targetEnvelopes,
      plan, operation, contextIdentity, locatorRevision, before.limitProfile, before, target, phaseCanonical.operationContext,
      authorityRoot)

  /** 先从完整 before/target 生成 phase base，再导出 context；不读时钟、不签发任何 owner。 */
  private[mysql] def canonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: Image,
      target: Image): MySqlDriftPhaseCanonical =
    if plan == null || operation == null || contextIdentity == null || before == null || target == null || phase == null then
      throw new IllegalArgumentException("complete fixed mode phase inputs are required")
    validatePlan(plan, operation, contextIdentity, locatorRevision, before)
    sameSnapshotContext(before, target)
    canonicalPhaseValidated(plan, operation, contextIdentity, locatorRevision, phase, before, target)

  /** H3历史专用phase builder：root验证来源；phase canonical保留在精确plan DATA child。 */
  private[mysql] def canonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: Image,
      target: Image,authorityRoot:MySqlFormat3ReadBudget,planDataBudget:MySqlFormat3ReadBudget):MySqlDriftPhaseCanonical =
    requirePlanDataBudget(authorityRoot,planDataBudget)
    if plan == null || operation == null || contextIdentity == null || before == null || target == null || phase == null ||
      !before.isInstanceOf[MySqlScriptSchemaSnapshot] || !target.isInstanceOf[MySqlScriptSchemaSnapshot] ||
      ((before.allocationBudget ne authorityRoot) && (before.allocationBudget ne planDataBudget)) ||
      (target.allocationBudget ne planDataBudget) then
      fail("H3 Full26 phase requires original-root authority and exact plan DATA graphs")
    val beforeDeployment=before.asInstanceOf[MySqlScriptSchemaSnapshot]
    val targetDeployment=target.asInstanceOf[MySqlScriptSchemaSnapshot]
    if beforeDeployment.inputs ne targetDeployment.inputs then fail("H3 Full26 phase switched original source inputs")
    MySqlFormat3QueryProjection.requireDeploymentSource(beforeDeployment,beforeDeployment.inputs,authorityRoot)
    MySqlFormat3QueryProjection.requireDeploymentSource(targetDeployment,beforeDeployment.inputs,authorityRoot)
    validatePlan(plan, operation, contextIdentity, locatorRevision, before,Some(authorityRoot))
    sameSnapshotContext(before, target)
    canonicalPhaseValidated(plan, operation, contextIdentity, locatorRevision, phase, before, target,
      Some(planDataBudget),Some(authorityRoot))

  private def canonicalPhaseValidated(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: Image,
      target: Image,canonicalBudget:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): MySqlDriftPhaseCanonical =
    sameSnapshotContext(before, target)
    val preimage = projectValidatedImage(before, plan, operation, contextIdentity, locatorRevision,
      phase, MySqlOperationEffectImage.Before,canonicalBudget,authorityRoot)
    val projectedTarget = projectValidatedImage(target, plan, operation, contextIdentity, locatorRevision,
      phase, MySqlOperationEffectImage.Target,canonicalBudget,authorityRoot)
    val request = driftRequest(operation)
    val key = withCanonicalOutputBudget(canonicalBudget) {
      MySqlRecoveryResourceKey.pod(request.podUuid, before.limitProfile)
    }
    val locator = oneLocator(before, request.binding.bindingUuid, request.podUuid)
    val actualRevision = long(locator, LocatorTable, "locator_revision")
    if actualRevision != locatorRevision then fail("locked locator revision differs from the canonical resource scope")
    val scope = withCanonicalOutputBudget(canonicalBudget) {
      MySqlRecoveryOperationScope.encode(contextIdentity,
        Vector(MySqlRecoveryResourceRevision(key, locatorRevision, Some(request.attemptUuid))))
    }
    val fingerprint = withCanonicalOutputBudget(canonicalBudget) {
      MySqlRecoveryCanonicalBytes.encode(FingerprintEncoding, before.limitProfile) { output =>
      MySqlRecoveryCanonicalBytes.writeText(output, FingerprintDomain)
      output.writeByte(1)
      MySqlRecoveryCanonicalBytes.writeText(output, phaseName(phase))
      writeUuid(output, request.binding.bindingUuid)
      writeUuid(output, request.podUuid)
      writeUuid(output, request.attemptUuid)
      writeUuid(output, plan.workflowUuid)
      writeUuid(output, plan.reserveOperationUuid)
      writeUuid(output, plan.executeOperationUuid)
      output.writeInt(plan.requestIdentity.canonical.length)
      plan.requestIdentity.canonical.writeTo(output)
      output.writeInt(scope.length)
      scope.writeTo(output)
      output.writeInt(preimage.length)
      preimage.writeTo(output)
      output.writeInt(projectedTarget.length)
      projectedTarget.writeTo(output)
      }
    }
    val operationUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    val operationContext = phaseOperationContext(contextIdentity, request, operationUuid,
      fingerprint.sha256, before.limitProfile)
    val contextBytes = withCanonicalOutputBudget(canonicalBudget) {
      operationContextCanonical(operationContext, before.limitProfile)
    }
    MySqlDriftPhaseCanonical(preimage, projectedTarget, scope, fingerprint, operationContext, contextBytes)

  private def withCanonicalOutputBudget[A](budget:Option[MySqlFormat3ReadBudget])(body: => A):A = budget match
    case Some(value) => MySqlRecoveryCanonicalBytes.withAllocationBudget(value) { body }
    case None => body

  private[mysql] def operationContextCanonicalBytes(context: MySqlOperationContext,
      profile: SourceLimitProfile): MySqlRecoveryCanonicalBytes = operationContextCanonical(context, profile)

  private def driftClosureOperationRows(snapshot: Image, binding: UUID,
      pod: UUID,allocationOwner:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Vector[Vector[MySqlFormat3Cell]] =
    selectDriftClosure(snapshot, binding, pod,allocationOwner,authorityRoot).find(_._1.table == OperationTable)
      .map(_._2).getOrElse(Vector.empty)

  private def requireEnvelopeSetExactly(expected: Vector[MySqlRecoveryOperationRawEnvelope],
      actual: Vector[MySqlRecoveryOperationRawEnvelope], profile: SourceLimitProfile, label: String): Unit =
    val expectedById = expected.map(value => value.operationUuid -> value).toMap
    val actualById = actual.map(value => value.operationUuid -> value).toMap
    if expectedById.size != expected.size || actualById.size != actual.size ||
      expectedById.keySet != actualById.keySet || expectedById.exists { case (uuid, left) =>
        !left.exactlyMatches(actualById(uuid).row, profile)
      } then fail(label)

  private def validateCurrentPhaseOperation(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, targetSnapshot: Image,
      preimage: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      scope: MySqlRecoveryCanonicalBytes, fingerprint: MySqlRecoveryCanonicalBytes,
      profile: SourceLimitProfile,allocationOwner:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Unit =
    val request = driftRequest(operation)
    val operationUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    val closureRows = driftClosureOperationRows(targetSnapshot, request.binding.bindingUuid, request.podUuid,
      allocationOwner,authorityRoot)
    val row = closureRows.find(rowOperationUuid(_) == operationUuid).getOrElse(fail("current phase O row in fixed closure"))
    val step = if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "DOMAIN_MUTATION"
    requireOperationHeader(row, request, plan, operationUuid, step, "COMMITTED")
    validateOperationIdentity(row, targetSnapshot, request, plan, contextIdentity, locatorRevision, operationUuid)
    val expectedContext = phaseOperationContext(contextIdentity, request, operationUuid, fingerprint.sha256, profile)
    val expectedContextBytes = operationContextCanonical(expectedContext, profile)
    requireCanonicalCell(row, OperationTable, "original_context_bytes",
      expectedContextBytes, targetSnapshot.allocationBudget, "current O original context")
    requireShaCell(row, OperationTable, "original_context_sha256", expectedContextBytes.sha256)
    requireCanonicalCell(row, OperationTable, "scope_bytes", scope, targetSnapshot.allocationBudget, "current O scope")
    requireShaCell(row, OperationTable, "scope_sha256", scope.sha256)
    requireCanonicalCell(row, OperationTable, "request_identity_bytes", plan.requestIdentity.canonical,
      targetSnapshot.allocationBudget, "current O request identity")
    requireShaCell(row, OperationTable, "request_identity_sha256", plan.requestIdentitySha256)
    requireCanonicalCell(row, OperationTable, "preimage_bytes", preimage,
      targetSnapshot.allocationBudget, "current O preimage")
    requireShaCell(row, OperationTable, "preimage_sha256", preimage.sha256)
    requireCanonicalCell(row, OperationTable, "target_bytes", target,
      targetSnapshot.allocationBudget, "current O target")
    requireShaCell(row, OperationTable, "target_sha256", target.sha256)
    requireShaCell(row, OperationTable, "operation_fingerprint", fingerprint.sha256)
    validateWriterReceipt(targetSnapshot, operationUuid, fingerprint.sha256)

  private def validatePlannedExecuteOperation(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, reserveTarget: Image,
      execute: MySqlDriftOperationEffect, profile: SourceLimitProfile,
      allocationOwner:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Unit =
    val request = driftRequest(operation)
    val row = driftClosureOperationRows(reserveTarget, request.binding.bindingUuid, request.podUuid,
      allocationOwner,authorityRoot)
      .find(rowOperationUuid(_) == plan.executeOperationUuid).getOrElse(fail("RESERVED execute O in reserve target"))
    requireOperationHeader(row, request, plan, plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED")
    validateOperationIdentity(row, reserveTarget, request, plan, contextIdentity, locatorRevision, plan.executeOperationUuid)
    val expectedContext = phaseOperationContext(contextIdentity, request, plan.executeOperationUuid,
      execute.fingerprint.sha256, profile)
    val expectedContextBytes = operationContextCanonical(expectedContext, profile)
    requireCanonicalCell(row, OperationTable, "original_context_bytes", expectedContextBytes,
      reserveTarget.allocationBudget, "planned execute O original context")
    requireShaCell(row, OperationTable, "original_context_sha256", expectedContextBytes.sha256)
    val expectedScope = MySqlRecoveryOperationScope.encode(contextIdentity,
      Vector(MySqlRecoveryResourceRevision(MySqlRecoveryResourceKey.pod(request.podUuid, profile),
        locatorRevision, Some(request.attemptUuid))))
    requireCanonicalCell(row, OperationTable, "scope_bytes", expectedScope,
      reserveTarget.allocationBudget, "planned execute O scope")
    requireShaCell(row, OperationTable, "scope_sha256", expectedScope.sha256)
    requireCanonicalCell(row, OperationTable, "request_identity_bytes", plan.requestIdentity.canonical,
      reserveTarget.allocationBudget, "planned execute O request identity")
    requireShaCell(row, OperationTable, "request_identity_sha256", plan.requestIdentitySha256)
    requireCanonicalCell(row, OperationTable, "preimage_bytes", execute.preimage,
      reserveTarget.allocationBudget, "planned execute O preimage")
    requireShaCell(row, OperationTable, "preimage_sha256", execute.preimage.sha256)
    requireCanonicalCell(row, OperationTable, "target_bytes", execute.target,
      reserveTarget.allocationBudget, "planned execute O target")
    requireShaCell(row, OperationTable, "target_sha256", execute.target.sha256)
    requireShaCell(row, OperationTable, "operation_fingerprint", execute.fingerprint.sha256)
    if value(row, OperationTable, "reserve_operation_uuid") != binary(plan.reserveOperationUuid) ||
      value(row, OperationTable, "previous_step_operation_uuid") != binary(plan.reserveOperationUuid) then
      fail("planned execute O must point to the fixed committed reserve O")

  private def validateOperationIdentity(row: Vector[MySqlFormat3Cell], snapshot: Image,
      request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, operationUuid: UUID): Unit =
    val footprint = oneCurrentFootprint(snapshot, request)
    if !uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, OperationTable, "workflow_uuid"), plan.workflowUuid) ||
      !uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
      !uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid) ||
      !uuidEquals(value(row, OperationTable, "footprint_uuid"), uuidCell(value(footprint, FootprintTable, "footprint_uuid"), "current footprint")) ||
      value(row, OperationTable, "source_unit_uuid") != MySqlFormat3Cell.SqlNull ||
      value(row, OperationTable, "forest_uuid") != MySqlFormat3Cell.SqlNull ||
      !uuidEquals(value(row, OperationTable, "boot_uuid"), context.tag.bootId) ||
      long(row, OperationTable, "boot_generation") != context.tag.bootGeneration ||
      !uuidEquals(value(row, OperationTable, "connection_generation_uuid"), context.barrierGenerationUuid) ||
      long(row, OperationTable, "connection_id") != context.actualConnectionId ||
      long(row, OperationTable, "fence_epoch") != context.writerEpoch ||
      long(row, OperationTable, "expected_domain_version") != request.expectedStateVersion ||
      long(row, OperationTable, "expected_locator_revision") != locatorRevision ||
      value(row, OperationTable, "expected_source_revision") != MySqlFormat3Cell.SqlNull ||
      value(row, OperationTable, "fixed_lease_until") != MySqlFormat3Cell.DateTime(context.fixedLeaseUntil) ||
      (operationUuid == plan.reserveOperationUuid &&
        (value(row, OperationTable, "reserve_operation_uuid") != MySqlFormat3Cell.SqlNull ||
          value(row, OperationTable, "previous_step_operation_uuid") != MySqlFormat3Cell.SqlNull)) ||
      (operationUuid == plan.executeOperationUuid &&
        (value(row, OperationTable, "reserve_operation_uuid") != binary(plan.reserveOperationUuid) ||
          value(row, OperationTable, "previous_step_operation_uuid") != binary(plan.reserveOperationUuid))) then
      fail("complete fixed operation row context differs from Main plan and locked mode state")

  private def phaseOperationContext(context: MySqlRepositoryOperationContextIdentity, request: Request,
      operationUuid: UUID, fingerprint: Vector[Byte], profile: SourceLimitProfile): MySqlOperationContext =
    if fingerprint == null || fingerprint.size != 32 || context.exactLimitProfile != profile then
      fail("fixed operation context fingerprint/profile")
    MySqlOperationContext(context.tag.bootId, context.writerEpoch, request.binding.bindingUuid, operationUuid,
      Vector(MySqlExpectedEffect("pod/" + request.podUuid.toString, request.expectedStateVersion, fingerprint)))

  private def operationContextCanonical(context: MySqlOperationContext,
      profile: SourceLimitProfile): MySqlRecoveryCanonicalBytes =
    if context == null || profile == null then throw new IllegalArgumentException("exact operation context/profile is required")
    val domain = "com.wi.floatingpod.mysql-operation-context/v1".getBytes(StandardCharsets.US_ASCII)
    MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1", profile) { output =>
      writeLengthFramed(output, domain)
      MySqlRecoveryCanonicalBytes.writeUuid(output, context.bootUuid)
      output.writeLong(context.writerEpoch)
      MySqlRecoveryCanonicalBytes.writeUuid(output, context.bindingUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, context.operationUuid)
      output.writeInt(context.expectedEffects.size)
      context.expectedEffects.foreach { expected =>
        writeLengthFramed(output, expected.targetIdentity.getBytes(StandardCharsets.US_ASCII))
        output.writeLong(expected.expectedVersion)
        writeLengthFramed(output, expected.desiredEffectFingerprintSha256.toArray)
      }
    }

  private def writeLengthFramed(output: DataOutputStream, bytes: Array[Byte]): Unit =
    if output == null || bytes == null then throw new SQLDataException("operation context byte field is missing")
    output.writeInt(bytes.length)
    output.write(bytes)

  private[mysql] def requireSuppliedOperationContext(context: MySqlOperationContext,
      row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget): Unit =
    val expected = operationContextCanonical(context, profile)
    if context.canonicalFingerprintSha256 != expected.sha256 ||
      text(row, OperationTable, "context_encoding") != "FP_RECOVERY_OPERATION_BE_V1" then
      fail("supplied MySqlOperationContext canonical identity differs from the frozen row encoding")
    requireCanonicalCell(row, OperationTable, "original_context_bytes", expected, budget,
      "supplied current operation context")
    requireShaCell(row, OperationTable, "original_context_sha256", expected.sha256)

  private def requireCanonicalCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      expected: MySqlRecoveryCanonicalBytes, budget: MySqlFormat3ReadBudget, label: String): Unit =
    val actualCell = value(row, table, name)
    val actual = actualCell match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => fail("typed raw bytes required for " + label)
    if expected == null || budget == null || actual.length != expected.length then fail(label + " length differs")
    if !expected.exactlyMatchesCell(actualCell) then fail(label + " bytes differ")

  private def requireShaCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      expected: Vector[Byte]): Unit =
    if expected == null || expected.size != 32 || !binaryEquals(value(row, table, name), expected) then
      fail("exact SHA-256 cell " + table + "." + name)

  private def validateWriterReceipt(snapshot: Image,
      operationUuid: UUID, fingerprint: Vector[Byte]): Unit =
    val writer = oneWriter(snapshot)
    if !uuidEquals(value(writer, WriterTable, "last_commit_operation_uuid"), operationUuid) ||
      !binaryEquals(value(writer, WriterTable, "last_commit_fingerprint"), fingerprint) then
      fail("W receipt slot must match the same fixed operation fingerprint")

  private[mysql] def projectImage(snapshot: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage): MySqlRecoveryCanonicalBytes =
    validatePlan(plan, operation, contextIdentity, locatorRevision, snapshot)
    projectValidatedImage(snapshot, plan, operation, contextIdentity, locatorRevision, phase, image)

  /** H3历史入口：显式root核来源，原before图可由root或其精确DATA child持有。 */
  private[mysql] def projectImage(snapshot: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      authorityRoot:MySqlFormat3ReadBudget,planDataBudget:MySqlFormat3ReadBudget):MySqlRecoveryCanonicalBytes =
    if snapshot == null || authorityRoot == null || authorityRoot.isClosed || authorityRoot.isScopedReadBudget ||
      planDataBudget == null || planDataBudget.isClosed || (planDataBudget.profile ne authorityRoot.profile) ||
      !planDataBudget.isCanonicalChildOf(authorityRoot) || !planDataBudget.sharesAllocationLedger(authorityRoot) ||
      planDataBudget.isCanonicalComparisonChildOf(authorityRoot) ||
      !snapshot.isInstanceOf[MySqlScriptSchemaSnapshot] ||
      ((snapshot.allocationBudget ne authorityRoot) && (snapshot.allocationBudget ne planDataBudget)) then
      fail("H3 drift projection requires its explicit Full26 authority root and root-linked source graph")
    val deployment = snapshot.asInstanceOf[MySqlScriptSchemaSnapshot]
    MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,authorityRoot)
    validatePlan(plan, operation, contextIdentity, locatorRevision, snapshot,Some(authorityRoot))
    projectValidatedImage(snapshot, plan, operation, contextIdentity, locatorRevision, phase, image,
      Some(planDataBudget),Some(authorityRoot))

  /** 内部effect验证入口在同原root先调用；不返回资格/capability。 */
  private[mysql] def requireProjectionPlan(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, snapshot: Image,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Unit =
    validatePlan(plan, operation, contextIdentity, locatorRevision, snapshot,authorityRoot)

  /** 仅上述固定callchain在同immutable refs完成plan核验后使用；返回DATA，不签SQL权。 */
  private[mysql] def projectValidatedImage(snapshot: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      canonicalBudget:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): MySqlRecoveryCanonicalBytes =
    snapshot match
      case deployment:MySqlScriptSchemaSnapshot=>
        val root = authorityRoot match
          case Some(explicitRoot) => explicitRoot
          case None => originalImageRoot(snapshot,plan)
        return canonicalBudget match
          case Some(budget) =>
            requirePlanDataBudget(root,budget)
            if (deployment.allocationBudget ne root) && (deployment.allocationBudget ne budget) then
              fail("H3 drift projection source must be owned by the explicit root or exact plan DATA child")
            val request=driftRequest(operation)
            val closure=selectDriftClosure(deployment,request.binding.bindingUuid,request.podUuid,
              Some(budget),Some(root))
            projectClosure(closure,deployment.limitProfile,plan,request,locatorRevision,phase,image,Some(budget))
          case None =>
            projectImage(deployment,deployment.inputs,root,plan,operation,
              contextIdentity,locatorRevision,phase,image)
      case _:MySqlFormat3FullSnapshot=>()
    if contextIdentity!=null && (contextIdentity.schemaFormat!=3 || contextIdentity.physicalFormat!=2) then
      fail("legacy drift projection requires its original format3/physical2 context")
    val request = driftRequest(operation)
    val root=authorityRoot match
      case Some(explicitRoot) => explicitRoot
      case None => originalImageRoot(snapshot,plan)
    val closure = selectDriftClosure(snapshot, request.binding.bindingUuid, request.podUuid,
      canonicalBudget,Some(root))
    projectClosure(closure,snapshot.limitProfile,plan,request,locatorRevision,phase,image,canonicalBudget)

  private def requirePlanDataBudget(root:MySqlFormat3ReadBudget,budget:MySqlFormat3ReadBudget):Unit =
    if root==null || root.isClosed || root.isScopedReadBudget || budget==null || budget.isClosed ||
      (budget.profile ne root.profile) || !budget.isCanonicalChildOf(root) ||
      !budget.sharesAllocationLedger(root) || budget.isCanonicalComparisonChildOf(root) then
      fail("H3 drift output requires the exact same-ledger plan DATA child")

  private[mysql] def projectImage(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,plan:MySqlOwnedRepositoryOperationPlan,operation:MySqlRepositoryOperation,
      context:MySqlRepositoryOperationContextIdentity,locatorRevision:Long,
      phase:MySqlOperationEffectPhase,image:MySqlOperationEffectImage):MySqlRecoveryCanonicalBytes =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    if plan==null || context==null || locatorRevision<0L || phase==null || image==null then
      throw new IllegalArgumentException("complete original Full26 drift projection inputs required")
    val request=driftRequest(operation)
    if context.schemaFormat!=5 || context.physicalFormat!=2 || plan.requestKind!="RUN" || context.requestKind!="RUN" ||
      (plan.exactLimitProfile ne root.profile) || (context.exactLimitProfile ne root.profile) || context.tag!=plan.tag ||
      context.workflowUuid!=plan.workflowUuid || context.reserveOperationUuid!=plan.reserveOperationUuid ||
      context.requestIdentitySha256!=plan.requestIdentitySha256 || context.bindingUuid!=request.binding.bindingUuid ||
      plan.executeOperationUuid!=plan.tag.operationId || context.schema!=snapshot.schema ||
      !context.ddlSha256.equalsIgnoreCase(inputs.ddlSha256) || context.serverUuid!=snapshot.connection.serverUuid ||
      context.serverVersion!=snapshot.connection.serverVersion || context.effectiveUser!=snapshot.connection.effectiveUser ||
      context.tag.bootId!=snapshot.observationScope.bootUuid || context.tag.bootGeneration!=snapshot.observationScope.bootGeneration ||
      context.tag.connectionGeneration!=snapshot.observationScope.connectionGeneration then
      fail("Full26 drift projection lost exact source/plan/context identity")
    plan.originalRetainedHandle match
      case Some(_)=>plan.requireSingle(operation,root.profile)
      case None=>MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { plan.requireSingle(operation,root.profile) }
    // mode/drift 使用同一固定 FK 闭包，六个额外关系作为完整不可变上下文保留。
    val closure=MySqlModeOperationEffectCodec.selectModeClosure(snapshot,inputs,root,request.binding.bindingUuid,request.podUuid)
    projectClosure(closure,root.profile,plan,request,locatorRevision,phase,image)

  private def projectClosure(closure:Vector[(MySqlFormat3RelationShape,Vector[Vector[MySqlFormat3Cell]])],
      profile:SourceLimitProfile,plan:MySqlOwnedRepositoryOperationPlan,request:Request,locatorRevision:Long,
      phase:MySqlOperationEffectPhase,image:MySqlOperationEffectImage,
      allocationBudget:Option[MySqlFormat3ReadBudget]=None):MySqlRecoveryCanonicalBytes =
    def encode():MySqlRecoveryCanonicalBytes = MySqlRecoveryCanonicalBytes.encode(ProjectionEncoding, profile) { output =>
      MySqlRecoveryCanonicalBytes.writeText(output, ProjectionDomain)
      output.writeByte(1)
      MySqlRecoveryCanonicalBytes.writeText(output, phaseName(phase))
      MySqlRecoveryCanonicalBytes.writeText(output, if image == MySqlOperationEffectImage.Before then "BEFORE" else "TARGET")
      writeUuid(output, request.binding.bindingUuid)
      writeUuid(output, request.podUuid)
      writeUuid(output, request.attemptUuid)
      writeUuid(output, plan.workflowUuid)
      writeUuid(output, plan.reserveOperationUuid)
      writeUuid(output, plan.executeOperationUuid)
      output.writeLong(request.expectedStateVersion)
      MySqlRecoveryCanonicalBytes.writeText(output, request.targetMode.databaseValue)
      output.writeLong(locatorRevision)
      output.writeInt(closure.size)
      closure.foreach { case (shape, selectedRows) =>
        MySqlRecoveryCanonicalBytes.writeText(output, shape.table)
        output.writeInt(shape.columns.size)
        shape.columns.foreach { column =>
          MySqlRecoveryCanonicalBytes.writeText(output, column.name)
          MySqlRecoveryCanonicalBytes.writeText(output, column.sqlDeclaration)
        }
        output.writeInt(selectedRows.size)
        selectedRows.foreach { row =>
          val key = shape.primaryKey.map(name => row(shape.columns.indexWhere(_.name==name)))
          shape.columns.zipWithIndex.foreach { (column, indexValue) =>
            if markerRequired(phase, image, shape.table, key, column.name, plan) then
              writeMarker(output, phase, shape, key, column, row(indexValue))
            else writeCell(output, row(indexValue))
          }
        }
      }
    }
    allocationBudget match
      case Some(budget) => MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) { encode() }
      case None => encode()

  private[mysql] def selectDriftClosure(snapshot: Image, binding: UUID,
      pod: UUID,allocationOwner:Option[MySqlFormat3ReadBudget]=None,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Vector[(MySqlFormat3RelationShape, Vector[Vector[MySqlFormat3Cell]])] =
    snapshot match
      case deployment:MySqlScriptSchemaSnapshot=>
        allocationOwner match
          case None => return MySqlModeOperationEffectCodec.selectModeClosure(deployment,binding,pod)
          case Some(owner) =>
            val root=authorityRoot.getOrElse(snapshot.allocationBudget)
            val imageOwner=deployment.allocationBudget
            if root==null || root.isClosed || root.isScopedReadBudget || owner==null || owner.isClosed ||
              (owner.profile ne root.profile) || !owner.sharesAllocationLedger(root) ||
              ((owner ne root) && !owner.isCanonicalChildOf(root)) ||
              (imageOwner.profile ne root.profile) || !imageOwner.sharesAllocationLedger(root) ||
              ((imageOwner ne root) && (imageOwner ne owner) &&
                (!imageOwner.isCanonicalChildOf(root) || imageOwner.isCanonicalComparisonChildOf(root))) then
              fail("Full26 drift closure requires the exact root and its same-ledger direct child")
            return MySqlRecoveryCanonicalBytes.withAllocationBudget(owner) {
              MySqlModeOperationEffectCodec.selectModeClosure(deployment,deployment.inputs,root,binding,pod)
            }
      case _:MySqlFormat3FullSnapshot=>()
    val owner=allocationOwner.getOrElse(snapshot.allocationBudget)
    allocationOwner.foreach { budget =>
      val root=authorityRoot.getOrElse(snapshot.allocationBudget)
      val imageOwner=snapshot.allocationBudget
      if root==null || root.isClosed || root.isScopedReadBudget || budget==null || budget.isClosed ||
        (budget.profile ne root.profile) || !budget.sharesAllocationLedger(root) ||
        ((budget ne root) && !budget.isCanonicalChildOf(root)) ||
        (imageOwner.profile ne root.profile) || !imageOwner.sharesAllocationLedger(root) ||
        ((imageOwner ne root) && (imageOwner ne budget) &&
          (!imageOwner.isCanonicalChildOf(root) || imageOwner.isCanonicalComparisonChildOf(root))) then
        fail("Full20 drift closure requires its exact root and same-ledger direct owner")
    }
    val estimate = Math.multiplyExact(snapshot.rowCount, 48L)
    owner.reserveTemporaryMemory(estimate, "mode closure selection")
    var primary: Throwable = null
    try
      val byTable = snapshot.relations.map(value => value.table -> value.rows).toMap
      val selectedKeys = mutable.HashMap.empty[String, mutable.HashSet[Vector[MySqlFormat3Cell]]]
      val attempts = mutable.HashSet.empty[Vector[Byte]]
      val footprints = mutable.HashSet.empty[Vector[Byte]]
      val units = mutable.HashSet.empty[Vector[Byte]]
      val forests = mutable.HashSet.empty[Vector[Byte]]
      val operations = mutable.HashSet.empty[Vector[Byte]]
      val writes = mutable.HashSet.empty[Vector[Byte]]
      val carrierKeys = mutable.HashSet.empty[(String, Vector[Byte])]
      var changed = true
      var rounds = 0

      def add(table: String, row: Vector[MySqlFormat3Cell]): Boolean =
        val shape = MySqlFormat3RelationShapes.forTable(table)
        val key = shape.primaryKey.map(name => row(index(table, name)))
        selectedKeys.getOrElseUpdate(table, mutable.HashSet.empty[Vector[MySqlFormat3Cell]]).add(key)

      def selected(table: String): Vector[Vector[MySqlFormat3Cell]] =
        val keys = selectedKeys.get(table).map(_.toSet).getOrElse(Set.empty[Vector[MySqlFormat3Cell]])
        byTable(table).filter(row =>
          keys.contains(MySqlFormat3RelationShapes.forTable(table).primaryKey.map(name => row(index(table, name)))))

      while changed do
        changed = false
        rounds = Math.addExact(rounds, 1)
        if rounds > 48 then fail("fixed FK closure exceeds 48 monotone passes")

        def take(table: String)(predicate: Vector[MySqlFormat3Cell] => Boolean): Unit =
          byTable(table).foreach { row =>
            if predicate(row) && add(table, row) then
              changed = true
              MySqlFormat3RelationShapes.forTable(table).columns.foreach { column =>
                val item = row(index(table, column.name))
                val name = column.name
                if name == "attempt_uuid" || name == "current_attempt_uuid" || name == "predecessor_attempt_uuid" then addUuid(attempts, item)
                if name == "footprint_uuid" || name == "current_footprint_uuid" || name == "target_footprint_uuid" ||
                  name == "from_footprint_uuid" || name == "to_footprint_uuid" || name == "predecessor_footprint_uuid" then
                  addUuid(footprints, item)
                if name == "source_unit_uuid" || name == "unit_uuid" then addUuid(units, item)
                if name == "forest_uuid" || name == "source_forest_uuid" || name == "old_forest_uuid" ||
                  name == "target_forest_uuid" || name == "current_forest_uuid" then addUuid(forests, item)
                if name == "write_uuid" then addUuid(writes, item)
                if isOperationReference(name) then addUuid(operations, item)
                if name == "operation_uuid" && table == OperationTable then addUuid(operations, item)
                if name == "carrier_key_hash" then
                  (item, value(row, table, "resource_kind")) match
                    case (MySqlFormat3Cell.Binary(hash), MySqlFormat3Cell.Text(kind)) => carrierKeys.add(kind -> keyBytes(hash))
                    case _ => ()
              }
          }

        take(DomainTable)(row => uuidEquals(value(row, DomainTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, DomainTable, "uuid"), pod))
        take(ManifestTable)(row => long(row, ManifestTable, "singleton_fixed1") == 1L)
        take(BindingTable)(row => long(row, BindingTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, BindingTable, "binding_uuid"), binding))
        take(WriterTable)(row => long(row, WriterTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, WriterTable, "binding_uuid"), binding))
        take(BootstrapIntentTable)(row => long(row, BootstrapIntentTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, BootstrapIntentTable, "binding_uuid"), binding))
        take(LocatorTable)(row => uuidEquals(value(row, LocatorTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, LocatorTable, "pod_uuid"), pod))
        take(FootprintTable)(row => uuidEquals(value(row, FootprintTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, FootprintTable, "pod_uuid"), pod))
        take(IntentTable)(row => uuidEquals(value(row, IntentTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, IntentTable, "pod_uuid"), pod))
        take(ChunkTable)(row => uuidEquals(value(row, ChunkTable, "binding_uuid"), binding) &&
          footprints.contains(uuidBytes(value(row, ChunkTable, "footprint_uuid"))))
        take(ObligationTable)(row => uuidEquals(value(row, ObligationTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, ObligationTable, "pod_uuid"), pod) &&
          attempts.contains(uuidBytes(value(row, ObligationTable, "attempt_uuid"))) &&
          footprints.contains(uuidBytes(value(row, ObligationTable, "footprint_uuid"))))
        take(CompletionTable)(row => uuidEquals(value(row, CompletionTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, CompletionTable, "pod_uuid"), pod))
        take(MoveTable)(row => uuidEquals(value(row, MoveTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, MoveTable, "pod_uuid"), pod))
        take(OperationTable)(row => uuidEquals(value(row, OperationTable, "binding_uuid"), binding) &&
          (uuidEquals(value(row, OperationTable, "pod_uuid"), pod) ||
            attempts.contains(uuidBytes(value(row, OperationTable, "attempt_uuid"))) ||
            footprints.contains(uuidBytes(value(row, OperationTable, "footprint_uuid"))) ||
            units.contains(uuidBytes(value(row, OperationTable, "source_unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, OperationTable, "forest_uuid"))) ||
            operations.contains(uuidBytes(value(row, OperationTable, "operation_uuid")))))
        take(SourceUnitTable)(row => uuidEquals(value(row, SourceUnitTable, "binding_uuid"), binding) &&
          units.contains(uuidBytes(value(row, SourceUnitTable, "unit_uuid"))))
        take(SourceForestTable)(row => uuidEquals(value(row, SourceForestTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceForestTable, "unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, SourceForestTable, "forest_uuid")))))
        take(SourceTransferTable)(row => uuidEquals(value(row, SourceTransferTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceTransferTable, "unit_uuid"))) ||
            operations.contains(uuidBytes(value(row, SourceTransferTable, "prepare_operation_uuid")))))
        take(SourceWriteTable)(row => uuidEquals(value(row, SourceWriteTable, "binding_uuid"), binding) &&
          (writes.contains(uuidBytes(value(row, SourceWriteTable, "write_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, SourceWriteTable)) ||
            referencesOperation(row, SourceWriteTable, operations)))
        take(SourceObligationTable)(row => uuidEquals(value(row, SourceObligationTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceObligationTable, "unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, SourceObligationTable, "forest_uuid"))) ||
            writes.contains(uuidBytes(value(row, SourceObligationTable, "write_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, SourceObligationTable)) ||
            referencesOperation(row, SourceObligationTable, operations)))
        take(ReservationTable)(row => uuidEquals(value(row, ReservationTable, "binding_uuid"), binding) &&
          (uuidEquals(value(row, ReservationTable, "resource_pod_uuid"), pod) ||
            units.contains(uuidBytes(value(row, ReservationTable, "resource_unit_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, ReservationTable)) ||
            referencesOperation(row, ReservationTable, operations)))

      MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
        shape -> selected(shape.table)
      }
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try owner.releaseTemporaryMemory(estimate, "mode closure selection release")
      catch
        case cleanup: Throwable =>
          if primary != null then
            if cleanup ne primary then primary.addSuppressed(cleanup)
          else throw cleanup

  private def validatePlan(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, revision: Long, snapshot: Image,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None): Unit =
    if plan == null || context == null || snapshot == null || revision < 0L then
      throw new IllegalArgumentException("mode workflow plan, actual context, profile and locator revision are required")
    val root=authorityRoot match
      case Some(explicitRoot) => explicitRoot
      case None => originalImageRoot(snapshot,plan)
    val format=snapshot match
      case _:MySqlFormat3FullSnapshot=>3
      case deployment:MySqlScriptSchemaSnapshot=>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)
        5
    if context.schemaFormat!=format || context.physicalFormat!=2 then
      fail("drift plan requires its exact complete-image schema/physical context")
    val request = driftRequest(operation)
    if plan.requestKind != "RUN" || context.requestKind != "RUN" || plan.exactLimitProfile != snapshot.limitProfile ||
      context.exactLimitProfile != snapshot.limitProfile || context.tag != plan.tag ||
      context.workflowUuid != plan.workflowUuid || context.reserveOperationUuid != plan.reserveOperationUuid ||
      context.requestIdentitySha256 != plan.requestIdentitySha256 || context.bindingUuid != request.binding.bindingUuid ||
      plan.executeOperationUuid != plan.tag.operationId then
      fail("Main plan/context/profile identities do not match the exact mode request")
    authorityRoot.foreach { exactRoot =>
      if exactRoot ne root then fail("drift plan validation lost its exact original authority root")
      snapshot match
        case _:MySqlScriptSchemaSnapshot=>()
        case legacy:MySqlFormat3FullSnapshot if legacy.allocationBudget ne exactRoot =>
          fail("legacy drift image differs from its explicit original authority root")
        case _=>()
    }
    plan.originalRetainedHandle match
      case Some(_) => plan.requireSingle(operation, snapshot.limitProfile)
      case None =>
        // 原纯DATA plan也在同root临时比较，不用独立ledger假证cap；不赋session/SQL权。
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          plan.requireSingle(operation, snapshot.limitProfile)
        }

  private def validatePreimage(snapshot: Image,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long): Unit =
    val request = driftRequest(operation)
    validateLockedHeaders(snapshot, context, request)
    val domain = oneDomain(snapshot, request.binding.bindingUuid, request.podUuid)
    if !criticalBaseMatches(domain, request) then fail("exact full critical drift preimage differs")
    if !ownerMatches(value(domain, DomainTable, "owner_uuid"), request.owner) ||
      long(domain, DomainTable, "is_active") != 1L || long(domain, DomainTable, "state_version") != request.expectedStateVersion ||
      long(domain, DomainTable, "dimension_id") != request.actualPose.dimensionId ||
      text(domain, DomainTable, "world_name") != request.actualPose.worldName ||
      !Set("NORMAL", "WORLD_IDLE").contains(text(domain, DomainTable, "operating_mode")) then
      fail("active owned mode preimage")
    val locator = oneLocator(snapshot, request.binding.bindingUuid, request.podUuid)
    if !uuidEquals(value(locator, LocatorTable, "current_attempt_uuid"), request.attemptUuid) ||
      text(locator, LocatorTable, "locator_state") != "ROW_ACTIVE" ||
      text(locator, LocatorTable, "reason_code") != "ROW_ACTIVE" ||
      long(locator, LocatorTable, "domain_version") != request.expectedStateVersion ||
      long(locator, LocatorTable, "target_dimension_id") != request.actualPose.dimensionId ||
      long(locator, LocatorTable, "locator_revision") != locatorRevision then fail("exact current ROW_ACTIVE locator")
    val footprint = oneCurrentFootprint(snapshot, request)
    if text(footprint, FootprintTable, "seal_state") != "SEALED" then fail("sealed current footprint")
    val cx = chunk(double(domain, "pos_x"))
    val cz = chunk(double(domain, "pos_z"))
    if cx != chunk(request.actualPose.posX) || cz != chunk(request.actualPose.posZ) then fail("same-chunk WID pose")
    if long(footprint, FootprintTable, "origin_version") > request.expectedStateVersion ||
      long(footprint, FootprintTable, "target_dimension_id") != request.actualPose.dimensionId ||
      int(footprint, FootprintTable, "center_chunk_x") != cx ||
      int(footprint, FootprintTable, "center_chunk_z") != cz then
      fail("current footprint origin and 5x5 center agree with the retained pose")
    val chunks = currentChunks(snapshot, request)
    val expected = (for x <- cx - 2 to cx + 2; z <- cz - 2 to cz + 2 yield (x, z)).toSet
    val actual = chunks.map(row => (int(row, ChunkTable, "chunk_x"), int(row, ChunkTable, "chunk_z"))).toSet
    if chunks.size != 25 || actual.size != 25 || actual != expected ||
      chunks.exists(row => long(row, ChunkTable, "dimension_id") != request.actualPose.dimensionId) then
      fail("exact immutable current 5x5 footprint")

  private def validateLockedHeaders(snapshot: Image,
      context: MySqlRepositoryOperationContextIdentity, request: Request): Unit =
    if context == null || snapshot.schema != context.schema ||
      snapshot.connection.serverUuid != context.serverUuid || snapshot.connection.serverVersion != context.serverVersion ||
      snapshot.connection.catalog != context.schema || snapshot.connection.effectiveUser != context.effectiveUser ||
      snapshot.observationScope.bootUuid != context.tag.bootId ||
      snapshot.observationScope.bootGeneration != context.tag.bootGeneration ||
      snapshot.observationScope.connectionGeneration != context.tag.connectionGeneration ||
      snapshot.maxAllowedPacketBytes < context.exactLimitProfile.maxSqlPacketBytes() then
      fail("same verified format3 observation identity/profile is required")

    val manifest = oneSingleton(snapshot, ManifestTable)
    if text(manifest, ManifestTable, "backend") != "MYSQL" ||
      long(manifest, ManifestTable, "format_version") != context.schemaFormat.toLong ||
      text(manifest, ManifestTable, "bootstrap_state") != "READY" ||
      !uuidEquals(value(manifest, ManifestTable, "expected_server_uuid"), context.serverUuid) ||
      !uuidEquals(value(manifest, ManifestTable, "expected_binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(manifest, ManifestTable, "provisioning_uuid"), context.provisioningUuid) ||
      !binaryEquals(value(manifest, ManifestTable, "ddl_sha256"), parseSha256(context.ddlSha256)) ||
      !binaryEquals(value(manifest, ManifestTable, "guard_manifest_sha256"), parseSha256(context.approvedObjectManifestSha256)) ||
      !binaryEquals(value(manifest, ManifestTable, "configuration_sha256"), context.configurationSha256) ||
      text(manifest, ManifestTable, "source_provenance_kind") != context.sourceProvenanceKind ||
      !binaryEquals(value(manifest, ManifestTable, "source_provenance_sha256"), context.sourceProvenanceSha256) ||
      text(manifest, ManifestTable, "limit_profile_encoding") != SourceLimitProfile.ENCODING ||
      !binaryEquals(value(manifest, ManifestTable, "limit_profile_sha256"), context.limitProfileSha256) then
      fail("exact READY format3 manifest identities")
    requireProfileInManifest(manifest, context.exactLimitProfile)
    requireSha256Cell(value(manifest, ManifestTable, "source_provenance_bytes"),
      context.sourceProvenanceSha256, "source provenance")

    val binding = oneSingleton(snapshot, BindingTable)
    if !uuidEquals(value(binding, BindingTable, "binding_uuid"), request.binding.bindingUuid) ||
      text(binding, BindingTable, "cutover_state") != "CERTIFIED" ||
      long(binding, BindingTable, "format_version") != 2L then
      fail("exact CERTIFIED physical2 binding")

    val writer = oneWriter(snapshot)
    if !uuidEquals(value(writer, WriterTable, "binding_uuid"), context.bindingUuid) ||
      long(writer, WriterTable, "writer_epoch") != context.writerEpoch ||
      !uuidEquals(value(writer, WriterTable, "owner_boot_uuid"), context.tag.bootId) ||
      value(writer, WriterTable, "lease_until") != MySqlFormat3Cell.DateTime(context.fixedLeaseUntil) then
      fail("exact current W binding/epoch/boot/lease tuple")
    val slotOperation = value(writer, WriterTable, "last_commit_operation_uuid")
    val slotFingerprint = value(writer, WriterTable, "last_commit_fingerprint")
    (slotOperation, slotFingerprint) match
      case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => ()
      case (MySqlFormat3Cell.Binary(operationBytes), MySqlFormat3Cell.Binary(fingerprintBytes))
          if operationBytes.length == 16 && fingerprintBytes.length == 32 => ()
      case _ => fail("W last receipt operation/fingerprint pair must be complete or SQL NULL together")

  private def oneSingleton(snapshot: Image, table: String): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(table).rows.filter(row => long(row, table, "singleton_fixed1") == 1L)
    if rows.size != 1 then fail("exact singleton " + table + " row")
    rows.head

  private def requireProfileInManifest(manifest: Vector[MySqlFormat3Cell], profile: SourceLimitProfile): Unit =
    val expected = profile.canonicalBytes()
    try
      value(manifest, ManifestTable, "limit_profile_bytes") match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(expected) => ()
        case _ => fail("M raw LimitProfile canonical bytes differ")
    finally Arrays.fill(expected, 0.toByte)
    val fields = Vector(
      "max_region_bytes" -> profile.maxRegionBytes(),
      "max_chunk_bytes" -> profile.maxChunkBytes(),
      "max_decompressed_bytes" -> profile.maxDecompressedBytes(),
      "max_nbt_depth" -> profile.maxNbtDepth(),
      "max_members" -> profile.maxMembers(),
      "max_retained_bytes" -> profile.maxRetainedBytes(),
      "max_sql_packet_bytes" -> profile.maxSqlPacketBytes(),
      "max_native_artifact_bytes" -> profile.maxNativeArtifactBytes())
    fields.foreach { (name, number) =>
      if long(manifest, ManifestTable, name) != number then fail("M decoded LimitProfile scalar differs: " + name)
    }

  private def requireSha256Cell(valueCell: MySqlFormat3Cell, expected: Vector[Byte], label: String): Unit = valueCell match
    case MySqlFormat3Cell.Binary(bytes) =>
      val digest = MessageDigest.getInstance("SHA-256")
      val sink = new DataOutputStream(new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(), digest))
      bytes.writeTo(sink)
      sink.flush()
      if digest.digest().toVector != expected then fail(label + " bytes/digest pair differs")
    case _ => fail(label + " full bytes are missing")

  private def parseSha256(value: String): Vector[Byte] =
    try java.util.HexFormat.of().parseHex(value).toVector
    catch case failure: IllegalArgumentException => throw new SQLDataException("approved identity has malformed SHA-256", failure)

  private def validateReserveShape(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, before: Image,
      after: Image): Unit =
    val request = driftRequest(operation)
    unchanged(before, after, DomainTable)
    unchanged(before, after, LocatorTable)
    before.shapes.filterNot(shape =>
      Set("fp_recovery_operation", "fp_recovery_reservation", "fp_runtime_writer", DomainTable, LocatorTable).contains(shape.table)
    ).foreach(shape => unchanged(before, after, shape.table))
    val beforeOps = keyed(before, OperationTable)
    val afterOps = keyed(after, OperationTable)
    val reserveKey = operationKey(request.binding.bindingUuid, plan.reserveOperationUuid)
    val executeKey = operationKey(request.binding.bindingUuid, plan.executeOperationUuid)
    if beforeOps.contains(reserveKey) || beforeOps.contains(executeKey) ||
      beforeOps.keySet.diff(afterOps.keySet).nonEmpty ||
      afterOps.keySet.diff(beforeOps.keySet) != Set(reserveKey, executeKey) ||
      beforeOps.exists { case (key, row) => afterOps.get(key).forall(_ != row) } then
      fail("reserve phase appends only the two fixed O rows")
    val reserveO = afterOps(reserveKey)
    val executeO = afterOps(executeKey)
    requireOperationHeader(reserveO, request, plan, plan.reserveOperationUuid, "RESERVE", "COMMITTED")
    requireOperationHeader(executeO, request, plan, plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED")
    if !uuidEquals(value(executeO, OperationTable, "reserve_operation_uuid"), plan.reserveOperationUuid) ||
      !uuidEquals(value(executeO, OperationTable, "previous_step_operation_uuid"), plan.reserveOperationUuid) then
      fail("executeO links to the exact reserveO")
    Vector("result_at", "settled_by_operation_uuid", "commit_witness_bytes", "commit_witness_sha256").foreach { name =>
      if value(executeO, OperationTable, name) != MySqlFormat3Cell.SqlNull then fail("RESERVED executeO SQL NULL " + name)
    }
    if optionalDate(reserveO, OperationTable, "result_at").isEmpty ||
      optionalDate(executeO, OperationTable, "result_at").nonEmpty ||
      value(reserveO, OperationTable, "planned_at") != value(executeO, OperationTable, "planned_at") ||
      value(reserveO, OperationTable, "fixed_lease_until") != value(executeO, OperationTable, "fixed_lease_until") then
      fail("fixed reserve/execute timestamp and terminal state")
    val oldReservation = rowsForPod(before, ReservationTable, request.binding.bindingUuid, request.podUuid)
    val newReservation = rowsForPod(after, ReservationTable, request.binding.bindingUuid, request.podUuid)
    if oldReservation.size > 1 || newReservation.size != 1 then fail("one historical/current POD reservation row")
    val oldReservationMap = keyed(before, ReservationTable)
    val newReservationMap = keyed(after, ReservationTable)
    val targetReservationKey = MySqlFormat3RelationShapes.forTable(ReservationTable).primaryKey
      .map(name => newReservation.head(index(ReservationTable, name)))
    val changedReservationKeys = oldReservationMap.keySet.union(newReservationMap.keySet).filter(key =>
      oldReservationMap.get(key) != newReservationMap.get(key))
    if changedReservationKeys != Set(targetReservationKey) ||
      oldReservationMap.keySet.diff(newReservationMap.keySet).nonEmpty ||
      newReservationMap.keySet.diff(oldReservationMap.keySet).exists(_ != targetReservationKey) then
      fail("reserve changes only the exact POD reservation row across full R history")
    val expectedRevision = oldReservation.headOption match
      case None => 0L
      case Some(previous) =>
        if text(previous, ReservationTable, "reservation_state") != "RELEASED" then
          fail("a previous HELD POD reservation retains its operation responsibility")
        Math.addExact(long(previous, ReservationTable, "reservation_revision"), 1L)
    validateReservationIdentity(newReservation.head, request, plan, before.limitProfile,
      expectedRevision, plan.reserveOperationUuid, "HELD")
    oldReservation.headOption.foreach { previous =>
      if !sameExcept(previous, newReservation.head, ReservationTable,
        Set("reservation_revision", "workflow_uuid", "operation_uuid", "reservation_state", "last_mutation_operation_uuid")) ||
        long(newReservation.head, ReservationTable, "reservation_revision") != expectedRevision then
        fail("released POD reservation is reused by exact identity with one revision increment")
    }
    val oldWriter = oneWriter(before)
    val newWriter = oneWriter(after)
    if !sameExcept(oldWriter, newWriter, WriterTable, Set("last_commit_operation_uuid", "last_commit_fingerprint")) ||
      !uuidEquals(value(newWriter, WriterTable, "last_commit_operation_uuid"), plan.reserveOperationUuid) ||
      !binaryOfLength(value(newWriter, WriterTable, "last_commit_fingerprint"), 32) then
      fail("reserve W updates only its last receipt slot")

  private def validateExecuteShape(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, before: Image,
      after: Image): Unit =
    val request = driftRequest(operation)
    before.shapes.filterNot(shape =>
      Set("fp_recovery_operation", "fp_recovery_reservation", WriterTable, DomainTable, LocatorTable).contains(shape.table)
    ).foreach(shape => unchanged(before, after, shape.table))
    val oldDomain = oneDomain(before, request.binding.bindingUuid, request.podUuid)
    val newDomain = oneDomain(after, request.binding.bindingUuid, request.podUuid)
    val oldDomains = keyed(before, DomainTable)
    val newDomains = keyed(after, DomainTable)
    val domainKey = Vector(binary(request.podUuid))
    if oldDomains.keySet != newDomains.keySet || oldDomains.exists { case (key, row) =>
      key != domainKey && newDomains.get(key).forall(_ != row)
    } then fail("mode change may advance only the exact domain PK")
    if !sameExcept(oldDomain, newDomain, DomainTable,
      Set("state_version", "world_name", "pos_x", "pos_y", "pos_z", "yaw", "pitch", "last_updated",
        "last_mutation_operation_uuid")) ||
      long(newDomain, DomainTable, "state_version") != Math.addExact(request.expectedStateVersion, 1L) ||
      text(newDomain, DomainTable, "world_name") != request.actualPose.worldName ||
      double(newDomain, "pos_x") != request.actualPose.posX || double(newDomain, "pos_y") != request.actualPose.posY ||
      double(newDomain, "pos_z") != request.actualPose.posZ || double(newDomain, "yaw") != request.actualPose.yaw.toDouble ||
      double(newDomain, "pitch") != request.actualPose.pitch.toDouble ||
      long(newDomain, DomainTable, "dimension_id") != request.actualPose.dimensionId ||
      text(newDomain, DomainTable, "operating_mode") != request.targetMode.databaseValue ||
      !uuidEquals(value(newDomain, DomainTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) then
      fail("execute domain row is not the exact one-version same-chunk mode/pose update")
    val oldLocator = oneLocator(before, request.binding.bindingUuid, request.podUuid)
    val newLocator = oneLocator(after, request.binding.bindingUuid, request.podUuid)
    val oldLocators = keyed(before, LocatorTable)
    val newLocators = keyed(after, LocatorTable)
    val locatorKey = MySqlFormat3RelationShapes.forTable(LocatorTable).primaryKey
      .map(name => oldLocator(index(LocatorTable, name)))
    if oldLocators.keySet != newLocators.keySet || oldLocators.exists { case (key, row) =>
      key != locatorKey && newLocators.get(key).forall(_ != row)
    } then fail("mode change may advance only the exact locator PK")
    if !sameExcept(oldLocator, newLocator, LocatorTable, Set("domain_version", "locator_revision", "last_mutation_operation_uuid")) ||
      long(newLocator, LocatorTable, "domain_version") != Math.addExact(request.expectedStateVersion, 1L) ||
      long(newLocator, LocatorTable, "locator_revision") != Math.addExact(long(oldLocator, LocatorTable, "locator_revision"), 1L) ||
      !uuidEquals(value(newLocator, LocatorTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) then
      fail("execute locator must advance domain version and revision with D")
    val oldOps = keyed(before, OperationTable)
    val newOps = keyed(after, OperationTable)
    val reserveKey = operationKey(request.binding.bindingUuid, plan.reserveOperationUuid)
    val executeKey = operationKey(request.binding.bindingUuid, plan.executeOperationUuid)
    if oldOps.keySet != newOps.keySet || oldOps.get(reserveKey) != newOps.get(reserveKey) ||
      oldOps.exists { case (key, row) => key != executeKey && newOps.get(key).forall(_ != row) } ||
      oldOps.get(executeKey).isEmpty || newOps.get(executeKey).isEmpty then
      fail("execute retains every operation row and the raw historical reserveO")
    val oldExecute = oldOps(executeKey)
    val newExecute = newOps(executeKey)
    if text(oldExecute, OperationTable, "operation_state") != "RESERVED" ||
      text(newExecute, OperationTable, "operation_state") != "COMMITTED" ||
      value(oldExecute, OperationTable, "result_at") != MySqlFormat3Cell.SqlNull ||
      optionalDate(newExecute, OperationTable, "result_at").isEmpty ||
      value(oldExecute, OperationTable, "commit_witness_bytes") != MySqlFormat3Cell.SqlNull ||
      value(oldExecute, OperationTable, "commit_witness_sha256") != MySqlFormat3Cell.SqlNull ||
      !binaryOfLength(value(newExecute, OperationTable, "commit_witness_bytes"), -1) ||
      !binaryOfLength(value(newExecute, OperationTable, "commit_witness_sha256"), 32) then
      fail("executeO moves only from genuine RESERVED NULL witness to COMMITTED witness")
    if !sameExcept(oldExecute, newExecute, OperationTable,
      Set("operation_state", "preimage_bytes", "preimage_sha256", "target_bytes", "target_sha256",
        "commit_witness_bytes", "commit_witness_sha256", "result_at", "operation_fingerprint")) then
      fail("executeO changes only its phase fields")
    val oldReservations = rowsForPod(before, ReservationTable, request.binding.bindingUuid, request.podUuid)
    val newReservations = rowsForPod(after, ReservationTable, request.binding.bindingUuid, request.podUuid)
    if oldReservations.size != 1 || newReservations.size != 1 ||
      !sameExcept(oldReservations.head, newReservations.head, ReservationTable,
        Set("reservation_revision", "reservation_state", "last_mutation_operation_uuid")) ||
      long(newReservations.head, ReservationTable, "reservation_revision") !=
        Math.addExact(long(oldReservations.head, ReservationTable, "reservation_revision"), 1L) ||
      text(oldReservations.head, ReservationTable, "reservation_state") != "HELD" ||
      text(newReservations.head, ReservationTable, "reservation_state") != "RELEASED" ||
      !uuidEquals(value(newReservations.head, ReservationTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) ||
      !uuidEquals(value(newReservations.head, ReservationTable, "operation_uuid"), plan.executeOperationUuid) ||
      !uuidEquals(value(newReservations.head, ReservationTable, "workflow_uuid"), plan.workflowUuid) then
      fail("execute releases the exact held POD reservation once")
    validateReservationIdentity(oldReservations.head, request, plan, before.limitProfile,
      long(oldReservations.head, ReservationTable, "reservation_revision"), plan.reserveOperationUuid, "HELD")
    validateReservationIdentity(newReservations.head, request, plan, before.limitProfile,
      Math.addExact(long(oldReservations.head, ReservationTable, "reservation_revision"), 1L),
      plan.executeOperationUuid, "RELEASED")
    val oldReservationMap = keyed(before, ReservationTable)
    val newReservationMap = keyed(after, ReservationTable)
    val targetReservationKey = MySqlFormat3RelationShapes.forTable(ReservationTable).primaryKey
      .map(name => oldReservations.head(index(ReservationTable, name)))
    val changedReservationKeys = oldReservationMap.keySet.union(newReservationMap.keySet).filter(key =>
      oldReservationMap.get(key) != newReservationMap.get(key))
    if oldReservationMap.keySet != newReservationMap.keySet || changedReservationKeys != Set(targetReservationKey) then
      fail("execute changes only the exact held POD reservation row across full R history")
    val oldWriter = oneWriter(before)
    val newWriter = oneWriter(after)
    if !sameExcept(oldWriter, newWriter, WriterTable, Set("last_commit_operation_uuid", "last_commit_fingerprint")) ||
      !uuidEquals(value(newWriter, WriterTable, "last_commit_operation_uuid"), plan.executeOperationUuid) ||
      !binaryOfLength(value(newWriter, WriterTable, "last_commit_fingerprint"), 32) then
      fail("execute W updates only its exact receipt slot")

  private def markerRequired(phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      table: String, key: Vector[MySqlFormat3Cell], column: String,
      plan: MySqlOwnedRepositoryOperationPlan): Boolean =
    if table == WriterTable && image == MySqlOperationEffectImage.Target && column == "last_commit_fingerprint" then true
    else if table != OperationTable then false
    else
      val operationUuid = uuidCell(key(index(OperationTable, "operation_uuid")), "operation_uuid key")
      val reserve = operationUuid == plan.reserveOperationUuid
      val execute = operationUuid == plan.executeOperationUuid
      phase match
        case MySqlOperationEffectPhase.Reserve if image == MySqlOperationEffectImage.Target =>
          (reserve && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "commit_witness_bytes", "commit_witness_sha256").contains(column)) ||
          (execute && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "preimage_bytes", "preimage_sha256").contains(column))
        case MySqlOperationEffectPhase.Execute =>
          (execute && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "preimage_bytes", "preimage_sha256").contains(column)) ||
          (execute && image == MySqlOperationEffectImage.Target &&
            Set("commit_witness_bytes", "commit_witness_sha256").contains(column))
        case _ => false

  private def writeMarker(output: DataOutputStream, phase: MySqlOperationEffectPhase,
      shape: MySqlFormat3RelationShape, key: Vector[MySqlFormat3Cell],
      column: MySqlFormat3ColumnShape, original: MySqlFormat3Cell): Unit =
    if original == MySqlFormat3Cell.SqlNull then fail("self-reference markers cannot replace SQL NULL")
    output.writeByte(0x7f)
    MySqlRecoveryCanonicalBytes.writeToken(output, "FP_TYPED_MARKER_V1")
    MySqlRecoveryCanonicalBytes.writeText(output, phaseName(phase))
    MySqlRecoveryCanonicalBytes.writeText(output, shape.table)
    output.writeInt(shape.primaryKey.size)
    shape.primaryKey.foreach(name => writeCell(output, key(index(shape.table, name))))
    MySqlRecoveryCanonicalBytes.writeText(output, column.name)
    MySqlRecoveryCanonicalBytes.writeText(output, column.sqlDeclaration)
    output.writeByte(1)

  private def requireOperationHeader(row: Vector[MySqlFormat3Cell], request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, operationUuid: UUID, step: String, state: String): Unit =
    if !uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, OperationTable, "operation_uuid"), operationUuid) ||
      !uuidEquals(value(row, OperationTable, "workflow_uuid"), plan.workflowUuid) ||
      text(row, OperationTable, "step") != step ||
      text(row, OperationTable, "operation_state") != state ||
      !uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
      !uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid) ||
      !uuidEquals(value(row, OperationTable, "boot_uuid"), plan.tag.bootId) then
      fail("fixed " + step + " O row identity")

  private def validateReservationIdentity(row: Vector[MySqlFormat3Cell], request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, profile: SourceLimitProfile, revision: Long,
      mutationOperation: UUID, state: String): Unit =
    val resource = MySqlRecoveryResourceKey.pod(request.podUuid, profile)
    val keyBudget = MySqlRecoveryCanonicalBytes.allocationBudgetFor(profile)
    val keyBytes = resource.copyCanonicalBytes(keyBudget, "POD reservation canonical identity verification")
    try
      value(row, ReservationTable, "resource_key") match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(keyBytes) => ()
        case _ => fail("POD reservation full key bytes differ")
    finally Arrays.fill(keyBytes, 0.toByte)
    if text(row, ReservationTable, "resource_kind") != "POD" ||
      !uuidEquals(value(row, ReservationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, ReservationTable, "resource_pod_uuid"), request.podUuid) ||
      value(row, ReservationTable, "resource_unit_uuid") != MySqlFormat3Cell.SqlNull ||
      !binaryEquals(value(row, ReservationTable, "resource_key_hash"), resource.keyHashSha256) ||
      !uuidEquals(value(row, ReservationTable, "workflow_uuid"), plan.workflowUuid) ||
      !uuidEquals(value(row, ReservationTable, "operation_uuid"), plan.executeOperationUuid) ||
      text(row, ReservationTable, "reservation_state") != state ||
      long(row, ReservationTable, "reservation_revision") != revision ||
      !uuidEquals(value(row, ReservationTable, "last_mutation_operation_uuid"), mutationOperation) then
      fail("exact immutable POD reservation identity/revision/operation tuple")

  private[mysql] def driftRequest(operation: MySqlRepositoryOperation): Request = operation match
    case MySqlRepositoryOperation.CommitTrackedActiveDrift(binding, base, desired, attempt) =>
      MySqlTrackedDriftRequest.requireSameChunkPoseOnly(binding, base, desired, attempt)
      Request(binding, base.uuid, base.ownerUuid, base.stateVersion, attempt, base.operatingMode,
        FloatingPodPose(desired.worldName, desired.posX, desired.posY, desired.posZ, desired.yaw, desired.pitch, desired.dimensionId.get), base)
    case _ => throw new IllegalArgumentException("drift effect accepts only exact CommitTrackedActiveDrift")

  private def ownerMatches(cell: MySqlFormat3Cell, owner: Option[UUID]): Boolean = owner match
    case None => cell == MySqlFormat3Cell.SqlNull
    case Some(uuid) => uuidEquals(cell, uuid)

  /** 锁定 full20 已通过 M/B/W 身份核验后，将稳定业务拒绝与结构故障分开。 */
  private[mysql] def preflight(snapshot: Image,
      operation: MySqlRepositoryOperation): MySqlDriftPreflight =
    if snapshot == null then throw new IllegalArgumentException("locked mode preflight snapshot is required")
    val request = driftRequest(operation)
    val bindingRows = snapshot.relation(BindingTable).rows
    if bindingRows.size != 1 then fail("exact singleton F3 binding row for mode preflight")
    val binding = bindingRows.head
    if !uuidEquals(value(binding, BindingTable, "binding_uuid"), request.binding.bindingUuid) ||
      text(binding, BindingTable, "cutover_state") != "CERTIFIED" ||
      long(binding, BindingTable, "format_version") != 2L then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.BindingMismatch)
    val domainRows = snapshot.relation(DomainTable).rows.filter(row =>
      uuidEquals(value(row, DomainTable, "uuid"), request.podUuid))
    if domainRows.isEmpty then return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.UnknownPod)
    if domainRows.size != 1 then fail("unique domain primary key for mode preflight")
    val domain = domainRows.head
    if !ownerMatches(value(domain, DomainTable, "owner_uuid"), request.owner) then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.CriticalBaseMismatch(long(domain, DomainTable, "state_version")))
    if !criticalBaseMatches(domain, request) then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.CriticalBaseMismatch(long(domain, DomainTable, "state_version")))
    val active = long(domain, DomainTable, "is_active")
    if active != 1L then return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.Inactive)
    val version = long(domain, DomainTable, "state_version")
    val dimension = value(domain, DomainTable, "dimension_id") match
      case MySqlFormat3Cell.SignedInteger(actual) if actual >= Int.MinValue.toLong && actual <= Int.MaxValue.toLong => actual.toInt
      case _ => fail("active mode row has an exact dimension")
    val world = text(domain, DomainTable, "world_name")
    val x = double(domain, "pos_x")
    val z = double(domain, "pos_z")
    val currentMode = text(domain, DomainTable, "operating_mode")
    if !Set("NORMAL", "WORLD_IDLE").contains(currentMode) then fail("known active operating mode")
    val locatorRows = snapshot.relation(LocatorTable).rows.filter(row =>
      uuidEquals(value(row, LocatorTable, "binding_uuid"), request.binding.bindingUuid) &&
        uuidEquals(value(row, LocatorTable, "pod_uuid"), request.podUuid))
    if locatorRows.size != 1 then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.PhysicalContextMismatch)
    val locator = locatorRows.head
    if !uuidEquals(value(locator, LocatorTable, "current_attempt_uuid"), request.attemptUuid) ||
      text(locator, LocatorTable, "locator_state") != "ROW_ACTIVE" ||
      text(locator, LocatorTable, "reason_code") != "ROW_ACTIVE" ||
      value(locator, LocatorTable, "domain_version") != MySqlFormat3Cell.SignedInteger(version) ||
      long(locator, LocatorTable, "target_dimension_id") != dimension.toLong then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.PhysicalContextMismatch)
    val footprintRows = snapshot.relation(FootprintTable).rows.filter(row =>
      value(row, FootprintTable, "binding_uuid") == value(locator, LocatorTable, "binding_uuid") &&
        value(row, FootprintTable, "pod_uuid") == value(locator, LocatorTable, "pod_uuid") &&
        value(row, FootprintTable, "attempt_uuid") == value(locator, LocatorTable, "current_attempt_uuid") &&
        value(row, FootprintTable, "footprint_uuid") == value(locator, LocatorTable, "current_footprint_uuid"))
    if footprintRows.size != 1 then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.PhysicalContextMismatch)
    val footprint = footprintRows.head
    val currentChunkX = chunk(x)
    val currentChunkZ = chunk(z)
    if text(footprint, FootprintTable, "seal_state") != "SEALED" ||
      long(footprint, FootprintTable, "origin_version") > version ||
      long(footprint, FootprintTable, "target_dimension_id") != dimension.toLong ||
      int(footprint, FootprintTable, "center_chunk_x") != currentChunkX ||
      int(footprint, FootprintTable, "center_chunk_z") != currentChunkZ then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.PhysicalContextMismatch)
    val footprintChunks = snapshot.relation(ChunkTable).rows.filter(row =>
      value(row, ChunkTable, "binding_uuid") == value(footprint, FootprintTable, "binding_uuid") &&
        value(row, ChunkTable, "pod_uuid") == value(footprint, FootprintTable, "pod_uuid") &&
        value(row, ChunkTable, "attempt_uuid") == value(footprint, FootprintTable, "attempt_uuid") &&
        value(row, ChunkTable, "footprint_uuid") == value(footprint, FootprintTable, "footprint_uuid"))
    val currentChunks = footprintChunks.map(row => (int(row, ChunkTable, "chunk_x"), int(row, ChunkTable, "chunk_z"))).toSet
    val expectedChunks = (for cx <- currentChunkX - 2 to currentChunkX + 2;
      cz <- currentChunkZ - 2 to currentChunkZ + 2 yield (cx, cz)).toSet
    if footprintChunks.size != 25 || currentChunks.size != 25 || currentChunks != expectedChunks ||
      footprintChunks.exists(row => long(row, ChunkTable, "dimension_id") != dimension.toLong) then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.PhysicalContextMismatch)
    val poseMatches = request.actualPose.worldName == world && request.actualPose.dimensionId == dimension &&
      request.actualPose.posX == x && request.actualPose.posY == double(domain, "pos_y") &&
      request.actualPose.posZ == z && request.actualPose.yaw.toDouble == double(domain, "yaw") &&
      request.actualPose.pitch.toDouble == double(domain, "pitch")
    val sameChunk = request.actualPose.worldName == world && request.actualPose.dimensionId == dimension &&
      chunk(request.actualPose.posX) == currentChunkX && chunk(request.actualPose.posZ) == currentChunkZ
    if version != request.expectedStateVersion then
      return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.CriticalBaseMismatch(version))
    if !sameChunk then return MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.CrossChunkMovement)
    if version == request.expectedStateVersion then MySqlDriftPreflight.Execute
    else MySqlDriftPreflight.Rejected(MySqlTrackedDriftRejection.CriticalBaseMismatch(version))

  private[mysql] def operationRows(snapshot: Image,
      operation: MySqlRepositoryOperation): Vector[Vector[MySqlFormat3Cell]] =
    val request = driftRequest(operation)
    snapshot.relation(OperationTable).rows.filter(row =>
      uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) &&
        (uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
          uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid)))

  private[mysql] def rowOperationUuid(row: Vector[MySqlFormat3Cell]): UUID =
    uuidCell(value(row, OperationTable, "operation_uuid"), "operation_uuid")

  private def oneDomain(snapshot: Image, binding: UUID, pod: UUID): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(DomainTable).rows.filter(row =>
      uuidEquals(value(row, DomainTable, "binding_uuid"), binding) && uuidEquals(value(row, DomainTable, "uuid"), pod))
    if rows.size != 1 then fail("exact binding-owned domain row")
    rows.head

  private def oneLocator(snapshot: Image, binding: UUID, pod: UUID): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(LocatorTable).rows.filter(row =>
      uuidEquals(value(row, LocatorTable, "binding_uuid"), binding) && uuidEquals(value(row, LocatorTable, "pod_uuid"), pod))
    if rows.size != 1 then fail("exact current locator row")
    rows.head

  private def oneCurrentFootprint(snapshot: Image, request: Request): Vector[MySqlFormat3Cell] =
    val locator = oneLocator(snapshot, request.binding.bindingUuid, request.podUuid)
    val rows = snapshot.relation(FootprintTable).rows.filter(row =>
      value(row, FootprintTable, "binding_uuid") == value(locator, LocatorTable, "binding_uuid") &&
        value(row, FootprintTable, "pod_uuid") == value(locator, LocatorTable, "pod_uuid") &&
        value(row, FootprintTable, "attempt_uuid") == value(locator, LocatorTable, "current_attempt_uuid") &&
        value(row, FootprintTable, "footprint_uuid") == value(locator, LocatorTable, "current_footprint_uuid"))
    if rows.size != 1 then fail("current locator must point to one exact immutable footprint")
    rows.head

  private def currentChunks(snapshot: Image,
      request: Request): Vector[Vector[MySqlFormat3Cell]] =
    val footprint = oneCurrentFootprint(snapshot, request)
    snapshot.relation(ChunkTable).rows.filter(row =>
      value(row, ChunkTable, "binding_uuid") == value(footprint, FootprintTable, "binding_uuid") &&
        value(row, ChunkTable, "pod_uuid") == value(footprint, FootprintTable, "pod_uuid") &&
        value(row, ChunkTable, "attempt_uuid") == value(footprint, FootprintTable, "attempt_uuid") &&
        value(row, ChunkTable, "footprint_uuid") == value(footprint, FootprintTable, "footprint_uuid"))

  private def oneWriter(snapshot: Image): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(WriterTable).rows.filter(row => long(row, WriterTable, "singleton_fixed1") == 1L)
    if rows.size != 1 then fail("exact singleton W row")
    rows.head

  private def rowsForPod(snapshot: Image, table: String,
      binding: UUID, pod: UUID): Vector[Vector[MySqlFormat3Cell]] =
    snapshot.relation(table).rows.filter(row =>
      uuidEquals(value(row, table, "binding_uuid"), binding) &&
        uuidEquals(value(row, table, "resource_pod_uuid"), pod))

  private def operationKey(bindingUuid: UUID, operationUuid: UUID): Vector[MySqlFormat3Cell] =
    Vector(binary(bindingUuid), binary(operationUuid))

  private def keyed(snapshot: Image, table: String): Map[Vector[MySqlFormat3Cell], Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    snapshot.relation(table).rows.map(row =>
      shape.primaryKey.map(name => row(index(table, name))) -> row).toMap

  private def unchanged(before: Image, after: Image, table: String): Unit =
    if before.relation(table).rows != after.relation(table).rows then fail("phase changed unrelated full20 relation " + table)

  private def sameExcept(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell],
      table: String, mutableNames: Set[String]): Boolean =
    val columns = MySqlFormat3RelationShapes.forTable(table).columns
    left.size == columns.size && right.size == columns.size &&
      columns.indices.forall(i => mutableNames.contains(columns(i).name) || left(i) == right(i))

  private[mysql] def value(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    val indexValue = index(table, name)
    row(indexValue)

  private def index(table: String, name: String): Int =
    val indexValue = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if indexValue < 0 then fail("fixed selector column " + table + "." + name)
    indexValue

  private def text(row: Vector[MySqlFormat3Cell], table: String, name: String): String =
    value(row, table, name) match
      case MySqlFormat3Cell.Text(result) => result
      case _ => fail("typed text cell " + table + "." + name)

  private def long(row: Vector[MySqlFormat3Cell], table: String, name: String): Long =
    value(row, table, name) match
      case MySqlFormat3Cell.SignedInteger(result) => result
      case _ => fail("typed integer cell " + table + "." + name)

  private def int(row: Vector[MySqlFormat3Cell], table: String, name: String): Int =
    Math.toIntExact(long(row, table, name))

  private def double(row: Vector[MySqlFormat3Cell], name: String): Double =
    value(oneDomainByUuidless(row), DomainTable, name) match
      case MySqlFormat3Cell.Floating(bits) => java.lang.Double.longBitsToDouble(bits)
      case _ => fail("typed DOUBLE cell " + DomainTable + "." + name)

  private def criticalBaseMatches(row: Vector[MySqlFormat3Cell], request: Request): Boolean =
    val base = request.criticalBase
    def raw(name: String, expected: Double): Boolean = value(row, DomainTable, name) match
      case MySqlFormat3Cell.Floating(bits) => bits == java.lang.Double.doubleToRawLongBits(expected)
      case _ => fail("typed DOUBLE critical preimage " + name)
    ownerMatches(value(row, DomainTable, "owner_uuid"), base.ownerUuid) &&
      uuidEquals(value(row, DomainTable, "uuid"), base.uuid) && long(row, DomainTable, "is_active") == 1L &&
      long(row, DomainTable, "state_version") == base.stateVersion && long(row, DomainTable, "last_updated") == base.lastUpdated &&
      long(row, DomainTable, "dimension_id") == base.dimensionId.get.toLong && text(row, DomainTable, "world_name") == base.worldName &&
      text(row, DomainTable, "operating_mode") == base.operatingMode.databaseValue &&
      raw("pos_x", base.posX) && raw("pos_y", base.posY) && raw("pos_z", base.posZ) &&
      raw("yaw", base.yaw.toDouble) && raw("pitch", base.pitch.toDouble)

  private def oneDomainByUuidless(row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] = row

  private def optionalDate(row: Vector[MySqlFormat3Cell], table: String, name: String): Option[LocalDateTime] =
    value(row, table, name) match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.DateTime(result) => Some(result)
      case _ => fail("typed nullable DATETIME cell " + table + "." + name)

  private def binaryOfLength(value: MySqlFormat3Cell, length: Int): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => length < 0 || bytes.length == length
    case _ => false

  private def binaryEquals(value: MySqlFormat3Cell, expected: Vector[Byte]): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsVector(expected)
    case _ => false

  private[mysql] def uuidEquals(value: MySqlFormat3Cell, expected: UUID): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(expected)
    case _ => false

  private def uuidCell(value: MySqlFormat3Cell, label: String): UUID = value match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
      // 原 immutable16bytes直接读取；tail查找不产生80bytes永久copy debt。
      var most = 0L
      var least = 0L
      var index = 0
      while index < 8 do
        most = (most << 8) | (bytes.byteAt(index).toLong & 0xffL)
        least = (least << 8) | (bytes.byteAt(index + 8).toLong & 0xffL)
        index += 1
      new UUID(most, least)
    case _ => fail("16-byte UUID " + label)

  private def uuidBytes(value: MySqlFormat3Cell): Vector[Byte] = value match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 => keyBytes(bytes)
    case _ => Vector.empty

  private def binary(value: UUID): MySqlFormat3Cell =
    if value == null then throw new IllegalArgumentException("exact UUID key is required")
    val bytes = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))

  private def keyBytes(value: MySqlFormat3Binary): Vector[Byte] =
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone { new MySqlFormat3ReadBudget(SourceLimitProfile.of(1, 1, 1, 1, 1, 1024, 1024, 1), 1024) }
    val bytes = value.copyBytes(budget, "fixed format3 key")
    try bytes.toVector finally Arrays.fill(bytes, 0.toByte)

  private def addUuid(set: mutable.Set[Vector[Byte]], cell: MySqlFormat3Cell): Unit =
    cell match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 => set.add(keyBytes(bytes))
      case _ => ()

  private def isOperationReference(name: String): Boolean =
    name.endsWith("_operation_uuid") && name != "operation_uuid"

  private def referencesOperation(row: Vector[MySqlFormat3Cell], table: String,
      ids: mutable.Set[Vector[Byte]]): Boolean =
    MySqlFormat3RelationShapes.forTable(table).columns.exists(column =>
      isOperationReference(column.name) && ids.contains(uuidBytes(value(row, table, column.name))))

  private def carrierTuple(row: Vector[MySqlFormat3Cell], table: String): (String, Vector[Byte]) =
    (value(row, table, "resource_kind"), value(row, table, "carrier_key_hash")) match
      case (MySqlFormat3Cell.Text(kind), MySqlFormat3Cell.Binary(hash)) => kind -> keyBytes(hash)
      case _ => "" -> Vector.empty

  private def phaseName(phase: MySqlOperationEffectPhase): String = phase match
    case MySqlOperationEffectPhase.Reserve => "RESERVE"
    case MySqlOperationEffectPhase.Execute => "EXECUTE"

  private def writeUuid(output: DataOutputStream, value: UUID): Unit =
    MySqlRecoveryCanonicalBytes.writeUuid(output, value)

  private def writeCell(output: DataOutputStream, value: MySqlFormat3Cell): Unit =
    value match
      case MySqlFormat3Cell.SqlNull => output.writeByte(0)
      case MySqlFormat3Cell.Text(textValue) => output.writeByte(1); MySqlRecoveryCanonicalBytes.writeText(output, textValue)
      case MySqlFormat3Cell.Binary(bytes) => output.writeByte(2); output.writeInt(bytes.length); bytes.writeTo(output)
      case MySqlFormat3Cell.SignedInteger(number) => output.writeByte(3); output.writeLong(number)
      case MySqlFormat3Cell.Floating(bits) => output.writeByte(4); output.writeLong(bits)
      case MySqlFormat3Cell.DateTime(dateTime) =>
        output.writeByte(5)
        output.writeInt(dateTime.getYear)
        output.writeByte(dateTime.getMonthValue)
        output.writeByte(dateTime.getDayOfMonth)
        output.writeByte(dateTime.getHour)
        output.writeByte(dateTime.getMinute)
        output.writeByte(dateTime.getSecond)
        output.writeInt(dateTime.getNano / 1000)

  private def chunk(position: Double): Int =
    val floor = Math.floor(position)
    if !java.lang.Double.isFinite(floor) || floor < Int.MinValue.toDouble || floor > Int.MaxValue.toDouble then
      fail("pose is outside representable Minecraft block coordinates")
    Math.floorDiv(floor.toInt, 16)

  private def fail(reason: String): Nothing =
    throw new SQLDataException("MySQL format3 mode O/R effect violates " + reason)
