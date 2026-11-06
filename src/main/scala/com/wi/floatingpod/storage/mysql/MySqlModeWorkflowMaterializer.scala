package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.PodOperatingMode

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.sql.SQLDataException
import java.time.{LocalDateTime, ZoneOffset}
import java.util.{Arrays, UUID}
import MySqlModeOperationEffectCodec.{Image, allocationBudget, limitProfile, relation, relations, schema,
  connection, observationScope, maxAllowedPacketBytes, shapes}

/** 两个不可变的计划阶段，保留 full20 图像与操作见证，但不保留 JDBC owner、连接或提交确认。 */
private[mysql] final class MySqlModeWorkflowMaterialization private[mysql] (
    val plannedAt: LocalDateTime,
    private val initialImage: Image,
    private val reservedImage: Image,
    private val executedImage: Image,
    val effects: Vector[MySqlModeOperationEffect],
    private val reserveWitness: MySqlRecoveryCanonicalBytes,
    private val executeWitness: MySqlRecoveryCanonicalBytes,
    private val plan: MySqlOwnedRepositoryOperationPlan,
    private val operation: MySqlRepositoryOperation
):
  private def legacy(image: Image): MySqlFormat3FullSnapshot = image match
    case value: MySqlFormat3FullSnapshot => value
    case _: MySqlScriptSchemaSnapshot =>
      throw new SQLDataException("Full26 mode materialization cannot enter a legacy Full20 getter")
  def preimage: MySqlFormat3FullSnapshot = legacy(initialImage)
  def reserveTarget: MySqlFormat3FullSnapshot = legacy(reservedImage)
  def executeTarget: MySqlFormat3FullSnapshot = legacy(executedImage)
  private[mysql] def completePreimage: Image = initialImage
  private[mysql] def completeReserveTarget: Image = reservedImage
  private[mysql] def completeExecuteTarget: Image = executedImage
  private[mysql] def isDeploymentReceipt: Boolean = initialImage match
    case _: MySqlScriptSchemaSnapshot => true
    case _: MySqlFormat3FullSnapshot => false
  private[mysql] var originalWitnessDraft: MySqlModeOriginalWitnessDraft = null
  private[mysql] var originalLocatorRevision: Long = -1L
  private[mysql] def originalReserveWitness: MySqlRecoveryCanonicalBytes = reserveWitness
  private[mysql] def originalExecuteWitness: MySqlRecoveryCanonicalBytes = executeWitness

  private[mysql] def exactPlan: MySqlOwnedRepositoryOperationPlan = plan
  private[mysql] def exactOperation: MySqlRepositoryOperation = operation

  private[mysql] def requireBoundTo(commandScope: MySqlRepositoryCommandScope,
      currentOperation: MySqlRepositoryOperation): Unit =
    if commandScope == null || currentOperation == null || (commandScope.operationPlan() ne plan) ||
      currentOperation != operation then
      throw new SQLDataException("mode workflow materialization differs from the exact Main command plan")
    commandScope.requireCurrent(commandScope.exactAuditScope(), plan)
    plan.requireSingle(currentOperation, plan.exactLimitProfile)
    verify()

  def verify(): Unit =
    if plannedAt == null || plannedAt.getNano % 1000 != 0 || effects == null ||
      effects.map(_.phase) != Vector(MySqlOperationEffectPhase.Reserve, MySqlOperationEffectPhase.Execute) then
      throw new SQLDataException("mode workflow materialization is incomplete or not DATETIME(6)")
    (initialImage, reservedImage, executedImage) match
      case (before: MySqlFormat3FullSnapshot, reserved: MySqlFormat3FullSnapshot, executed: MySqlFormat3FullSnapshot) =>
        effects(0).verifyPreimage(before)
        effects(0).verifyReadback(reserved)
        effects(1).verifyPreimage(reserved)
        effects(1).verifyReadback(executed)
      case (before: MySqlScriptSchemaSnapshot, reserved: MySqlScriptSchemaSnapshot, executed: MySqlScriptSchemaSnapshot) =>
        val root = reserved.allocationBudget
        effects(0).verifyDeploymentReceiptPreimage(before, root)
        effects(0).verifyDeploymentReceiptReadback(reserved, root)
        effects(1).verifyDeploymentReceiptPreimage(reserved, root)
        effects(1).verifyDeploymentReceiptReadback(executed, root)
      case _ => throw new SQLDataException("mode workflow crossed complete image families")
    verifyTerminalFields()

  private[mysql] def verifyTerminalFields(): Unit =
    require(plannedAt != null && plannedAt.getNano % 1000 == 0, "exact terminal planning timestamp required")
    requireWitness(reservedImage, plan.reserveOperationUuid, reserveWitness)
    requireWitness(executedImage, plan.executeOperationUuid, executeWitness)
    requireReservedExecuteNulls(reservedImage, plan.executeOperationUuid)
    requireTerminalTimes(reservedImage, executedImage, plannedAt, plan)

  private def requireWitness(snapshot: Image, operationUuid: UUID,
      expected: MySqlRecoveryCanonicalBytes): Unit =
    val row = findOperation(snapshot, operationUuid)
    if !expected.exactlyMatchesCell(MySqlModeOperationEffectCodec.value(
      row, "fp_recovery_operation", "commit_witness_bytes")) ||
      !expected.sha256Cell.equals(MySqlModeOperationEffectCodec.value(
        row, "fp_recovery_operation", "commit_witness_sha256")) then
      throw new SQLDataException("semantic operation witness differs from its fixed full20 phase inputs")

  private def requireReservedExecuteNulls(snapshot: Image, operationUuid: UUID): Unit =
    val row = findOperation(snapshot, operationUuid)
    if MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "operation_state") !=
      MySqlFormat3Cell.Text("RESERVED") ||
      MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "result_at") != MySqlFormat3Cell.SqlNull ||
      MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "settled_by_operation_uuid") != MySqlFormat3Cell.SqlNull ||
      MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "commit_witness_bytes") != MySqlFormat3Cell.SqlNull ||
      MySqlModeOperationEffectCodec.value(row, "fp_recovery_operation", "commit_witness_sha256") != MySqlFormat3Cell.SqlNull then
      throw new SQLDataException("reserve target must preserve the not-yet-executed operation as SQL NULL")

  private def requireTerminalTimes(reserve: Image, execute: Image,
      fixed: LocalDateTime, fixedPlan: MySqlOwnedRepositoryOperationPlan): Unit =
    val reserveRow = findOperation(reserve, fixedPlan.reserveOperationUuid)
    val reservedExecute = findOperation(reserve, fixedPlan.executeOperationUuid)
    val committedExecute = findOperation(execute, fixedPlan.executeOperationUuid)
    val expected = MySqlFormat3Cell.DateTime(fixed)
    if MySqlModeOperationEffectCodec.value(reserveRow, "fp_recovery_operation", "planned_at") != expected ||
      MySqlModeOperationEffectCodec.value(reserveRow, "fp_recovery_operation", "result_at") != expected ||
      MySqlModeOperationEffectCodec.value(reservedExecute, "fp_recovery_operation", "planned_at") != expected ||
      MySqlModeOperationEffectCodec.value(committedExecute, "fp_recovery_operation", "planned_at") != expected ||
      MySqlModeOperationEffectCodec.value(committedExecute, "fp_recovery_operation", "result_at") != expected then
      throw new SQLDataException("mode operation timestamps must retain the single fixed database plannedAt marker")

  private def findOperation(snapshot: Image, operationUuid: UUID): Vector[MySqlFormat3Cell] =
    MySqlModeOperationEffectCodec.operationRows(snapshot, operation)
      .find(row => MySqlModeOperationEffectCodec.rowOperationUuid(row) == operationUuid)
      .getOrElse(throw new SQLDataException("fixed mode operation row is missing from its full20 phase"))

/** 纯物化逻辑。运行时调用者只能从活动的 PlanningReadOwner 取得输入；此函数本身不签发时钟权威。 */
private[mysql] object MySqlModeWorkflowMaterializer:
  private val Domain = "com.wi.floatingpod.mysql-mode-semantic-witness/v1"
  private val Encoding = "FP_MYSQL_MODE_SEMANTIC_WITNESS_BE_V1"
  private val OperationTable = "fp_recovery_operation"
  private val ReservationTable = "fp_recovery_reservation"
  private val WriterTable = "fp_runtime_writer"
  private val DomainTable = "floating_pod_state"
  private val LocatorTable = "physical_recovery_locator"
  private val FootprintTable = "physical_recovery_footprint"

  private final case class WorkflowRows(
      reserveRow: Vector[MySqlFormat3Cell],
      reservedExecuteRow: Vector[MySqlFormat3Cell],
      reservationRow: Vector[MySqlFormat3Cell],
      footprintUuid: UUID
  )

  /** 纯行为测试显式传入 plannedAt；运行时只由 fenced runner 传入锁定 W 查询返回的时间。 */
  private[mysql] def materialize(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: MySqlFormat3FullSnapshot,
      locatorRevision: Long): MySqlModeWorkflowMaterialization =
    materializeInternal(plan, operation, context, plannedAt, before, locatorRevision, None, MySqlModeProjectionVersion.V2, before.allocationBudget).get

  /** 真producer先由既有registry登记draft，此入口不发行SQL或native能力。 */
  private[mysql] def materialize(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: MySqlFormat3FullSnapshot,
      locatorRevision: Long, draft: MySqlModeOriginalWitnessDraft): MySqlModeWorkflowMaterialization =
    require(draft != null, "registered original mode draft required")
    draft.requireProducerInputs(plan, operation, context, before)
    try materializeInternal(plan, operation, context, plannedAt, before, locatorRevision, Some(draft), MySqlModeProjectionVersion.V2, before.allocationBudget).get
    catch
      case cause: Throwable =>
        draft.recordFailure(cause)
        throw cause

  private[mysql] def materializeDeploymentReceipt(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: MySqlScriptSchemaSnapshot, locatorRevision: Long,
      root: MySqlFormat3ReadBudget, draft: MySqlModeOriginalWitnessDraft): MySqlModeWorkflowMaterialization =
    require(plan != null && before != null && root != null && draft != null && (draft.originalRoot eq root),
      "actual Full26 mode source and registered original draft/root required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before, before.inputs, root)
    if context == null || context.schemaFormat != 5 || context.physicalFormat != 2 ||
      (plan.retainedHandle.map(_.issuer.rootForOriginal(plan)).getOrElse(root) ne root) then
      throw new SQLDataException("Full26 mode materialization lost exact original plan/context/root")
    draft.requireProducerInputs(plan, operation, context, before)
    try materializeInternal(plan, operation, context, plannedAt, before, locatorRevision, Some(draft),
      MySqlModeProjectionVersion.V2, root, originalRootOverride = Some(root)).get
    catch
      case cause: Throwable =>
        draft.recordFailure(cause)
        throw cause

  private def retainSnapshot(draft: Option[MySqlModeOriginalWitnessDraft],
      stage: MySqlModeWitnessProducerStage, image: Image): Unit =
    draft.foreach { owner => image match
      case legacy: MySqlFormat3FullSnapshot => owner.retainProducerSnapshot(stage, legacy)
      case deployment: MySqlScriptSchemaSnapshot => owner.retainProducerSnapshot(stage, deployment)
    }

  /** 历史DATA只比较；child拥有全部新重建值，Unit返回禁止这些值逃逸到原receipt。 */
  private[mysql] def requireHistorical(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, initial: Image, locatorRevision: Long,
      version: MySqlModeProjectionVersion, phase: MySqlOperationEffectPhase,
      before: Image, target: Image,
      originalReserve: Vector[MySqlFormat3Cell], original: Vector[MySqlFormat3Cell],
      expectedContext: MySqlOperationContext, root: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget &&
      (initial.allocationBudget eq root) && (before.allocationBudget eq root) && (target.allocationBudget eq root),
      "historical mode comparison requires exact original image root")
    (initial,before,target) match
      case (a:MySqlScriptSchemaSnapshot,b:MySqlScriptSchemaSnapshot,c:MySqlScriptSchemaSnapshot)=>
        require(context.schemaFormat==5 && version==MySqlModeProjectionVersion.V2 &&
          (a.inputs eq b.inputs) && (a.inputs eq c.inputs),"historical Full26 mode requires exact original inputs")
        Vector(a,b,c).foreach(image=>MySqlFormat3QueryProjection.requireDeploymentSource(image,a.inputs,root))
      case (_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot)=>
        require(context.schemaFormat==3,"historical legacy mode requires schema3 context")
      case _=>throw new SQLDataException("historical mode crossed image families")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child = MySqlModeOperationEffectCodec.allocationOwner(root)
      require(child.isCanonicalComparisonChildOf(root), "historical mode requires original comparison child")
      child.reserveRetainedMemory(512L, "historical expected full-image comparison inputs")
      val expected = new HistoricalExpected(phase, before, target, originalReserve, original, expectedContext)
      materializeInternal(plan, operation, context, plannedAt, initial, locatorRevision, None, version, child, Some(expected))
      ()
    }

  /** H3 Full26 only: keep original-root graph authority while fresh history DATA uses its registered plan child. */
  private[mysql] def requireHistorical(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, initial: Image, locatorRevision: Long,
      version: MySqlModeProjectionVersion, phase: MySqlOperationEffectPhase,
      before: Image, target: Image,
      originalReserve: Vector[MySqlFormat3Cell], original: Vector[MySqlFormat3Cell],
      expectedContext: MySqlOperationContext, root: MySqlFormat3ReadBudget,
      planDataBudget: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && planDataBudget != null &&
      !planDataBudget.isClosed && (planDataBudget.profile eq root.profile) &&
      planDataBudget.isCanonicalChildOf(root) && planDataBudget.sharesAllocationLedger(root) &&
      initial.isInstanceOf[MySqlScriptSchemaSnapshot] && before.isInstanceOf[MySqlScriptSchemaSnapshot] &&
      target.isInstanceOf[MySqlScriptSchemaSnapshot] && (initial.allocationBudget eq planDataBudget) &&
      (before.allocationBudget eq planDataBudget) && (target.allocationBudget eq planDataBudget),
      "H3 Full26 history requires exact original root and same-ledger registered DATA child graphs")
    val images = Vector(initial, before, target).map(_.asInstanceOf[MySqlScriptSchemaSnapshot])
    val inputs = images.head.inputs
    require(context != null && context.schemaFormat == 5 && version == MySqlModeProjectionVersion.V2 &&
      images.forall(image => image.inputs eq inputs), "historical Full26 mode requires exact original inputs")
    images.foreach(image => MySqlFormat3QueryProjection.requireDeploymentSource(image, inputs, root))
    val expected = new HistoricalExpected(phase, before, target, originalReserve, original, expectedContext)
    materializeInternal(plan, operation, context, plannedAt, initial, locatorRevision, None, version,
      planDataBudget, Some(expected), Some(root), deferHistoricalValidation = true)
    ()

  private final class HistoricalExpected(val phase: MySqlOperationEffectPhase,
      val before: Image, val target: Image,
      val reserve: Vector[MySqlFormat3Cell], val original: Vector[MySqlFormat3Cell], val context: MySqlOperationContext):
    var generatedReserve: MySqlModePhaseCanonical = null
    var generatedExecute: MySqlModePhaseCanonical = null
    var generatedReserveTarget: Image = null
    var generatedExecuteTarget: Image = null

  private def materializeInternal(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image,
      locatorRevision: Long, draft: Option[MySqlModeOriginalWitnessDraft],
      version: MySqlModeProjectionVersion, allocation: MySqlFormat3ReadBudget,
      expected: Option[HistoricalExpected] = None,
      originalRootOverride: Option[MySqlFormat3ReadBudget] = None,
      deferHistoricalValidation: Boolean = false): Option[MySqlModeWorkflowMaterialization] =
    if before == null then throw new SQLDataException("original full20 root required before mode allocation")
    val originalRoot = originalRootOverride.getOrElse(before.allocationBudget)
    before match
      case deployment: MySqlScriptSchemaSnapshot =>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, originalRoot)
      case legacy: MySqlFormat3FullSnapshot =>
        if legacy.allocationBudget ne originalRoot then
          throw new SQLDataException("legacy mode producer lost exact original root")
    require(!originalRoot.isClosed && !originalRoot.isScopedReadBudget && allocation != null && !allocation.isClosed &&
      (allocation.profile eq originalRoot.profile) && ((allocation eq originalRoot) ||
        (draft.isEmpty && expected.nonEmpty && allocation.isCanonicalChildOf(originalRoot))),
      "exact mode allocation owner required")
    given MySqlFormat3ReadBudget = allocation
    given Option[MySqlModeOriginalWitnessDraft] = draft
    def produce(): Option[MySqlModeWorkflowMaterialization] =
      allocation.reserveRetainedMemory(512L, "mode producer entry/tuple structures before allocation")
      validateInputs(plan, operation, context, plannedAt, before, locatorRevision)
      val request = MySqlModeOperationEffectCodec.modeRequest(operation)
      val profile = before.limitProfile
      val scope = MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlRecoveryOperationScope.encode(context, Vector(MySqlRecoveryResourceRevision(
          MySqlRecoveryResourceKey.pod(request.podUuid, profile), locatorRevision, Some(request.attemptUuid))))
      }
      draft.foreach(_.retainProducerCanonical(MySqlModeWitnessProducerStage.Scope, scope))
      val footprintUuid = currentFootprintUuid(before, request)
      val seed = MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) { seedCanonical(profile) }
      draft.foreach(_.retainProducerCanonical(MySqlModeWitnessProducerStage.Seed, seed))
      val rows = initialRows(plan, request, context, plannedAt, before, scope, seed, footprintUuid, locatorRevision, draft)
      buildPhases(plan, operation, context, plannedAt, before, locatorRevision, scope, rows, seed, draft, version,
        expected, originalRoot, deferHistoricalValidation)
    val produced =
      if deferHistoricalValidation then produce()
      else MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) { produce() }
    if deferHistoricalValidation then
      val owner = expected.getOrElse(throw new SQLDataException("deferred historical validation requires its exact DATA input"))
      require(owner.generatedReserve != null && owner.generatedExecute != null &&
        owner.generatedReserveTarget != null && owner.generatedExecuteTarget != null,
        "deferred H3 Full26 mode validation inputs were not retained")
      MySqlModeOperationEffectCodec.requireHistoricalPhaseGraphs(plan, operation, context, locatorRevision,
        before, owner.generatedReserveTarget, owner.generatedExecuteTarget, owner.generatedReserve,
        owner.generatedExecute, originalRoot, allocation)
      None
    else produced

  private def reserveVector(size: Int, label: String)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(size.toLong, 64L)), label)
  private def withTemporary[A](amount: Long, label: String)(body: => A)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): A =
    budget.reserveTemporaryMemory(amount, label)
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try budget.releaseTemporaryMemory(amount, label + " dropped")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  private def canonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: Image, target: Image,
      version: MySqlModeProjectionVersion, originalRoot: MySqlFormat3ReadBudget)
      (using budget: MySqlFormat3ReadBudget): MySqlModePhaseCanonical =
    (before, target) match
      case (deployment: MySqlScriptSchemaSnapshot, deploymentTarget: MySqlScriptSchemaSnapshot) =>
        if version != MySqlModeProjectionVersion.V2 then
          throw new SQLDataException("Full26 mode phase requires original V2 projection framing")
        if budget eq originalRoot then
          MySqlModeOperationEffectCodec.canonicalDeploymentReceiptPhase(plan, operation, context, locatorRevision,
            phase, deployment, deploymentTarget, originalRoot)
        else
          MySqlModeOperationEffectCodec.canonicalDeploymentReceiptPhase(plan, operation, context, locatorRevision,
            phase, deployment, deploymentTarget, originalRoot, budget)
      case (_: MySqlFormat3FullSnapshot, _: MySqlFormat3FullSnapshot) =>
        MySqlModeOperationEffectCodec.canonicalPhase(plan, operation, context, locatorRevision,
          phase, before, target, version)
      case _ => throw new SQLDataException("mode phase cannot cross complete image families")

  private def buildPhases(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, locatorRevision: Long,
      scope: MySqlRecoveryCanonicalBytes, rows: WorkflowRows,
      seed: MySqlRecoveryCanonicalBytes, draft: Option[MySqlModeOriginalWitnessDraft], version: MySqlModeProjectionVersion,
      expected: Option[HistoricalExpected], originalRoot: MySqlFormat3ReadBudget,
      deferHistoricalValidation: Boolean)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Option[MySqlModeWorkflowMaterialization] =
    val reservePreimage = before match
      case deployment: MySqlScriptSchemaSnapshot =>
        if budget eq originalRoot then
          MySqlModeOperationEffectCodec.projectImage(deployment, deployment.inputs, originalRoot, plan, operation,
            context, locatorRevision, MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before)
        else
          MySqlModeOperationEffectCodec.projectImage(deployment, deployment.inputs, originalRoot, plan, operation,
            context, locatorRevision, MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before, budget)
      case _: MySqlFormat3FullSnapshot => MySqlModeOperationEffectCodec.projectImage(before, plan, operation,
        context, locatorRevision, MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before, version)
    draft.foreach(_.retainProducerCanonical(MySqlModeWitnessProducerStage.ReservePreimage, reservePreimage))
    val reserveRows = rows.copy(reserveRow = putPair(rows.reserveRow, "preimage_bytes", "preimage_sha256", reservePreimage))
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ReserveRow, reserveRows.reserveRow))
    val reserveScaffold = reserveScaffoldSnapshot(before, plan, reserveRows, draft, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ReserveScaffold, reserveScaffold)
    val reservePhase = canonicalPhase(plan, operation, context, locatorRevision,
      MySqlOperationEffectPhase.Reserve, before, reserveScaffold, version, originalRoot)
    draft.foreach(_.retainProducerPhase(MySqlModeWitnessProducerStage.ReservePhase, reservePhase))
    val reserveWitness = semanticWitness(plan, operation, context, plannedAt,
      MySqlOperationEffectPhase.Reserve, reservePhase, before.limitProfile, None)
    draft.foreach(_.retainProducerCanonical(MySqlModeWitnessProducerStage.ReserveWitness, reserveWitness))
    val reserveCommitted = withPhaseValues(reserveRows.reserveRow, plan, scope, reservePhase)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ReserveCommittedRow, reserveCommitted))
    val reserveFinal = putPair(reserveCommitted, "commit_witness_bytes", "commit_witness_sha256", reserveWitness)

    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ReserveFinalRow, reserveFinal))
    val reserveWriter = withWriterReceipt(reserveScaffold, plan.reserveOperationUuid, reservePhase.fingerprint.sha256,
      draft, MySqlModeWitnessProducerStage.ReserveWriterBeforeDigest, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ReserveWriter, reserveWriter)
    val reserveTargetBase = withRelations(reserveWriter, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(reserveWriter.relation(OperationTable).rows, reserveFinal))
    ), draft, MySqlModeWitnessProducerStage.ReserveTargetBaseBeforeDigest, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ReserveTargetBase, reserveTargetBase)
    val executeScaffold = executeScaffoldSnapshot(reserveTargetBase, plan, operation, context, plannedAt, scope,
      rows, locatorRevision, seed, draft, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ExecuteScaffold, executeScaffold)
    val executePhase = canonicalPhase(plan, operation, context, locatorRevision,
      MySqlOperationEffectPhase.Execute, reserveTargetBase, executeScaffold, version, originalRoot)
    draft.foreach(_.retainProducerPhase(MySqlModeWitnessProducerStage.ExecutePhase, executePhase))
    val executeReserved = withPhaseValues(rows.reservedExecuteRow, plan, scope, executePhase)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteReservedRow, executeReserved))
    val reserveTarget = withRelations(reserveTargetBase, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(reserveTargetBase.relation(OperationTable).rows, executeReserved))
    ), draft, MySqlModeWitnessProducerStage.ReserveTargetBeforeDigest, Some(originalRoot))

    retainSnapshot(draft, MySqlModeWitnessProducerStage.ReserveTarget, reserveTarget)
    val reserveEnvelope = MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      MySqlRecoveryOperationRawEnvelopeCodec.encode(reserveFinal, before.limitProfile, inputsForImage(before, originalRoot))
    }
    draft.foreach(_.retainProducerRaw(MySqlModeWitnessProducerStage.ReserveRaw, reserveEnvelope))
    val executeWitness = semanticWitness(plan, operation, context, plannedAt,
      MySqlOperationEffectPhase.Execute, executePhase, before.limitProfile, Some(reserveEnvelope))
    draft.foreach(_.retainProducerCanonical(MySqlModeWitnessProducerStage.ExecuteWitness, executeWitness))
    val executeCommittedDraft = operationRow(plan, MySqlModeOperationEffectCodec.modeRequest(operation),
      context, plannedAt, scope, seed, rows.footprintUuid, plan.executeOperationUuid,
      "DOMAIN_MUTATION", "COMMITTED", Some(plan.reserveOperationUuid), withWitness = true,
      locatorRevision = locatorRevision)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteCommittedDraft, executeCommittedDraft))
    val executeCommitted = withPhaseValues(executeCommittedDraft, plan, scope, executePhase)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteCommittedRow, executeCommitted))
    val executeTerminal = putPair(executeCommitted, "commit_witness_bytes", "commit_witness_sha256", executeWitness)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteTerminalRow, executeTerminal))
    val executeWriter = withWriterReceipt(executeScaffold, plan.executeOperationUuid, executePhase.fingerprint.sha256,
      draft, MySqlModeWitnessProducerStage.ExecuteWriterBeforeDigest, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ExecuteWriter, executeWriter)
    val executeTarget = withRelations(executeWriter, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(executeWriter.relation(OperationTable).rows, executeTerminal))
    ), draft, MySqlModeWitnessProducerStage.ExecuteTargetBeforeDigest, Some(originalRoot))
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ExecuteTarget, executeTarget)
    expected match
      case Some(e) =>
        require(draft.isEmpty && (budget.isCanonicalComparisonChildOf(originalRoot) ||
          (deferHistoricalValidation && budget.isCanonicalChildOf(originalRoot))),
          "Unit mode result cannot escape its original ledger comparison/plan child")
        if deferHistoricalValidation then
          e.generatedReserve = reservePhase
          e.generatedExecute = executePhase
          e.generatedReserveTarget = reserveTarget
          e.generatedExecuteTarget = executeTarget
        else
          MySqlModeOperationEffectCodec.requireHistoricalPhaseGraphs(plan, operation, context, locatorRevision,
            before, reserveTarget, executeTarget, reservePhase, executePhase)
        budget.reserveRetainedMemory(512L, "historical terminal-only validation wrapper")
        val terminal = new MySqlModeWorkflowMaterialization(plannedAt, before, reserveTarget, executeTarget,
          Vector.empty, reserveWitness, executeWitness, plan, operation)
        terminal.verifyTerminalFields()
        val actualTarget = if e.phase == MySqlOperationEffectPhase.Reserve then reserveTarget else executeTarget
        val actualContext = (if e.phase == MySqlOperationEffectPhase.Reserve then reservePhase else executePhase).operationContext
        if actualTarget.relations != e.target.relations ||
          (e.phase == MySqlOperationEffectPhase.Execute && reserveTarget.relations != e.before.relations) ||
          !reserveTarget.relation(OperationTable).rows.contains(e.reserve) ||
          !actualTarget.relation(OperationTable).rows.contains(e.original) || actualContext != e.context then
          throw new SQLDataException("historical complete mode RAW/context/target differs from original algorithm")
        if deferHistoricalValidation then Some(terminal) else None
      case None =>
        val effects = MySqlModeOperationEffectCodec.buildMaterializedWorkflow(plan, operation, context, locatorRevision,
          before, reserveTarget, executeTarget, reservePhase, executePhase, reserveEnvelope, draft)
        draft.foreach(_.retainProducerEffects(effects))
        val result = new MySqlModeWorkflowMaterialization(plannedAt, before, reserveTarget, executeTarget,
          effects, reserveWitness, executeWitness, plan, operation)
        draft.foreach { retained =>
          result.originalLocatorRevision = locatorRevision
          retained.bind(result)
          result.originalWitnessDraft = retained
        }
        result.verify()
        Some(result)

  private def validateInputs(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, locatorRevision: Long)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    if plan == null || operation == null || context == null || plannedAt == null || before == null ||
      locatorRevision < 0L || plannedAt.getNano % 1000 != 0 ||
      plannedAt.getYear < 1000 || plannedAt.getYear > 9999 then
      throw new SQLDataException("fixed mode planning inputs require a complete plan, full20 image and DATETIME(6)")
    if context.exactLimitProfile ne before.limitProfile then
      throw new SQLDataException("fixed mode planning profile differs from the full20 image")
    MySqlModeOperationEffectCodec.modeRequest(operation)
    plan.requireSingle(operation, before.limitProfile)

  private def seedCanonical(profile: SourceLimitProfile)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlRecoveryCanonicalBytes =
    MySqlRecoveryCanonicalBytes.encode("FP_MODE_PLANNING_SEED_BE_V1", profile) { output =>
      MySqlRecoveryCanonicalBytes.writeToken(output, "FP_MODE_PLANNING_SEED_V1")
      MySqlModeEnvelopeV2Census.int(output, 0)
    }

  private def initialRows(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlModeOperationEffectCodec.Request, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, scope: MySqlRecoveryCanonicalBytes,
      seed: MySqlRecoveryCanonicalBytes, footprintUuid: UUID, locatorRevision: Long,
      draft: Option[MySqlModeOriginalWitnessDraft])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): WorkflowRows =
    val reserve = operationRow(plan, request, context, plannedAt, scope, seed, footprintUuid,
      plan.reserveOperationUuid, "RESERVE", "COMMITTED", None, withWitness = true,
      locatorRevision = locatorRevision)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.InitialReserveRow, reserve))
    val execute = operationRow(plan, request, context, plannedAt, scope, seed, footprintUuid,
      plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED", Some(plan.reserveOperationUuid),
      withWitness = false, locatorRevision = locatorRevision)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.InitialExecuteRow, execute))
    val reservation = reservationRow(before, plan, request)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.InitialReservationRow, reservation))
    WorkflowRows(reserve, execute, reservation, footprintUuid)

  private def operationRow(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlModeOperationEffectCodec.Request, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, scope: MySqlRecoveryCanonicalBytes, seed: MySqlRecoveryCanonicalBytes,
      footprintUuid: UUID, operationUuid: UUID, step: String, state: String,
      parent: Option[UUID], withWitness: Boolean, locatorRevision: Long)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(OperationTable))
    row = put(row, OperationTable, "binding_uuid", uuidCell(request.binding.bindingUuid))
    row = put(row, OperationTable, "operation_uuid", uuidCell(operationUuid))
    row = put(row, OperationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
    row = put(row, OperationTable, "step", newTypedCell(MySqlFormat3Cell.Text(step)))
    row = put(row, OperationTable, "operation_state", newTypedCell(MySqlFormat3Cell.Text(state)))
    row = put(row, OperationTable, "pod_uuid", uuidCell(request.podUuid))
    row = put(row, OperationTable, "attempt_uuid", uuidCell(request.attemptUuid))
    row = put(row, OperationTable, "footprint_uuid", uuidCell(footprintUuid))
    row = put(row, OperationTable, "source_unit_uuid", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "forest_uuid", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "boot_uuid", uuidCell(context.tag.bootId))
    row = put(row, OperationTable, "boot_generation", newTypedCell(MySqlFormat3Cell.SignedInteger(context.tag.bootGeneration)))
    row = put(row, OperationTable, "connection_generation_uuid", uuidCell(context.barrierGenerationUuid))
    row = put(row, OperationTable, "connection_id", newTypedCell(MySqlFormat3Cell.SignedInteger(context.actualConnectionId)))
    row = put(row, OperationTable, "fence_epoch", newTypedCell(MySqlFormat3Cell.SignedInteger(context.writerEpoch)))
    row = putPair(row, "request_identity_bytes", "request_identity_sha256", plan.requestIdentity.canonical)
    row = put(row, OperationTable, "expected_domain_version", newTypedCell(MySqlFormat3Cell.SignedInteger(request.expectedStateVersion)))
    row = put(row, OperationTable, "expected_locator_revision", newTypedCell(MySqlFormat3Cell.SignedInteger(locatorRevision)))
    row = put(row, OperationTable, "expected_source_revision", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "context_encoding", newTypedCell(MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1")))
    row = putPair(row, "original_context_bytes", "original_context_sha256", seed)
    row = putPair(row, "scope_bytes", "scope_sha256", scope)
    row = putPair(row, "preimage_bytes", "preimage_sha256", seed)
    row = putPair(row, "target_bytes", "target_sha256", seed)
    row = put(row, OperationTable, "reserve_operation_uuid", parent.map(uuidCell).getOrElse(MySqlFormat3Cell.SqlNull))
    row = put(row, OperationTable, "previous_step_operation_uuid", parent.map(uuidCell).getOrElse(MySqlFormat3Cell.SqlNull))
    row = put(row, OperationTable, "settled_by_operation_uuid", MySqlFormat3Cell.SqlNull)
    if withWitness then row = putPair(row, "commit_witness_bytes", "commit_witness_sha256", seed)
    else
      row = put(row, OperationTable, "commit_witness_bytes", MySqlFormat3Cell.SqlNull)
      row = put(row, OperationTable, "commit_witness_sha256", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "original_diagnostic_sha256", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "original_diagnostic_identity", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "planned_at", newTypedCell(MySqlFormat3Cell.DateTime(plannedAt)))
    row = put(row, OperationTable, "fixed_lease_until", newTypedCell(MySqlFormat3Cell.DateTime(context.fixedLeaseUntil)))
    row = put(row, OperationTable, "result_at",
      if state == "COMMITTED" then newTypedCell(MySqlFormat3Cell.DateTime(plannedAt)) else MySqlFormat3Cell.SqlNull)
    put(row, OperationTable, "operation_fingerprint", zeroDigestCell())

  private def withPhaseValues(row: Vector[MySqlFormat3Cell], plan: MySqlOwnedRepositoryOperationPlan,
      scope: MySqlRecoveryCanonicalBytes, phase: MySqlModePhaseCanonical)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    var next = putPair(row, "request_identity_bytes", "request_identity_sha256", plan.requestIdentity.canonical)
    next = putPair(next, "scope_bytes", "scope_sha256", scope)
    next = putPair(next, "original_context_bytes", "original_context_sha256", phase.operationContextBytes)
    next = putPair(next, "preimage_bytes", "preimage_sha256", phase.preimage)
    next = putPair(next, "target_bytes", "target_sha256", phase.target)
    put(next, OperationTable, "operation_fingerprint", vectorCell(phase.fingerprint.sha256))

  private def reserveScaffoldSnapshot(before: Image,
      plan: MySqlOwnedRepositoryOperationPlan, rows: WorkflowRows,
      draft: Option[MySqlModeOriginalWitnessDraft],
      originalRoot: Option[MySqlFormat3ReadBudget] = None)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Image =
    val operationRows = before.relation(OperationTable).rows
    val newIds = Set(plan.reserveOperationUuid, plan.executeOperationUuid)
    if operationRows.exists(row =>
      newIds.contains(MySqlModeOperationEffectCodec.rowOperationUuid(row)) &&
        MySqlModeOperationEffectCodec.value(row, OperationTable, "binding_uuid") ==
          MySqlModeOperationEffectCodec.value(rows.reserveRow, OperationTable, "binding_uuid")) then
      throw new java.sql.SQLTransactionRollbackException("fixed mode operation UUID already has an O history row")
    val reservePlaceholder = withWriterReceipt(before, plan.reserveOperationUuid, Vector.fill(32)(0.toByte),
      draft, MySqlModeWitnessProducerStage.ReserveScaffoldWriterBeforeDigest, originalRoot)
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ReserveScaffoldWriter, reservePlaceholder)
    val allOperationRows = orderedRows(OperationTable, operationRows ++ Vector(rows.reserveRow, rows.reservedExecuteRow))
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ReserveOperationRows, allOperationRows))
    val reservationRows = reservationRowsFor(before, rows.reservationRow)
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ReserveReservationRows, reservationRows))
    withRelations(reservePlaceholder, Map(
      OperationTable -> allOperationRows,
      ReservationTable -> reservationRows
    ), draft, MySqlModeWitnessProducerStage.ReserveScaffoldBeforeDigest, originalRoot)

  private def executeScaffoldSnapshot(reserveBefore: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime,
      scope: MySqlRecoveryCanonicalBytes, rows: WorkflowRows, locatorRevision: Long,
      seed: MySqlRecoveryCanonicalBytes, draft: Option[MySqlModeOriginalWitnessDraft],
      originalRoot: Option[MySqlFormat3ReadBudget] = None)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Image =
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    val domain = exactPodRow(reserveBefore, DomainTable, request.binding.bindingUuid, request.podUuid)
    val locator = exactPodRow(reserveBefore, LocatorTable, request.binding.bindingUuid, request.podUuid)
    val released = releasedReservation(rows.reservationRow, plan.executeOperationUuid)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteReleasedReservation, released))
    val committedExecute = operationRow(plan, request, context, plannedAt, scope, seed, rows.footprintUuid,
      plan.executeOperationUuid, "DOMAIN_MUTATION", "COMMITTED", Some(plan.reserveOperationUuid),
      withWitness = true, locatorRevision = locatorRevision)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteScaffoldCommittedRow, committedExecute))
    val operationRows = replaceByUuid(reserveBefore.relation(OperationTable).rows, committedExecute)
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ExecuteOperationRows, operationRows))
    val writer = withWriterReceipt(reserveBefore, plan.executeOperationUuid, Vector.fill(32)(0.toByte),
      draft, MySqlModeWitnessProducerStage.ExecuteScaffoldWriterBeforeDigest, originalRoot)
    retainSnapshot(draft, MySqlModeWitnessProducerStage.ExecuteScaffoldWriter, writer)
    val changedDomainRow = changedDomain(domain, request, plannedAt, plan.executeOperationUuid)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteChangedDomain, changedDomainRow))
    val domainRows = orderedRows(DomainTable, replaceExactPod(reserveBefore, DomainTable, domain, changedDomainRow))
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ExecuteDomainRows, domainRows))
    val changedLocatorRow = changedLocator(locator, request, plan.executeOperationUuid)
    draft.foreach(_.retainProducerRow(MySqlModeWitnessProducerStage.ExecuteChangedLocator, changedLocatorRow))
    val locatorRows = orderedRows(LocatorTable, replaceExactPod(reserveBefore, LocatorTable, locator, changedLocatorRow))
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ExecuteLocatorRows, locatorRows))
    val orderedOperationRows = orderedRows(OperationTable, operationRows)
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ExecuteOrderedOperationRows, orderedOperationRows))
    val reservationRows = reservationRowsFor(reserveBefore, released)
    draft.foreach(_.retainProducerRelationRows(MySqlModeWitnessProducerStage.ExecuteReservationRows, reservationRows))
    withRelations(writer, Map(
      DomainTable -> domainRows,
      LocatorTable -> locatorRows,
      OperationTable -> orderedOperationRows,
      ReservationTable -> reservationRows
    ), draft, MySqlModeWitnessProducerStage.ExecuteScaffoldBeforeDigest, originalRoot)

  private def reservationRow(before: Image,
      plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlModeOperationEffectCodec.Request)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    val podKind = newTypedCell(MySqlFormat3Cell.Text("POD"))
    val existing = before.relation(ReservationTable).rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, ReservationTable, "binding_uuid"), request.binding.bindingUuid) &&
        MySqlModeOperationEffectCodec.value(row, ReservationTable, "resource_kind") == podKind &&
        MySqlModeOperationEffectCodec.uuidEquals(
          MySqlModeOperationEffectCodec.value(row, ReservationTable, "resource_pod_uuid"), request.podUuid))
    if existing.size > 1 then throw new SQLDataException("mode plan found duplicate historical POD reservation rows")
    existing.headOption match
      case Some(old) =>
        if MySqlModeOperationEffectCodec.value(old, ReservationTable, "reservation_state") != newTypedCell(MySqlFormat3Cell.Text("RELEASED")) then
          throw new java.sql.SQLTransactionRollbackException("held POD reservation retains its prior workflow responsibility")
        var row = put(old, ReservationTable, "reservation_revision",
          newTypedCell(MySqlFormat3Cell.SignedInteger(Math.addExact(signed(old, ReservationTable, "reservation_revision"), 1L))))
        row = put(row, ReservationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
        row = put(row, ReservationTable, "operation_uuid", uuidCell(plan.executeOperationUuid))
        row = put(row, ReservationTable, "reservation_state", newTypedCell(MySqlFormat3Cell.Text("HELD")))
        put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(plan.reserveOperationUuid))
      case None =>
        val resource = MySqlRecoveryResourceKey.pod(request.podUuid, before.limitProfile)
        val canonicalKey = MySqlRecoveryCanonicalBytes.encode(
          "FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", before.limitProfile)(resource.writeCanonicalTo)
        if canonicalKey.sha256 != resource.keyHashSha256 then
          throw new SQLDataException("POD reservation canonical key digest differs from its fixed resource identity")
        var row = defaultRow(MySqlFormat3RelationShapes.forTable(ReservationTable))
        row = put(row, ReservationTable, "binding_uuid", uuidCell(request.binding.bindingUuid))
        row = put(row, ReservationTable, "resource_kind", newTypedCell(MySqlFormat3Cell.Text("POD")))
        row = put(row, ReservationTable, "resource_key_hash", newDigestCell(canonicalKey))
        row = put(row, ReservationTable, "resource_key", canonicalKey.binaryCell)
        row = put(row, ReservationTable, "reservation_revision", newTypedCell(MySqlFormat3Cell.SignedInteger(0L)))
        row = put(row, ReservationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
        row = put(row, ReservationTable, "operation_uuid", uuidCell(plan.executeOperationUuid))
        row = put(row, ReservationTable, "reservation_state", newTypedCell(MySqlFormat3Cell.Text("HELD")))
        row = put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(plan.reserveOperationUuid))
        row = put(row, ReservationTable, "resource_pod_uuid", uuidCell(request.podUuid))
        put(row, ReservationTable, "resource_unit_uuid", MySqlFormat3Cell.SqlNull)

  private def releasedReservation(held: Vector[MySqlFormat3Cell], operationUuid: UUID)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    val revision = Math.addExact(signed(held, ReservationTable, "reservation_revision"), 1L)
    var row = put(held, ReservationTable, "reservation_revision", newTypedCell(MySqlFormat3Cell.SignedInteger(revision)))
    row = put(row, ReservationTable, "reservation_state", newTypedCell(MySqlFormat3Cell.Text("RELEASED")))
    put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def reservationRowsFor(snapshot: Image,
      replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(ReservationTable)
    val replacementKey = shape.primaryKey.map(name => cell(replacement, ReservationTable, name))
    val source = snapshot.relation(ReservationTable).rows
    val matches = source.indices.filter(index =>
      shape.primaryKey.map(name => cell(source(index), ReservationTable, name)) == replacementKey)
    if matches.size > 1 then throw new SQLDataException("duplicate exact reservation primary key in full20 image")
    val result = matches.headOption match
      case Some(index) => source.updated(index, replacement)
      case None => source :+ replacement
    orderedRows(ReservationTable, result)

  private def changedDomain(old: Vector[MySqlFormat3Cell],
      request: MySqlModeOperationEffectCodec.Request, plannedAt: LocalDateTime,
      operationUuid: UUID)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    val pose = request.actualPose
    var row = put(old, DomainTable, "state_version",
      newTypedCell(MySqlFormat3Cell.SignedInteger(Math.addExact(request.expectedStateVersion, 1L))))
    row = put(row, DomainTable, "world_name", newTypedCell(MySqlFormat3Cell.Text(pose.worldName)))
    row = put(row, DomainTable, "pos_x", doubleCell(pose.posX))
    row = put(row, DomainTable, "pos_y", doubleCell(pose.posY))
    row = put(row, DomainTable, "pos_z", doubleCell(pose.posZ))
    row = put(row, DomainTable, "yaw", doubleCell(pose.yaw.toDouble))
    row = put(row, DomainTable, "pitch", doubleCell(pose.pitch.toDouble))
    row = put(row, DomainTable, "last_updated",
      newTypedCell(MySqlFormat3Cell.SignedInteger(plannedAt.toInstant(ZoneOffset.UTC).toEpochMilli)))
    row = put(row, DomainTable, "operating_mode", newTypedCell(MySqlFormat3Cell.Text(request.targetMode.databaseValue)))
    put(row, DomainTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def changedLocator(old: Vector[MySqlFormat3Cell],
      request: MySqlModeOperationEffectCodec.Request, operationUuid: UUID)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    var row = put(old, LocatorTable, "domain_version",
      newTypedCell(MySqlFormat3Cell.SignedInteger(Math.addExact(request.expectedStateVersion, 1L))))
    row = put(row, LocatorTable, "locator_revision",
      newTypedCell(MySqlFormat3Cell.SignedInteger(Math.addExact(signed(old, LocatorTable, "locator_revision"), 1L))))
    put(row, LocatorTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def replaceExactPod(snapshot: Image, table: String,
      old: Vector[MySqlFormat3Cell], replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    val key = shape.primaryKey.map(name => cell(old, table, name))
    val rows = snapshot.relation(table).rows
    val matches = rows.indices.filter(index => shape.primaryKey.map(name => cell(rows(index), table, name)) == key)
    if matches.size != 1 then throw new java.sql.SQLTransactionRollbackException("locked mode row no longer has one exact primary key")
    reserveVector(rows.size, "mode replaced O row references")
    rows.updated(matches.head, replacement)

  private def withWriterReceipt(snapshot: Image,
      operationUuid: UUID, fingerprint: Vector[Byte], draft: Option[MySqlModeOriginalWitnessDraft],
      nestedStage: MySqlModeWitnessProducerStage,
      originalRoot: Option[MySqlFormat3ReadBudget] = None)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Image =
    if fingerprint == null || fingerprint.size != 32 then throw new SQLDataException("fixed W fingerprint must contain 32 bytes")
    val writerRows = snapshot.relation(WriterTable).rows
    if writerRows.size != 1 then throw new java.sql.SQLTransactionRollbackException("full20 image has no unique writer row")
    var writer = put(writerRows.head, WriterTable, "last_commit_operation_uuid", uuidCell(operationUuid))
    writer = put(writer, WriterTable, "last_commit_fingerprint", vectorCell(fingerprint))
    withRelations(snapshot, Map(WriterTable -> Vector(writer)), draft, nestedStage, originalRoot)

  private def currentFootprintUuid(snapshot: Image,
      request: MySqlModeOperationEffectCodec.Request)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): UUID =
    val locator = exactPodRow(snapshot, LocatorTable, request.binding.bindingUuid, request.podUuid)
    val attempt = MySqlModeOperationEffectCodec.value(locator, LocatorTable, "current_attempt_uuid")
    val footprint = MySqlModeOperationEffectCodec.value(locator, LocatorTable, "current_footprint_uuid")
    val matches = snapshot.relation(FootprintTable).rows.filter(row =>
      MySqlModeOperationEffectCodec.value(row, FootprintTable, "binding_uuid") ==
        MySqlModeOperationEffectCodec.value(locator, LocatorTable, "binding_uuid") &&
      MySqlModeOperationEffectCodec.value(row, FootprintTable, "pod_uuid") ==
        MySqlModeOperationEffectCodec.value(locator, LocatorTable, "pod_uuid") &&
      MySqlModeOperationEffectCodec.value(row, FootprintTable, "attempt_uuid") == attempt &&
      MySqlModeOperationEffectCodec.value(row, FootprintTable, "footprint_uuid") == footprint)
    if matches.size != 1 || MySqlModeOperationEffectCodec.value(matches.head, FootprintTable, "seal_state") !=
      newTypedCell(MySqlFormat3Cell.Text("SEALED")) then
      throw new java.sql.SQLTransactionRollbackException("locked mode resource has no unique sealed current footprint")
    footprint match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        var most = 0L
        var least = 0L
        var index = 0
        while index < 16 do
          val part = bytes.byteAt(index).toLong & 0xffL
          if index < 8 then most = (most << 8) | part else least = (least << 8) | part
          index += 1
        budget.reserveRetainedMemory(64L, "mode decoded footprint UUID before allocation")
        new UUID(most, least)
      case _ => throw new SQLDataException("current footprint UUID must retain its exact 16-byte typed cell")

  private def exactPodRow(snapshot: Image, table: String,
      binding: UUID, pod: UUID)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(table).rows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, table, "binding_uuid"), binding) &&
      MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, table, if table == DomainTable then "uuid" else "pod_uuid"), pod))
    if rows.size != 1 then throw new java.sql.SQLTransactionRollbackException("locked mode row is not unique in " + table)
    rows.head

  private def semanticWitness(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, phase: MySqlOperationEffectPhase,
      canonical: MySqlModePhaseCanonical, profile: SourceLimitProfile,
      historicalReserve: Option[MySqlRecoveryOperationRawEnvelope])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlRecoveryCanonicalBytes =
    if phase == MySqlOperationEffectPhase.Reserve && historicalReserve.nonEmpty then
      throw new SQLDataException("reserve witness cannot refer to a future execute envelope")
    if phase == MySqlOperationEffectPhase.Execute && historicalReserve.isEmpty then
      throw new SQLDataException("execute witness must bind the exact committed reserve envelope")
    withTemporary(97L + 64L, "mode semantic profile copy") {
      val profileBytes = profile.canonicalBytes()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
        MySqlRecoveryCanonicalBytes.encode(Encoding, profile) { output =>
          writeSemantic(output, plan, operation, context, plannedAt, phase, canonical, historicalReserve, profileBytes)
        }
      }
      finally Arrays.fill(profileBytes, 0.toByte)
    }

  private[mysql] def writeSemantic(output: DataOutputStream, plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime,
      phase: MySqlOperationEffectPhase, canonical: MySqlModePhaseCanonical,
      historicalReserve: Option[MySqlRecoveryOperationRawEnvelope], profileBytes: Array[Byte],
      retainedContextBytes: MySqlRecoveryCanonicalBytes = null)
      (using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    MySqlModeEnvelopeV2Census.text(output, Domain)
    MySqlModeEnvelopeV2Census.byte(output, 1)
    MySqlRecoveryCanonicalBytes.writeToken(output,
      if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
    MySqlRecoveryCanonicalBytes.writeUuid(output, request.binding.bindingUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, request.podUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, request.owner)
    MySqlRecoveryCanonicalBytes.writeUuid(output, request.attemptUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, plan.workflowUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, plan.reserveOperationUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, plan.executeOperationUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output,
      if phase == MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid)
    MySqlModeEnvelopeV2Census.long(output, request.expectedStateVersion)
    MySqlModeEnvelopeV2Census.text(output, request.targetMode.databaseValue)
    MySqlModeEnvelopeV2Census.text(output, request.actualPose.worldName)
    MySqlModeEnvelopeV2Census.long(output, java.lang.Double.doubleToRawLongBits(request.actualPose.posX))
    MySqlModeEnvelopeV2Census.long(output, java.lang.Double.doubleToRawLongBits(request.actualPose.posY))
    MySqlModeEnvelopeV2Census.long(output, java.lang.Double.doubleToRawLongBits(request.actualPose.posZ))
    MySqlModeEnvelopeV2Census.int(output, java.lang.Float.floatToRawIntBits(request.actualPose.yaw))
    MySqlModeEnvelopeV2Census.int(output, java.lang.Float.floatToRawIntBits(request.actualPose.pitch))
    MySqlModeEnvelopeV2Census.int(output, request.actualPose.dimensionId)
    writeCanonical(output, plan.requestIdentity.canonical)
    writeBytes(output, profileBytes)
    writeCanonical(output, if retainedContextBytes == null then context.canonical else retainedContextBytes)
    MySqlRecoveryCanonicalBytes.writeUuid(output, context.barrierGenerationUuid)
    MySqlModeEnvelopeV2Census.long(output, context.actualConnectionId)
    MySqlModeEnvelopeV2Census.long(output, context.writerEpoch)
    MySqlRecoveryCanonicalBytes.writeUuid(output, context.bindingUuid)
    writeDateTime(output, context.fixedLeaseUntil)
    writeDateTime(output, plannedAt)
    MySqlRecoveryCanonicalBytes.writeToken(output, "FIXED_TERMINAL_RESULT_MARKER_V1")
    writeDateTime(output, plannedAt)
    writeCanonical(output, canonical.scope)
    writeCanonical(output, canonical.operationContextBytes)
    writeCanonical(output, canonical.fingerprint)
    writeCanonical(output, canonical.preimage)
    writeCanonical(output, canonical.target)
    historicalReserve match
      case Some(envelope) =>
        MySqlModeEnvelopeV2Census.bool(output, true)
        MySqlModeEnvelopeV2Census.int(output, envelope.canonical.length)
        MySqlModeEnvelopeV2Census.canonical(output, envelope.canonical)
        envelope.sha256.foreach(value => MySqlModeEnvelopeV2Census.byte(output, value.toInt & 0xff))
      case None => MySqlModeEnvelopeV2Census.bool(output, false)


  private def withRelations(snapshot: Image,
      requested: => Map[String, Vector[Vector[MySqlFormat3Cell]]],
      draft: Option[MySqlModeOriginalWitnessDraft], nestedStage: MySqlModeWitnessProducerStage,
      originalRootOverride: Option[MySqlFormat3ReadBudget] = None)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Image =
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(snapshot.relations.size.toLong, 128L)),
      "mode bounded replacement map before allocation")
    val replacements = requested
    if replacements == null || replacements.keys.exists(table =>
      !snapshot.shapes.exists(_.table == table)) then
      throw new SQLDataException("mode workflow replacement names a relation outside its complete locked image")
    snapshot match
      case deployment: MySqlScriptSchemaSnapshot =>
        val original=draft.map(_.originalRoot).orElse(originalRootOverride).getOrElse(deployment.allocationBudget)
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,original)
        val historicalPlanChild = draft.isEmpty && originalRootOverride.nonEmpty &&
          budget.isCanonicalChildOf(original) && !budget.isCanonicalComparisonChildOf(original)
        if (budget ne original) && !(draft.isEmpty && budget.isCanonicalComparisonChildOf(original)) && !historicalPlanChild then
          throw new SQLDataException("Full26 mode snapshot differs from original root/comparison owner")
      case legacy: MySqlFormat3FullSnapshot =>
        if (legacy.allocationBudget ne budget) &&
          !(draft.isEmpty && budget.isCanonicalComparisonChildOf(legacy.allocationBudget)) then
          throw new SQLDataException("mode snapshot differs from original root/comparison owner")
    reserveVector(snapshot.relations.size, "mode full20 relation reference vector")
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(replacements.size.toLong, 128L)), "mode snapshot/replaced relation wrappers")
    var validation = 2048L
    snapshot.relations.foreach { relation =>
      replacements.getOrElse(relation.table, relation.rows).foreach { row =>
        validation = Math.addExact(validation, Math.addExact(256L, Math.multiplyExact(row.size.toLong, 96L)))
      }
    }
    withTemporary(validation, "mode snapshot strict validation structures") {
      val relations = snapshot.relations.map { relation =>
        replacements.get(relation.table) match
          case Some(rows) => MySqlFormat3RelationSnapshot(relation.table, rows)
          case None => relation
      }
      val next: Image = snapshot match
        case legacy: MySqlFormat3FullSnapshot =>
          new MySqlFormat3FullSnapshot(legacy.schema, legacy.connection, legacy.observationScope,
            legacy.maxAllowedPacketBytes, legacy.limitProfile, relations, legacy.allocationBudget)
        case deployment: MySqlScriptSchemaSnapshot =>
          val snapshotBudget =
            if draft.nonEmpty then draft.get.originalRoot
            else if originalRootOverride.nonEmpty && budget.isCanonicalChildOf(originalRootOverride.get) &&
              !budget.isCanonicalComparisonChildOf(originalRootOverride.get) then budget
            else deployment.allocationBudget
          MySqlScriptSchemaSnapshot.decoded(deployment.schema, deployment.connection, deployment.observationScope,
            deployment.maxAllowedPacketBytes, deployment.inputs, relations, snapshotBudget)
      // next已charged且建成；必须在digest可能失败之前强持，不能等helper返回。
      retainSnapshot(draft, nestedStage, next)
      next match
        case legacy: MySqlFormat3FullSnapshot =>
          if budget eq snapshot.allocationBudget then legacy.canonicalSha256
          else MySqlFormat3SnapshotCanonical.fingerprint(legacy, budget)
        case deployment: MySqlScriptSchemaSnapshot =>
          if budget eq deployment.allocationBudget then deployment.canonicalSha256
          else MySqlScriptSchemaSnapshotCanonical.fingerprint(deployment,budget)
      next
    }

  private def orderedRows(table: String,
      rows: Vector[Vector[MySqlFormat3Cell]])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[Vector[MySqlFormat3Cell]] =
    if rows == null || rows.exists(_ == null) then throw new SQLDataException("complete immutable rows are required for " + table)
    val shape = MySqlFormat3RelationShapes.forTable(table)
    reserveVector(rows.size, "mode sorted row references")
    withTemporary(Math.addExact(256L, Math.multiplyExact(rows.size.toLong, 64L)), "mode row sort workspace") {
      rows.sortWith { (left, right) =>
        var index = 0
        var compared = 0
        while index < shape.primaryKey.size && compared == 0 do
          val name = shape.primaryKey(index)
          compared = compareCell(cell(left, table, name), cell(right, table, name))
          index += 1
        compared < 0
      }
    }

  private def compareKeys(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Int =
    if left.size != right.size then throw new SQLDataException("locked primary-key widths differ")
    var index = 0
    while index < left.size do
      val compared = compareCell(left(index), right(index))
      if compared != 0 then return compared
      index += 1
    0

  private def compareCell(left: MySqlFormat3Cell, right: MySqlFormat3Cell)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Int = (left, right) match
    case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
    case (MySqlFormat3Cell.Floating(a), MySqlFormat3Cell.Floating(b)) => java.lang.Long.compareUnsigned(a, b)
    case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
    case _ => throw new SQLDataException("locked primary-key typed cells do not have matching SQL kinds")

  private def replaceByUuid(rows: Vector[Vector[MySqlFormat3Cell]],
      replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[Vector[MySqlFormat3Cell]] =
    val binding = MySqlModeOperationEffectCodec.value(replacement, OperationTable, "binding_uuid")
    val operation = MySqlModeOperationEffectCodec.value(replacement, OperationTable, "operation_uuid")
    val matches = rows.indices.filter(index =>
      MySqlModeOperationEffectCodec.value(rows(index), OperationTable, "binding_uuid") == binding &&
        MySqlModeOperationEffectCodec.value(rows(index), OperationTable, "operation_uuid") == operation)
    if matches.size != 1 then throw new SQLDataException("full20 operation replacement needs one exact composite O primary key")
    reserveVector(rows.size, "mode replaced O row references")
    rows.updated(matches.head, replacement)

  private def defaultRow(shape: MySqlFormat3RelationShape)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    allocationDraft.foreach(_.requirePartialRowSlot())
    reserveVector(shape.columns.size, "mode default typed row vector")
    budget.reserveRetainedMemory(Math.multiplyExact(shape.columns.size.toLong, 64L), "mode default typed cell wrappers")
    val result = shape.columns.map { column =>
      allocationDraft.foreach(_.requirePartialCellSlot())
      val produced = if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
        case MySqlFormat3SqlKind.Binary =>
          val length =
            if column.name.endsWith("_uuid") || column.name == "uuid" then 16
            else if column.name.endsWith("_sha256") || column.name.endsWith("_fingerprint") then 32
            else 1
          budget.reserveRetainedMemory(length.toLong + 64L, "mode default actual binary array")
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](length)(0.toByte)))
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
        case MySqlFormat3SqlKind.Floating =>
          MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 0, 0, 0, 0))
      allocationDraft.foreach(_.retainPartialCell(produced))
      produced
    }
    allocationDraft.foreach(_.retainPartialRow(result))
    result

  private def putPair(row: Vector[MySqlFormat3Cell], bytesColumn: String, shaColumn: String,
      canonical: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    if canonical == null then throw new SQLDataException("fixed canonical byte pair is required")
    budget.reserveRetainedMemory(160L, "mode canonical binary wrapper and actual SHA array before allocation")
    val withBytes = put(row, OperationTable, bytesColumn, canonical.binaryCell)
    put(withBytes, OperationTable, shaColumn, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(canonical.sha256.toArray, budget)))

  private def putPair(row: Vector[MySqlFormat3Cell], bytesColumn: String, shaColumn: String,
      table: String, canonical: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    if canonical == null then throw new SQLDataException("fixed canonical byte pair is required")
    budget.reserveRetainedMemory(160L, "mode canonical binary wrapper and actual SHA array before allocation")
    val withBytes = put(row, table, bytesColumn, canonical.binaryCell)
    put(withBytes, table, shaColumn, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(canonical.sha256.toArray, budget)))

  private def newDigestCell(value: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget,
      allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    allocationDraft.foreach(_.requirePartialCellSlot())
    budget.reserveRetainedMemory(160L, "mode new actual canonical SHA cell/array before allocation")
    val result = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value.sha256.toArray, budget))
    allocationDraft.foreach(_.retainPartialCell(result))
    result

  private def newTypedCell(value: => MySqlFormat3Cell)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    allocationDraft.foreach(_.requirePartialCellSlot())
    budget.reserveRetainedMemory(64L, "mode actual new typed scalar wrapper before allocation")
    val result = value
    allocationDraft.foreach(_.retainPartialCell(result))
    result

  private def put(row: Vector[MySqlFormat3Cell], table: String, name: String,
      value: MySqlFormat3Cell)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlFormat3Cell] =
    if row == null || value == null then throw new SQLDataException("typed row field cannot be null")
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 then throw new SQLDataException("fixed format3 row has no " + table + "." + name)
    // argument可能已由真正scalar/UUID helper产生；下一reserve前必须强持。
    allocationDraft.foreach(_.retainPartialCell(value))
    allocationDraft.foreach(_.requirePartialRowSlot())
    reserveVector(row.size, "mode updated immutable row reference vector")
    val result = row.updated(index, value)
    allocationDraft.foreach(_.retainPartialRow(result))
    result

  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if row == null || index < 0 || index >= row.size then
      throw new SQLDataException("fixed format3 row has no " + table + "." + name)
    row(index)

  private def signed(row: Vector[MySqlFormat3Cell], table: String, name: String)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Long =
    cell(row, table, name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException("typed BIGINT required for " + table + "." + name)

  private def uuidCell(uuid: UUID)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    if uuid == null then throw new SQLDataException("exact UUID is required")
    allocationDraft.foreach(_.requirePartialCellSlot())
    budget.reserveRetainedMemory(192L, "mode actual UUID array/buffer/cell before allocation")
    val bytes = ByteBuffer.allocate(16)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .array()
    val result = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))
    allocationDraft.foreach(_.retainProducedUuidCell(result))
    result

  private def vectorCell(bytes: Vector[Byte])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    if bytes == null then throw new SQLDataException("exact binary digest is required")
    allocationDraft.foreach(_.requirePartialCellSlot())
    budget.reserveRetainedMemory(bytes.size.toLong + 128L, "mode actual digest array/cell before allocation")
    val result = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray, budget))
    allocationDraft.foreach(_.retainPartialCell(result))
    result

  private def zeroDigestCell()(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    allocationDraft.foreach(_.requirePartialCellSlot())
    budget.reserveRetainedMemory(160L, "mode actual zero digest array/cell before allocation")
    val result = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(0.toByte), budget))
    allocationDraft.foreach(_.retainPartialCell(result))
    result

  private def doubleCell(value: Double)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): MySqlFormat3Cell =
    if !java.lang.Double.isFinite(value) then throw new SQLDataException("mode pose must contain finite coordinates")
    newTypedCell(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value)))

  private def writeCanonical(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    if output == null || value == null then throw new SQLDataException("semantic witness canonical field is missing")
    MySqlModeEnvelopeV2Census.int(output, value.length)
    MySqlModeEnvelopeV2Census.canonical(output, value)

  private def writeBytes(output: DataOutputStream, bytes: Array[Byte])(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    if output == null || bytes == null then throw new SQLDataException("semantic witness byte field is missing")
    MySqlModeEnvelopeV2Census.int(output, bytes.length)
    output.write(bytes)

  private def writeDateTime(output: DataOutputStream, value: LocalDateTime)(using budget: MySqlFormat3ReadBudget, allocationDraft: Option[MySqlModeOriginalWitnessDraft]): Unit =
    if output == null || value == null || value.getNano % 1000 != 0 then
      throw new SQLDataException("semantic witness DATETIME(6) value is invalid")
    MySqlModeEnvelopeV2Census.int(output, value.getYear)
    MySqlModeEnvelopeV2Census.byte(output, value.getMonthValue)
    MySqlModeEnvelopeV2Census.byte(output, value.getDayOfMonth)
    MySqlModeEnvelopeV2Census.byte(output, value.getHour)
    MySqlModeEnvelopeV2Census.byte(output, value.getMinute)
    MySqlModeEnvelopeV2Census.byte(output, value.getSecond)
    MySqlModeEnvelopeV2Census.int(output, value.getNano / 1000)
