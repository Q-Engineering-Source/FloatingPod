package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.PodOperatingMode

import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.sql.SQLDataException
import java.time.{LocalDateTime, ZoneOffset}
import java.util.{Arrays, UUID}
import MySqlModeOperationEffectCodec.{Image, allocationBudget, limitProfile, schema, connection,
  observationScope, maxAllowedPacketBytes, relation, relations, shapes}

/** 两个不可变的计划阶段，保留 full20 图像与操作见证，但不保留 JDBC owner、连接或提交确认。 */
private[mysql] final class MySqlDriftWorkflowMaterialization private[mysql] (
    val plannedAt: LocalDateTime,
    private val initialImage: Image,
    private val reservedImage: Image,
    private val executedImage: Image,
    val effects: Vector[MySqlDriftOperationEffect],
    private val reserveWitness: MySqlRecoveryCanonicalBytes,
    private val executeWitness: MySqlRecoveryCanonicalBytes,
    private val plan: MySqlOwnedRepositoryOperationPlan,
    private val operation: MySqlRepositoryOperation
):
  private def legacy(image:Image):MySqlFormat3FullSnapshot=image match
    case value:MySqlFormat3FullSnapshot=>value
    case _:MySqlScriptSchemaSnapshot=>throw new SQLDataException("Full26 drift cannot enter a legacy Full20 getter")
  def preimage:MySqlFormat3FullSnapshot=legacy(initialImage)
  def reserveTarget:MySqlFormat3FullSnapshot=legacy(reservedImage)
  def executeTarget:MySqlFormat3FullSnapshot=legacy(executedImage)
  private[mysql] def completePreimage:Image=initialImage
  private[mysql] def completeReserveTarget:Image=reservedImage
  private[mysql] def completeExecuteTarget:Image=executedImage
  private[mysql] def isDeploymentReceipt:Boolean=initialImage match
    case _:MySqlScriptSchemaSnapshot=>true
    case _:MySqlFormat3FullSnapshot=>false
  private[mysql] def exactPlan: MySqlOwnedRepositoryOperationPlan = plan
  private[mysql] def exactOperation: MySqlRepositoryOperation = operation

  private[mysql] def requireBoundTo(commandScope: MySqlRepositoryCommandScope,
      currentOperation: MySqlRepositoryOperation): Unit =
    if commandScope == null || currentOperation == null || (commandScope.operationPlan() ne plan) ||
      currentOperation != operation then
      throw new SQLDataException("tracked drift workflow materialization differs from the exact Main command plan")
    commandScope.requireCurrent(commandScope.exactAuditScope(), plan)
    plan.requireSingle(currentOperation, plan.exactLimitProfile)
    verify()

  def verify(): Unit =
    if plannedAt == null || plannedAt.getNano % 1000 != 0 || effects == null ||
      effects.map(_.phase) != Vector(MySqlOperationEffectPhase.Reserve, MySqlOperationEffectPhase.Execute) then
      throw new SQLDataException("tracked drift workflow materialization is incomplete or not DATETIME(6)")
    effects(0).verifyPreimage(initialImage)
    effects(0).verifyReadback(reservedImage)
    effects(1).verifyPreimage(reservedImage)
    effects(1).verifyReadback(executedImage)
    requireWitness(reservedImage, plan.reserveOperationUuid, reserveWitness)
    requireWitness(executedImage, plan.executeOperationUuid, executeWitness)
    requireReservedExecuteNulls(reservedImage, plan.executeOperationUuid)
    requireTerminalTimes(reservedImage, executedImage, plannedAt, plan)

  private def requireWitness(snapshot: Image, operationUuid: UUID,
      expected: MySqlRecoveryCanonicalBytes): Unit =
    val row = findOperation(snapshot, operationUuid)
    val digestMatches = MySqlDriftOperationEffectCodec.value(
      row, "fp_recovery_operation", "commit_witness_sha256") match
      case MySqlFormat3Cell.Binary(actual) => actual.equalsVector(expected.sha256)
      case _ => false
    if !expected.exactlyMatchesCell(MySqlDriftOperationEffectCodec.value(
      row, "fp_recovery_operation", "commit_witness_bytes")) || !digestMatches then
      throw new SQLDataException("semantic operation witness differs from its fixed full20 phase inputs")

  private def requireReservedExecuteNulls(snapshot: Image, operationUuid: UUID): Unit =
    val row = findOperation(snapshot, operationUuid)
    if MySqlDriftOperationEffectCodec.value(row, "fp_recovery_operation", "operation_state") !=
      MySqlFormat3Cell.Text("RESERVED") ||
      MySqlDriftOperationEffectCodec.value(row, "fp_recovery_operation", "result_at") != MySqlFormat3Cell.SqlNull ||
      MySqlDriftOperationEffectCodec.value(row, "fp_recovery_operation", "settled_by_operation_uuid") != MySqlFormat3Cell.SqlNull ||
      MySqlDriftOperationEffectCodec.value(row, "fp_recovery_operation", "commit_witness_bytes") != MySqlFormat3Cell.SqlNull ||
      MySqlDriftOperationEffectCodec.value(row, "fp_recovery_operation", "commit_witness_sha256") != MySqlFormat3Cell.SqlNull then
      throw new SQLDataException("reserve target must preserve the not-yet-executed operation as SQL NULL")

  private def requireTerminalTimes(reserve: Image, execute: Image,
      fixed: LocalDateTime, fixedPlan: MySqlOwnedRepositoryOperationPlan): Unit =
    val reserveRow = findOperation(reserve, fixedPlan.reserveOperationUuid)
    val reservedExecute = findOperation(reserve, fixedPlan.executeOperationUuid)
    val committedExecute = findOperation(execute, fixedPlan.executeOperationUuid)
    val expected = MySqlFormat3Cell.DateTime(fixed)
    if MySqlDriftOperationEffectCodec.value(reserveRow, "fp_recovery_operation", "planned_at") != expected ||
      MySqlDriftOperationEffectCodec.value(reserveRow, "fp_recovery_operation", "result_at") != expected ||
      MySqlDriftOperationEffectCodec.value(reservedExecute, "fp_recovery_operation", "planned_at") != expected ||
      MySqlDriftOperationEffectCodec.value(committedExecute, "fp_recovery_operation", "planned_at") != expected ||
      MySqlDriftOperationEffectCodec.value(committedExecute, "fp_recovery_operation", "result_at") != expected then
      throw new SQLDataException("mode operation timestamps must retain the single fixed database plannedAt marker")

  private def findOperation(snapshot: Image, operationUuid: UUID): Vector[MySqlFormat3Cell] =
    MySqlDriftOperationEffectCodec.operationRows(snapshot, operation)
      .find(row => MySqlDriftOperationEffectCodec.rowOperationUuid(row) == operationUuid)
      .getOrElse(throw new SQLDataException("fixed mode operation row is missing from its full20 phase"))

/** 纯物化逻辑。运行时调用者只能从活动的 PlanningReadOwner 取得输入；此函数本身不签发时钟权威。 */
private[mysql] object MySqlDriftWorkflowMaterializer:
  private val Domain = "com.wi.floatingpod.mysql-tracked-drift-semantic-witness/v1"
  private val Encoding = "FP_MYSQL_TRACKED_DRIFT_SEMANTIC_WITNESS_BE_V1"
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
      locatorRevision: Long): MySqlDriftWorkflowMaterialization =
    if before == null then throw new SQLDataException("original full20 root required before drift allocation")
    val budget = before.allocationBudget
    // 固定入口/WorkflowRows/phase元数据；不预收尚不存在的64份历史图。
    given MySqlFormat3ReadBudget = budget
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      budget.reserveRetainedMemory(512L, "drift materializer structures before allocation")
      materializeReserved(plan, operation, context, plannedAt, before, locatorRevision)
    }

  private[mysql] def materializeDeploymentReceipt(plan:MySqlOwnedRepositoryOperationPlan,
      operation:MySqlRepositoryOperation,context:MySqlRepositoryOperationContextIdentity,
      plannedAt:LocalDateTime,before:MySqlScriptSchemaSnapshot,locatorRevision:Long,
      root:MySqlFormat3ReadBudget):MySqlDriftWorkflowMaterialization =
    require(plan!=null && before!=null && root!=null,"actual Full26 drift plan/source/root required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    if context==null || context.schemaFormat!=5 || context.physicalFormat!=2 ||
      (MySqlModeOperationEffectCodec.originalImageRoot(before,plan) ne root) then
      throw new SQLDataException("Full26 drift lost original retained plan/context/root")
    given MySqlFormat3ReadBudget=root
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(512L,"Full26 drift materializer structures before allocation")
      materializeReserved(plan,operation,context,plannedAt,before,locatorRevision)
    }

  /** H3历史专用入口：原Full26 root保留authority，计划新DATA由真实holder提供的child计费。 */
  private[mysql] def materializeDeploymentReceipt(plan:MySqlOwnedRepositoryOperationPlan,
      operation:MySqlRepositoryOperation,context:MySqlRepositoryOperationContextIdentity,
      plannedAt:LocalDateTime,before:MySqlScriptSchemaSnapshot,locatorRevision:Long,
      authorityRoot:MySqlFormat3ReadBudget,planDataBudget:MySqlFormat3ReadBudget):MySqlDriftWorkflowMaterialization =
    requirePlanDataScope(plan, before, authorityRoot, planDataBudget)
    if context==null || context.schemaFormat!=5 || context.physicalFormat!=2 ||
      (context.exactLimitProfile ne authorityRoot.profile) then
      throw new SQLDataException("Full26 drift lost original retained plan/context/root")
    given MySqlFormat3ReadBudget=planDataBudget
    planDataBudget.reserveRetainedMemory(512L,"Full26 drift materializer structures before allocation")
    materializeReserved(plan,operation,context,plannedAt,before,locatorRevision,Some(authorityRoot))

  private def requirePlanDataScope(plan:MySqlOwnedRepositoryOperationPlan,
      before:MySqlScriptSchemaSnapshot,authorityRoot:MySqlFormat3ReadBudget,
      planDataBudget:MySqlFormat3ReadBudget):Unit =
    if plan==null || before==null || authorityRoot==null || authorityRoot.isClosed || authorityRoot.isScopedReadBudget ||
      planDataBudget==null || planDataBudget.isClosed || (planDataBudget.profile ne authorityRoot.profile) ||
      !planDataBudget.isCanonicalChildOf(authorityRoot) || !planDataBudget.sharesAllocationLedger(authorityRoot) ||
      planDataBudget.isCanonicalComparisonChildOf(authorityRoot) || (plan.exactLimitProfile ne authorityRoot.profile) ||
      ((before.allocationBudget ne authorityRoot) && (before.allocationBudget ne planDataBudget)) then
      throw new SQLDataException("H3 Full26 drift requires the explicit original root and its exact same-ledger plan DATA child")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,authorityRoot)

  /** 每次实际Vector重建先计256头/节点及每元素64的保守结构上界。
    * 共享旧行/cell/payload不复制；实际新array单独收费，不预造多份完整图。
    */
  private def reserveVector(size: Int, context: String)(using budget: MySqlFormat3ReadBudget): Unit =
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(size.toLong, 64L)), context)

  private def withTemporary[A](amount: Long, context: String)(body: => A)(using budget: MySqlFormat3ReadBudget): A =
    budget.reserveTemporaryMemory(amount, context)
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try budget.releaseTemporaryMemory(amount, context)
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  private def appendOperationRows(rows: Vector[Vector[MySqlFormat3Cell]],
      first: Vector[MySqlFormat3Cell], second: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    reserveVector(Math.addExact(rows.size, 2), "drift appended O history reference vector")
    budget.reserveRetainedMemory(384L, "drift two appended row reference vector")
    rows ++ Vector(first, second)

  private def zeroFingerprint()(using budget: MySqlFormat3ReadBudget): Vector[Byte] =
    reserveVector(32, "drift placeholder fingerprint vector")
    Vector.fill(32)(0.toByte)

  private def materializeReserved(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image,
      locatorRevision: Long, authorityRoot:Option[MySqlFormat3ReadBudget]=None)(using budget: MySqlFormat3ReadBudget): MySqlDriftWorkflowMaterialization =
    validateInputs(plan, operation, context, plannedAt, before, locatorRevision,authorityRoot)
    val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
    val profile = before.limitProfile
    val scope = MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      MySqlRecoveryOperationScope.encode(context, Vector(MySqlRecoveryResourceRevision(
        MySqlRecoveryResourceKey.pod(request.podUuid, profile), locatorRevision, Some(request.attemptUuid))))
    }
    val footprintUuid = currentFootprintUuid(before, request)
    val seed = seedCanonical(profile)
    val rows = initialRows(plan, request, context, plannedAt, before, scope, seed, footprintUuid, locatorRevision)
    buildPhases(plan, operation, context, plannedAt, before, locatorRevision, scope, rows, seed,
      authorityRoot.getOrElse(before.allocationBudget))

  private def buildPhases(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, locatorRevision: Long,
      scope: MySqlRecoveryCanonicalBytes, rows: WorkflowRows,
      seed: MySqlRecoveryCanonicalBytes,authorityRoot:MySqlFormat3ReadBudget)(using budget: MySqlFormat3ReadBudget): MySqlDriftWorkflowMaterialization =
    val reservePreimage = if budget eq authorityRoot then
      MySqlDriftOperationEffectCodec.projectImage(before, plan, operation, context, locatorRevision,
        MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before)
    else MySqlDriftOperationEffectCodec.projectImage(before, plan, operation, context, locatorRevision,
      MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before,authorityRoot,budget)
    budget.reserveRetainedMemory(128L, "drift phase row holder")
    val reserveRows = rows.copy(reserveRow = putPair(rows.reserveRow, "preimage_bytes", "preimage_sha256", reservePreimage))
    val reserveScaffold = reserveScaffoldSnapshot(before, plan, reserveRows,authorityRoot)
    val reservePhase = if budget eq authorityRoot then
      MySqlDriftOperationEffectCodec.canonicalPhase(plan, operation, context, locatorRevision,
        MySqlOperationEffectPhase.Reserve, before, reserveScaffold)
    else MySqlDriftOperationEffectCodec.canonicalPhase(plan, operation, context, locatorRevision,
      MySqlOperationEffectPhase.Reserve, before, reserveScaffold,authorityRoot,budget)
    val reserveWitness = semanticWitness(plan, operation, context, plannedAt,
      MySqlOperationEffectPhase.Reserve, reservePhase, before.limitProfile, None)
    val reserveCommitted = withPhaseValues(reserveRows.reserveRow, plan, scope, reservePhase)
    val reserveFinal = putPair(reserveCommitted, "commit_witness_bytes", "commit_witness_sha256", reserveWitness)

    val reserveWriter = withWriterReceipt(reserveScaffold, plan.reserveOperationUuid, reservePhase.fingerprint.sha256,authorityRoot)
    val reserveTargetBase = withRelations(reserveWriter, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(reserveWriter.relation(OperationTable).rows, reserveFinal))
    ),authorityRoot)
    val executeScaffold = executeScaffoldSnapshot(reserveTargetBase, plan, operation, context, plannedAt, scope,
      rows, locatorRevision, seed,authorityRoot)
    val executePhase = if budget eq authorityRoot then
      MySqlDriftOperationEffectCodec.canonicalPhase(plan, operation, context, locatorRevision,
        MySqlOperationEffectPhase.Execute, reserveTargetBase, executeScaffold)
    else MySqlDriftOperationEffectCodec.canonicalPhase(plan, operation, context, locatorRevision,
      MySqlOperationEffectPhase.Execute, reserveTargetBase, executeScaffold,authorityRoot,budget)
    val executeReserved = withPhaseValues(rows.reservedExecuteRow, plan, scope, executePhase)
    val reserveTarget = withRelations(reserveTargetBase, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(reserveTargetBase.relation(OperationTable).rows, executeReserved))
    ),authorityRoot)

    val reserveEnvelope = MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      MySqlRecoveryOperationRawEnvelopeCodec.encode(reserveFinal, before.limitProfile, inputsForImage(before, authorityRoot))
    }
    val executeWitness = semanticWitness(plan, operation, context, plannedAt,
      MySqlOperationEffectPhase.Execute, executePhase, before.limitProfile, Some(reserveEnvelope))
    val executeCommittedDraft = operationRow(plan, MySqlDriftOperationEffectCodec.driftRequest(operation),
      context, plannedAt, scope, seed, rows.footprintUuid, plan.executeOperationUuid,
      "DOMAIN_MUTATION", "COMMITTED", Some(plan.reserveOperationUuid), withWitness = true,
      locatorRevision = locatorRevision)
    val executeCommitted = withPhaseValues(executeCommittedDraft, plan, scope, executePhase)
    val executeTerminal = putPair(executeCommitted, "commit_witness_bytes", "commit_witness_sha256", executeWitness)
    val executeWriter = withWriterReceipt(executeScaffold, plan.executeOperationUuid, executePhase.fingerprint.sha256,authorityRoot)
    val executeTarget = withRelations(executeWriter, Map(
      OperationTable -> orderedRows(OperationTable,
        replaceByUuid(executeWriter.relation(OperationTable).rows, executeTerminal))
    ),authorityRoot)
    budget.reserveRetainedMemory(1024L, "drift two effect holders and result metadata")
    val effects = if budget eq authorityRoot then
      MySqlDriftOperationEffectCodec.buildMaterializedWorkflow(plan, operation, context, locatorRevision,
        before, reserveTarget, executeTarget, reservePhase, executePhase, reserveEnvelope)
    else MySqlDriftOperationEffectCodec.buildMaterializedWorkflow(plan, operation, context, locatorRevision,
      before, reserveTarget, executeTarget, reservePhase, executePhase, reserveEnvelope,authorityRoot,budget)
    val result = new MySqlDriftWorkflowMaterialization(plannedAt, before, reserveTarget, executeTarget,
      effects, reserveWitness, executeWitness, plan, operation)
    result.verify()
    result

  private def validateInputs(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, locatorRevision: Long,
      authorityRoot:Option[MySqlFormat3ReadBudget]=None)(using budget: MySqlFormat3ReadBudget): Unit =
    if plan == null || operation == null || context == null || plannedAt == null || before == null ||
      locatorRevision < 0L || plannedAt.getNano % 1000 != 0 ||
      plannedAt.getYear < 1000 || plannedAt.getYear > 9999 then
      throw new SQLDataException("fixed mode planning inputs require a complete plan, full20 image and DATETIME(6)")
    if context.exactLimitProfile ne before.limitProfile then
      throw new SQLDataException("fixed mode planning profile differs from the full20 image")
    MySqlDriftOperationEffectCodec.driftRequest(operation)
    authorityRoot match
      case Some(root) => MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        plan.requireSingle(operation, before.limitProfile)
      }
      case None => plan.requireSingle(operation, before.limitProfile)

  private def seedCanonical(profile: SourceLimitProfile)(using budget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      MySqlRecoveryCanonicalBytes.encode("FP_TRACKED_DRIFT_PLANNING_SEED_BE_V1", profile) { output =>
        MySqlRecoveryCanonicalBytes.writeToken(output, "FP_TRACKED_DRIFT_PLANNING_SEED_V1")
        output.writeInt(0)
      }
    }

  private def initialRows(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlDriftOperationEffectCodec.Request, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, before: Image, scope: MySqlRecoveryCanonicalBytes,
      seed: MySqlRecoveryCanonicalBytes, footprintUuid: UUID, locatorRevision: Long)(using budget: MySqlFormat3ReadBudget): WorkflowRows =
    val reserve = operationRow(plan, request, context, plannedAt, scope, seed, footprintUuid,
      plan.reserveOperationUuid, "RESERVE", "COMMITTED", None, withWitness = true,
      locatorRevision = locatorRevision)
    val execute = operationRow(plan, request, context, plannedAt, scope, seed, footprintUuid,
      plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED", Some(plan.reserveOperationUuid),
      withWitness = false, locatorRevision = locatorRevision)
    budget.reserveRetainedMemory(128L, "drift initial row holder")
    WorkflowRows(reserve, execute, reservationRow(before, plan, request), footprintUuid)

  private def operationRow(plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlDriftOperationEffectCodec.Request, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, scope: MySqlRecoveryCanonicalBytes, seed: MySqlRecoveryCanonicalBytes,
      footprintUuid: UUID, operationUuid: UUID, step: String, state: String,
      parent: Option[UUID], withWitness: Boolean, locatorRevision: Long)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(OperationTable))
    row = put(row, OperationTable, "binding_uuid", uuidCell(request.binding.bindingUuid))
    row = put(row, OperationTable, "operation_uuid", uuidCell(operationUuid))
    row = put(row, OperationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
    row = put(row, OperationTable, "step", MySqlFormat3Cell.Text(step))
    row = put(row, OperationTable, "operation_state", MySqlFormat3Cell.Text(state))
    row = put(row, OperationTable, "pod_uuid", uuidCell(request.podUuid))
    row = put(row, OperationTable, "attempt_uuid", uuidCell(request.attemptUuid))
    row = put(row, OperationTable, "footprint_uuid", uuidCell(footprintUuid))
    row = put(row, OperationTable, "source_unit_uuid", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "forest_uuid", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "boot_uuid", uuidCell(context.tag.bootId))
    row = put(row, OperationTable, "boot_generation", MySqlFormat3Cell.SignedInteger(context.tag.bootGeneration))
    row = put(row, OperationTable, "connection_generation_uuid", uuidCell(context.barrierGenerationUuid))
    row = put(row, OperationTable, "connection_id", MySqlFormat3Cell.SignedInteger(context.actualConnectionId))
    row = put(row, OperationTable, "fence_epoch", MySqlFormat3Cell.SignedInteger(context.writerEpoch))
    row = putPair(row, "request_identity_bytes", "request_identity_sha256", plan.requestIdentity.canonical)
    row = put(row, OperationTable, "expected_domain_version", MySqlFormat3Cell.SignedInteger(request.expectedStateVersion))
    row = put(row, OperationTable, "expected_locator_revision", MySqlFormat3Cell.SignedInteger(locatorRevision))
    row = put(row, OperationTable, "expected_source_revision", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "context_encoding", MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"))
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
    row = put(row, OperationTable, "planned_at", MySqlFormat3Cell.DateTime(plannedAt))
    row = put(row, OperationTable, "fixed_lease_until", MySqlFormat3Cell.DateTime(context.fixedLeaseUntil))
    row = put(row, OperationTable, "result_at",
      if state == "COMMITTED" then MySqlFormat3Cell.DateTime(plannedAt) else MySqlFormat3Cell.SqlNull)
    put(row, OperationTable, "operation_fingerprint", zeroDigestCell())

  private def withPhaseValues(row: Vector[MySqlFormat3Cell], plan: MySqlOwnedRepositoryOperationPlan,
      scope: MySqlRecoveryCanonicalBytes, phase: MySqlDriftPhaseCanonical)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    var next = putPair(row, "request_identity_bytes", "request_identity_sha256", plan.requestIdentity.canonical)
    next = putPair(next, "scope_bytes", "scope_sha256", scope)
    next = putPair(next, "original_context_bytes", "original_context_sha256", phase.operationContextBytes)
    next = putPair(next, "preimage_bytes", "preimage_sha256", phase.preimage)
    next = putPair(next, "target_bytes", "target_sha256", phase.target)
    put(next, OperationTable, "operation_fingerprint", vectorCell(phase.fingerprint.sha256))

  private def reserveScaffoldSnapshot(before: Image,
      plan: MySqlOwnedRepositoryOperationPlan, rows: WorkflowRows,
      authorityRoot:MySqlFormat3ReadBudget)(using budget: MySqlFormat3ReadBudget): Image =
    val operationRows = before.relation(OperationTable).rows
    budget.reserveRetainedMemory(384L, "drift fixed two operation UUID set")
    val newIds = Set(plan.reserveOperationUuid, plan.executeOperationUuid)
    if operationRows.exists(row =>
      newIds.contains(MySqlDriftOperationEffectCodec.rowOperationUuid(row)) &&
        MySqlDriftOperationEffectCodec.value(row, OperationTable, "binding_uuid") ==
          MySqlDriftOperationEffectCodec.value(rows.reserveRow, OperationTable, "binding_uuid")) then
      throw new java.sql.SQLTransactionRollbackException("fixed mode operation UUID already has an O history row")
    val reservePlaceholder = withWriterReceipt(before, plan.reserveOperationUuid, zeroFingerprint(),authorityRoot)
    val allOperationRows = orderedRows(OperationTable, appendOperationRows(operationRows, rows.reserveRow, rows.reservedExecuteRow))
    withRelations(reservePlaceholder, Map(
      OperationTable -> allOperationRows,
      ReservationTable -> reservationRowsFor(before, rows.reservationRow)
    ),authorityRoot)

  private def executeScaffoldSnapshot(reserveBefore: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime,
      scope: MySqlRecoveryCanonicalBytes, rows: WorkflowRows, locatorRevision: Long,
      seed: MySqlRecoveryCanonicalBytes,authorityRoot:MySqlFormat3ReadBudget)(using budget: MySqlFormat3ReadBudget): Image =
    val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
    val domain = exactPodRow(reserveBefore, DomainTable, request.binding.bindingUuid, request.podUuid)
    val locator = exactPodRow(reserveBefore, LocatorTable, request.binding.bindingUuid, request.podUuid)
    val released = releasedReservation(rows.reservationRow, plan.executeOperationUuid)
    val committedExecute = operationRow(plan, request, context, plannedAt, scope, seed, rows.footprintUuid,
      plan.executeOperationUuid, "DOMAIN_MUTATION", "COMMITTED", Some(plan.reserveOperationUuid),
      withWitness = true, locatorRevision = locatorRevision)
    val operationRows = replaceByUuid(reserveBefore.relation(OperationTable).rows, committedExecute)
    val writer = withWriterReceipt(reserveBefore, plan.executeOperationUuid, zeroFingerprint(),authorityRoot)
    withRelations(writer, Map(
      DomainTable -> orderedRows(DomainTable, replaceExactPod(reserveBefore, DomainTable, domain,
        changedDomain(domain, request, plannedAt, plan.executeOperationUuid))),
      LocatorTable -> orderedRows(LocatorTable, replaceExactPod(reserveBefore, LocatorTable, locator,
        changedLocator(locator, request, plan.executeOperationUuid))),
      OperationTable -> orderedRows(OperationTable, operationRows),
      ReservationTable -> reservationRowsFor(reserveBefore, released)
    ),authorityRoot)

  private def reservationRow(before: Image,
      plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlDriftOperationEffectCodec.Request)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    withTemporary(Math.addExact(512L, Math.multiplyExact(before.relation(ReservationTable).rows.size.toLong, 64L)), "drift reservation lookup filter references") {
      val existing = before.relation(ReservationTable).rows.filter(row =>
        MySqlDriftOperationEffectCodec.uuidEquals(
          MySqlDriftOperationEffectCodec.value(row, ReservationTable, "binding_uuid"), request.binding.bindingUuid) &&
          MySqlDriftOperationEffectCodec.value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("POD") &&
          MySqlDriftOperationEffectCodec.uuidEquals(
            MySqlDriftOperationEffectCodec.value(row, ReservationTable, "resource_pod_uuid"), request.podUuid))
      if existing.size > 1 then throw new SQLDataException("mode plan found duplicate historical POD reservation rows")
      existing.headOption match
        case Some(old) =>
          if MySqlDriftOperationEffectCodec.value(old, ReservationTable, "reservation_state") != MySqlFormat3Cell.Text("RELEASED") then
            throw new java.sql.SQLTransactionRollbackException("held POD reservation retains its prior workflow responsibility")
          var row = put(old, ReservationTable, "reservation_revision",
            MySqlFormat3Cell.SignedInteger(Math.addExact(signed(old, ReservationTable, "reservation_revision"), 1L)))
          row = put(row, ReservationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
          row = put(row, ReservationTable, "operation_uuid", uuidCell(plan.executeOperationUuid))
          row = put(row, ReservationTable, "reservation_state", MySqlFormat3Cell.Text("HELD"))
          put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(plan.reserveOperationUuid))
        case None =>
          val resource = MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
            MySqlRecoveryResourceKey.pod(request.podUuid, before.limitProfile)
          }
          val canonicalKey = MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
            MySqlRecoveryCanonicalBytes.encode(
              "FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", before.limitProfile)(resource.writeCanonicalTo)
          }
          if canonicalKey.sha256 != resource.keyHashSha256 then
            throw new SQLDataException("POD reservation canonical key digest differs from its fixed resource identity")
          var row = defaultRow(MySqlFormat3RelationShapes.forTable(ReservationTable))
          row = put(row, ReservationTable, "binding_uuid", uuidCell(request.binding.bindingUuid))
          row = put(row, ReservationTable, "resource_kind", MySqlFormat3Cell.Text("POD"))
          row = put(row, ReservationTable, "resource_key_hash", canonicalKey.sha256Cell)
          row = put(row, ReservationTable, "resource_key", canonicalKey.binaryCell)
          row = put(row, ReservationTable, "reservation_revision", MySqlFormat3Cell.SignedInteger(0L))
          row = put(row, ReservationTable, "workflow_uuid", uuidCell(plan.workflowUuid))
          row = put(row, ReservationTable, "operation_uuid", uuidCell(plan.executeOperationUuid))
          row = put(row, ReservationTable, "reservation_state", MySqlFormat3Cell.Text("HELD"))
          row = put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(plan.reserveOperationUuid))
          row = put(row, ReservationTable, "resource_pod_uuid", uuidCell(request.podUuid))
          put(row, ReservationTable, "resource_unit_uuid", MySqlFormat3Cell.SqlNull)
    }

  private def releasedReservation(held: Vector[MySqlFormat3Cell], operationUuid: UUID)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val revision = Math.addExact(signed(held, ReservationTable, "reservation_revision"), 1L)
    var row = put(held, ReservationTable, "reservation_revision", MySqlFormat3Cell.SignedInteger(revision))
    row = put(row, ReservationTable, "reservation_state", MySqlFormat3Cell.Text("RELEASED"))
    put(row, ReservationTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def reservationRowsFor(snapshot: Image,
      replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(ReservationTable)
    val source = snapshot.relation(ReservationTable).rows
    var matched = -1
    var index = 0
    while index < source.size do
      if compareRowKeys(source(index), replacement, ReservationTable, shape) == 0 then
        if matched >= 0 then throw new SQLDataException("duplicate exact reservation primary key in full20 image")
        matched = index
      index += 1
    reserveVector(if matched >= 0 then source.size else Math.addExact(source.size, 1),
      "drift reservation reference vector")
    val result = if matched >= 0 then source.updated(matched, replacement) else source :+ replacement
    orderedRows(ReservationTable, result)

  private def changedDomain(old: Vector[MySqlFormat3Cell],
      request: MySqlDriftOperationEffectCodec.Request, plannedAt: LocalDateTime,
      operationUuid: UUID)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val pose = request.actualPose
    var row = put(old, DomainTable, "state_version",
      MySqlFormat3Cell.SignedInteger(Math.addExact(request.expectedStateVersion, 1L)))
    row = put(row, DomainTable, "world_name", MySqlFormat3Cell.Text(pose.worldName))
    row = put(row, DomainTable, "pos_x", doubleCell(pose.posX))
    row = put(row, DomainTable, "pos_y", doubleCell(pose.posY))
    row = put(row, DomainTable, "pos_z", doubleCell(pose.posZ))
    row = put(row, DomainTable, "yaw", doubleCell(pose.yaw.toDouble))
    row = put(row, DomainTable, "pitch", doubleCell(pose.pitch.toDouble))
    row = put(row, DomainTable, "last_updated",
      MySqlFormat3Cell.SignedInteger(plannedAt.toInstant(ZoneOffset.UTC).toEpochMilli))
    row = put(row, DomainTable, "operating_mode", MySqlFormat3Cell.Text(request.targetMode.databaseValue))
    put(row, DomainTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def changedLocator(old: Vector[MySqlFormat3Cell],
      request: MySqlDriftOperationEffectCodec.Request, operationUuid: UUID)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    var row = put(old, LocatorTable, "domain_version",
      MySqlFormat3Cell.SignedInteger(Math.addExact(request.expectedStateVersion, 1L)))
    row = put(row, LocatorTable, "locator_revision",
      MySqlFormat3Cell.SignedInteger(Math.addExact(signed(old, LocatorTable, "locator_revision"), 1L)))
    put(row, LocatorTable, "last_mutation_operation_uuid", uuidCell(operationUuid))

  private def replaceExactPod(snapshot: Image, table: String,
      old: Vector[MySqlFormat3Cell], replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    val rows = snapshot.relation(table).rows
    var matched = -1
    var index = 0
    while index < rows.size do
      if compareRowKeys(rows(index), old, table, shape) == 0 then
        if matched >= 0 then throw new java.sql.SQLTransactionRollbackException("locked mode row no longer has one exact primary key")
        matched = index
      index += 1
    if matched < 0 then throw new java.sql.SQLTransactionRollbackException("locked mode row no longer has one exact primary key")
    reserveVector(rows.size, "drift exact pod replacement reference vector")
    rows.updated(matched, replacement)

  private def withWriterReceipt(snapshot: Image,
      operationUuid: UUID, fingerprint: Vector[Byte],authorityRoot:MySqlFormat3ReadBudget)(using budget: MySqlFormat3ReadBudget): Image =
    if fingerprint == null || fingerprint.size != 32 then throw new SQLDataException("fixed W fingerprint must contain 32 bytes")
    val writerRows = snapshot.relation(WriterTable).rows
    if writerRows.size != 1 then throw new java.sql.SQLTransactionRollbackException("full20 image has no unique writer row")
    var writer = put(writerRows.head, WriterTable, "last_commit_operation_uuid", uuidCell(operationUuid))
    writer = put(writer, WriterTable, "last_commit_fingerprint", vectorCell(fingerprint))
    withRelations(snapshot, Map(WriterTable -> Vector(writer)),authorityRoot)

  private def currentFootprintUuid(snapshot: Image,
      request: MySqlDriftOperationEffectCodec.Request)(using budget: MySqlFormat3ReadBudget): UUID =
    withTemporary(Math.addExact(512L, Math.multiplyExact(snapshot.relation(FootprintTable).rows.size.toLong, 64L)), "drift footprint lookup filter references") {
      val locator = exactPodRow(snapshot, LocatorTable, request.binding.bindingUuid, request.podUuid)
      val attempt = MySqlDriftOperationEffectCodec.value(locator, LocatorTable, "current_attempt_uuid")
      val footprint = MySqlDriftOperationEffectCodec.value(locator, LocatorTable, "current_footprint_uuid")
      val matches = snapshot.relation(FootprintTable).rows.filter(row =>
        MySqlDriftOperationEffectCodec.value(row, FootprintTable, "binding_uuid") ==
          MySqlDriftOperationEffectCodec.value(locator, LocatorTable, "binding_uuid") &&
        MySqlDriftOperationEffectCodec.value(row, FootprintTable, "pod_uuid") ==
          MySqlDriftOperationEffectCodec.value(locator, LocatorTable, "pod_uuid") &&
        MySqlDriftOperationEffectCodec.value(row, FootprintTable, "attempt_uuid") == attempt &&
        MySqlDriftOperationEffectCodec.value(row, FootprintTable, "footprint_uuid") == footprint)
      if matches.size != 1 || MySqlDriftOperationEffectCodec.value(matches.head, FootprintTable, "seal_state") !=
        MySqlFormat3Cell.Text("SEALED") then
        throw new java.sql.SQLTransactionRollbackException("locked mode resource has no unique sealed current footprint")
      footprint match
        case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
          val owned = bytes.copyBytes(budget, "current sealed footprint UUID")
          try
            withTemporary(1280L, "drift decoded footprint UUID vector") {
              MySqlStoredRows.decodeUuid(owned.toVector, "current sealed footprint UUID")
            }
          finally Arrays.fill(owned, 0.toByte)
        case _ => throw new SQLDataException("current footprint UUID must retain its exact 16-byte typed cell")
    }

  private def exactPodRow(snapshot: Image, table: String,
      binding: UUID, pod: UUID)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    withTemporary(Math.addExact(512L, Math.multiplyExact(snapshot.relation(table).rows.size.toLong, 64L)), "drift pod lookup filter references") {
      val rows = snapshot.relation(table).rows.filter(row =>
        MySqlDriftOperationEffectCodec.uuidEquals(
          MySqlDriftOperationEffectCodec.value(row, table, "binding_uuid"), binding) &&
        MySqlDriftOperationEffectCodec.uuidEquals(
          MySqlDriftOperationEffectCodec.value(row, table, if table == DomainTable then "uuid" else "pod_uuid"), pod))
      if rows.size != 1 then throw new java.sql.SQLTransactionRollbackException("locked mode row is not unique in " + table)
      rows.head
    }

  private def semanticWitness(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, phase: MySqlOperationEffectPhase,
      canonical: MySqlDriftPhaseCanonical, profile: SourceLimitProfile,
      historicalReserve: Option[MySqlRecoveryOperationRawEnvelope])(using budget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if phase == MySqlOperationEffectPhase.Reserve && historicalReserve.nonEmpty then
      throw new SQLDataException("reserve witness cannot refer to a future execute envelope")
    if phase == MySqlOperationEffectPhase.Execute && historicalReserve.isEmpty then
      throw new SQLDataException("execute witness must bind the exact committed reserve envelope")
    val request = MySqlDriftOperationEffectCodec.driftRequest(operation)
    withTemporary(161L, "drift shared profile canonical clone") {
      val profileBytes = profile.canonicalBytes()
      try
        MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
          MySqlRecoveryCanonicalBytes.encode(Encoding, profile) { output =>
            MySqlRecoveryCanonicalBytes.writeText(output, Domain)
            output.writeByte(1)
            MySqlRecoveryCanonicalBytes.writeToken(output,
              if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
            MySqlRecoveryCanonicalBytes.writeUuid(output, request.binding.bindingUuid)
            MySqlRecoveryCanonicalBytes.writeUuid(output, request.podUuid)
            MySqlRecoveryCanonicalBytes.writeOptionUuid(output, request.owner)
            MySqlRecoveryCanonicalBytes.writeUuid(output, request.attemptUuid)
            MySqlRecoveryCanonicalBytes.writeUuid(output, plan.workflowUuid)
            MySqlRecoveryCanonicalBytes.writeUuid(output, plan.reserveOperationUuid)
            MySqlRecoveryCanonicalBytes.writeUuid(output, plan.executeOperationUuid)
            MySqlRecoveryCanonicalBytes.writeUuid(output,
              if phase == MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid)
            output.writeLong(request.expectedStateVersion)
            MySqlRecoveryCanonicalBytes.writeText(output, request.targetMode.databaseValue)
            MySqlRecoveryCanonicalBytes.writeText(output, request.actualPose.worldName)
            output.writeLong(java.lang.Double.doubleToRawLongBits(request.actualPose.posX))
            output.writeLong(java.lang.Double.doubleToRawLongBits(request.actualPose.posY))
            output.writeLong(java.lang.Double.doubleToRawLongBits(request.actualPose.posZ))
            output.writeInt(java.lang.Float.floatToRawIntBits(request.actualPose.yaw))
            output.writeInt(java.lang.Float.floatToRawIntBits(request.actualPose.pitch))
            output.writeInt(request.actualPose.dimensionId)
            writeCanonical(output, plan.requestIdentity.canonical)
            writeBytes(output, profileBytes)
            writeCanonical(output, context.canonical)
            MySqlRecoveryCanonicalBytes.writeUuid(output, context.barrierGenerationUuid)
            output.writeLong(context.actualConnectionId)
            output.writeLong(context.writerEpoch)
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
                output.writeBoolean(true)
                output.writeInt(envelope.canonical.length)
                envelope.canonical.writeTo(output)
                withTemporary(96L, "drift historical envelope SHA output clone") {
                  output.write(envelope.sha256.toArray)
                }
              case None => output.writeBoolean(false)
          }
        }
      finally Arrays.fill(profileBytes, 0.toByte)
    }

  private def withRelations(snapshot: Image,
      requested: => Map[String, Vector[Vector[MySqlFormat3Cell]]],authorityRoot:MySqlFormat3ReadBudget)(using budget: MySqlFormat3ReadBudget): Image =
    // Map及wrapper先保留；by-name中的真实行构建另按分配收费。
    budget.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(snapshot.relations.size.toLong,128L)),
      "drift bounded replacement map metadata")
    val replacements = requested
    if replacements == null || replacements.keys.exists(table =>
      !snapshot.shapes.exists(_.table == table)) then
      throw new SQLDataException("tracked drift workflow replacement differs from original root or locked full20 set")
    snapshot match
      case deployment:MySqlScriptSchemaSnapshot=>
        if authorityRoot==null || authorityRoot.isClosed || authorityRoot.isScopedReadBudget ||
          (authorityRoot.profile ne budget.profile) || !authorityRoot.sharesAllocationLedger(budget) ||
          ((budget ne authorityRoot) && (!budget.isCanonicalChildOf(authorityRoot) ||
            budget.isCanonicalComparisonChildOf(authorityRoot))) ||
          ((deployment.allocationBudget ne authorityRoot) && (deployment.allocationBudget ne budget)) then
          throw new SQLDataException("Full26 drift graph lost exact original authority or same-ledger plan DATA owner")
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,authorityRoot)
      case legacy:MySqlFormat3FullSnapshot=>
        if legacy.allocationBudget ne budget then throw new SQLDataException("drift lost original legacy root")
    reserveVector(snapshot.relations.size, "drift full20 relation reference vector")
    budget.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(replacements.size.toLong, 128L)),
      "drift snapshot and replaced relation wrappers")
    // validateRelations的zip/key/iterator在本次验证结束撤销；不重新收共享payload。
    var validationBytes = 2048L
    var ri = 0
    while ri < snapshot.relations.size do
      val relation = snapshot.relations(ri)
      val rows = replacements.getOrElse(relation.table, relation.rows)
      var rowIndex = 0
      while rowIndex < rows.size do
        validationBytes = Math.addExact(validationBytes,
          Math.addExact(256L, Math.multiplyExact(rows(rowIndex).size.toLong, 96L)))
        rowIndex += 1
      ri += 1
    withTemporary(validationBytes, "drift snapshot validation structures") {
      val relations = snapshot.relations.map { relation =>
        replacements.get(relation.table) match
          case Some(rows) => MySqlFormat3RelationSnapshot(relation.table, rows)
          case None => relation
      }
      val next:Image=snapshot match
        case legacy:MySqlFormat3FullSnapshot=>
          val image=new MySqlFormat3FullSnapshot(legacy.schema,legacy.connection,legacy.observationScope,
            legacy.maxAllowedPacketBytes,legacy.limitProfile,relations,budget)
          image.canonicalSha256
          image
        case deployment:MySqlScriptSchemaSnapshot=>
          val image=MySqlScriptSchemaSnapshot.decoded(deployment.schema,deployment.connection,deployment.observationScope,
            deployment.maxAllowedPacketBytes,deployment.inputs,relations,budget)
          image.canonicalSha256
          image
      next
    }

  private def orderedRows(table: String,
      rows: Vector[Vector[MySqlFormat3Cell]])(using budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    if rows == null || rows.exists(_ == null) then throw new SQLDataException("complete immutable rows are required for " + table)
    val shape = MySqlFormat3RelationShapes.forTable(table)
    reserveVector(rows.size, "drift sorted row reference vector")
    withTemporary(Math.addExact(256L, Math.multiplyExact(rows.size.toLong, 64L)),
      "drift row sort scratch references") {
      rows.sortWith((left, right) => compareRowKeys(left, right, table, shape) < 0)
    }

  /** comparator只索引已有行/shape，不在每次比较创建key vectors。 */
  private def compareRowKeys(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell],
      table: String, shape: MySqlFormat3RelationShape)(using budget: MySqlFormat3ReadBudget): Int =
    var index = 0
    while index < shape.primaryKey.size do
      val name = shape.primaryKey(index)
      val compared = compareCell(cell(left, table, name), cell(right, table, name))
      if compared != 0 then return compared
      index += 1
    0

  private def compareKeys(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget): Int =
    if left.size != right.size then throw new SQLDataException("locked primary-key widths differ")
    var index = 0
    while index < left.size do
      val compared = compareCell(left(index), right(index))
      if compared != 0 then return compared
      index += 1
    0

  private def compareCell(left: MySqlFormat3Cell, right: MySqlFormat3Cell): Int = (left, right) match
    case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
    case (MySqlFormat3Cell.Floating(a), MySqlFormat3Cell.Floating(b)) => java.lang.Long.compareUnsigned(a, b)
    case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
    case _ => throw new SQLDataException("locked primary-key typed cells do not have matching SQL kinds")

  private def replaceByUuid(rows: Vector[Vector[MySqlFormat3Cell]],
      replacement: Vector[MySqlFormat3Cell])(using budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    val binding = MySqlDriftOperationEffectCodec.value(replacement, OperationTable, "binding_uuid")
    val operation = MySqlDriftOperationEffectCodec.value(replacement, OperationTable, "operation_uuid")
    var matched = -1
    var index = 0
    while index < rows.size do
      if MySqlDriftOperationEffectCodec.value(rows(index), OperationTable, "binding_uuid") == binding &&
        MySqlDriftOperationEffectCodec.value(rows(index), OperationTable, "operation_uuid") == operation then
        if matched >= 0 then throw new SQLDataException("full20 operation replacement needs one exact composite O primary key")
        matched = index
      index += 1
    if matched < 0 then throw new SQLDataException("full20 operation replacement needs one exact composite O primary key")
    reserveVector(rows.size, "drift exact O replacement reference vector")
    rows.updated(matched, replacement)

  private def defaultRow(shape: MySqlFormat3RelationShape)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    reserveVector(shape.columns.size, "drift default row reference vector")
    var cellBytes = 0L
    var columnIndex = 0
    while columnIndex < shape.columns.size do
      val column = shape.columns(columnIndex)
      if !column.nullable then
        cellBytes = Math.addExact(cellBytes, 128L)
        if column.kind == MySqlFormat3SqlKind.Binary then
          val length =
            if column.name.endsWith("_uuid") || column.name == "uuid" then 16L
            else if column.name.endsWith("_sha256") || column.name.endsWith("_fingerprint") then 32L
            else 1L
          cellBytes = Math.addExact(cellBytes, length)
      columnIndex += 1
    budget.reserveRetainedMemory(cellBytes, "drift default typed cells and owned binary arrays")
    shape.columns.map { column =>
      if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
        case MySqlFormat3SqlKind.Binary =>
          val length =
            if column.name.endsWith("_uuid") || column.name == "uuid" then 16
            else if column.name.endsWith("_sha256") || column.name.endsWith("_fingerprint") then 32
            else 1
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](length)(0.toByte)))
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
        case MySqlFormat3SqlKind.Floating =>
          MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 0, 0, 0, 0))
    }

  private def putPair(row: Vector[MySqlFormat3Cell], bytesColumn: String, shaColumn: String,
      canonical: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    if canonical == null then throw new SQLDataException("fixed canonical byte pair is required")
    val withBytes = put(row, OperationTable, bytesColumn, canonical.binaryCell)
    put(withBytes, OperationTable, shaColumn, canonical.sha256Cell)

  private def putPair(row: Vector[MySqlFormat3Cell], bytesColumn: String, shaColumn: String,
      table: String, canonical: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    if canonical == null then throw new SQLDataException("fixed canonical byte pair is required")
    val withBytes = put(row, table, bytesColumn, canonical.binaryCell)
    put(withBytes, table, shaColumn, canonical.sha256Cell)

  private def put(row: Vector[MySqlFormat3Cell], table: String, name: String,
      requested: => MySqlFormat3Cell)(using budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    if row == null then throw new SQLDataException("typed row field cannot be null")
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 then throw new SQLDataException("fixed format3 row has no " + table + "." + name)
    reserveVector(row.size, "drift updated row reference vector")
    // by-name先计新scalar/Binary cell wrapper与32-byte SHA副本的上界。
    budget.reserveRetainedMemory(192L, "drift updated typed cell representation")
    val value = requested
    if value == null then throw new SQLDataException("typed row field cannot be null")
    row.updated(index, value)

  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String)(using budget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if row == null || index < 0 || index >= row.size then
      throw new SQLDataException("fixed format3 row has no " + table + "." + name)
    row(index)

  private def signed(row: Vector[MySqlFormat3Cell], table: String, name: String)(using budget: MySqlFormat3ReadBudget): Long =
    cell(row, table, name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException("typed BIGINT required for " + table + "." + name)

  private def uuidCell(uuid: UUID)(using budget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    if uuid == null then throw new SQLDataException("exact UUID is required")
    budget.reserveRetainedMemory(144L, "drift UUID owned array and buffer")
    val bytes = ByteBuffer.allocate(16)
      .putLong(uuid.getMostSignificantBits)
      .putLong(uuid.getLeastSignificantBits)
      .array()
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))

  private def vectorCell(bytes: Vector[Byte])(using budget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    if bytes == null then throw new SQLDataException("exact binary digest is required")
    budget.reserveRetainedMemory(Math.addExact(bytes.size.toLong, 128L), "drift digest owned array")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray))

  private def zeroDigestCell()(using budget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    budget.reserveRetainedMemory(160L, "drift zero digest owned array")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(0.toByte)))

  private def doubleCell(value: Double)(using budget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    if !java.lang.Double.isFinite(value) then throw new SQLDataException("mode pose must contain finite coordinates")
    MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value))

  private def writeCanonical(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes)(using budget: MySqlFormat3ReadBudget): Unit =
    if output == null || value == null then throw new SQLDataException("semantic witness canonical field is missing")
    output.writeInt(value.length)
    value.writeTo(output)

  private def writeBytes(output: DataOutputStream, bytes: Array[Byte])(using budget: MySqlFormat3ReadBudget): Unit =
    if output == null || bytes == null then throw new SQLDataException("semantic witness byte field is missing")
    output.writeInt(bytes.length)
    output.write(bytes)

  private def writeDateTime(output: DataOutputStream, value: LocalDateTime)(using budget: MySqlFormat3ReadBudget): Unit =
    if output == null || value == null || value.getNano % 1000 != 0 then
      throw new SQLDataException("semantic witness DATETIME(6) value is invalid")
    output.writeInt(value.getYear)
    output.writeByte(value.getMonthValue)
    output.writeByte(value.getDayOfMonth)
    output.writeByte(value.getHour)
    output.writeByte(value.getMinute)
    output.writeByte(value.getSecond)
    output.writeInt(value.getNano / 1000)
