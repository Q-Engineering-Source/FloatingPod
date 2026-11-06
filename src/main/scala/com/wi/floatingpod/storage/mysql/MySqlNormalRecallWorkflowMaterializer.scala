package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import MySqlModeOperationEffectCodec.{Image,allocationBudget,limitProfile,relations,relation,shapes}

private[mysql] trait MySqlNormalRecallPlannedRoute:
  private[mysql] def executeNormalRecall(scope: MySqlRepositoryCommandScope,
      operation: MySqlRepositoryOperation, prepared: Option[MySqlFencedTransaction.NormalRecallCommitted]
  ): scala.util.Try[MySqlFencedTransaction.NormalRecallCommitted]

/** 计划 DATA，不能作为 commit 或物理许可。所有图与派生字节仍属于原 retained root。 */
private[mysql] final class MySqlNormalRecallMaterialization private[mysql] (
    val header: MySqlNormalRecallHeader, val plan: MySqlOwnedRepositoryOperationPlan,
    val plannedAt: LocalDateTime, val scope: MySqlRecoveryCanonicalBytes,
    private val initialImage: Image, private val reservedImage: Image,
    private val executedImage: Image, val reserve: MySqlNormalRecallPhase,
    val execute: MySqlNormalRecallPhase, val root: MySqlFormat3ReadBudget,
    val exactOperation: MySqlRepositoryOperation):
  def preimage: MySqlFormat3FullSnapshot = MySqlControlledDebugWorkflowMaterializer.legacyImage(initialImage)
  def reserveTarget: MySqlFormat3FullSnapshot = MySqlControlledDebugWorkflowMaterializer.legacyImage(reservedImage)
  def executeTarget: MySqlFormat3FullSnapshot = MySqlControlledDebugWorkflowMaterializer.legacyImage(executedImage)
  def completePreimage: Image = initialImage
  def completeReserveTarget: Image = reservedImage
  def completeExecuteTarget: Image = executedImage
  def isDeploymentReceipt: Boolean = initialImage.isInstanceOf[MySqlScriptSchemaSnapshot]
  def exactPlan: MySqlOwnedRepositoryOperationPlan = plan
  def originalRoot: MySqlFormat3ReadBudget = root
  def context: MySqlRepositoryOperationContextIdentity = header.context
  def effects: Vector[MySqlNormalRecallPhase] = Vector(reserve, execute)
  def requireObserved(phase: MySqlOperationEffectPhase, actual: Image, target: Boolean): Unit =
    (if phase == MySqlOperationEffectPhase.Reserve then reserve else execute).requireObserved(actual, target)
  def requireBoundTo(scope: MySqlRepositoryCommandScope, operation: MySqlRepositoryOperation): Unit =
    if scope == null || (scope.operationPlan() ne plan) || operation != exactOperation then
      throw new SQLDataException("normal recall command lost its exact retained plan")
    scope.requireCurrent(scope.exactAuditScope(), plan)
    plan.requireSingle(operation, root.profile)
    verify()
  def verify(): Unit =
    reserve.requireObserved(initialImage, false)
    reserve.requireObserved(reservedImage, true)
    execute.requireObserved(reservedImage, false)
    execute.requireObserved(executedImage, true)

private[mysql] final class MySqlNormalRecallPhase private[mysql] (
    val header: MySqlNormalRecallHeader, val plan: MySqlOwnedRepositoryOperationPlan,
    val plannedAt: LocalDateTime, val phase: MySqlOperationEffectPhase,
    val scope: MySqlRecoveryCanonicalBytes, val before: Image,
    val target: Image, val beforeBytes: MySqlRecoveryCanonicalBytes,
    val targetBytes: MySqlRecoveryCanonicalBytes, val fingerprint: MySqlRecoveryCanonicalBytes,
    val operationContext: MySqlOperationContext, val contextBytes: MySqlRecoveryCanonicalBytes,
    val witness: MySqlRecoveryCanonicalBytes, val originalReserve: Option[MySqlRecoveryOperationRawEnvelope],
    val root: MySqlFormat3ReadBudget, val receipt: Option[UUID] = None):
  val operationUuid: UUID = if phase == MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid
  def requireObserved(actual: Image, targetImage: Boolean): Unit =
    if root == null || root.isClosed || actual == null || actual.allocationBudget.isClosed ||
      !actual.allocationBudget.sharesAllocationLedger(root) then
      throw new SQLDataException("recall phase lost original open allocation root")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val beforeAgain = MySqlNormalRecallOperationEffectCodec.projection(before, header, phase,
        MySqlOperationEffectImage.Before, receipt, root)
      val targetAgain = MySqlNormalRecallOperationEffectCodec.projection(target, header, phase,
        MySqlOperationEffectImage.Target, receipt, root)
      val fingerprintAgain = MySqlNormalRecallOperationEffectCodec.fingerprint(header, phase,
        plan.requestIdentity.canonical, scope, beforeAgain, targetAgain, root)
      val contextAgain = MySqlNormalRecallOperationEffectCodec.context(header, operationUuid, fingerprintAgain)
      val encodedContext = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(contextAgain, root.profile)
      val witnessAgain = MySqlNormalRecallWorkflowMaterializer.witness(header, phase, plannedAt,
        scope, fingerprintAgain, encodedContext, beforeAgain, targetAgain, originalReserve, root)
      if !beforeBytes.exactlyEquals(beforeAgain) || !targetBytes.exactlyEquals(targetAgain) ||
        !fingerprint.exactlyEquals(fingerprintAgain) || !contextBytes.exactlyEquals(encodedContext) ||
        !witness.exactlyEquals(witnessAgain) then
        throw new SQLDataException("recall retained phase differs from independent regeneration")
      MySqlNormalRecallOperationEffectCodec.requireExactReadback(if targetImage then target else before, actual, root)
    }

private[mysql] object MySqlNormalRecallWorkflowMaterializer:
  import MySqlControlledDebugRows.*

  private def requireGraph(image: Image, context: MySqlRepositoryOperationContextIdentity,
      root: MySqlFormat3ReadBudget): Unit = image match
    case legacy: MySqlFormat3FullSnapshot =>
      require(context.schemaFormat == 3, "legacy recall graph requires schema3 context")
      MySqlControlledDebugRelations.requireValidFullGraph(legacy,root)
    case deployment: MySqlScriptSchemaSnapshot =>
      require(context.schemaFormat == 5, "Full26 recall graph requires schema5 context")
      MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(deployment,deployment.inputs,root)
  /** 私有草稿只共享已验 base 和完整 immutable 行；绝不实现 Image 或发行未验 snapshot。 */
  private final class Construction(val base: Image, val relations: Vector[MySqlFormat3RelationSnapshot],
      val root: MySqlFormat3ReadBudget):
    def relation(table: String): MySqlFormat3RelationSnapshot =
      relations.find(_.table == table).getOrElse(fail("construction table is outside the original closed layout"))

  private def construction(base: Image, root: MySqlFormat3ReadBudget): Construction =
    MySqlNormalRecallOperationEffectCodec.requireConstructionRelations(base, base.relations, root)
    root.reserveRetainedMemory(256L, "recall private construction holder")
    new Construction(base, base.relations, root)

  private def put(draft: Construction, table: String, row: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget, existing: Boolean): Construction =
    if draft == null || (draft.root ne root) || root.isClosed || root.isScopedReadBudget ||
        table == null || row == null then fail("exact private construction/root/row required")
    val shape = draft.base.shapes.find(_.table == table).getOrElse(fail("construction table is outside closed layout"))
    if row.size != shape.columns.size then fail("complete construction row width required")
    val source = draft.relation(table).rows
    root.reserveRetainedMemory(Math.addExact(16384L,
      Math.multiplyExact(Math.addExact(source.size.toLong, 1L), 256L)), "recall construction patch containers")
    var scratch = 16384L
    draft.relations.foreach { relation =>
      val width = draft.base.shapes.find(_.table == relation.table).getOrElse(fail("closed construction shape required")).columns.size.toLong
      scratch = Math.addExact(scratch, Math.multiplyExact(Math.addExact(relation.rows.size.toLong, 1L),
        Math.addExact(1024L, Math.multiplyExact(width, 256L))))
    }
    root.reserveTemporaryMemory(scratch, "recall construction patch ordering")
    var primary: Throwable = null
    try
      var matched = -1
      var index = 0
      while index < source.size do
        if MySqlControlledDebugDelta.compareKey(source(index), row, shape) == 0 then
          if matched >= 0 then fail("duplicate complete construction primary key")
          matched = index
        index += 1
      if existing && matched < 0 then fail("required original construction row is missing")
      if !existing && matched >= 0 then fail("new construction row collides with original key")
      val changed = if matched >= 0 then source.updated(matched, row)
        else (source :+ row).sortWith((left, right) => MySqlControlledDebugDelta.compareKey(left, right, shape) < 0)
      val complete = draft.relations.map { relation =>
        if relation.table == table then MySqlFormat3RelationSnapshot(table, changed) else relation
      }
      new Construction(draft.base, complete, root)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(scratch, "recall construction patch ordering release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def one(image: Image, table: String, matches: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] = image match
    case legacy: MySqlFormat3FullSnapshot => MySqlControlledDebugImages.one(legacy,table,matches)
    case deployment: MySqlScriptSchemaSnapshot => MySqlControlledDebugImages.one(deployment,table,matches)

  private def one(draft: Construction, table: String,
      matches: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
    var found: Vector[MySqlFormat3Cell] = null
    draft.relation(table).rows.foreach { row =>
      if matches(row) then
        if found != null then fail("construction has more than one exact row")
        found = row
    }
    if found == null then fail("construction lacks exact row")
    found

  private def writer(draft: Construction, id: UUID, fingerprint: Vector[Byte],
      root: MySqlFormat3ReadBudget): Construction =
    if fingerprint == null || fingerprint.size != 32 then fail("complete writer context digest required")
    val rows = draft.relation("fp_runtime_writer").rows
    if rows.size != 1 then fail("one original writer row required")
    root.reserveRetainedMemory(4096L, "recall construction writer field map")
    put(draft, "fp_runtime_writer", update(rows.head, "fp_runtime_writer", Map(
      "last_commit_operation_uuid" -> uuid(id),
      "last_commit_fingerprint" -> MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(fingerprint.toArray))), root), root, true)

  /** 最终字段全部确定后每个 target 只在这里严格接纳一次。 */
  private def admit(draft: Construction): Image =
    val base = draft.base
    val root = draft.root
    var scratch = 16384L
    draft.relations.foreach { relation =>
      val width = base.shapes.find(_.table == relation.table).getOrElse(fail("closed final construction shape required")).columns.size.toLong
      scratch = Math.addExact(scratch, Math.multiplyExact(Math.addExact(relation.rows.size.toLong, 1L),
        Math.addExact(1024L, Math.multiplyExact(width, 256L))))
    }
    root.reserveTemporaryMemory(scratch, "recall final strict image admission")
    var primary: Throwable = null
    try
      base match
        case legacy: MySqlFormat3FullSnapshot =>
          new MySqlFormat3FullSnapshot(legacy.schema, legacy.connection, legacy.observationScope,
            legacy.maxAllowedPacketBytes, legacy.limitProfile, draft.relations, root)
        case deployment: MySqlScriptSchemaSnapshot =>
          MySqlScriptSchemaSnapshot.decoded(deployment.schema, deployment.connection, deployment.observationScope,
            deployment.maxAllowedPacketBytes, deployment.inputs, draft.relations, root)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(scratch, "recall final strict image admission release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  /** 初次 P：只新增 Rp/P、接管原 POD R、提交 P 与 W；原 domain/locator/source/history 完全不变。 */
  def prepareInitial(request: MySqlNormalRecallRequest, plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity, at: LocalDateTime,
      before: Image, root: MySqlFormat3ReadBudget): MySqlNormalRecallMaterialization =
    if request == null || plan == null || context == null || at == null || before == null || root == null ||
      root.isClosed || root.isScopedReadBudget || !root.sharesAllocationLedger(before.allocationBudget) ||
      (root.profile ne context.exactLimitProfile) || (root.profile ne before.limitProfile) ||
      context.tag != plan.tag || context.workflowUuid != plan.workflowUuid ||
      context.reserveOperationUuid != plan.reserveOperationUuid || context.requestIdentitySha256 != plan.requestIdentitySha256 ||
      at.getNano % 1000 != 0 || at.getYear < 1000 || at.getYear > 9999 || !at.isBefore(context.fixedLeaseUntil) ||
      request.purpose != MySqlNormalRecallPurpose.NormalRecall then fail("exact initial P planning inputs required")
    val operation = MySqlRepositoryOperation.PrepareNormalRecall(request)
    plan.requireSingle(operation, root.profile)
    requireGraph(before, context, root)
    val locator = requireBase(before, request, context)
    val revision = number(locator, Locator, "locator_revision")
    val header = MySqlNormalRecallHeader(request, context, None, revision)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val scope = MySqlNormalRecallCanonical.prepare(header, request.purpose, before, None, None, None, None, root)
      materialize(header, plan, at, before, construction(before, root), scope, MySqlNormalRecallStage.Prepare, None, root, operation)
    }

  /** 无C分支的P2计划DATA；原P/Rp历史由真实完整库存核验，不发行恢复或物理资格。 */
  def prepareActiveRecoveryWithoutCheckpoint(request: MySqlNormalRecallRequest,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      at: LocalDateTime, before: Image, originalPrepare: UUID,
      root: MySqlFormat3ReadBudget): MySqlNormalRecallMaterialization =
    require(request != null && request.purpose == MySqlNormalRecallPurpose.RestoreCommittedActive,
      "active no-C P2 request required")
    prepareRecoveryData(request, plan, context, at, before, originalPrepare, None, root)

  /** 原C已提交的inactive P2计划DATA；仅重取原RELEASED POD键，不改变原inactive结果。 */
  def prepareInactiveRecovery(request: MySqlNormalRecallRequest,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      at: LocalDateTime, before: Image, originalPrepare: UUID, originalCheckpoint: UUID,
      root: MySqlFormat3ReadBudget): MySqlNormalRecallMaterialization =
    require(request != null && request.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive &&
      MySqlNormalRecallRequest.nonZero(originalCheckpoint), "inactive P2 requires its original committed C")
    prepareRecoveryData(request, plan, context, at, before, originalPrepare, Some(originalCheckpoint), root)

  private def prepareRecoveryData(request: MySqlNormalRecallRequest,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      at: LocalDateTime, before: Image, originalPrepare: UUID, originalCheckpoint: Option[UUID],
      root: MySqlFormat3ReadBudget): MySqlNormalRecallMaterialization =
    if request == null || plan == null || context == null || at == null || before == null || root == null ||
      root.isClosed || root.isScopedReadBudget || !root.sharesAllocationLedger(before.allocationBudget) ||
      (root.profile ne context.exactLimitProfile) || (root.profile ne before.limitProfile) ||
      context.schemaFormat != 5 || context.tag != plan.tag || context.workflowUuid != plan.workflowUuid ||
      context.reserveOperationUuid != plan.reserveOperationUuid || context.requestIdentitySha256 != plan.requestIdentitySha256 ||
      at.getNano % 1000 != 0 || at.getYear < 1000 || at.getYear > 9999 || !at.isBefore(context.fixedLeaseUntil) ||
      !MySqlNormalRecallRequest.nonZero(originalPrepare) then fail("exact P2 DATA planning inputs required")
    val operation = MySqlRepositoryOperation.PrepareNormalRecall(request)
    plan.requireSingle(operation, root.profile)
    requireGraph(before, context, root)
    val locator = requireBase(before, request, context)
    val previous = originalCheckpoint.getOrElse(originalPrepare)
    val header = MySqlNormalRecallHeader(request, context, Some(previous), number(locator, Locator, "locator_revision"))
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val predecessor = MySqlRecoveryOperationRawEnvelopeCodec.encode(
        this.operation(before, context.bindingUuid, previous), root.profile, inputsForImage(before, root))
      val scope = MySqlNormalRecallCanonical.prepare(header, request.purpose, before,
        Some(predecessor), Some(originalPrepare), originalCheckpoint, None, root)
      val inputs = before match
        case deployment: MySqlScriptSchemaSnapshot => deployment.inputs
        case _ => fail("P2 requires its original complete schema5 inventory")
      val decoded = MySqlNormalRecallCanonical.decodePrepare(scope.borrowedBinary, root, Some(inputs))
      if originalCheckpoint.isEmpty then
        MySqlNormalRecallRecovery.requireActivePrepareWithoutCheckpointHistoryData(decoded, before, root)
      else MySqlNormalRecallRecovery.requireInactivePrepareHistoryData(decoded, before, before.relations,
        MySqlOperationEffectPhase.Reserve, root)
      materialize(header, plan, at, before, construction(before, root), scope, MySqlNormalRecallStage.Prepare, None, root, operation)
    }

  private def materialize(header: MySqlNormalRecallHeader, plan: MySqlOwnedRepositoryOperationPlan,
      at: LocalDateTime, before: Image, domainTarget: Construction,
      scope: MySqlRecoveryCanonicalBytes, stage: MySqlNormalRecallStage, receipt: Option[UUID],
      root: MySqlFormat3ReadBudget, operation: MySqlRepositoryOperation): MySqlNormalRecallMaterialization =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(32768L, "recall materialization phase wrappers and fixed maps")
      val seed = MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_INTERNAL_SEED_V1", root)(_.writeByte(0))
      val reserveBefore = project(before, header, MySqlOperationEffectPhase.Reserve, false, root)
      val reserveSeed = MySqlControlledDebugOperationRows.preimage(operationRow(header, plan, at, true, stage, scope, seed, root), reserveBefore, root)
      val executeSeed = operationRow(header, plan, at, false, stage, scope, seed, root)
      val (held, existed) = if header.previousOperation.isEmpty then initialReservation(before, header, root)
        else (retargetReservation(before, header, root), true)
      var scaffold = put(construction(before, root), Operation, reserveSeed, root, false)
      scaffold = put(scaffold, Operation, executeSeed, root, false)
      scaffold = put(scaffold, Reservation, held, root, existed)
      scaffold = writer(scaffold, plan.reserveOperationUuid, seed.sha256, root)
      val reserveTargetBytes = projectConstruction(scaffold, header, MySqlOperationEffectPhase.Reserve, true, root)
      val reserveFp = MySqlNormalRecallOperationEffectCodec.fingerprint(header, MySqlOperationEffectPhase.Reserve,
        plan.requestIdentity.canonical, scope, reserveBefore, reserveTargetBytes, root)
      val reserveContext = MySqlNormalRecallOperationEffectCodec.context(header, plan.reserveOperationUuid, reserveFp)
      val reserveContextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(reserveContext, root.profile)
      val reserveWitness = witness(header, MySqlOperationEffectPhase.Reserve, at, scope, reserveFp,
        reserveContextBytes, reserveBefore, reserveTargetBytes, None, root)
      val reserveFinal = MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.phase(reserveSeed,
        reserveContextBytes, reserveBefore, reserveTargetBytes, reserveFp, root), reserveWitness, at, root)
      var reserveBase = put(scaffold, Operation, reserveFinal, root, true)
      reserveBase = writer(reserveBase, plan.reserveOperationUuid,
        reserveContext.canonicalFingerprintSha256, root)
      val executeBefore = projectConstruction(reserveBase, header, MySqlOperationEffectPhase.Execute, false, root)
      val preparedReservation = update(held, Reservation, Map(
        "reservation_revision" -> signed(Math.addExact(number(held, Reservation, "reservation_revision"), 1L)),
        "reservation_state" -> text(if stage == MySqlNormalRecallStage.Prepare then "HELD" else "RELEASED"),
        "last_mutation_operation_uuid" -> uuid(plan.executeOperationUuid)), root)
      var executeScaffold = applyDomainTarget(before, domainTarget, reserveBase, root)
      executeScaffold = put(executeScaffold, Reservation, preparedReservation, root, true)
      executeScaffold = put(executeScaffold, Operation,
        MySqlControlledDebugOperationRows.commit(MySqlControlledDebugOperationRows.preimage(executeSeed, executeBefore, root), seed, at, root), root, true)
      executeScaffold = writer(executeScaffold, plan.executeOperationUuid, seed.sha256, root)
      val executeTargetBytes = projectConstruction(executeScaffold, header, MySqlOperationEffectPhase.Execute, true, root, receipt)
      val executeFp = MySqlNormalRecallOperationEffectCodec.fingerprint(header, MySqlOperationEffectPhase.Execute,
        plan.requestIdentity.canonical, scope, executeBefore, executeTargetBytes, root)
      val executeContext = MySqlNormalRecallOperationEffectCodec.context(header, plan.executeOperationUuid, executeFp)
      val executeContextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(executeContext, root.profile)
      val executeStub = MySqlControlledDebugOperationRows.phase(executeSeed, executeContextBytes,
        executeBefore, executeTargetBytes, executeFp, root)
      val reserveDraft = put(reserveBase, Operation, executeStub, root, true)
      val reserveRaw = MySqlRecoveryOperationRawEnvelopeCodec.encode(reserveFinal, root.profile, inputsForImage(before, root))
      val executeWitness = witness(header, MySqlOperationEffectPhase.Execute, at, scope, executeFp,
        executeContextBytes, executeBefore, executeTargetBytes, Some(reserveRaw), root)
      val executeFinal = MySqlControlledDebugOperationRows.commit(executeStub, executeWitness, at, root)
      var executeDraft = put(executeScaffold, Operation, executeFinal, root, true)
      receipt.foreach { id =>
        val completion = one(executeDraft, "fp_lifecycle_completion", row =>
          value(row, "fp_lifecycle_completion", "receipt_uuid") == uuid(id) &&
          value(row, "fp_lifecycle_completion", "binding_uuid") == uuid(header.context.bindingUuid))
        executeDraft = put(executeDraft, "fp_lifecycle_completion",
          update(completion, "fp_lifecycle_completion", Map("original_context_bytes" -> executeContextBytes.binaryCell,
            "original_context_sha256" -> executeContextBytes.sha256Cell), root), root, true)
      }
      executeDraft = writer(executeDraft, plan.executeOperationUuid,
        executeContext.canonicalFingerprintSha256, root)
      val reserveTarget = admit(reserveDraft)
      val executeTarget = admit(executeDraft)
      val reservePhase = new MySqlNormalRecallPhase(header, plan, at, MySqlOperationEffectPhase.Reserve, scope,
        before, reserveTarget, reserveBefore, reserveTargetBytes, reserveFp, reserveContext, reserveContextBytes, reserveWitness, None, root)
      val executePhase = new MySqlNormalRecallPhase(header, plan, at, MySqlOperationEffectPhase.Execute, scope,
        reserveTarget, executeTarget, executeBefore, executeTargetBytes, executeFp, executeContext, executeContextBytes, executeWitness, Some(reserveRaw), root, receipt)
      val result = new MySqlNormalRecallMaterialization(header, plan, at, scope, before, reserveTarget, executeTarget,
        reservePhase, executePhase, root, operation)
      result.verify()
      result
    }

  private def project(snapshot: Image, header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase, target: Boolean, root: MySqlFormat3ReadBudget,
      receipt: Option[UUID] = None): MySqlRecoveryCanonicalBytes =
    MySqlNormalRecallOperationEffectCodec.projection(snapshot, header, phase,
      if target then MySqlOperationEffectImage.Target else MySqlOperationEffectImage.Before, receipt, root)

  private def projectConstruction(draft: Construction, header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase, target: Boolean, root: MySqlFormat3ReadBudget,
      receipt: Option[UUID] = None): MySqlRecoveryCanonicalBytes =
    if draft.root ne root then fail("construction projection switched original root")
    MySqlNormalRecallOperationEffectCodec.constructionProjection(draft.base, draft.relations, header, phase,
      if target then MySqlOperationEffectImage.Target else MySqlOperationEffectImage.Before, receipt, root)

  /** C 的计划来自已完成 P 和已发生的物理事实；本纯构造不把 DATA 升级为已提交凭证。 */
  def checkpoint(request: MySqlNormalRecallCheckpointRequest, prepared: MySqlNormalRecallMaterialization,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      at: LocalDateTime, before: Image, root: MySqlFormat3ReadBudget): MySqlNormalRecallMaterialization =
    if request == null || prepared == null || plan == null || context == null || at == null ||
      before == null || root == null || root.isClosed || root.isScopedReadBudget ||
      !before.allocationBudget.sharesAllocationLedger(root) || !prepared.root.sharesAllocationLedger(root) ||
      (root.profile ne context.exactLimitProfile) || (root.profile ne before.limitProfile) ||
      request.original != prepared.header.request || request.removal.tag != prepared.plan.tag ||
      plan.tag != context.tag || plan.workflowUuid != prepared.plan.workflowUuid || context.workflowUuid != plan.workflowUuid ||
      plan.reserveOperationUuid != context.reserveOperationUuid || plan.requestIdentitySha256 != context.requestIdentitySha256 ||
      context.tag.bootId != prepared.plan.tag.bootId || context.tag.bootGeneration != prepared.plan.tag.bootGeneration ||
      context.tag.connectionGeneration != prepared.plan.tag.connectionGeneration ||
      context.actualConnectionId != prepared.header.context.actualConnectionId ||
      context.barrierGenerationUuid != prepared.header.context.barrierGenerationUuid ||
      context.writerEpoch != prepared.header.context.writerEpoch || at.getNano % 1000 != 0 ||
      at.getYear < 1000 || at.getYear > 9999 || !at.isBefore(context.fixedLeaseUntil) then fail("exact original P and checkpoint inputs required")
    prepared.verify()
    plan.requireSingle(MySqlRepositoryOperation.CheckpointNormalRecall(request), root.profile)
    requireGraph(before, context, root)
    val p = operation(before, context.bindingUuid, prepared.plan.executeOperationUuid)
    if p != operation(prepared.completeExecuteTarget, context.bindingUuid, prepared.plan.executeOperationUuid) ||
      value(p, Operation, "operation_state") != text("COMMITTED") || value(p, Operation, "step") != text("RECALL_PREPARE") then
      fail("actual original committed P differs")
    val locator = requireBase(before, request.original, context)
    if number(locator, Locator, "locator_revision") != prepared.header.locatorRevision then fail("original P locator changed")
    val header = MySqlNormalRecallHeader(request.original, context, Some(prepared.plan.executeOperationUuid), prepared.header.locatorRevision)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(32768L, "recall checkpoint domain and completion maps")
      val domain = one(before, Domain, r => value(r, Domain, "uuid") == uuid(request.original.podUuid))
      val pose = request.original.finalPose
      val nextVersion = Math.addExact(request.original.criticalBase.stateVersion, 1L)
      val changed = update(domain, Domain, Map("is_active" -> signed(0), "state_version" -> signed(nextVersion),
        "world_name" -> text(pose.worldName), "pos_x" -> floating(pose.posX), "pos_y" -> floating(pose.posY),
        "pos_z" -> floating(pose.posZ), "yaw" -> floating(pose.yaw.toDouble), "pitch" -> floating(pose.pitch.toDouble),
        "last_updated" -> signed(at.toInstant(java.time.ZoneOffset.UTC).toEpochMilli),
        "last_mutation_operation_uuid" -> uuid(plan.executeOperationUuid)), root)
      var target = put(construction(before, root), Domain, changed, root, true)
      target = put(target, Locator, update(locator, Locator, Map(
        "locator_state" -> text("ROW_INACTIVE_PENDING"), "reason_code" -> text("ROW_INACTIVE_PENDING"),
        "domain_version" -> signed(nextVersion), "locator_revision" -> signed(Math.addExact(header.locatorRevision, 1L)),
        "last_mutation_operation_uuid" -> uuid(plan.executeOperationUuid)), root), root, true)
      target = pendingCheckpointObligation(target, header, root)
      val checkpointBytes = MySqlNormalRecallCanonical.checkpointConstruction(header, prepared.scope, request.removal,
        request.ticket, request.receiptUuid, before, target.base, target.relations, root)
      val removed = MySqlNormalRecallCanonical.removal(request.removal, root)
      val ticket = MySqlNormalRecallCanonical.ticket(request.ticket, root)
      // 两个 context 派生单元由本 C 的 exact marker 解环，随后由独立再生 context 填回。
      val seed = MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_INTERNAL_CONTEXT_V1", root)(_.writeByte(0))
      val completion = row("fp_lifecycle_completion", Map(
        "binding_uuid" -> uuid(context.bindingUuid), "receipt_uuid" -> uuid(request.receiptUuid),
        "completion_operation_uuid" -> uuid(context.tag.operationId), "completion_kind" -> text("RECALL"),
        "boot_uuid" -> uuid(context.tag.bootId), "boot_generation" -> signed(context.tag.bootGeneration),
        "world_identity" -> text(request.original.source.worldIdentity), "source_identity" -> text(request.original.source.sourceIdentity),
        "pod_uuid" -> uuid(request.original.podUuid), "owner_uuid" -> uuid(request.original.criticalBase.ownerUuid.get),
        "attempt_uuid" -> uuid(request.original.attemptUuid), "footprint_uuid" -> uuid(request.original.footprintUuid),
        "before_domain_version" -> signed(request.original.criticalBase.stateVersion), "after_domain_version" -> signed(nextVersion),
        "removal_result_bytes" -> removed.binaryCell, "removal_result_sha256" -> removed.sha256Cell,
        "ticket_result_bytes" -> ticket.binaryCell, "ticket_result_sha256" -> ticket.sha256Cell,
        "checkpoint_bytes" -> checkpointBytes.binaryCell, "checkpoint_sha256" -> checkpointBytes.sha256Cell,
        "original_context_bytes" -> seed.binaryCell, "original_context_sha256" -> seed.sha256Cell,
        "allowed_use" -> text("REARM_AFTER_RECALL"), "completion_state" -> text("CHECKPOINT_COMMITTED"),
        "consumed_by_operation_uuid" -> Null, "completion_revision" -> signed(0),
        "last_mutation_operation_uuid" -> uuid(context.tag.operationId)), root)
      target = put(target, "fp_lifecycle_completion", completion, root, false)
      materialize(header, plan, at, before, target, checkpointBytes, MySqlNormalRecallStage.Checkpoint,
        Some(request.receiptUuid), root, MySqlRepositoryOperation.CheckpointNormalRecall(request))
    }

  private def pendingCheckpointObligation(before: Construction, header: MySqlNormalRecallHeader,
      root: MySqlFormat3ReadBudget): Construction =
    val table = "physical_recovery_obligation"
    val request = header.request
    def selected(row: Vector[MySqlFormat3Cell], table: String): Boolean =
      value(row, table, "binding_uuid") == uuid(header.context.bindingUuid) && value(row, table, "pod_uuid") == uuid(request.podUuid) &&
        value(row, table, "attempt_uuid") == uuid(request.attemptUuid) && value(row, table, "footprint_uuid") == uuid(request.footprintUuid)
    val footprint = one(before, Footprint, selected(_, Footprint))
    val origin = value(footprint, Footprint, "origin_version")
    val existing = before.relation(table).rows.filter(selected(_, table))
    if existing.size > 1 then fail("duplicate checkpoint obligation")
    existing.headOption match
      case Some(old) =>
        if value(old, table, "obligation_state") != text("PENDING") || value(old, table, "origin_version") != origin ||
          value(old, table, "verified_operation_uuid") != Null || value(old, table, "verification_proof_bytes") != Null ||
          value(old, table, "verification_proof_sha256") != Null then fail("active checkpoint has an already verified or mismatched obligation")
        before
      case None =>
        val obligation = row(table, Map("binding_uuid" -> uuid(header.context.bindingUuid), "pod_uuid" -> uuid(request.podUuid),
          "attempt_uuid" -> uuid(request.attemptUuid), "footprint_uuid" -> uuid(request.footprintUuid), "origin_version" -> origin,
          "purpose" -> text("SAME_ATTEMPT_CHECKPOINT"), "created_operation_uuid" -> uuid(header.context.tag.operationId),
          "attempt_retirement_operation_uuid" -> Null, "obligation_state" -> text("PENDING"), "obligation_revision" -> signed(0),
          "verified_operation_uuid" -> Null, "verification_proof_bytes" -> Null, "verification_proof_sha256" -> Null,
          "last_mutation_operation_uuid" -> uuid(header.context.tag.operationId)), root)
        put(before, table, obligation, root, false)

  private def operation(snapshot: Image, binding: UUID, operation: UUID): Vector[MySqlFormat3Cell] =
    one(snapshot, Operation, row => value(row, Operation, "binding_uuid") == uuid(binding) &&
      value(row, Operation, "operation_uuid") == uuid(operation))

  private def retargetReservation(before: Image, header: MySqlNormalRecallHeader,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val old = one(before, Reservation, row => value(row, Reservation, "binding_uuid") == uuid(header.context.bindingUuid) &&
      value(row, Reservation, "resource_pod_uuid") == uuid(header.request.podUuid))
    val predecessor = header.previousOperation.getOrElse(fail("retarget needs actual predecessor"))
    val previous = operation(before, header.context.bindingUuid, predecessor)
    val key = MySqlRecoveryResourceKey.pod(header.request.podUuid, root.profile)
    val encodedKey = MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", root.profile)(key.writeCanonicalTo)
    val inactive = header.request.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive
    if value(old, Reservation, "reservation_state") != text(if inactive then "RELEASED" else "HELD") ||
      value(old, Reservation, "resource_kind") != text("POD") ||
      value(old, Reservation, "resource_key") != encodedKey.binaryCell || value(old, Reservation, "resource_key_hash") != encodedKey.sha256Cell ||
      value(old, Reservation, "resource_unit_uuid") != Null || value(old, Reservation, "operation_uuid") != uuid(predecessor) ||
      value(old, Reservation, "workflow_uuid") != uuid(header.context.workflowUuid) ||
      value(previous, Operation, "operation_state") != text("COMMITTED") || value(previous, Operation, "workflow_uuid") != uuid(header.context.workflowUuid) ||
      (inactive && (value(previous, Operation, "step") != text("RECALL_CHECKPOINT") ||
        value(old, Reservation, "last_mutation_operation_uuid") != uuid(predecessor))) then
      fail("retarget lost original held workflow and committed predecessor")
    update(old, Reservation, Map("operation_uuid" -> uuid(header.context.tag.operationId), "reservation_state" -> text("HELD"),
      "reservation_revision" -> signed(Math.addExact(number(old, Reservation, "reservation_revision"), 1L)),
      "last_mutation_operation_uuid" -> uuid(header.context.reserveOperationUuid)), root)

  private def applyDomainTarget(original: Image, target: Construction,
      reserve: Construction, root: MySqlFormat3ReadBudget): Construction =
    if (target.base ne original) || (reserve.base ne original) || (target.root ne root) || (reserve.root ne root) then
      fail("domain target construction lost its exact original base/root")
    var result = reserve
    target.relations.foreach { relation =>
      val before = original.relation(relation.table)
      if relation != before then
        if !Set(Domain, Locator, "physical_recovery_obligation", "fp_lifecycle_completion").contains(relation.table) then
          fail("Recall target changed unrelated source or history relation")
        val shape = MySqlFormat3RelationShapes.forTable(relation.table)
        val keyColumns = shape.primaryKey.map(name => shape.columns.indexWhere(_.name == name))
        def sameKey(a: Vector[MySqlFormat3Cell], b: Vector[MySqlFormat3Cell]): Boolean = keyColumns.forall(i => a(i) == b(i))
        if before.rows.exists(row => !relation.rows.exists(sameKey(row, _))) then fail("Recall target removed historical row")
        relation.rows.foreach { row =>
          val old = before.rows.find(sameKey(row, _))
          if !old.contains(row) then result = put(result, relation.table, row, root, old.nonEmpty)
        }
    }
    result

  private[mysql] def witness(header: MySqlNormalRecallHeader, phase: MySqlOperationEffectPhase,
      at: LocalDateTime, scope: MySqlRecoveryCanonicalBytes, fingerprint: MySqlRecoveryCanonicalBytes,
      context: MySqlRecoveryCanonicalBytes, before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      reserve: Option[MySqlRecoveryOperationRawEnvelope], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require((phase == MySqlOperationEffectPhase.Execute) == reserve.nonEmpty, "execute witness requires original reserve row")
    reserve.foreach { raw =>
      MySqlNormalRecallHistoricalPayload.requireValid(raw.row, root, raw.deploymentInputs)
      if !raw.exactlyMatches(raw.row, root.profile) ||
        value(raw.row, Operation, "binding_uuid") != uuid(header.context.bindingUuid) ||
        value(raw.row, Operation, "workflow_uuid") != uuid(header.context.workflowUuid) ||
        value(raw.row, Operation, "step") != text("RESERVE") ||
        value(raw.row, Operation, "operation_state") != text("COMMITTED") then fail("original reserve RAW context differs")
    }
    MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_WITNESS_BE_V2", root) { output =>
      output.writeByte(if phase == MySqlOperationEffectPhase.Reserve then 1 else 2)
      MySqlRecoveryCanonicalBytes.writeUuid(output, header.context.workflowUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, header.context.reserveOperationUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, header.context.tag.operationId)
      MySqlNormalRecallCanonical.cell(output, MySqlFormat3Cell.DateTime(at))
      Vector(scope, fingerprint, context, before, target).foreach { bytes => output.writeInt(bytes.length); bytes.writeTo(output) }
      output.writeBoolean(reserve.nonEmpty)
      reserve.foreach { raw =>
        if raw.operationUuid != header.context.reserveOperationUuid then fail("foreign reserve witness")
        output.writeInt(raw.length)
        val shape = MySqlFormat3RelationShapes.forTable(Operation)
        output.writeInt(shape.columns.size)
        shape.columns.zipWithIndex.foreach { (column, index) =>
          MySqlRecoveryCanonicalBytes.writeText(output, column.name)
          MySqlRecoveryCanonicalBytes.writeText(output, column.sqlDeclaration)
          if !MySqlNormalRecallHistoricalPayload.writeDescriptor(output, raw.row, column.name) then
            MySqlNormalRecallCanonical.cell(output, raw.row(index))
        }
      }
    }

  private def operationRow(header: MySqlNormalRecallHeader, plan: MySqlOwnedRepositoryOperationPlan,
      at: LocalDateTime, reserve: Boolean, stage: MySqlNormalRecallStage, scope: MySqlRecoveryCanonicalBytes,
      seed: MySqlRecoveryCanonicalBytes, root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val c = header.context
    val r = header.request
    root.reserveRetainedMemory(32768L, "recall original operation field map")
    row(Operation, Map(
      "binding_uuid" -> uuid(c.bindingUuid), "operation_uuid" -> uuid(if reserve then c.reserveOperationUuid else c.tag.operationId),
      "workflow_uuid" -> uuid(c.workflowUuid), "step" -> text(if reserve then "RESERVE" else stage.step),
      "operation_state" -> text(if reserve then "COMMITTED" else "RESERVED"),
      "pod_uuid" -> uuid(r.podUuid), "attempt_uuid" -> uuid(r.attemptUuid), "footprint_uuid" -> uuid(r.footprintUuid),
      "source_unit_uuid" -> Null, "forest_uuid" -> Null, "boot_uuid" -> uuid(c.tag.bootId),
      "boot_generation" -> signed(c.tag.bootGeneration), "connection_generation_uuid" -> uuid(c.barrierGenerationUuid),
      "connection_id" -> signed(c.actualConnectionId), "fence_epoch" -> signed(c.writerEpoch),
      "request_identity_bytes" -> plan.requestIdentity.canonical.binaryCell, "request_identity_sha256" -> plan.requestIdentity.canonical.sha256Cell,
      "expected_domain_version" -> signed(r.criticalBase.stateVersion), "expected_locator_revision" -> signed(header.locatorRevision),
      "expected_source_revision" -> Null, "context_encoding" -> text("FP_RECOVERY_OPERATION_BE_V1"),
      "original_context_bytes" -> seed.binaryCell, "original_context_sha256" -> seed.sha256Cell,
      "scope_bytes" -> scope.binaryCell, "scope_sha256" -> scope.sha256Cell,
      "preimage_bytes" -> seed.binaryCell, "preimage_sha256" -> seed.sha256Cell,
      "target_bytes" -> seed.binaryCell, "target_sha256" -> seed.sha256Cell,
      "reserve_operation_uuid" -> (if reserve then Null else uuid(c.reserveOperationUuid)),
      "previous_step_operation_uuid" -> header.previousOperation.map(uuid).getOrElse(Null), "settled_by_operation_uuid" -> Null,
      "commit_witness_bytes" -> (if reserve then seed.binaryCell else Null),
      "commit_witness_sha256" -> (if reserve then seed.sha256Cell else Null),
      "original_diagnostic_identity" -> Null, "original_diagnostic_sha256" -> Null,
      "planned_at" -> MySqlFormat3Cell.DateTime(at), "fixed_lease_until" -> MySqlFormat3Cell.DateTime(c.fixedLeaseUntil),
      "result_at" -> (if reserve then MySqlFormat3Cell.DateTime(at) else Null), "operation_fingerprint" -> seed.sha256Cell), root)

  private def initialReservation(before: Image, header: MySqlNormalRecallHeader,
      root: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell], Boolean) =
    val request = header.request
    val context = header.context
    root.reserveRetainedMemory(16384L, "recall exact POD reservation")
    val key = MySqlRecoveryResourceKey.pod(request.podUuid, root.profile)
    val canonical = MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", root.profile)(key.writeCanonicalTo)
    val existing = before.relation(Reservation).rows.filter(r => value(r, Reservation, "binding_uuid") == uuid(context.bindingUuid) &&
      value(r, Reservation, "resource_pod_uuid") == uuid(request.podUuid))
    if existing.size > 1 then fail("duplicate POD reservation")
    existing.headOption match
      case Some(old) =>
        if value(old, Reservation, "resource_kind") != text("POD") || value(old, Reservation, "resource_unit_uuid") != Null ||
          value(old, Reservation, "resource_key") != canonical.binaryCell || value(old, Reservation, "resource_key_hash") != canonical.sha256Cell ||
          value(old, Reservation, "reservation_state") != text("RELEASED") then fail("original POD reservation is held or has another key")
        (update(old, Reservation, Map("reservation_revision" -> signed(Math.addExact(number(old, Reservation, "reservation_revision"), 1L)),
          "workflow_uuid" -> uuid(context.workflowUuid), "operation_uuid" -> uuid(context.tag.operationId),
          "reservation_state" -> text("HELD"), "last_mutation_operation_uuid" -> uuid(context.reserveOperationUuid)), root), true)
      case None =>
        (row(Reservation, Map("binding_uuid" -> uuid(context.bindingUuid), "resource_kind" -> text("POD"),
          "resource_key_hash" -> canonical.sha256Cell, "resource_key" -> canonical.binaryCell, "reservation_revision" -> signed(0),
          "workflow_uuid" -> uuid(context.workflowUuid), "operation_uuid" -> uuid(context.tag.operationId), "reservation_state" -> text("HELD"),
          "last_mutation_operation_uuid" -> uuid(context.reserveOperationUuid), "resource_pod_uuid" -> uuid(request.podUuid), "resource_unit_uuid" -> Null), root), false)

  private def requireBase(before: Image, request: MySqlNormalRecallRequest,
      context: MySqlRepositoryOperationContextIdentity): Vector[MySqlFormat3Cell] =
    val domain = one(before, Domain, r => value(r, Domain, "uuid") == uuid(request.podUuid))
    val base = request.criticalBase
    val expected = Map("binding_uuid" -> uuid(context.bindingUuid), "owner_uuid" -> uuid(base.ownerUuid.get),
      "state_version" -> signed(base.stateVersion), "is_active" -> signed(if base.isActive then 1 else 0), "operating_mode" -> text("NORMAL"),
      "world_name" -> text(base.worldName), "pos_x" -> floating(base.posX), "pos_y" -> floating(base.posY),
      "pos_z" -> floating(base.posZ), "yaw" -> floating(base.yaw.toDouble), "pitch" -> floating(base.pitch.toDouble),
      "dimension_id" -> signed(base.dimensionId.get), "last_updated" -> signed(base.lastUpdated))
    if !expected.forall((name, cell) => value(domain, Domain, name) == cell) then fail("original committed Normal domain differs")
    val locator = one(before, Locator, r => value(r, Locator, "pod_uuid") == uuid(request.podUuid))
    val locatorExpected = Map("binding_uuid" -> uuid(context.bindingUuid), "current_attempt_uuid" -> uuid(request.attemptUuid),
      "current_footprint_uuid" -> uuid(request.footprintUuid),
      "locator_state" -> text(if base.isActive then "ROW_ACTIVE" else "ROW_INACTIVE_PENDING"),
      "domain_version" -> signed(base.stateVersion), "target_dimension_id" -> signed(base.dimensionId.get))
    if !locatorExpected.forall((name, cell) => value(locator, Locator, name) == cell) then fail("original active locator differs")
    val chunks = before.relation(Chunk).rows.filter(r => value(r, Chunk, "binding_uuid") == uuid(context.bindingUuid) &&
      value(r, Chunk, "pod_uuid") == uuid(request.podUuid) && value(r, Chunk, "attempt_uuid") == uuid(request.attemptUuid) &&
      value(r, Chunk, "footprint_uuid") == uuid(request.footprintUuid))
    if chunks.size != 25 || !request.originalFootprint.forall(c => chunks.count(r =>
      value(r, Chunk, "dimension_id") == signed(c.dimensionId) && value(r, Chunk, "chunk_x") == signed(c.chunkX) &&
      value(r, Chunk, "chunk_z") == signed(c.chunkZ)) == 1) then fail("original exact twenty-five footprint differs")
    val writer = before.relation("fp_runtime_writer").rows
    if writer.size != 1 || value(writer.head, "fp_runtime_writer", "binding_uuid") != uuid(context.bindingUuid) ||
      value(writer.head, "fp_runtime_writer", "owner_boot_uuid") != uuid(context.tag.bootId) ||
      value(writer.head, "fp_runtime_writer", "writer_epoch") != signed(context.writerEpoch) ||
      value(writer.head, "fp_runtime_writer", "lease_until") != MySqlFormat3Cell.DateTime(context.fixedLeaseUntil) then fail("original fixed writer differs")
    locator

  private def number(row: Vector[MySqlFormat3Cell], table: String, column: String): Long = value(row, table, column) match
    case MySqlFormat3Cell.SignedInteger(n) if n >= 0L => n
    case _ => fail("nonnegative exact revision required")
  private def fail(reason: String): Nothing = throw new SQLDataException("normal recall materialization: " + reason)

/** 本次原 O 的进程内责任；持久 P/C/P2/G 状态仍以实际 full20 为准。 */
private[mysql] enum MySqlNormalRecallWorkflowPhase:
  case PreparedForReserve, ReserveCommitUnknown, ReserveCommittedAwaitingReadback
  case ReserveCommittedReadbackUnknown, ReserveCommittedExecutePending, ExecuteFailedAfterReserve
  case ExecuteCommitUnknown, ExecuteCommittedAwaitingReadback, ExecuteCommittedReadbackUnknown

private[mysql] final class MySqlNormalRecallWorkflowBlockedException(
    val phase: MySqlNormalRecallWorkflowPhase, cause: Throwable)
    extends java.sql.SQLException("normal recall original operation remains responsible at " + phase, cause)

private[mysql] final class MySqlNormalRecallWorkflowResponsibility private[mysql] (
    val commandScope: MySqlRepositoryCommandScope, val operation: MySqlRepositoryOperation,
    val materialization: MySqlNormalRecallMaterialization, val admission: MySqlBusinessAdmission,
    val phase: MySqlNormalRecallWorkflowPhase, val blockedFailure: Option[MySqlNormalRecallWorkflowBlockedException]):
  require(commandScope != null && operation != null && materialization != null && admission != null &&
    phase != null && blockedFailure != null && !blockedFailure.exists(_ == null) &&
    commandScope.hasOperationPlan() && (commandScope.operationPlan() eq materialization.plan) &&
    materialization.exactOperation == operation && (admission.token.scope eq commandScope.exactAuditScope()),
    "exact normal recall scope/plan/materialization/admission required")
  require(operation.isInstanceOf[MySqlRepositoryOperation.PrepareNormalRecall] ||
    operation.isInstanceOf[MySqlRepositoryOperation.CheckpointNormalRecall], "dedicated recall operation required")
  require(blockedFailure.nonEmpty == (phase != MySqlNormalRecallWorkflowPhase.PreparedForReserve),
    "unsettled recall phase must retain its explicit original obligation")
  def sameWorkflow(scope: MySqlRepositoryCommandScope, op: MySqlRepositoryOperation,
      materialized: MySqlNormalRecallMaterialization): Boolean =
    (scope eq commandScope) && op == operation && (materialized eq materialization)

private[mysql] object MySqlNormalRecallWorkflowResponsibility:
  def issue(scope: MySqlRepositoryCommandScope, operation: MySqlRepositoryOperation,
      materialization: MySqlNormalRecallMaterialization, admission: MySqlBusinessAdmission,
      phase: MySqlNormalRecallWorkflowPhase, cause: Option[Throwable]): MySqlNormalRecallWorkflowResponsibility =
    require(cause != null && !cause.exists(_ == null), "original recall cause option required")
    val failure = phase match
      case MySqlNormalRecallWorkflowPhase.PreparedForReserve =>
        require(cause.isEmpty, "new recall phase cannot hide a failure")
        None
      case MySqlNormalRecallWorkflowPhase.ReserveCommittedAwaitingReadback |
          MySqlNormalRecallWorkflowPhase.ExecuteCommittedAwaitingReadback |
          MySqlNormalRecallWorkflowPhase.ReserveCommittedExecutePending =>
        require(cause.isEmpty, "acknowledged obligation does not erase an existing failure")
        Some(new MySqlNormalRecallWorkflowBlockedException(phase,
          new IllegalStateException("original recall commit/readback/execute responsibility is still pending")))
      case _ => Some(new MySqlNormalRecallWorkflowBlockedException(phase,
        cause.getOrElse(throw new IllegalArgumentException("original recall unknown/failure cause is required"))))
    new MySqlNormalRecallWorkflowResponsibility(scope, operation, materialization, admission, phase, failure)

private[mysql] object MySqlNormalRecallExecuteFailureBoundary:
  def shouldRecordKnownFailure(retainedExecutePending: Boolean, primary: Throwable): Boolean =
    if !retainedExecutePending || primary == null || primary.isInstanceOf[MySqlTransactionCleanupUncertainException] then false
    else primary match
      case blocked: MySqlNormalRecallWorkflowBlockedException =>
        blocked.phase != MySqlNormalRecallWorkflowPhase.ReserveCommitUnknown &&
          blocked.phase != MySqlNormalRecallWorkflowPhase.ReserveCommittedReadbackUnknown &&
          blocked.phase != MySqlNormalRecallWorkflowPhase.ExecuteCommitUnknown &&
          blocked.phase != MySqlNormalRecallWorkflowPhase.ExecuteCommittedReadbackUnknown
      case _ => true
  def afterReserve[A](knownFailureStillRecordable: Throwable => Boolean)(executePhase: => A)(recordKnownFailure: Throwable => Throwable): A =
    if knownFailureStillRecordable == null then
      throw new IllegalArgumentException("post-reserve execute failure classification is required")
    try executePhase
    catch
      case primary: Throwable =>
        val recordable =
          try knownFailureStillRecordable(primary)
          catch
            case checkFailure: Throwable =>
              if checkFailure ne primary then primary.addSuppressed(checkFailure)
              throw primary
        if !recordable then throw primary
        val recorded =
          try recordKnownFailure(primary)
          catch
            case recordFailure: Throwable =>
              if recordFailure ne primary then primary.addSuppressed(recordFailure)
              throw primary
        if recorded == null then
          val missingRecord = new IllegalStateException("post-reserve execute failure recorder returned no retained failure")
          if missingRecord ne primary then primary.addSuppressed(missingRecord)
          throw primary
        throw recorded
