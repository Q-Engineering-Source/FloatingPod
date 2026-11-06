package com.wi.floatingpod.storage.mysql

/** 固定 producer 槽，不是独立registry；一个草稿只持同一mode两阶段的实际返回产物。 */
private[mysql] enum MySqlModeWitnessProducerStage:
  case Before, Scope, Seed, InitialReserveRow, InitialExecuteRow, InitialReservationRow
  case ReservePreimage, ReserveRow, ReserveScaffold, ReservePhase, ReserveWitness
  case ReserveCommittedRow, ReserveFinalRow, ReserveWriter, ReserveTargetBase
  case ExecuteScaffold, ExecutePhase, ExecuteReservedRow, ReserveTarget, ReserveRaw
  case ExecuteWitness, ExecuteCommittedDraft, ExecuteCommittedRow, ExecuteTerminalRow
  case ExecuteWriter, ExecuteTarget, Effects, Materialization
  case ReserveScaffoldWriter, ExecuteReleasedReservation, ExecuteScaffoldCommittedRow
  case ExecuteScaffoldWriter, ExecuteChangedDomain, ExecuteChangedLocator
  case ReserveOperationRows, ReserveReservationRows, ExecuteOperationRows
  case ExecuteDomainRows, ExecuteLocatorRows, ExecuteReservationRows, ExecuteOrderedOperationRows
  case ReserveScaffoldWriterBeforeDigest, ReserveScaffoldBeforeDigest, ReserveWriterBeforeDigest
  case ReserveTargetBaseBeforeDigest, ExecuteScaffoldWriterBeforeDigest, ExecuteScaffoldBeforeDigest
  case ReserveTargetBeforeDigest, ExecuteWriterBeforeDigest, ExecuteTargetBeforeDigest

/** 同 retained entry 的 planning DATA 草稿；不签 native/SQL 资格，失败必须保原根。 */
private[mysql] final class MySqlModeOriginalWitnessDraft private[mysql] (
    plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
    context: MySqlRepositoryOperationContextIdentity, val originalRoot: MySqlFormat3ReadBudget
):
  require(plan != null && operation != null && context != null && originalRoot != null &&
    context.tag == plan.tag && context.workflowUuid == plan.workflowUuid &&
    context.reserveOperationUuid == plan.reserveOperationUuid &&
    (context.exactLimitProfile eq originalRoot.profile) && (plan.exactLimitProfile eq originalRoot.profile),
    "complete same-plan/context/root DATA draft required")
  private val owner = Thread.currentThread()
  private var retainedPlan = plan
  private var retainedOperation = operation
  private var retainedContext = context
  private var materialization: MySqlModeWorkflowMaterialization = null
  private var images = Vector.empty[MySqlFormat3CanonicalPayload]
  private var phases = Vector.empty[MySqlModePhaseCanonical]
  private var contextBytes: MySqlRecoveryCanonicalBytes = null
  // 一次预留全部固定槽，登记返回产物不再分配collection或执行callback。
  originalRoot.reserveRetainedMemory(8192L, "mode fixed typed producer partial-reference slots")
  // 固定两phase builder路径的有界槽；保所有已返回新cell/row直到exactdrop。
  // 四倍完整20-table列宽覆盖三次O rowbuilder及R/domain/locator/writer更新；不按payload计重复费用。
  private val partialCapacity =
    var columns = 0
    var index = 0
    while index < MySqlFormat3RelationShapes.canonicalOrder.size do
      columns = Math.addExact(columns, MySqlFormat3RelationShapes.canonicalOrder(index).columns.size)
      index += 1
    Math.multiplyExact(columns, 4)
  originalRoot.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(partialCapacity.toLong, 32L)),
    "mode fixed partial row/cell reference arrays before allocation")

  private val producerSlotCount = MySqlModeWitnessProducerStage.values.length
  private val producerCanonicals = new Array[MySqlRecoveryCanonicalBytes](producerSlotCount)
  private val producerSnapshots = new Array[MySqlFormat3FullSnapshot](producerSlotCount)
  private var deploymentSnapshots: Array[MySqlScriptSchemaSnapshot] = null
  private val producerPhases = new Array[MySqlModePhaseCanonical](producerSlotCount)
  private val producerRows = new Array[Vector[MySqlFormat3Cell]](producerSlotCount)
  private val producerRaw = new Array[MySqlRecoveryOperationRawEnvelope](producerSlotCount)
  private val producerRelationRows = new Array[Vector[Vector[MySqlFormat3Cell]]](producerSlotCount)
  private var producerEffects: Vector[MySqlModeOperationEffect] = Vector.empty
  private enum ProducerSource:
    case LegacyFull20(before: MySqlFormat3FullSnapshot)
    case DeploymentReceiptFull26(before: MySqlScriptSchemaSnapshot)
  private var producerSource: ProducerSource = null
  private var lastProducerStage: MySqlModeWitnessProducerStage = null
  private var reserveCompletedCharge = -1L
  private var executeScaffoldCharge = -1L
  private var executeScaffoldPeak = -1L
  private var censusPartial: MySqlModeEnvelopeV2Census.Partial = null
  private[mysql] def retainCensus(value: MySqlModeEnvelopeV2Census.Partial): Unit =
    requireOwner()
    requireLegacyEncoding()
    require(censusPartial == null && value != null && (value.root eq originalRoot) && (value.draft eq this))
    censusPartial = value
  private[mysql] def retainedCensus: MySqlModeEnvelopeV2Census.Partial = censusPartial
  private[mysql] def retainedPhaseAt(index: Int): MySqlModePhaseCanonical = { requireOwner(); phases(index) }
  private[mysql] def retainedContextCanonical: MySqlRecoveryCanonicalBytes = { requireOwner(); contextBytes }
  private var partialEncodingBinary: MySqlFormat3Binary = null
  private var reserveScaffoldWriterBeforeDigestCharge = -1L
  private var partialRows: Array[Vector[MySqlFormat3Cell]] = new Array[Vector[MySqlFormat3Cell]](partialCapacity)
  private var partialCells: Array[MySqlFormat3Cell] = new Array[MySqlFormat3Cell](partialCapacity)
  private var partialRowCount = 0
  private var partialCellCount = 0
  private var firstProducedUuidCell: MySqlFormat3Cell = null
  private var firstProducedUuidCharge = -1L

  private[mysql] def requirePartialRowSlot(): Unit =
    requireOwner()
    require(partialRows != null && partialRowCount < partialCapacity, "bounded partial row slot before allocation")
  private[mysql] def requirePartialCellSlot(): Unit =
    requireOwner()
    require(partialCells != null && partialCellCount < partialCapacity, "bounded partial cell slot before allocation")
  private[mysql] def retainPartialRow(value: Vector[MySqlFormat3Cell]): Unit =
    requireOwner()
    require(value != null && partialRows != null, "exact returned mode partial row")
    var index = 0
    while index < partialRowCount do
      if partialRows(index) eq value then return
      index += 1
    requirePartialRowSlot()
    partialRows(partialRowCount) = value
    partialRowCount += 1
  private[mysql] def retainPartialCell(value: MySqlFormat3Cell): Unit =
    requireOwner()
    require(value != null && partialCells != null, "exact returned mode partial cell")
    var index = 0
    while index < partialCellCount do
      if partialCells(index) eq value then return
      index += 1
    requirePartialCellSlot()
    partialCells(partialCellCount) = value
    partialCellCount += 1
  private[mysql] def retainProducedUuidCell(value: MySqlFormat3Cell): Unit =
    retainPartialCell(value)
    if firstProducedUuidCell == null then
      firstProducedUuidCell = value
      firstProducedUuidCharge = originalRoot.accountedMemoryBytes
  private[mysql] def firstUuidCellAccountedBytes: Long =
    requireOwner()
    firstProducedUuidCharge
  private[mysql] def firstUuidCellReference: MySqlFormat3Cell =
    requireOwner()
    firstProducedUuidCell
  private[mysql] def partialRowReferenceAt(index: Int): Vector[MySqlFormat3Cell] =
    requireOwner()
    require(index >= 0 && index < partialRowCount, "actual retained partial row index")
    partialRows(index)
  private[mysql] def partialRowReferenceCount: Int = partialRowCount
  private[mysql] def partialCellReferenceCount: Int = partialCellCount

  private var rawConstruction: MySqlModeRawEnvelopeConstruction = null
  private var failure: Throwable = null
  private var attempted = false
  private var dropped = false
  private[mysql] var encoded: MySqlModeWitnessCodec.Encoded = null
  val tag = plan.tag
  // ctor参数不成为fields；所有带payload的可达引用都在下面明确撤销。
  private[mysql] def originalPlan: MySqlOwnedRepositoryOperationPlan = retainedPlan
  private[mysql] def originalOperation: MySqlRepositoryOperation = retainedOperation
  private[mysql] def originalContext: MySqlRepositoryOperationContextIdentity = retainedContext
  private[mysql] def requireOwner(): Unit =
    require(Thread.currentThread() eq owner, "exact planning witness owner required")
    require(!dropped && originalRoot != null && !originalRoot.isClosed && retainedPlan != null &&
      (originalRoot.profile eq retainedPlan.exactLimitProfile), "original witness root/profile must remain open")
  private[mysql] def retainRawConstruction(value: MySqlModeRawEnvelopeConstruction): Unit =
    requireOwner()
    require(rawConstruction == null && value != null && (value.originalRoot eq originalRoot), "exact once original RAW construction")
    rawConstruction = value
  private[mysql] def retainedRawConstruction: MySqlModeRawEnvelopeConstruction =
    requireOwner()
    rawConstruction
  private[mysql] def retainedProducerRawCount: Int =
    requireOwner()
    if rawConstruction == null then 0 else rawConstruction.retainedCount
  private[mysql] def bind(value: MySqlModeWorkflowMaterialization): Unit =
    requireOwner()
    require(materialization == null && value != null && (value.exactPlan eq retainedPlan) &&
      value.exactOperation == retainedOperation && (value.completePreimage match
        case legacy: MySqlFormat3FullSnapshot => legacy.allocationBudget.sharesAllocationLedger(originalRoot)
        case deployment: MySqlScriptSchemaSnapshot =>
          MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, originalRoot)
          true),
      "actual original mode materialization required once")
    materialization = value
    if producerSource != null then lastProducerStage = MySqlModeWitnessProducerStage.Materialization
  private[mysql] def requireProducerInputs(actualPlan: MySqlOwnedRepositoryOperationPlan,
      actualOperation: MySqlRepositoryOperation, actualContext: MySqlRepositoryOperationContextIdentity,
      before: MySqlFormat3FullSnapshot): Unit =
    requireOwner()
    require((actualPlan eq retainedPlan) && actualOperation == retainedOperation &&
      (actualContext eq retainedContext) && before != null &&
      before.allocationBudget.sharesAllocationLedger(originalRoot) && producerSource == null,
      "exact registered original mode producer inputs required once")
    producerSource = ProducerSource.LegacyFull20(before)
    lastProducerStage = MySqlModeWitnessProducerStage.Before

  private[mysql] def requireProducerInputs(actualPlan: MySqlOwnedRepositoryOperationPlan,
      actualOperation: MySqlRepositoryOperation, actualContext: MySqlRepositoryOperationContextIdentity,
      before: MySqlScriptSchemaSnapshot): Unit =
    requireOwner()
    require((actualPlan eq retainedPlan) && actualOperation == retainedOperation &&
      (actualContext eq retainedContext) && before != null && producerSource == null,
      "exact registered original Full26 mode producer inputs required once")
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,originalRoot)
    // 先登记原前像，随后即使槽数组分配失败也保留真实来源/费用责任。
    producerSource = ProducerSource.DeploymentReceiptFull26(before)
    lastProducerStage = MySqlModeWitnessProducerStage.Before
    originalRoot.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(producerSlotCount.toLong,8L)),
      "mode Full26 fixed producer snapshot reference array before allocation")
    deploymentSnapshots = new Array[MySqlScriptSchemaSnapshot](producerSlotCount)
  private def producerSlot(stage: MySqlModeWitnessProducerStage): Int =
    requireOwner()
    require(stage != null && producerSource != null, "registered mode producer stage required")
    lastProducerStage = stage
    stage.ordinal
  private[mysql] def retainProducerCanonical(stage: MySqlModeWitnessProducerStage,
      value: MySqlRecoveryCanonicalBytes): Unit =
    val index = producerSlot(stage)
    require(value != null && producerCanonicals(index) == null, "canonical producer stage is once")
    producerCanonicals(index) = value
  private[mysql] def retainProducerSnapshot(stage: MySqlModeWitnessProducerStage,
      value: MySqlFormat3FullSnapshot): Unit =
    requireLegacyEncoding()
    val index = producerSlot(stage)
    require(value != null && producerSnapshots(index) == null &&
      value.allocationBudget.sharesAllocationLedger(originalRoot), "same-ledger producer snapshot stage is once")
    producerSnapshots(index) = value
    if stage == MySqlModeWitnessProducerStage.ReserveScaffoldWriterBeforeDigest then
      reserveScaffoldWriterBeforeDigestCharge = originalRoot.accountedMemoryBytes
    if stage == MySqlModeWitnessProducerStage.ReserveTargetBase then
      reserveCompletedCharge = originalRoot.accountedMemoryBytes
    if stage == MySqlModeWitnessProducerStage.ExecuteScaffold then
      executeScaffoldCharge = originalRoot.accountedMemoryBytes
      executeScaffoldPeak = originalRoot.successfulReserveHighWaterBytes

  private[mysql] def retainProducerSnapshot(stage: MySqlModeWitnessProducerStage,
      value: MySqlScriptSchemaSnapshot): Unit =
    val index = producerSlot(stage)
    val before = producerSource match
      case ProducerSource.DeploymentReceiptFull26(image) => image
      case _ => throw new IllegalStateException("original Full26 mode producer source required")
    require(value != null && deploymentSnapshots != null && deploymentSnapshots(index) == null,
      "Full26 producer snapshot stage is once")
    MySqlFormat3QueryProjection.requireDeploymentSource(value,before.inputs,originalRoot)
    deploymentSnapshots(index) = value
    if stage == MySqlModeWitnessProducerStage.ReserveScaffoldWriterBeforeDigest then
      reserveScaffoldWriterBeforeDigestCharge = originalRoot.accountedMemoryBytes
    if stage == MySqlModeWitnessProducerStage.ReserveTargetBase then
      reserveCompletedCharge = originalRoot.accountedMemoryBytes
    if stage == MySqlModeWitnessProducerStage.ExecuteScaffold then
      executeScaffoldCharge = originalRoot.accountedMemoryBytes
      executeScaffoldPeak = originalRoot.successfulReserveHighWaterBytes
  private[mysql] def retainProducerPhase(stage: MySqlModeWitnessProducerStage,
      value: MySqlModePhaseCanonical): Unit =
    val index = producerSlot(stage)
    require(value != null && producerPhases(index) == null, "phase producer stage is once")
    producerPhases(index) = value
  private[mysql] def retainProducerRow(stage: MySqlModeWitnessProducerStage,
      value: Vector[MySqlFormat3Cell]): Unit =
    val index = producerSlot(stage)
    require(value != null && producerRows(index) == null, "typed row producer stage is once")
    producerRows(index) = value
  private[mysql] def retainProducerRelationRows(stage: MySqlModeWitnessProducerStage,
      value: Vector[Vector[MySqlFormat3Cell]]): Unit =
    val index = producerSlot(stage)
    require(value != null && producerRelationRows(index) == null, "typed relation-row producer stage is once")
    producerRelationRows(index) = value
  private[mysql] def retainProducerRaw(stage: MySqlModeWitnessProducerStage,
      value: MySqlRecoveryOperationRawEnvelope): Unit =
    val index = producerSlot(stage)
    require(value != null && producerRaw(index) == null, "RAW producer stage is once")
    producerRaw(index) = value
  private[mysql] def retainProducerEffects(value: Vector[MySqlModeOperationEffect]): Unit =
    producerSlot(MySqlModeWitnessProducerStage.Effects)
    require(value != null && producerEffects.isEmpty, "complete ordered producer effects required once")
    producerEffects = value
  private[mysql] def producerSnapshot(stage: MySqlModeWitnessProducerStage): MySqlFormat3FullSnapshot =
    requireOwner()
    requireLegacyEncoding()
    producerSnapshots(stage.ordinal)
  private[mysql] def producerDeploymentReceiptSnapshot(stage: MySqlModeWitnessProducerStage): MySqlScriptSchemaSnapshot =
    requireOwner()
    producerSource match
      case ProducerSource.DeploymentReceiptFull26(_) =>
        require(stage != null && deploymentSnapshots != null,"registered Full26 producer snapshot slots required")
        deploymentSnapshots(stage.ordinal)
      case _ => throw new IllegalStateException("original Full26 mode producer source required")
  private[mysql] def producerCanonical(stage: MySqlModeWitnessProducerStage): MySqlRecoveryCanonicalBytes =
    requireOwner()
    producerCanonicals(stage.ordinal)
  private[mysql] def producerPhase(stage: MySqlModeWitnessProducerStage): MySqlModePhaseCanonical =
    requireOwner()
    producerPhases(stage.ordinal)
  private[mysql] def producerStage: MySqlModeWitnessProducerStage =
    requireOwner()
    lastProducerStage
  private[mysql] def reserveScaffoldWriterBeforeDigestAccountedBytes: Long =
    requireOwner()
    reserveScaffoldWriterBeforeDigestCharge
  private[mysql] def executeScaffoldAccountedBytes: Long =
    requireOwner()
    executeScaffoldCharge
  private[mysql] def executeScaffoldSuccessfulReservePeakBytes: Long =
    requireOwner()
    executeScaffoldPeak
  private[mysql] def reserveCompletedAccountedBytes: Long =
    requireOwner()
    reserveCompletedCharge
  private[mysql] def value: MySqlModeWorkflowMaterialization =
    requireOwner()
    require(materialization != null, "original mode materialization has not completed")
    materialization
  private[mysql] def beginEncoding(): Unit =
    requireOwner()
    requireLegacyEncoding()
    require(!attempted && failure == null, "mode witness encoding is one-shot")
    attempted = true
  private[mysql] def retainImage(value: MySqlFormat3CanonicalPayload): Unit =
    requireOwner()
    requireLegacyEncoding()
    originalRoot.reserveRetainedMemory(64L, "mode draft canonical image reference")
    images = images :+ value
  private[mysql] def retainPhase(value: MySqlModePhaseCanonical): Unit =
    requireOwner()
    originalRoot.reserveRetainedMemory(64L, "mode draft phase reference")
    phases = phases :+ value
  private[mysql] def retainEncodingBinary(value: MySqlFormat3Binary): Unit =
    requireOwner()
    require(value != null && partialEncodingBinary == null, "one retained partial envelope binary")
    partialEncodingBinary = value
  private[mysql] def retainedEncodingBinary: MySqlFormat3Binary =
    requireOwner()
    partialEncodingBinary
  private[mysql] def retainContextBytes(value: MySqlRecoveryCanonicalBytes): Unit =
    requireOwner()
    contextBytes = value
  private[mysql] def recordFailure(cause: Throwable): Unit =
    require(Thread.currentThread() eq owner, "exact witness failure owner required")
    require(cause != null, "original failure required")
    if failure == null then failure = cause
    else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure, cause)
  private[mysql] def retainedFailure: Option[Throwable] =
    require(Thread.currentThread() eq owner, "exact witness failure owner required")
    Option(failure)
  private[mysql] def requireDroppable(): Unit =
    requireOwner()
    if failure != null then throw new IllegalStateException("failed mode witness draft responsibility remains retained", failure)
  /** 仅原registry exactdrop入口调用；断引用先于任何root/child credit。 */
  private[mysql] def dropForExactProjectionRelease(): Unit =
    requireDroppable()
    encoded = null
    partialEncodingBinary = null
    censusPartial = null
    if materialization != null then materialization.originalWitnessDraft = null
    if partialRows != null then
      var index = 0
      while index < partialRows.length do
        partialRows(index) = null
        partialCells(index) = null
        index += 1
    partialRows = null
    partialCells = null
    partialRowCount = 0
    partialCellCount = 0
    firstProducedUuidCell = null
    if rawConstruction != null then rawConstruction.dropReferences()
    rawConstruction = null
    materialization = null
    images = Vector.empty
    phases = Vector.empty
    contextBytes = null
    retainedPlan = null
    retainedOperation = null
    retainedContext = null
    var slot = 0
    while slot < producerCanonicals.length do
      producerCanonicals(slot) = null
      producerSnapshots(slot) = null
      producerPhases(slot) = null
      producerRows(slot) = null
      producerRaw(slot) = null
      producerRelationRows(slot) = null
      slot += 1
    producerEffects = Vector.empty
    if deploymentSnapshots != null then
      var index = 0
      while index < deploymentSnapshots.length do
        deploymentSnapshots(index) = null
        index += 1
    deploymentSnapshots = null
    producerSource = null
    lastProducerStage = null
    dropped = true
  private[mysql] def retainedProducerReferenceCount: Int =
    require(Thread.currentThread() eq owner, "exact witness reference observation owner required")
    var count = if producerSource == null then 0 else 1
    var slot = 0
    while slot < producerSlotCount do
      if producerCanonicals(slot) != null then count += 1
      if producerSnapshots(slot) != null then count += 1
      if deploymentSnapshots != null && deploymentSnapshots(slot) != null then count += 1
      if producerPhases(slot) != null then count += 1
      if producerRows(slot) != null then count += 1
      if producerRaw(slot) != null then count += 1
      if producerRelationRows(slot) != null then count += 1
      slot += 1
    if producerEffects.nonEmpty then count += 1
    count
  private[mysql] def isDropped: Boolean = dropped

  private def requireLegacyEncoding(): Unit = producerSource match
    case ProducerSource.DeploymentReceiptFull26(_) =>
      throw new IllegalStateException("Full26 mode DATA cannot enter the legacy Full20 witness/census encoding")
    case _ => ()
