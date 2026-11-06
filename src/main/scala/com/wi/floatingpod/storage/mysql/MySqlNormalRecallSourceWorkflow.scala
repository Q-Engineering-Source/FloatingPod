package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.util.UUID

import MySqlModeOperationEffectCodec.{Image, allocationBudget, value}

/** Normal Recall source SQL delta planning. DATA here never issues a permit or native completion. */
private[mysql] object MySqlNormalRecallSourceWorkflow:
  private val OperationTable = "fp_recovery_operation"
  private val ReservationTable = "fp_recovery_reservation"
  private val WriterTable = "fp_runtime_writer"
  private val OperationContextEncoding = "FP_RECOVERY_OPERATION_BE_V1"
  private val SourceUnitTable = "physical_source_unit"
  private val SourceForestTable = "physical_source_forest"
  private val SourceWriteTable = "physical_source_write"
  private val SourceObligationTable = "physical_source_obligation"
  private val SourceObligationPurposes = Set("OLD_HOLD", "TARGET_STAGED", "CURRENT_HOLD", "OLD_RETIRE", "UNDO")

  final class RelationDelta private[MySqlNormalRecallSourceWorkflow] (
      val table: String,
      val beforeRows: Vector[Vector[MySqlFormat3Cell]],
      val targetRows: Vector[Vector[MySqlFormat3Cell]])

  /** Full exact relation delta for one source-write operation; this is planning DATA only. */
  final class SourceWritePlanData private[MySqlNormalRecallSourceWorkflow] (
      val step: String,
      val operationRow: Vector[MySqlFormat3Cell],
      val sourceUnitRows: Vector[Vector[MySqlFormat3Cell]],
      val sourceForestRows: Vector[Vector[MySqlFormat3Cell]],
      val writeBefore: Option[Vector[MySqlFormat3Cell]],
      val writeTarget: Vector[MySqlFormat3Cell],
      val carrierReservationBefore: Vector[MySqlFormat3Cell],
      val carrierReservationTarget: Vector[MySqlFormat3Cell],
      val sourceUnitReservationsBefore: Vector[Vector[MySqlFormat3Cell]],
      val sourceUnitReservationsTarget: Vector[Vector[MySqlFormat3Cell]],
      val obligationsBefore: Vector[Vector[MySqlFormat3Cell]],
      val obligationsTarget: Vector[Vector[MySqlFormat3Cell]],
      val deltas: Vector[RelationDelta],
      val sourceWrite: MySqlNormalRecallCanonical.DecodedSourceWrite,
      val sourceScope: MySqlNormalRecallCanonical.DecodedSourceWriteScope,
      val sourceRevisionData: SourceRevisionComparisonData)

  /** Exposes competing source revision facts without guessing which schema field the O CAS means. */
  final class SourceRevisionComparisonData private[MySqlNormalRecallSourceWorkflow] (
      val expectedSourceRevision: Option[Long],
      val unitRevisions: Vector[Long],
      val currentEpochs: Vector[Option[Long]])

  final class SourceChildParentClosureData private[MySqlNormalRecallSourceWorkflow] (
      val pod: UUID,
      val recallWorkflow: UUID,
      val prepare: UUID,
      val checkpoint: Option[UUID],
      val parentP2: UUID,
      val podStateVersion: Long,
      val p2Step: String,
      val p2State: String,
      val podReservationRevision: Long)

  final class SourceChildUnitCoverageData private[MySqlNormalRecallSourceWorkflow] (
      val binding: UUID,
      val unit: UUID,
      val worldIdentity: String,
      val saveIdentity: String,
      val loaderIdentity: String,
      val catalogGeneration: UUID,
      val readGeneration: Long,
      val unitRevision: Long,
      val state: String,
      val currentEpoch: Option[Long],
      val currentForest: Option[UUID])

  final class SourceChildCarrierCoverageData private[MySqlNormalRecallSourceWorkflow] (
      val carrier: MySqlBootSourceProtectionIndex.Carrier,
      val status: String,
      val localReasons: Vector[String])

  final class SourceChildFootprintChunkData private[MySqlNormalRecallSourceWorkflow] (
      val binding: UUID,
      val pod: UUID,
      val attempt: UUID,
      val footprint: UUID,
      val dimension: Int,
      val chunkX: Int,
      val chunkZ: Int)

  final class SourceChildUnresolvedData private[MySqlNormalRecallSourceWorkflow] (
      val code: String,
      val table: String)

  /** Main-facing closure values are copied primitives; the exact SQL image/history stay Fenced-private. */
  final class SourceChildReserveEffectClosureData private[MySqlNormalRecallSourceWorkflow] (
      val parent: SourceChildParentClosureData,
      val sourceUnits: Vector[SourceChildUnitCoverageData],
      val carriers: Vector[SourceChildCarrierCoverageData],
      val oldFootprintChunks: Vector[SourceChildFootprintChunkData],
      val unresolved: Vector[SourceChildUnresolvedData],
      private[mysql] val sourceProtectionIndex: MySqlBootSourceProtectionIndex):
    def globalUnresolved: Boolean = unresolved.nonEmpty
    def containsCarrier(locator: MySqlBootSourceProtectionIndex.Carrier): Boolean =
      locator != null && carriers.exists(_.carrier == locator)
    def containsChunk(dimension: Int, chunkX: Int, chunkZ: Int): Boolean =
      carriers.exists(value => value.carrier.chunk.dimension == dimension &&
        value.carrier.chunk.x == chunkX && value.carrier.chunk.z == chunkZ) ||
        oldFootprintChunks.exists(value => value.dimension == dimension && value.chunkX == chunkX &&
          value.chunkZ == chunkZ)

  /** H3 fresh Full26 planning DATA only; no SQL command, receipt, transaction or source permit is issued. */
  final class SourceChildReservePlanData private[MySqlNormalRecallSourceWorkflow] (
      val request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      val before: MySqlScriptSchemaSnapshot,
      val history: MySqlNormalRecallRecovery.ReceiptHistoryData,
      val effectClosure: SourceChildReserveEffectClosureData,
      private[MySqlNormalRecallSourceWorkflow] val parent: MySqlFencedTransaction.RestrictedSourceParent5,
      private[MySqlNormalRecallSourceWorkflow] val intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      private[MySqlNormalRecallSourceWorkflow] val root: MySqlFormat3ReadBudget,
      private[MySqlNormalRecallSourceWorkflow] val allocation: MySqlFencedTransaction.SourceReservePlanAllocation):
    private var precommitStage = 0
    private var precommitWitness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData = null
    private var expectedSnapshotAttemptRetention: AnyRef = null
    private var verifiedExpectedTargetEffect: MySqlNormalRecallCanonical.SourceChildReserveEffectData = null

    private[MySqlNormalRecallSourceWorkflow] def beginWitnessAttempt(): Unit = synchronized {
      require(precommitStage == 0, "SOURCE R0 precommit witness assembly is single-attempt per retained plan")
      precommitStage = 1
    }

    private[MySqlNormalRecallSourceWorkflow] def finishWitnessAttempt(
        witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
        dataBudget: MySqlFormat3ReadBudget): Unit = synchronized {
      require(precommitStage == 1, "SOURCE R0 witness attempt must remain on its original plan stage")
      require(witness != null && dataBudget != null && !dataBudget.isClosed &&
        (witness.root eq root) && (witness.dataBudget eq dataBudget) &&
        dataBudget.sharesAllocationLedger(root) && dataBudget.isCanonicalChildOf(root),
        "SOURCE R0 exact witness reference must retain this plan's original root and settled child")
      dataBudget.reserveRetainedMemory(8L,
        "SOURCE R0 exact A witness reference held by its original retained plan")
      precommitWitness = witness
      precommitStage = 2
    }

    private[MySqlNormalRecallSourceWorkflow] def requireExpectedSnapshotWitness(
        witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData): Unit = synchronized {
      require(precommitStage == 2 && witness != null && (precommitWitness eq witness),
        "SOURCE R0 expected snapshot requires the exact same A witness object retained by this plan")
    }

    private[MySqlNormalRecallSourceWorkflow] def beginExpectedSnapshotAttempt(
        witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData): Unit = synchronized {
      require(precommitStage == 2 && witness != null && (precommitWitness eq witness),
        "SOURCE R0 expected snapshot stage requires the exact same A witness object retained by this plan")
      precommitStage = 3
    }

    private[MySqlNormalRecallSourceWorkflow] def retainExpectedSnapshotRelations(
        relations: Vector[MySqlFormat3RelationSnapshot]): Unit = synchronized {
      require(precommitStage == 3 && relations != null && expectedSnapshotAttemptRetention == null,
        "SOURCE R0 expected snapshot attempt may retain only its one exact completed relation vector")
      expectedSnapshotAttemptRetention = relations
    }

    private[MySqlNormalRecallSourceWorkflow] def retainExpectedSnapshot(
        relations: Vector[MySqlFormat3RelationSnapshot], snapshot: MySqlScriptSchemaSnapshot,
        dataBudget: MySqlFormat3ReadBudget): Unit = synchronized {
      require(precommitStage == 3 && relations != null && snapshot != null && dataBudget != null &&
        (expectedSnapshotAttemptRetention eq relations) && (snapshot.allocationBudget eq dataBudget),
        "SOURCE R0 expected snapshot must replace its exact retained relation vector under the same child")
      expectedSnapshotAttemptRetention = snapshot
    }

    private[MySqlNormalRecallSourceWorkflow] def retainVerifiedExpectedTargetEffect(
        effect: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
        dataBudget: MySqlFormat3ReadBudget): Unit = synchronized {
      require(precommitStage == 3 && expectedSnapshotAttemptRetention.isInstanceOf[MySqlScriptSchemaSnapshot] &&
        effect != null && dataBudget != null && verifiedExpectedTargetEffect == null &&
        (effect.root eq root) && (effect.dataBudget eq dataBudget) &&
        (effect.request eq request) && effect.image == MySqlOperationEffectImage.Target,
        "SOURCE R0 verified expected TARGET effect must remain attached to this precommit plan and child")
      verifiedExpectedTargetEffect = effect
    }

    private[MySqlNormalRecallSourceWorkflow] def finishExpectedSnapshotAttempt(
        snapshot: MySqlScriptSchemaSnapshot): Unit = synchronized {
      require(precommitStage == 3 && snapshot != null && (expectedSnapshotAttemptRetention eq snapshot) &&
        verifiedExpectedTargetEffect != null,
        "SOURCE R0 expected snapshot attempt must finish with its exact plan-retained snapshot")
      precommitStage = 4
    }

  private def invalid(message: String): Nothing = throw new IllegalArgumentException(message)

  private def withSourceReserveScratch[A](root: MySqlFormat3ReadBudget, bytes: Long,
      context: String)(body: => A): A =
    require(root != null && !root.isClosed && bytes > 0L && context != null,
      "original-root bounded SOURCE R0 scratch reservation required")
    root.reserveTemporaryMemory(bytes, context + " BEFORE allocation")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try root.releaseTemporaryMemory(bytes, context + " cleanup")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  private def withSourceReserveScratch[A](root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, bytes: Long, context: String)(body: => A): A =
    require(root != null && !root.isClosed && dataBudget != null && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      dataBudget.isCanonicalChildOf(root) && bytes > 0L && context != null,
      "original-root validation and exact plan DATA child scratch reservation required")
    dataBudget.reserveTemporaryMemory(bytes, context + " BEFORE allocation")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(bytes, context + " cleanup")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  private def withHistoricalSourceReserveScratch[A](root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, bytes: Long, context: String)(body: => A): A =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && dataBudget != null &&
      !dataBudget.isClosed && (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)) && bytes > 0L && context != null,
      "historical SOURCE R0 scratch requires the original root and exact root/direct-child DATA owner")
    dataBudget.reserveTemporaryMemory(bytes, context + " BEFORE allocation")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(bytes, context + " cleanup")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  private def withSourceReserveRequestPrecharge[A](
      precharge: MySqlNormalRecallCanonical.SourceChildReserveRequestDataPrecharge)(body: => A): A =
    require(precharge != null, "one-use SOURCE R0 request retained-memory precharge required")
    try
      val result = body
      precharge.commit()
      result
    catch
      case cause: Throwable =>
        try precharge.releaseIfUncommitted()
        catch
          case cleanup: Throwable =>
            if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  private def withSourceReserveClosurePrecharge(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, bytes: Long,
      discardPartial: () => Unit)(body: => SourceChildReservePlanData): SourceChildReservePlanData =
    require(root != null && !root.isClosed && dataBudget != null && !dataBudget.isClosed &&
      dataBudget.sharesAllocationLedger(root) && dataBudget.isCanonicalChildOf(root) &&
      bytes > 0L && discardPartial != null,
      "original root, plan DATA child and retained source closure charge required")
    try
      val plan = body
      require(plan != null, "SOURCE R0 closure charge transfers only with a real retained plan DATA")
      plan
    catch
      case cause: Throwable =>
        try discardPartial()
        catch
          case cleanup: Throwable =>
            if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        try dataBudget.releaseRetainedMemory(bytes, "failed SOURCE R0 partial effect-closure DATA")
        catch
          case cleanup: Throwable =>
            if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  private def requireFull26(image: Image, root: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    require(image != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      image.allocationBudget.sharesAllocationLedger(root),
      "source SQL planning requires the exact open original Full26 root")
    image match
      case snapshot: MySqlScriptSchemaSnapshot =>
        require(snapshot.inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
          (snapshot.inputs.sourceLimitProfile eq root.profile),
          "source SQL planning requires schema5 Full26, not a reduced source projection")
        MySqlFormat3QueryProjection.requireDeploymentSource(snapshot, snapshot.inputs, root)
        MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(snapshot, snapshot.inputs, root)
        snapshot
      case _ => invalid("source SQL planning requires the original schema5 Full26 image")

  private def uuidCell(row: Vector[MySqlFormat3Cell], table: String, column: String,
      root: MySqlFormat3ReadBudget): Option[UUID] =
    value(row, table, column) match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        val raw = bytes.copyBytes(root, "source SQL " + table + "." + column)
        val buffer = ByteBuffer.wrap(raw)
        val result = new UUID(buffer.getLong(), buffer.getLong())
        require(MySqlNormalRecallRequest.nonZero(result), "source SQL UUID must be nonzero: " + table + "." + column)
        Some(result)
      case _ => invalid("source SQL UUID column is malformed: " + table + "." + column)

  private def sourceReserveUuid(row: Vector[MySqlFormat3Cell], table: String, column: String): UUID =
    value(row, table, column) match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        var high = 0L
        var low = 0L
        var index = 0
        while index < 8 do
          high = (high << 8) | (bytes.byteAt(index).toLong & 255L)
          low = (low << 8) | (bytes.byteAt(index + 8).toLong & 255L)
          index += 1
        val result = new UUID(high, low)
        require(MySqlNormalRecallRequest.nonZero(result),
          "source R0 UUID must be nonzero: " + table + "." + column)
        result
      case _ => invalid("source R0 UUID column is malformed: " + table + "." + column)

  private def sourceReserveOptionalUuid(row: Vector[MySqlFormat3Cell], table: String,
      column: String): Option[UUID] =
    value(row, table, column) match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        var high = 0L
        var low = 0L
        var index = 0
        while index < 8 do
          high = (high << 8) | (bytes.byteAt(index).toLong & 255L)
          low = (low << 8) | (bytes.byteAt(index + 8).toLong & 255L)
          index += 1
        val result = new UUID(high, low)
        require(MySqlNormalRecallRequest.nonZero(result),
          "source R0 optional UUID must be nonzero: " + table + "." + column)
        Some(result)
      case _ => invalid("source R0 optional UUID column is malformed: " + table + "." + column)

  private def sourceReserveKeyBefore(snapshot: MySqlScriptSchemaSnapshot, binding: UUID,
      expected: MySqlRecoveryResourceKey, root: MySqlFormat3ReadBudget): Option[Vector[MySqlFormat3Cell]] =
    require(snapshot != null && root != null && !root.isClosed && MySqlNormalRecallRequest.nonZero(binding) && expected != null &&
      expected.exactProfile == root.profile,
      "exact source R0 reservation-key lookup and original Full26 root required")
    var before = Option.empty[Vector[MySqlFormat3Cell]]
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val comparisonBudget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      val expectedBytes = expected.copyCanonicalBytes(comparisonBudget, "SOURCE R0 reservation full-key lookup")
      var matchCount = 0
      rows(snapshot, ReservationTable).foreach { row =>
        if hasUuid(row, ReservationTable, "binding_uuid", binding) then
          val keyHashMatches = value(row, ReservationTable, "resource_key_hash") match
            case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(expected.keyHashSha256)
            case _ => false
          val fullKeyMatches = value(row, ReservationTable, "resource_key") match
            case MySqlFormat3Cell.Binary(actual) => actual.equalsBytes(expectedBytes)
            case _ => false
          if keyHashMatches then
            require(value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text(expected.resourceKind) &&
              fullKeyMatches,
              "SOURCE R0 reservation hash candidate differs from its complete full key")
          if fullKeyMatches then
            require(keyHashMatches &&
              value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text(expected.resourceKind),
              "SOURCE R0 reservation full key differs from its exact kind/hash")
            matchCount = Math.addExact(matchCount, 1)
            before = Some(row)
      }
      require(matchCount <= 1, "SOURCE R0 reservation full key is duplicated in locked Full26")
    }
    before

  private def hasUuid(row: Vector[MySqlFormat3Cell], table: String, column: String, expected: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(value(row, table, column), expected)

  private def token(row: Vector[MySqlFormat3Cell], table: String, column: String): String =
    value(row, table, column) match
      case MySqlFormat3Cell.Text(text) => text
      case _ => invalid("source SQL token is absent: " + table + "." + column)

  private def signed(row: Vector[MySqlFormat3Cell], table: String, column: String): Long =
    value(row, table, column) match
      case MySqlFormat3Cell.SignedInteger(number) => number
      case _ => invalid("source SQL integer is absent: " + table + "." + column)

  private def rows(snapshot: MySqlScriptSchemaSnapshot, table: String): Vector[Vector[MySqlFormat3Cell]] =
    snapshot.relation(table).rows

  private def exactlyOne[A](found: Vector[A], label: String): A =
    require(found != null && found.size == 1, "source SQL expected one " + label)
    found.head

  private def operationContext(operationRow: Vector[MySqlFormat3Cell], step: String,
      write: MySqlNormalRecallCanonical.DecodedSourceWrite, before: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget): UUID =
    require(operationRow != null && step != null && write != null,
      "exact source operation row and typed write row are required")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(operationRow, root.profile, root, Some(before.inputs))
    val operation = uuidCell(operationRow, OperationTable, "operation_uuid", root).getOrElse(invalid("source operation UUID absent"))
    require(token(operationRow, OperationTable, "step") == step &&
      token(operationRow, OperationTable, "operation_state") == "RESERVED" &&
      hasUuid(operationRow, OperationTable, "binding_uuid", write.binding) &&
      hasUuid(operationRow, OperationTable, "workflow_uuid", write.workflow),
      "source operation must be the exact RESERVED child command for this immutable write")
    require(Vector("source_unit_uuid", "forest_uuid", "expected_source_revision").forall(column =>
      value(operationRow, OperationTable, column) == MySqlFormat3Cell.SqlNull),
      "CARRIER-scoped source-write O must leave unit/forest/expected-source-revision scalars NULL")
    require(!rows(before, OperationTable).exists(row => hasUuid(row, OperationTable, "binding_uuid", write.binding) &&
      hasUuid(row, OperationTable, "operation_uuid", operation)),
      "source operation UUID already exists in the original Full26 image")
    operation

  private def sourceContextRows(write: MySqlNormalRecallCanonical.DecodedSourceWrite,
      reservedUnitIds: Vector[UUID], before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget):
      (Vector[Vector[MySqlFormat3Cell]], Vector[Vector[MySqlFormat3Cell]], SourceRevisionComparisonData) =
    val obligationKeys = write.obligationRevisionData.values.map(item => (item.unit, item.forest)).distinct
    val scopedUnitIds = (reservedUnitIds ++ obligationKeys.map(_._1)).distinct.sortBy(_.toString)
    val scopedUnits = scopedUnitIds.map { id =>
      exactlyOne(rows(before, SourceUnitTable).filter(row => hasUuid(row, SourceUnitTable, "binding_uuid", write.binding) &&
        hasUuid(row, SourceUnitTable, "unit_uuid", id)), "original source unit " + id)
    }
    val unitCurrentForestKeys = scopedUnits.flatMap { unitRow =>
      val currentEpoch = value(unitRow, SourceUnitTable, "current_epoch") match
        case MySqlFormat3Cell.SqlNull => None
        case MySqlFormat3Cell.SignedInteger(epoch) => Some(epoch)
        case _ => invalid("source unit current epoch is malformed")
      val currentForest = uuidCell(unitRow, SourceUnitTable, "current_forest_uuid", root)
      require(currentEpoch.isDefined == currentForest.isDefined,
        "source unit current epoch/forest reference must be paired")
      val currentUnit = uuidCell(unitRow, SourceUnitTable, "unit_uuid", root)
        .getOrElse(invalid("source unit row lacks its exact unit identity"))
      currentForest.map(forest => (forest, currentUnit))
    }
    val scopedForestKeys = (unitCurrentForestKeys ++ obligationKeys.map((unit, forest) => (forest, unit)))
      .distinct.sortBy((forest, unit) => (unit.toString, forest.toString))
    val scopedForests = scopedForestKeys.map { (forest, unit) =>
      exactlyOne(rows(before, SourceForestTable).filter(row => hasUuid(row, SourceForestTable, "binding_uuid", write.binding) &&
        hasUuid(row, SourceForestTable, "unit_uuid", unit) && hasUuid(row, SourceForestTable, "forest_uuid", forest)),
        "original source forest " + forest)
    }
    scopedUnits.foreach { unitRow =>
      val unitRevision = signed(unitRow, SourceUnitTable, "unit_revision")
      require(unitRevision >= 0L, "source unit revision must be nonnegative")
      value(unitRow, SourceUnitTable, "current_epoch") match
        case MySqlFormat3Cell.SqlNull => ()
        case MySqlFormat3Cell.SignedInteger(epoch) => require(epoch >= 0L, "source unit epoch must be nonnegative")
        case _ => invalid("source unit current epoch is malformed")
    }
    scopedForests.foreach { forestRow =>
      require(token(forestRow, SourceForestTable, "forest_codec") == "FP_SOURCE_FOREST_BE_V2" &&
        signed(forestRow, SourceForestTable, "source_epoch") >= 0L,
        "source operation requires the exact original canonical forest row")
    }
    val revisionData = new SourceRevisionComparisonData(None,
      scopedUnits.map(row => signed(row, SourceUnitTable, "unit_revision")),
      scopedUnits.map { row => value(row, SourceUnitTable, "current_epoch") match
        case MySqlFormat3Cell.SqlNull => None
        case MySqlFormat3Cell.SignedInteger(epoch) => Some(epoch)
        case _ => invalid("source unit epoch is malformed") })
    (scopedUnits, scopedForests, revisionData)

  private final class SourceUnitReservationPlan private[MySqlNormalRecallSourceWorkflow] (
      val beforeRows: Vector[Vector[MySqlFormat3Cell]],
      val targetRows: Vector[Vector[MySqlFormat3Cell]],
      val unitIds: Vector[UUID])

  private def sourceUnitReservations(write: MySqlNormalRecallCanonical.DecodedSourceWrite,
      operation: UUID, before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): SourceUnitReservationPlan =
    val relationRows = rows(before, ReservationTable)
    val owned = relationRows.filter(row => hasUuid(row, ReservationTable, "binding_uuid", write.binding) &&
      token(row, ReservationTable, "resource_kind") == "SOURCE_UNIT" &&
      hasUuid(row, ReservationTable, "workflow_uuid", write.workflow))
    require(owned.nonEmpty, "source child must retain its complete held SOURCE_UNIT reservation set")
    val parsed = owned.map { row =>
      val unit = uuidCell(row, ReservationTable, "resource_unit_uuid", root)
        .getOrElse(invalid("source child SOURCE_UNIT reservation lacks its unit identity"))
      val key = MySqlRecoveryResourceKey.sourceUnit(unit, root.profile)
      require(value(row, ReservationTable, "reservation_state") == MySqlFormat3Cell.Text("HELD") &&
        value(row, ReservationTable, "resource_pod_uuid") == MySqlFormat3Cell.SqlNull,
        "source write requires every child SOURCE_UNIT reservation to remain held")
      value(row, ReservationTable, "resource_key_hash") match
        case MySqlFormat3Cell.Binary(hash) => require(hash.equalsVector(key.keyHashSha256), "source unit reservation key hash differs")
        case _ => invalid("source unit reservation key hash is absent")
      val expectedKey = key.copyCanonicalBytes(root, "source child SOURCE_UNIT reservation key")
      value(row, ReservationTable, "resource_key") match
        case MySqlFormat3Cell.Binary(actual) => require(actual.equalsBytes(expectedKey), "source unit reservation full key differs")
        case _ => invalid("source unit reservation full key is absent")
      val predecessor = uuidCell(row, ReservationTable, "operation_uuid", root)
        .getOrElse(invalid("source unit reservation predecessor is absent"))
      require(hasUuid(row, ReservationTable, "last_mutation_operation_uuid", predecessor),
        "source unit reservation predecessor and last mutation differ")
      val prior = exactlyOne(rows(before, OperationTable).filter(op => hasUuid(op, OperationTable, "binding_uuid", write.binding) &&
        hasUuid(op, OperationTable, "operation_uuid", predecessor)), "source unit reservation predecessor")
      require(token(prior, OperationTable, "operation_state") == "COMMITTED" &&
        hasUuid(prior, OperationTable, "workflow_uuid", write.workflow),
        "source unit reservation predecessor is not a committed operation of this child")
      val revision = signed(row, ReservationTable, "reservation_revision")
      require(revision >= 0L, "source unit reservation revision is negative")
      val target = MySqlControlledDebugRows.update(row, ReservationTable, Map(
        "operation_uuid" -> MySqlControlledDebugRows.uuid(operation),
        "last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(operation),
        "reservation_revision" -> MySqlControlledDebugRows.signed(Math.addExact(revision, 1L))), root)
      (unit, row, target)
    }
    val unitIds = parsed.map(_._1)
    require(unitIds.distinct.size == unitIds.size, "source child SOURCE_UNIT reservations repeat a unit")
    val beforeRows = parsed.map(_._2)
    val targetByBefore = parsed.map((_, old, next) => old -> next).toMap
    val targetRows = relationRows.map(row => targetByBefore.getOrElse(row, row))
    new SourceUnitReservationPlan(beforeRows, beforeRows.map(targetByBefore), unitIds)

  private def carrierReservation(write: MySqlNormalRecallCanonical.DecodedSourceWrite,
      operation: UUID, before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget):
      (Vector[MySqlFormat3Cell], Vector[MySqlFormat3Cell], Vector[Vector[MySqlFormat3Cell]],
        Vector[Vector[MySqlFormat3Cell]]) =
    val beforeRows = rows(before, ReservationTable)
    val matches = beforeRows.filter(row => hasUuid(row, ReservationTable, "binding_uuid", write.binding) &&
      token(row, ReservationTable, "resource_kind") == "CARRIER" &&
      (value(row, ReservationTable, "resource_key_hash") match
        case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(write.resourceKey.keyHashSha256)
        case _ => false))
    val original = exactlyOne(matches, "held CARRIER reservation")
    val expectedKey = write.resourceKey.copyCanonicalBytes(root, "source write CARRIER reservation key")
    value(original, ReservationTable, "resource_key") match
      case MySqlFormat3Cell.Binary(actual) => require(actual.equalsBytes(expectedKey), "source CARRIER reservation full key differs")
      case _ => invalid("source CARRIER reservation full key is absent")
    require(token(original, ReservationTable, "reservation_state") == "HELD" &&
      hasUuid(original, ReservationTable, "workflow_uuid", write.workflow) &&
      value(original, ReservationTable, "resource_pod_uuid") == MySqlFormat3Cell.SqlNull &&
      value(original, ReservationTable, "resource_unit_uuid") == MySqlFormat3Cell.SqlNull,
      "source write requires its original child workflow's exact held CARRIER reservation")
    val predecessor = uuidCell(original, ReservationTable, "operation_uuid", root).getOrElse(invalid("source CARRIER predecessor absent"))
    require(hasUuid(original, ReservationTable, "last_mutation_operation_uuid", predecessor),
      "source CARRIER reservation predecessor and last mutation differ")
    val predecessorRows = rows(before, OperationTable).filter(row => hasUuid(row, OperationTable, "binding_uuid", write.binding) &&
      hasUuid(row, OperationTable, "operation_uuid", predecessor))
    val prior = exactlyOne(predecessorRows, "committed CARRIER reservation predecessor")
    require(token(prior, OperationTable, "operation_state") == "COMMITTED" &&
      hasUuid(prior, OperationTable, "workflow_uuid", write.workflow),
      "source CARRIER reservation predecessor is not a committed operation of this child")
    val revision = signed(original, ReservationTable, "reservation_revision")
    require(revision >= 0L, "source CARRIER reservation revision is negative")
    val next = MySqlControlledDebugRows.update(original, ReservationTable, Map(
      "operation_uuid" -> MySqlControlledDebugRows.uuid(operation),
      "last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(operation),
      "reservation_revision" -> MySqlControlledDebugRows.signed(Math.addExact(revision, 1L))), root)
    val targetRows = beforeRows.map(row => if row == original then next else row)
    (original, next, beforeRows, targetRows)

  private def compareUuidUnsigned(left: UUID, right: UUID): Int =
    val high = java.lang.Long.compareUnsigned(left.getMostSignificantBits, right.getMostSignificantBits)
    if high != 0 then high else java.lang.Long.compareUnsigned(left.getLeastSignificantBits, right.getLeastSignificantBits)

  private def compareAscii(left: String, right: String): Int =
    var index = 0
    while index < Math.min(left.length, right.length) do
      val compared = Integer.compare(left.charAt(index).toInt, right.charAt(index).toInt)
      if compared != 0 then return compared
      index += 1
    Integer.compare(left.length, right.length)

  /** Fresh, locked Full26 -> strict R0 request and complete read-only source/effect closure DATA. */
  def prepareRestrictedSourceReservePlan(
      intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      parent: MySqlFencedTransaction.RestrictedSourceParent5,
      freshCurrent: MySqlScriptSchemaSnapshot,
      anchor: MySqlScriptSchemaSnapshot,
      freshWriter: MySqlBusinessWriterRow,
      exactFreshHistory: MySqlNormalRecallRecovery.ReceiptHistoryData,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      exactAllocation: MySqlFencedTransaction.SourceReservePlanAllocation): SourceChildReservePlanData =
    require(parent != null && root != null && exactAllocation != null,
      "exact restricted-source parent, original root and allocation holder required")
    parent.requireRetained()
    exactAllocation.withPlanDataBudget(parent, root, parent.actualLedger, intent) { planDataBudget =>
      prepareRestrictedSourceReservePlanData(intent, parent, freshCurrent, anchor, freshWriter,
        exactFreshHistory, inputs, root, exactAllocation, planDataBudget)
    }

  private def prepareRestrictedSourceReservePlanData(
      intent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent,
      parent: MySqlFencedTransaction.RestrictedSourceParent5,
      freshCurrent: MySqlScriptSchemaSnapshot,
      anchor: MySqlScriptSchemaSnapshot,
      freshWriter: MySqlBusinessWriterRow,
      exactFreshHistory: MySqlNormalRecallRecovery.ReceiptHistoryData,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      exactAllocation: MySqlFencedTransaction.SourceReservePlanAllocation,
      planDataBudget: MySqlFormat3ReadBudget): SourceChildReservePlanData =
    require(intent != null && parent != null && freshCurrent != null && anchor != null &&
      freshWriter != null && exactFreshHistory != null && inputs != null && root != null && !root.isClosed &&
      planDataBudget != null && !planDataBudget.isClosed && (planDataBudget.profile eq root.profile) &&
      planDataBudget.sharesAllocationLedger(root) && planDataBudget.isCanonicalChildOf(root) &&
      !root.isScopedReadBudget,
      "exact restricted-source intent, parent, fresh locked Full26/history, original root and plan DATA child required")
    parent.requireRetained()
    intent.requireOriginalReservePlanBinding(intent.exactLink, intent.parentReference, root,
      intent.exactDomainObservation, intent.expectedSourceObservation)
    require((parent.originalRoot eq root) && (intent.exactLink eq parent.link) &&
      (intent.parentReference eq parent.reference) &&
      intent.parentReference.workflow == parent.parentRecallWorkflow &&
      intent.parentReference.pod == parent.parentPod &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile) && (freshCurrent.inputs eq inputs) &&
      (anchor.inputs eq inputs) && (anchor eq parent.anchorFull26) &&
      (freshCurrent ne parent.currentFull26) &&
      (exactFreshHistory.current eq freshCurrent) && (exactFreshHistory.anchor eq anchor) &&
      (exactFreshHistory.inventory eq freshCurrent) &&
      (exactFreshHistory.retainedDataBudget eq planDataBudget),
      "fresh source R0 plan must retain its exact parent/link, current Full26, HANDOFF anchor and verified history")
    val before = requireFull26(freshCurrent, root)
    MySqlFormat3QueryProjection.requireDeploymentSource(anchor, inputs, root)
    MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(before, freshWriter)
    require(before.schema == anchor.schema && before.connection.serverUuid == anchor.connection.serverUuid &&
      before.connection.serverVersion == anchor.connection.serverVersion &&
      before.connection.effectiveUser == anchor.connection.effectiveUser,
      "fresh source R0 Full26 and exact HANDOFF history anchor must retain one database identity")

    val carrier = intent.sourceCarrierLocator
    val loadedRoot = intent.loadedRootPlan
    require(carrier != null && carrier.chunk != null && loadedRoot != null && loadedRoot.loaded() &&
      carrier.binding == parent.binding && carrier.unit != null && carrier.forest != null &&
      carrier.worldIdentity != null && carrier.worldIdentity.nonEmpty &&
      carrier.saveIdentity != null && carrier.saveIdentity.nonEmpty &&
      carrier.loaderIdentity != null && carrier.loaderIdentity.nonEmpty &&
      carrier.regionIdentity != null && carrier.regionIdentity.nonEmpty && carrier.generation >= 0L &&
      carrier.chunk.dimension == loadedRoot.dimension(),
      "original selected source carrier locator and its exact loaded physical domain are required")
    require(Vector(intent.childWorkflow, intent.reserveOperation, intent.plannedWrite).forall(
      MySqlNormalRecallRequest.nonZero) && intent.writeGeneration >= 0L,
      "fresh source R0 child, reserve, planned-write and generation identities are required")

    val historyInventories = MySqlNormalRecallRecovery.discover(before, parent.binding, Some(root))
    val parentInventory = exactlyOne(historyInventories.filter(_.workflow == parent.parentRecallWorkflow),
      "current original Recall/P2 history")
    require(parentInventory.pod == parent.parentPod && parentInventory.prepare == parent.reference.prepare &&
      parentInventory.checkpoint == parent.reference.checkpoint &&
      (parentInventory.cursor match
        case MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical(operation) =>
          operation.operationUuid == parent.parentP2Operation
        case _ => false),
      "fresh Full26/history no longer identifies the exact committed parent P2")
    val parentP2Row = exactlyOne(rows(before, OperationTable).filter(row =>
      hasUuid(row, OperationTable, "binding_uuid", parent.binding) &&
        hasUuid(row, OperationTable, "workflow_uuid", parent.parentRecallWorkflow) &&
        hasUuid(row, OperationTable, "operation_uuid", parent.parentP2Operation)),
      "exact current parent P2 operation row")
    require(token(parentP2Row, OperationTable, "step") == "RECALL_PREPARE" &&
      token(parentP2Row, OperationTable, "operation_state") == "COMMITTED" &&
      hasUuid(parentP2Row, OperationTable, "pod_uuid", parent.parentPod),
      "fresh Full26 parent P2 must be its exact committed Recall prepare")
    val podRow = exactlyOne(rows(before, "floating_pod_state").filter(row =>
      hasUuid(row, "floating_pod_state", "binding_uuid", parent.binding) &&
        hasUuid(row, "floating_pod_state", "uuid", parent.parentPod)),
      "exact current parent POD row")
    val podStateVersion = signed(podRow, "floating_pod_state", "state_version")
    require(podStateVersion >= 0L, "current parent POD state version must be nonnegative")
    val podKey = MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
      MySqlRecoveryResourceKey.pod(parent.parentPod, root.profile)
    }
    val podReservation = sourceReserveKeyBefore(before, parent.binding, podKey, root)
      .getOrElse(invalid("fresh Full26 lacks the exact held parent POD reservation"))
    val podReservationRevision = signed(podReservation, ReservationTable, "reservation_revision")
    require(MySqlModeOperationEffectCodec.uuidEquals(
        value(podReservation, ReservationTable, "resource_pod_uuid"), parent.parentPod) &&
      token(podReservation, ReservationTable, "reservation_state") == "HELD" &&
      hasUuid(podReservation, ReservationTable, "workflow_uuid", parent.parentRecallWorkflow) &&
      hasUuid(podReservation, ReservationTable, "operation_uuid", parent.parentP2Operation) &&
      hasUuid(podReservation, ReservationTable, "last_mutation_operation_uuid", parent.parentP2Operation) &&
      podReservationRevision >= 0L,
      "parent POD reservation must remain HELD at its exact committed P2")

    val sourceProtection = MySqlBootSourceProtectionIndex.fromDeploymentReceipt(before, inputs, root,
      exactAllocation, parent, parent.actualLedger, intent)
    var locatorMatches = 0
    sourceProtection.carriers.foreach { actual => if actual == carrier then locatorMatches = Math.addExact(locatorMatches, 1) }
    require(locatorMatches == 1,
      "selected original carrier locator must match exactly one carrier in the fresh Full26 forest closure")
    val locatorUnit = exactlyOne(rows(before, SourceUnitTable).filter(row =>
      hasUuid(row, SourceUnitTable, "binding_uuid", carrier.binding) &&
        hasUuid(row, SourceUnitTable, "unit_uuid", carrier.unit)),
      "selected source carrier unit row")
    require(token(locatorUnit, SourceUnitTable, "world_identity") == carrier.worldIdentity &&
      token(locatorUnit, SourceUnitTable, "save_identity") == carrier.saveIdentity &&
      token(locatorUnit, SourceUnitTable, "loader_identity") == carrier.loaderIdentity &&
      token(locatorUnit, SourceUnitTable, "unit_state") == "SEALED",
      "selected original carrier locator differs from its exact sealed source unit identity")
    val locatorForest = exactlyOne(rows(before, SourceForestTable).filter(row =>
      hasUuid(row, SourceForestTable, "binding_uuid", carrier.binding) &&
        hasUuid(row, SourceForestTable, "unit_uuid", carrier.unit) &&
        hasUuid(row, SourceForestTable, "forest_uuid", carrier.forest)),
      "selected source carrier forest row")
    val locatorEpoch = value(locatorUnit, SourceUnitTable, "current_epoch") match
      case MySqlFormat3Cell.SignedInteger(epoch) if epoch >= 0L => epoch
      case _ => invalid("selected source carrier unit lacks its exact current epoch")
    require(sourceReserveOptionalUuid(locatorUnit, SourceUnitTable, "current_forest_uuid").contains(carrier.forest) &&
      signed(locatorForest, SourceForestTable, "source_epoch") == locatorEpoch &&
      token(locatorForest, SourceForestTable, "forest_state") == "SEALED" &&
      token(locatorForest, SourceForestTable, "forest_codec") == "FP_SOURCE_FOREST_BE_V2",
      "selected source carrier must belong to its exact current sealed forest and epoch")

    val carrierKey = MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
      MySqlRecoveryResourceKey.carrier(carrier.saveIdentity, carrier.loaderIdentity, carrier.regionIdentity,
        carrier.chunk.x, carrier.chunk.z, root.profile)
    }
    val carrierIdentity = MySqlNormalRecallCanonical.sourceWriteCarrierIdentity(carrierKey,
      carrier.saveIdentity, carrier.loaderIdentity, carrier.regionIdentity,
      carrier.chunk.x, carrier.chunk.z, root, planDataBudget)

    val writeRows = rows(before, SourceWriteTable)
    require(!writeRows.exists(row => hasUuid(row, SourceWriteTable, "binding_uuid", parent.binding) &&
      hasUuid(row, SourceWriteTable, "write_uuid", intent.plannedWrite)),
      "fresh source R0 planned write UUID already exists in Full26")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val comparisonBudget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      val expectedKeyBytes = carrierKey.copyCanonicalBytes(comparisonBudget,
        "SOURCE R0 existing carrier-write key comparison")
      writeRows.iterator.filter(row => hasUuid(row, SourceWriteTable, "binding_uuid", parent.binding)).foreach { row =>
        val hashMatches = value(row, SourceWriteTable, "carrier_key_hash") match
          case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(carrierKey.keyHashSha256)
          case _ => false
        val fullKeyMatches = value(row, SourceWriteTable, "carrier_key") match
          case MySqlFormat3Cell.Binary(key) => key.equalsBytes(expectedKeyBytes)
          case _ => false
        if hashMatches then require(fullKeyMatches,
          "fresh source R0 carrier-write hash candidate has a different complete full key")
        if fullKeyMatches then
          require(hashMatches, "fresh source R0 existing carrier-write full key has a different hash")
          require(signed(row, SourceWriteTable, "write_generation") != intent.writeGeneration,
            "fresh source R0 CARRIER write generation already exists in Full26")
      }
    }
    val existingReserveOperations = rows(before, OperationTable).filter(row =>
      hasUuid(row, OperationTable, "binding_uuid", parent.binding) &&
        hasUuid(row, OperationTable, "operation_uuid", intent.reserveOperation))
    require(existingReserveOperations.isEmpty,
      "fresh source R0 reserve operation UUID already exists in locked Full26")
    require(!rows(before, OperationTable).exists(row =>
      hasUuid(row, OperationTable, "binding_uuid", parent.binding) &&
        hasUuid(row, OperationTable, "workflow_uuid", intent.childWorkflow)),
      "fresh source R0 child workflow UUID already has operation history")

    val obligationSourceRows = rows(before, SourceObligationTable)
    val obligationScanScratch = Math.addExact(1024L, Math.multiplyExact(obligationSourceRows.size.toLong, 128L))
    var exactObligationCount = 0L
    var openUnitCount = 0
    withSourceReserveScratch(root, planDataBudget, obligationScanScratch,
      "SOURCE R0 exact obligation count and OPEN-unit dedup workspace") {
      val openUnits = scala.collection.mutable.HashSet.empty[UUID]
      var exactCount = 0L
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        obligationSourceRows.foreach { row =>
          if hasUuid(row, SourceObligationTable, "binding_uuid", parent.binding) then
            val hashMatches = value(row, SourceObligationTable, "carrier_key_hash") match
              case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(carrierKey.keyHashSha256)
              case _ => false
            val fullKeyMatches = value(row, SourceObligationTable, "carrier_key") match
              case MySqlFormat3Cell.Binary(key) => key.compareTo(carrierIdentity.canonicalBytes) == 0
              case _ => false
            if hashMatches then require(value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER") &&
              fullKeyMatches, "source obligation hash candidate differs from its complete CARRIER full key")
            if fullKeyMatches then
              require(hashMatches && value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER"),
                "exact source obligation CARRIER key has a different kind/hash")
              exactCount = Math.addExact(exactCount, 1L)
              val purpose = token(row, SourceObligationTable, "purpose")
              val state = token(row, SourceObligationTable, "obligation_state")
              val revision = signed(row, SourceObligationTable, "obligation_revision")
              require(SourceObligationPurposes.contains(purpose) && revision >= 0L,
                "same-carrier obligation has unsupported purpose or negative revision")
              state match
                case "OPEN" => openUnits += sourceReserveUuid(row, SourceObligationTable, "unit_uuid")
                case "DISCHARGED" =>
                  throw new java.sql.SQLDataException(
                    "source R0 qualification BLOCKED: DISCHARGED carrier obligations require the missing typed original-history discharge-proof interpreter")
                case _ => invalid("same-carrier obligation has an unsupported state")
        }
      }
      require(exactCount > 0L, "fresh source R0 requires complete same-carrier obligation history")
      require(openUnits.nonEmpty,
        "fresh source R0 requires a nonempty OPEN obligation unit set; discharged history alone is insufficient")
      exactObligationCount = exactCount
      openUnitCount = openUnits.size
    }
    val exactObligationCountInt = Math.toIntExact(exactObligationCount)
    val resourceCount = Math.addExact(openUnitCount, 1)
    require(Math.addExact(Math.addExact(openUnitCount.toLong, exactObligationCount), resourceCount.toLong) <=
      root.profile.maxMembers(), "SOURCE R0 aggregate list items exceed the original member bound")
    val requestPrecharge = MySqlNormalRecallCanonical.prechargeSourceChildReserveRequestData(root,
      planDataBudget, openUnitCount, exactObligationCountInt, resourceCount)
    withSourceReserveRequestPrecharge(requestPrecharge) {

      val (obligationStamps, orderedUnitIds) = MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
        val openUnitIds = scala.collection.mutable.HashSet.empty[UUID]
        val obligationBuilder = Vector.newBuilder[MySqlNormalRecallCanonical.SourceWriteObligationStamp]
        var seenObligations = 0L
        obligationSourceRows.foreach { row =>
          if hasUuid(row, SourceObligationTable, "binding_uuid", parent.binding) then
            val hashMatches = value(row, SourceObligationTable, "carrier_key_hash") match
              case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(carrierKey.keyHashSha256)
              case _ => false
            val fullKeyMatches = value(row, SourceObligationTable, "carrier_key") match
              case MySqlFormat3Cell.Binary(key) => key.compareTo(carrierIdentity.canonicalBytes) == 0
              case _ => false
            if hashMatches then require(value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER") &&
              fullKeyMatches, "source obligation hash candidate differs from its complete CARRIER full key")
            if fullKeyMatches then
              require(hashMatches && value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER"),
                "exact source obligation CARRIER key has a different kind/hash")
              seenObligations = Math.addExact(seenObligations, 1L)
              val unit = sourceReserveUuid(row, SourceObligationTable, "unit_uuid")
              val forest = sourceReserveUuid(row, SourceObligationTable, "forest_uuid")
              val purpose = token(row, SourceObligationTable, "purpose")
              val state = token(row, SourceObligationTable, "obligation_state")
              val revision = signed(row, SourceObligationTable, "obligation_revision")
              require(SourceObligationPurposes.contains(purpose) && state == "OPEN" && revision >= 0L,
                "qualified source R0 may contain only history-qualified OPEN obligations")
              exactlyOne(rows(before, SourceForestTable).filter(forestRow =>
                hasUuid(forestRow, SourceForestTable, "binding_uuid", parent.binding) &&
                  hasUuid(forestRow, SourceForestTable, "unit_uuid", unit) &&
                  hasUuid(forestRow, SourceForestTable, "forest_uuid", forest)),
                "same-carrier obligation forest history " + forest)
              val key = MySqlNormalRecallCanonical.sourceObligationRevision(parent.binding, unit, forest,
                carrierKey.keyHashSha256, carrierIdentity.canonicalBytes, purpose, revision)
              obligationBuilder += MySqlNormalRecallCanonical.sourceWriteObligationStamp(key, state)
              openUnitIds += unit
        }
        require(seenObligations == exactObligationCount && openUnitIds.size == openUnitCount,
          "fresh locked Full26 obligation set changed between bounded count and DATA construction")
        val unsortedObligations = obligationBuilder.result()
        val sortScratch = Math.addExact(512L, Math.addExact(
          Math.multiplyExact(exactObligationCount, 64L), Math.multiplyExact(openUnitCount.toLong, 32L)))
        withSourceReserveScratch(root, planDataBudget, sortScratch, "SOURCE R0 obligation/unit canonical sorting") {
          val sortedObligations = unsortedObligations.sortWith { (left, right) =>
            val unit = compareUuidUnsigned(left.key.unit, right.key.unit)
            if unit != 0 then unit < 0
            else
              val forest = compareUuidUnsigned(left.key.forest, right.key.forest)
              if forest != 0 then forest < 0
              else compareAscii(left.key.purpose, right.key.purpose) < 0
          }
          val sortedUnits = openUnitIds.toVector.sortWith((left, right) => compareUuidUnsigned(left, right) < 0)
          (sortedObligations, sortedUnits)
        }
      }
      val unitStamps = MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) { orderedUnitIds.map { unit =>
        val unitRow = exactlyOne(rows(before, SourceUnitTable).filter(row =>
          hasUuid(row, SourceUnitTable, "binding_uuid", parent.binding) &&
            hasUuid(row, SourceUnitTable, "unit_uuid", unit)), "exact current OPEN-obligation source unit " + unit)
        val revision = signed(unitRow, SourceUnitTable, "unit_revision")
        val epoch = value(unitRow, SourceUnitTable, "current_epoch") match
          case MySqlFormat3Cell.SignedInteger(actual) if actual >= 0L => actual
          case _ => invalid("OPEN source obligation unit lacks a nonnegative current epoch")
        val forest = sourceReserveOptionalUuid(unitRow, SourceUnitTable, "current_forest_uuid")
          .getOrElse(invalid("OPEN source obligation unit lacks its current forest"))
        require(revision >= 0L && token(unitRow, SourceUnitTable, "unit_state") == "SEALED",
          "OPEN source obligation UnitExpectation requires a sealed nonnegative current unit")
        val forestRow = exactlyOne(rows(before, SourceForestTable).filter(row =>
          hasUuid(row, SourceForestTable, "binding_uuid", parent.binding) &&
            hasUuid(row, SourceForestTable, "unit_uuid", unit) &&
            hasUuid(row, SourceForestTable, "forest_uuid", forest)), "current OPEN-obligation forest " + forest)
        require(signed(forestRow, SourceForestTable, "source_epoch") == epoch &&
          token(forestRow, SourceForestTable, "forest_state") == "SEALED" &&
          token(forestRow, SourceForestTable, "forest_codec") == "FP_SOURCE_FOREST_BE_V2",
          "OPEN source obligation UnitExpectation differs from its exact sealed current forest/epoch")
        MySqlNormalRecallCanonical.sourceWriteUnitStamp(unit, revision, epoch, forest)
      }}
      val resourceScratch = Math.addExact(512L, Math.multiplyExact(resourceCount.toLong, 96L))
      val resources = withSourceReserveScratch(root, planDataBudget, resourceScratch,
        "SOURCE R0 lock-key/resource expectation assembly") {
        val resourceKeys = MySqlRecoveryResourceKey.orderForLocks(
          Vector(carrierKey) ++ orderedUnitIds.map(unit => MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
            MySqlRecoveryResourceKey.sourceUnit(unit, root.profile)
          }))
        resourceKeys.map { key =>
          val beforeRow = sourceReserveKeyBefore(before, parent.binding, key, root)
          MySqlNormalRecallCanonical.sourceChildReserveResourceExpectation(key, beforeRow, inputs, root,
            planDataBudget)
        }
      }
      require(resources.forall(_.before.forall(row =>
        token(row, ReservationTable, "reservation_state") != "HELD")),
        "fresh source R0 cannot adopt HELD reservations without their exact existing reserve operation")

      val allUnitRows = rows(before, SourceUnitTable)
      val footprintChunkRows = rows(before, "physical_recovery_footprint_chunk")
      val itemCount = Math.addExact(Math.addExact(allUnitRows.size.toLong, sourceProtection.carriers.size.toLong),
        Math.addExact(footprintChunkRows.size.toLong, sourceProtection.unresolved.size.toLong))
      require(itemCount <= root.profile.maxMembers(),
        "source R0 complete effect closure exceeds the original Full26 member bound")
      val closureCharge = Math.addExact(4352L, Math.addExact(
        Math.multiplyExact(allUnitRows.size.toLong, 256L), Math.addExact(
          Math.multiplyExact(sourceProtection.carriers.size.toLong, 256L), Math.addExact(
            Math.multiplyExact(footprintChunkRows.size.toLong, 256L),
            Math.multiplyExact(sourceProtection.unresolved.size.toLong, 128L)))))
      planDataBudget.reserveRetainedMemory(closureCharge, "SOURCE R0 complete closure DATA BEFORE allocation")
      var carrierCoverage: Vector[SourceChildCarrierCoverageData] = null
      var sourceUnits: Vector[SourceChildUnitCoverageData] = null
      var oldFootprintChunks: Vector[SourceChildFootprintChunkData] = null
      var unresolved: Vector[SourceChildUnresolvedData] = null
      var parentClosure: SourceChildParentClosureData = null
      var effectClosure: SourceChildReserveEffectClosureData = null
      var request: MySqlNormalRecallCanonical.SourceChildReserveRequestData = null
      withSourceReserveClosurePrecharge(root, planDataBudget, closureCharge, () => {
        carrierCoverage = null
        sourceUnits = null
        oldFootprintChunks = null
        unresolved = null
        parentClosure = null
        effectClosure = null
        request = null
      }) {
        MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
        carrierCoverage = sourceProtection.carriers.map { sourceCarrier =>
          val classification = sourceProtection.classify(sourceCarrier.chunk.dimension,
            sourceCarrier.chunk.x, sourceCarrier.chunk.z)
          val (status, reasons) = classification match
            case MySqlBootSourceProtectionIndex.Protected(local) => ("PROTECTED", local)
            case MySqlBootSourceProtectionIndex.Unknown(local, _) => ("UNKNOWN", local)
            case MySqlBootSourceProtectionIndex.Unprotected => ("UNPROTECTED", Vector.empty[String])
          new SourceChildCarrierCoverageData(sourceCarrier, status, reasons)
        }
        sourceUnits = allUnitRows.map { row =>
          val currentEpoch = value(row, SourceUnitTable, "current_epoch") match
            case MySqlFormat3Cell.SqlNull => None
            case MySqlFormat3Cell.SignedInteger(epoch) if epoch >= 0L => Some(epoch)
            case _ => invalid("complete source closure unit epoch is malformed")
          new SourceChildUnitCoverageData(sourceReserveUuid(row, SourceUnitTable, "binding_uuid"),
            sourceReserveUuid(row, SourceUnitTable, "unit_uuid"), token(row, SourceUnitTable, "world_identity"),
            token(row, SourceUnitTable, "save_identity"), token(row, SourceUnitTable, "loader_identity"),
            sourceReserveUuid(row, SourceUnitTable, "catalog_generation"),
            signed(row, SourceUnitTable, "read_generation"), signed(row, SourceUnitTable, "unit_revision"),
            token(row, SourceUnitTable, "unit_state"), currentEpoch,
            sourceReserveOptionalUuid(row, SourceUnitTable, "current_forest_uuid"))
        }
        oldFootprintChunks = footprintChunkRows.map { row =>
          new SourceChildFootprintChunkData(sourceReserveUuid(row, "physical_recovery_footprint_chunk", "binding_uuid"),
            sourceReserveUuid(row, "physical_recovery_footprint_chunk", "pod_uuid"),
            sourceReserveUuid(row, "physical_recovery_footprint_chunk", "attempt_uuid"),
            sourceReserveUuid(row, "physical_recovery_footprint_chunk", "footprint_uuid"),
            Math.toIntExact(signed(row, "physical_recovery_footprint_chunk", "dimension_id")),
            Math.toIntExact(signed(row, "physical_recovery_footprint_chunk", "chunk_x")),
            Math.toIntExact(signed(row, "physical_recovery_footprint_chunk", "chunk_z")))
        }
        unresolved = sourceProtection.unresolved.map(value =>
          new SourceChildUnresolvedData(value.code, value.table))
        parentClosure = new SourceChildParentClosureData(parent.parentPod, parent.parentRecallWorkflow,
          parent.reference.prepare, parent.reference.checkpoint, parent.parentP2Operation, podStateVersion,
          token(parentP2Row, OperationTable, "step"), token(parentP2Row, OperationTable, "operation_state"),
          podReservationRevision)
        effectClosure = new SourceChildReserveEffectClosureData(parentClosure, sourceUnits,
          carrierCoverage, oldFootprintChunks, unresolved, sourceProtection)
        }

        request = MySqlNormalRecallCanonical.sourceChildReserveRequestDataPrecharged(parent.binding,
          intent.childWorkflow, parent.parentRecallWorkflow, parent.parentP2Operation,
          intent.reserveOperation, intent.plannedWrite, intent.writeGeneration, carrierIdentity,
          unitStamps, obligationStamps, resources, inputs, root, planDataBudget, requestPrecharge)
        MySqlRecoveryCanonicalBytes.withAllocationBudget(planDataBudget) {
          new SourceChildReservePlanData(request, before, exactFreshHistory, effectClosure,
            parent, intent, root, exactAllocation)
        }
      }
    }
  private def sourceReserveExactRow(snapshot: MySqlScriptSchemaSnapshot, table: String,
      description: String)(matches: Vector[MySqlFormat3Cell] => Boolean): Vector[MySqlFormat3Cell] =
    require(snapshot != null && table != null && description != null && matches != null,
      "exact source R0 row lookup inputs required")
    var found: Vector[MySqlFormat3Cell] = null
    var count = 0
    rows(snapshot, table).foreach { row =>
      if matches(row) then
        count = Math.addExact(count, 1)
        found = row
    }
    require(count == 1, "fresh Full26 requires exactly one " + description)
    found

  private def sourceReserveFieldIndex(shape: MySqlFormat3RelationShape, name: String): Int =
    require(shape != null && name != null, "original Full26 column lookup requires a shape and name")
    val index = shape.columns.indexWhere(_.name == name)
    require(index >= 0, "original Full26 " + shape.table + " lacks its frozen column " + name)
    index

  private def sourceReserveResourceKeyMatches(resource: MySqlRecoveryResourceKey,
      actual: MySqlFormat3Binary): Boolean =
    require(resource != null && actual != null, "typed source R0 resource key and Full26 bytes required")
    if resource.canonicalLength != actual.length then false
    else
      var position = 0
      var matches = true
      val sink = new java.io.OutputStream:
        override def write(value: Int): Unit =
          if position >= actual.length || actual.byteAt(position) != value.toByte then matches = false
          position = Math.addExact(position, 1)
        override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
          if bytes == null || offset < 0 || length < 0 || offset > bytes.length - length then
            throw new IndexOutOfBoundsException("source R0 full-key comparison slice is invalid")
          var index = 0
          while index < length do
            write(bytes(offset + index).toInt)
            index += 1
      val output = new java.io.DataOutputStream(sink)
      resource.writeCanonicalTo(output)
      output.flush()
      matches && position == actual.length

  private def sourceReserveImageSnapshot(image: Image, label: String): MySqlScriptSchemaSnapshot = image match
    case snapshot: MySqlScriptSchemaSnapshot => snapshot
    case _ => invalid(label + " must retain the original schema5 Full26 snapshot")

  private def sourceReserveHistoryRelation(relations: Vector[MySqlFormat3RelationSnapshot],
      table: String, label: String): MySqlFormat3RelationSnapshot =
    require(relations != null && table != null && label != null,
      "typed source R0 history relation lookup inputs required")
    var found: MySqlFormat3RelationSnapshot = null
    var count = 0
    relations.foreach { relation =>
      if relation != null && relation.table == table then
        count = Math.addExact(count, 1)
        found = relation
    }
    require(count == 1, label + " must contain exactly one original " + table + " relation")
    found

  private def sourceReserveResourceRowMatches(row: Vector[MySqlFormat3Cell], binding: UUID,
      resource: MySqlRecoveryResourceKey): Boolean =
    row != null && resource != null && hasUuid(row, ReservationTable, "binding_uuid", binding) &&
      value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text(resource.resourceKind) &&
      (value(row, ReservationTable, "resource_key_hash") match
        case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(resource.keyHashSha256)
        case _ => false) &&
      (value(row, ReservationTable, "resource_key") match
        case MySqlFormat3Cell.Binary(key) => sourceReserveResourceKeyMatches(resource, key)
        case _ => false)

  /** A typed release predecessor retained internally until the complete anchored chain qualifies. */
  private[mysql] final class SourceReserveHistoryReceiptData private[MySqlNormalRecallSourceWorkflow] (
      val row: Vector[MySqlFormat3Cell], val context: MySqlOperationContext,
      val receiptFingerprint: Vector[Byte], val before: Vector[MySqlFormat3RelationSnapshot],
      val target: Vector[MySqlFormat3RelationSnapshot])

  private[mysql] def sourceReserveHistoryReceiptData(row: Vector[MySqlFormat3Cell],
      context: MySqlOperationContext, receiptFingerprint: Vector[Byte],
      before: Vector[MySqlFormat3RelationSnapshot], target: Vector[MySqlFormat3RelationSnapshot],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceReserveHistoryReceiptData =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && dataBudget != null &&
      !dataBudget.isClosed && (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)) && row != null && context != null &&
      receiptFingerprint != null && receiptFingerprint.size == 32 && before != null && target != null,
      "source reserve release evidence requires one exact typed receipt and original root/direct-child owner")
    dataBudget.reserveRetainedMemory(384L, "source reserve qualified receipt evidence before allocation")
    new SourceReserveHistoryReceiptData(row, context, receiptFingerprint, before, target)

  private def requireSourceReserveReleaseQualified(row: Vector[MySqlFormat3Cell],
      history: MySqlNormalRecallRecovery.ReceiptHistoryData, binding: UUID,
      resource: MySqlRecoveryResourceKey, root: MySqlFormat3ReadBudget): Unit =
    require(row != null && history != null && MySqlNormalRecallRequest.nonZero(binding) &&
      resource != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "released source reservation requires its exact Full26 history, binding, resource and original root")
    val owner = history.retainedDataBudget
    val evidence = history.newestFirst.map(receipt => sourceReserveHistoryReceiptData(receipt.row,
      receipt.context, receipt.receiptFingerprint, receipt.before, receipt.target, root, owner))
    requireSourceReserveReleaseQualifiedData(row, history.inventory, binding, resource, root, evidence)

  private[mysql] def qualifyHistoricalSourceChildReserveReleasePredecessors(
      candidate: MySqlNormalRecallCanonical.HistoricalSourceChildReserveReceiptData,
      inventory: Image, earlierValidated: scala.collection.Iterable[SourceReserveHistoryReceiptData],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    require(candidate != null && inventory != null && earlierValidated != null && root != null &&
      !root.isClosed && !root.isScopedReadBudget && dataBudget != null && !dataBudget.isClosed &&
      dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)) &&
      (candidate.root eq root) && (candidate.dataBudget eq dataBudget),
      "historical R0 predecessor qualification requires its exact full candidate, inventory and DATA owner")
    candidate.request.resources.foreach { entry =>
      entry.before.foreach(row => requireSourceReserveReleaseQualifiedData(row, inventory,
        candidate.request.binding, entry.resource, root, earlierValidated))
    }

  private[mysql] def requireSourceReserveReleaseQualifiedData(row: Vector[MySqlFormat3Cell],
      inventoryImage: Image, binding: UUID, resource: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget,
      earlierValidated: scala.collection.Iterable[SourceReserveHistoryReceiptData]): Unit =
    require(row != null && inventoryImage != null && MySqlNormalRecallRequest.nonZero(binding) &&
      resource != null && root != null && !root.isClosed && !root.isScopedReadBudget && earlierValidated != null,
      "released source reservation requires original Full26 inventory, earlier typed receipt evidence and root")
    val operation = sourceReserveUuid(row, ReservationTable, "operation_uuid")
    require(token(row, ReservationTable, "reservation_state") == "RELEASED" &&
      hasUuid(row, ReservationTable, "last_mutation_operation_uuid", operation),
      "released source reservation must name one exact final mutation operation")
    val inventory = sourceReserveImageSnapshot(inventoryImage, "released reservation inventory")
    val inventoryOperationRows = sourceReserveHistoryRelation(inventory.relations, OperationTable,
      "released reservation inventory").rows
    var predecessor: Vector[MySqlFormat3Cell] = null
    var predecessorCount = 0
    inventoryOperationRows.foreach { candidate =>
      if hasUuid(candidate, OperationTable, "binding_uuid", binding) &&
          hasUuid(candidate, OperationTable, "operation_uuid", operation) then
        predecessorCount = Math.addExact(predecessorCount, 1)
        predecessor = candidate
    }
    require(predecessorCount == 1,
      "released source reservation lacks its unique exact operation in the complete original inventory")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(predecessor, root.profile, root, inputsForImage(inventoryImage, root))
    require(token(predecessor, OperationTable, "operation_state") == "COMMITTED" &&
      hasUuid(predecessor, OperationTable, "workflow_uuid",
        sourceReserveUuid(row, ReservationTable, "workflow_uuid")),
      "released source reservation owner must be its exact committed original inventory operation")

    var matchingReceipts = 0
    earlierValidated.foreach { receipt =>
      if receipt != null && receipt.context.bindingUuid == binding &&
          receipt.context.operationUuid == operation then
        matchingReceipts = Math.addExact(matchingReceipts, 1)
        require(receipt.row == predecessor &&
          token(receipt.row, OperationTable, "operation_state") == "COMMITTED" &&
          receipt.context.canonicalFingerprintSha256 == receipt.receiptFingerprint &&
          hasUuid(receipt.row, OperationTable, "binding_uuid", binding) &&
          hasUuid(receipt.row, OperationTable, "operation_uuid", operation) &&
          signed(receipt.row, OperationTable, "fence_epoch") == receipt.context.writerEpoch,
          "released source reservation must bind one exact committed typed receipt and its original O/context")
        val beforeReservationRelation = sourceReserveHistoryRelation(receipt.before, ReservationTable,
          "released reservation receipt BEFORE")
        var beforeReservation: Vector[MySqlFormat3Cell] = null
        var beforeCount = 0
        beforeReservationRelation.rows.foreach { candidate =>
          if sourceReserveResourceRowMatches(candidate, binding, resource) then
            beforeCount = Math.addExact(beforeCount, 1)
            beforeReservation = candidate
        }
        require(beforeCount == 1 &&
          token(beforeReservation, ReservationTable, "reservation_state") == "HELD",
          "released source reservation receipt must begin from one exact HELD resource row")
        val releasedRelation = sourceReserveHistoryRelation(receipt.target, ReservationTable,
          "released reservation receipt TARGET")
        var releasedTarget: Vector[MySqlFormat3Cell] = null
        var targetCount = 0
        releasedRelation.rows.foreach { candidate =>
          if sourceReserveResourceRowMatches(candidate, binding, resource) then
            targetCount = Math.addExact(targetCount, 1)
            releasedTarget = candidate
        }
        require(targetCount == 1 && releasedTarget == row &&
          token(releasedTarget, ReservationTable, "reservation_state") == "RELEASED" &&
          hasUuid(releasedTarget, ReservationTable, "workflow_uuid",
            sourceReserveUuid(receipt.row, OperationTable, "workflow_uuid")) &&
          hasUuid(releasedTarget, ReservationTable, "operation_uuid", operation) &&
          hasUuid(releasedTarget, ReservationTable, "last_mutation_operation_uuid", operation),
          "released source reservation must equal the exact typed receipt's full TARGET row")
        val oldRevision = signed(beforeReservation, ReservationTable, "reservation_revision")
        val newRevision = signed(releasedTarget, ReservationTable, "reservation_revision")
        require(oldRevision >= 0L && newRevision == Math.addExact(oldRevision, 1L),
          "released source reservation receipt must advance its exact revision once")
        val shape = inventory.inputs.forTable(ReservationTable)
        shape.columns.zipWithIndex.foreach { (column, index) =>
          val mutable = column.name == "reservation_revision" || column.name == "workflow_uuid" ||
            column.name == "operation_uuid" || column.name == "reservation_state" ||
            column.name == "last_mutation_operation_uuid"
          if !mutable then
            require(beforeReservation(index) == releasedTarget(index),
              "released source reservation receipt changed immutable full-key/reference column " + column.name)
        }

        val writerRelation = sourceReserveHistoryRelation(receipt.target, WriterTable,
          "released reservation receipt W TARGET")
        require(writerRelation.rows.size == 1, "released reservation receipt W must retain its singleton TARGET")
        val writer = writerRelation.rows.head
        require(value(writer, WriterTable, "singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L) &&
          hasUuid(writer, WriterTable, "binding_uuid", binding) &&
          signed(writer, WriterTable, "writer_epoch") == receipt.context.writerEpoch &&
          hasUuid(writer, WriterTable, "owner_boot_uuid", receipt.context.bootUuid) &&
          hasUuid(writer, WriterTable, "last_commit_operation_uuid", operation) &&
          (value(writer, WriterTable, "last_commit_fingerprint") match
            case MySqlFormat3Cell.Binary(fingerprint) =>
              fingerprint.equalsVector(receipt.receiptFingerprint)
            case _ => false),
          "released source reservation must retain its exact original committed O/W receipt chain")
    }
    require(matchingReceipts == 1,
      "released source reservation lacks one exact typed committed receipt in the retained Full26 history")

  private def sourceReserveDateTime(value: LocalDateTime, label: String): Unit =
    require(value != null && value.getYear >= 1000 && value.getYear <= 9999 && value.getNano % 1000 == 0,
      label + " must be the caller's exact MySQL DATETIME(6) value")

  private def sourceReserveOwnedUuidCell(id: UUID, dataBudget: MySqlFormat3ReadBudget,
      label: String): MySqlFormat3Cell =
    require(MySqlNormalRecallRequest.nonZero(id) && dataBudget != null && !dataBudget.isClosed && label != null,
      "source R0 child-owned UUID Cell requires an exact nonzero identity and live plan child")
    dataBudget.reserveRetainedMemory(96L, label + " before immutable Cell allocation")
    val bytes = ByteBuffer.allocate(16)
    bytes.putLong(id.getMostSignificantBits)
    bytes.putLong(id.getLeastSignificantBits)
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.array(), dataBudget))

  private def sourceReserveOwnedDigestCell(digest: Vector[Byte],
      dataBudget: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    require(digest != null && digest.size == 32 && dataBudget != null &&
      !dataBudget.isClosed && label != null,
      "source R0 child-owned digest Cell requires one exact SHA-256 and live plan child")
    dataBudget.reserveRetainedMemory(96L, label + " before immutable Cell allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest.toArray, dataBudget))

  private def sourceReserveOwnedResourceKeyCell(resource: MySqlRecoveryResourceKey,
      dataBudget: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    require(resource != null && dataBudget != null && !dataBudget.isClosed && label != null,
      "source R0 child-owned resource-key Cell requires its exact key and live plan child")
    val bytes = resource.copyCanonicalBytes(dataBudget, label + " full-key payload")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, dataBudget))

  private def sourceReserveCanonicalCell(canonical: MySqlRecoveryCanonicalBytes,
      dataBudget: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    require(canonical != null && canonical.borrowedBinary != null && dataBudget != null &&
      !dataBudget.isClosed && label != null &&
      (canonical.borrowedBinary.storageBudget eq dataBudget),
      "source R0 literal canonical Cell must borrow its exact immutable plan-child bytes")
    MySqlFormat3Cell.Binary(canonical.borrowedBinary)

  private def sourceReserveEffectCellValue(
      row: Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell], index: Int,
      label: String): MySqlFormat3Cell =
    require(row != null && index >= 0 && index < row.size && label != null,
      "source R0 projected row primary key Cell index must be within the exact row")
    row(index) match
      case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell) => cell
      case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(_) =>
        throw new java.sql.SQLDataException(label + " cannot use a Derived primary-key Cell")

  private def sourceReserveComparePrimaryKeyCell(left: MySqlFormat3Cell,
      right: MySqlFormat3Cell, table: String, column: String): Int = (left, right) match
    case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) =>
      var leftOffset = 0
      var rightOffset = 0
      while leftOffset < a.length && rightOffset < b.length do
        val compared = Integer.compare(a.codePointAt(leftOffset), b.codePointAt(rightOffset))
        if compared != 0 then return compared
        leftOffset += Character.charCount(a.codePointAt(leftOffset))
        rightOffset += Character.charCount(b.codePointAt(rightOffset))
      Integer.compare(a.length - leftOffset, b.length - rightOffset)
    case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) =>
      java.lang.Long.compare(a, b)
    case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
    case _ => throw new java.sql.SQLDataException(
      s"source R0 projected $table.$column primary key has an unsupported or NULL Cell")

  private def sourceReserveComparePrimaryKey(
      left: Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell],
      right: Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell],
      shape: MySqlFormat3RelationShape): Int =
    shape.primaryKey.iterator.map { name =>
      val index = sourceReserveFieldIndex(shape, name)
      sourceReserveComparePrimaryKeyCell(
        sourceReserveEffectCellValue(left, index, "source R0 " + shape.table + " left primary key"),
        sourceReserveEffectCellValue(right, index, "source R0 " + shape.table + " right primary key"),
        shape.table, name)
    }.find(_ != 0).getOrElse(0)

  private def sourceReserveSortedEffectRows(
      rows: Vector[Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell]],
      shape: MySqlFormat3RelationShape): Vector[Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell]] =
    require(rows != null && shape != null, "source R0 relation sorting needs exact typed rows and shape")
    val sorted = rows.sortWith((left, right) => sourceReserveComparePrimaryKey(left, right, shape) < 0)
    var previous: Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell] = null
    sorted.foreach { row =>
      require(previous == null || sourceReserveComparePrimaryKey(previous, row, shape) < 0,
        "source R0 projected " + shape.table + " contains a duplicate or unsorted original primary key")
      previous = row
    }
    sorted

  private def sourceReserveEffectCandidateScratchBytes(inputs: MySqlScriptSchemaInputs,
      rowCounts: Vector[Int], sortedRows: Long): Long =
    require(inputs != null && rowCounts != null && rowCounts.size == inputs.canonicalOrder.size &&
      sortedRows >= 0L, "source R0 Full26 precharge needs every target row count and sort bound")
    var total = 0L
    inputs.canonicalOrder.zip(rowCounts).foreach { (shape, rowCount) =>
      require(rowCount >= 0, "source R0 projected Full26 row count cannot be negative")
      val perRow = Math.addExact(192L, Math.multiplyExact(shape.columns.size.toLong, 64L))
      total = Math.addExact(total, Math.addExact(256L, Math.multiplyExact(rowCount.toLong, perRow)))
    }
    Math.addExact(total, Math.addExact(512L, Math.multiplyExact(sortedRows, 32L)))

  private def requireSourceReservePlanLineage(plan: SourceChildReservePlanData,
      dataBudget: MySqlFormat3ReadBudget): MySqlScriptSchemaInputs =
    require(plan != null && dataBudget != null && !dataBudget.isClosed &&
      plan.parent != null && plan.intent != null && plan.root != null && !plan.root.isClosed &&
      !plan.root.isScopedReadBudget && plan.allocation != null &&
      (plan.parent.originalRoot eq plan.root) && (plan.parent.actualLedger != null) &&
      (plan.intent.exactLink eq plan.parent.link) &&
      (plan.intent.parentReference eq plan.parent.reference) &&
      (plan.before ne plan.parent.currentFull26) &&
      plan.before.allocationBudget.sharesAllocationLedger(plan.root) &&
      dataBudget.sharesAllocationLedger(plan.root) &&
      (dataBudget.profile eq plan.root.profile) && dataBudget.isCanonicalChildOf(plan.root),
      "SOURCE R0 assembler requires the exact retained Full26 parent, original root, direct plan child and registered holder")
    plan.parent.requireRetained()
    val inputs = plan.before.inputs
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq plan.root.profile) &&
      (plan.history != null) && (plan.history.inventory eq plan.before) &&
      (plan.history.current eq plan.before) && (plan.history.anchor eq plan.parent.anchorFull26) &&
      (plan.history.retainedDataBudget eq dataBudget),
      "SOURCE R0 assembler requires the exact current/inventory/anchor typed history and Full26 plan child")
    requireFull26(plan.before, plan.root)
    inputs

  private def requireSourceReserveContextAndWriter(plan: SourceChildReservePlanData,
      identity: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime): Unit =
    require(identity != null && identity.tag != null,
      "actual original MySqlRepositoryOperationContextIdentity is required for SOURCE R0 precommit")
    sourceReserveDateTime(plannedAt, "SOURCE R0 plannedAt")
    val request = plan.request
    val before = plan.before
    val parent = plan.parent
    val intent = plan.intent
    val inputs = before.inputs
    val scope = before.observationScope
    val connection = before.connection
    require(request != null && request.binding == parent.binding &&
      request.childWorkflow == intent.childWorkflow &&
      request.parentRecallWorkflow == parent.parentRecallWorkflow &&
      request.parentP2Operation == parent.parentP2Operation &&
      request.reserveOperation == intent.reserveOperation &&
      request.plannedWrite == intent.plannedWrite &&
      request.writeGeneration == intent.writeGeneration &&
      identity.tag.operationId == request.reserveOperation &&
      identity.reserveOperationUuid == request.reserveOperation &&
      identity.workflowUuid == request.childWorkflow &&
      identity.bindingUuid == request.binding &&
      identity.requestKind == "RUN" &&
      identity.requestIdentitySha256 == request.canonical.sha256 &&
      identity.tag.bootId == scope.bootUuid &&
      identity.tag.bootGeneration == scope.bootGeneration &&
      identity.tag.connectionGeneration == scope.connectionGeneration &&
      identity.serverUuid == connection.serverUuid &&
      identity.serverVersion == connection.serverVersion &&
      identity.schema == before.schema && connection.catalog == before.schema &&
      identity.effectiveUser == connection.effectiveUser &&
      inputs.schemaFormat == identity.schemaFormat && identity.schemaFormat == 5 &&
      identity.physicalFormat == 2 && inputs.ddlSha256.equalsIgnoreCase(identity.ddlSha256) &&
      (identity.exactLimitProfile eq plan.root.profile) &&
      identity.limitProfileSha256 == plan.root.profile.sha256().toVector,
      "actual SOURCE R0 context identity differs from its exact plan request, original Full26 connection or root")
    require(identity.writerEpoch > 0L && identity.fixedLeaseUntil.isAfter(plannedAt),
      "actual SOURCE R0 fence lease must remain valid through the caller's plannedAt")
    val writerShape = inputs.forTable(WriterTable)
    val writer = sourceReserveExactRow(before, WriterTable, "exact singleton runtime writer row") { row =>
      value(row, WriterTable, "singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L)
    }
    require(writerShape.primaryKey == Vector("singleton_fixed1") &&
      (value(writer, WriterTable, "binding_uuid") match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(request.binding)
        case _ => false),
      "actual SOURCE R0 singleton W must retain its exact binding primary row")
    require(signed(writer, WriterTable, "writer_epoch") == identity.writerEpoch &&
      (value(writer, WriterTable, "owner_boot_uuid") match
        case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(identity.tag.bootId)
        case _ => false),
      "actual SOURCE R0 W binding, fence epoch and owner boot differ from its captured context")
    require(value(writer, WriterTable, "lease_until") == MySqlFormat3Cell.DateTime(identity.fixedLeaseUntil),
      "actual SOURCE R0 W lease differs from its exact captured fixed lease")
    val parentP2 = sourceReserveExactRow(before, OperationTable, "exact committed parent P2 row") { row =>
      hasUuid(row, OperationTable, "binding_uuid", parent.binding) &&
        hasUuid(row, OperationTable, "workflow_uuid", parent.parentRecallWorkflow) &&
        hasUuid(row, OperationTable, "operation_uuid", parent.parentP2Operation)
    }
    require(token(parentP2, OperationTable, "step") == "RECALL_PREPARE" &&
      token(parentP2, OperationTable, "operation_state") == "COMMITTED" &&
      hasUuid(parentP2, OperationTable, "pod_uuid", parent.parentPod),
      "actual SOURCE R0 Full26 no longer retains its exact committed parent P2")

  private def requireSourceReserveRequestMatchesBefore(plan: SourceChildReservePlanData,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    require(dataBudget != null && !dataBudget.isClosed && dataBudget.sharesAllocationLedger(plan.root) &&
      dataBudget.isCanonicalChildOf(plan.root),
      "actual SOURCE R0 request comparison requires its exact direct plan DATA child")
    requireSourceReserveRequestMatchesBeforeData(plan.request, plan.before, plan.root, dataBudget)
    val evidenceBytes = Math.addExact(512L, Math.multiplyExact(plan.history.newestFirst.size.toLong, 32L))
    dataBudget.reserveRetainedMemory(evidenceBytes, "SOURCE R0 complete prior release-receipt evidence vector")
    val evidence = plan.history.newestFirst.map(receipt => sourceReserveHistoryReceiptData(receipt.row,
      receipt.context, receipt.receiptFingerprint, receipt.before, receipt.target, plan.root, dataBudget))
    plan.request.resources.foreach { entry =>
      entry.before.foreach(row => requireSourceReserveReleaseQualifiedData(row, plan.history.inventory,
        plan.request.binding, entry.resource, plan.root, evidence))
    }

  /** Shared pure preimage validator for live H3 and strict H1 history; release proof is a separate history gate. */
  private[mysql] def requireSourceReserveRequestMatchesBeforeData(
      request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    require(request != null && before != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      dataBudget != null && !dataBudget.isClosed && (dataBudget.profile eq root.profile) &&
      dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)),
      "SOURCE R0 complete preimage validation requires its original Full26 root and root/direct-child DATA owner")
    val binding = request.binding
    val reservationRows = rows(before, ReservationTable)
    val operationRows = rows(before, OperationTable)
    val unitRows = rows(before, SourceUnitTable)
    val obligationRows = rows(before, SourceObligationTable)
    require(!operationRows.exists(row => hasUuid(row, OperationTable, "binding_uuid", binding) &&
      hasUuid(row, OperationTable, "operation_uuid", request.reserveOperation)),
      "actual SOURCE R0 BEFORE already contains its exact O(binding,R0) primary key")
    require(!operationRows.exists(row => hasUuid(row, OperationTable, "binding_uuid", binding) &&
      hasUuid(row, OperationTable, "workflow_uuid", request.childWorkflow)),
      "actual SOURCE R0 BEFORE already contains operation history for the immutable child workflow")
    require(request.units != null && request.units.nonEmpty &&
      request.obligations != null && request.resources != null &&
      request.resources.size == Math.addExact(request.units.size, 1),
      "actual SOURCE R0 request U/O/R lists must retain their complete typed resource counts")

    var unitIndex = 0
    while unitIndex < request.units.size do
      val stamp = request.units(unitIndex)
      var unitMatches = 0
      var selectedUnit: Vector[MySqlFormat3Cell] = null
      unitRows.foreach { row =>
        if hasUuid(row, SourceUnitTable, "binding_uuid", binding) &&
            hasUuid(row, SourceUnitTable, "unit_uuid", stamp.unit) then
          unitMatches = Math.addExact(unitMatches, 1)
          selectedUnit = row
      }
      require(unitMatches == 1 &&
        signed(selectedUnit, SourceUnitTable, "unit_revision") == stamp.unitRevision &&
        value(selectedUnit, SourceUnitTable, "current_epoch") == MySqlFormat3Cell.SignedInteger(stamp.epoch) &&
        sourceReserveOptionalUuid(selectedUnit, SourceUnitTable, "current_forest_uuid").contains(stamp.currentForest) &&
        token(selectedUnit, SourceUnitTable, "unit_state") == "SEALED",
        "actual SOURCE R0 UnitExpectation differs from its exact SEALED source unit row")
      val forest = sourceReserveExactRow(before, SourceForestTable,
        "exact current SOURCE R0 unit forest row") { row =>
        hasUuid(row, SourceForestTable, "binding_uuid", binding) &&
          hasUuid(row, SourceForestTable, "unit_uuid", stamp.unit) &&
          hasUuid(row, SourceForestTable, "forest_uuid", stamp.currentForest)
      }
      require(signed(forest, SourceForestTable, "source_epoch") == stamp.epoch &&
        token(forest, SourceForestTable, "forest_state") == "SEALED" &&
        token(forest, SourceForestTable, "forest_codec") == "FP_SOURCE_FOREST_BE_V2",
        "actual SOURCE R0 UnitExpectation differs from its exact current SEALED forest row")
      var duplicateUnits = 0
      request.units.foreach(other => if other.unit == stamp.unit then duplicateUnits += 1)
      require(duplicateUnits == 1, "actual SOURCE R0 UnitExpectations contain a duplicate unit identity")
      unitIndex += 1

    var obligationCount = 0
    obligationRows.foreach { row =>
      if hasUuid(row, SourceObligationTable, "binding_uuid", binding) then
        val hashMatches = value(row, SourceObligationTable, "carrier_key_hash") match
          case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(request.carrierIdentity.resourceKey.keyHashSha256)
          case _ => false
        val fullKeyMatches = value(row, SourceObligationTable, "carrier_key") match
          case MySqlFormat3Cell.Binary(key) =>
            sourceReserveResourceKeyMatches(request.carrierIdentity.resourceKey, key)
          case _ => false
        if hashMatches then
          require(value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER") &&
            fullKeyMatches,
            "actual SOURCE R0 obligation hash candidate differs from its complete CARRIER key")
        if fullKeyMatches then
          require(hashMatches &&
            value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER"),
            "actual SOURCE R0 obligation full key differs from its exact CARRIER kind/hash")
          val state = token(row, SourceObligationTable, "obligation_state")
          if state == "DISCHARGED" then
            throw new java.sql.SQLDataException(
              "source R0 qualification BLOCKED: DISCHARGED carrier obligations require the missing typed original-history discharge-proof interpreter")
          require(state == "OPEN",
            "actual SOURCE R0 request cannot qualify an unsupported same-CARRIER obligation state")
          val unit = sourceReserveUuid(row, SourceObligationTable, "unit_uuid")
          val forest = sourceReserveUuid(row, SourceObligationTable, "forest_uuid")
          val purpose = token(row, SourceObligationTable, "purpose")
          val revision = signed(row, SourceObligationTable, "obligation_revision")
          var stampMatches = 0
          request.obligations.foreach { stamp =>
            if stamp != null && stamp.key.binding == binding && stamp.key.unit == unit &&
                stamp.key.forest == forest && stamp.key.purpose == purpose then
              stampMatches = Math.addExact(stampMatches, 1)
              require(stamp.beforeState == "OPEN" && stamp.key.revision == revision &&
                stamp.key.carrierKeyHash == request.carrierIdentity.resourceKey.keyHashSha256 &&
                sourceReserveResourceKeyMatches(request.carrierIdentity.resourceKey, stamp.key.carrierKey),
                "actual SOURCE R0 obligation row differs from its exact typed U/O request stamp")
          }
          require(stampMatches == 1,
            "actual SOURCE R0 same-CARRIER OPEN obligation is missing or duplicated in its typed request")
          obligationCount = Math.addExact(obligationCount, 1)
    }
    require(obligationCount == request.obligations.size && obligationCount > 0,
      "actual SOURCE R0 request must retain every complete same-CARRIER OPEN obligation row")
    request.obligations.foreach { obligation =>
      var unitFound = 0
      request.units.foreach(stamp => if stamp.unit == obligation.key.unit then unitFound += 1)
      require(obligation.beforeState == "OPEN" && unitFound == 1,
        "actual SOURCE R0 U/O lists differ from the exact nonempty OPEN obligation unit set")
    }

    request.resources.foreach { entry =>
      val resource = entry.resource
      require(resource != null && (resource.exactProfile eq root.profile) &&
        (resource.resourceKind == "CARRIER" || resource.resourceKind == "SOURCE_UNIT"),
        "actual SOURCE R0 resources must use the original-profile exact CARRIER/SOURCE_UNIT set")
      var selected: Vector[MySqlFormat3Cell] = null
      var matches = 0
      reservationRows.foreach { row =>
        if hasUuid(row, ReservationTable, "binding_uuid", binding) then
          val hashMatches = value(row, ReservationTable, "resource_key_hash") match
            case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(resource.keyHashSha256)
            case _ => false
          val fullKeyMatches = value(row, ReservationTable, "resource_key") match
            case MySqlFormat3Cell.Binary(key) => sourceReserveResourceKeyMatches(resource, key)
            case _ => false
          if hashMatches then
            require(value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text(resource.resourceKind) &&
              fullKeyMatches,
              "actual SOURCE R0 reservation hash candidate differs from its complete resource key")
          if fullKeyMatches then
            require(hashMatches &&
              value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text(resource.resourceKind),
              "actual SOURCE R0 reservation full key differs from its exact kind/hash")
            matches = Math.addExact(matches, 1)
            selected = row
      }
      require(matches <= 1 && entry.before == Option(selected),
        "actual SOURCE R0 reservation R entry differs from exact full-key BEFORE presence/content")
      entry.before.foreach { row =>
        require(token(row, ReservationTable, "reservation_state") == "RELEASED",
          "actual SOURCE R0 cannot adopt an existing HELD reservation or guess its ownership")
      }
    }

  private def sourceReserveCreateReservationRow(request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      resource: MySqlRecoveryResourceKey, dataBudget: MySqlFormat3ReadBudget,
      shape: MySqlFormat3RelationShape,
      bindingCell: MySqlFormat3Cell, childCell: MySqlFormat3Cell,
      reserveCell: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    require(request != null && request.resources != null && request.resources.nonEmpty && resource != null,
      "SOURCE R0 reservation row construction requires one exact request resource key")
    require(shape != null && shape.table == ReservationTable && shape.columns.size == 11 &&
      bindingCell != null && childCell != null && reserveCell != null,
      "SOURCE R0 reservation row must retain its exact 11-column shape and precharged plan-child identity Cells")
    Vector(
      bindingCell,
      MySqlFormat3Cell.Text(resource.resourceKind),
      sourceReserveOwnedDigestCell(resource.keyHashSha256, dataBudget, "SOURCE R0 reservation full-key digest"),
      sourceReserveOwnedResourceKeyCell(resource, dataBudget, "SOURCE R0 reservation full key"),
      MySqlFormat3Cell.SignedInteger(0L),
      childCell,
      reserveCell,
      MySqlFormat3Cell.Text("HELD"),
      reserveCell,
      MySqlFormat3Cell.SqlNull,
      resource.sourceUnitUuid match
        case Some(unit) => sourceReserveOwnedUuidCell(unit, dataBudget, "SOURCE R0 source-unit reservation identity")
        case None => MySqlFormat3Cell.SqlNull)

  private def sourceReserveReacquireReservationRow(row: Vector[MySqlFormat3Cell],
      request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      dataBudget: MySqlFormat3ReadBudget, childCell: MySqlFormat3Cell,
      reserveCell: MySqlFormat3Cell, shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    require(row != null && request != null && dataBudget != null,
      "SOURCE R0 released reservation reacquisition requires its exact row, request and child")
    require(childCell != null && reserveCell != null,
      "SOURCE R0 released reservation reacquisition must share its exact precharged identity Cells")
    require(request.resources != null && request.resources.nonEmpty,
      "SOURCE R0 released reservation update requires its exact nonempty resource set")
    require(shape != null && shape.table == ReservationTable && shape.columns.size == 11 &&
      row.size == shape.columns.size,
      "SOURCE R0 released reservation must retain its exact original Full26 11-column shape")
    val revision = signed(row, ReservationTable, "reservation_revision")
    require(revision >= 0L, "SOURCE R0 released reservation revision must be nonnegative")
    Vector.tabulate(shape.columns.size) { index =>
      shape.columns(index).name match
        case "reservation_revision" => MySqlFormat3Cell.SignedInteger(Math.addExact(revision, 1L))
        case "workflow_uuid" => childCell
        case "operation_uuid" => reserveCell
        case "reservation_state" => MySqlFormat3Cell.Text("HELD")
        case "last_mutation_operation_uuid" => reserveCell
        case _ => row(index)
    }

  private def sourceReserveOperationRow(request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, identity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, resultAt: LocalDateTime,
      scope: MySqlNormalRecallCanonical.SourceChildReserveScopeData,
      preimage: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      derived: Vector[MySqlNormalRecallCanonical.SourceChildReserveDerivedFieldData],
      dataBudget: MySqlFormat3ReadBudget, bindingCell: MySqlFormat3Cell,
      workflowCell: MySqlFormat3Cell, reserveCell: MySqlFormat3Cell): Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectCell] =
    require(request != null && inputs != null && identity != null && scope != null && preimage != null &&
      derived != null && dataBudget != null && bindingCell != null && workflowCell != null && reserveCell != null,
      "SOURCE R0 pure O target reconstruction requires its exact request, Full26 inputs, history identity, DATA and keys")
    val shape = inputs.forTable(OperationTable)
    def derivedCell(column: String): MySqlNormalRecallCanonical.SourceChildReserveEffectCell =
      val matches = derived.filter(field => field.columnName == column)
      require(matches.size == 1,
        "SOURCE R0 current O requires one exact typed DerivedField for " + column)
      MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(matches.head)
    val requestSha = sourceReserveOwnedDigestCell(request.canonical.sha256, dataBudget,
      "SOURCE R0 O request identity digest")
    val scopeSha = sourceReserveOwnedDigestCell(scope.canonical.sha256, dataBudget,
      "SOURCE R0 O scope digest")
    val preimageSha = sourceReserveOwnedDigestCell(preimage.canonical.sha256, dataBudget,
      "SOURCE R0 O preimage digest")
    Vector.tabulate(shape.columns.size) { index =>
      val column = shape.columns(index).name
      column match
        case "binding_uuid" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(bindingCell)
        case "operation_uuid" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(reserveCell)
        case "workflow_uuid" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(workflowCell)
        case "step" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Text("RESERVE"))
        case "operation_state" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Text("COMMITTED"))
        case "pod_uuid" | "attempt_uuid" | "footprint_uuid" | "source_unit_uuid" | "forest_uuid" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.SqlNull)
        case "boot_uuid" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            sourceReserveOwnedUuidCell(identity.tag.bootId, dataBudget, "SOURCE R0 O boot identity"))
        case "boot_generation" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            MySqlFormat3Cell.SignedInteger(identity.tag.bootGeneration))
        case "connection_generation_uuid" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            sourceReserveOwnedUuidCell(identity.barrierGenerationUuid, dataBudget, "SOURCE R0 O barrier generation"))
        case "connection_id" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            MySqlFormat3Cell.SignedInteger(identity.actualConnectionId))
        case "fence_epoch" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            MySqlFormat3Cell.SignedInteger(identity.writerEpoch))
        case "request_identity_bytes" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            sourceReserveCanonicalCell(request.canonical, dataBudget, "SOURCE R0 O request bytes"))
        case "request_identity_sha256" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(requestSha)
        case "expected_domain_version" | "expected_locator_revision" | "expected_source_revision" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.SqlNull)
        case "context_encoding" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            MySqlFormat3Cell.Text(OperationContextEncoding))
        case "original_context_bytes" | "original_context_sha256" | "target_bytes" | "target_sha256" |
            "commit_witness_bytes" | "commit_witness_sha256" | "operation_fingerprint" =>
          derivedCell(column)
        case "scope_bytes" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            sourceReserveCanonicalCell(scope.canonical, dataBudget, "SOURCE R0 O scope bytes"))
        case "scope_sha256" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(scopeSha)
        case "preimage_bytes" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            sourceReserveCanonicalCell(preimage.canonical, dataBudget, "SOURCE R0 O preimage bytes"))
        case "preimage_sha256" => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(preimageSha)
        case "reserve_operation_uuid" | "previous_step_operation_uuid" | "settled_by_operation_uuid" |
            "original_diagnostic_sha256" |
            "original_diagnostic_identity" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.SqlNull)
        case "planned_at" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.DateTime(plannedAt))
        case "fixed_lease_until" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(
            MySqlFormat3Cell.DateTime(identity.fixedLeaseUntil))
        case "result_at" =>
          MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.DateTime(resultAt))
        case other => invalid("SOURCE R0 O has an unhandled frozen Full26 column " + other)
    }

  private def sourceReserveProjectedTargetRelations(plan: SourceChildReservePlanData,
      identity: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime, resultAt: LocalDateTime,
      scope: MySqlNormalRecallCanonical.SourceChildReserveScopeData,
      preimage: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      dataBudget: MySqlFormat3ReadBudget, bindingCell: MySqlFormat3Cell,
      workflowCell: MySqlFormat3Cell, reserveCell: MySqlFormat3Cell):
      Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectRelationData] =
    require(plan != null && plan.before != null && plan.root != null,
      "exact retained SOURCE R0 plan is required for its existing live projection wrapper")
    sourceReserveProjectedTargetRelationsData(plan.request, plan.before, plan.root, identity,
      plannedAt, resultAt, scope, preimage, dataBudget, bindingCell, workflowCell, reserveCell,
      requirePlanDataChild = true)

  /** The existing live R0 target delta, factored as a pure BEFORE/request/identity/time transform. */
  private def sourceReserveProjectedTargetRelationsData(
      request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      identity: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime, resultAt: LocalDateTime,
      scope: MySqlNormalRecallCanonical.SourceChildReserveScopeData,
      preimage: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      dataBudget: MySqlFormat3ReadBudget, bindingCell: MySqlFormat3Cell,
      workflowCell: MySqlFormat3Cell, reserveCell: MySqlFormat3Cell,
      requirePlanDataChild: Boolean): Vector[MySqlNormalRecallCanonical.SourceChildReserveEffectRelationData] =
    require(request != null && before != null && root != null && identity != null && scope != null &&
      preimage != null && dataBudget != null && bindingCell != null && workflowCell != null && reserveCell != null,
      "SOURCE R0 pure target reconstruction requires its exact BEFORE/request/identity/time and DATA owner")
    val inputs = before.inputs
    require(!root.isClosed && !root.isScopedReadBudget && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)) &&
      (!requirePlanDataChild || dataBudget.isCanonicalChildOf(root)) &&
      (scope.contextIdentity eq identity) && (scope.request eq request) && (scope.inputs eq inputs) &&
      (preimage.request eq request) && (preimage.inputs eq inputs) && (preimage.root eq root) &&
      (preimage.dataBudget eq dataBudget) && preimage.image == MySqlOperationEffectImage.Before &&
      (scope.canonical.borrowedBinary.storageBudget eq dataBudget),
      "SOURCE R0 pure target reconstruction received a foreign root/owner/scope/request/preimage")
    sourceReserveDateTime(plannedAt, "SOURCE R0 pure target plannedAt")
    sourceReserveDateTime(resultAt, "SOURCE R0 pure target resultAt")
    val operationFields = Vector(
      "original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
      "commit_witness_bytes", "commit_witness_sha256", "operation_fingerprint").map { column =>
      MySqlNormalRecallCanonical.sourceChildReserveProjectedTargetDerivedFieldData(request, inputs,
        MySqlOperationEffectImage.Target, OperationTable, column, root, dataBudget, requirePlanDataChild)
    }
    val writerFingerprint = MySqlNormalRecallCanonical.sourceChildReserveProjectedTargetDerivedFieldData(
      request, inputs, MySqlOperationEffectImage.Target, WriterTable, "last_commit_fingerprint",
      root, dataBudget, requirePlanDataChild)
    val newOperation = sourceReserveOperationRow(request, inputs, identity, plannedAt, resultAt,
      scope, preimage, operationFields, dataBudget, bindingCell, workflowCell, reserveCell)
    inputs.canonicalOrder.map { shape =>
      val original = before.relation(shape.table).rows
      val sourceRows = original.map { row =>
        row.map(cell => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell))
      }
      val projectedRows = shape.table match
        case OperationTable =>
          sourceReserveSortedEffectRows(sourceRows :+ newOperation, shape)
        case WriterTable =>
          val currentWriter = sourceReserveExactRow(before, WriterTable,
            "exact singleton W row for SOURCE R0 target") { row =>
            value(row, WriterTable, "singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L)
          }
          require(original.size == 1 && original.head == currentWriter,
            "SOURCE R0 projected W must preserve the exact original singleton row")
          Vector(Vector.tabulate(shape.columns.size) { index =>
            shape.columns(index).name match
              case "last_commit_operation_uuid" =>
                MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(reserveCell)
              case "last_commit_fingerprint" =>
                MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(writerFingerprint)
              case _ => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(currentWriter(index))
          })
        case ReservationTable =>
          val replacedRows = original.map { row =>
            var matched: MySqlNormalRecallCanonical.SourceChildReserveResourceExpectation = null
            var matches = 0
            request.resources.foreach { entry =>
              if entry.before.exists(_ == row) then
                matches = Math.addExact(matches, 1)
                matched = entry
            }
            require(matches <= 1,
              "SOURCE R0 released reservation row is owned by more than one exact resource expectation")
            matched match
              case null =>
                row.map(cell => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell))
              case entry =>
                val updated = sourceReserveReacquireReservationRow(row, request, dataBudget,
                  workflowCell, reserveCell, shape)
                updated.map(cell => MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell))
          }
          val absentRows = request.resources.flatMap { entry =>
            if entry.before.nonEmpty then None
            else
              val created = sourceReserveCreateReservationRow(request, entry.resource, dataBudget,
                shape, bindingCell, workflowCell, reserveCell)
              Some(created.map(cell =>
                MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell)))
          }
          sourceReserveSortedEffectRows(replacedRows ++ absentRows, shape)
        case _ => sourceRows
      MySqlNormalRecallCanonical.SourceChildReserveEffectRelationData(shape.table, projectedRows)
    }

  private def sourceReserveTargetRowCounts(plan: SourceChildReservePlanData): (Vector[Int], Long) =
    require(plan != null, "exact SOURCE R0 plan required for target row counts")
    sourceReserveTargetRowCounts(plan.request, plan.before)

  private def sourceReserveTargetRowCounts(
      request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      before: MySqlScriptSchemaSnapshot): (Vector[Int], Long) =
    require(request != null && before != null,
      "SOURCE R0 target row counts require the exact request and original Full26 BEFORE")
    val inputs = before.inputs
    var missingReservations = 0L
    request.resources.foreach(entry => if entry.before.isEmpty then
      missingReservations = Math.addExact(missingReservations, 1L))
    val counts = inputs.canonicalOrder.map { shape =>
      val current = before.relation(shape.table).rows.size
      shape.table match
        case OperationTable => Math.addExact(current, 1)
        case ReservationTable => Math.addExact(current, Math.toIntExact(missingReservations))
        case _ => current
    }
    val sortedRows = Math.addExact(counts(inputs.canonicalOrder.indexWhere(_.table == OperationTable)).toLong,
      counts(inputs.canonicalOrder.indexWhere(_.table == ReservationTable)).toLong)
    (counts, sortedRows)

  /** Builds only H1 typed DATA from the exact settled R0 plan and caller-provided real identity/times. */
  private[mysql] def sourceChildReservePrecommitWitnessData(
      plan: SourceChildReservePlanData, identity: MySqlRepositoryOperationContextIdentity,
      plannedAt: LocalDateTime, resultAt: LocalDateTime):
      MySqlNormalRecallCanonical.SourceChildReserveWitnessData =
    require(plan != null && plan.parent != null && plan.root != null && plan.allocation != null &&
      plan.intent != null,
      "exact retained SOURCE R0 plan DATA, parent, root, intent and allocation holder are required")
    plan.allocation.withRetainedPlanDataBudget(plan.parent, plan.root, plan.parent.actualLedger,
      plan.intent, plan) { dataBudget =>
      val inputs = requireSourceReservePlanLineage(plan, dataBudget)
      requireSourceReserveContextAndWriter(plan, identity, plannedAt)
      sourceReserveDateTime(resultAt, "SOURCE R0 resultAt")
      plan.beginWitnessAttempt()
      val requestValidationScratch = Math.addExact(1024L, Math.addExact(
        Math.multiplyExact(plan.before.rowCount, 32L), Math.addExact(
          Math.multiplyExact(plan.request.units.size.toLong, 128L), Math.addExact(
            Math.multiplyExact(plan.request.obligations.size.toLong, 192L),
            Math.multiplyExact(plan.request.resources.size.toLong, 384L)))))
      withSourceReserveScratch(plan.root, dataBudget, requestValidationScratch,
        "SOURCE R0 actual Full26 U/O/R request comparison and typed release evidence") {
        requireSourceReserveRequestMatchesBefore(plan, dataBudget)
      }
      val scope = MySqlNormalRecallCanonical.sourceChildReserveScopeData(identity,
        plan.request, inputs, plan.root, dataBudget)
      val preimage = MySqlNormalRecallCanonical.sourceChildReserveBeforeEffectData(
        plan.before, plan.request, inputs, plan.root, dataBudget)
      val (targetRows, sortRows) = sourceReserveTargetRowCounts(plan)
      val candidateScratch = sourceReserveEffectCandidateScratchBytes(inputs, targetRows, sortRows)
      withSourceReserveScratch(plan.root, dataBudget, candidateScratch,
        "SOURCE R0 full 26-table typed TARGET candidate, sort workspace and canonical deep copy") {
        val bindingCell = sourceReserveOwnedUuidCell(plan.request.binding, dataBudget,
          "SOURCE R0 request binding")
        val workflowCell = sourceReserveOwnedUuidCell(plan.request.childWorkflow, dataBudget,
          "SOURCE R0 child workflow")
        val reserveCell = sourceReserveOwnedUuidCell(plan.request.reserveOperation, dataBudget,
          "SOURCE R0 reserve operation")
        val relations = sourceReserveProjectedTargetRelations(plan, identity, plannedAt, resultAt,
          scope, preimage, dataBudget, bindingCell, workflowCell, reserveCell)
        val projectedTarget = MySqlNormalRecallCanonical.sourceChildReserveProjectedTargetEffectData(
          plan.request, inputs, relations, plan.root, dataBudget)
        val fingerprint = MySqlNormalRecallCanonical.sourceChildReservePhaseFingerprintData(
          scope, preimage, projectedTarget, plan.root, dataBudget)
        val originalContext = MySqlNormalRecallCanonical.sourceChildReserveOriginalContextData(
          fingerprint, plan.root, dataBudget)
        val witness = MySqlNormalRecallCanonical.sourceChildReserveWitnessData(
          originalContext, resultAt, plan.root, dataBudget)
        plan.finishWitnessAttempt(witness, dataBudget)
        witness
      }
    }


  private def sourceReservePrecommitDerivedOrdinal(table: String, column: String): Int =
    if table == OperationTable then
      column match
        case "original_context_bytes" => 0
        case "original_context_sha256" => 1
        case "target_bytes" => 2
        case "target_sha256" => 3
        case "commit_witness_bytes" => 4
        case "commit_witness_sha256" => 5
        case "operation_fingerprint" => 6
        case _ => -1
    else if table == WriterTable && column == "last_commit_fingerprint" then 7
    else -1

  private def requireSourceReservePreimageMatchesPlan(plan: SourceChildReservePlanData,
      preimage: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      inputs: MySqlScriptSchemaInputs): Unit =
    val shapes = inputs.canonicalOrder
    require(preimage.relations.size == shapes.size,
      "SOURCE R0 witness BEFORE must retain the complete original Full26 relation vector")
    var relationIndex = 0
    while relationIndex < shapes.size do
      val shape = shapes(relationIndex)
      val expected = plan.before.relation(shape.table)
      val actual = preimage.relations(relationIndex)
      require(actual.table == shape.table && actual.rows.size == expected.rows.size,
        "SOURCE R0 witness BEFORE relation differs from its exact plan Full26 predecessor")
      var rowIndex = 0
      while rowIndex < expected.rows.size do
        val expectedRow = expected.rows(rowIndex)
        val actualRow = actual.rows(rowIndex)
        require(actualRow.size == shape.columns.size && expectedRow.size == shape.columns.size,
          "SOURCE R0 witness BEFORE row must retain every original Full26 column")
        var columnIndex = 0
        while columnIndex < shape.columns.size do
          actualRow(columnIndex) match
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell) =>
              require(cell == expectedRow(columnIndex),
                "SOURCE R0 witness BEFORE literal differs from the exact plan Full26 predecessor")
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(_) =>
              invalid("SOURCE R0 witness BEFORE cannot contain a DerivedField")
          columnIndex += 1
        rowIndex += 1
      relationIndex += 1

  private def requireSourceReserveProjectedDerivedBindings(plan: SourceChildReservePlanData,
      projected: MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      inputs: MySqlScriptSchemaInputs): Unit =
    val shapes = inputs.canonicalOrder
    require(projected.relations.size == shapes.size,
      "SOURCE R0 witness projected TARGET must retain the complete original Full26 relation vector")
    var seen = 0
    var relationIndex = 0
    while relationIndex < shapes.size do
      val shape = shapes(relationIndex)
      val relation = projected.relations(relationIndex)
      require(relation.table == shape.table,
        "SOURCE R0 projected TARGET relation order differs from the exact Full26 inputs")
      var rowIndex = 0
      while rowIndex < relation.rows.size do
        val row = relation.rows(rowIndex)
        require(row.size == shape.columns.size,
          "SOURCE R0 projected TARGET row must retain every original Full26 column")
        var columnIndex = 0
        while columnIndex < row.size do
          row(columnIndex) match
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(_) => ()
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(field) =>
              val column = shape.columns(columnIndex)
              val ordinal = sourceReservePrecommitDerivedOrdinal(shape.table, column.name)
              require(ordinal >= 0 && field != null && field.tableName == shape.table &&
                field.columnName == column.name && (field.request eq plan.request) &&
                (field.inputs eq inputs) && field.image == MySqlOperationEffectImage.Target &&
                field.columnSqlDeclaration == column.sqlDeclaration && field.primaryKey != null &&
                field.primaryKey.size == shape.primaryKey.size,
                "SOURCE R0 projected TARGET contains an unknown, misplaced or foreign DerivedField")
              val bit = 1 << ordinal
              require((seen & bit) == 0,
                "SOURCE R0 projected TARGET contains a duplicate exact O/W DerivedField")
              seen |= bit
              var keyIndex = 0
              while keyIndex < shape.primaryKey.size do
                val keyName = shape.primaryKey(keyIndex)
                val rowKey = sourceReserveEffectCellValue(row,
                  sourceReserveFieldIndex(shape, keyName),
                  "SOURCE R0 projected TARGET DerivedField primary key")
                require(sourceReserveComparePrimaryKeyCell(rowKey, field.primaryKey(keyIndex),
                  shape.table, keyName) == 0,
                  "SOURCE R0 projected TARGET DerivedField primary key differs from its exact row")
                keyIndex += 1
          columnIndex += 1
        rowIndex += 1
      relationIndex += 1
    require(seen == 0xff,
      "SOURCE R0 projected TARGET must contain each of its exact seven O and singleton W DerivedFields")

  private def requireSourceReserveWitnessBoundToPlan(plan: SourceChildReservePlanData,
      witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
      inputs: MySqlScriptSchemaInputs, dataBudget: MySqlFormat3ReadBudget):
      MySqlNormalRecallCanonical.SourceChildReserveEffectData =
    require(witness != null && inputs != null && dataBudget != null &&
      (witness.root eq plan.root) && (witness.dataBudget eq dataBudget) &&
      witness.originalContext != null && (witness.originalContext.root eq plan.root) &&
      (witness.originalContext.dataBudget eq dataBudget) &&
      witness.originalContext.fingerprint != null &&
      (witness.originalContext.fingerprint.root eq plan.root) &&
      (witness.originalContext.fingerprint.dataBudget eq dataBudget),
      "SOURCE R0 B requires the exact A witness chain, original root and retained plan DATA child")
    val fingerprint = witness.originalContext.fingerprint
    val scope = fingerprint.scope
    val preimage = fingerprint.preimage
    val projected = fingerprint.projectedTarget
    require(scope != null && (scope.request eq plan.request) && (scope.inputs eq inputs) &&
      preimage != null && (preimage.request eq plan.request) && (preimage.inputs eq inputs) &&
      (preimage.root eq plan.root) && (preimage.dataBudget eq dataBudget) &&
      preimage.image == MySqlOperationEffectImage.Before &&
      projected != null && (projected.request eq plan.request) && (projected.inputs eq inputs) &&
      (projected.root eq plan.root) && (projected.dataBudget eq dataBudget) &&
      projected.image == MySqlOperationEffectImage.Target,
      "SOURCE R0 B witness scope, BEFORE and projected TARGET must retain this plan's exact request/root/inputs/child")
    MySqlNormalRecallCanonical.encodeSourceChildReserveWitnessData(witness, plan.root, dataBudget)
    val validationScratch = Math.addExact(512L,
      Math.multiplyExact(plan.before.rowCount, 24L))
    withSourceReserveScratch(plan.root, dataBudget, validationScratch,
      "SOURCE R0 exact A witness plan/Full26 BEFORE and eight DerivedField binding checks") {
      requireSourceReservePreimageMatchesPlan(plan, preimage, inputs)
      requireSourceReserveProjectedDerivedBindings(plan, projected, inputs)
    }
    projected

  private def sourceReservePrepaidDigestCell(digest: Vector[Byte],
      dataBudget: MySqlFormat3ReadBudget, label: String): MySqlFormat3Cell =
    require(digest != null && digest.size == 32 && dataBudget != null && !dataBudget.isClosed && label != null,
      "SOURCE R0 precommit derived digest requires its exact SHA-256 and live plan child")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest.toArray, dataBudget))

  private def sourceReservePrecommitDerivedCell(field:
      MySqlNormalRecallCanonical.SourceChildReserveDerivedFieldData,
      witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
      dataBudget: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    val originalContext = witness.originalContext
    val fingerprint = originalContext.fingerprint
    sourceReservePrecommitDerivedOrdinal(field.tableName, field.columnName) match
      case 0 => sourceReserveCanonicalCell(originalContext.canonical, dataBudget,
        "SOURCE R0 expected O original context bytes")
      case 1 => sourceReservePrepaidDigestCell(originalContext.canonical.sha256, dataBudget,
        "SOURCE R0 expected O original context SHA")
      case 2 => sourceReserveCanonicalCell(fingerprint.projectedTarget.canonical, dataBudget,
        "SOURCE R0 expected O projected TARGET bytes")
      case 3 => sourceReservePrepaidDigestCell(fingerprint.projectedTarget.canonical.sha256, dataBudget,
        "SOURCE R0 expected O projected TARGET SHA")
      case 4 => sourceReserveCanonicalCell(witness.canonical, dataBudget,
        "SOURCE R0 expected O commit witness bytes")
      case 5 => sourceReservePrepaidDigestCell(witness.canonical.sha256, dataBudget,
        "SOURCE R0 expected O commit witness SHA")
      case 6 => sourceReservePrepaidDigestCell(fingerprint.canonical.sha256, dataBudget,
        "SOURCE R0 expected O phase fingerprint")
      case 7 => sourceReservePrepaidDigestCell(originalContext.context.canonicalFingerprintSha256,
        dataBudget, "SOURCE R0 expected W original context fingerprint")
      case _ => invalid("SOURCE R0 precommit TARGET contains an unknown DerivedField")

  private def sourceReservePrecommitTargetRelations(projected:
      MySqlNormalRecallCanonical.SourceChildReserveEffectData,
      witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData,
      inputs: MySqlScriptSchemaInputs, rowCounts: Vector[Int],
      dataBudget: MySqlFormat3ReadBudget): Vector[MySqlFormat3RelationSnapshot] =
    val shapes = inputs.canonicalOrder
    require(projected.relations.size == shapes.size && rowCounts != null && rowCounts.size == shapes.size,
      "SOURCE R0 precommit TARGET requires every exact projected Full26 relation and target row count")
    Vector.tabulate(shapes.size) { relationIndex =>
      val shape = shapes(relationIndex)
      val projectedRelation = projected.relations(relationIndex)
      require(projectedRelation.table == shape.table &&
        projectedRelation.rows.size == rowCounts(relationIndex),
        "SOURCE R0 precommit TARGET row count/order differs from its exact projected Full26 relation")
      val rows = Vector.tabulate(projectedRelation.rows.size) { rowIndex =>
        val projectedRow = projectedRelation.rows(rowIndex)
        require(projectedRow.size == shape.columns.size,
          "SOURCE R0 precommit TARGET row must retain every original Full26 column")
        Vector.tabulate(shape.columns.size) { columnIndex =>
          projectedRow(columnIndex) match
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell) => cell
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(field) =>
              sourceReservePrecommitDerivedCell(field, witness, dataBudget)
        }
      }
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }

  /** Full strict H1 effect proof shared with Recovery; it has no live-plan or commit-stage input. */
  private[mysql] def verifyHistoricalSourceChildReserveDelta(
      decoded: MySqlNormalRecallCanonical.HistoricalSourceChildReserveReceiptData,
      before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    require(decoded != null && before != null && target != null && root != null && !root.isClosed &&
      !root.isScopedReadBudget && dataBudget != null && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)) &&
      (decoded.root eq root) && (decoded.dataBudget eq dataBudget),
      "historical SOURCE R0 delta requires its exact complete candidate, images, original root and explicit DATA owner")
    val beforeFull = requireFull26(before, root)
    val targetFull = requireFull26(target, root)
    val request = decoded.request
    val inputs = decoded.scope.inputs
    require((beforeFull.inputs eq inputs) && (targetFull.inputs eq inputs) &&
      (decoded.preimage.inputs eq inputs) && (decoded.projectedTarget.inputs eq inputs) &&
      (decoded.preimage.request eq request) && (decoded.projectedTarget.request eq request) &&
      decoded.preimage.image == MySqlOperationEffectImage.Before &&
      decoded.projectedTarget.image == MySqlOperationEffectImage.Target &&
      beforeFull.schema == targetFull.schema && beforeFull.connection == targetFull.connection &&
      beforeFull.schema == decoded.scope.commonScope.schema &&
      beforeFull.connection.serverUuid == decoded.scope.commonScope.server &&
      beforeFull.connection.serverVersion == decoded.scope.commonScope.stableSession.serverVersion &&
      beforeFull.connection.effectiveUser == decoded.scope.commonScope.stableSession.effectiveUser,
      "historical SOURCE R0 images, H1 request and original schema5 database identity differ")

    val shapes = inputs.canonicalOrder
    require(decoded.preimage.relations.size == shapes.size && beforeFull.relations.size == shapes.size,
      "historical SOURCE R0 BEFORE must retain the complete Full26 relation vector")
    val beforeScratch = Math.addExact(512L, Math.multiplyExact(beforeFull.rowCount, 32L))
    withHistoricalSourceReserveScratch(root, dataBudget, beforeScratch,
      "historical SOURCE R0 exact literal BEFORE image comparison") {
      var relationIndex = 0
      while relationIndex < shapes.size do
        val shape = shapes(relationIndex)
        val expected = decoded.preimage.relations(relationIndex)
        val actual = beforeFull.relations(relationIndex)
        require(expected.table == shape.table && actual.table == shape.table &&
          expected.rows.size == actual.rows.size,
          "historical SOURCE R0 BEFORE relation order/rows differ from its complete Full26 image")
        var rowIndex = 0
        while rowIndex < expected.rows.size do
          val expectedRow = expected.rows(rowIndex)
          val actualRow = actual.rows(rowIndex)
          require(expectedRow.size == shape.columns.size && actualRow.size == shape.columns.size,
            "historical SOURCE R0 BEFORE must retain every exact Full26 column")
          var columnIndex = 0
          while columnIndex < shape.columns.size do
            expectedRow(columnIndex) match
              case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(cell) =>
                require(cell == actualRow(columnIndex),
                  "historical SOURCE R0 BEFORE literal differs from its complete predecessor image")
              case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(_) =>
                invalid("historical SOURCE R0 BEFORE cannot contain a DerivedField")
            columnIndex += 1
          rowIndex += 1
        relationIndex += 1
    }

    val requestValidationScratch = Math.addExact(1024L, Math.addExact(
      Math.multiplyExact(beforeFull.rowCount, 32L), Math.addExact(
        Math.multiplyExact(request.units.size.toLong, 128L), Math.addExact(
          Math.multiplyExact(request.obligations.size.toLong, 192L),
          Math.multiplyExact(request.resources.size.toLong, 384L)))))
    withHistoricalSourceReserveScratch(root, dataBudget, requestValidationScratch,
      "historical SOURCE R0 Full26 U/O/R preimage comparison") {
      requireSourceReserveRequestMatchesBeforeData(request, beforeFull, root, dataBudget)
    }
    val expectedPreimage = MySqlNormalRecallCanonical.sourceChildReserveBeforeEffectData(
      beforeFull, request, inputs, root, dataBudget, requirePlanDataChild = false)
    require(expectedPreimage.canonical.borrowedBinary == decoded.preimage.canonical.borrowedBinary,
      "historical SOURCE R0 stored BEFORE effect differs from its exact actual Full26 predecessor")
    MySqlNormalRecallRecovery.verifyHistoricalSourceChildReserveParentP2(beforeFull,
      request, root, dataBudget)
    val scope = decoded.scope.typedScopeData
    require((scope.request eq request) && (scope.inputs eq inputs) &&
      (scope.contextIdentity.exactLimitProfile eq root.profile),
      "historical SOURCE R0 projected delta requires its exact DATA-parsed scope identity and Full26 request")
    def dateTime(column: String): LocalDateTime = value(decoded.row, OperationTable, column) match
      case MySqlFormat3Cell.DateTime(at) => at
      case _ => throw new java.sql.SQLDataException("historical SOURCE R0 O." + column + " must retain its exact DateTime(6)")
    val plannedAt = dateTime("planned_at")
    val resultAt = dateTime("result_at")
    sourceReserveDateTime(plannedAt, "historical SOURCE R0 original plannedAt")
    sourceReserveDateTime(resultAt, "historical SOURCE R0 original resultAt")

    // Rebuild the legal post-R0 relation graph from the independently matched BEFORE,
    // decoded request and envelope identity before trusting any stored TARGET-derived bytes.
    val countsScratch = Math.addExact(512L,
      Math.multiplyExact(inputs.canonicalOrder.size.toLong, 32L))
    withHistoricalSourceReserveScratch(root, dataBudget, countsScratch,
      "historical SOURCE R0 target row-count vector") {
      val (rowCounts, sortRows) = sourceReserveTargetRowCounts(request, beforeFull)
      val projectionScratch = sourceReserveEffectCandidateScratchBytes(inputs, rowCounts, sortRows)
      withHistoricalSourceReserveScratch(root, dataBudget, projectionScratch,
        "historical SOURCE R0 BEFORE-derived projected Full26 graph") {
        val bindingCell = sourceReserveOwnedUuidCell(request.binding, dataBudget,
          "historical SOURCE R0 projected binding")
        val workflowCell = sourceReserveOwnedUuidCell(request.childWorkflow, dataBudget,
          "historical SOURCE R0 projected child workflow")
        val reserveCell = sourceReserveOwnedUuidCell(request.reserveOperation, dataBudget,
          "historical SOURCE R0 projected reserve operation")
        val projectedRelations = sourceReserveProjectedTargetRelationsData(request, beforeFull, root,
          scope.contextIdentity, plannedAt, resultAt, scope, expectedPreimage, dataBudget,
          bindingCell, workflowCell, reserveCell, requirePlanDataChild = false)
        val expectedProjected = MySqlNormalRecallCanonical.sourceChildReserveProjectedTargetEffectData(
          request, inputs, projectedRelations, root, dataBudget, requirePlanDataChild = false)
        val expectedFingerprint = MySqlNormalRecallCanonical.sourceChildReservePhaseFingerprintData(
          scope, expectedPreimage, expectedProjected, root, dataBudget, requirePlanDataChild = false)
        val expectedContext = MySqlNormalRecallCanonical.sourceChildReserveOriginalContextData(
          expectedFingerprint, root, dataBudget, requirePlanDataChild = false)
        val expectedWitness = MySqlNormalRecallCanonical.sourceChildReserveWitnessData(
          expectedContext, resultAt, root, dataBudget, requirePlanDataChild = false)
        require(expectedProjected.canonical.borrowedBinary == decoded.projectedTarget.canonical.borrowedBinary &&
          expectedFingerprint.canonical.borrowedBinary == decoded.fingerprint.canonical.borrowedBinary &&
          expectedContext.canonical.borrowedBinary == decoded.originalContext.canonical.borrowedBinary &&
          expectedWitness.canonical.borrowedBinary == decoded.witness.canonical.borrowedBinary,
          "historical SOURCE R0 stored TARGET/fingerprint/context/witness differs from its exact BEFORE-derived live delta")

        val actualTargetEffect = MySqlNormalRecallCanonical.sourceChildReserveVerifiedHistoricalActualTargetEffectData(
          targetFull, expectedWitness, root, dataBudget)
        require(actualTargetEffect.canonical.borrowedBinary == expectedProjected.canonical.borrowedBinary,
          "historical SOURCE R0 actual O/R/W TARGET differs from its exact BEFORE-derived projected effect")
        val expectedTargetScratch = sourceReserveEffectCandidateScratchBytes(inputs, rowCounts, sortRows)
        withHistoricalSourceReserveScratch(root, dataBudget, expectedTargetScratch,
          "historical SOURCE R0 exact projected O/R/W literal TARGET cells") {
          val expectedTarget = sourceReservePrecommitTargetRelations(expectedProjected,
            expectedWitness, inputs, rowCounts, dataBudget)
          require(expectedTarget == targetFull.relations,
            "historical SOURCE R0 complete O/R/W and unchanged Full26 TARGET differs from the legal BEFORE-derived delta")
        }
      }
    }

  private def sourceReservePrecommitSnapshotRetainedBytes(inputs: MySqlScriptSchemaInputs,
      rowCounts: Vector[Int]): Long =
    require(inputs != null && rowCounts != null && rowCounts.size == inputs.canonicalOrder.size,
      "SOURCE R0 expected snapshot retention precharge requires every exact Full26 relation row count")
    var total = Math.addExact(512L, Math.addExact(16L, Math.multiplyExact(8L, 96L)))
    inputs.canonicalOrder.zip(rowCounts).foreach { (shape, rowCount) =>
      require(rowCount >= 0, "SOURCE R0 expected snapshot row count cannot be negative")
      val perRow = Math.addExact(192L, Math.multiplyExact(shape.columns.size.toLong, 64L))
      total = Math.addExact(total, Math.addExact(256L, Math.multiplyExact(rowCount.toLong, perRow)))
    }
    total

  /** Same A witness -> one fully literal Full26 precommit snapshot; no DB read or receipt is produced. */
  private[mysql] def sourceChildReservePrecommitTargetSnapshotData(
      plan: SourceChildReservePlanData,
      witness: MySqlNormalRecallCanonical.SourceChildReserveWitnessData): MySqlScriptSchemaSnapshot =
    require(plan != null && plan.parent != null && plan.root != null && plan.allocation != null &&
      plan.intent != null && witness != null,
      "exact retained SOURCE R0 plan and its returned A witness DATA are required")
    plan.allocation.withRetainedPlanDataBudget(plan.parent, plan.root, plan.parent.actualLedger,
      plan.intent, plan) { dataBudget =>
      val inputs = requireSourceReservePlanLineage(plan, dataBudget)
      plan.requireExpectedSnapshotWitness(witness)
      val projected = requireSourceReserveWitnessBoundToPlan(plan, witness, inputs, dataBudget)
      plan.beginExpectedSnapshotAttempt(witness)
      val countScratch = Math.addExact(512L,
        Math.multiplyExact(inputs.canonicalOrder.size.toLong, 32L))
      withSourceReserveScratch(plan.root, dataBudget, countScratch,
        "SOURCE R0 exact precommit target relation-count vector") {
        val (rowCounts, sortRows) = sourceReserveTargetRowCounts(plan)
        val candidateScratch = sourceReserveEffectCandidateScratchBytes(inputs, rowCounts, sortRows)
        val retainedBytes = sourceReservePrecommitSnapshotRetainedBytes(inputs, rowCounts)
        withSourceReserveScratch(plan.root, dataBudget, candidateScratch,
          "SOURCE R0 full literal Full26 precommit target and canonical reprojection peak") {
          dataBudget.reserveRetainedMemory(retainedBytes,
            "SOURCE R0 full literal precommit TARGET rows/vectors/cells/hash copies and plan retention BEFORE allocation")
          val relations = sourceReservePrecommitTargetRelations(projected, witness,
            inputs, rowCounts, dataBudget)
          plan.retainExpectedSnapshotRelations(relations)
          val before = plan.before
          val snapshot = MySqlScriptSchemaSnapshot.decoded(before.schema, before.connection,
            before.observationScope, before.maxAllowedPacketBytes, inputs, relations, dataBudget)
          plan.retainExpectedSnapshot(relations, snapshot, dataBudget)
          MySqlFormat3QueryProjection.requireDeploymentSource(snapshot, inputs, plan.root)
          MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(snapshot, inputs, plan.root)
          val verifiedEffect = MySqlNormalRecallCanonical.sourceChildReserveVerifiedActualTargetEffectData(
            snapshot, witness, plan.root, dataBudget)
          plan.retainVerifiedExpectedTargetEffect(verifiedEffect, dataBudget)
          plan.finishExpectedSnapshotAttempt(snapshot)
          snapshot
        }
      }
    }

  private def capturedObligationRows(write: MySqlNormalRecallCanonical.DecodedSourceWrite,
      operation: UUID, before: MySqlScriptSchemaSnapshot, prepare: Boolean,
      scope: MySqlNormalRecallCanonical.DecodedSourceWriteScope,
      root: MySqlFormat3ReadBudget): (Vector[Vector[MySqlFormat3Cell]], Vector[Vector[MySqlFormat3Cell]]) =
    val beforeRows = rows(before, SourceObligationTable)
    val targets = beforeRows.map { row =>
      val captured = write.obligationRevisionData.values.filter(item =>
        hasUuid(row, SourceObligationTable, "binding_uuid", item.binding) &&
          hasUuid(row, SourceObligationTable, "unit_uuid", item.unit) &&
          hasUuid(row, SourceObligationTable, "forest_uuid", item.forest) &&
          (value(row, SourceObligationTable, "carrier_key_hash") match
            case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(item.carrierKeyHash)
            case _ => false) && token(row, SourceObligationTable, "purpose") == item.purpose)
      if captured.isEmpty then row
      else
        val item = exactlyOne(captured, "captured source obligation PK")
        require(signed(row, SourceObligationTable, "obligation_revision") == item.revision,
          "captured source obligation revision changed before planning")
        val stamp = exactlyOne(scope.obligations.filter(stamp =>
          stamp.key.unit == item.unit && stamp.key.forest == item.forest && stamp.key.purpose == item.purpose),
          "source-write nested obligation state stamp")
        require(stamp.key.revision == item.revision &&
          token(row, SourceObligationTable, "obligation_state") == stamp.beforeState,
          "source-write nested obligation state/revision differs from its exact Full26 predecessor")
        value(row, SourceObligationTable, "carrier_key") match
          case MySqlFormat3Cell.Binary(key) =>
            require(key.compareTo(item.carrierKey) == 0, "captured source obligation full CARRIER key differs")
          case _ => invalid("captured source obligation full CARRIER key is absent")
        require(value(row, SourceObligationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER"),
          "captured source obligation resource kind differs")
        if !prepare || item.purpose == "CURRENT_HOLD" then row
        else
          require(token(row, SourceObligationTable, "obligation_state") == "OPEN" &&
            value(row, SourceObligationTable, "write_uuid") == MySqlFormat3Cell.SqlNull &&
            value(row, SourceObligationTable, "discharged_operation_uuid") == MySqlFormat3Cell.SqlNull &&
            value(row, SourceObligationTable, "discharge_proof_bytes") == MySqlFormat3Cell.SqlNull,
            "SOURCE_WRITE_PREPARE may attach only an existing open unassigned obligation")
          MySqlControlledDebugRows.update(row, SourceObligationTable, Map(
            "write_uuid" -> MySqlControlledDebugRows.uuid(write.write),
            "last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(operation)), root)
    }
    write.obligationRevisionData.values.foreach { item =>
      require(beforeRows.count(row => hasUuid(row, SourceObligationTable, "binding_uuid", item.binding) &&
        hasUuid(row, SourceObligationTable, "unit_uuid", item.unit) &&
        hasUuid(row, SourceObligationTable, "forest_uuid", item.forest) &&
        (value(row, SourceObligationTable, "carrier_key_hash") match
          case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(item.carrierKeyHash)
          case _ => false) && token(row, SourceObligationTable, "purpose") == item.purpose) == 1,
        "captured source obligation must have one exact original Full26 row")
    }
    if prepare then
      val expectedAttached = write.obligationRevisionData.values.filterNot(_.purpose == "CURRENT_HOLD")
      val attached = targets.filter(row => hasUuid(row, SourceObligationTable, "binding_uuid", write.binding) &&
        hasUuid(row, SourceObligationTable, "write_uuid", write.write))
      require(attached.size == expectedAttached.size && expectedAttached.forall(item => attached.count(row =>
        hasUuid(row, SourceObligationTable, "unit_uuid", item.unit) &&
          hasUuid(row, SourceObligationTable, "forest_uuid", item.forest) &&
          token(row, SourceObligationTable, "purpose") == item.purpose) == 1),
        "SOURCE_WRITE_PREPARE must attach the exact captured non-CURRENT_HOLD obligation set")
    else require(targets == beforeRows, "SOURCE_WRITE_RESULT and RECONCILE cannot mutate source obligations")
    (beforeRows, targets)

  private def planWrite(beforeImage: Image, targetWrite: MySqlNormalRecallCanonical.DecodedSourceWrite,
      operationRow: Vector[MySqlFormat3Cell], expectedStep: String,
      root: MySqlFormat3ReadBudget): SourceWritePlanData =
    require(targetWrite != null && targetWrite.row != null && root != null,
      "validated immutable source-write row and exact Full26 root required")
    val before = requireFull26(beforeImage, root)
    root.reserveRetainedMemory(Math.addExact(2048L, Math.multiplyExact(
      rows(before, ReservationTable).size.toLong + rows(before, SourceObligationTable).size.toLong, 128L)),
      "source-write complete reservation/obligation delta DATA")
    val operation = operationContext(operationRow, expectedStep, targetWrite, before, root)
    val scopeBytes = value(operationRow, OperationTable, "scope_bytes") match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => invalid("source operation nested scope bytes are absent")
    val sourceScope = MySqlNormalRecallCanonical.decodeSourceWriteScope(scopeBytes, targetWrite, root)
    val unitReservationPlan = sourceUnitReservations(targetWrite, operation, before, root)
    val (unitRows, forestRows, revisionData) =
      sourceContextRows(targetWrite, unitReservationPlan.unitIds, before, root)
    require(sourceScope.units.map(_.unit).toSet == unitReservationPlan.unitIds.toSet &&
      sourceScope.units.size == unitReservationPlan.unitIds.size,
      "source-write nested UnitStamp vector differs from the child's complete SOURCE_UNIT reservation set")
    sourceScope.units.foreach { stamp =>
      val unitRow = exactlyOne(unitRows.filter(row => hasUuid(row, SourceUnitTable, "unit_uuid", stamp.unit)),
        "source-write nested UnitStamp Full26 row")
      val currentEpoch = value(unitRow, SourceUnitTable, "current_epoch") match
        case MySqlFormat3Cell.SignedInteger(epoch) => Some(epoch)
        case MySqlFormat3Cell.SqlNull => None
        case _ => invalid("source-write UnitStamp Full26 current epoch is malformed")
      val currentForest = uuidCell(unitRow, SourceUnitTable, "current_forest_uuid", root)
      require(signed(unitRow, SourceUnitTable, "unit_revision") == stamp.unitRevision &&
        currentEpoch.contains(stamp.epoch) && currentForest.contains(stamp.currentForest),
        "source-write UnitStamp revision/epoch/current forest differs from its exact Full26 row")
    }
    val writeRowsBefore = rows(before, SourceWriteTable)
    val existing = writeRowsBefore.filter(row => hasUuid(row, SourceWriteTable, "binding_uuid", targetWrite.binding) &&
      hasUuid(row, SourceWriteTable, "write_uuid", targetWrite.write))
    val writeBefore = existing.headOption
    require(existing.size <= 1, "source write primary key is duplicated in the original Full26 image")
    val targetDecoded = MySqlNormalRecallCanonical.decodeSourceWrite(targetWrite.row, root)
    require(targetDecoded.hasSameImmutableIdentity(targetWrite), "source write row changed after typed decoding")
    val writeTarget = targetDecoded.row
    val isPrepare = expectedStep == "SOURCE_WRITE_PREPARE"
    if isPrepare then
      require(writeBefore.isEmpty && targetDecoded.state == "PREPARED" &&
        targetDecoded.preparedOperation == operation && targetDecoded.lastMutation == operation &&
        targetDecoded.resolvedOperation.isEmpty && targetDecoded.revision == 0L,
        "SOURCE_WRITE_PREPARE requires one absent PK and its original immutable PREPARED row")
      require(!writeRowsBefore.exists(row => hasUuid(row, SourceWriteTable, "binding_uuid", targetDecoded.binding) &&
        (value(row, SourceWriteTable, "carrier_key_hash") match
          case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(targetDecoded.resourceKey.keyHashSha256)
          case _ => false) && signed(row, SourceWriteTable, "write_generation") == targetDecoded.writeGeneration),
        "source carrier write generation is already present in the original Full26 image")
    else
      val oldRow = writeBefore.getOrElse(invalid("source result/reconcile requires the exact original prepared write row"))
      val old = MySqlNormalRecallCanonical.decodeSourceWrite(oldRow, root)
      require(old.state == "PREPARED" || (expectedStep == "SOURCE_WRITE_RECONCILE" &&
        old.state == "FAILED_NEEDS_RECONCILIATION"),
        "source result/reconcile cannot rewrite a resolved or terminal write")
      require(old.hasSameImmutableIdentity(targetDecoded) && old.preparedOperation == targetDecoded.preparedOperation &&
        targetDecoded.resolvedOperation.contains(operation) && targetDecoded.lastMutation == operation &&
        targetDecoded.revision == Math.addExact(old.revision, 1L) && targetDecoded.state != "PREPARED",
        "source result/reconcile changed its original prepared tuple or row revision")
      if old.state == "FAILED_NEEDS_RECONCILIATION" then
        require(old.failureDiagnostic == targetDecoded.failureDiagnostic &&
          old.failureDiagnosticSha256 == targetDecoded.failureDiagnosticSha256,
          "source reconcile must preserve the original failure diagnostic identity")
    val writeRowsTarget = if isPrepare then writeRowsBefore :+ writeTarget else
      writeRowsBefore.map(row => if row == writeBefore.get then writeTarget else row)
    require(writeRowsTarget.count(row => hasUuid(row, SourceWriteTable, "binding_uuid", targetDecoded.binding) &&
      hasUuid(row, SourceWriteTable, "write_uuid", targetDecoded.write)) == 1,
      "source write target relation must contain exactly its original primary key")
    val (carrierBefore, carrierTarget, reservationsBefore, _) =
      carrierReservation(targetDecoded, operation, before, root)
    val reservationTargets = (unitReservationPlan.beforeRows.zip(unitReservationPlan.targetRows) :+ (carrierBefore -> carrierTarget)).toMap
    val reservationsTarget = reservationsBefore.map(row => reservationTargets.getOrElse(row, row))
    val (obligationsBefore, obligationsTarget) =
      capturedObligationRows(targetDecoded, operation, before, isPrepare, sourceScope, root)
    val writeDelta = new RelationDelta(SourceWriteTable, writeRowsBefore, writeRowsTarget)
    val reservationDelta = new RelationDelta(ReservationTable, reservationsBefore, reservationsTarget)
    val obligationDelta = new RelationDelta(SourceObligationTable, obligationsBefore, obligationsTarget)
    new SourceWritePlanData(expectedStep, operationRow, unitRows, forestRows, writeBefore, writeTarget,
      carrierBefore, carrierTarget, unitReservationPlan.beforeRows, unitReservationPlan.targetRows,
      obligationsBefore, obligationsTarget, Vector(writeDelta, reservationDelta, obligationDelta),
      targetDecoded, sourceScope, revisionData)

  /** 建立SOURCE_WRITE_PREPARE DATA delta；实际SourceWriteInput和唯一许可发行仍由其owner/Fenced持有。 */
  def prepareWrite(before: Image, preparedRow: MySqlNormalRecallCanonical.DecodedSourceWrite,
      exactOperationRow: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): SourceWritePlanData =
    planWrite(before, preparedRow, exactOperationRow, "SOURCE_WRITE_PREPARE", root)

  /** 记录既有write的确定结果DATA delta；caller须另核真实PublishedSourceResult/Region evidence。 */
  def recordWriteResult(before: Image, resultRow: MySqlNormalRecallCanonical.DecodedSourceWrite,
      exactOperationRow: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): SourceWritePlanData =
    planWrite(before, resultRow, exactOperationRow, "SOURCE_WRITE_RESULT", root)

  /** 仅原write受控结算的DATA delta；不发新permit、不改变transfer phase。 */
  def reconcileWrite(before: Image, reconciledRow: MySqlNormalRecallCanonical.DecodedSourceWrite,
      exactOperationRow: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): SourceWritePlanData =
    planWrite(before, reconciledRow, exactOperationRow, "SOURCE_WRITE_RECONCILE", root)
