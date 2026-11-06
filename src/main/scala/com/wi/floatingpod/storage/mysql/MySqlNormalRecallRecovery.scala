package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import java.sql.SQLDataException
import java.util.UUID
import MySqlModeOperationEffectCodec.{Image,allocationBudget,limitProfile,schema,connection,observationScope,maxAllowedPacketBytes,relations,relation,rowCount}

/** Main只持原工作流的有限索引；这些DATA不证明C提交，也不准许physical动作。 */
private[floatingpod] final case class MySqlNormalRecallRecoveryReference(
    workflow: UUID, pod: UUID, prepare: UUID, checkpoint: Option[UUID],
    checkpointObservation: MySqlNormalRecallCheckpointObservation)

/** 仅原受限Boot的不可变观察；既不是negative settlement也不是physical许可。 */
private[floatingpod] enum MySqlNormalRecallCheckpointObservation:
  case NotReserved, ReservedNeedsSettlement, Committed, OriginalResponsibility

/** Full20 DATA 决定继续核哪个原操作；不签发来源独占、negative witness 或恢复许可。 */
private[mysql] enum MySqlNormalRecallRecoveryCursor:
  case ReconcileOriginal(operation: MySqlRecoveryOperationRawEnvelope)
  case VerifyUnsubmittedCheckpoint(prepare: MySqlRecoveryOperationRawEnvelope)
  case VerifyCommittedCheckpoint(checkpoint: MySqlRecoveryOperationRawEnvelope)
  case VerifyNegativeSettlement(checkpoint: MySqlRecoveryOperationRawEnvelope, settlement: MySqlRecoveryOperationRawEnvelope)
  case ResumeRecoveryPrepare(operation: MySqlRecoveryOperationRawEnvelope)
  case ResumeRecoveryPhysical(operation: MySqlRecoveryOperationRawEnvelope)
  case ReconcileRecoverySettlement(operation: MySqlRecoveryOperationRawEnvelope)
  case FinishRecoveryMain(operation: MySqlRecoveryOperationRawEnvelope)

/** 原locked full20中发现的责任索引；即使cursor指向COMMITTED，也没有行动/Ready资格。 */
private[mysql] final case class MySqlNormalRecallRecoveryInventory(
    workflow: UUID, pod: UUID, prepare: UUID, checkpoint: Option[UUID], cursor: MySqlNormalRecallRecoveryCursor)

private[mysql] object MySqlNormalRecallRecovery:
  private val OperationTable = "fp_recovery_operation"
  private val ReservationTable = "fp_recovery_reservation"
  private val CompletionTable = "fp_lifecycle_completion"
  private def exactInputs(image:Image,root:MySqlFormat3ReadBudget):Option[MySqlScriptSchemaInputs]=image match
    case legacy:MySqlFormat3FullSnapshot=>
      root.requireFormat3Full20Layout()
      require(legacy.allocationBudget.sharesAllocationLedger(root),"historical legacy image switched root")
      None
    case deployment:MySqlScriptSchemaSnapshot=>
      MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)
      Some(deployment.inputs)
  /** Read the exact immutable input reference only after the outer image-graph validator has succeeded. */
  private def inputsFromValidatedImage(image: Image): Option[MySqlScriptSchemaInputs] = image match
    case deployment: MySqlScriptSchemaSnapshot => Some(deployment.inputs)
    case _: MySqlFormat3FullSnapshot => None
  private def requireSameValidatedImageInputs(image: Image,
      inputs: Option[MySqlScriptSchemaInputs]): Unit =
    val sameReference = image match
      case deployment: MySqlScriptSchemaSnapshot => inputs != null && inputs.exists(_ eq deployment.inputs)
      case _: MySqlFormat3FullSnapshot => inputs != null && inputs.isEmpty
    require(sameReference, "validated full image inputs must retain the exact immutable source reference")
  private def requireHistoryDataBudget(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && dataBudget != null && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)),
      "historical receipt DATA must retain the original root and one direct same-ledger allocation owner")
  private def withHistoryComparisonBudget(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget)(body: => Unit): Unit =
    if dataBudget eq root then MySqlRecoveryCanonicalBytes.withComparisonBudget(root)(body)
    else MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget)(body)
  private def requirePlanHistoryDataBudget(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    requireHistoryDataBudget(root, dataBudget)
    require(dataBudget ne root, "H3 retained receipt history requires its exact original-root plan DATA child")
  private def requireImageGraph(image:Image,root:MySqlFormat3ReadBudget):Unit =
    exactInputs(image,root)
    image match
      case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugRelations.requireValidFullGraph(legacy,root)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(deployment,deployment.inputs,root)
  private def requireWriter(image:Image,writer:MySqlBusinessWriterRow,root:MySqlFormat3ReadBudget):Unit =
    exactInputs(image,root)
    image match
      case legacy:MySqlFormat3FullSnapshot=>MySqlFreshFormat3Graph.requireWriterMatches(legacy,writer)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlDeploymentReceiptBootstrapGraph.requireWriterMatches(deployment,writer)
  private def value(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row, table, name)
  private def isUuid(row: Vector[MySqlFormat3Cell], table: String, name: String, id: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(value(row, table, name), id)
  private def token(row: Vector[MySqlFormat3Cell], name: String): String = value(row, OperationTable, name) match
    case MySqlFormat3Cell.Text(text) => text
    case _ => fail("operation token missing: " + name)

  /** 只证明整行与严格scope/请求/context/witness相符，不证明effect的full20语义或C提交边界。 */
  final class PersistedPhaseData private[MySqlNormalRecallRecovery] (
      val row: Vector[MySqlFormat3Cell], val header: MySqlNormalRecallCanonical.DecodedHeader,
      val prepare: MySqlNormalRecallCanonical.DecodedPrepare,
      val checkpoint: Option[MySqlNormalRecallCanonical.DecodedCheckpoint],
      val phase: MySqlOperationEffectPhase, val context: MySqlOperationContext,
      val beforeEffect: MySqlNormalRecallCanonical.DecodedEffect,
      val targetEffect: MySqlNormalRecallCanonical.DecodedEffect)

  def verifyPersistedPhase(row: Vector[MySqlFormat3Cell], originalReserve: Option[Vector[MySqlFormat3Cell]],
      root: MySqlFormat3ReadBudget,deploymentInputs:Option[MySqlScriptSchemaInputs]=None): PersistedPhaseData =
    require(root != null && !root.isClosed && !root.isScopedReadBudget,
      "original open Recall phase DATA required")
    verifyPersistedPhaseOnBudget(row, originalReserve, root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), deploymentInputs)

  /** H3 direct history caller: decoded receipt/effect DATA is retained by the issuer's plan child. */
  private[mysql] def verifyPersistedPhase(row: Vector[MySqlFormat3Cell], originalReserve: Option[Vector[MySqlFormat3Cell]],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): PersistedPhaseData =
    requirePlanHistoryDataBudget(root, dataBudget)
    verifyPersistedPhaseOnBudget(row, originalReserve, root, dataBudget, deploymentInputs)

  private def verifyPersistedPhaseOnBudget(row: Vector[MySqlFormat3Cell], originalReserve: Option[Vector[MySqlFormat3Cell]],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs],
      activeComparisonBudget: MySqlFormat3ReadBudget = null,
      currentRawInputs: Option[MySqlScriptSchemaInputs] = null): PersistedPhaseData =
    requireHistoryDataBudget(root, dataBudget)
    require(row != null && originalReserve != null, "original Recall phase row and reserve option required")
    val rawBudget =
      if activeComparisonBudget == null then dataBudget
      else
        MySqlNormalRecallCanonical.requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
        activeComparisonBudget
    val phaseDataOwner = if activeComparisonBudget == null then dataBudget else activeComparisonBudget
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, rawBudget, if currentRawInputs == null then deploymentInputs else currentRawInputs)
    def binary(name: String): MySqlFormat3Binary = value(row, OperationTable, name) match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => fail("original phase binary absent: " + name)
    val scopeBytes = binary("scope_bytes")
    def hasDomain(domain: String): Boolean =
      scopeBytes.length >= 4 + domain.length &&
        (0 until 4).forall(i => (scopeBytes.byteAt(i) & 255) == ((domain.length >>> (24 - 8 * i)) & 255)) &&
        domain.indices.forall(i => scopeBytes.byteAt(4 + i) == domain.charAt(i).toByte)
    val checkpoint = if hasDomain(MySqlNormalRecallCanonical.CheckpointDomain) then
      Some(if activeComparisonBudget != null then
        MySqlNormalRecallCanonical.decodeCheckpointWithActiveHistoryComparisonOwner(
          scopeBytes, root, dataBudget, deploymentInputs, activeComparisonBudget)
      else if dataBudget eq root then
        MySqlNormalRecallCanonical.decodeCheckpoint(scopeBytes, root, deploymentInputs)
      else MySqlNormalRecallCanonical.decodeCheckpoint(scopeBytes, root, dataBudget, deploymentInputs))
    else None
    val prepared = checkpoint.map(_.prepare).getOrElse {
      if !hasDomain(MySqlNormalRecallCanonical.PrepareDomain) then fail("unsupported original Recall scope domain")
      if activeComparisonBudget != null then
        MySqlNormalRecallCanonical.decodePrepareWithActiveHistoryComparisonOwner(
          scopeBytes, root, dataBudget, deploymentInputs, activeComparisonBudget)
      else if dataBudget eq root then MySqlNormalRecallCanonical.decodePrepare(scopeBytes, root, deploymentInputs)
      else MySqlNormalRecallCanonical.decodePrepare(scopeBytes, root, dataBudget, deploymentInputs)
    }
    val h = checkpoint.map(_.header).getOrElse(prepared.header)
    val s = h.scope
    val step = token(row, "step")
    val phase = if step == "RESERVE" then MySqlOperationEffectPhase.Reserve else MySqlOperationEffectPhase.Execute
    val expectedStep = if checkpoint.nonEmpty then "RECALL_CHECKPOINT" else "RECALL_PREPARE"
    require(step == "RESERVE" || step == expectedStep, "original Recall operation/scope purpose differs")
    require((phase == MySqlOperationEffectPhase.Execute) == originalReserve.nonEmpty,
      "execute phase requires its actual original reserve row")
    val operation = if phase == MySqlOperationEffectPhase.Reserve then s.reserve else s.tag.operationId
    val identity = Map("binding_uuid" -> s.binding, "operation_uuid" -> operation, "workflow_uuid" -> s.workflow,
      "pod_uuid" -> h.pod, "attempt_uuid" -> h.attempt, "footprint_uuid" -> h.footprint,
      "boot_uuid" -> s.tag.bootId, "connection_generation_uuid" -> s.stableSession.barrier)
    require(identity.forall((column, id) => isUuid(row, OperationTable, column, id)), "original Recall row/scope identity differs")
    require(value(row, OperationTable, "boot_generation") == MySqlFormat3Cell.SignedInteger(s.tag.bootGeneration) &&
      value(row, OperationTable, "connection_id") == MySqlFormat3Cell.SignedInteger(s.connectionId) &&
      value(row, OperationTable, "fence_epoch") == MySqlFormat3Cell.SignedInteger(s.epoch) &&
      value(row, OperationTable, "expected_domain_version") == MySqlFormat3Cell.SignedInteger(h.version) &&
      value(row, OperationTable, "expected_locator_revision") == MySqlFormat3Cell.SignedInteger(h.locatorRevision) &&
      value(row, OperationTable, "fixed_lease_until") == MySqlFormat3Cell.DateTime(s.lease),
      "original Recall row/scope generation/version/lease differs")
    require(Vector("source_unit_uuid", "forest_uuid", "expected_source_revision", "settled_by_operation_uuid",
      "original_diagnostic_identity", "original_diagnostic_sha256").forall(n => value(row, OperationTable, n) == MySqlFormat3Cell.SqlNull),
      "unsettled or foreign Recall row must retain its own original owner")
    require(h.previous.fold(value(row, OperationTable, "previous_step_operation_uuid") == MySqlFormat3Cell.SqlNull)(
      isUuid(row, OperationTable, "previous_step_operation_uuid", _)), "Recall predecessor differs from scope")
    require(if phase == MySqlOperationEffectPhase.Reserve then value(row, OperationTable, "reserve_operation_uuid") == MySqlFormat3Cell.SqlNull
      else isUuid(row, OperationTable, "reserve_operation_uuid", s.reserve), "Recall original reserve ID differs")
    val state = token(row, "operation_state")
    require(state == "COMMITTED" || (phase == MySqlOperationEffectPhase.Execute && state == "RESERVED"),
      "cancelled/unknown Recall requires original reconciliation, not positive phase DATA")
    val at = value(row, OperationTable, "planned_at") match
      case MySqlFormat3Cell.DateTime(v) if v.getYear >= 1000 && v.getYear <= 9999 && v.getNano % 1000 == 0 && v.isBefore(s.lease) => v
      case _ => fail("original Recall planned timestamp differs")
    require(value(row, OperationTable, "result_at") == (if state == "COMMITTED" then MySqlFormat3Cell.DateTime(at) else MySqlFormat3Cell.SqlNull),
      "Recall result timestamp differs from original plan")
    phaseDataOwner.reserveRetainedMemory(2048L, "Recall verified phase DATA/context")
    val fingerprintDomain = "FP_NORMAL_RECALL_FINGERPRINT_BE_V1"
    def writeFingerprint(out: java.io.DataOutputStream): Unit =
      MySqlRecoveryCanonicalBytes.writeToken(out, if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
      Vector(s.workflow, s.reserve, s.tag.operationId).foreach(MySqlRecoveryCanonicalBytes.writeUuid(out, _))
      Vector(binary("request_identity_bytes"), scopeBytes, binary("preimage_bytes"), binary("target_bytes")).foreach { b =>
        out.writeInt(b.length); b.writeTo(out)
      }
    val fingerprint = MySqlRecoveryCanonicalBytes.withAllocationBudget(phaseDataOwner) {
      MySqlNormalRecallCanonical.fingerprintStream(fingerprintDomain, root)(writeFingerprint)
    }
    val fingerprintMatches = value(row, OperationTable, "operation_fingerprint") match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsVector(fingerprint.sha256)
      case _ => false
    require(fingerprintMatches, "Recall independent fingerprint differs")
    withHistoryComparisonBudget(root, dataBudget) {
      val request = requestFrom(prepared)
      val operationRequest = checkpoint match
        case Some(c) => MySqlRepositoryOperation.CheckpointNormalRecall(MySqlNormalRecallCheckpointRequest(request, c.removal, c.ticket, c.receipt))
        case None => MySqlRepositoryOperation.PrepareNormalRecall(request)
      val requestBytes = MySqlRepositoryRequestIdentity.single(operationRequest, root.profile).canonical
      require(requestBytes.exactlyMatchesCell(value(row, OperationTable, "request_identity_bytes")), "Recall stored request differs from strict scope DATA")
      val context = MySqlOperationContext(s.tag.bootId, s.epoch, s.binding, operation,
        Vector(MySqlExpectedEffect("NORMAL_RECALL/" + s.binding + "/" + h.pod, h.version, fingerprint.sha256)))
      val contextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context, root.profile)
      require(contextBytes.exactlyMatchesCell(value(row, OperationTable, "original_context_bytes")), "Recall original context differs")
    }
    // Keep the nested reserve on the same explicit DATA/comparison owner; ordinary H3 callers retain their plan child.
    val checkedReserve = originalReserve.map(raw =>
      verifyPersistedPhaseOnBudget(raw, None, root, dataBudget, deploymentInputs, activeComparisonBudget, currentRawInputs))
    withHistoryComparisonBudget(root, dataBudget) {
      val context = MySqlOperationContext(s.tag.bootId, s.epoch, s.binding, operation,
        Vector(MySqlExpectedEffect("NORMAL_RECALL/" + s.binding + "/" + h.pod, h.version, fingerprint.sha256)))
      val contextBytes = MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context, root.profile)
      val reserve = originalReserve.zip(checkedReserve).map { (raw, checked) =>
        require(checked.phase == MySqlOperationEffectPhase.Reserve && checked.header == h &&
          value(raw, OperationTable, "scope_bytes") == MySqlFormat3Cell.Binary(scopeBytes) && token(raw, "operation_state") == "COMMITTED",
          "Recall witness belongs to another original reserve")
        MySqlRecoveryOperationRawEnvelopeCodec.encode(raw, root.profile, deploymentInputs)
      }
      if state == "COMMITTED" then
        val witnessMatches = MySqlNormalRecallCanonical.matchesStream("FP_NORMAL_RECALL_WITNESS_BE_V2",
          binary("commit_witness_bytes"), root) { out =>
          out.writeByte(if phase == MySqlOperationEffectPhase.Reserve then 1 else 2)
          Vector(s.workflow, s.reserve, s.tag.operationId).foreach(MySqlRecoveryCanonicalBytes.writeUuid(out, _))
          MySqlNormalRecallCanonical.cell(out, MySqlFormat3Cell.DateTime(at))
          out.writeInt(scopeBytes.length); scopeBytes.writeTo(out)
          out.writeInt(fingerprint.length)
          MySqlRecoveryCanonicalBytes.writeToken(out, fingerprintDomain); writeFingerprint(out)
          out.writeInt(contextBytes.length); contextBytes.writeTo(out)
          Vector(binary("preimage_bytes"), binary("target_bytes")).foreach { b => out.writeInt(b.length); b.writeTo(out) }
          out.writeBoolean(reserve.nonEmpty)
          reserve.foreach { raw =>
            out.writeInt(raw.length)
            val shape = MySqlFormat3RelationShapes.forTable(OperationTable)
            out.writeInt(shape.columns.size)
            shape.columns.zipWithIndex.foreach { (column, index) =>
              MySqlRecoveryCanonicalBytes.writeText(out, column.name); MySqlRecoveryCanonicalBytes.writeText(out, column.sqlDeclaration)
              if !MySqlNormalRecallHistoricalPayload.writeDescriptor(out, raw.row, column.name) then MySqlNormalRecallCanonical.cell(out, raw.row(index))
            }
          }
        }
        require(witnessMatches, "Recall complete witness differs from original phase")
      else require(value(row, OperationTable, "commit_witness_bytes") == MySqlFormat3Cell.SqlNull &&
        value(row, OperationTable, "commit_witness_sha256") == MySqlFormat3Cell.SqlNull, "RESERVED Recall cannot carry a positive witness")
    }
    val verifiedContext = MySqlRecoveryCanonicalBytes.withAllocationBudget(phaseDataOwner) {
      MySqlOperationContext(s.tag.bootId, s.epoch, s.binding, operation,
        Vector(MySqlExpectedEffect("NORMAL_RECALL/" + s.binding + "/" + h.pod, h.version, fingerprint.sha256)))
    }
    val receipt = checkpoint.map(_.receipt)
    def decodeEffect(input: MySqlFormat3Binary, image: MySqlOperationEffectImage): MySqlNormalRecallCanonical.DecodedEffect =
      if activeComparisonBudget != null then
        MySqlNormalRecallCanonical.decodeEffectWithActiveHistoryComparisonOwner(input, h, phase, image, receipt,
          root, dataBudget, activeComparisonBudget, deploymentInputs)
      else if dataBudget eq root then
        MySqlNormalRecallCanonical.decodeEffect(input, h, phase, image, receipt, root, deploymentInputs)
      else
        MySqlNormalRecallCanonical.decodeEffect(input, h, phase, image, receipt,
          root, dataBudget, deploymentInputs)
    val beforeEffect = decodeEffect(binary("preimage_bytes"), MySqlOperationEffectImage.Before)
    val targetEffect = decodeEffect(binary("target_bytes"), MySqlOperationEffectImage.Target)
    new PersistedPhaseData(row, h, prepared, checkpoint, phase, verifiedContext, beforeEffect, targetEffect)

  /** 仅统计本轮完整 phase DATA 检查；CANCELLED 仍需原结算消费者，不代表可恢复或 Ready。 */
  private[mysql] final case class PersistedPhaseSummary(verifiedExecutions: Long, verifiedReserves: Long,
      pendingSettlements: Long)

  private[mysql] def visitDbFirstPersistedPhases(snapshot: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget,
      visit: (Vector[MySqlFormat3Cell], Option[Vector[MySqlFormat3Cell]], Boolean) => Unit): PersistedPhaseSummary =
    val rows=snapshot.relation(OperationTable).rows
    val workspace=Math.addExact(1024L,Math.multiplyExact(rows.size.toLong,384L))
    root.reserveTemporaryMemory(workspace,"historical phase index BEFORE allocation")
    var primary: Throwable=null
    try
      type Key=(MySqlFormat3Cell,MySqlFormat3Cell)
      val byOperation=scala.collection.mutable.HashMap.empty[Key,Vector[MySqlFormat3Cell]]
      val references=scala.collection.mutable.HashMap.empty[Key,Int]
      def key(row: Vector[MySqlFormat3Cell],column:String):Key =
        (value(row,OperationTable,"binding_uuid"),value(row,OperationTable,column))
      def execution(row: Vector[MySqlFormat3Cell]): Boolean =
        val step=token(row,"step")
        step == "RECALL_PREPARE" || step == "RECALL_CHECKPOINT"
      def scopeDomain(row: Vector[MySqlFormat3Cell]): Boolean = value(row,OperationTable,"scope_bytes") match
        case MySqlFormat3Cell.Binary(bytes) =>
          def matches(domain:String):Boolean = bytes.length >= 4 + domain.length &&
            (0 until 4).forall(i => (bytes.byteAt(i) & 255) == ((domain.length >>> (24-8*i)) & 255)) &&
            domain.indices.forall(i => bytes.byteAt(4+i) == domain.charAt(i).toByte)
          matches(MySqlNormalRecallCanonical.PrepareDomain) || matches(MySqlNormalRecallCanonical.CheckpointDomain)
        case _ => false
      rows.foreach { row =>
        require(byOperation.put(key(row,"operation_uuid"),row).isEmpty,"duplicate historical operation identity")
        if execution(row) then
          val reserveKey=key(row,"reserve_operation_uuid")
          require(reserveKey._2 != MySqlFormat3Cell.SqlNull,"Recall phase requires its original reserve identity")
          val count=Math.addExact(references.getOrElse(reserveKey,0),1)
          require(count == 1,"Recall reserve referenced by multiple execute operations")
          references.put(reserveKey,count)
      }
      var executions=0L;var reserves=0L;var pending=0L
      rows.foreach { row =>
        if execution(row) then
          val reserve=byOperation.getOrElse(key(row,"reserve_operation_uuid"),
            throw new IllegalArgumentException("Recall original reserve absent"))
          require(token(reserve,"step") == "RESERVE","Recall original reserve purpose differs")
          Vector("workflow_uuid","pod_uuid","attempt_uuid","footprint_uuid","scope_bytes").foreach { column =>
            require(value(row,OperationTable,column) == value(reserve,OperationTable,column),
              "Recall original reserve identity/scope differs: " + column)
          }
          val cancelled=token(row,"operation_state") == "CANCELLED"
          visit(row,Some(reserve),cancelled)
          if cancelled then pending=Math.addExact(pending,1L) else executions=Math.addExact(executions,1L)
          if token(reserve,"operation_state") == "CANCELLED" then pending=Math.addExact(pending,1L)
          else reserves=Math.addExact(reserves,1L)
        else if token(row,"step") == "RESERVE" && scopeDomain(row) then
          require(references.getOrElse(key(row,"operation_uuid"),0) == 1,
            "Recall phase scope reserve lacks its unique original execute operation")
      }
      PersistedPhaseSummary(executions,reserves,pending)
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try root.releaseTemporaryMemory(workspace,"historical phase index detached")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private[mysql] def verifyDbFirstPersistedPhase(row: Vector[MySqlFormat3Cell],
      reserve: Option[Vector[MySqlFormat3Cell]], root: MySqlFormat3ReadBudget, child: MySqlFormat3ReadBudget,
      currentInputs: MySqlScriptSchemaInputs, historicalInputs: MySqlScriptSchemaInputs, cancelled: Boolean): Unit =
    require(currentInputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27,
      "actual Full27 phase image inputs required")
    root.requireScriptDataInputs(currentInputs)
    child.requireHistoricalPayloadInputs(historicalInputs)
    if cancelled then
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row,root.profile,child,Some(currentInputs))
      reserve.foreach { r =>
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(r,root.profile,child,Some(currentInputs))
        if token(r,"operation_state") != "CANCELLED" then
          verifyPersistedPhaseOnBudget(r,None,root,child,Some(historicalInputs),child,Some(currentInputs))
      }
    else
      verifyPersistedPhaseOnBudget(row,reserve,root,child,Some(historicalInputs),child,Some(currentInputs))
      ()

  private def requestFrom(p: MySqlNormalRecallCanonical.DecodedPrepare): MySqlNormalRecallRequest =
    val h = p.header
    val domain = p.domain.head.rows.head
    def cell(name: String) = value(domain, "floating_pod_state", name)
    val world = cell("world_name") match
      case MySqlFormat3Cell.Text(v) => v
      case _ => fail("original Recall domain world missing")
    val updated = cell("last_updated") match
      case MySqlFormat3Cell.SignedInteger(v) => v
      case _ => fail("original Recall domain timestamp missing")
    def pose(v: MySqlNormalRecallCanonical.DecodedPose) = com.wi.floatingpod.domain.FloatingPodPose(world,
      java.lang.Double.longBitsToDouble(v.x), java.lang.Double.longBitsToDouble(v.y), java.lang.Double.longBitsToDouble(v.z),
      java.lang.Float.intBitsToFloat(v.yaw), java.lang.Float.intBitsToFloat(v.pitch), h.dimension)
    val committed = pose(h.committed)
    val base = com.wi.floatingpod.domain.FloatingPodState(h.pod, h.owner, h.version, world,
      committed.posX, committed.posY, committed.posZ, committed.yaw, committed.pitch,
      p.purpose != MySqlNormalRecallPurpose.FinishCommittedInactive, updated, Some(h.dimension), com.wi.floatingpod.domain.PodOperatingMode.Normal)
    MySqlNormalRecallRequest(h.stop, com.wi.floatingpod.storage.PhysicalRecoveryBinding(h.scope.binding,
      com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified, 2), base, pose(h.finalPose), h.dirtyRevision,
      h.attempt, h.footprint, h.chunks, h.source, p.purpose)

  /** 完整原O填回有限派生值后的历史DATA。尚未比较合法delta，不签提交/恢复资格。 */
  final class ResolvedCommittedPhaseData private[MySqlNormalRecallRecovery] (
      val phase: PersistedPhaseData, val before: Vector[MySqlFormat3RelationSnapshot],
      val target: Vector[MySqlFormat3RelationSnapshot])

  /** EXPECTED仅核未提交C的目标；类型不兼容实际COMMITTED历史，actual库存不变。 */
  final class PendingPhaseImages private[MySqlNormalRecallRecovery] (
      val phase: PersistedPhaseData, val before: Vector[MySqlFormat3RelationSnapshot],
      val expectedTarget: Vector[MySqlFormat3RelationSnapshot],
      private[MySqlNormalRecallRecovery] val expectedTerminal: Vector[MySqlFormat3Cell])

  def resolveCommittedEffectData(phase: PersistedPhaseData, actual: Image,
      root: MySqlFormat3ReadBudget): ResolvedCommittedPhaseData =
    require(phase != null && token(phase.row, "operation_state") == "COMMITTED",
      "positive effect DATA needs the original committed phase row")
    resolveCommittedEffectDataOnBudget(phase, actual, root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  /** H3 direct history caller: resolved before/target rows are retained by the exact plan child. */
  private[mysql] def resolveCommittedEffectData(phase: PersistedPhaseData, actual: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): ResolvedCommittedPhaseData =
    requirePlanHistoryDataBudget(root, dataBudget)
    resolveCommittedEffectDataOnBudget(phase, actual, root, dataBudget)

  private def resolveCommittedEffectDataOnBudget(phase: PersistedPhaseData, actual: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): ResolvedCommittedPhaseData =
    requireHistoryDataBudget(root, dataBudget)
    require(phase != null && token(phase.row, "operation_state") == "COMMITTED",
      "positive effect DATA needs the original committed phase row")
    val (before, target) = resolveEffectImages(phase, actual, None, root, dataBudget)
    new ResolvedCommittedPhaseData(phase, before, target)

  /** Temporary nested phase images share the already validated immutable Full26 graph and active DATA owner. */
  private def resolveCommittedEffectDataOnValidatedGraph(phase: PersistedPhaseData, actual: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      validatedInputs: Option[MySqlScriptSchemaInputs]): ResolvedCommittedPhaseData =
    requireHistoryDataBudget(root, dataBudget)
    requireSameValidatedImageInputs(actual, validatedInputs)
    require(phase != null && token(phase.row, "operation_state") == "COMMITTED",
      "positive effect DATA needs the original committed phase row")
    val (before, target) = resolveEffectImagesOnValidatedGraph(phase, actual, None,
      root, dataBudget, validatedInputs)
    new ResolvedCommittedPhaseData(phase, before, target)

  def resolvePendingCheckpointImages(phase: PersistedPhaseData, actual: Image,
      root: MySqlFormat3ReadBudget): PendingPhaseImages =
    require(phase != null && actual != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      phase.checkpoint.nonEmpty && phase.phase == MySqlOperationEffectPhase.Execute &&
      token(phase.row, "operation_state") == "RESERVED", "actual RESERVED C required for EXPECTED target")
    val reserve = actual.relation(OperationTable).rows.filter(r =>
      isUuid(r, OperationTable, "binding_uuid", phase.header.scope.binding) &&
        isUuid(r, OperationTable, "operation_uuid", phase.header.scope.reserve))
    require(reserve.size == 1, "pending C lacks its exact original reserve")
    // 重新核actual原行；EXPECTED字节在原retained root中生成，不从临时比较child外泄。
    verifyPersistedPhase(phase.row,Some(reserve.head),root,exactInputs(actual,root))
    val terminal = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val witness = expectedCheckpointWitness(phase, reserve.head, root)
      MySqlControlledDebugRows.update(phase.row, OperationTable, Map(
        "operation_state" -> MySqlFormat3Cell.Text("COMMITTED"),
        "commit_witness_bytes" -> witness.binaryCell, "commit_witness_sha256" -> witness.sha256Cell,
        "result_at" -> value(phase.row, OperationTable, "planned_at")), root)
    }
    val (before, target) = resolveEffectImages(phase, actual, Some(terminal), root)
    val result = new PendingPhaseImages(phase, before, target, terminal)
    requireEffectDelta(phase, before, target, terminal, root)
    result

  private def expectedCheckpointWitness(p: PersistedPhaseData, reserve: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    def binary(name: String): MySqlFormat3Binary = value(p.row, OperationTable, name) match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => fail("pending C original binary absent: " + name)
    val s = p.header.scope
    val fingerprint = MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_FINGERPRINT_BE_V1", root) { out =>
      MySqlRecoveryCanonicalBytes.writeToken(out, "EXECUTE")
      Vector(s.workflow, s.reserve, s.tag.operationId).foreach(MySqlRecoveryCanonicalBytes.writeUuid(out, _))
      Vector("request_identity_bytes", "scope_bytes", "preimage_bytes", "target_bytes").foreach { name =>
        val bytes = binary(name); out.writeInt(bytes.length); bytes.writeTo(out)
      }
    }
    val raw = MySqlRecoveryOperationRawEnvelopeCodec.encode(reserve, root.profile, p.beforeEffect.deploymentInputs)
    MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_WITNESS_BE_V2", root) { out =>
      out.writeByte(2)
      Vector(s.workflow, s.reserve, s.tag.operationId).foreach(MySqlRecoveryCanonicalBytes.writeUuid(out, _))
      MySqlNormalRecallCanonical.cell(out, value(p.row, OperationTable, "planned_at"))
      val scope = binary("scope_bytes"); out.writeInt(scope.length); scope.writeTo(out)
      out.writeInt(fingerprint.length); fingerprint.writeTo(out)
      Vector("original_context_bytes", "preimage_bytes", "target_bytes").foreach { name =>
        val bytes = binary(name); out.writeInt(bytes.length); bytes.writeTo(out)
      }
      out.writeBoolean(true); out.writeInt(raw.length)
      val shape = MySqlFormat3RelationShapes.forTable(OperationTable)
      out.writeInt(shape.columns.size)
      shape.columns.zipWithIndex.foreach { (column, index) =>
        MySqlRecoveryCanonicalBytes.writeText(out, column.name); MySqlRecoveryCanonicalBytes.writeText(out, column.sqlDeclaration)
        if !MySqlNormalRecallHistoricalPayload.writeDescriptor(out, raw.row, column.name) then
          MySqlNormalRecallCanonical.cell(out, raw.row(index))
      }
    }

  private def resolveEffectImages(phase: PersistedPhaseData, actual: Image,
      expectedTerminal: Option[Vector[MySqlFormat3Cell]], root: MySqlFormat3ReadBudget)
      : (Vector[MySqlFormat3RelationSnapshot], Vector[MySqlFormat3RelationSnapshot]) =
    resolveEffectImages(phase, actual, expectedTerminal, root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private def resolveEffectImages(phase: PersistedPhaseData, actual: Image,
      expectedTerminal: Option[Vector[MySqlFormat3Cell]], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget)
      : (Vector[MySqlFormat3RelationSnapshot], Vector[MySqlFormat3RelationSnapshot]) =
    requireHistoryDataBudget(root, dataBudget)
    require(phase != null && actual != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (actual.limitProfile eq root.profile) && actual.allocationBudget.sharesAllocationLedger(root),
      "original complete inventory and shared root required for effect DATA resolution")
    requireImageGraph(actual, root)
    val validatedInputs = inputsFromValidatedImage(actual)
    resolveEffectImagesOnValidatedGraph(phase, actual, expectedTerminal, root, dataBudget, validatedInputs)

  /** Private core reachable only after requireImageGraph validates this same immutable actual/root pair. */
  private def resolveEffectImagesOnValidatedGraph(phase: PersistedPhaseData, actual: Image,
      expectedTerminal: Option[Vector[MySqlFormat3Cell]], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, validatedInputs: Option[MySqlScriptSchemaInputs])
      : (Vector[MySqlFormat3RelationSnapshot], Vector[MySqlFormat3RelationSnapshot]) =
    requireHistoryDataBudget(root, dataBudget)
    requireSameValidatedImageInputs(actual, validatedInputs)
    require(phase != null && actual != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (actual.limitProfile eq root.profile) && actual.allocationBudget.sharesAllocationLedger(root),
      "original complete inventory and shared root required for effect DATA resolution")
    val originals = actual.relation(OperationTable).rows
    require(originals.contains(phase.row), "phase does not belong to the complete original inventory")
    def original(binding: MySqlFormat3Cell, operation: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
      val matches = originals.filter(row => value(row, OperationTable, "binding_uuid") == binding &&
        value(row, OperationTable, "operation_uuid") == operation)
      require(matches.size == 1, "exact original effect operation is absent or ambiguous")
      val row = matches.head
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, dataBudget, phase.beforeEffect.deploymentInputs)
      row
    val totalRows = Math.addExact(phase.beforeEffect.relations.map(_.rows.size.toLong).sum,
      phase.targetEffect.relations.map(_.rows.size.toLong).sum)
    require(totalRows <= root.profile.maxMembers(), "combined before/target effect member budget exceeded")
    dataBudget.reserveRetainedMemory(Math.addExact(8192L, Math.multiplyExact(totalRows, 8192L)),
      "resolved effect rows and original lookup views")
    val writerFingerprint = MySqlFormat3Cell.Binary(MySqlFormat3Binary.copyOf(
      phase.context.canonicalFingerprintSha256.toArray, dataBudget, "resolved original W context fingerprint"))
    def resolve(effect: MySqlNormalRecallCanonical.DecodedEffect): Vector[MySqlFormat3RelationSnapshot] =
      import MySqlNormalRecallCanonical.EffectCell
      effect.relations.map { relation =>
        val shape = effect.deploymentInputs.map(_.forTable(relation.table)).getOrElse(MySqlFormat3RelationShapes.forTable(relation.table))
        val rows = relation.rows.map { cells =>
          def scalar(name: String): MySqlFormat3Cell = cells(shape.columns.indexWhere(_.name == name)) match
            case EffectCell.Scalar(v) => v
            case _ => fail("effect identity cannot be derived")
          val originalRow = if relation.table == OperationTable && cells.exists {
            case EffectCell.OperationDerived | EffectCell.HistoricalPayload(_, _) => true
            case _ => false
          } then Some(original(scalar("binding_uuid"), scalar("operation_uuid"))) else None
          val resolved = shape.columns.zip(cells).map { (column, cell) => cell match
            case EffectCell.Scalar(v) => v
            case EffectCell.OperationDerived if relation.table == "fp_runtime_writer" =>
              require(column.name == "last_commit_fingerprint" && effect.image == MySqlOperationEffectImage.Target,
                "unexpected derived W cell")
              writerFingerprint
            case EffectCell.OperationDerived =>
              val original = originalRow.get
              val payloadRow = if expectedTerminal.nonEmpty && effect.image == MySqlOperationEffectImage.Target &&
                original == phase.row && Set("commit_witness_bytes", "commit_witness_sha256").contains(column.name)
                then expectedTerminal.get else original
              val result = value(payloadRow, OperationTable, column.name)
              require(result != MySqlFormat3Cell.SqlNull, "original operation lacks its required derived payload")
              result
            case EffectCell.CompletionDerived => value(phase.row, OperationTable, column.name)
            case EffectCell.HistoricalPayload(length, digest) =>
              val row = originalRow.get
              require(Set("COMMITTED", "CANCELLED").contains(token(row, "operation_state")), "descriptor original row is not terminal")
              value(row, OperationTable, column.name) match
                case b @ MySqlFormat3Cell.Binary(bytes) =>
                  require(bytes.length.toLong == length && value(row, OperationTable, column.name match
                    case "original_diagnostic_identity" => "original_diagnostic_sha256"
                    case n => n.stripSuffix("_bytes") + "_sha256") == MySqlFormat3Cell.Binary(digest),
                    "descriptor does not match the complete actual original payload")
                  b
                case _ => fail("descriptor original payload is absent")
          }
          if cells.exists { case EffectCell.HistoricalPayload(_, _) => true; case _ => false } then
            require(originalRow.contains(resolved), "historical effect row differs from its complete original RAW")
          resolved
        }
        MySqlFormat3RelationSnapshot(relation.table, rows)
      }
    val before = resolve(phase.beforeEffect)
    val target = resolve(phase.targetEffect)
    require(phase.beforeEffect.deploymentInputs==phase.targetEffect.deploymentInputs &&
      phase.beforeEffect.deploymentInputs==validatedInputs,"Recall effect/current source family differs")
    Vector(before,target).foreach { relations=>
      phase.beforeEffect.deploymentInputs match
        case None=>MySqlFormat3SnapshotValidation.validateRelations(relations)
        case Some(_)=>MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(relations)
    }
    if phase.prepare.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive then
      requireInactivePrepareHistoryDataOnValidatedGraph(phase.prepare, actual, before, phase.phase,
        root, dataBudget, validatedInputs.getOrElse(fail("inactive P2 history requires exact schema5 Full26 inputs")))
    else if phase.prepare.purpose != MySqlNormalRecallPurpose.NormalRecall then
      requireActivePrepareWithoutCheckpointHistoryDataOnValidatedGraph(phase.prepare, actual, root,
        dataBudget, validatedInputs.getOrElse(fail("active P2 history requires exact schema5 Full26 inputs")))
    (before, target)

  /** 无C的P2历史DATA：完整原P/Rp及双向effect链，不以step名称代替原操作语义。 */
  private[mysql] def requireActivePrepareWithoutCheckpointHistoryData(
      prepared: MySqlNormalRecallCanonical.DecodedPrepare, actual: Image,
      root: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget,
      "original open Recall root required for P2 history DATA")
    requireActivePrepareWithoutCheckpointHistoryData(prepared, actual, root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private def requireActivePrepareWithoutCheckpointHistoryData(
      prepared: MySqlNormalRecallCanonical.DecodedPrepare, actual: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    requireHistoryDataBudget(root, dataBudget)
    require(prepared != null && actual != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      prepared.header.scope.stableSession.schemaFormat == 5 &&
      prepared.purpose == MySqlNormalRecallPurpose.RestoreCommittedActive &&
      prepared.originalPrepare.nonEmpty && prepared.originalCheckpoint.isEmpty && prepared.settlement.isEmpty &&
      prepared.predecessor.nonEmpty, "P2 history DATA requires the no-C active branch; S/inactive history remains separate")
    requireImageGraph(actual, root)
    val validatedInputs = inputsFromValidatedImage(actual).getOrElse(
      fail("P2 history DATA requires complete schema5 inputs"))
    requireActivePrepareWithoutCheckpointHistoryDataOnValidatedGraph(
      prepared, actual, root, dataBudget, validatedInputs)

  /** Unit-only P/Rp proof; all temporary phase/effect/delta values stay on this active owner. */
  private def requireActivePrepareWithoutCheckpointHistoryDataOnValidatedGraph(
      prepared: MySqlNormalRecallCanonical.DecodedPrepare, actual: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      inputs: MySqlScriptSchemaInputs): Unit =
    requireHistoryDataBudget(root, dataBudget)
    requireSameValidatedImageInputs(actual, Some(inputs))
    require(prepared != null && actual != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile) &&
      prepared.header.scope.stableSession.schemaFormat == 5 &&
      prepared.purpose == MySqlNormalRecallPurpose.RestoreCommittedActive &&
      prepared.originalPrepare.nonEmpty && prepared.originalCheckpoint.isEmpty && prepared.settlement.isEmpty &&
      prepared.predecessor.nonEmpty, "P2 history DATA requires the no-C active branch; S/inactive history remains separate")
    val originals = actual.relation(OperationTable).rows
    val indexScratch = Math.addExact(8192L, Math.multiplyExact(originals.size.toLong, 512L))
    withHistoryComparisonBudget(root, dataBudget) {
      val activeOwner = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      MySqlNormalRecallCanonical.requireHistoryComparisonOwner(root, dataBudget, activeOwner)
      activeOwner.reserveTemporaryMemory(indexScratch, "P2 no-C original history indexes BEFORE allocation")
      var primary: Throwable = null
      try
        val h = prepared.header
        val s = h.scope
        def op(id: UUID): Vector[MySqlFormat3Cell] =
          val found = originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
            isUuid(r, OperationTable, "operation_uuid", id))
          require(found.size == 1, "P2 original history operation absent or ambiguous")
          found.head
        val original = op(prepared.originalPrepare.get)
        val predecessorMatches = MySqlRecoveryCanonicalBytes.withAllocationBudget(activeOwner) {
          prepared.predecessor.get.exactlyMatches(original, root.profile, Some(inputs))
        }
        require(predecessorMatches, "P2 embedded original P differs from actual complete RAW")
        val reserveId = value(original, OperationTable, "reserve_operation_uuid")
        val reserveRows = originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
          value(r, OperationTable, "operation_uuid") == reserveId)
        require(reserveRows.size == 1, "P2 original P lacks its exact Rp")
        def verify(row: Vector[MySqlFormat3Cell], reserve: Option[Vector[MySqlFormat3Cell]]): PersistedPhaseData =
          verifyPersistedPhaseOnBudget(row, reserve, root, dataBudget, Some(inputs), activeOwner)
        def resolve(phase: PersistedPhaseData): ResolvedCommittedPhaseData =
          resolveCommittedEffectDataOnValidatedGraph(phase, actual, root, activeOwner, Some(inputs))
        val p = verify(original, Some(reserveRows.head))
        val rp = verify(reserveRows.head, None)
        require(token(original, "operation_state") == "COMMITTED" && p.phase == MySqlOperationEffectPhase.Execute &&
          p.checkpoint.isEmpty && p.prepare.purpose == MySqlNormalRecallPurpose.NormalRecall &&
          p.header.previous.isEmpty && rp.phase == MySqlOperationEffectPhase.Reserve && rp.header == p.header,
          "P2 no-C predecessor must be its original committed P/Rp")
        val allowed = Set(p.header.scope.tag.operationId, p.header.scope.reserve, s.tag.operationId, s.reserve)
        val workflow = originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
          isUuid(r, OperationTable, "workflow_uuid", s.workflow))
        require(workflow.forall(r => allowed.contains(MySqlModeOperationEffectCodec.rowOperationUuid(r))),
          "P2 no-C workflow contains C/Rc or unexplained successors")
        require(!actual.relation(CompletionTable).rows.exists(r => isUuid(r, CompletionTable, "binding_uuid", s.binding) &&
          isUuid(r, CompletionTable, "completion_operation_uuid", p.header.scope.tag.operationId)),
          "P2 no-C workflow has an orphan original completion")
        val ph = p.header
        require(h.previous.contains(ph.scope.tag.operationId) && s.workflow == ph.scope.workflow &&
          h.pod == ph.pod && h.attempt == ph.attempt && h.footprint == ph.footprint && h.stop == ph.stop &&
          h.owner == ph.owner && h.version == ph.version && h.locatorRevision == ph.locatorRevision &&
          h.dimension == ph.dimension && h.committed == ph.committed && h.finalPose == ph.committed &&
          h.chunks == ph.chunks && h.source == ph.source && prepared.domain == p.prepare.domain && prepared.locator == p.prepare.locator,
          "P2 active base must preserve original committed pose/owner/version/footprint and source identity")
        val resolvedP = resolve(p)
        val resolvedRp = resolve(rp)
        requireCommittedEffectDelta(resolvedP, root, activeOwner, propagateActiveComparisonBudget = true)
        requireCommittedEffectDelta(resolvedRp, root, activeOwner, propagateActiveComparisonBudget = true)
        require(resolvedRp.target == resolvedP.before, "P2 original Rp target differs from original P before")
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try activeOwner.releaseTemporaryMemory(indexScratch, "P2 no-C original history indexes dropped")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    }

  /** 原C的完整历史及inactive P2 Before；不从DATA推导C执行权或恢复资格。 */
  private[mysql] def requireInactivePrepareHistoryData(prepared: MySqlNormalRecallCanonical.DecodedPrepare,
      actual: Image, before: Vector[MySqlFormat3RelationSnapshot], phase: MySqlOperationEffectPhase,
      root: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget,
      "original open Recall root required for inactive P2 history DATA")
    requireInactivePrepareHistoryData(prepared, actual, before, phase, root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  /** Explicit retained DATA owner; ordinary five-argument callers keep their existing default owner. */
  private[mysql] def requireInactivePrepareHistoryData(prepared: MySqlNormalRecallCanonical.DecodedPrepare,
      actual: Image, before: Vector[MySqlFormat3RelationSnapshot], phase: MySqlOperationEffectPhase,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    requireHistoryDataBudget(root, dataBudget)
    require(prepared != null && actual != null && before != null && phase != null && root != null &&
      !root.isClosed && !root.isScopedReadBudget && prepared.header.scope.stableSession.schemaFormat == 5 &&
      prepared.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive && prepared.originalPrepare.nonEmpty &&
      prepared.originalCheckpoint.nonEmpty && prepared.settlement.isEmpty && prepared.predecessor.nonEmpty,
      "inactive P2 history requires its original P/C without S")
    requireImageGraph(actual, root)
    val inputs = inputsFromValidatedImage(actual).getOrElse(fail("inactive P2 requires complete schema5 history inputs"))
    requireInactivePrepareHistoryDataOnValidatedGraph(prepared, actual, before, phase, root, dataBudget, inputs)

  /** Unit-only P/Rp/C/Rc proof over one graph validation and one exact active DATA/comparison owner. */
  private def requireInactivePrepareHistoryDataOnValidatedGraph(
      prepared: MySqlNormalRecallCanonical.DecodedPrepare, actual: Image,
      before: Vector[MySqlFormat3RelationSnapshot], phase: MySqlOperationEffectPhase,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      inputs: MySqlScriptSchemaInputs): Unit =
    requireHistoryDataBudget(root, dataBudget)
    requireSameValidatedImageInputs(actual, Some(inputs))
    require(prepared != null && actual != null && before != null && phase != null && root != null &&
      !root.isClosed && !root.isScopedReadBudget && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile) && prepared.header.scope.stableSession.schemaFormat == 5 &&
      prepared.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive && prepared.originalPrepare.nonEmpty &&
      prepared.originalCheckpoint.nonEmpty && prepared.settlement.isEmpty && prepared.predecessor.nonEmpty,
      "inactive P2 history requires its original P/C without S")
    withHistoryComparisonBudget(root, dataBudget) {
      import MySqlControlledDebugRows.{uuid, signed, text, update}
      val activeOwner = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      MySqlNormalRecallCanonical.requireHistoryComparisonOwner(root, dataBudget, activeOwner)
      val originals = actual.relation(OperationTable).rows
      activeOwner.reserveRetainedMemory(Math.addExact(16384L, Math.multiplyExact(originals.size.toLong, 1024L)),
        "inactive P2 exact original P/Rp/C/Rc history indexes BEFORE allocation")
      val h = prepared.header
      val s = h.scope
      def op(id: UUID): Vector[MySqlFormat3Cell] =
        val found = originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
          isUuid(r, OperationTable, "operation_uuid", id))
        require(found.size == 1, "inactive P2 original history operation absent or ambiguous")
        found.head
      def committed(id: UUID): (PersistedPhaseData, PersistedPhaseData) =
        val operation = op(id)
        val reserve = originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
          value(r, OperationTable, "operation_uuid") == value(operation, OperationTable, "reserve_operation_uuid"))
        require(reserve.size == 1, "inactive P2 predecessor lacks its exact reserve")
        val execute = verifyPersistedPhaseOnBudget(operation, Some(reserve.head), root, dataBudget, Some(inputs), activeOwner)
        val rp = verifyPersistedPhaseOnBudget(reserve.head, None, root, dataBudget, Some(inputs), activeOwner)
        require(token(operation, "operation_state") == "COMMITTED" && execute.phase == MySqlOperationEffectPhase.Execute &&
          rp.phase == MySqlOperationEffectPhase.Reserve && rp.header == execute.header,
          "inactive P2 predecessor/reserve is not an exact committed phase pair")
        (execute, rp)
      val (p, rp) = committed(prepared.originalPrepare.get)
      val (c, rc) = committed(prepared.originalCheckpoint.get)
      val predecessorMatches = MySqlRecoveryCanonicalBytes.withAllocationBudget(activeOwner) {
        prepared.predecessor.get.exactlyMatches(c.row, root.profile, Some(inputs))
      }
      require(p.checkpoint.isEmpty && p.prepare.purpose == MySqlNormalRecallPurpose.NormalRecall &&
        p.header.previous.isEmpty && c.checkpoint.nonEmpty && c.prepare == p.prepare &&
        c.header.previous.contains(p.header.scope.tag.operationId) && c.header.scope.workflow == s.workflow &&
        predecessorMatches,
        "inactive P2 requires complete original P/Rp/C/Rc and exact embedded committed C")
      val allowed = Set(p.header.scope.tag.operationId,p.header.scope.reserve,c.header.scope.tag.operationId,
        c.header.scope.reserve,s.tag.operationId,s.reserve)
      require(originals.filter(r => isUuid(r, OperationTable, "binding_uuid", s.binding) &&
        isUuid(r, OperationTable, "workflow_uuid", s.workflow)).forall(r =>
          allowed.contains(MySqlModeOperationEffectCodec.rowOperationUuid(r))),
        "inactive P2 workflow contains S or unexplained successors")
      val checkpoint = c.checkpoint.get
      val ch = c.header
      require(h.previous.contains(ch.scope.tag.operationId) && s.workflow == p.header.scope.workflow &&
        h.pod == ch.pod && h.attempt == ch.attempt && h.footprint == ch.footprint && h.stop == ch.stop &&
        h.owner == ch.owner && h.version == checkpoint.afterVersion && h.locatorRevision == checkpoint.afterLocator &&
        h.dimension == ch.dimension && h.committed == ch.finalPose && h.finalPose == ch.finalPose &&
        h.chunks == ch.chunks && h.source == ch.source &&
        prepared.domain == checkpoint.targetDomainLocator.filter(_.table == "floating_pod_state") &&
        prepared.locator == checkpoint.targetDomainLocator.filter(_.table == "physical_recovery_locator"),
        "inactive P2 must preserve the exact original committed C domain/locator/pose/version")
      def selected(relations: Vector[MySqlFormat3RelationSnapshot], table: String,
          matches: Vector[MySqlFormat3Cell] => Boolean): Vector[MySqlFormat3Cell] =
        val found = relations.find(_.table == table).get.rows.filter(matches)
        require(found.size == 1, "inactive P2 exact original C target row absent or duplicated: " + table)
        found.head
      def podReservation(r: Vector[MySqlFormat3Cell]): Boolean =
        isUuid(r, ReservationTable, "binding_uuid", s.binding) && isUuid(r, ReservationTable, "resource_pod_uuid", h.pod)
      // Reservation不含派生marker；先核精确Before，拒绝路径不物化四份完整effect镜像。
      val reservationEffect = c.targetEffect.relations.find(_.table == ReservationTable).get
      activeOwner.reserveRetainedMemory(Math.addExact(1024L,Math.multiplyExact(reservationEffect.rows.size.toLong,4096L)),
        "inactive P2 scalar C reservation target view BEFORE allocation")
      val reservationTarget = MySqlFormat3RelationSnapshot(ReservationTable,reservationEffect.rows.map(_.map {
        case MySqlNormalRecallCanonical.EffectCell.Scalar(value) => value
        case _ => fail("inactive P2 original C reservation cannot contain derived cells")
      }))
      val released = selected(Vector(reservationTarget),ReservationTable,podReservation)
      require(value(released,ReservationTable,"reservation_state") == text("RELEASED") &&
        isUuid(released,ReservationTable,"workflow_uuid",s.workflow) &&
        isUuid(released,ReservationTable,"operation_uuid",ch.scope.tag.operationId) &&
        isUuid(released,ReservationTable,"last_mutation_operation_uuid",ch.scope.tag.operationId),
        "inactive P2 original C did not release its exact workflow/POD reservation")
      val expected = if phase == MySqlOperationEffectPhase.Reserve then released else
        val revision = value(released,ReservationTable,"reservation_revision") match
          case MySqlFormat3Cell.SignedInteger(n) => n
          case _ => fail("inactive P2 original C reservation revision missing")
        update(released,ReservationTable,Map("reservation_state" -> text("HELD"),
          "operation_uuid" -> uuid(s.tag.operationId),"last_mutation_operation_uuid" -> uuid(s.reserve),
          "reservation_revision" -> signed(Math.incrementExact(revision))),activeOwner)
      require(selected(before,ReservationTable,podReservation) == expected,
        "inactive P2 before must retain the exact original C reservation or its single Rp2 reacquisition")
      val resolvedP = resolveCommittedEffectDataOnValidatedGraph(p, actual, root, activeOwner, Some(inputs))
      val resolvedRp = resolveCommittedEffectDataOnValidatedGraph(rp, actual, root, activeOwner, Some(inputs))
      val resolvedC = resolveCommittedEffectDataOnValidatedGraph(c, actual, root, activeOwner, Some(inputs))
      val resolvedRc = resolveCommittedEffectDataOnValidatedGraph(rc, actual, root, activeOwner, Some(inputs))
      Vector(resolvedP,resolvedRp,resolvedC,resolvedRc).foreach(
        requireCommittedEffectDelta(_, root, activeOwner, propagateActiveComparisonBudget = true))
      require(resolvedRp.target == resolvedP.before && resolvedRc.target == resolvedC.before &&
        selected(resolvedC.target,ReservationTable,podReservation) == released,
        "inactive P2 original reserve targets differ from their execute before images")
      val completion: Vector[MySqlFormat3Cell] => Boolean = r =>
        isUuid(r,CompletionTable,"binding_uuid",s.binding) && isUuid(r,CompletionTable,"receipt_uuid",checkpoint.receipt)
      val originalCompletion = selected(resolvedC.target,CompletionTable,completion)
      require(selected(before,CompletionTable,completion) == originalCompletion &&
        selected(actual.relations,CompletionTable,completion) == originalCompletion,
        "inactive P2 original committed C completion changed or was consumed")
      checkpoint.targetObligations.foreach { relation =>
        require(before.find(_.table == relation.table).get.rows.filter(r =>
          isUuid(r,relation.table,"binding_uuid",s.binding) && isUuid(r,relation.table,"pod_uuid",h.pod)) == relation.rows,
          "inactive P2 before changed the original C target obligations")
      }
    }

  /** 独立重建本 Rp/P/Rc/C 与已闭合 active/inactive Rp2/P2 的完整差额；不签恢复资格。 */
  def requireCommittedEffectDelta(data: ResolvedCommittedPhaseData, root: MySqlFormat3ReadBudget): Unit =
    require(data != null && root != null && !root.isClosed && !root.isScopedReadBudget, "original resolved effect DATA required")
    requireEffectDelta(data.phase, data.before, data.target, data.phase.row, root, root)

  private[mysql] def requireCommittedEffectDelta(data: ResolvedCommittedPhaseData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    require(data != null, "original resolved effect DATA required")
    requireHistoryDataBudget(root, dataBudget)
    requireEffectDelta(data.phase, data.before, data.target, data.phase.row, root, dataBudget)

  private[mysql] def requireCommittedEffectDelta(data: ResolvedCommittedPhaseData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      propagateActiveComparisonBudget: Boolean): Unit =
    require(data != null, "original resolved effect DATA required")
    requireHistoryDataBudget(root, dataBudget)
    requireEffectDelta(data.phase, data.before, data.target, data.phase.row, root, dataBudget,
      propagateActiveComparisonBudget)

  private def requireEffectDelta(p: PersistedPhaseData, before: Vector[MySqlFormat3RelationSnapshot],
      target: Vector[MySqlFormat3RelationSnapshot], terminal: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget): Unit =
    requireEffectDelta(p, before, target, terminal, root, root)

  private def requireEffectDelta(p: PersistedPhaseData, before: Vector[MySqlFormat3RelationSnapshot],
      target: Vector[MySqlFormat3RelationSnapshot], terminal: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    requireEffectDelta(p, before, target, terminal, root, dataBudget,
      propagateActiveComparisonBudget = false)

  private def requireEffectDelta(p: PersistedPhaseData, before: Vector[MySqlFormat3RelationSnapshot],
      target: Vector[MySqlFormat3RelationSnapshot], terminal: Vector[MySqlFormat3Cell],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      propagateActiveComparisonBudget: Boolean): Unit =
    val h = p.header
    val s = h.scope
    require(p.prepare.purpose == MySqlNormalRecallPurpose.NormalRecall ||
      (p.prepare.purpose == MySqlNormalRecallPurpose.RestoreCommittedActive && p.checkpoint.isEmpty &&
        p.prepare.originalCheckpoint.isEmpty && p.prepare.settlement.isEmpty) ||
      (p.prepare.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive && p.checkpoint.isEmpty &&
        p.prepare.originalCheckpoint.nonEmpty && p.prepare.settlement.isEmpty),
      "S/G purpose requires its own exact delta verifier")
    withHistoryComparisonBudget(root, dataBudget) {
      import MySqlControlledDebugRows.{uuid, signed, text, floating, row, update, Null}
      val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(dataBudget)
      val nestedComparisonBudget = budget
      budget.reserveRetainedMemory(65536L, "Recall exact delta maps and scalar wrappers")
      var expected = before
      def rows(table: String): Vector[Vector[MySqlFormat3Cell]] = expected.find(_.table == table).get.rows
      def one(table: String, matches: Vector[MySqlFormat3Cell] => Boolean): Vector[MySqlFormat3Cell] =
        val found = rows(table).filter(matches)
        require(found.size == 1, "exact historical delta row missing or duplicated: " + table)
        found.head
      def op(id: UUID): Vector[MySqlFormat3Cell] = one(OperationTable, r =>
        isUuid(r, OperationTable, "binding_uuid", s.binding) && isUuid(r, OperationTable, "operation_uuid", id))
      def put(table: String, replacement: Vector[MySqlFormat3Cell], existed: Boolean): Unit =
        val shape = MySqlFormat3RelationShapes.forTable(table)
        val original = rows(table)
        val matches = original.zipWithIndex.filter((r, _) => MySqlControlledDebugDelta.compareKey(r, replacement, shape) == 0)
        require(matches.size == (if existed then 1 else 0), "unexpected historical delta key presence: " + table)
        budget.reserveRetainedMemory(Math.addExact(1024L, Math.multiplyExact(original.size.toLong + 1L, 128L)), "Recall expected relation containers")
        val changed = if existed then original.updated(matches.head._2, replacement)
          else (original :+ replacement).sortWith((a, b) => MySqlControlledDebugDelta.compareKey(a, b, shape) < 0)
        expected = expected.map(r => if r.table == table then MySqlFormat3RelationSnapshot(table, changed) else r)
      val at = value(p.row, OperationTable, "planned_at") match
        case MySqlFormat3Cell.DateTime(v) => v
        case _ => fail("original phase time missing")
      val domain = one("floating_pod_state", isUuid(_, "floating_pod_state", "uuid", h.pod))
      val locator = one("physical_recovery_locator", r => isUuid(r, "physical_recovery_locator", "binding_uuid", s.binding) &&
        isUuid(r, "physical_recovery_locator", "pod_uuid", h.pod))
      require(p.prepare.domain.head.rows == Vector(domain) && p.prepare.locator.head.rows == Vector(locator),
        "Recall original base scope differs from complete before image")
      if p.checkpoint.isEmpty then p.prepare.sources.foreach { relation =>
        require(rows(relation.table) == relation.rows, "P original full source relation differs: " + relation.table)
      }
      def selectedPod(table: String): Vector[MySqlFormat3Cell] => Boolean =
        r => isUuid(r, table, if table == "floating_pod_state" then "uuid" else "pod_uuid", h.pod)
      if p.prepare.purpose != MySqlNormalRecallPurpose.NormalRecall then
        require(p.prepare.originalPrepare.nonEmpty && p.prepare.predecessor.nonEmpty &&
          h.previous == (if p.prepare.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive then
            p.prepare.originalCheckpoint else p.prepare.originalPrepare) &&
          p.prepare.predecessor.get.exactlyMatches(op(h.previous.get), root.profile, p.beforeEffect.deploymentInputs),
          "P2 before image does not retain its exact embedded original predecessor")
      p.checkpoint.foreach { c =>
        val previous = op(p.prepare.header.scope.tag.operationId)
        require(token(previous, "operation_state") == "COMMITTED" &&
          value(previous, OperationTable, "scope_bytes") == MySqlFormat3Cell.Binary(c.prepareBytes),
          "C nested original P differs from complete committed predecessor")
        val verifiedP = verifyPersistedPhaseOnBudget(previous, Some(op(p.prepare.header.scope.reserve)),
          root, dataBudget, p.beforeEffect.deploymentInputs, nestedComparisonBudget)
        require(verifiedP.prepare == p.prepare && verifiedP.checkpoint.isEmpty, "C predecessor is not its original P")
        c.beforeObligations.foreach { relation =>
          require(rows(relation.table).filter(selectedPod(relation.table)) == relation.rows,
            "C scope obligation before differs from complete effect image")
        }
      }
      val w = one("fp_runtime_writer", _ => true)
      require(isUuid(w, "fp_runtime_writer", "binding_uuid", s.binding) &&
        isUuid(w, "fp_runtime_writer", "owner_boot_uuid", s.tag.bootId) &&
        value(w, "fp_runtime_writer", "writer_epoch") == signed(s.epoch) &&
        value(w, "fp_runtime_writer", "lease_until") == MySqlFormat3Cell.DateTime(s.lease), "original fixed W differs")
      val chunks = rows("physical_recovery_footprint_chunk").filter(r =>
        isUuid(r, "physical_recovery_footprint_chunk", "binding_uuid", s.binding) &&
          isUuid(r, "physical_recovery_footprint_chunk", "pod_uuid", h.pod) &&
          isUuid(r, "physical_recovery_footprint_chunk", "attempt_uuid", h.attempt) &&
          isUuid(r, "physical_recovery_footprint_chunk", "footprint_uuid", h.footprint))
      require(chunks.size == 25 && h.chunks.forall(c => chunks.count(r =>
        value(r, "physical_recovery_footprint_chunk", "dimension_id") == signed(c.dimensionId) &&
          value(r, "physical_recovery_footprint_chunk", "chunk_x") == signed(c.chunkX) &&
          value(r, "physical_recovery_footprint_chunk", "chunk_z") == signed(c.chunkZ)) == 1), "original full25 effect footprint differs")
      val key = MySqlRecoveryResourceKey.pod(h.pod, root.profile)
      val keyBytes = MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", root.profile)(key.writeCanonicalTo)
      val reservations = rows(ReservationTable).filter(r => isUuid(r, ReservationTable, "binding_uuid", s.binding) &&
        isUuid(r, ReservationTable, "resource_pod_uuid", h.pod))
      require(reservations.size <= 1, "duplicate original POD reservation")
      def revision(r: Vector[MySqlFormat3Cell]): Long = value(r, ReservationTable, "reservation_revision") match
        case MySqlFormat3Cell.SignedInteger(n) if n >= 0 => n
        case _ => fail("invalid original reservation revision")
      reservations.foreach { r => require(value(r, ReservationTable, "resource_kind") == text("POD") &&
        value(r, ReservationTable, "resource_key") == keyBytes.binaryCell &&
        value(r, ReservationTable, "resource_key_hash") == keyBytes.sha256Cell &&
        value(r, ReservationTable, "resource_unit_uuid") == Null, "original POD reservation canonical key differs") }
      if p.phase == MySqlOperationEffectPhase.Reserve then
        val next = reservations.headOption match
          case None =>
            require(h.previous.isEmpty, "retarget requires the original held reservation")
            row(ReservationTable, Map("binding_uuid" -> uuid(s.binding), "resource_kind" -> text("POD"),
              "resource_key_hash" -> keyBytes.sha256Cell, "resource_key" -> keyBytes.binaryCell,
              "reservation_revision" -> signed(0), "workflow_uuid" -> uuid(s.workflow), "operation_uuid" -> uuid(s.tag.operationId),
              "reservation_state" -> text("HELD"), "last_mutation_operation_uuid" -> uuid(s.reserve),
              "resource_pod_uuid" -> uuid(h.pod), "resource_unit_uuid" -> Null), budget)
          case Some(old) =>
            h.previous match
              case None => require(value(old, ReservationTable, "reservation_state") == text("RELEASED"), "initial P reused a held reservation")
              case Some(previous) =>
                val inactive = p.prepare.purpose == MySqlNormalRecallPurpose.FinishCommittedInactive
                require(value(old, ReservationTable, "reservation_state") == text(if inactive then "RELEASED" else "HELD") &&
                  isUuid(old, ReservationTable, "workflow_uuid", s.workflow) &&
                  isUuid(old, ReservationTable, "operation_uuid", previous) &&
                  token(op(previous), "operation_state") == "COMMITTED" && isUuid(op(previous), OperationTable, "workflow_uuid", s.workflow) &&
                  (!inactive || (token(op(previous),"step") == "RECALL_CHECKPOINT" &&
                    isUuid(old,ReservationTable,"last_mutation_operation_uuid",previous))),
                  "Recall retarget lost its original committed predecessor")
            update(old, ReservationTable, Map("workflow_uuid" -> uuid(s.workflow), "operation_uuid" -> uuid(s.tag.operationId),
              "reservation_state" -> text("HELD"), "reservation_revision" -> signed(Math.incrementExact(revision(old))),
              "last_mutation_operation_uuid" -> uuid(s.reserve)), budget)
        put(ReservationTable, next, reservations.nonEmpty)
        val executeTarget = target.find(_.table == OperationTable).get.rows.filter(r =>
          isUuid(r, OperationTable, "binding_uuid", s.binding) && isUuid(r, OperationTable, "operation_uuid", s.tag.operationId))
        require(executeTarget.size == 1 && token(executeTarget.head, "operation_state") == "RESERVED", "reserve target lacks its exact pending execute")
        val execute = verifyPersistedPhaseOnBudget(executeTarget.head, Some(p.row),
          root, dataBudget, p.beforeEffect.deploymentInputs, nestedComparisonBudget)
        require(execute.header == h && execute.checkpoint == p.checkpoint, "reserved execute scope differs from original reserve")
        put(OperationTable, p.row, false); put(OperationTable, executeTarget.head, false)
      else
        require(reservations.size == 1, "execute lost held reservation")
        val old = reservations.head
        require(value(old, ReservationTable, "reservation_state") == text("HELD") &&
          isUuid(old, ReservationTable, "workflow_uuid", s.workflow) &&
          isUuid(old, ReservationTable, "operation_uuid", s.tag.operationId) &&
          isUuid(old, ReservationTable, "last_mutation_operation_uuid", s.reserve), "execute original reserve ownership differs")
        val reserve = verifyPersistedPhaseOnBudget(op(s.reserve), None,
          root, dataBudget, p.beforeEffect.deploymentInputs, nestedComparisonBudget)
        val reserveFingerprintMatches = value(w, "fp_runtime_writer", "last_commit_fingerprint") match
          case MySqlFormat3Cell.Binary(bytes) => bytes.equalsVector(reserve.context.canonicalFingerprintSha256)
          case _ => false
        require(reserve.header == h && reserve.checkpoint == p.checkpoint &&
          isUuid(w, "fp_runtime_writer", "last_commit_operation_uuid", s.reserve) &&
          reserveFingerprintMatches, "execute original reserve/W receipt differs")
        val pending = update(p.row, OperationTable, Map("operation_state" -> text("RESERVED"),
          "commit_witness_bytes" -> Null, "commit_witness_sha256" -> Null, "result_at" -> Null), budget)
        require(op(s.tag.operationId) == pending, "execute before differs from original pending operation")
        put(OperationTable, terminal, true)
        put(ReservationTable, update(old, ReservationTable, Map("reservation_revision" -> signed(Math.incrementExact(revision(old))),
          "reservation_state" -> text(if p.checkpoint.nonEmpty then "RELEASED" else "HELD"),
          "last_mutation_operation_uuid" -> uuid(s.tag.operationId)), budget), true)
        p.checkpoint.foreach { c =>
          val pose = requestFrom(p.prepare).finalPose
          val changedDomain = update(domain, "floating_pod_state", Map("is_active" -> signed(0),
            "state_version" -> signed(c.afterVersion), "world_name" -> text(pose.worldName),
            "pos_x" -> floating(pose.posX), "pos_y" -> floating(pose.posY), "pos_z" -> floating(pose.posZ),
            "yaw" -> floating(pose.yaw.toDouble), "pitch" -> floating(pose.pitch.toDouble),
            "last_updated" -> signed(at.toInstant(java.time.ZoneOffset.UTC).toEpochMilli),
            "last_mutation_operation_uuid" -> uuid(s.tag.operationId)), budget)
          put("floating_pod_state", changedDomain, true)
          put("physical_recovery_locator", update(locator, "physical_recovery_locator", Map(
            "locator_state" -> text("ROW_INACTIVE_PENDING"), "reason_code" -> text("ROW_INACTIVE_PENDING"),
            "domain_version" -> signed(c.afterVersion), "locator_revision" -> signed(c.afterLocator),
            "last_mutation_operation_uuid" -> uuid(s.tag.operationId)), budget), true)
          val table = "physical_recovery_obligation"
          def selected(r: Vector[MySqlFormat3Cell], table: String): Boolean =
            isUuid(r, table, "binding_uuid", s.binding) && isUuid(r, table, "pod_uuid", h.pod) &&
              isUuid(r, table, "attempt_uuid", h.attempt) && isUuid(r, table, "footprint_uuid", h.footprint)
          val footprint = one("physical_recovery_footprint", selected(_, "physical_recovery_footprint"))
          val origin = value(footprint, "physical_recovery_footprint", "origin_version")
          val existing = rows(table).filter(selected(_, table))
          require(existing.size <= 1, "duplicate original checkpoint obligation")
          existing.headOption match
            case Some(o) => require(value(o, table, "obligation_state") == text("PENDING") && value(o, table, "origin_version") == origin &&
              Vector("verified_operation_uuid", "verification_proof_bytes", "verification_proof_sha256").forall(n => value(o, table, n) == Null),
              "checkpoint reused a verified/mismatched obligation")
            case None => put(table, row(table, Map("binding_uuid" -> uuid(s.binding), "pod_uuid" -> uuid(h.pod),
              "attempt_uuid" -> uuid(h.attempt), "footprint_uuid" -> uuid(h.footprint), "origin_version" -> origin,
              "purpose" -> text("SAME_ATTEMPT_CHECKPOINT"), "created_operation_uuid" -> uuid(s.tag.operationId),
              "attempt_retirement_operation_uuid" -> Null, "obligation_state" -> text("PENDING"), "obligation_revision" -> signed(0),
              "verified_operation_uuid" -> Null, "verification_proof_bytes" -> Null, "verification_proof_sha256" -> Null,
              "last_mutation_operation_uuid" -> uuid(s.tag.operationId)), budget), false)
          val removed = MySqlNormalRecallCanonical.removal(c.removal, root)
          val ticket = MySqlNormalRecallCanonical.ticket(c.ticket, root)
          put(CompletionTable, row(CompletionTable, Map("binding_uuid" -> uuid(s.binding), "receipt_uuid" -> uuid(c.receipt),
            "completion_operation_uuid" -> uuid(s.tag.operationId), "completion_kind" -> text("RECALL"),
            "boot_uuid" -> uuid(s.tag.bootId), "boot_generation" -> signed(s.tag.bootGeneration),
            "world_identity" -> text(h.source.worldIdentity), "source_identity" -> text(h.source.sourceIdentity),
            "pod_uuid" -> uuid(h.pod), "owner_uuid" -> uuid(h.owner.get), "attempt_uuid" -> uuid(h.attempt), "footprint_uuid" -> uuid(h.footprint),
            "before_domain_version" -> signed(h.version), "after_domain_version" -> signed(c.afterVersion),
            "removal_result_bytes" -> removed.binaryCell, "removal_result_sha256" -> removed.sha256Cell,
            "ticket_result_bytes" -> ticket.binaryCell, "ticket_result_sha256" -> ticket.sha256Cell,
            "checkpoint_bytes" -> value(p.row, OperationTable, "scope_bytes"), "checkpoint_sha256" -> value(p.row, OperationTable, "scope_sha256"),
            "original_context_bytes" -> value(p.row, OperationTable, "original_context_bytes"),
            "original_context_sha256" -> value(p.row, OperationTable, "original_context_sha256"),
            "allowed_use" -> text("REARM_AFTER_RECALL"), "completion_state" -> text("CHECKPOINT_COMMITTED"),
            "consumed_by_operation_uuid" -> Null, "completion_revision" -> signed(0), "last_mutation_operation_uuid" -> uuid(s.tag.operationId)), budget), false)
          (c.targetObligations ++ c.targetDomainLocator).foreach { relation =>
            require(rows(relation.table).filter(selectedPod(relation.table)) == relation.rows,
              "C scope target differs from independently rebuilt complete effect")
          }
        }
      val fingerprint = MySqlFormat3Cell.Binary(MySqlFormat3Binary.copyOf(p.context.canonicalFingerprintSha256.toArray,
        budget, "expected original W receipt fingerprint"))
      put("fp_runtime_writer", update(w, "fp_runtime_writer", Map("last_commit_operation_uuid" -> uuid(p.context.operationUuid),
        "last_commit_fingerprint" -> fingerprint), budget), true)
      require(expected == target, "Recall full20 target changes values outside its exact original phase delta")
    }

  /** latest receipt 的完整当前image DATA；原Actor/native/SQL排他仍须另行核验。 */
  def verifyLatestReceiptData(actual: Image, writer: MySqlBusinessWriterRow,
      previousBoot: UUID, nativeEpoch: Long, root: MySqlFormat3ReadBudget): ResolvedCommittedPhaseData =
    require(actual != null && writer != null && previousBoot != null && root != null &&
      !root.isClosed && (actual.limitProfile eq root.profile) && actual.allocationBudget.sharesAllocationLedger(root),
      "original current complete image required for receipt continuation DATA")
    requireWriter(actual,writer,root)
    val operation = writer.lastOperationUuid.getOrElse(fail("actual W has no original receipt"))
    val candidates = actual.relation(OperationTable).rows.filter(r =>
      isUuid(r, OperationTable, "binding_uuid", writer.bindingUuid) && isUuid(r, OperationTable, "operation_uuid", operation))
    require(candidates.size == 1, "actual W receipt operation is absent or duplicated")
    val raw = candidates.head
    val reserve = if token(raw, "step") == "RESERVE" then None else
      val found = actual.relation(OperationTable).rows.filter(r =>
        isUuid(r, OperationTable, "binding_uuid", writer.bindingUuid) &&
          value(r, OperationTable, "operation_uuid") == value(raw, OperationTable, "reserve_operation_uuid"))
      require(found.size == 1, "actual W execute receipt lacks its original reserve")
      Some(found.head)
    val phase = verifyPersistedPhase(raw,reserve,root,exactInputs(actual,root))
    val resolved = resolveCommittedEffectData(phase, actual, root)
    requireCommittedEffectDelta(resolved, root)
    val h = phase.header
    val s = h.scope
    require(phase.context.bootUuid == previousBoot && phase.context.writerEpoch == nativeEpoch &&
      writer.writerEpoch == nativeEpoch && writer.bindingUuid == phase.context.bindingUuid &&
      writer.lastOperationUuid.contains(phase.context.operationUuid) &&
      writer.lastFingerprint.contains(phase.context.canonicalFingerprintSha256), "actual W/native/original context receipt differs")
    require(actual.schema == s.schema && actual.connection.serverUuid == s.server &&
      actual.connection.serverVersion == s.stableSession.serverVersion && actual.connection.effectiveUser == s.stableSession.effectiveUser,
      "latest receipt belongs to another original database identity")
    require(!writer.leaseUntil.isAfter(writer.databaseNow), "receipt continuation requires the actual old writer lease to have expired")
    writer.ownerBootUuid match
      case Some(owner) => require(owner == previousBoot && !writer.leaseUntil.isBefore(s.lease),
        "actual W owner or renewal lease is outside the original lifecycle")
      case None => () // 原held barrier/native owner负责排他；不把NULL形状当旧release回执。
    require(actual.relations.filterNot(_.table == "fp_runtime_writer") ==
      resolved.target.filterNot(_.table == "fp_runtime_writer"), "latest receipt target differs from complete actual non-W inventory")
    resolved

  /** 一个同epoch的完整历史区段。anchor仍是DATA，不取得native来源或C决定资格。 */
  final class ReceiptData private[MySqlNormalRecallRecovery] (
      val row: Vector[MySqlFormat3Cell], val scope: MySqlNormalRecallCanonical.DecodedScope,
      val context: MySqlOperationContext, val receiptFingerprint: Vector[Byte],
      val before: Vector[MySqlFormat3RelationSnapshot], val target: Vector[MySqlFormat3RelationSnapshot],
      val recall: Option[ResolvedCommittedPhaseData],
      private[mysql] val retainedDataBudget: MySqlFormat3ReadBudget)

  private final class HistoricalSourceChildReserveSegmentCandidate(
      val data: MySqlNormalRecallCanonical.HistoricalSourceChildReserveReceiptData,
      val before: MySqlScriptSchemaSnapshot, val target: MySqlScriptSchemaSnapshot)

  /** Historical R0 parent check uses only original Full26/P/C DATA and cannot supply a live parent capability. */
  private[mysql] def verifyHistoricalSourceChildReserveParentP2(before: Image,
      request: MySqlNormalRecallCanonical.SourceChildReserveRequestData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    require(before != null && request != null && root != null && !root.isClosed &&
      !root.isScopedReadBudget && dataBudget != null && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
      ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)),
      "historical R0 parent qualification requires original Full26, exact request, root and DATA owner")
    val inputs = exactInputs(before, root).getOrElse(
      fail("historical R0 parent P2 requires the original schema5 Full26 image"))
    before match
      case deployment: MySqlScriptSchemaSnapshot =>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment, inputs, root)
      case _ => fail("historical R0 parent P2 requires the original schema5 Full26 deployment image")
    require(inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "historical R0 parent P2 requires the exact Full26 input profile")
    val operationRows = before.relation(OperationTable).rows
    var p2: Vector[MySqlFormat3Cell] = null
    var p2Count = 0
    operationRows.foreach { row =>
      if isUuid(row, OperationTable, "binding_uuid", request.binding) &&
          isUuid(row, OperationTable, "workflow_uuid", request.parentRecallWorkflow) &&
          isUuid(row, OperationTable, "operation_uuid", request.parentP2Operation) then
        p2 = row
        p2Count = Math.addExact(p2Count, 1)
    }
    require(p2Count == 1, "historical R0 parent P2 is absent or duplicated in its exact Full26 BEFORE")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(p2, root.profile, dataBudget, Some(inputs))
    require(token(p2, "step") == "RECALL_PREPARE" && token(p2, "operation_state") == "COMMITTED" &&
      (value(p2, OperationTable, "pod_uuid") match
        case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 => true
        case _ => false),
      "historical R0 parent must be the exact committed P2 row with a typed Pod")
    val pod = value(p2, OperationTable, "pod_uuid") match
      case MySqlFormat3Cell.Binary(bytes) =>
        var high = 0L; var low = 0L; var index = 0
        while index < 8 do
          high = (high << 8) | (bytes.byteAt(index).toLong & 255L)
          low = (low << 8) | (bytes.byteAt(index + 8).toLong & 255L)
          index += 1
        val id = new UUID(high, low)
        require(MySqlNormalRecallRequest.nonZero(id), "historical R0 parent P2 Pod UUID cannot be zero")
        id
      case _ => fail("historical R0 parent P2 Pod UUID is malformed")
    val inventory = discover(before, request.binding, root, dataBudget)
    val reserveOperation = value(p2, OperationTable, "reserve_operation_uuid") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        var high = 0L; var low = 0L; var index = 0
        while index < 8 do
          high = (high << 8) | (bytes.byteAt(index).toLong & 255L)
          low = (low << 8) | (bytes.byteAt(index + 8).toLong & 255L)
          index += 1
        new UUID(high, low)
      case _ => fail("historical R0 parent P2 lacks its exact original reserve operation UUID")
    var reserve: Vector[MySqlFormat3Cell] = null
    var reserveCount = 0
    operationRows.foreach { row =>
      if isUuid(row, OperationTable, "binding_uuid", request.binding) &&
          isUuid(row, OperationTable, "workflow_uuid", request.parentRecallWorkflow) &&
          isUuid(row, OperationTable, "operation_uuid", reserveOperation) then
        reserve = row
        reserveCount = Math.addExact(reserveCount, 1)
    }
    require(reserveCount == 1 && token(reserve, "step") == "RESERVE" &&
      token(reserve, "operation_state") == "COMMITTED",
      "historical R0 parent P2 lacks one exact committed original reserve row")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(reserve, root.profile, dataBudget, Some(inputs))
    val phase =
      if dataBudget eq root then
        verifyPersistedPhaseOnBudget(p2, Some(reserve), root, dataBudget, Some(inputs))
      else
        var verified: PersistedPhaseData = null
        withHistoryComparisonBudget(root, dataBudget) {
          val activeComparisonBudget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          verified = verifyPersistedPhaseOnBudget(p2, Some(reserve), root, dataBudget,
            Some(inputs), activeComparisonBudget)
        }
        verified
    require(phase.phase == MySqlOperationEffectPhase.Execute && phase.header.scope.binding == request.binding &&
      phase.header.scope.workflow == request.parentRecallWorkflow && phase.header.pod == pod &&
      phase.row == p2,
      "historical R0 parent P2 raw phase differs from its exact committed P/C identity")
    val originalPrepare = phase.prepare.originalPrepare.getOrElse(
      fail("historical R0 parent P2 does not retain its exact original P identity"))
    val parentInventories = inventory.filter { entry =>
      entry.workflow == request.parentRecallWorkflow && entry.pod == pod &&
        entry.prepare == originalPrepare && entry.checkpoint == phase.prepare.originalCheckpoint &&
        (entry.cursor match
          case MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical(operation) =>
            operation.operationUuid == request.parentP2Operation && operation.row == p2
          case _ => false)
    }
    require(parentInventories.size == 1,
      "historical R0 parent P2 must be the exact committed physical-resume cursor of its complete original P/C workflow")
    val resolved = resolveCommittedEffectDataOnBudget(phase, before, root, dataBudget)
    requireCommittedEffectDelta(resolved, root, dataBudget, propagateActiveComparisonBudget = true)

  private def verifyHistoricalSourceChildReserveCandidate(row: Vector[MySqlFormat3Cell],
      inventory: Image, cursor: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): HistoricalSourceChildReserveSegmentCandidate =
    require(cursor != null && inventory != null && root != null && dataBudget != null,
      "historical R0 candidate requires its original complete cursor, inventory and DATA owner")
    val inputs = exactInputs(inventory, root).getOrElse(
      fail("historical R0 candidate requires a complete schema5 Full26 inventory"))
    require((exactInputs(cursor, root).getOrElse(null) eq inputs) &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,
      "historical R0 candidate cursor and inventory must retain the same exact Full26 inputs")
    val decoded = MySqlNormalRecallCanonical.decodeHistoricalSourceChildReserveReceipt(
      row, inputs, root, dataBudget)
    val shapes = inputs.canonicalOrder
    require(decoded.preimage.relations.size == shapes.size,
      "historical R0 candidate BEFORE effect lacks its full original relation vector")
    var rows = 0L
    var cells = 0L
    var relationIndex = 0
    while relationIndex < shapes.size do
      val relation = decoded.preimage.relations(relationIndex)
      val shape = shapes(relationIndex)
      require(relation.table == shape.table,
        "historical R0 candidate BEFORE relation order differs from its original Full26 input")
      rows = Math.addExact(rows, relation.rows.size.toLong)
      cells = Math.addExact(cells,
        Math.multiplyExact(relation.rows.size.toLong, shape.columns.size.toLong))
      relationIndex += 1
    val retained = Math.addExact(512L, Math.addExact(Math.multiplyExact(rows, 128L),
      Math.multiplyExact(cells, 16L)))
    dataBudget.reserveRetainedMemory(retained,
      "historical R0 literal BEFORE snapshot rows and cells before allocation")
    val beforeRelations = Vector.tabulate(shapes.size) { relationIndex =>
      val shape = shapes(relationIndex)
      val relation = decoded.preimage.relations(relationIndex)
      val literalRows = Vector.tabulate(relation.rows.size) { rowIndex =>
        val cells = relation.rows(rowIndex)
        require(cells.size == shape.columns.size,
          "historical R0 BEFORE row differs from its exact Full26 width")
        Vector.tabulate(cells.size) { columnIndex =>
          cells(columnIndex) match
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Literal(value) => value
            case MySqlNormalRecallCanonical.SourceChildReserveEffectCell.Derived(_) =>
              throw new java.sql.SQLDataException("historical R0 BEFORE cannot contain a derived marker")
        }
      }
      MySqlFormat3RelationSnapshot(shape.table, literalRows)
    }
    val before = MySqlScriptSchemaSnapshot.decoded(cursor.schema, cursor.connection,
      cursor.observationScope, cursor.maxAllowedPacketBytes, inputs, beforeRelations, dataBudget)
    MySqlNormalRecallSourceWorkflow.verifyHistoricalSourceChildReserveDelta(
      decoded, before, cursor, root, dataBudget)
    dataBudget.reserveRetainedMemory(256L,
      "historical R0 complete candidate wrapper before allocation")
    new HistoricalSourceChildReserveSegmentCandidate(decoded, before, cursor)

  private def verifyAnyReceipt(row: Vector[MySqlFormat3Cell], inventory: Image,
      cursor: Image, root: MySqlFormat3ReadBudget): ReceiptData =
    verifyAnyReceipt(row, inventory, cursor, root, root)

  private def verifyAnyReceipt(row: Vector[MySqlFormat3Cell], inventory: Image,
      cursor: Image, root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): ReceiptData =
    requireHistoryDataBudget(root, dataBudget)
    val scope = value(row, OperationTable, "scope_bytes") match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => fail("receipt scope missing")
    if MySqlNormalRecallCanonical.isHistoricalSourceChildReserveScope(scope) then
      throw new java.sql.SQLDataException(
        "historical SOURCE_CHILD_RESERVE R0 requires complete anchored receipt history and predecessor qualification")
    if MySqlNormalRecallCanonical.isSourceHistoryDomain(scope) then
      throw new java.sql.SQLDataException(
        "unsupported or unknown SOURCE_* historical receipt domain fails closed")
    def starts(domain: String): Boolean = scope.length >= domain.length + 4 &&
      (0 until 4).forall(i => (scope.byteAt(i) & 255) == ((domain.length >>> (24 - i * 8)) & 255)) &&
      domain.indices.forall(i => scope.byteAt(i + 4) == domain.charAt(i).toByte)
    dataBudget.reserveRetainedMemory(512L, "complete kinded receipt DATA")
    if starts(MySqlNormalRecallCanonical.PrepareDomain) || starts(MySqlNormalRecallCanonical.CheckpointDomain) then
      val reserve = if token(row, "step") == "RESERVE" then None else
        val found = inventory.relation(OperationTable).rows.filter(r =>
          value(r, OperationTable, "binding_uuid") == value(row, OperationTable, "binding_uuid") &&
            value(r, OperationTable, "operation_uuid") == value(row, OperationTable, "reserve_operation_uuid"))
        require(found.size == 1, "receipt lacks its actual original reserve")
        Some(found.head)
      val inputs = exactInputs(inventory, root)
      val phase = verifyPersistedPhaseOnBudget(row, reserve, root, dataBudget, inputs)
      val resolved = resolveCommittedEffectDataOnBudget(phase, inventory, root, dataBudget)
      requireCommittedEffectDelta(resolved, root)
      new ReceiptData(row, phase.header.scope, phase.context, phase.context.canonicalFingerprintSha256,
        resolved.before, resolved.target, Some(resolved), dataBudget)
    else
      val data = if dataBudget eq root then MySqlHistoricalBusinessReceipt.verify(row, inventory, cursor, root)
        else MySqlHistoricalBusinessReceipt.verify(row, inventory, cursor, root, dataBudget)
      new ReceiptData(row, data.scope, data.context, data.receiptFingerprint, data.before, data.target, None, dataBudget)

  def verifyLatestAnyReceiptData(actual: Image, writer: MySqlBusinessWriterRow,
      previousBoot: UUID, nativeEpoch: Long, root: MySqlFormat3ReadBudget): ReceiptData =
    require(actual != null && writer != null && previousBoot != null && root != null && !root.isClosed &&
      (actual.limitProfile eq root.profile) && actual.allocationBudget.sharesAllocationLedger(root), "original latest receipt inventory/root")
    requireWriter(actual,writer,root)
    val id = writer.lastOperationUuid.getOrElse(fail("latest receipt absent"))
    val found = actual.relation(OperationTable).rows.filter(r => isUuid(r, OperationTable, "binding_uuid", writer.bindingUuid) &&
      isUuid(r, OperationTable, "operation_uuid", id))
    require(found.size == 1, "latest receipt original row absent or ambiguous")
    val data = verifyAnyReceipt(found.head, actual, actual, root)
    val s = data.scope
    require(data.context.bootUuid == previousBoot && data.context.writerEpoch == nativeEpoch && writer.writerEpoch == nativeEpoch &&
      writer.bindingUuid == data.context.bindingUuid && writer.lastFingerprint.contains(data.receiptFingerprint), "latest original native/W receipt differs")
    require(!writer.leaseUntil.isAfter(writer.databaseNow), "latest original writer has not expired")
    writer.ownerBootUuid.foreach(owner => require(owner == previousBoot && !writer.leaseUntil.isBefore(s.lease), "latest original owner/lease differs"))
    require(actual.schema == s.schema && actual.connection.serverUuid == s.server &&
      actual.connection.serverVersion == s.stableSession.serverVersion && actual.connection.effectiveUser == s.stableSession.effectiveUser,
      "latest original database identity differs")
    require(actual.relations.filterNot(_.table == "fp_runtime_writer") == data.target.filterNot(_.table == "fp_runtime_writer"),
      "latest full non-W target differs")
    data

  final class ReceiptHistoryData private[MySqlNormalRecallRecovery] (
      val inventory: Image, val current: Image, val anchor: Image, val newestFirst: Vector[ReceiptData],
      private[mysql] val retainedDataBudget: MySqlFormat3ReadBudget)

  /** 已验证完整 O/W 链中的单个 source-write DATA；没有 SQL/native 资格或解除保护语义。 */
  final class SourceWriteHistoryData private[MySqlNormalRecallRecovery] (
      val write: MySqlNormalRecallCanonical.DecodedSourceWrite,
      val sourceOperationsOldestFirst: Vector[ReceiptData],
      val openObligations: Vector[Vector[MySqlFormat3Cell]],
      val sourceUnitReservations: Vector[Vector[MySqlFormat3Cell]],
      val capturedObligationRevisions: MySqlNormalRecallCanonical.DecodedSourceObligationRevisions,
      val requiresRecapture: Boolean)

  /**
   * 仅接受本 recovery owner 构造的全量 ReceiptHistoryData；逐段重放 source-write 行差额，
   * 将每次状态变化绑回已验证的原 operation、W 提交指纹与 held CARRIER reservation。
   */
  def verifySourceWriteHistory(history: ReceiptHistoryData, binding: UUID, workflow: UUID,
      write: UUID, root: MySqlFormat3ReadBudget): SourceWriteHistoryData =
    require(history != null && MySqlNormalRecallRequest.nonZero(binding) &&
      MySqlNormalRecallRequest.nonZero(workflow) && MySqlNormalRecallRequest.nonZero(write) &&
      root != null && !root.isClosed && !root.isScopedReadBudget,
      "original source-write history, IDs and retained full26 root required")
    val inventory = history.inventory
    val current = history.current
    val anchor = history.anchor
    val images = Vector(inventory, current, anchor)
    require(images.forall(image => image != null && !image.allocationBudget.isClosed &&
      (image.limitProfile eq root.profile) && image.allocationBudget.sharesAllocationLedger(root)),
      "source-write history images must retain the same live original root")
    images.foreach { image =>
      require(exactInputs(image, root).nonEmpty, "source-write history requires the original schema5 Full26 family")
      requireImageGraph(image, root)
    }
    require(inventory.schema == current.schema && inventory.schema == anchor.schema &&
      inventory.connection.serverUuid == current.connection.serverUuid && current.connection.serverUuid == anchor.connection.serverUuid &&
      inventory.connection.serverVersion == current.connection.serverVersion && current.connection.serverVersion == anchor.connection.serverVersion &&
      inventory.connection.effectiveUser == current.connection.effectiveUser && current.connection.effectiveUser == anchor.connection.effectiveUser,
      "source-write history changed its original Full26 database identity")
    val stepCount = history.newestFirst.size.toLong
    require(stepCount <= root.profile.maxMembers(), "source-write history exceeds the retained operation bound")
    root.reserveRetainedMemory(Math.addExact(2048L, Math.multiplyExact(stepCount + 1L, 512L)),
      "source-write validated O/W/R replay DATA")

    def matchingWriteInRelations(relations: Vector[MySqlFormat3RelationSnapshot]): Option[Vector[MySqlFormat3Cell]] =
      val table = relations.find(_.table == "physical_source_write").getOrElse(fail("Full26 source-write relation absent"))
      val rows = table.rows.filter(row => isUuid(row, "physical_source_write", "binding_uuid", binding) &&
        isUuid(row, "physical_source_write", "write_uuid", write))
      require(rows.size <= 1, "source-write primary key is duplicated in a historical image")
      rows.headOption
    def matchingWriteInImage(image: Image): Option[Vector[MySqlFormat3Cell]] =
      matchingWriteInRelations(image.relations)
    def sameWrite(left: Option[Vector[MySqlFormat3Cell]], right: Option[Vector[MySqlFormat3Cell]]): Boolean = left == right
    def carrierReservation(relations: Vector[MySqlFormat3RelationSnapshot],
        decoded: MySqlNormalRecallCanonical.DecodedSourceWrite): Vector[MySqlFormat3Cell] =
      val table = relations.find(_.table == ReservationTable).getOrElse(fail("Full26 reservation relation absent"))
      val rows = table.rows.filter(row => isUuid(row, ReservationTable, "binding_uuid", binding) &&
        value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("CARRIER") &&
        (value(row, ReservationTable, "resource_key_hash") match
          case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(decoded.resourceKey.keyHashSha256)
          case _ => false))
      require(rows.size == 1, "source write has no unique original CARRIER reservation")
      val row = rows.head
      value(row, ReservationTable, "resource_key") match
        case MySqlFormat3Cell.Binary(key) =>
          require(key.equalsBytes(decoded.resourceKey.copyCanonicalBytes(root, "source-write held CARRIER full key")),
            "source-write CARRIER reservation full key differs")
        case _ => fail("source-write CARRIER reservation key is absent")
      require(value(row, ReservationTable, "reservation_state") == MySqlFormat3Cell.Text("HELD") &&
        isUuid(row, ReservationTable, "workflow_uuid", workflow),
        "source write requires its original child workflow's held CARRIER reservation")
      row
    def requireSourceOperation(receipt: ReceiptData, before: Vector[MySqlFormat3RelationSnapshot],
        target: Vector[MySqlFormat3RelationSnapshot], decodedBefore: Option[MySqlNormalRecallCanonical.DecodedSourceWrite],
        decodedAfter: MySqlNormalRecallCanonical.DecodedSourceWrite): Unit =
      val operation = receipt.context.operationUuid
      val opRow = receipt.row
      require(receipt.context.bindingUuid == binding && isUuid(opRow, OperationTable, "binding_uuid", binding) &&
        isUuid(opRow, OperationTable, "operation_uuid", operation) && isUuid(opRow, OperationTable, "workflow_uuid", workflow) &&
        token(opRow, "operation_state") == "COMMITTED" &&
        Set("SOURCE_WRITE_PREPARE", "SOURCE_WRITE_RESULT", "SOURCE_WRITE_RECONCILE").contains(token(opRow, "step")),
        "source-write row mutation lacks its exact committed source child operation")
      require(inventory.relation(OperationTable).rows.contains(opRow),
        "source-write operation row is not in the original complete receipt inventory")
      val afterWriter = target.find(_.table == "fp_runtime_writer").getOrElse(fail("source-write target W absent")).rows
      require(afterWriter.size == 1 && isUuid(afterWriter.head, "fp_runtime_writer", "binding_uuid", binding) &&
        isUuid(afterWriter.head, "fp_runtime_writer", "last_commit_operation_uuid", operation) &&
        (value(afterWriter.head, "fp_runtime_writer", "last_commit_fingerprint") match
          case MySqlFormat3Cell.Binary(fingerprint) => fingerprint.equalsVector(receipt.receiptFingerprint)
          case _ => false), "source-write receipt is not the exact committed O/W edge")
      val beforeReservation = carrierReservation(before, decodedAfter)
      val afterReservation = carrierReservation(target, decodedAfter)
      require(isUuid(afterReservation, ReservationTable, "operation_uuid", operation) &&
        isUuid(afterReservation, ReservationTable, "last_mutation_operation_uuid", operation),
        "source child CARRIER reservation was not retargeted to this exact operation")
      def requireReservationPredecessor(reservation: Vector[MySqlFormat3Cell], label: String): Unit =
        val predecessor = value(reservation, ReservationTable, "operation_uuid") match
          case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
            val raw = bytes.copyBytes(root, "source reservation predecessor operation UUID")
            val buffer = java.nio.ByteBuffer.wrap(raw)
            new UUID(buffer.getLong(), buffer.getLong())
          case _ => fail(label + " reservation predecessor operation is absent")
        require(isUuid(reservation, ReservationTable, "last_mutation_operation_uuid", predecessor),
          label + " reservation operation and last-mutation predecessor differ")
        val predecessors = before.find(_.table == OperationTable).getOrElse(fail("source operation predecessor relation absent")).rows.filter(row =>
          isUuid(row, OperationTable, "binding_uuid", binding) &&
            isUuid(row, OperationTable, "operation_uuid", predecessor))
        require(predecessors.size == 1 && token(predecessors.head, "operation_state") == "COMMITTED" &&
          isUuid(predecessors.head, OperationTable, "workflow_uuid", workflow) &&
          inventory.relation(OperationTable).rows.contains(predecessors.head),
          label + " reservation predecessor is not the exact committed source-child operation")
      requireReservationPredecessor(beforeReservation, "CARRIER")
      val beforeRevision = value(beforeReservation, ReservationTable, "reservation_revision") match
        case MySqlFormat3Cell.SignedInteger(value) => value
        case _ => fail("source CARRIER reservation revision is absent")
      val afterRevision = value(afterReservation, ReservationTable, "reservation_revision") match
        case MySqlFormat3Cell.SignedInteger(value) => value
        case _ => fail("source CARRIER target reservation revision is absent")
      require(afterRevision == Math.addExact(beforeRevision, 1L),
        "source child CARRIER reservation must advance exactly once with its operation")
      decodedBefore.foreach { old =>
        require(old.state == "PREPARED" || old.state == "FAILED_NEEDS_RECONCILIATION",
          "resolved source-write identity cannot be rewritten by a later operation")
        require(decodedAfter.hasSameImmutableIdentity(old) && decodedAfter.preparedOperation == old.preparedOperation &&
          decodedAfter.resolvedOperation.contains(operation) && decodedAfter.lastMutation == operation &&
          decodedAfter.revision == Math.addExact(old.revision, 1L),
          "source-write result changed its immutable tuple, owner, or revision")
        val step = token(opRow, "step")
        require((step == "SOURCE_WRITE_RESULT" && old.state == "PREPARED") ||
          (step == "SOURCE_WRITE_RECONCILE" && (old.state == "PREPARED" || old.state == "FAILED_NEEDS_RECONCILIATION")),
          "source-write state transition used the wrong closed command")
        if old.state == "FAILED_NEEDS_RECONCILIATION" then
          require(decodedAfter.failureDiagnostic == old.failureDiagnostic &&
            decodedAfter.failureDiagnosticSha256 == old.failureDiagnosticSha256,
            "source reconciliation replaced the original failure diagnostic identity")
      }
      if decodedBefore.isEmpty then
        require(token(opRow, "step") == "SOURCE_WRITE_PREPARE" && decodedAfter.state == "PREPARED" &&
          decodedAfter.preparedOperation == operation && decodedAfter.lastMutation == operation &&
          decodedAfter.resolvedOperation.isEmpty,
          "source-write insertion must be the exact PREPARED target of SOURCE_WRITE_PREPARE")
      val beforeObligations = before.find(_.table == "physical_source_obligation").getOrElse(fail("source obligation relation absent")).rows
      val afterObligations = target.find(_.table == "physical_source_obligation").getOrElse(fail("source obligation relation absent")).rows
      val obligationKeyColumns = Vector("binding_uuid", "unit_uuid", "forest_uuid", "carrier_key_hash", "purpose")
      def obligationKey(row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
        obligationKeyColumns.map(name => value(row, "physical_source_obligation", name))
      val capturedRevisions = decodedAfter.obligationRevisionData.values
      def matchesCapturedKey(row: Vector[MySqlFormat3Cell], captured: MySqlNormalRecallCanonical.SourceObligationRevision): Boolean =
        isUuid(row, "physical_source_obligation", "binding_uuid", captured.binding) &&
          isUuid(row, "physical_source_obligation", "unit_uuid", captured.unit) &&
          isUuid(row, "physical_source_obligation", "forest_uuid", captured.forest) &&
          (value(row, "physical_source_obligation", "carrier_key_hash") match
            case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(captured.carrierKeyHash)
            case _ => false) &&
          value(row, "physical_source_obligation", "purpose") == MySqlFormat3Cell.Text(captured.purpose)
      require(capturedRevisions != null && capturedRevisions.map(item =>
        (item.binding, item.unit, item.forest, item.carrierKeyHash, item.purpose)).distinct.size == capturedRevisions.size,
        "source-write obligation revision DATA must retain unique exact keys")
      capturedRevisions.foreach { captured =>
        val originalRows = beforeObligations.filter(matchesCapturedKey(_, captured))
        require(originalRows.size == 1 && isUuid(originalRows.head, "physical_source_obligation", "binding_uuid", binding) &&
          (value(originalRows.head, "physical_source_obligation", "obligation_revision") match
            case MySqlFormat3Cell.SignedInteger(revision) => revision == captured.revision
            case _ => false) &&
          (value(originalRows.head, "physical_source_obligation", "carrier_key") match
            case MySqlFormat3Cell.Binary(key) => key.compareTo(captured.carrierKey) == 0
            case _ => false),
          "source-write captured obligation key/revision/full CARRIER key is absent from its exact predecessor")
      }
      val beforeByKey = beforeObligations.map(row => obligationKey(row) -> row).toMap
      val afterByKey = afterObligations.map(row => obligationKey(row) -> row).toMap
      require(beforeByKey.size == beforeObligations.size && afterByKey.size == afterObligations.size &&
        beforeByKey.keySet == afterByKey.keySet,
        "SOURCE_WRITE operations cannot add or delete source obligations")
      beforeByKey.foreach { (key, old) =>
        val next = afterByKey(key)
        val purpose = value(old, "physical_source_obligation", "purpose")
        val oldRevision = value(old, "physical_source_obligation", "obligation_revision")
        val newRevision = value(next, "physical_source_obligation", "obligation_revision")
        require(oldRevision == newRevision, "SOURCE_WRITE operations cannot change captured obligation revisions")
        if purpose == MySqlFormat3Cell.Text("CURRENT_HOLD") then
          require(old == next, "CURRENT_HOLD cannot be discharged or rewritten by a source write")
        if old != next then
          val changedColumns = MySqlFormat3RelationShapes.forTable("physical_source_obligation").columns.zip(old.zip(next))
            .collect { case (column, (left, right)) if left != right => column.name }.toSet
          require(token(receipt.row, "step") == "SOURCE_WRITE_PREPARE" &&
            capturedRevisions.exists(matchesCapturedKey(old, _)) &&
            value(old, "physical_source_obligation", "obligation_state") == MySqlFormat3Cell.Text("OPEN") &&
            value(next, "physical_source_obligation", "obligation_state") == MySqlFormat3Cell.Text("OPEN") &&
            purpose != MySqlFormat3Cell.Text("CURRENT_HOLD") &&
            value(old, "physical_source_obligation", "write_uuid") == MySqlFormat3Cell.SqlNull &&
            isUuid(next, "physical_source_obligation", "write_uuid", write) &&
            isUuid(next, "physical_source_obligation", "last_mutation_operation_uuid", receipt.context.operationUuid) &&
            changedColumns.subsetOf(Set("write_uuid", "last_mutation_operation_uuid")),
            "SOURCE_WRITE_PREPARE may only attach an open non-CURRENT_HOLD obligation at its captured revision")
        }
      val attachedBefore = beforeObligations.filter(row => isUuid(row, "physical_source_obligation", "binding_uuid", binding) &&
        isUuid(row, "physical_source_obligation", "write_uuid", write))
      val attachedAfter = afterObligations.filter(row => isUuid(row, "physical_source_obligation", "binding_uuid", binding) &&
        isUuid(row, "physical_source_obligation", "write_uuid", write))
      if decodedBefore.isEmpty then
        val expectedAttached = capturedRevisions.filterNot(_.purpose == "CURRENT_HOLD")
        require(attachedBefore.isEmpty && attachedAfter.size == expectedAttached.size &&
          expectedAttached.forall(captured => attachedAfter.count(matchesCapturedKey(_, captured)) == 1) &&
          attachedAfter.forall(row => capturedRevisions.exists(captured =>
            captured.purpose != "CURRENT_HOLD" && matchesCapturedKey(row, captured))),
          "SOURCE_WRITE_PREPARE must attach exactly its captured open non-CURRENT_HOLD obligation set")
      else
        require(attachedBefore == attachedAfter,
          "SOURCE_WRITE_RESULT and RECONCILE cannot alter the captured source-obligation attachments")
      val beforePodReservations = before.find(_.table == ReservationTable).getOrElse(fail("Full26 reservation relation absent")).rows.filter(row =>
        value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("POD"))
      val afterPodReservations = target.find(_.table == ReservationTable).getOrElse(fail("Full26 reservation relation absent")).rows.filter(row =>
        value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("POD"))
      require(beforePodReservations == afterPodReservations,
        "source child cannot acquire, retarget, or release the original P2 POD reservation")

    var expected = matchingWriteInImage(anchor)
    expected.foreach(row => require(isUuid(row, "physical_source_write", "workflow_uuid", workflow),
      "anchor source-write belongs to another child workflow"))
    var latestDecoded = expected.map(MySqlNormalRecallCanonical.decodeSourceWrite(_, root))
    val related = Vector.newBuilder[ReceiptData]
    history.newestFirst.reverse.foreach { receipt =>
      val before = matchingWriteInRelations(receipt.before)
      val target = matchingWriteInRelations(receipt.target)
      require(sameWrite(before, expected), "source-write history skips or rewrites its exact predecessor row")
      if !sameWrite(before, target) then
        require(target.nonEmpty, "source-write history cannot delete a prepared/result row")
        val afterData = MySqlNormalRecallCanonical.decodeSourceWrite(target.get, root)
        require(afterData.binding == binding && afterData.workflow == workflow && afterData.write == write,
          "source-write history changed its original binding/workflow/write identity")
        val beforeData = before.map(MySqlNormalRecallCanonical.decodeSourceWrite(_, root))
        requireSourceOperation(receipt, receipt.before, receipt.target, beforeData, afterData)
        if afterData.state == "FAILED_NEEDS_RECONCILIATION" then
          require(token(receipt.row, "step") == "SOURCE_WRITE_RESULT" || token(receipt.row, "step") == "SOURCE_WRITE_RECONCILE",
            "failed source-write diagnostic is not attached to its resolving operation")
        related += receipt
        latestDecoded = Some(afterData)
      expected = target
    }
    require(sameWrite(expected, matchingWriteInImage(current)) && sameWrite(expected, matchingWriteInImage(inventory)),
      "source-write DATA differs from the complete current Full26 and receipt inventory")
    val currentData = latestDecoded.map { data =>
      val actual = MySqlNormalRecallCanonical.decodeSourceWrite(matchingWriteInImage(current).get, root)
      require(actual.hasSameImmutableIdentity(data) && actual.state == data.state &&
        actual.resolvedOperation == data.resolvedOperation && actual.revision == data.revision &&
        actual.lastMutation == data.lastMutation && actual.durableReceipt == data.durableReceipt &&
        actual.durableReceiptSha256 == data.durableReceiptSha256 &&
        actual.failureDiagnostic == data.failureDiagnostic &&
        actual.failureDiagnosticSha256 == data.failureDiagnosticSha256,
        "source-write final result differs from its last verified O/W receipt")
      actual
    }.getOrElse(fail("source-write is absent from its verified original operation history"))
    val sourceObligationRows = current.relation("physical_source_obligation").rows
    root.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(sourceObligationRows.size.toLong, 192L)),
      "source-write open obligation DATA")
    val openObligations = sourceObligationRows.filter(row =>
      isUuid(row, "physical_source_obligation", "binding_uuid", binding) &&
        isUuid(row, "physical_source_obligation", "write_uuid", write) &&
        value(row, "physical_source_obligation", "obligation_state") == MySqlFormat3Cell.Text("OPEN"))
    val sourceUnitReservationRows = current.relation(ReservationTable).rows.filter(row =>
      isUuid(row, ReservationTable, "binding_uuid", binding) &&
        value(row, ReservationTable, "resource_kind") == MySqlFormat3Cell.Text("SOURCE_UNIT") &&
        isUuid(row, ReservationTable, "workflow_uuid", workflow))
    new SourceWriteHistoryData(currentData, related.result(), openObligations, sourceUnitReservationRows,
      currentData.obligationRevisionData, requiresRecapture = true)

  final class CheckpointDecisionData private[MySqlNormalRecallRecovery] (
      val inventory: MySqlNormalRecallRecoveryInventory,
      val observation: MySqlNormalRecallCheckpointObservation,
      val committed: Option[ResolvedCommittedPhaseData], val pending: Option[PendingPhaseImages])

  /** full history由原Actor另证来源与连续性；此处不发行来源/fence或任何行动许可。 */
  def classifyCheckpointData(actual: Image, history: Vector[ReceiptHistoryData],
      inventory: MySqlNormalRecallRecoveryInventory, root: MySqlFormat3ReadBudget): CheckpointDecisionData =
    require(actual != null && history != null && inventory != null && root != null && !root.isClosed &&
      !root.isScopedReadBudget && (actual.limitProfile eq root.profile) && actual.allocationBudget.sharesAllocationLedger(root),
      "original complete inventory/history/root required for C DATA")
    requireImageGraph(actual,root)
    val count = actual.relation(OperationTable).rows.size.toLong
    val steps = history.foldLeft(0L)((n, h) => Math.addExact(n, h.newestFirst.size.toLong))
    require(count <= root.profile.maxMembers() && steps <= count, "C inventory/history exceeds original member bound")
    root.reserveRetainedMemory(Math.addExact(4096L, Math.multiplyExact(count + steps, 256L)), "C decision original indexes and DATA")
    val receipts = history.flatMap(_.newestFirst)
    val keys = receipts.map(p => (p.context.bindingUuid, p.context.operationUuid))
    require(keys.distinct.size == keys.size, "C history repeats an original receipt")
    val originals = actual.relation(OperationTable).rows
    require(receipts.forall(p => originals.contains(p.row)), "C history differs from actual original RAW inventory")
    val phases = receipts.flatMap(_.recall)
    val workflow = originals.filter(r => isUuid(r, OperationTable, "workflow_uuid", inventory.workflow))
    def op(id: UUID): Vector[MySqlFormat3Cell] =
      val found = workflow.filter(isUuid(_, OperationTable, "operation_uuid", id))
      require(found.size == 1, "C workflow original operation absent or ambiguous")
      found.head
    def committed(row: Vector[MySqlFormat3Cell]): ResolvedCommittedPhaseData =
      val found = phases.filter(_.phase.row == row)
      require(found.size == 1 && token(row, "operation_state") == "COMMITTED", "C lacks its actual positive history phase")
      found.head
    val pRow = op(inventory.prepare)
    val unsupported = workflow.exists(r => !Set("RECALL_PREPARE", "RECALL_CHECKPOINT", "RESERVE").contains(token(r, "step"))) ||
      workflow.count(r => token(r, "step") == "RECALL_PREPARE") != 1 ||
      token(pRow, "operation_state") != "COMMITTED" ||
      inventory.checkpoint.exists(id => !Set("RESERVED", "COMMITTED").contains(token(op(id), "operation_state")))
    if unsupported then
      return new CheckpointDecisionData(inventory, MySqlNormalRecallCheckpointObservation.OriginalResponsibility, None, None)
    val p = committed(pRow)
    require(p.phase.checkpoint.isEmpty && p.phase.prepare.purpose == MySqlNormalRecallPurpose.NormalRecall &&
      p.phase.header.previous.isEmpty && p.phase.header.pod == inventory.pod &&
      p.phase.header.scope.workflow == inventory.workflow, "C decision lost its original P scope")
    val rp = committed(op(p.phase.header.scope.reserve))
    require(rp.phase.phase == MySqlOperationEffectPhase.Reserve && rp.phase.header == p.phase.header &&
      rp.target == p.before, "P/Rp original full20 relation differs")
    inventory.checkpoint match
      case None =>
        require(workflow.toSet == Set(pRow, rp.phase.row), "no-C workflow contains unexplained original successors")
        require(!actual.relation(CompletionTable).rows.exists(r =>
          isUuid(r, CompletionTable, "completion_operation_uuid", inventory.prepare)), "no-C workflow has orphan completion")
        new CheckpointDecisionData(inventory, MySqlNormalRecallCheckpointObservation.NotReserved, None, None)
      case Some(id) =>
        val cRow = op(id)
        val reserveId = value(cRow, OperationTable, "reserve_operation_uuid")
        val reserves = workflow.filter(r => value(r, OperationTable, "operation_uuid") == reserveId)
        require(reserves.size == 1, "C original Rc absent or ambiguous")
        val rc = committed(reserves.head)
        val c = verifyPersistedPhase(cRow,Some(rc.phase.row),root,exactInputs(actual,root))
        require(c.checkpoint.nonEmpty && c.prepare == p.phase.prepare && c.header.previous.contains(inventory.prepare) &&
          rc.phase.phase == MySqlOperationEffectPhase.Reserve && rc.phase.header == c.header,
          "C/Rc scope does not name its exact original P")
        require(workflow.toSet == Set(pRow, rp.phase.row, cRow, rc.phase.row), "C workflow contains unexplained successors")
        if token(cRow, "operation_state") == "COMMITTED" then
          val positive = committed(cRow)
          require(positive.before == rc.target, "committed C before differs from actual Rc target")
          new CheckpointDecisionData(inventory, MySqlNormalRecallCheckpointObservation.Committed, Some(positive), None)
        else
          require(!phases.exists(_.phase.context.operationUuid == id) &&
            !actual.relation(CompletionTable).rows.exists(r => isUuid(r, CompletionTable, "completion_operation_uuid", id)),
            "pending C has a positive phase or orphan completion")
          val pending = resolvePendingCheckpointImages(c, actual, root)
          require(pending.before == rc.target, "RESERVED C before differs from actual Rc target")
          new CheckpointDecisionData(inventory, MySqlNormalRecallCheckpointObservation.ReservedNeedsSettlement, None, Some(pending))

  def verifyReceiptHistoryData(actual: Image, anchor: Image,
      root: MySqlFormat3ReadBudget): ReceiptHistoryData =
    verifyReceiptSegment(actual, actual, anchor, None, root)

  /** 历史完整body只恢复DATA；当前真实退出/租约/来源资格由原actor另核，不补造历史databaseNow。 */
  private[mysql] def verifyCompleteReceiptHistoryData(inventory:Image,current:Image,anchor:Image,
      root:MySqlFormat3ReadBudget):ReceiptHistoryData =
    verifyReceiptSegment(inventory,current,anchor,None,root)

  /** H3 history is built only inside the exact registered allocation holder and retains its DATA child. */
  private[mysql] def verifyCompleteReceiptHistoryData(inventory: Image, current: Image, anchor: Image,
      exactRoot: MySqlFormat3ReadBudget,
      exactAllocation: MySqlFencedTransaction.SourceReservePlanAllocation,
      exactParent: MySqlFencedTransaction.RestrictedSourceParent5,
      exactLedger: MySqlFormat3AllocationLedger,
      exactIntent: MySqlOwnedIoAdapter.RestrictedSourceReserveIntent): ReceiptHistoryData =
    require(exactRoot != null && exactAllocation != null && exactParent != null && exactLedger != null && exactIntent != null,
      "exact original source RESERVE holder and complete parent/root/ledger/intent required for H3 history")
    exactAllocation.withPlanDataBudget(exactParent, exactRoot, exactLedger, exactIntent) { dataBudget =>
      verifyReceiptSegment(inventory, current, anchor, None, exactRoot, dataBudget)
    }

  /** DATA only: the original actor separately owns the stopped session/native source and confirmed epoch edges. */
  def verifyStoppedReceiptHistoryData(inventory: Image, current: Image,
      anchor: Image, stopped: MySqlBusinessWriterRow,
      root: MySqlFormat3ReadBudget): ReceiptHistoryData =
    require(stopped != null, "complete original stopped W observation required")
    requireWriter(current,stopped,root)
    require(!stopped.leaseUntil.isAfter(stopped.databaseNow), "original segment terminal W observation must be at or after lease expiry")
    val w = anchor.relation("fp_runtime_writer").rows
    require(w.size == 1 && isUuid(w.head, "fp_runtime_writer", "binding_uuid", stopped.bindingUuid) &&
      value(w.head, "fp_runtime_writer", "writer_epoch") == MySqlFormat3Cell.SignedInteger(stopped.writerEpoch) &&
      value(w.head, "fp_runtime_writer", "owner_boot_uuid") != MySqlFormat3Cell.SqlNull &&
      stopped.ownerBootUuid.forall(isUuid(w.head, "fp_runtime_writer", "owner_boot_uuid", _)),
      "original segment terminal W binding/epoch/owner differs from its anchor")
    verifyReceiptSegment(inventory, current, anchor, Some(stopped), root)

  private def verifyReceiptSegment(inventory: Image, actual: Image,
      anchor: Image, stopped: Option[MySqlBusinessWriterRow],
      root: MySqlFormat3ReadBudget): ReceiptHistoryData =
    verifyReceiptSegment(inventory, actual, anchor, stopped, root, root)

  private def verifyReceiptSegment(inventory: Image, actual: Image,
      anchor: Image, stopped: Option[MySqlBusinessWriterRow],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): ReceiptHistoryData =
    requireHistoryDataBudget(root, dataBudget)
    require(actual != null && anchor != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (actual.limitProfile eq root.profile) && (anchor.limitProfile eq root.profile) &&
      actual.allocationBudget.sharesAllocationLedger(root) && anchor.allocationBudget.sharesAllocationLedger(root),
      "complete original history images must share the open retained root")
    require(inventory != null && (inventory.limitProfile eq root.profile) &&
      inventory.allocationBudget.sharesAllocationLedger(root) && !inventory.allocationBudget.isClosed &&
      inventory.schema == actual.schema && inventory.connection.serverUuid == actual.connection.serverUuid &&
      inventory.connection.serverVersion == actual.connection.serverVersion && inventory.connection.effectiveUser == actual.connection.effectiveUser,
      "complete original inventory must remain on the same database/root")
    require(actual.schema == anchor.schema && actual.connection.serverUuid == anchor.connection.serverUuid &&
      actual.connection.serverVersion == anchor.connection.serverVersion && actual.connection.effectiveUser == anchor.connection.effectiveUser,
      "receipt history database identity differs")
    requireImageGraph(actual,root)
    requireImageGraph(anchor,root)
    if inventory ne actual then requireImageGraph(inventory,root)
    val originals = inventory.relation(OperationTable).rows
    require(originals.size.toLong <= root.profile.maxMembers(), "receipt history inventory exceeds original profile")
    dataBudget.reserveRetainedMemory(Math.addExact(4096L, Math.multiplyExact(originals.size.toLong, 1024L)),
      "receipt history original index, visited set and result containers")
    val indexed = originals.map(r => (value(r, OperationTable, "binding_uuid"), value(r, OperationTable, "operation_uuid")) -> r).toMap
    require(indexed.size == originals.size, "duplicate original receipt history key")
    val visited = scala.collection.mutable.HashSet.empty[(MySqlFormat3Cell, MySqlFormat3Cell)]
    val result = Vector.newBuilder[Either[ReceiptData, HistoricalSourceChildReserveSegmentCandidate]]
    def writer(relations: Vector[MySqlFormat3RelationSnapshot]): Vector[MySqlFormat3Cell] =
      val rows = relations.find(_.table == "fp_runtime_writer").get.rows
      require(rows.size == 1, "receipt history requires one complete W")
      rows.head
    // Renew has no new O. It may advance lease at every edge, including the final
    // anchor edge, but cannot explain a changed owner/epoch/receipt or any other row.
    var terminal = stopped.nonEmpty
    def continues(earlier: Vector[MySqlFormat3RelationSnapshot], later: Vector[MySqlFormat3RelationSnapshot]): Boolean =
      val a = writer(earlier)
      val b = writer(later)
      val shape = MySqlFormat3RelationShapes.forTable("fp_runtime_writer")
      val lease = shape.columns.indexWhere(_.name == "lease_until")
      val owner = shape.columns.indexWhere(_.name == "owner_boot_uuid")
      val nonNullOwner = value(a, "fp_runtime_writer", "owner_boot_uuid") != MySqlFormat3Cell.SqlNull
      val released = terminal && stopped.exists(_.ownerBootUuid.isEmpty)
      val originalOwner = !terminal || a(owner) == value(writer(anchor.relations), "fp_runtime_writer", "owner_boot_uuid")
      val monotonicLease = (a(lease), b(lease)) match
        case (MySqlFormat3Cell.DateTime(x), MySqlFormat3Cell.DateTime(y)) => !y.isBefore(x)
        case _ => false
      nonNullOwner && originalOwner && (monotonicLease || released) &&
        a.indices.forall(i => i == lease || (released && i == owner && b(i) == MySqlFormat3Cell.SqlNull) || a(i) == b(i)) &&
        earlier.indices.forall(i => earlier(i).table == later(i).table &&
          (earlier(i).table == "fp_runtime_writer" || earlier(i) == later(i)))
    var cursor = actual.relations
    while !continues(anchor.relations, cursor) do
      val w = writer(cursor)
      val key = (value(w, "fp_runtime_writer", "binding_uuid"), value(w, "fp_runtime_writer", "last_commit_operation_uuid"))
      require(key._2 != MySqlFormat3Cell.SqlNull && visited.add(key) && visited.size <= originals.size,
        "receipt history is missing, cyclic or cannot reach its complete anchor")
      val raw = indexed.getOrElse(key, fail("receipt history lacks its exact original operation"))
      dataBudget.reserveRetainedMemory(512L, "historical complete cursor DATA wrapper")
      val cursorSnapshot:Image=exactInputs(actual,root) match
        case None=>new MySqlFormat3FullSnapshot(actual.schema,actual.connection,actual.observationScope,actual.maxAllowedPacketBytes,root.profile,cursor,dataBudget)
        case Some(inputs)=>MySqlScriptSchemaSnapshot.decoded(actual.schema,actual.connection,actual.observationScope,actual.maxAllowedPacketBytes,inputs,cursor,dataBudget)
      val rawScope = value(raw, OperationTable, "scope_bytes") match
        case MySqlFormat3Cell.Binary(bytes) => bytes
        case _ => fail("receipt history operation has no original scope bytes")
      if MySqlNormalRecallCanonical.isHistoricalSourceChildReserveScope(rawScope) then
        val fullCursor = cursorSnapshot match
          case deployment: MySqlScriptSchemaSnapshot => deployment
          case _ => fail("historical SOURCE R0 requires a complete schema5 Full26 cursor")
        val candidate = verifyHistoricalSourceChildReserveCandidate(raw, inventory,
          fullCursor, root, dataBudget)
        val scope = candidate.data.scope.commonScope
        require(actual.schema == scope.schema && actual.connection.serverUuid == scope.server &&
          actual.connection.serverVersion == scope.stableSession.serverVersion &&
          actual.connection.effectiveUser == scope.stableSession.effectiveUser,
          "historical SOURCE R0 scope belongs to another original database identity")
        require(continues(candidate.target.relations, cursor),
          "complete receipt history target/next-before differs for historical SOURCE R0")
        result += Right(candidate)
        cursor = candidate.before.relations
      else
        if MySqlNormalRecallCanonical.isSourceHistoryDomain(rawScope) then
          throw new java.sql.SQLDataException(
            "unsupported or unknown SOURCE_* receipt domain fails closed in complete history")
        val resolved = verifyAnyReceipt(raw, inventory, cursorSnapshot, root, dataBudget)
        require(actual.schema == resolved.scope.schema && actual.connection.serverUuid == resolved.scope.server &&
          actual.connection.serverVersion == resolved.scope.stableSession.serverVersion &&
          actual.connection.effectiveUser == resolved.scope.stableSession.effectiveUser,
          "receipt history phase belongs to another database identity")
        require(continues(resolved.target, cursor), "complete receipt history target/next-before differs")
        result += Left(resolved)
        cursor = resolved.before
      terminal = false
    val candidates = result.result()
    val chronologicalEvidence = scala.collection.mutable.ArrayBuffer.empty[
      MySqlNormalRecallSourceWorkflow.SourceReserveHistoryReceiptData]
    candidates.reverseIterator.foreach {
      case Left(receipt) =>
        chronologicalEvidence += MySqlNormalRecallSourceWorkflow.sourceReserveHistoryReceiptData(
          receipt.row, receipt.context, receipt.receiptFingerprint, receipt.before, receipt.target,
          root, dataBudget)
      case Right(candidate) =>
        MySqlNormalRecallSourceWorkflow.qualifyHistoricalSourceChildReserveReleasePredecessors(
          candidate.data, inventory, chronologicalEvidence, root, dataBudget)
        chronologicalEvidence += MySqlNormalRecallSourceWorkflow.sourceReserveHistoryReceiptData(
          candidate.data.row, candidate.data.originalContext.context,
          candidate.data.fingerprint.canonical.sha256, candidate.before.relations,
          candidate.target.relations, root, dataBudget)
    }
    val completed = Vector.newBuilder[ReceiptData]
    candidates.foreach {
      case Left(receipt) => completed += receipt
      case Right(candidate) =>
        dataBudget.reserveRetainedMemory(512L, "qualified historical SOURCE R0 public receipt DATA before allocation")
        completed += new ReceiptData(candidate.data.row, candidate.data.scope.commonScope,
          candidate.data.originalContext.context, candidate.data.fingerprint.canonical.sha256,
          candidate.before.relations, candidate.target.relations, None, dataBudget)
    }
    dataBudget.reserveRetainedMemory(512L, "complete receipt history retained owner wrapper")
    new ReceiptHistoryData(inventory, actual, anchor, completed.result(), dataBudget)

  /** 必须从整个实际inventory发现；不存在caller筛选集合/空集合快捷admission。 */
  def discover(snapshot: Image, binding: UUID,
      originalRoot:Option[MySqlFormat3ReadBudget]=None): Vector[MySqlNormalRecallRecoveryInventory] =
    require(snapshot != null && !snapshot.allocationBudget.isClosed && MySqlNormalRecallRequest.nonZero(binding),
      "original complete recall discovery image required")
    val root = originalRoot.getOrElse(snapshot.allocationBudget)
    discover(snapshot, binding, root, root)

  private[mysql] def discover(snapshot: Image, binding: UUID, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Vector[MySqlNormalRecallRecoveryInventory] =
    require(snapshot != null && !snapshot.allocationBudget.isClosed && MySqlNormalRecallRequest.nonZero(binding),
      "original complete recall discovery image required")
    requireHistoryDataBudget(root, dataBudget)
    exactInputs(snapshot,root)
    val rows = snapshot.relation(OperationTable).rows
    require(rows.size.toLong <= root.profile.maxMembers(), "recall inventory exceeds original profile")
    val scratch = Math.addExact(4096L, Math.multiplyExact(rows.size.toLong, 256L))
    dataBudget.reserveTemporaryMemory(scratch, "recall full workflow discovery")
    var primary: Throwable = null
    try
      requireImageGraph(snapshot,root)
      val recall = rows.filter(row => Set("RECALL_PREPARE", "RECALL_CHECKPOINT").contains(token(row, "step")))
      recall.foreach(row => require(isUuid(row, OperationTable, "binding_uuid", binding), "foreign recall binding in full inventory"))
      def uuid(row: Vector[MySqlFormat3Cell], name: String): UUID = value(row, OperationTable, name) match
        case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
          var hi = 0L; var lo = 0L; var index = 0
          while index < 8 do
            hi = (hi << 8) | (bytes.byteAt(index).toLong & 255L)
            lo = (lo << 8) | (bytes.byteAt(index + 8).toLong & 255L)
            index += 1
          val result = new UUID(hi, lo)
          require(MySqlNormalRecallRequest.nonZero(result), "zero recall inventory identity")
          result
        case _ => fail("missing exact recall inventory UUID: " + name)
      val groups = recall.groupBy(row => uuid(row, "workflow_uuid")).toVector.sortBy(_._1.toString)
      dataBudget.reserveRetainedMemory(Math.addExact(128L, Math.multiplyExact(groups.size.toLong, 512L)), "recall retained inventory DATA")
      groups.map { (workflow, group) =>
        val originals = group.filter(row => token(row, "step") == "RECALL_PREPARE" &&
          value(row, OperationTable, "previous_step_operation_uuid") == MySqlFormat3Cell.SqlNull)
        if originals.size != 1 then fail("recall workflow requires exactly one original P")
        val p = originals.head
        val pod = uuid(p, "pod_uuid")
        val checkpoints = group.filter(row => token(row, "step") == "RECALL_CHECKPOINT")
        if checkpoints.size > 1 then fail("recall workflow has multiple original C operations")
        val original = uuid(p, "operation_uuid")
        val checkpoint = checkpoints.headOption.map(uuid(_, "operation_uuid"))
        MySqlNormalRecallRecoveryInventory(workflow, pod, original, checkpoint,
          inspect(snapshot, binding, workflow, pod, original, checkpoint, root, dataBudget))
      }
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(scratch, "recall full workflow discovery")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  /** 读取已存 P2/G 优先于分流原 C，防止每次启动再起一条恢复工作流。 */
  def inspect(snapshot: Image, binding: UUID, workflow: UUID, pod: UUID,
      originalPrepare: UUID, originalCheckpoint: Option[UUID],
      originalRoot:Option[MySqlFormat3ReadBudget]=None): MySqlNormalRecallRecoveryCursor =
    require(snapshot != null && !snapshot.allocationBudget.isClosed &&
      Vector(binding, workflow, pod, originalPrepare).forall(MySqlNormalRecallRequest.nonZero) &&
      originalCheckpoint != null && originalCheckpoint.forall(MySqlNormalRecallRequest.nonZero),
      "original full20 and recall workflow identities required")
    val root = originalRoot.getOrElse(snapshot.allocationBudget)
    inspect(snapshot, binding, workflow, pod, originalPrepare, originalCheckpoint, root, root)

  private def inspect(snapshot: Image, binding: UUID, workflow: UUID, pod: UUID,
      originalPrepare: UUID, originalCheckpoint: Option[UUID], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlNormalRecallRecoveryCursor =
    require(snapshot != null && !snapshot.allocationBudget.isClosed &&
      Vector(binding, workflow, pod, originalPrepare).forall(MySqlNormalRecallRequest.nonZero) &&
      originalCheckpoint != null && originalCheckpoint.forall(MySqlNormalRecallRequest.nonZero),
      "original full20 and recall workflow identities required")
    requireHistoryDataBudget(root, dataBudget)
    exactInputs(snapshot,root)
    val all = snapshot.relation(OperationTable).rows
    val count = all.count(row => isUuid(row, OperationTable, "binding_uuid", binding) &&
      isUuid(row, OperationTable, "workflow_uuid", workflow))
    if count == 0 || count.toLong > root.profile.maxMembers() then fail("original workflow absent or over budget")
    val scratch = Math.multiplyExact(count.toLong, 192L)
    dataBudget.reserveTemporaryMemory(scratch, "recall recovery operation index")
    var primary: Throwable = null
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      val rows = all.filter(row => isUuid(row, OperationTable, "binding_uuid", binding) &&
        isUuid(row, OperationTable, "workflow_uuid", workflow))
      rows.foreach(row => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, dataBudget, inputsForImage(snapshot, root)))
      val byId = rows.map(row => MySqlModeOperationEffectCodec.rowOperationUuid(row) -> row).toMap
      if byId.size != rows.size then fail("duplicate original operation identity")
      def operation(id: UUID): Vector[MySqlFormat3Cell] = byId.getOrElse(id, fail("workflow predecessor missing"))
      def raw(row: Vector[MySqlFormat3Cell]): MySqlRecoveryOperationRawEnvelope =
        MySqlRecoveryOperationRawEnvelopeCodec.encode(row, root.profile, inputsForImage(snapshot, root))
      def linked(row: Vector[MySqlFormat3Cell], name: String): Vector[MySqlFormat3Cell] =
        rows.find(candidate => value(candidate, OperationTable, "operation_uuid") == value(row, OperationTable, name))
          .getOrElse(fail("missing exact workflow link: " + name))
      def requirePod(row: Vector[MySqlFormat3Cell]): Unit =
        if !isUuid(row, OperationTable, "pod_uuid", pod) then fail("foreign Pod in recall workflow")
      val prepare = operation(originalPrepare)
      requirePod(prepare)
      if token(prepare, "step") != "RECALL_PREPARE" then fail("original P is not recall prepare")
      val checkpoints = rows.filter(row => token(row, "step") == "RECALL_CHECKPOINT")
      if checkpoints.size > 1 || checkpoints.headOption.map(MySqlModeOperationEffectCodec.rowOperationUuid) != originalCheckpoint then
        fail("original C does not match complete workflow inventory")
      val execute = rows.filter(row => token(row, "step") != "RESERVE")
      execute.foreach(requirePod)
      execute.foreach { row =>
        val reserve = linked(row, "reserve_operation_uuid")
        if token(reserve, "step") != "RESERVE" || token(reserve, "operation_state") != "COMMITTED" then
          fail("execute operation has no actual committed reserve")
      }
      val successors = execute.filter(row => !isUuid(row, OperationTable, "operation_uuid", originalPrepare) &&
        !originalCheckpoint.exists(isUuid(row, OperationTable, "operation_uuid", _)) && token(row, "step") != "CANCEL")
      val successorIds = successors.map(row => value(row, OperationTable, "operation_uuid")).toSet
      val referenced = successors.map(row => value(row, OperationTable, "previous_step_operation_uuid")).toSet
      val leaves = successors.filter(row => !referenced.contains(value(row, OperationTable, "operation_uuid")))
      if successors.nonEmpty && leaves.size != 1 then fail("recovery workflow is forked or cyclic")
      if successors.nonEmpty then
        // 每个 recovery 后继都须可回溯到原 P/C 或实际 CANCEL；不能认孤立 G。
        val seen = scala.collection.mutable.HashSet.empty[MySqlFormat3Cell]
        var cursor = leaves.head
        var traversed = 0
        while successorIds.contains(value(cursor, OperationTable, "operation_uuid")) do
          val id = value(cursor, OperationTable, "operation_uuid")
          if !seen.add(id) then fail("recovery predecessor cycle")
          val predecessor = linked(cursor, "previous_step_operation_uuid")
          if token(predecessor, "operation_state") != "COMMITTED" then fail("unsettled recovery predecessor")
          if token(cursor, "step") == "RESERVATION_RELEASE" && token(predecessor, "step") != "RECALL_PREPARE" then
            fail("G must follow its actual prepared P2")
          cursor = predecessor
          traversed += 1
        if traversed != successors.size then fail("disconnected recovery successor history")
        if token(cursor, "step") == "CANCEL" then
          val cancelled = linked(cursor, "previous_step_operation_uuid")
          if token(cancelled, "operation_state") != "CANCELLED" ||
            value(cancelled, OperationTable, "settled_by_operation_uuid") != value(cursor, OperationTable, "operation_uuid") ||
            !originalCheckpoint.exists(isUuid(cancelled, OperationTable, "operation_uuid", _)) then
            fail("recovery settlement does not settle original C")
        else if !isUuid(cursor, OperationTable, "operation_uuid", originalPrepare) &&
          !originalCheckpoint.exists(isUuid(cursor, OperationTable, "operation_uuid", _)) then fail("recovery has no original predecessor")
        val latest = leaves.head
        requireReservation(snapshot, binding, pod, workflow, latest)
        (token(latest, "step"), token(latest, "operation_state")) match
          case ("RECALL_PREPARE", "RESERVED") => MySqlNormalRecallRecoveryCursor.ResumeRecoveryPrepare(raw(latest))
          case ("RECALL_PREPARE", "COMMITTED") => MySqlNormalRecallRecoveryCursor.ResumeRecoveryPhysical(raw(latest))
          case ("RESERVATION_RELEASE", "RESERVED") => MySqlNormalRecallRecoveryCursor.ReconcileRecoverySettlement(raw(latest))
          case ("RESERVATION_RELEASE", "COMMITTED") => MySqlNormalRecallRecoveryCursor.FinishRecoveryMain(raw(latest))
          case _ => fail("unsupported or cancelled recovery successor needs its original settlement owner")
      else
        token(prepare, "operation_state") match
          case "RESERVED" => MySqlNormalRecallRecoveryCursor.ReconcileOriginal(raw(prepare))
          case "COMMITTED" => originalCheckpoint match
            case None =>
              requireReservation(snapshot, binding, pod, workflow, prepare)
              MySqlNormalRecallRecoveryCursor.VerifyUnsubmittedCheckpoint(raw(prepare))
            case Some(id) =>
              val checkpoint = operation(id)
              if !isUuid(checkpoint, OperationTable, "previous_step_operation_uuid", originalPrepare) then fail("C does not follow original P")
              token(checkpoint, "operation_state") match
                case "RESERVED" => MySqlNormalRecallRecoveryCursor.ReconcileOriginal(raw(checkpoint))
                case "COMMITTED" =>
                  val matching = snapshot.relation(CompletionTable).rows.filter(row =>
                    isUuid(row, CompletionTable, "binding_uuid", binding) && isUuid(row, CompletionTable, "completion_operation_uuid", id))
                  if matching.size != 1 || !isUuid(matching.head, CompletionTable, "pod_uuid", pod) then fail("committed C completion absent or foreign")
                  MySqlNormalRecallRecoveryCursor.VerifyCommittedCheckpoint(raw(checkpoint))
                case "CANCELLED" =>
                  val settlement = linked(checkpoint, "settled_by_operation_uuid")
                  if token(settlement, "step") != "CANCEL" || token(settlement, "operation_state") != "COMMITTED" ||
                    !isUuid(settlement, OperationTable, "previous_step_operation_uuid", id) then fail("C cancellation lacks exact negative settlement")
                  MySqlNormalRecallRecoveryCursor.VerifyNegativeSettlement(raw(checkpoint), raw(settlement))
                case _ => fail("unsupported original checkpoint state")
          case _ => fail("original P requires its original failure settlement")
    }
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try dataBudget.releaseTemporaryMemory(scratch, "recall recovery operation index")
      catch case cleanup: Throwable => if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def requireReservation(snapshot: Image, binding: UUID, pod: UUID,
      workflow: UUID, operation: Vector[MySqlFormat3Cell]): Unit =
    val rows = snapshot.relation(ReservationTable).rows.filter(row => isUuid(row, ReservationTable, "binding_uuid", binding) &&
      isUuid(row, ReservationTable, "resource_pod_uuid", pod))
    if rows.size != 1 || !isUuid(rows.head, ReservationTable, "workflow_uuid", workflow) ||
      value(rows.head, ReservationTable, "operation_uuid") != value(operation, OperationTable, "operation_uuid") then
      fail("original recovery reservation ownership differs")
    val expected = if token(operation, "step") == "RESERVATION_RELEASE" && token(operation, "operation_state") == "COMMITTED" then "RELEASED" else "HELD"
    if value(rows.head, ReservationTable, "reservation_state") != MySqlFormat3Cell.Text(expected) then fail("original recovery reservation state differs")
  private def fail(reason: String): Nothing = throw new SQLDataException("normal recall recovery: " + reason)
