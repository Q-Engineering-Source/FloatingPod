package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{Decoded, DeployRequest, MutationReceipt, OperationPhase, Request, RetractRequest}
import java.io.{DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.{Arrays, HashMap, HashSet, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** full26 的恢复操作历史 DATA 校验；不发行原生 producer、witness 或 Ready 能力。 */
private[mysql] object MySqlRecoveryOperationHistoryValidation:
  private val RecoveryTable = "fp_recovery_operation"
  private val BindingTable = "physical_recovery_binding"
  private val ManifestTable = "fp_schema_manifest"
  private val DeploymentTable = MySqlDeploymentReceiptSchemaDefinition.OperationTable
  private val ContextEncoding = "FP_RECOVERY_OPERATION_BE_V1"
  private val ContextDomain = "com.wi.floatingpod.mysql-operation-context/v1"
  private val Namespace = Array[Byte](70, 80, 68, 67)
  private val MinimumEffectBytes = 49L
  private val LegacyAndDeploymentSteps =
    MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.toSet
  private val DeploymentSteps = MySqlDeploymentReceiptSchemaDefinition.RecoverySteps.iterator.map(_._2).toSet

  private final class RecoveryEntry(val operationUuid: UUID, val row: Vector[MySqlFormat3Cell])
  private final case class ContextHeader(
      bootUuid: UUID,
      writerEpoch: Long,
      bindingUuid: UUID,
      operationUuid: UUID,
      effectCount: Int)

  /** 调用方已完成闭合 shape、quota 与 31 列实际 wire-row 解码。 */
  private[mysql] def validate(relations: Vector[MySqlFormat3RelationSnapshot],
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget): Unit =
    if relations == null || inputs == null || originalRoot == null || originalRoot.isClosed then
      throw new SQLDataException("full26 recovery history requires exact open DATA inputs and original allocation owner")
    if (inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27) || inputs.deploymentBudget.isEmpty then
      throw new SQLDataException("recovery operation history requires explicit Full26 or Full27 DATA")
    originalRoot.requireScriptDataInputs(inputs)
    if originalRoot.profile ne inputs.sourceLimitProfile then
      throw new SQLDataException("full26 recovery history allocation profile differs from its exact inputs")

    val recoveryRelation = relation(relations, RecoveryTable)
    val deploymentRelation = relation(relations, DeploymentTable)
    if recoveryRelation.rows.isEmpty then
      if deploymentRelation.rows.nonEmpty then invalid("deployment rows have no recovery operation history")
      return

    val recoveryShape = inputs.forTable(RecoveryTable)
    val deploymentShape = MySqlDeploymentReceiptRelationShapes.operation
    val bindingRelation = relation(relations, BindingTable)
    if bindingRelation.rows.size != 1 then invalid("recovery rows require the singleton physical binding row")
    val bindingShape = inputs.forTable(BindingTable)
    val actualBinding = requiredBinary(bindingRelation.rows.head, bindingShape, "binding_uuid")
    if isZeroUuid(actualBinding) then invalid("physical recovery binding UUID is zero")
    if deploymentRelation.rows.nonEmpty then
      val state = requiredText(bindingRelation.rows.head, bindingShape, "cutover_state")
      val format = requiredInteger(bindingRelation.rows.head, bindingShape, "format_version")
      if state != "CERTIFIED" || format != MySqlScriptSchemaProfile.PhysicalBindingFormat.toLong then
        invalid("deployment rows require their actual certified format2 database binding")

    val recoveryCount = recoveryRelation.rows.size.toLong
    val deploymentCount = deploymentRelation.rows.size.toLong
    val indexCharge = Math.addExact(4_096L,
      Math.addExact(Math.multiplyExact(recoveryCount, 256L), Math.multiplyExact(deploymentCount, 512L)))
    withTemporary(originalRoot, indexCharge, "full26 recovery operation and deployment-link indexes") {
      validateIndexed(originalRoot, recoveryRelation.rows, recoveryShape,
        deploymentRelation.rows, deploymentShape, actualBinding,
        if inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then MySqlDbFirstSchemaDefinition.DataRecoverySteps.toSet
        else LegacyAndDeploymentSteps)
    }

  private def validateIndexed(root: MySqlFormat3ReadBudget,
      recoveryRows: Vector[Vector[MySqlFormat3Cell]], recoveryShape: MySqlFormat3RelationShape,
      deploymentRows: Vector[Vector[MySqlFormat3Cell]], deploymentShape: MySqlFormat3RelationShape,
      actualBinding: MySqlFormat3Binary, allowedSteps: Set[String]): Unit =
    val byOperationUuid = new HashMap[UUID, RecoveryEntry](hashCapacity(recoveryRows.size))
    var rowIndex = 0
    while rowIndex < recoveryRows.size do
      val row = recoveryRows(rowIndex)
      val binding = requiredBinary(row, recoveryShape, "binding_uuid")
      if binding != actualBinding then invalid(s"$RecoveryTable row $rowIndex uses another database authority binding")
      val operationUuid = requiredUuid(row, recoveryShape, "operation_uuid")
      if byOperationUuid.put(operationUuid, new RecoveryEntry(operationUuid, row)) != null then
        invalid(s"$RecoveryTable has duplicate operation_uuid inside its actual authority binding")
      rowIndex += 1

    rowIndex = 0
    while rowIndex < recoveryRows.size do
      validateRecoveryRow(recoveryRows(rowIndex), rowIndex, recoveryShape, root, allowedSteps)
      rowIndex += 1

    rowIndex = 0
    while rowIndex < recoveryRows.size do
      validateSelfReference(recoveryRows(rowIndex), rowIndex, recoveryShape,
        "reserve_operation_uuid", byOperationUuid)
      validateSelfReference(recoveryRows(rowIndex), rowIndex, recoveryShape,
        "previous_step_operation_uuid", byOperationUuid)
      validateSelfReference(recoveryRows(rowIndex), rowIndex, recoveryShape,
        "settled_by_operation_uuid", byOperationUuid)
      rowIndex += 1
    validatePreviousStepAcyclic(recoveryRows, recoveryShape, byOperationUuid, root)

    val linkedDeploymentSteps = new HashSet[UUID](hashCapacity(Math.addExact(recoveryRows.size, deploymentRows.size)))
    rowIndex = 0
    while rowIndex < deploymentRows.size do
      validateDeploymentRow(deploymentRows(rowIndex), rowIndex, deploymentShape, recoveryShape,
        root, actualBinding, byOperationUuid, linkedDeploymentSteps)
      rowIndex += 1

    rowIndex = 0
    while rowIndex < recoveryRows.size do
      val row = recoveryRows(rowIndex)
      val step = requiredText(row, recoveryShape, "step")
      val state = requiredText(row, recoveryShape, "operation_state")
      if DeploymentSteps.contains(step) && state == "COMMITTED" then
        val operationUuid = requiredUuid(row, recoveryShape, "operation_uuid")
        if !linkedDeploymentSteps.contains(operationUuid) then
          invalid(s"committed $step row is not linked to a durable deployment operation history")
      rowIndex += 1
    ()

  private def validateRecoveryRow(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      shape: MySqlFormat3RelationShape,
      budget: MySqlFormat3ReadBudget, allowedSteps: Set[String]): Unit =
    withTemporary(budget, 2_048L, s"$RecoveryTable row $rowIndex fixed DATA validation") {
      val binding = requiredBinary(row, shape, "binding_uuid")
      val operation = requiredUuid(row, shape, "operation_uuid")
      requireNonzeroUuid(requiredBinary(row, shape, "workflow_uuid"), s"$RecoveryTable.workflow_uuid")
      val step = requiredText(row, shape, "step")
      if !allowedSteps.contains(step) then invalid(s"$RecoveryTable row $rowIndex has a recovery step outside its exact schema family")
      val state = requiredText(row, shape, "operation_state")
      val boot = requiredBinary(row, shape, "boot_uuid")
      requireNonzeroUuid(boot, s"$RecoveryTable.boot_uuid")
      val generation = requiredInteger(row, shape, "boot_generation")
      val connectionGeneration = requiredBinary(row, shape, "connection_generation_uuid")
      requireNonzeroUuid(connectionGeneration, s"$RecoveryTable.connection_generation_uuid")
      val connectionId = requiredInteger(row, shape, "connection_id")
      val fenceEpoch = requiredInteger(row, shape, "fence_epoch")
      if generation <= 0L || connectionId <= 0L || fenceEpoch < 0L then
        invalid(s"$RecoveryTable row $rowIndex has an invalid boot generation, connection ID or fence epoch")
      optionalUuid(row, shape, "pod_uuid")
      optionalUuid(row, shape, "attempt_uuid")
      optionalUuid(row, shape, "footprint_uuid")
      optionalUuid(row, shape, "source_unit_uuid")
      optionalUuid(row, shape, "forest_uuid")
      optionalUuid(row, shape, "reserve_operation_uuid")
      optionalUuid(row, shape, "previous_step_operation_uuid")
      optionalUuid(row, shape, "settled_by_operation_uuid")

      val expectedDomain = optionalInteger(row, shape, "expected_domain_version")
      val expectedLocator = optionalInteger(row, shape, "expected_locator_revision")
      val expectedSource = optionalInteger(row, shape, "expected_source_revision")
      if expectedDomain.exists(_ < 0L) || expectedLocator.exists(_ < 0L) || expectedSource.exists(_ < 0L) then
        invalid(s"$RecoveryTable row $rowIndex has a negative expected revision")

      if requiredText(row, shape, "context_encoding") != ContextEncoding then
        invalid(s"$RecoveryTable row $rowIndex uses an unsupported original context encoding")
      val requestIdentity = requiredNonemptyBinary(row, shape, "request_identity_bytes")
      val scope = requiredNonemptyBinary(row, shape, "scope_bytes")
      val preimage = requiredBinary(row, shape, "preimage_bytes")
      val target = requiredNonemptyBinary(row, shape, "target_bytes")
      requireBinaryLength(requiredBinary(row, shape, "operation_fingerprint"), 32,
        s"$RecoveryTable.operation_fingerprint")
      if step == "DEPLOYMENT_REGISTER" then
        if preimage.length != 0 || expectedDomain != Some(0L) || optionalUuid(row, shape, "previous_step_operation_uuid").nonEmpty then
          invalid(s"$RecoveryTable REGISTER requires empty preimage, domain version zero and no previous step")
      else if preimage.length == 0 then
        invalid(s"$RecoveryTable non-REGISTER step requires a nonempty preimage")
      if step != "DEPLOYMENT_REGISTER" && DeploymentSteps.contains(step) then
        if preimage.length != 142 || expectedDomain.isEmpty || optionalUuid(row, shape, "previous_step_operation_uuid").isEmpty then
          invalid(s"$RecoveryTable deployment UPDATE step requires a 142-byte old receipt, prior revision and previous step")
      if DeploymentSteps.contains(step) then
        validateDeploymentIdentityEncoding(requestIdentity, rowIndex, budget)
      checkDigest(row, shape, "request_identity_bytes", "request_identity_sha256", allowNull = false, allowEmpty = false)
      checkDigest(row, shape, "scope_bytes", "scope_sha256", allowNull = false, allowEmpty = false)
      checkDigest(row, shape, "preimage_bytes", "preimage_sha256", allowNull = false, allowEmpty = true)
      checkDigest(row, shape, "target_bytes", "target_sha256", allowNull = false, allowEmpty = false)
      checkDigest(row, shape, "original_diagnostic_identity", "original_diagnostic_sha256", allowNull = true, allowEmpty = true)

      val resultAt = cell(row, shape, "result_at")
      val settled = cell(row, shape, "settled_by_operation_uuid")
      val witness = cell(row, shape, "commit_witness_bytes")
      val witnessSha = cell(row, shape, "commit_witness_sha256")
      state match
        case "RESERVED" =>
          if resultAt != MySqlFormat3Cell.SqlNull || settled != MySqlFormat3Cell.SqlNull ||
            witness != MySqlFormat3Cell.SqlNull || witnessSha != MySqlFormat3Cell.SqlNull then
            invalid(s"$RecoveryTable RESERVED row has result, settlement or witness data")
        case "COMMITTED" =>
          if !resultAt.isInstanceOf[MySqlFormat3Cell.DateTime] || settled != MySqlFormat3Cell.SqlNull then
            invalid(s"$RecoveryTable COMMITTED row has invalid result/settlement topology")
          checkDigest(row, shape, "commit_witness_bytes", "commit_witness_sha256", allowNull = false, allowEmpty = false)
        case "CANCELLED" =>
          if !resultAt.isInstanceOf[MySqlFormat3Cell.DateTime] || settled == MySqlFormat3Cell.SqlNull then
            invalid(s"$RecoveryTable CANCELLED row has invalid result/settlement topology")
          checkDigest(row, shape, "commit_witness_bytes", "commit_witness_sha256", allowNull = false, allowEmpty = false)
        case _ => invalid(s"$RecoveryTable row $rowIndex has an unsupported operation state")
      if step == "RESERVE" && (state != "COMMITTED" || optionalUuid(row, shape, "reserve_operation_uuid").nonEmpty) then
        invalid("RESERVE must be committed and cannot carry a reserve parent")
      validateContext(row, rowIndex, shape, operation, binding, boot, fenceEpoch, budget)
    }

  private def validateDeploymentIdentityEncoding(identity: MySqlFormat3Binary, rowIndex: Int,
      budget: MySqlFormat3ReadBudget): Unit =
    withTemporary(budget, 1_024L, s"$RecoveryTable row $rowIndex fixed deployment identity decode") {
      if identity.length != 154 then invalid(s"$RecoveryTable deployment request identity must be exactly 154 bytes")
      var index = 0
      while index < Namespace.length do
        if identity.byteAt(index) != Namespace(index) then invalid(s"$RecoveryTable deployment identity namespace differs at byte $index")
        index += 1
      var actorNonzero = false
      index = 4
      while index < 20 do
        actorNonzero ||= identity.byteAt(index) != 0
        index += 1
      if !actorNonzero then invalid(s"$RecoveryTable deployment identity actor UUID is zero")
      val requestBytes = new Array[Byte](134)
      index = 0
      while index < requestBytes.length do
        requestBytes(index) = identity.byteAt(20 + index)
        index += 1
      val request = try
        DeploymentCommandWireCodec.decodeRequest(requestBytes) match
          case decoded: Decoded[?] => decoded.value() match
            case value: DeployRequest => value
            case value: RetractRequest => value
            case _ => invalid(s"$RecoveryTable deployment identity is not a mutation request")
          case _ => invalid(s"$RecoveryTable deployment request decoder rejected its 134-byte identity")
      catch
        case failure: SQLDataException => throw failure
        case NonFatal(cause) => throw new SQLDataException(
          s"$RecoveryTable row $rowIndex deployment request identity decode failed", cause)
      if !Arrays.equals(DeploymentCommandWireCodec.encodeRequest(request), requestBytes) then
        invalid(s"$RecoveryTable deployment request identity is not canonical")
    }

  private def validateContext(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      shape: MySqlFormat3RelationShape, operationUuid: UUID, binding: MySqlFormat3Binary,
      boot: MySqlFormat3Binary, fenceEpoch: Long, budget: MySqlFormat3ReadBudget): Unit =
    withTemporary(budget, 1_024L, s"$RecoveryTable row $rowIndex generic context frame") {
      val contextBytes = requiredNonemptyBinary(row, shape, "original_context_bytes")
      val reader = new ContextReader(contextBytes)
      val header = reader.readHeader()
      if header.bootUuid == null || header.bindingUuid == null || header.operationUuid == null || header.writerEpoch < 0L ||
        !boot.equalsUuid(header.bootUuid) || !binding.equalsUuid(header.bindingUuid) ||
        !requiredBinary(row, shape, "boot_uuid").equalsUuid(header.bootUuid) ||
        !binding.equalsUuid(header.bindingUuid) || header.operationUuid != operationUuid ||
        requiredInteger(row, shape, "fence_epoch") != header.writerEpoch then
        invalid(s"$RecoveryTable row $rowIndex generic original context identity differs from its DB row")
      val contextScratch = Math.addExact(4_096L,
        Math.addExact(Math.multiplyExact(contextBytes.length.toLong, 6L),
          Math.multiplyExact(header.effectCount.toLong, 768L)))
      withTemporary(budget, contextScratch, s"$RecoveryTable row $rowIndex canonical context/effect decode and comparison") {
        validateContextEffects(row, rowIndex, shape, contextBytes, reader, header)
      }
    }

  private def validateContextEffects(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      shape: MySqlFormat3RelationShape, contextBytes: MySqlFormat3Binary,
      reader: ContextReader, header: ContextHeader): Unit =
    val effects = ArrayBuffer.empty[MySqlExpectedEffect]
    effects.sizeHint(header.effectCount)
    var index = 0
    while index < header.effectCount do
      val targetIdentity = reader.readEffectIdentity()
      val expectedVersion = reader.readLong()
      val fingerprint = reader.readEffectFingerprint()
      try effects += MySqlExpectedEffect(targetIdentity, expectedVersion, fingerprint)
      catch
        case NonFatal(cause) => throw new SQLDataException(
          s"$RecoveryTable row $rowIndex has an invalid generic expected effect", cause)
      index += 1
    reader.requireEnd()
    val context = try MySqlOperationContext(header.bootUuid, header.writerEpoch,
      header.bindingUuid, header.operationUuid, effects)
    catch
      case NonFatal(cause) => throw new SQLDataException(
        s"$RecoveryTable row $rowIndex generic operation context is empty, duplicated or noncanonical", cause)

    val digest = MessageDigest.getInstance("SHA-256")
    val actualSha = requiredBinary(row, shape, "original_context_sha256")
    val compare = new ContextCompareOutput(contextBytes, digest)
    val output = new DataOutputStream(compare)
    MySqlModeOperationEffectCodec.writeOperationContext(output, context)
    output.flush()
    compare.requireComplete()
    val writtenSha = digest.digest()
    if !actualSha.equalsBytes(writtenSha) then invalid(s"$RecoveryTable row $rowIndex original context SHA-256 differs")

  private def validateDeploymentRow(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      deploymentShape: MySqlFormat3RelationShape, recoveryShape: MySqlFormat3RelationShape,
      budget: MySqlFormat3ReadBudget,
      actualBinding: MySqlFormat3Binary, recoveryIndex: HashMap[UUID, RecoveryEntry],
      linkedDeploymentSteps: HashSet[UUID]): Unit =
    withTemporary(budget, 2_048L, s"$DeploymentTable row $rowIndex fixed recovery-link identity") {
      validateDeploymentRowInScope(row, rowIndex, deploymentShape, recoveryShape,
        budget, actualBinding, recoveryIndex, linkedDeploymentSteps)
    }

  private def validateDeploymentRowInScope(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      deploymentShape: MySqlFormat3RelationShape, recoveryShape: MySqlFormat3RelationShape,
      budget: MySqlFormat3ReadBudget, actualBinding: MySqlFormat3Binary,
      recoveryIndex: HashMap[UUID, RecoveryEntry],
      linkedDeploymentSteps: HashSet[UUID]): Unit =
    val authorityBinding = requiredBinary(row, deploymentShape, "authority_binding_uuid")
    if authorityBinding != actualBinding then invalid(s"$DeploymentTable row $rowIndex differs from the actual DB authority binding")
    val operationUuid = requiredUuid(row, deploymentShape, "operation_uuid")
    val registerUuid = requiredUuid(row, deploymentShape, "register_step_uuid")
    val lastUuid = requiredUuid(row, deploymentShape, "last_step_uuid")
    val lastCode = requiredInteger(row, deploymentShape, "last_step_code")
    val rowRevision = requiredInteger(row, deploymentShape, "row_revision")
    if rowRevision < 0L then invalid(s"$DeploymentTable row $rowIndex has a negative row revision")
    val requestBytes = requiredBinary(row, deploymentShape, "request_bytes")
    val actor = requiredBinary(row, deploymentShape, "actor_uuid")
    val namespace = requiredBinary(row, deploymentShape, "namespace")
    val expectedIdentity = identityFor(namespace, actor, requestBytes)
    val register = recoveryIndex.get(registerUuid)
    if register == null then invalid(s"$DeploymentTable row $rowIndex register_step_uuid has no recovery operation row")
    val last = recoveryIndex.get(lastUuid)
    if last == null then invalid(s"$DeploymentTable row $rowIndex last_step_uuid has no recovery operation row")
    val registerStep = requiredText(register.row, recoveryShape, "step")
    if register.operationUuid != registerUuid || registerStep != "DEPLOYMENT_REGISTER" then
      invalid(s"$DeploymentTable row $rowIndex register_step_uuid does not identify its exact REGISTER row")
    requireCommittedLinkedStep(register.row, recoveryShape, rowIndex, "REGISTER")
    requireRequestIdentity(register.row, recoveryShape, expectedIdentity, rowIndex, "REGISTER")
    if requiredInteger(register.row, recoveryShape, "expected_domain_version") != 0L ||
      requiredBinary(register.row, recoveryShape, "preimage_bytes").length != 0 ||
      optionalUuid(register.row, recoveryShape, "previous_step_operation_uuid").nonEmpty then
      invalid(s"$DeploymentTable row $rowIndex REGISTER does not use the exact empty-preimage/domain-zero contract")
    if !requiredBinary(register.row, recoveryShape, "target_bytes").equals(requiredBinary(row, deploymentShape, "enqueue_evidence_bytes")) ||
      !requiredBinary(register.row, recoveryShape, "target_sha256").equals(requiredBinary(row, deploymentShape, "enqueue_evidence_sha256")) then
      invalid(s"$DeploymentTable row $rowIndex REGISTER target differs from original enqueue evidence")
    if !requiredBinary(register.row, recoveryShape, "boot_uuid").equals(requiredBinary(row, deploymentShape, "created_boot_uuid")) ||
      requiredInteger(register.row, recoveryShape, "boot_generation") != requiredInteger(row, deploymentShape, "created_boot_generation") ||
      requiredInteger(register.row, recoveryShape, "fence_epoch") != requiredInteger(row, deploymentShape, "created_writer_epoch") then
      invalid(s"$DeploymentTable row $rowIndex REGISTER boot/generation/epoch differs from immutable created identity")

    val expectedStep = MySqlDeploymentReceiptSchemaDefinition.RecoverySteps.find(_._1 == lastCode)
      .map(_._2).getOrElse(invalid(s"$DeploymentTable row $rowIndex last_step_code is outside the closed five-step map"))
    if last.operationUuid != lastUuid || requiredText(last.row, recoveryShape, "step") != expectedStep then
      invalid(s"$DeploymentTable row $rowIndex last step UUID/code does not map to its exact recovery row")
    requireCommittedLinkedStep(last.row, recoveryShape, rowIndex, expectedStep)
    requireRequestIdentity(last.row, recoveryShape, expectedIdentity, rowIndex, expectedStep)
    if !requiredBinary(last.row, recoveryShape, "operation_fingerprint").equals(
        requiredBinary(row, deploymentShape, "last_step_fingerprint")) then
      invalid(s"$DeploymentTable row $rowIndex last step fingerprint differs from the recovery operation")
    if !requiredBinary(last.row, recoveryShape, "target_bytes").equals(
        requiredBinary(row, deploymentShape, "step_evidence_bytes")) ||
      !requiredBinary(last.row, recoveryShape, "target_sha256").equals(
        requiredBinary(row, deploymentShape, "step_evidence_sha256")) then
      invalid(s"$DeploymentTable row $rowIndex last step target differs from full step evidence")
    if !requiredBinary(last.row, recoveryShape, "boot_uuid").equals(
        requiredBinary(row, deploymentShape, "current_writer_boot_uuid")) ||
      requiredInteger(last.row, recoveryShape, "fence_epoch") != requiredInteger(row, deploymentShape, "current_writer_epoch") then
      invalid(s"$DeploymentTable row $rowIndex last step boot/epoch differs from current writer identity")

    if rowRevision == 0L then
      if registerUuid != lastUuid || lastCode != 1L then
        invalid(s"$DeploymentTable initial row must retain its REGISTER as the last step")
    else
      if registerUuid == lastUuid || lastCode == 1L ||
        requiredInteger(last.row, recoveryShape, "expected_domain_version") != Math.subtractExact(rowRevision, 1L) ||
        requiredBinary(last.row, recoveryShape, "preimage_bytes").length != 142 then
        invalid(s"$DeploymentTable row $rowIndex UPDATE lacks its prior revision and exact old 142-byte receipt")

    withMutationRequest(requestBytes, budget, s"$DeploymentTable row $rowIndex original request") { request =>
      var postPhase = withMutationReceipt(requiredBinary(row, deploymentShape, "receipt_bytes"), budget,
        s"$DeploymentTable row $rowIndex current receipt") { receipt =>
        if receipt.operationId() != operationUuid || requestFromReceipt(receipt) != request then
          invalid(s"$DeploymentTable row $rowIndex current receipt differs from the original mutation identity")
        receipt.phase()
      }
      var cursor = last
      var chainLength = 0L
      var currentRevision = rowRevision
      var reachedRegister = false
      val requiredChainLength = Math.addExact(rowRevision, 1L)
      while !reachedRegister do
        chainLength = Math.addExact(chainLength, 1L)
        if chainLength > requiredChainLength || chainLength > recoveryIndex.size().toLong then
          invalid(s"$DeploymentTable row $rowIndex recovery step chain cycles or exceeds its revision history")
        val cursorStep = requiredText(cursor.row, recoveryShape, "step")
        requireCommittedLinkedStep(cursor.row, recoveryShape, rowIndex, cursorStep)
        requireRequestIdentity(cursor.row, recoveryShape, expectedIdentity, rowIndex, cursorStep)
        if !linkedDeploymentSteps.add(cursor.operationUuid) then
          invalid(s"$DeploymentTable row $rowIndex shares one recovery step with another deployment history")
        if cursorStep == "DEPLOYMENT_REGISTER" then
          if cursor.operationUuid != registerUuid || optionalUuid(cursor.row, recoveryShape, "previous_step_operation_uuid").nonEmpty then
            invalid(s"$DeploymentTable row $rowIndex recovery chain does not terminate at its exact REGISTER root")
          if currentRevision != 0L then invalid(s"$DeploymentTable row $rowIndex REGISTER does not terminate at revision zero")
          if postPhase != OperationPhase.QUEUED then
            invalid(s"$DeploymentTable row $rowIndex REGISTER post-state must be QUEUED")
          reachedRegister = true
        else
          if cursor.operationUuid == registerUuid then
            invalid(s"$DeploymentTable row $rowIndex recovery chain reached REGISTER through a non-REGISTER step")
          if currentRevision <= 0L ||
            requiredInteger(cursor.row, recoveryShape, "expected_domain_version") != Math.subtractExact(currentRevision, 1L) then
            invalid(s"$DeploymentTable row $rowIndex recovery chain expected revision is not consecutive")
          if cursorStep == "DEPLOYMENT_START" &&
            requiredBinary(cursor.row, recoveryShape, "boot_uuid") != requiredBinary(row, deploymentShape, "created_boot_uuid") then
            invalid(s"$DeploymentTable row $rowIndex START must remain on the original boot")
          postPhase = validatePriorReceiptTransition(requiredBinary(cursor.row, recoveryShape, "preimage_bytes"),
            postPhase, request, operationUuid, budget, rowIndex, cursorStep)
          currentRevision = Math.subtractExact(currentRevision, 1L)
          val previous = optionalUuid(cursor.row, recoveryShape, "previous_step_operation_uuid")
            .getOrElse(invalid(s"$DeploymentTable row $rowIndex recovery chain has a missing previous step"))
          val parent = recoveryIndex.get(previous)
          if parent == null then invalid(s"$DeploymentTable row $rowIndex previous step is missing from the same DB authority")
          cursor = parent
      if chainLength != requiredChainLength then
        invalid(s"$DeploymentTable row $rowIndex recovery chain length differs from row_revision")
    }
    ()

  private def requireCommittedLinkedStep(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      deploymentRow: Int, label: String): Unit =
    if requiredText(row, shape, "operation_state") != "COMMITTED" then
      invalid(s"$DeploymentTable row $deploymentRow $label history must point to a durable COMMITTED recovery step")

  private def requireRequestIdentity(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      expected: Array[Byte], deploymentRow: Int, label: String): Unit =
    val actual = requiredBinary(row, shape, "request_identity_bytes")
    if actual.length != expected.length || !actual.equalsBytes(expected) then
      invalid(s"$DeploymentTable row $deploymentRow $label recovery request identity differs from namespace/actor/wire request")

  private def identityFor(namespace: MySqlFormat3Binary, actor: MySqlFormat3Binary,
      request: MySqlFormat3Binary): Array[Byte] =
    if namespace.length != 4 || actor.length != 16 || request.length != 134 || !isNamespace(namespace) then
      invalid(s"$DeploymentTable request identity source must be exact FPDC namespace, actor UUID and 134-byte wire request")
    val result = new Array[Byte](154)
    var index = 0
    while index < namespace.length do
      result(index) = namespace.byteAt(index); index += 1
    var offset = 0
    while offset < actor.length do
      result(4 + offset) = actor.byteAt(offset); offset += 1
    offset = 0
    while offset < request.length do
      result(20 + offset) = request.byteAt(offset); offset += 1
    result

  private def validatePriorReceiptTransition(preimage: MySqlFormat3Binary, expectedPostPhase: OperationPhase,
      expectedRequest: Request, operationUuid: UUID, budget: MySqlFormat3ReadBudget,
      deploymentRow: Int, step: String): OperationPhase =
    if preimage.length != 142 then invalid(s"$DeploymentTable row $deploymentRow $step preimage is not a 142-byte receipt")
    withMutationReceipt(preimage, budget, s"$DeploymentTable row $deploymentRow $step old preimage") { receipt =>
      if receipt.operationId() != operationUuid || requestFromReceipt(receipt) != expectedRequest then
        invalid(s"$DeploymentTable row $deploymentRow $step old receipt differs from original request identity")
      val priorPhase = receipt.phase()
      if !legalPublicPhaseTransition(step, priorPhase, expectedPostPhase) then
        invalid(s"$DeploymentTable row $deploymentRow $step violates its public $priorPhase->$expectedPostPhase receipt transition")
      priorPhase
    }

  private def withMutationRequest[A](requestBytes: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget,
      context: String)(body: Request => A): A =
    withTemporaryCopy(requestBytes, budget, s"$context wire request") { requestArray =>
      val request = try
        DeploymentCommandWireCodec.decodeRequest(requestArray) match
          case decoded: Decoded[?] => decoded.value() match
            case value: DeployRequest => value
            case value: RetractRequest => value
            case _ => invalid(s"$DeploymentTable original request is not a mutation")
          case _ => invalid(s"$DeploymentTable original request decoder rejected its frame")
      catch
        case failure: SQLDataException => throw failure
        case NonFatal(cause) => throw new SQLDataException(s"$context decode failed", cause)
      if !Arrays.equals(DeploymentCommandWireCodec.encodeRequest(request), requestArray) then
        invalid(s"$DeploymentTable original request is not canonical")
      body(request)
    }

  private def withMutationReceipt[A](receiptBytes: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget,
      context: String)(body: MutationReceipt => A): A =
    withTemporaryCopy(receiptBytes, budget, s"$context wire receipt") { receiptArray =>
      val receipt = try
        DeploymentCommandWireCodec.decodeResponse(receiptArray) match
          case decoded: Decoded[?] => decoded.value() match
            case value: MutationReceipt => value
            case _ => invalid(s"$context is not a MutationReceipt")
          case _ => invalid(s"$context decoder rejected its receipt frame")
      catch
        case failure: SQLDataException => throw failure
        case NonFatal(cause) => throw new SQLDataException(s"$context decode failed", cause)
      if !Arrays.equals(DeploymentCommandWireCodec.encodeResponse(receipt), receiptArray) then
        invalid(s"$context is not canonical r03 wire data")
      body(receipt)
    }

  private def requestFromReceipt(receipt: MutationReceipt): Request =
    receipt.commandKind() match
      case DeploymentCommandWireCodec.MutationKind.DEPLOY => new DeployRequest(receipt.operationId(),
        receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())
      case DeploymentCommandWireCodec.MutationKind.RETRACT => new RetractRequest(receipt.operationId(),
        receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())

  private def legalPublicPhaseTransition(step: String, before: OperationPhase, after: OperationPhase): Boolean =
    step match
      case "DEPLOYMENT_START" => before == OperationPhase.QUEUED && after == OperationPhase.EXECUTING
      case "DEPLOYMENT_FINALIZE" =>
        (before == OperationPhase.QUEUED && Set(OperationPhase.REJECTED, OperationPhase.FAILED,
          OperationPhase.UNKNOWN).contains(after)) ||
        (before == OperationPhase.EXECUTING && Set(OperationPhase.APPLIED, OperationPhase.FAILED,
          OperationPhase.UNKNOWN).contains(after))
      case "DEPLOYMENT_RECONCILE" | "DEPLOYMENT_CLEANUP" =>
        (before == OperationPhase.APPLIED || before == OperationPhase.REJECTED) && after == before ||
        (before == OperationPhase.FAILED || before == OperationPhase.UNKNOWN) &&
          Set(OperationPhase.APPLIED, OperationPhase.FAILED, OperationPhase.UNKNOWN).contains(after)
      case _ => false

  private def withTemporaryCopy[A](value: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget,
      context: String)(body: Array[Byte] => A): A =
    val copy = value.copyBytes(budget, context)
    val retained = Math.addExact(value.length.toLong, 64L)
    var primary: Throwable = null
    try body(copy)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseRetainedMemory(retained, s"$context temporary copy release")))

  private def isNamespace(value: MySqlFormat3Binary): Boolean =
    value.length == Namespace.length && {
      var index = 0
      var equal = true
      while index < Namespace.length && equal do
        equal = value.byteAt(index) == Namespace(index)
        index += 1
      equal
    }

  private def checkDigest(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      valueColumn: String, shaColumn: String, allowNull: Boolean, allowEmpty: Boolean): Unit =
    (cell(row, shape, valueColumn), cell(row, shape, shaColumn)) match
      case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) if allowNull => ()
      case (MySqlFormat3Cell.Binary(value), MySqlFormat3Cell.Binary(expected))
          if (allowEmpty || value.length > 0) && expected.length == 32 =>
        val digest = MessageDigest.getInstance("SHA-256")
        var index = 0
        while index < value.length do
          digest.update(value.byteAt(index)); index += 1
        val actual = digest.digest()
        if !expected.equalsBytes(actual) then invalid(s"$RecoveryTable.$valueColumn SHA-256 differs")
      case _ => invalid(s"$RecoveryTable.$valueColumn/$shaColumn digest pair shape differs")

  private def validateSelfReference(row: Vector[MySqlFormat3Cell], rowIndex: Int,
      shape: MySqlFormat3RelationShape, column: String,
      index: HashMap[UUID, RecoveryEntry]): Unit =
    optionalUuid(row, shape, column).foreach { referenced =>
      val current = requiredUuid(row, shape, "operation_uuid")
      if referenced == current then invalid(s"$RecoveryTable row $rowIndex $column references itself")
      if index.get(referenced) == null then invalid(s"$RecoveryTable row $rowIndex $column has no same-binding primary-key target")
    }

  private def validatePreviousStepAcyclic(rows: Vector[Vector[MySqlFormat3Cell]],
      shape: MySqlFormat3RelationShape, index: HashMap[UUID, RecoveryEntry],
      budget: MySqlFormat3ReadBudget): Unit =
    val scratch = Math.addExact(1_024L, Math.multiplyExact(rows.size.toLong, 96L))
    withTemporary(budget, scratch, s"$RecoveryTable predecessor history cycle walk") {
      val states = new HashMap[UUID, java.lang.Byte](hashCapacity(rows.size))
      val path = ArrayBuffer.empty[UUID]
      path.sizeHint(rows.size)
      var rowIndex = 0
      while rowIndex < rows.size do
        val start = requiredUuid(rows(rowIndex), shape, "operation_uuid")
        if !states.containsKey(start) then
          path.clear()
          var cursor: UUID = start
          while cursor != null && !states.containsKey(cursor) do
            states.put(cursor, java.lang.Byte.valueOf(1.toByte))
            path += cursor
            val entry = index.get(cursor)
            if entry == null then invalid(s"$RecoveryTable predecessor walk lost a same-binding target")
            cursor = optionalUuid(entry.row, shape, "previous_step_operation_uuid").orNull
          if cursor != null && states.get(cursor).byteValue() == 1.toByte then
            invalid(s"$RecoveryTable previous_step_operation_uuid forms a recovery predecessor cycle")
          var unwind = path.size - 1
          while unwind >= 0 do
            states.put(path(unwind), java.lang.Byte.valueOf(2.toByte))
            unwind -= 1
        rowIndex += 1
    }

  private def requireBinaryLength(value: MySqlFormat3Binary, expected: Int, label: String): Unit =
    if value.length != expected then invalid(s"$label must be exactly $expected bytes")

  private def optionalUuid(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): Option[UUID] = cell(row, shape, column) match
    case MySqlFormat3Cell.SqlNull => None
    case MySqlFormat3Cell.Binary(value) =>
      requireNonzeroUuid(value, s"$RecoveryTable.$column")
      Some(uuid(value))
    case _ => invalid(s"$RecoveryTable.$column must be a nullable UUID")

  private def requiredUuid(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): UUID =
    val value = requiredBinary(row, shape, column)
    requireNonzeroUuid(value, s"$RecoveryTable.$column")
    uuid(value)

  private def uuid(value: MySqlFormat3Binary): UUID =
    if value.length != 16 then invalid("fixed UUID bytes must contain 16 bytes")
    new UUID(readLong(value, 0), readLong(value, 8))

  private def readLong(value: MySqlFormat3Binary, offset: Int): Long =
    var result = 0L
    var index = 0
    while index < 8 do
      result = (result << 8) | (value.byteAt(offset + index).toLong & 0xffL)
      index += 1
    result

  private def isZeroUuid(value: MySqlFormat3Binary): Boolean =
    if value.length != 16 then true
    else
      var index = 0
      while index < 16 do
        if value.byteAt(index) != 0 then return false
        index += 1
      true

  private def requireNonzeroUuid(value: MySqlFormat3Binary, label: String): Unit =
    if isZeroUuid(value) then invalid(s"$label must be a nonzero 16-byte UUID")

  private def optionalInteger(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): Option[Long] = cell(row, shape, column) match
    case MySqlFormat3Cell.SqlNull => None
    case MySqlFormat3Cell.SignedInteger(value) => Some(value)
    case _ => invalid(s"$RecoveryTable.$column must be a nullable integer")

  private def requiredInteger(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): Long = cell(row, shape, column) match
    case MySqlFormat3Cell.SignedInteger(value) => value
    case _ => invalid(s"${shape.table}.$column must be an integer")

  private def requiredText(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): String = cell(row, shape, column) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => invalid(s"${shape.table}.$column must be text")

  private def requiredBinary(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): MySqlFormat3Binary = cell(row, shape, column) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => invalid(s"${shape.table}.$column must be binary")

  private def requiredNonemptyBinary(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): MySqlFormat3Binary =
    val value = requiredBinary(row, shape, column)
    if value.length == 0 then invalid(s"${shape.table}.$column must be nonempty")
    value

  private def cell(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String): MySqlFormat3Cell =
    val index = shape.columns.indexWhere(_.name == column)
    if index < 0 || index >= row.size then invalid(s"${shape.table}.$column is outside its closed row shape")
    row(index)

  private def relation(relations: Vector[MySqlFormat3RelationSnapshot], table: String): MySqlFormat3RelationSnapshot =
    relations.find(_.table == table).getOrElse(invalid(s"complete full26 DATA is missing $table"))

  private def hashCapacity(count: Int): Int =
    val requested = Math.addExact(Math.multiplyExact(count.toLong, 4L) / 3L, 1L)
    if requested > (1L << 30) then invalid("full26 operation history index exceeds supported in-memory key count")
    Math.max(1L, requested).toInt

  private def withTemporary[A](budget: MySqlFormat3ReadBudget, amount: Long,
      context: String)(body: => A): A =
    budget.reserveTemporaryMemory(amount, context)
    var primary: Throwable = null
    try body
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => budget.releaseTemporaryMemory(amount, context + " release")))

  private def invalid(message: String): Nothing = throw new SQLDataException(s"full26 recovery history $message")

  private final class ContextReader(private val value: MySqlFormat3Binary):
    private var offset = 0
    private def remaining: Int = value.length - offset
    private def need(count: Long): Unit =
      if count < 0L || count > remaining.toLong then invalid("original context is truncated or has an invalid frame length")
    private def octet(): Int =
      need(1L)
      val result = value.byteAt(offset) & 0xff
      offset += 1
      result
    private def int32(): Int =
      need(4L)
      (octet() << 24) | (octet() << 16) | (octet() << 8) | octet()
    def readLong(): Long =
      need(8L)
      var result = 0L
      var index = 0
      while index < 8 do
        result = (result << 8) | octet().toLong
        index += 1
      result
    private def framedLength(label: String): Int =
      val length = int32()
      if length < 0 then invalid(s"original context $label has a negative length")
      need(length.toLong)
      length
    private def readUuid(): UUID = new UUID(readLong(), readLong())
    private def requireDomain(): Unit =
      val length = framedLength("domain")
      if length != ContextDomain.length then invalid("original context domain differs from the generic operation context domain")
      var index = 0
      while index < length do
        if octet() != ContextDomain.charAt(index).toInt then invalid("original context domain differs from the generic operation context domain")
        index += 1
    def readHeader(): ContextHeader =
      requireDomain()
      val boot = readUuid()
      val writerEpoch = readLong()
      val binding = readUuid()
      val operation = readUuid()
      val count = int32()
      if count <= 0 || count.toLong > remaining.toLong / MinimumEffectBytes then
        invalid("original context expected-effect count is empty or exceeds its complete framing")
      ContextHeader(boot, writerEpoch, binding, operation, count)
    def readEffectIdentity(): String =
      val length = framedLength("effect identity")
      if length == 0 then invalid("original context effect identity is empty")
      val characters = new Array[Char](length)
      var index = 0
      while index < length do
        val value = octet()
        if value < 0x21 || value > 0x7e then invalid("original context effect identity is not canonical printable ASCII")
        characters(index) = value.toChar
        index += 1
      new String(characters)
    def readEffectFingerprint(): Vector[Byte] =
      val length = framedLength("effect fingerprint")
      if length != MySqlExpectedEffect.FingerprintLength then
        invalid("original context effect fingerprint is not exactly 32 bytes")
      val bytes = new Array[Byte](length)
      var index = 0
      while index < length do
        bytes(index) = octet().toByte
        index += 1
      bytes.toVector
    def requireEnd(): Unit =
      if offset != value.length then invalid("original context has trailing bytes")

  private final class ContextCompareOutput(input: MySqlFormat3Binary, digest: MessageDigest) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= input.length || input.byteAt(offset) != value.toByte then
        invalid("original context bytes are not the canonical generic operation context encoding")
      digest.update(value.toByte)
      offset += 1
    override def write(values: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, values.length)
      var index = 0
      while index < length do
        write(values(start + index).toInt)
        index += 1
    def requireComplete(): Unit =
      if offset != input.length then invalid("original context canonical re-encoding length differs")
