package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** Current O/R/W 的固定 tuple producer；seed 仅物化中的 cycle scaffold，不可作为完成图逃逸。 */
private[mysql] object MySqlControlledDebugOperationRows:
  import MySqlControlledDebugRows.*
  def operation(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,at: LocalDateTime,activation: Boolean,
      reserve: Boolean,scope: MySqlRecoveryCanonicalBytes,seed: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    operation(request,plan,context,at,activation,reserve,scope,seed,root,root)

  private[mysql] def operation(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,at: LocalDateTime,activation: Boolean,
      reserve: Boolean,scope: MySqlRecoveryCanonicalBytes,seed: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(32768L,"controlled current operation map/cells")
    val op=if reserve then plan.reserveOperationUuid else plan.executeOperationUuid
    val parent=if reserve then Null else uuid(plan.reserveOperationUuid)
    val fields=Map(
      "binding_uuid"->uuid(request.expectedBinding.bindingUuid),"operation_uuid"->uuid(op),
      "workflow_uuid"->uuid(plan.workflowUuid),"step"->text(if reserve then "RESERVE" else if activation then "ACTIVATE" else "REARM"),
      "operation_state"->text(if reserve then "COMMITTED" else "RESERVED"),
      "pod_uuid"->uuid(request.podUuid),"attempt_uuid"->uuid(request.attemptUuid),"footprint_uuid"->uuid(request.footprintUuid),
      "source_unit_uuid"->Null,"forest_uuid"->Null,"boot_uuid"->uuid(context.tag.bootId),
      "boot_generation"->signed(context.tag.bootGeneration),"connection_generation_uuid"->uuid(context.barrierGenerationUuid),
      "connection_id"->signed(context.actualConnectionId),"fence_epoch"->signed(context.writerEpoch),
      "request_identity_bytes"->plan.requestIdentity.canonical.binaryCell,"request_identity_sha256"->plan.requestIdentity.canonical.sha256Cell,
      "context_encoding"->text("FP_RECOVERY_OPERATION_BE_V1"),
      "original_context_bytes"->seed.binaryCell,"original_context_sha256"->seed.sha256Cell,
      "scope_bytes"->scope.binaryCell,"scope_sha256"->scope.sha256Cell,
      "preimage_bytes"->seed.binaryCell,"preimage_sha256"->seed.sha256Cell,
      "target_bytes"->seed.binaryCell,"target_sha256"->seed.sha256Cell,
      "reserve_operation_uuid"->parent,"previous_step_operation_uuid"->parent,"settled_by_operation_uuid"->Null,
      "commit_witness_bytes"->(if reserve then seed.binaryCell else Null),
      "commit_witness_sha256"->(if reserve then seed.sha256Cell else Null),
      "original_diagnostic_identity"->Null,"original_diagnostic_sha256"->Null,
      "planned_at"->MySqlFormat3Cell.DateTime(at),"fixed_lease_until"->MySqlFormat3Cell.DateTime(context.fixedLeaseUntil),
      "result_at"->(if reserve then MySqlFormat3Cell.DateTime(at) else Null),"operation_fingerprint"->seed.sha256Cell
    ) ++ expectedOperationColumns(activation,allocation)
    row(Operation,fields,allocation)

  def phase(rowValue: Vector[MySqlFormat3Cell],standard: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    phase(rowValue,standard,before,target,fingerprint,root,root)

  private[mysql] def phase(rowValue: Vector[MySqlFormat3Cell],standard: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes,root: MySqlFormat3ReadBudget,
      allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(8192L,"controlled exact phase field map")
    update(rowValue,Operation,Map("original_context_bytes"->standard.binaryCell,"original_context_sha256"->standard.sha256Cell,
      "preimage_bytes"->before.binaryCell,"preimage_sha256"->before.sha256Cell,
      "target_bytes"->target.binaryCell,"target_sha256"->target.sha256Cell,
      "operation_fingerprint"->fingerprint.sha256Cell),allocation)

  def commit(rowValue: Vector[MySqlFormat3Cell],witness: MySqlRecoveryCanonicalBytes,at: LocalDateTime,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    commit(rowValue,witness,at,root,root)

  private[mysql] def commit(rowValue: Vector[MySqlFormat3Cell],witness: MySqlRecoveryCanonicalBytes,at: LocalDateTime,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(4096L,"controlled exact terminal field map")
    update(rowValue,Operation,Map("operation_state"->text("COMMITTED"),"commit_witness_bytes"->witness.binaryCell,
      "commit_witness_sha256"->witness.sha256Cell,"result_at"->MySqlFormat3Cell.DateTime(at)),allocation)

  def preimage(rowValue: Vector[MySqlFormat3Cell],before: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    preimage(rowValue,before,root,root)

  private[mysql] def preimage(rowValue: Vector[MySqlFormat3Cell],before: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(2048L,"controlled independent before-image field map")
    update(rowValue,Operation,Map("preimage_bytes"->before.binaryCell,"preimage_sha256"->before.sha256Cell),allocation)

  def reservation(before: MySqlFormat3FullSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,key: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell],Boolean) =
    reservationRows(before.relation(Reservation).rows,request,plan,key,root,root)

  def reservation(before: MySqlScriptSchemaSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,key: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell],Boolean) =
    reservation(before,request,plan,key,root,root)

  private[mysql] def reservation(before: MySqlScriptSchemaSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,key: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell],Boolean) =
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    reservationRows(before.relation(Reservation).rows,request,plan,key,root,allocation)

  private def reservationRows(rows: Vector[Vector[MySqlFormat3Cell]],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,key: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell],Boolean) =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(16384L,"controlled fixed POD reservation map/cells")
    var existing: Vector[MySqlFormat3Cell]=null
    var index=0
    while index<rows.size do
      val candidate=rows(index)
      if value(candidate,Reservation,"resource_kind")==text("POD") &&
        MySqlModeOperationEffectCodec.uuidEquals(value(candidate,Reservation,"binding_uuid"),request.expectedBinding.bindingUuid) &&
        MySqlModeOperationEffectCodec.uuidEquals(value(candidate,Reservation,"resource_pod_uuid"),request.podUuid) then
        if existing!=null then fail("more than one actual POD reservation")
        existing=candidate
      index+=1
    if existing!=null then
      MySqlControlledDebugResourceKey.requireRow(existing,request,key,root)
      if value(existing,Reservation,"reservation_state")!=text("RELEASED") then fail("prior held reservation retains responsibility")
      val revision=value(existing,Reservation,"reservation_revision") match
        case MySqlFormat3Cell.SignedInteger(n) if n>=0 => Math.addExact(n,1L)
        case _ => fail("exact nonnegative original reservation revision")
      (update(existing,Reservation,Map("reservation_revision"->signed(revision),"workflow_uuid"->uuid(plan.workflowUuid),
        "operation_uuid"->uuid(plan.executeOperationUuid),"reservation_state"->text("HELD"),
        "last_mutation_operation_uuid"->uuid(plan.reserveOperationUuid)),allocation),true)
    else
      val canonical=MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
        MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1",root.profile)(key.writeCanonicalTo)
      }
      if canonical.sha256!=key.keyHashSha256 then fail("fixed full resource key digest differs")
      (row(Reservation,Map("binding_uuid"->uuid(request.expectedBinding.bindingUuid),"resource_kind"->text("POD"),
        "resource_key_hash"->canonical.sha256Cell,"resource_key"->canonical.binaryCell,"reservation_revision"->signed(0),
        "workflow_uuid"->uuid(plan.workflowUuid),"operation_uuid"->uuid(plan.executeOperationUuid),"reservation_state"->text("HELD"),
        "last_mutation_operation_uuid"->uuid(plan.reserveOperationUuid),"resource_pod_uuid"->uuid(request.podUuid),"resource_unit_uuid"->Null),allocation),false)

  def release(held: Vector[MySqlFormat3Cell],execute: UUID,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    release(held,execute,root,root)

  private[mysql] def release(held: Vector[MySqlFormat3Cell],execute: UUID,root: MySqlFormat3ReadBudget,
      allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    allocation.reserveRetainedMemory(4096L,"controlled terminal reservation map")
    val revision=value(held,Reservation,"reservation_revision") match
      case MySqlFormat3Cell.SignedInteger(n) if n>=0 => Math.addExact(n,1L)
      case _ => fail("exact held reservation revision required")
    update(held,Reservation,Map("reservation_revision"->signed(revision),"reservation_state"->text("RELEASED"),
      "last_mutation_operation_uuid"->uuid(execute)),allocation)

  def writer(before: MySqlFormat3FullSnapshot,operation: UUID,receiptFingerprint: Vector[Byte],
      root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    val changed = writerRow(before.relation("fp_runtime_writer").rows,operation,receiptFingerprint,root,root)
    MySqlControlledDebugImages.put(before,"fp_runtime_writer",changed,root,requireExisting=true)

  def writer(before: MySqlScriptSchemaSnapshot,operation: UUID,receiptFingerprint: Vector[Byte],
      root: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    val changed = writerRow(before.relation("fp_runtime_writer").rows,operation,receiptFingerprint,root,root)
    MySqlControlledDebugImages.put(before,"fp_runtime_writer",changed,root,requireExisting=true)

  private[mysql] def writer(before: MySqlScriptSchemaSnapshot,operation: UUID,receiptFingerprint: Vector[Byte],
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    MySqlFormat3QueryProjection.requireDeploymentSource(before,before.inputs,root)
    val changed = writerRow(before.relation("fp_runtime_writer").rows,operation,receiptFingerprint,root,allocation)
    MySqlControlledDebugImages.put(before,"fp_runtime_writer",changed,root,allocation,requireExisting=true)

  private def writerRow(rows: Vector[Vector[MySqlFormat3Cell]],operation: UUID,receiptFingerprint: Vector[Byte],
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    requirePlanDataAllocation(root,allocation)
    if receiptFingerprint == null || receiptFingerprint.size != 32 then
      throw new SQLDataException("controlled writer requires the complete receipt context digest")
    allocation.reserveRetainedMemory(4096L,"controlled writer field map")
    if rows.size!=1 then fail("one actual writer receipt row required")
    update(rows.head,"fp_runtime_writer",Map("last_commit_operation_uuid"->uuid(operation),
      "last_commit_fingerprint"->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(receiptFingerprint.toArray))),allocation)

  private def requirePlanDataAllocation(root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Unit =
    if allocation == null || allocation.isClosed || (allocation.profile ne root.profile) ||
        !allocation.sharesAllocationLedger(root) ||
        ((allocation ne root) && !allocation.isCanonicalChildOf(root)) then
      throw new SQLDataException("controlled Full26 plan DATA requires its original same-ledger allocation child")

  private def requireRoot(root: MySqlFormat3ReadBudget): Unit =
    if root==null || root.isClosed || root.isScopedReadBudget then fail("open original materialization root required")
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled current O/R tuple: "+reason)
