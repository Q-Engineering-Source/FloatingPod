package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** Current O/R/W 的固定 tuple producer；seed 仅物化中的 cycle scaffold，不可作为完成图逃逸。 */
private[mysql] object MySqlInactiveDomainOperationRows:
  import MySqlControlledDebugRows.*
  def operation(request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,at: LocalDateTime,locatorRevision: Option[Long],
      reserve: Boolean,scope: MySqlRecoveryCanonicalBytes,seed: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    if locatorRevision == null || locatorRevision.exists(_ < 0L) then fail("exact optional original locator revision required")
    root.reserveRetainedMemory(32768L,"inactive current operation map/cells")
    val op=if reserve then plan.reserveOperationUuid else plan.executeOperationUuid
    val parent=if reserve then Null else uuid(plan.reserveOperationUuid)
    val fields=Map(
      "binding_uuid"->uuid(request.expectedBinding.bindingUuid),"operation_uuid"->uuid(op),
      "workflow_uuid"->uuid(plan.workflowUuid),"step"->text(if reserve then "RESERVE" else "DOMAIN_MUTATION"),
      "operation_state"->text(if reserve then "COMMITTED" else "RESERVED"),
      "pod_uuid"->uuid(request.podUuid),"attempt_uuid"->Null,"footprint_uuid"->Null,
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
    ) ++ Map("expected_domain_version"->signed(request.expectedVersion),
      "expected_locator_revision"->locatorRevision.map(signed).getOrElse(Null),"expected_source_revision"->Null)
    row(Operation,fields,root)

  def phase(rowValue: Vector[MySqlFormat3Cell],standard: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    root.reserveRetainedMemory(8192L,"inactive exact phase field map")
    update(rowValue,Operation,Map("original_context_bytes"->standard.binaryCell,"original_context_sha256"->standard.sha256Cell,
      "preimage_bytes"->before.binaryCell,"preimage_sha256"->before.sha256Cell,
      "target_bytes"->target.binaryCell,"target_sha256"->target.sha256Cell,
      "operation_fingerprint"->fingerprint.sha256Cell),root)

  def commit(rowValue: Vector[MySqlFormat3Cell],witness: MySqlRecoveryCanonicalBytes,at: LocalDateTime,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    root.reserveRetainedMemory(4096L,"inactive exact terminal field map")
    update(rowValue,Operation,Map("operation_state"->text("COMMITTED"),"commit_witness_bytes"->witness.binaryCell,
      "commit_witness_sha256"->witness.sha256Cell,"result_at"->MySqlFormat3Cell.DateTime(at)),root)

  def preimage(rowValue: Vector[MySqlFormat3Cell],before: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    root.reserveRetainedMemory(2048L,"inactive independent before-image field map")
    update(rowValue,Operation,Map("preimage_bytes"->before.binaryCell,"preimage_sha256"->before.sha256Cell),root)

  def reservation(before: MySqlFormat3FullSnapshot,request: MySqlInactiveDomainUpdateRequest,
      plan: MySqlOwnedRepositoryOperationPlan,key: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget): (Vector[MySqlFormat3Cell],Boolean) =
    requireRoot(root)
    root.reserveRetainedMemory(16384L,"inactive fixed POD reservation map/cells")
    var existing: Vector[MySqlFormat3Cell]=null
    val rows=before.relation(Reservation).rows
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
      MySqlInactiveDomainResourceKey.requireRow(existing,request,key,root)
      if value(existing,Reservation,"reservation_state")!=text("RELEASED") then fail("prior held reservation retains responsibility")
      val revision=value(existing,Reservation,"reservation_revision") match
        case MySqlFormat3Cell.SignedInteger(n) if n>=0 => Math.addExact(n,1L)
        case _ => fail("exact nonnegative original reservation revision")
      (update(existing,Reservation,Map("reservation_revision"->signed(revision),"workflow_uuid"->uuid(plan.workflowUuid),
        "operation_uuid"->uuid(plan.executeOperationUuid),"reservation_state"->text("HELD"),
        "last_mutation_operation_uuid"->uuid(plan.reserveOperationUuid)),root),true)
    else
      val canonical=MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1",root.profile)(key.writeCanonicalTo)
      if canonical.sha256!=key.keyHashSha256 then fail("fixed full resource key digest differs")
      (row(Reservation,Map("binding_uuid"->uuid(request.expectedBinding.bindingUuid),"resource_kind"->text("POD"),
        "resource_key_hash"->canonical.sha256Cell,"resource_key"->canonical.binaryCell,"reservation_revision"->signed(0),
        "workflow_uuid"->uuid(plan.workflowUuid),"operation_uuid"->uuid(plan.executeOperationUuid),"reservation_state"->text("HELD"),
        "last_mutation_operation_uuid"->uuid(plan.reserveOperationUuid),"resource_pod_uuid"->uuid(request.podUuid),"resource_unit_uuid"->Null),root),false)

  def release(held: Vector[MySqlFormat3Cell],execute: UUID,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireRoot(root)
    root.reserveRetainedMemory(4096L,"inactive terminal reservation map")
    val revision=value(held,Reservation,"reservation_revision") match
      case MySqlFormat3Cell.SignedInteger(n) if n>=0 => Math.addExact(n,1L)
      case _ => fail("exact held reservation revision required")
    update(held,Reservation,Map("reservation_revision"->signed(revision),"reservation_state"->text("RELEASED"),
      "last_mutation_operation_uuid"->uuid(execute)),root)

  def writer(before: MySqlFormat3FullSnapshot,operation: UUID,fingerprint: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    requireRoot(root)
    root.reserveRetainedMemory(4096L,"inactive writer field map")
    val rows=before.relation("fp_runtime_writer").rows
    if rows.size!=1 then fail("one actual writer receipt row required")
    val changed=update(rows.head,"fp_runtime_writer",Map("last_commit_operation_uuid"->uuid(operation),
      "last_commit_fingerprint"->fingerprint.sha256Cell),root)
    MySqlControlledDebugImages.put(before,"fp_runtime_writer",changed,root,requireExisting=true)

  private def requireRoot(root: MySqlFormat3ReadBudget): Unit =
    if root==null || root.isClosed || root.isScopedReadBudget then fail("open original materialization root required")
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive current O/R tuple: "+reason)
