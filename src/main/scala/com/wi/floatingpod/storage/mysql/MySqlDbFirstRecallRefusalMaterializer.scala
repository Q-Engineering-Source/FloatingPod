package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.UUID

/** caller先保管此槽，再构造DATA；失败保留原输入、partial canonical与cause，不重试或退款。 */
private[mysql] final class MySqlDbFirstRecallRefusalDraft private[mysql] (
    val request:MySqlDbFirstRecallRefusalRequest,val facts:MySqlDbFirstRecallRefusalFacts,
    val before:MySqlScriptSchemaSnapshot,val root:MySqlFormat3ReadBudget):
  private[mysql] var attempted=false
  private[mysql] var failure:Throwable=null
  private[mysql] var scope:MySqlRecoveryCanonicalBytes=null
  private[mysql] var preimage:MySqlRecoveryCanonicalBytes=null
  private[mysql] var projectedTarget:MySqlRecoveryCanonicalBytes=null
  private[mysql] var fingerprint:MySqlRecoveryCanonicalBytes=null
  private[mysql] var context:MySqlRecoveryCanonicalBytes=null
  private[mysql] var witness:MySqlRecoveryCanonicalBytes=null
  private[mysql] var construction:Vector[MySqlFormat3RelationSnapshot]=null
  private[mysql] var reservedOperation:Vector[MySqlFormat3Cell]=null
  private[mysql] var target:MySqlScriptSchemaSnapshot=null
  private[mysql] var result:MySqlDbFirstRecallRefusalMaterialization=null

/** 完整O/W-only DATA；没有SQL执行、ACK或freeze能力，也不表示当前已安装SQL允许新step。 */
private[mysql] final class MySqlDbFirstRecallRefusalMaterialization private[mysql] (
    private val draft:MySqlDbFirstRecallRefusalDraft):
  def request:MySqlDbFirstRecallRefusalRequest=draft.request
  def before:MySqlScriptSchemaSnapshot=draft.before
  def target:MySqlScriptSchemaSnapshot=draft.target
  def reservedOperation:Vector[MySqlFormat3Cell]=draft.reservedOperation
  def verify():Unit=verifyTarget(target)
  def verifyTarget(candidate:MySqlScriptSchemaSnapshot):Unit =
    val root=draft.root
    require(root!=null && !root.isClosed && !root.isScopedReadBudget && draft.failure==null &&
      (draft.result eq this) && candidate!=null && (candidate.inputs eq before.inputs) &&
      (candidate.allocationBudget eq root) && !before.allocationBudget.isClosed,
      "actual live completed refusal DATA and original allocation family required")
    if candidate.schema!=target.schema || candidate.connection!=target.connection ||
      candidate.observationScope!=target.observationScope || candidate.maxAllowedPacketBytes!=target.maxAllowedPacketBytes then
      throw new SQLDataException("refusal target observation identity differs")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      MySqlDbFirstRecallRefusalMaterializer.preflight(request,draft.facts,before,root)
      val c=MySqlDbFirstRecallRefusalCanonical
      val scope=c.scope(request,draft.facts,before,root)
      val preimage=c.projection(request,before,before.relations,false,root)
      val projected=c.projection(request,before,candidate.relations,true,root)
      val fingerprint=c.fingerprint(request,scope,preimage,projected,root)
      val context=c.context(request,draft.facts,fingerprint,root)
      val witness=c.witness(request,scope,preimage,projected,fingerprint,context,root)
      if !draft.scope.exactlyEquals(scope) || !draft.preimage.exactlyEquals(preimage) ||
        !draft.projectedTarget.exactlyEquals(projected) || !draft.fingerprint.exactlyEquals(fingerprint) ||
        !draft.context.exactlyEquals(context) || !draft.witness.exactlyEquals(witness) then
        throw new SQLDataException("refusal canonical recomputation differs")
      val generic=c.operationContext(request,draft.facts,fingerprint,root).canonicalFingerprintSha256
      val operation=MySqlDbFirstRecallRefusalMaterializer.operation(request,draft.facts,before,scope,
        Some((context,preimage,projected,witness,generic)),active)
      val expected=MySqlDbFirstRecallRefusalMaterializer.transition(before,request,operation,
        MySqlDbFirstRecallRefusalMaterializer.binary(generic,active),active)
      if expected!=candidate.relations ||
        MySqlDbFirstRecallRefusalMaterializer.reserved(operation,before.inputs,active)!=reservedOperation then
        throw new SQLDataException("refusal independent complete O/W transition differs")
    }

/** 原C未应用结果的完整O/W-only DATA；运行时未应用证明与受限writer资格另行核验。 */
private[mysql] object MySqlDbFirstRecallRefusalMaterializer:
  private val O="fp_recovery_operation"
  private val W="fp_runtime_writer"
  private val Null=MySqlFormat3Cell.SqlNull
  private def fail(message:String):Nothing=throw new SQLDataException("recall refusal: "+message)
  private def value(row:Vector[MySqlFormat3Cell],table:String,name:String,inputs:MySqlScriptSchemaInputs)=
    MySqlDbFirstRecallMaterializer.value(row,table,name,inputs)
  private def equalsUuid(cell:MySqlFormat3Cell,id:UUID)=MySqlDbFirstRecallMaterializer.uuidEquals(cell,id)
  private def one(before:MySqlScriptSchemaSnapshot,table:String,select:Vector[MySqlFormat3Cell]=>Boolean):Vector[MySqlFormat3Cell] =
    var found:Vector[MySqlFormat3Cell]=null
    before.relation(table).rows.foreach { row=>
      if select(row) then
        if found!=null then fail("duplicate original row "+table)
        found=row
    }
    if found==null then fail("missing original row "+table)
    found

  def allocate(request:MySqlDbFirstRecallRefusalRequest,facts:MySqlDbFirstRecallRefusalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalDraft =
    require(request!=null && facts!=null && before!=null && root!=null && !root.isClosed &&
      !root.isScopedReadBudget && (before.allocationBudget eq root),
      "refusal original owned Full27 DATA root required")
    root.requireScriptDataInputs(before.inputs)
    root.reserveRetainedMemory(1024L,"refusal partial draft BEFORE allocation")
    new MySqlDbFirstRecallRefusalDraft(request,facts,before,root)

  def materialize(draft:MySqlDbFirstRecallRefusalDraft):MySqlDbFirstRecallRefusalMaterialization =
    require(draft!=null && !draft.attempted && draft.result==null && draft.failure==null,
      "original unattempted refusal draft required; no materialization retry")
    draft.attempted=true
    val root=draft.root
    try
      preflight(draft.request,draft.facts,draft.before,root)
      root.reserveRetainedMemory(65536L,"refusal fixed construction maps/metadata BEFORE allocation")
      val c=MySqlDbFirstRecallRefusalCanonical
      val request=draft.request;val before=draft.before;val facts=draft.facts
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        draft.scope=c.scope(request,facts,before,root)
        val unfinished=operation(request,facts,before,draft.scope,None,root)
        draft.construction=transition(before,request,unfinished,Null,root)
        draft.preimage=c.projection(request,before,before.relations,false,root)
        draft.projectedTarget=c.projection(request,before,draft.construction,true,root)
        draft.fingerprint=c.fingerprint(request,draft.scope,draft.preimage,draft.projectedTarget,root)
        draft.context=c.context(request,facts,draft.fingerprint,root)
        draft.witness=c.witness(request,draft.scope,draft.preimage,draft.projectedTarget,draft.fingerprint,draft.context,root)
        val generic=c.operationContext(request,facts,draft.fingerprint,root).canonicalFingerprintSha256
        val completed=operation(request,facts,before,draft.scope,
          Some((draft.context,draft.preimage,draft.projectedTarget,draft.witness,generic)),root)
        draft.reservedOperation=reserved(completed,before.inputs,root)
        val rows=transition(before,request,completed,binary(generic,root),root)
        draft.target=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
          before.maxAllowedPacketBytes,before.inputs,rows,root)
        draft.result=new MySqlDbFirstRecallRefusalMaterialization(draft)
      }
      draft.result.verify()
      draft.result
    catch case cause:Throwable=>
      draft.failure=cause
      throw cause

  /** 仅当前完整DATA的局部约束；真实历史审计、认证与held scope仍须由执行owner核定。 */
  private[mysql] def preflight(request:MySqlDbFirstRecallRefusalRequest,facts:MySqlDbFirstRecallRefusalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):Unit =
    require(request!=null && facts!=null && before!=null && root!=null && !root.isClosed &&
      !root.isScopedReadBudget && (before.allocationBudget eq root) &&
      before.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,"exact original complete refusal before required")
    requireBeforeRows(request,facts,before,root)

  /** 冷读仅在原comparison child重算DATA；不扩大实际draft的原root要求。 */
  private[mysql] def requireLocalTransition(before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot,
      request:MySqlDbFirstRecallRefusalRequest,facts:MySqlDbFirstRecallRefusalFacts,
      root:MySqlFormat3ReadBudget):Unit =
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(child.isCanonicalComparisonChildOf(root) && !child.isClosed &&
      (before.allocationBudget eq child) && (target.allocationBudget eq child),
      "exact original cold refusal comparison DATA required")
    child.reserveRetainedMemory(65536L,"cold refusal semantic reconstruction BEFORE allocation")
    MySqlControlledDebugRelations.requireValidDbFirstComparisonGraph(before,before.inputs,root)
    requireBeforeRows(request,facts,before,root)
    val c=MySqlDbFirstRecallRefusalCanonical
    val scope=c.scope(request,facts,before,root)
    val preimage=c.projection(request,before,before.relations,false,root)
    val projected=c.projection(request,before,target.relations,true,root)
    val fingerprint=c.fingerprint(request,scope,preimage,projected,root)
    val context=c.context(request,facts,fingerprint,root)
    val witness=c.witness(request,scope,preimage,projected,fingerprint,context,root)
    val generic=c.operationContext(request,facts,fingerprint,root).canonicalFingerprintSha256
    val own=operation(request,facts,before,scope,Some((context,preimage,projected,witness,generic)),child)
    val expected=transition(before,request,own,binary(generic,child),child)
    if expected!=target.relations then fail("persisted independent complete O/W transition differs")

  /** 完整DATA前提；不证明旧executor已退出、实际C未应用或当前SQL写入资格。 */
  private def requireBeforeRows(request:MySqlDbFirstRecallRefusalRequest,facts:MySqlDbFirstRecallRefusalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):Unit =
    require(root!=null && !root.isClosed && !root.isScopedReadBudget && before!=null &&
      !before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(root) &&
      ((before.allocationBudget eq root) || before.allocationBudget.isCanonicalComparisonChildOf(root)),
      "original live or cold refusal DATA allocation family required")
    root.requireScriptDataInputs(before.inputs)
    def v(row:Vector[MySqlFormat3Cell],table:String,name:String)=value(row,table,name,before.inputs)
    val manifest=one(before,"fp_schema_manifest",_=>true)
    val binding=one(before,"physical_recovery_binding",_=>true)
    val writer=one(before,W,_=>true)
    if !equalsUuid(v(manifest,"fp_schema_manifest","expected_binding_uuid"),request.bindingUuid) ||
      v(manifest,"fp_schema_manifest","format_version")!=MySqlFormat3Cell.SignedInteger(6L) ||
      v(manifest,"fp_schema_manifest","bootstrap_state")!=MySqlFormat3Cell.Text("READY") ||
      !equalsUuid(v(binding,"physical_recovery_binding","binding_uuid"),request.bindingUuid) ||
      v(binding,"physical_recovery_binding","format_version")!=MySqlFormat3Cell.SignedInteger(2L) ||
      v(binding,"physical_recovery_binding","cutover_state")!=MySqlFormat3Cell.Text("CERTIFIED") ||
      !equalsUuid(v(writer,W,"binding_uuid"),request.bindingUuid) || !equalsUuid(v(writer,W,"owner_boot_uuid"),request.tag.bootId) ||
      v(writer,W,"writer_epoch")!=MySqlFormat3Cell.SignedInteger(facts.writerEpoch) ||
      v(writer,W,"lease_until")!=MySqlFormat3Cell.DateTime(facts.leaseUntil) then fail("current settlement manifest/binding/writer differs")
    val operations=before.relation(O).rows
    if operations.exists(r=>equalsUuid(v(r,O,"binding_uuid"),request.bindingUuid) &&
      equalsUuid(v(r,O,"operation_uuid"),request.originalTag.operationId)) then
      fail("original C already exists; absent-C refusal forbidden even for RESERVED")
    val journal=originalJournal(before,request)
    MySqlDbFirstRecallJournalPersistedRead.requireCommittedIdentity(journal,before.inputs,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      val bytes=v(journal,O,"request_identity_bytes") match
        case MySqlFormat3Cell.Binary(b)=>b
        case _=>fail("original journal request must be binary")
      request.requireJournalIdentity(MySqlDbFirstRecallJournalRequest.decode(bytes,child))
    }
    // 自报COMMITTED/hash不够：重建原J完整before/target并独立复算其O/W-only变化。
    MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(before,request.journalTag.operationId,root)

  private def originalJournal(before:MySqlScriptSchemaSnapshot,request:MySqlDbFirstRecallRefusalRequest):Vector[MySqlFormat3Cell] =
    one(before,O,r=>equalsUuid(value(r,O,"binding_uuid",before.inputs),request.bindingUuid) &&
      equalsUuid(value(r,O,"operation_uuid",before.inputs),request.journalTag.operationId))

  private[mysql] def binary(bytes:Vector[Byte],owner:MySqlFormat3ReadBudget):MySqlFormat3Cell =
    owner.reserveRetainedMemory(64L+bytes.size,"refusal binary BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray,owner))
  private def uuid(id:UUID,owner:MySqlFormat3ReadBudget):MySqlFormat3Cell =
    owner.reserveRetainedMemory(80L,"refusal UUID BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(java.nio.ByteBuffer.allocate(16)
      .putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array(),owner))
  private def row(table:String,fields:Map[String,MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(fields.size==shape.columns.size && shape.columns.forall(c=>fields.contains(c.name)),"complete refusal row required")
    owner.reserveRetainedMemory(256L+96L*shape.columns.size,"refusal row BEFORE allocation")
    shape.columns.map(c=>fields(c.name))
  private def update(original:Vector[MySqlFormat3Cell],table:String,fields:Map[String,MySqlFormat3Cell],
      inputs:MySqlScriptSchemaInputs,owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    owner.reserveRetainedMemory(256L+96L*shape.columns.size,"refusal updated row BEFORE allocation")
    shape.columns.zip(original).map((c,v)=>fields.getOrElse(c.name,v))

  private[mysql] def operation(request:MySqlDbFirstRecallRefusalRequest,facts:MySqlDbFirstRecallRefusalFacts,
      before:MySqlScriptSchemaSnapshot,scope:MySqlRecoveryCanonicalBytes,
      derived:Option[(MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,Vector[Byte])],
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    owner.reserveRetainedMemory(16384L,"refusal exact operation fields BEFORE construction")
    val inputs=before.inputs
    val journal=originalJournal(before,request)
    def bytes(index:Int):MySqlFormat3Cell=derived.map { d=>
      (index match
        case 0=>d._1
        case 1=>d._2
        case 2=>d._3
        case _=>d._4).binaryCell
    }.getOrElse(Null)
    def hash(index:Int):MySqlFormat3Cell=derived.map { d=>
      binary((index match
        case 0=>d._1
        case 1=>d._2
        case 2=>d._3
        case _=>d._4).sha256,owner)
    }.getOrElse(Null)
    row(O,Map("binding_uuid"->uuid(request.bindingUuid,owner),"operation_uuid"->uuid(request.tag.operationId,owner),
      "workflow_uuid"->uuid(request.originalTag.operationId,owner),"step"->MySqlFormat3Cell.Text(MySqlDbFirstRecallRefusalCanonical.Step),
      "operation_state"->MySqlFormat3Cell.Text("COMMITTED"),"pod_uuid"->uuid(request.podUuid,owner),
      "attempt_uuid"->value(journal,O,"attempt_uuid",inputs),"footprint_uuid"->value(journal,O,"footprint_uuid",inputs),
      "source_unit_uuid"->Null,"forest_uuid"->Null,"expected_source_revision"->Null,"reserve_operation_uuid"->Null,
      "previous_step_operation_uuid"->uuid(request.journalTag.operationId,owner),"settled_by_operation_uuid"->Null,"original_diagnostic_identity"->Null,"original_diagnostic_sha256"->Null,
      "boot_uuid"->uuid(request.tag.bootId,owner),"boot_generation"->MySqlFormat3Cell.SignedInteger(request.tag.bootGeneration),
      "connection_generation_uuid"->uuid(facts.barrierGenerationUuid,owner),"connection_id"->MySqlFormat3Cell.SignedInteger(facts.connectionId),
      "fence_epoch"->MySqlFormat3Cell.SignedInteger(facts.writerEpoch),"request_identity_bytes"->request.canonical.binaryCell,
      "request_identity_sha256"->binary(request.canonical.sha256,owner),"expected_domain_version"->MySqlFormat3Cell.SignedInteger(request.expectedStateVersion),
      "expected_locator_revision"->value(journal,O,"expected_locator_revision",inputs),"context_encoding"->MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"),
      "original_context_bytes"->bytes(0),"original_context_sha256"->hash(0),"scope_bytes"->scope.binaryCell,"scope_sha256"->binary(scope.sha256,owner),
      "preimage_bytes"->bytes(1),"preimage_sha256"->hash(1),"target_bytes"->bytes(2),"target_sha256"->hash(2),
      "operation_fingerprint"->derived.map(d=>binary(d._5,owner)).getOrElse(Null),"commit_witness_bytes"->bytes(3),"commit_witness_sha256"->hash(3),
      "planned_at"->MySqlFormat3Cell.DateTime(facts.plannedAt),"fixed_lease_until"->MySqlFormat3Cell.DateTime(facts.leaseUntil),
      "result_at"->MySqlFormat3Cell.DateTime(facts.resultAt)),inputs,owner)

  private[mysql] def reserved(operation:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    update(operation,O,Map("operation_state"->MySqlFormat3Cell.Text("RESERVED"),"result_at"->Null,
      "commit_witness_bytes"->Null,"commit_witness_sha256"->Null),inputs,owner)

  private[mysql] def transition(before:MySqlScriptSchemaSnapshot,request:MySqlDbFirstRecallRefusalRequest,
      operation:Vector[MySqlFormat3Cell],fingerprint:MySqlFormat3Cell,owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3RelationSnapshot] =
    val inputs=before.inputs
    val old=before.relation(O)
    owner.reserveRetainedMemory(4096L+128L*(old.rows.size.toLong+1L)+64L*before.relations.size,"refusal O/W relation containers BEFORE allocation")
    val operations=(old.rows :+ operation).sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(O))<0)
    val writer=update(before.relation(W).rows.head,W,Map("last_commit_operation_uuid"->uuid(request.tag.operationId,owner),
      "last_commit_fingerprint"->fingerprint),inputs,owner)
    before.relations.map { r=>
      if r.table==O then MySqlFormat3RelationSnapshot(O,operations)
      else if r.table==W then MySqlFormat3RelationSnapshot(W,Vector(writer))
      else r
    }
