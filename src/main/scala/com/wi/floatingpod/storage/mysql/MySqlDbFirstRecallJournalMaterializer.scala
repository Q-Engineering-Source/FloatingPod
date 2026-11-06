package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.UUID

/** caller先保管此槽，再构造DATA；失败保留原输入、partial canonical与cause，不重试或退款。 */
private[mysql] final class MySqlDbFirstRecallJournalDraft private[mysql] (
    val request:MySqlDbFirstRecallJournalRequest,val facts:MySqlDbFirstRecallJournalFacts,
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
  private[mysql] var result:MySqlDbFirstRecallJournalMaterialization=null

/** 完整O/W-only DATA；没有SQL执行、ACK或freeze能力，也不表示当前已安装SQL允许新step。 */
private[mysql] final class MySqlDbFirstRecallJournalMaterialization private[mysql] (
    private val draft:MySqlDbFirstRecallJournalDraft):
  def request:MySqlDbFirstRecallJournalRequest=draft.request
  def before:MySqlScriptSchemaSnapshot=draft.before
  def target:MySqlScriptSchemaSnapshot=draft.target
  def reservedOperation:Vector[MySqlFormat3Cell]=draft.reservedOperation
  def verify():Unit=verifyTarget(target)
  def verifyTarget(candidate:MySqlScriptSchemaSnapshot):Unit =
    val root=draft.root
    require(root!=null && !root.isClosed && !root.isScopedReadBudget && draft.failure==null &&
      (draft.result eq this) && candidate!=null && (candidate.inputs eq before.inputs) &&
      (candidate.allocationBudget eq root) && !before.allocationBudget.isClosed,
      "actual live completed journal DATA and original allocation family required")
    if candidate.schema!=target.schema || candidate.connection!=target.connection ||
      candidate.observationScope!=target.observationScope || candidate.maxAllowedPacketBytes!=target.maxAllowedPacketBytes then
      throw new SQLDataException("journal target observation identity differs")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      MySqlDbFirstRecallJournalMaterializer.preflight(request,draft.facts,before,root)
      val c=MySqlDbFirstRecallJournalCanonical
      val scope=c.scope(request,draft.facts,before,root)
      val preimage=c.projection(request,before,before.relations,false,root)
      val projected=c.projection(request,before,candidate.relations,true,root)
      val fingerprint=c.fingerprint(request,scope,preimage,projected,root)
      val context=c.context(request,draft.facts,fingerprint,root)
      val witness=c.witness(request,scope,preimage,projected,fingerprint,context,root)
      if !draft.scope.exactlyEquals(scope) || !draft.preimage.exactlyEquals(preimage) ||
        !draft.projectedTarget.exactlyEquals(projected) || !draft.fingerprint.exactlyEquals(fingerprint) ||
        !draft.context.exactlyEquals(context) || !draft.witness.exactlyEquals(witness) then
        throw new SQLDataException("journal canonical recomputation differs")
      val generic=c.operationContext(request,draft.facts,fingerprint,root).canonicalFingerprintSha256
      val operation=MySqlDbFirstRecallJournalMaterializer.operation(request,draft.facts,before,scope,
        Some((context,preimage,projected,witness,generic)),active)
      val expected=MySqlDbFirstRecallJournalMaterializer.transition(before,request,operation,
        MySqlDbFirstRecallJournalMaterializer.binary(generic,active),active)
      if expected!=candidate.relations ||
        MySqlDbFirstRecallJournalMaterializer.reserved(operation,before.inputs,active)!=reservedOperation then
        throw new SQLDataException("journal independent complete O/W transition differs")
    }

/** 新Fresh journal的完整规范构造；旧live导入/负向结果使用自己的事实与域，不借此伪造。 */
private[mysql] object MySqlDbFirstRecallJournalMaterializer:
  private val O="fp_recovery_operation"
  private val W="fp_runtime_writer"
  private val D="floating_pod_state"
  private val L="physical_recovery_locator"
  private val Null=MySqlFormat3Cell.SqlNull
  private def fail(message:String):Nothing=throw new SQLDataException("recall journal: "+message)
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

  def allocate(request:MySqlDbFirstRecallJournalRequest,facts:MySqlDbFirstRecallJournalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):MySqlDbFirstRecallJournalDraft =
    require(request!=null && facts!=null && before!=null && root!=null && !root.isClosed &&
      !root.isScopedReadBudget && (before.allocationBudget eq root),
      "journal original owned Full27 DATA root required")
    root.requireScriptDataInputs(before.inputs)
    root.reserveRetainedMemory(1024L,"journal partial draft BEFORE allocation")
    new MySqlDbFirstRecallJournalDraft(request,facts,before,root)

  def materialize(draft:MySqlDbFirstRecallJournalDraft):MySqlDbFirstRecallJournalMaterialization =
    require(draft!=null && !draft.attempted && draft.result==null && draft.failure==null,
      "original unattempted journal draft required; no materialization retry")
    draft.attempted=true
    val root=draft.root
    try
      preflight(draft.request,draft.facts,draft.before,root)
      root.reserveRetainedMemory(65536L,"journal fixed construction maps/metadata BEFORE allocation")
      val c=MySqlDbFirstRecallJournalCanonical
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
        draft.result=new MySqlDbFirstRecallJournalMaterialization(draft)
      }
      draft.result.verify()
      draft.result
    catch case cause:Throwable=>
      draft.failure=cause
      throw cause

  /** 仅当前完整DATA的局部约束；真实历史审计、认证与held scope仍须由执行owner核定。 */
  private[mysql] def preflight(request:MySqlDbFirstRecallJournalRequest,facts:MySqlDbFirstRecallJournalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):Unit =
    require(request!=null && facts!=null && before!=null && root!=null && !root.isClosed &&
      !root.isScopedReadBudget && (before.allocationBudget eq root) &&
      before.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,"exact original complete journal before required")
    requireBeforeRows(request,facts,before,root)

  /** 冷读仅在原comparison child重算DATA；不扩大实际draft的原root要求。 */
  private[mysql] def requireLocalTransition(before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot,
      request:MySqlDbFirstRecallJournalRequest,facts:MySqlDbFirstRecallJournalFacts,
      root:MySqlFormat3ReadBudget):Unit =
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(child.isCanonicalComparisonChildOf(root) && !child.isClosed &&
      (before.allocationBudget eq child) && (target.allocationBudget eq child),
      "exact original cold journal comparison DATA required")
    child.reserveRetainedMemory(65536L,"cold journal semantic reconstruction BEFORE allocation")
    MySqlControlledDebugRelations.requireValidDbFirstComparisonGraph(before,before.inputs,root)
    requireBeforeRows(request,facts,before,root)
    val c=MySqlDbFirstRecallJournalCanonical
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

  private def requireBeforeRows(request:MySqlDbFirstRecallJournalRequest,facts:MySqlDbFirstRecallJournalFacts,
      before:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):Unit =
    require(root!=null && !root.isClosed && !root.isScopedReadBudget && before!=null &&
      !before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(root) &&
      ((before.allocationBudget eq root) || before.allocationBudget.isCanonicalComparisonChildOf(root)),
      "original live or cold journal DATA allocation family required")
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
      !equalsUuid(v(writer,W,"binding_uuid"),request.bindingUuid) || !equalsUuid(v(writer,W,"owner_boot_uuid"),request.journalTag.bootId) ||
      v(writer,W,"writer_epoch")!=MySqlFormat3Cell.SignedInteger(facts.writerEpoch) ||
      v(writer,W,"lease_until")!=MySqlFormat3Cell.DateTime(facts.leaseUntil) then fail("original manifest/binding/writer differs")
    val domain=one(before,D,r=>equalsUuid(v(r,D,"uuid"),request.podUuid))
    val locator=one(before,L,r=>equalsUuid(v(r,L,"binding_uuid"),request.bindingUuid) && equalsUuid(v(r,L,"pod_uuid"),request.podUuid))
    if !equalsUuid(v(domain,D,"binding_uuid"),request.bindingUuid) || !equalsUuid(v(domain,D,"owner_uuid"),request.actorUuid) ||
      v(domain,D,"state_version")!=MySqlFormat3Cell.SignedInteger(request.expectedStateVersion) ||
      v(domain,D,"is_active")!=MySqlFormat3Cell.SignedInteger(1L) ||
      v(locator,L,"domain_version")!=v(domain,D,"state_version") || v(locator,L,"locator_state")!=MySqlFormat3Cell.Text("ROW_ACTIVE") then
      fail("original active owned pod/version/locator differs")
    val operations=before.relation(O).rows
    if operations.exists(r=>equalsUuid(v(r,O,"binding_uuid"),request.bindingUuid) &&
      (equalsUuid(v(r,O,"operation_uuid"),request.journalTag.operationId) || equalsUuid(v(r,O,"operation_uuid"),request.originalTag.operationId))) then
      fail("original journal/business operation already exists")
    // J之外的历史RAW仍由完整历史owner验证；已有J的重复/未终局责任必须逐字段读取，不能只看最新W。
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      operations.foreach { row=>
        if v(row,O,"step")==MySqlFormat3Cell.Text(MySqlDbFirstRecallJournalCanonical.Step) then
          val bytes=v(row,O,"request_identity_bytes") match
            case MySqlFormat3Cell.Binary(b)=>b
            case _=>fail("existing journal request is not binary")
          val existing=MySqlDbFirstRecallJournalRequest.decode(bytes,child)
          if !equalsUuid(v(row,O,"binding_uuid"),existing.bindingUuid) ||
            !equalsUuid(v(row,O,"operation_uuid"),existing.journalTag.operationId) ||
            !equalsUuid(v(row,O,"workflow_uuid"),existing.originalTag.operationId) ||
            !equalsUuid(v(row,O,"pod_uuid"),existing.podUuid) ||
            !equalsUuid(v(row,O,"boot_uuid"),existing.journalTag.bootId) ||
            v(row,O,"boot_generation")!=MySqlFormat3Cell.SignedInteger(existing.journalTag.bootGeneration) ||
            v(row,O,"expected_domain_version")!=MySqlFormat3Cell.SignedInteger(existing.expectedStateVersion) ||
            v(row,O,"operation_state")!=MySqlFormat3Cell.Text("COMMITTED") then
            fail("existing journal request differs from its actual carrier O identity/state")
          v(row,O,"request_identity_sha256") match
            case MySqlFormat3Cell.Binary(hash) if hash.equalsVector(existing.canonical.sha256)=>()
            case _=>fail("existing journal request digest differs from carrier O")
          requireScopeRequestPrefix(v(row,O,"scope_bytes"),existing,child)
          if existing.bindingUuid==request.bindingUuid then
            if existing.actorUuid==request.actorUuid && request.clientRequestUuid.nonEmpty &&
              existing.clientRequestUuid==request.clientRequestUuid then fail("original client request already has a journal")
            val terminal=operations.exists(r=>equalsUuid(v(r,O,"operation_uuid"),existing.originalTag.operationId) &&
              equalsUuid(v(r,O,"binding_uuid"),request.bindingUuid) && v(r,O,"operation_state")==MySqlFormat3Cell.Text("COMMITTED") &&
              equalsUuid(v(r,O,"workflow_uuid"),existing.originalTag.operationId) &&
              equalsUuid(v(r,O,"pod_uuid"),existing.podUuid) &&
              v(r,O,"expected_domain_version")==MySqlFormat3Cell.SignedInteger(existing.expectedStateVersion) &&
              (v(r,O,"step")==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep) ||
                v(r,O,"step")==MySqlFormat3Cell.Text("DB_FIRST_RECALL_REFUSED")))
            if existing.podUuid==request.podUuid && !terminal then fail("pod retains unresolved original journal responsibility")
      }
    }

  /** scope中另存的原numeric connection generation也须匹配；完整历史其余frame另由strict reader负责。 */
  private def requireScopeRequestPrefix(cell:MySqlFormat3Cell,request:MySqlDbFirstRecallJournalRequest,
      owner:MySqlFormat3ReadBudget):Unit =
    owner.reserveRetainedMemory(512L,"existing journal scope identity cursor BEFORE verification")
    val bytes=cell match
      case MySqlFormat3Cell.Binary(value)=>value
      case _=>fail("existing journal scope is not binary")
    var offset=0
    def octet():Int =
      if offset>=bytes.length then fail("truncated existing journal scope identity")
      val value=bytes.byteAt(offset)&255
      offset+=1
      value
    def int32():Int=(octet()<<24)|(octet()<<16)|(octet()<<8)|octet()
    def int64():Long =
      var n=0L
      var i=0
      while i<8 do
        n=(n<<8)|octet().toLong
        i+=1
      n
    def uuid(id:UUID):Unit =
      if int64()!=id.getMostSignificantBits || int64()!=id.getLeastSignificantBits then
        fail("existing journal scope request UUID differs")
    val domain=MySqlDbFirstRecallJournalCanonical.Scope
    if int32()!=domain.length then fail("existing journal scope domain length differs")
    var i=0
    while i<domain.length do
      if octet()!=domain.charAt(i).toInt then fail("existing journal scope domain differs")
      i+=1
    if octet()!=1 then fail("existing journal scope version differs")
    uuid(request.journalTag.bootId)
    if int64()!=request.journalTag.bootGeneration || int64()!=request.journalTag.connectionGeneration then
      fail("existing journal scope request generation differs")
    uuid(request.journalTag.operationId);uuid(request.bindingUuid);uuid(request.podUuid);uuid(request.originalTag.operationId)

  private[mysql] def binary(bytes:Vector[Byte],owner:MySqlFormat3ReadBudget):MySqlFormat3Cell =
    owner.reserveRetainedMemory(64L+bytes.size,"journal binary BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray,owner))
  private def uuid(id:UUID,owner:MySqlFormat3ReadBudget):MySqlFormat3Cell =
    owner.reserveRetainedMemory(80L,"journal UUID BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(java.nio.ByteBuffer.allocate(16)
      .putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array(),owner))
  private def row(table:String,fields:Map[String,MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(fields.size==shape.columns.size && shape.columns.forall(c=>fields.contains(c.name)),"complete journal row required")
    owner.reserveRetainedMemory(256L+96L*shape.columns.size,"journal row BEFORE allocation")
    shape.columns.map(c=>fields(c.name))
  private def update(original:Vector[MySqlFormat3Cell],table:String,fields:Map[String,MySqlFormat3Cell],
      inputs:MySqlScriptSchemaInputs,owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    owner.reserveRetainedMemory(256L+96L*shape.columns.size,"journal updated row BEFORE allocation")
    shape.columns.zip(original).map((c,v)=>fields.getOrElse(c.name,v))

  private[mysql] def operation(request:MySqlDbFirstRecallJournalRequest,facts:MySqlDbFirstRecallJournalFacts,
      before:MySqlScriptSchemaSnapshot,scope:MySqlRecoveryCanonicalBytes,
      derived:Option[(MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,MySqlRecoveryCanonicalBytes,Vector[Byte])],
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    owner.reserveRetainedMemory(16384L,"journal exact operation fields BEFORE construction")
    val inputs=before.inputs
    val locator=one(before,L,r=>equalsUuid(value(r,L,"pod_uuid",inputs),request.podUuid) &&
      equalsUuid(value(r,L,"binding_uuid",inputs),request.bindingUuid))
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
    row(O,Map("binding_uuid"->uuid(request.bindingUuid,owner),"operation_uuid"->uuid(request.journalTag.operationId,owner),
      "workflow_uuid"->uuid(request.originalTag.operationId,owner),"step"->MySqlFormat3Cell.Text(MySqlDbFirstRecallJournalCanonical.Step),
      "operation_state"->MySqlFormat3Cell.Text("COMMITTED"),"pod_uuid"->uuid(request.podUuid,owner),
      "attempt_uuid"->value(locator,L,"current_attempt_uuid",inputs),"footprint_uuid"->value(locator,L,"current_footprint_uuid",inputs),
      "source_unit_uuid"->Null,"forest_uuid"->Null,"expected_source_revision"->Null,"reserve_operation_uuid"->Null,
      "previous_step_operation_uuid"->Null,"settled_by_operation_uuid"->Null,"original_diagnostic_identity"->Null,"original_diagnostic_sha256"->Null,
      "boot_uuid"->uuid(request.journalTag.bootId,owner),"boot_generation"->MySqlFormat3Cell.SignedInteger(request.journalTag.bootGeneration),
      "connection_generation_uuid"->uuid(facts.barrierGenerationUuid,owner),"connection_id"->MySqlFormat3Cell.SignedInteger(facts.connectionId),
      "fence_epoch"->MySqlFormat3Cell.SignedInteger(facts.writerEpoch),"request_identity_bytes"->request.canonical.binaryCell,
      "request_identity_sha256"->binary(request.canonical.sha256,owner),"expected_domain_version"->MySqlFormat3Cell.SignedInteger(request.expectedStateVersion),
      "expected_locator_revision"->value(locator,L,"locator_revision",inputs),"context_encoding"->MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"),
      "original_context_bytes"->bytes(0),"original_context_sha256"->hash(0),"scope_bytes"->scope.binaryCell,"scope_sha256"->binary(scope.sha256,owner),
      "preimage_bytes"->bytes(1),"preimage_sha256"->hash(1),"target_bytes"->bytes(2),"target_sha256"->hash(2),
      "operation_fingerprint"->derived.map(d=>binary(d._5,owner)).getOrElse(Null),"commit_witness_bytes"->bytes(3),"commit_witness_sha256"->hash(3),
      "planned_at"->MySqlFormat3Cell.DateTime(facts.plannedAt),"fixed_lease_until"->MySqlFormat3Cell.DateTime(facts.leaseUntil),
      "result_at"->MySqlFormat3Cell.DateTime(facts.resultAt)),inputs,owner)

  private[mysql] def reserved(operation:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3Cell] =
    update(operation,O,Map("operation_state"->MySqlFormat3Cell.Text("RESERVED"),"result_at"->Null,
      "commit_witness_bytes"->Null,"commit_witness_sha256"->Null),inputs,owner)

  private[mysql] def transition(before:MySqlScriptSchemaSnapshot,request:MySqlDbFirstRecallJournalRequest,
      operation:Vector[MySqlFormat3Cell],fingerprint:MySqlFormat3Cell,owner:MySqlFormat3ReadBudget):Vector[MySqlFormat3RelationSnapshot] =
    val inputs=before.inputs
    val old=before.relation(O)
    owner.reserveRetainedMemory(4096L+128L*(old.rows.size.toLong+1L)+64L*before.relations.size,"journal O/W relation containers BEFORE allocation")
    val operations=(old.rows :+ operation).sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(O))<0)
    val writer=update(before.relation(W).rows.head,W,Map("last_commit_operation_uuid"->uuid(request.journalTag.operationId,owner),
      "last_commit_fingerprint"->fingerprint),inputs,owner)
    before.relations.map { r=>
      if r.table==O then MySqlFormat3RelationSnapshot(O,operations)
      else if r.table==W then MySqlFormat3RelationSnapshot(W,Vector(writer))
      else r
    }
