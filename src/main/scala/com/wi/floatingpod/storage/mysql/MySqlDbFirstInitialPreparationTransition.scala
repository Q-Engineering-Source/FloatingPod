package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 原Work持有的完整准备DATA；真实SQL须先把O更新为COMMITTED，再依schema6精确prepare分支释放R。不是ACK。 */
private[mysql] final class MySqlDbFirstInitialPreparationTransition private[mysql] (
    val work:MySqlDbFirstInitialPreparationWork,val draft:MySqlDbFirstInitialPreparationDraft,
    val facts:MySqlDbFirstInitialPreparationFacts):
  private val root=work.root
  private val data=MySqlDbFirstInitialPreparationData(work.reservation.tag,work.reservation.request,
    work.reservation.activeLimit,work.requestBacking)
  private val O="fp_recovery_operation"
  private val R="fp_recovery_reservation"
  private val W="fp_runtime_writer"
  private val Null=MySqlFormat3Cell.SqlNull
  private var rows:Vector[MySqlFormat3RelationSnapshot]=null
  private var scope:MySqlRecoveryCanonicalBytes=null
  private var preimage:MySqlRecoveryCanonicalBytes=null
  private var projectedTarget:MySqlRecoveryCanonicalBytes=null
  private var fingerprint:MySqlRecoveryCanonicalBytes=null
  private var context:MySqlRecoveryCanonicalBytes=null
  private var witness:MySqlRecoveryCanonicalBytes=null
  private var held:Vector[MySqlFormat3Cell]=null
  private var reserved:Vector[MySqlFormat3Cell]=null
  private var complete:MySqlScriptSchemaSnapshot=null
  private var started=false
  private def current():Unit=work.reservation.core.requireExecutingPreparationTransition(this)
  private def before=draft.before
  private def inputs=before.inputs
  private def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=
    MySqlDbFirstInitialActivationMaterializer.value(row,table,name,inputs)
  private def signed(n:Long)=MySqlFormat3Cell.SignedInteger(n)
  private def text(s:String)=MySqlFormat3Cell.Text(s)
  private def uuid(id:java.util.UUID):MySqlFormat3Cell =
    root.reserveRetainedMemory(80L,"preparation UUID cell BEFORE allocation")
    val bytes=java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array()
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,root))
  private def binary(bytes:Vector[Byte]):MySqlFormat3Cell =
    root.reserveRetainedMemory(64L+bytes.size,"preparation binary vector BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray,root))
  private def hash(bytes:MySqlRecoveryCanonicalBytes)=binary(bytes.sha256)
  // 仅内部未完成scaffold允许derived字段为空；完整capture负责真实列约束。
  private def row(table:String,fields:Map[String,MySqlFormat3Cell]):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(fields.size==shape.columns.size && shape.columns.forall(c=>fields.contains(c.name)),"complete preparation row required")
    root.reserveRetainedMemory(256L+96L*shape.columns.size,"preparation row BEFORE allocation")
    shape.columns.map(c=>fields(c.name))
  private def update(old:Vector[MySqlFormat3Cell],table:String,fields:Map[String,MySqlFormat3Cell]):Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(old.size==shape.columns.size && fields.keys.forall(n=>shape.columns.exists(_.name==n)),"exact preparation update shape")
    root.reserveRetainedMemory(256L+96L*shape.columns.size,"preparation updated row BEFORE allocation")
    shape.columns.zip(old).map((c,v)=>fields.getOrElse(c.name,v))
  private def put(table:String,replacement:Vector[MySqlFormat3Cell],replace:Boolean):Unit =
    val shape=inputs.forTable(table)
    val old=rows.find(_.table==table).getOrElse(throw new SQLDataException("missing preparation relation "+table))
    def sameKey(r:Vector[MySqlFormat3Cell])=shape.primaryKey.forall(n=>value(r,table,n)==value(replacement,table,n))
    require(old.rows.count(sameKey)==(if replace then 1 else 0),"preparation original primary-key expectation differs")
    root.reserveRetainedMemory(4096L+128L*(old.rows.size.toLong+1L)+64L*rows.size,"preparation relation replacement BEFORE allocation")
    val changed=if replace then old.rows.map(r=>if sameKey(r) then replacement else r) else old.rows :+ replacement
    val ordered=changed.sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,shape)<0)
    rows=rows.map(r=>if r.table==table then MySqlFormat3RelationSnapshot(table,ordered) else r)

  private[mysql] def construct():Unit =
    current()
    require(!started && facts != null && facts.plannedAt==draft.plannedAt,"original one-shot preparation timing required")
    started=true
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      root.reserveRetainedMemory(32768L,"preparation transition fixed maps/context BEFORE allocation")
      val c=MySqlDbFirstInitialPreparationCanonical
      scope=c.scope(data,facts,before,root)
      rows=draft.domainConstruction
      val request=data.request
      val binding=request.expectedBinding.bindingUuid
      val op=data.tag.operationId
      val writer=before.relation(W).rows.head
      val released=MySqlDbFirstInitialPreparationRows.releasedReservation(data,inputs,root)
      held=update(released,R,Map("reservation_state"->text("HELD"),"reservation_revision"->signed(0)))
      val operation=row(O,Map(
        "source_unit_uuid"->Null,"forest_uuid"->Null,"expected_source_revision"->Null,
        "reserve_operation_uuid"->Null,"previous_step_operation_uuid"->Null,"settled_by_operation_uuid"->Null,
        "original_diagnostic_identity"->Null,"original_diagnostic_sha256"->Null,
        "original_context_bytes"->Null,"original_context_sha256"->Null,"preimage_bytes"->Null,"preimage_sha256"->Null,
        "target_bytes"->Null,"target_sha256"->Null,"operation_fingerprint"->Null,"commit_witness_bytes"->Null,"commit_witness_sha256"->Null,
        "binding_uuid"->uuid(binding),"operation_uuid"->uuid(op),"workflow_uuid"->uuid(op),
        "step"->text("REARM"),"operation_state"->text("COMMITTED"),"pod_uuid"->uuid(request.podUuid),
        "attempt_uuid"->uuid(request.attemptUuid),"footprint_uuid"->uuid(request.footprintUuid),
        "boot_uuid"->uuid(data.tag.bootId),"boot_generation"->signed(data.tag.bootGeneration),
        "connection_generation_uuid"->uuid(facts.barrierGenerationUuid),"connection_id"->signed(facts.connectionId),"fence_epoch"->signed(facts.writerEpoch),
        "request_identity_bytes"->data.backing.canonical.binaryCell,"request_identity_sha256"->binary(data.backing.sha256),
        "expected_domain_version"->signed(0),"expected_locator_revision"->signed(0),
        "context_encoding"->text("FP_RECOVERY_OPERATION_BE_V1"),"scope_bytes"->scope.binaryCell,"scope_sha256"->hash(scope),
        "planned_at"->MySqlFormat3Cell.DateTime(facts.plannedAt),"fixed_lease_until"->MySqlFormat3Cell.DateTime(facts.leaseUntil),
        "result_at"->MySqlFormat3Cell.DateTime(facts.resultAt)))
      put(R,released,false);put(O,operation,false)
      put(W,update(writer,W,Map("last_commit_operation_uuid"->uuid(op),"last_commit_fingerprint"->Null)),true)
      preimage=c.projection(data,before,before.relations,false,root)
      projectedTarget=c.projection(data,before,rows,true,root)
      fingerprint=c.fingerprint(data,scope,preimage,projectedTarget,root)
      context=c.context(data,facts,fingerprint,root)
      witness=c.witness(data,scope,preimage,projectedTarget,fingerprint,context,root)
      var finalO=operation
      Vector("original_context"->context,"preimage"->preimage,"target"->projectedTarget,"commit_witness"->witness).foreach { (prefix,bytes)=>
        finalO=update(finalO,O,Map((prefix+"_bytes")->bytes.binaryCell,(prefix+"_sha256")->hash(bytes)))
      }
      val generic=c.operationContext(data,facts,fingerprint,root).canonicalFingerprintSha256
      finalO=update(finalO,O,Map("operation_fingerprint"->binary(generic)))
      reserved=update(finalO,O,Map("operation_state"->text("RESERVED"),"result_at"->Null,
        "commit_witness_bytes"->Null,"commit_witness_sha256"->Null))
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(reserved,root.profile,root,Some(inputs))
      put(O,finalO,true)
      put(W,update(writer,W,Map("last_commit_operation_uuid"->uuid(op),"last_commit_fingerprint"->binary(generic))),true)
      complete=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,inputs,rows,root)
      verifyTarget(complete)
    }

  private[mysql] def target:MySqlScriptSchemaSnapshot =
    current();require(complete != null,"preparation complete target unavailable");complete
  private[mysql] def heldReservation:Vector[MySqlFormat3Cell] =
    target;held
  private[mysql] def reservedOperation:Vector[MySqlFormat3Cell] =
    target;reserved

  /** 从原before和完整candidate再算闭环；不接受单独hash或caller布尔ACK。 */
  private[mysql] def verifyTarget(candidate:MySqlScriptSchemaSnapshot):Unit =
    current()
    require(complete != null && candidate != null && (candidate.inputs eq inputs) &&
      (candidate.allocationBudget eq root) && candidate.exactlyEquals(complete),"exact original preparation target required")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val c=MySqlDbFirstInitialPreparationCanonical
      val s=c.scope(data,facts,before,root)
      val p=c.projection(data,before,before.relations,false,root)
      val t=c.projection(data,before,candidate.relations,true,root)
      val f=c.fingerprint(data,s,p,t,root)
      val x=c.context(data,facts,f,root)
      val w=c.witness(data,s,p,t,f,x,root)
      require(scope.exactlyEquals(s) && preimage.exactlyEquals(p) && projectedTarget.exactlyEquals(t) &&
        fingerprint.exactlyEquals(f) && context.exactlyEquals(x) && witness.exactlyEquals(w),
        "preparation canonical recomputation differs")
      val operation=candidate.relation(O).rows.find(r=>MySqlDbFirstInitialActivationMaterializer.uuidEquals(value(r,O,"operation_uuid"),data.tag.operationId))
        .getOrElse(throw new SQLDataException("original preparation O missing"))
      MySqlDbFirstInitialPreparationPersistedRead.requireCommittedIdentity(operation,inputs,root)
      Vector("scope_bytes"->s,"preimage_bytes"->p,"target_bytes"->t,"original_context_bytes"->x,"commit_witness_bytes"->w).foreach { (name,bytes)=>
        require(bytes.exactlyMatchesCell(value(operation,O,name)),"preparation persisted canonical differs: "+name)
      }
      val generic=c.operationContext(data,facts,f,root).canonicalFingerprintSha256
      def matches(cell:MySqlFormat3Cell)=cell match
        case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsVector(generic)
        case _=>false
      require(matches(value(operation,O,"operation_fingerprint")) &&
        matches(value(candidate.relation(W).rows.head,W,"last_commit_fingerprint")),"preparation O/W fingerprint differs")
    }

private[mysql] object MySqlDbFirstInitialPreparationTransition:
  val HolderBytes=2048L
