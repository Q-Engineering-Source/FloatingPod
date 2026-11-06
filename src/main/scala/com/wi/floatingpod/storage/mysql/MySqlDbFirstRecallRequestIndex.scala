package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRefusalReason

import com.wi.floatingpod.server.io.IoOperationTag
import java.sql.SQLDataException
import java.util.UUID

/** 持久DATA状态；保留态不签发终局反馈、重执行、absence或SQL资格。 */
private[floatingpod] enum MySqlDbFirstRecallRequestPhase:
  case JournalCommitted, OriginalCReserved, OriginalCCancelled, RecallCommitted, RefusedCommitted

/** 独立计费metadata，不保留request/canonical/Full27或比较child。 */
private[floatingpod] final class MySqlDbFirstRecallRequestRecord private[mysql] (
    val journalTag:IoOperationTag,val originalTag:IoOperationTag,val bindingUuid:UUID,val actorUuid:UUID,
    val podUuid:UUID,val expectedStateVersion:Long,val clientRequestUuid:Option[UUID],
    val journalRequestSha256:String,val phase:MySqlDbFirstRecallRequestPhase,
    val refusalReason:Option[RecallRefusalReason]=None)

/** 只索引实际J系列；没有匹配不是fresh SQL/admission许可，当前代检查由Main backing承担。 */
private[mysql] final class MySqlDbFirstRecallRequestIndex private (
    private val clients:Map[(UUID,UUID,UUID),MySqlDbFirstRecallRequestRecord],
    private val originals:Map[(UUID,UUID,UUID),MySqlDbFirstRecallRequestRecord],
    val census:Vector[MySqlDbFirstRecallRequestRecord]):
  private def namespace(binding:UUID,actor:UUID,id:UUID):Unit =
    val zero=new UUID(0L,0L)
    require(binding!=null && binding!=zero && actor!=null && actor!=zero && id!=null && id!=zero,
      "authenticated binding/actor and original request identity required")
  def client(binding:UUID,actor:UUID,request:UUID):Option[MySqlDbFirstRecallRequestRecord] =
    namespace(binding,actor,request);clients.get((binding,actor,request))
  def original(binding:UUID,actor:UUID,operation:UUID):Option[MySqlDbFirstRecallRequestRecord] =
    namespace(binding,actor,operation);originals.get((binding,actor,operation))

private[mysql] object MySqlDbFirstRecallRequestIndex:
  private val O="fp_recovery_operation"
  private def fail(message:String):Nothing=throw new SQLDataException("durable recall request index: "+message)
  def from(image:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlDbFirstRecallRequestIndex =
    MySqlFormat3QueryProjection.requireDbFirstSource(image,inputs,root)
    require(dataBudget!=null && !dataBudget.isClosed && dataBudget.sharesAllocationLedger(root) &&
      (dataBudget.profile eq root.profile),"independent metadata on exact live profile/ledger required")
    val rows=image.relation(O).rows
    def v(row:Vector[MySqlFormat3Cell],name:String)=MySqlDbFirstInitialActivationMaterializer.value(row,O,name,inputs)
    def uuid(row:Vector[MySqlFormat3Cell],name:String,id:UUID):Boolean=v(row,name) match
      case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(id)
      case _=>false
    def binary(row:Vector[MySqlFormat3Cell],name:String):MySqlFormat3Binary=v(row,name) match
      case MySqlFormat3Cell.Binary(bytes)=>bytes
      case _=>fail("required binary field: "+name)
    val count=rows.count(r=>v(r,"step")==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallJournalStep))
    dataBudget.reserveRetainedMemory(Math.addExact(1024L,Math.multiplyExact(count.toLong,2048L)),
      "durable J/C metadata and namespace maps BEFORE allocation")
    val clients=Map.newBuilder[(UUID,UUID,UUID),MySqlDbFirstRecallRequestRecord]
    val originals=Map.newBuilder[(UUID,UUID,UUID),MySqlDbFirstRecallRequestRecord]
    rows.foreach { row=>
      if v(row,"step")==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallJournalStep) then
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          val request=MySqlDbFirstRecallJournalRequest.decode(binary(row,"request_identity_bytes"),child)
          MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(image,request.journalTag.operationId,root)
          child.reserveRetainedMemory(128L+32L*rows.size,"original C selection metadata BEFORE allocation")
          val candidates=rows.filter(r=>uuid(r,"binding_uuid",request.bindingUuid) && uuid(r,"operation_uuid",request.originalTag.operationId))
          if candidates.size>1 then fail("original C row is not unique")
          var refusalReason=Option.empty[RecallRefusalReason]
          val phase=if candidates.isEmpty then MySqlDbFirstRecallRequestPhase.JournalCommitted else
            val actual=candidates.head
            if !uuid(actual,"workflow_uuid",request.originalTag.operationId) || !uuid(actual,"pod_uuid",request.podUuid) ||
                v(actual,"expected_domain_version")!=MySqlFormat3Cell.SignedInteger(request.expectedStateVersion) ||
                !uuid(actual,"previous_step_operation_uuid",request.journalTag.operationId) then fail("original C carrier differs from J")
            (v(actual,"step"),v(actual,"operation_state")) match
              case (MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallRefusedStep),MySqlFormat3Cell.Text("COMMITTED"))=>
                val refusal=MySqlDbFirstRecallRefusalRequest.decode(binary(actual,"request_identity_bytes"),child)
                refusal.requireJournalIdentity(request)
                refusalReason=refusal.refusalReason
                MySqlDbFirstRecallRefusalPersistedRead.verifyCommitted(image,request.originalTag.operationId,root)
                MySqlDbFirstRecallRequestPhase.RefusedCommitted
              case (MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep),state)=>
                MySqlDbFirstRecallPersistedRead.requireOriginalJournalRequest(actual,inputs,request,root)
                state match
                  case MySqlFormat3Cell.Text("COMMITTED")=>
                    MySqlDbFirstRecallPersistedRead.verifyCommitted(image,request.originalTag.operationId,root)
                    MySqlDbFirstRecallRequestPhase.RecallCommitted
                  case MySqlFormat3Cell.Text("RESERVED")=>MySqlDbFirstRecallRequestPhase.OriginalCReserved
                  case MySqlFormat3Cell.Text("CANCELLED")=>MySqlDbFirstRecallRequestPhase.OriginalCCancelled
                  case _=>fail("original C state unsupported")
              case _=>fail("original C exists with unsupported kind/state; never absence")
          def copyId(id:UUID)=new UUID(id.getMostSignificantBits,id.getLeastSignificantBits)
          def copyTag(tag:IoOperationTag)=IoOperationTag(copyId(tag.bootId),tag.bootGeneration,tag.connectionGeneration,copyId(tag.operationId))
          val entry=new MySqlDbFirstRecallRequestRecord(copyTag(request.journalTag),copyTag(request.originalTag),
            copyId(request.bindingUuid),copyId(request.actorUuid),copyId(request.podUuid),request.expectedStateVersion,
            request.clientRequestUuid.map(copyId),request.canonical.hexSha256,phase,refusalReason)
          originals+=((entry.bindingUuid,entry.actorUuid,entry.originalTag.operationId)->entry)
          entry.clientRequestUuid.foreach(id=>clients+=((entry.bindingUuid,entry.actorUuid,id)->entry))
        }
    }
    val byClient=clients.result();val byOriginal=originals.result()
    if byOriginal.size!=count then fail("duplicate original C namespace")
    val clientCount=byOriginal.valuesIterator.count(_.clientRequestUuid.nonEmpty)
    if byClient.size!=clientCount then fail("duplicate authenticated client request namespace")
    dataBudget.reserveRetainedMemory(Math.addExact(768L,Math.multiplyExact(count.toLong,16L)),
      "per-read durable request census/fixed metadata BEFORE allocation; independent Boot retention charged by Session")
    val census=byOriginal.valuesIterator.toVector
    new MySqlDbFirstRecallRequestIndex(byClient,byOriginal,census)
