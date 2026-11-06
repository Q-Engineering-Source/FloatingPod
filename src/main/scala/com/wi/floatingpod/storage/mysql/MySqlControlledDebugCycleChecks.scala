package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 每个 omission 的独立固定 expected bytes；RAW 自洽或 descriptor 不代替本检查。 */
private[mysql] object MySqlControlledDebugCycleChecks:
  def requireOperation(row: Vector[MySqlFormat3Cell],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,operationUuid: UUID,step: String,
      scope: MySqlRecoveryCanonicalBytes,standardContext: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes,witness: Option[MySqlRecoveryCanonicalBytes],
      resultAt: Option[LocalDateTime],root: MySqlFormat3ReadBudget): Unit =
    if row==null || request==null || plan==null || context==null || root==null || root.isClosed ||
      (context.exactLimitProfile ne root.profile) || context.tag!=plan.tag || context.workflowUuid!=plan.workflowUuid ||
      context.reserveOperationUuid!=plan.reserveOperationUuid || context.bindingUuid!=request.expectedBinding.bindingUuid ||
      operationUuid==null || (operationUuid!=plan.reserveOperationUuid && operationUuid!=plan.executeOperationUuid) ||
      (step!="RESERVE" && step!="REARM" && step!="ACTIVATE") || scope==null || standardContext==null ||
      before==null || target==null || fingerprint==null || witness==null || resultAt==null ||
      witness.exists(_==null) || resultAt.exists(_==null) || witness.nonEmpty!=resultAt.nonEmpty then
      fail("complete independently generated current operation expectations required")
    if scope.encoding!="FP_MYSQL_CONTROLLED_DEBUG_SCOPE_BE_V1" ||
      standardContext.encoding!="FP_RECOVERY_OPERATION_BE_V1" ||
      before.encoding!="FP_MYSQL_CONTROLLED_DEBUG_EFFECT_BE_V2" || target.encoding!="FP_MYSQL_CONTROLLED_DEBUG_EFFECT_BE_V2" ||
      fingerprint.encoding!="FP_MYSQL_CONTROLLED_DEBUG_FINGERPRINT_BE_V2" ||
      witness.exists(_.encoding!="FP_MYSQL_CONTROLLED_DEBUG_WITNESS_BE_V2") then fail("fixed controlled canonical domains required")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row,root.profile,child)
      requireUuid(row,"binding_uuid",request.expectedBinding.bindingUuid)
      requireUuid(row,"operation_uuid",operationUuid);requireUuid(row,"workflow_uuid",plan.workflowUuid)
      requireUuid(row,"pod_uuid",request.podUuid);requireUuid(row,"attempt_uuid",request.attemptUuid)
      requireUuid(row,"footprint_uuid",request.footprintUuid)
      requireUuid(row,"boot_uuid",context.tag.bootId)
      requireInteger(row,"boot_generation",context.tag.bootGeneration)
      requireUuid(row,"connection_generation_uuid",context.barrierGenerationUuid)
      requireInteger(row,"connection_id",context.actualConnectionId)
      requireInteger(row,"fence_epoch",context.writerEpoch)
      requireText(row,"step",step)
      requireText(row,"operation_state",if witness.nonEmpty then "COMMITTED" else "RESERVED")
      pair(row,"request_identity_bytes","request_identity_sha256",plan.requestIdentity.canonical)
      pair(row,"original_context_bytes","original_context_sha256",standardContext)
      pair(row,"scope_bytes","scope_sha256",scope)
      pair(row,"preimage_bytes","preimage_sha256",before)
      pair(row,"target_bytes","target_sha256",target)
      digest(row,"operation_fingerprint",fingerprint)
      witness match
        case Some(value) => pair(row,"commit_witness_bytes","commit_witness_sha256",value)
        case None =>
          requireNull(row,"commit_witness_bytes");requireNull(row,"commit_witness_sha256")
      resultAt match
        case Some(value) => cell(row,"result_at") match
          case MySqlFormat3Cell.DateTime(actual) if actual==value && actual.getNano%1000==0 => ()
          case _ => fail("current committed result time differs")
        case None => requireNull(row,"result_at")
      if witness.isEmpty then requireNull(row,"settled_by_operation_uuid")
    }

  def requireWriter(row: Vector[MySqlFormat3Cell],binding: UUID,operation: UUID,
      context: MySqlOperationContext): Unit =
    if row==null || binding==null || operation==null || context==null ||
      context.bindingUuid!=binding || context.operationUuid!=operation then fail("original phase writer context required")
    val table="fp_runtime_writer"
    def value(name: String)=MySqlModeOperationEffectCodec.value(row,table,name)
    value("binding_uuid") match
      case MySqlFormat3Cell.Binary(actual) if actual.equalsUuid(binding) => ()
      case _ => fail("writer binding differs")
    value("last_commit_operation_uuid") match
      case MySqlFormat3Cell.Binary(actual) if actual.equalsUuid(operation) => ()
      case _ => fail("writer phase operation differs")
    value("last_commit_fingerprint") match
      case MySqlFormat3Cell.Binary(actual) if actual.length==32 && actual.equalsVector(context.canonicalFingerprintSha256) => ()
      case _ => fail("writer independently generated fingerprint differs")

  private def pair(row: Vector[MySqlFormat3Cell],bytes: String,sha: String,expected: MySqlRecoveryCanonicalBytes): Unit =
    if !expected.exactlyMatchesCell(cell(row,bytes)) then fail("independently generated operation bytes differ")
    digest(row,sha,expected)
  private def digest(row: Vector[MySqlFormat3Cell],name: String,expected: MySqlRecoveryCanonicalBytes): Unit =
    cell(row,name) match
      case MySqlFormat3Cell.Binary(actual) if actual.length==32 && actual.equalsVector(expected.sha256) => ()
      case _ => fail("independently generated operation digest differs")
  private def cell(row: Vector[MySqlFormat3Cell],name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,"fp_recovery_operation",name)
  private def requireUuid(row: Vector[MySqlFormat3Cell],name: String,expected: UUID): Unit = cell(row,name) match
    case MySqlFormat3Cell.Binary(actual) if actual.equalsUuid(expected) => ()
    case _ => fail("original current operation identity differs")
  private def requireText(row: Vector[MySqlFormat3Cell],name: String,expected: String): Unit = cell(row,name) match
    case MySqlFormat3Cell.Text(actual) if actual==expected => ()
    case _ => fail("fixed current operation state differs")
  private def requireNull(row: Vector[MySqlFormat3Cell],name: String): Unit =
    if cell(row,name)!=MySqlFormat3Cell.SqlNull then fail("reserved current field requires actual SQL NULL")
  private def requireInteger(row: Vector[MySqlFormat3Cell],name: String,expected: Long): Unit = cell(row,name) match
    case MySqlFormat3Cell.SignedInteger(actual) if actual==expected => ()
    case _ => fail("original current connection or generation differs")
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled independent cycle validation: " + reason)
