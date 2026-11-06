package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.io.DataOutputStream
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 不可变规范DATA输入；backing只含原请求字节，没有Entry/SQL/placement资格。 */
private[mysql] final case class MySqlDbFirstInitialPreparationData(
    tag:IoOperationTag,request:MySqlControlledDebugPrepareRequest,activeLimit:Int,
    backing:MySqlRepositoryRequestIdentity):
  require(tag != null && request != null && backing != null && activeLimit>=1 && activeLimit<=256,
    "complete preparation request and configured capacity required")

/** 仅记录实际观测应绑定的事实，不以标量值签发held writer或commit资格。 */
private[mysql] final case class MySqlDbFirstInitialPreparationFacts(
    connectionId:Long,barrierGenerationUuid:UUID,writerEpoch:Long,
    leaseUntil:LocalDateTime,plannedAt:LocalDateTime,resultAt:LocalDateTime):
  private def micros(t:LocalDateTime)=t != null && t.getYear>=1000 && t.getYear<=9999 && t.getNano%1000==0
  require(connectionId>0L && barrierGenerationUuid != null && barrierGenerationUuid != new UUID(0L,0L) &&
    writerEpoch>0L && micros(leaseUntil) && micros(plannedAt) && micros(resultAt) &&
    !resultAt.isBefore(plannedAt) && leaseUntil.isAfter(resultAt),"exact preparation timing/connection facts required")

/** 完整27表准备transition规范字节；历史RAW绑定及derived占位规则独立于旧ACTIVATE域。 */
private[mysql] object MySqlDbFirstInitialPreparationCanonical:
  val Scope = "FP_DB_FIRST_INITIAL_PREPARATION_SCOPE_BE_V1"
  val Effect = "FP_DB_FIRST_INITIAL_PREPARATION_EFFECT_BE_V1"
  val Fingerprint = "FP_DB_FIRST_INITIAL_PREPARATION_FINGERPRINT_BE_V1"
  val Witness = "FP_DB_FIRST_INITIAL_PREPARATION_WITNESS_BE_V1"
  val Derived = "FP_DB_FIRST_INITIAL_PREPARATION_DERIVED_V1"
  private val Operation = "fp_recovery_operation"
  private val Writer = "fp_runtime_writer"
  private val derivedColumns = Set("original_context_bytes", "original_context_sha256",
    "preimage_bytes", "preimage_sha256", "target_bytes", "target_sha256",
    "operation_fingerprint", "commit_witness_bytes", "commit_witness_sha256")
  private val pairs = Map("request_identity_bytes" -> "request_identity_sha256",
    "original_context_bytes" -> "original_context_sha256", "scope_bytes" -> "scope_sha256",
    "preimage_bytes" -> "preimage_sha256", "target_bytes" -> "target_sha256",
    "commit_witness_bytes" -> "commit_witness_sha256", "original_diagnostic_identity" -> "original_diagnostic_sha256")
  private def encode(domain: String, root: MySqlFormat3ReadBudget)(body: DataOutputStream => Unit): MySqlRecoveryCanonicalBytes =
    val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(!active.isClosed && active.sharesAllocationLedger(root) &&
      ((active eq root) || active.isCanonicalChildOf(root)), "DB-first canonical requires original active allocation family")
    active.reserveTemporaryMemory(8192L,"DB-first canonical writer/row metadata BEFORE allocation")
    var primary: Throwable=null
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(active) {
      MySqlRecoveryCanonicalBytes.encode(domain,root.profile) { out =>
        MySqlRecoveryCanonicalBytes.writeToken(out,domain)
        out.writeByte(1)
        body(out)
      }
    }
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      active.releaseTemporaryMemory(8192L,"DB-first canonical writer/row metadata detached")))
  private def payload(out: DataOutputStream, bytes: MySqlRecoveryCanonicalBytes): Unit =
    out.writeInt(bytes.length);bytes.writeTo(out)
  private def vector(out: DataOutputStream, bytes: Vector[Byte]): Unit =
    out.writeInt(bytes.size);bytes.foreach(b => out.writeByte(b.toInt))

  private def requireRequest(request:MySqlDbFirstInitialPreparationData,root:MySqlFormat3ReadBudget):Unit =
    require(request != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "original preparation DATA request/root required")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val actual=MySqlRepositoryRequestIdentity.single(request.request.repositoryOperation,root.profile)
      require(actual.canonical.exactlyEquals(request.backing.canonical),"preparation backing differs from exact request")
    }

  def scope(request: MySqlDbFirstInitialPreparationData, facts: MySqlDbFirstInitialPreparationFacts,
      before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireRequest(request,root)
    require(facts != null && before != null && before.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "complete schema6 preparation facts/before required")
    root.requireScriptDataInputs(before.inputs)
    require(!before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(root) &&
      ((before.allocationBudget eq root) || before.allocationBudget.isSnapshotReadChildOf(root) ||
        before.allocationBudget.isCanonicalComparisonChildOf(root)),
      "preparation before must remain in original allocation family")
    require(before.observationScope.bootUuid==request.tag.bootId &&
      before.observationScope.bootGeneration==request.tag.bootGeneration &&
      before.observationScope.connectionGeneration==request.tag.connectionGeneration,
      "preparation original observation/tag differs")
    val writer=before.relation(Writer).rows
    require(writer.size==1,"preparation before requires unique writer")
    def writerValue(name:String)=MySqlDbFirstInitialActivationMaterializer.value(writer.head,Writer,name,before.inputs)
    def writerUuid(name:String,id:UUID)=MySqlDbFirstInitialActivationMaterializer.uuidEquals(writerValue(name),id)
    require(writerUuid("binding_uuid",request.request.expectedBinding.bindingUuid) && writerUuid("owner_boot_uuid",request.tag.bootId) &&
      writerValue("writer_epoch")==MySqlFormat3Cell.SignedInteger(facts.writerEpoch) &&
      writerValue("lease_until")==MySqlFormat3Cell.DateTime(facts.leaseUntil),
      "preparation facts differ from complete original writer DATA")
    encode(Scope,root) { out =>
      val t=request.tag
      MySqlRecoveryCanonicalBytes.writeUuid(out,t.bootId)
      out.writeLong(t.bootGeneration);out.writeLong(t.connectionGeneration)
      Vector(t.operationId,request.request.expectedBinding.bindingUuid,request.request.podUuid,request.request.attemptUuid,facts.barrierGenerationUuid)
        .foreach(MySqlRecoveryCanonicalBytes.writeUuid(out,_))
      out.writeLong(facts.connectionId);out.writeLong(facts.writerEpoch)
      out.writeInt(request.activeLimit)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.leaseUntil),out)
      out.writeLong(0L);out.writeLong(0L);out.writeLong(0L);out.writeLong(1L)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.plannedAt),out)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.resultAt),out)
      // 原before完整canonical同时绑定connection、manifest、旧W链与全部历史RAW。
      vector(out,before.canonicalSha256)
      vector(out,request.backing.sha256)
      vector(out,root.profile.sha256().toVector)
      // 原connection/header必须持久化，恢复时不能用重启后的观察替代原before。
      val c=before.connection
      MySqlRecoveryCanonicalBytes.writeUuid(out,c.serverUuid)
      Vector(c.serverVersion,c.catalog,c.effectiveUser,c.defaultStorageEngine).foreach(MySqlRecoveryCanonicalBytes.writeText(out,_))
      out.writeInt(c.innodbFlushLogAtTrxCommit);out.writeLong(c.syncBinlog)
      Vector(c.transactionIsolation,c.readTransactionIsolation,c.sqlMode,c.timeZone,c.tlsVersion,c.tlsCipher)
        .foreach(MySqlRecoveryCanonicalBytes.writeText(out,_))
      MySqlRecoveryCanonicalBytes.writeUuid(out,before.observationScope.bootUuid)
      out.writeLong(before.observationScope.bootGeneration);out.writeLong(before.observationScope.connectionGeneration)
      out.writeLong(before.maxAllowedPacketBytes)
    }

  def projection(request: MySqlDbFirstInitialPreparationData, before: MySqlScriptSchemaSnapshot,
      relations: Vector[MySqlFormat3RelationSnapshot], target: Boolean,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    val inputs=before.inputs
    requireRequest(request,root)
    require(inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,"preparation requires Full27 projection")
    require(relations.size==inputs.canonicalOrder.size,"DB-first projection must include all27 tables")
    var tableIndex=0
    while tableIndex<relations.size do
      require(relations(tableIndex).table==inputs.canonicalOrder(tableIndex).table,"DB-first projection closed table order")
      tableIndex+=1
    if target then
      val shape=inputs.forTable(Operation)
      val own=relations.find(_.table==Operation).get.rows.filter(row=>
        binary(row,shape,"binding_uuid").equalsUuid(request.request.expectedBinding.bindingUuid) &&
          binary(row,shape,"operation_uuid").equalsUuid(request.tag.operationId))
      require(own.size==1 && own.head(shape.columns.indexWhere(_.name=="step"))==MySqlFormat3Cell.Text("REARM"),
        "preparation target requires exactly its original REARM operation")
    encode(Effect,root) { out =>
      out.writeBoolean(target)
      MySqlRecoveryCanonicalBytes.writeText(out,before.schema)
      MySqlRecoveryCanonicalBytes.writeText(out,inputs.ddlSha256)
      vector(out,before.canonicalSha256)
      out.writeInt(relations.size)
      relations.foreach { relation =>
        val shape=inputs.forTable(relation.table)
        MySqlRecoveryCanonicalBytes.writeText(out,shape.table)
        out.writeInt(shape.columns.size)
        shape.columns.foreach { c =>
          MySqlRecoveryCanonicalBytes.writeText(out,c.name)
          MySqlRecoveryCanonicalBytes.writeText(out,c.sqlDeclaration)
        }
        out.writeInt(relation.rows.size)
        relation.rows.foreach { row =>
          if row.size != shape.columns.size then throw new SQLDataException("DB-first projection row shape")
          val own=shape.table==Operation && binary(row,shape,"binding_uuid").equalsUuid(request.request.expectedBinding.bindingUuid) &&
            binary(row,shape,"operation_uuid").equalsUuid(request.tag.operationId)
          val historical=shape.table==Operation && !own
          if historical then
            val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row,root.profile,active,Some(inputs))
          shape.columns.zipWithIndex.foreach { (column,index) =>
            val marker=target && ((own && derivedColumns.contains(column.name)) ||
              (shape.table==Writer && column.name=="last_commit_fingerprint"))
            if marker then
              out.writeByte(0x7f)
              MySqlRecoveryCanonicalBytes.writeToken(out,Derived)
              MySqlRecoveryCanonicalBytes.writeToken(out,"CURRENT")
              MySqlRecoveryCanonicalBytes.writeText(out,shape.table)
              out.writeInt(shape.primaryKey.size)
              shape.primaryKey.foreach(name => MySqlFormat3SnapshotCanonical.writeCell(row(shape.columns.indexWhere(_.name==name)),out))
              MySqlRecoveryCanonicalBytes.writeText(out,column.name)
              MySqlRecoveryCanonicalBytes.writeText(out,column.sqlDeclaration)
            else if historical && pairs.contains(column.name) && row(index)!=MySqlFormat3Cell.SqlNull then
              val bytes=binary(row,shape,column.name)
              val hash=binary(row,shape,pairs(column.name))
              out.writeByte(7)
              MySqlRecoveryCanonicalBytes.writeToken(out,Derived)
              MySqlRecoveryCanonicalBytes.writeToken(out,"HISTORICAL")
              MySqlRecoveryCanonicalBytes.writeText(out,column.name)
              MySqlRecoveryCanonicalBytes.writeText(out,column.sqlDeclaration)
              out.writeLong(bytes.length.toLong)
              hash.writeTo(out)
            else MySqlFormat3SnapshotCanonical.writeCell(row(index),out)
          }
        }
      }
    }

  private def binary(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape, name: String): MySqlFormat3Binary =
    row(shape.columns.indexWhere(_.name==name)) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => throw new SQLDataException("DB-first projection requires binary "+shape.table+"."+name)

  def fingerprint(request: MySqlDbFirstInitialPreparationData, scope: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes = encode(Fingerprint,root) { out =>
    payload(out,request.backing.canonical)
    Vector(scope,before,target).foreach(payload(out,_))
  }
  def context(request: MySqlDbFirstInitialPreparationData, facts: MySqlDbFirstInitialPreparationFacts,
      fingerprint: MySqlRecoveryCanonicalBytes, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    val context=operationContext(request,facts,fingerprint,root)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(active) {
      MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1",root.profile) { out =>
        MySqlModeOperationEffectCodec.writeOperationContext(out,context)
      }
    }
  def operationContext(request: MySqlDbFirstInitialPreparationData, facts: MySqlDbFirstInitialPreparationFacts,
      fingerprint: MySqlRecoveryCanonicalBytes, root: MySqlFormat3ReadBudget): MySqlOperationContext =
    val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(active.sharesAllocationLedger(root) && ((active eq root) || active.isCanonicalChildOf(root)),
      "DB-first generic context requires original allocation family")
    active.reserveRetainedMemory(2048L,"DB-first generic context/effect/fingerprint construction")
    MySqlOperationContext(request.tag.bootId,facts.writerEpoch,request.request.expectedBinding.bindingUuid,request.tag.operationId,
      Vector(MySqlExpectedEffect("DB_FIRST_INITIAL_PREPARATION:"+request.request.podUuid.toString,0L,fingerprint.sha256)))
  def witness(request: MySqlDbFirstInitialPreparationData, scope: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes, context: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes = encode(Witness,root) { out =>
    payload(out,request.backing.canonical)
    // 完整原字段仍在同一O中；见证绑定角色、规范域、长度和摘要，避免重复嵌套完整投影。
    // 验证必须从原inventory重算，不能信任O自报摘要；前像和目标的角色不可互换。
    def commitment(role: String, bytes: MySqlRecoveryCanonicalBytes): Unit =
      MySqlRecoveryCanonicalBytes.writeToken(out,role)
      MySqlRecoveryCanonicalBytes.writeToken(out,bytes.encoding)
      out.writeInt(bytes.length)
      bytes.sha256.foreach(b => out.writeByte(b.toInt))
    commitment("SCOPE",scope)
    commitment("PREIMAGE",before)
    commitment("TARGET",target)
    commitment("FINGERPRINT",fingerprint)
    commitment("CONTEXT",context)
  }
