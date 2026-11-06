package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import java.io.DataOutputStream
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** schema6的不可变DATA事实；连接数字与barrier UUID本身不证明held/fence或执行许可。 */
private[mysql] final case class MySqlDbFirstRecallFacts(
    connectionId: Long, barrierGenerationUuid: UUID, writerEpoch: Long,
    leaseUntil: LocalDateTime, locatorRevision: Long, heldReservationRevision: Long, releasedReservationRevision: Long,
    plannedAt: LocalDateTime, resultAt: LocalDateTime)

/** 单一原O的闭合27表语义投影；实际image仍保存全部历史RAW和NBT。 */
private[mysql] object MySqlDbFirstRecallCanonical:
  val ScopeV1 = "FP_DB_FIRST_RECALL_SCOPE_BE_V1"
  val Scope = "FP_DB_FIRST_RECALL_SCOPE_BE_V2"
  val Effect = "FP_DB_FIRST_RECALL_EFFECT_BE_V1"
  val Fingerprint = "FP_DB_FIRST_RECALL_FINGERPRINT_BE_V1"
  val Witness = "FP_DB_FIRST_RECALL_WITNESS_BE_V1"
  val Derived = "FP_DB_FIRST_RECALL_DERIVED_V1"
  val JournalScope = "FP_DB_FIRST_RECALL_SCOPE_BE_V3"
  val JournalEffect = "FP_DB_FIRST_RECALL_EFFECT_BE_V2"
  val JournalFingerprint = "FP_DB_FIRST_RECALL_FINGERPRINT_BE_V2"
  val JournalWitness = "FP_DB_FIRST_RECALL_WITNESS_BE_V2"
  val JournalDerived = "FP_DB_FIRST_RECALL_DERIVED_V2"
  val RecoveryScope = "FP_DB_FIRST_RECALL_SCOPE_BE_V4"
  val RecoveryEffect = "FP_DB_FIRST_RECALL_EFFECT_BE_V3"
  val RecoveryFingerprint = "FP_DB_FIRST_RECALL_FINGERPRINT_BE_V3"
  val RecoveryWitness = "FP_DB_FIRST_RECALL_WITNESS_BE_V3"
  val RecoveryDerived = "FP_DB_FIRST_RECALL_DERIVED_V3"
  private val CheckpointScope = "FP_DB_FIRST_ACTIVE_CHECKPOINT_SCOPE_BE_V2"
  private val CheckpointEffect = "FP_DB_FIRST_ACTIVE_CHECKPOINT_EFFECT_BE_V1"
  private val CheckpointFingerprint = "FP_DB_FIRST_ACTIVE_CHECKPOINT_FINGERPRINT_BE_V1"
  private val CheckpointWitness = "FP_DB_FIRST_ACTIVE_CHECKPOINT_WITNESS_BE_V1"
  private val CheckpointDerived = "FP_DB_FIRST_ACTIVE_CHECKPOINT_DERIVED_V1"
  /** 旧recall字节域保持原值；checkpoint不得借用recall的见证或清理语义。 */
  def domain(request: MySqlDbFirstSnapshotMutation, recallDomain: String): String =
    request.purpose match
      case DbFirstPodSnapshot.Purpose.RECALL =>
        request match
          case r:MySqlDbFirstRecallRequest if r.journalLink.exists(_.recoveredOriginal.nonEmpty)=>recallDomain match
            case Scope=>RecoveryScope
            case Effect=>RecoveryEffect
            case Fingerprint=>RecoveryFingerprint
            case Witness=>RecoveryWitness
            case Derived=>RecoveryDerived
            case _=>throw new IllegalArgumentException("unsupported recovered recall canonical domain")
          case r:MySqlDbFirstRecallRequest if r.journalLink.nonEmpty=>recallDomain match
            case Scope=>JournalScope
            case Effect=>JournalEffect
            case Fingerprint=>JournalFingerprint
            case Witness=>JournalWitness
            case Derived=>JournalDerived
            case _=>throw new IllegalArgumentException("unsupported journal-bound recall canonical domain")
          case _=>recallDomain
      case DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT =>
        // 投影循环只借用固定常量，不能为每个历史cell重复分配替换后的domain字符串。
        recallDomain match
          case Scope => CheckpointScope
          case Effect => CheckpointEffect
          case Fingerprint => CheckpointFingerprint
          case Witness => CheckpointWitness
          case Derived => CheckpointDerived
          case _ => throw new IllegalArgumentException("unsupported checkpoint canonical domain")
      case _ => throw new IllegalArgumentException("unsupported snapshot canonical purpose")
  def step(request: MySqlDbFirstSnapshotMutation): String = request.purpose match
    case DbFirstPodSnapshot.Purpose.RECALL => MySqlDbFirstSchemaDefinition.RecallStep
    case DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT => MySqlDbFirstSchemaDefinition.CheckpointStep
    case _ => throw new IllegalArgumentException("unsupported snapshot step")
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

  def scope(request: MySqlDbFirstSnapshotMutation, facts: MySqlDbFirstRecallFacts,
      before: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    encode(domain(request,Scope),root) { out =>
      val t=request.tag
      MySqlRecoveryCanonicalBytes.writeUuid(out,t.bootId)
      out.writeLong(t.bootGeneration);out.writeLong(t.connectionGeneration)
      Vector(t.operationId,request.bindingUuid,request.podUuid,request.attemptUuid,facts.barrierGenerationUuid)
        .foreach(MySqlRecoveryCanonicalBytes.writeUuid(out,_))
      out.writeLong(facts.connectionId);out.writeLong(facts.writerEpoch)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.leaseUntil),out)
      out.writeLong(request.capturedStateVersion);out.writeLong(facts.locatorRevision);out.writeLong(facts.heldReservationRevision);out.writeLong(facts.releasedReservationRevision)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.plannedAt),out)
      MySqlFormat3SnapshotCanonical.writeCell(MySqlFormat3Cell.DateTime(facts.resultAt),out)
      // 原before完整canonical同时绑定connection、manifest、旧W链与全部历史RAW。
      vector(out,before.canonicalSha256)
      vector(out,request.identity.sha256)
      vector(out,request.profileSha256)
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

  def projection(request: MySqlDbFirstSnapshotMutation, before: MySqlScriptSchemaSnapshot,
      relations: Vector[MySqlFormat3RelationSnapshot], target: Boolean,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    val inputs=before.inputs
    require(relations.size==inputs.canonicalOrder.size,"DB-first projection must include all27 tables")
    var tableIndex=0
    while tableIndex<relations.size do
      require(relations(tableIndex).table==inputs.canonicalOrder(tableIndex).table,"DB-first projection closed table order")
      tableIndex+=1
    encode(domain(request,Effect),root) { out =>
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
          val own=shape.table==Operation && binary(row,shape,"binding_uuid").equalsUuid(request.bindingUuid) &&
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
              MySqlRecoveryCanonicalBytes.writeToken(out,domain(request,Derived))
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
              MySqlRecoveryCanonicalBytes.writeToken(out,domain(request,Derived))
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

  def fingerprint(request: MySqlDbFirstSnapshotMutation, scope: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes = encode(domain(request,Fingerprint),root) { out =>
    vector(out,request.identity.canonicalBytes)
    Vector(scope,before,target).foreach(payload(out,_))
  }
  def context(request: MySqlDbFirstSnapshotMutation, facts: MySqlDbFirstRecallFacts,
      fingerprint: MySqlRecoveryCanonicalBytes, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    val context=operationContext(request,facts,fingerprint,root)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(active) {
      MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1",root.profile) { out =>
        MySqlModeOperationEffectCodec.writeOperationContext(out,context)
      }
    }
  def operationContext(request: MySqlDbFirstSnapshotMutation, facts: MySqlDbFirstRecallFacts,
      fingerprint: MySqlRecoveryCanonicalBytes, root: MySqlFormat3ReadBudget): MySqlOperationContext =
    val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(active.sharesAllocationLedger(root) && ((active eq root) || active.isCanonicalChildOf(root)),
      "DB-first generic context requires original allocation family")
    active.reserveRetainedMemory(2048L,"DB-first generic context/effect/fingerprint construction")
    MySqlOperationContext(request.tag.bootId,facts.writerEpoch,request.bindingUuid,request.tag.operationId,
      Vector(MySqlExpectedEffect(step(request)+":"+request.podUuid.toString,request.capturedStateVersion,fingerprint.sha256)))
  def witness(request: MySqlDbFirstSnapshotMutation, scope: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      fingerprint: MySqlRecoveryCanonicalBytes, context: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes = encode(domain(request,Witness),root) { out =>
    vector(out,request.identity.canonicalBytes)
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
