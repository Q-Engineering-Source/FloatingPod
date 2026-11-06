package com.wi.floatingpod.storage.mysql

import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID
import MySqlModeOperationEffectCodec.{allocationBudget,schema,connection,observationScope,
  maxAllowedPacketBytes,limitProfile,relations,relation,rowCount,shapes}

/** 完整 full20 镜像；仅本操作的有限派生列使用 marker，历史行逐值保留。 */
private[mysql] object MySqlNormalRecallOperationEffectCodec:
  private val Operation = "fp_recovery_operation"
  private val Writer = "fp_runtime_writer"
  private val Completion = "fp_lifecycle_completion"
  private val Context = Set("original_context_bytes", "original_context_sha256")
  private val Target = Set("target_bytes", "target_sha256")
  private val Preimage = Set("preimage_bytes", "preimage_sha256")
  private val Witness = Set("commit_witness_bytes", "commit_witness_sha256")

  def projection(snapshot: MySqlModeOperationEffectCodec.Image, header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      receipt: Option[UUID], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(snapshot != null && header != null && phase != null && image != null && receipt != null &&
      receipt.forall(MySqlNormalRecallRequest.nonZero) && root != null && !root.isClosed &&
      (root.profile eq snapshot.limitProfile) && (root.profile eq header.context.exactLimitProfile) &&
      snapshot.allocationBudget.sharesAllocationLedger(root), "original full20 recall projection required")
    requireFamily(snapshot,header.context.schemaFormat,root)
    val context = header.context
    require(snapshot.schema == context.schema && snapshot.connection.serverUuid == context.serverUuid &&
      snapshot.connection.serverVersion == context.serverVersion && snapshot.connection.effectiveUser == context.effectiveUser &&
      snapshot.observationScope.bootUuid == context.tag.bootId &&
      snapshot.observationScope.bootGeneration == context.tag.bootGeneration &&
      snapshot.observationScope.connectionGeneration == context.tag.connectionGeneration,
      "recall image belongs to another original connection/boot")
    if snapshot.rowCount > root.profile.maxMembers() then throw new SQLDataException("recall full20 exceeds member budget")
    encodeProjection(snapshot, snapshot.relations, header, phase, image, receipt, root)

  /** 仅编码私有建构 DATA；base 保有原身份，relations 从不取得 snapshot/SQL 资格。 */
  private[mysql] def constructionProjection(base: MySqlModeOperationEffectCodec.Image,
      completeRelations: Vector[MySqlFormat3RelationSnapshot], header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      receipt: Option[UUID], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireConstructionRelations(base, completeRelations, root)
    require(header != null && phase != null && image != null && receipt != null &&
      receipt.forall(MySqlNormalRecallRequest.nonZero) && (root.profile eq header.context.exactLimitProfile),
      "exact original recall construction projection required")
    requireFamily(base, header.context.schemaFormat, root)
    val context = header.context
    require(base.schema == context.schema && base.connection.serverUuid == context.serverUuid &&
      base.connection.serverVersion == context.serverVersion && base.connection.effectiveUser == context.effectiveUser &&
      base.observationScope.bootUuid == context.tag.bootId &&
      base.observationScope.bootGeneration == context.tag.bootGeneration &&
      base.observationScope.connectionGeneration == context.tag.connectionGeneration,
      "recall construction base belongs to another original connection/boot")
    encodeProjection(base, completeRelations, header, phase, image, receipt, root)

  /** 只有完整结构与原 base 身份的验证；不解码未完成的 context、不发行图像。 */
  private[mysql] def requireConstructionRelations(base: MySqlModeOperationEffectCodec.Image,
      completeRelations: Vector[MySqlFormat3RelationSnapshot], root: MySqlFormat3ReadBudget): Unit =
    require(base != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      !base.allocationBudget.isClosed && base.allocationBudget.sharesAllocationLedger(root) &&
      (base.limitProfile eq root.profile) && completeRelations != null,
      "original open base/root and complete construction relations required")
    val structures = completeRelations.foldLeft(4096L) { (bytes, relation) =>
      require(relation != null && relation.rows != null, "complete construction relation rows required")
      val width = base.shapes.find(_.table == relation.table).getOrElse(
        throw new SQLDataException("recall construction relation is outside the original closed shape")).columns.size.toLong
      Math.addExact(bytes, Math.multiplyExact(Math.addExact(relation.rows.size.toLong, 1L),
        Math.addExact(1024L, Math.multiplyExact(width, 256L))))
    }
    root.reserveTemporaryMemory(structures, "recall construction structural validation")
    var primary: Throwable = null
    try
      base match
        case _: MySqlFormat3FullSnapshot =>
          requireFamily(base, 3, root)
          MySqlFormat3SnapshotValidation.validateRelations(completeRelations)
        case deployment: MySqlScriptSchemaSnapshot =>
          requireFamily(base, 5, root)
          MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(completeRelations)
          val count = completeRelations.foldLeft(0L)((n, relation) => Math.addExact(n, relation.rows.size.toLong))
          require(count <= deployment.inputs.budget.rows, "recall construction exceeds original Full26 row budget")
      require(completeRelations.foldLeft(0L)((n, relation) => Math.addExact(n, relation.rows.size.toLong)) <=
        root.profile.maxMembers(), "recall construction exceeds original member budget")
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(structures, "recall construction structural validation release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def encodeProjection(base: MySqlModeOperationEffectCodec.Image,
      completeRelations: Vector[MySqlFormat3RelationSnapshot], header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      receipt: Option[UUID], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    val context = header.context
    MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_EFFECT_BE_V2", root) { output =>
      token(output, phaseName(phase)); token(output, imageName(image))
      uuid(output, context.bindingUuid); uuid(output, context.workflowUuid)
      uuid(output, context.reserveOperationUuid); uuid(output, context.tag.operationId)
      uuid(output, header.request.podUuid); uuid(output, header.request.attemptUuid)
      uuid(output, header.request.footprintUuid)
      output.writeInt(base.shapes.size)
      completeRelations.foreach { relation =>
        val shape = base.shapes.find(_.table==relation.table).getOrElse(
          throw new SQLDataException("recall complete relation has no original closed shape"))
        text(output, shape.table); output.writeInt(shape.columns.size)
        shape.columns.foreach { column => text(output, column.name); text(output, column.sqlDeclaration) }
        output.writeInt(relation.rows.size)
        relation.rows.foreach { row =>
          def column(name: String): MySqlFormat3Cell = row(shape.columns.indexWhere(_.name == name))
          val historical = historicalRow(relation.table, row, header, phase)
          if historical then MySqlNormalRecallHistoricalPayload.requireValid(row, root, inputsForImage(base, root))
          val exactCompletion = relation.table == Completion && receipt.exists { id =>
            MySqlModeOperationEffectCodec.uuidEquals(column("binding_uuid"), context.bindingUuid) &&
              MySqlModeOperationEffectCodec.uuidEquals(column("receipt_uuid"), id)
          }
          shape.columns.zipWithIndex.foreach { (field, index) =>
            if exactCompletion && phase == MySqlOperationEffectPhase.Execute &&
              image == MySqlOperationEffectImage.Target && Context.contains(field.name) then
              MySqlNormalRecallCanonical.writeDerivedCell(output, phase, image, Completion,
                Vector(column("binding_uuid"), column("receipt_uuid")), field.name, field.sqlDeclaration,
                row(index), context.bindingUuid, receipt.get, context.tag.operationId)
            else if operationMarker(relation.table, row, shape, field.name, header, phase, image) then
              require(row(index) != MySqlFormat3Cell.SqlNull, "recall derived operation fields cannot be SQL NULL")
              token(output, "FP_NORMAL_RECALL_OPERATION_DERIVED_V1")
              token(output, phaseName(phase)); token(output, imageName(image)); text(output, shape.table)
              output.writeInt(shape.primaryKey.size)
              shape.primaryKey.foreach { key => text(output, key); MySqlNormalRecallCanonical.cell(output, column(key)) }
              text(output, field.name); text(output, field.sqlDeclaration)
            else if !historical || !MySqlNormalRecallHistoricalPayload.writeDescriptor(output, row, field.name) then
              MySqlNormalRecallCanonical.cell(output, row(index))
          }
        }
      }
    }

  def fingerprint(header: MySqlNormalRecallHeader, phase: MySqlOperationEffectPhase,
      request: MySqlRecoveryCanonicalBytes, scope: MySqlRecoveryCanonicalBytes,
      before: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(header != null && phase != null && request != null && scope != null && before != null && target != null,
      "complete recall fingerprint inputs required")
    MySqlNormalRecallCanonical.encode("FP_NORMAL_RECALL_FINGERPRINT_BE_V1", root) { output =>
      token(output, phaseName(phase)); uuid(output, header.context.workflowUuid)
      uuid(output, header.context.reserveOperationUuid); uuid(output, header.context.tag.operationId)
      framed(output, request); framed(output, scope); framed(output, before); framed(output, target)
    }

  def context(header: MySqlNormalRecallHeader, operation: UUID,
      fingerprint: MySqlRecoveryCanonicalBytes): MySqlOperationContext =
    require(header != null && fingerprint != null &&
      (operation == header.context.tag.operationId || operation == header.context.reserveOperationUuid),
      "exact recall reserve/execute identity required")
    MySqlOperationContext(header.context.tag.bootId, header.context.writerEpoch, header.context.bindingUuid,
      operation, Vector(MySqlExpectedEffect("NORMAL_RECALL/" + header.context.bindingUuid + "/" + header.request.podUuid,
        header.request.criticalBase.stateVersion, fingerprint.sha256)))

  /** 原实际读回必须完整逐值相等；checksum只是额外身份，不替代这一比较。 */
  def requireExactReadback(expected: MySqlModeOperationEffectCodec.Image, actual: MySqlModeOperationEffectCodec.Image,
      root: MySqlFormat3ReadBudget): Unit =
    if actual != null then actual.relation(Operation).rows.foreach { row =>
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile,
        MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), inputsForImage(actual, root))
    }
    if expected == null || actual == null || expected.schema != actual.schema ||
      expected.connection != actual.connection || expected.observationScope != actual.observationScope ||
      expected.maxAllowedPacketBytes != actual.maxAllowedPacketBytes ||
      (expected.limitProfile ne actual.limitProfile) || expected.relations != actual.relations then
      throw new SQLDataException("normal recall full20 readback differs from original complete target")
    (expected,actual) match
      case (_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot)=>requireFamily(actual,3,root)
      case (a:MySqlScriptSchemaSnapshot,b:MySqlScriptSchemaSnapshot)=>
        if a.inputs ne b.inputs then throw new SQLDataException("Full26 recall readback switched exact input owner")
        requireFamily(a,5,root);requireFamily(b,5,root)
      case _=>throw new SQLDataException("recall readback crossed complete-image families")

  private def requireFamily(snapshot:MySqlModeOperationEffectCodec.Image,format:Int,
      root:MySqlFormat3ReadBudget):Unit=snapshot match
    case _:MySqlFormat3FullSnapshot=>
      require(format==3,"legacy recall projection requires schema3 context")
      root.requireFormat3Full20Layout()
    case deployment:MySqlScriptSchemaSnapshot=>
      require(format==5,"Full26 recall projection requires schema5 context")
      MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)

  private[mysql] def historicalRow(table: String, row: Vector[MySqlFormat3Cell], header: MySqlNormalRecallHeader,
      phase: MySqlOperationEffectPhase): Boolean =
    if table != Operation then false
    else
      val state = MySqlModeOperationEffectCodec.value(row, Operation, "operation_state")
      val terminal = state == MySqlFormat3Cell.Text("COMMITTED") || state == MySqlFormat3Cell.Text("CANCELLED")
      val sameBinding = MySqlModeOperationEffectCodec.uuidEquals(
        MySqlModeOperationEffectCodec.value(row, Operation, "binding_uuid"), header.context.bindingUuid)
      val operation = MySqlModeOperationEffectCodec.value(row, Operation, "operation_uuid")
      val current = sameBinding && (MySqlModeOperationEffectCodec.uuidEquals(operation, header.context.tag.operationId) ||
        (phase == MySqlOperationEffectPhase.Reserve && MySqlModeOperationEffectCodec.uuidEquals(operation, header.context.reserveOperationUuid)))
      terminal && !current

  private def operationMarker(table: String, row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String, header: MySqlNormalRecallHeader, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage): Boolean =
    if table == Writer then image == MySqlOperationEffectImage.Target && column == "last_commit_fingerprint"
    else if table != Operation then false
    else
      val binding = row(shape.columns.indexWhere(_.name == "binding_uuid"))
      if !MySqlModeOperationEffectCodec.uuidEquals(binding, header.context.bindingUuid) then false
      else
        val operation = row(shape.columns.indexWhere(_.name == "operation_uuid"))
        val reserve = MySqlModeOperationEffectCodec.uuidEquals(operation, header.context.reserveOperationUuid)
        val execute = MySqlModeOperationEffectCodec.uuidEquals(operation, header.context.tag.operationId)
        phase match
          case MySqlOperationEffectPhase.Reserve if image == MySqlOperationEffectImage.Target =>
            (reserve && (Context ++ Target ++ Witness + "operation_fingerprint").contains(column)) ||
              (execute && (Context ++ Target ++ Preimage + "operation_fingerprint").contains(column))
          case MySqlOperationEffectPhase.Execute =>
            (execute && (Context ++ Target ++ Preimage + "operation_fingerprint").contains(column)) ||
              (execute && image == MySqlOperationEffectImage.Target && Witness.contains(column))
          case _ => false

  private def phaseName(value: MySqlOperationEffectPhase): String =
    if value == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE"
  private def imageName(value: MySqlOperationEffectImage): String =
    if value == MySqlOperationEffectImage.Before then "BEFORE" else "TARGET"
  private def framed(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes): Unit =
    output.writeInt(value.length); value.writeTo(output)
  private def token(output: DataOutputStream, value: String): Unit = MySqlRecoveryCanonicalBytes.writeToken(output, value)
  private def text(output: DataOutputStream, value: String): Unit = MySqlRecoveryCanonicalBytes.writeText(output, value)
  private def uuid(output: DataOutputStream, value: UUID): Unit = MySqlRecoveryCanonicalBytes.writeUuid(output, value)

/** 冻结8D6E：仅编码已核完整历史O的七个payload；此表示从不替代原RAW/full20比较。 */
private[mysql] object MySqlNormalRecallHistoricalPayload:
  private val Table = "fp_recovery_operation"
  val Domain = "FP_NORMAL_RECALL_HISTORICAL_PAYLOAD_V1"
  private def digestColumn(column: String): Option[String] = column match
    case "request_identity_bytes" => Some("request_identity_sha256")
    case "original_context_bytes" => Some("original_context_sha256")
    case "scope_bytes" => Some("scope_sha256")
    case "preimage_bytes" => Some("preimage_sha256")
    case "target_bytes" => Some("target_sha256")
    case "commit_witness_bytes" => Some("commit_witness_sha256")
    case "original_diagnostic_identity" => Some("original_diagnostic_sha256")
    case _ => None
  def requireValid(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget, "original open recall root required")
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(!budget.isClosed && budget.sharesAllocationLedger(root) &&
      ((budget eq root) || budget.isCanonicalChildOf(root)), "historical validation cannot switch lexical ledger")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, budget, deploymentInputs)
    val state = MySqlModeOperationEffectCodec.value(row, Table, "operation_state")
    if state != MySqlFormat3Cell.Text("COMMITTED") && state != MySqlFormat3Cell.Text("CANCELLED") then
      throw new SQLDataException("recall historical descriptor requires a terminal original row")

  /** 调用方须先 requireValid 原完整行；false 表示必须保留原普通单元编码。 */
  def writeDescriptor(output: DataOutputStream, row: Vector[MySqlFormat3Cell], column: String): Boolean =
    digestColumn(column) match
      case None => false
      case Some(digestName) =>
        val payload = MySqlModeOperationEffectCodec.value(row, Table, column)
        val digest = MySqlModeOperationEffectCodec.value(row, Table, digestName)
        (payload, digest) match
          case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => false
          case (MySqlFormat3Cell.Binary(bytes), MySqlFormat3Cell.Binary(hash)) if hash.length == 32 =>
            val field = MySqlFormat3RelationShapes.forTable(Table).columns.find(_.name == column).get
            output.writeByte(7)
            MySqlRecoveryCanonicalBytes.writeToken(output, Domain)
            MySqlRecoveryCanonicalBytes.writeText(output, column)
            MySqlRecoveryCanonicalBytes.writeText(output, field.sqlDeclaration)
            output.writeLong(bytes.length.toLong); hash.writeTo(output)
            true
          case _ => throw new SQLDataException("recall historical payload/digest NULL or type mismatch")

  /** 已知冻结schema的严格解码/重编码比较；不按外部长度分配，不容尾随字段。 */
  def requireExactDescriptor(encoded: Array[Byte], row: Vector[MySqlFormat3Cell], column: String,
      root: MySqlFormat3ReadBudget): Unit =
    require(encoded != null && column != null, "exact descriptor inputs required")
    requireValid(row, root)
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    budget.reserveTemporaryMemory(1024L, "recall descriptor strict streaming comparison")
    var primary: Throwable = null
    try
      var offset = 0
      val sink = new java.io.OutputStream:
        override def write(value: Int): Unit =
          if offset >= encoded.length || (encoded(offset) & 255) != (value & 255) then
            throw new SQLDataException("recall historical descriptor differs or is truncated")
          offset += 1
      val output = new DataOutputStream(sink)
      if !writeDescriptor(output, row, column) then throw new SQLDataException("column has no non-NULL historical descriptor")
      output.flush()
      if offset != encoded.length then throw new SQLDataException("recall historical descriptor has trailing bytes")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try budget.releaseTemporaryMemory(1024L, "recall descriptor strict streaming comparison")
      catch case cleanup: Throwable => if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
