package com.wi.floatingpod.storage.mysql

import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID

/** DATA字节构造，无owner/admission/commit发行；运行时只能在原规划root的allocation scope中调用。 */
private[mysql] object MySqlControlledDebugCanonical:
  private val Domain="com.wi.floatingpod.mysql-controlled-debug-effect/v2"
  private val Encoding="FP_MYSQL_CONTROLLED_DEBUG_EFFECT_BE_V2"
  private val ContextColumns=Set("original_context_bytes","original_context_sha256")
  private val PreimageColumns=Set("preimage_bytes","preimage_sha256")
  private val TargetColumns=Set("target_bytes","target_sha256")
  private val WitnessColumns=Set("commit_witness_bytes","commit_witness_sha256")
  private val CyclicWriterColumns=Set("last_commit_fingerprint")

  def projection(snapshot: MySqlFormat3FullSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,
      activation: Boolean,phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,
      root: MySqlFormat3ReadBudget,sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): MySqlRecoveryCanonicalBytes =
    if snapshot == null || request == null || plan == null || context == null || phase == null || image == null ||
      context.schemaFormat!=3 || context.physicalFormat!=2 ||
      root == null || root.isClosed || root.isScopedReadBudget || (root.profile ne snapshot.limitProfile) ||
      !root.sharesAllocationLedger(snapshot.allocationBudget) || context.bindingUuid != request.expectedBinding.bindingUuid ||
      context.workflowUuid != plan.workflowUuid || context.reserveOperationUuid != plan.reserveOperationUuid ||
      (context.exactLimitProfile ne root.profile) || context.schema != snapshot.schema ||
      context.serverUuid != snapshot.connection.serverUuid || context.serverVersion != snapshot.connection.serverVersion ||
      context.effectiveUser != snapshot.connection.effectiveUser || context.tag.bootId != snapshot.observationScope.bootUuid ||
      context.tag.bootGeneration != snapshot.observationScope.bootGeneration ||
      context.tag.connectionGeneration != snapshot.observationScope.connectionGeneration then
      fail("complete original full20 projection inputs required")
    // Runtime actual PlanningReadOwner/Session/source membership checks precede this DATA call.
    // Complete graph/FK/raw validation remains unfiltered; only the canonical writer selects rows.
    MySqlControlledDebugRelations.requireControlledClosure(snapshot,request,plan,root,sourceRoot,continuation)
    withOriginalAllocation(root) {
      val length=measure(root) { output => writeProjection(output,snapshot,request,plan,context,activation,phase,image,root) }
      MySqlRecoveryCanonicalBytes.encodeExact(Encoding,snapshot.limitProfile,length) { output =>
        writeProjection(output,snapshot,request,plan,context,activation,phase,image,root)
      }
    }

  def projectionDeploymentReceipt(snapshot: MySqlScriptSchemaSnapshot,inputs: MySqlScriptSchemaInputs,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,activation: Boolean,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    projectionDeploymentReceipt(snapshot,inputs,request,plan,context,activation,phase,image,root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private[mysql] def projectionDeploymentReceipt(snapshot: MySqlScriptSchemaSnapshot,inputs: MySqlScriptSchemaInputs,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,activation: Boolean,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    if request==null || plan==null || context==null || phase==null || image==null ||
      context.schemaFormat!=5 || context.physicalFormat!=2 ||
      !context.ddlSha256.equalsIgnoreCase(inputs.ddlSha256) || (plan.exactLimitProfile ne root.profile) ||
      context.bindingUuid!=request.expectedBinding.bindingUuid || context.workflowUuid!=plan.workflowUuid ||
      context.reserveOperationUuid!=plan.reserveOperationUuid || (context.exactLimitProfile ne root.profile) ||
      context.schema!=snapshot.schema || context.serverUuid!=snapshot.connection.serverUuid ||
      context.serverVersion!=snapshot.connection.serverVersion || context.effectiveUser!=snapshot.connection.effectiveUser ||
      context.tag.bootId!=snapshot.observationScope.bootUuid ||
      context.tag.bootGeneration!=snapshot.observationScope.bootGeneration ||
      context.tag.connectionGeneration!=snapshot.observationScope.connectionGeneration then
      fail("complete original Full26 projection/context inputs required")
    requireDataAllocation(root,allocation)
    MySqlControlledDebugRelations.requireControlledDeploymentReceiptClosure(snapshot,request,plan,root)
    def write(output:DataOutputStream,scratchBudget:MySqlFormat3ReadBudget):Unit =
      writeProjectionRows(output,snapshot.schema,snapshot.connection,snapshot.observationScope,
        snapshot.maxAllowedPacketBytes,snapshot.inputs.sourceLimitProfile,snapshot.relations,Some(inputs),
        request,plan,context,activation,phase,image,scratchBudget)
    val length = if allocation eq root then measure(root)(output=>write(output,root)) else
      var measured=0L
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val comparison=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        measured=measure(comparison)(output=>write(output,comparison))
      }
      measured
    MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
      MySqlRecoveryCanonicalBytes.encodeExact(Encoding,snapshot.inputs.sourceLimitProfile,length)(output=>write(output,allocation))
    }

  def scope(request: MySqlControlledDebugPrepareRequest,context: MySqlRepositoryOperationContextIdentity,
      activation: Boolean,key: MySqlRecoveryResourceKey,root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    scope(request,context,activation,key,root,MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private[mysql] def scope(request: MySqlControlledDebugPrepareRequest,context: MySqlRepositoryOperationContextIdentity,
      activation: Boolean,key: MySqlRecoveryResourceKey,root: MySqlFormat3ReadBudget,
      allocation: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || context==null || root==null || root.isClosed || key==null ||
      (context.exactLimitProfile ne root.profile) || (key.exactProfile ne root.profile) ||
      key.resourceKind!="POD" || !key.podUuid.contains(request.podUuid) then fail("original controlled scope resource")
    encode(root,allocation,"FP_MYSQL_CONTROLLED_DEBUG_SCOPE_BE_V1") { (output,scratchBudget) =>
      text(output,"com.wi.floatingpod.mysql-controlled-debug-scope/v1");output.writeByte(1)
      output.writeInt(countScope(context,scratchBudget).toInt);context.writeScopeIdentityFields(output)
      text(output,if activation then "ACTIVATE" else "REARM")
      output.writeInt(key.canonicalLength);key.writeCanonicalTo(output)
      uuid(output,request.attemptUuid);uuid(output,request.footprintUuid);uuid(output,request.createOperationUuid)
      output.writeBoolean(!activation)
      writeCell(output,if activation then MySqlFormat3Cell.SignedInteger(0L) else MySqlFormat3Cell.SqlNull)
      writeCell(output,if activation then MySqlFormat3Cell.SignedInteger(0L) else MySqlFormat3Cell.SqlNull)
      writeCell(output,MySqlFormat3Cell.SqlNull)
    }
  def fingerprint(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,phase: MySqlOperationEffectPhase,
      scope: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    fingerprint(request,plan,context,phase,scope,before,target,root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private[mysql] def fingerprint(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,phase: MySqlOperationEffectPhase,
      scope: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || plan==null || context==null || phase==null || scope==null || before==null || target==null ||
      root==null || root.isClosed || (context.exactLimitProfile ne root.profile) then fail("complete fingerprint inputs")
    encode(root,allocation,"FP_MYSQL_CONTROLLED_DEBUG_FINGERPRINT_BE_V2") { (output,scratchBudget) =>
      text(output,"com.wi.floatingpod.mysql-controlled-debug-fingerprint/v2");output.writeByte(2)
      text(output,phaseName(phase));framed(output,plan.requestIdentity.canonical)
      output.writeInt(countScope(context,scratchBudget).toInt);context.writeScopeIdentityFields(output)
      identities(output,request,plan)
      framed(output,scope);framed(output,before);framed(output,target)
    }
  def witness(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,plannedAt: java.time.LocalDateTime,
      scope: MySqlRecoveryCanonicalBytes,fingerprint: MySqlRecoveryCanonicalBytes,
      standardContext: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,
      target: MySqlRecoveryCanonicalBytes,reserveRaw: Option[MySqlRecoveryOperationRawEnvelope],
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    witness(request,plan,phase,plannedAt,scope,fingerprint,standardContext,before,target,reserveRaw,root,
      MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))

  private[mysql] def witness(request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,plannedAt: java.time.LocalDateTime,
      scope: MySqlRecoveryCanonicalBytes,fingerprint: MySqlRecoveryCanonicalBytes,
      standardContext: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,
      target: MySqlRecoveryCanonicalBytes,reserveRaw: Option[MySqlRecoveryOperationRawEnvelope],
      root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || plan==null || phase==null || plannedAt==null || plannedAt.getNano%1000!=0 ||
      plannedAt.getYear<1000 || plannedAt.getYear>9999 || scope==null || fingerprint==null || standardContext==null ||
      before==null || target==null || reserveRaw==null || reserveRaw.exists(_==null) || root==null || root.isClosed ||
      (phase==MySqlOperationEffectPhase.Reserve && reserveRaw.nonEmpty) ||
      (phase==MySqlOperationEffectPhase.Execute && reserveRaw.isEmpty) then fail("fixed phase witness inputs")
    requireDataAllocation(root,allocation)
    reserveRaw.foreach { raw =>
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw.row,root.profile,allocation,raw.deploymentInputs)
      var exact = false
      if allocation eq root then exact=raw.exactlyMatches(raw.row,root.profile)
      else MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        exact=raw.exactlyMatches(raw.row,root.profile)
      }
      if !exact then fail("original reserve raw framing differs from complete row")
    }
    encode(root,allocation,"FP_MYSQL_CONTROLLED_DEBUG_WITNESS_BE_V2") { (output,_) =>
      text(output,"com.wi.floatingpod.mysql-controlled-debug-witness/v2");output.writeByte(2)
      text(output,phaseName(phase));identities(output,request,plan)
      uuid(output,if phase==MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid)
      writeCell(output,MySqlFormat3Cell.DateTime(plannedAt));framed(output,plan.requestIdentity.canonical)
      framed(output,scope);framed(output,fingerprint);framed(output,standardContext);framed(output,before);framed(output,target)
      output.writeBoolean(reserveRaw.nonEmpty)
      reserveRaw.foreach { raw =>
        if raw.operationUuid!=plan.reserveOperationUuid ||
          !matches(raw.row,"fp_recovery_operation","binding_uuid",request.expectedBinding.bindingUuid) ||
          !matches(raw.row,"fp_recovery_operation","workflow_uuid",plan.workflowUuid) then fail("original reserve witness row identity")
        uuid(output,request.expectedBinding.bindingUuid);uuid(output,plan.reserveOperationUuid);uuid(output,plan.workflowUuid)
        output.writeInt(raw.length)
        val shape=MySqlFormat3RelationShapes.forTable("fp_recovery_operation")
        output.writeInt(shape.columns.size)
        var index=0
        while index<shape.columns.size do
          val name=shape.columns(index).name
          text(output,name)
          if !MySqlControlledDebugHistoricalRow.writeDescriptor(output,raw.row,name) then writeCell(output,raw.row(index))
          index+=1
        output.writeInt(1)
      }
      }
  private def identities(output: DataOutputStream,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan): Unit =
    uuid(output,request.expectedBinding.bindingUuid);uuid(output,request.podUuid);uuid(output,request.attemptUuid)
    uuid(output,request.footprintUuid);uuid(output,request.createOperationUuid)
    uuid(output,plan.workflowUuid);uuid(output,plan.reserveOperationUuid);uuid(output,plan.executeOperationUuid)
  private def phaseName(phase: MySqlOperationEffectPhase): String =
    if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE"
  private def framed(output: DataOutputStream,value: MySqlRecoveryCanonicalBytes): Unit =
    output.writeInt(value.length);value.writeTo(output)
  private def encode(root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget,
      encoding: String)(write: (DataOutputStream,MySqlFormat3ReadBudget)=>Unit): MySqlRecoveryCanonicalBytes =
      requireDataAllocation(root,allocation)
      val length =
        if allocation eq root then measure(root)(output=>write(output,root))
        else
          var measured=0L
          MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
            val comparison=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
            measured=measure(comparison)(output=>write(output,comparison))
          }
          measured
      MySqlRecoveryCanonicalBytes.withAllocationBudget(allocation) {
      MySqlRecoveryCanonicalBytes.encodeExact(encoding,root.profile,length)(output=>write(output,allocation))
      }
  private def requireDataAllocation(root: MySqlFormat3ReadBudget,allocation: MySqlFormat3ReadBudget): Unit =
    if root==null || root.isClosed || root.isScopedReadBudget || allocation==null || allocation.isClosed ||
      (allocation.profile ne root.profile) || !allocation.sharesAllocationLedger(root) ||
      ((allocation ne root) && !allocation.isCanonicalChildOf(root)) then
      fail("controlled Full26 original root and same-ledger DATA allocation required")
  private def withOriginalAllocation[A](root: MySqlFormat3ReadBudget)(body: => A): A =
    if root==null || root.isClosed || root.isScopedReadBudget then fail("exact original open allocation root required")
    // 临时再生成只接受该 root 的实际 lexical child；不新建 ledger、不切换 sibling。
    val actual=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    if actual.isClosed || (actual.profile ne root.profile) ||
      ((actual ne root) && !actual.isCanonicalChildOf(root)) then fail("foreign canonical allocation scope")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(actual) { body }
  private def measure(budget: MySqlFormat3ReadBudget)(write: DataOutputStream=>Unit): Long =
    budget.reserveTemporaryMemory(512L,"controlled projection counting stream")
    var counter: MySqlFormat3CountingOutputStream=null
    var output: DataOutputStream=null
    var primary: Throwable=null
    try
      counter=new MySqlFormat3CountingOutputStream
      output=new DataOutputStream(counter)
      write(output);output.flush()
      counter.byteLength
    catch
      case cause: Throwable => primary=cause;throw cause
    finally
      if output != null then
        try output.close()
        catch case cleanup: Throwable => if primary==null then primary=cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      output=null;counter=null
      try budget.releaseTemporaryMemory(512L,"controlled projection counting stream")
      catch case cleanup: Throwable => if primary==null then primary=cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      if primary != null then throw primary

  private def writeProjection(output: DataOutputStream,snapshot: MySqlFormat3FullSnapshot,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,activation: Boolean,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,root: MySqlFormat3ReadBudget): Unit =
    writeProjectionRows(output,snapshot.schema,snapshot.connection,snapshot.observationScope,
      snapshot.maxAllowedPacketBytes,snapshot.limitProfile,snapshot.relations,None,
      request,plan,context,activation,phase,image,root)

  private def writeProjectionRows(output: DataOutputStream,schema: String,
      connection: MySqlFormat3ConnectionObservation,observation: MySqlFormat3ObservationScope,packet: Long,
      profile: com.wi.floatingpod.config.SourceLimitProfile,relations: Vector[MySqlFormat3RelationSnapshot],
      deploymentInputs: Option[MySqlScriptSchemaInputs],request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,
      activation: Boolean,phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,
      scratchBudget: MySqlFormat3ReadBudget): Unit =
    text(output,Domain);output.writeByte(2)
    text(output,if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
    text(output,if image==MySqlOperationEffectImage.Before then "BEFORE" else "TARGET")
    output.writeInt(plan.requestIdentity.canonicalLength);plan.requestIdentity.canonical.writeTo(output)
    val contextLength=countScope(context,scratchBudget)
    output.writeInt(contextLength.toInt);context.writeScopeIdentityFields(output)
    text(output,if activation then "ACTIVATE" else "REARM")
    uuid(output,request.expectedBinding.bindingUuid);uuid(output,request.podUuid);uuid(output,request.attemptUuid)
    uuid(output,request.footprintUuid);uuid(output,request.createOperationUuid)
    uuid(output,plan.workflowUuid);uuid(output,plan.reserveOperationUuid);uuid(output,plan.executeOperationUuid)
    output.writeBoolean(!activation)
    writeCell(output,if activation then MySqlFormat3Cell.SignedInteger(0L) else MySqlFormat3Cell.SqlNull)
    writeCell(output,if activation then MySqlFormat3Cell.SignedInteger(0L) else MySqlFormat3Cell.SqlNull)
    text(output,schema)
    asciiSha(output,MySqlFormat3RelationShapes.SourceFoundationSha256)
    asciiSha(output,MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256)
    asciiSha(output,MySqlFormat3RelationShapes.SourceLockedReadSha256)
    uuid(output,connection.serverUuid);text(output,connection.serverVersion);text(output,connection.catalog)
    text(output,connection.effectiveUser);text(output,connection.defaultStorageEngine)
    output.writeInt(connection.innodbFlushLogAtTrxCommit);output.writeLong(connection.syncBinlog)
    text(output,connection.transactionIsolation);text(output,connection.readTransactionIsolation)
    text(output,connection.sqlMode);text(output,connection.timeZone);text(output,connection.tlsVersion);text(output,connection.tlsCipher)
    uuid(output,observation.bootUuid);output.writeLong(observation.bootGeneration)
    output.writeLong(observation.connectionGeneration);output.writeLong(packet)
    writeProfile(output,profile,scratchBudget)
    output.writeInt(relations.size)
    relations.foreach { relation =>
      val shape=deploymentInputs match
        case None=>MySqlFormat3RelationShapes.forTable(relation.table)
        case Some(inputs)=>inputs.forTable(relation.table)
      def includes(row:Vector[MySqlFormat3Cell]):Boolean =
        if deploymentInputs.nonEmpty && !MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==relation.table) then true
        else MySqlControlledDebugSelection.includes(relation.table,row,request)
      text(output,relation.table);text(output,shape.lockClause);output.writeInt(shape.primaryKey.size)
      shape.primaryKey.foreach(text(output,_))
      output.writeInt(shape.columns.size)
      shape.columns.foreach { column =>
        text(output,column.name);text(output,column.sqlDeclaration)
        output.writeByte(column.kind.ordinal);output.writeBoolean(column.nullable)
        column.maximumLength match
          case Some(length) => output.writeBoolean(true);output.writeInt(length)
          case None => output.writeBoolean(false)
      }
      var selectedCount=0
      var rowIndex=0
      while rowIndex<relation.rows.size do
        if includes(relation.rows(rowIndex)) then
          selectedCount=Math.addExact(selectedCount,1)
        rowIndex+=1
      output.writeInt(selectedCount)
      relation.rows.foreach { row =>
        if includes(row) then
          output.writeInt(row.size)
          val historical=relation.table=="fp_recovery_operation" &&
            ((!matches(row,relation.table,"operation_uuid",plan.reserveOperationUuid) &&
              !matches(row,relation.table,"operation_uuid",plan.executeOperationUuid)) ||
              (phase==MySqlOperationEffectPhase.Execute && matches(row,relation.table,"operation_uuid",plan.reserveOperationUuid)))
          var index=0
          while index<row.size do
            val column=shape.columns(index)
            val omission=omissionReason(relation.table,row,column.name,request,plan,phase,image)
            if omission != null then
              output.writeByte(6);text(output,column.name);text(output,omission)
            else if !historical || !MySqlControlledDebugHistoricalRow.writeDescriptor(output,row,column.name) then
              writeCell(output,row(index))
            index+=1
      }
    }

  private def countScope(context: MySqlRepositoryOperationContextIdentity,budget: MySqlFormat3ReadBudget): Long =
    val result=measure(budget)(context.writeScopeIdentityFields)
    MySqlFormat3JvmArrayLimit.requireLength(result,"controlled scope identity frame")
    result
  private def asciiSha(output: DataOutputStream,value: String): Unit =
    if value==null || value.length!=64 then fail("frozen structural source SHA")
    var index=0
    while index<value.length do
      val character=value.charAt(index)
      if !((character>='0' && character<='9') || (character>='A' && character<='F')) then fail("frozen structural source SHA ASCII")
      output.writeByte(character.toInt);index+=1
  private def writeProfile(output: DataOutputStream,profile: com.wi.floatingpod.config.SourceLimitProfile,
      budget: MySqlFormat3ReadBudget): Unit =
    budget.reserveTemporaryMemory(192L,"controlled immutable profile framing copies")
    var bytes: Array[Byte]=null
    var hash: Array[Byte]=null
    var primary: Throwable=null
    try
      bytes=profile.canonicalBytes();hash=profile.sha256()
      if bytes.length!=97 || hash.length!=32 then fail("unique limit-profile framing")
      output.writeInt(bytes.length);output.write(bytes);output.write(hash)
    catch
      case cause: Throwable => primary=cause;throw cause
    finally
      bytes=null;hash=null
      try budget.releaseTemporaryMemory(192L,"controlled immutable profile framing copies")
      catch case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  private def omissionReason(table: String,row: Vector[MySqlFormat3Cell],column: String,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage): String =
    if table=="fp_recovery_operation" then
      val exact=matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) &&
        matches(row,table,"workflow_uuid",plan.workflowUuid) && matches(row,table,"pod_uuid",request.podUuid) &&
        matches(row,table,"attempt_uuid",request.attemptUuid) && matches(row,table,"footprint_uuid",request.footprintUuid)
      val execute=exact && matches(row,table,"operation_uuid",plan.executeOperationUuid)
      val reserve=exact && matches(row,table,"operation_uuid",plan.reserveOperationUuid)
      val common=ContextColumns.contains(column) || TargetColumns.contains(column) || column=="operation_fingerprint"
      val selected=
        if phase==MySqlOperationEffectPhase.Reserve then
          image==MySqlOperationEffectImage.Target &&
            ((reserve && (common || WitnessColumns.contains(column))) ||
             (execute && (common || PreimageColumns.contains(column))))
        else execute && (common || (if image==MySqlOperationEffectImage.Before then PreimageColumns.contains(column) else WitnessColumns.contains(column)))
      if selected then "CURRENT_PLAN_OPERATION_CYCLE_V2" else null
    else if table=="fp_runtime_writer" && image==MySqlOperationEffectImage.Target && CyclicWriterColumns.contains(column) &&
      matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) then "CURRENT_PHASE_WRITER_FINGERPRINT_V2"
    else null
  private def matches(row: Vector[MySqlFormat3Cell],table: String,name: String,id: UUID): Boolean =
    MySqlModeOperationEffectCodec.value(row,table,name) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
      case _ => false
  private def uuid(output: DataOutputStream,id: UUID): Unit = MySqlRecoveryCanonicalBytes.writeUuid(output,id)
  private def text(output: DataOutputStream,value: String): Unit =
    MySqlFormat3Utf8.write(value,output)
  private def writeCell(output: DataOutputStream,cell: MySqlFormat3Cell): Unit = cell match
    case MySqlFormat3Cell.SqlNull => output.writeByte(0)
    case MySqlFormat3Cell.Text(value) => output.writeByte(1);text(output,value)
    case MySqlFormat3Cell.Binary(value) => output.writeByte(2);output.writeInt(value.length);value.writeTo(output)
    case MySqlFormat3Cell.SignedInteger(value) => output.writeByte(3);output.writeLong(value)
    case MySqlFormat3Cell.Floating(bits) => output.writeByte(4);output.writeLong(bits)
    case MySqlFormat3Cell.DateTime(value) =>
      output.writeByte(5);output.writeInt(value.getYear)
      output.writeByte(value.getMonthValue);output.writeByte(value.getDayOfMonth)
      output.writeByte(value.getHour);output.writeByte(value.getMinute);output.writeByte(value.getSecond)
      output.writeInt(value.getNano)
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled canonical: " + reason)
