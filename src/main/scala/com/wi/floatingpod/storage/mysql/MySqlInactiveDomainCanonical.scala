package com.wi.floatingpod.storage.mysql

import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID

/** DATA字节构造，无owner/admission/commit发行；运行时只能在原规划root的allocation scope中调用。 */
private[mysql] object MySqlInactiveDomainCanonical:
  private val Domain="com.wi.floatingpod.mysql-inactive-domain-effect/v1"
  private val Encoding="FP_MYSQL_INACTIVE_DOMAIN_EFFECT_BE_V1"
  private val ContextColumns=Set("original_context_bytes","original_context_sha256")
  private val PreimageColumns=Set("preimage_bytes","preimage_sha256")
  private val TargetColumns=Set("target_bytes","target_sha256")
  private val WitnessColumns=Set("commit_witness_bytes","commit_witness_sha256")
  private val CyclicWriterColumns=Set("last_commit_fingerprint")

  def projection(snapshot: MySqlFormat3FullSnapshot,request: MySqlInactiveDomainUpdateRequest,
      plan: MySqlOwnedRepositoryOperationPlan,context: MySqlRepositoryOperationContextIdentity,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if snapshot == null || request == null || plan == null || context == null || phase == null || image == null ||
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
    MySqlControlledDebugRelations.requireValidFullGraph(snapshot,root)
    withOriginalAllocation(root) {
      val length=measure(root) { output => writeProjection(output,snapshot,request,plan,context,phase,image,root) }
      MySqlRecoveryCanonicalBytes.encodeExact(Encoding,snapshot.limitProfile,length) { output =>
        writeProjection(output,snapshot,request,plan,context,phase,image,root)
      }
    }

  def scope(request: MySqlInactiveDomainUpdateRequest,context: MySqlRepositoryOperationContextIdentity,
      key: MySqlRecoveryResourceKey,root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || context==null || root==null || root.isClosed || key==null ||
      (context.exactLimitProfile ne root.profile) || (key.exactProfile ne root.profile) ||
      key.resourceKind!="POD" || !key.podUuid.contains(request.podUuid) then fail("original inactive scope resource")
    encode(root,"FP_MYSQL_INACTIVE_DOMAIN_SCOPE_BE_V1") { output =>
      text(output,"com.wi.floatingpod.mysql-inactive-domain-scope/v1");output.writeByte(1)
      output.writeInt(countScope(context,root).toInt);context.writeScopeIdentityFields(output)
      text(output,"DOMAIN_MUTATION")
      output.writeInt(key.canonicalLength);key.writeCanonicalTo(output)
      uuid(output,request.podUuid);output.writeLong(request.expectedVersion)
      MySqlRecoveryCanonicalBytes.writeOptionUuid(output,request.expectedOwner)
      // 作用域只编码原请求的 CAS 身份；旧 locator revision 来自完整原图及固定 O tuple。
    }
  def fingerprint(request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,phase: MySqlOperationEffectPhase,
      scope: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,target: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || plan==null || context==null || phase==null || scope==null || before==null || target==null ||
      root==null || root.isClosed || (context.exactLimitProfile ne root.profile) then fail("complete fingerprint inputs")
    encode(root,"FP_MYSQL_INACTIVE_DOMAIN_FINGERPRINT_BE_V1") { output =>
      text(output,"com.wi.floatingpod.mysql-inactive-domain-fingerprint/v1");output.writeByte(1)
      text(output,phaseName(phase));framed(output,plan.requestIdentity.canonical)
      output.writeInt(countScope(context,root).toInt);context.writeScopeIdentityFields(output)
      identities(output,request,plan)
      framed(output,scope);framed(output,before);framed(output,target)
    }
  def witness(request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,plannedAt: java.time.LocalDateTime,
      scope: MySqlRecoveryCanonicalBytes,fingerprint: MySqlRecoveryCanonicalBytes,
      standardContext: MySqlRecoveryCanonicalBytes,before: MySqlRecoveryCanonicalBytes,
      target: MySqlRecoveryCanonicalBytes,reserveRaw: Option[MySqlRecoveryOperationRawEnvelope],
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    if request==null || plan==null || phase==null || plannedAt==null || plannedAt.getNano%1000!=0 ||
      plannedAt.getYear<1000 || plannedAt.getYear>9999 || scope==null || fingerprint==null || standardContext==null ||
      before==null || target==null || reserveRaw==null || reserveRaw.exists(_==null) || root==null || root.isClosed ||
      (phase==MySqlOperationEffectPhase.Reserve && reserveRaw.nonEmpty) ||
      (phase==MySqlOperationEffectPhase.Execute && reserveRaw.isEmpty) then fail("fixed phase witness inputs")
    withOriginalAllocation(root) {
      reserveRaw.foreach { raw =>
        MySqlControlledDebugHistoricalRow.requireValid(raw.row,root)
        if !raw.exactlyMatches(raw.row,root.profile) then fail("original reserve raw framing differs from complete row")
      }
      encode(root,"FP_MYSQL_INACTIVE_DOMAIN_WITNESS_BE_V1") { output =>
      text(output,"com.wi.floatingpod.mysql-inactive-domain-witness/v1");output.writeByte(1)
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
    }
  private def identities(output: DataOutputStream,request: MySqlInactiveDomainUpdateRequest,
      plan: MySqlOwnedRepositoryOperationPlan): Unit =
    uuid(output,request.expectedBinding.bindingUuid);uuid(output,request.podUuid)
    output.writeLong(request.expectedVersion);MySqlRecoveryCanonicalBytes.writeOptionUuid(output,request.expectedOwner)
    uuid(output,plan.workflowUuid);uuid(output,plan.reserveOperationUuid);uuid(output,plan.executeOperationUuid)
  private def phaseName(phase: MySqlOperationEffectPhase): String =
    if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE"
  private def framed(output: DataOutputStream,value: MySqlRecoveryCanonicalBytes): Unit =
    output.writeInt(value.length);value.writeTo(output)
  private def encode(root: MySqlFormat3ReadBudget,encoding: String)(write: DataOutputStream=>Unit): MySqlRecoveryCanonicalBytes =
    withOriginalAllocation(root) {
      val length=measure(root)(write)
      MySqlRecoveryCanonicalBytes.encodeExact(encoding,root.profile,length)(write)
    }
  private def withOriginalAllocation[A](root: MySqlFormat3ReadBudget)(body: => A): A =
    if root==null || root.isClosed || root.isScopedReadBudget then fail("exact original open allocation root required")
    // 临时再生成只接受该 root 的实际 lexical child；不新建 ledger、不切换 sibling。
    val actual=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    if actual.isClosed || (actual.profile ne root.profile) ||
      ((actual ne root) && !actual.isCanonicalComparisonChildOf(root)) then fail("foreign canonical allocation scope")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(actual) { body }
  private def measure(root: MySqlFormat3ReadBudget)(write: DataOutputStream=>Unit): Long =
    root.reserveTemporaryMemory(512L,"inactive projection counting stream")
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
      try root.releaseTemporaryMemory(512L,"inactive projection counting stream")
      catch case cleanup: Throwable => if primary==null then primary=cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      if primary != null then throw primary

  private def writeProjection(output: DataOutputStream,snapshot: MySqlFormat3FullSnapshot,
      request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage,root: MySqlFormat3ReadBudget): Unit =
    text(output,Domain);output.writeByte(1)
    text(output,if phase==MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
    text(output,if image==MySqlOperationEffectImage.Before then "BEFORE" else "TARGET")
    output.writeInt(plan.requestIdentity.canonicalLength);plan.requestIdentity.canonical.writeTo(output)
    val contextLength=countScope(context,root)
    output.writeInt(contextLength.toInt);context.writeScopeIdentityFields(output)
    text(output,"DOMAIN_MUTATION")
    uuid(output,request.expectedBinding.bindingUuid);uuid(output,request.podUuid)
    output.writeLong(request.expectedVersion);MySqlRecoveryCanonicalBytes.writeOptionUuid(output,request.expectedOwner)
    uuid(output,plan.workflowUuid);uuid(output,plan.reserveOperationUuid);uuid(output,plan.executeOperationUuid)
    text(output,snapshot.schema)
    asciiSha(output,MySqlFormat3RelationShapes.SourceFoundationSha256)
    asciiSha(output,MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256)
    asciiSha(output,MySqlFormat3RelationShapes.SourceLockedReadSha256)
    val connection=snapshot.connection
    uuid(output,connection.serverUuid);text(output,connection.serverVersion);text(output,connection.catalog)
    text(output,connection.effectiveUser);text(output,connection.defaultStorageEngine)
    output.writeInt(connection.innodbFlushLogAtTrxCommit);output.writeLong(connection.syncBinlog)
    text(output,connection.transactionIsolation);text(output,connection.readTransactionIsolation)
    text(output,connection.sqlMode);text(output,connection.timeZone);text(output,connection.tlsVersion);text(output,connection.tlsCipher)
    uuid(output,snapshot.observationScope.bootUuid);output.writeLong(snapshot.observationScope.bootGeneration)
    output.writeLong(snapshot.observationScope.connectionGeneration);output.writeLong(snapshot.maxAllowedPacketBytes)
    writeProfile(output,snapshot.limitProfile,root)
    output.writeInt(snapshot.relations.size)
    snapshot.relations.foreach { relation =>
      val shape=MySqlFormat3RelationShapes.forTable(relation.table)
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
        if MySqlInactiveDomainSelection.includes(relation.table,relation.rows(rowIndex),request) then
          selectedCount=Math.addExact(selectedCount,1)
        rowIndex+=1
      output.writeInt(selectedCount)
      relation.rows.foreach { row =>
        if MySqlInactiveDomainSelection.includes(relation.table,row,request) then
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

  private def countScope(context: MySqlRepositoryOperationContextIdentity,root: MySqlFormat3ReadBudget): Long =
    val result=measure(root)(context.writeScopeIdentityFields)
    MySqlFormat3JvmArrayLimit.requireLength(result,"inactive scope identity frame")
    result
  private def asciiSha(output: DataOutputStream,value: String): Unit =
    if value==null || value.length!=64 then fail("frozen structural source SHA")
    var index=0
    while index<value.length do
      val character=value.charAt(index)
      if !((character>='0' && character<='9') || (character>='A' && character<='F')) then fail("frozen structural source SHA ASCII")
      output.writeByte(character.toInt);index+=1
  private def writeProfile(output: DataOutputStream,profile: com.wi.floatingpod.config.SourceLimitProfile,
      root: MySqlFormat3ReadBudget): Unit =
    root.reserveTemporaryMemory(192L,"inactive immutable profile framing copies")
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
      try root.releaseTemporaryMemory(192L,"inactive immutable profile framing copies")
      catch case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  private def omissionReason(table: String,row: Vector[MySqlFormat3Cell],column: String,
      request: MySqlInactiveDomainUpdateRequest,plan: MySqlOwnedRepositoryOperationPlan,
      phase: MySqlOperationEffectPhase,image: MySqlOperationEffectImage): String =
    if table=="fp_recovery_operation" then
      val exact=matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) &&
        matches(row,table,"workflow_uuid",plan.workflowUuid) && matches(row,table,"pod_uuid",request.podUuid) &&
        MySqlModeOperationEffectCodec.value(row,table,"attempt_uuid")==MySqlFormat3Cell.SqlNull &&
        MySqlModeOperationEffectCodec.value(row,table,"footprint_uuid")==MySqlFormat3Cell.SqlNull
      val execute=exact && matches(row,table,"operation_uuid",plan.executeOperationUuid)
      val reserve=exact && matches(row,table,"operation_uuid",plan.reserveOperationUuid)
      val common=ContextColumns.contains(column) || TargetColumns.contains(column) || column=="operation_fingerprint"
      val selected=
        if phase==MySqlOperationEffectPhase.Reserve then
          image==MySqlOperationEffectImage.Target &&
            ((reserve && (common || WitnessColumns.contains(column))) ||
             (execute && (common || PreimageColumns.contains(column))))
        else execute && (common || (if image==MySqlOperationEffectImage.Before then PreimageColumns.contains(column) else WitnessColumns.contains(column)))
      if selected then "CURRENT_INACTIVE_PLAN_OPERATION_CYCLE_V1" else null
    else if table=="fp_runtime_writer" && image==MySqlOperationEffectImage.Target && CyclicWriterColumns.contains(column) &&
      matches(row,table,"binding_uuid",request.expectedBinding.bindingUuid) then "CURRENT_INACTIVE_PHASE_WRITER_FINGERPRINT_V1"
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
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive canonical: " + reason)
