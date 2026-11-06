package com.wi.floatingpod.storage.mysql

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction,StandardCharsets}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 持久journal身份、编码重建与独立业务重算；仅DATA，不代表SQL ACK或恢复资格。 */
private[mysql] object MySqlDbFirstRecallJournalPersistedRead:
  private val O="fp_recovery_operation"
  private val W="fp_runtime_writer"
  private val derived=Set("original_context_bytes","original_context_sha256","preimage_bytes","preimage_sha256",
    "target_bytes","target_sha256","operation_fingerprint","commit_witness_bytes","commit_witness_sha256")
  private val pairs=Map("request_identity_bytes"->"request_identity_sha256","original_context_bytes"->"original_context_sha256",
    "scope_bytes"->"scope_sha256","preimage_bytes"->"preimage_sha256","target_bytes"->"target_sha256",
    "commit_witness_bytes"->"commit_witness_sha256","original_diagnostic_identity"->"original_diagnostic_sha256")
  private def fail(message:String):Nothing=throw new SQLDataException("persisted recall journal identity: "+message)
  private def value(row:Vector[MySqlFormat3Cell],name:String,inputs:MySqlScriptSchemaInputs):MySqlFormat3Cell =
    MySqlDbFirstInitialActivationMaterializer.value(row,O,name,inputs)
  private def binary(cell:MySqlFormat3Cell):MySqlFormat3Binary=cell match
    case MySqlFormat3Cell.Binary(bytes)=>bytes
    case _=>fail("required binary field")

  private def key(row:Vector[MySqlFormat3Cell],shape:MySqlFormat3RelationShape):Vector[MySqlFormat3Cell] =
    shape.primaryKey.map(n=>row(shape.columns.indexWhere(_.name==n)))

  private final case class DecodedIdentity(request:MySqlDbFirstRecallJournalRequest,
      facts:MySqlDbFirstRecallJournalFacts,connection:MySqlFormat3ConnectionObservation,
      observation:MySqlFormat3ObservationScope,packet:Long,beforeSha:Vector[Byte])

  /** 原binary只读游标；每次分配按实际长度预收，拒负长度、截断、非法UTF8和尾随bytes。 */
  private final class Reader(input: MySqlFormat3Binary, inputs: MySqlScriptSchemaInputs, owner: MySqlFormat3ReadBudget):
    private var offset=0
    private def need(n: Long): Unit =
      if n<0L || n>input.length.toLong-offset then fail("truncated/negative frame")
    def end(): Unit=if offset!=input.length then fail("trailing bytes")
    def octet(): Int =
      need(1);val n=input.byteAt(offset)&255;offset+=1;n
    def int32(): Int=(octet()<<24)|(octet()<<16)|(octet()<<8)|octet()
    def int64(): Long =
      var n=0L
      var i=0
      while i<8 do
        n=(n<<8)|octet().toLong;i+=1
      n
    def bool(): Boolean=octet() match
      case 0=>false
      case 1=>true
      case _=>fail("noncanonical boolean")
    def count(max: Long): Int =
      val n=int32()
      if n<0 || n.toLong>max then fail("count exceeds explicit bound")
      n
    def bytes(n: Int): Array[Byte] =
      need(n.toLong)
      owner.reserveRetainedMemory(Math.addExact(n.toLong,64L),"persisted DATA binary BEFORE allocation")
      val result=new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(n.toLong,"persisted DATA frame"))
      var i=0
      while i<n do
        result(i)=input.byteAt(offset);offset+=1;i+=1
      result
    def vector(expected: Int): Vector[Byte] =
      if int32()!=expected then fail("fixed binary length differs")
      owner.reserveRetainedMemory(64L+16L*expected,"persisted DATA vector BEFORE allocation")
      bytes(expected).toVector
    def uuid(): UUID =
      val id=new UUID(int64(),int64())
      if id==new UUID(0L,0L) then fail("zero UUID")
      id
    def text(maximum: Long=inputs.budget.snapshotBytes): String =
      val n=count(maximum);need(n.toLong)
      owner.reserveRetainedMemory(64L+2L*n,"persisted DATA decoded String BEFORE allocation")
      val temporary=384L+3L*n
      owner.reserveTemporaryMemory(temporary,"persisted DATA strict UTF8 workspace")
      var primary: Throwable=null
      try
        val decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
        decoder.decode(ByteBuffer.wrap(bytes(n))).toString
      catch
        case cause: Throwable=>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => owner.releaseTemporaryMemory(temporary,"persisted UTF8 workspace detached")))
    def exact(expected: String): Unit=if text()!=expected then fail("fixed domain/shape differs: "+expected)
    def header(domain: String): Unit =
      exact(domain)
      if octet()!=1 then fail("canonical version differs")
    def datetime(): LocalDateTime =
      if octet()!=5 then fail("DATETIME cell tag differs")
      val result=LocalDateTime.of(int32(),octet(),octet(),octet(),octet(),octet(),int32())
      if result.getYear<1000 || result.getYear>9999 || result.getNano%1000!=0 then fail("invalid DATETIME6")
      result
    def ordinary(tag: Int,column: MySqlFormat3ColumnShape,table: String): MySqlFormat3Cell = tag match
      case 0=>
        if !column.nullable then fail("NULL in required column")
        MySqlFormat3Cell.SqlNull
      case 1=>MySqlFormat3Cell.Text(text(inputs.maximumPayloadBytes(table,column).getOrElse(column.maximumLength.map(_.toLong*4L).getOrElse(inputs.budget.snapshotBytes))))
      case 2=>
        val max=inputs.maximumPayloadBytes(table,column).getOrElse(column.maximumLength.map(_.toLong).getOrElse(inputs.budget.snapshotBytes))
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes(count(max)),owner))
      case 3=>MySqlFormat3Cell.SignedInteger(int64())
      case 4=>MySqlFormat3Cell.Floating(int64())
      case 5=>
        val t=LocalDateTime.of(int32(),octet(),octet(),octet(),octet(),octet(),int32())
        if t.getYear<1000 || t.getYear>9999 || t.getNano%1000!=0 then fail("invalid DATETIME6")
        MySqlFormat3Cell.DateTime(t)
      case _=>fail("unknown typed cell tag")
    def scope(request:MySqlDbFirstRecallJournalRequest):DecodedIdentity =
      header(MySqlDbFirstRecallJournalCanonical.Scope)
      val tag=request.journalTag
      if uuid()!=tag.bootId || int64()!=tag.bootGeneration || int64()!=tag.connectionGeneration ||
        uuid()!=tag.operationId || uuid()!=request.bindingUuid || uuid()!=request.podUuid ||
        uuid()!=request.originalTag.operationId then fail("scope/request identity differs")
      val barrier=uuid();val connection=int64();val epoch=int64();val lease=datetime()
      if int64()!=request.expectedStateVersion then fail("scope/request version differs")
      val planned=datetime();val result=datetime()
      val beforeSha=vector(32)
      if vector(32)!=request.canonical.sha256 || vector(32)!=inputs.sourceLimitProfile.sha256().toVector then
        fail("scope original request/profile digest differs")
      val server=uuid()
      val connectionHeader=MySqlFormat3ConnectionObservation(server,text(),text(),text(),text(),int32(),int64(),text(),text(),text(),text(),text(),text())
      val observation=MySqlFormat3ObservationScope(uuid(),int64(),int64())
      val packet=int64();end()
      if packet<=0L || connectionHeader.catalog!=inputs.schema || observation.bootUuid!=tag.bootId ||
        observation.bootGeneration!=tag.bootGeneration || observation.connectionGeneration!=tag.connectionGeneration then
        fail("original scope header/tag/catalog differs")
      DecodedIdentity(request,MySqlDbFirstRecallJournalFacts(connection,barrier,epoch,lease,planned,result),
        connectionHeader,observation,packet,beforeSha)

    def effect(target: Boolean, beforeSha: Vector[Byte],
        inventory: Map[Vector[MySqlFormat3Cell],Vector[MySqlFormat3Cell]], raw: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3RelationSnapshot] =
      header(MySqlDbFirstRecallJournalCanonical.Effect)
      if bool()!=target then fail("effect phase differs")
      exact(inputs.schema);exact(inputs.ddlSha256)
      if vector(32)!=beforeSha || int32()!=inputs.canonicalOrder.size then fail("effect before SHA/table count differs")
      var totalRows=0L
      val result=inputs.canonicalOrder.map { shape =>
        exact(shape.table)
        if int32()!=shape.columns.size then fail("effect column count differs")
        shape.columns.foreach { c=>exact(c.name);exact(c.sqlDeclaration) }
        val countRows=count(inputs.budget.rows)
        totalRows=Math.addExact(totalRows,countRows.toLong)
        if totalRows>inputs.budget.rows then fail("aggregate row count exceeded")
        owner.reserveRetainedMemory(256L+64L*countRows,"persisted effect row references BEFORE allocation")
        val rows=Vector.newBuilder[Vector[MySqlFormat3Cell]]
        var rowNumber=0
        while rowNumber<countRows do
          owner.reserveRetainedMemory(512L+192L*shape.columns.size,"persisted effect row/marker metadata BEFORE allocation")
          val values=new Array[MySqlFormat3Cell](shape.columns.size)
          val marked=new Array[Boolean](shape.columns.size)
          val markerKeys=new Array[Vector[MySqlFormat3Cell]](shape.columns.size)
          val literalPayload=new Array[Boolean](shape.columns.size)
          val descriptors=scala.collection.mutable.ArrayBuffer.empty[(Int,Long,Vector[Byte])]
          var index=0
          while index<shape.columns.size do
            val column=shape.columns(index)
            octet() match
              case 0x7f=>
                if !target then fail("CURRENT marker in preimage")
                exact(MySqlDbFirstRecallJournalCanonical.Derived);exact("CURRENT");exact(shape.table)
                if int32()!=shape.primaryKey.size then fail("CURRENT PK width differs")
                val markerKey=shape.primaryKey.map { name=>
                  val pkColumn=shape.columns.find(_.name==name).get
                  ordinary(octet(),pkColumn,shape.table)
                }
                exact(column.name);exact(column.sqlDeclaration)
                if shape.table==O && derived.contains(column.name) && markerKey==key(raw,shape) then
                  values(index)=raw(index)
                else if shape.table==W && column.name=="last_commit_fingerprint" &&
                  markerKey==Vector(MySqlFormat3Cell.SignedInteger(1L)) then values(index)=value(raw,"operation_fingerprint",inputs)
                else fail("CURRENT marker outside exact table/PK/column whitelist")
                marked(index)=true
                markerKeys(index)=markerKey
              case 7=>
                if shape.table!=O || !pairs.contains(column.name) then fail("historical descriptor outside RAW blob column")
                exact(MySqlDbFirstRecallJournalCanonical.Derived);exact("HISTORICAL");exact(column.name);exact(column.sqlDeclaration)
                val length=int64()
                if length<0 || length>inputs.maximumPayloadBytes(shape.table,column).getOrElse(inputs.budget.snapshotBytes) then fail("descriptor length exceeds column budget")
                val digest=bytes(32).toVector
                descriptors+=((index,length,digest))
                values(index)=MySqlFormat3Cell.SqlNull
              case tag=>
                values(index)=ordinary(tag,column,shape.table)
                literalPayload(index)=tag==2 && pairs.contains(column.name)
            index+=1
          var row=values.toVector
          val rowKey=key(row,shape)
          val own=shape.table==O && rowKey==key(raw,shape)
          if shape.table==O && own && !target then fail("new original O appears in preimage")
          shape.columns.zipWithIndex.foreach { (column,i)=>
            val expected=target && ((own && derived.contains(column.name)) || (shape.table==W && column.name=="last_commit_fingerprint"))
            if marked(i)!=expected || (marked(i) && markerKeys(i)!=rowKey) then fail("missing/unexpected CURRENT marker or PK")
          }
          if shape.table==O && !own then
            val original=inventory.getOrElse(rowKey,fail("missing original RAW by full PK"))
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(original,owner.profile,owner,Some(inputs))
            descriptors.foreach { (i,length,digest)=>
              val actual=binary(original(i))
              val hash=binary(value(original,pairs(shape.columns(i).name),inputs))
              if actual.length.toLong!=length || !hash.equalsVector(digest) then fail("historical RAW descriptor length/SHA differs")
              row=row.updated(i,original(i))
            }
            shape.columns.zipWithIndex.foreach { (c,i)=>
              if literalPayload(i) || (pairs.contains(c.name) && row(i)!=MySqlFormat3Cell.SqlNull && !descriptors.exists(_._1==i)) then
                fail("missing historical descriptor")
            }
            // 原projection的scalar/NULL可能早于当前inventory结算状态；只借其PK对应的原blob。
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row,owner.profile,owner,Some(inputs))
          else if descriptors.nonEmpty then fail("descriptor in current original O")
          rows+=row
          rowNumber+=1
        MySqlFormat3RelationSnapshot(shape.table,rows.result())
      }
      end()
      result

  /** 只接受原Full27分配族内的完整O行；临时解码对象不离开comparison作用域。 */
  def requireCommittedIdentity(raw:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      originalRoot:MySqlFormat3ReadBudget):Unit =
    require(raw != null && inputs != null && originalRoot != null && !originalRoot.isClosed &&
      !originalRoot.isScopedReadBudget && inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "actual Full27 recall journal DATA/root required")
    originalRoot.requireScriptDataInputs(inputs)
    raw.foreach {
      case MySqlFormat3Cell.Binary(bytes)=>
        require(bytes != null && bytes.storageBudget != null && !bytes.storageBudget.isClosed &&
          bytes.storageBudget.sharesAllocationLedger(originalRoot),"original recall journal RAW allocation family required")
      case _=>()
    }
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
      decodeIdentity(raw,inputs,child)
    }

  /** 只验证原C引用的J DATA；完整J业务重算由caller在同一原comparison family执行。 */
  private[mysql] def requireRecallLink(raw:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      request:MySqlDbFirstRecallRequest,originalRoot:MySqlFormat3ReadBudget):Unit =
    val link=request.journalLink.getOrElse(fail("explicit J-bound recall request required"))
    link.requireFor(request.tag,request.ownerUuid)
    requireCommittedIdentity(raw,inputs,originalRoot)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
      val original=decodeIdentity(raw,inputs,child).request
      if original.journalTag!=link.journalTag || original.originalTag!=link.originalTagFor(request.tag) ||
        original.bindingUuid!=request.bindingUuid || original.podUuid!=request.podUuid ||
        original.expectedStateVersion!=request.capturedStateVersion || original.actorUuid!=link.actorUuid ||
        original.clientRequestUuid!=link.clientRequestUuid || original.canonical.sha256!=link.journalRequestSha256 ||
        inputs.sourceLimitProfile.sha256().toVector!=request.profileSha256 then fail("original C/J identity, actor, client, version or digest differs")
    }

  private def decodeIdentity(raw:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      child:MySqlFormat3ReadBudget):DecodedIdentity =
    child.reserveRetainedMemory(16384L,"persisted recall journal identity metadata BEFORE allocation")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw,child.profile,child,Some(inputs))
    if value(raw,"step",inputs)!=MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallJournalStep) ||
      value(raw,"operation_state",inputs)!=MySqlFormat3Cell.Text("COMMITTED") then fail("original committed recall journal required")
    def blob(name:String)=binary(value(raw,name,inputs))
    val request=MySqlDbFirstRecallJournalRequest.decode(blob("request_identity_bytes"),child)
    if !request.canonical.exactlyMatchesCell(value(raw,"request_identity_bytes",inputs)) then fail("request does not exactly reencode")
    val decoded=new Reader(blob("scope_bytes"),inputs,child).scope(request)
    val tag=request.journalTag
    val facts=decoded.facts
    def same(name:String,expected:MySqlFormat3Cell):Unit=
      if value(raw,name,inputs)!=expected then fail("scope/RAW differs: "+name)
    def sameUuid(name:String,expected:UUID):Unit=
      if !binary(value(raw,name,inputs)).equalsUuid(expected) then fail("scope/RAW differs: "+name)
    Vector("binding_uuid"->request.bindingUuid,"pod_uuid"->request.podUuid,
      "operation_uuid"->tag.operationId,"workflow_uuid"->request.originalTag.operationId,"boot_uuid"->tag.bootId,
      "connection_generation_uuid"->facts.barrierGenerationUuid).foreach((name,id)=>sameUuid(name,id))
    Vector("boot_generation"->tag.bootGeneration,"connection_id"->facts.connectionId,"fence_epoch"->facts.writerEpoch,
      "expected_domain_version"->request.expectedStateVersion).foreach((name,n)=>same(name,MySqlFormat3Cell.SignedInteger(n)))
    Vector("planned_at"->facts.plannedAt,"fixed_lease_until"->facts.leaseUntil,"result_at"->facts.resultAt)
      .foreach((name,t)=>same(name,MySqlFormat3Cell.DateTime(t)))
    Vector("source_unit_uuid","forest_uuid","expected_source_revision","reserve_operation_uuid",
      "previous_step_operation_uuid","settled_by_operation_uuid","original_diagnostic_identity","original_diagnostic_sha256")
      .foreach(name=>same(name,MySqlFormat3Cell.SqlNull))
    decoded

  /** 重建原完整effect并校验规范闭环；尚不独立重做业务变化，也不签发恢复/SQL资格。 */
  def requireCommittedReconstruction(image:MySqlScriptSchemaSnapshot,operationUuid:UUID,
      originalRoot:MySqlFormat3ReadBudget):Unit =
    verify(image,operationUuid,originalRoot,false,(_,_)=>())

  /** 原before上的独立业务增量重算；仍不证明当前latest链、SQL提交或恢复执行资格。 */
  def verifyCommitted(image:MySqlScriptSchemaSnapshot,operationUuid:UUID,
      originalRoot:MySqlFormat3ReadBudget):Unit =
    withCommittedTransition(image,operationUuid,originalRoot)((_,_)=>())

  /** 使用原完整重建及业务重算；借用图仅在外层comparison scope内有效。 */
  private[mysql] def withCommittedTransition(image:MySqlScriptSchemaSnapshot,operationUuid:UUID,
      originalRoot:MySqlFormat3ReadBudget)(consume:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot)=>Unit):Unit =
    verify(image,operationUuid,originalRoot,true,consume)

  private def verify(image:MySqlScriptSchemaSnapshot,operationUuid:UUID,
      originalRoot:MySqlFormat3ReadBudget,checkBusiness:Boolean,
      consume:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot)=>Unit):Unit =
    require(image != null && originalRoot != null && !originalRoot.isClosed && !originalRoot.isScopedReadBudget &&
      !image.allocationBudget.isClosed && image.allocationBudget.sharesAllocationLedger(originalRoot) &&
      ((image.allocationBudget eq originalRoot) || image.allocationBudget.isSnapshotReadChildOf(originalRoot) ||
        (image.allocationBudget.isCanonicalComparisonChildOf(originalRoot) &&
          (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq image.allocationBudget))) &&
      image.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "persisted recall journal requires actual Full27 image/root")
    val inputs=image.inputs
    originalRoot.requireScriptDataInputs(inputs)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
      child.reserveRetainedMemory(65536L,"persisted recall journal reconstruction metadata BEFORE allocation")
      val shape=inputs.forTable(O)
      val all=image.relation(O).rows
      child.reserveRetainedMemory(256L+512L*all.size,"persisted recall journal original RAW PK index BEFORE allocation")
      val inventory=all.map(r=>key(r,shape)->r).toMap
      if inventory.size!=all.size then fail("duplicate original RAW PK")
      val selected=all.filter(r=>MySqlDbFirstInitialActivationMaterializer.uuidEquals(value(r,"operation_uuid",inputs),operationUuid))
      if selected.size!=1 then fail("original recall journal operation is not unique")
      val raw=selected.head
      val decoded=decodeIdentity(raw,inputs,child)
      def blob(name:String)=binary(value(raw,name,inputs))
      val beforeRows=new Reader(blob("preimage_bytes"),inputs,child).effect(false,decoded.beforeSha,inventory,raw)
      val before=MySqlScriptSchemaSnapshot.decodedForComparison(inputs.schema,decoded.connection,decoded.observation,
        decoded.packet,inputs,beforeRows,originalRoot,child)
      if before.canonicalSha256!=decoded.beforeSha then fail("reconstructed complete before SHA differs")
      val targetRows=new Reader(blob("target_bytes"),inputs,child).effect(true,decoded.beforeSha,inventory,raw)
      val target=MySqlScriptSchemaSnapshot.decodedForComparison(inputs.schema,decoded.connection,decoded.observation,
        decoded.packet,inputs,targetRows,originalRoot,child)
      val c=MySqlDbFirstRecallJournalCanonical
      val data=decoded.request
      val facts=decoded.facts
      val scope=c.scope(data,facts,before,originalRoot)
      val preimage=c.projection(data,before,before.relations,false,originalRoot)
      val projected=c.projection(data,before,target.relations,true,originalRoot)
      val fingerprint=c.fingerprint(data,scope,preimage,projected,originalRoot)
      val context=c.context(data,facts,fingerprint,originalRoot)
      val witness=c.witness(data,scope,preimage,projected,fingerprint,context,originalRoot)
      Vector("scope_bytes"->scope,"preimage_bytes"->preimage,"target_bytes"->projected,
        "original_context_bytes"->context,"commit_witness_bytes"->witness).foreach { (name,canonical)=>
        if !canonical.exactlyMatchesCell(value(raw,name,inputs)) then fail("reconstructed persisted field differs: "+name)
      }
      val generic=c.operationContext(data,facts,fingerprint,originalRoot).canonicalFingerprintSha256
      if !binary(value(raw,"operation_fingerprint",inputs)).equalsVector(generic) then fail("original O fingerprint differs")
      val writers=target.relation(W).rows
      if writers.size!=1 then fail("reconstructed target writer is not unique")
      def writerValue(name:String)=MySqlDbFirstInitialActivationMaterializer.value(writers.head,W,name,inputs)
      if !binary(writerValue("last_commit_fingerprint")).equalsVector(generic) ||
        !binary(writerValue("last_commit_operation_uuid")).equalsUuid(operationUuid) then fail("original target W commitment differs")
      if checkBusiness then MySqlDbFirstRecallJournalMaterializer.requireLocalTransition(before,target,data,facts,originalRoot)
      consume(before,target)
    }
