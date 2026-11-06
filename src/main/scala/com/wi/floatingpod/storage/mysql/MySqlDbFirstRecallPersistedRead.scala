package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 只验证单一持久COMMITTED O的完整local DATA transition；不证明latest W链、held或执行资格。 */
private[mysql] object MySqlDbFirstRecallPersistedRead:
  private val O="fp_recovery_operation"
  private val W="fp_runtime_writer"
  private val derived=Set("original_context_bytes","original_context_sha256","preimage_bytes","preimage_sha256",
    "target_bytes","target_sha256","operation_fingerprint","commit_witness_bytes","commit_witness_sha256")
  private val pairs=Map("request_identity_bytes"->"request_identity_sha256","original_context_bytes"->"original_context_sha256",
    "scope_bytes"->"scope_sha256","preimage_bytes"->"preimage_sha256","target_bytes"->"target_sha256",
    "commit_witness_bytes"->"commit_witness_sha256","original_diagnostic_identity"->"original_diagnostic_sha256")
  private def fail(message: String): Nothing=throw new SQLDataException("DB-first persisted DATA: "+message)
  private def value(row: Vector[MySqlFormat3Cell],table: String,name: String,inputs: MySqlScriptSchemaInputs): MySqlFormat3Cell =
    MySqlDbFirstRecallMaterializer.value(row,table,name,inputs)
  private def binary(cell: MySqlFormat3Cell): MySqlFormat3Binary=cell match
    case MySqlFormat3Cell.Binary(bytes)=>bytes
    case _=>fail("required binary field")
  private def key(row: Vector[MySqlFormat3Cell],shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    shape.primaryKey.map(n=>row(shape.columns.indexWhere(_.name==n)))

  private final case class ScopeData(facts: MySqlDbFirstRecallFacts, connection: MySqlFormat3ConnectionObservation,
      observation: MySqlFormat3ObservationScope, packet: Long, beforeSha: Vector[Byte])

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
    def request(checkpoint: Boolean): MySqlDbFirstSnapshotMutation =
      header("FP_MYSQL_REPOSITORY_REQUEST_BE_V1")
      if int32()!=1 then fail("request cardinality differs")
      val kind=text()
      if (checkpoint && kind!="DB_FIRST_ACTIVE_CHECKPOINT_V1") ||
        (!checkpoint && kind!="DB_FIRST_RECALL_V1" && kind!="DB_FIRST_RECALL_V2" && kind!="DB_FIRST_RECALL_V3") then fail("explicit snapshot request kind differs")
      val journalBound=kind=="DB_FIRST_RECALL_V2" || kind=="DB_FIRST_RECALL_V3"
      val boot=uuid();val generation=int64();val connectionGeneration=int64();val operation=uuid()
      val binding=uuid();val pod=uuid();val ownerUuid=if bool() then Some(uuid()) else None;val attempt=uuid()
      val version=int64();val format=int32();val nbtLength=int32()
      val nbtSha=vector(32)
      val profile=vector(inputs.sourceLimitProfile.canonicalBytes().length)
      val profileSha=vector(32)
      if !profile.equals(inputs.sourceLimitProfile.canonicalBytes().toVector) || profileSha!=inputs.sourceLimitProfile.sha256().toVector then fail("request exact profile differs")
      val world=text();val dimension=int32()
      val x=java.lang.Double.longBitsToDouble(int64());val y=java.lang.Double.longBitsToDouble(int64());val z=java.lang.Double.longBitsToDouble(int64())
      val yaw=java.lang.Float.intBitsToFloat(int32());val pitch=java.lang.Float.intBitsToFloat(int32())
      if generation<=0 || connectionGeneration<=0 || version<0 || format!=DbFirstPodSnapshot.FORMAT_VERSION ||
        nbtLength<=0 || nbtLength.toLong>MySqlDbFirstSchemaDefinition.maximumNbtBytes(inputs.sourceLimitProfile) || world.isEmpty ||
        !java.lang.Double.isFinite(x) || !java.lang.Double.isFinite(y) || !java.lang.Double.isFinite(z) ||
        !java.lang.Float.isFinite(yaw) || !java.lang.Float.isFinite(pitch) then fail("invalid request facts")
      val tag=IoOperationTag(boot,generation,connectionGeneration,operation)
      val link=if journalBound then
        val j=IoOperationTag(uuid(),int64(),int64(),uuid())
        val actor=uuid();val client=if bool() then Some(uuid()) else None;val digest=vector(32)
        val original=if kind=="DB_FIRST_RECALL_V3" then Some(IoOperationTag(uuid(),int64(),int64(),uuid())) else None
        val result=MySqlDbFirstRecallJournalLink(j,actor,client,digest,original)
        try result.requireFor(tag,ownerUuid)
        catch case cause:IllegalArgumentException=>throw new SQLDataException("persisted journal-bound request namespace/actor differs",cause)
        Some(result)
      else None
      end()
      owner.reserveRetainedMemory(4096L+16L*input.length,"persisted immutable request DATA BEFORE allocation")
      val all=input.copyBytes(owner,"persisted request canonical identity").toVector
      val digest=java.security.MessageDigest.getInstance("SHA-256").digest(input.copyBytes(owner,"persisted request digest input")).toVector
      val pose=new DbFirstPodSnapshot.Pose(world,dimension,x,y,z,yaw,pitch)
      if checkpoint then new MySqlDbFirstCheckpointRequest(tag,binding,pod,ownerUuid,attempt,
        version,format,nbtLength,nbtSha,profile,profileSha,pose,MySqlDbFirstCheckpointIdentity(all,digest,"RUN"))
      else new MySqlDbFirstRecallRequest(tag,binding,pod,ownerUuid,attempt,
        version,format,nbtLength,nbtSha,profile,profileSha,pose,MySqlDbFirstRecallIdentity(all,digest,"RUN"),link)
    def scope(request: MySqlDbFirstSnapshotMutation): ScopeData =
      header(MySqlDbFirstRecallCanonical.domain(request,MySqlDbFirstRecallCanonical.Scope))
      if uuid()!=request.tag.bootId || int64()!=request.tag.bootGeneration || int64()!=request.tag.connectionGeneration ||
        uuid()!=request.tag.operationId || uuid()!=request.bindingUuid || uuid()!=request.podUuid || uuid()!=request.attemptUuid then fail("scope/request identity differs")
      val barrier=uuid();val connectionId=int64();val epoch=int64();val lease=datetime()
      if int64()!=request.capturedStateVersion then fail("scope version differs")
      val revision=int64();val held=int64();val released=int64();val planned=datetime();val result=datetime()
      val beforeSha=vector(32)
      if vector(32)!=request.identity.sha256 || vector(32)!=request.profileSha256 then fail("scope request/profile digest differs")
      val server=uuid()
      val connection=MySqlFormat3ConnectionObservation(server,text(),text(),text(),text(),int32(),int64(),text(),text(),text(),text(),text(),text())
      val observation=MySqlFormat3ObservationScope(uuid(),int64(),int64())
      val packet=int64()
      if packet<=0 || connection.catalog!=inputs.schema then fail("original header catalog/packet differs")
      end()
      ScopeData(MySqlDbFirstRecallFacts(connectionId,barrier,epoch,lease,revision,held,released,planned,result),connection,observation,packet,beforeSha)

    def effect(request: MySqlDbFirstSnapshotMutation, target: Boolean, beforeSha: Vector[Byte],
        inventory: Map[Vector[MySqlFormat3Cell],Vector[MySqlFormat3Cell]], raw: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3RelationSnapshot] =
      header(MySqlDbFirstRecallCanonical.domain(request,MySqlDbFirstRecallCanonical.Effect))
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
                exact(MySqlDbFirstRecallCanonical.domain(request,MySqlDbFirstRecallCanonical.Derived));exact("CURRENT");exact(shape.table)
                if int32()!=shape.primaryKey.size then fail("CURRENT PK width differs")
                val markerKey=shape.primaryKey.map { name=>
                  val pkColumn=shape.columns.find(_.name==name).get
                  ordinary(octet(),pkColumn,shape.table)
                }
                exact(column.name);exact(column.sqlDeclaration)
                if shape.table==O && derived.contains(column.name) && markerKey==key(raw,shape) then
                  values(index)=raw(index)
                else if shape.table==W && column.name=="last_commit_fingerprint" &&
                  markerKey==Vector(MySqlFormat3Cell.SignedInteger(1L)) then values(index)=value(raw,O,"operation_fingerprint",inputs)
                else fail("CURRENT marker outside exact table/PK/column whitelist")
                marked(index)=true
                markerKeys(index)=markerKey
              case 7=>
                if shape.table!=O || !pairs.contains(column.name) then fail("historical descriptor outside RAW blob column")
                exact(MySqlDbFirstRecallCanonical.domain(request,MySqlDbFirstRecallCanonical.Derived));exact("HISTORICAL");exact(column.name);exact(column.sqlDeclaration)
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
              val hash=binary(value(original,O,pairs(shape.columns(i).name),inputs))
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

  def verifyCommitted(image: MySqlScriptSchemaSnapshot, operationUuid: UUID,
      originalRoot: MySqlFormat3ReadBudget): Unit =
    require(image!=null && originalRoot!=null && !originalRoot.isClosed && !originalRoot.isScopedReadBudget &&
      !image.allocationBudget.isClosed && image.allocationBudget.sharesAllocationLedger(originalRoot) &&
      ((image.allocationBudget eq originalRoot) || image.allocationBudget.isSnapshotReadChildOf(originalRoot)) &&
      image.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "persisted recall requires the actual Full27 image/root")
    val inputs=image.inputs
    originalRoot.requireScriptDataInputs(inputs)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
      decodeCommitted(image,operationUuid,originalRoot,child)
      ()
    }

  /** 当前原C可仍RESERVED/CANCELLED；只核完整request/J身份，不签发终局或重执行资格。 */
  private[mysql] def requireOriginalJournalRequest(raw:Vector[MySqlFormat3Cell],inputs:MySqlScriptSchemaInputs,
      journal:MySqlDbFirstRecallJournalRequest,root:MySqlFormat3ReadBudget):Unit =
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw,child.profile,child,Some(inputs))
      val request=new Reader(binary(value(raw,O,"request_identity_bytes",inputs)),inputs,child).request(false) match
        case actual:MySqlDbFirstRecallRequest=>actual
        case _=>fail("original journal C is not recall request")
      def sameUuid(column:String,id:UUID):Boolean=value(raw,O,column,inputs) match
        case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(id)
        case _=>false
      if !sameUuid("boot_uuid",request.tag.bootId) || !sameUuid("operation_uuid",request.tag.operationId) ||
          value(raw,O,"boot_generation",inputs)!=MySqlFormat3Cell.SignedInteger(request.tag.bootGeneration) then
        fail("original C RAW/request tag differs")
      val link=request.journalLink.getOrElse(fail("original C lost explicit journal link"))
      try link.requireJournalIdentity(journal,request.tag)
      catch case cause:IllegalArgumentException=>throw new SQLDataException("original C/J dual-scope identity differs",cause)
      if request.bindingUuid!=journal.bindingUuid ||
          request.podUuid!=journal.podUuid || request.capturedStateVersion!=journal.expectedStateVersion ||
          request.ownerUuid!=Some(journal.actorUuid) then
        fail("original C request does not match full J namespace/identity")
      val canonical=MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_REPOSITORY_REQUEST_BE_V1",child.profile) { out=>
        MySqlRepositoryRequestIdentity.writeDbFirstRecallData(out,request.tag,request.bindingUuid,request.podUuid,
          request.ownerUuid,request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,
          request.nbtByteLength,request.nbtSha256,request.profileBytes,request.profileSha256,request.pose,request.journalLink)
      }
      if !canonical.exactlyMatchesCell(value(raw,O,"request_identity_bytes",inputs)) then fail("original C request does not exactly reencode")
    }

  /** 同epoch schema6 mutation及journal的完整receipt DATA区段；未知kind/跨epoch不得跳过。 */
  private[mysql] def verifySnapshotReceiptSegment(image:MySqlScriptSchemaSnapshot,
      anchor:MySqlScriptSchemaSnapshot,originalRoot:MySqlFormat3ReadBudget):Long =
    verifyReceiptSegment(image,anchor,originalRoot,None,None,None)

  final case class CommittedStopSegment(steps:Long,sha256:String)
  private final case class StopSegmentScope(pod:UUID,activation:UUID,digest:java.security.MessageDigest)

  /** 仅本次已激活Pod停服事故：一条原activation及同Pod checkpoint；不承接其他业务kind。 */
  private[mysql] def verifyStopSnapshotReceiptSegment(image:MySqlScriptSchemaSnapshot,
      anchor:MySqlScriptSchemaSnapshot,originalRoot:MySqlFormat3ReadBudget,pod:UUID,activation:UUID,
      bootstrapReader:Option[MySqlDbFirstBootstrapSnapshotJdbc],
      nativeSource:Option[MySqlDbFirstBootstrapActor.NativePreparationSource]):CommittedStopSegment =
    require(pod!=null && activation!=null,"original stop incident Pod and activation required")
    originalRoot.reserveRetainedMemory(1024L,"committed stop chain digest and immutable evidence BEFORE allocation")
    val digest=java.security.MessageDigest.getInstance("SHA-256")
    val steps=verifyReceiptSegment(image,anchor,originalRoot,bootstrapReader,nativeSource,
      Some(StopSegmentScope(pod,activation,digest)))
    CommittedStopSegment(steps,java.util.HexFormat.of().formatHex(digest.digest()))

  private def verifyReceiptSegment(image:MySqlScriptSchemaSnapshot,
      anchor:MySqlScriptSchemaSnapshot,originalRoot:MySqlFormat3ReadBudget,
      bootstrapReader:Option[MySqlDbFirstBootstrapSnapshotJdbc],
      nativeSource:Option[MySqlDbFirstBootstrapActor.NativePreparationSource],
      stopScope:Option[StopSegmentScope]):Long =
    require(bootstrapReader.isEmpty || nativeSource.isEmpty,"one actual receipt-chain comparison source required")
    bootstrapReader.foreach(_.requirePreparationComparisonSource(image,originalRoot))
    nativeSource.foreach(_.requireComparisonSource(image,originalRoot))
    require(image!=null && anchor!=null && originalRoot!=null && !originalRoot.isClosed &&
      !originalRoot.isScopedReadBudget && !image.allocationBudget.isClosed && !anchor.allocationBudget.isClosed &&
      (image.inputs eq anchor.inputs) && image.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      image.allocationBudget.sharesAllocationLedger(originalRoot) && anchor.allocationBudget.sharesAllocationLedger(originalRoot) &&
      ((image.allocationBudget eq originalRoot) || image.allocationBudget.isSnapshotReadChildOf(originalRoot) ||
        bootstrapReader.nonEmpty || nativeSource.nonEmpty),
      "actual complete Full27 inventory/anchor and shared open root required")
    val inputs=image.inputs
    originalRoot.requireScriptDataInputs(inputs)
    if image.schema!=anchor.schema || image.connection.serverUuid!=anchor.connection.serverUuid ||
      image.connection.serverVersion!=anchor.connection.serverVersion || image.connection.effectiveUser!=anchor.connection.effectiveUser then
      fail("receipt segment database identity differs")
    var steps=0L
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot)
      val maximum=image.relation(O).rows.size.toLong
      if maximum>originalRoot.profile.maxMembers() then fail("receipt segment exceeds original member profile")
      child.reserveRetainedMemory(Math.addExact(4096L,Math.multiplyExact(maximum,128L)),"receipt segment visited/cursor metadata BEFORE allocation")
      val visited=scala.collection.mutable.HashSet.empty[UUID]
      def writer(rows:Vector[MySqlFormat3RelationSnapshot]):Vector[MySqlFormat3Cell] =
        val found=rows.find(_.table==W).getOrElse(fail("receipt segment has no W"))
        if found.rows.size!=1 then fail("receipt segment requires singleton W")
        found.rows.head
      val anchorWriter=writer(anchor.relations)
      val binding=value(anchorWriter,W,"binding_uuid",inputs)
      val epoch=value(anchorWriter,W,"writer_epoch",inputs)
      val owner=value(anchorWriter,W,"owner_boot_uuid",inputs)
      if owner==MySqlFormat3Cell.SqlNull then fail("receipt segment requires original non-null owner")
      var cursor=image.relations
      while !MySqlDbFirstRecallMaterializer.sameReceiptWithLeaseContinuation(anchor.relations,cursor,inputs) do
        val current=writer(cursor)
        if value(current,W,"binding_uuid",inputs)!=binding || value(current,W,"writer_epoch",inputs)!=epoch ||
          value(current,W,"owner_boot_uuid",inputs)!=owner then fail("receipt segment crossed original binding/epoch/owner")
        val bytes=binary(value(current,W,"last_commit_operation_uuid",inputs))
        if bytes.length!=16 then fail("receipt segment W operation is not UUID")
        var most=0L;var least=0L;var i=0
        while i<8 do
          most=(most<<8)|(bytes.byteAt(i).toLong&255L)
          least=(least<<8)|(bytes.byteAt(i+8).toLong&255L)
          i+=1
        child.reserveRetainedMemory(64L,"receipt segment UUID BEFORE allocation")
        val operation=new UUID(most,least)
        if visited.size.toLong>=maximum || !visited.add(operation) then fail("receipt segment missing/cyclic or cannot reach original target")
        val selected=image.relation(O).rows.filter(row=>
          MySqlDbFirstRecallMaterializer.uuidEquals(value(row,O,"operation_uuid",inputs),operation))
        if selected.size!=1 then fail("receipt segment operation is not unique")
        val raw=selected.head
        stopScope.foreach { scope=>
          val step=value(raw,O,"step",inputs)
          if !MySqlDbFirstRecallMaterializer.uuidEquals(value(raw,O,"pod_uuid",inputs),scope.pod) ||
              value(raw,O,"operation_state",inputs)!=MySqlFormat3Cell.Text("COMMITTED") ||
              !(step==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep) ||
                (step==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) && operation==scope.activation)) then
            fail("stop incident receipt chain contains another Pod, activation or unsupported operation")
          val fingerprint=binary(value(raw,O,"operation_fingerprint",inputs))
          if fingerprint.length!=32 then fail("stop incident operation fingerprint length differs")
          var b=7
          while b>=0 do
            scope.digest.update((operation.getMostSignificantBits >>> (b*8)).toByte);b-=1
          b=7
          while b>=0 do
            scope.digest.update((operation.getLeastSignificantBits >>> (b*8)).toByte);b-=1
          b=0
          while b<32 do
            scope.digest.update(fingerprint.byteAt(b));b+=1
        }
        def continue(before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot):Unit =
          if !MySqlDbFirstRecallMaterializer.sameReceiptWithLeaseContinuation(target.relations,cursor,inputs) then
            fail("receipt segment complete target does not continue to actual cursor")
          cursor=before.relations // 所有decoder复用本外层comparison child，游标不跨退款边界。
        value(raw,O,"step",inputs) match
          case MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallRefusedStep)=>
            MySqlDbFirstRecallRefusalPersistedRead.withCommittedTransition(image,operation,originalRoot)(continue)
          case MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallJournalStep)=>
            MySqlDbFirstRecallJournalPersistedRead.withCommittedTransition(image,operation,originalRoot)(continue)
          case MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep)=>
            MySqlDbFirstInitialActivationPersistedRead.withCommittedIdentity(image,operation,originalRoot,
              bootstrapReader,nativeSource)((before,target,_,_)=>continue(before,target))
          case MySqlFormat3Cell.Text("REARM") if
              MySqlDbFirstInitialPreparationPersistedRead.hasPreparationScope(value(raw,O,"scope_bytes",inputs))=>
            MySqlDbFirstInitialPreparationPersistedRead.withCommittedTransition(image,operation,originalRoot)(continue)
          case _=>
            val transition=decodeCommitted(image,operation,originalRoot,child)
            continue(transition.before,transition.target)
      stopScope.foreach(scope=>if !visited.contains(scope.activation) then fail("stop chain never reached its original activation"))
      steps=visited.size.toLong
    }
    steps

  private final case class Transition(request:MySqlDbFirstSnapshotMutation,facts:MySqlDbFirstRecallFacts,
      before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot)

  private def decodeCommitted(image:MySqlScriptSchemaSnapshot,operationUuid:UUID,
      originalRoot:MySqlFormat3ReadBudget,child:MySqlFormat3ReadBudget):Transition =
    val inputs=image.inputs
    child.reserveRetainedMemory(65536L,"persisted reader/index/facts fixed metadata BEFORE allocation")
    val shape=inputs.forTable(O)
    val all=image.relation(O).rows
    child.reserveRetainedMemory(256L+512L*all.size,"persisted original RAW PK index BEFORE allocation")
    val inventory=all.map(r=>key(r,shape)->r).toMap
    if inventory.size!=all.size then fail("duplicate original RAW PK")
    val selected=all.filter(r=>MySqlDbFirstRecallMaterializer.uuidEquals(value(r,O,"operation_uuid",inputs),operationUuid))
    if selected.size!=1 then fail("original operation is not unique")
    val raw=selected.head
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw,child.profile,child,Some(inputs))
    val checkpoint=value(raw,O,"step",inputs)==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep)
    if (!checkpoint && value(raw,O,"step",inputs)!=MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep)) ||
      value(raw,O,"operation_state",inputs)!=MySqlFormat3Cell.Text("COMMITTED") then fail("only committed recall/checkpoint local transitions are verified")
    def blob(name: String)=binary(value(raw,O,name,inputs))
    val request=new Reader(blob("request_identity_bytes"),inputs,child).request(checkpoint)
    val requestCanonical=MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_REPOSITORY_REQUEST_BE_V1",child.profile) { out=>
      request match
        case c: MySqlDbFirstCheckpointRequest => MySqlRepositoryRequestIdentity.writeDbFirstCheckpointData(out,c)
        case r: MySqlDbFirstRecallRequest =>
          MySqlRepositoryRequestIdentity.writeDbFirstRecallData(out,r.tag,r.bindingUuid,r.podUuid,r.ownerUuid,
            r.attemptUuid,r.capturedStateVersion,r.snapshotFormatVersion,r.nbtByteLength,
            r.nbtSha256,r.profileBytes,r.profileSha256,r.pose,r.journalLink)
        case _ => fail("unsupported persisted snapshot request")
    }
    if !requestCanonical.exactlyMatchesCell(value(raw,O,"request_identity_bytes",inputs)) then fail("request does not exactly reencode")
    val scope=new Reader(blob("scope_bytes"),inputs,child).scope(request)
    val p=new Reader(blob("preimage_bytes"),inputs,child).effect(request,false,scope.beforeSha,inventory,raw)
    val before=MySqlScriptSchemaSnapshot.decodedForComparison(inputs.schema,scope.connection,scope.observation,scope.packet,inputs,p,originalRoot,child)
    if before.canonicalSha256!=scope.beforeSha then fail("reconstructed complete before SHA differs")
    val targetRows=new Reader(blob("target_bytes"),inputs,child).effect(request,true,scope.beforeSha,inventory,raw)
    val target=MySqlScriptSchemaSnapshot.decodedForComparison(inputs.schema,scope.connection,scope.observation,scope.packet,inputs,targetRows,originalRoot,child)
    val snapshots=target.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.filter(r=>
      MySqlDbFirstRecallMaterializer.uuidEquals(value(r,MySqlDbFirstSchemaDefinition.SnapshotTable,"pod_uuid",inputs),request.podUuid) &&
      MySqlDbFirstRecallMaterializer.uuidEquals(value(r,MySqlDbFirstSchemaDefinition.SnapshotTable,"binding_uuid",inputs),request.bindingUuid))
    if snapshots.size!=1 then fail("target NBT snapshot is not unique")
    val nbt=binary(value(snapshots.head,MySqlDbFirstSchemaDefinition.SnapshotTable,"nbt_bytes",inputs))
    child.reserveRetainedMemory(1024L,"persisted caller-local DATA draft BEFORE allocation")
    val draft=new MySqlDbFirstRecallDraft(child)
    val facts=scope.facts
    val actualFacts=MySqlDbFirstRecallMaterializer.buildData(request,blob("request_identity_bytes"),nbt,before,
      facts.connectionId,facts.barrierGenerationUuid,facts.plannedAt,facts.resultAt,originalRoot,draft)
    if actualFacts!=facts || draft.target.relations!=target.relations then fail("independent complete local transition differs")
    Vector("scope_bytes"->draft.scope,"preimage_bytes"->draft.preimage,"target_bytes"->draft.projectedTarget,
      "original_context_bytes"->draft.context,"commit_witness_bytes"->draft.witness).foreach { (name,canonical)=>
      if !canonical.exactlyMatchesCell(value(raw,O,name,inputs)) then fail("recomputed persisted field differs: "+name)
    }
    Transition(request,facts,before,target)
