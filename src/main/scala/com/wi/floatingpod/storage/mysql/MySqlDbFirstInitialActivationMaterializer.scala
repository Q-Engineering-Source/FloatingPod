package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.config.SourceLimitProfile
import java.sql.SQLDataException
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

/** 原Entry先登记的部分构造责任；DATA失败不释放capture、原operation或freeze。 */
private[mysql] final class MySqlDbFirstInitialActivationDraft(val root: MySqlFormat3ReadBudget):
  var before: MySqlScriptSchemaSnapshot=null
  var construction: Vector[MySqlFormat3RelationSnapshot]=null
  var scope: MySqlRecoveryCanonicalBytes=null
  var preimage: MySqlRecoveryCanonicalBytes=null
  var projectedTarget: MySqlRecoveryCanonicalBytes=null
  var fingerprint: MySqlRecoveryCanonicalBytes=null
  var context: MySqlRecoveryCanonicalBytes=null
  var witness: MySqlRecoveryCanonicalBytes=null
  var heldReservation: Vector[MySqlFormat3Cell]=null
  var reservedOperation: Vector[MySqlFormat3Cell]=null
  var target: MySqlScriptSchemaSnapshot=null
  var result: MySqlDbFirstInitialActivationMaterialization=null

/** 完整27表DATA结果；不是SQL计划、held scope、生命周期完成或Ready。 */
private[mysql] final class MySqlDbFirstInitialActivationMaterialization private[mysql] (
    val request: MySqlDbFirstInitialActivationRequest, val facts: MySqlDbFirstInitialActivationFacts,
    val before: MySqlScriptSchemaSnapshot, val target: MySqlScriptSchemaSnapshot,
    val scope: MySqlRecoveryCanonicalBytes, val preimage: MySqlRecoveryCanonicalBytes,
    val projectedTarget: MySqlRecoveryCanonicalBytes, val fingerprint: MySqlRecoveryCanonicalBytes,
    val context: MySqlRecoveryCanonicalBytes, val witness: MySqlRecoveryCanonicalBytes,
    val heldReservation: Vector[MySqlFormat3Cell], val reservedOperation: Vector[MySqlFormat3Cell],
    private val root: MySqlFormat3ReadBudget):
  def verify(): Unit = verifyTarget(target)
  /** 已有原inventory与原O时严格再生；不从witness自报内容制造新的before或资格。 */
  def verifyTarget(candidate: MySqlScriptSchemaSnapshot): Unit =
    root.requireScriptDataInputs(before.inputs)
    if candidate == null || (candidate.inputs ne before.inputs) || (candidate.allocationBudget ne root) ||
      !candidate.exactlyEquals(target) then throw new SQLDataException("DB-first complete target differs from original materialization")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val active=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      active.reserveTemporaryMemory(16384L,"DB-first verification fixed lookup/context structures")
      var primary: Throwable=null
      try
        val c=MySqlDbFirstInitialActivationCanonical
        val s=c.scope(request,facts,before,root)
        val p=c.projection(request,before,before.relations,false,root)
        val t=c.projection(request,before,candidate.relations,true,root)
        val f=c.fingerprint(request,s,p,t,root)
        val x=c.context(request,facts,f,root)
        val genericFingerprint=c.operationContext(request,facts,f,root).canonicalFingerprintSha256
        val w=c.witness(request,s,p,t,f,x,root)
        if !scope.exactlyEquals(s) || !preimage.exactlyEquals(p) || !projectedTarget.exactlyEquals(t) ||
          !fingerprint.exactlyEquals(f) || !context.exactlyEquals(x) || !witness.exactlyEquals(w) then
          throw new SQLDataException("DB-first scope/effect/fingerprint/context/witness recomputation differs")
        val row=candidate.relation("fp_recovery_operation").rows.find(r =>
          MySqlDbFirstInitialActivationMaterializer.uuidEquals(MySqlDbFirstInitialActivationMaterializer.value(r,"fp_recovery_operation","operation_uuid",before.inputs),request.tag.operationId))
          .getOrElse(throw new SQLDataException("DB-first original operation missing"))
        val operationShape=before.inputs.forTable("fp_recovery_operation")
        operationShape.columns.zipWithIndex.foreach { (column,index) =>
          val expected=column.name match
            case "operation_state" => MySqlFormat3Cell.Text("RESERVED")
            case "result_at" | "commit_witness_bytes" | "commit_witness_sha256" => MySqlFormat3Cell.SqlNull
            case _ => row(index)
          if reservedOperation(index)!=expected then throw new SQLDataException("DB-first reserved O differs from same final operation")
        }
        val finalReservation=candidate.relation("fp_recovery_reservation").rows.find(r=>
          MySqlDbFirstInitialActivationMaterializer.uuidEquals(MySqlDbFirstInitialActivationMaterializer.value(r,"fp_recovery_reservation","resource_pod_uuid",before.inputs),request.podUuid))
          .getOrElse(throw new SQLDataException("DB-first POD reservation missing"))
        before.inputs.forTable("fp_recovery_reservation").columns.zipWithIndex.foreach { (column,index) =>
          val expected=column.name match
            case "reservation_state" => MySqlFormat3Cell.Text("HELD")
            case "reservation_revision" => MySqlFormat3Cell.SignedInteger(facts.heldReservationRevision)
            case _ => finalReservation(index)
          if heldReservation(index)!=expected then throw new SQLDataException("DB-first HELD reservation differs from same released R")
        }
        if MySqlDbFirstInitialActivationMaterializer.value(finalReservation,"fp_recovery_reservation","reservation_revision",before.inputs)!=
          MySqlFormat3Cell.SignedInteger(Math.addExact(facts.heldReservationRevision,1L)) then
          throw new SQLDataException("DB-first release must advance HELD reservation revision")
        def matchesFingerprint(cell: MySqlFormat3Cell)=cell match
          case MySqlFormat3Cell.Binary(bytes) => bytes.equalsVector(genericFingerprint)
          case _ => false
        if !matchesFingerprint(MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation","operation_fingerprint",before.inputs)) ||
          !matchesFingerprint(MySqlDbFirstInitialActivationMaterializer.value(candidate.relation("fp_runtime_writer").rows.head,"fp_runtime_writer","last_commit_fingerprint",before.inputs)) then
          throw new SQLDataException("DB-first O/W generic context fingerprint differs")
        val expected=Vector("scope_bytes"->s,"preimage_bytes"->p,"target_bytes"->t,
          "original_context_bytes"->x,"commit_witness_bytes"->w)
        expected.foreach { (name,bytes) =>
          if !bytes.exactlyMatchesCell(MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation",name,before.inputs)) then
            throw new SQLDataException("DB-first persisted derived field differs: "+name)
        }
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
        active.releaseTemporaryMemory(16384L,"DB-first verification fixed structures detached")))
    }

private[mysql] object MySqlDbFirstInitialActivationMaterializer:
  private val O="fp_recovery_operation"
  private val R="fp_recovery_reservation"
  private val D="floating_pod_state"
  private val L="physical_recovery_locator"
  private val W="fp_runtime_writer"
  private val S=MySqlDbFirstSchemaDefinition.SnapshotTable
  private val Null=MySqlFormat3Cell.SqlNull
  private def fail(reason: String): Nothing=throw new SQLDataException(reason)
  private[mysql] def value(row: Vector[MySqlFormat3Cell],table: String,name: String,inputs: MySqlScriptSchemaInputs): MySqlFormat3Cell =
    val index=inputs.forTable(table).columns.indexWhere(_.name==name)
    if index < 0 || row.size != inputs.forTable(table).columns.size then fail("DB-first exact row shape "+table+"."+name)
    row(index)
  private[mysql] def uuidEquals(cell: MySqlFormat3Cell,id: UUID): Boolean=cell match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
    case _ => false
  private def number(cell: MySqlFormat3Cell): Long=cell match
    case MySqlFormat3Cell.SignedInteger(n) => n
    case _ => fail("DB-first integer DATA required")
  private def datetime(cell: MySqlFormat3Cell): LocalDateTime=cell match
    case MySqlFormat3Cell.DateTime(t) => t
    case _ => fail("DB-first DATETIME6 DATA required")
  private def micros(t: LocalDateTime): Boolean=t != null && t.getYear>=1000 && t.getYear<=9999 && t.getNano%1000==0
  private def signed(n: Long)=MySqlFormat3Cell.SignedInteger(n)
  private def text(s: String)=MySqlFormat3Cell.Text(s)
  private def binary(bytes: Vector[Byte],root: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    root.reserveRetainedMemory(Math.addExact(bytes.size.toLong,64L),"DB-first immutable vector backing BEFORE allocation")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray,root))
  private def uuid(id: UUID,root: MySqlFormat3ReadBudget): MySqlFormat3Cell =
    root.reserveRetainedMemory(80L,"DB-first UUID backing BEFORE allocation")
    val bytes=new Array[Byte](16)
    var i=0
    while i<8 do
      bytes(i)=(id.getMostSignificantBits >>> (56-i*8)).toByte
      bytes(i+8)=(id.getLeastSignificantBits >>> (56-i*8)).toByte
      i+=1
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,root))
  private def hash(bytes: MySqlRecoveryCanonicalBytes,root: MySqlFormat3ReadBudget)=binary(bytes.sha256,root)
  private def row(table: String,fields: Map[String,MySqlFormat3Cell],inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(fields.size==shape.columns.size && shape.columns.forall(c=>fields.contains(c.name)),"DB-first complete row columns required")
    root.reserveRetainedMemory(256L+96L*shape.columns.size,"DB-first typed row BEFORE allocation")
    shape.columns.map(c=>fields(c.name))
  private def update(old: Vector[MySqlFormat3Cell],table: String,fields: Map[String,MySqlFormat3Cell],
      inputs: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val shape=inputs.forTable(table)
    require(fields.keys.forall(n=>shape.columns.exists(_.name==n)),"DB-first update column")
    root.reserveRetainedMemory(256L+96L*shape.columns.size,"DB-first updated row BEFORE allocation")
    shape.columns.zip(old).map((c,v)=>fields.getOrElse(c.name,v))
  private def put(relations: Vector[MySqlFormat3RelationSnapshot],table: String,replacement: Vector[MySqlFormat3Cell],
      inputs: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget): Vector[MySqlFormat3RelationSnapshot] =
    val shape=inputs.forTable(table)
    val old=relations.find(_.table==table).getOrElse(fail("DB-first missing table"))
    root.reserveRetainedMemory(2048L+64L*(old.rows.size.toLong+1L),"DB-first complete relation replacement workspace BEFORE allocation")
    def sameKey(r: Vector[MySqlFormat3Cell])=shape.primaryKey.forall(n=>value(r,table,n,inputs)==value(replacement,table,n,inputs))
    if old.rows.count(sameKey)>1 then fail("DB-first duplicate primary key")
    val rows=if old.rows.exists(sameKey) then old.rows.map(r=>if sameKey(r) then replacement else r) else old.rows :+ replacement
    val ordered=rows.sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,shape)<0)
    relations.map(r=>if r.table==table then MySqlFormat3RelationSnapshot(table,ordered) else r)

  private def copyBefore(source: MySqlScriptSchemaSnapshot,root: MySqlFormat3ReadBudget,
      draft: MySqlDbFirstInitialActivationDraft): MySqlScriptSchemaSnapshot =
    root.reserveRetainedMemory(4096L+source.rowCount*256L,"DB-first copied full27 row containers BEFORE allocation")
    draft.construction=source.relations.map { relation =>
      val rows=relation.rows.map { original =>
        root.reserveRetainedMemory(256L+96L*original.size,"DB-first copied row cells BEFORE allocation")
        original.map {
          case MySqlFormat3Cell.Binary(bytes) =>
            MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.copyBytes(root,"DB-first before binary destination copy"),root))
          case cell @ MySqlFormat3Cell.Text(value) =>
            root.reserveRetainedMemory(MySqlFormat3Utf8.byteLength(value),"DB-first retained before text")
            cell
          case cell => cell
        }
      }
      MySqlFormat3RelationSnapshot(relation.table,rows)
    }
    val image=MySqlScriptSchemaSnapshot.captureMaterialized(source.schema,source.connection,source.observationScope,
      source.maxAllowedPacketBytes,source.inputs,draft.construction,root)
    draft.before=image
    image

  def materialize(request: MySqlDbFirstInitialActivationRequest,captured: DbFirstPodSnapshot,
      backing: MySqlRepositoryRequestIdentity,source: MySqlScriptSchemaSnapshot,
      connectionId: Long,barrierGeneration: UUID,plannedAt: LocalDateTime,resultAt: LocalDateTime,
      draft: MySqlDbFirstInitialActivationDraft, activeLimit: Int): MySqlDbFirstInitialActivationMaterialization =
    val root=draft.root
    require(source != null && source.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "DB-first materialization requires complete Full27 source")
    val inputs=source.inputs
    root.requireScriptDataInputs(inputs)
    require(!source.allocationBudget.isClosed && source.allocationBudget.sharesAllocationLedger(root) &&
      (request.profileSha256==inputs.sourceLimitProfile.sha256().toVector) && (captured.limitProfile() eq root.profile),
      "DB-first source/root/capture exact allocation family")
    require(connectionId>0L && barrierGeneration!=null && barrierGeneration!=new UUID(0L,0L) &&
      micros(plannedAt) && micros(resultAt) && plannedAt.toEpochSecond(ZoneOffset.UTC)>=0L && !resultAt.isBefore(plannedAt),"explicit DB-first connection facts and ordered DATETIME6 required")
    require(captured.purpose()==DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION &&
      captured.initialCreationOperationUuid()==request.creationOperationUuid &&
      captured.operationUuid()==request.tag.operationId && captured.bindingUuid()==request.bindingUuid &&
      captured.podUuid()==request.podUuid && captured.attemptUuid()==request.attemptUuid &&
      captured.stateVersion()==request.capturedStateVersion && captured.byteLength()==request.nbtByteLength,
      "DB-first original captured snapshot differs")
    require(backing.canonical.length==request.identity.canonicalLength && backing.sha256==request.identity.sha256 &&
      backing.canonical.borrowedBinary.equalsVector(request.identity.canonicalBytes),"DB-first original request backing differs")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val before=copyBefore(source,root,draft)
      root.reserveRetainedMemory(captured.byteLength().toLong+64L,"DB-first captured DATA copy BEFORE allocation")
      val bytes=new Array[Byte](captured.byteLength())
      var index=0
      while index<bytes.length do
        bytes(index)=captured.byteAt(index)
        index+=1
      val nbt=MySqlFormat3Binary.takeOwned(bytes,root)
      val facts=buildData(request,backing.canonical.borrowedBinary,nbt,before,connectionId,barrierGeneration,
        plannedAt,resultAt,root,draft,activeLimit)
      draft.result=new MySqlDbFirstInitialActivationMaterialization(request,facts,before,draft.target,draft.scope,draft.preimage,
        draft.projectedTarget,draft.fingerprint,draft.context,draft.witness,draft.heldReservation,draft.reservedOperation,root)
      draft.result.verify()
      draft.result
    }

  /** 纯DATA重算共用路径；caller分别负责真capture或persisted解码，不建立执行资格。 */
  private[mysql] def buildData(request: MySqlDbFirstInitialActivationRequest, requestCanonical: MySqlFormat3Binary,
      nbtInput: MySqlFormat3Binary, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime,
      originalRoot: MySqlFormat3ReadBudget, draft: MySqlDbFirstInitialActivationDraft, activeLimit: Int): MySqlDbFirstInitialActivationFacts =
    val root=draft.root
    val inputs=before.inputs
    require(root != null && !root.isClosed && originalRoot != null && !originalRoot.isClosed &&
      ((root eq originalRoot) || root.isCanonicalComparisonChildOf(originalRoot)) && root.sharesAllocationLedger(originalRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq root),
      "DB-first DATA builder requires the actual original allocation family")
    originalRoot.requireScriptDataInputs(inputs)
    root.requireScriptDataInputs(inputs)
    require(connectionId>0L && barrierGeneration!=null && barrierGeneration!=new UUID(0L,0L) &&
      micros(plannedAt) && micros(resultAt) && plannedAt.toEpochSecond(ZoneOffset.UTC)>=0L && !resultAt.isBefore(plannedAt),
      "explicit DB-first connection facts and ordered DATETIME6 required")
    require(requestCanonical.equalsVector(request.identity.canonicalBytes) && nbtInput.length==request.nbtByteLength,
      "DB-first DATA request/NBT length differs")
    root.reserveTemporaryMemory(256L,"DB-first DATA NBT digest workspace")
    var primary: Throwable=null
    try
      val digest=java.security.MessageDigest.getInstance("SHA-256")
      var i=0
      while i<nbtInput.length do
        digest.update(nbtInput.byteAt(i));i+=1
      if digest.digest().toVector!=request.nbtSha256 then fail("DB-first DATA NBT digest differs")
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => root.releaseTemporaryMemory(256L,"DB-first DATA NBT digest detached")))
    root.reserveRetainedMemory(65536L,"DB-first fixed maps/facts/materialization metadata BEFORE construction")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      def v(r: Vector[MySqlFormat3Cell],t: String,n: String)=value(r,t,n,inputs)
      def one(table: String,predicate: Vector[MySqlFormat3Cell]=>Boolean): Vector[MySqlFormat3Cell] =
        var found: Vector[MySqlFormat3Cell]=null
        before.relation(table).rows.foreach { r =>
          if predicate(r) then
            if found!=null then fail("DB-first duplicate original row "+table)
            found=r
        }
        if found==null then fail("DB-first missing original row "+table)
        found
      require(activeLimit>=1 && activeLimit<=256,"actual configured active limit required")
      if before.relation(D).rows.count(r=>number(v(r,D,"is_active"))==1L)>=activeLimit then
        fail("initial activation exceeds original configured active capacity")
      val domain=one(D,r=>uuidEquals(v(r,D,"uuid"),request.podUuid))
      val locator=one(L,r=>uuidEquals(v(r,L,"binding_uuid"),request.bindingUuid) && uuidEquals(v(r,L,"pod_uuid"),request.podUuid))
      val writer=one(W,_=>true)
      val manifest=one("fp_schema_manifest",_=>true)
      val binding=one("physical_recovery_binding",_=>true)
      val lease=datetime(v(writer,W,"lease_until"))
      val epoch=number(v(writer,W,"writer_epoch"))
      val version=request.capturedStateVersion
      val revision=number(v(locator,L,"locator_revision"))
      if version<0L || epoch<=0L || revision<0L || !micros(lease) || !resultAt.isBefore(lease) ||
        !uuidEquals(v(writer,W,"binding_uuid"),request.bindingUuid) || !uuidEquals(v(writer,W,"owner_boot_uuid"),request.tag.bootId) ||
        !uuidEquals(v(binding,"physical_recovery_binding","binding_uuid"),request.bindingUuid) ||
        v(binding,"physical_recovery_binding","cutover_state")!=text("CERTIFIED") || number(v(binding,"physical_recovery_binding","format_version"))!=2L ||
        !uuidEquals(v(manifest,"fp_schema_manifest","expected_binding_uuid"),request.bindingUuid) ||
        number(v(manifest,"fp_schema_manifest","format_version"))!=6L || v(manifest,"fp_schema_manifest","bootstrap_state")!=text("READY") ||
        !uuidEquals(v(domain,D,"binding_uuid"),request.bindingUuid) || number(v(domain,D,"state_version"))!=version || number(v(domain,D,"is_active"))!=0L ||
        v(locator,L,"locator_state")!=text("DEBUG_ITEM_CREATE_PREPARED") || number(v(locator,L,"domain_version"))!=version || !uuidEquals(v(locator,L,"current_attempt_uuid"),request.attemptUuid) ||
        number(v(locator,L,"target_dimension_id"))!=request.pose.dimension().toLong ||
        number(v(domain,D,"dimension_id"))!=request.pose.dimension().toLong || v(domain,D,"world_name")!=text(request.pose.worldName()) ||
        v(domain,D,"owner_uuid")!=request.ownerUuid.map(uuid(_,root)).getOrElse(Null) ||
        before.observationScope.bootUuid!=request.tag.bootId || before.observationScope.bootGeneration!=request.tag.bootGeneration ||
        before.observationScope.connectionGeneration!=request.tag.connectionGeneration then fail("DB-first before authority DATA/capture identity mismatch")
      val intent=one("physical_recovery_intent",r=>uuidEquals(v(r,"physical_recovery_intent","binding_uuid"),request.bindingUuid) &&
        uuidEquals(v(r,"physical_recovery_intent","pod_uuid"),request.podUuid) &&
        uuidEquals(v(r,"physical_recovery_intent","attempt_uuid"),request.attemptUuid))
      val preparation=one(O,r=>v(r,O,"binding_uuid")==v(intent,"physical_recovery_intent","binding_uuid") &&
        v(r,O,"operation_uuid")==v(intent,"physical_recovery_intent","prepare_operation_uuid"))
      if version!=0L || request.ownerUuid.nonEmpty || v(domain,D,"operating_mode")!=text("WORLD_IDLE") ||
        v(intent,"physical_recovery_intent","phase")!=text("PREPARED") ||
        v(intent,"physical_recovery_intent","origin")!=text("DEBUG_ITEM_CREATE") ||
        number(v(intent,"physical_recovery_intent","expected_domain_version"))!=0L ||
        number(v(intent,"physical_recovery_intent","intent_revision"))!=0L ||
        v(intent,"physical_recovery_intent","expected_owner_uuid")!=Null ||
        !uuidEquals(v(intent,"physical_recovery_intent","create_operation_uuid"),request.creationOperationUuid) ||
        v(preparation,O,"step")!=text("REARM") || v(preparation,O,"operation_state")!=text("COMMITTED") ||
        !uuidEquals(v(preparation,O,"pod_uuid"),request.podUuid) || !uuidEquals(v(preparation,O,"attempt_uuid"),request.attemptUuid) ||
        v(domain,D,"last_mutation_operation_uuid")!=v(preparation,O,"operation_uuid") ||
        v(locator,L,"last_mutation_operation_uuid")!=v(preparation,O,"operation_uuid") ||
        v(intent,"physical_recovery_intent","last_mutation_operation_uuid")!=v(preparation,O,"operation_uuid") ||
        v(intent,"physical_recovery_intent","target_footprint_uuid")!=v(locator,L,"current_footprint_uuid") then
        fail("initial activation lacks exact original prepared creation lineage")
      if before.relation(S).rows.exists(r=>uuidEquals(v(r,S,"binding_uuid"),request.bindingUuid) &&
        uuidEquals(v(r,S,"pod_uuid"),request.podUuid)) then fail("initial activation cannot overwrite an existing snapshot")
      val footprint=one("physical_recovery_footprint",r=>
        v(r,"physical_recovery_footprint","binding_uuid")==v(locator,L,"binding_uuid") &&
        v(r,"physical_recovery_footprint","pod_uuid")==v(locator,L,"pod_uuid") &&
        v(r,"physical_recovery_footprint","attempt_uuid")==v(locator,L,"current_attempt_uuid") &&
        v(r,"physical_recovery_footprint","footprint_uuid")==v(locator,L,"current_footprint_uuid"))
      if v(footprint,"physical_recovery_footprint","seal_state")!=text("SEALED") ||
        v(footprint,"physical_recovery_footprint","purpose")!=text("CREATE_TARGET") ||
        number(v(footprint,"physical_recovery_footprint","origin_version"))!=0L ||
        v(footprint,"physical_recovery_footprint","created_operation_uuid")!=v(preparation,O,"operation_uuid") then fail("initial activation requires original sealed target")
      val poseFields=Vector("world_name"->text(request.pose.worldName()),"dimension_id"->signed(request.pose.dimension()),
        "pos_x"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(request.pose.x())),
        "pos_y"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(request.pose.y())),
        "pos_z"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(request.pose.z())),
        "yaw"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(request.pose.yaw().toDouble)),
        "pitch"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(request.pose.pitch().toDouble)))
      poseFields.foreach { (field,expected)=>
        if v(domain,D,field)!=expected || v(footprint,"physical_recovery_footprint","target_"+field)!=expected then
          fail("initial activation captured pose differs from prepared domain/footprint")
      }
      val centerX=Math.floor(request.pose.x()/16D).toLong
      val centerZ=Math.floor(request.pose.z()/16D).toLong
      if centerX<Int.MinValue.toLong+2L || centerX>Int.MaxValue.toLong-2L ||
        centerZ<Int.MinValue.toLong+2L || centerZ>Int.MaxValue.toLong-2L then fail("initial target chunk range")
      if number(v(footprint,"physical_recovery_footprint","center_chunk_x"))!=centerX ||
        number(v(footprint,"physical_recovery_footprint","center_chunk_z"))!=centerZ then fail("initial footprint center differs from captured target")
      val chunks=scala.collection.mutable.HashSet.empty[(Long,Long)]
      before.relation("physical_recovery_footprint_chunk").rows.foreach { r=>
        val table="physical_recovery_footprint_chunk"
        if v(r,table,"binding_uuid")==v(locator,L,"binding_uuid") &&
          v(r,table,"pod_uuid")==v(locator,L,"pod_uuid") &&
          v(r,table,"attempt_uuid")==v(locator,L,"current_attempt_uuid") &&
          v(r,table,"footprint_uuid")==v(locator,L,"current_footprint_uuid") then
          val x=number(v(r,table,"chunk_x"));val z=number(v(r,table,"chunk_z"))
          if number(v(r,table,"dimension_id"))!=request.pose.dimension().toLong ||
            x<centerX-2L || x>centerX+2L || z<centerZ-2L || z>centerZ+2L || !chunks.add((x,z)) then
            fail("initial target must contain exact original 5x5 chunks")
      }
      if chunks.size!=25 then fail("initial target lacks exact original 25 chunks")
      if before.relation(O).rows.exists(r=>uuidEquals(v(r,O,"operation_uuid"),request.tag.operationId)) then fail("DB-first original O already exists")
      (v(writer,W,"last_commit_operation_uuid"),v(writer,W,"last_commit_fingerprint")) match
        case (MySqlFormat3Cell.SqlNull,MySqlFormat3Cell.SqlNull) => ()
        case (MySqlFormat3Cell.Binary(oldOperation),MySqlFormat3Cell.Binary(oldFingerprint)) =>
          val previous=one(O,r=>v(r,O,"binding_uuid")==v(writer,W,"binding_uuid") && v(r,O,"operation_uuid")==MySqlFormat3Cell.Binary(oldOperation))
          if v(previous,O,"operation_state")!=text("COMMITTED") || v(previous,O,"operation_fingerprint")!=MySqlFormat3Cell.Binary(oldFingerprint) then
            fail("DB-first before W receipt lacks its complete original committed O")
        case _ => fail("DB-first before W receipt NULL topology")
      v(manifest,"fp_schema_manifest","limit_profile_bytes") match
        case MySqlFormat3Cell.Binary(bytes) if bytes.equalsVector(request.profileBytes) => ()
        case _ => fail("DB-first manifest profile differs from capture")
      val resource=MySqlRecoveryResourceKey.pod(request.podUuid,root.profile)
      val key=resource.copyCanonicalBytes(root,"DB-first original POD resource key")
      val keyBinary=MySqlFormat3Binary.takeOwned(key,root)
      root.reserveRetainedMemory(96L,"DB-first POD resource hash backing BEFORE allocation")
      val keyHash=java.security.MessageDigest.getInstance("SHA-256").digest(key)
      root.reserveRetainedMemory(256L+64L*before.relation(R).rows.size.toLong,"DB-first reservation selection references BEFORE allocation")
      val oldReservation=before.relation(R).rows.filter(r=>uuidEquals(v(r,R,"binding_uuid"),request.bindingUuid) && uuidEquals(v(r,R,"resource_pod_uuid"),request.podUuid))
      if oldReservation.size>1 then fail("DB-first duplicate POD reservation")
      val reservationRevision=oldReservation.headOption.map { r =>
        if v(r,R,"resource_kind")!=text("POD") || v(r,R,"resource_unit_uuid")!=Null ||
          v(r,R,"resource_key")!=MySqlFormat3Cell.Binary(keyBinary) || v(r,R,"reservation_state")!=text("RELEASED") then fail("DB-first POD reservation held/identity differs")
        val previousRevision=number(v(r,R,"reservation_revision"))
        if previousRevision<0L then fail("DB-first negative original reservation revision")
        Math.addExact(previousRevision,1L)
      }.getOrElse(0L)
      val releasedRevision=Math.addExact(reservationRevision,1L)
      val facts=MySqlDbFirstInitialActivationFacts(connectionId,barrierGeneration,epoch,lease,revision,reservationRevision,releasedRevision,plannedAt,resultAt,activeLimit)
      draft.scope=MySqlDbFirstInitialActivationCanonical.scope(request,facts,before,root)
      val op=request.tag.operationId
      val next=Math.addExact(version,1L)
      val pose=request.pose
      val changedDomain=update(domain,D,Map("is_active"->signed(1),"state_version"->signed(next),
        "world_name"->text(pose.worldName()),"dimension_id"->signed(pose.dimension()),
        "pos_x"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.x())),
        "pos_y"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.y())),
        "pos_z"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.z())),
        "yaw"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.yaw().toDouble)),
        "pitch"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.pitch().toDouble)),
        "last_updated"->signed(resultAt.toInstant(ZoneOffset.UTC).toEpochMilli),"last_mutation_operation_uuid"->uuid(op,root)),inputs,root)
      val changedLocator=update(locator,L,Map("domain_version"->signed(next),"locator_revision"->signed(Math.addExact(revision,1L)),
        "locator_state"->text("ROW_ACTIVE"),"reason_code"->text("ROW_ACTIVE"),
        "last_mutation_operation_uuid"->uuid(op,root)),inputs,root)
      val changedIntent=MySqlControlledDebugRows.activatedIntent(intent,changedDomain,op,root)
      val reservation=row(R,Map("binding_uuid"->uuid(request.bindingUuid,root),"resource_kind"->text("POD"),
        "resource_key_hash"->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(keyHash,root)),"resource_key"->MySqlFormat3Cell.Binary(keyBinary),
        "reservation_revision"->signed(releasedRevision),"workflow_uuid"->uuid(op,root),"operation_uuid"->uuid(op,root),
        "reservation_state"->text("RELEASED"),"last_mutation_operation_uuid"->uuid(op,root),
        "resource_pod_uuid"->uuid(request.podUuid,root),"resource_unit_uuid"->Null),inputs,root)
      draft.heldReservation=update(reservation,R,Map("reservation_state"->text("HELD"),"reservation_revision"->signed(reservationRevision)),inputs,root)
      val snapshot=row(S,Map("binding_uuid"->uuid(request.bindingUuid,root),"pod_uuid"->uuid(request.podUuid,root),
        "owner_uuid"->request.ownerUuid.map(uuid(_,root)).getOrElse(Null),"attempt_uuid"->uuid(request.attemptUuid,root),
        "snapshot_format_version"->signed(request.snapshotFormatVersion),"nbt_bytes"->MySqlFormat3Cell.Binary(nbtInput),
        "nbt_sha256"->binary(request.nbtSha256,root),"limit_profile_encoding"->text(SourceLimitProfile.ENCODING),
        "limit_profile_bytes"->binary(request.profileBytes,root),"limit_profile_sha256"->binary(request.profileSha256,root),
        "captured_state_version"->signed(version),"committed_state_version"->signed(next),"last_mutation_operation_uuid"->uuid(op,root)),inputs,root)
      // 只有非持久construction允许derived单元尚未填充；不进入完整history validator。
      val operation=row(O,Map(
        "source_unit_uuid"->Null,"forest_uuid"->Null,"expected_source_revision"->Null,
        "reserve_operation_uuid"->Null,"previous_step_operation_uuid"->Null,"settled_by_operation_uuid"->Null,
        "original_diagnostic_identity"->Null,"original_diagnostic_sha256"->Null,
        "original_context_bytes"->Null,"original_context_sha256"->Null,"preimage_bytes"->Null,"preimage_sha256"->Null,
        "target_bytes"->Null,"target_sha256"->Null,"operation_fingerprint"->Null,"commit_witness_bytes"->Null,"commit_witness_sha256"->Null,
        "binding_uuid"->uuid(request.bindingUuid,root),"operation_uuid"->uuid(op,root),"workflow_uuid"->uuid(op,root),
        "step"->text(MySqlDbFirstSchemaDefinition.InitialActivationStep),"operation_state"->text("COMMITTED"),
        "pod_uuid"->uuid(request.podUuid,root),"attempt_uuid"->uuid(request.attemptUuid,root),"footprint_uuid"->v(locator,L,"current_footprint_uuid"),
        "boot_uuid"->uuid(request.tag.bootId,root),"boot_generation"->signed(request.tag.bootGeneration),
        "connection_generation_uuid"->uuid(barrierGeneration,root),"connection_id"->signed(connectionId),"fence_epoch"->signed(epoch),
        "request_identity_bytes"->MySqlFormat3Cell.Binary(requestCanonical),"request_identity_sha256"->binary(request.identity.sha256,root),
        "expected_domain_version"->signed(version),"expected_locator_revision"->signed(revision),
        "context_encoding"->text("FP_RECOVERY_OPERATION_BE_V1"),"scope_bytes"->draft.scope.binaryCell,"scope_sha256"->hash(draft.scope,root),
        "planned_at"->MySqlFormat3Cell.DateTime(plannedAt),"fixed_lease_until"->MySqlFormat3Cell.DateTime(lease),"result_at"->MySqlFormat3Cell.DateTime(resultAt)),inputs,root)
      var rows=before.relations
      Vector(D->changedDomain,L->changedLocator,"physical_recovery_intent"->changedIntent,R->reservation,S->snapshot,O->operation,
        W->update(writer,W,Map("last_commit_operation_uuid"->uuid(op,root),"last_commit_fingerprint"->Null),inputs,root)).foreach { (t,r) => rows=put(rows,t,r,inputs,root) }
      draft.construction=rows
      draft.preimage=MySqlDbFirstInitialActivationCanonical.projection(request,before,before.relations,false,root)
      draft.projectedTarget=MySqlDbFirstInitialActivationCanonical.projection(request,before,rows,true,root)
      draft.fingerprint=MySqlDbFirstInitialActivationCanonical.fingerprint(request,draft.scope,draft.preimage,draft.projectedTarget,root)
      draft.context=MySqlDbFirstInitialActivationCanonical.context(request,facts,draft.fingerprint,root)
      draft.witness=MySqlDbFirstInitialActivationCanonical.witness(request,draft.scope,draft.preimage,draft.projectedTarget,draft.fingerprint,draft.context,root)
      var finalO=operation
      Vector("original_context"->draft.context,"preimage"->draft.preimage,"target"->draft.projectedTarget,"commit_witness"->draft.witness).foreach { (prefix,bytes) =>
        finalO=update(finalO,O,Map((prefix+"_bytes")->bytes.binaryCell,(prefix+"_sha256")->hash(bytes,root)),inputs,root)
      }
      val genericFingerprint=MySqlDbFirstInitialActivationCanonical.operationContext(request,facts,draft.fingerprint,root).canonicalFingerprintSha256
      finalO=update(finalO,O,Map("operation_fingerprint"->binary(genericFingerprint,root)),inputs,root)
      draft.reservedOperation=update(finalO,O,Map("operation_state"->text("RESERVED"),"result_at"->Null,
        "commit_witness_bytes"->Null,"commit_witness_sha256"->Null),inputs,root)
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(draft.reservedOperation,root.profile,root,Some(inputs))
      rows=put(rows,O,finalO,inputs,root)
      rows=put(rows,W,update(writer,W,Map("last_commit_operation_uuid"->uuid(op,root),"last_commit_fingerprint"->binary(genericFingerprint,root)),inputs,root),inputs,root)
      draft.construction=rows
      draft.target=if root eq originalRoot then
        MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
          before.maxAllowedPacketBytes,inputs,rows,root)
      else MySqlScriptSchemaSnapshot.decodedForComparison(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,inputs,rows,originalRoot,root)
      facts

    }
