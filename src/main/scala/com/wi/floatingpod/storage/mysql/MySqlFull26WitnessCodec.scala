package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1Full26WitnessReference, B1ResumeRecord, B1ResumeRecordCanonical, WindowsWorldProvisionReceiptDataCodec}
import java.io.{DataOutputStream, EOFException, IOException, OutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.{Arrays, HexFormat, UUID}

/** 完整 Full26 body DATA；没有 Original producer、native、SQL、Fresh 或 Ready 资格。 */
private[mysql] object MySqlFull26WitnessCodec:
  val Domain = "FP_B1_FULL26_ORIGINAL_WITNESS_BE_V1"
  private val DomainBytes = Domain.getBytes(StandardCharsets.US_ASCII)
  private val HolderFee = 512L

  /** 调用方先持有此 permanent partial owner，再开始任何 body/context/image 分配。
    * Wire: BE int32+ASCII domain, int32+canonical S97, int32+schema5 context,
    * int32+whole before, int32+whole target, BE int32 framing version1，严格 EOF。
    */
  def begin(inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Owner =
    requireRoot(inputs, root)
    root.reserveRetainedMemory(HolderFee, "Full26 caller-retained body partial owner BEFORE allocation")
    try new Owner(inputs, root)
    catch
      case failure: Throwable =>
        try root.releaseRetainedMemory(HolderFee, "unpublished Full26 body owner")
        catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  final class Owner private[MySqlFull26WitnessCodec] (
      private val inputs: MySqlScriptSchemaInputs, private val root: MySqlFormat3ReadBudget
  ) extends AutoCloseable:
    private val thread = Thread.currentThread()
    private var attempted = false
    private var ready = false
    private var closed = false
    private var active = false
    private var primary: Throwable = null
    private var cleanupFailure: Throwable = null
    private var storage: MySqlFormat3ReadBudget = null
    private var beforePayload: MySqlScriptSchemaCanonicalPayload = null
    private var targetPayload: MySqlScriptSchemaCanonicalPayload = null
    private var before: MySqlScriptSchemaSnapshot = null
    private var target: MySqlScriptSchemaSnapshot = null
    private var beforeGraph: MySqlB1CommonGraph = null
    private var targetGraph: MySqlB1CommonGraph = null
    private var record: B1ResumeRecord = null
    private var body: MySqlFormat3Binary = null
    private var context: MySqlFormat3Binary = null
    private var beforePart: MySqlScriptSchemaCanonicalPayload = null
    private var targetPart: MySqlScriptSchemaCanonicalPayload = null
    private var fullHash: Array[Byte] = null
    private var configuration: Array[Byte] = null
    private var ddl: Array[Byte] = null
    private var pendingCleanupBudgets:Vector[MySqlFormat3ReadBudget]=Vector.empty
    private var historicalCertify:Option[MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5]=None

    def failure: Option[Throwable] = Option(primary)
    def failedCleanup: Option[Throwable] = Option(cleanupFailure)
    def hasUnsettledResources: Boolean = !closed
    private def requireThread(): Unit =
      if Thread.currentThread() ne thread then throw new IllegalStateException("exact Full26 body owner thread required")
    private def requireLive(): Unit =
      requireThread(); requireRoot(inputs, root)
      if closed || cleanupFailure != null then throw new IllegalStateException("Full26 body owner closed or cleanup failed", cleanupFailure)
    private def start(): Unit =
      requireLive()
      if attempted then throw new IllegalStateException("Full26 body owner is single-use", primary)
      attempted = true
      storage = root.newCanonicalDataScope()
      storage.reserveRetainedMemory(4096L, "Full26 body reader/result/control structures BEFORE construction")
    private def attempt(action: => Unit): Unit =
      requireLive()
      if attempted then throw new IllegalStateException("Full26 body owner is single-use", primary)
      try
        start()
        action
        ready = true
      catch
        case failure: Throwable => primary = failure; throw failure

    /** 编码后也严格 cold decode，发布的是 owner 自有 decoded images，不借出 caller 的图。 */
    def encode(original: B1ResumeRecord, actualBefore: MySqlScriptSchemaSnapshot,
        actualTarget: MySqlScriptSchemaSnapshot): Unit = attempt {
      requireSnapshot(actualBefore, inputs, root); requireSnapshot(actualTarget, inputs, root)
      temporary(storage, 12L * 65536L + 32768L, "schema5 context encoder defensive peak") {
        val encoded = B1ResumeRecordCanonical.encodeSchema5Data(original)
        context = MySqlFormat3Binary.copyOf(encoded, storage, "Full26 encoded context immutable bytes")
      }
      beforePayload = MySqlScriptSchemaSnapshotCodec.encode(actualBefore, root)
      targetPayload = MySqlScriptSchemaSnapshotCodec.encode(actualTarget, root)
      body = encodeBody(context, beforePayload, targetPayload, inputs, storage)
      decodeBody()
    }

    private[mysql] def encodeWithHistoricalCertify(original:B1ResumeRecord,actualBefore:MySqlScriptSchemaSnapshot,
        actualTarget:MySqlScriptSchemaSnapshot,
        proof:MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5):Unit =
      requireLive()
      if attempted || proof==null then throw new IllegalStateException("original one-shot historical CERTIFY encoder required")
      historicalCertify=Some(proof)
      encode(original,actualBefore,actualTarget)

    /** input 仍属于调用方；owner 在分配第一份 retained bytes 前已登记 storage。 */
    def decode(input: Array[Byte]): Unit = attempt {
      if input == null || input.length <= 0 || input.length.toLong > inputs.sourceLimitProfile.maxNativeArtifactBytes() then
        reject("bounded complete Full26 body required")
      body = MySqlFormat3Binary.copyOf(input, storage, "Full26 cold immutable body defensive bytes")
      decodeBody()
    }

    private[mysql] def decodeWithHistoricalCertify(input:Array[Byte],
        proof:Option[MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5]):Unit =
      requireLive()
      if attempted || proof==null || proof.exists(_==null) then throw new IllegalStateException("original one-shot historical body decode required")
      historicalCertify=proof
      decode(input)

    private def decodeBody(): Unit =
      val reader = new Reader(body, storage)
      val domain = reader.blob(128L, "Full26 body domain")
      if !domain.equalsBytes(DomainBytes) then reject("unknown Full26 body domain")
      val s = reader.blob(97L, "Full26 body canonical S")
      temporary(storage, 192L, "Full26 expected S bytes/hash") {
        if s.length != 97 || !s.equalsBytes(inputs.sourceLimitProfile.canonicalBytes()) then reject("Full26 body S differs from exact inputs")
      }
      context = reader.blob(65536L, "Full26 complete schema5 context")
      val retainedContext = Math.addExact(16384L, Math.multiplyExact(context.length.toLong, 8L))
      storage.reserveRetainedMemory(retainedContext, "Full26 context decoder owned arrays/strings/DTOs BEFORE decode")
      temporary(storage, 12L * context.length.toLong + 32768L, "Full26 strict context decode/reencode peak") {
        val bytes = context.copyBytes(storage, "Full26 context decoder argument")
        var cause: Throwable = null
        try record = B1ResumeRecordCanonical.decodeSchema5Data(bytes)
        catch
          case failure: Throwable =>
            cause = failure
            throw failure
        finally
          Arrays.fill(bytes, 0.toByte)
          MySqlFormat3BudgetCleanup.run(cause, Vector(() => storage.releaseRetainedMemory(context.length.toLong + 64L, "Full26 context argument dropped")))
      }
      val beforeBytes = reader.blob(inputs.budget.snapshotBytes, "Full26 whole before canonical")
      val targetBytes = reader.blob(inputs.budget.snapshotBytes, "Full26 whole target canonical")
      if reader.int() != 1 || !reader.atEnd then reject("Full26 body framing version or EOF differs")
      storage.reserveRetainedMemory(256L, "Full26 paid immutable payload holders")
      beforePart = new MySqlScriptSchemaCanonicalPayload(beforeBytes, inputs, root, storage)
      targetPart = new MySqlScriptSchemaCanonicalPayload(targetBytes, inputs, root, storage)
      before = MySqlScriptSchemaSnapshotCodec.decode(beforePart, inputs, root)
      target = MySqlScriptSchemaSnapshotCodec.decode(targetPart, inputs, root)
      val a = record.assertions().pendingAttempt()
      if a == null then reject("Full26 body requires its actual original pending attempt; no retained-slot fallback")
      if historicalCertify.nonEmpty then
        temporary(storage,Math.addExact(Math.multiplyExact(12L,context.length.toLong),32768L),
          "original historical CERTIFY complete attempt/receipt proof workspace") {
          historicalCertify.foreach(_.requireBefore(record,before,root))
        }
      temporary(storage, 65536L, "Full26 identity and manifest comparison workspace") {
        validateIdentity(record, a, before, target, inputs, storage,historicalCertify)
        configuration = hashCell(before, "fp_schema_manifest", "configuration_sha256")
        ddl = HexFormat.of().parseHex(inputs.ddlSha256)
      }
      temporary(storage, 4L * context.length.toLong + 16384L, "Full26 graph assertion defensive getter peak") {
        beforeGraph = MySqlB1CommonGraph.reconstructDeploymentReceiptFull26Cold(before, inputs, root, a.beforeGraph())
        targetGraph = MySqlB1CommonGraph.reconstructDeploymentReceiptFull26Cold(target, inputs, root, a.targetGraph())
      }
      temporary(storage, 512L, "Full26 body hash/reencoding comparison") {
        fullHash = digest(body)
        val output = new DataOutputStream(new Compare(body))
        writeBody(output, context, beforePart, targetPart, inputs)
        output.flush()
        output.close()
      }

    private def requireReady(): Unit =
      requireLive()
      if !ready || primary != null then throw new IllegalStateException("Full26 body result unavailable", primary)

    /** 实际结构只在此同步 live scope 内消费；调用方不得保存裸图，需在自身 owner 内复制后才可离开。
      * close 在 callback 内明确拒绝；没有 Boolean-only 或假 decoded result。
      */
    def withData(consume: (B1ResumeRecord, MySqlScriptSchemaSnapshot, MySqlScriptSchemaSnapshot,
        MySqlB1CommonGraph, MySqlB1CommonGraph) => Unit): Unit =
      requireReady()
      if consume == null || active then throw new IllegalStateException("one actual Full26 body consumer at a time")
      active = true
      try consume(record, before, target, beforeGraph, targetGraph)
      finally active = false

    /** 历史phase重验的receipt/context副本先按实际transition长度计费；新图另受原root预算。 */
    private[mysql] def withBootstrapSemanticData(consume: (B1ResumeRecord, MySqlScriptSchemaSnapshot, MySqlScriptSchemaSnapshot,
        MySqlB1CommonGraph, MySqlB1CommonGraph) => Unit): Unit =
      requireReady()
      temporary(storage,Math.addExact(Math.multiplyExact(12L,context.length.toLong),32768L),
        "Full26 original bootstrap semantic receipt/context validation copies") {
        withData(consume)
      }

    def byteLength: Int = { requireReady(); body.length }
    def writeBodyTo(destination: OutputStream): Unit =
      requireReady()
      if destination == null then throw new IllegalArgumentException("actual DATA destination required")
      if active then throw new IllegalStateException("Full26 body consumer already active")
      active = true
      try body.writeTo(new DataOutputStream(destination)) // Caller owns/closes destination.
      finally active = false

    def requireReference(reference: B1Full26WitnessReference): Unit =
      requireReady()
      temporary(storage, 12L * context.length.toLong + 32768L, "Full26 complete reference comparison copies") {
        val a = record.assertions().pendingAttempt()
        if reference == null || reference.fullLength() != body.length.toLong ||
            !Arrays.equals(reference.fullSha256(), fullHash) || !Arrays.equals(reference.sourceAnchorSha256(), record.sourceAnchorSha256()) ||
            !Arrays.equals(reference.originalReceiptSha256(), MessageDigest.getInstance("SHA-256").digest(record.originalReceipt())) ||
            !Arrays.equals(reference.sourceLimitProfileSha256(), inputs.sourceLimitProfile.sha256()) ||
            !Arrays.equals(reference.configurationSha256(), configuration) || !Arrays.equals(reference.ddlSha256(), ddl) ||
            reference.attemptId() != a.attemptId() || reference.phase() != a.phase() || reference.logicalOperation() != a.logicalWriteUuid() then
          reject("Full26 complete body/reference source/receipt/attempt/C-D-S differs")
      }

    /** Partial fields detach BEFORE fees return; a failed cleanup stays observable and is never retried as success. */
    override def close(): Unit =
      requireThread()
      if active then throw new IllegalStateException("Full26 consumer still uses actual decoded structures")
      if cleanupFailure != null then throw new IllegalStateException("Full26 body cleanup failed; no automatic retry", cleanupFailure)
      if !closed then
        ready = false
        // 先把每个真实预算槽挂到 owner，再撤图；失败槽不能只剩局部 Vector。
        pendingCleanupBudgets=cleanupBudgets(beforeGraph,targetGraph,before,target,beforePayload,targetPayload)
        detachReferences()
        def settle(action: => Unit): Unit =
          try action
          catch
            case failure: Throwable =>
              if cleanupFailure == null then cleanupFailure = failure
              else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure,failure)
              if primary != null then MySqlOwnedIoAdapter.suppressExitCleanup(primary,failure)
        // These helpers return before their allocation budgets are refunded; no local graph references remain there.
        pendingCleanupBudgets=pendingCleanupBudgets.filter { budget=>
          var settled=false
          settle { budget.closeScopedReadBudget();settled=true }
          !settled
        }
        if storage != null then
          settle { storage.closeScopedReadBudget();storage=null }
        if cleanupFailure == null then settle(root.releaseRetainedMemory(HolderFee, "Full26 detached owner holder"))
        if cleanupFailure != null then throw new IllegalStateException("Full26 body cleanup incomplete", cleanupFailure)
        closed = true
    private def detachReferences(): Unit =
      beforeGraph = null; targetGraph = null; before = null; target = null
      beforePayload = null; targetPayload = null; beforePart = null; targetPart = null
      record = null; body = null; context = null; fullHash = null; configuration = null; ddl = null;historicalCertify=None
    override def toString: String = "Full26WitnessBodyOwner[DATA; no authority]"

  private def cleanupBudgets(bg: MySqlB1CommonGraph, tg: MySqlB1CommonGraph,
      b: MySqlScriptSchemaSnapshot, t: MySqlScriptSchemaSnapshot,
      bp: MySqlScriptSchemaCanonicalPayload, tp: MySqlScriptSchemaCanonicalPayload): Vector[MySqlFormat3ReadBudget] =
    Vector(if tg == null then null else tg.storageBudget.orNull, if bg == null then null else bg.storageBudget.orNull,
      if t == null then null else t.allocationBudget, if b == null then null else b.allocationBudget,
      if tp == null then null else tp.storageBudget, if bp == null then null else bp.storageBudget).filter(_ != null).distinct

  private def requireRoot(inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    if inputs == null || root == null || root.isClosed || root.isScopedReadBudget ||
        inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 || (root.profile ne inputs.sourceLimitProfile) then
      reject("exact open unscoped Full26 input/profile/root required")
    root.requireScriptDataInputs(inputs)
  private def requireSnapshot(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): Unit =
    requireRoot(inputs, root)
    if snapshot == null || (snapshot.inputs ne inputs) || snapshot.allocationBudget.isClosed ||
        (!(snapshot.allocationBudget eq root) && !snapshot.allocationBudget.isCanonicalChildOf(root)) ||
        !snapshot.allocationBudget.sharesAllocationLedger(root) then reject("Full26 image belongs to another or closed owner")
    snapshot.allocationBudget.requireScriptDataInputs(inputs)

  private def validateIdentity(record: B1ResumeRecord, a: B1ResumeRecord.AttemptAssertions,
      before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget,
      historicalCertify:Option[MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5]): Unit =
    val receipt = WindowsWorldProvisionReceiptDataCodec.decode(record.originalReceipt())
    val plan = receipt.plan()
    if plan.schemaFormat() != 5 || plan.physicalFormat() != 2 || plan.schema() != inputs.schema ||
        !plan.ddlSha256().equalsIgnoreCase(inputs.ddlSha256) then reject("Full26 body differs from its actual schema5 PlanData")
    for image <- Vector(before, target) do
      if image.schema != plan.schema() || image.connection.catalog != plan.schema() || image.connection.serverUuid != plan.mysqlServerUuid() then
        reject("Full26 body header differs from receipt server/schema")
      if !Arrays.equals(hashCell(image,"fp_schema_manifest","ddl_sha256"), HexFormat.of().parseHex(inputs.ddlSha256)) ||
          !binaryCell(image,"fp_schema_manifest","expected_server_uuid").equalsUuid(plan.mysqlServerUuid()) ||
          !binaryCell(image,"fp_schema_manifest","expected_binding_uuid").equalsUuid(plan.bindingUuid()) ||
          !binaryCell(image,"fp_schema_manifest","provisioning_uuid").equalsUuid(plan.provisioningUuid()) then
        reject("Full26 actual manifest differs from original receipt D/server/binding/provisioning")
      val intent = image.relation("binding_bootstrap_intent")
      if intent.rows.nonEmpty then
        if !binaryCell(image,"binding_bootstrap_intent","bootstrap_operation_uuid").equalsUuid(plan.bootstrapOperationUuid()) ||
            !binaryCell(image,"binding_bootstrap_intent","prepare_write_uuid").equalsUuid(plan.prepareWriteUuid()) ||
            !binaryCell(image,"binding_bootstrap_intent","certify_write_uuid").equalsUuid(plan.certifyWriteUuid()) ||
            !binaryCell(image,"binding_bootstrap_intent","binding_uuid").equalsUuid(plan.bindingUuid()) ||
            !binaryCell(image,"binding_bootstrap_intent","provisioning_uuid").equalsUuid(plan.provisioningUuid()) ||
            !binaryCell(image,"binding_bootstrap_intent","provision_receipt_sha256").equalsBytes(MessageDigest.getInstance("SHA-256").digest(record.originalReceipt())) then
          reject("Full26 B1 intent differs from complete original receipt plan/receipt SHA")
      val profile = inputs.sourceLimitProfile
      if cell(image,"fp_schema_manifest","limit_profile_encoding") != MySqlFormat3Cell.Text(com.wi.floatingpod.config.SourceLimitProfile.ENCODING) ||
          !binaryCell(image,"fp_schema_manifest","limit_profile_bytes").equalsBytes(profile.canonicalBytes()) ||
          !binaryCell(image,"fp_schema_manifest","limit_profile_sha256").equalsBytes(profile.sha256()) then
        reject("Full26 actual manifest S bytes/hash differs from exact profile")
      Vector("max_region_bytes"->profile.maxRegionBytes(),"max_chunk_bytes"->profile.maxChunkBytes(),
        "max_decompressed_bytes"->profile.maxDecompressedBytes(),"max_nbt_depth"->profile.maxNbtDepth().toLong,
        "max_members"->profile.maxMembers(),"max_retained_bytes"->profile.maxRetainedBytes(),
        "max_sql_packet_bytes"->profile.maxSqlPacketBytes(),"max_native_artifact_bytes"->profile.maxNativeArtifactBytes()).foreach { (key,value) =>
          if cell(image,"fp_schema_manifest",key) != MySqlFormat3Cell.SignedInteger(value) then reject("Full26 manifest S scalar differs")
      }
    if !Arrays.equals(hashCell(before,"fp_schema_manifest","configuration_sha256"),hashCell(target,"fp_schema_manifest","configuration_sha256")) then
      reject("Full26 before/target actual manifest C differs")
    if a.phase() == B1ResumeRecord.Phase.HANDOFF then
      requireWriter(before,a.beforeWriter(),preservedReceipt = true)
      requireWriter(target,a.targetWriter(),preservedReceipt = true)
      requirePreservedReceipt(before,target,a.beforeWriter(),a.targetWriter())
    else if a.phase()==B1ResumeRecord.Phase.CERTIFY && historicalCertify.nonEmpty then
      requireWriter(before,a.beforeWriter(),preservedReceipt=true);requireWriter(target,a.targetWriter())
    else
      requireWriter(before,a.beforeWriter());requireWriter(target,a.targetWriter())
    if a.context().binding() != plan.bindingUuid() || a.logicalWriteUuid() != a.context().operation() ||
        a.phase() == B1ResumeRecord.Phase.PREPARE && a.logicalWriteUuid() != plan.prepareWriteUuid() ||
        a.phase() == B1ResumeRecord.Phase.CERTIFY && a.logicalWriteUuid() != plan.certifyWriteUuid() then
      reject("Full26 attempt differs from fixed original operation context")

  private def requireWriter(image: MySqlScriptSchemaSnapshot, expected: B1ResumeRecord.WriterAssertions,
      preservedReceipt: Boolean = false): Unit =
    if expected == null || !binaryCell(image,"fp_runtime_writer","binding_uuid").equalsUuid(expected.binding()) ||
        cell(image,"fp_runtime_writer","writer_epoch") != MySqlFormat3Cell.SignedInteger(expected.epoch()) ||
        cell(image,"fp_runtime_writer","lease_until") != MySqlFormat3Cell.DateTime(expected.leaseUntil()) then reject("Full26 stored W differs from complete context W")
    def optionalUuid(name: String, value: UUID): Boolean =
      if value == null then cell(image,"fp_runtime_writer",name) == MySqlFormat3Cell.SqlNull
      else binaryCell(image,"fp_runtime_writer",name).equalsUuid(value)
    if !optionalUuid("owner_boot_uuid",expected.ownerBoot()) then reject("Full26 W owner differs from context")
    val slot=expected.lastSlot()
    if slot == null then
      if cell(image,"fp_runtime_writer","last_commit_operation_uuid") != MySqlFormat3Cell.SqlNull ||
          cell(image,"fp_runtime_writer","last_commit_fingerprint") != MySqlFormat3Cell.SqlNull then reject("Full26 W unexpected receipt slot")
    else if (!preservedReceipt && (slot.boot() != expected.ownerBoot() || slot.writerEpoch() != expected.epoch())) ||
        (preservedReceipt && slot.writerEpoch() > expected.epoch()) || slot.binding() != expected.binding() ||
        !optionalUuid("last_commit_operation_uuid",slot.operation()) ||
        !binaryCell(image,"fp_runtime_writer","last_commit_fingerprint").equalsBytes(slot.fingerprint()) then reject("Full26 W receipt differs from context")

  /** HANDOFF 只改变 custody/lease；原 logical receipt 的完整身份与 SQL 两列必须原样保留。 */
  private def requirePreservedReceipt(before: MySqlScriptSchemaSnapshot,target: MySqlScriptSchemaSnapshot,
      previous: B1ResumeRecord.WriterAssertions,next: B1ResumeRecord.WriterAssertions): Unit =
    val a=previous.lastSlot();val b=next.lastSlot()
    val sameSlot=if a == null || b == null then a == null && b == null
      else a.boot() == b.boot() && a.writerEpoch() == b.writerEpoch() && a.binding() == b.binding() &&
        a.operation() == b.operation() && Arrays.equals(a.fingerprint(),b.fingerprint())
    if previous.binding() != next.binding() || !sameSlot ||
        cell(before,"fp_runtime_writer","last_commit_operation_uuid") != cell(target,"fp_runtime_writer","last_commit_operation_uuid") ||
        cell(before,"fp_runtime_writer","last_commit_fingerprint") != cell(target,"fp_runtime_writer","last_commit_fingerprint") then
      reject("Full26 HANDOFF changed original complete logical receipt identity or SQL bytes")

  private def cell(image: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell =
    val rows=image.relation(table).rows;val index=image.inputs.forTable(table).columns.indexWhere(_.name==column)
    if rows.size != 1 || index < 0 then reject("Full26 singleton/column missing: "+table+"."+column)
    rows.head(index)
  private def binaryCell(image: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Binary = cell(image,table,column) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => reject("Full26 typed binary missing: "+table+"."+column)
  private def hashCell(image: MySqlScriptSchemaSnapshot, table: String, column: String): Array[Byte] =
    val value=binaryCell(image,table,column)
    if value.length!=32 then reject("Full26 exact hash missing: "+column)
    Array.tabulate(32)(value.byteAt)

  private def encodeBody(context: MySqlFormat3Binary, before: MySqlScriptSchemaCanonicalPayload,
      target: MySqlScriptSchemaCanonicalPayload, inputs: MySqlScriptSchemaInputs,
      storage: MySqlFormat3ReadBudget): MySqlFormat3Binary =
    val length=Math.addExact(24L+DomainBytes.length+97L,Math.addExact(context.length.toLong,Math.addExact(before.byteLength.toLong,target.byteLength.toLong)))
    if length>inputs.sourceLimitProfile.maxNativeArtifactBytes() || length>inputs.sourceLimitProfile.maxRetainedBytes() then reject("Full26 body exceeds explicit S envelope")
    val size=MySqlFormat3JvmArrayLimit.requireLength(length,"Full26 body exact buffer")
    storage.reserveRetainedMemory(length+64L,"Full26 exact final body buffer BEFORE allocation")
    temporary(storage,512L,"Full26 body final writer/S structures") {
      val sink=new Exact(new Array[Byte](size));val output=new DataOutputStream(sink)
      writeBody(output,context,before,target,inputs);output.flush()
      MySqlFormat3Binary.takeOwned(sink.finish(),storage)
    }
  private def writeBody(out: DataOutputStream, context: MySqlFormat3Binary,
      before: MySqlScriptSchemaCanonicalPayload, target: MySqlScriptSchemaCanonicalPayload,
      inputs: MySqlScriptSchemaInputs): Unit =
    out.writeInt(DomainBytes.length);out.write(DomainBytes)
    val s=inputs.sourceLimitProfile.canonicalBytes();out.writeInt(s.length);out.write(s)
    out.writeInt(context.length);context.writeTo(out)
    out.writeInt(before.byteLength);before.bytes.writeTo(out)
    out.writeInt(target.byteLength);target.bytes.writeTo(out);out.writeInt(1)
  private def digest(body: MySqlFormat3Binary): Array[Byte] =
    val d=MessageDigest.getInstance("SHA-256");var n=0
    while n<body.length do { d.update(body.byteAt(n));n+=1 }
    d.digest()
  private def temporary[A](budget: MySqlFormat3ReadBudget, fee: Long, label: String)(body: => A): A =
    budget.reserveTemporaryMemory(fee,label)
    var primary: Throwable=null
    try body
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(()=>budget.releaseTemporaryMemory(fee,label)))
  private final class Reader(input: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget):
    private var offset=0
    def atEnd: Boolean=offset==input.length
    private def available(n: Int): Unit =
      if n<0 || n>input.length-offset then throw new EOFException("truncated/negative Full26 body framing")
    def int(): Int =
      available(4);var value=0;var n=0
      while n<4 do { value=(value<<8)|(input.byteAt(offset+n)&255);n+=1 }
      offset+=4;value
    def blob(limit: Long, label: String): MySqlFormat3Binary =
      val size=int()
      if size<=0 || size.toLong>limit then reject(label+" length outside explicit bound")
      available(size);budget.reserveRetainedMemory(size.toLong+64L,label+" BEFORE allocation")
      val bytes=new Array[Byte](size);var n=0
      while n<size do { bytes(n)=input.byteAt(offset+n);n+=1 }
      offset+=size;MySqlFormat3Binary.takeOwned(bytes,budget)
  private final class Exact(private var bytes: Array[Byte]) extends OutputStream:
    private var offset=0
    override def write(value: Int): Unit =
      if bytes==null || offset>=bytes.length then throw new IOException("Full26 exact body writer overrun")
      bytes(offset)=value.toByte;offset+=1
    override def write(value: Array[Byte],start: Int,count: Int): Unit =
      java.util.Objects.checkFromIndexSize(start,count,value.length)
      if bytes==null || count>bytes.length-offset then throw new IOException("Full26 exact body writer overrun")
      System.arraycopy(value,start,bytes,offset,count);offset+=count
    def finish(): Array[Byte] =
      if bytes==null || offset!=bytes.length then throw new IOException("Full26 exact body writer underrun")
      val result=bytes;bytes=null;result
  private final class Compare(expected: MySqlFormat3Binary) extends OutputStream:
    private var offset=0
    override def write(value: Int): Unit =
      if offset>=expected.length || expected.byteAt(offset)!=value.toByte then reject("noncanonical Full26 body reencoding")
      offset+=1
    override def close(): Unit = if offset!=expected.length then reject("Full26 body reencoding length differs")
  private def reject(message: String): Nothing = throw new SQLDataException(message)
