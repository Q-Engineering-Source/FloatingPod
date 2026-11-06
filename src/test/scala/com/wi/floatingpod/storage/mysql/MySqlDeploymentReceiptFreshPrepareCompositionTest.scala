package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}
import com.wi.floatingpod.server.B1ResumeRecord.*
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.{List as JList, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 实际 DATA 组合及 codec；无 native handle、SQL、私有 producer 或 Fresh 资格。 */
final class MySqlDeploymentReceiptFreshPrepareCompositionTest:
  import MySqlScriptSchemaDataTestFixture.*
  private val I = Full26Inputs
  private val B = "physical_recovery_binding"
  private val B1 = "binding_bootstrap_intent"
  private val C = "fp_script_catalog"
  private val W = "fp_runtime_writer"
  private val M = "fp_schema_manifest"
  private val Source = "world://source/primary"
  private val Now = LocalDateTime.of(2026,10,23,12,0)
  private def id(n: Int): UUID = UUID.fromString(f"18a0b51d-4ff4-4a71-a27f-d67f48d3$n%04x")
  private val Boot = id(0xc7c1)
  private val Plan = new WindowsWorldProvisionReceiptDataCodec.PlanData(id(0xc701),id(0xc702),id(0xc703),id(0xc704),id(0xc705),
    2,Connection.serverUuid,I.schema,5,I.ddlSha256,ProvisioningUuid)
  private val Receipt = WindowsWorldProvisionReceiptDataCodec.encode(new WindowsWorldProvisionReceiptDataCodec.ReceiptData(Plan,
    "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=01010101010101010101010101010101",
    "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=02020202020202020202020202020202",
    1234L,133800000000000123L,new WindowsWorldProvisioner.BootIdentity(Boot,1L)))
  private val ReceiptHash = digest(Receipt).toVector
  private val Seed = MySqlBusinessWriterRow(Plan.bindingUuid(),0L,None,Now,None,None,Now)
  private val TargetW = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(),1L,Some(Boot),Now.plusMinutes(1),Plan.prepareWriteUuid())
  private val Namespace = MySqlSessionBarrierNamespace.forIdentity(Plan.mysqlServerUuid(),I.schema,Plan.provisioningUuid(),Plan.bindingUuid()).lockName()
  private def bytes(cell: MySqlFormat3Cell): Array[Byte] = cell match
    case MySqlFormat3Cell.Binary(value) => Array.tabulate(value.length)(value.byteAt)
    case _ => throw new AssertionError("actual binary required")
  private def cell(s: MySqlScriptSchemaSnapshot, table: String, name: String): MySqlFormat3Cell =
    s.relation(table).rows.head(I.forTable(table).columns.indexWhere(_.name == name))
  private def update(rows: Vector[MySqlFormat3RelationSnapshot], table: String, name: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val n = I.forTable(table).columns.indexWhere(_.name == name); require(n >= 0)
    rows.map(r => if r.table == table then r.copy(rows = r.rows.map(_.updated(n,value))) else r)
  private def empty(rows: Vector[MySqlFormat3RelationSnapshot], table: String): Vector[MySqlFormat3RelationSnapshot] =
    rows.map(r => if r.table == table then r.copy(rows = Vector.empty) else r)
  private def seedRows: Vector[MySqlFormat3RelationSnapshot] =
    var rows = full26GenesisRelations(I,catalogPresent = false)
    rows = empty(empty(rows,B),B1)
    rows = update(rows,M,"expected_binding_uuid",binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows,W,"binding_uuid",binary(uuidBytes(Plan.bindingUuid())))
    rows = update(rows,W,"lease_until",MySqlFormat3Cell.DateTime(Now))
    // 合法 DATA 非空 manifest payload，连同其实际 SHA 保存；不把 provenance DATA 签成 native 资格。
    val payload = Array.tabulate[Byte](8192)(n => (n % 251).toByte)
    rows = update(rows,M,"source_provenance_bytes",binary(payload))
    update(rows,M,"source_provenance_sha256",binary(digest(payload)))
  private def capture(rows: Vector[MySqlFormat3RelationSnapshot], r: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot = snapshot(I,r,rows)
  private def build(before: MySqlScriptSchemaSnapshot, r: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData = Plan, writer: MySqlFreshFormat3WriterTarget = TargetW): MySqlScriptSchemaSnapshot =
    MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,I,r,plan,Source,ReceiptHash,Seed,writer)
  private def validate(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, r: MySqlFormat3ReadBudget): Unit =
    MySqlDeploymentReceiptFreshPrepareComposition.requirePrepareTransition(before,target,I,r,Plan,Source,ReceiptHash,Seed,TargetW)
  private def close(s: MySqlScriptSchemaSnapshot, r: MySqlFormat3ReadBudget): Unit =
    if s != null && (s.allocationBudget ne r) && !s.allocationBudget.isClosed then s.allocationBudget.closeScopedReadBudget()
  private def fixture(use: (MySqlFormat3ReadBudget,MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot) => Unit): Unit =
    val r = root(I); var target: MySqlScriptSchemaSnapshot = null
    try
      val before = capture(seedRows,r); target = build(before,r); use(r,before,target)
    finally close(target,r); r.closeRootReadBudget()

  /** 独立固定 framing：不调用 production effect/projection，只复用既有 canonical cell ABI。 */
  private def oracle(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Array[Byte] =
    val buffer = new ByteArrayOutputStream(); val out = new DataOutputStream(buffer)
    def string(s: String): Unit =
      val b = s.getBytes(StandardCharsets.UTF_8); out.writeInt(b.length); out.write(b)
    def uuid(u: UUID): Unit = { out.writeLong(u.getMostSignificantBits); out.writeLong(u.getLeastSignificantBits) }
    string("floatingpod.mysql.script-publication1-deployment-receipt1-fresh-prepare-composition-effect/v1"); string("PREPARE")
    uuid(id(0xc701));uuid(id(0xc702));uuid(id(0xc703));uuid(id(0xc704));uuid(id(0xc705));out.writeInt(2)
    uuid(Connection.serverUuid);string(I.schema);out.writeInt(5);out.write(("5A" * 32).getBytes(StandardCharsets.US_ASCII));uuid(ProvisioningUuid)
    string("world://source/primary");out.writeInt(32);out.write(digest(Receipt))
    out.writeLong(before.canonicalByteLength);out.writeInt(32);out.write(before.canonicalSha256.toArray)
    for table <- Vector("physical_recovery_binding","binding_bootstrap_intent","fp_script_catalog") do
      string(table);out.writeInt(1)
      val row = target.relation(table).rows.head;out.writeInt(row.size)
      row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_,out))
    uuid(id(0xc705));out.writeLong(1L);out.writeByte(1);uuid(Boot)
    out.writeInt(2026);out.writeByte(10);out.writeByte(23);out.writeByte(12);out.writeByte(1);out.writeByte(0);out.writeInt(0)
    uuid(id(0xc703));out.writeByte(0);out.flush();buffer.toByteArray

  @Test def exactInitialCatalogAndOnlyFourDeltasPreserveAllTwentyTwoRelations(): Unit = fixture { (r,before,target) =>
    assertEquals(26,target.relations.size)
    assertArrayEquals(uuidBytes(Plan.bindingUuid()),bytes(cell(target,C,"binding_uuid")))
    assertEquals(MySqlFormat3Cell.SignedInteger(0L),cell(target,C,"catalog_revision"))
    assertArrayEquals(Array.emptyByteArray,bytes(cell(target,C,"snapshot_bytes")))
    assertArrayEquals(digest(Array.emptyByteArray),bytes(cell(target,C,"snapshot_sha256")))
    assertEquals(MySqlFormat3Cell.SignedInteger(1L),cell(target,C,"next_version"))
    assertEquals(MySqlFormat3Cell.SqlNull,cell(target,C,"last_mutation_operation_uuid"))
    val unchanged = before.relations.filterNot(r => Set(B,B1,W,C).contains(r.table))
    assertEquals(22,unchanged.size);unchanged.foreach(row => assertEquals(row,target.relation(row.table)))
    assertEquals(8192,bytes(cell(target,M,"source_provenance_bytes")).length)
    assertEquals(MySqlFormat3Cell.SignedInteger(0L),cell(target,MySqlDeploymentReceiptSchemaDefinition.QuotaTable,"used_records"))
    validate(before,target,r)
  }

  @Test def fixedIndependentCompositionFramingCommitsFinalCatalogAndSingleFinalWriter(): Unit = fixture { (r,before,target) =>
    val expected = oracle(before,target)
    val domain = "floatingpod.mysql.script-publication1-deployment-receipt1-fresh-prepare-composition-effect/v1".getBytes(StandardCharsets.UTF_8)
    assertEquals(domain.length,ByteBuffer.wrap(expected).getInt())
    assertArrayEquals(domain,expected.slice(4,4+domain.length))
    assertEquals(0,expected.last.toInt)
    val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,I,r,Plan,Source,ReceiptHash,TargetW)
    assertArrayEquals(digest(expected),context.expectedEffects.head.desiredEffectFingerprintSha256.toArray)
    assertFalse(java.util.Arrays.equals(digest(expected),context.canonicalFingerprintSha256.toArray))
    assertArrayEquals(context.canonicalFingerprintSha256.toArray,bytes(cell(target,W,"last_commit_fingerprint")))
    Vector(TargetW.copy(ownerBootUuid=Some(id(0xc799))),TargetW.copy(writerEpoch=2L),
      TargetW.copy(bindingUuid=id(0xc799)),TargetW.copy(operationUuid=id(0xc799))).foreach { changed=>
      assertThrows(classOf[Exception],()=>MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,I,r,Plan,Source,ReceiptHash,changed))
    }
    assertThrows(classOf[Exception],()=>MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,I,r,Plan,Source,Vector.fill(32)(9.toByte),TargetW))
    val intermediate = MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,I,r,Plan,Source,ReceiptHash,Seed,TargetW)
    try
      assertFalse(java.util.Arrays.equals(bytes(cell(intermediate,W,"last_commit_fingerprint")),bytes(cell(target,W,"last_commit_fingerprint"))))
      assertEquals(intermediate.relation(B),target.relation(B));assertEquals(intermediate.relation(B1),target.relation(B1))
      assertTrue(intermediate.relation(C).rows.isEmpty)
    finally close(intermediate,r)
  }

  @Test def validatorRejectsEachCatalogFieldFingerprintAndUnrelatedMutation(): Unit = fixture { (r,before,target) =>
    val mutations = Vector(
      (C,"binding_uuid",binary(uuidBytes(id(0xc799)))),
      (C,"catalog_revision",MySqlFormat3Cell.SignedInteger(1L)),
      (C,"snapshot_bytes",binary(Array[Byte](9))),
      (C,"snapshot_sha256",binary(Array.fill[Byte](32)(9))),
      (C,"next_version",MySqlFormat3Cell.SignedInteger(2L)),
      (C,"last_mutation_operation_uuid",binary(uuidBytes(id(0xc798)))),
      (W,"last_commit_fingerprint",binary(Array.fill[Byte](32)(9))),
      (W,"last_commit_operation_uuid",binary(uuidBytes(id(0xc798)))),
      (M,"configuration_sha256",binary(Array.fill[Byte](32)(9))))
    mutations.foreach { (table,name,value) =>
      assertThrows(classOf[Exception], () => {
        val altered = capture(update(target.relations,table,name,value),r)
        validate(before,altered,r)
      })
    }
    val duplicateRows = target.relations.map(rel => if rel.table == C then rel.copy(rows = rel.rows ++ rel.rows) else rel)
    assertThrows(classOf[Exception], () => validate(before,capture(duplicateRows,r),r))
    assertThrows(classOf[Exception], () => build(target,r))
  }

  @Test def wrongPlanWriterAndInitializerHistoryFailWithActualValidators(): Unit = fixture { (r,before,_) =>
    val wrong = id(0xc799)
    for writer <- Vector(TargetW.copy(operationUuid = wrong),TargetW.copy(writerEpoch = 2L),TargetW.copy(ownerBootUuid = None),TargetW.copy(bindingUuid = wrong)) do
      assertThrows(classOf[Exception], () => build(before,r,writer = writer))
    val plan = new WindowsWorldProvisionReceiptDataCodec.PlanData(Plan.creationOperationUuid(),Plan.bootstrapOperationUuid(),Plan.prepareWriteUuid(),
      Plan.certifyWriteUuid(),Plan.bindingUuid(),2,wrong,I.schema,5,I.ddlSha256,ProvisioningUuid)
    assertThrows(classOf[Exception], () => build(before,r,plan = plan))
    val existing = capture(full26GenesisRelations(I,catalogPresent = true),r)
    assertThrows(classOf[Exception], () => build(existing,r))
    // 真实脚本 operation 表非空即违反 initializer；不清除后再假称完成。
    val table = "fp_script_operation"
    val rows = before.relations.map(rel => if rel.table == table then rel.copy(rows = Vector(defaultRow(I.forTable(table)))) else rel)
    assertThrows(classOf[Exception], () => build(capture(rows,r),r))
  }

  @Test def ownedTargetSurvivesSourceChildCloseAndPartialFailureRefundsAggregateBudget(): Unit =
    val r = root(I)
    var before: MySqlScriptSchemaSnapshot = null
    var target: MySqlScriptSchemaSnapshot = null
    val seed = capture(seedRows,r)
    val payload = MySqlScriptSchemaSnapshotCodec.encode(seed,r)
    try
      before = MySqlScriptSchemaSnapshotCodec.decode(payload,I,r)
      target = build(before,r)
      close(before,r)
      assertEquals(MySqlFormat3Cell.Text("PREPARED"),cell(target,B1,"stage"))
      assertEquals(8192,bytes(cell(target,M,"source_provenance_bytes")).length)
      val owned = MySqlScriptSchemaSnapshotCodec.encode(target,r)
      owned.storageBudget.closeScopedReadBudget()
      assertThrows(classOf[Exception], () => build(before,r))
    finally close(target,r);payload.storageBudget.closeScopedReadBudget();r.closeRootReadBudget()
    val ledger = new MySqlFormat3AllocationLedger(I.sourceLimitProfile.maxRetainedBytes())
    val limited = root(I,ledger)
    try
      val actual = capture(seedRows,limited)
      actual.canonicalByteLength;actual.canonicalSha256
      val baseline = ledger.accountedBytes
      val warm = build(actual,limited)
      close(warm,limited)
      assertEquals(baseline,ledger.accountedBytes)
      val actualPeak = ledger.successfulReserveHighWater - baseline
      assertTrue(actualPeak > 65536L)
      // 基于真实同一实现的已收费峰值，留少一字节容量，覆盖分配后的 partial cleanup。
      val reserve = ledger.availableBytes - (actualPeak - 1L)
      limited.reserveRetainedMemory(reserve,"test actual aggregate pressure")
      try assertThrows(classOf[Exception], () => build(actual,limited))
      finally limited.releaseRetainedMemory(reserve,"test actual aggregate pressure dropped")
      assertEquals(baseline,ledger.accountedBytes)
    finally limited.closeRootReadBudget()

  @Test def exactRootProfileInputsAndSiblingOwnershipAreMandatory(): Unit = fixture { (_,before,_) =>
    val ledger = new MySqlFormat3AllocationLedger(I.sourceLimitProfile.maxRetainedBytes())
    val a = root(I,ledger);val b = root(I,ledger)
    try
      val sibling = capture(seedRows,b)
      assertTrue(sibling.allocationBudget.sharesAllocationLedger(a))
      assertThrows(classOf[Exception], () => build(sibling,a))
      assertThrows(classOf[Exception], () => build(before,a))
      assertThrows(classOf[Exception], () => MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,Inputs,a,Plan,Source,ReceiptHash,Seed,TargetW))
      val sameBytes = SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,2_000_000L,16_000_000L,4_000_000L,2_000_000L)
      val otherInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(I.schema,I.ddlSha256,sameBytes,I.budget,
        MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 1_000_000L,diagnosticsBytes = 1_000_000L))
      assertArrayEquals(I.sourceLimitProfile.canonicalBytes(),sameBytes.canonicalBytes())
      assertThrows(classOf[Exception], () => MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,otherInputs,a,Plan,Source,ReceiptHash,Seed,TargetW))
    finally b.closeRootReadBudget();a.closeRootReadBudget()
    assertThrows(classOf[Exception], () => build(before,a))
  }

  private def assertions(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, r: MySqlFormat3ReadBudget,
      phase: Phase, operation: UUID): AttemptAssertions =
    val bg = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,I,r)
    val tg = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,I,r)
    def writer(image: MySqlScriptSchemaSnapshot): WriterAssertions =
      val epoch = cell(image,W,"writer_epoch") match
        case MySqlFormat3Cell.SignedInteger(n) => n
        case _ => throw new AssertionError("actual epoch")
      val lease = cell(image,W,"lease_until") match
        case MySqlFormat3Cell.DateTime(t) => t
        case _ => throw new AssertionError("actual lease")
      val owner = if epoch == 0L then null else Boot
      val slot = if epoch == 0L then null else new ReceiptSlotAssertions(Boot,epoch,Plan.bindingUuid(),
        if phase == Phase.CERTIFY && (image eq target) then Plan.certifyWriteUuid() else Plan.prepareWriteUuid(),bytes(cell(image,W,"last_commit_fingerprint")))
      new WriterAssertions(Plan.bindingUuid(),epoch,owner,lease,slot)
    var primary: Throwable = null
    try
      val context = new OperationAssertions(Boot,1L,Plan.bindingUuid(),operation,JList.of(new EffectAssertions("data/composite-prepare",0L,Array.fill[Byte](32)(8))),
        bytes(cell(target,W,"last_commit_fingerprint")))
      new AttemptAssertions(if phase == Phase.PREPARE then id(0xc711) else id(0xc712),phase,operation,1L,id(0xc713),123L,Namespace,
        context,writer(before),writer(target),bg.toResumeAssertions,tg.toResumeAssertions)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary,Vector(
        () => tg.storageBudget.get.closeScopedReadBudget(),
        () => bg.storageBudget.get.closeScopedReadBudget()))

  @Test def actualCertifyPreservesFinalCatalogAndBothPhasesUseWholeBodyCommonContext(): Unit = fixture { (r,before,target) =>
    val prepared = MySqlBusinessWriterRow(Plan.bindingUuid(),1L,Some(Boot),TargetW.leaseUntil,Some(Plan.prepareWriteUuid()),
      Some(bytes(cell(target,W,"last_commit_fingerprint")).toVector),Now)
    val certified = MySqlDeploymentReceiptBootstrapGraph.certifyTarget(target,I,r,Plan,Source,ReceiptHash,Vector.fill[Byte](32)(0x55.toByte),
      prepared,TargetW.copy(operationUuid = Plan.certifyWriteUuid()))
    try
      assertEquals(target.relation(C),certified.relation(C))
      MySqlDeploymentReceiptBootstrapGraph.requireCertifyTransition(target,certified,I,r,Plan,Source,ReceiptHash,Vector.fill[Byte](32)(0x55.toByte),
        prepared,TargetW.copy(operationUuid = Plan.certifyWriteUuid()))
      val prepare = assertions(before,target,r,Phase.PREPARE,Plan.prepareWriteUuid())
      val certify = assertions(target,certified,r,Phase.CERTIFY,Plan.certifyWriteUuid())
      for (b,t,a) <- Vector((before,target,prepare),(target,certified,certify)) do
        val state = new FullStateAssertions(new NativeOwnerAssertions(1234L,133800000000000123L,Boot,1L),null,null,
          if a.phase() == Phase.PREPARE then null else prepare,a.beforeWriter(),a.targetWriter(),a,null,null,null)
        val record = new B1ResumeRecord(if a.phase() == Phase.PREPARE then 0L else 1L,
          if a.phase() == Phase.PREPARE then new Array[Byte](32) else Array.fill[Byte](32)(1),
          if a.phase() == Phase.PREPARE then Kind.GENESIS else Kind.DB_ACQUIRED,Array.fill[Byte](32)(19),Receipt,Namespace,state)
        val owner = MySqlFull26WitnessCodec.begin(I,r)
        val cold = MySqlFull26WitnessCodec.begin(I,r)
        try
          owner.encode(record,b,t)
          val output = new ByteArrayOutputStream();owner.writeBodyTo(output);cold.decode(output.toByteArray)
          cold.withData { (decoded,actualBefore,actualTarget,bg,tg) =>
            assertArrayEquals(B1ResumeRecordCanonical.encodeSchema5Data(record),B1ResumeRecordCanonical.encodeSchema5Data(decoded))
            assertTrue(actualBefore.exactlyEquals(b));assertTrue(actualTarget.exactlyEquals(t))
            assertTrue(bg.matchesResumeAssertions(a.beforeGraph()));assertTrue(tg.matchesResumeAssertions(a.targetGraph()))
          }
        finally cold.close();owner.close()
    finally close(certified,r)
  }
