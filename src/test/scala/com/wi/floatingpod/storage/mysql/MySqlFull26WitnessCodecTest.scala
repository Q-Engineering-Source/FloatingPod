package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.{B1Full26WitnessReference, B1ResumeRecord, B1ResumeRecordCanonical, WindowsMarkerIo, WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}
import com.wi.floatingpod.server.B1ResumeRecord.*
import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.util.{Arrays, HexFormat, List as JList, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 所有 FileID/receipt/native owner 都只是 DATA；不打开文件、进程、SQL 或 native handle。 */
final class MySqlFull26WitnessCodecTest:
  import MySqlScriptSchemaDataTestFixture.*
  private val I = Full26Inputs
  private val B = "physical_recovery_binding"
  private val B1 = "binding_bootstrap_intent"
  private val W = "fp_runtime_writer"
  private val M = "fp_schema_manifest"
  private val H = "fp_recovery_operation"
  private val Now = LocalDateTime.of(2026, 10, 23, 12, 0)
  private def id(last: String): UUID = UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3" + last)
  private val Boot = id("c7c1")
  private val Plan = new WindowsWorldProvisionReceiptDataCodec.PlanData(id("c701"),
    UUID.fromString("00000000-0000-0000-0000-000000000203"),
    UUID.fromString("00000000-0000-0000-0000-000000000204"),
    UUID.fromString("00000000-0000-0000-0000-000000000205"), id("c705"),
    2, Connection.serverUuid, I.schema, 5, I.ddlSha256, ProvisioningUuid)
  private val Receipt = WindowsWorldProvisionReceiptDataCodec.encode(new WindowsWorldProvisionReceiptDataCodec.ReceiptData(Plan,
    "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=01010101010101010101010101010101",
    "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=02020202020202020202020202020202",
    1234L, 133800000000000123L, new WindowsWorldProvisioner.BootIdentity(Boot, 1L)))
  private val ReceiptHash = digest(Receipt).toVector
  private val Namespace = MySqlSessionBarrierNamespace.forIdentity(Plan.mysqlServerUuid(), Plan.schema(), Plan.provisioningUuid(), Plan.bindingUuid()).lockName()
  private val Seed = MySqlBusinessWriterRow(Plan.bindingUuid(), 0L, None, Now, None, None, Now)
  private val PrepareW = MySqlFreshFormat3WriterTarget(Plan.bindingUuid(), 1L, Some(Boot), Now.plusMinutes(1), Plan.prepareWriteUuid())

  private def bytes(value: MySqlFormat3Cell): Array[Byte] = value match
    case MySqlFormat3Cell.Binary(b) => Array.tabulate(b.length)(b.byteAt)
    case _ => throw new AssertionError("actual binary cell required")
  private def at(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
    row(I.forTable(table).columns.indexWhere(_.name == column))
  private def at(s: MySqlScriptSchemaSnapshot, table: String, column: String): MySqlFormat3Cell = at(s.relation(table).rows.head, table, column)
  private def uuid(value: MySqlFormat3Cell): UUID =
    val b = ByteBuffer.wrap(bytes(value)); new UUID(b.getLong(), b.getLong())
  private def number(value: MySqlFormat3Cell): Long = value match
    case MySqlFormat3Cell.SignedInteger(n) => n
    case _ => throw new AssertionError("actual integer cell required")
  private def text(value: MySqlFormat3Cell): String = value match
    case MySqlFormat3Cell.Text(s) => s
    case _ => throw new AssertionError("actual text cell required")
  private def update(rows: Vector[MySqlFormat3RelationSnapshot], table: String, column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val n = I.forTable(table).columns.indexWhere(_.name == column)
    require(n >= 0, s"actual $table.$column")
    rows.map(r => if r.table == table then r.copy(rows = r.rows.map(_.updated(n, value))) else r)
  private def replace(rows: Vector[MySqlFormat3RelationSnapshot], table: String, values: Vector[Vector[MySqlFormat3Cell]]): Vector[MySqlFormat3RelationSnapshot] =
    rows.map(r => if r.table == table then r.copy(rows = values) else r)

  /** 原 shared fixture 的 binding 是 version0，不能冒充 PlanData 的 UUIDv4。
    * 仅重新绑定真实 binding columns/context；actor、request 世界、step IDs 和 opaque history 均保留。
    */
  private def rebound(rows: Vector[MySqlFormat3RelationSnapshot]): Vector[MySqlFormat3RelationSnapshot] =
    rows.map { relation =>
      val shape = I.forTable(relation.table)
      relation.copy(rows = relation.rows.map { original =>
        var row = original
        shape.columns.zipWithIndex.foreach { (column, n) =>
          if Set("binding_uuid", "expected_binding_uuid", "authority_binding_uuid").contains(column.name) then
            row = row.updated(n, binary(uuidBytes(Plan.bindingUuid())))
        }
        if relation.table == B1 then
          row = row.updated(shape.columns.indexWhere(_.name == "provision_receipt_sha256"), binary(ReceiptHash.toArray))
        if relation.table == H then
          val expected = at(row,H,"expected_domain_version") match
            case MySqlFormat3Cell.SqlNull => 0L
            case value => number(value)
          val operation = uuid(at(row,H,"operation_uuid"))
          val context = MySqlOperationContext(uuid(at(row,H,"boot_uuid")), number(at(row,H,"fence_epoch")), Plan.bindingUuid(), operation,
            Vector(MySqlExpectedEffect(s"fp_deployment_operation/$operation/${text(at(row,H,"step"))}", expected, digest(bytes(at(row,H,"target_bytes"))).toVector)))
          val encoded = encodeGenericContext(context)
          row = row.updated(shape.columns.indexWhere(_.name == "original_context_bytes"), binary(encoded))
          row = row.updated(shape.columns.indexWhere(_.name == "original_context_sha256"), binary(digest(encoded)))
        row
      })
    }
  private def seedRows: Vector[MySqlFormat3RelationSnapshot] =
    val rows = rebound(full26GenesisRelations(I, catalogPresent = true))
    update(replace(replace(rows,B,Vector.empty),B1,Vector.empty),W,"lease_until",MySqlFormat3Cell.DateTime(Now))
  private def capture(rows: Vector[MySqlFormat3RelationSnapshot], r: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot = snapshot(I,r,rows)
  private def writer(s: MySqlScriptSchemaSnapshot): WriterAssertions =
    val binding = uuid(at(s,W,"binding_uuid")); val epoch = number(at(s,W,"writer_epoch"))
    val boot = at(s,W,"owner_boot_uuid") match
      case MySqlFormat3Cell.SqlNull => null
      case value => uuid(value)
    val lease = at(s,W,"lease_until") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => throw new AssertionError("actual datetime required")
    val slot = at(s,W,"last_commit_operation_uuid") match
      case MySqlFormat3Cell.SqlNull => null
      case value => new ReceiptSlotAssertions(boot,epoch,binding,uuid(value),bytes(at(s,W,"last_commit_fingerprint")))
    new WriterAssertions(binding,epoch,boot,lease,slot)
  private def attempt(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, phase: Phase,
      operation: UUID, r: MySqlFormat3ReadBudget): AttemptAssertions =
    val bg = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,I,r)
    val tg = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,I,r)
    try
      val tw = writer(target)
      val fingerprint = if tw.lastSlot() == null then Array.fill[Byte](32)(7) else tw.lastSlot().fingerprint()
      val context = new OperationAssertions(tw.ownerBoot(),tw.epoch(),Plan.bindingUuid(),operation,
        JList.of(new EffectAssertions("test-only/actual-full26-data",0L,Array.fill[Byte](32)(8))),fingerprint)
      new AttemptAssertions(if phase == Phase.PREPARE then id("c711") else id("c712"),phase,operation,1L,id("c713"),123L,
        Namespace,context,writer(before),tw,bg.toResumeAssertions,tg.toResumeAssertions)
    finally
      tg.storageBudget.get.closeScopedReadBudget(); bg.storageBudget.get.closeScopedReadBudget()
  private final case class Fixture(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, record: B1ResumeRecord)
  private def prepareFixture(r: MySqlFormat3ReadBudget): Fixture =
    val before = capture(seedRows,r)
    val target = MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,I,r,Plan,"world://source/primary",ReceiptHash,Seed,PrepareW)
    val a = attempt(before,target,Phase.PREPARE,Plan.prepareWriteUuid(),r)
    val state = new FullStateAssertions(new NativeOwnerAssertions(1234L,133800000000000123L,Boot,1L),null,null,null,
      a.beforeWriter(),a.targetWriter(),a,null,null,null)
    Fixture(before,target,new B1ResumeRecord(0L,new Array[Byte](32),Kind.GENESIS,Array.fill[Byte](32)(19),Receipt,Namespace,state))
  private def handoffFixture(r: MySqlFormat3ReadBudget): Fixture =
    val original = prepareFixture(r)
    val before = capture(rebound(full26OrdinaryRelations(I)),r)
    val operation = id("c714")
    var rows = update(before.relations,W,"writer_epoch",MySqlFormat3Cell.SignedInteger(4L))
    rows = update(rows,W,"owner_boot_uuid",binary(uuidBytes(Boot)))
    rows = update(rows,W,"lease_until",MySqlFormat3Cell.DateTime(Now.plusHours(2)))
    rows = update(rows,W,"last_commit_operation_uuid",binary(uuidBytes(operation)))
    rows = update(rows,W,"last_commit_fingerprint",binary(Array.fill[Byte](32)(7)))
    val target = capture(rows,r)
    val a = attempt(before,target,Phase.HANDOFF,operation,r)
    val state = new FullStateAssertions(original.record.assertions().currentOwner(),null,null,original.record.assertions().pendingAttempt(),
      a.beforeWriter(),a.targetWriter(),a,null,null,null)
    // Original PREPARE assertions own their copied components in root; target snapshot is no longer needed.
    original.target.allocationBudget.closeScopedReadBudget()
    Fixture(before,target,new B1ResumeRecord(1L,Array.fill[Byte](32)(1),Kind.DB_ACQUIRED,original.record.sourceAnchorSha256(),Receipt,Namespace,state))
  private def encoded(owner: MySqlFull26WitnessCodec.Owner): Array[Byte] =
    val out = new ByteArrayOutputStream(); owner.writeBodyTo(out); out.toByteArray
  private def closeFixture(f: Fixture, r: MySqlFormat3ReadBudget): Unit =
    if (f.target.allocationBudget ne r) && !f.target.allocationBudget.isClosed then f.target.allocationBudget.closeScopedReadBudget()
  private def withFixture(handoff: Boolean)(use: (MySqlFormat3ReadBudget,Fixture) => Unit): Unit =
    val r = root(I)
    var f: Fixture = null
    try
      f = if handoff then handoffFixture(r) else prepareFixture(r)
      use(r,f)
    finally
      if f != null then closeFixture(f,r)
      r.closeRootReadBudget()
  private def reference(f: Fixture, body: Array[Byte], field: Int = -1): B1Full26WitnessReference =
    val a = f.record.assertions().pendingAttempt()
    def hash(n: Int, correct: Array[Byte]): Array[Byte] = if field == n then Array.fill[Byte](32)(99) else correct
    val full = hash(0,digest(body)); val attemptId = if field == 7 then id("c799") else a.attemptId()
    new B1Full26WitnessReference(B1Full26WitnessReference.basename(attemptId,full),new WindowsMarkerIo.FileIdentity(1L,Array.fill[Byte](16)(1)),
      body.length.toLong + (if field == 6 then 1L else 0L),full,hash(1,f.record.sourceAnchorSha256()),hash(2,digest(Receipt)),
      hash(3,I.sourceLimitProfile.sha256()),hash(4,bytes(at(f.before,M,"configuration_sha256"))),hash(5,HexFormat.of().parseHex(I.ddlSha256)),
      attemptId,if field == 8 then Phase.CERTIFY else a.phase(),if field == 9 then id("c798") else a.logicalWriteUuid())

  @Test def truePrepareAbsenceAndActualBootstrapTargetSurviveColdBodyRoundtrip(): Unit = withFixture(false) { (r,f) =>
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    val cold = MySqlFull26WitnessCodec.begin(I,r)
    try
      owner.encode(f.record,f.before,f.target)
      val body = encoded(owner)
      cold.decode(body)
      cold.withData { (record,before,target,bg,tg) =>
        assertArrayEquals(B1ResumeRecordCanonical.encodeSchema5Data(f.record),B1ResumeRecordCanonical.encodeSchema5Data(record))
        assertTrue(before.exactlyEquals(f.before)); assertTrue(target.exactlyEquals(f.target))
        assertTrue(bg.binding.isEmpty); assertTrue(tg.binding.nonEmpty)
        assertTrue(before.relation(B).rows.isEmpty); assertTrue(before.relation(B1).rows.isEmpty)
        assertThrows(classOf[IllegalStateException], () => cold.close())
        assertTrue(before.allocationBudget.isCanonicalChildOf(r))
      }
      assertArrayEquals(body,encoded(cold)); cold.requireReference(reference(f,body))
    finally cold.close(); owner.close()
  }

  @Test def certifiedCrossActorHistoryUnknownDiagnosticsAndQuotaAreCompleteColdData(): Unit = withFixture(true) { (r,f) =>
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    try
      owner.encode(f.record,f.before,f.target)
      owner.withData { (_,before,target,bg,tg) =>
        assertTrue(before.exactlyEquals(f.before)); assertTrue(target.exactlyEquals(f.target))
        assertEquals(7,before.relation(H).rows.size)
        assertEquals(2,before.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.size)
        assertEquals(MySqlFormat3Cell.SignedInteger(2L),at(before,MySqlDeploymentReceiptSchemaDefinition.QuotaTable,"used_records"))
        assertEquals(before.relation(H),target.relation(H))
        assertTrue(before.relation(H).rows.exists(row => at(row,H,"original_diagnostic_identity") != MySqlFormat3Cell.SqlNull))
        assertTrue(bg.binding.nonEmpty); assertTrue(tg.binding.nonEmpty)
      }
    finally owner.close()
  }

  @Test def actualCertifyBuilderCompleteIntentAndFixedCertifyWriteSurviveColdData(): Unit = withFixture(false) { (r,f) =>
    val prepared = MySqlBusinessWriterRow(Plan.bindingUuid(),1L,Some(Boot),PrepareW.leaseUntil,
      Some(Plan.prepareWriteUuid()),Some(bytes(at(f.target,W,"last_commit_fingerprint")).toVector),Now)
    val target = MySqlDeploymentReceiptBootstrapGraph.certifyTarget(f.target,I,r,Plan,"world://source/primary",ReceiptHash,
      Vector.fill[Byte](32)(0x55.toByte),prepared,PrepareW.copy(operationUuid = Plan.certifyWriteUuid()))
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    try
      val a = attempt(f.target,target,Phase.CERTIFY,Plan.certifyWriteUuid(),r)
      val state = new FullStateAssertions(f.record.assertions().currentOwner(),null,null,f.record.assertions().pendingAttempt(),
        a.beforeWriter(),a.targetWriter(),a,null,null,null)
      val record = new B1ResumeRecord(1L,Array.fill[Byte](32)(1),Kind.DB_ACQUIRED,f.record.sourceAnchorSha256(),Receipt,Namespace,state)
      owner.encode(record,f.target,target)
      owner.withData { (decoded,before,after,_,_) =>
        assertEquals(Phase.CERTIFY,decoded.assertions().pendingAttempt().phase())
        assertTrue(before.exactlyEquals(f.target)); assertTrue(after.exactlyEquals(target))
        assertEquals(MySqlFormat3Cell.Text("COMPLETE"),at(after,B1,"stage"))
        assertArrayEquals(uuidBytes(Plan.certifyWriteUuid()),bytes(at(after,W,"last_commit_operation_uuid")))
      }
    finally owner.close(); target.allocationBudget.closeScopedReadBudget()
  }

  @Test def independentEnvelopeUsesRealCanonicalComponentsAndStrictVersionEof(): Unit = withFixture(false) { (r,f) =>
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    val before = MySqlScriptSchemaSnapshotCodec.encode(f.before,r)
    val target = MySqlScriptSchemaSnapshotCodec.encode(f.target,r)
    try
      owner.encode(f.record,f.before,f.target)
      val expected = new ByteArrayOutputStream(); val out = new DataOutputStream(expected)
      def blob(value: Array[Byte]): Unit = { out.writeInt(value.length); out.write(value) }
      blob("FP_B1_FULL26_ORIGINAL_WITNESS_BE_V1".getBytes(StandardCharsets.US_ASCII))
      blob(I.sourceLimitProfile.canonicalBytes()); blob(B1ResumeRecordCanonical.encodeSchema5Data(f.record))
      out.writeInt(before.byteLength); before.bytes.writeTo(out)
      out.writeInt(target.byteLength); target.bytes.writeTo(out); out.writeInt(1); out.flush()
      val body = encoded(owner)
      assertArrayEquals(expected.toByteArray,body)
      assertEquals(owner.byteLength,body.length)
      val wrongVersion = body.clone(); wrongVersion(wrongVersion.length-1) = 2
      val wrongDomain = body.clone(); wrongDomain(4) = 'X'.toByte
      val negativeLength = body.clone(); ByteBuffer.wrap(negativeLength).putInt(-1)
      val wrongS = body.clone()
      val sOffset = 8 + MySqlFull26WitnessCodec.Domain.length
      wrongS(sOffset) = (wrongS(sOffset) ^ 1).toByte
      val corruptPayload = body.clone(); corruptPayload(corruptPayload.length-5) = (corruptPayload(corruptPayload.length-5) ^ 1).toByte
      for bad <- Vector(body.dropRight(1),body ++ Array[Byte](0),wrongVersion,wrongDomain,negativeLength,wrongS,corruptPayload) do
        val partial = MySqlFull26WitnessCodec.begin(I,r)
        try
          val failure = assertThrows(classOf[Exception], () => partial.decode(bad))
          assertSame(failure,partial.failure.get)
          val replay = assertThrows(classOf[IllegalStateException], () => partial.decode(body))
          assertSame(failure,replay.getCause); assertSame(failure,partial.failure.get)
        finally partial.close()
    finally
      target.storageBudget.closeScopedReadBudget(); before.storageBudget.closeScopedReadBudget(); owner.close()
  }

  @Test def everyFullReferenceHashLengthAttemptPhaseAndLogicalOperationIsChecked(): Unit = withFixture(true) { (r,f) =>
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    try
      owner.encode(f.record,f.before,f.target)
      val body = encoded(owner)
      owner.requireReference(reference(f,body))
      for field <- 0 to 9 do assertThrows(classOf[java.sql.SQLDataException], () => owner.requireReference(reference(f,body,field)))
      assertThrows(classOf[java.sql.SQLDataException], () => owner.requireReference(null))
    finally owner.close()
  }

  @Test def swappedWholeImagesAndActualManifestOrReceiptMismatchFailBeforePublication(): Unit = withFixture(false) { (r,f) =>
    def rejected(record: B1ResumeRecord, before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Unit =
      val owner = MySqlFull26WitnessCodec.begin(I,r)
      try
        val cause = assertThrows(classOf[Exception], () => owner.encode(record,before,target))
        assertSame(cause,owner.failure.get)
        assertThrows(classOf[IllegalStateException], () => owner.withData((_,_,_,_,_) => ()))
      finally owner.close()
    rejected(f.record,f.target,f.before)
    val changedC = capture(update(f.target.relations,M,"configuration_sha256",binary(Array.fill[Byte](32)(88))),r)
    rejected(f.record,f.before,changedC)
    val changedD = capture(update(f.target.relations,M,"ddl_sha256",binary(Array.fill[Byte](32)(89))),r)
    rejected(f.record,f.before,changedD)
    val changedGraph = capture(update(f.target.relations,"fp_script_catalog","catalog_revision",MySqlFormat3Cell.SignedInteger(7L)),r)
    rejected(f.record,f.before,changedGraph)
    val p = new WindowsWorldProvisionReceiptDataCodec.PlanData(Plan.creationOperationUuid(),id("c721"),Plan.prepareWriteUuid(),Plan.certifyWriteUuid(),
      Plan.bindingUuid(),2,Plan.mysqlServerUuid(),Plan.schema(),5,Plan.ddlSha256(),Plan.provisioningUuid())
    val data = WindowsWorldProvisionReceiptDataCodec.decode(Receipt)
    val wrongReceipt = WindowsWorldProvisionReceiptDataCodec.encode(new WindowsWorldProvisionReceiptDataCodec.ReceiptData(p,data.sourceIdentity(),data.guardIdentity(),data.creatorPid(),data.creatorCreationTime(),data.creationBoot()))
    val record = new B1ResumeRecord(0L,new Array[Byte](32),Kind.GENESIS,f.record.sourceAnchorSha256(),wrongReceipt,Namespace,f.record.assertions())
    rejected(record,f.before,f.target)
  }

  @Test def partialOwnersRefundAfterBudgetFailureAndRejectForeignOrClosedRoots(): Unit = withFixture(false) { (r,f) =>
    val ledger = new MySqlFormat3AllocationLedger(600L)
    val limited = root(I,ledger)
    val partial = MySqlFull26WitnessCodec.begin(I,limited)
    try
      val failure = assertThrows(classOf[Exception], () => partial.decode(Array[Byte](1)))
      assertSame(failure,partial.failure.get)
    finally partial.close()
    assertEquals(0L,ledger.accountedBytes)
    limited.closeRootReadBudget()
    val foreign = root(I)
    val owner = MySqlFull26WitnessCodec.begin(I,foreign)
    try assertThrows(classOf[java.sql.SQLDataException], () => owner.encode(f.record,f.before,f.target))
    finally owner.close(); foreign.closeRootReadBudget()
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull26WitnessCodec.begin(I,foreign))
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull26WitnessCodec.begin(Inputs,r))
    val equalProfile = SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,2_000_000L,16_000_000L,4_000_000L,2_000_000L)
    val otherInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(I.schema,I.ddlSha256,equalProfile,I.budget,
      MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 1_000_000L,diagnosticsBytes = 1_000_000L))
    assertArrayEquals(I.sourceLimitProfile.canonicalBytes(),equalProfile.canonicalBytes())
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull26WitnessCodec.begin(otherInputs,r))
  }

  @Test def bodyCloseDropsOwnedChildrenBeforeRefundAndConsumerFailureKeepsLiveOwner(): Unit = withFixture(false) { (r,f) =>
    // Cache caller-owned fingerprints before taking its accounting baseline.
    f.before.canonicalSha256; f.target.canonicalSha256
    val baseline = r.accountedMemoryBytes
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    owner.encode(f.record,f.before,f.target)
    val marker = new IOException("actual caller consumer failure")
    val actual = assertThrows(classOf[IOException], () => owner.withData((_,_,_,_,_) => throw marker))
    assertSame(marker,actual)
    assertTrue(owner.failure.isEmpty)
    owner.withData((_,before,_,_,_) => assertTrue(before.exactlyEquals(f.before)))
    owner.close(); owner.close()
    assertEquals(baseline,r.accountedMemoryBytes)
    assertFalse(owner.hasUnsettledResources)
    assertThrows(classOf[IllegalStateException], () => owner.writeBodyTo(new ByteArrayOutputStream()))
  }

  @Test def sameLedgerSiblingAndClosedCanonicalChildCannotSupplyBodyImages(): Unit = withFixture(false) { (_,f) =>
    val ledger = new MySqlFormat3AllocationLedger(I.sourceLimitProfile.maxRetainedBytes())
    val first = root(I,ledger); val sibling = root(I,ledger)
    val image = capture(seedRows,sibling)
    assertTrue(image.allocationBudget.sharesAllocationLedger(first))
    val owner = MySqlFull26WitnessCodec.begin(I,first)
    try assertThrows(classOf[java.sql.SQLDataException], () => owner.encode(f.record,image,image))
    finally owner.close(); sibling.closeRootReadBudget(); first.closeRootReadBudget()
    val actualRoot = root(I)
    val fixture = prepareFixture(actualRoot)
    fixture.target.allocationBudget.closeScopedReadBudget()
    val closedImage = MySqlFull26WitnessCodec.begin(I,actualRoot)
    try assertThrows(classOf[java.sql.SQLDataException], () => closedImage.encode(fixture.record,fixture.before,fixture.target))
    finally closedImage.close(); actualRoot.closeRootReadBudget()
  }

  @Test def callerPrematureRootClosePreservesPrimaryAndReportsSuppressedCleanupWithoutRetry(): Unit =
    val r = root(I)
    val owner = MySqlFull26WitnessCodec.begin(I,r)
    val primary = assertThrows(classOf[Exception], () => owner.decode(Array[Byte](1)))
    r.closeRootReadBudget() // Actual misuse, not an injected cleanup success/failure hook.
    val cleanup = assertThrows(classOf[IllegalStateException], () => owner.close())
    assertSame(primary,owner.failure.get)
    assertSame(owner.failedCleanup.get,cleanup.getCause)
    assertTrue(primary.getSuppressed.exists(_ eq cleanup.getCause))
    assertTrue(owner.hasUnsettledResources)
    val retry = assertThrows(classOf[IllegalStateException], () => owner.close())
    assertSame(cleanup.getCause,retry.getCause)

  @Test def actualCaptureOrCommonSeamRejectsOrphanPairAndWriterOutsideManifestBinding(): Unit =
    val r = root(I)
    try
      val rows = rebound(full26GenesisRelations(I,catalogPresent = true))
      for table <- Vector(B,B1) do
        assertThrows(classOf[Exception], () => {
          val image = capture(replace(rows,table,Vector.empty),r)
          val graph = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(image,I,r)
          graph.storageBudget.get.closeScopedReadBudget()
        })
      assertThrows(classOf[Exception], () => {
        val image = capture(update(seedRows,W,"binding_uuid",binary(uuidBytes(id("c799")))),r)
        val graph = MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(image,I,r)
        graph.storageBudget.get.closeScopedReadBudget()
      })
    finally r.closeRootReadBudget()
