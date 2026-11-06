package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import com.wi.floatingpod.server.B1ResumeRecord
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.charset.{CharacterCodingException, StandardCharsets}
import java.security.MessageDigest
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

private[mysql] object MySqlB1CommonGraphTestFixtures:
  import MySqlScriptSchemaDataTestFixture.*

  def inputsWithSnapshotBudget(bytes: Long): MySqlScriptSchemaInputs =
    MySqlScriptSchemaInputs.createDeploymentReceiptFull26(Schema, Full26DdlSha256, SourceProfile,
      Inputs.budget.copy(snapshotBytes = bytes), Full26Inputs.deploymentBudget.get)

  def ordinaryHandoff(
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      largeHistoryBytes: Int = 0,
      changeManifest: Vector[MySqlFormat3RelationSnapshot] => Vector[MySqlFormat3RelationSnapshot] =
        (rows: Vector[MySqlFormat3RelationSnapshot]) => rows
  ): (MySqlScriptSchemaSnapshot, MySqlScriptSchemaSnapshot, MySqlScriptSchemaB1Graph.Data) =
    var beforeRows = full26OrdinaryRelations(inputs)
    if largeHistoryBytes > 0 then
      val shape = inputs.forTable("physical_source_obligation")
      val relationIndex = beforeRows.indexWhere(_.table == shape.table)
      val relation = beforeRows(relationIndex)
      val key = Array.fill[Byte](largeHistoryBytes)(0x42.toByte)
      val digest = MessageDigest.getInstance("SHA-256").digest(key)
      val keyIndex = shape.columns.indexWhere(_.name == "carrier_key")
      val digestIndex = shape.columns.indexWhere(_.name == "carrier_key_hash")
      val row = relation.rows.head.updated(keyIndex, binary(key)).updated(digestIndex, binary(digest))
      beforeRows = beforeRows.updated(relationIndex, relation.copy(rows = Vector(row)))
    beforeRows = changeManifest(beforeRows)
    val writerShape = inputs.forTable("fp_runtime_writer")
    val epochIndex = writerShape.columns.indexWhere(_.name == "writer_epoch")
    val beforeEpoch = beforeRows.find(_.table == writerShape.table).flatMap(_.rows.headOption)
      .map(_(epochIndex)) match
        case Some(MySqlFormat3Cell.SignedInteger(value)) => value
        case _ => throw new IllegalStateException("ordinary full26 handoff requires an integer before writer epoch")
    val targetRows = beforeRows.map { relation =>
      if relation.table == "fp_runtime_writer" then
        relation.copy(rows = Vector(relation.rows.head.updated(epochIndex,
          MySqlFormat3Cell.SignedInteger(Math.addExact(beforeEpoch, 1L)))))
      else relation
    }
    val before = snapshot(inputs, root, beforeRows)
    val target = snapshot(inputs, root, targetRows)
    val data = MySqlScriptSchemaB1Graph.deploymentReceiptFull26OrdinaryWriterHandoffData(before, target, root)
    (before, target, data)

  def operationFacts(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): (UUID, com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.OperationPhase, Boolean) =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    val child = root.newCanonicalDataScope()
    var primary: Throwable = null
    try
      val values = row.zip(shape.columns).map { (cell, column) =>
        cell match
          case MySqlFormat3Cell.SqlNull => MySqlDeploymentOperationRows.ColumnValue.SqlNull
          case MySqlFormat3Cell.SignedInteger(value) => MySqlDeploymentOperationRows.ColumnValue.Integer(value)
          case MySqlFormat3Cell.Binary(value) => MySqlDeploymentOperationRows.ColumnValue.Binary(
            value.copyBytes(child, s"Full26 common test ${shape.table}.${column.name} decoder input"))
          case _ => throw new AssertionError(s"unexpected deployment operation cell ${shape.table}.${column.name}")
      }
      def uuid(column: String): UUID = values(MySqlDeploymentOperationRows.columnNames.indexOf(column)) match
        case MySqlDeploymentOperationRows.ColumnValue.Binary(bytes) =>
          val buffer = java.nio.ByteBuffer.wrap(bytes)
          new UUID(buffer.getLong(), buffer.getLong())
        case _ => throw new AssertionError(s"operation $column must be binary")
      val decoded = MySqlDeploymentOperationRows.decode(
        MySqlDeploymentOperationRows.ColumnValues(values), uuid("actor_uuid"), uuid("operation_uuid"), child)
      (decoded.actorUuid, decoded.receipt.phase(), decoded.copyBinary("diagnostic_bytes", child).isDefined)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => child.closeScopedReadBudget()))

/** 直接调用完整typed encoder/comparator/Java adapter；不模拟SQL或发行任何能力。 */
final class MySqlB1CommonGraphTest:
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")
  private val other = UUID.fromString("20000000-0000-0000-0000-000000000002")
  private val h = Vector.fill[Byte](32)(1)
  private val h2 = Vector.fill[Byte](32)(2)
  private def manifest = MySqlLockedManifest(1, "MYSQL", 2, h, id, "READY", id, id,
    Some(h), "NONE", Some(id), Some(h), Some(h), Some(h))
  private def repo = MySqlFullRepositorySnapshot.empty
  private def capture(m: MySqlLockedManifest = manifest) = MySqlB1CommonGraph.capture(m, None, repo)
  private def text(in: DataInputStream): String =
    val length = in.readInt(); assertTrue(length >= 0)
    val bytes = in.readNBytes(length); assertEquals(length, bytes.length)
    new String(bytes, StandardCharsets.UTF_8)
  private def uuid(in: DataInputStream): UUID = new UUID(in.readLong(), in.readLong())
  private def row = MySqlStoredRowData(MySqlStoredRows.encodeUuid(id), None, 0L, "world",
    0.0d, 64.0d, 0.0d, 0.0d, 0.0d, 0, 10L, None, "WORLD_IDLE")
  private def withRows(rows: Vector[MySqlStoredRowData]) = repo.copy(pods = rows)

  @Test def manifestEncodingReadsEveryActualColumnInFixedOrder(): Unit =
    val graph = capture(); val in = new DataInputStream(new ByteArrayInputStream(graph.manifestBytes.toArray))
    assertEquals(MySqlB1CommonGraph.ManifestEncoding, text(in))
    assertEquals(1, in.readInt()); assertEquals("MYSQL", text(in)); assertEquals(2, in.readInt())
    assertArrayEquals(h.toArray, in.readNBytes(32)); assertEquals(id, uuid(in))
    assertEquals("READY", text(in)); assertEquals(id, uuid(in)); assertEquals(id, uuid(in))
    assertEquals(1, in.readUnsignedByte()); assertArrayEquals(h.toArray, in.readNBytes(32))
    assertEquals("NONE", text(in)); assertEquals(1, in.readUnsignedByte()); assertEquals(id, uuid(in))
    for ignored <- 0 until 3 do
      assertEquals(1, in.readUnsignedByte()); assertArrayEquals(h.toArray, in.readNBytes(32))
    assertEquals(-1, in.read()); assertEquals(manifest, graph.manifest)
    assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(graph.canonicalBytes.toArray), graph.canonicalSha256.toArray)

  @Test def eachOfFourteenManifestFieldsChangesCompleteBytesAndExactComparison(): Unit =
    val m = manifest
    val variants = Vector(m.copy(singleton = 2), m.copy(backend = "OTHER"), m.copy(formatVersion = 1),
      m.copy(ddlSha256 = h2), m.copy(provisioningUuid = other), m.copy(bootstrapState = "BOOTSTRAPPING"),
      m.copy(serverUuid = other), m.copy(bindingUuid = other), m.copy(importManifestSha256 = Some(h2)),
      m.copy(importState = "IMPORTED"), m.copy(importOperationUuid = Some(other)),
      m.copy(importSnapshotSha256 = Some(h2)), m.copy(importReceiptSha256 = Some(h2)), m.copy(guardManifestSha256 = Some(h2)))
    assertEquals(14, variants.size)
    val before = capture(m)
    variants.foreach { changed =>
      val after = capture(changed)
      assertNotEquals(before.manifestBytes, after.manifestBytes)
      assertFalse(before.exactlyEquals(after)); assertNotEquals(before.canonicalBytes, after.canonicalBytes)
    }

  @Test def everyNullableManifestFieldPreservesNullInsteadOfZeroHashOrUuid(): Unit =
    val m = manifest
    Vector(m.copy(importManifestSha256 = None), m.copy(importOperationUuid = None),
      m.copy(importSnapshotSha256 = None), m.copy(importReceiptSha256 = None), m.copy(guardManifestSha256 = None)).foreach { absent =>
      assertFalse(capture(m).exactlyEquals(capture(absent)))
    }
    val zeros = m.copy(importManifestSha256 = Some(Vector.fill[Byte](32)(0)), importOperationUuid = Some(new UUID(0, 0)))
    assertFalse(capture(zeros).exactlyEquals(capture(zeros.copy(importManifestSha256 = None))))
    assertFalse(capture(zeros).exactlyEquals(capture(zeros.copy(importOperationUuid = None))))

  @Test def absenceAndAllBindingColumnsHaveIndependentCompleteEncoding(): Unit =
    val absent = capture(); val in = new DataInputStream(new ByteArrayInputStream(absent.bindingBytes.toArray))
    assertEquals(MySqlB1CommonGraph.BindingEncoding, text(in)); assertEquals(0, in.readUnsignedByte()); assertEquals(-1, in.read())
    def graph(b: PhysicalRecoveryBinding) = MySqlB1CommonGraph.capture(manifest, Some(b),
      repo.copy(recovery = repo.recovery.copy(binding = Some(b))))
    val b = PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.Certified, 2)
    val present = graph(b); val body = new DataInputStream(new ByteArrayInputStream(present.bindingBytes.toArray))
    assertEquals(MySqlB1CommonGraph.BindingEncoding, text(body)); assertEquals(1, body.readUnsignedByte())
    assertEquals(1, body.readInt()); assertEquals(id, uuid(body)); assertEquals("CERTIFIED", text(body)); assertEquals(2, body.readInt()); assertEquals(-1, body.read())
    assertFalse(absent.exactlyEquals(present))
    Vector(b.copy(bindingUuid = other), b.copy(cutoverState = PhysicalRecoveryCutoverState.LegacyUncertified), b.copy(formatVersion = 1)).foreach { v =>
      assertNotEquals(present.bindingBytes, graph(v).bindingBytes)
    }

  @Test def contradictoryIndependentBindingIsRejectedBeforeIssuingFacts(): Unit =
    val b = PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.Certified, 2)
    assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraph.capture(manifest, Some(b), repo))

  @Test def repositoryProtocolAndFullBytesAreReusedAndOrderedFactsCompareCanonically(): Unit =
    val a = withRows(Vector(row, row.copy(uuidBytes = MySqlStoredRows.encodeUuid(other))))
    val b = a.copy(pods = a.pods.reverse)
    val ga = MySqlB1CommonGraph.capture(manifest, None, a); val gb = MySqlB1CommonGraph.capture(manifest, None, b)
    assertEquals(a.canonicalBytes, ga.repositoryBytes); assertEquals(a.canonicalSha256, ga.repositorySha256)
    assertTrue(ga.exactlyEquals(gb)); assertTrue(ga.matchesResumeAssertions(gb.toResumeAssertions))
    val ent = a.copy(entitlements = Vector(PlayerEntitlement(id, 5, 2)))
    assertFalse(ga.exactlyEquals(MySqlB1CommonGraph.capture(manifest, None, ent)))

  @Test def allPodScalarsNullableValuesAndExactPrecisionChangeFullRepositoryComponent(): Unit =
    val original = row; val base = MySqlB1CommonGraph.capture(manifest, None, withRows(Vector(original)))
    val variants = Vector(original.copy(uuidBytes = MySqlStoredRows.encodeUuid(other)), original.copy(ownerUuidBytes = Some(MySqlStoredRows.encodeUuid(other))),
      original.copy(stateVersion = Long.MaxValue), original.copy(worldName = " world\u0000世界 "),
      original.copy(posX = -0.0d), original.copy(posY = Math.nextUp(64.0d)), original.copy(posZ = 1.0d),
      original.copy(yaw = Math.nextUp(0.0f).toDouble), original.copy(pitch = -0.0d), original.copy(isActiveFlag = 1, dimensionId = Some(0)),
      original.copy(lastUpdated = Long.MaxValue), original.copy(dimensionId = Some(0)), original.copy(operatingModeToken = "NORMAL"))
    variants.foreach { v =>
      val graph = MySqlB1CommonGraph.capture(manifest, None, withRows(Vector(v)))
      assertFalse(base.exactlyEquals(graph)); assertNotEquals(base.repositoryBytes, graph.repositoryBytes)
    }

  @Test def adapterCarriesFullProtocolsAndArraysCannotMutateStoredFacts(): Unit =
    val graph = capture(); val dto = graph.toResumeAssertions
    assertTrue(graph.matchesResumeAssertions(dto))
    assertEquals(MySqlB1CommonGraph.RepositoryEncoding, dto.repositoryEncoding())
    val original = graph.canonicalBytes
    for bytes <- Vector(dto.manifestBytes(), dto.bindingBytes(), dto.repositoryBytes()) do bytes(0) = (bytes(0) ^ 1).toByte
    assertTrue(graph.matchesResumeAssertions(dto)); assertEquals(original, graph.canonicalBytes)
    val changed = dto.manifestBytes(); changed(0) = (changed(0) ^ 1).toByte
    val forged = new B1ResumeRecord.CommonGraphAssertions(dto.manifestEncoding(), changed,
      dto.bindingEncoding(), dto.bindingBytes(), dto.repositoryEncoding(), dto.repositoryBytes())
    assertFalse(graph.matchesResumeAssertions(forged)); assertFalse(graph.matchesResumeAssertions(null))
    val hashOnly = new B1ResumeRecord.CommonGraphAssertions(dto.manifestEncoding(), graph.manifestSha256.toArray,
      dto.bindingEncoding(), graph.bindingSha256.toArray, dto.repositoryEncoding(), graph.repositorySha256.toArray)
    assertFalse(graph.matchesResumeAssertions(hashOnly))
    val wrongProtocol = new B1ResumeRecord.CommonGraphAssertions("different/v1", dto.manifestBytes(), dto.bindingEncoding(), dto.bindingBytes(), dto.repositoryEncoding(), dto.repositoryBytes())
    assertFalse(graph.matchesResumeAssertions(wrongProtocol))

  @Test def textAndOptionsRejectMalformedInputAndOversizeBeforeReturningGraph(): Unit =
    assertThrows(classOf[CharacterCodingException], () => capture(manifest.copy(bootstrapState = "\uD800")))
    assertThrows(classOf[IllegalArgumentException], () => capture(manifest.copy(ddlSha256 = Vector.empty)))
    assertThrows(classOf[IllegalArgumentException], () => capture(manifest.copy(importManifestSha256 = null)))
    assertThrows(classOf[IllegalArgumentException], () => capture(manifest.copy(importManifestSha256 = Some(null))))
    assertThrows(classOf[IllegalArgumentException], () => capture(manifest.copy(serverUuid = null)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraph.capture(manifest, null, repo))
    assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraph.capture(manifest, None, withRows(Vector(row.copy(worldName = "x" * 65536)))))
    assertFalse(capture(manifest.copy(bootstrapState = "READY ")).exactlyEquals(capture()))

  @Test def bootstrapHistoryIsRetainedThroughExistingFullRepositoryCanonical(): Unit =
    val b = PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.LegacyUncertified, 2)
    val certify = UUID.fromString("30000000-0000-0000-0000-000000000003")
    val intent = MySqlBootstrapIntentData(1, id, other, certify, id, 2, other,
      "FP_WORLD_SOURCE_V2/exact 世界", h, MySqlBootstrapIntentStage.Prepared, None)
    def graph(i: MySqlBootstrapIntentData) =
      val snapshot = repo.copy(recovery = repo.recovery.copy(binding = Some(b)), bootstrapIntents = Vector(i))
      val result = MySqlB1CommonGraph.capture(manifest, Some(b), snapshot)
      assertEquals(snapshot.canonicalBytes, result.repositoryBytes)
      result
    val before = graph(intent)
    Vector(intent.copy(bootstrapOperationUuid = certify), intent.copy(prepareWriteUuid = id),
      intent.copy(certifyWriteUuid = id), intent.copy(provisioningUuid = id),
      intent.copy(sourceWorldIdentity = intent.sourceWorldIdentity + " "), intent.copy(provisionReceiptSha256 = h2)).foreach { changed =>
      assertFalse(before.exactlyEquals(graph(changed)))
    }
    val completeBinding = b.copy(cutoverState = PhysicalRecoveryCutoverState.Certified)
    val complete = intent.copy(stage = MySqlBootstrapIntentStage.Complete, markerAckSha256 = Some(h2))
    val completedSnapshot = repo.copy(recovery = repo.recovery.copy(binding = Some(completeBinding)), bootstrapIntents = Vector(complete))
    val completed = MySqlB1CommonGraph.capture(manifest, Some(completeBinding), completedSnapshot)
    assertFalse(before.exactlyEquals(completed))
    assertEquals(completedSnapshot.canonicalBytes, completed.repositoryBytes)

  @Test def completeCommonFramingContainsAllThreeFullComponentsAndNoTrailingBytes(): Unit =
    val graph = capture()
    val in = new DataInputStream(new ByteArrayInputStream(graph.canonicalBytes.toArray))
    assertEquals(MySqlB1CommonGraph.CommonEncoding, text(in))
    def component(expected: Vector[Byte]): Unit =
      val size = in.readInt(); assertEquals(expected.size, size)
      assertArrayEquals(expected.toArray, in.readNBytes(size))
    component(graph.manifestBytes); component(graph.bindingBytes)
    assertEquals(MySqlB1CommonGraph.RepositoryEncoding, text(in)); component(graph.repositoryBytes)
    assertEquals(-1, in.read()); assertTrue(graph.exactlyEquals(capture()))

  @Test def deploymentReceiptFull26CommonGraphRetainsOrdinaryImageAndAllTypedHistory(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val root = MySqlScriptSchemaDataTestFixture.root(Full26Inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    var graph: MySqlB1CommonGraph = null
    var beforeGraph: MySqlB1CommonGraph = null
    try
      val (before, target, handoff) = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(Full26Inputs, root)
      data = handoff
      graph = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target)
      assertSame(target, graph.deploymentReceiptSnapshot.get)
      assertTrue(target.relations.size == 26)
      val operation = target.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable)
      assertEquals(2, operation.rows.size)
      assertEquals(31, operation.rows.head.size)
      val terminal = MySqlB1CommonGraphTestFixtures.operationFacts(operation.rows(0), root)
      val unknown = MySqlB1CommonGraphTestFixtures.operationFacts(operation.rows(1), root)
      assertEquals(com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.OperationPhase.APPLIED, terminal._2)
      assertEquals(com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.OperationPhase.UNKNOWN, unknown._2)
      assertNotEquals(terminal._1, unknown._1)
      assertFalse(terminal._3)
      assertTrue(unknown._3)
      assertEquals(2L, target.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(2)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertEquals(11L, target.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(3)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertThrows(classOf[IllegalStateException], () => graph.repository)
      assertEquals(MySqlB1CommonGraph.DeploymentReceiptFull26RepositoryEncoding,
        graph.toResumeAssertions.repositoryEncoding())
      assertTrue(graph.matchesResumeAssertions(graph.toResumeAssertions))
      assertEquals(MySqlB1CommonGraph.sha256(graph.repositoryBytes), graph.repositorySha256)
      beforeGraph = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Before)
      assertFalse(graph.exactlyEquals(beforeGraph))
    finally
      if beforeGraph != null then beforeGraph.storageBudget.foreach(_.closeScopedReadBudget())
      if graph != null then graph.storageBudget.foreach(_.closeScopedReadBudget())
      if data != null then data.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()

  @Test def deploymentReceiptFull26RejectsMisalignedManifestBindingAndLegacyLayout(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val root = MySqlScriptSchemaDataTestFixture.root(Full26Inputs)
    try
      def attempt(change: Vector[MySqlFormat3RelationSnapshot] => Vector[MySqlFormat3RelationSnapshot]): Unit =
        val created = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(Full26Inputs, root,
          changeManifest = change)._3
        try
          val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
            MySqlB1CommonGraph.captureDeploymentReceiptFull26(created,
              MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target))
          assertTrue(failure.getStackTrace.nonEmpty)
        finally created.storageBudget.closeScopedReadBudget()
      val manifestShape = Full26Inputs.forTable("fp_schema_manifest")
      val ddlIndex = manifestShape.columns.indexWhere(_.name == "ddl_sha256")
      val bindingIndex = manifestShape.columns.indexWhere(_.name == "expected_binding_uuid")
      attempt(_.map(r => if r.table == "fp_schema_manifest" then
        r.copy(rows = Vector(r.rows.head.updated(ddlIndex, binary(Array.fill[Byte](32)(0x61.toByte))))) else r))
      attempt(_.map(r => if r.table == "fp_schema_manifest" then
        r.copy(rows = Vector(r.rows.head.updated(bindingIndex, binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000000999")))))) else r))

      val oldRoot = MySqlScriptSchemaDataTestFixture.root()
      val oldBeforeRows = completeRelations
      val writerShape = Inputs.forTable("fp_runtime_writer")
      val epoch = writerShape.columns.indexWhere(_.name == "writer_epoch")
      val oldTargetRows = oldBeforeRows.map(r => if r.table == "fp_runtime_writer" then
        r.copy(rows = Vector(r.rows.head.updated(epoch, MySqlFormat3Cell.SignedInteger(1L)))) else r)
      val oldBefore = snapshot(oldRoot, oldBeforeRows)
      val oldTarget = snapshot(oldRoot, oldTargetRows)
      val oldData = MySqlScriptSchemaB1Graph.ordinaryWriterHandoffData(oldBefore, oldTarget, oldRoot)
      try
        assertThrows(classOf[IllegalArgumentException], () => MySqlB1CommonGraph.captureDeploymentReceiptFull26(
          oldData, MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target))
      finally
        oldData.storageBudget.closeScopedReadBudget()
        oldRoot.closeRootReadBudget()
    finally
      root.closeRootReadBudget()

  @Test def deploymentReceiptFull26CommonComponentCommitsLargeImageWithoutEmbeddingIt(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    val inputs = MySqlB1CommonGraphTestFixtures.inputsWithSnapshotBudget(500_000L)
    val root = MySqlScriptSchemaDataTestFixture.root(inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    var graph: MySqlB1CommonGraph = null
    try
      val (_, target, handoff) = MySqlB1CommonGraphTestFixtures.ordinaryHandoff(inputs, root, largeHistoryBytes = 70_000)
      data = handoff
      graph = MySqlB1CommonGraph.captureDeploymentReceiptFull26(data,
        MySqlB1CommonGraph.DeploymentReceiptFull26Selection.Target)
      assertTrue(target.canonicalByteLength > MySqlB1CommonGraph.MaxCanonicalBytes)
      assertTrue(graph.repositoryBytes.size < MySqlB1CommonGraph.MaxCanonicalBytes)
      assertTrue(graph.canonicalBytes.size <= MySqlB1CommonGraph.MaxCanonicalBytes)
      assertEquals(target.canonicalSha256, graph.deploymentReceiptSnapshot.get.canonicalSha256)
    finally
      if graph != null then graph.storageBudget.foreach(_.closeScopedReadBudget())
      if data != null then data.storageBudget.closeScopedReadBudget()
      root.closeRootReadBudget()
