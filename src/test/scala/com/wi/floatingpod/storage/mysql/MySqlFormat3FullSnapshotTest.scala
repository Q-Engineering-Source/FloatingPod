package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertNotEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.io.{ByteArrayInputStream, IOException, InputStream, StringReader}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MySqlFormat3FullSnapshotTest:
  private val testSchema = "fp_format3_reader_test"

  @Test def pendingDeploymentAssemblyRootChargesStaticDataButRejectsAllLayoutCapabilities(): Unit =
    val p=MySqlScriptSchemaDataTestFixture.SourceProfile
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(p,p.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(p.maxRetainedBytes()))
    try
      root.reserveRetainedMemory(32L,"synthetic static holder")
      root.reserveTemporaryMemory(64L,"synthetic static parse")
      val before=root.accountedMemoryBytes
      val i=MySqlScriptSchemaDataTestFixture.Full26Inputs
      val c=MySqlScriptSchemaDataTestFixture.Connection
      val s=MySqlScriptSchemaDataTestFixture.Scope
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.requireFormat3Full20Layout())
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.initialize(i.schema,c,s,i.budget.packetBytes))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.issueDeploymentReceiptSnapshotImage(i,i.schema,c,s,i.budget.packetBytes))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.newCanonicalScope())
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.newCanonicalDataScope())
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.newScopedReadBudget(i.budget.packetBytes))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.beginRawBorrow())
      assertThrows(classOf[Exception],()=>root.beginRow(1,"fp_schema_manifest",0L))
      assertEquals(before,root.accountedMemoryBytes)
      root.releaseTemporaryMemory(64L,"synthetic static parse ended")
      root.releaseRetainedMemory(32L,"synthetic static holder dropped")
    finally root.closeRootReadBudget()

  @Test def relationShapesMatchPinnedDefinitionsAndKeepCanonicalOrderSeparateFromLockOrder(): Unit =
    val source = resource("/com/wi/floatingpod/storage/mysql/format3-relation-definitions.json")
    assertEquals(MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256, sha256(source))
    val definitions = JsonParser.parseString(new String(source, StandardCharsets.UTF_8)).getAsJsonObject
    val canonicalNames = definitions.entrySet().asScala.toVector.map(_.getKey)
    assertEquals(canonicalNames, MySqlFormat3RelationShapes.canonicalOrder.map(_.table))
    assertEquals(MySqlFormat3RelationShapes.ExpectedRelationCount, canonicalNames.size)

    var columnCount = 0
    definitions.entrySet().asScala.foreach { entry =>
      val table = entry.getKey
      val definition = entry.getValue.getAsJsonObject
      val columns = definition.getAsJsonArray("cols").asScala.toVector.map { value =>
        val pair = value.getAsJsonArray
        pair.get(0).getAsString -> pair.get(1).getAsString
      }
      val shape = MySqlFormat3RelationShapes.forTable(table)
      assertEquals(columns.map(_._1), shape.columns.map(_.name), table)
      assertEquals(columns.map(_._2), shape.columns.map(_.sqlDeclaration), table)
      assertEquals(definition.getAsJsonArray("pk").asScala.toVector.map(_.getAsString), shape.primaryKey, table)
      columnCount = Math.addExact(columnCount, columns.size)
    }
    assertEquals(MySqlFormat3RelationShapes.ExpectedColumnCount, columnCount)
    val forestChecks = definitions.getAsJsonObject("physical_source_forest").getAsJsonArray("checks").asScala.map(_.getAsString).toVector
    assertTrue(forestChecks.contains("forest_codec='FP_SOURCE_FOREST_BE_V2'"))

    val lockedRead = resource("/com/wi/floatingpod/storage/mysql/format3-full20-locked-read.sql")
    assertEquals(MySqlFormat3RelationShapes.SourceLockedReadSha256, sha256(lockedRead))
    val text = new String(lockedRead, StandardCharsets.UTF_8)
    val statements = text.linesIterator.filterNot(_.trim.startsWith("--")).mkString("\n")
      .split(";").iterator.map(_.trim).filter(_.nonEmpty).toVector
    assertEquals(MySqlFormat3RelationShapes.ExpectedRelationCount, statements.size)
    val expected = statements.map(_.replace("`fp_test_20261007_format3_b2`", s"`$testSchema`") + ";")
    val generated = MySqlFormat3RelationShapes.lockedReadOrder.map(shape => MySqlFormat3FullSnapshotSql.select(testSchema, shape) + ";")
    assertEquals(expected, generated)
    assertNotEquals(MySqlFormat3RelationShapes.lockedReadOrder.map(_.table), canonicalNames)

  @Test def forestUuidIndexPreservesUnknownVsPresentPayloadAndValidatesTheNullablePair(): Unit =
    val forest = MySqlFormat3RelationShapes.forTable("physical_source_forest")
    val payloadIndex = forest.columns.indexWhere(_.name == "fp_pod_uuid_index_bytes")
    val digestIndex = forest.columns.indexWhere(_.name == "fp_pod_uuid_index_sha256")
    val payloadColumn = forest.columns(payloadIndex)
    val digestColumn = forest.columns(digestIndex)

    assertEquals("LONGBLOB NULL", payloadColumn.sqlDeclaration)
    assertEquals(MySqlFormat3SqlKind.Binary, payloadColumn.kind)
    assertTrue(payloadColumn.nullable)
    assertEquals(None, payloadColumn.maximumLength)
    assertEquals("VARBINARY(32) NULL", digestColumn.sqlDeclaration)
    assertEquals(MySqlFormat3SqlKind.Binary, digestColumn.kind)
    assertTrue(digestColumn.nullable)
    assertEquals(Some(32), digestColumn.maximumLength)

    val forestBytes = Array[Byte](0x41, 0x42, 0x43)
    val rowBase = defaultRow(forest)
      .updated(forest.columns.indexWhere(_.name == "forest_state"), MySqlFormat3Cell.Text("DRAFT"))
      .updated(forest.columns.indexWhere(_.name == "forest_codec"), MySqlFormat3Cell.Text("FP_SOURCE_FOREST_BE_V2"))
      .updated(forest.columns.indexWhere(_.name == "forest_bytes"), MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(forestBytes)))
      .updated(forest.columns.indexWhere(_.name == "forest_sha256"),
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MessageDigest.getInstance("SHA-256").digest(forestBytes))))

    def snapshotForIndex(payload: MySqlFormat3Cell, digest: MySqlFormat3Cell): MySqlFormat3FullSnapshot =
      val row = rowBase.updated(payloadIndex, payload).updated(digestIndex, digest)
      snapshotWith(extraRows = Map(forest.table -> Vector(row)))

    val unknown = snapshotForIndex(MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull)
    val emptyPayload = Array.emptyByteArray
    val emptyDigest = MessageDigest.getInstance("SHA-256").digest(emptyPayload)
    val presentEmpty = snapshotForIndex(
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(emptyPayload)),
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(emptyDigest))
    )
    assertNotEquals(unknown.canonicalSha256Hex, presentEmpty.canonicalSha256Hex)

    val indexedBytes = Array.tabulate[Byte](33)(index => (index + 1).toByte)
    val indexedDigest = MessageDigest.getInstance("SHA-256").digest(indexedBytes)
    val indexed = snapshotForIndex(
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(indexedBytes)),
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(indexedDigest))
    )
    assertNotEquals(presentEmpty.canonicalSha256Hex, indexed.canonicalSha256Hex)

    val alteredBytes = indexedBytes.clone()
    alteredBytes(0) = (alteredBytes(0) ^ 0x7f).toByte
    val altered = snapshotForIndex(
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(alteredBytes)),
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MessageDigest.getInstance("SHA-256").digest(alteredBytes)))
    )
    assertNotEquals(indexed.canonicalSha256Hex, altered.canonicalSha256Hex)

    assertThrows(classOf[java.sql.SQLDataException], () =>
      snapshotForIndex(MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array[Byte](1))), MySqlFormat3Cell.SqlNull))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      snapshotForIndex(MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(emptyDigest.clone()))))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      snapshotForIndex(
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(indexedBytes.clone())),
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](31)(0x01)))
      ))

  @Test def fullSnapshotPreservesEveryRelationRowAndHistoryWithoutJoinFiltering(): Unit =
    val obligation = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val first = defaultRow(obligation).updated(obligation.columns.indexWhere(_.name == "binding_uuid"), binaryCell(0x22.toByte, 16))
      .updated(obligation.columns.indexWhere(_.name == "carrier_key_hash"), binaryCell(0x01.toByte, 32))
    val second = first.updated(obligation.columns.indexWhere(_.name == "carrier_key_hash"), binaryCell(0x02.toByte, 32))
    val rows = Map(obligation.table -> Vector(first, second))
    val snapshot = snapshotWith(extraRows = rows)

    assertEquals(21L, snapshot.rowCount)
    assertEquals(2, snapshot.relation("physical_source_obligation").rows.size)
    assertEquals(1, snapshot.relation("physical_recovery_binding").rows.size)
    assertFalse(snapshot.exactlyEquals(snapshotWith(extraRows = Map(obligation.table -> Vector(first)))))
    assertTrue(snapshot.exactlyEquals(snapshotWith(extraRows = rows)))

  @Test def canonicalSnapshotDistinguishesNullEmptyTextEmptyBinaryNumbersRawDoubleAndDatetime6(): Unit =
    val base = snapshotWith()
    val nullableText = MySqlFormat3RelationShapes.canonicalOrder.iterator.flatMap { shape =>
      shape.columns.zipWithIndex.collectFirst {
        case (column, index) if column.kind == MySqlFormat3SqlKind.Text && column.nullable => (shape, index)
      }
    }.next()
    val nullableBinary = MySqlFormat3RelationShapes.canonicalOrder.iterator.flatMap { shape =>
      shape.columns.zipWithIndex.collectFirst {
        case (column, index) if column.kind == MySqlFormat3SqlKind.Binary && column.nullable => (shape, index)
      }
    }.next()
    val floating = MySqlFormat3RelationShapes.canonicalOrder.iterator.flatMap { shape =>
      shape.columns.zipWithIndex.collectFirst {
        case (column, index) if column.kind == MySqlFormat3SqlKind.Floating => (shape, index)
      }
    }.next()
    val integer = MySqlFormat3RelationShapes.canonicalOrder.iterator.flatMap { shape =>
      shape.columns.zipWithIndex.collectFirst {
        case (column, index) if column.kind == MySqlFormat3SqlKind.Integer && !column.nullable => (shape, index)
      }
    }.next()
    val dateTime = MySqlFormat3RelationShapes.canonicalOrder.iterator.flatMap { shape =>
      shape.columns.zipWithIndex.collectFirst {
        case (column, index) if column.kind == MySqlFormat3SqlKind.DateTime => (shape, index)
      }
    }.next()

    val emptyText = replaceCell(base, nullableText._1, nullableText._2, MySqlFormat3Cell.Text(""))
    val emptyBinary = replaceCell(base, nullableBinary._1, nullableBinary._2, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.emptyByteArray)))
    val changedInteger = replaceCell(base, integer._1, integer._2, MySqlFormat3Cell.SignedInteger(2L))
    val negativeZero = replaceCell(base, floating._1, floating._2,
      MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(-0.0d)))
    val laterMicros = replaceCell(base, dateTime._1, dateTime._2,
      MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 7, 1, 2, 3, 123457000)))

    assertNotEquals(base.canonicalSha256Hex, emptyText.canonicalSha256Hex)
    assertNotEquals(base.canonicalSha256Hex, emptyBinary.canonicalSha256Hex)
    assertNotEquals(base.canonicalSha256Hex, changedInteger.canonicalSha256Hex)
    assertNotEquals(base.canonicalSha256Hex, negativeZero.canonicalSha256Hex)
    assertNotEquals(base.canonicalSha256Hex, laterMicros.canonicalSha256Hex)

  @Test def binaryValuesAndDigestViewsDoNotExposeMutableSnapshotStorage(): Unit =
    val input = Array[Byte](0x01, 0x02, 0x7f)
    val binaryBudget = new MySqlFormat3ReadBudget(makeProfile(), packetLimit = 4_000_000L)
    binaryBudget.initialize(testSchema, observation(), scope(), maxAllowedPacket = 16_777_216L)
    val binary = MySqlFormat3Binary.copyOf(input, binaryBudget, "test binary copy")
    input(0) = 0x55
    val output = binary.copyBytes(binaryBudget, "test binary readback")
    assertArrayEquals(Array[Byte](0x01, 0x02, 0x7f), output)
    output(1) = 0x44
    assertTrue(binary.equalsBytes(Array[Byte](0x01, 0x02, 0x7f)))

    val snapshot = snapshotWith()
    val digest = snapshot.canonicalSha256.toArray
    digest(0) = (digest(0) ^ 0x7f).toByte
    assertNotEquals(digest.toVector, snapshot.canonicalSha256)
    assertEquals(64, snapshot.canonicalSha256Hex.length)
    assertEquals(32, snapshot.canonicalSha256.size)
    assertTrue(snapshot.canonicalByteLength > 0L)

  @Test def malformedRowsAndPrimaryKeyOrderFailInsteadOfProducingACompleteSnapshot(): Unit =
    val valid = snapshotWith()
    val pod = MySqlFormat3RelationShapes.forTable("floating_pod_state")
    val shortPod = valid.relations.map { relation =>
      if relation.table == pod.table then relation.copy(rows = Vector(Vector.empty)) else relation
    }
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(relations = shortPod))

    val nonNullableColumn = pod.columns.indexWhere(column => !column.nullable)
    val nullPod = valid.relations.map { relation =>
      if relation.table == pod.table then relation.copy(rows = Vector(relation.rows.head.updated(nonNullableColumn, MySqlFormat3Cell.SqlNull))) else relation
    }
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(relations = nullPod))

    val floatingColumn = pod.columns.indexWhere(_.kind == MySqlFormat3SqlKind.Floating)
    val wrongTypePod = valid.relations.map { relation =>
      if relation.table == pod.table then relation.copy(rows = Vector(relation.rows.head.updated(floatingColumn, MySqlFormat3Cell.Text("not a number")))) else relation
    }
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(relations = wrongTypePod))

    val textShape = MySqlFormat3RelationShapes.canonicalOrder.find(_.columns.exists(_.kind == MySqlFormat3SqlKind.Text)).get
    val textIndex = textShape.columns.indexWhere(_.kind == MySqlFormat3SqlKind.Text)
    val malformedTextRows = valid.relations.map { relation =>
      if relation.table == textShape.table then relation.copy(rows = Vector(relation.rows.head.updated(textIndex, MySqlFormat3Cell.Text("\uD800")))) else relation
    }
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(relations = malformedTextRows))

    val dateTimeShape = MySqlFormat3RelationShapes.canonicalOrder.find(_.columns.exists(_.kind == MySqlFormat3SqlKind.DateTime)).get
    val dateTimeIndex = dateTimeShape.columns.indexWhere(_.kind == MySqlFormat3SqlKind.DateTime)
    val invalidPrecisionRows = valid.relations.map { relation =>
      if relation.table == dateTimeShape.table then relation.copy(rows = Vector(relation.rows.head.updated(dateTimeIndex,
        MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 7, 1, 2, 3, 123456789))))) else relation
    }
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(relations = invalidPrecisionRows))

    val obligation = MySqlFormat3RelationShapes.forTable("physical_source_obligation")
    val first = defaultRow(obligation).updated(obligation.columns.indexWhere(_.name == "carrier_key_hash"), binaryCell(0x01.toByte, 32))
    val second = defaultRow(obligation).updated(obligation.columns.indexWhere(_.name == "carrier_key_hash"), binaryCell(0x02.toByte, 32))
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(extraRows = Map(obligation.table -> Vector(second, first))))
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(extraRows = Map(obligation.table -> Vector(first, first))))

  @Test def explicitRetainedAndPacketBudgetsRejectOversizeBeforeReturningSnapshot(): Unit =
    assertThrows(classOf[java.sql.SQLDataException], () => snapshotWith(limitProfile = makeProfile(retained = 128L)))

    val budget = new MySqlFormat3ReadBudget(makeProfile(retained = 1_000_000L), packetLimit = 48L)
    budget.initialize(testSchema, observation(), scope(), maxAllowedPacket = 48L)
    budget.beginRow(1, "test_table", 0L)
    budget.beginVariableCell("test_table row 0 column bytes")
    assertThrows(classOf[java.sql.SQLDataException], () => budget.addVariablePayload(40L, "test_table row 0 column bytes"))

    val tooSmallPacket = new MySqlFormat3ReadBudget(makeProfile(retained = 1_000_000L), packetLimit = 4L)
    tooSmallPacket.initialize(testSchema, observation(), scope(), maxAllowedPacket = 4L)
    assertThrows(classOf[java.sql.SQLDataException], () => tooSmallPacket.beginRow(1, "test_table", 0L))

  @Test def jdbcCellStreamsAreDecodedWithSimultaneousBufferBudgetsAndExactCleanup(): Unit =
    val payload = Array.tabulate[Byte](20_000)(index => (index * 17).toByte)
    val profile = makeProfile(retained = 200_000L)
    val binaryBudget = initializedBudget(profile)
    binaryBudget.beginRow(1, "test_table", 0L)
    val decoded = MySqlFormat3CellStreamDecoder.readBinary(
      new ByteArrayInputStream(payload), None, binaryBudget, "test_table row 0 binary")
    assertArrayEquals(payload, decoded)
    assertEquals(8_192L + 320L + payload.length.toLong, binaryBudget.accountedMemoryBytes)

    val textBudget = initializedBudget(profile)
    textBudget.beginRow(1, "test_table", 0L)
    val text = MySqlFormat3CellStreamDecoder.readText(new StringReader("x😀"), Some(2), textBudget, "test_table row 0 text")
    assertEquals("x😀", text)
    assertEquals(8_192L + 320L + 70L, textBudget.accountedMemoryBytes)

    val tooSmall = initializedBudget(makeProfile(retained = 50_000L))
    tooSmall.beginRow(1, "test_table", 0L)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFormat3CellStreamDecoder.readBinary(
      new ByteArrayInputStream(payload), None, tooSmall, "test_table row 0 limited binary"))
    assertEquals(8_192L + 320L, tooSmall.accountedMemoryBytes)

    val overlongBinaryBudget = initializedBudget(profile)
    overlongBinaryBudget.beginRow(1, "test_table", 0L)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFormat3CellStreamDecoder.readBinary(
      new ByteArrayInputStream(payload), Some(10_000), overlongBinaryBudget, "test_table row 0 oversized binary"))
    assertEquals(8_192L + 320L, overlongBinaryBudget.accountedMemoryBytes)

    val sourceFailure = new IOException("stream read failed")
    val closeFailure = new IOException("stream close failed")
    val failedInput = new InputStream:
      override def read(): Int = throw sourceFailure
      override def read(bytes: Array[Byte], offset: Int, length: Int): Int = throw sourceFailure
      override def close(): Unit = throw closeFailure
    val failedBudget = initializedBudget(profile)
    failedBudget.beginRow(1, "test_table", 0L)
    val readFailure = assertThrows(classOf[IOException], () => MySqlFormat3CellStreamDecoder.readBinary(
      failedInput, None, failedBudget, "test_table row 0 failing binary"))
    assertSame(sourceFailure, readFailure)
    val suppressed = readFailure.getSuppressed
    assertEquals(1, suppressed.length)
    assertSame(closeFailure, suppressed(0))
    assertEquals(8_192L + 320L, failedBudget.accountedMemoryBytes)

  @Test def textLengthAndSurrogateFailuresStopAtTheDecoderBoundary(): Unit =
    val profile = makeProfile(retained = 100_000L)
    val overlongBudget = initializedBudget(profile)
    overlongBudget.beginRow(1, "test_table", 0L)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFormat3CellStreamDecoder.readText(
      new StringReader("abc"), Some(2), overlongBudget, "test_table row 0 limited text"))
    assertEquals(8_192L + 320L, overlongBudget.accountedMemoryBytes)

    val surrogateBudget = initializedBudget(profile)
    surrogateBudget.beginRow(1, "test_table", 0L)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFormat3CellStreamDecoder.readText(
      new StringReader("\uD800"), None, surrogateBudget, "test_table row 0 invalid text"))
    assertEquals(8_192L + 320L, surrogateBudget.accountedMemoryBytes)

  @Test def profileIdentityIsIncludedInCompleteSnapshotComparison(): Unit =
    val first = snapshotWith(limitProfile = makeProfile(retained = 4_000_000L))
    val second = snapshotWith(limitProfile = makeProfile(retained = 4_000_001L))
    assertFalse(first.exactlyEquals(second))
    assertNotEquals(first.canonicalSha256Hex, second.canonicalSha256Hex)

  private def snapshotWith(
      limitProfile: SourceLimitProfile = makeProfile(),
      relations: Vector[MySqlFormat3RelationSnapshot] = null,
      extraRows: Map[String, Vector[Vector[MySqlFormat3Cell]]] = Map.empty
  ): MySqlFormat3FullSnapshot =
    val selected = if relations != null then relations else
      MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
        MySqlFormat3RelationSnapshot(shape.table, extraRows.getOrElse(shape.table, Vector(defaultRow(shape))))
      }
    val connectionObservation = observation()
    val observationScope = scope()
    val budget = new MySqlFormat3ReadBudget(limitProfile, limitProfile.maxSqlPacketBytes())
    budget.initialize(testSchema, connectionObservation, observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(selected)
    new MySqlFormat3FullSnapshot(testSchema, connectionObservation, observationScope, 16_777_216L, limitProfile, selected, budget)

  private def initializedBudget(profile: SourceLimitProfile): MySqlFormat3ReadBudget =
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    budget.initialize(testSchema, observation(), scope(), 16_777_216L)
    budget

  private def replaceCell(
      snapshot: MySqlFormat3FullSnapshot,
      shape: MySqlFormat3RelationShape,
      columnIndex: Int,
      cell: MySqlFormat3Cell
  ): MySqlFormat3FullSnapshot =
    val changed = snapshot.relations.map { relation =>
      if relation.table != shape.table then relation
      else relation.copy(rows = Vector(relation.rows.head.updated(columnIndex, cell)))
    }
    snapshotWith(limitProfile = snapshot.limitProfile, relations = changed)

  private def defaultRow(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    shape.columns.map { column =>
      if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
        case MySqlFormat3SqlKind.Binary => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](column.maximumLength.getOrElse(1))(0x01)))
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
        case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 7, 1, 2, 3, 123456000))
    }

  private def binaryCell(fill: Byte, size: Int): MySqlFormat3Cell =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](size)(fill)))

  private def observation(): MySqlFormat3ConnectionObservation = MySqlFormat3ConnectionObservation(
    UUID.fromString("00000000-0000-0000-0000-000000000001"), "9.7.2", testSchema, "test@127.0.0.1",
    "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")

  private def scope(): MySqlFormat3ObservationScope = MySqlFormat3ObservationScope(
    UUID.fromString("00000000-0000-0000-0000-000000000002"), 1L, 1L)

  private def makeProfile(retained: Long = 8_000_000L): SourceLimitProfile =
    SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
      retained, 4_000_000L, 2_000_000L)

  private def resource(path: String): Array[Byte] =
    val stream = getClass.getResourceAsStream(path)
    if stream == null then throw new AssertionError(s"required pinned format3 resource is missing: $path")
    try stream.readAllBytes()
    finally stream.close()

  private def sha256(bytes: Array[Byte]): String =
    java.util.HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))


  @Test def explicitUtf8ByteLimitChecksCompleteCodepointsBeforeBuilderGrowthAcrossChunks(): Unit =
    def run(text: String, limit: Long, accepted: Boolean, readsBeforeFailure: Int): Unit =
      val budget = initializedBudget(makeProfile(retained = 100_000L))
      budget.beginRow(1, "test_table", 0L)
      val originalDebt = budget.accountedMemoryBytes
      var reads = 0
      var closed = false
      val closeFailure = new IOException("UTF-8 limited reader close failed")
      val input = new java.io.Reader:
        private var position = 0
        override def read(chars: Array[Char], offset: Int, length: Int): Int =
          if position >= text.length then -1
          else
            chars(offset) = text.charAt(position)
            position += 1
            reads += 1
            1 // surrogate pair intentionally split across stream chunks
        override def close(): Unit =
          closed = true
          if !accepted then throw closeFailure
      if accepted then
        assertEquals(text, MySqlFormat3CellStreamDecoder.readText(input, None, budget, "UTF-8 exact", Some(limit)))
        assertEquals(originalDebt + 64L + text.length.toLong * 2L, budget.accountedMemoryBytes)
      else
        val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
          MySqlFormat3CellStreamDecoder.readText(input, None, budget, "UTF-8 rejected", Some(limit)))
        assertTrue(failure.getMessage.contains("explicit UTF-8 byte length"))
        assertEquals(readsBeforeFailure, reads)
        assertEquals(originalDebt, budget.accountedMemoryBytes, "failed decoder must release its temporary/retained debt")
        assertEquals(1, failure.getSuppressed.length)
        assertSame(closeFailure, failure.getSuppressed.head)
        if text == "😀" then
          assertEquals(originalDebt + 16_448L + 64L, budget.successfulReserveHighWaterBytes,
            "an unaffordable surrogate pair must fail before appending/growing for its high surrogate")
      assertTrue(closed)
    run("汉", 3L, true, 0)
    run("汉汉", 3L, false, 2)
    run("😀", 4L, true, 0)
    run("😀", 3L, false, 2)
    run("😀😀", 4L, false, 4)

  @Test def utf8LimitPreservesFrozenCharacterLimitAndOriginalReadCauseWithSuppressedClose(): Unit =
    val profile = makeProfile(retained = 100_000L)
    val lengthBudget = initializedBudget(profile)
    lengthBudget.beginRow(1, "test_table", 0L)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFormat3CellStreamDecoder.readText(
      new StringReader("汉汉"), Some(1), lengthBudget, "character boundary", Some(6L)))
    assertEquals(8_512L, lengthBudget.accountedMemoryBytes)
    val budget = initializedBudget(profile)
    budget.beginRow(1, "test_table", 0L)
    val source = new IOException("original text read cause")
    val cleanup = new IOException("text cleanup cause")
    val reader = new java.io.Reader:
      override def read(chars: Array[Char], offset: Int, length: Int): Int = throw source
      override def close(): Unit = throw cleanup
    val failure = assertThrows(classOf[IOException], () => MySqlFormat3CellStreamDecoder.readText(
      reader, None, budget, "text source failure", Some(4L)))
    assertSame(source, failure)
    assertEquals(1, failure.getSuppressed.length)
    assertSame(cleanup, failure.getSuppressed.head)
    assertEquals(8_512L, budget.accountedMemoryBytes)


  @Test def explicitUtf8CapDoesNotMaskMalformedSurrogatesOrSourceFailures(): Unit =
    Vector("\uD800", "\uD800x", "\uDC00").foreach { text =>
      val budget = initializedBudget(makeProfile(retained = 100_000L))
      budget.beginRow(1, "test_table", 0L)
      val closeFailure = new IOException("malformed surrogate close")
      val reader = new StringReader(text):
        override def close(): Unit = throw closeFailure
      val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlFormat3CellStreamDecoder.readText(reader, None, budget, "malformed UTF-16", Some(0L)))
      assertTrue(failure.getMessage.contains("unpaired UTF-16"), failure.getMessage)
      assertSame(closeFailure, failure.getSuppressed.head)
      assertEquals(8_512L, budget.accountedMemoryBytes)
    }
    val budget = initializedBudget(makeProfile(retained = 100_000L))
    budget.beginRow(1, "test_table", 0L)
    val sourceFailure = new IOException("read failed after high surrogate")
    val reader = new java.io.Reader:
      private var first = true
      override def read(chars: Array[Char], offset: Int, length: Int): Int =
        if first then
          first = false
          chars(offset) = '\uD800'
          1
        else throw sourceFailure
      override def close(): Unit = ()
    assertSame(sourceFailure, assertThrows(classOf[IOException], () =>
      MySqlFormat3CellStreamDecoder.readText(reader, None, budget, "source after surrogate", Some(0L))))
    assertEquals(8_512L, budget.accountedMemoryBytes)
