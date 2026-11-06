package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.io.{ByteArrayOutputStream, DataOutputStream, EOFException}
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** Fixture 只构造真实 typed DATA，没有 SQL/Boot/Native/admission issuer。 */
private[mysql] object MySqlFull20CodecFixture:
  val schema = "fp_codec_test"
  def profile(retained: Long = 8000000L): SourceLimitProfile =
    SourceLimitProfile.of(1000000L, 1000000L, 2000000L, 64L, 2000000L, retained, 4000000L, 2000000L)
  def observation: MySqlFormat3ConnectionObservation = MySqlFormat3ConnectionObservation(
    new UUID(1L, 2L), "9.7.2", schema, "data@test", "InnoDB", 1, 1L, "READ-COMMITTED",
    "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
  def scope: MySqlFormat3ObservationScope = MySqlFormat3ObservationScope(new UUID(3L, 4L), 1L, 1L)
  def row(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] = shape.columns.map { column =>
    if column.nullable then MySqlFormat3Cell.SqlNull
    else column.kind match
      case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
      case MySqlFormat3SqlKind.Binary => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](column.maximumLength.getOrElse(1))(1)))
      case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
      case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
      case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 10, 1, 2, 3, 123456000))
  }
  def snapshot(p: SourceLimitProfile = profile(), history: Int = 0): MySqlFormat3FullSnapshot =
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      var base = row(shape)
      if shape.table == "physical_source_forest" && history > 0 then
        base = base.updated(shape.columns.indexWhere(_.name == "forest_bytes"),
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.tabulate[Byte](history)(index => index.toByte))))
      val rows = if shape.table == "physical_source_obligation" && history > 0 then
        val key = shape.columns.indexWhere(_.name == "carrier_key_hash")
        val state = shape.columns.indexWhere(_.name == "obligation_state")
        Vector(base.updated(key, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(1))))
          .updated(state, MySqlFormat3Cell.Text("ACTIVE")),
          base.updated(key, MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(2))))
          .updated(state, MySqlFormat3Cell.Text("DISCHARGED")))
      else Vector(base)
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    build(p, relations)
  def build(p: SourceLimitProfile, relations: Vector[MySqlFormat3RelationSnapshot]): MySqlFormat3FullSnapshot =
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    root.initialize(schema, observation, scope, 16777216L)
    root.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, observation, scope, 16777216L, p, relations, root)
  def bytes(payload: MySqlFormat3CanonicalPayload): Array[Byte] =
    // Test-owned inspection copy, not production accounting oracle.
    Array.tabulate[Byte](payload.byteLength)(payload.bytes.byteAt)
  def capture(content: Array[Byte], source: MySqlFormat3FullSnapshot): MySqlFormat3CanonicalPayload =
    MySqlFormat3SnapshotCodec.capture(content, source.limitProfile, source.allocationBudget)
  def bodyOffset(source: MySqlFormat3FullSnapshot): Int =
    // Actual writer's count seam; no mirrored canonical parser/calculation.
    (MySqlFormat3SnapshotCanonical.fixedHeaderBytes(source.schema, source.connection, source.observationScope,
      source.maxAllowedPacketBytes, source.limitProfile, source.allocationBudget) - 80L).toInt
  def header(source: MySqlFormat3FullSnapshot, body: Array[Byte]): Array[Byte] =
    val output = MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget)
    bytes(output).take(bodyOffset(source)) ++ body
  def putInt(bytes: Array[Byte], offset: Int, value: Int): Unit = ByteBuffer.wrap(bytes, offset, 4).putInt(value)

final class MySqlFormat3SnapshotCodecTest:
  import MySqlFull20CodecFixture.*

  @Test def wholeTwentyHistoryAbove64KiBRoundtripsUsingTheSameWriterAndSharedRoot(): Unit =
    val source = snapshot(history = 70000)
    val root = source.allocationBudget
    val canonical = MySqlFormat3SnapshotCodec.encode(source, root)
    assertTrue(canonical.byteLength > 65536)
    val charge = root.accountedMemoryBytes
    val decoded = MySqlFormat3SnapshotCodec.decode(canonical, source.limitProfile, root)
    assertTrue(decoded.allocationBudget.sharesAllocationLedger(root))
    assertTrue(root.accountedMemoryBytes > charge)
    assertEquals(20, decoded.relations.size)
    assertEquals(2, decoded.relation("physical_source_obligation").rows.size)
    assertEquals(21L, decoded.rowCount)
    assertEquals(source.relations, decoded.relations)
    assertTrue(source.exactlyEquals(decoded))
    val again = MySqlFormat3SnapshotCodec.encode(decoded, root)
    assertArrayEquals(bytes(canonical), bytes(again))
    assertEquals(source.canonicalByteLength, canonical.byteLength.toLong)
    assertEquals(source.canonicalSha256, java.security.MessageDigest.getInstance("SHA-256").digest(bytes(canonical)).toVector)

  @Test def nullEmptyUnicodeRawFloatingBitsAndMicrosecondDatetimeRemainDistinct(): Unit =
    val base = snapshot()
    val shapes = MySqlFormat3RelationShapes.canonicalOrder
    val relations = base.relations.zip(shapes).map { (relation, shape) =>
      val changed = relation.rows.head.zip(shape.columns).map { (cell, column) =>
        if column.nullable then MySqlFormat3Cell.SqlNull
        else column.kind match
          case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text(if column.maximumLength.exists(_ < 3) then "" else "𐀀é")
          case MySqlFormat3SqlKind.Binary => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.emptyByteArray))
          case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(0x7ff8000000000042L)
          case _ => cell
      }
      relation.copy(rows = Vector(changed))
    }
    val source = build(base.limitProfile, relations)
    val encoded = MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget)
    val decoded = MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, source.allocationBudget)
    assertEquals(relations, decoded.relations)
    assertTrue(decoded.relations.flatMap(_.rows).flatten.exists(_ == MySqlFormat3Cell.SqlNull))
    assertTrue(decoded.relations.flatMap(_.rows).flatten.exists(_ == MySqlFormat3Cell.Floating(0x7ff8000000000042L)))

  @Test def frozenDomainSourceShapeProfileAndTrailingBytesAreStrict(): Unit =
    val source = snapshot()
    val raw = bytes(MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget))
    val domain = raw.clone(); domain(4) = (domain(4) ^ 1).toByte
    val hash = raw.clone(); hash(4 + MySqlFormat3SnapshotCanonical.Domain.length) = 'Z'.toByte
    val shape = raw.clone()
    val literal = "fp_schema_manifest".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val index = raw.indices.find(i => i + literal.length <= raw.length && raw.slice(i, i + literal.length).sameElements(literal)).get
    shape(index) = 'X'.toByte
    for malformed <- Vector(domain, hash, shape, raw ++ Array[Byte](0)) do
      val payload = capture(malformed, source)
      assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(payload, source.limitProfile, source.allocationBudget))
    val otherProfile = profile(8000001L)
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SnapshotCodec.decode(capture(raw, source), otherProfile, source.allocationBudget))
    val profileBytes = source.limitProfile.canonicalBytes()
    val profileIndex = raw.indices.find(i => i + profileBytes.length <= raw.length && raw.slice(i, i + profileBytes.length).sameElements(profileBytes)).get
    val changedProfile = raw.clone(); changedProfile(profileIndex + profileBytes.length - 1) = (changedProfile(profileIndex + profileBytes.length - 1) ^ 1).toByte
    val changedSha = raw.clone(); changedSha(profileIndex + profileBytes.length) = (changedSha(profileIndex + profileBytes.length) ^ 1).toByte
    for malformed <- Vector(changedProfile, changedSha) do
      assertThrows(classOf[SQLDataException], () =>
        MySqlFormat3SnapshotCodec.decode(capture(malformed, source), source.limitProfile, source.allocationBudget))

  @Test def prefixTruncationNegativeCountAndWrongRowWidthFailBeforeAllocation(): Unit =
    val source = snapshot()
    val raw = bytes(MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget))
    for size <- Vector(1, 3, 4, 100, bodyOffset(source) - 1, raw.length - 1) do
      assertThrows(classOf[EOFException], () =>
        MySqlFormat3SnapshotCodec.decode(capture(raw.take(size), source), source.limitProfile, source.allocationBudget))
    val negative = raw.clone(); putInt(negative, bodyOffset(source), -1)
    val width = raw.clone(); putInt(width, bodyOffset(source) + 4, 0)
    val huge = raw.clone(); putInt(huge, bodyOffset(source), Int.MaxValue)
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(negative, source), source.limitProfile, source.allocationBudget))
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(width, source), source.limitProfile, source.allocationBudget))
    assertThrows(classOf[EOFException], () => MySqlFormat3SnapshotCodec.decode(capture(huge, source), source.limitProfile, source.allocationBudget))

  @Test def malformedUtf8UnknownTagNullAndIntegerShapeRejectThroughActualDecoder(): Unit =
    val source = snapshot()
    val raw = bytes(MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget))
    // First manifest column is integer singleton; actual writer body framing places first tag after count+width.
    val tagOffset = bodyOffset(source) + 8
    val unknown = raw.clone(); unknown(tagOffset) = 99.toByte
    val nullTag = raw.clone(); nullTag(tagOffset) = 0.toByte
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(unknown, source), source.limitProfile, source.allocationBudget))
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(nullTag, source), source.limitProfile, source.allocationBudget))
    // Header schema is actual length-prefixed text, following fixed domain and three exact hash fields.
    val schemaOffset = 4 + MySqlFormat3SnapshotCanonical.Domain.length + 192 + 4
    val utf = raw.clone(); utf(schemaOffset) = 0xc0.toByte
    assertThrows(classOf[CharacterCodingException], () => MySqlFormat3SnapshotCodec.decode(capture(utf, source), source.limitProfile, source.allocationBudget))
    val integerRange = raw.clone(); ByteBuffer.wrap(integerRange, tagOffset + 1, 8).putLong(Long.MaxValue)
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(integerRange, source), source.limitProfile, source.allocationBudget))

  @Test def duplicatePrimaryKeyRowsCannotBeNormalizedIntoAValidSnapshot(): Unit =
    val source = snapshot()
    val raw = bytes(MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget))
    // Use the real writer on a DATA snapshot whose only relation rows are empty to locate full header.
    val row = source.relations.head.rows.head
    val emptyRelations = source.relations.map(_.copy(rows = Vector.empty))
    val empty = build(source.limitProfile, emptyRelations)
    val headerSize = bodyOffset(empty)
    // Produce a valid one-row first-table-only stream then duplicate its actual encoded row bytes.
    val oneRelations = emptyRelations.updated(0, emptyRelations.head.copy(rows = Vector(row)))
    val one = build(source.limitProfile, oneRelations)
    val oneBytes = bytes(MySqlFormat3SnapshotCodec.encode(one, one.allocationBudget))
    val rowBytes = oneBytes.slice(headerSize + 4, oneBytes.length - 19 * 4)
    val duplicated = oneBytes.take(headerSize) ++ ByteBuffer.allocate(4).putInt(2).array() ++ rowBytes ++ rowBytes ++ Array.fill[Byte](19 * 4)(0)
    assertThrows(classOf[SQLDataException], () =>
      MySqlFormat3SnapshotCodec.decode(capture(duplicated, source), source.limitProfile, source.allocationBudget))

  @Test def binaryMaximumBooleanFramingAndDatetimePrecisionStayStrict(): Unit =
    val source = snapshot()
    val raw = bytes(MySqlFormat3SnapshotCodec.encode(source, source.allocationBudget))
    def locate(pattern: Array[Byte]): Int =
      raw.indices.find(i => i + pattern.length <= raw.length && raw.slice(i, i + pattern.length).sameElements(pattern)).get
    val binaryPattern = Array[Byte](2, 0, 0, 0, 32) ++ Array.fill[Byte](32)(1)
    val binaryIndex = locate(binaryPattern)
    val tooWide = raw.clone(); putInt(tooWide, binaryIndex + 1, 33)
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(tooWide, source), source.limitProfile, source.allocationBudget))
    val huge = raw.clone(); putInt(huge, binaryIndex + 1, Int.MaxValue)
    assertThrows(classOf[EOFException], () => MySqlFormat3SnapshotCodec.decode(capture(huge, source), source.limitProfile, source.allocationBudget))
    val declaration = MySqlFormat3RelationShapes.canonicalOrder.head.columns.head.sqlDeclaration.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val nullableIndex = locate(declaration) + declaration.length + 1
    val nonBoolean = raw.clone(); nonBoolean(nullableIndex) = 2.toByte
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(nonBoolean, source), source.limitProfile, source.allocationBudget))
    val datetime = ByteBuffer.allocate(13).putInt(2026).put(10.toByte).put(10.toByte)
      .put(1.toByte).put(2.toByte).put(3.toByte).putInt(123456000).array()
    val dateIndex = locate(datetime)
    val nanosecond = raw.clone(); putInt(nanosecond, dateIndex + 9, 123456001)
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(capture(nanosecond, source), source.limitProfile, source.allocationBudget))
