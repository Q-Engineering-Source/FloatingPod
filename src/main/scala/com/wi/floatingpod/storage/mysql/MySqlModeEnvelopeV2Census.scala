package com.wi.floatingpod.storage.mysql

import java.io.{DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 仅 census DATA：保原 producer，记录同 writer 的 literals/borrowed spans，不生成 V2 payload。 */
private[mysql] object MySqlModeEnvelopeV2Census:
  val Domain = "FP_B1_MODE_FULL20_ORIGINAL_WITNESS_BE_V2"
  val ChunkBytes = 4096
  val BinaryEncoding = 11
  val Utf8Encoding = 12
  private[mysql] enum Method:
    case FlatV2, RecipeV3
  val RecipeDomain = "FP_B1_MODE_FULL20_ORIGINAL_WITNESS_BE_V3"
  final case class RecipeFacts(nodes: Int, segments: Long, leafBytes: Long, bodyReferences: Long,
      checkedExpandedBytes: Long, capturedMetadata: Long, nodeAndLinkMetadata: Long,
      leafSpanMetadata: Long, indexAndOtherMetadata: Long, wire: Facts)

  final case class Facts(b: Int, a: Int, r: Int, references: Long, wireBytes: Long,
      metadataBytes: Long, holderBytes: Long, scratchPeakBytes: Long,
      baselineAccounted: Long, baselineRemaining: Long, currentAccounted: Long,
      currentRemaining: Long, highWater: Long, nativeFits: Boolean, retainedFits: Boolean,
      headerWire: Long, bWire: Long, aWire: Long, rWire: Long, sourceWire: Long, planWire: Long,
      contextWire: Long, imagesWire: Long, phasesWire: Long, descriptorsWire: Long, footerWire: Long)

  private[mysql] trait Bytes:
    def length: Long
    def at(index: Long): Int

  private type RawBytes = Bytes | MySqlFormat3Binary | String
  private def rawLength(value: RawBytes, owner: Partial): Long = value match
    case null => throw new SQLDataException("MySQL format3 text value is null")
    case bytes: Bytes => bytes.length
    case binary: MySqlFormat3Binary => binary.length.toLong
    case text: String =>
      // 与原strict UTF8长度合同一致；raw引用没有缓存长度，新增扫描也必须逐charAt先计量。
      require(owner != null)
      var index = 0
      var length = 0L
      while index < text.length do
        owner.recordUtf8(1L)
        val current = text.charAt(index)
        if current <= 0x7f then length = Math.addExact(length, 1L)
        else if current <= 0x7ff then length = Math.addExact(length, 2L)
        else if Character.isHighSurrogate(current) then
          if index + 1 >= text.length then
            throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
          owner.recordUtf8(1L)
          if !Character.isLowSurrogate(text.charAt(index + 1)) then
            throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
          length = Math.addExact(length, 4L); index += 1
        else if Character.isLowSurrogate(current) then
          throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
        else length = Math.addExact(length, 3L)
        index += 1
      length
  private def rawByte(value: RawBytes, index: Long, owner: Partial): Int = value match
    case bytes: Bytes => bytes.at(index)
    case binary: MySqlFormat3Binary =>
      require(index >= 0L && index < binary.length)
      binary.byteAt(index.toInt).toInt & 255
    case text: String => utf8Byte(text, index, owner)

  private final class BinaryBytes(val value: MySqlFormat3Binary) extends Bytes:
    val length = value.length.toLong
    def at(index: Long): Int =
      require(index >= 0L && index < length)
      value.byteAt(index.toInt).toInt & 255

  private final class Literal(val kind: Int, val width: Int, val bits: Long) extends Bytes:
    val length = width.toLong
    def at(index: Long): Int =
      require(index >= 0L && index < length)
      ((bits >>> ((width - 1 - index.toInt) * 8)) & 255L).toInt

  /** 不分配 UTF8 array；严格 byteLength 先拒 malformed UTF16。 */
  private final class Utf8Bytes(val original: String, owner: Partial = null) extends Bytes:
    val length = MySqlFormat3Utf8.byteLength(original)
    def at(wanted: Long): Int =
      require(wanted >= 0L && wanted < length)
      utf8Byte(original, wanted, owner)
  private def utf8Byte(original: String, wanted: Long, owner: Partial): Int =
    require(wanted >= 0L)
    var character = 0
    var offset = 0L
    while character < original.length do
      if owner != null then owner.recordUtf8(1L)
      val first = original.charAt(character)
      val point = if Character.isHighSurrogate(first) then
        character += 1
        if owner != null then owner.recordUtf8(1L)
        Character.toCodePoint(first, original.charAt(character))
      else first.toInt
      val width = if point <= 127 then 1 else if point <= 2047 then 2 else if point <= 65535 then 3 else 4
      if wanted < offset + width then
        val byte = (wanted - offset).toInt
        if width == 1 then return point
        if byte == 0 then
          return (if width == 2 then 192 else if width == 3 then 224 else 240) | (point >> (6 * (width - 1)))
        return 128 | ((point >> (6 * (width - 1 - byte))) & 63)
      offset += width
      character += 1
    throw new IllegalStateException("strict UTF8 cursor underrun")

  private final class Slice(val bytes: RawBytes, val offset: Long, val size: Int, val encoding: Int, initialOwner: Partial):
    require(offset >= 0L && size > 0 && size <= ChunkBytes && offset <= rawLength(bytes, initialOwner) - size)
    def at(index: Int, owner: Partial): Int = rawByte(bytes, offset + index, owner)

  /** 每页先计费；没有 collection 自动扩容或未计费的 permutation。 */
  private final class Page:
    val values = new Array[AnyRef](64)
    var next: Page = null
  private class Store[T <: AnyRef](owner: Partial):
    private var first: Page = null
    private var last: Page = null
    private var pageIndex: Array[Page] = null
    private var pageCount = 0
    var size = 0
    private var prepared = false
    private var nextSize = 0
    private var pendingPage: Page = null
    private var pendingLookup: Array[Page] = null
    def allocatedPages: Int = pageCount + (if pendingPage == null then 0 else 1)
    /** 所有secondary reserve先于page/lookup/value allocation；prepare失败没有新local custody。 */
    def prepare(): Unit =
      owner.check(); require(!prepared)
      nextSize = Math.addExact(size, 1)
      if size % 64 == 0 then
        val grow = pageIndex == null || pageCount == pageIndex.length
        val capacity = if !grow then 0 else if pageIndex == null then 4 else Math.multiplyExact(pageIndex.length, 2)
        owner.charge(640L, "census page/array BEFORE allocation")
        if grow then owner.charge(Math.addExact(64L, Math.multiplyExact(capacity.toLong, 8L)),
          "census page lookup array BEFORE allocation")
        // 两项charge均成功后才配置；立即发布attempt字段，后续意外failure仍strongheld。
        pendingPage = new Page
        if grow then
          pendingLookup = new Array[Page](capacity)
          if pageIndex != null then System.arraycopy(pageIndex, 0, pendingLookup, 0, pageCount)
          pageIndex = pendingLookup
        pageIndex(pageCount) = pendingPage; pageCount += 1
        if first == null then first = pendingPage else last.next = pendingPage
        last = pendingPage; pendingPage = null
      prepared = true
    /** 仅prepare后的不可失败发布；不再reserve/check/算术或分配。 */
    def publish(value: T): Int =
      val index = size
      last.values(size % 64) = value
      size = nextSize
      prepared = false
      index
    def add(value: T): Int = { prepare(); publish(value) }
    def apply(index: Int): T =
      require(index >= 0 && index < size)
      pageIndex(index / 64).values(index % 64).asInstanceOf[T]

  private final class Sequence(val bytes: Bytes, val ids: Array[Int], val encoding: Int):
    var recipe = -1
  private final class Atom(val cell: MySqlFormat3Cell, val sequence: Sequence)
  private final class Row(val relation: Int, val original: Vector[MySqlFormat3Cell], val atoms: Array[Int])
  private final class RawReference(val association: Int, val list: Int, val ordinal: Int,
      val uuid: UUID, val bodyId: Int, val sha: Vector[Byte], val canonical: MySqlFormat3Binary)
  private final class Segment(val bytes: Bytes, val offset: Long, val length: Long, val start: Long)
  private trait Events:
    def size: Int
    def spanCount: Int
    def spanOrdinal(index: Int): Int
    def start(index: Int): Long
    def length(index: Int): Long
    def offset(index: Int): Long
    def isLiteral(index: Int): Boolean
    def kind(index: Int): Int
    def width(index: Int): Int
    def bits(index: Int): Long
    def bytes(index: Int): RawBytes
    def locate(byte: Long): Int
    def at(index: Int, local: Long): Int

  /** Same Store/Segment allocation as the frozen FlatV2 path; no additional adapter object. */
  private final class FlatEvents(owner: Partial) extends Store[Segment](owner) with Events:
    def spanCount: Int = throw new IllegalStateException("FlatV2 does not construct recipes")
    def spanOrdinal(index: Int): Int = throw new IllegalStateException("FlatV2 does not construct recipes")
    def start(index: Int): Long = apply(index).start
    def length(index: Int): Long = apply(index).length
    def offset(index: Int): Long = apply(index).offset
    def isLiteral(index: Int): Boolean = apply(index).bytes.isInstanceOf[Literal]
    def kind(index: Int): Int = apply(index).bytes.asInstanceOf[Literal].kind
    def width(index: Int): Int = apply(index).bytes.asInstanceOf[Literal].width
    def bits(index: Int): Long = apply(index).bytes.asInstanceOf[Literal].bits
    def bytes(index: Int): Bytes = apply(index).bytes
    def at(index: Int, local: Long): Int =
      require(local >= 0L && local < length(index))
      apply(index).bytes.at(Math.addExact(offset(index), local))
    def locate(byte: Long): Int =
      var low = 0; var high = size - 1
      while low <= high do
        val middle = low + (high - low) / 2
        val value = apply(middle)
        if byte < value.start then high = middle - 1
        else if byte >= value.start + value.length then low = middle + 1
        else return middle
      throw new IllegalStateException("census composite cursor underrun")

  private final class PackedPage:
    var metadata: Array[Long] = null
    var data: Array[Long] = null
    var spanBase = 0
    var spans = 0
    var first = 0L
    var length = 0L
    var used = 0

  private final class PayloadPage:
    var values: Array[RawBytes] = null

  /** Recipe events retain primitive fields directly; partially allocated pages remain owned here. */
  private final class PackedEvents(owner: Partial) extends Events:
    private var directory: Array[PackedPage] = null
    private var pendingDirectory: Array[PackedPage] = null
    private var pendingPage: PackedPage = null
    private var pages = 0
    private var spanDirectory: Array[PayloadPage] = null
    private var pendingSpanDirectory: Array[PayloadPage] = null
    private var pendingSpanPage: PayloadPage = null
    private var spanPages = 0
    private var spans = 0
    private var count = 0
    private var nextEvent = 0
    private var selected = -1
    private var selectedStart = 0L
    private var selectedEnd = 0L
    private var nextByte = 0L
    def size: Int = count
    def spanCount: Int = spans
    def retainedEventPages: Int = pages + (if pendingPage == null then 0 else 1)
    def retainedPayloadPages: Int = spanPages + (if pendingSpanPage == null then 0 else 1)
    def spanOrdinal(index: Int): Int =
      val current = page(index)
      require(!isLiteral(index))
      val local = ((current.metadata(index % 64) >>> 32) & 63L).toInt
      require(local < current.spans)
      val ordinal = Math.addExact(current.spanBase, local)
      require(ordinal >= 0 && ordinal < spans)
      ordinal
    def consumedEvents: Int = nextEvent
    def consumedBytes: Long = nextByte
    /** 原writer单调消费；随机读取不改变这些状态，每个record只在前进时检查一次。 */
    def sequentialAt(position: Long, total: Long): Int =
      require(position == nextByte && position >= 0L && position < total)
      while position >= selectedEnd && nextEvent < count do
        owner.recordSequentialProbe()
        selected = nextEvent
        nextEvent = Math.addExact(nextEvent, 1)
        selectedStart = selectedEnd
        selectedEnd = Math.addExact(selectedStart, length(selected))
      require(selected >= 0 && position >= selectedStart && position < selectedEnd,
        "capture cursor must cover the next original writer byte")
      val value = at(selected, position - selectedStart)
      nextByte = Math.addExact(nextByte, 1L)
      value
    def finishSequential(total: Long): Unit =
      require(nextByte == total && selectedEnd == total, "capture cursor exact byte settlement")
      while nextEvent < count do
        owner.recordSequentialProbe()
        val trailing = nextEvent
        nextEvent = Math.addExact(nextEvent, 1)
        require(length(trailing) == 0L, "capture cursor has an unconsumed nonempty record")
    private def page(index: Int): PackedPage =
      require(index >= 0 && index < count)
      directory(index / 64)
    def append(kind: Int, width: Int, bits: Long, bytes: RawBytes, offset: Long, length: Long, start: Long): Unit =
      require(start >= 0L && length >= 0L && offset >= 0L)
      val checkedLength = MySqlFormat3JvmArrayLimit.requireLength(length, "packed original event length")
      require((kind == 0 && width == 0 && bytes != null && offset <= rawLength(bytes, owner) - length) ||
        (kind >= 1 && kind <= 5 && width >= 1 && width <= 8 && length == width && bytes == null && offset == 0L))
      val nextCount = Math.addExact(count, 1)
      val nextSpans = if kind == 0 then Math.addExact(spans, 1) else spans
      val end = Math.addExact(start, length)
      if count % 64 != 0 then
        val previous = directory(pages - 1)
        require(Math.addExact(previous.first, previous.length) == start,
          "packed events preserve contiguous original positions")
      if count % 64 == 0 then
        val grow = directory == null || pages == directory.length
        val capacity = if !grow then 0 else if directory == null then 4 else Math.multiplyExact(directory.length, 2)
        owner.charge(1280L, "packed event page and two arrays BEFORE allocation")
        if grow then owner.charge(Math.addExact(64L, Math.multiplyExact(capacity.toLong, 8L)),
          "packed event directory BEFORE allocation")
        pendingPage = new PackedPage
        pendingPage.first = start
        pendingPage.spanBase = spans
        pendingPage.metadata = new Array[Long](64)
        pendingPage.data = new Array[Long](64)
        if grow then
          pendingDirectory = new Array[PackedPage](capacity)
          if directory != null then System.arraycopy(directory, 0, pendingDirectory, 0, pages)
          directory = pendingDirectory
          pendingDirectory = null
        directory(pages) = pendingPage
        pages += 1
        pendingPage = null
      // 页可以先进入目录，但所有record/ordinal计数必须在两种页均准备完成后才提交。
      if kind == 0 && spans % 64 == 0 then
        val grow = spanDirectory == null || spanPages == spanDirectory.length
        val capacity = if !grow then 0 else if spanDirectory == null then 4 else Math.multiplyExact(spanDirectory.length, 2)
        owner.charge(704L, "packed event page payload and array BEFORE allocation")
        if grow then owner.charge(Math.addExact(64L, Math.multiplyExact(capacity.toLong, 8L)),
          "packed event directory payload BEFORE allocation")
        pendingSpanPage = new PayloadPage
        pendingSpanPage.values = new Array[RawBytes](64)
        if grow then
          pendingSpanDirectory = new Array[PayloadPage](capacity)
          if spanDirectory != null then System.arraycopy(spanDirectory, 0, pendingSpanDirectory, 0, spanPages)
          spanDirectory = pendingSpanDirectory
          pendingSpanDirectory = null
        spanDirectory(spanPages) = pendingSpanPage
        spanPages += 1
        pendingSpanPage = null
      val current = directory(pages - 1)
      val slot = count % 64
      val ordinalBits = if kind == 0 then current.spans.toLong << 32 else 0L
      current.metadata(slot) = (kind.toLong << 56) | (width.toLong << 48) | ordinalBits | checkedLength.toLong
      current.data(slot) = if kind == 0 then offset else bits
      if kind == 0 then
        spanDirectory(spans / 64).values(spans % 64) = bytes
        current.spans += 1
      spans = nextSpans
      current.length = end - current.first
      current.used += 1
      count = nextCount
    def length(index: Int): Long = page(index).metadata(index % 64) & 0xffffffffL
    def isLiteral(index: Int): Boolean = kind(index) != 0
    def kind(index: Int): Int = ((page(index).metadata(index % 64) >>> 56) & 255L).toInt
    def width(index: Int): Int = ((page(index).metadata(index % 64) >>> 48) & 255L).toInt
    def bits(index: Int): Long = page(index).data(index % 64)
    def offset(index: Int): Long = if isLiteral(index) then 0L else bits(index)
    def bytes(index: Int): RawBytes =
      val ordinal = spanOrdinal(index)
      spanDirectory(ordinal / 64).values(ordinal % 64)
    def at(index: Int, local: Long): Int =
      require(local >= 0L && local < length(index))
      if isLiteral(index) then ((bits(index) >>> ((width(index) - 1 - local.toInt) * 8)) & 255L).toInt
      else rawByte(bytes(index), Math.addExact(offset(index), local), owner)
    def start(index: Int): Long =
      val value = page(index)
      var start = value.first; var slot = 0
      while slot < index % 64 do
        owner.recordProbe()
        start = Math.addExact(start, value.metadata(slot) & 0xffffffffL); slot += 1
      start
    def locate(byte: Long): Int =
      var low = 0; var high = pages - 1
      while low <= high do
        owner.recordProbe()
        val middle = low + (high - low) / 2
        val value = directory(middle)
        if byte < value.first then high = middle - 1
        else if byte >= value.first + value.length then low = middle + 1
        else
          var start = value.first; var slot = 0
          while slot < value.used do
            owner.recordProbe()
            val next = Math.addExact(start, value.metadata(slot) & 0xffffffffL)
            if byte >= start && byte < next then return middle * 64 + slot
            start = next; slot += 1
          throw new IllegalStateException("packed event page cursor underrun")
      throw new IllegalStateException("packed event cursor underrun")

  /** A pre-existing anonymous Sequence needs one borrowed span, not a 64-entry page. */
  private final class SingleSpanEvents(value: Bytes) extends Events:
    def size: Int = 1
    def spanCount: Int = 1
    def spanOrdinal(index: Int): Int = { check(index); 0 }
    private def check(index: Int): Unit = require(index == 0)
    def start(index: Int): Long = { check(index); 0L }
    def length(index: Int): Long = { check(index); value.length }
    def offset(index: Int): Long = { check(index); 0L }
    def isLiteral(index: Int): Boolean = { check(index); false }
    def kind(index: Int): Int = { check(index); 0 }
    def width(index: Int): Int = { check(index); 0 }
    def bits(index: Int): Long = { check(index); 0L }
    def bytes(index: Int): Bytes = { check(index); value }
    def at(index: Int, local: Long): Int = { check(index); value.at(local) }
    def locate(byte: Long): Int = { require(byte >= 0L && byte < value.length); 0 }
  private[mysql] final class Body private[MySqlModeEnvelopeV2Census] (
      val kind: Int, val encoding: Int, val association: Int, owner: Partial,
      expected: MySqlRecoveryCanonicalBytes) extends Bytes:
    private[MySqlModeEnvelopeV2Census] val segments: Events =
      if owner.method == Method.FlatV2 then new FlatEvents(owner)
      else new PackedEvents(owner)
    private[MySqlModeEnvelopeV2Census] var recipe = -1
    private var pendingBytes: RawBytes = null
    private var total = 0L
    var emitted = 0L
    var digest: Array[Byte] = null
    private[MySqlModeEnvelopeV2Census] var sequence: Sequence = null
    val expectedBinary = if expected == null then null else expected.borrowedBinary
    def length: Long = total
    def retainedEventCount: Int = segments.size
    def retainedSpanCount: Int = segments.spanCount
    def eventSpanOrdinal(index: Int): Int = segments.spanOrdinal(index)
    def borrowedEventIs(index: Int, value: MySqlFormat3Binary): Boolean = segments.bytes(index).asInstanceOf[AnyRef] eq value
    def retainedEventPages: Int = segments.asInstanceOf[PackedEvents].retainedEventPages
    def retainedPayloadPages: Int = segments.asInstanceOf[PackedEvents].retainedPayloadPages
    def pendingRawIs(value: MySqlFormat3Binary): Boolean = pendingBytes.asInstanceOf[AnyRef] eq value
    def sequentialEventCount: Int = segments.asInstanceOf[PackedEvents].consumedEvents
    def sequentialByteCount: Long = segments.asInstanceOf[PackedEvents].consumedBytes
    private[MySqlModeEnvelopeV2Census] def retainRaw(value: RawBytes): Unit = pendingBytes = value
    private[MySqlModeEnvelopeV2Census] def appendRaw(value: RawBytes, offset: Long = 0L, count: Long = -1L): Unit =
      pendingBytes = value
      require(owner.method == Method.RecipeV3)
      val length = rawLength(value, owner)
      val size = if count < 0L then length else count
      require(offset >= 0L && size >= 0L && offset <= length - size)
      val next = Math.addExact(total, size)
      segments.asInstanceOf[PackedEvents].append(0, 0, 0L, value, offset, size, total)
      total = next; pendingBytes = null
    def append(value: Bytes, offset: Long = 0L, count: Long = -1L): Unit =
      pendingBytes = value // caller刚配置的span/literal先强持，再进行任何fallible检查/charge。
      val size = if count < 0L then value.length else count
      require(offset >= 0L && size >= 0L && offset <= value.length - size)
      val next = Math.addExact(total, size)
      segments match
        case flat: FlatEvents =>
          flat.prepare()
          owner.charge(48L, "census composite segment BEFORE allocation")
          flat.publish(new Segment(value, offset, size, total))
        case packed: PackedEvents => value match
          case literal: Literal if offset == 0L && size == literal.width =>
            packed.append(literal.kind, literal.width, literal.bits, null, 0L, size, total)
          case _ => packed.append(0, 0, 0L, value, offset, size, total)
        case _ => throw new IllegalStateException("unexpected capture event store")
      total = next; pendingBytes = null
    def appendLiteral(kind: Int, width: Int, bits: Long): Unit =
      require(owner.method == Method.RecipeV3)
      val next = Math.addExact(total, width.toLong)
      segments.asInstanceOf[PackedEvents].append(kind, width, bits, null, 0L, width.toLong, total)
      total = next
    def at(index: Long): Int =
      require(index >= 0L && index < total)
      val selected = segments.locate(index)
      segments.at(selected, index - segments.start(selected))
    def observe(byte: Int): Unit =
      if owner.method == Method.RecipeV3 && owner.failure != null then throw owner.failure
      owner.recordComparison()
      if emitted >= total then
        throw new SQLDataException("census event/composite differs from unique writer")
      val actual = segments match
        case packed: PackedEvents => packed.sequentialAt(emitted, total)
        case _ => at(emitted)
      if actual != (byte & 255) then
        throw new SQLDataException("census event/composite differs from unique writer")
      if expectedBinary != null then owner.recordComparison()
      if expectedBinary != null &&
          (emitted >= expectedBinary.length || expectedBinary.byteAt(emitted.toInt) != byte.toByte) then
        throw new SQLDataException("census unique writer differs from retained original canonical")
      emitted = Math.addExact(emitted, 1L)
    def finish(hash: Array[Byte]): Unit =
      if emitted != total || expected != null &&
          (expected.length.toLong != total || !expected.sha256.indices.forall(i => expected.sha256(i) == hash(i))) then
        throw new SQLDataException("census canonical length/digest/original body mismatch")
      segments match
        case packed: PackedEvents => packed.finishSequential(total)
        case _ => ()
      owner.charge(96L, "census body digest custody BEFORE allocation")
      digest = hash.clone()

  private final class Stream(owner: Partial, body: Body, digest: MessageDigest) extends OutputStream:
    var advertised = 0L
    def advertisedSpan(size: Long): Unit = advertised = Math.addExact(advertised, size)
    def write(value: Int): Unit =
      if advertised == 0L then
        if owner.method == Method.RecipeV3 then body.appendLiteral(5, 1, value.toLong & 255L)
        else
          owner.charge(32L, "census fallback inline literal BEFORE allocation")
          body.append(new Literal(5, 1, value.toLong & 255L))
      else advertised -= 1L
      body.observe(value)
      digest.update(value.toByte)
    override def write(values: Array[Byte], start: Int, length: Int): Unit =
      require(values != null && start >= 0 && length >= 0 && start <= values.length - length)
      var index = 0
      while index < length do
        write(values(start + index).toInt)
        index += 1

  private final class EventOutput(val owner: Partial, val body: Body, val stream: Stream)
      extends DataOutputStream(stream)

  private def event(output: DataOutputStream): EventOutput = output match
    case value: EventOutput => value
    case _ => null
  private def advertise(output: DataOutputStream, value: Bytes): Unit =
    val e = event(output)
    if e != null then
      e.body.append(value)
      e.stream.advertisedSpan(value.length)

  def literal(output: DataOutputStream, kind: Int, width: Int, bits: Long): Unit =
    val e = event(output)
    if e != null then
      require(width > 0 && width <= 8)
      if e.owner.method == Method.RecipeV3 then
        e.body.appendLiteral(kind, width, bits)
        e.stream.advertisedSpan(width.toLong)
      else
        e.owner.charge(32L, "census inlineLiteral kind/width/rawBits BEFORE allocation")
        advertise(output, new Literal(kind, width, bits))
  def int(output: DataOutputStream, value: Int): Unit = { literal(output, 1, 4, value.toLong); output.writeInt(value) }
  def long(output: DataOutputStream, value: Long): Unit = { literal(output, 2, 8, value); output.writeLong(value) }
  def byte(output: DataOutputStream, value: Int): Unit = { literal(output, 3, 1, value.toLong); output.writeByte(value) }
  def bool(output: DataOutputStream, value: Boolean): Unit = { literal(output, 4, 1, if value then 1L else 0L); output.writeBoolean(value) }
  def binary(output: DataOutputStream, value: MySqlFormat3Binary): Unit =
    val e = event(output)
    if e != null then
      if e.owner.method == Method.RecipeV3 then
        e.body.appendRaw(value)
        e.stream.advertisedSpan(value.length.toLong)
      else
        e.owner.charge(48L, "census immutable BinarySpan BEFORE allocation")
        advertise(output, new BinaryBytes(value))
    value.writeTo(output)
  def canonical(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes): Unit =
    if event(output) == null then value.writeTo(output) else binary(output, value.borrowedBinary)
  def text(output: DataOutputStream, value: String): Unit =
    val e = event(output)
    if e != null then
      if e.owner.method == Method.RecipeV3 then e.body.retainRaw(value)
      val length = if e.owner.method == Method.RecipeV3 then rawLength(value, e.owner) else MySqlFormat3Utf8.byteLength(value)
      val size = MySqlFormat3JvmArrayLimit.requireLength(length, "census strict text length")
      literal(output, 1, 4, size.toLong)
      if e.owner.method == Method.RecipeV3 then
        e.body.appendRaw(value, 0L, size.toLong)
        e.stream.advertisedSpan(size.toLong)
      else
        e.owner.charge(48L, "census Utf8Span original String BEFORE allocation")
        advertise(output, new Utf8Bytes(value, e.owner))
    MySqlRecoveryCanonicalBytes.writeText(output, value)
  def beginTyped(output: DataOutputStream, cell: MySqlFormat3Cell): Boolean =
    val e = event(output)
    if e == null then false else
      e.owner.beginTypedField(cell)
      true
  def endTyped(output: DataOutputStream, active: Boolean): Unit =
    if active then event(output).owner.endTypedField()
  def typedRow(output: DataOutputStream, table: String, row: Vector[MySqlFormat3Cell]): Unit =
    val e = event(output)
    if e != null then e.owner.row(table, row)

  /** 固定 original producer dispatcher；不给 caller 任意writer回调或第二次materialization。 */
  private[mysql] def captureAll(source: MySqlModeWitnessCodec.SourceDescriptor, partial: Partial): Unit =
    partial.check()
    val draft = partial.draft
    val root = partial.root
    require((draft.retainedCensus eq partial) && partial.businessCount != null, "actual original count responsibility")
    val m = draft.value
    val context = draft.originalContext
    partial.capture(30, 8, 0, draft.retainedContextCanonical, output => context.writeIdentityFields(output))
    partial.capture(31, 7, 0, m.exactPlan.requestIdentity.canonical,
      output => MySqlRepositoryRequestIdentity.writeSingle(output, m.exactOperation))
    var image = 0
    while image < 3 do
      val actual = if image == 0 then m.preimage else if image == 1 then m.reserveTarget else m.executeTarget
      val body = partial.capture(image + 1, 1, image, null,
        output => MySqlFormat3SnapshotCanonical.writeSnapshot(actual, output, root))
      val expected = if image == 0 then partial.businessCount.image0End - partial.businessCount.contextEnd - 12L
        else if image == 1 then partial.businessCount.image1End - partial.businessCount.image0End - 8L
        else partial.businessCount.image2End - partial.businessCount.image1End - 8L
      require(body.emitted == expected, "complete full20 same-writer count/emitted body differs")
      val hash = actual.canonicalSha256
      require(hash.size == 32 && hash.indices.forall(i => hash(i) == body.digest(i)), "complete full20 original canonical digest")
      image += 1
    var phaseIndex = 0
    while phaseIndex < 2 do
      val index = phaseIndex
      val phase = draft.retainedPhaseAt(index)
      val effect = m.effects(index)
      val request = MySqlModeOperationEffectCodec.modeRequest(m.exactOperation)
      val key = MySqlRecoveryResourceKey.pod(request.podUuid, root.profile)
      root.reserveRetainedMemory(256L, "census scope resource/vector BEFORE allocation")
      val resources = Vector(MySqlRecoveryResourceRevision(key, m.originalLocatorRevision, Some(request.attemptUuid)))
      partial.capture(10, 9, index, phase.scope, output => MySqlRecoveryOperationScope.writeOrdered(output, context, resources))
      partial.capture(11, 10, index, phase.operationContextBytes,
        output => MySqlModeOperationEffectCodec.writeOperationContext(output, phase.operationContext))
      partial.capture(12, MySqlModeOperationEffectCodec.projectionVersion(phase.preimage).censusEncoding, index, phase.preimage,
        output => MySqlModeOperationEffectCodec.writeProjectedImage(output, (if index == 0 then m.preimage else m.reserveTarget), m.exactPlan,
          m.exactOperation, context, m.originalLocatorRevision, effect.phase, MySqlOperationEffectImage.Before, MySqlModeOperationEffectCodec.projectionVersion(phase.preimage)))
      partial.capture(13, MySqlModeOperationEffectCodec.projectionVersion(phase.target).censusEncoding, index, phase.target,
        output => MySqlModeOperationEffectCodec.writeProjectedImage(output, (if index == 0 then m.reserveTarget else m.executeTarget), m.exactPlan,
          m.exactOperation, context, m.originalLocatorRevision, effect.phase, MySqlOperationEffectImage.Target, MySqlModeOperationEffectCodec.projectionVersion(phase.target)))
      partial.capture(14, 4, index, phase.fingerprint,
        output => MySqlModeOperationEffectCodec.writePhaseFingerprint(output, m.exactPlan, request, effect.phase,
          phase.scope, phase.preimage, phase.target))
      root.reserveTemporaryMemory(161L, "census semantic profile97 copy BEFORE allocation")
      var primary: Throwable = null
      try
        val profile = root.profile.canonicalBytes()
        val historical = if index == 0 then None else
          m.effects(0).rawOperationEnvelopes.find(_.operationUuid == m.exactPlan.reserveOperationUuid)
        val witness = if index == 0 then m.originalReserveWitness else m.originalExecuteWitness
        partial.capture(15, 5, index, witness,
          output => MySqlModeWorkflowMaterializer.writeSemantic(output, m.exactPlan, m.exactOperation, context,
            m.plannedAt, effect.phase, phase, historical, profile, draft.retainedContextCanonical)(using root, Some(draft)))
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try root.releaseTemporaryMemory(161L, "census profile temporary references dropped")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      partial.capture(16, 14, index, null, output => MySqlModeWitnessCodec.writeCensusEffect(output, effect, phase, m.exactPlan))
      def raw(values: Vector[MySqlRecoveryOperationRawEnvelope]): Unit =
        var rowIndex = 0
        while rowIndex < values.size do
          val raw = values(rowIndex)
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw.row, root.profile, root)
          partial.capture(20, 2, index, raw.canonical,
            output => MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, raw.row,
              MySqlFormat3RelationShapes.forTable("fp_recovery_operation")))
          rowIndex += 1
      raw(effect.preimageRawOperationEnvelopes); raw(effect.rawOperationEnvelopes)
      phaseIndex += 1
    partial.finish(source, m, context)

  private[mysql] final class Partial private[mysql] (
      val root: MySqlFormat3ReadBudget, val draft: MySqlModeOriginalWitnessDraft,
      val method: Method = Method.FlatV2):
    private val thread = Thread.currentThread()
    /** Fixed counters only. No collection or second event owner is introduced. */
    final class Diagnostics:
      var literalCharge = 0L
      var spanCharge = 0L
      var segmentCharge = 0L
      var pageCharge = 0L
      var lookupCharge = 0L
      var bodyCharge = 0L
      var digestCharge = 0L
      var sequenceCharge = 0L
      var packedPageCharge = 0L
      var packedDirectoryCharge = 0L
      var otherCharge = 0L
      var successfulUnique = 0L
      var successfulDuplicate = 0L
      var uniqueLiterals = 0L
      var uniqueSpans = 0L
      var duplicateLiterals = 0L
      var duplicateSpans = 0L
      var comparisonBytes = 0L
      var utf8Characters = 0L
      var dagDescents = 0L
      var expandedBytes = 0L
      var eventProbes = 0L
      var sequentialEventProbes = 0L // 固定1024B诊断holder内的一个额外Long，无额外对象。
      var rangeSeeks = 0L
      var rangeBytes = 0L
      var rangeAdvances = 0L
      var dagPushes = 0L
      var dagPops = 0L
      var captureWork = 0L
      var recipeWork = 0L
      var otherWork = 0L
      private[MySqlModeEnvelopeV2Census] var current: Body = null
      def classifiedCharge: Long = literalCharge + spanCharge + segmentCharge + pageCharge + lookupCharge +
        bodyCharge + digestCharge + sequenceCharge + packedPageCharge + packedDirectoryCharge + otherCharge
      def currentKind: Int = if current == null then -1 else current.kind
      def currentEncoding: Int = if current == null then -1 else current.encoding
      def currentAssociation: Int = if current == null then -1 else current.association
      def currentSegments: Int = if current == null then 0 else current.segments.size
      def currentEmitted: Long = if current == null then 0L else current.emitted
      def currentLiterals: Int =
        if current == null then 0 else
          var count = 0; var index = 0
          while index < current.segments.size do
            if current.segments.isLiteral(index) then count += 1
            index += 1
          count
      private[MySqlModeEnvelopeV2Census] def charged(bytes: Long, label: String): Unit =
        if label.contains("packed event page") then packedPageCharge = Math.addExact(packedPageCharge, bytes)
        else if label.contains("packed event directory") then packedDirectoryCharge = Math.addExact(packedDirectoryCharge, bytes)
        else if label.contains("inlineLiteral") || label.contains("inline literal") then literalCharge = Math.addExact(literalCharge, bytes)
        else if label.contains("page lookup") then lookupCharge = Math.addExact(lookupCharge, bytes)
        else if label.contains("page/array") then pageCharge = Math.addExact(pageCharge, bytes)
        else if label.contains("composite segment") then segmentCharge = Math.addExact(segmentCharge, bytes)
        else if label.contains("BinarySpan") || label.contains("Utf8Span") || label.contains("borrowed binary sequence") || label.contains("borrowed UTF8 sequence") then spanCharge = Math.addExact(spanCharge, bytes)
        else if label.contains("BeginBody") then bodyCharge = Math.addExact(bodyCharge, bytes)
        else if label.contains("digest custody") then digestCharge = Math.addExact(digestCharge, bytes)
        else if label.contains("Bsequence") then sequenceCharge = Math.addExact(sequenceCharge, bytes)
        else otherCharge = Math.addExact(otherCharge, bytes)
      private[MySqlModeEnvelopeV2Census] def completed(body: Body, duplicate: Boolean): Unit =
        var literals = 0L; var spans = 0L; var index = 0
        while index < body.segments.size do
          if body.segments.isLiteral(index) then literals += 1L else spans += 1L
          index += 1
        if duplicate then
          successfulDuplicate = Math.addExact(successfulDuplicate, 1L)
          duplicateLiterals = Math.addExact(duplicateLiterals, literals)
          duplicateSpans = Math.addExact(duplicateSpans, spans)
        else
          successfulUnique = Math.addExact(successfulUnique, 1L)
          uniqueLiterals = Math.addExact(uniqueLiterals, literals)
          uniqueSpans = Math.addExact(uniqueSpans, spans)
        current = null
    var diagnostics: Diagnostics = null
    private def recordWork(size: Long): Unit =
      if diagnostics != null then
        if diagnostics.current != null then diagnostics.captureWork = Math.addExact(diagnostics.captureWork, size)
        else if stage == "RECIPE_DISCOVERY" then diagnostics.recipeWork = Math.addExact(diagnostics.recipeWork, size)
        else diagnostics.otherWork = Math.addExact(diagnostics.otherWork, size)
    private[MySqlModeEnvelopeV2Census] def recordComparison(): Unit =
      if diagnostics != null then
        diagnostics.comparisonBytes = Math.addExact(diagnostics.comparisonBytes, 1L); recordWork(1L)
    private[MySqlModeEnvelopeV2Census] def recordUtf8(size: Long): Unit =
      if diagnostics != null then
        diagnostics.utf8Characters = Math.addExact(diagnostics.utf8Characters, size); recordWork(size)
    private[MySqlModeEnvelopeV2Census] def recordProbe(): Unit =
      if diagnostics != null then
        diagnostics.eventProbes = Math.addExact(diagnostics.eventProbes, 1L); recordWork(1L)
    private[MySqlModeEnvelopeV2Census] def recordSequentialProbe(): Unit =
      if diagnostics != null then
        diagnostics.sequentialEventProbes = Math.addExact(diagnostics.sequentialEventProbes, 1L)
      recordProbe()
    var baselineAccounted = root.accountedMemoryBytes
    var baselineRemaining = root.availableMemoryBytes
    var businessCount: MySqlModeWitnessCodec.EnvelopeSizeFacts = null
    def establishBaseline(value: MySqlModeWitnessCodec.EnvelopeSizeFacts): Unit =
      check(); require(chunks == null && value != null)
      businessCount = value; baselineAccounted = value.accounted; baselineRemaining = value.remaining
    private var metadata = 0L
    private var scratchPeak = 0L
    private var initialTemporary = 0L
    private[mysql] def beginTemporaryObservation(value: Long): Unit = initialTemporary = value
    // Ledger already checked nonnegative/addition; scalar observation cannot fail after successful reserve.
    private[mysql] def observeTemporary(value: Long): Unit =
      scratchPeak = Math.max(scratchPeak, Math.max(0L, value - initialTemporary))
    private var references = 0L
    private var fieldOpen = false
    private var chunks: Store[Slice] = null
    private var atoms: Store[Atom] = null
    private var rows: Store[Row] = null
    private var bodies: Store[Body] = null
    private var attemptedBodies: Store[Body] = null
    private var sequences: Store[Sequence] = null
    private var rawReferences: Store[RawReference] = null
    private var pendingSequenceBytes: Bytes = null
    private var pendingChunk: Slice = null
    private var permutation: Array[Int] = null
    private var inverse: Array[Int] = null
    private var atomPermutation: Array[Int] = null
    private var atomInverse: Array[Int] = null
    private var rowPermutation: Array[Int] = null
    private var rowInverse: Array[Int] = null
    var failure: Throwable = null
    var businessFailure: MySqlModeWitnessCodec.EnvelopeCapacityFailure = null
    var facts: Facts = null
    var recipeFacts: RecipeFacts = null
    var stage = "REGISTERED_BEFORE_INDEX_ALLOCATION"
    var attempted = false
    private final class RecipeNode(val bytes: Bytes, val kind: Int, val encoding: Int,
        val association: Int, val digest: Array[Byte], val segments: Events):
      var targets: Array[Int] = null
      var starts: Array[Int] = null
      var leafIds: Array[Int] = null
      var resolvedSlots = 0
      var ready = false
      val length = bytes.length
      var wireSegments = 0
      def requireReady(): Unit = require(ready && resolvedSlots == segments.spanCount, "recipe node is not ready")
      def leafCount(event: Int): Int =
        MySqlFormat3JvmArrayLimit.requireLength(Math.addExact(segments.length(event), ChunkBytes - 1L) / ChunkBytes,
          "recipe leaf reference count")
      def leafPosition(event: Int, leaf: Int): Int =
        requireReady()
        val ordinal = segments.spanOrdinal(event)
        require(targets(ordinal) == -1 && leaf >= 0 && leaf < leafCount(event), "exact recipe span leaf bounds")
        val index = Math.addExact(starts(ordinal), leaf)
        require(leafIds != null && index < leafIds.length)
        index
    private var recipes: Store[RecipeNode] = null
    private var recipeBodyOrder: Array[Int] = null
    private var recipeBodyInverse: Array[Int] = null
    private var recipeSegments = 0L
    private var recipeBodyReferences = 0L
    private var recipeExpandedBytes = 0L
    private var recipeLeafBytes = 0L
    private var capturedMetadata = 0L
    private var recipeNodeAndLinkMetadata = 0L
    private var recipeLeafSpanMetadata = 0L
    private def recipeCharge(bytes: Long, leaf: Boolean, label: String): Unit =
      charge(bytes, label)
      if leaf then recipeLeafSpanMetadata = Math.addExact(recipeLeafSpanMetadata, bytes)
      else recipeNodeAndLinkMetadata = Math.addExact(recipeNodeAndLinkMetadata, bytes)
    def check(): Unit =
      require(Thread.currentThread() eq thread, "exact original census thread")
      draft.requireOwner()
      require((draft.originalRoot eq root) && !root.isClosed, "exact original census root")
    def charge(bytes: Long, label: String): Unit =
      check()
      root.reserveRetainedMemory(bytes, label)
      metadata = Math.addExact(metadata, bytes)
      if diagnostics != null then diagnostics.charged(bytes, label)
    def initialize(): Unit =
      try
        check(); require(chunks == null && !attempted)
        attempted = true
        if method == Method.RecipeV3 then
          charge(1024L, "fixed scalar diagnostics BEFORE allocation")
          diagnostics = new Diagnostics
          diagnostics.otherCharge = metadata
        charge(8192L, "census fixed stores/dispatch closures BEFORE allocation")
        chunks = new Store[Slice](this); atoms = new Store[Atom](this)
        rows = new Store[Row](this); bodies = new Store[Body](this); attemptedBodies = new Store[Body](this); sequences = new Store[Sequence](this); rawReferences = new Store[RawReference](this)
        stage = "INDEX_DISCOVERY"
      catch
        case cause: Throwable =>
          recordFailure(cause)
          throw cause
    def chunkCount: Int = if chunks == null then 0 else chunks.size
    def bodyCount: Int = if bodies == null then 0 else bodies.size
    def attemptedBodyCount: Int = if attemptedBodies == null then 0 else attemptedBodies.size
    def attemptedBody(index: Int): Body = attemptedBodies(index)
    def heldRecipeCount: Int = if recipes == null then 0 else recipes.size
    def recipeArrayMask(index: Int): Int =
      val node = recipes(index)
      (if node.targets == null then 0 else 1) | (if node.starts == null then 0 else 2) |
        (if node.leafIds == null then 0 else 4)
    def retainedMetadataBytes: Long = metadata
    /** 完成后的只读形状观察；不展开payload、不移动cursor、不改变诊断或预算。 */
    private def requireGuardShape(): Unit =
      check()
      require(method == Method.RecipeV3 && failure == null && facts != null && recipes != null &&
        stage == "COMPLETE_CENSUS_ONLY_NOT_BUSINESS_QUALIFICATION", "completed original recipe census required")
    private def guardNode(node: Int): RecipeNode =
      requireGuardShape()
      val value = recipes(node)
      value.requireReady()
      value
    private def guardUtf16Length(value: RawBytes): Int = value match
      case text: String => text.length
      case text: Utf8Bytes => text.original.length
      case _ => 0
    private[mysql] def guardNodeLength(node: Int): Long = guardNode(node).length
    private[mysql] def guardNodeEventCount(node: Int): Int = guardNode(node).segments.size
    private[mysql] def guardNodeWireSegmentCount(node: Int): Int = guardNode(node).wireSegments
    private[mysql] def guardEventLength(node: Int, event: Int): Long = guardNode(node).segments.length(event)
    private[mysql] def guardEventOffset(node: Int, event: Int): Long = guardNode(node).segments.offset(event)
    private[mysql] def guardEventTarget(node: Int, event: Int): Int =
      val value = guardNode(node)
      if value.segments.isLiteral(event) then -2 else value.targets(value.segments.spanOrdinal(event))
    private[mysql] def guardEventUtf16Length(node: Int, event: Int): Int =
      val value = guardNode(node)
      if value.segments.isLiteral(event) then 0 else guardUtf16Length(value.segments.bytes(event))
    private[mysql] def guardSequenceLength(sequence: Int): Long =
      requireGuardShape()
      sequences(sequence).bytes.length
    private[mysql] def guardSequenceUtf16Length(sequence: Int): Int =
      requireGuardShape()
      guardUtf16Length(sequences(sequence).bytes)
    private[mysql] def guardAttemptedBodyZeroEvents(body: Int): Int =
      requireGuardShape()
      val events = attemptedBodies(body).segments
      var index = 0
      var count = 0
      while index < events.size do
        if events.length(index) == 0L then count = Math.addExact(count, 1)
        index += 1
      count

    def allocatedPageCount: Int =
      if chunks == null then 0 else chunks.allocatedPages + atoms.allocatedPages + rows.allocatedPages +
        bodies.allocatedPages + attemptedBodies.allocatedPages + sequences.allocatedPages + rawReferences.allocatedPages
    def retainedSequenceCount: Int = if sequences == null then 0 else sequences.size
    def retainedSequenceIdCount: Int =
      if sequences == null then 0 else
        var result = 0; var index = 0
        while index < sequences.size do
          result = Math.addExact(result, sequences(index).ids.length)
          index += 1
        result
    def pendingChunkCount: Int = if pendingChunk == null then 0 else 1
    def rawReferenceCount: Int = if rawReferences == null then 0 else rawReferences.size
    def rawReferenceMatches(index: Int, association: Int, list: Int, ordinal: Int,
        expected: MySqlRecoveryOperationRawEnvelope): Boolean =
      check()
      val ref = rawReferences(index); val body = bodies(ref.bodyId)
      ref.association == association && ref.list == list && ref.ordinal == ordinal &&
        ref.uuid == expected.operationUuid && ref.sha == expected.sha256 &&
        ref.canonical == expected.canonical.borrowedBinary && body.kind == 20 && body.encoding == 2 &&
        body.association == association && body.expectedBinary == expected.canonical.borrowedBinary

    def sortedIdsAreBounded: Boolean =
      check(); require(inverse != null)
      var sequenceIndex = 0
      while sequenceIndex < sequences.size do
        val ids = sequences(sequenceIndex).ids
        var index = 0
        while index < ids.length do
          if ids(index) < 0 || ids(index) >= chunks.size then return false
          index += 1
        sequenceIndex += 1
      true
    def allRemappedSequencesExact: Boolean =
      check(); require(permutation != null && inverse != null)
      var sequenceIndex = 0
      while sequenceIndex < sequences.size do
        val sequence = sequences(sequenceIndex)
        var offset = 0L
        var reference = 0
        while reference < sequence.ids.length do
          val id = sequence.ids(reference)
          if id < 0 || id >= chunks.size then return false
          val chunk = chunks(permutation(id))
          if chunk.encoding != sequence.encoding || chunk.size != Math.min(ChunkBytes.toLong, sequence.bytes.length - offset).toInt then return false
          var index = 0
          while index < chunk.size do
            if chunk.at(index, this) != sequence.bytes.at(offset + index) then return false
            index += 1
          offset = Math.addExact(offset, chunk.size.toLong); reference += 1
        if offset != sequence.bytes.length then return false
        sequenceIndex += 1
      true
    def recordFailure(cause: Throwable): Unit =
      require(Thread.currentThread() eq thread)
      if failure == null then
        failure = cause
        stage = "NG_CENSUS_" + stage
      else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure, cause)
    private var leftRange: RangeReader = null
    private var rightRange: RangeReader = null
    private var recipeRange: RecipeReader = null
    private var readerDepth = 0
    private var readerCharge = 0L
    private var frameCharge = 0L
    private def rangeSeek(): Unit =
      if diagnostics != null then diagnostics.rangeSeeks = Math.addExact(diagnostics.rangeSeeks, 1L)
      recordWork(1L)
    private def rangeAdvance(): Unit =
      if diagnostics != null then diagnostics.rangeAdvances = Math.addExact(diagnostics.rangeAdvances, 1L)
      recordSequentialProbe()
    private def rangeByte(): Unit =
      if diagnostics != null then diagnostics.rangeBytes = Math.addExact(diagnostics.rangeBytes, 1L)
      recordWork(1L)
    private def dagMove(push: Boolean): Unit =
      if diagnostics != null then
        if push then diagnostics.dagPushes = Math.addExact(diagnostics.dagPushes, 1L)
        else diagnostics.dagPops = Math.addExact(diagnostics.dagPops, 1L)
        diagnostics.dagDescents = Math.addExact(diagnostics.dagDescents, 1L)
      recordWork(1L)
    private def openReaders(): Unit =
      check()
      if readerDepth == 0 then
        root.reserveTemporaryMemory(768L, "two independent range readers and binary cursors BEFORE allocation")
        readerCharge = 768L
        // 先记录已收费用；任何后续配置异常均由调用方finally撤引用后结算。
        leftRange = new RangeReader
        leftRange.initializeBinaryCursor()
        rightRange = new RangeReader
        rightRange.initializeBinaryCursor()
      readerDepth = Math.addExact(readerDepth, 1)
    private def closeReaders(primary: Throwable): Unit =
      if readerDepth > 0 then readerDepth -= 1
      if readerDepth == 0 then
        leftRange = null; rightRange = null; recipeRange = null
        val charged = Math.addExact(readerCharge, frameCharge)
        try
          if charged != 0L then root.releaseTemporaryMemory(charged, "range readers and frame arrays references dropped")
          readerCharge = 0L; frameCharge = 0L
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    private def requireReaders(): Unit = require(readerDepth > 0 && leftRange != null && rightRange != null)
    private def openFrames(): Unit =
      requireReaders()
      if recipeRange == null then
        val count = recipes.size
        val charge = Math.addExact(576L, Math.multiplyExact(28L, count.toLong))
        root.reserveTemporaryMemory(charge, "recipe frame holder and three Int two Long arrays BEFORE allocation")
        frameCharge = charge
        recipeRange = new RecipeReader
        recipeRange.nodes = new Array[Int](count)
        recipeRange.events = new Array[Int](count)
        recipeRange.leaves = new Array[Int](count)
        recipeRange.positions = new Array[Long](count)
        recipeRange.remaining = new Array[Long](count)
      require(recipeRange.nodes.length >= recipes.size)

    /** 192B固定状态；seek仅发生在range/事件起点，UTF8在同一range内从当前codepoint继续。 */
    private final class RangeReader:
      private var binaryCursor: MySqlFormat3Binary.Cursor = null
      def initializeBinaryCursor(): Unit =
        require(binaryCursor == null)
        binaryCursor = new MySqlFormat3Binary.Cursor
      private var body: Body = null
      private var raw: RawBytes = null
      private var remaining = 0L
      private var position = 0L
      private var event = 0
      private var local = 0L
      private var character = 0
      private var point = 0
      private var width = 0
      private var encoded = 0
      def clear(): Unit = { body = null; raw = null; remaining = 0L }
      private def loadPoint(text: String): Unit =
        if character >= text.length then throw new SQLDataException("sequential UTF8 range underrun")
        recordUtf8(1L)
        val first = text.charAt(character); character += 1
        if Character.isHighSurrogate(first) then
          if character >= text.length then throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
          recordUtf8(1L)
          val second = text.charAt(character); character += 1
          if !Character.isLowSurrogate(second) then throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
          point = Character.toCodePoint(first, second)
        else
          if Character.isLowSurrogate(first) then throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
          point = first.toInt
        width = if point <= 127 then 1 else if point <= 2047 then 2 else if point <= 65535 then 3 else 4
        encoded = 0
      private def select(value: RawBytes, offset: Long): Unit =
        raw = value match
          case binary: BinaryBytes => binary.value
          case utf: Utf8Bytes => utf.original
          case _: Body => throw new IllegalStateException("original events cannot contain nested Body payloads")
          case other => other
        position = offset
        character = 0; point = 0; width = 0; encoded = 0
        raw match
          case binary: MySqlFormat3Binary =>
            binaryCursor.begin(binary, Math.toIntExact(offset), Math.min(remaining, binary.length.toLong - offset).toInt)
            while binaryCursor.seekPending do
              recordProbe(); binaryCursor.seekStep()
          case text: String =>
            var skipped = 0L
            while skipped < offset do
              loadPoint(text)
              val step = Math.min(width.toLong, offset - skipped).toInt
              encoded = step; skipped = Math.addExact(skipped, step.toLong)
          case _ => ()
      def reset(value: RawBytes, offset: Long, count: Long): Unit =
        require(value != null && offset >= 0L && count >= 0L)
        Math.addExact(offset, count)
        rangeSeek(); clear(); remaining = count
        value match
          case actual: Body =>
            require(offset <= actual.length - count)
            body = actual
            if count > 0L then
              event = if offset == 0L then 0 else actual.segments.locate(offset)
              local = if offset == 0L then 0L else offset - actual.segments.start(event)
              selectEvent()
          case _ =>
            value match
              case binary: MySqlFormat3Binary => require(offset <= binary.length.toLong - count)
              case bytes: Bytes => require(offset <= bytes.length - count)
              case _: String => () // 原strict长度预检已执行；逐codepoint读到端点仍拒截断。
            select(value, offset)
      private def selectEvent(): Unit =
        val events = body.segments
        while event < events.size && local == events.length(event) do
          rangeAdvance(); event += 1; local = 0L
        require(event < events.size, "body range event underrun")
        rangeAdvance()
        if !events.isLiteral(event) then select(events.bytes(event), Math.addExact(events.offset(event), local))
        else raw = null
      private def nextRaw(): Int =
        val result = raw match
          case _: MySqlFormat3Binary =>
            if binaryCursor.needsAdvance then
              rangeAdvance(); binaryCursor.advance()
            binaryCursor.next().toInt & 255
          case text: String =>
            if encoded == width then loadPoint(text)
            val value = if width == 1 then point
              else if encoded == 0 then (if width == 2 then 192 else if width == 3 then 224 else 240) | (point >> (6 * (width - 1)))
              else 128 | ((point >> (6 * (width - 1 - encoded))) & 63)
            encoded += 1
            value
          case literal: Literal => literal.at(position)
          case _ => throw new IllegalStateException("unknown original range payload")
        position = Math.addExact(position, 1L)
        result
      def next(): Int =
        require(remaining > 0L)
        rangeByte()
        val result = if body == null then nextRaw() else
          if local == body.segments.length(event) then
            rangeAdvance(); event += 1; local = 0L; selectEvent()
          val value = if body.segments.isLiteral(event) then body.segments.at(event, local) else nextRaw()
          local = Math.addExact(local, 1L)
          value
        remaining -= 1L
        result

    /** 256B holder加5个原始数组，无逐frame对象；父位置在进入child前保留，返回后不重扫前缀。 */
    private final class RecipeReader:
      var nodes: Array[Int] = null
      var events: Array[Int] = null
      var leaves: Array[Int] = null
      var positions: Array[Long] = null
      var remaining: Array[Long] = null
      private var depth = 0
      private var bytesLeft = 0L
      private def push(nodeId: Int, offset: Long, count: Long): Unit =
        val node = recipes(nodeId); node.requireReady()
        require(depth < nodes.length && offset >= 0L && count >= 0L && offset <= node.length - count)
        dagMove(true)
        nodes(depth) = nodeId
        events(depth) = if offset == 0L || count == 0L then 0 else node.segments.locate(offset)
        positions(depth) = if offset == 0L || count == 0L then 0L else offset - node.segments.start(events(depth))
        leaves(depth) = -1; remaining(depth) = count
        depth += 1
      def reset(nodeId: Int, offset: Long, count: Long): Unit =
        require(depth == 0 && bytesLeft == 0L)
        rangeSeek(); bytesLeft = count
        push(nodeId, offset, count)
        drain()
      private def drain(): Unit =
        while depth > 0 && remaining(depth - 1) == 0L do
          dagMove(false); depth -= 1
      def next(): Int =
        require(bytesLeft > 0L)
        while depth > 0 do
          drain()
          require(depth > 0)
          val slot = depth - 1
          val node = recipes(nodes(slot)); val source = node.segments
          var event = events(slot)
          while event < source.size && positions(slot) == source.length(event) do
            rangeAdvance(); event += 1; positions(slot) = 0L; leaves(slot) = -1
          events(slot) = event
          require(event < source.size, "recipe sequential event underrun")
          val local = positions(slot)
          if source.isLiteral(event) then
            rangeByte()
            val value = source.at(event, local)
            positions(slot) += 1L; remaining(slot) -= 1L; bytesLeft -= 1L
            drain()
            return value
          val target = node.targets(source.spanOrdinal(event))
          if target >= 0 then
            require(target < nodes(slot) && recipes(target).length < node.length, "recipe backref/length violation")
            val length = Math.min(source.length(event) - local, remaining(slot))
            positions(slot) = Math.addExact(local, length); remaining(slot) -= length
            push(target, local, length)
          else
            val leafIndex = Math.toIntExact(local / ChunkBytes)
            val id = node.leafIds(node.leafPosition(event, leafIndex))
            val leaf = chunks(if permutation == null then id else permutation(id))
            val inLeaf = local % ChunkBytes
            if leaves(slot) != leafIndex then
              rangeAdvance()
              leftRange.reset(leaf.bytes, Math.addExact(leaf.offset, inLeaf),
                Math.min(Math.min(leaf.size.toLong - inLeaf, source.length(event) - local), remaining(slot)))
              leaves(slot) = leafIndex
            val value = leftRange.next()
            positions(slot) += 1L; remaining(slot) -= 1L; bytesLeft -= 1L
            drain()
            return value
        throw new SQLDataException("recipe sequential range underrun")

    private def compare(left: Slice, right: Slice): Int =
      var result = Integer.compare(left.encoding, right.encoding)
      if result == 0 then result = Integer.compare(left.size, right.size)
      if result == 0 && method == Method.RecipeV3 then
        requireReaders()
        leftRange.reset(left.bytes, left.offset, left.size.toLong)
        rightRange.reset(right.bytes, right.offset, right.size.toLong)
      var index = 0
      while result == 0 && index < left.size do
        recordComparison()
        result = if method == Method.RecipeV3 then Integer.compare(leftRange.next(), rightRange.next())
          else Integer.compare(left.at(index, this), right.at(index, this))
        index += 1
      result
    /** 候选payload已由原node/events强持；相等时不配置第二个Slice。 */
    private def compareCandidate(bytes: RawBytes, offset: Long, size: Int, encoding: Int, right: Slice): Int =
      var result = Integer.compare(encoding, right.encoding)
      if result == 0 then result = Integer.compare(size, right.size)
      if result == 0 then
        requireReaders()
        leftRange.reset(bytes, offset, size.toLong)
        rightRange.reset(right.bytes, right.offset, size.toLong)
      var index = 0
      while result == 0 && index < size do
        recordComparison()
        result = Integer.compare(leftRange.next(), rightRange.next()); index += 1
      result
    private def sequence(value: Bytes, encoding: Int): Sequence =
      pendingSequenceBytes = value
      stage = "SEQUENCE_INSERTION_PREPARE"
      val count = if method == Method.RecipeV3 then 0 else
        MySqlFormat3JvmArrayLimit.requireLength(Math.addExact(value.length, ChunkBytes - 1L) / ChunkBytes, "census B sequence count")
      sequences.prepare()
      charge(Math.addExact(112L, Math.multiplyExact(count.toLong, 4L)), "census Bsequence/IDarray BEFORE allocation")
      val ids = new Array[Int](count)
      val result = new Sequence(value, ids, encoding)
      sequences.publish(result)
      pendingSequenceBytes = null
      stage = "SEQUENCE_CHUNKS"
      var offset = 0L
      var position = 0
      while position < count do
        charge(64L, "census exact chunk borrowed span BEFORE allocation")
        pendingChunk = new Slice(value, offset, Math.min(ChunkBytes.toLong, value.length - offset).toInt, encoding, this)
        val candidate = pendingChunk
        var index = 0
        while index < chunks.size && compare(candidate, chunks(index)) != 0 do index += 1
        ids(position) = if index < chunks.size then index else chunks.add(candidate)
        pendingChunk = null
        offset = Math.addExact(offset, candidate.size.toLong); position += 1
        references = Math.addExact(references, 1L)
      result
    private def binarySequence(value: MySqlFormat3Binary, encoding: Int): Sequence =
      charge(48L, "census borrowed binary sequence BEFORE allocation")
      sequence(new BinaryBytes(value), encoding)
    private def textSequence(value: String): Sequence =
      charge(48L, "census borrowed UTF8 sequence BEFORE allocation")
      sequence(new Utf8Bytes(value, this), Utf8Encoding)
    private def tag(cell: MySqlFormat3Cell): Int = cell match
      case MySqlFormat3Cell.SqlNull => 0
      case MySqlFormat3Cell.Text(_) => 1
      case MySqlFormat3Cell.Binary(_) => 2
      case MySqlFormat3Cell.SignedInteger(_) => 3
      case MySqlFormat3Cell.Floating(_) => 4
      case MySqlFormat3Cell.DateTime(_) => 5
    private def compareCell(left: MySqlFormat3Cell, right: MySqlFormat3Cell): Int =
      val kind = Integer.compare(tag(left), tag(right))
      if kind != 0 then kind else (left, right) match
        case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => 0
        case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) => a.compareTo(b)
        case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
        case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
        case (MySqlFormat3Cell.Floating(a), MySqlFormat3Cell.Floating(b)) => java.lang.Long.compareUnsigned(a, b)
        case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
        case _ => throw new IllegalStateException("census typed comparison tag mismatch")
    private def atom(cell: MySqlFormat3Cell): Int =
      var index = 0
      while index < atoms.size && compareCell(cell, atoms(index).cell) != 0 do index += 1
      if index < atoms.size then index else
        charge(64L, "census exact typed atom BEFORE allocation")
        val bytes = cell match
          case MySqlFormat3Cell.Text(value) => textSequence(value)
          case MySqlFormat3Cell.Binary(value) => binarySequence(value, BinaryEncoding)
          case _ => null
        atoms.prepare()
        atoms.publish(new Atom(cell, bytes))
    def beginTypedField(cell: MySqlFormat3Cell): Unit =
      check(); require(!fieldOpen && cell != null)
      fieldOpen = true; atom(cell)
    def endTypedField(): Unit = { check(); require(fieldOpen); fieldOpen = false }
    def row(table: String, values: Vector[MySqlFormat3Cell]): Int =
      check()
      val relation = MySqlFormat3RelationShapes.canonicalOrder.indexWhere(_.table == table)
      require(relation >= 0 && values.size == MySqlFormat3RelationShapes.canonicalOrder(relation).columns.size)
      var index = 0
      while index < rows.size && (rows(index).relation != relation || rows(index).original != values) do index += 1
      if index < rows.size then index else
        charge(Math.addExact(112L, Math.multiplyExact(values.size.toLong, 4L)), "census row/Aref array BEFORE allocation")
        rows.prepare()
        val ids = new Array[Int](values.size)
        val result = new Row(relation, values, ids)
        val id = rows.publish(result)
        var column = 0
        while column < values.size do
          ids(column) = atom(values(column))
          column += 1
        id
    private[MySqlModeEnvelopeV2Census] def capture(kind: Int, encoding: Int, association: Int, expected: MySqlRecoveryCanonicalBytes,
        write: DataOutputStream => Unit): Body =
      try capturePrepared(kind, encoding, association, expected, write)
      catch
        case cause: Throwable =>
          recordFailure(cause)
          throw cause
    private def capturePrepared(kind: Int, encoding: Int, association: Int, expected: MySqlRecoveryCanonicalBytes,
        write: DataOutputStream => Unit): Body =
      check(); require(chunks != null)
      if expected != null then
        val name = encoding match
          case 2 => "FP_MYSQL_RECOVERY_OPERATION_RAW_ROW_BE_V1"
          case 3 => "FP_MYSQL_MODE_EFFECT_PROJECTION_BE_V1"
          case 16 => "FP_MYSQL_MODE_EFFECT_PROJECTION_BE_V2"
          case 4 => "FP_MYSQL_MODE_EFFECT_FINGERPRINT_BE_V1"
          case 5 => "FP_MYSQL_MODE_SEMANTIC_WITNESS_BE_V1"
          case 7 => "FP_MYSQL_REPOSITORY_REQUEST_BE_V1"
          case 8 => "FP_MYSQL_REPOSITORY_OPERATION_CONTEXT_BE_V1"
          case 9 => "FP_MYSQL_RECOVERY_OPERATION_SCOPE_BE_V1"
          case 10 => "FP_RECOVERY_OPERATION_BE_V1"
          case _ => throw new SQLDataException("census unknown canonical encoding association")
        require(expected.encoding == name, "census exact retained encoding association")
      stage = "BODY_INSERTION_PREPARE"
      attemptedBodies.prepare()
      charge(256L, "census BeginBody/composite/store BEFORE allocation")
      if method == Method.RecipeV3 then charge(320L, "packed event store holder with sequential cursor and payload directory BEFORE allocation")
      val body = new Body(kind, encoding, association, this, expected)
      attemptedBodies.publish(body)
      if diagnostics != null then diagnostics.current = body
      stage = "BODY_STREAM_WORKSPACE"
      root.reserveTemporaryMemory(2048L, "census digest/stream/comparison BEFORE allocation")
      scratchPeak = Math.max(scratchPeak, 2048L)
      var primary: Throwable = null
      try
        val digest = MessageDigest.getInstance("SHA-256")
        val stream = new Stream(this, body, digest)
        val output = new EventOutput(this, body, stream)
        write(output); output.flush()
        require(stream.advertised == 0L && !fieldOpen, "census EndBody exact event settlement")
        body.finish(digest.digest())
        var existing = 0
        while existing < bodies.size && !(bodies(existing).kind == kind && bodies(existing).encoding == encoding &&
            bodies(existing).association == association && body.expectedBinary != null &&
            bodies(existing).expectedBinary == body.expectedBinary) do existing += 1
        if existing < bodies.size then
          body.sequence = bodies(existing).sequence
          if diagnostics != null then diagnostics.completed(body, true)
          bodies(existing)
        else
          body.sequence = sequence(body, encoding)
          bodies.add(body)
          if diagnostics != null then diagnostics.completed(body, false)
          body
      catch
        case cause: Throwable => primary = cause; recordFailure(cause); throw cause
      finally
        try root.releaseTemporaryMemory(2048L, "census digest/stream references dropped")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    /** 真实fixed primitive seams供direct DATA tests；不接caller serializer。 */
    def observeSequence(value: MySqlFormat3Binary, encoding: Int): Unit =
      try { check(); require(sequences != null); binarySequence(value, encoding); () }
      catch
        case cause: Throwable =>
          recordFailure(cause)
          throw cause
    def observeBinary(value: MySqlFormat3Binary, encoding: Int): Body =
      capture(100, encoding, 0, null, output => binary(output, value))
    def observeUtf8(value: String): Body =
      capture(101, Utf8Encoding, 0, null, output => text(output, value))
    /** Fixed primitive/zero-span writer for page-boundary DATA verification; no caller serializer. */
    def observeEventBoundary(count: Int, empty: MySqlFormat3Binary): Body =
      require(count >= 1 && count <= 65 && empty != null && empty.length == 0)
      capture(103, BinaryEncoding, count, null, output =>
        var index = 0
        while index < 64 do
          binary(output, empty); index += 1
        index = 0
        while index < count do
          if index % 2 == 0 then long(output, Long.MinValue + index) else byte(output, index)
          index += 1
        index = 0
        while index < 64 do
          binary(output, empty); index += 1
        byte(output, 255)
        index = 0
        while index < 65 do
          binary(output, empty); index += 1)
    /** 固定空writer/全零record路径，验证没有byte时的尾部事件结算。 */
    def observeEmptyEvents(empty: MySqlFormat3Binary, records: Boolean): Body =
      require(empty != null && empty.length == 0)
      capture(105, BinaryEncoding, if records then 1 else 0, null, output =>
        if records then
          var index = 0
          while index < 65 do
            binary(output, empty); index += 1)
    /** 固定稀疏payload布局：纯literal/纯span/交替/64literal后span/span后63literal再span。 */
    def observeSparseEvents(count: Int, layout: Int, value: MySqlFormat3Binary): Body =
      require(count >= 1 && count <= 65 && layout >= 0 && layout <= 4 && value.length == 1)
      capture(108, BinaryEncoding, layout, null, output =>
        if layout == 3 then
          var prefix = 0
          while prefix < 64 do
            long(output, Long.MinValue + prefix); prefix += 1
        if layout == 4 then
          binary(output, value)
          var middle = 0
          while middle < 63 do
            byte(output, middle); middle += 1
          binary(output, value)
        else
          var index = 0
          while index < count do
            if layout == 0 || layout == 2 then long(output, Long.MinValue + index)
            if layout != 0 then binary(output, value)
            index += 1)
    def observeSlice(value: MySqlFormat3Binary, offset: Long, size: Long): Body =
      capture(104, BinaryEncoding, 0, null, output =>
        val e = event(output)
        if method == Method.RecipeV3 then e.body.appendRaw(value, offset, size)
        else
          charge(48L, "census immutable BinarySpan BEFORE allocation")
          e.body.append(new BinaryBytes(value), offset, size)
        e.stream.advertisedSpan(size)
        var index = 0L
        while index < size do
          output.writeByte(value.byteAt(Math.toIntExact(Math.addExact(offset, index))).toInt)
          index += 1L)
    /** 固定双编码/重复span writer，用原resolver核验leaf首次发现顺序。 */
    def observeLeafKinds(value: MySqlFormat3Binary, textValue: String): Unit =
      require(method == Method.RecipeV3 && bodies.size == 0)
      capture(106, BinaryEncoding, 0, null, output =>
        int(output, value.length); binary(output, value)
        int(output, value.length); binary(output, value))
      capture(107, Utf8Encoding, 0, null, output =>
        text(output, textValue); text(output, textValue))
    def resolveObservedLeafKinds(): Unit =
      require(method == Method.RecipeV3 && bodies.size == 2 && bodies(0).kind == 106 && bodies(1).kind == 107)
      try prepareRecipes()
      catch
        case cause: Throwable => recordFailure(cause); throw cause
    def resolveObservedSparseEvents(): Unit =
      require(method == Method.RecipeV3 && bodies.size == 1 && bodies(0).kind == 108)
      try prepareRecipes()
      catch
        case cause: Throwable => recordFailure(cause); throw cause
    def uniqueRecipeLeafCharge: Long = recipeLeafSpanMetadata
    def observedLeafId(bodyIndex: Int, eventIndex: Int, leafIndex: Int): Int =
      val node = recipes(bodies(bodyIndex).recipe)
      node.leafIds(node.leafPosition(eventIndex, leafIndex))
    /** 固定shared-child writer，经原capture/resolver形成严格递减DAG；不接caller serializer。 */
    def observeRepeatedBinary(value: MySqlFormat3Binary): Body =
      capture(109, BinaryEncoding, 0, null, output =>
        binary(output, value); binary(output, value); byte(output, 255))
    def resolveObservedRanges(): Unit =
      require(method == Method.RecipeV3 && bodies.size > 0)
      try prepareRecipes()
      catch
        case cause: Throwable => recordFailure(cause); throw cause
    /** 固定原Body range与独立binary比较，覆盖非零event起点；不复用capture cursor。 */
    def observedRangeEquals(bodyIndex: Int, offset: Long, expected: MySqlFormat3Binary): Boolean =
      var primary: Throwable = null
      try
        openReaders()
        leftRange.reset(bodies(bodyIndex), offset, expected.length.toLong)
        rightRange.reset(expected, 0L, expected.length.toLong)
        var index = 0
        while index < expected.length do
          recordComparison()
          if leftRange.next() != rightRange.next() then return false
          index += 1
        true
      catch
        case cause: Throwable => primary = cause; throw cause
      finally closeReaders(primary)
    // 固定临时责任seam：让DATA test在真实checked close失败后核primary/suppressed与撤引用。
    def openObservedReaderWorkspace(): Unit =
      require(method == Method.RecipeV3 && recipes != null)
      openReaders(); openFrames()
    def closeObservedReaderWorkspace(primary: Throwable): Unit = closeReaders(primary)
    def heldReaderWorkspaceBytes: Long = Math.addExact(readerCharge, frameCharge)
    def hasReaderWorkspaceReferences: Boolean = leftRange != null || rightRange != null || recipeRange != null

    def compareOriginalCanonical(expected: MySqlRecoveryCanonicalBytes, actual: MySqlFormat3Binary): Body =
      capture(102, 2, 0, expected, output => binary(output, actual))

    private def order(count: Int, comparison: (Int, Int) => Int): (Array[Int], Array[Int]) =
      if method != Method.RecipeV3 then return orderHeld(count, comparison)
      var primary: Throwable = null
      try
        openReaders()
        orderHeld(count, comparison)
      catch
        case cause: Throwable => primary = cause; throw cause
      finally closeReaders(primary)
    private def orderHeld(count: Int, comparison: (Int, Int) => Int): (Array[Int], Array[Int]) =
      charge(Math.addExact(192L, Math.multiplyExact(count.toLong, 8L)), "census permutation/inverse BEFORE allocation")
      val ids = new Array[Int](count); val back = new Array[Int](count)
      var index = 0
      while index < count do
        ids(index) = index
        var slot = index
        while slot > 0 && comparison(ids(slot - 1), ids(slot)) > 0 do
          val held = ids(slot - 1); ids(slot - 1) = ids(slot); ids(slot) = held; slot -= 1
        index += 1
      index = 0
      while index < count do
        back(ids(index)) = index
        index += 1
      (ids, back)
    private def compareRow(left: Row, right: Row): Int =
      var result = Integer.compare(left.relation, right.relation)
      var column = 0
      while result == 0 && column < left.original.size do
        result = compareCell(left.original(column), right.original(column)); column += 1
      result

    private def sameBytes(left: RawBytes, offset: Long, size: Long, right: Bytes): Boolean =
      require(offset >= 0L && size >= 0L && offset <= rawLength(left, this) - size)
      if size != right.length then false else
        requireReaders()
        leftRange.reset(left, offset, size)
        rightRange.reset(right, 0L, size)
        var index = 0L
        while index < size do
          recordComparison()
          if leftRange.next() != rightRange.next() then return false
          index += 1L
        true

    private def newRecipe(bytes: Bytes, kind: Int, encoding: Int, association: Int, digest: Array[Byte],
        originalSegments: Events = null): Int =
      recipes.prepare()
      recipeCharge(192L, false, "recipe node BEFORE allocation")
      val segments = if originalSegments != null then originalSegments else
        charge(96L, "recipe anonymous single-span holder BEFORE allocation")
        new SingleSpanEvents(bytes)
      val id = recipes.publish(new RecipeNode(bytes, kind, encoding, association, digest, segments))
      val node = recipes(id)
      val size = segments.spanCount
      val arrayCharge = Math.addExact(64L, Math.multiplyExact(size.toLong, 4L))
      recipeCharge(arrayCharge, false, "recipe span targets BEFORE allocation")
      node.targets = new Array[Int](size)
      recipeCharge(arrayCharge, false, "recipe span starts BEFORE allocation")
      node.starts = new Array[Int](size)
      id

    private def originalBody(bytes: RawBytes, offset: Long, length: Long, parentLength: Long): Int =
      var position = 0
      while position < recipeBodyOrder.length do
        val body = bodies(recipeBodyOrder(position))
        if body.recipe >= 0 && body.length < parentLength && sameBytes(bytes, offset, length, body) then
          return body.recipe
        position += 1
      -1

    /** 先按原事件顺序判定body/leaf并计算精确总量，再按同顺序发现leaf；不建立第二份事件图。 */
    private def resolveRecipe(nodeId: Int): Unit =
      val node = recipes(nodeId)
      val events = node.segments
      require(!node.ready && node.resolvedSlots == 0)
      var leaves = 0
      var event = 0
      while event < events.size do
        if events.isLiteral(event) then node.wireSegments = Math.addExact(node.wireSegments, 1)
        else
          val ordinal = events.spanOrdinal(event)
          require(ordinal == node.resolvedSlots)
          val bytes = events.bytes(event)
          val offset = events.offset(event); val length = events.length(event)
          require(offset >= 0L && length >= 0L && offset <= rawLength(bytes, this) - length)
          val body = originalBody(bytes, offset, length, node.length)
          node.starts(ordinal) = leaves
          node.targets(ordinal) = body
          if body >= 0 then
            require(body < nodeId, "recipe body reference must point backward")
            recipes(body).requireReady()
            node.wireSegments = Math.addExact(node.wireSegments, 1)
            recipeBodyReferences = Math.addExact(recipeBodyReferences, 1L)
          else
            val count = node.leafCount(event)
            leaves = Math.addExact(leaves, count)
            node.wireSegments = Math.addExact(node.wireSegments, count)
          node.resolvedSlots = Math.addExact(node.resolvedSlots, 1)
        event += 1
      require(node.resolvedSlots == events.spanCount)
      if leaves > 0 then
        recipeCharge(Math.addExact(64L, Math.multiplyExact(leaves.toLong, 4L)), false,
          "recipe flat leaf indices BEFORE allocation")
        node.leafIds = new Array[Int](leaves)
      event = 0
      var filled = 0
      while event < events.size do
        if !events.isLiteral(event) then
          val ordinal = events.spanOrdinal(event)
          if node.targets(ordinal) < 0 then
            val bytes = events.bytes(event)
            val length = events.length(event); val offset = events.offset(event)
            val encoding = bytes match
              case _: Utf8Bytes | _: String => Utf8Encoding
              case _ => BinaryEncoding
            var consumed = 0L
            var leafIndex = 0
            require(node.starts(ordinal) == filled)
            while consumed < length do
              val size = Math.min(ChunkBytes.toLong, length - consumed).toInt
              val candidateOffset = Math.addExact(offset, consumed)
              var candidate = 0
              while candidate < chunks.size && compareCandidate(bytes, candidateOffset, size, encoding, chunks(candidate)) != 0 do candidate += 1
              val id = if candidate < chunks.size then candidate else
                recipeCharge(64L, true, "recipe leaf slice BEFORE allocation")
                pendingChunk = new Slice(bytes, candidateOffset, size, encoding, this)
                recipeLeafBytes = Math.addExact(recipeLeafBytes, size.toLong)
                chunks.add(pendingChunk)
              pendingChunk = null
              node.leafIds(filled) = id
              filled = Math.addExact(filled, 1); leafIndex += 1
              consumed = Math.addExact(consumed, size.toLong)
            require(leafIndex == node.leafCount(event))
        event += 1
      require(filled == leaves)
      node.ready = true

    private def recipeDigest(bytes: Bytes): Array[Byte] =
      root.reserveTemporaryMemory(2048L, "recipe anonymous canonical digest scratch")
      var primary: Throwable = null
      try
        val digest = MessageDigest.getInstance("SHA-256")
        requireReaders()
        rightRange.reset(bytes, 0L, bytes.length)
        var index = 0L
        while index < bytes.length do
          digest.update(rightRange.next().toByte); index += 1L
        charge(96L, "recipe anonymous digest custody BEFORE allocation")
        digest.digest()
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try root.releaseTemporaryMemory(2048L, "recipe anonymous digest scratch dropped")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

    private def prepareRecipes(): Unit =
      var primary: Throwable = null
      try
        openReaders()
        prepareRecipesHeld()
      catch
        case cause: Throwable => primary = cause; throw cause
      finally closeReaders(primary)
    private def prepareRecipesHeld(): Unit =
      require(method == Method.RecipeV3 && recipes == null && chunks.size == 0)
      stage = "RECIPE_DISCOVERY"
      capturedMetadata = metadata
      charge(128L, "recipe node store BEFORE allocation")
      recipes = new Store[RecipeNode](this)
      val ordered = order(bodies.size, (a, b) =>
        val left = bodies(a); val right = bodies(b)
        var value = java.lang.Long.compare(left.length, right.length)
        if value == 0 then value = Integer.compare(left.kind, right.kind)
        if value == 0 then value = Integer.compare(left.encoding, right.encoding)
        if value == 0 then value = Integer.compare(left.association, right.association)
        if value == 0 then value = Integer.compare(a, b)
        value)
      recipeBodyOrder = ordered._1; recipeBodyInverse = ordered._2
      var position = 0
      while position < recipeBodyOrder.length do
        val body = bodies(recipeBodyOrder(position))
        body.recipe = newRecipe(body, body.kind, body.encoding, body.association, body.digest, body.segments)
        resolveRecipe(body.recipe)
        require(recipes(body.recipe).length == body.length, "recipe keeps every original writer byte")
        body.sequence.recipe = body.recipe
        position += 1
      var index = 0
      while index < sequences.size do
        val sequence = sequences(index)
        if sequence.recipe < 0 then
          val body = originalBody(sequence.bytes, 0L, sequence.bytes.length, Long.MaxValue)
          if body >= 0 then sequence.recipe = body
          else
            var existing = recipeBodyOrder.length
            while existing < recipes.size && !(recipes(existing).encoding == sequence.encoding &&
                sameBytes(sequence.bytes, 0L, sequence.bytes.length, recipes(existing).bytes)) do existing += 1
            if existing < recipes.size then sequence.recipe = existing
            else
              sequence.recipe = newRecipe(sequence.bytes, 0, sequence.encoding, -1, recipeDigest(sequence.bytes))
              resolveRecipe(sequence.recipe)
        index += 1
      index = 0
      while index < recipes.size do
        recipeSegments = Math.addExact(recipeSegments, recipes(index).wireSegments.toLong)
        index += 1
      require(allRecipesExact, "recipe expansion differs from original unique writer")

    /** Iterative descent, so an arbitrary DAG depth cannot consume the JVM call stack. */
    private def recipeByte(original: Int, at: Long): Int =
      var nodeId = original
      var offset = at
      var depth = 0
      while depth <= recipes.size do
        if diagnostics != null then
          diagnostics.dagDescents = Math.addExact(diagnostics.dagDescents, 1L); recordWork(1L)
        val node = recipes(nodeId)
        require(offset >= 0L && offset < node.length)
        val selectedIndex = node.segments.locate(offset)
        val local = offset - node.segments.start(selectedIndex)
        node.requireReady()
        if node.segments.isLiteral(selectedIndex) then return node.segments.at(selectedIndex, local)
        val ordinal = node.segments.spanOrdinal(selectedIndex)
        val target = node.targets(ordinal)
        if target < 0 then
          val leafIndex = (local / ChunkBytes).toInt
          val id = node.leafIds(node.leafPosition(selectedIndex, leafIndex))
          val leaf = chunks(if permutation == null then id else permutation(id))
          val inLeaf = (local % ChunkBytes).toInt
          require(inLeaf < leaf.size)
          return leaf.at(inLeaf, this)
        else
          require(target < nodeId && recipes(target).length < node.length, "recipe backref/length violation")
          nodeId = target; offset = local; depth += 1
      throw new SQLDataException("recipe descent exceeded finite DAG")

    def allRecipesExact: Boolean =
      check(); require(method == Method.RecipeV3 && recipes != null)
      var ready = 0
      while ready < recipes.size do
        recipes(ready).requireReady(); ready += 1
      var primary: Throwable = null
      try
        openReaders()
        openFrames()
        allRecipesExactHeld
      catch
        case cause: Throwable => primary = cause; throw cause
      finally closeReaders(primary)
    private def allRecipesExactHeld: Boolean =
      var total = 0L
      var nodeId = 0
      while nodeId < recipes.size do
        val node = recipes(nodeId)
        node.requireReady()
        if node.length != node.bytes.length then return false
        total = Math.addExact(total, node.length)
        recipeRange.reset(nodeId, 0L, node.length)
        rightRange.reset(node.bytes, 0L, node.length)
        var index = 0L
        while index < node.length do
          if diagnostics != null then
            diagnostics.expandedBytes = Math.addExact(diagnostics.expandedBytes, 1L); recordWork(1L)
          recordComparison()
          if recipeRange.next() != rightRange.next() then return false
          index += 1L
        nodeId += 1
      recipeExpandedBytes = total
      true
    private final class Count extends OutputStream:
      var length = 0L
      def write(value: Int): Unit = length = Math.addExact(length, 1L)
      override def write(value: Array[Byte], start: Int, size: Int): Unit =
        require(value != null && start >= 0 && size >= 0 && start <= value.length - size)
        length = Math.addExact(length, size.toLong)
      def bytes(size: Long): Unit = { require(size >= 0L); length = Math.addExact(length, size) }
    /** 固定 census grammar 的 primitive count sink；没有 output bytes。 */
    def finish(source: MySqlModeWitnessCodec.SourceDescriptor, materialization: MySqlModeWorkflowMaterialization,
        context: MySqlRepositoryOperationContextIdentity): Facts =
      check(); stage = "WIRE_DISCOVERY"
      // 所有 wire text/binary字段进入完整B，不只保摘要；这些小 descriptor view仍计费。
      val sourceBefore = binarySequence(source.predecessor, if source.predecessorKind == 4 then 6 else 15)
      val sourceReceipt = binarySequence(source.originalReceipt, 11)
      val request = binarySequence(materialization.exactPlan.requestIdentity.canonical.borrowedBinary, 7)
      val contextBody = binarySequence(draft.retainedContextCanonical.borrowedBinary, 8)
      // grammar header所需的所有String按原字段顺序登记；无省略字段。
      textSequence(materialization.exactPlan.requestKind)
      textSequence(context.barrierNamespace); textSequence(context.serverVersion); textSequence(context.schema)
      textSequence(context.effectiveUser); textSequence(context.ddlSha256); textSequence(context.approvedObjectManifestSha256)
      textSequence(context.sourceProvenanceKind); textSequence(context.requestKind)
      var image = 0
      while image < 3 do
        val snapshot = if image == 0 then materialization.preimage else if image == 1 then materialization.reserveTarget else materialization.executeTarget
        textSequence(snapshot.schema); textSequence(snapshot.connection.serverVersion); textSequence(snapshot.connection.catalog)
        textSequence(snapshot.connection.effectiveUser); textSequence(snapshot.connection.defaultStorageEngine)
        textSequence(snapshot.connection.transactionIsolation); textSequence(snapshot.connection.readTransactionIsolation)
        textSequence(snapshot.connection.sqlMode); textSequence(snapshot.connection.timeZone)
        textSequence(snapshot.connection.tlsVersion); textSequence(snapshot.connection.tlsCipher)
        image += 1
      if method == Method.RecipeV3 then prepareRecipes()
      stage = "SORT_AND_REMAP"
      val bp = order(chunks.size, (a, b) => compare(chunks(a), chunks(b))); permutation = bp._1; inverse = bp._2
      val ap = order(atoms.size, (a, b) => compareCell(atoms(a).cell, atoms(b).cell)); atomPermutation = ap._1; atomInverse = ap._2
      val rp = order(rows.size, (a, b) => compareRow(rows(a), rows(b))); rowPermutation = rp._1; rowInverse = rp._2
      var sequenceIndex = 0
      while sequenceIndex < sequences.size do
        val ids = sequences(sequenceIndex).ids
        var index = 0
        while index < ids.length do
          ids(index) = inverse(ids(index))
          index += 1
        sequenceIndex += 1
      if method == Method.RecipeV3 then
        var nodeId = 0
        while nodeId < recipes.size do
          val node = recipes(nodeId)
          node.requireReady()
          var index = 0
          while node.leafIds != null && index < node.leafIds.length do
            node.leafIds(index) = inverse(node.leafIds(index))
            index += 1
          nodeId += 1
        require(allRecipesExact, "sorted recipe leaf references changed original bytes")
      var rowIndex = 0
      while rowIndex < rows.size do
        val ids = rows(rowIndex).atoms
        var index = 0
        while index < ids.length do
          ids(index) = atomInverse(ids(index))
          index += 1
        rowIndex += 1
      charge(512L, "census primitive count sink BEFORE allocation")
      val count = new Count
      val out = new DataOutputStream(count)
      references = 0L
      def seq(value: Sequence): Unit =
        out.writeLong(value.bytes.length)
        if method == Method.RecipeV3 then
          require(value.recipe >= 0 && value.recipe < recipes.size)
          out.writeInt(value.recipe); references = Math.addExact(references, 1L)
        else
          out.writeInt(value.ids.length)
          var index = 0
          while index < value.ids.length do
            out.writeInt(value.ids(index)); references = Math.addExact(references, 1L); index += 1
      def text(value: String): Unit =
        var index = 0
        var found = false
        while index < sequences.size && !found do
          found = sequences(index).bytes match
            case utf: Utf8Bytes => utf.original == value
            case _ => false
          if !found then index += 1
        require(found, "all grammar text discovered before sort")
        seq(sequences(index))
      def uuid(value: UUID): Unit = { out.writeLong(value.getMostSignificantBits); out.writeLong(value.getLeastSignificantBits) }
      def date(value: LocalDateTime): Unit =
        require(value.getNano % 1000 == 0); out.writeInt(value.getYear)
        out.writeByte(value.getMonthValue); out.writeByte(value.getDayOfMonth); out.writeByte(value.getHour)
        out.writeByte(value.getMinute); out.writeByte(value.getSecond); out.writeInt(value.getNano / 1000)
      def hash(value: Vector[Byte]): Unit = { require(value.size == 32); value.foreach(v => out.writeByte(v.toInt)) }
      // Header domain is literal strict UTF8; exact profile97/SHA32 are inline, not borrowed temporary arrays.
      MySqlRecoveryCanonicalBytes.writeText(out, if method == Method.RecipeV3 then RecipeDomain else Domain)
      out.writeInt(if method == Method.RecipeV3 then 3 else 2); out.writeInt(1)
      out.writeInt(97); count.bytes(97L); count.bytes(32L)
      val headerEnd = count.length
      out.writeInt(chunks.size)
      var index = 0
      while index < chunks.size do
        val chunk = chunks(permutation(index)); out.writeInt(chunk.encoding); out.writeInt(chunk.size); count.bytes(chunk.size.toLong)
        index += 1
      if method == Method.RecipeV3 then
        out.writeInt(recipes.size)
        var nodeId = 0
        while nodeId < recipes.size do
          val node = recipes(nodeId)
          out.writeInt(node.kind); out.writeInt(node.encoding); out.writeInt(node.association)
          out.writeLong(node.length); out.write(node.digest); out.writeInt(node.wireSegments)
          var partIndex = 0
          while partIndex < node.segments.size do
            node.requireReady()
            if node.segments.isLiteral(partIndex) then
              out.writeByte(0); out.writeByte(node.segments.kind(partIndex)); out.writeByte(node.segments.width(partIndex))
              out.writeLong(node.segments.bits(partIndex))
            else
              val ordinal = node.segments.spanOrdinal(partIndex)
              val target = node.targets(ordinal)
              if target >= 0 then
                out.writeByte(2); out.writeInt(target); out.writeLong(0L); out.writeLong(node.segments.length(partIndex))
                references = Math.addExact(references, 1L)
              else
                var leaf = 0
                while leaf < node.leafCount(partIndex) do
                  val id = node.leafIds(node.leafPosition(partIndex, leaf))
                  out.writeByte(1); out.writeInt(id); out.writeLong(0L); out.writeLong(chunks(permutation(id)).size.toLong)
                  references = Math.addExact(references, 1L); leaf += 1
            partIndex += 1
          nodeId += 1
      val bEnd = count.length
      out.writeInt(atoms.size); index = 0
      while index < atoms.size do
        val value = atoms(atomPermutation(index)); out.writeByte(tag(value.cell))
        value.cell match
          case MySqlFormat3Cell.SqlNull => ()
          case MySqlFormat3Cell.Text(_) | MySqlFormat3Cell.Binary(_) => seq(value.sequence)
          case MySqlFormat3Cell.SignedInteger(bits) => out.writeLong(bits)
          case MySqlFormat3Cell.Floating(bits) => out.writeLong(bits)
          case MySqlFormat3Cell.DateTime(value) =>
            out.writeInt(value.getYear); out.writeByte(value.getMonthValue); out.writeByte(value.getDayOfMonth)
            out.writeByte(value.getHour); out.writeByte(value.getMinute); out.writeByte(value.getSecond); out.writeInt(value.getNano)
        index += 1
      val aEnd = count.length
      out.writeInt(rows.size); index = 0
      while index < rows.size do
        val value = rows(rowPermutation(index)); out.writeInt(value.relation); out.writeInt(value.atoms.length)
        var column = 0
        while column < value.atoms.length do
          out.writeInt(value.atoms(column)); references = Math.addExact(references, 1L); column += 1
        index += 1
      val rEnd = count.length
      // Source + plan + context: explicit same original DATA fields; UUID/SHA are inline fixed bytes.
      out.writeInt(source.predecessorKind); out.writeLong(source.predecessorSequence); out.writeLong(source.volume)
      source.fileId.foreach(v => out.writeByte(v.toInt)); seq(sourceBefore); hash(source.predecessorSha); hash(source.anchorSha); seq(sourceReceipt)
      val sourceEnd = count.length
      val plan = materialization.exactPlan
      uuid(plan.tag.bootId); out.writeLong(plan.tag.bootGeneration); out.writeLong(plan.tag.connectionGeneration); uuid(plan.tag.operationId)
      uuid(plan.workflowUuid); uuid(plan.reserveOperationUuid); uuid(plan.executeOperationUuid); text(plan.requestKind); seq(request); count.bytes(32L); out.writeInt(1)
      val planEnd = count.length
      uuid(context.tag.bootId); out.writeLong(context.tag.bootGeneration); out.writeLong(context.tag.connectionGeneration); uuid(context.tag.operationId)
      uuid(context.workflowUuid); uuid(context.reserveOperationUuid); uuid(context.barrierGenerationUuid); out.writeLong(context.actualConnectionId)
      text(context.barrierNamespace); uuid(context.bindingUuid); out.writeLong(context.writerEpoch); date(context.fixedLeaseUntil)
      uuid(context.serverUuid); text(context.serverVersion); text(context.schema); text(context.effectiveUser); uuid(context.provisioningUuid)
      out.writeInt(context.schemaFormat); out.writeInt(context.physicalFormat); text(context.ddlSha256); text(context.approvedObjectManifestSha256)
      hash(context.configurationSha256); text(context.sourceProvenanceKind); hash(context.sourceProvenanceSha256); hash(context.limitProfileSha256)
      text(context.requestKind); hash(context.requestIdentitySha256); seq(contextBody)
      date(materialization.plannedAt); out.writeLong(materialization.originalLocatorRevision)
      val contextEnd = count.length
      out.writeInt(3); image = 0
      while image < 3 do
        val snapshot = if image == 0 then materialization.preimage else if image == 1 then materialization.reserveTarget else materialization.executeTarget
        val c = snapshot.connection; val observation = snapshot.observationScope
        out.writeInt(image)
        var fullBody = 0
        while fullBody < bodies.size && !(bodies(fullBody).kind == image + 1 && bodies(fullBody).association == image) do fullBody += 1
        require(fullBody < bodies.size); out.writeInt(fullBody); references = Math.addExact(references, 1L)
        text(snapshot.schema); uuid(c.serverUuid); text(c.serverVersion); text(c.catalog); text(c.effectiveUser)
        text(c.defaultStorageEngine); out.writeInt(c.innodbFlushLogAtTrxCommit); out.writeLong(c.syncBinlog)
        text(c.transactionIsolation); text(c.readTransactionIsolation); text(c.sqlMode); text(c.timeZone); text(c.tlsVersion); text(c.tlsCipher)
        uuid(observation.bootUuid); out.writeLong(observation.bootGeneration); out.writeLong(observation.connectionGeneration)
        out.writeLong(snapshot.maxAllowedPacketBytes); count.bytes(97L + 32L + 96L); out.writeInt(20)
        var relation = 0
        while relation < 20 do
          val values = snapshot.relations(relation); out.writeInt(relation); out.writeInt(values.rows.size)
          var row = 0
          while row < values.rows.size do
            var candidate = 0
            while candidate < rows.size && (rows(candidate).relation != relation || rows(candidate).original != values.rows(row)) do candidate += 1
            require(candidate < rows.size, "complete image row retained by unique full20 writer")
            out.writeInt(rowInverse(candidate)); references = Math.addExact(references, 1L); row += 1
          relation += 1
        image += 1
      val imagesEnd = count.length
      out.writeInt(2); index = 0
      while index < 2 do
        val effect = materialization.effects(index)
        out.writeInt(index + 1); uuid(if index == 0 then plan.reserveOperationUuid else plan.executeOperationUuid)
        out.writeInt(index); out.writeInt(index + 1)
        // Exact seven canonical body references: scope/context/projectedbefore/projectedtarget/fingerprint/semantic plus effect DATA.
        var kind = 10
        while kind <= 16 do
          var bodyIndex = 0
          while bodyIndex < bodies.size && (bodies(bodyIndex).kind != kind || bodies(bodyIndex).association != index) do bodyIndex += 1
          require(bodyIndex < bodies.size, "every canonical body captured, no certificate-only substitution")
          out.writeInt(bodyIndex); references = Math.addExact(references, 1L); kind += 1
        def raw(values: Vector[MySqlRecoveryOperationRawEnvelope], list: Int): Unit =
          out.writeInt(values.size)
          var row = 0
          while row < values.size do
            var bodyIndex = 0
            while bodyIndex < bodies.size && !(bodies(bodyIndex).kind == 20 && bodies(bodyIndex).encoding == 2 &&
                bodies(bodyIndex).association == index && bodies(bodyIndex).expectedBinary == values(row).canonical.borrowedBinary) do bodyIndex += 1
            require(bodyIndex < bodies.size, "RAW body must belong to its own phase and exact encoding/binary")
            rawReferences.prepare()
            charge(128L, "census ordered RAW reference custody BEFORE allocation")
            rawReferences.publish(new RawReference(index, list, row, values(row).operationUuid, bodyIndex,
              values(row).sha256, values(row).canonical.borrowedBinary))
            uuid(values(row).operationUuid); out.writeInt(bodyIndex)
            references = Math.addExact(references, 1L); hash(values(row).sha256); row += 1
        raw(effect.preimageRawOperationEnvelopes, 0); raw(effect.rawOperationEnvelopes, 1); out.writeInt(1)
        index += 1
      val phasesEnd = count.length
      out.writeInt(bodies.size); index = 0
      while index < bodies.size do
        val body = bodies(index); out.writeInt(body.kind); out.writeInt(body.encoding); seq(body.sequence)
        out.writeLong(body.emitted); out.write(body.digest); out.writeInt(body.association); index += 1
      val descriptorsEnd = count.length
      out.writeInt(1); out.flush()
      if method == Method.RecipeV3 then charge(256L, "recipe measurement holder BEFORE allocation")
      // 包含实际原writer/key/validator的新增charge；不把原count512提前释放当可用信用。
      val liveDelta = Math.addExact(Math.subtractExact(root.accountedMemoryBytes, baselineAccounted), 512L)
      val measuredMetadata = Math.max(metadata, liveDelta)
      root.reserveRetainedMemory(512L, "census immutable facts holder BEFORE allocation")
      val needed = Math.addExact(count.length, Math.addExact(measuredMetadata, Math.addExact(512L, scratchPeak)))
      facts = Facts(chunks.size, atoms.size, rows.size, references, count.length, measuredMetadata, 512L, scratchPeak,
        baselineAccounted, baselineRemaining, root.accountedMemoryBytes, root.availableMemoryBytes,
        root.successfulReserveHighWaterBytes, count.length <= root.profile.maxNativeArtifactBytes(), needed <= baselineRemaining,
        headerEnd, bEnd - headerEnd, aEnd - bEnd, rEnd - aEnd, sourceEnd - rEnd, planEnd - sourceEnd,
        contextEnd - planEnd, imagesEnd - contextEnd, phasesEnd - imagesEnd, descriptorsEnd - phasesEnd, count.length - descriptorsEnd)
      if method == Method.RecipeV3 then
        recipeFacts = RecipeFacts(recipes.size, recipeSegments, recipeLeafBytes, recipeBodyReferences,
          recipeExpandedBytes, capturedMetadata, recipeNodeAndLinkMetadata, recipeLeafSpanMetadata,
          metadata - capturedMetadata - recipeNodeAndLinkMetadata - recipeLeafSpanMetadata, facts)
      stage = "COMPLETE_CENSUS_ONLY_NOT_BUSINESS_QUALIFICATION"
      facts
