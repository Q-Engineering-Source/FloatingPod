package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.io.{DataOutputStream, EOFException, OutputStream}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 完整 canonical DATA 的实际计费来源。构造器不向 caller 开放，不能以裸 binary 假称已计费。 */
private[mysql] final class MySqlFormat3CanonicalPayload private[mysql] (
    val bytes: MySqlFormat3Binary,
    private[mysql] val originalBudget: MySqlFormat3ReadBudget,
    private[mysql] val storageBudget: MySqlFormat3ReadBudget
):
  require(bytes != null && originalBudget != null && storageBudget != null &&
    storageBudget.sharesAllocationLedger(originalBudget), "complete canonical DATA ownership required")
  MySqlFormat3Binary.bindCanonicalStorage(bytes, storageBudget)
  def byteLength: Int = bytes.length

/** Cold DATA codec；任何文件内 connection/boot/lockClause 都是数据，不是当前 SQL 或世界资格。 */
private[mysql] object MySqlFormat3SnapshotCodec:
  private val HeaderStructures = 8192L
  private val ValidationWorkspace = 32768L

  private def requireOwner(profile: SourceLimitProfile, root: MySqlFormat3ReadBudget): Unit =
    require(profile != null && root != null && !root.isClosed && (root.profile eq profile),
      "canonical DATA requires its exact open original profile/budget")
    root.requireFormat3Full20Layout()

  /** 外部 bytes 先在原 ledger reserve，再防御复制；不接管 caller 的 mutable array。 */
  def capture(input: Array[Byte], profile: SourceLimitProfile,
      originalBudget: MySqlFormat3ReadBudget): MySqlFormat3CanonicalPayload =
    requireOwner(profile, originalBudget)
    require(input != null && input.nonEmpty && input.length.toLong <= profile.maxRetainedBytes(),
      "complete bounded canonical input required")
    inDataScope(originalBudget) { child =>
      child.reserveRetainedMemory(128L, "canonical payload holder")
      val binary = MySqlFormat3Binary.copyOf(input, child, "canonical input defensive copy")
      new MySqlFormat3CanonicalPayload(binary, originalBudget, child)
    }

  /** 只对已不可变 snapshot 做 exact count + 一次最终写入；没有任意 callback 或可变两次执行。 */
  def encode(snapshot: MySqlFormat3FullSnapshot,
      originalBudget: MySqlFormat3ReadBudget): MySqlFormat3CanonicalPayload =
    require(snapshot != null, "full20 snapshot required")
    requireOwner(snapshot.limitProfile, originalBudget)
    require(!snapshot.allocationBudget.isClosed && snapshot.allocationBudget.sharesAllocationLedger(originalBudget),
      "snapshot must remain charged to the same original ledger")
    inDataScope(originalBudget) { child =>
      child.reserveTemporaryMemory(512L, "canonical writer/count structures")
      var primary: Throwable = null
      try
        val counter = new MySqlFormat3CountingOutputStream
        val countOutput = new DataOutputStream(counter)
        MySqlFormat3SnapshotCanonical.writeSnapshot(snapshot, countOutput, child)
        countOutput.flush()
        val length = counter.byteLength
        if length <= 0L || length > snapshot.limitProfile.maxRetainedBytes() then
          throw new SQLDataException("full20 canonical output exceeds explicit retained bound")
        val size = MySqlFormat3JvmArrayLimit.requireLength(length, "full20 exact canonical output")
        child.reserveRetainedMemory(Math.addExact(length, 192L), "canonical final binary and holder")
        val sink = new ExactOutput(new Array[Byte](size))
        val output = new DataOutputStream(sink)
        MySqlFormat3SnapshotCanonical.writeSnapshot(snapshot, output, child)
        output.flush()
        new MySqlFormat3CanonicalPayload(MySqlFormat3Binary.takeOwned(sink.finish()), originalBudget, child)
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => child.releaseTemporaryMemory(512L, "canonical writer/count structures")))
    }

  /** 完整解析 + 同 writer 逐字节 roundtrip，不分配第二个完整 canonical buffer。 */
  def decode(payload: MySqlFormat3CanonicalPayload, expectedProfile: SourceLimitProfile,
      originalBudget: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    requireOwner(expectedProfile, originalBudget)
    require(payload != null && (payload.originalBudget eq originalBudget) &&
      !payload.storageBudget.isClosed && payload.storageBudget.sharesAllocationLedger(originalBudget),
      "actual original canonical payload/root identity required")
    if payload.byteLength.toLong > expectedProfile.maxRetainedBytes() then
      throw new SQLDataException("full20 input exceeds explicit retained bound")
    inDataScope(originalBudget) { child =>
      child.reserveRetainedMemory(HeaderStructures, "full20 decoder header/reader structures")
      val attempt = child.beginDecodeBorrow(payload)
      val snapshot = new Reader(payload.bytes, expectedProfile, child, attempt).readSnapshot()
      child.reserveTemporaryMemory(512L, "full20 roundtrip comparison structures")
      var primary: Throwable = null
      try
        val comparison = new CompareOutput(payload.bytes)
        val output = new DataOutputStream(comparison)
        MySqlFormat3SnapshotCanonical.writeSnapshot(snapshot, output, child)
        output.flush()
        comparison.finish()
        attempt.complete()
        snapshot
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => child.releaseTemporaryMemory(512L, "full20 roundtrip comparison structures")))
    }

  /** 失败 body 已退出，局部 array/typed refs 已失去可达引用，才释放这个 DATA child；原 input/root 不动。 */
  private def inDataScope[A](root: MySqlFormat3ReadBudget)(body: MySqlFormat3ReadBudget => A): A =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val child = root.newCanonicalDataScope()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(child) { body(child) }
      catch
        case failure: Throwable =>
          try child.abandonDecodedData()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private final class ExactOutput(private val content: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset == content.length then throw new SQLDataException("full20 canonical exact output overrun")
      content(offset) = value.toByte
      offset += 1
    override def write(bytes: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, bytes.length)
      if length > content.length - offset then throw new SQLDataException("full20 canonical exact output overrun")
      System.arraycopy(bytes, start, content, offset, length)
      offset += length
    def finish(): Array[Byte] =
      if offset != content.length then throw new SQLDataException("full20 canonical exact output underrun")
      content

  private final class CompareOutput(input: MySqlFormat3Binary) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= input.length || input.byteAt(offset) != value.toByte then
        throw new SQLDataException("full20 decoded DATA does not exactly reencode its original bytes")
      offset += 1
    override def write(bytes: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, bytes.length)
      var index = 0
      while index < length do
        write(bytes(start + index).toInt)
        index += 1
    def finish(): Unit =
      if offset != input.length then throw new SQLDataException("full20 canonical roundtrip length differs")

  private final class Reader(input: MySqlFormat3Binary, profile: SourceLimitProfile, budget: MySqlFormat3ReadBudget,
      attempt: MySqlFormat3BorrowAttempt):
    private var offset = 0
    private def remaining: Int = input.length - offset
    private def need(length: Long): Unit =
      if length < 0L then throw new SQLDataException("negative full20 framing length")
      if length > remaining.toLong then throw new EOFException("truncated full20 canonical framing")
    private def octet(): Int =
      need(1L)
      val value = input.byteAt(offset) & 0xff
      offset += 1
      value
    private def int32(): Int =
      need(4L)
      (octet() << 24) | (octet() << 16) | (octet() << 8) | octet()
    private def int64(): Long =
      need(8L)
      var value = 0L
      var index = 0
      while index < 8 do
        value = (value << 8) | octet().toLong
        index += 1
      value
    private def bool(): Boolean = octet() match
      case 0 => false
      case 1 => true
      case _ => throw new SQLDataException("noncanonical full20 boolean")
    private def length(): Int =
      val value = int32()
      if value < 0 then throw new SQLDataException("negative full20 length")
      need(value.toLong)
      value
    private def uuid(): UUID =
      need(16L)
      new UUID(int64(), int64())
    private def exactAscii(value: String): Unit =
      need(value.length.toLong)
      var index = 0
      while index < value.length do
        if octet() != value.charAt(index).toInt then throw new SQLDataException("full20 pinned source identity differs")
        index += 1
    private def exactText(value: String): Unit =
      val size = length()
      if size.toLong != MySqlFormat3Utf8.byteLength(value) then throw new SQLDataException("full20 frozen shape text length differs")
      // Frozen domain/shape text is ASCII; compare in place without allocating caller-declared metadata.
      var index = 0
      while index < value.length do
        if value.charAt(index) > 127 || octet() != value.charAt(index).toInt then
          throw new SQLDataException("full20 frozen domain/shape differs")
        index += 1
    private def text(size: Int): String =
      need(size.toLong)
      MySqlFormat3JvmArrayLimit.requireLength(size.toLong, "full20 text")
      val temporary = Math.addExact(Math.multiplyExact(size.toLong, 3L), 384L)
      budget.reserveRetainedMemory(Math.addExact(Math.multiplyExact(size.toLong, 2L), 64L), "full20 decoded String")
      budget.reserveTemporaryMemory(temporary, "full20 strict UTF8 bytes/chars/decoder")
      var bytes: Array[Byte] = null
      var chars: CharBuffer = null
      var primary: Throwable = null
      try
        bytes = new Array[Byte](size)
        var index = 0
        while index < size do
          bytes(index) = octet().toByte
          index += 1
        val decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
        chars = decoder.decode(ByteBuffer.wrap(bytes))
        chars.toString
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        if bytes != null then java.util.Arrays.fill(bytes, 0.toByte)
        bytes = null
        chars = null
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => budget.releaseTemporaryMemory(temporary, "full20 strict UTF8 bytes/chars/decoder")))
    private def headerText(): String = text(length())
    private def exactProfile(): Unit =
      budget.reserveTemporaryMemory(192L, "full20 expected profile bytes/hash")
      var expected: Array[Byte] = null
      var digest: Array[Byte] = null
      var primary: Throwable = null
      try
        expected = profile.canonicalBytes()
        digest = profile.sha256()
        if int32() != expected.length then throw new SQLDataException("full20 profile length differs")
        need(Math.addExact(expected.length.toLong, digest.length.toLong))
        var index = 0
        while index < expected.length do
          if octet().toByte != expected(index) then throw new SQLDataException("full20 explicit profile bytes differ")
          index += 1
        index = 0
        while index < digest.length do
          if octet().toByte != digest(index) then throw new SQLDataException("full20 explicit profile SHA differs")
          index += 1
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        expected = null
        digest = null
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => budget.releaseTemporaryMemory(192L, "full20 expected profile bytes/hash")))

    def readSnapshot(): MySqlFormat3FullSnapshot =
      exactText(MySqlFormat3SnapshotCanonical.Domain)
      exactAscii(MySqlFormat3RelationShapes.SourceFoundationSha256)
      exactAscii(MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256)
      exactAscii(MySqlFormat3RelationShapes.SourceLockedReadSha256)
      val schema = headerText()
      val server = uuid()
      val connection = MySqlFormat3ConnectionObservation(server, headerText(), headerText(), headerText(), headerText(),
        int32(), int64(), headerText(), headerText(), headerText(), headerText(), headerText(), headerText())
      val scope = MySqlFormat3ObservationScope(uuid(), int64(), int64())
      val packet = int64()
      if packet <= 0L then throw new SQLDataException("full20 observed packet is not positive")
      exactProfile()
      if int32() != MySqlFormat3RelationShapes.ExpectedRelationCount then throw new SQLDataException("full20 relation count differs")
      MySqlFormat3RelationShapes.canonicalOrder.foreach { shape =>
        exactText(shape.table)
        exactText(shape.lockClause)
        if int32() != shape.primaryKey.size then throw new SQLDataException("full20 PK count differs")
        shape.primaryKey.foreach(exactText)
        if int32() != shape.columns.size then throw new SQLDataException("full20 column shape count differs")
        shape.columns.foreach { column =>
          exactText(column.name)
          exactText(column.sqlDeclaration)
          if octet() != column.kind.ordinal || bool() != column.nullable then throw new SQLDataException("full20 column kind/nullability differs")
          val hasMaximum = bool()
          if hasMaximum != column.maximumLength.nonEmpty then throw new SQLDataException("full20 column maximum framing differs")
          if hasMaximum && int32() != column.maximumLength.get then throw new SQLDataException("full20 column maximum differs")
        }
      }
      // packet is recorded DATA, not a newly measured DB value; existing root limit can only become stricter.
      budget.bindCanonicalDataPacketLimit(packet)
      budget.initialize(schema, connection, scope, packet)
      val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
        val count = int32()
        if count < 0 then throw new SQLDataException("negative full20 row count")
        need(Math.multiplyExact(count.toLong, Math.addExact(4L, shape.columns.size.toLong)))
        val rows = Vector.newBuilder[Vector[MySqlFormat3Cell]]
        var row = 0
        while row < count do
          if int32() != shape.columns.size then throw new SQLDataException("full20 row width differs")
          budget.beginRow(shape.columns.size, shape.table, row.toLong)
          val cells = shape.columns.map(column => cell(column, shape.table))
          budget.endRow(shape.table, row.toLong)
          rows += cells
          row += 1
        MySqlFormat3RelationSnapshot(shape.table, rows.result())
      }
      if remaining != 0 then throw new SQLDataException("trailing full20 canonical bytes")
      budget.reserveTemporaryMemory(ValidationWorkspace, "full20 complete shape/PK validation workspace")
      var primary: Throwable = null
      try new MySqlFormat3FullSnapshot(schema, connection, scope, packet, profile, relations, budget)
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => budget.releaseTemporaryMemory(ValidationWorkspace, "full20 complete shape/PK validation workspace")))

    private def cell(column: MySqlFormat3ColumnShape, table: String): MySqlFormat3Cell =
      val context = column.name
      octet() match
        case 0 =>
          if !column.nullable then throw new SQLDataException("unexpected full20 SQL NULL")
          budget.beginNullCell(context)
          MySqlFormat3Cell.SqlNull
        case 1 =>
          if column.kind != MySqlFormat3SqlKind.Text then throw new SQLDataException("full20 text kind differs")
          val size = length()
          budget.beginVariableCell(context)
          budget.addVariablePayload(size.toLong, context)
          val value = text(size)
          if column.maximumLength.exists(_ < value.codePointCount(0, value.length)) then throw new SQLDataException("full20 text exceeds frozen maximum")
          MySqlFormat3Cell.Text(value)
        case 2 =>
          if column.kind != MySqlFormat3SqlKind.Binary then throw new SQLDataException("full20 binary kind differs")
          val size = length()
          if column.maximumLength.exists(_ < size) then throw new SQLDataException("full20 binary exceeds frozen maximum")
          budget.beginVariableCell(context)
          budget.addVariablePayload(size.toLong, context)
          need(size.toLong)
          val value = MySqlFormat3Binary.borrowedRange(input, offset, size, attempt)
          offset = Math.addExact(offset, size)
          MySqlFormat3Cell.Binary(value)
        case 3 =>
          if column.kind != MySqlFormat3SqlKind.Integer then throw new SQLDataException("full20 integer kind differs")
          budget.beginFixedCell(8, if column.sqlDeclaration.startsWith("TINYINT") then 1 else if column.sqlDeclaration.startsWith("INT ") then 4 else 8, context)
          MySqlFormat3Cell.SignedInteger(int64())
        case 4 =>
          if column.kind != MySqlFormat3SqlKind.Floating then throw new SQLDataException("full20 floating kind differs")
          budget.beginFixedCell(8, 64, context)
          MySqlFormat3Cell.Floating(int64())
        case 5 =>
          if column.kind != MySqlFormat3SqlKind.DateTime then throw new SQLDataException("full20 datetime kind differs")
          need(13L)
          budget.beginFixedCell(13, 32, context)
          val value = LocalDateTime.of(int32(), octet(), octet(), octet(), octet(), octet(), int32())
          MySqlFormat3Cell.DateTime(value)
        case _ => throw new SQLDataException("unknown full20 typed cell tag")
