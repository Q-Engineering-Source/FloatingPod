package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.io.{DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.Arrays

/** 不可变、域分离的规范字节值；内容只通过显式有界写入交付。 */
private[mysql] final class MySqlRecoveryCanonicalBytes private (
    private val payload: MySqlFormat3Binary,
    val sha256: Vector[Byte],
    val length: Int,
    val encoding: String
):
  require(payload != null && sha256 != null && sha256.size == 32 && length >= 0 && encoding != null,
    "complete recovery canonical bytes are required")

  private[mysql] def writeTo(output: DataOutputStream): Unit =
    if output == null then throw new IllegalArgumentException("canonical byte output is required")
    payload.writeTo(output)

  /** 在同一进程内复用不可变载荷，避免把已规范化的完整 bytes 再复制一份。 */
  private[mysql] def binaryCell: MySqlFormat3Cell.Binary = MySqlFormat3Cell.Binary(payload)

  /** Census只借原不可变payload；不创建Binary cell包装，不退款原账本。 */
  private[mysql] def borrowedBinary: MySqlFormat3Binary = payload

  private[mysql] def sha256Cell: MySqlFormat3Cell.Binary =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(sha256.toArray))

  private[mysql] def exactlyMatchesCell(value: MySqlFormat3Cell): Boolean = value match
    case MySqlFormat3Cell.Binary(actual) => payload == actual
    case _ => false
  private[mysql] def exactlyEquals(other: MySqlRecoveryCanonicalBytes): Boolean =
    other != null && encoding == other.encoding && length == other.length && sha256 == other.sha256 && payload == other.payload

  /** Boolean-only 完整 framing 比对，不生成第二个 canonical payload。 */
  private[mysql] def matchesWrittenBytes(write: DataOutputStream => Unit): Boolean =
    if write == null then throw new IllegalArgumentException("exact canonical comparison writer required")
    var position = 0L
    var matches = true
    val sink = new OutputStream:
      override def write(value: Int): Unit =
        if position >= length.toLong || payload.byteAt(position.toInt) != value.toByte then matches = false
        position = Math.addExact(position, 1L)
      override def write(values: Array[Byte], offset: Int, count: Int): Unit =
        if values == null || offset < 0 || count < 0 || offset > values.length - count then
          throw new IndexOutOfBoundsException("canonical comparison byte slice is invalid")
        var index = 0
        while index < count do
          write(values(offset + index).toInt)
          index += 1
    val output = new DataOutputStream(sink)
    write(output)
    output.flush()
    matches && position == length.toLong

  private[mysql] def compareContent(other: MySqlRecoveryCanonicalBytes): Int =
    if other == null then throw new IllegalArgumentException("canonical byte comparison value is required")
    payload.compareTo(other.payload)

  private[mysql] def copyBytes(budget: MySqlFormat3ReadBudget, context: String): Array[Byte] =
    if budget == null || context == null then throw new IllegalArgumentException("canonical byte copy requires an explicit budget")
    payload.copyBytes(budget, context)

  override def toString: String = s"MySqlRecoveryCanonicalBytes[$encoding,$length bytes,sha256=${hexSha256}]"
  def hexSha256: String = java.util.HexFormat.of().withUpperCase().formatHex(sha256.toArray)

private[mysql] object MySqlRecoveryCanonicalBytes:
  private val MinimumInitialCapacity = 32
  private val allocationScope = new ThreadLocal[MySqlFormat3ReadBudget]()

  /** 仅真实session issuer/worker materializer调用；无SQL、无等待，不改变canonical framing。 */
  private[mysql] def withAllocationBudget[A](budget: MySqlFormat3ReadBudget)(body: => A): A =
    if budget == null || budget.isClosed then throw new IllegalArgumentException("actual open retained root budget required")
    val previous = allocationScope.get()
    if previous != null && (previous ne budget) && !budget.isCanonicalChildOf(previous) then
      throw new IllegalStateException("canonical allocation cannot switch a retained workflow root")
    allocationScope.set(budget)
    try body
    finally
      if previous == null then allocationScope.remove()
      else allocationScope.set(previous)

  /** 仅内部不逃逸的验证编码使用child；共享原cap，body无canonical/snapshot返回值。
    * 生产调用者的局部比较值在Unit body返回前失去引用；原root的已接受/Unknown refs不释放。
    */
  private[mysql] def withComparisonBudget(root: MySqlFormat3ReadBudget)(body: => Unit): Unit =
    if root == null || root.isClosed || root.isScopedReadBudget then
      throw new IllegalArgumentException("exact open workflow root required for canonical comparison")
    val current = allocationScope.get()
    if current != null && current.isCanonicalComparisonChildOf(root) then body
    else
      // child/Some-parent/closure元数据在实际构造之前计入同root temporary。
      root.reserveTemporaryMemory(256L, "canonical comparison scope metadata")
      var child: MySqlFormat3ReadBudget = null
      var primary: Throwable = null
      try
        child = root.newCanonicalScope()
        withAllocationBudget(child) { body }
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try
          if child != null then child.closeScopedReadBudget()
        catch
          case cleanup: Throwable =>
            if primary == null then
              primary = cleanup
              throw cleanup
            else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)
        finally
          child = null
          try root.releaseTemporaryMemory(256L, "canonical comparison scope metadata release")
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  /** 实际planned scope中的decode/copy同样使用原root；纯DATA消费者才有独立预算。 */
  private[mysql] def allocationBudgetFor(profile: SourceLimitProfile): MySqlFormat3ReadBudget =
    if profile == null then throw new IllegalArgumentException("exact profile required")
    val current = allocationScope.get()
    if current == null then new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    else
      require((current.profile eq profile) && !current.isClosed, "decode/copy differs from actual allocation scope/profile")
      current

  /** 固定小字段decode仍保原字段上限；若处于实际scope，不私建第二cap。 */
  private[mysql] def allocationBudgetOrStandalone(fallback: => MySqlFormat3ReadBudget): MySqlFormat3ReadBudget =
    val current = allocationScope.get()
    if current == null then fallback
    else
      require(!current.isClosed, "actual decode/copy allocation scope closed")
      current

  def encode(encoding: String, profile: SourceLimitProfile)(write: DataOutputStream => Unit): MySqlRecoveryCanonicalBytes =
    if encoding == null || !encoding.matches("[A-Z0-9_./-]{1,64}") then
      throw new IllegalArgumentException("fixed ASCII canonical encoding token required")
    if profile == null then throw new IllegalArgumentException("approved explicit retained/packet profile required")
    val byteLimit = Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes())
    val scoped = allocationScope.get()
    if scoped != null && ((scoped.profile ne profile) || scoped.isClosed) then
      throw new IllegalArgumentException("canonical allocation scope differs from exact session profile")
    val budget = if scoped == null then new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes()) else scoped
    val sink = new BoundedSink(byteLimit, budget, encoding)
    val output = new DataOutputStream(sink)
    var primary: Throwable = null
    var result: MySqlRecoveryCanonicalBytes = null
    try
      write(output)
      output.flush()
      result = sink.finish(encoding)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try output.close()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
      if result == null then sink.releaseTemporary(primary)
    result

  /** 固定 immutable RAW framing；writer 只调用一次，最终数组直接移交，无增长或 finish 副本。 */
  private[mysql] def encodeExact(encoding: String, profile: SourceLimitProfile, exactLength: Long)
      (write: DataOutputStream => Unit): MySqlRecoveryCanonicalBytes =
    if encoding == null || !encoding.matches("[A-Z0-9_./-]{1,64}") || profile == null || write == null then
      throw new IllegalArgumentException("fixed exact canonical encoding/profile/writer required")
    val limit = Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes())
    if exactLength < 0L || exactLength > limit then
      throw new SQLDataException("exact canonical request exceeds explicit retained/SQL-packet profile")
    val size = MySqlFormat3JvmArrayLimit.requireLength(exactLength, "exact recovery canonical payload")
    val budget = allocationBudgetFor(profile)
    // 沿用 full20 fingerprint 的 SHA workspace/result 元数据预算，不是第二个 payload。
    val retained = Math.addExact(exactLength, 640L)
    budget.reserveTemporaryMemory(1024L, "exact canonical digest workspace")
    var retainedCharged = false
    var sink: ExactSink = null
    var primary: Throwable = null
    var transferred = false
    try
      budget.reserveRetainedMemory(retained, "exact immutable canonical payload and digest")
      retainedCharged = true
      sink = new ExactSink(new Array[Byte](size))
      val output = new DataOutputStream(sink)
      write(output)
      output.flush()
      val owned = sink.finish()
      val hash = MessageDigest.getInstance("SHA-256").digest(owned).toVector
      val result = new MySqlRecoveryCanonicalBytes(MySqlFormat3Binary.takeOwned(owned, budget), hash, size, encoding)
      sink.dropReference()
      transferred = true
      result
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      // 先放下数组引用再退账；成功的 immutable payload 留在原 root，不按完成自动释放。
      if sink != null then sink.dropReference()
      sink = null
      try
        if retainedCharged && !transferred then
          budget.releaseRetainedMemory(retained, "failed exact canonical payload release")
      catch
        case cleanup: Throwable =>
          if primary == null then
            primary = cleanup
            throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
      finally
        try budget.releaseTemporaryMemory(1024L, "exact canonical digest workspace release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)

  /** 仅已锁定的原历史O row使用；原固定writeRow仍是完整字节权威。 */
  private[mysql] def encodeHistoricalRaw(encoding: String, profile: SourceLimitProfile, exactLength: Long,
      row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape): MySqlRecoveryCanonicalBytes =
    val budget = allocationBudgetFor(profile)
    require(!budget.isScopedReadBudget && !budget.isClosed && (budget.profile eq profile))
    val size = MySqlFormat3JvmArrayLimit.requireLength(exactLength, "historical RAW complete length")
    require(exactLength <= Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes()))
    var borrowed = 0L; var spans = 0
    var index = 0
    while index < row.size do
      row(index) match
        case MySqlFormat3Cell.Binary(value) if value.length > 0 =>
          MySqlFormat3Binary.requireFlatBorrowSource(value)
          borrowed = Math.addExact(borrowed, value.length.toLong); spans = Math.addExact(spans, 1)
        case _ => ()
      index += 1
    val literals = MySqlFormat3JvmArrayLimit.requireLength(Math.subtractExact(exactLength, borrowed), "historical RAW literals")
    val capacity = Math.addExact(Math.multiplyExact(spans, 2), 1)
    val attempt = budget.beginRawBorrow()
    // attempt512包含固定construction/output holder；数组与canonical另逐项预收。
    attempt.rawConstruction = new RawBorrowConstruction(attempt, literals, capacity, size)
    val construction = attempt.rawConstruction
    var primary: Throwable = null
    var scratch = false
    try
      construction.initialize()
      budget.reserveRetainedMemory(640L, "historical RAW original canonical and digest custody")
      budget.reserveTemporaryMemory(1024L, "historical RAW full digest workspace")
      scratch = true
      val output = new RawBorrowOutput(construction)
      MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, row, shape)
      output.flush()
      val payload = construction.finish()
      val digest = MessageDigest.getInstance("SHA-256")
      index = 0
      while index < payload.length do
        digest.update(payload.byteAt(index)); index += 1
      val result = new MySqlRecoveryCanonicalBytes(payload, digest.digest().toVector, size, encoding)
      construction.dropReferences()
      attempt.rawConstruction = null
      result
    catch
      case cause: Throwable => primary = cause; attempt.recordFailure(cause); throw cause
    finally
      if scratch then
        try budget.releaseTemporaryMemory(1024L, "historical RAW digest workspace dropped")
        catch
          case cleanup: Throwable =>
            attempt.recordFailure(cleanup)
            if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  /** 不从write(Array)猜不可变性；只有Binary.writeTo明确调用borrow。 */
  private[mysql] final class RawBorrowOutput private[MySqlRecoveryCanonicalBytes] (sink: RawBorrowConstruction)
      extends DataOutputStream(sink):
    private[mysql] def borrow(value: MySqlFormat3Binary): Unit = sink.borrow(value)
  private[mysql] final class RawBorrowConstruction private[MySqlRecoveryCanonicalBytes] (
      attempt: MySqlFormat3BorrowAttempt, literalLength: Int, capacity: Int, totalLength: Int) extends OutputStream:
    private var literal: Array[Byte] = null
    private var literalSource: MySqlFormat3Binary = null
    private var sources: Array[MySqlFormat3Binary] = null
    private var offsets: Array[Int] = null
    private var lengths: Array[Int] = null
    private var ends: Array[Int] = null
    private var literalPosition = 0
    private var literalBegin = 0
    private var position = 0
    private var used = 0
    private var finished = false
    def initialize(): Unit =
      val budget = attempt.destination
      budget.reserveRetainedMemory(literalLength.toLong + 128L, "historical RAW literal array and private owned wrapper")
      literal = new Array[Byte](literalLength)
      literalSource = MySqlFormat3Binary.takeOwned(literal, budget)
      budget.reserveRetainedMemory(64L + 8L * capacity, "historical RAW source array BEFORE allocation")
      sources = new Array[MySqlFormat3Binary](capacity)
      budget.reserveRetainedMemory(64L + 4L * capacity, "historical RAW offset array BEFORE allocation")
      offsets = new Array[Int](capacity)
      budget.reserveRetainedMemory(64L + 4L * capacity, "historical RAW length array BEFORE allocation")
      lengths = new Array[Int](capacity)
      budget.reserveRetainedMemory(64L + 4L * capacity, "historical RAW end array BEFORE allocation")
      ends = new Array[Int](capacity)
    override def write(value: Int): Unit =
      require(!finished && literal != null && literalPosition < literal.length && position < totalLength,
        "historical RAW literal/count overrun")
      literal(literalPosition) = value.toByte
      literalPosition += 1; position += 1
    override def write(values: Array[Byte], offset: Int, count: Int): Unit =
      java.util.Objects.checkFromIndexSize(offset, count, values.length)
      var index = 0
      while index < count do
        write(values(offset + index).toInt); index += 1
    private def append(value: MySqlFormat3Binary, offset: Int, count: Int): Unit =
      require(used < capacity && count > 0 && offset >= 0 && offset <= value.length - count)
      val previous = if used == 0 then 0 else ends(used - 1)
      val end = Math.addExact(previous, count)
      require(end <= totalLength)
      sources(used) = value; offsets(used) = offset; lengths(used) = count; ends(used) = end
      used += 1
    private def flushLiterals(): Unit =
      if literalPosition > literalBegin then
        append(literalSource, literalBegin, literalPosition - literalBegin)
        literalBegin = literalPosition
    def borrow(value: MySqlFormat3Binary): Unit =
      require(!finished && value != null && value.length <= totalLength - position)
      if value.length > 0 then
        MySqlFormat3Binary.requireFlatBorrowSource(value)
        flushLiterals()
        attempt.destination.registerBorrow(value, attempt)
        append(value, 0, value.length)
        position += value.length
    def finish(): MySqlFormat3Binary =
      require(!finished && position == totalLength && literalPosition == literalLength,
        "historical RAW fixed writer must fill every literal and borrowed byte")
      flushLiterals()
      attempt.destination.reserveRetainedMemory(192L, "historical RAW segmented holder BEFORE allocation")
      val result = MySqlFormat3Binary.segmented(sources, offsets, lengths, ends, used, totalLength, attempt)
      attempt.complete(); finished = true
      result
    def dropReferences(): Unit =
      literal = null; literalSource = null; sources = null; offsets = null; lengths = null; ends = null

  private final class ExactSink(private var bytes: Array[Byte]) extends OutputStream:
    private var position = 0
    private var finished = false
    override def write(value: Int): Unit =
      requireSpace(1)
      bytes(position) = value.toByte
      position += 1
    override def write(values: Array[Byte], offset: Int, count: Int): Unit =
      if values == null || offset < 0 || count < 0 || offset > values.length - count then
        throw new IndexOutOfBoundsException("exact canonical byte slice is invalid")
      requireSpace(count)
      System.arraycopy(values, offset, bytes, position, count)
      position += count
    private def requireSpace(count: Int): Unit =
      if finished || bytes == null || count < 0 || count > bytes.length - position then
        throw new SQLDataException("exact canonical writer exceeds its computed framing length")
    def finish(): Array[Byte] =
      if finished || bytes == null || position != bytes.length then
        throw new SQLDataException("exact canonical writer did not fill its computed framing length")
      finished = true
      bytes
    def dropReference(): Unit =
      bytes = null
      finished = true

  def writeToken(output: DataOutputStream, token: String): Unit =
    if output == null || token == null || !token.matches("[A-Z0-9_./-]{1,96}") then
      throw new SQLDataException("canonical field token is not fixed printable ASCII")
    output.writeInt(token.length)
    var index = 0
    while index < token.length do
      output.writeByte(token.charAt(index).toInt)
      index += 1

  def writeText(output: DataOutputStream, value: String): Unit =
    if output == null then throw new SQLDataException("canonical text output is required")
    MySqlFormat3Utf8.write(value, output)

  def writeUuid(output: DataOutputStream, value: java.util.UUID): Unit =
    if output == null || value == null then throw new SQLDataException("canonical UUID is required")
    output.writeLong(value.getMostSignificantBits)
    output.writeLong(value.getLeastSignificantBits)

  def writeOptionUuid(output: DataOutputStream, value: Option[java.util.UUID]): Unit =
    if output == null || value == null || value.exists(_ == null) then throw new SQLDataException("canonical UUID option is malformed")
    value match
      case None => output.writeByte(0)
      case Some(uuid) => output.writeByte(1); writeUuid(output, uuid)

  private final class BoundedSink(limit: Long, budget: MySqlFormat3ReadBudget, label: String) extends OutputStream:
    if limit <= 0L || limit > MySqlFormat3JvmArrayLimit.MaximumLength || budget == null || label == null then
      throw new SQLDataException("canonical byte limits exceed explicit supported bounds")

    private var buffer = Array.emptyByteArray
    private var length = 0
    private var capacityReservation = 0L
    private var outputReservation = 0L
    private var finished = false

    override def write(value: Int): Unit =
      ensureWritable(1)
      buffer(length) = value.toByte
      length += 1

    override def write(values: Array[Byte], offset: Int, count: Int): Unit =
      if values == null || offset < 0 || count < 0 || offset > values.length - count then
        throw new IndexOutOfBoundsException("canonical byte slice is invalid")
      ensureWritable(count)
      System.arraycopy(values, offset, buffer, length, count)
      length += count

    private def ensureWritable(count: Int): Unit =
      if finished then throw new SQLDataException("canonical byte output is already finalized")
      if count < 0 then throw new SQLDataException("canonical byte count is invalid")
      val nextLength = Math.addExact(length.toLong, count.toLong)
      if nextLength > limit || nextLength > Int.MaxValue.toLong then
        throw new SQLDataException("canonical request exceeds explicit retained/SQL-packet profile")
      if nextLength > buffer.length.toLong then
        val doubled = if buffer.length == 0 then MinimumInitialCapacity.toLong else Math.multiplyExact(buffer.length.toLong, 2L)
        val nextCapacity = Math.min(limit, Math.max(nextLength, doubled))
        val nextInt = MySqlFormat3JvmArrayLimit.requireLength(nextCapacity, "recovery canonical output buffer")
        budget.reserveTemporaryMemory(nextCapacity, s"$label canonical output capacity")
        var allocated = false
        try
          val next = Arrays.copyOf(buffer, nextInt)
          allocated = true
          val oldReservation = capacityReservation
          buffer = next
          capacityReservation = nextCapacity
          if oldReservation > 0L then budget.releaseTemporaryMemory(oldReservation, s"$label old canonical output capacity")
        catch
          case failure: Throwable =>
            if !allocated then
              try budget.releaseTemporaryMemory(nextCapacity, s"$label failed canonical output capacity")
              catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
            throw failure

    def finish(encoding: String): MySqlRecoveryCanonicalBytes =
      if finished then throw new SQLDataException("canonical output can only be finalized once")
      finished = true
      budget.reserveRetainedMemory(length.toLong, s"$label immutable canonical payload")
      outputReservation = length.toLong
      var output: Array[Byte] = null
      var primary: Throwable = null
      try
        output = Arrays.copyOf(buffer, length)
        val hash = MessageDigest.getInstance("SHA-256").digest(output).toVector
        if hash.size != 32 then throw new IllegalStateException("SHA-256 returned an invalid recovery payload digest")
        val value = MySqlFormat3Binary.takeOwned(output, budget)
        output = null
        releaseCapacity(null)
        new MySqlRecoveryCanonicalBytes(value, hash, length, encoding)
      catch
        case failure: Throwable =>
          primary = failure
          if outputReservation > 0L then
            try budget.releaseRetainedMemory(outputReservation, s"$label failed immutable payload")
            catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
            outputReservation = 0L
          throw failure
      finally
        if output != null then output = null
        if primary != null then releaseCapacity(primary)

    def releaseTemporary(primary: Throwable): Unit =
      finished = true
      releaseCapacity(primary)

    private def releaseCapacity(primary: Throwable): Unit =
      if capacityReservation > 0L then
        val amount = capacityReservation
        buffer = Array.emptyByteArray
        capacityReservation = 0L
        try budget.releaseTemporaryMemory(amount, s"$label canonical output capacity release")
        catch
          case cleanup: Throwable =>
            if primary != null then
              if cleanup ne primary then primary.addSuppressed(cleanup)
            else throw cleanup
