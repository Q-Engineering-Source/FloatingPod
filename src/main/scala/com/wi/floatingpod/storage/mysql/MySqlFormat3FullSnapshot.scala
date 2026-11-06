package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.lang.{Byte as JavaByte}

import java.io.{BufferedOutputStream, DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.{Arrays, UUID}
import java.util.HexFormat
import java.sql.SQLDataException

/** 完整 format3 关系行中的一个有类型单元；SQL NULL 与空字符串/空字节严格区分。 */
private[mysql] enum MySqlFormat3Cell:
  case SqlNull
  case Text(value: String)
  case Binary(value: MySqlFormat3Binary)
  case SignedInteger(value: Long)
  case Floating(rawBits: Long)
  case DateTime(value: LocalDateTime)

/** 封闭不可变表示；完整byte语义相同，借用值另保实际owner存活责任。 */
private[mysql] sealed abstract class MySqlFormat3Binary:
  def length: Int
  private[mysql] def byteAt(index: Int): Byte
  private[mysql] def storageBudget: MySqlFormat3ReadBudget
  protected def writeContent(output: DataOutputStream): Unit
  private[mysql] final def writeTo(output: DataOutputStream): Unit = output match
    case raw: MySqlRecoveryCanonicalBytes.RawBorrowOutput => raw.borrow(this)
    case _ => writeContent(output)
  private[mysql] final def equalsBytes(other: Array[Byte]): Boolean =
    if other == null || length != other.length then false else
      var index = 0
      while index < length do
        if byteAt(index) != other(index) then return false
        index += 1
      true
  private[mysql] final def equalsVector(other: Vector[Byte]): Boolean =
    if other == null || length != other.length then false else
      var index = 0
      while index < length do
        if byteAt(index) != other(index) then return false
        index += 1
      true
  private[mysql] final def equalsUuid(other: UUID): Boolean =
    if other == null || length != 16 then false else
      var index = 0
      while index < 8 do
        if byteAt(index) != (other.getMostSignificantBits >>> (56 - index * 8)).toByte then return false
        if byteAt(index + 8) != (other.getLeastSignificantBits >>> (56 - index * 8)).toByte then return false
        index += 1
      true
  private[mysql] final def compareTo(other: MySqlFormat3Binary): Int =
    require(other != null, "binary comparison value is required")
    var index = 0
    val common = Math.min(length, other.length)
    while index < common do
      val compared = Integer.compare(byteAt(index).toInt & 255, other.byteAt(index).toInt & 255)
      if compared != 0 then return compared
      index += 1
    Integer.compare(length, other.length)
  private[mysql] final def copyBytes(budget: MySqlFormat3ReadBudget, context: String): Array[Byte] =
    require(budget != null && context != null, "binary copy requires an explicit allocation budget")
    val size = MySqlFormat3JvmArrayLimit.requireLength(length.toLong, context)
    val reservation = Math.addExact(size.toLong, 64L)
    budget.reserveRetainedMemory(reservation, context)
    try
      val result = new Array[Byte](size)
      var index = 0
      while index < size do
        result(index) = byteAt(index); index += 1
      result
    catch
      case failure: Throwable =>
        try budget.releaseRetainedMemory(reservation, s"$context failed copy cleanup")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(failure, cleanup)
        throw failure
  override final def equals(other: Any): Boolean = other match
    case value: MySqlFormat3Binary => length == value.length && compareTo(value) == 0
    case _ => false
  override final def hashCode(): Int =
    var result = 1
    var index = 0
    while index < length do
      result = 31 * result + byteAt(index).toInt; index += 1
    result
  override final def toString: String = s"MySqlFormat3Binary[$length bytes]"

private[mysql] object MySqlFormat3Binary:
  private final class Owned(val content: Array[Byte], private var owner: MySqlFormat3ReadBudget) extends MySqlFormat3Binary:
    def length: Int = content.length
    def byteAt(index: Int): Byte = content(index)
    def storageBudget: MySqlFormat3ReadBudget = owner
    def bind(budget: MySqlFormat3ReadBudget): Unit =
      require(budget != null && !budget.isClosed && (owner == null || (owner eq budget)),
        "canonical payload cannot overwrite an immutable backing storage owner")
      owner = budget
    protected def writeContent(output: DataOutputStream): Unit = output.write(content)
  private final class Range(val backing: Owned, val offset: Int, val length: Int,
      val edge: MySqlFormat3BorrowEdge) extends MySqlFormat3Binary:
    def storageBudget: MySqlFormat3ReadBudget = backing.storageBudget
    def byteAt(index: Int): Byte =
      edge.requireLive(); java.util.Objects.checkIndex(index, length)
      backing.byteAt(Math.addExact(offset, index))
    protected def writeContent(output: DataOutputStream): Unit =
      edge.requireLive(); output.write(backing.content, offset, length)
  private final class Segmented(val sources: Array[MySqlFormat3Binary], val offsets: Array[Int],
      val lengths: Array[Int], val ends: Array[Int], val used: Int, val length: Int,
      val attempt: MySqlFormat3BorrowAttempt) extends MySqlFormat3Binary:
    def storageBudget: MySqlFormat3ReadBudget = attempt.destination
    private[MySqlFormat3Binary] def live(): Unit =
      require(attempt.completed && !attempt.ended && attempt.failure == null && !attempt.destination.isClosed,
        "segmented RAW outlived its actual completed borrow attempt")
    def byteAt(index: Int): Byte =
      live(); java.util.Objects.checkIndex(index, length)
      var low = 0; var high = used - 1
      while low < high do
        val middle = (low + high) >>> 1
        if index < ends(middle) then high = middle else low = middle + 1
      val begin = if low == 0 then 0 else ends(low - 1)
      sources(low).byteAt(Math.addExact(offsets(low), index - begin))
    protected def writeContent(output: DataOutputStream): Unit =
      live()
      var segment = 0
      while segment < used do
        val value = sources(segment)
        var index = 0
        while index < lengths(segment) do
          output.writeByte(value.byteAt(offsets(segment) + index).toInt); index += 1
        segment += 1
  /** 每个顺序range独有192B状态；每次seekStep/advance由真实调用者在动作前计量。 */
  private[mysql] final class Cursor:
    private var original: MySqlFormat3Binary = null
    private var segmented: Segmented = null
    private var source: MySqlFormat3Binary = null
    private var position = 0
    private var remaining = 0
    private var local = 0
    private var segment = 0
    private var segmentEnd = 0
    private var low = 0
    private var high = 0
    private var seeking = false
    private def requireLive(): Unit = original match
      case range: Range => range.edge.requireLive()
      case value: Segmented => value.live()
      case _: Owned => ()
      case _ => throw new IllegalStateException("binary cursor has no original range")
    def begin(value: MySqlFormat3Binary, offset: Int, count: Int): Unit =
      require(value != null && offset >= 0 && count >= 0 && offset <= value.length - count)
      original = value; segmented = null; source = null; seeking = false
      position = offset; remaining = count; local = offset; segment = 0; segmentEnd = value.length
      requireLive()
      value match
        case parts: Segmented =>
          segmented = parts
          if count > 0 then
            require(parts.used > 0)
            low = 0; high = parts.used - 1; seeking = true
        case _ => source = value
    def seekPending: Boolean = seeking
    def seekStep(): Unit =
      requireLive(); require(seeking && remaining > 0)
      if low == high then
        segment = low; selectSegment(); seeking = false
      else
        val middle = low + ((high - low) >>> 1)
        if position < segmented.ends(middle) then high = middle else low = middle + 1
    private def selectSegment(): Unit =
      source = segmented.sources(segment)
      val start = if segment == 0 then 0 else segmented.ends(segment - 1)
      local = Math.addExact(segmented.offsets(segment), position - start)
      segmentEnd = segmented.ends(segment)
      require(position >= start && position < segmentEnd && !source.isInstanceOf[Segmented])
    def needsAdvance: Boolean = !seeking && remaining > 0 && segmented != null && position == segmentEnd
    def advance(): Unit =
      requireLive(); require(needsAdvance)
      segment = Math.addExact(segment, 1)
      require(segment < segmented.used)
      selectSegment()
    def next(): Byte =
      requireLive(); require(!seeking && !needsAdvance && remaining > 0)
      val value = source.byteAt(local)
      local += 1; position += 1; remaining -= 1
      value

  private[mysql] def bindCanonicalStorage(value: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget): Unit = value match
    case owned: Owned => owned.bind(budget)
    case _ => require(value.storageBudget eq budget, "canonical view storage owner cannot change")
  private[mysql] def borrowedRange(value: MySqlFormat3Binary, offset: Int, length: Int,
      attempt: MySqlFormat3BorrowAttempt): MySqlFormat3Binary =
    require(value != null && offset >= 0 && length >= 0 && offset <= value.length - length)
    val backing = value match
      case owned: Owned => owned
      case range: Range => range.edge.requireLive(); range.backing
      case _ => throw new IllegalArgumentException("nested segmented canonical source is not a fixed owned range")
    val absolute = value match
      case range: Range => Math.addExact(range.offset, offset)
      case _ => offset
    attempt.destination.reserveRetainedMemory(96L, "immutable range value BEFORE allocation")
    val edge = attempt.destination.registerBorrow(backing, attempt)
    new Range(backing, absolute, length, edge)
  private[mysql] def requireFlatBorrowSource(value: MySqlFormat3Binary): Unit = value match
    case _: Owned => require(value.storageBudget != null, "historical RAW needs an actual immutable storage owner")
    case range: Range => range.edge.requireLive()
    case _ => throw new IllegalArgumentException("historical RAW cannot borrow a nested segmented graph")
  private[mysql] def segmented(sources: Array[MySqlFormat3Binary], offsets: Array[Int],
      lengths: Array[Int], ends: Array[Int], used: Int, length: Int,
      attempt: MySqlFormat3BorrowAttempt): MySqlFormat3Binary =
    require(used >= 0 && used <= sources.length && offsets.length == sources.length &&
      lengths.length == sources.length && ends.length == sources.length)
    var previous = 0
    var index = 0
    while index < used do
      require(sources(index) != null && offsets(index) >= 0 && lengths(index) > 0 &&
        offsets(index) <= sources(index).length - lengths(index))
      require(ends(index) == Math.addExact(previous, lengths(index)))
      previous = ends(index); index += 1
    require(previous == length)
    new Segmented(sources, offsets, lengths, ends, used, length, attempt)
  private[mysql] def copyBytes(input: Array[Byte], budget: MySqlFormat3ReadBudget, context: String): Array[Byte] =
    require(input != null && budget != null && context != null, "binary copy requires bytes and an explicit allocation budget")
    MySqlFormat3JvmArrayLimit.requireLength(input.length.toLong, context)
    val reservation = Math.addExact(input.length.toLong, 64L)
    budget.reserveRetainedMemory(reservation, context)
    try input.clone()
    catch
      case failure: Throwable =>
        try budget.releaseRetainedMemory(reservation, s"$context failed copy cleanup")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(failure, cleanup)
        throw failure
  def copyOf(input: Array[Byte], budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Binary =
    val copied = copyBytes(input, budget, context)
    try new Owned(copied, budget)
    catch
      case failure: Throwable =>
        try budget.releaseRetainedMemory(input.length.toLong + 64L, s"$context failed immutable holder")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(failure, cleanup)
        throw failure
  /** 既有无owner转交不获得借出权限；真实JDBC/canonical固定入口另绑已计费storage。 */
  private[mysql] def takeOwned(input: Array[Byte]): MySqlFormat3Binary = takeOwned(input, null)
  private[mysql] def takeOwned(input: Array[Byte], budget: MySqlFormat3ReadBudget): MySqlFormat3Binary =
    require(input != null && (budget == null || !budget.isClosed), "binary cell bytes and live storage required")
    MySqlFormat3JvmArrayLimit.requireLength(input.length.toLong, "format3 binary cell")
    new Owned(input, budget)

private[mysql] object MySqlFormat3JvmArrayLimit:
  /** 使用保守的 Java 数组长度上限，不依赖截断或虚拟机特有的超大数组行为。 */
  val MaximumLength: Long = Int.MaxValue.toLong - 8L
  def requireLength(length: Long, context: String): Int =
    if context == null || length < 0L || length > MaximumLength then
      throw new SQLDataException(s"$context exceeds the explicit JVM array representation limit")
    length.toInt

private[mysql] final case class MySqlFormat3RelationSnapshot(
    table: String,
    rows: Vector[Vector[MySqlFormat3Cell]]
)

/** 连接事实仅作为观测上下文保存，不提升为 Ready 或世界保管证明。 */
private[mysql] final case class MySqlFormat3ConnectionObservation(
    serverUuid: UUID,
    serverVersion: String,
    catalog: String,
    effectiveUser: String,
    defaultStorageEngine: String,
    innodbFlushLogAtTrxCommit: Int,
    syncBinlog: Long,
    transactionIsolation: String,
    readTransactionIsolation: String,
    sqlMode: String,
    timeZone: String,
    tlsVersion: String,
    tlsCipher: String
):
  require(serverUuid != null, "observed MySQL server UUID is required")
  require(serverVersion != null && serverVersion.nonEmpty, "observed MySQL server version is required")
  require(catalog != null && catalog.nonEmpty, "observed MySQL catalog is required")
  require(readTransactionIsolation == "REPEATABLE-READ", "format3 snapshot transaction must use REPEATABLE READ")

  override def toString: String = "MySqlFormat3ConnectionObservation[redacted]"
private[mysql] object MySqlFormat3ConnectionObservation:
  def from(identity: MySqlConnectionIdentity, readTransactionIsolation: String): MySqlFormat3ConnectionObservation =
    require(identity != null, "verified MySQL identity is required")
    require(readTransactionIsolation != null, "snapshot transaction isolation is required")
    MySqlFormat3ConnectionObservation(identity.serverUuid, identity.serverVersion, identity.catalog,
      identity.effectiveUser, identity.defaultStorageEngine, identity.innodbFlushLogAtTrxCommit,
      identity.syncBinlog, identity.transactionIsolation, readTransactionIsolation, identity.sqlMode, identity.timeZone,
      identity.tlsVersion, identity.tlsCipher)

private[mysql] final case class MySqlFormat3ObservationScope(
    bootUuid: UUID,
    bootGeneration: Long,
    connectionGeneration: Long
):
  require(bootUuid != null, "boot UUID is required")
  require(bootGeneration > 0L && connectionGeneration > 0L, "boot and connection generations must be positive")

/**
 * 不可变的二十关系数据库完整观测，保留全部有序行和有类型 SQL 值，包括历史项与孤儿项；
 * 本值不认证 DDL、世界保管、世界覆盖范围或 Ready 状态。
 */
private[mysql] final class MySqlFormat3FullSnapshot private[mysql] (
    val schema: String,
    val connection: MySqlFormat3ConnectionObservation,
    val observationScope: MySqlFormat3ObservationScope,
    val maxAllowedPacketBytes: Long,
    val limitProfile: SourceLimitProfile,
    val relations: Vector[MySqlFormat3RelationSnapshot],
    private[mysql] val allocationBudget: MySqlFormat3ReadBudget
):
  require(schema != null && schema.matches("[a-z][a-z0-9_]{0,63}"), "format3 schema identifier is unsafe")
  require(connection != null && observationScope != null && limitProfile != null && allocationBudget != null, "format3 snapshot context is required")
  allocationBudget.requireFormat3Full20Layout()
  require(maxAllowedPacketBytes > 0L, "observed MySQL max_allowed_packet must be positive")
  require(relations != null, "complete format3 relations are required")
  MySqlFormat3SnapshotValidation.validateRelations(relations)

  private lazy val fingerprint = MySqlFormat3SnapshotCanonical.fingerprint(this, allocationBudget)
  def canonicalSha256: Vector[Byte] = fingerprint.sha256
  def canonicalSha256Hex: String = fingerprint.sha256Hex
  def canonicalByteLength: Long = fingerprint.byteLength
  def relation(table: String): MySqlFormat3RelationSnapshot =
    if table == null then throw new IllegalArgumentException("table name is required")
    relations.find(_.table == table).getOrElse(throw new IllegalArgumentException("table is outside the captured format3 relations"))

  /** 比较全部值和观测身份；摘要相同本身不能证明内容相等。 */
  def exactlyEquals(other: MySqlFormat3FullSnapshot): Boolean =
    other != null && schema == other.schema && connection == other.connection && observationScope == other.observationScope &&
      maxAllowedPacketBytes == other.maxAllowedPacketBytes && limitProfile == other.limitProfile && relations == other.relations &&
      canonicalSha256 == other.canonicalSha256

  override def toString: String = s"MySqlFormat3FullSnapshot[relations=${relations.size}, rows=${rowCount}, sha256=$canonicalSha256Hex]"
  def rowCount: Long = relations.foldLeft(0L)((count, relation) => Math.addExact(count, relation.rows.size.toLong))

private[mysql] object MySqlFormat3SnapshotValidation:
  def validateRelations(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    validateAgainst(relations, MySqlFormat3RelationShapes.canonicalOrder, "format3")

  /** ScriptPublication1 使用独立封闭的 24 表布局，不扩展 full20 入口。 */
  private[mysql] def validateScriptPublication1Relations(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    validateAgainst(relations, MySqlScriptSchemaInputs.canonicalOrder, "ScriptPublication1")

  private[mysql] def validateScriptPublication1DeploymentReceipt1Relations(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    validateAgainst(relations, MySqlDeploymentReceiptRelationShapes.canonicalOrder, "ScriptPublication1DeploymentReceipt1")

  private[mysql] def validateDbFirstRelations(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    validateAgainst(relations, MySqlDbFirstRelationShapes.canonicalOrder, MySqlDbFirstSchemaDefinition.Profile)

  private def validateAgainst(relations: Vector[MySqlFormat3RelationSnapshot],
      shapes: Vector[MySqlFormat3RelationShape], family: String): Unit =
    if relations == null then invalid("relation data is missing", family)
    if relations.size != shapes.size then invalid(s"complete $family relation count", family)
    if relations.map(_.table) != shapes.map(_.table) then invalid(s"complete $family canonical relation order", family)
    relations.zip(shapes).foreach { (relation, shape) =>
      if relation == null || relation.rows == null || relation.rows.exists(_ == null) then invalid(s"${shape.table} rows", family)
      if relation.table != shape.table then invalid(s"${shape.table} identity", family)
      relation.rows.foreach { row =>
        if row == null || row.size != shape.columns.size || row.exists(_ == null) then invalid(s"${shape.table} complete row width", family)
        row.zip(shape.columns).foreach { (cell, column) => validateCell(cell, column, shape.table, family) }
      }
      validateSourceForestUuidIndexPair(relation, shape, family)
      validatePrimaryKeyOrder(relation, shape, family)
    }

  private def validateSourceForestUuidIndexPair(relation: MySqlFormat3RelationSnapshot,
      shape: MySqlFormat3RelationShape, family: String): Unit =
    // Reader 只保留 SQL 可见的成对/定长不变量；索引语义由受控 source-forest producer 负责。
    if shape.table == "physical_source_forest" then
      val payloadIndex = shape.columns.indexWhere(_.name == "fp_pod_uuid_index_bytes")
      val digestIndex = shape.columns.indexWhere(_.name == "fp_pod_uuid_index_sha256")
      if payloadIndex < 0 || digestIndex < 0 then invalid("physical_source_forest UUID-index shape", family)
      relation.rows.foreach { row =>
        (row(payloadIndex), row(digestIndex)) match
          case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => ()
          case (MySqlFormat3Cell.Binary(_), MySqlFormat3Cell.Binary(digest)) if digest.length == 32 => ()
          case _ => invalid("physical_source_forest UUID-index payload and 32-byte digest must be SQL NULL together or present together", family)
      }

  private def validatePrimaryKeyOrder(relation: MySqlFormat3RelationSnapshot,
      shape: MySqlFormat3RelationShape, family: String): Unit =
    val indexes = shape.primaryKey.map { key =>
      val index = shape.columns.indexWhere(_.name == key)
      if index < 0 then invalid(s"${shape.table} primary key shape", family)
      val column = shape.columns(index)
      if column.nullable then invalid(s"${shape.table} nullable primary key shape", family)
      if column.kind == MySqlFormat3SqlKind.Text && !column.sqlDeclaration.contains("COLLATE utf8mb4_0900_bin") then
        invalid(s"${shape.table} non-binary text primary key collation", family)
      index
    }
    var rowIndex = 1
    while rowIndex < relation.rows.size do
      val order = comparePrimaryKey(relation.rows(rowIndex - 1), relation.rows(rowIndex), indexes, shape, family)
      if order >= 0 then invalid(s"${shape.table} rows are not strictly ordered by the frozen primary key", family)
      rowIndex += 1

  private def comparePrimaryKey(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell],
      indexes: Vector[Int], shape: MySqlFormat3RelationShape, family: String): Int =
    indexes.iterator.map { index =>
      comparePrimaryKeyCell(left(index), right(index), shape.table, shape.columns(index).name, family)
    }.find(_ != 0).getOrElse(0)

  private def comparePrimaryKeyCell(left: MySqlFormat3Cell, right: MySqlFormat3Cell,
      table: String, column: String, family: String): Int =
    (left, right) match
      case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
      case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) => compareUtf8CodePoints(a, b)
      case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
      case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
      case _ => invalid(s"$table.$column primary key has an unsupported or null value", family)

  private[mysql] def compareUnsigned(left: Array[Byte], right: Array[Byte]): Int =
    var index = 0
    val commonLength = Math.min(left.length, right.length)
    while index < commonLength do
      val compared = Integer.compare(JavaByte.toUnsignedInt(left(index)), JavaByte.toUnsignedInt(right(index)))
      if compared != 0 then return compared
      index += 1
    Integer.compare(left.length, right.length)

  private def compareUtf8CodePoints(left: String, right: String): Int =
    var leftOffset = 0
    var rightOffset = 0
    while leftOffset < left.length && rightOffset < right.length do
      val compared = Integer.compare(left.codePointAt(leftOffset), right.codePointAt(rightOffset))
      if compared != 0 then return compared
      leftOffset += Character.charCount(left.codePointAt(leftOffset))
      rightOffset += Character.charCount(right.codePointAt(rightOffset))
    Integer.compare(left.length - leftOffset, right.length - rightOffset)

  private[mysql] def validateCell(cell: MySqlFormat3Cell, column: MySqlFormat3ColumnShape, table: String): Unit =
    validateCell(cell, column, table, "format3")

  private def validateCell(cell: MySqlFormat3Cell, column: MySqlFormat3ColumnShape,
      table: String, family: String): Unit =
    cell match
      case MySqlFormat3Cell.SqlNull => if !column.nullable then invalid(s"$table.${column.name} unexpected SQL NULL", family)
      case MySqlFormat3Cell.Text(value) =>
        if column.kind != MySqlFormat3SqlKind.Text || value == null then invalid(s"$table.${column.name} text type", family)
        val length = MySqlFormat3Utf8.byteLength(value)
        if length > Int.MaxValue.toLong || column.maximumLength.exists(_ < value.codePointCount(0, value.length)) then
          invalid(s"$table.${column.name} text length", family)
      case MySqlFormat3Cell.Binary(value) =>
        if column.kind != MySqlFormat3SqlKind.Binary || value == null then invalid(s"$table.${column.name} binary type", family)
        if column.maximumLength.exists(_ < value.length) then invalid(s"$table.${column.name} binary length", family)
      case MySqlFormat3Cell.SignedInteger(value) =>
        if column.kind != MySqlFormat3SqlKind.Integer then invalid(s"$table.${column.name} integer type", family)
        val owner = if family == "format3" then "MySQL format3" else s"MySQL $family"
        column.requireSignedInteger(value, s"$owner snapshot violates $table.${column.name}")
      case MySqlFormat3Cell.Floating(_) => if column.kind != MySqlFormat3SqlKind.Floating then invalid(s"$table.${column.name} floating type", family)
      case MySqlFormat3Cell.DateTime(value) =>
        if column.kind != MySqlFormat3SqlKind.DateTime || value == null || value.getNano % 1000 != 0 then invalid(s"$table.${column.name} DATETIME(6) type/precision", family)

  private def invalid(reason: String, family: String): Nothing =
    val owner = if family == "format3" then "MySQL format3" else s"MySQL $family"
    throw new SQLDataException(s"$owner snapshot violates $reason")

private[mysql] object MySqlFormat3Utf8:
  def byteLength(value: String): Long =
    if value == null then throw new SQLDataException("MySQL format3 text value is null")
    var index = 0
    var length = 0L
    while index < value.length do
      val current = value.charAt(index)
      if current <= 0x7f then length = Math.addExact(length, 1L)
      else if current <= 0x7ff then length = Math.addExact(length, 2L)
      else if Character.isHighSurrogate(current) then
        if index + 1 >= value.length || !Character.isLowSurrogate(value.charAt(index + 1)) then
          throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
        length = Math.addExact(length, 4L)
        index += 1
      else if Character.isLowSurrogate(current) then
        throw new SQLDataException("MySQL format3 text contains an unpaired UTF-16 surrogate")
      else length = Math.addExact(length, 3L)
      index += 1
    length

  def write(value: String, output: DataOutputStream): Long =
    val length = byteLength(value)
    if length > Int.MaxValue.toLong then throw new SQLDataException("MySQL format3 text exceeds canonical int32 length")
    output.writeInt(length.toInt)
    var index = 0
    while index < value.length do
      val current = value.charAt(index)
      val codePoint =
        if Character.isHighSurrogate(current) then
          val point = Character.toCodePoint(current, value.charAt(index + 1))
          index += 1
          point
        else current.toInt
      if codePoint <= 0x7f then output.writeByte(codePoint)
      else if codePoint <= 0x7ff then
        output.writeByte(0xc0 | (codePoint >> 6))
        output.writeByte(0x80 | (codePoint & 0x3f))
      else if codePoint <= 0xffff then
        output.writeByte(0xe0 | (codePoint >> 12))
        output.writeByte(0x80 | ((codePoint >> 6) & 0x3f))
        output.writeByte(0x80 | (codePoint & 0x3f))
      else
        output.writeByte(0xf0 | (codePoint >> 18))
        output.writeByte(0x80 | ((codePoint >> 12) & 0x3f))
        output.writeByte(0x80 | ((codePoint >> 6) & 0x3f))
        output.writeByte(0x80 | (codePoint & 0x3f))
      index += 1
    length

private[mysql] final case class MySqlFormat3Fingerprint(sha256: Vector[Byte], sha256Hex: String, byteLength: Long)

private[mysql] object MySqlFormat3SnapshotCanonical:
  private[mysql] val Domain = "floatingpod.mysql.format3.full20-snapshot/v1"
  private val FingerprintLength = 32

  def fixedHeaderBytes(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, maxAllowedPacketBytes: Long, profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget): Long =
    val counter = new MySqlFormat3CountingOutputStream
    val output = new DataOutputStream(counter)
    writeHeader(output, schema, connection, scope, maxAllowedPacketBytes, profile, budget)
    output.flush()
    Math.addExact(counter.byteLength, Math.multiplyExact(MySqlFormat3RelationShapes.ExpectedRelationCount.toLong, 4L))

  def fingerprint(snapshot: MySqlFormat3FullSnapshot, budget: MySqlFormat3ReadBudget): MySqlFormat3Fingerprint =
    val retainedLimit = snapshot.limitProfile.maxRetainedBytes()
    val bufferSize = Math.min(8192L, retainedLimit).toInt
    val canonicalBufferBytes = Math.max(1, bufferSize).toLong
    val digestWorkspaceBytes = 1_024L
    val digestResultReservation = 640L
    budget.reserveTemporaryMemory(digestWorkspaceBytes, "format3 SHA-256 workspace")
    var canonicalBufferReserved = false
    var resultStorageReserved = false
    var output: DataOutputStream = null
    var primary: Throwable = null
    var completed = false
    var result: MySqlFormat3Fingerprint = null
    try
      budget.reserveTemporaryMemory(canonicalBufferBytes, "format3 canonical digest buffer")
      canonicalBufferReserved = true
      val digest = MessageDigest.getInstance("SHA-256")
      val counted = new MySqlFormat3DigestOutputStream(digest, retainedLimit)
      output = new DataOutputStream(new BufferedOutputStream(counted, Math.max(1, bufferSize)))
      writeSnapshot(snapshot, output, budget)
      output.flush()
      if counted.byteLength > snapshot.limitProfile.maxRetainedBytes() then
        throw new SQLDataException("MySQL format3 canonical snapshot exceeds explicit max_retained_bytes")
      budget.reserveRetainedMemory(digestResultReservation, "format3 canonical digest result")
      resultStorageReserved = true
      val hash = digest.digest()
      if hash.length != FingerprintLength then throw new IllegalStateException("SHA-256 returned an invalid format3 snapshot digest")
      val vector = hash.toVector
      val hex = HexFormat.of().withUpperCase().formatHex(hash)
      result = MySqlFormat3Fingerprint(vector, hex, counted.byteLength)
      completed = true
    catch
      case failure: Throwable =>
        primary = failure
    finally
      if output != null then
        try output.close()
        catch
          case cleanup: Throwable =>
            if primary == null then
              primary = cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)
      val releaseActions = Vector.newBuilder[() => Unit]
      if resultStorageReserved && (!completed || primary != null) then
        releaseActions += (() => budget.releaseRetainedMemory(digestResultReservation, "format3 discarded canonical digest result"))
        resultStorageReserved = false
      if canonicalBufferReserved then
        releaseActions += (() => budget.releaseTemporaryMemory(canonicalBufferBytes, "format3 canonical digest buffer"))
        canonicalBufferReserved = false
      releaseActions += (() => budget.releaseTemporaryMemory(digestWorkspaceBytes, "format3 SHA-256 workspace"))
      MySqlFormat3BudgetCleanup.run(primary, releaseActions.result())
    if primary != null then throw primary
    if !completed || result == null then throw new IllegalStateException("format3 canonical digest produced no complete result")
    result

  /** fingerprint、完整 DATA export 与 strict roundtrip 共用同一个既有 v1 writer。 */
  private[mysql] def writeSnapshot(snapshot: MySqlFormat3FullSnapshot, output: DataOutputStream,
      budget: MySqlFormat3ReadBudget): Unit =
    writeHeader(output, snapshot.schema, snapshot.connection, snapshot.observationScope,
      snapshot.maxAllowedPacketBytes, snapshot.limitProfile, budget)
    snapshot.relations.foreach { relation =>
      MySqlModeEnvelopeV2Census.int(output, relation.rows.size)
      relation.rows.foreach { row =>
        MySqlModeEnvelopeV2Census.int(output, row.size)
        MySqlModeEnvelopeV2Census.typedRow(output, relation.table, row)
        row.foreach(writeCell(_, output))
      }
    }

  private def writeHeader(output: DataOutputStream, schema: String,
      connection: MySqlFormat3ConnectionObservation, scope: MySqlFormat3ObservationScope,
      maxAllowedPacketBytes: Long, profile: SourceLimitProfile, budget: MySqlFormat3ReadBudget): Unit =
    writeString(output, Domain)
    writeAscii(output, MySqlFormat3RelationShapes.SourceFoundationSha256)
    writeAscii(output, MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256)
    writeAscii(output, MySqlFormat3RelationShapes.SourceLockedReadSha256)
    writeString(output, schema)
    writeUuid(output, connection.serverUuid)
    writeString(output, connection.serverVersion)
    writeString(output, connection.catalog)
    writeString(output, connection.effectiveUser)
    writeString(output, connection.defaultStorageEngine)
    MySqlModeEnvelopeV2Census.int(output, connection.innodbFlushLogAtTrxCommit)
    MySqlModeEnvelopeV2Census.long(output, connection.syncBinlog)
    writeString(output, connection.transactionIsolation)
    writeString(output, connection.readTransactionIsolation)
    writeString(output, connection.sqlMode)
    writeString(output, connection.timeZone)
    writeString(output, connection.tlsVersion)
    writeString(output, connection.tlsCipher)
    writeUuid(output, scope.bootUuid)
    MySqlModeEnvelopeV2Census.long(output, scope.bootGeneration)
    MySqlModeEnvelopeV2Census.long(output, scope.connectionGeneration)
    MySqlModeEnvelopeV2Census.long(output, maxAllowedPacketBytes)
    budget.reserveTemporaryMemory(192L, "format3 immutable source limit profile copies")
    var primary: Throwable = null
    try
      val profileBytes = profile.canonicalBytes()
      val profileHash = profile.sha256()
      MySqlModeEnvelopeV2Census.int(output, profileBytes.length)
      output.write(profileBytes)
      output.write(profileHash)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(192L, "format3 immutable source limit profile copies")))
    MySqlModeEnvelopeV2Census.int(output, MySqlFormat3RelationShapes.canonicalOrder.size)
    MySqlFormat3RelationShapes.canonicalOrder.foreach { shape =>
      writeString(output, shape.table)
      writeString(output, shape.lockClause)
      MySqlModeEnvelopeV2Census.int(output, shape.primaryKey.size)
      shape.primaryKey.foreach(writeString(output, _))
      MySqlModeEnvelopeV2Census.int(output, shape.columns.size)
      shape.columns.foreach { column =>
        writeString(output, column.name)
        writeString(output, column.sqlDeclaration)
        MySqlModeEnvelopeV2Census.byte(output, column.kind.ordinal)
        MySqlModeEnvelopeV2Census.bool(output, column.nullable)
        column.maximumLength match
          case Some(length) => MySqlModeEnvelopeV2Census.bool(output, true); MySqlModeEnvelopeV2Census.int(output, length)
          case None => MySqlModeEnvelopeV2Census.bool(output, false)
      }
    }

  private[mysql] def writeCell(cell: MySqlFormat3Cell, output: DataOutputStream): Unit =
    val recorded = MySqlModeEnvelopeV2Census.beginTyped(output, cell)
    try
      cell match
        case MySqlFormat3Cell.SqlNull => MySqlModeEnvelopeV2Census.byte(output, 0)
        case MySqlFormat3Cell.Text(value) => MySqlModeEnvelopeV2Census.byte(output, 1); MySqlModeEnvelopeV2Census.text(output, value)
        case MySqlFormat3Cell.Binary(value) => MySqlModeEnvelopeV2Census.byte(output, 2); MySqlModeEnvelopeV2Census.int(output, value.length); MySqlModeEnvelopeV2Census.binary(output, value)
        case MySqlFormat3Cell.SignedInteger(value) => MySqlModeEnvelopeV2Census.byte(output, 3); MySqlModeEnvelopeV2Census.long(output, value)
        case MySqlFormat3Cell.Floating(rawBits) => MySqlModeEnvelopeV2Census.byte(output, 4); MySqlModeEnvelopeV2Census.long(output, rawBits)
        case MySqlFormat3Cell.DateTime(value) =>
          MySqlModeEnvelopeV2Census.byte(output, 5)
          MySqlModeEnvelopeV2Census.int(output, value.getYear)
          MySqlModeEnvelopeV2Census.byte(output, value.getMonthValue)
          MySqlModeEnvelopeV2Census.byte(output, value.getDayOfMonth)
          MySqlModeEnvelopeV2Census.byte(output, value.getHour)
          MySqlModeEnvelopeV2Census.byte(output, value.getMinute)
          MySqlModeEnvelopeV2Census.byte(output, value.getSecond)
          MySqlModeEnvelopeV2Census.int(output, value.getNano)
    finally MySqlModeEnvelopeV2Census.endTyped(output, recorded)

  private[mysql] def writeString(output: DataOutputStream, value: String): Unit =
    if value == null then throw new SQLDataException("MySQL format3 canonical text is null")
    MySqlModeEnvelopeV2Census.text(output, value)

  private[mysql] def writeAscii(output: DataOutputStream, value: String): Unit =
    var index = 0
    while index < value.length do
      val character = value.charAt(index)
      if character > 0x7f then throw new SQLDataException("MySQL format3 pinned source hash is not ASCII")
      MySqlModeEnvelopeV2Census.byte(output, character.toInt)
      index += 1

  private[mysql] def writeUuid(output: DataOutputStream, value: UUID): Unit =
    MySqlModeEnvelopeV2Census.long(output, value.getMostSignificantBits)
    MySqlModeEnvelopeV2Census.long(output, value.getLeastSignificantBits)

private[mysql] final class MySqlFormat3CountingOutputStream extends OutputStream:
  private var count = 0L
  def byteLength: Long = count
  override def write(value: Int): Unit = count = Math.addExact(count, 1L)
  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = count = Math.addExact(count, length.toLong)

private[mysql] final class MySqlFormat3DigestOutputStream(digest: MessageDigest, limit: Long) extends OutputStream:
  require(digest != null && limit > 0L, "bounded snapshot digest requires SHA-256 and a positive explicit limit")
  private var count = 0L
  def byteLength: Long = count
  override def write(value: Int): Unit =
    reserve(1L)
    digest.update(value.toByte)
  override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
    if bytes == null then throw new NullPointerException("snapshot digest bytes")
    if offset < 0 || length < 0 || offset > bytes.length - length then throw new IndexOutOfBoundsException("snapshot digest slice")
    reserve(length.toLong)
    digest.update(bytes, offset, length)
  private def reserve(amount: Long): Unit =
    val next = Math.addExact(count, amount)
    if next > limit then throw new SQLDataException("MySQL format3 canonical snapshot exceeds explicit max_retained_bytes")
    count = next

private[mysql] object MySqlFormat3BudgetCleanup:
  def run(primary: Throwable, actions: Iterable[() => Unit]): Unit =
    var cleanupFailure: Throwable = null
    actions.foreach { action =>
      try action()
      catch
        case failure: Throwable =>
          if primary != null then
            if failure ne primary then primary.addSuppressed(failure)
          else if cleanupFailure == null then cleanupFailure = failure
          else if failure ne cleanupFailure then cleanupFailure.addSuppressed(failure)
    }
    if primary == null && cleanupFailure != null then throw cleanupFailure
