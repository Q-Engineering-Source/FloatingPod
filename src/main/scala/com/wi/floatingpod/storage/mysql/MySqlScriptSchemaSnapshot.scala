package com.wi.floatingpod.storage.mysql

import java.io.{BufferedOutputStream, DataOutputStream, EOFException, OutputStream}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}

/** 不可变的完整 closed-layout ScriptPublication1 image；其shape独立于format3/full20。 */
private[mysql] final class MySqlScriptSchemaSnapshot private (
    val schema: String,
    val connection: MySqlFormat3ConnectionObservation,
    val observationScope: MySqlFormat3ObservationScope,
    val maxAllowedPacketBytes: Long,
    val inputs: MySqlScriptSchemaInputs,
    val relations: Vector[MySqlFormat3RelationSnapshot],
    private[mysql] val allocationBudget: MySqlFormat3ReadBudget,
    private val measuredRetainedValueBytes: Long
):
  require(inputs != null && allocationBudget != null,
    "ScriptPublication1 snapshot requires explicit inputs, packet observation and allocation owner")
  require(schema == inputs.schema, "ScriptPublication1 snapshot schema must use the frozen profile identity")
  require(connection != null && connection.catalog == schema && observationScope != null,
    "ScriptPublication1 snapshot requires its exact observed schema/session context")
  require(maxAllowedPacketBytes > 0L,
    "ScriptPublication1 snapshot requires explicit inputs, packet observation and allocation owner")
  allocationBudget.requireScriptDataInputs(inputs)
  require(measuredRetainedValueBytes >= 0L, "validated retained value length is required")

  private lazy val fingerprint = MySqlScriptSchemaSnapshotCanonical.fingerprint(this, allocationBudget)
  def canonicalSha256: Vector[Byte] = fingerprint.sha256
  def canonicalSha256Hex: String = fingerprint.sha256Hex
  def canonicalByteLength: Long = fingerprint.byteLength
  def retainedValueBytes: Long = measuredRetainedValueBytes
  def rowCount: Long = relations.foldLeft(0L)((count, relation) => Math.addExact(count, relation.rows.size.toLong))

  def relation(table: String): MySqlFormat3RelationSnapshot =
    inputs.forTable(table)
    relations.find(_.table == table).getOrElse(throw new IllegalStateException("complete ScriptPublication1 relation is missing"))

  def exactlyEquals(other: MySqlScriptSchemaSnapshot): Boolean =
    other != null && schema == other.schema && connection == other.connection && observationScope == other.observationScope &&
      maxAllowedPacketBytes == other.maxAllowedPacketBytes && (inputs eq other.inputs) && relations == other.relations &&
      canonicalSha256 == other.canonicalSha256

  override def toString: String =
    s"MySqlScriptSchemaSnapshot[relations=${relations.size}, rows=$rowCount, sha256=$canonicalSha256Hex]"

private[mysql] object MySqlScriptSchemaSnapshot:
  /** 已物化 typed rows 的 DATA 边界；不接收调用方提供的 shape、SQL 或 domain。 */
  private[mysql] def captureMaterialized(
      schema: String,
      connection: MySqlFormat3ConnectionObservation,
      observationScope: MySqlFormat3ObservationScope,
      maxAllowedPacketBytes: Long,
      inputs: MySqlScriptSchemaInputs,
      relations: Vector[MySqlFormat3RelationSnapshot],
      allocationBudget: MySqlFormat3ReadBudget
  ): MySqlScriptSchemaSnapshot =
    require(inputs != null && allocationBudget != null, "exact ScriptPublication1 inputs and root budget are required")
    allocationBudget.requireScriptDataInputs(inputs)
    val measuredRetainedValueBytes = inputs.validateRelations(relations, maxAllowedPacketBytes, allocationBudget)
    if allocationBudget.isInitialized then
      allocationBudget.requireInitializedObservation(schema, connection, observationScope, maxAllowedPacketBytes)
    else allocationBudget.initialize(schema, connection, observationScope, maxAllowedPacketBytes)
    allocationBudget.recordMaterializedSnapshot(relations)
    new MySqlScriptSchemaSnapshot(schema, connection, observationScope, maxAllowedPacketBytes,
      inputs, relations, allocationBudget, measuredRetainedValueBytes)

  /** 原root的短命comparison decoder；原header不重绑当前root，也不取得borrow custody。 */
  private[mysql] def decodedForComparison(schema: String, connection: MySqlFormat3ConnectionObservation,
      observationScope: MySqlFormat3ObservationScope, packet: Long, inputs: MySqlScriptSchemaInputs,
      relations: Vector[MySqlFormat3RelationSnapshot], originalRoot: MySqlFormat3ReadBudget,
      child: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    require(inputs != null && originalRoot != null && !originalRoot.isClosed && !originalRoot.isScopedReadBudget &&
      child != null && !child.isClosed && child.isCanonicalComparisonChildOf(originalRoot) &&
      child.sharesAllocationLedger(originalRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq child),
      "decoded DATA requires its exact original root and active comparison child")
    originalRoot.requireScriptDataInputs(inputs)
    child.requireScriptDataInputs(inputs)
    val measured=inputs.validateRelationsForComparison(relations,packet,originalRoot,child)
    new MySqlScriptSchemaSnapshot(schema,connection,observationScope,packet,inputs,relations,child,measured)

  /** 仅供 decoder 使用；每个 cell 解码时已登记对应 row allocation。 */
  private[mysql] def decoded(
      schema: String,
      connection: MySqlFormat3ConnectionObservation,
      observationScope: MySqlFormat3ObservationScope,
      maxAllowedPacketBytes: Long,
      inputs: MySqlScriptSchemaInputs,
      relations: Vector[MySqlFormat3RelationSnapshot],
      allocationBudget: MySqlFormat3ReadBudget
  ): MySqlScriptSchemaSnapshot =
    allocationBudget.requireScriptDataInputs(inputs)
    val measuredRetainedValueBytes = inputs.validateRelations(relations, maxAllowedPacketBytes, allocationBudget)
    new MySqlScriptSchemaSnapshot(schema, connection, observationScope, maxAllowedPacketBytes,
      inputs, relations, allocationBudget, measuredRetainedValueBytes)

/** 保留的 canonical DATA 绑定其exact-layout root与负责计费的child。 */
private[mysql] final class MySqlScriptSchemaCanonicalPayload private[mysql] (
    val bytes: MySqlFormat3Binary,
    private[mysql] val inputs: MySqlScriptSchemaInputs,
    private[mysql] val originalBudget: MySqlFormat3ReadBudget,
    private[mysql] val storageBudget: MySqlFormat3ReadBudget
):
  require(bytes != null && inputs != null && originalBudget != null && storageBudget != null &&
    storageBudget.sharesAllocationLedger(originalBudget), "complete ScriptPublication1 canonical DATA ownership required")
  def byteLength: Int = bytes.length

private[mysql] object MySqlScriptSchemaSnapshotCodec:
  private val HeaderStructures = 16_384L

  private def requireOwner(inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    require(inputs != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (root.profile eq inputs.sourceLimitProfile),
      "ScriptPublication1 DATA requires its exact open original allocation profile/root")
    root.requireScriptDataInputs(inputs)

  /** 外部 canonical bytes 在防御性复制前先受预算约束。 */
  def capture(input: Array[Byte], inputs: MySqlScriptSchemaInputs,
      originalBudget: MySqlFormat3ReadBudget): MySqlScriptSchemaCanonicalPayload =
    requireOwner(inputs, originalBudget)
    if input == null || input.isEmpty then throw new SQLDataException(s"nonempty complete ${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical DATA is required")
    val limit = canonicalDataLimit(inputs)
    if input.length.toLong > limit then throw new SQLDataException("ScriptPublication1 canonical input exceeds explicit snapshot/retained budget")
    inDataScope(originalBudget) { child =>
      child.reserveRetainedMemory(128L, s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical payload holder")
      val copy = MySqlFormat3Binary.copyOf(input, child,
        s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical input defensive copy")
      new MySqlScriptSchemaCanonicalPayload(copy, inputs, originalBudget, child)
    }

  /** 精确计数后只分配一次最终内容，并使用所选闭合layout writer。 */
  def encode(snapshot: MySqlScriptSchemaSnapshot,
      originalBudget: MySqlFormat3ReadBudget): MySqlScriptSchemaCanonicalPayload =
    require(snapshot != null, "ScriptPublication1 snapshot is required")
    val inputs = snapshot.inputs
    requireOwner(inputs, originalBudget)
    require(!snapshot.allocationBudget.isClosed && snapshot.allocationBudget.sharesAllocationLedger(originalBudget),
      "ScriptPublication1 snapshot must remain charged to the same original allocation ledger")
    inDataScope(originalBudget) { child =>
      child.reserveTemporaryMemory(512L, s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical writer/count structures")
      var primary: Throwable = null
      try
        val counter = new MySqlFormat3CountingOutputStream
        val countOutput = new DataOutputStream(counter)
        MySqlScriptSchemaSnapshotCanonical.writeSnapshot(snapshot, countOutput, child)
        countOutput.flush()
        val length = counter.byteLength
        if length <= 0L || length > canonicalDataLimit(inputs) then
          throw new SQLDataException(s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical output exceeds explicit snapshot/retained budget")
        val size = MySqlFormat3JvmArrayLimit.requireLength(length, "ScriptPublication1 exact canonical output")
        child.reserveRetainedMemory(Math.addExact(length, 192L), s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} final canonical binary and holder")
        val sink = new ExactOutput(new Array[Byte](size))
        val output = new DataOutputStream(sink)
        MySqlScriptSchemaSnapshotCanonical.writeSnapshot(snapshot, output, child)
        output.flush()
        new MySqlScriptSchemaCanonicalPayload(MySqlFormat3Binary.takeOwned(sink.finish()), inputs, originalBudget, child)
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => child.releaseTemporaryMemory(512L, s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} canonical writer/count structures")))
    }

  /** 完整解析所选闭合layout wire，并逐字节比较流式重编码结果。 */
  def decode(payload: MySqlScriptSchemaCanonicalPayload, expectedInputs: MySqlScriptSchemaInputs,
      originalBudget: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    requireOwner(expectedInputs, originalBudget)
    require(payload != null && (payload.inputs eq expectedInputs) && (payload.originalBudget eq originalBudget) &&
      !payload.storageBudget.isClosed && payload.storageBudget.sharesAllocationLedger(originalBudget),
      "actual original ScriptPublication1 canonical payload/root identity required")
    if payload.byteLength.toLong > canonicalDataLimit(expectedInputs) then
      throw new SQLDataException("ScriptPublication1 input exceeds explicit snapshot/retained budget")
    inDataScope(originalBudget) { child =>
      child.reserveRetainedMemory(HeaderStructures, s"${MySqlScriptSchemaInputs.familyName(expectedInputs.dataLayout)} decoder header/reader structures")
      val snapshot = new Reader(payload.bytes, expectedInputs, child).readSnapshot()
      child.reserveTemporaryMemory(512L, s"${MySqlScriptSchemaInputs.familyName(expectedInputs.dataLayout)} streaming roundtrip comparison structures")
      var primary: Throwable = null
      try
        val comparison = new CompareOutput(payload.bytes)
        val output = new DataOutputStream(comparison)
        MySqlScriptSchemaSnapshotCanonical.writeSnapshot(snapshot, output, child)
        output.flush()
        comparison.finish()
        snapshot
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => child.releaseTemporaryMemory(512L, s"${MySqlScriptSchemaInputs.familyName(expectedInputs.dataLayout)} streaming roundtrip comparison structures")))
    }

  private def canonicalDataLimit(inputs: MySqlScriptSchemaInputs): Long =
    Math.min(Math.min(inputs.budget.snapshotBytes, inputs.budget.retainedBytes), inputs.sourceLimitProfile.maxRetainedBytes())

  /** 失败时仅释放局部对象退出后的 child；原始输入与 root 仍由原所有者持有。 */
  private def inDataScope[A](root: MySqlFormat3ReadBudget)(body: MySqlFormat3ReadBudget => A): A =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val child = root.newCanonicalDataScope()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(child) { body(child) }
      catch
        case failure: Throwable =>
          try child.closeScopedReadBudget()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private final class ExactOutput(private val content: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset == content.length then throw new SQLDataException("ScriptPublication1 canonical exact output overrun")
      content(offset) = value.toByte
      offset += 1
    override def write(bytes: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, bytes.length)
      if length > content.length - offset then throw new SQLDataException("ScriptPublication1 canonical exact output overrun")
      System.arraycopy(bytes, start, content, offset, length)
      offset += length
    def finish(): Array[Byte] =
      if offset != content.length then throw new SQLDataException("ScriptPublication1 canonical exact output underrun")
      content

  private final class CompareOutput(input: MySqlFormat3Binary) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= input.length || input.byteAt(offset) != value.toByte then
        throw new SQLDataException("ScriptPublication1 decoded DATA does not exactly reencode its original bytes")
      offset += 1
    override def write(bytes: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, bytes.length)
      var index = 0
      while index < length do
        write(bytes(start + index).toInt)
        index += 1
    def finish(): Unit =
      if offset != input.length then throw new SQLDataException("ScriptPublication1 canonical roundtrip length differs")

  private final class Reader(input: MySqlFormat3Binary, inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget):
    private var offset = 0
    private def remaining: Int = input.length - offset
    private def need(length: Long): Unit =
      if length < 0L then throw new SQLDataException("negative ScriptPublication1 framing length")
      if length > remaining.toLong then throw new EOFException("truncated ScriptPublication1 canonical framing")
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
      case _ => throw new SQLDataException("noncanonical ScriptPublication1 boolean")
    private def length(): Int =
      val value = int32()
      if value < 0 then throw new SQLDataException("negative ScriptPublication1 length")
      need(value.toLong)
      value
    private def uuid(): UUID = new UUID(int64(), int64())
    private def exactAscii(value: String): Unit =
      need(value.length.toLong)
      var index = 0
      while index < value.length do
        if octet() != value.charAt(index).toInt then throw new SQLDataException("ScriptPublication1 pinned SQL source identity differs")
        index += 1
    private def exactText(value: String): Unit =
      val size = length()
      if size.toLong != MySqlFormat3Utf8.byteLength(value) then throw new SQLDataException("ScriptPublication1 frozen shape text length differs")
      var index = 0
      while index < value.length do
        if value.charAt(index) > 127 || octet() != value.charAt(index).toInt then
          throw new SQLDataException("ScriptPublication1 frozen domain/profile shape differs")
        index += 1
    private def text(size: Int, context: String): String =
      need(size.toLong)
      MySqlFormat3JvmArrayLimit.requireLength(size.toLong, s"$context ScriptPublication1 text")
      val temporary = Math.addExact(Math.multiplyExact(size.toLong, 3L), 384L)
      val retained = Math.addExact(Math.multiplyExact(size.toLong, 2L), 64L)
      budget.reserveRetainedMemory(retained, s"$context decoded String")
      var bytes: Array[Byte] = null
      var chars: CharBuffer = null
      var temporaryReserved = false
      var primary: Throwable = null
      var result: String = null
      try
        budget.reserveTemporaryMemory(temporary, s"$context strict UTF8 bytes/chars/decoder")
        temporaryReserved = true
        bytes = new Array[Byte](size)
        var index = 0
        while index < size do
          bytes(index) = octet().toByte
          index += 1
        val decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
        chars = decoder.decode(ByteBuffer.wrap(bytes))
        result = chars.toString
        result
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        if bytes != null then java.util.Arrays.fill(bytes, 0.toByte)
        bytes = null
        chars = null
        val cleanup = Vector.newBuilder[() => Unit]
        if temporaryReserved then cleanup += (() => budget.releaseTemporaryMemory(temporary, s"$context strict UTF8 bytes/chars/decoder"))
        if primary != null then cleanup += (() => budget.releaseRetainedMemory(retained, s"$context failed decoded String"))
        MySqlFormat3BudgetCleanup.run(primary, cleanup.result())

    private def headerText(): String = text(length(), "header")

    private def exactProfileAndShapes(): Unit =
      budget.reserveTemporaryMemory(192L, "ScriptPublication1 expected source limit profile copies")
      var profileBytes: Array[Byte] = null
      var profileSha: Array[Byte] = null
      var primary: Throwable = null
      try
        profileBytes = inputs.sourceLimitProfile.canonicalBytes()
        profileSha = inputs.sourceLimitProfile.sha256()
        if int32() != profileBytes.length then throw new SQLDataException("ScriptPublication1 source limit profile length differs")
        need(Math.addExact(profileBytes.length.toLong, profileSha.length.toLong))
        profileBytes.indices.foreach(index => if octet().toByte != profileBytes(index) then throw new SQLDataException("ScriptPublication1 source limit profile bytes differ"))
        profileSha.indices.foreach(index => if octet().toByte != profileSha(index) then throw new SQLDataException("ScriptPublication1 source limit profile digest differs"))
        val expectedBudget = inputs.budget
        val actualBudget = Vector.fill(9)(int64())
        val pinnedBudget = Vector(expectedBudget.evidenceBytes, expectedBudget.diagnosticsBytes, expectedBudget.configBytes,
          expectedBudget.requestBytes, expectedBudget.snapshotBytes, expectedBudget.receiptBytes,
          expectedBudget.rows, expectedBudget.retainedBytes, expectedBudget.packetBytes)
        if actualBudget != pinnedBudget then throw new SQLDataException("ScriptPublication1 explicit DATA budget differs")
        inputs.deploymentBudget.foreach { expectedDeploymentBudget =>
          val evidenceBytes = int64()
          val diagnosticsBytes = int64()
          if evidenceBytes != expectedDeploymentBudget.evidenceBytes || diagnosticsBytes != expectedDeploymentBudget.diagnosticsBytes then
            throw new SQLDataException("ScriptPublication1 deployment receipt DATA budget differs")
        }
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally
        profileBytes = null
        profileSha = null
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => budget.releaseTemporaryMemory(192L, "ScriptPublication1 expected source limit profile copies")))

      if int32() != inputs.canonicalOrder.size then throw new SQLDataException("ScriptPublication1 table count differs")
      inputs.canonicalOrder.foreach { shape =>
        exactText(shape.table)
        exactText(shape.lockClause)
        if int32() != shape.primaryKey.size then throw new SQLDataException("ScriptPublication1 PK count differs")
        shape.primaryKey.foreach(exactText)
        if int32() != shape.columns.size then throw new SQLDataException("ScriptPublication1 column count differs")
        shape.columns.foreach { column =>
          exactText(column.name)
          exactText(column.sqlDeclaration)
          if octet() != column.kind.ordinal || bool() != column.nullable then
            throw new SQLDataException("ScriptPublication1 typed column profile differs")
          val hasMaximum = bool()
          if hasMaximum != column.maximumLength.nonEmpty then throw new SQLDataException("ScriptPublication1 column maximum framing differs")
          if hasMaximum && int32() != column.maximumLength.get then throw new SQLDataException("ScriptPublication1 column maximum differs")
        }
      }

    def readSnapshot(): MySqlScriptSchemaSnapshot =
      exactText(inputs.snapshotDomain)
      exactAscii(inputs.ddlSha256)
      if int32() != inputs.schemaFormat then throw new SQLDataException("ScriptPublication1 schema format differs")
      exactText(inputs.schema)
      val server = uuid()
      val connection = MySqlFormat3ConnectionObservation(server, headerText(), headerText(), headerText(), headerText(),
        int32(), int64(), headerText(), headerText(), headerText(), headerText(), headerText(), headerText())
      val scope = MySqlFormat3ObservationScope(uuid(), int64(), int64())
      val packet = int64()
      if packet <= 0L then throw new SQLDataException("ScriptPublication1 observed packet limit is not positive")
      exactProfileAndShapes()
      budget.bindCanonicalDataPacketLimit(packet)
      budget.initialize(inputs.schema, connection, scope, packet)

      var totalRows = 0L
      val relations = inputs.canonicalOrder.map { shape =>
        val count = int32()
        if count < 0 then throw new SQLDataException(s"${shape.table} has a negative ScriptPublication1 row count")
        totalRows = Math.addExact(totalRows, count.toLong)
        if totalRows > inputs.budget.rows then throw new SQLDataException("ScriptPublication1 total row budget exceeded during decode")
        if count > Int.MaxValue - 8 then throw new SQLDataException(s"${shape.table} row count exceeds the JVM Vector index range")
        val framedWidth = Math.addExact(4L, shape.columns.size.toLong)
        val minimum = try Math.multiplyExact(count.toLong, framedWidth)
        catch case overflow: ArithmeticException => throw new SQLDataException(s"${shape.table} row framing length overflow", overflow)
        need(minimum)
        val rows = Vector.newBuilder[Vector[MySqlFormat3Cell]]
        var rowNumber = 0
        while rowNumber < count do
          if int32() != shape.columns.size then throw new SQLDataException(s"${shape.table} row width differs")
          budget.beginRow(shape.columns.size, shape.table, rowNumber.toLong)
          val cells = shape.columns.map(column => cell(column, shape.table, rowNumber.toLong))
          budget.endRow(shape.table, rowNumber.toLong)
          rows += cells
          rowNumber += 1
        MySqlFormat3RelationSnapshot(shape.table, rows.result())
      }
      if remaining != 0 then throw new SQLDataException("trailing ScriptPublication1 canonical bytes")

      MySqlScriptSchemaSnapshot.decoded(inputs.schema, connection, scope, packet, inputs, relations, budget)

    private def cell(column: MySqlFormat3ColumnShape, table: String, rowNumber: Long): MySqlFormat3Cell =
      val context = s"$table row $rowNumber column ${column.name}"
      octet() match
        case 0 =>
          if !column.nullable then throw new SQLDataException(s"$context unexpectedly encodes SQL NULL")
          budget.beginNullCell(context)
          MySqlFormat3Cell.SqlNull
        case 1 =>
          if column.kind != MySqlFormat3SqlKind.Text then throw new SQLDataException(s"$context text type differs")
          val size = length()
          val maximumBytes = inputs.maximumPayloadBytes(table, column)
          maximumBytes.foreach(maximum => if size.toLong > maximum then throw new SQLDataException(s"$context exceeds explicit text byte budget"))
          budget.beginVariableCell(context)
          budget.addVariablePayload(size.toLong, context)
          val value = text(size, context)
          if column.maximumLength.exists(_ < value.codePointCount(0, value.length)) then
            throw new SQLDataException(s"$context exceeds frozen text character length")
          MySqlFormat3Cell.Text(value)
        case 2 =>
          if column.kind != MySqlFormat3SqlKind.Binary then throw new SQLDataException(s"$context binary type differs")
          val size = length()
          if column.maximumLength.exists(_ < size) then throw new SQLDataException(s"$context exceeds frozen binary length")
          inputs.maximumPayloadBytes(table, column).foreach(maximum => if size.toLong > maximum then throw new SQLDataException(s"$context exceeds explicit binary byte budget"))
          budget.beginVariableCell(context)
          budget.addVariablePayload(size.toLong, context)
          val charge = Math.addExact(size.toLong, 64L)
          budget.reserveRetainedMemory(charge, context)
          val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(size.toLong, context))
          var index = 0
          while index < size do
            bytes(index) = octet().toByte
            index += 1
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
        case 3 =>
          if column.kind != MySqlFormat3SqlKind.Integer then throw new SQLDataException(s"$context integer type differs")
          budget.beginFixedCell(8, integerWireWidth(column), context)
          MySqlFormat3Cell.SignedInteger(int64())
        case 4 =>
          if column.kind != MySqlFormat3SqlKind.Floating then throw new SQLDataException(s"$context floating type differs")
          budget.beginFixedCell(8, 64, context)
          MySqlFormat3Cell.Floating(int64())
        case 5 =>
          if column.kind != MySqlFormat3SqlKind.DateTime then throw new SQLDataException(s"$context datetime type differs")
          need(13L)
          budget.beginFixedCell(13, 32, context)
          MySqlFormat3Cell.DateTime(LocalDateTime.of(int32(), octet(), octet(), octet(), octet(), octet(), int32()))
        case _ => throw new SQLDataException("unknown ScriptPublication1 typed cell tag")

    private def integerWireWidth(column: MySqlFormat3ColumnShape): Int =
      if column.sqlDeclaration.startsWith("TINYINT") then 1
      else if column.sqlDeclaration.startsWith("SMALLINT") then 2
      else if column.sqlDeclaration.startsWith("INT ") then 4
      else 8

private[mysql] object MySqlScriptSchemaSnapshotCanonical:
  private val DigestBufferBytes = 8_192L
  private val FingerprintWorkspace = 1_024L
  private val DigestResultReservation = 640L

  def fixedHeaderBytes(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, packet: Long, inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Long =
    val counter = new MySqlFormat3CountingOutputStream
    val output = new DataOutputStream(counter)
    writeHeader(output, schema, connection, scope, packet, inputs, budget)
    output.flush()
    Math.addExact(counter.byteLength, Math.multiplyExact(inputs.canonicalOrder.size.toLong, 4L))

  def writeSnapshot(snapshot: MySqlScriptSchemaSnapshot, output: DataOutputStream,
      budget: MySqlFormat3ReadBudget): Unit =
    writeHeader(output, snapshot.schema, snapshot.connection, snapshot.observationScope,
      snapshot.maxAllowedPacketBytes, snapshot.inputs, budget)
    snapshot.relations.foreach { relation =>
      MySqlModeEnvelopeV2Census.int(output, relation.rows.size)
      relation.rows.foreach { row =>
        MySqlModeEnvelopeV2Census.int(output, row.size)
        MySqlModeEnvelopeV2Census.typedRow(output, relation.table, row)
        row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_, output))
      }
    }

  private def writeHeader(output: DataOutputStream, schema: String,
      connection: MySqlFormat3ConnectionObservation, scope: MySqlFormat3ObservationScope,
      packet: Long, inputs: MySqlScriptSchemaInputs, budget: MySqlFormat3ReadBudget): Unit =
    MySqlFormat3SnapshotCanonical.writeString(output, inputs.snapshotDomain)
    MySqlFormat3SnapshotCanonical.writeAscii(output, inputs.ddlSha256)
    MySqlModeEnvelopeV2Census.int(output, inputs.schemaFormat)
    MySqlFormat3SnapshotCanonical.writeString(output, schema)
    MySqlFormat3SnapshotCanonical.writeUuid(output, connection.serverUuid)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.serverVersion)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.catalog)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.effectiveUser)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.defaultStorageEngine)
    MySqlModeEnvelopeV2Census.int(output, connection.innodbFlushLogAtTrxCommit)
    MySqlModeEnvelopeV2Census.long(output, connection.syncBinlog)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.transactionIsolation)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.readTransactionIsolation)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.sqlMode)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.timeZone)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.tlsVersion)
    MySqlFormat3SnapshotCanonical.writeString(output, connection.tlsCipher)
    MySqlFormat3SnapshotCanonical.writeUuid(output, scope.bootUuid)
    MySqlModeEnvelopeV2Census.long(output, scope.bootGeneration)
    MySqlModeEnvelopeV2Census.long(output, scope.connectionGeneration)
    MySqlModeEnvelopeV2Census.long(output, packet)
    budget.reserveTemporaryMemory(192L, "ScriptPublication1 immutable source limit profile copies")
    var primary: Throwable = null
    try
      val profileBytes = inputs.sourceLimitProfile.canonicalBytes()
      val profileHash = inputs.sourceLimitProfile.sha256()
      MySqlModeEnvelopeV2Census.int(output, profileBytes.length)
      output.write(profileBytes)
      output.write(profileHash)
      val data = inputs.budget
      Vector(data.evidenceBytes, data.diagnosticsBytes, data.configBytes, data.requestBytes,
        data.snapshotBytes, data.receiptBytes, data.rows, data.retainedBytes, data.packetBytes)
        .foreach(MySqlModeEnvelopeV2Census.long(output, _))
      inputs.deploymentBudget.foreach { deployment =>
        MySqlModeEnvelopeV2Census.long(output, deployment.evidenceBytes)
        MySqlModeEnvelopeV2Census.long(output, deployment.diagnosticsBytes)
      }
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(192L, "ScriptPublication1 immutable source limit profile copies")))

    val shapes = inputs.canonicalOrder
    MySqlModeEnvelopeV2Census.int(output, shapes.size)
    shapes.foreach { shape =>
      MySqlFormat3SnapshotCanonical.writeString(output, shape.table)
      MySqlFormat3SnapshotCanonical.writeString(output, shape.lockClause)
      MySqlModeEnvelopeV2Census.int(output, shape.primaryKey.size)
      shape.primaryKey.foreach(MySqlFormat3SnapshotCanonical.writeString(output, _))
      MySqlModeEnvelopeV2Census.int(output, shape.columns.size)
      shape.columns.foreach { column =>
        MySqlFormat3SnapshotCanonical.writeString(output, column.name)
        MySqlFormat3SnapshotCanonical.writeString(output, column.sqlDeclaration)
        MySqlModeEnvelopeV2Census.byte(output, column.kind.ordinal)
        MySqlModeEnvelopeV2Census.bool(output, column.nullable)
        column.maximumLength match
          case Some(length) => MySqlModeEnvelopeV2Census.bool(output, true); MySqlModeEnvelopeV2Census.int(output, length)
          case None => MySqlModeEnvelopeV2Census.bool(output, false)
      }
    }

  def fingerprint(snapshot: MySqlScriptSchemaSnapshot,
      budget: MySqlFormat3ReadBudget): MySqlFormat3Fingerprint =
    val inputs = snapshot.inputs
    val limit = Math.min(Math.min(inputs.budget.snapshotBytes, inputs.budget.retainedBytes), inputs.sourceLimitProfile.maxRetainedBytes())
    budget.reserveTemporaryMemory(FingerprintWorkspace, "ScriptPublication1 SHA-256 workspace")
    var bufferReserved = false
    var resultReserved = false
    var output: DataOutputStream = null
    var primary: Throwable = null
    var complete = false
    var result: MySqlFormat3Fingerprint = null
    try
      budget.reserveTemporaryMemory(DigestBufferBytes, "ScriptPublication1 canonical digest buffer")
      bufferReserved = true
      val digest = MessageDigest.getInstance("SHA-256")
      val counted = new ScriptDigestOutputStream(digest, limit)
      output = new DataOutputStream(new BufferedOutputStream(counted, DigestBufferBytes.toInt))
      MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
        writeSnapshot(snapshot, output, budget)
        output.flush()
      }
      budget.reserveRetainedMemory(DigestResultReservation, "ScriptPublication1 canonical digest result")
      resultReserved = true
      val hash = digest.digest()
      if hash.length != 32 then throw new IllegalStateException("SHA-256 returned an invalid ScriptPublication1 digest")
      result = MySqlFormat3Fingerprint(hash.toVector, HexFormat.of().withUpperCase().formatHex(hash), counted.byteLength)
      complete = true
    catch
      case failure: Throwable => primary = failure
    finally
      if output != null then
        try output.close()
        catch
          case cleanup: Throwable =>
            if primary == null then primary = cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)
      val actions = Vector.newBuilder[() => Unit]
      if resultReserved && (!complete || primary != null) then
        actions += (() => budget.releaseRetainedMemory(DigestResultReservation, "ScriptPublication1 discarded canonical digest result"))
      if bufferReserved then actions += (() => budget.releaseTemporaryMemory(DigestBufferBytes, "ScriptPublication1 canonical digest buffer"))
      actions += (() => budget.releaseTemporaryMemory(FingerprintWorkspace, "ScriptPublication1 SHA-256 workspace"))
      MySqlFormat3BudgetCleanup.run(primary, actions.result())
    if primary != null then throw primary
    if !complete || result == null then throw new IllegalStateException("ScriptPublication1 canonical digest produced no complete result")
    result

  private final class ScriptDigestOutputStream(digest: MessageDigest, limit: Long) extends OutputStream:
    require(digest != null && limit > 0L, "bounded ScriptPublication1 digest requires SHA-256 and explicit budget")
    private var count = 0L
    def byteLength: Long = count
    override def write(value: Int): Unit =
      reserve(1L)
      digest.update(value.toByte)
    override def write(bytes: Array[Byte], offset: Int, length: Int): Unit =
      if bytes == null then throw new NullPointerException("ScriptPublication1 digest bytes")
      if offset < 0 || length < 0 || offset > bytes.length - length then throw new IndexOutOfBoundsException("ScriptPublication1 digest slice")
      reserve(length.toLong)
      digest.update(bytes, offset, length)
    private def reserve(amount: Long): Unit =
      val next = Math.addExact(count, amount)
      if next > limit then throw new SQLDataException("ScriptPublication1 canonical snapshot exceeds explicit snapshot/retained budget")
      count = next
