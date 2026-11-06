package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.storage.{OperatingModeChangeRejection, PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream, EOFException, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.{DigestOutputStream, MessageDigest}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{Arrays, UUID}
import scala.collection.mutable
import MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage

private[mysql] enum MySqlOperationEffectPhase:
  case Reserve
  case Execute

private[mysql] enum MySqlOperationEffectImage:
  case Before
  case Target

private[mysql] enum MySqlModeProjectionVersion(val number: Int, val domain: String, val encoding: String, val censusEncoding: Int):
  case V1 extends MySqlModeProjectionVersion(1, "com.wi.floatingpod.mysql-operation-effect-projection/v1", "FP_MYSQL_MODE_EFFECT_PROJECTION_BE_V1", 3)
  case V2 extends MySqlModeProjectionVersion(2, "com.wi.floatingpod.mysql-operation-effect-projection/v2", "FP_MYSQL_MODE_EFFECT_PROJECTION_BE_V2", 16)

private[mysql] enum MySqlModePreflight:
  case Rejected(reason: OperatingModeChangeRejection)
  case AlreadySet
  case Execute

/** 完整 typed operation 行的原始封套。封套内的历史 blob 保持 opaque bytes。 */
private[mysql] final class MySqlRecoveryOperationRawEnvelope private[mysql] (
    val operationUuid: UUID,
    private[mysql] val canonical: MySqlRecoveryCanonicalBytes,
    private val rawRow: Vector[MySqlFormat3Cell],
    private[mysql] val deploymentInputs: Option[MySqlScriptSchemaInputs]
):
  require(operationUuid != null && canonical != null && rawRow != null, "完整 operation 原始封套必需")
  val length: Int = canonical.length
  val sha256: Vector[Byte] = canonical.sha256
  def sha256Hex: String = canonical.hexSha256

  private[mysql] def row: Vector[MySqlFormat3Cell] = rawRow

  private[mysql] def exactlyMatches(actual: Vector[MySqlFormat3Cell], profile: SourceLimitProfile): Boolean =
    actual != null && rawRow == actual &&
      MySqlRecoveryOperationRawEnvelopeCodec.matchesCanonical(actual, profile, canonical, deploymentInputs)

  private[mysql] def exactlyMatches(actual: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      actualInputs: Option[MySqlScriptSchemaInputs]): Boolean =
    requireInputs(actualInputs)
    exactlyMatches(actual, profile)

  private[mysql] def requireInputs(actualInputs: Option[MySqlScriptSchemaInputs]): Unit =
    require(actualInputs != null && ((deploymentInputs, actualInputs) match
      case (None, None) => true
      case (Some(expected), Some(actual)) => expected eq actual
      case _ => false), "RAW envelope cannot cross its exact immutable image inputs")

  override def toString: String =
    "MySqlRecoveryOperationRawEnvelope[operation=" + operationUuid + ",length=" + length + ",sha256=" + sha256Hex + "]"

private[mysql] object MySqlRecoveryOperationRawEnvelopeCodec:
  private val Encoding = "FP_MYSQL_RECOVERY_OPERATION_RAW_ROW_BE_V1"
  private val Domain = "com.wi.floatingpod.mysql-recovery-operation-raw-row/v1"
  private val Table = "fp_recovery_operation"

  private[mysql] def requireValidRow(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget, inputs: MySqlScriptSchemaInputs): Unit =
    requireValidRow(row, profile, budget, Some(inputs))

  private[mysql] def encodeHistorical(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      originalRows: Vector[Vector[MySqlFormat3Cell]], inputs: MySqlScriptSchemaInputs): MySqlRecoveryOperationRawEnvelope =
    encodeHistorical(row, profile, originalRows, Some(inputs))

  private[mysql] def matchesCanonical(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      canonical: MySqlRecoveryCanonicalBytes, inputs: MySqlScriptSchemaInputs): Boolean =
    matchesCanonical(row, profile, canonical, Some(inputs))

  def decode(bytes: Array[Byte], expectedSha256: Vector[Byte], profile: SourceLimitProfile,
      inputs: MySqlScriptSchemaInputs): MySqlRecoveryOperationRawEnvelope =
    decode(bytes, expectedSha256, profile, Some(inputs))

  private[mysql] def inputsForImage(image: MySqlModeOperationEffectCodec.Image,
      root: MySqlFormat3ReadBudget): Option[MySqlScriptSchemaInputs] = image match
    case deployment: MySqlScriptSchemaSnapshot =>
      if deployment.inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 then
        require(root != null && !root.isClosed && !root.isScopedReadBudget &&
          (deployment.inputs.sourceLimitProfile eq root.profile) && deployment.schema == deployment.inputs.schema &&
          deployment.allocationBudget != null && !deployment.allocationBudget.isClosed &&
          ((deployment.allocationBudget eq root) || deployment.allocationBudget.isCanonicalChildOf(root)) &&
          deployment.allocationBudget.sharesAllocationLedger(root), "original complete Full27 RAW image/root required")
        root.requireScriptDataInputs(deployment.inputs)
      else MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, root)
      Some(deployment.inputs)
    case legacy: MySqlFormat3FullSnapshot =>
      root.requireFormat3Full20Layout()
      require(!legacy.allocationBudget.isClosed, "RAW legacy image must remain open")
      None

  private def activeBudget(profile: SourceLimitProfile,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): MySqlFormat3ReadBudget =
    require(deploymentInputs != null && deploymentInputs.forall(_ != null), "explicit RAW family inputs required")
    deploymentInputs match
      case None => MySqlRecoveryCanonicalBytes.allocationBudgetFor(profile)
      case Some(inputs) =>
        // inputs 只标识当前原行/完整图的分配家族，不代表 opaque 历史 payload 的 schema。
        require((inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
          inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27) &&
          (inputs.sourceLimitProfile eq profile), "RAW Full26/Full27 profile/layout must match its exact inputs")
        val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(
          throw new IllegalStateException("Full26/Full27 RAW requires an existing active allocation owner"))
        require(!budget.isClosed && (budget.profile eq profile), "Full26/Full27 RAW owner/profile must remain open and exact")
        budget.requireHistoricalPayloadInputs(inputs)
        budget

  private def retainInputsMetadata(profile: SourceLimitProfile,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): Unit =
    if deploymentInputs.nonEmpty then
      activeBudget(profile, deploymentInputs).reserveRetainedMemory(48L, "Full26/Full27 RAW immutable inputs reference metadata")

  def encode(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      inputs: MySqlScriptSchemaInputs): MySqlRecoveryOperationRawEnvelope = encode(row, profile, Some(inputs))

  def encode(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): MySqlRecoveryOperationRawEnvelope =
    if profile == null then throw new IllegalArgumentException("raw operation envelope needs the exact source limit profile")
    val shape = MySqlFormat3RelationShapes.forTable(Table)
    if row == null || row.size != shape.columns.size then invalid("raw operation row width")
    require(deploymentInputs != null && deploymentInputs.forall(_ != null), "explicit RAW family inputs required")
    if deploymentInputs.nonEmpty then activeBudget(profile, deploymentInputs)
    validateCells(row, shape)
    validateOperationRow(row, deploymentInputs)
    val operationUuid = uuidCell(cell(row, Table, "operation_uuid"), "operation_uuid")
    val canonical = MySqlRecoveryCanonicalBytes.encodeExact(Encoding, profile, encodedLength(row, shape)) { output =>
      writeRow(output, row, shape)
    }
    retainInputsMetadata(profile, deploymentInputs)
    new MySqlRecoveryOperationRawEnvelope(operationUuid, canonical, row, deploymentInputs)

  private[mysql] def encodeHistorical(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      originalRows: Vector[Vector[MySqlFormat3Cell]],
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): MySqlRecoveryOperationRawEnvelope =
    require(originalRows != null && originalRows.exists(_ eq row), "exact original preimage row identity required")
    val shape = MySqlFormat3RelationShapes.forTable(Table)
    require(row != null && row.size == shape.columns.size)
    require(deploymentInputs != null && deploymentInputs.forall(_ != null), "explicit RAW family inputs required")
    if deploymentInputs.nonEmpty then activeBudget(profile, deploymentInputs)
    validateCells(row, shape); validateOperationRow(row, deploymentInputs)
    val operationUuid = uuidCell(cell(row, Table, "operation_uuid"), "operation_uuid")
    val canonical = MySqlRecoveryCanonicalBytes.encodeHistoricalRaw(Encoding, profile, encodedLength(row, shape), row, shape)
    retainInputsMetadata(profile, deploymentInputs)
    new MySqlRecoveryOperationRawEnvelope(operationUuid, canonical, row, deploymentInputs)

  /** 与 encode 相同的 RAW typed/semantic 校验，不分配完整 canonical。
    * 仅返回 DATA 校验结果，不签 session/SQL/admission；调用者须先核真实 held owner。
    */
  private[mysql] def requireValidRow(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): Unit =
    if profile == null || budget == null || budget.isClosed || (budget.profile ne profile) then
      throw new IllegalArgumentException("exact open RAW validation budget/profile required")
    require(deploymentInputs != null && deploymentInputs.forall(_ != null), "explicit RAW family inputs required")
    // 显式 budget 是已存在的实际 owner；进入时仍拒绝 foreign root/sibling，Full26/Full27 不创建 standalone budget。
    // 在已有scope拒foreign root/sibling；只允许same或其实际canonical child。
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      if deploymentInputs.nonEmpty then activeBudget(profile, deploymentInputs)
      val shape = MySqlFormat3RelationShapes.forTable(Table)
      if row == null || row.size != shape.columns.size then invalid("raw operation row width")
      validateCells(row, shape)
      validateOperationRow(row, deploymentInputs)
      uuidCell(cell(row, Table, "operation_uuid"), "operation_uuid")
      val size = encodedLength(row, shape)
      if size > Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes()) ||
        size > MySqlFormat3JvmArrayLimit.MaximumLength then
        invalid(s"raw operation validation exceeds exact retained/packet/JVM profile: actual=$size, retained=${profile.maxRetainedBytes()}, packet=${profile.maxSqlPacketBytes()}, jvm=${MySqlFormat3JvmArrayLimit.MaximumLength}")
    }

  /** 完整 typed/semantic 检查后独立写原 framing；不按 UUID/hash 猜整行相同。 */
  private[mysql] def matchesCanonical(row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      canonical: MySqlRecoveryCanonicalBytes,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): Boolean =
    if profile == null || canonical == null then throw new IllegalArgumentException("exact raw comparison profile/canonical required")
    val budget = activeBudget(profile, deploymentInputs)
    if deploymentInputs.nonEmpty then
      require(canonical.borrowedBinary.storageBudget != null &&
        canonical.borrowedBinary.storageBudget.sharesAllocationLedger(budget),
        "Full26/Full27 RAW comparison must remain on its original allocation ledger")
    budget.reserveTemporaryMemory(320L, "raw canonical comparison stream metadata")
    var primary: Throwable = null
    try
      val shape = MySqlFormat3RelationShapes.forTable(Table)
      if row == null || row.size != shape.columns.size then invalid("raw operation row width")
      validateCells(row, shape)
      validateOperationRow(row, deploymentInputs)
      val size = encodedLength(row, shape)
      if size > Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes()) then
        invalid("raw operation comparison exceeds exact profile")
      canonical.encoding == Encoding && size == canonical.length.toLong &&
        canonical.matchesWrittenBytes(output => writeRow(output, row, shape))
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try budget.releaseTemporaryMemory(320L, "raw canonical comparison stream metadata release")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private[mysql] def writeRow(output: DataOutputStream, row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape): Unit =
    MySqlModeEnvelopeV2Census.typedRow(output, Table, row)
    MySqlModeEnvelopeV2Census.text(output, Domain)
    MySqlModeEnvelopeV2Census.byte(output, 1)
    MySqlModeEnvelopeV2Census.text(output, Table)
    MySqlModeEnvelopeV2Census.int(output, shape.columns.size)
    var index = 0
    while index < row.size do
      val column = shape.columns(index)
      MySqlModeEnvelopeV2Census.text(output, column.name)
      MySqlModeEnvelopeV2Census.text(output, column.sqlDeclaration)
      writeCell(output, row(index))
      index += 1

  private def validateCells(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape): Unit =
    var index = 0
    while index < row.size do
      validateCell(row(index), shape.columns(index))
      index += 1

  /** 已验证 immutable row 的精确长度；不重放 writer、不分配 text/payload 副本。 */
  private def encodedLength(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape): Long =
    def textLength(value: String): Long = Math.addExact(4L, MySqlFormat3Utf8.byteLength(value))
    var length = Math.addExact(Math.addExact(textLength(Domain), 1L), Math.addExact(textLength(Table), 4L))
    var index = 0
    while index < row.size do
      val column = shape.columns(index)
      length = Math.addExact(length, textLength(column.name))
      length = Math.addExact(length, textLength(column.sqlDeclaration))
      val payload = row(index) match
        case MySqlFormat3Cell.SqlNull => 0L
        case MySqlFormat3Cell.Text(value) => textLength(value)
        case MySqlFormat3Cell.Binary(value) => Math.addExact(4L, value.length.toLong)
        case MySqlFormat3Cell.SignedInteger(_) | MySqlFormat3Cell.Floating(_) => 8L
        case MySqlFormat3Cell.DateTime(_) => 13L // int32 year + five bytes + int32 microseconds
      length = Math.addExact(length, Math.addExact(1L, payload))
      index += 1
    length

  /** 先以显式限额读取，再核 SHA、列签名、严格类型、尾随数据及规范重编码。 */
  def decode(bytes: Array[Byte], expectedSha256: Vector[Byte],
      profile: SourceLimitProfile,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): MySqlRecoveryOperationRawEnvelope =
    if bytes == null || expectedSha256 == null || expectedSha256.size != 32 || profile == null then
      throw new SQLDataException("raw operation envelope bytes, SHA-256 and profile are required")
    val byteCount = bytes.length.toLong
    val limit = Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes())
    if byteCount <= 0L || byteCount > limit || byteCount > MySqlFormat3JvmArrayLimit.MaximumLength then
      throw new SQLDataException("raw operation envelope exceeds retained, packet or JVM array bounds")
    val budget = activeBudget(profile, deploymentInputs)
    val reservation = Math.addExact(byteCount, 64L)
    budget.reserveTemporaryMemory(reservation, "raw operation envelope input")
    var copy: Array[Byte] = null
    var primary: Throwable = null
    try
      copy = bytes.clone()
      if MessageDigest.getInstance("SHA-256").digest(copy).toVector != expectedSha256 then
        throw new SQLDataException("raw operation envelope SHA-256 differs from the exact input bytes")
      val row = decodeRow(copy, profile, budget, deploymentInputs)
      val envelope = encode(row, profile, deploymentInputs)
      val canonicalCopy = envelope.canonical.copyBytes(budget, "raw operation envelope canonical verification")
      try
        if !Arrays.equals(copy, canonicalCopy) then
          throw new SQLDataException("raw operation envelope is not canonically encoded")
      finally Arrays.fill(canonicalCopy, 0.toByte)
      if envelope.sha256 != expectedSha256 then
        throw new SQLDataException("raw operation envelope canonical digest differs from the supplied digest")
      envelope
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if copy != null then Arrays.fill(copy, 0.toByte)
      try budget.releaseTemporaryMemory(reservation, "raw operation envelope input release")
      catch
        case cleanup: Throwable =>
          if primary != null then
            if cleanup ne primary then primary.addSuppressed(cleanup)
          else throw cleanup

  private def decodeRow(bytes: Array[Byte], profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable(Table)
    val input = new DataInputStream(new ByteArrayInputStream(bytes))
    if readText(input, bytes.length, profile, budget, "raw operation domain") != Domain ||
      input.readUnsignedByte() != 1 then invalid("raw operation domain/version")
    if readText(input, bytes.length, profile, budget, "raw operation table") != Table then invalid("raw operation table")
    if input.readInt() != shape.columns.size then invalid("raw operation column count")
    val builder = Vector.newBuilder[MySqlFormat3Cell]
    shape.columns.foreach { column =>
      if readText(input, bytes.length, profile, budget, "raw operation column name") != column.name ||
        readText(input, bytes.length, profile, budget, "raw operation column declaration") != column.sqlDeclaration then
        invalid("raw operation frozen column signature")
      builder += readCell(input, bytes.length, profile, budget, column)
    }
    if input.available() != 0 then invalid("raw operation envelope has trailing bytes")
    val row = builder.result()
    validateOperationRow(row, deploymentInputs)
    uuidCell(cell(row, Table, "operation_uuid"), "operation_uuid")
    row

  /** Mirrors the frozen DDL's byte/hash pairs and terminal-state NULL topology. */
  private def validateOperationRow(row: Vector[MySqlFormat3Cell],
      deploymentInputs: Option[MySqlScriptSchemaInputs]): Unit =
    if cell(row, Table, "context_encoding") != MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1") then
      invalid("raw operation context encoding")
    val register = deploymentInputs.nonEmpty && cell(row, Table, "step") == MySqlFormat3Cell.Text("DEPLOYMENT_REGISTER")
    Vector(
      "request_identity_bytes" -> "request_identity_sha256",
      "original_context_bytes" -> "original_context_sha256",
      "scope_bytes" -> "scope_sha256",
      "preimage_bytes" -> "preimage_sha256",
      "target_bytes" -> "target_sha256"
    ).foreach { case (bytesName, shaName) =>
      validateDigestPair(row, bytesName, shaName, allowNull = false, allowEmpty = register && bytesName == "preimage_bytes")
    }
    validateDigestPair(row, "original_diagnostic_identity", "original_diagnostic_sha256", allowNull = true, allowEmpty = true)

    val operationState = cell(row, Table, "operation_state") match
      case MySqlFormat3Cell.Text(value) if Set("RESERVED", "COMMITTED", "CANCELLED").contains(value) => value
      case _ => invalid("raw operation state")
    val step = cell(row, Table, "step") match
      case MySqlFormat3Cell.Text(value) if (if deploymentInputs.exists(_.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27) then
          MySqlDbFirstSchemaDefinition.DataRecoverySteps.contains(value)
        else if deploymentInputs.nonEmpty then MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.contains(value)
        else Set("RESERVE", "CANCEL", "ENTITLEMENT_REGISTER", "DOMAIN_MUTATION", "REARM",
          "ACTIVATE", "RECALL_PREPARE", "RECALL_CHECKPOINT", "ABORT_CLEANUP", "MOVE_PREPARE", "MOVE_COMMIT", "MOVE_CLEANUP",
          "MOVE_REMEDIATE", "SOURCE_CAPTURE", "SOURCE_PREPARE", "SOURCE_WRITE_PREPARE", "SOURCE_WRITE_RESULT",
          "SOURCE_WRITE_RECONCILE", "SOURCE_CUTOVER", "SOURCE_RETIRE", "SOURCE_REMEDIATE", "RESERVATION_RELEASE").contains(value)) => value
      case _ => invalid("raw operation step")
    if register then
      if (cell(row, Table, "preimage_bytes") match
        case MySqlFormat3Cell.Binary(bytes) => bytes.length != 0
        case _ => true) || cell(row, Table, "expected_domain_version") != MySqlFormat3Cell.SignedInteger(0L) ||
        cell(row, Table, "previous_step_operation_uuid") != MySqlFormat3Cell.SqlNull then
        invalid("schema5 REGISTER requires exact empty preimage, revision zero and no predecessor")
    val resultAt = cell(row, Table, "result_at")
    val settledBy = cell(row, Table, "settled_by_operation_uuid")
    val witnessBytes = cell(row, Table, "commit_witness_bytes")
    val witnessSha = cell(row, Table, "commit_witness_sha256")
    operationState match
      case "RESERVED" =>
        if resultAt != MySqlFormat3Cell.SqlNull || settledBy != MySqlFormat3Cell.SqlNull ||
          witnessBytes != MySqlFormat3Cell.SqlNull || witnessSha != MySqlFormat3Cell.SqlNull then
          invalid("RESERVED operation result/settlement/witness must remain SQL NULL")
      case "COMMITTED" =>
        if !resultAt.isInstanceOf[MySqlFormat3Cell.DateTime] || settledBy != MySqlFormat3Cell.SqlNull then
          invalid("COMMITTED operation result/settlement topology")
        validateDigestPair(row, "commit_witness_bytes", "commit_witness_sha256", allowNull = false)
      case "CANCELLED" =>
        if !resultAt.isInstanceOf[MySqlFormat3Cell.DateTime] || settledBy == MySqlFormat3Cell.SqlNull then
          invalid("CANCELLED operation result/settlement topology")
        validateDigestPair(row, "commit_witness_bytes", "commit_witness_sha256", allowNull = false)
    if step == "RESERVE" then
      if operationState != "COMMITTED" || cell(row, Table, "reserve_operation_uuid") != MySqlFormat3Cell.SqlNull then
        invalid("RESERVE operation must be terminal COMMITTED with no reserve parent")
    else if operationState == "RESERVED" && settledBy != MySqlFormat3Cell.SqlNull then
      invalid("non-RESERVE planned operation cannot already be settled")

    val bootGeneration = cell(row, Table, "boot_generation") match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => invalid("raw operation boot generation type")
    val connectionId = cell(row, Table, "connection_id") match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => invalid("raw operation connection id type")
    val fenceEpoch = cell(row, Table, "fence_epoch") match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => invalid("raw operation fence epoch type")
    if bootGeneration <= 0L || connectionId <= 0L || fenceEpoch < 0L then
      invalid("raw operation generation, connection or epoch range")

  private def validateDigestPair(row: Vector[MySqlFormat3Cell], bytesName: String,
      shaName: String, allowNull: Boolean, allowEmpty: Boolean = false): Unit =
    (cell(row, Table, bytesName), cell(row, Table, shaName)) match
      case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) if allowNull => ()
      case (MySqlFormat3Cell.Binary(bytes), MySqlFormat3Cell.Binary(sha)) if (allowEmpty || bytes.length > 0) && sha.length == 32 =>
        if !digestMatches(bytes, sha) then invalid("raw operation " + bytesName + " SHA-256 differs")
      case _ => invalid("raw operation " + bytesName + "/" + shaName + " pair shape")

  private def digestMatches(value: MySqlFormat3Binary, expected: MySqlFormat3Binary): Boolean =
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone {
      new MySqlFormat3ReadBudget(SourceLimitProfile.of(1, 1, 1, 1, 1, 1664, 1664, 1), 1664)
    }
    budget.reserveTemporaryMemory(1664L, "raw operation SHA comparison workspace")
    var digest: MessageDigest = null
    var output: DataOutputStream = null
    var hash: Array[Byte] = null
    var primary: Throwable = null
    try
      digest = MessageDigest.getInstance("SHA-256")
      output = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))
      MySqlModeEnvelopeV2Census.binary(output, value)
      output.flush()
      hash = digest.digest()
      expected.equalsBytes(hash)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try
        if output != null then output.close()
      catch
        case cleanup: Throwable =>
          if primary == null then
            primary = cleanup
            throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)
      finally
        hash = null
        output = null
        digest = null
        try budget.releaseTemporaryMemory(1664L, "raw operation SHA comparison workspace release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def readCell(input: DataInputStream, totalBytes: Int, profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget, column: MySqlFormat3ColumnShape): MySqlFormat3Cell =
    val value = input.readUnsignedByte() match
      case 0 => MySqlFormat3Cell.SqlNull
      case 1 =>
        if column.kind != MySqlFormat3SqlKind.Text then invalid("raw operation cell tag differs from text shape")
        MySqlFormat3Cell.Text(readText(input, totalBytes, profile, budget, "raw operation text cell"))
      case 2 =>
        if column.kind != MySqlFormat3SqlKind.Binary then invalid("raw operation cell tag differs from binary shape")
        val length = readLength(input, totalBytes, profile)
        budget.reserveRetainedMemory(Math.addExact(length.toLong, 64L), "raw operation binary cell")
        val bytes = new Array[Byte](length)
        input.readFully(bytes)
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes, budget))
      case 3 =>
        if column.kind != MySqlFormat3SqlKind.Integer then invalid("raw operation cell tag differs from integer shape")
        MySqlFormat3Cell.SignedInteger(input.readLong())
      case 4 =>
        if column.kind != MySqlFormat3SqlKind.Floating then invalid("raw operation cell tag differs from DOUBLE shape")
        MySqlFormat3Cell.Floating(input.readLong())
      case 5 =>
        if column.kind != MySqlFormat3SqlKind.DateTime then invalid("raw operation cell tag differs from DATETIME shape")
        MySqlFormat3Cell.DateTime(readDateTime6(input))
      case _ => invalid("raw operation envelope has an unknown typed-cell tag")
    validateCell(value, column)
    value

  private def readText(input: DataInputStream, totalBytes: Int, profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget, label: String): String =
    val length = readLength(input, totalBytes, profile)
    val tempReservation = Math.addExact(Math.multiplyExact(length.toLong, 3L), 96L)
    budget.reserveTemporaryMemory(tempReservation, label + " UTF-8 decoder")
    val raw = new Array[Byte](length)
    var primary: Throwable = null
    try
      input.readFully(raw)
      val decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
      val result = decoder.decode(ByteBuffer.wrap(raw)).toString
      budget.reserveRetainedMemory(Math.addExact(64L,
        Math.max(length.toLong, Math.multiplyExact(result.length.toLong, 2L))), label + " retained string")
      result
    catch
      case failure: Throwable =>
        primary = failure
        if failure.isInstanceOf[EOFException] then throw new SQLDataException(label + " is truncated", failure)
        else throw failure
    finally
      Arrays.fill(raw, 0.toByte)
      try budget.releaseTemporaryMemory(tempReservation, label + " UTF-8 decoder release")
      catch
        case cleanup: Throwable =>
          if primary != null then
            if cleanup ne primary then primary.addSuppressed(cleanup)
          else throw cleanup

  private def readLength(input: DataInputStream, totalBytes: Int, profile: SourceLimitProfile): Int =
    val length = input.readInt()
    val maximum = Math.min(profile.maxRetainedBytes(), profile.maxSqlPacketBytes())
    if length < 0 || length.toLong > maximum || length.toLong > Int.MaxValue.toLong - 8L ||
      length.toLong > input.available().toLong || length.toLong > totalBytes.toLong then
      throw new SQLDataException("raw operation field length exceeds the exact envelope bounds")
    length

  private def writeCell(output: DataOutputStream, value: MySqlFormat3Cell): Unit =
    val recorded = MySqlModeEnvelopeV2Census.beginTyped(output, value)
    try
      value match
        case MySqlFormat3Cell.SqlNull => MySqlModeEnvelopeV2Census.byte(output, 0)
        case MySqlFormat3Cell.Text(text) => MySqlModeEnvelopeV2Census.byte(output, 1); MySqlModeEnvelopeV2Census.text(output, text)
        case MySqlFormat3Cell.Binary(bytes) => MySqlModeEnvelopeV2Census.byte(output, 2); MySqlModeEnvelopeV2Census.int(output, bytes.length); MySqlModeEnvelopeV2Census.binary(output, bytes)
        case MySqlFormat3Cell.SignedInteger(number) => MySqlModeEnvelopeV2Census.byte(output, 3); MySqlModeEnvelopeV2Census.long(output, number)
        case MySqlFormat3Cell.Floating(bits) => MySqlModeEnvelopeV2Census.byte(output, 4); MySqlModeEnvelopeV2Census.long(output, bits)
        case MySqlFormat3Cell.DateTime(value) =>
          MySqlModeEnvelopeV2Census.byte(output, 5)
          MySqlModeEnvelopeV2Census.int(output, value.getYear)
          MySqlModeEnvelopeV2Census.byte(output, value.getMonthValue)
          MySqlModeEnvelopeV2Census.byte(output, value.getDayOfMonth)
          MySqlModeEnvelopeV2Census.byte(output, value.getHour)
          MySqlModeEnvelopeV2Census.byte(output, value.getMinute)
          MySqlModeEnvelopeV2Census.byte(output, value.getSecond)
          MySqlModeEnvelopeV2Census.int(output, value.getNano / 1000)
    finally MySqlModeEnvelopeV2Census.endTyped(output, recorded)

  private def readDateTime6(input: DataInputStream): LocalDateTime =
    try
      val year = input.readInt()
      val month = input.readUnsignedByte()
      val day = input.readUnsignedByte()
      val hour = input.readUnsignedByte()
      val minute = input.readUnsignedByte()
      val second = input.readUnsignedByte()
      val micros = input.readInt()
      if year < 1000 || year > 9999 || micros < 0 || micros > 999999 then invalid("raw operation DATETIME(6) range")
      LocalDateTime.of(year, month, day, hour, minute, second, Math.multiplyExact(micros, 1000))
    catch
      case failure: Throwable => throw new SQLDataException("raw operation DATETIME(6) is invalid", failure)

  private def validateCell(value: MySqlFormat3Cell, column: MySqlFormat3ColumnShape): Unit =
    value match
      case MySqlFormat3Cell.SqlNull => if !column.nullable then invalid("unexpected NULL in " + column.name)
      case MySqlFormat3Cell.Text(text) =>
        if column.kind != MySqlFormat3SqlKind.Text || text == null ||
          column.maximumLength.exists(_ < text.codePointCount(0, text.length)) then invalid("text shape " + column.name)
        MySqlFormat3Utf8.byteLength(text)
      case MySqlFormat3Cell.Binary(bytes) =>
        if column.kind != MySqlFormat3SqlKind.Binary || bytes == null ||
          column.maximumLength.exists(_ < bytes.length) then invalid("binary shape " + column.name)
      case MySqlFormat3Cell.SignedInteger(number) =>
        if column.kind != MySqlFormat3SqlKind.Integer ||
          (column.sqlDeclaration.startsWith("TINYINT") && (number < Byte.MinValue || number > Byte.MaxValue)) ||
          (column.sqlDeclaration.startsWith("INT ") && (number < Int.MinValue || number > Int.MaxValue)) then
          invalid("integer shape " + column.name)
      case MySqlFormat3Cell.Floating(_) =>
        if column.kind != MySqlFormat3SqlKind.Floating then invalid("DOUBLE shape " + column.name)
      case MySqlFormat3Cell.DateTime(value) =>
        if column.kind != MySqlFormat3SqlKind.DateTime || value == null ||
          value.getYear < 1000 || value.getYear > 9999 || value.getNano % 1000 != 0 then invalid("DATETIME(6) shape " + column.name)

  private def uuidCell(value: MySqlFormat3Cell, label: String): UUID = value match
    case MySqlFormat3Cell.Binary(binary) if binary.length == 16 =>
      // 从已持有的 immutable16bytes读取；不复制数组，不留下80bytes scratch debt。
      var most = 0L
      var least = 0L
      var index = 0
      while index < 8 do
        most = (most << 8) | (binary.byteAt(index).toLong & 0xffL)
        least = (least << 8) | (binary.byteAt(index + 8).toLong & 0xffL)
        index += 1
      new UUID(most, least)
    case _ => throw new SQLDataException("raw operation " + label + " must be a 16-byte UUID")

  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 then invalid("raw operation frozen column " + name)
    row(index)

  private def invalid(reason: String): Nothing =
    throw new SQLDataException("MySQL " + reason)

private[mysql] final class MySqlModeOperationEffect private[mysql] (
    val phase: MySqlOperationEffectPhase,
    val preimage: MySqlRecoveryCanonicalBytes,
    val target: MySqlRecoveryCanonicalBytes,
    val fingerprint: MySqlRecoveryCanonicalBytes,
    val preimageRawOperationEnvelopes: Vector[MySqlRecoveryOperationRawEnvelope],
    val rawOperationEnvelopes: Vector[MySqlRecoveryOperationRawEnvelope],
    private val plan: MySqlOwnedRepositoryOperationPlan,
    private val operation: MySqlRepositoryOperation,
    private val contextIdentity: MySqlRepositoryOperationContextIdentity,
    private val locatorRevision: Long,
    private val profile: SourceLimitProfile,
    private val expectedPreimage: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
    private val expectedTarget: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot,
    private val retainedOperationContext: MySqlOperationContext
):
  def fingerprintSha256: Vector[Byte] = fingerprint.sha256
  def fingerprintHex: String = fingerprint.hexSha256

  /** 从此阶段的固定 full20 前像/目标重新导出同一 operation context；不读取时钟或数据库。 */
  private[mysql] def exactOperationContext: MySqlOperationContext =
    retainedOperationContext

  /** 此阶段的完整 context 在 projection 后构造，以免 effects hash 回指自身。 */
  def requireOperationContext(context: MySqlOperationContext): Unit =
    if context == null then throw new IllegalArgumentException("exact MySQL operation context is required")
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    val operationUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    if context.bootUuid != contextIdentity.tag.bootId || context.bindingUuid != request.binding.bindingUuid ||
      context.operationUuid != operationUuid || context.writerEpoch != contextIdentity.writerEpoch then
      throw new SQLDataException("operation context differs from this fixed mode phase")
    if context.expectedEffects.size != 1 ||
      context.expectedEffects.head.targetIdentity != ("pod/" + request.podUuid.toString) ||
      context.expectedEffects.head.expectedVersion != request.expectedStateVersion ||
      context.expectedEffects.head.desiredEffectFingerprintSha256 != fingerprint.sha256 then
      throw new SQLDataException("operation context does not bind the exact pod effect fingerprint")
    val currentUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    val envelope = rawOperationEnvelopes.find(_.operationUuid == currentUuid)
      .getOrElse(throw new SQLDataException("current phase operation raw envelope is missing"))
    withExpectedComparison(expectedTarget) { root =>
      MySqlModeOperationEffectCodec.requireSuppliedOperationContext(context, envelope.row, profile, root)
    }

  /** 完整二十关系先逐值核对，再独立核受影响图 projection 和原始 O 封套。 */
  def verifyReadback(actual: MySqlFormat3FullSnapshot): Unit =
    val expected = requireLegacyImage(expectedTarget)
    MySqlModeOperationEffectCodec.withGraphComparison(expected) {
      if actual == null || !MySqlModeOperationEffectCodec.sameCompleteImage(expected, actual) then
        throw new SQLDataException("full format3 readback differs from the fixed mode target")
      val projected = MySqlModeOperationEffectCodec.projectImage(actual, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Target, MySqlModeOperationEffectCodec.projectionVersion(target))
      if !target.exactlyEquals(projected) then throw new SQLDataException("mode target projection readback differs")
      requireRawOperationEnvelopes(actual.relation("fp_recovery_operation").rows, rawOperationEnvelopes, None)
    }

  def verifyPreimage(actual: MySqlFormat3FullSnapshot): Unit =
    val expected = requireLegacyImage(expectedPreimage)
    MySqlModeOperationEffectCodec.withGraphComparison(expected) {
      if actual == null || !MySqlModeOperationEffectCodec.sameCompleteImage(expected, actual) then
        throw new SQLDataException("full format3 preimage differs from its exact fixed snapshot")
      val projected = MySqlModeOperationEffectCodec.projectImage(actual, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Before, MySqlModeOperationEffectCodec.projectionVersion(preimage))
      if !preimage.exactlyEquals(projected) then throw new SQLDataException("mode preimage projection differs")
      requireRawOperationEnvelopes(actual.relation("fp_recovery_operation").rows, preimageRawOperationEnvelopes, None)
    }

  private def requireLegacyImage(image: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot): MySqlFormat3FullSnapshot =
    image match
      case legacy: MySqlFormat3FullSnapshot => legacy
      case _: MySqlScriptSchemaSnapshot =>
        throw new SQLDataException("Full26 mode effect requires its complete deployment receipt readback")

  private def withExpectedComparison(image: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot)
      (body: MySqlFormat3ReadBudget => Unit): Unit = image match
    case legacy: MySqlFormat3FullSnapshot =>
      MySqlModeOperationEffectCodec.withGraphComparison(legacy) { body(legacy.allocationBudget) }
    case deployment: MySqlScriptSchemaSnapshot =>
      val root = deployment.allocationBudget
      MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, root)
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { body(root) }

  private[mysql] def verifyDeploymentReceiptPreimage(actual: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget): Unit =
    verifyDeploymentReceiptImage(actual, expectedPreimage, root, MySqlOperationEffectImage.Before,
      preimage, preimageRawOperationEnvelopes)

  private[mysql] def verifyDeploymentReceiptReadback(actual: MySqlScriptSchemaSnapshot,
      root: MySqlFormat3ReadBudget): Unit =
    verifyDeploymentReceiptImage(actual, expectedTarget, root, MySqlOperationEffectImage.Target,
      target, rawOperationEnvelopes)

  private def verifyDeploymentReceiptImage(actual: MySqlScriptSchemaSnapshot,
      expected: MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      image: MySqlOperationEffectImage, canonical: MySqlRecoveryCanonicalBytes,
      envelopes: Vector[MySqlRecoveryOperationRawEnvelope]): Unit = expected match
    case _: MySqlFormat3FullSnapshot =>
      throw new SQLDataException("legacy mode effect cannot accept a Full26 readback")
    case deployment: MySqlScriptSchemaSnapshot =>
      MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, root)
      MySqlFormat3QueryProjection.requireDeploymentSource(actual, deployment.inputs, root)
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        if !deployment.exactlyEquals(actual) then
          throw new SQLDataException("complete Full26 mode image differs from its fixed phase")
        val projected = MySqlModeOperationEffectCodec.projectImage(actual, deployment.inputs, root,
          plan, operation, contextIdentity, locatorRevision, phase, image)
        if !canonical.exactlyEquals(projected) then
          throw new SQLDataException("Full26 mode projection differs from its fixed phase")
        requireRawOperationEnvelopes(actual.relation("fp_recovery_operation").rows, envelopes, Some(actual.inputs))
      }

  private def requireRawOperationEnvelopes(operationRows: Vector[Vector[MySqlFormat3Cell]],
      expected: Vector[MySqlRecoveryOperationRawEnvelope],
      deploymentInputs: Option[MySqlScriptSchemaInputs]): Unit =
    val rows = operationRows.filter(row =>
      MySqlModeOperationEffectCodec.uuidEquals(MySqlModeOperationEffectCodec.value(
        row, "fp_recovery_operation", "binding_uuid"), MySqlModeOperationEffectCodec.modeRequest(operation).binding.bindingUuid))
    expected.foreach { envelope =>
      envelope.requireInputs(deploymentInputs)
      val row = rows.find(value => MySqlModeOperationEffectCodec.rowOperationUuid(value) == envelope.operationUuid)
        .getOrElse(throw new SQLDataException("a fixed raw operation row is missing from full readback"))
      if !envelope.exactlyMatches(row, profile) then
        throw new SQLDataException("raw full operation envelope differs from its fixed bytes")
    }

  override def toString: String =
    "MySqlModeOperationEffect[phase=" + phase + ",fingerprint=" + fingerprintHex + "]"

/** 单个阶段在 targets 已确定后的一次性规范值；不持有 JDBC 或提交权限。 */
private[mysql] final case class MySqlModePhaseCanonical(
    preimage: MySqlRecoveryCanonicalBytes,
    target: MySqlRecoveryCanonicalBytes,
    scope: MySqlRecoveryCanonicalBytes,
    fingerprint: MySqlRecoveryCanonicalBytes,
    operationContext: MySqlOperationContext,
    operationContextBytes: MySqlRecoveryCanonicalBytes
)

private[mysql] object MySqlModeOperationEffectCodec:
  private[mysql] type Image = MySqlFormat3FullSnapshot | MySqlScriptSchemaSnapshot

  private[mysql] def originalImageRoot(image: Image, plan: MySqlOwnedRepositoryOperationPlan): MySqlFormat3ReadBudget =
    val root = plan.retainedHandle.map(_.issuer.rootForOriginal(plan)).getOrElse(image.allocationBudget)
    image match
      case deployment: MySqlScriptSchemaSnapshot =>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs, root)
      case legacy: MySqlFormat3FullSnapshot =>
        if legacy.allocationBudget ne root then fail("legacy mode image lost original plan root")
    root

  // 封闭的两种完整图像共享行规则；不创建子集或迁移 native witness 的编码家族。
  extension (image: Image)
    private[mysql] def schema: String = image match
      case value: MySqlFormat3FullSnapshot => value.schema
      case value: MySqlScriptSchemaSnapshot => value.schema
    private[mysql] def connection: MySqlFormat3ConnectionObservation = image match
      case value: MySqlFormat3FullSnapshot => value.connection
      case value: MySqlScriptSchemaSnapshot => value.connection
    private[mysql] def observationScope: MySqlFormat3ObservationScope = image match
      case value: MySqlFormat3FullSnapshot => value.observationScope
      case value: MySqlScriptSchemaSnapshot => value.observationScope
    private[mysql] def maxAllowedPacketBytes: Long = image match
      case value: MySqlFormat3FullSnapshot => value.maxAllowedPacketBytes
      case value: MySqlScriptSchemaSnapshot => value.maxAllowedPacketBytes
    private[mysql] def limitProfile: SourceLimitProfile = image match
      case value: MySqlFormat3FullSnapshot => value.limitProfile
      case value: MySqlScriptSchemaSnapshot => value.inputs.sourceLimitProfile
    private[mysql] def allocationBudget: MySqlFormat3ReadBudget = image match
      case value: MySqlFormat3FullSnapshot => value.allocationBudget
      case value: MySqlScriptSchemaSnapshot => value.allocationBudget
    private[mysql] def relations: Vector[MySqlFormat3RelationSnapshot] = image match
      case value: MySqlFormat3FullSnapshot => value.relations
      case value: MySqlScriptSchemaSnapshot => value.relations
    private[mysql] def rowCount: Long = image match
      case value: MySqlFormat3FullSnapshot => value.rowCount
      case value: MySqlScriptSchemaSnapshot => value.rowCount
    private[mysql] def relation(table: String): MySqlFormat3RelationSnapshot = image match
      case value: MySqlFormat3FullSnapshot => value.relation(table)
      case value: MySqlScriptSchemaSnapshot => value.relation(table)
    private[mysql] def shapes: Vector[MySqlFormat3RelationShape] = image match
      case _: MySqlFormat3FullSnapshot => MySqlFormat3RelationShapes.canonicalOrder
      case value: MySqlScriptSchemaSnapshot => value.inputs.canonicalOrder
  private val FingerprintEncoding = "FP_MYSQL_MODE_EFFECT_FINGERPRINT_BE_V1"
  private val FingerprintDomain = "com.wi.floatingpod.mysql-operation-effect-fingerprint/v1"
  private val ManifestTable = "fp_schema_manifest"
  private val BindingTable = "physical_recovery_binding"
  private val BootstrapIntentTable = "binding_bootstrap_intent"
  private val WriterTable = "fp_runtime_writer"
  private val DomainTable = "floating_pod_state"
  private val LocatorTable = "physical_recovery_locator"
  private val FootprintTable = "physical_recovery_footprint"
  private val ChunkTable = "physical_recovery_footprint_chunk"
  private val IntentTable = "physical_recovery_intent"
  private val ObligationTable = "physical_recovery_obligation"
  private val CompletionTable = "fp_lifecycle_completion"
  private val MoveTable = "physical_footprint_transfer"
  private val OperationTable = "fp_recovery_operation"
  private val ReservationTable = "fp_recovery_reservation"
  private val SourceUnitTable = "physical_source_unit"
  private val SourceForestTable = "physical_source_forest"
  private val SourceTransferTable = "physical_source_transfer"
  private val SourceWriteTable = "physical_source_write"
  private val SourceObligationTable = "physical_source_obligation"
  private[mysql] final case class Request(binding: PhysicalRecoveryBinding, podUuid: UUID, owner: UUID,
      expectedStateVersion: Long, attemptUuid: UUID, targetMode: PodOperatingMode, actualPose: FloatingPodPose)

  /** 只读实际canonical前缀；后续完整重编码仍校验全部bytes，绝不试错多版本。 */
  private[mysql] def projectionVersion(bytes: MySqlFormat3Binary): MySqlModeProjectionVersion =
    if bytes == null || bytes.length < 5 then fail("missing projection version")
    def matches(version: MySqlModeProjectionVersion): Boolean =
      val domain = version.domain
      bytes.length >= domain.length + 5 &&
        bytes.byteAt(0) == 0 && bytes.byteAt(1) == 0 && bytes.byteAt(2) == 0 &&
        (bytes.byteAt(3) & 255) == domain.length &&
        domain.indices.forall(i => (bytes.byteAt(i + 4) & 255) == domain.charAt(i).toInt) &&
        (bytes.byteAt(domain.length + 4) & 255) == version.number
    if matches(MySqlModeProjectionVersion.V1) then MySqlModeProjectionVersion.V1
    else if matches(MySqlModeProjectionVersion.V2) then MySqlModeProjectionVersion.V2
    else fail("unknown or inconsistent projection domain/version")

  private[mysql] def projectionVersion(value: MySqlRecoveryCanonicalBytes): MySqlModeProjectionVersion =
    val version = projectionVersion(value.borrowedBinary)
    if value.encoding != version.encoding then fail("projection encoding/domain differs")
    version

  private[mysql] def workflowProjectionVersion(plan: MySqlOwnedRepositoryOperationPlan,
      reserved: Image, executed: Image): MySqlModeProjectionVersion =
    val versions = Vector((reserved, plan.reserveOperationUuid), (executed, plan.executeOperationUuid)).flatMap { (snapshot, id) =>
      val rows = snapshot.relation(OperationTable).rows.filter(r => uuidEquals(value(r, OperationTable, "operation_uuid"), id))
      if rows.size != 1 then fail("original projection operation missing or ambiguous")
      Vector("preimage_bytes", "target_bytes").map { column =>
        value(rows.head, OperationTable, column) match
          case MySqlFormat3Cell.Binary(bytes) => projectionVersion(bytes)
          case _ => fail("original projection payload missing")
      }
    }
    if versions.distinct.size != 1 then fail("workflow projection versions differ")
    versions.head

  private[mysql] def allocationOwner(root: MySqlFormat3ReadBudget): MySqlFormat3ReadBudget =
    val owner = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(root != null && !root.isClosed && !root.isScopedReadBudget && !owner.isClosed &&
      (owner.profile eq root.profile) && ((owner eq root) || owner.isCanonicalComparisonChildOf(root)),
      "mode allocation requires original root or its exact comparison child")
    owner

  private[mysql] def sameCompleteImage(expected: Image, actual: Image): Boolean =
    actual != null && expected.schema == actual.schema && expected.connection == actual.connection &&
      expected.observationScope == actual.observationScope && expected.maxAllowedPacketBytes == actual.maxAllowedPacketBytes &&
      expected.limitProfile == actual.limitProfile && expected.relations == actual.relations &&
      ((expected, actual) match
        case (left: MySqlFormat3FullSnapshot, right: MySqlFormat3FullSnapshot) =>
          MySqlFormat3SnapshotCanonical.fingerprint(left, allocationOwner(left.allocationBudget)).sha256 ==
            MySqlFormat3SnapshotCanonical.fingerprint(right, allocationOwner(left.allocationBudget)).sha256
        case (left: MySqlScriptSchemaSnapshot, right: MySqlScriptSchemaSnapshot) =>
          (left.inputs eq right.inputs) && left.canonicalSha256 == right.canonicalSha256
        case _ => false)

  private def sameSnapshotContext(left: Image, right: Image): Unit =
    if left == null || right == null || left.schema != right.schema ||
      left.connection != right.connection || left.observationScope != right.observationScope ||
      left.maxAllowedPacketBytes != right.maxAllowedPacketBytes || left.limitProfile != right.limitProfile ||
      ((left, right) match
        case (_: MySqlFormat3FullSnapshot, _: MySqlFormat3FullSnapshot) => false
        case (a: MySqlScriptSchemaSnapshot, b: MySqlScriptSchemaSnapshot) => a.inputs ne b.inputs
        case _ => true) then
      fail("full20 images must keep the exact connection, scope, schema, packet observation and limit profile")

  /** 固定验证程序的Map/key/vector/zip元数据先按实际完整图行宽计上界。
    * 原typed cells/opaque payload只读，不复制；比较产生的canonical bytes在同ledger child内计费。
    */
  private[mysql] def withGraphComparison(snapshot: Image)(body: => Unit): Unit =
    if snapshot == null then throw new IllegalArgumentException("actual full20 snapshot required for mode comparison")
    withGraphComparisonOnRoot(snapshot, snapshot.allocationBudget)(body)

  private def withGraphComparisonOnRoot(snapshot: Image, root: MySqlFormat3ReadBudget,
      chargeStructuresToComparison: Boolean = false)(body: => Unit): Unit =
    if snapshot == null || root == null || root.isClosed || (snapshot.limitProfile ne root.profile) then
      throw new IllegalArgumentException("open exact-profile mode comparison root required")
    var structures = 4096L
    var relationIndex = 0
    while relationIndex < snapshot.relations.size do
      val rows = snapshot.relations(relationIndex).rows
      var rowIndex = 0
      while rowIndex < rows.size do
        structures = Math.addExact(structures,
          Math.addExact(256L, Math.multiplyExact(rows(rowIndex).size.toLong, 96L)))
        rowIndex += 1
      relationIndex += 1
    def runWithStructures(budget: MySqlFormat3ReadBudget)(run: => Unit): Unit =
      budget.reserveTemporaryMemory(structures, "mode graph comparison structures")
      var primary: Throwable = null
      try run
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try budget.releaseTemporaryMemory(structures, "mode graph comparison structures release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)
    if chargeStructuresToComparison then
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val comparison = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        runWithStructures(comparison)(body)
      }
    else
      root.reserveTemporaryMemory(structures, "mode graph comparison structures")
      var primary: Throwable = null
      try MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { body }
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try root.releaseTemporaryMemory(structures, "mode graph comparison structures release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)

  def buildWorkflow(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      reservePreimage: Image, reserveTarget: Image,
      executeTarget: Image): Vector[MySqlModeOperationEffect] =
    if reservePreimage == null then throw new IllegalArgumentException("actual full20 preimage required")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(reservePreimage.allocationBudget) {
      buildWorkflowInternal(plan, operation, contextIdentity, locatorRevision, reservePreimage,
        reserveTarget, executeTarget, None, None, None, None)
    }

  /** 只供同materializer已有canonical值复用；仍以最终真实full20重新编码逐byte核验。
    * 不接SQL/Main authority，不根据caller boolean跳过验证，也不签提交确认。
    */
  private[mysql] def buildMaterializedWorkflow(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, reservePreimage: Image,
      reserveTarget: Image, executeTarget: Image,
      reserveCanonical: MySqlModePhaseCanonical, executeCanonical: MySqlModePhaseCanonical,
      existingReserveEnvelope: MySqlRecoveryOperationRawEnvelope, draft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlModeOperationEffect] =
    if reserveCanonical == null || executeCanonical == null || existingReserveEnvelope == null then
      throw new IllegalArgumentException("same-materialization canonical values and raw reserve envelope required")
    buildWorkflowInternal(plan, operation, contextIdentity, locatorRevision, reservePreimage,
      reserveTarget, executeTarget, Some(reserveCanonical), Some(executeCanonical), Some(existingReserveEnvelope), draft)

  private def buildWorkflowInternal(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      reservePreimage: Image, reserveTarget: Image,
      executeTarget: Image, reserveCanonical: Option[MySqlModePhaseCanonical],
      executeCanonical: Option[MySqlModePhaseCanonical],
      existingReserveEnvelope: Option[MySqlRecoveryOperationRawEnvelope], draft: Option[MySqlModeOriginalWitnessDraft]): Vector[MySqlModeOperationEffect] =
    validatePlan(plan, operation, contextIdentity, locatorRevision, reservePreimage)
    sameSnapshotContext(reservePreimage, reserveTarget)
    sameSnapshotContext(reserveTarget, executeTarget)
    val root = originalImageRoot(reservePreimage, plan)
    val version = workflowProjectionVersion(plan, reserveTarget, executeTarget)
    val allocation = allocationOwner(root)
    if reserveCanonical.nonEmpty && ((reserveTarget.allocationBudget ne root) || (executeTarget.allocationBudget ne root)) then
      fail("mode effects must retain the exact original workflow allocation root")
    withGraphComparisonOnRoot(reservePreimage, root) {
      validatePreimage(reservePreimage, operation, contextIdentity, locatorRevision)
      validateReserveShape(plan, operation, reservePreimage, reserveTarget)
      validateExecuteShape(plan, operation, reserveTarget, executeTarget)
    }
    val rowCapacity = Math.addExact(Math.addExact(reservePreimage.relation(OperationTable).rows.size,
      reserveTarget.relation(OperationTable).rows.size), executeTarget.relation(OperationTable).rows.size)
    allocation.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(rowCapacity.toLong, 64L)),
      "mode workflow raw-envelope reference cache")
    // 唯一局部cache按已锁定三个完整图的实际O行数上界先保留；无全局registry/私有cap。
    val envelopes = new MySqlModeRawEnvelopeConstruction(allocation, rowCapacity, reservePreimage.relation(OperationTable).rows)
    draft.foreach(_.retainRawConstruction(envelopes))
    envelopes.initialize()
    existingReserveEnvelope.foreach { envelope =>
      if !reserveTarget.relation(OperationTable).rows.exists(row => row eq envelope.row) then
        fail("same-materialization reserve envelope has no exact final row reference")
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        envelope.requireInputs(inputsForImage(reservePreimage, root))
        if !envelope.exactlyMatches(envelope.row, reservePreimage.limitProfile) then
          fail("same-materialization reserve envelope differs from its actual immutable row")
      }
      envelopes.retain(envelope)
    }
    val reserve = buildPhase(plan, operation, contextIdentity, locatorRevision,
      MySqlOperationEffectPhase.Reserve, reservePreimage, reserveTarget, reserveCanonical, envelopes, version, root)
    val execute = buildPhase(plan, operation, contextIdentity, locatorRevision,
      MySqlOperationEffectPhase.Execute, reserveTarget, executeTarget, executeCanonical, envelopes, version, root)
    withGraphComparisonOnRoot(reserveTarget, root) {
      requireEnvelopeSetExactly(reserve.rawOperationEnvelopes, execute.preimageRawOperationEnvelopes,
        reservePreimage.limitProfile, "all historical closure O rows must remain raw exact at execute preimage")
      validatePlannedExecuteOperation(plan, operation, contextIdentity, locatorRevision,
        reserveTarget, execute.preimage, execute.target, execute.fingerprint, reservePreimage.limitProfile)
    }
    Vector(reserve, execute)

  /** 共享全部原phase规则；只省不逃逸的RAW封套字节副本。 */
  private[mysql] def requireHistoricalPhaseGraphs(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity, locator: Long,
      initial: Image, reserved: Image, executed: Image,
      reserve: MySqlModePhaseCanonical, execute: MySqlModePhaseCanonical): Unit =
    val root = initial.allocationBudget
    require(allocationOwner(root).isCanonicalComparisonChildOf(root), "historical phase validation requires original comparison child")
    validatePlan(plan, operation, context, locator, initial)
    sameSnapshotContext(initial, reserved); sameSnapshotContext(reserved, executed)
    require(workflowProjectionVersion(plan, reserved, executed) == projectionVersion(reserve.preimage) &&
      projectionVersion(reserve.preimage) == projectionVersion(execute.preimage), "historical workflow versions differ")
    withGraphComparisonOnRoot(initial, root) {
      validatePreimage(initial, operation, context, locator)
      validateReserveShape(plan, operation, initial, reserved)
      validateExecuteShape(plan, operation, reserved, executed)
      requireRetainedCanonicalPhase(plan, operation, context, locator, MySqlOperationEffectPhase.Reserve, initial, reserved, reserve, root)
      requireRetainedCanonicalPhase(plan, operation, context, locator, MySqlOperationEffectPhase.Execute, reserved, executed, execute, root)
      validateCurrentPhaseOperation(plan, operation, context, locator, MySqlOperationEffectPhase.Reserve, reserved,
        reserve.preimage, reserve.target, reserve.scope, reserve.fingerprint, root.profile)
      validateCurrentPhaseOperation(plan, operation, context, locator, MySqlOperationEffectPhase.Execute, executed,
        execute.preimage, execute.target, execute.scope, execute.fingerprint, root.profile)
      validatePlannedExecuteOperation(plan, operation, context, locator, reserved,
        execute.preimage, execute.target, execute.fingerprint, root.profile)
      val request = modeRequest(operation)
      // Rp.target与E.before使用同一完整reserved图；closure的原行/唯一ID与所有payload逐项核，绝不删历史。
      Vector(initial, reserved, executed).foreach { image =>
        val rows = modeClosureOperationRows(image, request.binding.bindingUuid, request.podUuid)
        require(rows.map(rowOperationUuid).distinct.size == rows.size, "duplicate historical closure operation")
        rows.foreach(row => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, allocationOwner(root), inputsForImage(image, root)))
      }
    }

  /** H3 Full26 history-only validation with separate original authority and exact retained DATA child. */
  private[mysql] def requireHistoricalPhaseGraphs(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity, locator: Long,
      initial: Image, reserved: Image, executed: Image,
      reserve: MySqlModePhaseCanonical, execute: MySqlModePhaseCanonical,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && dataBudget != null && !dataBudget.isClosed &&
      (dataBudget.profile eq root.profile) && dataBudget.isCanonicalChildOf(root) && dataBudget.sharesAllocationLedger(root),
      "H3 Full26 mode validation requires exact same-ledger DATA child and original authority root")
    val images = Vector(initial, reserved, executed)
    require(images.forall(_.isInstanceOf[MySqlScriptSchemaSnapshot]) &&
      images.forall(_.allocationBudget eq dataBudget), "H3 Full26 mode phase graphs must retain the exact DATA child")
    val deploymentImages = images.map(_.asInstanceOf[MySqlScriptSchemaSnapshot])
    val inputs = deploymentImages.head.inputs
    require(context != null && context.schemaFormat == 5 &&
      deploymentImages.forall(_.inputs eq inputs), "H3 historical Full26 mode requires exact original deployment inputs")
    deploymentImages.foreach(image => MySqlFormat3QueryProjection.requireDeploymentSource(image, inputs, root))
    validatePlan(plan, operation, context, locator, initial, Some(root))
    sameSnapshotContext(initial, reserved); sameSnapshotContext(reserved, executed)
    require(workflowProjectionVersion(plan, reserved, executed) == projectionVersion(reserve.preimage) &&
      projectionVersion(reserve.preimage) == projectionVersion(execute.preimage), "historical workflow versions differ")
    withGraphComparisonOnRoot(initial, root, chargeStructuresToComparison = true) {
      val comparison = MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      require(comparison.isCanonicalComparisonChildOf(root), "H3 history comparison must use an original-root lexical child")
      validatePreimage(initial, operation, context, locator)
      validateReserveShape(plan, operation, initial, reserved)
      validateExecuteShape(plan, operation, reserved, executed)
      requireRetainedCanonicalPhase(plan, operation, context, locator, MySqlOperationEffectPhase.Reserve,
        initial, reserved, reserve, root, Some(root), chargeStructuresToComparison = true)
      requireRetainedCanonicalPhase(plan, operation, context, locator, MySqlOperationEffectPhase.Execute,
        reserved, executed, execute, root, Some(root), chargeStructuresToComparison = true)
      validateCurrentPhaseOperation(plan, operation, context, locator, MySqlOperationEffectPhase.Reserve, reserved,
        reserve.preimage, reserve.target, reserve.scope, reserve.fingerprint, root.profile, Some(comparison))
      validateCurrentPhaseOperation(plan, operation, context, locator, MySqlOperationEffectPhase.Execute, executed,
        execute.preimage, execute.target, execute.scope, execute.fingerprint, root.profile, Some(comparison))
      validatePlannedExecuteOperation(plan, operation, context, locator, reserved,
        execute.preimage, execute.target, execute.fingerprint, root.profile, Some(comparison))
      val request = modeRequest(operation)
      Vector(initial, reserved, executed).foreach { image =>
        val rows = modeClosureOperationRows(image, request.binding.bindingUuid, request.podUuid)
        require(rows.map(rowOperationUuid).distinct.size == rows.size, "duplicate historical closure operation")
        rows.foreach(row => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, comparison, Some(inputs)))
      }
    }

  private def buildPhase(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, before: Image,
      target: Image, existingCanonical: Option[MySqlModePhaseCanonical],
      cache: MySqlModeRawEnvelopeConstruction, version: MySqlModeProjectionVersion,
      root: MySqlFormat3ReadBudget): MySqlModeOperationEffect =
    val allocation = allocationOwner(root)
    val phaseCanonical = existingCanonical match
      case None => canonicalPhase(plan, operation, contextIdentity, locatorRevision, phase, before, target, version)
      case Some(retained) =>
        withGraphComparisonOnRoot(target, root) {
          val actual = canonicalPhase(plan, operation, contextIdentity, locatorRevision, phase, before, target, version)
          if !retained.preimage.exactlyEquals(actual.preimage) || !retained.target.exactlyEquals(actual.target) ||
            !retained.scope.exactlyEquals(actual.scope) || !retained.fingerprint.exactlyEquals(actual.fingerprint) ||
            !retained.operationContextBytes.exactlyEquals(actual.operationContextBytes) ||
            retained.operationContext != actual.operationContext then fail("retained canonical phase differs from final full20 graph")
        }
        retained
    val preimage = phaseCanonical.preimage
    val projectedTarget = phaseCanonical.target
    val request = modeRequest(operation)
    def encodeRows(snapshot: Image): Vector[MySqlRecoveryOperationRawEnvelope] =
      allocation.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(snapshot.relation(OperationTable).rows.size.toLong, 64L)),
        "mode closure O row filter references before allocation")
      val rows = modeClosureOperationRows(snapshot, request.binding.bindingUuid, request.podUuid)
      allocation.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(rows.size.toLong, 64L)),
        "mode immutable phase raw-envelope references")
      rows.map { row =>
        cache.findSameRow(row, inputsForImage(snapshot, root)) match
          case Some(existing) =>
            MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
              existing.requireInputs(inputsForImage(snapshot, root))
              if !existing.exactlyMatches(row, snapshot.limitProfile) then
                fail("same immutable row reference differs from retained RAW bytes")
            }
            existing
          case None =>
            val metadata = Math.addExact(2048L, Math.multiplyExact(row.size.toLong, 128L))
            root.reserveTemporaryMemory(metadata, "mode raw-envelope row encoding metadata")
            var primary: Throwable = null
            val encoded =
              try
                val actual = if !allocation.isScopedReadBudget && cache.isOriginalHistoricalRow(row) then
                  MySqlRecoveryOperationRawEnvelopeCodec.encodeHistorical(row, snapshot.limitProfile, cache.originalRows, inputsForImage(snapshot, root))
                else MySqlRecoveryOperationRawEnvelopeCodec.encode(row, snapshot.limitProfile, inputsForImage(snapshot, root))
                cache.retain(actual)
                actual
              catch
                case cause: Throwable =>
                  primary = cause
                  throw cause
              finally
                try root.releaseTemporaryMemory(metadata, "mode raw-envelope row encoding metadata release")
                catch
                  case cleanup: Throwable =>
                    if primary == null then throw cleanup
                    else if (cleanup ne primary) && !primary.getSuppressed.contains(cleanup) then primary.addSuppressed(cleanup)
            encoded
      }
    val beforeEnvelopes = encodeRows(before)
    cache.retainRows(phase, MySqlOperationEffectImage.Before, beforeEnvelopes)
    val targetEnvelopes = encodeRows(target)
    cache.retainRows(phase, MySqlOperationEffectImage.Target, targetEnvelopes)
    withGraphComparisonOnRoot(target, root) {
      validateCurrentPhaseOperation(plan, operation, contextIdentity, locatorRevision, phase, target,
        preimage, projectedTarget, phaseCanonical.scope, phaseCanonical.fingerprint, before.limitProfile)
    }
    allocation.reserveRetainedMemory(256L, "mode actual phase effect wrapper before allocation")
    val effect = new MySqlModeOperationEffect(phase, preimage, projectedTarget, phaseCanonical.fingerprint, beforeEnvelopes, targetEnvelopes,
      plan, operation, contextIdentity, locatorRevision, before.limitProfile, before, target, phaseCanonical.operationContext)
    cache.retainEffect(effect)
    effect

  /** 先从完整 before/target 生成 phase base，再导出 context；不读时钟、不签发任何 owner。 */
  private[mysql] def canonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: Image,
      target: Image, version: MySqlModeProjectionVersion = MySqlModeProjectionVersion.V2): MySqlModePhaseCanonical =
    if plan == null || operation == null || contextIdentity == null || before == null || target == null || phase == null then
      throw new IllegalArgumentException("complete fixed mode phase inputs are required")
    before match
      case deployment: MySqlScriptSchemaSnapshot =>
        target match
          case deploymentTarget: MySqlScriptSchemaSnapshot =>
            if version != MySqlModeProjectionVersion.V2 then fail("Full26 mode phase requires original V2 projection framing")
            return canonicalDeploymentReceiptPhase(plan, operation, contextIdentity, locatorRevision,
              phase, deployment, deploymentTarget, originalImageRoot(before, plan))
          case _: MySqlFormat3FullSnapshot => fail("mode phase cannot cross complete image families")
      case _: MySqlFormat3FullSnapshot => ()
    validatePlan(plan, operation, contextIdentity, locatorRevision, before)
    sameSnapshotContext(before, target)
    val preimage = projectImage(before, plan, operation, contextIdentity, locatorRevision,
      phase, MySqlOperationEffectImage.Before, version)
    val projectedTarget = projectImage(target, plan, operation, contextIdentity, locatorRevision,
      phase, MySqlOperationEffectImage.Target, version)
    finishCanonicalPhase(plan, operation, contextIdentity, locatorRevision, phase,
      before.limitProfile, before.relation(LocatorTable).rows, preimage, projectedTarget)

  /** schema5 的 phase 使用完整26关系投影；原操作、资源版本与指纹编码保持唯一。 */
  private[mysql] def canonicalDeploymentReceiptPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget): MySqlModePhaseCanonical =
    canonicalDeploymentReceiptPhaseInternal(plan, operation, contextIdentity, locatorRevision, phase,
      before, target, root, None)

  /** H3 history-only overload; source authority stays at root while new canonical DATA uses its registered child. */
  private[mysql] def canonicalDeploymentReceiptPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlModePhaseCanonical =
    canonicalDeploymentReceiptPhaseInternal(plan, operation, contextIdentity, locatorRevision, phase,
      before, target, root, Some(dataBudget))

  private def canonicalDeploymentReceiptPhaseInternal(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, root: MySqlFormat3ReadBudget,
      outputBudget: Option[MySqlFormat3ReadBudget]): MySqlModePhaseCanonical =
    if before == null || target == null || root == null || phase == null then
      throw new IllegalArgumentException("complete Full26 mode phase inputs and original root required")
    MySqlFormat3QueryProjection.requireDeploymentSource(before, before.inputs, root)
    MySqlFormat3QueryProjection.requireDeploymentSource(target, before.inputs, root)
    outputBudget.foreach { budget =>
      require(!budget.isClosed && (budget.profile eq root.profile) && budget.isCanonicalChildOf(root) &&
        budget.sharesAllocationLedger(root), "H3 mode canonical output requires exact same-ledger DATA child")
    }
    if before.schema != target.schema || before.connection != target.connection ||
      before.observationScope != target.observationScope ||
      before.maxAllowedPacketBytes != target.maxAllowedPacketBytes ||
      (before.inputs ne target.inputs) then
      fail("Full26 mode phase images differ from their exact locked source identity")
    val preimage = outputBudget match
      case Some(budget) => projectImage(before, before.inputs, root, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Before, budget)
      case None => projectImage(before, before.inputs, root, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Before)
    val projectedTarget = outputBudget match
      case Some(budget) => projectImage(target, before.inputs, root, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Target, budget)
      case None => projectImage(target, before.inputs, root, plan, operation, contextIdentity,
        locatorRevision, phase, MySqlOperationEffectImage.Target)
    outputBudget match
      case Some(budget) => MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
        finishCanonicalPhase(plan, operation, contextIdentity, locatorRevision, phase,
          before.inputs.sourceLimitProfile, before.relation(LocatorTable).rows, preimage, projectedTarget)
      }
      case None => finishCanonicalPhase(plan, operation, contextIdentity, locatorRevision, phase,
        before.inputs.sourceLimitProfile, before.relation(LocatorTable).rows, preimage, projectedTarget)

  private def finishCanonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, profile: SourceLimitProfile,
      locators: Vector[Vector[MySqlFormat3Cell]], preimage: MySqlRecoveryCanonicalBytes,
      projectedTarget: MySqlRecoveryCanonicalBytes): MySqlModePhaseCanonical =
    val request = modeRequest(operation)
    val key = MySqlRecoveryResourceKey.pod(request.podUuid, profile)
    val matching = locators.filter(row => uuidEquals(value(row, LocatorTable, "binding_uuid"),
      request.binding.bindingUuid) && uuidEquals(value(row, LocatorTable, "pod_uuid"), request.podUuid))
    if matching.size != 1 then fail("exact current mode locator required for phase scope")
    val locator = matching.head
    val actualRevision = long(locator, LocatorTable, "locator_revision")
    if actualRevision != locatorRevision then fail("locked locator revision differs from the canonical resource scope")
    val scope = MySqlRecoveryOperationScope.encode(contextIdentity,
      Vector(MySqlRecoveryResourceRevision(key, locatorRevision, Some(request.attemptUuid))))
    val fingerprint = MySqlRecoveryCanonicalBytes.encode(FingerprintEncoding, profile) { output =>
      writePhaseFingerprint(output, plan, request, phase, scope, preimage, projectedTarget)
    }
    val operationUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    val operationContext = phaseOperationContext(contextIdentity, request, operationUuid,
      fingerprint.sha256, profile)
    val contextBytes = operationContextCanonical(operationContext, profile)
    MySqlModePhaseCanonical(preimage, projectedTarget, scope, fingerprint, operationContext, contextBytes)

  private[mysql] def writePhaseFingerprint(output: DataOutputStream, plan: MySqlOwnedRepositoryOperationPlan,
      request: Request, phase: MySqlOperationEffectPhase, scope: MySqlRecoveryCanonicalBytes,
      preimage: MySqlRecoveryCanonicalBytes, projectedTarget: MySqlRecoveryCanonicalBytes): Unit =
    MySqlModeEnvelopeV2Census.text(output, FingerprintDomain)
    MySqlModeEnvelopeV2Census.byte(output, 1)
    MySqlModeEnvelopeV2Census.text(output, phaseName(phase))
    writeUuid(output, request.binding.bindingUuid)
    writeUuid(output, request.podUuid)
    writeUuid(output, request.attemptUuid)
    writeUuid(output, plan.workflowUuid)
    writeUuid(output, plan.reserveOperationUuid)
    writeUuid(output, plan.executeOperationUuid)
    MySqlModeEnvelopeV2Census.int(output, plan.requestIdentity.canonical.length)
    MySqlModeEnvelopeV2Census.canonical(output, plan.requestIdentity.canonical)
    MySqlModeEnvelopeV2Census.int(output, scope.length)
    MySqlModeEnvelopeV2Census.canonical(output, scope)
    MySqlModeEnvelopeV2Census.int(output, preimage.length)
    MySqlModeEnvelopeV2Census.canonical(output, preimage)
    MySqlModeEnvelopeV2Census.int(output, projectedTarget.length)
    MySqlModeEnvelopeV2Census.canonical(output, projectedTarget)

  /** 复用真实 producer phase；完整原算法只在同原 ledger comparison child 校验。 */
  private[mysql] def requireRetainedCanonicalPhase(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      locator: Long, phase: MySqlOperationEffectPhase, before: Image,
      target: Image, retained: MySqlModePhaseCanonical,
      root: MySqlFormat3ReadBudget,
      originalRootOverride: Option[MySqlFormat3ReadBudget] = None,
      chargeStructuresToComparison: Boolean = false): MySqlModePhaseCanonical =
    require(retained != null && root != null && !root.isClosed &&
      before.allocationBudget.sharesAllocationLedger(root) && target.allocationBudget.sharesAllocationLedger(root),
      "actual retained phase and same original ledger required")
    withGraphComparisonOnRoot(target, root, chargeStructuresToComparison) {
      MySqlRecoveryCanonicalBytes.allocationBudgetFor(before.limitProfile).reserveRetainedMemory(2048L,
        "mode retained phase exact comparison sink/context wrappers")
      validatePlan(plan, operation, context, locator, before, originalRootOverride)
      validatePlan(plan, operation, context, locator, target, originalRootOverride)
      sameSnapshotContext(before, target)
      // 唯一writer逐完整typed closure/framing比较，不分配第二个历史canonical payload。
      val version = projectionVersion(retained.preimage)
      if projectionVersion(retained.target) != version ||
        !retained.preimage.matchesWrittenBytes(output =>
          writeProjectedImage(output, before, plan, operation, context, locator, phase, MySqlOperationEffectImage.Before, version)) ||
        !retained.target.matchesWrittenBytes(output =>
          writeProjectedImage(output, target, plan, operation, context, locator, phase, MySqlOperationEffectImage.Target, version)) then
        fail("retained canonical phase differs from final full20 graph")
      val request = modeRequest(operation)
      val key = MySqlRecoveryResourceKey.pod(request.podUuid, before.limitProfile)
      val actualRevision = long(oneLocator(before, request.binding.bindingUuid, request.podUuid), LocatorTable, "locator_revision")
      if actualRevision != locator then fail("locked locator revision differs from the canonical resource scope")
      val scope = MySqlRecoveryOperationScope.encode(context,
        Vector(MySqlRecoveryResourceRevision(key, locator, Some(request.attemptUuid))))
      if !retained.scope.exactlyEquals(scope) || retained.fingerprint.encoding != FingerprintEncoding ||
        !retained.fingerprint.matchesWrittenBytes(output =>
          writePhaseFingerprint(output, plan, request, phase, scope, retained.preimage, retained.target)) then
        fail("retained canonical phase differs from final full20 graph")
      val operationUuid = phase match
        case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
        case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
      val actualContext = phaseOperationContext(context, request, operationUuid, retained.fingerprint.sha256, before.limitProfile)
      val contextBytes = operationContextCanonical(actualContext, before.limitProfile)
      if !retained.operationContextBytes.exactlyEquals(contextBytes) || retained.operationContext != actualContext then
        fail("retained canonical phase differs from final full20 graph")
    }
    retained

  private[mysql] def operationContextCanonicalBytes(context: MySqlOperationContext,
      profile: SourceLimitProfile): MySqlRecoveryCanonicalBytes = operationContextCanonical(context, profile)

  private[mysql] def modeClosureOperationRows(snapshot: Image, binding: UUID,
      pod: UUID): Vector[Vector[MySqlFormat3Cell]] =
    selectModeClosure(snapshot, binding, pod).find(_._1.table == OperationTable)
      .map(_._2).getOrElse(Vector.empty)

  private def requireEnvelopeSetExactly(expected: Vector[MySqlRecoveryOperationRawEnvelope],
      actual: Vector[MySqlRecoveryOperationRawEnvelope], profile: SourceLimitProfile, label: String): Unit =
    val expectedById = expected.map(value => value.operationUuid -> value).toMap
    val actualById = actual.map(value => value.operationUuid -> value).toMap
    if expectedById.size != expected.size || actualById.size != actual.size ||
      expectedById.keySet != actualById.keySet || expectedById.exists { case (uuid, left) =>
        { left.requireInputs(actualById(uuid).deploymentInputs); !left.exactlyMatches(actualById(uuid).row, profile) }
      } then fail(label)

  private def validateCurrentPhaseOperation(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, targetSnapshot: Image,
      preimage: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      scope: MySqlRecoveryCanonicalBytes, fingerprint: MySqlRecoveryCanonicalBytes,
      profile: SourceLimitProfile,
      comparisonBudget: Option[MySqlFormat3ReadBudget] = None): Unit =
    val request = modeRequest(operation)
    val operationUuid = phase match
      case MySqlOperationEffectPhase.Reserve => plan.reserveOperationUuid
      case MySqlOperationEffectPhase.Execute => plan.executeOperationUuid
    val closureRows = modeClosureOperationRows(targetSnapshot, request.binding.bindingUuid, request.podUuid)
    val row = closureRows.find(rowOperationUuid(_) == operationUuid).getOrElse(fail("current phase O row in fixed closure"))
    val step = if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "DOMAIN_MUTATION"
    requireOperationHeader(row, request, plan, operationUuid, step, "COMMITTED")
    validateOperationIdentity(row, targetSnapshot, request, plan, contextIdentity, locatorRevision, operationUuid)
    val expectedContext = phaseOperationContext(contextIdentity, request, operationUuid, fingerprint.sha256, profile)
    val expectedContextBytes = operationContextCanonical(expectedContext, profile)
    requireCanonicalCell(row, OperationTable, "original_context_bytes",
      expectedContextBytes, comparisonBudget.getOrElse(targetSnapshot.allocationBudget), "current O original context")
    requireShaCell(row, OperationTable, "original_context_sha256", expectedContextBytes.sha256)
    requireCanonicalCell(row, OperationTable, "scope_bytes", scope, comparisonBudget.getOrElse(targetSnapshot.allocationBudget), "current O scope")
    requireShaCell(row, OperationTable, "scope_sha256", scope.sha256)
    requireCanonicalCell(row, OperationTable, "request_identity_bytes", plan.requestIdentity.canonical,
      comparisonBudget.getOrElse(targetSnapshot.allocationBudget), "current O request identity")
    requireShaCell(row, OperationTable, "request_identity_sha256", plan.requestIdentitySha256)
    requireCanonicalCell(row, OperationTable, "preimage_bytes", preimage,
      comparisonBudget.getOrElse(targetSnapshot.allocationBudget), "current O preimage")
    requireShaCell(row, OperationTable, "preimage_sha256", preimage.sha256)
    requireCanonicalCell(row, OperationTable, "target_bytes", target,
      comparisonBudget.getOrElse(targetSnapshot.allocationBudget), "current O target")
    requireShaCell(row, OperationTable, "target_sha256", target.sha256)
    requireShaCell(row, OperationTable, "operation_fingerprint", fingerprint.sha256)
    validateWriterReceipt(targetSnapshot, operationUuid, fingerprint.sha256)

  private def validatePlannedExecuteOperation(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, reserveTarget: Image,
      executePreimage: MySqlRecoveryCanonicalBytes, executeTarget: MySqlRecoveryCanonicalBytes,
      executeFingerprint: MySqlRecoveryCanonicalBytes, profile: SourceLimitProfile,
      comparisonBudget: Option[MySqlFormat3ReadBudget] = None): Unit =
    val request = modeRequest(operation)
    val row = modeClosureOperationRows(reserveTarget, request.binding.bindingUuid, request.podUuid)
      .find(rowOperationUuid(_) == plan.executeOperationUuid).getOrElse(fail("RESERVED execute O in reserve target"))
    requireOperationHeader(row, request, plan, plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED")
    validateOperationIdentity(row, reserveTarget, request, plan, contextIdentity, locatorRevision, plan.executeOperationUuid)
    val expectedContext = phaseOperationContext(contextIdentity, request, plan.executeOperationUuid,
      executeFingerprint.sha256, profile)
    val expectedContextBytes = operationContextCanonical(expectedContext, profile)
    requireCanonicalCell(row, OperationTable, "original_context_bytes", expectedContextBytes,
      comparisonBudget.getOrElse(reserveTarget.allocationBudget), "planned execute O original context")
    requireShaCell(row, OperationTable, "original_context_sha256", expectedContextBytes.sha256)
    val expectedScope = MySqlRecoveryOperationScope.encode(contextIdentity,
      Vector(MySqlRecoveryResourceRevision(MySqlRecoveryResourceKey.pod(request.podUuid, profile),
        locatorRevision, Some(request.attemptUuid))))
    requireCanonicalCell(row, OperationTable, "scope_bytes", expectedScope,
      comparisonBudget.getOrElse(reserveTarget.allocationBudget), "planned execute O scope")
    requireShaCell(row, OperationTable, "scope_sha256", expectedScope.sha256)
    requireCanonicalCell(row, OperationTable, "request_identity_bytes", plan.requestIdentity.canonical,
      comparisonBudget.getOrElse(reserveTarget.allocationBudget), "planned execute O request identity")
    requireShaCell(row, OperationTable, "request_identity_sha256", plan.requestIdentitySha256)
    requireCanonicalCell(row, OperationTable, "preimage_bytes", executePreimage,
      comparisonBudget.getOrElse(reserveTarget.allocationBudget), "planned execute O preimage")
    requireShaCell(row, OperationTable, "preimage_sha256", executePreimage.sha256)
    requireCanonicalCell(row, OperationTable, "target_bytes", executeTarget,
      comparisonBudget.getOrElse(reserveTarget.allocationBudget), "planned execute O target")
    requireShaCell(row, OperationTable, "target_sha256", executeTarget.sha256)
    requireShaCell(row, OperationTable, "operation_fingerprint", executeFingerprint.sha256)
    if value(row, OperationTable, "reserve_operation_uuid") != binary(plan.reserveOperationUuid) ||
      value(row, OperationTable, "previous_step_operation_uuid") != binary(plan.reserveOperationUuid) then
      fail("planned execute O must point to the fixed committed reserve O")

  private def validateOperationIdentity(row: Vector[MySqlFormat3Cell], snapshot: Image,
      request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, operationUuid: UUID): Unit =
    val footprint = oneCurrentFootprint(snapshot, request)
    if !uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, OperationTable, "workflow_uuid"), plan.workflowUuid) ||
      !uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
      !uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid) ||
      !uuidEquals(value(row, OperationTable, "footprint_uuid"), uuidCell(value(footprint, FootprintTable, "footprint_uuid"), "current footprint")) ||
      value(row, OperationTable, "source_unit_uuid") != MySqlFormat3Cell.SqlNull ||
      value(row, OperationTable, "forest_uuid") != MySqlFormat3Cell.SqlNull ||
      !uuidEquals(value(row, OperationTable, "boot_uuid"), context.tag.bootId) ||
      long(row, OperationTable, "boot_generation") != context.tag.bootGeneration ||
      !uuidEquals(value(row, OperationTable, "connection_generation_uuid"), context.barrierGenerationUuid) ||
      long(row, OperationTable, "connection_id") != context.actualConnectionId ||
      long(row, OperationTable, "fence_epoch") != context.writerEpoch ||
      long(row, OperationTable, "expected_domain_version") != request.expectedStateVersion ||
      long(row, OperationTable, "expected_locator_revision") != locatorRevision ||
      value(row, OperationTable, "expected_source_revision") != MySqlFormat3Cell.SqlNull ||
      value(row, OperationTable, "fixed_lease_until") != MySqlFormat3Cell.DateTime(context.fixedLeaseUntil) ||
      (operationUuid == plan.reserveOperationUuid &&
        (value(row, OperationTable, "reserve_operation_uuid") != MySqlFormat3Cell.SqlNull ||
          value(row, OperationTable, "previous_step_operation_uuid") != MySqlFormat3Cell.SqlNull)) ||
      (operationUuid == plan.executeOperationUuid &&
        (value(row, OperationTable, "reserve_operation_uuid") != binary(plan.reserveOperationUuid) ||
          value(row, OperationTable, "previous_step_operation_uuid") != binary(plan.reserveOperationUuid))) then
      fail("complete fixed operation row context differs from Main plan and locked mode state")

  private[mysql] def phaseOperationContext(context: MySqlRepositoryOperationContextIdentity, request: Request,
      operationUuid: UUID, fingerprint: Vector[Byte], profile: SourceLimitProfile): MySqlOperationContext =
    if fingerprint == null || fingerprint.size != 32 || context.exactLimitProfile != profile then
      fail("fixed operation context fingerprint/profile")
    MySqlOperationContext(context.tag.bootId, context.writerEpoch, request.binding.bindingUuid, operationUuid,
      Vector(MySqlExpectedEffect("pod/" + request.podUuid.toString, request.expectedStateVersion, fingerprint)))

  private def operationContextCanonical(context: MySqlOperationContext,
      profile: SourceLimitProfile): MySqlRecoveryCanonicalBytes =
    if context == null || profile == null then throw new IllegalArgumentException("exact operation context/profile is required")
    MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1", profile) { output =>
      writeOperationContext(output, context)
    }

  private[mysql] def writeOperationContext(output: DataOutputStream, context: MySqlOperationContext): Unit =
    val domain = "com.wi.floatingpod.mysql-operation-context/v1".getBytes(StandardCharsets.US_ASCII)
    writeLengthFramed(output, domain)
    MySqlRecoveryCanonicalBytes.writeUuid(output, context.bootUuid)
    MySqlModeEnvelopeV2Census.long(output, context.writerEpoch)
    MySqlRecoveryCanonicalBytes.writeUuid(output, context.bindingUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, context.operationUuid)
    MySqlModeEnvelopeV2Census.int(output, context.expectedEffects.size)
    context.expectedEffects.foreach { expected =>
      writeLengthFramed(output, expected.targetIdentity.getBytes(StandardCharsets.US_ASCII))
      MySqlModeEnvelopeV2Census.long(output, expected.expectedVersion)
      writeLengthFramed(output, expected.desiredEffectFingerprintSha256.toArray)
    }

  private def writeLengthFramed(output: DataOutputStream, bytes: Array[Byte]): Unit =
    if output == null || bytes == null then throw new SQLDataException("operation context byte field is missing")
    MySqlModeEnvelopeV2Census.int(output, bytes.length)
    output.write(bytes)

  private[mysql] def requireSuppliedOperationContext(context: MySqlOperationContext,
      row: Vector[MySqlFormat3Cell], profile: SourceLimitProfile,
      budget: MySqlFormat3ReadBudget): Unit =
    val expected = operationContextCanonical(context, profile)
    if context.canonicalFingerprintSha256 != expected.sha256 ||
      text(row, OperationTable, "context_encoding") != "FP_RECOVERY_OPERATION_BE_V1" then
      fail("supplied MySqlOperationContext canonical identity differs from the frozen row encoding")
    requireCanonicalCell(row, OperationTable, "original_context_bytes", expected, budget,
      "supplied current operation context")
    requireShaCell(row, OperationTable, "original_context_sha256", expected.sha256)

  private def requireCanonicalCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      expected: MySqlRecoveryCanonicalBytes, budget: MySqlFormat3ReadBudget, label: String): Unit =
    val actual = value(row, table, name) match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => fail("typed raw bytes required for " + label)
    if expected == null || budget == null || actual.length != expected.length then fail(label + " length differs")
    var copy: Array[Byte] = null
    var primary: Throwable = null
    try
      copy = expected.copyBytes(budget, label + " bounded exact comparison")
      if !actual.equalsBytes(copy) then fail(label + " bytes differ")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if copy != null then
        Arrays.fill(copy, 0.toByte)
        try budget.releaseRetainedMemory(Math.addExact(expected.length.toLong, 64L), label + " comparison release")
        catch
          case cleanup: Throwable =>
            if primary != null then
              if cleanup ne primary then primary.addSuppressed(cleanup)
            else throw cleanup

  private def requireShaCell(row: Vector[MySqlFormat3Cell], table: String, name: String,
      expected: Vector[Byte]): Unit =
    if expected == null || expected.size != 32 || !binaryEquals(value(row, table, name), expected) then
      fail("exact SHA-256 cell " + table + "." + name)

  private def validateWriterReceipt(snapshot: Image,
      operationUuid: UUID, fingerprint: Vector[Byte]): Unit =
    val writer = oneWriter(snapshot)
    if !uuidEquals(value(writer, WriterTable, "last_commit_operation_uuid"), operationUuid) ||
      !binaryEquals(value(writer, WriterTable, "last_commit_fingerprint"), fingerprint) then
      fail("W receipt slot must match the same fixed operation fingerprint")

  private[mysql] def projectImage(snapshot: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      version: MySqlModeProjectionVersion = MySqlModeProjectionVersion.V2): MySqlRecoveryCanonicalBytes =
    snapshot match
      case deployment: MySqlScriptSchemaSnapshot =>
        if version != MySqlModeProjectionVersion.V2 then fail("Full26 mode projection requires original V2 framing")
        return projectImage(deployment, deployment.inputs, originalImageRoot(snapshot, plan), plan,
          operation, contextIdentity, locatorRevision, phase, image)
      case _: MySqlFormat3FullSnapshot => ()
    validatePlan(plan, operation, contextIdentity, locatorRevision, snapshot)
    MySqlRecoveryCanonicalBytes.encode(version.encoding, snapshot.limitProfile) { output =>
      writeProjectedImage(output, snapshot, plan, operation, contextIdentity, locatorRevision, phase, image, version)
    }

  private[mysql] def writeProjectedImage(output: DataOutputStream, snapshot: Image,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      contextIdentity: MySqlRepositoryOperationContextIdentity, locatorRevision: Long,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      version: MySqlModeProjectionVersion = MySqlModeProjectionVersion.V2): Unit =
    val request = modeRequest(operation)
    val closure = selectModeClosure(snapshot, request.binding.bindingUuid, request.podUuid)
    writeProjectedClosure(output,closure,snapshot.limitProfile,snapshot.allocationBudget,plan,operation,
      locatorRevision,phase,image,version)

  /** schema5 使用完整26表的同一规范字段编码，不进入旧 Full20 native witness 容器。 */
  private[mysql] def projectImage(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage): MySqlRecoveryCanonicalBytes =
    projectImageInternal(snapshot, inputs, root, plan, operation, contextIdentity, locatorRevision, phase, image, None)

  /** H3 history-only overload; source authority remains root and fresh canonical DATA belongs to the exact child. */
  private[mysql] def projectImage(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage, dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    projectImageInternal(snapshot, inputs, root, plan, operation, contextIdentity, locatorRevision, phase, image, Some(dataBudget))

  private def projectImageInternal(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, contextIdentity: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage, outputBudget: Option[MySqlFormat3ReadBudget]): MySqlRecoveryCanonicalBytes =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    outputBudget.foreach { budget =>
      require(!budget.isClosed && (budget.profile eq root.profile) && budget.isCanonicalChildOf(root) &&
        budget.sharesAllocationLedger(root), "H3 mode projection requires exact same-ledger DATA child")
    }
    validatePlanProfile(plan,operation,contextIdentity,locatorRevision,inputs.sourceLimitProfile)
    if contextIdentity.schemaFormat!=5 || contextIdentity.physicalFormat!=2 ||
      (contextIdentity.exactLimitProfile ne root.profile) || (plan.exactLimitProfile ne root.profile) ||
      contextIdentity.schema!=snapshot.schema || !contextIdentity.ddlSha256.equalsIgnoreCase(inputs.ddlSha256) ||
      contextIdentity.serverUuid!=snapshot.connection.serverUuid || contextIdentity.serverVersion!=snapshot.connection.serverVersion ||
      contextIdentity.effectiveUser!=snapshot.connection.effectiveUser ||
      contextIdentity.tag.bootId!=snapshot.observationScope.bootUuid ||
      contextIdentity.tag.bootGeneration!=snapshot.observationScope.bootGeneration ||
      contextIdentity.tag.connectionGeneration!=snapshot.observationScope.connectionGeneration then
      fail("Full26 mode projection lost exact schema5 source/context identity")
    val version=MySqlModeProjectionVersion.V2
    def encodeProjection(): MySqlRecoveryCanonicalBytes = MySqlRecoveryCanonicalBytes.encode(version.encoding,inputs.sourceLimitProfile) { output=>
      val request=modeRequest(operation)
      val closure=selectModeClosure(snapshot,inputs,root,request.binding.bindingUuid,request.podUuid)
      writeProjectedClosure(output,closure,inputs.sourceLimitProfile,root,plan,operation,locatorRevision,phase,image,version,Some(inputs))
    }
    outputBudget match
      case Some(budget) => MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) { encodeProjection() }
      case None => encodeProjection()

  private def writeProjectedClosure(output: DataOutputStream,
      closure: Vector[(MySqlFormat3RelationShape,Vector[Vector[MySqlFormat3Cell]])],
      profile: SourceLimitProfile, root: MySqlFormat3ReadBudget,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      locatorRevision: Long, phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      version: MySqlModeProjectionVersion,
      deploymentInputs: Option[MySqlScriptSchemaInputs] = None): Unit =
    val request=modeRequest(operation)
    MySqlModeEnvelopeV2Census.text(output, version.domain)
    MySqlModeEnvelopeV2Census.byte(output, version.number)
    MySqlModeEnvelopeV2Census.text(output, phaseName(phase))
    MySqlModeEnvelopeV2Census.text(output, if image == MySqlOperationEffectImage.Before then "BEFORE" else "TARGET")
    writeUuid(output, request.binding.bindingUuid)
    writeUuid(output, request.podUuid)
    writeUuid(output, request.attemptUuid)
    writeUuid(output, plan.workflowUuid)
    writeUuid(output, plan.reserveOperationUuid)
    writeUuid(output, plan.executeOperationUuid)
    MySqlModeEnvelopeV2Census.long(output, request.expectedStateVersion)
    MySqlModeEnvelopeV2Census.text(output, request.targetMode.databaseValue)
    MySqlModeEnvelopeV2Census.long(output, locatorRevision)
    MySqlModeEnvelopeV2Census.int(output, closure.size)
    closure.foreach { case (shape, selectedRows) =>
      MySqlModeEnvelopeV2Census.text(output, shape.table)
      MySqlModeEnvelopeV2Census.int(output, shape.columns.size)
      shape.columns.foreach { column =>
        MySqlModeEnvelopeV2Census.text(output, column.name)
        MySqlModeEnvelopeV2Census.text(output, column.sqlDeclaration)
      }
      MySqlModeEnvelopeV2Census.int(output, selectedRows.size)
      selectedRows.foreach { row =>
        val historical = version == MySqlModeProjectionVersion.V2 && shape.table == OperationTable &&
          !uuidEquals(value(row, OperationTable, "operation_uuid"), plan.executeOperationUuid) &&
          (phase == MySqlOperationEffectPhase.Execute || !uuidEquals(value(row, OperationTable, "operation_uuid"), plan.reserveOperationUuid))
        if historical then
          val validation = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          require(!validation.isClosed && (validation.profile eq profile), "historical descriptor comparison profile")
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, validation, deploymentInputs)
          if !Set("COMMITTED", "CANCELLED").contains(text(row, OperationTable, "operation_state")) then
            fail("mode historical descriptor requires complete terminal RAW")
        val key = shape.primaryKey.map(name => row(shape.columns.indexWhere(_.name==name)))
        shape.columns.zipWithIndex.foreach { (column, indexValue) =>
          if markerRequired(phase, image, shape.table, key, column.name, plan) then
            writeMarker(output, phase, shape, key, column, row(indexValue))
          else if !historical || !writeHistoricalDescriptor(output, row, column) then writeCell(output, row(indexValue))
        }
      }
    }

  private def writeHistoricalDescriptor(output: DataOutputStream, row: Vector[MySqlFormat3Cell],
      column: MySqlFormat3ColumnShape): Boolean =
    val hashColumn = column.name match
      case "request_identity_bytes" => "request_identity_sha256"
      case "original_context_bytes" => "original_context_sha256"
      case "scope_bytes" => "scope_sha256"
      case "preimage_bytes" => "preimage_sha256"
      case "target_bytes" => "target_sha256"
      case "commit_witness_bytes" => "commit_witness_sha256"
      case "original_diagnostic_identity" => "original_diagnostic_sha256"
      case _ => return false
    (value(row, OperationTable, column.name), value(row, OperationTable, hashColumn)) match
      case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => false
      case (MySqlFormat3Cell.Binary(bytes), MySqlFormat3Cell.Binary(hash)) if hash.length == 32 =>
        MySqlModeEnvelopeV2Census.byte(output, 7)
        MySqlModeEnvelopeV2Census.text(output, "FP_MODE_HISTORICAL_PAYLOAD_V1")
        MySqlModeEnvelopeV2Census.text(output, column.name)
        MySqlModeEnvelopeV2Census.text(output, column.sqlDeclaration)
        MySqlModeEnvelopeV2Census.long(output, bytes.length.toLong)
        MySqlModeEnvelopeV2Census.binary(output, hash)
        true
      case _ => fail("historical payload/digest types or NULL pairing differ")

  private[mysql] def selectModeClosure(snapshot: Image, binding: UUID,
      pod: UUID): Vector[(MySqlFormat3RelationShape, Vector[Vector[MySqlFormat3Cell]])] =
    val owner = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(snapshot.allocationBudget)
    require(!owner.isClosed && (owner.profile eq snapshot.limitProfile), "mode closure comparison profile")
    selectModeClosureRows(snapshot.relations,snapshot.rowCount,snapshot.shapes,
      binding,pod,owner)

  /** 完整 schema5 DATA 入口；额外六表保留全部行，不从 Full20 容器重建或裁切。 */
  private[mysql] def selectModeClosure(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, binding: UUID,
      pod: UUID): Vector[(MySqlFormat3RelationShape, Vector[Vector[MySqlFormat3Cell]])] =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    val owner=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(!owner.isClosed && (owner.profile eq root.profile) &&
      ((owner eq root) || owner.isCanonicalChildOf(root)),"mode Full26 closure requires original allocation owner")
    selectModeClosureRows(snapshot.relations,snapshot.rowCount,inputs.canonicalOrder,binding,pod,owner)

  private def selectModeClosureRows(relations: Vector[MySqlFormat3RelationSnapshot], rowCount: Long,
      shapes: Vector[MySqlFormat3RelationShape], binding: UUID, pod: UUID,
      owner: MySqlFormat3ReadBudget): Vector[(MySqlFormat3RelationShape, Vector[Vector[MySqlFormat3Cell]])] =
    require(binding!=null && pod!=null,"exact mode binding and POD required")
    // 返回值只借原shape/row；其vector/tuple由调用者owner计费。可变索引和UUID副本不逃逸。
    owner.reserveRetainedMemory(Math.addExact(4096L, Math.multiplyExact(rowCount, 128L)), "mode closure borrowed result collections")
    var result: Vector[(MySqlFormat3RelationShape, Vector[Vector[MySqlFormat3Cell]])] = null
    def select(): Unit =
      val byTable = relations.map(value => value.table -> value.rows).toMap
      val selectedKeys = mutable.HashMap.empty[String, mutable.HashSet[Vector[MySqlFormat3Cell]]]
      val attempts = mutable.HashSet.empty[Vector[Byte]]
      val footprints = mutable.HashSet.empty[Vector[Byte]]
      val units = mutable.HashSet.empty[Vector[Byte]]
      val forests = mutable.HashSet.empty[Vector[Byte]]
      val operations = mutable.HashSet.empty[Vector[Byte]]
      val writes = mutable.HashSet.empty[Vector[Byte]]
      val carrierKeys = mutable.HashSet.empty[(String, Vector[Byte])]
      var changed = true
      var rounds = 0

      def add(table: String, row: Vector[MySqlFormat3Cell]): Boolean =
        val shape = MySqlFormat3RelationShapes.forTable(table)
        val key = shape.primaryKey.map(name => row(index(table, name)))
        selectedKeys.getOrElseUpdate(table, mutable.HashSet.empty[Vector[MySqlFormat3Cell]]).add(key)

      def selected(table: String): Vector[Vector[MySqlFormat3Cell]] =
        val keys = selectedKeys.get(table).map(_.toSet).getOrElse(Set.empty[Vector[MySqlFormat3Cell]])
        byTable(table).filter(row =>
          keys.contains(MySqlFormat3RelationShapes.forTable(table).primaryKey.map(name => row(index(table, name)))))

      while changed do
        changed = false
        rounds = Math.addExact(rounds, 1)
        if rounds > 48 then fail("fixed FK closure exceeds 48 monotone passes")

        def take(table: String)(predicate: Vector[MySqlFormat3Cell] => Boolean): Unit =
          byTable(table).foreach { row =>
            if predicate(row) && add(table, row) then
              changed = true
              MySqlFormat3RelationShapes.forTable(table).columns.foreach { column =>
                val item = row(index(table, column.name))
                val name = column.name
                if name == "attempt_uuid" || name == "current_attempt_uuid" || name == "predecessor_attempt_uuid" then addUuid(attempts, item)
                if name == "footprint_uuid" || name == "current_footprint_uuid" || name == "target_footprint_uuid" ||
                  name == "from_footprint_uuid" || name == "to_footprint_uuid" || name == "predecessor_footprint_uuid" then
                  addUuid(footprints, item)
                if name == "source_unit_uuid" || name == "unit_uuid" then addUuid(units, item)
                if name == "forest_uuid" || name == "source_forest_uuid" || name == "old_forest_uuid" ||
                  name == "target_forest_uuid" || name == "current_forest_uuid" then addUuid(forests, item)
                if name == "write_uuid" then addUuid(writes, item)
                if isOperationReference(name) then addUuid(operations, item)
                if name == "operation_uuid" && table == OperationTable then addUuid(operations, item)
                if name == "carrier_key_hash" then
                  (item, value(row, table, "resource_kind")) match
                    case (MySqlFormat3Cell.Binary(hash), MySqlFormat3Cell.Text(kind)) => carrierKeys.add(kind -> keyBytes(hash))
                    case _ => ()
              }
          }

        take(DomainTable)(row => uuidEquals(value(row, DomainTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, DomainTable, "uuid"), pod))
        take(ManifestTable)(row => long(row, ManifestTable, "singleton_fixed1") == 1L)
        take(BindingTable)(row => long(row, BindingTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, BindingTable, "binding_uuid"), binding))
        take(WriterTable)(row => long(row, WriterTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, WriterTable, "binding_uuid"), binding))
        take(BootstrapIntentTable)(row => long(row, BootstrapIntentTable, "singleton_fixed1") == 1L &&
          uuidEquals(value(row, BootstrapIntentTable, "binding_uuid"), binding))
        take(LocatorTable)(row => uuidEquals(value(row, LocatorTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, LocatorTable, "pod_uuid"), pod))
        take(FootprintTable)(row => uuidEquals(value(row, FootprintTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, FootprintTable, "pod_uuid"), pod))
        take(IntentTable)(row => uuidEquals(value(row, IntentTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, IntentTable, "pod_uuid"), pod))
        take(ChunkTable)(row => uuidEquals(value(row, ChunkTable, "binding_uuid"), binding) &&
          footprints.contains(uuidBytes(value(row, ChunkTable, "footprint_uuid"))))
        take(ObligationTable)(row => uuidEquals(value(row, ObligationTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, ObligationTable, "pod_uuid"), pod) &&
          attempts.contains(uuidBytes(value(row, ObligationTable, "attempt_uuid"))) &&
          footprints.contains(uuidBytes(value(row, ObligationTable, "footprint_uuid"))))
        take(CompletionTable)(row => uuidEquals(value(row, CompletionTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, CompletionTable, "pod_uuid"), pod))
        take(MoveTable)(row => uuidEquals(value(row, MoveTable, "binding_uuid"), binding) &&
          uuidEquals(value(row, MoveTable, "pod_uuid"), pod))
        take(OperationTable)(row => uuidEquals(value(row, OperationTable, "binding_uuid"), binding) &&
          (uuidEquals(value(row, OperationTable, "pod_uuid"), pod) ||
            attempts.contains(uuidBytes(value(row, OperationTable, "attempt_uuid"))) ||
            footprints.contains(uuidBytes(value(row, OperationTable, "footprint_uuid"))) ||
            units.contains(uuidBytes(value(row, OperationTable, "source_unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, OperationTable, "forest_uuid"))) ||
            operations.contains(uuidBytes(value(row, OperationTable, "operation_uuid")))))
        take(SourceUnitTable)(row => uuidEquals(value(row, SourceUnitTable, "binding_uuid"), binding) &&
          units.contains(uuidBytes(value(row, SourceUnitTable, "unit_uuid"))))
        take(SourceForestTable)(row => uuidEquals(value(row, SourceForestTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceForestTable, "unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, SourceForestTable, "forest_uuid")))))
        take(SourceTransferTable)(row => uuidEquals(value(row, SourceTransferTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceTransferTable, "unit_uuid"))) ||
            operations.contains(uuidBytes(value(row, SourceTransferTable, "prepare_operation_uuid")))))
        take(SourceWriteTable)(row => uuidEquals(value(row, SourceWriteTable, "binding_uuid"), binding) &&
          (writes.contains(uuidBytes(value(row, SourceWriteTable, "write_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, SourceWriteTable)) ||
            referencesOperation(row, SourceWriteTable, operations)))
        take(SourceObligationTable)(row => uuidEquals(value(row, SourceObligationTable, "binding_uuid"), binding) &&
          (units.contains(uuidBytes(value(row, SourceObligationTable, "unit_uuid"))) ||
            forests.contains(uuidBytes(value(row, SourceObligationTable, "forest_uuid"))) ||
            writes.contains(uuidBytes(value(row, SourceObligationTable, "write_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, SourceObligationTable)) ||
            referencesOperation(row, SourceObligationTable, operations)))
        take(ReservationTable)(row => uuidEquals(value(row, ReservationTable, "binding_uuid"), binding) &&
          (uuidEquals(value(row, ReservationTable, "resource_pod_uuid"), pod) ||
            units.contains(uuidBytes(value(row, ReservationTable, "resource_unit_uuid"))) ||
            carrierKeys.contains(carrierTuple(row, ReservationTable)) ||
            referencesOperation(row, ReservationTable, operations)))

      result = shapes.map { shape =>
        // 原二十表沿用同一闭包；脚本、部署配置和 quota 行是完整快照的固定上下文。
        val rows=if MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==shape.table) then
          selected(shape.table)
        else byTable(shape.table)
        shape -> rows
      }
    if owner.isScopedReadBudget then select()
    else MySqlRecoveryCanonicalBytes.withComparisonBudget(owner) { select() }
    result

  private def validatePlan(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, revision: Long, snapshot: Image,
      originalRootOverride: Option[MySqlFormat3ReadBudget] = None): Unit =
    if snapshot==null then
      throw new IllegalArgumentException("mode workflow plan, actual context, profile and locator revision are required")
    val expectedFormat = snapshot match
      case _: MySqlFormat3FullSnapshot => 3
      case deployment: MySqlScriptSchemaSnapshot =>
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment, deployment.inputs,
          originalRootOverride.getOrElse(originalImageRoot(snapshot, plan)))
        5
    if context!=null && (context.schemaFormat!=expectedFormat || context.physicalFormat!=2) then
      fail("mode projection requires its exact complete image schema/physical context")
    validatePlanProfile(plan,operation,context,revision,snapshot.limitProfile)

  private def validatePlanProfile(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, revision: Long, profile: SourceLimitProfile): Unit =
    if plan == null || context == null || profile == null || revision < 0L then
      throw new IllegalArgumentException("mode workflow plan, actual context, profile and locator revision are required")
    val request = modeRequest(operation)
    if plan.requestKind != "RUN" || context.requestKind != "RUN" || plan.exactLimitProfile != profile ||
      context.exactLimitProfile != profile || context.tag != plan.tag ||
      context.workflowUuid != plan.workflowUuid || context.reserveOperationUuid != plan.reserveOperationUuid ||
      context.requestIdentitySha256 != plan.requestIdentitySha256 || context.bindingUuid != request.binding.bindingUuid ||
      plan.executeOperationUuid != plan.tag.operationId then
      fail("Main plan/context/profile identities do not match the exact mode request")
    plan.requireSingle(operation, profile)

  private def validatePreimage(snapshot: Image,
      operation: MySqlRepositoryOperation, context: MySqlRepositoryOperationContextIdentity,
      locatorRevision: Long): Unit =
    val request = modeRequest(operation)
    validateLockedHeaders(snapshot, context, request)
    val domain = oneDomain(snapshot, request.binding.bindingUuid, request.podUuid)
    if !uuidEquals(value(domain, DomainTable, "owner_uuid"), request.owner) ||
      long(domain, DomainTable, "is_active") != 1L || long(domain, DomainTable, "state_version") != request.expectedStateVersion ||
      long(domain, DomainTable, "dimension_id") != request.actualPose.dimensionId ||
      text(domain, DomainTable, "world_name") != request.actualPose.worldName ||
      !Set("NORMAL", "WORLD_IDLE").contains(text(domain, DomainTable, "operating_mode")) then
      fail("active owned mode preimage")
    val locator = oneLocator(snapshot, request.binding.bindingUuid, request.podUuid)
    if !uuidEquals(value(locator, LocatorTable, "current_attempt_uuid"), request.attemptUuid) ||
      text(locator, LocatorTable, "locator_state") != "ROW_ACTIVE" ||
      text(locator, LocatorTable, "reason_code") != "ROW_ACTIVE" ||
      long(locator, LocatorTable, "domain_version") != request.expectedStateVersion ||
      long(locator, LocatorTable, "target_dimension_id") != request.actualPose.dimensionId ||
      long(locator, LocatorTable, "locator_revision") != locatorRevision then fail("exact current ROW_ACTIVE locator")
    val footprint = oneCurrentFootprint(snapshot, request)
    if text(footprint, FootprintTable, "seal_state") != "SEALED" then fail("sealed current footprint")
    val cx = chunk(double(domain, "pos_x"))
    val cz = chunk(double(domain, "pos_z"))
    if cx != chunk(request.actualPose.posX) || cz != chunk(request.actualPose.posZ) then fail("same-chunk WID pose")
    if long(footprint, FootprintTable, "origin_version") > request.expectedStateVersion ||
      long(footprint, FootprintTable, "target_dimension_id") != request.actualPose.dimensionId ||
      int(footprint, FootprintTable, "center_chunk_x") != cx ||
      int(footprint, FootprintTable, "center_chunk_z") != cz then
      fail("current footprint origin and 5x5 center agree with the retained pose")
    val chunks = currentChunks(snapshot, request)
    val expected = (for x <- cx - 2 to cx + 2; z <- cz - 2 to cz + 2 yield (x, z)).toSet
    val actual = chunks.map(row => (int(row, ChunkTable, "chunk_x"), int(row, ChunkTable, "chunk_z"))).toSet
    if chunks.size != 25 || actual.size != 25 || actual != expected ||
      chunks.exists(row => long(row, ChunkTable, "dimension_id") != request.actualPose.dimensionId) then
      fail("exact immutable current 5x5 footprint")

  private def validateLockedHeaders(snapshot: Image,
      context: MySqlRepositoryOperationContextIdentity, request: Request): Unit =
    if context == null || snapshot.schema != context.schema ||
      snapshot.connection.serverUuid != context.serverUuid || snapshot.connection.serverVersion != context.serverVersion ||
      snapshot.connection.catalog != context.schema || snapshot.connection.effectiveUser != context.effectiveUser ||
      snapshot.observationScope.bootUuid != context.tag.bootId ||
      snapshot.observationScope.bootGeneration != context.tag.bootGeneration ||
      snapshot.observationScope.connectionGeneration != context.tag.connectionGeneration ||
      snapshot.maxAllowedPacketBytes < context.exactLimitProfile.maxSqlPacketBytes() then
      fail("same verified format3 observation identity/profile is required")

    val manifest = oneSingleton(snapshot, ManifestTable)
    if text(manifest, ManifestTable, "backend") != "MYSQL" ||
      long(manifest, ManifestTable, "format_version") != context.schemaFormat.toLong ||
      text(manifest, ManifestTable, "bootstrap_state") != "READY" ||
      !uuidEquals(value(manifest, ManifestTable, "expected_server_uuid"), context.serverUuid) ||
      !uuidEquals(value(manifest, ManifestTable, "expected_binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(manifest, ManifestTable, "provisioning_uuid"), context.provisioningUuid) ||
      !binaryEquals(value(manifest, ManifestTable, "ddl_sha256"), parseSha256(context.ddlSha256)) ||
      !binaryEquals(value(manifest, ManifestTable, "guard_manifest_sha256"), parseSha256(context.approvedObjectManifestSha256)) ||
      !binaryEquals(value(manifest, ManifestTable, "configuration_sha256"), context.configurationSha256) ||
      text(manifest, ManifestTable, "source_provenance_kind") != context.sourceProvenanceKind ||
      !binaryEquals(value(manifest, ManifestTable, "source_provenance_sha256"), context.sourceProvenanceSha256) ||
      text(manifest, ManifestTable, "limit_profile_encoding") != SourceLimitProfile.ENCODING ||
      !binaryEquals(value(manifest, ManifestTable, "limit_profile_sha256"), context.limitProfileSha256) then
      fail("exact READY format3 manifest identities")
    requireProfileInManifest(manifest, context.exactLimitProfile)
    requireSha256Cell(value(manifest, ManifestTable, "source_provenance_bytes"),
      context.sourceProvenanceSha256, "source provenance")

    val binding = oneSingleton(snapshot, BindingTable)
    if !uuidEquals(value(binding, BindingTable, "binding_uuid"), request.binding.bindingUuid) ||
      text(binding, BindingTable, "cutover_state") != "CERTIFIED" ||
      long(binding, BindingTable, "format_version") != 2L then
      fail("exact CERTIFIED physical2 binding")

    val writer = oneWriter(snapshot)
    if !uuidEquals(value(writer, WriterTable, "binding_uuid"), context.bindingUuid) ||
      long(writer, WriterTable, "writer_epoch") != context.writerEpoch ||
      !uuidEquals(value(writer, WriterTable, "owner_boot_uuid"), context.tag.bootId) ||
      value(writer, WriterTable, "lease_until") != MySqlFormat3Cell.DateTime(context.fixedLeaseUntil) then
      fail("exact current W binding/epoch/boot/lease tuple")
    val slotOperation = value(writer, WriterTable, "last_commit_operation_uuid")
    val slotFingerprint = value(writer, WriterTable, "last_commit_fingerprint")
    (slotOperation, slotFingerprint) match
      case (MySqlFormat3Cell.SqlNull, MySqlFormat3Cell.SqlNull) => ()
      case (MySqlFormat3Cell.Binary(operationBytes), MySqlFormat3Cell.Binary(fingerprintBytes))
          if operationBytes.length == 16 && fingerprintBytes.length == 32 => ()
      case _ => fail("W last receipt operation/fingerprint pair must be complete or SQL NULL together")

  private def oneSingleton(snapshot: Image, table: String): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(table).rows.filter(row => long(row, table, "singleton_fixed1") == 1L)
    if rows.size != 1 then fail("exact singleton " + table + " row")
    rows.head

  private def requireProfileInManifest(manifest: Vector[MySqlFormat3Cell], profile: SourceLimitProfile): Unit =
    val expected = profile.canonicalBytes()
    try
      value(manifest, ManifestTable, "limit_profile_bytes") match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(expected) => ()
        case _ => fail("M raw LimitProfile canonical bytes differ")
    finally Arrays.fill(expected, 0.toByte)
    val fields = Vector(
      "max_region_bytes" -> profile.maxRegionBytes(),
      "max_chunk_bytes" -> profile.maxChunkBytes(),
      "max_decompressed_bytes" -> profile.maxDecompressedBytes(),
      "max_nbt_depth" -> profile.maxNbtDepth(),
      "max_members" -> profile.maxMembers(),
      "max_retained_bytes" -> profile.maxRetainedBytes(),
      "max_sql_packet_bytes" -> profile.maxSqlPacketBytes(),
      "max_native_artifact_bytes" -> profile.maxNativeArtifactBytes())
    fields.foreach { (name, number) =>
      if long(manifest, ManifestTable, name) != number then fail("M decoded LimitProfile scalar differs: " + name)
    }

  private def requireSha256Cell(valueCell: MySqlFormat3Cell, expected: Vector[Byte], label: String): Unit = valueCell match
    case MySqlFormat3Cell.Binary(bytes) =>
      val digest = MessageDigest.getInstance("SHA-256")
      val sink = new DataOutputStream(new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(), digest))
      bytes.writeTo(sink)
      sink.flush()
      if digest.digest().toVector != expected then fail(label + " bytes/digest pair differs")
    case _ => fail(label + " full bytes are missing")

  private def parseSha256(value: String): Vector[Byte] =
    try java.util.HexFormat.of().parseHex(value).toVector
    catch case failure: IllegalArgumentException => throw new SQLDataException("approved identity has malformed SHA-256", failure)

  private def validateReserveShape(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, before: Image,
      after: Image): Unit =
    val request = modeRequest(operation)
    unchanged(before, after, DomainTable)
    unchanged(before, after, LocatorTable)
    before.shapes.filterNot(shape =>
      Set("fp_recovery_operation", "fp_recovery_reservation", "fp_runtime_writer", DomainTable, LocatorTable).contains(shape.table)
    ).foreach(shape => unchanged(before, after, shape.table))
    val beforeOps = keyed(before, OperationTable)
    val afterOps = keyed(after, OperationTable)
    val reserveKey = operationKey(request.binding.bindingUuid, plan.reserveOperationUuid)
    val executeKey = operationKey(request.binding.bindingUuid, plan.executeOperationUuid)
    if beforeOps.contains(reserveKey) || beforeOps.contains(executeKey) ||
      beforeOps.keySet.diff(afterOps.keySet).nonEmpty ||
      afterOps.keySet.diff(beforeOps.keySet) != Set(reserveKey, executeKey) ||
      beforeOps.exists { case (key, row) => afterOps.get(key).forall(_ != row) } then
      fail("reserve phase appends only the two fixed O rows")
    val reserveO = afterOps(reserveKey)
    val executeO = afterOps(executeKey)
    requireOperationHeader(reserveO, request, plan, plan.reserveOperationUuid, "RESERVE", "COMMITTED")
    requireOperationHeader(executeO, request, plan, plan.executeOperationUuid, "DOMAIN_MUTATION", "RESERVED")
    if !uuidEquals(value(executeO, OperationTable, "reserve_operation_uuid"), plan.reserveOperationUuid) ||
      !uuidEquals(value(executeO, OperationTable, "previous_step_operation_uuid"), plan.reserveOperationUuid) then
      fail("executeO links to the exact reserveO")
    Vector("result_at", "settled_by_operation_uuid", "commit_witness_bytes", "commit_witness_sha256").foreach { name =>
      if value(executeO, OperationTable, name) != MySqlFormat3Cell.SqlNull then fail("RESERVED executeO SQL NULL " + name)
    }
    if optionalDate(reserveO, OperationTable, "result_at").isEmpty ||
      optionalDate(executeO, OperationTable, "result_at").nonEmpty ||
      value(reserveO, OperationTable, "planned_at") != value(executeO, OperationTable, "planned_at") ||
      value(reserveO, OperationTable, "fixed_lease_until") != value(executeO, OperationTable, "fixed_lease_until") then
      fail("fixed reserve/execute timestamp and terminal state")
    val oldReservation = rowsForPod(before, ReservationTable, request.binding.bindingUuid, request.podUuid)
    val newReservation = rowsForPod(after, ReservationTable, request.binding.bindingUuid, request.podUuid)
    if oldReservation.size > 1 || newReservation.size != 1 then fail("one historical/current POD reservation row")
    val oldReservationMap = keyed(before, ReservationTable)
    val newReservationMap = keyed(after, ReservationTable)
    val targetReservationKey = MySqlFormat3RelationShapes.forTable(ReservationTable).primaryKey
      .map(name => newReservation.head(index(ReservationTable, name)))
    val changedReservationKeys = oldReservationMap.keySet.union(newReservationMap.keySet).filter(key =>
      oldReservationMap.get(key) != newReservationMap.get(key))
    if changedReservationKeys != Set(targetReservationKey) ||
      oldReservationMap.keySet.diff(newReservationMap.keySet).nonEmpty ||
      newReservationMap.keySet.diff(oldReservationMap.keySet).exists(_ != targetReservationKey) then
      fail("reserve changes only the exact POD reservation row across full R history")
    val expectedRevision = oldReservation.headOption match
      case None => 0L
      case Some(previous) =>
        if text(previous, ReservationTable, "reservation_state") != "RELEASED" then
          fail("a previous HELD POD reservation retains its operation responsibility")
        Math.addExact(long(previous, ReservationTable, "reservation_revision"), 1L)
    validateReservationIdentity(newReservation.head, request, plan, before.limitProfile,
      expectedRevision, plan.reserveOperationUuid, "HELD")
    oldReservation.headOption.foreach { previous =>
      if !sameExcept(previous, newReservation.head, ReservationTable,
        Set("reservation_revision", "workflow_uuid", "operation_uuid", "reservation_state", "last_mutation_operation_uuid")) ||
        long(newReservation.head, ReservationTable, "reservation_revision") != expectedRevision then
        fail("released POD reservation is reused by exact identity with one revision increment")
    }
    val oldWriter = oneWriter(before)
    val newWriter = oneWriter(after)
    if !sameExcept(oldWriter, newWriter, WriterTable, Set("last_commit_operation_uuid", "last_commit_fingerprint")) ||
      !uuidEquals(value(newWriter, WriterTable, "last_commit_operation_uuid"), plan.reserveOperationUuid) ||
      !binaryOfLength(value(newWriter, WriterTable, "last_commit_fingerprint"), 32) then
      fail("reserve W updates only its last receipt slot")

  private def validateExecuteShape(plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation, before: Image,
      after: Image): Unit =
    val request = modeRequest(operation)
    before.shapes.filterNot(shape =>
      Set("fp_recovery_operation", "fp_recovery_reservation", WriterTable, DomainTable, LocatorTable).contains(shape.table)
    ).foreach(shape => unchanged(before, after, shape.table))
    val oldDomain = oneDomain(before, request.binding.bindingUuid, request.podUuid)
    val newDomain = oneDomain(after, request.binding.bindingUuid, request.podUuid)
    val oldDomains = keyed(before, DomainTable)
    val newDomains = keyed(after, DomainTable)
    val domainKey = Vector(binary(request.podUuid))
    if oldDomains.keySet != newDomains.keySet || oldDomains.exists { case (key, row) =>
      key != domainKey && newDomains.get(key).forall(_ != row)
    } then fail("mode change may advance only the exact domain PK")
    if !sameExcept(oldDomain, newDomain, DomainTable,
      Set("state_version", "world_name", "pos_x", "pos_y", "pos_z", "yaw", "pitch", "last_updated",
        "operating_mode", "last_mutation_operation_uuid")) ||
      long(newDomain, DomainTable, "state_version") != Math.addExact(request.expectedStateVersion, 1L) ||
      text(newDomain, DomainTable, "world_name") != request.actualPose.worldName ||
      double(newDomain, "pos_x") != request.actualPose.posX || double(newDomain, "pos_y") != request.actualPose.posY ||
      double(newDomain, "pos_z") != request.actualPose.posZ || double(newDomain, "yaw") != request.actualPose.yaw.toDouble ||
      double(newDomain, "pitch") != request.actualPose.pitch.toDouble ||
      long(newDomain, DomainTable, "dimension_id") != request.actualPose.dimensionId ||
      text(newDomain, DomainTable, "operating_mode") != request.targetMode.databaseValue ||
      !uuidEquals(value(newDomain, DomainTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) then
      fail("execute domain row is not the exact one-version same-chunk mode/pose update")
    val oldLocator = oneLocator(before, request.binding.bindingUuid, request.podUuid)
    val newLocator = oneLocator(after, request.binding.bindingUuid, request.podUuid)
    val oldLocators = keyed(before, LocatorTable)
    val newLocators = keyed(after, LocatorTable)
    val locatorKey = MySqlFormat3RelationShapes.forTable(LocatorTable).primaryKey
      .map(name => oldLocator(index(LocatorTable, name)))
    if oldLocators.keySet != newLocators.keySet || oldLocators.exists { case (key, row) =>
      key != locatorKey && newLocators.get(key).forall(_ != row)
    } then fail("mode change may advance only the exact locator PK")
    if !sameExcept(oldLocator, newLocator, LocatorTable, Set("domain_version", "locator_revision", "last_mutation_operation_uuid")) ||
      long(newLocator, LocatorTable, "domain_version") != Math.addExact(request.expectedStateVersion, 1L) ||
      long(newLocator, LocatorTable, "locator_revision") != Math.addExact(long(oldLocator, LocatorTable, "locator_revision"), 1L) ||
      !uuidEquals(value(newLocator, LocatorTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) then
      fail("execute locator must advance domain version and revision with D")
    val oldOps = keyed(before, OperationTable)
    val newOps = keyed(after, OperationTable)
    val reserveKey = operationKey(request.binding.bindingUuid, plan.reserveOperationUuid)
    val executeKey = operationKey(request.binding.bindingUuid, plan.executeOperationUuid)
    if oldOps.keySet != newOps.keySet || oldOps.get(reserveKey) != newOps.get(reserveKey) ||
      oldOps.exists { case (key, row) => key != executeKey && newOps.get(key).forall(_ != row) } ||
      oldOps.get(executeKey).isEmpty || newOps.get(executeKey).isEmpty then
      fail("execute retains every operation row and the raw historical reserveO")
    val oldExecute = oldOps(executeKey)
    val newExecute = newOps(executeKey)
    if text(oldExecute, OperationTable, "operation_state") != "RESERVED" ||
      text(newExecute, OperationTable, "operation_state") != "COMMITTED" ||
      value(oldExecute, OperationTable, "result_at") != MySqlFormat3Cell.SqlNull ||
      optionalDate(newExecute, OperationTable, "result_at").isEmpty ||
      value(oldExecute, OperationTable, "commit_witness_bytes") != MySqlFormat3Cell.SqlNull ||
      value(oldExecute, OperationTable, "commit_witness_sha256") != MySqlFormat3Cell.SqlNull ||
      !binaryOfLength(value(newExecute, OperationTable, "commit_witness_bytes"), -1) ||
      !binaryOfLength(value(newExecute, OperationTable, "commit_witness_sha256"), 32) then
      fail("executeO moves only from genuine RESERVED NULL witness to COMMITTED witness")
    if !sameExcept(oldExecute, newExecute, OperationTable,
      Set("operation_state", "preimage_bytes", "preimage_sha256", "target_bytes", "target_sha256",
        "commit_witness_bytes", "commit_witness_sha256", "result_at", "operation_fingerprint")) then
      fail("executeO changes only its phase fields")
    val oldReservations = rowsForPod(before, ReservationTable, request.binding.bindingUuid, request.podUuid)
    val newReservations = rowsForPod(after, ReservationTable, request.binding.bindingUuid, request.podUuid)
    if oldReservations.size != 1 || newReservations.size != 1 ||
      !sameExcept(oldReservations.head, newReservations.head, ReservationTable,
        Set("reservation_revision", "reservation_state", "last_mutation_operation_uuid")) ||
      long(newReservations.head, ReservationTable, "reservation_revision") !=
        Math.addExact(long(oldReservations.head, ReservationTable, "reservation_revision"), 1L) ||
      text(oldReservations.head, ReservationTable, "reservation_state") != "HELD" ||
      text(newReservations.head, ReservationTable, "reservation_state") != "RELEASED" ||
      !uuidEquals(value(newReservations.head, ReservationTable, "last_mutation_operation_uuid"), plan.executeOperationUuid) ||
      !uuidEquals(value(newReservations.head, ReservationTable, "operation_uuid"), plan.executeOperationUuid) ||
      !uuidEquals(value(newReservations.head, ReservationTable, "workflow_uuid"), plan.workflowUuid) then
      fail("execute releases the exact held POD reservation once")
    validateReservationIdentity(oldReservations.head, request, plan, before.limitProfile,
      long(oldReservations.head, ReservationTable, "reservation_revision"), plan.reserveOperationUuid, "HELD")
    validateReservationIdentity(newReservations.head, request, plan, before.limitProfile,
      Math.addExact(long(oldReservations.head, ReservationTable, "reservation_revision"), 1L),
      plan.executeOperationUuid, "RELEASED")
    val oldReservationMap = keyed(before, ReservationTable)
    val newReservationMap = keyed(after, ReservationTable)
    val targetReservationKey = MySqlFormat3RelationShapes.forTable(ReservationTable).primaryKey
      .map(name => oldReservations.head(index(ReservationTable, name)))
    val changedReservationKeys = oldReservationMap.keySet.union(newReservationMap.keySet).filter(key =>
      oldReservationMap.get(key) != newReservationMap.get(key))
    if oldReservationMap.keySet != newReservationMap.keySet || changedReservationKeys != Set(targetReservationKey) then
      fail("execute changes only the exact held POD reservation row across full R history")
    val oldWriter = oneWriter(before)
    val newWriter = oneWriter(after)
    if !sameExcept(oldWriter, newWriter, WriterTable, Set("last_commit_operation_uuid", "last_commit_fingerprint")) ||
      !uuidEquals(value(newWriter, WriterTable, "last_commit_operation_uuid"), plan.executeOperationUuid) ||
      !binaryOfLength(value(newWriter, WriterTable, "last_commit_fingerprint"), 32) then
      fail("execute W updates only its exact receipt slot")

  private def markerRequired(phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      table: String, key: Vector[MySqlFormat3Cell], column: String,
      plan: MySqlOwnedRepositoryOperationPlan): Boolean =
    if table == WriterTable && image == MySqlOperationEffectImage.Target && column == "last_commit_fingerprint" then true
    else if table != OperationTable then false
    else
      val operationUuid = uuidCell(key(index(OperationTable, "operation_uuid")), "operation_uuid key")
      val reserve = operationUuid == plan.reserveOperationUuid
      val execute = operationUuid == plan.executeOperationUuid
      phase match
        case MySqlOperationEffectPhase.Reserve if image == MySqlOperationEffectImage.Target =>
          (reserve && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "commit_witness_bytes", "commit_witness_sha256").contains(column)) ||
          (execute && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "preimage_bytes", "preimage_sha256").contains(column))
        case MySqlOperationEffectPhase.Execute =>
          (execute && Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
            "operation_fingerprint", "preimage_bytes", "preimage_sha256").contains(column)) ||
          (execute && image == MySqlOperationEffectImage.Target &&
            Set("commit_witness_bytes", "commit_witness_sha256").contains(column))
        case _ => false

  private def writeMarker(output: DataOutputStream, phase: MySqlOperationEffectPhase,
      shape: MySqlFormat3RelationShape, key: Vector[MySqlFormat3Cell],
      column: MySqlFormat3ColumnShape, original: MySqlFormat3Cell): Unit =
    if original == MySqlFormat3Cell.SqlNull then fail("self-reference markers cannot replace SQL NULL")
    MySqlModeEnvelopeV2Census.byte(output, 0x7f)
    MySqlRecoveryCanonicalBytes.writeToken(output, "FP_TYPED_MARKER_V1")
    MySqlModeEnvelopeV2Census.text(output, phaseName(phase))
    MySqlModeEnvelopeV2Census.text(output, shape.table)
    MySqlModeEnvelopeV2Census.int(output, shape.primaryKey.size)
    shape.primaryKey.foreach(name => writeCell(output, key(index(shape.table, name))))
    MySqlModeEnvelopeV2Census.text(output, column.name)
    MySqlModeEnvelopeV2Census.text(output, column.sqlDeclaration)
    MySqlModeEnvelopeV2Census.byte(output, 1)

  private def requireOperationHeader(row: Vector[MySqlFormat3Cell], request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, operationUuid: UUID, step: String, state: String): Unit =
    if !uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, OperationTable, "operation_uuid"), operationUuid) ||
      !uuidEquals(value(row, OperationTable, "workflow_uuid"), plan.workflowUuid) ||
      text(row, OperationTable, "step") != step ||
      text(row, OperationTable, "operation_state") != state ||
      !uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
      !uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid) ||
      !uuidEquals(value(row, OperationTable, "boot_uuid"), plan.tag.bootId) then
      fail("fixed " + step + " O row identity")

  private def validateReservationIdentity(row: Vector[MySqlFormat3Cell], request: Request,
      plan: MySqlOwnedRepositoryOperationPlan, profile: SourceLimitProfile, revision: Long,
      mutationOperation: UUID, state: String): Unit =
    val resource = MySqlRecoveryResourceKey.pod(request.podUuid, profile)
    val keyBudget = MySqlRecoveryCanonicalBytes.allocationBudgetFor(profile)
    val keyBytes = resource.copyCanonicalBytes(keyBudget, "POD reservation canonical identity verification")
    try
      value(row, ReservationTable, "resource_key") match
        case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(keyBytes) => ()
        case _ => fail("POD reservation full key bytes differ")
    finally Arrays.fill(keyBytes, 0.toByte)
    if text(row, ReservationTable, "resource_kind") != "POD" ||
      !uuidEquals(value(row, ReservationTable, "binding_uuid"), request.binding.bindingUuid) ||
      !uuidEquals(value(row, ReservationTable, "resource_pod_uuid"), request.podUuid) ||
      value(row, ReservationTable, "resource_unit_uuid") != MySqlFormat3Cell.SqlNull ||
      !binaryEquals(value(row, ReservationTable, "resource_key_hash"), resource.keyHashSha256) ||
      !uuidEquals(value(row, ReservationTable, "workflow_uuid"), plan.workflowUuid) ||
      !uuidEquals(value(row, ReservationTable, "operation_uuid"), plan.executeOperationUuid) ||
      text(row, ReservationTable, "reservation_state") != state ||
      long(row, ReservationTable, "reservation_revision") != revision ||
      !uuidEquals(value(row, ReservationTable, "last_mutation_operation_uuid"), mutationOperation) then
      fail("exact immutable POD reservation identity/revision/operation tuple")

  private[mysql] def modeRequest(operation: MySqlRepositoryOperation): Request = operation match
    case MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, owner, version, attempt, mode, pose) =>
      if binding == null || binding.cutoverState != PhysicalRecoveryCutoverState.Certified || binding.formatVersion != 2 ||
        pod == null || owner == null || attempt == null || mode == null || pose == null || version < 0L then
        throw new IllegalArgumentException("exact owned mode request for certified physical2 binding required")
      Request(binding, pod, owner, version, attempt, mode, pose)
    case _ => throw new IllegalArgumentException("mode effect codec accepts only SetOperatingModeTrackedIfOwned")

  /** 锁定 full20 已通过 M/B/W 身份核验后，将稳定业务拒绝与结构故障分开。 */
  private[mysql] def preflight(snapshot: Image,
      operation: MySqlRepositoryOperation): MySqlModePreflight =
    if snapshot == null then throw new IllegalArgumentException("locked mode preflight snapshot is required")
    val request = modeRequest(operation)
    val bindingRows = snapshot.relation(BindingTable).rows
    if bindingRows.size != 1 then fail("exact singleton F3 binding row for mode preflight")
    val binding = bindingRows.head
    if !uuidEquals(value(binding, BindingTable, "binding_uuid"), request.binding.bindingUuid) ||
      text(binding, BindingTable, "cutover_state") != "CERTIFIED" ||
      long(binding, BindingTable, "format_version") != 2L then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.BindingMismatch)
    val domainRows = snapshot.relation(DomainTable).rows.filter(row =>
      uuidEquals(value(row, DomainTable, "uuid"), request.podUuid))
    if domainRows.isEmpty then return MySqlModePreflight.Rejected(OperatingModeChangeRejection.UnknownPod)
    if domainRows.size != 1 then fail("unique domain primary key for mode preflight")
    val domain = domainRows.head
    if !uuidEquals(value(domain, DomainTable, "owner_uuid"), request.owner) then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.NotOwner)
    val active = long(domain, DomainTable, "is_active")
    if active != 1L then return MySqlModePreflight.Rejected(OperatingModeChangeRejection.Inactive)
    val version = long(domain, DomainTable, "state_version")
    val dimension = value(domain, DomainTable, "dimension_id") match
      case MySqlFormat3Cell.SignedInteger(actual) if actual >= Int.MinValue.toLong && actual <= Int.MaxValue.toLong => actual.toInt
      case _ => fail("active mode row has an exact dimension")
    val world = text(domain, DomainTable, "world_name")
    val x = double(domain, "pos_x")
    val z = double(domain, "pos_z")
    val currentMode = text(domain, DomainTable, "operating_mode")
    if !Set("NORMAL", "WORLD_IDLE").contains(currentMode) then fail("known active operating mode")
    val locatorRows = snapshot.relation(LocatorTable).rows.filter(row =>
      uuidEquals(value(row, LocatorTable, "binding_uuid"), request.binding.bindingUuid) &&
        uuidEquals(value(row, LocatorTable, "pod_uuid"), request.podUuid))
    if locatorRows.size != 1 then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
    val locator = locatorRows.head
    if !uuidEquals(value(locator, LocatorTable, "current_attempt_uuid"), request.attemptUuid) ||
      text(locator, LocatorTable, "locator_state") != "ROW_ACTIVE" ||
      text(locator, LocatorTable, "reason_code") != "ROW_ACTIVE" ||
      value(locator, LocatorTable, "domain_version") != MySqlFormat3Cell.SignedInteger(version) ||
      long(locator, LocatorTable, "target_dimension_id") != dimension.toLong then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
    val footprintRows = snapshot.relation(FootprintTable).rows.filter(row =>
      value(row, FootprintTable, "binding_uuid") == value(locator, LocatorTable, "binding_uuid") &&
        value(row, FootprintTable, "pod_uuid") == value(locator, LocatorTable, "pod_uuid") &&
        value(row, FootprintTable, "attempt_uuid") == value(locator, LocatorTable, "current_attempt_uuid") &&
        value(row, FootprintTable, "footprint_uuid") == value(locator, LocatorTable, "current_footprint_uuid"))
    if footprintRows.size != 1 then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
    val footprint = footprintRows.head
    val currentChunkX = chunk(x)
    val currentChunkZ = chunk(z)
    if text(footprint, FootprintTable, "seal_state") != "SEALED" ||
      long(footprint, FootprintTable, "origin_version") > version ||
      long(footprint, FootprintTable, "target_dimension_id") != dimension.toLong ||
      int(footprint, FootprintTable, "center_chunk_x") != currentChunkX ||
      int(footprint, FootprintTable, "center_chunk_z") != currentChunkZ then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
    val footprintChunks = snapshot.relation(ChunkTable).rows.filter(row =>
      value(row, ChunkTable, "binding_uuid") == value(footprint, FootprintTable, "binding_uuid") &&
        value(row, ChunkTable, "pod_uuid") == value(footprint, FootprintTable, "pod_uuid") &&
        value(row, ChunkTable, "attempt_uuid") == value(footprint, FootprintTable, "attempt_uuid") &&
        value(row, ChunkTable, "footprint_uuid") == value(footprint, FootprintTable, "footprint_uuid"))
    val currentChunks = footprintChunks.map(row => (int(row, ChunkTable, "chunk_x"), int(row, ChunkTable, "chunk_z"))).toSet
    val expectedChunks = (for cx <- currentChunkX - 2 to currentChunkX + 2;
      cz <- currentChunkZ - 2 to currentChunkZ + 2 yield (cx, cz)).toSet
    if footprintChunks.size != 25 || currentChunks.size != 25 || currentChunks != expectedChunks ||
      footprintChunks.exists(row => long(row, ChunkTable, "dimension_id") != dimension.toLong) then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.RecoveryObligationMismatch)
    val poseMatches = request.actualPose.worldName == world && request.actualPose.dimensionId == dimension &&
      request.actualPose.posX == x && request.actualPose.posY == double(domain, "pos_y") &&
      request.actualPose.posZ == z && request.actualPose.yaw.toDouble == double(domain, "yaw") &&
      request.actualPose.pitch.toDouble == double(domain, "pitch")
    val sameChunk = request.actualPose.worldName == world && request.actualPose.dimensionId == dimension &&
      chunk(request.actualPose.posX) == currentChunkX && chunk(request.actualPose.posZ) == currentChunkZ
    if version != request.expectedStateVersion then
      return MySqlModePreflight.Rejected(OperatingModeChangeRejection.VersionMismatch(version))
    if !sameChunk then return MySqlModePreflight.Rejected(OperatingModeChangeRejection.CrossChunkMovement)
    if currentMode == request.targetMode.databaseValue && poseMatches then MySqlModePreflight.AlreadySet
    else if version == request.expectedStateVersion then MySqlModePreflight.Execute
    else MySqlModePreflight.Rejected(OperatingModeChangeRejection.VersionMismatch(version))

  private[mysql] def operationRows(snapshot: Image,
      operation: MySqlRepositoryOperation): Vector[Vector[MySqlFormat3Cell]] =
    val request = modeRequest(operation)
    snapshot.relation(OperationTable).rows.filter(row =>
      uuidEquals(value(row, OperationTable, "binding_uuid"), request.binding.bindingUuid) &&
        (uuidEquals(value(row, OperationTable, "pod_uuid"), request.podUuid) ||
          uuidEquals(value(row, OperationTable, "attempt_uuid"), request.attemptUuid)))

  private[mysql] def rowOperationUuid(row: Vector[MySqlFormat3Cell]): UUID =
    uuidCell(value(row, OperationTable, "operation_uuid"), "operation_uuid")

  private def oneDomain(snapshot: Image, binding: UUID, pod: UUID): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(DomainTable).rows.filter(row =>
      uuidEquals(value(row, DomainTable, "binding_uuid"), binding) && uuidEquals(value(row, DomainTable, "uuid"), pod))
    if rows.size != 1 then fail("exact binding-owned domain row")
    rows.head

  private def oneLocator(snapshot: Image, binding: UUID, pod: UUID): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(LocatorTable).rows.filter(row =>
      uuidEquals(value(row, LocatorTable, "binding_uuid"), binding) && uuidEquals(value(row, LocatorTable, "pod_uuid"), pod))
    if rows.size != 1 then fail("exact current locator row")
    rows.head

  private def oneCurrentFootprint(snapshot: Image, request: Request): Vector[MySqlFormat3Cell] =
    val locator = oneLocator(snapshot, request.binding.bindingUuid, request.podUuid)
    val rows = snapshot.relation(FootprintTable).rows.filter(row =>
      value(row, FootprintTable, "binding_uuid") == value(locator, LocatorTable, "binding_uuid") &&
        value(row, FootprintTable, "pod_uuid") == value(locator, LocatorTable, "pod_uuid") &&
        value(row, FootprintTable, "attempt_uuid") == value(locator, LocatorTable, "current_attempt_uuid") &&
        value(row, FootprintTable, "footprint_uuid") == value(locator, LocatorTable, "current_footprint_uuid"))
    if rows.size != 1 then fail("current locator must point to one exact immutable footprint")
    rows.head

  private def currentChunks(snapshot: Image,
      request: Request): Vector[Vector[MySqlFormat3Cell]] =
    val footprint = oneCurrentFootprint(snapshot, request)
    snapshot.relation(ChunkTable).rows.filter(row =>
      value(row, ChunkTable, "binding_uuid") == value(footprint, FootprintTable, "binding_uuid") &&
        value(row, ChunkTable, "pod_uuid") == value(footprint, FootprintTable, "pod_uuid") &&
        value(row, ChunkTable, "attempt_uuid") == value(footprint, FootprintTable, "attempt_uuid") &&
        value(row, ChunkTable, "footprint_uuid") == value(footprint, FootprintTable, "footprint_uuid"))

  private def oneWriter(snapshot: Image): Vector[MySqlFormat3Cell] =
    val rows = snapshot.relation(WriterTable).rows.filter(row => long(row, WriterTable, "singleton_fixed1") == 1L)
    if rows.size != 1 then fail("exact singleton W row")
    rows.head

  private def rowsForPod(snapshot: Image, table: String,
      binding: UUID, pod: UUID): Vector[Vector[MySqlFormat3Cell]] =
    snapshot.relation(table).rows.filter(row =>
      uuidEquals(value(row, table, "binding_uuid"), binding) &&
        uuidEquals(value(row, table, "resource_pod_uuid"), pod))

  private def operationKey(bindingUuid: UUID, operationUuid: UUID): Vector[MySqlFormat3Cell] =
    Vector(binary(bindingUuid), binary(operationUuid))

  private def keyed(snapshot: Image, table: String): Map[Vector[MySqlFormat3Cell], Vector[MySqlFormat3Cell]] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    snapshot.relation(table).rows.map(row =>
      shape.primaryKey.map(name => row(index(table, name))) -> row).toMap

  private def unchanged(before: Image, after: Image, table: String): Unit =
    if before.relation(table).rows != after.relation(table).rows then fail("phase changed unrelated full20 relation " + table)

  private def sameExcept(left: Vector[MySqlFormat3Cell], right: Vector[MySqlFormat3Cell],
      table: String, mutableNames: Set[String]): Boolean =
    val columns = MySqlFormat3RelationShapes.forTable(table).columns
    left.size == columns.size && right.size == columns.size &&
      columns.indices.forall(i => mutableNames.contains(columns(i).name) || left(i) == right(i))

  private[mysql] def value(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    val indexValue = index(table, name)
    row(indexValue)

  private def index(table: String, name: String): Int =
    val indexValue = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if indexValue < 0 then fail("fixed selector column " + table + "." + name)
    indexValue

  private def text(row: Vector[MySqlFormat3Cell], table: String, name: String): String =
    value(row, table, name) match
      case MySqlFormat3Cell.Text(result) => result
      case _ => fail("typed text cell " + table + "." + name)

  private def long(row: Vector[MySqlFormat3Cell], table: String, name: String): Long =
    value(row, table, name) match
      case MySqlFormat3Cell.SignedInteger(result) => result
      case _ => fail("typed integer cell " + table + "." + name)

  private def int(row: Vector[MySqlFormat3Cell], table: String, name: String): Int =
    Math.toIntExact(long(row, table, name))

  private def double(row: Vector[MySqlFormat3Cell], name: String): Double =
    value(oneDomainByUuidless(row), DomainTable, name) match
      case MySqlFormat3Cell.Floating(bits) => java.lang.Double.longBitsToDouble(bits)
      case _ => fail("typed DOUBLE cell " + DomainTable + "." + name)

  private def oneDomainByUuidless(row: Vector[MySqlFormat3Cell]): Vector[MySqlFormat3Cell] = row

  private def optionalDate(row: Vector[MySqlFormat3Cell], table: String, name: String): Option[LocalDateTime] =
    value(row, table, name) match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.DateTime(result) => Some(result)
      case _ => fail("typed nullable DATETIME cell " + table + "." + name)

  private def binaryOfLength(value: MySqlFormat3Cell, length: Int): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => length < 0 || bytes.length == length
    case _ => false

  private def binaryEquals(value: MySqlFormat3Cell, expected: Vector[Byte]): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsVector(expected)
    case _ => false

  private[mysql] def uuidEquals(value: MySqlFormat3Cell, expected: UUID): Boolean = value match
    case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(expected)
    case _ => false

  private def uuidCell(value: MySqlFormat3Cell, label: String): UUID = value match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
      val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone { new MySqlFormat3ReadBudget(SourceLimitProfile.of(1, 1, 1, 1, 1, 512, 512, 1), 512) }
      budget.reserveRetainedMemory(64L, label + " UUID object before allocation")
      var most = 0L
      var least = 0L
      var index = 0
      while index < 8 do
        most = (most << 8) | (bytes.byteAt(index).toLong & 0xffL)
        least = (least << 8) | (bytes.byteAt(index + 8).toLong & 0xffL)
        index += 1
      new UUID(most, least)
    case _ => fail("16-byte UUID " + label)

  private def uuidBytes(value: MySqlFormat3Cell): Vector[Byte] = value match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 => keyBytes(bytes)
    case _ => Vector.empty

  private def binary(value: UUID): MySqlFormat3Cell =
    if value == null then throw new IllegalArgumentException("exact UUID key is required")
    val bytes = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))

  private def keyBytes(value: MySqlFormat3Binary): Vector[Byte] =
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone { new MySqlFormat3ReadBudget(SourceLimitProfile.of(1, 1, 1, 1, 1, 1024, 1024, 1), 1024) }
    budget.reserveRetainedMemory(Math.addExact(64L, Math.multiplyExact(value.length.toLong, 16L)),
      "mode actual immutable key vector before allocation")
    Vector.tabulate(value.length)(value.byteAt)

  private def addUuid(set: mutable.Set[Vector[Byte]], cell: MySqlFormat3Cell): Unit =
    cell match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 => set.add(keyBytes(bytes))
      case _ => ()

  private def isOperationReference(name: String): Boolean =
    name.endsWith("_operation_uuid") && name != "operation_uuid"

  private def referencesOperation(row: Vector[MySqlFormat3Cell], table: String,
      ids: mutable.Set[Vector[Byte]]): Boolean =
    MySqlFormat3RelationShapes.forTable(table).columns.exists(column =>
      isOperationReference(column.name) && ids.contains(uuidBytes(value(row, table, column.name))))

  private def carrierTuple(row: Vector[MySqlFormat3Cell], table: String): (String, Vector[Byte]) =
    (value(row, table, "resource_kind"), value(row, table, "carrier_key_hash")) match
      case (MySqlFormat3Cell.Text(kind), MySqlFormat3Cell.Binary(hash)) => kind -> keyBytes(hash)
      case _ => "" -> Vector.empty

  private def phaseName(phase: MySqlOperationEffectPhase): String = phase match
    case MySqlOperationEffectPhase.Reserve => "RESERVE"
    case MySqlOperationEffectPhase.Execute => "EXECUTE"

  private def writeUuid(output: DataOutputStream, value: UUID): Unit =
    MySqlRecoveryCanonicalBytes.writeUuid(output, value)

  private def writeCell(output: DataOutputStream, value: MySqlFormat3Cell): Unit =
    val recorded = MySqlModeEnvelopeV2Census.beginTyped(output, value)
    try
      value match
        case MySqlFormat3Cell.SqlNull => MySqlModeEnvelopeV2Census.byte(output, 0)
        case MySqlFormat3Cell.Text(textValue) => MySqlModeEnvelopeV2Census.byte(output, 1); MySqlModeEnvelopeV2Census.text(output, textValue)
        case MySqlFormat3Cell.Binary(bytes) => MySqlModeEnvelopeV2Census.byte(output, 2); MySqlModeEnvelopeV2Census.int(output, bytes.length); MySqlModeEnvelopeV2Census.binary(output, bytes)
        case MySqlFormat3Cell.SignedInteger(number) => MySqlModeEnvelopeV2Census.byte(output, 3); MySqlModeEnvelopeV2Census.long(output, number)
        case MySqlFormat3Cell.Floating(bits) => MySqlModeEnvelopeV2Census.byte(output, 4); MySqlModeEnvelopeV2Census.long(output, bits)
        case MySqlFormat3Cell.DateTime(dateTime) =>
          MySqlModeEnvelopeV2Census.byte(output, 5)
          MySqlModeEnvelopeV2Census.int(output, dateTime.getYear)
          MySqlModeEnvelopeV2Census.byte(output, dateTime.getMonthValue)
          MySqlModeEnvelopeV2Census.byte(output, dateTime.getDayOfMonth)
          MySqlModeEnvelopeV2Census.byte(output, dateTime.getHour)
          MySqlModeEnvelopeV2Census.byte(output, dateTime.getMinute)
          MySqlModeEnvelopeV2Census.byte(output, dateTime.getSecond)
          MySqlModeEnvelopeV2Census.int(output, dateTime.getNano / 1000)
    finally MySqlModeEnvelopeV2Census.endTyped(output, recorded)

  private def chunk(position: Double): Int =
    val floor = Math.floor(position)
    if !java.lang.Double.isFinite(floor) || floor < Int.MinValue.toDouble || floor > Int.MaxValue.toDouble then
      fail("pose is outside representable Minecraft block coordinates")
    Math.floorDiv(floor.toInt, 16)

  private def fail(reason: String): Nothing =
    throw new SQLDataException("MySQL format3 mode O/R effect violates " + reason)

/** 单tuple有界RAW产物责任；先登记holder再分配cache，不发行SQL/native能力。 */
private[mysql] final class MySqlModeRawEnvelopeConstruction(val originalRoot: MySqlFormat3ReadBudget, capacity: Int,
    private[mysql] var originalRows: Vector[Vector[MySqlFormat3Cell]] = Vector.empty):
  require(originalRoot != null && !originalRoot.isClosed && capacity >= 0, "original RAW construction root/capacity")
  private val ownerThread = Thread.currentThread()
  private def requireOwner(): Unit =
    require(Thread.currentThread() eq ownerThread, "original RAW construction owner thread")
  private var rows: Array[MySqlRecoveryOperationRawEnvelope] = null
  private var count = 0
  private var firstRetainedCharge = -1L
  private var firstRetainedPeak = -1L
  private var reserveBeforeRows: Vector[MySqlRecoveryOperationRawEnvelope] = null
  private var reserveTargetRows: Vector[MySqlRecoveryOperationRawEnvelope] = null
  private var executeBeforeRows: Vector[MySqlRecoveryOperationRawEnvelope] = null
  private var executeTargetRows: Vector[MySqlRecoveryOperationRawEnvelope] = null
  private var reserveEffect: MySqlModeOperationEffect = null
  private var executeEffect: MySqlModeOperationEffect = null
  private var dropped = false
  def isOriginalHistoricalRow(row: Vector[MySqlFormat3Cell]): Boolean =
    requireOwner(); require(!dropped)
    originalRows.exists(_ eq row)
  def initialize(): Unit =
    requireOwner()
    require(rows == null && !dropped, "one RAW cache allocation")
    originalRoot.reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(capacity.toLong, 64L)), "mode bounded RAW cache array before allocation")
    rows = new Array[MySqlRecoveryOperationRawEnvelope](capacity)
  def retain(value: MySqlRecoveryOperationRawEnvelope): Unit =
    requireOwner()
    require(rows != null && !dropped && value != null && count < capacity, "bounded RAW production retention")
    rows(count) = value
    count += 1
    if count == 1 then
      firstRetainedCharge = originalRoot.accountedMemoryBytes
      firstRetainedPeak = originalRoot.successfulReserveHighWaterBytes
  def retainRows(phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      value: Vector[MySqlRecoveryOperationRawEnvelope]): Unit =
    requireOwner()
    require(value != null && rows != null && !dropped, "actual returned RAW vector responsibility")
    (phase, image) match
      case (MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before) =>
        require(reserveBeforeRows == null, "one reserve before vector"); reserveBeforeRows = value
      case (MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Target) =>
        require(reserveTargetRows == null, "one reserve target vector"); reserveTargetRows = value
      case (MySqlOperationEffectPhase.Execute, MySqlOperationEffectImage.Before) =>
        require(executeBeforeRows == null, "one execute before vector"); executeBeforeRows = value
      case (MySqlOperationEffectPhase.Execute, MySqlOperationEffectImage.Target) =>
        require(executeTargetRows == null, "one execute target vector"); executeTargetRows = value
  def retainEffect(value: MySqlModeOperationEffect): Unit =
    requireOwner()
    require(value != null && rows != null && !dropped, "live actual phase effect responsibility")
    value.phase match
      case MySqlOperationEffectPhase.Reserve =>
        require(reserveEffect == null, "one actual reserve effect")
        reserveEffect = value
      case MySqlOperationEffectPhase.Execute =>
        require(executeEffect == null, "one actual execute effect")
        executeEffect = value
  def firstRawAccountedBytes: Long =
    requireOwner()
    firstRetainedCharge
  def firstRawSuccessfulReservePeakBytes: Long =
    requireOwner()
    firstRetainedPeak
  def findSameRow(row: Vector[MySqlFormat3Cell],
      deploymentInputs: Option[MySqlScriptSchemaInputs]): Option[MySqlRecoveryOperationRawEnvelope] =
    requireOwner()
    require(rows != null && !dropped, "live RAW construction")
    var index = 0
    while index < count do
      if rows(index).row eq row then
        rows(index).requireInputs(deploymentInputs)
        return Some(rows(index))
      index += 1
    None
  def retainedCount: Int = count
  def dropReferences(): Unit =
    requireOwner()
    originalRows = Vector.empty
    if rows != null then
      var index = 0
      while index < rows.length do
        rows(index) = null
        index += 1
    rows = null
    reserveBeforeRows = null
    reserveTargetRows = null
    executeBeforeRows = null
    executeTargetRows = null
    reserveEffect = null
    executeEffect = null
    count = 0
    dropped = true
