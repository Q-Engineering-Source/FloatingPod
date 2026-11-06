package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, PodOperatingMode}
import com.wi.floatingpod.server.{B1ResumeRecordCanonical, B1ResumeRecordV5}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{DataOutputStream, EOFException, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 固定 mode 三图 DATA；解析旧身份不会重建 retainedHandle、JDBC owner 或 native scope。 */
private[mysql] object MySqlModeWitnessCodec:
  val Domain = "FP_B1_MODE_FULL20_ORIGINAL_WITNESS_BE_V1"
  private val PlanDomain = "FP_B1_MODE_PLAN_DATA_BE_V1"
  private val EffectDomain = "FP_B1_MODE_EFFECT_DATA_BE_V1"

  final case class SourceDescriptor(predecessorKind: Int, predecessorSequence: Long,
      volume: Long, fileId: Vector[Byte], predecessor: MySqlFormat3Binary,
      predecessorSha: Vector[Byte], anchorSha: Vector[Byte], originalReceipt: MySqlFormat3Binary):
    require((predecessorKind == 4 || predecessorKind == 5) && predecessorSequence >= 0L &&
      fileId != null && fileId.size == 16 && predecessor != null && originalReceipt != null &&
      predecessorSha != null && predecessorSha.size == 32 && anchorSha != null && anchorSha.size == 32,
      "complete DATA source descriptor required; no installed/native authority")

  /** 只含实际 count writer 的固定标量；不是容量/SQL/native资格。 */
  final case class EnvelopeSizeFacts(sourceEnd: Long, contextEnd: Long,
      image0End: Long, image1End: Long, image2End: Long,
      reserveEffectEnd: Long, reserveWitnessEnd: Long,
      executeEffectEnd: Long, executeWitnessEnd: Long, total: Long,
      nativeLimit: Long, retainedLimit: Long, accounted: Long, remaining: Long, highWater: Long)

  final class EnvelopeCapacityFailure(val facts: EnvelopeSizeFacts)
      extends SQLDataException(s"mode witness envelope exceeds explicit native/retained bound: $facts")

  final class Encoded private[MySqlModeWitnessCodec] (val sizeFacts: EnvelopeSizeFacts, val bytes: MySqlFormat3Binary,
      val root: MySqlFormat3ReadBudget, val source: SourceDescriptor,
      val images: Vector[MySqlFormat3FullSnapshot])

  final class Decoded private[MySqlModeWitnessCodec] (val source: SourceDescriptor,
      val plan: MySqlOwnedRepositoryOperationPlan,
      val context: MySqlRepositoryOperationContextIdentity, val plannedAt: LocalDateTime,
      val locatorRevision: Long, val images: Vector[MySqlFormat3FullSnapshot],
      val effects: Vector[MySqlModeOperationEffect])

  /** 在读取第一字段前由 caller 保留；失败后仍持所有已分配 blob、typed image、原 cause。 */
  final class DecodeDraft private[mysql] (val root: MySqlFormat3ReadBudget):
    private[MySqlModeWitnessCodec] var input: MySqlFormat3Binary = null
    private[MySqlModeWitnessCodec] var blobs = Vector.empty[MySqlFormat3Binary]
    private[MySqlModeWitnessCodec] var imagePayloads = Vector.empty[MySqlFormat3CanonicalPayload]
    private[MySqlModeWitnessCodec] var decodedChildren: Array[MySqlFormat3ReadBudget] = null
    private[MySqlModeWitnessCodec] var images = Vector.empty[MySqlFormat3FullSnapshot]
    private[MySqlModeWitnessCodec] var rawEnvelopes = Vector.empty[MySqlRecoveryOperationRawEnvelope]
    private[MySqlModeWitnessCodec] var partialEffects = Vector.empty[MySqlModeOperationEffect]
    private[MySqlModeWitnessCodec] var partialPhases = Vector.empty[MySqlModePhaseCanonical]
    private[MySqlModeWitnessCodec] var partialSource: SourceDescriptor = null
    private[MySqlModeWitnessCodec] var partialPlan: MySqlOwnedRepositoryOperationPlan = null
    private[MySqlModeWitnessCodec] var partialContext: MySqlRepositoryOperationContextIdentity = null
    private[MySqlModeWitnessCodec] var result: Decoded = null
    private[MySqlModeWitnessCodec] var attempted = false
    private[MySqlModeWitnessCodec] var failure: Throwable = null
    def retainedFailure: Option[Throwable] = Option(failure)
    def decodedImageCount: Int = images.size
    def retainedBlobCount: Int = blobs.size

  def newDecodeDraft(root: MySqlFormat3ReadBudget): DecodeDraft =
    require(root != null, "original DATA root required")
    requireRoot(root, root.profile)
    root.reserveRetainedMemory(512L, "mode witness partial DATA draft")
    new DecodeDraft(root)

  private def requireRoot(root: MySqlFormat3ReadBudget, profile: SourceLimitProfile): Unit =
    require(root != null && profile != null && !root.isClosed && (root.profile eq profile),
      "exact open original mode witness root/profile required")

  private def blob(output: DataOutputStream, value: MySqlFormat3Binary): Unit =
    MySqlModeEnvelopeV2Census.int(output, value.length)
    MySqlModeEnvelopeV2Census.binary(output, value)
  private def canonical(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes): Unit =
    MySqlModeEnvelopeV2Census.int(output, value.length)
    MySqlModeEnvelopeV2Census.canonical(output, value)
  private def text(output: DataOutputStream, value: String): Unit = MySqlModeEnvelopeV2Census.text(output, value)
  private def uuid(output: DataOutputStream, value: UUID): Unit = MySqlRecoveryCanonicalBytes.writeUuid(output, value)
  private def hash(output: DataOutputStream, value: Vector[Byte]): Unit =
    if value == null || value.size != 32 then throw new SQLDataException("mode witness exact SHA256 required")
    value.foreach(v => MySqlModeEnvelopeV2Census.byte(output, v.toInt))

  private[mysql] def writeDateTime(output: DataOutputStream, value: LocalDateTime): Unit =
    if value == null || value.getYear < 1000 || value.getYear > 9999 || value.getNano % 1000 != 0 then
      throw new SQLDataException("mode witness DATETIME(6) is invalid")
    MySqlModeEnvelopeV2Census.int(output, value.getYear)
    MySqlModeEnvelopeV2Census.byte(output, value.getMonthValue)
    MySqlModeEnvelopeV2Census.byte(output, value.getDayOfMonth)
    MySqlModeEnvelopeV2Census.byte(output, value.getHour)
    MySqlModeEnvelopeV2Census.byte(output, value.getMinute)
    MySqlModeEnvelopeV2Census.byte(output, value.getSecond)
    MySqlModeEnvelopeV2Census.int(output, value.getNano / 1000)

  private def writePlan(output: DataOutputStream, plan: MySqlOwnedRepositoryOperationPlan): Unit =
    text(output, PlanDomain)
    uuid(output, plan.tag.bootId)
    MySqlModeEnvelopeV2Census.long(output, plan.tag.bootGeneration)
    MySqlModeEnvelopeV2Census.long(output, plan.tag.connectionGeneration)
    uuid(output, plan.tag.operationId)
    uuid(output, plan.workflowUuid)
    uuid(output, plan.reserveOperationUuid)
    uuid(output, plan.executeOperationUuid)
    text(output, plan.requestKind)
    canonical(output, plan.requestIdentity.canonical)
    val digest = plan.exactLimitProfile.sha256()
    digest.foreach(b => MySqlModeEnvelopeV2Census.byte(output, b.toInt))
    MySqlModeEnvelopeV2Census.int(output, 1)

  private def writeRaw(output: DataOutputStream, values: Vector[MySqlRecoveryOperationRawEnvelope]): Unit =
    MySqlModeEnvelopeV2Census.int(output, values.size)
    values.foreach { value =>
      uuid(output, value.operationUuid)
      canonical(output, value.canonical)
      hash(output, value.sha256)
    }

  private def writeEffect(output: DataOutputStream, effect: MySqlModeOperationEffect,
      phase: MySqlModePhaseCanonical, plan: MySqlOwnedRepositoryOperationPlan): Unit =
    text(output, EffectDomain)
    val reserve = effect.phase == MySqlOperationEffectPhase.Reserve
    MySqlModeEnvelopeV2Census.int(output, if reserve then 1 else 2)
    uuid(output, if reserve then plan.reserveOperationUuid else plan.executeOperationUuid)
    MySqlModeEnvelopeV2Census.int(output, if reserve then 0 else 1)
    MySqlModeEnvelopeV2Census.int(output, if reserve then 1 else 2)
    canonical(output, phase.scope)
    canonical(output, phase.operationContextBytes)
    canonical(output, effect.preimage)
    canonical(output, effect.target)
    canonical(output, effect.fingerprint)
    writeRaw(output, effect.preimageRawOperationEnvelopes)
    writeRaw(output, effect.rawOperationEnvelopes)
    MySqlModeEnvelopeV2Census.int(output, 1)

  /** 只走一次实际 writer；count 只观察不可变已物化值，不接受 arbitrary callback。 */
  private def writeEnvelope(output: DataOutputStream, source: SourceDescriptor,
      m: MySqlModeWorkflowMaterialization, context: MySqlRepositoryOperationContextIdentity,
      locator: Long, images: Vector[MySqlFormat3FullSnapshot], imageLengths: Vector[Int],
      phases: Vector[MySqlModePhaseCanonical], contextBytes: MySqlRecoveryCanonicalBytes,
      count: Counter = null): Unit =
    text(output, Domain)
    val profile = m.preimage.limitProfile.canonicalBytes()
    MySqlModeEnvelopeV2Census.int(output, profile.length)
    output.write(profile)
    MySqlModeEnvelopeV2Census.int(output, source.predecessorKind)
    MySqlModeEnvelopeV2Census.long(output, source.predecessorSequence)
    MySqlModeEnvelopeV2Census.long(output, source.volume)
    source.fileId.foreach(v => MySqlModeEnvelopeV2Census.byte(output, v.toInt))
    blob(output, source.predecessor)
    hash(output, source.predecessorSha)
    hash(output, source.anchorSha)
    blob(output, source.originalReceipt)
    if count != null then count.sourceEnd = count.length
    writePlan(output, m.exactPlan)
    canonical(output, contextBytes)
    writeDateTime(output, m.plannedAt)
    MySqlModeEnvelopeV2Census.long(output, locator)
    if count != null then count.contextEnd = count.length
    MySqlModeEnvelopeV2Census.int(output, 3)
    images.zipWithIndex.foreach { (image, index) =>
      MySqlModeEnvelopeV2Census.int(output, index)
      MySqlModeEnvelopeV2Census.int(output, imageLengths(index))
      MySqlFormat3SnapshotCanonical.writeSnapshot(image, output, image.allocationBudget)
      if count != null then
        if index == 0 then count.image0End = count.length
        else if index == 1 then count.image1End = count.length
        else count.image2End = count.length
    }
    MySqlModeEnvelopeV2Census.int(output, 2)
    for index <- 0 until 2 do
      writeEffect(output, m.effects(index), phases(index), m.exactPlan)
      if count != null then
        if index == 0 then count.reserveEffectEnd = count.length
        else count.executeEffectEnd = count.length
      canonical(output, if index == 0 then m.originalReserveWitness else m.originalExecuteWitness)
      if count != null then
        if index == 0 then count.reserveWitnessEnd = count.length
        else count.executeWitnessEnd = count.length
    MySqlModeEnvelopeV2Census.int(output, 1)

  def encode(source: SourceDescriptor, draft: MySqlModeOriginalWitnessDraft): Encoded = encodeInternal(source, draft, null)

  private def encodeInternal(source: SourceDescriptor, draft: MySqlModeOriginalWitnessDraft,
      census: MySqlModeEnvelopeV2Census.Partial): Encoded =
    draft.beginEncoding()
    val root = draft.originalRoot
    val m = draft.value
    requireRoot(root, m.preimage.limitProfile)
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      // 原2048固定wrapper预留中1024字节用于固定count标量及immutable facts；无额外reserve遮蔽size首因。
      root.reserveRetainedMemory(2048L, "mode fixed encode collections/phase metadata including 1024-byte size facts")
      m.verify()
      validateSource(source, root)
      val images = Vector(m.preimage, m.reserveTarget, m.executeTarget)
      val imageLengths = images.map { image =>
        require(!image.allocationBudget.isClosed && image.allocationBudget.sharesAllocationLedger(root) &&
          (image.limitProfile eq root.profile), "actual open same-ledger full20 image required")
        root.reserveTemporaryMemory(512L, "mode full20 image count workspace")
        var primary: Throwable = null
        try
          val counter = new Counter
          val output = new DataOutputStream(counter)
          MySqlFormat3SnapshotCanonical.writeSnapshot(image, output, root)
          output.flush()
          val length = counter.length
          if length <= 0L || length > root.profile.maxRetainedBytes() then
            throw new SQLDataException("mode full20 canonical image exceeds explicit retained bound")
          MySqlFormat3JvmArrayLimit.requireLength(length, "mode full20 image framing")
        catch
          case cause: Throwable =>
            primary = cause
            throw cause
        finally
          try root.releaseTemporaryMemory(512L, "mode image count workspace release")
          catch
            case cleanup: Throwable =>
              if primary == null then throw cleanup
              else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      }
      val context = draft.originalContext
      val reservePhase = MySqlModeOperationEffectCodec.requireRetainedCanonicalPhase(m.exactPlan, m.exactOperation, context,
        m.originalLocatorRevision, MySqlOperationEffectPhase.Reserve, m.preimage, m.reserveTarget,
        draft.producerPhase(MySqlModeWitnessProducerStage.ReservePhase), root)
      draft.retainPhase(reservePhase)
      val executePhase = MySqlModeOperationEffectCodec.requireRetainedCanonicalPhase(m.exactPlan, m.exactOperation, context,
        m.originalLocatorRevision, MySqlOperationEffectPhase.Execute, m.reserveTarget, m.executeTarget,
        draft.producerPhase(MySqlModeWitnessProducerStage.ExecutePhase), root)
      draft.retainPhase(executePhase)
      val phases = Vector(reservePhase, executePhase)
      val contextBytes = context.canonical
      draft.retainContextBytes(contextBytes)
      root.reserveTemporaryMemory(512L, "mode witness exact writer/count workspace")
      var primary: Throwable = null
      try
        val counter = new Counter
        val counting = new DataOutputStream(counter)
        writeEnvelope(counting, source, m, context, m.originalLocatorRevision, images, imageLengths, phases, contextBytes, counter)
        counting.flush()
        val length = counter.length
        val facts = EnvelopeSizeFacts(counter.sourceEnd, counter.contextEnd,
          counter.image0End, counter.image1End, counter.image2End,
          counter.reserveEffectEnd, counter.reserveWitnessEnd,
          counter.executeEffectEnd, counter.executeWitnessEnd, length,
          root.profile.maxNativeArtifactBytes(), root.profile.maxRetainedBytes(),
          root.accountedMemoryBytes, root.availableMemoryBytes, root.successfulReserveHighWaterBytes)
        if census != null then census.establishBaseline(facts)
        if length <= 0L || length > root.profile.maxNativeArtifactBytes() || length > root.profile.maxRetainedBytes() then
          val cause = new EnvelopeCapacityFailure(facts)
          if census != null then census.businessFailure = cause
          throw cause
        if census != null then null // 诊断只保 count 原件，不配置任何 final payload。
        else
          val size = MySqlFormat3JvmArrayLimit.requireLength(length, "mode witness exact output")
          root.reserveRetainedMemory(Math.addExact(length, 192L), "mode witness complete final payload/holder")
          val content = new Array[Byte](size)
          val retainedBinary = MySqlFormat3Binary.takeOwned(content)
          draft.retainEncodingBinary(retainedBinary)
          val sink = new ExactOutput(content)
          val output = new DataOutputStream(sink)
          writeEnvelope(output, source, m, context, m.originalLocatorRevision, images, imageLengths, phases, contextBytes)
          output.flush()
          val encoded = new Encoded(facts, { sink.finish(); retainedBinary }, root, source, images)
          draft.encoded = encoded
          encoded
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try root.releaseTemporaryMemory(512L, "mode witness writer/count release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    }
    catch
      case cause: Throwable =>
        draft.recordFailure(cause)
        throw cause

  /** 独立 DATA 诊断；返回原 draft 强持的 observation，failure 非空即明确 NG。 */
  private[mysql] def census(source: SourceDescriptor, draft: MySqlModeOriginalWitnessDraft): MySqlModeEnvelopeV2Census.Partial =
    censusUsing(source, draft, MySqlModeEnvelopeV2Census.Method.FlatV2)

  /** 仅原writer recipe计量；不发行新格式artifact，不改变产品encode/decode。 */
  private[mysql] def censusRecipes(source: SourceDescriptor, draft: MySqlModeOriginalWitnessDraft): MySqlModeEnvelopeV2Census.Partial =
    censusUsing(source, draft, MySqlModeEnvelopeV2Census.Method.RecipeV3)

  private def censusUsing(source: SourceDescriptor, draft: MySqlModeOriginalWitnessDraft,
      method: MySqlModeEnvelopeV2Census.Method): MySqlModeEnvelopeV2Census.Partial =
    require(draft != null)
    draft.requireOwner()
    val root = draft.originalRoot
    root.reserveRetainedMemory(512L, "census partial original responsibility BEFORE allocation")
    val partial = new MySqlModeEnvelopeV2Census.Partial(root, draft, method)
    draft.retainCensus(partial)
    var observing = false
    try
      try encodeInternal(source, draft, partial)
      catch
        case cause: EnvelopeCapacityFailure =>
          require(partial.businessFailure eq cause, "original V1 count capacity cause")
          val cleanup = cause.getSuppressed
          if cleanup.nonEmpty then partial.recordFailure(cleanup(0))
        case cause: Throwable => partial.recordFailure(cause)
      if partial.failure == null then MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        root.beginCensusObservation(partial)
        observing = true
        partial.initialize()
        MySqlModeEnvelopeV2Census.captureAll(source, partial)
      }
    catch case cause: Throwable => partial.recordFailure(cause)
    finally
      if observing then
        try root.endCensusObservation(partial)
        catch case cleanup: Throwable => partial.recordFailure(cleanup)
    partial

  private[mysql] def writeCensusEffect(output: DataOutputStream, effect: MySqlModeOperationEffect,
      phase: MySqlModePhaseCanonical, plan: MySqlOwnedRepositoryOperationPlan): Unit =
    writeEffect(output, effect, phase, plan)

  private def validateSource(source: SourceDescriptor, root: MySqlFormat3ReadBudget): Unit =
    require(source != null, "source descriptor DATA required")
    root.reserveTemporaryMemory(Math.addExact(Math.multiplyExact(source.predecessor.length.toLong, 8L), 32768L),
      "mode source DATA decoder workspace")
    var primary: Throwable = null
    var bytes: Array[Byte] = null
    var record: com.wi.floatingpod.server.B1ResumeRecord = null
    var completed = false
    try
      bytes = source.predecessor.copyBytes(root, "mode DATA predecessor copy")
      record = if source.predecessorKind == 4 then B1ResumeRecordCanonical.decodeData(bytes)
        else B1ResumeRecordV5.decodeData(bytes).transition()
      if record.sequence() != source.predecessorSequence ||
        MessageDigest.getInstance("SHA-256").digest(bytes).toVector != source.predecessorSha ||
        record.sourceAnchorSha256().toVector != source.anchorSha ||
        !source.originalReceipt.equalsBytes(record.originalReceipt()) then
        throw new SQLDataException("mode source DATA predecessor differs from complete source facts")
      completed = true
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      record = null
      if bytes != null then
        val charge = Math.addExact(bytes.length.toLong, 64L)
        java.util.Arrays.fill(bytes, 0.toByte)
        bytes = null
        try root.releaseRetainedMemory(charge, "mode predecessor scratch actual drop")
        catch
          case cleanup: Throwable =>
            if primary == null then primary = cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      try root.releaseTemporaryMemory(Math.addExact(Math.multiplyExact(source.predecessor.length.toLong, 8L), 32768L),
        "mode source DATA decoder workspace release")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      if completed && primary != null then throw primary

  private def requireProfileBytes(binary: MySqlFormat3Binary, root: MySqlFormat3ReadBudget): Unit =
    root.reserveTemporaryMemory(161L, "mode profile97 comparison scratch")
    var bytes: Array[Byte] = null
    var primary: Throwable = null
    try
      bytes = root.profile.canonicalBytes()
      if !binary.equalsBytes(bytes) then throw new SQLDataException("mode exact profile bytes differ")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      if bytes != null then java.util.Arrays.fill(bytes, 0.toByte)
      bytes = null
      try root.releaseTemporaryMemory(161L, "mode profile comparison drop")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private final class Counter extends OutputStream:
    var length = 0L
    var sourceEnd = 0L
    var contextEnd = 0L
    var image0End = 0L
    var image1End = 0L
    var image2End = 0L
    var reserveEffectEnd = 0L
    var reserveWitnessEnd = 0L
    var executeEffectEnd = 0L
    var executeWitnessEnd = 0L
    override def write(value: Int): Unit = length = Math.addExact(length, 1L)
    override def write(bytes: Array[Byte], offset: Int, size: Int): Unit =
      java.util.Objects.checkFromIndexSize(offset, size, bytes.length)
      length = Math.addExact(length, size.toLong)

  private final class ExactOutput(private val bytes: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= bytes.length then throw new SQLDataException("mode exact output overrun")
      bytes(offset) = value.toByte
      offset += 1
    override def write(source: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, source.length)
      if length > bytes.length - offset then throw new SQLDataException("mode exact output overrun")
      System.arraycopy(source, start, bytes, offset, length)
      offset += length
    def finish(): Array[Byte] =
      if offset != bytes.length then throw new SQLDataException("mode exact output underrun")
      bytes

  private final class Reader(input: MySqlFormat3Binary, draft: DecodeDraft):
    private val root = draft.root
    private var offset = 0
    def remaining: Int = input.length - offset
    private def need(length: Long): Unit =
      if length < 0L then throw new SQLDataException("negative mode witness length")
      if length > remaining.toLong then throw new EOFException("truncated mode witness framing")
    def octet(): Int =
      need(1L)
      val result = input.byteAt(offset).toInt & 255
      offset += 1
      result
    def int(): Int =
      need(4L)
      var result = 0
      for _ <- 0 until 4 do result = (result << 8) | octet()
      result
    def long(): Long =
      need(8L)
      var result = 0L
      for _ <- 0 until 8 do result = (result << 8) | octet().toLong
      result
    def uuid(): UUID =
      root.reserveRetainedMemory(32L, "mode parsed UUID DATA")
      val value = new UUID(long(), long())
      if (value.getMostSignificantBits | value.getLeastSignificantBits) == 0L then
        throw new SQLDataException("mode UUID must be nonzero")
      value
    def fixed(length: Int): Vector[Byte] =
      need(length.toLong)
      root.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length.toLong, 24L), 64L), "mode parsed fixed bytes")
      Vector.fill(length)(octet().toByte)
    def blob(): MySqlFormat3Binary =
      val length = int()
      if length < 0 || length.toLong > root.profile.maxNativeArtifactBytes() ||
        length.toLong > root.profile.maxRetainedBytes() then throw new SQLDataException("mode blob bound")
      need(length.toLong)
      root.reserveRetainedMemory(Math.addExact(length.toLong, 128L), "mode decoded blob/retained registry")
      val bytes = new Array[Byte](length)
      var index = 0
      while index < length do
        bytes(index) = octet().toByte
        index += 1
      val value = MySqlFormat3Binary.takeOwned(bytes, root)
      draft.blobs = draft.blobs :+ value
      value
    def text(): String =
      val binary = blob()
      root.reserveTemporaryMemory(Math.addExact(binary.length.toLong, 64L), "mode UTF8 byte decode scratch")
      root.reserveRetainedMemory(Math.addExact(Math.multiplyExact(binary.length.toLong, 2L), 64L), "mode UTF8 text DATA")
      var bytes = new Array[Byte](binary.length)
      var index = 0
      while index < bytes.length do
        bytes(index) = binary.byteAt(index)
        index += 1
      var primary: Throwable = null
      try StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        bytes = null
        try root.releaseTemporaryMemory(Math.addExact(binary.length.toLong, 64L), "mode UTF8 decode scratch release")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    def dateTime(): LocalDateTime =
      val year = int()
      val month = octet()
      val day = octet()
      val hour = octet()
      val minute = octet()
      val second = octet()
      val micros = int()
      if year < 1000 || year > 9999 || micros < 0 || micros > 999999 then
        throw new SQLDataException("mode DATETIME(6) bound")
      root.reserveRetainedMemory(64L, "mode DATETIME DATA")
      LocalDateTime.of(year, month, day, hour, minute, second, Math.multiplyExact(micros, 1000))
    def expect(value: Int): Unit = if int() != value then throw new SQLDataException("mode fixed framing/count/tag differs")
    def expectText(value: String): Unit = if text() != value then throw new SQLDataException("mode fixed text/domain differs")
    def end(): Unit = if remaining != 0 then throw new SQLDataException("mode trailing data")

  private def same(bytes: MySqlFormat3Binary, expected: MySqlRecoveryCanonicalBytes,
      root: MySqlFormat3ReadBudget, label: String): Unit =
    var copy = expected.copyBytes(root, label)
    var primary: Throwable = null
    try
      if !bytes.equalsBytes(copy) then throw new SQLDataException(label + " differs from complete typed inputs")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      val charge = Math.addExact(copy.length.toLong, 64L)
      java.util.Arrays.fill(copy, 0.toByte)
      copy = null
      try root.releaseRetainedMemory(charge, label + " scratch release")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def parseRequest(binary: MySqlFormat3Binary, draft: DecodeDraft): MySqlRepositoryOperation =
    val r = new Reader(binary, draft)
    r.expectText("FP_MYSQL_REPOSITORY_REQUEST_BE_V1")
    if r.octet() != 1 then throw new SQLDataException("mode request version")
    r.expect(1)
    r.expectText("SET_OPERATING_MODE_TRACKED_IF_OWNED")
    val bindingUuid = r.uuid()
    val cutover = r.text() match
      case "CERTIFIED" => PhysicalRecoveryCutoverState.Certified
      case "LEGACY_UNCERTIFIED" => PhysicalRecoveryCutoverState.LegacyUncertified
      case _ => throw new SQLDataException("mode binding cutover DATA")
    val binding = PhysicalRecoveryBinding(bindingUuid, cutover, r.int())
    val pod = r.uuid()
    val owner = r.uuid()
    val version = r.long()
    val attempt = r.uuid()
    val mode = PodOperatingMode.fromDatabase(r.text())
    val world = r.text()
    val pose = FloatingPodPose(world, java.lang.Double.longBitsToDouble(r.long()),
      java.lang.Double.longBitsToDouble(r.long()), java.lang.Double.longBitsToDouble(r.long()),
      java.lang.Float.intBitsToFloat(r.int()), java.lang.Float.intBitsToFloat(r.int()), r.int())
    r.end()
    val result = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, owner, version, attempt, mode, pose)
    same(binary, MySqlRepositoryRequestIdentity.single(result, draft.root.profile).canonical, draft.root,
      "parsed mode request canonical")
    result

  private def parsePlan(r: Reader, draft: DecodeDraft): (MySqlOwnedRepositoryOperationPlan, MySqlRepositoryOperation) =
    r.expectText(PlanDomain)
    val tag = IoOperationTag(r.uuid(), r.long(), r.long(), r.uuid())
    val workflow = r.uuid()
    val reserve = r.uuid()
    val execute = r.uuid()
    if execute != tag.operationId then throw new SQLDataException("mode execute differs from exact tag")
    r.expectText("RUN")
    val request = r.blob()
    if r.fixed(32) != draft.root.profile.sha256().toVector then throw new SQLDataException("mode plan profile SHA")
    r.expect(1)
    val operation = parseRequest(request, draft)
    // 公开 single 仅 DATA，无 retained handle；不会用于执行或 Held 发行。
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, workflow, reserve, operation, draft.root.profile)
    (plan, operation)

  private def parseContext(binary: MySqlFormat3Binary, draft: DecodeDraft,
      plan: MySqlOwnedRepositoryOperationPlan): MySqlRepositoryOperationContextIdentity =
    val r = new Reader(binary, draft)
    r.expectText("FP_MYSQL_REPOSITORY_OPERATION_CONTEXT_BE_V1")
    if r.octet() != 1 then throw new SQLDataException("mode context version")
    val tag = IoOperationTag(r.uuid(), r.long(), r.long(), r.uuid())
    val workflow = r.uuid()
    val reserve = r.uuid()
    val generation = r.uuid()
    val cid = r.long()
    val namespace = r.text()
    val binding = r.uuid()
    val epoch = r.long()
    val lease = r.dateTime()
    val server = r.uuid()
    val serverVersion = r.text()
    val schema = r.text()
    val user = r.text()
    val provision = r.uuid()
    val schemaFormat = r.int()
    val physicalFormat = r.int()
    val ddl = r.text()
    val guard = r.text()
    val requestKind = r.text()
    val requestSha = r.fixed(32)
    val config = r.fixed(32)
    val sourceKind = r.text()
    val sourceSha = r.fixed(32)
    val profileSha = r.fixed(32)
    r.end()
    if tag != plan.tag || workflow != plan.workflowUuid || reserve != plan.reserveOperationUuid ||
      requestKind != plan.requestKind || requestSha != plan.requestIdentitySha256 then
      throw new SQLDataException("mode DATA context differs from fixed plan")
    val context = new MySqlRepositoryOperationContextIdentity(tag, workflow, reserve, generation, cid,
      namespace, binding, epoch, lease, server, serverVersion, schema, user, provision, schemaFormat,
      physicalFormat, ddl, guard, config, sourceKind, sourceSha, profileSha, requestKind, requestSha, draft.root.profile)
    draft.partialContext = context
    same(binary, context.canonical, draft.root, "parsed complete mode context")
    context

  private def readRaw(r: Reader, draft: DecodeDraft, expected: Vector[MySqlRecoveryOperationRawEnvelope]): Unit =
    val count = r.int()
    if count != expected.size then throw new SQLDataException("complete ordered RAW envelope count differs")
    var index = 0
    while index < count do
      val id = r.uuid()
      val binary = r.blob()
      val sha = r.fixed(32)
      var bytes = binary.copyBytes(draft.root, "mode RAW strict decoder argument")
      val argumentCharge = Math.addExact(bytes.length.toLong, 64L)
      var primary: Throwable = null
      try
        val decoded = MySqlRecoveryOperationRawEnvelopeCodec.decode(bytes, sha, draft.root.profile)
        draft.root.reserveRetainedMemory(64L, "mode decoded RAW registry reference")
        draft.rawEnvelopes = draft.rawEnvelopes :+ decoded
        if id != decoded.operationUuid || id != expected(index).operationUuid ||
          decoded.row != expected(index).row || !decoded.canonical.exactlyEquals(expected(index).canonical) then
          throw new SQLDataException("mode ordered RAW typed/fullbytes differ")
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        java.util.Arrays.fill(bytes, 0.toByte)
        bytes = null
        try draft.root.releaseRetainedMemory(argumentCharge, "mode RAW argument drop")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      index += 1

  private def parseScope(binary: MySqlFormat3Binary, draft: DecodeDraft,
      context: MySqlRepositoryOperationContextIdentity, request: MySqlModeOperationEffectCodec.Request,
      locator: Long): Unit =
    val r = new Reader(binary, draft)
    r.expectText("FP_MYSQL_RECOVERY_OPERATION_SCOPE_BE_V1")
    if r.octet() != 1 then throw new SQLDataException("mode scope version")
    if r.uuid() != context.tag.bootId || r.long() != context.tag.bootGeneration ||
      r.long() != context.tag.connectionGeneration || r.uuid() != context.tag.operationId ||
      r.uuid() != context.workflowUuid || r.uuid() != context.reserveOperationUuid ||
      r.uuid() != context.barrierGenerationUuid || r.long() != context.actualConnectionId then
      throw new SQLDataException("mode scope exact actor/tag identities")
    r.expectText(context.barrierNamespace)
    if r.uuid() != context.bindingUuid || r.long() != context.writerEpoch ||
      r.dateTime() != context.fixedLeaseUntil || r.uuid() != context.serverUuid then
      throw new SQLDataException("mode scope writer/server identity")
    r.expectText(context.serverVersion)
    r.expectText(context.schema)
    r.expectText(context.effectiveUser)
    if r.uuid() != context.provisioningUuid then throw new SQLDataException("mode scope provision identity")
    r.expect(context.schemaFormat)
    r.expect(context.physicalFormat)
    r.expectText(context.ddlSha256)
    r.expectText(context.approvedObjectManifestSha256)
    if r.fixed(32) != context.configurationSha256 then throw new SQLDataException("mode scope config identity")
    r.expectText(context.sourceProvenanceKind)
    if r.fixed(32) != context.sourceProvenanceSha256 || r.fixed(32) != context.limitProfileSha256 then
      throw new SQLDataException("mode scope provenance/profile identity")
    r.expect(1)
    r.expectText("POD")
    val key = new Reader(r.blob(), draft)
    key.expectText("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1")
    if key.octet() != 1 then throw new SQLDataException("mode resource key version")
    key.expectText("POD")
    if key.uuid() != request.podUuid then throw new SQLDataException("mode resource exact pod key")
    key.end()
    if r.long() != locator || r.octet() != 1 || r.uuid() != request.attemptUuid then
      throw new SQLDataException("mode resource exact revision/attempt")
    r.end()

  private def parseOperationContext(binary: MySqlFormat3Binary, draft: DecodeDraft,
      expected: MySqlOperationContext): Unit =
    val r = new Reader(binary, draft)
    r.expectText("com.wi.floatingpod.mysql-operation-context/v1")
    if r.uuid() != expected.bootUuid || r.long() != expected.writerEpoch ||
      r.uuid() != expected.bindingUuid || r.uuid() != expected.operationUuid then
      throw new SQLDataException("mode phase operation context identity")
    r.expect(expected.expectedEffects.size)
    expected.expectedEffects.foreach { effect =>
      r.expectText(effect.targetIdentity)
      if r.long() != effect.expectedVersion || !r.blob().equalsVector(effect.desiredEffectFingerprintSha256) then
        throw new SQLDataException("mode ordered operation effect context")
    }
    r.end()

  private def readEffect(r: Reader, draft: DecodeDraft, effect: MySqlModeOperationEffect,
      phase: MySqlModePhaseCanonical, plan: MySqlOwnedRepositoryOperationPlan,
      context: MySqlRepositoryOperationContextIdentity, operation: MySqlRepositoryOperation, locator: Long): Unit =
    r.expectText(EffectDomain)
    val reserve = effect.phase == MySqlOperationEffectPhase.Reserve
    r.expect(if reserve then 1 else 2)
    if r.uuid() != (if reserve then plan.reserveOperationUuid else plan.executeOperationUuid) then
      throw new SQLDataException("mode phase operation differs")
    r.expect(if reserve then 0 else 1)
    r.expect(if reserve then 1 else 2)
    val scopeBytes = r.blob()
    parseScope(scopeBytes, draft, context, MySqlModeOperationEffectCodec.modeRequest(operation), locator)
    same(scopeBytes, phase.scope, draft.root, "complete phase scope")
    val operationContextBytes = r.blob()
    parseOperationContext(operationContextBytes, draft, phase.operationContext)
    same(operationContextBytes, phase.operationContextBytes, draft.root, "complete phase operation context")
    same(r.blob(), effect.preimage, draft.root, "complete phase preimage")
    same(r.blob(), effect.target, draft.root, "complete phase target")
    same(r.blob(), effect.fingerprint, draft.root, "complete phase fingerprint")
    readRaw(r, draft, effect.preimageRawOperationEnvelopes)
    readRaw(r, draft, effect.rawOperationEnvelopes)
    r.expect(1)

  def capture(bytes: Array[Byte], root: MySqlFormat3ReadBudget): Encoded =
    require(root != null && bytes != null, "external mode DATA input/root required")
    requireRoot(root, root.profile)
    if bytes.length <= 0 || bytes.length.toLong > root.profile.maxNativeArtifactBytes() then
      throw new SQLDataException("mode input exceeds native/profile bound")
    root.reserveRetainedMemory(128L, "captured mode DATA holder")
    new Encoded(null, MySqlFormat3Binary.copyOf(bytes, root, "captured mode input"), root, null, Vector.empty)

  def decode(payload: Encoded, draft: DecodeDraft): Decoded =
    require(draft != null && !draft.attempted && payload != null && (payload.root eq draft.root),
      "once retained mode DATA payload/exact original root required")
    val input = payload.bytes
    requireRoot(draft.root, draft.root.profile)
    if input.length <= 0 || input.length.toLong > draft.root.profile.maxNativeArtifactBytes() then
      throw new SQLDataException("mode decode input exceeds native bound")
    draft.attempted = true
    draft.input = input
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(draft.root) {
      draft.root.reserveRetainedMemory(8192L, "mode decoder fixed framing/context/collection structures")
      val r = new Reader(input, draft)
      r.expectText(Domain)
      val profile = r.blob()
      requireProfileBytes(profile, draft.root)
      val source = SourceDescriptor(r.int(), r.long(), r.long(), r.fixed(16), r.blob(), r.fixed(32), r.fixed(32), r.blob())
      draft.partialSource = source
      validateSource(source, draft.root)
      val (plan, operation) = parsePlan(r, draft)
      draft.partialPlan = plan
      val context = parseContext(r.blob(), draft, plan)
      val plannedAt = r.dateTime()
      val locator = r.long()
      if locator < 0L then throw new SQLDataException("mode locator revision is negative")
      r.expect(3)
      draft.root.reserveRetainedMemory(88L, "three successful decoded child owners BEFORE allocation")
      draft.decodedChildren = new Array[MySqlFormat3ReadBudget](3)
      for index <- 0 until 3 do
        r.expect(index)
        val image = r.blob()
        draft.root.reserveRetainedMemory(128L, "mode decoded image payload holder")
        val payload = new MySqlFormat3CanonicalPayload(image, draft.root, draft.root)
        draft.imagePayloads = draft.imagePayloads :+ payload
        val decoded = MySqlFormat3SnapshotCodec.decode(payload, draft.root.profile, draft.root)
        draft.decodedChildren(index) = decoded.allocationBudget
        require(decoded.allocationBudget.sharesAllocationLedger(draft.root), "decoded mode image differs from original ledger")
        draft.root.reserveRetainedMemory(512L, "mode cold original-root DATA image wrapper")
        val originalRootImage = new MySqlFormat3FullSnapshot(decoded.schema, decoded.connection, decoded.observationScope,
          decoded.maxAllowedPacketBytes, decoded.limitProfile, decoded.relations, draft.root)
        draft.images = draft.images :+ originalRootImage
      val images = draft.images
      val projectionVersion = MySqlModeOperationEffectCodec.workflowProjectionVersion(plan, images(1), images(2))
      val effects = MySqlModeOperationEffectCodec.buildWorkflow(plan, operation, context, locator,
        images(0), images(1), images(2))
      draft.partialEffects = effects
      r.expect(2)
      for index <- 0 until 2 do
        val phase = MySqlModeOperationEffectCodec.canonicalPhase(plan, operation, context, locator,
          effects(index).phase, images(index), images(index + 1), projectionVersion)
        draft.root.reserveRetainedMemory(64L, "mode partial phase registry reference")
        draft.partialPhases = draft.partialPhases :+ phase
        readEffect(r, draft, effects(index), phase, plan, context, operation, locator)
        val semantic = r.blob()
        requireStoredSemantic(semantic, images(index + 1),
          if index == 0 then plan.reserveOperationUuid else plan.executeOperationUuid, plannedAt)
        parseSemantic(semantic, draft, plan, operation, context, plannedAt, phase, effects(index))
      r.expect(1)
      r.end()
      val result = new Decoded(source, plan, context, plannedAt, locator, images, effects)
      draft.result = result
      result
    }
    catch
      case cause: Throwable =>
        draft.failure = cause
        throw cause

  /** 完整语义载荷必须就是真实目标 O 的 commit witness，不能另附合法但无关的 blob。 */
  private def requireStoredSemantic(binary: MySqlFormat3Binary, image: MySqlFormat3FullSnapshot,
      operationUuid: UUID, plannedAt: LocalDateTime): Unit =
    val table = "fp_recovery_operation"
    val shape = MySqlFormat3RelationShapes.canonicalOrder.find(_.table == table)
      .getOrElse(throw new SQLDataException("fixed operation shape missing"))
    val row = image.relation(table).rows.find(value =>
      MySqlModeOperationEffectCodec.rowOperationUuid(value) == operationUuid)
      .getOrElse(throw new SQLDataException("semantic current operation row missing"))
    def cell(name: String): MySqlFormat3Cell =
      val index = shape.columns.indexWhere(_.name == name)
      if index < 0 then throw new SQLDataException("semantic fixed operation column missing")
      row(index)
    if cell("commit_witness_bytes") != MySqlFormat3Cell.Binary(binary) ||
      cell("planned_at") != MySqlFormat3Cell.DateTime(plannedAt) ||
      cell("result_at") != MySqlFormat3Cell.DateTime(plannedAt) then
      throw new SQLDataException("semantic witness/actor timestamp differs from full target O")

  /** 逐字段读现 semantic writer，而非接受 opaque blob/hash/prefix。 */
  private def parseSemantic(binary: MySqlFormat3Binary, draft: DecodeDraft,
      plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, plannedAt: LocalDateTime,
      canonicalPhase: MySqlModePhaseCanonical, effect: MySqlModeOperationEffect): Unit =
    val r = new Reader(binary, draft)
    val request = MySqlModeOperationEffectCodec.modeRequest(operation)
    r.expectText("com.wi.floatingpod.mysql-mode-semantic-witness/v1")
    if r.octet() != 1 then throw new SQLDataException("mode semantic version")
    r.expectText(if effect.phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
    val expectedIds = Vector(request.binding.bindingUuid, request.podUuid, request.owner, request.attemptUuid,
      plan.workflowUuid, plan.reserveOperationUuid, plan.executeOperationUuid,
      if effect.phase == MySqlOperationEffectPhase.Reserve then plan.reserveOperationUuid else plan.executeOperationUuid)
    expectedIds.foreach { expected => if r.uuid() != expected then throw new SQLDataException("mode semantic UUID differs") }
    if r.long() != request.expectedStateVersion then throw new SQLDataException("mode semantic expected version")
    r.expectText(request.targetMode.databaseValue)
    r.expectText(request.actualPose.worldName)
    val pose = request.actualPose
    if r.long() != java.lang.Double.doubleToRawLongBits(pose.posX) ||
      r.long() != java.lang.Double.doubleToRawLongBits(pose.posY) ||
      r.long() != java.lang.Double.doubleToRawLongBits(pose.posZ) ||
      r.int() != java.lang.Float.floatToRawIntBits(pose.yaw) ||
      r.int() != java.lang.Float.floatToRawIntBits(pose.pitch) || r.int() != pose.dimensionId then
      throw new SQLDataException("mode semantic actual pose bits differ")
    same(r.blob(), plan.requestIdentity.canonical, draft.root, "semantic request")
    requireProfileBytes(r.blob(), draft.root)
    same(r.blob(), context.canonical, draft.root, "semantic writer/context")
    if r.uuid() != context.barrierGenerationUuid || r.long() != context.actualConnectionId ||
      r.long() != context.writerEpoch || r.uuid() != context.bindingUuid ||
      r.dateTime() != context.fixedLeaseUntil || r.dateTime() != plannedAt then
      throw new SQLDataException("semantic fixed actor identities/times")
    r.expectText("FIXED_TERMINAL_RESULT_MARKER_V1")
    if r.dateTime() != plannedAt then throw new SQLDataException("semantic terminal timestamp")
    same(r.blob(), canonicalPhase.scope, draft.root, "semantic scope")
    same(r.blob(), canonicalPhase.operationContextBytes, draft.root, "semantic phase context")
    same(r.blob(), canonicalPhase.fingerprint, draft.root, "semantic phase fingerprint")
    same(r.blob(), canonicalPhase.preimage, draft.root, "semantic phase preimage")
    same(r.blob(), canonicalPhase.target, draft.root, "semantic phase target")
    val hasReserve = r.octet()
    if effect.phase == MySqlOperationEffectPhase.Reserve then
      if hasReserve != 0 then throw new SQLDataException("reserve semantic must not contain prior reserve")
    else
      if hasReserve != 1 then throw new SQLDataException("execute semantic requires original reserve RAW")
      val original = effect.preimageRawOperationEnvelopes.find(_.operationUuid == plan.reserveOperationUuid)
        .getOrElse(throw new SQLDataException("execute missing fixed reserve RAW dependency"))
      same(r.blob(), original.canonical, draft.root, "execute semantic original reserve RAW")
      if r.fixed(32) != original.sha256 then throw new SQLDataException("execute reserve RAW SHA")
    r.end()
