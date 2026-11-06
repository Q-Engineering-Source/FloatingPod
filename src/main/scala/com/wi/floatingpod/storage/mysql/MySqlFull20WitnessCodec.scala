package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical}
import java.io.{EOFException, DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.Arrays

/** Cold完整 DATA，文件字段不授予当前 native/JDBC/admission/Ready。 */
private[mysql] final class MySqlFull20WitnessData private[mysql] (
    val originalRecord: B1ResumeRecord,
    val before: MySqlFormat3CanonicalBundle,
    val target: MySqlFormat3CanonicalBundle,
    val envelope: MySqlFormat3Binary,
    val originalRoot: MySqlFormat3ReadBudget,
    private[mysql] val contextCanonical: MySqlFormat3Binary
):
  def exactlyMatches(original: MySqlFull20OriginalWitness): Boolean =
    original != null && (originalRoot eq original.originalRoot) &&
      (envelope == original.payload) && before.snapshot.exactlyEquals(original.originalPlan.before.format3Snapshot.get) &&
      target.snapshot.exactlyEquals(original.originalPlan.target.format3Snapshot.get)

/** 固定 envelope reader；调用同一full20 strict codec，不复制NBT/森林协议。 */
private[mysql] object MySqlFull20WitnessCodec:
  private val Domain = "FP_B1_FULL20_ORIGINAL_WITNESS_BE_V1".getBytes(StandardCharsets.US_ASCII)
  /** 固定 DATA framing，private producer 与 cold regression 共用；不发行 plan/native 权限。 */
  def encodeParts(profileBytes: Array[Byte], context: Array[Byte],
      before: MySqlFormat3CanonicalPayload, target: MySqlFormat3CanonicalPayload,
      root: MySqlFormat3ReadBudget): MySqlFormat3Binary =
    require(root != null && !root.isClosed && profileBytes != null && context != null &&
      before != null && target != null, "complete live witness DATA parts required")
    require(!before.storageBudget.isClosed && !target.storageBudget.isClosed &&
      before.originalBudget.sharesAllocationLedger(root) && target.originalBudget.sharesAllocationLedger(root) &&
      (before.originalBudget.profile eq root.profile) && (target.originalBudget.profile eq root.profile),
      "complete parts must remain on original ledger/profile")
    require(profileBytes.length == 97 && context.length > 0 && context.length <= 65536,
      "fixed profile and complete V4 context bounds")
    val length = Math.addExact(Math.addExact(Math.addExact(Math.addExact(
      24L + Domain.length, profileBytes.length.toLong), context.length.toLong),
      before.byteLength.toLong), target.byteLength.toLong)
    require(length <= root.profile.maxNativeArtifactBytes() && length <= root.profile.maxRetainedBytes(),
      "complete witness envelope exceeds original profile")
    val size = MySqlFormat3JvmArrayLimit.requireLength(length, "complete witness envelope")
    root.reserveRetainedMemory(Math.addExact(length, 64L), "complete witness exact final buffer")
    root.reserveTemporaryMemory(256L, "complete witness exact writer structures")
    var primary: Throwable = null
    try
      val sink = new Exact(new Array[Byte](size))
      val out = new DataOutputStream(sink)
      out.writeInt(Domain.length); out.write(Domain)
      out.writeInt(profileBytes.length); out.write(profileBytes)
      out.writeInt(context.length); out.write(context)
      out.writeInt(before.byteLength); before.bytes.writeTo(out)
      out.writeInt(target.byteLength); target.bytes.writeTo(out)
      out.writeInt(1); out.flush()
      MySqlFormat3Binary.takeOwned(sink.finish())
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try root.releaseTemporaryMemory(256L, "exact DATA writer dropped")
      catch
        case cleanup: Throwable =>
          if primary != null && (cleanup ne primary) then primary.addSuppressed(cleanup)
          else if primary == null then throw cleanup

  private final class Exact(private var bytes: Array[Byte]) extends OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if bytes == null || offset >= bytes.length then throw new IllegalStateException("witness exact writer overrun")
      bytes(offset) = value.toByte
      offset += 1
    override def write(source: Array[Byte], start: Int, count: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, count, source.length)
      if bytes == null || count > bytes.length - offset then throw new IllegalStateException("witness exact writer overrun")
      System.arraycopy(source, start, bytes, offset, count)
      offset += count
    def finish(): Array[Byte] =
      if bytes == null || offset != bytes.length then throw new IllegalStateException("witness exact writer underrun")
      val result = bytes
      bytes = null
      result

  /** 先由实际 transfer登记；partial typed/context/bytes在任何后续失败时仍可观察。 */
  final class DecodeDraft private[MySqlFull20WitnessCodec] (
      val input: Array[Byte], val profile: SourceLimitProfile, val root: MySqlFormat3ReadBudget):
    private[mysql] var envelope: MySqlFormat3Binary = null
    private[mysql] var context: MySqlFormat3Binary = null
    private[mysql] var record: B1ResumeRecord = null
    private[mysql] var beforeBytes: MySqlFormat3Binary = null
    private[mysql] var targetBytes: MySqlFormat3Binary = null
    private[mysql] var before: MySqlFormat3CanonicalBundle = null
    private[mysql] var beforeChild: MySqlFormat3ReadBudget = null
    private[mysql] var targetChild: MySqlFormat3ReadBudget = null
    private[mysql] var target: MySqlFormat3CanonicalBundle = null
    private[mysql] var result: MySqlFull20WitnessData = null
    private[mysql] var failure: Throwable = null
    private[MySqlFull20WitnessCodec] var attempted = false
    private[MySqlFull20WitnessCodec] var targetAttempted = false

  def draft(input: Array[Byte], profile: SourceLimitProfile, root: MySqlFormat3ReadBudget): DecodeDraft =
    require(input != null && profile != null && root != null && !root.isClosed && (root.profile eq profile),
      "exact live DATA decode root/profile required")
    root.reserveRetainedMemory(256L, "retained cold decode draft/reader")
    new DecodeDraft(input, profile, root)

  def decode(draft: DecodeDraft): MySqlFull20WitnessData =
    decodeBefore(draft)
    completeTarget(draft)

  def decodeBefore(draft: DecodeDraft): Unit =
    require(draft != null && !draft.attempted, "cold decode draft is one-shot")
    draft.attempted = true
    try decodeRetainedBefore(draft)
    catch
      case primary: Throwable =>
        draft.failure = primary
        throw primary

  def completeTarget(draft: DecodeDraft): MySqlFull20WitnessData =
    require(draft != null && draft.before != null && draft.failure == null && !draft.targetAttempted,
      "exact successful original before decode is required once")
    draft.targetAttempted = true
    try
      draft.target = decodePart(draft.targetBytes, draft.profile, draft.root)
      draft.targetChild = draft.target.snapshot.allocationBudget
      require(draft.before.snapshot.schema == draft.target.snapshot.schema, "original before/target schema differs")
      draft.result = new MySqlFull20WitnessData(draft.record, draft.before, draft.target,
        draft.envelope, draft.root, draft.context)
      draft.result
    catch
      case primary: Throwable =>
        draft.failure = primary
        throw primary

  private def decodeRetainedBefore(draft: DecodeDraft): Unit =
    val input = draft.input
    val profile = draft.profile
    val root = draft.root
    require(input != null && profile != null && root != null && !root.isClosed && (root.profile eq profile),
      "witness DATA requires its exact open original profile/root")
    require(input.length > 0 && input.length.toLong <= profile.maxNativeArtifactBytes(), "witness artifact profile bound")
    draft.envelope = MySqlFormat3Binary.copyOf(input, root, "cold witness immutable defensive bytes")
    val envelope = draft.envelope
    val reader = new Reader(envelope, root)
    val domain = reader.blob(128, "witness domain")
    require(domain.length == Domain.length && Domain.indices.forall(i => domain.byteAt(i) == Domain(i)), "unknown witness envelope domain")
    val profileBytes = reader.blob(97, "witness profile")
    require(profileBytes.length == 97, "complete canonical profile required")
    root.reserveTemporaryMemory(192L, "expected witness profile canonical bytes/hash")
    var expected: Array[Byte] = null
    var primary: Throwable = null
    try
      expected = profile.canonicalBytes()
      require(profileBytes.equalsBytes(expected), "cold witness profile identity differs")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if expected != null then Arrays.fill(expected, 0.toByte)
      expected = null
      try root.releaseTemporaryMemory(192L, "expected witness profile bytes dropped")
      catch
        case cleanup: Throwable =>
          if primary != null && (cleanup ne primary) then primary.addSuppressed(cleanup)
          else if primary == null then throw cleanup
    draft.context = reader.blob(65536, "complete original V4 context")
    val context = draft.context
    // 原context decoder防御复制/UTF8/固定DTO；保守依据每实际context bytes计费，非private ledger。
    root.reserveRetainedMemory(Math.addExact(Math.multiplyExact(context.length.toLong, 8L), 16384L),
      "complete original V4 DATA decode arrays/strings/fixed holders")
    var contextArray = context.copyBytes(root, "V4 strict decoder input")
    var decodeFailure: Throwable = null
    try
      draft.record = B1ResumeRecordCanonical.decodeData(contextArray)
    catch
      case failure: Throwable =>
        decodeFailure = failure
        throw failure
    finally
      Arrays.fill(contextArray, 0.toByte)
      contextArray = null
      try root.releaseRetainedMemory(context.length.toLong + 64L, "V4 decoder input dropped after complete DTO copy")
      catch
        case cleanup: Throwable =>
          if decodeFailure != null && (cleanup ne decodeFailure) then decodeFailure.addSuppressed(cleanup)
          else if decodeFailure == null then throw cleanup
    draft.beforeBytes = reader.blob(profile.maxRetainedBytes(), "complete before canonical")
    draft.targetBytes = reader.blob(profile.maxRetainedBytes(), "complete target canonical")
    require(reader.int() == 1, "unknown witness framing version")
    if !reader.atEnd then throw new IllegalArgumentException("witness trailing bytes")
    draft.before = decodePart(draft.beforeBytes, profile, root)
    draft.beforeChild = draft.before.snapshot.allocationBudget

  private def decodePart(bytes: MySqlFormat3Binary, profile: SourceLimitProfile,
      root: MySqlFormat3ReadBudget): MySqlFormat3CanonicalBundle =
    // Reader已在原根reserve且拥有不可变binary；不能另开root或把DATA声明成SQL source。
    root.reserveRetainedMemory(128L, "cold full20 canonical holder")
    val payload = new MySqlFormat3CanonicalPayload(bytes, root, root)
    MySqlFormat3CanonicalBundle.decode(payload, profile, root)

  private final class Reader(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget):
    private var offset = 0
    def atEnd: Boolean = offset == input.length
    private def available(size: Int): Unit =
      if size < 0 then throw new IllegalArgumentException("negative witness framing")
      if size > input.length - offset then throw new EOFException("truncated witness framing")
    def int(): Int =
      available(4)
      var result = 0
      var i = 0
      while i < 4 do
        result = (result << 8) | (input.byteAt(offset + i) & 255)
        i += 1
      offset += 4
      result
    def blob(limit: Long, label: String): MySqlFormat3Binary =
      val size = int()
      if size <= 0 || size.toLong > limit then throw new IllegalArgumentException(label + " outside explicit bound")
      available(size)
      root.reserveRetainedMemory(Math.addExact(size.toLong, 64L), label)
      val bytes = new Array[Byte](size)
      var i = 0
      while i < size do
        bytes(i) = input.byteAt(offset + i)
        i += 1
      offset += size
      MySqlFormat3Binary.takeOwned(bytes, root)
