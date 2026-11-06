package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.source.SourceForestAllocationBudget
import com.wi.floatingpod.server.source.SourceForestAllocationBudget.Scope
import java.io.{DataOutputStream, OutputStream}
import java.sql.SQLDataException
import java.sql.SQLTransactionRollbackException
import java.util.UUID
import java.util.function.Consumer

/** reservation 的完整 canonical key；摘要只作索引，不替代完整 key 比较。 */
final class MySqlRecoveryResourceKey private (
    val resourceKind: String,
    private val canonical: MySqlRecoveryCanonicalBytes,
    val podUuid: Option[UUID],
    val sourceUnitUuid: Option[UUID],
    private val profile: SourceLimitProfile
):
  require(resourceKind != null && canonical != null && podUuid != null && sourceUnitUuid != null && profile != null,
    "complete recovery resource key is required")
  require(podUuid.isDefined == (resourceKind == "POD") &&
    sourceUnitUuid.isDefined == (resourceKind == "SOURCE_UNIT"), "resource kind and UUID reference columns disagree")

  val keyHashSha256: Vector[Byte] = canonical.sha256
  val canonicalLength: Int = canonical.length
  val keyHashHex: String = canonical.hexSha256

  def writeCanonicalTo(output: DataOutputStream): Unit = canonical.writeTo(output)
  private[mysql] def copyCanonicalBytes(budget: MySqlFormat3ReadBudget, context: String): Array[Byte] =
    canonical.copyBytes(budget, context)
  private[mysql] def exactProfile: SourceLimitProfile = profile
  private[mysql] def compareCanonical(other: MySqlRecoveryResourceKey): Int =
    if other == null then throw new IllegalArgumentException("recovery resource key comparison value is required")
    canonical.compareContent(other.canonical)

  def exactlyEquals(other: MySqlRecoveryResourceKey): Boolean =
    other != null && resourceKind == other.resourceKind && canonical.exactlyEquals(other.canonical)

  override def equals(other: Any): Boolean = other match
    case value: MySqlRecoveryResourceKey => exactlyEquals(value)
    case _ => false
  override def hashCode(): Int = (resourceKind, keyHashSha256).hashCode
  override def toString: String = s"MySqlRecoveryResourceKey[$resourceKind,$canonicalLength bytes,sha256=$keyHashHex]"

object MySqlRecoveryResourceKey:
  private val Encoding = "FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1"

  /** Strictly decoded CARRIER DATA only; this value carries no key or storage authority. */
  final class DecodedCarrierKeyData private[MySqlRecoveryResourceKey] (
      val saveIdentity: String,
      val loaderIdentity: String,
      val regionIdentity: String,
      val chunkX: Int,
      val chunkZ: Int
  ):
    /** Conservative retained-memory charge for a caller that keeps this value after the callback. */
    def retainedChargeBytes: Long =
      val utf16Units = Math.addExact(Math.addExact(saveIdentity.length.toLong, loaderIdentity.length.toLong), regionIdentity.length.toLong)
      Math.addExact(256L, Math.multiplyExact(2L, utf16Units))

  def pod(podUuid: UUID, profile: SourceLimitProfile): MySqlRecoveryResourceKey =
    requireUuid(podUuid, "POD")
    build("POD", Some(podUuid), None, profile) { output => MySqlRecoveryCanonicalBytes.writeUuid(output, podUuid) }

  def sourceUnit(unitUuid: UUID, profile: SourceLimitProfile): MySqlRecoveryResourceKey =
    requireUuid(unitUuid, "SOURCE_UNIT")
    build("SOURCE_UNIT", None, Some(unitUuid), profile) { output => MySqlRecoveryCanonicalBytes.writeUuid(output, unitUuid) }

  def carrier(
      saveIdentity: String,
      loaderIdentity: String,
      regionIdentity: String,
      slotX: Int,
      slotZ: Int,
      profile: SourceLimitProfile
  ): MySqlRecoveryResourceKey =
    if saveIdentity == null || loaderIdentity == null || regionIdentity == null then
      throw new IllegalArgumentException("complete CARRIER identity strings are required")
    build("CARRIER", None, None, profile) { output =>
      writeCarrierFields(output, saveIdentity, loaderIdentity, regionIdentity, slotX, slotZ)
    }

  /**
   * Synchronously decodes one complete canonical CARRIER key as pure DATA. Input bytes are borrowed
   * without copying, must already be accounted to this owner, and must remain stable until this method
   * returns. A consumer that retains the value must call allocation.reserveTemporary(
   * data.retainedChargeBytes, "retained CARRIER DATA") before keeping it, then release that same
   * reservation after dropping the retained value.
   */
  def consumeCarrierKeyData(exactBytes: Array[Byte], exactProfile: SourceLimitProfile,
      allocation: SourceForestAllocationBudget, consumer: Consumer[DecodedCarrierKeyData]): Unit =
    if exactBytes == null || exactProfile == null || allocation == null || consumer == null then
      throw new IllegalArgumentException("exact CARRIER bytes, profile, original allocation and consumer are required")
    if (allocation.exactProfile() ne exactProfile) then
      throw new SQLDataException("CARRIER key allocation must use the exact source profile instance")
    val maximum = Math.min(exactProfile.maxSqlPacketBytes(), exactProfile.maxRetainedBytes())
    if exactBytes.length == 0 || exactBytes.length.toLong > maximum then
      throw new SQLDataException("CARRIER key exceeds the exact SQL-packet or retained-byte profile")

    val work = Scope.open(allocation, exactProfile)
    var primary: Throwable = null
    var decoded: DecodedCarrierKeyData = null
    try
      decoded = decodeCarrierKeyData(exactBytes, exactProfile, work)
      consumer.accept(decoded)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      decoded = null
      try work.close()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  /** SQL reservation resource locks use kind, unsigned SHA-256, then the full key bytes. */
  def orderForLocks(values: Iterable[MySqlRecoveryResourceKey]): Vector[MySqlRecoveryResourceKey] =
    if values == null then throw new IllegalArgumentException("recovery resource key collection is required")
    val keys = values.iterator.toVector
    if keys.exists(_ == null) then throw new IllegalArgumentException("recovery resource keys must not contain null")
    val ordered = keys.sortWith(compareForLock(_, _) < 0)
    ordered.sliding(2).foreach {
      case Vector(left, right) if left.resourceKind == right.resourceKind &&
          compareUnsigned(left.keyHashSha256, right.keyHashSha256) == 0 =>
        if left.exactlyEquals(right) then
          throw new SQLDataException("duplicate full recovery resource key cannot be reserved twice")
        else
          throw new SQLTransactionRollbackException("different full recovery resource keys share one index hash")
      case _ => ()
    }
    ordered

  private def build(
      kind: String,
      podUuid: Option[UUID],
      unitUuid: Option[UUID],
      profile: SourceLimitProfile
  )(writeIdentity: DataOutputStream => Unit): MySqlRecoveryResourceKey =
    if profile == null then throw new IllegalArgumentException("explicit source limit profile is required")
    val canonical = MySqlRecoveryCanonicalBytes.encode(Encoding, profile) { output =>
      writeCanonicalHeader(output, kind)
      writeIdentity(output)
    }
    new MySqlRecoveryResourceKey(kind, canonical, podUuid, unitUuid, profile)

  private def writeCanonicalHeader(output: DataOutputStream, kind: String): Unit =
    MySqlRecoveryCanonicalBytes.writeToken(output, Encoding)
    output.writeByte(1)
    MySqlRecoveryCanonicalBytes.writeToken(output, kind)

  private def writeCarrierFields(output: DataOutputStream, saveIdentity: String, loaderIdentity: String,
      regionIdentity: String, chunkX: Int, chunkZ: Int): Unit =
    MySqlRecoveryCanonicalBytes.writeText(output, saveIdentity)
    MySqlRecoveryCanonicalBytes.writeText(output, loaderIdentity)
    MySqlRecoveryCanonicalBytes.writeText(output, regionIdentity)
    output.writeInt(chunkX)
    output.writeInt(chunkZ)

  private def writeCarrierCanonical(output: DataOutputStream, data: DecodedCarrierKeyData): Unit =
    writeCanonicalHeader(output, "CARRIER")
    writeCarrierFields(output, data.saveIdentity, data.loaderIdentity, data.regionIdentity, data.chunkX, data.chunkZ)

  private def decodeCarrierKeyData(bytes: Array[Byte], profile: SourceLimitProfile,
      work: Scope): DecodedCarrierKeyData =
    work.charge(512L, "CARRIER canonical cursor, UTF-8 validation and cleanup workspace BEFORE allocation")
    var offset = readExpectedToken(bytes, 0, Encoding)
    if readUnsignedByte(bytes, offset) != 1 then throw new SQLDataException("unsupported recovery resource key version")
    offset += 1
    offset = readExpectedToken(bytes, offset, "CARRIER")

    val saveLength = readTextLength(bytes, offset, profile)
    val saveStart = offset + 4
    val saveUnits = countUtf16Units(bytes, saveStart, saveLength)
    offset = saveStart + saveLength

    val loaderLength = readTextLength(bytes, offset, profile)
    val loaderStart = offset + 4
    val loaderUnits = countUtf16Units(bytes, loaderStart, loaderLength)
    offset = loaderStart + loaderLength

    val regionLength = readTextLength(bytes, offset, profile)
    val regionStart = offset + 4
    val regionUnits = countUtf16Units(bytes, regionStart, regionLength)
    offset = regionStart + regionLength

    val chunkX = readInt32(bytes, offset)
    val chunkZ = readInt32(bytes, offset + 4)
    offset += 8
    if offset != bytes.length then throw new SQLDataException("CARRIER key has trailing canonical data")

    val totalUnits = Math.addExact(Math.addExact(saveUnits.toLong, loaderUnits.toLong), regionUnits.toLong)
    // Each output String can coexist with its UTF-16 decode array during construction.
    val stringAndDataCharge = Math.addExact(384L, Math.multiplyExact(totalUnits, 4L))
    val simultaneousPeak = Math.addExact(bytes.length.toLong,
      Math.addExact(64L, Math.addExact(64L, Math.addExact(512L, Math.addExact(stringAndDataCharge, 256L)))))
    if simultaneousPeak > profile.maxRetainedBytes() then
      throw new SQLDataException("CARRIER decode and roundtrip peak exceeds the retained-byte profile")
    work.charge(stringAndDataCharge,
      "CARRIER UTF-16 arrays, String backing storage and metadata BEFORE allocation")
    val saveIdentity = decodeUtf8(bytes, saveStart, saveLength, saveUnits)
    val loaderIdentity = decodeUtf8(bytes, loaderStart, loaderLength, loaderUnits)
    val regionIdentity = decodeUtf8(bytes, regionStart, regionLength, regionUnits)
    work.charge(128L, "CARRIER decoded DATA callback value BEFORE allocation")
    val decoded = new DecodedCarrierKeyData(saveIdentity, loaderIdentity, regionIdentity, chunkX, chunkZ)
    work.charge(128L, "CARRIER byte-comparison writer and stream BEFORE allocation")
    if !matchesCarrierCanonical(bytes, decoded) then
      throw new SQLDataException("CARRIER key failed exact factory-writer roundtrip")
    decoded

  private def readExpectedToken(bytes: Array[Byte], offset: Int, expected: String): Int =
    val length = readInt32(bytes, offset)
    if length != expected.length || offset > bytes.length - 4 - length then
      throw new SQLDataException("recovery resource key token length differs from canonical encoding")
    val tokenStart = offset + 4
    var index = 0
    while index < length do
      if (bytes(tokenStart + index) & 0xff) != expected.charAt(index).toInt then
        throw new SQLDataException("recovery resource key token differs from canonical encoding")
      index += 1
    tokenStart + length

  private def readTextLength(bytes: Array[Byte], offset: Int, profile: SourceLimitProfile): Int =
    val length = readInt32(bytes, offset)
    val maximum = Math.min(profile.maxSqlPacketBytes(), profile.maxRetainedBytes())
    val remaining = bytes.length - offset - 4
    if length < 0 || length > remaining || length.toLong > maximum then
      throw new SQLDataException("CARRIER identity text exceeds canonical framing or profile bounds")
    length

  private def readUnsignedByte(bytes: Array[Byte], offset: Int): Int =
    if offset < 0 || offset >= bytes.length then throw new SQLDataException("truncated recovery resource key")
    bytes(offset) & 0xff

  private def readInt32(bytes: Array[Byte], offset: Int): Int =
    if offset < 0 || offset > bytes.length - 4 then throw new SQLDataException("truncated recovery resource key integer")
    ((bytes(offset) & 0xff) << 24) | ((bytes(offset + 1) & 0xff) << 16) |
      ((bytes(offset + 2) & 0xff) << 8) | (bytes(offset + 3) & 0xff)

  private def countUtf16Units(bytes: Array[Byte], start: Int, length: Int): Int =
    val end = start + length
    var position = start
    var units = 0
    while position < end do
      val first = bytes(position) & 0xff
      var codePoint = 0
      var width = 0
      if first <= 0x7f then
        codePoint = first
        width = 1
      else if first >= 0xc2 && first <= 0xdf then
        if end - position < 2 then throw new SQLDataException("truncated strict UTF-8 CARRIER identity")
        val second = bytes(position + 1) & 0xff
        requireContinuation(second)
        codePoint = ((first & 0x1f) << 6) | (second & 0x3f)
        width = 2
      else if first >= 0xe0 && first <= 0xef then
        if end - position < 3 then throw new SQLDataException("truncated strict UTF-8 CARRIER identity")
        val second = bytes(position + 1) & 0xff
        val third = bytes(position + 2) & 0xff
        requireContinuation(second); requireContinuation(third)
        if (first == 0xe0 && second < 0xa0) || (first == 0xed && second > 0x9f) then
          throw new SQLDataException("non-canonical UTF-8 CARRIER identity")
        codePoint = ((first & 0x0f) << 12) | ((second & 0x3f) << 6) | (third & 0x3f)
        width = 3
      else if first >= 0xf0 && first <= 0xf4 then
        if end - position < 4 then throw new SQLDataException("truncated strict UTF-8 CARRIER identity")
        val second = bytes(position + 1) & 0xff
        val third = bytes(position + 2) & 0xff
        val fourth = bytes(position + 3) & 0xff
        requireContinuation(second); requireContinuation(third); requireContinuation(fourth)
        if (first == 0xf0 && second < 0x90) || (first == 0xf4 && second > 0x8f) then
          throw new SQLDataException("non-canonical UTF-8 CARRIER identity")
        codePoint = ((first & 0x07) << 18) | ((second & 0x3f) << 12) |
          ((third & 0x3f) << 6) | (fourth & 0x3f)
        width = 4
      else throw new SQLDataException("invalid strict UTF-8 CARRIER identity")
      units = Math.addExact(units, if codePoint > 0xffff then 2 else 1)
      position += width
    units

  private def decodeUtf8(bytes: Array[Byte], start: Int, length: Int, expectedUnits: Int): String =
    val characters = new Array[Char](expectedUnits)
    val end = start + length
    var position = start
    var target = 0
    while position < end do
      val first = bytes(position) & 0xff
      var codePoint = 0
      var width = 0
      if first <= 0x7f then
        codePoint = first
        width = 1
      else if first <= 0xdf then
        val second = bytes(position + 1) & 0xff
        codePoint = ((first & 0x1f) << 6) | (second & 0x3f)
        width = 2
      else if first <= 0xef then
        val second = bytes(position + 1) & 0xff
        val third = bytes(position + 2) & 0xff
        codePoint = ((first & 0x0f) << 12) | ((second & 0x3f) << 6) | (third & 0x3f)
        width = 3
      else
        val second = bytes(position + 1) & 0xff
        val third = bytes(position + 2) & 0xff
        val fourth = bytes(position + 3) & 0xff
        codePoint = ((first & 0x07) << 18) | ((second & 0x3f) << 12) |
          ((third & 0x3f) << 6) | (fourth & 0x3f)
        width = 4
      if codePoint <= 0xffff then
        characters(target) = codePoint.toChar
        target += 1
      else
        val adjusted = codePoint - 0x10000
        characters(target) = (0xd800 | (adjusted >>> 10)).toChar
        characters(target + 1) = (0xdc00 | (adjusted & 0x3ff)).toChar
        target += 2
      position += width
    if target != expectedUnits then throw new SQLDataException("CARRIER UTF-8 character count changed during decode")
    new String(characters)

  private def requireContinuation(value: Int): Unit =
    if (value & 0xc0) != 0x80 then throw new SQLDataException("invalid strict UTF-8 CARRIER identity")

  private def matchesCarrierCanonical(bytes: Array[Byte], data: DecodedCarrierKeyData): Boolean =
    val comparison = new ByteComparingOutputStream(bytes)
    val output = new DataOutputStream(comparison)
    writeCarrierCanonical(output, data)
    output.flush()
    comparison.matchesExactly

  private final class ByteComparingOutputStream(expected: Array[Byte]) extends OutputStream:
    private var position = 0L
    private var matches = true

    override def write(value: Int): Unit =
      if position >= expected.length.toLong || expected(position.toInt) != value.toByte then matches = false
      position = Math.addExact(position, 1L)

    override def write(values: Array[Byte], offset: Int, length: Int): Unit =
      if values == null || offset < 0 || length < 0 || offset > values.length - length then
        throw new IndexOutOfBoundsException("canonical comparison byte slice is invalid")
      var index = 0
      while index < length do
        write(values(offset + index).toInt)
        index += 1

    def matchesExactly: Boolean = matches && position == expected.length.toLong

  private def requireUuid(value: UUID, kind: String): Unit =
    if value == null || (value.getMostSignificantBits | value.getLeastSignificantBits) == 0L then
      throw new IllegalArgumentException(s"non-zero $kind resource UUID is required")

  private def compareForLock(left: MySqlRecoveryResourceKey, right: MySqlRecoveryResourceKey): Int =
    val kindOrder = left.resourceKind.compareTo(right.resourceKind)
    if kindOrder != 0 then kindOrder
    else
      val hashOrder = compareUnsigned(left.keyHashSha256, right.keyHashSha256)
      if hashOrder != 0 then hashOrder else left.compareCanonical(right)

  private def compareUnsigned(left: Vector[Byte], right: Vector[Byte]): Int =
    val common = Math.min(left.size, right.size)
    var index = 0
    while index < common do
      val difference = (left(index) & 0xff) - (right(index) & 0xff)
      if difference != 0 then return difference
      index += 1
    Integer.compare(left.size, right.size)
