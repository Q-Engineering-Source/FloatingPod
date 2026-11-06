package com.wi.floatingpod.storage.mysql

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import java.util.HexFormat

/** 一项待提交的领域效果身份与其目标状态摘要；摘要本身不是权限或 custody 证明。 */
final case class MySqlExpectedEffect(
    targetIdentity: String,
    expectedVersion: Long,
    desiredEffectFingerprintSha256: Vector[Byte]
):
  require(
    targetIdentity != null && targetIdentity.nonEmpty && targetIdentity.forall(character => character >= '!' && character <= '~'),
    "effect target identity must use non-empty printable ASCII"
  )
  require(expectedVersion >= 0L, "effect expectedVersion must not be negative")
  require(desiredEffectFingerprintSha256 != null, "effect fingerprint must not be null")
  require(desiredEffectFingerprintSha256.size == MySqlExpectedEffect.FingerprintLength, "effect fingerprint must contain 32 bytes")

  override def toString: String = "MySqlExpectedEffect[redacted]"

object MySqlExpectedEffect:
  private[mysql] val FingerprintLength = 32

/** 一次外层 MySQL 写操作的 boot、fence、binding、operation 与预期效果快照。 */
final class MySqlOperationContext private (
    val bootUuid: UUID,
    val writerEpoch: Long,
    val bindingUuid: UUID,
    val operationUuid: UUID,
    val expectedEffects: Vector[MySqlExpectedEffect]
):
  lazy val canonicalFingerprintSha256: Vector[Byte] =
    MySqlOperationContext.fingerprint(bootUuid, writerEpoch, bindingUuid, operationUuid, expectedEffects)

  def canonicalFingerprintHex: String =
    HexFormat.of().formatHex(canonicalFingerprintSha256.toArray)

  /** 只比较 receipt slot 身份；效果摘要另由精确重放判定比较。 */
  def hasSameOperationIdentity(other: MySqlOperationContext): Boolean =
    other != null
      && bootUuid == other.bootUuid
      && writerEpoch == other.writerEpoch
      && bindingUuid == other.bindingUuid
      && operationUuid == other.operationUuid

  /** 只有 slot 和完整规范化效果集合都相同才算精确重放。 */
  def isExactReplayOf(other: MySqlOperationContext): Boolean =
    hasSameOperationIdentity(other)
      && MessageDigest.isEqual(canonicalFingerprintSha256.toArray, other.canonicalFingerprintSha256.toArray)

  override def equals(other: Any): Boolean = other match
    case that: MySqlOperationContext =>
      hasSameOperationIdentity(that) && expectedEffects == that.expectedEffects
    case _ => false

  override def hashCode(): Int =
    (bootUuid, writerEpoch, bindingUuid, operationUuid, expectedEffects).hashCode

  override def toString: String = "MySqlOperationContext[redacted]"

object MySqlOperationContext:
  private val FingerprintDomain = "com.wi.floatingpod.mysql-operation-context/v1"
  private val Sha256Length = 32

  def apply(
      bootUuid: UUID,
      writerEpoch: Long,
      bindingUuid: UUID,
      operationUuid: UUID,
      expectedEffects: Iterable[MySqlExpectedEffect]
  ): MySqlOperationContext =
    require(bootUuid != null, "bootUuid must not be null")
    require(writerEpoch >= 0L, "writerEpoch must not be negative")
    require(bindingUuid != null, "bindingUuid must not be null")
    require(operationUuid != null, "operationUuid must not be null")
    require(expectedEffects != null, "expectedEffects must not be null")

    val canonicalEffects = expectedEffects.iterator.toVector
    require(canonicalEffects.nonEmpty, "expectedEffects must not be empty")
    require(canonicalEffects.forall(_ != null), "expectedEffects must not contain null")
    val orderedEffects = canonicalEffects.sortBy(_.targetIdentity)
    require(
      orderedEffects.iterator.map(_.targetIdentity).toSet.size == orderedEffects.size,
      "expectedEffects must have unique target identities"
    )

    new MySqlOperationContext(bootUuid, writerEpoch, bindingUuid, operationUuid, orderedEffects)

  private def fingerprint(
      bootUuid: UUID,
      writerEpoch: Long,
      bindingUuid: UUID,
      operationUuid: UUID,
      expectedEffects: Vector[MySqlExpectedEffect]
  ): Vector[Byte] =
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    try
      writeBytes(output, FingerprintDomain.getBytes(StandardCharsets.US_ASCII))
      writeUuid(output, bootUuid)
      output.writeLong(writerEpoch)
      writeUuid(output, bindingUuid)
      writeUuid(output, operationUuid)
      output.writeInt(expectedEffects.size)
      expectedEffects.foreach { effect =>
        writeBytes(output, effect.targetIdentity.getBytes(StandardCharsets.US_ASCII))
        output.writeLong(effect.expectedVersion)
        writeBytes(output, effect.desiredEffectFingerprintSha256.toArray)
      }
      output.flush()
      val digest = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray)
      if digest.length != Sha256Length then throw new IllegalStateException("SHA-256 returned an invalid digest length")
      digest.toVector
    finally output.close()

  private def writeUuid(output: DataOutputStream, value: UUID): Unit =
    output.writeLong(value.getMostSignificantBits)
    output.writeLong(value.getLeastSignificantBits)

  private def writeBytes(output: DataOutputStream, value: Array[Byte]): Unit =
    output.writeInt(value.length)
    output.write(value)

/** commit() 已执行但确认结果未知时保留 operation 身份与原始 cause。 */
final class MySqlCommitUnknownException private (
    val operationContext: MySqlOperationContext,
    originalCause: Throwable
) extends IllegalStateException(
      "MySQL commit outcome is unknown for operation " + operationContext.operationUuid,
      originalCause
    )

object MySqlCommitUnknownException:
  def apply(operationContext: MySqlOperationContext, originalCause: Throwable): MySqlCommitUnknownException =
    require(operationContext != null, "operationContext must not be null")
    require(originalCause != null, "originalCause must not be null")
    new MySqlCommitUnknownException(operationContext, originalCause)
