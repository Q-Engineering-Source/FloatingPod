package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.server.io.IoOperationTag
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 直接验证编码器与 Main 计划；这些值不授予数据库或世界权限。 */
class MySqlRepositoryIdentityCodecTest:
  private def profile(retained: Long = 1_048_576L, packet: Long = 1_048_576L): SourceLimitProfile =
    SourceLimitProfile.of(1_048_576L, 1_048_576L, 1_048_576L, 64L, 65_536L,
      retained, packet, 1_048_576L)

  private def id(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")

  private def pod(value: Int, label: String = "identity-test"): FloatingPodState =
    FloatingPodState(id(value), None, 0L, label, 1.25, -2.5, 3.75, 15.0f, -5.0f, false, 1234L)

  @Test def mainPlanPinsExactTagAndReencodedOriginalRequestBeforeSql(): Unit =
    val limits = profile()
    val tag = IoOperationTag(id(1), 7L, 11L, id(2))
    val original = MySqlRepositoryOperation.Insert(pod(3))
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, id(4), id(5), original, limits)
    val replay = MySqlOwnedRepositoryOperationPlan.single(tag, id(4), id(5), original, limits)

    plan.requireTag(tag)
    plan.requireSingle(original, limits)
    assertEquals(plan, replay)
    assertEquals(plan.requestIdentitySha256, replay.requestIdentitySha256)
    assertEquals(tag.operationId, plan.executeOperationUuid)
    assertEquals("RUN", plan.requestKind)
    assertThrows(classOf[IllegalArgumentException], () => plan.requireTag(IoOperationTag(id(1), 7L, 12L, id(2))))
    assertThrows(classOf[IllegalArgumentException], () => plan.requireSingle(
      MySqlRepositoryOperation.Insert(pod(3).copy(posX = 1.5)), limits))
    assertThrows(classOf[IllegalArgumentException], () => {
      MySqlOwnedRepositoryOperationPlan.single(tag, id(4), tag.operationId, original, limits)
      ()
    })

  @Test def transactionIdentityPreservesOrderedEffectsAndRejectsAChangedOperation(): Unit =
    val limits = profile()
    val tag = IoOperationTag(id(11), 8L, 12L, id(12))
    val operations = Vector(MySqlRepositoryOperation.Insert(pod(13)), MySqlRepositoryOperation.Delete(id(14)))
    val plan = MySqlOwnedRepositoryOperationPlan.transaction(tag, id(15), id(16), operations, limits)
    plan.requireTransaction(operations, limits)
    assertEquals(plan.requestIdentitySha256,
      MySqlRepositoryRequestIdentity.transaction(operations, limits).sha256)
    assertThrows(classOf[IllegalArgumentException], () =>
      plan.requireTransaction(operations.reverse, limits))
    assertThrows(classOf[IllegalArgumentException], () =>
      plan.requireTransaction(operations.updated(0, MySqlRepositoryOperation.Insert(pod(13, "changed"))), limits))

  @Test def requestEncoderRejectsMalformedUnicodeAndExplicitBudgetOverflow(): Unit =
    val limits = profile()
    val invalidName = "unpaired-" + 0xd800.toChar
    val malformed = MySqlRepositoryOperation.Insert(pod(21, invalidName))
    assertThrows(classOf[SQLDataException], () => {
      MySqlRepositoryRequestIdentity.single(malformed, limits)
      ()
    })

    val narrow = profile(retained = 64L, packet = 1024L)
    assertThrows(classOf[SQLDataException], () => {
      MySqlRepositoryRequestIdentity.single(MySqlRepositoryOperation.FindByUuid(id(22)), narrow)
      ()
    })

  @Test def resourceKeyHasFixedBigEndianEncodingAndPreservesCompleteIdentity(): Unit =
    val limits = profile()
    val value = MySqlRecoveryResourceKey.pod(id(1), limits)
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    value.writeCanonicalTo(output)
    output.flush()
    assertEquals(
      "0000002446505F4D5953514C5F5245434F564552595F5245534F555243455F4B45595F42455F56310100000003504F4400000000000000000000000000000001",
      HexFormat.of().withUpperCase().formatHex(bytes.toByteArray))
    assertEquals(32, value.keyHashSha256.size)
    assertTrue(value.exactlyEquals(MySqlRecoveryResourceKey.pod(id(1), limits)))
    assertFalse(value.exactlyEquals(MySqlRecoveryResourceKey.pod(id(2), limits)))
    assertFalse(value.exactlyEquals(MySqlRecoveryResourceKey.sourceUnit(id(1), limits)))
    assertFalse(MySqlRecoveryResourceKey.carrier("save", "loader", "region", 1, 2, limits)
      .exactlyEquals(MySqlRecoveryResourceKey.carrier("save", "loader", "region", 2, 1, limits)))

  @Test def resourceLockOrderIsStableAndRejectsDuplicateFullKeys(): Unit =
    val limits = profile()
    val podA = MySqlRecoveryResourceKey.pod(id(31), limits)
    val podB = MySqlRecoveryResourceKey.pod(id(32), limits)
    val unit = MySqlRecoveryResourceKey.sourceUnit(id(33), limits)
    val carrier = MySqlRecoveryResourceKey.carrier("save", "loader", "region", -2, 3, limits)
    val input = Vector(unit, podB, carrier, podA)
    assertEquals(MySqlRecoveryResourceKey.orderForLocks(input),
      MySqlRecoveryResourceKey.orderForLocks(input.reverse))
    assertThrows(classOf[SQLDataException], () => {
      MySqlRecoveryResourceKey.orderForLocks(Vector(podA, MySqlRecoveryResourceKey.pod(id(31), limits)))
      ()
    })

  @Test def scopeEncodesIdentityResourcesAndRevisionsButNotRequestOrContextDigests(): Unit =
    val limits = profile()
    val resource = MySqlRecoveryResourceKey.pod(id(41), limits)
    val attempt = Some(id(42))
    val first = context(limits, Vector.fill(32)(1.toByte), "RUN")
    val changedRequestDigest = context(limits, Vector.fill(32)(2.toByte), "TRANSACTION")
    val originalScope = MySqlRecoveryOperationScope.encode(first, Vector(MySqlRecoveryResourceRevision(resource, 7L, attempt)))
    val sameScopeIdentity = MySqlRecoveryOperationScope.encode(changedRequestDigest,
      Vector(MySqlRecoveryResourceRevision(resource, 7L, attempt)))
    val nextRevision = MySqlRecoveryOperationScope.encode(first, Vector(MySqlRecoveryResourceRevision(resource, 8L, attempt)))

    assertNotEquals(first.canonicalSha256, changedRequestDigest.canonicalSha256)
    assertTrue(originalScope.exactlyEquals(sameScopeIdentity), "scope cannot nest request/context digests")
    assertFalse(originalScope.exactlyEquals(nextRevision), "resource revision is part of the scope")
  
  @Test def scopeResourceInputOrderDoesNotChangeCanonicalBytes(): Unit =
    val limits = profile()
    val firstResource = MySqlRecoveryResourceKey.pod(id(51), limits)
    val secondResource = MySqlRecoveryResourceKey.sourceUnit(id(52), limits)
    val contextIdentity = context(limits, Vector.fill(32)(3.toByte), "RUN")
    val forward = Vector(MySqlRecoveryResourceRevision(firstResource, 2L, Some(id(53))),
      MySqlRecoveryResourceRevision(secondResource, 4L, None))
    assertTrue(MySqlRecoveryOperationScope.encode(contextIdentity, forward)
      .exactlyEquals(MySqlRecoveryOperationScope.encode(contextIdentity, forward.reverse)))

  private def context(
      limits: SourceLimitProfile,
      requestSha: Vector[Byte],
      requestKind: String
  ): MySqlRepositoryOperationContextIdentity =
    new MySqlRepositoryOperationContextIdentity(
      IoOperationTag(id(61), 9L, 13L, id(62)), id(63), id(64), id(65), 123L, "fpb1:" + ("A" * 43),
      id(66), 3L, LocalDateTime.of(2026, 10, 8, 12, 30, 15, 123456000), id(67), "9.7.2", "fp_format3_test",
      "fp_test@127.0.0.1", id(68), 3, 2, "a" * 64, "b" * 64, Vector.fill(32)(4.toByte),
      "FRESH_PLAN", Vector.fill(32)(5.toByte), limits.sha256().toVector, requestKind, requestSha, limits)
