package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID

class MySqlOperationContextTest:
  @Test
  def canonicalFingerprintDoesNotDependOnEffectInputOrder(): Unit =
    val first = context(effects = Vector(effect("pod/one", 3L, 11), effect("locator/one", 4L, 29)))
    val reversed = context(effects = Vector(effect("locator/one", 4L, 29), effect("pod/one", 3L, 11)))

    assertEquals(first.canonicalFingerprintSha256, reversed.canonicalFingerprintSha256)
    assertEquals(first.expectedEffects, reversed.expectedEffects)
    assertTrue(first.isExactReplayOf(reversed))

  @Test
  def sameOperationIdentityWithDifferentExpectedEffectIsNotAnExactReplay(): Unit =
    val original = context()
    val changedEffect = context(effects = Vector(effect("pod/one", 3L, 12), effect("locator/one", 4L, 29)))

    assertTrue(original.hasSameOperationIdentity(changedEffect))
    assertFalse(original.isExactReplayOf(changedEffect))
    assertNotEquals(original.canonicalFingerprintSha256, changedEffect.canonicalFingerprintSha256)

  @Test
  def canonicalFingerprintBindsBootEpochBindingAndOperationIdentity(): Unit =
    val original = context()
    val changedBoot = context(bootUuid = uuid("00000000-0000-0000-0000-000000000002"))
    val changedEpoch = context(writerEpoch = 8L)
    val changedBinding = context(bindingUuid = uuid("00000000-0000-0000-0000-000000000003"))
    val changedOperation = context(operationUuid = uuid("00000000-0000-0000-0000-000000000004"))

    List(changedBoot, changedEpoch, changedBinding, changedOperation).foreach { changed =>
      assertFalse(original.hasSameOperationIdentity(changed))
      assertFalse(original.isExactReplayOf(changed))
      assertNotEquals(original.canonicalFingerprintSha256, changed.canonicalFingerprintSha256)
    }

  @Test
  def rejectsEmptyDuplicateOrMalformedExpectedEffects(): Unit =
    assertThrows(
      classOf[IllegalArgumentException],
      () => context(effects = Vector.empty)
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => context(effects = Vector(effect("pod/one", 3L, 11), effect("pod/one", 4L, 29)))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlExpectedEffect(" ", 0L, fingerprint(1))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlExpectedEffect("pod/one", -1L, fingerprint(1))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => MySqlExpectedEffect("pod/one", 0L, Vector(1.toByte))
    )
    assertThrows(
      classOf[IllegalArgumentException],
      () => context(writerEpoch = -1L)
    )

  @Test
  def unknownCommitRetainsTheOriginalCauseAndOperationContext(): Unit =
    val operation = context()
    val primary = new IllegalStateException("commit acknowledgement was lost")

    val unknown = MySqlCommitUnknownException(operation, primary)

    assertSame(primary, unknown.getCause)
    assertEquals(operation, unknown.operationContext)
    assertTrue(unknown.getMessage.contains(operation.operationUuid.toString))

  private def context(
      bootUuid: UUID = uuid("00000000-0000-0000-0000-000000000001"),
      writerEpoch: Long = 7L,
      bindingUuid: UUID = uuid("00000000-0000-0000-0000-000000000002"),
      operationUuid: UUID = uuid("00000000-0000-0000-0000-000000000003"),
      effects: Vector[MySqlExpectedEffect] = Vector(effect("pod/one", 3L, 11), effect("locator/one", 4L, 29))
  ): MySqlOperationContext =
    MySqlOperationContext(bootUuid, writerEpoch, bindingUuid, operationUuid, effects)

  private def effect(target: String, version: Long, seed: Int): MySqlExpectedEffect =
    MySqlExpectedEffect(target, version, fingerprint(seed))

  private def fingerprint(seed: Int): Vector[Byte] =
    Vector.tabulate(32)(index => (seed + index).toByte)

  private def uuid(value: String): UUID = UUID.fromString(value)
  @Test
  def canonicalFingerprintHasAStableVersionedBigEndianEncoding(): Unit =
    val singleEffect = context(effects = Vector(effect("pod/one", 3L, 11)))

    assertEquals("764654a2daf09f9cda9bd649d9ec904c62a44b04ee5be1b2611356ba3154c188", singleEffect.canonicalFingerprintHex)
