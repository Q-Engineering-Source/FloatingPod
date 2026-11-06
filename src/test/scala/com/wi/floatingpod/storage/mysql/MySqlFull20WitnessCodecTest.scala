package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1FreshPrepareRecordFixture, B1ResumeRecordCanonical}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.io.EOFException

/** 实际 fixed DATA encoder/decoder；fixture 不发行 actor/SQL/native 资格。 */
final class MySqlFull20WitnessCodecTest:
  private var retainedDrafts = Vector.empty[MySqlFull20WitnessCodec.DecodeDraft]
  private def decode(input: Array[Byte], profile: com.wi.floatingpod.config.SourceLimitProfile,
      root: MySqlFormat3ReadBudget): MySqlFull20WitnessData =
    val draft = MySqlFull20WitnessCodec.draft(input, profile, root)
    retainedDrafts = retainedDrafts :+ draft
    MySqlFull20WitnessCodec.decode(draft)
  private def envelope(source: MySqlFormat3FullSnapshot): MySqlFormat3Binary =
    val root = source.allocationBudget
    val before = MySqlFormat3SnapshotCodec.encode(source, root)
    val target = MySqlFormat3SnapshotCodec.encode(source, root)
    val context = B1ResumeRecordCanonical.encode(B1FreshPrepareRecordFixture.validRecord())
    MySqlFull20WitnessCodec.encodeParts(source.limitProfile.canonicalBytes(), context, before, target, root)
  private def copy(binary: MySqlFormat3Binary): Array[Byte] = Array.tabulate[Byte](binary.length)(binary.byteAt)

  @Test def completeHistoryAbove64KiBRoundtripsBothTypedGraphsOnOriginalLedger(): Unit =
    val source = MySqlFull20CodecFixture.snapshot(history = 70000)
    val encoded = envelope(source)
    assertTrue(encoded.length > 65536)
    val decoded = decode(copy(encoded), source.limitProfile, source.allocationBudget)
    assertTrue(decoded.before.snapshot.exactlyEquals(source))
    assertTrue(decoded.target.snapshot.exactlyEquals(source))
    assertTrue(decoded.before.snapshot.allocationBudget.sharesAllocationLedger(source.allocationBudget))
    assertEquals(20, decoded.before.snapshot.relations.size)
    assertEquals(21L, decoded.before.snapshot.rowCount)
    val again = MySqlFull20WitnessCodec.encodeParts(source.limitProfile.canonicalBytes(),
      B1ResumeRecordCanonical.encode(decoded.originalRecord), decoded.before.canonical, decoded.target.canonical,
      source.allocationBudget)
    assertEquals(encoded, again)
    val draft = retainedDrafts.last
    assertSame(decoded.before.snapshot.allocationBudget, draft.beforeChild)
    assertSame(decoded.target.snapshot.allocationBudget, draft.targetChild)
    assertNotSame(draft.beforeChild, draft.targetChild)
    val accounted = source.allocationBudget.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () => draft.beforeChild.closeScopedReadBudget())
    assertThrows(classOf[IllegalArgumentException], () => draft.targetChild.closeScopedReadBudget())
    assertThrows(classOf[IllegalArgumentException], () => source.allocationBudget.closeRootReadBudget())
    assertEquals(accounted, source.allocationBudget.accountedMemoryBytes)
    assertFalse(source.allocationBudget.isClosed)

  @Test def actualFramingTruncationTrailingAndForeignProfileFailWithoutRetiringRoot(): Unit =
    val source = MySqlFull20CodecFixture.snapshot()
    val bytes = copy(envelope(source))
    assertThrows(classOf[EOFException], () => decode(bytes.take(3), source.limitProfile, source.allocationBudget))
    assertThrows(classOf[IllegalArgumentException], () => decode(bytes ++ Array[Byte](0), source.limitProfile, source.allocationBudget))
    val wrong = bytes.clone()
    wrong(4) = (wrong(4) ^ 1).toByte
    assertThrows(classOf[IllegalArgumentException], () => decode(wrong, source.limitProfile, source.allocationBudget))
    assertThrows(classOf[IllegalArgumentException], () => decode(bytes,
      MySqlFull20CodecFixture.profile(), source.allocationBudget))
    assertFalse(source.allocationBudget.isClosed)

  @Test def partsFromForeignLedgerAndClosedOriginalCannotIssueAnEnvelope(): Unit =
    val first = MySqlFull20CodecFixture.snapshot()
    val second = MySqlFull20CodecFixture.snapshot(p = first.limitProfile)
    val a = MySqlFormat3SnapshotCodec.encode(first, first.allocationBudget)
    val b = MySqlFormat3SnapshotCodec.encode(second, second.allocationBudget)
    val record = B1ResumeRecordCanonical.encode(B1FreshPrepareRecordFixture.validRecord())
    assertThrows(classOf[IllegalArgumentException], () => MySqlFull20WitnessCodec.encodeParts(
      first.limitProfile.canonicalBytes(), record, a, b, first.allocationBudget))
    assertFalse(first.allocationBudget.isClosed)
    first.allocationBudget.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlFull20WitnessCodec.encodeParts(
      first.limitProfile.canonicalBytes(), record, a, a, first.allocationBudget))

  @Test def sharedRootNearCapacityRejectsBeforeFinalArrayAndLeavesOriginalHistoryCharged(): Unit =
    val source = MySqlFull20CodecFixture.snapshot(history = 70000)
    val root = source.allocationBudget
    val canonical = MySqlFormat3SnapshotCodec.encode(source, root)
    val record = B1ResumeRecordCanonical.encode(B1FreshPrepareRecordFixture.validRecord())
    root.reserveTemporaryMemory(root.availableMemoryBytes - 1L, "test-owned live competing workspace")
    val charged = root.accountedMemoryBytes
    val heldHistory = source.relation("physical_source_forest")
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull20WitnessCodec.encodeParts(
      source.limitProfile.canonicalBytes(), record, canonical, canonical, root))
    assertEquals(charged, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertSame(heldHistory, source.relation("physical_source_forest"))
    assertEquals(21L, source.rowCount)

  @Test def successfulBeforeRemainsRetainedWhenTargetCapacityFailsWithOriginalCause(): Unit =
    val source = MySqlFull20CodecFixture.snapshot(history = 70000)
    val draft = MySqlFull20WitnessCodec.draft(copy(envelope(source)),source.limitProfile,source.allocationBudget)
    MySqlFull20WitnessCodec.decodeBefore(draft)
    val originalBefore = draft.before
    val originalContext = draft.record
    val root = source.allocationBudget
    assertTrue(source.exactlyEquals(originalBefore.snapshot))
    root.reserveTemporaryMemory(root.availableMemoryBytes - 1L,"actual live competing test workspace")
    val failure = assertThrows(classOf[java.sql.SQLDataException], () => MySqlFull20WitnessCodec.completeTarget(draft))
    assertSame(failure,draft.failure)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertSame(originalBefore,draft.before)
    assertSame(originalContext,draft.record)
    assertTrue(source.exactlyEquals(draft.before.snapshot))
    assertNull(draft.target)
    assertNull(draft.result)
    assertFalse(root.isClosed)

  @Test def successfulBeforeAndContextRemainRetainedWhenTargetIsMalformed(): Unit =
    val source = MySqlFull20CodecFixture.snapshot()
    val root = source.allocationBudget
    val before = MySqlFormat3SnapshotCodec.encode(source,root)
    val badBytes = MySqlFull20CodecFixture.bytes(before)
    badBytes(0) = (badBytes(0) ^ 1).toByte
    val target = MySqlFormat3SnapshotCodec.capture(badBytes,source.limitProfile,root)
    val content = MySqlFull20WitnessCodec.encodeParts(source.limitProfile.canonicalBytes(),
      B1ResumeRecordCanonical.encode(B1FreshPrepareRecordFixture.validRecord()),before,target,root)
    val draft = MySqlFull20WitnessCodec.draft(copy(content),source.limitProfile,root)
    MySqlFull20WitnessCodec.decodeBefore(draft)
    val originalBefore = draft.before
    val originalContext = draft.record
    val failure = assertThrows(classOf[Throwable], () => MySqlFull20WitnessCodec.completeTarget(draft))
    assertSame(failure,draft.failure)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertSame(originalBefore,draft.before)
    assertSame(originalContext,draft.record)
    assertNotNull(draft.targetBytes)
    assertTrue(source.exactlyEquals(draft.before.snapshot))
    assertNull(draft.target)
    assertNull(draft.result)
    assertFalse(root.isClosed)
