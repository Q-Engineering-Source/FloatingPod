package com.wi.floatingpod.storage.mysql

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.sql.SQLDataException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

final class MySqlImmutableBinaryBorrowTest:
  private def root(): MySqlFormat3ReadBudget =
    val profile = MySqlFull20CodecFixture.profile()
    new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())

  @Test def realRangeKeepsUnsignedHashWriteCopyAndStorageLifetime(): Unit =
    val budget = root()
    val original = Array[Byte](9, -128, -1, 0, 127, 8)
    val owned = MySqlFormat3Binary.copyOf(original, budget, "actual immutable source")
    original(2) = 10
    val payload = new MySqlFormat3CanonicalPayload(owned, budget, budget)
    val child = budget.newCanonicalDataScope()
    val attempt = child.beginDecodeBorrow(payload)
    val range = MySqlFormat3Binary.borrowedRange(owned, 1, 4, attempt)
    attempt.complete()
    val expected = Array[Byte](-128, -1, 0, 127)
    assertTrue(range.equalsBytes(expected)); assertTrue(range.equalsVector(expected.toVector))
    assertEquals(java.util.Arrays.hashCode(expected), range.hashCode())
    val dense = MySqlFormat3Binary.copyOf(expected, budget, "independent equality source")
    assertEquals(dense, range); assertEquals(range, dense); assertEquals(0, range.compareTo(dense))
    assertTrue(range.compareTo(MySqlFormat3Binary.copyOf(Array[Byte](127), budget, "unsigned comparison")) > 0)
    val bytes = new ByteArrayOutputStream; range.writeTo(new DataOutputStream(bytes))
    assertArrayEquals(expected, bytes.toByteArray); assertArrayEquals(expected, range.copyBytes(budget, "actual range copy"))
    assertThrows(classOf[IllegalArgumentException], () => budget.closeRootReadBudget())
    assertThrows(classOf[IllegalArgumentException], () => child.closeScopedReadBudget())
    assertFalse(budget.isClosed); assertFalse(child.isClosed)

  @Test def emptyAndForeignRangesRejectBeforePublishingAnotherBorrow(): Unit =
    val budget = root(); val owned = MySqlFormat3Binary.copyOf(Array[Byte](1,2,3), budget, "source")
    val payload = new MySqlFormat3CanonicalPayload(owned, budget, budget)
    val child = budget.newCanonicalDataScope(); val attempt = child.beginDecodeBorrow(payload)
    val before = budget.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () => { MySqlFormat3Binary.borrowedRange(owned, Int.MaxValue, 1, attempt); () })
    assertEquals(before, budget.accountedMemoryBytes)
    val empty = MySqlFormat3Binary.borrowedRange(owned, 3, 0, attempt)
    assertEquals(0, empty.length)
    val cursor = new MySqlFormat3Binary.Cursor; cursor.begin(empty, 0, 0)
    assertFalse(cursor.seekPending); assertFalse(cursor.needsAdvance)
    assertThrows(classOf[IllegalArgumentException], () => { cursor.next(); () })
    val foreign = root().newCanonicalDataScope()
    assertThrows(classOf[IllegalArgumentException], () => { foreign.beginDecodeBorrow(payload); () })
    val comparison = budget.newCanonicalScope()
    assertThrows(classOf[IllegalArgumentException], () => { comparison.beginDecodeBorrow(payload); () })
    attempt.complete()

  @Test def sequentialCursorKeepsTwoRangesIndependentAndNeverReseeksEachByte(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val budget = images.before.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(images.before, budget)
    val decoded = MySqlFormat3SnapshotCodec.decode(encoded, budget.profile, budget)
    val rows = decoded.relation("fp_recovery_operation").rows
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      val historical = MySqlRecoveryOperationRawEnvelopeCodec.encodeHistorical(rows.head, budget.profile, rows)
      val dense = MySqlRecoveryOperationRawEnvelopeCodec.encode(rows.head, budget.profile)
      assertTrue(historical.canonical.exactlyEquals(dense.canonical))
      val value = historical.canonical.borrowedBinary
      val left = new MySqlFormat3Binary.Cursor; val right = new MySqlFormat3Binary.Cursor
      var advances = 0
      for start <- Vector(0, value.length / 2, value.length - 1, 1, 0) do
        val count = Math.min(257, value.length - start)
        left.begin(value, start, count); right.begin(value, start, count)
        var probes = 0
        while left.seekPending do
          left.seekStep(); probes += 1
        while right.seekPending do right.seekStep()
        assertTrue(probes > 0 && probes < 16)
        var index = 0
        while index < count do
          if left.needsAdvance then { left.advance(); advances += 1 }
          if right.needsAdvance then right.advance()
          assertEquals(dense.canonical.borrowedBinary.byteAt(start + index), left.next())
          assertEquals(value.byteAt(start + index), right.next())
          assertFalse(left.seekPending); index += 1
      left.begin(value, 0, value.length)
      while left.seekPending do left.seekStep()
      var index = 0
      while index < value.length do
        if left.needsAdvance then { left.advance(); advances += 1 }
        assertEquals(dense.canonical.borrowedBinary.byteAt(index), left.next()); index += 1
      assertTrue(advances > 0)
      assertEquals(dense.canonical.sha256, historical.canonical.sha256)
      assertThrows(classOf[IllegalArgumentException], () => budget.closeRootReadBudget())
    }

  @Test def actualFailedDecodeUnlinksChildButPreservesOriginalStorageAndCause(): Unit =
    val snapshot = MySqlFull20CodecFixture.snapshot()
    val budget = snapshot.allocationBudget
    val valid = MySqlFormat3SnapshotCodec.encode(snapshot, budget)
    val bytes = MySqlFull20CodecFixture.bytes(valid) ++ Array[Byte](0)
    val malformed = MySqlFormat3SnapshotCodec.capture(bytes, budget.profile, budget)
    val children = budget.retainedCanonicalDataChildCount
    val failure = assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(malformed, budget.profile, budget))
    assertEquals("trailing full20 canonical bytes", failure.getMessage)
    assertEquals(children, budget.retainedCanonicalDataChildCount)
    assertFalse(malformed.storageBudget.isClosed)
    assertTrue(malformed.bytes.equalsBytes(bytes))
    assertFalse(malformed.storageBudget.hasActiveImmutableBorrows)

  @Test def exactProjectionRegistryEndsOnlyItsRegisteredInternalGroup(): Unit =
    val budget = root()
    val source = MySqlFormat3Binary.copyOf(Array[Byte](1,2,3), budget, "projection source")
    val payload = new MySqlFormat3CanonicalPayload(source, budget, budget)
    val child = budget.newCanonicalDataScope(); val attempt = child.beginDecodeBorrow(payload)
    MySqlFormat3Binary.borrowedRange(source, 0, 3, attempt); attempt.complete()
    val tag = com.wi.floatingpod.server.io.IoOperationTag(new java.util.UUID(1L,2L), 1L, 1L, new java.util.UUID(3L,4L))
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), tag.bootId, tag.bootGeneration, tag.connectionGeneration)
    registry.retainRoot(tag, budget)
    registry.release(tag)
    assertTrue(budget.isClosed); assertTrue(child.isClosed)
    assertEquals(0, registry.pendingCount)
    assertThrows(classOf[IllegalArgumentException], () => registry.release(tag))

  @Test def partialRawBeforeFirstEdgeCannotRefundItsRetainedConstruction(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val profile = images.fixture.profile
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val row = images.before.relation("fp_recovery_operation").rows.head.map {
      case MySqlFormat3Cell.Binary(value) =>
        val input = value.copyBytes(images.before.allocationBudget, "test actual detached input")
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.copyOf(input, budget, "actual immutable original row source"))
      case value => value
    }
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      val dense = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
      val binaryBytes = row.collect { case MySqlFormat3Cell.Binary(value) => value.length.toLong }.sum
      val literalBytes = dense.canonical.borrowedBinary.length.toLong - binaryBytes
      // 实际literal array先成功；紧随的source array预收只剩1byte，尚未登记任何edge。
      val remaining = 4096L + 128L + 512L + literalBytes + 128L + 1L
      budget.reserveRetainedMemory(budget.availableMemoryBytes - remaining, "existing obligation before RAW source array")
      val cause = assertThrows(classOf[SQLDataException], () =>
        MySqlRecoveryOperationRawEnvelopeCodec.encodeHistorical(row, profile, Vector(row)))
      assertTrue(cause.getMessage.contains("historical RAW source array"))
      val retained = budget.accountedMemoryBytes
      assertThrows(classOf[IllegalArgumentException], () => budget.closeRootReadBudget())
      assertFalse(budget.isClosed)
      assertEquals(retained, budget.accountedMemoryBytes)
      assertThrows(classOf[IllegalArgumentException], () => budget.closeRootReadBudget())
      assertEquals(retained, budget.accountedMemoryBytes)
      assertThrows(classOf[IllegalArgumentException], () => budget.endProjectionBorrowGroup())
      assertEquals(retained, budget.accountedMemoryBytes)
    }
