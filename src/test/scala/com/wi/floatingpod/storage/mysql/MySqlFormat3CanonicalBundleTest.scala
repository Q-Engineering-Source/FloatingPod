package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.SQLDataException

final class MySqlFormat3CanonicalBundleTest:
  import MySqlFull20CodecFixture.*

  @Test def completeBundleRetainsOriginalBytesDecodedRowsAndEveryLiveSharedCharge(): Unit =
    val source = snapshot(history = 70000)
    val root = source.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(source, root)
    val before = root.accountedMemoryBytes
    val bundle = MySqlFormat3CanonicalBundle.decode(encoded, source.limitProfile, root)
    assertSame(encoded, bundle.canonical)
    assertSame(root, bundle.originalBudget)
    assertSame(source.limitProfile, bundle.profile)
    assertTrue(bundle.snapshot.allocationBudget.sharesAllocationLedger(root))
    assertTrue(bundle.byteLength > 65536)
    assertEquals(source.relations, bundle.snapshot.relations)
    assertTrue(root.accountedMemoryBytes > before + 70000L)
    assertFalse(encoded.storageBudget.isClosed)
    assertFalse(bundle.snapshot.allocationBudget.isClosed)
    val again = MySqlFormat3SnapshotCodec.encode(bundle.snapshot, root)
    assertArrayEquals(bytes(encoded), bytes(again))
    assertTrue(root.accountedMemoryBytes > before + bundle.byteLength.toLong)

  @Test def malformedFailureDropsOnlyItsLocalDecodeDebtAndKeepsInputAndOriginalCause(): Unit =
    val source = snapshot()
    val root = source.allocationBudget
    val valid = MySqlFormat3SnapshotCodec.encode(source, root)
    val malformed = bytes(valid) ++ Array[Byte](0)
    val input = capture(malformed, source)
    val before = root.accountedMemoryBytes
    val childCount = root.retainedCanonicalDataChildCount
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlFormat3CanonicalBundle.decode(input, source.limitProfile, root))
    val stack = cause.getStackTrace
    assertEquals("trailing full20 canonical bytes", cause.getMessage)
    assertTrue(stack.nonEmpty)
    assertTrue(stack.sameElements(cause.getStackTrace))
    assertEquals(before + 4096L + 128L, root.accountedMemoryBytes,
      "first borrow group and surviving input member remain charged; failed decoder child is settled")
    assertEquals(childCount, root.retainedCanonicalDataChildCount,
      "failed DATA child must be unlinked before its metadata is credited")
    val warmed = root.accountedMemoryBytes
    assertThrows(classOf[SQLDataException], () => MySqlFormat3CanonicalBundle.decode(input, source.limitProfile, root))
    assertEquals(warmed, root.accountedMemoryBytes, "repeated failed decode cannot retain another child or charge a second group")
    assertEquals(childCount, root.retainedCanonicalDataChildCount)
    assertFalse(root.isClosed)
    assertFalse(input.storageBudget.isClosed)
    assertArrayEquals(malformed, bytes(input))
    assertArrayEquals(bytes(valid), bytes(MySqlFormat3SnapshotCodec.encode(source, root)))

  @Test def actualRemainingSharedRootRejectsBeforeAnotherPayloadOrDecodedGraphCanAllocate(): Unit =
    val source = snapshot()
    val root = source.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(source, root)
    // A real second retained owner consumes this same ledger, not a private mirror budget.
    val otherDebt = root.availableMemoryBytes - 1L
    root.reserveRetainedMemory(otherDebt, "test second actual retained owner")
    val before = root.accountedMemoryBytes
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, root))
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SnapshotCodec.encode(source, root))
    assertEquals(before, root.accountedMemoryBytes)
    assertEquals(1L, root.availableMemoryBytes)
    assertFalse(encoded.storageBudget.isClosed)
    root.releaseRetainedMemory(otherDebt, "test second owner dropped its refs")
    val decoded = MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, root)
    assertEquals(source.relations, decoded.relations)

  @Test def foreignProfileRootAndCurrentScopeCannotBorrowOriginalPayloadOwnership(): Unit =
    val source = snapshot()
    val root = source.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(source, root)
    val foreign = new MySqlFormat3ReadBudget(source.limitProfile, source.limitProfile.maxSqlPacketBytes())
    val before = root.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, foreign))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SnapshotCodec.encode(source, foreign))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3CanonicalBundle.decode(encoded, profile(), root))
    MySqlRecoveryCanonicalBytes.withAllocationBudget(foreign) {
      assertThrows(classOf[IllegalStateException], () => MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, root))
    }
    assertEquals(before, root.accountedMemoryBytes)
    assertEquals(0L, foreign.accountedMemoryBytes)

  @Test def closedPayloadAndClosedRootFailExplicitlyInsteadOfManufacturingCurrentData(): Unit =
    val source = snapshot()
    val root = source.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(source, root)
    // Deliberate lifecycle misuse in DATA-only test, not a production ACK or resource-close claim.
    encoded.storageBudget.closeScopedReadBudget()
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SnapshotCodec.decode(encoded, source.limitProfile, root))
    root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlFormat3SnapshotCodec.encode(source, root))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SnapshotCodec.capture(Array[Byte](1), source.limitProfile, root))

  @Test def inputCaptureIsDefensiveAndASecondLiveCanonicalCopyStaysCharged(): Unit =
    val source = snapshot()
    val root = source.allocationBudget
    val encoded = MySqlFormat3SnapshotCodec.encode(source, root)
    val mutable = bytes(encoded)
    val before = root.accountedMemoryBytes
    val held = MySqlFormat3SnapshotCodec.capture(mutable, source.limitProfile, root)
    mutable(0) = (mutable(0) ^ 0xff).toByte
    assertArrayEquals(bytes(encoded), bytes(held))
    assertTrue(root.accountedMemoryBytes >= before + encoded.byteLength.toLong)
    assertEquals(source.relations, MySqlFormat3SnapshotCodec.decode(held, source.limitProfile, root).relations)
