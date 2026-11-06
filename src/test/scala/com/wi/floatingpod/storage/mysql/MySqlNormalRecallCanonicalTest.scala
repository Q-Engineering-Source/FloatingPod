package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.PhysicalRecoveryChunk
import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream, ByteArrayInputStream}
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** DATA wire 与预算边界；不制造 Ready 或物理 ACK。 */
final class MySqlNormalRecallCanonicalTest:
  private def id(n: Long): UUID = new UUID(0L, n)
  private def profile(limit: Long = 100000L): SourceLimitProfile =
    SourceLimitProfile.of(100000L, 100000L, 100000L, 32L, 256L, limit, limit, 100000L)
  private def facts: MySqlNormalRecallRemovalFacts =
    MySqlNormalRecallRemovalFacts(IoOperationTag(id(1), 2L, 3L, id(4)), id(5), id(6),
      7L, -1, 9, 10L, 12L, 13L)
  private def chunks: Vector[PhysicalRecoveryChunk] =
    (for x <- -2 to 2; z <- -2 to 2 yield PhysicalRecoveryChunk(-1, x, z)).toVector

  private def withRoot[A](run: MySqlFormat3ReadBudget => A): A =
    val p = profile()
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    try run(root)
    finally root.closeRootReadBudget()
  private def raw(ticket: Boolean): Array[Byte] = withRoot { root =>
    val encoded = if ticket then MySqlNormalRecallCanonical.ticket(MySqlNormalRecallTicketFacts(facts, id(8), chunks), root)
      else MySqlNormalRecallCanonical.removal(facts, root)
    encoded.copyBytes(root, "strict physical test input")
  }
  private def rejectPhysical(bytes: Array[Byte], ticket: Boolean): Unit = withRoot { root =>
    val input = MySqlFormat3Binary.takeOwned(bytes)
    assertThrows(classOf[Exception], () => {
      if ticket then MySqlNormalRecallCanonical.decodeTicket(input, root)
      else MySqlNormalRecallCanonical.decodeRemoval(input, root)
      ()
    })
  }

  @Test def strictPhysicalDecodersRoundtripCompleteOriginalFacts(): Unit = withRoot { root =>
    val removed = MySqlNormalRecallCanonical.removal(facts, root)
    assertEquals(facts, MySqlNormalRecallCanonical.decodeRemoval(removed.borrowedBinary, root))
    val released = MySqlNormalRecallTicketFacts(facts, id(8), chunks)
    val ticket = MySqlNormalRecallCanonical.ticket(released, root)
    assertEquals(released, MySqlNormalRecallCanonical.decodeTicket(ticket.borrowedBinary, root))
    assertThrows(classOf[IllegalArgumentException], () => facts.copy(tag = facts.tag.copy(operationId = new UUID(0, 0))))
  }

  @Test def strictRemovalRejectsWrongDomainEveryTruncationTailOverflowAndInvalidEnd(): Unit =
    val bytes = raw(false)
    for size <- 0 until bytes.length do rejectPhysical(bytes.take(size), false)
    rejectPhysical(bytes :+ 0.toByte, false)
    val domain = bytes.clone(); domain(4) = 'X'.toByte; rejectPhysical(domain, false)
    for size <- Vector(-1, Int.MaxValue) do
      val changed = bytes.clone(); java.nio.ByteBuffer.wrap(changed).putInt(size); rejectPhysical(changed, false)
    val ordinal = bytes.clone()
    java.nio.ByteBuffer.wrap(ordinal).putLong(ordinal.length - 8, facts.requestedEnd)
    rejectPhysical(ordinal, false)

  @Test def strictTicketRejectsNestedMismatchUnsortedFootprintAndNonexactResults(): Unit =
    val bytes = raw(true)
    val nestedLengthOffset = 4 + MySqlNormalRecallCanonical.TicketDomain.length
    val nestedSize = java.nio.ByteBuffer.wrap(bytes).getInt(nestedLengthOffset)
    val nestedStart = nestedLengthOffset + 4
    val hashStart = nestedStart + nestedSize
    val countOffset = hashStart + 32
    for size <- Vector(-1, Int.MaxValue, nestedSize - 1, nestedSize + 1) do
      val changed = bytes.clone(); java.nio.ByteBuffer.wrap(changed).putInt(nestedLengthOffset, size)
      rejectPhysical(changed, true)
    val digest = bytes.clone(); digest(hashStart) = (digest(hashStart) ^ 1).toByte; rejectPhysical(digest, true)
    val domain = bytes.clone(); domain(nestedStart + 4) = 'X'.toByte; rejectPhysical(domain, true)
    val unordered = bytes.clone()
    System.arraycopy(bytes, countOffset + 4 + 12, unordered, countOffset + 4, 12)
    System.arraycopy(bytes, countOffset + 4, unordered, countOffset + 4 + 12, 12)
    rejectPhysical(unordered, true)
    val count = bytes.clone(); java.nio.ByteBuffer.wrap(count).putInt(countOffset, 24); rejectPhysical(count, true)
    val identity = bytes.clone(); identity(countOffset + 4 + 300 + 15) = 99.toByte; rejectPhysical(identity, true)
    for index <- bytes.length - 3 until bytes.length do
      val result = bytes.clone(); result(index) = 0; rejectPhysical(result, true)
    for size <- 0 until bytes.length do rejectPhysical(bytes.take(size), true)
    rejectPhysical(bytes :+ 0.toByte, true)

  @Test def strictPhysicalDecodeSharesOriginalBudgetAndRejectsClosedRoot(): Unit =
    // 两个存活结果加第三次的临时峰值必须在同一ledger计入；此值仅为fixture。
    val p = profile(30000L)
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    val input = MySqlFormat3Binary.takeOwned(raw(true))
    try
      val before = root.accountedMemoryBytes
      MySqlNormalRecallCanonical.decodeTicket(input, root)
      assertTrue(root.accountedMemoryBytes >= before + 8192L)
      MySqlNormalRecallCanonical.decodeTicket(input, root)
      assertThrows(classOf[java.sql.SQLDataException], () => MySqlNormalRecallCanonical.decodeTicket(input, root))
    finally root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallCanonical.decodeTicket(input, root))

  @Test def removalWirePreservesOriginalOperationAndActualSubsequentEnd(): Unit =
    val p = profile()
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    try
      val result = MySqlNormalRecallCanonical.removal(facts, root)
      val input = new DataInputStream(new ByteArrayInputStream(result.copyBytes(root, "test exact recall wire")))
      val token = new Array[Byte](input.readInt())
      input.readFully(token)
      assertEquals(MySqlNormalRecallCanonical.RemovalDomain, new String(token, java.nio.charset.StandardCharsets.US_ASCII))
      def readUuid(): UUID = new UUID(input.readLong(), input.readLong())
      assertEquals(id(1), readUuid()); assertEquals(2L, input.readLong()); assertEquals(3L, input.readLong())
      assertEquals(id(4), readUuid()); assertEquals(id(5), readUuid()); assertEquals(id(6), readUuid())
      assertEquals(7L, input.readLong()); assertEquals(-1, input.readInt()); assertEquals(9, input.readInt())
      assertEquals(10L, input.readLong()); assertEquals(12L, input.readLong()); assertEquals(13L, input.readLong())
      assertEquals(-1, input.read())
      assertFalse(result.exactlyEquals(MySqlNormalRecallCanonical.removal(facts.copy(observedEnd = 14L), root)))
      assertThrows(classOf[IllegalArgumentException], () => facts.copy(observedEnd = 12L))
    finally root.closeRootReadBudget()

  @Test def ticketUsesCanonicalExact25AndCannotReplaceOriginalRemoval(): Unit =
    val p = profile()
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    try
      val original = MySqlNormalRecallTicketFacts(facts, id(8), chunks)
      val first = MySqlNormalRecallCanonical.ticket(original, root)
      assertTrue(first.exactlyEquals(MySqlNormalRecallCanonical.ticket(original.copy(footprint = chunks.reverse), root)))
      assertFalse(first.exactlyEquals(MySqlNormalRecallCanonical.ticket(original.copy(removal = facts.copy(entityId = 10)), root)))
      assertThrows(classOf[IllegalArgumentException], () => original.copy(footprint = chunks.drop(1)))
      assertThrows(classOf[IllegalArgumentException], () => original.copy(footprint = chunks.updated(0, chunks(1))))
    finally root.closeRootReadBudget()

  @Test def repeatedPayloadsShareOriginalBudgetAndClosedRootCannotEncode(): Unit =
    val p = profile(4096L)
    val root = new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
    try
      val first = MySqlNormalRecallCanonical.removal(facts, root)
      assertTrue(root.accountedMemoryBytes >= first.length)
      var exhausted = false
      var count = 0
      while !exhausted && count < 100 do
        try
          MySqlNormalRecallCanonical.removal(facts, root)
          count += 1
        catch case _: java.sql.SQLDataException => exhausted = true
      assertTrue(exhausted, "each call must consume the same cumulative root ledger")
    finally root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException], () => MySqlNormalRecallCanonical.removal(facts, root))

  @Test def markerIsRestrictedToExactCurrentReceiptTargetContextCells(): Unit =
    def uuidCell(n: Long): MySqlFormat3Cell =
      val bytes = java.nio.ByteBuffer.allocate(16).putLong(0L).putLong(n).array()
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
    val table = "fp_lifecycle_completion"
    val column = "original_context_bytes"
    val sqlType = MySqlFormat3RelationShapes.forTable(table).columns.find(_.name == column).get.sqlDeclaration
    val output = new DataOutputStream(new ByteArrayOutputStream)
    def write(image: MySqlOperationEffectImage, key: Vector[MySqlFormat3Cell], col: String, value: MySqlFormat3Cell): Unit =
      MySqlNormalRecallCanonical.writeDerivedCell(output, MySqlOperationEffectPhase.Execute, image,
        table, key, col, sqlType, value, id(1), id(2), id(3))
    val key = Vector(uuidCell(1), uuidCell(2))
    write(MySqlOperationEffectImage.Target, key, column, uuidCell(9))
    assertThrows(classOf[IllegalArgumentException], () => write(MySqlOperationEffectImage.Before, key, column, uuidCell(9)))
    assertThrows(classOf[IllegalArgumentException], () => write(MySqlOperationEffectImage.Target, Vector(uuidCell(1), uuidCell(4)), column, uuidCell(9)))
    assertThrows(classOf[IllegalArgumentException], () => write(MySqlOperationEffectImage.Target, key, "removal_result_bytes", uuidCell(9)))
    assertThrows(classOf[IllegalArgumentException], () => write(MySqlOperationEffectImage.Target, key, column, MySqlFormat3Cell.SqlNull))

  @Test def unknownCheckpointNeverChoosesActiveRecovery(): Unit =
    assertEquals(None, MySqlNormalRecallRecoveryTarget.purpose(MySqlNormalRecallCheckpointDecision.Unknown))
    assertEquals(Some(MySqlNormalRecallPurpose.RestoreCommittedActive),
      MySqlNormalRecallRecoveryTarget.purpose(MySqlNormalRecallCheckpointDecision.DefinitivelyNotCommitted))
    assertEquals(Some(MySqlNormalRecallPurpose.FinishCommittedInactive),
      MySqlNormalRecallRecoveryTarget.purpose(MySqlNormalRecallCheckpointDecision.Committed))
