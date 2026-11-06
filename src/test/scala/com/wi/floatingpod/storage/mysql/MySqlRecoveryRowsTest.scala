package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{ObservedUnboundSourceKind, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, UnboundIntentOrigin}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.sql.{SQLDataException, Types}
import java.util.UUID
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}

class MySqlRecoveryRowsTest:
  private val pod = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")
  private val attempt = UUID.fromString("ffeeddcc-bbaa-9988-7766-554433221100")
  private val binding = PhysicalRecoveryBinding(new UUID(0L, 0L), PhysicalRecoveryCutoverState.Certified, 2)

  @Test
  def bindingKeepsZeroUuidAndRejectsWrongSingleton(): Unit =
    val row = rows(Vector(Types.TINYINT, Types.VARBINARY, Types.VARCHAR, Types.INTEGER), Vector(1, bytes(binding.bindingUuid), "CERTIFIED", 2))
    try
      assertEquals(binding, MySqlRecoveryRows.readBinding(row))
      row.updateInt(1, 2)
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryRows.readBinding(row))
    finally row.close()

  @Test
  def locatorPreservesNullVersionMinimumDimensionAndUntrimmedReason(): Unit =
    val row = rows(Vector(Types.VARBINARY, Types.VARBINARY, Types.VARCHAR, Types.LONGVARCHAR, Types.BIGINT, Types.INTEGER),
      Vector(bytes(pod), bytes(attempt), "QUARANTINED", " reason 原文 ", null, Int.MinValue))
    try
      val locator = MySqlRecoveryRows.readLocator(row)
      assertEquals(None, locator.domainVersion)
      assertEquals(Some(Int.MinValue), locator.targetDimensionId)
      assertEquals(" reason 原文 ", locator.reasonCode)
      assertEquals(PhysicalRecoveryLocatorState.Quarantined, locator.state)
      assertEquals(Nil, locator.chunks)
    finally row.close()

  @Test
  def chunkKeepsParentAndSignedCoordinates(): Unit =
    val row = rows(Vector(Types.VARBINARY, Types.INTEGER, Types.INTEGER, Types.INTEGER), Vector(bytes(pod), Int.MinValue, -4096, Int.MaxValue))
    try assertEquals(MySqlRecoveryChunkRow(pod, PhysicalRecoveryChunk(Int.MinValue, -4096, Int.MaxValue)), MySqlRecoveryRows.readChunk(row))
    finally row.close()

  @Test
  def preparedIntentReadsAllHistoryColumnsWithoutMaterializingPublicPose(): Unit =
    val row = intentRow(committed = false)
    try
      val intent = MySqlRecoveryRows.readIntent(row)
      assertEquals(UnboundIntentOrigin.DebugItemCreate, intent.origin)
      assertEquals(MySqlRecoveryIntentPhase.Prepared, intent.phase)
      assertEquals(None, intent.sourceKind)
      assertEquals(None, intent.sourceIdentitySha256)
      assertEquals(Some(attempt), intent.createOperationUuid)
      assertEquals(" world 原文 ", intent.targetWorldName)
      assertEquals(0.10000000000000002D, intent.targetPosX)
      assertEquals(0.1D, intent.targetYaw)
      assertEquals(-7, intent.targetChunkX)
      assertEquals(None, intent.committedAt)
      assertEquals(None, intent.committedDimensionId)
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent)).validateGraph()
    finally row.close()

  @Test
  def committedIntentRetainsExactVersionTimestampAndPose(): Unit =
    val row = intentRow(committed = true)
    try
      val intent = MySqlRecoveryRows.readIntent(row)
      assertEquals(MySqlRecoveryIntentPhase.Committed, intent.phase)
      assertEquals(Some(1L), intent.committedDomainVersion)
      assertEquals(Some(Long.MaxValue), intent.committedAt)
      assertEquals(Some(0.10000000000000002D), intent.committedPosX)
      assertEquals(Some(0.1D), intent.committedYaw)
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent)).validateGraph()
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent.copy(committedDimensionId = None))).validateGraph())
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent.copy(committedPosX = Some(2D)))).validateGraph())
    finally row.close()

  @Test
  def adoptionReadsSourceDigestAndCatalogWithoutTrimming(): Unit =
    val row = intentRow(committed = false)
    try
      row.updateString(4, "UNBOUND_ADOPTION")
      row.updateString(6, "LEGACY_UNREGISTERED")
      row.updateString(7, " source 原文 ")
      row.updateBytes(8, Array.tabulate[Byte](32)(_.toByte))
      row.updateBytes(9, bytes(new UUID(0L, 0L)))
      row.updateNull(10)
      val intent = MySqlRecoveryRows.readIntent(row)
      assertEquals(Some(" source 原文 "), intent.sourceIdentity)
      assertEquals(Some(Vector.tabulate[Byte](32)(_.toByte)), intent.sourceIdentitySha256)
      assertEquals(Some(new UUID(0L, 0L)), intent.catalogGeneration)
      assertEquals(None, intent.createOperationUuid)
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent)).validateGraph()
    finally row.close()

  @Test
  def graphKeepsOrphanThenExplicitlyRejectsItAndDuplicateChunks(): Unit =
    val orphan = MySqlRecoveryChunkRow(pod, PhysicalRecoveryChunk(0, 1, 2))
    val graph = MySqlRecoveryRows.assembleGraph(Some(binding), Vector.empty, Vector(orphan), Vector.empty)
    assertEquals(Vector(orphan), graph.chunks)
    assertThrows(classOf[SQLDataException], () => graph.validateGraph())
    val locator = PhysicalRecoveryLocator(pod, attempt, PhysicalRecoveryLocatorState.RowActive, "row_active", Some(1L), Some(0), Nil)
    val valid = MySqlRecoveryRows.assembleGraph(Some(binding), Vector(locator), Vector(orphan), Vector.empty)
    valid.validateGraph()
    assertEquals(List(orphan.chunk), valid.locators.head.chunks)
    assertThrows(classOf[SQLDataException], () => MySqlRecoveryRows.assembleGraph(Some(binding), Vector(locator), Vector(orphan, orphan), Vector.empty).validateGraph())

  @Test
  def graphRejectsPreparedCommittedFieldsAndBindingMismatch(): Unit =
    val row = intentRow(committed = false)
    try
      val intent = MySqlRecoveryRows.readIntent(row)
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(None, Vector.empty, Vector.empty, Vector(intent)).validateGraph())
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(intent.copy(committedAt = Some(0L)))).validateGraph())
    finally row.close()

  @Test
  def malformedRequiredUuidAndUnknownEnumFailAtDecode(): Unit =
    val row = intentRow(committed = false)
    try
      row.updateNull(1)
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryRows.readIntent(row))
      row.updateBytes(1, Array.fill[Byte](15)(0))
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryRows.readIntent(row))
      row.updateBytes(1, bytes(pod))
      row.updateString(5, "UNKNOWN")
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryRows.readIntent(row))
    finally row.close()

  @Test
  def graphRejectsSameCreateOperationAcrossDifferentPodsAndAttempts(): Unit =
    val row = intentRow(committed = false)
    try
      val first = MySqlRecoveryRows.readIntent(row)
      val second = first.copy(podUuid = new UUID(7L, 1L), attemptUuid = new UUID(7L, 2L))
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(first, second)).validateGraph())
    finally row.close()

  @Test
  def graphRejectsSameObservedSourceCompositeKeyAcrossDifferentPods(): Unit =
    val row = intentRow(committed = false)
    try
      val first = asAdoption(MySqlRecoveryRows.readIntent(row))
      val second = first.copy(podUuid = new UUID(8L, 1L), attemptUuid = new UUID(8L, 2L))
      assertThrows(classOf[SQLDataException], () => MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(first, second)).validateGraph())
    finally row.close()

  @Test
  def differentCreateKeysPermitRepeatedNullObservedSourceKeys(): Unit =
    val row = intentRow(committed = false)
    try
      val first = MySqlRecoveryRows.readIntent(row)
      val second = first.copy(podUuid = new UUID(9L, 1L), attemptUuid = new UUID(9L, 2L), createOperationUuid = Some(new UUID(9L, 3L)))
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(first, second)).validateGraph()
      assertEquals(None, first.sourceKind)
      assertEquals(None, second.catalogGeneration)
      assertEquals(None, second.sourceIdentitySha256)
    finally row.close()

  @Test
  def differentObservedSourceKeysPermitRepeatedNullCreateOperation(): Unit =
    val row = intentRow(committed = false)
    try
      val first = asAdoption(MySqlRecoveryRows.readIntent(row))
      val second = first.copy(podUuid = new UUID(10L, 1L), attemptUuid = new UUID(10L, 2L),
        sourceIdentity = Some(" second source "), sourceIdentitySha256 = Some(Vector.fill[Byte](32)(2)))
      MySqlRecoveryGraph(Some(binding), Vector.empty, Vector.empty, Vector(first, second)).validateGraph()
      assertEquals(None, first.createOperationUuid)
      assertEquals(None, second.createOperationUuid)
    finally row.close()

  private def asAdoption(intent: MySqlRecoveryIntentData): MySqlRecoveryIntentData =
    intent.copy(origin = UnboundIntentOrigin.ObservedArtifactAdoption,
      sourceKind = Some(ObservedUnboundSourceKind.FreshArtifact),
      sourceIdentity = Some(" source "), sourceIdentitySha256 = Some(Vector.fill[Byte](32)(1)),
      catalogGeneration = Some(new UUID(0L, 0L)), createOperationUuid = None)

  private def bytes(uuid: UUID): Array[Byte] = MySqlStoredRows.encodeUuid(uuid).toArray

  private def intentRow(committed: Boolean): CachedRowSet =
    val types = Vector(Types.VARBINARY, Types.VARBINARY, Types.VARBINARY, Types.VARCHAR, Types.VARCHAR,
      Types.VARCHAR, Types.LONGVARCHAR, Types.VARBINARY, Types.VARBINARY, Types.VARBINARY, Types.BIGINT,
      Types.LONGVARCHAR, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
      Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.BIGINT, Types.BIGINT, Types.LONGVARCHAR,
      Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.INTEGER)
    val prefix = Vector[Any](bytes(pod), bytes(attempt), bytes(binding.bindingUuid), "DEBUG_ITEM_CREATE",
      if committed then "COMMITTED" else "PREPARED", null, null, null, null, bytes(attempt), 0L,
      " world 原文 ", 0.10000000000000002D, -4D, 10D, 0.1D, -13.125D, Int.MinValue, -7, 9)
    val suffix = if committed then Vector[Any](1L, Long.MaxValue, " world 原文 ", 0.10000000000000002D, -4D, 10D, 0.1D, -13.125D, Int.MinValue) else Vector.fill[Any](9)(null)
    rows(types, prefix ++ suffix)

  private def rows(types: Vector[Int], values: Vector[Any]): CachedRowSet =
    assertEquals(types.size, values.size)
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(types.size)
    types.zipWithIndex.foreach { (kind, index) =>
      metadata.setColumnType(index + 1, kind)
      metadata.setColumnName(index + 1, s"column${index + 1}")
    }
    val row = RowSetProvider.newFactory().createCachedRowSet()
    row.setMetaData(metadata)
    row.moveToInsertRow()
    values.zipWithIndex.foreach { (value, index) =>
      val column = index + 1
      value match
        case null => row.updateNull(column)
        case bytes: Array[Byte] => row.updateBytes(column, bytes)
        case text: String => row.updateString(column, text)
        case value: Int => row.updateInt(column, value)
        case value: Long => row.updateLong(column, value)
        case value: Double => row.updateDouble(column, value)
        case _ => throw new IllegalArgumentException("Unsupported test JDBC value")
    }
    row.insertRow()
    row.moveToCurrentRow()
    row.beforeFirst()
    assertTrue(row.next())
    row
