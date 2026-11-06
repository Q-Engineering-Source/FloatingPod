package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryChunk}
import com.wi.floatingpod.domain.PlayerEntitlement
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.{SQLDataException, Types}
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}

class MySqlRecoverySnapshotTest:
  private val id = new UUID(0L, 0L)
  private val other = new UUID(1L, 2L)
  private val hash = Vector.tabulate[Byte](32)(_.toByte)
  private val now = LocalDateTime.of(2026, 10, 7, 12, 0)
  private val writer = MySqlSnapshotWriter(1, id, 7L, Some(other), now.plusSeconds(5), Some(id), Some(hash))
  private val manifest = MySqlSnapshotManifest(1, "MYSQL", 2, hash, id, "BOOTSTRAPPING", id, id,
    None, "NONE", None, None, None, None)

  @Test def bootWithSameIdentitiesAndNullGuardRejectsDifferentAuditorBaseline(): Unit =
    val readerManifest = manifest
    val auditorManifest = manifest.copy()
    assertEquals(readerManifest, auditorManifest)
    assertEquals("BOOTSTRAPPING", readerManifest.bootstrapState)
    assertEquals(None, readerManifest.guardManifestSha256)
    val readerBaselineA = "a" * 64
    val auditorBaselineB = "b" * 64
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.requireReaderStaticBaseline(auditorBaselineB, readerBaselineA))

  @Test def bootWithNullGuardAcceptsMatchingReaderAndAuditorStaticBaseline(): Unit =
    assertEquals("BOOTSTRAPPING", manifest.bootstrapState)
    assertEquals(None, manifest.guardManifestSha256)
    val readerBaseline = "a" * 64
    MySqlRecoverySnapshotRules.requireReaderStaticBaseline(readerBaseline, readerBaseline)

  private def manifestRow(): CachedRowSet =
    val types = Vector(Types.TINYINT, Types.VARCHAR, Types.INTEGER, Types.VARBINARY, Types.VARBINARY,
      Types.VARCHAR, Types.VARBINARY, Types.VARBINARY, Types.VARBINARY, Types.VARCHAR,
      Types.VARBINARY, Types.VARBINARY, Types.VARBINARY, Types.VARBINARY)
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(types.size)
    types.zipWithIndex.foreach { (kind, index) =>
      metadata.setColumnType(index + 1, kind)
      metadata.setColumnName(index + 1, s"c${index + 1}")
    }
    val row = RowSetProvider.newFactory().createCachedRowSet()
    row.setMetaData(metadata)
    row.moveToInsertRow()
    row.updateInt(1, 1)
    row.updateString(2, "MYSQL")
    row.updateInt(3, 2)
    row.updateBytes(4, hash.toArray)
    Vector(5, 7, 8).foreach(index => row.updateBytes(index, MySqlStoredRows.encodeUuid(id).toArray))
    row.updateString(6, "BOOTSTRAPPING")
    row.updateNull(9)
    row.updateString(10, "NONE")
    Vector(11, 12, 13, 14).foreach(row.updateNull)
    row.insertRow()
    row.moveToCurrentRow()
    row.beforeFirst()
    assertTrue(row.next())
    row

  @Test def manifestDecoderPreservesAllRawColumnsAndNulls(): Unit =
    val row = manifestRow()
    try
      assertEquals(manifest, MySqlRecoverySnapshotRules.decodeManifest(row))
      row.updateString(10, "PENDING")
      row.updateBytes(9, hash.toArray)
      row.updateBytes(11, MySqlStoredRows.encodeUuid(other).toArray)
      row.updateBytes(12, hash.reverse.toArray)
      row.updateBytes(14, hash.toArray)
      assertEquals(manifest.copy(importState = "PENDING", importManifestSha256 = Some(hash),
        importOperationUuid = Some(other), importSnapshotSha256 = Some(hash.reverse),
        guardManifestSha256 = Some(hash)), MySqlRecoverySnapshotRules.decodeManifest(row))
    finally row.close()

  @Test def manifestDecoderRejectsNullAndMalformedBinaryAtResponsibilityBoundary(): Unit =
    Vector(1, 2, 3, 4, 5, 6, 7, 8, 10).foreach { index =>
      val row = manifestRow()
      try
        row.updateNull(index)
        assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlRecoverySnapshotRules.decodeManifest(row))
      finally row.close()
    }
    Vector(4, 5, 7, 8, 9, 11, 12, 13, 14).foreach { index =>
      val row = manifestRow()
      try
        row.updateBytes(index, Array[Byte](1))
        assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => MySqlRecoverySnapshotRules.decodeManifest(row))
      finally row.close()
    }

  @Test def beforeAfterChecksEntireWriterTupleIncludingReceiptAndLease(): Unit =
    val changes = Vector(writer.copy(bindingUuid = other), writer.copy(epoch = 8), writer.copy(ownerBootUuid = None),
      writer.copy(leaseUntil = now), writer.copy(lastOperationUuid = Some(other)), writer.copy(lastFingerprint = Some(hash.reverse)),
      writer.copy(singleton = 2))
    changes.foreach { changed =>
      assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateStableControls(manifest, manifest, None, None,
        writer, changed, now, now.plusSeconds(1)))
    }
    MySqlRecoverySnapshotRules.validateStableControls(manifest, manifest, None, None, writer, writer, now, now)

  @Test def stableControlsRejectChangedManifestBindingAndBackwardsDatabaseTime(): Unit =
    val binding = Some(PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.LegacyUncertified, 2))
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateStableControls(manifest,
      manifest.copy(provisioningUuid = other), binding, binding, writer, writer, now, now))
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateStableControls(manifest,
      manifest, None, binding, writer, writer, now, now))
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateStableControls(manifest,
      manifest, None, None, writer, writer, now, now.minusNanos(1000)))

  @Test def bootstrapAbsentLegacyAndExpiredWriterRemainObservations(): Unit =
    assertEquals(MySqlRecoveryObservationState.Bootstrapping, MySqlRecoverySnapshotRules.observationState(manifest, None))
    val ready = manifest.copy(bootstrapState = "READY")
    assertEquals(MySqlRecoveryObservationState.BindingAbsent, MySqlRecoverySnapshotRules.observationState(ready, None))
    assertEquals(MySqlRecoveryObservationState.LegacyBinding, MySqlRecoverySnapshotRules.observationState(ready,
      Some(PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.LegacyUncertified, 2))))
    assertEquals(MySqlObservedWriterLease.OwnedUnexpired, MySqlRecoverySnapshotRules.writerLease(writer, now))
    assertEquals(MySqlObservedWriterLease.OwnedExpired, MySqlRecoverySnapshotRules.writerLease(writer, writer.leaseUntil))
    assertEquals(MySqlObservedWriterLease.Unowned, MySqlRecoverySnapshotRules.writerLease(writer.copy(ownerBootUuid = None), now))

  @Test def independentInventoryRetainsOrphansAndFailsExplicitGraphValidation(): Unit =
    val orphan = MySqlRecoveryChunkRow(id, PhysicalRecoveryChunk(0, -1, 1))
    val graph = MySqlRecoveryRows.assembleGraph(None, Vector.empty, Vector(orphan), Vector.empty)
    assertEquals(Vector(orphan), graph.chunks)
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateInventory(Vector.empty,
      Vector.empty, graph, Vector.empty))
    MySqlRecoverySnapshotRules.validateInventory(Vector.empty, Vector.empty,
      MySqlRecoveryRows.assembleGraph(None, Vector.empty, Vector.empty, Vector.empty), Vector.empty)

  @Test def inventoryChecksBusinessPrimaryKeysWithoutActiveFiltering(): Unit =
    val pod = MySqlStoredRowData(MySqlStoredRows.encodeUuid(id), None, 0L, " raw world ",
      1D, 2D, 3D, 4D, 5D, 0, 0L, None, "NORMAL")
    val empty = MySqlRecoveryRows.assembleGraph(None, Vector.empty, Vector.empty, Vector.empty)
    MySqlRecoverySnapshotRules.validateInventory(Vector(pod), Vector(PlayerEntitlement(id)), empty, Vector.empty)
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateInventory(Vector(pod, pod), Vector.empty, empty, Vector.empty))
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateInventory(Vector.empty,
      Vector(PlayerEntitlement(id), PlayerEntitlement(id)), empty, Vector.empty))
    assertThrows(classOf[SQLDataException], () => MySqlRecoverySnapshotRules.validateInventory(Vector(pod.copy(isActiveFlag = 2)), Vector.empty, empty, Vector.empty))

  @Test def uncertainCleanupExceptionKeepsOriginalCauseAndSuppressedDiagnostics(): Unit =
    val cause = new IllegalStateException("primary")
    val cleanup = new IllegalStateException("rollback failed")
    MySqlSchemaAuditor.addSuppressed(cause, cleanup)
    val failure = new MySqlSnapshotCleanupUncertainException(cause)
    assertSame(cause, failure.getCause)
    assertSame(cleanup, failure.getCause.getSuppressed.head)
    assertTrue(failure.getCause.getStackTrace.nonEmpty)
