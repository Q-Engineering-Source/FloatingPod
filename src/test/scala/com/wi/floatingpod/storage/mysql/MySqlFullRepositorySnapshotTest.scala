package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{
  ObservedUnboundIdentity,
  ObservedUnboundSourceKind,
  PhysicalRecoveryBinding,
  PhysicalRecoveryChunk,
  PhysicalRecoveryCutoverState,
  PhysicalRecoveryLocator,
  PhysicalRecoveryLocatorReason,
  PhysicalRecoveryLocatorState,
  UnboundIntentOrigin
}

import java.security.MessageDigest
import java.util.UUID
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.Test

final class MySqlFullRepositorySnapshotTest:
  private val podId = UUID.fromString("10000000-0000-0000-0000-000000000001")
  private val bindingId = UUID.fromString("20000000-0000-0000-0000-000000000002")
  private val attemptId = UUID.fromString("30000000-0000-0000-0000-000000000003")
  private val catalogId = UUID.fromString("40000000-0000-0000-0000-000000000004")

  @Test
  def canonicalSnapshotRetainsRawFloatingPointBitsAndNulls(): Unit =
    val base = MySqlFullRepositorySnapshot(Vector(row()), Vector.empty, emptyGraph, Vector.empty)
    val signedZero = MySqlFullRepositorySnapshot(Vector(row().copy(posX = -0.0d)), Vector.empty, emptyGraph, Vector.empty)
    assertDoesNotThrow((() => base.validate()): Executable)
    assertDoesNotThrow((() => signedZero.validate()): Executable)
    assertNotEquals(base.canonicalSha256, signedZero.canonicalSha256)
    assertTrue(!base.exactlyEquals(signedZero))
    assertTrue(base.exactlyEquals(MySqlFullRepositorySnapshot(Vector(row()), Vector.empty, emptyGraph, Vector.empty)))

  @Test
  def activeUnclaimedRequiresWorldIdleInCompleteDomainSnapshot(): Unit =
    val invalid = MySqlFullRepositorySnapshot(
      Vector(row(active = 1, mode = "NORMAL")),
      Vector.empty,
      emptyGraph,
      Vector.empty
    )
    assertThrows(classOf[IllegalStateException], () => invalid.validate())

    val valid = MySqlFullRepositorySnapshot(
      Vector(row(active = 1, mode = "WORLD_IDLE")),
      Vector.empty,
      emptyGraph,
      Vector.empty
    )
    assertDoesNotThrow((() => valid.validate()): Executable)

  @Test
  def rowActiveLocatorMustMatchTheExactDomainVersionAndChunkFootprint(): Unit =
    val active = row(active = 1, mode = "WORLD_IDLE", version = 4L, dimension = Some(7))
    val chunks = square(7, 0, 0)
    val rawChunks = chunks.map(chunk => MySqlRecoveryChunkRow(podId, chunk))
    val locator = PhysicalRecoveryLocator(
      podId,
      attemptId,
      PhysicalRecoveryLocatorState.RowActive,
      PhysicalRecoveryLocatorReason.RowActive,
      Some(4L),
      Some(7),
      chunks.toList
    )
    val graph = MySqlRecoveryRows.assembleGraph(
      Some(PhysicalRecoveryBinding(bindingId, PhysicalRecoveryCutoverState.Certified, 2)),
      Vector(locator),
      rawChunks,
      Vector.empty
    )
    assertDoesNotThrow((() => MySqlFullRepositorySnapshot(Vector(active), Vector.empty, graph, Vector.empty).validate()): Executable)
    val staleLocator = locator.copy(domainVersion = Some(3L))
    val staleGraph = MySqlRecoveryRows.assembleGraph(graph.binding, Vector(staleLocator), rawChunks, Vector.empty)
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlFullRepositorySnapshot(Vector(active), Vector.empty, staleGraph, Vector.empty).validate()
    )

  @Test
  def preparedUnboundIntentMustMatchOwnerlessInactiveWorldIdleDomainAndExactPhysicalObligation(): Unit =
    val binding = PhysicalRecoveryBinding(bindingId, PhysicalRecoveryCutoverState.Certified, 2)
    val prepared = row(active = 0, mode = "WORLD_IDLE", version = 0L, dimension = Some(7))
    val chunks = square(7, 0, 0)
    val rawChunks = chunks.map(chunk => MySqlRecoveryChunkRow(podId, chunk))
    val locator = PhysicalRecoveryLocator(
      podId,
      attemptId,
      PhysicalRecoveryLocatorState.UnboundAdoptionPrepared,
      PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared,
      Some(0L),
      Some(7),
      chunks.toList
    )
    val source = ObservedUnboundIdentity(ObservedUnboundSourceKind.LegacyUnregistered, catalogId, "stable-source-identity")
    val sourceHash = MessageDigest.getInstance("SHA-256").digest("stable-source-identity".getBytes(java.nio.charset.StandardCharsets.UTF_8)).toVector
    val intent = MySqlRecoveryIntentData(
      podId,
      attemptId,
      bindingId,
      UnboundIntentOrigin.ObservedArtifactAdoption,
      MySqlRecoveryIntentPhase.Prepared,
      Some(ObservedUnboundSourceKind.LegacyUnregistered),
      Some(source.sourceIdentity),
      Some(sourceHash),
      Some(catalogId),
      None,
      0L,
      "world",
      8.0d,
      64.0d,
      8.0d,
      0.0d,
      0.0d,
      7,
      0,
      0,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    val graph = MySqlRecoveryRows.assembleGraph(Some(binding), Vector(locator), rawChunks, Vector(intent))
    assertDoesNotThrow((() => MySqlFullRepositorySnapshot(Vector(prepared), Vector.empty, graph, Vector.empty).validate()): Executable)

    val ownerModeViolation = prepared.copy(operatingModeToken = "NORMAL")
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlFullRepositorySnapshot(Vector(ownerModeViolation), Vector.empty, graph, Vector.empty).validate()
    )
    val missingDomain = MySqlRecoveryRows.assembleGraph(Some(binding), Vector(locator), rawChunks, Vector(intent))
    assertThrows(
      classOf[IllegalStateException],
      () => MySqlFullRepositorySnapshot(Vector.empty, Vector.empty, missingDomain, Vector.empty).validate()
    )

  private def row(
      active: Int = 0,
      mode: String = "NORMAL",
      version: Long = 0L,
      dimension: Option[Int] = None
  ): MySqlStoredRowData =
    MySqlStoredRowData(
      MySqlStoredRows.encodeUuid(podId),
      None,
      version,
      "world",
      8.0d,
      64.0d,
      8.0d,
      0.0d,
      0.0d,
      active,
      10L,
      dimension,
      mode
    )

  private def emptyGraph: MySqlRecoveryGraph =
    MySqlRecoveryGraph(None, Vector.empty, Vector.empty, Vector.empty)

  private def square(dimension: Int, centerX: Int, centerZ: Int): Vector[PhysicalRecoveryChunk] =
    (for x <- centerX - 2 to centerX + 2; z <- centerZ - 2 to centerZ + 2 yield
      PhysicalRecoveryChunk(dimension, x, z)).toVector
