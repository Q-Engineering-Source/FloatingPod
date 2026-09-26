package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState}

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable.ListBuffer

class ExistingWorldInventoryTest:
  @Test
  def completeExactLocatorPreservesUnknownAndDuplicateFloatingPodObservations(): Unit =
    val podUuid = uuid(1)
    val unknownUuid = uuid(2)
    val chunks = square(7, 10, -5)
    val observations = List(
      ExistingWorldInventoryObservation(podUuid, 7, 10, -5, 41),
      ExistingWorldInventoryObservation(unknownUuid, 7, 11, -5, 42),
      ExistingWorldInventoryObservation(unknownUuid, 7, 11, -4, 43)
    )
    val probe = new FakeProbe(chunks.toSet, List(
      ExistingWorldInventoryLoadedChunk(7, 10, -5, observations.take(1)),
      ExistingWorldInventoryLoadedChunk(7, 11, -5, observations.drop(1).take(1)),
      ExistingWorldInventoryLoadedChunk(7, 11, -4, observations.drop(2))
    ))

    val report = new ExistingWorldInventory(probe).inspect(List(locator(podUuid, chunks))).get

    assertEquals(List(ExistingWorldInventoryRequiredChunks(podUuid, chunks, chunks)), report.requiredChunks)
    assertTrue(report.requiredChunks.head.complete)
    assertEquals(observations, report.observations)
    assertEquals(chunks, probe.loadAttempts.toList)

  @Test
  def missingGeneratedLocatorChunkStaysIncompleteWithoutAnAbsenceClaim(): Unit =
    val podUuid = uuid(3)
    val chunks = square(7, -3, 8)
    val probe = new FakeProbe(chunks.drop(1).toSet, Nil)

    val report = new ExistingWorldInventory(probe).inspect(List(locator(podUuid, chunks))).get

    assertEquals(List(ExistingWorldInventoryRequiredChunks(podUuid, chunks, chunks.drop(1))), report.requiredChunks)
    assertTrue(!report.requiredChunks.head.complete)
    assertTrue(report.observations.isEmpty)
    assertEquals(chunks, probe.loadAttempts.toList)

  @Test
  def malformedLocatorIsRejectedBeforeAnyProbeCall(): Unit =
    val podUuid = uuid(4)
    val probe = new FakeProbe(Set.empty, Nil)

    val result = new ExistingWorldInventory(probe).inspect(List(locator(podUuid, square(7, 0, 0).drop(1))))

    assertTrue(result.isFailure)
    assertTrue(probe.loadAttempts.isEmpty)

  @Test
  def probeFailureRetainsTheOriginalCause(): Unit =
    val podUuid = uuid(5)
    val failure = new IllegalStateException("generated chunk probe failed")
    val probe = new FakeProbe(square(7, 4, 4).toSet, Nil, Some(failure))

    val result = new ExistingWorldInventory(probe).inspect(List(locator(podUuid, square(7, 4, 4))))

    assertTrue(result.isFailure)
    assertSame(failure, result.failed.get)

  @Test
  def supplierFactoryDefersServerResolutionAndFailsBeforeAnyGenerationWhenUnavailable(): Unit =
    val chunks = square(7, 4, 4)
    var supplierCalls = 0
    val inventory = ExistingWorldInventory.withServerSupplier(() => { supplierCalls += 1; null })

    assertEquals(0, supplierCalls)
    val result = inventory.inspect(List(locator(uuid(6), chunks)))

    assertTrue(result.isFailure)
    assertEquals(1, supplierCalls)

  private def uuid(value: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$value%012d")
  private def square(dimensionId: Int, startX: Int, startZ: Int): List[PhysicalRecoveryChunk] =
    (for
      chunkX <- startX to startX + 4
      chunkZ <- startZ to startZ + 4
    yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toList
  private def locator(podUuid: UUID, chunks: List[PhysicalRecoveryChunk]): PhysicalRecoveryLocator =
    PhysicalRecoveryLocator(podUuid, uuid(100 + podUuid.variant()), PhysicalRecoveryLocatorState.RowInactiveVerified, "test_locator", Some(1L), Some(7), chunks)

  private final class FakeProbe(
      generatedChunks: Set[PhysicalRecoveryChunk],
      snapshot: List[ExistingWorldInventoryLoadedChunk],
      failure: Option[Throwable] = None
  ) extends ExistingWorldInventoryProbe:
    val loadAttempts = ListBuffer.empty[PhysicalRecoveryChunk]

    override def loadIfAlreadyGenerated(chunk: PhysicalRecoveryChunk): Boolean =
      loadAttempts += chunk
      failure.foreach(throw _)
      generatedChunks.contains(chunk)
    override def loadedChunks(): List[ExistingWorldInventoryLoadedChunk] =
      failure.foreach(throw _)
      snapshot
