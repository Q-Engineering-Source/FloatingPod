package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.UUID

/** 已认证 binding 的 canonical world marker 只读验证 tracer。 */
class WorldBindingTest:
  @Test
  def createsAndRetainsAnExactMarkerForTheSuppliedLegacyBinding(): Unit =
    val directory = Files.createTempDirectory("floatingpod-world-binding-create")
    val marker = directory.resolve("canonical.marker")
    val legacyBinding = PhysicalRecoveryBinding(
      UUID.fromString("00000000-0000-0000-0000-000000000892"),
      PhysicalRecoveryCutoverState.LegacyUncertified,
      1
    )

    assertEquals(marker, WorldBinding.createOrRetainMarker(legacyBinding, marker).get)
    assertArrayEquals(legacyExactMarker, Files.readAllBytes(marker))
    assertEquals(1L, regularChildCount(directory))
    val createdAt = Files.getLastModifiedTime(marker)

    assertEquals(marker, WorldBinding.createOrRetainMarker(legacyBinding, marker).get)
    assertArrayEquals(legacyExactMarker, Files.readAllBytes(marker))
    assertEquals(createdAt, Files.getLastModifiedTime(marker))
    assertEquals(1L, regularChildCount(directory))
    assertEquals(
      ValidatedWorldBinding(legacyBinding.copy(cutoverState = PhysicalRecoveryCutoverState.Certified), marker),
      WorldBinding.validate(legacyBinding.copy(cutoverState = PhysicalRecoveryCutoverState.Certified), marker).get
    )

  @Test
  def validatesExactCertifiedBindingMarkerWithoutMutatingTheMarker(): Unit =
    val marker = Files.createTempDirectory("floatingpod-world-binding").resolve("canonical.marker")
    val binding = PhysicalRecoveryBinding(
      UUID.fromString("00000000-0000-0000-0000-000000000891"),
      PhysicalRecoveryCutoverState.Certified,
      1
    )
    val exactMarker = Array[Byte](
      0x46, 0x50, 0x57, 0x42,
      0x00, 0x00, 0x00, 0x01,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x91.toByte
    )
    Files.write(marker, exactMarker)
    val originalModifiedAt = Files.getLastModifiedTime(marker)

    val result = WorldBinding.validate(binding, marker)

    assertEquals(ValidatedWorldBinding(binding, marker), result.get)
    assertArrayEquals(exactMarker, Files.readAllBytes(marker))
    assertEquals(originalModifiedAt, Files.getLastModifiedTime(marker))

  @Test
  def rejectsMissingTruncatedAndTrailingMarkers(): Unit =
    val directory = Files.createTempDirectory("floatingpod-world-binding-invalid-length")
    val binding = certifiedBinding()
    val marker = exactMarker

    assertTrue(WorldBinding.validate(binding, directory.resolve("missing.marker")).isFailure)
    val truncated = directory.resolve("truncated.marker")
    Files.write(truncated, marker.dropRight(1))
    assertTrue(WorldBinding.validate(binding, truncated).isFailure)
    val trailing = directory.resolve("trailing.marker")
    Files.write(trailing, marker :+ 0x00.toByte)
    assertTrue(WorldBinding.validate(binding, trailing).isFailure)

  @Test
  def rejectsNonRegularMarkerAndLegacyUncertifiedBinding(): Unit =
    val directory = Files.createTempDirectory("floatingpod-world-binding-ineligible")
    val binding = certifiedBinding()
    val regularMarker = directory.resolve("canonical.marker")
    Files.write(regularMarker, exactMarker)

    assertTrue(WorldBinding.validate(binding, directory).isFailure)
    assertTrue(WorldBinding.validate(binding.copy(cutoverState = PhysicalRecoveryCutoverState.LegacyUncertified), regularMarker).isFailure)

  @Test
  def rejectsMagicFormatVersionAndUuidThatDoNotExactlyMatchTheCertifiedBinding(): Unit =
    val directory = Files.createTempDirectory("floatingpod-world-binding-mismatch")
    val binding = certifiedBinding()

    val wrongMagic = exactMarker
    wrongMagic(0) = 0x00.toByte
    assertRejected(directory.resolve("wrong-magic.marker"), wrongMagic, binding)
    val wrongVersion = exactMarker
    wrongVersion(7) = 0x02.toByte
    assertRejected(directory.resolve("wrong-version.marker"), wrongVersion, binding)
    val wrongUuid = exactMarker
    wrongUuid(23) = 0x92.toByte
    assertRejected(directory.resolve("wrong-uuid.marker"), wrongUuid, binding)

  private def certifiedBinding(): PhysicalRecoveryBinding =
    PhysicalRecoveryBinding(
      UUID.fromString("00000000-0000-0000-0000-000000000891"),
      PhysicalRecoveryCutoverState.Certified,
      1
    )

  private def exactMarker: Array[Byte] = Array[Byte](
    0x46, 0x50, 0x57, 0x42,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x91.toByte
  )

  private def legacyExactMarker: Array[Byte] = Array[Byte](
    0x46, 0x50, 0x57, 0x42,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x92.toByte
  )

  private def regularChildCount(directory: java.nio.file.Path): Long =
    val children = Files.list(directory)
    try children.count()
    finally children.close()

  private def assertRejected(marker: java.nio.file.Path, bytes: Array[Byte], binding: PhysicalRecoveryBinding): Unit =
    Files.write(marker, bytes)
    assertTrue(WorldBinding.validate(binding, marker).isFailure)
