package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import scala.util.Try

/** supplied token 的 world binding cutover 只经 coordinator seam 验证。 */
class WorldBindingCoordinatorTest:
  @Test
  def preparesAndCertifiesTheSuppliedBindingWithoutCreatingDomainRowsOrLocatorsAndRetainsAnExactReplay(): Unit =
    val database = Files.createTempDirectory("floatingpod-world-binding-coordinator").resolve("pods.db")
    val markerParent = Files.createTempDirectory("floatingpod-world-binding-marker-parent")
    val marker = markerParent.resolve("canonical.marker")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000893")
    val expectedBinding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 1)
    val expected = ValidatedWorldBinding(expectedBinding, marker)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val coordinator = new WorldBindingCoordinator(repository, TestCanonicalMarkerPublicationAcknowledgement)
    try
      assertEquals(expected, coordinator.prepareAndCertify(bindingUuid, 1, marker).get)
      assertEquals(Some(expectedBinding), repository.findPhysicalRecoveryBinding().get)
      assertArrayEquals(expectedMarker, Files.readAllBytes(marker))
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)

      val createdAt = Files.getLastModifiedTime(marker)
      assertEquals(expected, coordinator.prepareAndCertify(bindingUuid, 1, marker).get)
      assertEquals(Some(expectedBinding), repository.findPhysicalRecoveryBinding().get)
      assertArrayEquals(expectedMarker, Files.readAllBytes(marker))
      assertEquals(createdAt, Files.getLastModifiedTime(marker))
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  @Test
  def defaultCoordinatorFailsClosedWithoutPublishingOrCertifyingAnAbsentBindingAndRetainsTheExactLegacyReplayToken(): Unit =
    val database = Files.createTempDirectory("floatingpod-world-binding-default-barrier").resolve("pods.db")
    val markerParent = Files.createTempDirectory("floatingpod-world-binding-default-marker-parent")
    val marker = markerParent.resolve("canonical.marker")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000896")
    val expectedLegacyBinding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 1)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val coordinator = new WorldBindingCoordinator(repository)
    try
      assertTrue(coordinator.prepareAndCertify(bindingUuid, 1, marker).isFailure)
      assertEquals(Some(expectedLegacyBinding), repository.findPhysicalRecoveryBinding().get)
      assertFalse(Files.exists(marker))
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)

      assertTrue(coordinator.prepareAndCertify(bindingUuid, 1, marker).isFailure)
      assertEquals(Some(expectedLegacyBinding), repository.findPhysicalRecoveryBinding().get)
      assertFalse(Files.exists(marker))
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  @Test
  def productionBarrierPublishesTheExactMarkerAndReplaysIdempotentlyUnderADeterministicToken(): Unit =
    val database = Files.createTempDirectory("floatingpod-world-binding-production-barrier").resolve("pods.db")
    val markerParent = Files.createTempDirectory("floatingpod-world-binding-production-marker-parent")
    val marker = markerParent.resolve("floatingpod-world-binding.bin")
    val folderName = "Dim-Transit-Station"
    val bindingUuid = WorldBindingBootstrap.bindingTokenFor(folderName)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    val coordinator = new WorldBindingCoordinator(repository, FileSystemCanonicalMarkerPublicationBarrier)
    try
      assertEquals(bindingUuid, WorldBindingBootstrap.bindingTokenFor(folderName))
      assertFalse(bindingUuid == WorldBindingBootstrap.bindingTokenFor("Another-World"))
      assertEquals(1, WorldBindingBootstrap.InitialFormatVersion)
      val expected = ValidatedWorldBinding(PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 1), marker)

      assertEquals(expected, coordinator.prepareAndCertify(bindingUuid, WorldBindingBootstrap.InitialFormatVersion, marker).get)
      assertEquals(24L, Files.size(marker))
      val createdAt = Files.getLastModifiedTime(marker)

      assertEquals(expected, coordinator.prepareAndCertify(bindingUuid, WorldBindingBootstrap.InitialFormatVersion, marker).get)
      assertEquals(createdAt, Files.getLastModifiedTime(marker))
      assertEquals(Some(expected.binding), repository.findPhysicalRecoveryBinding().get)
      assertEquals(Nil, repository.findAll().get)
    finally repository.close()

  private def expectedMarker: Array[Byte] = Array[Byte](
    0x46, 0x50, 0x57, 0x42,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x93.toByte
  )

/** 测试专用 acknowledgement：只复用现有 marker publication，不代表 parent namespace 已耐久同步。 */
private[server] object TestCanonicalMarkerPublicationAcknowledgement extends CanonicalMarkerPublicationBarrier:
  override def publish(binding: PhysicalRecoveryBinding, canonicalMarker: Path): Try[Path] =
    WorldBinding.createOrRetainMarker(binding, canonicalMarker)
