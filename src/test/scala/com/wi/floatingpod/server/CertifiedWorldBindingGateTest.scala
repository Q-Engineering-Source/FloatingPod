package com.wi.floatingpod.server

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import net.minecraftforge.fml.common.event.FMLServerAboutToStartEvent
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.nio.file.Files
import java.util.UUID

/** AboutToStart 只读认证 binding replay gate 的公开行为 tracer。 */
class CertifiedWorldBindingGateTest:
  @Test
  def validatesAnExistingCertifiedBindingAndExactMarkerWithoutMutatingTheFixture(): Unit =
    val database = Files.createTempDirectory("floatingpod-certified-world-binding-gate").resolve("pods.db")
    val markerParent = Files.createTempDirectory("floatingpod-certified-world-binding-marker")
    val marker = markerParent.resolve("floatingpod-world-binding.bin")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000894")
    val expectedBinding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 1)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      new WorldBindingCoordinator(repository, TestCanonicalMarkerPublicationAcknowledgement).prepareAndCertify(bindingUuid, 1, marker).get
      val originalMarker = Files.readAllBytes(marker)
      val originalModifiedAt = Files.getLastModifiedTime(marker)
      val gate = new CertifiedWorldBindingGate(repository, _ => marker)

      assertEquals(ValidatedWorldBinding(expectedBinding, marker), gate.validate(new FMLServerAboutToStartEvent(null.asInstanceOf[Object])))
      assertEquals(Some(expectedBinding), repository.findPhysicalRecoveryBinding().get)
      assertArrayEquals(originalMarker, Files.readAllBytes(marker))
      assertEquals(originalModifiedAt, Files.getLastModifiedTime(marker))
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  @Test
  def failsClosedWhenThePhysicalRecoveryBindingIsMissingWithoutCreatingAnyStorageState(): Unit =
    val database = Files.createTempDirectory("floatingpod-certified-world-binding-missing").resolve("pods.db")
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      val gate = new CertifiedWorldBindingGate(repository, _ => throw new AssertionError("marker resolver must not run without a binding"))

      assertThrows(classOf[IllegalStateException], () => gate.validate(event()))
      assertEquals(None, repository.findPhysicalRecoveryBinding().get)
      assertEquals(Nil, repository.findAll().get)
    finally repository.close()

  @Test
  def failsClosedForALegacyUncertifiedBindingWithoutResolvingOrMutatingTheMarker(): Unit =
    val database = Files.createTempDirectory("floatingpod-certified-world-binding-legacy").resolve("pods.db")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000895")
    val legacyBinding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 1)
    val repository = new SQLiteFloatingPodRepository(database.toString)
    try
      repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
      val gate = new CertifiedWorldBindingGate(repository, _ => throw new AssertionError("marker resolver must not run for a legacy binding"))

      assertThrows(classOf[IllegalStateException], () => gate.validate(event()))
      assertEquals(Some(legacyBinding), repository.findPhysicalRecoveryBinding().get)
      assertEquals(Nil, repository.findAll().get)
      assertEquals(None, repository.findPhysicalRecoveryLocator(bindingUuid).get)
    finally repository.close()

  private def event(): FMLServerAboutToStartEvent = new FMLServerAboutToStartEvent(null.asInstanceOf[Object])
