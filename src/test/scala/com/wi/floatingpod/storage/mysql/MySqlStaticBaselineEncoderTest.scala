package com.wi.floatingpod.storage.mysql

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.HexFormat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/** synthetic pack只检查真实encoder/loader对称性，绝不是managed结构批准来源或SQL证据。 */
class MySqlStaticBaselineEncoderTest:
  @TempDir var directory: Path = _

  @Test def exactStaticDataRoundTripsWithoutIssuingRuntimeCapability(): Unit =
    val fixture = DeploymentReceiptStaticTestData.fixture(directory)
    val pin = fixture.metadata.baseline()
    val id = MySqlApprovedBaselineIdentity(pin.format(), pin.schemaFormat(), pin.coverage(),
      DeploymentReceiptStaticTestData.server, pin.serverVersion(), fixture.profile.schema,
      pin.provisioningUuid(), DeploymentReceiptStaticTestData.binding, pin.ddlSha256(),
      pin.auditDefiner(), pin.sourceCaptureSha256())
    val original = MySqlApprovedBaselineLoader.load(pin.absolutePath(), pin.fileSha256(), id)
    val root = fixture.root()
    val before = root.accountedMemoryBytes
    val output = new ByteArrayOutputStream()
    val hash = MySqlStaticBaselineEncoder.write(id, original.objects, output, root)
    assertEquals(before, root.accountedMemoryBytes)
    val bytes = output.toByteArray
    assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), hash)
    val destination = directory.resolve("roundtrip.json")
    Files.write(destination, bytes)
    val actual = MySqlApprovedBaselineLoader.load(destination, hash, id)
    assertEquals(id, actual.identity)
    assertEquals(original.objects.resultSets, actual.objects.resultSets)
    assertEquals(original.objects.canonicalSha256, actual.objects.canonicalSha256)
    val second = new ByteArrayOutputStream()
    assertEquals(hash, MySqlStaticBaselineEncoder.write(id, original.objects, second, root))
    assertArrayEquals(bytes, second.toByteArray)

  @Test def outputFailurePreservesCauseAndReleasesOnlyItsTemporaryWorkspace(): Unit =
    val fixture = DeploymentReceiptStaticTestData.fixture(directory)
    val pin = fixture.metadata.baseline()
    val id = MySqlApprovedBaselineIdentity(pin.format(), pin.schemaFormat(), pin.coverage(),
      DeploymentReceiptStaticTestData.server, pin.serverVersion(), fixture.profile.schema,
      pin.provisioningUuid(), DeploymentReceiptStaticTestData.binding, pin.ddlSha256(),
      pin.auditDefiner(), pin.sourceCaptureSha256())
    val original = MySqlApprovedBaselineLoader.load(pin.absolutePath(), pin.fileSha256(), id)
    val root = fixture.root()
    val before = root.accountedMemoryBytes
    val primary = new java.io.IOException("forced output failure")
    val destination = new java.io.OutputStream:
      override def write(value: Int): Unit = throw primary
    val actual = assertThrows(classOf[java.io.IOException], () =>
      MySqlStaticBaselineEncoder.write(id, original.objects, destination, root))
    assertSame(primary, actual)
    assertEquals(before, root.accountedMemoryBytes)
