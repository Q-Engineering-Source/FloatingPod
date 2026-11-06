package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.HexFormat
import scala.jdk.CollectionConverters.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 纯语义投影回归；只使用显式测试数据，不连接、建表或发行运行资格。 */
class MySqlManagedConfigurationDigestTest:
  import DeploymentReceiptStaticTestData.*

  @TempDir var directory: Path = _

  private def managedK(f: MySqlDbFirstHistoricalTestFixture.StaticFixture,
      operation: String = "00000000-0000-0000-0000-000000000011"): Array[Byte] =
    val value = f.contract.deepCopy()
    val managedDdl = MySqlDbFirstSchemaProfile.renderExistingDatabase(f.profile).sha256
    val oldDdl = value.get("ddlSha256").getAsString
    value.addProperty("ddlSha256", managedDdl)
    value.remove("approvedBaselineSha256")
    value.remove("sourceCaptureSha256")
    val source = new String(HexFormat.of().parseHex(value.get("sourceProvenanceHex").getAsString), StandardCharsets.UTF_8)
      .replace(MySqlDbFirstRuntimeContract.RecipeEncoding, MySqlDeploymentReceiptRuntimeContract.ManagedDbFirstRecipeEncoding)
      .replace("ddlSha256=" + oldDdl.toLowerCase(java.util.Locale.ROOT), "ddlSha256=" + managedDdl)
      .replace("creationOperationUuid=00000000-0000-0000-0000-000000000011\n", s"creationOperationUuid=$operation\n")
      .getBytes(StandardCharsets.UTF_8)
    value.addProperty("sourceProvenanceKind", "MANAGED_INSTALL")
    value.addProperty("sourceProvenanceHex", HexFormat.of().formatHex(source))
    value.addProperty("sourceProvenanceSha256", sha(source))
    gson.toJson(value).getBytes(StandardCharsets.UTF_8)

  private def generated(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): MySqlDeploymentReceiptRuntimeContract.Generated =
    MySqlDeploymentReceiptRuntimeContract.generateManagedDbFirst(profile)

  private def metadataFields(f: MySqlDbFirstHistoricalTestFixture.StaticFixture): Map[String, String] =
    f.metadata.deploymentSemantics().canonicalFields().asScala.toMap.updated("approvedDdlSha256",
      MySqlDbFirstSchemaProfile.renderExistingDatabase(f.profile).sha256.toUpperCase(java.util.Locale.ROOT))

  @Test def managedRecipeIsStrictCanonicalDataWithoutNativeApproval(): Unit =
    val f = MySqlDbFirstHistoricalTestFixture.fixture(directory, defaultProfile())
    val source = managedK(f)
    val json = JsonParser.parseString(new String(source, StandardCharsets.UTF_8)).getAsJsonObject
    val recipe = HexFormat.of().parseHex(json.get("sourceProvenanceHex").getAsString)
    val decoded = MySqlDeploymentReceiptRuntimeContract.decodeManagedDbFirstRecipe(recipe)
    assertArrayEquals(recipe, decoded.canonicalBytes)
    assertFalse(json.has("approvedBaselineSha256"))
    assertFalse(json.has("sourceCaptureSha256"))
    val bad = new String(recipe, StandardCharsets.UTF_8).replace("physicalFormat=2", "physicalFormat=3")
      .getBytes(StandardCharsets.UTF_8)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlDeploymentReceiptRuntimeContract.decodeManagedDbFirstRecipe(bad))

  @Test def schema6MetadataProjectionRejectsAnyBackwardFilePin(): Unit =
    val f = MySqlDbFirstHistoricalTestFixture.fixture(directory, defaultProfile())
    val fields = metadataFields(f)
    val accepted = MySqlRuntimeMetadata.parseDbFirstManifestProjection(fields.asJava)
    assertEquals(MySqlDbFirstRuntimeContract.Encoding,
      accepted.canonicalFields().get("dbFirstManifestConfigurationEncoding"))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeMetadata.parseDbFirstManifestProjection((fields + ("dbFirstRuntimeContractSha256" -> ("0" * 64))).asJava))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeMetadata.parseDbFirstManifestProjection((fields + ("unexpected" -> "1")).asJava))

  @Test def managedDigestUsesTheSharedPinFreeCanonicalRecipeAndTracksSemanticChanges(): Unit =
    val f = MySqlDbFirstHistoricalTestFixture.fixture(directory, defaultProfile())
    val root = f.root()
    val metadata = MySqlRuntimeMetadata.parseDbFirstManifestProjection(
      metadataFields(f).asJava)
    val render = generated(f.profile)
    val original = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(managedK(f), root, f.profile, render)
    val repeat = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(managedK(f), root, f.profile, render)
    val changed = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(
      managedK(f, "00000000-0000-0000-0000-000000000021"), root, f.profile, render)
    try
      val first = MySqlCurrentConfigurationProjection.managedDbFirstDigest(metadata, original, policy)
      assertArrayEquals(first, MySqlCurrentConfigurationProjection.managedDbFirstDigest(metadata, repeat, policy))
      assertFalse(java.util.Arrays.equals(first, MySqlCurrentConfigurationProjection.managedDbFirstDigest(metadata, changed, policy)))
      assertTrue(root.accountedMemoryBytes > 0L)

      val withPinsA = JsonParser.parseString(new String(managedK(f), StandardCharsets.UTF_8)).getAsJsonObject
      val withPinsB = withPinsA.deepCopy()
      withPinsA.addProperty("approvedBaselineSha256", "0" * 64)
      withPinsA.addProperty("sourceCaptureSha256", "1" * 64)
      withPinsB.addProperty("approvedBaselineSha256", "F" * 64)
      withPinsB.addProperty("sourceCaptureSha256", "E" * 64)
      val a = new java.io.ByteArrayOutputStream()
      val b = new java.io.ByteArrayOutputStream()
      MySqlDeploymentReceiptRuntimeContract.writeSemanticFields(withPinsA, a)
      MySqlDeploymentReceiptRuntimeContract.writeSemanticFields(withPinsB, b)
      assertArrayEquals(a.toByteArray, b.toByteArray)
    finally
      changed.close()
      repeat.close()
      original.close()
      assertEquals(0L, root.accountedMemoryBytes)
      root.closeRootReadBudget()

  @Test def malformedManagedSemanticDoesNotYieldDigestData(): Unit =
    val f = MySqlDbFirstHistoricalTestFixture.fixture(directory, defaultProfile())
    val root = f.root()
    val render = generated(f.profile)
    val unknown = JsonParser.parseString(new String(managedK(f), StandardCharsets.UTF_8)).getAsJsonObject
    unknown.addProperty("approvedBaselineSha256", "0" * 64)
    try
      assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(gson.toJson(unknown).getBytes(StandardCharsets.UTF_8),
          root, f.profile, render))
      assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlCurrentConfigurationProjection.managedDbFirstDigest(null, null, policy))
      assertEquals(0L, root.accountedMemoryBytes)
    finally root.closeRootReadBudget()

  @Test def initialDigestMatchesNormalPinnedManagedAssembly(): Unit =
    val f = MySqlDbFirstHistoricalTestFixture.fixture(directory, defaultProfile())
    val root = f.root()
    val semantic = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(
      managedK(f), root, f.profile, generated(f.profile))
    val projected = MySqlRuntimeMetadata.parseDbFirstManifestProjection(metadataFields(f).asJava)
    val initial = try MySqlCurrentConfigurationProjection.managedDbFirstDigest(projected, semantic, policy)
      finally semantic.close()
    val originalPin = f.metadata.baseline()
    val pack = JsonParser.parseString(java.nio.file.Files.readString(originalPin.absolutePath())).getAsJsonObject
    pack.addProperty("ddlSha256", MySqlDbFirstSchemaProfile.renderExistingDatabase(f.profile).sha256.toUpperCase(java.util.Locale.ROOT))
    val baselineBytes = gson.toJson(pack).getBytes(StandardCharsets.UTF_8)
    java.nio.file.Files.write(originalPin.absolutePath(), baselineBytes)
    val contract = JsonParser.parseString(new String(managedK(f), StandardCharsets.UTF_8)).getAsJsonObject
    contract.addProperty("approvedBaselineSha256", sha(baselineBytes))
    contract.addProperty("sourceCaptureSha256", originalPin.sourceCaptureSha256())
    val contractBytes = gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
    java.nio.file.Files.write(f.metadata.runtimeContract().absolutePath(), contractBytes)
    val fields = f.fields ++ Map(
      "approvedDdlSha256" -> MySqlDbFirstSchemaProfile.renderExistingDatabase(f.profile).sha256.toUpperCase(java.util.Locale.ROOT),
      "approvedBaselineSha256" -> sha(baselineBytes), "dbFirstRuntimeContractSha256" -> sha(contractBytes))
    def escape(value: String): String = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
    val metadata = MySqlRuntimeMetadata.fromDbFirstBytes(fields.toVector.sortBy(_._1)
      .map((key, value) => s"$key=${escape(value)}\n").mkString.getBytes(StandardCharsets.UTF_8))
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    try
      val graph = MySqlRuntimeInputs.assembleDbFirstStatic(metadata, owner, policy)
      assertArrayEquals(initial, HexFormat.of().parseHex(graph.configurationSha256))
      val io = MySqlOwnedIoIdentity(java.util.UUID.randomUUID(), 1L, 1L)
      val sessionInputs = MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph, root, io)
      try
        sessionInputs.requireForSession(root, io)
        assertTrue(sessionInputs.settings.utf8mb4ResultsRequired)
      finally sessionInputs.close()
    finally
      owner.close()
      // Full27 root binding remains owned by the workflow root until its explicit close.
      root.closeRootReadBudget()
      assertEquals(0L, root.accountedMemoryBytes)
