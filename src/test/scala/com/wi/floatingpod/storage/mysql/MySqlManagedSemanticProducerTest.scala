package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.security.MessageDigest
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*
import scala.jdk.CollectionConverters.*

/** 仅用合成观测覆盖类型化序列化与当前闭合解析器；不代表生产 MySQL 观测、安全批准或预期策略来源。 */
class MySqlManagedSemanticProducerTest:
  @TempDir var directory: Path = _

  private val server = UUID.fromString("1248b69d-94aa-4ae3-a0d5-27a665a94803")
  private val provisioning = UUID.fromString("3b298503-9c57-4501-97c9-2c07112f5d31")
  private val binding = UUID.fromString("df1ba15c-6f29-4849-b95e-aee48e45542d")
  private val creation = UUID.fromString("439fe1d5-c412-4e55-9df6-a2e16f0817b6")
  private val bootstrap = UUID.fromString("5596a525-930e-4727-a151-5534b841e97d")
  private val prepare = UUID.fromString("16cbfd14-b85e-4bae-804e-239dc5784e44")
  private val certify = UUID.fromString("a991f3c4-4a52-43a0-b015-842f16797a22")
  private val profile = DeploymentReceiptStaticTestData.defaultProfile()
  private val policy = DeploymentReceiptStaticTestData.policy

  private def root(): MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile.sourceLimitProfile,
      profile.sourceLimitProfile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))

  /** 仅测试使用的合成 SHOW 文本，由实际渲染器派生；不是数据库观测证据。 */
  private def callerGrantsFor(selectedProfile: MySqlDeploymentReceiptSchemaProfile.Inputs): Vector[String] =
    val renderedSql = MySqlDbFirstSchemaProfile.renderExistingDatabase(selectedProfile).sql
    val grantee = selectedProfile.accounts.runtime.grantee
    renderedSql.linesIterator.filter(line => line.startsWith("GRANT ") && line.contains(s" TO $grantee;"))
      .map { line =>
        val columns = "UPDATE \\(([^)]*)\\)".r
        columns.replaceAllIn(line.stripSuffix(";"), matched =>
          "UPDATE (" + matched.group(1).split(",").map(name => "`" + name.trim + "`").mkString(",") + ")")
      }.toVector :+ s"GRANT USAGE ON *.* TO $grantee"

  /** 合成 fixture 元数据与渲染器形状的行；绝不作为生产 expected result。 */
  private def definerPrivilegesFor(selectedProfile: MySqlDeploymentReceiptSchemaProfile.Inputs): Vector[MySqlAuditResultSet] =
    val source = DeploymentReceiptStaticTestData.privileges(MySqlDeploymentReceiptSchemaProfile.render(selectedProfile))
    val rows = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(MySqlDbFirstSchemaProfile.renderExistingDatabase(selectedProfile))
    source.zip(rows).map { (metadata, values) =>
      metadata.copy(rows = values.map(value => MySqlAuditRow(value.map(MySqlAuditText.apply))))
    }

  private def produce(actualRoot: MySqlFormat3ReadBudget, connected: String = "fp_s5_runtime@127.0.0.1",
      grants: Vector[String] = callerGrantsFor(profile),
      privileges: Vector[MySqlAuditResultSet] = definerPrivilegesFor(profile)): MySqlManagedSemanticProducer.Data =
    MySqlManagedSemanticProducer.create(actualRoot, profile, server, provisioning, binding,
      creation, bootstrap, prepare, certify, profile.accounts.runtime.identity, connected,
      policy, grants, privileges)

  private def bytes(data: MySqlManagedSemanticProducer.Data): Array[Byte] =
    val output = new ByteArrayOutputStream()
    data.writeSemantic(output)
    output.toByteArray

  private def sha(text: String): String = HexFormat.of().formatHex(
    MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)))

  @Test def preSeedConfigurationDigestMatchesTheExistingTypedManagedDigestSeam(): Unit =
    val fixture = MySqlDbFirstHistoricalTestFixture.fixture(directory, profile)
    val budget = fixture.root()
    val generated = MySqlDeploymentReceiptRuntimeContract.generateManagedDbFirst(fixture.profile)
    val metadataFields = fixture.metadata.deploymentSemantics().canonicalFields().asScala.toMap.updated(
      "approvedDdlSha256", MySqlDbFirstSchemaProfile.renderExistingDatabase(fixture.profile).sha256
        .toUpperCase(java.util.Locale.ROOT))
    val metadata = MySqlRuntimeMetadata.parseDbFirstManifestProjection(metadataFields.asJava)
    val fixtureServer = UUID.fromString(fixture.fields("expectedServerUuid"))
    val fixtureProvisioning = UUID.fromString(fixture.fields("expectedProvisioningUuid"))
    val fixtureBinding = UUID.fromString(fixture.fields("expectedBindingUuid"))
    var data: MySqlManagedSemanticProducer.Data = null
    var parsed: MySqlDeploymentReceiptRuntimeContract.ManagedSemantic = null
    try
      data = MySqlManagedSemanticProducer.create(budget, fixture.profile, fixtureServer, fixtureProvisioning,
        fixtureBinding, creation, bootstrap, prepare, certify, fixture.profile.accounts.runtime.identity,
        "fp_s5_runtime@127.0.0.1", policy, callerGrantsFor(fixture.profile), definerPrivilegesFor(fixture.profile))
      val semanticBytes = bytes(data)
      parsed = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(
        semanticBytes, budget, fixture.profile, generated)
      val retainedBefore = budget.accountedMemoryBytes
      val expected = MySqlCurrentConfigurationProjection.managedDbFirstDigest(metadata, parsed, policy)
      assertArrayEquals(expected, data.configurationDigest(metadata, fixture.profile, generated, policy))
      assertEquals(retainedBefore, budget.accountedMemoryBytes,
        "pre-seed C production must release its temporary typed-parse and canonical-write workspaces")
      parsed.close()
      parsed = null
      data.close()
      assertThrows(classOf[java.sql.SQLDataException], () => data.configurationDigest(metadata, fixture.profile, generated, policy))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally
      if parsed != null then parsed.close()
      if data != null then data.close()
      budget.closeRootReadBudget()

  @Test def typedManagedSemanticIsStablePinFreeAndCanAppendExplicitFinalPins(): Unit =
    val budget = root()
    val first = produce(budget)
    val second = produce(budget)
    try
      val firstBytes = bytes(first)
      val secondBytes = bytes(second)
      assertArrayEquals(firstBytes, secondBytes)
      val semantic = JsonParser.parseString(new String(firstBytes, StandardCharsets.UTF_8)).getAsJsonObject
      assertEquals("FP_MYSQL_DB_FIRST1_RUNTIME_CONTRACT_V1", semantic.get("format").getAsString)
      assertEquals("MANAGED_INSTALL", semantic.get("sourceProvenanceKind").getAsString)
      assertFalse(semantic.has("approvedBaselineSha256"))
      assertFalse(semantic.has("sourceCaptureSha256"))
      assertEquals(6, semantic.get("schemaFormat").getAsInt)
      val encodedSets = semantic.getAsJsonArray("definerPrivileges").asScala.toVector.map(_.getAsJsonObject)
      val observedSets = definerPrivilegesFor(profile)
      assertEquals(observedSets.size, encodedSets.size)
      observedSets.zip(encodedSets).foreach { (observed, encoded) =>
        assertEquals(observed.id, encoded.get("id").getAsString)
        val columns = encoded.getAsJsonArray("columns").asScala.toVector.map(_.getAsJsonObject)
        assertEquals(observed.columns.map(c => (c.label, c.jdbcType, c.typeName, c.precision, c.scale, c.nullable, c.signed)),
          columns.map(c => (c.get("label").getAsString, c.get("jdbcType").getAsInt, c.get("typeName").getAsString,
            c.get("precision").getAsInt, c.get("scale").getAsInt, c.get("nullable").getAsInt, c.get("signed").getAsBoolean)))
        val actualRows = encoded.getAsJsonArray("rows").asScala.toVector.map { row =>
          row.getAsJsonArray.asScala.toVector.map(cell => cell.getAsJsonObject.get("value").getAsString)
        }
        val expectedRows = observed.rows.map(_.cells.map {
          case MySqlAuditText(value) => value
          case other => throw new AssertionError("synthetic fixture must remain text-only: " + other)
        })
        assertEquals(expectedRows.sortBy(_.mkString("\u0000")), actualRows.sortBy(_.mkString("\u0000")))
      }
      val recipe = HexFormat.of().parseHex(semantic.get("sourceProvenanceHex").getAsString)
      assertArrayEquals(recipe, MySqlDeploymentReceiptRuntimeContract.decodeManagedDbFirstRecipe(recipe).canonicalBytes)
      assertEquals(DeploymentReceiptStaticTestData.sha(recipe), semantic.get("sourceProvenanceSha256").getAsString)

      val full = new ByteArrayOutputStream()
      val baselineSha = sha("synthetic test-only baseline pin")
      val captureSha = sha("synthetic test-only capture pin")
      first.writeFullK(full, baselineSha, captureSha)
      val pinned = JsonParser.parseString(new String(full.toByteArray, StandardCharsets.UTF_8)).getAsJsonObject
      assertEquals(baselineSha, pinned.get("approvedBaselineSha256").getAsString)
      assertEquals(captureSha, pinned.get("sourceCaptureSha256").getAsString)
      pinned.remove("approvedBaselineSha256")
      pinned.remove("sourceCaptureSha256")
      assertEquals(semantic, pinned)
    finally
      first.close()
      second.close()
      assertEquals(0L, budget.accountedMemoryBytes)
      budget.closeRootReadBudget()

  @Test def scopedGrantParserRejectsWrongAccountForeignScopeAndDuplicateObservation(): Unit =
    val valid = callerGrantsFor(profile)
    val wrongSchema = valid.updated(0, valid.head.replace(profile.schema, "foreign_schema"))
    val extra = valid :+ s"GRANT SELECT ON `foreign_schema`.* TO ${profile.accounts.runtime.grantee}"
    val wrongAccount = valid.updated(0, valid.head.replace(profile.accounts.runtime.grantee, "'other_runtime'@'localhost'"))
    val duplicate = valid :+ valid.head
    val cases = Vector(wrongSchema, wrongAccount, extra, duplicate)
    cases.foreach { input =>
      val budget = root()
      try
        assertThrows(classOf[java.sql.SQLDataException], () => produce(budget, grants = input))
        assertEquals(0L, budget.accountedMemoryBytes)
      finally budget.closeRootReadBudget()
    }

  @Test def managedSecurityRowsAreRevalidatedAndFailureReleasesTheOriginalRoot(): Unit =
    val original = definerPrivilegesFor(profile)
    val alteredFirst = original.head.copy(rows = original.head.rows.updated(0,
      MySqlAuditRow(original.head.rows.head.cells.updated(0, MySqlAuditText("'foreign'@'host'")))))
    val budget = root()
    try
      assertThrows(classOf[IllegalArgumentException], () => produce(budget, privileges = original.updated(0, alteredFirst)))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()
