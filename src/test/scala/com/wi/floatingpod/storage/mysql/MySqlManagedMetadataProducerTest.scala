package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.ManagedDatabaseConfiguration
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.HexFormat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*
import scala.jdk.CollectionConverters.*

/** 直接验证配置投影与既有 metadata parser；不连接数据库，也不把测试 pin 当批准文件。 */
class MySqlManagedMetadataProducerTest:
  import DeploymentReceiptStaticTestData.*

  @TempDir var directory: Path = _

  private val installerSecret = "installer-密钥=仅测试"

  private def configuration(root: Path, extra: String = ""): ManagedDatabaseConfiguration =
    val configPath = root.resolve("config/floatingpod/database.properties")
    Files.createDirectories(configPath.getParent)
    val config =
      s"""DBHost=db.example.test
         |Port=3306
         |Database=fp_schema5_test
         |Username=fp_installer
         |Password=$installerSecret
         |TrustStore=secrets/信任 store.p12
         |TrustStorePasswordFile=secrets/trust secret.txt
         |$extra""".stripMargin
    Files.writeString(configPath, config, StandardCharsets.UTF_8)
    ManagedDatabaseConfiguration.load(root, configPath)

  private def record(profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      rendered: MySqlDbFirstSchemaProfile.Rendered): ManagedInstallationIdentity.Record =
    ManagedInstallationIdentity.Record(
      java.util.UUID.fromString("00000000-0000-4000-8000-000000000001"),
      server,
      binding,
      provisioning,
      java.util.UUID.fromString("00000000-0000-4000-8000-000000000002"),
      java.util.UUID.fromString("00000000-0000-4000-8000-000000000003"),
      java.util.UUID.fromString("00000000-0000-4000-8000-000000000004"),
      java.util.UUID.fromString("00000000-0000-4000-8000-000000000005"),
      profile.schema,
      "a" * 64,
      rendered.sha256)

  private def root(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile.sourceLimitProfile,
      profile.sourceLimitProfile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))

  private def producer(config: ManagedDatabaseConfiguration, profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      budget: MySqlFormat3ReadBudget): MySqlManagedMetadataProducer.Projection =
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
    MySqlManagedMetadataProducer.create(budget, config, record(profile, rendered), profile.sourceLimitProfile,
      rendered, directory.resolve("secrets/runtime 密钥.txt"), directory.resolve("state/B baseline=stable.json"),
      directory.resolve("state/K runtime=contract.json"))

  private def pins(suffix: String = "base"): MySqlManagedMetadataProducer.Pins =
    def sha(value: String): String = HexFormat.of().formatHex(
      MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)))
    MySqlManagedMetadataProducer.Pins(sha(s"$suffix B bytes"), sha(s"$suffix source capture bytes"),
      sha(s"$suffix K bytes"))

  private def fullBytes(value: MySqlManagedMetadataProducer.SerializedMetadata): Array[Byte] =
    val output = new ByteArrayOutputStream()
    value.writeTo(output)
    output.toByteArray

  @Test def projectionUsesRuntimeIdentityAndRoundTripsEscapedPathsWithoutInstallerSecret(): Unit =
    val profile = defaultProfile()
    val budget = root(profile)
    val config = configuration(directory)
    var projection: MySqlManagedMetadataProducer.Projection = null
    var full: MySqlManagedMetadataProducer.SerializedMetadata = null
    try
      projection = producer(config, profile, budget)
      val fields = projection.projectionFields.asScala.toMap
      val parsedProjection = MySqlRuntimeMetadata.parseDbFirstManifestProjection(fields.asJava)
      assertEquals(profile.accounts.runtime.user, fields("username"))
      assertEquals(profile.accounts.runtime.identity, fields("expectedEffectiveUser"))
      assertEquals(profile.accounts.auditDefiner.identity, fields("expectedAuditDefiner"))
      assertEquals("fp_installer", config.username())
      assertNotEquals(config.username(), fields("username"))
      assertEquals("file:" + directory.resolve("secrets/runtime 密钥.txt").toAbsolutePath.normalize(),
        fields("credentialSourceRef"))
      assertEquals("file:" + directory.resolve("secrets/trust secret.txt").toAbsolutePath.normalize(),
        fields("trustSecretSourceRef"))
      assertEquals(directory.resolve("secrets/信任 store.p12").toAbsolutePath.normalize().toString,
        fields("pkcs12TrustStorePath"))
      assertEquals(MySqlRuntimeMetadata.DB_FIRST_MANIFEST,
        parsedProjection.canonicalFields().get("dbFirstManifestConfigurationEncoding"))
      assertFalse(fields.contains("approvedBaselineSha256"))
      assertFalse(fields.contains("approvedSourceCaptureSha256"))
      assertFalse(fields.contains("dbFirstRuntimeContractSha256"))

      full = projection.serializeFull(pins())
      val bytes = fullBytes(full)
      val serialized = new String(bytes, StandardCharsets.UTF_8)
      assertFalse(serialized.contains(installerSecret))
      assertFalse(serialized.contains("fp_installer"))
      assertFalse(projection.toString.contains(installerSecret))
      assertFalse(full.toString.contains(installerSecret))
      val decoded = MySqlRuntimeMetadata.fromDbFirstBytes(bytes)
      val complete = decoded.completeCanonicalFields()
      assertEquals(fields("approvedBaselinePath"), complete.get("approvedBaselinePath"))
      assertEquals(fields("dbFirstRuntimeContractPath"), complete.get("dbFirstRuntimeContractPath"))
      assertEquals(pins().approvedBaselineSha256, complete.get("approvedBaselineSha256"))
      assertEquals(pins().approvedSourceCaptureSha256, complete.get("approvedSourceCaptureSha256"))
      assertEquals(pins().dbFirstRuntimeContractSha256, complete.get("dbFirstRuntimeContractSha256"))
      assertTrue(budget.accountedMemoryBytes > 0L)
    finally
      if full != null then full.close()
      if projection != null then projection.close()
      config.close()
      assertEquals(0L, budget.accountedMemoryBytes)
      budget.closeRootReadBudget()

  @Test def absentMalformedAndChangedPinsAreExplicitAndNeverCached(): Unit =
    val profile = defaultProfile()
    val budget = root(profile)
    val config = configuration(directory)
    var projection: MySqlManagedMetadataProducer.Projection = null
    var first: MySqlManagedMetadataProducer.SerializedMetadata = null
    var changed: MySqlManagedMetadataProducer.SerializedMetadata = null
    try
      projection = producer(config, profile, budget)
      assertThrows(classOf[IllegalArgumentException], () => projection.serializeFull(null))
      assertThrows(classOf[IllegalArgumentException], () => MySqlManagedMetadataProducer.Pins(null,
        "0" * 64, "1" * 64))
      assertThrows(classOf[IllegalArgumentException], () => MySqlManagedMetadataProducer.Pins("0" * 64,
        null, "1" * 64))
      assertThrows(classOf[IllegalArgumentException], () => MySqlManagedMetadataProducer.Pins("0" * 64,
        "1" * 64, null))
      assertThrows(classOf[IllegalArgumentException], () => MySqlManagedMetadataProducer.Pins("z" * 64,
        "0" * 64, "1" * 64))
      first = projection.serializeFull(pins("initial"))
      changed = projection.serializeFull(pins("changed actual file bytes"))
      val before = MySqlRuntimeMetadata.fromDbFirstBytes(fullBytes(first)).completeCanonicalFields()
      val after = MySqlRuntimeMetadata.fromDbFirstBytes(fullBytes(changed)).completeCanonicalFields()
      assertNotEquals(before.get("approvedBaselineSha256"), after.get("approvedBaselineSha256"))
      assertNotEquals(before.get("approvedSourceCaptureSha256"), after.get("approvedSourceCaptureSha256"))
      assertNotEquals(before.get("dbFirstRuntimeContractSha256"), after.get("dbFirstRuntimeContractSha256"))
      assertEquals(projection.projectionFields, MySqlRuntimeMetadata.parseDbFirstManifestProjection(
        projection.projectionFields).canonicalFields())
    finally
      if changed != null then changed.close()
      if first != null then first.close()
      if projection != null then projection.close()
      config.close()
      assertEquals(0L, budget.accountedMemoryBytes)
      budget.closeRootReadBudget()

  @Test def ordinaryTimeoutsOutsideTheStaticMetadataContractFailClearly(): Unit =
    // 普通配置入口必须先拒绝，不能等到创建数据库对象之后才发现 metadata 上限不兼容。
    val connectFailure = assertThrows(classOf[IllegalArgumentException], () =>
      configuration(directory.resolve("connect"), "ConnectTimeoutMillis=30001\n"))
    assertTrue(connectFailure.getMessage.contains("ConnectTimeoutMillis"))
    val socketFailure = assertThrows(classOf[IllegalArgumentException], () =>
      configuration(directory.resolve("socket"), "SocketTimeoutMillis=300001\n"))
    assertTrue(socketFailure.getMessage.contains("SocketTimeoutMillis"))
