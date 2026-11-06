package com.wi.floatingpod.storage

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.google.gson.JsonObject
import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.DriverManager
import java.security.MessageDigest
import java.util.UUID
import java.util.HexFormat
import scala.util.Failure
import scala.util.Try

/**
 * 直接验证迁移包的完整性校验、冲突策略与收回状态，保证导入不产生第二个活动权威。
 */
class JsonImportExportTest:
  /** 验证空存储导出包含 schema、内容哈希和空状态数组。 */
  @Test
  def exportsEmptyDatabaseWithContentHash(): Unit = withRepository { repository =>
    val file = Files.createTempFile("floatingpod-empty-", ".json").toFile
    try
      val io = new JsonImportExport(repository)
      io.exportToFile(file).get
      val root = JsonParser.parseString(Files.readString(file.toPath, StandardCharsets.UTF_8)).getAsJsonObject
      assertEquals(2L, root.get("schema_version").getAsLong)
      assertTrue(root.get("content_hash").getAsString.matches("[0-9a-f]{64}"))
      assertEquals(0, root.getAsJsonArray("states").size())
    finally Files.deleteIfExists(file.toPath)
  }

  /** 验证内容哈希只依赖 schema 和状态，不随导出时间变化。 */
  @Test
  def exportsDeterministicContentHash(): Unit = withRepository { repository =>
    val first = Files.createTempFile("floatingpod-hash-first-", ".json").toFile
    val second = Files.createTempFile("floatingpod-hash-second-", ".json").toFile
    try
      repository.insert(sampleState("00000000-0000-0000-0000-000000000100")).get
      val io = new JsonImportExport(repository)
      io.exportToFile(first).get
      io.exportToFile(second).get
      val firstHash = JsonParser.parseString(Files.readString(first.toPath, StandardCharsets.UTF_8))
        .getAsJsonObject.get("content_hash").getAsString
      val secondHash = JsonParser.parseString(Files.readString(second.toPath, StandardCharsets.UTF_8))
        .getAsJsonObject.get("content_hash").getAsString
      assertEquals(firstHash, secondHash)
    finally
      Files.deleteIfExists(first.toPath)
      Files.deleteIfExists(second.toPath)
  }

  /** 验证非空导出可由另一隔离存储导入，并强制导入记录为收回状态。 */
  @Test
  def importsValidPackageAsRecalledStates(): Unit =
    val file = Files.createTempFile("floatingpod-valid-", ".json").toFile
    try
      withRepository { source =>
        val state = sampleState("00000000-0000-0000-0000-000000000101", active = true).copy(dimensionId = Some(4))
        persistActive(source, state, 4)
        new JsonImportExport(source).exportToFile(file).get
      }
      withRepository { target =>
        val imported = new JsonImportExport(target).importFromFile(file, replaceOnConflict = false).get
        assertEquals(1, imported)
        val recalled = target.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000101")).get.get
        assertFalse(recalled.isActive)
        assertEquals(None, recalled.dimensionId)
      }
    finally Files.deleteIfExists(file.toPath)

  @Test
  def importsFixedV1FixtureWithOriginalHashAndMigratesUnclaimedActiveToWorldIdle(): Unit = withRepository { target =>
    val file = Files.createTempFile("floatingpod-v1-legacy-", ".json").toFile
    val legacyV1 = """{"schema_version":1,"exported_at":0,"content_hash":"65b07a6b6cc747b7774ae24639c3f7a541f23112937884a15d0da391fb217ba2","states":[{"uuid":"00000000-0000-0000-0000-000000000051","owner_uuid":"11111111-1111-1111-1111-111111111111","state_version":7,"world_name":"legacy-world","pos_x":1.0,"pos_y":64.0,"pos_z":2.0,"yaw":0.0,"pitch":0.0,"is_active":true,"last_updated":123},{"uuid":"00000000-0000-0000-0000-000000000052","owner_uuid":null,"state_version":8,"world_name":"legacy-world","pos_x":1.0,"pos_y":64.0,"pos_z":2.0,"yaw":0.0,"pitch":0.0,"is_active":true,"last_updated":124},{"uuid":"00000000-0000-0000-0000-000000000053","owner_uuid":null,"state_version":9,"world_name":"legacy-world","pos_x":1.0,"pos_y":64.0,"pos_z":2.0,"yaw":0.0,"pitch":0.0,"is_active":false,"last_updated":125}]}"""
    try
      Files.writeString(file.toPath, legacyV1, StandardCharsets.UTF_8)

      assertEquals(3, new JsonImportExport(target).importFromFile(file, replaceOnConflict = false).get)
      val ownedActive = target.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000051")).get.get
      assertFalse(ownedActive.isActive)
      assertEquals(None, ownedActive.dimensionId)
      assertEquals(Some(UUID.fromString("11111111-1111-1111-1111-111111111111")), ownedActive.ownerUuid)
      assertEquals(PodOperatingMode.Normal, ownedActive.operatingMode)
      assertEquals(7L, ownedActive.stateVersion)
      val unclaimedActive = target.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000052")).get.get
      assertEquals(None, unclaimedActive.ownerUuid)
      assertEquals(PodOperatingMode.WorldIdle, unclaimedActive.operatingMode)
      assertEquals(8L, unclaimedActive.stateVersion)
      val unclaimedInactive = target.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000053")).get.get
      assertEquals(None, unclaimedInactive.ownerUuid)
      assertEquals(PodOperatingMode.Normal, unclaimedInactive.operatingMode)
      assertEquals(9L, unclaimedInactive.stateVersion)
    finally Files.deleteIfExists(file.toPath)
  }

  @Test
  def importsLegacyGsonV1GoldenPackageWhoseNullOwnerWasOmitted(): Unit = withRepository { target =>
    val file = Files.createTempFile("floatingpod-v1-gson-omitted-owner-", ".json").toFile
    try
      val golden = legacyGsonV1UnclaimedActivePackage()
      assertEquals(LegacyGsonV1UnclaimedActiveGolden, golden)
      val exportedState = JsonParser.parseString(golden).getAsJsonObject.getAsJsonArray("states").get(0).getAsJsonObject
      assertFalse(exportedState.has("owner_uuid"))
      Files.writeString(file.toPath, LegacyGsonV1UnclaimedActiveGolden, StandardCharsets.UTF_8)

      assertEquals(1, new JsonImportExport(target).importFromFile(file, replaceOnConflict = false).get)
      val recalled = target.findByUuid(UUID.fromString("00000000-0000-0000-0000-000000000057")).get.get
      assertEquals(None, recalled.ownerUuid)
      assertFalse(recalled.isActive)
      assertEquals(None, recalled.dimensionId)
      assertEquals(PodOperatingMode.WorldIdle, recalled.operatingMode)
      assertEquals(0L, recalled.stateVersion)
    finally Files.deleteIfExists(file.toPath)
  }

  @Test
  def v2WorldIdleModeSurvivesExportImportAndDatabaseReopen(): Unit =
    val sourceDatabase = Files.createTempDirectory("floatingpod-mode-export-source").resolve("source.db")
    val targetDatabase = Files.createTempDirectory("floatingpod-mode-export-target").resolve("target.db")
    val file = Files.createTempFile("floatingpod-mode-export-", ".json").toFile
    val normalOwnedUuid = UUID.fromString("00000000-0000-0000-0000-000000000054")
    val idleOwnedUuid = UUID.fromString("00000000-0000-0000-0000-000000000055")
    val idleUnclaimedUuid = UUID.fromString("00000000-0000-0000-0000-000000000056")
    try
      val source = new SQLiteFloatingPodRepository(sourceDatabase.toString)
      try
        persistActive(source, sampleState(normalOwnedUuid.toString, active = true, operatingMode = PodOperatingMode.Normal), 7)
        persistActive(source, sampleState(idleOwnedUuid.toString, active = true, operatingMode = PodOperatingMode.WorldIdle, ownerUuid = Some(UUID.fromString("22222222-2222-2222-2222-222222222222"))), 7)
        source.insert(sampleState(idleUnclaimedUuid.toString, ownerUuid = None, operatingMode = PodOperatingMode.WorldIdle)).get
        new JsonImportExport(source).exportToFile(file).get
      finally source.close()

      val exported = JsonParser.parseString(Files.readString(file.toPath, StandardCharsets.UTF_8)).getAsJsonObject
      assertEquals(2L, exported.get("schema_version").getAsLong)
      assertEquals(3, exported.getAsJsonArray("states").size())
      assertEquals("WorldIdle", exported.getAsJsonArray("states").get(1).getAsJsonObject.get("operating_mode").getAsString)

      val target = new SQLiteFloatingPodRepository(targetDatabase.toString)
      try assertEquals(3, new JsonImportExport(target).importFromFile(file, replaceOnConflict = false).get)
      finally target.close()

      val reopened = new SQLiteFloatingPodRepository(targetDatabase.toString)
      try
        val importedNormal = reopened.findByUuid(normalOwnedUuid).get.get
        assertEquals(Some(UUID.fromString("11111111-1111-1111-1111-111111111111")), importedNormal.ownerUuid)
        assertEquals(PodOperatingMode.Normal, importedNormal.operatingMode)
        assertEquals(1L, importedNormal.stateVersion)
        assertFalse(importedNormal.isActive)
        assertEquals(None, importedNormal.dimensionId)
        val importedOwnedIdle = reopened.findByUuid(idleOwnedUuid).get.get
        assertEquals(Some(UUID.fromString("22222222-2222-2222-2222-222222222222")), importedOwnedIdle.ownerUuid)
        assertEquals(PodOperatingMode.WorldIdle, importedOwnedIdle.operatingMode)
        assertEquals(1L, importedOwnedIdle.stateVersion)
        assertFalse(importedOwnedIdle.isActive)
        assertEquals(None, importedOwnedIdle.dimensionId)
        val importedUnclaimedIdle = reopened.findByUuid(idleUnclaimedUuid).get.get
        assertEquals(None, importedUnclaimedIdle.ownerUuid)
        assertEquals(PodOperatingMode.WorldIdle, importedUnclaimedIdle.operatingMode)
        assertFalse(importedUnclaimedIdle.isActive)
        assertEquals(None, importedUnclaimedIdle.dimensionId)
      finally reopened.close()
    finally Files.deleteIfExists(file.toPath)

  @Test
  def activeUnclaimedV2SqliteRowAndLocatorImportReopensRecalledAsWorldIdle(): Unit =
    val sourceDatabase = Files.createTempDirectory("floatingpod-active-unclaimed-v2-source").resolve("source.db")
    val targetDatabase = Files.createTempDirectory("floatingpod-active-unclaimed-v2-target").resolve("target.db")
    val file = Files.createTempFile("floatingpod-active-unclaimed-v2-", ".json").toFile
    val podUuid = UUID.fromString("00000000-0000-0000-0000-000000000058")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000059")
    val attemptUuid = UUID.fromString("00000000-0000-0000-0000-00000000005a")
    try
      val source = new SQLiteFloatingPodRepository(sourceDatabase.toString)
      try
        val active = persistActiveUnclaimedWithLocator(
          source,
          sourceDatabase,
          sampleState(podUuid.toString, operatingMode = PodOperatingMode.WorldIdle, ownerUuid = None),
          bindingUuid,
          attemptUuid,
          dimensionId = 7
        )
        assertEquals(None, active.ownerUuid)
        assertTrue(active.isActive)
        assertEquals(Some(7), active.dimensionId)
        assertEquals(PodOperatingMode.WorldIdle, active.operatingMode)
        assertEquals(1L, active.stateVersion)
        val locator = source.findPhysicalRecoveryLocator(podUuid).get.get
        assertEquals(PhysicalRecoveryLocatorState.RowActive, locator.state)
        assertEquals(Some(1L), locator.domainVersion)
        assertEquals(Some(7), locator.targetDimensionId)
        assertEquals(25, locator.chunks.size)

        new JsonImportExport(source).exportToFile(file).get
      finally source.close()

      val packageRoot = JsonParser.parseString(Files.readString(file.toPath, StandardCharsets.UTF_8)).getAsJsonObject
      assertEquals(2L, packageRoot.get("schema_version").getAsLong)
      val exported = packageRoot.getAsJsonArray("states").get(0).getAsJsonObject
      assertTrue(exported.get("owner_uuid").isJsonNull)
      assertTrue(exported.get("is_active").getAsBoolean)
      assertEquals("WorldIdle", exported.get("operating_mode").getAsString)

      val target = new SQLiteFloatingPodRepository(targetDatabase.toString)
      try assertEquals(1, new JsonImportExport(target).importFromFile(file, replaceOnConflict = false).get)
      finally target.close()

      val reopened = new SQLiteFloatingPodRepository(targetDatabase.toString)
      try
        val recalled = reopened.findByUuid(podUuid).get.get
        assertEquals(None, recalled.ownerUuid)
        assertFalse(recalled.isActive)
        assertEquals(None, recalled.dimensionId)
        assertEquals(PodOperatingMode.WorldIdle, recalled.operatingMode)
        assertEquals(1L, recalled.stateVersion)
        assertEquals(None, reopened.findPhysicalRecoveryLocator(podUuid).get)
      finally reopened.close()
    finally Files.deleteIfExists(file.toPath)

  @Test
  def v1MissingOwnerIsTheOnlyOptionalStateFieldAndV2StillRequiresIt(): Unit = withRepository { target =>
    val file = Files.createTempFile("floatingpod-migration-field-shape-", ".json").toFile
    try
      val missingV1Field = JsonParser.parseString(LegacyGsonV1UnclaimedActiveGolden).getAsJsonObject
      missingV1Field.getAsJsonArray("states").get(0).getAsJsonObject.remove("world_name")
      Files.writeString(file.toPath, new Gson().toJson(missingV1Field), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
      assertTrue(target.findAll().get.isEmpty)

      val unknownV1Field = JsonParser.parseString(LegacyGsonV1UnclaimedActiveGolden).getAsJsonObject
      unknownV1Field.getAsJsonArray("states").get(0).getAsJsonObject.addProperty("unknown", true)
      Files.writeString(file.toPath, new Gson().toJson(unknownV1Field), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
      assertTrue(target.findAll().get.isEmpty)

      val duplicatedV1Field = LegacyGsonV1UnclaimedActiveGolden.replace(
        "\"uuid\":\"00000000-0000-0000-0000-000000000057\",\"state_version\":0",
        "\"uuid\":\"00000000-0000-0000-0000-000000000057\",\"uuid\":\"00000000-0000-0000-0000-000000000057\",\"state_version\":0"
      )
      Files.writeString(file.toPath, duplicatedV1Field, StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
      assertTrue(target.findAll().get.isEmpty)

      val v2Source = new SQLiteFloatingPodRepository(":memory:")
      try
        v2Source.insert(sampleState("00000000-0000-0000-0000-00000000005b")).get
        new JsonImportExport(v2Source).exportToFile(file).get
      finally v2Source.close()
      val missingV2Owner = JsonParser.parseString(Files.readString(file.toPath, StandardCharsets.UTF_8)).getAsJsonObject
      missingV2Owner.getAsJsonArray("states").get(0).getAsJsonObject.remove("owner_uuid")
      missingV2Owner.addProperty("content_hash", recomputeV2Hash(missingV2Owner))
      Files.writeString(file.toPath, new Gson().toJson(missingV2Owner), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
      assertTrue(target.findAll().get.isEmpty)
    finally Files.deleteIfExists(file.toPath)
  }

  @Test
  def rejectsV2ModeAndHashTamperingBeforeWritingAndRejectsInvalidUnclaimedActiveMode(): Unit =
    val file = Files.createTempFile("floatingpod-mode-tamper-", ".json").toFile
    val database = Files.createTempDirectory("floatingpod-mode-tamper").resolve("target.db")
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000053")
    try
      val source = new SQLiteFloatingPodRepository(":memory:")
      try
        source.insert(sampleState(uuid.toString, operatingMode = PodOperatingMode.WorldIdle)).get
        new JsonImportExport(source).exportToFile(file).get
      finally source.close()
      val validContent = Files.readString(file.toPath, StandardCharsets.UTF_8)

      val target = new SQLiteFloatingPodRepository(database.toString)
      try
        val modeTamper = JsonParser.parseString(validContent).getAsJsonObject
        modeTamper.getAsJsonArray("states").get(0).getAsJsonObject.addProperty("operating_mode", "Normal")
        Files.writeString(file.toPath, new Gson().toJson(modeTamper), StandardCharsets.UTF_8)
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
        assertTrue(target.findAll().get.isEmpty)

        val hashTamper = JsonParser.parseString(validContent).getAsJsonObject
        hashTamper.addProperty("content_hash", "0" * 64)
        Files.writeString(file.toPath, new Gson().toJson(hashTamper), StandardCharsets.UTF_8)
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
        assertTrue(target.findAll().get.isEmpty)

        val invalidUnclaimedActive = JsonParser.parseString(validContent).getAsJsonObject
        val invalidState = invalidUnclaimedActive.getAsJsonArray("states").get(0).getAsJsonObject
        invalidState.add("owner_uuid", com.google.gson.JsonNull.INSTANCE)
        invalidState.addProperty("is_active", true)
        invalidState.addProperty("operating_mode", "Normal")
        invalidUnclaimedActive.addProperty("content_hash", recomputeV2Hash(invalidUnclaimedActive))
        val serializeNullsGson = new GsonBuilder().serializeNulls().create()
        val invalidUnclaimedActiveJson = serializeNullsGson.toJson(invalidUnclaimedActive)
        val serializedInvalidState = JsonParser.parseString(invalidUnclaimedActiveJson).getAsJsonObject.getAsJsonArray("states").get(0).getAsJsonObject
        assertTrue(serializedInvalidState.has("owner_uuid"))
        assertTrue(serializedInvalidState.get("owner_uuid").isJsonNull)
        Files.writeString(file.toPath, invalidUnclaimedActiveJson, StandardCharsets.UTF_8)
        val invalidUnclaimedActiveFailure = failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false))
        assertEquals("Active unclaimed migration state must use WorldIdle", invalidUnclaimedActiveFailure.getMessage)
        assertTrue(target.findAll().get.isEmpty)

        val missingMode = JsonParser.parseString(validContent).getAsJsonObject
        missingMode.getAsJsonArray("states").get(0).getAsJsonObject.remove("operating_mode")
        missingMode.addProperty("content_hash", recomputeV2Hash(missingMode))
        Files.writeString(file.toPath, new Gson().toJson(missingMode), StandardCharsets.UTF_8)
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
        assertTrue(target.findAll().get.isEmpty)

        val unknownMode = JsonParser.parseString(validContent).getAsJsonObject
        unknownMode.getAsJsonArray("states").get(0).getAsJsonObject.addProperty("operating_mode", "Unknown")
        unknownMode.addProperty("content_hash", recomputeV2Hash(unknownMode))
        Files.writeString(file.toPath, new Gson().toJson(unknownMode), StandardCharsets.UTF_8)
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false)).isInstanceOf[IllegalArgumentException])
        assertTrue(target.findAll().get.isEmpty)
      finally target.close()
    finally Files.deleteIfExists(file.toPath)

  /** 验证冲突默认拒绝且已经处理的前置记录会随事务一并回滚。 */
  @Test
  def rejectsConflictsAtomically(): Unit =
    val file = Files.createTempFile("floatingpod-conflict-", ".json").toFile
    val newUuid = UUID.fromString("00000000-0000-0000-0000-000000000001")
    val conflictUuid = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")
    try
      withRepository { source =>
        source.insert(sampleState(newUuid.toString)).get
        source.insert(sampleState(conflictUuid.toString, worldName = "source")).get
        new JsonImportExport(source).exportToFile(file).get
      }
      withRepository { target =>
        target.insert(sampleState(conflictUuid.toString, worldName = "target")).get
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = false))
          .isInstanceOf[IllegalStateException])
        assertEquals(None, target.findByUuid(newUuid).get)
        assertEquals("target", target.findByUuid(conflictUuid).get.get.worldName)
      }
    finally Files.deleteIfExists(file.toPath)

  /** 验证导入即使被显式允许替换，也绝不删除已有活动记录。 */
  @Test
  def rejectsReplacingActiveConflict(): Unit =
    val file = Files.createTempFile("floatingpod-replace-", ".json").toFile
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000102")
    try
      withRepository { source =>
        persistActive(source, sampleState(uuid.toString, worldName = "replacement", active = true), 5)
        new JsonImportExport(source).exportToFile(file).get
      }
      withRepository { target =>
        persistActive(target, sampleState(uuid.toString, worldName = "existing", active = true), 5)
        assertTrue(failureOf(new JsonImportExport(target).importFromFile(file, replaceOnConflict = true))
          .isInstanceOf[IllegalStateException])
        val result = target.findByUuid(uuid).get.get
        assertEquals("existing", result.worldName)
        assertTrue(result.isActive)
      }
    finally Files.deleteIfExists(file.toPath)

  /** 验证显式 replace 可原子替换非活动冲突，并始终将导入记录收回且清除维度。 */
  @Test
  def replacesInactiveConflictAsRecalledState(): Unit =
    val file = Files.createTempFile("floatingpod-inactive-replace-", ".json").toFile
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000105")
    try
      withRepository { source =>
        persistActive(source, sampleState(uuid.toString, worldName = "replacement", active = true).copy(dimensionId = Some(6)), 6)
        new JsonImportExport(source).exportToFile(file).get
      }
      withRepository { target =>
        target.insert(sampleState(uuid.toString, worldName = "existing", active = false).copy(dimensionId = Some(3))).get
        assertEquals(1, new JsonImportExport(target).importFromFile(file, replaceOnConflict = true).get)
        val imported = target.findByUuid(uuid).get.get
        assertEquals("replacement", imported.worldName)
        assertFalse(imported.isActive)
        assertEquals(None, imported.dimensionId)
      }
    finally Files.deleteIfExists(file.toPath)

  /** 验证 schema 版本不匹配在开始事务前明确失败。 */
  @Test
  def rejectsSchemaVersionMismatch(): Unit = withRepository { repository =>
    val file = exportOneState(repository, "00000000-0000-0000-0000-000000000103")
    try
      val root = JsonParser.parseString(Files.readString(file.toPath, StandardCharsets.UTF_8)).getAsJsonObject
      root.addProperty("schema_version", 3)
      Files.writeString(file.toPath, new Gson().toJson(root), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(repository).importFromFile(file, false))
        .isInstanceOf[IllegalArgumentException])
    finally Files.deleteIfExists(file.toPath)
  }

  /** 验证被篡改哈希、未知字段与重复字段在写入前均被严格拒绝。 */
  @Test
  def rejectsTamperedHashUnknownAndDuplicateFields(): Unit = withRepository { repository =>
    val file = exportOneState(repository, "00000000-0000-0000-0000-000000000104")
    try
      val validContent = Files.readString(file.toPath, StandardCharsets.UTF_8)
      val root = JsonParser.parseString(validContent).getAsJsonObject
      root.addProperty("content_hash", "0" * 64)
      Files.writeString(file.toPath, new Gson().toJson(root), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(repository).importFromFile(file, false))
        .isInstanceOf[IllegalArgumentException])

      Files.writeString(file.toPath, validContent, StandardCharsets.UTF_8)
      root.addProperty("unexpected", true)
      Files.writeString(file.toPath, new Gson().toJson(root), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(repository).importFromFile(file, false))
        .isInstanceOf[IllegalArgumentException])

      Files.writeString(file.toPath, "{\"schema_version\":1," + validContent.drop(1), StandardCharsets.UTF_8)
      assertTrue(failureOf(new JsonImportExport(repository).importFromFile(file, false))
        .isInstanceOf[IllegalArgumentException])
    finally Files.deleteIfExists(file.toPath)
  }

  private def withRepository(body: SQLiteFloatingPodRepository => Unit): Unit =
    val repository = new SQLiteFloatingPodRepository(":memory:")
    try body(repository)
    finally repository.close()

  private def exportOneState(repository: SQLiteFloatingPodRepository, uuid: String): File =
    val file = Files.createTempFile("floatingpod-schema-", ".json").toFile
    repository.insert(sampleState(uuid)).get
    new JsonImportExport(repository).exportToFile(file).get
    file

  private def failureOf(result: Try[?]): Throwable =
    result match
      case Failure(error) => error
      case _ => throw new AssertionError("Expected a failed Try")

  private def recomputeV2Hash(root: JsonObject): String =
    val canonical = new JsonObject()
    canonical.addProperty("schema_version", root.get("schema_version").getAsLong)
    canonical.add("states", root.getAsJsonArray("states").deepCopy())
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(new GsonBuilder().serializeNulls().create().toJson(canonical).getBytes(StandardCharsets.UTF_8)))

  private def persistActiveUnclaimedWithLocator(
      repository: SQLiteFloatingPodRepository,
      database: java.nio.file.Path,
      inactive: FloatingPodState,
      bindingUuid: UUID,
      attemptUuid: UUID,
      dimensionId: Int
  ): FloatingPodState =
    val chunks = (for chunkX <- -2 to 2; chunkZ <- -2 to 2 yield PhysicalRecoveryChunk(dimensionId, chunkX, chunkZ)).toSet
    repository.createOrRetainLegacyUncertifiedBinding(bindingUuid, 1).get
    repository.certifyPhysicalRecoveryBinding(bindingUuid, 1).get
    val persisted = repository.insert(inactive.copy(isActive = false, dimensionId = None)).get
    repository.prepareCreateLocator(persisted.uuid, persisted.stateVersion, attemptUuid, dimensionId, chunks).get

    // The regular owned activation path cannot create an unclaimed row; establish this existing legacy ledger fixture atomically in real SQLite.
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${database.toAbsolutePath}")
    try
      connection.setAutoCommit(false)
      val activate = connection.prepareStatement(
        "UPDATE floating_pod_state SET is_active = 1, dimension_id = ?, state_version = state_version + 1, last_updated = last_updated + 1 WHERE uuid = ? AND owner_uuid IS NULL AND state_version = ? AND is_active = 0 AND operating_mode = 'WORLD_IDLE'"
      )
      try
        activate.setInt(1, dimensionId)
        activate.setBytes(2, uuidBytes(persisted.uuid))
        activate.setLong(3, persisted.stateVersion)
        assertEquals(1, activate.executeUpdate())
      finally activate.close()
      val activateLocator = connection.prepareStatement(
        "UPDATE physical_recovery_locator SET locator_state = 'ROW_ACTIVE', reason_code = 'row_active', domain_version = ? WHERE pod_uuid = ? AND attempt_uuid = ? AND locator_state = 'CREATE_PREPARED' AND reason_code = 'create_prepared' AND domain_version = ? AND target_dimension_id = ?"
      )
      try
        activateLocator.setLong(1, persisted.stateVersion + 1L)
        activateLocator.setBytes(2, uuidBytes(persisted.uuid))
        activateLocator.setBytes(3, uuidBytes(attemptUuid))
        activateLocator.setLong(4, persisted.stateVersion)
        activateLocator.setInt(5, dimensionId)
        assertEquals(1, activateLocator.executeUpdate())
      finally activateLocator.close()
      connection.commit()
    catch
      case failure: Throwable =>
        try connection.rollback()
        catch case rollbackFailure: Throwable => failure.addSuppressed(rollbackFailure)
        throw failure
    finally connection.close()
    repository.findByUuid(persisted.uuid).get.get

  private def uuidBytes(uuid: UUID): Array[Byte] =
    ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()

  /** Reconstructs the old v1 JsonObject encoder using Gson's actual null-omission behavior. */
  private def legacyGsonV1UnclaimedActivePackage(): String =
    val gson = new Gson()
    val state = new JsonObject()
    state.addProperty("uuid", "00000000-0000-0000-0000-000000000057")
    state.add("owner_uuid", com.google.gson.JsonNull.INSTANCE)
    state.addProperty("state_version", 0L)
    state.addProperty("world_name", "overworld")
    state.addProperty("pos_x", 1.0D)
    state.addProperty("pos_y", 64.0D)
    state.addProperty("pos_z", 2.0D)
    state.addProperty("yaw", 0.0F)
    state.addProperty("pitch", 0.0F)
    state.addProperty("is_active", true)
    state.addProperty("last_updated", 100L)
    val states = new com.google.gson.JsonArray()
    states.add(state)
    val canonical = new JsonObject()
    canonical.addProperty("schema_version", 1L)
    canonical.add("states", states.deepCopy())
    val hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(gson.toJson(canonical).getBytes(StandardCharsets.UTF_8)))
    val root = new JsonObject()
    root.addProperty("schema_version", 1L)
    root.addProperty("exported_at", 0L)
    root.addProperty("content_hash", hash)
    root.add("states", states)
    gson.toJson(root)

  private val LegacyGsonV1UnclaimedActiveGolden =
    """{"schema_version":1,"exported_at":0,"content_hash":"abf6fa8c9d0418d8b5868b13f64ab15c2b79a4964b89b45b68ab52e61e98ea32","states":[{"uuid":"00000000-0000-0000-0000-000000000057","state_version":0,"world_name":"overworld","pos_x":1.0,"pos_y":64.0,"pos_z":2.0,"yaw":0.0,"pitch":0.0,"is_active":true,"last_updated":100}]}"""

  private def sampleState(
      uuid: String,
      worldName: String = "overworld",
      active: Boolean = false,
      operatingMode: PodOperatingMode = PodOperatingMode.Normal,
      ownerUuid: Option[UUID] = Some(UUID.fromString("11111111-1111-1111-1111-111111111111"))
  ): FloatingPodState =
    FloatingPodState(
      uuid = UUID.fromString(uuid),
      ownerUuid = ownerUuid,
      stateVersion = 0L,
      worldName = worldName,
      posX = 1.0D,
      posY = 64.0D,
      posZ = 2.0D,
      yaw = 0.0F,
      pitch = 0.0F,
      isActive = active,
      lastUpdated = 100L,
      dimensionId = if active then Some(0) else None,
      operatingMode = operatingMode
    )

  private def persistActive(repository: SQLiteFloatingPodRepository, state: FloatingPodState, dimensionId: Int): FloatingPodState =
    val inactive = state.copy(isActive = false, dimensionId = None)
    repository.insert(inactive).get
    repository.activateIfAdmissible(inactive, inactive.ownerUuid.get, inactive.stateVersion, dimensionId, 256).get
      .asInstanceOf[AdmissionOutcome.Accepted].state
