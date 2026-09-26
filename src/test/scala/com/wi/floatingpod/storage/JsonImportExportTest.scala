package com.wi.floatingpod.storage

import com.google.gson.Gson
import com.google.gson.JsonParser
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.storage.sqlite.SQLiteFloatingPodRepository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
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
      assertEquals(1L, root.get("schema_version").getAsLong)
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
      root.addProperty("schema_version", 2)
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

  private def sampleState(
      uuid: String,
      worldName: String = "overworld",
      active: Boolean = false
  ): FloatingPodState =
    FloatingPodState(
      uuid = UUID.fromString(uuid),
      ownerUuid = Some(UUID.fromString("11111111-1111-1111-1111-111111111111")),
      stateVersion = 0L,
      worldName = worldName,
      posX = 1.0D,
      posY = 64.0D,
      posZ = 2.0D,
      yaw = 0.0F,
      pitch = 0.0F,
      isActive = active,
      lastUpdated = 100L,
      dimensionId = if active then Some(0) else None
    )

  private def persistActive(repository: SQLiteFloatingPodRepository, state: FloatingPodState, dimensionId: Int): FloatingPodState =
    val inactive = state.copy(isActive = false, dimensionId = None)
    repository.insert(inactive).get
    repository.activateIfAdmissible(inactive, inactive.ownerUuid.get, inactive.stateVersion, dimensionId, 256).get
      .asInstanceOf[AdmissionOutcome.Accepted].state
