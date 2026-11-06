package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import java.nio.charset.StandardCharsets
import java.sql.SQLDataException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.jdk.CollectionConverters.*

/** 检查真实 direct SELECT transport 校准；历史 CALL 校准不能替代当前路径，也不授予 baseline 资格。 */
class MySqlManagedJdbcMetadataTest:
  private def calibrated(resource: String = "/db/mysql/managed-direct-static-jdbc-profile-v1.json"): Vector[MySqlAuditResultSet] =
    val input = getClass.getResourceAsStream(resource)
    assertNotNull(input)
    val json = try JsonParser.parseString(new String(input.readAllBytes(), StandardCharsets.UTF_8)).getAsJsonObject
      finally input.close()
    json.getAsJsonArray("sets").asScala.toVector.map { entry =>
      val set = entry.getAsJsonObject
      val columns = set.getAsJsonArray("columns").asScala.toVector.map { item =>
        val c = item.getAsJsonObject
        MySqlAuditColumn(c.get("label").getAsString, c.get("jdbcType").getAsInt,
          c.get("typeName").getAsString, c.get("precision").getAsInt, c.get("scale").getAsInt,
          c.get("nullable").getAsInt, c.get("signed").getAsBoolean)
      }
      MySqlAuditResultSet(set.get("id").getAsString, columns, Vector.empty)
    }

  @Test def acceptsAll123ActualColumnDescriptionsAndRejectsEveryChangedMetadataField(): Unit =
    val profile = DeploymentReceiptStaticTestData.defaultProfile().sourceLimitProfile
    val root = MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile,
      profile.maxSqlPacketBytes(), new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    try
      val sets = calibrated()
      assertEquals(123, sets.map(_.columns.size).sum)
      MySqlManagedJdbcMetadata.validate(sets, root)
      val first = sets.head.columns.head
      val changes = Vector(first.copy(label = first.label + "_changed"),
        first.copy(jdbcType = first.jdbcType + 1), first.copy(typeName = first.typeName + "_changed"),
        first.copy(precision = first.precision + 1), first.copy(scale = first.scale + 1),
        first.copy(nullable = (first.nullable + 1) % 3), first.copy(signed = !first.signed))
      changes.foreach { changed =>
        val altered = sets.updated(0, sets.head.copy(columns = sets.head.columns.updated(0, changed)))
        assertThrows(classOf[SQLDataException], () => MySqlManagedJdbcMetadata.validate(altered, root))
        assertEquals(0L, root.accountedMemoryBytes)
      }
      assertThrows(classOf[SQLDataException], () => MySqlManagedJdbcMetadata.validate(sets.reverse, root))
      assertEquals(0L, root.accountedMemoryBytes)
    finally root.closeRootReadBudget()

  @Test def historicalCallCalibrationCannotApproveCurrentDirectSelectMetadata(): Unit =
    val profile = DeploymentReceiptStaticTestData.defaultProfile().sourceLimitProfile
    val root = MySqlFormat3ReadBudget.rootOnLedger(profile, profile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    try
      val direct = calibrated()
      val historical = calibrated("/db/mysql/managed-static-jdbc-profile-v1.json")
      assertEquals(15, direct.zip(historical).flatMap((a, b) => a.columns.zip(b.columns)).count((a, b) => a != b))
      assertEquals(6144, direct.head.columns(8).precision)
      assertEquals(2048, historical.head.columns(8).precision)
      assertThrows(classOf[SQLDataException], () => MySqlManagedJdbcMetadata.validate(historical, root))
      assertEquals(0L, root.accountedMemoryBytes)
    finally root.closeRootReadBudget()
