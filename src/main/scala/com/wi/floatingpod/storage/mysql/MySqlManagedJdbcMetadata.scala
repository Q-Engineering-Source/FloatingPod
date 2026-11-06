package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonObject, JsonParser}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.HexFormat
import scala.jdk.CollectionConverters.*

/** 固定发布输入：managed 原始 SELECT 的真实 JDBC 校准，不借用历史 CALL 的编码或对象批准。 */
private[mysql] object MySqlManagedJdbcMetadata:
  private val Resource = "/db/mysql/managed-direct-static-jdbc-profile-v1.json"
  private val ResourceSha256 = "94510741b6381f2e5b31e2e63f16c21a315dd4c322b6209d7c60bd14ef38012a"
  private val RootFields = Set("format", "serverVersion", "sqlSdkSha256", "captureRole", "sets",
    "connectorJarSha256", "observationSha256", "queryPlanSha256", "sessionPlanSha256")
  private val ColumnFields = Set("label", "jdbcType", "typeName", "precision", "scale", "nullable", "signed")

  def validate(sets: Vector[MySqlAuditResultSet], root: MySqlFormat3ReadBudget): Unit =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if sets == null || sets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then
      reject("managed static capture has different ordered result-set identities")
    MySqlManifestConfigurationIdentity.temporary(root, 6L * 1024L * 1024L) {
      val stream = getClass.getResourceAsStream(Resource)
      if stream == null then reject("managed JDBC metadata release calibration is missing")
      var primary: Throwable = null
      val bytes = try
        val value = stream.readNBytes(65537)
        if value.length > 65536 then reject("managed JDBC metadata release calibration exceeds bound")
        value
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try stream.close()
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)
      val hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
      if hash != ResourceSha256 then reject("managed JDBC metadata release calibration SHA-256 differs")
      // 固定资源已逐字 SHA 校验；Gson 在这里不处理用户或数据库提供的 JSON。
      val data = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject
      exactKeys(data, RootFields)
      if data.get("format").getAsString != "FP_MANAGED_DIRECT_STATIC_JDBC_CALIBRATION_V1" ||
          data.get("serverVersion").getAsString != MySqlConnectionSource.SupportedServerVersion ||
          data.get("sqlSdkSha256").getAsString != "848F99FC14941DF52EA52FD51AA75BF13D3556FEEA4CBA2BF665D59A9FB0B5DB" ||
          data.get("connectorJarSha256").getAsString != "69084713593A4AA8D07C383619B9639276F08BCCF8FAF1C562178147D389B1E1" then
        reject("managed JDBC metadata release calibration environment differs")
      val expected = data.getAsJsonArray("sets").asScala.toVector
      if expected.size != sets.size then reject("managed JDBC metadata calibration coverage differs")
      sets.zip(expected).foreach { (actual, entry) =>
        val set = entry.getAsJsonObject
        exactKeys(set, Set("id", "columns"))
        if set.get("id").getAsString != actual.id then reject("managed JDBC metadata set order differs")
        val columns = set.getAsJsonArray("columns").asScala.toVector.map { entry =>
          val value = entry.getAsJsonObject
          exactKeys(value, ColumnFields)
          MySqlAuditColumn(value.get("label").getAsString, value.get("jdbcType").getAsInt,
            value.get("typeName").getAsString, value.get("precision").getAsInt,
            value.get("scale").getAsInt, value.get("nullable").getAsInt, value.get("signed").getAsBoolean)
        }
        if actual.columns != columns then reject("managed JDBC metadata differs from release calibration: " + actual.id)
      }
    }

  private def exactKeys(value: JsonObject, expected: Set[String]): Unit =
    if value.keySet().asScala.toSet != expected then reject("managed JDBC metadata release resource has different fields")
  private def reject(message: String): Nothing = throw new SQLDataException(message)
