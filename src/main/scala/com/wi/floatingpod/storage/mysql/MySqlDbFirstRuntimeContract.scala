package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata

/** schema6 独立文件/语义入口；共用有界解析器，不转换为 schema5 或 native Plan。 */
private[mysql] object MySqlDbFirstRuntimeContract:
  val Format = "FP_MYSQL_DB_FIRST1_RUNTIME_CONTRACT_V1"
  val Encoding = MySqlRuntimeMetadata.DB_FIRST_MANIFEST
  val RecipeEncoding = "FP_MYSQL_DB_FIRST1_FRESH_RECIPE_V1"
  def decodeRecipe(bytes: Array[Byte]): MySqlDeploymentReceiptRuntimeContract.Recipe =
    MySqlDeploymentReceiptRuntimeContract.decodeRecipeFor(bytes, MySqlDeploymentReceiptRuntimeContract.DbFirstFamily)
  private[mysql] def load(metadata: MySqlRuntimeMetadata.DbFirstStatic,
      baseline: MySqlApprovedStaticBaseline, owner: MySqlRuntimeInputs.AssemblyOwner): MySqlDeploymentReceiptRuntimeContract.Loaded =
    MySqlDeploymentReceiptRuntimeContract.loadDbFirst(metadata, baseline, owner)

  private[mysql] final case class HistorySource(path: java.nio.file.Path, sha256: String)
  private[mysql] def validateHistory(value: com.google.gson.JsonObject): Option[HistorySource] =
    import scala.jdk.CollectionConverters.*
    def invalid(): Nothing = throw new java.sql.SQLDataException("schema6 historicalSchema5 requires a closed explicit source")
    if value == null || !value.has("kind") || !value.get("kind").isJsonPrimitive || !value.getAsJsonPrimitive("kind").isString then invalid()
    value.get("kind").getAsString match
      case "NONE" =>
        if value.keySet().asScala.toSet != Set("kind") then invalid()
        None // 未提供来源不构成没有历史的证明。
      case "SCHEMA5" =>
        if value.keySet().asScala.toSet != Set("kind","metadataPath","metadataSha256") then invalid()
        def text(key: String): String =
          val item=value.get(key)
          if item == null || !item.isJsonPrimitive || !item.getAsJsonPrimitive.isString then invalid()
          item.getAsString
        val name=text("metadataPath"); val digest=text("metadataSha256")
        if name.isBlank || !digest.matches("[0-9a-f]{64}") then invalid()
        val path=java.nio.file.Path.of(name)
        if !path.isAbsolute then invalid()
        Some(HistorySource(path,digest))
      case _ => invalid()
