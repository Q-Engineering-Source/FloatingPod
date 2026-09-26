package com.wi.floatingpod.storage

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.wi.floatingpod.domain.FloatingPodState

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

import java.io.File
import java.io.StringReader
import java.lang.Double
import java.lang.Float
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.HexFormat
import java.util.Objects
import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * 定义管理员显式迁移包的导入和导出边界，使迁移数据不参与正常运行时写入。
 */
trait ImportExportService:
  /**
   * 将全部恢复记录导出为含 schema 与内容哈希的 JSON 迁移包。
   *
   * @param file 目标 JSON 文件。
   * @return 成功时返回实际写入的文件。
   */
  def exportToFile(file: File): Try[File]

  /**
   * 严格验证 JSON 迁移包后在一次事务中导入，默认拒绝 UUID 冲突。
   *
   * @param file 待导入 JSON 文件。
   * @param replaceOnConflict 是否显式允许替换已有 UUID。
   * @return 成功时返回导入记录数。
   */
  def importFromFile(file: File, replaceOnConflict: Boolean): Try[Int]

/**
 * 使用 Gson 编解码版本化迁移包，并将格式验证和数据库提交分离以保持导入原子性。
 *
 * @param repository 提供事务和状态读写的恢复存储。
 */
final class JsonImportExport(repository: FloatingPodRepository) extends ImportExportService:
  require(repository != null, "repository must not be null")

  import JsonImportExport.*

  private val logger: Logger = LogManager.getLogger("floatingpod-storage")
  private val gson = new Gson()

  /**
   * 导出全部状态和确定性内容哈希，调用方可在传输后验证迁移包内容未被修改。
   */
  override def exportToFile(file: File): Try[File] =
    guard("export", file) {
      Objects.requireNonNull(file, "file must not be null")
      val states = repository.findAll().get
      val root = new JsonObject()
      root.addProperty("schema_version", CurrentSchemaVersion)
      root.addProperty("exported_at", System.currentTimeMillis())
      root.addProperty("content_hash", contentHash(states))
      root.add("states", encodeStates(states))
      Files.writeString(
        file.toPath,
        gson.toJson(root),
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
      )
      file
    }

  /**
   * 在开始数据库事务前校验完整 JSON 包；成功导入后所有状态均保持收回状态。
   */
  override def importFromFile(file: File, replaceOnConflict: Boolean): Try[Int] =
    guard("import", file) {
      Objects.requireNonNull(file, "file must not be null")
      val states = parseAndValidate(file)
      repository.transaction {
        states.foreach { imported =>
          val state = imported.toRecalledState
          repository.findByUuid(state.uuid).get match
            case Some(_) if !replaceOnConflict =>
              throw new IllegalStateException(s"Import conflicts with existing UUID ${state.uuid}")
            case Some(existing) if existing.isActive =>
              throw new IllegalStateException(s"Import must not replace active UUID ${state.uuid}")
            case Some(_) =>
              if !repository.delete(state.uuid).get then
                throw new IllegalStateException(s"Import replacement could not delete UUID ${state.uuid}")
            case None => ()
          val recalledState = state.copy(isActive = false, dimensionId = None)
          repository.insert(recalledState).get
        }
      }.get
      states.size
    }

  private def guard[T](operation: String, file: File)(fn: => T): Try[T] =
    Try(fn) match
      case success @ Success(_) => success
      case Failure(exception) =>
        val path = Option(file).map(_.getAbsolutePath).getOrElse("N/A")
        logger.error(s"SQLite migration package $operation failed: file=$path", exception)
        Failure(exception)

  private def parseAndValidate(file: File): List[MigrationState] =
    val content = Files.readString(file.toPath, StandardCharsets.UTF_8)
    val root = parseStrictJson(content)
    if !root.isJsonObject then
      throw new IllegalArgumentException("Migration package root must be a JSON object")
    val objectRoot = root.getAsJsonObject
    requireExactFields(objectRoot, RootFields, "migration package")
    requireLong(objectRoot, "schema_version", "migration package") match
      case CurrentSchemaVersion => ()
      case version => throw new IllegalArgumentException(s"Unsupported migration schema_version: $version")
    requireLong(objectRoot, "exported_at", "migration package") match
      case value if value >= 0L => ()
      case _ => throw new IllegalArgumentException("exported_at must not be negative")
    val suppliedHash = requireString(objectRoot, "content_hash", "migration package")
    if !suppliedHash.matches("[0-9a-f]{64}") then
      throw new IllegalArgumentException("content_hash must be a lowercase SHA-256 hex string")
    val statesElement = objectRoot.get("states")
    if !statesElement.isJsonArray then
      throw new IllegalArgumentException("states must be a JSON array")
    val states = statesElement.getAsJsonArray.asScala.toList.map(decodeState)
    if states.map(_.uuid).distinct.size != states.size then
      throw new IllegalArgumentException("Migration package contains duplicate UUID values")
    val computedHash = contentHashImported(states)
    if !MessageDigest.isEqual(
        suppliedHash.getBytes(StandardCharsets.US_ASCII),
        computedHash.getBytes(StandardCharsets.US_ASCII)
      )
    then throw new IllegalArgumentException("Migration package content_hash does not match states")
    states

  private def parseStrictJson(content: String): JsonElement =
    val reader = new JsonReader(new StringReader(content))
    try
      val root = readStrictElement(reader)
      if reader.peek() != JsonToken.END_DOCUMENT then
        throw new IllegalArgumentException("Migration package contains trailing JSON content")
      root
    finally reader.close()

  private def readStrictElement(reader: JsonReader): JsonElement =
    reader.peek() match
      case JsonToken.BEGIN_OBJECT =>
        reader.beginObject()
        val objectValue = new JsonObject()
        val fieldNames = mutable.Set.empty[String]
        while reader.hasNext() do
          val fieldName = reader.nextName()
          if !fieldNames.add(fieldName) then
            throw new IllegalArgumentException(s"Migration package contains duplicate JSON field $fieldName")
          objectValue.add(fieldName, readStrictElement(reader))
        reader.endObject()
        objectValue
      case JsonToken.BEGIN_ARRAY =>
        reader.beginArray()
        val array = new JsonArray()
        while reader.hasNext() do array.add(readStrictElement(reader))
        reader.endArray()
        array
      case JsonToken.STRING => new JsonPrimitive(reader.nextString())
      case JsonToken.NUMBER => new JsonPrimitive(new BigDecimal(reader.nextString()))
      case JsonToken.BOOLEAN => new JsonPrimitive(reader.nextBoolean())
      case JsonToken.NULL =>
        reader.nextNull()
        JsonNull.INSTANCE
      case token => throw new IllegalArgumentException(s"Unexpected JSON token in migration package: $token")

  private def encodeStates(states: List[FloatingPodState]): JsonArray =
    val array = new JsonArray()
    states.sortBy(_.uuid.toString).foreach(state => array.add(encodeState(state)))
    array

  private def encodeState(state: FloatingPodState): JsonObject =
    val json = new JsonObject()
    json.addProperty("uuid", state.uuid.toString)
    state.ownerUuid match
      case Some(ownerUuid) => json.addProperty("owner_uuid", ownerUuid.toString)
      case None => json.add("owner_uuid", JsonNull.INSTANCE)
    json.addProperty("state_version", state.stateVersion)
    json.addProperty("world_name", state.worldName)
    json.addProperty("pos_x", state.posX)
    json.addProperty("pos_y", state.posY)
    json.addProperty("pos_z", state.posZ)
    json.addProperty("yaw", state.yaw)
    json.addProperty("pitch", state.pitch)
    json.addProperty("is_active", state.isActive)
    json.addProperty("last_updated", state.lastUpdated)
    json

  private def decodeState(element: JsonElement): MigrationState =
    if !element.isJsonObject then
      throw new IllegalArgumentException("Each migration state must be a JSON object")
    val state = element.getAsJsonObject
    requireExactFields(state, StateFields, "migration state")
    val owner = state.get("owner_uuid") match
      case value if value.isJsonNull => None
      case value => Some(UUID.fromString(requireString(value, "owner_uuid", "migration state")))
    MigrationState(
      uuid = UUID.fromString(requireString(state, "uuid", "migration state")),
      ownerUuid = owner,
      stateVersion = requireNonNegativeLong(state, "state_version", "migration state"),
      worldName = requireString(state, "world_name", "migration state"),
      posX = requireFiniteDouble(state, "pos_x", "migration state"),
      posY = requireFiniteDouble(state, "pos_y", "migration state"),
      posZ = requireFiniteDouble(state, "pos_z", "migration state"),
      yaw = requireFiniteFloat(state, "yaw", "migration state"),
      pitch = requireFiniteFloat(state, "pitch", "migration state"),
      isActive = requireBoolean(state, "is_active", "migration state"),
      lastUpdated = requireNonNegativeLong(state, "last_updated", "migration state")
    )

  private def contentHash(states: List[FloatingPodState]): String =
    val canonical = new JsonObject()
    canonical.addProperty("schema_version", CurrentSchemaVersion)
    canonical.add("states", encodeStates(states))
    hashCanonical(canonical)

  private def contentHashImported(states: List[MigrationState]): String =
    val canonical = new JsonObject()
    canonical.addProperty("schema_version", CurrentSchemaVersion)
    canonical.add("states", encodeImportedStates(states))
    hashCanonical(canonical)

  private def hashCanonical(canonical: JsonObject): String =
    val digest = MessageDigest.getInstance("SHA-256")
    HexFormat.of().formatHex(digest.digest(gson.toJson(canonical).getBytes(StandardCharsets.UTF_8)))

  private def encodeImportedStates(states: List[MigrationState]): JsonArray =
    val array = new JsonArray()
    states.sortBy(_.uuid.toString).foreach(state => array.add(encodeImportedState(state)))
    array

  private def encodeImportedState(state: MigrationState): JsonObject =
    val json = new JsonObject()
    json.addProperty("uuid", state.uuid.toString)
    state.ownerUuid match
      case Some(ownerUuid) => json.addProperty("owner_uuid", ownerUuid.toString)
      case None => json.add("owner_uuid", JsonNull.INSTANCE)
    json.addProperty("state_version", state.stateVersion)
    json.addProperty("world_name", state.worldName)
    json.addProperty("pos_x", state.posX)
    json.addProperty("pos_y", state.posY)
    json.addProperty("pos_z", state.posZ)
    json.addProperty("yaw", state.yaw)
    json.addProperty("pitch", state.pitch)
    json.addProperty("is_active", state.isActive)
    json.addProperty("last_updated", state.lastUpdated)
    json

  private def requireExactFields(objectValue: JsonObject, expected: Set[String], context: String): Unit =
    val actual = objectValue.keySet().asScala.toSet
    if actual != expected then
      throw new IllegalArgumentException(s"$context fields do not match the required schema")

  private def requireString(objectValue: JsonObject, name: String, context: String): String =
    requireString(requiredField(objectValue, name, context), name, context)

  private def requireString(value: JsonElement, name: String, context: String): String =
    if !value.isJsonPrimitive || !value.getAsJsonPrimitive.isString then
      throw new IllegalArgumentException(s"$context field $name must be a string")
    val result = value.getAsString
    if result.trim.isEmpty then
      throw new IllegalArgumentException(s"$context field $name must not be blank")
    result

  private def requireLong(objectValue: JsonObject, name: String, context: String): Long =
    val value = requiredField(objectValue, name, context)
    if !value.isJsonPrimitive || !value.getAsJsonPrimitive.isNumber then
      throw new IllegalArgumentException(s"$context field $name must be an integer")
    value.getAsBigDecimal.longValueExact()

  private def requireNonNegativeLong(objectValue: JsonObject, name: String, context: String): Long =
    val value = requireLong(objectValue, name, context)
    if value < 0L then
      throw new IllegalArgumentException(s"$context field $name must not be negative")
    value

  private def requireFiniteDouble(objectValue: JsonObject, name: String, context: String): Double =
    val value = requiredField(objectValue, name, context)
    if !value.isJsonPrimitive || !value.getAsJsonPrimitive.isNumber then
      throw new IllegalArgumentException(s"$context field $name must be numeric")
    val result = value.getAsDouble
    if !Double.isFinite(result) then
      throw new IllegalArgumentException(s"$context field $name must be finite")
    result

  private def requireFiniteFloat(objectValue: JsonObject, name: String, context: String): Float =
    val value = requireFiniteDouble(objectValue, name, context).toFloat
    if !Float.isFinite(value) then
      throw new IllegalArgumentException(s"$context field $name must be a finite float")
    value

  private def requireBoolean(objectValue: JsonObject, name: String, context: String): Boolean =
    val value = requiredField(objectValue, name, context)
    if !value.isJsonPrimitive || !value.getAsJsonPrimitive.isBoolean then
      throw new IllegalArgumentException(s"$context field $name must be boolean")
    value.getAsBoolean

  private def requiredField(objectValue: JsonObject, name: String, context: String): JsonElement =
    Option(objectValue.get(name)).getOrElse {
      throw new IllegalArgumentException(s"$context is missing field $name")
    }

private[storage] object JsonImportExport:
  private[storage] val CurrentSchemaVersion = 1L
  private[storage] val RootFields = Set("schema_version", "exported_at", "content_hash", "states")
  private[storage] val StateFields = Set(
    "uuid",
    "owner_uuid",
    "state_version",
    "world_name",
    "pos_x",
    "pos_y",
    "pos_z",
    "yaw",
    "pitch",
    "is_active",
    "last_updated"
  )

  private final case class MigrationState(
      uuid: UUID,
      ownerUuid: Option[UUID],
      stateVersion: Long,
      worldName: String,
      posX: Double,
      posY: Double,
      posZ: Double,
      yaw: Float,
      pitch: Float,
      isActive: Boolean,
      lastUpdated: Long
  ):
    def toRecalledState: FloatingPodState =
      FloatingPodState(uuid, ownerUuid, stateVersion, worldName, posX, posY, posZ, yaw, pitch, false, lastUpdated, None)
