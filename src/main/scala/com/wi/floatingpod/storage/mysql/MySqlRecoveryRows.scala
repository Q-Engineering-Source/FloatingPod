package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{ObservedUnboundSourceKind, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, UnboundIntentOrigin}

import java.sql.{Connection, ResultSet, SQLDataException}
import java.util.UUID

/** chunk 表独立行，保留 parent UUID；孤儿不能因组装 locator 而消失。 */
final case class MySqlRecoveryChunkRow(podUuid: UUID, chunk: PhysicalRecoveryChunk)

enum MySqlRecoveryIntentPhase:
  case Prepared, Committed

/** 完整 SQL history，不投影为仅支持 PREPARED 的领域收据；DOUBLE/nullable committed 列原样保留。 */
final case class MySqlRecoveryIntentData(
    podUuid: UUID,
    attemptUuid: UUID,
    bindingUuid: UUID,
    origin: UnboundIntentOrigin,
    phase: MySqlRecoveryIntentPhase,
    sourceKind: Option[ObservedUnboundSourceKind],
    sourceIdentity: Option[String],
    sourceIdentitySha256: Option[Vector[Byte]],
    catalogGeneration: Option[UUID],
    createOperationUuid: Option[UUID],
    preparedDomainVersion: Long,
    targetWorldName: String,
    targetPosX: Double,
    targetPosY: Double,
    targetPosZ: Double,
    targetYaw: Double,
    targetPitch: Double,
    targetDimensionId: Int,
    targetChunkX: Int,
    targetChunkZ: Int,
    committedDomainVersion: Option[Long],
    committedAt: Option[Long],
    committedWorldName: Option[String],
    committedPosX: Option[Double],
    committedPosY: Option[Double],
    committedPosZ: Option[Double],
    committedYaw: Option[Double],
    committedPitch: Option[Double],
    committedDimensionId: Option[Int]
)

/** 分表全读的不可变观察；不是 worldReady、catalog custody 或事务一致性证明。 */
final case class MySqlRecoveryGraph(
    binding: Option[PhysicalRecoveryBinding],
    locators: Vector[PhysicalRecoveryLocator],
    chunks: Vector[MySqlRecoveryChunkRow],
    intents: Vector[MySqlRecoveryIntentData]
):
  /** 调用方须在自己的 admitted snapshot transaction 内读取后，显式验证已知边。 */
  def validateGraph(): Unit = MySqlRecoveryRows.validateGraph(this)

/** 只读调用方已有事务和线程；不 acquire fence，不改变 connection/transaction lifecycle。 */
final class MySqlRecoveryRows(connection: Connection, schema: String):
  require(connection != null, "caller-owned connection is required")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "fixed schema identifier is unsafe")
  private val ownerThread = Thread.currentThread()
  private def table(name: String): String = s"`$schema`.`$name`"

  def readBinding(): Option[PhysicalRecoveryBinding] =
    MySqlBusinessRows.single(query(s"SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM ${table("physical_recovery_binding")} ORDER BY singleton_fixed1")(MySqlRecoveryRows.readBinding))

  /** 此处 chunks 留空；readGraph 另从独立全量 chunk inventory 组装，且仍保留原 chunk rows。 */
  def readAllLocatorRows(): Vector[PhysicalRecoveryLocator] =
    query(s"SELECT pod_uuid,attempt_uuid,locator_state,reason_code,domain_version,target_dimension_id FROM ${table("physical_recovery_locator")} ORDER BY pod_uuid")(MySqlRecoveryRows.readLocator)

  def readAllChunkRows(): Vector[MySqlRecoveryChunkRow] =
    query(s"SELECT pod_uuid,dimension_id,chunk_x,chunk_z FROM ${table("physical_recovery_locator_chunk")} ORDER BY pod_uuid,dimension_id,chunk_x,chunk_z")(MySqlRecoveryRows.readChunk)

  def readAllIntents(): Vector[MySqlRecoveryIntentData] =
    val columns = "pod_uuid,attempt_uuid,binding_uuid,origin,phase,source_kind,source_identity,source_identity_sha256,catalog_generation,create_operation_uuid,prepared_domain_version,target_world_name,target_pos_x,target_pos_y,target_pos_z,target_yaw,target_pitch,target_dimension_id,target_chunk_x,target_chunk_z,committed_domain_version,committed_at,committed_world_name,committed_pos_x,committed_pos_y,committed_pos_z,committed_yaw,committed_pitch,committed_dimension_id"
    query(s"SELECT $columns FROM ${table("physical_recovery_intent")} ORDER BY pod_uuid")(MySqlRecoveryRows.readIntent)

  def readGraph(): MySqlRecoveryGraph =
    val binding = readBinding()
    val parents = readAllLocatorRows()
    val chunks = readAllChunkRows()
    val intents = readAllIntents()
    MySqlRecoveryRows.assembleGraph(binding, parents, chunks, intents)

  private def query[A](sql: String)(decode: ResultSet => A): Vector[A] =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("MySQL recovery read requires its caller-owned thread")
    MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        val rows = Vector.newBuilder[A]
        while result.next() do rows += decode(result)
        rows.result()
      }
    }

object MySqlRecoveryRows:
  private[mysql] def readBinding(row: ResultSet): PhysicalRecoveryBinding =
    if requiredInt(row, 1, "singleton_fixed1") != 1 then invalid("binding singleton key")
    PhysicalRecoveryBinding(requiredUuid(row, 2, "binding_uuid"), PhysicalRecoveryCutoverState.fromDatabase(requiredText(row, 3, "cutover_state")), requiredInt(row, 4, "format_version"))

  private[mysql] def readLocator(row: ResultSet): PhysicalRecoveryLocator =
    PhysicalRecoveryLocator(requiredUuid(row, 1, "pod_uuid"), requiredUuid(row, 2, "attempt_uuid"),
      PhysicalRecoveryLocatorState.fromDatabase(requiredText(row, 3, "locator_state")), requiredText(row, 4, "reason_code"),
      nullableLong(row, 5), nullableInt(row, 6), Nil)

  private[mysql] def readChunk(row: ResultSet): MySqlRecoveryChunkRow =
    MySqlRecoveryChunkRow(requiredUuid(row, 1, "pod_uuid"), PhysicalRecoveryChunk(requiredInt(row, 2, "dimension_id"), requiredInt(row, 3, "chunk_x"), requiredInt(row, 4, "chunk_z")))

  private[mysql] def readIntent(row: ResultSet): MySqlRecoveryIntentData =
    val origin = requiredText(row, 4, "origin") match
      case "UNBOUND_ADOPTION" => UnboundIntentOrigin.ObservedArtifactAdoption
      case "DEBUG_ITEM_CREATE" => UnboundIntentOrigin.DebugItemCreate
      case _ => invalid("intent origin")
    val phase = requiredText(row, 5, "phase") match
      case "PREPARED" => MySqlRecoveryIntentPhase.Prepared
      case "COMMITTED" => MySqlRecoveryIntentPhase.Committed
      case _ => invalid("intent phase")
    val sourceKind = nullableText(row, 6).map {
      case "FRESH" => ObservedUnboundSourceKind.FreshArtifact
      case "LEGACY_UNREGISTERED" => ObservedUnboundSourceKind.LegacyUnregistered
      case _ => invalid("intent source_kind")
    }
    MySqlRecoveryIntentData(requiredUuid(row, 1, "pod_uuid"), requiredUuid(row, 2, "attempt_uuid"), requiredUuid(row, 3, "binding_uuid"),
      origin, phase, sourceKind, nullableText(row, 7), binary(row, 8, "source_identity_sha256", 32),
      nullableUuid(row, 9, "catalog_generation"), nullableUuid(row, 10, "create_operation_uuid"),
      requiredLong(row, 11, "prepared_domain_version"), requiredText(row, 12, "target_world_name"),
      requiredDouble(row, 13, "target_pos_x"), requiredDouble(row, 14, "target_pos_y"), requiredDouble(row, 15, "target_pos_z"),
      requiredDouble(row, 16, "target_yaw"), requiredDouble(row, 17, "target_pitch"), requiredInt(row, 18, "target_dimension_id"),
      requiredInt(row, 19, "target_chunk_x"), requiredInt(row, 20, "target_chunk_z"), nullableLong(row, 21), nullableLong(row, 22),
      nullableText(row, 23), nullableDouble(row, 24), nullableDouble(row, 25), nullableDouble(row, 26), nullableDouble(row, 27),
      nullableDouble(row, 28), nullableInt(row, 29))

  private[mysql] def assembleGraph(binding: Option[PhysicalRecoveryBinding], parents: Vector[PhysicalRecoveryLocator], chunks: Vector[MySqlRecoveryChunkRow], intents: Vector[MySqlRecoveryIntentData]): MySqlRecoveryGraph =
    val byParent = chunks.groupMap(_.podUuid)(_.chunk)
    MySqlRecoveryGraph(binding, parents.map(p => p.copy(chunks = byParent.getOrElse(p.podUuid, Vector.empty).toList)), chunks, intents)

  /** 只检查此 snapshot 可证明的主键、FK 与 intent phase/origin 列组合；不推断世界状态或 domain FK。 */
  private[mysql] def validateGraph(graph: MySqlRecoveryGraph): Unit =
    unique(graph.locators.map(_.podUuid), "locator pod_uuid")
    unique(graph.locators.map(_.attemptUuid), "locator attempt_uuid")
    unique(graph.chunks.map(r => (r.podUuid, r.chunk)), "chunk primary key")
    unique(graph.intents.map(_.podUuid), "intent pod_uuid")
    unique(graph.intents.map(_.attemptUuid), "intent attempt_uuid")
    // MySQL UNIQUE 忽略包含 SQL NULL 的复合键；不可将 None 当成相等的非 NULL 键。
    val observedSourceKeys = graph.intents.flatMap { i =>
      for
        sourceKind <- i.sourceKind
        catalogGeneration <- i.catalogGeneration
        digest <- i.sourceIdentitySha256
      yield (i.bindingUuid, sourceKind, catalogGeneration, digest)
    }
    unique(observedSourceKeys, "intent observed source unique key")
    val createOperationKeys = graph.intents.flatMap(i => i.createOperationUuid.map(operation => (i.bindingUuid, operation)))
    unique(createOperationKeys, "intent create operation unique key")
    val parents = graph.locators.map(_.podUuid).toSet
    if graph.chunks.exists(r => !parents.contains(r.podUuid)) then invalid("orphan chunk parent")
    graph.locators.foreach { p =>
      if p.chunks != graph.chunks.filter(_.podUuid == p.podUuid).map(_.chunk).toList then invalid("locator chunk inventory mismatch")
    }
    graph.intents.foreach { i =>
      if !graph.binding.exists(_.bindingUuid == i.bindingUuid) then invalid("intent binding FK mismatch")
      if i.preparedDomainVersion != 0L then invalid("intent prepared version")
      val adoption = i.sourceKind.nonEmpty && i.sourceIdentity.exists(_.trim.nonEmpty) && i.sourceIdentitySha256.nonEmpty && i.catalogGeneration.nonEmpty && i.createOperationUuid.isEmpty
      val debug = i.sourceKind.isEmpty && i.sourceIdentity.isEmpty && i.sourceIdentitySha256.isEmpty && i.catalogGeneration.isEmpty && i.createOperationUuid.nonEmpty
      if (i.origin == UnboundIntentOrigin.ObservedArtifactAdoption && !adoption) || (i.origin == UnboundIntentOrigin.DebugItemCreate && !debug) then invalid("intent origin fields")
      val committed = Vector(i.committedDomainVersion, i.committedAt, i.committedWorldName, i.committedPosX, i.committedPosY, i.committedPosZ, i.committedYaw, i.committedPitch, i.committedDimensionId)
      i.phase match
        case MySqlRecoveryIntentPhase.Prepared => if committed.exists(_.nonEmpty) then invalid("PREPARED committed fields")
        case MySqlRecoveryIntentPhase.Committed =>
          if committed.exists(_.isEmpty) then invalid("COMMITTED missing fields")
          if !i.committedDomainVersion.contains(i.preparedDomainVersion + 1L) || !i.committedWorldName.contains(i.targetWorldName) ||
              !i.committedPosX.contains(i.targetPosX) || !i.committedPosY.contains(i.targetPosY) || !i.committedPosZ.contains(i.targetPosZ) ||
              !i.committedYaw.contains(i.targetYaw) || !i.committedPitch.contains(i.targetPitch) || !i.committedDimensionId.contains(i.targetDimensionId) then invalid("COMMITTED target mismatch")
    }

  private def unique[A](values: Vector[A], label: String): Unit =
    if values.distinct.size != values.size then invalid(s"duplicate $label")

  private def binary(row: ResultSet, index: Int, label: String, length: Int): Option[Vector[Byte]] =
    val bytes = row.getBytes(index)
    if bytes == null then None
    else
      if bytes.length != length then invalid(s"$label byte length")
      Some(bytes.toVector)

  private def nullableUuid(row: ResultSet, index: Int, label: String): Option[UUID] =
    binary(row, index, label, 16).map(MySqlStoredRows.decodeUuid(_, label))
  private def requiredUuid(row: ResultSet, index: Int, label: String): UUID =
    nullableUuid(row, index, label).getOrElse(missing(label))
  private def nullableText(row: ResultSet, index: Int): Option[String] = Option(row.getString(index))
  private def requiredText(row: ResultSet, index: Int, label: String): String = nullableText(row, index).getOrElse(missing(label))
  private def nullableLong(row: ResultSet, index: Int): Option[Long] =
    val value = row.getLong(index)
    if row.wasNull() then None else Some(value)
  private def requiredLong(row: ResultSet, index: Int, label: String): Long = nullableLong(row, index).getOrElse(missing(label))
  private def nullableInt(row: ResultSet, index: Int): Option[Int] =
    val value = row.getInt(index)
    if row.wasNull() then None else Some(value)
  private def requiredInt(row: ResultSet, index: Int, label: String): Int = nullableInt(row, index).getOrElse(missing(label))
  private def nullableDouble(row: ResultSet, index: Int): Option[Double] =
    val value = row.getDouble(index)
    if row.wasNull() then None else Some(value)
  private def requiredDouble(row: ResultSet, index: Int, label: String): Double = nullableDouble(row, index).getOrElse(missing(label))
  private def missing(label: String): Nothing = throw new SQLDataException(s"MySQL required recovery column $label is SQL NULL")
  private def invalid(boundary: String): Nothing = throw new SQLDataException(s"MySQL recovery row/graph violates $boundary")
