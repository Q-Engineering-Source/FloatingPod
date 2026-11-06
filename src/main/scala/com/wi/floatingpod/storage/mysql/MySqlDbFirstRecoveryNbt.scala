package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodState
import com.wi.floatingpod.entity.{PodArtifactNbtCodec,PodDiagnosticIdentity}
import com.wi.floatingpod.server.BoundedNbtStructurePreflight
import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.UUID
import net.minecraft.nbt.{CompressedStreamTools, NBTTagCompound, NBTSizeTracker}

/** 只解码当前Full27中的完整快照；资格与费用由原Main backing提供，不制造Boot/Ready收据。 */
private[mysql] object MySqlDbFirstRecoveryNbt:
  private val table = MySqlDbFirstSchemaDefinition.SnapshotTable
  private val columns = MySqlDbFirstRelationShapes.forTable(table).columns.map(_.name)

  private def cell(row: MySqlOwnedFormat3Row, name: String): MySqlOwnedFormat3Value =
    require(row.cells.size == columns.size, "complete schema6 snapshot row required")
    row.cells(columns.indexOf(name))
  private def binary(row: MySqlOwnedFormat3Row, name: String): MySqlOwnedFormat3Binary = cell(row, name) match
    case MySqlOwnedFormat3Value.Binary(value) => value
    case _ => throw new IllegalStateException("Snapshot binary field required: " + name)
  private def id(row: MySqlOwnedFormat3Row, name: String): UUID =
    val bytes = binary(row, name)
    require(bytes.length == 16, "snapshot UUID must have exactly 16 bytes")
    val source = java.nio.ByteBuffer.wrap(bytes.bytes)
    new UUID(source.getLong(), source.getLong())
  private def version(row: MySqlOwnedFormat3Row, name: String): Long = cell(row, name) match
    case MySqlOwnedFormat3Value.SignedInteger(value) => value
    case _ => throw new IllegalStateException("Snapshot version required: " + name)

  /** 同一Pod的未决事务不能因旧快照仍匹配domain而重新获得活动体；共享未决scope亦不能猜成局部。 */
  private[mysql] def requireSettled(operations: MySqlOwnedFormat3Relation,
      reservations: MySqlOwnedFormat3Relation, pod: UUID): Unit =
    def value(relation: MySqlOwnedFormat3Relation, row: MySqlOwnedFormat3Row, column: String) =
      val shape = MySqlDbFirstRelationShapes.forTable(relation.table)
      require(row.cells.size == shape.columns.size, "complete recovery responsibility row required")
      row.cells(shape.columns.indexWhere(_.name == column))
    def samePod(value: MySqlOwnedFormat3Value): Boolean = value match
      case MySqlOwnedFormat3Value.Binary(bytes) =>
        require(bytes.length == 16, "recovery responsibility Pod UUID required")
        bytes.equalsUuid(pod)
      case MySqlOwnedFormat3Value.SqlNull => true
      case _ => throw new IllegalStateException("recovery responsibility Pod UUID or SQL NULL required")
    require(operations.table == "fp_recovery_operation" && reservations.table == "fp_recovery_reservation",
      "actual schema6 operation/reservation relations required")
    operations.rows.foreach { row =>
      val state = value(operations, row, "operation_state")
      val pending = state == MySqlOwnedFormat3Value.Text("RESERVED") ||
        (state == MySqlOwnedFormat3Value.Text("CANCELLED") &&
          value(operations, row, "settled_by_operation_uuid") == MySqlOwnedFormat3Value.SqlNull)
      require(!pending || !samePod(value(operations, row, "pod_uuid")),
        "Pod recovery requires actual reconciliation of its unresolved operation scope")
    }
    reservations.rows.foreach { row =>
      if value(reservations, row, "reservation_state") == MySqlOwnedFormat3Value.Text("HELD") &&
        value(reservations, row, "resource_kind") == MySqlOwnedFormat3Value.Text("POD") then
        require(!samePod(value(reservations, row, "resource_pod_uuid")),
          "Pod recovery cannot bypass its held database reservation")
    }

  /** 与完整reader已验证的行再核当前恢复目标；历史快照不取得较新业务状态的完整恢复资格。 */
  private[mysql] def requireCurrent(row: MySqlOwnedFormat3Row, state: FloatingPodState,
      binding: UUID, attempt: UUID): Unit =
    require(state != null && binding != null && attempt != null && state.isActive && state.dimensionId.nonEmpty,
      "exact active dimensioned schema6 restore state required")
    require(id(row, "pod_uuid") == state.uuid && id(row, "binding_uuid") == binding &&
      id(row, "attempt_uuid") == attempt && version(row, "snapshot_format_version") == 1L &&
      version(row, "committed_state_version") == state.stateVersion &&
      version(row, "captured_state_version") == Math.subtractExact(state.stateVersion, 1L),
      "snapshot does not cover the exact current Pod/binding/attempt/version")
    val owner = cell(row, "owner_uuid") match
      case MySqlOwnedFormat3Value.SqlNull => None
      case _: MySqlOwnedFormat3Value.Binary => Some(id(row, "owner_uuid"))
      case _ => throw new IllegalStateException("Snapshot owner UUID or SQL NULL required")
    require(owner == state.ownerUuid, "snapshot owner differs from current committed owner")

  def consume(relation: MySqlOwnedFormat3Relation, state: FloatingPodState, binding: UUID, attempt: UUID,
      profile: SourceLimitProfile, retain: (Long, String) => AutoCloseable, use: NBTTagCompound => Unit): Unit =
    require(relation.table == table && retain != null && use != null, "actual snapshot relation/consumer required")
    // UUID copies/index traversal也在原账本预留后进行；不复制整张关系或payload。
    val indexCharge = retain(Math.addExact(2048L, Math.multiplyExact(relation.rows.size.toLong, 64L)),
      "schema6 recovery snapshot lookup")
    var primary: Throwable = null
    try
      val matches = relation.rows.filter(row => id(row, "pod_uuid") == state.uuid)
      require(matches.size == 1, "active Pod has no unique complete database snapshot")
      val row = matches.head
      requireCurrent(row, state, binding, attempt)
      val payload = binary(row, "nbt_bytes")
      require(payload.length > 0 && payload.length.toLong <= profile.maxDecompressedBytes(), "bounded complete snapshot required")
      val inputCharge = retain(Math.addExact(payload.length.toLong, 2048L), "schema6 recovery owned NBT input")
      var inputFailure: Throwable = null
      try
        val bytes = payload.bytes
        val decodedBytes = BoundedNbtStructurePreflight.decodedAllocationBytes(bytes, profile)
        val decodeCharge = retain(decodedBytes, "schema6 recovery NBT tree and decoder workspace")
        var decodeFailure: Throwable = null
        try
          val input = new DataInputStream(new ByteArrayInputStream(bytes))
          val nbt = CompressedStreamTools.read(input, new NBTSizeTracker(decodedBytes))
          require(input.available() == 0 && nbt.hasUniqueId("UUID") && nbt.getUniqueId("UUID") == state.uuid,
            "complete snapshot entity UUID differs")
          require(!nbt.hasKey(PodDiagnosticIdentity.ROOT_KEY),"world diagnostic identity cannot restore active DB business")
          val artifact = PodArtifactNbtCodec.read(nbt).orElseThrow()
          require(artifact.bindingUuid() == binding && artifact.attemptUuid() == attempt,
            "complete snapshot artifact differs from current authority")
          // Entity.deserializeNBT不创建Passengers；禁止静默丢弃非空乘客树。
          require(!nbt.hasKey("Passengers") || (nbt.getTag("Passengers") match
            case list: net.minecraft.nbt.NBTTagList => list.tagCount() == 0
            case _ => false),
            "passenger-bearing snapshot requires complete passenger recovery")
          use(nbt)
        catch case cause: Throwable =>
          decodeFailure = cause
          throw cause
        finally close(decodeCharge, decodeFailure)
      catch case cause: Throwable =>
        inputFailure = cause
        throw cause
      finally close(inputCharge, inputFailure)
    catch case cause: Throwable =>
      primary = cause
      throw cause
    finally close(indexCharge, primary)

  private def close(charge: AutoCloseable, primary: Throwable): Unit =
    try charge.close()
    catch case cleanup: Throwable =>
      if primary == null then throw cleanup
      if cleanup ne primary then primary.addSuppressed(cleanup)
