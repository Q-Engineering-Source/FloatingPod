package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import java.nio.ByteBuffer
import java.sql.SQLDataException
import java.util.UUID

/** 仅供 Fresh B1 使用：从同一份 Format 3 快照派生转换视图，不读取旧 SQL 图，也不证明锁或丢弃历史的权限。 */
private[mysql] object MySqlFreshFormat3TransitionProjection:
  private val FreshOwnedRelations = Set(
    "fp_schema_manifest",
    "fp_runtime_writer",
    "physical_recovery_binding",
    "binding_bootstrap_intent"
  )

  def project(snapshot: MySqlFormat3FullSnapshot): MySqlFullRepositorySnapshot =
    if snapshot == null then invalid("complete Format 3 snapshot is required")
    MySqlFormat3SnapshotValidation.validateRelations(snapshot.relations)
    snapshot.allocationBudget.reserveRetainedMemory(4_096L, "Fresh B1 typed transition projection")

    MySqlFormat3RelationShapes.canonicalOrder.iterator
      .filterNot(shape => FreshOwnedRelations.contains(shape.table))
      .find(shape => snapshot.relation(shape.table).rows.nonEmpty)
      .foreach(shape => invalid("Fresh B1 projection cannot discard relation history: " + shape.table))

    val binding = decodeBinding(snapshot)
    val intents = decodeBootstrapIntents(snapshot)
    val projected = MySqlFullRepositorySnapshot(
      Vector.empty,
      Vector.empty,
      MySqlRecoveryGraph(binding, Vector.empty, Vector.empty, Vector.empty),
      intents
    )
    projected.validate()
    MySqlFreshFormat3Graph.requireBindingMatches(snapshot, binding)
    MySqlFreshFormat3Graph.requireBootstrapIntentsMatch(snapshot, intents)
    projected

  private def decodeBinding(snapshot: MySqlFormat3FullSnapshot): Option[PhysicalRecoveryBinding] =
    val relation = snapshot.relation("physical_recovery_binding")
    relation.rows match
      case Vector() => None
      case Vector(Vector(
            MySqlFormat3Cell.SignedInteger(1L),
            MySqlFormat3Cell.Binary(bindingUuid),
            MySqlFormat3Cell.Text(state),
            MySqlFormat3Cell.SignedInteger(formatVersion)
          )) =>
        if formatVersion != 2L then invalid("Fresh physical binding must retain physical format 2")
        if formatVersion < Int.MinValue.toLong || formatVersion > Int.MaxValue.toLong then
          invalid("Format 3 B binding format does not fit the transition domain")
        val cutoverState = try PhysicalRecoveryCutoverState.fromDatabase(state)
        catch case failure: IllegalArgumentException => throw new SQLDataException("Format 3 B cutover state is unknown", failure)
        Some(PhysicalRecoveryBinding(decodeUuid(bindingUuid, snapshot, "Fresh B binding UUID"), cutoverState, formatVersion.toInt))
      case _ => invalid("Format 3 Fresh B must be absent or one exact four-cell singleton row")

  private def decodeBootstrapIntents(snapshot: MySqlFormat3FullSnapshot): Vector[MySqlBootstrapIntentData] =
    val relation = snapshot.relation("binding_bootstrap_intent")
    val shape = MySqlFormat3RelationShapes.forTable("binding_bootstrap_intent")
    if shape.columns.size != 11 then invalid("Format 3 B1 fixed eleven-column shape changed")
    if relation.rows.size > 1 then invalid("Fresh B1 projection requires at most its singleton history row")
    val columnIndex = shape.columns.iterator.zipWithIndex.map((column, index) => column.name -> index).toMap

    relation.rows.map { row =>
      if row == null || row.size != 11 then invalid("Format 3 B1 row cell count")
      def cell(name: String): MySqlFormat3Cell = row(columnIndex.getOrElse(name, invalid("Format 3 B1 column " + name)))
      def integer(name: String): Long = cell(name) match
        case MySqlFormat3Cell.SignedInteger(value) => value
        case _ => invalid("Format 3 B1 integer cell " + name)
      def text(name: String): String = cell(name) match
        case MySqlFormat3Cell.Text(value) if value != null => value
        case _ => invalid("Format 3 B1 text cell " + name)
      def uuid(name: String): UUID = cell(name) match
        case MySqlFormat3Cell.Binary(value) => decodeUuid(value, snapshot, "Fresh B1 " + name)
        case _ => invalid("Format 3 B1 binary UUID cell " + name)
      def hash(name: String): Vector[Byte] = cell(name) match
        case MySqlFormat3Cell.Binary(value) if value.length == 32 =>
          copiedHashVector(value, snapshot, "Fresh B1 " + name)
        case _ => invalid("Format 3 B1 SHA-256 cell " + name)
      def optionalHash(name: String): Option[Vector[Byte]] = cell(name) match
        case MySqlFormat3Cell.SqlNull => None
        case MySqlFormat3Cell.Binary(value) if value.length == 32 =>
          Some(copiedHashVector(value, snapshot, "Fresh B1 " + name))
        case _ => invalid("Format 3 B1 nullable SHA-256 cell " + name)
      def int(name: String): Int =
        val value = integer(name)
        if value < Int.MinValue.toLong || value > Int.MaxValue.toLong then invalid("Format 3 B1 integer range " + name)
        value.toInt

      val stage = text("stage") match
        case "PREPARED" => MySqlBootstrapIntentStage.Prepared
        case "COMPLETE" => MySqlBootstrapIntentStage.Complete
        case _ => invalid("Format 3 B1 stage token")
      MySqlBootstrapIntentData(
        int("singleton_fixed1"),
        uuid("bootstrap_operation_uuid"),
        uuid("prepare_write_uuid"),
        uuid("certify_write_uuid"),
        uuid("binding_uuid"),
        int("binding_format_version"),
        uuid("provisioning_uuid"),
        text("source_world_identity"),
        hash("provision_receipt_sha256"),
        stage,
        optionalHash("marker_ack_sha256")
      )
    }

  private def decodeUuid(value: MySqlFormat3Binary, snapshot: MySqlFormat3FullSnapshot, context: String): UUID =
    if value == null || value.length != 16 then invalid(context + " width")
    val bytes = value.copyBytes(snapshot.allocationBudget, context)
    val buffer = ByteBuffer.wrap(bytes)
    new UUID(buffer.getLong(), buffer.getLong())

  private def copiedHashVector(value: MySqlFormat3Binary, snapshot: MySqlFormat3FullSnapshot, context: String): Vector[Byte] =
    val bytes = value.copyBytes(snapshot.allocationBudget, context)
    // The transition row retains a Scala Vector as well as the defensive array copy.
    snapshot.allocationBudget.reserveRetainedMemory(Math.addExact(bytes.length.toLong, 128L), context + " retained vector")
    bytes.toVector

  private def invalid(reason: String): Nothing =
    throw new SQLDataException("MySQL Fresh Format3 transition projection rejected: " + reason)
