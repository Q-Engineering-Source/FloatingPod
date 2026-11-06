package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlDbFirstSnapshotRowsTest:
  private def inputs: MySqlScriptSchemaInputs =
    MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))
  private def root(i: MySqlScriptSchemaInputs) =
    i.newAllocationRoot(i.budget.packetBytes, new MySqlFormat3AllocationLedger(i.sourceLimitProfile.maxRetainedBytes()))
  private def bytes(value: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value))
  private def id(value: Int): MySqlFormat3Cell = bytes(java.nio.ByteBuffer.allocate(16).putLong(0).putLong(value.toLong).array())
  private def number(value: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(value)
  private def text(value: String): MySqlFormat3Cell = MySqlFormat3Cell.Text(value)
  private def digest(value: Array[Byte]): MySqlFormat3Cell = bytes(MessageDigest.getInstance("SHA-256").digest(value))
  private val snapshotTable = MySqlDbFirstSchemaDefinition.SnapshotTable

  // 只测试快照行责任边界；未指定的旧表字段不作为完整历史 DATA 的合法样本。
  private def rowFixture(i: MySqlScriptSchemaInputs): Vector[MySqlFormat3RelationSnapshot] =
    val nbt = Array[Byte](10, 0, 0, 0)
    val values = Map(
      snapshotTable -> Map("binding_uuid" -> id(1), "pod_uuid" -> id(2), "owner_uuid" -> id(3), "attempt_uuid" -> id(4),
        "last_mutation_operation_uuid" -> id(5), "snapshot_format_version" -> number(1), "nbt_bytes" -> bytes(nbt),
        "nbt_sha256" -> digest(nbt), "limit_profile_encoding" -> text(SourceLimitProfile.ENCODING),
        "limit_profile_bytes" -> bytes(i.sourceLimitProfile.canonicalBytes()), "limit_profile_sha256" -> bytes(i.sourceLimitProfile.sha256()),
        "captured_state_version" -> number(8), "committed_state_version" -> number(9)),
      "floating_pod_state" -> Map("uuid" -> id(2), "binding_uuid" -> id(1), "owner_uuid" -> id(3),
        "state_version" -> number(9), "is_active" -> number(0), "last_mutation_operation_uuid" -> id(5)),
      "fp_recovery_operation" -> Map("binding_uuid" -> id(1), "operation_uuid" -> id(5), "pod_uuid" -> id(2),
        "attempt_uuid" -> id(4), "step" -> text(MySqlDbFirstSchemaDefinition.RecallStep),
        "operation_state" -> text("COMMITTED"), "expected_domain_version" -> number(8)),
      "fp_schema_manifest" -> Map("format_version" -> number(6), "expected_binding_uuid" -> id(1),
        "limit_profile_bytes" -> bytes(i.sourceLimitProfile.canonicalBytes()), "limit_profile_sha256" -> bytes(i.sourceLimitProfile.sha256())),
      "physical_recovery_binding" -> Map("binding_uuid" -> id(1)))
    i.canonicalOrder.map { shape =>
      val rows = values.get(shape.table).map(v => Vector(shape.columns.map(c => v.getOrElse(c.name, MySqlFormat3Cell.SqlNull)))).getOrElse(Vector.empty)
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
  private def replace(rows: Vector[MySqlFormat3RelationSnapshot], i: MySqlScriptSchemaInputs,
      table: String, column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val index = i.forTable(table).columns.indexWhere(_.name == column)
    rows.map(r => if r.table == table then r.copy(rows = Vector(r.rows.head.updated(index, value))) else r)

  private def initialFixture(i: MySqlScriptSchemaInputs, b: MySqlFormat3ReadBudget): Vector[MySqlFormat3RelationSnapshot] =
    var rows=rowFixture(i)
    Vector((snapshotTable,"owner_uuid",MySqlFormat3Cell.SqlNull),
      (snapshotTable,"captured_state_version",number(0)),(snapshotTable,"committed_state_version",number(1)),
      ("floating_pod_state","owner_uuid",MySqlFormat3Cell.SqlNull),("floating_pod_state","state_version",number(1)),
      ("floating_pod_state","is_active",number(1)),
      ("fp_recovery_operation","step",text(MySqlDbFirstSchemaDefinition.InitialActivationStep)),
      ("fp_recovery_operation","expected_domain_version",number(0))).foreach { (t,c,v)=>rows=replace(rows,i,t,c,v) }
    def row(table:String,fields:Map[String,MySqlFormat3Cell])=
      i.forTable(table).columns.map(c=>fields.getOrElse(c.name,MySqlFormat3Cell.SqlNull))
    val pose=com.wi.floatingpod.domain.FloatingPodPose("world",1,64,2,3,4,0)
    val at=java.time.LocalDateTime.of(2026,11,2,12,0)
    val domain=MySqlRecoveryCanonicalBytes.withAllocationBudget(b) {
      val prepared=MySqlControlledDebugRows.preparedDomain(new UUID(0,1),new UUID(0,2),pose,new UUID(0,6),at,b)
      MySqlControlledDebugRows.activatedDomain(prepared,new UUID(0,5),at.plusSeconds(1),b)
    }
    rows.map { relation=>
      if relation.table=="floating_pod_state" then relation.copy(rows=Vector(domain))
      else if relation.table=="physical_recovery_intent" then relation.copy(rows=Vector(row(relation.table,Map(
        "binding_uuid"->id(1),"pod_uuid"->id(2),"attempt_uuid"->id(4),"origin"->text("DEBUG_ITEM_CREATE"),
        "phase"->text("ACTIVATED"),"create_operation_uuid"->id(7),"prepare_operation_uuid"->id(6),
        "activation_operation_uuid"->id(5),"expected_domain_version"->number(0),"committed_domain_version"->number(1)))))
      else if relation.table=="fp_recovery_operation" then relation.copy(rows=relation.rows :+ row(relation.table,Map(
        "binding_uuid"->id(1),"operation_uuid"->id(6),"pod_uuid"->id(2),"attempt_uuid"->id(4),
        "step"->text("REARM"),"operation_state"->text("COMMITTED"))))
      else relation
    }

  @Test def initialSnapshotRequiresPreparedLineageAndActiveV1FinalState(): Unit =
    val i=inputs;val b=root(i)
    try
      val rows=initialFixture(i,b)
      MySqlDbFirstSnapshotRows.validate(rows,i,b)
      Vector((snapshotTable,"owner_uuid",id(3)),("floating_pod_state","is_active",number(0)),
        ("floating_pod_state","operating_mode",text("NORMAL")),
        ("floating_pod_state","operating_mode",text("WorldIdle")),
        ("physical_recovery_intent","phase",text("PREPARED")),
        ("physical_recovery_intent","create_operation_uuid",MySqlFormat3Cell.SqlNull),
        ("physical_recovery_intent","activation_operation_uuid",id(9)),
        ("physical_recovery_intent","prepare_operation_uuid",id(9)),
        ("physical_recovery_intent","expected_owner_uuid",id(3)),
        ("physical_recovery_intent","committed_domain_version",number(2))).foreach { (t,c,v)=>
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstSnapshotRows.validate(replace(rows,i,t,c,v),i,b))
      }
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstSnapshotRows.validate(
        rows.map(r=>if r.table==snapshotTable then r.copy(rows=Vector.empty) else r),i,b))
      MySqlDbFirstSnapshotRows.validate(rows,i,b)
    finally b.closeRootReadBudget()

  @Test def rowValidationKeepsOriginalCommitWhileAllowingLaterDomainVersions(): Unit =
    val i = inputs; val b = root(i)
    try
      val rows = rowFixture(i)
      MySqlDbFirstSnapshotRows.validate(rows, i, b)
      val later = replace(replace(rows, i, "floating_pod_state", "state_version", number(10)), i,
        "floating_pod_state", "is_active", number(1))
      MySqlDbFirstSnapshotRows.validate(later, i, b)
    finally b.closeRootReadBudget()

  @Test def rowValidationRejectsCorruptionAndMismatchedOriginalIdentity(): Unit =
    val i = inputs; val b = root(i)
    try
      val original = rowFixture(i)
      Vector(
        (snapshotTable, "nbt_sha256", bytes(new Array[Byte](32))),
        (snapshotTable, "limit_profile_bytes", bytes(Array[Byte](1))),
        (snapshotTable, "committed_state_version", number(10)),
        (snapshotTable, "attempt_uuid", id(9)),
        (snapshotTable, "pod_uuid", id(9)),
        (snapshotTable, "binding_uuid", id(9)),
        ("fp_recovery_operation", "operation_state", text("RESERVED")),
        ("fp_recovery_operation", "step", text("RECALL_CHECKPOINT")),
        ("floating_pod_state", "state_version", number(8)),
        ("floating_pod_state", "is_active", number(1)),
        ("floating_pod_state", "owner_uuid", id(9))
      ).foreach { (table, column, value) =>
        assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotRows.validate(replace(original, i, table, column, value), i, b))
        MySqlDbFirstSnapshotRows.validate(original, i, b)
      }
    finally b.closeRootReadBudget()

  @Test def completeFull27AdmissionCannotDropTheSnapshotRelationOrCloseWithLiveChild(): Unit =
    val i = inputs; val b = root(i)
    try
      val rows = i.canonicalOrder.map { shape =>
        val values = if shape.table == MySqlDeploymentReceiptSchemaDefinition.QuotaTable then
          Vector(Vector(bytes(Array[Byte](70, 80, 68, 67)), number(4096), number(0), number(0))) else Vector.empty
        MySqlFormat3RelationSnapshot(shape.table, values)
      }
      i.validateRelations(rows, i.budget.packetBytes, b)
      assertThrows(classOf[SQLDataException], () => i.validateRelations(rows.dropRight(1), i.budget.packetBytes, b))
      assertThrows(classOf[SQLDataException], () =>
        MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(rows))
      val child = b.newCanonicalDataScope()
      assertThrows(classOf[IllegalStateException], () => b.closeRootReadBudget())
      child.closeScopedReadBudget()
      val connection = MySqlFormat3ConnectionObservation(new UUID(0, 24), "9.7.2", i.schema, "data@test", "InnoDB",
        1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
      val scope = MySqlFormat3ObservationScope(new UUID(0, 25), 1L, 1L)
      val snapshot = MySqlScriptSchemaSnapshot.captureMaterialized(i.schema, connection, scope, i.budget.packetBytes, i, rows, b)
      val payload = MySqlScriptSchemaSnapshotCodec.encode(snapshot, b)
      try
        val decoded = MySqlScriptSchemaSnapshotCodec.decode(payload, i, b)
        try
          assertEquals(27, decoded.relations.size)
          assertEquals(snapshot.canonicalSha256Hex, decoded.canonicalSha256Hex)
          assertEquals(rows, decoded.relations)
        finally decoded.allocationBudget.closeScopedReadBudget()
      finally payload.storageBudget.closeScopedReadBudget()
    finally b.closeRootReadBudget()

  @Test def committedRecallRequiresItsLatestSnapshotAndCannotFallBackToOlderPayload(): Unit =
    val i = inputs; val b = root(i)
    try
      val original = rowFixture(i)
      val missing = original.map(r => if r.table == snapshotTable then r.copy(rows = Vector.empty) else r)
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotRows.validate(missing, i, b))
      val advanced = replace(replace(replace(original, i, "floating_pod_state", "state_version", number(11)), i,
        "floating_pod_state", "last_mutation_operation_uuid", id(6)), i, "fp_recovery_operation", "expected_domain_version", number(10))
      val newerOperation = replace(advanced, i, "fp_recovery_operation", "operation_uuid", id(6))
      val history = newerOperation.map { r =>
        if r.table == "fp_recovery_operation" then r.copy(rows = original.find(_.table == r.table).get.rows ++ r.rows) else r
      }
      assertThrows(classOf[SQLDataException], () => MySqlDbFirstSnapshotRows.validate(history, i, b))
      val latest = replace(replace(replace(history, i, snapshotTable, "last_mutation_operation_uuid", id(6)), i,
        snapshotTable, "captured_state_version", number(10)), i, snapshotTable, "committed_state_version", number(11))
      MySqlDbFirstSnapshotRows.validate(latest, i, b)
      val reactivated = replace(replace(latest, i, "floating_pod_state", "state_version", number(12)), i,
        "floating_pod_state", "is_active", number(1))
      MySqlDbFirstSnapshotRows.validate(reactivated, i, b)
    finally b.closeRootReadBudget()
