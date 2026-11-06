package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.storage.*
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.util.UUID

/** 机械保留既有 full20 typed DATA fixture；不是 JDBC/Boot admission issuer。 */
private[floatingpod] object MainBootProjectionFixture:
  val schema = "fp_query_projection_test"
  def id(n: Long): UUID = new UUID(0L,n)
  def i(n: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(n)
  def t(value: String): MySqlFormat3Cell = MySqlFormat3Cell.Text(value)
  def f(value: Double): MySqlFormat3Cell = MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value))
  def u(value: UUID): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
    ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()))
  def binary(value: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value))
  def row(table: String, values: (String,MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
    val supplied = values.toMap
    MySqlFormat3RelationShapes.forTable(table).columns.map { c => supplied.getOrElse(c.name,
      if c.nullable then MySqlFormat3Cell.SqlNull else c.kind match
        case MySqlFormat3SqlKind.Text => t("x")
        case MySqlFormat3SqlKind.Binary => binary(Array.fill[Byte](c.maximumLength.getOrElse(1))(1))
        case MySqlFormat3SqlKind.Integer => i(1)
        case MySqlFormat3SqlKind.Floating => f(1.25)
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026,10,9,0,0,0,123456000))) }
  def pod(number: Long = 10L,owner: MySqlFormat3Cell = MySqlFormat3Cell.SqlNull,
      active: Long = 0L,dimension: MySqlFormat3Cell = i(0),yaw: Double = 1.25,updated: Long = 1234567890123L): Vector[MySqlFormat3Cell] =
    row("floating_pod_state","uuid"->u(id(number)),"owner_uuid"->owner,"binding_uuid"->u(id(2)),
      "state_version"->i(5),"world_name"->t("世界"),"pos_x"->f(1.25),"pos_y"->f(2.5),"pos_z"->f(3.75),
      "yaw"->f(yaw),"pitch"->f(-0.0),"is_active"->i(active),"last_updated"->i(updated),
      "dimension_id"->dimension,"operating_mode"->t("WORLD_IDLE"))
  def snapshot(extra: Map[String,Vector[Vector[MySqlFormat3Cell]]] = Map.empty,
      state: String = "READY",retained: Long = 32_000_000L,
      packet: Long = 4_000_000L): MySqlFormat3FullSnapshot =
    val profile = SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,256L,retained,packet,2_000_000L)
    val base = Map(
      "fp_schema_manifest"->Vector(row("fp_schema_manifest","bootstrap_state"->t(state),"format_version"->i(3))),
      "physical_recovery_binding"->Vector(row("physical_recovery_binding","singleton_fixed1"->i(1),
        "binding_uuid"->u(id(2)),"cutover_state"->t("CERTIFIED"),"format_version"->i(2))))
    val all = base ++ extra
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map(shape => MySqlFormat3RelationSnapshot(shape.table,all.getOrElse(shape.table,Vector.empty)))
    val connection = MySqlFormat3ConnectionObservation(id(1),"9.7.2",schema,"pure@localhost","InnoDB",1,1L,
      "READ-COMMITTED","REPEATABLE-READ","STRICT_ALL_TABLES","+00:00","TLSv1.3","TLS_AES_256_GCM_SHA384")
    val scope = MySqlFormat3ObservationScope(id(3),1L,1L)
    val budget = new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    budget.initialize(schema,connection,scope,packet)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema,connection,scope,packet,profile,relations,budget)
  def accountedMemoryBytes(s: MySqlFormat3FullSnapshot): Long = s.allocationBudget.accountedMemoryBytes
  def projection(s: MySqlFormat3FullSnapshot): MySqlFormat3QueryProjection = MySqlFormat3QueryProjection.from(s,s.allocationBudget)
  def changes(row: Vector[MySqlFormat3Cell],table: String,name: String,value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    row.updated(MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name==name),value)
  def history(origin: String = "DEBUG_ITEM_CREATE",phase: String = "PREPARED",seal: String = "SEALED",
      current: Boolean = true): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val footprint = row("physical_recovery_footprint","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),
      "attempt_uuid"->u(id(20)),"footprint_uuid"->u(id(30)),"origin_version"->i(0),"target_world_name"->t("世界"),
      "target_pos_x"->f(1.25),"target_pos_y"->f(2.5),"target_pos_z"->f(3.75),"target_yaw"->f(1.25),"target_pitch"->f(-0.0),
      "target_dimension_id"->i(0),"center_chunk_x"->i(0),"center_chunk_z"->i(0),"seal_state"->t(seal))
    val cs = (for x <- -2 to 2; z <- -2 to 2 yield row("physical_recovery_footprint_chunk","binding_uuid"->u(id(2)),
      "pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),"footprint_uuid"->u(id(30)),"dimension_id"->i(0),"chunk_x"->i(x),"chunk_z"->i(z))).toVector
    var intent = row("physical_recovery_intent","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),"attempt_uuid"->u(id(20)),
      "origin"->t(origin),"phase"->t(phase),"target_footprint_uuid"->u(id(30)),"expected_domain_version"->i(0),
      "create_operation_uuid"->(if origin=="DEBUG_ITEM_CREATE" then u(id(40)) else MySqlFormat3Cell.SqlNull))
    if origin=="UNBOUND_ADOPTION" then
      val identity = "raw source identity"
      intent = changes(intent,"physical_recovery_intent","source_kind",t("FRESH"))
      intent = changes(intent,"physical_recovery_intent","source_identity",t(identity))
      intent = changes(intent,"physical_recovery_intent","source_identity_sha256",binary(java.security.MessageDigest.getInstance("SHA-256").digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8))))
      intent = changes(intent,"physical_recovery_intent","catalog_generation",u(id(50)))
    val locator = row("physical_recovery_locator","binding_uuid"->u(id(2)),"pod_uuid"->u(id(10)),
      "current_attempt_uuid"->u(id(20)),"current_footprint_uuid"->u(id(30)),"locator_state"->t(if origin=="DEBUG_ITEM_CREATE" then "DEBUG_ITEM_CREATE_PREPARED" else "UNBOUND_ADOPTION_PREPARED"),
      "reason_code"->t(if origin=="DEBUG_ITEM_CREATE" then "DEBUG_ITEM_CREATE_PREPARED" else "UNBOUND_ADOPTION_PREPARED"),"domain_version"->i(0),"target_dimension_id"->i(0))
    Map("floating_pod_state"->Vector(changes(pod(),"floating_pod_state","state_version",i(0))),"physical_recovery_footprint"->Vector(footprint),"physical_recovery_footprint_chunk"->(if seal=="DRAFT" then cs.take(3) else cs),
      "physical_recovery_intent"->Vector(intent),"physical_recovery_locator"->(if current then Vector(locator) else Vector.empty))


  /** 只生成已有 typed DATA 的 capture stamp，不能构造实际 claimed MainBootProjection。 */
  def stampedBoot(source: MySqlFormat3FullSnapshot, tag: com.wi.floatingpod.server.io.IoOperationTag,
      capturedEpoch: Long): MySqlOwnedFormat3Snapshot =
    MySqlOwnedFormat3Snapshot.fromAcceptedBoot(source, tag, capturedEpoch)
  def unstampedBoot(source: MySqlFormat3FullSnapshot): MySqlOwnedFormat3Snapshot =
    MySqlOwnedFormat3Snapshot.fromForBoot(source)

  /** 显式 opt-in：仅给调用测试声明的有限 UUID 补齐完整图的 operation DATA，不签 SQL 资格。
    * 引用集合必须与声明完全相同；未知/遗漏 FK 不会被自动修复。默认 snapshot/history 不变。
    */
  def withCommittedOperationReferences(extra: Map[String,Vector[Vector[MySqlFormat3Cell]]],
      expected: Set[UUID]): Map[String,Vector[Vector[MySqlFormat3Cell]]] =
    val references = extra.toVector.flatMap { (table,rs) =>
      val columns = MySqlFormat3RelationShapes.forTable(table).columns
      rs.flatMap(r => columns.zip(r).collect {
        case (column,MySqlFormat3Cell.Binary(value)) if column.name.endsWith("operation_uuid") =>
          val candidates = expected.filter(value.equalsUuid)
          require(candidates.size == 1,"fixture operation reference is not an explicitly declared UUID")
          candidates.head
      })
    }.toSet
    require(references == expected,"fixture explicit operation UUID set differs from actual references")
    val payload = Array[Byte](1,2,3)
    val digest = java.security.MessageDigest.getInstance("SHA-256").digest(payload)
    val pairs = Vector("request_identity","original_context","scope","preimage","target").flatMap(n =>
      Vector(n+"_bytes"->binary(payload),n+"_sha256"->binary(digest)))
    val operations = expected.toVector.sortWith { (left,right) =>
      val hi = java.lang.Long.compareUnsigned(left.getMostSignificantBits,right.getMostSignificantBits)
      hi < 0 || (hi == 0 && java.lang.Long.compareUnsigned(left.getLeastSignificantBits,right.getLeastSignificantBits) < 0)
    }.map { operation =>
      row("fp_recovery_operation",(pairs ++ Vector("binding_uuid"->u(id(2)),"operation_uuid"->u(operation),
        "workflow_uuid"->u(id(9000)),"step"->t("DOMAIN_MUTATION"),"operation_state"->t("COMMITTED"),
        "context_encoding"->t("FP_RECOVERY_OPERATION_BE_V1"),
        "result_at"->MySqlFormat3Cell.DateTime(LocalDateTime.of(2026,10,9,0,0)),
        "commit_witness_bytes"->binary(payload),"commit_witness_sha256"->binary(digest)))*)
    }
    extra.updated("fp_recovery_operation",operations)
