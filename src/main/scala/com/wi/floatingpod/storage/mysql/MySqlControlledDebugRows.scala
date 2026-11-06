package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodPose
import java.sql.SQLDataException
import java.time.{LocalDateTime,ZoneOffset}
import java.util.UUID

/** DDL7 controlled debug producer 的唯一列构造；无缺列默认值、无资格发行。 */
private[mysql] object MySqlControlledDebugRows:
  val Domain = "floating_pod_state"
  val Footprint = "physical_recovery_footprint"
  val Chunk = "physical_recovery_footprint_chunk"
  val Intent = "physical_recovery_intent"
  val Locator = "physical_recovery_locator"
  val Operation = "fp_recovery_operation"
  val Reservation = "fp_recovery_reservation"
  val Null = MySqlFormat3Cell.SqlNull

  def value(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  def uuid(id: UUID): MySqlFormat3Cell =
    if id == null || id == new UUID(0L,0L) then fail("nonzero original UUID required")
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(java.nio.ByteBuffer.allocate(16)
      .putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array()))
  def signed(n: Long): MySqlFormat3Cell = MySqlFormat3Cell.SignedInteger(n)
  def text(s: String): MySqlFormat3Cell =
    if s == null then fail("text cannot be null")
    MySqlFormat3Cell.Text(s)
  def floating(n: Double): MySqlFormat3Cell =
    if !java.lang.Double.isFinite(n) then fail("finite target DOUBLE required")
    MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(n))
  def row(table: String, supplied: Map[String,MySqlFormat3Cell], budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    if supplied == null || budget == null || budget.isClosed then fail("exact open producer root required")
    val shape=MySqlFormat3RelationShapes.forTable(table)
    budget.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(shape.columns.size.toLong,96L)),"controlled typed row " + table)
    if supplied.size != shape.columns.size || !shape.columns.forall(c => supplied.contains(c.name)) then
      fail("complete exact column map required for " + table)
    shape.columns.foreach(column => validateCell(supplied(column.name),column,table))
    shape.columns.map(column => retainInput(supplied(column.name),budget,table + "." + column.name))
  def update(old: Vector[MySqlFormat3Cell],table: String,updates: Map[String,MySqlFormat3Cell],budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val shape=MySqlFormat3RelationShapes.forTable(table)
    if old == null || old.size != shape.columns.size || updates == null ||
      updates.keys.exists(name => !shape.columns.exists(_.name==name)) then fail("exact update column shape")
    budget.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(shape.columns.size.toLong,96L)),"controlled updated typed row " + table)
    shape.columns.zip(old).foreach { (column,original) =>
      validateCell(updates.getOrElse(column.name,original),column,table)
    }
    shape.columns.zip(old).map { (column,original) =>
      retainInput(updates.getOrElse(column.name,original),budget,table + "." + column.name)
    }

  private def retainInput(cell: MySqlFormat3Cell,budget: MySqlFormat3ReadBudget,context: String): MySqlFormat3Cell = cell match
    case MySqlFormat3Cell.Binary(value) if value.storageBudget == null =>
      val copied = value.copyBytes(budget,"controlled immutable input copy " + context)
      MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(copied,budget))
    case _ => cell

  private def validateCell(cell: MySqlFormat3Cell,column: MySqlFormat3ColumnShape,table: String): Unit =
    if cell == null then fail("missing cell " + table + "." + column.name)
    cell match
      case MySqlFormat3Cell.SqlNull =>
        if !column.nullable then fail("nonnullable cell " + table + "." + column.name)
      case MySqlFormat3Cell.Text(value) =>
        if column.kind != MySqlFormat3SqlKind.Text || value == null ||
          column.maximumLength.exists(_ < value.codePointCount(0,value.length)) then fail("text shape " + table + "." + column.name)
        MySqlFormat3Utf8.byteLength(value)
      case MySqlFormat3Cell.Binary(value) =>
        if column.kind != MySqlFormat3SqlKind.Binary || value == null ||
          column.maximumLength.exists(_ < value.length) then fail("binary shape " + table + "." + column.name)
      case MySqlFormat3Cell.SignedInteger(value) =>
        if column.kind != MySqlFormat3SqlKind.Integer ||
          (column.sqlDeclaration.startsWith("TINYINT") && (value < Byte.MinValue || value > Byte.MaxValue)) ||
          (column.sqlDeclaration.startsWith("INT ") && (value < Int.MinValue || value > Int.MaxValue)) then
          fail("integer shape " + table + "." + column.name)
      case MySqlFormat3Cell.Floating(bits) =>
        if column.kind != MySqlFormat3SqlKind.Floating || !java.lang.Double.isFinite(java.lang.Double.longBitsToDouble(bits)) then
          fail("finite floating shape " + table + "." + column.name)
      case MySqlFormat3Cell.DateTime(value) =>
        if column.kind != MySqlFormat3SqlKind.DateTime || value == null || value.getNano % 1000 != 0 ||
          value.getYear < 1000 || value.getYear > 9999 then fail("DATETIME6 shape " + table + "." + column.name)

  /** REARM是不存在的前像：O expected columns SQL NULL，目标domain/locator仍是0。 */
  def expectedOperationColumns(activation: Boolean,budget: MySqlFormat3ReadBudget): Map[String,MySqlFormat3Cell] =
    budget.reserveRetainedMemory(1024L,"controlled expected NULL-or-zero column map")
    Map("expected_domain_version" -> (if activation then signed(0L) else Null),
      "expected_locator_revision" -> (if activation then signed(0L) else Null),"expected_source_revision" -> Null)

  private def poseFields(pose: FloatingPodPose): Map[String,MySqlFormat3Cell] =
    if pose == null || pose.worldName.trim.isEmpty || !java.lang.Float.isFinite(pose.yaw) || !java.lang.Float.isFinite(pose.pitch) then fail("complete finite target pose")
    Map("world_name"->text(pose.worldName),"pos_x"->floating(pose.posX),"pos_y"->floating(pose.posY),"pos_z"->floating(pose.posZ),
      "yaw"->floating(pose.yaw.toDouble),"pitch"->floating(pose.pitch.toDouble),"dimension_id"->signed(pose.dimensionId))
  private def milliseconds(at: LocalDateTime): Long =
    if at == null || at.getNano % 1000 != 0 || at.getYear<1000 || at.getYear>9999 then fail("actual fixed DATETIME6 required")
    val n=at.toInstant(ZoneOffset.UTC).toEpochMilli
    if n<0 then fail("nonnegative actual domain time required")
    n
  def preparedDomain(binding: UUID,pod: UUID,pose: FloatingPodPose,execute: UUID,at: LocalDateTime,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(8192L,"controlled prepared domain cell/map inputs")
    row(Domain,poseFields(pose) ++ Map("uuid"->uuid(pod),"owner_uuid"->Null,"state_version"->signed(0),
      "is_active"->signed(0),"last_updated"->signed(milliseconds(at)),"operating_mode"->text("WORLD_IDLE"),
      "binding_uuid"->uuid(binding),"last_mutation_operation_uuid"->uuid(execute)),budget)
  def activatedDomain(old: Vector[MySqlFormat3Cell],execute: UUID,at: LocalDateTime,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(4096L,"controlled activated domain input map")
    if value(old,Domain,"owner_uuid") != Null || value(old,Domain,"is_active") != signed(0) ||
      value(old,Domain,"state_version") != signed(0) || value(old,Domain,"operating_mode") != text("WORLD_IDLE") then fail("exact inactive unclaimed v0 domain required")
    update(old,Domain,Map("state_version"->signed(1),"is_active"->signed(1),"last_updated"->signed(milliseconds(at)),
      "last_mutation_operation_uuid"->uuid(execute)),budget)

  def preparedFootprint(binding: UUID,pod: UUID,attempt: UUID,footprint: UUID,pose: FloatingPodPose,execute: UUID,
      budget: MySqlFormat3ReadBudget,seal: String = "SEALED"): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(8192L,"controlled footprint cell/map inputs")
    if seal != "DRAFT" && seal != "SEALED" then fail("exact footprint seal token")
    val fields=poseFields(pose)
    row(Footprint,Map("binding_uuid"->uuid(binding),"pod_uuid"->uuid(pod),"attempt_uuid"->uuid(attempt),"footprint_uuid"->uuid(footprint),
      "origin_version"->signed(0),"target_world_name"->fields("world_name"),"target_pos_x"->fields("pos_x"),"target_pos_y"->fields("pos_y"),
      "target_pos_z"->fields("pos_z"),"target_yaw"->fields("yaw"),"target_pitch"->fields("pitch"),"target_dimension_id"->fields("dimension_id"),
      "center_chunk_x"->signed(chunk(pose.posX)),"center_chunk_z"->signed(chunk(pose.posZ)),"purpose"->text("CREATE_TARGET"),
      "seal_state"->text(seal),"created_operation_uuid"->uuid(execute)),budget)
  def preparedChunks(binding: UUID,pod: UUID,attempt: UUID,footprint: UUID,pose: FloatingPodPose,budget: MySqlFormat3ReadBudget): Vector[Vector[MySqlFormat3Cell]] =
    budget.reserveRetainedMemory(131072L,"controlled exact25 chunk cells/vector inputs")
    val cx=chunk(pose.posX);val cz=chunk(pose.posZ)
    (for x <- -2 to 2; z <- -2 to 2 yield row(Chunk,Map("binding_uuid"->uuid(binding),"pod_uuid"->uuid(pod),"attempt_uuid"->uuid(attempt),
      "footprint_uuid"->uuid(footprint),"dimension_id"->signed(pose.dimensionId),"chunk_x"->signed(Math.addExact(cx,x)),"chunk_z"->signed(Math.addExact(cz,z))),budget)).toVector
  private def chunk(n: Double): Int =
    if !java.lang.Double.isFinite(n) || n< Int.MinValue.toDouble*16D || n>Int.MaxValue.toDouble*16D then fail("target chunk range")
    Math.floor(n/16D).toInt

  def preparedIntent(binding: UUID,pod: UUID,attempt: UUID,footprint: UUID,execute: UUID,create: UUID,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val shape=MySqlFormat3RelationShapes.forTable(Intent)
    budget.reserveTemporaryMemory(Math.addExact(512L,Math.multiplyExact(shape.columns.size.toLong,128L)),"controlled intent input map")
    var primary: Throwable=null
    try
      val required=Map("binding_uuid"->uuid(binding),"pod_uuid"->uuid(pod),"attempt_uuid"->uuid(attempt),"origin"->text("DEBUG_ITEM_CREATE"),
        "phase"->text("PREPARED"),"target_footprint_uuid"->uuid(footprint),"prepare_operation_uuid"->uuid(execute),
        "expected_owner_uuid"->Null,"expected_domain_version"->signed(0),"create_operation_uuid"->uuid(create),
        "intent_revision"->signed(0),"last_mutation_operation_uuid"->uuid(execute))
      val supplied=shape.columns.map(c => c.name -> required.getOrElse(c.name,if c.nullable then Null else fail("unprovided mandatory intent field " + c.name))).toMap
      row(Intent,supplied,budget)
    catch
      case cause: Throwable => primary=cause;throw cause
    finally
      try budget.releaseTemporaryMemory(Math.addExact(512L,Math.multiplyExact(shape.columns.size.toLong,128L)),"controlled intent input map")
      catch case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  def activatedIntent(old: Vector[MySqlFormat3Cell],domain: Vector[MySqlFormat3Cell],execute: UUID,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(8192L,"controlled activated intent input map")
    if value(old,Intent,"phase") != text("PREPARED") || value(old,Intent,"origin") != text("DEBUG_ITEM_CREATE") ||
      value(old,Intent,"intent_revision") != signed(0) then fail("exact DEBUG prepared intent required")
    val committed=Map("phase"->text("ACTIVATED"),"intent_revision"->signed(1),"activation_operation_uuid"->uuid(execute),
      "committed_domain_version"->value(domain,Domain,"state_version"),"committed_at"->value(domain,Domain,"last_updated"),
      "committed_world_name"->value(domain,Domain,"world_name"),"committed_pos_x"->value(domain,Domain,"pos_x"),
      "committed_pos_y"->value(domain,Domain,"pos_y"),"committed_pos_z"->value(domain,Domain,"pos_z"),"committed_yaw"->value(domain,Domain,"yaw"),
      "committed_pitch"->value(domain,Domain,"pitch"),"committed_dimension_id"->value(domain,Domain,"dimension_id"),"last_mutation_operation_uuid"->uuid(execute))
    update(old,Intent,committed,budget)
  def preparedLocator(binding: UUID,pod: UUID,attempt: UUID,footprint: UUID,dimension: Int,execute: UUID,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(8192L,"controlled prepared locator cell/map inputs")
    row(Locator,Map("binding_uuid"->uuid(binding),"pod_uuid"->uuid(pod),"current_attempt_uuid"->uuid(attempt),"current_footprint_uuid"->uuid(footprint),
      "locator_state"->text("DEBUG_ITEM_CREATE_PREPARED"),"reason_code"->text("DEBUG_ITEM_CREATE_PREPARED"),"domain_version"->signed(0),
      "target_dimension_id"->signed(dimension),"locator_revision"->signed(0),"last_mutation_operation_uuid"->uuid(execute)),budget)
  def activatedLocator(old: Vector[MySqlFormat3Cell],execute: UUID,budget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    budget.reserveRetainedMemory(4096L,"controlled activated locator input map")
    if value(old,Locator,"locator_state") != text("DEBUG_ITEM_CREATE_PREPARED") || value(old,Locator,"domain_version") != signed(0) ||
      value(old,Locator,"locator_revision") != signed(0) then fail("exact DEBUG prepared locator required")
    update(old,Locator,Map("locator_state"->text("ROW_ACTIVE"),"reason_code"->text("ROW_ACTIVE"),"domain_version"->signed(1),
      "locator_revision"->signed(1),"last_mutation_operation_uuid"->uuid(execute)),budget)
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled DEBUG row rejected: " + reason)
