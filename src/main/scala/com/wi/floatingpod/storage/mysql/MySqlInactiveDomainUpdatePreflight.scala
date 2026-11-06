package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

/** 完整图先验证；下列判断仅 DATA 业务拒绝，不授予 SQL 或 absence 权限。 */
private[mysql] object MySqlInactiveDomainUpdatePreflight:
  import MySqlControlledDebugRows.{value, signed, text}
  private val Domain = "floating_pod_state"
  private val Locator = "physical_recovery_locator"

  def requireInputGraph(snapshot: MySqlFormat3FullSnapshot, root: MySqlFormat3ReadBudget): Unit =
    if snapshot == null || root == null then fail("actual original workflow snapshot/root required")
    val manifest = one(snapshot,"fp_schema_manifest",_ => true)
    if value(manifest,"fp_schema_manifest","format_version") != signed(3) ||
      value(manifest,"fp_schema_manifest","bootstrap_state") != text("READY") then
      fail("inactive update requires certified format3 normal domain, not BOOTSTRAPPING")
    MySqlControlledDebugRelations.requireValidFullGraph(snapshot, root)

  private def domainRejection(snapshot: MySqlFormat3FullSnapshot, request: MySqlInactiveDomainUpdateRequest): Option[MySqlInactiveDomainUpdateRejection] =
    if snapshot == null || request == null then fail("complete locked snapshot and request required")
    val binding = one(snapshot,"physical_recovery_binding",_ => true)
    if !matches(binding,"physical_recovery_binding","binding_uuid",request.expectedBinding.bindingUuid) ||
      value(binding,"physical_recovery_binding","cutover_state") != text("CERTIFIED") ||
      value(binding,"physical_recovery_binding","format_version") != signed(2) then
      return Some(MySqlInactiveDomainUpdateRejection.BindingMismatch)
    val domain = find(snapshot,Domain,row => matches(row,Domain,"uuid",request.podUuid))
    if domain == null then return Some(MySqlInactiveDomainUpdateRejection.UnknownPod)
    if !matches(domain,Domain,"binding_uuid",request.expectedBinding.bindingUuid) then
      return Some(MySqlInactiveDomainUpdateRejection.BindingMismatch)
    val actualOwner = value(domain,Domain,"owner_uuid")
    val sameOwner = request.expectedOwner match
      case None => actualOwner == MySqlFormat3Cell.SqlNull
      case Some(owner) => matches(domain,Domain,"owner_uuid",owner)
    if !sameOwner then return Some(MySqlInactiveDomainUpdateRejection.OwnerMismatch)
    val version = integer(domain,Domain,"state_version")
    if version != request.expectedVersion then return Some(MySqlInactiveDomainUpdateRejection.VersionMismatch(version))
    if value(domain,Domain,"is_active") != signed(0) then return Some(MySqlInactiveDomainUpdateRejection.Active)
    None

  def rejection(snapshot: MySqlFormat3FullSnapshot, request: MySqlInactiveDomainUpdateRequest,
      root: MySqlFormat3ReadBudget): Option[MySqlInactiveDomainUpdateRejection] =
    requireInputGraph(snapshot,root)
    // 正常 CURRENT_HOLD 或没有 pending transfer 也必须完整解码全部历史/无关 forest。
    // Boolean 仅 DATA，不据它拒绝合法 pod 或授予 absence；后续未解责任按 exact key 关联。
    val _ = MySqlFormat3SourceForestMembershipScan.contains(snapshot,root,request.podUuid)
    val domainConflict=domainRejection(snapshot,request)
    if domainConflict.nonEmpty then return domainConflict
    // 04_bu COMMITTED 核整个 binding 的实际 joined graph；只目标 NULL 可在本 reserve Tx 同步。
    // unrelated 不一致保留为具体恢复依赖，不能先写 reserve 或顺手修另一 pod。
    val locators=snapshot.relation(Locator).rows
    var locatorIndex=0
    while locatorIndex<locators.size do
      val current=locators(locatorIndex)
      if matches(current,Locator,"binding_uuid",request.expectedBinding.bindingUuid) then
        val domain=find(snapshot,Domain,row => value(row,Domain,"binding_uuid")==value(current,Locator,"binding_uuid") &&
          value(row,Domain,"uuid")==value(current,Locator,"pod_uuid"))
        val footprint=find(snapshot,"physical_recovery_footprint",row =>
          value(row,"physical_recovery_footprint","binding_uuid")==value(current,Locator,"binding_uuid") &&
          value(row,"physical_recovery_footprint","pod_uuid")==value(current,Locator,"pod_uuid") &&
          value(row,"physical_recovery_footprint","attempt_uuid")==value(current,Locator,"current_attempt_uuid") &&
          value(row,"physical_recovery_footprint","footprint_uuid")==value(current,Locator,"current_footprint_uuid"))
        if domain!=null && footprint!=null then
          val targetNull=matches(current,Locator,"pod_uuid",request.podUuid) &&
            value(current,Locator,"domain_version")==MySqlFormat3Cell.SqlNull
          val versionCoherent=value(current,Locator,"domain_version")==value(domain,Domain,"state_version") || targetNull
          val physicalCoherent=value(footprint,"physical_recovery_footprint","seal_state")==text("SEALED") &&
            value(current,Locator,"target_dimension_id")==value(footprint,"physical_recovery_footprint","target_dimension_id")
          if !versionCoherent || !physicalCoherent then
            return Some(MySqlInactiveDomainUpdateRejection.CommitGraphRecoveryPending(uuidValue(current,Locator,"pod_uuid")))
      locatorIndex+=1
    val unresolvedOperation=snapshot.relation("fp_recovery_operation").rows.exists(row =>
      matches(row,"fp_recovery_operation","pod_uuid",request.podUuid) &&
      value(row,"fp_recovery_operation","operation_state")==text("RESERVED"))
    val heldPod=snapshot.relation("fp_recovery_reservation").rows.exists(row =>
      matches(row,"fp_recovery_reservation","resource_pod_uuid",request.podUuid) &&
      value(row,"fp_recovery_reservation","resource_kind")==text("POD") &&
      value(row,"fp_recovery_reservation","reservation_state")==text("HELD"))
    val unresolvedPhysical=snapshot.relation("physical_recovery_obligation").rows.exists(row =>
      matches(row,"physical_recovery_obligation","pod_uuid",request.podUuid) &&
      value(row,"physical_recovery_obligation","obligation_state")==text("PENDING"))
    val locator=find(snapshot,Locator,row => matches(row,Locator,"pod_uuid",request.podUuid))
    val unfinishedCurrentIntent=locator!=null && snapshot.relation("physical_recovery_intent").rows.exists(row =>
      matches(row,"physical_recovery_intent","pod_uuid",request.podUuid) &&
      value(row,"physical_recovery_intent","attempt_uuid")==value(locator,Locator,"current_attempt_uuid") &&
      (value(row,"physical_recovery_intent","phase")==text("PREPARED") ||
       value(row,"physical_recovery_intent","phase")==text("CLEANUP_PENDING")))
    if unresolvedOperation || heldPod || unresolvedPhysical || unfinishedCurrentIntent then
      return Some(MySqlInactiveDomainUpdateRejection.RecoveryPending)
    val sources=snapshot.relation("physical_source_obligation").rows
    var index=0
    while index<sources.size do
      val obligation=sources(index)
      if value(obligation,"physical_source_obligation","obligation_state")==text("OPEN") &&
        value(obligation,"physical_source_obligation","purpose")!=text("CURRENT_HOLD") then
        root.reserveTemporaryMemory(128L,"inactive exact source-obligation unit/forest UUIDs")
        var primary: Throwable=null
        try
          val unit=uuidValue(obligation,"physical_source_obligation","unit_uuid")
          val forest=uuidValue(obligation,"physical_source_obligation","forest_uuid")
          if MySqlFormat3SourceForestMembershipScan.containsInForest(snapshot,root,request.podUuid,unit,forest) then
            return Some(MySqlInactiveDomainUpdateRejection.RecoveryPending)
        catch
          case cause: Throwable =>
            primary=cause
            throw cause
        finally
          try root.releaseTemporaryMemory(128L,"inactive exact source-obligation UUID scratch release")
          catch case cleanup: Throwable =>
            if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      index+=1
    // 持续 CURRENT_HOLD 本身不是未结迁移；真正未结 transfer 必须按同 unit/forest 全解码关联。
    val transfers=snapshot.relation("physical_source_transfer").rows
    index=0
    while index<transfers.size do
      val transfer=transfers(index)
      if value(transfer,"physical_source_transfer","phase")!=text("COMPLETED") then
        root.reserveTemporaryMemory(192L,"inactive exact pending transfer UUIDs")
        var primary: Throwable=null
        try
          val unit=uuidValue(transfer,"physical_source_transfer","unit_uuid")
          val oldForest=uuidValue(transfer,"physical_source_transfer","old_forest_uuid")
          val targetForest=uuidValue(transfer,"physical_source_transfer","target_forest_uuid")
          val inOld=MySqlFormat3SourceForestMembershipScan.containsInForest(snapshot,root,request.podUuid,unit,oldForest)
          val inTarget=MySqlFormat3SourceForestMembershipScan.containsInForest(snapshot,root,request.podUuid,unit,targetForest)
          if inOld || inTarget then return Some(MySqlInactiveDomainUpdateRejection.RecoveryPending)
        catch
          case cause: Throwable =>
            primary=cause
            throw cause
        finally
          try root.releaseTemporaryMemory(192L,"inactive pending transfer UUID scratch release")
          catch case cleanup: Throwable =>
            if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
      index+=1
    None
  /** desired 不控制 version/time/owner/mode；固定物理 locator 只接受同 footprint 的 domain pose。 */
  def domainTarget(snapshot: MySqlFormat3FullSnapshot, request: MySqlInactiveDomainUpdateRequest,
      execute: UUID, plannedAt: LocalDateTime, root: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    if execute == null || plannedAt == null || plannedAt.getNano % 1000 != 0 ||
      plannedAt.getYear < 1000 || plannedAt.getYear > 9999 || root == null || root.isClosed then
      fail("fixed execute identity, DB DATETIME6 and original budget required")
    domainRejection(snapshot,request).foreach(reason => fail("cannot materialize rejected inactive update: " + reason))
    val old = one(snapshot,Domain,row => matches(row,Domain,"uuid",request.podUuid))
    val desired = request.desired
    if value(old,Domain,"operating_mode") != text(desired.operatingMode.databaseValue) then
      fail("inactive update cannot replace operating mode")
    val dimension = desired.dimensionId match
      case None => MySqlFormat3Cell.SqlNull
      case Some(n) => signed(n)
    if value(old,Domain,"dimension_id") != dimension then fail("inactive update cannot replace dimension identity")
    val at = plannedAt.toInstant(ZoneOffset.UTC).toEpochMilli
    if at < 0L then fail("domain time must remain nonnegative")
    root.reserveRetainedMemory(4096L,"inactive domain update cells and column map")
    MySqlControlledDebugRows.update(old,Domain,Map(
      "state_version" -> signed(Math.addExact(request.expectedVersion,1L)),
      "world_name" -> text(desired.worldName),
      "pos_x" -> MySqlControlledDebugRows.floating(desired.posX),
      "pos_y" -> MySqlControlledDebugRows.floating(desired.posY),
      "pos_z" -> MySqlControlledDebugRows.floating(desired.posZ),
      "yaw" -> MySqlControlledDebugRows.floating(desired.yaw.toDouble),
      "pitch" -> MySqlControlledDebugRows.floating(desired.pitch.toDouble),
      "last_updated" -> signed(at),"last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(execute)),root)

  /** DDL6 12_bu：只在真实 RESERVED execute O / HELD POD 之后于 reserve Tx 同步目标 NULL。 */
  def normalizedLocatorTarget(snapshot: MySqlFormat3FullSnapshot, request: MySqlInactiveDomainUpdateRequest,
      execute: UUID, root: MySqlFormat3ReadBudget): Option[Vector[MySqlFormat3Cell]] =
    if execute == null || root == null || root.isClosed then fail("fixed nullable locator normalization scope required")
    val old = find(snapshot,Locator,row => matches(row,Locator,"pod_uuid",request.podUuid))
    if old == null || value(old,Locator,"domain_version") != MySqlFormat3Cell.SqlNull then None
    else
      root.reserveRetainedMemory(256L,"inactive reserve nullable locator exact cells")
      Some(MySqlControlledDebugRows.update(old,Locator,
        Map("domain_version" -> signed(request.expectedVersion),
          "locator_revision" -> signed(Math.addExact(integer(old,Locator,"locator_revision"),1L)),
          "last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(execute)),root))
  def locatorTarget(snapshot: MySqlFormat3FullSnapshot, request: MySqlInactiveDomainUpdateRequest,
      execute: UUID, root: MySqlFormat3ReadBudget): Option[Vector[MySqlFormat3Cell]] =
    if execute == null || root == null || root.isClosed then fail("fixed locator execute operation and original root required")
    val old = find(snapshot,Locator,row => matches(row,Locator,"pod_uuid",request.podUuid))
    if old == null then None
    else
      value(old,Locator,"domain_version") match
        case MySqlFormat3Cell.SqlNull => ()
        case MySqlFormat3Cell.SignedInteger(n) if n==request.expectedVersion => ()
        case _ => fail("exact original inactive locator version required")
      root.reserveRetainedMemory(256L,"inactive locator version-only map and optional row")
      Some(MySqlControlledDebugRows.update(old,Locator,
        Map("domain_version" -> signed(Math.addExact(request.expectedVersion,1L)),
          "locator_revision" -> signed(Math.addExact(integer(old,Locator,"locator_revision"),1L)),
          "last_mutation_operation_uuid" -> MySqlControlledDebugRows.uuid(execute)),root))

  private def uuidValue(row: Vector[MySqlFormat3Cell],table: String,column: String): UUID = value(row,table,column) match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length==16 =>
      var most=0L
      var least=0L
      var index=0
      while index<8 do
        most=(most << 8) | (bytes.byteAt(index).toLong & 255L)
        least=(least << 8) | (bytes.byteAt(index+8).toLong & 255L)
        index+=1
      new UUID(most,least)
    case _ => fail("exact source obligation UUID required")
  private def integer(row: Vector[MySqlFormat3Cell], table: String, column: String): Long = value(row,table,column) match
    case MySqlFormat3Cell.SignedInteger(n) => n
    case _ => fail("exact integer required: " + table + "." + column)
  private def matches(row: Vector[MySqlFormat3Cell], table: String, column: String, id: UUID): Boolean =
    value(row,table,column) match
      case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(id)
      case _ => false
  private def one(snapshot: MySqlFormat3FullSnapshot,table: String,predicate: Vector[MySqlFormat3Cell] => Boolean): Vector[MySqlFormat3Cell] =
    val result = find(snapshot,table,predicate)
    if result == null then fail("required exact row is missing: " + table)
    result
  private def find(snapshot: MySqlFormat3FullSnapshot,table: String,predicate: Vector[MySqlFormat3Cell] => Boolean): Vector[MySqlFormat3Cell] =
    var result: Vector[MySqlFormat3Cell] = null
    val rows = snapshot.relation(table).rows
    var index = 0
    while index < rows.size do
      if predicate(rows(index)) then
        if result != null then fail("duplicate exact row: " + table)
        result = rows(index)
      index += 1
    result
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive domain update: " + reason)
