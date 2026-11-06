package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, UnboundIntentOrigin}

import java.sql.{Connection, PreparedStatement, Types}
import java.util.UUID

/** 两个已验证完整图之间的确定性业务差异；拒绝清除物理历史或改变 bootstrap 权威。 */
private[mysql] final class MySqlFullRepositoryDelta private (
    val before: MySqlFullRepositorySnapshot,
    val after: MySqlFullRepositorySnapshot
):
  def isEmpty: Boolean = before.exactlyEquals(after)

  def apply(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope, expectation: MySqlSchemaAuditExpectation): Unit =
    require(verified != null && scope != null, "exact owned session scope required")
    verified.readmitSessionBarrier(expectation, scope)
    val schema = expectation.schema
    val connection = verified.connection
    require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "fixed schema identifier is unsafe")
    if connection.getAutoCommit then throw new IllegalStateException("full repository delta requires an active caller transaction")
    MySqlFullRepositoryDelta.validate(before, after)
    if !isEmpty then applyRows(connection, schema)

  private def applyRows(connection: Connection, schema: String): Unit =
    val bPods = before.pods.map(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid") -> row).toMap
    val aPods = after.pods.map(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid") -> row).toMap
    val bEntitlements = before.entitlements.map(row => row.playerUuid -> row).toMap
    val aEntitlements = after.entitlements.map(row => row.playerUuid -> row).toMap
    val bLocators = before.recovery.locators.map(row => row.podUuid -> row).toMap
    val aLocators = after.recovery.locators.map(row => row.podUuid -> row).toMap
    val bChunks = before.recovery.chunks.map(row => (row.podUuid, row.chunk) -> row).toMap
    val aChunks = after.recovery.chunks.map(row => (row.podUuid, row.chunk) -> row).toMap
    val bIntents = before.recovery.intents.map(row => row.podUuid -> row).toMap
    val aIntents = after.recovery.intents.map(row => row.podUuid -> row).toMap

    val insertedPods = aPods.keySet.diff(bPods.keySet).toVector.sortBy(_.toString).map(aPods)
    val updatedPods = bPods.keySet.intersect(aPods.keySet).toVector.sortBy(_.toString)
      .flatMap(id => Option.when(!MySqlFullRepositoryDelta.samePod(bPods(id), aPods(id)))(bPods(id) -> aPods(id)))
    val deletedPods = bPods.keySet.diff(aPods.keySet).toVector.sortBy(_.toString).map(bPods)
    val insertedEntitlements = aEntitlements.keySet.diff(bEntitlements.keySet).toVector.sortBy(_.toString).map(aEntitlements)
    val insertedIntents = aIntents.keySet.diff(bIntents.keySet).toVector.sortBy(_.toString).map(aIntents)
    val updatedIntents = bIntents.keySet.intersect(aIntents.keySet).toVector.sortBy(_.toString)
      .flatMap(id => Option.when(bIntents(id) != aIntents(id))(bIntents(id) -> aIntents(id)))
    val insertedLocators = aLocators.keySet.diff(bLocators.keySet).toVector.sortBy(_.toString).map(aLocators)
    val updatedLocators = bLocators.keySet.intersect(aLocators.keySet).toVector.sortBy(_.toString)
      .flatMap(id => Option.when(bLocators(id) != aLocators(id))(bLocators(id) -> aLocators(id)))
    val insertedChunks = aChunks.keySet.diff(bChunks.keySet).toVector
      .sortBy((pod, chunk) => (pod.toString, chunk.dimensionId, chunk.chunkX, chunk.chunkZ)).map(aChunks)

    insertedPods.foreach(insertPod(connection, schema, _))
    insertedEntitlements.foreach(insertEntitlement(connection, schema, _))
    // Unbound intent guards require the new inactive domain row and B.CERTIFIED to exist first.
    insertedIntents.foreach(insertIntent(connection, schema, _))
    // Its insert guard requires the PREPARED intent to be visible in this transaction.
    insertedLocators.foreach(insertLocator(connection, schema, _))
    insertedChunks.foreach(insertChunk(connection, schema, _))
    // Activation/deactivation domain UPDATE must precede the matching locator and intent transitions.
    updatedPods.foreach((oldRow, newRow) => updatePod(connection, schema, oldRow, newRow))
    updatedLocators.foreach((oldRow, newRow) => updateLocator(connection, schema, oldRow, newRow))
    updatedIntents.foreach((oldRow, newRow) => updateIntent(connection, schema, oldRow, newRow))
    deletedPods.foreach(deletePod(connection, schema, _))

  private def table(schema: String, name: String): String =
    if !name.matches("[a-z][a-z0-9_]{0,63}") then throw new IllegalArgumentException("internal MySQL table identifier is unsafe")
    s"`$schema`.`$name`"

  private def statement[A](connection: Connection, sql: String)(body: PreparedStatement => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql))(body)

  private def insertPod(connection: Connection, schema: String, row: MySqlStoredRowData): Unit =
    val sql = s"INSERT INTO ${table(schema, "floating_pod_state")} (uuid,owner_uuid,state_version,world_name,pos_x,pos_y,pos_z,yaw,pitch,is_active,last_updated,dimension_id,operating_mode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    statement(connection, sql) { p =>
      bindPod(p, row, 1)
      exactlyOne(p.executeUpdate(), "insert pod")
    }

  private def updatePod(connection: Connection, schema: String, before: MySqlStoredRowData, after: MySqlStoredRowData): Unit =
    val sql = s"UPDATE ${table(schema, "floating_pod_state")} SET owner_uuid=?,state_version=?,world_name=?,pos_x=?,pos_y=?,pos_z=?,yaw=?,pitch=?,is_active=?,last_updated=?,dimension_id=?,operating_mode=? WHERE uuid=? AND state_version=?"
    statement(connection, sql) { p =>
      bindPodFields(p, after, 1)
      p.setBytes(13, before.uuidBytes.toArray)
      p.setLong(14, before.stateVersion)
      exactlyOne(p.executeUpdate(), "update pod")
    }

  private def deletePod(connection: Connection, schema: String, row: MySqlStoredRowData): Unit =
    val sql = s"DELETE FROM ${table(schema, "floating_pod_state")} WHERE uuid=? AND state_version=? AND is_active=0"
    statement(connection, sql) { p =>
      p.setBytes(1, row.uuidBytes.toArray)
      p.setLong(2, row.stateVersion)
      exactlyOne(p.executeUpdate(), "delete inactive pod")
    }

  private def insertEntitlement(connection: Connection, schema: String, row: PlayerEntitlement): Unit =
    val sql = s"INSERT INTO ${table(schema, "player_entitlement")} (player_uuid,registration_limit,simultaneous_active_limit) VALUES (?,?,?)"
    statement(connection, sql) { p =>
      p.setBytes(1, MySqlStoredRows.encodeUuid(row.playerUuid).toArray)
      p.setInt(2, row.registrationLimit)
      p.setInt(3, row.simultaneousActiveLimit)
      exactlyOne(p.executeUpdate(), "insert player entitlement")
    }

  private def insertLocator(connection: Connection, schema: String, row: PhysicalRecoveryLocator): Unit =
    val sql = s"INSERT INTO ${table(schema, "physical_recovery_locator")} (pod_uuid,attempt_uuid,locator_state,reason_code,domain_version,target_dimension_id) VALUES (?,?,?,?,?,?)"
    statement(connection, sql) { p =>
      p.setBytes(1, MySqlStoredRows.encodeUuid(row.podUuid).toArray)
      p.setBytes(2, MySqlStoredRows.encodeUuid(row.attemptUuid).toArray)
      p.setString(3, row.state.databaseValue)
      p.setString(4, row.reasonCode)
      setOptionalLong(p, 5, row.domainVersion)
      setOptionalInt(p, 6, row.targetDimensionId)
      exactlyOne(p.executeUpdate(), "insert recovery locator")
    }

  private def updateLocator(connection: Connection, schema: String, before: PhysicalRecoveryLocator, after: PhysicalRecoveryLocator): Unit =
    val sql = s"UPDATE ${table(schema, "physical_recovery_locator")} SET locator_state=?,reason_code=?,domain_version=?,target_dimension_id=? WHERE pod_uuid=? AND attempt_uuid=? AND locator_state=? AND reason_code=? AND domain_version <=> ? AND target_dimension_id <=> ?"
    statement(connection, sql) { p =>
      p.setString(1, after.state.databaseValue)
      p.setString(2, after.reasonCode)
      setOptionalLong(p, 3, after.domainVersion)
      setOptionalInt(p, 4, after.targetDimensionId)
      p.setBytes(5, MySqlStoredRows.encodeUuid(before.podUuid).toArray)
      p.setBytes(6, MySqlStoredRows.encodeUuid(before.attemptUuid).toArray)
      p.setString(7, before.state.databaseValue)
      p.setString(8, before.reasonCode)
      setOptionalLong(p, 9, before.domainVersion)
      setOptionalInt(p, 10, before.targetDimensionId)
      exactlyOne(p.executeUpdate(), "update recovery locator")
    }

  private def insertChunk(connection: Connection, schema: String, row: MySqlRecoveryChunkRow): Unit =
    val sql = s"INSERT INTO ${table(schema, "physical_recovery_locator_chunk")} (pod_uuid,dimension_id,chunk_x,chunk_z) VALUES (?,?,?,?)"
    statement(connection, sql) { p =>
      p.setBytes(1, MySqlStoredRows.encodeUuid(row.podUuid).toArray)
      p.setInt(2, row.chunk.dimensionId)
      p.setInt(3, row.chunk.chunkX)
      p.setInt(4, row.chunk.chunkZ)
      exactlyOne(p.executeUpdate(), "insert recovery locator chunk")
    }

  private def insertIntent(connection: Connection, schema: String, row: MySqlRecoveryIntentData): Unit =
    val sql =
      s"INSERT INTO ${table(schema, "physical_recovery_intent")} " +
        "(pod_uuid,attempt_uuid,binding_uuid,origin,phase,source_kind,source_identity,catalog_generation,create_operation_uuid,prepared_domain_version," +
        "target_world_name,target_pos_x,target_pos_y,target_pos_z,target_yaw,target_pitch,target_dimension_id,target_chunk_x,target_chunk_z," +
        "committed_domain_version,committed_at,committed_world_name,committed_pos_x,committed_pos_y,committed_pos_z,committed_yaw,committed_pitch,committed_dimension_id) " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    statement(connection, sql) { p =>
      bindIntent(p, row)
      exactlyOne(p.executeUpdate(), "insert PREPARED physical recovery intent")
    }

  private def updateIntent(connection: Connection, schema: String, before: MySqlRecoveryIntentData, after: MySqlRecoveryIntentData): Unit =
    if before.phase != MySqlRecoveryIntentPhase.Prepared || after.phase != MySqlRecoveryIntentPhase.Committed then
      throw new IllegalStateException("physical recovery intent updates must be PREPARED to COMMITTED")
    val sql =
      s"UPDATE ${table(schema, "physical_recovery_intent")} SET phase=?,committed_domain_version=?,committed_at=?,committed_world_name=?," +
        "committed_pos_x=?,committed_pos_y=?,committed_pos_z=?,committed_yaw=?,committed_pitch=?,committed_dimension_id=? " +
        "WHERE pod_uuid=? AND attempt_uuid=? AND phase='PREPARED'"
    statement(connection, sql) { p =>
      p.setString(1, "COMMITTED")
      setOptionalLong(p, 2, after.committedDomainVersion)
      setOptionalLong(p, 3, after.committedAt)
      setOptionalText(p, 4, after.committedWorldName)
      setOptionalDouble(p, 5, after.committedPosX)
      setOptionalDouble(p, 6, after.committedPosY)
      setOptionalDouble(p, 7, after.committedPosZ)
      setOptionalDouble(p, 8, after.committedYaw)
      setOptionalDouble(p, 9, after.committedPitch)
      setOptionalInt(p, 10, after.committedDimensionId)
      p.setBytes(11, MySqlStoredRows.encodeUuid(before.podUuid).toArray)
      p.setBytes(12, MySqlStoredRows.encodeUuid(before.attemptUuid).toArray)
      exactlyOne(p.executeUpdate(), "commit physical recovery intent")
    }

  private def bindIntent(p: PreparedStatement, i: MySqlRecoveryIntentData): Unit =
    p.setBytes(1, MySqlStoredRows.encodeUuid(i.podUuid).toArray)
    p.setBytes(2, MySqlStoredRows.encodeUuid(i.attemptUuid).toArray)
    p.setBytes(3, MySqlStoredRows.encodeUuid(i.bindingUuid).toArray)
    p.setString(4, i.origin match
      case UnboundIntentOrigin.ObservedArtifactAdoption => "UNBOUND_ADOPTION"
      case UnboundIntentOrigin.DebugItemCreate => "DEBUG_ITEM_CREATE")
    p.setString(5, "PREPARED")
    setOptionalText(p, 6, i.sourceKind.map {
      case com.wi.floatingpod.storage.ObservedUnboundSourceKind.FreshArtifact => "FRESH"
      case com.wi.floatingpod.storage.ObservedUnboundSourceKind.LegacyUnregistered => "LEGACY_UNREGISTERED"
    })
    setOptionalText(p, 7, i.sourceIdentity)
    setOptionalUuid(p, 8, i.catalogGeneration)
    setOptionalUuid(p, 9, i.createOperationUuid)
    p.setLong(10, i.preparedDomainVersion)
    p.setString(11, i.targetWorldName)
    p.setDouble(12, i.targetPosX)
    p.setDouble(13, i.targetPosY)
    p.setDouble(14, i.targetPosZ)
    p.setDouble(15, i.targetYaw)
    p.setDouble(16, i.targetPitch)
    p.setInt(17, i.targetDimensionId)
    p.setInt(18, i.targetChunkX)
    p.setInt(19, i.targetChunkZ)
    setOptionalLong(p, 20, i.committedDomainVersion)
    setOptionalLong(p, 21, i.committedAt)
    setOptionalText(p, 22, i.committedWorldName)
    setOptionalDouble(p, 23, i.committedPosX)
    setOptionalDouble(p, 24, i.committedPosY)
    setOptionalDouble(p, 25, i.committedPosZ)
    setOptionalDouble(p, 26, i.committedYaw)
    setOptionalDouble(p, 27, i.committedPitch)
    setOptionalInt(p, 28, i.committedDimensionId)

  private def bindPod(p: PreparedStatement, row: MySqlStoredRowData, start: Int): Unit =
    p.setBytes(start, row.uuidBytes.toArray)
    bindPodFields(p, row, start + 1)

  private def bindPodFields(p: PreparedStatement, row: MySqlStoredRowData, start: Int): Unit =
    setOptionalBytes(p, start, row.ownerUuidBytes)
    p.setLong(start + 1, row.stateVersion)
    p.setString(start + 2, row.worldName)
    p.setDouble(start + 3, row.posX)
    p.setDouble(start + 4, row.posY)
    p.setDouble(start + 5, row.posZ)
    p.setDouble(start + 6, row.yaw)
    p.setDouble(start + 7, row.pitch)
    p.setInt(start + 8, row.isActiveFlag)
    p.setLong(start + 9, row.lastUpdated)
    setOptionalInt(p, start + 10, row.dimensionId)
    p.setString(start + 11, row.operatingModeToken)

  private def setOptionalBytes(p: PreparedStatement, index: Int, value: Option[Vector[Byte]]): Unit =
    value match
      case None => p.setNull(index, Types.VARBINARY)
      case Some(bytes) => p.setBytes(index, bytes.toArray)
  private def setOptionalUuid(p: PreparedStatement, index: Int, value: Option[UUID]): Unit =
    value match
      case None => p.setNull(index, Types.VARBINARY)
      case Some(id) => p.setBytes(index, MySqlStoredRows.encodeUuid(id).toArray)
  private def setOptionalInt(p: PreparedStatement, index: Int, value: Option[Int]): Unit =
    value match
      case None => p.setNull(index, Types.INTEGER)
      case Some(number) => p.setInt(index, number)
  private def setOptionalLong(p: PreparedStatement, index: Int, value: Option[Long]): Unit =
    value match
      case None => p.setNull(index, Types.BIGINT)
      case Some(number) => p.setLong(index, number)
  private def setOptionalDouble(p: PreparedStatement, index: Int, value: Option[Double]): Unit =
    value match
      case None => p.setNull(index, Types.DOUBLE)
      case Some(number) => p.setDouble(index, number)
  private def setOptionalText(p: PreparedStatement, index: Int, value: Option[String]): Unit =
    value match
      case None => p.setNull(index, Types.VARCHAR)
      case Some(text) => p.setString(index, text)

  private def exactlyOne(count: Int, operation: String): Unit =
    if count != 1 then throw new java.sql.SQLTransactionRollbackException(s"MySQL $operation affected $count rows instead of one")

object MySqlFullRepositoryDelta:
  def freeze(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): MySqlFullRepositoryDelta =
    require(before != null && after != null, "full snapshot delta requires before and after graphs")
    before.validate()
    after.validate()
    validate(before, after)
    new MySqlFullRepositoryDelta(before, after)

  private[mysql] def validate(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): Unit =
    if before.recovery.binding != after.recovery.binding || before.bootstrapIntents != after.bootstrapIntents then
      reject("normal certified snapshot mutation cannot change physical binding or B1 bootstrap evidence")

    val beforePods = before.pods.map(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid") -> row).toMap
    val afterPods = after.pods.map(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid") -> row).toMap
    beforePods.keySet.intersect(afterPods.keySet).foreach { id =>
      val oldRow = beforePods(id)
      val newRow = afterPods(id)
      if !samePod(oldRow, newRow) then validatePodTransition(oldRow, newRow, before, after)
    }
    beforePods.keySet.diff(afterPods.keySet).foreach { id =>
      val oldRow = beforePods(id)
      if oldRow.isActiveFlag != 0 || before.recovery.locators.exists(_.podUuid == id) ||
        before.recovery.chunks.exists(_.podUuid == id) || before.recovery.intents.exists(_.podUuid == id) then
        reject("domain deletion must be inactive and free of every locator, chunk, and intent reference")
    }
    afterPods.keySet.diff(beforePods.keySet).foreach { id =>
      val row = afterPods(id)
      if row.isActiveFlag != 0 || row.stateVersion != 0L then reject("generic domain insert must be inactive at version zero")
    }

    val beforeEntitlements = before.entitlements.map(row => row.playerUuid -> row).toMap
    val afterEntitlements = after.entitlements.map(row => row.playerUuid -> row).toMap
    if beforeEntitlements.keySet.diff(afterEntitlements.keySet).nonEmpty then reject("runtime entitlement delete is not authorized")
    beforeEntitlements.keySet.intersect(afterEntitlements.keySet).foreach { id =>
      if beforeEntitlements(id) != afterEntitlements(id) then reject("runtime entitlement update is not authorized")
    }

    validateLocatorTransitions(before, after)
    validateChunkTransitions(before, after)
    validateIntentTransitions(before, after)

  private def validatePodTransition(
      oldRow: MySqlStoredRowData,
      newRow: MySqlStoredRowData,
      before: MySqlFullRepositorySnapshot,
      after: MySqlFullRepositorySnapshot
  ): Unit =
    if oldRow.uuidBytes != newRow.uuidBytes then reject("domain UUID is immutable")
    if newRow.stateVersion == oldRow.stateVersion then
      val allowedBackfill =
        oldRow.isActiveFlag == 1 && oldRow.dimensionId.isEmpty && newRow.dimensionId.nonEmpty &&
          samePodExceptDimension(oldRow, newRow)
      if !allowedBackfill then reject("same-version domain mutation is not the authorized NULL-dimension backfill")
    else
      if oldRow.stateVersion == Long.MaxValue || newRow.stateVersion != oldRow.stateVersion + 1L then
        reject("domain version must increase exactly once")
      if oldRow.operatingModeToken != newRow.operatingModeToken &&
        !(oldRow.isActiveFlag == 1 && newRow.isActiveFlag == 1 && oldRow.ownerUuidBytes.nonEmpty) then
        reject("operating mode changes require an owned active tracked transition")
      if oldRow.isActiveFlag == 0 && newRow.isActiveFlag == 1 then
        val oldLocator = before.recovery.locators.find(_.podUuid == MySqlStoredRows.decodeUuid(oldRow.uuidBytes, "uuid"))
        val nextLocator = after.recovery.locators.find(_.podUuid == MySqlStoredRows.decodeUuid(newRow.uuidBytes, "uuid"))
        if !nextLocator.exists(locator => locator.state == PhysicalRecoveryLocatorState.RowActive &&
          locator.domainVersion.contains(newRow.stateVersion)) then reject("activation requires an atomically updated ROW_ACTIVE locator")
        if oldLocator.isEmpty then reject("activation requires a pre-existing CREATE_PREPARED or unbound prepared locator")
      if oldRow.isActiveFlag == 1 && newRow.isActiveFlag == 0 then
        if !after.recovery.locators.exists(locator => locator.podUuid == MySqlStoredRows.decodeUuid(newRow.uuidBytes, "uuid") &&
          locator.state == PhysicalRecoveryLocatorState.RowInactivePending && locator.domainVersion.contains(newRow.stateVersion)) then
          reject("tracked deactivation requires a ROW_INACTIVE_PENDING locator")
      if oldRow.isActiveFlag == newRow.isActiveFlag && oldRow.isActiveFlag == 1 then
        val id = MySqlStoredRows.decodeUuid(newRow.uuidBytes, "uuid")
        val oldLocator = before.recovery.locators.find(_.podUuid == id)
        val nextLocator = after.recovery.locators.find(_.podUuid == id)
        if oldLocator.exists(_.state == PhysicalRecoveryLocatorState.RowActive) &&
          !nextLocator.exists(locator => locator.state == PhysicalRecoveryLocatorState.RowActive &&
            locator.domainVersion.contains(newRow.stateVersion)) then
          reject("tracked active update requires the locator domainVersion to move in the same transaction")

  private def validateLocatorTransitions(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): Unit =
    val oldRows = before.recovery.locators.map(row => row.podUuid -> row).toMap
    val newRows = after.recovery.locators.map(row => row.podUuid -> row).toMap
    if oldRows.keySet.diff(newRows.keySet).nonEmpty then reject("physical locator history cannot be deleted")
    newRows.foreach { case (id, next) =>
      oldRows.get(id) match
        case None =>
          if next.state != PhysicalRecoveryLocatorState.CreatePrepared &&
            next.state != PhysicalRecoveryLocatorState.UnboundAdoptionPrepared &&
            next.state != PhysicalRecoveryLocatorState.DebugItemCreatePrepared then
            reject("new locator must be a typed PREPARED state")
        case Some(old) if old == next => ()
        case Some(old) =>
          if old.attemptUuid != next.attemptUuid || old.targetDimensionId != next.targetDimensionId || old.chunks != next.chunks then
            reject("locator identity, target dimension, and chunk footprint are immutable")
          val transitionAllowed =
            (old.state == PhysicalRecoveryLocatorState.CreatePrepared && next.state == PhysicalRecoveryLocatorState.RowActive) ||
              ((old.state == PhysicalRecoveryLocatorState.UnboundAdoptionPrepared || old.state == PhysicalRecoveryLocatorState.DebugItemCreatePrepared) &&
                next.state == PhysicalRecoveryLocatorState.RowActive) ||
              (old.state == PhysicalRecoveryLocatorState.RowActive && next.state == PhysicalRecoveryLocatorState.RowActive &&
                next.domainVersion == old.domainVersion.map(_ + 1L)) ||
              (old.state == PhysicalRecoveryLocatorState.RowActive && next.state == PhysicalRecoveryLocatorState.RowInactivePending) ||
              (old.state == PhysicalRecoveryLocatorState.RowInactivePending && next.state == PhysicalRecoveryLocatorState.RowInactiveVerified &&
                next.domainVersion == old.domainVersion)
          if !transitionAllowed then reject("physical locator lifecycle transition is not permitted")
    }

  private def validateChunkTransitions(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): Unit =
    val oldChunks = before.recovery.chunks.map(row => (row.podUuid, row.chunk)).toSet
    val nextChunks = after.recovery.chunks.map(row => (row.podUuid, row.chunk)).toSet
    if !oldChunks.subsetOf(nextChunks) then reject("physical chunk history cannot be removed or changed")
    val newParents = after.recovery.locators.map(_.podUuid).toSet.diff(before.recovery.locators.map(_.podUuid).toSet)
    if (nextChunks.diff(oldChunks).exists((podUuid, _) => !newParents.contains(podUuid))) then
      reject("new physical chunks require a new prepared locator parent")

  private def validateIntentTransitions(before: MySqlFullRepositorySnapshot, after: MySqlFullRepositorySnapshot): Unit =
    val oldRows = before.recovery.intents.map(row => row.podUuid -> row).toMap
    val newRows = after.recovery.intents.map(row => row.podUuid -> row).toMap
    if oldRows.keySet.diff(newRows.keySet).nonEmpty then reject("physical intent history cannot be deleted")
    newRows.foreach { case (id, next) =>
      oldRows.get(id) match
        case None =>
          if next.phase != MySqlRecoveryIntentPhase.Prepared then reject("new physical intent must begin PREPARED")
        case Some(old) if old == next => ()
        case Some(old) =>
          if old.phase != MySqlRecoveryIntentPhase.Prepared || next.phase != MySqlRecoveryIntentPhase.Committed ||
            !sameIntentImmutableFields(old, next) then
            reject("physical intent permits only the exact PREPARED to COMMITTED transition")
    }

  private def sameIntentImmutableFields(a: MySqlRecoveryIntentData, b: MySqlRecoveryIntentData): Boolean =
    a.podUuid == b.podUuid && a.attemptUuid == b.attemptUuid && a.bindingUuid == b.bindingUuid &&
      a.origin == b.origin && a.sourceKind == b.sourceKind && a.sourceIdentity == b.sourceIdentity &&
      a.sourceIdentitySha256 == b.sourceIdentitySha256 && a.catalogGeneration == b.catalogGeneration &&
      a.createOperationUuid == b.createOperationUuid && a.preparedDomainVersion == b.preparedDomainVersion &&
      a.targetWorldName == b.targetWorldName && sameDouble(a.targetPosX, b.targetPosX) &&
      sameDouble(a.targetPosY, b.targetPosY) && sameDouble(a.targetPosZ, b.targetPosZ) &&
      sameDouble(a.targetYaw, b.targetYaw) && sameDouble(a.targetPitch, b.targetPitch) &&
      a.targetDimensionId == b.targetDimensionId && a.targetChunkX == b.targetChunkX && a.targetChunkZ == b.targetChunkZ

  private def samePod(a: MySqlStoredRowData, b: MySqlStoredRowData): Boolean =
    a.uuidBytes == b.uuidBytes && a.ownerUuidBytes == b.ownerUuidBytes && a.stateVersion == b.stateVersion &&
      a.worldName == b.worldName && sameDouble(a.posX, b.posX) && sameDouble(a.posY, b.posY) &&
      sameDouble(a.posZ, b.posZ) && sameDouble(a.yaw, b.yaw) && sameDouble(a.pitch, b.pitch) &&
      a.isActiveFlag == b.isActiveFlag && a.lastUpdated == b.lastUpdated &&
      a.dimensionId == b.dimensionId && a.operatingModeToken == b.operatingModeToken

  private def samePodExceptDimension(a: MySqlStoredRowData, b: MySqlStoredRowData): Boolean =
    a.uuidBytes == b.uuidBytes && a.ownerUuidBytes == b.ownerUuidBytes && a.stateVersion == b.stateVersion &&
      a.worldName == b.worldName && sameDouble(a.posX, b.posX) && sameDouble(a.posY, b.posY) &&
      sameDouble(a.posZ, b.posZ) && sameDouble(a.yaw, b.yaw) && sameDouble(a.pitch, b.pitch) &&
      a.isActiveFlag == b.isActiveFlag && a.lastUpdated == b.lastUpdated &&
      a.operatingModeToken == b.operatingModeToken

  private def reject(reason: String): Nothing = throw new IllegalStateException("MySQL full repository delta violates " + reason)
  private def sameDouble(a: Double, b: Double): Boolean =
    java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
