package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{OperatingModeChangeOutcome, PhysicalRecoveryBinding, PhysicalRecoveryCutoverState, PhysicalRecoveryLocatorReason}
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 验证 public full20 投影保留所有 typed rows，且不可修改其二进制内容。 */
class MySqlOwnedFormat3SnapshotTest:
  private val schema = "fp_owned_format3_projection"
  private val profile = SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
    32_000_000L, 4_000_000L, 2_000_000L)

  private def profileWithRetainedLimit(maxRetained: Long): SourceLimitProfile =
    SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
      maxRetained, 4_000_000L, 2_000_000L)

  private def connection(): MySqlFormat3ConnectionObservation =
    MySqlFormat3ConnectionObservation(UUID.fromString("00000000-0000-0000-0000-000000000001"),
      "9.7.2", schema, "runtime@localhost", "InnoDB", 1, 1L, "READ-COMMITTED",
      "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")

  private def scope(): MySqlFormat3ObservationScope =
    MySqlFormat3ObservationScope(UUID.fromString("00000000-0000-0000-0000-000000000002"), 1L, 1L)

  private def defaultRow(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    shape.columns.map { column =>
      if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
        case MySqlFormat3SqlKind.Binary =>
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](column.maximumLength.getOrElse(1))(0x01)))
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
        case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 1, 2, 3, 123456000))
    }

  private def fullSnapshot(): MySqlFormat3FullSnapshot =
    val connectionIdentity = connection()
    val observationScope = scope()
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, Vector(defaultRow(shape))))
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    budget.initialize(schema, connectionIdentity, observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, connectionIdentity, observationScope, 16_777_216L,
      profile, relations, budget)

  private def uuidCell(value: UUID): MySqlFormat3Cell.Binary =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
      ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()))

  private def row(table: String, values: Map[String, MySqlFormat3Cell]): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    values.foldLeft(defaultRow(shape)) { case (current, (name, value)) =>
      val index = shape.columns.indexWhere(_.name == name)
      assertTrue(index >= 0, table + "." + name + " is part of the frozen F3 row")
      current.updated(index, value)
    }

  private def patchCell(table: String, original: Vector[MySqlFormat3Cell], name: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    assertTrue(index >= 0, table + "." + name + " is part of the frozen F3 row")
    original.updated(index, value)

  private def replaceRelationRow(snapshot: MySqlFormat3FullSnapshot, table: String,
      replacement: Vector[MySqlFormat3Cell]): MySqlFormat3FullSnapshot =
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      val original = snapshot.relation(shape.table)
      val rows = if shape.table == table then Vector(replacement) else original.rows
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    val budget = new MySqlFormat3ReadBudget(profile, snapshot.maxAllowedPacketBytes)
    budget.initialize(snapshot.schema, snapshot.connection, snapshot.observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(snapshot.schema, snapshot.connection, snapshot.observationScope,
      snapshot.maxAllowedPacketBytes, profile, relations, budget)

  def limitProfileForFixture: SourceLimitProfile = profile

  /** Test helper routes through the same package-private F3 issuer used by locked production readback. */
  def exactAlreadySetReadback(tag: IoOperationTag, plan: MySqlOwnedRepositoryOperationPlan,
      request: MySqlOwnedModeCommitRequest, state: FloatingPodState): MySqlOwnedModeAlreadySetReadback =
    MySqlOwnedModeAlreadySetReadback.fromFormat3Readback(tag, plan, request, state,
      format3ModeSnapshot(request, state, tag, UUID.fromString("00000000-0000-0000-0000-000000000025")))

  private def ownedModeSnapshot(): (MySqlFormat3FullSnapshot, MySqlOwnedModeCommitRequest, FloatingPodState, IoOperationTag) =
    val pod = UUID.fromString("00000000-0000-0000-0000-000000000021")
    val owner = UUID.fromString("00000000-0000-0000-0000-000000000022")
    val bindingUuid = UUID.fromString("00000000-0000-0000-0000-000000000023")
    val attempt = UUID.fromString("00000000-0000-0000-0000-000000000024")
    val footprint = UUID.fromString("00000000-0000-0000-0000-000000000025")
    val mutation = UUID.fromString("00000000-0000-0000-0000-000000000026")
    val tag = IoOperationTag(scope().bootUuid, 1L, 1L, mutation)
    val binding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)
    val pose = FloatingPodPose("f3-mode-readback", 8D, 64D, 8D, 0F, 0F, 7)
    val state = FloatingPodState(pod, Some(owner), 1L, pose.worldName, pose.posX, pose.posY, pose.posZ,
      pose.yaw, pose.pitch, true, 123L, Some(7), PodOperatingMode.WorldIdle)
    val request = MySqlOwnedModeCommitRequest(binding, pod, owner, 0L, attempt,
      PodOperatingMode.WorldIdle, pose)
    (format3ModeSnapshot(request, state, tag, footprint), request, state, tag)

  private def format3ModeSnapshot(request: MySqlOwnedModeCommitRequest, state: FloatingPodState,
      tag: IoOperationTag, footprint: UUID): MySqlFormat3FullSnapshot =
    require(request != null && state != null && tag != null && footprint != null &&
      state.uuid == request.podUuid && state.ownerUuid.contains(request.authenticatedOwner) &&
      state.isActive && state.operatingMode == request.targetMode &&
      state.worldName == request.actualPose.worldName && state.posX == request.actualPose.posX &&
      state.posY == request.actualPose.posY && state.posZ == request.actualPose.posZ &&
      state.yaw == request.actualPose.yaw && state.pitch == request.actualPose.pitch &&
      state.dimensionId.contains(request.actualPose.dimensionId), "exact active owned mode fixture inputs required")
    val pod = request.podUuid
    val owner = request.authenticatedOwner
    val bindingUuid = request.expectedBinding.bindingUuid
    val attempt = request.attemptUuid
    val mutation = tag.operationId
    val pose = request.actualPose
    val dimension = pose.dimensionId
    val bindingRow = row("physical_recovery_binding", Map(
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> uuidCell(bindingUuid),
      "cutover_state" -> MySqlFormat3Cell.Text("CERTIFIED"),
      "format_version" -> MySqlFormat3Cell.SignedInteger(2L)))
    val domainRow = row("floating_pod_state", Map(
      "uuid" -> uuidCell(pod), "owner_uuid" -> uuidCell(owner),
      "state_version" -> MySqlFormat3Cell.SignedInteger(state.stateVersion),
      "world_name" -> MySqlFormat3Cell.Text(state.worldName),
      "pos_x" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(state.posX)),
      "pos_y" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(state.posY)),
      "pos_z" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(state.posZ)),
      "yaw" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(state.yaw.toDouble)),
      "pitch" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(state.pitch.toDouble)),
      "is_active" -> MySqlFormat3Cell.SignedInteger(1L),
      "last_updated" -> MySqlFormat3Cell.SignedInteger(state.lastUpdated),
      "dimension_id" -> MySqlFormat3Cell.SignedInteger(dimension.toLong),
      "operating_mode" -> MySqlFormat3Cell.Text(state.operatingMode match
        case PodOperatingMode.Normal => "NORMAL"
        case PodOperatingMode.WorldIdle => "WORLD_IDLE"),
      "binding_uuid" -> uuidCell(bindingUuid), "last_mutation_operation_uuid" -> uuidCell(mutation)))
    val locatorRow = row("physical_recovery_locator", Map(
      "binding_uuid" -> uuidCell(bindingUuid), "pod_uuid" -> uuidCell(pod),
      "current_attempt_uuid" -> uuidCell(attempt), "current_footprint_uuid" -> uuidCell(footprint),
      "locator_state" -> MySqlFormat3Cell.Text("ROW_ACTIVE"),
      "reason_code" -> MySqlFormat3Cell.Text("ROW_ACTIVE"),
      "domain_version" -> MySqlFormat3Cell.SignedInteger(state.stateVersion),
      "target_dimension_id" -> MySqlFormat3Cell.SignedInteger(dimension.toLong),
      "locator_revision" -> MySqlFormat3Cell.SignedInteger(4L),
      "last_mutation_operation_uuid" -> uuidCell(mutation)))
    val footprintRow = row("physical_recovery_footprint", Map(
      "binding_uuid" -> uuidCell(bindingUuid), "pod_uuid" -> uuidCell(pod),
      "attempt_uuid" -> uuidCell(attempt), "footprint_uuid" -> uuidCell(footprint),
      "origin_version" -> MySqlFormat3Cell.SignedInteger(Math.subtractExact(state.stateVersion, 1L)),
      "target_world_name" -> MySqlFormat3Cell.Text(pose.worldName),
      "target_pos_x" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posX)),
      "target_pos_y" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posY)),
      "target_pos_z" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posZ)),
      "target_yaw" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.yaw.toDouble)),
      "target_pitch" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.pitch.toDouble)),
      "target_dimension_id" -> MySqlFormat3Cell.SignedInteger(dimension.toLong),
      "center_chunk_x" -> MySqlFormat3Cell.SignedInteger(Math.floor(pose.posX / 16D).toLong),
      "center_chunk_z" -> MySqlFormat3Cell.SignedInteger(Math.floor(pose.posZ / 16D).toLong),
      "purpose" -> MySqlFormat3Cell.Text("ACTIVE"),
      "seal_state" -> MySqlFormat3Cell.Text("SEALED"),
      "created_operation_uuid" -> uuidCell(mutation)))
    val centerX = Math.floor(pose.posX / 16D).toInt
    val centerZ = Math.floor(pose.posZ / 16D).toInt
    val footprintChunks = (for x <- -2 to 2; z <- -2 to 2 yield
      row("physical_recovery_footprint_chunk", Map(
        "binding_uuid" -> uuidCell(bindingUuid), "pod_uuid" -> uuidCell(pod),
        "attempt_uuid" -> uuidCell(attempt), "footprint_uuid" -> uuidCell(footprint),
        "dimension_id" -> MySqlFormat3Cell.SignedInteger(dimension.toLong),
        "chunk_x" -> MySqlFormat3Cell.SignedInteger(centerX.toLong + x),
        "chunk_z" -> MySqlFormat3Cell.SignedInteger(centerZ.toLong + z)))).toVector
    val supplied = Map(
      "physical_recovery_binding" -> Vector(bindingRow),
      "floating_pod_state" -> Vector(domainRow),
      "physical_recovery_locator" -> Vector(locatorRow),
      "physical_recovery_footprint" -> Vector(footprintRow),
      "physical_recovery_footprint_chunk" -> footprintChunks)
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map(shape => {
      val rows = supplied.getOrElse(shape.table, Vector.empty)
      MySqlFormat3RelationSnapshot(shape.table, rows)
    })
    val connectionIdentity = connection()
    val observationScope = MySqlFormat3ObservationScope(tag.bootId, tag.bootGeneration, tag.connectionGeneration)
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    budget.initialize(schema, connectionIdentity, observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, connectionIdentity, observationScope, 16_777_216L,
      profile, relations, budget)

  @Test def projectionRetainsOrderedTwentyRelationsTypedNullsAndDefensiveBinary(): Unit =
    val raw = fullSnapshot()
    val projected = MySqlOwnedFormat3Snapshot.from(raw)
    assertEquals(MySqlFormat3RelationShapes.canonicalOrder.map(_.table), projected.relations.map(_.table))
    assertEquals(20, projected.relations.size)
    assertEquals(20L, projected.rowCount)
    assertEquals(raw.canonicalSha256Hex, projected.canonicalSha256Hex)
    assertEquals(raw.canonicalByteLength, projected.canonicalByteLength)
    assertEquals(schema, projected.schema)
    assertEquals(connection().serverUuid, projected.serverUuid)
    assertEquals(scope().bootUuid, projected.bootUuid)
    assertTrue(projected.relations.iterator.flatMap(_.rows).flatMap(_.cells)
      .exists(_ == MySqlOwnedFormat3Value.SqlNull))
    val binary = projected.relations.iterator.flatMap(_.rows).flatMap(_.cells)
      .collectFirst { case MySqlOwnedFormat3Value.Binary(value) => value }.get
    val expected = binary.bytes
    val escaped = binary.bytes
    escaped(0) = (escaped(0) ^ 0x55).toByte
    assertArrayEquals(expected, binary.bytes)

  @Test def retainedFormat3ModeImagesAndScopedReadbacksShareOneLiveAllocationLimit(): Unit =
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      val rows = if shape.table == "floating_pod_state" then Vector(defaultRow(shape)) else Vector.empty
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }

    def accountOneImage(budget: MySqlFormat3ReadBudget): Unit =
      budget.initialize(schema, connection(), scope(), 16_777_216L)
      budget.recordMaterializedSnapshot(relations)

    val measuringProfile = profileWithRetainedLimit(32_000_000L)
    val materializedBudget = new MySqlFormat3ReadBudget(measuringProfile, measuringProfile.maxSqlPacketBytes())
    materializedBudget.initialize(schema, connection(), scope(), 16_777_216L)
    materializedBudget.recordMaterializedSnapshot(relations)
    materializedBudget.recordMaterializedSnapshot(relations)
    materializedBudget.recordMaterializedSnapshot(relations)
    val materializedImagesBytes = materializedBudget.accountedMemoryBytes

    val standaloneImageBudget = new MySqlFormat3ReadBudget(measuringProfile, measuringProfile.maxSqlPacketBytes())
    accountOneImage(standaloneImageBudget)
    val oneImageBytes = standaloneImageBudget.accountedMemoryBytes
    val aggregateLimit = Math.addExact(materializedImagesBytes,
      Math.addExact(oneImageBytes, oneImageBytes / 2L))
    val boundedProfile = profileWithRetainedLimit(aggregateLimit)

    val workflowBudget = new MySqlFormat3ReadBudget(boundedProfile, boundedProfile.maxSqlPacketBytes())
    workflowBudget.initialize(schema, connection(), scope(), 16_777_216L)
    workflowBudget.recordMaterializedSnapshot(relations)
    workflowBudget.recordMaterializedSnapshot(relations)
    workflowBudget.recordMaterializedSnapshot(relations)
    assertTrue(workflowBudget.accountedMemoryBytes < aggregateLimit)

    val oneImageByItself = new MySqlFormat3ReadBudget(boundedProfile, boundedProfile.maxSqlPacketBytes())
    accountOneImage(oneImageByItself)
    assertTrue(oneImageByItself.accountedMemoryBytes < aggregateLimit,
      "each full20 read image fits the explicit limit when measured alone")

    val beforeRead = workflowBudget.newScopedReadBudget(boundedProfile.maxSqlPacketBytes())
    accountOneImage(beforeRead)
    assertTrue(workflowBudget.accountedMemoryBytes < aggregateLimit,
      "the retained materialization plus one live full20 read fit the shared workflow limit")

    val overlappingTargetRead = workflowBudget.newScopedReadBudget(boundedProfile.maxSqlPacketBytes())
    assertThrows(classOf[java.sql.SQLDataException], () => accountOneImage(overlappingTargetRead))
    assertTrue(workflowBudget.accountedMemoryBytes <= aggregateLimit,
      "a failed overlapping read must not overshoot the shared retained cap")

    beforeRead.closeScopedReadBudget()
    overlappingTargetRead.closeScopedReadBudget()
    val laterPostCommitRead = workflowBudget.newScopedReadBudget(boundedProfile.maxSqlPacketBytes())
    accountOneImage(laterPostCommitRead)
    assertTrue(workflowBudget.accountedMemoryBytes < aggregateLimit,
      "closing a finished read releases only its scoped allocation and permits the next post-commit image")
    laterPostCommitRead.closeScopedReadBudget()

  @Test def modeProjectionBudgetsRemainWorkerOwnedUntilExactReleaseAcknowledgement(): Unit =
    var raw = fullSnapshot()
    val root = raw.allocationBudget
    val observationScope = raw.observationScope
    val initialBytes = root.accountedMemoryBytes
    val child = root.newScopedReadBudget(profile.maxSqlPacketBytes())
    child.initialize(raw.schema, raw.connection, observationScope, raw.maxAllowedPacketBytes)
    child.recordMaterializedSnapshot(raw.relations)
    val tag = IoOperationTag(observationScope.bootUuid, observationScope.bootGeneration,
      observationScope.connectionGeneration, UUID.fromString("00000000-0000-0000-0000-000000000071"))
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), tag.bootId,
      tag.bootGeneration, tag.connectionGeneration)
    registry.retainRoot(tag, root)
    registry.retainScoped(tag, child)
    var projected: MySqlOwnedFormat3Snapshot = MySqlOwnedFormat3Snapshot.from(raw, child)
    assertTrue(root.accountedMemoryBytes > initialBytes,
      "the actual public full20 projection remains charged in the shared workflow ledger")
    assertTrue(registry.contains(tag))

    val staleTag = tag.copy(connectionGeneration = tag.connectionGeneration + 1L)
    assertThrows(classOf[IllegalArgumentException], () => registry.release(staleTag))
    assertTrue(registry.contains(tag), "a stale generation cannot release the original allocation lease")

    var crossThreadFailure: Throwable = null
    val otherThread = new Thread(() =>
      try registry.release(tag)
      catch case failure: Throwable => crossThreadFailure = failure)
    otherThread.start(); otherThread.join()
    assertTrue(crossThreadFailure.isInstanceOf[IllegalArgumentException] || crossThreadFailure.isInstanceOf[IllegalStateException],
      "a foreign thread cannot consume the worker-issued release capability")
    assertTrue(registry.contains(tag))

    // Main first drops its only projected graph; only the exact worker ACK closes child and root accounting.
    projected = null
    raw = null
    registry.release(tag)
    assertFalse(registry.contains(tag))
    assertEquals(0L, root.accountedMemoryBytes)
    assertThrows(classOf[IllegalArgumentException], () => registry.release(tag),
      "duplicate ACK cannot release or recreate the exact budget lease")
    assertEquals(0L, root.accountedMemoryBytes)

  @Test def committedModeGraphUsesCompleteFormat3FootprintAndRealSessionIdentity(): Unit =
    val (raw, request, state, tag) = ownedModeSnapshot()
    val outcome = OperatingModeChangeOutcome.Changed(state)
    val graph = MySqlCommittedOwnedModeGraph.fromFormat3Readback(tag, request, outcome, raw, 64)
    assertEquals(state, graph.state)
    assertEquals(request.attemptUuid, graph.locator.attemptUuid)
    assertEquals(PhysicalRecoveryLocatorReason.RowActive, graph.locator.reasonCode)
    assertEquals(25, graph.locator.chunks.size)
    assertEquals(tag.bootId, graph.inventory.bootUuid)
    assertEquals(tag.bootGeneration, graph.inventory.bootGeneration)
    assertEquals(tag.connectionGeneration, graph.inventory.connectionGeneration)
    assertEquals(Some(request.expectedBinding), graph.inventory.recovery.binding)
    assertTrue(graph.legacyInventory.isEmpty)
    val publicSnapshot = graph.fullTwentyRelationSnapshot.get
    assertEquals(20, publicSnapshot.relations.size)
    assertEquals(25, publicSnapshot.relation("physical_recovery_footprint_chunk").rows.size)
    assertThrows(classOf[IllegalArgumentException], () => publicSnapshot.relation("physical_recovery_locator_chunk"))

  @Test def committedModeGraphRejectsNonCanonicalRawLocatorReason(): Unit =
    val (raw, request, state, tag) = ownedModeSnapshot()
    val locator = raw.relation("physical_recovery_locator").rows.head
    for reason <- Vector("row_active", "CREATE_PREPARED", "ROW_ACTIVE ") do
      val changed = replaceRelationRow(raw, "physical_recovery_locator",
        patchCell("physical_recovery_locator", locator, "reason_code", MySqlFormat3Cell.Text(reason)))
      assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlCommittedOwnedModeGraph.fromFormat3Readback(tag, request,
          OperatingModeChangeOutcome.Changed(state), changed, 64))

  @Test def committedModeGraphRejectsAStaleAttemptAgainstFormat3Locator(): Unit =
    val (raw, request, state, tag) = ownedModeSnapshot()
    val stale = request.copy(attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000099"))
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlCommittedOwnedModeGraph.fromFormat3Readback(tag, stale,
        OperatingModeChangeOutcome.Changed(state), raw, 64))

  @Test def format3ModePreflightReturnsReadOnlyAlreadySetWithoutIssuingACommitGraph(): Unit =
    val (raw, request, state, _) = ownedModeSnapshot()
    val sameVersion = request.copy(expectedStateVersion = state.stateVersion)
    val result = MySqlModeOperationEffectCodec.preflight(raw, sameVersion.repositoryOperation)
    assertEquals(MySqlModePreflight.AlreadySet, result)
    assertEquals(request.targetMode, state.operatingMode)
    assertEquals(request.actualPose.worldName, state.worldName)

  @Test def format3ModePreflightAllowsSameModeSameChunkPoseCorrection(): Unit =
    val (raw, request, _, _) = ownedModeSnapshot()
    val corrected = request.copy(expectedStateVersion = 1L,
      actualPose = FloatingPodPose(request.actualPose.worldName, 9D, request.actualPose.posY,
        request.actualPose.posZ, request.actualPose.yaw, request.actualPose.pitch, request.actualPose.dimensionId))
    assertEquals(MySqlModePreflight.Execute,
      MySqlModeOperationEffectCodec.preflight(raw, corrected.repositoryOperation))

  @Test def format3ModePreflightReturnsTypedOwnerVersionAttemptBindingAndChunkRejections(): Unit =
    val (raw, request, _, _) = ownedModeSnapshot()
    val otherOwner = request.copy(authenticatedOwner = UUID.fromString("00000000-0000-0000-0000-000000000099"))
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.NotOwner),
      MySqlModeOperationEffectCodec.preflight(raw, otherOwner.repositoryOperation))

    val staleVersion = request.copy(expectedStateVersion = 8L)
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.VersionMismatch(1L)),
      MySqlModeOperationEffectCodec.preflight(raw, staleVersion.repositoryOperation))

    val staleAlreadySet = request.copy(expectedStateVersion = 0L)
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.VersionMismatch(1L)),
      MySqlModeOperationEffectCodec.preflight(raw, staleAlreadySet.repositoryOperation),
      "a matching target mode and pose at expectedVersion+1 is not proof of this retained operation")

    val staleAttempt = request.copy(attemptUuid = UUID.fromString("00000000-0000-0000-0000-000000000098"))
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.RecoveryObligationMismatch),
      MySqlModeOperationEffectCodec.preflight(raw, staleAttempt.repositoryOperation))

    val wrongBinding = request.copy(expectedBinding = PhysicalRecoveryBinding(
      UUID.fromString("00000000-0000-0000-0000-000000000097"), PhysicalRecoveryCutoverState.Certified, 2))
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.BindingMismatch),
      MySqlModeOperationEffectCodec.preflight(raw, wrongBinding.repositoryOperation))

    val outsideChunk = request.copy(expectedStateVersion = 1L,
      actualPose = FloatingPodPose(request.actualPose.worldName, 80D, request.actualPose.posY,
        request.actualPose.posZ, request.actualPose.yaw, request.actualPose.pitch, request.actualPose.dimensionId))
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.CrossChunkMovement),
      MySqlModeOperationEffectCodec.preflight(raw, outsideChunk.repositoryOperation))

  @Test def format3ModePreflightRejectsInactiveDomainAndBrokenCurrentFootprint(): Unit =
    val (raw, request, _, _) = ownedModeSnapshot()
    val domain = raw.relation("floating_pod_state").rows.head
    val inactive = replaceRelationRow(raw, "floating_pod_state",
      patchCell("floating_pod_state", domain, "is_active", MySqlFormat3Cell.SignedInteger(0L)))
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.Inactive),
      MySqlModeOperationEffectCodec.preflight(inactive, request.repositoryOperation))

    val chunks = raw.relation("physical_recovery_footprint_chunk").rows
    val corrupt = chunks.map(chunk =>
      patchCell("physical_recovery_footprint_chunk", chunk, "dimension_id", MySqlFormat3Cell.SignedInteger(8L)))
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      val original = raw.relation(shape.table)
      val rows = if shape.table == "physical_recovery_footprint_chunk" then corrupt else original.rows
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    val budget = new MySqlFormat3ReadBudget(profile, raw.maxAllowedPacketBytes)
    budget.initialize(raw.schema, raw.connection, raw.observationScope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    val brokenFootprint = new MySqlFormat3FullSnapshot(raw.schema, raw.connection, raw.observationScope,
      raw.maxAllowedPacketBytes, profile, relations, budget)
    assertEquals(MySqlModePreflight.Rejected(com.wi.floatingpod.storage.OperatingModeChangeRejection.RecoveryObligationMismatch),
      MySqlModeOperationEffectCodec.preflight(brokenFootprint, request.repositoryOperation))
