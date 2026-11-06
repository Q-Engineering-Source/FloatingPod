package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 原full20 materializer/preflight的WorldIdle两owner态；typed DATA，不伪造SQL/physical ACK。 */
final class MySqlWorldIdleShutdownCheckpointTest:
  @Test def ownedAndUnclaimedWorldIdleRetainActiveRowModeOwnerAttemptAndExact25(): Unit =
    for unclaimed <- Vector(false, true) do
      val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
      val f = images.fixture
      val roots = Vector(images.before, images.reserveTarget, images.executeTarget).map(_.allocationBudget).distinct
      try
        val originalRow = images.before.relation("floating_pod_state").rows.head
        val ownerIndex = MySqlFormat3RelationShapes.forTable("floating_pod_state").columns.indexWhere(_.name == "owner_uuid")
        val modeIndex = MySqlFormat3RelationShapes.forTable("floating_pod_state").columns.indexWhere(_.name == "operating_mode")
        val changed = originalRow.updated(modeIndex, MySqlFormat3Cell.Text(PodOperatingMode.WorldIdle.databaseValue))
        val row = if unclaimed then changed.updated(ownerIndex, MySqlFormat3Cell.SqlNull) else changed
        val before = new MySqlFormat3FullSnapshot(images.before.schema, images.before.connection,
          images.before.observationScope, images.before.maxAllowedPacketBytes, f.profile,
          images.before.relations.map(r => if r.table == "floating_pod_state" then r.copy(rows = Vector(row)) else r),
          images.before.allocationBudget)
        val owner = if unclaimed then None else Some(UUID.fromString("00000000-0000-0000-0000-000000000012"))
        val base = FloatingPodState(f.podUuid, owner, 0L, "world", 0D, 64D, 0D, 0F, 0F,
          true, 100L, Some(7), PodOperatingMode.WorldIdle)
        val request = MySqlTrackedDriftRequest(f.binding, base, base.copy(posX = 2D, yaw = 12F), f.attemptUuid)
        val operation = request.repositoryOperation
        val plan = MySqlOwnedRepositoryOperationPlan.single(f.plan.tag, f.plan.workflowUuid,
          f.plan.reserveOperationUuid, operation, f.profile)
        val old = f.context
        val context = new MySqlRepositoryOperationContextIdentity(old.tag, old.workflowUuid, old.reserveOperationUuid,
          old.barrierGenerationUuid, old.actualConnectionId, old.barrierNamespace, old.bindingUuid, old.writerEpoch,
          old.fixedLeaseUntil, old.serverUuid, old.serverVersion, old.schema, old.effectiveUser, old.provisioningUuid,
          old.schemaFormat, old.physicalFormat, old.ddlSha256, old.approvedObjectManifestSha256,
          old.configurationSha256, old.sourceProvenanceKind, old.sourceProvenanceSha256,
          old.limitProfileSha256, "RUN", plan.requestIdentitySha256, f.profile)
        assertEquals(MySqlDriftPreflight.Execute, MySqlDriftOperationEffectCodec.preflight(before, operation))
        val result = MySqlDriftWorkflowMaterializer.materialize(plan, operation, context,
          LocalDateTime.of(2026, 10, 8, 17, 45, 0, 123456000), before, 0L)
        result.verify()
        def cell(table: String, name: String): MySqlFormat3Cell =
          MySqlDriftOperationEffectCodec.value(result.executeTarget.relation(table).rows.head, table, name)
        assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell("floating_pod_state", "is_active"))
        assertEquals(MySqlFormat3Cell.Text(PodOperatingMode.WorldIdle.databaseValue), cell("floating_pod_state", "operating_mode"))
        assertEquals(row(ownerIndex), cell("floating_pod_state", "owner_uuid"))
        assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell("floating_pod_state", "state_version"))
        assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(2D)), cell("floating_pod_state", "pos_x"))
        assertEquals(MySqlFormat3Cell.Text("ROW_ACTIVE"), cell("physical_recovery_locator", "locator_state"))
        assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell("physical_recovery_locator", "domain_version"))
        assertEquals(before.relation("physical_recovery_footprint").rows, result.executeTarget.relation("physical_recovery_footprint").rows)
        assertEquals(before.relation("physical_recovery_footprint_chunk").rows, result.executeTarget.relation("physical_recovery_footprint_chunk").rows)
        assertEquals(25, result.executeTarget.relation("physical_recovery_footprint_chunk").rows.size)
        val foreignOwner = Some(UUID.randomUUID())
        val wrongOwner = request.copy(criticalBase = base.copy(ownerUuid = foreignOwner),
          desired = request.desired.copy(ownerUuid = foreignOwner))
        assertInstanceOf(classOf[MySqlDriftPreflight.Rejected],
          MySqlDriftOperationEffectCodec.preflight(before, wrongOwner.repositoryOperation))
      finally roots.foreach(_.closeRootReadBudget())
