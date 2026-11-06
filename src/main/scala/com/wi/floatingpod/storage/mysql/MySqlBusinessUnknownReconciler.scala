package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import scala.util.{Failure, Success, Try}

private[mysql] final case class MySqlBusinessUnknownDeleteReferences(
    locatorPodUuids: Vector[UUID],
    chunkPodUuids: Vector[UUID],
    intentPodUuids: Vector[UUID]
):
  require(locatorPodUuids != null && chunkPodUuids != null && intentPodUuids != null, "delete-reference readbacks are required")
  require(locatorPodUuids.forall(_ != null) && chunkPodUuids.forall(_ != null) && intentPodUuids.forall(_ != null), "delete-reference rows must not be null")
  override def toString: String =
    s"MySqlBusinessUnknownDeleteReferences(locatorRows=${locatorPodUuids.size}, chunkRows=${chunkPodUuids.size}, intentRows=${intentPodUuids.size})"

/** 完整且有类型的当前业务效果读回；map 缺键与 SQL NULL/无行是不同状态。 */
private[mysql] final case class MySqlBusinessUnknownEffectReadback(
    podRows: Map[UUID, Option[MySqlStoredRowData]],
    entitlementRows: Map[UUID, Option[PlayerEntitlement]],
    deletedPodReferences: Map[UUID, MySqlBusinessUnknownDeleteReferences],
    fullRepositorySnapshot: Option[MySqlFullRepositorySnapshot] = None
):
  require(podRows != null && entitlementRows != null && deletedPodReferences != null && fullRepositorySnapshot != null,
    "business effect readback values are required")
  require(podRows.forall((uuid, row) => uuid != null && row != null && row.forall(_ != null)), "pod readback entries must be non-null")
  require(entitlementRows.forall((uuid, row) => uuid != null && row != null && row.forall(_ != null)), "entitlement readback entries must be non-null")
  require(deletedPodReferences.forall((uuid, refs) => uuid != null && refs != null), "delete reference readback entries must be non-null")
  require(fullRepositorySnapshot.forall(_ != null), "full repository snapshot readback must not contain null")
  override def toString: String =
    s"MySqlBusinessUnknownEffectReadback(pods=${podRows.size}, entitlements=${entitlementRows.size}, deletions=${deletedPodReferences.size}, fullSnapshot=${fullRepositorySnapshot.isDefined})"

/** sealed 证明由本文件中的 JDBC reconciler 唯一实现，不能由普通调用端构造或用 Boolean 代替。 */
private[mysql] sealed trait MySqlBusinessUnknownProof:
  private[mysql] def snapshot: MySqlBusinessUnknownSnapshot
  private[mysql] def manifest: MySqlLockedManifest
  private[mysql] def binding: MySqlLockedBinding
  private[mysql] def writerRow: MySqlBusinessWriterRow
  private[mysql] def effectReadback: MySqlBusinessUnknownEffectReadback
  private[mysql] def monotonicAnchorNanos: Long

/** 只有完整类型化读回与冻结计划完全相符才接受证明，不接受调用端成功标志。 */
private[mysql] object MySqlBusinessUnknownProofRules:
  def requireFrozenResponsibility(
      plan: Option[MySqlBusinessEffectPlan],
      oldAdmission: Option[MySqlBusinessAdmission]
  ): (MySqlBusinessEffectPlan, MySqlBusinessAdmission) =
    if plan == null || oldAdmission == null then
      MySqlSchemaAuditor.reject("business unknown reconciliation requires captured plan and old admission options")
    val exactPlan = plan.getOrElse(
      MySqlSchemaAuditor.reject("business unknown has no captured typed plan and must remain permanently frozen")
    )
    val exactAdmission = oldAdmission.getOrElse(
      MySqlSchemaAuditor.reject("business unknown has no captured old writer admission and cannot be reconciled")
    )
    if exactPlan == null || exactAdmission == null then
      MySqlSchemaAuditor.reject("captured business plan and old admission must both be non-null")
    (exactPlan, exactAdmission)

  def requireCompleteEffects(
      plan: MySqlBusinessEffectPlan,
      readback: MySqlBusinessUnknownEffectReadback
  ): Unit =
    if plan == null || readback == null then MySqlSchemaAuditor.reject("typed plan and complete effect readback are required")
    val fullSnapshots = plan.mutations.collect { case value: MySqlBusinessMutation.ReplaceFullRepositorySnapshot => value }
    if fullSnapshots.nonEmpty then
      if fullSnapshots.size != 1 || plan.mutations.size != 1 || readback.podRows.nonEmpty ||
        readback.entitlementRows.nonEmpty || readback.deletedPodReferences.nonEmpty then
        MySqlSchemaAuditor.reject("a full repository snapshot must be the single complete effect for its operation")
      val desired = fullSnapshots.head.desiredRepositorySnapshot.getOrElse(
        MySqlSchemaAuditor.reject("full repository mutation lost its immutable desired snapshot")
      )
      val actual = readback.fullRepositorySnapshot.getOrElse(
        MySqlSchemaAuditor.reject("full repository business readback is missing")
      )
      if !actual.exactlyEquals(desired) then
        MySqlFencedTransactionSupport.rejectConflict("complete MySQL domain, entitlement, recovery, and bootstrap snapshot differs from the frozen effect")
      return
    if readback.fullRepositorySnapshot.nonEmpty then
      MySqlSchemaAuditor.reject("a row-scoped effect cannot be reconciled by a full snapshot readback")
    val podMutations = plan.mutations.collect {
      case value: MySqlBusinessMutation.InsertInactivePod => value
      case value: MySqlBusinessMutation.ReplaceInactivePod => value
      case value: MySqlBusinessMutation.DeleteInactivePod => value
    }
    val entitlementMutations = plan.mutations.collect { case value: MySqlBusinessMutation.ChangeEntitlement => value }
    if podMutations.size + entitlementMutations.size != plan.mutations.size then
      MySqlSchemaAuditor.reject("frozen operation plan contains an effect outside this inactive pod and entitlement slice")
    val expectedPodIds = podMutations.map(MySqlBusinessMutation.podUuid).toSet
    val expectedEntitlementIds = entitlementMutations.map { mutation =>
      mutation.expectedEntitlement.orElse(mutation.desiredEntitlement).get.playerUuid
    }.toSet
    val expectedDeleteIds = podMutations.collect { case value: MySqlBusinessMutation.DeleteInactivePod => MySqlBusinessMutation.podUuid(value) }.toSet
    if readback.podRows.keySet != expectedPodIds ||
      readback.entitlementRows.keySet != expectedEntitlementIds ||
      readback.deletedPodReferences.keySet != expectedDeleteIds then
      MySqlSchemaAuditor.reject("full business readback target set differs from the frozen operation plan")

    podMutations.foreach { mutation =>
      val uuid = MySqlBusinessMutation.podUuid(mutation)
      if !MySqlBusinessMutation.samePodOption(readback.podRows(uuid), mutation.desiredPod) then
        MySqlFencedTransactionSupport.rejectConflict("complete MySQL pod state does not match the frozen desired row")
    }
    entitlementMutations.foreach { mutation =>
      val uuid = mutation.expectedEntitlement.orElse(mutation.desiredEntitlement).get.playerUuid
      if readback.entitlementRows(uuid) != mutation.desiredEntitlement then
        MySqlFencedTransactionSupport.rejectConflict("complete MySQL entitlement state does not match the frozen desired row")
    }
    expectedDeleteIds.foreach { uuid =>
      val references = readback.deletedPodReferences(uuid)
      if references.locatorPodUuids.nonEmpty || references.chunkPodUuids.nonEmpty || references.intentPodUuids.nonEmpty then
        MySqlFencedTransactionSupport.rejectConflict("deleted inactive pod still has a physical locator, chunk, or recovery intent")
    }

  def requireExactWriterReceipt(
      context: MySqlOperationContext,
      expectedBindingUuid: UUID,
      expectedBootUuid: UUID,
      expectedWriterEpoch: Long,
      expectedLeaseUntil: LocalDateTime,
      row: MySqlBusinessWriterRow
  ): Unit =
    if context == null || expectedBindingUuid == null || expectedBootUuid == null || expectedLeaseUntil == null || row == null then
      MySqlSchemaAuditor.reject("exact writer reconciliation inputs are required")
    if row.bindingUuid != expectedBindingUuid || row.bindingUuid != context.bindingUuid ||
      row.writerEpoch != expectedWriterEpoch || row.writerEpoch != context.writerEpoch ||
      row.ownerBootUuid != Some(expectedBootUuid) || expectedBootUuid != context.bootUuid ||
      row.leaseUntil != expectedLeaseUntil || !row.leaseUntil.isAfter(row.databaseNow) then
      MySqlSchemaAuditor.reject("locked writer row is not the original boot, epoch, exact lease, and live database scope")
    MySqlCommitReceipts.verifyReadback(context, row.receipt) match
      case MySqlCommitReceiptVerification.Confirmed(_) => ()
      case _ => MySqlSchemaAuditor.reject("exact operation receipt is not present in the locked writer slot")

  def validate(
      snapshot: MySqlBusinessUnknownSnapshot,
      proof: MySqlBusinessUnknownProof,
      expectation: MySqlSchemaAuditExpectation
  ): Unit =
    if snapshot == null || proof == null || expectation == null then
      MySqlSchemaAuditor.reject("business unknown snapshot, sealed SQL proof, and schema expectation are required")
    if (proof.snapshot ne snapshot) || (snapshot.verified ne proof.snapshot.verified) ||
      (snapshot.scope ne proof.snapshot.scope) then
      MySqlSchemaAuditor.reject("SQL proof belongs to a different frozen snapshot or reconnect scope")
    val oldToken = snapshot.oldAdmission.token
    if snapshot.plan == null || snapshot.context == null || snapshot.context.expectedEffects != snapshot.plan.expectedEffects ||
      snapshot.context.bootUuid != oldToken.bootUuid || snapshot.context.writerEpoch != oldToken.writerEpoch ||
      snapshot.context.bindingUuid != oldToken.bindingUuid then
      MySqlSchemaAuditor.reject("frozen business operation context does not exactly match its immutable plan")

    val qualification = snapshot.qualification
    if qualification.profile != expectation.profile ||
      qualification.expectedPhysicalFormat != expectation.expectedPhysicalFormat ||
      qualification.serverUuid != expectation.serverUuid || qualification.schema != expectation.schema ||
      qualification.bindingUuid != expectation.bindingUuid ||
      qualification.physicalBinding != MySqlCertifiedPhysicalBinding(expectation.bindingUuid, expectation.expectedPhysicalFormat.version) ||
      qualification.provisioningUuid != expectation.provisioningUuid ||
      qualification.ddlSha256 != expectation.ddlSha256 ||
      qualification.objectManifestSha256 != expectation.objects.canonicalSha256 ||
      qualification.writerEpochObservedBeforeAcquire != oldToken.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(oldToken.bootUuid) then
      MySqlSchemaAuditor.reject("fresh reconnect qualification differs from the original approved writer scope")

    val expectedGuard = HexFormat.of().parseHex(expectation.objects.canonicalSha256).toVector
    val expectedDdl = HexFormat.of().parseHex(expectation.ddlSha256).toVector
    val manifest = proof.manifest
    if manifest.singleton != 1 || manifest.backend != "MYSQL" ||
      manifest.formatVersion != expectation.profile.schemaFormat || manifest.bootstrapState != "READY" ||
      manifest.ddlSha256 != expectedDdl || manifest.provisioningUuid != expectation.provisioningUuid ||
      manifest.serverUuid != expectation.serverUuid || manifest.bindingUuid != expectation.bindingUuid ||
      manifest.guardManifestSha256 != Some(expectedGuard) ||
      !MySqlFencedTransactionSupport.importMatches(manifest, expectation.expectedImport) then
      MySqlSchemaAuditor.reject("locked schema manifest is not the exact approved READY import and guard tuple")

    val binding = proof.binding
    if binding.singleton != 1 || binding.bindingUuid != expectation.bindingUuid ||
      binding.cutoverState != "CERTIFIED" || binding.formatVersion != expectation.expectedPhysicalFormat.version then
      MySqlSchemaAuditor.reject("locked physical binding is not the exact certified fixture")

    requireExactWriterReceipt(
      snapshot.context,
      expectation.bindingUuid,
      oldToken.bootUuid,
      oldToken.writerEpoch,
      oldToken.leaseUntil,
      proof.writerRow
    )

    requireCompleteEffects(snapshot.plan, proof.effectReadback)

/** 对当前有限 typed slice 做单连接完整效果核验；不重放、不修改业务行、不递增 epoch 或 receipt。 */
final class MySqlBusinessUnknownReconciler(
    expectation: MySqlSchemaAuditExpectation,
    auditor: MySqlSchemaAuditor,
    coordinator: MySqlWriterFenceCoordinator
):
  require(expectation != null && auditor != null && coordinator != null, "business unknown reconciler dependencies are required")
  require(MySqlSchemaAuditor.SchemaIdentifier.matches(expectation.schema), "fixed MySQL schema identifier is unsafe")

  private final class SqlVerifiedBusinessUnknownProof private (
      private[mysql] val snapshot: MySqlBusinessUnknownSnapshot,
      private[mysql] val manifest: MySqlLockedManifest,
      private[mysql] val binding: MySqlLockedBinding,
      private[mysql] val writerRow: MySqlBusinessWriterRow,
      private[mysql] val effectReadback: MySqlBusinessUnknownEffectReadback,
      private[mysql] val monotonicAnchorNanos: Long
  ) extends MySqlBusinessUnknownProof

  private object SqlVerifiedBusinessUnknownProof:
    def afterSuccessfulReadTransaction(
        snapshot: MySqlBusinessUnknownSnapshot,
        manifest: MySqlLockedManifest,
        binding: MySqlLockedBinding,
        writerRow: MySqlBusinessWriterRow,
        effectReadback: MySqlBusinessUnknownEffectReadback,
        monotonicAnchorNanos: Long
    ): MySqlBusinessUnknownProof =
      new SqlVerifiedBusinessUnknownProof(snapshot, manifest, binding, writerRow, effectReadback, monotonicAnchorNanos)

  def reconcile(
      verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope
  ): Try[MySqlBusinessAdmission] =
    var snapshot: MySqlBusinessUnknownSnapshot = null
    try
      snapshot = coordinator.beginBusinessUnknownReconciliation(verified, scope)
      val proof = readAndVerify(snapshot)
      val token = coordinator.issueBusinessUnknownRecoveryToken(snapshot, proof)
      val observation = auditor.readCurrentObjectManifest(verified, scope)
      val receipt = auditor.auditAfterWriterAcquire(observation, snapshot.qualification, scope, token.fence)
      Success(coordinator.confirmBusinessUnknownReconciliation(snapshot, proof, token, receipt))
    catch
      case primary: Throwable =>
        try coordinator.recordBusinessUnknownReconciliationFailure(Option(snapshot), primary)
        catch case recordingFailure: Throwable => MySqlFencedTransactionSupport.addSuppressed(primary, recordingFailure)
        Failure(primary)

  private def readAndVerify(snapshot: MySqlBusinessUnknownSnapshot): MySqlBusinessUnknownProof =
    val connection = snapshot.verified.connection
    if !connection.getAutoCommit then
      MySqlSchemaAuditor.reject("business unknown reconciliation requires an idle auto-commit reconnect")
    val quotedSchema = MySqlFencedTransactionSupport.quote(expectation.schema)
    var beginAttempted = false
    var cleanupComplete = false
    try
      beginAttempted = true
      connection.setAutoCommit(false)
      snapshot.verified.readmitSessionBarrier(expectation, snapshot.scope)
      val manifest = lockManifest(connection, quotedSchema)
      val binding = lockBinding(connection, quotedSchema)
      val writer = lockWriter(connection, quotedSchema)
      val effects = readCompleteEffects(connection, quotedSchema, snapshot.plan)
      val writerAnchor = System.nanoTime()
      val databaseNow = readDatabaseUtc(connection)
      val writerAtProof = writer.copy(databaseNow = databaseNow)
      val proofParts = (manifest, binding, writerAtProof, effects, writerAnchor)

      connection.rollback()
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then
        MySqlSchemaAuditor.reject("business unknown read transaction did not restore auto-commit")
      cleanupComplete = true

      val proof = SqlVerifiedBusinessUnknownProof.afterSuccessfulReadTransaction(
        snapshot,
        proofParts._1,
        proofParts._2,
        proofParts._3,
        proofParts._4,
        proofParts._5
      )
      MySqlBusinessUnknownProofRules.validate(snapshot, proof, expectation)
      proof
    catch
      case primary: Throwable =>
        if beginAttempted && !cleanupComplete then rollbackAndRestore(connection, primary)
        throw primary

  private def lockManifest(connection: Connection, quotedSchema: String): MySqlLockedManifest =
    val sql =
      "SELECT singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state," +
        "expected_server_uuid,expected_binding_uuid,import_manifest_sha256,import_state,import_operation_uuid," +
        "import_snapshot_sha256,import_receipt_sha256,guard_manifest_sha256 FROM " +
        table(quotedSchema, "fp_schema_manifest") + " WHERE singleton_fixed1=1 FOR SHARE"
    withStatement(connection, sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "business reconciliation manifest") { row =>
          MySqlLockedManifest(
            singleton = MySqlFencedTransactionSupport.requiredInt(row, 1, "manifest singleton"),
            backend = MySqlFencedTransactionSupport.requiredText(row, 2, "manifest backend"),
            formatVersion = MySqlFencedTransactionSupport.requiredInt(row, 3, "manifest format version"),
            ddlSha256 = MySqlFencedTransactionSupport.requiredBytes(row, 4, "manifest DDL SHA-256", 32).toVector,
            provisioningUuid = MySqlFencedTransactionSupport.requiredUuid(row, 5, "manifest provisioning UUID"),
            bootstrapState = MySqlFencedTransactionSupport.requiredText(row, 6, "manifest bootstrap state"),
            serverUuid = MySqlFencedTransactionSupport.requiredUuid(row, 7, "manifest server UUID"),
            bindingUuid = MySqlFencedTransactionSupport.requiredUuid(row, 8, "manifest binding UUID"),
            importManifestSha256 = MySqlFencedTransactionSupport.optionalHash(row, 9, "manifest import hash"),
            importState = MySqlFencedTransactionSupport.requiredText(row, 10, "manifest import state"),
            importOperationUuid = MySqlFencedTransactionSupport.optionalUuid(row, 11, "manifest import operation UUID"),
            importSnapshotSha256 = MySqlFencedTransactionSupport.optionalHash(row, 12, "manifest import snapshot hash"),
            importReceiptSha256 = MySqlFencedTransactionSupport.optionalHash(row, 13, "manifest import receipt hash"),
            guardManifestSha256 = MySqlFencedTransactionSupport.optionalHash(row, 14, "manifest guard hash")
          )
        }
      }
    }

  private def lockBinding(connection: Connection, quotedSchema: String): MySqlLockedBinding =
    val sql = "SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM " +
      table(quotedSchema, "physical_recovery_binding") + " WHERE singleton_fixed1=1 FOR UPDATE"
    withStatement(connection, sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "business reconciliation physical binding") { row =>
          MySqlLockedBinding(
            MySqlFencedTransactionSupport.requiredInt(row, 1, "binding singleton"),
            MySqlFencedTransactionSupport.requiredUuid(row, 2, "physical binding UUID"),
            MySqlFencedTransactionSupport.requiredText(row, 3, "physical binding state"),
            MySqlFencedTransactionSupport.requiredInt(row, 4, "physical binding format")
          )
        }
      }
    }

  private def lockWriter(connection: Connection, quotedSchema: String): MySqlBusinessWriterRow =
    val sql =
      "SELECT singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until,last_commit_operation_uuid," +
        "last_commit_fingerprint,UTC_TIMESTAMP(6) FROM " + table(quotedSchema, "fp_runtime_writer") +
        " WHERE singleton_fixed1=1 FOR UPDATE"
    withStatement(connection, sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "business reconciliation runtime writer") { row =>
          if MySqlFencedTransactionSupport.requiredInt(row, 1, "writer singleton") != 1 then
            MySqlSchemaAuditor.reject("runtime writer singleton key is not one")
          MySqlBusinessWriterRow(
            bindingUuid = MySqlFencedTransactionSupport.requiredUuid(row, 2, "writer binding UUID"),
            writerEpoch = MySqlFencedTransactionSupport.requiredLong(row, 3, "writer epoch"),
            ownerBootUuid = MySqlFencedTransactionSupport.optionalUuid(row, 4, "writer owner boot UUID"),
            leaseUntil = MySqlFencedTransactionSupport.requiredLocalDateTime(row, 5, "writer lease"),
            lastOperationUuid = MySqlFencedTransactionSupport.optionalUuid(row, 6, "writer operation UUID"),
            lastFingerprint = MySqlFencedTransactionSupport.optionalHash(row, 7, "writer effect fingerprint"),
            databaseNow = MySqlFencedTransactionSupport.requiredLocalDateTime(row, 8, "database UTC time")
          )
        }
      }
    }

  private def readDatabaseUtc(connection: Connection): LocalDateTime =
    withStatement(connection, "SELECT UTC_TIMESTAMP(6)") { statement =>
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then MySqlSchemaAuditor.reject("database UTC clock query returned no row")
        val now = MySqlFencedTransactionSupport.requiredLocalDateTime(result, 1, "database UTC time")
        if result.next() then MySqlSchemaAuditor.reject("database UTC clock query returned multiple rows")
        now
      }
    }

  private def readCompleteEffects(
      connection: Connection,
      quotedSchema: String,
      plan: MySqlBusinessEffectPlan
  ): MySqlBusinessUnknownEffectReadback =
    val fullSnapshots = plan.mutations.collect { case value: MySqlBusinessMutation.ReplaceFullRepositorySnapshot => value }
    if fullSnapshots.nonEmpty then
      if fullSnapshots.size != 1 || plan.mutations.size != 1 then
        MySqlSchemaAuditor.reject("a full repository snapshot must be the single complete effect for its operation")
      val snapshot = MySqlFullSnapshotJdbc.read(connection, expectation.schema, lockRows = true)
      return MySqlBusinessUnknownEffectReadback(Map.empty, Map.empty, Map.empty, Some(snapshot))
    val podMutations = plan.mutations.filter(mutation => mutation.expectedPod.nonEmpty || mutation.desiredPod.nonEmpty)
    val entitlementMutations = plan.mutations.filter(mutation => mutation.expectedEntitlement != None || mutation.desiredEntitlement != None)
    val podUuids = podMutations.map(MySqlBusinessMutation.podUuid).distinct.sortBy(_.toString)
    val entitlementUuids = entitlementMutations.map { mutation =>
      mutation.expectedEntitlement.orElse(mutation.desiredEntitlement).get.playerUuid
    }.distinct.sortBy(_.toString)
    val deleteUuids = podMutations.collect { case mutation: MySqlBusinessMutation.DeleteInactivePod => MySqlBusinessMutation.podUuid(mutation) }.distinct.sortBy(_.toString)
    val pods = podUuids.map(uuid => uuid -> readPod(connection, quotedSchema, uuid)).toMap
    val entitlements = entitlementUuids.map(uuid => uuid -> readEntitlement(connection, quotedSchema, uuid)).toMap
    val references = deleteUuids.map(uuid => uuid -> readDeleteReferences(connection, quotedSchema, uuid)).toMap
    MySqlBusinessUnknownEffectReadback(pods, entitlements, references)

  private def readPod(connection: Connection, quotedSchema: String, uuid: UUID): Option[MySqlStoredRowData] =
    val sql = "SELECT " + MySqlFencedTransactionSupport.PodColumns + " FROM " +
      table(quotedSchema, "floating_pod_state") + " WHERE uuid=? FOR UPDATE"
    withStatement(connection, sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then None
        else
          val row = MySqlBusinessRows.readPod(result)
          if result.next() then MySqlSchemaAuditor.reject("pod primary key returned multiple rows during business reconciliation")
          Some(row)
      }
    }

  private def readEntitlement(connection: Connection, quotedSchema: String, uuid: UUID): Option[PlayerEntitlement] =
    val sql = "SELECT player_uuid,registration_limit,simultaneous_active_limit FROM " +
      table(quotedSchema, "player_entitlement") + " WHERE player_uuid=? FOR SHARE"
    withStatement(connection, sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then None
        else
          val row = MySqlBusinessRows.readEntitlement(result)
          if result.next() then MySqlSchemaAuditor.reject("player entitlement primary key returned multiple rows during business reconciliation")
          Some(row)
      }
    }

  private def readDeleteReferences(
      connection: Connection,
      quotedSchema: String,
      uuid: UUID
  ): MySqlBusinessUnknownDeleteReferences =
    MySqlBusinessUnknownDeleteReferences(
      readUuidReferences(connection, quotedSchema, "physical_recovery_locator", uuid),
      readUuidReferences(connection, quotedSchema, "physical_recovery_locator_chunk", uuid),
      readUuidReferences(connection, quotedSchema, "physical_recovery_intent", uuid)
    )

  private def readUuidReferences(connection: Connection, quotedSchema: String, tableName: String, uuid: UUID): Vector[UUID] =
    val sql = "SELECT pod_uuid FROM " + table(quotedSchema, tableName) + " WHERE pod_uuid=? FOR UPDATE"
    withStatement(connection, sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(uuid).toArray)
      withResultSet(statement.executeQuery()) { result =>
        val values = Vector.newBuilder[UUID]
        while result.next() do values += MySqlFencedTransactionSupport.requiredUuid(result, 1, tableName + " pod UUID")
        values.result()
      }
    }

  private def table(quotedSchema: String, name: String): String =
    if !name.matches("[a-z][a-z0-9_]{0,63}") then MySqlSchemaAuditor.reject("internal MySQL table name is unsafe")
    quotedSchema + "." + MySqlFencedTransactionSupport.quote(name)

  private def withStatement[A](connection: Connection, sql: String)(body: PreparedStatement => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql))(body)

  private def withResultSet[A](result: ResultSet)(body: ResultSet => A): A =
    MySqlBusinessRows.using(result)(body)

  private def rollbackAndRestore(connection: Connection, primary: Throwable): Unit =
    var rollbackSucceeded = false
    try
      connection.rollback()
      rollbackSucceeded = true
    catch
      case rollbackFailure: Throwable => MySqlFencedTransactionSupport.addSuppressed(primary, rollbackFailure)
    if rollbackSucceeded then
      try
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then
          MySqlSchemaAuditor.reject("business unknown rollback did not restore auto-commit")
      catch case restoreFailure: Throwable => MySqlFencedTransactionSupport.addSuppressed(primary, restoreFailure)
