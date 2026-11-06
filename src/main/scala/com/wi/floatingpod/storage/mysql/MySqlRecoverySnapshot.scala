package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.sql.{Connection, ResultSet, SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}

final case class MySqlSnapshotManifest(singleton: Int, backend: String, formatVersion: Int,
    ddlSha256: Vector[Byte], provisioningUuid: UUID, bootstrapState: String, serverUuid: UUID,
    bindingUuid: UUID, importManifestSha256: Option[Vector[Byte]], importState: String,
    importOperationUuid: Option[UUID], importSnapshotSha256: Option[Vector[Byte]],
    importReceiptSha256: Option[Vector[Byte]], guardManifestSha256: Option[Vector[Byte]])

final case class MySqlSnapshotWriter(singleton: Int, bindingUuid: UUID, epoch: Long,
    ownerBootUuid: Option[UUID], leaseUntil: LocalDateTime, lastOperationUuid: Option[UUID],
    lastFingerprint: Option[Vector[Byte]])

enum MySqlRecoveryObservationState:
  case Bootstrapping, BindingAbsent, LegacyBinding, CertifiedSchemaObserved

enum MySqlObservedWriterLease:
  case Unowned, OwnedUnexpired, OwnedExpired

/** 此失败必须由 IO owner 退休连接；不能继续复用不确定的 session。 */
final class MySqlSnapshotCleanupUncertainException(cause: Throwable)
    extends SQLNonTransientConnectionException("MySQL recovery snapshot cleanup left caller-owned connection uncertain; retire it", cause)

/** 同一 RR transaction 的九表原始观察。任何 state 都不代表 Business/Fresh/custody/Ready。 */
final case class MySqlRecoverySnapshot(
    bootUuid: UUID, bootGeneration: Long, connectionGeneration: Long,
    manifest: MySqlSnapshotManifest, writer: MySqlSnapshotWriter,
    pods: Vector[MySqlStoredRowData], entitlements: Vector[PlayerEntitlement],
    recovery: MySqlRecoveryGraph, bootstrapIntents: Vector[MySqlBootstrapIntentData],
    databaseTimeBefore: LocalDateTime, databaseTimeAfter: LocalDateTime,
    staticObjectSha256: String, state: MySqlRecoveryObservationState, writerLease: MySqlObservedWriterLease)

/** 不取得 writer admission；使用完整已批准 static baseline，允许 BOOT/NoneB/Legacy 的观察。
  * explicit RR transaction + M→optional B→W locks；原 READ_COMMITTED 会在锁释放后恢复。
  */
final class MySqlRecoverySnapshotReader(expectation: MySqlSchemaAuditExpectation,
    auditor: MySqlSchemaAuditor, verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope):
  require(expectation != null && auditor != null && verified != null && scope != null, "snapshot dependencies required")
  require(expectation.profile == MySqlSchemaAuditProfile.Format2, "nine-table snapshot requires format 2")
  require(scope.ownerThread eq Thread.currentThread(), "snapshot must be constructed on owned IO thread")
  private val connection = verified.connection

  def read(): MySqlRecoverySnapshot =
    if Thread.currentThread() ne scope.ownerThread then throw new IllegalStateException("snapshot requires owned IO thread")
    if !scope.verifiedConnection.contains(verified) || (scope.connectionToken ne connection) ||
        (scope.connectionIdentity ne verified.identity) then invalid("exact verified connection scope")
    if !connection.getAutoCommit then invalid("idle auto-commit connection required")
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly).foreach(failure => throw failure)
    try readOwned(rollbackOnly)
    finally MySqlConnectionTransactionGate.leave(connection, rollbackOnly)

  private def readOwned(rollbackOnly: MySqlRollbackOnlyState): MySqlRecoverySnapshot =
    val oldIsolation = connection.getTransactionIsolation
    var beginAttempted = false
    var primary: Throwable = null
    var cleanupFailed = false
    var result: MySqlRecoverySnapshot = null
    try
      val beforeAudit = auditor.readCurrentObjectManifest(verified, scope)
      auditor.validateObservationEnvelope(beforeAudit)
      val beforeStatic = auditor.compareRequiredObjectManifest(beforeAudit)
      MySqlRecoverySnapshotRules.requireReaderStaticBaseline(beforeStatic.actualCanonicalSha256, expectation.objects.canonicalSha256)
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then invalid("RR isolation was not applied")
      beginAttempted = true
      connection.setAutoCommit(false)
      verified.readmitSessionBarrier(expectation, scope)
      if connection.getAutoCommit then invalid("explicit transaction was not started")
      val manifest = required(query("fp_schema_manifest", MySqlRecoverySnapshotRules.ManifestColumns, true)(MySqlRecoverySnapshotRules.decodeManifest), "manifest")
      val bindings = query("physical_recovery_binding", "singleton_fixed1,binding_uuid,cutover_state,format_version", true)(MySqlRecoveryRows.readBinding)
      val binding = MySqlBusinessRows.single(bindings)
      val (writer, timeBefore) = required(query("fp_runtime_writer", MySqlRecoverySnapshotRules.WriterColumns, true)(MySqlRecoverySnapshotRules.decodeWriter), "writer")
      MySqlRecoverySnapshotRules.validateControls(manifest, binding, writer, expectation)
      MySqlRecoverySnapshotRules.validateAuditManifest(beforeAudit, manifest)
      val business = new MySqlBusinessRows(connection, expectation.schema)
      val recoveryRows = new MySqlRecoveryRows(connection, expectation.schema)
      val pods = business.readAllPods()
      val entitlements = business.readAllEntitlements()
      val recovery = MySqlRecoveryRows.assembleGraph(binding, recoveryRows.readAllLocatorRows(), recoveryRows.readAllChunkRows(), recoveryRows.readAllIntents())
      val intents = new MySqlBootstrapIntentRows(connection, expectation.schema).readAll()
      MySqlRecoverySnapshotRules.validateInventory(pods, entitlements, recovery, intents)
      val manifestAfter = required(query("fp_schema_manifest", MySqlRecoverySnapshotRules.ManifestColumns, true)(MySqlRecoverySnapshotRules.decodeManifest), "manifest")
      val bindingAfter = MySqlBusinessRows.single(query("physical_recovery_binding", "singleton_fixed1,binding_uuid,cutover_state,format_version", true)(MySqlRecoveryRows.readBinding))
      val (writerAfter, timeAfter) = required(query("fp_runtime_writer", MySqlRecoverySnapshotRules.WriterColumns, true)(MySqlRecoverySnapshotRules.decodeWriter), "writer")
      MySqlRecoverySnapshotRules.validateStableControls(manifest, manifestAfter, binding, bindingAfter, writer, writerAfter, timeBefore, timeAfter)
      val afterAudit = auditor.readCurrentObjectManifest(verified, scope)
      auditor.validateObservationEnvelope(afterAudit)
      val afterStatic = auditor.compareRequiredObjectManifest(afterAudit)
      MySqlRecoverySnapshotRules.requireReaderStaticBaseline(afterStatic.actualCanonicalSha256, expectation.objects.canonicalSha256)
      MySqlRecoverySnapshotRules.validateAuditManifest(afterAudit, manifest)
      val auditBeforeEnd = MySqlRecoverySnapshotRules.auditTime(beforeAudit.resultSets.last)
      val auditAfterStart = MySqlRecoverySnapshotRules.auditTime(afterAudit.resultSets.head)
      if timeBefore.isBefore(auditBeforeEnd) || auditAfterStart.isBefore(timeAfter) ||
          MySqlRecoverySnapshotRules.auditTime(beforeAudit.resultSets.head).isAfter(auditBeforeEnd) ||
          MySqlRecoverySnapshotRules.auditTime(afterAudit.resultSets.last).isBefore(auditAfterStart) then invalid("audit/database time order")
      if beforeStatic.actualCanonicalSha256 != afterStatic.actualCanonicalSha256 || beforeAudit.before != afterAudit.after then invalid("audit static/caller tuple changed")
      if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then invalid("transaction state changed across audit CALL")
      val elapsed = System.nanoTime() - beforeAudit.auditStartedAtNanos
      if elapsed < 0L || elapsed > MySqlSchemaAuditor.MaxReceiptAgeNanos then invalid("snapshot exceeded bounded observation window")
      rollbackOnly.cause.foreach(failure => throw failure)
      result = MySqlRecoverySnapshot(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration, manifest, writer,
        pods, entitlements, recovery, intents, timeBefore, timeAfter, afterStatic.actualCanonicalSha256,
        MySqlRecoverySnapshotRules.observationState(manifest, binding), MySqlRecoverySnapshotRules.writerLease(writer, timeAfter))
    catch
      case failure: Throwable => primary = failure
    finally
      // 不提交任何数据；cleanup失败不能返回已构建的局部/完整候选。
      def cleanup(action: => Unit): Unit =
        try action
        catch case failure: Throwable =>
          cleanupFailed = true
          if primary == null then primary = failure else MySqlSchemaAuditor.addSuppressed(primary, failure)
      var rollbackAcknowledged = !beginAttempted
      if beginAttempted then cleanup { connection.rollback(); rollbackAcknowledged = true }
      // rollback失败时禁止 setAutoCommit(true) 隐式提交未知事务。
      if rollbackAcknowledged then
        cleanup(connection.setAutoCommit(true))
        cleanup(connection.setTransactionIsolation(oldIsolation))
        cleanup {
          if !connection.getAutoCommit || connection.getTransactionIsolation != oldIsolation then invalid("connection settings restoration failed")
        }
    if cleanupFailed then throw new MySqlSnapshotCleanupUncertainException(primary)
    if primary != null then throw primary
    result

  private def query[A](table: String, columns: String, lock: Boolean)(decode: ResultSet => A): Vector[A] =
    val suffix = if lock then " FOR UPDATE" else ""
    MySqlBusinessRows.using(connection.prepareStatement(s"SELECT $columns FROM `${expectation.schema}`.`$table` ORDER BY singleton_fixed1$suffix")) { statement =>
      MySqlBusinessRows.using(statement.executeQuery()) { rows =>
        val result = Vector.newBuilder[A]
        while rows.next() do result += decode(rows)
        result.result()
      }
    }
  private def required[A](rows: Vector[A], label: String): A = MySqlBusinessRows.single(rows).getOrElse(invalid(s"missing $label singleton"))
  private def invalid(label: String): Nothing = MySqlRecoverySnapshotRules.invalid(label)

private[mysql] object MySqlRecoverySnapshotRules:
  private val support = MySqlFencedTransactionSupport
  val ManifestColumns = "singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state,expected_server_uuid,expected_binding_uuid,import_manifest_sha256,import_state,import_operation_uuid,import_snapshot_sha256,import_receipt_sha256,guard_manifest_sha256"
  val WriterColumns = "singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until,last_commit_operation_uuid,last_commit_fingerprint,UTC_TIMESTAMP(6)"

  /** auditor 的独立 expectation 不能替代 reader 的完整批准基线；BOOT/NULL guard 同样适用。 */
  def requireReaderStaticBaseline(actualCanonicalSha256: String, readerCanonicalSha256: String): Unit =
    if actualCanonicalSha256 != readerCanonicalSha256 then invalid("auditor static baseline differs from reader expectation")

  def decodeManifest(row: ResultSet): MySqlSnapshotManifest =
    MySqlSnapshotManifest(support.requiredInt(row, 1, "manifest singleton"), support.requiredText(row, 2, "backend"),
      support.requiredInt(row, 3, "format version"), support.requiredBytes(row, 4, "DDL digest", 32).toVector,
      support.requiredUuid(row, 5, "provisioning UUID"), support.requiredText(row, 6, "bootstrap state"),
      support.requiredUuid(row, 7, "server UUID"), support.requiredUuid(row, 8, "binding UUID"),
      support.optionalHash(row, 9, "import manifest digest"), support.requiredText(row, 10, "import state"),
      support.optionalUuid(row, 11, "import operation"), support.optionalHash(row, 12, "import snapshot digest"),
      support.optionalHash(row, 13, "import receipt digest"), support.optionalHash(row, 14, "guard digest"))

  def decodeWriter(row: ResultSet): (MySqlSnapshotWriter, LocalDateTime) =
    (MySqlSnapshotWriter(support.requiredInt(row, 1, "writer singleton"), support.requiredUuid(row, 2, "writer binding"),
      support.requiredLong(row, 3, "writer epoch"), support.optionalUuid(row, 4, "writer owner"),
      support.requiredLocalDateTime(row, 5, "writer lease"), support.optionalUuid(row, 6, "writer operation"),
      support.optionalHash(row, 7, "writer fingerprint")), support.requiredLocalDateTime(row, 8, "database time"))

  def auditTime(set: MySqlAuditResultSet): LocalDateTime = auditCell(set, "observed_db_time") match
    case MySqlAuditTimestamp(value) => value
    case _ => invalid("audit database timestamp")

  private def auditCell(set: MySqlAuditResultSet, label: String): MySqlAuditCell =
    if set.rows.size != 1 then invalid("audit singleton")
    val index = set.columns.indexWhere(_.label == label)
    if index < 0 then invalid("audit required column")
    set.rows.head.cells(index)

  def validateAuditManifest(observation: UnverifiedMySqlSchemaObservation, manifest: MySqlSnapshotManifest): Unit =
    val expected: Vector[(String, MySqlAuditCell)] = Vector(
      "singleton_fixed1" -> MySqlAuditText(manifest.singleton.toString), "backend" -> MySqlAuditText(manifest.backend),
      "format_version" -> MySqlAuditText(manifest.formatVersion.toString), "ddl_sha256" -> MySqlAuditBinary(manifest.ddlSha256.toArray),
      "provisioning_uuid" -> MySqlAuditBinary(MySqlStoredRows.encodeUuid(manifest.provisioningUuid).toArray),
      "bootstrap_state" -> MySqlAuditText(manifest.bootstrapState),
      "expected_server_uuid" -> MySqlAuditBinary(MySqlStoredRows.encodeUuid(manifest.serverUuid).toArray),
      "expected_binding_uuid" -> MySqlAuditBinary(MySqlStoredRows.encodeUuid(manifest.bindingUuid).toArray),
      "import_state" -> MySqlAuditText(manifest.importState),
      "import_operation_uuid" -> manifest.importOperationUuid.map(u => MySqlAuditBinary(MySqlStoredRows.encodeUuid(u).toArray)).getOrElse(MySqlAuditNull),
      "import_manifest_sha256" -> manifest.importManifestSha256.map(h => MySqlAuditBinary(h.toArray)).getOrElse(MySqlAuditNull),
      "import_snapshot_sha256" -> manifest.importSnapshotSha256.map(h => MySqlAuditBinary(h.toArray)).getOrElse(MySqlAuditNull),
      "import_receipt_sha256" -> manifest.importReceiptSha256.map(h => MySqlAuditBinary(h.toArray)).getOrElse(MySqlAuditNull),
      "guard_manifest_sha256" -> manifest.guardManifestSha256.map(h => MySqlAuditBinary(h.toArray)).getOrElse(MySqlAuditNull))
    Vector(observation.resultSets.head, observation.resultSets.last).foreach { set =>
      expected.foreach { (label, cell) => if auditCell(set, label) != cell then invalid("audit/locked manifest tuple mismatch") }
    }

  def validateControls(m: MySqlSnapshotManifest, b: Option[PhysicalRecoveryBinding], w: MySqlSnapshotWriter,
      expected: MySqlSchemaAuditExpectation): Unit =
    if m.singleton != 1 || m.backend != "MYSQL" || m.formatVersion != 2 ||
        m.ddlSha256 != HexFormat.of().parseHex(expected.ddlSha256).toVector || m.provisioningUuid != expected.provisioningUuid ||
        m.serverUuid != expected.serverUuid || m.bindingUuid != expected.bindingUuid then invalid("exact manifest/provisioning target")
    if !Set("BOOTSTRAPPING", "READY").contains(m.bootstrapState) then invalid("bootstrap state")
    val hashes = Vector(m.importManifestSha256, m.importSnapshotSha256, m.importReceiptSha256, m.guardManifestSha256)
    if hashes.exists(_.exists(_.size != 32)) then invalid("manifest hash lengths")
    val hasImport = m.importOperationUuid.nonEmpty && m.importManifestSha256.nonEmpty && m.importSnapshotSha256.nonEmpty
    m.importState match
      case "NONE" => if hasImport || m.importOperationUuid.nonEmpty || hashes.take(3).exists(_.nonEmpty) then invalid("NONE import tuple")
      case "PENDING" => if !hasImport || m.importReceiptSha256.nonEmpty then invalid("PENDING import tuple")
      case "COMMITTED" => if !hasImport || m.importReceiptSha256.isEmpty then invalid("COMMITTED import tuple")
      case _ => invalid("import state")
    if m.bootstrapState == "READY" then
      if m.guardManifestSha256 != Some(HexFormat.of().parseHex(expected.objects.canonicalSha256).toVector) || m.importState == "PENDING" then invalid("READY guard/import tuple")
      val locked = MySqlLockedManifest(m.singleton, m.backend, m.formatVersion, m.ddlSha256, m.provisioningUuid, m.bootstrapState,
        m.serverUuid, m.bindingUuid, m.importManifestSha256, m.importState, m.importOperationUuid,
        m.importSnapshotSha256, m.importReceiptSha256, m.guardManifestSha256)
      if !support.importMatches(locked, expected.expectedImport) then invalid("exact approved import tuple")
    b.foreach { binding =>
      if binding.bindingUuid != expected.bindingUuid || binding.formatVersion != expected.expectedPhysicalFormat.version then invalid("physical binding target")
    }
    if w.singleton != 1 || w.bindingUuid != expected.bindingUuid || w.epoch < 0L ||
        w.lastOperationUuid.isDefined != w.lastFingerprint.isDefined || w.lastFingerprint.exists(_.size != 32) then invalid("complete writer tuple")

  def validateStableControls(m: MySqlSnapshotManifest, afterM: MySqlSnapshotManifest,
      b: Option[PhysicalRecoveryBinding], afterB: Option[PhysicalRecoveryBinding],
      w: MySqlSnapshotWriter, afterW: MySqlSnapshotWriter, before: LocalDateTime, after: LocalDateTime): Unit =
    if m != afterM || b != afterB || w != afterW then invalid("control tuple changed during snapshot")
    if before == null || after == null || after.isBefore(before) then invalid("database time moved backwards")

  def observationState(m: MySqlSnapshotManifest, binding: Option[PhysicalRecoveryBinding]): MySqlRecoveryObservationState =
    if m.bootstrapState != "READY" then MySqlRecoveryObservationState.Bootstrapping
    else binding match
      case None => MySqlRecoveryObservationState.BindingAbsent
      case Some(b) if b.cutoverState == PhysicalRecoveryCutoverState.LegacyUncertified => MySqlRecoveryObservationState.LegacyBinding
      case _ => MySqlRecoveryObservationState.CertifiedSchemaObserved

  def writerLease(w: MySqlSnapshotWriter, now: LocalDateTime): MySqlObservedWriterLease =
    if w.ownerBootUuid.isEmpty then MySqlObservedWriterLease.Unowned
    else if w.leaseUntil.isAfter(now) then MySqlObservedWriterLease.OwnedUnexpired
    else MySqlObservedWriterLease.OwnedExpired

  def validateInventory(pods: Vector[MySqlStoredRowData], entitlements: Vector[PlayerEntitlement],
      recovery: MySqlRecoveryGraph, bootstrap: Vector[MySqlBootstrapIntentData]): Unit =
    if pods.map(_.uuidBytes).distinct.size != pods.size || entitlements.map(_.playerUuid).distinct.size != entitlements.size then invalid("duplicate business primary key")
    pods.foreach { p =>
      if p.uuidBytes.size != 16 || p.ownerUuidBytes.exists(_.size != 16) || p.stateVersion < 0 ||
          !Set(0, 1).contains(p.isActiveFlag) || !Set("NORMAL", "WORLD_IDLE").contains(p.operatingModeToken) then invalid("business row checks")
    }
    recovery.validateGraph()
    val domain = pods.map(p => MySqlStoredRows.decodeUuid(p.uuidBytes, "pod UUID")).toSet
    if recovery.intents.exists(i => !domain.contains(i.podUuid)) then invalid("intent domain FK")
    MySqlBootstrapIntentRows.validateGraph(bootstrap)

  def invalid(label: String): Nothing = throw new SQLDataException(s"MySQL recovery snapshot violates $label")
