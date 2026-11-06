package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, SQLDataException}
import scala.collection.mutable

/** FirstBinding6 的一次性 Full27 DATA reader；只借用 caller 已建立的 RR/gate，不接管事务生命周期。 */
private[mysql] final class MySqlDbFirstBootstrapSnapshotJdbc private (
    private val inputs: MySqlDbFirstSessionInputs,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private val ownerThread: Thread,
    private var inputBorrow: AutoCloseable,
    private var holderFee: Long,
    private var dataBudget: MySqlFormat3ReadBudget,
    private var partialRelations: mutable.LinkedHashMap[String, MySqlFormat3RelationSnapshot]
) extends AutoCloseable:
  private var attempted = false
  private var reading = false
  private var closed = false
  private var closing = false
  private var failure: Throwable = null
  private var cleanupFailure: Throwable = null
  private var snapshotValue: MySqlScriptSchemaSnapshot = null
  private var verifiedValue: VerifiedMySqlConnection = null
  private var scopeValue: MySqlAuditBootScope = null
  private var qualificationValue: VerifiedSchemaQualification = null
  private var readModeValue: MySqlDbFirstBootstrapSnapshotJdbc.ReadMode = null
  private var gateValue: MySqlConnectionTransactionGate.Owner = null
  private var rollbackValue: MySqlRollbackOnlyState = null
  private var lockedValue: MySqlDbFirstBootstrapSnapshotJdbc.LockedRead = null
  private var packetValue = 0L

  private def requireThread(): Unit =
    if Thread.currentThread() ne ownerThread then
      throw new IllegalStateException("schema6 bootstrap snapshot requires its original IO worker")

  private def requireHolderOpen(): Unit =
    requireThread()
    if closed || closing || cleanupFailure != null then
      throw new IllegalStateException("schema6 bootstrap snapshot reader detached or cleanup incomplete", cleanupFailure)
    inputs.requireForSession(root, identity)
    if inputs.ioIdentity ne identity then
      throw new IllegalStateException("schema6 bootstrap reader requires the original boot/connection identity")

  def firstFailure: Option[Throwable] = { requireThread(); Option(failure) }

  def cleanupObservation: MySqlDbFirstBootstrapSnapshotJdbc.CleanupObservation =
    requireThread()
    MySqlDbFirstBootstrapSnapshotJdbc.CleanupObservation(closed, dataBudget != null, snapshotValue != null,
      (if partialRelations == null then 0 else partialRelations.size), inputBorrow != null, holderFee,
      Option(failure), Option(cleanupFailure))

  /** Snapshot 与数据 child 由此 holder 持有；调用方须保持 reader 打开直到 graph 完成借用。 */
  def snapshot: MySqlScriptSchemaSnapshot =
    requireHolderOpen()
    if failure != null then throw new IllegalStateException("schema6 bootstrap Full27 DATA capture failed", failure)
    if snapshotValue == null then throw new IllegalStateException("schema6 bootstrap Full27 DATA has no complete capture")
    snapshotValue

  /** 仅借本reader已完成的Certified Full27 DATA做持久P比较；不发行普通读代或SQL资格。 */
  private[mysql] def requirePreparationComparisonSource(image:MySqlScriptSchemaSnapshot,
      originalRoot:MySqlFormat3ReadBudget):Unit =
    requireHolderOpen()
    require(image!=null && (originalRoot eq root) && attempted && !reading && failure==null &&
      snapshotValue!=null && (snapshotValue eq image) && (image.allocationBudget eq dataBudget) &&
      dataBudget.isCanonicalChildOf(root) && !dataBudget.isClosed &&
      readModeValue==MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.CertifiedReadback,
      "original completed Certified bootstrap reader/image/root required for preparation comparison")

  /** 在 caller 已经持有的事务内锁读完整 27 表；从不 begin、commit、rollback 或离开 gate。 */
  def readLocked(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      qualification: VerifiedSchemaQualification, gate: MySqlConnectionTransactionGate.Owner,
      rollbackOnly: MySqlRollbackOnlyState): MySqlScriptSchemaSnapshot =
    readLockedKernel(verified, scope, qualification, gate, rollbackOnly,
      MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap)

  /** CERTIFY/RELEASE readback；只承接同owner的current Certified2 DATA，不发行业务authority。 */
  def readCertifiedReadbackLocked(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      qualification: VerifiedSchemaQualification, gate: MySqlConnectionTransactionGate.Owner,
      rollbackOnly: MySqlRollbackOnlyState): MySqlScriptSchemaSnapshot =
    readLockedKernel(verified, scope, qualification, gate, rollbackOnly,
      MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.CertifiedReadback)

  private def readLockedKernel(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      qualification: VerifiedSchemaQualification, gate: MySqlConnectionTransactionGate.Owner,
      rollbackOnly: MySqlRollbackOnlyState,
      readMode: MySqlDbFirstBootstrapSnapshotJdbc.ReadMode): MySqlScriptSchemaSnapshot =
    requireHolderOpen()
    if attempted then throw new IllegalStateException("schema6 bootstrap snapshot read is one-shot, including failure", failure)
    attempted = true
    try
      if verified == null || scope == null || qualification == null || gate == null || rollbackOnly == null then
        throw new IllegalArgumentException("actual verified connection/scope/qualification/gate/rollback state required")
      // Validate the gate before retaining it: a foreign gate must never be marked rollback-only here.
      MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollbackOnly)
      verifiedValue = verified
      scopeValue = scope
      qualificationValue = qualification
      readModeValue = readMode
      gateValue = gate
      rollbackValue = rollbackOnly
      reading = true
      requireTransaction()
      packetValue = readSessionMaxAllowedPacket()
      dataBudget.bindCanonicalDataPacketLimit(packetValue)
      dataBudget.requireScriptDataInputs(inputs.full27Inputs)
      requireTransaction()
      val observation = MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
      val observationScope = MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
      // A canonical DATA child needs its own bounded header/row budget; this initializes no root image or SQL authority.
      dataBudget.initialize(inputs.full27Inputs.schema, observation, observationScope, packetValue)
      lockedValue = new MySqlDbFirstBootstrapSnapshotJdbc.LockedRead(this, inputs.full27Inputs,
        dataBudget, packetValue)

      val cells = new MySqlFormat3FullSnapshotReader(verified, scope, inputs.full27Inputs.schema,
        inputs.full27Inputs.sourceLimitProfile)
      MySqlDbFirstSnapshotSql.lockedReadOrder.foreach { shape =>
        requireTransaction()
        lockedValue.requireReader(verified, scope, dataBudget, packetValue)
        val relation = cells.readDbFirstBootstrapRelation(lockedValue, shape.table, dataBudget, packetValue)
        if partialRelations.contains(shape.table) then throw new SQLDataException("schema6 bootstrap repeated a Full27 relation")
        partialRelations.put(shape.table, relation)
        validateBootstrapRootRelation(relation, qualification, readMode)
        requireTransaction()
      }
      if partialRelations.size != MySqlDbFirstRelationShapes.ExpectedRelationCount then
        throw new SQLDataException("schema6 bootstrap requires all 27 Full27 relations")
      val ordered = MySqlDbFirstSnapshotSql.canonicalOrder.map(shape =>
        partialRelations.getOrElse(shape.table,
          throw new SQLDataException("schema6 bootstrap omitted a canonical Full27 relation")))
      snapshotValue = MySqlScriptSchemaSnapshot.decoded(inputs.full27Inputs.schema, observation,
        observationScope, packetValue, inputs.full27Inputs, ordered, dataBudget)
      MySqlDbFirstSnapshotSql.requireManifest(snapshotValue, inputs.auditExpectation)
      requireTransaction()
      snapshotValue
    catch
      case cause: Throwable =>
        if failure == null then failure = cause
        else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure, cause)
        if rollbackValue != null then
          try rollbackValue.mark(cause)
          catch case marking: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, marking)
        throw cause
    finally
      reading = false

  private def requireTransaction(): Unit =
    requireCurrentContext()
    val connection = verifiedValue.connection
    if connection.isClosed || connection.getAutoCommit ||
      connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLDataException("schema6 bootstrap requires the original active REPEATABLE READ transaction")

  /** No JDBC getter is used here: FullSnapshotReader calls this again while each ResultSet is streaming. */
  private def requireCurrentContext(): Unit =
    requireHolderOpen()
    val verified = verifiedValue
    val scope = scopeValue
    val qualification = qualificationValue
    val gate = gateValue
    val rollback = rollbackValue
    val expectation = inputs.auditExpectation
    val full27 = inputs.full27Inputs
    if verified == null || scope == null || qualification == null || gate == null || rollback == null ||
      (scope.ownerThread ne ownerThread) || (scope.connectionToken ne verified.connection) ||
      !scope.verifiedConnection.contains(verified) || (scope.connectionIdentity ne verified.identity) ||
      scope.bootUuid != identity.bootId || scope.bootGeneration != identity.bootGeneration ||
      scope.connectionGeneration != identity.connectionGeneration ||
      identity.connectionGeneration != scope.connectionGeneration ||
      verified.identity.serverUuid != expectation.serverUuid || verified.identity.catalog != expectation.schema ||
      verified.identity.serverVersion != expectation.serverVersion || verified.identity.effectiveUser != expectation.effectiveUser ||
      expectation.profile != MySqlSchemaAuditProfile.DbFirst1 ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || expectation.dbFirstInputs.isEmpty ||
      expectation.expectedImport != MySqlExpectedNoImport ||
      full27.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 ||
      (full27.sourceLimitProfile ne root.profile) || (dataBudget.profile ne root.profile) ||
      !qualification.isCurrent(identity.bootId, identity.bootGeneration, scope.connectionGeneration,
        expectation.serverUuid, expectation.schema, expectation.bindingUuid, MySqlSchemaAuditProfile.DbFirst1,
        MySqlExpectedPhysicalFormat.Format2, verified, ownerThread, System.nanoTime()) ||
      qualification.profile != MySqlSchemaAuditProfile.DbFirst1 ||
      qualification.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
      qualification.serverUuid != expectation.serverUuid || qualification.schema != expectation.schema ||
      qualification.bindingUuid != expectation.bindingUuid || qualification.provisioningUuid != expectation.provisioningUuid ||
      !qualification.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) ||
      !qualification.objectManifestSha256.equalsIgnoreCase(expectation.objects.canonicalSha256) ||
      qualification.importState != "NONE" || qualification.importOperationUuid.nonEmpty ||
      qualification.importManifestSha256.nonEmpty || qualification.importSnapshotSha256.nonEmpty ||
      qualification.importReceiptSha256.nonEmpty then
      throw new IllegalStateException("schema6 bootstrap reader differs from the current Full27 scope/qualification")
    val physicalModeMatches = (readModeValue, qualification.physicalBinding) match
      case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap, MySqlNoPhysicalBinding) => true
      case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap,
          MySqlLegacyPhysicalBinding(binding, 2)) if binding == expectation.bindingUuid => true
      case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.CertifiedReadback,
          MySqlCertifiedPhysicalBinding(binding, 2)) if binding == expectation.bindingUuid => true
      case _ => false
    if !physicalModeMatches then
      readModeValue match
        case MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap =>
          throw new IllegalStateException("FirstBinding6 bootstrap permits only no binding or exact Legacy Format2")
        case MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.CertifiedReadback =>
          throw new IllegalStateException("schema6 certified readback requires the exact Certified Format2 qualification")
        case _ => throw new IllegalStateException("schema6 reader mode is absent")
    MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollback)
    root.requireScriptDataInputs(full27)
    dataBudget.requireScriptDataInputs(full27)

  private def readSessionMaxAllowedPacket(): Long =
    requireTransaction()
    val sql = "SELECT @@SESSION.max_allowed_packet"
    if sql.length.toLong + 16L > math.min(root.profile.maxSqlPacketBytes(), inputs.full27Inputs.budget.packetBytes) then
      throw new SQLDataException("schema6 bootstrap packet-observation query exceeds the original packet profile")
    val workspace = 2048L
    root.reserveTemporaryMemory(workspace, "schema6 bootstrap packet statement/result handles BEFORE allocation")
    var primary: Throwable = null
    try
      MySqlBusinessRows.using(verifiedValue.connection.prepareStatement(sql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { result =>
          if !result.next() then throw new SQLDataException("schema6 bootstrap packet observation returned no row")
          val packet = result.getLong(1)
          val wasNull = result.wasNull()
          if wasNull || packet <= 0L || result.next() then
            throw new SQLDataException("schema6 bootstrap packet observation requires exactly one positive value")
          packet
        }
      }
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() =>
        root.releaseTemporaryMemory(workspace, "schema6 bootstrap packet statement/result handles released")))

  private def validateBootstrapRootRelation(relation: MySqlFormat3RelationSnapshot,
      qualification: VerifiedSchemaQualification, readMode: MySqlDbFirstBootstrapSnapshotJdbc.ReadMode): Unit =
    def index(name: String): Int =
      val value = inputs.full27Inputs.forTable(relation.table).columns.indexWhere(_.name == name)
      if value < 0 then throw new SQLDataException(s"${relation.table} is missing frozen root column $name")
      value
    def field(row: Vector[MySqlFormat3Cell], name: String): MySqlFormat3Cell = row(index(name))
    def integer(row: Vector[MySqlFormat3Cell], name: String): Long = field(row, name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException(s"${relation.table}.$name must be an integer")
    def text(row: Vector[MySqlFormat3Cell], name: String): String = field(row, name) match
      case MySqlFormat3Cell.Text(value) => value
      case _ => throw new SQLDataException(s"${relation.table}.$name must be text")
    def binding(row: Vector[MySqlFormat3Cell], name: String): Unit = field(row, name) match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(inputs.auditExpectation.bindingUuid) => ()
      case _ => throw new SQLDataException(s"${relation.table}.$name differs from the approved Full27 binding")
    relation.table match
      case "fp_schema_manifest" =>
        if relation.rows.size != 1 then throw new SQLDataException("FirstBinding6 requires exactly one schema manifest row")
        val row = relation.rows.head
        if integer(row, "singleton_fixed1") != 1L || text(row, "backend") != "MYSQL" ||
          integer(row, "format_version") != 6L || text(row, "bootstrap_state") != "READY" ||
          text(row, "import_state") != "NONE" then
          throw new SQLDataException("FirstBinding6 requires a single MYSQL READY6 no-import manifest")
      case "physical_recovery_binding" =>
        if relation.rows.size > 1 then throw new SQLDataException("FirstBinding6 permits at most one physical binding row")
        (readMode, qualification.physicalBinding) match
          case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap, MySqlNoPhysicalBinding)
              if relation.rows.isEmpty => ()
          case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.InitialBootstrap,
              MySqlLegacyPhysicalBinding(bindingUuid, 2))
              if bindingUuid == inputs.auditExpectation.bindingUuid && relation.rows.size == 1 =>
            val row = relation.rows.head
            if integer(row, "singleton_fixed1") != 1L then
              throw new SQLDataException("FirstBinding6 legacy physical binding singleton differs")
            binding(row, "binding_uuid")
            if text(row, "cutover_state") != "LEGACY_UNCERTIFIED" || integer(row, "format_version") != 2L then
              throw new SQLDataException("FirstBinding6 permits only the exact LEGACY_UNCERTIFIED Format2 binding")
          case (MySqlDbFirstBootstrapSnapshotJdbc.ReadMode.CertifiedReadback,
              MySqlCertifiedPhysicalBinding(bindingUuid, 2))
              if bindingUuid == inputs.auditExpectation.bindingUuid && relation.rows.size == 1 =>
            val row = relation.rows.head
            if integer(row, "singleton_fixed1") != 1L then
              throw new SQLDataException("schema6 certified readback physical binding singleton differs")
            binding(row, "binding_uuid")
            if text(row, "cutover_state") != "CERTIFIED" || integer(row, "format_version") != 2L then
              throw new SQLDataException("schema6 certified readback requires the exact CERTIFIED Format2 binding")
          case _ => throw new SQLDataException("locked physical binding differs from the exact qualification for this schema6 DATA read mode")
      case "fp_runtime_writer" =>
        if relation.rows.size != 1 then throw new SQLDataException("FirstBinding6 requires exactly one runtime writer row")
        val row = relation.rows.head
        if integer(row, "singleton_fixed1") != 1L then
          throw new SQLDataException("FirstBinding6 runtime writer singleton differs")
        binding(row, "binding_uuid")
      case _ => ()

  private[MySqlDbFirstBootstrapSnapshotJdbc] def requireLocked(token: MySqlDbFirstBootstrapSnapshotJdbc.LockedRead,
      verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope, budget: MySqlFormat3ReadBudget,
      packet: Long): Unit =
    requireCurrentContext()
    if !reading || token == null || (lockedValue ne token) || (verifiedValue ne verified) ||
      (scopeValue ne scope) || (dataBudget ne budget) || (token.inputs ne inputs.full27Inputs) ||
      (token.budget ne dataBudget) || packet != packetValue || packet <= 0L || token.packet != packetValue then
      throw new IllegalStateException("schema6 bootstrap Full27 reader token is stale or belongs to another scope/root/packet")

  override def close(): Unit =
    requireThread()
    if closed then return
    if cleanupFailure != null then
      throw new IllegalStateException("schema6 bootstrap reader cleanup remains incomplete", cleanupFailure)
    if reading then throw new IllegalStateException("schema6 bootstrap reader cannot close while a relation is streaming")
    closing = true
    try
      if dataBudget != null then
        dataBudget.closeScopedReadBudget()
        dataBudget = null
      if inputBorrow != null then
        inputBorrow.close()
        inputBorrow = null
      if holderFee != 0L then
        root.releaseRetainedMemory(holderFee, "schema6 bootstrap DATA reader holder detached")
        holderFee = 0L
      partialRelations = null
      snapshotValue = null
      lockedValue = null
      verifiedValue = null
      scopeValue = null
      qualificationValue = null
      readModeValue = null
      gateValue = null
      rollbackValue = null
      closed = true
    catch
      case cause: Throwable =>
        cleanupFailure = cause
        if failure == null then failure = cause else MySqlOwnedSessionLifecycle.suppress(failure, cause)
        throw cause
    finally closing = false

  override def toString: String = "MySqlDbFirstBootstrapSnapshotJdbc[Full27 DATA; caller RR/gate borrowed; no commit/rollback/image issuer]"

private[mysql] object MySqlDbFirstBootstrapSnapshotJdbc:
  private enum ReadMode:
    case InitialBootstrap
    case CertifiedReadback

  private val HolderBytes = 2048L
  private val DataStructureBytes = 4096L

  final case class CleanupObservation(closed: Boolean, dataChildOpen: Boolean, snapshotRetained: Boolean,
      partialRelationCount: Int, inputBorrowRetained: Boolean, holderBytes: Long,
      failure: Option[Throwable], cleanupFailure: Option[Throwable])

  final class CreationFailure private[MySqlDbFirstBootstrapSnapshotJdbc] (
      val original: Throwable, val retainedHolder: MySqlDbFirstBootstrapSnapshotJdbc)
      extends RuntimeException("schema6 bootstrap reader creation failed with retained cleanup responsibility", original)

  final class LockedRead private[MySqlDbFirstBootstrapSnapshotJdbc] (
      holder: MySqlDbFirstBootstrapSnapshotJdbc, private[mysql] val inputs: MySqlScriptSchemaInputs,
      private[mysql] val budget: MySqlFormat3ReadBudget, private[mysql] val packet: Long):
    private[mysql] def requireReader(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
        actualBudget: MySqlFormat3ReadBudget, actualPacket: Long): Unit =
      holder.requireLocked(this, verified, scope, actualBudget, actualPacket)

  /** Holder/borrow/child are charged before returning; callers can register this holder before the first SQL. */
  def create(inputs: MySqlDbFirstSessionInputs, root: MySqlFormat3ReadBudget,
      identity: MySqlOwnedIoIdentity): MySqlDbFirstBootstrapSnapshotJdbc =
    if inputs == null || root == null || identity == null then
      throw new IllegalArgumentException("exact schema6 bootstrap inputs/root/original IO identity required")
    inputs.requireForSession(root, identity)
    if inputs.ioIdentity ne identity then
      throw new IllegalArgumentException("FirstBinding6 bootstrap requires the original boot/connection identity")
    val full27 = inputs.full27Inputs
    root.requireScriptDataInputs(full27)
    if full27.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 || root.isScopedReadBudget || root.isClosed then
      throw new IllegalArgumentException("FirstBinding6 bootstrap requires the exact open Full27 workflow root")
    root.reserveRetainedMemory(HolderBytes, "schema6 bootstrap Full27 reader holder BEFORE allocation")
    var holder: MySqlDbFirstBootstrapSnapshotJdbc = null
    try
      holder = new MySqlDbFirstBootstrapSnapshotJdbc(inputs, root, identity, Thread.currentThread(),
        null, HolderBytes, null, null)
      holder.inputBorrow = inputs.borrowForReader()
      holder.dataBudget = root.newCanonicalDataScope()
      holder.dataBudget.requireScriptDataInputs(full27)
      holder.dataBudget.reserveRetainedMemory(DataStructureBytes,
        "schema6 bootstrap Full27 relation index/snapshot references BEFORE allocation")
      holder.partialRelations = mutable.LinkedHashMap.empty[String, MySqlFormat3RelationSnapshot]
      holder
    catch
      case cause: Throwable =>
        if holder == null then
          try root.releaseRetainedMemory(HolderBytes, "unallocated schema6 bootstrap reader holder")
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        else
          holder.failure = cause
          try holder.close()
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          if holder.dataBudget != null || holder.inputBorrow != null || holder.holderFee != 0L then
            throw new CreationFailure(cause, holder)
        throw cause
