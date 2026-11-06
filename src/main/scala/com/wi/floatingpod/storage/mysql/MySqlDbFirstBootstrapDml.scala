package com.wi.floatingpod.storage.mysql

import java.sql.{PreparedStatement, SQLDataException, Types}

/**
 * FirstBinding6 的固定表差量执行器。原 actor 在 durable intent 后调用；不提交、不授予 writer/Ready。
 * holder 必须在 execute 前登记，statement close 未确认时保留引用和原预算，不继续下一条 SQL。
 */
private[mysql] final class MySqlDbFirstBootstrapDml(
    inputs: MySqlScriptSchemaInputs,
    root: MySqlFormat3ReadBudget
):
  import MySqlDbFirstBootstrapDml.*
  private val thread = Thread.currentThread()
  private var attempted = false
  private var statement: PreparedStatement = null
  private var statementCloseAttempted = false
  private var fee = 0L
  private var workspaceFee = 0L
  private var failure: Throwable = null
  private var originalVerified: VerifiedMySqlConnection = null

  private def requireOwner(): Unit =
    require(Thread.currentThread() eq thread, "original FirstBinding6 DML worker required")
    require(inputs != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27, "original Full27 DML inputs/root required")
    root.requireScriptDataInputs(inputs)

  /** 上层先重建并核对 phase target；这里再次限制全图、表、列和精确旧行 CAS。 */
  def execute(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      gate: MySqlConnectionTransactionGate.Owner, rollback: MySqlRollbackOnlyState,
      before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot, phase: Phase): Unit =
    requireOwner()
    require(!attempted, "FirstBinding6 DML is one-shot, including failed send/close")
    attempted = true
    try
      root.reserveTemporaryMemory(65536L, "FirstBinding6 fixed phase validation/SQL workspace BEFORE allocation")
      workspaceFee = 65536L
      require(verified != null && scope != null && gate != null && rollback != null && phase != null,
        "actual FirstBinding6 transaction and closed phase required")
      MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollback)
      originalVerified = verified
      require(scope.ownerThread eq thread, "actual FirstBinding6 SQL scope thread required")
      require((scope.connectionToken eq verified.connection) && scope.verifiedConnection.contains(verified) &&
        verified.identity.catalog == inputs.schema && !verified.connection.getAutoCommit &&
        verified.connection.getTransactionIsolation == java.sql.Connection.TRANSACTION_REPEATABLE_READ,
        "actual original FirstBinding6 RR transaction required")
      requireImages(before, target)
      if phase == Phase.StartupRetirement then
        val lease = target.relation("fp_runtime_writer").rows.head(
          inputs.forTable("fp_runtime_writer").columns.indexWhere(_.name == "lease_until")) match
          case MySqlFormat3Cell.DateTime(value) => value
          case _ => throw new SQLDataException("startup retirement target lease must be typed DATETIME(6)")
        MySqlDbFirstAbandonedStartupData.requireRetirementTarget(before, target, lease.plusNanos(1000L))
      val tables = phase match
        // B1 INSERT要求B尚为空；B INSERT要求同一PREPARED B1先存在，catalog再消费B/B1。
        case Phase.Prepare => Vector("binding_bootstrap_intent", "physical_recovery_binding", "fp_script_catalog", "fp_runtime_writer")
        case Phase.Certify => Vector("physical_recovery_binding", "binding_bootstrap_intent", "fp_runtime_writer")
        case Phase.Release | Phase.Handoff | Phase.StartupRetirement => Vector("fp_runtime_writer")
      before.relations.foreach { relation =>
        if !tables.contains(relation.table) && relation != target.relation(relation.table) then
          reject("phase changed an unrelated Full27 relation: " + relation.table)
      }
      tables.foreach { table =>
        MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollback)
        val oldRows = before.relation(table).rows
        val rows = target.relation(table).rows
        if rows.size != 1 || oldRows.size > 1 then reject("fixed phase requires exact singleton target: " + table)
        val columns = inputs.forTable(table).columns
        val inserted = oldRows.isEmpty
        if inserted && (phase != Phase.Prepare || table == "fp_runtime_writer") then
          reject("only PREPARE may insert B/B1/catalog")
        val indices = columns.indices.filter(index => inserted || oldRows.head(index) != rows.head(index)).toVector
        val allowed = mutableColumns(table, phase)
        if !inserted && indices.exists(index => !allowed.contains(columns(index).name)) then
          reject("phase changes an immutable or ungranted column: " + table)
        if indices.isEmpty then reject("fixed phase has no actual row mutation: " + table)
        val quoted = columns.map(column => "`" + column.name + "`")
        val selected = indices.map(quoted)
        val prefix = s"`${inputs.schema}`.`$table`"
        val sql = if inserted then
          s"INSERT INTO $prefix (${quoted.mkString(",")}) VALUES (${columns.map(_ => "?").mkString(",")})"
        else
          s"UPDATE $prefix SET ${selected.map(_ + "=?").mkString(",")} WHERE ${quoted.map(_ + " <=> ?").mkString(" AND ")}"
        val values = indices.map(rows.head) ++ oldRows.headOption.getOrElse(Vector.empty)
        val payload = values.foldLeft(0L)((total, cell) => Math.addExact(total, cell match
          case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong, 16L)
          case MySqlFormat3Cell.Text(value) => Math.addExact(Math.multiplyExact(value.length.toLong, 4L), 16L)
          case _ => 64L))
        if Math.addExact(MySqlFormat3FullSnapshotSql.utf8Length(sql), payload) >
            inputs.effectivePacketLimit(target.maxAllowedPacketBytes) then reject("fixed phase exceeds actual packet budget")
        val reserved = Math.addExact(8192L, payload)
        root.reserveTemporaryMemory(reserved, "FirstBinding6 exact DML/parameter copies BEFORE JDBC allocation")
        fee = reserved
        statementCloseAttempted = false
        var primary: Throwable = null
        try
          statement = verified.connection.prepareStatement(sql)
          values.zipWithIndex.foreach((cell, index) => bind(statement, index + 1, cell))
          if statement.executeUpdate() != 1 then reject("exact old-row CAS changed other than one row: " + table)
        catch
          case cause: Throwable => primary = cause; throw cause
        finally
          try closeStatement()
          catch
            case cleanup: Throwable =>
              if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
              else throw cleanup
      }
    catch
      case cause: Throwable =>
        failure = cause
        if rollback != null then
          try rollback.mark(cause)
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause
    finally
      if statement == null && fee == 0L then
        MySqlFormat3BudgetCleanup.run(failure, Vector(() => releaseWorkspace()))

  private def requireImages(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot): Unit =
    if before == null || target == null || (before.inputs ne inputs) || (target.inputs ne inputs) ||
        before.schema != inputs.schema || before.connection != target.connection ||
        before.observationScope != target.observationScope || before.maxAllowedPacketBytes != target.maxAllowedPacketBytes ||
        before.relations.map(_.table) != inputs.canonicalOrder.map(_.table) ||
        target.relations.map(_.table) != inputs.canonicalOrder.map(_.table) then
      reject("exact complete Full27 before/target headers required")
    Vector(before, target).foreach { image =>
      if image.allocationBudget == null || image.allocationBudget.isClosed ||
          !(image.allocationBudget eq root) && !image.allocationBudget.isCanonicalChildOf(root) ||
          !image.allocationBudget.sharesAllocationLedger(root) then reject("phase images belong to another root")
    }

  private def closeStatement(): Unit =
    if statement != null then
      if statementCloseAttempted then throw new IllegalStateException("FirstBinding6 statement close remains unresolved", failure)
      statementCloseAttempted = true
      statement.close()
      statement = null
    if fee != 0L then
      root.releaseTemporaryMemory(fee, "FirstBinding6 DML parameters detached after confirmed statement close")
      fee = 0L

  def close(): Unit =
    requireOwner()
    closeStatement()
    releaseWorkspace()

  /** 原连接已确认关闭后只撤本地残留；不重发 SQL、不重试未知 statement.close。 */
  def retireAfterClosedConnection(actual: VerifiedMySqlConnection): Unit =
    requireOwner()
    require(actual != null && (originalVerified eq actual) && actual.connection.isClosed,
      "exact original closed FirstBinding6 connection required before local DML retirement")
    statement = null
    if fee != 0L then
      root.releaseTemporaryMemory(fee, "FirstBinding6 DML copies detached after original connection closed")
      fee = 0L
    releaseWorkspace()
    originalVerified = null

  private def releaseWorkspace(): Unit =
    if workspaceFee != 0L then
      root.releaseTemporaryMemory(workspaceFee, "FirstBinding6 fixed phase workspace detached")
      workspaceFee = 0L

  def hasUnresolvedStatement: Boolean = { requireOwner(); statement != null || fee != 0L || workspaceFee != 0L }

private[mysql] object MySqlDbFirstBootstrapDml:
  enum Phase:
    case Prepare, Certify, Handoff, Release, StartupRetirement

  private def mutableColumns(table: String, phase: Phase): Set[String] = table match
    case "physical_recovery_binding" if phase == Phase.Certify => Set("cutover_state")
    case "binding_bootstrap_intent" if phase == Phase.Certify => Set("stage", "marker_ack_sha256")
    case "fp_runtime_writer" if phase == Phase.Release || phase == Phase.StartupRetirement => Set("owner_boot_uuid", "lease_until")
    case "fp_runtime_writer" if phase == Phase.Handoff => Set("writer_epoch", "owner_boot_uuid", "lease_until")
    case "fp_runtime_writer" => Set("writer_epoch", "owner_boot_uuid", "lease_until", "last_commit_operation_uuid", "last_commit_fingerprint")
    case _ => Set.empty

  private def bind(statement: PreparedStatement, index: Int, cell: MySqlFormat3Cell): Unit = cell match
    case MySqlFormat3Cell.SqlNull => statement.setNull(index, Types.NULL)
    case MySqlFormat3Cell.Text(value) => statement.setString(index, value)
    case MySqlFormat3Cell.Binary(value) => statement.setBytes(index, Array.tabulate(value.length)(value.byteAt))
    case MySqlFormat3Cell.SignedInteger(value) => statement.setLong(index, value)
    case MySqlFormat3Cell.Floating(value) => statement.setDouble(index, java.lang.Double.longBitsToDouble(value))
    case MySqlFormat3Cell.DateTime(value) => statement.setObject(index, value)

  private def reject(message: String): Nothing = throw new SQLDataException("FirstBinding6 DML rejected: " + message)
