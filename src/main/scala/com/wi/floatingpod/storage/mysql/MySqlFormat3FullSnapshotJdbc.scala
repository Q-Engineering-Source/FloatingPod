package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile

import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLDataException, SQLException, SQLNonTransientConnectionException}
import java.time.LocalDateTime

/**
 * 只在调用方已验证的连接上读取 format3 全部二十张表。
 * read() 自行管理只读 REPEATABLE READ 生命周期；fenced 入口沿用调用方已经持有的事务。
 */
private[mysql] final class MySqlFormat3FullSnapshotReader(
    verified: VerifiedMySqlConnection,
    scope: MySqlAuditBootScope,
    schema: String,
    expectedProfile: SourceLimitProfile,
    sharedRootLedger: Option[MySqlFormat3AllocationLedger] = None
) extends MySqlFormat3QuerySnapshotSource:
  require(verified != null && scope != null && expectedProfile != null, "format3 reader requires verified connection, owned scope, and explicit limit profile")
  require(schema != null && schema.matches("[a-z][a-z0-9_]{0,63}"), "fixed format3 schema identifier is unsafe")
  require(sharedRootLedger != null && !sharedRootLedger.exists(_ == null), "optional exact root allocation ledger required")
  private val ownerThread = Thread.currentThread()
  require(scope.ownerThread eq ownerThread, "format3 reader must be created on its owned IO thread")
  private val connection = verified.connection
  require(scope.verifiedConnection.contains(verified) && (scope.connectionToken eq connection) &&
    (scope.connectionIdentity eq verified.identity), "format3 reader requires the exact verified connection scope")
  require(schema == verified.identity.catalog, "format3 schema must equal the verified connection catalog")

  def read(): MySqlFormat3FullSnapshot = readWithRetainedBudget(None)

  /** Mode readback 为同一工作流保留图像；本次读分配共享原 materialization 的 aggregate cap。 */
  private[mysql] def readWithinModeWorkflow(
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("mode readback requires the exact open workflow allocation budget")
    readWithRetainedBudget(Some(retainedBudget))

  private[mysql] def readWithinInactiveDomainWorkflow(
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("inactive domain readback requires the exact open workflow allocation budget")
    readWithRetainedBudget(Some(retainedBudget))

  private[mysql] def readWithinDriftWorkflow(
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("mode readback requires the exact open workflow allocation budget")
    readWithRetainedBudget(Some(retainedBudget))

  private[mysql] def readWithinNormalRecallWorkflow(
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("normal recall readback requires the exact open workflow allocation budget")
    readWithRetainedBudget(Some(retainedBudget))


  private[mysql] def readWithinControlledDebugWorkflow(
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget then
      throw new SQLNonTransientConnectionException("controlled debug readback requires the exact open workflow allocation budget")
    readWithRetainedBudget(Some(retainedBudget))


  /** Session 的首个固定查询使用原 Main reservation root，保留完整 capture 与回复直到 exact drop。 */
  override def readWithinQueryWorkflow(root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    if root == null || (root.profile ne expectedProfile) || root.isScopedReadBudget || root.isClosed then
      throw new SQLNonTransientConnectionException("query requires the actual open reserved allocation root")
    readWithRetainedBudget(None, Some(root))

  private def readWithRetainedBudget(
      retainedBudget: Option[MySqlFormat3ReadBudget],
      initialQueryRoot: Option[MySqlFormat3ReadBudget] = None
  ): MySqlFormat3FullSnapshot =
    requireOwnerThread()
    initialQueryRoot.foreach(_.requireFormat3Full20Layout())
    retainedBudget.foreach { budget =>
      budget.requireFormat3Full20Layout()
      if (budget.profile ne expectedProfile) || budget.isClosed then
        throw new SQLNonTransientConnectionException("format3 read budget differs from the exact open workflow profile")
    }
    if connection.isClosed then throw new SQLNonTransientConnectionException("format3 snapshot connection is already closed")
    if !connection.getAutoCommit then throw new IllegalStateException("format3 snapshot requires an idle auto-commit owned connection")

    val oldIsolation = connection.getTransactionIsolation
    var isolationChangeAttempted = false
    var beginAttempted = false
    var primary: Throwable = null
    var cleanupFailed = false
    var result: MySqlFormat3FullSnapshot = null
    def recordCleanupFailure(failure: Throwable): Unit =
      cleanupFailed = true
      if primary == null then primary = failure
      else if failure ne primary then primary.addSuppressed(failure)
    try
      isolationChangeAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 snapshot could not establish REPEATABLE READ")
      beginAttempted = true
      connection.setAutoCommit(false)
      if connection.getAutoCommit then throw new SQLNonTransientConnectionException("format3 snapshot transaction did not start")
      result = readLockedRelations(retainedBudget, initialQueryRoot)
    catch
      case failure: Throwable => primary = failure

    var rollbackAcknowledged = !beginAttempted
    if beginAttempted then
      try
        connection.rollback()
        rollbackAcknowledged = true
      catch
        case failure: Throwable =>
          recordCleanupFailure(failure)

    var autocommitRestored = false
    if rollbackAcknowledged then
      try
        if !connection.getAutoCommit then connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new SQLNonTransientConnectionException("format3 snapshot auto-commit restoration failed")
        autocommitRestored = true
      catch
        case failure: Throwable =>
          recordCleanupFailure(failure)

    if autocommitRestored && isolationChangeAttempted then
      try
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then
          throw new SQLNonTransientConnectionException("format3 snapshot isolation restoration failed")
      catch
        case failure: Throwable =>
          recordCleanupFailure(failure)

    if cleanupFailed then
      val cleanupCause = if primary != null then primary else new SQLException("format3 snapshot transaction cleanup failed")
      releaseScopedSnapshot(result, cleanupCause)
      throw new MySqlFormat3SnapshotCleanupUncertainException(cleanupCause)
    if primary != null then
      releaseScopedSnapshot(result, primary)
      throw primary
    if result == null then throw new SQLNonTransientConnectionException("format3 snapshot returned no complete observation")
    try
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        throw failure

  private def releaseScopedSnapshot(snapshot: MySqlFormat3FullSnapshot, primary: Throwable): Unit =
    if snapshot != null && snapshot.allocationBudget.isScopedReadBudget then
      try snapshot.allocationBudget.closeScopedReadBudget()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  /** 仅真正未admitted recovery actor的私有locked owner；不是normal/Fresh资格借用。 */
  private[mysql] def readWithinCertifiedRecoveryInspection(
      owner: MySqlCertifiedRecoveryInspectionActor.RecoveryLockedReadOwner): MySqlFormat3FullSnapshot =
    try
      requireOwnerThread()
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      require(!connection.getAutoCommit && connection.getTransactionIsolation == Connection.TRANSACTION_REPEATABLE_READ,
        "certified recovery full20 requires actual active RR transaction")
      val result = readLockedRelations(retainedBudget = Some(owner.originalRetainedRoot))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case cause: Throwable =>
        owner.markFailure(cause)
        throw cause

  /** 在真实 fenced business facade 中读完整图，不触碰 caller transaction 生命周期。 */
  private[mysql] def readWithinFencedTransaction(
      owner: MySqlFencedTransaction.LockedReadOwner,
      transaction: MySqlBusinessTransaction
  ): MySqlFormat3FullSnapshot =
    try
      requireOwnerThread()
      transaction.requireFormat3ReaderAccess(owner, verified, scope, schema, expectedProfile)
      if connection.getAutoCommit then
        throw new IllegalStateException("format3 fenced snapshot requires the caller transaction to remain active")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 fenced snapshot requires caller REPEATABLE READ isolation")
      val result = readLockedRelations()
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        transaction.markFailure(failure)
        throw failure

  /**
   * 首个 operation context 尚未生成时的只读 planning 路径。Owner 只能由真实 F3 runner 在 M→B→W
   * 锁与身份核验后发行；此入口不开始、提交、回滚或恢复调用方事务。
   */
  private[mysql] def readWithinPlanningTransaction(
      owner: MySqlFencedTransaction.PlanningReadOwner
  ): MySqlFormat3FullSnapshot =
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 planning-read owner is required")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 planning read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 planning read requires caller REPEATABLE READ isolation")
      val result = readLockedRelations(initialPlanningRoot = owner.originalRetainedRoot)
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        throw failure

  /** Mode writer readback is issued only after the exact M→B→W locks and staged F3 writes. */
  private[mysql] def readWithinModeWriteTransaction(
      owner: MySqlFencedTransaction.ModeWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    var result: MySqlFormat3FullSnapshot = null
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 mode-write read owner is required")
      if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget || retainedBudget.isClosed then
        throw new SQLNonTransientConnectionException("mode-write read requires the exact open workflow allocation budget")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 mode-write read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 mode-write read requires caller REPEATABLE READ isolation")
      result = readLockedRelations(Some(retainedBudget))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        if owner != null then owner.markFailure(failure)
        throw failure

  private[mysql] def readWithinNormalRecallWriteTransaction(
      owner: MySqlFencedTransaction.NormalRecallWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    var result: MySqlFormat3FullSnapshot = null
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 normal-recall-write read owner is required")
      if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget || retainedBudget.isClosed then
        throw new SQLNonTransientConnectionException("normal-recall-write read requires the exact open workflow allocation budget")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      owner.requireRetainedRoot(retainedBudget)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 normal-recall-write read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 normal-recall-write read requires caller REPEATABLE READ isolation")
      result = readLockedRelations(Some(retainedBudget))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        if owner != null then owner.markFailure(failure)
        throw failure


  private[mysql] def readWithinControlledDebugWriteTransaction(
      owner: MySqlFencedTransaction.ControlledDebugWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    var result: MySqlFormat3FullSnapshot = null
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 controlled-debug-write read owner is required")
      if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget || retainedBudget.isClosed then
        throw new SQLNonTransientConnectionException("controlled-debug-write read requires the exact open workflow allocation budget")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      owner.requireRetainedRoot(retainedBudget)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 controlled-debug-write read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 controlled-debug-write read requires caller REPEATABLE READ isolation")
      result = readLockedRelations(Some(retainedBudget))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        if owner != null then owner.markFailure(failure)
        throw failure


  private[mysql] def readWithinModeWorkflowTransaction(
      owner: MySqlFencedTransaction.ModeWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot = readWithinModeWriteTransaction(owner, retainedBudget)

  /** Fresh 的私有 B1 路径先实锁 M→B→W，再由同一 active gate 读取完整二十关系。 */
  private[mysql] def readWithinFreshBootstrapTransaction(
      owner: MySqlFreshWorldBootstrap.Format3ReadOwner
  ): MySqlFormat3FullSnapshot =
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("Fresh format3 locked-read owner is required")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("Fresh format3 full20 read requires its active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("Fresh format3 full20 read requires caller REPEATABLE READ isolation")
      val result = readLockedRelations(retainedBudget = Some(owner.originalRetainedRoot))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        if owner != null then owner.markFailure(failure)
        throw failure

  private[mysql] def readWithinInactiveDomainWriteTransaction(
      owner: MySqlFencedTransaction.InactiveDomainWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    var result: MySqlFormat3FullSnapshot = null
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 inactive domain-write read owner is required")
      if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget || retainedBudget.isClosed then
        throw new SQLNonTransientConnectionException("inactive domain-write read requires the exact open workflow allocation budget")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 inactive domain-write read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 inactive domain-write read requires caller REPEATABLE READ isolation")
      result = readLockedRelations(Some(retainedBudget))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        if owner != null then owner.markFailure(failure)
        throw failure

  private[mysql] def readWithinDriftWriteTransaction(
      owner: MySqlFencedTransaction.DriftWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot =
    var result: MySqlFormat3FullSnapshot = null
    try
      requireOwnerThread()
      if owner == null then throw new IllegalStateException("exact format3 drift-write read owner is required")
      if retainedBudget == null || (retainedBudget.profile ne expectedProfile) || retainedBudget.isScopedReadBudget || retainedBudget.isClosed then
        throw new SQLNonTransientConnectionException("drift-write read requires the exact open workflow allocation budget")
      owner.requireCurrent(verified, scope, schema, expectedProfile, connection)
      if connection.isClosed || connection.getAutoCommit then
        throw new SQLNonTransientConnectionException("format3 drift-write read requires the active owned transaction")
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLNonTransientConnectionException("format3 drift-write read requires caller REPEATABLE READ isolation")
      result = readLockedRelations(Some(retainedBudget))
      owner.requireSnapshot(result)
      result.canonicalSha256
      result
    catch
      case failure: Throwable =>
        releaseScopedSnapshot(result, failure)
        if owner != null then owner.markFailure(failure)
        throw failure

  private[mysql] def readWithinModeWorkflowTransaction(
      owner: MySqlFencedTransaction.DriftWriteReadOwner,
      retainedBudget: MySqlFormat3ReadBudget
  ): MySqlFormat3FullSnapshot = readWithinDriftWriteTransaction(owner, retainedBudget)

  private def readLockedRelations(
      retainedBudget: Option[MySqlFormat3ReadBudget] = None,
      initialPlanningRoot: Option[MySqlFormat3ReadBudget] = None
  ): MySqlFormat3FullSnapshot =
    require(!(retainedBudget.nonEmpty && initialPlanningRoot.nonEmpty), "initial root and subsequent image scope are distinct")
    if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLNonTransientConnectionException("format3 locked read no longer has REPEATABLE READ isolation")
    val observation = MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
    val observationScope = MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
    val sessionMaxPacket = readSessionMaxAllowedPacket()
    if sessionMaxPacket <= 0L then throw new SQLDataException("MySQL @@session.max_allowed_packet is not a positive integer")
    val effectivePacketLimit = Math.min(expectedProfile.maxSqlPacketBytes(), sessionMaxPacket)
    val budget = initialPlanningRoot match
      case Some(root) =>
        if root.profile ne expectedProfile then
          throw new SQLNonTransientConnectionException("planning root differs from actual session profile")
        root.bindInitialMeasuredPacketLimit(sessionMaxPacket)
        root
      case None =>
        retainedBudget match
          case Some(parent) =>
            if parent.profile ne expectedProfile then
              throw new SQLNonTransientConnectionException("format3 read image differs from its workflow allocation profile")
            parent.issueOwnedSnapshotImage(schema, observation, observationScope, sessionMaxPacket)
          case None => sharedRootLedger match
            case Some(ledger) => MySqlFormat3ReadBudget.rootOnLedger(expectedProfile, effectivePacketLimit, ledger)
            case None => new MySqlFormat3ReadBudget(expectedProfile, effectivePacketLimit)
    try
      if retainedBudget.isEmpty then budget.initialize(schema, observation, observationScope, sessionMaxPacket)
      val capturedByTable = scala.collection.mutable.Map.empty[String, MySqlFormat3RelationSnapshot]
      MySqlFormat3RelationShapes.lockedReadOrder.foreach { shape =>
        val relation = readRelation(shape, budget, sessionMaxPacket)
        if capturedByTable.contains(shape.table) then throw new SQLDataException(s"format3 locked read repeated ${shape.table}")
        capturedByTable.put(shape.table, relation)
        if shape.table == "fp_schema_manifest" then validateManifestProfile(relation, observation, budget)
      }
      if capturedByTable.size != MySqlFormat3RelationShapes.ExpectedRelationCount then
        throw new SQLDataException("format3 locked read did not capture all twenty relations")
      val canonicalRelations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
        capturedByTable.getOrElse(shape.table, throw new SQLDataException(s"format3 locked read omitted ${shape.table}"))
      }
      new MySqlFormat3FullSnapshot(schema, observation, observationScope, sessionMaxPacket,
        expectedProfile, canonicalRelations, budget)
    catch
      case failure: Throwable =>
        if budget.isScopedReadBudget then
          try budget.closeScopedReadBudget()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  private def readSessionMaxAllowedPacket(): Long =
    requireOwnerThread()
    val sql = "SELECT @@SESSION.max_allowed_packet"
    validateProfileSqlPacket(Math.addExact(MySqlFormat3FullSnapshotSql.utf8Length(sql), 16L))
    MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
      MySqlBusinessRows.using(statement.executeQuery()) { result =>
        if !result.next() then throw new SQLDataException("MySQL max_allowed_packet query returned no row")
        val value = result.getLong(1)
        if result.wasNull() then throw new SQLDataException("MySQL max_allowed_packet query returned SQL NULL")
        if result.next() then throw new SQLDataException("MySQL max_allowed_packet query returned more than one row")
        value
      }
    }

  /** 只能由新 schema 私有 owner 调用封闭表读取；不公开任意 SQL/shape。 */
  private[mysql] def readScriptSchemaRelation(owner: MySqlFreshWorldBootstrap.ScriptSchemaReadOwner,
      table: String, budget: MySqlFormat3ReadBudget, packet: Long): MySqlFormat3RelationSnapshot =
    owner.requireCurrent(verified, scope)
    budget.requireScriptPublication1Inputs(owner.inputs)
    readRelation(MySqlScriptSchemaInputs.forTable(table), budget, packet, Some(owner.inputs))

  /** schema6只接受原holder在真实gate/qualification核对后发行的短命token。 */
  private[mysql] def readDbFirstRelation(owner: MySqlDbFirstSnapshotJdbc.LockedRead,
      table: String, budget: MySqlFormat3ReadBudget, packet: Long): MySqlFormat3RelationSnapshot =
    require(owner != null,"actual schema6 locked reader token required")
    owner.requireReader(verified,scope,budget,packet)
    val inputs=owner.inputs
    budget.requireScriptDataInputs(inputs)
    readRelation(inputs.forTable(table),budget,packet,Some(inputs),Some(owner))

  /** FirstBinding6 bootstrap/readback Full27 DATA capture; its token is distinct from ordinary CERTIFIED/service/inspection reads. */
  private[mysql] def readDbFirstBootstrapRelation(owner: MySqlDbFirstBootstrapSnapshotJdbc.LockedRead,
      table: String, budget: MySqlFormat3ReadBudget, packet: Long): MySqlFormat3RelationSnapshot =
    require(owner != null,"actual schema6 bootstrap locked reader token required")
    owner.requireReader(verified,scope,budget,packet)
    val inputs=owner.inputs
    budget.requireScriptDataInputs(inputs)
    val observation = MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
    val observationScope = MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
    budget.requireInitializedObservation(inputs.schema, observation, observationScope, packet)
    readRelation(inputs.forTable(table),budget,packet,Some(inputs),None,Some(owner))

  private def readRelation(shape: MySqlFormat3RelationShape, budget: MySqlFormat3ReadBudget, sessionMaxPacket: Long, script: Option[MySqlScriptSchemaInputs] = None,
      dbFirst: Option[MySqlDbFirstSnapshotJdbc.LockedRead] = None,
      bootstrap: Option[MySqlDbFirstBootstrapSnapshotJdbc.LockedRead] = None): MySqlFormat3RelationSnapshot =
    requireOwnerThread()
    val schema6=script.exists(_.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27)
    if schema6 != (dbFirst.nonEmpty || bootstrap.nonEmpty) || (dbFirst.nonEmpty && bootstrap.nonEmpty) then
      throw new IllegalStateException("schema6 stream requires exactly one original typed locked reader token")
    dbFirst.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
    bootstrap.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
    val consistentObservation=dbFirst.exists(_.consistentObservation)
    val sqlLength = if schema6 then MySqlDbFirstSnapshotSql.selectUtf8Length(schema,shape.table,consistentObservation) else if script.nonEmpty then MySqlScriptSchemaSnapshotSql.selectUtf8Length(shape.table) else MySqlFormat3FullSnapshotSql.selectUtf8Length(schema, shape)
    validateSqlPacketLength(Math.addExact(sqlLength, 16L), sessionMaxPacket)
    MySqlFormat3JvmArrayLimit.requireLength(sqlLength, s"${shape.table} fixed SELECT")
    val sqlStringAndBuilderBytes = Math.addExact(128L, Math.multiplyExact(sqlLength, 4L))
    budget.reserveTemporaryMemory(sqlStringAndBuilderBytes, s"${shape.table} fixed SELECT construction")
    var primary: Throwable = null
    try
      val sql = if schema6 then MySqlDbFirstSnapshotSql.select(schema,shape.table,consistentObservation) else if script.nonEmpty then MySqlScriptSchemaSnapshotSql.select(shape.table) else MySqlFormat3FullSnapshotSql.select(schema, shape)
      MySqlBusinessRows.using(connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) { statement =>
        // 选择 Connector/J 的 forward-only 流读取选项；驱动内部行包缓冲仍须由实际驱动验收确认。
        statement.setFetchSize(Integer.MIN_VALUE)
        MySqlBusinessRows.using(statement.executeQuery()) { result =>
          if schema6 then MySqlDbFirstSnapshotSql.validateMetadata(result.getMetaData,shape.table)
          else if script.nonEmpty then MySqlScriptSchemaSnapshotSql.validateMetadata(result.getMetaData, shape.table)
          else MySqlFormat3FullSnapshotSql.validateMetadata(result.getMetaData, shape)
          val rows = Vector.newBuilder[Vector[MySqlFormat3Cell]]
          var rowCount = 0L
          while result.next() do
            requireOwnerThread()
            dbFirst.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
            bootstrap.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
            if rowCount >= Int.MaxValue.toLong then throw new SQLDataException(s"${shape.table} row count exceeds the JVM Vector index range")
            budget.beginRow(shape.columns.size, shape.table, rowCount)
            val row = Vector.newBuilder[MySqlFormat3Cell]
            var index = 0
            while index < shape.columns.size do
              row += readCell(result, index + 1, shape, shape.columns(index), budget, rowCount, script)
              index += 1
            budget.endRow(shape.table, rowCount)
            rows += row.result()
            rowCount = Math.addExact(rowCount, 1L)
          dbFirst.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
          bootstrap.foreach(_.requireReader(verified,scope,budget,sessionMaxPacket))
          MySqlFormat3RelationSnapshot(shape.table, rows.result())
        }
      }
    catch
      case failure: SQLException =>
        val wrapped = new MySqlFormat3SnapshotReadException(shape.table, "query-or-decode", failure)
        primary = wrapped
        throw wrapped
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => budget.releaseTemporaryMemory(sqlStringAndBuilderBytes, s"${shape.table} fixed SELECT string")))

  private def readCell(result: ResultSet, index: Int, shape: MySqlFormat3RelationShape,
      column: MySqlFormat3ColumnShape, budget: MySqlFormat3ReadBudget, rowNumber: Long, script: Option[MySqlScriptSchemaInputs]): MySqlFormat3Cell =
    val context = s"${shape.table} row $rowNumber column ${column.name}"
    try
      column.kind match
        case MySqlFormat3SqlKind.Text =>
          val reader = result.getCharacterStream(index)
          if reader == null then nullCell(column, budget, context)
          else
            val value = MySqlFormat3CellStreamDecoder.readText(reader, column.maximumLength, budget, context,
              script.flatMap(_.maximumPayloadBytes(shape.table, column)))
            MySqlFormat3Cell.Text(value)
        case MySqlFormat3SqlKind.Binary =>
          val stream = result.getBinaryStream(index)
          if stream == null then nullCell(column, budget, context)
          else
            val value = MySqlFormat3CellStreamDecoder.readBinary(stream, script.flatMap(_.maximumPayloadBytes(shape.table, column)).map(n => Math.min(n, Int.MaxValue.toLong).toInt).orElse(column.maximumLength), budget, context)
            MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value, budget))
        case MySqlFormat3SqlKind.Integer =>
          val value = result.getLong(index)
          if result.wasNull() then nullCell(column, budget, context)
          else
            column.requireSignedInteger(value, s"$context is outside signed")
            budget.beginFixedCell(8, integerWireWidth(column), context)
            MySqlFormat3Cell.SignedInteger(value)
        case MySqlFormat3SqlKind.Floating =>
          val value = result.getDouble(index)
          if result.wasNull() then nullCell(column, budget, context)
          else
            budget.beginFixedCell(8, 64, context)
            MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value))
        case MySqlFormat3SqlKind.DateTime =>
          val value = result.getObject(index, classOf[LocalDateTime])
          if value == null then nullCell(column, budget, context)
          else
            val local = value
            if local.getNano % 1000 != 0 then throw new SQLDataException(s"$context exceeds DATETIME(6) microsecond precision")
            budget.beginFixedCell(13, 32, context)
            MySqlFormat3Cell.DateTime(local)
    catch
      case failure: SQLException => throw failure
      case failure: Throwable => throw new MySqlFormat3CellReadException(shape.table, column.name, rowNumber, failure)

  private def nullCell(column: MySqlFormat3ColumnShape, budget: MySqlFormat3ReadBudget, context: String): MySqlFormat3Cell =
    if !column.nullable then throw new SQLDataException(s"$context is SQL NULL but the frozen format3 shape is NOT NULL")
    budget.beginNullCell(context)
    MySqlFormat3Cell.SqlNull

  private def validateManifestProfile(relation: MySqlFormat3RelationSnapshot,
      observation: MySqlFormat3ConnectionObservation, budget: MySqlFormat3ReadBudget): Unit =
    if relation.rows.size != 1 then throw new SQLDataException("format3 fp_schema_manifest must return exactly one row")
    val shape = MySqlFormat3RelationShapes.forTable("fp_schema_manifest")
    val row = relation.rows.head
    def field(name: String): MySqlFormat3Cell =
      val index = shape.columns.indexWhere(_.name == name)
      if index < 0 then throw new SQLDataException(s"format3 manifest column $name is outside the fixed shape")
      row(index)
    def text(name: String): String = field(name) match
      case MySqlFormat3Cell.Text(value) => value
      case _ => throw new SQLDataException(s"format3 manifest column $name has an invalid typed value")
    def binary(name: String): MySqlFormat3Binary = field(name) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => throw new SQLDataException(s"format3 manifest column $name has an invalid typed value")
    def integer(name: String): Long = field(name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException(s"format3 manifest column $name has an invalid typed value")

    if integer("singleton_fixed1") != 1L || text("backend") != "MYSQL" || integer("format_version") != 3L then
      throw new SQLDataException("format3 manifest backend/version does not match the reader")
    if !binary("expected_server_uuid").equalsUuid(observation.serverUuid) then
      throw new SQLDataException("format3 manifest server UUID differs from the verified connection")
    if text("limit_profile_encoding") != SourceLimitProfile.ENCODING then
      throw new SQLDataException("format3 manifest limit profile encoding is unsupported")
    budget.reserveTemporaryMemory(192L, "format3 expected source limit profile copies")
    var profilePrimary: Throwable = null
    val profileMatches = try
      val expectedBytes = expectedProfile.canonicalBytes()
      val expectedHash = expectedProfile.sha256()
      binary("limit_profile_bytes").equalsBytes(expectedBytes) && binary("limit_profile_sha256").equalsBytes(expectedHash)
    catch
      case failure: Throwable =>
        profilePrimary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(profilePrimary, Vector(
        () => budget.releaseTemporaryMemory(192L, "format3 expected source limit profile copies")))
    if !profileMatches then
      throw new SQLDataException("format3 manifest does not contain the explicitly expected limit profile")
    budget.reserveTemporaryMemory(1_024L, "format3 expected source limit scalar checks")
    var scalarPrimary: Throwable = null
    try
      val expectedValues = Vector(
        "max_region_bytes" -> expectedProfile.maxRegionBytes(),
        "max_chunk_bytes" -> expectedProfile.maxChunkBytes(),
        "max_decompressed_bytes" -> expectedProfile.maxDecompressedBytes(),
        "max_nbt_depth" -> expectedProfile.maxNbtDepth(),
        "max_members" -> expectedProfile.maxMembers(),
        "max_retained_bytes" -> expectedProfile.maxRetainedBytes(),
        "max_sql_packet_bytes" -> expectedProfile.maxSqlPacketBytes(),
        "max_native_artifact_bytes" -> expectedProfile.maxNativeArtifactBytes()
      )
      expectedValues.foreach { (name, expected) =>
        if integer(name) != expected then throw new SQLDataException(s"format3 manifest budget $name differs from the explicit profile")
      }
    catch
      case failure: Throwable =>
        scalarPrimary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(scalarPrimary, Vector(
        () => budget.releaseTemporaryMemory(1_024L, "format3 expected source limit scalar checks")))

  private def integerWireWidth(column: MySqlFormat3ColumnShape): Int =
    column.integerPacketWidth

  private def validateSqlPacketLength(bytes: Long, sessionMaxPacket: Long): Unit =
    if bytes > expectedProfile.maxSqlPacketBytes() || bytes > sessionMaxPacket then
      throw new SQLDataException("format3 SELECT exceeds the configured or observed MySQL packet budget")

  private def validateProfileSqlPacket(bytes: Long): Unit =
    if bytes > expectedProfile.maxSqlPacketBytes() then
      throw new SQLDataException("format3 session-probe SELECT exceeds explicit max_sql_packet_bytes")

  private def requireOwnerThread(): Unit =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("format3 JDBC snapshot is confined to its owned IO thread")

/** 固定 format3 SQL 生成和 JDBC 元数据对照，不接受 caller 传入的列或排序键。 */
private[mysql] object MySqlFormat3FullSnapshotSql:
  private[mysql] def utf8Length(sql: String): Long =
    if sql == null then throw new IllegalArgumentException("fixed SQL is required")
    MySqlFormat3Utf8.byteLength(sql)

  private[mysql] def selectUtf8Length(schema: String, shape: MySqlFormat3RelationShape): Long =
    validateInput(schema, shape)
    var length = 7L // SELECT<space>
    var index = 0
    while index < shape.columns.size do
      if index > 0 then length = Math.addExact(length, 1L)
      length = Math.addExact(length, Math.addExact(shape.columns(index).name.length.toLong, 2L))
      index += 1
    length = Math.addExact(length, 6L) // <space>FROM<space>
    length = Math.addExact(length, Math.addExact(schema.length.toLong, Math.addExact(shape.table.length.toLong, 5L)))
    length = Math.addExact(length, 10L) // <space>ORDER<space>BY<space>
    index = 0
    while index < shape.primaryKey.size do
      if index > 0 then length = Math.addExact(length, 1L)
      length = Math.addExact(length, Math.addExact(shape.primaryKey(index).length.toLong, 2L))
      index += 1
    Math.addExact(length, Math.addExact(1L, shape.lockClause.length.toLong))

  def select(schema: String, shape: MySqlFormat3RelationShape): String =
    validateInput(schema, shape)
    val requiredLength = selectUtf8Length(schema, shape)
    val capacity = MySqlFormat3JvmArrayLimit.requireLength(requiredLength, s"${shape.table} fixed SELECT")
    val builder = new StringBuilder(capacity)
    builder.append("SELECT ")
    var index = 0
    while index < shape.columns.size do
      if index > 0 then builder.append(',')
      builder.append('`').append(shape.columns(index).name).append('`')
      index += 1
    builder.append(" FROM `").append(schema).append("`.`").append(shape.table).append("` ORDER BY ")
    index = 0
    while index < shape.primaryKey.size do
      if index > 0 then builder.append(',')
      builder.append('`').append(shape.primaryKey(index)).append('`')
      index += 1
    builder.append(' ').append(shape.lockClause)
    val sql = builder.toString
    if utf8Length(sql) != requiredLength then throw new SQLDataException("format3 fixed SELECT length calculation differs from generated SQL")
    sql

  private def validateInput(schema: String, shape: MySqlFormat3RelationShape): Unit =
    require(schema != null && schema.matches("[a-z][a-z0-9_]{0,63}"), "fixed schema identifier is unsafe")
    require(shape != null && MySqlFormat3RelationShapes.lockedReadOrder.contains(shape), "relation shape is outside the frozen format3 set")

  private[mysql] def validateMetadata(metadata: ResultSetMetaData, shape: MySqlFormat3RelationShape): Unit =
    if metadata == null then throw new SQLDataException(s"${shape.table} result has no JDBC metadata")
    if metadata.getColumnCount != shape.columns.size then throw new SQLDataException(s"${shape.table} result does not expose all frozen columns")
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val index = offset + 1
      if metadata.getColumnLabel(index) != column.name then throw new SQLDataException(s"${shape.table} result column order differs at ${column.name}")
      if metadata.getColumnType(index) != column.jdbcType then throw new SQLDataException(s"${shape.table}.${column.name} JDBC type differs from its frozen shape")
      val nullable = metadata.isNullable(index)
      val expectedNullable = if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls
      if nullable != expectedNullable then throw new SQLDataException(s"${shape.table}.${column.name} JDBC nullability differs from its frozen shape")
      if column.kind == MySqlFormat3SqlKind.DateTime && metadata.getScale(index) != 6 then
        throw new SQLDataException(s"${shape.table}.${column.name} is not exposed as DATETIME(6)")
    }

private[mysql] final class MySqlFormat3AllocationLedger(private val limitBytes: Long):
  require(limitBytes > 0L, "format3 aggregate allocation limit must be positive")
  private var retainedBytes = 0L
  private var temporaryBytes = 0L
  private var successfulReserveHighWaterBytes = 0L
  // 独立DATA诊断只观察同ledger临时峰值；不修改计数/限额，不调用可失败callback。
  private var censusObservation: MySqlModeEnvelopeV2Census.Partial = null
  def beginCensus(value: MySqlModeEnvelopeV2Census.Partial): Unit = synchronized {
    require(value != null && censusObservation == null, "one original census observation")
    value.beginTemporaryObservation(temporaryBytes)
    censusObservation = value
  }
  def endCensus(value: MySqlModeEnvelopeV2Census.Partial): Unit = synchronized {
    require(censusObservation eq value, "exact original census observation")
    censusObservation = null
  }

  def reserve(amount: Long, context: String, retained: Boolean): Unit = synchronized {
    if amount < 0L then throw new SQLDataException(s"$context has a negative allocation size")
    val current = Math.addExact(retainedBytes, temporaryBytes)
    val next = Math.addExact(current, amount)
    if next > limitBytes then throw new SQLDataException(s"$context exceeds the shared explicit retained-memory budget")
    if retained then retainedBytes = Math.addExact(retainedBytes, amount)
    else temporaryBytes = Math.addExact(temporaryBytes, amount)
    successfulReserveHighWaterBytes = Math.max(successfulReserveHighWaterBytes, next)
    if censusObservation != null then censusObservation.observeTemporary(temporaryBytes)
  }

  def release(amount: Long, context: String, retained: Boolean): Unit = synchronized {
    if amount < 0L then throw new SQLDataException(s"$context has a negative release size")
    val current = if retained then retainedBytes else temporaryBytes
    if amount > current then throw new IllegalStateException(s"$context releases more bytes than were reserved")
    if retained then retainedBytes -= amount else temporaryBytes -= amount
  }

  def releaseAll(retained: Long, temporary: Long, context: String): Unit = synchronized {
    if retained < 0L || temporary < 0L || retained > retainedBytes || temporary > temporaryBytes then
      throw new IllegalStateException(s"$context cannot release allocations owned by this read scope")
    retainedBytes -= retained
    temporaryBytes -= temporary
  }

  def availableBytes: Long = synchronized {
    limitBytes - Math.addExact(retainedBytes, temporaryBytes)
  }

  def successfulReserveHighWater: Long = synchronized { successfulReserveHighWaterBytes }

  def accountedBytes: Long = synchronized {
    Math.addExact(retainedBytes, temporaryBytes)
  }

private[mysql] enum MySqlFormat3SnapshotLayout:
  case Format3Full20, ScriptPublication1Full24, ScriptPublication1DeploymentReceiptFull26, DbFirstFull27, DeploymentOriginalSingleRow, DeploymentAssembly

private[floatingpod] final class MySqlFormat3ReadBudget private (
    private[floatingpod] val profile: SourceLimitProfile,
    private var packetLimit: Long,
    private val allocationLedger: MySqlFormat3AllocationLedger,
    private val scopedReadBudget: Boolean,
    private val canonicalParent: Option[MySqlFormat3ReadBudget] = None,
    private val scopeKind: MySqlFormat3ReadBudget.ScopeKind = MySqlFormat3ReadBudget.ScopeKind.WorkflowRoot,
    private val initialLayout: MySqlFormat3SnapshotLayout = MySqlFormat3SnapshotLayout.Format3Full20,
    private val initialScriptInputs: Option[MySqlScriptSchemaInputs] = None,
    private val retainedImageOwner: Option[(AnyRef, Thread)] = None
):
  require(profile != null && packetLimit > 0L && allocationLedger != null,
    "format3 snapshot budgets must have an explicit profile, packet limit and allocation ledger")
  import MySqlFormat3ReadBudget.DeploymentState
  private var deploymentState: Option[DeploymentState] =
    if initialLayout == MySqlFormat3SnapshotLayout.DeploymentAssembly then Some(DeploymentState.Pending(null)) else None
  private def snapshotLayout: MySqlFormat3SnapshotLayout = deploymentState match
    case Some(DeploymentState.Bound(inputs)) if inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 =>
      MySqlFormat3SnapshotLayout.DbFirstFull27
    case Some(DeploymentState.Bound(_)) => MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26
    case _ => initialLayout
  private def scriptInputs: Option[MySqlScriptSchemaInputs] = deploymentState match
    case Some(DeploymentState.Bound(inputs)) => Some(inputs)
    case _ => initialScriptInputs
  private def requireDeploymentBound(): Unit = deploymentState match
    case Some(DeploymentState.Bound(_)) => ()
    case Some(DeploymentState.Failed(cause)) => throw new SQLNonTransientConnectionException("schema5 root binding failed; no retry", cause)
    case Some(_) => throw new SQLNonTransientConnectionException("schema5 assembly root has no live Full26 binding")
    case None => canonicalParent.foreach(_.requireDeploymentBound())
  private[mysql] def registerDeploymentAssemblyOwner(owner: MySqlRuntimeInputs.AssemblyOwner): Unit = this.synchronized {
    require(owner != null,"actual assembly owner required")
    owner.requireDeploymentAssemblyRoot(this)
    deploymentState match
      case Some(DeploymentState.Pending(null)) => deploymentState = Some(DeploymentState.Pending(owner))
      case None => () // legacy beginV2 retains its original domain
      case _ => throw new IllegalStateException("schema5 root already has an original assembly owner")
  }
  private[mysql] def bindDeploymentAssembly(owner: MySqlRuntimeInputs.AssemblyOwner,
      graph: MySqlRuntimeInputs.DeploymentReceiptStaticGraph, lease: MySqlRuntimeInputs.ConsumerLease,
      inputs: MySqlScriptSchemaInputs): Unit = this.synchronized {
    try
      ensureOpen("schema5 root bind")
      deploymentState match
        case Some(DeploymentState.Failed(original)) => throw original
        case Some(DeploymentState.Pending(original)) if original != null && (original eq owner) => ()
        case _ => throw new IllegalStateException("schema5 original Pending root/owner required; bind is one-shot")
      if Thread.currentThread() ne imageIssuerThread then throw new IllegalStateException("schema5 root bind requires original worker")
      if initialized || activeRow || originalImageObservation.nonEmpty || dataChildren != null || deploymentChildren != null ||
          hasActiveImmutableBorrows || borrowGroupCharged || scopedReadBudget then
        throw new IllegalStateException("schema5 root bind requires untouched layout/children/borrow state")
      owner.requireDeploymentRootBinding(this, graph, lease, inputs)
      deploymentState = Some(DeploymentState.Bound(inputs))
    catch
      case failure: Throwable =>
        deploymentState match
          case Some(DeploymentState.Pending(_)) => deploymentState = Some(DeploymentState.Failed(failure))
          case _ => () // a rejected second bind never replaces the original bound inputs
        throw failure
  }
  private[mysql] def bindDbFirstAssembly(owner: MySqlRuntimeInputs.AssemblyOwner,
      graph: MySqlRuntimeInputs.DbFirstStaticGraph, lease: MySqlRuntimeInputs.ConsumerLease,
      inputs: MySqlScriptSchemaInputs): Unit = this.synchronized {
    try
      ensureOpen("schema6 root bind")
      deploymentState match
        case Some(DeploymentState.Failed(original)) => throw original
        case Some(DeploymentState.Pending(original)) if original != null && (original eq owner) => ()
        case _ => throw new IllegalStateException("schema6 requires its original Pending root; binding is one-shot")
      if Thread.currentThread() ne imageIssuerThread then throw new IllegalStateException("schema6 root binding requires original worker")
      if initialized || activeRow || originalImageObservation.nonEmpty || dataChildren != null || deploymentChildren != null ||
          hasActiveImmutableBorrows || borrowGroupCharged || scopedReadBudget then
        throw new IllegalStateException("schema6 root bind requires untouched layout/children/borrow state")
      owner.requireDbFirstRootBinding(this, graph, lease, inputs)
      deploymentState = Some(DeploymentState.Bound(inputs))
    catch
      case failure: Throwable =>
        deploymentState match
          case Some(DeploymentState.Pending(_)) => deploymentState = Some(DeploymentState.Failed(failure))
          case _ => ()
        throw failure
  }

  private val exactScriptInputLayout = snapshotLayout match
    case MySqlFormat3SnapshotLayout.ScriptPublication1Full24 =>
      scriptInputs.exists(input => (input.sourceLimitProfile eq profile) &&
        input.dataLayout == MySqlScriptSchemaDataLayout.Full24)
    case MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 =>
      scriptInputs.exists(input => (input.sourceLimitProfile eq profile) &&
        input.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26)
    case MySqlFormat3SnapshotLayout.DbFirstFull27 =>
      scriptInputs.exists(input => (input.sourceLimitProfile eq profile) &&
        input.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27)
    case _ => false
  require(((snapshotLayout == MySqlFormat3SnapshotLayout.Format3Full20 ||
    snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow ||
    snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentAssembly) && scriptInputs.isEmpty) ||
    exactScriptInputLayout,
    "snapshot budget requires one exact closed data layout and its matching ScriptPublication1 inputs")
  private var canonicalBytes = 0L
  private var retainedMemoryBytes = 0L
  private var temporaryMemoryBytes = 0L
  private var currentPacketBytes = 0L
  private var activeRow = false
  private var initialized = false
  private var deploymentJdbcAttempted = false
  private var deploymentInitializationAttempted = false
  private var deploymentRowAttempted = false
  private var initializedObservation: Option[(String, MySqlFormat3ConnectionObservation, MySqlFormat3ObservationScope, Long)] = None
  private def requireOriginalSnapshotObservation(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, packet: Long): Unit =
    if !initializedObservation.contains((schema, connection, scope, packet)) then
      throw new SQLNonTransientConnectionException("initialized original root/session observation changed")

  private var closed = false
  // DATA子成员用既有budget对象链接；首次借用另收固定group和每成员128B，不建自动增长集合。
  private var dataChildren: MySqlFormat3ReadBudget = null
  private var dataNext: MySqlFormat3ReadBudget = null
  // Assembly and already-bound Full26 workflow roots retain live children even without RAW borrows.
  private var deploymentChildren: MySqlFormat3ReadBudget = null
  private var deploymentNext: MySqlFormat3ReadBudget = null
  private def deploymentTrackingRoot: Option[MySqlFormat3ReadBudget] =
    if deploymentState.nonEmpty || (!scopedReadBudget &&
      (snapshotLayout == MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 ||
        snapshotLayout == MySqlFormat3SnapshotLayout.DbFirstFull27)) then Some(this)
    else canonicalParent.flatMap(_.deploymentTrackingRoot)
  private def registerDeploymentChild(child: MySqlFormat3ReadBudget): Unit =
    deploymentTrackingRoot.foreach { parent =>
      parent.requireDeploymentBound()
      child.deploymentNext = parent.deploymentChildren
      parent.deploymentChildren = child
    }
  private def unlinkDeploymentChild(): Unit = canonicalParent.flatMap(_.deploymentTrackingRoot).foreach { parent =>
      var previous: MySqlFormat3ReadBudget = null
      var current = parent.deploymentChildren
      while current != null && (current ne this) do
        previous = current; current = current.deploymentNext
      if current != null then
        if previous == null then parent.deploymentChildren = deploymentNext else previous.deploymentNext = deploymentNext
      deploymentNext = null
  }
  private var borrowIncoming: MySqlFormat3BorrowEdge = null
  private var borrowOutgoing: MySqlFormat3BorrowEdge = null
  private var borrowAttempts: MySqlFormat3BorrowAttempt = null
  private var borrowGroupCharged = false
  private var borrowMemberCharged = false
  private var borrowThread: Thread = null
  private def borrowRoot: MySqlFormat3ReadBudget =
    if !scopedReadBudget then this
    else if scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalData then canonicalParent.get.borrowRoot
    else throw new IllegalStateException("snapshot/comparison scope cannot acquire immutable borrow custody")
  private def requireBorrowThread(): Unit =
    val actual = borrowRoot
    if actual.borrowThread != null && (actual.borrowThread ne Thread.currentThread()) then
      throw new IllegalStateException("immutable borrow belongs to the actual owner thread")
  private def prepareBorrowMember(): Unit =
    requireDeploymentBound()
    ensureOpen("immutable borrow member")
    val actual = borrowRoot
    actual.ensureOpen("immutable borrow root")
    requireBorrowThread()
    if !actual.borrowGroupCharged then
      actual.reserveRetainedMemory(4096L, "immutable borrow group fixed ownership fields")
      actual.borrowGroupCharged = true
      actual.borrowThread = Thread.currentThread()
    if !borrowMemberCharged then
      reserveRetainedMemory(128L, "immutable borrow member links and state")
      borrowMemberCharged = true
  private[mysql] def beginDecodeBorrow(source: MySqlFormat3CanonicalPayload): MySqlFormat3BorrowAttempt =
    rejectDeploymentSnapshotCapability()
    require(source != null && scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalData &&
      canonicalParent.exists(_ eq source.originalBudget) && (source.storageBudget.borrowRoot eq borrowRoot),
      "decode borrow requires its real canonical payload and exact registered DATA child")
    beginBorrowAttempt(source.storageBudget)
  private[mysql] def beginRawBorrow(): MySqlFormat3BorrowAttempt =
    rejectDeploymentSnapshotCapability()
    require(!scopedReadBudget, "historical RAW borrow belongs to the actual workflow root")
    beginBorrowAttempt(null)
  private def beginBorrowAttempt(source: MySqlFormat3ReadBudget): MySqlFormat3BorrowAttempt =
    prepareBorrowMember()
    reserveRetainedMemory(512L, "immutable borrow attempt custody BEFORE allocation")
    val attempt = new MySqlFormat3BorrowAttempt(this, source)
    attempt.next = borrowAttempts; borrowAttempts = attempt
    attempt
  private[mysql] def registerBorrow(source: MySqlFormat3Binary,
      attempt: MySqlFormat3BorrowAttempt): MySqlFormat3BorrowEdge =
    rejectDeploymentSnapshotCapability()
    if source != null && source.storageBudget != null then source.storageBudget.rejectDeploymentSnapshotCapability()
    require(attempt != null && (attempt.destination eq this) && attempt.failure == null &&
      !attempt.completed && !attempt.ended && attempt.pending == null, "registered live immutable borrow attempt required")
    requireBorrowThread()
    val origin = source.storageBudget
    require(origin != null && !origin.isClosed && (origin.borrowRoot eq borrowRoot) &&
      (attempt.decodeSource == null && !scopedReadBudget || (attempt.decodeSource eq origin)),
      "immutable borrow rejects unowned/foreign/sibling/closed source")
    prepareBorrowMember(); origin.prepareBorrowMember()
    reserveRetainedMemory(256L, "immutable dual-owner borrow edge BEFORE allocation")
    val edge = new MySqlFormat3BorrowEdge(source, origin, this, attempt)
    attempt.pending = edge
    edge.nextAttempt = attempt.edges; attempt.edges = edge
    // 任一侧登记失败仍保pending和已登记侧；不发布view，不退款。
    edge.nextIncoming = origin.borrowIncoming; origin.borrowIncoming = edge; edge.incomingRegistered = true
    ensureOpen("immutable borrow destination publication")
    edge.nextOutgoing = borrowOutgoing; borrowOutgoing = edge; edge.outgoingRegistered = true
    attempt.pending = null
    edge
  private def unlinkBorrow(edge: MySqlFormat3BorrowEdge): Unit =
    requireBorrowThread()
    if edge.ended then throw new IllegalStateException("immutable borrow edge already ended")
    if edge.incomingRegistered then
      var previous: MySqlFormat3BorrowEdge = null
      var current = edge.origin.borrowIncoming
      while current != null && (current ne edge) do
        previous = current; current = current.nextIncoming
      require(current eq edge, "actual incoming borrow registration required")
      if previous == null then edge.origin.borrowIncoming = edge.nextIncoming else previous.nextIncoming = edge.nextIncoming
    if edge.outgoingRegistered then
      var previous: MySqlFormat3BorrowEdge = null
      var current = borrowOutgoing
      while current != null && (current ne edge) do
        previous = current; current = current.nextOutgoing
      require(current eq edge, "actual outgoing borrow registration required")
      if previous == null then borrowOutgoing = edge.nextOutgoing else previous.nextOutgoing = edge.nextOutgoing
    edge.backing = null; edge.nextIncoming = null; edge.nextOutgoing = null
    edge.ended = true
  private def endAttempt(attempt: MySqlFormat3BorrowAttempt): Unit =
    var edge = attempt.edges
    while edge != null do
      val next = edge.nextAttempt
      unlinkBorrow(edge); edge.nextAttempt = null
      edge = next
    attempt.pending = null; attempt.edges = null; attempt.ended = true
  /** 只在原decode body或失败bundle构造已退出并撤局部结果后调用；成功cold持有者不得调用。 */
  private[mysql] def abandonDecodedData(): Unit =
    require(scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalData && scopedReadBudget,
      "only an abandoned canonical DATA result may settle its outgoing borrows")
    requireBorrowThread()
    require(borrowIncoming == null, "abandoned DATA still has external consumers")
    var attempt = borrowAttempts
    while attempt != null do
      if !attempt.ended then endAttempt(attempt)
      attempt = attempt.next
    closeScopedReadBudget()
  /** 仅registry在真实Main exact drop并撤整图引用后调用；先全核，再结束，最后由调用者checked close。 */
  private[mysql] def endProjectionBorrowGroup(): Unit =
    require(!scopedReadBudget, "exact projection workflow root required")
    requireBorrowThread()
    def validate(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        require(attempt.failure == null && attempt.pending == null && attempt.completed,
          "failed or unpublished immutable borrow attempt cannot acquire projection drop")
        attempt = attempt.next
      var edge = member.borrowIncoming
      while edge != null do
        require((edge.destination.borrowRoot eq this) && edge.outgoingRegistered && !edge.ended,
          "live external incoming borrow prevents projection release")
        edge = edge.nextIncoming
      edge = member.borrowOutgoing
      while edge != null do
        require((edge.origin.borrowRoot eq this) && edge.incomingRegistered && !edge.ended,
          "live external outgoing borrow prevents projection release")
        edge = edge.nextOutgoing
    validate(this)
    var child = dataChildren
    while child != null do
      validate(child); child = child.dataNext
    def end(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        if !attempt.ended then member.endAttempt(attempt)
        attempt = attempt.next
    end(this)
    child = dataChildren
    while child != null do
      end(child); child = child.dataNext
    child = dataChildren
    while child != null do
      val next = child.dataNext
      child.closeScopedReadBudget(); child = next
  /** 原Actor先证明最后consumer；全组预核成功后才进入Ending，不借用projection权限。 */
  private[mysql] def endCertifiedOrdinaryBorrowGroup(actor: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    actor.requireOriginalOrdinaryBorrowRetirement(this,state)
    if scopedReadBudget then throw new IllegalStateException("exact original Actor root required")
    requireBorrowThread()
    def validate(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        require(attempt.failure == null && attempt.pending == null && attempt.completed,
          "failed or unpublished immutable borrow attempt cannot acquire projection drop")
        attempt = attempt.next
      var edge = member.borrowIncoming
      while edge != null do
        require((edge.destination.borrowRoot eq this) && edge.outgoingRegistered && !edge.ended,
          "live external incoming borrow prevents projection release")
        edge = edge.nextIncoming
      edge = member.borrowOutgoing
      while edge != null do
        require((edge.origin.borrowRoot eq this) && edge.incomingRegistered && !edge.ended,
          "live external outgoing borrow prevents projection release")
        edge = edge.nextOutgoing
    validate(this)
    var child = dataChildren
    while child != null do
      validate(child); child = child.dataNext
    def end(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        if !attempt.ended then member.endAttempt(attempt)
        attempt = attempt.next
    actor.beginOriginalOrdinaryBorrowRetirement(this,state)
    end(this)
    child = dataChildren
    while child != null do
      end(child); child = child.dataNext
    child = dataChildren
    while child != null do
      val next = child.dataNext
      child.closeScopedReadBudget(); child = next
  /** 原Session保留Boot source到实际close；仅SQL/native末consumer已结束后核同原root组。 */
  private[mysql] def endCertifiedOrdinaryBootBorrowGroup(session: MySqlOwnedDatabaseSession,
      permit: MySqlCertifiedOrdinaryNativeExitPermit): Unit =
    session.requireOrdinaryBootBorrowRetirement(this,permit)
    if scopedReadBudget then throw new IllegalStateException("exact original Actor root required")
    requireBorrowThread()
    def validate(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        require(attempt.failure == null && attempt.pending == null && attempt.completed,
          "failed or unpublished immutable borrow attempt cannot acquire projection drop")
        attempt = attempt.next
      var edge = member.borrowIncoming
      while edge != null do
        require((edge.destination.borrowRoot eq this) && edge.outgoingRegistered && !edge.ended,
          "live external incoming borrow prevents projection release")
        edge = edge.nextIncoming
      edge = member.borrowOutgoing
      while edge != null do
        require((edge.origin.borrowRoot eq this) && edge.incomingRegistered && !edge.ended,
          "live external outgoing borrow prevents projection release")
        edge = edge.nextOutgoing
    validate(this)
    var child = dataChildren
    while child != null do
      validate(child); child = child.dataNext
    def end(member: MySqlFormat3ReadBudget): Unit =
      var attempt = member.borrowAttempts
      while attempt != null do
        if !attempt.ended then member.endAttempt(attempt)
        attempt = attempt.next
    end(this)
    child = dataChildren
    while child != null do
      end(child); child = child.dataNext
    child = dataChildren
    while child != null do
      val next = child.dataNext
      child.closeScopedReadBudget(); child = next
  private def requireNoBorrows(): Unit =
    require(borrowIncoming == null && borrowOutgoing == null,
      "immutable borrow still retains this storage owner; exact drop must precede close")
    var attempt = borrowAttempts
    while attempt != null do
      // 首edge前也可能已保partial RAW；没有edge不代表attempt责任已经结束。
      require(attempt.pending == null && attempt.rawConstruction == null &&
        (attempt.ended || (attempt.completed && attempt.failure == null && attempt.edges == null)),
        "partial immutable borrow custody prevents close")
      attempt = attempt.next
  private def unlinkDataMember(): Unit =
    if scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalData then
      val actual = borrowRoot
      var previous: MySqlFormat3ReadBudget = null
      var current = actual.dataChildren
      while current != null && (current ne this) do
        previous = current; current = current.dataNext
      // 构造失败可能发生在成员发布前；此时没有root引用可撤。
      if current != null then
        if previous == null then actual.dataChildren = dataNext else previous.dataNext = dataNext
      dataNext = null
  private[mysql] def retainedCanonicalDataChildCount: Int =
    var child = borrowRoot.dataChildren
    var count = 0
    while child != null do
      count = Math.addExact(count, 1); child = child.dataNext
    count
  private[mysql] def hasActiveImmutableBorrows: Boolean = borrowIncoming != null || borrowOutgoing != null ||
    { var current = borrowAttempts; var active = false
      while current != null && !active do
        active = current.pending != null || current.edges != null; current = current.next
      active }

  private[mysql] def this(profile: SourceLimitProfile, packetLimit: Long) =
    this(profile, packetLimit, new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()), false)

  private[mysql] def isScopedReadBudget: Boolean = scopedReadBudget

  private[floatingpod] def isClosed: Boolean = this.synchronized { closed }

  private[mysql] def isInitialized: Boolean = this.synchronized { initialized }

  private[mysql] def requireScriptPublication1Inputs(inputs: MySqlScriptSchemaInputs): Unit =
    requireDeploymentBound()
    if snapshotLayout != MySqlFormat3SnapshotLayout.ScriptPublication1Full24 || inputs == null ||
      inputs.dataLayout != MySqlScriptSchemaDataLayout.Full24 || !scriptInputs.exists(_ eq inputs) then
      throw new SQLNonTransientConnectionException("exact ScriptPublication1 full24 layout/input owner is required")

  /** New closed-layout DATA codecs share the ledger; legacy schema4 JDBC/B1 callers stay full24-only. */
  private var historicalPayloadInputs: MySqlScriptSchemaInputs = null
  private var historicalPayloadBorrow: MySqlDbFirstHistoryInputs.PayloadBorrow = null

  /** 仅原历史借用授权的短命比较子作用域可读取旧 DATA，不授予 SQL 或生产资格。 */
  private[mysql] def withHistoricalPayloadInputs(inputs: MySqlScriptSchemaInputs,
      borrow: MySqlDbFirstHistoryInputs.PayloadBorrow)(body: => Unit): Unit =
    val parent = canonicalParent.getOrElse(throw new IllegalStateException("historical DATA requires original comparison child"))
    ensureOpen("historical DATA scope")
    require(isCanonicalComparisonChildOf(parent) && !parent.isClosed &&
      parent.snapshotLayout == MySqlFormat3SnapshotLayout.DbFirstFull27 &&
      inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq profile) && borrow != null && historicalPayloadBorrow == null &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(parent) eq this),
      "exact lexical historical DATA comparison child required")
    borrow.requireInputs(parent, inputs)
    historicalPayloadInputs=inputs;historicalPayloadBorrow=borrow
    try body
    finally { historicalPayloadInputs=null;historicalPayloadBorrow=null }

  private[mysql] def requireHistoricalPayloadInputs(inputs: MySqlScriptSchemaInputs): Unit =
    ensureOpen("historical DATA inputs")
    if historicalPayloadBorrow == null then requireScriptDataInputs(inputs)
    else if historicalPayloadInputs eq inputs then
      val parent=canonicalParent.getOrElse(throw new IllegalStateException("historical DATA parent absent"))
      require(!parent.isClosed && isCanonicalComparisonChildOf(parent) &&
        (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(parent) eq this),
        "historical DATA requires active original comparison child")
      historicalPayloadBorrow.requireInputs(parent,inputs)
    else requireScriptDataInputs(inputs)

  private[mysql] def requireScriptDataInputs(inputs: MySqlScriptSchemaInputs): Unit =
    requireDeploymentBound()
    val layoutMatches = if inputs == null then false else snapshotLayout match
      case MySqlFormat3SnapshotLayout.ScriptPublication1Full24 => inputs.dataLayout == MySqlScriptSchemaDataLayout.Full24
      case MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 =>
        inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26
      case MySqlFormat3SnapshotLayout.DbFirstFull27 => inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27
      case _ => false
    if !layoutMatches || inputs == null || !(inputs.sourceLimitProfile eq profile) ||
      !scriptInputs.exists(_ eq inputs) then
      throw new SQLNonTransientConnectionException("exact closed ScriptPublication1 DATA layout/input owner is required")

  /** Native FirstBinding6 bridge validates the original bound root without exporting its private DATA input type. */
  private[floatingpod] def requireDbFirstNativeRoot(): Unit =
    ensureOpen("FirstBinding6 native root")
    val exact = scriptInputs.getOrElse(throw new SQLNonTransientConnectionException("FirstBinding6 native root has no bound DATA inputs"))
    if scopedReadBudget || snapshotLayout != MySqlFormat3SnapshotLayout.DbFirstFull27 ||
        scopeKind != MySqlFormat3ReadBudget.ScopeKind.WorkflowRoot || exact.canonicalOrder.size != 27 ||
        (imageIssuerThread ne Thread.currentThread()) then
      throw new SQLNonTransientConnectionException("FirstBinding6 native work requires its original bound Full27 workflow root/worker")
    requireScriptDataInputs(exact)

  private[mysql] def requireFormat3Full20Layout(): Unit =
    requireDeploymentBound()
    if snapshotLayout != MySqlFormat3SnapshotLayout.Format3Full20 || scriptInputs.nonEmpty then
      throw new SQLNonTransientConnectionException("exact format3 full20 layout owner is required")

  private def rejectDeploymentSnapshotCapability(): Unit =
    requireDeploymentBound()
    if snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow then
      throw new SQLNonTransientConnectionException("deployment-original root cannot acquire snapshot, DATA or immutable borrow capability")

  private[mysql] def requireDeploymentOriginalReadLayout(): Unit =
    ensureOpen("deployment-original read layout")
    if snapshotLayout != MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow || scopedReadBudget ||
      scopeKind != MySqlFormat3ReadBudget.ScopeKind.WorkflowRoot || scriptInputs.nonEmpty then
      throw new SQLNonTransientConnectionException("exact deployment-original workflow root required")
    if imageIssuerThread == null || (imageIssuerThread ne Thread.currentThread()) || retainedImageOwner.isEmpty then
      throw new SQLNonTransientConnectionException("deployment-original read requires its actual bound worker")
    if imageIssuerFailure != null then throw imageIssuerFailure

  private[mysql] def claimDeploymentOriginalJdbcAttempt(): Unit = this.synchronized {
    requireDeploymentOriginalReadLayout()
    if deploymentJdbcAttempted || deploymentInitializationAttempted || initialized then
      throw new SQLNonTransientConnectionException("deployment-original JDBC attempt cannot replay")
    deploymentJdbcAttempted = true
  }

  private[mysql] def recordDeploymentOriginalJdbcFailure(primary: Throwable): Unit = this.synchronized {
    ensureOpen("deployment-original JDBC failure")
    if snapshotLayout != MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow || scopedReadBudget ||
      scopeKind != MySqlFormat3ReadBudget.ScopeKind.WorkflowRoot || scriptInputs.nonEmpty ||
      imageIssuerThread == null || (imageIssuerThread ne Thread.currentThread()) ||
      !retainedImageOwner.exists { (_, worker) => worker eq Thread.currentThread() } ||
      !deploymentJdbcAttempted || primary == null then
      throw new SQLNonTransientConnectionException("deployment-original JDBC failure requires its claimed actual worker root and primary cause")
    if imageIssuerFailure == null then imageIssuerFailure = primary
  }

  private def isSafeDeploymentSchema(schema: String): Boolean =
    if schema == null || schema.isEmpty || schema.length > 64 then return false
    var index = 0
    while index < schema.length do
      val value = schema.charAt(index)
      if !(value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
        value >= '0' && value <= '9' || value == '_') then return false
      index += 1
    true

  private[mysql] def initializeDeploymentOriginalRead(schema: String,
      connection: MySqlFormat3ConnectionObservation, scope: MySqlFormat3ObservationScope,
      actualMaxPacket: Long): Unit = this.synchronized {
    requireDeploymentOriginalReadLayout()
    if deploymentInitializationAttempted || initialized then
      throw new SQLNonTransientConnectionException("deployment-original initialization cannot replay")
    if !isSafeDeploymentSchema(schema) || connection == null ||
      connection.catalog != schema || scope == null || actualMaxPacket <= 0L then
      throw new SQLDataException("deployment-original read requires its exact safe schema/session observation and positive packet limit")
    deploymentInitializationAttempted = true
    try
      reserveRetainedMemory(256L, "deployment-original fixed observation representation")
      packetLimit = Math.min(packetLimit, Math.min(profile.maxSqlPacketBytes(), actualMaxPacket))
      initializedObservation = Some((schema, connection, scope, actualMaxPacket))
      initialized = true
    catch
      case failure: Throwable =>
        imageIssuerFailure = failure
        throw failure
  }

  private[mysql] def requireInitializedObservation(schema: String,
      connection: MySqlFormat3ConnectionObservation, scope: MySqlFormat3ObservationScope, packet: Long): Unit =
    this.synchronized {
      ensureInitialized("existing ScriptPublication1 DATA root observation")
      requireOriginalSnapshotObservation(schema, connection, scope, packet)
    }

  /** 只核对调用方已持有的同一账本，不暴露或构造替代账本/root。 */
  private[mysql] def requireLedgerIdentity(expected: MySqlFormat3AllocationLedger): Unit =
    ensureOpen("exact workflow ledger identity")
    if scopedReadBudget || expected == null || (allocationLedger ne expected) then
      throw new IllegalArgumentException("exact workflow root allocation ledger required")

  private[mysql] def sharesAllocationLedger(other: MySqlFormat3ReadBudget): Boolean =
    other != null && (allocationLedger eq other.allocationLedger)

  /** Release a root workflow budget only after its exact Main projection is no longer retained. */
  private[mysql] def closeRootReadBudget(): Unit = this.synchronized {
    if scopedReadBudget then throw new IllegalStateException("a scoped format3 read budget cannot be closed as a workflow root")
    if !closed then
      if activeRow then throw new IllegalStateException("format3 workflow budget cannot close during an active row")
      if deploymentChildren != null then throw new IllegalStateException("schema5 root still owns live snapshot/DATA/comparison children")
      requireNoBorrows()
      var child = dataChildren
      while child != null do
        child.requireNoBorrows(); child = child.dataNext
      allocationLedger.releaseAll(retainedMemoryBytes, temporaryMemoryBytes, "format3 workflow root budget")
      retainedMemoryBytes = 0L
      temporaryMemoryBytes = 0L
      currentPacketBytes = 0L
      closed = true
      if deploymentState.nonEmpty then deploymentState = Some(DeploymentState.Closed)
  }

  /** 临时canonical comparison同cap计费；它不发行snapshot/SQL资格，也不需要假initialize。 */
  private[mysql] def newCanonicalScope(): MySqlFormat3ReadBudget = this.synchronized {
    requireDeploymentBound()
    ensureOpen("retained canonical comparison budget")
    if scopedReadBudget || activeRow then throw new IllegalStateException("canonical comparison requires an idle workflow root")
    val child = new MySqlFormat3ReadBudget(profile, packetLimit, allocationLedger, true, Some(this),
      MySqlFormat3ReadBudget.ScopeKind.CanonicalComparison, snapshotLayout, scriptInputs)
    registerDeploymentChild(child)
    child
  }

  private[mysql] def isCanonicalChildOf(parent: MySqlFormat3ReadBudget): Boolean =
    parent != null && canonicalParent.exists(_ eq parent)

  private[mysql] def isSnapshotReadChildOf(parent: MySqlFormat3ReadBudget): Boolean =
    scopeKind == MySqlFormat3ReadBudget.ScopeKind.SnapshotRead && isCanonicalChildOf(parent)

  private[mysql] def isCanonicalDataChildOf(parent: MySqlFormat3ReadBudget): Boolean =
    scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalData && isCanonicalChildOf(parent)

  private[mysql] def isCanonicalComparisonChildOf(parent: MySqlFormat3ReadBudget): Boolean =
    scopeKind == MySqlFormat3ReadBudget.ScopeKind.CanonicalComparison && isCanonicalChildOf(parent)

  /** Cold codec 的 DATA scope 只共享现有账本；不测量 DB、不发行 read-owner/admission。 */
  private[mysql] def newCanonicalDataScope(): MySqlFormat3ReadBudget = this.synchronized {
    rejectDeploymentSnapshotCapability()
    ensureOpen("canonical DATA owner budget")
    if activeRow then throw new IllegalStateException("canonical DATA scope cannot interrupt an active row")
    reserveTemporaryMemory(256L, "canonical DATA scope construction")
    var child: MySqlFormat3ReadBudget = null
    var primary: Throwable = null
    try
      child = new MySqlFormat3ReadBudget(profile, packetLimit, allocationLedger, true, Some(this),
        MySqlFormat3ReadBudget.ScopeKind.CanonicalData, snapshotLayout, scriptInputs)
      registerDeploymentChild(child) // reserve/cleanup失败也仍由原root持有partial child
      child.reserveRetainedMemory(256L, "canonical DATA scope retained metadata")
      val actual = borrowRoot
      child.dataNext = actual.dataChildren; actual.dataChildren = child
      child
    catch
      case failure: Throwable =>
        primary = failure
        if child != null then
          try child.closeScopedReadBudget()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => releaseTemporaryMemory(256L, "canonical DATA scope construction")))
  }

  /** 文件 header 只收紧 DATA child 的限额，不声称它是本次 DB 测量。 */
  private[mysql] def bindCanonicalDataPacketLimit(recordedPacket: Long): Unit = this.synchronized {
    ensureOpen("canonical DATA packet bound")
    if !scopedReadBudget || canonicalParent.isEmpty || activeRow || initialized || recordedPacket <= 0L then
      throw new IllegalStateException("canonical DATA packet bound requires an idle uninitialized child")
    packetLimit = Math.min(packetLimit, Math.min(profile.maxSqlPacketBytes(), recordedPacket))
  }

  /** 首次真实 planning read 用实际 session 值收紧原 root，不重建 allocator/释放计划。 */
  private[mysql] def bindInitialMeasuredPacketLimit(sessionMaxPacket: Long): Unit = this.synchronized {
    requireDeploymentBound()
    ensureOpen("initial planning root packet observation")
    if scopedReadBudget || activeRow || initialized then
      throw new SQLNonTransientConnectionException("initial packet observation requires original uninitialized root")
    if sessionMaxPacket <= 0L then
      throw new SQLDataException("actual MySQL session packet limit must be positive")
    packetLimit = Math.min(packetLimit, Math.min(profile.maxSqlPacketBytes(), sessionMaxPacket))
  }

  private var imageIssuerThread: Thread = if retainedImageOwner.isEmpty then Thread.currentThread() else null
  private var originalImageObservation: Option[(String, MySqlFormat3ConnectionObservation, MySqlFormat3ObservationScope, Long)] = None
  private var imageIssuerFailure: Throwable = null

  /** 原core在实际worker首次执行时消费私有identity；不转移账本或发行SQL资格。 */
  private[mysql] def bindRetainedImageWorker(capability: AnyRef): Unit = this.synchronized {
    ensureOpen("retained image worker handoff")
    if snapshotLayout != MySqlFormat3SnapshotLayout.Format3Full20 &&
      snapshotLayout != MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow &&
      snapshotLayout != MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 &&
      snapshotLayout != MySqlFormat3SnapshotLayout.DbFirstFull27 then
      throw new SQLNonTransientConnectionException("retained worker handoff requires a closed full20, Full26, Full27 or deployment-original layout")
    if !retainedImageOwner.exists { (identity, worker) =>
        (identity eq capability) && (worker eq Thread.currentThread())
      } || imageIssuerThread != null || scopedReadBudget || activeRow || initialized ||
        originalImageObservation.nonEmpty || imageIssuerFailure != null then
      throw new SQLNonTransientConnectionException("original idle retained root requires its exact one-shot worker handoff")
    imageIssuerThread = Thread.currentThread()
  }

  /** 原 obligation root只绑定一次实际session；每次image另发同ledger child。不是SQL资格issuer。 */
  private[mysql] def issueOwnedSnapshotImage(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, actualPacket: Long): MySqlFormat3ReadBudget = this.synchronized {
    requireFormat3Full20Layout()
    issueSnapshotImage(schema, connection, scope, actualPacket)
  }

  private[mysql] def issueScriptSchemaSnapshotImage(inputs: MySqlScriptSchemaInputs,
      schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, actualPacket: Long): MySqlFormat3ReadBudget = this.synchronized {
    requireScriptPublication1Inputs(inputs)
    issueSnapshotImage(schema, connection, scope, actualPacket)
  }

  /** schema5 专用 image issuer；旧 full24 入口的接受域保持不变。 */
  private[mysql] def issueDeploymentReceiptSnapshotImage(inputs: MySqlScriptSchemaInputs,
      schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, actualPacket: Long): MySqlFormat3ReadBudget = this.synchronized {
    requireScriptDataInputs(inputs)
    if snapshotLayout != MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 ||
        inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
        schema != inputs.schema || connection == null || connection.catalog != schema ||
        scope == null || actualPacket <= 0L then
      throw new SQLNonTransientConnectionException("exact schema5 Full26 session observation/root required")
    issueSnapshotImage(schema, connection, scope, actualPacket)
  }

  private[mysql] def issueDbFirstSnapshotImage(inputs: MySqlScriptSchemaInputs,
      schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, actualPacket: Long,
      holder: MySqlDbFirstSnapshotJdbc): MySqlFormat3ReadBudget = this.synchronized {
    requireScriptDataInputs(inputs)
    if snapshotLayout != MySqlFormat3SnapshotLayout.DbFirstFull27 ||
      inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 || schema != inputs.schema ||
      connection == null || connection.catalog != schema || scope == null || actualPacket <= 0L || holder == null then
      throw new SQLNonTransientConnectionException("exact schema6 caller-retained Full27 observation/root required")
    holder.requireImageIssuer(this,inputs)
    issueSnapshotImage(schema,connection,scope,actualPacket,Some(holder))
  }

  /** 已登记fault inspection的独立终端image；普通issuer的固定身份/首因不读取、覆盖或重置。 */
  private[mysql] def issueDbFirstInspectionSnapshotImage(inputs:MySqlScriptSchemaInputs,
      schema:String,connection:MySqlFormat3ConnectionObservation,scope:MySqlFormat3ObservationScope,
      actualPacket:Long,holder:MySqlDbFirstSnapshotJdbc):MySqlFormat3ReadBudget = this.synchronized {
    require(holder!=null,"actual inspection snapshot reader required")
    holder.requireInspectionImageIssuer(this,inputs,connection,scope,actualPacket)
    issueDbFirstRegisteredSnapshotImage(inputs,schema,connection,scope,actualPacket,holder)
  }

  private[mysql] def issueDbFirstServiceSnapshotImage(inputs:MySqlScriptSchemaInputs,
      schema:String,connection:MySqlFormat3ConnectionObservation,scope:MySqlFormat3ObservationScope,
      actualPacket:Long,holder:MySqlDbFirstSnapshotJdbc):MySqlFormat3ReadBudget = this.synchronized {
    require(holder!=null,"actual service snapshot reader required")
    holder.requireServiceImageIssuer(this,inputs,connection,scope,actualPacket)
    issueDbFirstRegisteredSnapshotImage(inputs,schema,connection,scope,actualPacket,holder)
  }

  /** 仅供上述两个已核实际owner的入口；不接收caller提供的代际许可或重绑原issuer。 */
  private def issueDbFirstRegisteredSnapshotImage(inputs:MySqlScriptSchemaInputs,
      schema:String,connection:MySqlFormat3ConnectionObservation,scope:MySqlFormat3ObservationScope,
      actualPacket:Long,holder:MySqlDbFirstSnapshotJdbc):MySqlFormat3ReadBudget =
    requireScriptDataInputs(inputs)
    ensureOpen("actual registered schema6 image")
    require(snapshotLayout==MySqlFormat3SnapshotLayout.DbFirstFull27 &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 && schema==inputs.schema &&
      connection!=null && connection.catalog==schema && scope!=null && actualPacket>0L && holder!=null,
      "actual schema6 inspection image inputs/connection/scope/packet/holder required")
    if (Thread.currentThread() ne imageIssuerThread) || scopedReadBudget || activeRow || !initialized then
      throw new SQLNonTransientConnectionException("inspection image requires original initialized allocation root/worker")
    var child:MySqlFormat3ReadBudget=null
    var callerRetained=false
    try
      child=newScopedReadBudget(Math.min(packetLimit,actualPacket))
      holder.retainIssuedImage(this,child)
      callerRetained=true // 此后initialize失败由实际reader/Session保管；不污染普通issuer或丢partial image。
      child.initialize(schema,connection,scope,actualPacket)
      child
    catch case cause:Throwable=>
      if child!=null && !callerRetained then
        try child.closeScopedReadBudget()
        catch case cleanup:Throwable=>if cleanup ne cause then cause.addSuppressed(cleanup)
      throw cause

  private def issueSnapshotImage(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, actualPacket: Long,
      dbFirstHolder: Option[MySqlDbFirstSnapshotJdbc] = None): MySqlFormat3ReadBudget =
    ensureOpen("owned snapshot image issuer")
    if (Thread.currentThread() ne imageIssuerThread) || scopedReadBudget || activeRow then
      throw new SQLNonTransientConnectionException("actual root owner/thread is required for each image")
    if imageIssuerFailure != null then
      throw new SQLNonTransientConnectionException("original image issuer failure remains retained", imageIssuerFailure)
    var child: MySqlFormat3ReadBudget = null
    var callerRetained=false
    try
      val actual = (schema, connection, scope, actualPacket)
      originalImageObservation match
        case Some(original) =>
          if original != actual then throw new SQLNonTransientConnectionException("original actor session/image observation changed")
        case None =>
          if !initialized then
            bindInitialMeasuredPacketLimit(actualPacket)
            initialize(schema, connection, scope, actualPacket)
          else
            // Existing planning roots already carry actual initialized metadata; do not reinitialize them.
            requireOriginalSnapshotObservation(schema, connection, scope, actualPacket)
          originalImageObservation = Some(actual)
      child = newScopedReadBudget(Math.min(packetLimit, actualPacket))
      dbFirstHolder.foreach { holder =>
        holder.retainIssuedImage(this,child)
        callerRetained=true
      }
      child.initialize(schema, connection, scope, actualPacket)
      child
    catch
      case primary: Throwable =>
        imageIssuerFailure = primary
        if child != null && !callerRetained then
          try child.closeScopedReadBudget()
          catch
            case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
        throw primary

  /** Each actual full20 read gets a child scope; it shares the workflow cap but keeps its own packet/canonical counters. */
  private[mysql] def newScopedReadBudget(effectivePacketLimit: Long): MySqlFormat3ReadBudget = this.synchronized {
    rejectDeploymentSnapshotCapability()
    ensureOpen("format3 workflow allocation budget")
    if scopedReadBudget || activeRow || initialized == false || effectivePacketLimit <= 0L ||
      effectivePacketLimit > profile.maxSqlPacketBytes() then
      throw new SQLNonTransientConnectionException("format3 child read requires an initialized open workflow budget and bounded packet limit")
    val child = new MySqlFormat3ReadBudget(profile, effectivePacketLimit, allocationLedger, true, Some(this),
      MySqlFormat3ReadBudget.ScopeKind.SnapshotRead, snapshotLayout, scriptInputs)
    registerDeploymentChild(child)
    child
  }

  private[mysql] def closeScopedReadBudget(): Unit = this.synchronized {
    if !scopedReadBudget then throw new IllegalStateException("only a scoped format3 read budget can be closed")
    if !closed then
      requireNoBorrows()
      allocationLedger.releaseAll(retainedMemoryBytes, temporaryMemoryBytes, "format3 scoped read budget")
      retainedMemoryBytes = 0L
      temporaryMemoryBytes = 0L
      currentPacketBytes = 0L
      activeRow = false
      unlinkDataMember()
      unlinkDeploymentChild()
      closed = true
  }

  private[mysql] def initialize(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, maxAllowedPacket: Long): Unit =
    rejectDeploymentSnapshotCapability()
    ensureOpen("format3 snapshot budget")
    if snapshotLayout == MySqlFormat3SnapshotLayout.DbFirstFull27 && retainedImageOwner.nonEmpty &&
      (imageIssuerThread == null || (Thread.currentThread() ne imageIssuerThread)) then
      throw new SQLNonTransientConnectionException("retained Full27 snapshot requires its actual bound worker")
    if initialized then throw new IllegalStateException("format3 snapshot budget was initialized more than once")
    snapshotLayout match
      case MySqlFormat3SnapshotLayout.Format3Full20 =>
        reserveRetainedMemory(8_192L, "format3 fixed snapshot structures")
        val header = MySqlFormat3SnapshotCanonical.fixedHeaderBytes(schema, connection, scope, maxAllowedPacket, profile, this)
        reserveCanonicalBytes(header, "format3 snapshot canonical header")
        initializedObservation = Some((schema, connection, scope, maxAllowedPacket))
        initialized = true
      case MySqlFormat3SnapshotLayout.ScriptPublication1Full24 =>
        initializeScriptPublication1(schema, connection, scope, maxAllowedPacket)
      case MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 =>
        initializeScriptPublication1(schema, connection, scope, maxAllowedPacket)
      case MySqlFormat3SnapshotLayout.DbFirstFull27 =>
        initializeScriptPublication1(schema, connection, scope, maxAllowedPacket)
      case MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow =>
        throw new SQLNonTransientConnectionException("deployment-original root cannot initialize a full snapshot")
      case MySqlFormat3SnapshotLayout.DeploymentAssembly =>
        throw new SQLNonTransientConnectionException("pending schema5 root cannot initialize a snapshot")

  private def initializeScriptPublication1(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, maxAllowedPacket: Long): Unit =
    val inputs = scriptInputs.getOrElse(throw new IllegalStateException("ScriptPublication1 snapshot budget lost its frozen inputs"))
    if schema != inputs.schema then throw new SQLDataException("ScriptPublication1 snapshot budget schema differs from its fixed profile")
    if connection == null || connection.catalog != schema || scope == null || maxAllowedPacket <= 0L then
      throw new SQLDataException("ScriptPublication1 DATA root requires its exact schema/session observation")
    var fixedStructuresReserved = false
    var canonicalHeaderReserved = 0L
    var primary: Throwable = null
    try
      reserveRetainedMemory(8_192L, "ScriptPublication1 fixed snapshot structures")
      fixedStructuresReserved = true
      val header = MySqlScriptSchemaSnapshotCanonical.fixedHeaderBytes(schema, connection, scope, maxAllowedPacket, inputs, this)
      reserveCanonicalBytes(header, "ScriptPublication1 snapshot canonical header")
      canonicalHeaderReserved = header
      initializedObservation = Some((schema, connection, scope, maxAllowedPacket))
      initialized = true
    catch
      case failure: Throwable =>
        primary = failure
        initialized = false
        initializedObservation = None
        throw failure
    finally
      if primary != null then
        val cleanup = Vector.newBuilder[() => Unit]
        if canonicalHeaderReserved > 0L then cleanup += (() => releaseCanonicalBytes(canonicalHeaderReserved,
          "failed ScriptPublication1 canonical header cleanup"))
        if fixedStructuresReserved then cleanup += (() => releaseRetainedMemory(8_192L,
          "failed ScriptPublication1 fixed snapshot structures cleanup"))
        MySqlFormat3BudgetCleanup.run(primary, cleanup.result())

  private var scriptRows = 0L

  private[mysql] def beginRow(columnCount: Int, table: String, rowNumber: Long): Unit =
    if snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow then
      beginDeploymentOriginalRow(columnCount, table, rowNumber)
      return
    ensureInitialized(s"$table row $rowNumber")
    if activeRow then throw new IllegalStateException("format3 row budget was not closed")
    scriptInputs.foreach { inputs =>
      if scriptRows >= inputs.budget.rows then
        throw new SQLDataException(s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} total row budget exceeded before cell materialization")
      scriptRows = Math.addExact(scriptRows, 1L)
    }
    if columnCount < 0 then throw new SQLDataException(s"$table row $rowNumber has an invalid column count")
    reserveCanonicalBytes(4L, s"$table row $rowNumber column-count field")
    // 固定余量覆盖行与向量节点、单元包装对象及有界构建器扩展。
    val rowRepresentation = Math.addExact(256L, Math.multiplyExact(columnCount.toLong, 64L))
    reserveRetainedMemory(rowRepresentation, s"$table row $rowNumber typed representation")
    // 固定余量覆盖二进制行头/空值位图，或文本行内每列一个 SQL NULL 标记。
    currentPacketBytes = Math.addExact(9L, columnCount.toLong)
    if currentPacketBytes > packetLimit then throw new SQLDataException(s"$table row $rowNumber exceeds the explicit MySQL row-packet budget")
    activeRow = true

  private def beginDeploymentOriginalRow(columnCount: Int, table: String, rowNumber: Long): Unit = this.synchronized {
    requireDeploymentOriginalReadLayout()
    ensureInitialized("deployment-original row")
    if deploymentRowAttempted || activeRow || table != "fp_deployment_operation" || columnCount != 31 || rowNumber != 0L then
      throw new SQLDataException("deployment-original read permits exactly row zero of the fixed 31-column relation")
    deploymentRowAttempted = true
    try
      reserveCanonicalBytes(4L, "deployment-original column-count field")
      reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(columnCount.toLong, 64L)),
        "deployment-original row typed representation")
      currentPacketBytes = Math.addExact(9L, columnCount.toLong)
      if currentPacketBytes > packetLimit then
        throw new SQLDataException("deployment-original row exceeds the explicit MySQL row-packet budget")
      activeRow = true
    catch
      case failure: Throwable =>
        imageIssuerFailure = failure
        throw failure
  }

  private[mysql] def endRow(table: String, rowNumber: Long): Unit =
    if snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow then requireDeploymentOriginalReadLayout()
    ensureOpen(s"$table row $rowNumber")
    if !activeRow then throw new SQLDataException(s"$table row $rowNumber ended without an active row")
    activeRow = false
    currentPacketBytes = 0L

  private[mysql] def beginNullCell(context: String): Unit =
    ensureActive(context)
    reserveCanonicalBytes(1L, context)

  private[mysql] def beginVariableCell(context: String): Unit =
    ensureActive(context)
    reserveCanonicalBytes(5L, context)
    addPacket(9L, context)

  private[mysql] def beginFixedCell(canonicalWidth: Int, packetWidth: Int, context: String): Unit =
    ensureActive(context)
    if canonicalWidth < 0 || packetWidth < 0 then throw new SQLDataException(s"$context has an invalid fixed width")
    reserveCanonicalBytes(Math.addExact(1L, canonicalWidth.toLong), context)
    addPacket(packetWidth.toLong, context)

  private[mysql] def addVariablePayload(amount: Long, context: String): Unit =
    ensureActive(context)
    if amount < 0L then throw new SQLDataException(s"$context has a negative payload length")
    reserveCanonicalBytes(amount, context)
    addPacket(amount, context)

  private[mysql] def variablePayloadRemaining: Long = this.synchronized {
    ensureOpen("format3 variable payload")
    if !activeRow then throw new IllegalStateException("format3 payload is outside a row")
    Math.min(Math.min(profile.maxRetainedBytes() - canonicalBytes, packetLimit - currentPacketBytes),
      allocationLedger.availableBytes)
  }

  private[floatingpod] def reserveRetainedMemory(amount: Long, context: String): Unit =
    reserveMemory(amount, context, retained = true)

  private[floatingpod] def releaseRetainedMemory(amount: Long, context: String): Unit =
    releaseMemory(amount, context, retained = true)

  private[floatingpod] def reserveTemporaryMemory(amount: Long, context: String): Unit =
    reserveMemory(amount, context, retained = false)

  private[floatingpod] def releaseTemporaryMemory(amount: Long, context: String): Unit =
    releaseMemory(amount, context, retained = false)

  private[mysql] def beginCensusObservation(value: MySqlModeEnvelopeV2Census.Partial): Unit =
    ensureOpen("original census observation")
    require(value != null && (value.root eq this)); value.check()
    allocationLedger.beginCensus(value)
  private[mysql] def endCensusObservation(value: MySqlModeEnvelopeV2Census.Partial): Unit =
    require(value != null && (value.root eq this)); value.check()
    allocationLedger.endCensus(value)

  private[mysql] def availableMemoryBytes: Long = allocationLedger.availableBytes

  private[mysql] def accountedMemoryBytes: Long = allocationLedger.accountedBytes

  /** 实际成功预留的峰值；不增加容量、不签阶段/SQL资格。 */
  private[mysql] def successfulReserveHighWaterBytes: Long =
    ensureOpen("actual successful-reserve high-water observation")
    allocationLedger.successfulReserveHighWater

  /** 直接构造的模型样例须先为已存在的不可变值登记预算，再计算规范摘要。 */
  private[mysql] def recordMaterializedSnapshot(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    rejectDeploymentSnapshotCapability()
    ensureOpen("format3 materialized snapshot budget")
    if relations == null then throw new SQLDataException("format3 prebuilt snapshot relations are missing")
    if snapshotLayout == MySqlFormat3SnapshotLayout.ScriptPublication1Full24 ||
      snapshotLayout == MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26 ||
      snapshotLayout == MySqlFormat3SnapshotLayout.DbFirstFull27 then
      recordMaterializedScriptPublication1Snapshot(relations)
      return
    relations.foreach { relation =>
      relation.rows.foreach { row =>
        reserveRetainedMemory(Math.addExact(256L, Math.multiplyExact(row.size.toLong, 64L)), s"${relation.table} prebuilt row representation")
        row.zip(MySqlFormat3RelationShapes.forTable(relation.table).columns).foreach { (cell, column) =>
          cell match
            case MySqlFormat3Cell.Text(value) =>
              reserveRetainedMemory(Math.addExact(64L, Math.max(MySqlFormat3Utf8.byteLength(value), Math.multiplyExact(value.length.toLong, 2L))), s"${relation.table}.${column.name} prebuilt String")
            case MySqlFormat3Cell.Binary(value) =>
              reserveRetainedMemory(value.length.toLong, s"${relation.table}.${column.name} prebuilt binary")
            case _ => ()
        }
      }
    }

  private def recordMaterializedScriptPublication1Snapshot(relations: Vector[MySqlFormat3RelationSnapshot]): Unit =
    val inputs = scriptInputs.getOrElse(throw new IllegalStateException("ScriptPublication1 snapshot budget lost its frozen inputs"))
    if initializedObservation.isEmpty then
      throw new SQLNonTransientConnectionException("ScriptPublication1 materialized snapshot requires an initialized original root")
    reserveTemporaryMemory(MySqlScriptSchemaInputs.ValidationWorkspace,
      s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} materialized snapshot accounting workspace")
    var reserved = 0L
    var primary: Throwable = null
    try
      relations.foreach { relation =>
        val shape = inputs.forTable(relation.table)
        relation.rows.foreach { row =>
          val rowCharge = Math.addExact(256L, Math.multiplyExact(row.size.toLong, 64L))
          val rowTotal = Math.addExact(reserved, rowCharge)
          reserveRetainedMemory(rowCharge,
            s"${relation.table} ${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} prebuilt row representation")
          reserved = rowTotal
          var columnIndex = 0
          while columnIndex < shape.columns.size do
            val cell = row(columnIndex)
            val column = shape.columns(columnIndex)
            val charge = cell match
              case MySqlFormat3Cell.Text(value) =>
                Math.addExact(64L, Math.max(MySqlFormat3Utf8.byteLength(value), Math.multiplyExact(value.length.toLong, 2L)))
              case MySqlFormat3Cell.Binary(value) => Math.addExact(value.length.toLong, 64L)
              case _ => 0L
            if charge > 0L then
              val cellTotal = Math.addExact(reserved, charge)
              reserveRetainedMemory(charge,
                s"${relation.table}.${column.name} ${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} prebuilt cell")
              reserved = cellTotal
            columnIndex += 1
        }
      }
    catch
      case failure: Throwable =>
        primary = failure
        if reserved > 0L then
          try releaseRetainedMemory(reserved,
            s"failed ${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} materialized snapshot cleanup")
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => releaseTemporaryMemory(MySqlScriptSchemaInputs.ValidationWorkspace,
          s"${MySqlScriptSchemaInputs.familyName(inputs.dataLayout)} materialized snapshot accounting workspace")))

  private def ensureOpen(context: String): Unit =
    if closed then throw new IllegalStateException(s"$context is already closed")

  private def ensureInitialized(context: String): Unit =
    if snapshotLayout == MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow then requireDeploymentOriginalReadLayout()
    ensureOpen(context)
    if !initialized then throw new IllegalStateException(s"$context uses an uninitialized format3 budget")

  private def ensureActive(context: String): Unit =
    ensureInitialized(context)
    if !activeRow then throw new SQLDataException(s"$context is outside a format3 row")

  private def reserveCanonicalBytes(amount: Long, context: String): Unit = this.synchronized {
    ensureOpen(context)
    if amount < 0L then throw new SQLDataException(s"$context has a negative retained-byte count")
    val next = Math.addExact(canonicalBytes, amount)
    if next > profile.maxRetainedBytes() then throw new SQLDataException(s"$context exceeds explicit max_retained_bytes")
    canonicalBytes = next
  }

  private def releaseCanonicalBytes(amount: Long, context: String): Unit = this.synchronized {
    if amount < 0L || amount > canonicalBytes then throw new IllegalStateException(s"$context cannot release unreserved canonical bytes")
    canonicalBytes -= amount
  }

  private def reserveMemory(amount: Long, context: String, retained: Boolean): Unit = this.synchronized {
    ensureOpen(context)
    if amount < 0L then throw new SQLDataException(s"$context has a negative allocation size")
    val current = Math.addExact(retainedMemoryBytes, temporaryMemoryBytes)
    val next = Math.addExact(current, amount)
    if next > profile.maxRetainedBytes() then throw new SQLDataException(s"$context exceeds explicit retained-memory budget")
    allocationLedger.reserve(amount, context, retained)
    if retained then retainedMemoryBytes = Math.addExact(retainedMemoryBytes, amount)
    else temporaryMemoryBytes = Math.addExact(temporaryMemoryBytes, amount)
  }

  private def releaseMemory(amount: Long, context: String, retained: Boolean): Unit = this.synchronized {
    ensureOpen(context)
    if amount < 0L then throw new SQLDataException(s"$context has a negative release size")
    val current = if retained then retainedMemoryBytes else temporaryMemoryBytes
    if amount > current then throw new IllegalStateException(s"$context releases more bytes than were reserved")
    allocationLedger.release(amount, context, retained)
    if retained then retainedMemoryBytes -= amount else temporaryMemoryBytes -= amount
  }

  private def addPacket(amount: Long, context: String): Unit =
    ensureOpen(context)
    currentPacketBytes = Math.addExact(currentPacketBytes, amount)
    if currentPacketBytes > packetLimit then throw new SQLDataException(s"$context exceeds the explicit MySQL row-packet budget")
private[mysql] final class MySqlFormat3SnapshotReadException(table: String, stage: String, cause: Throwable)
    extends SQLException(s"MySQL format3 relation $table failed during $stage", cause)

private[mysql] final class MySqlFormat3CellReadException(table: String, column: String, row: Long, cause: Throwable)
    extends SQLDataException(s"MySQL format3 relation $table row $row column $column could not be decoded", cause)

private[mysql] final class MySqlFormat3SnapshotCleanupUncertainException(cause: Throwable)
    extends SQLNonTransientConnectionException("MySQL format3 snapshot rollback or session restoration failed; retire the caller-owned connection", cause)

/** Worker session roots share one actual aggregate cap, while retaining their own row/packet counters. */
private[floatingpod] object MySqlFormat3ReadBudget:
  private enum DeploymentState:
    case Pending(owner: MySqlRuntimeInputs.AssemblyOwner)
    case Bound(inputs: MySqlScriptSchemaInputs)
    case Failed(cause: Throwable)
    case Closed
  private enum ScopeKind:
    case WorkflowRoot, SnapshotRead, CanonicalComparison, CanonicalData

  private[mysql] def rootOnLedger(profile: SourceLimitProfile, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    require(profile != null && ledger != null && packetLimit > 0L && packetLimit <= profile.maxSqlPacketBytes(),
      "explicit shared-root profile, packet limit and allocation ledger required")
    new MySqlFormat3ReadBudget(profile, packetLimit, ledger, false)

  private[mysql] def deploymentAssemblyRootOnLedger(profile: SourceLimitProfile, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    require(profile != null && ledger != null && packetLimit > 0L && packetLimit <= profile.maxSqlPacketBytes(),
      "actual schema5 assembly profile/packet/ledger required")
    new MySqlFormat3ReadBudget(profile, packetLimit, ledger, false,
      initialLayout = MySqlFormat3SnapshotLayout.DeploymentAssembly)

  /** Main只预留内存；实际core的worker入口才可绑定image发行权。identity不反向引用core。 */
  private[mysql] def retainedRootOnLedger(profile: SourceLimitProfile, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger, capability: AnyRef, worker: Thread): MySqlFormat3ReadBudget =
    require(profile != null && ledger != null && packetLimit > 0L && packetLimit <= profile.maxSqlPacketBytes() &&
      capability != null && worker != null && (worker ne Thread.currentThread()),
      "actual retained root profile, ledger and private worker identity required")
    new MySqlFormat3ReadBudget(profile, packetLimit, ledger, false,
      retainedImageOwner = Some((capability, worker)))

  private[mysql] def retainedDeploymentOriginalRootOnLedger(profile: SourceLimitProfile, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger, capability: AnyRef, worker: Thread): MySqlFormat3ReadBudget =
    require(profile != null && ledger != null && packetLimit > 0L && packetLimit <= profile.maxSqlPacketBytes() &&
      capability != null && worker != null && (worker ne Thread.currentThread()),
      "actual deployment-original retained root profile, ledger and private worker identity required")
    new MySqlFormat3ReadBudget(profile, packetLimit, ledger, false, None,
      ScopeKind.WorkflowRoot, MySqlFormat3SnapshotLayout.DeploymentOriginalSingleRow, None,
      Some((capability, worker)))

  /** Main只保留原Session的完整输入引用；实际image仍由原core单次移交给worker。 */
  private[mysql] def retainedDeploymentReceiptRootOnLedger(inputs: MySqlScriptSchemaInputs,packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger,capability: AnyRef,worker: Thread): MySqlFormat3ReadBudget =
    require(inputs!=null && ledger!=null && capability!=null && worker!=null &&
      (worker ne Thread.currentThread()) && inputs.dataLayout==MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      packetLimit>0L && packetLimit<=inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit<=inputs.budget.packetBytes,
      "actual retained Full26 inputs, packet bound, shared ledger and private worker identity required")
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile,packetLimit,ledger,false,None,
      ScopeKind.WorkflowRoot,MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26,Some(inputs),
      Some((capability,worker)))

  /** Full27原输入与账本保持独立家族；原core绑定worker前不能发行image。 */
  private[mysql] def retainedDbFirstRootOnLedger(inputs: MySqlScriptSchemaInputs, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger, capability: AnyRef, worker: Thread): MySqlFormat3ReadBudget =
    require(inputs != null && ledger != null && capability != null && worker != null &&
      (worker ne Thread.currentThread()) && inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      packetLimit > 0L && packetLimit <= inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit <= inputs.budget.packetBytes,
      "actual retained Full27 inputs, packet bound, shared ledger and private worker identity required")
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile, packetLimit, ledger, false, None,
      ScopeKind.WorkflowRoot, MySqlFormat3SnapshotLayout.DbFirstFull27, Some(inputs), Some((capability, worker)))

  /** Boot worker只可在原core正在构造该intent时分配；原Main工厂的异线程gate不放宽。 */
  private[mysql] def retainedBootTakeoverRootOnWorker(inputs:MySqlScriptSchemaInputs,packetLimit:Long,
      ledger:MySqlFormat3AllocationLedger,capability:AnyRef,worker:Thread,core:MySqlRetainedPlanReservationCore,
      intent:MySqlDbFirstBootRecallTakeoverIntent):MySqlFormat3ReadBudget =
    require(inputs!=null && ledger!=null && capability!=null && worker!=null && (worker eq Thread.currentThread()) &&
      core!=null && inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 && packetLimit>0L &&
      packetLimit<=inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit<=inputs.budget.packetBytes,
      "actual Boot worker Full27 inputs and bounded shared ledger required")
    core.requireBootTakeoverRootConstruction(intent,inputs,ledger,capability,worker)
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile,packetLimit,ledger,false,None,
      ScopeKind.WorkflowRoot,MySqlFormat3SnapshotLayout.DbFirstFull27,Some(inputs),Some((capability,worker)))

  /** schema4 DATA root 共享既有账本，但不能作为 format3/full20 root 构造。 */
  private[mysql] def scriptPublication1Root(inputs: MySqlScriptSchemaInputs, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    require(inputs != null && ledger != null && packetLimit > 0L &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.Full24 &&
      packetLimit <= inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit <= inputs.budget.packetBytes,
      "explicit ScriptPublication1 full24 inputs, packet budget and allocation ledger required")
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile, packetLimit, ledger, false, None,
      ScopeKind.WorkflowRoot, MySqlFormat3SnapshotLayout.ScriptPublication1Full24, Some(inputs))

  /** schema5 full26 DATA root remains on the caller's exact original shared ledger. */
  private[mysql] def scriptPublication1DeploymentReceiptRoot(inputs: MySqlScriptSchemaInputs, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    require(inputs != null && ledger != null && packetLimit > 0L &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      packetLimit <= inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit <= inputs.budget.packetBytes,
      "explicit ScriptPublication1 deployment-receipt full26 inputs, packet budget and allocation ledger required")
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile, packetLimit, ledger, false, None,
      ScopeKind.WorkflowRoot, MySqlFormat3SnapshotLayout.ScriptPublication1DeploymentReceiptFull26, Some(inputs))


  /** Full27 DATA 根与全部 child 使用调用方实际 shared ledger；不发行 SQL 或 Session 资格。 */
  private[mysql] def dbFirstRoot(inputs: MySqlScriptSchemaInputs, packetLimit: Long,
      ledger: MySqlFormat3AllocationLedger): MySqlFormat3ReadBudget =
    require(inputs != null && ledger != null && packetLimit > 0L &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      packetLimit <= inputs.sourceLimitProfile.maxSqlPacketBytes() && packetLimit <= inputs.budget.packetBytes,
      "explicit Full27 inputs, packet budget and shared ledger required")
    new MySqlFormat3ReadBudget(inputs.sourceLimitProfile, packetLimit, ledger, false, None,
      ScopeKind.WorkflowRoot, MySqlFormat3SnapshotLayout.DbFirstFull27, Some(inputs))

/** 每个实际attempt512B；先登记到真实budget，故部分配置/失败不依赖constructor局部变量。 */
private[mysql] final class MySqlFormat3BorrowAttempt private[mysql] (
    val destination: MySqlFormat3ReadBudget, val decodeSource: MySqlFormat3ReadBudget):
  private[mysql] var next: MySqlFormat3BorrowAttempt = null
  private[mysql] var edges: MySqlFormat3BorrowEdge = null
  private[mysql] var pending: MySqlFormat3BorrowEdge = null
  private[mysql] var completed = false
  private[mysql] var ended = false
  private[mysql] var failure: Throwable = null
  private[mysql] var rawConstruction: MySqlRecoveryCanonicalBytes.RawBorrowConstruction = null
  def recordFailure(cause: Throwable): Unit =
    if failure == null then failure = cause
    else if failure ne cause then MySqlOwnedSessionLifecycle.suppress(failure, cause)
  def complete(): Unit =
    require(failure == null && pending == null && !completed && !ended)
    completed = true

/** 固定256B双侧链接；不保mutable array，不把同ledger等同同owner。 */
private[mysql] final class MySqlFormat3BorrowEdge private[mysql] (
    private[mysql] var backing: MySqlFormat3Binary,
    private[mysql] val origin: MySqlFormat3ReadBudget,
    private[mysql] val destination: MySqlFormat3ReadBudget,
    private[mysql] val attempt: MySqlFormat3BorrowAttempt):
  private[mysql] var nextIncoming: MySqlFormat3BorrowEdge = null
  private[mysql] var nextOutgoing: MySqlFormat3BorrowEdge = null
  private[mysql] var nextAttempt: MySqlFormat3BorrowEdge = null
  private[mysql] var incomingRegistered = false
  private[mysql] var outgoingRegistered = false
  private[mysql] var ended = false
  def requireLive(): Unit =
    require(!ended && incomingRegistered && outgoingRegistered && backing != null &&
      !origin.isClosed && !destination.isClosed, "immutable borrowed bytes outlived their actual storage responsibility")
