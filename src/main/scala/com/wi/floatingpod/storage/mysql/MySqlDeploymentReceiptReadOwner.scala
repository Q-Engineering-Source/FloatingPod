package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec
import java.sql.{Connection, ResultSet, ResultSetMetaData, SQLDataException, Types}
import java.time.LocalDateTime

/** 原 Session 登记的完整锁图 owner；事务、snapshot child 与失败均留在同一 holder。 */
private[mysql] final class MySqlDeploymentReceiptReadOwner private (
    session: MySqlDeploymentReceiptSession, actor: MySqlDeploymentReceiptBootstrapActor,
    verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
    expectation: MySqlSchemaAuditExpectation, inputs: MySqlScriptSchemaInputs,
    root: MySqlFormat3ReadBudget,
    private[mysql] val unknownInspectionOwner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5
):
  private var attempted = false
  private var gate: MySqlConnectionTransactionGate.Owner = null
  private var rollbackOnly = new MySqlRollbackOnlyState
  private var critical: MySqlDeploymentReceiptSession.LockedReadScope5 = null
  private var oldIsolation = Option.empty[Int]
  private var transactionAttempted = false
  private var gateEntryAttempted = false
  private var gateEntryReturned = false
  private var gateEntrySucceeded = false
  private var gateEntryRejected = false
  private var gateOwnerReturned = false
  private var restrictedSourceAckReadback:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5=null
  private var image: MySqlFormat3ReadBudget = null
  private var snapshot: MySqlScriptSchemaSnapshot = null
  private var primary: Throwable = null
  private var cleanupAttempted = false
  private var cleanupSettled = false
  private val settlement = new MySqlDeploymentReceiptReadOwner.ReadSettlement
  private var writerFacts: MySqlBusinessWriterRow = null
  private var databaseNow: LocalDateTime = null
  private var terminalRetirementAttempted=false
  private var holderRetired=false
  private var projectionOwnsImage=false
  private var snapshotChildCloseReturned=false
  private var snapshotReferencesCleared=false
  private var snapshotRetirementFailure:Throwable=null
  private[mysql] var restrictedSourceSnapshotRetirementReturned=false
  private[mysql] var restrictedSourceHolderRefundAttempted=false
  private[mysql] var restrictedSourceHolderRefundReturned=false
  private[mysql] var restrictedSourceRegistryRemovalReturned=false
  private[mysql] var readOwnerRegistryRemovalReturned=false
  private[mysql] var restrictedSourceRetirementFailure:Throwable=null

  /** 成功只读交易的结算与 image 退休分离；B 的 before/body 仍由原 child 持有。 */
  def settleSuccessfulRead(actualActor: MySqlDeploymentReceiptBootstrapActor): Unit =
    requireCurrent()
    if (actualActor ne actor) || snapshot == null || critical != null then throw new IllegalStateException("original independently owned Full26 read required")
    endReadTransaction()

  /** ACK 读回采集失败后，仅结算归属仍可确认的自有事务；保留登记 reader、部分 image 与全部留存计费。 */
  private[mysql] def settleFailedRestrictedSourceAckReadback(
      actualSession:MySqlDeploymentReceiptSession,
      actualActor:MySqlDeploymentReceiptBootstrapActor,
      actualLease:MySqlDeploymentReceiptSession.RestrictedSourceReadLease5,
      actualRoot:MySqlFormat3ReadBudget,
      original:Throwable):Boolean =
    if original==null then throw new IllegalArgumentException("original restricted SOURCE R0 ACK readback failure required")
    if (actualActor ne actor) || (actualRoot ne root) || (scope.ownerThread ne Thread.currentThread()) || critical!=null then
      throw new IllegalStateException("exact original Session/actor/root and independent ACK reader required",original)
    session.requireRestrictedSourceReader(actualLease,this)
    requireRestrictedSourceRetirementIdentity(actualSession,actualActor,actualRoot)
    if transactionSettled then return true
    if settlement.isUncertain then return false
    if transactionAttempted then
      if !gateEntryAttempted || !gateEntryReturned || !gateEntrySucceeded || gateEntryRejected ||
          !gateOwnerReturned || gate==null then return false
      try
        endReadTransaction()
        true
      catch
        case cleanup:Throwable=>
          MySqlOwnedIoAdapter.suppressExitCleanup(original,cleanup)
          false
    else if !gateEntryAttempted then
      !gateEntryReturned && !gateEntrySucceeded && !gateEntryRejected && !gateOwnerReturned && gate==null
    else if !gateEntryReturned then false
    else if gateEntryRejected then
      !gateEntrySucceeded && !gateOwnerReturned && gate==null
    else if !gateEntrySucceeded || !gateOwnerReturned || gate==null then false
    else
      try
        MySqlConnectionTransactionGate.leave(verified.connection,rollbackOnly)
        gate=null
        true
      catch
        case cleanup:Throwable=>
          MySqlOwnedIoAdapter.suppressExitCleanup(original,cleanup)
          false

  private def endReadTransaction(): Unit =
    if settlement.isSettled then return
    if settlement.isUncertain then throw settlement.failure.get
    val c=verified.connection
    try
      c.rollback()
      settlement.rollbackReturned()
      settlement.requireRestorable()
      c.setAutoCommit(true)
      if !c.getAutoCommit then throw new IllegalStateException("Full26 read end autocommit not restored")
      oldIsolation.foreach { value =>
        c.setTransactionIsolation(value)
        if c.getTransactionIsolation != value then throw new IllegalStateException("Full26 read end isolation not restored")
      }
      MySqlConnectionTransactionGate.leave(c,rollbackOnly)
      gate=null
      settlement.restorationReturned()
    catch case failure: Throwable =>
      settlement.failed(failure)
      if primary == null then primary=failure else MySqlOwnedIoAdapter.suppressExitCleanup(primary,failure)
      rollbackOnly.mark(failure)
      throw failure // no restoration or clean gate drop after rollback/restore uncertainty

  def withRetainedBefore[A](actualActor: MySqlDeploymentReceiptBootstrapActor)
      (consume: (MySqlScriptSchemaSnapshot, MySqlBusinessWriterRow) => A): A =
    session.requireReadOwner(this,actualActor)
    if (actualActor ne actor) || !transactionSettled || snapshot == null || writerFacts == null || image.isClosed then
      throw new IllegalStateException("settled original Full26 before/writer image required",primary)
    root.requireScriptDataInputs(inputs)
    consume(snapshot,writerFacts)

  private[mysql] def ownsRetainedImage(actual:MySqlScriptSchemaSnapshot):Boolean =
    snapshot!=null && (snapshot eq actual) && transactionSettled && image!=null && !image.isClosed

  def retireSnapshot(actualActor: MySqlDeploymentReceiptBootstrapActor): Unit =
    session.requireReadOwner(this,actualActor)
    if (actualActor ne actor) || !transactionSettled then throw new IllegalStateException("Full26 read transaction must settle before snapshot retirement",primary)
    if snapshotRetirementFailure!=null then throw snapshotRetirementFailure
    if !snapshotChildCloseReturned then
      try
        if image!=null then image.closeScopedReadBudget()
        snapshotChildCloseReturned=true
      catch
        case cause:Throwable=>
          snapshotRetirementFailure=cause
          throw cause
    if !snapshotReferencesCleared then
      snapshot=null;writerFacts=null;image=null
      snapshotReferencesCleared=true

  /** 即使两个Session registry已移除此reader，退休tombstone仍核对构造时绑定的owner身份。 */
  private[mysql] def requireRestrictedSourceRetirementIdentity(
      actualSession:MySqlDeploymentReceiptSession,actualActor:MySqlDeploymentReceiptBootstrapActor,
      actualRoot:MySqlFormat3ReadBudget):Unit =
    if (actualSession ne session) || (actualActor ne actor) || (actualRoot ne root) then
      throw new IllegalStateException("restricted source reader retirement belongs to another original Session/actor/root",primary)

  private[mysql] def retireForTerminal(intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    session.requireTerminalReader(this,actor,intent)
    if terminalRetirementAttempted || cleanupAttempted || !transactionSettled || settlement.isUncertain || primary!=null then
      throw new IllegalStateException("settled original Full26 terminal reader required",primary)
    terminalRetirementAttempted=true
    if image!=null then
      image.closeScopedReadBudget()
      snapshot=null;writerFacts=null;image=null
    root.releaseRetainedMemory(1024L,"Full26 original terminal read holder retired")
    holderRetired=true

  /** 原 operation 结束前撤掉 reader 的真实 child 和 holder，之后才能回 ACK 释放 plan root。 */
  private[mysql] def retireForPurpose(actualActor:MySqlDeploymentReceiptBootstrapActor,
      actualRoot:MySqlFormat3ReadBudget):Unit =
    session.requirePurposeReader(this,actualActor,actualRoot)
    if (actualActor ne actor) || (actualRoot ne root) || terminalRetirementAttempted || cleanupAttempted ||
        holderRetired || !transactionSettled || settlement.isUncertain || primary!=null then
      throw new IllegalStateException("settled original Full26 purpose reader/root required",primary)
    if image!=null then
      image.closeScopedReadBudget()
      snapshot=null;writerFacts=null;image=null
    root.releaseRetainedMemory(1024L,"Full26 original purpose read holder retired before plan ACK")
    holderRetired=true

  private[mysql] def purposeImageBudget(actualActor:MySqlDeploymentReceiptBootstrapActor,
      actualRoot:MySqlFormat3ReadBudget):MySqlFormat3ReadBudget =
    session.requirePurposeReader(this,actualActor,actualRoot)
    if (actualRoot ne root) || image==null || !transactionSettled || holderRetired || primary!=null ||
        !image.isSnapshotReadChildOf(root) || image.isClosed then
      throw new IllegalStateException("settled original Full26 purpose image child required",primary)
    image

  /** 原 projection registry 已实际接管同一 child；这里只撤本 reader 引用与 holder，不退 cell 费用。 */
  private[mysql] def detachPurposeImage(actualActor:MySqlDeploymentReceiptBootstrapActor,
      actualRoot:MySqlFormat3ReadBudget,transferredChild:MySqlFormat3ReadBudget):Unit =
    if purposeImageBudget(actualActor,actualRoot) ne transferredChild then
      throw new IllegalStateException("exact transferred Full26 purpose child required")
    projectionOwnsImage=true
    root.releaseRetainedMemory(1024L,"Full26 purpose holder retired after original registry acquired child")
    holderRetired=true
    snapshot=null;writerFacts=null;image=null

  /** 初始化失败的只读事务收口；不产生 terminal/native/admission 证明。 */
  def abortInitialization(original: Throwable): Boolean =
    if Thread.currentThread() ne scope.ownerThread then throw new IllegalStateException("exact Full26 cleanup worker required")
    if session.hasPendingRestrictedSourceFreshWritePreparation(this) then return false
    critical match
      case restricted:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5 if !restricted.readerRetirementComplete =>
        return false // scope 的 Session 仍负责结算和退休；rollback 返回不表示 holder 已可释放。
      case _=>()
    if cleanupAttempted then return cleanupSettled
    cleanupAttempted = true
    if projectionOwnsImage then return false // 原 registry 已取得 child，失败路径不能再由 reader 关闭或退费。
    if critical != null && !critical.isSettled then return false
    var settled = true
    def close(action: => Unit): Unit =
      try action
      catch case failure: Throwable =>
        settled = false
        MySqlOwnedSessionLifecycle.suppress(original, failure)
    val c = verified.connection
    if transactionAttempted then close(endReadTransaction())
    else if gate != null && critical == null then close { MySqlConnectionTransactionGate.leave(c,rollbackOnly);gate=null }
    if settlement.isUncertain then return false // physical connection close remains Session responsibility; retain gate/image/root
    if image != null then close { image.closeScopedReadBudget(); image=null;snapshot=null;writerFacts=null }
    if settled then close(root.releaseRetainedMemory(1024L, "Full26 read holder detached after failed initialization"))
    cleanupSettled = settled
    settled

  private def requireCurrent(): Unit =
    session.requireReadOwner(this, actor)
    root.requireScriptDataInputs(inputs)
    if inputs.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 ||
        expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1 ||
        scope.ownerThread != Thread.currentThread() then throw new IllegalStateException("exact schema5 Full26 owner required")
    if unknownInspectionOwner!=null then
      unknownInspectionOwner.requireReader(this)
    else if restrictedSourceAckReadback!=null then
      val readback=restrictedSourceAckReadback
      session.requireAcknowledgedRestrictedSourceR0Readback(readback.plan,readback.outcome,
        readback.plan.issuer,readback)
    else
      critical match
        case restricted:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5 =>
          restricted.readmitCurrentBarrier()
        case _ => verified.readmitSessionBarrier(expectation, scope)
    if gate != null then MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollbackOnly)
    if primary != null then throw primary
    if transactionSettled then throw new IllegalStateException("ended Full26 read cannot assert current SQL lock authority")
    if critical != null then critical.requireFor(session,actor,verified)

  private def transactionSettled: Boolean = settlement.isSettled || (critical != null && critical.isSettled)
  private[mysql] def isSettled: Boolean = transactionSettled
  private[mysql] def primaryFailure: Option[Throwable] = Option(primary)
  private[mysql] def verifiedConnection:VerifiedMySqlConnection = verified
  private[mysql] def auditScope:MySqlAuditBootScope = scope

  def captureLockedFull26(write: Option[MySqlDeploymentReceiptSession.CriticalWriteScope] = None): Unit =
    captureInScope(write)

  private[mysql] def capturePurposeLockedFull26(write:MySqlDeploymentReceiptSession.PurposeReadScope5):Unit =
    if write==null then throw new IllegalArgumentException("actual Session purpose read scope required")
    captureInScope(Some(write))

  /** 事务 gate 进入前先登记 scope，使无事务失败也由同一 reader/scope 收口。 */
  private[mysql] def registerRestrictedSourceFreshWriteScope(
      write:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if write==null || !write.isBoundReader(this) || attempted || critical!=null || primary!=null ||
        transactionAttempted || gate!=null then
      throw new IllegalStateException("exact unstarted restricted fresh-write reader/scope registration required",primary)
    session.requireReadOwner(this,actor)
    critical=write
    rollbackOnly=write.rollbackOnly

  /** 独立 post-DML reader role；同一 Session scope/gate，不重开 before reader 的一次性 image。 */
  private[mysql] def registerRestrictedSourceFreshTargetReader(
      write:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    if write==null || !write.isBoundTargetReader(this) || attempted || critical!=null || primary!=null ||
        transactionAttempted || gate!=null then
      throw new IllegalStateException("exact unstarted registered restricted fresh-target reader/scope required",primary)
    session.requireReadOwner(this,actor)
    critical=write
    rollbackOnly=write.rollbackOnly

  /** 仅 Session 签发的精确 ACK role 可将此 reader 路由到已预付的 readmit guard。 */
  private[mysql] def registerRestrictedSourceAckReadback(
      readback:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5):Unit =
    if readback==null || (readback.session ne session) || (readback.reader ne this) || !readback.attemptStarted ||
        !readback.isAttemptedState(readback.outcome.postCommitReadback) || restrictedSourceAckReadback!=null ||
        attempted || critical!=null || primary!=null || gate!=null || gateEntryAttempted || transactionAttempted then
      throw new IllegalStateException("exact unstarted Session-issued restricted SOURCE ACK readback reader required",primary)
    session.requireRestrictedSourceReader(readback.readerLease,this)
    restrictedSourceAckReadback=readback

  /** Session guard 只接受精确 role 的空闲入口或已确认 RR gate 活跃状态。 */
  private[mysql] def hasCurrentRestrictedSourceAckReadbackConnectionState(
      readback:MySqlDeploymentReceiptSession.RestrictedSourceR0PostCommitReadback5,
      originalIsolation:Int):Boolean =
    if readback==null || (restrictedSourceAckReadback ne readback) || (readback.reader ne this) then false
    else
      val c=verified.connection
      val idle=c.getAutoCommit && c.getTransactionIsolation==originalIsolation
      if transactionSettled then idle
      else if !attempted && !gateEntryAttempted && !gateEntryReturned && !transactionAttempted && gate==null then idle
      else if gateEntryAttempted && gateEntryReturned && gateEntrySucceeded && !gateEntryRejected &&
          gateOwnerReturned && gate!=null && transactionAttempted && !settlement.isSettled &&
          !settlement.isUncertain && oldIsolation.contains(originalIsolation) then
        !c.getAutoCommit && c.getTransactionIsolation==Connection.TRANSACTION_REPEATABLE_READ
      else false

  /** restricted SOURCE 只可借用 Session 私有签发的 fresh-write scope。 */
  private[mysql] def captureRestrictedSourceLockedFull26(
      write:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5):Unit =
    val workspacePrecharged=write!=null &&
      (if write.isBoundReader(this) then write.captureWorkspacePrecharged
       else if write.isBoundTargetReader(this) then write.targetCaptureWorkspacePrecharged
       else false)
    if write==null || (critical ne write) || !workspacePrecharged then
      throw new IllegalArgumentException("precharged original restricted fresh-write scope required")
    gate=write.gate
    captureInScope(Some(write))

  private def captureInScope(write:Option[MySqlDeploymentReceiptSession.LockedReadScope5]):Unit =
    requireCurrent()
    if attempted then throw new IllegalStateException("Full26 lock/capture is one-shot", primary)
    attempted = true
    val c = verified.connection
    val borrowedRestrictedWorkspace=write.exists {
      case scope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5 => scope.captureWorkspacePrecharged
      case _ => false
    }
    if !borrowedRestrictedWorkspace then root.reserveTemporaryMemory(8192L, "Full26 transaction/root-lock/control workspace BEFORE allocation")
    try
      write match
        case Some(actual) =>
          actual.requireFor(session,actor,verified)
          critical=actual
          rollbackOnly=actual.rollbackOnly
          gate=actual.gate
        case None =>
          if !c.getAutoCommit then throw new SQLDataException("Full26 requires an idle original connection before transaction")
          gateEntryAttempted=true
          val gateEntry=MySqlConnectionTransactionGate.enter(c,rollbackOnly)
          gateEntryReturned=true
          gateEntry match
            case Some(rejection)=>
              gateEntryRejected=true
              throw rejection
            case None=>
              gateEntrySucceeded=true
              val returnedGate=MySqlConnectionTransactionGate.currentOwner(c,rollbackOnly)
              if returnedGate==null then throw new IllegalStateException("Full26 gate owner was not returned after exact gate entry")
              gate=returnedGate
              gateOwnerReturned=true
          oldIsolation = Some(c.getTransactionIsolation)
          transactionAttempted = true
          c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
          c.setAutoCommit(false)
      if c.getAutoCommit || c.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLDataException("Full26 original RR transaction could not be established")
      val packetSql = "SELECT @@SESSION.max_allowed_packet"
      packetLength(MySqlFormat3FullSnapshotSql.utf8Length(packetSql) + 16L, inputs.budget.packetBytes)
      val packet = MySqlBusinessRows.using(c.prepareStatement(packetSql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { rs =>
          if !rs.next() then throw new SQLDataException("Full26 session packet observation missing")
          val value = rs.getLong(1)
          if rs.wasNull() || value <= 0L || rs.next() then throw new SQLDataException("Full26 packet observation is not one positive value")
          value
        }
      }
      Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer").foreach { table =>
        requireCurrent()
        val sql = MySqlDeploymentReceiptReadOwner.rootLockSql(inputs.schema, table)
        packetLength(MySqlFormat3FullSnapshotSql.utf8Length(sql) + 16L, packet)
        MySqlBusinessRows.using(c.prepareStatement(sql)) { statement =>
          MySqlBusinessRows.using(statement.executeQuery()) { rs =>
            val present = rs.next()
            if !present && table != "physical_recovery_binding" then throw new SQLDataException(s"Full26 required root $table missing")
            if present && (rs.getLong(1) != 1L || rs.wasNull() || rs.next()) then throw new SQLDataException(s"Full26 root $table singleton differs")
          }
        }
      }
      val nowSql="SELECT UTC_TIMESTAMP(6)"
      packetLength(MySqlFormat3FullSnapshotSql.utf8Length(nowSql)+16L,packet)
      databaseNow=MySqlBusinessRows.using(c.prepareStatement(nowSql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { rs =>
          if !rs.next() then throw new SQLDataException("Full26 locked writer database time missing")
          val now=rs.getObject(1,classOf[LocalDateTime])
          if now == null || now.getNano%1000 != 0 || rs.next() then throw new SQLDataException("Full26 locked writer database time differs")
          now
        }
      }
      val connectionObservation=MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ")
      val observationScope=MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration)
      image = if unknownInspectionOwner==null then
        root.issueDeploymentReceiptSnapshotImage(inputs, inputs.schema, connectionObservation, observationScope, packet)
      else
        unknownInspectionOwner.prechargeSnapshotImage(this)
        root.reserveTemporaryMemory(512L, "UNKNOWN Full26 snapshot-child construction workspace BEFORE allocation")
        var allocationFailure:Throwable=null
        try
          val child=root.newScopedReadBudget(Math.min(root.profile.maxSqlPacketBytes(),packet))
          unknownInspectionOwner.bindSnapshotImage(this,child)
          child.initialize(inputs.schema,connectionObservation,observationScope,packet)
          child
        catch
          case cause:Throwable =>
            allocationFailure=cause
            throw cause
        finally
          MySqlFormat3BudgetCleanup.run(allocationFailure,Vector(() =>
            root.releaseTemporaryMemory(512L,"UNKNOWN Full26 snapshot-child construction workspace released")))
      val relations = inputs.canonicalOrder.map(shape => readRelation(shape, packet))
      snapshot = MySqlScriptSchemaSnapshot.decoded(inputs.schema,
        MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ"),
        MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration),
        packet, inputs, relations, image)
      snapshot.canonicalSha256
      writerFacts=decodeWriter(snapshot,databaseNow)
      requireCurrent()
    catch
      case failure: Throwable =>
        val restrictedFreshnessRejection=write.exists {
          case scope:MySqlDeploymentReceiptSession.RestrictedSourceFreshWriteScope5 =>
            scope.isStaleRejection(failure)
          case _=>false
        }
        if !restrictedFreshnessRejection then primary = failure
        rollbackOnly.mark(failure)
        throw failure
    finally
      if !borrowedRestrictedWorkspace then MySqlFormat3BudgetCleanup.run(primary, Vector(() =>
        root.releaseTemporaryMemory(8192L, "Full26 transaction/root-lock/control workspace detached")))

  private[mysql] def withLockedImage[A](actualActor:MySqlDeploymentReceiptBootstrapActor,
      sessionInputs:MySqlDeploymentReceiptSessionInputs)(consume:(MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow)=>A):A =
    requireCurrent()
    if (actualActor ne actor) || snapshot==null || (sessionInputs.full26Inputs ne inputs) then throw new IllegalArgumentException("exact schema5 locked image/inputs required")
    var fee=8192L
    snapshot.relation("fp_schema_manifest").rows.foreach(_.foreach {
      case MySqlFormat3Cell.Binary(value)=>fee=Math.addExact(fee,Math.multiplyExact(value.length.toLong,4L))
      case _=>()
    })
    image.reserveTemporaryMemory(fee,"Full26 locked C/K/profile actual defensive copies BEFORE allocation")
    var failure:Throwable=null
    try MySqlDeploymentReceiptReadOwner.requireManifest(snapshot,expectation)
    catch
      case cause:Throwable=>
        failure=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(failure,Vector(()=>image.releaseTemporaryMemory(fee,"Full26 locked C/K/profile comparison complete")))
    consume(snapshot,writerFacts)

  private def decodeWriter(image:MySqlScriptSchemaSnapshot,now:LocalDateTime):MySqlBusinessWriterRow =
    val relation=image.relation("fp_runtime_writer")
    if relation.rows.size!=1 then throw new SQLDataException("Full26 requires singleton actual writer")
    val shape=inputs.forTable(relation.table)
    def cell(name:String)=relation.rows.head(shape.columns.indexWhere(_.name==name))
    def uuid(name:String):java.util.UUID=cell(name) match
      case MySqlFormat3Cell.Binary(value) if value.length==16 =>
        val bytes=Array.tabulate(16)(value.byteAt)
        val buffer=java.nio.ByteBuffer.wrap(bytes)
        new java.util.UUID(buffer.getLong,buffer.getLong)
      case _=>throw new SQLDataException("Full26 writer UUID missing: "+name)
    def optionalUuid(name:String):Option[java.util.UUID]=cell(name) match
      case MySqlFormat3Cell.SqlNull=>None
      case _=>Some(uuid(name))
    val epoch=cell("writer_epoch") match
      case MySqlFormat3Cell.SignedInteger(value)=>value
      case _=>throw new SQLDataException("Full26 writer epoch missing")
    val lease=cell("lease_until") match
      case MySqlFormat3Cell.DateTime(value)=>value
      case _=>throw new SQLDataException("Full26 writer lease missing")
    val hash=cell("last_commit_fingerprint") match
      case MySqlFormat3Cell.SqlNull=>None
      case MySqlFormat3Cell.Binary(value) if value.length==32=>Some(Vector.tabulate(32)(value.byteAt))
      case _=>throw new SQLDataException("Full26 writer fingerprint malformed")
    MySqlBusinessWriterRow(uuid("binding_uuid"),epoch,optionalUuid("owner_boot_uuid"),lease,optionalUuid("last_commit_operation_uuid"),hash,now)

  /** 只在原 actor 内消费完整图；不把 graph/Connection 交 Main。 */
  def withCurrentConfiguration[A](actualActor: MySqlDeploymentReceiptBootstrapActor,
      sessionInputs: MySqlDeploymentReceiptSessionInputs, plan: WindowsWorldProvisionReceiptDataCodec.PlanData)
      (consume: MySqlScriptSchemaSnapshot => A): A =
    requireCurrent()
    if actualActor ne actor then throw new IllegalStateException("original Full26 actor required")
    if snapshot == null || (sessionInputs.full26Inputs ne inputs) || !image.isSnapshotReadChildOf(root) ||
        !image.sharesAllocationLedger(root) then throw new IllegalStateException("original complete Full26 image required")
    sessionInputs.requirePlanData(plan)
    if snapshot.relation("physical_recovery_binding").rows.nonEmpty then
      throw new SQLDataException("Fresh Full26 locked image no longer has noBinding")
    val writer = snapshot.relation("fp_runtime_writer")
    if writer.rows.size != 1 then throw new SQLDataException("Fresh Full26 requires original singleton writer")
    val writerShape = inputs.forTable(writer.table)
    def writerCell(name: String): MySqlFormat3Cell = writer.rows.head(writerShape.columns.indexWhere(_.name == name))
    if writerCell("writer_epoch") != MySqlFormat3Cell.SignedInteger(0L) ||
        writerCell("owner_boot_uuid") != MySqlFormat3Cell.SqlNull ||
        writerCell("last_commit_operation_uuid") != MySqlFormat3Cell.SqlNull ||
        writerCell("last_commit_fingerprint") != MySqlFormat3Cell.SqlNull then
      throw new SQLDataException("Fresh Full26 original writer has already been acquired or committed")
    writerCell("binding_uuid") match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(plan.bindingUuid()) => ()
      case _ => throw new SQLDataException("Fresh Full26 writer binding differs from original plan")
    val leaseUntil=writerCell("lease_until") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => throw new SQLDataException("Fresh Full26 writer lease timestamp missing")
    writerFacts=MySqlBusinessWriterRow(plan.bindingUuid(),0L,None,leaseUntil,None,None,databaseNow)
    var bytes = 8192L
    snapshot.relation("fp_schema_manifest").rows.foreach(_.foreach {
      case MySqlFormat3Cell.Binary(value) => bytes = Math.addExact(bytes, Math.multiplyExact(value.length.toLong, 4L))
      case _ => ()
    })
    image.reserveTemporaryMemory(bytes, "Full26 bounded current C/K/profile comparison BEFORE copies")
    var failure: Throwable = null
    try MySqlDeploymentReceiptReadOwner.requireManifest(snapshot, expectation)
    catch
      case cause: Throwable =>
        failure = cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(failure, Vector(() => image.releaseTemporaryMemory(bytes, "Full26 C/K/profile comparison detached")))
    val result = consume(snapshot)
    requireCurrent()
    result

  private def packetLength(length: Long, packet: Long): Unit =
    if length <= 0L || length > inputs.effectivePacketLimit(packet) then throw new SQLDataException("Full26 fixed SQL exceeds actual/profile packet budget")

  private def readRelation(shape: MySqlFormat3RelationShape, packet: Long): MySqlFormat3RelationSnapshot =
    requireCurrent()
    val length = MySqlDeploymentReceiptReadOwner.selectLength(inputs.schema, shape)
    packetLength(Math.addExact(length, 16L), packet)
    val fee = Math.addExact(128L, Math.multiplyExact(length, 4L))
    image.reserveTemporaryMemory(fee, s"${shape.table} Full26 fixed SQL BEFORE construction")
    var failure: Throwable = null
    try
      val sql = MySqlDeploymentReceiptReadOwner.select(inputs.schema, shape.table)
      MySqlBusinessRows.using(verified.connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) { statement =>
        statement.setFetchSize(Integer.MIN_VALUE)
        MySqlBusinessRows.using(statement.executeQuery()) { rs =>
          MySqlDeploymentReceiptReadOwner.validateMetadata(rs.getMetaData, shape.table)
          val rows = Vector.newBuilder[Vector[MySqlFormat3Cell]]
          var rowNumber = 0L
          while rs.next() do
            requireCurrent()
            if rowNumber >= Int.MaxValue.toLong then throw new SQLDataException("Full26 row count exceeds JVM Vector bound")
            image.beginRow(shape.columns.size, shape.table, rowNumber)
            val row = shape.columns.zipWithIndex.map { (column, index) => readCell(rs, index + 1, shape, column, rowNumber) }
            image.endRow(shape.table, rowNumber)
            rows += row
            rowNumber = Math.addExact(rowNumber, 1L)
          MySqlFormat3RelationSnapshot(shape.table, rows.result())
        }
      }
    catch
      case cause: Throwable =>
        failure = cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(failure, Vector(() => image.releaseTemporaryMemory(fee, "Full26 fixed SQL detached")))

  private def readCell(rs: ResultSet, index: Int, shape: MySqlFormat3RelationShape,
      column: MySqlFormat3ColumnShape, row: Long): MySqlFormat3Cell =
    val context = s"${shape.table} row $row column ${column.name}"
    def sqlNull(): MySqlFormat3Cell =
      if !column.nullable then throw new SQLDataException(s"$context must not be NULL")
      image.beginNullCell(context)
      MySqlFormat3Cell.SqlNull
    try column.kind match
      case MySqlFormat3SqlKind.Text =>
        val reader = rs.getCharacterStream(index)
        if reader == null then sqlNull()
        else MySqlFormat3Cell.Text(MySqlFormat3CellStreamDecoder.readText(reader, column.maximumLength, image, context,
          inputs.maximumPayloadBytes(shape.table, column)))
      case MySqlFormat3SqlKind.Binary =>
        val stream = rs.getBinaryStream(index)
        if stream == null then sqlNull()
        else
          val bound = inputs.maximumPayloadBytes(shape.table, column).map(n => Math.min(n, Int.MaxValue.toLong).toInt).orElse(column.maximumLength)
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MySqlFormat3CellStreamDecoder.readBinary(stream, bound, image, context), image))
      case MySqlFormat3SqlKind.Integer =>
        val value = rs.getLong(index)
        if rs.wasNull() then sqlNull()
        else
          column.requireSignedInteger(value, context)
          image.beginFixedCell(8, column.integerPacketWidth, context)
          MySqlFormat3Cell.SignedInteger(value)
      case MySqlFormat3SqlKind.Floating =>
        val value = rs.getDouble(index)
        if rs.wasNull() then sqlNull()
        else
          image.beginFixedCell(8, 64, context)
          MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(value))
      case MySqlFormat3SqlKind.DateTime =>
        val value = rs.getObject(index, classOf[LocalDateTime])
        if value == null then sqlNull()
        else
          if value.getNano % 1000 != 0 then throw new SQLDataException(s"$context exceeds DATETIME(6)")
          image.beginFixedCell(13, 32, context)
          MySqlFormat3Cell.DateTime(value)
    catch case cause: Throwable => throw new MySqlFormat3CellReadException(shape.table, column.name, row, cause)

private[mysql] object MySqlDeploymentReceiptReadOwner:
  /** 非权限的实际调用状态；失败后绝不允许继续恢复连接属性。 */
  private[mysql] final class ReadSettlement:
    private enum State:
      case Active, RolledBack, Settled
      case Uncertain(cause: Throwable)
    private var state: State=State.Active
    def rollbackReturned(): Unit =
      if state != State.Active then throw new IllegalStateException("Full26 rollback is one-shot")
      state=State.RolledBack
    def requireRestorable(): Unit =
      if state != State.RolledBack then throw new IllegalStateException("Full26 rollback did not return; restoration forbidden",failure.orNull)
    def restorationReturned(): Unit =
      requireRestorable();state=State.Settled
    def failed(cause: Throwable): Unit =
      require(cause != null,"original read settlement failure required")
      state match
        case State.Uncertain(original) => MySqlOwnedIoAdapter.suppressExitCleanup(original,cause)
        case _ => state=State.Uncertain(cause)
    def isSettled: Boolean=state == State.Settled
    def isUncertain: Boolean=state match
      case State.Uncertain(_) => true
      case _ => false
    def failure: Option[Throwable]=state match
      case State.Uncertain(cause) => Some(cause)
      case _ => None
  def allocate(session: MySqlDeploymentReceiptSession, actor: MySqlDeploymentReceiptBootstrapActor,
      verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope, expectation: MySqlSchemaAuditExpectation,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): MySqlDeploymentReceiptReadOwner =
    session.requireActor(actor)
    root.requireScriptDataInputs(inputs)
    root.reserveRetainedMemory(1024L, "Full26 read partial holder BEFORE allocation")
    try new MySqlDeploymentReceiptReadOwner(session, actor, verified, scope, expectation, inputs, root, null)
    catch
      case failure: Throwable =>
        try root.releaseRetainedMemory(1024L, "unpublished Full26 read holder")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(failure, cleanup)
        throw failure

  private[mysql] def allocateForRestrictedSourceUnknown(
      session:MySqlDeploymentReceiptSession,actor:MySqlDeploymentReceiptBootstrapActor,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,expectation:MySqlSchemaAuditExpectation,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget,
      owner:MySqlDeploymentReceiptSession.RestrictedSourceR0UnknownInspection5):MySqlDeploymentReceiptReadOwner =
    if session==null || actor==null || owner==null || verified==null || scope==null || expectation==null ||
        inputs==null || root==null || (owner.session ne session) || (owner.newVerified ne verified) ||
        (owner.newScope ne scope) || (owner.root ne root) || owner.reader!=null then
      throw new IllegalArgumentException("exact Session-issued independent UNKNOWN reader owner required")
    session.requireRestrictedSourceUnknownInspection(owner)
    root.requireScriptDataInputs(inputs)
    root.reserveRetainedMemory(1024L,"UNKNOWN Full26 read partial holder BEFORE allocation")
    try new MySqlDeploymentReceiptReadOwner(session,actor,verified,scope,expectation,inputs,root,owner)
    catch
      case failure:Throwable=>
        try root.releaseRetainedMemory(1024L,"unpublished UNKNOWN Full26 read holder")
        catch case cleanup:Throwable=>MySqlOwnedSessionLifecycle.suppress(failure,cleanup)
        throw failure

  private def schema(value: String): Unit =
    require(value != null && value.matches("[a-z][a-z0-9_]{0,63}"), "fixed safe schema identifier required")
  def rootLockSql(schemaName: String, table: String): String =
    schema(schemaName)
    require(Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer").contains(table), "fixed M/B/W root required")
    s"SELECT `singleton_fixed1` FROM `$schemaName`.`$table` ORDER BY `singleton_fixed1` FOR UPDATE"
  def select(schemaName: String, table: String): String =
    schema(schemaName)
    val shape = MySqlDeploymentReceiptRelationShapes.forTable(table)
    val columns = shape.columns.map(c => "`" + c.name + "`").mkString(",")
    val keys = shape.primaryKey.map(k => "`" + k + "`").mkString(",")
    s"SELECT $columns FROM `$schemaName`.`${shape.table}` ORDER BY $keys ${shape.lockClause}"
  def selectLength(schemaName: String, shape: MySqlFormat3RelationShape): Long =
    schema(schemaName)
    require(MySqlDeploymentReceiptRelationShapes.canonicalOrder.exists(_ eq shape), "exact closed Full26 shape required")
    val columns = shape.columns.foldLeft(0L)((n, c) => Math.addExact(n, c.name.length.toLong + 2L))
    val keys = shape.primaryKey.foldLeft(0L)((n, k) => Math.addExact(n, k.length.toLong + 2L))
    7L + columns + shape.columns.size - 1L + 6L + schemaName.length + shape.table.length + 5L +
      10L + keys + shape.primaryKey.size - 1L + 1L + shape.lockClause.length
  def validateMetadata(metadata: ResultSetMetaData, table: String): Unit =
    val shape = MySqlDeploymentReceiptRelationShapes.forTable(table)
    if metadata == null || metadata.getColumnCount != shape.columns.size then throw new SQLDataException("complete frozen Full26 columns required")
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val i = offset + 1
      val jdbc = if column.sqlDeclaration.startsWith("MEDIUMBLOB") then Types.LONGVARBINARY else column.jdbcType
      val nullable = if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls
      if metadata.getColumnLabel(i) != column.name || metadata.getColumnType(i) != jdbc || metadata.isNullable(i) != nullable ||
          (column.kind == MySqlFormat3SqlKind.DateTime && metadata.getScale(i) != 6) then
        throw new SQLDataException(s"${shape.table}.${column.name} Full26 column identity/type/nullability/precision differs")
    }

  private[mysql] def requireManifestData(snapshot:MySqlScriptSchemaSnapshot,expectation:MySqlSchemaAuditExpectation,
      budget:MySqlFormat3ReadBudget):Unit =
    require(snapshot!=null && expectation!=null && budget!=null && !budget.isClosed &&
      (snapshot.inputs.sourceLimitProfile eq budget.profile) && snapshot.allocationBudget.sharesAllocationLedger(budget),
      "original complete schema5 manifest DATA/budget required")
    var fee=8192L
    snapshot.relation("fp_schema_manifest").rows.foreach(_.foreach {
      case MySqlFormat3Cell.Binary(value)=>fee=Math.addExact(fee,Math.multiplyExact(value.length.toLong,4L))
      case _=>()
    })
    budget.reserveTemporaryMemory(fee,"schema5 manifest DATA actual defensive copies BEFORE allocation")
    var primary:Throwable=null
    try
      requireManifest(snapshot,expectation)
    catch
      case cause:Throwable =>
        primary=cause
        throw cause
    finally
      MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
        budget.releaseTemporaryMemory(fee,"schema5 manifest DATA comparison complete")))

  private def requireManifest(snapshot: MySqlScriptSchemaSnapshot, expectation: MySqlSchemaAuditExpectation): Unit =
    val relation = snapshot.relation("fp_schema_manifest")
    if relation.rows.size != 1 then throw new SQLDataException("Full26 requires exactly one READY manifest")
    val columns = snapshot.inputs.forTable(relation.table).columns
    def field(name: String): MySqlFormat3Cell = relation.rows.head(columns.indexWhere(_.name == name))
    def check(name: String, expected: MySqlFormat3Cell): Unit =
      if field(name) != expected then throw new SQLDataException(s"schema5 manifest $name differs")
    check("singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
    check("backend", MySqlFormat3Cell.Text("MYSQL"))
    check("format_version", MySqlFormat3Cell.SignedInteger(5L))
    check("bootstrap_state", MySqlFormat3Cell.Text("READY"))
    check("import_state", MySqlFormat3Cell.Text("NONE"))
    def uuid(name: String, value: java.util.UUID): Unit = field(name) match
      case MySqlFormat3Cell.Binary(actual) if actual.equalsUuid(value) => ()
      case _ => throw new SQLDataException(s"schema5 manifest $name differs from approval")
    uuid("provisioning_uuid", expectation.provisioningUuid)
    uuid("expected_server_uuid", expectation.serverUuid)
    uuid("expected_binding_uuid", expectation.bindingUuid)
    field("ddl_sha256") match
      case MySqlFormat3Cell.Binary(actual) if actual.equalsBytes(java.util.HexFormat.of().parseHex(expectation.ddlSha256)) => ()
      case _ => throw new SQLDataException("schema5 manifest DDL differs from approval")
    Vector("import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256").foreach(check(_, MySqlFormat3Cell.SqlNull))
    val security = expectation.deploymentReceiptInputs.getOrElse(throw new SQLDataException("schema5 independent C/K/profile missing"))
    val labels = security.manifestLabels
    val cells = labels.map(name => field(name) match
      case MySqlFormat3Cell.Text(value) => MySqlAuditText(value)
      case MySqlFormat3Cell.SignedInteger(value) => MySqlAuditText(value.toString)
      case MySqlFormat3Cell.Binary(value) =>
        val bytes = new Array[Byte](value.length)
        var i = 0
        while i < bytes.length do
          bytes(i) = value.byteAt(i)
          i += 1
        MySqlAuditBinary(bytes)
      case MySqlFormat3Cell.SqlNull => MySqlAuditNull
      case _ => throw new SQLDataException("schema5 manifest C/K/profile cell type differs"))
    security.validateManifest(MySqlAuditResultSet("manifest", labels.map(n =>
      MySqlAuditColumn(n, Types.VARCHAR, "VARCHAR", 0, 0, ResultSetMetaData.columnNullable, false)), Vector(MySqlAuditRow(cells))), MySqlAuditRow(cells))
