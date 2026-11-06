package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException}
import java.time.LocalDateTime
import java.util.UUID

/** 调用方在首条SQL前持有；失败child、首因与清理责任不会转为局部临时变量。 */
private[mysql] final class MySqlDbFirstSnapshotJdbc private (
    private val inputs: MySqlDbFirstSessionInputs,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private val scopeConnectionGeneration: Long,
    private val ownerThread: Thread,
    private val full27Inputs: MySqlScriptSchemaInputs,
    private var inputBorrow: AutoCloseable,
    private var holderFee: Long,
    private val observationOwner: MySqlDbFirstSession,
    private val retiredObservationOwner: MySqlDbFirstSession.RetiredRecallObservation,
    private var serviceOwner: MySqlDbFirstSession
) extends AutoCloseable:
  private var attempted=false
  private var reading=false
  private var closed=false
  private var cleanupAttempted=false
  private var cleanupFailure: Throwable=null
  private var failure: Throwable=null
  private var image: MySqlFormat3ReadBudget=null
  private var snapshotValue: MySqlScriptSchemaSnapshot=null
  private var verifiedValue: VerifiedMySqlConnection=null
  private var scopeValue: MySqlAuditBootScope=null
  private var qualificationValue: VerifiedSchemaQualification=null
  private var gateValue: MySqlConnectionTransactionGate.Owner=null
  private var rollbackValue: MySqlRollbackOnlyState=null
  private var lockedValue: MySqlDbFirstSnapshotJdbc.LockedRead=null
  private var packetValue=0L
  private var writerDatabaseTime:LocalDateTime=null
  private var writerTimeAnchorNanos=0L
  private var writerTimeFee=0L
  private var writerTimeStatement:PreparedStatement=null
  private var writerTimeRows:ResultSet=null
  private var writerTimeCleanupFailure:Throwable=null
  private var recallLeaseWindow:MySqlAdmissionWindow=null
  private def requireThread(): Unit =
    if Thread.currentThread() ne ownerThread then throw new IllegalStateException("schema6 reader requires original IO worker")
  private def requireHolderOpen(): Unit =
    requireThread()
    if closed || cleanupAttempted then throw new IllegalStateException("schema6 reader detached or cleanup incomplete",cleanupFailure)
    inputs.requireForSession(root,identity)
  def firstFailure: Option[Throwable] = { requireThread();Option(failure) }
  def cleanupObservation: MySqlDbFirstSnapshotJdbc.CleanupObservation =
    requireThread()
    MySqlDbFirstSnapshotJdbc.CleanupObservation(closed,image != null,snapshotValue != null,inputBorrow != null,
      holderFee,Option(cleanupFailure).orElse(Option(writerTimeCleanupFailure)),
      writerTimeStatement!=null || writerTimeRows!=null,writerTimeFee)
  def snapshot: MySqlScriptSchemaSnapshot =
    requireHolderOpen()
    if failure != null then throw new IllegalStateException("schema6 capture failed",failure)
    if snapshotValue == null then throw new IllegalStateException("schema6 capture has no completed image")
    snapshotValue

  /** 实际完整RR读取及SQL时钟的只读窗口；不是writer token或Main副作用许可。 */
  private[mysql] def observedRecallLease(binding:UUID,boot:UUID,epoch:Long,
      originalLease:LocalDateTime):MySqlAdmissionWindow =
    val image=snapshot
    require(writerDatabaseTime!=null && writerTimeFee!=0L && writerTimeCleanupFailure==null &&
      writerTimeRows==null && writerTimeStatement==null,"actual completed observation SQL clock required")
    val validity=MySqlDbFirstSnapshotJdbc.observedRecallLeaseNanos(image,binding,boot,epoch,
      originalLease,writerDatabaseTime)
    if recallLeaseWindow==null then recallLeaseWindow=MySqlAdmissionWindow(writerTimeAnchorNanos,validity)
    require(recallLeaseWindow.validityNanos==validity,"original observed lease window cannot change")
    require(recallLeaseWindow.remainingAt(System.nanoTime())>0L,"actual observed recall lease window expired")
    recallLeaseWindow

  /** read已完整返回后只保留DATA/image/input borrow，不能继续钉住将退休的writer audit资格图。 */
  private[mysql] def detachCompletedReadAuthority():Unit =
    requireHolderOpen()
    require(attempted && !reading && failure==null && snapshotValue!=null && lockedValue==null,
      "only an actual completed schema6 image may detach read authority")
    verifiedValue=null;scopeValue=null;qualificationValue=null;gateValue=null;rollbackValue=null

  /** 只借用调用方已存在的gate/RR事务；不begin、commit、rollback或发行Ready。 */
  def readLocked(verified: VerifiedMySqlConnection,scope: MySqlAuditBootScope,
      qualification: VerifiedSchemaQualification,gate: MySqlConnectionTransactionGate.Owner,
      rollbackOnly: MySqlRollbackOnlyState): MySqlScriptSchemaSnapshot =
    read(verified,scope,qualification,gate,rollbackOnly,false)

  /** 失败操作的独立诊断视图：同样核真实gate/qualification，但所有27表SELECT均不申请行锁。 */
  private[mysql] def readConsistentObservation(verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      qualification:VerifiedSchemaQualification,gate:MySqlConnectionTransactionGate.Owner,
      rollbackOnly:MySqlRollbackOnlyState):MySqlScriptSchemaSnapshot =
    read(verified,scope,qualification,gate,rollbackOnly,true)

  private def read(verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,
      qualification:VerifiedSchemaQualification,gate:MySqlConnectionTransactionGate.Owner,
      rollbackOnly:MySqlRollbackOnlyState,consistentObservation:Boolean):MySqlScriptSchemaSnapshot =
    requireHolderOpen()
    if attempted then throw new IllegalStateException("schema6 capture is one-shot, including failure",failure)
    attempted=true
    try
      require(verified != null && scope != null && qualification != null && gate != null && rollbackOnly != null,
        "actual schema6 connection/scope/qualification/gate/rollback state required")
      // 先认证真实gate；不能把foreign rollback state误标为本次读失败。
      MySqlConnectionTransactionGate.requireCurrent(gate,verified.connection,rollbackOnly)
      rollbackValue=rollbackOnly
      verifiedValue=verified;scopeValue=scope;qualificationValue=qualification;gateValue=gate
      requireTransaction()
      reading=true
      lockedValue=new MySqlDbFirstSnapshotJdbc.LockedRead(this,inputs.full27Inputs,consistentObservation)
      val connection=verified.connection
      val sql="SELECT @@SESSION.max_allowed_packet"
      if sql.length.toLong+16L > root.profile.maxSqlPacketBytes() then
        throw new SQLDataException("schema6 packet observation SQL exceeds original packet profile")
      packetValue=MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { result =>
          if !result.next() then throw new SQLDataException("schema6 packet observation missing")
          val value=result.getLong(1)
          if result.wasNull() || value <= 0L || result.next() then
            throw new SQLDataException("schema6 packet observation requires one positive value")
          value
        }
      }
      requireTransaction()
      val observation=MySqlFormat3ConnectionObservation.from(verified.identity,"REPEATABLE-READ")
      val observationScope=MySqlFormat3ObservationScope(scope.bootUuid,scope.bootGeneration,scope.connectionGeneration)
      val full27=inputs.full27Inputs
      if serviceOwner!=null then
        root.issueDbFirstServiceSnapshotImage(full27,full27.schema,observation,observationScope,packetValue,this)
      else if observationOwner==null && retiredObservationOwner==null then
        root.issueDbFirstSnapshotImage(full27,full27.schema,observation,observationScope,packetValue,this)
      else
        root.issueDbFirstInspectionSnapshotImage(full27,full27.schema,observation,observationScope,packetValue,this)
      val cells=new MySqlFormat3FullSnapshotReader(verified,scope,full27.schema,full27.sourceLimitProfile)
      val captured=scala.collection.mutable.Map.empty[String,MySqlFormat3RelationSnapshot]
      MySqlDbFirstSnapshotSql.lockedReadOrder.foreach { shape =>
        requireTransaction() // 上一ResultSet已关闭，才允许驱动的事务状态查询。
        lockedValue.requireReader(verified,scope,image,packetValue)
        val relation=cells.readDbFirstRelation(lockedValue,shape.table,image,packetValue)
        if captured.contains(shape.table) then throw new SQLDataException("schema6 repeated locked relation")
        requireRootRow(relation)
        captured.put(shape.table,relation)
      }
      if captured.size != 27 then throw new SQLDataException("schema6 capture requires all27 relations")
      val relations=MySqlDbFirstSnapshotSql.canonicalOrder.map(shape=>captured.getOrElse(shape.table,
        throw new SQLDataException("schema6 capture omitted a canonical relation")))
      snapshotValue=MySqlScriptSchemaSnapshot.decoded(full27.schema,observation,observationScope,packetValue,full27,relations,image)
      MySqlDbFirstSnapshotSql.requireManifest(snapshotValue,inputs.auditExpectation)
      snapshotValue.canonicalSha256
      requireTransaction()
      if consistentObservation then readObservationDatabaseTime()
      requireTransaction()
      snapshotValue
    catch case cause: Throwable =>
      if failure == null then failure=cause else MySqlOwnedSessionLifecycle.suppress(failure,cause)
      if rollbackValue != null then
        try rollbackValue.mark(cause)
        catch case marking: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,marking)
      throw cause
    finally
      reading=false;lockedValue=null

  /** 在完整一致性视图之后取真实DB时钟；单调锚点在SQL之前，不能把网络耗时加回租约。 */
  private def readObservationDatabaseTime():Unit =
    requireTransaction()
    require(writerTimeFee==0L && writerDatabaseTime==null,"observation SQL clock is one-shot")
    val sql="SELECT UTC_TIMESTAMP(6)"
    if sql.length.toLong+16L>math.min(packetValue,math.min(root.profile.maxSqlPacketBytes(),inputs.full27Inputs.budget.packetBytes)) then
      throw new SQLDataException("observation SQL clock exceeds original packet bound")
    root.reserveRetainedMemory(1024L,"observation SQL clock handles/window BEFORE allocation")
    writerTimeFee=1024L
    writerTimeAnchorNanos=System.nanoTime()
    var primary:Throwable=null
    try
      writerTimeStatement=verifiedValue.connection.prepareStatement(sql)
      writerTimeRows=writerTimeStatement.executeQuery()
      if !writerTimeRows.next() then throw new SQLDataException("observation database time is missing")
      val value=writerTimeRows.getObject(1,classOf[LocalDateTime])
      val wasNull=writerTimeRows.wasNull()
      if value==null || wasNull || value.getNano%1000!=0 || writerTimeRows.next() then
        throw new SQLDataException("observation database time must be singleton DATETIME6")
      writerDatabaseTime=value
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally
      try closeWriterTimeCursor()
      catch case cleanup:Throwable=>
        if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private def closeWriterTimeCursor():Unit =
    if writerTimeCleanupFailure!=null then throw writerTimeCleanupFailure
    try MySqlFormat3BudgetCleanup.run(null,Vector(
      ()=>if writerTimeRows!=null then { writerTimeRows.close();writerTimeRows=null },
      ()=>if writerTimeStatement!=null then { writerTimeStatement.close();writerTimeStatement=null }))
    catch case cause:Throwable=>
      writerTimeCleanupFailure=cause
      throw cause

  private def requireCurrent(): Unit =
    requireHolderOpen()
    val verified=verifiedValue
    val scope=scopeValue
    val qualification=qualificationValue
    val expectation=inputs.auditExpectation
    if verified == null || scope == null || qualification == null ||
      (scope.ownerThread ne ownerThread) || (scope.connectionToken ne verified.connection) ||
      !scope.verifiedConnection.contains(verified) || (scope.connectionIdentity ne verified.identity) ||
      scope.bootUuid != identity.bootId || scope.bootGeneration != identity.bootGeneration ||
      scope.connectionGeneration != scopeConnectionGeneration then
      throw new IllegalStateException("schema6 reader differs from original connection/worker/boot identity")
    MySqlConnectionTransactionGate.requireCurrent(gateValue,verified.connection,rollbackValue)
    val certifiedPhysical=qualification.physicalBinding match
      case MySqlCertifiedPhysicalBinding(binding,2) if binding == expectation.bindingUuid => true
      case _ => false
    if !certifiedPhysical || expectation.profile != MySqlSchemaAuditProfile.DbFirst1 ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || expectation.dbFirstInputs.isEmpty ||
      !qualification.isCurrent(identity.bootId,identity.bootGeneration,scopeConnectionGeneration,
        expectation.serverUuid,expectation.schema,expectation.bindingUuid,MySqlSchemaAuditProfile.DbFirst1,
        MySqlExpectedPhysicalFormat.Format2,verified,ownerThread,System.nanoTime()) ||
      verified.identity.serverUuid != expectation.serverUuid || verified.identity.catalog != expectation.schema ||
      verified.identity.serverVersion != expectation.serverVersion || verified.identity.effectiveUser != expectation.effectiveUser ||
      qualification.provisioningUuid != expectation.provisioningUuid ||
      !qualification.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) ||
      !qualification.objectManifestSha256.equalsIgnoreCase(expectation.objects.canonicalSha256) then
      throw new IllegalStateException("schema6 current qualification differs from original security expectation")
    expectation.expectedImport match
      case MySqlExpectedNoImport if qualification.importState == "NONE" && qualification.importOperationUuid.isEmpty &&
        qualification.importManifestSha256.isEmpty && qualification.importSnapshotSha256.isEmpty && qualification.importReceiptSha256.isEmpty => ()
      case _ => throw new IllegalStateException("schema6 qualification import evidence differs")

  /** 仅在所有流式ResultSet都已关闭时调用；驱动getter可能执行查询。 */
  private def requireTransaction(): Unit =
    requireCurrent()
    val connection=verifiedValue.connection
    if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
      throw new SQLDataException("schema6 capture requires original active REPEATABLE READ transaction")

  private def requireRootRow(relation: MySqlFormat3RelationSnapshot): Unit =
    if relation.table == "fp_schema_manifest" || relation.table == "physical_recovery_binding" || relation.table == "fp_runtime_writer" then
      val shape=inputs.full27Inputs.forTable(relation.table)
      if relation.rows.size != 1 then throw new SQLDataException("schema6 root lock requires exactly one "+relation.table)
      def field(name: String)=relation.rows.head(shape.columns.indexWhere(_.name == name))
      if field("singleton_fixed1") != MySqlFormat3Cell.SignedInteger(1L) then throw new SQLDataException("schema6 root singleton differs")
      if relation.table == "fp_schema_manifest" &&
        (field("backend") != MySqlFormat3Cell.Text("MYSQL") || field("format_version") != MySqlFormat3Cell.SignedInteger(6L) ||
          field("bootstrap_state") != MySqlFormat3Cell.Text("READY")) then
        throw new SQLDataException("schema6 root lock requires MYSQL READY6 manifest")
      if relation.table != "fp_schema_manifest" then
        field("binding_uuid") match
          case MySqlFormat3Cell.Binary(value) if value.equalsUuid(inputs.auditExpectation.bindingUuid) => ()
          case _ => throw new SQLDataException("schema6 root binding identity differs")
      if relation.table == "physical_recovery_binding" &&
        (field("cutover_state") != MySqlFormat3Cell.Text("CERTIFIED") || field("format_version") != MySqlFormat3Cell.SignedInteger(2L)) then
        throw new SQLDataException("schema6 ordinary capture requires existing CERTIFIED physical Format2 binding")

  private[mysql] def requireImageIssuer(actualRoot: MySqlFormat3ReadBudget,full27: MySqlScriptSchemaInputs): Unit =
    requireCurrent()
    if !reading || lockedValue == null || (actualRoot ne root) || (full27 ne inputs.full27Inputs) || image != null then
      throw new IllegalStateException("schema6 original holder requires its single image")
  private[mysql] def requireServiceRegistration(actual:MySqlDbFirstSession,
      actualInputs:MySqlDbFirstSessionInputs,actualRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity):Unit =
    requireThread()
    require(!attempted && !closed && !cleanupAttempted && serviceOwner==null &&
      observationOwner==null && retiredObservationOwner==null && holderFee==MySqlDbFirstSnapshotJdbc.HolderBytes &&
      (inputs eq actualInputs) && (root eq actualRoot) && (identity eq actualIdentity) &&
      (inputs.serviceOwnerForSnapshot(root,identity) eq actual),
      "actual newly allocated service reader/input binding required before private registration")

  private[mysql] def requireServiceReaderRetired(actual:MySqlDbFirstSession):Unit =
    requireThread()
    require((serviceOwner eq actual) && cleanupAttempted && !reading && image==null && snapshotValue==null &&
      inputBorrow==null && holderFee==0L && writerTimeFee==0L && writerTimeStatement==null && writerTimeRows==null &&
      cleanupFailure==null && writerTimeCleanupFailure==null,
      "actual service reader DATA/borrow/fee retirement required before private owner edge detachment")

  /** 新服务代的普通读图只认实际Session私有reader登记及真实SQL观察，不重置原root header。 */
  private[mysql] def requireServiceImageIssuer(actualRoot:MySqlFormat3ReadBudget,
      full27:MySqlScriptSchemaInputs,connection:MySqlFormat3ConnectionObservation,
      observedScope:MySqlFormat3ObservationScope,packet:Long):Unit =
    requireImageIssuer(actualRoot,full27)
    require(serviceOwner!=null && observationOwner==null && retiredObservationOwner==null &&
      connection==MySqlFormat3ConnectionObservation.from(verifiedValue.identity,"REPEATABLE-READ") &&
      observedScope==MySqlFormat3ObservationScope(scopeValue.bootUuid,scopeValue.bootGeneration,scopeValue.connectionGeneration) &&
      packet==packetValue,"actual service reader and SQL observation/packet required")
    serviceOwner.requireServiceImageIssuer(this,actualRoot,inputs,identity,verifiedValue,scopeValue)

  /** 核真实SQL资格/gate后，再核实际Session私有登记；metadata不能选择inspection发行路径。 */
  private[mysql] def requireInspectionImageIssuer(actualRoot:MySqlFormat3ReadBudget,
      full27:MySqlScriptSchemaInputs,connection:MySqlFormat3ConnectionObservation,
      observedScope:MySqlFormat3ObservationScope,packet:Long):Unit =
    requireImageIssuer(actualRoot,full27)
    require(connection==MySqlFormat3ConnectionObservation.from(verifiedValue.identity,"REPEATABLE-READ") &&
      observedScope==MySqlFormat3ObservationScope(scopeValue.bootUuid,scopeValue.bootGeneration,scopeValue.connectionGeneration) &&
      packet==packetValue,"actual inspection observation/packet required")
    if retiredObservationOwner!=null then
      require(observationOwner==null,"inspection reader cannot mix initialization and retired observation owners")
      retiredObservationOwner.requireImageIssuer(this,actualRoot,inputs,identity,verifiedValue,scopeValue)
    else
      require(observationOwner!=null,"actual registered inspection owner required")
      observationOwner.requireInspectionImageIssuer(this,actualRoot,inputs,identity,verifiedValue,scopeValue)

  private[mysql] def retainIssuedImage(actualRoot: MySqlFormat3ReadBudget,child: MySqlFormat3ReadBudget): Unit =
    // 分配后不得再查时钟、qualification、连接或consumer；先登记真实child。
    if (actualRoot ne root) || !reading || lockedValue == null || image != null || child == null then
      throw new IllegalStateException("schema6 original pending image issuance required")
    image=child // 在所有后续initialize/current检查之前保留。
  private def requireLocked(token: MySqlDbFirstSnapshotJdbc.LockedRead,verified: VerifiedMySqlConnection,
      scope: MySqlAuditBootScope,budget: MySqlFormat3ReadBudget,packet: Long): Unit =
    requireCurrent()
    if !reading || (token ne lockedValue) || (verified ne verifiedValue) || (scope ne scopeValue) ||
      (budget ne image) || packet != packetValue || image == null then
      throw new IllegalStateException("schema6 stream token differs from actual locked capture")
    budget.requireScriptDataInputs(inputs.full27Inputs)

  override def close(): Unit =
    requireThread()
    if closed then return
    if cleanupAttempted then throw new IllegalStateException("schema6 reader cleanup incomplete; no automatic retry",cleanupFailure)
    if reading then throw new IllegalStateException("schema6 capture still executing")
    if writerTimeCleanupFailure!=null then throw new IllegalStateException("observation SQL clock cleanup retained; no automatic retry",writerTimeCleanupFailure)
    if serviceOwner!=null then serviceOwner.requireServiceSnapshotReader(this)
    cleanupAttempted=true
    try
      root.requireScriptDataInputs(full27Inputs)
      // 不要求gate仍active；SQL资源已由using退出，snapshot先detach，再退休实际child。
      snapshotValue=null;lockedValue=null;recallLeaseWindow=null;writerDatabaseTime=null
      if image != null then
        image.closeScopedReadBudget()
        image=null
      if inputBorrow != null then
        inputBorrow.close()
        inputBorrow=null
      if writerTimeFee!=0L then
        root.releaseRetainedMemory(writerTimeFee,"observation SQL clock/window detached")
        writerTimeFee=0L
      if holderFee != 0L then
        root.releaseRetainedMemory(holderFee,"schema6 snapshot JDBC holder detached")
        holderFee=0L
      verifiedValue=null;scopeValue=null;qualificationValue=null;gateValue=null;rollbackValue=null
      if serviceOwner!=null then
        serviceOwner.detachServiceSnapshotReader(this)
        serviceOwner=null
      closed=true
    catch case cause: Throwable =>
      cleanupFailure=cause
      if failure == null then failure=cause else MySqlOwnedSessionLifecycle.suppress(failure,cause)
      throw cause
  override def toString: String = "MySqlDbFirstSnapshotJdbc[redacted; no admission]"

private[mysql] object MySqlDbFirstSnapshotJdbc:
  /** 只核实际读图中的原W及DB时钟；纯DATA结果不能直接转换为token/admission。 */
  private[mysql] def observedRecallLeaseNanos(image:MySqlScriptSchemaSnapshot,binding:UUID,boot:UUID,
      epoch:Long,originalLease:LocalDateTime,databaseTime:LocalDateTime):Long =
    require(image!=null && binding!=null && boot!=null && epoch>0L && originalLease!=null && databaseTime!=null,
      "original recall writer identity/lease and actual observation time required")
    require(!image.allocationBudget.isClosed && image.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "actual live complete Full27 observation required")
    image.allocationBudget.requireScriptDataInputs(image.inputs)
    if originalLease.getNano%1000!=0 || databaseTime.getNano%1000!=0 then
      throw new SQLDataException("original recall lease/observation time must be DATETIME6")
    val rows=image.relation("fp_runtime_writer").rows
    if rows.size!=1 then throw new SQLDataException("observed recall writer singleton differs")
    def value(name:String)=MySqlDbFirstRecallMaterializer.value(rows.head,"fp_runtime_writer",name,image.inputs)
    if value("singleton_fixed1")!=MySqlFormat3Cell.SignedInteger(1L) ||
      !MySqlDbFirstRecallMaterializer.uuidEquals(value("binding_uuid"),binding) ||
      !MySqlDbFirstRecallMaterializer.uuidEquals(value("owner_boot_uuid"),boot) ||
      value("writer_epoch")!=MySqlFormat3Cell.SignedInteger(epoch) then
      throw new SQLDataException("observed recall writer differs from original binding/boot/epoch")
    val lease=value("lease_until") match
      case MySqlFormat3Cell.DateTime(at) if at!=null && at.getNano%1000==0 && !at.isBefore(originalLease)=>at
      case _=>throw new SQLDataException("observed recall writer lease regressed or is malformed")
    MySqlSchemaAuditor.boundedLeaseValidityNanos(databaseTime,lease)

  private val HolderBytes=4096L // holder/token/reader/observations及27表索引与canonical引用槽。
  final case class CleanupObservation(closed: Boolean,imagePending: Boolean,snapshotPending: Boolean,
      inputBorrowPending: Boolean,holderFee: Long,failure: Option[Throwable],
      writerTimePending:Boolean=false,writerTimeFee:Long=0L)
  /** 构造失败若仍有实际资源，原caller必须接住异常中的holder并保留其清理责任；不是成功输入。 */
  final class CreationFailure private[MySqlDbFirstSnapshotJdbc] (
      original: Throwable,val retainedHolder: MySqlDbFirstSnapshotJdbc)
      extends RuntimeException("schema6 reader creation failed with retained cleanup responsibility",original)
  def create(inputs: MySqlDbFirstSessionInputs,root: MySqlFormat3ReadBudget,
      actualIoIdentity: MySqlOwnedIoIdentity): MySqlDbFirstSnapshotJdbc =
    require(inputs != null && root != null && actualIoIdentity != null,"actual schema6 inputs/root/IO identity required")
    createBound(inputs,root,actualIoIdentity,actualIoIdentity.connectionGeneration,null,null)

  /** 新SQL连接代只来自实际inspection owner；不能以caller给出的数字重绑reader。 */
  private[mysql] def createForObservation(session:MySqlDbFirstSession,inputs:MySqlDbFirstSessionInputs,
      root:MySqlFormat3ReadBudget,actualIoIdentity:MySqlOwnedIoIdentity):MySqlDbFirstSnapshotJdbc =
    require(session!=null,"actual inspection Session required")
    val generation=session.requireDbFirstObservationReaderSource(inputs,root,actualIoIdentity)
    createBound(inputs,root,actualIoIdentity,generation,session,null)

  /** 退休后的新reader只由原fault owner私有登记Work给定真实现有连接代。 */
  private[mysql] def createForRetiredObservation(work:MySqlDbFirstSession.RetiredRecallObservation,
      inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,
      actualIoIdentity:MySqlOwnedIoIdentity):MySqlDbFirstSnapshotJdbc =
    require(work!=null,"actual registered retired recall observation Work required")
    val generation=work.requireReaderSource(inputs,root,actualIoIdentity)
    createBound(inputs,root,actualIoIdentity,generation,null,work)

  private def createBound(inputs:MySqlDbFirstSessionInputs,root:MySqlFormat3ReadBudget,
      actualIoIdentity:MySqlOwnedIoIdentity,scopeConnectionGeneration:Long,
      observationOwner:MySqlDbFirstSession,
      retiredObservationOwner:MySqlDbFirstSession.RetiredRecallObservation):MySqlDbFirstSnapshotJdbc =
    inputs.requireForSession(root,actualIoIdentity)
    val full27=inputs.full27Inputs
    val thread=Thread.currentThread()
    // 先预收再构造只有赋值的holder；borrow在holder已可保留之后取得。
    root.reserveRetainedMemory(HolderBytes,"schema6 snapshot JDBC holder BEFORE allocation")
    var holder: MySqlDbFirstSnapshotJdbc=null
    try
      val service=if observationOwner==null && retiredObservationOwner==null then
        inputs.serviceOwnerForSnapshot(root,actualIoIdentity) else null
      holder=new MySqlDbFirstSnapshotJdbc(inputs,root,actualIoIdentity,scopeConnectionGeneration,thread,full27,null,HolderBytes,observationOwner,retiredObservationOwner,null)
      if service!=null then
        service.registerServiceSnapshotReader(holder,inputs,root,actualIoIdentity)
        holder.serviceOwner=service // 实际节点已入私有表；后续borrow失败仍由原holder及Session共同保管。
      holder.inputBorrow=inputs.borrowForReader()
      holder
    catch case cause: Throwable =>
      if holder == null then
        try root.releaseRetainedMemory(HolderBytes,"unallocated schema6 snapshot JDBC holder")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      else
        holder.failure=cause
        try holder.close()
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
        if holder.image != null || holder.inputBorrow != null || holder.holderFee != 0L then
          throw new CreationFailure(cause,holder)
      throw cause
  final class LockedRead private[MySqlDbFirstSnapshotJdbc] (
      holder: MySqlDbFirstSnapshotJdbc,private[mysql] val inputs: MySqlScriptSchemaInputs,
      private[mysql] val consistentObservation:Boolean):
    private[mysql] def requireReader(verified: VerifiedMySqlConnection,scope: MySqlAuditBootScope,
        budget: MySqlFormat3ReadBudget,packet: Long): Unit = holder.requireLocked(this,verified,scope,budget,packet)
