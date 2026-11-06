package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.ProductionBootstrapControl
import com.wi.floatingpod.server.{DbFirstBootstrapRecordCodec as Records, DbFirstWorldAdoptionPlanData,
  DbFirstStartupRetirementCodec as Retirement,
  DbFirstWorldAdoptionReceiptCodec as Sources, WindowsDbFirstAdoptionStore as Store,
  WindowsDbFirstMarkerAdoption, WindowsMarkerIo, WindowsWorldProvisioner}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLDataException}
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/**
 * 原 managed worker 的 FirstBinding6 owner。阶段 intent/全图见证先耐久，SQL 只执行一次；
 * 重启以原 intent 的 before/target 完整图解释结果，不以过期 lease 或单个 W slot 猜测提交。
 */
private[mysql] final class MySqlDbFirstBootstrapActor(
    session: MySqlDbFirstSession, inputs: MySqlDbFirstSessionInputs, root: MySqlFormat3ReadBudget,
    identity: MySqlOwnedIoIdentity, control: ProductionBootstrapControl.DbFirstControl, prepaidHolderBytes: Long
):
  import MySqlDbFirstBootstrapActor.*
  private val thread = Thread.currentThread()
  private val full27 = inputs.full27Inputs
  private val installed = control.managedBundle().orElseThrow().identity()
  private val plan = new DbFirstWorldAdoptionPlanData(installed.creationOperationUuid(), installed.bootstrapOperationUuid(),
    installed.prepareWriteUuid(), installed.certifyWriteUuid(), installed.bindingUuid(), 2, installed.serverUuid(),
    installed.schema(), 6, inputs.auditExpectation.ddlSha256, installed.provisioningUuid())
  private val quotaMaximum = inputs.firstBindingQuotaMaximumRecords
  private var started = false
  private var completed = false
  private var closed = false
  private var failure: Throwable = null
  private var verified: VerifiedMySqlConnection = null
  private var scope: MySqlAuditBootScope = null
  private var held: HeldSessionBarrier = null
  private var qualification: VerifiedSchemaQualification = null
  private var nativeHolder: WindowsMarkerIo.SourceIoHolder = null
  private var nativeIo: WindowsMarkerIo = null
  private var store: Store = null
  private var source: Sources.SourceFacts = null
  private var sourceSha: String = null
  private var owner: Records.OwnerData = null
  private var marker: Records.MarkerData = null
  private var markerPublication: WindowsDbFirstMarkerAdoption.AdoptionData = null
  private val owners = mutable.Map.empty[UUID, Records.OwnerData]
  private val entries = mutable.ArrayBuffer.empty[Entry]
  private val readers = mutable.ArrayBuffer.empty[MySqlDbFirstBootstrapSnapshotJdbc]
  private val graphs = mutable.ArrayBuffer.empty[MySqlFormat3ReadBudget]
  private val nativePreparationSources=mutable.AnyRefMap.empty[MySqlScriptSchemaSnapshot,NativePreparationSource]
  private var nativePreparationSourceFee=0L
  private val dmlOwners = mutable.ArrayBuffer.empty[MySqlDbFirstBootstrapDml]
  private var pendingReader: MySqlDbFirstBootstrapSnapshotJdbc = null
  private var pendingPayload: MySqlScriptSchemaCanonicalPayload = null
  private var pendingBody: Array[Byte] = null
  private var recordFee = 0L
  private var rollback: MySqlRollbackOnlyState = null
  private var gate: MySqlConnectionTransactionGate.Owner = null
  private var gateHeld = false
  private var transactionAttempted = false
  private var isolationAttempted = false
  private var oldIsolation: Option[Int] = None
  private var commitUnknown = false
  private var sqlStatement: PreparedStatement = null
  private var sqlRows: ResultSet = null
  private var sqlStatementCloseAttempted = false
  private var sqlRowsCloseAttempted = false
  private var sqlFee = 0L
  private var sourceAck: Store.DurableArtifact = null
  private var ownerAck: Store.DurableArtifact = null
  private var completeAck: Store.DurableArtifact = null
  private var rollbackAttempted = false
  private var rollbackConfirmed = false
  private var autoCommitRestoreAttempted = false
  private var isolationRestoreAttempted = false

  private def requireOwner(): Unit =
    require((Thread.currentThread() eq thread) && !root.isClosed && prepaidHolderBytes >= 524288L,
      "original precharged FirstBinding6 worker/root required")
    root.requireScriptDataInputs(full27)

  private def requireNativePreparationSource(source:NativePreparationSource,
      image:MySqlScriptSchemaSnapshot,originalRoot:MySqlFormat3ReadBudget):Unit =
    requireOwner()
    require(started && !closed && store!=null && (originalRoot eq root) && image!=null &&
      nativePreparationSources.get(image).exists(_ eq source) &&
      image.allocationBudget.isCanonicalDataChildOf(root) && !image.allocationBudget.isClosed &&
      (image.inputs eq full27),"original decoded native witness/image/root required")

  def resourceObservation: ResourceObservation =
    requireOwner()
    ResourceObservation(started, completed, nativeHolder != null || store != null,
      pendingReader != null || readers.nonEmpty || graphs.nonEmpty || pendingPayload != null,
      dmlOwners.exists(_.hasUnresolvedStatement) || sqlStatement != null || sqlRows != null || sqlFee != 0L,
      gateHeld, commitUnknown, closed, Option(failure))

  def hasCompletedWithBarrier(actual: VerifiedMySqlConnection, actualScope: MySqlAuditBootScope,
      actualHeld: HeldSessionBarrier): Boolean =
    requireOwner()
    completed && !closed && !commitUnknown && !gateHeld && (actual eq verified) &&
      (actualScope eq scope) && (actualHeld eq held) && held != null && store != null

  def ensureCertified(actual: VerifiedMySqlConnection, actualScope: MySqlAuditBootScope,
      initial: VerifiedSchemaQualification): VerifiedSchemaQualification =
    requireOwner()
    require(!started && !closed && inputs.managedFirstBindingAllowed && actual != null && actualScope != null && initial != null,
      "one original managed FirstBinding6 invocation required")
    started = true; verified = actual; scope = actualScope; qualification = initial
    try
      held = session.bootstrapAcquireBarrier(this, verified, scope, control.barrierWait())
      nativeHolder = WindowsMarkerIo.SourceIoHolder.reserveDbFirst(root)
      nativeIo = nativeHolder.initialize()
      store = new Store(nativeIo, nativeHolder.dbFirstAllocation(), control.worldPath(), plan,
        new WindowsWorldProvisioner.BootIdentity(identity.bootId, identity.bootGeneration))
      store.initialize()
      val keys = store.discoverKeys().asScala.toVector
      readOrCreateSource(keys)
      readOwners(keys)
      registerCurrentOwner()
      readHistory(keys)
      historyTail() // completed restart also requires an unambiguous, fully linked original phase history
      val complete = readComplete()
      if complete.nonEmpty then
        require(initial.physicalBinding == MySqlCertifiedPhysicalBinding(plan.bindingUuid(), 2),
          "completed FirstBinding6 source requires the actual Certified2 database")
        publishOrRebarrierMarker(requireExistingIntent = true)
        requireSameMarker(complete.get.intent().marker(), marker)
        recoverAbandonedStartup(keys, complete.get, initial)
      else
        continueInstallation()
      retireData()
      qualification = session.bootstrapFreshAudit(this)
      require(qualification.physicalBinding == MySqlCertifiedPhysicalBinding(plan.bindingUuid(), 2),
        "FirstBinding6 final actual audit did not observe exact Certified2")
      session.bootstrapVerifiedBarrier(this, verified, scope)
      completed = true
      qualification
    catch
      case cause: Throwable =>
        failure = cause
        if rollback != null then
          try rollback.mark(cause)
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def inspectAbandonedStartup(base: MySqlScriptSchemaSnapshot, original: Records.OwnerData,
      initial: VerifiedSchemaQualification, predecessorSha: String): Unit =
    require(owners.get(original.boot()).contains(original), "startup inspection lacks its exact native OWNER")
    store.requirePriorProcessEnded(original.pid(), original.creationTime())
    val (current, writer) = observe()
    val inspected = try MySqlDbFirstAbandonedStartupData.inspect(base, current, original.boot(),
      initial.writerOwnerBootObservedBeforeAcquire, initial.writerEpochObservedBeforeAcquire, writer.databaseNow)
    catch
      case unchanged:MySqlDbFirstAbandonedStartupInspection if unchanged.safeSummary=="REJECTED_NON_WRITER_DATA"=>
        if MySqlDbFirstAbandonedStartupData.hasSnapshotTail(current,writer.databaseNow) then
          var creationFailure:MySqlDbFirstAbandonedStartupInspection=null
          val creation=try
            Some(MySqlDbFirstAbandonedStartupData.inspectCommittedCreationActivation(base,current,original.boot(),
              initial.writerOwnerBootObservedBeforeAcquire,initial.writerEpochObservedBeforeAcquire,writer.databaseNow,
              root,Some(readers.last),None))
          catch
            case cause:MySqlDbFirstAbandonedStartupInspection=>creationFailure=cause;None
          creation match
            case Some(active)=>
              val preserved=active.preservedActivePods.map { (pod,version)=>s"$pod:$version" }.mkString(",")
              throw new MySqlDbFirstAbandonedStartupInspection(
                s"EXACT_COMMITTED_NEW_PREPARATION_ACTIVATION_PROCESS_ENDED boot=${original.boot()} epoch=${active.epoch} " +
                s"create=${active.create} preparation=${active.preparation} activation=${active.activation} pod=${active.pod} " +
                s"attempt=${active.attempt} footprint=${active.footprint} last=${active.lastOperation} kind=${active.lastKind} " +
                s"steps=${active.steps} chainSha256=${active.chainSha256} version=${active.version} " +
                s"preservedActivePods=$preserved pid=${original.pid()} creationTime=${original.creationTime()} " +
                s"predecessor=$predecessorSha mutation=false")
            case None=>
              var legacyFailure:Throwable=null
              val legacy=try
                Some(MySqlDbFirstAbandonedStartupData.inspectCommittedActive(base,current,original.boot(),
                  initial.writerOwnerBootObservedBeforeAcquire,initial.writerEpochObservedBeforeAcquire,writer.databaseNow,
                  root,Some(readers.last),None))
              catch
                case cause:java.sql.SQLDataException=>legacyFailure=cause;None
              legacy match
                case Some(active)=>
                  throw new MySqlDbFirstAbandonedStartupInspection(
                    s"EXACT_COMMITTED_ACTIVE_CHAIN_PROCESS_ENDED boot=${original.boot()} epoch=${active.epoch} " +
                    s"create=${active.create} preparation=${active.preparation} activation=${active.activation} pod=${active.pod} " +
                    s"attempt=${active.attempt} footprint=${active.footprint} last=${active.lastOperation} kind=${active.lastKind} " +
                    s"steps=${active.steps} chainSha256=${active.chainSha256} version=${active.version} " +
                    s"pid=${original.pid()} creationTime=${original.creationTime()} predecessor=$predecessorSha mutation=false")
                case None=>
                  val unsupported=new MySqlDbFirstAbandonedStartupInspection(
                    "UNSUPPORTED_NOT_EXACT_CREATION_ACTIVATION_CHAIN",creationFailure)
                  if legacyFailure!=null then unsupported.addSuppressed(legacyFailure)
                  throw unsupported
        else
          val prepared=MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(base,current,original.boot(),
            initial.writerOwnerBootObservedBeforeAcquire,initial.writerEpochObservedBeforeAcquire,writer.databaseNow,root,
            Some(readers.last))
          throw new MySqlDbFirstAbandonedStartupInspection(
            s"EXACT_COMMITTED_PREPARATION_PROCESS_ENDED boot=${original.boot()} epoch=${prepared.epoch} " +
            s"create=${prepared.create} preparation=${prepared.preparation} pod=${prepared.pod} " +
            s"attempt=${prepared.attempt} footprint=${prepared.footprint} " +
            s"pid=${original.pid()} creationTime=${original.creationTime()} predecessor=$predecessorSha mutation=false")
    throw new MySqlDbFirstAbandonedStartupInspection(
      s"EXACT_ACQUIRE_RENEW_ONLY_PROCESS_ENDED boot=${original.boot()} epoch=${inspected.writerEpoch} " +
        s"pid=${original.pid()} creationTime=${original.creationTime()} predecessor=$predecessorSha mutation=false")

  private def nativeRef(ack: Store.DurableArtifact): Records.WitnessRef =
    new Records.WitnessRef(ack.fileId(), ack.sha256(), ack.byteLength().toLong)

  private def requireNativeRef(key: Store.Key, expected: Records.WitnessRef): Unit =
    val read = requireArtifact(key)
    try require(nativeRef(read.acknowledgement()) == expected, "startup incident native FileId/hash/length changed")
    finally read.close()

  private def scopedStartupRequest(failedBoot: UUID): Option[Retirement.RequestData] =
    val fee = 65536L
    root.reserveTemporaryMemory(fee, "bounded exact-boot private incident read/codec BEFORE allocation")
    var primary: Throwable = null
    try
      val result = control.readStartupRetirementRequest(failedBoot)
      if result.isPresent then Some(result.get()) else None
    catch
      case cause: Throwable => primary = cause; throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,
      Vector(() => root.releaseTemporaryMemory(fee, "exact-boot incident read workspace retired")))

  private def recoverAbandonedStartup(keys: Vector[Store.Key], complete: Records.SettlementData,
      initial: VerifiedSchemaQualification): Unit =
    val intentKeys = keys.filter(_.role() == Store.Role.STARTUP_RETIRE_INTENT)
    val incidentKeys = keys.filter(_.role() == Store.Role.STARTUP_INCIDENT)
    val artifactRoles = Set(Store.Role.STARTUP_RETIRE_BEFORE, Store.Role.STARTUP_RETIRE_TARGET, Store.Role.STARTUP_RETIRE_CONFIRMED)
    require(keys.filter(key => artifactRoles(key.role())).forall(key => intentKeys.exists(_.ownerBoot() == key.ownerBoot())),
      "orphan startup retirement artifact requires reconciliation")
    // discoverKeys 已受原 source maxMembers 约束；每个保留的解码记录先向同 root 收费。
    val history = intentKeys.map { key =>
      reserveRecord()
      val read = requireArtifact(key)
      try
        val intent = codecWorkspace { read.decode(bytes => Retirement.decodeIntent(bytes)) }
        require(intent.executor().boot() == key.ownerBoot() && owners.get(intent.executor().boot()).contains(intent.executor()),
          "retirement executor differs from its exact native OWNER")
        (key, intent, read.acknowledgement())
      finally read.close()
    }
    Retirement.requireUniqueIdentities(history.map(_._2).asJava)
    var remaining = history
    var base = historyTail().getOrElse(throw new SQLDataException("startup retirement requires original B1 RELEASE tail"))
    var baseAck = completeAck
    var baseKey = Store.Key.of(Store.Role.COMPLETE)
    val consumedIncidents = mutable.Set.empty[UUID]
    var walking = true
    while walking do
      val successor = Retirement.successor(remaining.map(_._2).asJava, baseAck.sha256())
      if successor.isEmpty then walking = false
      else
        val (key, intent, intentAck) = remaining.find(_._2 == successor.get()).get
        validateStartupIncident(intent.incident(), complete, baseAck, baseKey)
        val failedBoot = intent.incident().failedOwner().boot()
        val incidentRead = requireArtifact(new Store.Key(Store.Role.STARTUP_INCIDENT, failedBoot))
        try
          require(incidentRead.acknowledgement().sha256() == intent.incidentSha256() &&
            codecWorkspace { incidentRead.decode(bytes => Retirement.decodeIncident(bytes)) } == intent.incident(),
            "retirement intent differs from actual incident acceptance")
        finally incidentRead.close()
        consumedIncidents += failedBoot
        val before = readWitness(intent.before(), new Store.Key(Store.Role.STARTUP_RETIRE_BEFORE, intent.executor().boot()))
        val target = readWitness(intent.target(), new Store.Key(Store.Role.STARTUP_RETIRE_TARGET, intent.executor().boot()))
        validateStartupRetirement(intent, before, target, base)
        val settlementKey = new Store.Key(Store.Role.STARTUP_RETIRE_CONFIRMED, intent.executor().boot())
        val settledRead = store.read(settlementKey)
        if settledRead == null then
          require(remaining.size == 1 && incidentKeys.size == consumedIncidents.size,
            "pending retirement cannot have later or unexplained incidents")
          requireStartupRequest(intent.incident().request())
          store.requirePriorProcessEnded(intent.executor().pid(), intent.executor().creationTime())
          val (actual, writer) = observe()
          if sameData(actual, target) then
            confirmStartupRetirement(intent, intentAck, actual, writer.databaseNow, applied = true)
          else if sameData(actual, before) then
            confirmStartupRetirement(intent, intentAck, actual, writer.databaseNow, applied = false)
            throw new SQLDataException("startup retirement NOT_APPLIED; original operation cannot be automatically replayed")
          else throw new SQLDataException("startup retirement UNKNOWN; original intent/full before-target retained")
          return // 先完成唯一旧操作；本 boot 不同时执行新的退役副作用。
        try
          reserveRecord()
          val settled = codecWorkspace { settledRead.decode(bytes => Retirement.decodeSettlement(bytes)) }
          require(settled.intent() == intent && settled.intentSha256() == intentAck.sha256() &&
            owners.get(settled.observer().boot()).contains(settled.observer()), "retirement settlement intent/observer changed")
          val actual = readWitness(settled.actual(), readbackKey(settled.observer(), settled.actual().sha256()))
          requireStartupObserver(actual, settled.observer())
          require(sameData(actual, if settled.applied() then target else before), "retirement settlement Full27 differs")
          require(settled.applied(), "NOT_APPLIED retirement cannot be a predecessor or automatically replayed")
          base = actual
          baseAck = settledRead.acknowledgement()
          baseKey = settlementKey
        finally settledRead.close()
        remaining = remaining.filterNot(_._1 == key)
    require(remaining.isEmpty, "startup retirement has an unreachable predecessor, skipped node or cycle")
    val unpublished = incidentKeys.filterNot(key => consumedIncidents(key.ownerBoot()))
    if initial.writerOwnerBootObservedBeforeAcquire.isEmpty then
      require(unpublished.isEmpty, "unsettled accepted startup incident requires reconciliation")
      return // 不用当前正常业务图与历史空图比较。
    val failedBoot = initial.writerOwnerBootObservedBeforeAcquire.get
    val original = owners.getOrElse(failedBoot, throw new SQLDataException("occupied W has no actual native OWNER"))
    val requested = scopedStartupRequest(failedBoot)
    if requested.isEmpty then
      require(unpublished.isEmpty, "accepted incident lacks its exact current operator scope")
      inspectAbandonedStartup(base, original, initial, baseAck.sha256())
      return
    val request = requested.get
    require(!consumedIncidents(failedBoot) && !history.exists(_._2.incident().request().operationUuid() == request.operationUuid()),
      "consumed startup incident cannot retire another writer")
    require(request.predecessorReceiptSha256() == baseAck.sha256(), "new incident does not name the unique applied chain tip")
    require(unpublished.forall(_.ownerBoot() == failedBoot), "unrelated accepted startup incident remains unresolved")
    store.requirePriorProcessEnded(original.pid(), original.creationTime())
    val (before, writer) = observe()
    inspectRetirementData(request,base,before,original.boot(),
      initial.writerOwnerBootObservedBeforeAcquire,initial.writerEpochObservedBeforeAcquire,writer.databaseNow)
    val oldOwnerRead = requireArtifact(new Store.Key(Store.Role.OWNER, original.boot()))
    reserveRecord()
    val proposedIncident = try
      val requestSha = codecWorkspace {
        pendingBody = Retirement.encodeRequest(request)
        HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(pendingBody))
      }
      new Retirement.IncidentData(request, requestSha, original, owner, writer.writerEpoch,
        nativeRef(sourceAck), nativeRef(oldOwnerRead.acknowledgement()), nativeRef(completeAck), nativeRef(baseAck))
    finally oldOwnerRead.close()
    val incident = if unpublished.nonEmpty then
      val read = requireArtifact(unpublished.head)
      try
        val retained = codecWorkspace { read.decode(bytes => Retirement.decodeIncident(bytes)) }
        require(retained.request() == request, "existing startup incident belongs to another operator request")
        retained
      finally read.close()
    else proposedIncident
    validateStartupIncident(incident, complete, baseAck, baseKey)
    val incidentAck = codecWorkspace {
      pendingBody = Retirement.encodeIncident(incident)
      store.publish(new Store.Key(Store.Role.STARTUP_INCIDENT, original.boot()), pendingBody)
    }
    val target = MySqlDbFirstAbandonedStartupData.retirementTarget(before, writer.databaseNow, root)
    graphs += target.allocationBudget
    val beforeRef = witness(new Store.Key(Store.Role.STARTUP_RETIRE_BEFORE, owner.boot()), before)
    val targetRef = witness(new Store.Key(Store.Role.STARTUP_RETIRE_TARGET, owner.boot()), target)
    reserveRecord()
    val intent = new Retirement.IntentData(incident, incidentAck.sha256(), owner, held.namespace.lockName(),
      held.actualConnectionId, held.connectionGenerationId, writer.databaseNow, beforeRef, targetRef)
    validateStartupRetirement(intent, before, target, base)
    val intentAck = codecWorkspace {
      pendingBody = Retirement.encodeIntent(intent)
      store.publish(new Store.Key(Store.Role.STARTUP_RETIRE_INTENT, owner.boot()), pendingBody)
    }
    sourceAck.rebarrier(); completeAck.rebarrier(); baseAck.rebarrier(); ownerAck.rebarrier(); incidentAck.rebarrier(); intentAck.rebarrier()
    requireNativeRef(new Store.Key(Store.Role.STARTUP_RETIRE_BEFORE, owner.boot()), beforeRef)
    requireNativeRef(new Store.Key(Store.Role.STARTUP_RETIRE_TARGET, owner.boot()), targetRef)
    val (locked, lockedWriter) = readCurrent()
    require(sameData(locked, before) && locked.connection == before.connection &&
      locked.observationScope == before.observationScope && locked.maxAllowedPacketBytes == before.maxAllowedPacketBytes,
      "startup retirement locked Full27 changed since durable intent")
    // post-service 事故也只退休死亡 W，不结算旧 Anvil 责任。最终锁定读必须重新检查实际 DB 时钟和完整业务图。
    inspectRetirementData(request,base,locked,original.boot(),
      initial.writerOwnerBootObservedBeforeAcquire,initial.writerEpochObservedBeforeAcquire,lockedWriter.databaseNow)
    store.requirePriorProcessEnded(original.pid(),original.creationTime())
    require(!lockedWriter.databaseNow.isBefore(writer.databaseNow),
      "startup retirement database clock moved backwards after durable intent")
    store.requirePriorProcessEnded(original.pid(), original.creationTime())
    val dml = new MySqlDbFirstBootstrapDml(full27, root)
    dmlOwners += dml
    dml.execute(verified, scope, gate, rollback, locked, target, MySqlDbFirstBootstrapDml.Phase.StartupRetirement)
    try verified.connection.commit()
    catch
      case cause: Throwable =>
        commitUnknown = true
        throw new java.sql.SQLTransientConnectionException("startup retirement commit UNKNOWN; original intent retained", cause)
    restoreIdle()
    val (actual, actualWriter) = observe()
    require(sameData(actual, target), "startup retirement post-commit Full27 differs; original intent retained")
    confirmStartupRetirement(intent, intentAck, actual, actualWriter.databaseNow, applied = true)

  private def requireStartupRequest(request: Retirement.RequestData): Unit =
    require(scopedStartupRequest(request.failedBoot()).contains(request),
      "the exact current operator startup incident is required")

  private def validateStartupIncident(incident: Retirement.IncidentData, complete: Records.SettlementData,
      baseAck: Store.DurableArtifact, baseKey: Store.Key): Unit =
    if incident.request().formatVersion() == 1 then
      require(baseKey == Store.Key.of(Store.Role.COMPLETE) && incident.failedOwner() == complete.intent().owner(),
        "legacy v1 incident must retain its original COMPLETE owner/root")
    else require(baseKey.role() == Store.Role.STARTUP_RETIRE_CONFIRMED,
      "v2 chained incident must follow the exact verified applied retirement")
    require(owners.get(incident.failedOwner().boot()).contains(incident.failedOwner()) &&
      owners.get(incident.acceptedBy().boot()).contains(incident.acceptedBy()) && incident.sourceSha256() == sourceSha &&
      incident.request().completeSha256() == completeAck.sha256(), "startup incident differs from original completed binding")
    require(incident.request().predecessorReceiptSha256() == baseAck.sha256() &&
      incident.predecessorNative() == nativeRef(baseAck), "startup incident predecessor FileId/hash/length changed")
    requireNativeRef(baseKey, incident.predecessorNative())
    requireNativeRef(Store.Key.of(Store.Role.SOURCE), incident.sourceNative())
    requireNativeRef(Store.Key.of(Store.Role.COMPLETE), incident.completeNative())
    requireNativeRef(new Store.Key(Store.Role.OWNER, incident.failedOwner().boot()), incident.ownerNative())
    val digest = codecWorkspace {
      pendingBody = Retirement.encodeRequest(incident.request())
      HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(pendingBody))
    }
    require(digest == incident.requestSha256(), "startup incident operator bytes changed")

  private def requireStartupObserver(actual: MySqlScriptSchemaSnapshot, observer: Records.OwnerData): Unit =
    require(actual.connection.serverUuid == installed.serverUuid() && actual.schema == plan.schema() &&
      actual.observationScope.bootUuid == observer.boot() && actual.observationScope.bootGeneration == observer.generation(),
      "startup retirement snapshot does not belong to its exact native observer")

  private def validateStartupRetirement(intent: Retirement.IntentData, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, base: MySqlScriptSchemaSnapshot): Unit =
    requireStartupObserver(before, intent.executor())
    require(intent.barrier() == held.namespace.lockName(), "startup retirement barrier namespace differs")
    val inspected = inspectRetirementData(intent.incident().request(),base,before,intent.incident().failedOwner().boot(),
      Some(intent.incident().failedOwner().boot()), intent.incident().writerEpoch(), intent.databaseNow())
    require(inspected.writerEpoch == intent.incident().writerEpoch(), "startup retirement incident epoch differs")
    MySqlDbFirstAbandonedStartupData.requireRetirementTarget(before, target, intent.databaseNow())

  /** v3核原P，v4只核实测的单一A；旧事故仍须unchanged，均不重放业务。 */
  private def inspectRetirementData(request:Retirement.RequestData,base:MySqlScriptSchemaSnapshot,
      current:MySqlScriptSchemaSnapshot,originalBoot:UUID,auditedOwner:Option[UUID],epoch:Long,
      now:LocalDateTime):MySqlBusinessWriterRow =
    if request.formatVersion()==5 then
      val native=nativePreparationSources.get(current)
      val active=MySqlDbFirstAbandonedStartupData.inspectCommittedCreationActivation(base,current,originalBoot,
        auditedOwner,epoch,now,root,if native.isEmpty then Some(readers.last) else None,native)
      val expected=request.activeChainIdentity().orElseThrow()
      if !request.failedCreateOperationUuid().orElseThrow().equals(active.create) ||
          !request.preparationOperationUuid().orElseThrow().equals(active.preparation) ||
          !expected.podUuid().equals(active.pod) || !expected.activationOperationUuid().equals(active.activation) ||
          expected.writerEpoch()!=active.epoch || expected.chainSha256()!=active.chainSha256 ||
          active.steps!=2L || active.lastOperation!=active.activation ||
          active.lastKind!=MySqlDbFirstSchemaDefinition.InitialActivationStep || active.version!=1L then
        throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_NEW_PREPARATION_ACTIVATION_OPERATOR_IDENTITIES")
      MySqlDbFirstBootstrapProof.writer(current,now)
    else if request.formatVersion()==4 then
      val native=nativePreparationSources.get(current)
      val active=MySqlDbFirstAbandonedStartupData.inspectCommittedActive(base,current,originalBoot,
        auditedOwner,epoch,now,root,if native.isEmpty then Some(readers.last) else None,native)
      val expected=request.activeChainIdentity().orElseThrow()
      if !request.failedCreateOperationUuid().orElseThrow().equals(active.create) ||
          !request.preparationOperationUuid().orElseThrow().equals(active.preparation) ||
          !expected.podUuid().equals(active.pod) || !expected.activationOperationUuid().equals(active.activation) ||
          expected.writerEpoch()!=active.epoch || expected.chainSha256()!=active.chainSha256 ||
          active.steps!=1L || active.lastOperation!=active.activation ||
          active.lastKind!=MySqlDbFirstSchemaDefinition.InitialActivationStep || active.version!=1L then
        throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_SINGLE_ACTIVATION_OPERATOR_IDENTITIES")
      MySqlDbFirstBootstrapProof.writer(current,now)
    else if request.formatVersion()!=3 then
      MySqlDbFirstAbandonedStartupData.inspect(base,current,originalBoot,auditedOwner,epoch,now)
    else
      val native=nativePreparationSources.get(current)
      val prepared=MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(base,current,originalBoot,
        auditedOwner,epoch,now,root,if native.isEmpty then Some(readers.last) else None,native)
      if !request.failedCreateOperationUuid().orElseThrow().equals(prepared.create) ||
          !request.preparationOperationUuid().orElseThrow().equals(prepared.preparation) then
        throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_PREPARATION_OPERATOR_IDENTITIES")
      MySqlDbFirstBootstrapProof.writer(current,now)

  private def confirmStartupRetirement(intent: Retirement.IntentData, intentAck: Store.DurableArtifact,
      actual: MySqlScriptSchemaSnapshot, now: LocalDateTime, applied: Boolean): Unit =
    require(!gateHeld && !commitUnknown, "startup retirement settlement requires settled actual observer")
    requireStartupObserver(actual, owner)
    val actualRef = witness(readbackKey(owner, actual.canonicalSha256Hex), actual)
    reserveRecord()
    val settled = new Retirement.SettlementData(intent, intentAck.sha256(), owner, held.namespace.lockName(),
      held.actualConnectionId, held.connectionGenerationId, now, actualRef, applied)
    codecWorkspace {
      pendingBody = Retirement.encodeSettlement(settled)
      store.publish(new Store.Key(Store.Role.STARTUP_RETIRE_CONFIRMED, intent.executor().boot()), pendingBody)
    }
    System.out.println(s"FloatingPod startup retirement settled operation=${intent.incident().request().operationUuid()} applied=$applied")

  private def readOrCreateSource(keys: Vector[Store.Key]): Unit =
    val existing = store.read(Store.Key.of(Store.Role.SOURCE))
    if existing != null then
      try
        source = codecWorkspace { existing.decode(bytes => Sources.decode(bytes)) }
        sourceAck = existing.acknowledgement(); sourceSha = sourceAck.sha256()
      finally existing.close()
    else
      require(qualification.physicalBinding == MySqlNoPhysicalBinding && keys.isEmpty,
        "FirstBinding6 missing source receipt cannot reinterpret existing B1/Certified/history as Fresh")
      source = store.captureOriginalSource(inputs.configurationSha256.toLowerCase(java.util.Locale.ROOT),
        inputs.auditExpectation.objects.canonicalSha256.toLowerCase(java.util.Locale.ROOT))
      codecWorkspace {
        pendingBody = Sources.encode(source)
        sourceAck = store.publish(Store.Key.of(Store.Role.SOURCE), pendingBody)
        sourceSha = sourceAck.sha256()
        pendingBody = null
      }
    store.requireOriginalSource(source)
    if source.creationBoot().bootId() != identity.bootId then
      store.requirePriorProcessEnded(source.creatorPid(), source.creatorCreationTime())
    require(source.plan() == plan && source.configurationSha256().equalsIgnoreCase(inputs.configurationSha256) &&
      source.securityGuardSha256().equalsIgnoreCase(inputs.auditExpectation.objects.canonicalSha256),
      "FirstBinding6 original source differs from the audited managed identity/configuration/security")

  private def readOwners(keys: Vector[Store.Key]): Unit =
    keys.filter(_.role() == Store.Role.OWNER).foreach { key =>
      val read = requireArtifact(key)
      try
        reserveRecord()
        val previous = codecWorkspace { read.decode(bytes => Records.decodeOwner(bytes)) }
        require(previous.boot() == key.ownerBoot() && previous.sourceReceiptSha256() == sourceSha && !owners.contains(previous.boot()),
          "FirstBinding6 durable owner/source/key differs")
        owners.put(previous.boot(), previous)
        if previous.boot() == identity.bootId then
          require(previous.generation() == identity.bootGeneration, "FirstBinding6 current owner generation changed")
        else store.requirePriorProcessEnded(previous.pid(), previous.creationTime())
      finally read.close()
    }

  private def registerCurrentOwner(): Unit =
    val process = nativeIo.captureCurrentProcessIdentity(plan.bootstrapOperationUuid())
    owner = new Records.OwnerData(identity.bootId, identity.bootGeneration, process.pid(), process.creationFileTime(), sourceSha)
    owners.get(identity.bootId).foreach(old => require(old == owner, "FirstBinding6 current native owner differs"))
    codecWorkspace {
      pendingBody = Records.encodeOwner(owner)
      ownerAck = store.publish(new Store.Key(Store.Role.OWNER, owner.boot()), pendingBody)
      pendingBody = null
    }
    owners.put(owner.boot(), owner)

  private def readHistory(keys: Vector[Store.Key]): Unit =
    keys.filter(key => intentRoles.contains(key.role())).foreach { key =>
      val read = requireArtifact(key)
      try
        reserveRecord()
        val record = codecWorkspace { read.decode(bytes => Records.decodeRecord(bytes)) }
        require(record.kind() == Records.Kind.INTENT && record.owner().boot() == key.ownerBoot() &&
          intentRole(record.phase()) == key.role() && owners.get(record.owner().boot()).contains(record.owner()),
          "FirstBinding6 original phase/owner/key differs")
        val before = readWitness(record.beforeFull27(), new Store.Key(beforeRole(record.phase()), record.owner().boot()))
        val target = readWitness(record.targetFull27(), new Store.Key(targetRole(record.phase()), record.owner().boot()))
        val proof = MySqlDbFirstBootstrapProof.validate(record, before, target, source, sourceSha, full27, root, quotaMaximum)
        val entry = new Entry(record, read.acknowledgement(), proof, before, target)
        entries += entry
        val confirmed = store.read(new Store.Key(confirmationRole(record.phase()), record.owner().boot()))
        if confirmed != null then
          try
            val settled = codecWorkspace { confirmed.decode(bytes => Records.decodeSettlement(bytes)) }
            require(settled.intent() == record && settled.intentSha256() == entry.intentAck.sha256() &&
              owners.get(settled.observer().boot()).contains(settled.observer()), "FirstBinding6 settlement intent/observer differs")
            val actual = readWitness(settled.actualFull27(), readbackKey(settled.observer(), settled.actualFull27().sha256()))
            MySqlDbFirstBootstrapProof.validateSettlement(settled, proof, actual, root)
            entry.settlement = settled
          finally confirmed.close()
      finally read.close()
    }

  private def readComplete(): Option[Records.SettlementData] =
    val read = store.read(Store.Key.of(Store.Role.COMPLETE))
    if read == null then None
    else
      try
        val result = codecWorkspace { read.decode(bytes => Records.decodeSettlement(bytes)) }
        completeAck = read.acknowledgement()
        require(result.intent().phase() == Records.Phase.RELEASE && result.outcome() == Records.Outcome.APPLIED &&
          entries.forall(_.settlement != null) && entries.exists(entry => entry.settlement == result),
          "FirstBinding6 completion lacks its exact settled applied RELEASE history")
        Some(result)
      finally read.close()

  private def codecWorkspace[A](body: => A): A =
    val fee = 2097152L
    root.reserveTemporaryMemory(fee, "FirstBinding6 bounded record codec/defensive copy workspace BEFORE encoding")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      pendingBody = null
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => root.releaseTemporaryMemory(fee, "FirstBinding6 record codec workspace retired")))

  private def reserveRecord(): Unit =
    root.reserveRetainedMemory(16384L, "FirstBinding6 closed record/owner/proof metadata BEFORE retention")
    recordFee = Math.addExact(recordFee, 16384L)

  private def requireArtifact(key: Store.Key): Store#ReadArtifact =
    val read = store.read(key)
    if read == null then throw new SQLDataException("FirstBinding6 required immutable artifact is absent: " + key.role())
    read

  private def readWitness(reference: Records.WitnessRef, key: Store.Key): MySqlScriptSchemaSnapshot =
    val read = requireArtifact(key)
    try
      val ack = read.acknowledgement()
      require(reference.fileId() == ack.fileId() && reference.sha256() == ack.sha256() && reference.byteLength() == ack.byteLength(),
        "FirstBinding6 actual native witness FileId/hash/length differs from original reference")
      read.decode { bytes =>
        pendingPayload = MySqlScriptSchemaSnapshotCodec.capture(bytes, full27, root)
        val image = MySqlScriptSchemaSnapshotCodec.decode(pendingPayload, full27, root)
        graphs += image.allocationBudget
        root.reserveRetainedMemory(256L,"actual native witness/source identity binding BEFORE retention")
        nativePreparationSourceFee=Math.addExact(nativePreparationSourceFee,256L)
        nativePreparationSources.put(image,new NativePreparationSource(this,image,root,ack))
        pendingPayload.storageBudget.closeScopedReadBudget(); pendingPayload = null
        image
      }
    finally read.close()

  private def witness(key: Store.Key, image: MySqlScriptSchemaSnapshot): Records.WitnessRef =
    require(pendingPayload == null && pendingBody == null, "previous FirstBinding6 payload responsibility remains")
    pendingPayload = MySqlScriptSchemaSnapshotCodec.encode(image, root)
    pendingBody = pendingPayload.bytes.copyBytes(pendingPayload.storageBudget, "FirstBinding6 complete witness transfer array")
    val ack = store.publish(key, pendingBody)
    require(ack.sha256().equalsIgnoreCase(image.canonicalSha256Hex) && ack.byteLength().toLong == image.canonicalByteLength,
      "FirstBinding6 encoded witness differs from its complete Full27 image")
    val result = new Records.WitnessRef(ack.fileId(), ack.sha256(), ack.byteLength().toLong)
    pendingBody = null
    pendingPayload.storageBudget.closeScopedReadBudget(); pendingPayload = null
    result

  private def readbackKey(actualOwner: Records.OwnerData, sha: String): Store.Key =
    new Store.Key(Store.Role.READBACK, actualOwner.boot(), sha.toLowerCase(java.util.Locale.ROOT))

  private def beginRead(): Unit =
    require(!gateHeld && !transactionAttempted && !commitUnknown && sqlStatement == null && sqlRows == null,
      "FirstBinding6 requires the original idle transaction slot")
    session.bootstrapVerifiedBarrier(this, verified, scope)
    qualification = session.bootstrapFreshAudit(this)
    val connection = verified.connection
    require(connection.getAutoCommit, "FirstBinding6 starts only from idle auto-commit")
    oldIsolation = Some(connection.getTransactionIsolation)
    rollback = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollback) match
      case Some(cause) => throw cause
      case None => gateHeld = true
    gate = MySqlConnectionTransactionGate.currentOwner(connection, rollback)
    rollbackAttempted = false; rollbackConfirmed = false
    autoCommitRestoreAttempted = false; isolationRestoreAttempted = false
    isolationAttempted = true
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
    transactionAttempted = true
    connection.setAutoCommit(false)
    require(!connection.getAutoCommit && connection.getTransactionIsolation == Connection.TRANSACTION_REPEATABLE_READ,
      "FirstBinding6 did not enter its original RR transaction")

  private def readCurrent(): (MySqlScriptSchemaSnapshot, MySqlBusinessWriterRow) =
    beginRead()
    try pendingReader = MySqlDbFirstBootstrapSnapshotJdbc.create(inputs, root, identity)
    catch
      case creation: MySqlDbFirstBootstrapSnapshotJdbc.CreationFailure =>
        pendingReader = creation.retainedHolder
        throw creation
    readers += pendingReader
    val reader = pendingReader
    pendingReader = null
    val image = qualification.physicalBinding match
      case MySqlCertifiedPhysicalBinding(binding, 2) if binding == plan.bindingUuid() =>
        reader.readCertifiedReadbackLocked(verified, scope, qualification, gate, rollback)
      case _ => reader.readLocked(verified, scope, qualification, gate, rollback)
    val now = databaseNow()
    (image, MySqlDbFirstBootstrapProof.writer(image, now))

  private def observe(): (MySqlScriptSchemaSnapshot, MySqlBusinessWriterRow) =
    val result = readCurrent()
    finishRead()
    result

  private def databaseNow(): LocalDateTime =
    require(gateHeld && sqlStatement == null && sqlRows == null && sqlFee == 0L,
      "FirstBinding6 database clock requires the original idle query slot")
    MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollback)
    root.reserveTemporaryMemory(8192L, "FirstBinding6 actual database clock statement/result BEFORE open")
    sqlFee = 8192L
    sqlRowsCloseAttempted = false; sqlStatementCloseAttempted = false
    var primary: Throwable = null
    try
      sqlStatement = verified.connection.prepareStatement("SELECT UTC_TIMESTAMP(6)")
      sqlRows = sqlStatement.executeQuery()
      MySqlFencedTransactionSupport.one(sqlRows, "FirstBinding6 actual UTC database clock") { row =>
        MySqlFencedTransactionSupport.requiredLocalDateTime(row, 1, "FirstBinding6 databaseNow")
      }
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try closeClock()
      catch case cleanup: Throwable =>
        if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
        else throw cleanup

  private def closeClock(): Unit =
    if sqlRows != null then
      require(!sqlRowsCloseAttempted, "FirstBinding6 clock ResultSet close remains unknown")
      sqlRowsCloseAttempted = true; sqlRows.close(); sqlRows = null
    if sqlStatement != null then
      require(!sqlStatementCloseAttempted, "FirstBinding6 clock statement close remains unknown")
      sqlStatementCloseAttempted = true; sqlStatement.close(); sqlStatement = null
    if sqlFee != 0L then
      root.releaseTemporaryMemory(sqlFee, "FirstBinding6 actual database clock handles detached")
      sqlFee = 0L

  private def finishRead(): Unit =
    require(gateHeld && !commitUnknown, "original known FirstBinding6 read transaction required")
    if !rollbackConfirmed then
      require(!rollbackAttempted, "FirstBinding6 rollback outcome remains unknown")
      rollbackAttempted = true
      verified.connection.rollback()
      rollbackConfirmed = true
    restoreIdle()

  private def restoreIdle(): Unit =
    val connection = verified.connection
    require(!autoCommitRestoreAttempted && !isolationRestoreAttempted && oldIsolation.nonEmpty,
      "FirstBinding6 idle restoration is one-shot")
    autoCommitRestoreAttempted = true
    connection.setAutoCommit(true)
    isolationRestoreAttempted = true
    connection.setTransactionIsolation(oldIsolation.get)
    require(connection.getAutoCommit && connection.getTransactionIsolation == oldIsolation.get,
      "FirstBinding6 idle transaction restoration differs")
    MySqlConnectionTransactionGate.leave(connection, rollback)
    gateHeld = false; gate = null; rollback = null
    transactionAttempted = false; isolationAttempted = false; oldIsolation = None

  private def planPhase(phase: Records.Phase, before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot, now: LocalDateTime): Entry =
    require(!gateHeld && !commitUnknown, "FirstBinding6 intent publication requires settled planning read")
    graphs += target.allocationBudget
    val beforeRef = witness(new Store.Key(beforeRole(phase), owner.boot()), before)
    val targetRef = witness(new Store.Key(targetRole(phase), owner.boot()), target)
    val operation = phase match
      case Records.Phase.PREPARE => plan.prepareWriteUuid()
      case Records.Phase.CERTIFY => plan.certifyWriteUuid()
      case _ => UUID.randomUUID()
    val record = new Records.RecordData(phase, Records.Kind.INTENT, operation, sourceSha, owner,
      held.namespace.lockName(), held.actualConnectionId, now, held.connectionGenerationId, beforeRef, targetRef, null,
      if phase == Records.Phase.PREPARE then null else marker)
    reserveRecord()
    val proof = MySqlDbFirstBootstrapProof.validate(record, before, target, source, sourceSha, full27, root, quotaMaximum)
    val ack = codecWorkspace {
      pendingBody = Records.encodeRecord(record)
      val value = store.publish(new Store.Key(intentRole(phase), owner.boot()), pendingBody)
      pendingBody = null
      value
    }
    val result = new Entry(record, ack, proof, before, target)
    entries += result
    result

  private def executePhase(entry: Entry): Unit =
    require(entry.settlement == null && entry.record.owner() == owner && !gateHeld,
      "original current-owner unsettled FirstBinding6 plan required")
    sourceAck.rebarrier(); ownerAck.rebarrier(); entry.intentAck.rebarrier()
    // 完整永久见证再读回；仅核 intent 或单个 W slot 不足以准许 DML。
    val before = readWitness(entry.record.beforeFull27(), new Store.Key(beforeRole(entry.record.phase()), owner.boot()))
    val target = readWitness(entry.record.targetFull27(), new Store.Key(targetRole(entry.record.phase()), owner.boot()))
    MySqlDbFirstBootstrapProof.validate(entry.record, before, target, source, sourceSha, full27, root, quotaMaximum)
    val (locked, _) = readCurrent()
    require(sameData(locked, before) && locked.connection == before.connection &&
      locked.observationScope == before.observationScope && locked.maxAllowedPacketBytes == before.maxAllowedPacketBytes,
      "FirstBinding6 fresh locked Full27 differs from its exact original before image")
    val dml = new MySqlDbFirstBootstrapDml(full27, root)
    dmlOwners += dml
    val phase = entry.record.phase() match
      case Records.Phase.PREPARE => MySqlDbFirstBootstrapDml.Phase.Prepare
      case Records.Phase.CERTIFY => MySqlDbFirstBootstrapDml.Phase.Certify
      case Records.Phase.HANDOFF => MySqlDbFirstBootstrapDml.Phase.Handoff
      case Records.Phase.RELEASE => MySqlDbFirstBootstrapDml.Phase.Release
    dml.execute(verified, scope, gate, rollback, locked, target, phase)
    try verified.connection.commit()
    catch
      case cause: Throwable =>
        commitUnknown = true
        throw MySqlCommitUnknownException(entry.proof.context, cause)
    // commit 返回仍不等于业务完成；恢复 idle、重新审计并锁读全部27表后才能确认。
    restoreIdle()
    val (actual, writer) = observe()
    require(sameData(actual, target), "FirstBinding6 acknowledged commit full readback differs from target; original intent retained")
    confirm(entry, actual, writer.databaseNow, Records.Outcome.APPLIED)

  private def confirm(entry: Entry, actual: MySqlScriptSchemaSnapshot, now: LocalDateTime,
      outcome: Records.Outcome): Unit =
    require(!gateHeld && !commitUnknown && entry.settlement == null, "original idle FirstBinding6 confirmation slot required")
    val actualRef = witness(readbackKey(owner, actual.canonicalSha256Hex), actual)
    val settled = new Records.SettlementData(entry.record, entry.intentAck.sha256(), owner,
      held.namespace.lockName(), held.actualConnectionId, now, held.connectionGenerationId, actualRef, outcome)
    MySqlDbFirstBootstrapProof.validateSettlement(settled, entry.proof, actual, root)
    codecWorkspace {
      pendingBody = Records.encodeSettlement(settled)
      store.publish(new Store.Key(confirmationRole(entry.record.phase()), entry.record.owner().boot()), pendingBody)
      pendingBody = null
    }
    entry.settlement = settled

  private def historyTail(): Option[MySqlScriptSchemaSnapshot] =
    MySqlDbFirstBootstrapActor.historyTail(entries.iterator.map(entry =>
      HistoryPhase(entry.proof, Option(entry.settlement))).toVector)

  private def continueInstallation(): Unit =
    val historical = historyTail()
    val pending = entries.find(_.settlement == null)
    val (observed, observedWriter) = observe()
    pending match
      case Some(entry) =>
        val outcome = classifyOriginal(entry.proof, observed)
        confirm(entry, observed, observedWriter.databaseNow, outcome)
      case None => historical.foreach(expected => require(sameData(observed, expected),
        "FirstBinding6 actual database differs from its complete confirmed original history"))
    historyTail().foreach(expected => require(sameData(observed, expected),
      "FirstBinding6 reconciled complete Full27 history differs from the actual database"))

    var current = observed
    var writer = observedWriter
    if current.relation("physical_recovery_binding").rows.isEmpty then
      require(entries.forall(e => e.record.phase() == Records.Phase.PREPARE && e.settlement != null &&
        e.settlement.outcome() == Records.Outcome.NOT_APPLIED), "FirstBinding6 unbound database has unexplained B1 history")
      val targetWriter = MySqlFreshFormat3WriterTarget(plan.bindingUuid(), Math.addExact(writer.writerEpoch, 1L),
        Some(owner.boot()), newLease(writer.databaseNow), plan.prepareWriteUuid())
      val target = MySqlDbFirstPrepareComposition.prepareTarget(current, full27, root, plan,
        source.selectedSaveFileId(), shaBytes(sourceSha), writer, targetWriter, quotaMaximum)
      executePhase(planPhase(Records.Phase.PREPARE, current, target, writer.databaseNow))
      val fresh = observe(); current = fresh._1; writer = fresh._2

    qualification.physicalBinding match
      case MySqlLegacyPhysicalBinding(binding, 2) if binding == plan.bindingUuid() =>
        val owned = ensureCurrentWriter(current, writer)
        current = owned._1; writer = owned._2
        publishOrRebarrierMarker(requireExistingIntent = false)
        // Native publication may consume time. Re-read the actual lease before making CERTIFY intent.
        val fresh = observe()
        val stillOwned = ensureCurrentWriter(fresh._1, fresh._2)
        current = stillOwned._1; writer = stillOwned._2
        val targetWriter = MySqlFreshFormat3WriterTarget(plan.bindingUuid(), writer.writerEpoch,
          Some(owner.boot()), writer.leaseUntil, plan.certifyWriteUuid())
        val target = MySqlDbFirstBootstrapGraph.certifyTarget(current, full27, root, plan,
          source.selectedSaveFileId(), shaBytes(sourceSha), shaBytes(marker.markerSha256()), writer,
          targetWriter, quotaMaximum)
        executePhase(planPhase(Records.Phase.CERTIFY, current, target, writer.databaseNow))
      case MySqlCertifiedPhysicalBinding(binding, 2) if binding == plan.bindingUuid() =>
        publishOrRebarrierMarker(requireExistingIntent = true)
      case _ => throw new SQLDataException("FirstBinding6 requires its exact original Legacy2 or Certified2 phase")

    val certified = observe()
    current = certified._1; writer = certified._2
    require(qualification.physicalBinding == MySqlCertifiedPhysicalBinding(plan.bindingUuid(), 2),
      "FirstBinding6 release requires actual post-CERTIFY qualification")
    if writer.ownerBootUuid.nonEmpty then
      val owned = ensureCurrentWriter(current, writer)
      current = owned._1; writer = owned._2
      val targetWriter = writer.copy(ownerBootUuid = None, leaseUntil = writer.databaseNow.minusNanos(1000L))
      val target = MySqlDbFirstBootstrapWriterGraph.releaseTarget(current, full27, root, plan,
        source.selectedSaveFileId(), shaBytes(sourceSha), Some(shaBytes(marker.markerSha256())), writer,
        targetWriter, quotaMaximum)
      executePhase(planPhase(Records.Phase.RELEASE, current, target, writer.databaseNow))
    val finalActual = observe()
    val release = entries.filter(e => e.record.phase() == Records.Phase.RELEASE && e.settlement != null &&
      e.settlement.outcome() == Records.Outcome.APPLIED && sameData(e.target, finalActual._1))
    require(release.size == 1 && finalActual._2.ownerBootUuid.isEmpty,
      "FirstBinding6 completion requires the exact applied full RELEASE readback")
    historyTail().foreach(expected => require(sameData(expected, finalActual._1),
      "FirstBinding6 final database differs from its complete applied history"))
    codecWorkspace {
      pendingBody = Records.encodeSettlement(release.head.settlement)
      store.publish(Store.Key.of(Store.Role.COMPLETE), pendingBody)
      pendingBody = null
    }

  private def newLease(now: LocalDateTime): LocalDateTime = now.plus(control.policy().writerLease())

  private def ensureCurrentWriter(initial: MySqlScriptSchemaSnapshot, initialWriter: MySqlBusinessWriterRow)
      : (MySqlScriptSchemaSnapshot, MySqlBusinessWriterRow) =
    if initialWriter.ownerBootUuid.contains(owner.boot()) && initialWriter.leaseUntil.isAfter(initialWriter.databaseNow) then
      return (initial, initialWriter)
    var current = initial
    var writer = initialWriter
    val old = writer.ownerBootUuid.getOrElse(throw new SQLDataException("FirstBinding6 cannot hand off an unexplained unowned phase"))
    val originalOwner = owners.getOrElse(old, throw new SQLDataException("FirstBinding6 old writer has no original durable owner evidence"))
    require(originalOwner.boot() != owner.boot(), "FirstBinding6 current boot lease expired; retain history and restart instead of overwriting its phase intent")
    store.requirePriorProcessEnded(originalOwner.pid(), originalOwner.creationTime())
    session.bootstrapVerifiedBarrier(this, verified, scope)
    if writer.leaseUntil.isAfter(writer.databaseNow) then
      val remaining = java.time.Duration.between(writer.databaseNow, writer.leaseUntil)
      require(remaining.compareTo(control.policy().writerLease()) <= 0,
        "FirstBinding6 prior lease exceeds the configured bounded handoff wait")
      val millis = Math.addExact(remaining.toMillis, if remaining.toNanos % 1000000L == 0L then 0L else 1L)
      Thread.sleep(millis)
      val fresh = observe(); current = fresh._1; writer = fresh._2
    require(writer.ownerBootUuid.contains(old) && !writer.leaseUntil.isAfter(writer.databaseNow),
      "FirstBinding6 actual prior writer/lease changed or has not expired after bounded wait")
    val targetWriter = MySqlWriterFenceRow(plan.bindingUuid(), Math.addExact(writer.writerEpoch, 1L), Some(owner.boot()),
      newLease(writer.databaseNow), writer.lastOperationUuid, writer.lastFingerprint)
    val markerHash = Option(marker).map(value => shaBytes(value.markerSha256()))
    val target = MySqlDbFirstBootstrapWriterGraph.handoffTarget(current, full27, root, plan,
      source.selectedSaveFileId(), shaBytes(sourceSha), markerHash, writer, targetWriter, quotaMaximum)
    executePhase(planPhase(Records.Phase.HANDOFF, current, target, writer.databaseNow))
    observe()

  private def publishOrRebarrierMarker(requireExistingIntent: Boolean): Unit =
    require(!gateHeld && !commitUnknown, "FirstBinding6 marker cutover requires a settled SQL stage")
    val intentKey = Store.Key.of(Store.Role.MARKER_INTENT)
    val existing = store.read(intentKey)
    var cutover: Records.CutoverIntentData = null
    var intentAck: Store.DurableArtifact = null
    if existing != null then
      try
        cutover = codecWorkspace { existing.decode(bytes => Records.decodeCutoverIntent(bytes)) }
        intentAck = existing.acknowledgement()
        require(cutover.sourceReceiptSha256() == sourceSha && cutover.operationUuid() == plan.bootstrapOperationUuid() &&
          cutover.newBindingUuid() == plan.bindingUuid() && owners.get(cutover.owner().boot()).contains(cutover.owner()),
          "FirstBinding6 original marker intent/source/operation differs")
      finally existing.close()
    else
      require(!requireExistingIntent, "FirstBinding6 Certified/history is missing its original marker intent")
      cutover = new Records.CutoverIntentData(sourceSha, owner, plan.bootstrapOperationUuid(), plan.bindingUuid())
      codecWorkspace {
        pendingBody = Records.encodeCutoverIntent(cutover)
        intentAck = store.publish(intentKey, pendingBody)
        pendingBody = null
      }
    intentAck.rebarrier(); sourceAck.rebarrier(); ownerAck.rebarrier()
    val previous = source.oldMarker()
    val expectation: WindowsDbFirstMarkerAdoption.OldMarkerExpectation =
      if previous.present() then new WindowsDbFirstMarkerAdoption.ExpectedOldMarker(
        parseFileIdentity(previous.fileId()), previous.markerBytes().orElseThrow())
      else WindowsDbFirstMarkerAdoption.ExpectedAbsence.INSTANCE
    val request = new WindowsDbFirstMarkerAdoption.Request(control.worldPath().resolve("data").resolve("floatingpod-world-binding.bin"),
      plan.bindingUuid(), plan.bootstrapOperationUuid(), store.dataDirectoryIdentity(), expectation)
    markerPublication = new WindowsDbFirstMarkerAdoption(nativeIo, nativeHolder.dbFirstAllocation()).adopt(request)
    val actual = markerPublication.markerPublicationAck()
    val actualMarker = new Records.MarkerData(actual.fileIdentity().versionedIdentity(), actual.markerSha256(),
      previous.present(), if previous.present() then previous.fileId() else null)
    val durableKey = Store.Key.of(Store.Role.MARKER_DURABLE)
    val durable = store.read(durableKey)
    if durable != null then
      try
        val confirmed = codecWorkspace { durable.decode(bytes => Records.decodeCutoverConfirmed(bytes)) }
        require(confirmed.intent() == cutover && confirmed.intentSha256() == intentAck.sha256(),
          "FirstBinding6 marker confirmation differs from original durable cutover intent")
        requireSameMarker(confirmed.marker(), actualMarker)
      finally durable.close()
    else
      require(!requireExistingIntent, "FirstBinding6 Certified/history is missing its original durable marker acknowledgement")
      codecWorkspace {
        pendingBody = Records.encodeCutoverConfirmed(new Records.CutoverConfirmedData(cutover, intentAck.sha256(), actualMarker))
        store.publish(durableKey, pendingBody)
        pendingBody = null
      }
    marker = actualMarker

  private def requireSameMarker(expected: Records.MarkerData, actual: Records.MarkerData): Unit =
    require(expected != null && actual != null && expected == actual &&
      actual.preservedOldMarkerPresent() == source.oldMarker().present() &&
      (!source.oldMarker().present() || actual.preservedOldFileId() == source.oldMarker().fileId()),
      "FirstBinding6 canonical marker FileId/hash or preserved original marker identity differs")

  private def retireData(): Unit =
    require(!gateHeld && !commitUnknown, "FirstBinding6 DATA cannot retire while original SQL outcome remains unresolved")
    entries.clear(); owners.clear()
    nativePreparationSources.clear()
    if nativePreparationSourceFee!=0L then
      root.releaseRetainedMemory(nativePreparationSourceFee,"native witness source metadata detached")
      nativePreparationSourceFee=0L
    if pendingPayload != null then
      pendingBody = null
      pendingPayload.storageBudget.closeScopedReadBudget(); pendingPayload = null
    if pendingReader != null then
      pendingReader.close(); pendingReader = null
    while readers.nonEmpty do
      readers.head.close(); readers.remove(0)
    while graphs.nonEmpty do
      val child = graphs.head
      if !child.isClosed then child.closeScopedReadBudget()
      graphs.remove(0)
    while dmlOwners.nonEmpty do
      dmlOwners.head.close(); dmlOwners.remove(0)
    if recordFee != 0L then
      root.releaseRetainedMemory(recordFee, "FirstBinding6 historical proof/owner graph metadata detached")
      recordFee = 0L

  /** 先收束已知读事务；未知提交/close 不重试，Session 仍须继续关闭原 JDBC。 */
  def beforeJdbcClose(): Unit =
    requireOwner()
    if closed || !started then return
    var primary: Throwable = null
    def settle(action: => Unit): Unit =
      try action
      catch case cause: Throwable =>
        if primary == null then primary = cause else MySqlOwnedSessionLifecycle.suppress(primary, cause)
    settle(closeClock())
    dmlOwners.foreach(dml => settle(dml.close()))
    if gateHeld && !commitUnknown && !autoCommitRestoreAttempted && !isolationRestoreAttempted then
      settle(finishRead())
    if !gateHeld && !commitUnknown && primary == null then settle(retireData())
    if primary != null then
      if failure == null then failure = primary else MySqlOwnedSessionLifecycle.suppress(failure, primary)
      throw primary

  /** 只有原物理连接已关闭，才能撤销本地未知 SQL 责任并最终关闭 native custody。 */
  def afterJdbcClose(): Unit =
    requireOwner()
    require(started && !closed && verified != null && verified.connection.isClosed,
      "FirstBinding6 local retirement requires the actual original JDBC connection closed")
    if gateHeld then
      MySqlConnectionTransactionGate.leave(verified.connection, rollback)
      gateHeld = false; gate = null; rollback = null
    sqlRows = null; sqlStatement = null
    if sqlFee != 0L then
      root.releaseTemporaryMemory(sqlFee, "FirstBinding6 dead clock handles detached after original connection closed")
      sqlFee = 0L
    dmlOwners.foreach { dml =>
      if dml.hasUnresolvedStatement then dml.retireAfterClosedConnection(verified)
    }
    commitUnknown = false
    transactionAttempted = false; isolationAttempted = false; oldIsolation = None
    retireData()
    markerPublication = null; marker = null; owner = null; source = null
    sourceAck = null; ownerAck = null; completeAck = null
    if store != null then { store.close(); store = null }
    if nativeHolder != null then { nativeHolder.close(); nativeHolder = null; nativeIo = null }
    held = null; qualification = null; scope = null; verified = null
    closed = true

  def retireUnstarted(): Unit =
    requireOwner()
    require(!started && !closed && verified == null && scope == null && nativeHolder == null && store == null &&
      !gateHeld && !commitUnknown && pendingReader == null && pendingPayload == null && readers.isEmpty &&
      graphs.isEmpty && dmlOwners.isEmpty && sqlStatement == null && sqlRows == null && sqlFee == 0L && recordFee == 0L,
      "only the actual unstarted zero-effect FirstBinding6 holder may retire without parent JDBC closure")
    closed = true

private[mysql] object MySqlDbFirstBootstrapActor:
  /** 只由原actor的实际readWitness解码登记；没有SQL执行或普通read-generation资格。 */
  final class NativePreparationSource private[MySqlDbFirstBootstrapActor] (
      actor:MySqlDbFirstBootstrapActor,image:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget,
      ack:Store.DurableArtifact):
    private[mysql] def requireComparisonSource(actual:MySqlScriptSchemaSnapshot,
        originalRoot:MySqlFormat3ReadBudget):Unit =
      require((actual eq image) && (originalRoot eq root),"exact original native preparation witness required")
      actor.requireNativePreparationSource(this,actual,originalRoot)
      ack.rebarrier()
      require(image.canonicalSha256Hex.equalsIgnoreCase(ack.sha256()) &&
        image.canonicalByteLength==ack.byteLength().toLong,"native preparation witness bytes changed")
  final case class ResourceObservation(started: Boolean, completed: Boolean, nativePending: Boolean,
      readersPending: Boolean, dmlPending: Boolean, gateHeld: Boolean, commitUnknown: Boolean,
      closed: Boolean, failure: Option[Throwable])

  private final class Entry(val record: Records.RecordData, val intentAck: Store.DurableArtifact,
      val proof: MySqlDbFirstBootstrapProof.ValidatedPhase, val before: MySqlScriptSchemaSnapshot,
      val target: MySqlScriptSchemaSnapshot):
    var settlement: Records.SettlementData = null

  /** Pure reconciliation of already validated immutable DATA; it grants no SQL/native capability. */
  private[mysql] final case class HistoryPhase(proof: MySqlDbFirstBootstrapProof.ValidatedPhase,
      settlement: Option[Records.SettlementData]):
    require(proof != null && settlement != null && settlement.forall(_.intent() == proof.record),
      "history phase requires its exact original validated intent")

  private[mysql] def historyTail(phases: Vector[HistoryPhase]): Option[MySqlScriptSchemaSnapshot] =
    require(phases != null, "complete original FirstBinding6 history required")
    require(phases.isEmpty || phases.forall(p => p.proof.sourceReceiptSha256 == phases.head.proof.sourceReceiptSha256 &&
      p.proof.source == phases.head.proof.source), "FirstBinding6 history mixes original sources")
    if phases.isEmpty then return None
    val seeds = phases.filter(_.proof.record.phase() == Records.Phase.PREPARE).map(_.proof.before)
    require(seeds.nonEmpty && seeds.forall(sameData(_, seeds.head)), "FirstBinding6 history has no unique original seed")
    var tail = seeds.head
    val known = mutable.ArrayBuffer(tail)
    val remaining = mutable.ArrayBuffer.from(phases.filter(e => e.settlement.nonEmpty &&
      e.settlement.get.outcome() == Records.Outcome.APPLIED))
    while remaining.nonEmpty do
      val next = remaining.filter(e => sameData(e.proof.before, tail))
      require(next.size == 1, "FirstBinding6 applied history is forked, disconnected, or cyclic")
      val edge = next.head
      require(!known.exists(sameData(_, edge.proof.target)), "FirstBinding6 applied history revisits a previous full image")
      tail = edge.proof.target; known += tail; remaining -= edge
    phases.filter(e => e.settlement.nonEmpty && e.settlement.get.outcome() == Records.Outcome.NOT_APPLIED).foreach { e =>
      require(known.exists(sameData(_, e.proof.before)), "FirstBinding6 not-applied intent does not belong to the original history")
    }
    val pending = phases.filter(_.settlement.isEmpty)
    require(pending.size <= 1 && pending.forall(e => sameData(e.proof.before, tail)),
      "FirstBinding6 unresolved history is not one original tail intent")
    Some(tail)

  private[mysql] def classifyOriginal(proof: MySqlDbFirstBootstrapProof.ValidatedPhase,
      actual: MySqlScriptSchemaSnapshot): Records.Outcome =
    require(proof != null && actual != null && !actual.allocationBudget.isClosed &&
      (actual.inputs eq proof.before.inputs) && actual.schema == proof.before.schema &&
      actual.connection.serverUuid == proof.before.connection.serverUuid,
      "FirstBinding6 reconciliation requires the original live Full27 input/server/schema")
    if sameData(actual, proof.target) then Records.Outcome.APPLIED
    else if sameData(actual, proof.before) then Records.Outcome.NOT_APPLIED
    else throw new SQLDataException("FirstBinding6 original pending intent has UNKNOWN complete Full27 outcome; no SQL replay")

  private val intentRoles = Set(Store.Role.PREPARE_INTENT, Store.Role.HANDOFF_INTENT,
    Store.Role.CERTIFY_INTENT, Store.Role.RELEASE_INTENT)
  private def intentRole(phase: Records.Phase): Store.Role = phase match
    case Records.Phase.PREPARE => Store.Role.PREPARE_INTENT
    case Records.Phase.HANDOFF => Store.Role.HANDOFF_INTENT
    case Records.Phase.CERTIFY => Store.Role.CERTIFY_INTENT
    case Records.Phase.RELEASE => Store.Role.RELEASE_INTENT
  private def beforeRole(phase: Records.Phase): Store.Role = phase match
    case Records.Phase.PREPARE => Store.Role.PREPARE_BEFORE
    case Records.Phase.HANDOFF => Store.Role.HANDOFF_BEFORE
    case Records.Phase.CERTIFY => Store.Role.CERTIFY_BEFORE
    case Records.Phase.RELEASE => Store.Role.RELEASE_BEFORE
  private def targetRole(phase: Records.Phase): Store.Role = phase match
    case Records.Phase.PREPARE => Store.Role.PREPARE_TARGET
    case Records.Phase.HANDOFF => Store.Role.HANDOFF_TARGET
    case Records.Phase.CERTIFY => Store.Role.CERTIFY_TARGET
    case Records.Phase.RELEASE => Store.Role.RELEASE_TARGET
  private def confirmationRole(phase: Records.Phase): Store.Role = phase match
    case Records.Phase.PREPARE => Store.Role.PREPARED
    case Records.Phase.HANDOFF => Store.Role.HANDOFF_CONFIRMED
    case Records.Phase.CERTIFY => Store.Role.CERTIFIED
    case Records.Phase.RELEASE => Store.Role.RELEASED
  private def sameData(left: MySqlScriptSchemaSnapshot, right: MySqlScriptSchemaSnapshot): Boolean =
    left != null && right != null && left.inputs == right.inputs && left.schema == right.schema &&
      left.relations == right.relations
  private def shaBytes(value: String): Vector[Byte] = HexFormat.of().parseHex(value).toVector
  private def parseFileIdentity(value: String): WindowsMarkerIo.FileIdentity =
    val prefix = "FP_WIN32_FILE_ID_V1;volume="
    require(value != null && value.startsWith(prefix) && value.length == prefix.length + 16 + 4 + 32 &&
      value.substring(prefix.length + 16, prefix.length + 20) == ";id=", "exact original Windows FileId DATA required")
    val parsed = new WindowsMarkerIo.FileIdentity(java.lang.Long.parseUnsignedLong(
      value.substring(prefix.length, prefix.length + 16), 16), HexFormat.of().parseHex(value.substring(prefix.length + 20)))
    require(parsed.versionedIdentity() == value, "canonical original Windows FileId DATA required")
    parsed


