package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, WindowsB1SourceAnchorStore, WindowsB1Full20WitnessStore, WindowsB1TwoSlotStore, WindowsCanonicalMarkerPublicationBarrier, WindowsMarkerIo, WindowsWorldProvisioner}
import com.wi.floatingpod.storage.mysql.MySqlFreshPrepareCapabilities.{DurableFreshGenesis, Origin, PlannedFreshPrepare, PreparedConfirmed, WriterTuple}
import com.wi.floatingpod.storage.mysql.MySqlFreshCertifyCapabilities.{CurrentMarker, PlannedFreshCertify, DurableCertifyIntent, CertifiedConfirmed}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.security.MessageDigest
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, SQLTransactionRollbackException}
import java.time.{Duration, LocalDateTime}
import java.util.{HexFormat, UUID}

final class MySqlBootstrapCommittedUnverifiedException(
    val stage: String, val operationContext: Option[MySqlOperationContext], cause: Throwable
) extends SQLException("MySQL B1 " + stage + " committed but its durable state could not be verified", cause)

final class MySqlBootstrapCleanupUncertainException(
    val stage: String, val operationContext: Option[MySqlOperationContext], cause: Throwable
) extends SQLException("MySQL B1 " + stage + " cleanup left the owned connection uncertain", cause)

enum MySqlFreshWorldBootstrapStatus:
  case New, Prepared, MarkerAcknowledged, Completed, RuntimeAdmitted, Unknown

/** Format 3/physical2 的真实 Fresh + current-process custody 才能开启的两阶段 MySQL B1 bootstrap。 */
final class MySqlFreshWorldBootstrap(
    expectation: MySqlSchemaAuditExpectation,
    auditor: MySqlSchemaAuditor,
    verified: VerifiedMySqlConnection,
    scope: MySqlAuditBootScope,
    provisioner: WindowsWorldProvisioner,
    markerPublisher: WindowsCanonicalMarkerPublicationBarrier,
    leaseDuration: Duration,
    originalSessionLedger: MySqlFormat3AllocationLedger
):
  require(expectation != null && auditor != null && verified != null && scope != null && provisioner != null && markerPublisher != null,
    "MySQL Fresh bootstrap dependencies are required")
  require(expectation.profile == MySqlSchemaAuditProfile.Format3 &&
    expectation.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2,
    "fresh world bootstrap requires the explicit format 3 schema and physical2 target")
  require(expectation.expectedImport == MySqlExpectedNoImport, "fresh world bootstrap cannot consume an import scope")
  require(leaseDuration != null && !leaseDuration.isNegative && !leaseDuration.isZero,
    "bootstrap writer lease duration must be positive")
  private val leaseNanos = leaseDuration.toNanos
  require(leaseNanos > 0L && leaseNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos && leaseNanos % 1000L == 0L,
    "bootstrap writer lease must be bounded and have DATETIME(6) precision")
  require(scope.ownerThread eq Thread.currentThread(), "B1 bootstrap must be constructed on the owned MySQL IO thread")

  private val connection = verified.connection
  private val ownerThread = Thread.currentThread()
  // One actor obligation root on the actual Session ledger; no per-read budget/cap mint.
  private val originalWitnessProfile = expectation.format3Inputs.getOrElse(
    throw new IllegalStateException("actual Fresh actor requires independent Format3 profile")).limitProfile
  require(originalSessionLedger != null, "actual Session allocation ledger required before actor planning")
  private val originalWitnessRoot = MySqlFormat3ReadBudget.rootOnLedger(
    originalWitnessProfile, originalWitnessProfile.maxSqlPacketBytes(), originalSessionLedger)
  private val originalWitnessConnection = verified.identity
  private val originalWitnessBoot = scope.bootUuid
  private val originalWitnessGeneration = scope.bootGeneration
  private val originalWitnessConnectionGeneration = scope.connectionGeneration
  private def requireOriginalWitnessRoot(root: MySqlFormat3ReadBudget): Unit =
    require((Thread.currentThread() eq ownerThread) && (scope.ownerThread eq ownerThread) &&
      (root eq originalWitnessRoot) && !root.isClosed && (root.profile eq originalWitnessProfile) &&
      verified.identity == originalWitnessConnection && scope.bootUuid == originalWitnessBoot &&
      scope.bootGeneration == originalWitnessGeneration && scope.connectionGeneration == originalWitnessConnectionGeneration,
      "exact retained actor witness root/session/generation required")
  // No closeRoot here: original prepare/certify/pending references remain live until exact settlement.
  private val quotedSchema = MySqlFencedTransactionSupport.quote(expectation.schema)
  private val plannedLifecycle = new MySqlFreshPrepareLifecycle(ownerThread)
  private val plannedNonce = UUID.randomUUID()
  private var plannedValue: PlannedFreshPrepare = null
  private var genesisValue: DurableFreshGenesis = null
  private var confirmedValue: PreparedConfirmed = null
  private var retainedDraft: Option[FreshPrepareDraft] = None
  private var normalDraft: Option[NormalPrepareDraft] = None
  private var normalPlanValue: MySqlNormalPreGenesisCapabilities.PlannedNormalPreGenesis = null
  private var normalAckValue: WindowsB1TwoSlotStore.DurablePreGenesisNativeAck = null
  private var normalWorldValue: WindowsWorldProvisioner.ProvisionedWorld = null
  private val certifyLifecycle = new MySqlFreshCertifyLifecycle(ownerThread)
  private var currentMarkerValue: CurrentMarker = null
  private var certifyPlanValue: PlannedFreshCertify = null
  private var originalWitnessDraft: MySqlFull20OriginalWitness.Draft = null
  private var originalPrepareWitnessDraft: MySqlFull20OriginalWitness.Draft = null
  private var prepareWitnessTransfer: MySqlFull20NativeTransfer = null
  private var certifyWitnessTransfer: MySqlFull20NativeTransfer = null
  private var prepareWitnessStore: WindowsB1Full20WitnessStore = null
  private var certifyWitnessStore: WindowsB1Full20WitnessStore = null
  private var prepareWitnessAck: WindowsB1Full20WitnessStore.NativeArtifactDurability = null
  private var certifyWitnessAck: WindowsB1Full20WitnessStore.NativeArtifactDurability = null
  private var certifyIntentValue: DurableCertifyIntent = null
  private var certifiedValue: CertifiedConfirmed = null
  private var certifyDraft: Option[B1ResumeRecord] = None
  private var certifyRetirements = Vector.empty[WindowsB1TwoSlotStore.RetiredInactiveSlot]
  private var completionProofValue: MySqlFreshCertifyCapabilities.CompletionProof = null
  private var prepared: MySqlFreshWorldBootstrap.Prepared = null
  private var published: MySqlFreshWorldBootstrap.MarkerAcknowledgement = null
  private var completed: MySqlFreshWorldBootstrap.Completed = null
  private var admittedCoordinator: MySqlWriterFenceCoordinator = null
  private var admittedValue: MySqlBusinessAdmission = null
  private var unknownFailure: Throwable = null
  private var unknownStage: String = null
  private var activeRollbackOnly: MySqlRollbackOnlyState = null
  private var certifiedExitPrepared = false
  private var certifiedWitnessesDropped = false
  private var certifiedWitnessRootClosed = false

  /** 准备正常退出仍走原 actor/native/full20；续租只允许 W 改变，不跳过其他十九关系。 */
  private[mysql] def prepareCertifiedCompletionExit(actualCoordinator: MySqlWriterFenceCoordinator,
      actualAdmission: MySqlBusinessAdmission): Unit =
    requireOwnedThread(); requireNotUnknown()
    require(!certifiedExitPrepared && !certifiedWitnessesDropped &&
      actualCoordinator != null && (actualCoordinator eq admittedCoordinator) &&
      admittedValue != null && certifiedValue != null && completionProofValue != null &&
      normalPlanValue == null && normalDraft.isEmpty && prepared == null && completed == null,
      "original admitted planned Fresh actor required for completion exit")
    try
      val observation = plannedCertifyObservation
      require(observation.phase == MySqlFreshCertifyPhase.Admitted && observation.failure.isEmpty,
        "actual successful Fresh certification required for completion exit")
      val plan = certifiedValue.originalPlan()
      val origin = plan.marker().prepared().originalPlan().origin()
      requireCurrentNative(origin, certifiedValue.ack(), plan.confirmationBytes())
      rebarrierMarker(plan.marker())
      val currentAdmission = actualCoordinator.requireBusinessAdmission(verified, scope)
      require(actualAdmission != null && (currentAdmission eq actualAdmission) &&
        currentAdmission.bootUuid == admittedValue.bootUuid &&
        currentAdmission.bindingUuid == admittedValue.bindingUuid &&
        currentAdmission.writerEpoch == admittedValue.writerEpoch &&
        currentAdmission.token.connectionGeneration == originalWitnessConnectionGeneration,
        "Fresh completion changed its original session/fence; current renewal admission is required")
      var current = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema,
        originalWitnessProfile, Some(originalSessionLedger)).read()
      val root = current.allocationBudget
      var primary: Throwable = null
      try
        val certified = plan.targetGraph().format3Snapshot.getOrElse(
          throw new IllegalStateException("original certification lost its full20 source"))
        MySqlOrdinaryHandoffGraph.requireOnlyWriterChanged(certified, current)
        require(actualCoordinator.requireBusinessAdmission(verified, scope) eq currentAdmission,
          "Fresh completion admission changed across the actual full20 read")
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        current = null
        try root.closeRootReadBudget()
        catch
          case cleanup: Throwable =>
            if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
            else throw cleanup
      certifiedExitPrepared = true
    catch
      case cause: Throwable => freezeCertify("certified-completion-exit-validation", cause); throw cause

  /** 只有原 session 在 Main 输入已释放后调用；旧失败路径不进入此方法。 */
  private[mysql] def dropCertifiedWitnessesForExit(): Unit =
    requireOwnedThread(); requireNotUnknown()
    require(certifiedExitPrepared && !certifiedWitnessesDropped,
      "prepared original Fresh witness drop is one-shot")
    certifiedWitnessesDropped = true
    plannedValue = null; genesisValue = null; confirmedValue = null; retainedDraft = None
    currentMarkerValue = null; certifyPlanValue = null
    originalWitnessDraft = null; originalPrepareWitnessDraft = null
    prepareWitnessTransfer = null; certifyWitnessTransfer = null
    prepareWitnessStore = null; certifyWitnessStore = null
    prepareWitnessAck = null; certifyWitnessAck = null
    certifyIntentValue = null; certifiedValue = null; certifyDraft = None
    certifyRetirements = Vector.empty; completionProofValue = null
    try
      originalWitnessRoot.closeRootReadBudget()
      certifiedWitnessRootClosed = true
    catch
      case cause: Throwable => freezeCertify("certified-completion-witness-root-close", cause); throw cause

  private[mysql] def certifiedWitnessExitComplete: Boolean =
    requireOwnedThread()
    certifiedWitnessesDropped && certifiedWitnessRootClosed && unknownFailure == null

  require(connection != null && verified.identity != null, "verified MySQL session is incomplete")
  require(scope.verifiedConnection.contains(verified) && (scope.connectionToken eq connection) &&
    (scope.connectionIdentity eq verified.identity), "B1 bootstrap requires the exact verified connection in its immutable scope")
  require(scope.ownerThread eq ownerThread, "B1 bootstrap scope belongs to another IO thread")
  require(verified.identity.serverUuid == expectation.serverUuid &&
    verified.identity.serverVersion == expectation.serverVersion &&
    verified.identity.catalog == expectation.schema && verified.identity.effectiveUser == expectation.effectiveUser,
    "verified MySQL identity differs from the approved format 3 target")

  def status: MySqlFreshWorldBootstrapStatus = synchronized {
    if unknownFailure != null then MySqlFreshWorldBootstrapStatus.Unknown
    else if admittedValue != null then MySqlFreshWorldBootstrapStatus.RuntimeAdmitted
    else if completed != null || certifiedValue != null then MySqlFreshWorldBootstrapStatus.Completed
    else if published != null || currentMarkerValue != null then MySqlFreshWorldBootstrapStatus.MarkerAcknowledged
    else if prepared != null || confirmedValue != null then MySqlFreshWorldBootstrapStatus.Prepared
    else MySqlFreshWorldBootstrapStatus.New
  }

  /** 独立planned路径；旧prepare/certify/resume的v3 blocker不因这三个issuer而消失。 */
  def plannedPrepareObservation: MySqlFreshPrepareAttemptObservation = synchronized {
    requireOwnedThread()
    plannedLifecycle.observe(Option(plannedValue), Option(genesisValue), retainedDraft.map(_.genesis))
  }

  /** 原生核验在DB事务之外；这里只读共同锁与完整图，checked cleanup后才能签plan。 */
  def planFreshPrepare(world: WindowsWorldProvisioner.ProvisionedWorld,
      anchorStore: WindowsB1SourceAnchorStore, location: WindowsB1SourceAnchorStore.ControlledAnchorLocation,
      sourceAck: WindowsB1SourceAnchorStore.DurableB1SourceAnchor, slots: WindowsB1TwoSlotStore,
      actualNativeIo: WindowsMarkerIo, held: HeldSessionBarrier): PlannedFreshPrepare = synchronized {
    requireOwnedThread(); requireNotUnknown(); plannedLifecycle.beginPlanning()
    try
      require(anchorStore != null && location != null && sourceAck != null && slots != null && actualNativeIo != null && held != null,
        "actual controlled source/native/session issuers required")
      val (fresh, custody) = validateFreshWorld(world)
      require(custody.bootId() == scope.bootUuid && custody.generation() == scope.bootGeneration,
        "native boot/generation differs from actual SQL scope")
      val bootstrap = planIdentity(fresh.plan())
      val anchor = anchorStore.rebarrierExisting(location, sourceAck.verifiedInput())
      val anchorInput = anchor.verifiedInput()
      if anchorInput.expectedPlan() != fresh.plan() || anchorInput.originalSourceIdentity() != fresh.sourceWorldIdentity() then
        reject("original native anchor differs from actual Fresh source")
      MySqlFreshPrepareRules.exactBytes(fresh.receiptCanonicalBytes(), anchorInput.originalReceiptCanonicalBytes())
      held.readmit(verified, expectation, scope)
      if anchorInput.barrierNamespace() != held.namespace.lockName() then reject("source anchor namespace differs from actual held SQL session")
      val nativeScope = slots.bindSource(anchorStore, location, anchor, custody)
      val records = slots.readCurrent(nativeScope)
      if records.current() != null || records.inactive() != null then reject("Fresh planning cannot replace an existing durable attempt")
      val process = actualNativeIo.captureCurrentProcessIdentity(bootstrap.bootstrapOperationUuid)
      val nativeOwner = new B1ResumeRecord.NativeOwnerAssertions(process.pid(), process.creationFileTime(), custody.bootId(), custody.generation())
      val origin = new Origin(this, plannedNonce, verified, scope, held, world, fresh, custody, slots, nativeScope, anchor)
      val qualification = qualify(Set(MySqlNoPhysicalBinding), requireUnownedWriter = true)
      val attemptId = UUID.randomUUID() // 仅此actual planning attempt生成一次；不是phase UUID。
      val draft = readonlyWithGate("planned-prepare-plan") {
        held.readmit(verified, expectation, scope)
        val beforeRead = readLockedFormat3Graph()
        val manifest = beforeRead.manifest
        val binding = beforeRead.binding
        if binding.nonEmpty then reject("Fresh planning requires the actual B row absent")
        val writer = beforeRead.writer
        validateBootstrapAcquirePreimage(writer, qualification)
        val before = beforeRead.repository
        if !before.exactlyEquals(MySqlFullRepositorySnapshot.empty) then reject("Fresh planning requires exact complete initial seed graph")
        MySqlFreshFormat3Graph.requireFreshInitialSeed(beforeRead.full20, before, manifest, writer,
          qualification.writerEpochObservedBeforeAcquire, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val beforeGraph = beforeRead.graph
        val target = MySqlBootstrapSnapshotTransitions.prepareFresh(before, bootstrap, fresh.sourceWorldIdentity(),
          MySqlFreshWorldBootstrap.sha256(fresh.receiptCanonicalBytes()))
        val nextEpoch = Math.addExact(writer.writerEpoch, 1L)
        val lease = writer.databaseNow.plusNanos(leaseNanos)
        val normalizedWriter = MySqlFreshFormat3WriterTarget(bootstrap.bindingUuid, nextEpoch, Some(scope.bootUuid),
          lease, bootstrap.prepareWriteUuid)
        val context = format3OperationContext("PREPARE", bootstrap, beforeRead.full20, before, target, normalizedWriter)
        val contextWriter = writer.copy(bindingUuid = bootstrap.bindingUuid, writerEpoch = nextEpoch,
          ownerBootUuid = Some(scope.bootUuid), leaseUntil = lease, lastOperationUuid = Some(context.operationUuid),
          lastFingerprint = Some(context.canonicalFingerprintSha256))
        val targetFull20 = MySqlFreshFormat3Graph.targetSnapshot(beforeRead.full20, before, target, contextWriter)
        val targetGraph = MySqlB1CommonGraph.captureFormat3(manifest, target.recovery.binding, target, targetFull20,
          expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val beforeW = MySqlFreshPrepareRules.writer(writer)
        val targetW = new WriterTuple(bootstrap.bindingUuid, nextEpoch, scope.bootUuid, lease,
          context.operationUuid, context.canonicalFingerprintSha256.toArray)
        // First Fresh严格无lastslot；target的原scope来自本次context，绝不writer.receipt重建。
        val beforeAssertion = new B1ResumeRecord.WriterAssertions(beforeW.binding(), beforeW.epoch(), beforeW.ownerBoot(), beforeW.leaseUntil(), null)
        val targetSlot = new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid, context.writerEpoch,
          context.bindingUuid, context.operationUuid, context.canonicalFingerprintSha256.toArray)
        val targetAssertion = new B1ResumeRecord.WriterAssertions(targetW.binding(), targetW.epoch(), targetW.ownerBoot(), targetW.leaseUntil(), targetSlot)
        val attempt = new B1ResumeRecord.AttemptAssertions(attemptId, B1ResumeRecord.Phase.PREPARE, bootstrap.prepareWriteUuid,
          scope.bootGeneration, held.connectionGenerationId, held.actualConnectionId, held.namespace.lockName(),
          MySqlFreshPrepareRules.operation(context), beforeAssertion, targetAssertion,
          beforeGraph.toResumeAssertions, targetGraph.toResumeAssertions)
        val initial = new B1ResumeRecord.FullStateAssertions(nativeOwner, null, null, attempt,
          beforeAssertion, targetAssertion, attempt, null, null, null)
        val genesis = new B1ResumeRecord(0L, new Array[Byte](32), B1ResumeRecord.Kind.GENESIS,
          HexFormat.of().parseHex(anchor.anchorSha256()), anchorInput.originalReceiptCanonicalBytes(), held.namespace.lockName(), initial)
        val genesisBytes = B1ResumeRecordCanonical.encode(genesis)
        val complete = new B1ResumeRecord.FullStateAssertions(nativeOwner, null, null, attempt,
          targetAssertion, targetAssertion, null, null, null, null)
        val confirmation = new B1ResumeRecord(1L, MessageDigest.getInstance("SHA-256").digest(genesisBytes),
          B1ResumeRecord.Kind.PREPARED_CONFIRMED, genesis.sourceAnchorSha256(), genesis.originalReceipt(), genesis.barrierNamespace(), complete)
        val confirmationBytes = B1ResumeRecordCanonical.encode(confirmation) // 总64KiB预检，早于任何DML。
        custody.validateLive()
        val result = FreshPrepareDraft(origin, attemptId, bootstrap, context, beforeGraph, targetGraph, writer,
          beforeW, targetW, genesis, genesisBytes.toVector, confirmation, confirmationBytes.toVector)
        retainedDraft = Some(result) // cleanup失败时仅保留facts，不提前发行opaque plan。
        result
      }
      // readonlyWithGate只有rollback/restore/gateleave全部成功才能返回。
      plannedValue = MySqlFreshPrepareCapabilities.issuePlan(draft.origin, draft.attempt, draft.bootstrap, draft.context,
        draft.before, draft.target, draft.writer, draft.beforeW, draft.targetW, draft.genesis, draft.genesisBytes.toArray,
        draft.confirmation, draft.confirmationBytes.toArray)
      plannedLifecycle.planningCleaned()
      plannedValue
    catch
      case primary: Throwable =>
        freezePlanned("planned-prepare-plan", primary)
        throw primary
  }

  /** 原 anchor/full old owner 的初始 seed：不创建世界，不把 DATA 变 Fresh proof。 */
  def planNormalPreGenesis(inspection: WindowsWorldProvisioner.ResumeInspection,
      anchorStore: WindowsB1SourceAnchorStore, location: WindowsB1SourceAnchorStore.ControlledAnchorLocation,
      sourceAck: WindowsB1SourceAnchorStore.DurableB1SourceAnchor, slots: WindowsB1TwoSlotStore,
      held: HeldSessionBarrier): MySqlNormalPreGenesisCapabilities.PlannedNormalPreGenesis = synchronized {
    requireOwnedThread(); requireNotUnknown(); plannedLifecycle.beginPlanning()
    try
      require(anchorStore != null && location != null && sourceAck != null && slots != null && inspection != null && held != null,
        "actual controlled source/native/session issuers required")
      inspection.validateInspection()
      val fresh = inspection.freshProvisioningProof()
      require(inspection.targetOwner().boot() == scope.bootUuid && inspection.targetOwner().generation() == scope.bootGeneration,
        "inspection target boot/generation differs from actual SQL scope")
      val bootstrap = planIdentity(fresh.plan())
      val anchor = anchorStore.rebarrierExisting(location, sourceAck.verifiedInput())
      val anchorInput = anchor.verifiedInput()
      if anchorInput.expectedPlan() != fresh.plan() || anchorInput.originalSourceIdentity() != fresh.sourceWorldIdentity() then
        reject("original native anchor differs from actual Fresh source")
      MySqlFreshPrepareRules.exactBytes(fresh.receiptCanonicalBytes(), anchorInput.originalReceiptCanonicalBytes())
      held.readmit(verified, expectation, scope)
      if anchorInput.barrierNamespace() != held.namespace.lockName() then reject("source anchor namespace differs from actual held SQL session")
      val nativeScope = slots.bindResumeSource(anchorStore, location, anchor, inspection)
      val records = slots.readCurrent(nativeScope)
      if records.current() != null || records.inactive() != null then
        throw new MySqlNormalPreGenesisUnresolved(records, inspection)
      val nativeOwner = inspection.targetOwner()
      val origin = new MySqlNormalPreGenesisCapabilities.Source(this, plannedNonce, verified, scope, held,
        inspection, slots, nativeScope, anchorStore, location, anchor)
      val qualification = qualify(Set(MySqlNoPhysicalBinding), requireUnownedWriter = true)
      val attemptId = UUID.randomUUID() // 仅此actual planning attempt生成一次；不是phase UUID。
      val draft = readonlyWithGate("normal-pre-genesis-plan") {
        held.readmit(verified, expectation, scope)
        val beforeRead = readLockedFormat3Graph()
        val manifest = beforeRead.manifest
        val binding = beforeRead.binding
        if binding.nonEmpty then reject("Fresh planning requires the actual B row absent")
        val writer = beforeRead.writer
        validateBootstrapAcquirePreimage(writer, qualification)
        val before = beforeRead.repository
        if !before.exactlyEquals(MySqlFullRepositorySnapshot.empty) then reject("Fresh planning requires exact complete initial seed graph")
        MySqlFreshFormat3Graph.requireFreshInitialSeed(beforeRead.full20, before, manifest, writer,
          qualification.writerEpochObservedBeforeAcquire, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val beforeGraph = beforeRead.graph
        val target = MySqlBootstrapSnapshotTransitions.prepareFresh(before, bootstrap, fresh.sourceWorldIdentity(),
          MySqlFreshWorldBootstrap.sha256(fresh.receiptCanonicalBytes()))
        val nextEpoch = Math.addExact(writer.writerEpoch, 1L)
        val lease = writer.databaseNow.plusNanos(leaseNanos)
        val normalizedWriter = MySqlFreshFormat3WriterTarget(bootstrap.bindingUuid, nextEpoch, Some(scope.bootUuid),
          lease, bootstrap.prepareWriteUuid)
        val context = format3OperationContext("PREPARE", bootstrap, beforeRead.full20, before, target, normalizedWriter)
        val contextWriter = writer.copy(bindingUuid = bootstrap.bindingUuid, writerEpoch = nextEpoch,
          ownerBootUuid = Some(scope.bootUuid), leaseUntil = lease, lastOperationUuid = Some(context.operationUuid),
          lastFingerprint = Some(context.canonicalFingerprintSha256))
        val targetFull20 = MySqlFreshFormat3Graph.targetSnapshot(beforeRead.full20, before, target, contextWriter)
        val targetGraph = MySqlB1CommonGraph.captureFormat3(manifest, target.recovery.binding, target, targetFull20,
          expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val beforeW = MySqlFreshPrepareRules.writer(writer)
        val targetW = new WriterTuple(bootstrap.bindingUuid, nextEpoch, scope.bootUuid, lease,
          context.operationUuid, context.canonicalFingerprintSha256.toArray)
        // Normal初始seed严格无lastslot；target的原scope来自本次context，绝不writer.receipt重建。
        val beforeAssertion = new B1ResumeRecord.WriterAssertions(beforeW.binding(), beforeW.epoch(), beforeW.ownerBoot(), beforeW.leaseUntil(), null)
        val targetSlot = new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid, context.writerEpoch,
          context.bindingUuid, context.operationUuid, context.canonicalFingerprintSha256.toArray)
        val targetAssertion = new B1ResumeRecord.WriterAssertions(targetW.binding(), targetW.epoch(), targetW.ownerBoot(), targetW.leaseUntil(), targetSlot)
        val attempt = new B1ResumeRecord.AttemptAssertions(attemptId, B1ResumeRecord.Phase.PREPARE, bootstrap.prepareWriteUuid,
          scope.bootGeneration, held.connectionGenerationId, held.actualConnectionId, held.namespace.lockName(),
          MySqlFreshPrepareRules.operation(context), beforeAssertion, targetAssertion,
          beforeGraph.toResumeAssertions, targetGraph.toResumeAssertions)
        val initial = new B1ResumeRecord.FullStateAssertions(nativeOwner, inspection.previousOwner(), null, null,
          beforeAssertion, targetAssertion, attempt, null, null, null)
        val genesis = new B1ResumeRecord(0L, new Array[Byte](32), B1ResumeRecord.Kind.GENESIS,
          HexFormat.of().parseHex(anchor.anchorSha256()), anchorInput.originalReceiptCanonicalBytes(), held.namespace.lockName(), initial)
        val genesisBytes = B1ResumeRecordCanonical.encode(genesis)
        val complete = new B1ResumeRecord.FullStateAssertions(nativeOwner, inspection.previousOwner(), null, attempt,
          targetAssertion, targetAssertion, null, null, null, null)
        val confirmation = new B1ResumeRecord(1L, MessageDigest.getInstance("SHA-256").digest(genesisBytes),
          B1ResumeRecord.Kind.PREPARED_CONFIRMED, genesis.sourceAnchorSha256(), genesis.originalReceipt(), genesis.barrierNamespace(), complete)
        val confirmationBytes = B1ResumeRecordCanonical.encode(confirmation) // 总64KiB预检，早于任何DML。
        inspection.validateInspection()
        val result = NormalPrepareDraft(origin, attemptId, bootstrap, context, beforeGraph, targetGraph, writer,
          beforeW, targetW, genesis, genesisBytes.toVector, confirmation, confirmationBytes.toVector)
        normalDraft = Some(result) // cleanup失败时仅保留facts，不提前发行opaque plan。
        result
      }
      // readonlyWithGate只有rollback/restore/gateleave全部成功才能返回。
      normalPlanValue = MySqlNormalPreGenesisCapabilities.issuePlan(draft.origin, draft.attempt, draft.bootstrap, draft.context,
        draft.before, draft.target, draft.writer, draft.beforeW, draft.targetW, draft.genesis, draft.genesisBytes.toArray,
        draft.confirmation, draft.confirmationBytes.toArray)
      plannedLifecycle.planningCleaned()
      normalPlanValue
    catch
      case primary: Throwable =>
        freezePlanned("normal-pre-genesis-plan", primary)
        throw primary
  }

  /** 仅此 actual actor 的 plan 可发布；native ACK/install失败完整保留，不重算lease或重送。 */
  def publishNormalPreGenesis(plan: MySqlNormalPreGenesisCapabilities.PlannedNormalPreGenesis): DurableFreshGenesis = synchronized {
    requireOwnedThread(); requireNotUnknown(); requireNormalPlan(plan)
    plannedLifecycle.beginGenesis()
    try
      val source = plan.source()
      requireIdleAutocommit(); requireNormalPlan(plan)
      normalAckValue = source.slots.publishPreGenesisRecovery(source.nativeScope, plan.genesisRecord().assertions())
      MySqlFreshPrepareRules.exactBytes(plan.genesisCanonicalBytes(), normalAckValue.canonicalBytes())
      normalWorldValue = provisioner.installResumeOwner(source.inspection, normalAckValue)
      val world = normalWorldValue; val fresh = world.freshProvisioningProof(); val custody = world.bootWorldCustodyProof()
      require(!world.createdThisBoot() && (fresh eq source.inspection.freshProvisioningProof()) &&
        (custody.freshProvisioningProof() eq fresh) && custody.bootId() == scope.bootUuid && custody.generation() == scope.bootGeneration,
        "Normal install must retain exact original creation and actual successor custody")
      custody.validateLive()
      val anchor = source.anchorStore.rebarrierExisting(source.location, source.anchor.verifiedInput())
      val nativeScope = source.slots.bindSource(source.anchorStore, source.location, anchor, custody)
      val records = source.slots.readCurrent(nativeScope)
      if records.inactive() != null then reject("Normal native install gained unexpected inactive record")
      MySqlFreshCertifyRules.requireRecord(plan.genesisCanonicalBytes(), records.current())
      val ack = source.slots.rebarrierCurrent(nativeScope, records.current())
      val installed = MySqlNormalPreGenesisCapabilities.installed(plan, normalAckValue, world)
      val origin = new Origin(this, plannedNonce, verified, scope, source.held, world, fresh, custody,
        source.slots, nativeScope, anchor, installed)
      plannedValue = MySqlFreshPrepareCapabilities.issuePlan(origin, plan.attemptId(), plan.bootstrap(), plan.context(),
        plan.beforeGraph(), plan.targetGraph(), plan.initialWriter(), plan.beforeWriter(), plan.targetWriter(),
        plan.genesisRecord(), plan.genesisCanonicalBytes(), plan.confirmation(), plan.confirmationBytes())
      requireIssuedPlan(plannedValue)
      genesisValue = MySqlFreshPrepareCapabilities.issueGenesis(plannedValue, ack)
      plannedLifecycle.genesisDurable()
      genesisValue
    catch case primary: Throwable =>
      freezePlanned("normal-pre-genesis-native-install", primary); throw primary
  }

  private def requireOriginWorld(origin: Origin): Unit =
    if origin.normal == null then freshWorldStillLive(origin.world, origin.fresh, origin.custody)
    else
      val normal = origin.normal
      val plan = normal.plan; val source = plan.source()
      require((plan eq normalPlanValue) && (normal.ack eq normalAckValue) && (normal.world eq normalWorldValue) &&
        (normal.world eq origin.world) && !origin.world.createdThisBoot() && (source.actor eq this) &&
        source.nonce == plannedNonce && (source.verified eq verified) && (source.scope eq scope) && (source.held eq origin.held),
        "Normal actual plan/native ACK/installed origin changed")
      MySqlFreshPrepareRules.exactBytes(plan.genesisCanonicalBytes(), normal.ack.canonicalBytes())
      require((origin.world.freshProvisioningProof() eq origin.fresh) &&
        (origin.fresh eq source.inspection.freshProvisioningProof()) &&
        (origin.world.bootWorldCustodyProof() eq origin.custody) && (origin.custody.freshProvisioningProof() eq origin.fresh) &&
        origin.custody.bootId() == scope.bootUuid && origin.custody.generation() == scope.bootGeneration,
        "Normal original creation/successor custody changed")
      require(plan.genesisRecord().assertions().previousOwner() == source.inspection.previousOwner() &&
        plan.genesisRecord().assertions().currentOwner() == source.inspection.targetOwner(), "Normal actual owner linkage changed")
      origin.custody.validateLive()

  private def requireNormalPlan(plan: MySqlNormalPreGenesisCapabilities.PlannedNormalPreGenesis): Unit =
    require(plan != null && (plan eq normalPlanValue), "exact original Normal readonly plan required")
    val source = plan.source()
    require((source.actor eq this) && source.nonce == plannedNonce && (source.verified eq verified) && (source.scope eq scope),
      "Normal plan actor/session/scope changed")
    source.held.readmit(verified, expectation, scope)
    source.inspection.validateInspection()
    val live = source.anchorStore.rebarrierExisting(source.location, source.anchor.verifiedInput())
    if live.anchorSha256() != source.anchor.anchorSha256() || live.fileIdentity() != source.anchor.fileIdentity() then
      reject("Normal original source anchor changed")
    val records = source.slots.readCurrent(source.nativeScope)
    if records.current() != null || records.inactive() != null then reject("Normal readonly plan requires no previous GENESIS/history")

  def normalPreGenesisObservation: MySqlNormalPreGenesisObservation = synchronized {
    requireOwnedThread()
    MySqlNormalPreGenesisObservation(Option(normalPlanValue), normalDraft.map(_.genesis), Option(normalAckValue),
      Option(normalWorldValue), plannedPrepareObservation, plannedCertifyObservation)
  }

  def publishPlannedGenesis(plan: PlannedFreshPrepare): DurableFreshGenesis = synchronized {
    requireOwnedThread(); requireNotUnknown(); requireIssuedPlan(plan)
    plannedLifecycle.beginGenesis()
    try
      requireIdleAutocommit()
      val origin = plan.origin()
      val ack = origin.slots.publishGenesis(origin.nativeScope, plan.genesisRecord().assertions())
      MySqlFreshPrepareRules.exactBytes(plan.genesisCanonicalBytes(), ack.facts().canonicalBytes())
      MySqlFreshPrepareRules.exactBytes(plan.genesisCanonicalBytes(), B1ResumeRecordCanonical.encode(ack.facts().record()))
      requireIssuedPlan(plan)
      genesisValue = MySqlFreshPrepareCapabilities.issueGenesis(plan, ack)
      plannedLifecycle.genesisDurable()
      genesisValue
    catch
      case primary: Throwable =>
        freezePlanned("planned-genesis-native", primary)
        throw primary
  }

  def executePlannedPrepare(genesis: DurableFreshGenesis): PreparedConfirmed = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(genesis != null && (genesis eq genesisValue), "exact original actor genesis ACK required")
    val plan = genesis.originalPlan(); requireIssuedPlan(plan)
    plannedLifecycle.beginExecution()
    try
      requireIdleAutocommit()
      val origin = plan.origin()
      // 所有native publication/rebarrier在共同DB锁之外；此ACK仍须exact原plan和source scope。
      val current = origin.slots.readCurrent(origin.nativeScope)
      if current.current() == null || current.inactive() != null then reject("exact standalone genesis no longer current")
      MySqlFreshPrepareRules.exactBytes(plan.genesisCanonicalBytes(), current.current().canonicalBytes())
      val liveGenesis = origin.slots.rebarrierCurrent(origin.nativeScope, current.current())
      MySqlFreshPrepareRules.exactBytes(genesis.ack().facts().canonicalBytes(), liveGenesis.facts().canonicalBytes())
      val qualification = qualify(Set(MySqlNoPhysicalBinding), requireUnownedWriter = true)
      val sql = transactAdmitted("planned-prepare", () => plannedLifecycle.sqlAcknowledged()) { register =>
        requireIssuedPlan(plan)
        val beforeRead = readLockedFormat3Graph()
        val writer = beforeRead.writer
        validateBootstrapAcquirePreimage(writer, qualification)
        val before = beforeRead.repository
        if !before.exactlyEquals(MySqlFullRepositorySnapshot.empty) then reject("Fresh prepare replay requires exact complete initial seed graph")
        MySqlFreshFormat3Graph.requireFreshInitialSeed(beforeRead.full20, before, beforeRead.manifest, writer,
          qualification.writerEpochObservedBeforeAcquire, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val graph = beforeRead.graph
        if !plan.beforeGraph().exactlyEquals(graph) then reject("complete original planned before M/B/repository changed")
        MySqlFreshPrepareRules.requireWriter(plan.beforeWriter(), writer)
        MySqlFreshPrepareRules.requireUnexpired(plan.targetWriter().leaseUntil(), writer.databaseNow)
        register(plan.context())
        insertPreparedIntent(plan.targetGraph().repository.bootstrapIntents.head)
        insertLegacyBinding(plan.bootstrap())
        acquireWriterAndWriteReceipt(plan.initialWriter(), plan.targetWriter().epoch(), plan.targetWriter().leaseUntil(), plan.context())
        val afterRead = readLockedFormat3Graph()
        if !plan.targetGraph().exactlyEquals(afterRead.graph) then reject("complete atomic PREPARED full20 target graph differs")
        val afterWriter = afterRead.writer
        MySqlFreshPrepareRules.requireWriter(plan.targetWriter(), afterWriter)
        MySqlFreshPrepareRules.requireUnexpired(plan.targetWriter().leaseUntil(), afterWriter.databaseNow)
        origin.held.readmit(verified, expectation, scope)
        MySqlFreshWorldBootstrap.PreparedSqlResult(afterRead.repository, afterRead.full20, afterRead.graph, plan.context(), afterWriter)
      } { result =>
        // 外层gate仍持有；另一次只读locking transaction在commit后核全部M/B/W/业务图，并checked rollback。
        readonlyBody("planned-prepare-postcommit") {
          requireIssuedPlan(plan)
          val readback = readLockedFormat3Graph()
          if !plan.targetGraph().exactlyEquals(readback.graph) then reject("postcommit complete target M/B/full20 graph differs")
          MySqlFreshPrepareRules.requireWriter(plan.targetWriter(), readback.writer)
          MySqlFreshPrepareRules.requireUnexpired(plan.targetWriter().leaseUntil(), readback.writer.databaseNow)
          origin.custody.validateLive()
        }
        plannedLifecycle.readbackConfirmed()
      }
      // sql保存于完整原plan；此处native失败保留commit/readback事实与pending，不重送SQL。
      val ack = origin.slots.publishNext(origin.nativeScope, liveGenesis, B1ResumeRecord.Kind.PREPARED_CONFIRMED, plan.confirmation().assertions())
      MySqlFreshPrepareRules.exactBytes(plan.confirmationBytes(), ack.facts().canonicalBytes())
      MySqlFreshPrepareRules.exactBytes(plan.confirmationBytes(), B1ResumeRecordCanonical.encode(ack.facts().record()))
      confirmedValue = MySqlFreshPrepareCapabilities.issueConfirmed(genesis, ack)
      plannedLifecycle.nativeConfirmed()
      confirmedValue
    catch
      case primary: Throwable =>
        freezePlanned("planned-prepare-execute-or-native-confirm", primary)
        throw primary
  }

  /** 新正常路径；旧同步 B1/Resume BLOCK 不因本issuer而解除。 */
  def plannedCertifyObservation: MySqlFreshCertifyObservation = synchronized {
    requireOwnedThread()
    certifyLifecycle.observe(Option(certifyPlanValue), Option(certifyIntentValue), certifyDraft, certifyRetirements)
  }

  def publishPreparedMarker(value: PreparedConfirmed): CurrentMarker = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(value != null && (value eq confirmedValue), "exact current actor PreparedConfirmed required")
    certifyLifecycle.beginMarker()
    try
      requireCurrentPrepared(value); requireIdleAutocommit()
      val prepare = value.originalPlan(); val origin = prepare.origin()
      requireCurrentNative(origin, value.ack(), prepare.confirmationBytes())
      qualify(Set(MySqlLegacyPhysicalBinding(prepare.bootstrap().bindingUuid, 2)), requireUnownedWriter = false)
      readonlyWithGate("planned-marker-prepared-read") {
        requireCertifyOrigin(prepare)
        requireLockedGraph(prepare.targetGraph(), prepare.targetWriter())
        MySqlBootstrapSnapshotTransitions.requirePreparedMatches(prepare.targetGraph().repository, prepare.bootstrap(),
          origin.fresh.sourceWorldIdentity(), MySqlFreshWorldBootstrap.receiptHash(origin.fresh))
      }
      // 没有SQL事务；真实DB prepared/current source先核过，才允许首次marker publication。
      requireIdleAutocommit(); requireCurrentPrepared(value)
      requireCurrentNative(origin, value.ack(), prepare.confirmationBytes())
      val location = provisioner.ensureCanonicalMarkerLocation(origin.custody)
      val ack = markerPublisher.publish(location.barrierRequest())
      location.validateLive(); origin.custody.validateLive()
      MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(ack, location, prepare.bootstrap())
      currentMarkerValue = MySqlFreshCertifyCapabilities.issueMarker(value, location, ack)
      certifyLifecycle.markerAcknowledged()
      currentMarkerValue
    catch case primary: Throwable =>
      freezeCertify("planned-marker-publication", primary); throw primary
  }

  def planFreshCertify(marker: CurrentMarker): PlannedFreshCertify = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(marker != null && (marker eq currentMarkerValue), "exact actor marker capability required")
    certifyLifecycle.beginPlanning()
    try
      requireCurrentMarker(marker); requireIdleAutocommit()
      val prepare = marker.prepared().originalPlan(); val origin = prepare.origin()
      requireCurrentNative(origin, marker.prepared().ack(), prepare.confirmationBytes())
      rebarrierMarker(marker)
      qualify(Set(MySqlLegacyPhysicalBinding(prepare.bootstrap().bindingUuid, 2)), requireUnownedWriter = false)
      val attemptId = UUID.randomUUID()
      val draft = readonlyWithGate("planned-certify-plan") {
        requireCertifyOrigin(prepare)
        val beforeRead = readLockedFormat3Graph()
        val manifest = beforeRead.manifest
        val writer = beforeRead.writer
        val beforeRepository = beforeRead.repository
        val before = beforeRead.graph
        if !prepare.targetGraph().exactlyEquals(before) then reject("certify locked before differs from complete prepared full20 target")
        MySqlFreshPrepareRules.requireWriter(prepare.targetWriter(), writer)
        MySqlFreshPrepareRules.requireUnexpired(prepare.targetWriter().leaseUntil(), writer.databaseNow)
        verifyWriterReceipt(writer, prepare.context(), scope.bootUuid, prepare.targetWriter().epoch(), prepare.targetWriter().leaseUntil())
        val bootstrap = prepare.bootstrap()
        val targetRepository = MySqlBootstrapSnapshotTransitions.complete(beforeRepository, bootstrap,
          origin.fresh.sourceWorldIdentity(), MySqlFreshWorldBootstrap.receiptHash(origin.fresh), MySqlFreshWorldBootstrap.parseAckHash(marker.ack().markerSha256()))
        val beforeW = MySqlFreshPrepareRules.writer(writer)
        val normalizedWriter = MySqlFreshFormat3WriterTarget(writer.bindingUuid, writer.writerEpoch,
          writer.ownerBootUuid, writer.leaseUntil, bootstrap.certifyWriteUuid)
        val context = format3OperationContext("CERTIFY", bootstrap, beforeRead.full20, beforeRepository,
          targetRepository, normalizedWriter)
        val targetWriterRow = writer.copy(lastOperationUuid = Some(context.operationUuid),
          lastFingerprint = Some(context.canonicalFingerprintSha256))
        val targetFull20 = MySqlFreshFormat3Graph.targetSnapshot(beforeRead.full20, beforeRepository, targetRepository, targetWriterRow)
        val target = MySqlB1CommonGraph.captureFormat3(manifest, targetRepository.recovery.binding, targetRepository,
          targetFull20, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
        val targetW = new WriterTuple(targetWriterRow.bindingUuid, targetWriterRow.writerEpoch,
          targetWriterRow.ownerBootUuid.orNull, targetWriterRow.leaseUntil,
          targetWriterRow.lastOperationUuid.orNull, targetWriterRow.lastFingerprint.map(_.toArray).orNull)
        val beforeAssertion = writerAssertions(beforeW, prepare.context())
        val targetAssertion = writerAssertions(targetW, context)
        val attempt = new B1ResumeRecord.AttemptAssertions(attemptId, B1ResumeRecord.Phase.CERTIFY, bootstrap.certifyWriteUuid,
          scope.bootGeneration, origin.held.connectionGenerationId, origin.held.actualConnectionId, origin.held.namespace.lockName(),
          MySqlFreshPrepareRules.operation(context), beforeAssertion, targetAssertion, before.toResumeAssertions, target.toResumeAssertions)
        val previous = marker.prepared().confirmedRecord(); val original = previous.assertions()
        val facts = MySqlFreshCertifyRules.markerFacts(marker.ack(), origin.custody.generation())
        val pending = new B1ResumeRecord.FullStateAssertions(original.currentOwner(), original.previousOwner(), original.retirement(),
          original.originalPrepareWitness(), beforeAssertion, targetAssertion, attempt, original.latestSettlement(), facts, null)
        val intent = MySqlFreshCertifyRules.successor(previous, B1ResumeRecord.Kind.NATIVE_ACK_CERTIFY_INTENT, pending)
        certifyDraft = Some(intent) // preserve exact attempt/graphs/W even if capacity or cleanup fails
        val confirmed = new B1ResumeRecord.FullStateAssertions(original.currentOwner(), original.previousOwner(), original.retirement(),
          original.originalPrepareWitness(), targetAssertion, targetAssertion, null, original.latestSettlement(), facts, attempt)
        val confirmation = MySqlFreshCertifyRules.successor(intent, B1ResumeRecord.Kind.CERTIFIED, confirmed)
        val intentBytes = B1ResumeRecordCanonical.encode(intent)
        val confirmationBytes = B1ResumeRecordCanonical.encode(confirmation) // full double witness/marker 64KiB before native intent or DML
        (context, before, target, beforeW, targetW, intent, confirmation, intentBytes, confirmationBytes)
      }
      certifyPlanValue = MySqlFreshCertifyCapabilities.issuePlan(marker, attemptId, draft._1, draft._2, draft._3,
        draft._4, draft._5, draft._6, draft._8, draft._7, draft._9)
      certifyLifecycle.planned()
      certifyPlanValue
    catch case primary: Throwable =>
      freezeCertify("planned-certify-plan", primary); throw primary
  }

  /** 仅使用实际同 actor genesis ACK；保存部分 holder 后才执行原生副作用。 */
  private[mysql] def publishOriginalPrepareWitness(plan: PlannedFreshPrepare)
      : WindowsB1Full20WitnessStore.NativeArtifactDurability = synchronized {
    requireOwnedThread(); requireNotUnknown()
    try
      requireIssuedPlan(plan); requireOriginalWitnessRoot(originalWitnessRoot)
      require(genesisValue != null && (genesisValue.originalPlan() eq plan), "actual original genesis ACK required")
      require(originalPrepareWitnessDraft == null, "prepare witness is one-shot; failure cannot retry")
      originalPrepareWitnessDraft = MySqlFull20OriginalWitness.draft(plan, originalWitnessRoot)
      val witness = MySqlFull20OriginalWitness.materialize(originalPrepareWitnessDraft)
      prepareWitnessTransfer = witness.transferForNative()
      val origin = plan.origin()
      val nativeScope = origin.slots.witnessScope(origin.nativeScope, genesisValue.ack())
      prepareWitnessStore = origin.slots.full20WitnessStore(nativeScope, prepareWitnessTransfer)
      prepareWitnessAck = prepareWitnessStore.publish(prepareWitnessTransfer)
      prepareWitnessStore.readCold(prepareWitnessAck)
      prepareWitnessTransfer.completeColdComparison()
      prepareWitnessAck
    catch
      case primary: Throwable =>
        freezePlanned("original-prepare-full20-native-witness", primary)
        throw primary
  }

  /** 原 prepared/current-marker 资格仅约束来源；native ACK 不替代 SQL/V4 certification。 */
  private[mysql] def publishOriginalCertifyWitness(plan: PlannedFreshCertify)
      : WindowsB1Full20WitnessStore.NativeArtifactDurability = synchronized {
    requireOwnedThread(); requireNotUnknown()
    try
      requireCertifyPlan(plan); requireOriginalWitnessRoot(originalWitnessRoot)
      require(certifyWitnessTransfer == null, "certify witness publication is one-shot")
      val witness = captureOriginalFull20Witness(plan)
      certifyWitnessTransfer = witness.transferForNative()
      val prepared = plan.marker().prepared()
      val origin = prepared.originalPlan().origin()
      val nativeScope = origin.slots.witnessScope(origin.nativeScope, prepared.ack())
      certifyWitnessStore = origin.slots.full20WitnessStore(nativeScope, certifyWitnessTransfer)
      certifyWitnessAck = certifyWitnessStore.publish(certifyWitnessTransfer)
      certifyWitnessStore.readCold(certifyWitnessAck)
      certifyWitnessTransfer.completeColdComparison()
      certifyWitnessAck
    catch
      case primary: Throwable =>
        freezeCertify("original-certify-full20-native-witness", primary)
        throw primary
  }

  /** 实际 private plan 发行后捕获完整 DATA；不修改现 V4 SQL/native certification 协议。 */
  private[mysql] def captureOriginalFull20Witness(plan: PlannedFreshCertify): MySqlFull20OriginalWitness = synchronized {
    requireOwnedThread(); requireNotUnknown()
    try
      requireCertifyPlan(plan); requireOriginalWitnessRoot(originalWitnessRoot)
      require(originalWitnessDraft == null, "original full20 witness capture is one-shot")
      originalWitnessDraft = MySqlFull20OriginalWitness.draft(plan, originalWitnessRoot)
      MySqlFull20OriginalWitness.materialize(originalWitnessDraft)
    catch
      case primary: Throwable =>
        freezeCertify("original-full20-witness-capture", primary)
        throw primary
  }

  def publishCertifyIntent(plan: PlannedFreshCertify): DurableCertifyIntent = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(plan != null && (plan eq certifyPlanValue), "exact actor planned certify required")
    certifyLifecycle.beginIntent()
    try
      requireCertifyPlan(plan); requireIdleAutocommit()
      rebarrierMarker(plan.marker())
      val prepare = plan.marker().prepared().originalPlan(); val origin = prepare.origin()
      val live = requireCurrentNative(origin, plan.marker().prepared().ack(), prepare.confirmationBytes())
      val retained = retireForSuccessor(origin, live)
      val ack = origin.slots.publishNext(origin.nativeScope, retained, B1ResumeRecord.Kind.NATIVE_ACK_CERTIFY_INTENT, plan.intentRecord().assertions())
      MySqlFreshCertifyRules.requireRecord(plan.intentCanonicalBytes(), ack.facts())
      requireCertifyPlan(plan)
      certifyIntentValue = MySqlFreshCertifyCapabilities.issueIntent(plan, ack)
      certifyLifecycle.intentDurable()
      certifyIntentValue
    catch case primary: Throwable =>
      freezeCertify("planned-certify-intent-native", primary); throw primary
  }

  def executePlannedCertify(value: DurableCertifyIntent): CertifiedConfirmed = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(value != null && (value eq certifyIntentValue), "exact same actor durable certify intent required")
    val plan = value.originalPlan()
    require(plan eq certifyPlanValue, "exact current actor certify plan required")
    certifyLifecycle.beginExecution()
    try
      requireCertifyPlan(plan); requireIdleAutocommit(); rebarrierMarker(plan.marker())
      val prepare = plan.marker().prepared().originalPlan(); val origin = prepare.origin()
      val live = requireCurrentNative(origin, value.ack(), plan.intentCanonicalBytes())
      qualify(Set(MySqlLegacyPhysicalBinding(prepare.bootstrap().bindingUuid, 2)), requireUnownedWriter = false)
      transactAdmitted("planned-certify", () => certifyLifecycle.sqlAcknowledged()) { register =>
        requireCertifyPlan(plan)
        val beforeRead = readLockedFormat3Graph()
        val writer = beforeRead.writer
        if !plan.beforeGraph().exactlyEquals(beforeRead.graph) then reject("certify complete locked before full20 graph changed")
        MySqlFreshPrepareRules.requireWriter(plan.beforeWriter(), writer)
        MySqlFreshPrepareRules.requireUnexpired(plan.beforeWriter().leaseUntil(), writer.databaseNow)
        verifyWriterReceipt(writer, prepare.context(), scope.bootUuid, plan.beforeWriter().epoch(), plan.beforeWriter().leaseUntil())
        register(plan.context())
        certifyBinding(prepare.bootstrap())
        completeIntent(beforeRead.repository.bootstrapIntents.head, MySqlFreshWorldBootstrap.parseAckHash(plan.marker().ack().markerSha256()))
        updateWriterReceipt(writer, prepare.context(), plan.context())
        val afterRead = readLockedFormat3Graph()
        if !plan.targetGraph().exactlyEquals(afterRead.graph) then reject("CERTIFIED full20 target graph differs")
        MySqlFreshPrepareRules.requireWriter(plan.targetWriter(), afterRead.writer)
        MySqlFreshPrepareRules.requireUnexpired(plan.targetWriter().leaseUntil(), afterRead.writer.databaseNow)
        origin.held.readmit(verified, expectation, scope)
        MySqlFreshWorldBootstrap.PreparedSqlResult(afterRead.repository, afterRead.full20, afterRead.graph, plan.context(), afterRead.writer)
      } { _ =>
        readonlyBody("planned-certify-postcommit") {
          requireCertifyPlan(plan)
          requireLockedGraph(plan.targetGraph(), plan.targetWriter())
        }
        certifyLifecycle.readbackConfirmed()
      }
      // SQL acknowledged/readback remains retained if retirement or canonical write fails. Never resend SQL.
      requireIdleAutocommit(); rebarrierMarker(plan.marker())
      val retained = retireForSuccessor(origin, live)
      val ack = origin.slots.publishNext(origin.nativeScope, retained, B1ResumeRecord.Kind.CERTIFIED, plan.confirmation().assertions())
      MySqlFreshCertifyRules.requireRecord(plan.confirmationBytes(), ack.facts())
      certifiedValue = MySqlFreshCertifyCapabilities.issueCertified(value, ack)
      certifyLifecycle.nativeConfirmed()
      certifiedValue
    catch case primary: Throwable =>
      freezeCertify("planned-certify-execute-or-native-confirm", primary); throw primary
  }

  def admitCertifiedRuntime(value: CertifiedConfirmed, coordinator: MySqlWriterFenceCoordinator): MySqlBusinessAdmission = synchronized {
    requireOwnedThread(); requireNotUnknown()
    require(value != null && (value eq certifiedValue), "exact actor current CertifiedConfirmed required")
    require(coordinator != null, "actual normal coordinator required")
    if admittedValue != null then
      if admittedCoordinator eq coordinator then return coordinator.requireBusinessAdmission(verified, scope)
      reject("planned Fresh admission belongs to a different coordinator")
    certifyLifecycle.beginAudit()
    try
      verifyCurrentCertified(value)
      val plan = value.originalPlan(); val bootstrap = plan.marker().prepared().originalPlan().bootstrap()
      val qualification = qualify(Set(MySqlCertifiedPhysicalBinding(bootstrap.bindingUuid, 2)), requireUnownedWriter = false)
      if qualification.writerEpochObservedBeforeAcquire != plan.targetWriter().epoch() ||
        qualification.writerOwnerBootObservedBeforeAcquire != Some(scope.bootUuid) then reject("certified audit observed another held W")
      val before = readWriter(lock = false)
      MySqlFreshPrepareRules.requireWriter(plan.targetWriter(), before)
      verifyWriterReceipt(before, plan.context(), scope.bootUuid, plan.targetWriter().epoch(), plan.targetWriter().leaseUntil())
      // Existing owned epoch/lease: no acquire, renewal, or target rewrite.
      val fence = MySqlWriterFenceExpectation.fromAcquiredFence(bootstrap.bindingUuid, scope.bootUuid,
        before.writerEpoch, before.leaseUntil, before.databaseNow, System.nanoTime())
      val token = new MySqlWriterFenceToken(verified, scope, qualification, fence)
      val observation = auditor.readCurrentObjectManifest(verified, scope)
      val receipt = auditor.auditAfterWriterAcquire(observation, qualification, scope, fence)
      verifyCurrentCertified(value)
      val after = readWriter(lock = false)
      MySqlFreshPrepareRules.requireWriter(plan.targetWriter(), after)
      verifyWriterReceipt(after, plan.context(), scope.bootUuid, plan.targetWriter().epoch(), plan.targetWriter().leaseUntil())
      completionProofValue = MySqlFreshCertifyCapabilities.issueCompletion(this, value, coordinator, verified, scope, after, token, receipt)
      val admission = coordinator.admitPlannedFreshWorldBootstrap(completionProofValue)
      admittedCoordinator = coordinator; admittedValue = admission
      certifyLifecycle.admitted()
      admission
    catch case primary: Throwable =>
      freezeCertify("planned-certified-fresh-audit-or-admission", primary); throw primary
  }

  /** Coordinator回调验证actual current capability，不接caller事实签权。 */
  private[mysql] def validatePlannedCompletionProof(proof: MySqlFreshCertifyCapabilities.CompletionProof,
      expected: MySqlSchemaAuditExpectation, target: MySqlWriterFenceCoordinator): Unit = synchronized {
    requireOwnedThread(); requireNotUnknown()
    if proof == null || (proof ne completionProofValue) || (proof.owner() ne this) ||
      (proof.verified() ne verified) || (proof.scope() ne scope) then reject("foreign planned completion issuer/session/generation")
    requireCurrentCertified(proof.certified())
    val certify = proof.certified().originalPlan()
    val original = certify.marker().prepared().originalPlan()
    val origin = original.origin()
    val coordinator = proof.coordinator()
    val plan = original.bootstrap()
    val world = origin.world; val fresh = origin.fresh; val custody = origin.custody
    val location = certify.marker().location(); val ack = certify.marker().ack()
    val preparedGraph = original.targetGraph()
    val certifyBeforeGraph = certify.beforeGraph()
    val completedGraph = certify.targetGraph()
    if preparedGraph.format3Snapshot.isEmpty || certifyBeforeGraph.format3Snapshot.isEmpty || completedGraph.format3Snapshot.isEmpty ||
      !preparedGraph.exactlyEquals(certifyBeforeGraph) then
      reject("B1 certify proof must retain the exact PREPARED and full20 before/after graphs")
    val preparedSnapshot = preparedGraph.repository
    val completedSnapshot = certify.targetGraph().repository
    val context = certify.context(); val writer = proof.writer(); val token = proof.token(); val receipt = proof.receipt()
    MySqlFreshPrepareRules.requireWriter(certify.targetWriter(), writer)
    if expected == null || target == null || (coordinator ne target) ||
      !MySqlFreshWorldBootstrap.planMatchesExpectation(plan, expected) || context.operationUuid != plan.certifyWriteUuid ||
      context.bootUuid != scope.bootUuid || context.writerEpoch != token.writerEpoch ||
      context.bindingUuid != plan.bindingUuid ||
      completedSnapshot.recovery.binding != Some(PhysicalRecoveryBinding(plan.bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)) ||
      completedSnapshot.bootstrapIntents.size != 1 then reject("B1 completion plan, final graph, and operation context differ")
    verifyCurrentCertified(proof.certified())
    requireOriginWorld(origin)
    val expectedComplete = MySqlBootstrapSnapshotTransitions.complete(
      preparedSnapshot, plan, fresh.sourceWorldIdentity(), MySqlFreshWorldBootstrap.receiptHash(fresh), MySqlFreshWorldBootstrap.parseAckHash(ack.markerSha256()))
    if !completedSnapshot.exactlyEquals(expectedComplete) then reject("B1 complete graph differs from PREPARED source and marker ACK")
    if completedGraph.format3Snapshot.isEmpty then reject("B1 final completion omits the actual twenty-relation typed snapshot")
    val intent = MySqlBootstrapSnapshotTransitions.requireCompleteMatches(
      completedSnapshot, plan, fresh.sourceWorldIdentity(), MySqlFreshWorldBootstrap.receiptHash(fresh), MySqlFreshWorldBootstrap.parseAckHash(ack.markerSha256()))
    if intent.stage != MySqlBootstrapIntentStage.Complete || intent.markerAckSha256 != Some(MySqlFreshWorldBootstrap.parseAckHash(ack.markerSha256())) then
      reject("B1 durable intent does not carry the exact marker ACK")
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(ack, location, plan)
    if writer.bindingUuid != plan.bindingUuid || writer.ownerBootUuid != Some(scope.bootUuid) ||
      writer.writerEpoch != token.writerEpoch || writer.leaseUntil != token.leaseUntil ||
      writer.lastOperationUuid != Some(plan.certifyWriteUuid) ||
      writer.lastFingerprint != Some(context.canonicalFingerprintSha256) ||
      !writer.leaseUntil.isAfter(writer.databaseNow) then
      reject("B1 writer readback does not prove the exact live certify receipt and epoch")
    if (token.verified ne verified) || (token.scope ne scope) ||
      token.qualification.physicalBinding != MySqlCertifiedPhysicalBinding(plan.bindingUuid, 2) ||
      token.fence.ownerBootUuid != scope.bootUuid || token.fence.bindingUuid != plan.bindingUuid ||
      token.fence.epoch != writer.writerEpoch || token.fence.leaseUntil != writer.leaseUntil then
      reject("runtime token is not the exact certified B1 writer tuple")
    receipt.assertCurrent(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration,
      expected.serverUuid, expected.schema, plan.bindingUuid, expected.profile, expected.expectedPhysicalFormat,
      verified, Some(token.fence), System.nanoTime())
    if receipt.writerEpoch != writer.writerEpoch || receipt.bindingUuid != plan.bindingUuid ||
      receipt.serverUuid != expected.serverUuid || receipt.schema != expected.schema ||
      !receipt.ddlSha256.equalsIgnoreCase(expected.ddlSha256) ||
      !receipt.objectManifestSha256.equalsIgnoreCase(expected.objects.canonicalSha256) then
      reject("normal post-acquire audit receipt differs from the exact B1 target")
  }

  private def requireCurrentPrepared(value: PreparedConfirmed): Unit =
    require(value != null && (value eq confirmedValue), "exact current actor PreparedConfirmed required")
    requireIssuedPlan(value.originalPlan())
  private def requireCurrentMarker(value: CurrentMarker): Unit =
    require(value != null && (value eq currentMarkerValue), "exact actor marker capability required")
    requireCurrentPrepared(value.prepared())
    value.location().validateLive()
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(value.ack(), value.location(), value.prepared().originalPlan().bootstrap())
  private def requireCertifyPlan(value: PlannedFreshCertify): Unit =
    require(value != null && (value eq certifyPlanValue), "exact actor planned certify required")
    require(value.marker() eq currentMarkerValue, "exact current marker issuer required")
    val original = value.marker().prepared().originalPlan()
    requireCertifyOrigin(original)
    val origin = original.origin()
    val attempt = value.intentRecord().assertions().pendingAttempt()
    require(attempt.connectionGeneration() == origin.held.connectionGenerationId && attempt.connectionId() == origin.held.actualConnectionId &&
      attempt.bootGeneration() == scope.bootGeneration && attempt.namespace() == origin.held.namespace.lockName(), "certify session generation changed")
  /** SQL transaction内部只核真实对象/session；native live validation在事务外独立完成。 */
  private def requireCertifyOrigin(prepare: PlannedFreshPrepare): Unit =
    require(prepare != null && (prepare eq plannedValue) && confirmedValue != null &&
      (confirmedValue.originalPlan() eq prepare), "exact original prepared actor plan required")
    val origin = prepare.origin()
    require((origin.issuer eq this) && origin.nonce == plannedNonce && (origin.verified eq verified) && (origin.scope eq scope),
      "certify origin issuer/session/scope changed")
    requireOriginWorld(origin)
    origin.held.readmit(verified, expectation, scope)
  private def requireCurrentCertified(value: CertifiedConfirmed): Unit =
    require(value != null && (value eq certifiedValue), "exact actor current CertifiedConfirmed required")
    requireCertifyPlan(value.originalPlan())
  private def requireCurrentNative(origin: Origin, ack: WindowsB1TwoSlotStore.NativeRecordDurability,
      expected: Array[Byte]): WindowsB1TwoSlotStore.NativeRecordDurability =
    requireIdleAutocommit()
    val current = origin.slots.readCurrent(origin.nativeScope).current()
    MySqlFreshCertifyRules.requireRecord(expected, current)
    if current.fileIdentity() != ack.facts().fileIdentity() then reject("current canonical native identity changed")
    val live = origin.slots.rebarrierCurrent(origin.nativeScope, ack.facts())
    MySqlFreshCertifyRules.requireRecord(expected, live.facts())
    live
  private def rebarrierMarker(marker: CurrentMarker): Unit =
    requireIdleAutocommit(); requireCurrentMarker(marker)
    val actual = markerPublisher.rebarrierExisting(marker.location().barrierRequest(), marker.ack())
    MySqlFreshCertifyRules.requireSameMarker(marker.ack(), actual)
  private def retireForSuccessor(origin: Origin, current: WindowsB1TwoSlotStore.NativeRecordDurability): WindowsB1TwoSlotStore.NativeRecordDurability =
    requireIdleAutocommit()
    val records = origin.slots.readCurrent(origin.nativeScope)
    MySqlFreshCertifyRules.requireRecord(current.facts().canonicalBytes(), records.current())
    if records.inactive() == null then current
    else
      val result = origin.slots.retireInactiveSlot(origin.nativeScope, current, records.inactive())
      certifyRetirements :+= result // actual checked observation only, not crash-durable termination proof
      result.retainedRecord()
  private def requireLockedGraph(expected: MySqlB1CommonGraph, expectedW: WriterTuple): Unit =
    val actual = readLockedFormat3Graph()
    if !expected.exactlyEquals(actual.graph) then reject("complete locked M/B/full20 graph differs from fixed target")
    MySqlFreshPrepareRules.requireWriter(expectedW, actual.writer)
    MySqlFreshPrepareRules.requireUnexpired(expectedW.leaseUntil(), actual.writer.databaseNow)

  /** 先锁定 M→B→W，再通过真实 Format3 reader 锁完并保留全部二十张表。 */
  private def readLockedFormat3Graph(): MySqlFreshWorldBootstrap.Format3ReadFacts =
    val rollbackOnly = Option(activeRollbackOnly).getOrElse(reject("Fresh full20 read requires the active owned transaction gate"))
    val manifest = lockManifest(); validateManifest(manifest)
    val binding = lockBinding()
    val writer = readWriter(lock = true)
    val gateOwner = MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly)
    val readerOwner = new MySqlFreshWorldBootstrap.Format3ReadOwner(
      this, gateOwner, verified, scope, expectation, rollbackOnly, ownerThread, manifest, binding, writer, originalWitnessRoot)
    val profile = expectation.format3Inputs.getOrElse(reject("Fresh Format3 reader requires explicit independent format3 inputs")).limitProfile
    val format3 = new MySqlFormat3FullSnapshotReader(verified, scope, expectation.schema, profile)
      .readWithinFreshBootstrapTransaction(readerOwner)
    // The transition value is decoded from these exact locked Format3 rows; no legacy SQL shape is queried.
    val repository = MySqlFreshFormat3TransitionProjection.project(format3)
    val graph = MySqlB1CommonGraph.captureFormat3(manifest, binding, repository, format3,
      expectation.format3Inputs.getOrElse(reject("Fresh Format3 independent inputs disappeared")))
    MySqlFreshFormat3Graph.requireWriterMatches(format3, writer)
    MySqlFreshWorldBootstrap.Format3ReadFacts(manifest, binding, writer, repository, format3, graph)
  private def writerAssertions(writer: WriterTuple, context: MySqlOperationContext): B1ResumeRecord.WriterAssertions =
    val slot = new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid, context.writerEpoch, context.bindingUuid,
      context.operationUuid, context.canonicalFingerprintSha256.toArray)
    new B1ResumeRecord.WriterAssertions(writer.binding(), writer.epoch(), writer.ownerBoot(), writer.leaseUntil(), slot)
  private def verifyCurrentCertified(value: CertifiedConfirmed): Unit =
    requireCurrentCertified(value); requireIdleAutocommit()
    val plan = value.originalPlan(); val origin = plan.marker().prepared().originalPlan().origin()
    requireCurrentNative(origin, value.ack(), plan.confirmationBytes())
    rebarrierMarker(plan.marker())
    readonlyWithGate("planned-certified-current-read") {
      requireCertifyPlan(plan); requireLockedGraph(plan.targetGraph(), plan.targetWriter())
    }
  private def freezeCertify(stage: String, cause: Throwable): Unit =
    certifyLifecycle.fail(stage, cause); noteUnknown(stage, cause)
  private final case class NormalPrepareDraft(origin: MySqlNormalPreGenesisCapabilities.Source,
      attempt: UUID, bootstrap: MySqlBootstrapPlanIdentity, context: MySqlOperationContext,
      before: MySqlB1CommonGraph, target: MySqlB1CommonGraph, writer: MySqlBusinessWriterRow,
      beforeW: WriterTuple, targetW: WriterTuple, genesis: B1ResumeRecord, genesisBytes: Vector[Byte],
      confirmation: B1ResumeRecord, confirmationBytes: Vector[Byte])

  private final case class FreshPrepareDraft(origin: Origin, attempt: UUID, bootstrap: MySqlBootstrapPlanIdentity,
      context: MySqlOperationContext, before: MySqlB1CommonGraph, target: MySqlB1CommonGraph, writer: MySqlBusinessWriterRow,
      beforeW: WriterTuple, targetW: WriterTuple, genesis: B1ResumeRecord, genesisBytes: Vector[Byte],
      confirmation: B1ResumeRecord, confirmationBytes: Vector[Byte])

  private def requireIssuedPlan(plan: PlannedFreshPrepare): Unit =
    require(plan != null && (plan eq plannedValue), "exact same actor issued plan required")
    val origin = plan.origin()
    require((origin.issuer eq this) && origin.nonce == plannedNonce && (origin.verified eq verified) && (origin.scope eq scope),
      "original plan issuer/session/scope changed")
    origin.held.readmit(verified, expectation, scope)
    val assertions = plan.genesisRecord().assertions()
    val attempt = if origin.normal == null then assertions.originalPrepareWitness() else assertions.pendingAttempt()
    require(attempt != null, "exact original prepare witness required")
    require(attempt.connectionGeneration() == origin.held.connectionGenerationId && attempt.connectionId() == origin.held.actualConnectionId &&
      attempt.bootGeneration() == scope.bootGeneration && attempt.namespace() == origin.held.namespace.lockName(), "planned session generation differs")
    requireOriginWorld(origin)

  private def freezePlanned(stage: String, primary: Throwable): Unit =
    plannedLifecycle.fail(stage, primary)
    noteUnknown(stage, primary) // 仅此owned actor冻结；不停止全局server、不清理world/records/W。

  private def readonlyWithGate[A](stage: String)(body: => A): A =
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nested) => throw nested
      case None => ()
    if activeRollbackOnly != null then
      val failure = new IllegalStateException("Fresh bootstrap already retains an active transaction owner")
      rollbackOnly.mark(failure)
      MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
      throw failure
    activeRollbackOnly = rollbackOnly
    var primary: Throwable = null
    try
      val result = readonlyBody(stage)(body)
      rollbackOnly.cause.foreach(throw _)
      result
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      try MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
      catch case cleanup: Throwable =>
        if primary != null then MySqlFencedTransactionSupport.addSuppressed(primary, cleanup)
        else throw new MySqlBootstrapCleanupUncertainException(stage, None, cleanup)
      finally activeRollbackOnly = null

  /** 只做locking reads；不设JDBC READ ONLY从而误拒FOR UPDATE，不提交，不发表native记录。 */
  private def readonlyBody[A](stage: String)(body: => A): A =
    requireOwnedThread(); requireIdleAutocommit()
    val oldIsolation = connection.getTransactionIsolation
    var beginAttempted = false
    var primary: Throwable = null
    var result: Option[A] = None
    try
      beginAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        reject("B1 read transaction could not establish REPEATABLE READ")
      connection.setAutoCommit(false)
      if connection.getAutoCommit then reject("planning locking transaction did not disable autocommit")
      verified.readmitSessionBarrier(expectation, scope)
      result = Some(body)
    catch case failure: Throwable => primary = failure
    if beginAttempted then
      val actions = new MySqlFreshPrepareCleanupActions:
        def rollback(): Unit = connection.rollback()
        def restoreAndObserveIdle(): Unit =
          connection.setAutoCommit(true)
          if !connection.getAutoCommit then reject("planning rollback did not restore idle autocommit")
          connection.setTransactionIsolation(oldIsolation)
          if connection.getTransactionIsolation != oldIsolation then reject("planning rollback did not restore the prior isolation")
      MySqlFreshPrepareReadOnlyCleanup.finish(actions, Option(primary)) match
        case Left(original) => throw new MySqlBootstrapCleanupUncertainException(stage, None, original)
        case Right(_) => ()
    if primary != null then throw primary
    result.get

  /** 只接受本 boot 新建并仍由同一 opaque native factory 持有的世界。 */
  def prepare(world: WindowsWorldProvisioner.ProvisionedWorld): MySqlFreshWorldBootstrap.Prepared = synchronized {
    requireOwnedThread()
    requireNotUnknown()
    MySqlB1V3MutationGate.requireImplementedIssuers()
    if prepared != null then
      if world eq prepared.world then
        freshWorldStillLive(world, prepared.fresh, prepared.custody)
        return prepared
      reject("this B1 service already owns a different Fresh provision proof")

    val (fresh, custody) = validateFreshWorld(world)
    val plan = planIdentity(fresh.plan())
    val sourceIdentity = fresh.sourceWorldIdentity()
    val receiptHash = MySqlFreshWorldBootstrap.sha256(fresh.receiptCanonicalBytes())
    val qualification = qualify(Set(MySqlNoPhysicalBinding), requireUnownedWriter = true)
    val committed = transact("prepare") { register =>
      val beforeRead = readLockedFormat3Graph()
      validateManifest(beforeRead.manifest)
      if beforeRead.binding.nonEmpty then reject("fresh B1 prepare requires the physical binding row to be absent")
      val writer = beforeRead.writer
      validateBootstrapAcquirePreimage(writer, qualification)
      val before = beforeRead.repository
      if !before.exactlyEquals(MySqlFullRepositorySnapshot.empty) then
        reject("fresh B1 prepare requires the exact typed transition projection to be empty")
      MySqlFreshFormat3Graph.requireFreshInitialSeed(beforeRead.full20, before, beforeRead.manifest, writer,
        qualification.writerEpochObservedBeforeAcquire, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
      val desired = MySqlBootstrapSnapshotTransitions.prepareFresh(before, plan, sourceIdentity, receiptHash)
      val nextEpoch = Math.addExact(writer.writerEpoch, 1L)
      val leaseUntil = writer.databaseNow.plusNanos(leaseNanos)
      val normalizedWriter = MySqlFreshFormat3WriterTarget(plan.bindingUuid, nextEpoch, Some(scope.bootUuid), leaseUntil, plan.prepareWriteUuid)
      val context = format3OperationContext("PREPARE", plan, beforeRead.full20, before, desired, normalizedWriter)
      val targetWriter = writer.copy(bindingUuid = plan.bindingUuid, writerEpoch = nextEpoch,
        ownerBootUuid = Some(scope.bootUuid), leaseUntil = leaseUntil, lastOperationUuid = Some(context.operationUuid),
        lastFingerprint = Some(context.canonicalFingerprintSha256))
      val targetFull20 = MySqlFreshFormat3Graph.targetSnapshot(beforeRead.full20, before, desired, targetWriter)
      val targetGraph = MySqlB1CommonGraph.captureFormat3(beforeRead.manifest, desired.recovery.binding, desired,
        targetFull20, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
      register(context)
      insertPreparedIntent(desired.bootstrapIntents.head)
      insertLegacyBinding(plan)
      acquireWriterAndWriteReceipt(writer, nextEpoch, leaseUntil, context)
      val actual = readLockedFormat3Graph()
      if !targetGraph.exactlyEquals(actual.graph) then reject("B1 PREPARED full20 intent and LEGACY binding differ from its exact target")
      val writerAfter = actual.writer
      verifyWriterReceipt(writerAfter, context, scope.bootUuid, nextEpoch, leaseUntil)
      MySqlFreshWorldBootstrap.PreparedSqlResult(actual.repository, actual.full20, actual.graph, context, writerAfter)
    } { result =>
      verifyPostCommit(result.graph, result.context, scope.bootUuid, result.writer.writerEpoch, result.writer.leaseUntil)
      freshWorldStillLive(world, fresh, custody)
    }
    prepared = new MySqlFreshWorldBootstrap.Prepared(this, world, fresh, custody,
      plan, sourceIdentity, receiptHash, committed.snapshot, committed.full20, committed.graph, committed.context, committed.writer)
    prepared
  }

  /** 在 DB PREPARED 已持久化后发布唯一固定路径 marker；同一 ACK 可被重用。 */
  def publishMarker(value: MySqlFreshWorldBootstrap.Prepared): MySqlFreshWorldBootstrap.MarkerAcknowledgement = synchronized {
    requireOwnedThread()
    requireNotUnknown()
    MySqlB1V3MutationGate.requireImplementedIssuers()
    requirePrepared(value)
    if published != null then
      published.location.validateLive()
      published.prepared.custody.validateLive()
      MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(published.ack, published.location, published.prepared.plan)
      return published
    value.custody.validateLive()
    val location = provisioner.ensureCanonicalMarkerLocation(value.custody)
    location.validateLive()
    val request = location.barrierRequest()
    if request.operationUuid() != value.plan.bootstrapOperationUuid ||
      request.bindingUuid() != value.plan.bindingUuid || request.physicalFormat() != 2 ||
      request.canonicalPath().toAbsolutePath().normalize() != location.canonicalPath().toAbsolutePath().normalize() ||
      !request.expectedParentIdentity().equals(location.dataDirectoryIdentity()) then
      reject("native marker request differs from exact Fresh custody, operation, binding, or data parent")
    val acknowledgement = markerPublisher.publish(request)
    location.validateLive()
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(acknowledgement, location, value.plan)
    published = new MySqlFreshWorldBootstrap.MarkerAcknowledgement(this, value, location, acknowledgement)
    published
  }

  /** ACK 后才将同一 PREPARED graph 转为 CERTIFIED/COMPLETE；不创建 runtime admission。 */
  def certify(value: MySqlFreshWorldBootstrap.MarkerAcknowledgement): MySqlFreshWorldBootstrap.Completed = synchronized {
    requireOwnedThread()
    requireNotUnknown()
    MySqlB1V3MutationGate.requireImplementedIssuers()
    requirePublished(value)
    value.location.validateLive()
    value.prepared.custody.validateLive()
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(value.ack, value.location, value.prepared.plan)
    if completed != null then
      verifyCurrentCompletedGraph(completed)
      return completed
    val qualification = qualify(Set(MySqlLegacyPhysicalBinding(value.prepared.plan.bindingUuid, 2)), requireUnownedWriter = false)
    if qualification.writerEpochObservedBeforeAcquire != value.prepared.writer.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(scope.bootUuid) then
      reject("fresh qualification does not observe the exact held B1 writer epoch")

    val result = transact("certify") { register =>
      val beforeRead = readLockedFormat3Graph()
      validateManifest(beforeRead.manifest)
      val binding = beforeRead.binding.getOrElse(reject("B1 certify requires its persisted LEGACY binding"))
      if binding != PhysicalRecoveryBinding(value.prepared.plan.bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 2) then
        reject("B1 certify cannot replace a conflicting physical binding")
      val writer = beforeRead.writer
      verifyWriterReceipt(writer, value.prepared.context, scope.bootUuid,
        value.prepared.writer.writerEpoch, value.prepared.writer.leaseUntil)
      val before = beforeRead.repository
      if !value.prepared.graph.exactlyEquals(beforeRead.graph) then reject("persisted PREPARED full20 graph changed before marker certification")
      MySqlBootstrapSnapshotTransitions.requirePreparedMatches(
        before, value.prepared.plan, value.prepared.sourceWorldIdentity, value.prepared.receiptSha256)
      val ackHash = MySqlFreshWorldBootstrap.parseAckHash(value.ack.markerSha256())
      val desired = MySqlBootstrapSnapshotTransitions.complete(
        before, value.prepared.plan, value.prepared.sourceWorldIdentity, value.prepared.receiptSha256, ackHash)
      val normalizedWriter = MySqlFreshFormat3WriterTarget(writer.bindingUuid, writer.writerEpoch,
        writer.ownerBootUuid, writer.leaseUntil, value.prepared.plan.certifyWriteUuid)
      val context = format3OperationContext("CERTIFY", value.prepared.plan, beforeRead.full20, before, desired, normalizedWriter)
      val targetWriter = writer.copy(lastOperationUuid = Some(context.operationUuid),
        lastFingerprint = Some(context.canonicalFingerprintSha256))
      val targetFull20 = MySqlFreshFormat3Graph.targetSnapshot(beforeRead.full20, before, desired, targetWriter)
      val targetGraph = MySqlB1CommonGraph.captureFormat3(beforeRead.manifest, desired.recovery.binding, desired,
        targetFull20, expectation.format3Inputs.getOrElse(reject("Fresh Format3 inputs are missing")))
      register(context)
      certifyBinding(value.prepared.plan)
      completeIntent(before.bootstrapIntents.head, ackHash)
      updateWriterReceipt(writer, value.prepared.context, context)
      value.location.validateLive()
      value.prepared.custody.validateLive()
      val actual = readLockedFormat3Graph()
      if !targetGraph.exactlyEquals(actual.graph) then reject("CERTIFIED full20 binding and COMPLETE intent differ from the exact target")
      val writerAfter = actual.writer
      verifyWriterReceipt(writerAfter, context, scope.bootUuid, writer.writerEpoch, writer.leaseUntil)
      MySqlFreshWorldBootstrap.PreparedSqlResult(actual.repository, actual.full20, actual.graph, context, writerAfter)
    } { committedResult =>
      verifyPostCommit(committedResult.graph, committedResult.context, scope.bootUuid,
        committedResult.writer.writerEpoch, committedResult.writer.leaseUntil)
      value.location.validateLive()
      value.prepared.custody.validateLive()
      MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(value.ack, value.location, value.prepared.plan)
    }
    completed = new MySqlFreshWorldBootstrap.Completed(this, value, result.snapshot, result.full20, result.graph,
      result.context, result.writer)
    completed
  }

  /** 重新审计后，将同一 boot/epoch/lease 的完成证明交给 normal Certified coordinator。 */
  def admitRuntime(value: MySqlFreshWorldBootstrap.Completed, coordinator: MySqlWriterFenceCoordinator): MySqlBusinessAdmission = synchronized {
    requireOwnedThread()
    requireNotUnknown()
    MySqlB1V3MutationGate.requireImplementedIssuers()
    requireCompleted(value)
    require(coordinator != null, "normal certified writer coordinator is required")
    if admittedValue != null then
      if admittedCoordinator eq coordinator then return coordinator.requireBusinessAdmission(verified, scope)
      reject("runtime admission already belongs to another writer coordinator")
    value.marker.location.validateLive()
    value.marker.prepared.custody.validateLive()
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(value.marker.ack, value.marker.location, value.marker.prepared.plan)
    verifyCurrentCompletedGraph(value)

    val observation = auditor.readCurrentObjectManifest(verified, scope)
    val qualification = auditor.auditBeforeWriterAcquire(observation, scope)
    if qualification.physicalBinding != MySqlCertifiedPhysicalBinding(value.marker.prepared.plan.bindingUuid, 2) ||
      qualification.writerEpochObservedBeforeAcquire != value.writer.writerEpoch ||
      qualification.writerOwnerBootObservedBeforeAcquire != Some(scope.bootUuid) then
      reject("fresh post-certification audit does not observe the exact CERTIFIED binding and held epoch")
    val beforePostAudit = readWriter(lock = false)
    verifyWriterReceipt(beforePostAudit, value.context, scope.bootUuid, value.writer.writerEpoch, value.writer.leaseUntil)
    val fence = MySqlWriterFenceExpectation.fromAcquiredFence(
      value.marker.prepared.plan.bindingUuid, scope.bootUuid, beforePostAudit.writerEpoch,
      beforePostAudit.leaseUntil, beforePostAudit.databaseNow, System.nanoTime())
    val token = new MySqlWriterFenceToken(verified, scope, qualification, fence)
    val postAcquireObservation = auditor.readCurrentObjectManifest(verified, scope)
    val receipt = auditor.auditAfterWriterAcquire(postAcquireObservation, qualification, scope, fence)
    val afterPostAudit = readWriter(lock = false)
    verifyWriterReceipt(afterPostAudit, value.context, scope.bootUuid, value.writer.writerEpoch, value.writer.leaseUntil)
    value.marker.location.validateLive()
    value.marker.prepared.custody.validateLive()
    verifyCurrentCompletedGraph(value)

    val proof = new MySqlFreshWorldBootstrap.CompletionProof(
      this, coordinator, verified, scope, value.marker.prepared.plan, value.marker.prepared.world, value.marker.prepared.fresh,
      value.marker.prepared.custody, value.marker.location, value.marker.ack, value.marker.prepared.snapshot,
      value.marker.prepared.graph, value.snapshot, value.graph, value.context, afterPostAudit, token, receipt)
    val admission = coordinator.admitFreshWorldBootstrap(proof)
    admittedCoordinator = coordinator
    admittedValue = admission
    admission
  }

  private[mysql] def validateCompletionProof(proof: MySqlFreshWorldBootstrap.CompletionProof): Unit = synchronized {
    requireOwnedThread()
    requireCompleted(completed)
    if proof == null || (proof.owner ne this) || (proof.plan != completed.marker.prepared.plan) ||
      (proof.ack ne completed.marker.ack) || !proof.completedSnapshot.exactlyEquals(completed.snapshot) ||
      !proof.preparedFormat3Graph.exactlyEquals(completed.marker.prepared.graph) ||
      !proof.completedFormat3Graph.exactlyEquals(completed.graph) then
      reject("completion proof no longer matches the exact B1 service result")
    proof.location.validateLive()
    proof.custody.validateLive()
    MySqlFreshWorldBootstrap.validateMarkerAcknowledgement(proof.ack, proof.location, proof.plan)
    verifyCurrentCompletedGraph(completed)
  }

  private def validateFreshWorld(
      world: WindowsWorldProvisioner.ProvisionedWorld
  ): (WindowsWorldProvisioner.FreshProvisioningProof, WindowsWorldProvisioner.BootWorldCustodyProof) =
    if world == null || !world.createdThisBoot() then reject("B1 accepts only a world created by this current-process Fresh operation")
    val fresh = world.freshProvisioningProof()
    val custody = world.bootWorldCustodyProof()
    if fresh == null || custody == null || (custody.freshProvisioningProof() ne fresh) then
      reject("Fresh provision and current boot custody are not the same opaque source")
    if custody.bootId() != scope.bootUuid || custody.generation() != scope.bootGeneration ||
      fresh.creationBoot().bootId() != scope.bootUuid || fresh.creationBoot().generation() != scope.bootGeneration then
      reject("Fresh provision and world custody belong to a different boot generation")
    custody.validateLive()
    val plan = planIdentity(fresh.plan())
    validatePlanExpectation(plan)
    val source = fresh.sourceWorldIdentity()
    if source == null || source.isEmpty then reject("opaque versioned source-world identity is required")
    val bytes = fresh.receiptCanonicalBytes()
    if bytes == null || bytes.isEmpty || !MySqlFreshWorldBootstrap.sha256Hex(MySqlFreshWorldBootstrap.sha256(bytes))
        .equalsIgnoreCase(fresh.receiptSha256()) then
      reject("opaque durable Fresh receipt bytes differ from their exact SHA-256")
    (fresh, custody)

  private def freshWorldStillLive(world: WindowsWorldProvisioner.ProvisionedWorld,
      fresh: WindowsWorldProvisioner.FreshProvisioningProof,
      expectedCustody: WindowsWorldProvisioner.BootWorldCustodyProof): Unit =
    if world == null || !world.createdThisBoot() || (world.freshProvisioningProof() ne fresh) then
      reject("Fresh source proof changed across B1 prepare")
    val custody = world.bootWorldCustodyProof()
    if custody == null || (custody ne expectedCustody) || (custody.freshProvisioningProof() ne fresh) ||
      custody.bootId() != scope.bootUuid || custody.generation() != scope.bootGeneration then
      reject("world custody changed across B1 prepare")
    custody.validateLive()

  private def qualify(
      allowedBinding: Set[MySqlObservedPhysicalBinding],
      requireUnownedWriter: Boolean
  ): VerifiedSchemaQualification =
    requireIdleAutocommit()
    val observation = auditor.readCurrentObjectManifest(verified, scope)
    val qualification = auditor.auditBeforeWriterAcquire(observation, scope)
    if !qualification.isCurrent(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration,
      expectation.serverUuid, expectation.schema, expectation.bindingUuid, expectation.profile,
      expectation.expectedPhysicalFormat, verified, scope.ownerThread, System.nanoTime()) then
      reject("format 3 schema qualification is stale for the exact owned connection")
    if qualification.profile != MySqlSchemaAuditProfile.Format3 ||
      qualification.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
      qualification.serverUuid != expectation.serverUuid || qualification.schema != expectation.schema ||
      qualification.bindingUuid != expectation.bindingUuid || qualification.provisioningUuid != expectation.provisioningUuid ||
      !qualification.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) ||
      !qualification.objectManifestSha256.equalsIgnoreCase(expectation.objects.canonicalSha256) ||
      qualification.importState != "NONE" || qualification.importOperationUuid.nonEmpty ||
      qualification.importManifestSha256.nonEmpty || qualification.importSnapshotSha256.nonEmpty ||
      qualification.importReceiptSha256.nonEmpty || !allowedBinding.contains(qualification.physicalBinding) then
      reject("approved format 3 schema qualification differs from the exact Fresh B1 target")
    if requireUnownedWriter != qualification.writerOwnerBootObservedBeforeAcquire.isEmpty then
      reject("format 3 qualification writer ownership differs from the requested B1 phase")
    qualification

  private def validatePlanExpectation(plan: MySqlBootstrapPlanIdentity): Unit =
    if !MySqlFreshWorldBootstrap.planMatchesExpectation(plan, expectation) then
      reject("opaque Fresh provision plan differs from the independently approved MySQL target")

  private def planIdentity(plan: WindowsWorldProvisioner.Plan): MySqlBootstrapPlanIdentity =
    if plan == null then reject("opaque Fresh provision plan is required")
    MySqlBootstrapPlanIdentity(plan.creationOperationUuid(), plan.bootstrapOperationUuid(),
      plan.prepareWriteUuid(), plan.certifyWriteUuid(), plan.bindingUuid(), plan.mysqlServerUuid(),
      plan.schema(), plan.schemaFormat(), plan.physicalFormat(), plan.ddlSha256(), plan.provisioningUuid())

  private def format3OperationContext(
      stage: String,
      plan: MySqlBootstrapPlanIdentity,
      beforeFull20: MySqlFormat3FullSnapshot,
      beforeRepository: MySqlFullRepositorySnapshot,
      targetRepository: MySqlFullRepositorySnapshot,
      normalizedWriter: MySqlFreshFormat3WriterTarget
  ): MySqlOperationContext =
    val operation = stage match
      case "PREPARE" => plan.prepareWriteUuid
      case "CERTIFY" => plan.certifyWriteUuid
      case _ => reject("Fresh Format3 operation stage is fixed")
    val hash = MySqlFreshFormat3Graph.effectFingerprint(stage, plan, beforeFull20,
      beforeRepository, targetRepository, normalizedWriter)
    MySqlOperationContext(scope.bootUuid, normalizedWriter.writerEpoch, plan.bindingUuid, operation,
      Vector(MySqlExpectedEffect("bootstrap/full20-graph", 0L, hash)))

  private def validateBootstrapAcquirePreimage(
      writer: MySqlBusinessWriterRow,
      qualification: VerifiedSchemaQualification
  ): Unit =
    if writer.bindingUuid != expectation.bindingUuid ||
      writer.writerEpoch != qualification.writerEpochObservedBeforeAcquire ||
      writer.ownerBootUuid.nonEmpty || writer.leaseUntil.isAfter(writer.databaseNow) ||
      writer.lastOperationUuid.nonEmpty || writer.lastFingerprint.nonEmpty then
      reject("fresh bootstrap writer preimage is owned, unexpired, version-changed, or has prior slot history")

  private def insertPreparedIntent(intent: MySqlBootstrapIntentData): Unit =
    if intent.stage != MySqlBootstrapIntentStage.Prepared || intent.markerAckSha256.nonEmpty then
      reject("B1 first write must insert PREPARED with no marker ACK")
    val sql = "INSERT INTO " + table("binding_bootstrap_intent") +
      " (singleton_fixed1,bootstrap_operation_uuid,prepare_write_uuid,certify_write_uuid,binding_uuid,binding_format_version," +
      "provisioning_uuid,source_world_identity,provision_receipt_sha256,stage,marker_ack_sha256) VALUES (1,?,?,?,?,?,?,?,?, 'PREPARED',NULL)"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(intent.bootstrapOperationUuid).toArray)
      statement.setBytes(2, MySqlStoredRows.encodeUuid(intent.prepareWriteUuid).toArray)
      statement.setBytes(3, MySqlStoredRows.encodeUuid(intent.certifyWriteUuid).toArray)
      statement.setBytes(4, MySqlStoredRows.encodeUuid(intent.bindingUuid).toArray)
      statement.setInt(5, intent.bindingFormatVersion)
      statement.setBytes(6, MySqlStoredRows.encodeUuid(intent.provisioningUuid).toArray)
      statement.setString(7, intent.sourceWorldIdentity)
      statement.setBytes(8, intent.provisionReceiptSha256.toArray)
      exactlyOne(statement.executeUpdate(), "insert B1 PREPARED intent")
    }

  private def insertLegacyBinding(plan: MySqlBootstrapPlanIdentity): Unit =
    val sql = "INSERT INTO " + table("physical_recovery_binding") +
      " (singleton_fixed1,binding_uuid,cutover_state,format_version) VALUES (1,?,'LEGACY_UNCERTIFIED',2)"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(plan.bindingUuid).toArray)
      exactlyOne(statement.executeUpdate(), "insert B1 LEGACY_UNCERTIFIED binding")
    }

  private def acquireWriterAndWriteReceipt(
      before: MySqlBusinessWriterRow,
      epoch: Long,
      leaseUntil: LocalDateTime,
      context: MySqlOperationContext
  ): Unit =
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET writer_epoch=?,owner_boot_uuid=?,lease_until=?,last_commit_operation_uuid=?,last_commit_fingerprint=?" +
      " WHERE singleton_fixed1=1 AND binding_uuid=? AND writer_epoch=? AND owner_boot_uuid IS NULL" +
      " AND lease_until=? AND lease_until<=? AND last_commit_operation_uuid IS NULL AND last_commit_fingerprint IS NULL"
    withStatement(sql) { statement =>
      statement.setLong(1, epoch)
      statement.setBytes(2, MySqlStoredRows.encodeUuid(scope.bootUuid).toArray)
      statement.setObject(3, leaseUntil)
      statement.setBytes(4, MySqlStoredRows.encodeUuid(context.operationUuid).toArray)
      statement.setBytes(5, context.canonicalFingerprintSha256.toArray)
      statement.setBytes(6, MySqlStoredRows.encodeUuid(before.bindingUuid).toArray)
      statement.setLong(7, before.writerEpoch)
      statement.setObject(8, before.leaseUntil)
      statement.setObject(9, before.databaseNow)
      exactlyOne(statement.executeUpdate(), "claim bootstrap writer and install PREPARED receipt")
    }

  private def certifyBinding(plan: MySqlBootstrapPlanIdentity): Unit =
    val sql = "UPDATE " + table("physical_recovery_binding") +
      " SET cutover_state='CERTIFIED' WHERE singleton_fixed1=1 AND binding_uuid=?" +
      " AND cutover_state='LEGACY_UNCERTIFIED' AND format_version=2"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(plan.bindingUuid).toArray)
      exactlyOne(statement.executeUpdate(), "certify B1 physical binding")
    }

  private def completeIntent(before: MySqlBootstrapIntentData, ackHash: Vector[Byte]): Unit =
    if before.stage != MySqlBootstrapIntentStage.Prepared || before.markerAckSha256.nonEmpty then
      reject("B1 completion requires the immutable PREPARED intent")
    val sql = "UPDATE " + table("binding_bootstrap_intent") +
      " SET stage='COMPLETE',marker_ack_sha256=? WHERE singleton_fixed1=1" +
      " AND bootstrap_operation_uuid=? AND prepare_write_uuid=? AND certify_write_uuid=? AND binding_uuid=?" +
      " AND binding_format_version=2 AND provisioning_uuid=?" +
      " AND CAST(source_world_identity AS BINARY)=CAST(? AS BINARY) AND provision_receipt_sha256=?" +
      " AND stage='PREPARED' AND marker_ack_sha256 IS NULL"
    withStatement(sql) { statement =>
      statement.setBytes(1, ackHash.toArray)
      statement.setBytes(2, MySqlStoredRows.encodeUuid(before.bootstrapOperationUuid).toArray)
      statement.setBytes(3, MySqlStoredRows.encodeUuid(before.prepareWriteUuid).toArray)
      statement.setBytes(4, MySqlStoredRows.encodeUuid(before.certifyWriteUuid).toArray)
      statement.setBytes(5, MySqlStoredRows.encodeUuid(before.bindingUuid).toArray)
      statement.setBytes(6, MySqlStoredRows.encodeUuid(before.provisioningUuid).toArray)
      statement.setString(7, before.sourceWorldIdentity)
      statement.setBytes(8, before.provisionReceiptSha256.toArray)
      exactlyOne(statement.executeUpdate(), "complete B1 intent with durable marker ACK")
    }

  private def updateWriterReceipt(
      before: MySqlBusinessWriterRow,
      previousContext: MySqlOperationContext,
      nextContext: MySqlOperationContext
  ): Unit =
    if previousContext.operationUuid == nextContext.operationUuid then reject("B1 write phases require distinct fixed operation UUIDs")
    val sql = "UPDATE " + table("fp_runtime_writer") +
      " SET last_commit_operation_uuid=?,last_commit_fingerprint=? WHERE singleton_fixed1=1 AND binding_uuid=?" +
      " AND writer_epoch=? AND owner_boot_uuid=? AND lease_until=? AND lease_until>?" +
      " AND last_commit_operation_uuid=? AND last_commit_fingerprint=?"
    withStatement(sql) { statement =>
      statement.setBytes(1, MySqlStoredRows.encodeUuid(nextContext.operationUuid).toArray)
      statement.setBytes(2, nextContext.canonicalFingerprintSha256.toArray)
      statement.setBytes(3, MySqlStoredRows.encodeUuid(before.bindingUuid).toArray)
      statement.setLong(4, before.writerEpoch)
      statement.setBytes(5, MySqlStoredRows.encodeUuid(scope.bootUuid).toArray)
      statement.setObject(6, before.leaseUntil)
      statement.setObject(7, before.databaseNow)
      statement.setBytes(8, MySqlStoredRows.encodeUuid(previousContext.operationUuid).toArray)
      statement.setBytes(9, previousContext.canonicalFingerprintSha256.toArray)
      exactlyOne(statement.executeUpdate(), "advance exact B1 writer receipt slot")
    }

  private def verifyWriterReceipt(
      writer: MySqlBusinessWriterRow,
      context: MySqlOperationContext,
      boot: UUID,
      epoch: Long,
      leaseUntil: LocalDateTime
  ): Unit =
    if writer.bindingUuid != context.bindingUuid || writer.writerEpoch != epoch ||
      writer.ownerBootUuid != Some(boot) || writer.leaseUntil != leaseUntil then
      reject("B1 writer binding, boot, epoch, or exact lease differs from its operation")
    if !writer.leaseUntil.isAfter(writer.databaseNow) then reject("B1 writer lease expired according to database UTC time")
    MySqlCommitReceipts.verifyReadback(context, writer.receipt) match
      case MySqlCommitReceiptVerification.Confirmed(_) => ()
      case MySqlCommitReceiptVerification.Conflict(reason) => reject("B1 writer receipt conflicts with its operation: " + reason.toString)
      case MySqlCommitReceiptVerification.Unproven => reject("B1 writer receipt does not prove the exact operation effect")

  private def verifyPostCommit(
      expected: MySqlB1CommonGraph,
      context: MySqlOperationContext,
      boot: UUID,
      epoch: Long,
      leaseUntil: LocalDateTime
  ): Unit =
    if expected == null || expected.format3Snapshot.isEmpty then reject("B1 postcommit requires its immutable full20 target")
    // The outer transaction gate remains owned; rollback/restore this read-only full20 check before returning.
    readonlyBody("b1-full20-postcommit-readback") {
      val actual = readLockedFormat3Graph()
      if !actual.graph.exactlyEquals(expected) then reject("B1 committed Format3 full20 graph differs from its immutable target")
      MySqlFreshPrepareRules.requireWriter(
        new WriterTuple(context.bindingUuid, epoch, boot, leaseUntil, context.operationUuid, context.canonicalFingerprintSha256.toArray),
        actual.writer
      )
      MySqlFreshPrepareRules.requireUnexpired(leaseUntil, actual.writer.databaseNow)
      verifyWriterReceipt(actual.writer, context, boot, epoch, leaseUntil)
    }

  private def verifyCurrentCompletedGraph(value: MySqlFreshWorldBootstrap.Completed): Unit =
    requireIdleAutocommit()
    if value.graph == null || value.graph.format3Snapshot.isEmpty then reject("current B1 completion requires its exact Format3 full20 graph")
    readonlyWithGate("b1-completed-full20-current-readback") {
      val actual = readLockedFormat3Graph()
      if !actual.graph.exactlyEquals(value.graph) then reject("current B1 Format3 full20 graph differs from its exact committed snapshot")
      MySqlFreshPrepareRules.requireWriter(MySqlFreshPrepareRules.writer(value.writer), actual.writer)
      MySqlFreshPrepareRules.requireUnexpired(value.writer.leaseUntil, actual.writer.databaseNow)
      verifyWriterReceipt(actual.writer, value.context, scope.bootUuid, value.writer.writerEpoch, value.writer.leaseUntil)
    }

  private def validateManifest(row: MySqlLockedManifest): Unit =
    val expectedGuard = HexFormat.of().parseHex(expectation.objects.canonicalSha256).toVector
    val expectedDdl = HexFormat.of().parseHex(expectation.ddlSha256).toVector
    if row.singleton != 1 || row.backend != "MYSQL" || row.formatVersion != 3 || row.bootstrapState != "READY" ||
      row.ddlSha256 != expectedDdl || row.provisioningUuid != expectation.provisioningUuid ||
      row.serverUuid != expectation.serverUuid || row.bindingUuid != expectation.bindingUuid ||
      row.guardManifestSha256 != Some(expectedGuard) || !MySqlFencedTransactionSupport.importMatches(row, MySqlExpectedNoImport) then
      reject("locked format 3 manifest is not the exact approved READY fresh-empty scope")

  private def lockManifest(): MySqlLockedManifest =
    val sql = "SELECT singleton_fixed1,backend,format_version,ddl_sha256,provisioning_uuid,bootstrap_state," +
      "expected_server_uuid,expected_binding_uuid,import_manifest_sha256,import_state,import_operation_uuid," +
      "import_snapshot_sha256,import_receipt_sha256,guard_manifest_sha256 FROM " + table("fp_schema_manifest") +
      " WHERE singleton_fixed1=1 FOR SHARE"
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "B1 schema manifest") { row =>
          MySqlLockedManifest(
            MySqlFencedTransactionSupport.requiredInt(row, 1, "manifest singleton"),
            MySqlFencedTransactionSupport.requiredText(row, 2, "manifest backend"),
            MySqlFencedTransactionSupport.requiredInt(row, 3, "manifest format"),
            MySqlFencedTransactionSupport.requiredBytes(row, 4, "manifest DDL SHA-256", 32).toVector,
            MySqlFencedTransactionSupport.requiredUuid(row, 5, "manifest provisioning UUID"),
            MySqlFencedTransactionSupport.requiredText(row, 6, "manifest bootstrap state"),
            MySqlFencedTransactionSupport.requiredUuid(row, 7, "manifest server UUID"),
            MySqlFencedTransactionSupport.requiredUuid(row, 8, "manifest binding UUID"),
            MySqlFencedTransactionSupport.optionalHash(row, 9, "manifest import manifest SHA-256"),
            MySqlFencedTransactionSupport.requiredText(row, 10, "manifest import state"),
            MySqlFencedTransactionSupport.optionalUuid(row, 11, "manifest import operation UUID"),
            MySqlFencedTransactionSupport.optionalHash(row, 12, "manifest import snapshot SHA-256"),
            MySqlFencedTransactionSupport.optionalHash(row, 13, "manifest import receipt SHA-256"),
            MySqlFencedTransactionSupport.optionalHash(row, 14, "manifest guard SHA-256")
          )
        }
      }
    }

  private def lockBinding(): Option[PhysicalRecoveryBinding] =
    val sql = "SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM " +
      table("physical_recovery_binding") + " WHERE singleton_fixed1=1 FOR UPDATE"
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        if !result.next() then None
        else
          val binding = MySqlRecoveryRows.readBinding(result)
          if result.next() then reject("physical recovery binding singleton returned more than one row")
          Some(binding)
      }
    }

  private def readWriter(lock: Boolean): MySqlBusinessWriterRow =
    val suffix = if lock then " FOR UPDATE" else ""
    val sql = "SELECT singleton_fixed1,binding_uuid,writer_epoch,owner_boot_uuid,lease_until,last_commit_operation_uuid," +
      "last_commit_fingerprint,UTC_TIMESTAMP(6) FROM " + table("fp_runtime_writer") +
      " WHERE singleton_fixed1=1" + suffix
    withStatement(sql) { statement =>
      withResultSet(statement.executeQuery()) { result =>
        MySqlFencedTransactionSupport.one(result, "B1 runtime writer") { row =>
          if MySqlFencedTransactionSupport.requiredInt(row, 1, "writer singleton") != 1 then
            reject("B1 runtime writer singleton key is not one")
          MySqlBusinessWriterRow(
            MySqlFencedTransactionSupport.requiredUuid(row, 2, "writer binding UUID"),
            MySqlFencedTransactionSupport.requiredLong(row, 3, "writer epoch"),
            MySqlFencedTransactionSupport.optionalUuid(row, 4, "writer owner boot UUID"),
            MySqlFencedTransactionSupport.requiredLocalDateTime(row, 5, "writer lease"),
            MySqlFencedTransactionSupport.optionalUuid(row, 6, "writer operation UUID"),
            MySqlFencedTransactionSupport.optionalHash(row, 7, "writer operation fingerprint"),
            MySqlFencedTransactionSupport.requiredLocalDateTime(row, 8, "database UTC time")
          )
        }
      }
    }

  private def table(name: String): String =
    if name == null || !name.matches("[a-z][a-z0-9_]{0,63}") then reject("internal B1 SQL table identifier is unsafe")
    quotedSchema + "." + MySqlFencedTransactionSupport.quote(name)

  private def withStatement[A](sql: String)(body: PreparedStatement => A): A =
    MySqlBusinessRows.using(connection.prepareStatement(sql))(body)

  private def withResultSet[A](result: ResultSet)(body: ResultSet => A): A =
    MySqlBusinessRows.using(result)(body)

  private def transact[A](stage: String)(body: (MySqlOperationContext => Unit) => A)(postCommit: A => Unit): A =
    requireOwnedThread()
    MySqlB1V3MutationGate.requireImplementedIssuers()
    transactAdmitted(stage, () => ())(body)(postCommit)

  private def transactAdmitted[A](stage: String, onCommitAcknowledged: () => Unit)(body: (MySqlOperationContext => Unit) => A)(postCommit: A => Unit): A =
    requireOwnedThread()
    requireIdleAutocommit()
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly) match
      case Some(nestedFailure) => throw nestedFailure
      case None => ()
    if activeRollbackOnly != null then
      val failure = new IllegalStateException("Fresh bootstrap already retains an active transaction owner")
      rollbackOnly.mark(failure)
      MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
      throw failure
    activeRollbackOnly = rollbackOnly
    var beginAttempted = false
    var commitAttempted = false
    var commitAcknowledged = false
    var registeredContext: MySqlOperationContext = null
    var primaryForGate: Throwable = null
    val oldIsolation = connection.getTransactionIsolation
    try
      beginAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      if connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        reject("B1 write transaction could not establish REPEATABLE READ")
      connection.setAutoCommit(false)
      verified.readmitSessionBarrier(expectation, scope)
      if connection.getAutoCommit then reject("B1 JDBC transaction did not disable autocommit")
      val result = body { context =>
        if context == null || registeredContext != null then reject("B1 transaction must register exactly one immutable context")
        registeredContext = context
      }
      if registeredContext == null then reject("B1 transaction did not freeze its operation context before writing")
      if rollbackOnly.cause.nonEmpty then throw rollbackOnly.cause.get
      commitAttempted = true
      connection.commit()
      commitAcknowledged = true
      onCommitAcknowledged()
      connection.setAutoCommit(true)
      if !connection.getAutoCommit then reject("B1 committed transaction did not restore autocommit")
      connection.setTransactionIsolation(oldIsolation)
      if connection.getTransactionIsolation != oldIsolation then reject("B1 committed transaction did not restore the prior isolation")
      postCommit(result)
      result
    catch
      case primary: Throwable =>
        val outgoing =
          if commitAttempted && !commitAcknowledged then
            noteUnknown(stage, primary)
            if registeredContext == null then new MySqlBootstrapCleanupUncertainException(stage, None, primary)
            else MySqlCommitUnknownException(registeredContext, primary)
          else if commitAcknowledged then
            noteUnknown(stage, primary)
            new MySqlBootstrapCommittedUnverifiedException(stage, Option(registeredContext), primary)
          else if beginAttempted then rollbackAndRestore(stage, Option(registeredContext), primary, oldIsolation)
          else primary
        primaryForGate = outgoing
        throw outgoing
    finally
      try MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
      catch
        case cleanup: Throwable =>
          noteUnknown(stage, cleanup)
          if primaryForGate != null then MySqlFencedTransactionSupport.addSuppressed(primaryForGate, cleanup)
          else throw new MySqlBootstrapCleanupUncertainException(stage, Option(registeredContext), cleanup)
      finally activeRollbackOnly = null

  private def rollbackAndRestore(stage: String, context: Option[MySqlOperationContext], primary: Throwable,
      oldIsolation: Int): Throwable =
    var cleanupFailed = false
    try connection.rollback()
    catch
      case rollbackFailure: Throwable =>
        MySqlFencedTransactionSupport.addSuppressed(primary, rollbackFailure)
        cleanupFailed = true
    if !cleanupFailed then
      try
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then reject("B1 rollback did not restore autocommit")
        connection.setTransactionIsolation(oldIsolation)
        if connection.getTransactionIsolation != oldIsolation then reject("B1 rollback did not restore the prior isolation")
      catch
        case restoreFailure: Throwable =>
          MySqlFencedTransactionSupport.addSuppressed(primary, restoreFailure)
          cleanupFailed = true
    if cleanupFailed then
      noteUnknown(stage, primary)
      new MySqlBootstrapCleanupUncertainException(stage, context, primary)
    else primary

  private def noteUnknown(stage: String, primary: Throwable): Unit =
    if unknownFailure == null then
      unknownFailure = primary
      unknownStage = stage
    else if unknownFailure ne primary then MySqlFencedTransactionSupport.addSuppressed(unknownFailure, primary)

  private def requirePrepared(value: MySqlFreshWorldBootstrap.Prepared): Unit =
    if value == null || (value.owner ne this) || (prepared ne value) then reject("B1 PREPARED receipt is stale or belongs to another service")

  private def requirePublished(value: MySqlFreshWorldBootstrap.MarkerAcknowledgement): Unit =
    if value == null || (value.owner ne this) || (published ne value) then reject("B1 marker ACK is stale or belongs to another service")
    requirePrepared(value.prepared)

  private def requireCompleted(value: MySqlFreshWorldBootstrap.Completed): Unit =
    if value == null || (value.owner ne this) || (completed ne value) then reject("B1 COMPLETE receipt is stale or belongs to another service")
    requirePublished(value.marker)

  private def requireOwnedThread(): Unit =
    if Thread.currentThread() ne ownerThread then reject("B1 MySQL work must run on its captured owned IO thread")

  private def requireIdleAutocommit(): Unit =
    if !connection.getAutoCommit then reject("B1 operation requires an idle autocommit connection")

  private def requireNotUnknown(): Unit =
    if unknownFailure != null then throw new IllegalStateException(
      "B1 bootstrap remains frozen after an uncertain " + unknownStage + " outcome", unknownFailure)

  private def exactlyOne(count: Int, operation: String): Unit =
    if count != 1 then throw new SQLTransactionRollbackException(
      "MySQL " + operation + " affected " + count + " rows; exact singleton CAS required")

  private def reject(message: String): Nothing =
    throw new SQLTransactionRollbackException("MySQL B1 bootstrap rejected: " + message)

object MySqlFreshWorldBootstrap:
  /** 独立 schema4 检查；没有 native、writer acquire 或 runtime admission。 */
  private[mysql] def inspectScriptSchema(verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation, inputs: MySqlScriptSchemaInputs,
      auditor: MySqlSchemaAuditor, root: MySqlFormat3ReadBudget)(consume: MySqlScriptSchemaSnapshot => Unit): Unit =
    require(expectation.profile == MySqlSchemaAuditProfile.ScriptPublication1 &&
      expectation.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 &&
      expectation.expectedImport == MySqlExpectedNoImport && (scope.ownerThread eq Thread.currentThread()),
      "independent schema4/physical2/no-import owned inspection required")
    val connection = verified.connection
    require(scope.verifiedConnection.contains(verified) && (scope.connectionToken eq connection) &&
      (scope.connectionIdentity eq verified.identity), "exact verified inspection scope required")
    require(!connection.isClosed && connection.getAutoCommit, "inspection requires idle owned connection")
    root.requireScriptPublication1Inputs(inputs)
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly).foreach(throw _)
    var primary: Throwable = null
    var snapshot: MySqlScriptSchemaSnapshot = null
    var oldIsolation: Option[Int] = None
    var transactionAttempted = false
    try
      oldIsolation = Some(connection.getTransactionIsolation)
      transactionAttempted = true
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ)
      connection.setAutoCommit(false)
      if connection.getAutoCommit || connection.getTransactionIsolation != Connection.TRANSACTION_REPEATABLE_READ then
        throw new SQLTransactionRollbackException("schema4 inspection could not establish REPEATABLE READ")
      verified.readmitSessionBarrier(expectation, scope)
      val packetSql = "SELECT @@SESSION.max_allowed_packet"
      if MySqlFormat3FullSnapshotSql.utf8Length(packetSql) + 16L > inputs.budget.packetBytes then
        throw new SQLTransactionRollbackException("schema4 session probe exceeds explicit packet budget")
      val actualPacket = MySqlBusinessRows.using(connection.prepareStatement(packetSql)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { rs =>
          if !rs.next() then throw new SQLTransactionRollbackException("schema4 actual session packet observation missing")
          val value = rs.getLong(1)
          if rs.wasNull() || value <= 0L || rs.next() then
            throw new SQLTransactionRollbackException("schema4 actual session packet observation differs")
          value
        }
      }
      // 保持既有 M→B→W 根锁顺序；随后完整24读取仍按固定顺序重核全部值。
      Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer").foreach { table =>
        val sql = s"SELECT `singleton_fixed1` FROM `${inputs.schema}`.`$table` ORDER BY `singleton_fixed1` FOR UPDATE"
        if MySqlFormat3FullSnapshotSql.utf8Length(sql) + 16L > inputs.effectivePacketLimit(actualPacket) then
          throw new SQLTransactionRollbackException("schema4 root lock SQL exceeds explicit packet budget")
        MySqlBusinessRows.using(connection.prepareStatement(sql)) { statement =>
          MySqlBusinessRows.using(statement.executeQuery()) { rs =>
            val present = rs.next()
            if !present && table != "physical_recovery_binding" then
              throw new SQLTransactionRollbackException(s"schema4 required root $table is missing")
            if present && (rs.getLong(1) != 1L || rs.wasNull() || rs.next()) then
              throw new SQLTransactionRollbackException(s"schema4 root $table singleton differs")
          }
        }
      }
      auditor.auditBeforeWriterAcquire(auditor.readCurrentObjectManifest(verified, scope), scope)
      val owner = new ScriptSchemaReadOwner(verified, scope, expectation, inputs, root,
        MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly), rollbackOnly)
      snapshot = new MySqlScriptSchemaSnapshotJdbc(verified, scope, owner).read()
      if snapshot.maxAllowedPacketBytes != actualPacket then
        throw new SQLTransactionRollbackException("schema4 session packet observation changed across root locks")
      consume(snapshot)
      owner.requireCurrent(verified, scope)
    catch case failure: Throwable => primary = failure
    val cleanup = Vector.newBuilder[() => Unit]
    if transactionAttempted then
      cleanup += (() => connection.rollback())
      cleanup += (() => {
        connection.setAutoCommit(true)
        if !connection.getAutoCommit then throw new IllegalStateException("schema4 inspection did not restore autocommit")
      })
      oldIsolation.foreach { isolation => cleanup += (() => {
        connection.setTransactionIsolation(isolation)
        if connection.getTransactionIsolation != isolation then throw new IllegalStateException("schema4 inspection did not restore isolation")
      }) }
    if snapshot != null then cleanup += (() => snapshot.allocationBudget.closeScopedReadBudget())
    cleanup += (() => MySqlConnectionTransactionGate.leave(connection, rollbackOnly))
    MySqlFormat3BudgetCleanup.run(primary, cleanup.result())
    if primary != null then throw primary

  private[mysql] final class ScriptSchemaReadOwner private[MySqlFreshWorldBootstrap] (
      verified: VerifiedMySqlConnection, scope: MySqlAuditBootScope,
      expectation: MySqlSchemaAuditExpectation, val inputs: MySqlScriptSchemaInputs,
      val root: MySqlFormat3ReadBudget, gate: MySqlConnectionTransactionGate.Owner,
      rollbackOnly: MySqlRollbackOnlyState
  ):
    def requireCurrent(actual: VerifiedMySqlConnection, actualScope: MySqlAuditBootScope): Unit =
      require((actual eq verified) && (actualScope eq scope) && (Thread.currentThread() eq scope.ownerThread) &&
        expectation.profile == MySqlSchemaAuditProfile.ScriptPublication1 && !root.isClosed,
        "exact live schema4 inspection owner required")
      root.requireScriptPublication1Inputs(inputs)
      MySqlConnectionTransactionGate.requireCurrent(gate, verified.connection, rollbackOnly)
      verified.readmitSessionBarrier(expectation, scope)

    def requireSnapshot(snapshot: MySqlScriptSchemaSnapshot): Unit =
      requireCurrent(verified, scope)
      require(snapshot != null && (snapshot.inputs eq inputs) &&
        snapshot.connection == MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ") &&
        snapshot.observationScope == MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration) &&
        snapshot.allocationBudget.isSnapshotReadChildOf(root) && snapshot.allocationBudget.sharesAllocationLedger(root),
        "schema4 snapshot escaped the original session/transaction/allocation root")
      snapshot.allocationBudget.reserveTemporaryMemory(8_192L, "schema4 independent manifest comparison")
      var primary: Throwable = null
      try MySqlScriptSchemaSnapshotJdbcValidation.requireManifest(snapshot, expectation)
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally MySqlFormat3BudgetCleanup.run(primary, Vector(() =>
        snapshot.allocationBudget.releaseTemporaryMemory(8_192L, "schema4 independent manifest comparison")))

    def markFailure(cause: Throwable): Unit = rollbackOnly.mark(cause)

  /** 完整 Fresh 锁图，只供同一 actor 内比较；不向调用方发行 connection/gate capability。 */
  private[mysql] final case class Format3ReadFacts(
      manifest: MySqlLockedManifest,
      binding: Option[PhysicalRecoveryBinding],
      writer: MySqlBusinessWriterRow,
      repository: MySqlFullRepositorySnapshot,
      full20: MySqlFormat3FullSnapshot,
      graph: MySqlB1CommonGraph
  )

  /** 私有发行且绑定实际 M/B/W 锁、同一 transaction gate、scope/thread 与 rollback-only 状态。 */
  private[mysql] final class Format3ReadOwner private[MySqlFreshWorldBootstrap] (
      private val issuer: MySqlFreshWorldBootstrap,
      private val gateOwner: MySqlConnectionTransactionGate.Owner,
      private val verified: VerifiedMySqlConnection,
      private val scope: MySqlAuditBootScope,
      private val expectation: MySqlSchemaAuditExpectation,
      private val rollbackOnly: MySqlRollbackOnlyState,
      private val ownerThread: Thread,
      private val manifest: MySqlLockedManifest,
      private val binding: Option[PhysicalRecoveryBinding],
      private val writer: MySqlBusinessWriterRow,
      val originalRetainedRoot: MySqlFormat3ReadBudget
  ):
    def requireCurrent(readerVerified: VerifiedMySqlConnection, readerScope: MySqlAuditBootScope,
        readerSchema: String, readerProfile: com.wi.floatingpod.config.SourceLimitProfile,
        readerConnection: java.sql.Connection): Unit =
      if issuer == null || (readerVerified ne verified) || (readerScope ne scope) ||
        (readerConnection ne verified.connection) || (ownerThread ne Thread.currentThread()) ||
        (scope.ownerThread ne ownerThread) || (readerSchema != expectation.schema) ||
        (readerProfile ne expectation.format3Inputs.map(_.limitProfile).orNull) then
        throw new IllegalStateException("Fresh full20 read owner differs from the exact verified bootstrap scope")
      issuer.requireOriginalWitnessRoot(originalRetainedRoot)
      MySqlConnectionTransactionGate.requireCurrent(gateOwner, readerConnection, rollbackOnly)
      rollbackOnly.cause.foreach(throw _)
      verified.readmitSessionBarrier(expectation, scope)
      if expectation.profile != MySqlSchemaAuditProfile.Format3 ||
        expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
        expectation.expectedImport != MySqlExpectedNoImport then
        throw new IllegalStateException("Fresh full20 reader requires approved schema3/physical2/no-import context")

    def requireSnapshot(snapshot: MySqlFormat3FullSnapshot): Unit =
      if snapshot == null || snapshot.schema != expectation.schema ||
        snapshot.connection != MySqlFormat3ConnectionObservation.from(verified.identity, "REPEATABLE-READ") ||
        snapshot.observationScope != MySqlFormat3ObservationScope(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration) ||
        snapshot.limitProfile != expectation.format3Inputs.map(_.limitProfile).orNull then
        throw new SQLTransactionRollbackException("Fresh Format3 full20 observation identity differs from its locked owner")
      issuer.requireOriginalWitnessRoot(originalRetainedRoot)
      if !snapshot.allocationBudget.isSnapshotReadChildOf(originalRetainedRoot) ||
        !snapshot.allocationBudget.sharesAllocationLedger(originalRetainedRoot) then
        throw new SQLTransactionRollbackException("Fresh full20 snapshot escaped original actor witness ledger")
      MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest,
        expectation.format3Inputs.getOrElse(throw new IllegalStateException("Fresh Format3 independent inputs are missing")))
      MySqlFreshFormat3Graph.requireBindingMatches(snapshot, binding)
      MySqlFreshFormat3Graph.requireWriterMatches(snapshot, writer)

    def markFailure(failure: Throwable): Unit = rollbackOnly.mark(failure)
    override def toString: String = "MySqlFreshWorldBootstrap.Format3ReadOwner[private active transaction]"

  private[MySqlFreshWorldBootstrap] final case class PreparedSqlResult(
      snapshot: MySqlFullRepositorySnapshot,
      full20: MySqlFormat3FullSnapshot,
      graph: MySqlB1CommonGraph,
      context: MySqlOperationContext,
      writer: MySqlBusinessWriterRow):
    require(snapshot != null && full20 != null && graph != null && context != null && writer != null,
      "Fresh SQL result must retain its exact typed and full20 commit facts")
    require(graph.format3Snapshot.exists(_.exactlyEquals(full20)) && graph.repository.exactlyEquals(snapshot),
      "Fresh SQL result projections must match the exact full20 graph")

  final class Prepared private[MySqlFreshWorldBootstrap] (
      private[mysql] val owner: MySqlFreshWorldBootstrap,
      private[mysql] val world: WindowsWorldProvisioner.ProvisionedWorld,
      private[mysql] val fresh: WindowsWorldProvisioner.FreshProvisioningProof,
      private[mysql] val custody: WindowsWorldProvisioner.BootWorldCustodyProof,
      private[mysql] val plan: MySqlBootstrapPlanIdentity,
      private[mysql] val sourceWorldIdentity: String,
      private[mysql] val receiptSha256: Vector[Byte],
      private[mysql] val snapshot: MySqlFullRepositorySnapshot,
      private[mysql] val full20: MySqlFormat3FullSnapshot,
      private[mysql] val graph: MySqlB1CommonGraph,
      private[mysql] val context: MySqlOperationContext,
      private[mysql] val writer: MySqlBusinessWriterRow
  ):
    override def toString: String = "MySqlFreshWorldBootstrap.Prepared[redacted]"

  final class MarkerAcknowledgement private[MySqlFreshWorldBootstrap] (
      private[mysql] val owner: MySqlFreshWorldBootstrap,
      private[mysql] val prepared: Prepared,
      private[mysql] val location: WindowsWorldProvisioner.CanonicalMarkerLocation,
      private[mysql] val ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck
  ):
    override def toString: String = "MySqlFreshWorldBootstrap.MarkerAcknowledgement[redacted]"

  final class Completed private[MySqlFreshWorldBootstrap] (
      private[mysql] val owner: MySqlFreshWorldBootstrap,
      private[mysql] val marker: MarkerAcknowledgement,
      private[mysql] val snapshot: MySqlFullRepositorySnapshot,
      private[mysql] val full20: MySqlFormat3FullSnapshot,
      private[mysql] val graph: MySqlB1CommonGraph,
      private[mysql] val context: MySqlOperationContext,
      private[mysql] val writer: MySqlBusinessWriterRow
  ):
    override def toString: String = "MySqlFreshWorldBootstrap.Completed[redacted]"

  /** 唯有 B1 owner 完整读回后才能产生的 normal-runtime transfer proof。 */
  private[mysql] final class CompletionProof private[MySqlFreshWorldBootstrap] (
      private[mysql] val owner: MySqlFreshWorldBootstrap,
      private[mysql] val coordinator: MySqlWriterFenceCoordinator,
      private[mysql] val verified: VerifiedMySqlConnection,
      private[mysql] val scope: MySqlAuditBootScope,
      private[mysql] val plan: MySqlBootstrapPlanIdentity,
      private[mysql] val world: WindowsWorldProvisioner.ProvisionedWorld,
      private[mysql] val fresh: WindowsWorldProvisioner.FreshProvisioningProof,
      private[mysql] val custody: WindowsWorldProvisioner.BootWorldCustodyProof,
      private[mysql] val location: WindowsWorldProvisioner.CanonicalMarkerLocation,
      private[mysql] val ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck,
      private[mysql] val preparedSnapshot: MySqlFullRepositorySnapshot,
      private[mysql] val preparedFormat3Graph: MySqlB1CommonGraph,
      private[mysql] val completedSnapshot: MySqlFullRepositorySnapshot,
      private[mysql] val completedFormat3Graph: MySqlB1CommonGraph,
      private[mysql] val context: MySqlOperationContext,
      private[mysql] val writer: MySqlBusinessWriterRow,
      private[mysql] val token: MySqlWriterFenceToken,
      private[mysql] val receipt: RuntimeSchemaAuditReceipt
  ):
    private[mysql] def validateFor(expected: MySqlSchemaAuditExpectation, target: MySqlWriterFenceCoordinator): Unit =
      if expected == null || target == null || (coordinator ne target) ||
        !planMatchesExpectation(plan, expected) || context.operationUuid != plan.certifyWriteUuid ||
        context.bootUuid != scope.bootUuid || context.writerEpoch != token.writerEpoch ||
        context.bindingUuid != plan.bindingUuid ||
        completedSnapshot.recovery.binding != Some(PhysicalRecoveryBinding(plan.bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)) ||
        completedSnapshot.bootstrapIntents.size != 1 || preparedFormat3Graph == null || completedFormat3Graph == null ||
        preparedFormat3Graph.format3Snapshot.isEmpty || completedFormat3Graph.format3Snapshot.isEmpty ||
        !preparedFormat3Graph.repository.exactlyEquals(preparedSnapshot) ||
        !completedFormat3Graph.repository.exactlyEquals(completedSnapshot) then
        rejectProof("B1 completion plan, final full20 graph, and operation context differ")
      owner.validateCompletionProof(this)
      if !world.createdThisBoot() || (world.freshProvisioningProof() ne fresh) ||
        (world.bootWorldCustodyProof() ne custody) || (custody.freshProvisioningProof() ne fresh) then
        rejectProof("proof does not retain the exact current-process Fresh source and custody objects")
      val expectedComplete = MySqlBootstrapSnapshotTransitions.complete(
        preparedSnapshot, plan, fresh.sourceWorldIdentity(), receiptHash(fresh), parseAckHash(ack.markerSha256()))
      if !completedSnapshot.exactlyEquals(expectedComplete) then rejectProof("B1 complete graph differs from PREPARED source and marker ACK")
      val intent = MySqlBootstrapSnapshotTransitions.requireCompleteMatches(
        completedSnapshot, plan, fresh.sourceWorldIdentity(), receiptHash(fresh), parseAckHash(ack.markerSha256()))
      if intent.stage != MySqlBootstrapIntentStage.Complete || intent.markerAckSha256 != Some(parseAckHash(ack.markerSha256())) then
        rejectProof("B1 durable intent does not carry the exact marker ACK")
      validateMarkerAcknowledgement(ack, location, plan)
      if writer.bindingUuid != plan.bindingUuid || writer.ownerBootUuid != Some(scope.bootUuid) ||
        writer.writerEpoch != token.writerEpoch || writer.leaseUntil != token.leaseUntil ||
        writer.lastOperationUuid != Some(plan.certifyWriteUuid) ||
        writer.lastFingerprint != Some(context.canonicalFingerprintSha256) ||
        !writer.leaseUntil.isAfter(writer.databaseNow) then
        rejectProof("B1 writer readback does not prove the exact live certify receipt and epoch")
      if (token.verified ne verified) || (token.scope ne scope) ||
        token.qualification.physicalBinding != MySqlCertifiedPhysicalBinding(plan.bindingUuid, 2) ||
        token.fence.ownerBootUuid != scope.bootUuid || token.fence.bindingUuid != plan.bindingUuid ||
        token.fence.epoch != writer.writerEpoch || token.fence.leaseUntil != writer.leaseUntil then
        rejectProof("runtime token is not the exact certified B1 writer tuple")
      receipt.assertCurrent(scope.bootUuid, scope.bootGeneration, scope.connectionGeneration,
        expected.serverUuid, expected.schema, plan.bindingUuid, expected.profile, expected.expectedPhysicalFormat,
        verified, Some(token.fence), System.nanoTime())
      if receipt.writerEpoch != writer.writerEpoch || receipt.bindingUuid != plan.bindingUuid ||
        receipt.serverUuid != expected.serverUuid || receipt.schema != expected.schema ||
        !receipt.ddlSha256.equalsIgnoreCase(expected.ddlSha256) ||
        !receipt.objectManifestSha256.equalsIgnoreCase(expected.objects.canonicalSha256) then
        rejectProof("normal post-acquire audit receipt differs from the exact B1 target")

  private[MySqlFreshWorldBootstrap] def planMatchesExpectation(
      plan: MySqlBootstrapPlanIdentity, expected: MySqlSchemaAuditExpectation
  ): Boolean =
    plan != null && expected != null && plan.mysqlServerUuid == expected.serverUuid &&
      plan.schema == expected.schema && plan.schemaFormat == expected.profile.schemaFormat &&
      plan.physicalFormat == expected.expectedPhysicalFormat.version &&
      plan.provisioningUuid == expected.provisioningUuid && plan.bindingUuid == expected.bindingUuid &&
      plan.canonicalDdlSha256.equalsIgnoreCase(expected.ddlSha256) &&
      expected.profile == MySqlSchemaAuditProfile.Format3 &&
      expected.expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 &&
      expected.expectedImport == MySqlExpectedNoImport

  private[MySqlFreshWorldBootstrap] def validateMarkerAcknowledgement(
      ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck,
      location: WindowsWorldProvisioner.CanonicalMarkerLocation,
      plan: MySqlBootstrapPlanIdentity
  ): Unit =
    if ack == null || location == null || plan == null then rejectProof("native marker ACK inputs are required")
    if ack.operationUuid() != plan.bootstrapOperationUuid || ack.bindingUuid() != plan.bindingUuid ||
      ack.physicalFormat() != 2 ||
      ack.canonicalPath().toAbsolutePath().normalize() != location.canonicalPath().toAbsolutePath().normalize() ||
      !ack.parentIdentity().equals(location.dataDirectoryIdentity()) || ack.fileIdentity() == null ||
      ack.durabilityProtocol() != "WIN32_WRITE_FLUSH_MOVE_WRITE_THROUGH_CANONICAL_FLUSH_READ_CLOSE_V1" then
      rejectProof("native marker ACK is not the exact durable fixed-path publication")
    val expectedHash = sha256Hex(sha256(WindowsMarkerIo.markerBytes(plan.bindingUuid, 2)))
    if !ack.markerSha256().equalsIgnoreCase(expectedHash) then rejectProof("native marker ACK digest differs from the exact physical2 token bytes")

  private[MySqlFreshWorldBootstrap] def receiptHash(fresh: WindowsWorldProvisioner.FreshProvisioningProof): Vector[Byte] =
    if fresh == null then rejectProof("opaque Fresh receipt is required")
    val bytes = fresh.receiptCanonicalBytes()
    val hash = sha256(bytes)
    if !sha256Hex(hash).equalsIgnoreCase(fresh.receiptSha256()) then rejectProof("opaque Fresh receipt changed after prepare")
    hash

  private[MySqlFreshWorldBootstrap] def parseAckHash(value: String): Vector[Byte] =
    if value == null || !value.matches("(?i)[0-9a-f]{64}") then rejectProof("native marker ACK SHA-256 is malformed")
    HexFormat.of().parseHex(value).toVector

  private def sha256(bytes: Array[Byte]): Vector[Byte] =
    if bytes == null then rejectProof("B1 digest input is required")
    MessageDigest.getInstance("SHA-256").digest(bytes).toVector

  private def sha256Hex(bytes: Vector[Byte]): String = HexFormat.of().formatHex(bytes.toArray)

  private def rejectProof(message: String): Nothing =
    throw new SQLTransactionRollbackException("MySQL B1 completion proof rejected: " + message)
